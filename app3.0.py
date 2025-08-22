import io
from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st

# =============================
# Utilidades comunes
# =============================

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Fecha alta", "Fecha entrada", "Fecha salida"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            st.error(f"Falta la columna obligatoria: {col}")
            st.stop()
    return df


def load_excel(files: List[io.BytesIO]) -> pd.DataFrame:
    frames = []
    for f in files:
        try:
            xls = pd.ExcelFile(f)
            sheet = (
                "Estado de pagos de las reservas"
                if "Estado de pagos de las reservas" in xls.sheet_names
                else xls.sheet_names[0]
            )
            df = pd.read_excel(xls, sheet_name=sheet)
            frames.append(df)
        except Exception as e:
            st.error(f"No se pudo leer un archivo: {e}")
            st.stop()
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    expected = ["Alojamiento", "Fecha alta", "Fecha entrada", "Fecha salida", "Precio"]
    missing = [c for c in expected if c not in df_all.columns]
    if missing:
        st.error(f"Faltan columnas requeridas: {', '.join(missing)}")
        st.stop()
    df_all = parse_dates(df_all)
    df_all["Alojamiento"] = df_all["Alojamiento"].astype(str).str.strip()
    df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce").fillna(0.0)
    return df_all


def expand_reservations(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        entrada, salida = r["Fecha entrada"], r["Fecha salida"]
        if pd.isna(entrada) or pd.isna(salida):
            continue
        noches = pd.date_range(entrada, salida - timedelta(days=1), freq="D")
        if len(noches) == 0:
            continue
        for n in noches:
            if start <= n <= end:
                rows.append(
                    {
                        "Alojamiento": r["Alojamiento"],
                        "Fecha": n,
                        "Precio": r["Precio"],
                        "Total noches reserva": len(noches),
                    }
                )
    if not rows:
        return pd.DataFrame(
            columns=["Alojamiento", "Fecha", "Precio", "Total noches reserva"]
        )
    return pd.DataFrame(rows)


def compute_kpis(
    df_all: pd.DataFrame,
    cutoff: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    inventory_override: Optional[int] = None,
    filter_props: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, dict]:
    # 1) Filtrar por corte y propiedades
    df_cut = df_all[df_all["Fecha alta"] <= cutoff].copy()
    if filter_props:
        df_cut = df_cut[df_cut["Alojamiento"].isin(filter_props)]

    # 2) Expandir a noches
    nightly = expand_reservations(df_cut, period_start, period_end)

    if nightly.empty:
        inv = len(set(filter_props)) if filter_props else df_all["Alojamiento"].nunique()
        if inventory_override is not None and inventory_override > 0:
            inv = inventory_override
        days = (period_end - period_start).days + 1
        total = {
            "noches_ocupadas": 0,
            "noches_disponibles": inv * days,
            "ocupacion_pct": 0.0,
            "ingresos": 0.0,
            "adr": 0.0,
            "revpar": 0.0,
        }
        return (
            pd.DataFrame(columns=["Alojamiento", "Noches ocupadas", "Ingresos", "ADR"]),
            total,
        )

    nightly["Ingreso proporcional"] = nightly["Precio"] / nightly["Total noches reserva"]

    by_prop = (
        nightly.groupby("Alojamiento")
        .agg(**{"Noches ocupadas": ("Fecha", "count"), "Ingresos": ("Ingreso proporcional", "sum")})
        .reset_index()
    )
    by_prop["ADR"] = by_prop["Ingresos"] / by_prop["Noches ocupadas"]

    noches_ocupadas = int(by_prop["Noches ocupadas"].sum())
    ingresos = float(by_prop["Ingresos"].sum())
    adr = float(ingresos / noches_ocupadas) if noches_ocupadas > 0 else 0.0

    inv = len(set(filter_props)) if filter_props else df_all["Alojamiento"].nunique()
    if inventory_override is not None and inventory_override > 0:
        inv = inventory_override
    days = (period_end - period_start).days + 1
    noches_disponibles = inv * days
    ocupacion_pct = (noches_ocupadas / noches_disponibles * 100) if noches_disponibles > 0 else 0.0
    revpar = ingresos / noches_disponibles if noches_disponibles > 0 else 0.0

    tot = {
        "noches_ocupadas": noches_ocupadas,
        "noches_disponibles": noches_disponibles,
        "ocupacion_pct": ocupacion_pct,
        "ingresos": ingresos,
        "adr": adr,
        "revpar": revpar,
    }

    return by_prop.sort_values("Alojamiento"), tot


# =============================
# App
# =============================

st.set_page_config(page_title="Consultas OTB por corte", layout="wide")
st.title("📅 Consultas OTB – Ocupación, ADR y RevPAR a fecha de corte")
st.caption("Elige un modo en el menú lateral. Cada modo es independiente y permite subir archivos.")

# -----------------------------
# Menú de modos (independientes)
# -----------------------------
mode = st.sidebar.radio(
    "Modo de consulta",
    [
        "Consulta normal",
        "KPIs por meses",
        "Evolución por fecha de corte",
    ],
)

# =============================
# MODO 1: Consulta normal
# =============================
if mode == "Consulta normal":
    with st.sidebar:
        st.header("Archivos (obligatorio)")
        files_normal = st.file_uploader(
            "Sube uno o varios Excel",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="files_normal",
        )
        st.divider()
        st.header("Parámetros")
        cutoff_normal = st.date_input("Fecha de corte", value=date(2024, 8, 21), key="cutoff_normal")
        c1, c2 = st.columns(2)
        with c1:
            start_normal = st.date_input("Inicio del periodo", value=date(2024, 9, 1), key="start_normal")
        with c2:
            end_normal = st.date_input("Fin del periodo", value=date(2024, 9, 30), key="end_normal")
        inv_normal = st.number_input(
            "Sobrescribir inventario (nº alojamientos)",
            min_value=0,
            value=0,
            step=1,
            key="inv_normal",
        )

    if not files_normal:
        st.info("➡️ Sube archivos para calcular.")
        st.stop()

    raw_n = load_excel(files_normal)
    all_props_n = sorted(raw_n["Alojamiento"].dropna().astype(str).unique().tolist())

    with st.sidebar:
        props_normal = st.multiselect(
            "Filtrar alojamientos (opcional)", options=all_props_n, default=[], key="props_normal"
        )

    by_prop_n, total_n = compute_kpis(
        df_all=raw_n,
        cutoff=pd.to_datetime(cutoff_normal),
        period_start=pd.to_datetime(start_normal),
        period_end=pd.to_datetime(end_normal),
        inventory_override=int(inv_normal) if inv_normal > 0 else None,
        filter_props=props_normal if props_normal else None,
    )

    st.subheader("Resultados totales")
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    col1.metric("Noches ocupadas", f"{total_n['noches_ocupadas']:,}".replace(",", "."))
    col2.metric("Noches disponibles", f"{total_n['noches_disponibles']:,}".replace(",", "."))
    col3.metric("Ocupación", f"{total_n['ocupacion_pct']:.2f}%")
    col4.metric("Ingresos (€)", f"{total_n['ingresos']:.2f}")
    col5.metric("ADR (€)", f"{total_n['adr']:.2f}")
    col6.metric("RevPAR (€)", f"{total_n['revpar']:.2f}")

    st.divider()
    st.subheader("Detalle por alojamiento")
    if by_prop_n.empty:
        st.warning("Sin noches ocupadas en el periodo a la fecha de corte.")
    else:
        st.dataframe(by_prop_n, use_container_width=True)
        csv = by_prop_n.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 Descargar detalle (CSV)", data=csv, file_name="detalle_por_alojamiento.csv", mime="text/csv"
        )

# =============================
# MODO 2: KPIs por meses (línea)
# =============================
elif mode == "KPIs por meses":
    with st.sidebar:
        st.header("Archivos (obligatorio)")
        files_m = st.file_uploader(
            "Sube uno o varios Excel",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="files_months",
        )
        st.divider()
        cutoff_m = st.date_input("Fecha de corte", value=date(2024, 8, 21), key="cutoff_months")

    if not files_m:
        st.info("➡️ Sube archivos para calcular.")
        st.stop()

    raw_m = load_excel(files_m)
    all_props_m = sorted(raw_m["Alojamiento"].dropna().astype(str).unique().tolist())

    with st.sidebar:
        props_m = st.multiselect(
            "Filtrar alojamientos (opcional)", options=all_props_m, default=[], key="props_months"
        )
        inv_m = st.number_input(
            "Sobrescribir inventario (nº alojamientos)", min_value=0, value=0, step=1, key="inv_months"
        )
        # Meses disponibles
        _min = pd.concat([raw_m["Fecha entrada"].dropna(), raw_m["Fecha salida"].dropna()]).min()
        _max = pd.concat([raw_m["Fecha entrada"].dropna(), raw_m["Fecha salida"].dropna()]).max()
        months_options = []
        if pd.notna(_min) and pd.notna(_max):
            months_options = [str(p) for p in pd.period_range(_min.to_period("M"), _max.to_period("M"), freq="M")]
        selected_months_m = st.multiselect(
            "Meses a graficar (YYYY-MM)", options=months_options, default=[], key="months_months"
        )
        metric_choice = st.radio(
            "Métrica a graficar",
            ["Ocupación %", "ADR (€)", "RevPAR (€)"]
        )

    st.subheader("📈 KPIs por meses (a fecha de corte)")
    if selected_months_m:
        rows_m = []
        for ym in selected_months_m:
            p = pd.Period(ym, freq="M")
            start_m = p.to_timestamp(how="start")
            end_m = p.to_timestamp(how="end")
            _bp, _tot = compute_kpis(
                df_all=raw_m,
                cutoff=pd.to_datetime(cutoff_m),
                period_start=start_m,
                period_end=end_m,
                inventory_override=int(inv_m) if inv_m > 0 else None,
                filter_props=props_m if props_m else None,
            )
            rows_m.append(
                {
                    "Mes": ym,
                    "Noches ocupadas": _tot["noches_ocupadas"],
                    "Noches disponibles": _tot["noches_disponibles"],
                    "Ocupación %": _tot["ocupacion_pct"],
                    "ADR (€)": _tot["adr"],
                    "RevPAR (€)": _tot["revpar"],
                }
            )
        df_months = pd.DataFrame(rows_m).sort_values("Mes")
        st.line_chart(df_months.set_index("Mes")[[metric_choice]], height=280)
        st.dataframe(df_months, use_container_width=True)
        csvm = df_months.to_csv(index=False).encode("utf-8-sig")
        st.download_button("📥 Descargar KPIs por mes (CSV)", data=csvm, file_name="kpis_por_mes.csv", mime="text/csv")
    else:
        st.info("Selecciona meses en la barra lateral para ver la gráfica.")

# =============================
# MODO 3: Evolución por fecha de corte
# =============================
elif mode == "Evolución por fecha de corte":
    with st.sidebar:
        st.header("Archivos (obligatorio)")
        files_e = st.file_uploader(
            "Sube uno o varios Excel",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="files_evo",
        )

        st.divider()
        st.header("Rango de corte")
        evo_cut_start = st.date_input("Inicio de corte", value=date(2024, 4, 1), key="evo_cut_start_new")
        evo_cut_end = st.date_input("Fin de corte", value=date(2024, 4, 30), key="evo_cut_end_new")

        st.header("Periodo objetivo")
        evo_target_start = st.date_input("Inicio del periodo", value=date(2024, 9, 1), key="evo_target_start_new")
        evo_target_end = st.date_input("Fin del periodo", value=date(2024, 9, 30), key="evo_target_end_new")

        inv_e = st.number_input(
            "Sobrescribir inventario (nº alojamientos)", min_value=0, value=0, step=1, key="inv_evo"
        )
        run_evo = st.button("Calcular evolución", type="primary", key="btn_evo")

    if not files_e:
        st.info("➡️ Sube archivos para calcular.")
        st.stop()

    raw_e = load_excel(files_e)
    all_props_e = sorted(raw_e["Alojamiento"].dropna().astype(str).unique().tolist())

    with st.sidebar:
        props_e = st.multiselect(
            "Filtrar alojamientos (opcional)", options=all_props_e, default=[], key="props_evo"
        )

    st.subheader("📉 Evolución de KPIs vs fecha de corte")
    if run_evo:
        cut_start_ts = pd.to_datetime(evo_cut_start)
        cut_end_ts = pd.to_datetime(evo_cut_end)
        if cut_start_ts > cut_end_ts:
            st.error("El inicio del rango de corte no puede ser posterior al fin.")
        else:
            rows_e = []
            for c in pd.date_range(cut_start_ts, cut_end_ts, freq="D"):
                _bp, tot_c = compute_kpis(
                    df_all=raw_e,
                    cutoff=c,
                    period_start=pd.to_datetime(evo_target_start),
                    period_end=pd.to_datetime(evo_target_end),
                    inventory_override=int(inv_e) if inv_e > 0 else None,
                    filter_props=props_e if props_e else None,
                )
                rows_e.append(
                    {
                        "Corte": c.date().isoformat(),
                        "Noches ocupadas": tot_c["noches_ocupadas"],
                        "Noches disponibles": tot_c["noches_disponibles"],
                        "Ocupación %": tot_c["ocupacion_pct"],
                        "ADR (€)": tot_c["adr"],
                        "RevPAR (€)": tot_c["revpar"],
                        "Ingresos (€)": tot_c["ingresos"],
                    }
                )
            df_evo = pd.DataFrame(rows_e)
            if df_evo.empty:
                st.info("No hay datos para el rango seleccionado.")
            else:
                metric_choice_e = st.radio(
                    "Métrica a graficar",
                    ["Ocupación %", "ADR (€)", "RevPAR (€)"],
                    horizontal=True,
                    key="metric_evo",
                )
                st.line_chart(df_evo.set_index("Corte")[[metric_choice_e]], height=300)
                st.dataframe(df_evo, use_container_width=True)
                csve = df_evo.to_csv(index=False).encode("utf-8-sig")
                st.download_button("📥 Descargar evolución (CSV)", data=csve, file_name="evolucion_kpis.csv", mime="text/csv")
    else:
        st.caption("Configura los parámetros en la barra lateral y pulsa **Calcular evolución**.")

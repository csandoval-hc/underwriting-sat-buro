# Ruta: src/underwriting/ui/sat_views.py
# Archivo: sat_views.py

from __future__ import annotations

import pandas as pd
import streamlit as st
from underwriting.domain.models import TaxStatus




_MESES_ES = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}


def _format_date_es(value: str | None) -> str:
    """
    Espera 'YYYY-MM-DD' o 'YYYY-MM-DDTHH:MM:SS...'
    Devuelve 'D de mes de YYYY'. Si viene vacío, '-'.
    """
    if not value:
        return "-"
    s = str(value).strip()
    if not s:
        return "-"

    # nos quedamos con la parte de fecha si viene datetime
    if "T" in s:
        s = s.split("T", 1)[0]

    parts = s.split("-")
    if len(parts) != 3:
        return str(value)

    try:
        y = int(parts[0])
        m = int(parts[1])
        d = int(parts[2])
        mes = _MESES_ES.get(m, str(m))
        return f"{d} de {mes} de {y}"
    except Exception:
        return str(value)


def _format_percentage(value: float | None) -> str:
    if value is None:
        return ""
    try:
        # 70.0 -> 70%
        return f"{int(round(float(value)))}%"
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# NUEVO (mínimo): Donut + tablas Top10 + barra ventas/gastos (si existen en sesión)
# ─────────────────────────────────────────────────────────────────────────────
def _drop_transactions_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "transactions" in df.columns:
        return df.drop(columns=["transactions"])
    return df


def _guess_label_value_cols(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Intenta inferir columnas:
      - label: primera columna tipo texto
      - value: primera columna numérica (o convertible) que NO sea 'transactions'
    """
    if df is None or df.empty:
        return None, None

    cols = list(df.columns)

    # label: la primera col que se vea "texto"
    label_col = None
    for c in cols:
        if c == "transactions":
            continue
        s = df[c]
        if s.dtype == "object":
            label_col = c
            break

    # value: primer numérico o convertible, excluyendo transactions
    value_col = None
    for c in cols:
        if c == "transactions":
            continue
        if c == label_col:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().any():
            value_col = c
            break

    return label_col, value_col


def _render_donut(df: pd.DataFrame, title: str) -> None:
    """
    Donut chart (pie con centro vacío).
    Usa Altair (evita dependencia de matplotlib).
    """
    if df is None or df.empty:
        st.info("Sin datos para el periodo.")
        return

    label_col, value_col = _guess_label_value_cols(df)
    if not label_col or not value_col:
        st.info("Sin columnas suficientes para graficar distribución.")
        return

    d = df[[label_col, value_col]].copy()
    d[label_col] = d[label_col].astype(str).fillna("")
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)

    if float(d[value_col].sum()) <= 0:
        st.info("Sin montos positivos para graficar distribución.")
        return

    import altair as alt  # Altair viene con Streamlit

    chart = (
        alt.Chart(d)
        .mark_arc(innerRadius=55)
        .encode(
            theta=alt.Theta(field=value_col, type="quantitative"),
            color=alt.Color(field=label_col, type="nominal", legend=None),
            tooltip=[alt.Tooltip(label_col, type="nominal"), alt.Tooltip(value_col, type="quantitative")],
        )
        .properties(title=title, height=260)
    )

    st.altair_chart(chart, use_container_width=True)


def _render_top10_card(title: str, df: pd.DataFrame) -> None:
    with st.container(border=True):
        st.markdown(f"### {title}")

        # Donut arriba
        _render_donut(df, "Distribución")

        # Tabla debajo (sin transactions)
        df_show = _drop_transactions_for_display(df)
        if df_show is None or df_show.empty:
            st.info("Sin datos para el periodo.")
        else:
            st.dataframe(df_show, use_container_width=True, hide_index=True)


def _render_utilidad_12m_grouped(df: pd.DataFrame) -> None:
    """
    Gráfica 'Utilidad Fiscal últimos 12 meses':
    - Ventas y Gastos como barras individuales lado a lado
    - (Si existe utilidad, la pintamos como línea)
    """
    if df is None or df.empty:
        st.info("Sin datos para el periodo.")
        return

    # heurística de columnas
    cols_lower = {c.lower(): c for c in df.columns}

    # mes
    month_col = cols_lower.get("mes") or cols_lower.get("month") or cols_lower.get("period") or cols_lower.get("fecha")
    ventas_col = cols_lower.get("ventas") or cols_lower.get("ingresos")
    gastos_col = cols_lower.get("gastos") or cols_lower.get("egresos")
    utilidad_col = (
        cols_lower.get("utilidad_fiscal")
        or cols_lower.get("utilidad")
        or cols_lower.get("ebitda")  # por si lo traes así y lo renombraste en UI
    )

    if not month_col or not ventas_col or not gastos_col:
        st.info("No encontré columnas requeridas para la gráfica (mes/ventas/gastos).")
        return

    d = df.copy()
    d[ventas_col] = pd.to_numeric(d[ventas_col], errors="coerce").fillna(0.0)
    d[gastos_col] = pd.to_numeric(d[gastos_col], errors="coerce").fillna(0.0)
    if utilidad_col and utilidad_col in d.columns:
        d[utilidad_col] = pd.to_numeric(d[utilidad_col], errors="coerce").fillna(0.0)

    # ordenar por mes si es posible
    # (no forzamos parseo agresivo para no cambiar nada de tu data)
    try:
        d["_ord"] = pd.to_datetime(d[month_col], errors="coerce")
        if d["_ord"].notna().any():
            d = d.sort_values("_ord")
        d = d.drop(columns=["_ord"])
    except Exception:
        pass

    import altair as alt

    d_long = d.melt(
        id_vars=[month_col],
        value_vars=[ventas_col, gastos_col],
        var_name="Tipo",
        value_name="Monto",
    )

    chart = (
        alt.Chart(d_long)
        .mark_bar()
        .encode(
            x=alt.X(f"{month_col}:N", title="Mes"),
            xOffset=alt.XOffset("Tipo:N"),
            y=alt.Y("Monto:Q", title="Monto"),
            color=alt.Color("Tipo:N"),
            tooltip=[alt.Tooltip(f"{month_col}:N"), "Tipo:N", alt.Tooltip("Monto:Q", format=",.2f")],
        )
        .properties(height=320)
    )

    st.altair_chart(chart, use_container_width=True)

    # utilidad como línea (si existe)
    if utilidad_col and utilidad_col in d.columns:
        line = (
            alt.Chart(d)
            .mark_line(point=True)
            .encode(
                x=alt.X(f"{month_col}:N"),
                y=alt.Y(f"{utilidad_col}:Q"),
                tooltip=[alt.Tooltip(f"{month_col}:N"), alt.Tooltip(f"{utilidad_col}:Q", format=",.2f")],
            )
            .properties(height=220)
        )
        st.altair_chart(line, use_container_width=True)


def render_tax_status_cards(tax_status: TaxStatus) -> None:
    # --------
    # Card 1: Actividades Económicas
    # --------
    with st.container(border=True):
        st.markdown("### 💼 Actividades Económicas")

        if not tax_status.economicActivities:
            st.info("No se encontraron actividades económicas.")
        else:
            df = pd.DataFrame(
                [
                    {
                        "Nombre": a.name,
                        "Inicio": _format_date_es(a.startDate),
                        "Fin": _format_date_es(a.endDate) if a.endDate else "-",
                        "Porcentaje": _format_percentage(a.percentage),
                    }
                    for a in tax_status.economicActivities
                ]
            )

            # Orden de columnas fijo (por si pandas reordena)
            df = df[["Nombre", "Inicio", "Fin", "Porcentaje"]]

            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Orden": st.column_config.NumberColumn(width="small"),
                    "Nombre": st.column_config.TextColumn(width="large"),
                    "Inicio": st.column_config.TextColumn(width="medium"),
                    "Fin": st.column_config.TextColumn(width="medium"),
                    "Porcentaje": st.column_config.TextColumn(width="small"),
                },
            )

    st.write("")  # espacio visual entre cards

    # --------
    # Card 2: Regímenes Fiscales
    # --------
    with st.container(border=True):
        st.markdown("### 🧾 Regímenes Fiscales")

        if not tax_status.taxRegimes:
            st.info("No se encontraron regímenes fiscales.")
        else:
            df = pd.DataFrame(
                [
                    {
                        "Código": r.code,
                        "Nombre": r.name,
                        "Inicio": _format_date_es(r.startDate),
                        "Fin": _format_date_es(r.endDate) if r.endDate else "-",
                    }
                    for r in tax_status.taxRegimes
                ]
            )

            df = df[["Código", "Nombre", "Inicio", "Fin"]]

            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Código": st.column_config.TextColumn(width="small"),
                    "Nombre": st.column_config.TextColumn(width="large"),
                    "Inicio": st.column_config.TextColumn(width="medium"),
                    "Fin": st.column_config.TextColumn(width="medium"),
                },
            )

    # ==========================================================
    # ✅ Cards de Facturas (Prod/Serv resumen) bajo Regímenes
    # ==========================================================
    st.write("")

    egresos_df = st.session_state.get("prodserv_resumen_egresos_df")
    ingresos_df = st.session_state.get("prodserv_resumen_ingresos_df")

    # ==========================================================
    # ✅ NUEVO: Top 10 + donuts (si existen en session_state)
    # ==========================================================
    st.write("")

    top10_clientes = st.session_state.get("top10_clientes_df")
    top10_proveedores = st.session_state.get("top10_proveedores_df")

    if top10_clientes is not None or top10_proveedores is not None:
        col_a, col_b = st.columns(2, gap="large")

        with col_a:
            if isinstance(top10_clientes, pd.DataFrame):
                _render_top10_card("Top 10 clientes", top10_clientes)

        with col_b:
            if isinstance(top10_proveedores, pd.DataFrame):
                _render_top10_card("Top 10 proveedores", top10_proveedores)

    # ==========================================================
    # ✅ NUEVO: Utilidad Fiscal últimos 12 meses (barras separadas)
    # ==========================================================
    st.write("")

    utilidad_12m = st.session_state.get("utilidad_fiscal_12m_df")
    if isinstance(utilidad_12m, pd.DataFrame) and not utilidad_12m.empty:
        with st.container(border=True):
            st.markdown("### Utilidad Fiscal últimos 12 meses")
            _render_utilidad_12m_grouped(utilidad_12m)

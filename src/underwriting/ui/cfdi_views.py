# Ruta: src/underwriting/ui/cfdi_views.py
# Archivo: cfdi_views.py

from __future__ import annotations

import pandas as pd
import streamlit as st


def _make_unique(cols):
    seen = {}
    out = []
    for c in cols:
        c = str(c)
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c} ({seen[c]})")
    return out


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [str(c) for c in df.columns]
    if len(set(cols)) == len(cols):
        return df
    out = df.copy()
    out.columns = _make_unique(cols)
    return out


def _safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    out = df.copy().reset_index(drop=True)

    # columnas a str + únicas
    out.columns = [str(c) for c in out.columns]
    out = _dedupe_columns(out)

    # celdas no serializables -> str
    for c in out.columns:
        s = out[c]
        try:
            if s.map(lambda x: isinstance(x, (list, dict, tuple, set))).any():
                out[c] = s.map(lambda x: str(x) if isinstance(x, (list, dict, tuple, set)) else x)
        except Exception:
            # si algo raro pasa, castea todo a str
            out[c] = s.astype(str)

    # normaliza inf -> NaN (al frontend no le encanta)
    out = out.replace([float("inf"), float("-inf")], pd.NA)

    return out


def _fmt_money_no_decimals(x) -> str:
    try:
        return f"${int(round(float(x))):,}"
    except Exception:
        return "$0"


def _format_df_no_decimals_and_money(df: pd.DataFrame) -> pd.DataFrame:
    """Formato usado por las cards de Prod/Serv:
    - Redondea numéricos a enteros (sin decimales)
    - Formatea columnas típicas de monto como $1,234
    """
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    # 1) Redondear numéricos (sin decimales)
    for c in d.columns:
        s = pd.to_numeric(d[c], errors="coerce")
        if s.notna().mean() > 0.7:  # mayoría numérica
            d[c] = s.round(0).astype("Int64")

    # 2) Formato moneda en columnas típicas de monto
    money_cols = {"monto", "total", "subtotal", "importe", "paidamount", "dueamount", "discount"}
    for c in d.columns:
        if str(c).strip().lower() in money_cols:
            d[c] = d[c].map(_fmt_money_no_decimals)

    return d


def _render_table_card(title: str, df: pd.DataFrame, key: str) -> None:
    with st.container(border=True):
        st.markdown(f"### 🧾 {title}")
        if df is None or getattr(df, "empty", True):
            st.info("Sin datos para el periodo.")
            return

        safe = _safe_df(df)
        safe = _format_df_no_decimals_and_money(safe)
        safe = _dedupe_columns(safe)

        st.dataframe(
            safe,
            use_container_width=False,   # ✅ tu CSS lo estira a 100%
            hide_index=True,
            key=key,
        )


def render_prodserv_dual_cards(
    *,
    title_left: str,
    df_left: pd.DataFrame,
    title_right: str,
    df_right: pd.DataFrame,
) -> None:
    st.write("")
    col1, col2 = st.columns(2)
    with col1:
        _render_table_card(title_left, df_left, key="df_prodserv_resumen_ingresos_card")
    with col2:
        _render_table_card(title_right, df_right, key="df_prodserv_resumen_egresos_card")

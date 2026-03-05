from __future__ import annotations

import os
import re
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
import streamlit as st
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Path bootstrap (same as app.py, but isolated so we don't import app.py)
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in os.sys.path:
    os.sys.path.insert(0, str(SRC))

from underwriting.infrastructure.config import load_settings
from underwriting.infrastructure.syntage_client import SyntageClient
from underwriting.application.cfdi_service import CfdiService


def _bootstrap_env_from_secrets() -> None:
    """Expose Streamlit secrets as env vars so existing services keep working."""
    load_dotenv()
    if hasattr(st, "secrets"):
        for key in ["SYNTAGE_API_KEY", "SYNTAGE_BASE_URL", "MOFFIN_TOKEN"]:
            try:
                val = st.secrets.get(key)
            except Exception:
                val = None
            if val:
                os.environ[key] = str(val).strip()


# -----------------------------------------------------------------------------
# RFC sanitizer (same logic as app.py)
# -----------------------------------------------------------------------------
_RFC_RE = re.compile(r"[^A-Z0-9&Ñ]")


def _clean_rfc(x: str) -> str:
    s = (x or "").strip().upper()
    return _RFC_RE.sub("", s)


@st.cache_resource
def get_cfdi_service() -> CfdiService:
    settings = load_settings()
    client = SyntageClient(settings=settings)
    return CfdiService(client=client)


@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_cfdi(rfc: str, date_from: date | None, date_to: date | None, local_dir: str):
    svc = get_cfdi_service()
    return svc.fetch_syntage_xml(rfc=_clean_rfc(rfc), date_from=date_from, date_to=date_to)


def _st_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """Make DataFrame safe for Streamlit frontend."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    d = df.copy()

    # Ensure unique string columns
    cols = [str(c) for c in d.columns]
    seen = {}
    out_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out_cols.append(c)
        else:
            seen[c] += 1
            out_cols.append(f"{c}__{seen[c]}")
    d.columns = out_cols

    def _cell(v):
        if v is None:
            return None
        try:
            if isinstance(v, (dict, list, tuple, set)):
                return str(v)
            if isinstance(v, (bytes, bytearray)):
                return v.decode(errors="ignore")
            return v
        except Exception:
            return str(v)

    for c in d.columns:
        if d[c].dtype == "object":
            d[c] = d[c].map(_cell)

    d = d.replace([pd.NA, float("inf"), float("-inf")], None)
    return d


def _render_df(df: pd.DataFrame, *, key: str, money_cols: list[str] | None = None) -> None:
    if df is None or df.empty:
        st.info("Sin datos para el periodo.")
        return

    d = df.copy()
    d = d.replace([np.inf, -np.inf], np.nan)

    money_cols = money_cols or []
    for c in money_cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0).round(0)

    d = _st_safe_df(d)

    sty = d.style
    if money_cols:
        fmt = {c: "${:,.0f}" for c in money_cols if c in d.columns}
        if fmt:
            sty = sty.format(fmt)

    st.dataframe(sty, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
_bootstrap_env_from_secrets()

st.title("Facturas (CFDI)")
st.caption("Vista de facturas listadas por Syntage (emitidas y recibidas)")

# Prefer reuse of data already loaded in main app (shared session_state across pages)
cfdi_data = st.session_state.get("cfdi_data")
last_rfc = st.session_state.get("last_rfc")

with st.container(border=True):
    c1, c2, c3, c4 = st.columns([2.2, 1.3, 1.3, 1.2])

    with c1:
        rfc_in = st.text_input("RFC", value=last_rfc or "", placeholder="PEIC211118IS0", key="facturas_rfc")
        rfc = _clean_rfc(rfc_in)

    with c2:
        d_from = st.date_input(
            "Desde",
            value=st.session_state.get("cfdi_date_from") or (date.today().replace(day=1)),
            key="facturas_from",
        )

    with c3:
        d_to = st.date_input(
            "Hasta",
            value=st.session_state.get("cfdi_date_to") or (date.today()),
            key="facturas_to",
        )

    with c4:
        run = st.button("Cargar", type="primary", use_container_width=True, disabled=not (12 <= len(rfc) <= 13))

if run:
    with st.spinner("Consultando facturas en Syntage…"):
        local_dir = str(ROOT / "data" / "cfdi_xml")
        try:
            cfdi_data = fetch_cfdi(rfc=rfc, date_from=d_from, date_to=d_to, local_dir=local_dir)
            st.session_state["cfdi_data"] = cfdi_data
            st.session_state["last_rfc"] = rfc
            st.session_state["cfdi_date_from"] = d_from
            st.session_state["cfdi_date_to"] = d_to
        except Exception as e:
            st.error(f"Error consultando facturas: {e}")
            cfdi_data = None

if not cfdi_data:
    st.info("Carga un RFC y rango, o primero presiona **Calcular** en la página principal para reutilizar el cache.")
    st.stop()

emit_df = cfdi_data.get("emit_invoices_df", pd.DataFrame())
rec_df = cfdi_data.get("rec_invoices_df", pd.DataFrame())

meta = cfdi_data.get("meta") or {}
if meta:
    st.caption(
        f"meta: emit_listed={meta.get('emit_listed')} downloaded={meta.get('emit_downloaded')} failed={meta.get('emit_failed')} | "
        f"rec_listed={meta.get('rec_listed')} downloaded={meta.get('rec_downloaded')} failed={meta.get('rec_failed')}"
    )


tabs = st.tabs(["Emitidas", "Recibidas"])

with tabs[0]:
    st.subheader("Facturas emitidas")
    _render_df(
        emit_df,
        key="facturas_emit",
        money_cols=[c for c in ["subtotal", "total", "monto", "amount", "importe"] if c in emit_df.columns],
    )

    if emit_df is not None and not emit_df.empty:
        csv = emit_df.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV (emitidas)", data=csv, file_name="facturas_emitidas.csv", mime="text/csv")

with tabs[1]:
    st.subheader("Facturas recibidas")
    _render_df(
        rec_df,
        key="facturas_rec",
        money_cols=[c for c in ["subtotal", "total", "monto", "amount", "importe"] if c in rec_df.columns],
    )

    if rec_df is not None and not rec_df.empty:
        csv = rec_df.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV (recibidas)", data=csv, file_name="facturas_recibidas.csv", mime="text/csv")

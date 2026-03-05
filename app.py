# Ruta: app.py
# Archivo: app.py

from __future__ import annotations

from pathlib import Path
import sys
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
import re  # ✅ [FIX] limpiar RFC para evitar '.' u otros caracteres inválidos

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


import os
import requests

import pandas as pd
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

# --- AUTH INTEGRATION ---
from auth import require_login, logout_button

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

from dateutil.relativedelta import relativedelta

from underwriting.infrastructure.config import load_settings
from underwriting.infrastructure.syntage_client import SyntageClient
from underwriting.application.sat_service import SatService
from underwriting.application.cfdi_service import CfdiService
from underwriting.ui.sat_views import render_tax_status_cards
from underwriting.ui.cfdi_views import render_prodserv_dual_cards
from types import SimpleNamespace
from underwriting.application.buro_service import obtener_buro_moffin_por_rfc
from underwriting.application.cap_table_service import CapTableService


load_dotenv()

LOGO_PATH = ROOT / "src" / "underwriting" / "assets" / "HayCash_Logo_FC_RGB.png"
FAVICON_PATH = ROOT / "src" / "underwriting" / "assets" / "HayCash_Simbolo_FC_RGB.png"

# ✅ Configuración de página con Sidebar contraído por defecto
st.set_page_config(
    page_title="Underwriting",
    page_icon=str(FAVICON_PATH),  # ✅ favicon
    layout="wide",
    initial_sidebar_state="collapsed", 
)

# ✅ Botón de Descarga PDF
def render_pdf_download_button():
    components.html(
        """
        <script>
        function printPage() {
            window.print();
        }
        </script>
        <div style="display: flex; justify-content: flex-end;">
            <button onclick="printPage()" style="
                background-color: #ff4b4b;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
                font-family: sans-serif;
            ">📄 Descargar PDF</button>
        </div>
        """,
        height=45,
    )

# =============================================================================
# 🔐 AUTHENTICATION GATE
# =============================================================================
user_logged_in = require_login()

# Información de usuario en el sidebar
with st.sidebar:
    st.markdown(f"👤 **Usuario:** {user_logged_in}")
    logout_button()
    st.divider()

_bootstrap_env_from_secrets()

st.markdown(
    """
<style>

/* ==========================
   DATAFRAMES - FULL WIDTH INSIDE CARDS
   Mantiene estabilidad (sin use_container_width) pero estira el componente al 100%.
   ========================== */
div[data-testid="stDataFrame"] { width: 100% !important; }
div[data-testid="stDataFrame"] > div { width: 100% !important; }
div[data-testid="stDataFrame"] table { width: 100% !important; }
div[data-testid="stDataFrame"] .stDataFrame { width: 100% !important; }

/* También para data_editor (si se usa en el futuro) */
div[data-testid="stDataEditor"] { width: 100% !important; }
div[data-testid="stDataEditor"] > div { width: 100% !important; }
/* ==========================
   TOP INPUT BAR - FIXED (PRO)
   Se asigna la clase .uw-topbar via JS al bloque que contiene el marker.
   ========================== */

/* Reserva espacio para que la barra fija no tape el contenido */
div[data-testid="stAppViewContainer"] .main .block-container{
  padding-top: 6.6rem !important;
}


/* Barra fija REAL: pegada arriba del viewport del área main */
.uw-topbar{
  position: fixed !important;
  top: 0 !important;
  left: 0 !important;
  right: 0 !important;
  z-index: 99999 !important;

  background: rgba(255,255,255,0.98) !important;
  backdrop-filter: blur(6px) !important;
  -webkit-backdrop-filter: blur(6px) !important;
  border-bottom: 1px solid rgba(49, 51, 63, 0.12) !important;

  padding: 0.25rem 1rem !important;
  padding-right: 4.5rem !important;
}


/* Compacta un poco los widgets dentro de la barra */
.uw-topbar [data-testid="stTextInput"],
.uw-topbar [data-testid="stSelectbox"],
.uw-topbar [data-testid="stButton"]{
  margin-top: 0.15rem !important;
  margin-bottom: 0.15rem !important;
}

/* Botones en una sola línea */
.uw-topbar button *{
  white-space: nowrap !important;
}

.uw-topbar-spacer{
  height: 0.5rem;   /* ajusta: 5.0–7.0rem */
}

/* Ocultar elementos en la impresión para reporte limpio */
@media print {
    div[data-testid="stSidebar"], 
    .uw-topbar, 
    button, 
    [data-testid="stHeader"], 
    [data-testid="stTabs"] > div:first-child {
        display: none !important;
    }
}

</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# ✅ [FIX] RFC sanitizer (evita '.' y caracteres inválidos que rompen rutas de Syntage)
# =============================================================================
def _clean_rfc(x: str) -> str:
    s = (x or "").strip().upper()
    # deja solo caracteres típicos del RFC (letras/números y & Ñ)
    s = re.sub(r"[^A-Z0-9&Ñ]", "", s)
    return s


# =============================================================================

@st.cache_resource
def get_service() -> SatService:
    settings = load_settings()
    client = SyntageClient(settings=settings)
    return SatService(client=client)


@st.cache_resource
def get_cfdi_service() -> CfdiService:
    settings = load_settings()
    client = SyntageClient(settings=settings)
    return CfdiService(client=client)

# =============================================================================
# ✅ Cap Table: service + cache
# =============================================================================
@st.cache_resource
def get_cap_table_service() -> CapTableService:
    settings = load_settings()
    client = SyntageClient(settings=settings)
    return CapTableService(client=client)


@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_cap_table_df(
    rfc: str,
    *,
    type_filter: str | None = None,
    name_filter: str | None = None,
    rfc_filter: str | None = None,
):
    svc = get_cap_table_service()
    res = svc.get_cap_table_df(
        rfc=_clean_rfc(rfc),  # ✅ [FIX]
        type_filter=type_filter,
        name_filter=name_filter,
        rfc_filter=rfc_filter,
        items_per_page=200,
        max_pages=50,
    )
    return {
        "entity_id": res.entity_id,
        "entity_iri": res.entity_iri,
        "df": res.cap_table,
    }


@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_tax_status(rfc: str):
    service = get_service()
    return service.get_tax_status(_clean_rfc(rfc))  # ✅ [FIX]


@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_cfdi(rfc: str, source: str, date_from: date | None, date_to: date | None, local_dir: str):
    service = get_cfdi_service()
    if source == "local":
        return service.fetch_local_xml(local_dir)
    if source == "syntage":
        return service.fetch_syntage_xml(rfc=_clean_rfc(rfc), date_from=date_from, date_to=date_to)  # ✅ [FIX]
    raise ValueError(f"Fuente CFDI inválida: {source!r}")


@st.cache_data(show_spinner=False)
def load_ps_catalog() -> pd.DataFrame:
    p = ROOT / "src" / "underwriting" / "assets" / "catalogo_productos_servicios_SAT.csv"
    if not p.exists():
        return pd.DataFrame(columns=["clave_prodserv", "producto"])

    df = pd.read_csv(p, dtype=str)
    df = df.rename(columns={c: str(c).strip().lower() for c in df.columns})

    if "clave_prodserv" not in df.columns:
        df = df.rename(columns={df.columns[0]: "clave_prodserv"})
    if "producto" not in df.columns:
        for c in df.columns:
            if c in {"descripcion", "description", "desc"}:
                df = df.rename(columns={c: "producto"})
                break
    if "producto" not in df.columns and len(df.columns) > 1:
        df = df.rename(columns={df.columns[1]: "producto"})

    return df[["clave_prodserv", "producto"]].copy()

# =============================================================================
# Syntage: Concentración (Top clientes / proveedores) - con caché
# =============================================================================
SYNTAGE_BASE_URL = "https://api.syntage.com"


@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_syntage_concentration(rfc: str, kind: str, from_dt: str | None = None, to_dt: str | None = None):
    """
    kind:
      - "customer"  -> /insights/{rfc}/customer-concentration
      - "supplier"  -> /insights/{rfc}/supplier-concentration
    from_dt / to_dt: ISO UTC string, e.g. "2025-01-01T00:00:00Z"
    """
    api_key = os.getenv("SYNTAGE_API_KEY", "")
    if not api_key:
        return None

    r = _clean_rfc(rfc)
    if not r:
        return None

    if kind == "customer":
        path = "customer-concentration"
    elif kind == "supplier":
        path = "supplier-concentration"
    else:
        return None

    url = f"{SYNTAGE_BASE_URL}/insights/{r}/{path}"

    params = {}
    if from_dt:
        params["options[from]"] = from_dt
    if to_dt:
        params["options[to]"] = to_dt

    try:
        resp = requests.get(
            url,
            headers={"X-API-Key": api_key},
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        obj = resp.json()
        data = obj.get("data")
        return data if isinstance(data, list) else None
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_last_extraction_at(rfc: str, extractor: str | None = None) -> str | None:
    api_key = os.getenv("SYNTAGE_API_KEY", "")
    if not api_key:
        return None

    r = _clean_rfc(rfc)
    if not r:
        return None

    url = f"{SYNTAGE_BASE_URL}/extractions"

    params = {
        "taxpayer.id": r,
        "status": "finished",
        "order[finishedAt]": "desc",
        "itemsPerPage": 1,
    }
    if extractor:
        params["extractor"] = extractor  # ej: "invoice" si quieres forzarlo

    try:
        resp = requests.get(
            url,
            headers={
                "X-API-Key": api_key,
                "Accept": "application/ld+json",
            },
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            return None

        obj = resp.json() or {}
        members = obj.get("hydra:member") or []
        if not members:
            return None

        last = members[0] or {}
        dt = last.get("finishedAt") or last.get("updatedAt") or last.get("createdAt")
        if not dt:
            return None

        # Convertir a hora CDMX
        ts = pd.to_datetime(dt, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        ts_cdmx = ts.tz_convert("America/Mexico_City")
        return ts_cdmx.strftime("%Y-%m-%d %H:%M")

    except Exception:
        return None
    
@st.cache_data(show_spinner=False, ttl=60 * 10)
def fetch_ciec_updated_at(rfc: str) -> str | None:
    svc = get_service()  # SatService ya trae SyntageClient configurado
    dt = svc.get_ciec_last_updated_at(_clean_rfc(rfc))  # <- tu método nuevo
    if not dt:
        return None
    return dt.strftime("%Y-%m-%d %H:%M")



def _conc_to_df(conc_list) -> pd.DataFrame:
    """
    conc_list: lista de dicts con keys típicas: rfc, name, total, share, transactions

    Devuelve DF con Top10 + una fila extra "Todos los demás" (11 filas total),
    con columnas display (share/total formateadas) + columnas numéricas auxiliares:
      - _share_num (0..100)
      - _total_num  (monto)
    """
    if not conc_list:
        return pd.DataFrame(columns=["name", "rfc", "share", "total", "transactions", "_share_num", "_total_num"])

    df = pd.DataFrame(conc_list)

    # Normaliza columnas esperadas
    for c in ["rfc", "name", "total", "share", "transactions"]:
        if c not in df.columns:
            df[c] = None

    # Coerciones
    df["name"] = df["name"].astype(str).fillna("").str.strip()
    df["rfc"] = df["rfc"].astype(str).fillna("").str.strip()
    df["_total_num"] = pd.to_numeric(df["total"], errors="coerce").fillna(0.0)
    df["_share_num"] = pd.to_numeric(df["share"], errors="coerce").fillna(0.0)
    df["transactions"] = pd.to_numeric(df["transactions"], errors="coerce").fillna(0).astype(int)

    # Si share viene 0..1 lo convertimos a %
    if df["_share_num"].max() <= 1.0:
        df["_share_num"] = df["_share_num"] * 100.0

    # Orden por share desc
    df = df.sort_values("_share_num", ascending=False).reset_index(drop=True)

    # Top 10 (o menos si API manda menos)
    top = df.head(10).copy()
    rest = df.iloc[10:].copy()

    share_top = float(top["_share_num"].sum())
    share_rest = max(0.0, min(100.0, 100.0 - share_top))

    # Si la API ya trae "share" bien, pero por redondeo no llega exacto a 100, tolera un poco
    should_add_others = share_rest >= 0.01  # umbral para que sí aparezca

    # Totales:
    # - Si hay "rest" real: usamos total_all - total_top
    # - Si NO hay rest (API solo manda Top10): inferimos total_all usando share_top
    total_top = float(top["_total_num"].sum())

    if not rest.empty:
        total_all = float(df["_total_num"].sum())
        total_rest = max(0.0, total_all - total_top)
        tx_rest = int(rest["transactions"].sum())
    else:
        # inferencia: total_top representa share_top% del total
        if share_top > 0:
            total_all = total_top / (share_top / 100.0)
            total_rest = max(0.0, total_all - total_top)
        else:
            total_rest = 0.0
        tx_rest = 0  # no lo sabemos si la API no lo mandó

    # Agrega "Todos los demás" aunque NO haya filas en rest, si el top no suma 100%
    if should_add_others:
        others = pd.DataFrame(
            [{
                "name": "Todos los demás",
                "rfc": "",
                "total": None,
                "share": None,
                "transactions": tx_rest,
                "_total_num": float(total_rest),
                "_share_num": float(share_rest),
            }]
        )
        out = pd.concat([top, others], ignore_index=True)
    else:
        out = top

    # Formatos para mostrar
    out["share"] = out["_share_num"].map(lambda x: f"{x:,.2f}%")
    out["total"] = out["_total_num"].map(lambda x: _money(x))

    return out[["name", "rfc", "share", "total", "transactions", "_share_num", "_total_num"]].copy()

def _concentration_from_cfdi_headers(
    *,
    rfc: str,
    ing_headers: pd.DataFrame | None,
    egr_headers: pd.DataFrame | None,
) -> tuple[list[dict], list[dict]]:
    """
    Devuelve (conc_customers, conc_suppliers) en el MISMO formato que esperaba _conc_to_df():
      [{rfc, name, total, share, transactions}, ...]

    Reglas:
      - Clientes: CFDI tipo I emitidos (ing_headers) agrupados por receptor (cliente).
      - Proveedores: gasto neto por proveedor = (I recibidas) - (E recibidas) (egr_headers) agrupado por emisor.
    """
    r = (rfc or "").strip().upper()

    def _get_col(df: pd.DataFrame, *cands: str) -> str | None:
        cols = {str(c).strip().lower(): c for c in df.columns}
        for cand in cands:
            if cand.lower() in cols:
                return cols[cand.lower()]
        return None

    def _build_list_customers(df_in: pd.DataFrame | None) -> list[dict]:
        H = _ensure_header_cols(df_in)
        if H.empty:
            return []

        # intenta nombres (si existen)
        name_col = _get_col(df_in, "receptor_nombre", "receiver_name", "receptor_name", "nombre_receptor")
        H2 = H.copy()
        if name_col and isinstance(df_in, pd.DataFrame) and name_col in df_in.columns:
            H2["_name"] = df_in[name_col].astype(str).fillna("").str.strip()
        else:
            H2["_name"] = ""

        # ✅ Clientes netos: (I emitidas) - (E emitidas) por receptor (cliente)
        m = (H2["emisor_rfc"] == r) & (H2["tipo"].isin(["I", "E"]))
        X = H2.loc[m, ["receptor_rfc", "uuid", "total", "tipo", "_name"]].copy()
        if X.empty:
            return []

        X["_sign"] = X["tipo"].map(lambda t: 1.0 if t == "I" else (-1.0 if t == "E" else 0.0))
        X["_net"] = pd.to_numeric(X["total"], errors="coerce").fillna(0.0) * X["_sign"]

        agg = (
            X.groupby("receptor_rfc", as_index=False)
            .agg(
                name=("_name", lambda s: next((v for v in s.astype(str) if v.strip() and v.strip().lower() not in {"none", "nan"}), "")),
                total=("_net", "sum"),
                transactions=("uuid", "nunique"),
            )
            .rename(columns={"receptor_rfc": "rfc"})
        )

        agg["name"] = agg["name"].astype(str).fillna("").str.strip()
        agg["rfc"] = agg["rfc"].astype(str).fillna("").str.strip()
        agg["total"] = pd.to_numeric(agg["total"], errors="coerce").fillna(0.0)
        agg["transactions"] = pd.to_numeric(agg["transactions"], errors="coerce").fillna(0).astype(int)

        agg = agg.sort_values("total", ascending=False).reset_index(drop=True)
        total_all = float(agg["total"].sum())
        agg["share"] = (agg["total"] / total_all * 100.0) if total_all > 0 else 0.0

        out = []
        for _, row in agg.iterrows():
            out.append(
                {
                    "rfc": row["rfc"],
                    "name": row["name"] if row["name"] else row["rfc"],
                    "total": float(row["total"]),
                    "share": float(row["share"]),
                    "transactions": int(row["transactions"]),
                }
            )
        return out

    def _build_list_suppliers_net(df_in: pd.DataFrame | None) -> list[dict]:
        H = _ensure_header_cols(df_in)
        if H.empty:
            return []

        # intenta nombres (si existen)
        name_col = _get_col(df_in, "emisor_nombre", "issuer_name", "emisor_name", "nombre_emisor")
        H2 = H.copy()
        if name_col and name_col in df_in.columns:
            H2["_name"] = df_in[name_col].astype(str).fillna("").str.strip()
        else:
            H2["_name"] = ""

        # solo recibidas del RFC (receptor = RFC)
        m_base = (H2["receptor_rfc"] == r) & (H2["tipo"].isin(["I", "E"]))
        X = H2.loc[m_base, ["emisor_rfc", "uuid", "total", "tipo", "_name"]].copy()
        if X.empty:
            return []

        # neto = I - E por proveedor
        X["_sign"] = X["tipo"].map(lambda t: 1.0 if t == "I" else (-1.0 if t == "E" else 0.0))
        X["_net"] = pd.to_numeric(X["total"], errors="coerce").fillna(0.0) * X["_sign"]

        agg = (
            X.groupby("emisor_rfc", as_index=False)
            .agg(
                name=("_name", lambda s: next((v for v in s.astype(str) if v.strip() and v.strip().lower() not in {"none", "nan"}), "")),
                total=("_net", "sum"),
                transactions=("uuid", "nunique"),
            )
            .rename(columns={"emisor_rfc": "rfc"})
        )

        agg["name"] = agg["name"].astype(str).fillna("").str.strip()
        agg["rfc"] = agg["rfc"].astype(str).fillna("").str.strip()
        agg["total"] = pd.to_numeric(agg["total"], errors="coerce").fillna(0.0)
        agg["transactions"] = pd.to_numeric(agg["transactions"], errors="coerce").fillna(0).astype(int)

        # solo proveedores con gasto neto > 0 (si quieres ver negativos también, lo quitamos)
        agg = agg[agg["total"] > 0].copy()
        if agg.empty:
            return []

        agg = agg.sort_values("total", ascending=False).reset_index(drop=True)
        total_all = float(agg["total"].sum())
        if total_all > 0:
            agg["share"] = agg["total"] / total_all * 100.0
        else:
            agg["share"] = 0.0

        out = []
        for _, row in agg.iterrows():
            out.append(
                {
                    "rfc": row["rfc"],
                    "name": row["name"] if row["name"] else row["rfc"],
                    "total": float(row["total"]),
                    "share": float(row["share"]),
                    "transactions": int(row["transactions"]),
                }
            )
        return out

    conc_customers = _build_list_customers(ing_headers)
    conc_suppliers = _build_list_suppliers_net(egr_headers)
    return conc_customers, conc_suppliers



def _top10_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """DF para mostrar en tabla Top10: quita 'transactions' y columnas auxiliares."""
    if df is None or df.empty:
        return df
    drop_cols = [c for c in ["transactions", "_share_num", "_total_num"] if c in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df

def _with_color_legend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega una columna visual de color (●) para mapear filas ↔ dona.
    No modifica datos originales.
    """
    if df is None or df.empty:
        return df

    # paleta (debe coincidir con la usada en la dona)
    palette = [
        "#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
        "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC","#9E9E9E",
    ]

    d = df.copy().reset_index(drop=True)
    colors = [palette[i % len(palette)] for i in range(len(d))]

    # ● más grande + centrado
    d.insert(
        0,
        " ",
        [f"<span style='color:{c}; font-size:26px; line-height:1'>●</span>" for c in colors],
    )
    return d

def _apply_any_column_filters(df: pd.DataFrame, *, key_prefix: str) -> pd.DataFrame:
    """
    Filtro genérico por cualquier columna:
    - numéricas: rango min/max
    - fechas/datetime: rango
    - texto/categóricas: contiene + selección de valores (si son pocos)
    """
    if df is None or df.empty:
        return df

    d = df.copy()

    with st.container(border=True):
        st.markdown("#### Filtros")

        cols = list(d.columns)
        sel_cols = st.multiselect(
            "Columnas a filtrar",
            options=cols,
            default=[],
            key=f"{key_prefix}__cols",
        )

        for c in sel_cols:
            s = d[c]

            # intenta detectar datetime
            is_dt = False
            s_dt = None
            try:
                s_dt = pd.to_datetime(s, errors="coerce")
                is_dt = s_dt.notna().any() and (s_dt.notna().mean() > 0.5)
            except Exception:
                is_dt = False

            if is_dt:
                vmin = s_dt.min()
                vmax = s_dt.max()
                if pd.isna(vmin) or pd.isna(vmax):
                    st.info(f"'{c}': no hay fechas válidas para filtrar.")
                    continue

                c1, c2 = st.columns(2)
                with c1:
                    d_from = st.date_input(
                        f"{c} · Desde",
                        value=vmin.date(),
                        key=f"{key_prefix}__{c}__dt_from",
                    )
                with c2:
                    d_to = st.date_input(
                        f"{c} · Hasta",
                        value=vmax.date(),
                        key=f"{key_prefix}__{c}__dt_to",
                    )

                mask = s_dt.dt.date.between(d_from, d_to)
                d = d.loc[mask].copy()
                continue

            # numérico
            s_num = pd.to_numeric(s, errors="coerce")
            if s_num.notna().any() and (s_num.notna().mean() > 0.7):
                mn = float(s_num.min())
                mx = float(s_num.max())
                if mn == mx:
                    st.caption(f"{c}: valor único {mn}")
                    continue

                r = st.slider(
                    f"{c} · Rango",
                    min_value=mn,
                    max_value=mx,
                    value=(mn, mx),
                    key=f"{key_prefix}__{c}__num_rng",
                )
                d = d.loc[s_num.between(r[0], r[1])].copy()
                continue

            # texto/categórico
            s_txt = s.astype(str).fillna("")
            c1, c2 = st.columns([1.3, 1.7])

            with c1:
                q = st.text_input(
                    f"{c} · Contiene",
                    value="",
                    key=f"{key_prefix}__{c}__txt",
                    placeholder="Ej. 'SERVICIO', 'MX', '01010101'...",
                )

            with c2:
                uniques = sorted([u for u in s_txt.unique().tolist() if u not in {"None", "nan"}])
                use_select = len(uniques) <= 200
                selected = st.multiselect(
                    f"{c} · Valores",
                    options=uniques if use_select else [],
                    default=[],
                    key=f"{key_prefix}__{c}__vals",
                    disabled=not use_select,
                    help=None if use_select else "Demasiados valores únicos para listar.",
                )

            if q:
                d = d.loc[s_txt.str.contains(q, case=False, na=False)].copy()
                s_txt = d[c].astype(str).fillna("")

            if selected:
                d = d.loc[s_txt.isin(selected)].copy()

    return d


def _render_donut(df: pd.DataFrame, *, title: str, value_col: str = "_total_num", label_col: str = "name") -> None:
    """Gráfica de anillo (donut) usando Altair (sin matplotlib), con colores alineados a la tabla."""
    if df is None or df.empty:
        st.info("Sin datos para el periodo.")
        return
    if value_col not in df.columns or label_col not in df.columns:
        st.info("Sin columnas suficientes para graficar distribución.")
        return

    d = df[[label_col, value_col]].copy().reset_index(drop=True)
    d[label_col] = d[label_col].astype(str).fillna("")
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce").fillna(0.0)

    if float(d[value_col].sum()) <= 0:
        st.info("Sin montos positivos para graficar distribución.")
        return

    # paleta EXACTA (debe coincidir con _with_color_legend)
    palette = [
        "#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
        "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC", "#9E9E9E",
    ]

    # dominio en el MISMO orden que la tabla (orden de filas)
    domain = d[label_col].tolist()
    rng = palette[: len(domain)]

    import altair as alt

    chart = (
        alt.Chart(d)
        .mark_arc(innerRadius=55)
        .encode(
            theta=alt.Theta(field=value_col, type="quantitative"),
            color=alt.Color(
                field=label_col,
                type="nominal",
                legend=None,
                sort=domain,  # fuerza orden
                scale=alt.Scale(domain=domain, range=rng),  # fuerza color por categoría
            ),
            tooltip=[
                alt.Tooltip(label_col, type="nominal"),
                alt.Tooltip(value_col, type="quantitative", format=",.0f"),
            ],
        )
        .properties(title=title, height=260)
    )

    st.altair_chart(chart, use_container_width=True)



# =============================================================================
# Helpers 
# =============================================================================
def _or0(x) -> float:
    try:
        if x is None:
            return 0.0
        if pd.isna(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _num_s(s) -> pd.Series:
    if isinstance(s, pd.Series):
        x = s.astype(str)
    else:
        x = pd.Series(s).astype(str)
    x = x.str.replace(",", "", regex=False).str.replace("$", "", regex=False).str.strip()
    return pd.to_numeric(x, errors="coerce").fillna(0.0)


def _ensure_header_cols(headers: pd.DataFrame | None) -> pd.DataFrame:
    if headers is None or not isinstance(headers, pd.DataFrame) or headers.empty:
        return pd.DataFrame(columns=["total", "emisor_rfc", "receptor_rfc", "tipo", "uuid"])
    H = headers.copy()
    H.columns = [str(c).strip().lower() for c in H.columns]

    # Normaliza nombres básicos si vienen con variantes
    ren = {}
    if "total" not in H.columns:
        for c in H.columns:
            if c in {"importe", "monto", "total_cfdi", "totalfactura"}:
                ren[c] = "total"
                break
    if "emisor_rfc" not in H.columns:
        for c in H.columns:
            if c in {"rfc_emisor", "emisorrfc", "emisor"}:
                ren[c] = "emisor_rfc"
                break
    if "receptor_rfc" not in H.columns:
        for c in H.columns:
            if c in {"rfc_receptor", "receptorrfc", "receptor"}:
                ren[c] = "receptor_rfc"
                break
    if "tipo" not in H.columns:
        for c in H.columns:
            if c in {"tipocfdi", "tipo_cfdi"}:
                ren[c] = "tipo"
                break
    if "uuid" not in H.columns:
        for c in H.columns:
            if c in {"id", "folio_fiscal", "foliofiscal"}:
                ren[c] = "uuid"
                break
    if ren:
        H = H.rename(columns=ren)

    # Asegura columnas
    for c in ["total", "emisor_rfc", "receptor_rfc", "tipo", "uuid"]:
        if c not in H.columns:
            H[c] = None

    H["emisor_rfc"] = H["emisor_rfc"].astype(str).str.strip().str.upper()
    H["receptor_rfc"] = H["receptor_rfc"].astype(str).str.strip().str.upper()
    H["tipo"] = H["tipo"].astype(str).str.strip().str.upper()
    H["uuid"] = H["uuid"].astype(str).str.strip()
    H["total"] = _num_s(H["total"])
    return H



def _st_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """Hace un DataFrame seguro para Streamlit (evita objetos raros que rompen React)."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    d = df.copy()

    # Columnas -> strings y únicas
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

    # Valores problemáticos -> str
    def _cell(v):
        if v is None:
            return None
        try:
            if isinstance(v, (dict, list, tuple, set)):
                return str(v)
            if isinstance(v, (bytes, bytearray)):
                return v.decode(errors="ignore")
            # pandas / numpy time-like
            if isinstance(v, (pd.Timestamp, datetime, date)):
                try:
                    return v.isoformat()
                except Exception:
                    return str(v)
            return v
        except Exception:
            return str(v)

    for c in d.columns:
        if d[c].dtype == "object":
            d[c] = d[c].map(_cell)

    # NaN/inf -> None (más serializable)
    d = d.replace([pd.NA, float("inf"), float("-inf")], None)

    return d


def kpi_ingresos(headers: pd.DataFrame | None, rfc: str) -> float:
    H = _ensure_header_cols(headers)
    r = (rfc or "").strip().upper()
    base_pos = _or0(H.loc[(H["emisor_rfc"] == r) & (H["tipo"] == "I"), "total"].sum())
    base_neg = _or0(H.loc[(H["emisor_rfc"] == r) & (H["tipo"] == "E"), "total"].sum())
    # [CHANGE] Ingresos NO restan nómina (tipo N). Solo I emitidos - E emitidos.
    return base_pos - base_neg


def kpi_egresos(headers: pd.DataFrame | None, rfc: str, headers_emitidos: pd.DataFrame | None = None) -> float:
    H = _ensure_header_cols(headers)
    r = (rfc or "").strip().upper()
    base_pos = _or0(H.loc[(H["receptor_rfc"] == r) & (H["tipo"] == "I"), "total"].sum())
    base_neg = _or0(H.loc[(H["receptor_rfc"] == r) & (H["tipo"] == "E"), "total"].sum())
    # [KEEP] Egresos incluyen nómina: sumar CFDI tipo N emitidos por el RFC
    H_emit = _ensure_header_cols(headers_emitidos) if headers_emitidos is not None else H
    nomina_emit = _or0(H_emit.loc[(H_emit["emisor_rfc"] == r) & (H_emit["tipo"] == "N"), "total"].sum())
    return base_pos - base_neg + nomina_emit


def kpi_nomina(headers: pd.DataFrame | None, conceptos: pd.DataFrame | None, rfc: str) -> float:
    H = _ensure_header_cols(headers)
    r = (rfc or "").strip().upper()
    return _or0(H.loc[(H["emisor_rfc"] == r) & (H["tipo"] == "N"), "total"].sum())


def kpi_interes(headers: pd.DataFrame | None, conceptos: pd.DataFrame | None, rfc: str) -> float:
    H = _ensure_header_cols(headers)
    C = conceptos if isinstance(conceptos, pd.DataFrame) else pd.DataFrame()
    if H.empty or C.empty:
        return 0.0

    C2 = C.copy()
    C2.columns = [str(c).strip().lower() for c in C2.columns]

    # Normaliza campos mínimos (descripcion)
    desc_col = None
    for c in C2.columns:
        if c in {"descripcion", "description", "concepto", "conceptodescripcion"}:
            desc_col = c
            break
    if desc_col is None:
        desc = pd.Series([""] * len(C2))
    else:
        desc = C2[desc_col].astype(str).fillna("")

    # patrón "interes" tolerante (quita acentos)
    trans = str.maketrans({"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"})
    desc_n = desc.str.lower().str.translate(trans)
    rows_int = desc_n.str.contains("interes", regex=False)

    if not rows_int.any():
        return 0.0

    if "importe" not in C2.columns:
        C2["importe"] = 0
    C2["importe"] = _num_s(C2["importe"])

    if "uuid" not in C2.columns:
        # si no existe uuid, no hay forma de cruzar
        return 0.0

    by_uuid = (
        C2.loc[rows_int, ["uuid", "importe"]]
        .groupby("uuid", as_index=False)["importe"]
        .sum()
        .rename(columns={"importe": "monto_interes"})
    )
    if by_uuid.empty:
        return 0.0

    r = (rfc or "").strip().upper()
    H_rec = H.loc[H["receptor_rfc"] == r, ["uuid"]].copy()
    out = H_rec.merge(by_uuid, on="uuid", how="left")
    return _or0(out["monto_interes"].sum())


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)

def _month_end(d: date) -> date:
    first = date(d.year, d.month, 1)
    next_m = first + relativedelta(months=1)
    return next_m - timedelta(days=1)

def _month_label(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def _parse_month_label(s: str) -> date:
    y, m = s.split("-")
    return date(int(y), int(m), 1)

def _build_month_options(*, months_back: int = 60) -> list[str]:
    today = date.today()
    cur = date(today.year, today.month, 1)
    opts = []
    for i in range(months_back, -1, -1):
        d = cur - relativedelta(months=i)
        opts.append(_month_label(d))
    return opts


def _set_range_cb(months: int | None = None, years: int | None = None) -> None:
    today = date.today()
    end = _month_end(today)  # fin de mes actual
    if months is not None:
        start = _month_start(today - relativedelta(months=months - 1))  # incluye mes actual como 1M
        st.session_state["cfdi_date_from"] = start
        st.session_state["cfdi_date_to"] = end
    elif years is not None:
        start = _month_start(today - relativedelta(years=years) + relativedelta(months=1))
        st.session_state["cfdi_date_from"] = start
        st.session_state["cfdi_date_to"] = end

def _reset_range_cb() -> None:
    today = date.today()
    st.session_state["cfdi_date_from"] = _month_start(today - relativedelta(months=2))  # últimos ~3 meses calendario
    st.session_state["cfdi_date_to"] = _month_end(today)

def _anchor_end_from_headers(h_ing: pd.DataFrame | None, h_egr: pd.DataFrame | None) -> pd.Timestamp:
    """
    Devuelve the 'end' exclusivo (primer día del mes siguiente) basado en la fecha más reciente
    disponible en los headers (emitidos/recibidos). Si no hay fechas válidas, cae a hoy.
    """
    dts = []

    for h in (h_ing, h_egr):
        if h is None or not isinstance(h, pd.DataFrame) or h.empty:
            continue
        if "fecha" in h.columns:
            s = pd.to_datetime(h["fecha"], errors="coerce")
            if s.notna().any():
                dts.append(s.max())

    max_dt = max(dts) if dts else pd.Timestamp(date.today())
    # ancla al inicio del mes siguiente (end exclusivo)
    month_start = pd.Timestamp(year=max_dt.year, month=max_dt.month, day=1)
    anchor_end = month_start + relativedelta(months=1)
    return anchor_end

def _get_cfdi_for_yoy_24m(*, rfc: str) -> dict:
    """
    Siempre trae (o asegura en cache-base) los últimos 24 meses calendario completos
    para calcular YoY 12M vs 12M previos, INDEPENDIENTE del rango seleccionado.
    """
    local_dir = str(ROOT / "data" / "cfdi_xml")

    # 24 meses completos: desde el primer día del mes hace 23 meses, hasta fin del mes actual
    end_month = _month_end(date.today())
    start_24m = _month_start(date.today() - relativedelta(months=23))

    return _ensure_cfdi_from_base_or_fetch(
        rfc=rfc,
        cfdi_source="syntage",
        date_from=start_24m,
        date_to=end_month,
        local_dir=local_dir,
    )



# =============================================================================
# Ventas / Gastos / Utilidad Fiscal (basado en KPIs anteriores)
# =============================================================================
def _pick_date_col(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    cols = [str(c).strip().lower() for c in df.columns]
    candidates = [
        "fecha",
        "fecha_emision",
        "fechaemision",
        "fecha_timbrado",
        "fechatimbrado",
        "fecha_cfdi",
        "fechacfdi",
        "fch_emision",
        "fch_timbrado",
        "date",
        "issued_at",
        "created_at",
        "timestamp",
    ]
    for cand in candidates:
        if cand in cols:
            return df.columns[cols.index(cand)]
    for c in df.columns:
        name = str(c).strip().lower()
        if "fecha" in name or "date" in name or "timbr" in name or "emisi" in name:
            return c
    return None


def _with_dt(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=["dt"])
    dcol = _pick_date_col(df)
    out = df.copy()
    if dcol is None:
        out["dt"] = pd.NaT
        return out
    out["dt"] = pd.to_datetime(out[dcol], errors="coerce")
    return out

def build_clientes_proveedores_tables(
    *,
    rfc: str,
    ing_headers: pd.DataFrame | None,
    egr_headers: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Tablas desde CFDI (headers):
      - Clientes: emitidas (ing_headers) agrupado por receptor_rfc
      - Proveedores: recibidas (egr_headers) agrupado por emisor_rfc

    Requiere que headers ya traiga:
      emisor_rfc, receptor_rfc, fecha, uuid, subtotal, total
    y para nombre:
      emisor_nombre, receptor_nombre   (los agregamos en el parser)
    """

    r = (rfc or "").strip().upper()

    def _first_non_empty_name(s: pd.Series) -> str:
        try:
            for v in s.astype(str).fillna("").tolist():
                v2 = v.strip()
                if v2 and v2.lower() not in {"none", "nan"}:
                    return v2
        except Exception:
            pass
        return ""

    def _prep(df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame(
                columns=[
                    "uuid", "fecha", "subtotal", "total", "emisor_rfc", "receptor_rfc",
                    "emisor_nombre", "receptor_nombre",
                ]
            )

        d = df.copy()

        # Asegura columnas (sin romper si faltan)
        for c in [
            "uuid", "fecha", "subtotal", "total", "emisor_rfc", "receptor_rfc",
            "emisor_nombre", "receptor_nombre",
        ]:
            if c not in d.columns:
                d[c] = None

        d["fecha"] = pd.to_datetime(d["fecha"], errors="coerce")
        d["subtotal"] = pd.to_numeric(d["subtotal"], errors="coerce").fillna(0.0)
        d["total"] = pd.to_numeric(d["total"], errors="coerce").fillna(0.0)

        d["emisor_rfc"] = d["emisor_rfc"].astype(str).str.strip().str.upper()
        d["receptor_rfc"] = d["receptor_rfc"].astype(str).str.strip().str.upper()
        d["uuid"] = d["uuid"].astype(str).str.strip()

        d["emisor_nombre"] = d["emisor_nombre"].astype(str).fillna("").str.strip()
        d["receptor_nombre"] = d["receptor_nombre"].astype(str).fillna("").str.strip()

        return d

    ing = _prep(ing_headers)
    egr = _prep(egr_headers)

    # ── CLIENTES (emitidas): receptor_rfc ─────────────────────────────────────
    ing2 = ing.loc[ing["emisor_rfc"] == r].copy() if not ing.empty else ing

    clientes = (
        ing2.groupby("receptor_rfc", as_index=False)
        .agg(
            Cliente=("receptor_nombre", _first_non_empty_name),
            **{
                "# CFDI": ("uuid", "nunique"),
                "Subtotal": ("subtotal", "sum"),
                "Total": ("total", "sum"),
                "Primera fecha": ("fecha", "min"),
                "Última fecha": ("fecha", "max"),
            }
        )
        .rename(columns={"receptor_rfc": "RFC"})
    )

    # ── PROVEEDORES (recibidas): emisor_rfc ───────────────────────────────────
    egr2 = egr.loc[egr["receptor_rfc"] == r].copy() if not egr.empty else egr

    proveedores = (
        egr2.groupby("emisor_rfc", as_index=False)
        .agg(
            Proveedor=("emisor_nombre", _first_non_empty_name),
            **{
                "# CFDI": ("uuid", "nunique"),
                "Subtotal": ("subtotal", "sum"),
                "Total": ("total", "sum"),
                "Primera fecha": ("fecha", "min"),
                "Última fecha": ("fecha", "max"),
            }
        )
        .rename(columns={"emisor_rfc": "RFC"})
    )

    # Formato final
    for df in (clientes, proveedores):
        if df is not None and not df.empty:
            df["Primera fecha"] = pd.to_datetime(df["Primera fecha"], errors="coerce").dt.date
            df["Última fecha"] = pd.to_datetime(df["Última fecha"], errors="coerce").dt.date
            df["Subtotal"] = df["Subtotal"].round(2)
            df["Total"] = df["Total"].round(2)
            df.sort_values(["Total"], ascending=False, inplace=True, ignore_index=True)

    return clientes, proveedores



def build_clientes_net_table(
    *,
    rfc: str,
    ing_headers: pd.DataFrame | None,
) -> pd.DataFrame:
    """Clientes (desde CFDI XML) con ingreso NETO (I emitidas - E emitidas) por cliente."""
    r = (rfc or "").strip().upper()
    H = _ensure_header_cols(ing_headers)

    if H.empty:
        return pd.DataFrame(
            columns=[
                "RFC",
                "Cliente",
                "# CFDI",
                "Facturas (I)",
                "Notas crédito (E)",
                "Emitido Neto",
                "Primera fecha",
                "Última fecha",
            ]
        )

    # fecha desde DF original si existe
    if isinstance(ing_headers, pd.DataFrame) and "fecha" in ing_headers.columns:
        dt = pd.to_datetime(ing_headers["fecha"], errors="coerce")
    else:
        dt = pd.to_datetime(pd.Series([pd.NaT] * len(H)), errors="coerce")

    X = H.copy()
    X["_fecha"] = dt
    X = X.loc[(X["emisor_rfc"] == r) & (X["tipo"].isin(["I", "E"]))].copy()
    if X.empty:
        return pd.DataFrame(
            columns=[
                "RFC",
                "Cliente",
                "# CFDI",
                "Facturas (I)",
                "Notas crédito (E)",
                "Emitido Neto",
                "Primera fecha",
                "Última fecha",
            ]
        )

    # nombre de cliente si viene en headers original
    cli_name = None
    if isinstance(ing_headers, pd.DataFrame) and not ing_headers.empty:
        cols = {str(c).strip().lower(): c for c in ing_headers.columns}
        for cand in ["receptor_nombre", "receiver_name", "receptor_name", "nombre_receptor"]:
            if cand in cols:
                cli_name = cols[cand]
                break
    if cli_name and isinstance(ing_headers, pd.DataFrame) and cli_name in ing_headers.columns:
        X["_cli_name"] = ing_headers[cli_name].astype(str).fillna("").str.strip()
    else:
        X["_cli_name"] = ""

    X["_total"] = pd.to_numeric(X["total"], errors="coerce").fillna(0.0).round(2)
    X["_is_i"] = (X["tipo"] == "I")
    X["_is_e"] = (X["tipo"] == "E")

    def _sum_masked(s: pd.Series, mask: pd.Series) -> float:
        try:
            return float(s.loc[mask.loc[s.index]].sum())
        except Exception:
            return 0.0

    agg = (
        X.groupby("receptor_rfc", as_index=False)
        .agg(
            Cliente=("_cli_name", lambda s: next((v for v in s.astype(str) if v.strip() and v.strip().lower() not in {"none", "nan"}), "")),
            **{
                "# CFDI": ("uuid", "nunique"),
                "Facturas (I)": ("_total", lambda s: _sum_masked(s, X["_is_i"])),
                "Notas crédito (E)": ("_total", lambda s: _sum_masked(s, X["_is_e"])),
                "Primera fecha": ("_fecha", "min"),
                "Última fecha": ("_fecha", "max"),
            },
        )
        .rename(columns={"receptor_rfc": "RFC"})
    )

    agg["Emitido Neto"] = (
        pd.to_numeric(agg["Facturas (I)"], errors="coerce").fillna(0.0)
        - pd.to_numeric(agg["Notas crédito (E)"], errors="coerce").fillna(0.0)
    ).round(2)

    agg["Cliente"] = agg.apply(
        lambda row: row["Cliente"] if str(row["Cliente"]).strip() else str(row["RFC"]).strip(),
        axis=1,
    )

    agg["Primera fecha"] = pd.to_datetime(agg["Primera fecha"], errors="coerce").dt.date
    agg["Última fecha"] = pd.to_datetime(agg["Última fecha"], errors="coerce").dt.date
    agg = agg.sort_values("Emitido Neto", ascending=False).reset_index(drop=True)

    return agg[
        [
            "RFC",
            "Cliente",
            "# CFDI",
            "Facturas (I)",
            "Notas crédito (E)",
            "Emitido Neto",
            "Primera fecha",
            "Última fecha",
        ]
    ].copy()


def build_proveedores_net_table(
    *,
    rfc: str,
    egr_headers: pd.DataFrame | None,
) -> pd.DataFrame:
    """Proveedores (desde CFDI XML) with gasto NETO (I recibidas - E recibidas) por proveedor."""
    r = (rfc or "").strip().upper()
    H = _ensure_header_cols(egr_headers)

    if H.empty:
        return pd.DataFrame(
            columns=[
                "RFC",
                "Proveedor",
                " # CFDI",
                "Compras (I)",
                "Notas crédito (E)",
                "Recibido Neto",
                "Primera fecha",
                "Última fecha",
            ]
        )

    if isinstance(egr_headers, pd.DataFrame) and "fecha" in egr_headers.columns:
        dt = pd.to_datetime(egr_headers["fecha"], errors="coerce")
    else:
        dt = pd.to_datetime(pd.Series([pd.NaT] * len(H)), errors="coerce")

    X = H.copy()
    X["_fecha"] = dt
    X = X.loc[(X["receptor_rfc"] == r) & (X["tipo"].isin(["I", "E"]))].copy()
    if X.empty:
        return pd.DataFrame(
            columns=[
                "RFC",
                "Proveedor",
                "# CFDI",
                "Compras (I)",
                "Notas crédito (E)",
                "Recibido Neto",
                "Primera fecha",
                "Última fecha",
            ]
        )

    prov_name = None
    if isinstance(egr_headers, pd.DataFrame) and not egr_headers.empty:
        cols = {str(c).strip().lower(): c for c in egr_headers.columns}
        for cand in ["emisor_nombre", "supplier_name", "emisor_name", "nombre_emisor"]:
            if cand in cols:
                prov_name = cols[cand]
                break
    if prov_name and isinstance(egr_headers, pd.DataFrame) and prov_name in egr_headers.columns:
        X["_prov_name"] = egr_headers[prov_name].astype(str).fillna("").str.strip()
    else:
        X["_prov_name"] = ""

    X["_total"] = pd.to_numeric(X["total"], errors="coerce").fillna(0.0).round(2)
    X["_is_i"] = (X["tipo"] == "I")
    X["_is_e"] = (X["tipo"] == "E")

    def _sum_masked(s: pd.Series, mask: pd.Series) -> float:
        try:
            return float(s.loc[mask.loc[s.index]].sum())
        except Exception:
            return 0.0

    agg = (
        X.groupby("emisor_rfc", as_index=False)
        .agg(
            Proveedor=("_prov_name", lambda s: next((v for v in s.astype(str) if v.strip() and v.strip().lower() not in {"none", "nan"}), "")),
            **{
                "# CFDI": ("uuid", "nunique"),
                "Compras (I)": ("_total", lambda s: _sum_masked(s, X["_is_i"])),
                "Notas crédito (E)": ("_total", lambda s: _sum_masked(s, X["_is_e"])),
                "Primera fecha": ("_fecha", "min"),
                "Última fecha": ("_fecha", "max"),
            },
        )
        .rename(columns={"emisor_rfc": "RFC"})
    )

    agg["Recibido Neto"] = (
        pd.to_numeric(agg["Compras (I)"], errors="coerce").fillna(0.0)
        - pd.to_numeric(agg["Notas crédito (E)"], errors="coerce").fillna(0.0)
    ).round(2)

    agg["Proveedor"] = agg.apply(
        lambda row: row["Proveedor"] if str(row["Proveedor"]).strip() else str(row["RFC"]).strip(),
        axis=1,
    )

    agg["Primera fecha"] = pd.to_datetime(agg["Primera fecha"], errors="coerce").dt.date
    agg["Última fecha"] = pd.to_datetime(agg["Última fecha"], errors="coerce").dt.date
    agg = agg.sort_values("Recibido Neto", ascending=False).reset_index(drop=True)

    return agg[
        [
            "RFC",
            "Proveedor",
            "# CFDI",
            "Compras (I)",
            "Notas crédito (E)",
            "Recibido Neto",
            "Primera fecha",
            "Última fecha",
        ]
    ].copy()


def _slice_headers_by_date(headers: pd.DataFrame | None, d_from: date, d_to: date) -> pd.DataFrame:
    if headers is None or not isinstance(headers, pd.DataFrame) or headers.empty:
        return pd.DataFrame()

    h = headers.copy()
    # tu header trae 'fecha'
    h["_dt"] = pd.to_datetime(h["fecha"], errors="coerce")
    start = pd.Timestamp(d_from)
    end = pd.Timestamp(d_to) + pd.Timedelta(days=1)  # inclusivo
    out = h[(h["_dt"] >= start) & (h["_dt"] < end)].drop(columns=["_dt"])
    return out.reset_index(drop=True)


def _slice_conceptos_by_uuid(conceptos: pd.DataFrame | None, uuids: pd.Series) -> pd.DataFrame:
    if conceptos is None or not isinstance(conceptos, pd.DataFrame) or conceptos.empty:
        return pd.DataFrame()
    c = conceptos.copy()
    return c[c["uuid"].isin(set(uuids.astype(str)))].reset_index(drop=True)


def _slice_cfdi_data(cfdi_base: dict, d_from: date, d_to: date) -> dict:
    """Recorta cfdi_data base al rango solicitado (sin re-descargar)."""
    ing0 = cfdi_base.get("ingresos")
    egr0 = cfdi_base.get("egresos")

    ing_h = _slice_headers_by_date(getattr(ing0, "headers", None), d_from, d_to) if ing0 is not None else pd.DataFrame()
    egr_h = _slice_headers_by_date(getattr(egr0, "headers", None), d_from, d_to) if egr0 is not None else pd.DataFrame()

    ing_c = _slice_conceptos_by_uuid(getattr(ing0, "conceptos", None), ing_h["uuid"]) if ing0 is not None else pd.DataFrame()
    egr_c = _slice_conceptos_by_uuid(getattr(egr0, "conceptos", None), egr_h["uuid"]) if egr0 is not None else pd.DataFrame()

    out = {}
    out["meta"] = cfdi_base.get("meta") or {}

    out["ingresos"] = SimpleNamespace(headers=ing_h, conceptos=ing_c)
    out["egresos"] = SimpleNamespace(headers=egr_h, conceptos=egr_c)

    out["clientes_df"] = cfdi_base.get("clientes_df", pd.DataFrame())
    out["proveedores_df"] = cfdi_base.get("proveedores_df", pd.DataFrame())

    out["emit_invoices_df"] = cfdi_base.get("emit_invoices_df", pd.DataFrame())
    out["rec_invoices_df"] = cfdi_base.get("rec_invoices_df", pd.DataFrame())

    return out


def _params_key_for_base(rfc: str, cfdi_source: str, local_dir: str) -> tuple:
    return ((rfc or "").strip().upper(), cfdi_source, str(local_dir))


def _ensure_cfdi_from_base_or_fetch(*, rfc: str, cfdi_source: str, date_from: date, date_to: date, local_dir: str) -> dict:
    """
    Base inteligente:
    - Si el rango pedido está dentro de la base -> recorta (sin descargar).
    - Si el rango pedido se sale -> descarga la UNIÓN (min(from), max(to)) y guarda como nueva base.
      (La base nunca se hace más chica.)
    """
    base_key = _params_key_for_base(rfc, cfdi_source, local_dir)

    base = st.session_state.get("cfdi_data_base")
    base_meta = st.session_state.get("cfdi_data_base_meta")  # (key, from, to)

    # Caso: ya hay base para el mismo RFC/source/local_dir
    if base is not None and base_meta is not None:
        saved_key, saved_from, saved_to = base_meta
        if saved_key == base_key:
            # Si el rango está contenido, solo recorta
            if saved_from <= date_from and date_to <= saved_to:
                st.session_state["_cfdi_last_action"] = "slice"
                return _slice_cfdi_data(base, date_from, date_to)

            # Si NO está contenido, expandimos: descargamos la UNIÓN
            new_from = min(saved_from, date_from)
            new_to = max(saved_to, date_to)

            cfdi_new_base = fetch_cfdi(
                rfc=rfc,
                source=cfdi_source,
                date_from=new_from,
                date_to=new_to,
                local_dir=local_dir,
            )
            st.session_state["cfdi_data_base"] = cfdi_new_base
            st.session_state["cfdi_data_base_meta"] = (base_key, new_from, new_to)
            st.session_state["_cfdi_last_action"] = "fetch_union"

            return _slice_cfdi_data(cfdi_new_base, date_from, date_to)

    # Caso: no hay base (o cambió RFC/source/local_dir) -> descarga exacto y guarda base
    cfdi_new_base = fetch_cfdi(
        rfc=rfc,
        source=cfdi_source,
        date_from=date_from,
        date_to=date_to,
        local_dir=local_dir,
    )
    st.session_state["cfdi_data_base"] = cfdi_new_base
    st.session_state["cfdi_data_base_meta"] = (base_key, date_from, date_to)
    st.session_state["_cfdi_last_action"] = "fetch_exact"

    return _slice_cfdi_data(cfdi_new_base, date_from, date_to)




def _period_sum_ingresos(headers_emit: pd.DataFrame | None, rfc: str, start: pd.Timestamp, end: pd.Timestamp) -> float:
    H0 = _with_dt(headers_emit)
    H = _ensure_header_cols(H0)
    H["dt"] = H0.get("dt", pd.NaT)
    r = (rfc or "").strip().upper()
    m = (H["dt"] >= start) & (H["dt"] < end)
    base_pos = _or0(H.loc[m & (H["emisor_rfc"] == r) & (H["tipo"] == "I"), "total"].sum())
    base_neg = _or0(H.loc[m & (H["emisor_rfc"] == r) & (H["tipo"] == "E"), "total"].sum())
    return base_pos - base_neg


def _period_sum_egresos(headers_rec: pd.DataFrame | None, headers_emit: pd.DataFrame | None, rfc: str, start: pd.Timestamp, end: pd.Timestamp) -> float:
    Hr0 = _with_dt(headers_rec)
    Hr = _ensure_header_cols(Hr0)
    Hr["dt"] = Hr0.get("dt", pd.NaT)

    He0 = _with_dt(headers_emit)
    He = _ensure_header_cols(He0)
    He["dt"] = He0.get("dt", pd.NaT)

    r = (rfc or "").strip().upper()

    mrec = (Hr["dt"] >= start) & (Hr["dt"] < end)
    memi = (He["dt"] >= start) & (He["dt"] < end)

    base_pos = _or0(Hr.loc[mrec & (Hr["receptor_rfc"] == r) & (Hr["tipo"] == "I"), "total"].sum())
    base_neg = _or0(Hr.loc[mrec & (Hr["receptor_rfc"] == r) & (Hr["tipo"] == "E"), "total"].sum())
    nomina_emit = _or0(He.loc[memi & (He["emisor_rfc"] == r) & (He["tipo"] == "N"), "total"].sum())
    return base_pos - base_neg + nomina_emit


def _money(x: float | int | None) -> str:
    """Formato moneda SIN centavos (UI compacta).
    Ej: 1234.56 -> $1,235
    """
    try:
        if x is None:
            return "$0"
        v = float(x)
        return f"${v:,.0f}"
    except Exception:
        return "$0"


def _money_cents(x: float | int | None) -> str:
    """Formato moneda CON centavos (para conciliación al centavo)."""
    try:
        if x is None:
            return "$0.00"
        v = float(x)
        return f"${v:,.2f}"
    except Exception:
        return "$0.00"


def _set_range(months: int | None = None, years: int | None = None) -> None:
    today = date.today()
    if months is not None:
        st.session_state["cfdi_date_from_value"] = today - relativedelta(months=months)
        st.session_state["cfdi_date_to_value"] = today
    elif years is not None:
        st.session_state["cfdi_date_from_value"] = today - relativedelta(years=years)
        st.session_state["cfdi_date_to_value"] = today


def render_filterable_grid(df: pd.DataFrame, *, key: str) -> None:
    """Render de tabla *ultra-estable* (evita errores React).

    Nota: desactivamos AG Grid porque es un componente React externo que, en Streamlit,
    puede detonar el error minified React #185 en ciertos entornos/reruns.
    """
    if df is None or getattr(df, "empty", True):
        st.info("Sin datos para el periodo.")
        return

    d = df.copy()
    # Asegura nombres de columnas únicos (Streamlit/React se rompe con duplicados)
    cols = [str(c) for c in d.columns]
    seen = {}
    new_cols = []
    for c in cols:
        k = c
        if k in seen:
            seen[k] += 1
            k = f"{k} ({seen[c]})"
        else:
            seen[k] = 0
        new_cols.append(k)
    d.columns = new_cols

    # Normaliza NaN/inf para el frontend
    d = d.replace([np.inf, -np.inf], np.nan)

    # Render simple (sin key/column_config)
    st.dataframe(d, use_container_width=True, hide_index=True)


# =============================================================================
# TOPBAR
# =============================================================================
st.markdown('<div id="topbar">', unsafe_allow_html=True)

# Inicializa fechas SOLO si no existen (en modo meses completos)
if "cfdi_date_from" not in st.session_state:
    st.session_state["cfdi_date_from"] = _month_start(date.today() - relativedelta(months=2))
if "cfdi_date_to" not in st.session_state:
    st.session_state["cfdi_date_to"] = _month_end(date.today())

with st.container():
    # ---- defaults de fechas (mes completo) ----
    default_from = st.session_state.get("cfdi_date_from") or _month_start(date.today() - relativedelta(months=2))
    default_to = st.session_state.get("cfdi_date_to") or _month_end(date.today())

    # =============================================================================
    # FILA ÚNICA: Logo + RFC + Calcular/Cancelar + Desde/Hasta (MES) + botones de rango
    # =============================================================================
    st.markdown('<span data-uw="inputbar-marker"></span>', unsafe_allow_html=True)

    components.html(
        """
    <script>
    (function () {
    const doc = window.parent.document;

    function apply() {
        const marker = doc.querySelector('[data-uw="inputbar-marker"]');
        if (!marker) return false;

        // Sube hasta un stBlock (más estable)
        let el = marker;
        for (let i = 0; i < 80; i++) {
        if (!el) break;
        if (el.getAttribute && el.getAttribute("data-testid") === "stBlock") break;
        el = el.parentElement;
        }
        if (!el) return false;

        el.classList.add("uw-topbar");
        return true;
    }

    if (apply()) return;

    let tries = 0;
    const t = setInterval(() => {
        tries += 1;
        if (apply() || tries > 80) clearInterval(t);
    }, 100);
    })();
    </script>
    """,
        height=0,
    )

    # ⬇️ Layout: [Logo | RFC | Calcular | Cancelar | Desde | Hasta | 1M..Reset]
    row = st.columns(
        [2.2, 2.6, 1.8, 1.8, 2.1, 2.1, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.95]
    )

    # 0) Logo
    with row[0]:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=135)  # ajusta 120–150 según te guste
        else:
            st.caption("Logo no encontrado")

    # 1) RFC (aquí se define rfc_valid para 12/13)
    with row[1]:
        rfc_raw = st.text_input("RFC", placeholder="PEIC211118IS0", key="inp_rfc")
        rfc = _clean_rfc(rfc_raw)

    rfc_valid = 12 <= len(rfc) <= 13  # ✅ PM(12) o PF(13)

    # 2) Calcular
    with row[2]:
        run = st.button(
            "Calcular",
            type="primary",
            use_container_width=True,
            disabled=not rfc_valid,
            key="btn_run",
        )

    # 3) Cancelar
    with row[3]:
        clear = st.button("Cancelar", use_container_width=True, key="btn_clear")

    # ====== Selector por mes completo ======
    month_opts = _build_month_options(months_back=84)  # 7 años hacia atrás

    default_from = st.session_state.get("cfdi_date_from") or _month_start(date.today() - relativedelta(months=2))
    default_to = st.session_state.get("cfdi_date_to") or _month_end(date.today())

    from_label = _month_label(default_from)
    to_label = _month_label(default_to)

    try:
        from_idx = month_opts.index(from_label)
    except ValueError:
        from_idx = max(0, len(month_opts) - 13)

    try:
        to_idx = month_opts.index(to_label)
    except ValueError:
        to_idx = len(month_opts) - 1

    with row[4]:
        sel_from = st.selectbox("Desde (mes)", options=month_opts, index=from_idx, key="cfdi_month_from")

    with row[5]:
        sel_to = st.selectbox("Hasta (mes)", options=month_opts, index=to_idx, key="cfdi_month_to")

    # Normaliza: asegura from <= to
    d_from = _parse_month_label(sel_from)
    d_to = _parse_month_label(sel_to)
    if d_from > d_to:
        d_from = d_to
        st.session_state["cfdi_month_from"] = _month_label(d_from)

    st.session_state["cfdi_date_from"] = _month_start(d_from)
    st.session_state["cfdi_date_to"] = _month_end(d_to)

    # ✅ Botones de rango usando callbacks (mes completo)
    row[6].button("1M", use_container_width=True, key="rng_1m", on_click=_set_range_cb, kwargs={"months": 1})
    row[7].button("3M", use_container_width=True, key="rng_3m", on_click=_set_range_cb, kwargs={"months": 3})
    row[8].button("6M", use_container_width=True, key="rng_6m", on_click=_set_range_cb, kwargs={"months": 6})
    row[9].button("1A", use_container_width=True, key="rng_1y", on_click=_set_range_cb, kwargs={"years": 1})
    row[10].button("2A", use_container_width=True, key="rng_2y", on_click=_set_range_cb, kwargs={"years": 2})
    row[11].button("3A", use_container_width=True, key="rng_3y", on_click=_set_range_cb, kwargs={"years": 3})
    row[12].button("⟳", use_container_width=True, key="rng_reset", on_click=_reset_range_cb)

    if clear:
        st.session_state.pop("tax_status", None)
        st.session_state.pop("last_rfc", None)
        for k in ["cfdi_data", "cfdi_date_from", "cfdi_date_to"]:
            st.session_state.pop(k, None)
        st.rerun()

# Fuente CFDI fija (Syntage)
cfdi_source = "syntage"

st.markdown("</div>", unsafe_allow_html=True)
st.markdown('<div class="uw-topbar-spacer"></div>', unsafe_allow_html=True)



# =============================================================================
# Tabs
# =============================================================================
tabs = st.tabs(["SAT", "Buró"])

if run:
    if not rfc_valid:
        st.warning("RFC inválido. Debe tener 12 o 13 caracteres.")
    else:
        with st.spinner("Consultando SAT y CFDI..."):
            try:
                st.session_state["tax_status"] = fetch_tax_status(rfc)
                st.session_state["last_rfc"] = rfc
            except Exception as e:
                st.error(f"Error consultando SAT en Syntage: {e}")

            try:
                local_dir = st.session_state.get("cfdi_local_dir", str(ROOT / "data" / "cfdi_xml"))

                # ✅ Cambio: usa base-cache + recorte (en vez de re-descargar todo siempre)
                st.session_state["cfdi_data"] = _ensure_cfdi_from_base_or_fetch(
                    rfc=rfc,
                    cfdi_source=cfdi_source,
                    date_from=st.session_state.get("cfdi_date_from"),
                    date_to=st.session_state.get("cfdi_date_to"),
                    local_dir=local_dir,
                )

                st.session_state["_last_cfdi_params"] = (
                    "syntage",
                    st.session_state.get("cfdi_date_from"),
                    st.session_state.get("cfdi_date_to"),
                )

            except Exception as e:
                st.error(f"Error consultando CFDIs: {e}")


# =============================================================================
# TAB 0: SAT
# =============================================================================
with tabs[0]:
    # ✅ PDF Download Button at the top
    render_pdf_download_button()

    st.caption("SAT →  Tax Status → Actividades económicas y regímenes")

    # Fecha de última extracción (Syntage)
    _rfc_for_status = st.session_state.get("last_rfc") or (rfc if rfc_valid else None)

    _last_ext = None
    if _rfc_for_status:
        _last_ext = fetch_last_extraction_at(_rfc_for_status, extractor="invoice")

    if _last_ext:
        st.info(f"📦 Fecha de última extracción: {_last_ext} (hora CDMX)")
    else:
        st.caption("📦 Fecha de última extracción: no disponible")


    tax_status = st.session_state.get("tax_status")
    if tax_status:
        render_tax_status_cards(tax_status)

        cfdi_data = st.session_state.get("cfdi_data") or {}
        meta = cfdi_data.get("meta") or {}
        if meta:
            st.caption(
                f"CFDI meta: emit listed={meta.get('emit_listed')} downloaded={meta.get('emit_downloaded')} "
                f"failed={meta.get('emit_failed')} | rec listed={meta.get('rec_listed')} "
                f"downloaded={meta.get('rec_downloaded')} failed={meta.get('rec_failed')} "
                f"(workers={meta.get('max_workers')})"
            )

        if not cfdi_data:
            st.info("Para ver Prod/Serv, selecciona fuente/rango arriba y presiona Calcular.")
        else:
            catalogo = load_ps_catalog()
            service = get_cfdi_service()

            ing = cfdi_data.get("ingresos")
            egr = cfdi_data.get("egresos")

            resumen_ing = (
                service.prodserv_summary_shiny(
                    rfc=rfc,
                    headers=ing.headers,
                    conceptos=ing.conceptos,
                    catalogo=catalogo,
                    tipo="I",
                    rol="emisor",
                    top_n=25,
                )
                if ing is not None and ing.conceptos is not None and ing.headers is not None
                else pd.DataFrame()
            )

            resumen_egr = (
                service.prodserv_summary_shiny(
                    rfc=rfc,
                    headers=egr.headers,
                    conceptos=egr.conceptos,
                    catalogo=catalogo,
                    tipo="I",
                    rol="receptor",
                    top_n=25,
                )
                if egr is not None and egr.conceptos is not None and egr.headers is not None
                else pd.DataFrame()
            )

            render_prodserv_dual_cards(
                title_left="Ingresos facturados",
                df_left=resumen_ing,
                title_right="Egresos facturados",
                df_right=resumen_egr,
            )

            # =============================================================================
            # KPIs debajo de las cards 
            # =============================================================================
            h_ing = ing.headers if ing is not None else None
            c_ing = ing.conceptos if ing is not None else None
            h_egr = egr.headers if egr is not None else None
            c_egr = egr.conceptos if egr is not None else None

            k_ing = kpi_ingresos(h_ing, rfc)
            k_nom = kpi_nomina(h_ing, c_ing, rfc)
            k_egr = kpi_egresos(h_egr, rfc, headers_emitidos=h_ing)
            k_int = kpi_interes(h_egr, c_egr, rfc)

            # =============================================================================
            # Ventas / Gastos / Utilidad Fiscal (tabla + gráfico)
            # =============================================================================
            today = pd.Timestamp(st.session_state.get("cfdi_date_to") or date.today())
            y2 = today.year
            y1 = y2 - 1
            y0 = y2 - 2

            def _year_window(y: int) -> tuple[pd.Timestamp, pd.Timestamp]:
                return (pd.Timestamp(year=y, month=1, day=1), pd.Timestamp(year=y + 1, month=1, day=1))

            # últimos 12 meses (por mes calendario, 12 puntos)
            start_12m = pd.Timestamp(year=today.year, month=today.month, day=1) - relativedelta(months=11)
            end_next_month = (pd.Timestamp(year=today.year, month=today.month, day=1) + relativedelta(months=1))

            ventas_y0 = _period_sum_ingresos(h_ing, rfc, *_year_window(y0))
            ventas_y1 = _period_sum_ingresos(h_ing, rfc, *_year_window(y1))
            ventas_y2 = _period_sum_ingresos(h_ing, rfc, *_year_window(y2))
            ventas_12m = _period_sum_ingresos(h_ing, rfc, start_12m, end_next_month)
            avg_ventas_12m = ventas_12m / 12.0



            gastos_y0 = _period_sum_egresos(h_egr, h_ing, rfc, *_year_window(y0))
            gastos_y1 = _period_sum_egresos(h_egr, h_ing, rfc, *_year_window(y1))
            gastos_y2 = _period_sum_egresos(h_egr, h_ing, rfc, *_year_window(y2))
            gastos_12m = _period_sum_egresos(h_egr, h_ing, rfc, start_12m, end_next_month)
            avg_gastos_12m = gastos_12m / 12.0


            util_y0 = ventas_y0 - gastos_y0
            util_y1 = ventas_y1 - gastos_y1
            util_y2 = ventas_y2 - gastos_y2
            util_12m = ventas_12m - gastos_12m
            avg_util_12m   = util_12m   / 12.0

            # --- YoY pesado: hacerlo bajo demanda ---
            calc_yoy = st.checkbox(
                "Calcular YoY (últimos 12M vs 12M previos) [tarda más]",
                value=st.session_state.get("_calc_yoy", False),
                key="_calc_yoy",
            )


            # ================================
            # Crec. vs 12M previos (YoY 12M) - INDEPENDIENTE del rango seleccionado
            # ================================
            # valores por default (si no se calcula YoY)
            ventas_12m_yoy = gastos_12m_yoy = util_12m_yoy = 0.0
            ventas_prev_12m = gastos_prev_12m = util_prev_12m = 0.0
            g_ventas_12m = g_gastos_12m = g_util_12m = None

            if calc_yoy:
                rfc_for_yoy = st.session_state.get("last_rfc") or rfc

                with st.spinner("Calculando YoY (24 meses de CFDI)…"):
                    cfdi_yoy = _get_cfdi_for_yoy_24m(rfc=rfc_for_yoy)

                    ing_yoy = cfdi_yoy.get("ingresos")
                    egr_yoy = cfdi_yoy.get("egresos")

                    h_ing_yoy = getattr(ing_yoy, "headers", None) if ing_yoy is not None else None
                    h_egr_yoy = getattr(egr_yoy, "headers", None) if egr_yoy is not None else None

                    anchor_end = pd.Timestamp(year=date.today().year, month=date.today().month, day=1) + relativedelta(months=1)
                    start_12m_yoy = anchor_end - relativedelta(months=12)
                    start_prev_12m = anchor_end - relativedelta(months=24)

                    ventas_12m_yoy = _period_sum_ingresos(h_ing_yoy, rfc_for_yoy, start_12m_yoy, anchor_end)
                    gastos_12m_yoy = _period_sum_egresos(h_egr_yoy, h_ing_yoy, rfc_for_yoy, start_12m_yoy, anchor_end)
                    util_12m_yoy = ventas_12m_yoy - gastos_12m_yoy

                    ventas_prev_12m = _period_sum_ingresos(h_ing_yoy, rfc_for_yoy, start_prev_12m, start_12m_yoy)
                    gastos_prev_12m = _period_sum_egresos(h_egr_yoy, h_ing_yoy, rfc_for_yoy, start_prev_12m, start_12m_yoy)
                    util_prev_12m = ventas_prev_12m - gastos_prev_12m

                    def _growth_pct(cur: float, prev: float) -> float | None:
                        if prev == 0:
                            return None
                        return (cur - prev) / abs(prev) * 100.0

                    g_ventas_12m = _growth_pct(ventas_12m_yoy, ventas_prev_12m)
                    g_gastos_12m = _growth_pct(gastos_12m_yoy, gastos_prev_12m)
                    g_util_12m = _growth_pct(util_12m_yoy, util_prev_12m)


            #==================================================
            # 
            # =================================================

            with st.container(border=True):
                st.markdown("### 💲 Ventas y Utilidad Fiscal")



                def _fmt_pct(x: float | None) -> str:
                    return "—" if x is None else f"{x:,.2f}%"

                tbl = pd.DataFrame(
                    {
                        str(y0): [_money(ventas_y0), _money(gastos_y0), _money(util_y0)],
                        str(y1): [_money(ventas_y1), _money(gastos_y1), _money(util_y1)],
                        str(y2): [_money(ventas_y2), _money(gastos_y2), _money(util_y2)],

                        "Últimos 12 Meses": [
                            _money(ventas_12m_yoy) if calc_yoy else "—",
                            _money(gastos_12m_yoy) if calc_yoy else "—",
                            _money(util_12m_yoy) if calc_yoy else "—",
                        ],

                        "12M previos (base YoY)": [
                            _money(ventas_prev_12m) if calc_yoy else "—",
                            _money(gastos_prev_12m) if calc_yoy else "—",
                            _money(util_prev_12m) if calc_yoy else "—",
                        ],

                        "Promedio mensual (últ. 12M)": [
                            _money(ventas_12m_yoy / 12.0) if calc_yoy else "—",
                            _money(gastos_12m_yoy / 12.0) if calc_yoy else "—",
                            _money(util_12m_yoy / 12.0) if calc_yoy else "—",
                        ],

                        "Crec. vs 12M previos": [
                            _fmt_pct(g_ventas_12m) if calc_yoy else "—",
                            _fmt_pct(g_gastos_12m) if calc_yoy else "—",
                            _fmt_pct(g_util_12m) if calc_yoy else "—",
                        ],

                    },
                    index=["Ventas Anuales", "Gastos Anuales", "Utilidad Fiscal"],
                )



                st.dataframe(tbl, use_container_width=True)

                # KPIs en una sola línea debajo (fuente más chica)
                kpi_line = (
                    f"<b>KPIs SAT</b> · "
                    f"Ingresos: {_money(k_ing)} · "
                    f"Egresos: {_money(k_egr)} · "
                    f"Nómina: {_money(k_nom)} · "
                    f"Interés: {_money(k_int)}"
                )

                st.markdown(
                    f"<div style='font-size:12px; opacity:0.85; margin-top:6px;'>{kpi_line}</div>",
                    unsafe_allow_html=True,
                )


            # gráfico mensual de utilidad fiscal (últimos 12 meses) con barras ventas vs gastos
            months = pd.date_range(start=start_12m, end=end_next_month, freq="MS")[:12]
            rows = []
            for m in months:
                m2 = m + relativedelta(months=1)
                v = _period_sum_ingresos(h_ing, rfc, m, m2)
                g = _period_sum_egresos(h_egr, h_ing, rfc, m, m2)
                rows.append({"Mes": m.strftime("%Y-%m"), "Ventas": float(v), "Gastos": float(g), "Utilidad Fiscal": float(v - g)})

            monthly = pd.DataFrame(rows)

            with st.container(border=True):
                import altair as alt

                # Dropdown para seleccionar qué serie ver (tomadas de la misma tabla / cálculo base)
                sel_metric = st.selectbox(
                    "Mostrar",
                    options=[
                        "Utilidad Fiscal últimos 12 meses",
                        "Ventas Anuales",
                        "Gastos Anuales",
                    ],
                    index=0,
                    key="sat_last12_metric",
                )

                # Mapeo: nombres UI -> columna en `monthly`
                metric_map = {
                    "Utilidad Fiscal últimos 12 meses": "Utilidad Fiscal",
                    "Ventas Anuales": "Ventas",
                    "Gastos Anuales": "Gastos",
                }

                col = metric_map.get(sel_metric, "Utilidad Fiscal")

                # Título dinámico
                st.markdown(f"### 📈 {sel_metric}")

                # Dataset para graficar SOLO la métrica seleccionada
                d_plot = monthly[["Mes", col]].rename(columns={col: "Monto"}).copy()

                chart = (
                    alt.Chart(d_plot)
                    .mark_bar()
                    .encode(
                        x=alt.X("Mes:N", title="Mes"),
                        y=alt.Y("Monto:Q", title="Monto"),
                        tooltip=["Mes:N", alt.Tooltip("Monto:Q", format=",.2f")],
                    )
                    .properties(height=320)
                )

                st.altair_chart(chart, use_container_width=True)


            # =============================================================================
            # Top 10 Clientes / Proveedores (Syntage)
            # =============================================================================
            # Respeta filtro Desde/Hasta del usuario (si existe)
            d_from = st.session_state.get("cfdi_date_from_value") or st.session_state.get("cfdi_date_from")
            d_to = st.session_state.get("cfdi_date_to_value") or st.session_state.get("cfdi_date_to")

            # fallback por si algo viene vacío
            if d_from is None:
                d_from = date.today() - timedelta(days=365)
            if d_to is None:
                d_to = date.today()

            # ISO UTC (incluye día completo)
            from_dt = pd.Timestamp(d_from).strftime("%Y-%m-%d")
            to_dt = pd.Timestamp(d_to).strftime("%Y-%m-%d")


            rfc_for_conc = st.session_state.get("last_rfc") or rfc

            # ✅ NUEVO: concentración calculada 100% desde CFDI descargados (headers)
            conc_customers, conc_suppliers = _concentration_from_cfdi_headers(
                rfc=rfc_for_conc,
                ing_headers=ing.headers if ing is not None else None,
                egr_headers=egr.headers if egr is not None else None,
            )


            df_customers = _conc_to_df(conc_customers)
            df_suppliers = _conc_to_df(conc_suppliers)

            cols_conc = st.columns(2)
            with cols_conc[0]:
                with st.container(border=True):
                    st.markdown("### 🧑‍💼 Top 10 Clientes")
                    if df_customers.empty:
                        st.info("Sin datos de concentración de clientes para el periodo.")
                    else:
                        _render_donut(df_customers, title="Distribución")
                        # Tabla interactiva: mantener numéricos para sort correcto
                        df_show = df_customers.copy()

                        # Nos quedamos con lo útil + numérico
                        keep = ["name", "transactions", "_share_num", "_total_num"]
                        df_show = df_show[[c for c in keep if c in df_show.columns]].copy()

                        # Renombrar para UI
                        df_show = df_show.rename(
                            columns={
                                "name": "Cliente",
                                "transactions": "# CFDI",
                                "_share_num": "Participación %",
                                "_total_num": "Monto",
                            }
                        )

                        # --- redondeos numéricos (siguen siendo numéricos) ---
                        if "Monto" in df_show.columns:
                            df_show["Monto"] = pd.to_numeric(df_show["Monto"], errors="coerce").fillna(0).round(0)
                        if "Participación %" in df_show.columns:
                            df_show["Participación %"] = pd.to_numeric(df_show["Participación %"], errors="coerce").fillna(0).round(2)
                        if "# CFDI" in df_show.columns:
                            df_show["# CFDI"] = pd.to_numeric(df_show["# CFDI"], errors="coerce").fillna(0).astype(int)

                        # --- formato SOLO visual (no convierte df_show a string) ---
                        sty = df_show.style.format(
                            {
                                "Monto": "${:,.0f}",            # moneda sin decimales
                                "Participación %": "{:,.2f}",   # si quieres con % lo hacemos luego
                            }
                        )

                        st.dataframe(sty, use_container_width=True, hide_index=True)

            with cols_conc[1]:
                with st.container(border=True):
                    st.markdown("### 🏭 Top 10 Proveedores")
                    if df_suppliers.empty:
                        st.info("Sin datos de concentración de proveedores para el periodo.")
                    else:
                        _render_donut(df_suppliers, title="Distribución")
                        df_show = df_suppliers.copy()

                        keep = ["name", "transactions", "_share_num", "_total_num"]
                        df_show = df_show[[c for c in keep if c in df_show.columns]].copy()

                        df_show = df_show.rename(
                            columns={
                                "name": "Proveedor",
                                "transactions": "# CFDI",
                                "_share_num": "Participación %",
                                "_total_num": "Monto",
                            }
                        )

                        if "Monto" in df_show.columns:
                            df_show["Monto"] = pd.to_numeric(df_show["Monto"], errors="coerce").fillna(0).round(0)
                        if "Participación %" in df_show.columns:
                            df_show["Participación %"] = pd.to_numeric(df_show["Participación %"], errors="coerce").fillna(0).round(2)
                        if "# CFDI" in df_show.columns:
                            df_show["# CFDI"] = pd.to_numeric(df_show["# CFDI"], errors="coerce").fillna(0).astype(int)

                        sty = df_show.style.format(
                            {
                                "Monto": "${:,.0f}",
                                "Participación %": "{:,.2f}",
                            }
                        )

                        st.dataframe(sty, use_container_width=True, hide_index=True)

            # =============================================================================
            # Prod/Serv (resumen)_egresos (MISMA card que en Facturas, con filtros)
            # =============================================================================
            with st.container(border=True):
                st.markdown("Productos y servicios comprados")

                if resumen_egr is None or resumen_egr.empty:
                    st.info("Sin datos para el periodo.")
                else:
                    d = resumen_egr.copy()

                    # monto -> numérico y redondeado a entero
                    if "monto" in d.columns:
                        d["monto"] = pd.to_numeric(d["monto"], errors="coerce").fillna(0).round(0)

                    # Formato de moneda SOLO para display (no convierte el DF a string)
                    sty = d.style.format({"monto": "${:,.0f}"})

                    st.dataframe(
                        sty,
                        use_container_width=True,
                        hide_index=True,
                    )

            # =============================================================================
            # Clientes / Proveedores (desde CFDI XML) - con nombres
            # =============================================================================
            clientes_df = cfdi_data.get("clientes_df", pd.DataFrame())
            proveedores_df = cfdi_data.get("proveedores_df", pd.DataFrame())

            # (opcional) si estás en modo local y no existen, caes al builder XML
            if (clientes_df is None or clientes_df.empty) and cfdi_source == "local":
                clientes_df, proveedores_df = build_clientes_proveedores_tables(
                    rfc=rfc,
                    ing_headers=ing.headers if ing is not None else None,
                    egr_headers=egr.headers if egr is not None else None,
                )

            with st.container(border=True):
                st.markdown("Clientes (desde CFDI XML)")

                # ✅ Universo contable único con Top 10 Clientes:
                # neto = I emitidas - E emitidas (notas de crédito / devoluciones)
                cli_net = build_clientes_net_table(
                    rfc=rfc_for_conc,
                    ing_headers=ing.headers if ing is not None else None,
                )

                if cli_net is None or cli_net.empty:
                    st.info("Sin clientes para el periodo.")
                else:
                            d = cli_net.copy().reset_index(drop=True)

                            money_cols = ["Facturas (I)", "Notas crédito (E)", "Emitido Neto"]
                            for c in money_cols:
                                if c in d.columns:
                                    d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0).round(0)

                            # ✅ sanitiza (pero sin tocar las columnas numéricas)
                            d = _st_safe_df(d)

                            MAX_ROWS = 5000
                            d_show = d.head(MAX_ROWS) if len(d) > MAX_ROWS else d

                            # ✅ formato moneda SOLO visual (no convierte el DF a string)
                            sty = d_show.style.format({c: "${:,.0f}" for c in money_cols if c in d_show.columns})

                            st.dataframe(sty, use_container_width=True, hide_index=True)


            with st.container(border=True):
                st.markdown("Proveedores (desde CFDI XML)")

                # ✅ Universo contable único con Top 10 Proveedores:
                # neto = I recibidas - E recibidas (notas de crédito / devoluciones)
                prov_net = build_proveedores_net_table(
                    rfc=rfc_for_conc,
                    egr_headers=egr.headers if egr is not None else None,
                )

                if prov_net is None or prov_net.empty:
                    st.info("Sin proveedores para el periodo.")
                else:
                    d = prov_net.copy().reset_index(drop=True)

                    money_cols = ["Compras (I)", "Notas crédito (E)", "Recibido Neto"]
                    for c in money_cols:
                        if c in d.columns:
                            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0).round(0)

                    # (opcional pero recomendado por tu historial React) sanitiza
                    d = _st_safe_df(d)

                    MAX_ROWS = 5000
                    d_show = d.head(MAX_ROWS) if len(d) > MAX_ROWS else d

                    sty = d_show.style.format({c: "${:,.0f}" for c in money_cols if c in d_show.columns})

                    st.dataframe(sty, use_container_width=True, hide_index=True)
            
            # =============================================================================
            # Cap Table (Syntage) - debajo de Proveedores (desde CFDI XML)
            # =============================================================================
            with st.container(border=True):
                st.markdown("Cap Table")

                # usa el RFC cargado (cuando ya se presionó Calcular)
                rfc_for_cap = st.session_state.get("last_rfc") or rfc

                if not rfc_for_cap or not (12 <= len(rfc_for_cap) <= 13):
                    st.info("Ingresa un RFC válido arriba y presiona Calcular.")
                else:
                    try:
                        out = fetch_cap_table_df(rfc_for_cap)
                        df_cap = out.get("df")
                        entity_id = out.get("entity_id") or ""
                    except Exception as e:
                        st.error(f"Error consultando Cap Table: {e}")
                        df_cap = None
                        entity_id = ""

                    if not entity_id:
                        st.info("No se encontró una Entity asociada a este RFC en tu cuenta de Syntage.")
                    elif df_cap is None or getattr(df_cap, "empty", True):
                        st.info("Sin accionistas para mostrar.")
                    else:
                        # Mismo formato que Clientes/Proveedores: AG Grid con filtros
                        render_filterable_grid(df_cap, key="sat_cap_table_grid")
            
            # =============================================================================
            # Empleados (Syntage Insights) jd
            # =============================================================================

            sat_service = get_service()

            with st.container(border=True):
                st.markdown("### 👥 Número de empleados")

                rfc_for_emp = st.session_state.get("last_rfc") or rfc

                if not rfc_for_emp or not (12 <= len(rfc_for_emp) <= 13):   
                    st.info("Ingresa un RFC válido arriba y presiona Calcular.")
                else:
                    try:
                        # reutilizamos el mismo rango que concentración
                        df_emp = sat_service.get_employees_table(
                            rfc=rfc_for_emp,
                            from_dt=f"{from_dt}T00:00:00Z",
                            to_dt=f"{to_dt}T23:59:59Z",
                        )
                    except Exception as e:
                        st.error(f"Error consultando empleados: {e}")
                        df_emp = None

                    if df_emp is None or df_emp.empty:
                        st.info("Sin datos de empleados para el periodo seleccionado.")
                    else:
                        # tabla
                        # Preparar tabla para mostrar
                        df_emp = df_emp.iloc[1:].reset_index(drop=True)

                        df_show = df_emp.copy()

                        # Renombrar columnas
                        df_show = df_show.rename(
                            columns={
                                "mes": "Mes",
                                "numero_empleados": "Empleados",
                            }
                        )


                        # Estilo
                        styled = (
                            df_show.style
                            .set_properties(**{"text-align": "center"})
                            .set_table_styles([
                                {"selector": "th", "props": [("text-align", "center"), ("font-weight", "bold")]}
                            ])
                        )

                        st.dataframe(styled, use_container_width=True)


                        # métrica actual
                        current_emp = int(df_emp.iloc[-1]["numero_empleados"])
                        st.metric("Empleados actuales", current_emp)

                        import altair as alt

                        df_emp["mes"] = pd.to_datetime(df_emp["mes"])

                        chart = (
                            alt.Chart(df_emp)
                            .mark_bar()
                            .encode(
                                x=alt.X(
                                    "mes:T",
                                    title="Mes",
                                    timeUnit="yearmonth",
                                    axis=alt.Axis(format="%b-%Y", labelAngle=0)
                                ),
                                y=alt.Y(
                                    "numero_empleados:Q",
                                    title="Número de empleados"
                                ),
                                tooltip=[
                                    alt.Tooltip("mes:T", title="Mes", format="%b-%Y"),
                                    alt.Tooltip("numero_empleados:Q", title="Empleados"),
                                ],
                            )
                            .properties(height=320)
                        )

                        st.altair_chart(chart, use_container_width=True)


            # =============================================================================
            # Indicadores de Riesgo
            # =============================================================================
            with st.container(border=True):
                st.markdown("## ⚠️ Indicadores de Riesgo (desde Syntage)")

                risk = sat_service.get_risk_indicators(rfc_for_emp) or {}

                # --------- normalización segura ----------
                opinion = (risk.get("opinion_cumplimiento") or None)
                blacklist = risk.get("estatus_lista_negra")
                contras = risk.get("contrapartes_lista_negra")
                interco = risk.get("facturacion_intercompania")
                cancel_emit = risk.get("cancelacion_emitidas_pct")
                cancel_rec = risk.get("cancelacion_recibidas_pct")

                def _fmt_pct(x):
                    if x is None:
                        return "—"
                    try:
                        return f"{float(x)*100:.0f}%"
                    except Exception:
                        return "—"

                def _fmt_int(x):
                    if x is None:
                        return "—"
                    try:
                        return f"{int(x):,}"
                    except Exception:
                        return str(x)

                def _badge(status: str) -> str:
                    # status: ok | risk | na
                    if status == "ok":
                        return "<span class='uw-badge uw-ok'>OK</span>"
                    if status == "risk":
                        return "<span class='uw-badge uw-risk'>Riesgo</span>"
                    return "<span class='uw-badge uw-na'>N/A</span>"

                def _card(title: str, value: str, badge: str, subtitle: str = "") -> str:
                    sub = f"<div class='uw-sub'>{subtitle}</div>" if subtitle else ""
                    return f"""
                    <div class="uw-risk-card">
                    <div class="uw-top">
                        <div class="uw-title">{title}</div>
                        {badge}
                    </div>
                    <div class="uw-value">{value}</div>
                    {sub}
                    </div>
                    """

                # --------- evaluación ----------
                # Opinión: OK si 'positive'
                op_status = "na" if opinion is None else ("ok" if str(opinion).lower() == "positive" else "risk")
                op_value = "Sin registro" if opinion is None else str(opinion).capitalize()

                # Lista negra: OK si None / vacío
                bl_status = "ok" if blacklist is None else "risk"
                bl_value = "Sin registro" if blacklist is None else str(blacklist).capitalize()

                # Contrapartes: OK si 0
                c_val_num = None
                try:
                    c_val_num = int(contras) if contras is not None else None
                except Exception:
                    c_val_num = None
                contras_status = "na" if c_val_num is None else ("ok" if c_val_num == 0 else "risk")

                # Interco: OK si 0
                i_val_num = None
                try:
                    i_val_num = int(interco) if interco is not None else None
                except Exception:
                    i_val_num = None
                interco_status = "na" if i_val_num is None else ("ok" if i_val_num == 0 else "risk")

                # Cancelaciones: OK si <= 10%
                def _cancel_status(x):
                    if x is None:
                        return "na"
                    try:
                        return "ok" if float(x) <= 0.10 else "risk"
                    except Exception:
                        return "na"

                emit_status = _cancel_status(cancel_emit)
                rec_status = _cancel_status(cancel_rec)

                cards_html = "\n".join(
                    [
                        _card("Opinión de Cumplimiento", op_value, _badge(op_status), "OK si es 'Positive'"),
                        _card("Estatus de Lista Negra", bl_value, _badge(bl_status), "OK si no hay registro"),
                        _card("Contrapartes en Lista Negra", _fmt_int(contras), _badge(contras_status), "OK si 0"),
                        _card("Facturación Intercompañía", _fmt_int(interco), _badge(interco_status), "OK si 0"),
                        _card("Cancelación de Facturas Emitidas", _fmt_pct(cancel_emit), _badge(emit_status), "OK si ≤ 10%"),
                        _card("Cancelación de Facturas Recibidas", _fmt_pct(cancel_rec), _badge(rec_status), "OK si ≤ 10%"),
                    ]
                )

                # ✅ Render robusto de HTML/CSS (no cambia la funcionalidad)
                import streamlit.components.v1 as components

                components.html(
                    f"""
                    <style>
                    .uw-risk-grid{{
                        display:grid;
                        grid-template-columns: repeat(3, minmax(0, 1fr));
                        gap: 12px;
                        margin-top: 6px;
                    }}
                    .uw-risk-card{{
                        border: 1px solid rgba(49,51,63,0.12);
                        border-radius: 12px;
                        padding: 12px 12px 10px 12px;
                        background: rgba(255,255,255,0.7);
                        font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
                    }}
                    .uw-top{{
                        display:flex;
                        align-items:center;
                        justify-content:space-between;
                        gap: 10px;
                        margin-bottom: 6px;
                    }}
                    .uw-title{{
                        font-size: 12px;
                        opacity: 0.85;
                        line-height: 1.2;
                    }}
                    .uw-value{{
                        font-size: 22px;
                        font-weight: 700;
                        line-height: 1.15;
                    }}
                    .uw-sub{{
                        margin-top: 4px;
                        font-size: 11px;
                        opacity: 0.7;
                    }}
                    .uw-badge{{
                        font-size: 11px;
                        font-weight: 700;
                        padding: 4px 8px;
                        border-radius: 999px;
                        border: 1px solid rgba(49,51,63,0.12);
                    }}
                    .uw-ok{{ background: rgba(0, 200, 83, 0.12); }}
                    .uw-risk{{ background: rgba(255, 171, 0, 0.16); }}
                    .uw-na{{ background: rgba(158, 158, 158, 0.14); }}

                    @media (max-width: 900px){{
                        .uw-risk-grid{{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
                    }}
                    </style>

                    <div class='uw-risk-grid'>
                    {cards_html}
                    </div>
                    """,
                    height=260,
                )
            


    else:
        st.info("Ingresa un RFC arriba, selecciona fuente/rango CFDI y presiona Calcular.")

# =============================================================================


# TAB 1: BURO
# =============================================================================
with tabs[1]:
    # ✅ Botón de Descarga PDF en la pestaña Buró
    render_pdf_download_button()

    st.caption("Buró de Crédito → Moffin (PF / PM)")

    # =====================================================
    # SESSION STATE INIT
    # =====================================================
    if "df_buro" not in st.session_state:
        st.session_state.df_buro = None
        st.session_state.last_rfc = None

    if "df_editor_pm" not in st.session_state:
        st.session_state.df_editor_pm = None

    # =====================================================
    # CONSULTA CONTROLADA
    # =====================================================
    if run:
        if not rfc_valid:
            st.warning("RFC inválido.")
            st.stop()

        if (
            st.session_state.df_buro is None
            or st.session_state.last_rfc != rfc
        ):
            with st.spinner("Consultando Buró de Crédito..."):
                try:
                    resultado = obtener_buro_moffin_por_rfc(rfc)

                    # PF -> df
                    # PM -> (df, personas) en tu implementación nueva
                    if isinstance(resultado, tuple):
                        df_buro, personas_pm = resultado
                        st.session_state.personas_pm = personas_pm
                    else:
                        df_buro = resultado
                        st.session_state.personas_pm = None

                    st.session_state.df_buro = df_buro
                    st.session_state.last_rfc = rfc
                    st.session_state.df_editor_pm = None

                except Exception as e:
                    st.error(f"Error: {e}")
                    st.stop()

    df_buro = st.session_state.df_buro

    if df_buro is None or df_buro.empty:
        st.info("No hay información disponible.")
        st.stop()

    # =====================================================
    # DETECTAR TIPO RFC
    # =====================================================
    es_pf = len(rfc.strip()) == 13
    es_pm = len(rfc.strip()) == 12

    # =====================================================
    # ==================== PERSONA MORAL ==================
    # =====================================================
    if es_pm:

        # =====================================================
        # PREPARAR TABLA
        # =====================================================
        cols_visibles = [
            c for c in df_buro.columns
            if c not in ["Fecha Consulta", "MontoTotalPagar"]
        ]
        df_tabla = df_buro[cols_visibles].copy()

        # saldo numérico para abiertos/cerrados
        if "Saldo Actual" in df_tabla.columns:
            df_tabla["_monto_num"] = pd.to_numeric(
                df_tabla["Saldo Actual"].astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False),
                errors="coerce"
            ).fillna(0)
        else:
            df_tabla["_monto_num"] = 0.0

        df_abiertos = df_tabla[df_tabla["_monto_num"] > 0].drop(columns="_monto_num")
        df_cerrados = df_tabla[df_tabla["_monto_num"] == 0].drop(columns="_monto_num")

        # =====================================================
        # INICIALIZAR EDITOR
        # =====================================================
        if not df_abiertos.empty:

            if st.session_state.df_editor_pm is None:
                df_init = df_abiertos.copy()

                if {"pago_a", "pago_b", "pago_c", "pago_d"}.issubset(df_init.columns):
                    df_init["Forma de pago"] = "A"
                    df_init["valor_x"] = df_init["pago_a"]
                else:
                    df_init["valor_x"] = 0.0

                st.session_state.df_editor_pm = df_init

            df_editado = st.session_state.df_editor_pm.copy()

            mapa = {"A": "pago_a", "B": "pago_b", "C": "pago_c", "D": "pago_d"}

            # SOLO recalcular si NO es Manual
            if "Forma de pago" in df_editado.columns:
                for i, row in df_editado.iterrows():
                    forma = row.get("Forma de pago")
                    if forma in mapa and mapa[forma] in df_editado.columns:
                        df_editado.at[i, "valor_x"] = row[mapa[forma]]

            df_editado["valor_x"] = pd.to_numeric(df_editado.get("valor_x"), errors="coerce").fillna(0).round(2)
        else:
            df_editado = pd.DataFrame()

        # =====================================================
        # BANDA 1 - HEADER
        # =====================================================
        fecha_consulta = df_buro["Fecha Consulta"].iloc[0] if "Fecha Consulta" in df_buro.columns else "N/A"
        total_dinamico = df_editado["valor_x"].sum() if not df_editado.empty and "valor_x" in df_editado.columns else 0.0

        col_info, col_fecha, col_kpi = st.columns(3)

        with col_info:
            st.markdown("""
            **Formas de pago**
            - **A:** Saldo vigente / plazo  
            - **B:** Promedio pagado  
            - **C:** 10% saldo vigente  
            - **D:** Saldo inicial / plazo  
            - **Manual:** Editable libremente  
            """)

        with col_fecha:
            st.metric("Fecha de consulta", f"{fecha_consulta}")

        with col_kpi:
            st.metric("Pago mensual consolidado", _money(total_dinamico))

    

        # =====================================================
        # BANDA 2 - RESUMEN + DONUTS
        # =====================================================
        col_resumen, col_donut = st.columns(2)

        # ---- DONUT + TABLA POR TIPO (numero de creditos) ----
        with col_resumen:
            with st.container(border=True):
                st.markdown("### 📊 Distribución por tipo de crédito")

                if not df_editado.empty and "Tipo de contrato" in df_editado.columns:
                    df_tipo = (
                        df_editado
                        .groupby("Tipo de contrato")
                        .agg(numero_creditos=("Tipo de contrato", "count"),
                             pago_mensual_total=("valor_x", "sum"))
                        .reset_index()
                    )

                    # donut por # de créditos
                    df_pie_tipo = df_tipo.copy()
                    df_pie_tipo["_total_num"] = df_pie_tipo["numero_creditos"]
                    df_pie_tipo["name"] = df_pie_tipo["Tipo de contrato"].astype(str)

                    _render_donut(df_pie_tipo, title="Distribución", value_col="_total_num", label_col="name")

                    # tabla abajo
                    df_tipo["Participación %"] = (
                        df_tipo["numero_creditos"] / df_tipo["numero_creditos"].sum()
                    ).apply(lambda x: f"{x:.1%}")

                    df_tipo["Pago Mensual Total"] = df_tipo["pago_mensual_total"].apply(lambda x: f"${x:,.2f}")
                    df_show = df_tipo.rename(columns={"numero_creditos": "# Créditos"})[
                        ["Tipo de contrato", "# Créditos", "Participación %", "Pago Mensual Total"]
                    ]

                    st.dataframe(df_show, use_container_width=True, hide_index=True)
                else:
                    st.info("Sin datos suficientes para distribución por tipo.")

        # ---- DONUT + TABLA POR PEOR MOP (incluye cerrados) ----
        with col_donut:
            with st.container(border=True):
                st.markdown("### 🚨 Distribución por severidad (Peor MOP)")

                if "peor_mop" in df_buro.columns:
                    df_mop = pd.concat([df_editado, df_cerrados], ignore_index=True).copy()
                    df_mop["peor_mop"] = pd.to_numeric(df_mop["peor_mop"], errors="coerce")

                    df_pie = (
                        df_mop
                        .groupby("peor_mop", dropna=False)
                        .size()
                        .reset_index(name="_total_num")
                    )

                    df_pie["name"] = df_pie["peor_mop"].fillna("Sin historial").astype(str)

                    _render_donut(df_pie, title="Distribución", value_col="_total_num", label_col="name")

                    df_pie["Participación %"] = (
                        df_pie["_total_num"] / df_pie["_total_num"].sum()
                    ).apply(lambda x: f"{x:.1%}")

                    df_show = df_pie.rename(columns={"name": "Peor MOP", "_total_num": "# Créditos"})[
                        ["Peor MOP", "# Créditos", "Participación %"]
                    ]

                    st.dataframe(df_show, use_container_width=True, hide_index=True)
                else:
                    st.info("No se encontró 'peor_mop' en el buró.")

        st.markdown("---")

        # =====================================================
        # BANDA 3 - CREDITOS ABIERTOS (editable) + % ocupación visible
        # =====================================================
        st.markdown("### ✅ Créditos abiertos")

        if df_abiertos.empty:
            st.info("No se encontraron créditos abiertos.")
        else:
            # construir columna visual % Ocupación
            if "Porcentaje Ocupación" in df_editado.columns:
                df_editado["Porcentaje Ocupación"] = pd.to_numeric(df_editado["Porcentaje Ocupación"], errors="coerce")

                def format_ocupacion(val):
                    if pd.isna(val):
                        return ""
                    return f"🔴 {val:.0%}" if val > 0.98 else f"🟢 {val:.0%}"

                df_editado["% Ocupación"] = df_editado["Porcentaje Ocupación"].apply(format_ocupacion)

            df_editado = st.data_editor(
                df_editado,
                key="editor_buro_pm",
                column_config={
                    "Forma de pago": st.column_config.SelectboxColumn(
                        "Forma de pago",
                        options=["A", "B", "C", "D", "Manual"],
                        required=True,
                    ),
                    "valor_x": st.column_config.NumberColumn(
                        "Pago seleccionado",
                        format="$%.2f",
                        min_value=0.0
                    ),
                    "% Ocupación": st.column_config.TextColumn(
                        "% Ocupación",
                        disabled=True,
                    ),
                    # ocultar técnicas
                    "pago_a": None,
                    "pago_b": None,
                    "pago_c": None,
                    "pago_d": None,
                    "Porcentaje Ocupación": None,
                    "last_update": None,
                },
                use_container_width=True,
                hide_index=True,
            )

            # guardar cambios del editor
            st.session_state.df_editor_pm = df_editado

            # recalcular KPI con lo editado (incluye Manual)
            total_dinamico = pd.to_numeric(df_editado["valor_x"], errors="coerce").fillna(0).sum()
            col_kpi.metric("Pago mensual consolidado", _money(total_dinamico))

        st.markdown("---")

        # =====================================================
        # CREDITOS CERRADOS
        # =====================================================
        st.markdown("### ⛔ Créditos cerrados")

        if df_cerrados.empty:
            st.info("No se encontraron créditos cerrados.")
        else:
            st.dataframe(
                df_cerrados.drop(
                    columns=["pago_a", "pago_b", "pago_c", "pago_d", "valor_x", "last_update", "Porcentaje Ocupación"],
                    errors="ignore"
                ),
                use_container_width=True,
                hide_index=True,
            )

        # =====================================================
        # ACCIONISTAS (AUTOMÁTICOS + MANUALES)
        # =====================================================
        if "personas_pm" in st.session_state and st.session_state.personas_pm is not None:

            if "rfcs_extra_pm" not in st.session_state:
                st.session_state.rfcs_extra_pm = []

            df_personas = st.session_state.personas_pm

            st.markdown("---")
            st.markdown("## 👥 Accionistas")

            # =====================================================
            # 1️⃣ Construir lista de RFCs automáticos
            # =====================================================
            rfcs_base = []
            labels_base = []

            if not df_personas.empty:
                for _, row in df_personas.iterrows():
                    nombre = str(row["nombreAccionista"]).title()
                    rfc_acc = row["rfc"]
                    rfcs_base.append(rfc_acc)
                    labels_base.append(f"👤 {nombre} ({rfc_acc})")

            # =====================================================
            # 2️⃣ RFCs agregados manualmente
            # =====================================================
            rfcs_extra = st.session_state.rfcs_extra_pm
            labels_extra = [f"➕ Manual ({r})" for r in rfcs_extra]

            # =====================================================
            # 3️⃣ Construir tabs dinámicos
            # =====================================================
            tabs_labels = labels_base + labels_extra + ["➕"]

            tabs_dinamicos = st.tabs(tabs_labels)

            # =====================================================
            # 4️⃣ Renderizar accionistas automáticos
            # =====================================================
            for i, rfc_accionista in enumerate(rfcs_base):

                with tabs_dinamicos[i]:

                    try:
                        df_pf = obtener_buro_moffin_por_rfc(rfc_accionista)
                    except Exception:
                        st.warning("No se pudo consultar el buró del accionista.")
                        continue

                    if df_pf is None or df_pf.empty:
                        st.info("Sin información de buró.")
                        continue

                    # ---------------- HEADER PF ----------------
                    fecha_consulta = df_pf["Fecha Consulta"].iloc[0] if "Fecha Consulta" in df_pf.columns else "N/A"
                    peor_mop_total = df_pf["PEOR_MOP TOTAL"].iloc[0] if "PEOR_MOP TOTAL" in df_pf.columns else None
                    monto_total = df_pf["Monto Total"].iloc[0] if "Monto Total" in df_pf.columns else 0
                    total_cuentas = len(df_pf)

                    c1, c2, c3, c4 = st.columns(4)

                    with c1:
                        st.metric("Fecha consulta", fecha_consulta)
                    with c2:
                        st.metric("Total cuentas", total_cuentas)
                    with c3:
                        try:
                            st.metric("Monto total", _money(monto_total))
                        except:
                            st.metric("Monto total", str(monto_total))
                    with c4:
                        st.metric("Peor MOP", peor_mop_total)

                    st.markdown("---")

                    # ---------------- DONUTS ----------------
                    col_tipo, col_mop = st.columns(2)

                    with col_tipo:
                        with st.container(border=True):
                            st.markdown("### 📊 Distribución por tipo de contrato")

                            if "Tipo de contrato" in df_pf.columns:
                                df_tipo = (
                                    df_pf
                                    .groupby("Tipo de contrato")
                                    .size()
                                    .reset_index(name="_total_num")
                                )
                                df_tipo["name"] = df_tipo["Tipo de contrato"].astype(str)

                                _render_donut(
                                    df_tipo,
                                    title="Distribución",
                                    value_col="_total_num",
                                    label_col="name"
                                )
                                df_tipo["Participación %"] = (df_tipo["_total_num"] / df_tipo["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                                st.dataframe(
                                    df_tipo.rename(columns={"Tipo de contrato": "Tipo", "_total_num": "# Cuentas"})[["Tipo", "# Cuentas", "Participación %"]],
                                    use_container_width=True,
                                    hide_index=True
                                )

                    with col_mop:
                        with st.container(border=True):
                            st.markdown("### 🚨 Distribución por severidad (Peor MOP)")

                            if "peor_mop" in df_pf.columns:
                                df_mop = (
                                    df_pf
                                    .groupby("peor_mop", dropna=False)
                                    .size()
                                    .reset_index(name="_total_num")
                                )
                                df_mop["name"] = df_mop["peor_mop"].fillna("Sin historial").astype(str)

                                _render_donut(
                                    df_mop,
                                    title="Distribución",
                                    value_col="_total_num",
                                    label_col="name"
                                )
                                df_mop["Participación %"] = (df_mop["_total_num"] / df_mop["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                                st.dataframe(
                                    df_mop.rename(columns={"name": "Peor MOP", "_total_num": "# Cuentas"})[["Peor MOP", "# Cuentas", "Participación %"]],
                                    use_container_width=True,
                                    hide_index=True
                                )


                    st.markdown("---")

                    # ---------------- DETALLE ----------------
                    st.markdown("### 📄 Detalle de cuentas")

                    columnas_constantes = [
                        "Monto Total",
                        "Monto Máx",
                        "PEOR_MOP TOTAL",
                        "MontoTotalPagar",
                        "Personal",
                        "Tarjeta de Crédito",
                        "Hipotecario",
                        "Línea de Crédito",
                        "Automotriz",
                        "Arrendamiento",
                    ]

                    df_detalle = df_pf.drop(columns=columnas_constantes, errors="ignore")

                    st.dataframe(df_detalle, use_container_width=True, hide_index=True)

            # =====================================================
            # 5️⃣ Renderizar RFCs manuales
            # =====================================================
            offset = len(rfcs_base)

            for j, rfc_manual in enumerate(rfcs_extra):

                with tabs_dinamicos[offset + j]:

                    try:
                        df_pf = obtener_buro_moffin_por_rfc(rfc_manual)
                    except Exception:
                        st.warning("No se pudo consultar el buró.")
                        continue

                    if df_pf is None or df_pf.empty:
                        st.info("Sin información.")
                        continue

                    # ---------------- HEADER PF ----------------
                    fecha_consulta = df_pf["Fecha Consulta"].iloc[0] if "Fecha Consulta" in df_pf.columns else "N/A"
                    peor_mop_total = df_pf["PEOR_MOP TOTAL"].iloc[0] if "PEOR_MOP TOTAL" in df_pf.columns else None
                    monto_total = df_pf["Monto Total"].iloc[0] if "Monto Total" in df_pf.columns else 0
                    total_cuentas = len(df_pf)

                    c1, c2, c3, c4 = st.columns(4)

                    with c1:
                        st.metric("Fecha consulta", fecha_consulta)
                    with c2:
                        st.metric("Total cuentas", total_cuentas)
                    with c3:
                        try:
                            st.metric("Monto total", _money(monto_total))
                        except:
                            st.metric("Monto total", str(monto_total))
                    with c4:
                        st.metric("Peor MOP", peor_mop_total)

                    st.markdown("---")

                    # ---------------- DONUTS ----------------
                    col_tipo, col_mop = st.columns(2)

                    with col_tipo:
                        with st.container(border=True):
                            st.markdown("### 📊 Distribución por tipo de contrato")

                            if "Tipo de contrato" in df_pf.columns:
                                df_tipo = (
                                    df_pf
                                    .groupby("Tipo de contrato")
                                    .size()
                                    .reset_index(name="_total_num")
                                )
                                df_tipo["name"] = df_tipo["Tipo de contrato"].astype(str)

                                _render_donut(
                                    df_tipo,
                                    title="Distribución",
                                    value_col="_total_num",
                                    label_col="name"
                                )
                                df_tipo["Participación %"] = (df_tipo["_total_num"] / df_tipo["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                                st.dataframe(
                                    df_tipo.rename(columns={"Tipo de contrato": "Tipo", "_total_num": "# Cuentas"})[["Tipo", "# Cuentas", "Participación %"]],
                                    use_container_width=True,
                                    hide_index=True
                                )

                    with col_mop:
                        with st.container(border=True):
                            st.markdown("### 🚨 Distribución por severidad (Peor MOP)")

                            if "peor_mop" in df_pf.columns:
                                df_mop = (
                                    df_pf
                                    .groupby("peor_mop", dropna=False)
                                    .size()
                                    .reset_index(name="_total_num")
                                )
                                df_mop["name"] = df_mop["peor_mop"].fillna("Sin historial").astype(str)

                                _render_donut(
                                    df_mop,
                                    title="Distribución",
                                    value_col="_total_num",
                                    label_col="name"
                                )
                                df_mop["Participación %"] = (df_mop["_total_num"] / df_mop["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                                st.dataframe(
                                    df_mop.rename(columns={"name": "Peor MOP", "_total_num": "# Cuentas"})[["Peor MOP", "# Cuentas", "Participación %"]],
                                    use_container_width=True,
                                    hide_index=True
                                )


                    st.markdown("---")

                    # ---------------- DETALLE ----------------
                    st.markdown("### 📄 Detalle de cuentas")

                    columnas_constantes = [
                        "Monto Total",
                        "Monto Máx",
                        "PEOR_MOP TOTAL",
                        "MontoTotalPagar",
                        "Personal",
                        "Tarjeta de Crédito",
                        "Hipotecario",
                        "Línea de Crédito",
                        "Automotriz",
                        "Arrendamiento",
                    ]

                    df_detalle = df_pf.drop(columns=columnas_constantes, errors="ignore")

                    st.dataframe(df_detalle, use_container_width=True, hide_index=True)

            # =====================================================
            # 6️⃣ TAB "+"
            # =====================================================
            with tabs_dinamicos[-1]:

                st.markdown("### ➕ Agregar RFC manual")

                nuevo_rfc = st.text_input("Ingresa RFC del accionista")

                if st.button("Agregar RFC"):

                    nuevo_rfc = nuevo_rfc.strip().upper()

                    if len(nuevo_rfc) == 13:

                        if nuevo_rfc not in st.session_state.rfcs_extra_pm:
                            st.session_state.rfcs_extra_pm.append(nuevo_rfc)
                            st.success("RFC agregado correctamente.")
                            st.rerun()
                        else:
                            st.warning("Ese RFC ya fue agregado.")

                    else:
                        st.error("RFC inválido. Debe tener 13 caracteres (Persona Física).")


    # =====================================================
    # ==================== PERSONA FISICA =================
    # =====================================================
    elif es_pf:

        fecha_consulta = df_buro["Fecha Consulta"].iloc[0] if "Fecha Consulta" in df_buro.columns else "N/A"

        peor_mop_total = df_buro["PEOR_MOP TOTAL"].iloc[0] if "PEOR_MOP TOTAL" in df_buro.columns else None
        monto_total = df_buro["Monto Total"].iloc[0] if "Monto Total" in df_buro.columns else 0
        monto_max = df_buro["Monto Máx"].iloc[0] if "Monto Máx" in df_buro.columns else 0
        total_cuentas = len(df_buro)

        # -------- BANDA 1 --------
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Fecha consulta", fecha_consulta)
        with col2:
            st.metric("Total cuentas", total_cuentas)
        with col3:
            try:
                st.metric("Monto total", _money(monto_total))
            except Exception:
                st.metric("Monto total", str(monto_total))
        with col4:
            st.metric("Peor MOP histórico", peor_mop_total)

        st.markdown("---")

        # -------- BANDA 1.5 (CONSTANTES) --------
        columnas_constantes_pf = [
            "Monto Total",
            "Monto Máx",
            "PEOR_MOP TOTAL",
            "MontoTotalPagar",
            "Personal",
            "Tarjeta de Crédito",
            "Hipotecario",
            "Línea de Crédito",
            "Automotriz",
            "Arrendamiento",
        ]
        presentes_pf = [c for c in columnas_constantes_pf if c in df_buro.columns]
        if len(presentes_pf) > 0:
            with st.container(border=True):
                st.markdown("### 💼 Resumen financiero consolidado")

                fila = df_buro.iloc[0]
                df_resumen_fin = pd.DataFrame({
                    "Métrica": presentes_pf,
                    "Valor": [fila.get(c, 0) for c in presentes_pf]
                })

                def _fmt_money_pf(v):
                    if v is None:
                        return "$0.00"
                    if isinstance(v, str) and "$" in v:
                        return v
                    try:
                        return _money(v)
                    except Exception:
                        return str(v)

                df_resumen_fin["Valor"] = df_resumen_fin["Valor"].apply(_fmt_money_pf)
                st.dataframe(df_resumen_fin, use_container_width=True, hide_index=True)

        st.markdown("---")

        # -------- BANDA 2 --------
        col_tipo, col_mop = st.columns(2)

        with col_tipo:
            with st.container(border=True):
                st.markdown("### 📊 Distribución por tipo de contrato")

                if "Tipo de contrato" in df_buro.columns:
                    df_tipo = (
                        df_buro
                        .groupby("Tipo de contrato")
                        .size()
                        .reset_index(name="_total_num")
                    )
                    df_tipo["name"] = df_tipo["Tipo de contrato"].astype(str)

                    _render_donut(df_tipo, title="Distribución", value_col="_total_num", label_col="name")

                    df_tipo["Participación %"] = (df_tipo["_total_num"] / df_tipo["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                    st.dataframe(
                        df_tipo.rename(columns={"Tipo de contrato": "Tipo", "_total_num": "# Cuentas"})[["Tipo", "# Cuentas", "Participación %"]],
                        use_container_width=True,
                        hide_index=True
                    )

        with col_mop:
            with st.container(border=True):
                st.markdown("### 🚨 Distribución por severidad (Peor MOP)")

                if "peor_mop" in df_buro.columns:
                    df_mop = (
                        df_buro
                        .groupby("peor_mop", dropna=False)
                        .size()
                        .reset_index(name="_total_num")
                    )
                    df_mop["name"] = df_mop["peor_mop"].fillna("Sin historial").astype(str)

                    _render_donut(df_mop, title="Distribución", value_col="_total_num", label_col="name")

                    df_mop["Participación %"] = (df_mop["_total_num"] / df_mop["_total_num"].sum()).apply(lambda x: f"{x:.1%}")
                    st.dataframe(
                        df_mop.rename(columns={"name": "Peor MOP", "_total_num": "# Cuentas"})[["Peor MOP", "# Cuentas", "Participación %"]],
                        use_container_width=True,
                        hide_index=True
                    )

        st.markdown("---")

        # -------- BANDA 3 (DETALLE) --------
        st.markdown("### 📄 Detalle de cuentas")

        # no mostrar constantes en detalle
        df_detalle = df_buro.drop(columns=columnas_constantes_pf, errors="ignore")

        st.dataframe(df_detalle, use_container_width=True, hide_index=True)

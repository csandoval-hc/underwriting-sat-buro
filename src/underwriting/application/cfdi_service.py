# Ruta: src/underwriting/application/cfdi_service.py
# Archivo: cfdi_service.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from underwriting.infrastructure.syntage_client import SyntageClient
from .cfdi_xml_parser import CfdiXmlParser, normalize_clave8


@dataclass(frozen=True)
class CfdiData:
    headers: pd.DataFrame
    conceptos: pd.DataFrame


def _default_max_workers() -> int:
    env = os.getenv("CFDI_MAX_WORKERS", "").strip()
    if env.isdigit() and int(env) > 0:
        return int(env)
    cpu = os.cpu_count() or 4
    return min(16, cpu * 4)


class CfdiService:
    def __init__(self, client: SyntageClient):
        self.client = client
        self.parser = CfdiXmlParser()
        self.max_workers = _default_max_workers()

    def _normalize_id(self, v: Any) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        if not s:
            return ""
        if s.startswith("http://") or s.startswith("https://"):
            if "/invoices/" in s:
                s = s.split("/invoices/")[-1]
            s = s.strip("/")
        if "/invoices/" in s:
            s = s.split("/invoices/")[-1].strip("/")
        return s.strip("/")

    def _extract_ids(self, items: List[Dict[str, Any]]) -> List[str]:
        ids: List[str] = []
        for x in items or []:
            if not isinstance(x, dict):
                continue
            v = x.get("id") or x.get("@id") or x.get("invoiceId") or x.get("uuid") or x.get("documentId")
            v = self._normalize_id(v)
            if v:
                ids.append(v)

        seen = set()
        out: List[str] = []
        for i in ids:
            if i not in seen:
                out.append(i)
                seen.add(i)
        return out

    # -------------------------------------------------------------------------
    # NUEVO: helpers para armar tablas clientes/proveedores con metadata de API
    # -------------------------------------------------------------------------
    def _coerce_dt(self, s: pd.Series) -> pd.Series:
        try:
            return pd.to_datetime(s, errors="coerce", utc=True)
        except Exception:
            return pd.to_datetime(pd.Series([None] * len(s)), errors="coerce", utc=True)

    def _invoices_to_df(self, items: List[Dict[str, Any]] | None) -> pd.DataFrame:
        if not items:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue

            issuer = it.get("issuer") or {}
            receiver = it.get("receiver") or {}

            # ✅ [CHANGE] Capturar uso CFDI (para Descuentos=G02)
            usage_val = it.get("usage") or it.get("cfdiUsage") or it.get("usoCfdi")

            rows.append(
                {
                    "uuid": it.get("uuid") or it.get("folioFiscal") or it.get("id"),
                    "type": it.get("type"),
                    "status": it.get("status"),
                    "paymentType": it.get("paymentType"),
                    "usage": usage_val,  # ✅ [CHANGE]
                    "issuedAt": it.get("issuedAt"),
                    "canceledAt": it.get("canceledAt"),
                    "fullyPaidAt": it.get("fullyPaidAt"),
                    "lastPaymentDate": it.get("lastPaymentDate"),
                    "total": it.get("total"),
                    "discount": it.get("discount"),
                    "paidAmount": it.get("paidAmount"),
                    "dueAmount": it.get("dueAmount"),
                    "issuer_rfc": issuer.get("rfc"),
                    "issuer_name": issuer.get("name"),
                    "receiver_rfc": receiver.get("rfc"),
                    "receiver_name": receiver.get("name"),
                    "isIssuer": it.get("isIssuer"),
                    "isReceiver": it.get("isReceiver"),
                }
            )

        df = pd.DataFrame(rows)

        # Normalizaciones suaves
        for c in ["type", "status", "paymentType", "issuer_rfc", "receiver_rfc"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip().str.upper()

        for c in ["issuer_name", "receiver_name"]:
            if c in df.columns:
                df[c] = df[c].astype(str).fillna("").str.strip()

        for c in ["total", "discount", "paidAmount", "dueAmount"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        for c in ["issuedAt", "canceledAt", "fullyPaidAt", "lastPaymentDate"]:
            if c in df.columns:
                df[c] = self._coerce_dt(df[c])

        # ✅ incluye usage en normalización (ya existe ahora)
        for c in ["type", "status", "paymentType", "usage", "issuer_rfc", "receiver_rfc"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip().str.upper()

        # fallback de uuid limpio
        if "uuid" in df.columns:
            df["uuid"] = df["uuid"].astype(str).str.strip()

        return df

    def _safe_name(self, rfc: str, name: str) -> str:
        n = (name or "").strip()
        return n if n else (rfc or "").strip()

    def _build_counterparty_tables(
        self,
        taxpayer_rfc: str,
        emit_items: List[Dict[str, Any]],
        rec_items: List[Dict[str, Any]],
        pagos_df: pd.DataFrame | None = None,  # ✅ [CHANGE] para fallback de días por pagos
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Devuelve (clientes_df, proveedores_df)
        - clientes: contraparte principal = receiver (cuando taxpayer es issuer)
        - proveedores: contraparte principal = issuer (cuando taxpayer es receiver)

        NOTA: también agrega métricas “del otro lado” para la misma contraparte,
        por si una entidad te factura y también le facturas.
        """
        rfc_app = (taxpayer_rfc or "").strip().upper()

        emit_df = self._invoices_to_df(emit_items)
        rec_df = self._invoices_to_df(rec_items)

        # Prepara pagos (fallback días para cobrar)
        p = pagos_df if isinstance(pagos_df, pd.DataFrame) else pd.DataFrame()
        if not p.empty:
            # columnas esperadas del parser: uuid_factura, fecha_pago, imp_pagado
            if "uuid_factura" not in p.columns:
                p["uuid_factura"] = None
            if "fecha_pago" not in p.columns:
                p["fecha_pago"] = None
            if "imp_pagado" not in p.columns:
                p["imp_pagado"] = 0.0

            p["uuid_factura"] = p["uuid_factura"].astype(str).str.strip()
            p["_fecha_pago_dt"] = pd.to_datetime(p["fecha_pago"], errors="coerce", utc=True)
            p["_imp_pagado"] = pd.to_numeric(p["imp_pagado"], errors="coerce").fillna(0.0)

        # En teoría emit_items ya vienen con isIssuer=True y rec_items con isIssuer=False,
        # pero no dependemos 100% de eso.
        if not emit_df.empty:
            emit_df = emit_df[emit_df.get("issuer_rfc", "") == rfc_app].copy()
        if not rec_df.empty:
            rec_df = rec_df[rec_df.get("receiver_rfc", "") == rfc_app].copy()

        def _agg_for_counterparty(counter_rfc: str, counter_name: str) -> Dict[str, Any]:
            # emitidas hacia counterparty
            e = pd.DataFrame() if emit_df.empty else emit_df[emit_df["receiver_rfc"] == counter_rfc].copy()
            # recibidas desde counterparty
            r = pd.DataFrame() if rec_df.empty else rec_df[rec_df["issuer_rfc"] == counter_rfc].copy()

            # ------- Emitido (I) -------
            e_I = e[e.get("type") == "I"] if not e.empty else e
            e_I_vig = e_I[e_I.get("status") == "VIGENTE"] if not e_I.empty else e_I
            e_I_can = e_I[e_I.get("status") == "CANCELADO"] if not e_I.empty else e_I

            emitido_total = float(e_I_vig["total"].sum()) if not e_I_vig.empty else 0.0
            cancelado_emitido = float(e_I_can["total"].sum()) if not e_I_can.empty else 0.0
            denom = emitido_total + cancelado_emitido
            pct_cancelado = (cancelado_emitido / denom * 100.0) if denom > 0 else 0.0

            # ✅ Descuentos = CFDI con UsoCFDI G02 (Devoluciones, descuentos o bonificaciones)
            descuentos_emitidos = 0.0
            if not e_I_vig.empty and "usage" in e_I_vig.columns:
                descuentos_emitidos = float(e_I_vig.loc[e_I_vig["usage"] == "G02", "total"].sum())

            # Notas de crédito emitidas (E) hacia counterparty
            e_E = e[e.get("type") == "E"] if not e.empty else e
            e_E_vig = e_E[e_E.get("status") == "VIGENTE"] if not e_E.empty else e_E
            notas_credito_emitidas = float(e_E_vig["total"].sum()) if not e_E_vig.empty else 0.0

            # Emitido por cobrar (dueAmount de emitidas, típicamente PPD)
            emitido_por_cobrar = float(e_I_vig["dueAmount"].sum()) if (not e_I_vig.empty and "dueAmount" in e_I_vig.columns) else 0.0

            # Emitido Neto (definición práctica inicial)
            emitido_neto = emitido_total - descuentos_emitidos - notas_credito_emitidas

            # Días para cobrar: promedio (fecha_de_pago - issuedAt) en emitidas PPD
            # Prioridad: fullyPaidAt (cuando se liquidó) -> lastPaymentDate (último pago relacionado)
            # Fallback: Complemento de pagos (CFDI tipo P) por uuid_factura
            dias_para_cobrar = 0.0
            if not e_I_vig.empty and "paymentType" in e_I_vig.columns:
                e_ppd = e_I_vig[e_I_vig["paymentType"] == "PPD"].copy()

                if not e_ppd.empty and "issuedAt" in e_ppd.columns and e_ppd["issuedAt"].notna().any():
                    # 1) intenta con fechas de API
                    pay_dt = None
                    if "fullyPaidAt" in e_ppd.columns and e_ppd["fullyPaidAt"].notna().any():
                        pay_dt = e_ppd["fullyPaidAt"]
                    elif "lastPaymentDate" in e_ppd.columns and e_ppd["lastPaymentDate"].notna().any():
                        pay_dt = e_ppd["lastPaymentDate"]

                    if pay_dt is not None:
                        paid = e_ppd[pay_dt.notna() & e_ppd["issuedAt"].notna()].copy()
                        if not paid.empty:
                            delta_days = (pay_dt.loc[paid.index] - paid["issuedAt"]).dt.total_seconds() / 86400.0
                            delta_days = pd.to_numeric(delta_days, errors="coerce").dropna()
                            if not delta_days.empty:
                                dias_para_cobrar = float(delta_days.mean())

                    # 2) fallback por pagos XML si no se pudo calcular arriba
                    if dias_para_cobrar == 0.0 and not p.empty:
                        uuids = e_ppd["uuid"].astype(str).str.strip()
                        p2 = p[p["uuid_factura"].isin(set(uuids))].copy()
                        if not p2.empty:
                            # usa la última fecha de pago registrada por factura
                            agg = (
                                p2.groupby("uuid_factura", as_index=False)
                                .agg(
                                    _last_pay_dt=("_fecha_pago_dt", "max"),
                                    _imp_pagado_sum=("_imp_pagado", "sum"),
                                )
                            )
                            base = e_ppd[["uuid", "issuedAt"]].copy()
                            base["uuid"] = base["uuid"].astype(str).str.strip()
                            m = base.merge(agg, left_on="uuid", right_on="uuid_factura", how="left")

                            m = m[m["_last_pay_dt"].notna() & m["issuedAt"].notna()].copy()
                            if not m.empty:
                                m["_delta_days"] = (m["_last_pay_dt"] - m["issuedAt"]).dt.total_seconds() / 86400.0
                                m["_delta_days"] = pd.to_numeric(m["_delta_days"], errors="coerce")
                                m = m[m["_delta_days"].notna()].copy()
                                if not m.empty:
                                    w = pd.to_numeric(m["_imp_pagado_sum"], errors="coerce").fillna(0.0)
                                    # si no hay pesos útiles, usa promedio simple
                                    if float(w.sum()) > 0:
                                        dias_para_cobrar = float((m["_delta_days"] * w).sum() / w.sum())
                                    else:
                                        dias_para_cobrar = float(m["_delta_days"].mean())

            # ------- Recibido (I) -------
            r_I = r[r.get("type") == "I"] if not r.empty else r
            r_I_vig = r_I[r_I.get("status") == "VIGENTE"] if not r_I.empty else r_I

            # Notas de crédito recibidas (E) desde counterparty
            r_E = r[r.get("type") == "E"] if not r.empty else r
            r_E_vig = r_E[r_E.get("status") == "VIGENTE"] if not r_E.empty else r_E
            notas_credito_recibidas = float(r_E_vig["total"].sum()) if not r_E_vig.empty else 0.0

            recibido_total = float(r_I_vig["total"].sum()) if not r_I_vig.empty else 0.0
            descuentos_recibidos = float(r_I_vig["discount"].sum()) if (not r_I_vig.empty and "discount" in r_I_vig.columns) else 0.0
            recibido_neto = recibido_total - descuentos_recibidos - notas_credito_recibidas

            # PUE / PPD recibido (método de pago)
            pue_rec = float(r_I_vig[r_I_vig.get("paymentType") == "PUE"]["total"].sum()) if (not r_I_vig.empty and "paymentType" in r_I_vig.columns) else 0.0
            ppd_rec = float(r_I_vig[r_I_vig.get("paymentType") == "PPD"]["total"].sum()) if (not r_I_vig.empty and "paymentType" in r_I_vig.columns) else 0.0

            # Conteo de PPD (recibidas)
            conteo_ppd = int((r_I_vig.get("paymentType") == "PPD").sum()) if (not r_I_vig.empty and "paymentType" in r_I_vig.columns) else 0

            # Monto pagado / por pagar en PPD (recibidas)
            r_ppd = r_I_vig[r_I_vig.get("paymentType") == "PPD"].copy() if (not r_I_vig.empty and "paymentType" in r_I_vig.columns) else pd.DataFrame()
            monto_pagado_ppd = float(r_ppd["paidAmount"].sum()) if (not r_ppd.empty and "paidAmount" in r_ppd.columns) else 0.0
            recibido_por_pagar = float(r_ppd["dueAmount"].sum()) if (not r_ppd.empty and "dueAmount" in r_ppd.columns) else 0.0

            denom_ppd = monto_pagado_ppd + recibido_por_pagar
            pct_pagado_ppd = (monto_pagado_ppd / denom_ppd * 100.0) if denom_ppd > 0 else 0.0

            return {
                "Cliente": self._safe_name(counter_rfc, counter_name),
                "Días para cobrar": dias_para_cobrar,
                "Emitido total": emitido_total,
                "Total Cancelado Emitido": cancelado_emitido,
                "Porcentaje cancelado": pct_cancelado,
                "Descuentos": descuentos_emitidos,
                "Notas de crédito emitidas": notas_credito_emitidas,
                "Emitido por cobrar": emitido_por_cobrar,
                "Emitido Neto": emitido_neto,
                "Notas de crédito recibidas": notas_credito_recibidas,
                "Recibido Neto": recibido_neto,
                "PUE Recibido": pue_rec,
                "PPD Recibido": ppd_rec,
                "Conteo de PPD": conteo_ppd,
                "Monto pagado de PPD": monto_pagado_ppd,
                "Porcentaje pagado de PPD": pct_pagado_ppd,
                "Recibido por pagar": recibido_por_pagar,
            }

        # -------------------------
        # CLIENTES: base = receptores en emitidas
        # -------------------------
        clientes = []
        if not emit_df.empty:
            base = (
                emit_df[emit_df.get("type") == "I"]
                .groupby(["receiver_rfc", "receiver_name"], dropna=False)
                .size()
                .reset_index(name="_n")
            )
            for _, row in base.iterrows():
                crfc = str(row["receiver_rfc"]).strip().upper()
                cname = str(row["receiver_name"]).strip()
                if not crfc:
                    continue
                clientes.append(_agg_for_counterparty(crfc, cname))

        clientes_df = pd.DataFrame(clientes)

        # -------------------------
        # PROVEEDORES: base = emisores en recibidas
        # -------------------------
        proveedores = []
        if not rec_df.empty:
            base = (
                rec_df[rec_df.get("type") == "I"]
                .groupby(["issuer_rfc", "issuer_name"], dropna=False)
                .size()
                .reset_index(name="_n")
            )
            for _, row in base.iterrows():
                prfc = str(row["issuer_rfc"]).strip().upper()
                pname = str(row["issuer_name"]).strip()
                if not prfc:
                    continue
                proveedores.append(_agg_for_counterparty(prfc, pname))

        proveedores_df = pd.DataFrame(proveedores)

        # Orden bonito: por Emitido total (clientes) / Recibido Neto (proveedores) si existen
        if not clientes_df.empty and "Emitido total" in clientes_df.columns:
            clientes_df = clientes_df.sort_values("Emitido total", ascending=False).reset_index(drop=True)
        if not proveedores_df.empty and "Recibido Neto" in proveedores_df.columns:
            proveedores_df = proveedores_df.sort_values("Recibido Neto", ascending=False).reset_index(drop=True)

        # Tipos/formatos mínimos (sin formatear a $ aquí; eso lo haces en UI)
        for ddf in [clientes_df, proveedores_df]:
            if not ddf.empty:
                if "Días para cobrar" in ddf.columns:
                    ddf["Días para cobrar"] = pd.to_numeric(ddf["Días para cobrar"], errors="coerce").fillna(0.0).round(2)
                if "Porcentaje cancelado" in ddf.columns:
                    ddf["Porcentaje cancelado"] = pd.to_numeric(ddf["Porcentaje cancelado"], errors="coerce").fillna(0.0).round(2)
                if "Porcentaje pagado de PPD" in ddf.columns:
                    ddf["Porcentaje pagado de PPD"] = pd.to_numeric(ddf["Porcentaje pagado de PPD"], errors="coerce").fillna(0.0).round(2)

        return clientes_df, proveedores_df

    # -------------------------------------------------------------------------
    # EXISTENTE: fetch_syntage_xml (se conserva) + agrega tablas nuevas
    # -------------------------------------------------------------------------
    def fetch_syntage_xml(self, rfc: str, *, date_from: date | None, date_to: date | None):
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_emit = ex.submit(self.client.list_invoices, rfc, True, date_from, date_to)
            f_rec = ex.submit(self.client.list_invoices, rfc, False, date_from, date_to)
            emit = f_emit.result()
            rec = f_rec.result()

        emit_ids = self._extract_ids(emit)
        rec_ids = self._extract_ids(rec)

        def fetch_many(ids: List[str]) -> tuple[List[bytes], List[str]]:
            if not ids:
                return [], []

            max_w = min(self.max_workers, max(4, len(ids)))
            xmls_by_id: Dict[str, bytes] = {}
            fail_ids: List[str] = []

            # Pass 1
            with ThreadPoolExecutor(max_workers=max_w) as ex:
                futs = {ex.submit(self.client.get_cfdi_xml, i): i for i in ids}
                for fut in as_completed(futs):
                    i = futs[fut]
                    try:
                        content = fut.result() or b""
                    except Exception:
                        content = b""
                    if content and b"<" in content:
                        xmls_by_id[i] = content
                    else:
                        fail_ids.append(i)

            # Pass 2 suave
            if fail_ids:
                retry_ids = list(dict.fromkeys(fail_ids))
                fail_ids = []
                soft_w = min(4, max(1, len(retry_ids)))

                with ThreadPoolExecutor(max_workers=soft_w) as ex:
                    futs = {ex.submit(self.client.get_cfdi_xml, i): i for i in retry_ids}
                    for fut in as_completed(futs):
                        i = futs[fut]
                        try:
                            content = fut.result() or b""
                        except Exception:
                            content = b""
                        if content and b"<" in content:
                            xmls_by_id[i] = content
                        else:
                            fail_ids.append(i)

            xmls_ok = [xmls_by_id[i] for i in ids if i in xmls_by_id]
            return xmls_ok, fail_ids

        emit_xmls, emit_fail = fetch_many(emit_ids)
        rec_xmls, rec_fail = fetch_many(rec_ids)

        # ✅ [CHANGE] Usar parse_many_full para extraer pagos (CFDI tipo P)
        h1, c1, p1 = self.parser.parse_many_full(emit_xmls)  # ✅ bytes
        h2, c2, p2 = self.parser.parse_many_full(rec_xmls)   # ✅ bytes

        pagos_df = pd.concat([p1, p2], ignore_index=True) if isinstance(p1, pd.DataFrame) and isinstance(p2, pd.DataFrame) else pd.DataFrame()

        # ✅ NUEVO: construir tablas clientes/proveedores usando metadata de list_invoices + pagos
        clientes_df, proveedores_df = self._build_counterparty_tables(rfc, emit, rec, pagos_df=pagos_df)

        return {
            "ingresos": CfdiData(headers=h1, conceptos=c1),
            "egresos": CfdiData(headers=h2, conceptos=c2),

            # ✅ NUEVO: deja disponibles las tablas “financieras” por contraparte
            "clientes_df": clientes_df,
            "proveedores_df": proveedores_df,

            # ✅ NUEVO: por si quieres inspeccionar pagos después (no rompe nada si no lo usas)
            "pagos_df": pagos_df,

            # (opcional) por si quieres inspeccionar luego
            "emit_invoices_df": self._invoices_to_df(emit),
            "rec_invoices_df": self._invoices_to_df(rec),

            "meta": {
                "emit_listed": len(emit_ids),
                "emit_downloaded": len(emit_xmls),
                "emit_failed": len(emit_fail),
                "rec_listed": len(rec_ids),
                "rec_downloaded": len(rec_xmls),
                "rec_failed": len(rec_fail),
                "emit_headers": int(getattr(h1, "shape", (0, 0))[0]),
                "rec_headers": int(getattr(h2, "shape", (0, 0))[0]),
                "max_workers": int(self.max_workers),
            },
        }

    def fetch_local_xml(self, xml_dir: str | Path):
        p = Path(xml_dir)
        empty = CfdiData(headers=pd.DataFrame(), conceptos=pd.DataFrame())
        if not p.exists() or not p.is_dir():
            return {"ingresos": empty, "egresos": empty, "meta": {"mode": "local", "files": 0}}

        xml_blobs: List[bytes] = []
        files = sorted(list(p.glob("*.xml")))
        for fp in files:
            try:
                xml_blobs.append(fp.read_bytes())
            except Exception:
                continue

        h, c = self.parser.parse_many(xml_blobs)
        return {"ingresos": CfdiData(headers=h, conceptos=c), "egresos": empty, "meta": {"mode": "local", "files": len(files)}}

    def prodserv_summary_shiny(
        self,
        *,
        rfc: str,
        headers: pd.DataFrame,
        conceptos: pd.DataFrame,
        catalogo: pd.DataFrame,
        tipo: str,
        rol: str | None = None,
        top_n: int = 25,
    ) -> pd.DataFrame:
        out_cols = ["producto", "conteo", "monto"]

        if headers is None or headers.empty or conceptos is None or conceptos.empty:
            return pd.DataFrame(columns=out_cols)

        h = headers.copy()
        c = conceptos.copy()

        if "uuid" not in h.columns or "uuid" not in c.columns:
            return pd.DataFrame(columns=out_cols)

        needed_h = {"tipo", "emisor_rfc", "receptor_rfc"}
        if not needed_h.issubset(set(h.columns)):
            return pd.DataFrame(columns=out_cols)

        c["clave_prodserv"] = c.get("clave_prodserv").apply(normalize_clave8)
        c["importe"] = pd.to_numeric(c.get("importe"), errors="coerce").fillna(0.0)

        df = c.merge(h[["uuid", "tipo", "emisor_rfc", "receptor_rfc"]], on="uuid", how="left")

        df["tipo"] = df["tipo"].astype(str).str.upper().str.strip()
        df["emisor_rfc"] = df["emisor_rfc"].astype(str).str.upper().str.strip()
        df["receptor_rfc"] = df["receptor_rfc"].astype(str).str.upper().str.strip()
        rfc_app = str(rfc).upper().strip()

        df = df[df["tipo"] == str(tipo).upper()].copy()
        if df.empty:
            return pd.DataFrame(columns=out_cols)

        if rol is not None:
            rol_norm = str(rol).strip().lower()
            if rol_norm == "emisor":
                df = df[df["emisor_rfc"] == rfc_app].copy()
            elif rol_norm == "receptor":
                df = df[df["receptor_rfc"] == rfc_app].copy()
            else:
                return pd.DataFrame(columns=out_cols)

        df = df[df["clave_prodserv"].notna()].copy()
        if df.empty:
            return pd.DataFrame(columns=out_cols)

        agg = (
            df.groupby("clave_prodserv", dropna=False)
            .agg(conteo=("clave_prodserv", "size"), monto=("importe", "sum"))
            .reset_index()
            .sort_values("monto", ascending=False)
        )

        cat = catalogo.copy() if catalogo is not None else pd.DataFrame()
        if not cat.empty:
            cat = cat.rename(columns={cc: str(cc).strip().lower() for cc in cat.columns})
            if "clave_prodserv" not in cat.columns:
                cat = cat.rename(columns={cat.columns[0]: "clave_prodserv"})
            if "producto" not in cat.columns:
                for cc in cat.columns:
                    if cc in {"descripcion", "description", "desc"}:
                        cat = cat.rename(columns={cc: "producto"})
                        break
            if "producto" not in cat.columns and len(cat.columns) > 1:
                cat = cat.rename(columns={cat.columns[1]: "producto"})

            cat["clave_prodserv"] = cat["clave_prodserv"].apply(normalize_clave8)
            agg = agg.merge(cat[["clave_prodserv", "producto"]], on="clave_prodserv", how="left")
        else:
            agg["producto"] = None

        agg["producto"] = agg["producto"].fillna("SIN DESCRIPCIÓN / NO MATCH CATÁLOGO")

        if top_n and top_n > 0:
            agg = agg.head(int(top_n))

        return agg[out_cols]

# sat_service.py

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, Optional

from underwriting.domain.models import TaxStatus, EconomicActivity, TaxRegime
from underwriting.infrastructure.syntage_client import SyntageClient
import pandas as pd
from typing import Dict, Any


def _parse_iso_dt(s: Any) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_float_percentage(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _iter_members(raw: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Normaliza respuesta JSON-LD:
    - Si viene como colección Hydra: itera hydra:member
    - Si viene como objeto simple: itera solo ese objeto
    """
    if isinstance(raw, dict) and isinstance(raw.get("hydra:member"), list):
        for item in raw["hydra:member"]:
            if isinstance(item, dict):
                yield item
    else:
        if isinstance(raw, dict):
            yield raw


def _extract_tax_status_uuid(raw: Dict[str, Any]) -> str | None:
    iri = raw.get("@id") or ""
    if isinstance(iri, str) and "/tax-status/" in iri:
        uuid = iri.split("/tax-status/")[-1].strip("/")
        return uuid or None
    return None


class SatService:
    def __init__(self, client: SyntageClient):
        self.client = client

    # ✅ NUEVO: Mejor práctica (Service expone intención de negocio)
    def get_ciec_last_updated_at(self, rfc: str, *, tz: str = "America/Mexico_City") -> Optional[datetime]:
        raw = self.client.list_credentials(
            rfc=rfc,
            cred_type="ciec",
            items_per_page=50,
            order_updated_desc=True,
        )

        items = raw.get("hydra:member")
        if not isinstance(items, list) or not items:
            return None

        # Ordena por updatedAt/createdAt (si no viene updatedAt en la lista)
        def _key(x: Dict[str, Any]) -> str:
            return str(x.get("updatedAt") or x.get("createdAt") or "")

        items = [x for x in items if isinstance(x, dict)]
        items.sort(key=_key, reverse=True)
        cred = items[0] if items else None
        if not cred:
            return None

        updated = _parse_iso_dt(cred.get("updatedAt"))
        if updated is None:
            cred_id = cred.get("id")
            if cred_id:
                detail = self.client.get_credential(str(cred_id))
                updated = _parse_iso_dt(detail.get("updatedAt"))

        if updated is None:
            return None

        return updated.astimezone(ZoneInfo(tz))

    # ✅ TU MÉTODO EXISTENTE (sin cambiar la lógica)
    def get_tax_status(self, rfc: str) -> TaxStatus:
        raw: Dict[str, Any] = self.client.get_tax_status_by_rfc(rfc)

        members = list(_iter_members(raw))
        has_any_activities = any(
            (m.get("economicActivities") or (m.get("company") or {}).get("economicActivities") or []) for m in members
        )
        if not has_any_activities:
            uuid = _extract_tax_status_uuid(members[0]) if members else _extract_tax_status_uuid(raw)
            if uuid:
                raw = self.client.get_tax_status_by_uuid(uuid)
                members = list(_iter_members(raw))

        econ: list[EconomicActivity] = []
        regimes: list[TaxRegime] = []

        for m in members:
            activities_raw = (
                m.get("economicActivities")
                or (m.get("company") or {}).get("economicActivities")
                or (m.get("person") or {}).get("economicActivities")
                or (m.get("taxStatus") or {}).get("economicActivities")
                or []
            )

            regimes_raw = (
                m.get("taxRegimes")
                or (m.get("company") or {}).get("taxRegimes")
                or (m.get("person") or {}).get("taxRegimes")
                or (m.get("taxStatus") or {}).get("taxRegimes")
                or []
            )

            for a in activities_raw or []:
                if not isinstance(a, dict):
                    continue
                econ.append(
                    EconomicActivity(
                        name=a.get("name", ""),
                        order=int(a["order"]) if a.get("order") is not None else None,
                        percentage=_to_float_percentage(a.get("percentage")),
                        startDate=a.get("startDate") or a.get("startAt") or a.get("startedAt"),
                        endDate=a.get("endDate") or a.get("endAt"),
                    )
                )

            for r in regimes_raw or []:
                if not isinstance(r, dict):
                    continue
                regimes.append(
                    TaxRegime(
                        code=str(r.get("code")) if r.get("code") is not None else None,
                        name=r.get("name"),
                        startDate=r.get("startDate") or r.get("startAt"),
                        endDate=r.get("endDate") or r.get("endAt"),
                    )
                )

        def econ_key(x: EconomicActivity) -> tuple:
            return (x.order, x.name, x.startDate, x.endDate, x.percentage)

        def regime_key(x: TaxRegime) -> tuple:
            return (x.code, x.name, x.startDate, x.endDate)

        econ_dedup = list({econ_key(x): x for x in econ}.values())
        regimes_dedup = list({regime_key(x): x for x in regimes}.values())

        econ_sorted = sorted(econ_dedup, key=lambda x: (x.order is None, x.order))
        regimes_sorted = sorted(regimes_dedup, key=lambda x: (x.startDate is None, x.startDate))

        rfc_out = rfc
        status_out = None
        for m in members:
            if isinstance(m.get("rfc"), str) and m["rfc"].strip():
                rfc_out = m["rfc"].strip()
                break

        for m in members:
            if m.get("status") is not None:
                status_out = m.get("status")
                break

        return TaxStatus(
            rfc=rfc_out,
            status=status_out,
            economicActivities=econ_sorted,
            taxRegimes=regimes_sorted,
        )


    # Método para obtener empleados (JD)
    def get_employees_table(
        self,
        rfc: str,
        from_dt: Optional[str] = None,
        to_dt: Optional[str] = None,
        periodicity: str = "monthly",
    ) -> pd.DataFrame:
        """
        Devuelve DataFrame con columnas:
        - mes
        - numero_empleados
        """

        path = f"/insights/{rfc}/employees"

        params = {}
        if from_dt:
            params["options[from]"] = from_dt
        if to_dt:
            params["options[to]"] = to_dt
        if periodicity:
            params["options[periodicity]"] = periodicity

        response = self.client._get_json(path, params=params)

        data = response.get("data", [])

        if not data:
            return pd.DataFrame(columns=["mes", "numero_empleados"])

        rows = [
            {
                "mes": item.get("date"),
                "numero_empleados": item.get("total"),
            }
            for item in data
        ]

        df = pd.DataFrame(rows)
        df = df.sort_values("mes").reset_index(drop=True)

        return df
    




    def get_risk_indicators(self, rfc: str) -> Dict[str, Any]:
        """
        Devuelve únicamente los 6 KPIs de riesgo
        necesarios para la UI.
        """

        raw = self.client.get_risks(rfc)
        data = raw.get("data", {})

        def _safe_value(key: str, default=None):
            item = data.get(key)
            if not isinstance(item, dict):
                return default
            return item.get("value", default)

        def _safe_risky(key: str, default=False):
            item = data.get(key)
            if not isinstance(item, dict):
                return default
            return item.get("risky", default)

        return {
            # 1️⃣ Opinión de Cumplimiento
            "opinion_cumplimiento": _safe_value("taxCompliance"),
            # "opinion_cumplimiento_risky": _safe_risky("taxCompliance"),

            # 2️⃣ Estatus Lista Negra
            "estatus_lista_negra": _safe_value("blacklistStatus"),
            # "estatus_lista_negra_risky": _safe_risky("blacklistStatus"),

            # 3️⃣ Contrapartes en Lista Negra
            "contrapartes_lista_negra": _safe_value("blacklistedCounterparties", 0),
            # "contrapartes_lista_negra_risky": _safe_risky("blacklistedCounterparties"),

            # 4️⃣ Facturación Intercompañía
            "facturacion_intercompania": _safe_value("intercompanyTransactions", 0),
            # "facturacion_intercompania_risky": _safe_risky("intercompanyTransactions"),

            # 5️⃣ Cancelación Emitidas
            "cancelacion_emitidas_pct": _safe_value("canceledIssuedInvoices", 0.0),
            # "cancelacion_emitidas_risky": _safe_risky("canceledIssuedInvoices"),

            # 6️⃣ Cancelación Recibidas
            "cancelacion_recibidas_pct": _safe_value("canceledReceivedInvoices", 0.0),
            # "cancelacion_recibidas_risky": _safe_risky("canceledReceivedInvoices"),
        }

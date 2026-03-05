# Ruta: src/underwriting/application/cap_table_service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from underwriting.infrastructure.syntage_client import SyntageClient


@dataclass(frozen=True)
class CapTableResult:
    entity_id: str
    entity_iri: str
    cap_table: pd.DataFrame


class CapTableService:
    def __init__(self, client: SyntageClient):
        self.client = client

    def resolve_entity_id_by_rfc(self, rfc: str) -> str | None:
        """
        Busca en /entities filtrando taxpayer.id (RFC). Devuelve el UUID del entity.
        """
        r = (rfc or "").strip().upper()
        if not r:
            return None

        items = self.client.list_entities(taxpayer_id=r, items_per_page=50, max_pages=10)

        # ✅ [FIX] Normaliza: a veces el cliente puede devolver dict (Hydra) o basura en la lista
        if isinstance(items, dict):
            items = items.get("hydra:member") or items.get("member") or []
        if not isinstance(items, list):
            return None
        items = [it for it in items if isinstance(it, dict)]
        if not items:
            return None

        # Preferir match exacto si viene taxpayer.id en el item
        def _taxpayer_id(it: Dict[str, Any]) -> str:
            tx = it.get("taxpayer") or {}
            if not isinstance(tx, dict):
                tx = {}
            v = tx.get("id") or tx.get("rfc") or ""
            return str(v).strip().upper()

        exact = [it for it in items if _taxpayer_id(it) == r]
        chosen = exact[0] if exact else items[0]

        if not isinstance(chosen, dict):
            return None

        ent_id = chosen.get("id")
        if isinstance(ent_id, str) and ent_id.strip():
            return ent_id.strip()

        # fallback: si solo viene @id tipo "/entities/<uuid>"
        iri = chosen.get("@id")
        if isinstance(iri, str) and "/entities/" in iri:
            return iri.split("/entities/")[-1].strip("/")

        return None

    def get_cap_table_df(
        self,
        *,
        rfc: str,
        type_filter: str | None = None,
        name_filter: str | None = None,
        rfc_filter: str | None = None,
        items_per_page: int = 200,
        max_pages: int = 50,
    ) -> CapTableResult:
        """
        Devuelve DataFrame con acciones y % ownership (desde relations[]).
        """
        entity_id = self.resolve_entity_id_by_rfc(rfc)
        if not entity_id:
            return CapTableResult(entity_id="", entity_iri="", cap_table=pd.DataFrame())

        entity_iri = f"/entities/{entity_id}"

        shareholders = self.client.list_entity_shareholders(
            entity_id,
            items_per_page=items_per_page,
            max_pages=max_pages,
            type_filter=type_filter,
            name=name_filter,
            rfc=rfc_filter,
            order_name="asc",
        )

        # ✅ [FIX] Normaliza: a veces puede venir como Hydra collection dict
        if isinstance(shareholders, dict):
            shareholders = shareholders.get("hydra:member") or shareholders.get("member") or []
        if not isinstance(shareholders, list):
            shareholders = []

        rows: List[Dict[str, Any]] = []

        for sh in shareholders or []:
            if not isinstance(sh, dict):
                continue

            relations = sh.get("relations") or []
            rel_match: Dict[str, Any] | None = None

            if isinstance(relations, list):
                for rel in relations:
                    if not isinstance(rel, dict):
                        continue
                    # intenta varias llaves típicas
                    link = rel.get("entity") or rel.get("link") or rel.get("entityIri") or rel.get("entity_id")
                    link_s = str(link).strip() if link is not None else ""
                    if link_s == entity_iri or (entity_id and entity_id in link_s):
                        rel_match = rel
                        break

                # si no encontramos match, usa la primera (mejor que nada)
                if rel_match is None and relations:
                    first = relations[0]
                    rel_match = first if isinstance(first, dict) else None

            shares = None
            total_shares = None
            ownership = None

            if isinstance(rel_match, dict):
                shares = rel_match.get("shares")
                total_shares = rel_match.get("totalShares") or rel_match.get("total_shares")
                ownership = rel_match.get("ownership") or rel_match.get("ownershipPercentage") or rel_match.get("ownership_percentage")
            
            ownership_pct = None
            try:
                if ownership is not None:
                    ow = float(ownership)
                    ownership_pct = ow * 100.0 if ow <= 1.5 else ow
            except Exception:
                ownership_pct = None

            # ✅ Fallback: si no viene ownership, calcular con shares / totalShares
            try:
                if ownership_pct is None:
                    sh_ = float(shares) if shares is not None else None
                    ts = float(total_shares) if total_shares is not None else None
                    if sh_ is not None and ts is not None and ts > 0:
                        ownership_pct = (sh_ / ts) * 100.0
            except Exception:
                pass


            def _to_float(x):
                try:
                    if x is None:
                        return None
                    return float(x)
                except Exception:
                    return None

            rows.append(
                {
                    "Accionista": sh.get("name"),
                    "RFC": sh.get("rfc"),
                    "Tipo": sh.get("type"),
                    "Acciones": _to_float(shares),
                    "Acciones totales": _to_float(total_shares),
                    "% Ownership": _to_float(ownership_pct),
                    "Creado": sh.get("createdAt"),
                    "Actualizado": sh.get("updatedAt"),
                }
            )

        df = pd.DataFrame(rows)

        # Limpieza suave
        if not df.empty:
            for c in ["Accionista", "RFC", "Tipo"]:
                if c in df.columns:
                    df[c] = df[c].astype(str).fillna("").str.strip()

            for c in ["Acciones", "Acciones totales", "% Ownership"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            if "% Ownership" in df.columns:
                df = df.sort_values("% Ownership", ascending=False, na_position="last").reset_index(drop=True)

        return CapTableResult(entity_id=entity_id, entity_iri=entity_iri, cap_table=df)

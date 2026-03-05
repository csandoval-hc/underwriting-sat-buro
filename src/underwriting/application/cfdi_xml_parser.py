# Ruta: src/underwriting/application/cfdi_xml_parser.py
# Archivo: cfdi_xml_parser.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import math
import re
import xml.etree.ElementTree as ET

import pandas as pd


_DIGITS = re.compile(r"\D+")


def _to_float(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        try:
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                return 0.0
            return float(x)
        except Exception:
            return 0.0
    s = str(x).strip().replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def normalize_clave8(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        v = int(round(v))
    s = str(v).strip()
    if not s:
        return None

    try:
        if re.fullmatch(r"[-+]?\d+(\.\d+)?([eE][-+]?\d+)?", s):
            f = float(s)
            if not (math.isnan(f) or math.isinf(f)):
                s = str(int(round(f)))
    except Exception:
        pass

    s = _DIGITS.sub("", s)
    if not s:
        return None
    s = s[:8] if len(s) >= 8 else s.zfill(8)
    if s == "00000000":
        return None
    return s


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_by_local(root: ET.Element, local_name: str) -> ET.Element | None:
    for el in root.iter():
        if _local(el.tag).lower() == local_name.lower():
            return el
    return None


def _find_all_by_local(root: ET.Element, local_name: str) -> List[ET.Element]:
    out = []
    for el in root.iter():
        if _local(el.tag).lower() == local_name.lower():
            out.append(el)
    return out


@dataclass(frozen=True)
class CfdiXmlParser:
    """
    Parser CFDI (XML).
    IMPORTANTE: Nombres de columnas alineados a Shiny:
      headers: uuid, emisor_rfc, receptor_rfc, tipo, total, ...
      conceptos: uuid, clave_prodserv, importe, ...
    """

    def parse_one(self, xml_text: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """
        Compatibilidad: NO CAMBIAR firma.
        """
        h, it, _pay = self.parse_one_full(xml_text)
        return h, it

    def parse_one_full(self, xml_text: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        """
        NUEVO: además devuelve filas de pagos (para CFDI tipo P).
        """
        root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)

        comp = root if _local(root.tag).lower() == "comprobante" else _find_first_by_local(root, "Comprobante")
        if comp is None:
            return {}, [], []

        emisor = _find_first_by_local(root, "Emisor")
        receptor = _find_first_by_local(root, "Receptor")

        tfd = _find_first_by_local(root, "TimbreFiscalDigital")
        uuid = tfd.attrib.get("UUID") if tfd is not None else None

        tipo = (comp.attrib.get("TipoDeComprobante") or comp.attrib.get("tipoDeComprobante"))
        metodo_pago = (comp.attrib.get("MetodoPago") or comp.attrib.get("metodoPago"))
        descuento_comp = _to_float(comp.attrib.get("Descuento") or comp.attrib.get("descuento"))

        header = {
            "uuid": uuid,
            "fecha": comp.attrib.get("Fecha") or comp.attrib.get("fecha"),
            "subtotal": _to_float(comp.attrib.get("SubTotal") or comp.attrib.get("subTotal")),
            "total": _to_float(comp.attrib.get("Total") or comp.attrib.get("total")),
            "tipo": tipo,
            "metodo_pago": metodo_pago,                 # ✅ NUEVO (PUE/PPD)
            "descuento": descuento_comp,                # ✅ NUEVO (a nivel comprobante)
            "emisor_rfc": (emisor.attrib.get("Rfc") if emisor is not None else None),
            "receptor_rfc": (receptor.attrib.get("Rfc") if receptor is not None else None),
            "emisor_nombre": ((emisor.attrib.get("Nombre") or emisor.attrib.get("nombre")) if emisor is not None else None),
            "receptor_nombre": ((receptor.attrib.get("Nombre") or receptor.attrib.get("nombre")) if receptor is not None else None),
        }

        # ── Conceptos (incluye descuento por concepto si existe) ────────────────
        conceptos = _find_all_by_local(root, "Concepto")
        items: list[dict[str, Any]] = []
        for c in conceptos:
            clave_raw = c.attrib.get("ClaveProdServ") or c.attrib.get("claveProdServ")
            items.append(
                {
                    "uuid": uuid,
                    "clave_prodserv": normalize_clave8(clave_raw),
                    "descripcion": c.attrib.get("Descripcion") or c.attrib.get("descripcion"),
                    "importe": _to_float(c.attrib.get("Importe") or c.attrib.get("importe")),
                    "descuento": _to_float(c.attrib.get("Descuento") or c.attrib.get("descuento")),  # ✅ NUEVO
                }
            )

        # ── Pagos (CFDI tipo P) - Complemento Pagos 2.0 ────────────────────────
        pagos_rows: list[dict[str, Any]] = []
        try:
            if str(tipo).strip().upper() == "P":
                # buscamos nodos Pago y dentro DoctoRelacionado (sin depender del namespace)
                pagos = _find_all_by_local(root, "Pago")
                for p in pagos:
                    fecha_pago = p.attrib.get("FechaPago") or p.attrib.get("fechaPago")
                    monto_pago = _to_float(p.attrib.get("Monto") or p.attrib.get("monto"))
                    moneda_p = p.attrib.get("MonedaP") or p.attrib.get("monedaP") or p.attrib.get("Moneda") or p.attrib.get("moneda")

                    doctos = []
                    # DoctoRelacionado puede venir dentro del mismo 'Pago'
                    for dr in p.iter():
                        if _local(dr.tag).lower() == "doctorelacionado":
                            doctos.append(dr)

                    for dr in doctos:
                        id_doc = dr.attrib.get("IdDocumento") or dr.attrib.get("idDocumento")
                        imp_pagado = _to_float(dr.attrib.get("ImpPagado") or dr.attrib.get("impPagado"))
                        imp_saldo_ins = _to_float(dr.attrib.get("ImpSaldoInsoluto") or dr.attrib.get("impSaldoInsoluto"))
                        imp_saldo_ant = _to_float(dr.attrib.get("ImpSaldoAnt") or dr.attrib.get("impSaldoAnt"))

                        pagos_rows.append(
                            {
                                "uuid_pago": uuid,                 # UUID del CFDI P
                                "fecha_pago": fecha_pago,
                                "monto_pago": monto_pago,
                                "moneda_p": moneda_p,
                                "uuid_factura": id_doc,           # UUID de la factura relacionada
                                "imp_pagado": imp_pagado,
                                "imp_saldo_ant": imp_saldo_ant,
                                "imp_saldo_insoluto": imp_saldo_ins,
                            }
                        )
        except Exception:
            # no rompemos el parseo por pagos
            pagos_rows = pagos_rows or []

        return header, items, pagos_rows

    def parse_many(self, xml_texts: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compatibilidad: NO CAMBIAR firma.
        """
        h, c, _p = self.parse_many_full(xml_texts)
        return h, c

    def parse_many_full(self, xml_texts: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        NUEVO: devuelve (headers, conceptos, pagos)
        """
        headers: list[dict[str, Any]] = []
        conceptos: list[dict[str, Any]] = []
        pagos: list[dict[str, Any]] = []

        for x in xml_texts:
            try:
                h, it, pay = self.parse_one_full(x)
                if h:
                    headers.append(h)
                conceptos.extend(it)
                pagos.extend(pay)
            except Exception:
                continue

        return pd.DataFrame(headers), pd.DataFrame(conceptos), pd.DataFrame(pagos)

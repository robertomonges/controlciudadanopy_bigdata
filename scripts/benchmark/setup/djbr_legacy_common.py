from __future__ import annotations

import os
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DEFAULT_OFFICIAL_JSON_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/json")
DEFAULT_LEGACY_JSON_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/json_legacy_v1")


@dataclass
class LegacyJsonRecord:
    source_path: Path
    target_path: Path
    payload: dict[str, Any]


def parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, list):
        return None

    text = str(value).strip()
    if not text or text.lower() == "null":
        return None

    cleaned = re.sub(r"[^0-9,.\-]", "", text)
    if not cleaned or cleaned in {"-", ".", ",", "-.", "-,"}:
        return None

    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def parse_filename_metadata(path: Path) -> dict[str, str | None]:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) < 3:
        return {
            "source_file_name": path.name,
            "id_cabecera_djb": None,
            "document": None,
            "remote_id": None,
        }
    return {
        "source_file_name": path.name,
        "id_cabecera_djb": parts[0] or None,
        "document": parts[1] or None,
        "remote_id": parts[2] or None,
    }


def sum_portal_items(items: Any) -> Decimal | None:
    if items is None:
        return None
    if isinstance(items, list):
        total = Decimal("0")
        found = False
        for item in items:
            if isinstance(item, dict):
                number = parse_decimal(item.get("importe"))
            else:
                number = parse_decimal(item)
            if number is not None:
                total += number
                found = True
        return total if found else None
    return parse_decimal(items)


def map_instituciones(detalle_funcionarios: Any) -> list[dict[str, Any]]:
    if not isinstance(detalle_funcionarios, list):
        return []

    instituciones: list[dict[str, Any]] = []
    for item in detalle_funcionarios:
        if not isinstance(item, dict):
            continue
        instituciones.append(
            {
                "cargo": item.get("cargo"),
                "institucion": item.get("institucionActual"),
                "dependencia": item.get("dependencia"),
                "tipoCargo": item.get("tipoCargo"),
                "categoria": item.get("categoria"),
                "fechaNombramiento": item.get("fechaNombramiento"),
                "fechaMotivoPresentacion": item.get("fechaMotivoPres"),
                "direccionLaboral": item.get("direccionLaboral"),
            }
        )
    return instituciones


def normalize_portal_json_to_legacy(portal_payload: dict[str, Any], source_path: Path) -> dict[str, Any]:
    meta = parse_filename_metadata(source_path)

    activos = parse_decimal(portal_payload.get("totalActivo"))
    pasivos = parse_decimal(portal_payload.get("totalPasivo"))
    patrimonio = parse_decimal(portal_payload.get("patrimonioNeto"))
    if patrimonio is None and activos is not None and pasivos is not None:
        patrimonio = activos - pasivos

    ingresos_mensual = sum_portal_items(portal_payload.get("ingresosMensuales"))
    ingresos_anual = sum_portal_items(portal_payload.get("ingresosAnuales"))
    egresos_mensual = sum_portal_items(portal_payload.get("egresosMensuales"))
    egresos_anual = sum_portal_items(portal_payload.get("egresosAnuales"))

    fecha = (
        portal_payload.get("fecha")
        or portal_payload.get("fechaConfirmacion")
        or portal_payload.get("fechaCreacion")
        or portal_payload.get("fechaCorte")
    )

    return {
        "__meta__": {
            **meta,
            "source_shape": "PORTALDJBR_JSON_V1",
            "canonical_shape": "DJBR_CANONICAL_LEGACY_V1",
        },
        "cedula": portal_payload.get("cedula") or meta["document"],
        "nombres": portal_payload.get("nombres"),
        "apellidos": portal_payload.get("apellidos"),
        "fecha": fecha,
        "fechaConfirmacion": portal_payload.get("fechaConfirmacion"),
        "fechaCreacion": portal_payload.get("fechaCreacion"),
        "fechaCorte": portal_payload.get("fechaCorte"),
        "idCabeceraDjb": portal_payload.get("idCabeceraDjb") or meta["id_cabecera_djb"],
        "versionDjb": portal_payload.get("versionDjb"),
        "activos": str(activos) if activos is not None else None,
        "pasivos": str(pasivos) if pasivos is not None else None,
        "patrimonioNeto": str(patrimonio) if patrimonio is not None else None,
        "resumen": {
            "totalActivo": str(activos) if activos is not None else None,
            "totalPasivo": str(pasivos) if pasivos is not None else None,
            "patrimonioNeto": str(patrimonio) if patrimonio is not None else None,
        },
        "ingresosMensual": str(ingresos_mensual) if ingresos_mensual is not None else None,
        "ingresosAnual": str(ingresos_anual) if ingresos_anual is not None else None,
        "egresosMensual": str(egresos_mensual) if egresos_mensual is not None else None,
        "egresosAnual": str(egresos_anual) if egresos_anual is not None else None,
        "instituciones": map_instituciones(portal_payload.get("detalleFuncionarios")),
        "detalleFuncionarios": portal_payload.get("detalleFuncionarios") or [],
        "muebles": portal_payload.get("muebles") or [],
        "inmuebles": portal_payload.get("inmuebles") or [],
        "vehiculos": portal_payload.get("vehiculos") or [],
        "deudas": portal_payload.get("deudas") or [],
        "depositos": portal_payload.get("depositos") or [],
        "cuentasXCobrar": portal_payload.get("cuentasXCobrar") or [],
        "otrosActivos": portal_payload.get("otrosActivos") or [],
        "actividadesAgro": portal_payload.get("actividadesAgro") or [],
        "buquesAeros": portal_payload.get("buquesAeros") or [],
        "efectivo": portal_payload.get("efectivo"),
        "descEstado": portal_payload.get("descEstado"),
        "descEstadoCivil": portal_payload.get("descEstadoCivil"),
        "descNacionalidad": portal_payload.get("descNacionalidad"),
        "descGradoAcademico": portal_payload.get("descGradoAcademico"),
        "descTituloObtenido": portal_payload.get("descTituloObtenido"),
        "telefonoDomicilio": portal_payload.get("telefonoDomicilio"),
        "correoUsuario": portal_payload.get("correoUsuario"),
        "raw_portal_json": portal_payload,
    }


def extract_charge(payload: dict[str, Any]) -> str | None:
    instituciones = payload.get("instituciones") or []
    if not instituciones:
        return None
    first = instituciones[0]
    cargo = (first.get("cargo") or "").strip() if isinstance(first, dict) else ""
    institucion = (first.get("institucion") or "").strip() if isinstance(first, dict) else ""
    if cargo and institucion:
        return f"{cargo} ({institucion})"
    return cargo or institucion or None


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    variants = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for fmt in variants:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def legacy_payload_to_pg_row(payload: dict[str, Any], raw_data_id: int | None) -> dict[str, Any]:
    resumen = payload.get("resumen") or {}
    return {
        "raw_data_id": raw_data_id,
        "charge": extract_charge(payload),
        "active": parse_decimal(resumen.get("totalActivo") or payload.get("activos")),
        "passive": parse_decimal(resumen.get("totalPasivo") or payload.get("pasivos")),
        "net_worth": parse_decimal(resumen.get("patrimonioNeto") or payload.get("patrimonioNeto")),
        "monthly_income": parse_decimal(payload.get("ingresosMensual")),
        "anual_income": parse_decimal(payload.get("ingresosAnual")),
        "monthly_expenses": parse_decimal(payload.get("egresosMensual")),
        "anual_expenses": parse_decimal(payload.get("egresosAnual")),
        "declaration_date": parse_timestamp(payload.get("fecha") or payload.get("fechaConfirmacion") or payload.get("fechaCreacion")),
        "source": "DJBR_CANONICAL_LEGACY_V1",
        "parsed": json.dumps(payload, ensure_ascii=False),
    }


def official_payload_to_pg_row(portal_payload: dict[str, Any], source_path: Path, raw_data_id: int | None) -> dict[str, Any]:
    active = parse_decimal(portal_payload.get("totalActivo"))
    passive = parse_decimal(portal_payload.get("totalPasivo"))
    net_worth = parse_decimal(portal_payload.get("patrimonioNeto"))
    if net_worth is None and active is not None and passive is not None:
        net_worth = active - passive

    detalles = portal_payload.get("detalleFuncionarios") or []
    charge = None
    if detalles and isinstance(detalles[0], dict):
        first = detalles[0]
        cargo = (first.get("cargo") or "").strip()
        institucion = (first.get("institucionActual") or "").strip()
        if cargo and institucion:
            charge = f"{cargo} ({institucion})"
        else:
            charge = cargo or institucion or None

    declaration_date = parse_timestamp(
        portal_payload.get("fecha")
        or portal_payload.get("fechaConfirmacion")
        or portal_payload.get("fechaCreacion")
        or portal_payload.get("fechaCorte")
    )

    return {
        "raw_data_id": raw_data_id,
        "charge": charge,
        "active": active,
        "passive": passive,
        "net_worth": net_worth,
        "monthly_income": sum_portal_items(portal_payload.get("ingresosMensuales")),
        "anual_income": sum_portal_items(portal_payload.get("ingresosAnuales")),
        "monthly_expenses": sum_portal_items(portal_payload.get("egresosMensuales")),
        "anual_expenses": sum_portal_items(portal_payload.get("egresosAnuales")),
        "declaration_date": declaration_date,
        "source": "PORTALDJBR_JSON",
        "parsed": json.dumps(portal_payload, ensure_ascii=False),
    }


def iter_official_json_files(source_dir: Path, sorted_order: bool = True):
    import os
    if sorted_order:
        for path in sorted(source_dir.glob("*.json")):
            yield path
    else:
        with os.scandir(source_dir) as it:
            for entry in it:
                if entry.name.endswith(".json") and entry.is_file():
                    yield Path(entry.path)


def iter_legacy_json_files(source_dir: Path):
    for path in sorted(source_dir.glob("*.json")):
        yield path

"""APP-2c - previsualisation d'ecriture HH sur copies uniquement."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any

import openpyxl

import app.config as cfg
from app.readers.saisie_hh_reader import (
    MANUAL_COL_MAP,
    find_first_empty_data_row,
    _col_index,
)
from app.services import saisie_hh_service as saisie_svc
from app.services.saisie_hh_schema_migration import (
    NEW_SAISIE_FIELDS,
    migrate_ref_setup_copy,
    migrate_saisie_copy,
)
from app.writers.saisie_hh_writer import (
    _check_formula_cells,
    _check_structural_preservation,
    _measure_structure,
)


DRYRUNS_DIR = cfg.DATA_DIR / "dryruns"
SAISIE_COPY_NAME = "SAISIE_ReservationsHorsHostaway_copie.xlsx"
REF_COPY_NAME = "REF_Setup_copie.xlsm"
MASTER_SIM_NAME = "MASTER_FACT_MAN_ReservationsHorsHostaway_simule.xlsx"
RESULT_LOT4A_NAME = "resultat_lot4a.json"
MANIFEST_NAME = "manifest.json"


class DryRunError(RuntimeError):
    """Erreur bloquante de simulation APP-2c."""


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _fingerprint(path: Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"exists": False}
    stat = p.stat()
    return {
        "exists": True,
        "sha256": _sha256(p),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _token() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{uuid.uuid4().hex[:12]}"


def _safe_token(token: str) -> str:
    clean = str(token).strip()
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-TZ")
    if not clean or any(ch not in allowed for ch in clean):
        raise DryRunError("Identifiant de previsualisation invalide")
    return clean


def _load_lot4a():
    root = cfg.PROJECT_ROOT / "02_TRAVAIL"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import lib_lot4a_reservations_hh as lot4a  # type: ignore
    return lot4a


def _copy_source(source: Path, destination: Path) -> None:
    if not source.exists():
        raise DryRunError(f"Source introuvable pour simulation: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _normalise_cell_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return "OUI" if value else ""
    return value


def _build_simulated_row(preview: dict[str, Any]) -> dict[str, Any]:
    row = saisie_svc.build_row_data(preview)
    for field in NEW_SAISIE_FIELDS:
        value = preview.get(field)
        if field.startswith("confirmation_override_"):
            value = "OUI" if value else ""
        row[field] = _normalise_cell_value(value)
    return row


def _write_simulated_row(saisie_copy: Path, row_data: dict[str, Any]) -> tuple[int, list[str]]:
    target_row = find_first_empty_data_row(saisie_copy)
    if target_row is None:
        raise DryRunError("Aucune ligne libre dans la copie SAISIE")

    formula_violations = _check_formula_cells(saisie_copy, target_row)
    if formula_violations:
        raise DryRunError("; ".join(formula_violations))

    struct_avant = _measure_structure(saisie_copy)
    wb = openpyxl.load_workbook(str(saisie_copy), data_only=False)
    try:
        ws = wb["SAISIE"]
        headers = [str(cell.value or "").strip() for cell in ws[1]]
        header_idx = {name: idx + 1 for idx, name in enumerate(headers) if name}

        for col_letter, field_name in MANUAL_COL_MAP.items():
            if field_name in row_data:
                ws.cell(
                    row=target_row,
                    column=_col_index(col_letter),
                    value=_normalise_cell_value(row_data[field_name]),
                )
        for field in NEW_SAISIE_FIELDS:
            col = header_idx.get(field)
            if col is not None:
                ws.cell(row=target_row, column=col, value=_normalise_cell_value(row_data.get(field)))

        wb.calculation.fullCalcOnLoad = True
        wb.save(str(saisie_copy))
    finally:
        wb.close()

    structure_violations = _check_structural_preservation(saisie_copy, saisie_copy, struct_avant)
    structure_violations.extend(_check_formula_cells(saisie_copy, target_row))
    if structure_violations:
        raise DryRunError("; ".join(structure_violations))
    return target_row, structure_violations


def _write_master_simulation(path: Path, lot4a: Any, result: dict[str, Any]) -> None:
    wb = openpyxl.Workbook()
    ws_master = wb.active
    ws_master.title = "MASTER"
    ws_vue = wb.create_sheet("VUE_ACTIVE")
    for ws, rows in (
        (ws_master, result.get("master_rows", [])),
        (ws_vue, result.get("vue_active_rows", [])),
    ):
        ws.append(list(lot4a.MASTER_COLUMNS))
        for rec in rows:
            ws.append([rec.get(col, "") for col in lot4a.MASTER_COLUMNS])
    wb.save(str(path))
    wb.close()


def _count_acomptes(rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        value = row.get("acompte_facture")
        if value not in (None, ""):
            count += 1
    return count


def _lot4a_result(
    lot4a: Any,
    saisie_path: Path,
    ref_path: Path,
    as_of_iso: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    saisie_rows = lot4a.read_saisie_values(saisie_path)
    taux_rows = lot4a.read_taux_rows(ref_path)
    return saisie_rows, lot4a.build_master(saisie_rows, taux_rows, as_of_iso)


def _find_row(rows: list[dict[str, Any]], pk: str) -> dict[str, Any] | None:
    for row in rows:
        if str(row.get("reservation_hh_id", "")).strip() == pk:
            return row
    return None


def _comparatif(
    pk: str,
    saisie_before: list[dict[str, Any]],
    saisie_after: list[dict[str, Any]],
    lot4a_before: dict[str, Any],
    lot4a_after: dict[str, Any],
) -> list[dict[str, Any]]:
    master_before = lot4a_before.get("master_rows", [])
    master_after = lot4a_after.get("master_rows", [])
    return [
        {"element": "Lignes SAISIE HH", "avant": len(saisie_before), "apres": len(saisie_after)},
        {"element": "Lignes MASTER HH", "avant": len(master_before), "apres": len(master_after)},
        {
            "element": "Reservation HH simulee",
            "avant": "Presente" if _find_row(saisie_before, pk) else "Absente",
            "apres": "Presente" if _find_row(saisie_after, pk) else "Absente",
        },
        {
            "element": "Anomalies taux",
            "avant": len(lot4a_before.get("anomalies_taux", [])),
            "apres": len(lot4a_after.get("anomalies_taux", [])),
        },
        {
            "element": "Acomptes crees",
            "avant": _count_acomptes(master_before),
            "apres": _count_acomptes(master_after),
        },
    ]


def _payload_summary(preview: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "reservation_hh_id", "logement_id", "proprietaire_id", "canal_id",
        "date_arrivee", "date_depart", "total_percu", "mode_paiement_id",
        "code_impact", "comptabilisation", "taux_commission_override",
        "menage_override",
    ]
    return {field: preview.get(field) for field in fields}


def run_previsualisation(
    form_data: dict[str, str],
    *,
    saisie_source: Path | None = None,
    ref_setup_source: Path | None = None,
    dryruns_root: Path | None = None,
    as_of_iso: str | None = None,
) -> dict[str, Any]:
    """Valide et execute une simulation complete sur copies.

    La fonction ne contourne pas `HH_REAL_WRITE_ENABLED` : elle n'appelle jamais
    le writer reel et n'ecrit que dans le sous-dossier de dry-run.
    """
    source_saisie = Path(saisie_source or cfg.SAISIE_RESERVATIONS_HH)
    source_ref = Path(ref_setup_source or cfg.REF_SETUP)
    root = Path(dryruns_root or DRYRUNS_DIR)
    simulation_id = _token()
    run_dir = root / simulation_id
    run_dir.mkdir(parents=True, exist_ok=False)

    validation = saisie_svc.valider(
        form_data,
        saisie_path=source_saisie,
        ref_setup_path=source_ref,
    )
    if not validation["ok"]:
        manifest = {
            "simulation_id": simulation_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "app_version": "APP-2c",
            "hh_real_write_enabled": cfg.HH_REAL_WRITE_ENABLED,
            "status": "VALIDATION_REFUSEE",
            "errors": validation["erreurs"],
            "payload_summary": {},
        }
        (run_dir / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )
        return {"ok": False, "token": simulation_id, "run_dir": run_dir, "manifest": manifest}

    preview = validation["preview"]
    pk = validation["pk"]
    saisie_copy = run_dir / SAISIE_COPY_NAME
    ref_copy = run_dir / REF_COPY_NAME
    master_sim = run_dir / MASTER_SIM_NAME
    result_lot4a_path = run_dir / RESULT_LOT4A_NAME
    now_iso = as_of_iso or datetime.now(timezone.utc).isoformat()

    source_hashes = {
        "saisie": _fingerprint(source_saisie),
        "ref_setup": _fingerprint(source_ref),
    }
    errors: list[str] = []
    status = "OK"
    target_row: int | None = None
    added_fields: list[str] = []
    ref_migrated = False
    lot4a_before: dict[str, Any] = {}
    lot4a_after: dict[str, Any] = {}
    saisie_before: list[dict[str, Any]] = []
    saisie_after: list[dict[str, Any]] = []
    simulated_master_row: dict[str, Any] | None = None

    try:
        _copy_source(source_saisie, saisie_copy)
        _copy_source(source_ref, ref_copy)
        added_fields = migrate_saisie_copy(saisie_copy)
        ref_migrated = migrate_ref_setup_copy(ref_copy)

        lot4a = _load_lot4a()
        saisie_before, lot4a_before = _lot4a_result(lot4a, saisie_copy, ref_copy, now_iso)
        row_data = _build_simulated_row(preview)
        target_row, _ = _write_simulated_row(saisie_copy, row_data)
        saisie_after, lot4a_after = _lot4a_result(lot4a, saisie_copy, ref_copy, now_iso)
        simulated_master_row = _find_row(lot4a_after.get("master_rows", []), pk)
        _write_master_simulation(master_sim, lot4a, lot4a_after)
        if lot4a_after.get("statut") != "ANALYSE_TERMINEE":
            status = "ERREUR_LOT4A"
            errors.append(str(lot4a_after.get("motif_blocage") or lot4a_after.get("statut")))
    except Exception as exc:
        status = "ERREUR"
        errors.append(str(exc))

    resultat_lot4a = {
        "before": lot4a_before,
        "after": lot4a_after,
        "reservation_simulee": simulated_master_row,
        "status": status,
        "errors": errors,
    }
    result_lot4a_path.write_text(
        json.dumps(resultat_lot4a, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    copy_hashes = {
        "saisie": _fingerprint(saisie_copy),
        "ref_setup": _fingerprint(ref_copy),
        "master_simule": _fingerprint(master_sim),
        "resultat_lot4a": _fingerprint(result_lot4a_path),
    }
    comparatif = _comparatif(pk, saisie_before, saisie_after, lot4a_before, lot4a_after)
    manifest = {
        "simulation_id": simulation_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "app_version": "APP-2c",
        "hh_real_write_enabled": cfg.HH_REAL_WRITE_ENABLED,
        "source_hashes": source_hashes,
        "copy_hashes": copy_hashes,
        "paths": {
            "run_dir": str(run_dir),
            "saisie_copy": str(saisie_copy),
            "ref_setup_copy": str(ref_copy),
            "master_simule": str(master_sim),
            "resultat_lot4a": str(result_lot4a_path),
        },
        "payload_summary": _payload_summary(preview),
        "migration": {
            "saisie_fields_added": added_fields,
            "ref_direct_proprietaire_added": ref_migrated,
        },
        "target_row": target_row,
        "status": status,
        "errors": errors,
        "comparatif": comparatif,
        "lot4a_status": lot4a_after.get("statut"),
    }
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    return {
        "ok": status == "OK",
        "token": simulation_id,
        "run_dir": run_dir,
        "manifest": manifest,
        "preview": preview,
        "pk": pk,
        "target_row": target_row,
        "saisie_copy": saisie_copy,
        "ref_copy": ref_copy,
        "master_simule": master_sim,
        "resultat_lot4a": resultat_lot4a,
        "simulated_master_row": simulated_master_row,
        "comparatif": comparatif,
    }


def load_previsualisation(token: str, *, dryruns_root: Path | None = None) -> dict[str, Any]:
    simulation_id = _safe_token(token)
    root = Path(dryruns_root or DRYRUNS_DIR)
    run_dir = root / simulation_id
    manifest_path = run_dir / MANIFEST_NAME
    result_path = run_dir / RESULT_LOT4A_NAME
    if not manifest_path.exists():
        raise DryRunError("Previsualisation introuvable")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    resultat_lot4a = {}
    if result_path.exists():
        resultat_lot4a = json.loads(result_path.read_text(encoding="utf-8"))
    return {
        "token": simulation_id,
        "run_dir": run_dir,
        "manifest": manifest,
        "resultat_lot4a": resultat_lot4a,
        "preview": manifest.get("payload_summary", {}),
        "simulated_master_row": resultat_lot4a.get("reservation_simulee"),
        "comparatif": manifest.get("comparatif", []),
    }

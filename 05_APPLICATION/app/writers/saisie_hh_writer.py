"""Écriture atomique SAISIE HH — opérations fichier uniquement (APP-2b).

Aucun accès SQLite dans ce module. Toute journalisation, guard et rollback
sont délégués à app.services.saisie_hh_orchestrator.

Étapes de write_row :
1. Détecter ~$<fichier> (Excel ouvert) → refus.
2. Acquérir verrou exclusif (.lock atomique).
3. assert_writable.
4. sha256 avant.
5. Préflight formules sur ligne cible (B/C/K/N/O/Q/V/Y/Z : figée → ERREUR).
6. Mesure structure avant (plages, DV, MFC, feuilles, fullCalcOnLoad).
7. Copier vers temp sur même volume.
8. Écrire sur copie (openpyxl data_only=False, fullCalcOnLoad=True forcé).
9. Revalider valeurs écrites.
10. Vérifier delta cellulaire (seulement 21 cols manuelles de la ligne cible).
11. Vérifier préservation structurelle.
12. os.replace(temp → cible).
13. sha256 après.
14. Libérer verrou (toujours, même en erreur).
"""
from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg
from app.readers.saisie_hh_reader import (
    FORMULA_COLS,
    MANUAL_COL_MAP,
    _col_index,
    _col_letter,
)
from app.services.file_registry import assert_writable
from app.services.saisie_hh_schema_migration import NEW_SAISIE_FIELDS

SHEET_SAISIE = "SAISIE"
_LOCK_NAME = "SAISIE_HH_APP2B.lock"
FORMULE_LIGNE_MODELE_ABSENTE = "FORMULE_LIGNE_MODELE_ABSENTE"
FORMULE_LIGNE_MODELE_FIGEE = "FORMULE_LIGNE_MODELE_FIGEE"


# ── Utilitaires ────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Verrou et détection Excel ──────────────────────────────────────────────────

def _detect_excel_lock(saisie_path: Path) -> bool:
    """True si Excel a le fichier ouvert (~$<nom> présent dans le même dossier)."""
    return (saisie_path.parent / ("~$" + saisie_path.name)).exists()


def _acquire_write_lock(lock_path: Path) -> bool:
    """Création atomique du fichier lock (O_CREAT | O_EXCL). True si acquis."""
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except (FileExistsError, OSError):
        return False


def _release_write_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _same_volume(p1: Path, p2: Path) -> bool:
    if sys.platform == "win32":
        try:
            import ctypes
            buf = ctypes.create_unicode_buffer(260)
            ctypes.windll.kernel32.GetVolumePathNameW(str(p1), buf, 260)
            v1 = buf.value
            ctypes.windll.kernel32.GetVolumePathNameW(str(p2), buf, 260)
            return v1 == buf.value
        except Exception:
            return str(p1)[:3].upper() == str(p2)[:3].upper()
    try:
        return os.stat(p1).st_dev == os.stat(p2).st_dev
    except Exception:
        return True


# ── Préflight formules ─────────────────────────────────────────────────────────

def _check_formula_cells(saisie_path: Path, target_row: int) -> list[str]:
    """Verifie que les 9 formules de la ligne cible sont presentes et actives."""
    violations: list[str] = []
    try:
        wb = openpyxl.load_workbook(str(saisie_path), read_only=False, data_only=False)
        ws = wb[SHEET_SAISIE]
        for col_letter in sorted(FORMULA_COLS):
            v = ws.cell(row=target_row, column=_col_index(col_letter)).value
            if isinstance(v, str) and v.startswith("="):
                continue
            if v is None or (isinstance(v, str) and v.strip() == ""):
                violations.append(
                    f"{FORMULE_LIGNE_MODELE_ABSENTE}: col {col_letter} ligne {target_row}"
                )
                continue
            violations.append(
                f"{FORMULE_LIGNE_MODELE_FIGEE}: col {col_letter} ligne {target_row} "
                f"valeur {v!r} (attendu formule texte commencant par '=')"
            )
        wb.close()
    except Exception as exc:
        violations.append(f"Lecture preflight formules impossible : {exc}")
    return violations


# ── Structure ──────────────────────────────────────────────────────────────────

def _defined_names_signature(wb: Any) -> tuple:
    items = []
    for dn in wb.defined_names.values():
        try:
            destinations = tuple(sorted((str(s), str(r)) for s, r in dn.destinations))
        except Exception:
            destinations = ()
        items.append((
            str(getattr(dn, "name", "")),
            str(getattr(dn, "attr_text", "")),
            destinations,
            str(getattr(dn, "scope", "")),
        ))
    return tuple(sorted(items))


def _data_validations_signature(wb: Any) -> tuple:
    items = []
    for ws in wb.worksheets:
        for dv in ws.data_validations.dataValidation:
            items.append((
                ws.title,
                str(dv.sqref),
                str(dv.type),
                str(dv.operator),
                str(dv.formula1),
                str(dv.formula2),
                bool(dv.allowBlank),
                bool(dv.showErrorMessage),
                bool(dv.showInputMessage),
                str(dv.errorStyle),
            ))
    return tuple(sorted(items))


def _mfc_signature(wb: Any) -> tuple:
    items = []
    for ws in wb.worksheets:
        for sqref, rules in ws.conditional_formatting._cf_rules.items():
            for rule in rules:
                items.append((
                    ws.title,
                    str(sqref),
                    str(getattr(rule, "type", "")),
                    tuple(str(f) for f in (getattr(rule, "formula", None) or [])),
                    getattr(rule, "priority", None),
                    bool(getattr(rule, "stopIfTrue", False)),
                    str(getattr(rule, "operator", "")),
                    str(getattr(rule, "dxfId", "")),
                    str(getattr(rule, "rank", "")),
                    str(getattr(rule, "percent", "")),
                ))
    return tuple(sorted(items))


def _tables_signature(wb: Any) -> tuple:
    items = []
    for ws in wb.worksheets:
        for table in ws.tables.values():
            items.append((ws.title, str(table.name), str(table.ref)))
    return tuple(sorted(items))


def _protections_signature(wb: Any) -> tuple:
    sheet_items = tuple(
        (ws.title, bool(ws.protection.sheet), str(ws.protection.password))
        for ws in wb.worksheets
    )
    book = wb.security
    workbook_item = (
        bool(getattr(book, "lockStructure", False)),
        bool(getattr(book, "lockWindows", False)),
        str(getattr(book, "workbookPassword", None)),
    )
    return sheet_items, workbook_item


def _external_links_signature(wb: Any) -> tuple:
    links = getattr(wb, "_external_links", []) or []
    return tuple(sorted(str(getattr(link, "file_link", link)) for link in links))


def _structure_signature(wb_path: Path) -> dict[str, Any]:
    wb = openpyxl.load_workbook(str(wb_path), read_only=False, data_only=False)
    try:
        return {
            "sheetnames": tuple(wb.sheetnames),
            "defined_names": _defined_names_signature(wb),
            "data_validations": _data_validations_signature(wb),
            "mfc_rules": _mfc_signature(wb),
            "full_calc_on_load": bool(getattr(wb.calculation, "fullCalcOnLoad", False)),
            "tables": _tables_signature(wb),
            "protections": _protections_signature(wb),
            "external_links": _external_links_signature(wb),
        }
    finally:
        wb.close()


def _measure_structure(wb_path: Path) -> dict[str, Any]:
    sig = _structure_signature(wb_path)
    return {
        "sheets": len(sig["sheetnames"]),
        "named_ranges": len(sig["defined_names"]),
        "data_validations": len(sig["data_validations"]),
        "mfc_rules": len(sig["mfc_rules"]),
        "full_calc_on_load": sig["full_calc_on_load"],
        "signature": sig,
    }


def _check_structural_preservation(
    orig_path: Path,
    tmp_path: Path,
    struct_avant: dict[str, Any],
) -> list[str]:
    violations: list[str] = []
    try:
        sig_avant = struct_avant.get("signature") or _structure_signature(orig_path)
        sig_apres = _structure_signature(tmp_path)
    except Exception as exc:
        return [f"Mesure structure apres ecriture impossible : {exc}"]

    for key in (
        "sheetnames", "defined_names", "data_validations", "mfc_rules",
        "tables", "protections", "external_links",
    ):
        if sig_apres.get(key) != sig_avant.get(key):
            violations.append(f"{key} modifie")

    if not sig_apres.get("full_calc_on_load", False):
        violations.append("fullCalcOnLoad perdu apres ecriture")

    return violations


def check_post_replace_integrity(
    reference_path: Path,
    final_path: Path,
    target_row: int,
) -> list[str]:
    """Controle reel apres os.replace contre la copie rollback de reference."""
    struct = _measure_structure(reference_path)
    violations = _check_structural_preservation(reference_path, final_path, struct)
    violations.extend(_check_cell_delta(reference_path, final_path, target_row))
    return violations


# ── Écriture et vérification ───────────────────────────────────────────────────

def _write_row_to_ws(ws: Any, row_data: dict[str, Any], target_row: int) -> None:
    for col_letter, field_name in MANUAL_COL_MAP.items():
        if col_letter in FORMULA_COLS:
            continue
        value = row_data.get(field_name)
        if value is None:
            continue
        ws.cell(row=target_row, column=_col_index(col_letter)).value = value
    headers = {str(cell.value or "").strip(): cell.column for cell in ws[1]}
    for field_name in NEW_SAISIE_FIELDS:
        if field_name not in row_data or field_name not in headers:
            continue
        value = row_data.get(field_name)
        if value is None:
            continue
        ws.cell(row=target_row, column=headers[field_name]).value = value


def _values_match(expected: Any, actual: Any) -> bool:
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False
    try:
        return float(expected) == float(actual)
    except (TypeError, ValueError):
        return str(expected) == str(actual)


def _blank_equivalent(expected: Any, actual: Any) -> bool:
    return expected == "" and actual is None


def _verify_written(ws: Any, row_data: dict[str, Any], target_row: int) -> list[str]:
    ecarts: list[str] = []
    for col_letter, field_name in MANUAL_COL_MAP.items():
        if col_letter in FORMULA_COLS:
            continue
        expected = row_data.get(field_name)
        actual = ws.cell(row=target_row, column=_col_index(col_letter)).value
        if (expected is None and actual is None) or _blank_equivalent(expected, actual):
            continue
        if expected is not None and actual is None:
            ecarts.append(f"{col_letter}/{field_name}: attendu {expected!r} écrit None")
        elif not _values_match(expected, actual):
            ecarts.append(f"{col_letter}/{field_name}: attendu {expected!r} lu {actual!r}")
    headers = {str(cell.value or "").strip(): cell.column for cell in ws[1]}
    for field_name in NEW_SAISIE_FIELDS:
        if field_name not in row_data or field_name not in headers:
            continue
        expected = row_data.get(field_name)
        actual = ws.cell(row=target_row, column=headers[field_name]).value
        if (expected is None and actual is None) or _blank_equivalent(expected, actual):
            continue
        if expected is not None and actual is None:
            ecarts.append(f"{field_name}: attendu {expected!r} écrit None")
        elif not _values_match(expected, actual):
            ecarts.append(f"{field_name}: attendu {expected!r} lu {actual!r}")
    return ecarts


def _check_cell_delta(
    orig_path: Path,
    tmp_path: Path,
    target_row: int,
) -> list[str]:
    """Seules les cols manuelles de target_row doivent différer."""
    violations: list[str] = []
    allowed_col_indices = {
        _col_index(c) for c in MANUAL_COL_MAP if c not in FORMULA_COLS
    }
    try:
        wb_o = openpyxl.load_workbook(str(orig_path), data_only=False)
        wb_t = openpyxl.load_workbook(str(tmp_path), data_only=False)
        ws_o = wb_o[SHEET_SAISIE]
        ws_t = wb_t[SHEET_SAISIE]
        max_row = max(ws_o.max_row or 1, ws_t.max_row or 1)
        headers = {str(cell.value or "").strip(): cell.column for cell in ws_o[1]}
        allowed_col_indices.update(
            headers[field] for field in NEW_SAISIE_FIELDS if field in headers
        )
        max_col = max(ws_o.max_column or 30, ws_t.max_column or 30)
        for row in range(1, min(max_row, 510) + 1):
            for col in range(1, min(max_col, 80) + 1):
                v_o = ws_o.cell(row=row, column=col).value
                v_t = ws_t.cell(row=row, column=col).value
                if v_o == v_t:
                    continue
                if row == target_row and col in allowed_col_indices:
                    continue
                violations.append(f"{_col_letter(col)}{row}: {v_o!r}→{v_t!r}")
                if len(violations) >= 10:
                    violations.append("… (tronqué)")
                    break
            else:
                continue
            break
        wb_o.close()
        wb_t.close()
    except Exception as exc:
        violations.append(f"Erreur comparaison delta : {exc}")
    return violations


# ── Point d'entrée ─────────────────────────────────────────────────────────────

def write_row(
    row_data: dict[str, Any],
    saisie_path: Path,
    target_row: int,
) -> dict[str, Any]:
    """Écrit une ligne dans SAISIE de manière atomique. Aucun accès SQLite.

    Retourne :
        {"statut": "OK"|"ERREUR", "sha256_avant": str|None,
         "sha256_apres": str|None, "details": str|None}
    """
    lock_path = saisie_path.parent / _LOCK_NAME
    lock_acquired = False
    tmp_path: Path | None = None

    try:
        if not cfg.HH_REAL_WRITE_ENABLED:
            return {
                "statut": "GARDE_SECURITE",
                "sha256_avant": None,
                "sha256_apres": None,
                "details": "HH_REAL_WRITE_ENABLED = False - aucune ecriture effectuee",
            }

        # ── 1. Excel ouvert ? ─────────────────────────────────────────────
        if _detect_excel_lock(saisie_path):
            return {
                "statut": "ERREUR", "sha256_avant": None, "sha256_apres": None,
                "details": f"~${saisie_path.name} détecté — fermer Excel avant écriture",
            }

        # ── 2. Verrou exclusif ────────────────────────────────────────────
        if not _acquire_write_lock(lock_path):
            return {
                "statut": "ERREUR", "sha256_avant": None, "sha256_apres": None,
                "details": "Verrou d'écriture déjà détenu — une autre opération est en cours",
            }
        lock_acquired = True

        # ── 3. assert_writable ────────────────────────────────────────────
        assert_writable(saisie_path)

        # ── 4. sha256 avant ───────────────────────────────────────────────
        sha256_avant = _sha256(saisie_path)

        # ── 5. Préflight formules ─────────────────────────────────────────
        formula_v = _check_formula_cells(saisie_path, target_row)
        if formula_v:
            return {
                "statut": "ERREUR", "sha256_avant": sha256_avant, "sha256_apres": None,
                "details": "Préflight formules : " + "; ".join(formula_v),
            }

        # ── 6. Mesure structure avant ─────────────────────────────────────
        struct_avant = _measure_structure(saisie_path)

        # ── 7. Copie temp (même volume) ───────────────────────────────────
        with tempfile.NamedTemporaryFile(
            dir=saisie_path.parent, suffix=".xlsx", delete=False
        ) as tmp_f:
            tmp_path = Path(tmp_f.name)

        if not _same_volume(saisie_path, tmp_path):
            tmp_path.unlink(missing_ok=True)
            tmp_path = None
            return {
                "statut": "ERREUR", "sha256_avant": sha256_avant, "sha256_apres": None,
                "details": "Fichier temporaire sur volume différent — atomicité impossible",
            }

        shutil.copy2(str(saisie_path), str(tmp_path))

        # ── 8. Écriture sur copie ─────────────────────────────────────────
        wb = openpyxl.load_workbook(str(tmp_path), data_only=False)
        ws = wb[SHEET_SAISIE]
        _write_row_to_ws(ws, row_data, target_row)
        wb.calculation.fullCalcOnLoad = True
        wb.save(str(tmp_path))
        wb.close()

        # ── 9. Revalidation valeurs ───────────────────────────────────────
        wb_v = openpyxl.load_workbook(str(tmp_path), data_only=False)
        ecarts = _verify_written(wb_v[SHEET_SAISIE], row_data, target_row)
        wb_v.close()
        if ecarts:
            tmp_path.unlink(missing_ok=True)
            tmp_path = None
            return {
                "statut": "ERREUR", "sha256_avant": sha256_avant, "sha256_apres": None,
                "details": "Revalidation valeurs : " + "; ".join(ecarts),
            }

        # ── 10. Delta cellulaire ──────────────────────────────────────────
        delta_v = _check_cell_delta(saisie_path, tmp_path, target_row)
        if delta_v:
            tmp_path.unlink(missing_ok=True)
            tmp_path = None
            return {
                "statut": "ERREUR", "sha256_avant": sha256_avant, "sha256_apres": None,
                "details": "Delta hors périmètre : " + "; ".join(delta_v[:5]),
            }

        # ── 11. Préservation structurelle ─────────────────────────────────
        struct_v = _check_structural_preservation(saisie_path, tmp_path, struct_avant)
        if struct_v:
            tmp_path.unlink(missing_ok=True)
            tmp_path = None
            return {
                "statut": "ERREUR", "sha256_avant": sha256_avant, "sha256_apres": None,
                "details": "Structure non préservée : " + "; ".join(struct_v),
            }

        # ── 12. Remplacement atomique ─────────────────────────────────────
        os.replace(str(tmp_path), str(saisie_path))
        tmp_path = None

        # ── 13. sha256 après ──────────────────────────────────────────────
        sha256_apres = _sha256(saisie_path)

        return {
            "statut": "OK",
            "sha256_avant": sha256_avant,
            "sha256_apres": sha256_apres,
            "details": None,
        }

    except PermissionError as exc:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return {
            "statut": "ERREUR", "sha256_avant": None, "sha256_apres": None,
            "details": f"Accès refusé : {exc}",
        }
    except Exception as exc:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return {
            "statut": "ERREUR", "sha256_avant": None, "sha256_apres": None,
            "details": f"Erreur inattendue : {exc}",
        }
    finally:
        if lock_acquired:
            _release_write_lock(lock_path)

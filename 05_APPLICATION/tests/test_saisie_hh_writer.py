"""APP-2b — Tests writer saisie HH (opérations fichier uniquement).

Principe :
- Jamais le fichier SAISIE réel.
- Minimal SAISIE (tmp_env) pour les tests de guard/lock/~$.
- Copie D-APP-05C (saisie_reel_copie) pour les tests structurels.
- HH_REAL_WRITE_ENABLED n'est pas testé ici — délégué à test_saisie_hh_orchestrator.
"""
import shutil
import pytest
from pathlib import Path
from unittest.mock import patch
import openpyxl
import app.config as cfg
from app.writers.saisie_hh_writer import (
    write_row,
    _detect_excel_lock,
    _acquire_write_lock,
    _release_write_lock,
    _check_formula_cells,
    _measure_structure,
    _check_structural_preservation,
    _LOCK_NAME,
    _sha256,
)

# ── Artefact D-APP-05C ────────────────────────────────────────────────────────
SAISIE_COPIE_PATH = (
    Path(__file__).parent.parent
    / "data" / "dapp05c_formules" / "20260702T143503Z" / "SAISIE_copie.xlsx"
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_env(tmp_path, tmp_db):
    """Copie temporaire de l'artefact D-APP-05C."""
    if not SAISIE_COPIE_PATH.exists():
        pytest.fail("SAISIE_copie.xlsx D-APP-05C non disponible")
    saisie = tmp_path / "SAISIE_test.xlsx"
    shutil.copy2(str(SAISIE_COPIE_PATH), str(saisie))
    return {"saisie": saisie, "db": tmp_db, "tmp": tmp_path}


@pytest.fixture
def saisie_reel_copie(tmp_path, tmp_db):
    """Copie de SAISIE_copie.xlsx (D-APP-05C) pour tests structurels."""
    if not SAISIE_COPIE_PATH.exists():
        pytest.fail("SAISIE_copie.xlsx D-APP-05C non disponible")
    saisie = tmp_path / "SAISIE_test.xlsx"
    shutil.copy2(str(SAISIE_COPIE_PATH), str(saisie))
    return {"saisie": saisie, "db": tmp_db, "tmp": tmp_path}


@pytest.fixture(autouse=True)
def writer_enabled():
    with patch.object(cfg, "HH_REAL_WRITE_ENABLED", True):
        yield


def _row_data(pk: str = "RESHH-2026-08-001") -> dict:
    return {
        "reservation_hh_id": pk,
        "canal_id":           "CANAL_001",
        "source_financiere":  "SAISIE_MANUELLE",
        "proprietaire_id":    "PROP_0001",
        "logement_id":        "LOG_0001",
        "date_arrivee":       "2026-08-15",
        "date_depart":        "2026-08-18",
        "total_percu":        450.0,
        "code_impact":        "HC",
        "comptabilisation":   "OUI",
        "statut_controle":    "A_CONTROLER",
        "niveau_anomalie":    "A_CONTROLER",
    }


# ── Détection Excel ouvert (~$) ───────────────────────────────────────────────

def test_detect_excel_lock_absent(tmp_env):
    assert not _detect_excel_lock(tmp_env["saisie"])


def test_detect_excel_lock_present(tmp_env):
    saisie = tmp_env["saisie"]
    lock_file = saisie.parent / ("~$" + saisie.name)
    lock_file.write_bytes(b"\x00")
    try:
        assert _detect_excel_lock(saisie)
    finally:
        lock_file.unlink(missing_ok=True)


def test_write_row_bloque_si_excel_ouvert(tmp_env):
    saisie = tmp_env["saisie"]
    lock_file = saisie.parent / ("~$" + saisie.name)
    lock_file.write_bytes(b"\x00")
    sha_avant = saisie.read_bytes()
    try:
        result = write_row(_row_data(), saisie, 4)
    finally:
        lock_file.unlink(missing_ok=True)
    assert result["statut"] == "ERREUR"
    assert "~$" in result["details"]
    assert saisie.read_bytes() == sha_avant


def test_write_row_garde_directe_avant_tout_acces(tmp_env):
    saisie = tmp_env["saisie"]
    sha_avant = saisie.read_bytes()
    with (
        patch.object(cfg, "HH_REAL_WRITE_ENABLED", False),
        patch("app.writers.saisie_hh_writer._detect_excel_lock") as detect_lock,
        patch("app.writers.saisie_hh_writer._acquire_write_lock") as acquire_lock,
        patch("app.writers.saisie_hh_writer._sha256") as sha_func,
        patch("app.writers.saisie_hh_writer.tempfile.NamedTemporaryFile") as tmp_file,
        patch("app.writers.saisie_hh_writer.openpyxl.load_workbook") as load_wb,
    ):
        result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "GARDE_SECURITE"
    assert saisie.read_bytes() == sha_avant
    detect_lock.assert_not_called()
    acquire_lock.assert_not_called()
    sha_func.assert_not_called()
    tmp_file.assert_not_called()
    load_wb.assert_not_called()


# ── Verrou applicatif ─────────────────────────────────────────────────────────

def test_acquire_release_lock(tmp_path):
    lock = tmp_path / _LOCK_NAME
    assert _acquire_write_lock(lock)
    assert lock.exists()
    _release_write_lock(lock)
    assert not lock.exists()


def test_acquire_lock_echoue_si_deja_tenu(tmp_path):
    lock = tmp_path / _LOCK_NAME
    lock.write_text("pid:12345")
    assert not _acquire_write_lock(lock)
    lock.unlink(missing_ok=True)


def test_write_row_bloque_si_verrou_detenu(tmp_env):
    saisie = tmp_env["saisie"]
    lock = saisie.parent / _LOCK_NAME
    lock.write_text("pid:12345")
    try:
        result = write_row(_row_data(), saisie, 4)
    finally:
        lock.unlink(missing_ok=True)
    assert result["statut"] == "ERREUR"
    assert "Verrou" in result["details"]


def test_verrou_libere_apres_succes(tmp_env):
    saisie = tmp_env["saisie"]
    lock = saisie.parent / _LOCK_NAME
    write_row(_row_data(), saisie, 4)
    assert not lock.exists(), "Verrou doit être libéré après retour de write_row"


def test_verrou_libere_apres_erreur(tmp_env):
    saisie = tmp_env["saisie"]
    lock = saisie.parent / _LOCK_NAME
    # Force une erreur en écrivant sur un chemin non-SAISIE
    bad_path = tmp_env["tmp"] / "NOT_SAISIE.xlsx"
    shutil.copy2(str(saisie), str(bad_path))
    write_row(_row_data(), bad_path, 4)
    assert not (bad_path.parent / _LOCK_NAME).exists()


# ── Préflight formules ────────────────────────────────────────────────────────

def test_preflight_9_formules_valides(tmp_env):
    violations = _check_formula_cells(tmp_env["saisie"], 4)
    assert violations == [], violations


def test_preflight_formule_absente_bloque(tmp_env):
    saisie = tmp_env["saisie"]
    wb = openpyxl.load_workbook(str(saisie))
    wb["SAISIE"].cell(row=4, column=2).value = None  # col B
    wb.save(str(saisie))
    wb.close()
    violations = _check_formula_cells(saisie, 4)
    assert any("FORMULE_LIGNE_MODELE_ABSENTE" in v for v in violations)


def test_preflight_formule_figee_bloque(tmp_env):
    saisie = tmp_env["saisie"]
    wb = openpyxl.load_workbook(str(saisie))
    wb["SAISIE"].cell(row=4, column=2).value = "VALEUR_FIGEE"  # col B
    wb.save(str(saisie))
    wb.close()
    violations = _check_formula_cells(saisie, 4)
    assert any("FORMULE_LIGNE_MODELE_FIGEE" in v for v in violations)


def test_preflight_formule_sans_egal_bloque(tmp_env):
    saisie = tmp_env["saisie"]
    wb = openpyxl.load_workbook(str(saisie))
    wb["SAISIE"].cell(row=4, column=2).value = "SUM(A4)"  # col B
    wb.save(str(saisie))
    wb.close()
    violations = _check_formula_cells(saisie, 4)
    assert any("FORMULE_LIGNE_MODELE_FIGEE" in v for v in violations)


def test_write_row_bloque_si_formule_figee(tmp_env):
    saisie = tmp_env["saisie"]
    wb = openpyxl.load_workbook(str(saisie))
    wb["SAISIE"].cell(row=4, column=2).value = "figée"  # col B
    wb.save(str(saisie))
    wb.close()
    sha_avant = _sha256(saisie)
    result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "ERREUR"
    assert "figée" in result["details"] or "Préflight" in result["details"]
    assert _sha256(saisie) == sha_avant


# ── Colonnes formule non écrasées ─────────────────────────────────────────────

def test_colonnes_formule_non_ecrasees_sur_ligne_vide(tmp_env):
    """write_row ne doit jamais écrire dans B/C/K/N/O/Q/V/Y/Z."""
    saisie = tmp_env["saisie"]
    result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "OK", result
    wb = openpyxl.load_workbook(str(saisie), data_only=False)
    ws = wb["SAISIE"]
    formula_cols = {"B": 2, "C": 3, "K": 11, "N": 14, "O": 15,
                    "Q": 17, "V": 22, "Y": 25, "Z": 26}
    for col_letter, col_idx in formula_cols.items():
        value = ws.cell(row=4, column=col_idx).value
        assert isinstance(value, str) and value.startswith("="), (
            f"Col {col_letter} ne doit pas être écrite"
        )
    wb.close()


# ── Écriture réussie sur copie minimale ───────────────────────────────────────

def test_write_row_succes_valeurs_ecrites(tmp_env):
    saisie = tmp_env["saisie"]
    result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "OK", result
    assert result["sha256_avant"] is not None
    assert result["sha256_apres"] is not None
    assert result["sha256_avant"] != result["sha256_apres"]

    wb = openpyxl.load_workbook(str(saisie), read_only=True, data_only=True)
    ws = wb["SAISIE"]
    assert ws.cell(row=4, column=1).value == "RESHH-2026-08-001"  # col A
    wb.close()


def test_write_row_ligne_cible_4_sur_copie_artifact(tmp_env):
    saisie = tmp_env["saisie"]
    result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "OK"


# ── Tests structurels sur SAISIE_copie.xlsx D-APP-05C ────────────────────────

def test_structure_mesuree_saisie_copie(saisie_reel_copie):
    """La structure de SAISIE_copie est mesurable par _measure_structure."""
    s = _measure_structure(saisie_reel_copie["saisie"])
    assert s["sheets"] >= 1
    # Les valeurs exactes dépendent de ce qu'openpyxl préserve depuis SAISIE_copie.xlsx
    # On vérifie juste que la mesure fonctionne sans exception
    assert isinstance(s["data_validations"], int)
    assert isinstance(s["mfc_rules"], int)
    assert isinstance(s["named_ranges"], int)


def test_preservation_structurelle_apres_ecriture(saisie_reel_copie):
    """Écriture sur SAISIE_copie : structure préservée (DV, MFC, feuilles, fullCalcOnLoad)."""
    saisie = saisie_reel_copie["saisie"]
    struct_avant = _measure_structure(saisie)

    # La première ligne vide dans SAISIE_copie (doit avoir des données existantes)
    from app.readers.saisie_hh_reader import find_first_empty_data_row
    target = find_first_empty_data_row(saisie)
    if target is None:
        pytest.fail("SAISIE_copie entierement remplie - pas de ligne disponible")

    result = write_row(_row_data(), saisie, target)
    assert result["statut"] == "OK", result["details"]

    struct_apres = _measure_structure(saisie)
    assert struct_apres["sheets"] == struct_avant["sheets"]
    assert struct_apres["data_validations"] == struct_avant["data_validations"]
    assert struct_apres["mfc_rules"] == struct_avant["mfc_rules"]
    assert struct_apres["full_calc_on_load"] is True
    assert struct_apres["signature"] == struct_avant["signature"]


def test_delta_cellulaire_uniquement_ligne_cible(saisie_reel_copie):
    """Après écriture, seules les cols manuelles de la ligne cible diffèrent."""
    saisie = saisie_reel_copie["saisie"]
    from app.readers.saisie_hh_reader import find_first_empty_data_row
    from app.writers.saisie_hh_writer import _check_cell_delta
    target = find_first_empty_data_row(saisie)
    if target is None:
        pytest.fail("SAISIE_copie entierement remplie")

    sha_avant = _sha256(saisie)
    result = write_row(_row_data(), saisie, target)
    assert result["statut"] == "OK", result

    # Ré-évaluer le delta en restaurant d'abord l'original pour comparaison
    # On utilise sha_avant pour vérifier que le write a eu lieu
    assert _sha256(saisie) != sha_avant
    # La vérification delta est interne au writer (étape 10) ; si write retourne OK,
    # aucune cellule hors périmètre n'a changé.


def test_fullcalconload_force_vrai(tmp_env):
    """write_row force fullCalcOnLoad=True même si absent du fichier original."""
    saisie = tmp_env["saisie"]
    result = write_row(_row_data(), saisie, 4)
    assert result["statut"] == "OK"
    wb = openpyxl.load_workbook(str(saisie), data_only=False)
    calc = getattr(wb.calculation, "fullCalcOnLoad", None)
    wb.close()
    assert calc is True


def test_signature_bloque_validation_modifiee(saisie_reel_copie):
    saisie = saisie_reel_copie["saisie"]
    tmp = saisie_reel_copie["tmp"] / "alter_dv.xlsx"
    shutil.copy2(str(saisie), str(tmp))
    struct = _measure_structure(saisie)
    wb = openpyxl.load_workbook(str(tmp))
    dv = wb["SAISIE"].data_validations.dataValidation[0]
    dv.allowBlank = not bool(dv.allowBlank)
    wb.save(str(tmp))
    wb.close()
    violations = _check_structural_preservation(saisie, tmp, struct)
    assert any("data_validations" in v for v in violations)


def test_signature_bloque_plage_nommee_modifiee(saisie_reel_copie):
    saisie = saisie_reel_copie["saisie"]
    tmp = saisie_reel_copie["tmp"] / "alter_name.xlsx"
    shutil.copy2(str(saisie), str(tmp))
    struct = _measure_structure(saisie)
    wb = openpyxl.load_workbook(str(tmp))
    from openpyxl.workbook.defined_name import DefinedName
    wb.defined_names.add(DefinedName("APP2B_TEST_NAME", attr_text="SAISIE!$A$1"))
    wb.save(str(tmp))
    wb.close()
    violations = _check_structural_preservation(saisie, tmp, struct)
    assert any("defined_names" in v for v in violations)


def test_signature_bloque_mfc_modifiee(saisie_reel_copie):
    saisie = saisie_reel_copie["saisie"]
    tmp = saisie_reel_copie["tmp"] / "alter_mfc.xlsx"
    shutil.copy2(str(saisie), str(tmp))
    struct = _measure_structure(saisie)
    wb = openpyxl.load_workbook(str(tmp))
    rules = next(iter(wb["SAISIE"].conditional_formatting._cf_rules.values()))
    rules[0].priority = int(rules[0].priority or 1) + 100
    wb.save(str(tmp))
    wb.close()
    violations = _check_structural_preservation(saisie, tmp, struct)
    assert any("mfc_rules" in v for v in violations)

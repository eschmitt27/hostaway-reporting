"""Tests APP-3b-1 — Prévisualisation saisie charge sur copie.

Toutes les opérations s'effectuent sur des copies dans tmp_path (external).
SAISIE_Charges_Flux.xlsx n'est jamais modifié.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date
from pathlib import Path

import pytest

import app.config as cfg
from app.readers.saisie_charges_reader import (
    FORMULA_COL_INDICES,
    MANUAL_COL_MAP,
    _col_index,
    count_charges_with_prefix,
    find_model_row,
    read_all_charge_ids,
    read_ref_assoc_mode,
    read_ref_associes,
    read_ref_cartes_paiement,
    read_ref_categories_charges,
    read_ref_cloture,
    read_ref_codes_impact,
    read_ref_logements,
    read_ref_modes_paiement,
    read_ref_statuts,
    read_ref_types_affectation,
    read_ref_types_flux,
)
from app.services.charges_preview_service import (
    ChargesPreviewError,
    generate_charge_id,
    load_form_refs,
    load_previsualisation,
    previsualiser,
    resolve_assoc_mode,
    validate_charge,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _valid_form() -> dict[str, str]:
    """Formulaire minimal valide (mois 2026-06 ouvert), aligné référentiels Excel.

    Catégorie éligible au formulaire standard (CHG_005 logiciel, famille GLOBAL).
    sens_flux, type_flux_id, statut_controle, niveau_anomalie, prise_en_compta ne sont plus
    saisis (injectés/dérivés serveur). CHG_005 + PAY_001 + IC → type dérivé TYPE_FLUX_020
    (défaut IC → aucun commentaire requis).
    """
    return {
        "date_charge": "2026-06-15",
        "montant": "85.00",
        "categorie_charge_id": "CHG_005",
        "code_impact": "IC",
        "mode_paiement_id": "PAY_001",
        "affectation_type": "GLOBAL",
    }


def _refs() -> dict:
    return load_form_refs()


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def saisie_copy(tmp_path: Path) -> Path:
    """Copie de SAISIE_Charges_Flux.xlsx dans tmp_path (jamais le vrai fichier)."""
    dest = tmp_path / "SAISIE_Charges_Flux_test.xlsx"
    shutil.copy2(str(cfg.SAISIE_CHARGES), str(dest))
    return dest


@pytest.fixture()
def refs() -> dict:
    return load_form_refs()


# ── 1. Flags de sécurité ──────────────────────────────────────────────────────

def test_flag_charges_real_write_est_false():
    assert cfg.CHARGES_REAL_WRITE_ENABLED is False


def test_flag_charges_real_write_confirmation_est_false():
    assert cfg.CHARGES_REAL_WRITE_CONFIRMATION_ENABLED is False


def test_flag_hh_real_write_est_false():
    assert cfg.HH_REAL_WRITE_ENABLED is False


def test_flag_ref_assoc_mode_est_false():
    assert cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED is False


# ── 2. Hash source inchangé ───────────────────────────────────────────────────

def test_source_hash_inchange_apres_previsualisation(tmp_path: Path):
    hash_avant = _sha256(cfg.SAISIE_CHARGES)
    form = _valid_form()
    previsualiser(form, dryruns_root=tmp_path / "dryruns")
    hash_apres = _sha256(cfg.SAISIE_CHARGES)
    assert hash_avant == hash_apres, "SAISIE_Charges_Flux.xlsx modifié pendant la prévisualisation"


def test_manifest_source_inchangee_true(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    manifest = result["manifest"]
    assert manifest["source_inchangee"] is True
    assert manifest["source_hash_avant"] == manifest.get("source_hash_apres", manifest["source_hash_avant"])


# ── 3. Validation — champs obligatoires ──────────────────────────────────────

def test_validate_date_manquante(refs):
    form = _valid_form()
    form["date_charge"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V01_DATE") for c in codes)


def test_validate_date_invalide(refs):
    form = _valid_form()
    form["date_charge"] = "not-a-date"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V01_DATE") for c in codes)


def test_validate_montant_manquant(refs):
    form = _valid_form()
    form["montant"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V03_MONTANT") for c in codes)


def test_validate_montant_negatif(refs):
    form = _valid_form()
    form["montant"] = "-10"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V03_MONTANT") for c in codes)


def test_validate_montant_zero(refs):
    form = _valid_form()
    form["montant"] = "0"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V03_MONTANT") for c in codes)


def test_validate_categorie_manquante(refs):
    form = _valid_form()
    form["categorie_charge_id"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V04_CATEGORIE") for c in codes)


def test_validate_categorie_invalide(refs):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_INEXISTANT"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V04_CATEGORIE") for c in codes)


def test_type_flux_non_saisi_derive_ok(refs):
    # type_flux_id n'est plus saisi : absent du formulaire, dérivé serveur. Aucune erreur V05 manquant.
    form = _valid_form()
    form.pop("type_flux_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert not any(c == "V05_TYPE_FLUX_MANQUANT" for c in codes)


def test_type_flux_navigateur_ignore(refs):
    # Une valeur type_flux_id envoyée par le navigateur est ignorée (jamais validée depuis le form).
    form = _valid_form()
    form["type_flux_id"] = "TYPE_FLUX_999_BIDON"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert not any(c.startswith("V05_TYPE_FLUX_INVALIDE") for c in codes)


def test_validate_code_impact_manquant(refs):
    form = _valid_form()
    form["code_impact"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V06_CODE_IMPACT") for c in codes)


def test_validate_mode_paiement_manquant(refs):
    form = _valid_form()
    form["mode_paiement_id"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V07_MODE_PAIEMENT") for c in codes)


def test_validate_statut_controle_non_saisi_ok(refs):
    # statut_controle n'est plus un champ saisi : le formulaire valide sans lui.
    form = _valid_form()
    form.pop("statut_controle", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert not any(c.startswith("V14_STATUT_CONTROLE") for c in codes)


def test_validate_affectation_type_manquant(refs):
    form = _valid_form()
    form["affectation_type"] = ""
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V11_AFFECTATION") for c in codes)


# ── 4. Validation — mois clôturé ──────────────────────────────────────────────

def test_validate_mois_cloture(refs):
    form = _valid_form()
    form["date_charge"] = "2025-03-10"  # 2025-03 = CLOTURE
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V02_MOIS_CLOTURE" in codes


def test_validate_mois_ouvert_accepte(refs):
    form = _valid_form()
    form["date_charge"] = "2026-06-15"  # 2026-06 = OUVERT
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V02_MOIS_CLOTURE" not in codes


# ── 5. Validation — mode paiement et associé ─────────────────────────────────

def test_validate_associe_requis_pay003(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_003"
    form.pop("associe_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V08_ASSOCIE") for c in codes)


def test_validate_associe_requis_pay004(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_004"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V08_ASSOCIE") for c in codes)


def test_validate_carte_requise_pay003(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_003"
    form["associe_id"] = "PERS_EWAN"
    form.pop("carte_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V09_CARTE") for c in codes)


def test_validate_carte_interdite_si_pas_pay003(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_001"
    form["carte_id"] = "CARTE_001"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V09_CARTE_INTERDITE" in codes


def test_validate_carte_mauvais_associe(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_003"
    form["associe_id"] = "PERS_EWAN"
    form["carte_id"] = "CARTE_001"  # CARTE_001 appartient à PERS_WAFA
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V09_CARTE_MAUVAIS_ASSOCIE" in codes


# ── 6. Validation — ASSOC_MODE résolvable ────────────────────────────────────

def test_validate_assoc_mode_non_resolvable_si_associe_absent_pay001(refs):
    # PAY_001 sans associe → BANQUE → résolvable (associe vide = match AM_001)
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_001"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V10_ASSOC_MODE_NON_RESOLVABLE" not in codes


def test_validate_assoc_mode_non_resolvable_combinaison_inconnue(refs):
    assoc_rows = refs["assoc_mode"]
    result = resolve_assoc_mode("PAY_001", "PERS_INCONNU", assoc_rows)
    assert result is None


def test_resolve_assoc_mode_banque():
    rows = read_ref_assoc_mode()
    result = resolve_assoc_mode("PAY_001", None, rows)
    assert result == "BANQUE"


def test_resolve_assoc_mode_ewan_cb():
    rows = read_ref_assoc_mode()
    result = resolve_assoc_mode("PAY_003", "PERS_EWAN", rows)
    assert result == "EWAN-CB"


def test_resolve_assoc_mode_wafa_perso():
    rows = read_ref_assoc_mode()
    result = resolve_assoc_mode("PAY_004", "PERS_WAFA", rows)
    assert result == "WAFA-PERSO"


# ── 7. Validation — affectation et logement ──────────────────────────────────

def test_validate_logement_requis_si_aff_logement(refs):
    form = _valid_form()
    form["affectation_type"] = "LOGEMENT"
    form.pop("logement_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert any(c.startswith("V12_LOGEMENT") for c in codes)


def test_validate_logement_invalide(refs):
    form = _valid_form()
    form["affectation_type"] = "LOGEMENT"
    form["logement_id"] = "LOG_INEXISTANT"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V12_LOGEMENT_INVALIDE" in codes


def test_validate_proprietaire_requis_si_aff_proprietaire(refs):
    form = _valid_form()
    form["affectation_type"] = "PROPRIETAIRE"
    form.pop("proprietaire_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V12B_PROPRIETAIRE_MANQUANT" in codes


def test_validate_global_ne_requiert_pas_logement(refs):
    form = _valid_form()
    form["affectation_type"] = "GLOBAL"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert not any(c.startswith("V12") for c in codes)


def test_validate_affectation_prefixe_aff_refuse(refs):
    # Ancien style AFF_* n'est plus accepté : seules les valeurs canoniques REF_LOCALE.
    form = _valid_form()
    form["affectation_type"] = "AFF_GLOBAL"
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V11_AFFECTATION_INVALIDE" in codes


# ── 8. Validation — reservation_id (CHG_021) ─────────────────────────────────

def test_validate_reservation_requise_chg021(refs):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_021"
    form.pop("reservation_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V13_RESERVATION_MANQUANTE" in codes


def test_validate_reservation_non_requise_categorie_standard(refs):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_005"  # catégorie standard éligible
    form.pop("reservation_id", None)
    errors = validate_charge(form, refs)
    codes = [e["code"] for e in errors]
    assert "V13_RESERVATION_MANQUANTE" not in codes


# ── 9. Génération charge_id ───────────────────────────────────────────────────

def test_generate_charge_id_format(saisie_copy: Path):
    d = date(2026, 6, 15)
    cid = generate_charge_id(d, "IC", "BANQUE", saisie_copy)
    assert cid.startswith("CHG-2026-06-IC-BANQUE-")
    parts = cid.split("-")
    assert len(parts) >= 6
    nnn = parts[-1]
    assert nnn.isdigit() and len(nnn) == 3


def test_generate_charge_id_nnn_commence_a_001(saisie_copy: Path):
    d = date(2026, 6, 15)
    cid = generate_charge_id(d, "HC", "LIQ", saisie_copy)
    assert cid.endswith("-001"), f"Premier ID devrait finir en -001 : {cid}"


def test_generate_charge_id_assoc_mode_ewan_cb(saisie_copy: Path):
    d = date(2026, 6, 1)
    cid = generate_charge_id(d, "IC", "EWAN-CB", saisie_copy)
    assert "EWAN-CB" in cid


# ── 10. Prévisualisation — token et manifest ──────────────────────────────────

def test_previsualiser_retourne_token(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert "token" in result
    assert result["token"].startswith("CHG_")


def test_previsualiser_cree_manifest_json(tmp_path: Path):
    dryruns = tmp_path / "dryruns"
    result = previsualiser(_valid_form(), dryruns_root=dryruns)
    token = result["token"]
    manifest_path = dryruns / token / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "OK"
    assert manifest["charges_real_write_enabled"] is False


def test_previsualiser_cree_copie_saisie(tmp_path: Path):
    dryruns = tmp_path / "dryruns"
    result = previsualiser(_valid_form(), dryruns_root=dryruns)
    assert result["ok"]
    manifest = result["manifest"]
    copy_path = Path(manifest["paths"]["saisie_copy"])
    assert copy_path.exists()


def test_previsualiser_copie_differente_de_source(tmp_path: Path):
    hash_source = _sha256(cfg.SAISIE_CHARGES)
    dryruns = tmp_path / "dryruns"
    result = previsualiser(_valid_form(), dryruns_root=dryruns)
    copy_path = Path(result["manifest"]["paths"]["saisie_copy"])
    hash_copy = _sha256(copy_path)
    assert hash_copy != hash_source, "La copie doit différer de la source (ligne injectée)"


def test_previsualiser_manifest_charge_id_present(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert result["manifest"]["charge_id"] is not None
    assert result["manifest"]["charge_id"].startswith("CHG-")


def test_previsualiser_manifest_target_row_present(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert isinstance(result["manifest"]["target_row"], int)
    assert result["manifest"]["target_row"] >= 2


# ── 11. Validation — retour formulaire si erreurs ────────────────────────────

def test_previsualiser_ok_false_si_erreurs(tmp_path: Path):
    form = _valid_form()
    form["date_charge"] = ""
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"] is False
    assert result["manifest"]["status"] == "VALIDATION_REFUSEE"
    assert len(result["manifest"]["errors"]) > 0


def test_previsualiser_ne_cree_pas_copie_si_erreurs(tmp_path: Path):
    dryruns = tmp_path / "dryruns"
    form = _valid_form()
    form["date_charge"] = ""
    result = previsualiser(form, dryruns_root=dryruns)
    assert "paths" not in result["manifest"]


# ── 12. Chargement prévisualisation par token ─────────────────────────────────

def test_load_previsualisation_par_token(tmp_path: Path):
    dryruns = tmp_path / "dryruns"
    result = previsualiser(_valid_form(), dryruns_root=dryruns)
    token = result["token"]
    loaded = load_previsualisation(token, dryruns_root=dryruns)
    assert loaded["token"] == token
    assert loaded["manifest"]["status"] == "OK"


def test_load_previsualisation_token_inexistant(tmp_path: Path):
    with pytest.raises(ChargesPreviewError):
        load_previsualisation("CHG_20260101T000000Z_faketoken000", dryruns_root=tmp_path)


def test_load_previsualisation_token_invalide():
    with pytest.raises(ChargesPreviewError):
        load_previsualisation("../../../etc/passwd")


# ── 13. Ligne modèle ──────────────────────────────────────────────────────────

def test_find_model_row_retourne_int(saisie_copy: Path):
    row = find_model_row(saisie_copy)
    assert isinstance(row, int)
    assert row >= 2


def test_find_model_row_source_hash_inchange(saisie_copy: Path):
    hash_avant = _sha256(saisie_copy)
    find_model_row(saisie_copy)
    hash_apres = _sha256(saisie_copy)
    assert hash_avant == hash_apres


# ── 14. Colonnes formule non écrites ─────────────────────────────────────────

def test_colonnes_formule_non_ecrites_dans_copie(tmp_path: Path):
    import openpyxl

    dryruns = tmp_path / "dryruns"
    result = previsualiser(_valid_form(), dryruns_root=dryruns)
    assert result["ok"]
    copy_path = Path(result["manifest"]["paths"]["saisie_copy"])
    target_row = result["manifest"]["target_row"]

    wb = openpyxl.load_workbook(str(copy_path), data_only=False)
    try:
        ws = wb["SAISIE"]
        for col_idx in FORMULA_COL_INDICES:
            cell = ws.cell(row=target_row, column=col_idx)
            val = cell.value
            assert val is None or str(val).startswith("="), (
                f"Colonne formule col={col_idx} row={target_row} contient une valeur calculée : {val!r}"
            )
    finally:
        wb.close()


# ── 15. Lecture référentiels ──────────────────────────────────────────────────

def test_read_ref_assoc_mode_non_vide():
    rows = read_ref_assoc_mode()
    assert len(rows) >= 7


def test_read_ref_categories_charges_non_vide():
    cats = read_ref_categories_charges()
    assert len(cats) >= 20


def test_read_ref_modes_paiement_contient_pay001():
    modes = read_ref_modes_paiement()
    ids = [str(m.get("mode_paiement_id", "")).strip() for m in modes]
    assert "PAY_001" in ids


def test_read_ref_cloture_non_vide():
    cloture = read_ref_cloture()
    assert len(cloture) > 0


def test_read_ref_codes_impact_contient_ic_hc_hr():
    codes = read_ref_codes_impact()
    ids = {str(c.get("code_impact", "")).strip() for c in codes}
    assert {"IC", "HC", "HR"}.issubset(ids)


# ── 16. Sens flux injecté serveur (jamais saisi) ─────────────────────────────

def test_sens_flux_navigateur_ignore(refs):
    # sens_flux n'est plus saisi : toute valeur navigateur est ignorée, aucune validation.
    for v in ("INCONNU", "CHARGE", "PRODUIT", "REFACTURATION"):
        form = _valid_form()
        form["sens_flux"] = v
        codes = [e["code"] for e in validate_charge(form, refs)]
        assert not any(c.startswith("V15_SENS_FLUX") for c in codes), v


def _injected_cell(result, col_letter: str):
    import openpyxl
    copy_path = Path(result["manifest"]["paths"]["saisie_copy"])
    target_row = result["manifest"]["target_row"]
    col = _col_index(col_letter)
    wb = openpyxl.load_workbook(str(copy_path), data_only=True)
    try:
        return wb["SAISIE"].cell(row=target_row, column=col).value
    finally:
        wb.close()


def test_previsualiser_sens_flux_defaut_depense(tmp_path: Path):
    form = _valid_form()
    form["sens_flux"] = ""
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert str(_injected_cell(result, "E") or "").upper() == "DEPENSE"


# ── 17. Alignement référentiels Excel (Commit 1) ─────────────────────────────

def test_statut_controle_auto_injecte_a_controler(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne X = statut_controle
    assert str(_injected_cell(result, "X") or "").strip() == "A_CONTROLER"


def test_niveau_anomalie_auto_injecte_info(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne Y = niveau_anomalie
    assert str(_injected_cell(result, "Y") or "").strip() == "INFO"


def test_prise_en_compta_derivee_ic_oui(tmp_path: Path):
    form = _valid_form()
    form["code_impact"] = "IC"
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne K = prise_en_compta
    assert str(_injected_cell(result, "K") or "").strip() == "OUI"


def test_prise_en_compta_derivee_hc_non(tmp_path: Path):
    form = _valid_form()
    form["code_impact"] = "HC"
    # TF020 défaut IC ; HC diffère → commentaire de justification obligatoire (V19)
    form["commentaire"] = "Hors compta volontaire"
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert str(_injected_cell(result, "K") or "").strip() == "NON"


def test_prise_en_compta_form_ignoree(tmp_path: Path):
    # Une valeur prise_en_compta envoyée par le formulaire est ignorée (dérivée du code_impact).
    form = _valid_form()
    form["code_impact"] = "IC"
    form["prise_en_compta"] = "NON"  # mensonge → doit être écrasé par OUI (IC)
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert str(_injected_cell(result, "K") or "").strip() == "OUI"


def test_code_impact_hr_refuse(refs):
    form = _valid_form()
    form["code_impact"] = "HR"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V06_CODE_IMPACT_HORS_STANDARD" in codes


def test_code_impact_ic_hc_acceptes(refs):
    for ok in ("IC", "HC"):
        form = _valid_form()
        form["code_impact"] = ok
        codes = [e["code"] for e in validate_charge(form, refs)]
        assert not any(c.startswith("V06_CODE_IMPACT") for c in codes), ok


@pytest.mark.parametrize("cat", [
    "CHG_001", "CHG_002", "CHG_012", "CHG_013", "CHG_014",
    "CHG_015", "CHG_019", "CHG_020", "CHG_021", "CHG_022",
])
def test_categorie_hors_formulaire_refusee(refs, cat):
    form = _valid_form()
    form["categorie_charge_id"] = cat
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V04_CATEGORIE_HORS_FORMULAIRE" in codes, cat


def test_load_form_refs_categories_excluent_parcours_dedies(refs):
    ids = {str(c.get("categorie_charge_id", "")).strip() for c in refs["categories"]}
    for excluded in ("CHG_001", "CHG_002", "CHG_012", "CHG_013", "CHG_014",
                     "CHG_015", "CHG_019", "CHG_020", "CHG_021", "CHG_022"):
        assert excluded not in ids, excluded


def test_load_form_refs_modes_excluent_pay005_pay006(refs):
    ids = {str(m.get("mode_paiement_id", "")).strip() for m in refs["modes_paiement"]}
    assert "PAY_005" not in ids
    assert "PAY_006" not in ids


def test_load_form_refs_codes_impact_ic_hc_seulement(refs):
    ids = {str(c.get("code_impact", "")).strip() for c in refs["codes_impact"]}
    assert ids <= {"IC", "HC"}
    assert "HR" not in ids


def test_load_form_refs_statuts_controle_famille(refs):
    # Le formulaire n'utilise plus la famille import ; famille statut_controle uniquement.
    assert "statuts_import" not in refs
    familles = {str(s.get("famille_statut", "")).strip() for s in refs["statuts_controle"]}
    assert familles == {"statut_controle"}
    statuts = {str(s.get("statut", "")).strip() for s in refs["statuts_controle"]}
    assert "A_CONTROLER" in statuts
    # Aucune valeur de la famille import (A_IMPORTER/IMPORTE/CORRIGE...) présente
    for import_val in ("A_IMPORTER", "IMPORTE", "CORRIGE", "REJETE"):
        assert import_val not in statuts


def test_load_form_refs_affectation_canonique(refs):
    vals = {str(a.get("type_affectation", "")).strip() for a in refs["affectation_types"]}
    assert vals == {"LOGEMENT", "PROPRIETAIRE", "GLOBAL", "NON_AFFECTABLE"}


def test_previsualiser_affectation_canonique_injectee(tmp_path: Path):
    form = _valid_form()
    form["affectation_type"] = "GLOBAL"
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne O = affectation_type
    assert str(_injected_cell(result, "O") or "").strip() == "GLOBAL"


# ── 18. Profils d'impact — prévisualisation (Commit 3) ───────────────────────

from app.services.charges_preview_service import resolve_profil_impact  # noqa: E402


@pytest.mark.parametrize("cat", ["CHG_003", "CHG_004", "CHG_027"])
def test_menage_categorie_bloquee(refs, cat):
    # Catégorie ménage sans parcours (pas de menage_mode) → V16 (parcours ménage requis).
    form = _valid_form()
    form["categorie_charge_id"] = cat
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V16_MENAGE_PARCOURS_DEDIE" in codes, cat


@pytest.mark.parametrize("cat", ["CHG_016", "CHG_023"])
def test_categorie_hors_catalogue_refusee(refs, cat):
    # Forfait client (CHG_016) et forfait cave récurrent (CHG_023) : hors Nouvelle charge.
    form = _valid_form()
    form["categorie_charge_id"] = cat
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V04_CATEGORIE_HORS_FORMULAIRE" in codes, cat


def test_global_categorie_previsualisable(refs):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_005"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V16_MENAGE_PARCOURS_DEDIE" not in codes
    assert "V04_CATEGORIE_HORS_FORMULAIRE" not in codes


def test_resolve_profil_impact_global(refs):
    assert resolve_profil_impact("CHG_005", refs) == "GLOBAL"


def test_resolve_profil_impact_chg024_global(refs):
    assert resolve_profil_impact("CHG_024", refs) == "GLOBAL"


def test_profil_impact_injecte_dans_copie(tmp_path: Path):
    result = previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne AH = profil_impact_charge
    assert str(_injected_cell(result, "AH") or "").strip() == "GLOBAL"
    assert result["manifest"]["profil_impact"] == "GLOBAL"


def _form_chg024() -> dict[str, str]:
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_024"
    form["libelle_categorie_personnalise"] = "Frais divers exceptionnel"
    return form


def test_chg024_libelle_requis(refs):
    form = _form_chg024()
    form["libelle_categorie_personnalise"] = ""
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V17_LIBELLE_PERSONNALISE_MANQUANT" in codes


def test_chg024_valide_ok(refs):
    codes = [e["code"] for e in validate_charge(_form_chg024(), refs)]
    assert not any(c.startswith("V17") or c.startswith("V18") for c in codes), codes


def test_chg024_logement_interdit(refs):
    form = _form_chg024()
    form["affectation_type"] = "LOGEMENT"
    form["logement_id"] = "LOG_0001"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V18_PERSONNALISE_NON_GLOBAL" in codes


def test_chg024_reservation_interdite(refs):
    form = _form_chg024()
    form["reservation_id"] = "12345"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V18_PERSONNALISE_RESERVATION_INTERDITE" in codes


def test_chg024_intervenant_interdit(refs):
    form = _form_chg024()
    form["intervenant_concerne"] = "INT_0001"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V18_PERSONNALISE_INTERVENANT_INTERDIT" in codes


def test_chg024_refacturable_interdit(refs):
    form = _form_chg024()
    form["refacturable"] = "OUI"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V18_PERSONNALISE_REFAC_INTERDITE" in codes


def test_chg024_previsualisation_force_global_et_menage_non(tmp_path: Path):
    result = previsualiser(_form_chg024(), dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert result["manifest"]["profil_impact"] == "GLOBAL"
    assert str(_injected_cell(result, "AH") or "").strip() == "GLOBAL"
    # AF = affectable_menage forcé NON
    assert str(_injected_cell(result, "AF") or "").strip() == "NON"
    # AI = libelle_categorie_personnalise
    assert str(_injected_cell(result, "AI") or "").strip() == "Frais divers exceptionnel"


def test_menage_categorie_refuse_previsualisation(tmp_path: Path):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_003"
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"] is False
    codes = [e["code"] for e in result["manifest"]["errors"]]
    assert "V16_MENAGE_PARCOURS_DEDIE" in codes


# ── 19. Dérivation type_flux serveur (Phase 2 — D-CHG-TYPEFLUX-01) ────────────

from app.services.charges_preview_service import derive_type_flux  # noqa: E402


def test_derive_pay001_tf020():
    assert derive_type_flux("CHG_005", "PAY_001", None, None) == "TYPE_FLUX_020"


def test_derive_pay002_recupere_tf008():
    assert derive_type_flux("CHG_005", "PAY_002", None, "OUI") == "TYPE_FLUX_008"


def test_derive_pay002_non_recupere_tf004():
    assert derive_type_flux("CHG_005", "PAY_002", None, "NON") == "TYPE_FLUX_004"
    assert derive_type_flux("CHG_005", "PAY_002", None, None) == "TYPE_FLUX_004"


def test_derive_pay003_pay004_tf004():
    assert derive_type_flux("CHG_005", "PAY_003", None, None) == "TYPE_FLUX_004"
    assert derive_type_flux("CHG_005", "PAY_004", None, None) == "TYPE_FLUX_004"


def test_derive_pay005_pay006_none():
    assert derive_type_flux("CHG_005", "PAY_005", None, None) is None
    assert derive_type_flux("CHG_005", "PAY_006", None, None) is None


def test_derive_chg010_tf016():
    assert derive_type_flux("CHG_010", "PAY_001", None, None) == "TYPE_FLUX_016"


def test_derive_chg016_tf012():
    assert derive_type_flux("CHG_016", "PAY_002", None, None) == "TYPE_FLUX_012"


def test_derive_chg008_chg011_refacturable_tf011():
    assert derive_type_flux("CHG_008", "PAY_001", "OUI", None) == "TYPE_FLUX_011"
    assert derive_type_flux("CHG_011", "PAY_002", "OUI", None) == "TYPE_FLUX_011"


def test_derive_chg008_chg011_non_refacturable_suit_reglement():
    assert derive_type_flux("CHG_008", "PAY_001", "NON", None) == "TYPE_FLUX_020"
    assert derive_type_flux("CHG_011", "PAY_003", None, None) == "TYPE_FLUX_004"


def test_derive_chg024_jamais_tf011():
    # CHG_024 refacturable ignoré → jamais TF011 ; suit le règlement.
    assert derive_type_flux("CHG_024", "PAY_001", "OUI", None) == "TYPE_FLUX_020"


def test_previsualiser_injecte_type_flux_derive(tmp_path: Path):
    form = _valid_form()  # CHG_005 + PAY_001
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    # Colonne G = type_flux_id
    assert str(_injected_cell(result, "G") or "").strip() == "TYPE_FLUX_020"
    assert result["manifest"]["type_flux_id"] == "TYPE_FLUX_020"


def test_previsualiser_chg010_injecte_tf016(tmp_path: Path):
    form = _valid_form()
    form["categorie_charge_id"] = "CHG_010"
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert str(_injected_cell(result, "G") or "").strip() == "TYPE_FLUX_016"


def test_mode_interdit_pay005_refuse(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_005"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V07_MODE_PAIEMENT_INTERDIT" in codes


def test_mode_interdit_pay006_refuse(refs):
    form = _valid_form()
    form["mode_paiement_id"] = "PAY_006"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V07_MODE_PAIEMENT_INTERDIT" in codes


# ── 20. Commentaire obligatoire si impact ≠ défaut du type ───────────────────

def test_commentaire_requis_si_impact_diff_defaut(refs):
    # CHG_005 + PAY_001 → TF020 (défaut IC). Choisir HC diffère → commentaire requis.
    form = _valid_form()
    form["code_impact"] = "HC"
    form.pop("commentaire", None)
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V19_COMMENTAIRE_JUSTIFICATION_REQUIS" in codes


def test_commentaire_fourni_leve_exigence(refs):
    form = _valid_form()
    form["code_impact"] = "HC"
    form["commentaire"] = "Justification : hors compta volontaire"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V19_COMMENTAIRE_JUSTIFICATION_REQUIS" not in codes


def test_pas_de_commentaire_requis_si_impact_egal_defaut(refs):
    # CHG_005 + PAY_001 → TF020 (défaut IC). Choisir IC = défaut → pas de commentaire requis.
    form = _valid_form()
    form["code_impact"] = "IC"
    form.pop("commentaire", None)
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V19_COMMENTAIRE_JUSTIFICATION_REQUIS" not in codes


# ── 21. CHG_024 : forçage serveur complet (requête manipulée) ────────────────

def test_chg024_force_toutes_valeurs_global_meme_si_manipule(tmp_path: Path):
    # Requête manipulée envoyant logement/refac/reservation → doit être refusée (V18) OU forcée.
    # Ici on envoie un formulaire CHG_024 valide et on vérifie le forçage serveur.
    form = _form_chg024()  # PAY_001, code IC
    result = previsualiser(form, dryruns_root=tmp_path / "dryruns")
    assert result["ok"], result["manifest"].get("errors")
    assert result["manifest"]["profil_impact"] == "GLOBAL"
    assert str(_injected_cell(result, "AH") or "").strip() == "GLOBAL"   # profil_impact_charge
    assert str(_injected_cell(result, "O") or "").strip() == "GLOBAL"    # affectation_type forcé
    assert str(_injected_cell(result, "AF") or "").strip() == "NON"      # affectable_menage
    assert str(_injected_cell(result, "S") or "").strip() in ("NON", "")  # refacturable forcé NON/None
    # logement (P), proprietaire (Q), reservation (R), intervenant (AG) vides
    for col in ("P", "Q", "R", "AG"):
        assert (_injected_cell(result, col) in (None, "")), col
    # type_flux dérivé du règlement (PAY_001 → TF020), jamais TF011
    assert str(_injected_cell(result, "G") or "").strip() == "TYPE_FLUX_020"


# ── 22. type_flux_id et sens_flux absents du HTML formulaire ──────────────────

def test_type_flux_et_sens_flux_absents_du_html(client):
    r = client.get("/fournisseurs/nouvelle")
    assert r.status_code == 200
    html = r.text
    assert 'name="type_flux_id"' not in html
    assert 'name="sens_flux"' not in html
    assert 'name="statut_controle"' not in html
    assert 'name="niveau_anomalie"' not in html
    assert 'name="prise_en_compta"' not in html


def test_categories_dediees_absentes_du_formulaire(client):
    r = client.get("/fournisseurs/nouvelle")
    assert r.status_code == 200
    html = r.text
    # Catégories PARCOURS_DEDIE + forfait client (CHG_016) + forfait cave récurrent (CHG_023) absentes.
    for cat in ("CHG_001", "CHG_002", "CHG_012", "CHG_013", "CHG_014",
                "CHG_015", "CHG_016", "CHG_019", "CHG_021", "CHG_022", "CHG_023"):
        assert f'value="{cat}"' not in html, cat
    # PAY_005 / PAY_006 absents du dropdown mode
    assert 'value="PAY_005"' not in html
    assert 'value="PAY_006"' not in html


def test_categories_menage_presentes_dans_formulaire(client):
    # Le nouveau modèle : les catégories ménage SONT visibles (elles ouvrent le parcours ménage).
    r = client.get("/fournisseurs/nouvelle")
    assert r.status_code == 200
    html = r.text
    for cat in ("CHG_003", "CHG_004", "CHG_027"):
        assert f'value="{cat}"' in html, cat


def test_source_saisie_inchangee_apres_previsualisation_phase2(tmp_path: Path):
    # Aucune écriture réelle : hash SAISIE inchangé.
    hash_avant = _sha256(cfg.SAISIE_CHARGES)
    previsualiser(_valid_form(), dryruns_root=tmp_path / "dryruns")
    previsualiser(_form_chg024(), dryruns_root=tmp_path / "dryruns2")
    hash_apres = _sha256(cfg.SAISIE_CHARGES)
    assert hash_avant == hash_apres

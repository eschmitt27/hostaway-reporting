"""Tests de schéma — migration profils d'impact + catégorie personnalisée CHG_024.

Lecture seule sur les référentiels réels (REF_Setup.xlsm, SAISIE_Charges_Flux.xlsx),
plus tests d'idempotence de la migration sur copies (tmp_path).
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.readers.saisie_charges_reader import (
    MANUAL_COL_MAP,
    read_ref_categories_charges,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.migrer_profils_impact import (  # noqa: E402
    FAMILLE_MENAGE,
    FAMILLE_PARCOURS_DEDIE,
    LST_TYPESFLUX_ADD,
    PROFILS_IMPACT,
    TYPE_FLUX_020_ID,
    TYPE_FLUX_020_ROW,
    migrate_ref_setup,
    migrate_ref_types_flux,
    migrate_saisie,
    run,
)
from app.readers.saisie_charges_reader import read_ref_types_flux  # noqa: E402

FAMILLES = {"GLOBAL", "LOGEMENT_DIRECT", "MENAGE", "PARCOURS_DEDIE"}
# Catégories reclassées PARCOURS_DEDIE (décision types flux)
RECLASSEES = {"CHG_012", "CHG_013", "CHG_015", "CHG_019"}


def _cats() -> dict[str, dict]:
    return {
        str(c.get("categorie_charge_id", "")).strip(): c
        for c in read_ref_categories_charges()
    }


def _types_flux() -> dict[str, dict]:
    return {
        str(t.get("type_flux_id", "")).strip(): t
        for t in read_ref_types_flux()
    }


# ── REF_Categories_Charges : colonnes profils ────────────────────────────────

def test_ref_categories_a_colonnes_profils():
    cats = _cats()
    sample = cats["CHG_005"]
    assert "famille_impact_categorie" in sample
    assert "profils_impact_autorises" in sample


def test_chaque_categorie_a_famille_valide():
    for cid, row in _cats().items():
        fam = str(row.get("famille_impact_categorie", "")).strip()
        assert fam in FAMILLES, f"{cid} famille={fam!r}"


def test_parcours_dedie_categories():
    cats = _cats()
    for cid in FAMILLE_PARCOURS_DEDIE:
        assert str(cats[cid]["famille_impact_categorie"]).strip() == "PARCOURS_DEDIE", cid
        assert str(cats[cid]["profils_impact_autorises"]).strip() == "PARCOURS_DEDIE", cid


def test_menage_categories_profils_intervenant_ou_logements():
    cats = _cats()
    for cid in FAMILLE_MENAGE:
        assert str(cats[cid]["famille_impact_categorie"]).strip() == "MENAGE", cid
        profils = str(cats[cid]["profils_impact_autorises"]).strip()
        assert profils == "MENAGE_INTERVENANT,MENAGE_LOGEMENTS", cid


def test_menage_externe_interne_restent_parcours_dedie():
    # CHG_001 (ménage externe) et CHG_002 (ménage interne) ne deviennent jamais
    # des choix ménage du formulaire standard.
    cats = _cats()
    for cid in ("CHG_001", "CHG_002"):
        assert str(cats[cid]["famille_impact_categorie"]).strip() == "PARCOURS_DEDIE", cid


def test_categories_global_profil_global():
    cats = _cats()
    for cid, row in cats.items():
        if str(row.get("famille_impact_categorie", "")).strip() == "GLOBAL":
            assert str(row["profils_impact_autorises"]).strip() == "GLOBAL", cid


# ── CHG_024 — catégorie personnalisée ────────────────────────────────────────

def test_chg024_present_et_global():
    cats = _cats()
    assert "CHG_024" in cats, "CHG_024 absent de REF_Categories_Charges"
    c = cats["CHG_024"]
    assert str(c["famille_impact_categorie"]).strip() == "GLOBAL"
    assert str(c["profils_impact_autorises"]).strip() == "GLOBAL"
    assert str(c["actif"]).strip().upper() == "OUI"


def test_chg024_regles_forcees():
    c = _cats()["CHG_024"]
    assert str(c["refacturable_defaut"]).strip().upper() == "NON"
    assert str(c["hors_compta_defaut"]).strip().upper() == "NON"
    assert str(c["filtre_vue_menage"]).strip().upper() == "NON"


# ── SAISIE_Charges_Flux : colonnes profil ────────────────────────────────────

def test_saisie_a_colonnes_profil():
    wb = openpyxl.load_workbook(str(cfg.SAISIE_CHARGES), read_only=True, data_only=True)
    try:
        headers = [str(c.value).strip() if c.value else "" for c in next(wb["SAISIE"].iter_rows(max_row=1))]
    finally:
        wb.close()
    assert "profil_impact_charge" in headers
    assert "libelle_categorie_personnalise" in headers


def test_manual_col_map_inclut_profil():
    assert MANUAL_COL_MAP.get("AH") == "profil_impact_charge"
    assert MANUAL_COL_MAP.get("AI") == "libelle_categorie_personnalise"


def test_reflocale_chg024_et_profils():
    wb = openpyxl.load_workbook(str(cfg.SAISIE_CHARGES), read_only=True, data_only=True)
    try:
        rl = wb["REF_LOCALE"]
        rows = list(rl.iter_rows(values_only=True))
    finally:
        wb.close()
    headers = [str(h).strip() if h else "" for h in rows[0]]
    assert "lst_Profils_Impact" in headers
    cat_col = headers.index("lst_Categories")
    cats = {str(r[cat_col]).strip() for r in rows[1:] if r[cat_col]}
    assert "CHG_024" in cats
    prof_col = headers.index("lst_Profils_Impact")
    profils = {str(r[prof_col]).strip() for r in rows[1:] if r[prof_col]}
    assert set(PROFILS_IMPACT) <= profils


# ── Idempotence (sur copies) ─────────────────────────────────────────────────

def test_migration_idempotente(tmp_path: Path):
    ref = tmp_path / "REF_Setup.xlsm"
    sai = tmp_path / "SAISIE.xlsx"
    shutil.copy2(str(cfg.REF_SETUP), str(ref))
    shutil.copy2(str(cfg.SAISIE_CHARGES), str(sai))
    # 1re passe : rien à ajouter (déjà migré dans les fichiers réels copiés)
    r1 = migrate_ref_setup(ref)
    s1 = migrate_saisie(sai)
    assert r1["colonnes_ajoutees"] == []
    assert r1["chg024_ajoute"] is False
    assert s1["colonnes_saisie"] == []
    assert s1["chg024_reflocale"] is False
    assert s1["lst_profils"] is False


def test_migration_run_backup_et_hash(tmp_path: Path):
    ref = tmp_path / "REF_Setup.xlsm"
    sai = tmp_path / "SAISIE.xlsx"
    shutil.copy2(str(cfg.REF_SETUP), str(ref))
    shutil.copy2(str(cfg.SAISIE_CHARGES), str(sai))
    out = run(ref, sai, tmp_path / "bak")
    assert len(out["backups"]) == 2
    assert out["ref_setup"]["hash_avant"] and out["ref_setup"]["hash_apres"]
    assert out["saisie"]["hash_avant"] and out["saisie"]["hash_apres"]
    # Déjà migré → hash inchangé (idempotent : réécriture identique de contenu)
    for f in (tmp_path / "bak" / "REF_Setup.xlsm", tmp_path / "bak" / "SAISIE.xlsx"):
        assert f.exists()


@pytest.mark.parametrize("profil", PROFILS_IMPACT)
def test_profils_impact_liste_canonique(profil):
    assert profil in {
        "GLOBAL", "LOGEMENT_DIRECT", "MENAGE_INTERVENANT", "MENAGE_LOGEMENTS", "PARCOURS_DEDIE"
    }


# ── TYPE_FLUX_020 + reclassement (migration types flux et parcours dédiés) ───

def test_type_flux_020_present_et_canonique():
    tf = _types_flux()
    assert TYPE_FLUX_020_ID in tf, "TYPE_FLUX_020 absent de REF_Types_Flux"
    row = tf[TYPE_FLUX_020_ID]
    assert str(row["type_flux"]).strip() == "CHARGE_SOCIETE_COMPTE_PRO"
    assert str(row["code_impact_defaut"]).strip() == "IC"
    assert str(row["comptabilisable_defaut"]).strip() == "OUI"
    assert str(row["actif"]).strip().upper() == "OUI"


def test_type_flux_020_unique():
    ids = [str(t.get("type_flux_id", "")).strip() for t in read_ref_types_flux()]
    assert ids.count(TYPE_FLUX_020_ID) == 1


def test_categories_reclassees_parcours_dedie():
    cats = _cats()
    for cid in RECLASSEES:
        assert str(cats[cid]["famille_impact_categorie"]).strip() == "PARCOURS_DEDIE", cid
        assert str(cats[cid]["profils_impact_autorises"]).strip() == "PARCOURS_DEDIE", cid


def test_categories_standard_restantes_global():
    cats = _cats()
    # CHG_018 reclassé MENAGE→GLOBAL (Achat divers) ; CHG_025/026 ajoutées GLOBAL.
    standard_attendu = {
        "CHG_005", "CHG_006", "CHG_007", "CHG_008", "CHG_009",
        "CHG_010", "CHG_011", "CHG_016", "CHG_017", "CHG_018",
        "CHG_024", "CHG_025", "CHG_026",
    }
    global_reel = {
        cid for cid, row in cats.items()
        if str(row.get("famille_impact_categorie", "")).strip() == "GLOBAL"
    }
    assert global_reel == standard_attendu


def test_nouvelles_categories_presentes():
    cats = _cats()
    assert "CHG_025" in cats and str(cats["CHG_025"]["famille_impact_categorie"]).strip() == "GLOBAL"
    assert "CHG_026" in cats and str(cats["CHG_026"]["famille_impact_categorie"]).strip() == "GLOBAL"
    assert "CHG_027" in cats and str(cats["CHG_027"]["famille_impact_categorie"]).strip() == "MENAGE"


def test_chg018_reclasse_global():
    assert str(_cats()["CHG_018"]["famille_impact_categorie"]).strip() == "GLOBAL"


def test_menage_categories_actuelles():
    cats = _cats()
    menage = {
        cid for cid, row in cats.items()
        if str(row.get("famille_impact_categorie", "")).strip() == "MENAGE"
    }
    assert menage == {"CHG_003", "CHG_004", "CHG_023", "CHG_027"}


def test_lst_typesflux_lot3_contient_tf016_tf020():
    wb = openpyxl.load_workbook(str(cfg.SAISIE_CHARGES), read_only=True, data_only=True)
    try:
        rl = wb["REF_LOCALE"]
        rows = list(rl.iter_rows(values_only=True))
    finally:
        wb.close()
    headers = [str(h).strip() if h else "" for h in rows[0]]
    col = headers.index("lst_TypesFlux_Lot3")
    vals = [str(r[col]).strip() for r in rows[1:] if r[col]]
    for tf in LST_TYPESFLUX_ADD:
        assert vals.count(tf) == 1, tf


def test_migration_types_flux_idempotente(tmp_path: Path):
    ref = tmp_path / "REF_Setup.xlsm"
    shutil.copy2(str(cfg.REF_SETUP), str(ref))
    # Fichier réel déjà migré → réapplication n'ajoute rien
    r = migrate_ref_types_flux(ref)
    assert r["tf020_ajoute"] is False


def test_migration_saisie_typesflux_idempotente(tmp_path: Path):
    sai = tmp_path / "SAISIE.xlsx"
    shutil.copy2(str(cfg.SAISIE_CHARGES), str(sai))
    s = migrate_saisie(sai)
    assert s["typesflux_ajoutes"] == []

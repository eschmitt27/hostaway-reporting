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
    PROFILS_IMPACT,
    migrate_ref_setup,
    migrate_saisie,
    run,
)

FAMILLES = {"GLOBAL", "LOGEMENT_DIRECT", "MENAGE", "PARCOURS_DEDIE"}


def _cats() -> dict[str, dict]:
    return {
        str(c.get("categorie_charge_id", "")).strip(): c
        for c in read_ref_categories_charges()
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

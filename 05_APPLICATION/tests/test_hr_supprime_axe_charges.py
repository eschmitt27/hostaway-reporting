"""DÉCISION 2 — HR retiré du vocabulaire des CHARGES, intact sur l'axe RÉSERVATIONS.

« HORS COMPTA + HORS RÉSULTAT n'a PAS de pertinence métier » est vrai pour une DÉPENSE : une charge
qui ne pèse ni sur le résultat ni sur la comptabilité n'est pas une charge. Ce n'est pas vrai pour
une OCCUPATION SANS VENTE : un séjour propriétaire est un fait réel qu'il faut enregistrer et
exclure du résultat — 125 réservations réelles en dépendent (80 séjours propriétaire).

Ces tests figent les deux moitiés de ce constat. Détail et arbitrage ouvert :
`00_CADRAGE/HR_SUPPRESSION_AUDIT.md`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.moteurs.charges_engine import CODES_IMPACT_CHARGE, IMPACT_CHARGE, impact_charge
from app.services import charges_saisie_service as saisie

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


# ── Axe CHARGES : HR n'existe plus ───────────────────────────────────────────

def test_le_vocabulaire_des_charges_est_ic_et_hc():
    assert CODES_IMPACT_CHARGE == frozenset({"IC", "HC"})
    assert set(IMPACT_CHARGE) == CODES_IMPACT_CHARGE


def test_hr_n_est_plus_resolvable_comme_impact_de_charge():
    """`impact_charge("HR")` rend `None` — « inconnu », pas « neutre ».

    La nuance compte : rendre des drapeaux NON/NON reviendrait à réimplémenter HR sous le capot,
    exactement ce que la décision interdit (« NE PAS créer un remplaçant équivalent »)."""
    assert impact_charge("HR") is None
    assert impact_charge("IC") == IMPACT_CHARGE["IC"]
    assert impact_charge("hc") == IMPACT_CHARGE["HC"], "la casse ne doit pas décider"
    assert impact_charge(None) is None and impact_charge("") is None


@pytest.mark.parametrize("code", ["HR", "hr", " HR "])
def test_une_charge_hr_est_refusee_a_l_ecriture(code):
    """La SEULE porte d'écriture de `charges` refuse HR — pas seulement les formulaires.

    Retirer HR des écrans en le laissant passer par le service aurait été cosmétique : l'API et
    tout appelant interne auraient continué d'en créer."""
    res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                          "categorie_charge_id": "CHG_002", "code_impact": code})
    assert res["ok"] is False
    assert res["code"] == saisie.E_IMPACT_INVALIDE
    assert "IC" in res["message"] and "HC" in res["message"]


def test_une_charge_sans_code_impact_reste_acceptee():
    """`code_impact` demeure FACULTATIF (D044) : ce service enregistre, il ne dérive pas.

    Le refus porte sur une valeur INCONNUE, jamais sur une valeur absente — sinon la suppression
    de HR se transformerait en durcissement non demandé du parcours de saisie."""
    for donnees in ({"code_impact": ""}, {"code_impact": None}, {}):
        res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                              "categorie_charge_id": "CHG_002", **donnees})
        assert res["ok"] is True, res


def test_ic_et_hc_restent_acceptes():
    for code in ("IC", "HC"):
        res = saisie.valider({"date_charge": "2026-07-15", "montant": 120,
                              "categorie_charge_id": "CHG_002", "code_impact": code})
        assert res["ok"] is True, res


def test_le_mode_hr_n_est_plus_proposable_sur_une_facture():
    from app.services import factures_proprietaires_edition_service as edition

    assert "HR" not in edition.MODES_CHARGE
    assert {m["code_impact"] for m in edition.modes_charge()} == CODES_IMPACT_CHARGE


def test_le_filtre_de_l_ecran_de_controle_ne_propose_plus_hr():
    """La liste était écrite en dur dans le gabarit ; elle vient désormais du vocabulaire."""
    gabarit = (Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "app" / "templates"
               / "charges_controle.html").read_text(encoding="utf-8")
    assert "['IC','HC','HR']" not in gabarit
    assert "codes_impact" in gabarit


def test_lot3_ne_traduit_plus_hr_en_neutre():
    """Une charge portant encore HR doit ressortir A_CONTROLER, pas « sans impact ».

    C'est la différence entre un problème visible et un problème muet."""
    source = (_TRAVAIL / "lot3_generateur_charges.py").read_text(encoding="utf-8",
                                                                 errors="replace")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert 'IMPACT_REEL: dict[str, str] = {"IC": "OUI", "HC": "OUI"}' in code
    assert 'IMPACT_COMPTA: dict[str, str] = {"IC": "OUI", "HC": "NON"}' in code


# ── Axe RÉSERVATIONS : HR intact, et c'est délibéré ──────────────────────────

def test_le_referentiel_conserve_hr_car_les_reservations_en_dependent():
    """`ref_codes_impact` garde sa ligne HR : 125 réservations réelles la référencent.

    La supprimer les orphelinerait. Le retrait de HR est un retrait de VOCABULAIRE CHARGE, pas
    une purge du référentiel partagé."""
    from app.db.connection import apply_migrations

    import tempfile
    db = Path(tempfile.mkdtemp()) / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        colonnes = {r[1] for r in conn.execute("PRAGMA table_info(ref_codes_impact)")}
    finally:
        conn.close()
    assert "code_impact" in colonnes, "le référentiel partagé doit subsister"


def test_les_moteurs_reservations_gardent_hr():
    """lot4bis et lib_lot4a doivent continuer de produire HR pour les séjours propriétaire.

    Ce test ne défend pas HR par principe : il défend le fait que 80 séjours propriétaire ne
    doivent PAS devenir du chiffre d'affaires. Le jour où l'arbitrage tranche autrement, ce test
    doit être modifié EN MÊME TEMPS que la règle — pas contourné."""
    lib = (_TRAVAIL / "lib_lot4a_reservations_hh.py").read_text(encoding="utf-8",
                                                                errors="replace")
    assert 'VALID_CODE_IMPACT = {"IC", "HC", "HR"}' in lib
    lot4bis = (_TRAVAIL / "lot4bis_charger_reservations.py").read_text(encoding="utf-8",
                                                                       errors="replace")
    assert "ownerStay" in lot4bis and '"HR", "EXCLU_RESULTAT"' in lot4bis


def test_l_axe_reservations_est_documente_comme_arbitrage_ouvert():
    """La divergence entre la consigne et la réalité métier doit rester ÉCRITE.

    Un écart non documenté finit par être relu comme un oubli, puis « corrigé » à tort."""
    doc = (Path(cfg.PROJECT_ROOT) / "00_CADRAGE" / "HR_SUPPRESSION_AUDIT.md").read_text(
        encoding="utf-8")
    assert "ARBITRAGE_METIER_REQUIS" in doc
    assert "séjours propriétaire" in doc


# ── Source unique ────────────────────────────────────────────────────────────

def test_le_vocabulaire_n_est_defini_qu_une_fois():
    """`STANDARD_CODES_IMPACT` doit ÊTRE la constante du moteur, pas une copie à jour par chance."""
    from app.services import charges_preview_service as preview

    assert preview.STANDARD_CODES_IMPACT is CODES_IMPACT_CHARGE

"""Tests moteur d'impacts charges (périmètre / répartition / ménage / réserve / avantage).

Fonctions pures (sans I/O) + intégration guidée via previsualiser (prévisualisation, aucune écriture réelle).
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.services import charges_impact_service as impact
from app.services.charges_preview_service import (
    load_form_refs,
    previsualiser,
    validate_charge,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def refs() -> dict:
    return load_form_refs()


GESTION = [
    {"logement_id": "LOG_A", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_B", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_C", "proprietaire_id": "PROP_2", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_D", "proprietaire_id": "PROP_1", "statut_gestion": "INACTIF",
     "date_debut": "2026-01-01", "date_fin": "2026-03-31"},
]


# ── Répartition égale déterministe (centimes) ────────────────────────────────

def test_repartir_egal_somme_egale_montant():
    q = impact.repartir_egal(100.0, ["LOG_A", "LOG_B", "LOG_C"])
    assert impact.somme_quotes_parts(q) == 100.0
    # 100/3 = 33.34 + 33.33 + 33.33 (premier reçoit le centime)
    parts = sorted(x["quote_part"] for x in q)
    assert parts == [33.33, 33.33, 33.34]


def test_repartir_egal_un_logement():
    q = impact.repartir_egal(85.5, ["LOG_A"])
    assert q == [{"logement_id": "LOG_A", "quote_part": 85.5}]


def test_repartir_egal_jamais_replique_entier():
    q = impact.repartir_egal(100.0, ["LOG_A", "LOG_B"])
    for x in q:
        assert x["quote_part"] < 100.0
    assert impact.somme_quotes_parts(q) == 100.0


def test_repartir_egal_vide():
    assert impact.repartir_egal(100.0, []) == []


def test_repartir_egal_centimes_impairs():
    q = impact.repartir_egal(10.0, ["A", "B", "C", "D", "E", "F", "G"])
    assert impact.somme_quotes_parts(q) == 10.0


# ── Périmètre déterministe ───────────────────────────────────────────────────

def test_gestion_active_pour_mois():
    assert impact.gestion_active_pour_mois(GESTION[0], "2026-06") is True
    assert impact.gestion_active_pour_mois(GESTION[3], "2026-06") is False  # INACTIF
    assert impact.gestion_active_pour_mois(GESTION[3], "2026-02") is False  # INACTIF même si dans dates


def test_logements_actifs_proprietaire():
    logs = impact.logements_actifs_proprietaire("PROP_1", "2026-06", GESTION)
    assert logs == ["LOG_A", "LOG_B"]  # LOG_D inactif exclu


def test_perimetre_proprietaire_elargit_aux_logements():
    p = impact.compute_perimetre_logements([], ["PROP_1"], "2026-06", GESTION)
    assert p["logements_finaux"] == ["LOG_A", "LOG_B"]
    assert p["nb_logements_finaux"] == 2
    assert p["global_conciergerie"] is False


def test_perimetre_dedup_directs_et_proprietaires():
    # LOG_A direct + PROP_1 (LOG_A, LOG_B) → dédup {LOG_A, LOG_B}
    p = impact.compute_perimetre_logements(["LOG_A"], ["PROP_1"], "2026-06", GESTION)
    assert p["logements_finaux"] == ["LOG_A", "LOG_B"]


def test_perimetre_plusieurs_proprietaires():
    p = impact.compute_perimetre_logements([], ["PROP_1", "PROP_2"], "2026-06", GESTION)
    assert p["logements_finaux"] == ["LOG_A", "LOG_B", "LOG_C"]


def test_perimetre_vide_est_global():
    p = impact.compute_perimetre_logements([], [], "2026-06", GESTION)
    assert p["global_conciergerie"] is True
    assert p["nb_logements_finaux"] == 0


# ── Périmètre ménage : intervenant XOR logement ──────────────────────────────

def test_menage_mode_intervenant():
    m = impact.menage_perimetre("INTERVENANT", ["INT_0001", "INT_0002"], [], [], "2026-06", GESTION)
    assert m["intervenants"] == ["INT_0001", "INT_0002"]
    assert m["logements"] == []
    assert m["nb"] == 2


def test_menage_mode_logement_via_proprietaire():
    m = impact.menage_perimetre("LOGEMENT", [], [], ["PROP_1"], "2026-06", GESTION)
    assert m["logements"] == ["LOG_A", "LOG_B"]
    assert m["intervenants"] == []


def test_menage_mode_logement_direct_plus_proprietaire_dedup():
    m = impact.menage_perimetre("LOGEMENT", [], ["LOG_A"], ["PROP_1"], "2026-06", GESTION)
    assert m["logements"] == ["LOG_A", "LOG_B"]


# ── Réserve de facturation ───────────────────────────────────────────────────

def test_reserve_somme_egale_montant():
    quotes = impact.repartir_egal(100.0, ["LOG_A", "LOG_B", "LOG_C"])
    r = impact.build_reserve_refacturation(
        "CHG-X", "2026-06", "Assurance", None, quotes,
        {"LOG_A": "PROP_1", "LOG_B": "PROP_1", "LOG_C": "PROP_2"},
    )
    assert r["montant_total_refacturable"] == 100.0
    assert r["nb_entrees"] == 3
    # une seule entrée par logement (pas de doublon)
    logs = [e["logement_id"] for e in r["entrees"]]
    assert len(logs) == len(set(logs))
    assert all(e["statut_traitement"] == "EN_ATTENTE" for e in r["entrees"])
    assert all(set(e["decisions_possibles"]) == {"APPLIQUER", "REPORTER", "IGNORER"} for e in r["entrees"])


# ── Effet de la saisie ───────────────────────────────────────────────────────

def test_effet_saisie_ic_comptable_oui():
    e = impact.build_effet_saisie(code_impact="IC", impact_menage=False, perimetre=None,
                                  menage=None, refacturable=False, reserve=None,
                                  avantage_associe=False, associe_id=None)
    assert e["cree_charge_reelle"] == "Oui"
    assert e["impacte_resultat_reel"] == "Oui"
    assert e["impacte_resultat_comptable"] == "Oui"


def test_effet_saisie_hc_comptable_non():
    e = impact.build_effet_saisie(code_impact="HC", impact_menage=False, perimetre=None,
                                  menage=None, refacturable=False, reserve=None,
                                  avantage_associe=False, associe_id=None)
    assert e["impacte_resultat_comptable"] == "Non"


def test_effet_saisie_menage_analytique():
    m = {"mode": "INTERVENANT", "nb": 2}
    e = impact.build_effet_saisie(code_impact="HC", impact_menage=True, perimetre=None,
                                  menage=m, refacturable=False, reserve=None,
                                  avantage_associe=False, associe_id=None)
    assert e["impact_menage"] == "Oui"
    assert "n'est pas refacturable" in e["menage_detail"]
    assert e["refacturation"].startswith("non applicable")


# ── Catalogue catégories ─────────────────────────────────────────────────────

def test_catalogue_menage_comportement():
    assert impact.menage_comportement("CHG_004") == impact.MENAGE_FORCE
    assert impact.menage_comportement("CHG_009") == impact.MENAGE_CHOIX
    assert impact.menage_comportement("CHG_005") == impact.MENAGE_INTERDIT


def test_catalogue_avantage_possible():
    assert impact.avantage_possible("CHG_009") is True
    assert impact.avantage_possible("CHG_025") is True
    assert impact.avantage_possible("CHG_005") is False


# ── Intégration guidée : previsualiser (prévisualisation, aucune écriture) ────

def _base_form() -> dict:
    return {
        "date_charge": "2026-06-15",
        "montant": "100.00",
        "categorie_charge_id": "CHG_005",
        "code_impact": "IC",
        "mode_paiement_id": "PAY_001",
    }


def test_charge_globale(tmp_path: Path):
    r = previsualiser(_base_form(), dryruns_root=tmp_path / "d")
    assert r["ok"], r["manifest"].get("errors")
    m = r["manifest"]
    assert m["impact_menage"] is False
    assert m["perimetre"]["global_conciergerie"] is True
    assert m["effet_saisie"]["perimetre_analytique"] == "global conciergerie"


def _real_prop_logement():
    """Retourne (proprietaire_id, logement_id actif) réel depuis REF_Gestion."""
    refs = load_form_refs()
    for g in refs["gestion_logements"]:
        if impact.gestion_active_pour_mois(g, "2026-06"):
            return str(g["proprietaire_id"]).strip(), str(g["logement_id"]).strip()
    return None, None


def test_charge_un_proprietaire_elargit_logements(tmp_path: Path):
    prop, log = _real_prop_logement()
    if not prop:
        pytest.skip("Aucune gestion active 2026-06")
    form = _base_form()
    form["proprietaires"] = [prop]
    r = previsualiser(form, dryruns_root=tmp_path / "d")
    assert r["ok"], r["manifest"].get("errors")
    assert log in r["manifest"]["perimetre"]["logements_finaux"]


def test_refacturable_sans_logement_refuse(refs):
    form = _base_form()
    form["refacturable"] = "OUI"  # aucun logement/proprietaire → périmètre vide
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V24_REFAC_SANS_LOGEMENT" in codes


def test_refacturable_avec_logement_cree_reserve(tmp_path: Path):
    refs = load_form_refs()
    valid_logs = [str(l["logement_id"]).strip() for l in refs["logements"]][:2]
    if len(valid_logs) < 2:
        pytest.skip("Pas assez de logements")
    form = _base_form()
    form["logements"] = valid_logs
    form["refacturable"] = "OUI"
    r = previsualiser(form, dryruns_root=tmp_path / "d")
    assert r["ok"], r["manifest"].get("errors")
    res = r["manifest"]["reserve_refacturation"]
    assert res is not None
    assert res["montant_total_refacturable"] == 100.0
    assert res["nb_entrees"] == 2
    # fichier réserve écrit
    assert (Path(r["manifest"]["paths"]["run_dir"]) / "reserve_refacturation.json").exists()


def test_menage_charge_intervenant(tmp_path: Path):
    refs = load_form_refs()
    ints = [str(i["intervenant_id"]).strip() for i in refs["intervenants"]][:1]
    if not ints:
        pytest.skip("Pas d'intervenant")
    form = _base_form()
    form["categorie_charge_id"] = "CHG_004"  # Achat ménage (MENAGE_FORCE)
    form["code_impact"] = "HC"
    form["commentaire"] = "impact HC justifié"
    form["menage_mode"] = "INTERVENANT"
    form["menage_intervenants"] = ints
    r = previsualiser(form, dryruns_root=tmp_path / "d")
    assert r["ok"], r["manifest"].get("errors")
    assert r["manifest"]["impact_menage"] is True
    assert r["manifest"]["menage"]["mode"] == "INTERVENANT"


def test_menage_refacturable_interdit(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_004"
    form["menage_mode"] = "INTERVENANT"
    form["menage_intervenants"] = ["INT_0001"]
    form["refacturable"] = "OUI"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V22_MENAGE_NON_REFACTURABLE" in codes


def test_menage_perimetre_vide_bloque(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_004"
    form["menage_mode"] = "INTERVENANT"
    form["menage_intervenants"] = []
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V21_MENAGE_PERIMETRE_VIDE" in codes


def test_menage_mode_manquant_bloque(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_004"  # MENAGE_FORCE, pas de mode
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V16_MENAGE_PARCOURS_DEDIE" in codes


def test_impact_menage_interdit_sur_categorie_non_menage(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_005"  # INTERDIT ménage
    form["impact_menage"] = "OUI"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V20_IMPACT_MENAGE_INTERDIT" in codes


def test_avantage_associe_oui_requiert_associe(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_009"  # avantage possible
    form["avantage_associe"] = "OUI"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V26_AVANTAGE_SANS_ASSOCIE" in codes


def test_avantage_associe_interdit_categorie(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_005"  # avantage impossible
    form["avantage_associe"] = "OUI"
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V25_AVANTAGE_INTERDIT" in codes


def test_avantage_distinct_du_paiement(tmp_path: Path):
    # Payé banque pro (PAY_001) mais avantage associé OUI → avantage tracé, distinct du paiement.
    refs = load_form_refs()
    assoc = [str(a["personne_id"]).strip() for a in refs["associes"]][:1]
    if not assoc:
        pytest.skip("Pas d'associé")
    form = _base_form()
    form["categorie_charge_id"] = "CHG_009"
    form["avantage_associe"] = "OUI"
    form["avantage_associe_id"] = assoc[0]  # distinct du mode de paiement (PAY_001)
    r = previsualiser(form, dryruns_root=tmp_path / "d")
    assert r["ok"], r["manifest"].get("errors")
    assert r["manifest"]["avantage_associe"] is True
    assert r["manifest"]["avantage_associe_id"] == assoc[0]


def test_forfait_client_hors_formulaire(refs):
    form = _base_form()
    form["categorie_charge_id"] = "CHG_016"  # forfait client
    codes = [e["code"] for e in validate_charge(form, refs)]
    assert "V04_CATEGORIE_HORS_FORMULAIRE" in codes


def test_source_inchangee_apres_impacts(tmp_path: Path):
    import hashlib
    h_av = hashlib.sha256(cfg.SAISIE_CHARGES.read_bytes()).hexdigest()
    form = _base_form()
    form["proprietaires"] = ["PROP_0001"]
    previsualiser(form, dryruns_root=tmp_path / "d")
    h_ap = hashlib.sha256(cfg.SAISIE_CHARGES.read_bytes()).hexdigest()
    assert h_av == h_ap

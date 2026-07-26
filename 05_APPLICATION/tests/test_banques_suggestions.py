"""Moteur de suggestions de rapprochement : scores explicables, niveaux, refus persistant,
acceptation sans validation silencieuse."""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations
from app.services import banques_rapprochement_service as rappro
from app.services import banques_suggestions_service as svc

MOUVEMENT = {
    "id_opaque": "MVT-TEST01", "montant": 850.0, "date_operation": "2026-06-05",
    "libelle": "VIR HOSTAWAY PAYOUT RES_A1",
}


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


# ── Scoring ───────────────────────────────────────────────────────────────────

def test_score_exact_reference_montant_et_date():
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0,
         "date": "2026-06-05"}
    s = svc.evaluer(MOUVEMENT, c)
    assert s["niveau"] == svc.NIVEAU_EXACT
    assert s["score"] >= svc.SEUIL_EXACT
    assert any("identifiant" in x for x in s["criteres_concordants"])
    assert "montant exact" in s["criteres_concordants"]
    assert "date exacte" in s["criteres_concordants"]
    assert s["criteres_divergents"] == []


def test_score_probable_montant_et_date_proche_sans_reference():
    c = {"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_999", "montant": 850.0,
         "date": "2026-06-07"}
    s = svc.evaluer(MOUVEMENT, c)
    assert s["niveau"] == svc.NIVEAU_PROBABLE
    assert "montant exact" in s["criteres_concordants"]


def test_score_faible_et_criteres_divergents():
    c = {"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_888", "montant": 12.0,
         "date": "2020-01-01"}
    s = svc.evaluer(MOUVEMENT, c)
    assert s["niveau"] == svc.NIVEAU_FAIBLE
    assert any("date éloignée" in x for x in s["criteres_divergents"])


def test_montant_superieur_au_disponible_est_divergent():
    c = {"type_objet": "RESERVATION", "objet_id": "RES_X", "montant": 5000.0,
         "date": "2026-06-05"}
    s = svc.evaluer(MOUVEMENT, c, montant_disponible=850.0)
    assert any("supérieur au disponible" in x for x in s["criteres_divergents"])


def test_montant_compatible_partiel_est_concordant():
    c = {"type_objet": "RESERVATION", "objet_id": "RES_P", "montant": 300.0,
         "date": "2026-06-05"}
    s = svc.evaluer(MOUVEMENT, c, montant_disponible=850.0)
    assert any("partiel" in x for x in s["criteres_concordants"])
    assert s["montant_propose"] == 300.0


# ── Suggestions (filtrage, décisions) ────────────────────────────────────────

def test_suggerer_trie_par_score(db):
    candidats = [
        {"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_888", "montant": 850.0, "date": "2026-06-20"},
        {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"},
    ]
    props = svc.suggerer(MOUVEMENT, candidats, db_path=db)
    assert len(props) == 2
    assert props[0]["objet_id"] == "RES_A1"          # meilleur score en tête
    assert props[0]["niveau"] == svc.NIVEAU_EXACT


def test_aucun_candidat_aucune_suggestion(db):
    props = svc.suggerer(MOUVEMENT, [], db_path=db)
    assert props == []


def test_candidat_sans_critere_concordant_est_ecarte(db):
    c = [{"type_objet": "RESERVATION", "objet_id": "RES_ZZ", "montant": 0, "date": ""}]
    assert svc.suggerer(MOUVEMENT, c, db_path=db) == []


def test_suggestion_refusee_ne_reapparait_pas(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    assert len(props) == 1

    svc.refuser(MOUVEMENT, props[0], acteur="recette", commentaire="pas cette réservation", db_path=db)
    assert svc.suggerer(MOUVEMENT, [c], db_path=db) == []      # ne réapparaît pas


def test_suggestion_refusee_reapparait_si_objet_modifie(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    svc.refuser(MOUVEMENT, props[0], acteur="recette", db_path=db)
    assert svc.suggerer(MOUVEMENT, [c], db_path=db) == []

    # L'objet change (montant corrigé) -> l'empreinte change -> la suggestion redevient proposable.
    c2 = dict(c, montant=900.0)
    props2 = svc.suggerer(MOUVEMENT, [c2], db_path=db)
    assert len(props2) == 1
    assert props2[0]["deja_refusee_puis_modifiee"] is True


def test_ignorer_temporairement_masque_aussi(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    svc.refuser(MOUVEMENT, props[0], definitif=False, acteur="recette", db_path=db)
    assert svc.suggerer(MOUVEMENT, [c], db_path=db) == []
    hist = svc.historique_decisions("MVT-TEST01", db_path=db)
    assert hist[0]["decision"] == svc.DEC_IGNOREE


# ── Acceptation : jamais de validation silencieuse ───────────────────────────

def test_accepter_cree_un_rapprochement_au_statut_propose(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    assert props[0]["niveau"] == svc.NIVEAU_EXACT          # même EXACT...

    res = svc.accepter(MOUVEMENT, props[0], acteur="recette", db_path=db)
    assert res["ok"], res
    liens = rappro.lister("MVT-TEST01", db_path=db)
    assert len(liens) == 1
    assert liens[0]["statut"] == rappro.ST_PROPOSE          # ...reste à confirmer par un humain
    assert liens[0]["source"] == "AUTO"
    assert liens[0]["score_explicable"] == props[0]["score"]


def test_accepter_avec_montant_modifie(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    res = svc.accepter(MOUVEMENT, props[0], montant=400.0, acteur="recette", db_path=db)
    assert res["ok"] and res["montant_rapproche"] == 400.0
    etat = rappro.etat_rapprochement("MVT-TEST01", 850.0, db_path=db)
    assert etat["statut"] == "PARTIEL" and etat["montant_restant"] == 450.0


def test_plus_de_suggestion_si_mouvement_entierement_rapproche(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    svc.accepter(MOUVEMENT, props[0], acteur="recette", db_path=db)
    autre = {"type_objet": "RESERVATION", "objet_id": "RES_B2", "montant": 850.0, "date": "2026-06-05"}
    assert svc.suggerer(MOUVEMENT, [autre], db_path=db) == []


def test_historique_des_decisions_conserve(db):
    c = {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0, "date": "2026-06-05"}
    props = svc.suggerer(MOUVEMENT, [c], db_path=db)
    svc.refuser(MOUVEMENT, props[0], acteur="recette", commentaire="non", db_path=db)
    c2 = dict(c, montant=900.0)
    props2 = svc.suggerer(MOUVEMENT, [c2], db_path=db)
    svc.accepter(MOUVEMENT, props2[0], acteur="recette", db_path=db)

    hist = svc.historique_decisions("MVT-TEST01", db_path=db)
    assert len(hist) == 2
    assert {h["decision"] for h in hist} == {svc.DEC_REFUSEE, svc.DEC_ACCEPTEE}
    assert all(h["acteur"] == "recette" for h in hist)

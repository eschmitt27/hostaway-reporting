"""Mission 6 quater — deux parcours distincts pour les référentiels temporels :

A. CHANGEMENT NORMAL (date future) — aucune justification exigée, action journalisée normale.
B. CORRECTION RÉTROACTIVE (date passée ou aujourd'hui) — justification obligatoire, contrôlée
   côté BACKEND (la route refuse avant tout appel au service d'écriture, pas seulement le HTML),
   journalisée sous l'action `CORRECTION_RETROACTIVE`.

Réutilise entièrement l'infrastructure existante (transaction atomique, journal
`ref_admin_evenements`, refus de chevauchement) — ce fichier couvre uniquement ce qui est nouveau :
`referentiel_admin_service.est_retroactif`/`verifier_justification_retroactive`, le paramètre
`justification` des 4 services de gestion, et l'enforcement dans les routes.
"""
import datetime as dt

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import canape_gestion_service as canape
from app.services import couts_menage_gestion_service as cm
from app.services import logements_gestion_service as logs
from app.services import referentiel_admin_service as adm
from app.services import regle_version_gestion_service as regv

DEMAIN = (dt.date.today() + dt.timedelta(days=30)).isoformat()
HIER = (dt.date.today() - dt.timedelta(days=30)).isoformat()


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


# ── est_retroactif / verifier_justification_retroactive (résolveur pur) ─────────────────────────

def test_date_future_nest_pas_retroactive():
    assert adm.est_retroactif(DEMAIN, aujourdhui=dt.date.today()) is False


def test_date_passee_est_retroactive():
    assert adm.est_retroactif(HIER, aujourdhui=dt.date.today()) is True


def test_aujourdhui_est_retroactif():
    """Aujourd'hui compris comme rétroactif (§4 : période déjà commencée, jamais purement à
    venir)."""
    today = dt.date.today().isoformat()
    assert adm.est_retroactif(today, aujourdhui=dt.date.today()) is True


def test_verifier_justification_ok_si_futur_sans_justification():
    assert adm.verifier_justification_retroactive(DEMAIN, "") is None


def test_verifier_justification_refuse_si_retroactif_sans_justification():
    res = adm.verifier_justification_retroactive(HIER, "")
    assert res is not None and res["code"] == adm.E_JUSTIFICATION_REQUISE


def test_verifier_justification_ok_si_retroactif_avec_justification():
    assert adm.verifier_justification_retroactive(HIER, "Erreur de date d'effet") is None


# ── Parcours A : changement futur (route logements) ─────────────────────────────────────────────

def test_changement_futur_ne_demande_aucune_justification(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/logements/LOG_A1/changer-taux-commission",
                       data={"taux_commission": "0.20", "date_debut": DEMAIN},
                       follow_redirects=False)
    assert resp.status_code == 303
    assert "erreur" not in resp.headers["location"]
    evts = adm.historique_evenements("ref_taux_commission", db_path=tmp_db)
    dernier = [e for e in evts if e["action"] != "CLOTURE_PERIODE"][0]
    assert dernier["action"] == "CHANGEMENT_TAUX"


# ── Parcours B : correction rétroactive (route logements) ──────────────────────────────────────

def test_correction_retroactive_refusee_sans_justification(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/logements/LOG_A1/changer-taux-commission",
                       data={"taux_commission": "0.20", "date_debut": HIER},
                       follow_redirects=False)
    assert resp.status_code == 303
    assert "erreur" in resp.headers["location"]
    # Aucune écriture n'a eu lieu : le taux d'origine reste seul actif.
    hist = logs.historique("LOG_A1", db_path=tmp_db)
    ouverts = [t for t in hist["taux"] if not t["date_fin"]]
    assert len(ouverts) == 1 and float(ouverts[0]["taux_commission"]) == 0.19


def test_correction_retroactive_acceptee_avec_justification(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/logements/LOG_A1/changer-taux-commission",
                       data={"taux_commission": "0.20", "date_debut": HIER,
                             "justification": "Erreur de date d'effet"},
                       follow_redirects=False)
    assert resp.status_code == 303
    assert "erreur" not in resp.headers["location"]

    evts = adm.historique_evenements("ref_taux_commission", db_path=tmp_db)
    ouverture = [e for e in evts if e["action"] == "CORRECTION_RETROACTIVE"]
    assert len(ouverture) >= 1
    assert ouverture[0]["commentaire"] == "Erreur de date d'effet"


# ── Justification conservée dans l'audit pour les 4 services ────────────────────────────────────

def test_justification_conservee_couts_menage(ref):
    res = cm.changer_cout("TYPE_001", 55, HIER, justification="Correction facture", db_path=ref)
    assert res["ok"], res
    evts = adm.historique_evenements("ref_couts_standards_menage", db_path=ref)
    corrections = [e for e in evts if e["action"] == "CORRECTION_RETROACTIVE"]
    assert corrections and corrections[0]["commentaire"] == "Correction facture"


def test_justification_conservee_canape(ref):
    res = canape.changer_parametres("LOG_A1", 5, 30, HIER, justification="Correction", db_path=ref)
    assert res["ok"], res
    evts = adm.historique_evenements("ref_canape_parametres", db_path=ref)
    corrections = [e for e in evts if e["action"] == "CORRECTION_RETROACTIVE"]
    assert corrections and corrections[0]["commentaire"] == "Correction"


def test_justification_conservee_regle_version(ref):
    res = regv.changer_version("REGLE_TEST", "V2", HIER, commentaire="Correction", db_path=ref)
    assert res["ok"], res
    evts = adm.historique_evenements("ref_regles_versions", db_path=ref)
    corrections = [e for e in evts if e["action"] == "CORRECTION_RETROACTIVE"]
    assert corrections and corrections[0]["commentaire"] == "Correction"


def test_changement_futur_action_normale_pas_correction(ref):
    """Un changement futur n'est jamais journalisé comme CORRECTION_RETROACTIVE, même sans
    justification fournie."""
    res = cm.changer_cout("TYPE_001", 55, DEMAIN, db_path=ref)
    assert res["ok"], res
    evts = adm.historique_evenements("ref_couts_standards_menage", db_path=ref)
    dernier = [e for e in evts if e["action"] != "CLOTURE_PERIODE"][0]
    assert dernier["action"] == "CHANGEMENT_COUT_MENAGE"


# ── Chevauchement toujours bloqué (non-régression) ──────────────────────────────────────────────

def test_chevauchement_toujours_refuse_apres_ajout_justification(ref):
    cm.changer_cout("TYPE_001", 45, "2026-01-01", db_path=ref)
    res = adm.inserer("ref_couts_standards_menage", {
        "cout_standard_id": "CSM_MANUEL", "type_logement_id": "TYPE_001",
        "cout_standard_menage": "99", "date_debut_validite": "2026-01-01",
        "date_fin_validite": "", "actif": "OUI"}, action="CREATION", db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_PERIODE_INCOHERENTE


# ── Trous de période : impossibles par construction (pas une détection à part) ──────────────────

def test_aucun_trou_possible_par_construction(ref):
    """Les 4 services ferment systématiquement à `veille(date_debut)` de la nouvelle période :
    aucun trou ne peut être créé par le parcours normal, jamais une politique de détection
    séparée n'est nécessaire pour ces référentiels."""
    cm.changer_cout("TYPE_001", 45, "2026-01-01", db_path=ref)
    cm.changer_cout("TYPE_001", 55, "2026-07-01", db_path=ref)
    lignes = cm.historique("TYPE_001", db_path=ref)
    close = [l for l in lignes if l["date_fin_validite"]][0]
    ouverte = [l for l in lignes if not l["date_fin_validite"]][0]
    fin_close = adm.txt(close["date_fin_validite"])
    debut_ouverte = adm.txt(ouverte["date_debut_validite"])
    assert (dt.date.fromisoformat(debut_ouverte) - dt.date.fromisoformat(fin_close)).days == 1

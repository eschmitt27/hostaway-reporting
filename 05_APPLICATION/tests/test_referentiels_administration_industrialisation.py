"""Mission « référentiels SQLite administrables » — les manques réels identifiés à l'audit.

L'essentiel existait déjà (`referentiel_admin_service`, `logements_gestion_service`,
`fournisseurs_referentiel_service`) : ce fichier couvre les 4 manques réels comblés par cette
mission, pas ce qui fonctionnait déjà (voir `test_logements_gestion.py`, `test_fournisseurs_
referentiel.py`, déjà verts).

1. Atomicité clôture+ouverture (`referentiel_admin_service.transaction`) : si l'ouverture échoue
   après une clôture, celle-ci est annulée — jamais de logement sans période ouverte.
2. Historisation de `ref_couts_standards_menage` (nouveau `couts_menage_gestion_service`), jusque-là
   librement éditable par l'écran générique sans discipline de clôture/ouverture.
3. Refus de chevauchement avec une période déjà CLOSE (au-delà du refus déjà existant sur une
   période déjà OUVERTE).
4. Garde de cohérence propriétaire↔logement actif à la désactivation.
"""
import sqlite3

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import couts_menage_gestion_service as cm
from app.services import logements_gestion_service as svc
from app.services import referentiel_admin_service as adm


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _gestion(db_path):
    return [r for r in fx.lignes(db_path, "ref_gestion_logements_hist")]


def _taux(db_path):
    return [r for r in fx.lignes(db_path, "ref_taux_commission")]


# ── 1. Atomicité clôture + ouverture ─────────────────────────────────────────────────────────────

def test_changer_proprietaire_rollback_si_ouverture_echoue(ref):
    """Une collision de clé sur la nouvelle ligne (simulée) ne doit jamais laisser l'ancienne
    période close sans qu'une nouvelle soit ouverte : la clôture est annulée avec l'échec."""
    # Pré-insère une ligne qui collisionne avec la clé que `changer_proprietaire` va générer.
    conn = sqlite3.connect(str(ref))
    conn.execute(
        "INSERT INTO ref_gestion_logements_hist "
        "(gestion_id, logement_id, proprietaire_id, date_debut, date_fin, statut_gestion, "
        "source, commentaire, import_id) VALUES (?,?,?,?,?,?,?,?,?)",
        ("GST_LOG_A1_PROP_B_2026-07-01", "LOG_A1", "PROP_B", "2020-01-01", "2020-12-31",
         "RETIRE", "FICTIF", "collision test", fx.IMPORT_TEST))
    conn.commit()
    conn.close()

    res = svc.changer_proprietaire("LOG_A1", "PROP_B", "2026-07-01", db_path=ref)
    assert res["ok"] is False

    gest = [g for g in _gestion(ref) if g["logement_id"] == "LOG_A1"]
    ouvertes = [g for g in gest if not g["date_fin"]]
    assert len(ouvertes) == 1                          # l'ancienne période reste ouverte
    assert ouvertes[0]["gestion_id"] == "GST_1"
    assert ouvertes[0]["proprietaire_id"] == "PROP_A"   # jamais changé


def test_changer_taux_rollback_si_ouverture_echoue(ref):
    conn = sqlite3.connect(str(ref))
    conn.execute(
        "INSERT INTO ref_taux_commission "
        "(taux_commission_id, proprietaire_id, logement_id, taux_commission, date_debut, "
        "date_fin, actif, justification, commentaire, import_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
        ("TX_LOG_A1_2026-07-01", "PROP_A", "LOG_A1", "0.20", "2020-01-01", "2020-12-31",
         "OUI", "FICTIF", "collision test", fx.IMPORT_TEST))
    conn.commit()
    conn.close()

    res = svc.changer_taux_commission("LOG_A1", 0.15, "2026-07-01", db_path=ref)
    assert res["ok"] is False

    taux = [t for t in _taux(ref) if t["logement_id"] == "LOG_A1"]
    ouverts = [t for t in taux if not t["date_fin"]]
    assert len(ouverts) == 1
    assert float(ouverts[0]["taux_commission"]) == 0.19    # jamais modifié


def test_archiver_transactionnel_ok(ref):
    """Le chemin nominal (sans collision) reste inchangé : clôture + désactivation en un commit."""
    res = svc.archiver("LOG_A1", "2026-06-30", db_path=ref)
    assert res["ok"], res
    logs = {r["logement_id"]: r for r in fx.lignes(ref, "ref_logements")}
    assert logs["LOG_A1"]["actif"] == "NON"
    gest = [g for g in _gestion(ref) if g["logement_id"] == "LOG_A1"]
    assert gest[0]["date_fin"] == "2026-06-30"


# ── 2. Historisation ref_couts_standards_menage ──────────────────────────────────────────────────

def test_couts_menage_desormais_lecture_seule_sur_ecran_generique(ref):
    assert "ref_couts_standards_menage" in adm.LECTURE_SEULE
    res = adm.creer_ligne("ref_couts_standards_menage", {
        "cout_standard_id": "CSM_X", "type_logement_id": "TYPE_001",
        "cout_standard_menage": "50"}, db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_ECRITURE


def test_changer_cout_menage_ouvre_une_premiere_periode(ref):
    res = cm.changer_cout("TYPE_001", 45.5, "2026-07-01", db_path=ref)
    assert res["ok"], res
    lignes = cm.historique("TYPE_001", db_path=ref)
    assert len(lignes) == 1
    assert float(lignes[0]["cout_standard_menage"]) == 45.5
    assert lignes[0]["date_debut_validite"] == "2026-07-01"
    assert lignes[0]["date_fin_validite"] == ""


def test_changer_cout_menage_cloture_lancien_et_ouvre_le_nouveau(ref):
    cm.changer_cout("TYPE_001", 45.5, "2026-01-01", db_path=ref)
    res = cm.changer_cout("TYPE_001", 60.0, "2026-07-01", db_path=ref)
    assert res["ok"], res
    lignes = cm.historique("TYPE_001", db_path=ref)
    assert len(lignes) == 2
    ancien = [l for l in lignes if l["date_fin_validite"]][0]
    assert float(ancien["cout_standard_menage"]) == 45.5 and ancien["date_fin_validite"] == "2026-06-30"
    nouveau = [l for l in lignes if not l["date_fin_validite"]][0]
    assert float(nouveau["cout_standard_menage"]) == 60.0


def test_changer_cout_menage_ne_modifie_jamais_une_ligne_close(ref):
    cm.changer_cout("TYPE_001", 45.5, "2026-01-01", db_path=ref)
    cm.changer_cout("TYPE_001", 60.0, "2026-07-01", db_path=ref)
    cm.changer_cout("TYPE_001", 70.0, "2026-08-01", db_path=ref)
    lignes = cm.historique("TYPE_001", db_path=ref)
    assert len(lignes) == 3
    premiere = [l for l in lignes if float(l["cout_standard_menage"]) == 45.5][0]
    assert premiere["date_fin_validite"] == "2026-06-30"      # jamais retouché


def test_changer_cout_menage_type_inconnu_refuse(ref):
    res = cm.changer_cout("TYPE_ZZZ", 10, "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == cm.E_TYPE_INCONNU


def test_changer_cout_menage_montant_invalide_refuse(ref):
    res = cm.changer_cout("TYPE_001", "abc", "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == cm.E_MONTANT_INVALIDE


def test_changer_cout_menage_date_invalide_refusee(ref):
    res = cm.changer_cout("TYPE_001", 10, "01/07/2026", db_path=ref)
    assert res["ok"] is False and res["code"] == cm.E_DATE_INVALIDE


# ── 3. Refus de chevauchement avec une période close ────────────────────────────────────────────

def test_inserer_refuse_chevauchement_avec_periode_close(ref):
    """Une période historisée dont le début tombe dans une période déjà close du même grain est
    refusée : elle rendrait deux lignes actives sur le même intervalle passé."""
    cm.changer_cout("TYPE_001", 45.5, "2026-01-01", db_path=ref)   # ouvre une 1re période
    cm.changer_cout("TYPE_001", 60.0, "2026-07-01", db_path=ref)   # la clôt au 2026-06-30

    # Tentative d'insertion directe d'une période dont le début (2026-03-01) tombe dans
    # l'intervalle déjà clos [2026-01-01, 2026-06-30].
    res = adm.inserer("ref_couts_standards_menage", {
        "cout_standard_id": "CSM_CHEVAUCHEMENT", "type_logement_id": "TYPE_001",
        "cout_standard_menage": "99", "date_debut_validite": "2026-03-01",
        "date_fin_validite": "", "actif": "OUI", "commentaire": ""},
        action="CREATION", db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_PERIODE_INCOHERENTE


# ── 4. Garde propriétaire ↔ logement actif ──────────────────────────────────────────────────────

def test_desactivation_proprietaire_refusee_si_logement_actif_rattache(ref):
    res = adm.basculer_activation("ref_proprietaires", "PROP_A", False, db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_PROPRIETAIRE_REFERENCE


def test_desactivation_proprietaire_acceptee_sans_rattachement_actif(ref):
    """PROP_C ne gère plus que LOG_INACTIF, dont le rattachement est déjà clos (fixture) : rien
    ne l'empêche d'être désactivé."""
    res = adm.basculer_activation("ref_proprietaires", "PROP_C", False, db_path=ref)
    assert res["ok"], res


def test_reactivation_proprietaire_toujours_libre(ref):
    """La garde ne s'applique qu'à la désactivation : réactiver reste toujours possible."""
    res = adm.basculer_activation("ref_proprietaires", "PROP_B", True, db_path=ref)
    assert res["ok"], res


# ── UI : écran générique existant, aucune deuxième page ─────────────────────────────────────────

def test_ecran_administration_index_reference_les_fournisseurs(client):
    resp = client.get("/administration/referentiels")
    assert resp.status_code == 200
    assert "/referentiel-fournisseurs" in resp.text


def test_ecran_detail_couts_menage_affiche_le_formulaire_dedie(client, tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_couts_standards_menage")
    assert resp.status_code == 200
    assert "Changer le coût standard" in resp.text
    assert "changer-cout" in resp.text


def test_route_changer_cout_menage_ecrit_et_redirige(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/administration/referentiels/ref_couts_standards_menage/changer-cout",
                       data={"type_logement_id": "TYPE_001", "cout_standard_menage": "55",
                             "date_debut": "2026-07-01", "justification": "Test"}, follow_redirects=False)
    assert resp.status_code == 303
    lignes = cm.historique("TYPE_001", db_path=tmp_db)
    assert len(lignes) == 1 and float(lignes[0]["cout_standard_menage"]) == 55

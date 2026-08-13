"""Créances propriétaires, dettes fournisseurs, échéancier et balance générale.

Ces vues répondent aux trois questions du quotidien — qui me doit quoi, à qui dois-je quoi, et
quand. Elles n'inventent aucun montant : elles agrègent ce que les services existants produisent.

Fixtures synthétiques uniquement.
"""
import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_balance_service as bal
from app.services import comptabilite_ecritures_service as compta
from app.services import creances_dettes_service as cd
from app.services import factures_proprietaires_service as fpr

EMETTEUR = {"nom": "SAS DEMO", "adresse": "1 rue Demo", "siret": "00000000000000"}
DESTINATAIRE = {"nom": "Client Demo", "adresse": "2 rue Demo"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)
    monkeypatch.setenv("FACTURATION_DELAI_PAIEMENT_JOURS", "30")
    apply_migrations(chemin)
    return chemin


@pytest.fixture()
def client(db):
    from app.main import app as application
    return TestClient(application)


def _facture_emise(db, *, logement="LOG_1", mois="2026-06", date_facture="2026-07-01",
                   montant=500.0):
    src = {"mois": mois, "proprietaire_id": "PROP_1", "logement_id": logement,
           "COMMISSION_CONCIERGERIE": montant, "montant_du_conciergerie": montant}
    f = fpr.creer(src, db_path=db)
    fid = f["facture_id_opaque"]
    fpr.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    return fpr.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                       date_facture=date_facture, db_path=db)


# ── Créances ────────────────────────────────────────────────────────────────────────────────────

def test_facture_non_emise_n_est_pas_une_creance(db):
    """Rien n'a encore été facturé : un brouillon ne doit pas apparaître en créance."""
    fpr.creer({"mois": "2026-06", "proprietaire_id": "PROP_1", "logement_id": "LOG_1",
               "COMMISSION_CONCIERGERIE": 500.0, "montant_du_conciergerie": 500.0}, db_path=db)
    assert cd.creances(db_path=db) == []


def test_facture_emise_devient_creance(db):
    emise = _facture_emise(db)
    lignes = cd.creances(db_path=db)
    assert len(lignes) == 1
    l = lignes[0]
    assert l["type"] == cd.CREANCE
    assert l["tiers_id"] == "PROP_1"
    assert l["numero"] == emise["numero_facture"]
    assert l["total"] == 500.0
    assert l["solde"] == 500.0
    assert l["statut_reglement"] == cd.ST_NON_REGLEE
    assert l["date_echeance"] == "2026-07-31"      # 1er juillet + 30 jours


def test_facture_annulee_absente_des_creances(db):
    f = fpr.creer({"mois": "2026-06", "proprietaire_id": "PROP_1", "logement_id": "LOG_1",
                   "COMMISSION_CONCIERGERIE": 500.0, "montant_du_conciergerie": 500.0},
                  db_path=db)
    fpr.annuler(f["facture_id_opaque"], motif="erreur", db_path=db)
    assert cd.creances(db_path=db) == []


def test_avoir_apparait_en_negatif(db):
    """Un avoir réduit ce que le propriétaire doit : il est inclus, avec un montant négatif."""
    emise = _facture_emise(db)
    avoir = fpr.creer_avoir(emise["facture_id_opaque"], motif="remise", db_path=db)
    aid = avoir["facture_id_opaque"]
    fpr.valider(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    fpr.emettre(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, date_facture="2026-07-15",
                db_path=db)

    lignes = cd.creances(db_path=db)
    assert len(lignes) == 2
    assert round(sum(l["solde"] for l in lignes), 2) == 0.0
    assert any(l["type_document"] == fpr.TYPE_AVOIR and l["solde"] == -500.0 for l in lignes)


def test_filtres_creances(db):
    _facture_emise(db, logement="LOG_1", mois="2026-06")
    _facture_emise(db, logement="LOG_2", mois="2026-07")
    assert len(cd.creances(db_path=db)) == 2
    assert len(cd.creances(logement_id="LOG_1", db_path=db)) == 1
    assert len(cd.creances(mois="2026-07", db_path=db)) == 1
    assert cd.creances(proprietaire_id="AUTRE", db_path=db) == []


def test_echeance_passee_marque_echue(db):
    _facture_emise(db, date_facture="2020-01-01")     # échéance largement dépassée
    l = cd.creances(db_path=db)[0]
    assert l["echue"] is True
    assert l["jours_retard"] > 0


def test_echeance_future_non_echue(db):
    _facture_emise(db, date_facture="2099-01-01")
    l = cd.creances(db_path=db)[0]
    assert l["echue"] is False
    assert l["jours_retard"] < 0


# ── Synthèse et agrégation ──────────────────────────────────────────────────────────────────────

def test_synthese(db):
    _facture_emise(db, logement="LOG_1", date_facture="2020-01-01")   # échue
    _facture_emise(db, logement="LOG_2", date_facture="2099-01-01")   # à venir
    s = cd.synthese(db_path=db)
    assert s["creances"]["nombre"] == 2
    assert s["creances"]["total"] == 1000.0
    assert s["creances"]["echu"] == 500.0
    assert s["creances"]["a_venir"] == 500.0
    assert s["position_nette"] == 1000.0 - s["dettes"]["total"]


def test_agregation_par_tiers(db):
    _facture_emise(db, logement="LOG_1")
    _facture_emise(db, logement="LOG_2")
    par = cd.par_tiers(db_path=db)["proprietaires"]
    assert len(par) == 1
    assert par[0]["tiers_id"] == "PROP_1"
    assert par[0]["nombre"] == 2
    assert par[0]["total"] == 1000.0


# ── Échéancier ──────────────────────────────────────────────────────────────────────────────────

def test_echeancier_ventile_par_tranche(db):
    _facture_emise(db, logement="LOG_1", date_facture="2020-01-01")   # échu
    _facture_emise(db, logement="LOG_2", date_facture="2099-01-01")   # au-delà de 30 jours
    ech = cd.echeancier(db_path=db)
    tranches = {t["cle"]: t for t in ech["tranches"]}
    assert tranches["ECHU"]["creances_nb"] == 1
    assert tranches["PLUS_TARD"]["creances_nb"] == 1
    assert sum(t["creances_nb"] for t in ech["tranches"]) == 2


def test_tranche_sans_echeance_isolee(db, monkeypatch):
    """Sans échéance calculable, la ligne est isolée plutôt que rangée arbitrairement."""
    monkeypatch.delenv("FACTURATION_DELAI_PAIEMENT_JOURS", raising=False)
    monkeypatch.setattr(cfg, "FACTURATION_DELAI_PAIEMENT_JOURS", "", raising=False)
    _facture_emise(db)
    tranches = {t["cle"]: t for t in cd.echeancier(db_path=db)["tranches"]}
    assert tranches["SANS_ECHEANCE"]["creances_nb"] == 1


# ── Balance ─────────────────────────────────────────────────────────────────────────────────────

def test_balance_vide(db):
    b = bal.balance(db_path=db)
    assert b["comptes"] == []
    assert b["equilibree"] is True


def test_balance_apres_vente(db):
    emise = _facture_emise(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    b = bal.balance(db_path=db)
    comptes = {c["compte"]: c for c in b["comptes"]}
    assert comptes[compta.COMPTE_PROPRIETAIRES]["debit"] == 500.0
    assert comptes[compta.COMPTE_PROPRIETAIRES]["solde_debiteur"] == 500.0
    assert comptes[compta.COMPTE_VENTE_GENERIQUE]["credit"] == 500.0
    assert comptes[compta.COMPTE_VENTE_GENERIQUE]["solde_crediteur"] == 500.0
    assert b["total_debit"] == b["total_credit"] == 500.0
    assert b["equilibree"] is True
    assert b["ecart"] == 0.0


def test_balance_classes_comptables(db):
    emise = _facture_emise(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    classes = {c["compte"]: c["classe"] for c in bal.balance(db_path=db)["comptes"]}
    assert classes[compta.COMPTE_PROPRIETAIRES] == "Tiers"
    assert classes[compta.COMPTE_VENTE_GENERIQUE] == "Produits"


def test_balance_filtre_periode(db):
    emise = _facture_emise(db, mois="2026-06")
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    assert bal.balance(periode_debut="2026-06", periode_fin="2026-06", db_path=db)["comptes"]
    assert bal.balance(periode_debut="2026-09", db_path=db)["comptes"] == []


def test_balance_exclut_les_contrepassees(db):
    emise = _facture_emise(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    conn = get_db(db)
    try:
        conn.execute("UPDATE ecritures SET statut=?", (compta.ST_CONTREPASSEE,))
        conn.commit()
    finally:
        conn.close()
    assert bal.balance(db_path=db)["comptes"] == []


def test_periodes_disponibles(db):
    emise = _facture_emise(db, mois="2026-06")
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    assert "2026-06" in bal.periodes_disponibles(db_path=db)


# ── Routes ──────────────────────────────────────────────────────────────────────────────────────

def test_ecrans_repondent(client):
    for url in ("/creances", "/dettes", "/echeancier", "/comptabilite/balance"):
        r = client.get(url)
        assert r.status_code == 200, url


def test_ecran_creances_affiche_la_facture(db, client):
    emise = _facture_emise(db)
    r = client.get("/creances")
    assert emise["numero_facture"] in r.text
    assert "PROP_1" in r.text


def test_ecran_echeancier_affiche_la_synthese(db, client):
    _facture_emise(db)
    r = client.get("/echeancier")
    assert "Position nette" in r.text
    assert "500.00" in r.text or "500,00" in r.text


def test_ecran_balance_affiche_les_comptes(db, client):
    emise = _facture_emise(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    r = client.get("/comptabilite/balance")
    assert compta.COMPTE_PROPRIETAIRES in r.text
    assert compta.COMPTE_VENTE_GENERIQUE in r.text


# ── Persistance ─────────────────────────────────────────────────────────────────────────────────

def test_creances_et_dettes_survivent_a_un_redemarrage(db):
    """Les vues lisent la base : rien ne dépend d'un état en mémoire."""
    _facture_emise(db)
    avant = cd.creances(db_path=db)

    # Nouvelle instance de client, comme après un redémarrage du serveur.
    from app.main import app as application
    with TestClient(application) as nouveau:
        assert nouveau.get("/creances").status_code == 200

    apres = cd.creances(db_path=db)
    assert apres == avant
    assert apres[0]["solde"] == 500.0

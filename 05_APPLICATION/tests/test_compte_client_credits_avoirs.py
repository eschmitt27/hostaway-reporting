"""Compte client : brouillon sans impact, crédit repris, imputation à l'émission, vrais avoirs.

Mission du 2026-10-03 — les 11 preuves demandées, sur base temporaire (aucune donnée réelle).
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import compte_proprietaire_service as cpt
from app.services import creances_dettes_service as creances
from app.services import creances_reglement_service as reglement
from app.services import credits_clients_service as credits
from app.services import factures_proprietaires_edition_service as edition
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie T", "adresse": "1 rue T", "siret": "00000000000000"}
DEST = {"nom": "Didier T", "adresse": "2 rue T"}
PID = "PROP_D"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups", raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-T','2026-09-01T00:00:00','x','x','IMPORTE',1,1)")
    conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                 "prenom_proprietaire, actif, import_id) VALUES (?, 'T', 'Didier', 'OUI', 'IMP-T')",
                 (PID,))
    conn.commit()
    conn.close()
    return chemin


def _brouillon(db, montant, logement="LOG_1", mois="2026-09"):
    return svc.creer({"mois": mois, "proprietaire_id": PID, "logement_id": logement,
                      "source_calcul": f"PREF-{logement}", "COMMISSION_CONCIERGERIE": montant,
                      "montant_du_conciergerie": montant}, db_path=db)["facture_id_opaque"]


def _emettre(db, fid, *, hors_compta=False):
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    f = svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture="2026-10-03",
                    db_path=db, hors_compta=hors_compta)
    if not hors_compta:
        compta.comptabiliser_facture_emise(f, acteur="t", db_path=db)
    return f


def _reprise(db, montant=300):
    r = credits.creer_reprise_solde(PID, montant, "2026-09-01", acteur="t", db_path=db)
    assert r["ok"], r
    return r


def _solde_compte(db, compte, auxiliaire=None):
    conn = get_db(db)
    try:
        sql = ("SELECT COALESCE(SUM(l.debit),0) - COALESCE(SUM(l.credit),0) FROM ecriture_lignes l "
               "JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
               "WHERE e.statut <> 'ANNULEE' AND l.compte = ?")
        args = [compte]
        if auxiliaire:
            sql += " AND l.auxiliaire = ?"
            args.append(auxiliaire)
        return round(conn.execute(sql, args).fetchone()[0], 2)
    finally:
        conn.close()


def _solde_facture(db, fid):
    return next(f["solde"] for f in cpt.position(PID, db_path=db)["factures"]
                if f["facture_id_opaque"] == fid)


# 1 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_01_brouillon_aucun_impact(db):
    _reprise(db)
    fid = _brouillon(db, 140)
    pos = cpt.position(PID, db_path=db)
    assert pos["creance_restante"] == 0 and pos["factures"] == []
    assert pos["credit_disponible"] == 300
    assert creances.creances(db_path=db) == []
    crd = credits.lister(proprietaire_id=PID, db_path=db)[0]["credit_id_opaque"]
    assert credits.imputer(crd, fid, 50, acteur="t", db_path=db)["code"] == credits.E_NON_EMISE
    with pytest.raises(svc.FactureProprietaireError):
        edition.ajouter_acompte(fid, montant=10, date_mouvement="2026-09-30", mode_reglement="",
                                commentaire="", acteur="t", db_path=db)
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque=?",
                            (fid,)).fetchone()[0] == 0
    finally:
        conn.close()


# 2 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_02_facture_emise_cree_la_creance(db):
    fid = _brouillon(db, 140)
    _emettre(db, fid)
    c = creances.creances(db_path=db)
    assert [(l["facture_id_opaque"], l["solde"]) for l in c] == [(fid, 140.0)]
    assert cpt.position(PID, db_path=db)["creance_restante"] == 140
    assert _solde_compte(db, "411000", PID) == 140


# 3 + 11 ────────────────────────────────────────────────────────────────────────────────────────
def test_03_11_credit_initial_didier_et_ecritures(db):
    r = _reprise(db)
    pos = cpt.position(PID, db_path=db)
    assert pos["credit_disponible"] == 300 and pos["etat_compte"] == cpt.POS_CREDITEUR
    c = credits.lister(proprietaire_id=PID, db_path=db)[0]
    assert c["origine"] == credits.ORIGINE_REPRISE_SOLDE and c["reste"] == 300
    assert c["reference"] == "Solde créditeur repris de l'ancienne structure"
    assert len(r["ecritures"]) == 2
    assert _solde_compte(db, "467100") == 0          # ancienne structure soldée
    assert _solde_compte(db, "654000") == 300        # perte constatée
    assert _solde_compte(db, "419100", PID) == -300  # Didier créditeur
    assert credits.creer_reprise_solde(PID, 300, "2026-09-01", acteur="t",
                                       db_path=db)["code"] == credits.E_REPRISE_EXISTANTE


# 4 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_04_facture_140_avec_credit_300(db):
    _reprise(db)
    fid = _brouillon(db, 140)
    f = _emettre(db, fid)
    assert f["montant_total"] == 140                 # la facture reste une facture de 140 €
    assert _solde_facture(db, fid) == 0
    pos = cpt.position(PID, db_path=db)
    assert pos["credit_disponible"] == 160 and pos["creance_restante"] == 0
    deco = __import__("json").loads(svc.lire(fid, db_path=db)["snapshot_json"])["decomposition"]
    assert deco["total_credit_client"] == 140 and deco["net"] == 0
    assert deco["credit_client_restant"] == 160
    assert _solde_compte(db, "411000", PID) == 0      # 140 facturés − 140 de crédit imputé
    assert _solde_compte(db, "419100", PID) == -160
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM factures_proprietaires WHERE type_document='AVOIR'"
                            ).fetchone()[0] == 0, "aucun avoir pour consommer un crédit"
    finally:
        conn.close()


# 5 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_05_facture_superieure_au_credit(db):
    _reprise(db, 100)
    fid = _brouillon(db, 250)
    _emettre(db, fid)
    assert _solde_facture(db, fid) == 150
    pos = cpt.position(PID, db_path=db)
    assert pos["credit_disponible"] == 0 and pos["creance_restante"] == 150
    assert pos["etat_compte"] == cpt.POS_DEBITEUR


# 6 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_06_avoir_brouillon_aucun_impact(db):
    fid = _brouillon(db, 140)
    _emettre(db, fid)
    avoir = svc.creer_avoir_libre(proprietaire_id=PID, facture_origine=fid, motif="geste",
                                  montant=50, db_path=db)
    assert avoir["statut"] == svc.ST_BROUILLON and avoir["montant_total"] == -50
    assert cpt.position(PID, db_path=db)["creance_restante"] == 140
    assert [l["solde"] for l in creances.creances(db_path=db)] == [140.0]


# 7 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_07_avoir_emis_diminue_la_creance(db):
    fid = _brouillon(db, 140)
    _emettre(db, fid)
    aid = svc.creer_avoir_libre(proprietaire_id=PID, facture_origine=fid, motif="geste",
                                montant=40, db_path=db)["facture_id_opaque"]
    a = _emettre(db, aid)
    assert a["numero_facture"] and a["type_document"] == svc.TYPE_AVOIR
    assert _solde_facture(db, fid) == 100
    assert cpt.position(PID, db_path=db)["creance_restante"] == 100
    assert _solde_compte(db, "411000", PID) == 100


# 8 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_08_avoir_superieur_a_la_creance_devient_credit(db):
    fid = _brouillon(db, 140)
    _emettre(db, fid)
    aid = svc.creer_avoir_libre(proprietaire_id=PID, facture_origine=fid, motif="trop facturé",
                                montant=150, db_path=db)["facture_id_opaque"]
    _emettre(db, aid)
    pos = cpt.position(PID, db_path=db)
    assert pos["creance_restante"] == 0 and pos["credit_disponible"] == 10
    assert pos["etat_compte"] == cpt.POS_CREDITEUR
    # Le surplus est consommé par la facture émise ensuite.
    f2 = _brouillon(db, 30, logement="LOG_2")
    _emettre(db, f2)
    pos = cpt.position(PID, db_path=db)
    assert pos["credit_disponible"] == 0 and pos["creance_restante"] == 20


# 9 ─────────────────────────────────────────────────────────────────────────────────────────────
def test_09_aucune_double_consommation(db):
    _reprise(db)
    f1, f2 = _brouillon(db, 140), _brouillon(db, 200, logement="LOG_2")
    _emettre(db, f1)
    _emettre(db, f2)
    assert _solde_facture(db, f1) == 0 and _solde_facture(db, f2) == 40
    conn = get_db(db)
    try:
        assert conn.execute("SELECT SUM(montant_impute) FROM imputations_airbnb").fetchone()[0] == 300
    finally:
        conn.close()
    pos = cpt.position(PID, db_path=db)
    assert pos["credit_disponible"] == 0 and pos["creance_restante"] == 40
    # Un paiement ultérieur ne solde que ce qui reste vraiment dû (FIFO net des crédits imputés).
    from app.services import proprietaires_tresorerie_service as tres
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, "
                     "proprietaire_id, sens, nature, montant, date_mouvement, statut, actif) "
                     "VALUES ('MVT-T', ?, 'PROPRIETAIRE_VERS_SOCIETE', 'ACOMPTE_PROPRIETAIRE', 100, "
                     "'2026-10-04', 'VALIDE', 1)", (PID,))
        conn.commit()
    finally:
        conn.close()
    _ = tres
    pos = cpt.position(PID, db_path=db)
    assert _solde_facture(db, f1) == 0 and _solde_facture(db, f2) == 0
    assert pos["credit_disponible"] == 60


# 10 ────────────────────────────────────────────────────────────────────────────────────────────
def test_10_creances_et_compte_proprietaire_concordent(db):
    _reprise(db, 100)
    f1 = _brouillon(db, 250)
    _emettre(db, f1)
    aid = svc.creer_avoir_libre(proprietaire_id=PID, facture_origine=f1, motif="geste",
                                montant=20, db_path=db)["facture_id_opaque"]
    _emettre(db, aid)
    _brouillon(db, 999, logement="LOG_9")              # brouillon : ignoré partout
    pos = cpt.position(PID, db_path=db)
    ligne = next(p for p in reglement.positions(inclure_soldes=True, db_path=db)
                 if p["proprietaire_id"] == PID)
    assert ligne["restant_du"] == pos["creance_restante"] == 130
    assert ligne["credit_disponible"] == pos["credit_disponible"] == 0
    assert creances.synthese(db_path=db)["creances"]["total"] == 130


def test_hors_compta_sans_imputation_de_credit(db):
    _reprise(db)
    fid = _brouillon(db, 140)
    _emettre(db, fid, hors_compta=True)
    assert cpt.position(PID, db_path=db)["credit_disponible"] == 300
    assert creances.creances(db_path=db) == []

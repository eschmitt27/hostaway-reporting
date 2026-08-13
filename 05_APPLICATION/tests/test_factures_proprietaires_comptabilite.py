"""La facture propriétaire ÉMISE est la source unique de l'écriture de vente.

Chaîne cible : Lot 10 calcule → Lot 12 prépare le relevé → la facture constate la vente →
le règlement éteint la créance → la Banque prouve le mouvement. Aucun de ces objets n'en crée
un autre.

Ces tests vérifient surtout ce qui NE doit pas arriver : qu'une même prestation ne soit jamais
comptabilisée deux fois, quel que soit le nombre de rejeux des pipelines.
"""
import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie Fixture", "adresse": "1 rue de Test", "siret": "00000000000000"}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    apply_migrations(chemin)
    return chemin


def source(**kw):
    base = {"mois": "2026-06", "proprietaire_id": "PROP_FIXT_1", "logement_id": "LOG_FIXT_1",
            "source_calcul": "PREF-FIXT-001",
            "COMMISSION_CONCIERGERIE": 300.0, "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0,
            "montant_du_conciergerie": 500.0, "TOTAL_PAYOUT": 2000.0}
    base.update(kw)
    return base


def _emettre(db, **kw):
    f = svc.creer(source(**kw), db_path=db)
    fid = f["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    return svc.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, serie="RECETTE-2026",
                       date_facture="2026-07-01", db_path=db)


def _ventes(db):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ecritures WHERE journal='VENTES' AND statut <> ?",
            (compta.ST_CONTREPASSEE,)).fetchall()]
    finally:
        conn.close()


# ── Moment de création ──────────────────────────────────────────────────────────────────────────

def test_brouillon_aucune_ecriture(db):
    f = svc.creer(source(), db_path=db)
    res = compta.generer_ecriture_vente_facture(f, db_path=db)
    assert res["ok"] is False
    assert _ventes(db) == []


def test_valide_aucune_ecriture(db):
    f = svc.creer(source(), db_path=db)
    v = svc.valider(f["facture_id_opaque"], emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                    db_path=db)
    res = compta.generer_ecriture_vente_facture(v, db_path=db)
    assert res["ok"] is False
    assert _ventes(db) == []


def test_emis_une_ecriture(db):
    emise = _emettre(db)
    res = compta.generer_ecriture_vente_facture(emise, db_path=db)
    assert res["ok"] is True

    ventes = _ventes(db)
    assert len(ventes) == 1
    assert ventes[0]["origine_type"] == compta.ORIGINE_FACTURE
    assert ventes[0]["origine_id_opaque"] == emise["facture_id_opaque"]
    assert ventes[0]["total_debit"] == ventes[0]["total_credit"] == 500.0


# ── Réconciliation ──────────────────────────────────────────────────────────────────────────────

def test_total_ecriture_egale_total_facture(db):
    """300 + 150 + 50 = 500 : le produit comptabilisé réconcilie la facture à 0,00 € près."""
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    ecr = compta.charger_par_origine(compta.ORIGINE_FACTURE, emise["facture_id_opaque"], db)

    lignes = compta.lignes(ecr["ecriture_id_opaque"], db)
    produits = round(sum(l["credit"] for l in lignes if l["compte"] == compta.COMPTE_VENTE_GENERIQUE), 2)
    creance = round(sum(l["debit"] for l in lignes if l["compte"] == compta.COMPTE_PROPRIETAIRES), 2)

    total_lignes_facture = round(sum(l["montant"] for l in emise["lignes"]), 2)
    assert total_lignes_facture == emise["montant_total"] == 500.0
    assert produits == 500.0
    assert creance == 500.0
    assert round(produits - total_lignes_facture, 2) == 0.0


# ── Idempotence et double comptage ──────────────────────────────────────────────────────────────

def test_idempotence_generateur(db):
    emise = _emettre(db)
    for _ in range(3):
        compta.generer_ecriture_vente_facture(emise, db_path=db)
    ventes = _ventes(db)
    assert len(ventes) == 1
    assert round(sum(v["total_credit"] for v in ventes), 2) == 500.0


def test_lot12_refuse_si_facture_a_deja_constate(db):
    """Rejouer le pipeline Lot 12 après émission ne doit pas re-comptabiliser la vente."""
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    res = compta.generer_ecriture_vente("PROP_FIXT_1", "2026-06", 500.0, db_path=db)
    assert res["ok"] is False
    assert compta.E_DOUBLE_SOURCE in res["code"]
    assert len(_ventes(db)) == 1


def test_facture_refuse_si_lot12_a_deja_constate(db):
    """Garde symétrique : un mois déjà comptabilisé par l'ancien mécanisme n'est pas refacturé."""
    compta.generer_ecriture_vente("PROP_FIXT_1", "2026-06", 500.0, db_path=db)
    assert len(_ventes(db)) == 1

    emise = _emettre(db)
    res = compta.generer_ecriture_vente_facture(emise, db_path=db)
    assert res["ok"] is False
    assert compta.E_DOUBLE_SOURCE in res["code"]
    assert len(_ventes(db)) == 1


def test_montant_total_reste_500_apres_rejeux_croises(db):
    """Test critique : quels que soient les rejeux, la vente vaut 500 € — jamais 1000 ni 1500."""
    emise = _emettre(db)
    for _ in range(2):
        compta.generer_ecriture_vente_facture(emise, db_path=db)
        compta.generer_ecriture_vente("PROP_FIXT_1", "2026-06", 500.0, db_path=db)

    ventes = _ventes(db)
    assert len(ventes) == 1
    assert round(sum(v["total_credit"] for v in ventes), 2) == 500.0


# ── Le montant vient du document, pas d'un recalcul ─────────────────────────────────────────────

def test_ecriture_suit_le_montant_fige_de_la_facture(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    ecr = compta.charger_par_origine(compta.ORIGINE_FACTURE, emise["facture_id_opaque"], db)
    assert ecr["total_credit"] == emise["montant_total"]
    assert emise["numero_facture"] in ecr["piece"]


# ── Règlement ───────────────────────────────────────────────────────────────────────────────────

def test_reglement_partiel_ne_touche_pas_la_vente(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    avant = _ventes(db)

    s = svc.solde(emise["facture_id_opaque"], paiements_imputes=200.0, db_path=db)
    assert (s["montant_total"], s["paiements_imputes"], s["solde"]) == (500.0, 200.0, 300.0)
    assert s["statut_reglement"] == "PARTIELLEMENT_REGLEE"
    assert _ventes(db) == avant          # le règlement ne recrée aucun produit


def test_reglement_total_ne_cree_aucune_vente(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    s = svc.solde(emise["facture_id_opaque"], paiements_imputes=500.0, db_path=db)
    assert s["solde"] == 0.0 and s["statut_reglement"] == "REGLEE"
    assert len(_ventes(db)) == 1
    assert round(sum(v["total_credit"] for v in _ventes(db)), 2) == 500.0


def test_extinction_par_reglement_et_compensation(db):
    """Une compensation éteint la créance au même titre qu'un règlement : facture 500 =
    200 réglés + 300 compensés, solde 0, et toujours une seule vente."""
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    regle, compense = 200.0, 300.0
    s = svc.solde(emise["facture_id_opaque"], paiements_imputes=regle + compense, db_path=db)
    assert s["solde"] == 0.0 and s["statut_reglement"] == "REGLEE"
    assert len(_ventes(db)) == 1


# ── Avoir ───────────────────────────────────────────────────────────────────────────────────────

def _emettre_avoir(db, facture_emise, motif="correction"):
    a = svc.creer_avoir(facture_emise["facture_id_opaque"], motif=motif, db_path=db)
    aid = a["facture_id_opaque"]
    svc.valider(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    return svc.emettre(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, serie="RECETTE-2026",
                       date_facture="2026-07-15", db_path=db)


def test_avoir_total_net_comptable_nul(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)
    avoir = _emettre_avoir(db, emise)
    compta.generer_ecriture_vente_facture(avoir, db_path=db)

    ventes = _ventes(db)
    assert len(ventes) == 2
    produits = 0.0
    for v in ventes:
        for l in compta.lignes(v["ecriture_id_opaque"], db):
            if l["compte"] == compta.COMPTE_VENTE_GENERIQUE:
                produits += (l["credit"] or 0) - (l["debit"] or 0)
    assert round(produits, 2) == 0.0        # 500 constatés, 500 annulés

    originale = svc.lire(emise["facture_id_opaque"], db_path=db)
    assert originale["statut"] == svc.ST_EMIS and originale["montant_total"] == 500.0


def test_avoir_partiel_net_400(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    avoir = svc.creer_avoir(emise["facture_id_opaque"], motif="remise", db_path=db)
    # Avoir partiel : seule une des trois lignes est annulée (−100 sur le ménage).
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM factures_proprietaires_lignes WHERE facture_id_opaque=?",
                     (avoir["facture_id_opaque"],))
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes (ligne_id_opaque, facture_id_opaque, "
            "numero_ligne, type_ligne, libelle, montant) VALUES (?,?,?,?,?,?)",
            ("FPRL-PARTIEL", avoir["facture_id_opaque"], 1, "MENAGE_FACTURE",
             "Avoir partiel — ménage", -100.0))
        conn.execute("UPDATE factures_proprietaires SET montant_total=? WHERE facture_id_opaque=?",
                     (-100.0, avoir["facture_id_opaque"]))
        conn.commit()
    finally:
        conn.close()

    aid = avoir["facture_id_opaque"]
    svc.valider(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    a_emis = svc.emettre(aid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, serie="RECETTE-2026",
                         date_facture="2026-07-15", db_path=db)
    compta.generer_ecriture_vente_facture(a_emis, db_path=db)

    produits = 0.0
    for v in _ventes(db):
        for l in compta.lignes(v["ecriture_id_opaque"], db):
            if l["compte"] == compta.COMPTE_VENTE_GENERIQUE:
                produits += (l["credit"] or 0) - (l["debit"] or 0)
    assert round(produits, 2) == 400.0
    assert svc.lire(emise["facture_id_opaque"], db_path=db)["montant_total"] == 500.0


# ── Traçabilité ─────────────────────────────────────────────────────────────────────────────────

def test_facture_tracee_vers_ecriture_et_retour(db):
    emise = _emettre(db)
    compta.generer_ecriture_vente_facture(emise, db_path=db)

    ecr = compta.charger_par_origine(compta.ORIGINE_FACTURE, emise["facture_id_opaque"], db)
    assert ecr is not None
    assert ecr["origine_type"] == "FACTURE_PROPRIETAIRE"
    assert ecr["origine_id_opaque"] == emise["facture_id_opaque"]
    assert ecr["journal"] == "VENTES"

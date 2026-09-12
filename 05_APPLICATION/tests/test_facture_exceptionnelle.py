"""§79 — facturer une prestation ponctuelle, hors du cycle mensuel.

LE MANQUE. Toute facture propriétaire naissait d'une source Lot12 : un calcul mensuel, par
propriétaire et par logement. Une intervention d'urgence, un service rendu une fois, n'ont aucune
source de ce genre — et n'avaient donc AUCUN chemin. Il fallait attendre le cycle suivant et l'y
glisser, ou renoncer à facturer.

CE QUI NE CHANGE PAS. Le document reste une facture entière : `type_document = FACTURE`, même
numérotation, même PDF, même conformité, même créance, même écriture VENTES. Fiscalement, rien ne
distingue une facture exceptionnelle. Ce qui la distingue tient dans `source_calcul`, qui dit
d'où elle vient — et c'est ce qui la libère de l'index d'unicité du cycle mensuel.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_service as svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    conn = get_db(p)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES (?,?,?,?,?)",
            ("PROP_0001", "UZON", "Didier", "OUI", "TEST"))
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("LOG_0001", "Studio - 46", "Studio - 46", "", "TOULOUSE", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return p


def _creer(db, montant="150,00", libelle="Intervention d'urgence — fuite salle de bain", **kw):
    return svc.creer_exceptionnelle(
        proprietaire_id=kw.pop("proprietaire_id", "PROP_0001"),
        logement_id=kw.pop("logement_id", "LOG_0001"),
        mois=kw.pop("mois", "2026-09"),
        lignes=kw.pop("lignes", [{"libelle": libelle, "montant": montant}]),
        acteur="test", db_path=db, **kw)


# ── Le cas nominal : 150 € facturés hors cycle ──────────────────────────────────────────────────

def test_une_prestation_ponctuelle_devient_une_facture(db):
    res = _creer(db)
    assert res["montant_total"] == 150.0
    assert res["statut"] == svc.ST_BROUILLON
    assert res["type_document"] == svc.TYPE_FACTURE, "fiscalement, c'est une facture"
    assert res["source_calcul"] == svc.SOURCE_EXCEPTIONNELLE, "et elle dit d'où elle vient"


def test_les_lignes_sont_celles_qu_on_a_donnees(db):
    res = _creer(db, lignes=[{"libelle": "Déplacement", "montant": "40"},
                             {"libelle": "Main d'œuvre", "montant": "110"}])
    assert res["montant_total"] == 150.0 and res["nb_lignes"] == 2
    lignes = svc.lire(res["facture_id_opaque"], db_path=db)["lignes"]
    assert [l["libelle"] for l in lignes] == ["Déplacement", "Main d'œuvre"]
    assert all(l["type_ligne"] == "EXTRA" for l in lignes)


def test_deux_interventions_le_meme_mois_font_deux_factures(db):
    """C'est le cas NORMAL, et l'index d'unicité l'interdisait par effet de bord."""
    a = _creer(db, montant="150")
    b = _creer(db, montant="90", libelle="Remplacement de serrure")
    assert a["facture_id_opaque"] != b["facture_id_opaque"]


# ── Ce qui est refusé ───────────────────────────────────────────────────────────────────────────

def test_aucune_ligne(db):
    with pytest.raises(svc.FactureProprietaireError, match="aucune ligne facturable"):
        _creer(db, lignes=[])


def test_un_montant_negatif_est_un_avoir_pas_une_facture(db):
    with pytest.raises(svc.FactureProprietaireError, match="avoir"):
        _creer(db, lignes=[{"libelle": "Remise", "montant": "-50"}])


def test_un_montant_nul_ne_se_facture_pas(db):
    with pytest.raises(svc.FactureProprietaireError, match="montant"):
        _creer(db, lignes=[{"libelle": "Geste commercial", "montant": "0"}])


def test_une_ligne_sans_libelle(db):
    with pytest.raises(svc.FactureProprietaireError, match="sans libellé"):
        _creer(db, lignes=[{"libelle": "   ", "montant": "10"}])


def test_un_montant_illisible_est_refuse_pas_interprete(db):
    with pytest.raises(svc.FactureProprietaireError, match="illisible"):
        _creer(db, lignes=[{"libelle": "X", "montant": "cent cinquante"}])


def test_un_type_de_ligne_non_facturable(db):
    with pytest.raises(svc.FactureProprietaireError, match="non facturable"):
        _creer(db, lignes=[{"libelle": "X", "montant": "10", "type_ligne": "PAYOUT"}])


@pytest.mark.parametrize("manquant", ["proprietaire_id", "logement_id", "mois"])
def test_les_trois_champs_de_rattachement_sont_obligatoires(db, manquant):
    with pytest.raises(svc.FactureProprietaireError, match="obligatoires"):
        _creer(db, **{manquant: ""})


# ── L'anti-doublon du cycle mensuel n'a PAS été affaibli ────────────────────────────────────────

def test_deux_factures_mensuelles_restent_interdites(db):
    """L'index d'unicité garde toujours le cycle : c'est son seul et vrai rôle."""
    import sqlite3

    conn = get_db(db)
    try:
        def _inserer(ident):
            conn.execute(
                "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, "
                "proprietaire_id, logement_id, mois, montant_total, statut, source_calcul) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (ident, svc.TYPE_FACTURE, "PROP_0001", "LOG_0001", "2026-09", 1.0,
                 svc.ST_BROUILLON, "LOT12"))

        _inserer("FPR-CYCLE1")
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _inserer("FPR-CYCLE2")
    finally:
        conn.close()


def test_une_exceptionnelle_coexiste_avec_la_facture_mensuelle(db):
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, source_calcul) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("FPR-MENSUELLE", svc.TYPE_FACTURE, "PROP_0001", "LOG_0001", "2026-09", 465.88,
             svc.ST_BROUILLON, "LOT12"))
        conn.commit()
    finally:
        conn.close()
    res = _creer(db)
    assert res["montant_total"] == 150.0, "la ponctuelle n'entre pas en collision avec la mensuelle"


# ── Elle suit ensuite le parcours ORDINAIRE d'une facture ───────────────────────────────────────

def test_elle_est_editable_comme_un_brouillon_ordinaire(db):
    fid = _creer(db)["facture_id_opaque"]
    svc.ajouter_ligne(fid, type_ligne="EXTRA", libelle="Fournitures", montant=25.0,
                      acteur="test", db_path=db)
    assert svc.lire(fid, db_path=db)["montant_total"] == 175.0


def test_elle_est_journalisee_comme_exceptionnelle(db):
    fid = _creer(db)["facture_id_opaque"]
    conn = get_db(db)
    try:
        evt = conn.execute(
            "SELECT type_evenement, commentaire FROM factures_proprietaires_evenements "
            "WHERE facture_id_opaque=?", (fid,)).fetchone()
    finally:
        conn.close()
    assert evt["type_evenement"] == "CREATION_EXCEPTIONNELLE"
    assert "150.00" in evt["commentaire"]

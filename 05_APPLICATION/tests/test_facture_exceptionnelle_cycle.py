"""§79/§80 — le cycle complet d'une facture exceptionnelle, et ce qu'il ne consomme PAS.

UN NUMÉRO DE FACTURE EST FISCAL. Une fois attribué, il est consommé pour toujours. Le faire naître
à la CRÉATION obligerait à émettre un avoir pour la moindre faute de frappe, et laisserait des
trous dans une séquence qui doit être continue.

La création ne produit donc qu'un BROUILLON, modifiable autant qu'on veut. C'est l'ÉMISSION —
geste explicite et séparé — qui consomme le numéro, fige le snapshot, produit le PDF, ouvre la
créance et constate la vente.

UNE SEULE SÉRIE POUR LE MOIS. Une facture exceptionnelle n'a pas son compteur à elle : elle prend
le numéro suivant de la série du mois de prestation, exactement comme la facture mensuelle. La
série ne branche que sur le TYPE DE DOCUMENT (facture ou avoir) — jamais sur l'origine, ni sur la
nature des prestations facturées.
"""
from __future__ import annotations

import sqlite3

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import creances_dettes_service as cd
from app.services import facturation_config_service as fconf
from app.services import factures_proprietaires_service as svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                 "COMPTABILITE_REAL_WRITE_ENABLED",
                 "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True)
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


EMETTEUR = {"nom": "CHOUETTE PATRIMOINE", "forme_juridique": "SAS", "capital": "200,00 €",
            "adresse": "48E Route de Larnavey, 33650 Saint-Selve", "siren": "109624767",
            "rcs": "R.C.S. Bordeaux"}
DESTINATAIRE = {"nom": "Didier UZON", "adresse": "46 allees Charles de Fitte"}


def _creer(db, montant="150,00", libelle="Intervention d'urgence", mois="2026-09"):
    return svc.creer_exceptionnelle(
        proprietaire_id="PROP_0001", logement_id="LOG_0001", mois=mois,
        lignes=[{"libelle": libelle, "montant": montant}], acteur="test", db_path=db)


def _sequence(db, serie):
    conn = get_db(db)
    try:
        row = conn.execute(
            "SELECT dernier_numero FROM factures_proprietaires_sequence WHERE serie=?",
            (serie,)).fetchone()
    finally:
        conn.close()
    return row["dernier_numero"] if row else 0


def _emettre(db, fid, date_facture="2026-09-12"):
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="test", db_path=db)
    return svc.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                       date_facture=date_facture, acteur="test", db_path=db)


def _nb_ecritures(db, fid):
    conn = get_db(db)
    try:
        return conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                            (fid,)).fetchone()["c"]
    finally:
        conn.close()


# ── 1 — La création ne consomme RIEN ────────────────────────────────────────────────────────────

def test_la_creation_ne_produit_qu_un_brouillon(db):
    fid = _creer(db)["facture_id_opaque"]
    f = svc.lire(fid, db_path=db)
    assert f["statut"] == svc.ST_BROUILLON
    assert f["numero_facture"] is None, "aucun numéro fiscal consommé à la création"
    assert f["document_nom"] is None, "aucun PDF définitif"
    assert f["snapshot_json"] is None, "aucun snapshot figé"
    assert _sequence(db, "2026-09") == 0, "la séquence du mois n'a pas bougé"


def test_la_creation_n_ouvre_ni_creance_ni_ecriture(db):
    fid = _creer(db)["facture_id_opaque"]
    assert not [l for l in cd.creances(db_path=db) if l["facture_id_opaque"] == fid], \
        "une facture non émise n'est pas une créance"
    assert _nb_ecritures(db, fid) == 0, "la vente se constate à l'émission, pas avant"


def test_valider_ne_consomme_toujours_pas_le_numero(db):
    fid = _creer(db)["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="test", db_path=db)
    assert svc.lire(fid, db_path=db)["numero_facture"] is None
    assert _sequence(db, "2026-09") == 0


# ── 2 — Le cycle demandé : 150 → 155 → émission ─────────────────────────────────────────────────

def test_cycle_creation_modification_apercu_emission(db):
    """Une erreur de saisie se corrige sur le brouillon, sans brûler de numéro."""
    fid = _creer(db, montant="150,00")["facture_id_opaque"]
    assert svc.lire(fid, db_path=db)["montant_total"] == 150.0

    ligne = svc.lire(fid, db_path=db)["lignes"][0]
    svc.modifier_ligne(fid, ligne["ligne_id_opaque"], montant=155.0, acteur="test", db_path=db)

    apercu = svc.lire(fid, db_path=db)
    assert apercu["montant_total"] == 155.0, "l'aperçu reflète la correction"
    assert apercu["numero_facture"] is None
    assert _sequence(db, "2026-09") == 0, "modifier ne consomme aucun numéro"

    emise = _emettre(db, fid)
    f = svc.lire(fid, db_path=db)
    assert f["statut"] == svc.ST_EMIS
    assert f["numero_facture"] == "2026-09-001"
    assert f["montant_total"] == 155.0, "c'est le montant CORRIGÉ qui est facturé"
    assert f["snapshot_json"], "le snapshot est figé à l'émission"
    assert _sequence(db, "2026-09") == 1

    creance = next(l for l in cd.creances(db_path=db) if l["facture_id_opaque"] == fid)
    assert creance["total"] == 155.0 and creance["numero"] == "2026-09-001"
    assert emise["numero_facture"] == "2026-09-001"


def test_un_brouillon_abandonne_ne_troue_pas_la_sequence(db):
    """C'est la raison d'être du numéro tardif : un abandon ne laisse aucun trou."""
    abandonne = _creer(db, montant="150")["facture_id_opaque"]
    svc.annuler(abandonne, motif="erreur de saisie", acteur="test", db_path=db)

    suivante = _creer(db, montant="90", libelle="Remplacement de serrure")["facture_id_opaque"]
    assert _emettre(db, suivante)["numero_facture"] == "2026-09-001", \
        "le premier numéro du mois reste disponible"


# ── 3 — L'écriture VENTES : équilibrée, sur le bon auxiliaire, une seule fois ───────────────────

def test_l_emission_constate_une_vente_equilibree(db):
    fid = _creer(db, montant="155")["facture_id_opaque"]
    emise = _emettre(db, fid)
    assert compta.generer_ecriture_vente_facture(emise, acteur="test", db_path=db)["ok"] is True

    conn = get_db(db)
    try:
        lignes = conn.execute(
            "SELECT compte, auxiliaire, debit, credit FROM ecriture_lignes el "
            "JOIN ecritures e ON e.ecriture_id_opaque = el.ecriture_id_opaque "
            "WHERE e.origine_id_opaque=? ORDER BY el.ligne_num", (fid,)).fetchall()
        journal = conn.execute("SELECT journal FROM ecritures WHERE origine_id_opaque=?",
                               (fid,)).fetchone()["journal"]
    finally:
        conn.close()

    assert journal == "VENTES"
    assert round(sum(l["debit"] for l in lignes), 2) == 155.0
    assert round(sum(l["credit"] for l in lignes), 2) == 155.0
    client = next(l for l in lignes if l["compte"] == compta.COMPTE_PROPRIETAIRES)
    assert client["debit"] == 155.0, "le client (411) est débité de ce qu'il doit"
    assert client["auxiliaire"] == "PROP_0001", "sur son compte auxiliaire"
    assert any(l["compte"] == compta.COMPTE_VENTE_GENERIQUE and l["credit"] == 155.0
               for l in lignes), "le produit est crédité"


def test_rejouer_la_generation_ne_double_jamais_la_vente(db):
    """Idempotence : rafraîchir, rouvrir ou recalculer laisse UNE écriture."""
    fid = _creer(db)["facture_id_opaque"]
    emise = _emettre(db, fid)

    premiere = compta.generer_ecriture_vente_facture(emise, acteur="test", db_path=db)
    seconde = compta.generer_ecriture_vente_facture(emise, acteur="test", db_path=db)
    troisieme = compta.generer_ecriture_vente_facture(
        svc.lire(fid, db_path=db), acteur="test", db_path=db)

    assert seconde.get("deja_generee") is True
    assert troisieme.get("deja_generee") is True
    assert seconde["ecriture_id_opaque"] == premiere["ecriture_id_opaque"]
    assert _nb_ecritures(db, fid) == 1


# ── 4 — UNE SEULE SÉRIE POUR TOUTES LES FACTURES DU MOIS ────────────────────────────────────────

def test_exceptionnelles_et_mensuelle_partagent_la_serie_du_mois(db):
    """Trois émissions dans le désordre → 001, 002, 003. Un compteur par origine en donnerait deux 001."""
    numeros = []

    a = _creer(db, montant="150", libelle="Intervention d'urgence")["facture_id_opaque"]
    numeros.append(_emettre(db, a)["numero_facture"])

    # La facture MENSUELLE du même mois, issue du cycle (source LOT12).
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, source_calcul) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("FPR-MENSUELLE", svc.TYPE_FACTURE, "PROP_0001", "LOG_0001", "2026-09", 465.88,
             svc.ST_BROUILLON, "LOT12"))
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes (ligne_id_opaque, facture_id_opaque, "
            "numero_ligne, type_ligne, libelle, montant) VALUES (?,?,?,?,?,?)",
            ("FPRL-MENSUELLE", "FPR-MENSUELLE", 1, "COMMISSION_CONCIERGERIE",
             "Commission de conciergerie", 465.88))
        conn.commit()
    finally:
        conn.close()
    numeros.append(_emettre(db, "FPR-MENSUELLE")["numero_facture"])

    b = _creer(db, montant="90", libelle="Remplacement de serrure")["facture_id_opaque"]
    numeros.append(_emettre(db, b)["numero_facture"])

    assert numeros == ["2026-09-001", "2026-09-002", "2026-09-003"]
    assert len(set(numeros)) == 3
    assert _sequence(db, "2026-09") == 3, "UN seul compteur, avancé trois fois"


def test_la_serie_ne_branche_que_sur_le_type_de_document(db):
    """`EXTRA` est une nature de prestation, pas une série documentaire."""
    assert fconf.serie_mois(svc.TYPE_FACTURE, "2026-09") == "2026-09"
    assert fconf.serie_mois(svc.TYPE_AVOIR, "2026-09") != "2026-09", "l'avoir a sa propre série"
    assert "EXTRA" not in fconf.serie_mois(svc.TYPE_FACTURE, "2026-09")
    # Une exceptionnelle est `type_document = FACTURE` : elle tombe dans la série du mois.
    res = _creer(db)
    assert res["type_document"] == svc.TYPE_FACTURE


def test_le_numero_est_unique_au_niveau_du_schema(db):
    """La contrainte garantit l'unicité indépendamment du code qui attribue."""
    fid = _creer(db)["facture_id_opaque"]
    _emettre(db, fid)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            conn.execute(
                "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, "
                "proprietaire_id, logement_id, mois, montant_total, statut, numero_facture) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("FPR-COLLISION", svc.TYPE_FACTURE, "PROP_0001", "LOG_0001", "2026-10", 1.0,
                 svc.ST_EMIS, "2026-09-001"))
    finally:
        conn.close()


def test_l_allocation_du_numero_est_serialisee(db):
    """`BEGIN IMMEDIATE` : deux émissions concurrentes ne peuvent pas obtenir le même numéro.

    On vérifie la propriété par le CODE plutôt que par une course de threads, qui serait
    non déterministe : l'allocation prend un verrou d'écriture avant de lire la séquence.
    """
    import inspect
    source = inspect.getsource(svc._attribuer_numero)
    assert "BEGIN IMMEDIATE" in source
    assert "dernier_numero = dernier_numero + 1" in source, "incrément atomique en SQL"

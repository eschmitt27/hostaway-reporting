"""Verrous d'écriture bancaire, et ce que l'écran dit d'une écriture encore en attente.

DEUX DÉCISIONS D'EXPLOITATION DISTINCTES. Ouvrir la comptabilité autorise un comptable à valider
des écritures ; cela ne doit pas autoriser un écran bancaire à en créer. Sans verrou propre à la
Banque, `COMPTABILITE_REAL_WRITE_*` aurait suffi à faire écrire depuis Qonto — ce n'a jamais été
le contrat de ce drapeau.

ET « RAPPROCHÉ » N'EST PAS « COMPTABILISÉ ». Une écriture naît `PROPOSEE` et attend le workflow
comptable canonique. L'écran le dit, au lieu de laisser croire que l'affaire est close.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import qonto_ecran_service as ecran
from app.services import qonto_validation_service as validation
from tests.test_qonto_raw_import import ClientDouble, mouvement
from tests.test_qonto_validation_humaine import CREDIT_200, retrait

ASSOCIE = "PERS_TEST"

TOUS_LES_VERROUS = ("MODE_REEL_ECRITURES",
                    "BANQUE_REAL_WRITE_ENABLED", "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED",
                    "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED")


def poser_verrous(monkeypatch, *, banque: bool, comptabilite: bool, contexte: bool = True):
    """Positionne les verrous un par un — c'est leur COMBINAISON qui est testée."""
    monkeypatch.setattr(cfg, "RECETTE_MODE", False, raising=False)
    monkeypatch.setattr(cfg, "MODE_REEL_ECRITURES", contexte, raising=False)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", banque, raising=False)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", banque, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", comptabilite, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", comptabilite,
                        raising=False)


@pytest.fixture()
def base(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db, raising=False)
    conn = get_db(db)
    try:
        conn.execute("INSERT OR REPLACE INTO ref_associes (personne_id, nom_personne, "
                     "type_personne, actif, import_id) VALUES (?,?,?,?,?)",
                     (ASSOCIE, "Associé de test", "ASSOCIE", "OUI", "IMP-TEST"))
        conn.commit()
    finally:
        conn.close()
    return db


def _importer(db, mouvements):
    return ecran.actualiser(client=ClientDouble(pages=[mouvements]), db_path=db)


def _uuid(db, statut="completed"):
    conn = get_db(db)
    try:
        return conn.execute("SELECT qonto_transaction_uuid FROM qonto_transactions_raw "
                            "WHERE statut=?", (statut,)).fetchone()[0]
    finally:
        conn.close()


def _compter(db, table):
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


# ══ Banque fermée, comptabilité ouverte ═══════════════════════════════════════════════════════
def test_comptabilite_ouverte_seule_ne_permet_pas_d_ecrire_depuis_la_banque(base, monkeypatch):
    poser_verrous(monkeypatch, banque=False, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])

    refus = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                               db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_VERROU_BANQUE, \
        "le refus doit nommer la banque, pas la comptabilité : c'est elle qui est fermée"
    assert _compter(base, "banque_rapprochements") == 0
    assert _compter(base, "ecritures") == 0


def test_banque_fermee_refuse_aussi_l_annulation_et_l_automatisme_caisse(base, monkeypatch):
    poser_verrous(monkeypatch, banque=False, comptabilite=True)
    _importer(base, [retrait(statut="completed")])

    assert validation.annuler("BRP-QUELCONQUE", motif="test",
                              db_path=base)["code"] == validation.E_VERROU_BANQUE
    bilan = validation.confirmer_transferts_caisse(db_path=base)
    assert bilan["code"] == validation.E_VERROU_BANQUE
    assert bilan["comptabilises"] == 0
    assert _compter(base, "ecritures") == 0


# ══ Banque ouverte, comptabilité fermée ═══════════════════════════════════════════════════════
def test_banque_ouverte_sans_comptabilite_refuse_une_operation_qui_ecrit_une_ecriture(
        base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=False)
    _importer(base, [mouvement(1, **CREDIT_200)])

    refus = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                               db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_VERROU_COMPTA
    assert _compter(base, "banque_rapprochements") == 0, \
        "rien ne doit être écrit à moitié : pas de rapprochement sans son écriture"
    assert _compter(base, "ecritures") == 0


def test_le_verrou_comptable_ne_vise_que_les_natures_qui_ecrivent(base):
    """Un règlement délègue à un service qui produit son effet séparément : le bloquer ici serait
    interdire une opération qui n'écrit encore aucune écriture."""
    assert validation.NATURES_AVEC_ECRITURE == (validation.APPORT_ASSOCIE,
                                                validation.TRANSFERT_CAISSE)
    assert validation.REGLEMENT_CHARGE not in validation.NATURES_AVEC_ECRITURE
    assert validation.REVERSEMENT_PROPRIETAIRE not in validation.NATURES_AVEC_ECRITURE


# ══ Contexte d'écriture ═══════════════════════════════════════════════════════════════════════
def test_les_drapeaux_dedies_ne_suffisent_pas_sans_contexte_d_ecriture(base, monkeypatch):
    """`MODE_REEL_ECRITURES` fait partie de la combinaison, il n'est pas décoratif."""
    poser_verrous(monkeypatch, banque=True, comptabilite=True, contexte=False)
    _importer(base, [mouvement(1, **CREDIT_200)])

    refus = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                               db_path=base)
    assert refus["code"] == validation.E_VERROU_BANQUE
    assert validation.verrous() == {"banque": False, "comptabilite": False, "contexte": False}


# ══ Tout ouvert ═══════════════════════════════════════════════════════════════════════════════
def test_banque_et_comptabilite_ouvertes_autorisent_la_validation(base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])

    resultat = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                  acteur="test", db_path=base)
    assert resultat["ok"] is True
    assert resultat["ecriture"]["ok"] is True
    assert _compter(base, "banque_rapprochements") == 1
    assert _compter(base, "ecritures") == 1
    assert validation.verrous() == {"banque": True, "comptabilite": True, "contexte": True}


def test_une_transaction_deja_rapprochee_ne_produit_aucun_doublon(base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, db_path=base)

    second = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                db_path=base)
    assert second["ok"] is False
    assert second["code"] == validation.E_DEJA_AFFECTE
    assert _compter(base, "banque_rapprochements") == 1
    assert _compter(base, "ecritures") == 1
    assert _compter(base, "ecriture_lignes") == 2


# ══ Statut comptable à l'écran ════════════════════════════════════════════════════════════════
def _ligne_rapprochee(db):
    return [t for t in ecran.tableau_de_bord(db_path=db)["transactions"] if t["rapprochements"]][0]


def test_une_ecriture_proposee_s_affiche_comme_restant_a_valider(base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])
    resultat = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE,
                                  objet_id=ASSOCIE, db_path=base)

    conn = get_db(base)
    try:
        statut = conn.execute("SELECT statut FROM ecritures").fetchone()[0]
    finally:
        conn.close()
    assert statut == compta.ST_PROPOSEE, \
        "la validation bancaire ne valide PAS l'écriture : ce n'est pas son rôle"

    ligne = _ligne_rapprochee(base)
    assert ligne["traitement_libelle"] == "Rapproché — écriture à valider"
    assert ligne["rapprochements"][0]["etat_comptable"] == ecran.ETAT_ECRITURE_A_VALIDER
    assert resultat["ecriture"]["deja_generee"] is False


def test_une_ecriture_validee_s_affiche_comme_comptabilisee(base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])
    resultat = validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE,
                                  objet_id=ASSOCIE, db_path=base)

    # Validation par le WORKFLOW COMPTABLE CANONIQUE, jamais par le module bancaire.
    validee = compta.valider(resultat["ecriture"]["ecriture_id_opaque"], acteur="comptable",
                             db_path=base)
    assert validee["ok"] is True

    ligne = _ligne_rapprochee(base)
    assert ligne["traitement_libelle"] == "Rapproché — comptabilisé"
    assert ligne["rapprochements"][0]["etat_comptable"] == ecran.ETAT_COMPTABILISE


def test_le_module_bancaire_ne_valide_jamais_une_ecriture_de_lui_meme():
    """Aucun appel à la validation comptable depuis le service bancaire."""
    from pathlib import Path
    source = (Path(cfg.APP_ROOT) / "app" / "services"
              / "qonto_validation_service.py").read_text(encoding="utf-8")
    assert "compta.valider" not in source
    assert "ST_VALIDEE" not in source


# ══ Qonto ═════════════════════════════════════════════════════════════════════════════════════
def test_aucun_verrou_n_ouvre_une_ecriture_vers_qonto(monkeypatch):
    """Ces drapeaux gouvernent SQLite. Le client bancaire, lui, ne sait faire que des GET."""
    from app.adapters import qonto_client
    poser_verrous(monkeypatch, banque=True, comptabilite=True)

    client = qonto_client.ClientQontoLectureSeule("faux", "faux")
    for interdite in ("POST", "PUT", "PATCH", "DELETE"):
        with pytest.raises(qonto_client.MethodeInterdite):
            client.requete(interdite, "/transactions")
    for verbe in ("post", "put", "patch", "delete"):
        assert not hasattr(qonto_client.ClientQontoLectureSeule, verbe)


# ══ L'écran ═══════════════════════════════════════════════════════════════════════════════════
@pytest.fixture()
def client(base):
    from app.main import app
    return TestClient(app)


def test_l_ecran_affiche_l_ecriture_restant_a_valider(client, base, monkeypatch):
    poser_verrous(monkeypatch, banque=True, comptabilite=True)
    _importer(base, [mouvement(1, **CREDIT_200)])
    validation.valider(_uuid(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                       db_path=base)

    html = client.get("/banques-caisse?vue=banque").text
    assert "Rapproché — écriture à valider" in html
    assert "Apport compte courant — Associé de test" in html


def test_l_ecran_de_traitement_annonce_les_verrous_fermes(client, base, monkeypatch):
    poser_verrous(monkeypatch, banque=False, comptabilite=False)
    _importer(base, [mouvement(1, **CREDIT_200)])
    conn = get_db(base)
    try:
        opaque = conn.execute(
            "SELECT mouvement_id_opaque FROM qonto_transactions_statut_local").fetchone()[0]
    finally:
        conn.close()

    page = client.get(f"/banques-caisse/qonto/{opaque}/traiter?nature=APPORT_ASSOCIE"
                      f"&objet_id={ASSOCIE}")
    assert page.status_code == 200
    assert 'data-testid="verrous-fermes"' in page.text
    assert "BANQUE_REAL_WRITE_ENABLED" in page.text, "dire QUOI activer, pas seulement « refusé »"
    assert 'data-testid="bouton-valider"' not in page.text

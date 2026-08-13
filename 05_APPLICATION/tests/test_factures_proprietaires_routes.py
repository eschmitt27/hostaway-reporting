"""Parcours applicatif complet des factures propriétaires, via de vraies requêtes HTTP.

Prévisualiser -> créer un brouillon -> valider -> émettre -> télécharger le PDF -> avoir.
Aucune donnée réelle : base temporaire, identités et montants de fixture.
"""
import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_service as svc


@pytest.fixture()
def client(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_NOM", "Conciergerie Fixture", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_ADRESSE", "1 rue de Test", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_SIRET", "00000000000000", raising=False)
    apply_migrations(chemin)

    from app.main import app as application
    return TestClient(application), chemin, tmp_path


def _source():
    return {
        "mois": "2026-06", "proprietaire_id": "PROP_FIXT_1", "logement_id": "LOG_FIXT_1",
        "source_calcul": "PREF-FIXT-001",
        "COMMISSION_CONCIERGERIE": 300.0, "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0,
        "montant_du_conciergerie": 500.0, "TOTAL_PAYOUT": 2000.0,
    }


def test_liste_repond(client):
    c, _, _ = client
    r = c.get("/factures-proprietaires")
    assert r.status_code == 200
    assert "Factures propri" in r.text


def test_previsualisation_sans_ecriture(client):
    c, db, _ = client
    r = c.get("/factures-proprietaires/proposer?mois=2026-06")
    assert r.status_code == 200
    assert svc.lister(db_path=db) == []      # lecture pure : rien n'a été créé


def test_parcours_complet_jusqu_au_pdf(client):
    c, db, tmp = client
    facture = svc.creer(_source(), db_path=db)
    fid = facture["facture_id_opaque"]

    r = c.get(f"/factures-proprietaires/{fid}")
    assert r.status_code == 200
    assert "BROUILLON" in r.text
    assert "500.00" in r.text or "500,00" in r.text

    assert c.post(f"/factures-proprietaires/{fid}/valider",
                  follow_redirects=False).status_code == 303
    assert svc.lire(fid, db_path=db)["statut"] == svc.ST_VALIDE

    assert c.post(f"/factures-proprietaires/{fid}/emettre",
                  data={"date_facture": "2026-07-01"},
                  follow_redirects=False).status_code == 303
    emise = svc.lire(fid, db_path=db)
    assert emise["statut"] == svc.ST_EMIS
    assert emise["numero_facture"]
    assert emise["document_hash"]

    doc = c.get(f"/factures-proprietaires/{fid}/document")
    assert doc.status_code == 200
    assert doc.headers["content-type"] == "application/pdf"
    assert doc.content.startswith(b"%PDF")
    # Le fichier servi est celui qui a été figé, pas un PDF reconstruit à la volée.
    assert hashlib.sha256(doc.content).hexdigest() == emise["document_hash"]


def test_document_absent_avant_emission(client):
    c, db, _ = client
    f = svc.creer(_source(), db_path=db)
    r = c.get(f"/factures-proprietaires/{f['facture_id_opaque']}/document")
    assert r.status_code == 404
    assert svc.C_PDF_ABSENT in r.text


def test_avoir_depuis_l_interface(client):
    c, db, _ = client
    f = svc.creer(_source(), db_path=db)
    fid = f["facture_id_opaque"]
    c.post(f"/factures-proprietaires/{fid}/valider", follow_redirects=False)
    c.post(f"/factures-proprietaires/{fid}/emettre", data={"date_facture": "2026-07-01"},
           follow_redirects=False)

    r = c.post(f"/factures-proprietaires/{fid}/avoir", data={"motif": "montant errone"},
               follow_redirects=False)
    assert r.status_code == 303
    avoirs = [x for x in svc.lister(db_path=db) if x["type_document"] == svc.TYPE_AVOIR]
    assert len(avoirs) == 1
    assert avoirs[0]["montant_total"] == -500.0
    assert svc.lire(fid, db_path=db)["montant_total"] == 500.0   # originale intacte


def test_documents_ecrits_sous_data_dir_redirige(client):
    """Non-régression : le répertoire des PDF doit suivre DATA_DIR, résolu à l'appel.

    Un défaut initial figeait ce répertoire à l'import de la configuration : une instance de
    recette (ou ce test) écrivait alors son PDF dans le vrai dossier `data/` de l'application.
    """
    from app.routes import factures_proprietaires as routes

    c, db, tmp = client
    f = svc.creer(_source(), db_path=db)
    fid = f["facture_id_opaque"]
    c.post(f"/factures-proprietaires/{fid}/valider", follow_redirects=False)
    c.post(f"/factures-proprietaires/{fid}/emettre", data={"date_facture": "2026-07-01"},
           follow_redirects=False)

    repertoire = routes._repertoire_documents()
    assert Path(tmp) in repertoire.parents or repertoire == Path(tmp) / "factures_proprietaires"
    emise = svc.lire(fid, db_path=db)
    assert (repertoire / "2026" / "06" / emise["document_nom"]).exists()


def test_identite_emetteur_incomplete_bloque_la_validation(client, monkeypatch):
    c, db, _ = client
    monkeypatch.setattr(cfg, "SOCIETE_SIRET", "", raising=False)
    f = svc.creer(_source(), db_path=db)
    fid = f["facture_id_opaque"]
    with pytest.raises(svc.FactureProprietaireError, match=svc.C_IDENTITE):
        c.post(f"/factures-proprietaires/{fid}/valider", follow_redirects=False)
    assert svc.lire(fid, db_path=db)["statut"] == svc.ST_BROUILLON

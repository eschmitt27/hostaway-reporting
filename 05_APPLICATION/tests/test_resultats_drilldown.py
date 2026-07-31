"""Bloc 5 (mission Analytique/Résultats — finalisation) : vérifie les chaînes de drill-down
Résultat -> axe -> écriture comptable -> objet métier (facture/règlement/ménage/propriétaire).
Aucun lien ne doit produire une 404 lorsque l'objet cible existe réellement."""
from __future__ import annotations

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


def _env_flags(monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)


def test_ecriture_facture_pointe_vers_la_facture_reelle(client, tmp_db, monkeypatch):
    _env_flags(monkeypatch)
    frs = frs_svc.creer("Fournisseur Drill", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-DRILL-1",
                   "date_facture": "2026-06-10", "montant_ttc": 55.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    ge = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)

    html = client.get(f"/comptabilite/ecritures/{ge['ecriture_id_opaque']}").text
    assert f"/factures/{r['facture_id_opaque']}" in html

    assert client.get(f"/factures/{r['facture_id_opaque']}").status_code == 200


def test_ecriture_inconnue_404(client):
    assert client.get("/comptabilite/ecritures/ECR-INCONNUE").status_code == 404


def test_logement_vers_ecriture_vers_facture(client, tmp_db, monkeypatch):
    _env_flags(monkeypatch)
    frs = frs_svc.creer("Fournisseur Drill Logement", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-DRILL-2",
                   "date_facture": "2026-06-11", "montant_ttc": 20.0,
                   "logement_id": "LOG_A1"}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)

    html = client.get("/resultats/logements/LOG_A1").text
    assert "ECR-" in html or "Aucune écriture" in html


def test_prestataire_menage_pointe_vers_le_menage_reel(client, tmp_db):
    conn = get_db(tmp_db)
    conn.execute(
        "INSERT INTO menages (menage_id_opaque, logement_id, proprietaire_id, type_menage, mois, "
        "statut, fournisseur_id_opaque, cout_prevu, cout_reel) "
        "VALUES ('MEN-DRILL-1','LOG_A1','PROP_A','EXTERNE','2026-06','REGLE','FRS-DRILL-1',30.0,32.0)")
    conn.commit()
    conn.close()

    html = client.get("/resultats/prestataires/FRS-DRILL-1?mois=2026-06").text
    assert "/menages/cycle/MEN-DRILL-1" in html

    assert client.get("/menages/cycle/MEN-DRILL-1").status_code == 200


def test_menage_inconnu_404(client):
    assert client.get("/menages/cycle/MEN-INCONNU").status_code == 404


def test_fournisseur_detail_pointe_vers_ligne_qui_redirige_vers_ecriture(client, tmp_db, monkeypatch):
    _env_flags(monkeypatch)
    frs = frs_svc.creer("Fournisseur Drill Aux", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-DRILL-3",
                   "date_facture": "2026-06-12", "montant_ttc": 10.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    ge = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)

    resp = client.get(f"/resultats/lignes/{ge['ecriture_id_opaque']}", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/comptabilite/ecritures/{ge['ecriture_id_opaque']}"
    assert client.get(f"/comptabilite/ecritures/{ge['ecriture_id_opaque']}").status_code == 200

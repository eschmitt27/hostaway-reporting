"""APP-3F — Routes de l'interface de rapprochement (dans /proprietaires-reglements).

Aucun paiement, aucun virement, aucune donnée bancaire sensible. Fixtures : source bancaire
monkeypatchée en lecture seule.
"""
import pytest

from app.services import proprietaires_paiement_service as pay
from app.services import rapprochement_reglements_service as rap


class _FakeSource:
    def __init__(self, etat, lignes):
        self.etat = type("E", (), {"etat": etat})()
        self.lignes = lignes


_MVT = {"mouvement_id": "M001", "date_operation": "2026-01-05", "montant": -120.0, "sens": "DEBIT",
        "libelle": "VIR PROP", "reference": "R1"}


def _demarrer(client, prop="PROP_R", mois="2026-01"):
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": prop, "mois": mois}, follow_redirects=False)
    return r.headers["location"].rsplit("/", 1)[-1]


def _paye(client, opaque, monkeypatch):
    from app.services import proprietaires_reglements_service as svc
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"net": 120.0, "ca_retenu": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [{"logement_id": "L1"}], "factures": [], "anomalies": []})
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/pret-a-payer")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/marquer-paye",
                data={"reference_interne": "vu le 05/01"})


def _patch_banque(monkeypatch, etat, lignes):
    from app.readers import banques_reader as b
    monkeypatch.setattr(b, "mouvements", lambda: _FakeSource(etat, lignes))


def test_a_payer_liste_affiche_rapprochement(client):
    r = client.get("/proprietaires-reglements/a-payer")
    assert r.status_code == 200 and "Rapprochement" in r.text


def test_fiche_rapprochement_200(client):
    opq = _demarrer(client)
    r = client.get(f"/proprietaires-reglements/{opq}/rapprochement")
    assert r.status_code == 200


def test_fiche_rapprochement_404_inconnu(client):
    assert client.get("/proprietaires-reglements/REG-0000000000/rapprochement").status_code == 404


def test_route_statique_export_non_capturee(client):
    """/a-payer/rapprochement-export.csv est servie par sa route dédiée (pas un identifiant dynamique)."""
    r = client.get("/proprietaires-reglements/a-payer/rapprochement-export.csv")
    assert r.status_code == 200 and "ne constitue ni un ordre de paiement" in r.text


def test_recherche_candidat_exact(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher", follow_redirects=False)
    assert r.status_code == 303
    rapp = rap.charger_par_releve(opq)
    assert rapp["statut"] == rap.ST_PROPOSITION_DISPONIBLE and rapp["mouvement_id_opaque"]


def test_recherche_aucun_candidat_source_absente(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_FICHIER_ABSENT, [])
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher", follow_redirects=False)
    assert r.status_code == 303
    rapp = rap.charger_par_releve(opq)
    assert rapp["statut"] == rap.ST_NON_RAPPROCHE   # pas de candidat -> reste non rapproché


def test_flux_confirmation_complet(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/controler")
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/confirmer",
                    data={"commentaire": "ok"}, follow_redirects=False)
    assert r.status_code == 303
    assert rap.charger_par_releve(opq)["statut"] == rap.ST_RAPPROCHE


def test_confirmation_reglement_non_paye_refusee_sans_500(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)   # jamais marqué payé
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/controler")
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/confirmer", follow_redirects=False)
    assert r.status_code == 303   # refus propre, pas de 500
    assert rap.charger_par_releve(opq)["statut"] != rap.ST_RAPPROCHE


def test_ecarter_route(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/ecarter",
                    data={"motif": "pas le bon mouvement"}, follow_redirects=False)
    assert r.status_code == 303 and rap.charger_par_releve(opq)["statut"] == rap.ST_ECARTE


def test_anomalie_route(client):
    opq = _demarrer(client)
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/anomalie",
                    data={"motif": "ambigu"}, follow_redirects=False)
    assert r.status_code == 303 and rap.charger_par_releve(opq)["statut"] == rap.ST_ANOMALIE


def test_reouverture_route(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/controler")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/confirmer")
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/rouvrir",
                    data={"motif": "erreur"}, follow_redirects=False)
    assert r.status_code == 303 and rap.charger_par_releve(opq)["statut"] == rap.ST_ROUVERT


def test_double_confirmation_sans_500(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [_MVT])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/controler")
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/confirmer")
    r = client.post(f"/proprietaires-reglements/{opq}/rapprochement/confirmer", follow_redirects=False)
    assert r.status_code == 303   # 2e confirmation refusée proprement (RAPPROCHE->RAPPROCHE interdit)


def test_fiche_ne_montre_aucune_donnee_bancaire(client, monkeypatch):
    from app.readers import rapprochement_bancaire_reader as contrat
    opq = _demarrer(client)
    _paye(client, opq, monkeypatch)
    _patch_banque(monkeypatch, contrat.ETAT_OK, [
        {**_MVT, "compte_id": "CM_02211_00021321603", "libelle_brut": "IBAN FR76 1234"}])
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/rechercher")
    txt = client.get(f"/proprietaires-reglements/{opq}/rapprochement").text
    assert "FR76" not in txt and "02211" not in txt and "IBAN" not in txt.upper().replace("IBAN FR", "")


def test_fiche_montre_mention_declarative(client):
    opq = _demarrer(client)
    txt = client.get(f"/proprietaires-reglements/{opq}/rapprochement").text
    assert "ne constitue ni un ordre de paiement ni une preuve bancaire certifiée" in txt


def test_aucun_id_sqlite_dans_url_rapprochement(client):
    opq = _demarrer(client)
    assert opq.startswith("REG-") and not opq.isdigit()

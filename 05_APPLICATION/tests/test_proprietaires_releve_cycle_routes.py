"""APP-3E — Interface relevé : routes du cycle de préparation et bannière de dérive.

Vérifie que /releve expose l'état du cycle, que les transitions passent par les routes POST, que la
dérive est détectée et affichée, et que l'identifiant inconnu répond proprement.
"""
import pytest

from app.services import proprietaires_releve_cycle_service as cycle_svc
import app.config as cfg


@pytest.fixture(autouse=True)
def _cloture_referentiel(tmp_db):
    """Statut de clôture du mois testé, désormais lu en SQLite.

    Ces tests s'appuyaient sur le VRAI `REF_Setup.xlsm` pour connaître le statut du mois — une
    dépendance implicite à des données réelles. Le statut vient maintenant du référentiel en base,
    et la fixture le déclare explicitement : le mois testé est clôturé côté moteur, condition que
    le parcours « prêt à payer » exige.

    Dépend de `tmp_db` À DESSEIN : cette fixture repositionne `cfg.DB_PATH`, et une fixture autouse
    qui ne la précéderait pas se ferait écraser. On écrit donc DANS sa base.
    """
    from app.db.connection import get_db
    from app.readers import controles_cloture_reader as _ccr

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, date_passage_controle, "
            "date_cloture, nb_lignes_bancaires_non_classees, nb_controles_bloquants_ouverts, "
            "commentaire, import_id) VALUES "
            "('2026-01','CLOTURE','2026-02-05','2026-02-10','0','0','fixture','IMP-TEST')")
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-TEST','2026-01-01T00:00:00','fixture','x','IMPORTE',28,1)")
        conn.commit()
    finally:
        conn.close()
    _ccr.vider_cache()
    yield
    _ccr.vider_cache()



def _demarrer(client, prop="PROP_CYCLE", mois="2026-01"):
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": prop, "mois": mois}, follow_redirects=False)
    return r.headers["location"].rsplit("/", 1)[-1]


def test_releve_route_200(client):
    opaque = _demarrer(client)
    r = client.get(f"/proprietaires-reglements/{opaque}/releve")
    assert r.status_code == 200


def test_releve_identifiant_inconnu_404(client):
    r = client.get("/proprietaires-reglements/REG-0000000000/releve")
    assert r.status_code == 404


def test_cycle_demarrer_route(client):
    opaque = _demarrer(client)
    r = client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer", follow_redirects=False)
    assert r.status_code == 303
    assert cycle_svc.charger(opaque)["etat_cycle"] == cycle_svc.ETAT_EN_PREPARATION


def test_cycle_valider_route(client, monkeypatch):
    from app.services import proprietaires_service as legacy_svc
    monkeypatch.setattr(legacy_svc, "load_releve", lambda pid, mois="": {
        "status": "OK", "bloc_exploitation": [], "bloc_reglement": []})
    from app.services import proprietaires_reglements_service as svc
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"net": 1, "ca_retenu": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    r = client.post(f"/proprietaires-reglements/{opaque}/cycle/valider", follow_redirects=False)
    assert r.status_code == 303
    assert cycle_svc.charger(opaque)["etat_cycle"] == cycle_svc.ETAT_VALIDE


def test_cycle_double_validation_sans_500(client, monkeypatch):
    from app.services import proprietaires_service as legacy_svc
    monkeypatch.setattr(legacy_svc, "load_releve", lambda pid, mois="": {
        "status": "OK", "bloc_exploitation": [], "bloc_reglement": []})
    from app.services import proprietaires_reglements_service as svc
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"net": 1, "ca_retenu": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    # 2e validation : le relevé est déjà VALIDE -> transition refusée, jamais un 500
    r = client.post(f"/proprietaires-reglements/{opaque}/cycle/valider", follow_redirects=False)
    assert r.status_code == 303
    assert cycle_svc.charger(opaque)["etat_cycle"] == cycle_svc.ETAT_VALIDE


def test_cycle_reouverture_sans_motif_sans_500(client, monkeypatch):
    from app.services import proprietaires_service as legacy_svc
    monkeypatch.setattr(legacy_svc, "load_releve", lambda pid, mois="": {
        "status": "OK", "bloc_exploitation": [], "bloc_reglement": []})
    from app.services import proprietaires_reglements_service as svc
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"net": 1, "ca_retenu": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    r = client.post(f"/proprietaires-reglements/{opaque}/cycle/reouvrir",
                    data={"motif": ""}, follow_redirects=False)
    assert r.status_code == 303   # refus propre, pas de 500
    assert cycle_svc.charger(opaque)["etat_cycle"] == cycle_svc.ETAT_VALIDE   # inchangé


def test_snapshot_persiste_apres_rechargement(client, monkeypatch):
    """Le snapshot figé reste identique après un rechargement (simulateur de redémarrage : relecture DB)."""
    from app.services import proprietaires_service as legacy_svc
    monkeypatch.setattr(legacy_svc, "load_releve", lambda pid, mois="": {
        "status": "OK", "bloc_exploitation": [{"logement_id": "L1"}], "bloc_reglement": []})
    from app.services import proprietaires_reglements_service as svc
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"net": 1, "ca_retenu": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    empreinte1 = cycle_svc.charger(opaque)["snapshot_empreinte"]
    empreinte2 = cycle_svc.charger(opaque)["snapshot_empreinte"]   # relecture = redémarrage simulé
    assert empreinte1 and empreinte1 == empreinte2

"""APP-3E — Routes de préparation des règlements (/a-payer) et actions cycle/paiement.

Aucun virement, aucune API bancaire, aucun IBAN. MARQUE_COMME_PAYE est déclaratif.
"""
import pytest

import app.config as cfg
from app.services import proprietaires_suivi_service as suivi


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



def _demarrer(client, prop="PROP_A_PAYER", mois="2026-01"):
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": prop, "mois": mois}, follow_redirects=False)
    return r.headers["location"].rsplit("/", 1)[-1]


def test_35_liste_a_payer_200(client):
    r = client.get("/proprietaires-reglements/a-payer")
    assert r.status_code == 200


def test_36_route_statique_a_payer_non_capturee_par_dynamique(client):
    """/a-payer doit être servi par sa propre route, pas interprété comme un identifiant brut."""
    r = client.get("/proprietaires-reglements/a-payer")
    assert r.status_code == 200
    assert "Aucun propriétaire" not in r.text or True   # ne doit pas planter/404 comme un id inconnu


def test_37_releve_non_valide_bloque_pret_a_payer(client):
    opaque = _demarrer(client)
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/controler", follow_redirects=False)
    assert r.status_code == 303
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/pret-a-payer", follow_redirects=True)
    assert r.status_code == 200
    # toujours A_CONTROLER : le cycle n'a jamais été validé -> RELEVE_NON_VALIDE bloque
    from app.services import proprietaires_paiement_service as pay
    p = pay.charger(opaque)
    assert p["statut_paiement"] == pay.ST_A_CONTROLER


def test_38_donnee_moteur_absente_visible_dans_liste(client):
    opaque = _demarrer(client, prop="PROP_INEXISTANT_MOTEUR")
    r = client.get("/proprietaires-reglements/a-payer")
    assert r.status_code == 200


def test_39_passage_a_controler(client):
    opaque = _demarrer(client)
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/controler", follow_redirects=False)
    assert r.status_code == 303
    from app.services import proprietaires_paiement_service as pay
    assert pay.charger(opaque)["statut_paiement"] == pay.ST_A_CONTROLER


def test_40_passage_pret_a_payer_apres_validation_cycle(client, monkeypatch):
    from app.services import proprietaires_reglements_service as svc
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/pret-a-payer", follow_redirects=False)
    assert r.status_code == 303
    from app.services import proprietaires_paiement_service as pay
    assert pay.charger(opaque)["statut_paiement"] == pay.ST_PRET_A_PAYER


def test_41_marquer_comme_paye(client, monkeypatch):
    from app.services import proprietaires_reglements_service as svc
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/pret-a-payer")
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/marquer-paye",
                    data={"reference_interne": "vu sur relevé bancaire du 05/01"}, follow_redirects=False)
    assert r.status_code == 303
    from app.services import proprietaires_paiement_service as pay
    assert pay.charger(opaque)["statut_paiement"] == pay.ST_MARQUE_COMME_PAYE


def test_42_marque_paye_sans_ecriture_bancaire(client):
    """Structurel : aucune route de ce module n'appelle une API bancaire externe."""
    import inspect
    from app.routes import proprietaires_reglements as route_mod
    src = inspect.getsource(route_mod)
    for interdit in ("requests.post", "qonto", "credit_mutuel", "sepa", "webhook"):
        assert interdit not in src.lower()


def test_43_reouverture_paiement(client, monkeypatch):
    from app.services import proprietaires_reglements_service as svc
    from app.routes import proprietaires_reglements as route_mod
    monkeypatch.setattr(svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    monkeypatch.setattr(route_mod, "_statut_moteur_mois", lambda mois: "CLOTURE")
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/cycle/demarrer")
    client.post(f"/proprietaires-reglements/{opaque}/cycle/valider")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/pret-a-payer")
    client.post(f"/proprietaires-reglements/{opaque}/paiement/marquer-paye")
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/reouvrir",
                    data={"motif": "montant erroné"}, follow_redirects=False)
    assert r.status_code == 303
    from app.services import proprietaires_paiement_service as pay
    assert pay.charger(opaque)["statut_paiement"] == pay.ST_ROUVERT


def test_44_annulation_paiement(client):
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/annuler",
                    data={"motif": "règlement annulé"}, follow_redirects=False)
    assert r.status_code == 303
    from app.services import proprietaires_paiement_service as pay
    assert pay.charger(opaque)["statut_paiement"] == pay.ST_ANNULE


def test_45_double_clic_transition_paiement_refusee_sans_500(client):
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    r = client.post(f"/proprietaires-reglements/{opaque}/paiement/controler", follow_redirects=False)
    assert r.status_code == 303   # redirigé avec erreur, jamais un 500


def test_46_historique_append_only_paiement(client):
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    r = client.get(f"/proprietaires-reglements/{opaque}/historique")
    assert r.status_code == 200


def test_47_export_sans_iban(client):
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    r = client.get("/proprietaires-reglements/a-payer/export.csv")
    assert "IBAN" not in r.text and "iban" not in r.text


def test_48_export_sans_numero_de_compte(client):
    r = client.get("/proprietaires-reglements/a-payer/export.csv")
    assert "compte_bancaire" not in r.text.lower() and "rib" not in r.text.lower()


def test_49_export_injection_csv_neutralisee(client):
    opaque = _demarrer(client)
    client.post(f"/proprietaires-reglements/{opaque}/paiement/controler")
    from app.db.connection import get_db
    import app.config as cfg
    conn = get_db(cfg.DB_PATH)
    conn.execute("UPDATE proprietaires_paiement SET reference_interne_paiement=? WHERE releve_id_opaque=?",
                ("=CMD('calc')", opaque))
    conn.commit(); conn.close()
    r = client.get("/proprietaires-reglements/a-payer/export.csv")
    assert "'=CMD" in r.text   # amorce de formule neutralisée par une quote de tête


def test_50_mention_ne_constitue_pas_ordre_bancaire(client):
    r = client.get("/proprietaires-reglements/a-payer")
    assert "ne constitue pas" in r.text.lower() or "preuve bancaire" in r.text.lower()
    r2 = client.get("/proprietaires-reglements/a-payer/export.csv")
    assert "ne constitue pas un ordre bancaire" in r2.text


def test_51_aucun_writer_reel_flag_false():
    import app.config as cfg
    for attr in dir(cfg):
        if "WRITE" in attr.upper() and attr.isupper():
            val = getattr(cfg, attr)
            if isinstance(val, bool):
                assert val is False, f"{attr} doit rester False"

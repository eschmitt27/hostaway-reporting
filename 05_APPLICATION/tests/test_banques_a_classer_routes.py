"""File humaine de classement Banque (HTTP) : liste, détail, prévisualisation, décision, historique."""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.services import banques_controle_service as ctrl_svc



def _ligne(mid, montant=8.64, sens="DEBIT", categorie="", statut_classification="A_ENVOYER_IA"):
    """Un mouvement de la file « à classer ».

    Le libellé est affiché, et c'est légitime : la banque n'en fournit pas de version expurgée. Ce qui
    ne doit jamais sortir est la contrepartie brute, que la fabrique remplit avec un marqueur.
    """
    return fx.mouvement(mid, "2026-01-05", "PAIEMENT CB TEST", montant, sens,
                        compte="CM_TEST", categorie=categorie, niveau_risque="MOYEN",
                        statut_controle="A_CONTROLER", niveau_anomalie="A_CONTROLER",
                        statut_classification=statut_classification, regle_id="R_099",
                        type_flux="")


@pytest.fixture
def ref(tmp_db, tmp_path, monkeypatch):
    """Jeu Banque en base. Aucun classeur."""
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    fx.construire(tmp_db, mouvements=[
        _ligne("MVT-CLASS-001"),
        _ligne("MVT-CLASS-002", montant=200.0, sens="CREDIT"),
    ])
    reader.vider_cache()
    ctrl_svc.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl_svc.vider_cache()


def _opq(mid):
    return ctrl_svc.id_opaque(mid)


def test_01_liste(client, ref):
    r = client.get("/banques-caisse/a-classer")
    assert r.status_code == 200 and "2 mouvements" in r.text


def test_02_filtre_montant(client, ref):
    r = client.get("/banques-caisse/a-classer?montant_min=100")
    assert "1 mouvement" in r.text


def test_03_pagination_presente(client, ref):
    r = client.get("/banques-caisse/a-classer")
    assert r.status_code == 200   # peu de lignes ici, juste vérifier que la page ne casse pas


def test_04_detail(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.get(f"/banques-caisse/a-classer/{opq}")
    assert r.status_code == 200 and opq in r.text


def test_05_previsualisation_categoriser(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/previsualiser",
                    data={"type_decision": "CATEGORISER", "nouvelle_categorie": "LOGICIEL_GESTION"})
    assert r.status_code == 200 and "LOGICIEL_GESTION" in r.text


def test_06_categorie_existante(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "CATEGORISER", "nouvelle_categorie": "LOGICIEL_GESTION"},
                    follow_redirects=True)
    assert "CATEGORISER" in r.text


def test_07_categorie_invalide(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "CATEGORISER", "nouvelle_categorie": "INVENTEE"},
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"]


def test_08_maintenir_a_controler(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "MAINTENIR_A_CONTROLER"}, follow_redirects=True)
    assert "MAINTENIR_A_CONTROLER" in r.text


def test_09_non_classe(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "NON_CLASSE"}, follow_redirects=True)
    assert "NON_CLASSE" in r.text


def test_10_reporter(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "REPORTER"}, follow_redirects=True)
    assert "REPORTER" in r.text


def test_11_justification(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "REPORTER", "justification": "à revoir"},
                    follow_redirects=False)
    assert r.status_code == 303


def test_12_anomalie_moteur(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "REPORTER", "anomalie_moteur": "libellé tronqué"},
                    follow_redirects=False)
    assert r.status_code == 303


def test_13_future_regle(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision",
                    data={"type_decision": "REPORTER", "future_regle": "nouvelle règle"},
                    follow_redirects=False)
    assert r.status_code == 303


def test_14_historique_append_only(client, ref):
    opq = _opq("MVT-CLASS-001")
    client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "REPORTER"})
    client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "NON_CLASSE"})
    r = client.get(f"/banques-caisse/a-classer/{opq}/historique")
    assert r.status_code == 200
    assert r.text.count("REPORTER") >= 1 and "NON_CLASSE" in r.text


def test_15_seconde_decision(client, ref):
    opq = _opq("MVT-CLASS-001")
    client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "REPORTER"})
    client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "NON_CLASSE"})
    r = client.get(f"/banques-caisse/a-classer/{opq}")
    assert "NON_CLASSE" in r.text


def test_16_aucune_ia_externe(client, ref):
    import inspect
    from app.services import banques_classement_service as svc
    src = inspect.getsource(svc)
    for interdit in ("requests.", "httpx.", "openai", "anthropic"):
        assert interdit not in src


def test_17_aucune_requete_reseau(client, ref):
    # Aucune dépendance réseau importée dans les routes du module (couvert par test_16 côté service).
    import inspect
    from app.routes import banques as routes_mod
    src = inspect.getsource(routes_mod)
    for interdit in ("requests.", "httpx.get", "httpx.post"):
        assert interdit not in src


def test_18_aucune_pii(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.get(f"/banques-caisse/a-classer/{opq}")
    assert "iban" not in r.text.lower()


def test_19_aucun_libelle_brut_complet(client, ref):
    r = client.get("/banques-caisse/a-classer")
    assert "NUMERO COMPTE SECRET" not in r.text


def test_20_le_brut_bancaire_nest_jamais_reecrit(client, ref):
    """Une décision passée par l'écran ne touche pas ce que la banque a envoyé.

    Il n'y a plus de fichier dont vérifier la date : la garantie porte sur la table du brut.
    """
    from app.db.connection import get_db

    def _brut():
        conn = get_db(ref)
        try:
            return conn.execute(
                "SELECT mouvement_id_opaque, montant, sens, libelle_brut, fingerprint "
                "FROM banque_mouvements ORDER BY mouvement_id_opaque").fetchall()
        finally:
            conn.close()

    avant = _brut()
    opq = _opq("MVT-CLASS-001")
    client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "NON_CLASSE"})
    assert _brut() == avant


def test_21_aucune_ecriture_reelle(client, ref):
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False


def test_22_garde_recette_non_bloquante(client, ref):
    opq = _opq("MVT-CLASS-001")
    r = client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "REPORTER"},
                    follow_redirects=False)
    assert r.status_code == 303


def test_23_idempotence(client, ref):
    opq = _opq("MVT-CLASS-001")
    r1 = client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "REPORTER"},
                     follow_redirects=False)
    r2 = client.post(f"/banques-caisse/a-classer/{opq}/decision", data={"type_decision": "REPORTER"},
                     follow_redirects=False)
    assert r1.status_code == r2.status_code == 303


def test_24_404_propre(client, ref):
    r = client.get("/banques-caisse/a-classer/MVT-INEXISTANT")
    assert r.status_code == 404
    r2 = client.get("/banques-caisse/a-classer/MVT-INEXISTANT/historique")
    assert r2.status_code == 404


def test_25_headers_securite(client, ref):
    r = client.get("/banques-caisse/a-classer")
    assert r.headers.get("X-Frame-Options") == "DENY"

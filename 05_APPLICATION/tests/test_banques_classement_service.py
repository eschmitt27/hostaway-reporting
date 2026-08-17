"""File humaine de classement Banque (A_ENVOYER_IA, migration 0026). Fixtures fictives uniquement."""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.services import banques_classement_service as svc
from app.services import banques_controle_service as ctrl_svc



def _ligne(mid, montant=8.64, sens="DEBIT", categorie="", statut_classification="A_ENVOYER_IA",
          date_operation="2026-01-05"):
    """Un mouvement A_ENVOYER_IA par défaut : c'est la file que ces tests exercent."""
    return fx.mouvement(mid, date_operation, "PAIEMENT CB TEST", montant, sens,
                        compte="CM_TEST", categorie=categorie, niveau_risque="MOYEN",
                        statut_controle="A_CONTROLER", niveau_anomalie="A_CONTROLER",
                        statut_classification=statut_classification, regle_id="R_099",
                        type_flux="")


@pytest.fixture
def source(tmp_db, tmp_path, monkeypatch):
    """Jeu Banque en base. Aucun classeur."""
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    fx.construire(tmp_db, mouvements=[
        _ligne("MVT-TEST-001"),
        _ligne("MVT-TEST-002", montant=120.0, sens="CREDIT"),
        _ligne("MVT-TEST-003", montant=45.0, statut_classification="CLASSE",
               categorie="LOGICIEL_GESTION"),
    ])
    reader.vider_cache()
    ctrl_svc.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl_svc.vider_cache()


def _opq(mid):
    return ctrl_svc.id_opaque(mid)


def test_01_lister_ne_retient_que_a_envoyer_ia(source, tmp_db):
    lignes = svc.lister(db_path=tmp_db)
    assert len(lignes) == 2
    assert all(l["statut_humain"] == "SANS_DECISION" for l in lignes)


def test_02_compter(source):
    assert svc.compter() == 2


@pytest.fixture
def source_avec_doublon(tmp_db, tmp_path, monkeypatch):
    """Deux mouvements DISTINCTS partageant tout sauf leur identifiant.

    Le cas d'origine — deux lignes portant le MÊME `mouvement_id`, dupliquées en amont par lot8a — ne
    peut plus se produire : `mouvement_id_opaque` est UNIQUE en base. La garantie n'est plus à tester,
    elle est structurelle, et un test la vérifie directement ci-dessous.

    Ce qui reste à couvrir est le cas voisin et bien réel : deux mouvements identiques au montant et
    au libellé près, qui doivent rester DEUX lignes — un double prélèvement existe.
    """
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    fx.construire(tmp_db, mouvements=[
        _ligne("MVT-DUP-001", montant=120.0),
        _ligne("MVT-DUP-002", montant=120.0),
        _ligne("MVT-TEST-002", montant=45.0),
    ])
    reader.vider_cache()
    ctrl_svc.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl_svc.vider_cache()


def test_02a_identifiant_duplique_impossible_en_base(tmp_db):
    """Ce que le classeur ne pouvait pas empêcher, la base l'interdit."""
    import sqlite3

    fx.construire(tmp_db, mouvements=[_ligne("MVT-UNIQUE-001")])
    with pytest.raises(sqlite3.IntegrityError):
        fx.construire(tmp_db, mouvements=[_ligne("MVT-UNIQUE-001")])


def test_02b_deux_mouvements_identiques_restent_deux(source_avec_doublon):
    """Même montant, même libellé : deux vrais mouvements possibles. Les fondre en perdrait un."""
    assert svc.compter() == 3


def test_02c_chaque_mouvement_a_son_propre_identifiant_opaque(source_avec_doublon, tmp_db):
    lignes = svc.lister(db_path=tmp_db)
    assert len(lignes) == 3
    ids_opaques = [l["id_opaque"] for l in lignes]
    assert len(ids_opaques) == len(set(ids_opaques)), "aucun id_opaque ne doit apparaître deux fois"


def test_02d_une_decision_ne_porte_que_sur_son_mouvement(source_avec_doublon, tmp_db):
    """Décider sur l'un des deux mouvements identiques ne décide pas pour l'autre."""
    opq = _opq("MVT-DUP-001")
    svc.decider(opq, "MAINTENIR_A_CONTROLER", db_path=tmp_db)
    lignes = svc.lister(db_path=tmp_db)
    ligne = next(l for l in lignes if l["id_opaque"] == opq)
    assert ligne["statut_humain"] == "MAINTENIR_A_CONTROLER"
    assert sum(1 for l in lignes if l["id_opaque"] == opq) == 1

    autre = next(l for l in lignes if l["id_opaque"] == _opq("MVT-DUP-002"))
    assert autre["statut_humain"] != "MAINTENIR_A_CONTROLER", \
        "le mouvement jumeau garde son propre statut"


def test_03_filtre_montant(source, tmp_db):
    assert len(svc.lister(montant_min=100, db_path=tmp_db)) == 1


def test_04_filtre_sens(source, tmp_db):
    assert len(svc.lister(sens="CREDIT", db_path=tmp_db)) == 1


def test_05_charger_detail(source, tmp_db):
    d = svc.charger(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert d is not None and d["categorie_moteur"] == ""
    assert d["historique"] == []


def test_06_mouvement_introuvable(source):
    assert svc.charger("MVT-INEXISTANT") is None


def test_07_previsualiser_categoriser(source):
    r = svc.previsualiser(_opq("MVT-TEST-001"), "CATEGORISER", nouvelle_categorie="LOGICIEL_GESTION")
    assert r["ok"] and r["apercu"]["nouvelle_categorie"] == "LOGICIEL_GESTION"


def test_08_categorie_invalide_refusee(source):
    r = svc.previsualiser(_opq("MVT-TEST-001"), "CATEGORISER", nouvelle_categorie="INVENTEE")
    assert not r["ok"] and r["code"] == svc.E_CATEGORIE_INCONNUE


def test_09_categorie_manquante_refusee(source):
    r = svc.previsualiser(_opq("MVT-TEST-001"), "CATEGORISER")
    assert not r["ok"] and r["code"] == svc.E_CATEGORIE_MANQUANTE


def test_10_type_decision_invalide(source):
    r = svc.previsualiser(_opq("MVT-TEST-001"), "TYPE_INVENTE")
    assert not r["ok"] and r["code"] == svc.E_TYPE_DECISION_INCONNU


def test_11_decider_maintenir_a_controler(source, tmp_db):
    r = svc.decider(_opq("MVT-TEST-001"), "MAINTENIR_A_CONTROLER", db_path=tmp_db)
    assert r["ok"]
    d = svc.charger(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert d["statut_humain"] == "MAINTENIR_A_CONTROLER"


def test_12_decider_non_classe(source, tmp_db):
    r = svc.decider(_opq("MVT-TEST-001"), "NON_CLASSE", db_path=tmp_db)
    assert r["ok"]
    assert svc.charger(_opq("MVT-TEST-001"), db_path=tmp_db)["statut_humain"] == "NON_CLASSE"


def test_13_decider_reporter(source, tmp_db):
    r = svc.decider(_opq("MVT-TEST-001"), "REPORTER", db_path=tmp_db)
    assert r["ok"]


def test_14_decider_avec_justification(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", justification="à vérifier avec le relevé suivant",
               db_path=tmp_db)
    h = svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert h[0]["justification"] == "à vérifier avec le relevé suivant"


def test_15_signaler_anomalie_moteur(source, tmp_db):
    r = svc.decider(_opq("MVT-TEST-001"), "REPORTER", anomalie_moteur="libellé tronqué", db_path=tmp_db)
    assert r["ok"]
    assert svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)[0]["anomalie_moteur"] == "libellé tronqué"


def test_16_proposer_future_regle(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", future_regle="détecter PAIEMENT CB + montant < 10€",
               db_path=tmp_db)
    assert svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)[0]["future_regle"]


def test_17_historique_append_only(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", db_path=tmp_db)
    svc.decider(_opq("MVT-TEST-001"), "NON_CLASSE", db_path=tmp_db)
    h = svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert len(h) == 2   # les deux décisions restent, rien supprimé


def test_18_seconde_decision_devient_active(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", db_path=tmp_db)
    svc.decider(_opq("MVT-TEST-001"), "NON_CLASSE", db_path=tmp_db)
    d = svc.charger(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert d["statut_humain"] == "NON_CLASSE"   # la plus récente fait foi
    h = svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)
    actifs = [x for x in h if x["actif"] == 1]
    assert len(actifs) == 1 and actifs[0]["type_decision"] == "NON_CLASSE"


def test_19_aucune_ia_externe_aucun_reseau(source):
    import inspect
    src = inspect.getsource(svc)
    for interdit in ("requests.", "httpx.", "openai", "anthropic", "urllib.request"):
        assert interdit not in src


def test_20_aucune_pii_dans_les_decisions(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "CATEGORISER", nouvelle_categorie="LOGICIEL_GESTION",
               db_path=tmp_db)
    h = svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert "iban" not in str(h).lower() and "compte_id" not in str(h)


def test_21_aucun_libelle_complet_dans_la_liste(source, tmp_db):
    lignes = svc.lister(db_path=tmp_db)
    assert all("PAIEMENT CB TEST" != l.get("libelle_brut") for l in lignes)  # champ jamais exposé


def test_22_le_brut_bancaire_nest_jamais_reecrit(source, tmp_db):
    """Une décision de classement ne touche pas ce que la banque a envoyé.

    Il n'y a plus de fichier dont vérifier la date : la garantie porte désormais sur la table du
    brut, qui est ce qu'il fallait protéger depuis le début.
    """
    from app.db.connection import get_db

    def _brut():
        conn = get_db(source)
        try:
            return conn.execute(
                "SELECT mouvement_id_opaque, montant, sens, libelle_brut, fingerprint "
                "FROM banque_mouvements ORDER BY mouvement_id_opaque").fetchall()
        finally:
            conn.close()

    avant = _brut()
    svc.decider(_opq("MVT-TEST-001"), "NON_CLASSE", db_path=tmp_db)
    assert _brut() == avant


def test_23_idempotence_meme_decision_repetee(source, tmp_db):
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", db_path=tmp_db)
    svc.decider(_opq("MVT-TEST-001"), "REPORTER", db_path=tmp_db)
    h = svc.historique(_opq("MVT-TEST-001"), db_path=tmp_db)
    assert len(h) == 2   # deux événements distincts, pas une fusion — mais un seul actif
    assert sum(1 for x in h if x["actif"] == 1) == 1

"""APP-5B — Validation POSITIVE du runner, 100% SQLite (fermeture de Lot11).

Prouve que le chemin SÛR fonctionne réellement : les contrôles sont RECALCULÉS (le vrai moteur
`controles_lot11_service`, jamais une fonction factice), le contrôle bancaire est présent avant /
absent après classification simulée, et AUCUNE donnée réelle n'est touchée.

CE QUI A CHANGÉ
Le runner ne fabrique plus de mini-projet Excel ni ne lance de sous-processus : Lot11 étant SQLite
natif, il recalcule sur une COPIE de la base. Ce fichier teste donc la même propriété métier
(« une décision humaine fait-elle disparaître le contrôle ? ») sans aucun classeur — la question
reste posée au moteur de contrôle, pas déduite d'un état applicatif.
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import banques_controle_service as bq
from app.services import controles_runner_service as runner

REEL = Path(r"C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie")
MOIS, MOIS2 = "2099-01", "2099-02"


def _sha(p):
    p = Path(p)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else "ABSENT"


def _etat_sqlite(db_path):
    """État bancaire + clôture nécessaire pour que le contrôle bancaire se déclenche.

    MVT-TEST-A porte le contrôle qu'on cherche à faire disparaître ; MVT-TEST-B est déjà classé ;
    MVT-TEST-C est sur un autre mois, et doit rester intact quoi qu'il arrive au premier.
    """
    import fixtures_banque as fx
    from app.services import banque_classification_service as cls

    fx.construire(db_path, mouvements=[
        fx.mouvement("MVT-TEST-A", MOIS + "-15", "TEST MVT-TEST-A", 100.0, "CREDIT",
                     statut_controle="A_CONTROLER",
                     statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
        fx.mouvement("MVT-TEST-B", MOIS + "-15", "TEST MVT-TEST-B", 5.0, "DEBIT",
                     categorie="FRAIS_BANCAIRES"),
        fx.mouvement("MVT-TEST-C", MOIS2 + "-15", "TEST MVT-TEST-C", 33.0, "DEBIT",
                     statut_controle="A_CONTROLER",
                     statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
    ])
    conn = get_db(db_path)
    try:
        for mois in (MOIS, MOIS2):
            conn.execute(
                "INSERT OR REPLACE INTO ref_cloture_mensuelle (mois, statut_mois, "
                "date_passage_controle, date_cloture, nb_lignes_bancaires_non_classees, "
                "nb_controles_bloquants_ouverts, commentaire, import_id) "
                "VALUES (?, 'OUVERT', '', '', '', '', '', 'IMP-TEST')", (mois,))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def mini(tmp_path, tmp_db, monkeypatch):
    """État SQLite factice + workspace runner isolé hors de l'arbre réel."""
    faux_reel = tmp_path / "faux_reel"
    faux_reel.mkdir()
    _etat_sqlite(tmp_db)
    monkeypatch.setattr(cfg, "PROJECT_ROOT", faux_reel)
    monkeypatch.setattr(cfg, "CONTROLES_RUNNER_WORKSPACE", tmp_path / "runner_ws")
    from app.readers import banques_reader as reader
    reader.vider_cache()
    bq.vider_cache()
    return tmp_db


def _element():
    return {"ctrl_opaque": "CTRL-TESTBANK", "module": "BANQUE",
            "code": "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE", "mois": MOIS,
            "entite_id": bq.id_opaque("MVT-TEST-A")}


# ── Chemin sûr fonctionnel ──────────────────────────────────────────────────

def test_01_classification_resout_le_controle(mini, tmp_db):
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    etapes = {e["etape"]: e for e in res["etapes"]}
    assert res["statut"] == "SUCCES", res.get("motif")
    # Les deux recalculs ont réellement eu lieu, sur la copie.
    assert etapes["CONTROLES_BASELINE"]["ok"] is True
    assert etapes["CONTROLES_APRES"]["ok"] is True
    assert etapes["CLASSIFICATION_SIMULEE"]["mouvement_classe"] is True
    # Contrôle présent avant, absent après → verdict correct.
    assert res["n_avant"] == 1 and res["n_apres"] == 0 and res["verdict"] == "RESOLU_MOTEUR"
    assert res["reel_intact"] is True


def test_02_sans_classification_controle_maintenu(mini, tmp_db):
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=False, db_path=tmp_db)
    assert res["verdict"] == "TOUJOURS_PRESENT" and res["n_avant"] == 1 and res["n_apres"] == 1


def test_03_base_du_run_jamais_modifiee(mini, tmp_db):
    """La classification simulée n'est écrite que dans la copie : l'état métier réel reste intact."""
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    conn = get_db(tmp_db)
    try:
        # La clé stockée est l'identifiant métier ; `id_opaque` n'est que sa forme exposée à l'UI.
        statut = conn.execute(
            "SELECT statut_classification FROM banque_classifications "
            "WHERE mouvement_id_opaque = 'MVT-TEST-A' ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    assert statut is not None and statut[0] == "RAPPROCHEMENT_REQUIS"
    assert res["reel_intact"] is True


def test_04_autre_mois_non_affecte(mini, tmp_db):
    """Le contrôle de février existe indépendamment de la décision prise sur janvier."""
    res = runner.recalculer_sur_copie(
        {**_element(), "mois": MOIS2}, appliquer_classification=False, db_path=tmp_db)
    assert res["n_avant"] == 1 and res["n_apres"] == 1
    assert res["verdict"] == "TOUJOURS_PRESENT"


def test_05_aucun_chemin_reel_dans_les_sorties(mini, tmp_db):
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    reel = str(REEL).lower()
    hors = [c for c in res["chemins"] if reel in c.lower()]
    assert hors == [] and len(res["chemins"]) > 0
    ws = str(cfg.CONTROLES_RUNNER_WORKSPACE).lower()
    assert all(ws in c.lower() for c in res["chemins"])


def test_06_workspace_nettoye(mini, tmp_db):
    runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    ws_root = Path(cfg.CONTROLES_RUNNER_WORKSPACE)
    restes = list(ws_root.glob("*")) if ws_root.exists() else []
    assert restes == []


# ── Échec lisible, aucun 500 ────────────────────────────────────────────────

def test_07_echec_lisible(mini, tmp_db, monkeypatch):
    """Un recalcul qui échoue devient un run ÉCHEC lisible, jamais une exception."""
    from app.services import controles_lot11_service as lot11

    monkeypatch.setattr(lot11, "construire",
                        lambda **kw: {"ok": False, "message": "panne simulée"})
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    assert res["statut"] == "ECHEC" and res["verdict"] == "ERREUR_MOTEUR"
    assert res.get("motif")


def test_08_module_hors_perimetre_bloque(mini, tmp_db):
    res = runner.recalculer_sur_copie({**_element(), "module": "MENAGES"},
                                      appliquer_classification=True, db_path=tmp_db)
    assert res["statut"] == "BLOQUE" and res["verdict"] == "NON_COMPARABLE"


# ── Garde d'isolation ───────────────────────────────────────────────────────

def test_09_workspace_dans_reel_refuse(mini, monkeypatch, tmp_db):
    monkeypatch.setattr(cfg, "CONTROLES_RUNNER_WORKSPACE", Path(cfg.PROJECT_ROOT) / "interne_ws")
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    assert res["statut"] == "BLOQUE"
    assert res["reel_intact"] is True

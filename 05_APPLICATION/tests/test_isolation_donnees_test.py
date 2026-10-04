"""Pytest est hermétique : toute donnée produite par la suite vit dans un dossier temporaire.

L'INCIDENT
`app.config` dérive à l'import tous ses dossiers de données de `APP_DATA_DIR`. La suite n'en posait
pas : base, dry-runs, sauvegardes… retombaient sur `05_APPLICATION/data/` (BASE_DEV_OBSOLETE), et
une suite complète y laissait des centaines de dossiers de dry-run. Le garde-fou des tests ne les
voyait pas : `pathlib` écrit par `io.open` et `os.mkdir`, pas par `builtins.open`.

CE QUI EST VERROUILLÉ ICI
`tests/conftest.py` pose `PILOTAGE_ENVIRONNEMENT=TEST` et un `APP_DATA_DIR` de session (mkdtemp)
AVANT tout import de l'application ; la garde couvre `io.open` et `os.mkdir`.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import app.config as cfg
import conftest
from app import environnement as envmod

APP_ROOT = Path(__file__).resolve().parents[1]
DEV_DATA = APP_ROOT / "data"


def _sous(chemin, racine) -> bool:
    chemin, racine = Path(chemin).resolve(), Path(racine).resolve()
    return chemin == racine or racine in chemin.parents


def _contenu(dossier: Path) -> set[str]:
    if not dossier.exists():
        return set()
    return {str(p.relative_to(dossier)) for p in dossier.rglob("*")}


@pytest.fixture()
def dev_inchange():
    """Photographie `05_APPLICATION/data/` avant le test et la compare après."""
    avant = {p: (p.stat().st_size, p.stat().st_mtime_ns)
             for p in DEV_DATA.rglob("*")} if DEV_DATA.exists() else {}
    yield
    apres = {p: (p.stat().st_size, p.stat().st_mtime_ns)
             for p in DEV_DATA.rglob("*")} if DEV_DATA.exists() else {}
    assert apres == avant, "05_APPLICATION/data/ a changé pendant le test"


# 1-5. Environnement et dossiers de la session

def test_01_environnement_test():
    assert os.environ["PILOTAGE_ENVIRONNEMENT"] == "TEST"
    assert envmod.environnement() == envmod.TEST


def test_02_05_tous_les_dossiers_de_donnees_sont_dans_la_session():
    session = conftest.SESSION_DIR
    assert _sous(session, tempfile.gettempdir()) and session.name.startswith("pilotage_pytest_")
    assert Path(os.environ["APP_DATA_DIR"]) == session / "data"
    for nom in ("DATA_DIR", "DB_PATH", "DRYRUNS_DIR", "BACKUPS_DIR", "SNAPSHOTS_DIR",
                "RESTORE_DIR", "JUSTIFICATIFS_ROOT", "ARCHIVAGE_PIECES_DIR",
                "MENAGES_RECALC_WORKSPACE", "MENAGES_CHAINE_WORKSPACE",
                "BANQUE_CONTROLE_WORKSPACE", "CONTROLES_RUNNER_WORKSPACE"):
        valeur = getattr(cfg, nom)
        # JUSTIFICATIFS_ROOT est redirigé plus finement encore, test par test, vers `tmp_path`
        # (fixture autouse `_justificatifs_isoles`) : un dossier temporaire dans les deux cas.
        attendu = tempfile.gettempdir() if nom == "JUSTIFICATIFS_ROOT" else session
        assert _sous(valeur, attendu), f"{nom} hors du temporaire : {valeur}"
        assert not _sous(valeur, DEV_DATA), f"{nom} sous la base dev obsolète"


# 14 (mission). Test d'import : la configuration voit le dossier temporaire DÈS l'import

def test_import_config_voit_le_dossier_temporaire_des_l_import():
    """Session pytest NEUVE, shell sans APP_DATA_DIR : le premier `import app.config` (fait par
    la sonde) lit déjà le dossier de session — jamais `05_APPLICATION/data`."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("APP_DATA_DIR", "PILOTAGE_ENVIRONNEMENT", "PILOTAGE_IGNORE_ENV_FILE")}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-s", "-p", "no:cacheprovider",
         str(APP_ROOT / "tests" / "test_isolation_donnees_test.py::test_sonde_session")],
        cwd=str(APP_ROOT), env=env, capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stdout[-1500:] + proc.stderr[-1500:]
    ligne = next(l for l in proc.stdout.splitlines() if l.startswith("SONDE"))
    data_dir = Path(ligne.split()[1])
    assert data_dir.name == "data" and data_dir.parent.name.startswith("pilotage_pytest_")
    assert not _sous(data_dir, DEV_DATA)
    # 15. Fin de session : le dossier temporaire a été supprimé.
    assert not data_dir.parent.exists(), "la session temporaire doit être nettoyée"


def test_sonde_session():
    """Sonde lancée par le test précédent dans une session pytest neuve (sans effet ici)."""
    print("SONDE", cfg.DATA_DIR, "|", cfg.DRYRUNS_DIR)


# 6. Dry-runs : 100 dry-runs, 100 dossiers dans la session, rien sous la base dev

def test_06_cent_dry_runs_restent_dans_la_session(tmp_db, monkeypatch, dev_inchange):
    from app.services import banques_import_service as svc

    monkeypatch.setattr(cfg, "MASTER_BANQUE", Path(tempfile.gettempdir()) / "ABSENT.xlsx")
    csv = ("Date operation;Libelle;Debit;Credit\n"
           "05/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n").encode("utf-8")
    racine = Path(cfg.DRYRUNS_DIR) / "banque_import"
    avant = _contenu(racine)
    tokens = set()
    for _ in range(100):
        res = svc.previsualiser(csv, "releve.csv", "CM_TEST")
        assert res["ok"], res
        tokens.add(res["token"])
    nouveaux = {n for n in _contenu(racine) - avant if "/" not in n and "\\" not in n}
    assert nouveaux == tokens and len(tokens) == 100
    assert _sous(racine, conftest.SESSION_DIR)


# 7, 12. Sauvegarde et migrations sur la base de SESSION (aucune fixture de redirection)

def test_07_12_migrations_et_sauvegarde_dans_la_session(dev_inchange):
    from app.db.connection import apply_migrations, get_db
    from app.services import backup_service

    assert _sous(cfg.DB_PATH, conftest.SESSION_DIR)
    apply_migrations(cfg.DB_PATH)
    conn = get_db()
    try:
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] > 0
    finally:
        conn.close()
    res = backup_service.sauvegarder("TEST_ISOLATION")
    assert res["ok"], res
    copies = list(Path(cfg.BACKUPS_DIR).rglob("*.db"))
    assert copies and all(_sous(c, conftest.SESSION_DIR) for c in copies)


# 8. Export : rien sous la base dev

def test_08_export_n_ecrit_rien_sous_la_base_dev(tmp_db, tmp_path, dev_inchange):
    from app.services import lot13_export_service as lot13

    assert lot13.produire(db_path=tmp_db)["ok"]
    assert lot13.exporter(db_path=tmp_db, destination=tmp_path / "exports")["ok"]


# 9-10. Base et dossier réels toujours refusés

def test_09_10_reel_toujours_refuse(tmp_path, monkeypatch):
    reel = tmp_path / "reel"
    reel.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text(f"APP_DATA_DIR={reel.as_posix()}\n", encoding="utf-8")
    monkeypatch.setattr(cfg, "ENV_FILE", env_file)
    envmod._data_dir_reel_depuis.cache_clear()
    try:
        from app.db.connection import get_db
        with pytest.raises(envmod.BaseReelleInterdite):
            get_db(reel / "app.db")
    finally:
        envmod._data_dir_reel_depuis.cache_clear()


def test_10_dossier_reel_de_la_machine_refuse_par_la_garde():
    reel = envmod.data_dir_reel()
    if reel is None:
        pytest.skip("aucun APP_DATA_DIR réel déclaré sur cette machine")
    with pytest.raises(AssertionError, match="REEL"):
        os.mkdir(reel / "pilotage_pytest_sonde")
    assert not (reel / "pilotage_pytest_sonde").exists()


# 11, 13. Base de test fonctionnelle, aucune fuite d'un test à l'autre

_PREMIERE: list[Path] = []


def test_11_13_a_premiere_base(tmp_db):
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    conn.execute("CREATE TABLE IF NOT EXISTS sonde_isolation (v TEXT)")
    conn.execute("INSERT INTO sonde_isolation VALUES ('premier')")
    conn.commit()
    conn.close()
    _PREMIERE.append(tmp_db)


def test_11_13_b_seconde_base_vierge(tmp_db):
    from app.db.connection import get_db

    assert not _PREMIERE or _PREMIERE[0] != tmp_db
    conn = get_db(tmp_db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='sonde_isolation'"
                            ).fetchone()[0] == 0
    finally:
        conn.close()


# 14. Deux sessions parallèles : deux dossiers distincts

def test_14_sessions_paralleles_sans_collision():
    env = {k: v for k, v in os.environ.items()
           if k not in ("APP_DATA_DIR", "PILOTAGE_ENVIRONNEMENT", "PILOTAGE_IGNORE_ENV_FILE")}
    commande = [sys.executable, "-m", "pytest", "-q", "-s", "-p", "no:cacheprovider",
                str(APP_ROOT / "tests" / "test_isolation_donnees_test.py::test_sonde_session")]
    procs = [subprocess.Popen(commande, cwd=str(APP_ROOT), env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) for _ in range(2)]
    sorties = [p.communicate(timeout=300)[0] for p in procs]
    assert all(p.returncode == 0 for p in procs)
    dossiers = {next(l for l in s.splitlines() if l.startswith("SONDE")).split()[1]
                for s in sorties}
    assert len(dossiers) == 2, "chaque session a son propre APP_DATA_DIR"

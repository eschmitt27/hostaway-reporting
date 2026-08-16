"""Les snapshots doivent respecter APP_DATA_DIR — jamais écrire dans le dossier de données réel.

Bug constaté : `snapshot_service` faisait `from app.config import SNAPSHOTS_DIR`, ce qui FIGE le
chemin au moment de l'import du module. Rediriger `APP_DATA_DIR` — ou monkeypatcher `cfg` — n'avait
alors plus aucun effet, et des snapshots de test se sont écrits dans le vrai
`05_APPLICATION/data/snapshots/`. C'est exactement l'anti-pattern déjà documenté pour
`cfg.DB_PATH` : les chemins de configuration se lisent À L'APPEL, jamais à l'import.

Ces tests verrouillent la règle pour les snapshots et pour les restaurations.
"""
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import snapshot_service as snap


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "snap.db"
    apply_migrations(p)
    return p


def test_snapshot_suit_la_configuration_redirigee(tmp_path, monkeypatch, db):
    """Rediriger la configuration doit suffire — sans patcher le module lui-même."""
    cible = tmp_path / "snapshots_isoles"
    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", cible)

    source = tmp_path / "fichier.txt"
    source.write_text("contenu", encoding="utf-8")

    res = snap.create_snapshot("TEST_ISOLATION", [source], db_path=db)

    assert Path(res["path"]).is_relative_to(cible), (
        f"snapshot écrit hors de la configuration redirigée : {res['path']}")
    assert res["files"] == 1


def test_snapshot_n_ecrit_jamais_dans_le_dossier_reel(tmp_path, monkeypatch, db):
    """Garde-fou explicite : le vrai dossier de données ne doit pas grossir pendant un test."""
    reel = Path(cfg.APP_ROOT) / "data" / "snapshots"
    avant = {p.name for p in reel.iterdir()} if reel.exists() else set()

    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", tmp_path / "ailleurs")
    source = tmp_path / "fichier.txt"
    source.write_text("contenu", encoding="utf-8")
    snap.create_snapshot("TEST_ISOLATION", [source], db_path=db)

    apres = {p.name for p in reel.iterdir()} if reel.exists() else set()
    assert apres == avant, f"snapshots créés dans le dossier réel : {sorted(apres - avant)}"


def test_restauration_suit_la_configuration_redirigee(tmp_path, monkeypatch, db):
    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(cfg, "RESTORE_DIR", tmp_path / "restaurations")

    source = tmp_path / "fichier.txt"
    source.write_text("contenu", encoding="utf-8")
    res = snap.create_snapshot("TEST_ISOLATION", [source], db_path=db)

    restore = snap.restore_to_workspace(res["id"], db_path=db)
    assert restore["ok"] is True
    assert Path(restore["workspace"]).is_relative_to(tmp_path / "restaurations")


def test_le_module_ne_fige_aucun_chemin_a_l_import():
    """Garde-fou : importer une constante de chemin depuis la config la gèle définitivement.

    On inspecte les seules lignes d'import — pas le texte entier, sinon un commentaire qui
    *explique* le problème déclencherait l'alerte.
    """
    lignes = Path(snap.__file__).read_text(encoding="utf-8").splitlines()
    imports = [l.strip() for l in lignes
               if l.startswith(("from app.config import", "from app import config import"))]
    figes = [l for l in imports if "DIR" in l or "PATH" in l]
    assert figes == [], f"Chemins figés à l'import : {figes}"

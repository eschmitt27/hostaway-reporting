"""Non-régression APP-2c/APP-2e : garde chemin migration sur copies.

Vérifie que :
- _assert_copy_path rejette les chemins sous APP_ROOT hors dryruns ;
- _assert_copy_path accepte les chemins externes (tmp_path pytest) ;
- _assert_copy_path accepte les chemins sous dryruns même sous APP_ROOT ;
- _assert_copy_path rejette les composants FORBIDDEN (01_SOURCES_BRUTES, MASTER, 03_EXPORTS) ;
- tmp_path fourni par pytest est bien hors APP_ROOT (détecte basetemp mal configuré).
"""
import pytest
from pathlib import Path

import app.config as cfg
from app.services.saisie_hh_schema_migration import _assert_copy_path


def test_guard_rejette_chemin_sous_app_root():
    """Tout chemin sous APP_ROOT hors dryruns est refusé."""
    chemin = cfg.APP_ROOT / "some_subdir" / "copy.xlsx"
    with pytest.raises(RuntimeError, match="Migration refusee"):
        _assert_copy_path(chemin)


def test_guard_accepte_chemin_externe(tmp_path):
    """Chemin dans tmp_path (hors APP_ROOT) est accepté."""
    fake = tmp_path / "copy_saisie.xlsx"
    fake.write_bytes(b"PK")  # contenu minimal
    result = _assert_copy_path(fake)
    assert result == fake.resolve()


def test_guard_accepte_chemin_dryruns():
    """Chemin sous DATA_DIR/dryruns est accepté même s'il est sous APP_ROOT."""
    dryruns_root = (cfg.DATA_DIR / "dryruns").resolve()
    dryruns_root.mkdir(parents=True, exist_ok=True)
    fake = dryruns_root / "test_guard_copy.xlsx"
    fake.write_bytes(b"PK")
    try:
        result = _assert_copy_path(fake)
        assert result == fake
    finally:
        fake.unlink(missing_ok=True)


def test_guard_rejette_composant_sources_brutes(tmp_path):
    """Chemin contenant 01_SOURCES_BRUTES est refusé quelle que soit la racine."""
    fake_path = tmp_path / "01_SOURCES_BRUTES" / "copy.xlsx"
    fake_path.parent.mkdir(parents=True, exist_ok=True)
    fake_path.write_bytes(b"PK")
    with pytest.raises(RuntimeError, match="Migration refusee"):
        _assert_copy_path(fake_path)


def test_tmp_path_hors_app_root(tmp_path):
    """tmp_path pytest doit être hors APP_ROOT.

    Si ce test échoue, relancer avec --basetemp hors du dépôt :
        --basetemp="$env:TEMP\\pytest_app"
    """
    app_root = cfg.APP_ROOT.resolve()
    resolved = tmp_path.resolve()
    assert app_root not in resolved.parents and resolved != app_root, (
        f"\ntmp_path {tmp_path} est sous APP_ROOT {app_root}.\n"
        f"Cela déclencherait les gardes APP-2c/APP-2e.\n"
        f"Relancer avec --basetemp hors du dépôt :\n"
        f'  --basetemp="$env:TEMP\\pytest_app"'
    )

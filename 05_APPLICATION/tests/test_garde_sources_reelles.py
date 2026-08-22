"""La garde anti-écriture dans les sources réelles fonctionne réellement (mission §21-24).

Une garde qu'on n'attaque jamais est une garde dont on ignore si elle protège. Ces tests TENTENT
l'écriture interdite et vérifient qu'elle échoue — et vérifient aussi qu'une écriture légitime
(`tmp_path`) reste possible, sans quoi la garde rendrait la suite inutilisable.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
import garde_sources_reelles as garde


def _chemin_reel(*parties) -> Path:
    return Path(cfg.PROJECT_ROOT).joinpath(*parties)


# ── Périmètre : ce qui est protégé, ce qui ne l'est pas ─────────────────────

def test_racines_protegees_initialisees():
    assert garde._RACINES_PROTEGEES, "la garde n'a pas été initialisée par pytest_configure"


@pytest.mark.parametrize("parties", [
    ("02_TRAVAIL", "Lot1_Hostaway", "MASTER_FACT_HA_Reservations.xlsx"),
    ("02_TRAVAIL", "Lot11_Controles", "MASTER_CTRL_Coherence.xlsx"),
    ("01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm"),
    ("03_EXPORTS", "PowerBI", "PBI_Flux.csv"),
    ("02_DONNEES_NORMALISEES", "menages", "M04_MENAGES_PowerQuery.xlsx"),
])
def test_sources_reelles_sont_protegees(parties):
    assert garde.est_protege(_chemin_reel(*parties)), parties


def test_base_reelle_est_protegee():
    assert garde.est_protege(Path(cfg.APP_ROOT) / "data" / "app.db")


def test_tmp_nest_pas_protege(tmp_path):
    assert not garde.est_protege(tmp_path / "libre.db")


# ── Attaques : l'écriture interdite doit échouer ────────────────────────────

def test_open_en_ecriture_refuse():
    cible = _chemin_reel("02_TRAVAIL", "Lot11_Controles", "MASTER_CTRL_Coherence.xlsx")
    with pytest.raises(AssertionError, match="INTERDITE"):
        open(cible, "w").close()


def test_open_en_append_refuse():
    with pytest.raises(AssertionError, match="INTERDITE"):
        open(_chemin_reel("01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm"), "a").close()


def test_openpyxl_save_refuse():
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    with pytest.raises(AssertionError, match="CLASSEUR"):
        wb.save(str(_chemin_reel("02_TRAVAIL", "Lot9_FluxUnifie", "MASTER_CALC_Flux.xlsx")))


def test_shutil_copy_vers_source_reelle_refuse(tmp_path):
    import shutil

    source = tmp_path / "source.txt"
    source.write_text("x", encoding="utf-8")
    with pytest.raises(AssertionError, match="COPIE"):
        shutil.copy(source, _chemin_reel("02_TRAVAIL", "copie_interdite.txt"))


def test_sqlite_connect_sur_base_reelle_lecture_seule():
    """La connexion à la base réelle est permise (lecture) ; toute écriture y échoue."""
    import sqlite3

    reel = Path(cfg.APP_ROOT) / "data" / "app.db"
    if not reel.exists():
        pytest.skip("app.db réelle absente de cet environnement")
    conn = sqlite3.connect(str(reel))
    try:
        conn.execute("SELECT 1").fetchone()  # la lecture réussit
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("CREATE TABLE IF NOT EXISTS garde_essai_ecriture (v TEXT)")
    finally:
        conn.close()


# ── La garde ne gêne pas un test correctement isolé ─────────────────────────

def test_ecriture_dans_tmp_path_autorisee(tmp_path):
    cible = tmp_path / "fichier.txt"
    cible.write_text("contenu", encoding="utf-8")
    assert cible.read_text(encoding="utf-8") == "contenu"


def test_base_temporaire_autorisee(tmp_db):
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS essai (v TEXT)")
        conn.execute("INSERT INTO essai VALUES ('ok')")
        conn.commit()
        assert conn.execute("SELECT v FROM essai").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_lecture_source_reelle_toujours_permise():
    """La garde interdit l'écriture, pas la lecture : la parité en dépend."""
    cible = _chemin_reel("01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
    if not cible.exists():
        pytest.skip("REF_Setup.xlsm absent de cet environnement")
    with open(cible, "rb") as f:
        assert f.read(2) == b"PK"        # un .xlsm est une archive ZIP

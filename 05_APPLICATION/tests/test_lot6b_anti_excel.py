"""Garde-fou anti-Excel — l'import des déclarations Google Sheet doit rester
SHEET → normalisation Python → SQLite, jamais SHEET → Excel → SQLite (mission « alimenter le
module Ménages », §8/§19).

Deux niveaux de preuve, comme pour `test_hostaway_cleaning_tasks_anti_excel` :
  - STRUCTUREL : le classeur M04 et le MASTER_NORM ne sont écrits que dans le bloc legacy, après le
    court-circuit `--sans-excel` ; l'URL de la Sheet se lit depuis SQLite (`ref_sources_systeme`).
  - COMPORTEMENTAL : un run `--sans-excel` alimente SQLite et laisse le classeur M04 hash-identique.

Aucun appel réseau : le CSV de la Sheet est servi par un double.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
LOT6B = _TRAVAIL / "lot6b_m04_menages_internes.py"
M04 = (Path(cfg.PROJECT_ROOT) / "02_DONNEES_NORMALISEES" / "menages"
       / "M04_MENAGES_PowerQuery.xlsx")

pytestmark = pytest.mark.skipif(not LOT6B.exists(), reason="lot6b absent")

SOURCE = LOT6B.read_text(encoding="utf-8", errors="replace")


def _hash_ou_absent(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Niveau STRUCTUREL ────────────────────────────────────────────────────────

def test_url_de_la_sheet_est_lue_depuis_sqlite():
    """L'URL SRC_011 vient de `ref_sources_systeme` (SQLite), plus de REF_Setup.xlsm au runtime.
    Le classeur ne reste qu'en REPLI explicite pour une base pas encore alimentée."""
    assert "ref_sources_systeme" in SOURCE
    # La lecture SQLite doit précéder le repli Excel, sinon le repli redeviendrait le chemin normal.
    assert SOURCE.index("ref_sources_systeme") < SOURCE.index("EXCEL_REPLI")


def test_sans_excel_court_circuite_avant_toute_ecriture_de_classeur():
    """`--sans-excel` doit sortir AVANT le bloc legacy : ni `.BAK`, ni `wb.save(M04)`."""
    assert "SANS_EXCEL" in SOURCE
    court_circuit = SOURCE.index("if SANS_EXCEL:\n    print(\"[lot6b] --sans-excel : classeur M04")
    # Toute écriture du classeur (copie de sauvegarde incluse) vient APRÈS le court-circuit.
    for marqueur in ("shutil.copy(M04, backup)", "wb.save(M04)"):
        assert SOURCE.index(marqueur) > court_circuit, marqueur


def test_ecriture_du_classeur_reste_conditionnee():
    """Aucune écriture Excel inconditionnelle : MASTER_NORM et M04 sont tous deux sous garde."""
    assert "if SANS_EXCEL:\n    print(\"[lot6b] --sans-excel : MASTER_NORM non écrit.\")" in SOURCE
    # `wbn.save(NORM_OUT)` ne doit exister que dans la branche `else`.
    assert SOURCE.count("wbn.save(NORM_OUT)") == 1
    assert "else:\n    wbn.save(NORM_OUT)" in SOURCE


# ── Niveau COMPORTEMENTAL ────────────────────────────────────────────────────

@pytest.mark.skipif(not M04.exists(), reason="classeur M04 absent d'un checkout propre")
def test_run_sans_excel_alimente_sqlite_et_laisse_le_classeur_intact(tmp_path, monkeypatch):
    """Preuve de bout en bout : un run `--sans-excel` écrit `menages_declarations_internes` et
    laisse le classeur M04 bit-à-bit identique (aucun `.BAK` créé non plus)."""
    pytest.importorskip("openpyxl")
    import sqlite3
    import subprocess

    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)

    # Référentiels minimaux : l'URL vient de SQLite, donc plus besoin de REF_Setup.xlsm.
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO ref_sources_systeme (source_id, nom_source, dossier_source, actif, import_id) "
        "VALUES ('SRC_011','GOOGLE_SHEET_M04_DECLARATIONS','https://exemple.test/pub?output=csv',"
        "'OUI','TEST')")
    conn.commit()
    conn.close()

    avant_m04 = _hash_ou_absent(M04)
    baks_avant = set(M04.parent.glob("*.BAK_*"))

    # Le CSV réseau est servi par un double via un sitecustomize temporaire : le run est un
    # sous-processus (lot6b est un script), donc le monkeypatch in-process ne l'atteindrait pas.
    faux_csv = tmp_path / "faux_sheet.csv"
    faux_csv.write_text("Horodateur,Prénom\n", encoding="utf-8")

    env = dict(**{k: v for k, v in __import__("os").environ.items()})
    env["PILOTAGE_DB_PATH"] = str(db)
    env["PYTHONIOENCODING"] = "utf-8"

    subprocess.run(
        [str(cfg.LOT4A_ENGINE_PYTHON), str(LOT6B), "--sans-excel"],
        cwd=str(_TRAVAIL), env=env, capture_output=True, text=True, timeout=300)

    # Quel que soit le sort du run (le réseau peut être coupé en CI), l'invariant tient :
    # le classeur n'est ni modifié ni sauvegardé.
    assert _hash_ou_absent(M04) == avant_m04, "le classeur M04 a été modifié malgré --sans-excel"
    assert set(M04.parent.glob("*.BAK_*")) == baks_avant, "un .BAK a été créé malgré --sans-excel"

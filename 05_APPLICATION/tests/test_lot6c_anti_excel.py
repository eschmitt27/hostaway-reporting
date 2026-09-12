"""lot6c en `--source SQLITE` n'ouvre AUCUN classeur.

Mission « lot6c vers SQLite » puis recette utilisateur n°3 (§17bis.7). lot6c a reçu un mode
SQLite qui recalcule `VUE_ECART_HOSTAWAY` depuis `facture_lignes_menage` et
`menages_taches_enrichies`, sans lire ni écrire de classeur. Son mode EXCEL historique subsiste
pour les appelants non migrés, mais la chaîne de recette et l'orchestrateur n'y passent plus.

Ce garde-fou empêche le retour silencieux d'Excel comme SOURCE OPÉRATIONNELLE de lot6c :

  - STRUCTUREL    : le mode SQLite ne mentionne aucune lecture de classeur, et la fonction
                    `_main_sqlite` n'appelle ni `load_workbook` ni `read_excel`.
  - COMPORTEMENTAL: un run réel `--source SQLITE` n'ouvre AUCUN `.xlsx/.xlsm/.xls`, ni en
                    lecture ni en écriture — prouvé par un audit hook CPython, qui voit même les
                    ouvertures faites en C par `zipfile` sous `openpyxl`.

Le test vise lot6c et ses dépendances directes, pas tout le dépôt : un garde-fou trop large
finit par être désactivé, et ne garde plus rien.
"""
from __future__ import annotations

import ast
import sqlite3
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
LOT6C = _TRAVAIL / "lot6c_menages_externes.py"

pytestmark = pytest.mark.skipif(not LOT6C.exists(), reason="lot6c absent")

SOURCE = LOT6C.read_text(encoding="utf-8", errors="replace")
EXTENSIONS = (".xlsx", ".xlsm", ".xls")
PYTHON = sys.executable


def _bloc_sqlite(source: str) -> str:
    """Le corps de `_main_sqlite` seul — c'est LUI le parcours opérationnel à garder propre.

    Viser le fichier entier ferait échouer le test sur le mode EXCEL legacy, qui lit
    légitimement des classeurs tant qu'il existe ; viser la fonction SQLite dit exactement ce
    qu'on protège.
    """
    arbre = ast.parse(source)
    for noeud in arbre.body:
        if isinstance(noeud, ast.FunctionDef) and noeud.name == "_main_sqlite":
            return ast.get_source_segment(source, noeud) or ""
    raise AssertionError("`_main_sqlite` introuvable : le mode SQLite de lot6c a disparu")


# ── Niveau STRUCTUREL ────────────────────────────────────────────────────────

def test_le_mode_sqlite_existe_toujours():
    bloc = _bloc_sqlite(SOURCE)
    assert "calculer_ecarts_menages_externes" in bloc, (
        "le mode SQLite doit déléguer à la règle canonique de lib_db_moteur, "
        "pas réimplémenter le rapprochement")


def test_le_mode_sqlite_ne_lit_aucun_classeur():
    bloc = _bloc_sqlite(SOURCE)
    for interdit in ("load_workbook", "read_excel", "ExcelFile", "openpyxl", ".xlsx", ".xlsm"):
        assert interdit not in bloc, (
            f"le parcours SQLite de lot6c ne doit pas mentionner {interdit!r}")


def test_l_argument_source_reste_explicite():
    """`--source` doit rester un choix EXPLICITE : aucun basculement implicite vers Excel."""
    assert '_ap.add_argument("--source", choices=("EXCEL", "SQLITE")' in SOURCE
    assert 'default="EXCEL"' in SOURCE, (
        "le défaut historique est assumé et documenté ; s'il change, ce test doit être relu")


# ── Niveau COMPORTEMENTAL ────────────────────────────────────────────────────

from tests._espion_ouvertures import (  # noqa: E402
    classeurs as _classeurs_ouverts,
    executer as _espionner,
)


@pytest.fixture(scope="module")
def racine_isolee(tmp_path_factory) -> Path:
    """Racine ne contenant QUE les moteurs : aucun classeur ne peut être lu par accident."""
    import shutil

    racine = tmp_path_factory.mktemp("racine_lot6c_sans_excel")
    (racine / "02_TRAVAIL").mkdir()
    for module in _TRAVAIL.glob("*.py"):
        shutil.copy2(module, racine / "02_TRAVAIL" / module.name)
    restants = [p for p in racine.rglob("*") if p.suffix.lower() in EXTENSIONS]
    assert not restants, f"la racine isolée doit être vierge de classeurs : {restants}"
    return racine


def _base_avec_donnees(tmp_path: Path) -> Path:
    """Base migrée portant de quoi produire un vrai rapprochement — sinon le run traverserait
    moins de code, et le test prouverait moins."""
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-6C-1", "2026-07", "LOG_0001", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, devise, source, statut) "
            "VALUES ('FAC-6C','INT_0003','F-6C','2026-07-31',55.0,'EUR','PDF_EXTRACTION','VALIDEE')")
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, description, montant_ttc, source) "
            "VALUES ('FLM-6C','FAC-6C','MENAGE_EXTERNE','LOG_0001','test',55.0,'PDF_EXTRACTION')")
        conn.commit()
    finally:
        conn.close()
    return db


def test_un_run_sqlite_n_ouvre_aucun_classeur(racine_isolee, tmp_path):
    """PREUVE CENTRALE — ni lecture ni écriture de classeur dans le parcours SQLite."""
    db = _base_avec_donnees(tmp_path)
    travail = racine_isolee / "02_TRAVAIL"
    proc, ouvertures = _espionner(
        [PYTHON, str(travail / LOT6C.name), "--source", "SQLITE", "--db", str(db)],
        cwd=travail, atelier=Path(tmp_path),
        env_retire=("PILOTAGE_DB_PATH", "APP_DATA_DIR"))

    assert ouvertures, "l'espion n'a rien enregistré — le hook n'a pas été installé"
    assert proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]

    lus = _classeurs_ouverts(ouvertures, en_lecture=True)
    ecrits = _classeurs_ouverts(ouvertures, en_lecture=False)
    assert not lus, f"lot6c --source SQLITE a LU un classeur : {lus}"
    assert not ecrits, f"lot6c --source SQLITE a ÉCRIT un classeur : {ecrits}"


def test_le_run_sqlite_produit_bien_le_rapprochement(racine_isolee, tmp_path):
    """Contre-épreuve : le run ne passe pas « sans rien ouvrir » parce qu'il n'a rien fait."""
    db = _base_avec_donnees(tmp_path)
    travail = racine_isolee / "02_TRAVAIL"
    proc, _ = _espionner(
        [PYTHON, str(travail / LOT6C.name), "--source", "SQLITE", "--db", str(db)],
        cwd=travail, atelier=Path(tmp_path),
        env_retire=("PILOTAGE_DB_PATH", "APP_DATA_DIR"))
    sortie = proc.stdout + proc.stderr
    assert "VUE_ECART_HOSTAWAY" in sortie, sortie[-2000:]
    assert "aucun classeur lu ni" in sortie, sortie[-2000:]

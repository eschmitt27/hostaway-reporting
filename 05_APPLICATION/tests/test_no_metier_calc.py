"""APP-0 — Absence de calcul métier dans le code applicatif.
Scan textuel des modules app/ — aucune réimplémentation de règle métier autorisée.
"""
import ast
from pathlib import Path

APP_DIR = Path(__file__).parent.parent / "app"

# Patterns interdits dans le code Python de l'app
FORBIDDEN_PATTERNS = [
    "commission",          # calcul de commission propriétaire
    "net_proprietaire",    # calcul net propriétaire
    "taux_commission",     # taux de commission (réimplémentation)
    "revenu_net",          # calcul revenu net
    "cout_complet",        # calcul coût complet ménages
    "gain_perte",          # calcul gain/perte ménages
    "rapprochement_calcul", # calcul de rapprochement
    "statut_mois.*CLOTURE.*=",  # écriture du statut clôture (hors routes dédiées)
]

# Fichiers exclus du scan (ces patterns peuvent y apparaître légitimement comme constantes/lectures)
EXCLUDED_FILES = {
    "run_log_reader.py",   # lit des colonnes nommées avec ces mots
    "pipeline_registry.py",  # noms de scripts
}

FORBIDDEN_STRINGS = [
    "* commission_rate",
    "taux_commission *",
    "net_prop =",
    "revenu_net =",
    "cout_complet =",
]


def get_python_files():
    return [
        f for f in APP_DIR.rglob("*.py")
        if f.name not in EXCLUDED_FILES
        and "__pycache__" not in f.parts
    ]


def test_no_arithmetic_on_metier_fields():
    """Aucune multiplication/division sur des champs métier clés."""
    violations = []
    for f in get_python_files():
        src = f.read_text(encoding="utf-8")
        lines = src.splitlines()
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            # Ignorer les commentaires et strings
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue
            for pattern in FORBIDDEN_STRINGS:
                if pattern in stripped:
                    violations.append(f"{f.relative_to(APP_DIR)}:{i} — {stripped[:80]}")
    assert not violations, "Calculs métier interdits détectés :\n" + "\n".join(violations)


def test_no_import_of_travail_modules():
    """L'app n'importe jamais directement les modules 02_TRAVAIL/.

    La détection porte sur de VRAIES instructions d'import en début de ligne, pas sur une
    sous-chaîne : depuis la migration Lot10 (0044), les tables s'appellent `lot10_*` et un
    `SELECT ... FROM lot10_runs` contient littéralement « from lot » sans rien importer. Un test
    qui interdit un mot dans du SQL n'interdit plus un import — il signale du bruit.
    """
    import re

    _IMPORT_LOT = re.compile(r"^[ \t]*(?:import[ \t]+lot|from[ \t]+lot\w*[ \t]+import)",
                             re.MULTILINE)

    violations = []
    for f in get_python_files():
        src = f.read_text(encoding="utf-8")
        if _IMPORT_LOT.search(src):
            violations.append(str(f))
        if "02_TRAVAIL" in src and "import" in src:
            # Toléré si c'est juste une référence de chemin (config)
            for line in src.splitlines():
                if "import" in line and "02_TRAVAIL" in line:
                    violations.append(f"{f}: {line.strip()}")
    assert not violations, "Import direct de modules 02_TRAVAIL interdit :\n" + "\n".join(violations)


def test_no_bidirectional_sync():
    """Aucun module ne doit accéder à la DB ET écrire Excel (sync bidirectionnelle interdite).

    Flux autorisé : formulaire validé → writer Excel → journal SQLite.
    Flux interdit : SELECT SQLite → row_data → wb.save().
    Détection : import effectif de la couche DB (get_db / from app.db) + appel wb.save().
    Les commentaires/docstrings mentionnant "SQLite" ne déclenchent pas de violation.
    """
    DB_ACCESS_PATTERNS = ("from app.db", "import sqlite3", "get_db(", "get_db ")
    # Writers vérifiés dont le `save()` cible une COPIE isolée (jamais une source métier réelle) et
    # dont l'accès DB est le journal APP autorisé (formulaire validé → writer Excel COPIE → journal
    # SQLite). Flux conforme au docstring ci-dessus ; les flags d'écriture réelle restent False.
    WRITERS_COPIE_AUTORISES = {
        "banques_controle_writer.py",       # APP-4B : override sur copie + journal
        "controles_runner_service.py",      # APP-5B : classification sur copie workspace + journal runs
        "charges_validation_service.py",    # APP-3f : write-guard + remplacement atomique de la
                                             # SAISIE (vérité) puis journal SQLite one-way (jamais de
                                             # relecture SQLite réinjectée dans l'Excel)
        "banques_import_service.py",        # Module Banque : write-guard + remplacement atomique de
                                             # NORM_Banque puis journal SQLite one-way (import_id
                                             # seulement — jamais de relecture SQLite dans l'Excel)
    }
    violations = []
    for f in get_python_files():
        if f.name in WRITERS_COPIE_AUTORISES:
            continue
        src = f.read_text(encoding="utf-8")
        has_db_access = any(p in src for p in DB_ACCESS_PATTERNS)
        has_excel_write = "save(" in src
        if has_db_access and has_excel_write:
            violations.append(str(f))
    assert not violations, "Sync bidirectionnelle DB→Excel interdite :\n" + "\n".join(violations)


# `saisie_hh_orchestrator.py` (avec son unique appelant, la route POST /nouvelle/confirmer) a été
# retiré : le circuit HH passe désormais par previsualiser -> previsualisation/{token} -> enregistrer
# -> reservations_hh_saisie_service.creer (SQLite). `test_saisie_hh_orchestrator_no_excel_write_api`
# testait ce module - retiré avec lui.

# `writers/saisie_hh_writer.py` (0 appelant réel — cf. mission nettoyage legacy 2026-08-22) a été
# supprimé. `test_saisie_hh_writer_no_db_access` testait ce module - retiré avec lui.


def test_no_sqlite_to_row_data_flow():
    violations = []
    for f in get_python_files():
        src = f.read_text(encoding="utf-8")
        if "SELECT " in src and "row_data" in src:
            violations.append(str(f.relative_to(APP_DIR)))
    assert not violations, "Flux SQLite -> row_data interdit :\n" + "\n".join(violations)


def test_app_code_parseable():
    """Tous les fichiers Python de l'app sont syntaxiquement valides."""
    errors = []
    for f in APP_DIR.rglob("*.py"):
        if "__pycache__" in f.parts:
            continue
        try:
            ast.parse(f.read_text(encoding="utf-8"))
        except SyntaxError as e:
            errors.append(f"{f}: {e}")
    assert not errors, "Erreurs de syntaxe :\n" + "\n".join(errors)

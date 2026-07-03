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
    """L'app n'importe jamais directement les modules 02_TRAVAIL/."""
    violations = []
    for f in get_python_files():
        src = f.read_text(encoding="utf-8")
        if "import lot" in src.lower() or "from lot" in src.lower():
            violations.append(str(f))
        if "02_TRAVAIL" in src and "import" in src:
            # Toléré si c'est juste une référence de chemin (config)
            for line in src.splitlines():
                if "import" in line and "02_TRAVAIL" in line:
                    violations.append(f"{f}: {line.strip()}")
    assert not violations, "Import direct de modules 02_TRAVAIL interdit :\n" + "\n".join(violations)


def test_no_bidirectional_sync():
    """Aucune écriture depuis SQLite vers Excel (synchronisation bidirectionnelle interdite)."""
    violations = []
    for f in get_python_files():
        if f.name == "saisie_writer.py":
            continue
        src = f.read_text(encoding="utf-8")
        # Chercher des patterns d'écriture xlsx depuis des résultats SQLite
        if "sqlite" in src.lower() and "save(" in src.lower():
            violations.append(str(f))
    assert not violations, "Sync bidirectionnelle SQLite→Excel interdite :\n" + "\n".join(violations)


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

from pathlib import Path
from app.config import PROJECT_ROOT, SAISIE_PATTERN


# Chemins jamais modifiables par l'application.
#
# `01_SOURCES_BRUTES/REF_Setup` est le RÉPERTOIRE, pas le classeur. L'entrée pointait auparavant
# vers `01_SOURCES_BRUTES/REF_Setup.xlsm`, un chemin qui n'existe pas : le classeur vit dans un
# sous-dossier (correction APP-1 appliquée à `config.py`, jamais répercutée ici). Le garde couvrait
# donc un fichier fantôme. `REF_Setup.xlsm` restait protégé par le contrôle de nom — il ne commence
# pas par `SAISIE_` — mais un fichier nommé `SAISIE_*.xlsx` déposé DANS ce répertoire aurait été
# jugé modifiable. Viser le répertoire couvre le classeur et tout ce qui l'accompagne.
_READONLY_ROOTS = [
    PROJECT_ROOT / "01_SOURCES_BRUTES" / "REF_Setup",
    PROJECT_ROOT / "02_TRAVAIL",
    PROJECT_ROOT / "03_EXPORTS",
    PROJECT_ROOT / "01_SOURCES_BRUTES" / "REF_Cloture_Mensuelle.xlsx",
    PROJECT_ROOT / "01_SOURCES_BRUTES" / "REF_Banque_Regles.xlsx",
]

_READONLY_NAME_PATTERNS = ["MASTER_", "REF_", "BANQUE_"]


def is_writable(path: Path) -> bool:
    """Seuls les SAISIE_*.xlsx sont writables."""
    p = Path(path)
    if not p.name.startswith(SAISIE_PATTERN):
        return False
    if not p.suffix.lower() in (".xlsx", ".xlsm"):
        return False
    # Ne jamais écrire dans un chemin qui contient un répertoire read-only
    for ro in _READONLY_ROOTS:
        try:
            p.relative_to(ro)
            return False
        except ValueError:
            pass
    return True


def assert_writable(path: Path) -> None:
    if not is_writable(path):
        raise PermissionError(
            f"INTERDIT — écriture refusée sur chemin non-SAISIE : {path}\n"
            "L'application ne modifie jamais les sources brutes, REF_*, MASTER_* ou exports."
        )


def is_source_readable(path: Path) -> bool:
    return Path(path).exists()


def classify_path(path: Path) -> str:
    """Retourne 'SAISIE_WRITABLE' | 'READ_ONLY' | 'APP_DATA' | 'UNKNOWN'."""
    p = Path(path)
    from app.config import DATA_DIR
    try:
        p.relative_to(DATA_DIR)
        return "APP_DATA"
    except ValueError:
        pass
    if is_writable(p):
        return "SAISIE_WRITABLE"
    for pattern in _READONLY_NAME_PATTERNS:
        if p.name.startswith(pattern):
            return "READ_ONLY"
    return "READ_ONLY"

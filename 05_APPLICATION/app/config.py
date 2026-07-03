from pathlib import Path
import os

# Racine du dossier applicatif (05_APPLICATION/)
APP_ROOT = Path(__file__).resolve().parent.parent

# Racine du projet (parent de 05_APPLICATION/)
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", str(APP_ROOT.parent)))

PORT = int(os.environ.get("PORT", 8000))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info")

# Chemins sources (lecture seule)
SOURCES_BRUTES = PROJECT_ROOT / "01_SOURCES_BRUTES"
TRAVAIL = PROJECT_ROOT / "02_TRAVAIL"
EXPORTS_POWERBI = PROJECT_ROOT / "03_EXPORTS" / "PowerBI"
# REF_Setup.xlsm vit dans un sous-dossier REF_Setup/ (corrigé APP-1)
REF_SETUP = PROJECT_ROOT / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm"
# Export généré par le moteur — source unique de la LISTE logements (propriétaire + dates déjà résolus)
PBI_LOGEMENTS = EXPORTS_POWERBI / "PBI_Referentiel_Logements.csv"
# Réservations hors Hostaway (APP-2a) — LECTURE SEULE
# MASTER généré par Power Query = source de consultation (onglet MASTER)
MASTER_RESERVATIONS_HH = TRAVAIL / "Lot4_ReservationsHH" / "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
# SAISIE = source amont — nommée pour l'origine uniquement, jamais lue/écrite au Lot APP-2a
SAISIE_RESERVATIONS_HH = SOURCES_BRUTES / "ReservationsHH" / "SAISIE_ReservationsHorsHostaway.xlsx"
MASTER_RUN_LOG = TRAVAIL / "Lot1_Hostaway" / "MASTER_RUN_Log.xlsx"
MASTER_CTRL_COHERENCE = TRAVAIL / "Lot11_Controles"
# Chemin autonome obsolète — source officielle : REF_SETUP onglet REF_Cloture_Mensuelle.
# Ne jamais utiliser cette constante ; elle désigne un fichier inexistant.
_REF_CLOTURE_OBSOLETE: None = None

# Écriture SAISIE HH — garde de sécurité (APP-2b).
# Ne jamais activer implicitement ni par défaut.
HH_REAL_WRITE_ENABLED = False

# Chemins saisie (écriture atomique uniquement — activée aux lots dédiés)
SAISIE_ROOT = PROJECT_ROOT / "01_SOURCES_BRUTES"
SAISIE_PATTERN = "SAISIE_"

# Données applicatives (seul endroit où l'app écrit)
DATA_DIR = APP_ROOT / "data"
DB_PATH = DATA_DIR / "app.db"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
RESTORE_DIR = DATA_DIR / "restore_workspace"

# Templates et static
TEMPLATES_DIR = APP_ROOT / "app" / "templates"
STATIC_DIR = APP_ROOT / "app" / "static"

# Pipeline scripts (01_TRAVAIL/*.py)
PIPELINE_SCRIPTS_ROOT = TRAVAIL

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
# Charges / Fournisseurs (APP-3a) — LECTURE SEULE
MASTER_CHARGES = TRAVAIL / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx"
SAISIE_CHARGES = SOURCES_BRUTES / "Charges" / "SAISIE_Charges_Flux.xlsx"
# Source de vérité durable des impacts analytiques d'une charge (saisie, distincte des masters calc).
# Onglets normalisés liés par charge_id : AFFECTATIONS, MENAGE, RESERVE_REFACTURATION.
SAISIE_CHARGES_IMPACTS = SOURCES_BRUTES / "Charges" / "SAISIE_Charges_Impacts.xlsx"
# Source de saisie durable des avantages associés (Lot7 existant, onglet SOURCE_SAISIE).
SAISIE_IK_AVANTAGES = TRAVAIL / "Lot7_IK_Avantages" / "MASTER_FACT_MAN_IK_Avantages.xlsx"
# Réservations résolues (APP-3b-1) — source de validation reservation_id, LECTURE SEULE
MASTER_CALC_RESERVATIONS_RESOLUES = TRAVAIL / "Lot4quater_SourceResolue" / "MASTER_CALC_Reservations_Resolues.xlsx"
# Propriétaires & règlements (APP-3c) — LECTURE SEULE
MASTER_NET_PROPRIETAIRE = TRAVAIL / "Lot10_Resultats" / "MASTER_CALC_NetProprietaire.xlsx"
MASTER_FACT_PROPRIETAIRES = TRAVAIL / "Lot12_Factures" / "MASTER_FACT_Proprietaires.xlsx"
# Ménages (APP-2) — LECTURE SEULE
MASTER_RAPPROCHEMENT_MENAGES = TRAVAIL / "Lot6d_Rapprochement_Menages" / "MASTER_CTRL_Rapprochement_Menages.xlsx"
MASTER_GAINPERTE_MENAGES = TRAVAIL / "Lot6e_GainPerte_Menages" / "MASTER_CALC_GainPerte_Menages.xlsx"
MASTER_COUTCOMPLET_MENAGES = TRAVAIL / "Lot6f_CoutComplet_Menages" / "MASTER_CALC_CoutComplet_Menages.xlsx"
MASTER_CTRL_COHERENCE = TRAVAIL / "Lot11_Controles"
# Chemin autonome obsolète — source officielle : REF_SETUP onglet REF_Cloture_Mensuelle.
# Ne jamais utiliser cette constante ; elle désigne un fichier inexistant.
_REF_CLOTURE_OBSOLETE: None = None

# Écriture SAISIE HH — garde de sécurité (APP-2b).
# Ne jamais activer implicitement ni par défaut.
HH_REAL_WRITE_ENABLED = False
HH_REAL_WRITE_CONFIRMATION_ENABLED = False

# Migration REF_Assoc_Mode dans REF_Setup.xlsm — garde de sécurité (APP-3b-0).
# Ne jamais activer implicitement ni par défaut.
REF_ASSOC_MODE_REAL_WRITE_ENABLED = False

# Écriture SAISIE Charges — garde de sécurité (APP-3b-1).
# Ne jamais activer implicitement ni par défaut.
CHARGES_REAL_WRITE_ENABLED = False
CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = False

# Chemins saisie (écriture atomique uniquement — activée aux lots dédiés)
SAISIE_ROOT = PROJECT_ROOT / "01_SOURCES_BRUTES"
SAISIE_PATTERN = "SAISIE_"

# Données applicatives (seul endroit où l'app écrit)
DATA_DIR = APP_ROOT / "data"
DB_PATH = DATA_DIR / "app.db"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
RESTORE_DIR = DATA_DIR / "restore_workspace"
DRYRUNS_DIR = DATA_DIR / "dryruns"
LOT4A_ENGINE_PYTHON = Path(os.environ.get("LOT4A_ENGINE_PYTHON", r"C:\Program Files\Python312\python.exe"))
LOT4A_ENGINE_TIMEOUT_SECONDS = int(os.environ.get("LOT4A_ENGINE_TIMEOUT_SECONDS", "60"))

# Templates et static
TEMPLATES_DIR = APP_ROOT / "app" / "templates"
STATIC_DIR = APP_ROOT / "app" / "static"

# Pipeline scripts (01_TRAVAIL/*.py)
PIPELINE_SCRIPTS_ROOT = TRAVAIL

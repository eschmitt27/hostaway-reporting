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
# Réservations table commune (APP-5B) — détail VRBO/charge fixe, onglet MASTER, LECTURE SEULE
MASTER_CALC_RESERVATIONS = TRAVAIL / "Lot4bis_TableCommune" / "MASTER_CALC_Reservations.xlsx"
# Propriétaires & règlements (APP-3c) — LECTURE SEULE
MASTER_NET_PROPRIETAIRE = TRAVAIL / "Lot10_Resultats" / "MASTER_CALC_NetProprietaire.xlsx"
MASTER_FACT_PROPRIETAIRES = TRAVAIL / "Lot12_Factures" / "MASTER_FACT_Proprietaires.xlsx"
# Ménages (APP-2) — LECTURE SEULE
# Les 4 flux restent portés par 4 fichiers distincts : jamais fusionnés côté application.
MASTER_RAPPROCHEMENT_MENAGES = TRAVAIL / "Lot6d_Rapprochement_Menages" / "MASTER_CTRL_Rapprochement_Menages.xlsx"
MASTER_GAINPERTE_MENAGES = TRAVAIL / "Lot6e_GainPerte_Menages" / "MASTER_CALC_GainPerte_Menages.xlsx"
MASTER_COUTCOMPLET_MENAGES = TRAVAIL / "Lot6f_CoutComplet_Menages" / "MASTER_CALC_CoutComplet_Menages.xlsx"
# Flux B — tâches Hostaway (comptage opérationnel uniquement, jamais valorisation).
MASTER_HA_CLEANINGTASKS = TRAVAIL / "Lot1_Hostaway" / "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"
# Flux C — déclarations internes M04 (heures + taux internes).
MASTER_DECLARATIONS_INTERNES = TRAVAIL / "Lot6b_DeclarationsInternes" / "MASTER_NORM_Declarations_Internes.xlsx"
# Flux D — ménages externes facturés (le coût vient de la facture).
MASTER_MENAGES_EXTERNES = TRAVAIL / "Lot6c_MenagesExternes" / "MASTER_FACT_MEN_MenagesExternes.xlsx"
MASTER_CTRL_COHERENCE_DIR = TRAVAIL / "Lot11_Controles"
MASTER_CTRL_COHERENCE = MASTER_CTRL_COHERENCE_DIR / "MASTER_CTRL_Coherence.xlsx"
# ── Intégration : constantes des modules APP-4 / APP-3C / APP-5A ──────────────
# Banques & caisse (APP-4A) — LECTURE SEULE.
MASTER_BANQUE = TRAVAIL / "Lot8_Banque" / "BANQUE_LOT8_IMPORT.xlsx"
MASTER_CAISSE = None
# Propriétaires & règlements (APP-3C) — complète MASTER_NET_PROPRIETAIRE / MASTER_FACT_PROPRIETAIRES.
MASTER_COMMISSIONS = TRAVAIL / "Lot10_Resultats" / "MASTER_CALC_Commissions.xlsx"
MASTER_RESULTATS = TRAVAIL / "Lot10_Resultats" / "MASTER_CALC_Resultats.xlsx"
# Relevés propriétaires (APP-3D) — sources brutes non encore lues par aucun module applicatif.
SAISIE_ACOMPTES_PROPRIETAIRES = SOURCES_BRUTES / "AcomptesProprietaires" / "SAISIE_AcomptesProprietaires.xlsx"
SAISIE_AIRCOVER = SOURCES_BRUTES / "AirCover" / "SAISIE_AirCover.xlsx"
SAISIE_IMPUTATIONS_AIRBNB = SOURCES_BRUTES / "ImputationsAirbnb" / "SAISIE_ImputationsAirbnb.xlsx"
SAISIE_AJUSTEMENTS_POST_CLOTURE = SOURCES_BRUTES / "AjustementsPostCloture" / "SAISIE_Ajustements_PostCloture.xlsx"
PROPRIETAIRE_OPAQUE_SALT = "APP3D_RELEVES_v1"
# Contrôles & clôture (APP-5A) — alias fichier (MASTER_CTRL_COHERENCE pointe déjà le fichier).
MASTER_CTRL_COHERENCE_FILE = MASTER_CTRL_COHERENCE
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

# ── MODE RECETTE — données fictives isolées ──────────────────────────────────
# Activé par RECETTE_MODE=1. Quand actif : l'interface affiche un bandeau, et le write-guard
# (app/recette_guard.py) n'autorise une écriture que si le chemin cible est sous RECETTE_ROOT.
# Ne change RIEN au comportement par défaut (RECETTE_MODE absent => False).
RECETTE_MODE = os.environ.get("RECETTE_MODE", "0").strip() in ("1", "true", "True")


def _env_flag(nom: str) -> bool:
    return os.environ.get(nom, "0").strip() in ("1", "true", "True")


# Écriture SAISIE Charges — garde de sécurité (APP-3b-1).
# Ne jamais activer implicitement ni par défaut. Activables par variable d'environnement
# UNIQUEMENT en mode recette : une instance NON recette ne peut jamais écrire, même si les
# variables sont positionnées (double verrou : RECETTE_MODE ET la variable dédiée).
CHARGES_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("CHARGES_REAL_WRITE_ENABLED")
CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = RECETTE_MODE and _env_flag("CHARGES_REAL_WRITE_CONFIRMATION_ENABLED")
# Racine unique autorisée en écriture en mode recette. Par défaut, le PROJECT_ROOT courant : en
# lançant l'instance de recette avec PROJECT_ROOT=<dossier data_recette>, TOUTES les sources et
# sorties vivent déjà dans ce dossier isolé, et le guard interdit toute écriture en dehors.
RECETTE_ROOT = Path(os.environ.get("RECETTE_ROOT", str(PROJECT_ROOT))).resolve()

# Chemins saisie (écriture atomique uniquement — activée aux lots dédiés)
SAISIE_ROOT = PROJECT_ROOT / "01_SOURCES_BRUTES"
SAISIE_PATTERN = "SAISIE_"

# Données applicatives (seul endroit où l'app écrit)
# DATA_DIR isolable par env (APP_DATA_DIR) : permet de lancer une instance de validation sans
# toucher la vraie base ni le vrai dossier data/ (snapshots, workspaces). Défaut = comportement normal.
DATA_DIR = Path(os.environ.get("APP_DATA_DIR", str(APP_ROOT / "data")))
DB_PATH = DATA_DIR / "app.db"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
RESTORE_DIR = DATA_DIR / "restore_workspace"
DRYRUNS_DIR = DATA_DIR / "dryruns"
LOT4A_ENGINE_PYTHON = Path(os.environ.get("LOT4A_ENGINE_PYTHON", r"C:\Program Files\Python312\python.exe"))
LOT4A_ENGINE_TIMEOUT_SECONDS = int(os.environ.get("LOT4A_ENGINE_TIMEOUT_SECONDS", "60"))

# ── Recalcul ménages (APP-2b) — garde de sécurité. Jamais activé implicitement. ──
# Deux modes distincts :
#   MODE_COPIES : recette isolée. Copie le sous-arbre nécessaire dans un workspace
#                 sous data/, y exécute lot6d/lot6e, ne touche AUCUN fichier réel.
#                 Toujours autorisé (aucune écriture métier possible).
#   MODE_REEL   : régénérerait les MASTER ménages EN PLACE (via run_menages_pipeline,
#                 qui commence par lot6b → Google Sheet + réécriture M04/MASTER_NORM).
#                 BLOQUÉ tant que ce flag est False. Ne jamais l'activer par défaut.
# Aligné sur le double verrou commun à tous les writers (RECETTE_MODE + variable dédiée) : c'était
# la dernière garde codée en dur, ce qui faisait mentir la règle « double verrou partout » et
# empêchait de l'activer comme les autres. Reste False par défaut ; mode réel jamais activé.
MENAGES_REAL_RECALC_ENABLED = RECETTE_MODE and _env_flag("MENAGES_REAL_RECALC_ENABLED")
# Interpréteur moteur du recalcul (porte openpyxl ; réutilise l'interpréteur pandas du moteur).
MENAGES_ENGINE_PYTHON = Path(os.environ.get("MENAGES_ENGINE_PYTHON", str(LOT4A_ENGINE_PYTHON)))
MENAGES_RECALC_TIMEOUT_SECONDS = int(os.environ.get("MENAGES_RECALC_TIMEOUT_SECONDS", "300"))
# Workspace isolé des recalculs sur copies (sous data/, jamais dans l'arbre métier).
MENAGES_RECALC_WORKSPACE = DATA_DIR / "menages_recalc"
# Runner hors paquet app/ (sous-processus, interpréteur moteur — jamais importé par FastAPI).
MENAGES_RECALC_RUNNER = APP_ROOT / "runners" / "menages_recalcul_runner.py"

# ── Chaîne ménages COMPLÈTE sur copies (APP-2b+) — recette 6a→6b→6c→6d→6e→6f→11. ──
# Régénère les TROIS sources (Hostaway via stub, déclarations depuis une source copiée,
# ménages externes depuis lot6c) AVANT le rapprochement, dans un workspace isolé.
# Aucune requête réseau/API, aucun fichier réel touché. Le mode RÉEL reste gardé par
# MENAGES_REAL_RECALC_ENABLED (ci-dessus) : jamais activé implicitement.
MENAGES_CHAINE_WORKSPACE = DATA_DIR / "menages_chaine"
MENAGES_CHAINE_TIMEOUT_SECONDS = int(os.environ.get("MENAGES_CHAINE_TIMEOUT_SECONDS", "600"))
# Stubs contrôlés (réseau/API remplacés dans le workspace uniquement — jamais dans le dépôt).
MENAGES_STUBS_DIR = APP_ROOT / "runners" / "stubs_menages"
# Dossier RÉEL des factures de ménage externes (PDF), lecture seule — source documentaire.
MENAGES_PDF_DIR = PROJECT_ROOT / "01_SOURCES_BRUTES" / "MenagesExternes" / "Factures_PDF"
MENAGES_PDF_DIR_REL = r"01_SOURCES_BRUTES\MenagesExternes\Factures_PDF"

# ── APP-4B — Contrôle & catégorisation bancaire sur copies. Garde de sécurité. ──
# Aucune écriture bancaire réelle possible tant que ces flags sont False. Jamais activés par défaut.
#   Le writer refuse toute écriture réelle ; il ne travaille que sur une COPIE de
#   BANQUE_LOT8_IMPORT.xlsx dans un workspace isolé sous data/. Excel reste la vérité métier ;
#   SQLite ne fait que JOURNALISER les décisions applicatives (jamais une nouvelle vérité).
#   Import bancaire (APP-3F+) : mêmes flags, même double verrou que Charges — activables
#   UNIQUEMENT en mode recette (RECETTE_MODE ET variable d'environnement dédiée). Une instance NON
#   recette ne peut jamais écrire NORM_Banque, même si les variables sont positionnées.
BANQUE_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("BANQUE_REAL_WRITE_ENABLED")
BANQUE_REAL_WRITE_CONFIRMATION_ENABLED = RECETTE_MODE and _env_flag("BANQUE_REAL_WRITE_CONFIRMATION_ENABLED")
# Workspace isolé des overrides bancaires sur copies (jamais dans l'arbre métier).
BANQUE_CONTROLE_WORKSPACE = DATA_DIR / "banque_controle"
# Onglet d'override écrit dans la COPIE (jamais dans les onglets moteur BRUT/NORM/CTRL).
BANQUE_OVERRIDE_SHEET = "OVERRIDE_APP4B"
# Sel de l'empreinte opaque des mouvements (aucune donnée de compte dans l'identifiant public).
BANQUE_OPAQUE_SALT = "APP4B_BANQUE_v1"

# ── Factures fournisseurs & règlements — garde de sécurité ───────────────────
# Même double verrou que Charges / Banque / Logements : activables UNIQUEMENT en mode recette
# (RECETTE_MODE ET variable d'environnement dédiée). Une instance NON recette ne peut jamais
# enregistrer une facture ni un règlement, même si les variables sont positionnées.
FACTURES_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("FACTURES_REAL_WRITE_ENABLED")
FACTURES_REAL_WRITE_CONFIRMATION_ENABLED = RECETTE_MODE and _env_flag("FACTURES_REAL_WRITE_CONFIRMATION_ENABLED")

# ── Pilotage des calculs (runs de pipeline) — garde de sécurité ──────────────
# Le mode RÉEL (exécution des lots sur l'arborescence métier réelle) reste DÉSACTIVÉ par défaut.
# En recette, les lots tournent sous PROJECT_ROOT=<data_recette>, donc sans jamais toucher le réel.
CALCULS_REAL_RUN_ENABLED = RECETTE_MODE and _env_flag("CALCULS_REAL_RUN_ENABLED")
CALCULS_REAL_RUN_CONFIRMATION_ENABLED = RECETTE_MODE and _env_flag("CALCULS_REAL_RUN_CONFIRMATION_ENABLED")

# ── APP-5B — Contrôles détaillés & suivi humain ──────────────────────────────
#   Le moteur (Lot11) reste la vérité de l'anomalie. SQLite JOURNALISE uniquement le suivi humain
#   (prise en charge, résolution, exception) — jamais une nouvelle vérité, jamais un masquage moteur.
#   Le recalcul se fait sur COPIES isolées (runner Lot8c/Lot11), jamais sur les sources métier.
CONTROLES_REAL_WRITE_ENABLED = False
CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED = False
# Workspace isolé du runner Lot8c/Lot11 sur copies (jamais dans l'arbre métier réel).
CONTROLES_RUNNER_WORKSPACE = DATA_DIR / "controles_runner"
# Sel de l'empreinte opaque publique des contrôles détaillés (aucune donnée sensible dans l'id).
CONTROLES_OPAQUE_SALT = "APP5B_CTRL_v1"

# ── APP-SEC-1 — Diagnostics, chemins locaux, exposition réseau ──────────────
#   /health reste minimal en public. Le détail technique (chemins, état fichiers) ne vit que dans
#   /health/diagnostic, désactivé par défaut, jamais lié dans l'UI, réservé au client local.
DIAGNOSTIC_DETAILS_ENABLED = os.environ.get("DIAGNOSTIC_DETAILS_ENABLED", "false").lower() == "true"
#   Application locale mono-utilisateur : aucune écoute réseau implicite. 0.0.0.0 / IP LAN / IPv6
#   global refusés par run_app.py sauf activation explicite de ce flag (jamais par défaut).
ALLOW_NETWORK_BIND = os.environ.get("ALLOW_NETWORK_BIND", "false").lower() == "true"

# ── APP-5C — Suivi humain de la clôture mensuelle ────────────────────────────
#   La clôture RÉELLE reste exclusivement REF_Cloture_Mensuelle (REF_Setup.xlsm, moteur, D024),
#   jamais écrite par l'application. Ce module journalise uniquement la préparation/validation
#   humaine (checklist, revue APP-5B), jamais une nouvelle vérité de clôture.
CLOTURE_OPAQUE_SALT = "APP5C_CLOTURE_v1"

# Templates et static
TEMPLATES_DIR = APP_ROOT / "app" / "templates"
STATIC_DIR = APP_ROOT / "app" / "static"

# Pipeline scripts (01_TRAVAIL/*.py)
PIPELINE_SCRIPTS_ROOT = TRAVAIL

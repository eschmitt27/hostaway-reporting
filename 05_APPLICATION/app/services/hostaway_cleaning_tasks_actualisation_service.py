"""Actualisation Hostaway CleaningTasks — point d'entrée unique, réel.

    actualiser() → HostawayClient.get_tasks() (API) → normalisation Python → SQLite versionné

ZÉRO EXCEL. L'ancienne version passait par un pont `lot1_hostaway_extract.py --only-cleaning-tasks`
(sous-processus) qui écrivait `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`, puis relisait ce
fichier pour l'enregistrer en SQLite — un effet de bord sur un fichier réel du dépôt à chaque
actualisation (mission « supprimer le dernier effet de bord Excel »). Ce module appelle
`HostawayAuth`/`HostawayClient`/`extraire_cleaning_tasks` (`app/adapters/hostaway_client.py` — le
moteur Hostaway canonique, MÊME client, MÊME transformation de champs, MÊME correctif de
pagination — rien réimplémenté ; déplacé depuis `02_TRAVAIL/lot1_hostaway_extract.py` mission
stabilisation 2026-09-09 pour ne plus dépendre d'un module `02_TRAVAIL` depuis `app/`, cf.
`tests/test_no_metier_calc.py::test_no_import_of_travail_modules`) EN PROCESS, sans sous-processus
ni fichier intermédiaire, et transmet directement le résultat à `hostaway_cleaning_tasks_raw_service`
(versioning déjà existant, réutilisé tel quel).

Un export Excel explicite reste possible via `exporter_cleaning_tasks_excel()` (§3 mission) — jamais
appelé par ce module ni par aucun parcours automatique (Actualiser Hostaway/les ménages/toute
l'activité, scheduler).

CADENCE SÉPARÉE, JAMAIS AUTOMATIQUE
Ce service est appelé UNIQUEMENT par une demande explicite (`inclure_imports_externes=True`,
cf. `orchestrateur_dag.py` — nœud HOSTAWAY_CLEANING_TASKS marqué `externe=True`). H6 a déjà essuyé
des 429 sévères par le passé ; le déclencher à chaque « Actualiser toute l'activité » serait la même
erreur.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.adapters.hostaway_client import AnomalyDetector, HostawayAuth, HostawayClient
from app.adapters.hostaway_client import extraire_cleaning_tasks
from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import run_history_service as history

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"

E_CREDENTIALS_ABSENTES = "HOSTAWAY_CLEANING_TASKS_CREDENTIALS_ABSENTES"
E_API_ECHOUEE = "HOSTAWAY_CLEANING_TASKS_API_ECHOUEE"
E_MIGRATION_ABSENTE = "MIGRATION_ABSENTE"
E_DEPOT_INDISPONIBLE = "HOSTAWAY_CLEANING_TASKS_DEPOT_INDISPONIBLE"
E_TACHES_NON_PUBLIEES = "HOSTAWAY_CLEANING_TASKS_NON_PUBLIEES"
E_DEJA_SYNCHRONISEES = "HOSTAWAY_CLEANING_TASKS_DEJA_SYNCHRONISEES"

# Transport des tâches. Le DÉPÔT publié par le pipeline GitHub est la source canonique, comme pour
# les réservations : aucun identifiant Hostaway local. L'appel API direct reste disponible sur
# demande explicite (`source=SOURCE_API`) mais n'est emprunté par aucun parcours.
SOURCE_DEPOT = "DEPOT_GITHUB"
SOURCE_API = "API"

MESSAGES = {
    E_DEPOT_INDISPONIBLE: ("Le dépôt de données Hostaway n'a pas pu être lu : les tâches de ménage "
                           "affichées restent celles du dernier import réussi."),
    E_TACHES_NON_PUBLIEES: ("Les tâches de ménage ne sont pas publiées dans le dernier état du dépôt "
                            "de données : le dernier import réussi est conservé."),
    E_DEJA_SYNCHRONISEES: ("Les tâches de ménage publiées sont identiques à celles déjà en base : "
                           "rien de nouveau à importer."),
    # Message ACTIONNABLE : il dit quoi faire et OÙ, pas seulement ce qui manque. Sans le chemin,
    # l'utilisateur sait qu'il manque des identifiants mais pas où les déposer — et le fichier
    # `.env` étant ignoré par Git, il est normalement absent d'un worktree neuf.
    E_CREDENTIALS_ABSENTES: (
        "Identifiants Hostaway absents : l'appel à l'API est impossible. "
        "Renseignez HOSTAWAY_CLIENT_ID, HOSTAWAY_CLIENT_SECRET et HOSTAWAY_ACCOUNT_ID dans le "
        "fichier « .env » à la racine du projet (modèle fourni : « .env.example » — copiez-le en "
        "« .env » et complétez-le), puis redémarrez l'application."),
}


def diagnostic_configuration() -> dict[str, Any]:
    """État de la configuration Hostaway, SANS jamais exposer une valeur NI un chemin absolu.

    Deux exigences se rencontrent ici et sont toutes deux tenues :
      · dire à l'utilisateur OÙ déposer ses identifiants, sinon le message « identifiants absents »
        ne lui apprend rien d'actionnable ;
      · ne jamais publier de chemin absolu dans un écran (règle APP-SEC-1) — il porterait le nom de
        l'utilisateur Windows.
    D'où un chemin SANITISÉ (`<PROJECT_ROOT>/.env`) : l'emplacement est parfaitement désigné, la
    machine reste anonyme. Les valeurs, elles, ne sortent jamais : uniquement des booléens.
    """
    from dotenv import load_dotenv

    from app.services import path_sanitizer

    chemin = Path(cfg.PROJECT_ROOT) / ".env"
    load_dotenv(chemin)
    requis = ("HOSTAWAY_CLIENT_ID", "HOSTAWAY_CLIENT_SECRET", "HOSTAWAY_ACCOUNT_ID")
    presents = {nom: bool(os.getenv(nom, "").strip()) for nom in requis}
    return {
        "chemin_env": path_sanitizer.sanitize_path(chemin),
        "fichier_present": chemin.exists(),
        "variables": presents,
        "manquantes": [nom for nom, ok in presents.items() if not ok],
        "complet": all(presents.values()),
        "modele": path_sanitizer.sanitize_path(Path(cfg.PROJECT_ROOT) / ".env.example"),
        # Ce qui conditionne réellement l'import des tâches : leur publication dans le dépôt.
        "taches_publiees": source_disponible(),
    }


def _credentials() -> tuple[str, str, str, str] | None:
    from dotenv import load_dotenv

    load_dotenv(Path(cfg.PROJECT_ROOT) / ".env")
    client_id = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com")
    if not (client_id and client_secret and account_id):
        return None
    return base_url, client_id, client_secret, account_id


def credentials_disponibles() -> bool:
    """Préflight SANS appel réseau — vrai si les 3 identifiants requis sont configurés.

    Réutilisé par `menages_actualisation_service.actualiser()` pour arrêter la chaîne AVANT le
    PDF/Sheet si Hostaway n'est de toute façon pas configuré (mission « Hostaway non configuré ->
    arrêt immédiat »), sans dupliquer `_credentials()`.
    """
    return _credentials() is not None


def source_disponible() -> bool:
    """Préflight SANS réseau : le dernier état connu du dépôt publie-t-il les tâches de ménage ?

    Lit l'état local du dépôt (le `git fetch` a lieu dans `actualiser()` et dans l'étape « dépôt »
    de l'actualisation Ménages) — jamais un appel Hostaway, jamais un `.env`.
    """
    try:
        from app.services import hostaway_depot_service as depot

        lib = depot._lib_depot()
        etat = lib.etat(cfg.PROJECT_ROOT, remote=depot.REMOTE_DEFAUT,
                        branche=depot.BRANCHE_DEFAUT, rafraichir=False)
        return bool(etat.get("cleaning_tasks_publiees"))
    except Exception:      # noqa: BLE001 — dépôt illisible : source indisponible, pas une erreur d'écran
        return False


_CLES_CONTENU = ("task_id", "reservation_id", "listing_map_id", "title", "status",
                 "can_start_from", "assignee_user_id")


def _empreinte_contenu(taches):
    """Multiensemble des tâches, colonne par colonne telles que stockées. Deux jeux de même
    empreinte sont identiques : les réimporter fabriquerait une extraction indiscernable."""
    from collections import Counter

    def _norm(v):
        return "" if v is None else str(v)

    return Counter(tuple(_norm(raw._valeur(t, c)) for c in _CLES_CONTENU) for t in taches)


def _actualiser_depuis_depot(*, declencheur: str, date_from: str, db_path) -> dict[str, Any]:
    """Tâches lues dans le dépôt publié → MÊME normalisation → MÊME couche RAW versionnée.

    Rien n'est ouvert en base tant que le jeu publié n'est pas lu en entier : un dépôt illisible,
    des tâches non publiées ou un jeu vide laissent la dernière extraction réussie en place.
    """
    from app.services import hostaway_depot_service as depot
    from app.services.path_sanitizer import sanitize_erreur_externe

    history_run_id = history.demarrer("HOSTAWAY_CLEANING_TASKS", acteur=declencheur, db_path=db_path)
    log = _LogRelais()
    try:
        lib = depot._lib_depot()
        client = lib.SourceDepotGitHub(cfg.PROJECT_ROOT, log, remote=depot.REMOTE_DEFAUT,
                                       branche=depot.BRANCHE_DEFAUT, rafraichir=True)
    except Exception as exc:      # noqa: BLE001
        history.marquer_echec(history_run_id,
                              erreur=f"{E_DEPOT_INDISPONIBLE} : {sanitize_erreur_externe(str(exc))}",
                              db_path=db_path)
        return {"ok": False, "code": E_DEPOT_INDISPONIBLE, "message": MESSAGES[E_DEPOT_INDISPONIBLE]}

    etat = client.etat_source
    provenance = {"source_ref": etat.get("commit"), "commit_court": etat.get("commit_court"),
                  "source_horodatage": etat.get("source_horodatage")}
    if not etat.get("cleaning_tasks_publiees"):
        history.marquer_echec(history_run_id, erreur=E_TACHES_NON_PUBLIEES, db_path=db_path)
        return {"ok": False, "code": E_TACHES_NON_PUBLIEES,
                "message": MESSAGES[E_TACHES_NON_PUBLIEES], **provenance}

    lignes, statut_extraction = extraire_cleaning_tasks(client, date_from, AnomalyDetector(set()), log)
    if statut_extraction != "OK" or not lignes:
        history.marquer_echec(
            history_run_id,
            erreur=f"{E_DEPOT_INDISPONIBLE} : lecture {statut_extraction}, {len(lignes)} tâche(s)",
            db_path=db_path)
        return {"ok": False, "code": E_DEPOT_INDISPONIBLE,
                "message": MESSAGES[E_DEPOT_INDISPONIBLE], **provenance}

    precedente = raw.derniere_extraction_utilisable(db_path=db_path)
    if precedente and _empreinte_contenu(lignes) == _empreinte_contenu(
            raw.taches(extraction_id=precedente, db_path=db_path)):
        history.marquer_succes(history_run_id, db_path=db_path)
        return {"ok": True, "importe": False, "code": E_DEJA_SYNCHRONISEES,
                "message": MESSAGES[E_DEJA_SYNCHRONISEES], "declencheur": declencheur,
                "extraction_id": precedente, "nb_taches": len(lignes), **provenance}

    extraction_id = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, run_id=history_run_id or "",
                               db_path=db_path)
    if not extraction_id:
        history.marquer_echec(history_run_id, erreur="migration 0035 absente", db_path=db_path)
        return {"ok": False, "code": E_MIGRATION_ABSENTE,
                "message": "Table hostaway_cleaning_tasks_extractions absente (migration 0035 non "
                           "appliquée)."}
    try:
        raw.enregistrer(extraction_id, taches=lignes, db_path=db_path)
    except Exception as exc:
        raw.cloturer(extraction_id, statut=raw.ST_ECHEC, message=type(exc).__name__, db_path=db_path)
        history.marquer_echec(history_run_id, erreur=type(exc).__name__, db_path=db_path)
        raise
    resultat_cloture = raw.cloturer(
        extraction_id, statut=raw.ST_SUCCES, db_path=db_path,
        message=(f"Dépôt {provenance['commit_court']} — données produites le "
                 f"{provenance['source_horodatage']}"))
    history.marquer_succes(history_run_id, db_path=db_path)
    return {"ok": True, "importe": True, "declencheur": declencheur,
            "history_run_id": history_run_id, "nb_taches": len(lignes), **provenance,
            **resultat_cloture}


def actualiser(*, declencheur: str = DECLENCHEUR_MANUEL, date_from: str = "2026-01-01",
               db_path=None, source: str = SOURCE_DEPOT) -> dict[str, Any]:
    """Récupère les tâches ménage Hostaway (H6) et les enregistre en SQLite versionné.

    SOURCE CANONIQUE : le dépôt publié par le pipeline GitHub (`SOURCE_DEPOT`, défaut) — aucun
    identifiant Hostaway local, aucun `.env`. `SOURCE_API` conserve l'appel direct, qu'aucun
    parcours n'emprunte. Aucun fichier Excel créé ni lu. `date_from` : mêmes tâches que
    `lot1_hostaway_extract.py --only-cleaning-tasks` (défaut identique).
    """
    if source == SOURCE_DEPOT:
        return _actualiser_depuis_depot(declencheur=declencheur, date_from=date_from,
                                        db_path=db_path)
    creds = _credentials()
    if creds is None:
        return {"ok": False, "code": E_CREDENTIALS_ABSENTES,
                "message": MESSAGES[E_CREDENTIALS_ABSENTES]}
    base_url, client_id, client_secret, account_id = creds

    debut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    history_run_id = history.demarrer("HOSTAWAY_CLEANING_TASKS", acteur=declencheur, db_path=db_path)

    log = _LogRelais()
    auth = HostawayAuth(base_url, client_id, client_secret)
    client = HostawayClient(auth, account_id, log)
    detector = AnomalyDetector(set())

    try:
        lignes, statut_extraction = extraire_cleaning_tasks(client, date_from, detector, log)
    except Exception as exc:
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        return {"ok": False, "code": E_API_ECHOUEE,
                "message": f"{MESSAGES.get(E_API_ECHOUEE, 'Appel API échoué')} ({type(exc).__name__})"}

    if statut_extraction == "FAILED":
        history.marquer_echec(history_run_id, erreur="extraction FAILED", db_path=db_path)
        return {"ok": False, "code": E_API_ECHOUEE,
                "message": "L'extraction des tâches ménage a échoué (voir logs)."}

    extraction_id = raw.ouvrir(mode=raw.MODE_API, run_id=history_run_id or "", db_path=db_path)
    if not extraction_id:
        history.marquer_echec(history_run_id, erreur="migration 0035 absente", db_path=db_path)
        return {"ok": False, "code": E_MIGRATION_ABSENTE,
                "message": "Table hostaway_cleaning_tasks_extractions absente (migration 0035 non "
                           "appliquée)."}
    try:
        raw.enregistrer(extraction_id, taches=lignes, db_path=db_path)
    except Exception as exc:
        raw.cloturer(extraction_id, statut=raw.ST_ECHEC, message=f"{type(exc).__name__}: {exc}",
                     db_path=db_path)
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        raise

    statut_cloture = raw.ST_SUCCES if statut_extraction == "OK" else raw.ST_PARTIEL
    resultat_cloture = raw.cloturer(extraction_id, statut=statut_cloture, db_path=db_path)
    history.marquer_succes(history_run_id, db_path=db_path)

    return {"ok": True, "lance_le": debut, "declencheur": declencheur, "code_retour": 0,
            "history_run_id": history_run_id, "extraction_id": extraction_id,
            "statut": statut_cloture, "nb_taches": len(lignes), **resultat_cloture}


class _LogRelais:
    """`extraire_cleaning_tasks` attend un logger (`.info`/`.warning`) — relais minimal vers le
    logger applicatif standard, jamais un `print` silencieux ni un logger inventé."""

    def __init__(self) -> None:
        import logging
        self._log = logging.getLogger("hostaway_cleaning_tasks_actualisation")

    def info(self, msg: str) -> None:
        self._log.info(msg)

    def warning(self, msg: str) -> None:
        self._log.warning(msg)

    def error(self, msg: str) -> None:
        self._log.error(msg)


def exporter_cleaning_tasks_excel(*, db_path=None, racine: Path | None = None) -> dict[str, Any]:
    """Export EXPLICITE (diagnostic/comparaison legacy uniquement) — jamais appelé par
    `actualiser()` ni par aucun parcours automatique (§3 mission). Réutilise l'extraction active
    déjà en SQLite ; n'appelle pas l'API."""
    import pandas as pd

    from app.db.connection import get_db

    extraction_id = raw.derniere_extraction_utilisable(db_path=db_path)
    if not extraction_id:
        return {"ok": False, "code": "AUCUNE_EXTRACTION_UTILISABLE",
                "message": "Aucune extraction Cleaning Tasks disponible à exporter."}

    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT task_id, reservation_id, listing_map_id, title, status, can_start_from, "
            "assignee_user_id, extrait_le, row_hash FROM hostaway_cleaning_tasks "
            "WHERE extraction_id = ?", (extraction_id,)
        ).fetchall()
    finally:
        conn.close()

    chemin = (Path(racine) if racine else Path(cfg.PROJECT_ROOT)).joinpath(
        "02_TRAVAIL", "Lot1_Hostaway", "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx")
    df = pd.DataFrame([dict(r) for r in rows])
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(chemin, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="data", index=False)
    return {"ok": True, "extraction_id": extraction_id, "nb_lignes": len(rows),
            "chemin": str(chemin)}

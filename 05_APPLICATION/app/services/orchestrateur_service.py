"""Orchestrateur — « Actualiser toute l'activité » et actualisations ciblées.

RAISONNE EN DATASETS, PAS EN FICHIERS
Le DAG (`orchestrateur_dag`) décrit les dépendances entre TABLES SQLite. La fraîcheur d'un dataset
se lit dans `orchestrateur_datasets` (migration 0050) : quel run l'a produit et quand. Aucun `mtime`
n'intervient — un fichier touché ne rend rien « frais », et un Lot10 calculé sur un ancien Lot9 ne
peut pas s'afficher à jour (§33/§34).

RÉUTILISE LES REGISTRES EXISTANTS
Les runs sont écrits dans `moteur_runs`/`moteur_run_etapes` (0031), dont le vocabulaire prévoyait
déjà ce cas : statuts EN_COURS/SUCCES/PARTIEL/ECHEC/INTERROMPU et `declencheur` ORCHESTRATEUR.
Aucun quatrième registre n'est créé.

ATOMICITÉ ET HONNÊTETÉ DES ÉTATS
Chaque dataset est marqué A_JOUR seulement après le succès réel de son service. Un échec en cours de
chaîne rend le run PARTIEL : les datasets déjà recalculés restent valides, les suivants restent
A_RECALCULER. Un dataset dont un amont a échoué n'est jamais tenté — le calculer sur une entrée
périmée produirait un résultat faux présenté comme frais.
"""
from __future__ import annotations

import importlib
import inspect
import json
import os
import socket
import uuid
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlite3

import app.config as cfg
from app.db.connection import get_db
from app.services import backup_service
from app.services import orchestrateur_dag as dag
from app.services import run_history_service as history

# Statuts de dataset
ST_JAMAIS = "JAMAIS_CALCULE"
ST_A_JOUR = "A_JOUR"
ST_A_RECALCULER = "A_RECALCULER"
ST_EN_COURS = "EN_COURS"
ST_ECHEC = "ECHEC"
ST_PARTIEL = "PARTIEL"

# Statuts de run (vocabulaire `moteur_runs`, 0031 — repris tel quel)
RUN_EN_COURS = "EN_COURS"
RUN_SUCCES = "SUCCES"
RUN_PARTIEL = "PARTIEL"
RUN_ECHEC = "ECHEC"
RUN_INTERROMPU = "INTERROMPU"

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"
DECLENCHEUR_ORCHESTRATEUR = "ORCHESTRATEUR"

LOT_ORCHESTRATEUR = "orchestrateur"

PORTEE_GLOBALE = "PIPELINE_GLOBAL"
BAIL_DEFAUT_S = 3600

E_VERROU = "ORCHESTRATEUR_VERROU_PRIS"
E_DATASET_INCONNU = "ORCHESTRATEUR_DATASET_INCONNU"
E_AMONT_INDISPONIBLE = "ORCHESTRATEUR_AMONT_INDISPONIBLE"
E_SANS_SERVICE = "ORCHESTRATEUR_DATASET_SANS_SERVICE"
E_NON_CONFIGURE = "ORCHESTRATEUR_SOURCE_NON_CONFIGUREE"

# Statuts d'ÉTAPE (moteur_run_etapes). Un run réel est PLANIFIÉ dès son ouverture : chaque étape
# existe en EN_ATTENTE, passe EN_COURS quand elle démarre, puis prend son statut final. C'est ce
# que l'écran lit pour montrer la progression — aucun minuteur, aucune estimation.
ETAPE_EN_ATTENTE = "EN_ATTENTE"
ETAPE_EN_COURS = "EN_COURS"
ETAPE_SUCCES = "SUCCES"
ETAPE_ECHEC = "ECHEC"
ETAPE_IGNOREE = "IGNOREE"
ETAPE_NON_CONFIGUREE = "NON_CONFIGUREE"
ETAPE_SAUVEGARDE = "SAUVEGARDE"

# Nature d'une étape ignorée (détail JSON) : ce qui distingue un blocage d'un saut légitime.
NATURE_BLOQUEE = "BLOQUEE"
NATURE_SANS_SERVICE = "SANS_SERVICE"
NATURE_NON_DECLENCHEE = "NON_DECLENCHEE"
NATURE_INCHANGEE = "INCHANGEE"
NATURE_NON_EXECUTEE = "NON_EXECUTEE"


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _horodatage(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _identite() -> str:
    return f"{socket.gethostname()}/{os.getpid()}"


def _table_presente(nom: str, db_path=None) -> bool:
    """Une base pas encore migrée n'est pas une panne : c'est un état.

    La base réelle est en 0016 tant que la migration n'a pas été appliquée ; l'écran doit alors
    afficher « jamais calculé » et non un 500. Toute lecture de l'orchestrateur passe donc par
    cette garde.
    """
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone() \
            is not None
    finally:
        conn.close()


# ── Verrou (§36) ────────────────────────────────────────────────────────────────────────────────

def prendre_verrou(portee: str, run_id: str, *, bail_s: int = BAIL_DEFAUT_S,
                   db_path=None) -> dict[str, Any]:
    """Prend un bail sur une portée. Un bail expiré est repris — un processus tué ne bloque pas.

    Deux portées différentes ne se gênent pas : c'est ce qui permet à deux actualisations réellement
    indépendantes de tourner en parallèle sans se bloquer inutilement.

    ATOMIQUE : lecture et écriture du bail tiennent dans UNE transaction `BEGIN IMMEDIATE`. Sans
    elle, deux demandeurs simultanés (un battement du scheduler et un clic) lisaient tous deux
    « libre » avant que l'un n'écrive, et chacun croyait détenir le verrou.
    """
    maintenant = datetime.now(timezone.utc)
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existant = conn.execute(
            "SELECT run_id, detenu_par, expire_le FROM orchestrateur_verrous WHERE portee = ?",
            (portee,)).fetchone()
        if existant is not None and existant["expire_le"] > _horodatage(maintenant) \
                and existant["run_id"] != run_id:
            conn.rollback()
            return {"ok": False, "code": E_VERROU, "portee": portee,
                    "detenu_par": existant["detenu_par"], "run_id": existant["run_id"],
                    "message": f"Un recalcul est déjà en cours sur {portee}."}
        conn.execute(
            "INSERT INTO orchestrateur_verrous (portee, run_id, detenu_par, pris_le, expire_le) "
            "VALUES (?,?,?,?,?) ON CONFLICT(portee) DO UPDATE SET run_id=excluded.run_id, "
            "detenu_par=excluded.detenu_par, pris_le=excluded.pris_le, expire_le=excluded.expire_le",
            (portee, run_id, _identite(), _horodatage(maintenant),
             _horodatage(maintenant + timedelta(seconds=bail_s))))
        conn.commit()
        return {"ok": True, "portee": portee, "run_id": run_id}
    finally:
        conn.close()


def liberer_verrou(portee: str, run_id: str, *, db_path=None) -> None:
    conn = get_db(db_path)
    try:
        conn.execute("DELETE FROM orchestrateur_verrous WHERE portee = ? AND run_id = ?",
                     (portee, run_id))
        conn.commit()
    finally:
        conn.close()


def verrou_actif(portee: str, *, db_path=None) -> dict[str, Any] | None:
    """Bail en cours sur `portee`, s'il y en a un — LECTURE SEULE, le verrou n'est jamais pris.

    Sert à décider sans rien réserver (« une synchronisation tourne-t-elle ? »). La décision reste
    indicative : seul `prendre_verrou` garantit l'exclusion.
    """
    if not _table_presente("orchestrateur_verrous", db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT portee, run_id, detenu_par, pris_le, expire_le FROM orchestrateur_verrous "
            "WHERE portee = ? AND expire_le > ?", (portee, _maintenant())).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


# ── État des datasets (§33) ─────────────────────────────────────────────────────────────────────

def _etat_brut(db_path=None) -> dict[str, dict[str, Any]]:
    if not _table_presente("orchestrateur_datasets", db_path):
        return {}
    conn = get_db(db_path)
    try:
        return {r["dataset"]: dict(r)
                for r in conn.execute("SELECT * FROM orchestrateur_datasets")}
    finally:
        conn.close()


def _journaliser_dataset(conn, dataset: str, avant: str | None, apres: str, motif: str,
                         detail: Any = None) -> None:
    conn.execute(
        "INSERT INTO orchestrateur_dataset_evenements (dataset, statut_avant, statut_apres, "
        "motif, detail) VALUES (?,?,?,?,?)",
        (dataset, avant, apres, motif, json.dumps(detail, default=str) if detail else None))


def marquer_dataset(dataset: str, statut: str, *, run_id: str = "", declencheur: str = "",
                    nb_lignes: int | None = None, detail: Any = None, erreur_code: str = "",
                    erreur_message: str = "", motif: str = "RECALCUL", db_path=None) -> None:
    """Écrit l'état d'un dataset et journalise la transition."""
    conn = get_db(db_path)
    try:
        # `calcule_le` DOIT être lu ici : c'est la date du dernier calcul RÉUSSI, conservée à travers
        # EN_COURS/ECHEC. Ne lire que `statut` l'effaçait à chaque transition — l'ordonnanceur voyait
        # alors une source en échec comme « jamais actualisée » et la relançait à chaque battement,
        # sans jamais appliquer son palier.
        avant = conn.execute(
            "SELECT statut, calcule_le FROM orchestrateur_datasets WHERE dataset = ?",
            (dataset,)).fetchone()
        avant_statut = avant["statut"] if avant else None
        calcule_le = _maintenant() if statut == ST_A_JOUR else (
            avant["calcule_le"] if avant and "calcule_le" in avant.keys() else None)
        conn.execute(
            "INSERT INTO orchestrateur_datasets (dataset, statut, calcule_le, source_run_id, "
            "declencheur, nb_lignes, detail, erreur_code, erreur_message, maj_le) "
            "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dataset) DO UPDATE SET "
            "statut=excluded.statut, calcule_le=excluded.calcule_le, "
            "source_run_id=excluded.source_run_id, declencheur=excluded.declencheur, "
            "nb_lignes=excluded.nb_lignes, detail=excluded.detail, "
            "erreur_code=excluded.erreur_code, erreur_message=excluded.erreur_message, "
            "maj_le=excluded.maj_le",
            (dataset, statut, calcule_le, run_id, declencheur, nb_lignes,
             json.dumps(detail, default=str) if detail else None, erreur_code, erreur_message,
             _maintenant()))
        _journaliser_dataset(conn, dataset, avant_statut, statut, motif, detail)
        conn.commit()
    finally:
        conn.close()


def invalider_descendants(dataset: str, *, db_path=None) -> list[str]:
    """Marque A_RECALCULER tous les descendants d'un dataset qui vient de changer (§34).

    Les exports optionnels ne sont pas invalidés : ils sont reconstructibles à la demande et ne
    conditionnent aucun calcul. Les prétendre « périmés » ferait clignoter une alerte pour une
    sortie que personne n'attend.
    """
    touches = []
    for aval in dag.descendants(dataset):
        if dag.NOEUDS[aval].type_noeud == dag.TYPE_EXPORT:
            continue
        marquer_dataset(aval, ST_A_RECALCULER, motif="INVALIDATION_AMONT",
                        detail={"amont": dataset}, db_path=db_path)
        touches.append(aval)
    return touches


def etat_datasets(db_path=None) -> list[dict[str, Any]]:
    """État de chaque dataset du DAG, prêt pour l'affichage (§32/§33).

    Un dataset absent de la table n'est pas une anomalie : c'est un dataset jamais calculé. Il est
    rendu comme tel, jamais comme « à jour ».
    """
    brut = _etat_brut(db_path)
    out = []
    for nom in dag.ordre_topologique():
        noeud = dag.NOEUDS[nom]
        e = brut.get(nom, {})
        out.append({
            "dataset": nom,
            "libelle": noeud.libelle,
            "type": noeud.type_noeud,
            "depend_de": list(noeud.depend_de),
            "statut": e.get("statut") or ST_JAMAIS,
            "calcule_le": e.get("calcule_le"),
            "source_run_id": e.get("source_run_id"),
            "declencheur": e.get("declencheur"),
            "nb_lignes": e.get("nb_lignes"),
            "erreur_code": e.get("erreur_code"),
            "erreur_message": e.get("erreur_message"),
            "commentaire": noeud.commentaire,
        })
    return out


# ── Exécution d'un dataset ──────────────────────────────────────────────────────────────────────

# Déclencheur du run en cours, lu par `_appeler_service`. Un paramètre de plus aurait été plus direct,
# mais la signature `(chemin, db_path)` est le point de substitution de toute la suite de tests ; une
# variable de contexte transmet l'information sans la casser, et reste propre à chaque fil — un
# battement du scheduler et une requête web ne se la partagent pas.
_declencheur_courant: ContextVar[str] = ContextVar("orchestrateur_declencheur",
                                                   default=DECLENCHEUR_MANUEL)

# Options du run en cours (ex. `hostaway_a_la_demande`) et étape en cours d'exécution — même
# mécanisme que le déclencheur, pour la même raison : la signature des services ne change pas.
_options_run: ContextVar[dict | None] = ContextVar("orchestrateur_options", default=None)
_etape_courante: ContextVar[tuple | None] = ContextVar("orchestrateur_etape", default=None)


def option_run(nom: str, defaut: Any = None) -> Any:
    """Option du run orchestré en cours, lue par un service ; `defaut` hors run."""
    return (_options_run.get() or {}).get(nom, defaut)


def definir_option_run(nom: str, valeur: Any) -> None:
    """Un service transmet une information aux étapes suivantes du MÊME run."""
    options = _options_run.get()
    if options is not None:
        options[nom] = valeur


def signaler_sous_etapes(sous_etapes: list[dict]) -> None:
    """Publie l'avancement interne de l'étape en cours (ex. les phases de l'extraction Hostaway)
    dans son détail : l'écran les affiche sous l'étape, en direct. Sans run en cours : rien."""
    courante = _etape_courante.get()
    if courante is None:
        return
    run_id, etape, db_path = courante
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE moteur_run_etapes SET detail = ? WHERE run_id = ? AND etape = ? AND statut = ?",
            (json.dumps({"sous_etapes": sous_etapes}, default=str, ensure_ascii=False), run_id,
             etape, ETAPE_EN_COURS))
        conn.commit()
    finally:
        conn.close()


def _appeler_service(chemin: str, db_path) -> dict[str, Any]:
    """Résout "module:fonction" et l'appelle avec `db_path`.

    La résolution est tardive : le DAG reste une carte, et l'orchestrateur n'importe que ce qu'il
    exécute réellement.

    Un service qui déclare un paramètre `declencheur` le reçoit : un import Hostaway lancé depuis
    l'écran reste MANUEL jusqu'au journal, un battement du scheduler reste AUTO. Les autres services
    sont appelés exactement comme avant.
    """
    module_nom, fonction_nom = chemin.split(":")
    fonction = getattr(importlib.import_module(module_nom), fonction_nom)
    arguments: dict[str, Any] = {"db_path": db_path}
    if "declencheur" in inspect.signature(fonction).parameters:
        arguments["declencheur"] = _declencheur_courant.get()
    # Seuls les moteurs acceptant un mois reçoivent cette option ; les autres
    # gardent leur chaîne globale cohérente. Le contexte est propre à ce run.
    if option_run("mois") and "mois" in inspect.signature(fonction).parameters:
        arguments["mois"] = option_run("mois")
    resultat = fonction(**arguments)
    return resultat if isinstance(resultat, dict) else {"ok": bool(resultat)}


def _executer_service(chemin: str, db_path, declencheur: str) -> dict[str, Any]:
    jeton = _declencheur_courant.set(declencheur)
    try:
        return _appeler_service(chemin, db_path)
    finally:
        _declencheur_courant.reset(jeton)


def _compter_lignes(tables: tuple[str, ...], db_path) -> int | None:
    if not tables:
        return None
    conn = get_db(db_path)
    try:
        total = 0
        for t in tables:
            if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (t,)).fetchone() is None:
                continue
            total += conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        return total
    finally:
        conn.close()


def recalculer_dataset(dataset: str, *, run_id: str = "",
                       declencheur: str = DECLENCHEUR_MANUEL, db_path=None) -> dict[str, Any]:
    """Recalcule UN dataset. Ne touche pas ses descendants — c'est le rôle de l'appelant.

    Un dataset sans service (import piloté par l'utilisateur : référentiel Setup, relevé bancaire) n'est
    jamais « fabriqué » : la fonction le dit explicitement au lieu de laisser croire à un recalcul.
    """
    if dataset not in dag.NOEUDS:
        return {"ok": False, "code": E_DATASET_INCONNU, "dataset": dataset,
                "message": f"Dataset inconnu du DAG : {dataset}"}
    noeud = dag.NOEUDS[dataset]
    if not noeud.service:
        return {"ok": False, "code": E_SANS_SERVICE, "dataset": dataset,
                "message": f"{noeud.libelle} : donnée fournie de l'extérieur, "
                           "l'orchestrateur ne la recalcule pas."}

    statut_avant = _statut_dataset(dataset, db_path)
    marquer_dataset(dataset, ST_EN_COURS, run_id=run_id, declencheur=declencheur, db_path=db_path)
    try:
        resultat = _executer_service(noeud.service, db_path, declencheur)
    except Exception as exc:   # noqa: BLE001 — toute panne doit devenir un état lisible
        marquer_dataset(dataset, ST_ECHEC, run_id=run_id, declencheur=declencheur,
                        erreur_code=type(exc).__name__, erreur_message=str(exc)[:500],
                        motif="ECHEC", db_path=db_path)
        return {"ok": False, "dataset": dataset, "code": type(exc).__name__,
                "message": str(exc)[:500]}

    if resultat.get("non_configure"):
        # Rien n'a été interrogé : la donnée en place n'est PAS rafraîchie, donc jamais « à jour »
        # à cette date — et ce n'est pas une panne non plus. Un import « à recalculer » ne bloque
        # pas l'aval (`_amonts_en_echec`).
        nouveau = ST_JAMAIS if statut_avant in (None, ST_JAMAIS) else ST_A_RECALCULER
        marquer_dataset(dataset, nouveau, run_id=run_id, declencheur=declencheur,
                        erreur_code=E_NON_CONFIGURE, erreur_message=resultat.get("message", ""),
                        motif="NON_CONFIGURE", db_path=db_path)
        return {"ok": False, "dataset": dataset, "code": E_NON_CONFIGURE, **resultat,
                "statut_dataset": nouveau}

    if not resultat.get("ok", False):
        marquer_dataset(dataset, ST_ECHEC, run_id=run_id, declencheur=declencheur,
                        erreur_code=resultat.get("code", "ECHEC"),
                        erreur_message=resultat.get("message", ""), motif="ECHEC", db_path=db_path)
        return {"ok": False, "dataset": dataset, **resultat}

    marquer_dataset(dataset, ST_A_JOUR, run_id=run_id, declencheur=declencheur,
                    nb_lignes=_compter_lignes(noeud.tables, db_path), detail=resultat,
                    db_path=db_path)
    return {"ok": True, "dataset": dataset, **resultat}


def _statut_dataset(dataset: str, db_path) -> str | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT statut FROM orchestrateur_datasets WHERE dataset = ?",
                         (dataset,)).fetchone()
        return r["statut"] if r else None
    finally:
        conn.close()


# ── Runs (réutilise moteur_runs / moteur_run_etapes) ────────────────────────────────────────────

def _nouveau_run_id() -> str:
    return f"ORCH-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _ouvrir_run(declencheur: str, cible: str, db_path, *, run_id: str | None = None) -> str:
    run_id = run_id or _nouveau_run_id()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur, arguments, "
            "pid, hote) VALUES (?,?,?,?,?,?,?,?)",
            (run_id, LOT_ORCHESTRATEUR, _maintenant(), RUN_EN_COURS, declencheur, cible,
             os.getpid(), socket.gethostname()))
        conn.commit()
    finally:
        conn.close()
    return run_id


def _planifier(run_id: str, a_traiter: list[str], *, sauvegarde: bool, db_path) -> None:
    """Écrit le PLAN du run : une étape EN_ATTENTE par dataset, dans l'ordre du DAG (et la
    sauvegarde en tête d'une actualisation globale). Idempotent : un run déjà planifié (préparé
    par l'écran avant la tâche de fond) ne l'est pas deux fois."""
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM moteur_run_etapes WHERE run_id = ? LIMIT 1",
                        (run_id,)).fetchone():
            return
        maintenant = _maintenant()
        lignes = ([(run_id, ETAPE_SAUVEGARDE, 0)] if sauvegarde else []) + [
            (run_id, d, i) for i, d in enumerate(a_traiter, start=1)]
        conn.executemany(
            "INSERT INTO moteur_run_etapes (run_id, etape, ordre, started_at, statut) "
            "VALUES (?,?,?,?,'" + ETAPE_EN_ATTENTE + "')",
            [(r, e, o, maintenant) for r, e, o in lignes])
        conn.commit()
    finally:
        conn.close()


def _etape_debut(run_id: str, etape: str, db_path) -> None:
    """L'étape démarre : c'est ce passage EN_COURS que l'écran affiche en direct."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE moteur_run_etapes SET statut = ?, started_at = ? "
            "WHERE run_id = ? AND etape = ? AND statut = ?",
            (ETAPE_EN_COURS, _maintenant(), run_id, etape, ETAPE_EN_ATTENTE))
        conn.commit()
    finally:
        conn.close()


def _etape(run_id: str, dataset: str, ordre: int, statut: str, debut: str,
           erreur: str, nb_ecrits: int | None, db_path, *, detail: dict | None = None) -> None:
    """Statut final d'une étape : met à jour l'étape planifiée, ou l'insère (dry-run, run ancien)."""
    contenu = json.dumps(detail, default=str, ensure_ascii=False) if detail else None
    conn = get_db(db_path)
    try:
        maj = conn.execute(
            "UPDATE moteur_run_etapes SET ordre = ?, started_at = ?, ended_at = ?, statut = ?, "
            "nb_ecrits = ?, erreur = ?, detail = ? WHERE run_id = ? AND etape = ?",
            (ordre, debut, _maintenant(), statut, nb_ecrits, erreur or None, contenu, run_id,
             dataset))
        if maj.rowcount == 0:
            conn.execute(
                "INSERT INTO moteur_run_etapes (run_id, etape, ordre, started_at, ended_at, "
                "statut, nb_ecrits, erreur, detail) VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, dataset, ordre, debut, _maintenant(), statut, nb_ecrits, erreur or None,
                 contenu))
        conn.commit()
    finally:
        conn.close()


def _solder_etapes(conn, run_id: str, motif: str) -> None:
    """Un run terminé n'a plus d'étape « en attente » ni « en cours » : celles qui n'ont pas eu
    lieu sont dites non exécutées, jamais laissées à faire croire qu'elles vont démarrer."""
    conn.execute(
        "UPDATE moteur_run_etapes SET statut = ?, ended_at = COALESCE(ended_at, ?), "
        "erreur = COALESCE(erreur, ?), detail = COALESCE(detail, ?) "
        "WHERE run_id = ? AND statut IN (?, ?)",
        (ETAPE_IGNOREE, _maintenant(), motif,
         json.dumps({"nature": NATURE_NON_EXECUTEE}), run_id, ETAPE_EN_ATTENTE, ETAPE_EN_COURS))


def _cloturer_run(run_id: str, statut: str, nb_ok: int, nb_ko: int, resume: str, db_path) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE moteur_runs SET ended_at = ?, statut = ?, nb_etapes = ?, nb_etapes_ok = ?, "
            "nb_etapes_ko = ?, erreur_resume = ? WHERE run_id = ?",
            (_maintenant(), statut, nb_ok + nb_ko, nb_ok, nb_ko, resume or None, run_id))
        _solder_etapes(conn, run_id, "Non exécutée : le run s'est terminé avant cette étape.")
        conn.commit()
    finally:
        conn.close()


def _amonts_en_echec(dataset: str, etats: dict[str, str]) -> list[str]:
    """Amonts qui empêchent de calculer `dataset` sans produire un résultat trompeur.

    Mission 14b — le premier run réel a montré `FLUX_LOT9` déclaré `SUCCES` avec 0 ligne alors que
    `RESERVATIONS` était bloqué par l'échec de `HOSTAWAY_RAW` : seul `ST_ECHEC` était bloquant ici,
    pas `ST_A_RECALCULER` (l'état que prend justement un amont invalidé en cascade) ni `ST_JAMAIS`
    pour un `CALCUL_SQLITE` qui n'a encore aucun service branché (`RESERVATIONS`/`MENAGES`
    aujourd'hui). Un `IMPORT_SQLITE` jamais alimenté (ex. `BANQUE` sans relevé importé) reste un état
    normal — l'orchestrateur ne fabrique jamais une donnée que personne ne lui a fournie — mais un
    `CALCUL_SQLITE` jamais produit signifie que la couche économique elle-même n'existe pas encore :
    le traiter comme un apport vide légitime serait exactement le résultat faux que ce garde-fou
    doit empêcher.

    Un IMPORT « à recalculer » ne bloque pas : il ne tire pas sa fraîcheur de l'amont, sa
    dernière extraction réussie reste en place, et rien ne le « recalcule » en aval. Cet état n'est
    que la trace d'un ancien marquage en cascade — qui gelait toute la chaîne indéfiniment, faute
    d'un parcours capable de le lever (Ménages bloqué par les tâches Hostaway, elles-mêmes marquées
    « à recalculer » par un échec de réservations pourtant réimportées depuis).
    """
    bloquants = []
    for a in dag.NOEUDS[dataset].depend_de:
        if not dag.NOEUDS[a].bloque_l_aval:
            continue    # source d'appoint : la dernière version valide reste en place
        statut = etats.get(a)
        if statut == ST_ECHEC:
            bloquants.append(a)
        elif statut == ST_A_RECALCULER and dag.NOEUDS[a].type_noeud != dag.TYPE_IMPORT:
            bloquants.append(a)
        elif statut == ST_JAMAIS and dag.NOEUDS[a].type_noeud == dag.TYPE_CALCUL:
            bloquants.append(a)
    return bloquants


# ── Parcours principal ──────────────────────────────────────────────────────────────────────────

def _import_externe_demande(dataset: str, cibles: list[str] | None,
                            inclure_imports_externes: bool) -> bool:
    """Un import externe ne part que s'il est DEMANDÉ : autorisé pour ce run ET désigné lui-même.

    Être le descendant d'une cible ne suffit pas. HOSTAWAY_CLEANING_TASKS dépend de HOSTAWAY_RAW :
    sans cette règle, chaque actualisation des réservations — donc chaque battement du scheduler —
    relançait aussi l'import des tâches de ménage, dont la cadence n'est pas arbitrée, et son échec
    bloquait ensuite Ménages et toute la chaîne économique en aval.

    Sur une actualisation GLOBALE (`cibles=None`), seuls les imports marqués
    `actualisation_globale` partent : ceux qui lisent le dépôt publié, sans appel d'API.
    """
    if not inclure_imports_externes:
        return False
    if cibles is None:
        return dag.NOEUDS[dataset].actualisation_globale
    return dataset in cibles


def _bloquants(dataset: str, etats: dict[str, str], cibles: list[str] | None,
               inclure_imports_externes: bool) -> list[str]:
    """`_amonts_en_echec`, sauf pour un import qui ne sera de toute façon pas tenté.

    Un import non demandé n'est ni exécuté ni invalidé : le dire « bloqué » compterait un échec
    pour une étape qui n'a jamais été voulue, et masquerait sa vraie raison (non déclenché).
    """
    noeud = dag.NOEUDS[dataset]
    if noeud.type_noeud == dag.TYPE_IMPORT and not (
            noeud.service and noeud.externe
            and _import_externe_demande(dataset, cibles, inclure_imports_externes)):
        return []
    return _amonts_en_echec(dataset, etats)


def _perimetre(cibles: list[str]) -> list[str]:
    """Cibles et descendants nécessaires, en ordre topologique — sans traverser un import externe
    atteint par propagation.

    Un tel import reste dans le périmètre (son étape « non déclenché » est visible), mais rien ne se
    propage à travers lui : il n'est pas exécuté, donc ses descendants ne reçoivent aucune donnée
    nouvelle de ce run. Un descendant atteignable par un autre chemin reste traité.
    """
    retenus: set[str] = set()
    a_voir = list(cibles)
    while a_voir:
        nom = a_voir.pop()
        if nom in retenus:
            continue
        retenus.add(nom)
        if nom not in cibles and dag.NOEUDS[nom].externe:
            continue
        a_voir.extend(dag.enfants(nom))
    return [n for n in dag.ordre_topologique() if n in retenus]


def dernier_detail_a_jour(dataset: str, *, db_path=None) -> dict[str, Any] | None:
    """Détail rendu par le service lors du dernier passage À JOUR de `dataset` (compteurs, version
    d'entrée), lu dans le journal des transitions.

    `orchestrateur_datasets.detail` ne le conserve pas : chaque passage EN_COURS ou ECHEC l'écrase.
    C'est pourtant ce qui permet à un import de dire si la donnée qu'il sert est celle sur laquelle
    l'aval a déjà été calculé.
    """
    if not _table_presente("orchestrateur_dataset_evenements", db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT detail FROM orchestrateur_dataset_evenements WHERE dataset = ? "
            "AND statut_apres = ? AND detail IS NOT NULL ORDER BY id DESC LIMIT 1",
            (dataset, ST_A_JOUR)).fetchone()
    finally:
        conn.close()
    if r is None:
        return None
    try:
        valeur = json.loads(r["detail"])
    except (TypeError, ValueError):
        return None
    return valeur if isinstance(valeur, dict) else None


def _invalider_enfants(dataset: str, perimetre: list[str], *, run_id: str, declencheur: str,
                       db_path) -> None:
    """`dataset` vient de CHANGER : ses enfants de calcul du périmètre cessent d'être « à jour »
    avant même d'être recalculés.

    Ils le seront dans la foulée — mais si le run s'interrompt entre-temps, un enfant resté A_JOUR
    sur l'ancienne entrée serait ensuite pris pour inchangé et conservé à tort (§15). Seuls les
    CALCULS avec service sont concernés : un import ne tire pas sa fraîcheur de l'amont, et un
    dataset sans service ne pourrait jamais revenir à jour.
    """
    for enfant in dag.enfants(dataset):
        noeud = dag.NOEUDS[enfant]
        if enfant in perimetre and noeud.type_noeud == dag.TYPE_CALCUL and noeud.service:
            marquer_dataset(enfant, ST_A_RECALCULER, run_id=run_id, declencheur=declencheur,
                            motif="INVALIDATION_AMONT", detail={"amont": dataset},
                            db_path=db_path)


def _integrity_ok(db_path) -> bool:
    cible = cfg.DB_PATH if db_path is None else db_path
    conn = sqlite3.connect(str(cible))
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def plan(cibles: list[str] | None = None, *, inclure_exports: bool = False) -> list[str]:
    """Étapes d'un run, dans l'ordre du DAG — la SEULE liste : l'écran, la route et la boucle
    d'exécution la lisent ici, personne ne la recopie."""
    a_traiter = _perimetre(cibles) if cibles else dag.ordre_topologique()
    if not inclure_exports:
        a_traiter = [n for n in a_traiter if dag.NOEUDS[n].type_noeud != dag.TYPE_EXPORT]
    return a_traiter


def preparer_actualisation_globale(*, declencheur: str = DECLENCHEUR_MANUEL,
                                   db_path=None) -> dict[str, Any]:
    """Ouvre et planifie « Actualiser toute l'activité » AVANT la tâche de fond.

    Le clic doit montrer tout de suite le plan (toutes les étapes « en attente ») : sans cela,
    l'écran rechargé avant que la tâche de fond n'ait écrit sa première ligne affichait encore le
    run précédent. Le verrou est pris ICI, sous l'identifiant du run : un double clic ou un second
    onglet est refusé sans créer de run fantôme.
    """
    marquer_runs_interrompus(db_path=db_path)
    run_id = _nouveau_run_id()
    verrou = prendre_verrou(PORTEE_GLOBALE, run_id, db_path=db_path)
    if not verrou["ok"]:
        return {"ok": False, "deja_en_cours": True, "run_id": verrou.get("run_id"),
                "code": verrou.get("code"), "message": "Une actualisation est déjà en cours."}
    _ouvrir_run(declencheur, "TOUT", db_path, run_id=run_id)
    _planifier(run_id, plan(None), sauvegarde=True, db_path=db_path)
    return {"ok": True, "run_id": run_id}


def _volume(dataset: str, resultat: dict, db_path) -> str | None:
    """Volume lisible d'une étape réussie (« 1 651 réservations ») — affichage seulement : une
    lecture qui échouerait ne doit jamais faire échouer le run."""
    try:
        from app.services import actualisation_progression_service as progression
        return progression.volume(dataset, resultat, db_path=db_path)
    except Exception:   # noqa: BLE001
        return None


def actualiser(*, cibles: list[str] | None = None, declencheur: str = DECLENCHEUR_MANUEL,
               inclure_exports: bool = False, inclure_imports_externes: bool = False,
               dry_run: bool = False, run_id: str | None = None,
               hostaway_a_la_demande: bool = False, mois: str | None = None,
               db_path=None) -> dict[str, Any]:
    """Actualise le pipeline : tout par défaut, ou les descendants des `cibles` demandées.

    `cibles=None` → « Actualiser toute l'activité » (§30).
    `cibles=[MENAGES]` → recalcule Ménages puis TOUS ses descendants nécessaires (§29), dans
    l'ordre du DAG.

    Un échec n'interrompt pas ce qui est réellement indépendant : les datasets dont aucun amont n'a
    échoué continuent d'être calculés, et le run se termine en PARTIEL (§30.5). Les datasets bloqués
    par un amont en échec sont explicitement rendus comme tels, jamais silencieusement ignorés.

    `dry_run=True` (mission industrialisation, Phase 8) : calcule le même plan d'exécution
    (dépendances, blocages, exclusions) mais n'appelle AUCUN service et n'active rien — chaque
    étape est rendue "IGNOREE (dry-run)". Sert à vérifier un DAG avant de l'exécuter pour de vrai.
    Aucune sauvegarde ni entrée `run_history` n'est créée : rien n'est risqué.

    Sur une actualisation GLOBALE réelle (`cibles=None`, `dry_run=False`), une sauvegarde de
    `app.db` est prise avant le premier dataset (Phase 4) et le run est journalisé dans
    `run_history` en parallèle de `moteur_runs`/`moteur_run_etapes` (registres existants, non
    remplacés). Si `PRAGMA integrity_check` échoue après le run — la seule panne qu'aucun état de
    dataset ne peut représenter honnêtement — la sauvegarde prise au départ est restaurée
    automatiquement et le run est marqué ROLLED_BACK dans `run_history`.
    """
    if cibles:
        inconnues = [c for c in cibles if c not in dag.NOEUDS]
        if inconnues:
            return {"ok": False, "code": E_DATASET_INCONNU, "datasets": inconnues,
                    "message": f"Dataset(s) inconnu(s) du DAG : {inconnues}"}
    a_traiter = plan(cibles, inclure_exports=inclure_exports)

    # `run_id` fourni : run préparé par l'écran (`preparer_actualisation_globale`), qui détient
    # déjà le verrou — le reprendre sous le même identifiant ne fait que prolonger le bail.
    if run_id is None:
        run_id = _ouvrir_run(declencheur, ",".join(cibles) if cibles else "TOUT", db_path)
    verrou = prendre_verrou(PORTEE_GLOBALE, run_id, db_path=db_path)
    if not verrou["ok"]:
        _cloturer_run(run_id, RUN_ECHEC, 0, 0, verrou["message"], db_path)
        return {"ok": False, "run_id": run_id, **verrou}
    global_reel = cibles is None and not dry_run
    if not dry_run:
        _planifier(run_id, a_traiter, sauvegarde=global_reel, db_path=db_path)

    # Phase 4 (industrialisation) : sauvegarde obligatoire avant une actualisation GLOBALE réelle —
    # jamais sur une cible unique (coût disproportionné pour un recalcul ciblé) ni en dry-run (rien
    # n'est risqué). Le run centralisé `run_history` est ouvert en parallèle de `moteur_runs` :
    # celui-ci reste la source de vérité détaillée, `run_history` sert la vue d'ensemble (§32).
    sauvegarde_id: str | None = None
    history_run_id: str | None = None
    if global_reel:
        debut_sauvegarde = _maintenant()
        _etape_debut(run_id, ETAPE_SAUVEGARDE, db_path)
        sauvegarde = backup_service.sauvegarder("ACTUALISATION_GLOBALE", db_path=db_path)
        _etape(run_id, ETAPE_SAUVEGARDE, 0, ETAPE_SUCCES if sauvegarde["ok"] else ETAPE_ECHEC,
               debut_sauvegarde, "" if sauvegarde["ok"] else "Sauvegarde impossible ou corrompue.",
               None, db_path, detail={"volume": f"Copie de sécurité {sauvegarde.get('sauvegarde_id')}"}
               if sauvegarde["ok"] else {"code": "E_SAUVEGARDE_ECHOUEE"})
        if not sauvegarde["ok"]:
            liberer_verrou(PORTEE_GLOBALE, run_id, db_path=db_path)
            _cloturer_run(run_id, RUN_ECHEC, 0, 0,
                         "Sauvegarde préalable impossible ou corrompue — actualisation refusée.",
                         db_path)
            return {"ok": False, "run_id": run_id, "code": "E_SAUVEGARDE_ECHOUEE",
                    "message": "Sauvegarde préalable impossible ou corrompue — actualisation "
                               "refusée."}
        sauvegarde_id = sauvegarde["sauvegarde_id"]
        history_run_id = history.demarrer("ACTUALISATION_GLOBALE", acteur=declencheur,
                                          sauvegarde_id=sauvegarde_id, db_path=db_path)
        history.marquer_validating(history_run_id, db_path=db_path)

    # `hostaway_a_la_demande` : le clic manuel « Actualiser toute l'activité » fait EXTRAIRE
    # Hostaway maintenant (pipeline GitHub canonique) au lieu de relire la dernière publication.
    # Le scheduler et les actualisations ciblées ne le demandent pas : leur comportement est
    # inchangé.
    jeton_options = _options_run.set({"hostaway_a_la_demande": bool(hostaway_a_la_demande),
                                      "mois": mois})
    etats: dict[str, str] = {d["dataset"]: d["statut"] for d in etat_datasets(db_path)}
    etapes: list[dict[str, Any]] = []
    nb_ok = nb_ko = 0
    # Datasets de ce run dont on SAIT qu'ils n'ont pas changé : un import qui l'a déclaré
    # (`donnees_modifiees=False`), ou un descendant conservé pour cette raison. Tout le reste est
    # présumé modifié — dans le doute, on recalcule.
    inchanges: set[str] = set()
    try:
        for ordre, dataset in enumerate(a_traiter, start=1):
            noeud = dag.NOEUDS[dataset]
            debut = _maintenant()

            if dry_run:
                # Plan uniquement : ni marquage de dataset, ni appel de service. Les blocages
                # (amont en échec, dataset sans service, import externe) restent visibles dans le
                # plan pour que l'utilisateur voie exactement ce qui SERAIT ignoré en réel.
                bloquants_dry = _bloquants(dataset, etats, cibles, inclure_imports_externes)
                if bloquants_dry:
                    motif = f"DRY-RUN : serait ignoré (amont en échec : {bloquants_dry})."
                elif not noeud.service:
                    motif = f"DRY-RUN : {noeud.libelle} — non recalculable ici."
                elif noeud.externe and not _import_externe_demande(dataset, cibles,
                                                                   inclure_imports_externes):
                    motif = f"DRY-RUN : {noeud.libelle} — import externe non déclenché."
                else:
                    motif = f"DRY-RUN : {noeud.libelle} — serait exécuté."
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path)
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                continue

            bloquants = _bloquants(dataset, etats, cibles, inclure_imports_externes)
            if bloquants:
                motif = (f"Amont(s) en échec : {bloquants}. Calcul non tenté — le faire sur une "
                         "entrée périmée produirait un résultat faux présenté comme frais.")
                if noeud.type_noeud != dag.TYPE_IMPORT:
                    # Un import n'est jamais invalidé en cascade : sa dernière extraction reste
                    # valable, et seul un nouvel import pourrait lever cet état — aucun recalcul.
                    marquer_dataset(dataset, ST_A_RECALCULER, run_id=run_id,
                                    declencheur=declencheur, erreur_code=E_AMONT_INDISPONIBLE,
                                    erreur_message=motif, motif="INVALIDATION_AMONT",
                                    db_path=db_path)
                    etats[dataset] = ST_A_RECALCULER
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path,
                       detail={"nature": NATURE_BLOQUEE, "amonts": bloquants})
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif,
                               "bloque": True})
                nb_ko += 1
                continue

            if not noeud.service:
                # Point d'entrée fourni de l'extérieur, ou chaîne pas entièrement migrée : on ne
                # fabrique rien, on constate. Le commentaire du DAG dit pourquoi.
                motif = f"{noeud.libelle} : non recalculable ici. {noeud.commentaire}"
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path,
                       detail={"nature": NATURE_SANS_SERVICE})
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                continue

            if noeud.externe and not _import_externe_demande(dataset, cibles,
                                                                   inclure_imports_externes):
                # Un import externe consomme un quota d'API et peut être limité (429) : il ne part
                # pas à chaque recalcul interne. L'ordonnanceur et le bouton dédié le demandent
                # explicitement.
                motif = (f"{noeud.libelle} : import externe non déclenché "
                         "(demander explicitement cette source pour l'actualiser).")
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path,
                       detail={"nature": NATURE_NON_DECLENCHEE})
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                continue

            amonts_du_run = [a for a in noeud.depend_de if a in a_traiter]
            if (cibles and dataset not in cibles and etats.get(dataset) == ST_A_JOUR
                    and amonts_du_run and all(a in inchanges for a in amonts_du_run)):
                # Aucun amont de ce run n'a changé : le jeu à jour a été calculé sur exactement ces
                # entrées, le recalculer rendrait le même résultat au prix de la chaîne entière
                # (§15). `_invalider_enfants` garantit qu'un dataset encore « à jour » ici l'est
                # vraiment. Réservé aux actualisations CIBLÉES : « Actualiser toute l'activité »
                # reste un recalcul complet, demandé comme tel.
                motif = (f"{noeud.libelle} : amont(s) inchangé(s) ({', '.join(amonts_du_run)}) "
                         "— recalcul inutile, jeu à jour conservé.")
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path,
                       detail={"nature": NATURE_INCHANGEE})
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                inchanges.add(dataset)
                continue

            _etape_debut(run_id, dataset, db_path)
            jeton_etape = _etape_courante.set((run_id, dataset, db_path))
            try:
                resultat = recalculer_dataset(dataset, run_id=run_id, declencheur=declencheur,
                                              db_path=db_path)
            finally:
                _etape_courante.reset(jeton_etape)
            sous_etapes = resultat.get("sous_etapes")
            if resultat.get("non_configure"):
                # Ni succès ni échec : la source n'a pas été interrogée, et l'écran le dit.
                etats[dataset] = resultat.get("statut_dataset", etats.get(dataset))
                message = resultat.get("message", "")
                _etape(run_id, dataset, ordre, ETAPE_NON_CONFIGUREE, debut, message, None,
                       db_path, detail={"code": E_NON_CONFIGURE})
                etapes.append({"dataset": dataset, "statut": ETAPE_NON_CONFIGUREE,
                               "motif": message})
                continue
            if resultat.get("ok"):
                nb_ok += 1
                etats[dataset] = ST_A_JOUR
                if resultat.get("donnees_modifiees") is False:
                    inchanges.add(dataset)
                else:
                    _invalider_enfants(dataset, a_traiter, run_id=run_id,
                                       declencheur=declencheur, db_path=db_path)
                _etape(run_id, dataset, ordre, "SUCCES", debut, "",
                       resultat.get("nb_constats") or resultat.get("nb_entetes"), db_path,
                       detail={"volume": _volume(dataset, resultat, db_path),
                               "sous_etapes": sous_etapes})
                etapes.append({"dataset": dataset, "statut": "SUCCES",
                               "detail": {k: v for k, v in resultat.items()
                                          if k not in ("ok", "dataset")}})
            else:
                nb_ko += 1
                etats[dataset] = ST_ECHEC
                message = resultat.get("message", "")
                _etape(run_id, dataset, ordre, "ECHEC", debut, message, None, db_path,
                       detail={"code": resultat.get("code"), "sous_etapes": sous_etapes})
                etapes.append({"dataset": dataset, "statut": "ECHEC",
                               "code": resultat.get("code"), "motif": message})

        if dry_run:
            statut = "DRY_RUN"
        elif nb_ko == 0:
            statut = RUN_SUCCES
        elif nb_ok == 0:
            statut = RUN_ECHEC
        else:
            statut = RUN_PARTIEL
        # Résumé = ce qui a empêché le calcul : échecs et blocages. Une étape normalement ignorée
        # (relevé bancaire ou référentiel importés par l'utilisateur, jeu à jour conservé) reste
        # dans le détail des étapes ; la mêler au résumé noyait la vraie cause sous dix lignes.
        resume = "; ".join(f"{e['dataset']}: {e.get('motif', '')}" for e in etapes
                           if e["statut"] == "ECHEC" or e.get("bloque")
                           or (dry_run and e["statut"] == "IGNOREE"))
        _cloturer_run(run_id, statut, nb_ok, nb_ko, resume, db_path)

        rollback: dict[str, Any] | None = None
        if history_run_id is not None:
            # Un dataset en échec (PARTIEL) n'est PAS une panne critique : les données précédentes
            # valides restent en place par construction (aucun dataset n'est jamais écrasé avant
            # que son propre calcul n'ait réussi). Seule une base réellement corrompue après le run
            # justifie une restauration — vérifiée explicitement, jamais devinée depuis `statut`.
            if not _integrity_ok(db_path):
                rollback = backup_service.restaurer(sauvegarde_id, confirmer=True, cible=db_path,
                                                    db_path=db_path)
                # `restaurer()` remplace tout le fichier cible — y compris la ligne `run_history`
                # de CE run, écrite avant la restauration. On rejournalise l'issue APRÈS coup, sur
                # le fichier qui subsiste réellement (même correctif que `migration_service.py`).
                if rollback["ok"]:
                    history_run_id = history.demarrer(
                        "ACTUALISATION_GLOBALE", sauvegarde_id=sauvegarde_id, db_path=db_path)
                history.marquer_rollback(
                    history_run_id,
                    erreur=f"integrity_check en échec après le run {run_id} — base restaurée "
                           f"depuis {sauvegarde_id}.",
                    db_path=db_path)
            elif statut == RUN_ECHEC:
                history.marquer_echec(history_run_id, erreur=resume, db_path=db_path)
            else:
                history.marquer_succes(history_run_id, db_path=db_path)

        return {"ok": statut in (RUN_SUCCES, RUN_PARTIEL, "DRY_RUN"), "run_id": run_id,
                "statut": statut, "nb_succes": nb_ok, "nb_echecs": nb_ko, "etapes": etapes,
                "datasets": etat_datasets(db_path), "sauvegarde_id": sauvegarde_id,
                "history_run_id": history_run_id, "rollback": rollback}
    finally:
        _options_run.reset(jeton_options)
        liberer_verrou(PORTEE_GLOBALE, run_id, db_path=db_path)


# ── Reprise après crash (§37) ───────────────────────────────────────────────────────────────────

def marquer_runs_interrompus(*, db_path=None) -> list[str]:
    """Un run resté EN_COURS alors que plus rien ne tourne est INTERROMPU, pas « en cours ».

    Appelée au démarrage de l'application et avant un nouveau run : sans cela, un crash laisserait
    un run éternellement ouvert, le verrou finirait par expirer mais l'écran continuerait d'afficher
    une actualisation fantôme.
    """
    if not (_table_presente("moteur_runs", db_path)
            and _table_presente("orchestrateur_verrous", db_path)):
        return []
    conn = get_db(db_path)
    try:
        ouverts = [r["run_id"] for r in conn.execute(
            "SELECT run_id FROM moteur_runs WHERE lot = ? AND statut = ?",
            (LOT_ORCHESTRATEUR, RUN_EN_COURS))]
        actifs = {r["run_id"] for r in conn.execute(
            "SELECT run_id FROM orchestrateur_verrous WHERE expire_le > ?", (_maintenant(),))}
        interrompus = [r for r in ouverts if r not in actifs]
        for run_id in interrompus:
            conn.execute(
                "UPDATE moteur_runs SET statut = ?, ended_at = ?, erreur_resume = ? "
                "WHERE run_id = ?",
                (RUN_INTERROMPU, _maintenant(),
                 "Run resté ouvert sans verrou actif : considéré interrompu.", run_id))
            _solder_etapes(conn, run_id, "Non exécutée : le run a été interrompu.")
        conn.commit()
    finally:
        conn.close()

    for run_id in interrompus:
        _marquer_datasets_du_run_interrompu(run_id, db_path)
    return interrompus


def _marquer_datasets_du_run_interrompu(run_id: str, db_path) -> None:
    """Un dataset laissé EN_COURS par un run interrompu redevient A_RECALCULER.

    Il n'est jamais présenté comme à jour : le calcul n'a pas prouvé sa réussite.
    """
    conn = get_db(db_path)
    try:
        datasets = [r["dataset"] for r in conn.execute(
            "SELECT dataset FROM orchestrateur_datasets WHERE source_run_id = ? AND statut = ?",
            (run_id, ST_EN_COURS))]
    finally:
        conn.close()
    for dataset in datasets:
        marquer_dataset(dataset, ST_A_RECALCULER, run_id=run_id,
                        erreur_code="RUN_INTERROMPU",
                        erreur_message="Recalcul interrompu : dataset non validé.",
                        motif="REPRISE_INTERROMPU", db_path=db_path)


# ── Lecture pour l'interface ────────────────────────────────────────────────────────────────────

def dernier_run(*, db_path=None) -> dict[str, Any] | None:
    if not _table_presente("moteur_runs", db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM moteur_runs WHERE lot = ? ORDER BY started_at DESC, id DESC LIMIT 1",
            (LOT_ORCHESTRATEUR,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def etapes_run(run_id: str, *, db_path=None) -> list[dict[str, Any]]:
    if not _table_presente("moteur_run_etapes", db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM moteur_run_etapes WHERE run_id = ? ORDER BY ordre, id", (run_id,))]
    finally:
        conn.close()


def historique(*, limite: int = 10, db_path=None) -> list[dict[str, Any]]:
    if not _table_presente("moteur_runs", db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM moteur_runs WHERE lot = ? ORDER BY started_at DESC, id DESC LIMIT ?",
            (LOT_ORCHESTRATEUR, limite))]
    finally:
        conn.close()


def etat_global(*, db_path=None) -> dict[str, Any]:
    """Tout ce que l'écran d'actualisation doit afficher, en une lecture."""
    run = dernier_run(db_path=db_path)
    return {
        "datasets": etat_datasets(db_path),
        "dernier_run": run,
        "etapes": etapes_run(run["run_id"], db_path=db_path) if run else [],
        "verrous": _verrous_actifs(db_path),
    }


def _verrous_actifs(db_path) -> list[dict[str, Any]]:
    if not _table_presente("orchestrateur_verrous", db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM orchestrateur_verrous WHERE expire_le > ?", (_maintenant(),))]
    finally:
        conn.close()

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
import json
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import orchestrateur_dag as dag

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
    """
    maintenant = datetime.now(timezone.utc)
    conn = get_db(db_path)
    try:
        existant = conn.execute(
            "SELECT run_id, detenu_par, expire_le FROM orchestrateur_verrous WHERE portee = ?",
            (portee,)).fetchone()
        if existant is not None and existant["expire_le"] > _horodatage(maintenant) \
                and existant["run_id"] != run_id:
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
        avant = conn.execute("SELECT statut FROM orchestrateur_datasets WHERE dataset = ?",
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

def _appeler_service(chemin: str, db_path) -> dict[str, Any]:
    """Résout "module:fonction" et l'appelle avec `db_path`.

    La résolution est tardive : le DAG reste une carte, et l'orchestrateur n'importe que ce qu'il
    exécute réellement.
    """
    module_nom, fonction_nom = chemin.split(":")
    fonction = getattr(importlib.import_module(module_nom), fonction_nom)
    resultat = fonction(db_path=db_path)
    return resultat if isinstance(resultat, dict) else {"ok": bool(resultat)}


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

    marquer_dataset(dataset, ST_EN_COURS, run_id=run_id, declencheur=declencheur, db_path=db_path)
    try:
        resultat = _appeler_service(noeud.service, db_path)
    except Exception as exc:   # noqa: BLE001 — toute panne doit devenir un état lisible
        marquer_dataset(dataset, ST_ECHEC, run_id=run_id, declencheur=declencheur,
                        erreur_code=type(exc).__name__, erreur_message=str(exc)[:500],
                        motif="ECHEC", db_path=db_path)
        return {"ok": False, "dataset": dataset, "code": type(exc).__name__,
                "message": str(exc)[:500]}

    if not resultat.get("ok", False):
        marquer_dataset(dataset, ST_ECHEC, run_id=run_id, declencheur=declencheur,
                        erreur_code=resultat.get("code", "ECHEC"),
                        erreur_message=resultat.get("message", ""), motif="ECHEC", db_path=db_path)
        return {"ok": False, "dataset": dataset, **resultat}

    marquer_dataset(dataset, ST_A_JOUR, run_id=run_id, declencheur=declencheur,
                    nb_lignes=_compter_lignes(noeud.tables, db_path), detail=resultat,
                    db_path=db_path)
    return {"ok": True, "dataset": dataset, **resultat}


# ── Runs (réutilise moteur_runs / moteur_run_etapes) ────────────────────────────────────────────

def _ouvrir_run(declencheur: str, cible: str, db_path) -> str:
    run_id = f"ORCH-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
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


def _etape(run_id: str, dataset: str, ordre: int, statut: str, debut: str,
           erreur: str, nb_ecrits: int | None, db_path) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO moteur_run_etapes (run_id, etape, ordre, started_at, ended_at, statut, "
            "nb_ecrits, erreur) VALUES (?,?,?,?,?,?,?,?)",
            (run_id, dataset, ordre, debut, _maintenant(), statut, nb_ecrits, erreur or None))
        conn.commit()
    finally:
        conn.close()


def _cloturer_run(run_id: str, statut: str, nb_ok: int, nb_ko: int, resume: str, db_path) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE moteur_runs SET ended_at = ?, statut = ?, nb_etapes = ?, nb_etapes_ok = ?, "
            "nb_etapes_ko = ?, erreur_resume = ? WHERE run_id = ?",
            (_maintenant(), statut, nb_ok + nb_ko, nb_ok, nb_ko, resume or None, run_id))
        conn.commit()
    finally:
        conn.close()


def _amonts_en_echec(dataset: str, etats: dict[str, str]) -> list[str]:
    """Amonts qui empêchent de calculer `dataset` sans produire un résultat faux."""
    return [a for a in dag.NOEUDS[dataset].depend_de
            if etats.get(a) in (ST_ECHEC,)]


# ── Parcours principal ──────────────────────────────────────────────────────────────────────────

def actualiser(*, cibles: list[str] | None = None, declencheur: str = DECLENCHEUR_MANUEL,
               inclure_exports: bool = False, inclure_imports_externes: bool = False,
               db_path=None) -> dict[str, Any]:
    """Actualise le pipeline : tout par défaut, ou les descendants des `cibles` demandées.

    `cibles=None` → « Actualiser toute l'activité » (§30).
    `cibles=[MENAGES]` → recalcule Ménages puis TOUS ses descendants nécessaires (§29), dans
    l'ordre du DAG.

    Un échec n'interrompt pas ce qui est réellement indépendant : les datasets dont aucun amont n'a
    échoué continuent d'être calculés, et le run se termine en PARTIEL (§30.5). Les datasets bloqués
    par un amont en échec sont explicitement rendus comme tels, jamais silencieusement ignorés.
    """
    if cibles:
        inconnues = [c for c in cibles if c not in dag.NOEUDS]
        if inconnues:
            return {"ok": False, "code": E_DATASET_INCONNU, "datasets": inconnues,
                    "message": f"Dataset(s) inconnu(s) du DAG : {inconnues}"}
        a_traiter: list[str] = []
        for cible in cibles:
            for nom in [cible, *dag.descendants(cible)]:
                if nom not in a_traiter:
                    a_traiter.append(nom)
        a_traiter = [n for n in dag.ordre_topologique() if n in a_traiter]
    else:
        a_traiter = dag.ordre_topologique()

    if not inclure_exports:
        a_traiter = [n for n in a_traiter if dag.NOEUDS[n].type_noeud != dag.TYPE_EXPORT]

    run_id = _ouvrir_run(declencheur, ",".join(cibles) if cibles else "TOUT", db_path)
    verrou = prendre_verrou(PORTEE_GLOBALE, run_id, db_path=db_path)
    if not verrou["ok"]:
        _cloturer_run(run_id, RUN_ECHEC, 0, 0, verrou["message"], db_path)
        return {"ok": False, "run_id": run_id, **verrou}

    etats: dict[str, str] = {d["dataset"]: d["statut"] for d in etat_datasets(db_path)}
    etapes: list[dict[str, Any]] = []
    nb_ok = nb_ko = 0
    try:
        for ordre, dataset in enumerate(a_traiter, start=1):
            noeud = dag.NOEUDS[dataset]
            debut = _maintenant()

            bloquants = _amonts_en_echec(dataset, etats)
            if bloquants:
                motif = (f"Amont(s) en échec : {bloquants}. Calcul non tenté — le faire sur une "
                         "entrée périmée produirait un résultat faux présenté comme frais.")
                marquer_dataset(dataset, ST_A_RECALCULER, run_id=run_id, declencheur=declencheur,
                                erreur_code=E_AMONT_INDISPONIBLE, erreur_message=motif,
                                motif="INVALIDATION_AMONT", db_path=db_path)
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path)
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                etats[dataset] = ST_A_RECALCULER
                nb_ko += 1
                continue

            if not noeud.service:
                # Point d'entrée fourni de l'extérieur, ou chaîne pas entièrement migrée : on ne
                # fabrique rien, on constate. Le commentaire du DAG dit pourquoi.
                motif = f"{noeud.libelle} : non recalculable ici. {noeud.commentaire}"
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path)
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                continue

            if noeud.externe and not inclure_imports_externes:
                # Un import externe consomme un quota d'API et peut être limité (429) : il ne part
                # pas à chaque recalcul interne. L'ordonnanceur et le bouton dédié le demandent
                # explicitement.
                motif = (f"{noeud.libelle} : import externe non déclenché "
                         "(demander explicitement cette source pour l'actualiser).")
                _etape(run_id, dataset, ordre, "IGNOREE", debut, motif, None, db_path)
                etapes.append({"dataset": dataset, "statut": "IGNOREE", "motif": motif})
                continue

            resultat = recalculer_dataset(dataset, run_id=run_id, declencheur=declencheur,
                                          db_path=db_path)
            if resultat.get("ok"):
                nb_ok += 1
                etats[dataset] = ST_A_JOUR
                _etape(run_id, dataset, ordre, "SUCCES", debut, "",
                       resultat.get("nb_constats") or resultat.get("nb_entetes"), db_path)
                etapes.append({"dataset": dataset, "statut": "SUCCES",
                               "detail": {k: v for k, v in resultat.items()
                                          if k not in ("ok", "dataset")}})
            else:
                nb_ko += 1
                etats[dataset] = ST_ECHEC
                message = resultat.get("message", "")
                _etape(run_id, dataset, ordre, "ECHEC", debut, message, None, db_path)
                etapes.append({"dataset": dataset, "statut": "ECHEC",
                               "code": resultat.get("code"), "motif": message})

        if nb_ko == 0:
            statut = RUN_SUCCES
        elif nb_ok == 0:
            statut = RUN_ECHEC
        else:
            statut = RUN_PARTIEL
        resume = "; ".join(f"{e['dataset']}: {e.get('motif', '')}" for e in etapes
                           if e["statut"] in ("ECHEC", "IGNOREE"))
        _cloturer_run(run_id, statut, nb_ok, nb_ko, resume, db_path)
        return {"ok": statut in (RUN_SUCCES, RUN_PARTIEL), "run_id": run_id, "statut": statut,
                "nb_succes": nb_ok, "nb_echecs": nb_ko, "etapes": etapes,
                "datasets": etat_datasets(db_path)}
    finally:
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

"""Actualisation Hostaway — point d'entrée unique.

    actualiser() → sous-processus Lot 1 → API → couche RAW SQLite

UN SEUL CHEMIN MÉTIER
Le bouton de l'interface et un futur déclenchement automatique appellent la MÊME fonction. Deux
implémentations divergeraient : l'une aurait un garde-fou, l'autre pas, et personne ne saurait
laquelle s'est exécutée. `actualiser()` ne prend aucun objet HTTP en paramètre, précisément pour
qu'un ordonnanceur puisse l'appeler tel quel.

L'API N'EST PAS RÉIMPLÉMENTÉE ICI
Ce service lance `lot1_hostaway_extract.py`, qui parle à Hostaway et écrit lui-même la couche RAW.
Réécrire l'appel API dans l'application créerait un second client, avec ses propres règles de
pagination et de nouvelle tentative — deux clients pour une seule API finissent toujours par se
comporter différemment sous limitation de débit.

PAS DE REQUÊTE WEB BLOQUÉE
Le sous-processus est lancé sans attendre sa fin. L'état se lit ensuite dans `moteur_runs` et
`hostaway_extractions`, qui sont déjà la source de vérité : aucun second système de tâches n'est
introduit, le journal de run EST le suivi.

CE QUE « FRAIS » VEUT DIRE
La fraîcheur vient du journal, jamais d'un fichier. Un classeur récent ne prouve pas une extraction
récente, et une extraction récente n'écrit plus forcément de classeur.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import run_history_service as history

SCRIPT = "lot1_hostaway_extract.py"

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"

ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"
ST_INTERROMPU = "INTERROMPU"

E_SCRIPT_ABSENT = "HOSTAWAY_SCRIPT_ABSENT"
E_INTERPRETEUR = "HOSTAWAY_INTERPRETEUR_ABSENT"
E_DEJA_EN_COURS = "HOSTAWAY_ACTUALISATION_EN_COURS"
E_LANCEMENT = "HOSTAWAY_LANCEMENT_IMPOSSIBLE"

MESSAGES = {
    E_SCRIPT_ABSENT: "Le moteur d'extraction Hostaway est introuvable sur cette installation.",
    E_INTERPRETEUR: ("Aucun interpréteur Python avec pandas n'est disponible : le moteur ne peut "
                     "pas être lancé."),
    E_DEJA_EN_COURS: ("Une actualisation Hostaway est déjà en cours. Attendez qu'elle se termine "
                      "avant d'en lancer une autre."),
    E_LANCEMENT: "Le moteur n'a pas pu être lancé.",
}

# Les tâches de ménage sont extraites par la même commande mais relèvent d'une autre chaîne, et leur
# endpoint limite le débit bien plus tôt. Les inclure ferait échouer une actualisation de
# réservations pour une raison qui ne la concerne pas.
#
# --sans-excel (mission 14 — activation réelle contrôlée) : la base SQLite est alimentée AVANT les
# classeurs legacy (cf. lot1_hostaway_extract.py, commentaire « ÉCRITURE SQLITE RAW (chemin normal)
# ») — rien en aval de l'application ne relit plus ces MASTER_*.xlsx (RESERVATIONS/MENAGES n'ont
# aucun `service` dans le DAG tant que lot4quater/lot6b/lot6c ne savent pas lire SQLITE, cf.
# `orchestrateur_dag.py`). Un run réel ou schedulé ne doit donc plus les écrire, même en parité :
# aligné sur le même flag déjà utilisé par `orchestrateur_moteur.executer_lot10`.
ARGUMENTS_DEFAUT = ("--skip-cleaning-tasks", "--sans-excel")

# Arguments du chemin CANONIQUE : les faits viennent du dépôt publié par le pipeline GitHub, qui
# détient les identifiants Hostaway. Aucun secret local n'est requis. Le moteur d'extraction est le
# même — seul son transport change (cf. `lot1_hostaway_extract.py`, constante `SOURCE_DEPOT`).
ARGUMENTS_DEPOT = ("--source", "DEPOT_GITHUB", "--skip-cleaning-tasks", "--sans-excel")

# Durée au-delà de laquelle un run encore ouvert n'est plus crédible. Une extraction complète
# s'exécute en quelques minutes (secondes depuis le dépôt) ; deux heures laissent une marge très
# large sans laisser un processus mort bloquer indéfiniment le bouton.
BAIL_RUN_S = 7200

_COLS_RUN = ("run_id", "lot", "started_at", "ended_at", "statut", "declencheur", "pid",
             "nb_etapes", "nb_etapes_ok", "nb_etapes_ko", "duree_s", "erreur_resume")
_COLS_ETAPE = ("etape", "ordre", "started_at", "ended_at", "statut", "nb_lus", "nb_ecrits",
               "position", "tentatives", "http_status", "erreur")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _table_presente(nom: str, *, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())
    finally:
        conn.close()


def _racine_moteur() -> Path:
    return Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


def _interpreteur() -> str | None:
    """Interpréteur portant pandas. Le moteur en dépend ; l'application peut en être dépourvue.

    Résolu à l'appel, jamais figé : une installation peut changer d'interpréteur sans redémarrer.
    """
    candidats = []
    if os.environ.get("PILOTAGE_ENGINE_PYTHON"):
        candidats.append(os.environ["PILOTAGE_ENGINE_PYTHON"])
    candidats += [sys.executable, r"C:\Program Files\Python312\python.exe"]
    for chemin in candidats:
        if not chemin or not Path(chemin).exists():
            continue
        try:
            r = subprocess.run([chemin, "-c", "import pandas"], capture_output=True, timeout=30)
        except Exception:
            continue
        if r.returncode == 0:
            return chemin
    return None


# ── Lancement ───────────────────────────────────────────────────────────────────────────────────

def _nom_machine() -> str:
    """Nom de cette machine, dans la MÊME forme que celle écrite au journal de run.

    `lib_run_journal` enregistre `socket.gethostname()[:100]` : reprendre exactement cette
    expression est ce qui garantit qu'un run local est reconnu comme local. Comparer deux
    conventions différentes ferait passer tous les runs pour distants, et le contrôle par PID ne
    s'appliquerait plus jamais.
    """
    import socket

    try:
        return socket.gethostname()[:100]
    except Exception:      # noqa: BLE001 — sans nom de machine, on ne conclura pas par le PID
        return ""


def _processus_vivant(pid: Any) -> bool | None:
    """Le processus existe-t-il encore ? `None` quand la question n'a pas de réponse fiable.

    Trois réponses, et la troisième compte autant que les deux autres : oui, non, et « je ne sais
    pas ». Un PID absent du journal, illisible, ou appartenant à une AUTRE machine ne se vérifie
    pas d'ici — et répondre « mort » par défaut tuerait un run réellement en cours sur un autre
    poste partageant la base.
    """
    if pid in (None, "", 0):
        return None
    try:
        numero = int(pid)
    except (TypeError, ValueError):
        return None
    if numero <= 0:
        return None
    try:
        if os.name == "nt":
            # `tasklist` plutôt que `os.kill(pid, 0)` : sous Windows, `os.kill` avec un signal 0
            # n'est pas un test d'existence — il termine le processus.
            sortie = subprocess.run(
                ["tasklist", "/FI", f"PID eq {numero}", "/NH"],
                capture_output=True, text=True, timeout=10)
            if sortie.returncode != 0:
                return None
            return str(numero) in sortie.stdout
        os.kill(numero, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Le processus existe mais appartient à quelqu'un d'autre : vivant.
        return True
    except Exception:      # noqa: BLE001 — outil indisponible : on ne sait pas, on le dit
        return None


def marquer_runs_interrompus(*, db_path=None, bail_s: int = BAIL_RUN_S) -> list[str]:
    """Requalifie en INTERROMPU les runs `EN_COURS` dont on peut ÉTABLIR qu'ils ne tournent plus.

    CE QUE CE CORRECTIF RÉPARE, ET COMMENT ON S'EN EST APERÇU
    Un run lancé le 2026-09-10 à 13h21 n'a jamais été clos : son sous-processus s'est arrêté sans
    écrire de statut. Deux jours plus tard, il était toujours `EN_COURS` en base, et comme
    `actualisation_en_cours()` refuse tout nouveau lancement tant qu'un run est ouvert, le bouton
    d'actualisation Hostaway était devenu DÉFINITIVEMENT inopérant — en répondant « une
    actualisation est déjà en cours », ce qui était faux et désignait la mauvaise cause.

    LE CONTRAT : TROIS ISSUES, ET AUCUNE PRÉSOMPTION DE MORT
      SUCCES / PARTIEL / ECHEC — le sous-processus a conclu lui-même ;
      INTERROMPU              — on a ÉTABLI qu'il ne tourne plus ;
      EN_COURS                — il tourne, ou on ne peut pas prouver le contraire.

    Un `EN_COURS` n'est donc JAMAIS présumé mort. Deux preuves sont acceptées, dans cet ordre :

      1. LE PID. Le journal porte le numéro de processus et le nom de la machine. Si le run a
         démarré sur CETTE machine et que ce PID n'existe plus, la conclusion est immédiate et
         certaine — inutile d'attendre le bail. Si le PID vit encore, le run est en cours, même
         s'il dépasse le bail : le tuer serait pire que l'attendre.
      2. LE TEMPS ÉCOULÉ, en dernier recours, quand le PID ne répond pas de la question (absent,
         illisible, ou enregistré sur une autre machine — on ne conclut jamais sur un processus
         qu'on ne peut pas voir). Au-delà du bail, un run n'est plus une hypothèse raisonnable.

    Le run n'est pas effacé : il est marqué INTERROMPU, avec la preuve retenue. Un échec doit
    rester visible — le faire disparaître empêcherait de comprendre pourquoi la donnée est ancienne.
    """
    if not _table_presente("moteur_runs", db_path=db_path):
        return []
    limite = (datetime.now(timezone.utc) - timedelta(seconds=bail_s)).strftime(
        "%Y-%m-%dT%H:%M:%S")
    machine = _nom_machine()
    conn = get_db(db_path)
    try:
        ouverts = list(conn.execute(
            "SELECT run_id, pid, hote, started_at FROM moteur_runs "
            "WHERE lot = 'lot1_hostaway_extract' AND statut = ?", (ST_EN_COURS,)))
        perimes: list[tuple[str, str]] = []
        for run in ouverts:
            run_id, pid, hote, debut = run[0], run[1], _txt(run[2]), _txt(run[3])
            vivant = _processus_vivant(pid) if (not hote or hote == machine) else None
            if vivant is True:
                continue                      # il tourne : on ne le touche pas, bail ou pas
            if vivant is False:
                perimes.append((run_id, f"Processus {pid} absent de cette machine : le run ne "
                                        "tourne plus."))
            elif debut and debut < limite:
                perimes.append((run_id, f"Run ouvert depuis plus de {bail_s // 60} minutes sans "
                                        "clôture, et son processus n'est pas vérifiable"
                                        + (f" (démarré sur « {hote} »)" if hote else "")
                                        + " : considéré interrompu."))
        for run_id, motif in perimes:
            conn.execute(
                "UPDATE moteur_runs SET statut = ?, ended_at = ?, erreur_resume = ? "
                "WHERE run_id = ?",
                (ST_INTERROMPU, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 motif, run_id))
        conn.commit()
    finally:
        conn.close()
    return [run_id for run_id, _ in perimes]


def actualisation_en_cours(*, db_path=None) -> dict[str, Any] | None:
    """Run Hostaway encore ouvert, s'il y en a un.

    Empêche deux extractions simultanées : elles écriraient deux extractions concurrentes et la
    dernière close deviendrait « la » courante, quel que soit son contenu.

    Les runs périmés sont d'abord requalifiés : sans cela, un processus mort bloquerait le bouton
    pour toujours, en prétendant qu'une extraction tourne encore.
    """
    if not _table_presente("moteur_runs", db_path=db_path):
        return None
    marquer_runs_interrompus(db_path=db_path)
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs "
            "WHERE lot = 'lot1_hostaway_extract' AND statut = ? "
            "ORDER BY started_at DESC LIMIT 1", (ST_EN_COURS,)).fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_RUN, r)) if r else None


def actualiser(*, declencheur: str = DECLENCHEUR_MANUEL, arguments: tuple[str, ...] = (),
               db_path=None, attendre: bool = False, timeout_s: int = 3600) -> dict[str, Any]:
    """Lance une actualisation Hostaway. Rend immédiatement, sauf `attendre=True`.

    Appelable sans contexte HTTP : c'est ce qui permettra à un ordonnanceur d'emprunter exactement ce
    chemin, sans seconde implémentation.

    `attendre` n'existe que pour les tests et un usage en ligne de commande : une requête web ne doit
    jamais rester bloquée sur une extraction, qui dure des minutes.
    """
    script = _racine_moteur() / SCRIPT
    if not script.exists():
        return {"ok": False, "code": E_SCRIPT_ABSENT, "message": MESSAGES[E_SCRIPT_ABSENT]}

    en_cours = actualisation_en_cours(db_path=db_path)
    if en_cours:
        return {"ok": False, "code": E_DEJA_EN_COURS, "message": MESSAGES[E_DEJA_EN_COURS],
                "run": en_cours}

    interpreteur = _interpreteur()
    if interpreteur is None:
        return {"ok": False, "code": E_INTERPRETEUR, "message": MESSAGES[E_INTERPRETEUR]}

    base = Path(db_path or cfg.DB_PATH)
    commande = [interpreteur, str(script), "--db", str(base),
                *(arguments or ARGUMENTS_DEFAUT)]

    # Environnement DÉRIVÉ : celui du serveur n'est jamais modifié. `PILOTAGE_DB_PATH` double
    # l'argument `--db` pour que les lots appelés en cascade visent la même base.
    env = dict(os.environ)
    env["PROJECT_ROOT"] = str(cfg.PROJECT_ROOT)
    env["PILOTAGE_DB_PATH"] = str(base)
    env["PYTHONIOENCODING"] = "utf-8"

    debut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # `run_history` (mission scheduler Hostaway) : uniquement sur le chemin SYNCHRONE
    # (`attendre=True`, celui qu'empruntent l'orchestrateur et l'ordonnanceur) — c'est le seul où
    # l'issue réelle (code_retour) est connue avant de répondre. Le bouton "fire-and-forget" reste
    # suivi par `moteur_runs`/`hostaway_extractions`, écrits par le sous-processus lui-même.
    history_run_id = history.demarrer("HOSTAWAY", acteur=declencheur, db_path=db_path) \
        if attendre else None

    try:
        if attendre:
            # `subprocess.run()` rend un `CompletedProcess` : contrairement à `Popen`, il n'expose
            # aucun `.pid` (mission 14b — le lire ici a fait planter le tout premier run réel,
            # APRÈS le succès effectif de l'extraction, en construisant seulement la valeur de
            # retour). `pid` reste donc `None` sur ce chemin — jamais un PID inventé.
            proc = subprocess.run(commande, cwd=str(cfg.PROJECT_ROOT), env=env,
                                  capture_output=True, text=True, timeout=timeout_s)
            code = proc.returncode
            pid = None
        else:
            proc = subprocess.Popen(commande, cwd=str(cfg.PROJECT_ROOT), env=env,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            code = None
            pid = proc.pid
    except Exception as exc:
        if history_run_id is not None:
            history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}",
                                  db_path=db_path)
        return {"ok": False, "code": E_LANCEMENT,
                "message": f"{MESSAGES[E_LANCEMENT]} ({type(exc).__name__})"}

    if history_run_id is not None:
        if code == 0:
            history.marquer_succes(history_run_id, db_path=db_path)
        else:
            history.marquer_echec(history_run_id, erreur=f"code_retour={code}", db_path=db_path)

    return {"ok": True, "lance_le": debut, "declencheur": declencheur, "pid": pid,
            "code_retour": code, "attendu": attendre, "etat": etat(db_path=db_path),
            "history_run_id": history_run_id}


# ── État ────────────────────────────────────────────────────────────────────────────────────────

def dernier_run(*, db_path=None) -> dict[str, Any] | None:
    """Dernier run d'extraction Hostaway, quel que soit son statut."""
    if not _table_presente("moteur_runs", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs "
            "WHERE lot = 'lot1_hostaway_extract' ORDER BY started_at DESC, id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_RUN, r)) if r else None


def etapes(run_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Étapes d'un run. C'est ce qui permet de dire « réservations oui, ménages non »."""
    if not run_id or not _table_presente("moteur_run_etapes", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_ETAPE, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_ETAPE)} FROM moteur_run_etapes "
            "WHERE run_id = ? ORDER BY ordre, id", (run_id,))]
    finally:
        conn.close()


def etat(*, db_path=None) -> dict[str, Any]:
    """État complet pour l'affichage : dernier run, ses étapes, la donnée qui en découle.

    Un run PARTIEL est rendu comme tel, avec le détail par étape. Résumer un run partiel en « à jour »
    laisserait croire que tout a été rafraîchi — c'est exactement la fraîcheur mensongère qu'on
    cherche à éviter.
    """
    from app.services import hostaway_raw_service as raw
    from app.services import reservations_dataset_service as ds

    run = dernier_run(db_path=db_path)
    resultat: dict[str, Any] = {
        "run": run,
        "etapes": etapes(_txt(run and run.get("run_id")), db_path=db_path) if run else [],
        "en_cours": bool(run and run["statut"] == ST_EN_COURS),
        "extraction": raw.fraicheur(db_path=db_path),
        "reservations": ds.fraicheur(db_path=db_path),
    }
    resultat["complet"] = bool(run and run["statut"] == ST_SUCCES)
    resultat["partiel"] = bool(run and run["statut"] == ST_PARTIEL)
    return resultat


def historique(*, limite: int = 10, db_path=None) -> list[dict[str, Any]]:
    if not _table_presente("moteur_runs", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_RUN, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs WHERE lot = 'lot1_hostaway_extract' "
            "ORDER BY started_at DESC, id DESC LIMIT ?", (limite,))]
    finally:
        conn.close()

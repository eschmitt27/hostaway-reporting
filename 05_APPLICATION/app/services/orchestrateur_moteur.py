"""Exécution d'un lot moteur pandas en mode SQLite, pour l'orchestrateur.

Certains calculs (Lot10) n'ont pas été réécrits en Python applicatif : ils restent des scripts
pandas de `02_TRAVAIL`, mais savent lire et écrire SQLite (`--source SQLITE --db <base>`). Les
réécrire ici produirait deux moteurs de calcul divergents — exactement ce que la migration a évité
partout ailleurs. L'orchestrateur les exécute donc tels quels, en sous-processus.

Le sous-processus est ATTENDU : l'orchestrateur doit connaître le résultat réel du calcul avant de
déclarer le dataset à jour. Un lancement en tâche de fond rendrait « succès » avant même que le
calcul ait commencé.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import app.config as cfg

TIMEOUT_DEFAUT_S = 1800

E_SCRIPT_ABSENT = "MOTEUR_SCRIPT_ABSENT"
E_INTERPRETEUR = "MOTEUR_INTERPRETEUR_ABSENT"
E_TIMEOUT = "MOTEUR_TIMEOUT"
E_CODE_RETOUR = "MOTEUR_CODE_RETOUR"


def _racine_moteur() -> Path:
    """Les scripts moteur du WORKTREE, à côté du paquet `app` — jamais ceux d'un autre arbre."""
    return Path(cfg.APP_ROOT).parent / "02_TRAVAIL"


def interpreteur_pandas() -> str | None:
    """Interpréteur disposant de pandas (l'application peut tourner sans)."""
    candidats = [os.environ.get("PILOTAGE_ENGINE_PYTHON"), sys.executable,
                 r"C:\Program Files\Python312\python.exe"]
    for chemin in candidats:
        if not chemin or not Path(chemin).exists():
            continue
        try:
            r = subprocess.run([chemin, "-c", "import pandas, openpyxl"],
                               capture_output=True, timeout=30)
        except Exception:
            continue
        if r.returncode == 0:
            return chemin
    return None


def executer(script: str, *, db_path=None, arguments: tuple[str, ...] = (),
             timeout_s: int = TIMEOUT_DEFAUT_S) -> dict[str, Any]:
    """Exécute un lot moteur en mode SQLite et rend un résultat exploitable par l'orchestrateur.

    Toute panne devient un dictionnaire `{"ok": False, "code", "message"}` — jamais une exception
    remontant jusqu'à l'écran, et jamais un « erreur de calcul » sans détail : le code retour et la
    fin de `stderr` sont conservés (§38).
    """
    chemin = _racine_moteur() / script
    if not chemin.exists():
        return {"ok": False, "code": E_SCRIPT_ABSENT,
                "message": f"Script moteur introuvable : {script}"}

    python = interpreteur_pandas()
    if python is None:
        return {"ok": False, "code": E_INTERPRETEUR,
                "message": "Aucun interpréteur avec pandas (définir PILOTAGE_ENGINE_PYTHON)."}

    base = Path(db_path or cfg.DB_PATH)
    commande = [python, str(chemin), "--source", "SQLITE", "--db", str(base), *arguments]

    # Environnement DÉRIVÉ : celui du serveur n'est jamais modifié.
    env = dict(os.environ)
    env["PROJECT_ROOT"] = str(cfg.PROJECT_ROOT)
    env["PILOTAGE_DB_PATH"] = str(base)
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        proc = subprocess.run(commande, cwd=str(_racine_moteur()), env=env,
                              capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {"ok": False, "code": E_TIMEOUT,
                "message": f"{script} : dépassement de {timeout_s}s."}
    except Exception as exc:   # noqa: BLE001
        return {"ok": False, "code": type(exc).__name__, "message": str(exc)[:400]}

    if proc.returncode != 0:
        return {"ok": False, "code": E_CODE_RETOUR,
                "message": f"{script} rc={proc.returncode} : {_cause(proc)}"}
    return {"ok": True, "script": script, "code_retour": proc.returncode}


_MARQUEURS_CAUSE = ("BLOQUANT", "ERROR", "Traceback", "Error:", "REFUS", "ABSENT", "MANQUANT",
                    "Exception")


def _cause(proc: subprocess.CompletedProcess) -> str:
    """Extrait la CAUSE d'un échec, pas les dernières lignes du journal.

    Les lots moteur écrivent beaucoup d'INFO ; prendre bêtement la fin de la sortie affiche un
    en-tête de log là où l'utilisateur attend la raison de l'échec. On privilégie donc les lignes
    portant un marqueur d'erreur, et on ne retombe sur la fin de sortie que s'il n'y en a aucune.
    """
    sortie = ((proc.stderr or "") + "\n" + (proc.stdout or "")).strip().splitlines()
    interessantes = [l.strip() for l in sortie
                     if any(m.lower() in l.lower() for m in _MARQUEURS_CAUSE)]
    retenues = interessantes[-3:] if interessantes else [l.strip() for l in sortie[-2:]]
    return " | ".join(retenues)[:400] or "aucune sortie"


def executer_lot10(*, db_path=None) -> dict[str, Any]:
    """Lot10 — résultats économiques, entrée `flux_unifies`, sorties 0044."""
    return executer("lot10_calculer_resultats.py", db_path=db_path,
                    arguments=("--sans-excel",))


def executer_lot4bis(*, db_path=None) -> dict[str, Any]:
    """Lot4bis — table commune des réservations (moteur S1-S7 inchangé, mission 14e).

    `--source-hostaway SQLITE` couvre aussi les référentiels et les réservations hors Hostaway
    (même argument, cf. `lot4bis_charger_reservations.py::main`) : explicite, refuse plutôt que
    de se rabattre sur un classeur si l'une des trois sources SQLite est indisponible.
    """
    return executer("lot4bis_charger_reservations.py", db_path=db_path,
                    arguments=("--source-hostaway", "SQLITE", "--sans-excel"))


def executer_lot4quater(*, db_path=None) -> dict[str, Any]:
    """Lot4quater — résolution mois ouvert/clos, sortie `reservations_resolues`."""
    return executer("lot4quater_resoudre_source_reservations.py", db_path=db_path,
                    arguments=("--source", "SQLITE", "--sans-excel"))


def executer_reservations(*, db_path=None) -> dict[str, Any]:
    """RESERVATIONS — chaîne complète lot4bis puis lot4quater (mission 14e).

    Séquentiel et fail-closed : lot4quater lit `reservations_calculees` (sortie de lot4bis), donc
    un échec de lot4bis ne doit jamais laisser croire que la résolution qui suit reflète des
    données fraîches.
    """
    resultat_bis = executer_lot4bis(db_path=db_path)
    if not resultat_bis.get("ok"):
        return {"ok": False, "code": resultat_bis.get("code", E_CODE_RETOUR),
                "message": f"lot4bis : {resultat_bis.get('message', '')}"}
    resultat_quater = executer_lot4quater(db_path=db_path)
    if not resultat_quater.get("ok"):
        return {"ok": False, "code": resultat_quater.get("code", E_CODE_RETOUR),
                "message": f"lot4quater : {resultat_quater.get('message', '')}"}
    return {"ok": True}


def _referentiel_intervenants_importe(chemin_base: Path) -> bool:
    """`ref_intervenants` non vide — connexion nue, jamais `get_db()` (ne force pas WAL)."""
    import sqlite3
    try:
        conn = sqlite3.connect(str(chemin_base))
        try:
            n = conn.execute("SELECT COUNT(*) FROM ref_intervenants").fetchone()[0]
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return n > 0


def executer_menages(*, db_path=None) -> dict[str, Any]:
    """MENAGES — lot6d puis lot6e puis lot6f, chaîne interne SQLite (mission 14e).

    lot6a (import Hostaway cleaning tasks) et lot6b (M04, Google Sheet) restent des imports
    externes optionnels — jamais rendus obligatoires ici, exactement comme BANQUE : une déclaration
    interne de ménage n'a pas besoin d'une tâche Hostaway pour exister économiquement. lot6c
    (ménages externes) reste hors chaîne : son rôle économique est déjà couvert en SQLite pur par
    `facture_menage_pdf_service` → `facture_lignes_menage`, que lot6d lit directement.

    Fail-closed uniquement sur ce qui est réellement obligatoire : le référentiel intervenants
    (`ref_intervenants`) doit avoir été importé, sinon lot6d tourne silencieusement sans aucune
    correspondance déclaration/intervenant — un recalcul « réussi » à zéro sens n'est pas un succès.
    """
    base = Path(db_path or cfg.DB_PATH)
    if not _referentiel_intervenants_importe(base):
        return {"ok": False, "code": "MENAGES_REFERENTIEL_ABSENT",
                "message": "ref_intervenants vide : référentiel jamais importé."}
    for script in ("lot6d_rapprochement_menages.py", "lot6e_gainperte_menages.py",
                   "lot6f_cout_complet_menages.py"):
        resultat = executer(script, db_path=db_path, arguments=("--source", "SQLITE", "--sans-excel"))
        if not resultat.get("ok"):
            return {"ok": False, "code": resultat.get("code", E_CODE_RETOUR),
                    "message": f"{script} : {resultat.get('message', '')}"}
    return {"ok": True}


def importer_hostaway(*, db_path=None) -> dict[str, Any]:
    """Import Hostaway pour l'orchestrateur — MÊME service que le bouton manuel et l'ordonnanceur.

    ATTENDU jusqu'au bout (`attendre=True`). `hostaway_actualisation_service.actualiser` rend la
    main dès le lancement quand on ne l'attend pas : l'orchestrateur marquerait alors le dataset
    « à jour » avant même que l'extraction ait commencé, et tout l'aval serait recalculé sur les
    données précédentes en croyant l'inverse.
    """
    from app.services import hostaway_actualisation_service as hostaway

    resultat = hostaway.actualiser(declencheur=hostaway.DECLENCHEUR_AUTO, db_path=db_path,
                                   attendre=True)
    if not resultat.get("ok"):
        return resultat
    # Le code retour du lot fait foi : un lancement réussi n'est pas une extraction réussie.
    code = resultat.get("code_retour")
    if code not in (0, None):
        return {"ok": False, "code": E_CODE_RETOUR,
                "message": f"lot1_hostaway_extract rc={code}"}
    return {"ok": True, **{k: v for k, v in resultat.items() if k != "ok"}}


def importer_hostaway_cleaning_tasks(*, db_path=None) -> dict[str, Any]:
    """Import Hostaway CleaningTasks pour l'orchestrateur — nœud `externe=True`, jamais entraîné par
    « Actualiser toute l'activité ». Déclenché uniquement par une demande explicite
    (`inclure_imports_externes=True`, ex. le bouton « Actualiser les ménages »)."""
    from app.services import hostaway_cleaning_tasks_actualisation_service as cleaning_tasks

    resultat = cleaning_tasks.actualiser(declencheur=cleaning_tasks.DECLENCHEUR_AUTO, db_path=db_path)
    if not resultat.get("ok"):
        return resultat
    return {"ok": True, **{k: v for k, v in resultat.items() if k != "ok"}}

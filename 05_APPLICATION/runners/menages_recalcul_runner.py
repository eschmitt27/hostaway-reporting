"""APP-2b — Runner de recalcul ménages. Exécuté HORS du processus FastAPI.

Ce fichier est un **script**, jamais un module importé par l'application. Il est lancé en
sous-processus avec l'interpréteur moteur (celui qui porte openpyxl), sur le modèle éprouvé des
runners `charges_post_write_runner` (APP-3b) et `lot4a_dryrun_runner` (APP-2c).

Particularité ménages : les lots 6d/6e ne sont **pas** des fonctions à chemins injectables mais des
scripts qui dérivent tous leurs chemins de `ROOT = dirname(dirname(abspath(__file__)))`. On ne les
importe donc pas : on les **exécute** dans un arbre miroir (le *workspace* copié) où `ROOT` résout
vers la copie. Le moteur lit et écrit exclusivement dans le workspace — aucun fichier réel n'est
touché. Ce runner n'importe jamais le moteur : il le lance en sous-processus.

Entrée : requete.json = {allowed_root, workspace, steps:[{name, script, args:[...], produces:[...]}],
                          timeout}.
`args` (optionnel) : arguments CLI supplémentaires (ex. `--source SQLITE --db <copie> --mois AAAA-MM
--sans-excel`) — mêmes scripts lot6d/6e/6b/6c, chemin d'entrée/sortie choisi par l'appelant.
Sortie : reponse.json = {ok, steps:[{name, statut, returncode, stdout_tail, stderr_tail,
                          produces:[{path, exists, sha256, size}]}]}.

Le code retour du processus ne porte JAMAIS l'échec métier (il est dans le JSON) : un run n'est
« SUCCES » que si chaque étape rend rc==0 ET produit tous ses fichiers attendus.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

OK = "OK"
ECHEC = "ECHEC"
TAIL = 4000  # caractères de queue conservés pour stdout/stderr (jamais le flux entier)


def _assert_under(path: Path, allowed_root: Path) -> Path:
    resolved = Path(path).resolve()
    root = Path(allowed_root).resolve()
    if resolved != root and root not in resolved.parents:
        raise RuntimeError(f"Chemin refusé hors workspace : {resolved}")
    return resolved


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _verifier_sorties(produces: list[str], workspace: Path, allowed_root: Path) -> list[dict[str, Any]]:
    resultats = []
    for rel in produces:
        p = _assert_under(workspace / rel, allowed_root)
        if p.exists() and p.is_file():
            resultats.append({"path": rel, "exists": True, "sha256": _sha256(p), "size": p.stat().st_size})
        else:
            resultats.append({"path": rel, "exists": False, "sha256": None, "size": None})
    return resultats


def _executer_etape(step: dict[str, Any], workspace: Path, allowed_root: Path, timeout: int) -> dict[str, Any]:
    name = step["name"]
    script = _assert_under(workspace / step["script"], allowed_root)
    if not script.exists():
        return {"name": name, "statut": ECHEC, "returncode": None,
                "erreur_code": "E_SCRIPT_ABSENT", "stdout_tail": "", "stderr_tail": "",
                "detail": f"Script absent du workspace : {step['script']}", "produces": []}

    cwd = script.parent  # <workspace>/02_TRAVAIL — ROOT du script = <workspace>
    debut = time.monotonic()
    try:
        proc = subprocess.run(
            [sys.executable, str(script), *[str(a) for a in step.get("args", [])]],
            cwd=str(cwd), capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return {"name": name, "statut": ECHEC, "returncode": None,
                "erreur_code": "E_TIMEOUT", "stdout_tail": (exc.stdout or "")[-TAIL:] if isinstance(exc.stdout, str) else "",
                "stderr_tail": "", "detail": f"Dépassement du délai ({timeout}s).", "produces": []}
    duree = round(time.monotonic() - debut, 2)

    produces = _verifier_sorties(step.get("produces", []), workspace, allowed_root)
    sorties_ok = all(p["exists"] for p in produces)
    if proc.returncode == 0 and sorties_ok:
        statut, erreur_code, detail = OK, None, "Étape terminée, sorties présentes."
    elif proc.returncode != 0:
        statut, erreur_code, detail = ECHEC, "E_RUNNER", f"Code retour {proc.returncode}."
    else:
        statut, erreur_code, detail = ECHEC, "E_SORTIE_ABSENTE", "Sortie attendue absente après exécution."

    return {
        "name": name, "statut": statut, "returncode": proc.returncode, "erreur_code": erreur_code,
        "detail": detail, "duree_secondes": duree,
        "stdout_tail": (proc.stdout or "")[-TAIL:], "stderr_tail": (proc.stderr or "")[-TAIL:],
        "produces": produces,
    }


def _main(input_path: str, output_path: str) -> int:
    requete = json.loads(Path(input_path).read_text(encoding="utf-8"))
    allowed_root = Path(requete["allowed_root"]).resolve()
    workspace = _assert_under(Path(requete["workspace"]), allowed_root)
    timeout = int(requete.get("timeout", 300))

    reponse: dict[str, Any] = {"ok": True, "workspace": str(workspace), "steps": []}
    for step in requete["steps"]:
        try:
            resultat = _executer_etape(step, workspace, allowed_root, timeout)
        except Exception as exc:  # garde de dernier recours — jamais de trace nue au client
            resultat = {"name": step.get("name"), "statut": ECHEC, "returncode": None,
                        "erreur_code": "E_ETAT_INCOHERENT", "detail": f"{type(exc).__name__}: {exc}",
                        "stdout_tail": "", "stderr_tail": "", "produces": []}
        reponse["steps"].append(resultat)
        if resultat["statut"] != OK:
            reponse["ok"] = False
            break  # bloquant : une étape échouée arrête la chaîne (comme run_menages_pipeline)

    Path(output_path).write_text(
        json.dumps(reponse, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return 0  # le code retour ne porte JAMAIS l'échec métier : il est dans la réponse JSON


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: menages_recalcul_runner.py requete.json reponse.json")
    raise SystemExit(_main(sys.argv[1], sys.argv[2]))

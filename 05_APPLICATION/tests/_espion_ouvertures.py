"""Espion d'ouvertures de fichiers pour les garde-fous anti-Excel.

POURQUOI UN AUDIT HOOK, ET PAS UNE LECTURE DU SOURCE
Un test qui cherche `load_workbook` dans le code se fait berner par un import indirect : il suffit
qu'une bibliothèque tierce ouvre le classeur pour qu'il ne voie rien. `sys.addaudithook` est posé
par CPython lui-même et voit TOUTES les ouvertures, y compris celles faites en C par `zipfile` sous
`openpyxl`. Une lecture de classeur ne peut pas lui échapper.

POURQUOI UN MODULE PARTAGÉ
Le script espion est un texte Python écrit dans un fichier, donc sensible à l'échappement. Le
dupliquer dans chaque fichier de test, c'est accepter qu'une des copies devienne silencieusement
inopérante — un espion cassé rend « aucune lecture détectée », ce qui ressemble exactement à un
succès. Il n'en existe donc qu'un seul exemplaire.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

EXTENSIONS_CLASSEUR = (".xlsx", ".xlsm", ".xls")

#: Chargé automatiquement par CPython au démarrage du sous-processus (mécanisme `site`).
SITECUSTOMIZE = '''\
import atexit, os, sys

_LOG = os.environ.get("AUDIT_OPEN_LOG")
if _LOG:
    _vus = []
    _reentrant = [False]

    def _hook(evenement, args):
        # Le hook ouvre lui-meme un fichier pour ecrire son journal : sans garde, il se
        # rappellerait indefiniment.
        if evenement != "open" or _reentrant[0]:
            return
        _reentrant[0] = True
        try:
            _vus.append("%s\\t%s" % (args[0], args[1]))
        except Exception:
            pass
        finally:
            _reentrant[0] = False

    def _vider():
        _reentrant[0] = True
        with open(_LOG, "w", encoding="utf-8", errors="replace") as fh:
            fh.write("\\n".join(str(x) for x in _vus))

    atexit.register(_vider)
    sys.addaudithook(_hook)
'''


def installer(dossier: Path) -> Path:
    """Écrit le sitecustomize espion et rend le dossier à mettre sur PYTHONPATH."""
    dossier.mkdir(parents=True, exist_ok=True)
    (dossier / "sitecustomize.py").write_text(SITECUSTOMIZE, encoding="utf-8")
    return dossier


def executer(commande: list[str], *, cwd: Path, atelier: Path,
             env_supplementaire: dict[str, str] | None = None,
             env_retire: tuple[str, ...] = (), timeout: int = 600):
    """Lance `commande` sous audit hook. Rend (process, [(chemin, mode), ...]).

    `atelier` accueille le sitecustomize et le journal : un dossier par appel, pour que deux runs
    d'un même test ne se relisent pas l'un l'autre.
    """
    atelier.mkdir(parents=True, exist_ok=True)
    espion = installer(atelier / "espion")
    journal = atelier / "ouvertures.txt"

    env = dict(os.environ)
    env["PYTHONPATH"] = str(espion)
    env["AUDIT_OPEN_LOG"] = str(journal)
    env["PYTHONIOENCODING"] = "utf-8"
    env.update(env_supplementaire or {})
    # Retirees APRES l'ajout : un test qui veut prouver qu'un moteur trouve sa base par `--db`
    # doit pouvoir garantir qu'il ne l'herite pas de la session.
    for cle in env_retire:
        env.pop(cle, None)

    proc = subprocess.run(commande, cwd=str(cwd), env=env, capture_output=True, text=True,
                          timeout=timeout)

    ouvertures: list[tuple[str, str]] = []
    if journal.exists():
        for ligne in journal.read_text(encoding="utf-8", errors="replace").splitlines():
            chemin, _, mode = ligne.partition("\t")
            ouvertures.append((chemin, mode))
    return proc, ouvertures


def classeurs(ouvertures, *, en_lecture: bool | None = None):
    """Ouvertures de classeur. `en_lecture=None` : toutes ; True : lectures ; False : écritures."""
    retenues = []
    for chemin, mode in ouvertures:
        if not chemin.lower().endswith(EXTENSIONS_CLASSEUR):
            continue
        if en_lecture is None:
            retenues.append((chemin, mode))
            continue
        lecture = ("r" in mode and "+" not in mode) if mode and mode != "None" else True
        if lecture is en_lecture:
            retenues.append((chemin, mode))
    return retenues

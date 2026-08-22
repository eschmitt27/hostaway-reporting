"""Garde-fou : aucun test ne doit écrire dans l'arbre réel du projet (mission §21-24).

POURQUOI CETTE GARDE EXISTE
Un test de fumée de l'orchestrateur a déclenché une extraction Hostaway RÉELLE et réécrit neuf
classeurs suivis par Git. Ils ont été restaurés et la cause corrigée, mais rien n'empêchait
STRUCTURELLEMENT que cela se reproduise : n'importe quel test résolvant un chemin réel et l'ouvrant
en écriture pouvait abîmer les données du projet, silencieusement.

CE QUI EST INTERDIT
Toute ouverture en ÉCRITURE d'un fichier situé sous l'arbre réel : masters calculés
(`02_TRAVAIL/`), sources brutes (`01_SOURCES_BRUTES/`, dont `REF_Setup.xlsm`), exports
opérationnels (`03_EXPORTS/`), données normalisées, et la vraie `app.db` (`05_APPLICATION/data/`).
Sont couverts : `open()`, `shutil.copy/copy2/copyfile/move`, `openpyxl.Workbook.save`,
`sqlite3.connect` en écriture.

CE QUI RESTE PERMIS
La LECTURE : de nombreux tests comparent légitimement à des données réelles (parité, reprise,
vérification d'empreinte). Et l'écriture partout ailleurs : `tmp_path`, un `APP_DATA_DIR` isolé,
le scratchpad. La garde ne gêne donc aucun test correctement isolé — elle ne se déclenche que là
où il y avait un vrai risque.
"""
from __future__ import annotations

from pathlib import Path

# Renseigné au démarrage de la session de tests par `pytest_configure`.
_RACINES_PROTEGEES: tuple[Path, ...] = ()

_MODES_ECRITURE = ("w", "a", "x", "+")


def initialiser() -> tuple[Path, ...]:
    """Calcule les racines à protéger depuis la configuration réelle de l'application."""
    global _RACINES_PROTEGEES
    import app.config as cfg

    candidats = [
        cfg.PROJECT_ROOT / "02_TRAVAIL",
        cfg.PROJECT_ROOT / "01_SOURCES_BRUTES",
        cfg.PROJECT_ROOT / "02_DONNEES_NORMALISEES",
        cfg.PROJECT_ROOT / "03_EXPORTS",
        Path(cfg.APP_ROOT) / "data",
    ]
    racines = []
    for c in candidats:
        try:
            racines.append(Path(c).resolve())
        except (OSError, ValueError):
            continue
    _RACINES_PROTEGEES = tuple(racines)
    return _RACINES_PROTEGEES


def est_protege(chemin) -> bool:
    """Vrai si `chemin` tombe sous une racine réelle protégée."""
    if not _RACINES_PROTEGEES:
        return False
    try:
        resolu = Path(chemin).resolve()
    except (OSError, ValueError, TypeError):
        return False
    return any(resolu == racine or racine in resolu.parents for racine in _RACINES_PROTEGEES)


def _refuser(quoi: str, chemin) -> None:
    raise AssertionError(
        f"{quoi} INTERDITE dans une source reelle du projet : {chemin}\n"
        "Un test ne doit jamais ecrire dans les masters, les sources brutes, les exports ou la "
        "base reelle. Utiliser tmp_path ou un APP_DATA_DIR isole."
    )


def armer(monkeypatch) -> None:
    """Installe la garde pour la durée d'un test."""
    import builtins
    import shutil
    import sqlite3

    open_original = builtins.open

    def open_garde(file, mode="r", *args, **kwargs):
        if any(m in str(mode) for m in _MODES_ECRITURE) and est_protege(file):
            _refuser("ECRITURE", file)
        return open_original(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", open_garde)

    for nom in ("copy", "copy2", "copyfile", "move"):
        original = getattr(shutil, nom, None)
        if original is None:
            continue

        def fabriquer(original=original, nom=nom):
            def garde(src, dst, *args, **kwargs):
                if est_protege(dst):
                    _refuser(f"COPIE ({nom})", dst)
                return original(src, dst, *args, **kwargs)
            return garde

        monkeypatch.setattr(shutil, nom, fabriquer())

    try:
        import openpyxl

        save_original = openpyxl.Workbook.save

        def save_garde(self, filename, *args, **kwargs):
            if est_protege(filename):
                _refuser("ECRITURE CLASSEUR", filename)
            return save_original(self, filename, *args, **kwargs)

        monkeypatch.setattr(openpyxl.Workbook, "save", save_garde)
    except Exception:
        # openpyxl absent d'un environnement de test minimal : la garde `open` couvre déjà le cas.
        pass

    connect_original = sqlite3.connect

    def connect_garde(database, *args, **kwargs):
        # `:memory:` et les URI ne désignent pas un fichier du projet.
        if isinstance(database, (str, Path)) and str(database) != ":memory:" \
                and est_protege(database):
            # La lecture reste permise (voir docstring du module) : on ouvre en lecture seule via
            # le mode URI `mode=ro`, que SQLite fait respecter lui-même — toute écriture ultérieure
            # sur cette connexion lève `sqlite3.OperationalError: attempt to write a readonly
            # database`. On ne bloque donc plus la connexion elle-même, seulement l'écriture.
            uri = f"file:{Path(database).resolve().as_posix()}?mode=ro"
            return connect_original(uri, *args, uri=True, **kwargs)
        return connect_original(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect_garde)

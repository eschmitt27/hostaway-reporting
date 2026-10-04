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
# Dossier de données RÉEL (`APP_DATA_DIR` déclaré dans le `.env`) : là, même la LECTURE est refusée.
# Un test n'a aucune raison de lire la vraie base ; s'il le fait, il en dépend, et il cesserait
# d'être reproductible le jour où cette base change.
_DONNEES_REELLES: Path | None = None

_MODES_ECRITURE = ("w", "a", "x", "+")


def initialiser() -> tuple[Path, ...]:
    """Calcule les racines à protéger depuis la configuration réelle de l'application."""
    global _RACINES_PROTEGEES, _DONNEES_REELLES
    import app.config as cfg
    from app import environnement

    _DONNEES_REELLES = environnement.data_dir_reel()

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


def est_donnee_reelle(chemin) -> bool:
    """Vrai si `chemin` tombe sous le dossier de données réel de l'exploitant."""
    if _DONNEES_REELLES is None:
        return False
    try:
        resolu = Path(chemin).resolve()
    except (OSError, ValueError, TypeError):
        return False
    return resolu == _DONNEES_REELLES or _DONNEES_REELLES in resolu.parents


def _refuser_reel(quoi: str) -> None:
    # Le chemin n'est pas recopié : c'est celui de la machine de l'exploitant.
    raise AssertionError(
        f"{quoi} INTERDITE dans le dossier de donnees REEL (APP_DATA_DIR du .env) : un test "
        "n'utilise jamais la vraie base. Utiliser tmp_db, tmp_path ou un APP_DATA_DIR isole."
    )


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
        if isinstance(file, (str, Path)) and est_donnee_reelle(file):
            _refuser_reel("OUVERTURE")
        if any(m in str(mode) for m in _MODES_ECRITURE) and est_protege(file):
            _refuser("ECRITURE", file)
        return open_original(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", open_garde)
    # `pathlib` (write_text, write_bytes, open) passe par `io.open`, pas par `builtins.open` : c'est
    # par là que des centaines de manifestes de dry-run ont atterri dans `05_APPLICATION/data/`
    # sans que cette garde les voie.
    import io
    import os

    monkeypatch.setattr(io, "open", open_garde)

    mkdir_original = os.mkdir

    def mkdir_garde(path, *args, **kwargs):
        # Un dossier DÉJÀ présent n'est pas une écriture (`mkdir(exist_ok=True)` reste neutre).
        if est_protege(path) and not os.path.isdir(path):
            _refuser("CREATION DE DOSSIER", path)
        if est_donnee_reelle(path):
            _refuser_reel("CREATION DE DOSSIER")
        return mkdir_original(path, *args, **kwargs)

    monkeypatch.setattr(os, "mkdir", mkdir_garde)

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
        if isinstance(database, (str, Path)) and str(database) != ":memory:":
            nu = str(database)
            if nu.startswith("file:"):
                nu = nu[5:].split("?", 1)[0]
            if est_donnee_reelle(nu):
                _refuser_reel("CONNEXION SQLITE")
        # `:memory:` et les URI ne désignent pas un fichier du projet.
        cible = str(database) if isinstance(database, (str, Path)) else ""
        if cible.startswith("file:"):
            cible = cible[5:].split("?", 1)[0]
        if cible and cible != ":memory:" and est_protege(cible):
            # La lecture reste permise (voir docstring du module) : on ouvre en lecture seule via
            # le mode URI `mode=ro`, que SQLite fait respecter lui-même — toute écriture ultérieure
            # sur cette connexion lève `sqlite3.OperationalError: attempt to write a readonly
            # database`. On ne bloque donc plus la connexion elle-même, seulement l'écriture.
            # `immutable=1` en plus : en WAL, même une lecture `mode=ro` met à jour le fichier
            # d'index `-shm` à côté de la base — une écriture tout de même, dans un dossier réel.
            uri = f"file:{Path(cible).resolve().as_posix()}?mode=ro&immutable=1"
            kwargs.pop("uri", None)
            return connect_original(uri, *args, uri=True, **kwargs)
        return connect_original(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect_garde)

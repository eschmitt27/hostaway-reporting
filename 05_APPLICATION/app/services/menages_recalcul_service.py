"""APP-2b — Service de recalcul ménages (sécurisé, sur copies), SQLite-first.

Ce service orchestre un cycle de recalcul CONTRÔLÉ du rapprochement ménages, sans jamais importer
le moteur (interdiction `test_no_import_of_travail_modules`) et sans jamais toucher la vraie base
app.db ni un fichier métier réel.

Deux modes (cf. `config.MENAGES_REAL_RECALC_ENABLED`) :

  MODE_COPIES  — recette isolée. On copie la base SQLite réelle dans un workspace sous `data/`
                 (jamais ouverte en écriture), on y exécute lot6d puis lot6e en `--source SQLITE`
                 sur cette copie, et on compare l'état produit à l'état réel. **Aucune écriture sur
                 app.db réelle.** Toujours autorisé.

  MODE_REEL    — écrirait directement dans app.db réelle. **Bloqué** tant que le flag est False :
                 `confirmer(MODE_REEL)` refuse et trace un run BLOQUE, sans rien exécuter.

Garde-fous repris de la chaîne charges (APP-3b) :
  * verrou interprocessus atomique (fichier de verrou dédié), libéré dans un `finally` quoi qu'il arrive ;
  * snapshot horodaté + manifeste sha256 de la base réelle AVANT tout (jamais un fichier Excel) ;
  * un run n'est SUCCES que si chaque étape rend rc==0 ET produit ses lignes dans la copie ;
  * vérification que la base RÉELLE est bits-pour-bits identique avant/après.

PRÉCONDITION DATASET, PAS FICHIER
Avant ce module, `preparer()`/`confirmer()` vérifiaient l'existence de classeurs Excel
(MASTER_FACT_HA_CleaningTasks_Discovery.xlsx, etc.). lot6d/lot6e lisent désormais ces mêmes données
depuis SQLite (`menages_taches_enrichies`/`menages_declarations_internes`/`facture_lignes_menage`,
migrations 0038/0037/0039) via `--source SQLITE` : la précondition est donc « le dataset a des
lignes pour ce mois », jamais « le fichier existe ». Un dataset absent rend un état explicite
(`DATASET_NON_INITIALISE`), jamais une `FileNotFoundError`.

lot6f (coût complet) reste **exclu** du recalcul sur copies : il lit encore la Google Sheet (réseau)
via `menages_declarations_internes` en SQLite — déterministe une fois la déclaration importée — mais
la mécanique de lavage n'est pas couverte par ce recalcul volontairement restreint à 6d/6e (portée
inchangée depuis la version Excel de ce service).
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import snapshot_service
from app.services import saisie_charges_lock_service as verrou_lib  # verrou interprocessus générique
from app.services.audit_service import log_event

MODE_COPIES = "COPIES"
MODE_REEL = "REEL"
LOCK_NAME = ".menages_recalcul.lock"
OPERATION = "menages_recalcul"

# Étapes exécutées sur copies — hors ligne, déterministes. lot6f exclu (cf. docstring).
STEPS_COPIES: list[dict[str, Any]] = [
    {"name": "lot6d_rapprochement", "script": "02_TRAVAIL/lot6d_rapprochement_menages.py"},
    {"name": "lot6e_gainperte", "script": "02_TRAVAIL/lot6e_gainperte_menages.py"},
]

# Tables SQLite requises en ENTRÉE de lot6d/6e en mode --source SQLITE (0029/0038/0037/0039,
# toutes déjà migrées) — remplace les anciens SOURCES_A_COPIER (classeurs Excel).
DATASETS_REQUIS = ("menages_taches_enrichies", "menages_declarations_internes")

# Scripts moteur copiés (exécutés dans le workspace ; ROOT y résout).
# `lib_db_moteur`/`lib_ref_history` : lot6d/6e les importent en mode --source SQLITE (résolution
# proprietaire_id par période, cf. lot6d) — sans la copie, l'import échoue et le script sort en
# rc=1 avant même d'atteindre son propre code.
SCRIPTS_A_COPIER = [
    "02_TRAVAIL/lot6d_rapprochement_menages.py",
    "02_TRAVAIL/lot6e_gainperte_menages.py",
    "02_TRAVAIL/lib_db_moteur.py",
    "02_TRAVAIL/lib_ref_history.py",
]

# Tables SQLite produites (pour comparaison avant/après) — remplace les anciens classeurs Excel.
SORTIES_TABLES = {
    "rapprochement": "menages_rapprochement",
    "gainperte": "menages_gainperte",
}

STATUT_SUCCES = "SUCCES"
STATUT_ECHEC = "ECHEC"
STATUT_PARTIEL = "PARTIEL"
STATUT_VERROUILLE = "VERROUILLE"
STATUT_BLOQUE = "BLOQUE"

E_DATASET_NON_INITIALISE = "E_DATASET_NON_INITIALISE"


# ── Chemins ──────────────────────────────────────────────────────────────────

def _project_root() -> Path:
    return Path(cfg.PROJECT_ROOT)


def _lock_path() -> Path:
    return Path(cfg.DATA_DIR) / LOCK_NAME


def _sha256(path: Path) -> str:
    return snapshot_service._sha256(path)  # même implémentation, une seule source de vérité


def _git_head() -> str | None:
    try:
        r = subprocess.run(
            ["git", "-C", str(_project_root()), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() or None if r.returncode == 0 else None
    except Exception:
        return None


# ── Détection Excel ouvert (conservée : utilisée par menages_chaine_service) ──

def _fichier_office_verrou(path: Path) -> Path:
    return path.parent / ("~$" + path.name)


def detecter_excel_ouvert(paths: list[Path]) -> list[str]:
    """Retourne les noms des fichiers dont un verrou Office (~$) existe — donc probablement ouverts."""
    ouverts = []
    for p in paths:
        p = Path(p)
        if _fichier_office_verrou(p).exists():
            ouverts.append(p.name)
    return ouverts


# ── Base réelle : copie sûre (jamais une écriture), datasets, fraîcheur ──────

_TABLES_DOMAINE = ("menages_taches_enrichies", "menages_declarations_internes",
                   "menages_rapprochement", "menages_gainperte")


def _empreinte_domaine(db_path: Path) -> str:
    """Empreinte du contenu des tables DOMAINE (jamais des tables journal — `snapshots`/
    `menages_recalcul_runs`/`audit_events` s'écrivent normalement dans la même base réelle à
    chaque run, ce n'est pas une modification à détecter). Un sha256 du fichier entier donnerait
    toujours un écart, y compris quand rien de métier n'a changé — cette empreinte est ciblée."""
    import hashlib
    conn = get_db(db_path)
    try:
        h = hashlib.sha256()
        for table in _TABLES_DOMAINE:
            if not conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone():
                continue
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"):
                h.update("|".join(str(v) for v in row).encode("utf-8", "replace"))
                h.update(b"\n")
        return h.hexdigest()
    finally:
        conn.close()


def _copier_base(source: Path, destination: Path) -> None:
    """Copie une base SQLite via l'API backup (sûre même si WAL/journal actifs) — jamais un
    `shutil.copy2` brut, qui peut copier un fichier incohérent si la base est en écriture."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(str(source))
    try:
        dst_conn = sqlite3.connect(str(destination))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


def _mois_disponibles(db_path: Path) -> list[str]:
    """Mois présents dans `menages_taches_enrichies`, du plus récent au plus ancien."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='menages_taches_enrichies'"
        ).fetchone():
            return []
        rows = conn.execute(
            "SELECT DISTINCT mois FROM menages_taches_enrichies "
            "WHERE mois IS NOT NULL ORDER BY mois DESC").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _dernier_mois(db_path: Path) -> str | None:
    mois = _mois_disponibles(db_path)
    return mois[0] if mois else None


def _dataset_dispo(db_path: Path, table: str, mois: str) -> bool:
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone():
            return False
        return conn.execute(
            f"SELECT 1 FROM {table} WHERE mois = ? LIMIT 1", (mois,)).fetchone() is not None
    finally:
        conn.close()


def _lire_comparaison(db_path: Path, mois: str) -> dict[str, Any] | None:
    """Compte lignes / statuts / écarts de `menages_rapprochement` pour un mois — équivalent SQLite
    de l'ancienne lecture du classeur TABLEAU_COMPARAISON."""
    if not db_path or not Path(db_path).exists():
        return None
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='menages_rapprochement'"
        ).fetchone():
            return None
        rows = conn.execute(
            "SELECT statut_controle, ecart FROM menages_rapprochement WHERE mois = ?",
            (mois,)).fetchall()
    finally:
        conn.close()
    statuts: dict[str, int] = {}
    nb_ecarts = 0
    for statut_controle, ecart in rows:
        st = str(statut_controle or "")
        statuts[st] = statuts.get(st, 0) + 1
        try:
            if int(ecart or 0) != 0:
                nb_ecarts += 1
        except (TypeError, ValueError):
            pass
    return {"nb_lignes": len(rows), "statuts": statuts, "nb_ecarts": nb_ecarts}


# ── Préparation (préflight, aucune mutation) ─────────────────────────────────

def preparer(mode: str = MODE_COPIES, mois: str | None = None) -> dict[str, Any]:
    """Diagnostic préalable. Ne lance rien, n'écrit rien. Backe l'écran de préparation."""
    db_reel = Path(cfg.DB_PATH)
    mois_defaut = _dernier_mois(db_reel)
    mois = mois or mois_defaut

    datasets = [{"table": t, "present": bool(mois) and _dataset_dispo(db_reel, t, mois)}
               for t in DATASETS_REQUIS]
    datasets_absents = [d["table"] for d in datasets if not d["present"]]

    etat_verrou = verrou_lib.inspecter_verrou(_lock_path())

    bloque = False
    raison = None
    if mode == MODE_REEL and not cfg.MENAGES_REAL_RECALC_ENABLED:
        bloque = True
        raison = ("Le mode RÉEL est désactivé (MENAGES_REAL_RECALC_ENABLED = False). "
                  "Seule la recette sur copies est exécutable : elle n'écrit jamais dans app.db réelle.")

    warnings: list[str] = []
    if not mois:
        warnings.append("Aucun mois disponible : le dataset Hostaway ménages (Lot6a) n'a encore "
                        "produit aucune ligne (DATASET_NON_INITIALISE).")
    if datasets_absents:
        warnings.append("Dataset(s) SQLite absent(s) pour ce mois : " + ", ".join(datasets_absents) + ".")
    if etat_verrou["etat"] != verrou_lib.ETAT_ABSENT:
        warnings.append("Un verrou de recalcul est présent (" + etat_verrou["etat"] + ") : "
                        + etat_verrou["raison"])

    prete = (not bloque) and bool(mois) and (not datasets_absents) \
        and etat_verrou["etat"] == verrou_lib.ETAT_ABSENT

    return {
        "mode": mode,
        "mois": mois,
        "mois_disponibles": _mois_disponibles(db_reel),
        "datasets": datasets,
        "etapes": [s["name"] for s in STEPS_COPIES],
        "verrou": etat_verrou,
        "datasets_absents": datasets_absents,
        "reel_active": bool(cfg.MENAGES_REAL_RECALC_ENABLED),
        "bloque": bloque,
        "raison": raison,
        "prete": prete,
        "warnings": warnings,
    }


# ── Fraîcheur : sources plus récentes que le dernier rapprochement ? ─────────

def etat_fraicheur() -> dict[str, Any]:
    """Indicateur (non comptable) : une source a-t-elle été recalculée après le dernier
    rapprochement ? Comparaison de `date_calcul` (colonnes 0038), jamais un mtime de fichier."""
    db_reel = Path(cfg.DB_PATH)
    conn = get_db(db_reel)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='menages_rapprochement'"
        ).fetchone():
            return {"etat": "JAMAIS_CALCULE", "sources_recentes": [],
                    "master_maj": None, "recommander_relance": False,
                    "libelle": "Jamais calculé"}
        rapp_maj = conn.execute(
            "SELECT MAX(date_calcul) FROM menages_rapprochement").fetchone()[0]
        if not rapp_maj:
            return {"etat": "JAMAIS_CALCULE", "sources_recentes": [],
                    "master_maj": None, "recommander_relance": False,
                    "libelle": "Jamais calculé"}
        recentes = []
        for table in DATASETS_REQUIS:
            if not conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone():
                continue
            maj = conn.execute(f"SELECT MAX(date_calcul) FROM {table}").fetchone()[0]
            if maj and maj > rapp_maj:
                recentes.append(table)
    finally:
        conn.close()
    return {
        "etat": "SOURCES_PLUS_RECENTES" if recentes else "A_JOUR",
        "sources_recentes": recentes,
        "master_maj": rapp_maj,
        "recommander_relance": bool(recentes),
        "libelle": "Sources modifiées depuis le dernier rapprochement" if recentes else "À jour",
    }


# ── Construction du workspace (copies) ───────────────────────────────────────

def _construire_workspace(run_ts: str, db_reel: Path) -> tuple[Path, Path]:
    """Miroir minimal : scripts moteur + COPIE de `db_reel` (jamais ouverte en écriture).

    Retourne (workspace, chemin_base_copie). `db_reel` est explicite — jamais `cfg.DB_PATH` relu
    ici : `confirmer()` peut recevoir un `db_path` différent (tests, isolation A/B), et une
    résolution interne divergente copierait la mauvaise base sans qu'aucune erreur ne le signale.
    """
    base = Path(cfg.MENAGES_RECALC_WORKSPACE)
    workspace = base / run_ts
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    root = _project_root()
    for rel in SCRIPTS_A_COPIER:
        src = root / rel
        dst = workspace / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    db_copie = workspace / "app_data.db"
    _copier_base(db_reel, db_copie)
    return workspace, db_copie


# ── Enregistrement d'un run ──────────────────────────────────────────────────

def _enregistrer_run(db_path: Path, **champs: Any) -> int:
    conn = get_db(db_path)
    try:
        cols = ", ".join(champs.keys())
        placeholders = ", ".join("?" for _ in champs)
        cur = conn.execute(
            f"INSERT INTO menages_recalcul_runs ({cols}) VALUES ({placeholders})",
            tuple(champs.values()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


# ── Confirmation / exécution ─────────────────────────────────────────────────

def confirmer(mode: str = MODE_COPIES, mois: str | None = None,
              db_path: Path | None = None) -> dict[str, Any]:
    """Exécute le recalcul. MODE_COPIES seulement ; MODE_REEL refusé tant que le flag est False."""
    db_path = db_path or cfg.DB_PATH          # lu À CHAUD — jamais le défaut figé à l'import (défaut get_db)
    db_reel = Path(db_path)
    mois = mois or _dernier_mois(db_reel)

    # ── Garde mode réel ──────────────────────────────────────────────────────
    if mode == MODE_REEL and not cfg.MENAGES_REAL_RECALC_ENABLED:
        run_id = _enregistrer_run(
            db_path, mode=MODE_REEL, periode=mois, statut=STATUT_BLOQUE,
            git_head=_git_head(), erreur_code="E_MODE_REEL_DESACTIVE",
            erreur_resume="MENAGES_REAL_RECALC_ENABLED=False — aucune exécution.",
        )
        log_event("MENAGE_RECALC_BLOQUE", {"mode": MODE_REEL, "mois": mois}, db_path=db_path)
        return {"ok": False, "run_id": run_id, "statut": STATUT_BLOQUE,
                "erreur_code": "E_MODE_REEL_DESACTIVE",
                "message": "Recalcul réel désactivé. app.db réelle n'a pas été touchée."}

    if mode not in (MODE_COPIES, MODE_REEL):
        raise ValueError(f"Mode inconnu : {mode}")

    # ── Préflight bloquant (dataset non initialisé) ──────────────────────────
    if not mois:
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=None, statut=STATUT_ECHEC, git_head=_git_head(),
            erreur_code=E_DATASET_NON_INITIALISE,
            erreur_resume="Aucun mois disponible dans menages_taches_enrichies.",
        )
        return {"ok": False, "run_id": run_id, "statut": STATUT_ECHEC,
                "erreur_code": E_DATASET_NON_INITIALISE,
                "message": "Aucune donnée Hostaway ménages disponible : le dataset n'est pas "
                           "initialisé pour un mois quelconque."}

    datasets_absents = [t for t in DATASETS_REQUIS if not _dataset_dispo(db_reel, t, mois)]
    if datasets_absents:
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=mois, statut=STATUT_ECHEC, git_head=_git_head(),
            erreur_code=E_DATASET_NON_INITIALISE,
            erreur_resume="Absents pour " + mois + " : " + ", ".join(datasets_absents),
        )
        return {"ok": False, "run_id": run_id, "statut": STATUT_ECHEC,
                "erreur_code": E_DATASET_NON_INITIALISE,
                "message": "Dataset(s) SQLite absent(s) pour " + mois + " : "
                           + ", ".join(datasets_absents)}

    scripts_absents = [Path(rel).name for rel in SCRIPTS_A_COPIER
                       if not (_project_root() / rel).exists()]
    if scripts_absents:
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=mois, statut=STATUT_ECHEC, git_head=_git_head(),
            erreur_code="E_SCRIPT_MOTEUR_ABSENT", erreur_resume="Absents : " + ", ".join(scripts_absents),
        )
        return {"ok": False, "run_id": run_id, "statut": STATUT_ECHEC,
                "erreur_code": "E_SCRIPT_MOTEUR_ABSENT",
                "message": "Script(s) moteur absent(s) du projet : " + ", ".join(scripts_absents)
                           + ". La simulation n'a pas été lancée ; rien n'a été touché."}

    # ── Verrou interprocessus (dédié) — libéré dans finally quoi qu'il arrive ─
    try:
        verrou = verrou_lib.acquerir_verrou(operation=OPERATION, lock_path=_lock_path())
    except verrou_lib.VerrouSaisieChargesDejaPrisError as exc:
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=mois, statut=STATUT_VERROUILLE, git_head=_git_head(),
            erreur_code="E_VERROU", erreur_resume=str(exc),
        )
        return {"ok": False, "run_id": run_id, "statut": STATUT_VERROUILLE,
                "erreur_code": "E_VERROU", "message": "Un recalcul est déjà en cours."}

    date_debut = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    try:
        # ── Empreinte des tables DOMAINE de la base RÉELLE avant (traçabilité, lecture seule) ───
        sha_avant = _sha256(db_reel) if db_reel.exists() else None       # affichage/traçabilité
        empreinte_avant = _empreinte_domaine(db_reel) if db_reel.exists() else None  # garde réelle
        comparaison_avant = _lire_comparaison(db_reel, mois)
        snap = snapshot_service.create_snapshot(
            "MENAGES_RECALC_AVANT", [db_reel] if db_reel.exists() else [], db_path=db_path)

        # ── Workspace : scripts + COPIE de la base (jamais d'écriture sur la réelle) ─
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        workspace, db_copie = _construire_workspace(run_ts, db_reel)
        steps = [
            {**s, "args": ["--source", "SQLITE", "--db", str(db_copie), "--mois", mois,
                          "--sans-excel", "--run-id", run_ts]}
            for s in STEPS_COPIES
        ]
        requete = {"allowed_root": str(workspace.resolve()), "workspace": str(workspace.resolve()),
                   "steps": steps, "timeout": cfg.MENAGES_RECALC_TIMEOUT_SECONDS}
        requete_path = workspace / "_requete.json"
        reponse_path = workspace / "_reponse.json"
        requete_path.write_text(json.dumps(requete, ensure_ascii=False, indent=2), encoding="utf-8")

        # ── Invocation du runner HORS paquet app/, interpréteur moteur ──────
        runner = Path(cfg.MENAGES_RECALC_RUNNER)
        engine_py = Path(cfg.MENAGES_ENGINE_PYTHON)
        erreur_code = None
        erreur_resume = None
        etapes: list[dict[str, Any]] = []
        if not runner.exists():
            statut = STATUT_ECHEC
            erreur_code, erreur_resume = "E_RUNNER", f"Runner absent : {runner}"
        elif not engine_py.exists():
            statut = STATUT_ECHEC
            erreur_code, erreur_resume = "E_RUNNER", f"Interpréteur moteur absent : {engine_py}"
        else:
            try:
                proc = subprocess.run(
                    [str(engine_py), str(runner), str(requete_path), str(reponse_path)],
                    capture_output=True, text=True,
                    timeout=cfg.MENAGES_RECALC_TIMEOUT_SECONDS + 30,
                )
            except subprocess.TimeoutExpired:
                statut = STATUT_ECHEC
                erreur_code, erreur_resume = "E_TIMEOUT", "Le runner a dépassé le délai."
            else:
                if not reponse_path.exists():
                    statut = STATUT_ECHEC
                    erreur_code = "E_REPONSE_INVALIDE"
                    erreur_resume = f"Pas de réponse runner (rc={proc.returncode}). {proc.stderr[-500:]}"
                else:
                    reponse = json.loads(reponse_path.read_text(encoding="utf-8"))
                    etapes = reponse.get("steps", [])
                    nb_ok = sum(1 for e in etapes if e.get("statut") == "OK")
                    if reponse.get("ok") and nb_ok == len(STEPS_COPIES):
                        statut = STATUT_SUCCES
                    elif nb_ok == 0:
                        statut = STATUT_ECHEC
                    else:
                        statut = STATUT_PARTIEL
                    if statut != STATUT_SUCCES:
                        premiere = next((e for e in etapes if e.get("statut") != "OK"), {})
                        erreur_code = premiere.get("erreur_code") or "E_CONTROLE_ECHEC"
                        erreur_resume = premiere.get("detail")

        # ── Vérification : la copie porte bien des lignes pour ce mois (0038) ────
        comparaison_apres = _lire_comparaison(db_copie, mois)
        if statut == STATUT_SUCCES and (comparaison_apres is None or comparaison_apres["nb_lignes"] == 0):
            statut = STATUT_ECHEC
            erreur_code = "E_SORTIE_INVALIDE"
            erreur_resume = "Aucune ligne menages_rapprochement produite pour " + mois + " dans la copie."

        comparaison = {"avant": comparaison_avant, "apres": comparaison_apres}

        # ── Garde : les tables DOMAINE de la base RÉELLE n'ont pas bougé (jamais ouvertes en
        # écriture) — pas une comparaison de fichier entier : le journal (snapshots,
        # menages_recalcul_runs, audit_events) s'écrit normalement dans cette même base à chaque
        # run, et le détecter comme une anomalie ferait échouer tout run, y compris un run correct.
        sha_apres = _sha256(db_reel) if db_reel.exists() else None       # affichage/traçabilité
        empreinte_apres = _empreinte_domaine(db_reel) if db_reel.exists() else None
        reel_intact = empreinte_apres == empreinte_avant
        if not reel_intact:
            statut = STATUT_ECHEC
            erreur_code = "E_REEL_MODIFIE"
            erreur_resume = "ANOMALIE GRAVE : les données ménage de app.db réelle ont changé " \
                            "pendant un recalcul sur copies."

        duree = round(time.monotonic() - t0, 2)
        date_fin = datetime.now(timezone.utc).isoformat()
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=mois, statut=statut,
            date_debut=date_debut, date_fin=date_fin, duree_secondes=duree,
            snapshot_id=snap["id"], git_head=_git_head(), workspace_path=str(workspace),
            sha256_sources_avant=json.dumps({"app.db": sha_avant}, ensure_ascii=False),
            sha256_sorties_avant=json.dumps({"app.db": sha_avant}, ensure_ascii=False),
            sha256_sorties_apres=json.dumps({"app.db": sha_apres}, ensure_ascii=False),
            comparaison_json=json.dumps({"rapprochement": comparaison, "reel_intact": reel_intact},
                                        ensure_ascii=False),
            etapes_json=json.dumps(etapes, ensure_ascii=False, default=str),
            erreur_code=erreur_code, erreur_resume=erreur_resume,
        )
        log_event("MENAGE_RECALC", {"mode": mode, "mois": mois, "statut": statut,
                                    "run_id": run_id, "reel_intact": reel_intact}, db_path=db_path)
        return {
            "ok": statut == STATUT_SUCCES, "run_id": run_id, "statut": statut,
            "mode": mode, "mois": mois, "duree_secondes": duree,
            "snapshot_id": snap["id"], "workspace": str(workspace),
            "etapes": etapes, "comparaison": comparaison,
            "sha256_sorties_avant": {"app.db": sha_avant}, "sha256_sorties_apres": {"app.db": sha_apres},
            "reel_intact": reel_intact,
            "erreur_code": erreur_code, "erreur_resume": erreur_resume,
        }
    except Exception as exc:
        # Défense en profondeur : toute erreur inattendue devient un run ECHEC lisible, jamais un 500
        # ni un traceback dans le navigateur. Le type d'exception seul est exposé (aucun chemin absolu).
        run_id = _enregistrer_run(
            db_path, mode=mode, periode=mois, statut=STATUT_ECHEC, date_debut=date_debut,
            git_head=_git_head(), erreur_code="E_INATTENDU",
            erreur_resume=f"{type(exc).__name__}: {exc}",
        )
        log_event("MENAGE_RECALC_ERREUR", {"mode": mode, "mois": mois,
                                           "erreur": type(exc).__name__}, db_path=db_path)
        return {"ok": False, "run_id": run_id, "statut": STATUT_ECHEC,
                "erreur_code": "E_INATTENDU",
                "message": "Une erreur inattendue a interrompu la simulation "
                           f"({type(exc).__name__}). app.db réelle n'a pas été touchée."}
    finally:
        try:
            verrou_lib.liberer_verrou(verrou)
        except verrou_lib.LiberationVerrouSaisieChargesError:
            # La libération n'a pas pu conclure : le verrou reste en place (comportement sûr).
            # On ne masque pas : l'anomalie est tracée en audit.
            log_event("MENAGE_RECALC_VERROU_NON_LIBERE", {"lock": str(_lock_path())}, db_path=db_path)


# ── Lecture historique ───────────────────────────────────────────────────────

def load_runs(limit: int = 5, db_path: Path | None = None) -> list[dict[str, Any]]:
    db_path = db_path or cfg.DB_PATH
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT id, ts, mode, periode, statut, duree_secondes, snapshot_id, git_head, "
            "erreur_code FROM menages_recalcul_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def load_run(run_id: int, db_path: Path | None = None) -> dict[str, Any] | None:
    db_path = db_path or cfg.DB_PATH
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM menages_recalcul_runs WHERE id=?", (run_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    d = dict(row)
    for champ in ("etapes_json", "comparaison_json", "sha256_sources_avant",
                  "sha256_sorties_avant", "sha256_sorties_apres"):
        if d.get(champ):
            try:
                d[champ.replace("_json", "")] = json.loads(d[champ])
            except (json.JSONDecodeError, TypeError):
                pass
    d["etapes"] = d.get("etapes") or (json.loads(d["etapes_json"]) if d.get("etapes_json") else [])
    if isinstance(d.get("comparaison"), dict):
        d["reel_intact"] = d["comparaison"].get("reel_intact")
    return d

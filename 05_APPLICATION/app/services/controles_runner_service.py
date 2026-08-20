"""Runner de recalcul des contrôles sur COPIE (APP-5B) — 100% SQLite depuis la fermeture de Lot11.

CE QUE CE MODULE PROUVE
Pour un élément bancaire ouvert, il répond à une question que l'application ne doit pas trancher
seule : « si cette ligne était classée, le contrôle disparaîtrait-il ? ». La réponse est obtenue en
RECALCULANT réellement les contrôles (`controles_lot11_service.construire`) avant et après une
classification SIMULÉE — jamais en déduisant la réponse d'un état applicatif.

POURQUOI IL N'Y A PLUS NI SOUS-PROCESSUS NI CLASSEUR
Auparavant, Lot11 était un script pandas lisant des classeurs : le runner fabriquait donc un
workspace, y écrivait un classeur bancaire et des classeurs de réservations depuis SQLite, lançait
`lot8c_rapprochement_banque.py` puis `lot11_controles_coherence.py` en sous-processus, et relisait
le résultat. Lot11 étant désormais SQLite natif (`controles_lot11_service`), cette boucle
SQLite → XLSX → moteur → SQLite n'a plus d'objet : elle ne prouverait rien de plus et entretiendrait
une dépendance Excel artificielle. Le moteur de contrôle appelé ici est le MÊME que celui de
l'application — il n'existe plus qu'une seule implémentation des règles Lot11.

CE QUI NE CHANGE PAS : LA BASE RÉELLE N'EST JAMAIS MODIFIÉE
Le recalcul se fait sur une COPIE de la base (API `backup`, jamais `copy2`), dans un workspace isolé
hors de l'arbre réel. La classification simulée n'est écrite QUE dans cette copie. L'empreinte de la
base réelle est vérifiée avant/après, les flags d'écriture réelle doivent rester désactivés, et tout
échec devient un run ÉCHEC/BLOQUÉ lisible — jamais un 500.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import banques_controle_service as banque_ctrl
from app.services.path_sanitizer import sanitize_text as _sanitize

STATUT_SUCCES = "SUCCES"
STATUT_ECHEC = "ECHEC"
STATUT_BLOQUE = "BLOQUE"

# Verdicts issus du recalcul des contrôles (jamais déduits d'un état applicatif).
V_RESOLU = "RESOLU_MOTEUR"
V_PRESENT = "TOUJOURS_PRESENT"
V_TRANSFORME = "TRANSFORME"
V_ERREUR = "ERREUR_MOTEUR"
V_NON_COMPARABLE = "NON_COMPARABLE"

CODE_CTRL_BANQUE_NON_CLASSEE = "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE"

TYPE_ACTION = "RECALCUL_COPIE_CONTROLES_SQLITE"


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def _now_ns() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def _preflight() -> tuple[bool, str]:
    if cfg.CONTROLES_REAL_WRITE_ENABLED or cfg.CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED:
        return False, "Flags d'écriture réelle actifs : recalcul sur copie refusé."
    return True, ""


def _valider_workspace_isole(ws: Path) -> tuple[bool, str]:
    """Garde post-incident : le workspace doit être HORS de l'arbre réel.

    L'incident du 18/07 venait d'un traitement lancé contre l'arbre réel. Même sans sous-processus
    ni écriture de classeur, cette garde reste : le workspace porte une copie de la base, et rien
    ne doit pouvoir l'écrire sous `cfg.PROJECT_ROOT`.
    """
    real = Path(cfg.PROJECT_ROOT).resolve()
    wsr = ws.resolve()
    if wsr == real or real in wsr.parents:
        return False, f"Workspace imbriqué dans l'arbre réel ({real}) : refusé."
    return True, ""


def _copier_base(source: Path, destination: Path) -> None:
    """Copie une base SQLite via l'API backup (sûre même si WAL/journal actifs)."""
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


def _compter_controle_banque(db: Path, mois: str) -> int | None:
    """Nombre de constats `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` pour un mois, dans la
    base passée. `None` si les constats n'ont pas pu être produits."""
    conn = get_db(db)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name='controles_lot11_constats'").fetchone() is None:
            return None
        return conn.execute(
            "SELECT COUNT(*) FROM controles_lot11_constats c "
            "JOIN controles_lot11_constats_champs f ON f.ctrl_pk = c.ctrl_pk "
            "WHERE c.code_controle = ? AND f.mois = ?",
            (CODE_CTRL_BANQUE_NON_CLASSEE, mois)).fetchone()[0]
    finally:
        conn.close()


def _simuler_classification(db: Path, mouvement_id: str) -> bool:
    """Présente un mouvement comme CLASSÉ dans la COPIE, sans toucher la base réelle.

    Écrit une classification sur le run de classification courant : c'est exactement l'état qu'
    aurait la base si la décision humaine était prise, donc la question posée au moteur de contrôle
    est bien « et si c'était classé ? », pas « et si on masquait la ligne ? ».
    """
    from app.services import banque_classification_service as cls

    conn = get_db(db)
    try:
        run = conn.execute(
            "SELECT classification_run_id FROM banque_classifications "
            "WHERE mouvement_id_opaque = ? ORDER BY id DESC LIMIT 1", (mouvement_id,)).fetchone()
        if run is None:
            return False
        conn.execute(
            "UPDATE banque_classifications SET statut_classification = ?, statut_controle = ? "
            "WHERE mouvement_id_opaque = ? AND classification_run_id = ?",
            (cls.CLASS_CLASSE, cls.ST_VALIDE, mouvement_id, run[0]))
        conn.commit()
        return True
    finally:
        conn.close()


def _journaliser(type_action, statut, nb, workspace, reel_intact, avant, apres,
                 err_code="", err_resume="", db_path=None) -> int:
    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO controles_runs (type_action, statut, nb_elements, workspace_path, "
            "reel_intact, avant_json, apres_json, erreur_code, erreur_resume) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (type_action, statut, nb, workspace, 1 if reel_intact else 0,
             json.dumps(avant, default=str), json.dumps(apres, default=str), err_code, err_resume))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def recalculer_sur_copie(element: dict[str, Any], appliquer_classification: bool = True,
                         db_path=None) -> dict[str, Any]:
    """Recalcule réellement les contrôles sur copie pour un élément. Ne touche jamais le réel.

    `appliquer_classification` : True simule la décision « classé » (cas A) — le contrôle peut
    disparaître ; False laisse le mouvement non classé (cas B) — le contrôle reste présent.
    """
    from app.services import controles_lot11_service as lot11

    ok, motif = _preflight()
    banque_ctrl.vider_cache()
    ws = Path(cfg.CONTROLES_RUNNER_WORKSPACE) / _now_ns()
    reel_db = Path(db_path) if db_path else Path(cfg.DB_PATH)
    sha_db = _sha(reel_db) if reel_db.exists() else ""
    avant = {"element": element.get("ctrl_opaque"), "code": element.get("code"),
             "mois": element.get("mois")}

    def _reel_intact() -> bool:
        return (_sha(reel_db) if reel_db.exists() else "") == sha_db

    if not ok:
        rid = _journaliser("PREFLIGHT", STATUT_BLOQUE, 0, "", True, avant, None, "FLAGS", motif,
                           db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": True, "chemins": [], "etapes": []}

    if element.get("module") != "BANQUE":
        motif = ("Recalcul sur copie disponible pour les mouvements bancaires. Pour ce module, la "
                 "disparition se constate au prochain recalcul des contrôles.")
        rid = _journaliser(TYPE_ACTION, STATUT_BLOQUE, 1, "", _reel_intact(), avant,
                           {"note": motif}, "HORS_PERIMETRE", motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": _reel_intact(), "chemins": [], "etapes": []}

    ws_ok, ws_motif = _valider_workspace_isole(ws)
    if not ws_ok:
        rid = _journaliser(TYPE_ACTION, STATUT_BLOQUE, 1, str(ws), True, avant, None,
                           "WORKSPACE_NON_ISOLE", ws_motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": ws_motif,
                "run_id": rid, "reel_intact": True, "chemins": [], "etapes": []}

    etapes: list[dict[str, Any]] = []
    try:
        ws.mkdir(parents=True, exist_ok=True)
        mois = element.get("mois", "")
        mvt_reel = banque_ctrl.resoudre_opaque(element.get("entite_id", ""))
        db_copie = ws / "controles_recalcul.db"

        _copier_base(reel_db, db_copie)
        etapes.append({"etape": "COPIE_BASE", "chemin": str(db_copie)})

        # 1) baseline : contrôles recalculés sur la copie, avant toute décision simulée.
        t0 = datetime.now()
        r_base = lot11.construire(db_path=db_copie)
        etapes.append({"etape": "CONTROLES_BASELINE", "ok": r_base.get("ok"),
                       "nb_constats": r_base.get("nb_constats"),
                       "duree_s": round((datetime.now() - t0).total_seconds(), 1)})
        if not r_base.get("ok"):
            raise RuntimeError(f"Recalcul baseline échoué : {r_base.get('message')}")
        n_avant = _compter_controle_banque(db_copie, mois)

        # 2) décision simulée, écrite UNIQUEMENT dans la copie.
        classifie = False
        if appliquer_classification and mvt_reel:
            classifie = _simuler_classification(db_copie, mvt_reel)
            etapes.append({"etape": "CLASSIFICATION_SIMULEE", "mouvement_classe": classifie})

        # 3) contrôles recalculés après la décision simulée.
        t1 = datetime.now()
        r_apres = lot11.construire(db_path=db_copie)
        etapes.append({"etape": "CONTROLES_APRES", "ok": r_apres.get("ok"),
                       "nb_constats": r_apres.get("nb_constats"),
                       "duree_s": round((datetime.now() - t1).total_seconds(), 1)})
        if not r_apres.get("ok"):
            raise RuntimeError(f"Recalcul après décision échoué : {r_apres.get('message')}")
        n_apres = _compter_controle_banque(db_copie, mois)

        # 4) verdict, tiré du recalcul — jamais d'un état applicatif.
        if n_avant is None or n_apres is None:
            verdict = V_NON_COMPARABLE
        elif classifie and n_apres < n_avant:
            verdict = V_RESOLU
        elif n_apres == n_avant:
            verdict = V_PRESENT
        else:
            verdict = V_TRANSFORME

        reel_intact = _reel_intact()
        apres = {"n_controle_avant": n_avant, "n_controle_apres": n_apres, "mois": mois,
                 "classification_appliquee": classifie}
        statut = STATUT_SUCCES if reel_intact else STATUT_ECHEC
        rid = _journaliser(TYPE_ACTION, statut, 1, str(ws), reel_intact, avant, apres, "", "",
                           db_path)
        return {"statut": statut, "verdict": verdict,
                "motif": _motif_verdict(verdict, n_avant, n_apres), "run_id": rid,
                "reel_intact": reel_intact, "chemins": [str(db_copie)], "manquants": [],
                "etapes": etapes, "n_avant": n_avant, "n_apres": n_apres}
    except Exception as exc:
        motif_sanitise = _sanitize(f"Échec : {exc}")
        rid = _journaliser(TYPE_ACTION, STATUT_ECHEC, 1, _sanitize(str(ws)), _reel_intact(),
                           avant, {"etapes": etapes}, "EXCEPTION", motif_sanitise, db_path)
        return {"statut": STATUT_ECHEC, "verdict": V_ERREUR, "motif": motif_sanitise,
                "run_id": rid, "reel_intact": _reel_intact(), "chemins": [], "etapes": etapes}
    finally:
        try:
            if ws.exists():
                shutil.rmtree(ws, ignore_errors=True)
        except Exception:
            pass


def _motif_verdict(verdict: str, n_avant, n_apres) -> str:
    return {
        V_RESOLU: f"Contrôle bancaire résolu par le recalcul ({n_avant} → {n_apres} pour ce mois).",
        V_PRESENT: f"Contrôle toujours présent après recalcul ({n_apres} pour ce mois).",
        V_TRANSFORME: f"Contrôle transformé ({n_avant} → {n_apres}).",
        V_NON_COMPARABLE: "Comparaison impossible (constats non produits).",
        V_ERREUR: "Erreur pendant le recalcul.",
    }.get(verdict, verdict)


def load_run(run_id: int, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM controles_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

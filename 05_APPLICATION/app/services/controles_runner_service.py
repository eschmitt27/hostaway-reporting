"""Runner de recalcul moteur sur COPIES (APP-5B) — Lot8c puis Lot11 RÉELLEMENT exécutés.

Corrige la limite APP-4B (« modifier NORM_Banque ne prouve rien ») ET la réserve APP-5B (« Lot8c/Lot11
pas réellement exécutés »). Pour un élément bancaire, on construit un workspace isolé contenant toutes
les entrées, puis on lance réellement `lot8c_rapprochement_banque.py --project-root <ws>
--no-real-write` puis `lot11_controles_coherence.py --project-root <ws> --no-real-write`. On compare la
présence du contrôle bancaire AVANT / APRÈS. La résolution n'est jamais déduite de SQLite : elle est
prouvée par le moteur.

LA BANQUE VIENT DE SQLITE, PAS DU CLASSEUR
Le classeur bancaire du workspace n'est plus une copie du réel : il est FABRIQUÉ depuis la base par
`banque_adaptateur_moteur`, une fois avec le mouvement laissé en l'état, une fois avec ce même
mouvement présenté comme classé. Les deux moteurs ne savent pas encore lire SQLite, et leur apprendre
relèverait du chantier Lot9/10/11 ; recréer leurs règles ici produirait deux moteurs de contrôle
divergents. Ce classeur intermédiaire est donc un adaptateur jetable, pas une source : aucun service
applicatif ne le lit, et la base n'est jamais modifiée pour le produire.

Le réel n'est jamais touché (SHA avant/après vérifié). Aucun chemin résolu ne sort du workspace.
Flags réels toujours False. Aucun 500 : tout échec devient un run ÉCHEC/BLOQUE lisible.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
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

# Verdicts moteur (jamais déduits de SQLite).
V_RESOLU = "RESOLU_MOTEUR"
V_PRESENT = "TOUJOURS_PRESENT"
V_TRANSFORME = "TRANSFORME"
V_ERREUR = "ERREUR_MOTEUR"
V_NON_COMPARABLE = "NON_COMPARABLE"

TIMEOUT_MOTEUR_S = 300

# Entrées Lot11 (relatives à PROJECT_ROOT) — copiées dans le workspace, chemins confinés au workspace.
INPUTS_LOT11 = [
    "02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx",
    # Reservations resolues et payouts : plus copies non plus, fabriques depuis SQLite dans le
    # workspace (voir `reservations_adaptateur_moteur`).
    "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx",
    "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx",
    "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx",
    "02_TRAVAIL/Lot1_Hostaway/MASTER_CTRL_HA_Anomalies.xlsx",
    "02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx",
    "01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx",
    "02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx",
    "02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx",
    "02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx",
    "02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx",
    "02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx",
    "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm",
    # La Banque N'EST PLUS copiée : elle est fabriquée depuis SQLite dans le workspace.
    "01_SOURCES_BRUTES/AirCover/SAISIE_AirCover.xlsx",
    "01_SOURCES_BRUTES/ImputationsAirbnb/SAISIE_ImputationsAirbnb.xlsx",
    "01_SOURCES_BRUTES/AjustementsPostCloture/SAISIE_Ajustements_PostCloture.xlsx",
    "02_TRAVAIL/Lot6b_DeclarationsInternes/MASTER_NORM_Declarations_Internes.xlsx",
    "02_TRAVAIL/Lot6f_CoutComplet_Menages/MASTER_CALC_CoutComplet_Menages.xlsx",
]
BANQUE_REL = "02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx"
OUT_REL = "02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx"
SCRIPT_LOT8C = "lot8c_rapprochement_banque.py"
SCRIPT_LOT11 = "lot11_controles_coherence.py"


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


def _engine_python() -> str | None:
    """Interpréteur avec pandas (le moteur en dépend ; l'app peut tourner sous un python sans pandas)."""
    candidats = []
    if os.environ.get("PILOTAGE_ENGINE_PYTHON"):
        candidats.append(os.environ["PILOTAGE_ENGINE_PYTHON"])
    candidats += [sys.executable, r"C:\Program Files\Python312\python.exe"]
    for c in candidats:
        if not c or not Path(c).exists():
            continue
        try:
            r = subprocess.run([c, "-c", "import pandas, openpyxl"], capture_output=True, timeout=30)
            if r.returncode == 0:
                return c
        except Exception:
            continue
    return None


INJECTION_MARQUEUR = "PILOTAGE_PROJECT_ROOT"


def _scripts_dir() -> Path:
    """Scripts moteur du WORKTREE isolé (injectés), jamais ceux du dépôt réel.

    Les scripts injectés (`--project-root`/`--no-real-write`) vivent dans l'arbre applicatif isolé,
    à côté du package `app` (APP_ROOT.parent/02_TRAVAIL). Utiliser `cfg.PROJECT_ROOT` pointerait vers
    l'arbre RÉEL dont les scripts ne sont pas injectés et écriraient le réel : interdit.
    """
    return Path(cfg.APP_ROOT).parent / "02_TRAVAIL"


def _script_injecte(script: str) -> bool:
    """Vrai si le script moteur contient bien le marqueur d'injection (sinon : refus de l'exécuter)."""
    p = _scripts_dir() / script
    if not p.exists():
        return False
    try:
        return INJECTION_MARQUEUR in p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False


def _build_workspace(ws: Path) -> tuple[list[str], list[str]]:
    """Copie les entrées dans le workspace. Retourne (chemins_resolus, manquants)."""
    resolus, manquants = [], []
    for rel in INPUTS_LOT11:
        src = Path(cfg.PROJECT_ROOT) / rel
        dst = ws / rel
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            resolus.append(str(dst))
        else:
            manquants.append(rel)
    (ws / "02_TRAVAIL/Lot11_Controles").mkdir(parents=True, exist_ok=True)
    return resolus, manquants


def _chemins_hors_workspace(resolus: list[str], ws: Path) -> list[str]:
    wsr = ws.resolve()
    return [p for p in resolus if wsr not in Path(p).resolve().parents]


def _valider_workspace_isole(ws: Path) -> tuple[bool, str]:
    """Garde post-incident : le workspace et les scripts doivent être HORS de l'arbre réel.

    L'incident du 18/07 venait de scripts NON injectés lancés contre l'arbre réel. Ici on refuse en
    plus tout workspace qui résoudrait À L'INTÉRIEUR de `cfg.PROJECT_ROOT` (le vrai dépôt / OneDrive) :
    même avec des scripts injectés, écrire sous l'arbre réel est interdit. Les scripts moteur doivent
    aussi provenir du worktree isolé (jamais de `cfg.PROJECT_ROOT`).
    """
    real = Path(cfg.PROJECT_ROOT).resolve()
    wsr = ws.resolve()
    if wsr == real or real in wsr.parents:
        return False, f"Workspace imbriqué dans l'arbre réel ({real}) : refusé."
    scripts = _scripts_dir().resolve()
    if scripts == real or real in scripts.parents:
        return False, f"Scripts moteur situés dans l'arbre réel ({real}) : refusé."
    return True, ""


def _generer_reservations(ws: Path, db_path=None) -> dict[str, Any]:
    """Fabrique les classeurs de reservations attendus par Lot11, depuis SQLite.

    Meme statut que le classeur bancaire : jetable, lie au run, jamais canonique. Lot11 lit la source
    RESOLUE et les payouts ; les copier depuis l'arbre du projet ferait dependre le controle d'un
    calcul anterieur, potentiellement plus ancien que le dataset courant.
    """
    from app.services import reservations_adaptateur_moteur as adaptateur

    return adaptateur.ecrire_tout(ws, db_path=db_path)


def _generer_banque(ws: Path, mouvement_classe: str = "", db_path=None) -> dict[str, Any]:
    """Fabrique le classeur bancaire du workspace depuis SQLite.

    Appelé deux fois : sans `mouvement_classe` pour la baseline, puis avec, pour simuler la décision
    humaine. La simulation se fait donc dans le classeur jetable — la base n'est jamais modifiée pour
    répondre à une question.
    """
    from app.services import banque_adaptateur_moteur as adaptateur

    return adaptateur.ecrire_classeur_moteur(ws / BANQUE_REL, mouvement_classe=mouvement_classe,
                                            db_path=db_path)


CODE_CTRL_BANQUE_NON_CLASSEE = "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE"


def _compter_controle_banque(ws: Path, mois: str, ws_db: Path) -> int | None:
    """Nombre de constats CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE pour un mois, produits par
    le run Lot11 courant dans `ws`.

    Ne relit plus le classeur `MASTER_CTRL_Coherence.xlsx` directement (openpyxl ad hoc) : reprend
    par `controles_lot11_adapter`, le même mécanisme que l'application (mission Ménages, Bloc B §8
    — « L'application relit le résultat SQLite. Jamais le workbook. »), sur une base SCRATCH propre
    au workspace du run — jamais la base réelle, jamais `controles_lot11_constats` de l'app (ce
    comptage est une simulation baseline/après, pas l'état applicatif courant).
    """
    from app.db.connection import apply_migrations
    from app.services import controles_lot11_adapter as lot11_adapter

    if not lot11_adapter.master_disponible(racine=ws):
        return None
    if not ws_db.exists():
        apply_migrations(ws_db)
    resultat = lot11_adapter.reprendre(racine=ws, db_path=ws_db)
    if not resultat.get("ok"):
        return None
    conn = get_db(ws_db)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM controles_lot11_constats c "
            "JOIN controles_lot11_constats_champs f ON f.ctrl_pk = c.ctrl_pk "
            "WHERE c.code_controle = ? AND f.mois = ?",
            (CODE_CTRL_BANQUE_NON_CLASSEE, mois)).fetchone()[0]
    finally:
        conn.close()


def _run_script(py: str, script: str, ws: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [py, str(_scripts_dir() / script), "--project-root", str(ws), "--no-real-write", "--run-id", ws.name],
        capture_output=True, text=True, cwd=str(_scripts_dir()), timeout=TIMEOUT_MOTEUR_S)


def _journaliser(type_action, statut, nb, workspace, reel_intact, avant, apres,
                 err_code="", err_resume="", db_path=None) -> int:
    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO controles_runs (type_action, statut, nb_elements, workspace_path, "
            "reel_intact, avant_json, apres_json, erreur_code, erreur_resume) VALUES (?,?,?,?,?,?,?,?,?)",
            (type_action, statut, nb, workspace, 1 if reel_intact else 0,
             json.dumps(avant, default=str), json.dumps(apres, default=str), err_code, err_resume))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def recalculer_sur_copie(element: dict[str, Any], appliquer_classification: bool = True,
                         db_path=None) -> dict[str, Any]:
    """Recalcule réellement Lot8c+Lot11 sur copie pour un élément. Ne touche jamais le réel, jamais 500.

    `appliquer_classification` : True simule la décision « classé » (case A) — le contrôle peut
    disparaître ; False laisse le mouvement non classé (case B) — le contrôle reste présent.
    """
    ok, motif = _preflight()
    # Résoudre l'opaque contre la banque ACTUELLEMENT configurée (jamais un index mémorisé obsolète).
    banque_ctrl.vider_cache()
    ws = Path(cfg.CONTROLES_RUNNER_WORKSPACE) / _now_ns()
    reel_ctrl = Path(cfg.MASTER_CTRL_COHERENCE)
    sha_ctrl = _sha(reel_ctrl) if reel_ctrl.exists() else ""
    # Le classeur bancaire n'est plus une entrée ; la Banque à protéger est la base. On la surveille
    # comme on surveillait le fichier : par empreinte avant/après, pas par confiance.
    reel_db = Path(cfg.DB_PATH)
    sha_db = _sha(reel_db) if reel_db.exists() else ""
    avant = {"element": element.get("ctrl_opaque"), "code": element.get("code"),
             "mois": element.get("mois")}

    def _reel_intact() -> bool:
        c = (_sha(reel_ctrl) if reel_ctrl.exists() else "") == sha_ctrl
        d = (_sha(reel_db) if reel_db.exists() else "") == sha_db
        return c and d

    if not ok:
        rid = _journaliser("PREFLIGHT", STATUT_BLOQUE, 0, "", True, avant, None, "FLAGS", motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": True, "chemins": [], "etapes": []}

    if element.get("module") != "BANQUE":
        motif = ("Recalcul Lot8c/Lot11 sur copie disponible pour les mouvements bancaires. Pour ce "
                 "module, la disparition se constate à la prochaine génération moteur.")
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_BLOQUE, 1, "", _reel_intact(),
                           avant, {"note": motif}, "HORS_PERIMETRE", motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": _reel_intact(), "chemins": [], "etapes": []}

    # Garde-fou dur : ne jamais exécuter un script moteur NON injecté (il écrirait le réel).
    non_injectes = [s for s in (SCRIPT_LOT8C, SCRIPT_LOT11) if not _script_injecte(s)]
    if non_injectes:
        motif = (f"Scripts moteur non injectés (marqueur absent) : {non_injectes}. Exécution refusée "
                 "pour ne jamais écrire le réel.")
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_BLOQUE, 1, str(ws), True,
                           avant, None, "SCRIPT_NON_INJECTE", motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": True, "chemins": [], "etapes": []}

    # Garde-fou post-incident : workspace + scripts obligatoirement HORS de l'arbre réel.
    ws_ok, ws_motif = _valider_workspace_isole(ws)
    if not ws_ok:
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_BLOQUE, 1, str(ws), True,
                           avant, None, "WORKSPACE_NON_ISOLE", ws_motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": ws_motif, "run_id": rid,
                "reel_intact": True, "chemins": [], "etapes": []}

    py = _engine_python()
    if py is None:
        motif = "Aucun interpréteur Python avec pandas trouvé (définir PILOTAGE_ENGINE_PYTHON)."
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_BLOQUE, 1, str(ws), True,
                           avant, None, "PANDAS_ABSENT", motif, db_path)
        return {"statut": STATUT_BLOQUE, "verdict": V_NON_COMPARABLE, "motif": motif, "run_id": rid,
                "reel_intact": True, "chemins": [], "etapes": []}

    etapes: list[dict[str, Any]] = []
    try:
        ws.mkdir(parents=True, exist_ok=True)
        resolus, manquants = _build_workspace(ws)
        hors = _chemins_hors_workspace(resolus, ws)
        if hors:
            raise RuntimeError(f"Chemins résolus hors workspace refusés : {hors[:3]}")
        mois = element.get("mois", "")
        mvt_reel = banque_ctrl.resoudre_opaque(element.get("entite_id", ""))
        ws_db = ws / "controles_lot11_scratch.db"   # scratch, jamais la base réelle ni l'app-facing

        # 0) Banque du workspace, fabriquée depuis SQLite — état actuel, sans décision simulée.
        gen = _generer_banque(ws, db_path=db_path)
        if not gen.get("ok"):
            raise RuntimeError(gen.get("message", "Banque indisponible"))
        etapes.append({"etape": "BANQUE_DEPUIS_SQLITE", "nb_mouvements": gen["nb_mouvements"],
                       "nb_controles": gen["nb_controles"],
                       "nb_mois_clotures": gen["nb_mois_clotures"]})

        gen_res = _generer_reservations(ws, db_path=db_path)
        if not gen_res.get("ok"):
            raise RuntimeError(gen_res.get("message", "Reservations indisponibles"))
        etapes.append({"etape": "RESERVATIONS_DEPUIS_SQLITE",
                       "nb_resolues": gen_res["resolues"]["nb_lignes"],
                       "nb_payouts": gen_res["payouts"]["nb_lignes"]})

        # 1) baseline : Lot11 sur copie (avant décision)
        t0 = datetime.now()
        r11a = _run_script(py, SCRIPT_LOT11, ws)
        etapes.append({"etape": "LOT11_BASELINE", "rc": r11a.returncode,
                       "duree_s": round((datetime.now() - t0).total_seconds(), 1)})
        if r11a.returncode != 0:
            raise RuntimeError(f"Lot11 baseline rc={r11a.returncode} : {_sanitize(r11a.stderr[-300:])}")
        n_avant = _compter_controle_banque(ws, mois, ws_db)

        # 2) décision simulée : on régénère la Banque du workspace avec ce mouvement présenté comme
        #    classé. Rien n'est modifié en base — la question posée au moteur reste hypothétique.
        classifie = False
        if appliquer_classification and mvt_reel:
            gen2 = _generer_banque(ws, mouvement_classe=mvt_reel, db_path=db_path)
            classifie = bool(gen2.get("ok") and gen2.get("mouvement_classe"))
            etapes.append({"etape": "CLASSIFICATION_SIMULEE", "mouvement_classe": classifie})

        # 3) Lot8c puis Lot11 après décision
        t1 = datetime.now()
        r8c = _run_script(py, SCRIPT_LOT8C, ws)
        etapes.append({"etape": "LOT8C", "rc": r8c.returncode,
                       "duree_s": round((datetime.now() - t1).total_seconds(), 1)})
        if r8c.returncode != 0:
            raise RuntimeError(f"Lot8c rc={r8c.returncode} : {_sanitize(r8c.stderr[-300:])}")
        t2 = datetime.now()
        r11b = _run_script(py, SCRIPT_LOT11, ws)
        etapes.append({"etape": "LOT11", "rc": r11b.returncode,
                       "duree_s": round((datetime.now() - t2).total_seconds(), 1)})
        if r11b.returncode != 0:
            raise RuntimeError(f"Lot11 rc={r11b.returncode} : {_sanitize(r11b.stderr[-300:])}")
        n_apres = _compter_controle_banque(ws, mois, ws_db)

        # 4) verdict moteur (jamais SQLite)
        if n_avant is None or n_apres is None:
            verdict = V_NON_COMPARABLE
        elif classifie and n_apres < n_avant:
            verdict = V_RESOLU
        elif n_apres == n_avant:
            verdict = V_PRESENT
        elif n_apres != n_avant:
            verdict = V_TRANSFORME
        else:
            verdict = V_NON_COMPARABLE

        reel_intact = _reel_intact()
        apres = {"n_controle_avant": n_avant, "n_controle_apres": n_apres, "mois": mois,
                 "classification_appliquee": classifie, "manquants": manquants,
                 "master_ctrl_copie": (ws / OUT_REL).exists()}
        statut = STATUT_SUCCES if reel_intact else STATUT_ECHEC
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", statut, 1, str(ws), reel_intact,
                           avant, apres, "", "", db_path)
        return {"statut": statut, "verdict": verdict, "motif": _motif_verdict(verdict, n_avant, n_apres),
                "run_id": rid, "reel_intact": reel_intact, "chemins": resolus, "manquants": manquants,
                "etapes": etapes, "n_avant": n_avant, "n_apres": n_apres, "python": py}
    except subprocess.TimeoutExpired:
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_ECHEC, 1, _sanitize(str(ws)), _reel_intact(),
                           avant, {"etapes": etapes}, "TIMEOUT", "Timeout moteur", db_path)
        return {"statut": STATUT_ECHEC, "verdict": V_ERREUR, "motif": "Timeout du moteur.",
                "run_id": rid, "reel_intact": _reel_intact(), "chemins": [], "etapes": etapes}
    except Exception as exc:
        motif_sanitise = _sanitize(f"Échec : {exc}")
        rid = _journaliser("RECALCUL_COPIE_LOT8C_LOT11", STATUT_ECHEC, 1, _sanitize(str(ws)), _reel_intact(),
                           avant, {"etapes": etapes}, "EXCEPTION", motif_sanitise, db_path)
        return {"statut": STATUT_ECHEC, "verdict": V_ERREUR, "motif": motif_sanitise,
                "run_id": rid, "reel_intact": _reel_intact(), "chemins": [], "etapes": etapes}
    finally:
        try:
            if ws.exists():
                shutil.rmtree(ws, ignore_errors=True)   # nettoyage : locks/copies isolés
        except Exception:
            pass


def _motif_verdict(verdict: str, n_avant, n_apres) -> str:
    return {
        V_RESOLU: f"Contrôle bancaire résolu par le moteur ({n_avant} → {n_apres} pour ce mois).",
        V_PRESENT: f"Contrôle toujours présent après recalcul moteur ({n_apres} pour ce mois).",
        V_TRANSFORME: f"Contrôle transformé ({n_avant} → {n_apres}).",
        V_NON_COMPARABLE: "Comparaison impossible (sortie moteur absente).",
        V_ERREUR: "Erreur moteur.",
    }.get(verdict, verdict)


def load_run(run_id: int, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM controles_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

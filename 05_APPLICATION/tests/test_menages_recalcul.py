"""APP-2b — Recalcul ménages sécurisé (sur copies, mode réel gardé), SQLite-first.

Deux familles de tests :
  * logique du service avec un RUNNER STUB (python de test) — rapide, déterministe, sans moteur ;
  * une RECETTE E2E réelle qui exécute lot6d/lot6e sur copies et prouve que app.db réelle
    ne bouge pas (ignorée si l'interpréteur moteur est absent).

Aucun test ne touche un fichier métier réel ni la vraie base app.db.
"""
import hashlib
import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import menages_recalcul_service as rc
from app.services import snapshot_service
from app.services import saisie_charges_lock_service as verrou_lib

MOIS_TEST = "2026-05"


def _seed_datasets(db_path, mois=MOIS_TEST):
    """Une ligne dans chaque table requise par `DATASETS_REQUIS` — le dataset SQLite minimal dont
    lot6d/lot6e ont besoin en `--source SQLITE`."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-TEST-001", mois, "LOG_TEST", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle) VALUES (?,?,?,?,?)",
            (mois, "LOG_TEST", "INT_TEST", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


@pytest.fixture
def mini_project(tmp_path, monkeypatch, db):
    """Faux projet minimal : chaque script moteur attendu existe (fichier factice), le dataset
    SQLite minimal est semé dans `db` (source réelle depuis la migration SQLite-first).

    Redirige PROJECT_ROOT, DB_PATH, le workspace de recalcul et le répertoire de snapshots vers
    tmp. Aucune écriture ne peut donc atteindre l'arbre réel ni la vraie app.db.
    """
    root = tmp_path / "projet"
    for rel in rc.SCRIPTS_A_COPIER:
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"FIXTURE " + rel.encode())
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", tmp_path / "snapshots")
    _seed_datasets(db)
    return root


@pytest.fixture
def stub_runner(tmp_path, monkeypatch):
    """Installe un runner STUB piloté par la variable d'env STUB_OUTCOME.

    - ok        : chaque étape OK
    - fail      : première étape ECHEC (E_RUNNER)
    - noreponse : n'écrit aucune réponse (→ E_REPONSE_INVALIDE)
    """
    script = tmp_path / "stub_runner.py"
    script.write_text(textwrap.dedent('''
        import json, os, sqlite3, sys
        req = json.loads(open(sys.argv[1], encoding="utf-8").read())
        outcome = os.environ.get("STUB_OUTCOME", "ok")
        if outcome == "noreponse":
            sys.exit(0)
        steps = []
        for i, s in enumerate(req["steps"]):
            statut = "OK"
            if outcome == "fail" and i == 0:
                statut = "ECHEC"
            if statut == "OK":
                # Simule ce que lot6d/lot6e écriraient réellement : une ligne dans la table
                # attendue de la copie SQLite pointée par --db (le service vérifie ce contenu,
                # pas seulement le statut déclaré ici).
                args = s.get("args", [])
                if "--db" in args:
                    db_copie = args[args.index("--db") + 1]
                    mois = args[args.index("--mois") + 1] if "--mois" in args else "2026-05"
                    table = ("menages_rapprochement" if "lot6d" in s["name"]
                            else "menages_gainperte" if "lot6e" in s["name"] else None)
                    if table:
                        conn = sqlite3.connect(db_copie)
                        try:
                            conn.execute(f"INSERT INTO {table} (mois, logement_id, "
                                        f"intervenant_id, statut_controle) VALUES (?,?,?,?)",
                                        (mois, "LOG_STUB", "INT_STUB", "VALIDE"))
                            conn.commit()
                        finally:
                            conn.close()
            steps.append({"name": s["name"], "statut": statut,
                          "returncode": 0 if statut == "OK" else 2,
                          "erreur_code": None if statut == "OK" else "E_RUNNER",
                          "detail": "stub", "produces": [{"path": p, "exists": statut == "OK",
                          "sha256": "0"*64 if statut == "OK" else None, "size": 1}
                          for p in s.get("produces", [])]})
            if statut != "OK":
                break
        rep = {"ok": all(st["statut"] == "OK" for st in steps) and len(steps) == len(req["steps"]),
               "workspace": req["workspace"], "steps": steps}
        open(sys.argv[2], "w", encoding="utf-8").write(json.dumps(rep))
    '''), encoding="utf-8")
    monkeypatch.setattr(cfg, "MENAGES_RECALC_RUNNER", script)
    monkeypatch.setattr(cfg, "MENAGES_ENGINE_PYTHON", Path(sys.executable))
    monkeypatch.setenv("STUB_OUTCOME", "ok")
    return script


# ── preparer ─────────────────────────────────────────────────────────────────

def test_preparer_mode_copies(mini_project):
    plan = rc.preparer(rc.MODE_COPIES)
    assert plan["mode"] == "COPIES"
    assert plan["reel_active"] is False
    assert plan["bloque"] is False
    assert plan["etapes"] == ["lot6d_rapprochement", "lot6e_gainperte"]
    assert plan["prete"] is True


def test_preparer_mode_reel_est_bloque(mini_project):
    plan = rc.preparer(rc.MODE_REEL)
    assert plan["bloque"] is True
    assert "MENAGES_REAL_RECALC_ENABLED" in plan["raison"]
    assert plan["prete"] is False


def test_preparer_signale_dataset_non_initialise(mini_project, db):
    """Aucune ligne pour un mois inconnu : `DATASET_NON_INITIALISE`, jamais un plantage."""
    plan = rc.preparer(rc.MODE_COPIES, mois="1999-01")
    assert plan["datasets_absents"]
    assert plan["prete"] is False


# ── confirmer : gardes ───────────────────────────────────────────────────────

def test_confirmer_mode_reel_refuse_sans_rien_executer(mini_project, db):
    res = rc.confirmer(rc.MODE_REEL, db_path=db)
    assert res["statut"] == rc.STATUT_BLOQUE
    assert res["erreur_code"] == "E_MODE_REEL_DESACTIVE"
    # run tracé, aucun workspace créé
    assert not (cfg.MENAGES_RECALC_WORKSPACE).exists()
    run = rc.load_run(res["run_id"], db_path=db)
    assert run["statut"] == "BLOQUE" and run["mode"] == "REEL"


def test_confirmer_reel_ne_pose_ni_verrou_ni_snapshot(mini_project, db):
    """Garde du flag EN PREMIER : aucun verrou, aucun snapshot, aucun workspace si flag False."""
    import sqlite3
    rc.confirmer(rc.MODE_REEL, db_path=db)
    assert verrou_lib.inspecter_verrou(rc._lock_path())["etat"] == verrou_lib.ETAT_ABSENT
    assert not cfg.MENAGES_RECALC_WORKSPACE.exists()
    conn = sqlite3.connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 0
    finally:
        conn.close()


def test_confirmer_reel_journalise_le_refus(mini_project, db):
    import sqlite3
    rc.confirmer(rc.MODE_REEL, db_path=db)
    conn = sqlite3.connect(db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM audit_events WHERE action='MENAGE_RECALC_BLOQUE'").fetchone()[0]
    finally:
        conn.close()
    assert n >= 1


def test_confirmer_flag_reste_false(mini_project, db):
    """Le flag n'est jamais modifié par le service."""
    assert cfg.MENAGES_REAL_RECALC_ENABLED is False
    rc.confirmer(rc.MODE_REEL, db_path=db)
    assert cfg.MENAGES_REAL_RECALC_ENABLED is False


def test_confirmer_dataset_absent_refuse(mini_project, db):
    """Aucune donnée pour le mois demandé : refus explicite, jamais un plantage."""
    res = rc.confirmer(rc.MODE_COPIES, mois="1999-01", db_path=db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == rc.E_DATASET_NON_INITIALISE
    # aucun verrou laissé
    assert verrou_lib.inspecter_verrou(rc._lock_path())["etat"] == verrou_lib.ETAT_ABSENT


def test_confirmer_verrou_deja_pris(mini_project, db):
    v = verrou_lib.acquerir_verrou(operation="autre", lock_path=rc._lock_path())
    try:
        res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    finally:
        verrou_lib.liberer_verrou(v)
    assert res["statut"] == rc.STATUT_VERROUILLE
    assert res["erreur_code"] == "E_VERROU"


def test_confirmer_runner_absent(mini_project, db, monkeypatch):
    monkeypatch.setattr(cfg, "MENAGES_RECALC_RUNNER", mini_project / "inexistant.py")
    monkeypatch.setattr(cfg, "MENAGES_ENGINE_PYTHON", Path(sys.executable))
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_RUNNER"
    assert res["reel_intact"] is True  # rien n'a été écrit dans le réel


# ── confirmer : logique via runner stub ──────────────────────────────────────

def test_confirmer_succes_via_stub(mini_project, db, stub_runner):
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] == rc.STATUT_SUCCES
    assert res["reel_intact"] is True
    assert len(res["etapes"]) == 2 and all(e["statut"] == "OK" for e in res["etapes"])
    # verrou libéré
    assert verrou_lib.inspecter_verrou(rc._lock_path())["etat"] == verrou_lib.ETAT_ABSENT
    # snapshot pris
    assert res["snapshot_id"]


def test_confirmer_echec_via_stub(mini_project, db, stub_runner, monkeypatch):
    monkeypatch.setenv("STUB_OUTCOME", "fail")
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] in (rc.STATUT_ECHEC, rc.STATUT_PARTIEL)
    assert res["erreur_code"] == "E_RUNNER"
    assert verrou_lib.inspecter_verrou(rc._lock_path())["etat"] == verrou_lib.ETAT_ABSENT


def test_confirmer_reponse_absente_via_stub(mini_project, db, stub_runner, monkeypatch):
    monkeypatch.setenv("STUB_OUTCOME", "noreponse")
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_REPONSE_INVALIDE"


def test_deux_runs_produisent_deux_lignes(mini_project, db, stub_runner):
    r1 = rc.confirmer(rc.MODE_COPIES, db_path=db)
    r2 = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert r1["run_id"] != r2["run_id"]
    runs = rc.load_runs(db_path=db)
    assert len(runs) >= 2


def test_confirmer_ne_modifie_aucune_source_reelle(mini_project, db, stub_runner):
    """Les tables DOMAINE ne bougent pas — le journal (snapshots/menages_recalcul_runs/
    audit_events), lui, s'écrit normalement dans la même base à chaque run : une comparaison
    fichier entier détecterait donc TOUJOURS un écart, y compris pour un run correct."""
    avant = rc._empreinte_domaine(Path(db))
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["reel_intact"] is True
    apres = rc._empreinte_domaine(Path(db))
    assert avant == apres


# ── Routes ───────────────────────────────────────────────────────────────────

def test_route_recalculer_form_200(client):
    r = client.get("/menages/recalculer")
    assert r.status_code == 200
    assert "rapprochement" in r.text.lower()


def test_route_run_inconnu_404(client):
    r = client.get("/menages/recalculer/runs/999999")
    assert r.status_code == 404


def test_route_confirmer_reel_redirige_vers_run_bloque(client):
    # PRG : POST → 303 → GET run. Mode réel bloqué, aucune exécution.
    r = client.post("/menages/recalculer/confirmer", data={"mode": "REEL"}, follow_redirects=False)
    assert r.status_code == 303
    loc = r.headers["location"]
    assert loc.startswith("/menages/recalculer/runs/")
    page = client.get(loc)
    assert page.status_code == 200
    assert "BLOQUE" in page.text
    assert "E_MODE_REEL_DESACTIVE" in page.text


def test_route_diagnostic_lie_le_recalcul(client):
    r = client.get("/menages/diagnostic")
    assert r.status_code == 200
    assert "/menages/recalculer" in r.text


def test_routes_recalcul_ne_touchent_pas_la_base_reelle(client):
    """Les routes de recalcul écrivent dans la base de test, jamais la vraie app.db."""
    reelle = cfg.APP_ROOT / "data" / "app.db"
    if not reelle.exists():
        pytest.skip("app.db réelle absente")
    sha_avant = hashlib.sha256(reelle.read_bytes()).hexdigest()
    mtime_avant = reelle.stat().st_mtime_ns
    client.get("/menages/recalculer")
    client.post("/menages/recalculer/confirmer", data={"mode": "REEL"}, follow_redirects=True)
    assert hashlib.sha256(reelle.read_bytes()).hexdigest() == sha_avant
    assert reelle.stat().st_mtime_ns == mtime_avant


# ── Recette E2E réelle (moteur) — copies, réel intact ────────────────────────

# ── Export CSV & explications (sections 9/10/12) ─────────────────────────────

def test_route_export_csv(client):
    r = client.get("/menages/export.csv?mois=2026-05")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers.get("content-disposition", "")
    # En-tête métier, aucun chemin interne
    assert "statut_moteur" in r.text and "cout_complet" in r.text
    assert "C:\\" not in r.text and "02_TRAVAIL" not in r.text


def test_export_csv_filtre_ecart_seul(client):
    complet = client.get("/menages/export.csv?mois=2026-05")
    filtre = client.get("/menages/export.csv?mois=2026-05&ecart_seul=true")
    # Le filtre « avec écart » ne peut pas produire plus de lignes que la vue complète.
    assert filtre.text.count("\n") <= complet.text.count("\n")


def test_explication_controle():
    from app.services import menages_service as svc
    assert "tâches Hostaway" in svc.explication_controle("MENAGE_TOTAL_ECART_HOSTAWAY")
    assert svc.explication_controle("CODE_INEXISTANT_XYZ") == ""


def test_fraicheur_structure(client):
    # La fraîcheur est calculée et exposée sur le tableau de bord.
    r = client.get("/menages")
    assert r.status_code == 200
    fr = rc.etat_fraicheur()
    assert fr["etat"] in ("A_JOUR", "SOURCES_PLUS_RECENTES", "JAMAIS_CALCULE")


@pytest.mark.skipif(not Path(cfg.MENAGES_ENGINE_PYTHON).exists(),
                    reason="Interpréteur moteur (Python312) absent")
def test_recette_e2e_copies_reel_intact(tmp_path, db, monkeypatch):
    """Exécute RÉELLEMENT lot6d+lot6e (vrai interpréteur moteur, vrais scripts du worktree) sur une
    COPIE de `db`, et prouve que `db` elle-même ne bouge jamais.

    `db` porte des données synthétiques (`_seed_datasets`), pas la vraie app.db : app.db réelle
    reste gelée en 0016 (règle du projet) et n'a donc pas les tables 0038 que ce recalcul lit —
    aucune donnée réelle n'est disponible à ce niveau. Le script moteur exécuté, lui, EST le vrai
    script du worktree, aucun stub.
    """
    monkeypatch.setattr(cfg, "PROJECT_ROOT", Path(cfg.APP_ROOT).parent)
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", tmp_path / "snap")
    _seed_datasets(db)
    empreinte_avant = rc._empreinte_domaine(Path(db))

    res = rc.confirmer(rc.MODE_COPIES, db_path=db)

    assert res["statut"] == rc.STATUT_SUCCES, res.get("erreur_resume")
    assert res["reel_intact"] is True
    assert res["comparaison"]["apres"]["nb_lignes"] > 0
    # preuve indépendante : les tables DOMAINE de `db` n'ont pas changé (le journal, lui,
    # reçoit normalement le run — cf. `_empreinte_domaine`).
    assert rc._empreinte_domaine(Path(db)) == empreinte_avant
    # la sortie recalculée existe bien dans la COPIE du workspace, jamais dans `db`
    conn = get_db(Path(res["workspace"]) / "app_data.db")
    try:
        n = conn.execute("SELECT COUNT(*) FROM menages_rapprochement WHERE mois=?",
                         (MOIS_TEST,)).fetchone()[0]
    finally:
        conn.close()
    assert n > 0
    conn = get_db(db)
    try:
        n_reel = conn.execute("SELECT COUNT(*) FROM menages_rapprochement WHERE mois=?",
                              (MOIS_TEST,)).fetchone()[0]
    finally:
        conn.close()
    assert n_reel == 0, "menages_rapprochement ne doit exister QUE dans la copie workspace"

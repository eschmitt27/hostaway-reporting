"""APP-2b — Recalcul ménages sécurisé (sur copies, mode réel gardé).

Deux familles de tests :
  * logique du service avec un RUNNER STUB (python de test) — rapide, déterministe, sans moteur ;
  * une RECETTE E2E réelle qui exécute lot6d/lot6e sur copies et prouve que les fichiers réels
    ne bougent pas (ignorée si l'interpréteur moteur est absent).

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


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


@pytest.fixture
def mini_project(tmp_path, monkeypatch):
    """Faux projet minimal : chaque source/script attendu existe (fichier factice).

    Redirige PROJECT_ROOT, le workspace de recalcul et le répertoire de snapshots vers tmp.
    Aucune écriture ne peut donc atteindre l'arbre réel.
    """
    root = tmp_path / "projet"
    for rel in rc.SOURCES_A_COPIER + rc.SCRIPTS_A_COPIER:
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"FIXTURE " + rel.encode())
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.setattr(snapshot_service, "SNAPSHOTS_DIR", tmp_path / "snapshots")
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
        import json, os, sys
        req = json.loads(open(sys.argv[1], encoding="utf-8").read())
        outcome = os.environ.get("STUB_OUTCOME", "ok")
        if outcome == "noreponse":
            sys.exit(0)
        steps = []
        for i, s in enumerate(req["steps"]):
            statut = "OK"
            if outcome == "fail" and i == 0:
                statut = "ECHEC"
            steps.append({"name": s["name"], "statut": statut,
                          "returncode": 0 if statut == "OK" else 2,
                          "erreur_code": None if statut == "OK" else "E_RUNNER",
                          "detail": "stub", "produces": [{"path": p, "exists": statut == "OK",
                          "sha256": "0"*64 if statut == "OK" else None, "size": 1} for p in s["produces"]]})
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


def test_preparer_signale_excel_ouvert(mini_project):
    src = mini_project / rc.SOURCES_A_COPIER[0]
    (src.parent / ("~$" + src.name)).write_bytes(b"lock")
    plan = rc.preparer(rc.MODE_COPIES)
    assert plan["excel_ouverts"]
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


def test_confirmer_excel_ouvert_refuse(mini_project, db):
    src = mini_project / rc.SOURCES_A_COPIER[0]
    (src.parent / ("~$" + src.name)).write_bytes(b"lock")
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_EXCEL_OUVERT"
    # aucun verrou laissé
    assert verrou_lib.inspecter_verrou(rc._lock_path())["etat"] == verrou_lib.ETAT_ABSENT


def test_confirmer_source_absente_refuse(mini_project, db):
    (mini_project / rc.SOURCES_A_COPIER[1]).unlink()
    res = rc.confirmer(rc.MODE_COPIES, db_path=db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_SOURCE_ABSENTE"


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
    avant = {p: hashlib.sha256((mini_project / p).read_bytes()).hexdigest()
             for p in rc.SOURCES_A_COPIER}
    rc.confirmer(rc.MODE_COPIES, db_path=db)
    apres = {p: hashlib.sha256((mini_project / p).read_bytes()).hexdigest()
             for p in rc.SOURCES_A_COPIER}
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
    """Exécute lot6d+lot6e sur copies des VRAIES sources et prouve que le réel ne bouge pas."""
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(snapshot_service, "SNAPSHOTS_DIR", tmp_path / "snap")
    reel = cfg.PROJECT_ROOT / rc.SORTIES_REELLES["rapprochement"]
    if not reel.exists():
        pytest.skip("MASTER rapprochement réel absent")
    sha_avant = hashlib.sha256(reel.read_bytes()).hexdigest()

    res = rc.confirmer(rc.MODE_COPIES, db_path=db)

    assert res["statut"] == rc.STATUT_SUCCES, res.get("erreur_resume")
    assert res["reel_intact"] is True
    assert res["comparaison"]["apres"]["nb_lignes"] > 0
    # preuve indépendante : le MASTER réel n'a pas changé
    assert hashlib.sha256(reel.read_bytes()).hexdigest() == sha_avant
    # la sortie recalculée existe bien dans le workspace, pas dans le réel
    produit = Path(res["workspace"]) / rc.SORTIES_REELLES["rapprochement"]
    assert produit.exists()

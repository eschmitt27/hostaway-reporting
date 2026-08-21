"""APP-SEC-1 — Sécurisation des diagnostics, chemins locaux, exposition réseau.

35 points d'attaque : /health minimal, diagnostic gardé, sanitisation, erreurs sans détail technique,
headers, garde de démarrage, confidentialité runner. Données factices uniquement.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.services import path_sanitizer as ps

REEL = Path(r"C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie")
WT_ROOT = Path(cfg.APP_ROOT).parent


# ── 1-4 : /health public minimal ──────────────────────────────────────────────

def test_01_health_aucun_chemin(client):
    r = client.get("/health")
    assert r.status_code == 200
    blob = json.dumps(r.json())
    assert "\\" not in blob and "/Users" not in blob


def test_02_health_pas_onedrive(client):
    assert "OneDrive" not in client.get("/health").text


def test_03_health_pas_users(client):
    assert "Users" not in client.get("/health").text


def test_04_health_pas_nom_profil(client):
    assert "Ewan" not in client.get("/health").text


def test_04b_health_contrat_exact(client):
    body = client.get("/health").json()
    assert set(body.keys()) == {"status", "application", "database", "sources", "writers_enabled"}
    assert body["status"] in ("ok", "degraded")
    assert body["writers_enabled"] is False


# ── 5-7 : diagnostic gardé ─────────────────────────────────────────────────────

def test_05_diagnostic_desactive_par_defaut(client):
    assert client.get("/health/diagnostic").status_code == 404


def test_06_diagnostic_ip_non_locale_refuse(client, monkeypatch):
    monkeypatch.setattr(cfg, "DIAGNOSTIC_DETAILS_ENABLED", True)
    from app.routes import health as health_mod
    monkeypatch.setattr(health_mod, "_client_local", lambda req: False)
    r = client.get("/health/diagnostic")
    assert r.status_code == 403


def test_07_diagnostic_local_avec_flag_explicite(client, monkeypatch):
    monkeypatch.setattr(cfg, "DIAGNOSTIC_DETAILS_ENABLED", True)
    r = client.get("/health/diagnostic")
    assert r.status_code == 200
    blob = json.dumps(r.json())
    assert "OneDrive" not in blob and "Users" not in blob and "Ewan" not in blob


# ── 8-9 : sanitisation chemins ────────────────────────────────────────────────

def test_08_sanitise_chemin_windows():
    s = ps.sanitize_text(str(cfg.PROJECT_ROOT) + r"\02_TRAVAIL\x.xlsx")
    assert "<PROJECT_ROOT>" in s and "OneDrive" not in s


def test_09_sanitise_chemin_unix_style():
    s = ps.sanitize_text(str(cfg.PROJECT_ROOT).replace("\\", "/") + "/02_TRAVAIL/x.xlsx")
    assert "<PROJECT_ROOT>" in s and "OneDrive" not in s


def test_09b_sanitise_apres_data_dir_sans_fuite_username():
    s = ps.sanitize_text(str(cfg.DATA_DIR) + r"\snapshots")
    assert "<APP_DATA_DIR>" in s
    assert "Ewan" not in s


# ── 10-11 : exception sanitisée, aucun traceback au client ───────────────────

def test_10_exception_avec_chemin_sanitisee():
    try:
        raise RuntimeError(f"lecture impossible: {cfg.PROJECT_ROOT}\\x.xlsx")
    except RuntimeError as e:
        msg = ps.sanitize_exception(e)
    assert "<PROJECT_ROOT>" in msg and "OneDrive" not in msg


def test_11_traceback_non_retourne_au_client(tmp_db):
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    @fastapi_app.get("/__boom_test__")
    def _boom():
        raise RuntimeError(f"secret path {cfg.PROJECT_ROOT}")

    c = TestClient(fastapi_app, raise_server_exceptions=False)
    r = c.get("/__boom_test__")
    assert r.status_code == 500
    body = r.json()
    assert "Traceback" not in json.dumps(body) and "OneDrive" not in json.dumps(body)
    assert body["detail"].startswith("Une erreur technique est survenue. Référence : ERR-")
    fastapi_app.router.routes = [r for r in fastapi_app.router.routes if getattr(r, "path", "") != "/__boom_test__"]


# ── 12-14 : 404/422/500 sans donnée sensible ──────────────────────────────────

def test_12_404_sans_chemin(client):
    r = client.get("/controles-cloture/element/CTRL-000000000000")
    assert r.status_code == 404 and "OneDrive" not in r.text and "\\" not in r.text


def test_13_422_sans_donnee_sensible(client):
    r = client.get("/banques-caisse?page=abc")
    # FastAPI/Starlette : page:int mal typé -> 422 (ou ignoré selon route) ; jamais de chemin/stack
    assert r.status_code in (200, 422)
    assert "OneDrive" not in r.text


def test_14_500_sans_detail_technique(tmp_db):
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    @fastapi_app.get("/__boom500__")
    def _boom2():
        raise ValueError(f"select * from x where path='{cfg.PROJECT_ROOT}'")

    c = TestClient(fastapi_app, raise_server_exceptions=False)
    r = c.get("/__boom500__")
    assert r.status_code == 500
    assert "select" not in r.text.lower() and "OneDrive" not in r.text
    fastapi_app.router.routes = [r for r in fastapi_app.router.routes if getattr(r, "path", "") != "/__boom500__"]


# ── 15-18 : logs, CSV, HTML, OpenAPI ───────────────────────────────────────────

def test_15_logs_sans_chemin_absolu(capsys, tmp_db):
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    @fastapi_app.get("/__boomlog__")
    def _boom3():
        raise RuntimeError(f"fail at {cfg.PROJECT_ROOT}\\file.xlsx")

    c = TestClient(fastapi_app, raise_server_exceptions=False)
    c.get("/__boomlog__")
    out = capsys.readouterr().out
    assert "OneDrive" not in out
    fastapi_app.router.routes = [r for r in fastapi_app.router.routes if getattr(r, "path", "") != "/__boomlog__"]


def test_16_csv_sans_chemin(client):
    r = client.get("/controles-cloture/export.csv")
    body = r.content.decode("utf-8", errors="replace")
    assert "OneDrive" not in body and "C:\\" not in body


def test_17_html_sans_chemin(client):
    for u in ("/banques-caisse", "/controles-cloture"):
        t = client.get(u).text
        assert "OneDrive" not in t and "C:\\Users" not in t


def test_18_openapi_sans_chemin_metier(client):
    r = client.get("/openapi.json")
    if r.status_code == 200:
        assert "OneDrive" not in r.text and "C:\\Users" not in r.text


# ── 19-22 : headers ────────────────────────────────────────────────────────────

def test_19_cors_non_permissif(client):
    r = client.get("/health")
    assert r.headers.get("access-control-allow-origin") != "*"


def test_20_cache_control_no_store(client):
    for u in ("/health", "/banques-caisse", "/controles-cloture"):
        assert client.get(u).headers.get("cache-control") == "no-store"


def test_21_protection_clickjacking(client):
    assert client.get("/banques-caisse").headers.get("x-frame-options") == "DENY"


def test_22_mime_sniffing_interdit(client):
    assert client.get("/banques-caisse").headers.get("x-content-type-options") == "nosniff"


# ── 23-26 : garde de démarrage (audit statique, sans lancer de vrai serveur) ──

def test_23_host_local_defaut():
    src = (WT_ROOT / "05_APPLICATION" / "run_app.py").read_text(encoding="utf-8")
    assert 'HOST = "127.0.0.1"' in src


def test_24_host_0000_refuse():
    src = (WT_ROOT / "05_APPLICATION" / "run_app.py").read_text(encoding="utf-8")
    assert "_refuse_si_reseau" in src and "_HOTES_LOCAUX" in src
    import importlib
    spec = importlib.util.spec_from_file_location("run_app_test", WT_ROOT / "05_APPLICATION" / "run_app.py")
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(WT_ROOT / "05_APPLICATION"))
    spec.loader.exec_module(mod)
    with pytest.raises(SystemExit):
        mod._refuse_si_reseau("0.0.0.0")


def test_25_ip_lan_refusee_sans_flag():
    import importlib
    spec = importlib.util.spec_from_file_location("run_app_test2", WT_ROOT / "05_APPLICATION" / "run_app.py")
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(WT_ROOT / "05_APPLICATION"))
    spec.loader.exec_module(mod)
    with pytest.raises(SystemExit):
        mod._refuse_si_reseau("192.168.1.50")


def test_26_bind_reseau_autorise_seulement_par_flag(monkeypatch, capsys):
    import importlib
    spec = importlib.util.spec_from_file_location("run_app_test3", WT_ROOT / "05_APPLICATION" / "run_app.py")
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(WT_ROOT / "05_APPLICATION"))
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "ALLOW_NETWORK_BIND", True)
    mod._refuse_si_reseau("192.168.1.50")   # ne lève pas


# ── 27 : flags writers toujours False ─────────────────────────────────────────

def test_27_flags_writers_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.CONTROLES_REAL_WRITE_ENABLED is False
    assert cfg.DIAGNOSTIC_DETAILS_ENABLED is False
    assert cfg.ALLOW_NETWORK_BIND is False


# ── 28-30 : confidentialité runner ────────────────────────────────────────────

def test_28_29_runner_messages_sanitises():
    """Aucun chemin absolu ne doit fuir dans un motif ou un journal de run.

    Le runner ne lance plus de sous-processus : il n'y a plus de `stderr` moteur à assainir. Ce qui
    reste à protéger, ce sont les messages d'échec et le chemin de workspace journalisés — ils
    doivent tous passer par `path_sanitizer`.
    """
    src = (WT_ROOT / "05_APPLICATION" / "app" / "services" / "controles_runner_service.py").read_text(encoding="utf-8")
    assert "path_sanitizer" in src
    assert "_sanitize(f\"Échec : {exc}\")" in src        # message d'exception assaini
    assert "_sanitize(str(ws))" in src                    # chemin de workspace assaini
    # Et plus aucun sous-processus dont il faudrait assainir la sortie.
    assert "import subprocess" not in src


def test_30_journal_sqlite_sanitise():
    src = (WT_ROOT / "05_APPLICATION" / "app" / "services" / "controles_runner_service.py").read_text(encoding="utf-8")
    assert "motif_sanitise" in src


# ── 31-32 : aucun compte/mouvement brut ───────────────────────────────────────

def test_31_32_aucun_compte_mouvement_brut(client):
    for u in ("/banques-caisse", "/controles-cloture"):
        t = client.get(u).text
        assert "00021321603" not in t and "CM_02211" not in t


# ── 33-34 : aucun chemin worktree/temp brut dans les réponses ────────────────

def test_33_aucun_chemin_worktree(client):
    t = client.get("/health").text
    assert str(WT_ROOT) not in t


def test_34_aucun_chemin_temporaire_brut(client):
    import tempfile
    t = client.get("/health").text
    assert tempfile.gettempdir() not in t


# ── 35 : aucun secret fictif renvoyé ───────────────────────────────────────────

def test_35_aucun_secret_dans_health(client):
    t = client.get("/health").text.lower()
    for mot in ("password", "secret", "token", "api_key", "apikey"):
        assert mot not in t


# ── Intégrité réelle ───────────────────────────────────────────────────────────

def test_reel_intact_apres_tests():
    import hashlib
    def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
    b = sha(REEL / "02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx")
    m = sha(REEL / "02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx")
    assert b is None or b.startswith("baca5dbc")
    assert m is None or m.startswith("d4504b33")

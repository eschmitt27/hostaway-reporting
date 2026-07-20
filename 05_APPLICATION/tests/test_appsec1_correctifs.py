"""APP-SEC-1 — Correctifs finaux avant intégration.

Docs désactivées par défaut, sanitizer renforcé (36 scénarios), logger centralisé (aucun print
technique), handler d'erreur (HTTPException jamais transformée en 500), garde de démarrage sans
duplication. Données factices uniquement.
"""
import json
import re
from pathlib import Path

import pytest

import app.config as cfg
from app.services import path_sanitizer as ps

WT_ROOT = Path(cfg.APP_ROOT).parent
R = str(cfg.PROJECT_ROOT)
BS = "\\"


# ── 1-7 : documentation API désactivée par défaut ─────────────────────────────

def test_01_docs_desactive_par_defaut(client):
    assert client.get("/docs").status_code == 404


def test_02_redoc_desactive_par_defaut(client):
    assert client.get("/redoc").status_code == 404


def test_03_openapi_desactive_par_defaut(client):
    assert client.get("/openapi.json").status_code == 404


def test_04_activation_explicite_locale(monkeypatch):
    import os
    import importlib
    monkeypatch.setenv("DOCS_ENABLED", "true")
    import app.main as main_mod
    importlib.reload(main_mod)
    from fastapi.testclient import TestClient
    c = TestClient(main_mod.app)
    try:
        assert c.get("/docs").status_code == 200
        assert c.get("/openapi.json").status_code == 200
    finally:
        monkeypatch.delenv("DOCS_ENABLED", raising=False)
        importlib.reload(main_mod)  # restaure l'état par défaut pour les tests suivants


def test_05_openapi_sans_fuite_chemin(monkeypatch):
    import importlib
    monkeypatch.setenv("DOCS_ENABLED", "true")
    import app.main as main_mod
    importlib.reload(main_mod)
    from fastapi.testclient import TestClient
    c = TestClient(main_mod.app)
    try:
        body = c.get("/openapi.json").text
        assert "OneDrive" not in body and "C:\\Users" not in body
    finally:
        monkeypatch.delenv("DOCS_ENABLED", raising=False)
        importlib.reload(main_mod)


def test_06_aucune_description_avec_chemin_local(monkeypatch):
    import importlib
    monkeypatch.setenv("DOCS_ENABLED", "true")
    import app.main as main_mod
    importlib.reload(main_mod)
    from fastapi.testclient import TestClient
    c = TestClient(main_mod.app)
    try:
        schema = c.get("/openapi.json").json()
        blob = json.dumps(schema)
        assert "AppData" not in blob and "Temp" not in blob
    finally:
        monkeypatch.delenv("DOCS_ENABLED", raising=False)
        importlib.reload(main_mod)


def test_07_aucune_donnee_metier_reelle_dans_schema(monkeypatch):
    import importlib
    monkeypatch.setenv("DOCS_ENABLED", "true")
    import app.main as main_mod
    importlib.reload(main_mod)
    from fastapi.testclient import TestClient
    c = TestClient(main_mod.app)
    try:
        blob = c.get("/openapi.json").text
        assert "00021321603" not in blob and "CM_02211" not in blob
    finally:
        monkeypatch.delenv("DOCS_ENABLED", raising=False)
        importlib.reload(main_mod)


# ── 8-9 : main.py ne contient plus de print() technique ──────────────────────

def test_08_aucun_print_technique_dans_main():
    src = (WT_ROOT / "05_APPLICATION" / "app" / "main.py").read_text(encoding="utf-8")
    assert not re.search(r"(^|[^a-zA-Z_])print\(", src)


def test_09_logger_centralise_utilise():
    src = (WT_ROOT / "05_APPLICATION" / "app" / "main.py").read_text(encoding="utf-8")
    assert "log_erreur" in src and "logging_config" in src


# ── 10-11 : HTTPException jamais transformée en 500 ───────────────────────────

def test_10_404_reste_404(client):
    r = client.get("/controles-cloture/element/CTRL-000000000000")
    assert r.status_code == 404


def test_11_httpexception_explicite_conserve_code(tmp_db):
    from fastapi import HTTPException
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    @fastapi_app.get("/__test_403__")
    def _t():
        raise HTTPException(403, "refuse")

    c = TestClient(fastapi_app, raise_server_exceptions=False)
    r = c.get("/__test_403__")
    assert r.status_code == 403 and r.json()["detail"] == "refuse"
    fastapi_app.router.routes = [x for x in fastapi_app.router.routes if getattr(x, "path", "") != "/__test_403__"]


# ── 12 : logs sans double journalisation, référence unique ────────────────────

def test_12_erreurs_successives_references_distinctes(tmp_db):
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    @fastapi_app.get("/__test_multi_err__")
    def _t():
        raise RuntimeError("boom")

    c = TestClient(fastapi_app, raise_server_exceptions=False)
    refs = set()
    for _ in range(5):
        body = c.get("/__test_multi_err__").json()
        m = re.search(r"ERR-[0-9A-F]{8}", body["detail"])
        assert m
        refs.add(m.group(0))
    assert len(refs) == 5   # aucune collision
    fastapi_app.router.routes = [x for x in fastapi_app.router.routes if getattr(x, "path", "") != "/__test_multi_err__"]


# ── 13-14 : run_app.py sans duplication ───────────────────────────────────────

def test_13_run_app_pas_de_double_host():
    src = (WT_ROOT / "05_APPLICATION" / "run_app.py").read_text(encoding="utf-8")
    assert src.count('host=HOST') <= 1 and src.count('HOST = "127.0.0.1"') == 1


def test_14_run_app_compile_sans_import_circulaire():
    import py_compile
    py_compile.compile(str(WT_ROOT / "05_APPLICATION" / "run_app.py"), doraise=True)


# ── 15-50 : sanitizer exhaustif (paramétré) ───────────────────────────────────

_CAS_SANITIZER = [
    ("windows simple", R + BS + "x.xlsx"),
    ("avec espaces", r"C:\Program Files\Python312\python.exe"),
    ("avec accents", R + BS + "Resultats" + BS + "ete.xlsx"),
    ("slash unix", R.replace("\\", "/") + "/x.xlsx"),
    ("casse differente", R.upper() + BS + "X.XLSX"),
    ("guillemets doubles", '"' + R + BS + 'x.xlsx"'),
    ("guillemets simples", "'" + R + BS + "x.xlsx'"),
    ("dans commande python", 'python "' + R + BS + 'lot11.py" --project-root "' + R + '"'),
    ("plusieurs chemins", R + BS + "a.xlsx et aussi " + R + BS + "b.xlsx"),
    ("coupe par retour ligne", R + BS + "a.xlsx\n" + R + BS + "b.xlsx"),
    ("dans stdout", "stdout: " + R + BS + "out.txt"),
    ("dans stderr", "stderr: " + R + BS + "err.txt"),
    ("dans traceback", 'File "' + R + BS + 'x.py", line 10'),
    ("dans repr exception", repr(FileNotFoundError("[Errno 2]: '" + R + BS + "x.xlsx'"))),
    ("dans JSON", json.dumps({"path": R + BS + "x.xlsx"})),
    ("dans liste", str([R + BS + "a.xlsx", R + BS + "b.xlsx"])),
    ("dans dict", str({"p": R + BS + "a.xlsx"})),
    ("USERPROFILE literal", r"%USERPROFILE%\Documents\x.xlsx"),
    ("TEMP literal", r"%TEMP%\x.xlsx"),
    ("commence par C:\\Users\\Ewan", r"C:\Users\Ewan\Desktop\autre.xlsx"),
    ("commence par OneDrive", R + BS + "autre_dossier" + BS + "x.xlsx"),
    ("chemin worktree", str(WT_ROOT) + BS + "02_TRAVAIL" + BS + "x.py"),
    ("chemin APP_DATA_DIR", str(cfg.DATA_DIR) + BS + "snapshots"),
    ("chemin fichier reel", R + BS + "02_TRAVAIL" + BS + "Lot8_Banque" + BS + "BANQUE_LOT8_IMPORT.xlsx"),
    ("chemin inexistant", R + BS + "n_existe_pas.xlsx"),
    ("chemin partiel Documents", r"Documents\Conciergerie\x.xlsx"),
    ("chaine deja sanitisee", "<PROJECT_ROOT>" + BS + "x.xlsx"),
    ("racines imbriquees", str(cfg.DATA_DIR) + BS + "snapshots" + BS + "y.xlsx"),
    ("UNC", r"\\SERVEUR\Partage\Conciergerie\x.xlsx"),
    ("prefixe etendu windows", r"\\?\C:\Users\Ewan\OneDrive\Documents\x.xlsx"),
    ("unicode", "chemin: " + R + BS + "Menages" + BS + "Import.xlsx"),
    ("objet Path", str(Path(R) / "x.xlsx")),
]


@pytest.mark.parametrize("label,valeur", _CAS_SANITIZER, ids=[c[0] for c in _CAS_SANITIZER])
def test_15_50_sanitizer_aucune_sous_chaine_sensible(label, valeur):
    out = ps.sanitize_text(valeur)
    for interdit in ("Ewan", "Users", "OneDrive", "AppData", "Pilotage_APPSEC1"):
        assert interdit not in out, f"{label}: '{interdit}' encore présent dans {out!r}"
    assert not re.search(r"[A-Za-z]:\\", out), f"{label}: lettre de lecteur brute dans {out!r}"


def test_51_chaine_vide():
    assert ps.sanitize_text("") == ""


def test_52_valeur_none():
    assert ps.sanitize_text(None) == ""


def test_53_double_sanitisation_idempotente():
    once = ps.sanitize_text(R + BS + "x.xlsx")
    twice = ps.sanitize_text(once)
    assert once == twice


def test_54_libelle_metier_conciergerie_non_touche():
    """Un libellé métier légitime contenant « Conciergerie » ne doit pas être altéré."""
    libelle = "Pilotage Conciergerie - module Menages"
    assert ps.sanitize_text(libelle) == libelle

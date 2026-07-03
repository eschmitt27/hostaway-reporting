"""APP-0 — Test boot : l'app démarre et les routes répondent."""
from fastapi.testclient import TestClient


def test_app_imports():
    from app.main import app
    assert app is not None


def test_home_responds(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "Chouette" in r.text or "Accueil" in r.text


def test_sources_calculs_responds(client):
    r = client.get("/sources-calculs")
    assert r.status_code == 200


def test_health_responds(client):
    r = client.get("/health")
    assert r.status_code in (200, 503)
    data = r.json()
    assert "status" in data
    assert "checks" in data


def test_static_css_accessible(client):
    r = client.get("/static/css/app.css")
    assert r.status_code == 200
    assert "--color-primary" in r.text


def test_logo_accessible(client):
    r = client.get("/static/img/logo-main.png")
    assert r.status_code == 200


def test_gitignore_excludes_db():
    from pathlib import Path
    gi = Path(__file__).parent.parent / ".gitignore"
    content = gi.read_text(encoding="utf-8")
    assert "data/app.db" in content
    assert ".env" in content
    assert "snapshots" in content

"""Couche HTTP du pilotage des calculs : page, prévisualisation, lancement, suivi, clôture."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.services import calculs_executeur_service as ex
from app.services import calculs_pipeline_service as pipe


@pytest.fixture
def env(tmp_db, tmp_path, monkeypatch):
    racine = tmp_path / "projet"
    (racine / "02_TRAVAIL").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "Charges").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "REF_Setup").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Flux.xlsx").write_bytes(b"x")
    (racine / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm").write_bytes(b"y")
    (racine / "02_TRAVAIL" / "faux.py").write_text(
        "from pathlib import Path\n"
        "p = Path('out/f.txt'); p.parent.mkdir(parents=True, exist_ok=True)\n"
        "p.write_text('v1')\nprint('faux ok')\n", encoding="utf-8")

    monkeypatch.setattr(cfg, "PROJECT_ROOT", racine)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "LOT4A_ENGINE_PYTHON", Path(sys.executable))
    monkeypatch.setattr(cfg, "DRYRUNS_DIR", tmp_path / "dryruns")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")

    tous = dict(ex.TOUS_LES_LOTS)
    tous["faux"] = ex.Lot("faux", "faux.py", sorties=("out/f.txt",), requiert_pandas=False)
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)
    from app.routes import calculs as route_calculs
    monkeypatch.setitem(route_calculs.CHAINES, "aval", ["faux"])
    return {"db": tmp_db, "racine": racine}


# ── Page d'accueil ───────────────────────────────────────────────────────────

def test_page_calculs_accessible(client, env):
    html = client.get("/calculs?mois=2026-06").text
    assert "Pilotage des calculs" in html
    assert "Interpréteur des lots" in html
    assert 'data-testid="cloture"' in html


def test_page_affiche_environnement_et_mode(client, env):
    html = client.get("/calculs?mois=2026-06").text
    assert sys.executable.replace("\\", "\\") in html or "python" in html.lower()
    assert "RECETTE" in html


def test_nav_expose_calculs(client, env):
    assert 'href="/calculs"' in client.get("/calculs").text


def test_page_affiche_les_lots_de_la_chaine(client, env):
    html = client.get("/calculs?mois=2026-06").text
    assert "faux" in html
    assert "out/f.txt" in html


def test_page_expose_la_selection_de_lots(client, env):
    """« Rejouer un lot » : chaque lot est cochable dans le formulaire de lancement."""
    html = client.get("/calculs?mois=2026-06").text
    assert 'data-testid="selection-lots"' in html
    assert 'name="lots" value="faux"' in html


# ── Sélection de lots (rejouer un lot isolé) ─────────────────────────────────

def test_selection_vide_execute_toute_la_chaine(client, env, monkeypatch):
    from app.routes import calculs as rc
    monkeypatch.setitem(rc.CHAINES, "aval", ["faux", "faux2"])
    tous = dict(ex.TOUS_LES_LOTS)
    tous["faux2"] = ex.Lot("faux2", "faux.py", sorties=("out/f.txt",), requiert_pandas=False)
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)

    r = client.post("/calculs/previsualiser", data={"mois": "2026-06", "chaine": "aval"},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    assert pipe.charger_manifest(token)["lots"] == ["faux", "faux2"]


def test_selection_d_un_seul_lot_le_rejoue_isolement(client, env, monkeypatch):
    from app.routes import calculs as rc
    monkeypatch.setitem(rc.CHAINES, "aval", ["faux", "faux2"])
    tous = dict(ex.TOUS_LES_LOTS)
    tous["faux2"] = ex.Lot("faux2", "faux.py", sorties=("out/f.txt",), requiert_pandas=False)
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)

    r = client.post("/calculs/previsualiser",
                    data={"mois": "2026-06", "chaine": "aval", "lots": ["faux2"]},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    assert pipe.charger_manifest(token)["lots"] == ["faux2"]


def test_ordre_de_la_chaine_prime_sur_l_ordre_du_formulaire(client, env, monkeypatch):
    """Le formulaire ne doit jamais décider de l'ordre d'exécution des moteurs."""
    from app.routes import calculs as rc
    monkeypatch.setitem(rc.CHAINES, "aval", ["faux", "faux2"])
    tous = dict(ex.TOUS_LES_LOTS)
    tous["faux2"] = ex.Lot("faux2", "faux.py", sorties=("out/f.txt",), requiert_pandas=False)
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)

    r = client.post("/calculs/previsualiser",
                    data={"mois": "2026-06", "chaine": "aval", "lots": ["faux2", "faux"]},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    assert pipe.charger_manifest(token)["lots"] == ["faux", "faux2"]


def test_lot_inconnu_ignore_et_repli_sur_la_chaine(client, env):
    r = client.post("/calculs/previsualiser",
                    data={"mois": "2026-06", "chaine": "aval", "lots": ["inexistant"]},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    assert pipe.charger_manifest(token)["lots"] == ["faux"]


# ── Prévisualisation ─────────────────────────────────────────────────────────

def test_previsualiser_puis_lancer(client, env):
    r = client.post("/calculs/previsualiser", data={"mois": "2026-06", "chaine": "aval"},
                    follow_redirects=False)
    assert r.status_code == 303
    loc = r.headers["location"]
    assert "/calculs/previsualisation/" in loc

    html = client.get(loc).text
    assert 'data-testid="bloc-confirmation"' in html
    assert "Aucun lot n'a encore été exécuté" in html
    assert not (env["racine"] / "out" / "f.txt").exists()      # rien lancé

    token = loc.rsplit("/", 1)[-1]
    r2 = client.post(f"/calculs/lancer/{token}", data={"acteur": "recette"},
                     follow_redirects=False)
    assert r2.status_code == 303
    assert "/calculs/runs/RUN-" in r2.headers["location"]
    assert (env["racine"] / "out" / "f.txt").exists()          # lancé après confirmation


def test_previsualisation_token_inconnu_404(client, env):
    assert client.get("/calculs/previsualisation/inexistant").status_code == 404


def test_lancement_refuse_si_entrees_modifiees(client, env):
    r = client.post("/calculs/previsualiser", data={"mois": "2026-06", "chaine": "aval"},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    (env["racine"] / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm").write_bytes(b"CHANGE")
    r2 = client.post(f"/calculs/lancer/{token}", follow_redirects=False)
    html = client.get(r2.headers["location"]).text
    assert "entrées ont changé" in html


# ── Suivi d'un run ───────────────────────────────────────────────────────────

def test_page_run_affiche_le_suivi(client, env):
    r = client.post("/calculs/previsualiser", data={"mois": "2026-06", "chaine": "aval"},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    r2 = client.post(f"/calculs/lancer/{token}", follow_redirects=False)
    html = client.get(r2.headers["location"]).text
    assert "Suivi des lots" in html
    assert "SUCCES" in html
    assert "faux ok" in html                     # stdout consultable


def test_run_inconnu_404(client, env):
    assert client.get("/calculs/runs/RUN-INEXISTANT").status_code == 404


def test_restauration_depuis_la_page_run(client, env):
    sortie = env["racine"] / "out" / "f.txt"
    sortie.parent.mkdir(parents=True, exist_ok=True)
    sortie.write_text("ORIGINAL")

    r = client.post("/calculs/previsualiser", data={"mois": "2026-06", "chaine": "aval"},
                    follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    r2 = client.post(f"/calculs/lancer/{token}", follow_redirects=False)
    run_url = r2.headers["location"]
    assert sortie.read_text() == "v1"

    run_id = run_url.rsplit("/", 1)[-1]
    r3 = client.post(f"/calculs/runs/{run_id}/restaurer", follow_redirects=False)
    html = client.get(r3.headers["location"]).text
    assert "restauré" in html
    assert sortie.read_text() == "ORIGINAL"


# ── Clôture ──────────────────────────────────────────────────────────────────

def test_transition_cloture_via_http(client, env):
    r = client.post("/calculs/cloture/2026-06", data={"statut": pipe.CL_EN_CALCUL},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Clôture : EN_CALCUL" in html


def test_validation_cloture_refusee_si_conditions_non_reunies(client, env):
    client.post("/calculs/cloture/2026-06", data={"statut": pipe.CL_EN_CALCUL})
    client.post("/calculs/cloture/2026-06", data={"statut": pipe.CL_A_CONTROLER})
    r = client.post("/calculs/cloture/2026-06", data={"statut": pipe.CL_VALIDEE},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Conditions de clôture non réunies" in html


def test_transition_interdite_via_http(client, env):
    r = client.post("/calculs/cloture/2026-06", data={"statut": pipe.CL_CLOTUREE},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Transition de clôture interdite" in html


def test_conditions_affichees_sur_la_page(client, env):
    html = client.get("/calculs?mois=2026-06").text
    assert "run reussi" in html or "run_reussi" in html.replace("_", "_")
    assert "Contrôles bloquants" in html

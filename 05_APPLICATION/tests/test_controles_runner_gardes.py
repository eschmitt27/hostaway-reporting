"""APP-5B — Tests d'attaque des garde-fous du runner (post-incident 18/07).

Vérifient que le runner REFUSE les configurations dangereuses SANS jamais lancer le moteur sur des
données réelles. Fichiers factices uniquement ; aucune écriture réelle ; réel intact.
"""
import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.services import controles_runner_service as runner

SCRIPTS = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
scripts_presents = pytest.mark.skipif(
    not (SCRIPTS / "lot8c_rapprochement_banque.py").exists(), reason="scripts moteur absents")


def _sha_si_present(p: Path) -> str | None:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


# Capturé à la collection du module (avant l'exécution de tout test de ce fichier) — preuve RELATIVE
# que les tests d'attaque des garde-fous ci-dessous n'ont touché aucun fichier réel, jamais un hash
# figé dans le temps qui casserait à chaque mise à jour légitime des données métier réelles.
_HASH_AVANT_BANQUE = _sha_si_present(Path(cfg.MASTER_BANQUE))
_HASH_AVANT_CTRL = _sha_si_present(Path(cfg.MASTER_CTRL_COHERENCE))
reels_presents = pytest.mark.skipif(
    _HASH_AVANT_BANQUE is None or _HASH_AVANT_CTRL is None,
    reason="BANQUE_LOT8_IMPORT.xlsx et/ou MASTER_CTRL_Coherence.xlsx absent(s)")


# ── Garde 1/9 : workspace imbriqué dans l'arbre réel ─────────────────────────

def test_01_09_workspace_dans_reel_refuse(monkeypatch, tmp_path):
    faux_reel = tmp_path / "faux_reel"
    faux_reel.mkdir()
    monkeypatch.setattr(cfg, "PROJECT_ROOT", faux_reel)
    ws_dans_reel = faux_reel / "data" / "controles_runner" / "run1"
    ok, motif = runner._valider_workspace_isole(ws_dans_reel)
    assert ok is False and "imbriqué" in motif.lower()


def test_01b_workspace_egal_reel_refuse(monkeypatch, tmp_path):
    faux_reel = tmp_path / "faux_reel"; faux_reel.mkdir()
    monkeypatch.setattr(cfg, "PROJECT_ROOT", faux_reel)
    ok, _ = runner._valider_workspace_isole(faux_reel)
    assert ok is False


def test_01c_workspace_hors_reel_accepte(monkeypatch, tmp_path):
    faux_reel = tmp_path / "faux_reel"; faux_reel.mkdir()
    autre = tmp_path / "workspace" / "run1"
    monkeypatch.setattr(cfg, "PROJECT_ROOT", faux_reel)
    ok, _ = runner._valider_workspace_isole(autre)
    assert ok is True


# ── Garde 2 : scripts du repo réel refusés ───────────────────────────────────

def test_02_scripts_dans_reel_refuse(monkeypatch, tmp_path):
    # si _scripts_dir est sous PROJECT_ROOT réel → refus
    monkeypatch.setattr(cfg, "PROJECT_ROOT", Path(runner._scripts_dir()).parent)
    ok, motif = runner._valider_workspace_isole(tmp_path / "ws")
    assert ok is False and "scripts" in motif.lower()


def test_02b_scripts_dir_est_le_worktree_pas_le_reel():
    # _scripts_dir = arbre applicatif isolé (APP_ROOT.parent), jamais cfg.PROJECT_ROOT réel
    assert runner._scripts_dir() == Path(cfg.APP_ROOT).parent / "02_TRAVAIL"


# ── Garde 3 : script sans marqueur d'injection refusé ────────────────────────

def test_03_script_sans_marqueur_refuse(monkeypatch, tmp_path):
    faux = tmp_path / "02_TRAVAIL"
    faux.mkdir()
    (faux / "lot8c_rapprochement_banque.py").write_text("print('pas de marqueur')", encoding="utf-8")
    (faux / "lot11_controles_coherence.py").write_text("print('pas de marqueur')", encoding="utf-8")
    monkeypatch.setattr(runner, "_scripts_dir", lambda: faux)
    assert runner._script_injecte("lot8c_rapprochement_banque.py") is False
    assert runner._script_injecte("lot11_controles_coherence.py") is False


def test_03b_scripts_reels_ont_le_marqueur():
    assert runner._script_injecte("lot8c_rapprochement_banque.py") is True
    assert runner._script_injecte("lot11_controles_coherence.py") is True


# ── Gardes 4/5/8 : chemins échappant au workspace refusés ────────────────────

def test_04_chemin_parent_hors_workspace(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir()
    dehors = tmp_path / "autre" / "fichier.xlsx"
    dehors.parent.mkdir(); dehors.write_text("x")
    hors = runner._chemins_hors_workspace([str(dehors)], ws)
    assert hors == [str(dehors)]


def test_05_chemin_absolu_reel_hors_workspace(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir()
    reel = str(Path(cfg.MASTER_BANQUE))   # chemin absolu réel
    hors = runner._chemins_hors_workspace([reel], ws)
    assert hors == [reel]


def test_08_chemin_dans_workspace_accepte(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir()
    dedans = ws / "02_TRAVAIL" / "f.xlsx"
    dedans.parent.mkdir(parents=True); dedans.write_text("x")
    assert runner._chemins_hors_workspace([str(dedans)], ws) == []


# ── Garde 6 : symlink sortant du workspace refusé (resolve suit le lien) ──────

def test_06_symlink_vers_reel_hors_workspace(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir()
    cible = tmp_path / "reel_cible.xlsx"; cible.write_text("x")
    lien = ws / "lien.xlsx"
    try:
        lien.symlink_to(cible)
    except (OSError, NotImplementedError):
        pytest.skip("symlink non autorisé dans cet environnement")
    # resolve() suit le lien -> hors workspace -> refusé
    assert runner._chemins_hors_workspace([str(lien)], ws) == [str(lien)]


# ── Garde 10 : scripts refusent --no-real-write sans racine explicite ────────

@scripts_presents
def test_10_no_real_write_sans_root_refuse():
    """`--no-real-write` sans --project-root/env → refus explicite, AUCUNE écriture réelle.

    Utilise l'interpréteur moteur (pandas requis par Lot11) ; sinon le run échoue à l'import bien
    avant toute écriture — dans les deux cas, aucune écriture réelle n'est possible.
    """
    py = runner._engine_python()
    if py is None:
        pytest.skip("aucun python avec pandas")
    for script in ("lot8c_rapprochement_banque.py", "lot11_controles_coherence.py"):
        r = subprocess.run([py, str(SCRIPTS / script), "--no-real-write"],
                           capture_output=True, text=True, timeout=90)
        assert r.returncode != 0, f"{script} aurait dû refuser"
        assert "no-real-write" in (r.stdout + r.stderr).lower()


def test_10b_garde_no_real_write_presente_dans_les_scripts():
    for script in ("lot8c_rapprochement_banque.py", "lot11_controles_coherence.py"):
        src = (SCRIPTS / script).read_text(encoding="utf-8", errors="ignore")
        assert "--no-real-write exige" in src and "sys.exit" in src


# ── Intégrité : les tests de garde n'ont touché aucun fichier réel ───────────

@reels_presents
def test_11_reel_intact_apres_gardes():
    """Comparaison relative (hash à la collection du module vs hash à l'exécution) — jamais un hash
    figé, qui casserait dès que les données réelles évoluent légitimement entre deux sessions."""
    assert _sha_si_present(Path(cfg.MASTER_BANQUE)) == _HASH_AVANT_BANQUE
    assert _sha_si_present(Path(cfg.MASTER_CTRL_COHERENCE)) == _HASH_AVANT_CTRL

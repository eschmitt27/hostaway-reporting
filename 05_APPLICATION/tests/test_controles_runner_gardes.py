"""APP-5B — Tests d'attaque des garde-fous du runner (post-incident 18/07).

Vérifient que le runner REFUSE les configurations dangereuses et ne touche jamais de données
réelles. Fichiers factices uniquement ; aucune écriture réelle ; réel intact.

MISE À JOUR — RUNNER 100% SQLITE (fermeture de Lot11)
Le runner ne lance plus de sous-processus moteur et n'écrit plus de classeur : il recalcule les
contrôles via `controles_lot11_service` sur une COPIE de la base. Les gardes qui protégeaient
l'exécution de scripts (marqueur d'injection, chemins de classeurs confinés au workspace) n'ont
plus d'objet côté runner — elles sont remplacées par les gardes qui comptent désormais : workspace
hors de l'arbre réel, base réelle intacte, aucun sous-processus ni classeur dans le runner. La garde
`--no-real-write` des scripts legacy reste vérifiée : les scripts existent toujours et doivent
rester sûrs si quelqu'un les lance à la main.
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
from app.services import controles_runner_service as runner

SCRIPTS = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
scripts_presents = pytest.mark.skipif(
    not (SCRIPTS / "lot8c_rapprochement_banque.py").exists(), reason="scripts moteur absents")


def _sha_si_present(p: Path) -> str | None:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


# Capturé à la collection du module — preuve RELATIVE que ces tests n'ont touché aucun fichier réel,
# jamais un hash figé qui casserait à chaque mise à jour légitime des données métier réelles.
_HASH_AVANT_BANQUE = _sha_si_present(Path(cfg.MASTER_BANQUE))
_HASH_AVANT_CTRL = _sha_si_present(Path(cfg.MASTER_CTRL_COHERENCE))
reels_presents = pytest.mark.skipif(
    _HASH_AVANT_BANQUE is None or _HASH_AVANT_CTRL is None,
    reason="BANQUE_LOT8_IMPORT.xlsx et/ou MASTER_CTRL_Coherence.xlsx absent(s)")


# ── Garde 1 : workspace imbriqué dans l'arbre réel ───────────────────────────

def test_01_workspace_dans_reel_refuse(monkeypatch, tmp_path):
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


# ── Garde 2 : le runner n'exécute plus rien et n'écrit plus de classeur ──────

def test_02_runner_sans_sous_processus_ni_classeur():
    """Structurel : la boucle SQLite → XLSX → moteur → SQLite ne doit pas revenir par inadvertance.

    Un `subprocess` ou un `openpyxl` réapparaissant ici signifierait qu'on a recréé la dépendance
    Excel que la fermeture de Lot11 a supprimée.
    """
    src = (Path(cfg.APP_ROOT) / "app" / "services" / "controles_runner_service.py").read_text(
        encoding="utf-8")
    interdits = [m for m in ("import subprocess", "import openpyxl", "load_workbook", "Workbook(")
                 if m in src]
    assert not interdits, f"Le runner ne doit plus exécuter de moteur ni écrire de classeur : {interdits}"


def test_02b_runner_appelle_le_meme_moteur_que_l_application():
    """Une seule implémentation des règles Lot11 : le runner recalcule via `controles_lot11_service`,
    exactement comme l'application — jamais une seconde logique de contrôle."""
    src = (Path(cfg.APP_ROOT) / "app" / "services" / "controles_runner_service.py").read_text(
        encoding="utf-8")
    assert "controles_lot11_service" in src


# ── Garde 3 : flags d'écriture réelle → refus ────────────────────────────────

def test_03_flags_ecriture_reelle_refusent(monkeypatch):
    monkeypatch.setattr(cfg, "CONTROLES_REAL_WRITE_ENABLED", True)
    ok, motif = runner._preflight()
    assert ok is False and "flags" in motif.lower()


def test_03b_flags_desactives_acceptent(monkeypatch):
    monkeypatch.setattr(cfg, "CONTROLES_REAL_WRITE_ENABLED", False)
    monkeypatch.setattr(cfg, "CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED", False)
    ok, _ = runner._preflight()
    assert ok is True


# ── Garde 4 : la copie de base est une VRAIE copie, la source reste intacte ──

def test_04_copie_base_ne_modifie_pas_la_source(tmp_path):
    from app.db.connection import apply_migrations, get_db

    source = tmp_path / "source.db"
    apply_migrations(source)
    sha_avant = hashlib.sha256(source.read_bytes()).hexdigest()

    copie = tmp_path / "copie" / "copie.db"
    runner._copier_base(source, copie)
    assert copie.exists()

    # Écrire dans la COPIE ne doit rien changer à la source.
    conn = get_db(copie)
    try:
        conn.execute("INSERT INTO controles_lot11_runs (run_id, source, statut) "
                     "VALUES ('R-TEST', 'SQLITE_NATIF', 'SUCCES')")
        conn.commit()
    finally:
        conn.close()

    assert hashlib.sha256(source.read_bytes()).hexdigest() == sha_avant


# ── Garde 5 : la garde `--no-real-write` des scripts legacy reste en place ───

@scripts_presents
def test_05_garde_no_real_write_presente_dans_les_scripts():
    """Le runner ne les lance plus, mais ces scripts existent toujours : leur garde doit rester."""
    for script in ("lot8c_rapprochement_banque.py", "lot11_controles_coherence.py"):
        src = (SCRIPTS / script).read_text(encoding="utf-8", errors="ignore")
        assert "--no-real-write exige" in src and "sys.exit" in src


# ── Intégrité : les tests de garde n'ont touché aucun fichier réel ───────────

@reels_presents
def test_06_reel_intact_apres_gardes():
    assert _sha_si_present(Path(cfg.MASTER_BANQUE)) == _HASH_AVANT_BANQUE
    assert _sha_si_present(Path(cfg.MASTER_CTRL_COHERENCE)) == _HASH_AVANT_CTRL

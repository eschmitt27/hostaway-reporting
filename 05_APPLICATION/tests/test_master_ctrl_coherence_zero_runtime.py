"""§9 — `MASTER_CTRL_Coherence.xlsx` : ZÉRO dépendance au runtime.

Le classeur de contrôles produit par le moteur legacy ne doit plus être ouvert par quoi que ce soit
au runtime : ni le service de contrôles, ni le runner de recalcul, ni les lecteurs, ni les écrans.
Lot11 étant SQLite natif (`controles_lot11_service`), il n'existe plus qu'une seule implémentation
des règles — le classeur n'est plus qu'une sortie historique du moteur legacy, utile en parité,
jamais en exécution.

La preuve est ACTIVE : toute tentative d'ouverture du classeur permanent fait échouer le test
immédiatement, quel que soit le chemin de code emprunté.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg

INTERDIT = "MASTER_CTRL_Coherence.xlsx"


@pytest.fixture(autouse=True)
def _interdire_master_ctrl(monkeypatch):
    import openpyxl

    original = openpyxl.load_workbook

    def garde(chemin, *a, **kw):
        if Path(chemin).name == INTERDIT:
            raise AssertionError(
                f"MASTER_CTRL_Coherence.xlsx ouvert au runtime : {chemin}. Lot11 est SQLite natif, "
                "plus aucun chemin d'exécution ne doit lire ce classeur.")
        return original(chemin, *a, **kw)

    monkeypatch.setattr(openpyxl, "load_workbook", garde)


def test_service_controles_recalcule_sans_le_classeur(tmp_db):
    from app.services import controles_lot11_service as svc

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res


def test_runner_recalcule_sans_le_classeur(tmp_db, tmp_path, monkeypatch):
    """Le runner de recalcul sur copie ne passe plus par le classeur ni par un sous-processus."""
    from app.services import controles_runner_service as runner

    faux_reel = tmp_path / "faux_reel"
    faux_reel.mkdir()
    monkeypatch.setattr(cfg, "PROJECT_ROOT", faux_reel)
    monkeypatch.setattr(cfg, "CONTROLES_RUNNER_WORKSPACE", tmp_path / "ws")
    element = {"ctrl_opaque": "CTRL-X", "module": "BANQUE", "code": "PEU_IMPORTE",
               "mois": "2099-01", "entite_id": "INCONNU"}
    res = runner.recalculer_sur_copie(element, appliquer_classification=False, db_path=tmp_db)
    # Le verdict importe peu ici (aucune donnée bancaire) : ce qui compte est qu'aucun classeur
    # n'ait été ouvert et qu'aucune exception ne remonte.
    assert res["statut"] in ("SUCCES", "ECHEC", "BLOQUE")
    assert "verdict" in res


def test_lecteur_menages_lit_les_constats_en_base(tmp_db):
    from app.readers import menages_reader as reader
    from app.services import controles_lot11_service as svc

    svc.construire(db_path=tmp_db)
    reader.vider_cache()
    source = reader.controles_lot11()
    assert source is not None
    reader.vider_cache()


def test_ecrans_controles_repondent_sans_le_classeur(client, tmp_db):
    """Les écrans de contrôles/clôture restent verts sans jamais ouvrir le classeur."""
    from app.services import controles_lot11_service as svc

    svc.construire(db_path=tmp_db)
    for url in ("/controles-cloture", "/controles-cloture?mois=2026-06"):
        r = client.get(url)
        assert r.status_code == 200, (url, r.status_code)


def test_aucun_module_applicatif_ne_reference_le_classeur():
    """Garde structurelle : plus aucun module `app/` ne doit nommer ce classeur pour le LIRE.

    `cfg.MASTER_CTRL_COHERENCE` reste défini (le chemin sert encore aux comparaisons de parité et
    aux vérifications d'intégrité « le réel n'a pas bougé »), mais aucun service ne doit l'ouvrir.
    """
    app_dir = Path(cfg.APP_ROOT) / "app"
    coupables = []
    for py in app_dir.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        src = py.read_text(encoding="utf-8", errors="ignore")
        if "MASTER_CTRL_COHERENCE" not in src and INTERDIT not in src:
            continue
        # Seule une LECTURE est interdite : nommer le chemin pour en vérifier l'empreinte est permis.
        for ligne in src.splitlines():
            if ("MASTER_CTRL_COHERENCE" in ligne or INTERDIT in ligne) and (
                    "load_workbook" in ligne or "read_sheet" in ligne or "list_sheets" in ligne):
                coupables.append(f"{py.name}: {ligne.strip()[:90]}")
    assert not coupables, "Lecture runtime du classeur Lot11 :\n" + "\n".join(coupables)

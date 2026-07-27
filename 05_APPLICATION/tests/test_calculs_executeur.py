"""Exécuteur de lots : interpréteur, prérequis, capture, timeout, jamais de faux succès."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.services import calculs_executeur_service as ex


@pytest.fixture
def racine(tmp_path, monkeypatch):
    """Arborescence projet minimale avec des scripts de lot factices."""
    (tmp_path / "02_TRAVAIL").mkdir()
    (tmp_path / "01_SOURCES_BRUTES" / "Charges").mkdir(parents=True)
    monkeypatch.setattr(cfg, "PROJECT_ROOT", tmp_path)
    # Interpréteur = celui des tests (présent et fonctionnel, sans dépendre de pandas).
    monkeypatch.setattr(cfg, "LOT4A_ENGINE_PYTHON", Path(sys.executable))
    return tmp_path


def _script(racine: Path, nom: str, corps: str):
    (racine / "02_TRAVAIL" / nom).write_text(corps, encoding="utf-8")


def _enregistrer_lot(monkeypatch, lot: ex.Lot):
    tous = dict(ex.TOUS_LES_LOTS)
    tous[lot.nom] = lot
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)


# ── Interpréteur ─────────────────────────────────────────────────────────────

def test_verifier_interpreteur_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "LOT4A_ENGINE_PYTHON", tmp_path / "inexistant.exe")
    r = ex.verifier_interpreteur()
    assert r["ok"] is False and "introuvable" in r["message"]


def test_verifier_interpreteur_signale_pandas_absent(racine):
    """L'interpréteur des tests (miniconda) n'a pas pandas : le vérificateur doit le DIRE,
    pas l'ignorer — c'est précisément la cause de la limite mal documentée des tours précédents."""
    r = ex.verifier_interpreteur()
    assert r["chemin"] == sys.executable
    assert r["ok"] is r["pandas"]
    if not r["pandas"]:
        assert "pandas absent" in r["message"]


def test_interpreteur_des_lots_vient_de_la_configuration(racine):
    assert ex.interpreteur_lots() == Path(sys.executable)


# ── Prérequis ────────────────────────────────────────────────────────────────

def test_prerequis_script_manquant(racine):
    r = ex.verifier_prerequis(["lot9"], racine=racine)
    assert r["ok"] is False
    assert any("lot9" in s for s in r["scripts_manquants"])


def test_prerequis_lot_inconnu_signale(racine):
    r = ex.verifier_prerequis(["lot_inexistant"], racine=racine)
    assert any("inconnu" in s for s in r["scripts_manquants"])


def test_prerequis_dependance_absente_signalee(racine, monkeypatch):
    _script(racine, "lot10_calculer_resultats.py", "print('ok')")
    r = ex.verifier_prerequis(["lot10"], racine=racine)
    # lot10 dépend de lot9 : ni demandé, ni sortie présente -> signalé (sans bloquer par lui-même).
    assert any("lot10 dépend de lot9" in d for d in r["dependances_manquantes"])


def test_prerequis_ok_quand_tout_est_la(racine, monkeypatch):
    _script(racine, "faux.py", "print('ok')")
    _enregistrer_lot(monkeypatch, ex.Lot("faux", "faux.py", requiert_pandas=False))
    r = ex.verifier_prerequis(["faux"], racine=racine)
    assert r["scripts_manquants"] == []


# ── Exécution ────────────────────────────────────────────────────────────────

def test_lot_succes_avec_sortie_produite(racine, monkeypatch):
    _script(racine, "produit.py",
            "from pathlib import Path\n"
            "p = Path('sortie/fichier.txt'); p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text('ok')\nprint('produit ok')\n")
    _enregistrer_lot(monkeypatch, ex.Lot("produit", "produit.py", requiert_pandas=False,
                                         sorties=("sortie/fichier.txt",)))
    res = ex.executer_lot("produit", racine=racine)
    assert res.statut == ex.ST_SUCCES
    assert res.code_retour == 0
    assert "produit ok" in res.stdout
    assert res.sorties == {"sortie/fichier.txt": True}
    assert res.duree_s >= 0


def test_code_retour_zero_mais_sortie_absente_est_un_echec(racine, monkeypatch):
    """Le piège classique : le script « réussit » sans rien produire. Jamais un faux succès."""
    _script(racine, "vide.py", "print('je ne produis rien')\n")
    _enregistrer_lot(monkeypatch, ex.Lot("vide", "vide.py", requiert_pandas=False, sorties=("attendu/absent.xlsx",)))
    res = ex.executer_lot("vide", racine=racine)
    assert res.statut == ex.ST_ECHEC
    assert res.code_retour == 0
    assert "sortie(s) absente(s)" in res.message


def test_code_retour_non_nul_est_un_echec(racine, monkeypatch):
    _script(racine, "casse.py", "import sys\nprint('avant')\nsys.exit(3)\n")
    _enregistrer_lot(monkeypatch, ex.Lot("casse", "casse.py", requiert_pandas=False))
    res = ex.executer_lot("casse", racine=racine)
    assert res.statut == ex.ST_ECHEC and res.code_retour == 3
    assert "Code retour 3" in res.message


def test_stderr_capture(racine, monkeypatch):
    _script(racine, "err.py", "import sys\nsys.stderr.write('erreur metier\\n')\n")
    _enregistrer_lot(monkeypatch, ex.Lot("err", "err.py", requiert_pandas=False))
    res = ex.executer_lot("err", racine=racine)
    assert "erreur metier" in res.stderr


def test_timeout_respecte(racine, monkeypatch):
    _script(racine, "lent.py", "import time\ntime.sleep(30)\n")
    _enregistrer_lot(monkeypatch, ex.Lot("lent", "lent.py", requiert_pandas=False))
    res = ex.executer_lot("lent", racine=racine, timeout_s=2)
    assert res.statut == ex.ST_TIMEOUT
    assert "Délai dépassé" in res.message


def test_project_root_transmis_au_lot(racine, monkeypatch):
    _script(racine, "root.py",
            "import os\nfrom pathlib import Path\n"
            "p = Path('vu.txt'); p.write_text(os.environ.get('PROJECT_ROOT',''))\n")
    _enregistrer_lot(monkeypatch, ex.Lot("root", "root.py", requiert_pandas=False, sorties=("vu.txt",)))
    res = ex.executer_lot("root", racine=racine)
    assert res.statut == ex.ST_SUCCES
    assert (racine / "vu.txt").read_text().strip() == str(racine)


def test_environnement_global_non_modifie(racine, monkeypatch):
    import os
    avant = dict(os.environ)
    _script(racine, "env.py", "print('ok')")
    _enregistrer_lot(monkeypatch, ex.Lot("env", "env.py", requiert_pandas=False))
    ex.executer_lot("env", racine=racine)
    assert dict(os.environ) == avant


def test_script_absent_refuse_proprement(racine, monkeypatch):
    _enregistrer_lot(monkeypatch, ex.Lot("fantome", "fantome.py", requiert_pandas=False))
    res = ex.executer_lot("fantome", racine=racine)
    assert res.statut == ex.ST_ECHEC and "Script absent" in res.message


def test_lot_inconnu_refuse(racine):
    res = ex.executer_lot("nexistepas", racine=racine)
    assert res.statut == ex.ST_ECHEC and "inconnu" in res.message


# ── Nettoyage des chemins ────────────────────────────────────────────────────

def test_nettoyer_chemins_masque_racine_et_utilisateur(racine):
    texte = f"Erreur dans {racine}/02_TRAVAIL/x.py et {Path.home()}/secret"
    nettoye = ex.nettoyer_chemins(texte, racine)
    assert str(racine) not in nettoye
    assert "<projet>" in nettoye
    assert "<utilisateur>" in nettoye


# ── Ordre des lots repris des orchestrateurs existants ──────────────────────

def test_chaine_aval_respecte_l_ordre_du_runner_existant():
    """L'ordre doit être celui de 02_TRAVAIL/run_regression_pipeline.py (STEPS), pas un ordre
    réinventé."""
    noms = [l.nom for l in ex.CHAINE_AVAL]
    assert noms == ["lot4quater", "lot9", "lot10", "lot11", "lot12", "lot13"]


def test_chaine_menages_respecte_l_ordre_du_runner_existant():
    """L'ordre vient de `menages_chaine_service.STEPS_CHAINE`, seule cartographie auditée.

    Comparé à cette source plutôt qu'à une liste recopiée : une divergence entre le pilotage et
    l'orchestrateur existant doit casser ce test au lieu de passer inaperçue. `lot6c` manquait —
    c'est pourtant une étape du runner, et il produit les ménages externes que lot6d consomme.
    """
    from app.services import menages_chaine_service as chaine
    attendu = [e["name"].split("_")[0] for e in chaine.STEPS_CHAINE
               if e["name"].startswith("lot6")]
    assert [l.nom for l in ex.CHAINE_MENAGES] == attendu


def test_chaque_lot_menages_declare_ses_sorties():
    """Sans sortie déclarée, `sorties_ok` vaut True par construction : la garantie « jamais de faux
    succès » ne s'appliquait PAS à ces lots. Aucun n'en déclarait."""
    for lot in ex.CHAINE_MENAGES:
        assert lot.sorties, f"{lot.nom} ne déclare aucune sortie : faux succès possible."


def test_dependances_declarees_coherentes():
    for lot in ex.TOUS_LES_LOTS.values():
        for dep in lot.depend_de:
            assert dep in ex.TOUS_LES_LOTS, f"{lot.nom} dépend d'un lot inconnu : {dep}"

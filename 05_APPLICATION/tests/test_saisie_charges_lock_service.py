"""APP-3b / commit 4 — verrou exclusif interprocessus des écritures de charges.

Aucun fichier métier réel n'est touché, et aucun verrou n'est posé à l'emplacement de production :
chaque test injecte son propre `lock_path` sous tmp_path.
"""
from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import pytest

import app.config as cfg
from app.services import saisie_charges_lock_service as lk


@pytest.fixture
def lock_path(tmp_path: Path) -> Path:
    return tmp_path / lk.LOCK_NAME


# ── 1-2 : acquisition et contenu ─────────────────────────────────────────────

def test_1_acquisition_reussie_dun_verrou_absent(lock_path):
    assert not lock_path.exists()
    verrou = lk.acquerir_verrou(charge_id="CHG-2026-06-IC-BANQUE-001", lock_path=lock_path)
    try:
        assert lock_path.exists()
        assert verrou.est_detenu()
        assert verrou.token
    finally:
        lk.liberer_verrou(verrou)


def test_2_contenu_json_du_verrou(lock_path):
    verrou = lk.acquerir_verrou(operation="saisie_charge", charge_id="CHG-X", lock_path=lock_path)
    try:
        donnees = json.loads(lock_path.read_text(encoding="utf-8"))
        assert donnees["pid"] == os.getpid()
        assert donnees["hostname"] == socket.gethostname()
        assert donnees["operation"] == "saisie_charge"
        assert donnees["charge_id"] == "CHG-X"
        assert donnees["token"] == verrou.token
        assert donnees["created_at_utc"].startswith("20")
    finally:
        lk.liberer_verrou(verrou)


def test_2b_charge_id_nul_accepte(lock_path):
    """Le verrou est acquis AVANT la génération de l'identifiant — c'est le but même."""
    verrou = lk.acquerir_verrou(charge_id=None, lock_path=lock_path)
    try:
        assert json.loads(lock_path.read_text(encoding="utf-8"))["charge_id"] is None
    finally:
        lk.liberer_verrou(verrou)


# ── 3-4 : exclusion mutuelle ─────────────────────────────────────────────────

def test_3_refus_dun_deuxieme_detenteur(lock_path):
    premier = lk.acquerir_verrou(charge_id="CHG-1", lock_path=lock_path)
    try:
        with pytest.raises(lk.VerrouSaisieChargesDejaPrisError) as exc:
            lk.acquerir_verrou(charge_id="CHG-2", lock_path=lock_path)
        assert exc.value.metadonnees["token"] == premier.token
        assert str(os.getpid()) in str(exc.value)
    finally:
        lk.liberer_verrou(premier)


def test_4_le_premier_verrou_survit_a_lechec_du_second(lock_path):
    premier = lk.acquerir_verrou(lock_path=lock_path)
    try:
        with pytest.raises(lk.VerrouSaisieChargesDejaPrisError):
            lk.acquerir_verrou(lock_path=lock_path)
        # Le verrou du premier est intact : ni écrasé, ni supprimé.
        assert lock_path.exists()
        assert premier.est_detenu()
        assert json.loads(lock_path.read_text(encoding="utf-8"))["token"] == premier.token
    finally:
        lk.liberer_verrou(premier)


# ── 5-6 : libération et propriété ────────────────────────────────────────────

def test_5_liberation_reussie_par_le_detenteur(lock_path):
    verrou = lk.acquerir_verrou(lock_path=lock_path)
    lk.liberer_verrou(verrou)
    assert not lock_path.exists()
    assert not verrou.est_detenu()


def test_6_refus_de_liberation_avec_un_mauvais_token(lock_path):
    vrai = lk.acquerir_verrou(lock_path=lock_path)
    usurpateur = lk.VerrouSaisieCharges(lock_path=lock_path, token="mauvais-token", metadonnees={})
    try:
        with pytest.raises(lk.LiberationVerrouSaisieChargesError) as exc:
            lk.liberer_verrou(usurpateur)
        assert "autre processus" in str(exc.value)
        assert lock_path.exists()             # le verrou du vrai détenteur est intact
    finally:
        lk.liberer_verrou(vrai)


def test_6b_liberation_dun_verrou_disparu(lock_path):
    verrou = lk.acquerir_verrou(lock_path=lock_path)
    lock_path.unlink()                        # un tiers l'a supprimé
    with pytest.raises(lk.LiberationVerrouSaisieChargesError) as exc:
        lk.liberer_verrou(verrou)
    assert "déjà absent" in str(exc.value)


def test_6c_liberation_refusee_si_le_verrou_est_illisible(lock_path):
    """Propriété non vérifiable ⇒ on ne supprime pas. L'échec est explicite, jamais silencieux."""
    verrou = lk.acquerir_verrou(lock_path=lock_path)
    lock_path.write_text("{ ceci n'est pas du JSON", encoding="utf-8")
    try:
        with pytest.raises(lk.LiberationVerrouSaisieChargesError) as exc:
            lk.liberer_verrou(verrou)
        assert "illisible" in str(exc.value)
        assert lock_path.exists()
    finally:
        lock_path.unlink()


# ── 7 : le context manager libère toujours ───────────────────────────────────

def test_7_liberation_dans_un_finally_apres_erreur(lock_path):
    with pytest.raises(ValueError):
        with lk.verrou_saisie_charges(lock_path=lock_path):
            assert lock_path.exists()
            raise ValueError("échec métier simulé")
    assert not lock_path.exists()             # libéré malgré l'exception


def test_7b_context_manager_refuse_si_deja_pris(lock_path):
    premier = lk.acquerir_verrou(lock_path=lock_path)
    try:
        with pytest.raises(lk.VerrouSaisieChargesDejaPrisError):
            with lk.verrou_saisie_charges(lock_path=lock_path):
                pytest.fail("le corps ne doit jamais s'exécuter")
        assert premier.est_detenu()           # non libéré par l'échec du second
    finally:
        lk.liberer_verrou(premier)


# ── 11 : verrou corrompu ─────────────────────────────────────────────────────

def test_11_erreur_claire_si_le_verrou_est_illisible(lock_path):
    lock_path.write_text("pas du json", encoding="utf-8")
    with pytest.raises(lk.VerrouSaisieChargesInvalideError) as exc:
        lk.lire_metadonnees(lock_path)
    assert "JSON invalide" in str(exc.value)

    # Un verrou corrompu bloque quand même l'acquisition : on ne devine pas, on refuse.
    with pytest.raises(lk.VerrouSaisieChargesDejaPrisError) as exc2:
        lk.acquerir_verrou(lock_path=lock_path)
    assert "métadonnées illisibles" in str(exc2.value)


# ── 12-14 : diagnostic (ne supprime JAMAIS) ──────────────────────────────────

def test_12_diagnostic_dun_verrou_absent(lock_path):
    rapport = lk.inspecter_verrou(lock_path)
    assert rapport["etat"] == lk.ETAT_ABSENT
    assert rapport["metadonnees"] is None


def test_12b_diagnostic_dun_verrou_actif(lock_path):
    verrou = lk.acquerir_verrou(lock_path=lock_path)
    try:
        rapport = lk.inspecter_verrou(lock_path)
        assert rapport["etat"] == lk.ETAT_ACTIF_PROBABLE      # notre propre PID tourne
        assert rapport["metadonnees"]["pid"] == os.getpid()
    finally:
        lk.liberer_verrou(verrou)


def test_13_14_diagnostic_dun_verrou_potentiellement_perime_sans_suppression(lock_path):
    """PID mort ⇒ POTENTIELLEMENT_PERIME. Un PID est recyclable : aucune suppression automatique."""
    metadonnees = {
        "pid": 999_999_999, "hostname": socket.gethostname(),
        "created_at_utc": "2026-01-01T00:00:00+00:00",
        "operation": "saisie_charge", "charge_id": None, "token": "abc",
    }
    lock_path.write_text(json.dumps(metadonnees), encoding="utf-8")

    rapport = lk.inspecter_verrou(lock_path)
    assert rapport["etat"] == lk.ETAT_POTENTIELLEMENT_PERIME
    assert "recyclable" in rapport["raison"]

    # test 14 : le diagnostic n'a RIEN supprimé, et l'acquisition reste refusée.
    assert lock_path.exists()
    with pytest.raises(lk.VerrouSaisieChargesDejaPrisError):
        lk.acquerir_verrou(lock_path=lock_path)
    assert lock_path.exists()
    lock_path.unlink()


def test_13b_diagnostic_indeterminable_sur_une_autre_machine(lock_path):
    metadonnees = {
        "pid": 4242, "hostname": "UNE-AUTRE-MACHINE",
        "created_at_utc": "2026-01-01T00:00:00+00:00",
        "operation": "saisie_charge", "charge_id": None, "token": "abc",
    }
    lock_path.write_text(json.dumps(metadonnees), encoding="utf-8")
    rapport = lk.inspecter_verrou(lock_path)
    assert rapport["etat"] == lk.ETAT_INDETERMINABLE
    assert "autre machine" in rapport["raison"]
    lock_path.unlink()


def test_13c_diagnostic_indeterminable_si_le_verrou_est_corrompu(lock_path):
    lock_path.write_text("{{{", encoding="utf-8")
    rapport = lk.inspecter_verrou(lock_path)
    assert rapport["etat"] == lk.ETAT_INDETERMINABLE
    assert rapport["metadonnees"] is None
    assert lock_path.exists()                 # non supprimé
    lock_path.unlink()


def test_14b_aucune_suppression_forcee_nest_branchee(lock_path):
    """Aucune API de suppression forcée n'existe dans ce commit : la reprise reste humaine."""
    fonctions = dir(lk)
    assert not [f for f in fonctions if "forcer" in f.lower() or "purger" in f.lower()]
    assert not [f for f in fonctions if "supprimer" in f.lower()]


# ── Emplacement de production ────────────────────────────────────────────────

def test_emplacement_par_defaut_est_stable_et_hors_git():
    p = lk.chemin_verrou_par_defaut()
    assert p.name == ".saisie_charges_write.lock"
    assert p.parent == Path(cfg.DATA_DIR)     # emplacement applicatif stable, jamais aléatoire

    ignore = (Path(cfg.PROJECT_ROOT) / ".gitignore").read_text(encoding="utf-8")
    assert "05_APPLICATION/data/.saisie_charges_write.lock" in ignore


def test_acquisition_atomique_sans_verification_prealable():
    """La création doit être atomique (O_CREAT|O_EXCL), jamais un `if not exists` (course)."""
    source = Path(lk.__file__).read_text(encoding="utf-8")
    assert "os.O_CREAT | os.O_EXCL | os.O_WRONLY" in source

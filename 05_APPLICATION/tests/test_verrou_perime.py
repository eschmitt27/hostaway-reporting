"""Récupération d'un verrou périmé — les huit cas.

Un run interrompu (Ctrl-C, kill, coupure) laissait son verrou derrière lui et bloquait
définitivement les exécutions suivantes. Le symptôme observé était très éloigné de la cause :
`test_chaine_e2e_reelle_sur_copies` échouait sur `KeyError: 'reel_intact'`, parce que
`executer_chaine` sortait avant terme sans jamais atteindre ce champ.

La suppression était volontairement laissée à l'humain, au motif qu'un PID est recyclable.
L'argument tient mais porte sur le sens inverse : un PID recyclé rend le verrou **actif**, donc
protégé. Le vrai risque d'une reprise automatique est la **course entre deux repreneurs** — écarté
ici par un renommage atomique : le premier qui renomme gagne.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from app.services import saisie_charges_lock_service as verrou_lib


@pytest.fixture
def lock(tmp_path) -> Path:
    return tmp_path / ".test_chaine.lock"


def _ecrire(lock: Path, *, pid: int, hostname: str | None = None, horodatage: str | None = None):
    import socket
    from datetime import datetime, timezone
    lock.write_text(json.dumps({
        "pid": pid,
        "hostname": hostname if hostname is not None else socket.gethostname(),
        "created_at_utc": horodatage or datetime.now(timezone.utc).isoformat(),
        "operation": "test_chaine",
        "token": "t",
    }), encoding="utf-8")


def _pid_mort() -> int:
    """PID très improbable d'être vivant. Vérifié : s'il l'est, le test s'ignore plutôt que mentir."""
    candidat = 999_999
    if verrou_lib._pid_actif(candidat) is True:      # pragma: no cover - quasi impossible
        pytest.skip("Le PID témoin est occupé sur cette machine.")
    return candidat


# ── 1. Verrou absent ─────────────────────────────────────────────────────────

def test_verrou_absent(lock):
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is False
    assert res["code"] == verrou_lib.RECUP_ABSENT


# ── 2. Verrou actif ──────────────────────────────────────────────────────────

def test_verrou_actif_jamais_supprime(lock):
    """Le verrou d'un processus vivant ne doit JAMAIS être écarté."""
    _ecrire(lock, pid=os.getpid())
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is False
    assert res["code"] == verrou_lib.RECUP_ACTIF
    assert lock.exists(), "Le verrou d'un processus vivant a été supprimé."


# ── 3. Verrou dont le PID est mort ───────────────────────────────────────────

def test_verrou_pid_mort_recupere(lock):
    _ecrire(lock, pid=_pid_mort())
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is True
    assert res["code"] == verrou_lib.RECUP_EFFECTUEE
    assert not lock.exists()


def test_le_verrou_ecarte_est_conserve_pour_inspection(lock):
    """Jamais supprimé : mis de côté, donc inspectable après coup."""
    _ecrire(lock, pid=_pid_mort())
    res = verrou_lib.recuperer_verrou_perime(lock)
    ecarte = Path(res["ecarte_vers"])
    assert ecarte.exists()
    assert json.loads(ecarte.read_text(encoding="utf-8"))["operation"] == "test_chaine"


# ── 4. Verrou malformé ───────────────────────────────────────────────────────

def test_verrou_malforme_non_recupere(lock):
    """Un verrou illisible n'autorise aucune décision automatique."""
    lock.write_text("{ ceci n'est pas du json", encoding="utf-8")
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is False
    assert res["code"] == verrou_lib.RECUP_INDETERMINABLE
    assert lock.exists()


def test_verrou_d_une_autre_machine_non_recupere(lock):
    """PID non sondable depuis ici : on ne touche pas."""
    _ecrire(lock, pid=_pid_mort(), hostname="UNE-AUTRE-MACHINE")
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is False
    assert res["code"] == verrou_lib.RECUP_INDETERMINABLE
    assert lock.exists()


# ── 5. Âge du verrou ─────────────────────────────────────────────────────────

def test_verrou_ancien_recupere_et_age_calcule(lock):
    _ecrire(lock, pid=_pid_mort(), horodatage="2020-01-01T00:00:00+00:00")
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is True
    assert res["age_s"] > 60
    assert res["age_suspect"] is False


def test_verrou_tout_juste_pose_mais_pid_mort_est_signale(lock):
    """Récupérable, mais l'anomalie est remarquée : un verrou neuf dont le PID est déjà mort."""
    _ecrire(lock, pid=_pid_mort())
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is True
    assert res["age_suspect"] is True


def test_horodatage_illisible_ne_bloque_pas_la_reprise(lock):
    _ecrire(lock, pid=_pid_mort(), horodatage="pas-une-date")
    res = verrou_lib.recuperer_verrou_perime(lock)
    assert res["recupere"] is True
    assert res["age_s"] is None


# ── 6. Deux lancements concurrents ───────────────────────────────────────────

def test_un_seul_repreneur_gagne(lock):
    """Renommage atomique : un seul repreneur réussit, l'autre ne relance rien."""
    _ecrire(lock, pid=_pid_mort())
    premier = verrou_lib.recuperer_verrou_perime(lock)
    second = verrou_lib.recuperer_verrou_perime(lock)
    assert premier["recupere"] is True
    assert second["recupere"] is False
    assert second["code"] == verrou_lib.RECUP_ABSENT


def test_deux_acquisitions_simultanees_restent_exclusives(lock):
    """La reprise n'affaiblit pas l'exclusion : un verrou pris reste pris."""
    v = verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    try:
        with pytest.raises(verrou_lib.VerrouSaisieChargesDejaPrisError):
            verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    finally:
        verrou_lib.liberer_verrou(v)


# ── 7. Libération normale ────────────────────────────────────────────────────

def test_liberation_normale_puis_reacquisition(lock):
    v = verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    verrou_lib.liberer_verrou(v)
    assert not lock.exists()
    v2 = verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    verrou_lib.liberer_verrou(v2)


# ── 8. Processus interrompu → la chaîne repart ───────────────────────────────

def test_apres_interruption_l_acquisition_redevient_possible(lock):
    """Le scénario réel : un run tué laisse son verrou ; le suivant doit pouvoir démarrer."""
    _ecrire(lock, pid=_pid_mort())
    with pytest.raises(verrou_lib.VerrouSaisieChargesDejaPrisError):
        verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)

    verrou_lib.recuperer_verrou_perime(lock, operation="test_chaine")
    v = verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    verrou_lib.liberer_verrou(v)


# ── Confidentialité ──────────────────────────────────────────────────────────

def test_le_nom_de_machine_n_apparait_pas_dans_le_message(lock):
    """Ce message remonte à l'interface et aux logs partageables."""
    _ecrire(lock, pid=os.getpid(), hostname="POSTE-SECRET-042")
    try:
        verrou_lib.acquerir_verrou(operation="test_chaine", lock_path=lock)
    except verrou_lib.VerrouSaisieChargesDejaPrisError as exc:
        assert "POSTE-SECRET-042" not in str(exc)
        assert exc.metadonnees["hostname"] == "POSTE-SECRET-042"   # dispo pour le diagnostic local
    else:                                                          # pragma: no cover
        pytest.fail("Le verrou aurait dû être refusé.")

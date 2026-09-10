"""Inventaire des verrous d'écriture — invariants de sûreté, toutes catégories confondues.

Deux catégories coexistent, et c'est **délibéré** :

1. **Double verrou** — `_verrou_ecriture("…")`, soit
   `(RECETTE_MODE or MODE_REEL_ECRITURES) and _env_flag("…")`. Deux leviers SIMULTANÉS, dans l'un
   de deux contextes : recette isolée (`RECETTE_MODE`) ou production réelle
   (`MODE_REEL_ECRITURES`, mission activation recette 2026-09-10). Concernent les modules dont
   l'écriture a été exercée : Charges, Banque, Factures, Calculs, Ménages, Comptabilité.
   Avant cette mission le contexte était `RECETTE_MODE` SEUL : une instance de production ne
   pouvait jamais écrire, même variable posée — le seul chemin d'activation réelle était d'éditer
   `config.py`. Le second contexte lève cette impasse **sans** toucher au défaut : sans variable
   d'environnement, les deux leviers restent faux.
2. **Gelés** — littéralement `False`, sans aucun chemin d'activation par variable d'environnement.
   « Ne jamais activer implicitement ni par défaut. » Concernent HH, REF_Assoc_Mode et Contrôles.

Un rapport précédent affirmait que `MENAGES_REAL_RECALC_ENABLED` était « la dernière garde codée en
dur ». **C'était faux** : cinq gardes gelées subsistent. Elles ne sont pas une dérive — les geler
est plus sûr que de leur ouvrir un chemin d'activation. Ce test rend la frontière explicite pour
qu'aucune des deux catégories ne dérive en silence.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import app.config as cfg

CONFIG = Path(cfg.__file__)

# Flags à double verrou : activables en recette, jamais seuls.
DOUBLE_VERROU = {
    "CHARGES_REAL_WRITE_ENABLED",
    "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED",
    "BANQUE_REAL_WRITE_ENABLED",
    "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED",
    "FACTURES_REAL_WRITE_ENABLED",
    "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
    "CALCULS_REAL_RUN_ENABLED",
    "CALCULS_REAL_RUN_CONFIRMATION_ENABLED",
    "MENAGES_REAL_RECALC_ENABLED",
    "MENAGES_CYCLE_REAL_WRITE_ENABLED",
    "MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED",
    "COMPTABILITE_REAL_WRITE_ENABLED",
    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
}

# Flags gelés : aucun chemin d'activation. Les ouvrir serait un ÉLARGISSEMENT de surface,
# à décider explicitement — pas un alignement cosmétique.
GELES = {
    "HH_REAL_WRITE_ENABLED",
    "HH_REAL_WRITE_CONFIRMATION_ENABLED",
    "REF_ASSOC_MODE_REAL_WRITE_ENABLED",
    "CONTROLES_REAL_WRITE_ENABLED",
    "CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED",
}


def _source() -> str:
    return CONFIG.read_text(encoding="utf-8")


def _flags_declares(src: str) -> set[str]:
    """Tout identifiant de niveau module qui ressemble à un verrou d'écriture réelle."""
    return {
        m.group(1) for m in re.finditer(
            r"^(\w*(?:REAL_WRITE|REAL_RUN|REAL_RECALC)\w*)\s*=", src, re.M)
    }


# ── Complétude de l'inventaire ───────────────────────────────────────────────

def test_l_inventaire_couvre_tous_les_flags():
    """Un nouveau verrou ajouté sans être classé casse ce test — c'est le but."""
    declares = _flags_declares(_source())
    connus = DOUBLE_VERROU | GELES
    inconnus = declares - connus
    assert not inconnus, (
        f"Verrous non classés : {sorted(inconnus)}. Les ranger en DOUBLE_VERROU ou en GELES.")
    disparus = connus - declares
    assert not disparus, f"Verrous disparus de config.py : {sorted(disparus)}"


# ── Invariant 1 : tout est faux par défaut ───────────────────────────────────

def test_tous_les_flags_sont_faux_par_defaut():
    """Sans RECETTE_MODE ni variable dédiée, aucune écriture réelle n'est possible."""
    actifs = [n for n in (DOUBLE_VERROU | GELES) if getattr(cfg, n)]
    assert not actifs, f"Verrous actifs par défaut : {actifs}"


def test_recette_mode_est_faux_par_defaut():
    assert cfg.RECETTE_MODE is False


def test_mode_reel_ecritures_est_faux_par_defaut():
    """Le second contexte d'activation ne s'allume jamais tout seul."""
    assert cfg.MODE_REEL_ECRITURES is False


# ── Invariant 2 : aucun flag activable seul ──────────────────────────────────

@pytest.mark.parametrize("flag", sorted(DOUBLE_VERROU))
def test_la_variable_seule_ne_suffit_pas(flag, monkeypatch):
    """Poser la variable d'environnement sans AUCUN contexte d'écriture ne doit rien activer."""
    monkeypatch.setenv(flag, "1")
    monkeypatch.delenv("RECETTE_MODE", raising=False)
    monkeypatch.delenv("MODE_REEL_ECRITURES", raising=False)
    import importlib
    recharge = importlib.reload(cfg)
    try:
        assert getattr(recharge, flag) is False
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


@pytest.mark.parametrize("flag", sorted(DOUBLE_VERROU))
def test_le_flag_est_bien_a_double_verrou_dans_le_source(flag):
    src = _source()
    m = re.search(rf"^{flag}\s*=\s*(.+)$", src, re.M)
    assert m, f"{flag} introuvable."
    expression = m.group(1)
    assert expression.strip() == f'_verrou_ecriture("{flag}")', (
        f"{flag} n'est pas à double verrou : {expression}")


def test_le_verrou_exige_deux_leviers_simultanes():
    """La définition même du verrou : un contexte ET la variable dédiée, jamais l'un des deux."""
    src = _source()
    m = re.search(r"def _verrou_ecriture\(nom: str\) -> bool:.*?\n    return ([^\n]+)\n",
                  src, re.S)
    assert m, "_verrou_ecriture introuvable dans config.py."
    assert m.group(1).strip() == "(RECETTE_MODE or MODE_REEL_ECRITURES) and _env_flag(nom)"


@pytest.mark.parametrize("flag", sorted(DOUBLE_VERROU))
def test_mode_reel_ecritures_seul_ne_suffit_pas(flag, monkeypatch):
    """Le contexte production seul n'active rien : la variable dédiée reste obligatoire."""
    monkeypatch.setenv("MODE_REEL_ECRITURES", "1")
    monkeypatch.delenv("RECETTE_MODE", raising=False)
    monkeypatch.delenv(flag, raising=False)
    import importlib
    recharge = importlib.reload(cfg)
    try:
        assert getattr(recharge, flag) is False
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


@pytest.mark.parametrize("flag", sorted(DOUBLE_VERROU))
def test_les_deux_contextes_ouvrent_le_meme_verrou(flag, monkeypatch):
    """Recette isolée ET production réelle activent le writer — c'est le but du second contexte.

    Sans cela, une instance de production ne pouvait écrire qu'en se déguisant en recette.
    """
    import importlib
    for contexte in ("RECETTE_MODE", "MODE_REEL_ECRITURES"):
        monkeypatch.setenv(contexte, "1")
        monkeypatch.setenv(flag, "1")
        autre = "MODE_REEL_ECRITURES" if contexte == "RECETTE_MODE" else "RECETTE_MODE"
        monkeypatch.delenv(autre, raising=False)
        recharge = importlib.reload(cfg)
        try:
            assert getattr(recharge, flag) is True, f"{flag} inactif avec {contexte}=1"
        finally:
            monkeypatch.undo()
    importlib.reload(cfg)


@pytest.mark.parametrize("flag", sorted(GELES))
def test_le_flag_gele_n_a_aucun_chemin_d_activation(flag, monkeypatch):
    """Poser la variable ET RECETTE_MODE ne doit toujours rien activer."""
    src = _source()
    m = re.search(rf"^{flag}\s*=\s*(.+)$", src, re.M)
    assert m and m.group(1).strip() == "False", (
        f"{flag} devait rester littéralement False, trouvé : {m.group(1) if m else '?'}")

    monkeypatch.setenv(flag, "1")
    monkeypatch.setenv("RECETTE_MODE", "1")
    import importlib
    recharge = importlib.reload(cfg)
    try:
        assert getattr(recharge, flag) is False
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


# ── Invariant 3 : RECETTE_MODE seul n'écrit rien non plus ────────────────────

def test_recette_mode_seul_n_active_aucune_ecriture(monkeypatch):
    monkeypatch.setenv("RECETTE_MODE", "1")
    for flag in DOUBLE_VERROU:
        monkeypatch.delenv(flag, raising=False)
    import importlib
    recharge = importlib.reload(cfg)
    try:
        actifs = [n for n in DOUBLE_VERROU if getattr(recharge, n)]
        assert not actifs, f"RECETTE_MODE seul a activé : {actifs}"
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


def test_mode_reel_ecritures_seul_n_active_aucune_ecriture(monkeypatch):
    """Symétrique du précédent pour le contexte production."""
    monkeypatch.setenv("MODE_REEL_ECRITURES", "1")
    for flag in DOUBLE_VERROU | GELES:
        monkeypatch.delenv(flag, raising=False)
    import importlib
    recharge = importlib.reload(cfg)
    try:
        actifs = [n for n in (DOUBLE_VERROU | GELES) if getattr(recharge, n)]
        assert not actifs, f"MODE_REEL_ECRITURES seul a activé : {actifs}"
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


@pytest.mark.parametrize("flag", sorted(GELES))
def test_le_flag_gele_reste_faux_meme_en_mode_reel(flag, monkeypatch):
    """Le second contexte n'ouvre AUCUNE des gardes gelées — il ne les concerne pas."""
    monkeypatch.setenv("MODE_REEL_ECRITURES", "1")
    monkeypatch.setenv(flag, "1")
    import importlib
    recharge = importlib.reload(cfg)
    try:
        assert getattr(recharge, flag) is False
    finally:
        monkeypatch.undo()
        importlib.reload(cfg)


# ── Invariant 4 : le write-guard borne la racine ─────────────────────────────

def test_le_write_guard_refuse_hors_racine_autorisee(tmp_path, monkeypatch):
    """Même flags actifs, une écriture hors RECETTE_ROOT doit être refusée."""
    from app import recette_guard
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path / "recette")
    (tmp_path / "recette").mkdir()

    recette_guard.assert_ecriture_autorisee(tmp_path / "recette" / "ok.xlsx")   # sous la racine
    with pytest.raises(Exception):
        recette_guard.assert_ecriture_autorisee(tmp_path / "ailleurs" / "ko.xlsx")

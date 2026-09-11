"""§16-19 — TYPE_FLUX_005 (REMBOURSEMENT_ASSOCIE) : `code_impact` n'est PAS applicable, et c'est
délibéré.

L'AUDIT (exhaustif, sur le code ET sur la donnée)
  · 0 charge réelle, 0 ligne `flux_unifies`, 0 `ref_charges_recurrentes` n'utilisent ce type —
    il est vivant côté `lot7` (avantages associé), pas côté saisie de charge standard.
  · `code_impact_defaut = NULL` depuis la migration 0079 (suppression de HR).
  · L'UNIQUE consommateur runtime de `code_impact_defaut` est la règle V19 de
    `charges_preview_service` : « commentaire de justification obligatoire si le `code_impact`
    choisi diffère du défaut du type ». Elle lit `_type_flux_impact_defaut()`, qui rend `''` — une
    chaîne vide, pas la chaîne littérale "None" — pour un défaut NULL. Le garde `if impact_defaut
    and ...` traite alors ce type comme « aucun défaut à comparer » : V19 ne se déclenche JAMAIS
    pour TYPE_FLUX_005, quel que soit le `code_impact` choisi.

LA QUESTION POSÉE PAR LA MISSION
« A-t-on besoin d'un code_impact pour TYPE_FLUX_005, ou son sens_flux suffit-il déjà à définir
correctement son effet ? »

LA RÉPONSE, VÉRIFIÉE ICI
`sens_flux = REMBOURSEMENT` détermine déjà l'effet — `lot3_generateur_charges.SENS_BY_SENS_FLUX`
le traduit en `sens = NEUTRALISATION`, AVANT toute lecture de `code_impact`. `code_impact` ne
détermine RIEN pour ce type : `code_impact_defaut = NULL` n'est donc pas une case restée vide,
c'est le contrat correct. Lui attribuer `IC` ou `HC` par défaut inventerait un impact qu'aucune
donnée réelle ne réclame.

CE QUE CE FICHIER VERROUILLE
Si un jour une vraie charge TYPE_FLUX_005 apparaît et qu'on est tenté de lui donner un défaut IC/HC
« pour combler le vide », ces tests échoueront et pointeront vers cette explication — au lieu de
laisser quelqu'un `deviner` un impact.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg

TID = "TYPE_FLUX_005"

_TRAVAIL = str(Path(cfg.APP_ROOT).parent / "02_TRAVAIL")
if _TRAVAIL not in sys.path:
    sys.path.insert(0, _TRAVAIL)


@pytest.fixture()
def base(tmp_path):
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    return db


def _inserer_type_flux_005(db) -> None:
    """Sur base vierge, `ref_types_flux` est vide : on y pose le type tel que le référentiel réel
    le porte (mêmes valeurs que l'import réel), pour tester le contrat en isolation."""
    import sqlite3

    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO ref_types_flux (type_flux_id, type_flux, description, "
            "code_impact_defaut, avantage_brut_defaut, deduit_avantage_defaut, "
            "comptabilisable_defaut, actif, import_id) VALUES (?,?,?,?,?,?,?,?,?)",
            (TID, "REMBOURSEMENT_ASSOCIE",
             "Remboursement d'une dépense personnelle payée par le compte pro",
             None, "NON", "NON", "NON", "OUI", "TEST"))
        conn.commit()
    finally:
        conn.close()


# ── Le contrat en base ────────────────────────────────────────────────────────

def test_code_impact_defaut_est_null_pas_une_chaine_vide(base):
    """Distinction qui compte : NULL (non applicable) vs '' (applicable, vide par erreur).

    Une valeur NULL en base porte l'intention explicite ; une chaîne vide serait ambiguë avec un
    oubli de saisie référentielle."""
    import sqlite3

    _inserer_type_flux_005(base)
    conn = sqlite3.connect(str(base))
    try:
        valeur = conn.execute(
            "SELECT code_impact_defaut FROM ref_types_flux WHERE type_flux_id=?", (TID,)
        ).fetchone()[0]
    finally:
        conn.close()
    assert valeur is None


def test_reel_confirme_zero_charge_sur_ce_type():
    """L'audit qui a motivé la décision reste vrai : rien ne consomme économiquement ce type."""
    import sqlite3

    c = sqlite3.connect(f"file:{cfg.DB_PATH}?mode=ro", uri=True)
    try:
        for table in ("charges", "flux_unifies", "ref_charges_recurrentes"):
            n = c.execute(f"SELECT COUNT(*) FROM {table} WHERE type_flux_id=?", (TID,)).fetchone()[0]
            assert n == 0, f"{table} porte désormais une ligne TYPE_FLUX_005 — ré-auditer avant de conclure"
    finally:
        c.close()


# ── Le seul consommateur runtime : V19 ───────────────────────────────────────

def test_le_lookup_rend_une_chaine_vide_jamais_le_mot_none():
    """`str(None)` vaut `"None"` en Python — un piège classique. Le lookup doit l'éviter :
    une valeur NULL doit rendre `''` (faux), jamais la chaîne "NONE" (vrai)."""
    from app.services.charges_preview_service import _type_flux_impact_defaut

    refs = {"types_flux": [{"type_flux_id": TID, "code_impact_defaut": None}]}
    resultat = _type_flux_impact_defaut(TID, refs)
    assert resultat == "", f"attendu '', obtenu {resultat!r} (bug NULL -> 'NONE' ?)"
    assert not resultat, "la valeur doit être FAUSSE pour que le garde V19 la traite comme absente"


@pytest.mark.parametrize("code_impact_choisi", ["IC", "HC"])
def test_v19_ne_se_declenche_jamais_pour_ce_type(code_impact_choisi):
    """Le scénario représentatif demandé par la mission : quel que soit le code choisi, aucune
    justification n'est exigée — parce qu'il n'y a pas de défaut à contredire."""
    from app.services.charges_preview_service import _type_flux_impact_defaut

    refs = {"types_flux": [{"type_flux_id": TID, "code_impact_defaut": None}]}
    impact_defaut = _type_flux_impact_defaut(TID, refs)

    # Reproduction exacte du garde V19 (charges_preview_service.previsualiser, bloc V19).
    declenche = bool(impact_defaut) and code_impact_choisi != impact_defaut
    assert declenche is False, (
        f"V19 s'est déclenché pour {code_impact_choisi} sur TYPE_FLUX_005 : "
        "un défaut est en train d'être appliqué à un type qui ne doit pas en avoir.")


def test_reel_le_meme_lookup_sur_le_referentiel_reel():
    """Preuve sur la vraie donnée, pas seulement sur un fixture construit à la main."""
    from app.services import referentiel_admin_service as ref_admin
    from app.services.charges_preview_service import _type_flux_impact_defaut

    refs = {"types_flux": ref_admin.lignes("ref_types_flux", db_path=cfg.DB_PATH)}
    resultat = _type_flux_impact_defaut(TID, refs)
    assert resultat == ""


# ── L'effet réel vient de sens_flux, pas de code_impact ──────────────────────

def test_sens_flux_remboursement_neutralise_independamment_du_code_impact():
    """C'est la réponse à la question posée par la mission : le sens suffit déjà.

    `lot3_generateur_charges.SENS_BY_SENS_FLUX["REMBOURSEMENT"] = "NEUTRALISATION"` — cette
    traduction ne lit `code_impact` nulle part. Un TYPE_FLUX_005 est neutralisé par construction,
    quel que soit le code d'impact qu'on lui donnerait."""
    import lot3_generateur_charges as lot3

    assert lot3.SENS_BY_SENS_FLUX["REMBOURSEMENT"] == "NEUTRALISATION"
    # Aucune branche de dérivation du sens ne consulte code_impact : la neutralisation ne peut
    # donc pas dépendre d'un défaut qui n'existe pas.
    source = Path(lot3.__file__).read_text(encoding="utf-8", errors="replace")
    bloc_sens = source.split("SENS_BY_SENS_FLUX: dict[str, str] = {")[0][-200:] + \
        source.split("SENS_BY_SENS_FLUX: dict[str, str] = {")[1].split("}")[0]
    assert "code_impact" not in bloc_sens


def test_aucun_code_impact_ic_hc_n_est_invente_par_defaut():
    """STRUCTUREL — aucun code du dépôt ne doit attribuer IC/HC à TYPE_FLUX_005 « pour combler
    le vide ». Si ce test échoue, quelqu'un a réintroduit exactement le risque que ce fichier
    documente."""
    import re

    cible = Path(cfg.APP_ROOT) / "app" / "services" / "charges_preview_service.py"
    source = cible.read_text(encoding="utf-8", errors="replace")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert not re.search(r'TYPE_FLUX_005["\']\s*:\s*["\'](IC|HC)["\']', code)

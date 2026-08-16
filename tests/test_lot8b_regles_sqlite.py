"""Lot8b — les règles de classification viennent de SQLite, jamais d'une écriture dans REF_Setup.

Le lot re-semait l'onglet `REF_Banque_Regles` dans `REF_Setup.xlsm` à chaque exécution
(suppression puis recréation) et y ajoutait `TYPE_FLUX_016`. Ce semis est historique : il a déjà
été fait. Le refaire rendait le lot **inexécutable** dès lors que le référentiel réel ne doit pas
être modifié — et bloquait de fait toute la chaîne Banque.

Ces tests verrouillent le contrat : lecture seule sur Excel, source canonique SQLite, ordre des
règles préservé. Aucune donnée bancaire ni référentielle réelle : fixtures fictives en tmp_path.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

LOT8B = Path(__file__).resolve().parent.parent / "02_TRAVAIL" / "lot8b_banque_regles.py"

pytestmark = pytest.mark.skipif(not LOT8B.exists(), reason="lot8b absent de cet environnement")


def _charger_module(monkeypatch, argv):
    """Importe le lot comme un module, avec un argv maîtrisé (il parse au chargement)."""
    monkeypatch.setattr(sys, "argv", ["lot8b_banque_regles.py", *argv])
    for nom in list(sys.modules):
        if nom == "lot8b_banque_regles":
            del sys.modules[nom]
    monkeypatch.syspath_prepend(str(LOT8B.parent))
    import lot8b_banque_regles as mod
    return mod


def _db_avec_regles(chemin: Path, regles: list[tuple], colonnes: list[str]) -> Path:
    conn = sqlite3.connect(chemin)
    try:
        cols = ", ".join(f"{c} TEXT" for c in colonnes)
        conn.execute(f"CREATE TABLE ref_banque_regles ({cols}, import_id TEXT)")
        trous = ", ".join("?" * len(colonnes))
        conn.executemany(
            f"INSERT INTO ref_banque_regles ({', '.join(colonnes)}) VALUES ({trous})", regles)
        conn.commit()
    finally:
        conn.close()
    return chemin


def _regle(mod, regle_id, priorite, motif="MOTIF"):
    """Une règle complète, positionnelle, alignée sur REGLES_HDR."""
    valeurs = {
        "regle_id": regle_id, "priorite": priorite, "actif": "OUI", "compte_id": "*",
        "type_match": "COMMENCE_PAR", "champ_cible": "libelle", "motif": motif,
        "tiers_detecte": "TIERS", "categorie": "CAT", "type_flux_id": "TYPE_FLUX_001",
        "code_impact": "IC", "source_economique": "SRC", "rapprochement_requis": "NON",
        "validation_automatique": "OUI", "niveau_risque": "FAIBLE",
        "statut_controle_defaut": "VALIDE", "statut_classification_defaut": "CLASSE",
        "date_debut_validite": "", "date_fin_validite": "", "commentaire": "fixture",
    }
    return tuple(str(valeurs[c]) for c in mod.REGLES_HDR)


# ── Le contrat : aucune écriture dans le référentiel ────────────────────────────────────────────

def test_le_lot_ne_contient_plus_aucune_ecriture_du_referentiel():
    """Garde-fou textuel : `wb.save(REF_PATH)` ne doit jamais réapparaître."""
    source = LOT8B.read_text(encoding="utf-8")
    assert "wb.save(REF_PATH)" not in source
    assert "def update_ref_setup" not in source


def test_le_referentiel_excel_n_est_ouvert_qu_en_lecture_seule():
    source = LOT8B.read_text(encoding="utf-8")
    for ligne in source.splitlines():
        if "load_workbook(REF_PATH" in ligne:
            assert "read_only=True" in ligne, ligne


# ── Source SQLite ───────────────────────────────────────────────────────────────────────────────

def test_regles_chargees_depuis_sqlite(monkeypatch, tmp_path):
    mod = _charger_module(monkeypatch, [])
    db = _db_avec_regles(tmp_path / "app.db",
                         [_regle(mod, "R_002", "20"), _regle(mod, "R_001", "10")],
                         list(mod.REGLES_HDR))
    mod = _charger_module(monkeypatch, ["--source-regles", "SQLITE", "--db", str(db)])

    regles, source = mod.charger_regles()
    assert source == "SQLITE"
    assert [r["regle_id"] for r in regles] == ["R_001", "R_002"]


def test_priorite_triee_en_entier_pas_en_texte(monkeypatch, tmp_path):
    """SQLite rend du TEXTE : trier « 10 » et « 9 » comme des chaînes inverserait leur ordre,
    donc changerait la règle appliquée à un mouvement."""
    mod = _charger_module(monkeypatch, [])
    db = _db_avec_regles(tmp_path / "app.db",
                         [_regle(mod, "R_DIX", "10"), _regle(mod, "R_NEUF", "9")],
                         list(mod.REGLES_HDR))
    mod = _charger_module(monkeypatch, ["--source-regles", "SQLITE", "--db", str(db)])

    regles, _ = mod.charger_regles()
    assert [r["regle_id"] for r in regles] == ["R_NEUF", "R_DIX"]
    assert [r["priorite"] for r in regles] == [9, 10]


def test_priorite_illisible_passe_en_dernier(monkeypatch, tmp_path):
    mod = _charger_module(monkeypatch, [])
    db = _db_avec_regles(tmp_path / "app.db",
                         [_regle(mod, "R_KO", "sans_priorite"), _regle(mod, "R_OK", "50")],
                         list(mod.REGLES_HDR))
    mod = _charger_module(monkeypatch, ["--source-regles", "SQLITE", "--db", str(db)])

    regles, _ = mod.charger_regles()
    assert [r["regle_id"] for r in regles] == ["R_OK", "R_KO"]


def test_valeurs_vides_ramenees_a_none(monkeypatch, tmp_path):
    """Même contrat de sortie que l'ancien `rules_as_dicts` : une cellule vide vaut None."""
    mod = _charger_module(monkeypatch, [])
    db = _db_avec_regles(tmp_path / "app.db", [_regle(mod, "R_001", "10")],
                         list(mod.REGLES_HDR))
    mod = _charger_module(monkeypatch, ["--source-regles", "SQLITE", "--db", str(db)])

    regles, _ = mod.charger_regles()
    assert regles[0]["date_debut_validite"] is None
    assert regles[0]["motif"] == "MOTIF"


# ── Refus explicites ────────────────────────────────────────────────────────────────────────────

def test_sqlite_exige_mais_base_absente_arrete_le_lot(monkeypatch, tmp_path):
    mod = _charger_module(
        monkeypatch, ["--source-regles", "SQLITE", "--db", str(tmp_path / "inexistante.db")])
    with pytest.raises(SystemExit) as exc:
        mod.charger_regles()
    assert "SQLITE" in str(exc.value)


def test_sqlite_exige_mais_table_vide_arrete_le_lot(monkeypatch, tmp_path):
    mod = _charger_module(monkeypatch, [])
    db = _db_avec_regles(tmp_path / "vide.db", [], list(mod.REGLES_HDR))
    mod = _charger_module(monkeypatch, ["--source-regles", "SQLITE", "--db", str(db)])
    with pytest.raises(SystemExit) as exc:
        mod.charger_regles()
    assert "vide" in str(exc.value)


def test_source_inconnue_refusee(monkeypatch):
    mod = _charger_module(monkeypatch, ["--source-regles", "MAGIQUE"])
    with pytest.raises(SystemExit):
        mod.charger_regles()


# ── Secours ─────────────────────────────────────────────────────────────────────────────────────

def test_seed_reste_accessible_explicitement(monkeypatch):
    """SEED permet de reconstruire un référentiel vierge — jamais choisi par défaut."""
    mod = _charger_module(monkeypatch, ["--source-regles", "SEED"])
    regles, source = mod.charger_regles()
    assert source == "SEED"
    assert len(regles) == len(mod.SEED_RULES)


def test_aucune_base_designee_ne_tombe_pas_sur_la_base_reelle(monkeypatch):
    """`_chemin_db()` ne doit JAMAIS deviner un chemin de production."""
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    mod = _charger_module(monkeypatch, [])
    assert mod._chemin_db() is None

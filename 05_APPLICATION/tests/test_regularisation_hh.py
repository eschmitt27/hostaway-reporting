"""Mission 18b — régularisation DIRECT_SANS_SAISIE_HH / VRBO_MONTANT_NON_RENSEIGNE depuis l'app.

Utilise un CLONE de l'app.db réelle (via `sqlite3.Connection.backup()`, jamais une copie de fichier
brute, jamais le fichier réel lui-même) : l'état réel post-Mission-17/18 contient déjà les 27
anomalies réelles avec leurs contrôles Lot11 authentiques — un fixture synthétique redéfinirait
artificiellement ce que le moteur produit réellement. Le clone est jetable, jamais réutilisé comme
vérité, et la base réelle n'est jamais ouverte en écriture par ce fichier.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.services import controles_actionnable_service as act
from app.services import regularisation_hh_service as regul


REAL_DB = Path(cfg.DB_PATH)


def _cloner(tmp_path: Path) -> Path:
    dest = tmp_path / "clone_regularisation.db"
    src_conn = sqlite3.connect(str(REAL_DB))
    dst_conn = sqlite3.connect(str(dest))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()
    return dest


@pytest.fixture(scope="module")
def clone(tmp_path_factory) -> Path:
    if not REAL_DB.exists():
        pytest.skip("app.db réelle absente de cet environnement.")
    tmp_path = tmp_path_factory.mktemp("regularisation")
    return _cloner(tmp_path)


def _forcer_db_active(monkeypatch, db_path: Path) -> None:
    """`controles_cloture_reader.controles()` lit `cfg.DB_PATH` À CHAUD (get_db(None)) sans jamais
    recevoir le `db_path=` explicite passé à `load_dashboard()` — un `db_path=` seul ne suffit donc
    pas à isoler la liste des contrôles agrégés. Convention déjà établie par `conftest.py` (fixture
    `tmp_db`) : monkeypatcher `cfg.DB_PATH` (function-scoped, reverti automatiquement)."""
    monkeypatch.setattr(cfg, "DB_PATH", Path(db_path))


def _ctrl_opaque_par_classification(classification: str, db_path: Path, monkeypatch) -> str:
    _forcer_db_active(monkeypatch, db_path)
    data = act.load_dashboard(vue="tous", classification=classification, page=1, db_path=str(db_path))
    assert data["rows"], f"Aucun élément réel de classification {classification} trouvé dans le clone."
    return data["rows"][0]["ctrl_opaque"]


# ── Prérequis : les deux classifications existent réellement dans le clone ──────────────────────

def test_direct_hors_hostaway_et_vrbo_sans_montant_presents(clone, monkeypatch):
    direct = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", clone, monkeypatch)
    vrbo = _ctrl_opaque_par_classification("VRBO_SANS_MONTANT", clone, monkeypatch)
    assert direct and vrbo


# ── Préremplissage structurel — jamais de montant ────────────────────────────────────────────────

def test_preremplissage_direct_ne_contient_jamais_de_montant(clone, monkeypatch):
    ctrl = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", clone, monkeypatch)
    prep = regul.preparer_formulaire(ctrl, db_path=str(clone))
    assert prep is not None
    assert prep["reservation_id_hostaway"]
    assert prep["logement_id"]
    assert prep["proprietaire_id"]
    assert prep["date_arrivee"] and prep["date_depart"]
    assert prep["canal_label"] == "DIRECT"
    assert prep["prix_affiche_hostaway_non_autoritaire"] is None
    assert "montant_percu" not in prep
    assert "total_price" not in prep


def test_preremplissage_vrbo(clone, monkeypatch):
    ctrl = _ctrl_opaque_par_classification("VRBO_SANS_MONTANT", clone, monkeypatch)
    prep = regul.preparer_formulaire(ctrl, db_path=str(clone))
    assert prep is not None
    assert prep["canal_label"] == "VRBO"
    assert prep["prix_affiche_hostaway_non_autoritaire"] is None


def test_element_non_regularisable_hors_perimetre(clone, monkeypatch):
    # Une classification hors périmètre (ex. inconnue) ne doit jamais être régularisable ici.
    _forcer_db_active(monkeypatch, clone)
    assert regul.element_regularisable("CTRL-INEXISTANT", db_path=str(clone)) is None


# ── Refus : montant vide, jamais déduit ──────────────────────────────────────────────────────────

def test_montant_vide_refuse(clone, monkeypatch):
    ctrl = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", clone, monkeypatch)
    resultat = regul.regulariser(ctrl, montant_percu="", code_impact="IMP_001", db_path=str(clone))
    assert resultat["ok"] is False
    assert resultat["code"] == "MONTANT_PERCU_OBLIGATOIRE"


def test_code_impact_vide_refuse(clone, monkeypatch):
    ctrl = _ctrl_opaque_par_classification("VRBO_SANS_MONTANT", clone, monkeypatch)
    resultat = regul.regulariser(ctrl, montant_percu="123.45", code_impact="", db_path=str(clone))
    assert resultat["ok"] is False
    assert resultat["code"] == "CODE_IMPACT_OBLIGATOIRE"


# ── Montant explicitement 0, distinct d'une absence de saisie ───────────────────────────────────

def test_montant_zero_explicite_distinct_de_absence(tmp_path, monkeypatch):
    dest = _cloner(tmp_path)
    ctrl = _ctrl_opaque_par_classification("VRBO_SANS_MONTANT", dest, monkeypatch)
    prep_avant = regul.preparer_formulaire(ctrl, db_path=str(dest))
    resultat = regul.regulariser(
        ctrl, montant_percu="0", code_impact=_premier_code_impact(dest),
        commentaire="Montant nul confirmé", db_path=str(dest))
    assert resultat["ok"] is True
    conn = sqlite3.connect(str(dest))
    try:
        row = conn.execute(
            "SELECT montant_percu FROM reservations_hors_hostaway WHERE reservation_id_hostaway=?",
            (prep_avant["reservation_id_hostaway"],)).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == 0.0


def _premier_code_impact(db_path: Path) -> str:
    from app.services import saisie_hh_service as saisie_refs
    _, rows = saisie_refs.get_codes_impact(db_path=str(db_path))
    for r in rows:
        code = str(r.get("code_impact") or "").strip()
        if code:
            return code
    pytest.skip("Aucun code impact disponible dans le référentiel du clone.")


# ── Régularisation réelle DIRECT : jamais total_price copié ──────────────────────────────────────

def test_regulariser_direct_cree_saisie_hh_montant_jamais_egal_total_price(tmp_path, monkeypatch):
    dest = _cloner(tmp_path)
    ctrl = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", dest, monkeypatch)
    prep = regul.preparer_formulaire(ctrl, db_path=str(dest))
    rid = prep["reservation_id_hostaway"]

    conn = sqlite3.connect(str(dest))
    try:
        total_price_row = conn.execute(
            "SELECT total_price FROM hostaway_reservations WHERE reservation_id=? LIMIT 1", (rid,)
        ).fetchone()
    finally:
        conn.close()
    total_price = total_price_row[0] if total_price_row else None

    montant_saisi = "1.23"  # volontairement distinct de total_price pour prouver l'absence de copie
    resultat = regul.regulariser(
        ctrl, montant_percu=montant_saisi, code_impact=_premier_code_impact(dest),
        acteur="test", db_path=str(dest))
    assert resultat["ok"] is True
    assert resultat.get("reservation_hh_id")

    conn = sqlite3.connect(str(dest))
    try:
        row = conn.execute(
            "SELECT montant_percu, reservation_id_hostaway, logement_id, proprietaire_id, "
            "date_arrivee, date_depart, statut_controle FROM reservations_hors_hostaway "
            "WHERE reservation_id_hostaway=?", (rid,)).fetchone()
    finally:
        conn.close()
    assert row is not None
    montant_percu, rid_stocke, logement_id, proprietaire_id, arrivee, depart, statut = row
    assert montant_percu == 1.23
    if total_price is not None:
        assert montant_percu != total_price
    assert rid_stocke == rid
    assert logement_id == prep["logement_id"]
    assert proprietaire_id == prep["proprietaire_id"]
    assert arrivee == prep["date_arrivee"]
    assert depart == prep["date_depart"]
    assert statut == "VALIDE"


# ── Dédoublonnage : deuxième clic retrouve la saisie existante ──────────────────────────────────

def test_deuxieme_clic_ne_duplique_pas(tmp_path, monkeypatch):
    dest = _cloner(tmp_path)
    ctrl = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", dest, monkeypatch)
    prep = regul.preparer_formulaire(ctrl, db_path=str(dest))
    rid = prep["reservation_id_hostaway"]
    code_impact = _premier_code_impact(dest)

    r1 = regul.regulariser(ctrl, montant_percu="200.00", code_impact=code_impact, db_path=str(dest))
    assert r1["ok"] is True
    r2 = regul.regulariser(ctrl, montant_percu="250.00", code_impact=code_impact, db_path=str(dest))
    assert r2["ok"] is True
    assert r2["reservation_hh_id"] == r1["reservation_hh_id"]

    conn = sqlite3.connect(str(dest))
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM reservations_hors_hostaway WHERE reservation_id_hostaway=?",
            (rid,)).fetchone()[0]
        montant = conn.execute(
            "SELECT montant_percu FROM reservations_hors_hostaway WHERE reservation_id_hostaway=?",
            (rid,)).fetchone()[0]
    finally:
        conn.close()
    assert n == 1
    assert montant == 250.0  # la modification a bien mis à jour, pas créé une seconde ligne


# ── Aucune mutation Hostaway distante ─────────────────────────────────────────────────────────────

def test_regularisation_ne_touche_pas_hostaway(tmp_path, monkeypatch):
    dest = _cloner(tmp_path)
    ctrl = _ctrl_opaque_par_classification("VRBO_SANS_MONTANT", dest, monkeypatch)

    conn = sqlite3.connect(str(dest))
    try:
        n_res_avant = conn.execute("SELECT COUNT(*) FROM hostaway_reservations").fetchone()[0]
        n_ext_avant = conn.execute("SELECT COUNT(*) FROM hostaway_extractions").fetchone()[0]
    finally:
        conn.close()

    regul.regulariser(ctrl, montant_percu="50.00", code_impact=_premier_code_impact(dest), db_path=str(dest))

    conn = sqlite3.connect(str(dest))
    try:
        n_res_apres = conn.execute("SELECT COUNT(*) FROM hostaway_reservations").fetchone()[0]
        n_ext_apres = conn.execute("SELECT COUNT(*) FROM hostaway_extractions").fetchone()[0]
    finally:
        conn.close()
    assert n_res_avant == n_res_apres
    assert n_ext_avant == n_ext_apres


# ── E2E réel : saisie → RESERVATIONS → Lot9 → Lot10 → Lot11 → Lot12 ─────────────────────────────
# Marqué "slow" implicitement (enchaîne le DAG réel sur une copie) — pas de mock du moteur.

def test_regularisation_puis_recalcul_resout_reellement_lanomalie(tmp_path, monkeypatch):
    dest = _cloner(tmp_path)
    _forcer_db_active(monkeypatch, dest)
    target_rid = "54480315"  # Mission 18, groupe A — DIRECT_SANS_SAISIE_HH, non legacy
    ctrl = None
    data = act.load_dashboard(vue="tous", classification="DIRECT_HORS_HOSTAWAY", page=1, db_path=str(dest))
    for r in data["rows"]:
        if r["donnees"].get("reservation_id") == target_rid:
            ctrl = r["ctrl_opaque"]
            break
    assert ctrl, f"Réservation {target_rid} introuvable dans le clone (référence Mission 18 périmée ?)."

    resultat = regul.regulariser(ctrl, montant_percu="79.00", code_impact="IC", db_path=str(dest))
    assert resultat["ok"] is True

    recalc = regul.recalculer(db_path=str(dest))
    assert recalc["ok"] is True

    conn = sqlite3.connect(str(dest))
    conn.row_factory = sqlite3.Row
    try:
        dataset_id = conn.execute(
            "SELECT dataset_id FROM reservations_datasets WHERE etape='RESOLUES' AND actif=1"
        ).fetchone()[0]
        row = conn.execute(
            "SELECT source, montant_retenu, statut_controle, code_anomalie FROM reservations_resolues "
            "WHERE dataset_id=? AND reservation_id_hostaway=?", (dataset_id, target_rid)).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["source"] == "HOSTAWAY_DIRECT_HH"
    assert row["montant_retenu"] == 79.0
    assert row["statut_controle"] == "VALIDE"
    assert not row["code_anomalie"]


# ── La vraie app.db n'est jamais ouverte en écriture ──────────────────────────────────────────────

def test_vraie_app_db_jamais_modifiee(clone, monkeypatch):
    import hashlib
    h_avant = hashlib.sha256(REAL_DB.read_bytes()).hexdigest()
    ctrl = _ctrl_opaque_par_classification("DIRECT_HORS_HOSTAWAY", clone, monkeypatch)
    regul.preparer_formulaire(ctrl, db_path=str(clone))
    h_apres = hashlib.sha256(REAL_DB.read_bytes()).hexdigest()
    assert h_avant == h_apres

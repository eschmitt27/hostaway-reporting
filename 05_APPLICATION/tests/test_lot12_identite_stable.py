"""Lot12 — l'identifiant de préfacture dérive du GRAIN MÉTIER, jamais d'une position.

Le legacy suffixait `facture_id` d'un compteur positionnel par mois : l'identifiant désignait une
position de parcours, pas une préfacture. Deux moteurs lisant les mêmes données dans un ordre
différent produisaient des identifiants différents (102/285 lors de la parité réelle Lot12), et
l'ajout d'une préfacture renumérotait toutes les suivantes du mois.

Ces tests verrouillent la propriété inverse : mêmes données ⇒ mêmes identifiants, quel que soit
l'ordre ; un ajout ne renomme rien ; un montant qui change ne change pas l'identité.
"""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import lot12_prefactures_service as svc

MOIS = "2026-06"
LOGEMENTS = [("LOG_B", "PROP_B", 200.0), ("LOG_A", "PROP_A", 100.0), ("LOG_C", "PROP_C", 300.0)]


def _seed(db, lignes, *, montants=None):
    """Sème un run Lot10 actif + le référentiel minimal, dans l'ordre d'insertion fourni."""
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('L10-T','SUCCES',1)")
        for log_id, prop_id, montant in lignes:
            m = (montants or {}).get(log_id, montant)
            conn.execute(
                "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
                "montant_du_conciergerie, reste_a_payer_conciergerie, nb_reservations) "
                "VALUES ('L10-T', ?, ?, ?, ?, ?, 2)", (MOIS, log_id, prop_id, m, m))
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES ('IMP','2026-01-01T00:00:00Z','t','t','IMPORTE')")
        for _, prop_id, _ in lignes:
            conn.execute(
                "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, mode_facturation, "
                "actif, import_id) VALUES (?, 'MENSUEL', 'OUI', 'IMP')", (prop_id,))
        for log_id, _, _ in lignes:
            conn.execute(
                "INSERT OR IGNORE INTO ref_logements (logement_id, actif, statut_parc, import_id) "
                "VALUES (?, 'OUI', 'GERE', 'IMP')", (log_id,))
        conn.commit()
    finally:
        conn.close()


def _ids(db) -> list[str]:
    conn = get_db(db)
    try:
        return sorted(r[0] for r in conn.execute("SELECT facture_id FROM lot12_prefactures_entete"))
    finally:
        conn.close()


@pytest.fixture
def base(tmp_path):
    def _fabriquer(nom, lignes, montants=None):
        db = tmp_path / f"{nom}.db"
        apply_migrations(db)
        _seed(db, lignes, montants=montants)
        assert svc.construire(db_path=db)["ok"]
        return db
    return _fabriquer


def test_meme_donnees_ordre_different_memes_ids(base):
    """§15 — l'ordre d'insertion ne doit avoir aucun effet sur l'identité."""
    deux = LOGEMENTS[:2]
    db1 = base("ordre1", deux)
    db2 = base("ordre2", list(reversed(deux)))
    assert _ids(db1) == _ids(db2)


def test_ajout_prefacture_ne_renomme_pas_les_autres(base):
    """§15 — ajouter une préfacture au même mois laisse les identifiants existants intacts."""
    db1 = base("avant", LOGEMENTS[:2])
    db2 = base("apres", LOGEMENTS)
    avant, apres = set(_ids(db1)), set(_ids(db2))
    assert avant.issubset(apres)
    assert len(apres) == len(avant) + 1


def test_montant_modifie_identite_inchangee(base):
    """§15 — un montant qui change ne change pas l'identité logique de la préfacture."""
    db1 = base("montant1", LOGEMENTS[:2])
    db2 = base("montant2", LOGEMENTS[:2], montants={"LOG_A": 999.0, "LOG_B": 1.0})
    assert _ids(db1) == _ids(db2)


def test_identifiant_ne_contient_aucun_suffixe_positionnel(base):
    """L'identifiant est exactement `PREF-{mois}-{proprietaire}-{logement}` : pas de rang."""
    db = base("forme", LOGEMENTS)
    for fid in _ids(db):
        assert fid.startswith(f"PREF-{MOIS}-")
        # Un suffixe positionnel produirait un dernier segment purement numérique.
        assert not fid.rsplit("-", 1)[-1].isdigit(), fid


def test_mapping_legacy_disponible_et_complet(base):
    """§14 — l'ancien identifiant positionnel reste retrouvable (traçabilité), sans porter de calcul."""
    db = base("mapping", LOGEMENTS)
    conn = get_db(db)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT facture_id, facture_id_legacy FROM lot12_prefactures_id_legacy")]
    finally:
        conn.close()
    assert len(rows) == len(_ids(db))
    for r in rows:
        assert r["facture_id_legacy"].startswith(r["facture_id"] + "-")
        assert r["facture_id_legacy"].rsplit("-", 1)[-1].isdigit()


def test_lignes_rattachees_aux_identifiants_stables(base):
    """Les lignes de préfacture pointent vers l'identifiant stable, jamais vers le legacy."""
    db = base("lignes", LOGEMENTS)
    conn = get_db(db)
    try:
        ids_lignes = {r[0] for r in conn.execute(
            "SELECT DISTINCT facture_id FROM lot12_prefactures_lignes")}
    finally:
        conn.close()
    assert ids_lignes == set(_ids(db))

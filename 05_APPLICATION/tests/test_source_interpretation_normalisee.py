"""Toute facture issue d'un PDF dit explicitement d'où viennent ses lignes (migration 0094).

Les factures importées avant que la notion d'interprétation existe portaient `NULL` : l'écran
affichait « non posée » alors que leurs lignes venaient du parseur PDF. La normalisation pose
`PDF` pour celles-là — et pour elles seules : une facture saisie à la main n'a ni PDF ni MD, donc
aucune provenance de document à déclarer.

Ce qui est vérifié ici : le remplissage est IDEMPOTENT, il ne touche à rien d'autre (lignes,
montants, statuts, rapprochements), et il n'invente pas d'interprétation là où il n'y en a pas.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_interpretation_service as interpretation
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact

MIGRATION = "0094"


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


def _facture(db, *, ref, source="PDF_EXTRACTION", source_interpretation=None, montant=100.0):
    opaque = f"FAC-{ref}"
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "facture_ref_source, date_facture, montant_ttc, statut, source, justificatif, "
            "source_interpretation) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (opaque, "INT_0004", ref, ref, "2026-08-31", montant, fact.ST_A_CONTROLER, source,
             f"{ref}.pdf", source_interpretation))
        conn.execute("INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, "
                     "type_ligne, montant_ttc, description, source) VALUES (?,?,?,?,?,?)",
                     (f"FLM-{ref}", opaque, "MENAGE_EXTERNE", montant, "ligne", "PDF_EXTRACTION"))
        conn.commit()
    finally:
        conn.close()
    return opaque


def _sources(db) -> dict[str, str | None]:
    conn = get_db(db)
    try:
        return {r["facture_ref"]: r["source_interpretation"] for r in conn.execute(
            "SELECT facture_ref, source_interpretation FROM factures")}
    finally:
        conn.close()


def _rejouer_migration(db):
    """Rejoue le SQL de la migration comme si elle repassait sur une base déjà à jour."""
    sql = next(p for p in (cfg.APP_ROOT / "app" / "db" / "migrations").glob(f"{MIGRATION}_*.sql"))
    conn = get_db(db)
    try:
        conn.executescript(sql.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def base(tmp_path):
    """Trois factures : deux historiques sans source, une déjà en MD, plus une saisie manuelle."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _facture(db, ref="HIST-1")
    _facture(db, ref="HIST-2")
    _facture(db, ref="DEJA-MD", source_interpretation=interpretation.SOURCE_MD)
    _facture(db, ref="MANUELLE", source="SAISIE")
    return db


def test_les_factures_pdf_sans_source_sont_normalisees(base):
    # La migration a déjà tourné à la création de la base : les factures posées ensuite simulent
    # l'existant historique, on rejoue donc le SQL comme le ferait une base plus ancienne.
    _rejouer_migration(base)
    sources = _sources(base)
    assert sources["HIST-1"] == interpretation.SOURCE_PDF
    assert sources["HIST-2"] == interpretation.SOURCE_PDF
    assert None not in [sources["HIST-1"], sources["HIST-2"], sources["DEJA-MD"]]


def test_une_source_deja_posee_n_est_jamais_ecrasee(base):
    _rejouer_migration(base)
    assert _sources(base)["DEJA-MD"] == interpretation.SOURCE_MD, \
        "une facture lue depuis son MD ne redevient pas « PDF »"


def test_une_facture_saisie_a_la_main_n_est_pas_concernee(base):
    _rejouer_migration(base)
    assert _sources(base)["MANUELLE"] is None, \
        "sans PDF ni MD, il n'y a aucune provenance de document à déclarer"
    opaque = "FAC-MANUELLE"
    assert interpretation.etat(opaque, db_path=base) == {}, \
        "l'écran n'affiche pas « Extraction PDF » pour une saisie manuelle"


def test_la_normalisation_est_idempotente(base):
    _rejouer_migration(base)
    premier = _sources(base)
    _rejouer_migration(base)
    _rejouer_migration(base)
    assert _sources(base) == premier, "rejouer ne change plus rien"


def test_rien_d_autre_n_est_touche(base):
    conn = get_db(base)
    try:
        avant = [tuple(r) for r in conn.execute(
            "SELECT facture_id_opaque, montant_ttc, statut, facture_ref, date_facture FROM factures "
            "ORDER BY facture_id_opaque")]
        lignes_avant = [tuple(r) for r in conn.execute(
            "SELECT ligne_id_opaque, montant_ttc, statut_ligne, source FROM facture_lignes_menage "
            "ORDER BY ligne_id_opaque")]
    finally:
        conn.close()

    _rejouer_migration(base)

    conn = get_db(base)
    try:
        apres = [tuple(r) for r in conn.execute(
            "SELECT facture_id_opaque, montant_ttc, statut, facture_ref, date_facture FROM factures "
            "ORDER BY facture_id_opaque")]
        lignes_apres = [tuple(r) for r in conn.execute(
            "SELECT ligne_id_opaque, montant_ttc, statut_ligne, source FROM facture_lignes_menage "
            "ORDER BY ligne_id_opaque")]
        versions = conn.execute("SELECT COUNT(*) FROM facture_interpretations").fetchone()[0]
    finally:
        conn.close()
    assert apres == avant, "montants, statuts et références inchangés"
    assert lignes_apres == lignes_avant, "aucune ligne touchée"
    assert versions == 0, "normaliser un champ n'est pas une nouvelle interprétation"
    assert flm.somme_lignes_effectives("FAC-HIST-1", db_path=base) == pytest.approx(100.0)

"""APP-3D — Contrats des 4 lecteurs (Acomptes, AirCover, Imputations, Ajustements).

AirCover / Imputations Airbnb / Ajustements post-clôture sont lus en base depuis la migration 0054
(app/readers/proprietaires_extras_reader.py) — les sources réelles étaient de toute façon VIDES
(fichiers présents, zéro ligne de donnée) avant la bascule. Les tests ci-dessous valident le CONTRAT
(colonnes attendues, comportement sur table absente/vide) via des lignes synthétiques insérées
directement en base, jamais de données personnelles/financières réelles.
"""
from app.db.connection import get_db
from app.readers import proprietaires_extras_reader as extras

COLONNES_AIRCOVER = ["aircover_id", "date", "montant", "beneficiaire_reel", "proprietaire_id",
                     "logement_id", "reservation_id", "mois", "justificatif", "traitement",
                     "statut_controle", "commentaire"]
COLONNES_IMPUTATIONS = ["imputation_airbnb_id", "transaction_banque_id", "reference_airbnb",
                        "proprietaire_id", "logement_id", "mois", "document_id", "montant_impute",
                        "date_imputation", "justificatif", "statut", "commentaire"]
COLONNES_AJUSTEMENTS = ["ajustement_id", "mois_origine", "mois_effet", "source_module", "source_pk",
                        "logement_id", "proprietaire_id", "type_ajustement", "montant", "sens",
                        "impact_reel", "impact_comptable", "motif", "justificatif", "auteur",
                        "date_saisie", "statut_validation"]

COLONNES_OBLIGATOIRES = {
    "aircover": ["proprietaire_id", "mois", "montant"],
    "imputations": ["proprietaire_id", "mois", "montant_impute"],
    "ajustements": ["proprietaire_id", "mois_effet", "montant", "motif"],
}


def _insere_aircover(db_path, aircover_id, proprietaire_id, mois, montant=100,
                     logement_id="LOG1", commentaire=""):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO aircover (aircover_id, date_aircover, montant, beneficiaire_reel, "
            "proprietaire_id, logement_id, reservation_id, mois, justificatif, traitement, "
            "statut_controle, commentaire) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (aircover_id, "2026-01-01", montant, "prop", proprietaire_id, logement_id, "R1", mois,
             "j", "t", "OK", commentaire),
        )
        conn.commit()
    finally:
        conn.close()


# ── 1. Chemins Excel legacy conservés seulement pour l'inventaire/test zéro-Excel ────────────

def test_01_chemins_legacy_toujours_reference_pour_inventaire():
    import app.config as cfg
    for attr in ("SAISIE_ACOMPTES_PROPRIETAIRES", "SAISIE_AIRCOVER",
                "SAISIE_IMPUTATIONS_AIRBNB", "SAISIE_AJUSTEMENTS_POST_CLOTURE"):
        p = getattr(cfg, attr)
        assert str(p).endswith(".xlsx")


# ── 2. Contrat de colonnes obligatoires vs facultatives ───────────────────────

def test_02_colonnes_obligatoires_documentees():
    for cle, colonnes in (("aircover", COLONNES_AIRCOVER), ("imputations", COLONNES_IMPUTATIONS),
                          ("ajustements", COLONNES_AJUSTEMENTS)):
        for oblig in COLONNES_OBLIGATOIRES[cle]:
            assert oblig in colonnes, f"colonne obligatoire {oblig} absente du contrat {cle}"


# ── 3-4. Types et dates ────────────────────────────────────────────────────────

def test_03_to_nombre_gere_virgule_et_espace():
    assert extras.to_nombre("1 234,56") == 1234.56
    assert extras.to_nombre(None) is None
    assert extras.to_nombre("") is None


def test_04_to_date_et_to_mois_formats():
    import datetime as dt
    assert extras.to_mois(dt.date(2026, 3, 15)) == "2026-03"
    assert extras.to_date(dt.date(2026, 3, 15)) == "2026-03-15"


# ── 5. Doublons (aucune déduplication automatique — signalé, jamais masqué) ───

def test_05_doublons_non_masques(tmp_db):
    _insere_aircover(tmp_db, "AC1", "PROP_X", "2026-01")
    _insere_aircover(tmp_db, "AC2", "PROP_X", "2026-01")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert len(rows) == 2   # pas de deduplication silencieuse — visible pour controle humain


# ── 6. Table vide (migration jouée, aucune ligne saisie) ─────────────────────

def test_06_table_vide_etat_clair(tmp_db):
    s = extras.aircover(tmp_db)
    assert s.etat.etat == extras.ETAT_VIDE
    assert s.lignes == []   # jamais un faux zero silencieux : etat explicite VIDE, pas juste []


# ── 7. Table absente (base non migrée) ────────────────────────────────────────

def test_07_table_absente_etat_non_initialise(tmp_path):
    from app.db.connection import get_db
    db_path = tmp_path / "vide.db"
    conn = get_db(db_path)  # cree le fichier sans jouer les migrations
    conn.close()
    s = extras.aircover(db_path)
    assert s.etat.etat == extras.ETAT_NON_INITIALISE
    assert s.lignes == []


# ── 8. Colonne facultative absente (NULL en base) — pas de crash ─────────────

def test_08_colonne_facultative_null_pas_de_crash(tmp_db):
    _insere_aircover(tmp_db, "AC1", None, "2026-01")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert rows == []   # to_texte(None) -> "" -> jamais == "PROP_X", pas de crash


# ── 9. Montant invalide (texte non numérique stocké malgré tout) ─────────────

def test_09_montant_invalide_devient_none_pas_exception(tmp_db):
    _insere_aircover(tmp_db, "AC1", "PROP_X", "2026-01", montant="N/A")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert extras.to_nombre(rows[0]["montant"]) is None


# ── 10. Mois invalide ──────────────────────────────────────────────────────────

def test_10_mois_invalide_ne_correspond_a_rien(tmp_db):
    _insere_aircover(tmp_db, "AC1", "PROP_X", "PAS-UN-MOIS")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert rows == []


# ── 11. Espaces superflus dans les identifiants ───────────────────────────────

def test_11_espaces_normalises_dans_identifiants(tmp_db):
    _insere_aircover(tmp_db, "AC1", "  PROP_X  ", "2026-01")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert len(rows) == 1


# ── 12. Valeur vide dans une cellule optionnelle ─────────────────────────────

def test_12_valeur_vide_geree(tmp_db):
    _insere_aircover(tmp_db, "AC1", "PROP_X", "2026-01", commentaire="")
    rows = extras.aircover_prop_mois("PROP_X", "2026-01", tmp_db)
    assert len(rows) == 1 and extras.to_texte(rows[0]["commentaire"]) == ""


# ── 13. Imputations Airbnb et Ajustements suivent le même contrat de lecture ──

def test_13_imputations_lecture_sqlite(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO imputations_airbnb (imputation_airbnb_id, proprietaire_id, mois, "
            "montant_impute) VALUES (?,?,?,?)", ("IMP1", "PROP_X", "2026-01", 50),
        )
        conn.commit()
    finally:
        conn.close()
    rows = extras.imputations_prop_mois("PROP_X", "2026-01", tmp_db)
    assert len(rows) == 1 and rows[0]["montant_impute"] == 50


def test_14_ajustements_filtre_sur_mois_effet(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ajustements_post_cloture (ajustement_id, proprietaire_id, mois_origine, "
            "mois_effet, montant, motif) VALUES (?,?,?,?,?,?)",
            ("AJ1", "PROP_X", "2025-12", "2026-01", 10, "correction"),
        )
        conn.commit()
    finally:
        conn.close()
    assert len(extras.ajustements_prop_mois("PROP_X", "2026-01", tmp_db)) == 1
    assert len(extras.ajustements_prop_mois("PROP_X", "2025-12", tmp_db)) == 0

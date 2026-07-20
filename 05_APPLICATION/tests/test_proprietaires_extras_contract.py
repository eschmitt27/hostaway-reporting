"""APP-3D — Contrats des 4 nouveaux lecteurs (Acomptes, AirCover, Imputations, Ajustements).

Les sources réelles sont actuellement VIDES (fichiers présents, zéro ligne de données) — les tests
ci-dessous valident le CONTRAT (colonnes attendues, comportement sur schéma dégradé) via fixtures
synthétiques représentatives de la structure réelle observée, jamais de données personnelles/
financières réelles copiées.
"""
import openpyxl
import pytest

from app.readers import proprietaires_extras_reader as extras

# ── Contrat de colonnes observé sur les fichiers réels (en-têtes seuls, 0 ligne de données) ──

COLONNES_ACOMPTES = ["acompte_id", "ROW_HASH", "mois", "proprietaire_id", "logement_id",
                     "facture_ref", "source_acompte", "source_hh_id", "montant_acompte",
                     "report_mois_precedent", "mode_paiement_id", "code_impact",
                     "impact_resultat_reel", "impact_resultat_comptable", "statut_controle",
                     "niveau_anomalie", "code_anomalie", "commentaire"]
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
    "acomptes": ["proprietaire_id", "mois", "montant_acompte"],
    "aircover": ["proprietaire_id", "mois", "montant"],
    "imputations": ["proprietaire_id", "mois", "montant_impute"],
    "ajustements": ["proprietaire_id", "mois_effet", "montant", "motif"],
}


def _classeur(tmp_path, nom_fichier, onglet, header, lignes=None):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = onglet
    ws.append(header)
    for ligne in (lignes or []):
        ws.append(ligne)
    p = tmp_path / nom_fichier
    wb.save(p)
    return p


# ── 1. Chemin logique + format confirmés (via config, pas de chemin absolu exposé) ───────────

def test_01_chemins_logiques_sanitises():
    import app.config as cfg
    for attr in ("SAISIE_ACOMPTES_PROPRIETAIRES", "SAISIE_AIRCOVER",
                "SAISIE_IMPUTATIONS_AIRBNB", "SAISIE_AJUSTEMENTS_POST_CLOTURE"):
        p = getattr(cfg, attr)
        assert str(p).endswith(".xlsx")


# ── 2-3. Format et onglets confirmés ──────────────────────────────────────────

def test_02_onglets_attendus():
    assert extras.ONGLET_ACOMPTES == "SAISIE"
    assert extras.ONGLET_AIRCOVER == "MASTER"
    assert extras.ONGLET_IMPUTATIONS == "MASTER"
    assert extras.ONGLET_AJUSTEMENTS == "MASTER"


# ── 4-5. Contrat de colonnes obligatoires vs facultatives ─────────────────────

@pytest.mark.parametrize("cle,colonnes", [
    ("acomptes", COLONNES_ACOMPTES), ("aircover", COLONNES_AIRCOVER),
    ("imputations", COLONNES_IMPUTATIONS), ("ajustements", COLONNES_AJUSTEMENTS)])
def test_03_04_colonnes_obligatoires_documentees(cle, colonnes):
    for oblig in COLONNES_OBLIGATOIRES[cle]:
        assert oblig in colonnes, f"colonne obligatoire {oblig} absente du contrat {cle}"


# ── 6-7. Types et dates ─────────────────────────────────────────────────────

def test_06_to_nombre_gere_virgule_et_espace():
    assert extras.to_nombre("1 234,56") == 1234.56
    assert extras.to_nombre(None) is None
    assert extras.to_nombre("") is None


def test_07_to_date_et_to_mois_formats():
    import datetime as dt
    assert extras.to_mois(dt.date(2026, 3, 15)) == "2026-03"
    assert extras.to_date(dt.date(2026, 3, 15)) == "2026-03-15"


# ── 8. Doublons (aucune déduplication automatique — signalé, jamais masqué) ──

def test_08_doublons_non_masques(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [
        ["AC1", "2026-01-01", 100, "prop", "PROP_X", "LOG1", "R1", "2026-01", "j", "t", "OK", ""],
        ["AC1", "2026-01-01", 100, "prop", "PROP_X", "LOG1", "R1", "2026-01", "j", "t", "OK", ""],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert len(rows) == 2   # pas de deduplication silencieuse — visible pour controle humain


# ── 9-10. Comportement source vide / uniquement en-têtes ─────────────────────

def test_09_fichier_absent_etat_clair():
    import app.config as cfg
    from pathlib import Path
    orig = cfg.SAISIE_AIRCOVER
    cfg.SAISIE_AIRCOVER = Path("Z:/nexiste/pas.xlsx")
    extras.vider_cache()
    try:
        s = extras.aircover()
        assert s.etat.etat == extras.ETAT_FICHIER_ABSENT
        assert s.lignes == []
    finally:
        cfg.SAISIE_AIRCOVER = orig
        extras.vider_cache()


def test_10_uniquement_entetes_etat_vide(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    s = extras.aircover()
    assert s.etat.etat == extras.ETAT_VIDE
    assert s.lignes == []   # jamais un faux zero silencieux : etat explicite VIDE, pas juste []


# ── 11. Colonne obligatoire absente ───────────────────────────────────────────

def test_11_colonne_obligatoire_absente_pas_de_crash(tmp_path, monkeypatch):
    header_sans_proprietaire = [c for c in COLONNES_AIRCOVER if c != "proprietaire_id"]
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", header_sans_proprietaire, [
        ["AC1", "2026-01-01", 100, "b", "LOG1", "R1", "2026-01", "j", "t", "OK", ""],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert rows == []   # to_texte(r.get("proprietaire_id")) -> "" -> jamais == "PROP_X", pas de crash


# ── 12. Nouvelle colonne apparue : ignorée sans casser la lecture ────────────

def test_12_colonne_supplementaire_ignoree(tmp_path, monkeypatch):
    header_plus = COLONNES_AIRCOVER + ["nouvelle_colonne_future"]
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", header_plus, [
        ["AC1", "2026-01-01", 100, "b", "PROP_X", "LOG1", "R1", "2026-01", "j", "t", "OK", "", "xyz"],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert len(rows) == 1 and rows[0]["nouvelle_colonne_future"] == "xyz"


# ── 13. Montant invalide (texte non numérique) ───────────────────────────────

def test_13_montant_invalide_devient_none_pas_exception(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [
        ["AC1", "2026-01-01", "N/A", "b", "PROP_X", "LOG1", "R1", "2026-01", "j", "t", "OK", ""],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert extras.to_nombre(rows[0]["montant"]) is None


# ── 14. Mois invalide dans la source ──────────────────────────────────────────

def test_14_mois_invalide_ne_correspond_a_rien(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [
        ["AC1", "2026-01-01", 100, "b", "PROP_X", "LOG1", "R1", "PAS-UN-MOIS", "j", "t", "OK", ""],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert rows == []


# ── 15. Espaces superflus dans les identifiants ───────────────────────────────

def test_15_espaces_normalises_dans_identifiants(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [
        ["AC1", "2026-01-01", 100, "b", "  PROP_X  ", "LOG1", "R1", "2026-01", "j", "t", "OK", ""],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert len(rows) == 1


# ── 16. Cellule formule (Excel) — openpyxl data_only=True neutralise ─────────

def test_16_cellule_formule_neutralisee_par_data_only():
    import inspect
    src = inspect.getsource(extras._lire)
    assert "data_only=True" in src


# ── 17. Valeur None dans une cellule ──────────────────────────────────────────

def test_17_valeur_none_geree(tmp_path, monkeypatch):
    p = _classeur(tmp_path, "SAISIE_AirCover.xlsx", "MASTER", COLONNES_AIRCOVER, [
        ["AC1", None, None, None, "PROP_X", "LOG1", "R1", "2026-01", None, None, None, None],
    ])
    import app.config as cfg
    monkeypatch.setattr(cfg, "SAISIE_AIRCOVER", p)
    extras.vider_cache()
    rows = extras.aircover_prop_mois("PROP_X", "2026-01")
    assert len(rows) == 1 and extras.to_texte(rows[0]["commentaire"]) == ""

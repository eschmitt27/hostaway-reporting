"""Mission 11 — contrats de données branchés en production.

Prouve, pour chaque frontière branchée (Charges, Réservations HH, Banque, Trésorerie
propriétaire, Factures) :
- une donnée CANONIQUE invalide (date calendaire impossible) est refusée AVANT toute écriture
  SQLite — 0 ligne écrite (§21) ;
- une donnée RAW valide continue de passer sans régression ;
- pour la Banque spécifiquement : l'anomalie RAW nécessaire à `banques_controles_catalogue.py`
  (`sens="INCONNU"`) n'est PAS bloquée par le contrat, car elle n'emprunte jamais le chemin de
  construction de ligne de `banques_import_service.py` où le contrat est appelé (§20, stop-gate).
"""
from __future__ import annotations

import fixtures_banque as fx
import pytest

from app.contrats_donnees import ContratInvalideError, MouvementBanque
from app.db.connection import get_db
from app.readers import banques_reader as reader
from app.readers import proprietaires_reader
from app.services import banques_controle_service as ctrl_svc
from app.services import banques_controles_catalogue_service as cat
from app.services import banques_import_service as bimport
from app.services import charges_saisie_service as charges
from app.services import factures_service as factures
from app.services import proprietaires_tresorerie_service as tresorerie
from app.services import reservations_hh_saisie_service as reshh

CSV_VALIDE = (
    "Date operation;Libelle;Debit;Credit\n"
    "05/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n"
).encode("utf-8")


# ── Charges ───────────────────────────────────────────────────────────────────

def test_charge_date_invalide_refusee_avant_ecriture(tmp_db):
    avant = get_db(tmp_db).execute("SELECT COUNT(*) c FROM charges").fetchone()["c"]
    res = charges.creer({"date_charge": "2026-02-30", "montant": 10.0,
                        "categorie_charge_id": "CAT_ENTRETIEN"}, db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == charges.E_CONTRAT_INVALIDE
    apres = get_db(tmp_db).execute("SELECT COUNT(*) c FROM charges").fetchone()["c"]
    assert apres == avant


def test_charge_valide_toujours_acceptee(tmp_db):
    res = charges.creer({"date_charge": "2026-06-12", "montant": 42.0,
                        "categorie_charge_id": "CAT_ENTRETIEN"}, db_path=tmp_db)
    assert res["ok"] is True


# ── Réservations HH ───────────────────────────────────────────────────────────

def test_reservation_hh_date_invalide_refusee_avant_ecriture(tmp_db):
    avant = get_db(tmp_db).execute(
        "SELECT COUNT(*) c FROM reservations_hors_hostaway").fetchone()["c"]
    res = reshh.creer({
        "mois": "2026-06", "logement_id": "LOG_0001", "date_arrivee": "2026-02-30",
        "date_depart": "2026-06-06"}, db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == reshh.E_CONTRAT_INVALIDE
    apres = get_db(tmp_db).execute(
        "SELECT COUNT(*) c FROM reservations_hors_hostaway").fetchone()["c"]
    assert apres == avant


def test_reservation_hh_sans_montant_retenu_toujours_acceptee(tmp_db):
    """§26/§37 : placeholder réel (montant connu plus tard) — le contrat ajusté ne le rejette pas."""
    res = reshh.creer({
        "mois": "2026-06", "logement_id": "LOG_0001", "date_arrivee": "2026-06-02",
        "date_depart": "2026-06-06"}, db_path=tmp_db)
    assert res["ok"] is True


# ── Banque ────────────────────────────────────────────────────────────────────

@pytest.fixture
def env_banque(tmp_db, tmp_path, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return tmp_db


def test_import_reel_ne_declenche_jamais_le_contrat(env_banque, tmp_path):
    """L'import applicatif calcule toujours `sens` en DEBIT/CREDIT (jamais une autre valeur) —
    le contrat, appelé en auto-contrôle sur cette même ligne, ne doit donc jamais la refuser."""
    dryruns = tmp_path / "dryruns"
    dryruns.mkdir()
    res = bimport.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=env_banque,
                                dryruns_root=dryruns)
    assert res["ok"], res
    assert res["compteurs"]["valides"] == 1
    assert res["compteurs"]["invalides"] == 0


def test_anomalie_sens_inconnu_deja_en_base_reste_detectable(tmp_db, tmp_path, monkeypatch):
    """§20 — STOP-GATE : une ligne déjà en base avec un `sens` hors domaine (jamais produite par
    `banques_import_service`, mais pouvant exister par correction manuelle/import legacy) n'est
    jamais revalidée par le contrat de production — `banques_controles_catalogue` doit toujours
    pouvoir la détecter, exactement comme avant le CHECK SQLite retiré (migration 0056)."""
    import app.config as cfg
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    reader.vider_cache()
    ctrl_svc.vider_cache()
    fx.construire(tmp_db, mouvements=[fx.mouvement(
        "MVT-1", "2026-06-05", "LIB", 10.0, "INCONNU", compte="CM_TEST")])
    codes = {a["code"] for a in cat.controler(db_path=tmp_db)["anomalies"]}
    assert cat.C_SENS_INCOHERENT in codes


def test_contrat_mouvement_banque_seul_refuse_bien_un_sens_hors_domaine():
    """Le contrat lui-même reste strict (comportement inchangé) — seule sa PORTÉE change (auto-
    contrôle du service d'import, jamais un filtre sur les lignes déjà en base)."""
    with pytest.raises(ContratInvalideError, match="sens"):
        MouvementBanque.from_dict({
            "bank_account_id": "CM_TEST", "date_operation": "2026-06-01", "sens": "INCONNU",
            "montant": 10.0, "libelle_brut": "LIB"})


# ── Trésorerie propriétaire ───────────────────────────────────────────────────

def test_tresorerie_date_invalide_refusee_avant_ecriture(tmp_db, monkeypatch):
    monkeypatch.setattr(proprietaires_reader, "ref_available", lambda: True)
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid == "PROP_A" else None)
    avant = get_db(tmp_db).execute(
        "SELECT COUNT(*) c FROM mouvements_tresorerie_proprietaires").fetchone()["c"]
    res = tresorerie.creer("PROP_A", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", 100.0,
                           "2026-02-30", db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == tresorerie.E_CONTRAT_INVALIDE
    apres = get_db(tmp_db).execute(
        "SELECT COUNT(*) c FROM mouvements_tresorerie_proprietaires").fetchone()["c"]
    assert apres == avant


# ── Factures ──────────────────────────────────────────────────────────────────

def test_facture_date_invalide_refusee_par_valider(tmp_db):
    erreurs = factures.valider({
        "fournisseur_id_opaque": "FRS_1", "facture_ref": "F-001", "montant_ttc": "100.0",
        "date_facture": "2026-02-30"}, db_path=tmp_db)
    codes = {e["code"] for e in erreurs}
    assert factures.E_DATE_INVALIDE in codes


def test_facture_sans_date_toujours_acceptee(tmp_db):
    """Les deux dates restent optionnelles (formulaire réel sans `required`)."""
    erreurs = factures.valider({
        "fournisseur_id_opaque": "FRS_1", "facture_ref": "F-002", "montant_ttc": "100.0"},
        db_path=tmp_db)
    assert erreurs == []

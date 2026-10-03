"""Réservations DIRECTES Hostaway — décision 2026-10-03 : Hostaway fait foi.

Constat réel : deux réservations directes (LOG_0001, septembre 2026) étaient absentes des factures
propriétaires — classées À CONTRÔLER à 0 € faute de saisie hors Hostaway, sans aucune alerte.

  · Lot1 valorise une directe = loyer + remises + ménage facturé (« Encaissement Total Séjour »
    du rapport Hostaway) ; extras non arbitrés → PAYOUT_INCOMPLET ; aucun montant → À CONTRÔLER.
  · Une facture propriétaire est refusée (création, validation, émission) tant qu'une réservation
    du logement et du mois reste À CONTRÔLER : plus jamais d'omission silencieuse.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_service as svc

LOT1 = Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "lot1_hostaway_extract.py"


@pytest.fixture(scope="module")
def lot1():
    import sys
    if str(LOT1.parent) not in sys.path:
        sys.path.insert(0, str(LOT1.parent))
    spec = importlib.util.spec_from_file_location("lot1_direct_test", LOT1)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ff(**champs):
    return [{"name": k, "value": v} for k, v in champs.items()]


def test_direct_encaissement_hostaway_avec_remise(lot1):
    """Gigi, 28/09/2026 : 281 − 14,05 + 35 = 301,95 € (rapport Hostaway), pas totalPrice 350 €."""
    payout, src, statut, _menage, _meta = lot1.PayoutCalculator().calc(
        {"status": "new", "listingMapId": 482204, "checkInDate": "2026-09-28"},
        _ff(baseRate=281.0, weeklyDiscount=-14.05, cleaningFee=35.0,
            totalPriceFromChannel=350.0), [], channel="DIRECT")
    assert (payout, src, statut) == (301.95, "direct_composantes", "NORMAL")


def test_direct_encaissement_hostaway_simple(lot1):
    """Milo, 25/09/2026 : 82 + 35 = 117 €."""
    payout, _src, statut, *_ = lot1.PayoutCalculator().calc(
        {"status": "modified"}, _ff(baseRate=82.0, cleaningFee=35.0,
                                    totalPriceFromChannel=75.0), [], channel="DIRECT")
    assert (payout, statut) == (117.0, "NORMAL")


def test_direct_remise_mensuelle(lot1):
    payout, *_ = lot1.PayoutCalculator().calc(
        {"status": "new"}, _ff(baseRate=3487.0, monthlyDiscount=-697.4, cleaningFee=40.0),
        [], channel="DIRECT")
    assert payout == 2829.6


def test_direct_sans_montant_reste_a_controler(lot1):
    payout, src, statut, *_ = lot1.PayoutCalculator().calc({"status": "new"}, [], [],
                                                         channel="DIRECT")
    assert payout is None and statut == "A_CONTROLER" and src == "DIRECT_SANS_MONTANT"


def test_direct_avec_extras_non_arbitres_reste_a_controler(lot1):
    payout, src, statut, *_ = lot1.PayoutCalculator().calc(
        {"status": "new"}, _ff(baseRate=100.0, reservationExpensesAndExtras=20.0), [],
        channel="DIRECT")
    assert statut == "PAYOUT_INCOMPLET" and src == "direct_composantes_extras"


def test_vrbo_inchange(lot1):
    *_x, statut, _m, _meta = lot1.PayoutCalculator().calc({"status": "new"}, [], [],
                                                          channel="VRBO")
    assert statut == "A_CONTROLER"


# ── Garde-fou facturation ──────────────────────────────────────────────────────────────────────

@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO reservations_datasets (dataset_id, etape, actif) "
                 "VALUES ('RDS-T', 'RESOLUES', 1)")
    conn.commit()
    conn.close()
    return chemin


def _resolue(db, calc_id, statut, *, logement="LOG_FIXT_1", mois="2026-09", code=None):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, "
        "reservation_id_hostaway, logement_id, mois, date_arrivee, statut_controle, "
        "code_anomalie) VALUES ('RDS-T',?,?,?,?,?,?,?)",
        (calc_id, calc_id.replace("RES-HA-", ""), logement, mois, mois + "-10", statut, code))
    conn.commit()
    conn.close()


def _source(**kw):
    base = {"mois": "2026-09", "proprietaire_id": "PROP_FIXT_1", "logement_id": "LOG_FIXT_1",
            "source_calcul": "PREF-T", "COMMISSION_CONCIERGERIE": 100.0,
            "montant_du_conciergerie": 100.0}
    base.update(kw)
    return base


def test_facture_refusee_si_reservation_a_controler(db):
    _resolue(db, "RES-HA-1", "VALIDE")
    _resolue(db, "RES-HA-2", "A_CONTROLER", code="DIRECT_CHEVAUCHE_RESERVATION")
    with pytest.raises(svc.FactureProprietaireError) as exc:
        svc.creer(_source(), db_path=db)
    assert svc.C_RESERVATIONS_A_CONTROLER in str(exc.value)
    assert "DIRECT_CHEVAUCHE_RESERVATION" in str(exc.value)
    assert svc.lister(db_path=db) == []


def test_facture_creee_si_tout_est_valide_ou_concerne_un_autre_logement(db):
    _resolue(db, "RES-HA-1", "VALIDE")
    _resolue(db, "RES-HA-9", "A_CONTROLER", logement="LOG_AUTRE")
    _resolue(db, "RES-HA-8", "A_CONTROLER", mois="2026-10")
    f = svc.creer(_source(), db_path=db)
    assert f["statut"] == svc.ST_BROUILLON


def test_validation_refusee_si_une_reservation_devient_a_controler(db):
    f = svc.creer(_source(), db_path=db)
    _resolue(db, "RES-HA-3", "A_CONTROLER", code="DIRECT_PAYOUT_PAYOUT_INCOMPLET")
    emetteur = {"nom": "E", "adresse": "A", "siret": "00000000000000"}
    with pytest.raises(svc.FactureProprietaireError) as exc:
        svc.valider(f["facture_id_opaque"], emetteur=emetteur, destinataire={"nom": "P"},
                    db_path=db)
    assert svc.C_RESERVATIONS_A_CONTROLER in str(exc.value)
    assert svc.lire(f["facture_id_opaque"], db_path=db)["statut"] == svc.ST_BROUILLON

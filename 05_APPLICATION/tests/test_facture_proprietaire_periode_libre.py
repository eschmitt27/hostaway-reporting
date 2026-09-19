"""Facture propriétaire sur une période choisie — recette utilisateur n°4, lot 2 §1-§4, §26.

CE QUI MANQUAIT : on ne pouvait facturer qu'un mois entier, une fois le mois passé. Un
propriétaire qui arrête son contrat le 10 septembre attendait donc le 30 pour être facturé de ses
dix jours.

CE QUI NE DOIT PAS CHANGER : le cycle mensuel automatique, et le fait que les montants viennent du
moteur (calcul par réservation) — jamais d'un calcul refait ici.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_periode_service as periode
from app.services import factures_proprietaires_service as svc

PROP = "PROP_0001"
LOG = "LOG_0001"
RUN = "L10-TEST"


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


def _reservation(conn, *, reservation, arrivee, depart, commission, menage, net, logement=LOG):
    conn.execute(
        "INSERT INTO lot10_commissions (run_id, flux_source_pk, reservation_calc_id, logement_id, "
        "proprietaire_id, mois, date_arrivee, date_depart, nuits, payout_calcule, menage_retenu, "
        "assiette_commission, taux_commission, commission_conciergerie, net_proprietaire) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (RUN, reservation, reservation, logement, PROP, arrivee[:7], arrivee, depart, 2.0,
         commission + menage + net, menage, commission / 0.2, 0.2, commission, net))


@pytest.fixture()
def base(tmp_path):
    """Un run Lot10 actif, quatre réservations de septembre : deux avant le 10, deux après."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, actif) VALUES (?, 1)", (RUN,))
        _reservation(conn, reservation="RES-1", arrivee="2026-09-02", depart="2026-09-05",
                     commission=40.0, menage=50.0, net=210.0)
        _reservation(conn, reservation="RES-2", arrivee="2026-09-08", depart="2026-09-10",
                     commission=30.0, menage=50.0, net=120.0)
        _reservation(conn, reservation="RES-3", arrivee="2026-09-15", depart="2026-09-18",
                     commission=25.0, menage=50.0, net=100.0)
        _reservation(conn, reservation="RES-4", arrivee="2026-09-22", depart="2026-09-25",
                     commission=35.0, menage=50.0, net=140.0)
        conn.commit()
    finally:
        conn.close()
    return db


# ── §1/§26 — facturer une période partielle, mois non terminé ───────────────────────────────────

def test_facture_du_01_au_10_sans_attendre_la_fin_du_mois(base):
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-10", db_path=base)
    assert apercu["ok"] is True
    assert apercu["nb_reservations"] == 2, "seules les arrivées du 01 au 10"
    proposition = apercu["propositions"][0]
    assert proposition["logement_id"] == LOG
    montants = {l["type_ligne"]: l["montant"] for l in proposition["lignes"]}
    assert montants == {"COMMISSION_CONCIERGERIE": 70.0, "MENAGE_FACTURE": 100.0}
    assert proposition["montant_total"] == 170.0

    res = periode.creer(PROP, "2026-09-01", "2026-09-10", acteur="test", db_path=base)
    assert res["ok"] is True and res["nb"] == 1
    facture = svc.lire(res["creees"][0]["facture_id_opaque"], db_path=base)
    assert facture["statut"] == svc.ST_BROUILLON
    assert facture["montant_total"] == 170.0
    assert (facture["periode_debut"], facture["periode_fin"]) == ("2026-09-01", "2026-09-10")
    assert facture["proprietaire_id"] == PROP and facture["logement_id"] == LOG
    assert not facture.get("numero_facture"), "un brouillon n'a pas encore de numéro"


def test_les_montants_viennent_du_moteur_et_ne_sont_pas_recalcules(base):
    """Somme exacte des valeurs déjà calculées par réservation : aucune règle de commission ici."""
    conn = get_db(base)
    try:
        attendu = conn.execute(
            "SELECT ROUND(SUM(commission_conciergerie),2), ROUND(SUM(menage_retenu),2) "
            "FROM lot10_commissions WHERE date_arrivee BETWEEN ? AND ?",
            ("2026-09-01", "2026-09-30")).fetchone()
    finally:
        conn.close()
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    montants = {l["type_ligne"]: l["montant"] for l in apercu["propositions"][0]["lignes"]}
    assert montants["COMMISSION_CONCIERGERIE"] == attendu[0]
    assert montants["MENAGE_FACTURE"] == attendu[1]


def test_deux_periodes_qui_se_suivent_sont_autorisees(base):
    premiere = periode.creer(PROP, "2026-09-01", "2026-09-10", acteur="test", db_path=base)
    seconde = periode.creer(PROP, "2026-09-11", "2026-09-30", acteur="test", db_path=base)
    assert premiere["ok"] is True and seconde["ok"] is True
    assert seconde["chevauchements"] == [], "11→30 ne recouvre pas 01→10"
    totaux = sorted(svc.lire(c["facture_id_opaque"], db_path=base)["montant_total"]
                    for r in (premiere, seconde) for c in r["creees"])
    assert totaux == [160.0, 170.0]


def test_periode_chevauchante_alerte_sans_bloquer(base):
    periode.creer(PROP, "2026-09-01", "2026-09-10", acteur="test", db_path=base)
    apercu = periode.previsualiser(PROP, "2026-09-05", "2026-09-15", db_path=base)
    assert len(apercu["chevauchements"]) == 1
    alerte = apercu["chevauchements"][0]
    assert alerte["statut"] == svc.ST_BROUILLON
    assert alerte["montant_total"] == 170.0
    assert (alerte["periode_debut_effective"], alerte["periode_fin_effective"]) == \
        ("2026-09-01", "2026-09-10")
    # Alerte, pas blocage : la création reste possible si elle est légitime.
    seconde = periode.creer(PROP, "2026-09-05", "2026-09-15", acteur="test", db_path=base)
    assert seconde["ok"] is True and len(seconde["chevauchements"]) == 1


def test_une_facture_mensuelle_existante_est_vue_comme_chevauchante(base):
    """Le cycle mensuel n'écrit pas de période : son mois vaut du 1er au dernier jour."""
    svc.creer_exceptionnelle(proprietaire_id=PROP, logement_id=LOG, mois="2026-09",
                             lignes=[{"libelle": "Commission", "montant": 100.0}],
                             acteur="test", db_path=base)
    apercu = periode.previsualiser(PROP, "2026-09-20", "2026-09-25", db_path=base)
    assert [c["periode_debut_effective"] for c in apercu["chevauchements"]] == ["2026-09-01"]
    assert [c["periode_fin_effective"] for c in apercu["chevauchements"]] == ["2026-09-30"]


def test_periode_sans_reservation_ne_cree_rien(base):
    res = periode.creer(PROP, "2026-10-01", "2026-10-31", acteur="test", db_path=base)
    assert res["ok"] is False and res["code"] == periode.E_AUCUNE_RESERVATION
    assert svc.lister(proprietaire_id=PROP, db_path=base) == []


@pytest.mark.parametrize("debut,fin", [("", "2026-09-10"), ("2026-09-10", "2026-09-01"),
                                       ("pas-une-date", "2026-09-10")])
def test_periode_invalide_refusee(base, debut, fin):
    res = periode.previsualiser(PROP, debut, fin, db_path=base)
    assert res["ok"] is False and res["code"] == periode.E_PERIODE_INVALIDE


def test_un_seul_proprietaire_a_la_fois(base):
    """Le parcours manuel ne force pas à générer toutes les factures du parc."""
    conn = get_db(base)
    try:
        conn.execute(
            "INSERT INTO lot10_commissions (run_id, reservation_calc_id, logement_id, "
            "proprietaire_id, mois, date_arrivee, date_depart, commission_conciergerie, "
            "menage_retenu, net_proprietaire) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (RUN, "RES-9", "LOG_0099", "PROP_0002", "2026-09", "2026-09-03", "2026-09-06",
             99.0, 50.0, 200.0))
        conn.commit()
    finally:
        conn.close()
    res = periode.creer(PROP, "2026-09-01", "2026-09-10", acteur="test", db_path=base)
    assert res["ok"] is True
    factures = svc.lister(db_path=base)
    assert [f["proprietaire_id"] for f in factures] == [PROP]


# ── L'écran ─────────────────────────────────────────────────────────────────────────────────────

def test_ecran_propose_la_nouvelle_facture_et_previsualise(client, tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, actif) VALUES (?, 1)", (RUN,))
        _reservation(conn, reservation="RES-1", arrivee="2026-09-02", depart="2026-09-05",
                     commission=40.0, menage=50.0, net=210.0)
        conn.commit()
    finally:
        conn.close()
    liste = client.get("/factures-proprietaires").text
    assert 'data-testid="nouvelle-facture-proprietaire"' in liste

    page = client.get("/factures-proprietaires/nouvelle",
                      params={"proprietaire_id": PROP, "debut": "2026-09-01", "fin": "2026-09-10"})
    assert page.status_code == 200
    assert 'data-testid="creer-brouillon"' in page.text
    assert "90.00" in page.text.replace("&nbsp;", " ")

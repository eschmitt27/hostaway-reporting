"""E2E A — Créer une réservation hors Hostaway DEPUIS L'ÉCRAN, et la suivre jusqu'au bout.

Recette utilisateur n°4, §2 et §3. L'utilisateur disait ne pas réussir à créer une réservation hors
Hostaway. Le parcours écrivait pourtant bien en base : deux défauts le rendaient inutilisable.

  1. L'écran désactivait son bouton de validation pour tout mois POSTÉRIEUR au mois courant, parce
     que la liste des mois ouverts s'arrêtait au mois courant — alors que la règle D10 accepte tout
     mois postérieur à la dernière clôture. Or une réservation se prend AVANT le séjour.
  2. La ligne était écrite avec statut_controle = A_CONTROLER, valeur qu'aucune route ne pouvait
     lever, et que `lot4bis_charger_reservations` écarte : la réservation n'entrait donc dans aucun
     calcul — ni table commune, ni résultat, ni commission, ni facture propriétaire — en silence.

Ces tests rejouent le VRAI parcours HTTP (aucun service mocké) sur une base temporaire, puis font
traverser la chaîne canonique RESERVATIONS -> FLUX -> LOT10 -> LOT11 -> LOT12.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import get_db
from app.main import app
from app.services import (
    controles_lot11_service,
    flux_unifie_service,
    lot12_prefactures_service,
    menages_origine_service,
    orchestrateur_moteur as om,
    reservations_hh_confirmation_service as confirmation,
)

LOGEMENT = "LOG_E2E1"
PROPRIETAIRE = "PROP_E2E"
CANAL_DIRECT = "CANAL_004"       # libellé humain attendu à l'écran : « Direct »
TYPE_LOGEMENT = "TYPE_E2E"
COUT_MENAGE = 60.0
TOTAL_PERCU = 500.0


def _mois_decale(nb_mois: int) -> str:
    aujourdhui = date.today()
    total = (aujourdhui.year * 12 + aujourdhui.month - 1) + nb_mois
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def _referentiels(db_path) -> None:
    """Le minimum pour qu'un logement soit géré, facturable et rapprochable."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "nom_court, statut_parc, actif, import_id) "
            "VALUES (?,?,?,?,'GERE','OUI','IMP-E2E')",
            (LOGEMENT, "888001", TYPE_LOGEMENT, "Studio E2E"))
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-E2E',?,?,'2025-01-01','','ACTIF','IMP-E2E')", (LOGEMENT, PROPRIETAIRE))
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-E2E','Hostaway','listingMapId','888001',?,'OUI','IMP-E2E')", (LOGEMENT,))
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES (?,'E2E','Proprio','OUI','IMP-E2E')",
            (PROPRIETAIRE,))
        conn.execute(
            "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
            "taux_commission, date_debut, actif, import_id) "
            "VALUES ('TXC-E2E',?,?,0.20,'2025-01-01','OUI','IMP-E2E')", (PROPRIETAIRE, LOGEMENT))
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, date_fin_validite, actif, import_id) "
            "VALUES ('COUT-E2E',?,?,'2025-01-01','','OUI','IMP-E2E')", (TYPE_LOGEMENT, COUT_MENAGE))
        conn.execute(
            "INSERT INTO ref_canaux_reservation (canal_id, canal, source_principale, dans_hostaway,"
            " hors_compta_possible, actif, import_id) "
            "VALUES (?,'Direct','Direct','NON','OUI','OUI','IMP-E2E')", (CANAL_DIRECT,))
        conn.execute(
            "INSERT INTO ref_modes_paiement (mode_paiement_id, mode_paiement, impact_banque, "
            "impact_caisse, impact_associee, actif, import_id) "
            "VALUES ('PAY_001','BANQUE_PRO','OUI','NON','NON','OUI','IMP-E2E')")
        for code, comptable, reel in (("IC", "OUI", "OUI"), ("HC", "NON", "OUI")):
            conn.execute(
                "INSERT INTO ref_codes_impact (code_impact, libelle, impact_resultat_comptable, "
                "impact_resultat_extra, impact_resultat_reel, impact_avantages, actif, import_id) "
                "VALUES (?,?,?,'NON',?,'SELON_FLUX','OUI','IMP-E2E')",
                (code, code, comptable, reel))
        # Frontière de clôture : tout ce qui suit est saisissable, y compris les mois à venir.
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'CLOTURE', 'IMP-E2E')", (_mois_decale(-6),))
        # lot4bis refuse de tourner sans extraction Hostaway de référence.
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-E2E','R-E2E','API','2026-01-01T00:00:00Z','SUCCES',0,0,0)")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def client(tmp_db, tmp_path, monkeypatch):
    """Parcours HTTP réel sur base temporaire, manifests de prévisualisation isolés."""
    monkeypatch.setattr(confirmation, "DRYRUNS_DIR", tmp_path / "dryruns")
    _referentiels(tmp_db)
    with TestClient(app) as c:
        yield c


def _formulaire(mois: str) -> dict[str, str]:
    return {
        "logement_id": LOGEMENT,
        "proprietaire_id": PROPRIETAIRE,
        "canal_id": CANAL_DIRECT,
        "source_financiere": "SAISIE_MANUELLE",
        "date_arrivee": f"{mois}-10",
        "date_depart": f"{mois}-14",
        "total_percu": str(TOTAL_PERCU),
        "menage": str(COUT_MENAGE),
        "code_impact": "IC",
        "mode_paiement_id": "PAY_001",
        "commentaire": "Réservation directe E2E",
    }


def _creer_par_l_ecran(client, mois: str) -> str:
    """Rejoue les cinq étapes de l'écran et rend l'identifiant de la réservation créée."""
    form = _formulaire(mois)

    verif = client.post("/reservations/nouvelle/verifier", data=form)
    assert verif.status_code == 200, verif.text[:2000]

    previsu = client.post("/reservations/nouvelle/previsualiser", data=form,
                          follow_redirects=False)
    assert previsu.status_code == 303, previsu.text[:2000]
    token = previsu.headers["location"].rsplit("/", 1)[-1]

    page = client.get(f"/reservations/nouvelle/previsualisation/{token}")
    assert page.status_code == 200
    # Le canal se lit en clair, jamais sous sa forme technique.
    assert "Direct" in page.text and CANAL_DIRECT not in page.text

    ecriture = client.post(f"/reservations/nouvelle/previsualisation/{token}/enregistrer", data={})
    assert ecriture.status_code == 200, ecriture.text[:2000]
    assert "enregistrée" in ecriture.text

    conn = sqlite3.connect(str(cfg.DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        lignes = [dict(r) for r in conn.execute(
            "SELECT reservation_hh_id, statut, statut_controle, niveau_anomalie, code_impact, "
            "canal_id, mois, montant_percu FROM reservations_hors_hostaway")]
    finally:
        conn.close()
    assert len(lignes) == 1, lignes
    return lignes[0]


# ── Le blocage que vivait l'utilisateur ─────────────────────────────────────────────────────────

def test_un_mois_futur_est_proposé_par_l_ecran_comme_le_backend_l_accepte(client):
    """Une réservation prise à l'avance doit être saisissable : l'écran disait l'inverse."""
    page = client.get("/reservations/nouvelle")
    assert page.status_code == 200
    for decalage in (1, 2, 6):
        assert _mois_decale(decalage) in page.text, (
            f"le mois {_mois_decale(decalage)} doit être proposé : il est postérieur à la dernière "
            "clôture, donc ouvert au sens de la règle D10")


def test_creation_dans_un_mois_futur_aboutit_a_une_reservation_exploitable(client):
    mois_futur = _mois_decale(2)
    ligne = _creer_par_l_ecran(client, mois_futur)

    assert ligne["mois"] == mois_futur
    assert ligne["statut"] == "ACTIVE"
    # Le coeur du défaut : A_CONTROLER faisait disparaître la réservation de tous les calculs.
    assert ligne["statut_controle"] == "VALIDE"
    assert ligne["niveau_anomalie"] == "INFO"
    assert ligne["montant_percu"] == pytest.approx(TOTAL_PERCU)


def test_la_fiche_affiche_le_canal_en_clair(client):
    ligne = _creer_par_l_ecran(client, _mois_decale(1))
    fiche = client.get(f"/reservations/{ligne['reservation_hh_id']}")
    assert fiche.status_code == 200
    assert "Direct" in fiche.text
    assert CANAL_DIRECT not in fiche.text, "le canal technique ne doit jamais s'afficher tel quel"


# ── Les impacts aval (§3) ───────────────────────────────────────────────────────────────────────

def test_la_reservation_saisie_traverse_jusqua_la_prefacture_proprietaire(client, tmp_db):
    """Persistance -> table commune -> flux -> résultat -> commission -> préfacture propriétaire.

    Mois précédent, et non mois courant : lot10 écarte volontairement le mois EN_COURS de ses
    sorties (`exclure_mois_clotures`) — un mois qui n'est pas fini ne produit pas de règlement.
    """
    mois = _mois_decale(-1)
    ligne = _creer_par_l_ecran(client, mois)

    assert om.executer_reservations(db_path=tmp_db)["ok"] is True
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    try:
        resolues = [dict(r) for r in conn.execute(
            "SELECT source, statut_controle, mois, montant_retenu FROM reservations_resolues")]
        assert len(resolues) == 1, "la réservation saisie doit entrer dans la table commune"
        assert resolues[0]["source"] == "MANUEL_HORS_HOSTAWAY"
        assert resolues[0]["statut_controle"] == "VALIDE"
        assert resolues[0]["mois"] == mois, "elle doit tomber dans la période de son arrivée"

        assert flux_unifie_service.construire(db_path=tmp_db)["ok"] is True
        flux = [dict(r) for r in conn.execute("SELECT montant, code_impact FROM flux_unifies")]
        assert len(flux) == 1 and flux[0]["montant"] == pytest.approx(TOTAL_PERCU)

        assert om.executer_lot10(db_path=tmp_db)["ok"] is True
        commissions = [dict(r) for r in conn.execute(
            "SELECT payout_calcule, commission_conciergerie, net_proprietaire "
            "FROM lot10_commissions")]
        assert len(commissions) == 1, "sans elle, aucune commission n'était calculée"
        # Assiette = encaissé - ménage : le ménage n'est pas une recette du propriétaire.
        assiette = TOTAL_PERCU - COUT_MENAGE
        assert commissions[0]["commission_conciergerie"] == pytest.approx(assiette * 0.20)
        assert commissions[0]["net_proprietaire"] == pytest.approx(assiette * 0.80)

        assert controles_lot11_service.construire(db_path=tmp_db)["ok"] is True
        resultat_lot12 = lot12_prefactures_service.construire(db_path=tmp_db)
        assert resultat_lot12["ok"] is True
        assert resultat_lot12["nb_entetes"] == 1, "la facture propriétaire doit exister"
    finally:
        conn.close()

    # Ménage attendu hors Hostaway : la réservation en appelle un, et un seul.
    origines = menages_origine_service.origines(mois=mois, db_path=tmp_db)
    assert origines["hors_hostaway"]["attendus"] == 1
    assert ligne["reservation_hh_id"].startswith("RESHH-")


def test_une_reservation_exclue_n_est_pas_comptee_comme_menage_attendu(client, tmp_db):
    """Le compteur ne filtrait que les annulations : il gonflait d'un travail inexistant."""
    mois = _mois_decale(-1)
    _creer_par_l_ecran(client, mois)
    conn = get_db(tmp_db)
    try:
        conn.execute("UPDATE reservations_hors_hostaway SET statut_controle = 'EXCLU_RESULTAT'")
        conn.commit()
    finally:
        conn.close()

    origines = menages_origine_service.origines(mois=mois, db_path=tmp_db)
    assert origines["hors_hostaway"]["attendus"] == 0

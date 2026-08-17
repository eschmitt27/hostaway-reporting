"""Hostaway et réservations en SQLite : couche RAW, datasets, lecteurs, actualisation.

CE QUE CE FICHIER PROUVE
Le chemin normal est API → SQLite. Les masters legacy sont rendus introuvables dans toute la classe
de tests : si un service en avait encore besoin, il échouerait ici plutôt qu'en production.

Les chemins de configuration sont pointés sur des fichiers inexistants — plus fiable que de supposer
qu'ils sont absents de la machine, et sans rien déplacer sur le disque.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
import fixtures_hostaway as fx
from app.services import hostaway_raw_service as raw
from app.services import reservations_dataset_service as ds

PROP = "PROP_0001"
LOG = fx.LOGEMENT


@pytest.fixture(autouse=True)
def sans_masters(tmp_db, tmp_path, monkeypatch):
    """Aucun master réservations atteignable, quelle que soit la machine."""
    absent = tmp_path / "AUCUN_MASTER"
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES",
                        absent / "MASTER_CALC_Reservations_Resolues.xlsx")
    if hasattr(cfg, "MASTER_CALC_RESERVATIONS"):
        monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS",
                            absent / "MASTER_CALC_Reservations.xlsx")
    yield absent


def _dataset(db_path, etape: str, lignes: list[dict], *, extraction_id: str = "") -> str:
    """Écrit un dataset de réservations. Reproduit ce que le moteur écrit, sans le lancer."""
    import uuid

    from app.db.connection import get_db

    dataset_id = "RDS-" + uuid.uuid4().hex[:12].upper()
    table = "reservations_calculees" if etape == ds.ETAPE_CALCULEES else "reservations_resolues"
    colonnes = (ds.COLONNES_BASE if etape == ds.ETAPE_CALCULEES
                else ds.COLONNES_BASE + ds.COLONNES_RESOLUTION)

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE reservations_datasets SET actif = 0 WHERE etape = ?", (etape,))
        conn.execute(
            "INSERT INTO reservations_datasets (dataset_id, etape, extraction_id, nb_lignes, "
            "actif) VALUES (?,?,?,?,1)",
            (dataset_id, etape, extraction_id or None, len(lignes)))
        trous = ", ".join(["?"] * (len(colonnes) + 1))
        conn.executemany(
            f"INSERT INTO {table} (dataset_id, {', '.join(colonnes)}) VALUES ({trous})",
            [(dataset_id, *(l.get(c) for c in colonnes)) for l in lignes])
        conn.commit()
    finally:
        conn.close()
    return dataset_id


def _ligne(calc_id: str, *, mois: str = "2026-07", source: str = "HOSTAWAY_AIRBNB",
           reservation_id: str = "", montant: float = 200.0, guests: int = 2,
           statut: str = "VALIDE", etat_mois: str = "OUVERT", **extra) -> dict:
    base = {
        "reservation_calc_id": calc_id, "row_hash": f"H-{calc_id}", "source": source,
        "reservation_id_hostaway": reservation_id, "reservation_hh_id": "", "mois": mois,
        "logement_id": LOG, "proprietaire_id": PROP, "date_arrivee": f"{mois}-05",
        "date_depart": f"{mois}-07", "nuits": 2, "guest_count": guests,
        "source_guest_count": "API_LIST", "montant_retenu": montant,
        "source_montant": "HOSTAWAY_PAYOUT", "code_impact": "IC",
        "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI",
        "statut_controle": statut, "niveau_anomalie": "", "code_anomalie": "",
        "commentaire": "", "source_module": "LOT1", "source_table": "hostaway_reservations",
        "source_pk": reservation_id, "date_integration": "2026-08-17",
        "canal": "AIRBNB", "etat_mois": etat_mois, "origine_initiale": "API_HOSTAWAY",
        "source_ligne": source, "methode": "LIVE", "payout_calcule": montant - 50,
        "menage_retenu": 50.0, "assiette_commission": montant - 100,
    }
    base.update(extra)
    return base


# ── 1. Les masters sont bien introuvables ───────────────────────────────────────────────────────

def test_les_masters_reservations_sont_introuvables(sans_masters):
    assert not Path(cfg.MASTER_CALC_RESERVATIONS_RESOLUES).exists()


# ── 2. Couche RAW : API → SQLite ────────────────────────────────────────────────────────────────

def test_extraction_ecrit_les_sept_groupes(tmp_db):
    eid = fx.extraire(
        tmp_db,
        listings=[fx.listing()],
        reservations=[fx.reservation("70001", check_in="2026-07-01")],
        payouts=[fx.payout("70001")],
        fees=[fx.fee("70001")],
        finance_fields=[fx.finance_field("70001")],
        anomalies=[fx.anomalie("70001")])

    assert len(raw.listings(db_path=tmp_db)) == 1
    assert len(raw.reservations(db_path=tmp_db)) == 1
    assert len(raw.payouts(db_path=tmp_db)) == 1
    assert len(raw.fees(db_path=tmp_db)) == 1
    assert len(raw.finance_fields(db_path=tmp_db)) == 1
    assert len(raw.anomalies(db_path=tmp_db)) == 1
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == eid


def test_rejouer_la_meme_extraction_ne_duplique_rien(tmp_db):
    """Idempotence : la contrainte porte sur (extraction, réservation)."""
    eid = raw.ouvrir(mode=raw.MODE_API, db_path=tmp_db)
    lignes = [fx.reservation("70001", check_in="2026-07-01")]
    raw.enregistrer(eid, reservations=lignes, db_path=tmp_db)
    raw.enregistrer(eid, reservations=lignes, db_path=tmp_db)
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    assert len(raw.reservations(extraction_id=eid, db_path=tmp_db)) == 1


def test_deux_extractions_sont_deux_etats_comparables(tmp_db):
    """Une nouvelle extraction ne modifie jamais la précédente."""
    a = fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01",
                                                         total_price=100.0)])
    b = fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01",
                                                         total_price=999.0)])
    assert raw.reservations(extraction_id=a, db_path=tmp_db)[0]["total_price"] == 100.0
    assert raw.reservations(extraction_id=b, db_path=tmp_db)[0]["total_price"] == 999.0
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == b


def test_une_extraction_echouee_nest_pas_utilisable(tmp_db):
    """Ses lignes sont un fragment ; s'en servir donnerait un compte faux."""
    fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01")])
    fx.extraire(tmp_db, reservations=[fx.reservation("70002", check_in="2026-07-02")],
                statut=raw.ST_ECHEC)
    utilisable = raw.derniere_extraction_utilisable(db_path=tmp_db)
    assert [r["reservation_id"] for r in raw.reservations(db_path=tmp_db)] == ["70001"]
    assert utilisable


def test_une_extraction_partielle_reste_utilisable_et_signalee(tmp_db):
    """Incomplète n'est pas fausse — mais l'écran doit pouvoir le dire."""
    fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01")],
                statut=raw.ST_PARTIEL)
    f = raw.fraicheur(db_path=tmp_db)
    assert f["disponible"] is True
    assert f["complet"] is False
    assert f["code"] == raw.E_EXTRACTION_PARTIELLE


def test_reservation_et_payout_restent_deux_faits(tmp_db):
    """« Payout absent » ne doit pas ressembler à « payout à zéro »."""
    fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01"),
                                      fx.reservation("70002", check_in="2026-07-02")],
                payouts=[fx.payout("70001")])
    assert len(raw.reservations(db_path=tmp_db)) == 2
    assert [p["reservation_id"] for p in raw.payouts(db_path=tmp_db)] == ["70001"]


def test_sans_extraction_letat_est_nomme(tmp_db):
    f = raw.fraicheur(db_path=tmp_db)
    assert f["disponible"] is False
    assert f["code"] == raw.E_AUCUNE_EXTRACTION
    assert "actualisation" in f["message"].lower()


# ── 3. guestCount : payload tronqué ─────────────────────────────────────────────────────────────

def test_guest_count_survit_a_un_payload_tronque(tmp_db):
    """Les payloads repris des masters sont coupés à 4 000 caractères : le JSON devient invalide.

    On reproduit exactement ce cas : un payload volumineux tronqué à la limite d'une cellule Excel.
    Le nombre de voyageurs figure avant la coupure, donc l'information EST là — seul `json.loads`
    refuse de la lire. C'est la seule situation où le repli textuel a une raison d'exister.
    """
    complet = fx.reservation("70001", check_in="2026-07-01", payload_guests=5)
    tronque = dict(fx.reservation("70002", check_in="2026-07-02", payload_guests=3,
                                  payload_bourrage=8000))
    tronque["json_snapshot"] = tronque["json_snapshot"][:4000]

    import json
    with pytest.raises(ValueError):
        json.loads(tronque["json_snapshot"])

    fx.extraire(tmp_db, reservations=[complet, tronque])
    trouve = raw.guest_count_par_reservation(db_path=tmp_db)
    assert trouve["70001"] == 5
    assert trouve["70002"] == 3, "le repli textuel doit lire un payload que json refuse"


def test_guest_count_absent_du_payload_tronque_nest_pas_invente(tmp_db):
    """Si la coupure emporte l'information, on ne la devine pas."""
    ampute = dict(fx.reservation("70003", check_in="2026-07-03", payload_guests=9))
    ampute["json_snapshot"] = ampute["json_snapshot"][:40]
    fx.extraire(tmp_db, reservations=[ampute])
    assert "70003" not in raw.guest_count_par_reservation(db_path=tmp_db)


def test_guest_count_sur_payload_volumineux(tmp_db):
    """Au-delà de l'ancienne limite de cellule, la valeur doit rester lisible."""
    grosse = fx.reservation("70001", check_in="2026-07-01", payload_guests=7,
                            payload_bourrage=6000)
    assert len(grosse["json_snapshot"]) > 4000
    fx.extraire(tmp_db, reservations=[grosse])
    assert raw.guest_count_par_reservation(db_path=tmp_db)["70001"] == 7


def test_guest_count_absent_nest_pas_invente(tmp_db):
    sans = fx.reservation("70001", check_in="2026-07-01", payload_guests=None)
    fx.extraire(tmp_db, reservations=[sans])
    assert "70001" not in raw.guest_count_par_reservation(db_path=tmp_db)


# ── 4. Identifiants : opaques et textuels ───────────────────────────────────────────────────────

def test_identifiants_conserves_tels_quels(tmp_db):
    """Un identifiant à zéro non significatif ne doit pas être mutilé par une conversion."""
    fx.extraire(tmp_db, reservations=[fx.reservation("00700123", check_in="2026-07-01")],
                payouts=[fx.payout("00700123")])
    assert raw.reservations(db_path=tmp_db)[0]["reservation_id"] == "00700123"
    assert raw.payouts(db_path=tmp_db)[0]["reservation_id"] == "00700123"


def test_payout_retrouve_sa_reservation(tmp_db):
    """Non-régression : la jointure réservation ↔ payout échouait en silence."""
    fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01")],
                payouts=[fx.payout("70001", montant=180.0)])
    par_id = {p["reservation_id"]: p for p in raw.payouts(db_path=tmp_db)}
    for r in raw.reservations(db_path=tmp_db):
        assert par_id.get(r["reservation_id"]) is not None
        assert par_id[r["reservation_id"]]["payout_calcule"] == 180.0


# ── 5. Datasets : live, résolu, historique ──────────────────────────────────────────────────────

def test_dataset_courant_et_etat(tmp_db):
    assert ds.etat(db_path=tmp_db) == ds.ETAT_NON_INITIALISE
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001")])
    assert ds.etat(db_path=tmp_db) == ds.ETAT_OK
    assert ds.disponible(db_path=tmp_db)


def test_un_recalcul_remplace_le_jeu_courant_sans_effacer_le_precedent(tmp_db):
    premier = _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001")])
    second = _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001"),
                                                  _ligne("RES-2026-07-HA-002")])
    courant = ds.dataset_courant(db_path=tmp_db)
    assert courant["dataset_id"] == second
    assert len(ds.lignes(db_path=tmp_db)) == 2
    assert len(ds.lignes(dataset_id=premier, db_path=tmp_db)) == 1, \
        "le jeu précédent reste lisible : deux recalculs doivent rester comparables"


def test_par_source_et_index(tmp_db):
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [
        _ligne("RES-2026-07-HA-001", source="HOSTAWAY_VRBO_A_CONTROLER", reservation_id="70001"),
        _ligne("RES-2026-07-HA-002", reservation_id="70002")])
    assert len(ds.par_source("HOSTAWAY_VRBO_A_CONTROLER", db_path=tmp_db)) == 1
    assert "70002" in ds.index_par_reservation(db_path=tmp_db)


def test_existe_cherche_les_deux_identifiants(tmp_db):
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [
        _ligne("RES-2026-07-HA-001", reservation_id="70001"),
        _ligne("RES-2026-07-HH-001", reservation_id="", reservation_hh_id="HH-001")])
    assert ds.existe("70001", db_path=tmp_db)
    assert ds.existe("HH-001", db_path=tmp_db)
    assert not ds.existe("INCONNU", db_path=tmp_db)


def test_sans_dataset_rien_nest_invente(tmp_db):
    assert ds.lignes(db_path=tmp_db) == []
    assert ds.existe("70001", db_path=tmp_db) is False


# ── 6. Les lecteurs applicatifs ─────────────────────────────────────────────────────────────────

def test_controles_detail_vrbo_lit_le_resolu(tmp_db):
    """Le périmètre de contrôle est le RÉSOLU : le live ferait réapparaître des mois clos."""
    from app.readers import controles_detail_reader as detail

    _dataset(tmp_db, ds.ETAPE_RESOLUES, [
        _ligne("RES-2026-07-HA-001", source="HOSTAWAY_VRBO_A_CONTROLER", reservation_id="70001")])
    _dataset(tmp_db, ds.ETAPE_CALCULEES, [
        _ligne("RES-2026-07-HA-001", source="HOSTAWAY_VRBO_A_CONTROLER", reservation_id="70001"),
        _ligne("RES-2025-01-HA-009", source="HOSTAWAY_VRBO_A_CONTROLER", reservation_id="50009",
               mois="2025-01")])
    detail.vider_cache()

    assert len(detail.reservations_vrbo()) == 1
    hors = detail.reservations_vrbo_hors_perimetre()
    assert [r["reservation_id_hostaway"] for r in hors] == ["50009"]


def test_controles_detail_index_lit_le_live(tmp_db):
    """Le rattachement d'une commission doit couvrir aussi les mois clos."""
    from app.readers import controles_detail_reader as detail

    _dataset(tmp_db, ds.ETAPE_CALCULEES, [_ligne("RES-2025-01-HA-009", reservation_id="50009",
                                                 mois="2025-01")])
    detail.vider_cache()
    assert "50009" in detail.reservations_index()


def test_saisie_charges_verifie_la_reservation_en_base(tmp_db):
    from app.readers import saisie_charges_reader as saisie

    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001", reservation_id="70001")])
    assert saisie.reservation_id_exists("70001") is True
    assert saisie.reservation_id_exists("INCONNU") is False


def test_saisie_charges_refuse_sans_dataset(tmp_db):
    """Refuser une référence qu'on ne peut pas vérifier vaut mieux que l'accepter."""
    from app.readers import saisie_charges_reader as saisie

    assert saisie.reservation_id_exists("70001") is False


def test_prerequis_calculs_porte_sur_le_dataset(tmp_db):
    """Un classeur laissé par un calcul précédent répondait « oui » à tort."""
    from app.services import calculs_executeur_service as ex

    assert ex._dataset_disponible(ds.ETAPE_RESOLUES) is False
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001")])
    assert ex._dataset_disponible(ds.ETAPE_RESOLUES) is True


def test_lot4quater_declare_un_dataset_pas_un_fichier(tmp_db):
    from app.services import calculs_executeur_service as ex

    lot = ex.TOUS_LES_LOTS["lot4quater"]
    assert lot.dataset == "RESOLUES"
    assert lot.sorties == (), "aucun fichier ne doit conditionner le succès d'un lot migré"


# ── 7. Adaptateur workspace pour Lot 11 ─────────────────────────────────────────────────────────

def test_adaptateur_fabrique_les_classeurs_du_moteur(tmp_db, tmp_path):
    from app.services import reservations_adaptateur_moteur as adaptateur

    fx.extraire(tmp_db, reservations=[fx.reservation("70001", check_in="2026-07-01")],
                payouts=[fx.payout("70001")])
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001", reservation_id="70001")])

    ws = tmp_path / "workspace_run"
    res = adaptateur.ecrire_tout(ws, db_path=tmp_db)
    assert res["ok"]
    assert (ws / adaptateur.RESOLUES_REL).exists()
    assert (ws / adaptateur.PAYOUT_REL).exists()
    # Jetable : rien n'est écrit dans l'arbre du projet.
    assert not Path(cfg.MASTER_CALC_RESERVATIONS_RESOLUES).exists()

    import openpyxl
    wb = openpyxl.load_workbook(ws / adaptateur.RESOLUES_REL, read_only=True)
    try:
        assert adaptateur.ONGLET_RESOLUES in wb.sheetnames
        assert adaptateur.ONGLET_VUE_FLUX in wb.sheetnames
    finally:
        wb.close()


def test_adaptateur_refuse_de_fabriquer_un_classeur_vide(tmp_db, tmp_path):
    """Un classeur vide ferait conclure au moteur « aucune anomalie », en code retour 0."""
    from app.services import reservations_adaptateur_moteur as adaptateur

    res = adaptateur.ecrire_tout(tmp_path / "ws", db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == adaptateur.E_AUCUN_DATASET
    assert not (tmp_path / "ws" / adaptateur.RESOLUES_REL).exists()


# ── 8. Historique des mois clos ─────────────────────────────────────────────────────────────────

def _archiver(db_path, mois: str, cle: str, montant: float, archive: str = "ARC-TEST") -> None:
    from app.db.connection import get_db

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO reservations_historique_cloture (archive_id, cle_historisation, "
            "reservation_calc_id, mois, montant_retenu, guest_count) VALUES (?,?,?,?,?,2)",
            (archive, cle, f"RES-{mois}-HA-001", mois, montant))
        conn.commit()
    finally:
        conn.close()


def test_un_mois_clos_ne_se_reecrit_pas(tmp_db):
    """La garantie est dans le schéma, pas dans le code appelant."""
    _archiver(tmp_db, "2025-01", "50009", 300.0)
    _archiver(tmp_db, "2025-01", "50009", 999.0, archive="ARC-SECOND")

    lignes = ds.historique(mois="2025-01", db_path=tmp_db)
    assert len(lignes) == 1
    assert lignes[0]["montant_retenu"] == 300.0, "la première valeur figée fait foi"


def test_historique_nappartient_a_aucun_dataset(tmp_db):
    """Un recalcul produit un nouveau dataset et ne touche pas les mois clos."""
    _archiver(tmp_db, "2025-01", "50009", 300.0)
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001")])
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001", montant=999.0)])
    assert ds.historique(db_path=tmp_db)[0]["montant_retenu"] == 300.0
    assert ds.mois_historises(db_path=tmp_db) == ["2025-01"]


# ── 9. Actualisation : service unique ───────────────────────────────────────────────────────────

def test_le_service_est_appelable_sans_contexte_http(tmp_db):
    """C'est ce qui permettra à un ordonnanceur d'emprunter exactement ce chemin."""
    import inspect

    from app.services import hostaway_actualisation_service as act

    parametres = inspect.signature(act.actualiser).parameters
    assert "request" not in parametres
    assert set(parametres) >= {"declencheur", "db_path"}


def test_actualisation_refusee_si_une_autre_est_en_cours(tmp_db):
    """Deux extractions simultanées écriraient deux extractions concurrentes."""
    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as act

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur) "
            "VALUES ('RUN-EN-COURS','lot1_hostaway_extract','2026-08-17T10:00:00Z',"
            "'EN_COURS','MANUEL')")
        conn.commit()
    finally:
        conn.close()

    res = act.actualiser(db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == act.E_DEJA_EN_COURS


def test_etat_distingue_partiel_de_succes(tmp_db):
    """Résumer un run partiel en « à jour » laisserait croire que tout a été rafraîchi."""
    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as act

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, ended_at, statut, declencheur, "
            "nb_etapes, nb_etapes_ok, nb_etapes_ko) VALUES ('RUN-P','lot1_hostaway_extract',"
            "'2026-08-17T10:00:00Z','2026-08-17T10:05:00Z','PARTIEL','MANUEL',3,2,1)")
        conn.executemany(
            "INSERT INTO moteur_run_etapes (run_id, etape, ordre, started_at, statut, nb_ecrits) "
            "VALUES (?,?,?,?,?,?)",
            [("RUN-P", "RESERVATIONS", 1, "2026-08-17T10:00:00Z", "SUCCES", 12),
             ("RUN-P", "PAYOUTS", 2, "2026-08-17T10:01:00Z", "SUCCES", 11),
             ("RUN-P", "CLEANING_TASKS", 3, "2026-08-17T10:02:00Z", "ECHEC", None)])
        conn.commit()
    finally:
        conn.close()

    etat = act.etat(db_path=tmp_db)
    assert etat["partiel"] is True
    assert etat["complet"] is False
    assert etat["en_cours"] is False
    echouees = [e["etape"] for e in etat["etapes"] if e["statut"] == "ECHEC"]
    assert echouees == ["CLEANING_TASKS"], "l'étape en échec doit rester nommée"


def test_ecran_actualisation_repond_sans_donnees(tmp_db, client):
    r = client.get("/hostaway")
    assert r.status_code == 200
    assert "bouton-actualiser" in r.text


def test_ecran_actualisation_affiche_la_fraicheur(tmp_db, client):
    fx.jeu_minimal(tmp_db, nb=2)
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [_ligne("RES-2026-07-HA-001")])
    r = client.get("/hostaway")
    assert r.status_code == 200
    assert "fraicheur-donnees" in r.text
    assert "2 réservation" in r.text


def test_ecran_actualisation_nexpose_aucun_chemin(tmp_db, client):
    fx.jeu_minimal(tmp_db, nb=1)
    texte = client.get("/hostaway").text
    for interdit in ("C:\\", "OneDrive", ".xlsx"):
        assert interdit not in texte, f"l'écran expose {interdit}"


# ── 10. Ordre de lecture : la clé du moteur en dépend ───────────────────────────────────────────
#
# `reservation_calc_id` vaut `RES-<mois>-HA-<n>`, où n est un compteur d'itération : la clé dépend de
# l'ORDRE dans lequel les réservations sont parcourues. C'est une faiblesse du modèle de clé — une
# clé stable devrait dériver de l'identifiant de la réservation, pas de sa position — et elle reste
# ouverte côté moteur.
#
# Ce que la migration DOIT garantir, et que ces tests vérifient : la lecture SQLite rend toujours le
# même ordre, donc deux exécutions produisent les mêmes clés. Sans cela, un simple recalcul
# renumérotait chaque ligne sans changer un seul total : les agrégats restaient justes et toutes les
# clés étaient décalées d'un cran.

def test_ordre_de_lecture_stable(tmp_db):
    """Deux lectures successives rendent les réservations dans le même ordre."""
    fx.extraire(tmp_db, reservations=[
        fx.reservation("70003", check_in="2026-07-03"),
        fx.reservation("70001", check_in="2026-07-01"),
        fx.reservation("70002", check_in="2026-07-02")])

    premiere = [r["reservation_id"] for r in raw.reservations(db_path=tmp_db)]
    seconde = [r["reservation_id"] for r in raw.reservations(db_path=tmp_db)]
    assert premiere == seconde


def test_ordre_de_lecture_suit_l_arrivee_pas_l_identifiant(tmp_db):
    """L'ordre rendu est celui de l'extraction, pas celui des identifiants.

    Trier par identifiant décalerait toutes les clés calculées sans changer un seul montant.
    """
    fx.extraire(tmp_db, reservations=[
        fx.reservation("70003", check_in="2026-07-03"),
        fx.reservation("70001", check_in="2026-07-01"),
        fx.reservation("70002", check_in="2026-07-02")])

    assert [r["reservation_id"] for r in raw.reservations(db_path=tmp_db)] == \
        ["70003", "70001", "70002"]


def test_ordre_des_lignes_de_dataset_stable(tmp_db):
    _dataset(tmp_db, ds.ETAPE_RESOLUES, [
        _ligne("RES-2026-07-HA-003"), _ligne("RES-2026-07-HA-001"),
        _ligne("RES-2026-07-HA-002")])
    attendu = ["RES-2026-07-HA-003", "RES-2026-07-HA-001", "RES-2026-07-HA-002"]
    assert [r["reservation_calc_id"] for r in ds.lignes(db_path=tmp_db)] == attendu
    assert [r["reservation_calc_id"] for r in ds.lignes(db_path=tmp_db)] == attendu

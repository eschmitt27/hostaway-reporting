"""Fabrique de données Hostaway en SQLite, pour les tests.

Remplace les masters `MASTER_*_HA_*.xlsx` que les tests devaient fabriquer. Les identifiants sont
FOURNIS par l'appelant : un test qui affirme quelque chose sur une réservation doit pouvoir la nommer.

Insertion par le service RAW, pas en SQL direct — contrairement à la fabrique Banque. La raison
diffère : ici, le chemin d'écriture EST ce qu'on veut exercer, puisque l'extraction API l'emprunte.
Le contourner ferait passer les tests avec un service cassé.

Aucune donnée réelle : identifiants inventés, pas de nom de voyageur, pas de montant repris.
"""
from __future__ import annotations

import json
from typing import Any

from app.services import hostaway_raw_service as raw

COMPTE_LISTING = "480001"
LOGEMENT = "LOG_0001"


def listing(listing_map_id: str = COMPTE_LISTING, *, nom: str = "Logement de demonstration",
            actif: str = "OUI") -> dict[str, Any]:
    return {
        "listingMapId": listing_map_id,
        "listing_id_ha": f"HA-{listing_map_id}",
        "nom_listing": nom,
        "internalName": nom,
        "ville": "VILLE_DEMO",
        "actif": actif,
        "special_status": "",
        "airbnb_status": "",
        "bookingcom_status": "",
        "sur_hostaway": "OUI",
        "extrait_le": "2026-08-17",
        "ROW_HASH": f"H-{listing_map_id}",
    }


def reservation(reservation_id: str, *, check_in: str, check_out: str = "",
                listing_map_id: str = COMPTE_LISTING, source: str = "airbnbOfficial",
                status: str = "new", nights: int = 2, guests: int = 2,
                total_price: float = 250.0, cleaning_fee: float = 55.0,
                payload_guests: int | None = None, payload_bourrage: int = 0,
                is_owner_stay: str = "NON") -> dict[str, Any]:
    """Une réservation telle que l'API la rend.

    `payload_guests` place un nombre de voyageurs dans le payload brut, et `payload_bourrage`
    l'entoure de texte : c'est ce qui permet de reproduire un payload TRONQUÉ, seul cas où le repli
    historique de `guestCount` a une raison d'exister.
    """
    charge: dict[str, Any] = {"id": reservation_id, "listingMapId": listing_map_id,
                              "channelName": source, "status": status}
    if payload_guests is not None:
        charge["numberOfGuests"] = payload_guests
    if payload_bourrage:
        charge["commentaire"] = "x" * payload_bourrage
    texte = json.dumps(charge, ensure_ascii=False)

    return {
        "reservation_id": reservation_id,
        "listingMapId": listing_map_id,
        "source": source,
        "channel_type": "OTA",
        "source_financiere": "HOSTAWAY",
        "status": status,
        "paymentStatus": "paid",
        "checkInDate": check_in,
        "checkOutDate": check_out or check_in,
        "nights": nights,
        "numberOfGuests": guests,
        "guestCount": guests,
        "source_guestCount": "API_LIST",
        "controle_guestCount": "OK",
        "code_controle_guestCount": "",
        "totalPrice": total_price,
        "cleaningFee_res": cleaning_fee,
        "channelCommission": 0.0,
        "airbnbExpectedPayout": total_price - cleaning_fee,
        "is_ownerStay": is_owner_stay,
        "inclure_resultat": "OUI",
        "updatedOn": "2026-08-17T00:00:00Z",
        "createdOn": "2026-01-01T00:00:00Z",
        "extrait_le": "2026-08-17",
        "ROW_HASH": f"H-{reservation_id}",
        "json_snapshot": texte,
    }


def payout(reservation_id: str, *, montant: float = 195.0, menage: float = 55.0,
           listing_map_id: str = COMPTE_LISTING,
           statut: str = "PAYOUT_HOSTAWAY") -> dict[str, Any]:
    return {
        "reservation_id": reservation_id,
        "listingMapId": listing_map_id,
        "source": "airbnbOfficial",
        "channel_type": "OTA",
        "statut_calcul_payout": statut,
        "payout_calcule": montant,
        "source_payout": "HOSTAWAY_PAYOUT",
        "menage_retenu": menage,
        "assiette_commission": montant - menage,
        "menage_retenu_source": "REF_COUT_STANDARD_MENAGE",
        "cout_standard_id": "CSM_0001",
        "cout_standard_menage_snapshot": menage,
        "cout_standard_date_debut_validite": "2025-01-01",
        "cout_standard_date_fin_validite": "",
        "logement_id_snapshot": LOGEMENT,
        "type_logement_id_snapshot": "TYPE_0001",
        "date_reference_cout_menage": "2026-01-01",
        "inclure_resultat_auto": "OUI",
        "extrait_le": "2026-08-17",
        "ROW_HASH": f"P-{reservation_id}",
    }


def fee(reservation_id: str, *, nom: str = "cleaningFee", montant: float = 55.0) -> dict[str, Any]:
    return {"reservation_id": reservation_id, "fee_id": f"F-{reservation_id}", "fee_name": nom,
            "fee_type": "FIXE", "amount": montant, "currency": "EUR",
            "ROW_HASH": f"F-{reservation_id}"}


def finance_field(reservation_id: str, *, nom: str = "totalPrice",
                  valeur: str = "250.0") -> dict[str, Any]:
    return {"reservation_id": reservation_id, "financeField_name": nom,
            "financeField_value": valeur, "currency": "EUR", "ROW_HASH": f"FF-{reservation_id}"}


def anomalie(reservation_id: str, *, code: str = "PAYOUT_INCOMPLET",
             severite: str = "A_CONTROLER") -> dict[str, Any]:
    return {"reservation_id": reservation_id, "code_anomalie": code, "severite": severite,
            "description": "Anomalie de demonstration", "statut": "OUVERTE",
            "date_detection": "2026-08-17", "ROW_HASH": f"A-{reservation_id}"}


def extraire(db_path, *, listings=(), reservations=(), payouts=(), fees=(), finance_fields=(),
             anomalies=(), mode: str = raw.MODE_API, statut: str = raw.ST_SUCCES,
             run_id: str = "RUN-TEST-HA") -> str:
    """Simule une extraction complète : ouverture, écriture, clôture.

    Emprunte le chemin réel du service — c'est celui que l'extraction API utilise, et le contourner
    ferait passer les tests avec un service cassé.
    """
    extraction_id = raw.ouvrir(mode=mode, run_id=run_id, db_path=db_path)
    raw.enregistrer(extraction_id, db_path=db_path, listings=listings, reservations=reservations,
                    payouts=payouts, fees=fees, finance_fields=finance_fields,
                    anomalies=anomalies)
    raw.cloturer(extraction_id, statut=statut, db_path=db_path)
    return extraction_id


def jeu_minimal(db_path, *, nb: int = 3, mois: str = "2026-07", **kw) -> str:
    """Une extraction complète et cohérente : `nb` réservations, leurs payouts, un listing."""
    reservations = [
        reservation(f"9000{i}", check_in=f"{mois}-0{i}", check_out=f"{mois}-0{i + 1}",
                    total_price=200.0 + i * 10, payload_guests=2)
        for i in range(1, nb + 1)
    ]
    payouts = [payout(r["reservation_id"], montant=150.0 + i * 10)
               for i, r in enumerate(reservations, start=1)]
    return extraire(db_path, listings=[listing()], reservations=reservations, payouts=payouts,
                    fees=[fee(r["reservation_id"]) for r in reservations],
                    finance_fields=[finance_field(r["reservation_id"]) for r in reservations],
                    **kw)


def peupler_reservations(db_path, lignes=None, *, etape="RESOLUES", dataset_id=None):
    """Ecrit un dataset de reservations et le rend courant.

    Les ecrans de controle lisent les reservations en base : sans dataset, ils n'ouvrent aucun
    element et un test conclurait a tort que le controle a disparu.
    """
    import uuid

    from app.db.connection import get_db
    from app.services import reservations_dataset_service as ds

    dataset_id = dataset_id or "RDS-" + uuid.uuid4().hex[:12].upper()
    table = "reservations_calculees" if etape == ds.ETAPE_CALCULEES else "reservations_resolues"
    colonnes = (ds.COLONNES_BASE if etape == ds.ETAPE_CALCULEES
                else ds.COLONNES_BASE + ds.COLONNES_RESOLUTION)
    lignes = list(lignes if lignes is not None else [ligne_reservation("RES-2026-07-HA-001")])

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE reservations_datasets SET actif = 0 WHERE etape = ?", (etape,))
        conn.execute(
            "INSERT INTO reservations_datasets (dataset_id, etape, nb_lignes, actif) "
            "VALUES (?,?,?,1)", (dataset_id, etape, len(lignes)))
        trous = ", ".join(["?"] * (len(colonnes) + 1))
        conn.executemany(
            f"INSERT INTO {table} (dataset_id, {', '.join(colonnes)}) VALUES ({trous})",
            [(dataset_id, *(l.get(c) for c in colonnes)) for l in lignes])
        conn.commit()
    finally:
        conn.close()
    return dataset_id


def ligne_reservation(calc_id, *, mois="2026-07", source="HOSTAWAY_AIRBNB", reservation_id="",
                      montant=200.0, guests=2, statut="VALIDE", **extra):
    """Une ligne de dataset de reservations, tous champs remplis."""
    base = {
        "reservation_calc_id": calc_id, "row_hash": f"H-{calc_id}", "source": source,
        "reservation_id_hostaway": reservation_id, "reservation_hh_id": "", "mois": mois,
        "logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
        "date_arrivee": f"{mois}-05", "date_depart": f"{mois}-07", "nuits": 2,
        "guest_count": guests, "source_guest_count": "API_LIST", "montant_retenu": montant,
        "source_montant": "HOSTAWAY_PAYOUT", "code_impact": "IC",
        "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI",
        "statut_controle": statut, "niveau_anomalie": "", "code_anomalie": "",
        "commentaire": "", "source_module": "LOT1", "source_table": "hostaway_reservations",
        "source_pk": reservation_id, "date_integration": "2026-08-18", "canal": "AIRBNB",
        "etat_mois": "OUVERT", "origine_initiale": "API_HOSTAWAY", "source_ligne": source,
        "methode": "LIVE", "payout_calcule": montant - 50, "menage_retenu": 50.0,
        "assiette_commission": montant - 100,
    }
    base.update(extra)
    return base

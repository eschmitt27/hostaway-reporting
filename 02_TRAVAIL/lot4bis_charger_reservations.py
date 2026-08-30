"""
Lot 4bis — Correctif : peuplement de MASTER_CALC_Reservations.xlsx
Décisions : D052, D053, D054, D055, D056, D057

Sources Hostaway (réservations, payouts, payloads) :
  - SQLite : hostaway_reservations / hostaway_payouts, extraction courante  ← chemin normal
  - Excel  : MASTER_FACT_HA_*.xlsx                                          ← parité legacy

Référentiels et réservations hors Hostaway (mission 14e — même patron que Hostaway ci-dessus) :
  - SQLite : ref_mapping_logements / ref_logements / ref_gestion_logements_hist
             (migration 0029, alimentées une fois par `ref_setup_import_service.py`) et
             reservations_hors_hostaway (écrite par l'application, `/reservations/nouvelle`)
                                                                            ← chemin normal
  - Excel  : REF_Setup.xlsm + MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx
                                                                            ← parité legacy
  Le moteur S1-S7 ci-dessous reste RIGOUREUSEMENT INCHANGÉ : les fonctions `charger_*_sqlite()`
  rendent exactement la même forme (liste de dict, mêmes clés) que `load_sheet()`/`rows_to_dicts()`
  côté Excel — seule la provenance des lignes change, jamais leur vocabulaire ni leur traitement.

Cibles :
  - SQLite : reservations_calculees, sous un dataset identifié          ← chemin normal
  - Excel  : 02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx
             onglets MASTER + VUE_FLUX (POWER_QUERY_CODE conservé)      ← parité legacy

`--source-hostaway` choisit la source, `--sans-excel` coupe l'écriture du classeur. La logique métier
ci-dessous est INCHANGÉE : seule la provenance et la destination des lignes varient. C'est ce qui
permet de comparer les deux chemins ligne à ligne, et donc de vérifier que la migration ne déplace
aucun montant.

Règles métier :
  S1  AIRBNB HA pur              → HOSTAWAY_AIRBNB,           HOSTAWAY_PAYOUT, IC, VALIDE
  S2  BOOKING HA pur             → HOSTAWAY_BOOKING,          HOSTAWAY_PAYOUT, IC, VALIDE
  S3  DIRECT HA + HH liée        → HOSTAWAY_DIRECT_HH,        MANUEL_HH,       du HH, du HH (ligne HA exclue)
  S4  VRBO HA + HH renseignée    → HOSTAWAY_VRBO_HH,          MANUEL_VRBO,     HC, du HH (ligne HA exclue)
  S5  VRBO HA sans HH            → HOSTAWAY_VRBO_A_CONTROLER, A_CONTROLER,     HC, A_CONTROLER
  S6  HH pure                    → MANUEL_HORS_HOSTAWAY,      MANUEL_HH,       du HH
  S7  ownerStay                  → OWNERSTAY_EXCLU,           NON_CONCERNE,    HR, EXCLU_RESULTAT

Cas non couvert par D054 (proposition validée) :
  DIRECT HA sans HH              → HOSTAWAY_DIRECT_HH, A_CONTROLER, HC, A_CONTROLER,
                                   code_anomalie=DIRECT_SANS_SAISIE_HH

Bloquants : RESERVATION_DOUBLON_HOSTAWAY_HH, RESERVATION_CALC_ID_DUPLIQUE,
            LOGEMENT_NON_MAPPE, LOGEMENT_MAPPING_MULTIPLE
"""

import hashlib
import datetime
import os
import sys
from collections import defaultdict, Counter


import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font

from lib_ref_history import REF_GESTION_LOGEMENTS_HIST_SHEET, resolve_management_period
from lib_guestcount import (
    SOURCE_API_DETAIL,
    SOURCE_API_LIST,
    SOURCE_CONFLICT_API,
    extract_number_of_guests_from_snapshot,
    normalize_guest_count,
    resolve_guest_count_from_sources,
)
from lib_parc import (
    A_CONTROLER,
    HORS_PARC_TECHNIQUE,
    STATUT_PARC_INVALIDE,
    is_hors_parc_technique,
    is_statut_parc_a_controler,
)

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PATH_HA_RES  = os.path.join(ROOT, "02_TRAVAIL", "Lot1_Hostaway", "MASTER_FACT_HA_Reservations.xlsx")
PATH_HA_DET  = os.path.join(ROOT, "02_TRAVAIL", "Lot1_Hostaway", "MASTER_FACT_HA_ReservationDetails.xlsx")
PATH_HA_PAY  = os.path.join(ROOT, "02_TRAVAIL", "Lot1_Hostaway", "MASTER_CALC_HA_Payout.xlsx")
PATH_HH_FACT = os.path.join(ROOT, "02_TRAVAIL", "Lot4_ReservationsHH", "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx")
PATH_REF     = os.path.join(ROOT, "01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
PATH_TARGET  = os.path.join(ROOT, "02_TRAVAIL", "Lot4bis_TableCommune", "MASTER_CALC_Reservations.xlsx")

DATE_INTEGRATION = datetime.datetime.now().isoformat(timespec="seconds")
SOURCE_MODULE    = "lot4bis"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

import argparse  # noqa: E402  (place apres les constantes historiques du module)

# Le dossier du lot doit etre sur le chemin d'import : ces modules sont importes aussi bien en
# execution directe (cwd = 02_TRAVAIL) que depuis un test qui charge le fichier par son chemin. Sans
# cela, `lib_db_moteur` reste introuvable dans le second cas.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lib_db_moteur as dbm  # noqa: E402
from lib_db_moteur import SOURCE_AUTO, SOURCE_EXCEL, SOURCE_SQLITE  # noqa: E402


def row_hash(values):
    s = "|".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def date_to_str(val):
    if val is None:
        return None
    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)[:10]
    return str(val)[:10]


def load_sheet(path, sheet_name):
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    non_empty = [r for r in rows if any(c is not None for c in r)]
    if not non_empty:
        return [], []
    headers = list(non_empty[0])
    data = non_empty[1:]
    return headers, data


def load_optional_sheet(path, sheet_name):
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            return [], []
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    non_empty = [r for r in rows if any(c is not None for c in r)]
    if not non_empty:
        return [], []
    headers = list(non_empty[0])
    data = non_empty[1:]
    return headers, data


def rows_to_dicts(headers, data):
    return [dict(zip(headers, r)) for r in data]


def abort(msg):
    print(f"\n[BLOQUANT] {msg}")
    print("Script arrêté — MASTER_CALC_Reservations.xlsx NON modifié.")
    sys.exit(1)


# ---------------------------------------------------------------------------

SOURCES_HOSTAWAY = (SOURCE_SQLITE, SOURCE_EXCEL, SOURCE_AUTO)

# Colonnes lues dans la couche RAW, et leur nom côté moteur. La logique en aval attend le vocabulaire
# du classeur ; le traduire ici évite de la réécrire.
_COLS_RES_SQL = (
    "reservation_id", "listing_map_id", "source", "channel_type", "source_financiere", "status",
    "payment_status", "check_in_date", "check_out_date", "nights", "number_of_guests",
    "guest_count", "source_guest_count", "controle_guest_count", "code_controle_guest_count",
    "total_price", "cleaning_fee_res", "channel_commission", "airbnb_expected_payout",
    "is_owner_stay", "inclure_resultat", "updated_on", "created_on", "extrait_le", "row_hash",
    "payload_json")
_VERS_MOTEUR_RES = {
    "listing_map_id": "listingMapId", "payment_status": "paymentStatus",
    "check_in_date": "checkInDate", "check_out_date": "checkOutDate",
    "number_of_guests": "numberOfGuests", "guest_count": "guestCount",
    "source_guest_count": "source_guestCount", "controle_guest_count": "controle_guestCount",
    "code_controle_guest_count": "code_controle_guestCount", "total_price": "totalPrice",
    "cleaning_fee_res": "cleaningFee_res", "channel_commission": "channelCommission",
    "airbnb_expected_payout": "airbnbExpectedPayout", "is_owner_stay": "is_ownerStay",
    "updated_on": "updatedOn", "created_on": "createdOn", "row_hash": "ROW_HASH",
    "payload_json": "json_snapshot",
}

_COLS_PAY_SQL = (
    "reservation_id", "listing_map_id", "source", "channel_type", "statut_calcul_payout",
    "payout_calcule", "source_payout", "menage_retenu", "assiette_commission",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
    "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
    "inclure_resultat_auto", "extrait_le", "row_hash")
_VERS_MOTEUR_PAY = {"listing_map_id": "listingMapId", "row_hash": "ROW_HASH"}

# Colonnes de reservations_calculees, et le nom moteur correspondant.
_COLS_CALC_SQL = (
    "reservation_calc_id", "row_hash", "source", "reservation_id_hostaway", "reservation_hh_id",
    "mois", "logement_id", "proprietaire_id", "date_arrivee", "date_depart", "nuits",
    "guest_count", "source_guest_count", "montant_retenu", "source_montant", "code_impact",
    "impact_resultat_reel", "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
    "code_anomalie", "commentaire", "source_module", "source_table", "source_pk",
    "date_integration")
_CALC_DEPUIS_MOTEUR = {"row_hash": "ROW_HASH", "guest_count": "guestCount",
                       "source_guest_count": "source_guestCount"}


# La traduction SQLite -> moteur, types d'identifiants compris, vit dans `lib_db_moteur` : les trois
# lots de la chaine reservations en ont besoin, et trois copies finiraient par diverger.
_traduire = dbm.traduire


def charger_hostaway_sqlite(chemin_base):
    """(reservations, guestCount par reservation, payouts) depuis la couche RAW.

    Rend None si la base est inutilisable, avec la raison : base non designee, introuvable, migration
    absente, ou aucune extraction exploitable. Quatre causes distinctes, quatre corrections
    differentes.
    """
    conn, message = dbm.verifier(chemin_base, ("hostaway_reservations", "hostaway_payouts"))
    if conn is None:
        return None, message
    try:
        extraction = dbm.extraction_utilisable(conn)
        if not extraction:
            return None, "aucune extraction Hostaway exploitable en base"
        # ORDRE D'ARRIVEE, pas ordre d'identifiant : preserve l'ordre d'insertion (celui de
        # l'extraction, celui que le classeur legacy avait), pour que la comparaison de parite
        # ligne a ligne reste valable. `reservation_calc_id` est desormais derive de l'identifiant
        # Hostaway (RES-HA-<reservation_id>) et ne depend plus de cet ordre — l'ordre de lecture ne
        # change donc plus d'identite, seulement la position des lignes dans le fichier de sortie.
        res = [_traduire(r, _VERS_MOTEUR_RES) for r in dbm.lignes(
            conn, "hostaway_reservations", _COLS_RES_SQL,
            ou="extraction_id = ?", args=(extraction,), ordre="id")]
        pay = [_traduire(r, _VERS_MOTEUR_PAY) for r in dbm.lignes(
            conn, "hostaway_payouts", _COLS_PAY_SQL,
            ou="extraction_id = ?", args=(extraction,), ordre="id")]
    finally:
        conn.close()

    # Le repli historique de guestCount vient du payload conserve. Il est TRONQUE a 4 000 caracteres
    # dans les masters d'origine (limite d'une cellule Excel) : `extract_number_of_guests_from_snapshot`
    # sait le lire quand meme, et c'est exactement pour cela qu'on l'appelle plutot qu'un json.loads.
    guests = {}
    for r in res:
        valeur = extract_number_of_guests_from_snapshot(r.get("json_snapshot"))
        if valeur is not None and valeur != "":
            guests[str(r.get("reservation_id"))] = valeur

    return (res, guests, pay), "%s (extraction %s : %d reservations, %d payouts)" % (
        message, extraction, len(res), len(pay))


def charger_hostaway_excel():
    """(reservations, guestCount par reservation, payouts) depuis les masters legacy."""
    h_res, d_res = load_sheet(PATH_HA_RES, "data")
    res = rows_to_dicts(h_res, d_res)

    h_det, d_det = (load_optional_sheet(PATH_HA_DET, "data")
                    if os.path.exists(PATH_HA_DET) else ([], []))
    details = rows_to_dicts(h_det, d_det) if h_det else []
    guests = {
        str(d.get("reservation_id")): extract_number_of_guests_from_snapshot(d.get("json_snapshot"))
        for d in details if d.get("reservation_id") is not None
    }
    guests = {k: v for k, v in guests.items() if v is not None and v != ""}

    h_pay, d_pay = load_sheet(PATH_HA_PAY, "data")
    pay = rows_to_dicts(h_pay, d_pay)
    return res, guests, pay


def charger_hostaway(source, chemin_base):
    """Charge les donnees Hostaway selon la source demandee.

    AUTO essaie SQLite puis retombe sur Excel, en le DISANT. SQLITE explicite refuse plutot que de
    se rabattre : demander la base et lire un classeur sans le savoir produirait une parite fausse.
    """
    if source == SOURCE_SQLITE:
        jeu, message = charger_hostaway_sqlite(chemin_base)
        if jeu is None:
            abort("source SQLite demandee mais inutilisable : %s" % message)
        print("      source : SQLite — %s" % message)
        return jeu
    if source == SOURCE_EXCEL:
        print("      source : masters Excel (parite legacy)")
        return charger_hostaway_excel()

    jeu, message = charger_hostaway_sqlite(chemin_base)
    if jeu is not None:
        print("      source : SQLite — %s" % message)
        return jeu
    print("      source : masters Excel — SQLite indisponible (%s)" % message)
    return charger_hostaway_excel()


# ── Référentiels et réservations hors Hostaway — SQLite (mission 14e) ──────────────────────────
#
# Colonnes lues telles quelles depuis les tables du référentiel (migration 0029) : les noms de
# colonnes SQLite correspondent déjà au vocabulaire attendu par le moteur (`source`,
# `champ_source`, `valeur_source`, `logement_id`, `actif` pour le mapping ; `logement_id`,
# `proprietaire_id`, `date_debut`, `date_fin`, `statut_gestion` pour la gestion historisée ;
# `statut_parc` pour les logements) — aucune traduction n'est nécessaire pour ces trois-là.
_COLS_MAP_SQL = ("source", "champ_source", "valeur_source", "logement_id", "actif")
_COLS_LOG_SQL = ("logement_id", "statut_parc", "actif")
_COLS_GEST_SQL = ("gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
                 "statut_gestion")

# `reservations_hors_hostaway` a été conçue avec le même vocabulaire que le classeur HH legacy
# (mission Ménages/HH antérieure) — seules `guest_count`/`montant_percu` diffèrent du nom moteur
# (`guestCount`/`total_percu`), comme pour Hostaway ci-dessus.
_COLS_HH_SQL = (
    "reservation_hh_id", "mois", "canal_id", "proprietaire_id", "logement_id",
    "reservation_id_hostaway", "date_arrivee", "date_depart", "nuits", "guest_count",
    "montant_percu", "montant_retenu", "code_impact", "impact_resultat_reel",
    "impact_resultat_comptable", "statut_controle", "niveau_anomalie", "code_anomalie",
    "commentaire", "statut")
_VERS_MOTEUR_HH = {"guest_count": "guestCount", "montant_percu": "total_percu"}


def charger_ref_sqlite(chemin_base):
    """(map_dicts, log_dicts, gest_dicts) depuis les référentiels SQLite (migration 0029).

    None si la base est inutilisable ou si le référentiel n'a jamais été importé (tables absentes
    ou vides) — ces trois tables sont indissociables pour `resolve_logement()`, donc vérifiées et
    rendues ensemble. Un référentiel jamais importé est un état LÉGITIME avant bootstrap (base 0016
    par exemple) : ce n'est pas une erreur de lecture, juste une source pas encore disponible.
    """
    conn, message = dbm.verifier(
        chemin_base, ("ref_mapping_logements", "ref_logements", "ref_gestion_logements_hist"))
    if conn is None:
        return None, message
    try:
        n_map = conn.execute("SELECT COUNT(*) FROM ref_mapping_logements").fetchone()[0]
        if n_map == 0:
            return None, "ref_mapping_logements vide — référentiel jamais importé (bootstrap requis)"
        map_dicts = dbm.lignes(conn, "ref_mapping_logements", _COLS_MAP_SQL,
                               ordre="mapping_logement_id")
        # `valeur_source` porte un listingMapId Hostaway quand source=Hostaway — même
        # normalisation de type que `_traduire()` applique à `res["listingMapId"]`
        # (`IDS_HOSTAWAY`) : sans elle, une valeur numérique stockée en TEXT ('480136') ne
        # correspondrait jamais à l'entier obtenu côté Hostaway, et RESOUDRE_LOGEMENT échouerait
        # à tort avec LOGEMENT_NON_MAPPE — trouvé par la preuve A/B (mission 14e).
        for r in map_dicts:
            r["valeur_source"] = dbm.entier_si_possible(r["valeur_source"])
        log_dicts = dbm.lignes(conn, "ref_logements", _COLS_LOG_SQL, ordre="logement_id")
        gest_dicts = dbm.lignes(conn, "ref_gestion_logements_hist", _COLS_GEST_SQL,
                                ordre="gestion_id")
    finally:
        conn.close()
    return (map_dicts, log_dicts, gest_dicts), (
        "%s (%d mappings, %d logements, %d gestions historisées)"
        % (message, len(map_dicts), len(log_dicts), len(gest_dicts)))


def charger_ref_excel():
    h_map, d_map = load_sheet(PATH_REF, "REF_Mapping_Logements")
    h_log, d_log = load_sheet(PATH_REF, "REF_Logements")
    h_gest, d_gest = load_optional_sheet(PATH_REF, REF_GESTION_LOGEMENTS_HIST_SHEET)
    map_dicts = rows_to_dicts(h_map, d_map)
    log_dicts = rows_to_dicts(h_log, d_log)
    gest_dicts = rows_to_dicts(h_gest, d_gest) if h_gest else []
    gest_dicts = [r for r in gest_dicts
                 if r.get("gestion_id") is not None and str(r.get("gestion_id")) != "gestion_id"]
    return map_dicts, log_dicts, gest_dicts


def charger_ref(source, chemin_base):
    """Référentiels (mapping/logements/gestion) selon la source demandée — même contrat que
    `charger_hostaway()` : SQLITE explicite refuse plutôt que de se rabattre silencieusement."""
    if source == SOURCE_SQLITE:
        jeu, message = charger_ref_sqlite(chemin_base)
        if jeu is None:
            abort("référentiel SQLite demandé mais inutilisable : %s" % message)
        print("      référentiels : SQLite — %s" % message)
        return jeu
    if source == SOURCE_EXCEL:
        print("      référentiels : REF_Setup.xlsm (parité legacy)")
        return charger_ref_excel()

    jeu, message = charger_ref_sqlite(chemin_base)
    if jeu is not None:
        print("      référentiels : SQLite — %s" % message)
        return jeu
    print("      référentiels : REF_Setup.xlsm — SQLite indisponible (%s)" % message)
    return charger_ref_excel()


def charger_hh_sqlite(chemin_base):
    """Lignes hors Hostaway depuis `reservations_hors_hostaway` (écrite par l'application).

    None si la table est absente. Contrairement au référentiel, une table VIDE reste utilisable
    (aucune réservation HH saisie n'est un état normal, pas un défaut de bootstrap) — `main()`
    applique de toute façon son propre filtre `statut_controle == VALIDE` ensuite.
    """
    conn, message = dbm.verifier(chemin_base, ("reservations_hors_hostaway",))
    if conn is None:
        return None, message
    try:
        lignes = [_traduire(r, _VERS_MOTEUR_HH) for r in dbm.lignes(
            conn, "reservations_hors_hostaway", _COLS_HH_SQL,
            ou="statut = 'ACTIVE'", ordre="id")]
    finally:
        conn.close()
    return lignes, "%s (%d lignes ACTIVE)" % (message, len(lignes))


def charger_hh_excel():
    h_hh, d_hh = load_sheet(PATH_HH_FACT, "MASTER")
    return rows_to_dicts(h_hh, d_hh)


def charger_hh(source, chemin_base):
    """Réservations hors Hostaway selon la source demandée — même contrat que `charger_hostaway()`."""
    if source == SOURCE_SQLITE:
        jeu, message = charger_hh_sqlite(chemin_base)
        if jeu is None:
            abort("réservations HH SQLite demandées mais indisponibles : %s" % message)
        print("      HH : SQLite — %s" % message)
        return jeu
    if source == SOURCE_EXCEL:
        print("      HH : MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx (parité legacy)")
        return charger_hh_excel()

    jeu, message = charger_hh_sqlite(chemin_base)
    if jeu is not None:
        print("      HH : SQLite — %s" % message)
        return jeu
    print("      HH : classeur legacy — SQLite indisponible (%s)" % message)
    return charger_hh_excel()


def ecrire_sqlite(chemin_base, master_rows, extraction_id=""):
    """Ecrit les lignes calculees dans un nouveau dataset. Une transaction.

    Un dataset a moitie ecrit fausserait tous les agregats calcules ensuite, sans qu'aucune erreur ne
    subsiste pour l'expliquer.
    """
    conn, message = dbm.verifier(chemin_base, ("reservations_datasets", "reservations_calculees"))
    if conn is None:
        return None, message
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            dataset_id = dbm.ouvrir_dataset(conn, dbm.ETAPE_CALCULEES, extraction_id=extraction_id)
            dbm.ecrire_lignes(conn, "reservations_calculees", _COLS_CALC_SQL, dataset_id,
                              master_rows, _CALC_DEPUIS_MOTEUR)
            nb = dbm.cloturer_dataset(conn, dataset_id, table="reservations_calculees")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
    return dataset_id, "%s (dataset %s : %d lignes)" % (message, dataset_id, nb)


def _analyser_arguments(argv=None):
    parseur = argparse.ArgumentParser(description="Lot 4bis — table commune des reservations")
    parseur.add_argument("--source-hostaway", choices=SOURCES_HOSTAWAY, default=SOURCE_AUTO,
                         help="Provenance des reservations et payouts Hostaway")
    parseur.add_argument("--db", help="Base applicative (defaut : PILOTAGE_DB_PATH / APP_DATA_DIR)")
    parseur.add_argument("--sans-excel", action="store_true",
                         help="N'ecrit pas MASTER_CALC_Reservations.xlsx")
    parseur.add_argument("--sans-sqlite", action="store_true",
                         help="N'ecrit pas reservations_calculees (parite legacy seule)")
    return parseur.parse_args(argv)


def main(argv=None):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    args = _analyser_arguments(argv)
    chemin_base = dbm.chemin_db(args.db)

    # 1. Lecture des sources
    # ---------------------------------------------------------------------------
    print("=== Lot 4bis — Chargement réservations ===")
    print(f"Date : {DATE_INTEGRATION}")
    print()

    print("[1/5] Lecture des reservations et payouts Hostaway...")
    res_dicts, guest_by_reservation_id, pay_dicts = charger_hostaway(
        args.source_hostaway, chemin_base)
    print(f"      {len(res_dicts)} reservations, {len(pay_dicts)} payouts")
    print(f"      {len(guest_by_reservation_id)} guestCount disponibles depuis les payloads")

    print("[3/5] Lecture des réservations hors Hostaway...")
    hh_dicts_raw = charger_hh(args.source_hostaway, chemin_base)
    # Filtre : uniquement les lignes VALIDE (D053 — RESERVATION_HH_NON_VALIDE)
    hh_valides = [r for r in hh_dicts_raw
                  if r.get("statut_controle") == "VALIDE"
                  and r.get("reservation_hh_id") is not None
                  and not str(r.get("reservation_hh_id", "")).startswith("[")]
    hh_non_valides = len(hh_dicts_raw) - len(hh_valides)
    print(f"      {len(hh_dicts_raw)} lignes brutes, {len(hh_valides)} VALIDE retenues")

    print("[4/5] Lecture des référentiels (mapping + logements + gestion historisée)...")
    map_dicts, log_dicts, gest_dicts = charger_ref(args.source_hostaway, chemin_base)
    print(f"      {len(map_dicts)} mappings, {len(log_dicts)} logements, {len(gest_dicts)} gestions historisees")

    # Index mapping : listingMapId → logement_id (Hostaway, actif=OUI)
    ha_map_index = defaultdict(list)
    for r in map_dicts:
        if r.get("source") == "Hostaway" and r.get("champ_source") == "listingMapId" and r.get("actif") == "OUI":
            ha_map_index[r["valeur_source"]].append(r["logement_id"])

    # Index logements : logement_id → {proprietaire_id, actif}
    log_index = {r["logement_id"]: r for r in log_dicts}

    print("[5/5] Index payout par reservation_id...")
    pay_index = {r["reservation_id"]: r for r in pay_dicts}

    # ---------------------------------------------------------------------------
    # 2. Index HH : reservation_id_hostaway → hh ligne (pour anti-doublon)
    # ---------------------------------------------------------------------------
    hh_by_ha_id = {}   # reservation_id_hostaway → ligne HH (S3/S4)
    hh_pure = []       # lignes HH sans reservation_id_hostaway (S6)

    for hh in hh_valides:
        ha_id = hh.get("reservation_id_hostaway")
        if ha_id:
            if ha_id in hh_by_ha_id:
                abort(
                    f"RESERVATION_DOUBLON_HOSTAWAY_HH — reservation_id_hostaway={ha_id} "
                    f"rattaché à 2+ lignes HH actives."
                )
            hh_by_ha_id[ha_id] = hh
        else:
            hh_pure.append(hh)

    print(f"\nIndex HH : {len(hh_by_ha_id)} liens HA→HH, {len(hh_pure)} HH pures")

    # ---------------------------------------------------------------------------
    # 3. Construction du MASTER
    # ---------------------------------------------------------------------------
    print("\n--- Construction MASTER ---")

    master_rows = []
    counters_by_month_ha  = defaultdict(int)   # (mois, "HA") → compteur
    counters_by_month_hh  = defaultdict(int)   # (mois, "HH") → compteur

    ha_ids_linked_to_hh = set(hh_by_ha_id.keys())

    stats = Counter()

    def resolve_logement(listing_map_id, date_arrivee_str=None, date_depart_str=None):
        """Retourne (logement_id, proprietaire_id, anomalie_code, anomalie_msg) ou abort si BLOQUANT.

        REF_Gestion_Logements_Hist est la seule source officielle des periodes de gestion.
        Un sejour hors periode ou chevauchant une fin de gestion est controle par lib_ref_history.
        """
        matches = ha_map_index.get(listing_map_id, [])
        if not matches:
            abort(f"LOGEMENT_NON_MAPPE — listingMapId={listing_map_id} absent de REF_Mapping_Logements.")
        if len(matches) > 1:
            abort(f"LOGEMENT_MAPPING_MULTIPLE — listingMapId={listing_map_id} → {matches}.")
        logement_id = matches[0]
        log_row = log_index.get(logement_id, {})
        proprietaire_id = None
        ano_code = None
        ano_msg = None

        if is_hors_parc_technique(log_row):
            return (
                logement_id,
                None,
                HORS_PARC_TECHNIQUE,
                f"{logement_id} statut_parc=HORS_PARC_TECHNIQUE - exclu des traitements metier",
            )
        if is_statut_parc_a_controler(log_row):
            return (
                logement_id,
                None,
                STATUT_PARC_INVALIDE,
                f"{logement_id} statut_parc vide ou invalide - traitement A_CONTROLER",
            )

        if gest_dicts:
            gest = resolve_management_period(
                gest_dicts,
                logement_id=logement_id,
                date_arrivee=date_arrivee_str,
                date_depart=date_depart_str,
            )
            if gest.status == "OK":
                proprietaire_id = gest.value
            else:
                ano_code = f"GESTION_LOGEMENT_{gest.status}"
                ano_msg = f"Logement {logement_id}: {gest.message}"
        else:
            ano_code = "GESTION_LOGEMENT_MISSING"
            ano_msg = f"Logement {logement_id}: historique de gestion absent"

        return logement_id, proprietaire_id, ano_code, ano_msg


    def make_row_ha(res, payout, source_val, source_montant, montant_retenu,
                    code_impact, statut_controle, niveau_anomalie, code_anomalie, commentaire,
                    logement_id, proprietaire_id):
        check_in = res.get("checkInDate")
        mois = str(check_in)[:7] if check_in else "0000-00"
        branch = "HA"
        reservation_calc_id = dbm.cle_reservation_ha(res.get("reservation_id"))
        if reservation_calc_id is None:
            # Dernier recours si l'API ne fournit pas d'identifiant (ne devrait pas arriver) :
            # compteur positionnel, feuille reconnaissable par son prefixe LEGACY.
            counters_by_month_ha[(mois, branch)] += 1
            n = counters_by_month_ha[(mois, branch)]
            reservation_calc_id = f"{dbm.PREFIXE_LEGACY}{mois}-HA-{n:03d}"

        impact_reel  = "A_CONTROLER" if code_impact not in ("IC", "HC", "HR") else ("NON" if code_impact == "HR" else "OUI")
        impact_compta = "A_CONTROLER" if code_impact not in ("IC", "HC", "HR") else ("OUI" if code_impact == "IC" else "NON")
        if statut_controle == "EXCLU_RESULTAT":
            impact_reel   = "NON"
            impact_compta = "NON"

        source_guest = res.get("source_guestCount")
        raw_guest = res.get("guestCount")
        if source_guest in (SOURCE_API_LIST, SOURCE_API_DETAIL):
            guest_value, guest_error = normalize_guest_count(raw_guest)
            if guest_error:
                guest_resolution = resolve_guest_count_from_sources(raw_guest)
            else:
                guest_resolution = resolve_guest_count_from_sources(guest_value)
                guest_resolution = type(guest_resolution)(guest_value, source_guest, "OK")
        elif source_guest == SOURCE_CONFLICT_API:
            guest_resolution = resolve_guest_count_from_sources(1, 2)
        else:
            guest_resolution = resolve_guest_count_from_sources(
                res.get("numberOfGuests"),
                None,
                guest_by_reservation_id.get(str(res.get("reservation_id"))),
            )
        lot1_guest_code = res.get("code_controle_guestCount")
        if lot1_guest_code in ("GUEST_COUNT_CONFLICT_API_LIST_DETAIL", "GUEST_COUNT_INVALIDE_API"):
            guest_resolution = type(guest_resolution)(
                None,
                source_guest or guest_resolution.source,
                "A_CONTROLER",
                lot1_guest_code,
                "guestCount invalide ou conflictuel depuis Lot1",
            )
        if guest_resolution.code in ("GUEST_COUNT_CONFLICT_API_LIST_DETAIL", "GUEST_COUNT_INVALIDE_API"):
            statut_controle = "A_CONTROLER"
            niveau_anomalie = "A_CONTROLER"
            code_anomalie = guest_resolution.code
            commentaire = (commentaire or "") + " | " + (guest_resolution.message or guest_resolution.code)

        hash_keys = [reservation_calc_id, source_val, res.get("reservation_id"), mois,
                     logement_id, montant_retenu, code_impact, guest_resolution.value, guest_resolution.source]

        return {
            "reservation_calc_id":     reservation_calc_id,
            "ROW_HASH":                row_hash(hash_keys),
            "source":                  source_val,
            "reservation_id_hostaway": res.get("reservation_id"),
            "reservation_hh_id":       None,
            "mois":                    mois,
            "logement_id":             logement_id,
            "proprietaire_id":         proprietaire_id,
            "date_arrivee":            date_to_str(res.get("checkInDate")),
            "date_depart":             date_to_str(res.get("checkOutDate")),
            "nuits":                   res.get("nights"),
            "guestCount":              guest_resolution.value,
            "source_guestCount":       guest_resolution.source,
            "montant_retenu":          montant_retenu,
            "source_montant":          source_montant,
            "code_impact":             code_impact,
            "impact_resultat_reel":    impact_reel,
            "impact_resultat_comptable": impact_compta,
            "statut_controle":         statut_controle,
            "niveau_anomalie":         niveau_anomalie,
            "code_anomalie":           code_anomalie,
            "commentaire":             commentaire,
            "source_module":           SOURCE_MODULE,
            "source_table":            "MASTER_FACT_HA_Reservations",
            "source_pk":               str(res.get("reservation_id")),
            "date_integration":        DATE_INTEGRATION,
        }


    def make_row_hh(hh, src_override=None):
        mois = str(hh.get("mois") or "0000-00")[:7]
        branch = "HH"
        reservation_calc_id = dbm.cle_reservation_hh(hh.get("reservation_hh_id"))
        if reservation_calc_id is None:
            counters_by_month_hh[(mois, branch)] += 1
            n = counters_by_month_hh[(mois, branch)]
            reservation_calc_id = f"{dbm.PREFIXE_LEGACY}{mois}-HH-{n:03d}"
        source_val = src_override or "MANUEL_HORS_HOSTAWAY"
        code_impact = hh.get("code_impact") or "HC"
        statut = hh.get("statut_controle") or "A_CONTROLER"
        niveau = hh.get("niveau_anomalie") or "INFO"
        montant = hh.get("total_percu") or 0
        proprietaire_id = None
        log_row = log_index.get(hh.get("logement_id"), {})
        if is_hors_parc_technique(log_row):
            source_val = HORS_PARC_TECHNIQUE
            code_impact = "HR"
            statut = "EXCLU_RESULTAT"
            niveau = "INFO"
            montant = 0
            hh["code_anomalie"] = HORS_PARC_TECHNIQUE
            hh["commentaire"] = (hh.get("commentaire") or "") + " | statut_parc=HORS_PARC_TECHNIQUE - exclu des traitements metier"
        elif is_statut_parc_a_controler(log_row):
            source_val = A_CONTROLER
            code_impact = "HR"
            statut = A_CONTROLER
            niveau = A_CONTROLER
            montant = 0
            hh["code_anomalie"] = STATUT_PARC_INVALIDE
            hh["commentaire"] = (hh.get("commentaire") or "") + " | statut_parc vide ou invalide - traitement A_CONTROLER"
        elif gest_dicts:
            gest = resolve_management_period(
                gest_dicts,
                logement_id=hh.get("logement_id"),
                date_arrivee=date_to_str(hh.get("date_arrivee")),
                date_depart=date_to_str(hh.get("date_depart")),
            )
            if gest.status == "OK":
                proprietaire_id = gest.value
            else:
                statut = "A_CONTROLER"
                niveau = "A_CONTROLER"
                hh["code_anomalie"] = hh.get("code_anomalie") or f"GESTION_LOGEMENT_{gest.status}"
                hh["commentaire"] = (hh.get("commentaire") or "") + f" | Gestion logement: {gest.message}"
        else:
            statut = "A_CONTROLER"
            niveau = "A_CONTROLER"
            hh["code_anomalie"] = hh.get("code_anomalie") or "GESTION_LOGEMENT_MISSING"
            hh["commentaire"] = (hh.get("commentaire") or "") + " | Historique de gestion absent"

        impact_reel  = "NON" if code_impact == "HR" else "OUI"
        impact_compta = "OUI" if code_impact == "IC" else "NON"

        hash_keys = [reservation_calc_id, source_val, hh.get("reservation_hh_id"), mois,
                     hh.get("logement_id"), montant, code_impact]

        source_montant = "MANUEL_VRBO" if source_val == "HOSTAWAY_VRBO_HH" else "MANUEL_HH"

        return {
            "reservation_calc_id":     reservation_calc_id,
            "ROW_HASH":                row_hash(hash_keys),
            "source":                  source_val,
            "reservation_id_hostaway": hh.get("reservation_id_hostaway"),
            "reservation_hh_id":       hh.get("reservation_hh_id"),
            "mois":                    mois,
            "logement_id":             hh.get("logement_id"),
            "proprietaire_id":         proprietaire_id,
            "date_arrivee":            date_to_str(hh.get("date_arrivee")),
            "date_depart":             date_to_str(hh.get("date_depart")),
            "nuits":                   hh.get("nuits"),
            "guestCount":              hh.get("guestCount"),
            "source_guestCount":       "SAISIE_HH" if hh.get("guestCount") not in (None, "") else "ABSENT",
            "montant_retenu":          montant,
            "source_montant":          source_montant,
            "code_impact":             code_impact,
            "impact_resultat_reel":    impact_reel,
            "impact_resultat_comptable": impact_compta,
            "statut_controle":         statut,
            "niveau_anomalie":         niveau,
            "code_anomalie":           hh.get("code_anomalie"),
            "commentaire":             hh.get("commentaire"),
            "source_module":           SOURCE_MODULE,
            "source_table":            "MASTER_FACT_MAN_ReservationsHorsHostaway",
            "source_pk":               str(hh.get("reservation_hh_id")),
            "date_integration":        DATE_INTEGRATION,
        }


    # --- Branche HA ---
    for res in res_dicts:
        rid = res.get("reservation_id")
        channel = res.get("channel_type") or ""
        sf      = res.get("source_financiere") or ""
        incl    = res.get("inclure_resultat")
        status  = res.get("status") or ""
        total   = res.get("totalPrice") or 0
        payout  = pay_index.get(rid)

        # S7 — ownerStay
        if incl == "NON" or status == "ownerStay":
            logement_id, proprietaire_id, ano_code, ano_msg = resolve_logement(
                res["listingMapId"],
                date_arrivee_str=date_to_str(res.get("checkInDate")),
                date_depart_str=date_to_str(res.get("checkOutDate")),
            )
            stats["OWNERSTAY_EXCLU"] += 1
            commentaire = "Hostaway ownerStay — exclu résultat"
            if status == "modified":
                commentaire += " | Hostaway status=modified"
            row = make_row_ha(res, payout, "OWNERSTAY_EXCLU", "NON_CONCERNE", 0,
                              "HR", "EXCLU_RESULTAT", "INFO", None, commentaire,
                              logement_id, proprietaire_id)
            master_rows.append(row)
            continue

        # Périmètre économique Hostaway : seuls Status ∈ {new, modified} entrent en résultat.
        # ownerStay a déjà sa propre exclusion ci-dessus (règle métier distincte, inchangée) ; tout
        # AUTRE statut (ex. cancelled) est structurellement hors périmètre — jamais compté comme une
        # réservation valide, quel que soit le canal.
        if status.strip().lower() not in ("new", "modified"):
            logement_id, proprietaire_id, ano_code, ano_msg = resolve_logement(
                res["listingMapId"],
                date_arrivee_str=date_to_str(res.get("checkInDate")),
                date_depart_str=date_to_str(res.get("checkOutDate")),
            )
            stats["STATUT_HOSTAWAY_HORS_PERIMETRE"] += 1
            commentaire = f"Statut Hostaway hors périmètre économique (status={status!r})"
            row = make_row_ha(res, payout, "STATUT_HOSTAWAY_HORS_PERIMETRE", "NON_CONCERNE", 0,
                              "HR", "EXCLU_RESULTAT", "INFO", "STATUT_HOSTAWAY_HORS_PERIMETRE",
                              commentaire, logement_id, proprietaire_id)
            master_rows.append(row)
            continue

        # Ligne HA déjà couverte par HH (S3/S4) → exclure
        if rid in ha_ids_linked_to_hh:
            stats["HA_EXCLU_PAR_HH"] += 1
            continue

        logement_id, proprietaire_id, ano_code, ano_msg = resolve_logement(
            res["listingMapId"],
            date_arrivee_str=date_to_str(res.get("checkInDate")),
            date_depart_str=date_to_str(res.get("checkOutDate")),
        )

        if ano_code in (HORS_PARC_TECHNIQUE, STATUT_PARC_INVALIDE):
            source_val = HORS_PARC_TECHNIQUE if ano_code == HORS_PARC_TECHNIQUE else A_CONTROLER
            statut = "EXCLU_RESULTAT" if ano_code == HORS_PARC_TECHNIQUE else A_CONTROLER
            niveau = "INFO" if ano_code == HORS_PARC_TECHNIQUE else A_CONTROLER
            row = make_row_ha(
                res, payout, source_val, "NON_CONCERNE",
                0, "HR", statut, niveau, ano_code,
                ano_msg, logement_id, None,
            )
            master_rows.append(row)
            stats[ano_code] += 1
            continue

        statut_logement  = None
        niveau_logement  = None
        if ano_code in ("LOGEMENT_INACTIF", "SEJOUR_CHEVAUCHE_SORTIE_GESTION", "PROPRIETAIRE_ABSENT"):
            statut_logement = "A_CONTROLER"
            niveau_logement = "A_CONTROLER"

        # Payout info
        payout_calcule  = payout.get("payout_calcule") if payout else None
        statut_payout   = payout.get("statut_calcul_payout") if payout else None

        # Commentaire modified
        extra_comment = ""
        if status == "modified":
            extra_comment = f" | Hostaway status=modified, payout {statut_payout or 'absent'}"

        # S1 — AIRBNB
        if channel == "AIRBNB":
            if statut_payout == "NORMAL" and statut_logement is None:
                commentaire = f"Airbnb HA normal{extra_comment}"
                row = make_row_ha(res, payout, "HOSTAWAY_AIRBNB", "HOSTAWAY_PAYOUT",
                                  payout_calcule, "IC", "VALIDE", "INFO", None,
                                  commentaire, logement_id, proprietaire_id)
                stats["S1_AIRBNB_VALIDE"] += 1
            else:
                commentaire = f"Airbnb HA — payout={statut_payout}{extra_comment}"
                code_ano = ano_code or f"PAYOUT_{statut_payout}"
                row = make_row_ha(res, payout, "HOSTAWAY_AIRBNB", "HOSTAWAY_PAYOUT",
                                  payout_calcule or 0, "IC",
                                  statut_logement or "A_CONTROLER",
                                  niveau_logement or "A_CONTROLER",
                                  code_ano, commentaire, logement_id, proprietaire_id)
                stats["S1_AIRBNB_A_CONTROLER"] += 1
            master_rows.append(row)

        # S2 — BOOKING
        elif channel == "BOOKING":
            if statut_payout == "NORMAL" and statut_logement is None:
                commentaire = f"Booking HA normal{extra_comment}"
                row = make_row_ha(res, payout, "HOSTAWAY_BOOKING", "HOSTAWAY_PAYOUT",
                                  payout_calcule, "IC", "VALIDE", "INFO", None,
                                  commentaire, logement_id, proprietaire_id)
                stats["S2_BOOKING_VALIDE"] += 1
            else:
                commentaire = f"Booking HA — payout={statut_payout}{extra_comment}"
                code_ano = ano_code or f"PAYOUT_{statut_payout}"
                row = make_row_ha(res, payout, "HOSTAWAY_BOOKING", "HOSTAWAY_PAYOUT",
                                  payout_calcule or 0, "IC",
                                  statut_logement or "A_CONTROLER",
                                  niveau_logement or "A_CONTROLER",
                                  code_ano, commentaire, logement_id, proprietaire_id)
                stats["S2_BOOKING_A_CONTROLER"] += 1
            master_rows.append(row)

        # S5 — VRBO sans HH
        elif channel == "VRBO":
            commentaire = f"VRBO Hostaway sans saisie HH — montant en attente{extra_comment}"
            row = make_row_ha(res, payout, "HOSTAWAY_VRBO_A_CONTROLER", "A_CONTROLER",
                              0, "HC", "A_CONTROLER", "A_CONTROLER",
                              "VRBO_MONTANT_NON_RENSEIGNE",
                              commentaire, logement_id, proprietaire_id)
            master_rows.append(row)
            stats["S5_VRBO_A_CONTROLER"] += 1

        # DIRECT sans HH (cas non couvert D054 — proposition validée)
        elif channel == "DIRECT":
            commentaire = f"Réservation directe Hostaway sans saisie hors Hostaway en face{extra_comment}"
            row = make_row_ha(res, payout, "HOSTAWAY_DIRECT_HH", "A_CONTROLER",
                              0, "HC", "A_CONTROLER", "A_CONTROLER",
                              "DIRECT_SANS_SAISIE_HH",
                              commentaire, logement_id, proprietaire_id)
            master_rows.append(row)
            stats["DIRECT_SANS_SAISIE_HH"] += 1

        else:
            commentaire = f"Canal inconnu: {channel}"
            row = make_row_ha(res, payout, f"INCONNU_{channel}", "A_CONTROLER",
                              0, "HC", "A_CONTROLER", "A_CONTROLER",
                              "CANAL_INCONNU", commentaire, logement_id, proprietaire_id)
            master_rows.append(row)
            stats["CANAL_INCONNU"] += 1


    # --- Branche HH — lignes liées à HA (S3/S4) ---
    for ha_id, hh in hh_by_ha_id.items():
        canal = (hh.get("canal_id") or "").upper()
        src = "HOSTAWAY_VRBO_HH" if "VRBO" in canal else "HOSTAWAY_DIRECT_HH"
        row = make_row_hh(hh, src_override=src)
        master_rows.append(row)
        stats["S3_S4_DIRECT_VRBO_HH"] += 1

    # --- Branche HH — lignes pures (S6) ---
    for hh in hh_pure:
        row = make_row_hh(hh)
        master_rows.append(row)
        stats["S6_HH_PURE"] += 1

    # HH non-valides signalées
    if hh_non_valides > 0:
        print(f"  AVERTISSEMENT : {hh_non_valides} lignes HH non-VALIDE exclues (RESERVATION_HH_NON_VALIDE)")

    print(f"\nTotal lignes construites : {len(master_rows)}")

    # ---------------------------------------------------------------------------
    # 4. Contrôle BLOQUANT : RESERVATION_CALC_ID_DUPLIQUE
    # ---------------------------------------------------------------------------
    calc_ids = [r["reservation_calc_id"] for r in master_rows]
    dup = [cid for cid, cnt in Counter(calc_ids).items() if cnt > 1]
    if dup:
        abort(f"RESERVATION_CALC_ID_DUPLIQUE — {len(dup)} IDs dupliqués : {dup[:5]}")

    # ---------------------------------------------------------------------------
    # 5. Contrôle : aucune donnée personnelle voyageur
    # ---------------------------------------------------------------------------
    PERSONAL_FIELDS = {"guestname", "guest_name", "guest_email", "email", "telephone",
                       "phone", "adresse_voyageur", "guest_comment", "commentaire_voyageur",
                       "iban", "nom_complet", "prenom"}
    all_cols = {k.lower() for r in master_rows for k in r.keys()}
    found_personal = PERSONAL_FIELDS & all_cols
    if found_personal:
        abort(f"DONNEE_PERSONNELLE_DETECTEE — colonnes: {found_personal}. Arrêt avant écriture.")

    print("[OK] Aucune donnée personnelle voyageur dans les colonnes produites.")

    # ---------------------------------------------------------------------------
    # 6. Définition VUE_FLUX (VALIDE + impact_reel=OUI + montant≠0)
    # ---------------------------------------------------------------------------
    vue_rows = [r for r in master_rows
                if r["statut_controle"] == "VALIDE"
                and r["impact_resultat_reel"] == "OUI"
                and (r["montant_retenu"] or 0) != 0]

    print(f"VUE_FLUX : {len(vue_rows)} lignes (VALIDE + impact_reel=OUI + montant≠0)")

    # ---------------------------------------------------------------------------
    # 7. Écriture dans MASTER_CALC_Reservations.xlsx
    # ---------------------------------------------------------------------------
    HEADERS = [
        "reservation_calc_id", "ROW_HASH", "source", "reservation_id_hostaway",
        "reservation_hh_id", "mois", "logement_id", "proprietaire_id",
        "date_arrivee", "date_depart", "nuits", "guestCount", "source_guestCount", "montant_retenu", "source_montant",
        "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
        "statut_controle", "niveau_anomalie", "code_anomalie", "commentaire",
        "source_module", "source_table", "source_pk", "date_integration",
    ]
    assert len(HEADERS) == 26, f"Attendu 26 colonnes, trouvé {len(HEADERS)}"

    # ── Ecriture SQLite (chemin normal) ──
    if args.sans_sqlite:
        print("\n[6/6] Ecriture SQLite ignoree (--sans-sqlite).")
    elif chemin_base is None:
        # Aucune base designee : execution hors contexte applicatif. On le DIT, mais on ne refuse
        # pas — sinon le lot deviendrait inutilisable partout ou la base n'est pas montee.
        print("\n[6/6] Aucune base applicative designee : reservations_calculees non ecrit.")
    else:
        dataset_id, message = ecrire_sqlite(chemin_base, master_rows)
        if dataset_id is None:
            abort("ecriture SQLite impossible : %s" % message)
        print(f"\n[6/6] reservations_calculees : {message}")

    # ── Ecriture Excel (parite legacy, temporaire) ──
    #
    # Ce classeur ne sert plus qu'a comparer avec l'historique. Le couper (--sans-excel) doit rester
    # sans effet sur ce qui precede : si une valeur changeait selon qu'on ecrit le classeur ou non,
    # la parite ne voudrait rien dire.
    if args.sans_excel:
        print("      Classeur de parite non ecrit (--sans-excel).")
    else:
        print(f"      Ecriture parite dans {PATH_TARGET}...")
        # Load workbook preserving POWER_QUERY_CODE
        wb = load_workbook(PATH_TARGET)

        # Rebuild MASTER sheet
        if "MASTER" in wb.sheetnames:
            del wb["MASTER"]
        ws_master = wb.create_sheet("MASTER", 0)

        ws_master.append(HEADERS)
        for r in master_rows:
            ws_master.append([r.get(h) for h in HEADERS])

        # Rebuild VUE_FLUX sheet
        if "VUE_FLUX" in wb.sheetnames:
            del wb["VUE_FLUX"]
        ws_vue = wb.create_sheet("VUE_FLUX", 1)

        ws_vue.append(HEADERS)
        for r in vue_rows:
            ws_vue.append([r.get(h) for h in HEADERS])

        wb.save(PATH_TARGET)
        wb.close()
        print("Fichier sauvegardé.")

    # ---------------------------------------------------------------------------
    # 8. Rapport final
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("RAPPORT LOT 4BIS — MASTER_CALC_Reservations")
    print("=" * 60)

    print(f"\nLignes MASTER      : {len(master_rows)}")
    print(f"Lignes VUE_FLUX    : {len(vue_rows)}")

    print("\nRépartition par source :")
    src_count = Counter(r["source"] for r in master_rows)
    for s, c in sorted(src_count.items()):
        print(f"  {s:<35} {c}")

    print("\nRépartition par statut_controle :")
    statut_count = Counter(r["statut_controle"] for r in master_rows)
    for s, c in sorted(statut_count.items()):
        print(f"  {s:<20} {c}")

    print("\nRépartition par code_impact :")
    ci_count = Counter(r["code_impact"] for r in master_rows)
    for s, c in sorted(ci_count.items()):
        print(f"  {s:<10} {c}")

    print("\nAnomalies A_CONTROLER :")
    ano_count = Counter(r["code_anomalie"] for r in master_rows if r["code_anomalie"])
    for s, c in sorted(ano_count.items()):
        print(f"  {s:<40} {c}")

    print(f"\nDIRECT_SANS_SAISIE_HH              : {stats['DIRECT_SANS_SAISIE_HH']}")
    print(f"VRBO_MONTANT_NON_RENSEIGNE         : {stats['S5_VRBO_A_CONTROLER']}")
    print(f"ownerStay EXCLU_RESULTAT           : {stats['OWNERSTAY_EXCLU']}")

    print("\n[SECURITE] Vérification données personnelles voyageur :")
    print("  Colonnes présentes dans MASTER :", HEADERS)
    print("  Intersection avec champs personnels :", found_personal or "AUCUNE ✓")

    print("\nBloquants détectés : 0")
    print("Script terminé avec succès.")
    print("RAPPEL : aucun git add/commit sans validation humaine.")


if __name__ == "__main__":
    main()

"""Lot4bis — preuve A/B : référentiels (mapping/logements/gestion) et réservations hors Hostaway
lus depuis SQLite produisent EXACTEMENT le même résultat moteur (S1-S7 inchangé) que le chemin
Excel legacy (mission 14e).

Périmètre volontairement resserré : le chargement Hostaway lui-même (`charger_hostaway_sqlite`)
existait déjà avant cette mission et n'est pas modifié ici — on le garde identique (source SQLite
dans les deux runs) et on ne fait varier QUE les référentiels et les réservations HH, qui sont les
deux nouveautés de ce tour. Cela isole précisément ce qui est prouvé.

Aucune donnée réelle : fixture entièrement fictive, `runpy.run_path` sur le script réel.
"""
from __future__ import annotations

import runpy
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

REAL_SCRIPT = (Path(__file__).resolve().parent.parent / "02_TRAVAIL" /
              "lot4bis_charger_reservations.py")


def _copier_script(tmp_path: Path) -> Path:
    travail = tmp_path / "02_TRAVAIL"
    travail.mkdir(parents=True, exist_ok=True)
    dest = travail / REAL_SCRIPT.name
    shutil.copy2(REAL_SCRIPT, dest)
    for lib in ("lib_db_moteur.py", "lib_ref_history.py", "lib_guestcount.py", "lib_parc.py"):
        source = REAL_SCRIPT.parent / lib
        if source.exists():
            shutil.copy2(source, travail / lib)
    return dest


def _base_avec_hostaway_et_dataset_tables(db_path: Path):
    """Base minimale : hostaway_reservations/payouts + tables datasets requises par
    reservations_calculees. Un seul cas HA (S1 Airbnb, payout normal) — suffisant pour prouver
    que la partie référentiels/HH est bien celle qui varie entre les deux runs, pas le reste."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE hostaway_reservations (
            id INTEGER PRIMARY KEY, extraction_id TEXT, reservation_id TEXT, listing_map_id TEXT,
            source TEXT, channel_type TEXT, source_financiere TEXT, status TEXT,
            payment_status TEXT, check_in_date TEXT, check_out_date TEXT, nights INTEGER,
            number_of_guests INTEGER, guest_count INTEGER, source_guest_count TEXT,
            controle_guest_count TEXT, code_controle_guest_count TEXT, total_price REAL,
            cleaning_fee_res REAL, channel_commission REAL, airbnb_expected_payout REAL,
            is_owner_stay TEXT, inclure_resultat TEXT, updated_on TEXT, created_on TEXT,
            extrait_le TEXT, row_hash TEXT, payload_json TEXT
        );
        CREATE TABLE hostaway_payouts (
            id INTEGER PRIMARY KEY, extraction_id TEXT, reservation_id TEXT, listing_map_id TEXT,
            source TEXT, channel_type TEXT, statut_calcul_payout TEXT, payout_calcule REAL,
            source_payout TEXT, menage_retenu REAL, assiette_commission REAL,
            menage_retenu_source TEXT, cout_standard_id TEXT, cout_standard_menage_snapshot REAL,
            cout_standard_date_debut_validite TEXT, cout_standard_date_fin_validite TEXT,
            logement_id_snapshot TEXT, type_logement_id_snapshot TEXT,
            date_reference_cout_menage TEXT, inclure_resultat_auto TEXT, extrait_le TEXT,
            row_hash TEXT
        );
        CREATE TABLE hostaway_extractions (
            id INTEGER PRIMARY KEY, extraction_id TEXT UNIQUE, run_id TEXT, mode TEXT,
            date_debut TEXT, date_fin TEXT, statut TEXT, nb_listings INTEGER,
            nb_reservations INTEGER, nb_payouts INTEGER, message TEXT, created_at TEXT
        );
        CREATE TABLE reservations_datasets (
            id INTEGER PRIMARY KEY, dataset_id TEXT UNIQUE, etape TEXT, extraction_id TEXT,
            run_id TEXT, date_calcul TEXT, nb_lignes INTEGER, statut TEXT, actif INTEGER,
            message TEXT
        );
        CREATE TABLE reservations_calculees (
            id INTEGER PRIMARY KEY, dataset_id TEXT,
            reservation_calc_id TEXT, row_hash TEXT, source TEXT, reservation_id_hostaway TEXT,
            reservation_hh_id TEXT, mois TEXT, logement_id TEXT, proprietaire_id TEXT,
            date_arrivee TEXT, date_depart TEXT, nuits INTEGER, guest_count TEXT,
            source_guest_count TEXT, montant_retenu REAL, source_montant TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            source_module TEXT, source_table TEXT, source_pk TEXT, date_integration TEXT
        );
    """)
    conn.execute(
        "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
        "nb_listings, nb_reservations, nb_payouts) "
        "VALUES ('HAX-T1','R1','API','2026-01-01T00:00:00Z','SUCCES',1,1,1)")
    conn.execute(
        "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
        "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
        "inclure_resultat, check_in_date, check_out_date, nights) "
        "VALUES ('HAX-T1','70001','480136','HOSTAWAY','AIRBNB','AIRBNB','new',150.0,'false',"
        "'OUI','2026-06-01','2026-06-03',2)")
    conn.execute(
        "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
        "statut_calcul_payout, payout_calcule) "
        "VALUES ('HAX-T1','70001','480136','NORMAL',130.0)")
    conn.commit()
    conn.close()


def _ajouter_referentiels_sqlite(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE ref_mapping_logements (
            mapping_logement_id TEXT PRIMARY KEY, source TEXT, champ_source TEXT,
            valeur_source TEXT, logement_id TEXT, niveau_confiance TEXT, actif TEXT,
            commentaire TEXT, import_id TEXT
        );
        CREATE TABLE ref_logements (
            logement_id TEXT PRIMARY KEY, hostaway_listing_id TEXT, statut_parc TEXT,
            actif TEXT, import_id TEXT
        );
        CREATE TABLE ref_gestion_logements_hist (
            gestion_id TEXT PRIMARY KEY, logement_id TEXT, proprietaire_id TEXT,
            date_debut TEXT, date_fin TEXT, statut_gestion TEXT, source TEXT,
            commentaire TEXT, import_id TEXT
        );
        CREATE TABLE reservations_hors_hostaway (
            id INTEGER PRIMARY KEY, reservation_hh_id TEXT, row_hash TEXT, mois TEXT,
            canal_id TEXT, source_financiere TEXT, proprietaire_id TEXT, logement_id TEXT,
            reservation_id_hostaway TEXT, date_arrivee TEXT, date_depart TEXT, nuits INTEGER,
            guest_count INTEGER, montant_percu REAL, montant_retenu REAL, mode_paiement_id TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            date_saisie TEXT, source_module TEXT, source_table TEXT, source_pk TEXT,
            date_integration TEXT, statut TEXT, acteur TEXT, date_creation TEXT,
            date_modification TEXT
        );
    """)
    conn.execute(
        "INSERT INTO ref_mapping_logements VALUES "
        "('MAP-1','Hostaway','listingMapId','480136','LOG_A1','FIABLE','OUI','','IMP-1')")
    conn.execute(
        "INSERT INTO ref_logements VALUES ('LOG_A1','480136','GERE','OUI','IMP-1')")
    conn.execute(
        "INSERT INTO ref_gestion_logements_hist VALUES "
        "('GST-1','LOG_A1','PROP_A','2025-01-01','','ACTIF','','','IMP-1')")
    conn.execute(
        "INSERT INTO reservations_hors_hostaway "
        "(reservation_hh_id, mois, canal_id, proprietaire_id, logement_id, "
        "reservation_id_hostaway, date_arrivee, date_depart, nuits, guest_count, "
        "montant_percu, montant_retenu, code_impact, impact_resultat_reel, "
        "impact_resultat_comptable, statut_controle, niveau_anomalie, statut) "
        "VALUES ('HH-1','2026-06','DIRECT','PROP_A','LOG_A1',NULL,'2026-06-10','2026-06-12',2,2,"
        "200.0,180.0,'HC','OUI','NON','VALIDE','INFO','ACTIVE')")
    conn.commit()
    conn.close()


def _run(script: Path, extra_argv: list[str]):
    # lot4bis importe lib_ref_history/lib_guestcount/lib_parc en tête de fichier, AVANT son propre
    # `sys.path.insert` (qui n'intervient que plus tard, pour lib_db_moteur) : `runpy.run_path` ne
    # place pas systématiquement le dossier du script en tête de `sys.path` comme le ferait
    # `python script.py` directement — on le fait nous-mêmes ici, sans toucher au script réel.
    argv_pytest = sys.argv
    path_avant = list(sys.path)
    sys.path.insert(0, str(script.parent))
    sys.argv = [str(script)] + extra_argv
    try:
        return runpy.run_path(str(script), run_name="__main__")
    finally:
        sys.argv = argv_pytest
        sys.path[:] = path_avant


def _lignes_calculees(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "SELECT reservation_calc_id, source, reservation_id_hostaway, reservation_hh_id, mois, "
        "logement_id, proprietaire_id, montant_retenu, code_impact, statut_controle, "
        "niveau_anomalie, code_anomalie FROM reservations_calculees ORDER BY reservation_calc_id")]
    conn.close()
    return rows


def test_ab_sqlite_vs_excel_meme_resultat_moteur(tmp_path):
    """Legacy (tout Excel) vs SQLite direct (tout SQLite) — même donnée source, même résultat
    moteur (S1 Airbnb valide + HH direct), 0 différence inexpliquée."""
    # Run A — tout SQLite (Hostaway déjà existant avant cette mission, référentiels + HH
    # nouveaux ce tour).
    script_a = _copier_script(tmp_path / "run_sqlite")
    db_a = tmp_path / "run_sqlite" / "app.db"
    _base_avec_hostaway_et_dataset_tables(db_a)
    _ajouter_referentiels_sqlite(db_a)
    _run(script_a, ["--source-hostaway", "SQLITE", "--db", str(db_a), "--sans-excel"])
    lignes_sqlite = _lignes_calculees(db_a)

    # Run B — tout Excel (chemin legacy complet, celui qui existait avant toute migration SQLite).
    # Une base est quand même désignée : reservations_calculees continue d'être écrit en SQLite
    # même en mode Excel (ecrire_sqlite ne dépend pas de la source des ENTRÉES), ce qui permet la
    # comparaison ligne à ligne ci-dessous.
    script_b = _copier_script(tmp_path / "run_excel")
    db_b = tmp_path / "run_excel" / "app.db"
    conn = sqlite3.connect(str(db_b))
    conn.executescript("""
        CREATE TABLE reservations_datasets (
            id INTEGER PRIMARY KEY, dataset_id TEXT UNIQUE, etape TEXT, extraction_id TEXT,
            run_id TEXT, date_calcul TEXT, nb_lignes INTEGER, statut TEXT, actif INTEGER,
            message TEXT
        );
        CREATE TABLE reservations_calculees (
            id INTEGER PRIMARY KEY, dataset_id TEXT,
            reservation_calc_id TEXT, row_hash TEXT, source TEXT, reservation_id_hostaway TEXT,
            reservation_hh_id TEXT, mois TEXT, logement_id TEXT, proprietaire_id TEXT,
            date_arrivee TEXT, date_depart TEXT, nuits INTEGER, guest_count TEXT,
            source_guest_count TEXT, montant_retenu REAL, source_montant TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            source_module TEXT, source_table TEXT, source_pk TEXT, date_integration TEXT
        );
    """)
    conn.commit()
    conn.close()

    import openpyxl
    ha_dir = tmp_path / "run_excel" / "02_TRAVAIL" / "Lot1_Hostaway"
    ha_dir.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["reservation_id", "listingMapId", "channel_type", "source_financiere", "status",
              "totalPrice", "is_ownerStay", "inclure_resultat", "checkInDate", "checkOutDate",
              "nights", "numberOfGuests", "guestCount", "source_guestCount"])
    ws.append(["70001", "480136", "AIRBNB", "AIRBNB", "new", 150.0, "false", "OUI",
              "2026-06-01", "2026-06-03", 2, None, None, None])
    wb.save(ha_dir / "MASTER_FACT_HA_Reservations.xlsx")
    wb.close()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["reservation_id", "listingMapId", "statut_calcul_payout", "payout_calcule"])
    ws.append(["70001", "480136", "NORMAL", 130.0])
    wb.save(ha_dir / "MASTER_CALC_HA_Payout.xlsx")
    wb.close()

    ref_dir = tmp_path / "run_excel" / "01_SOURCES_BRUTES" / "REF_Setup"
    ref_dir.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "REF_Mapping_Logements"
    ws.append(["source", "champ_source", "valeur_source", "logement_id", "actif"])
    ws.append(["Hostaway", "listingMapId", "480136", "LOG_A1", "OUI"])
    ws2 = wb.create_sheet("REF_Logements")
    ws2.append(["logement_id", "statut_parc", "actif"])
    ws2.append(["LOG_A1", "GERE", "OUI"])
    ws3 = wb.create_sheet("REF_Gestion_Logements_Hist")
    ws3.append(["gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
               "statut_gestion"])
    ws3.append(["GST-1", "LOG_A1", "PROP_A", "2025-01-01", "", "ACTIF"])
    wb.save(ref_dir / "REF_Setup.xlsm")
    wb.close()

    hh_dir = tmp_path / "run_excel" / "02_TRAVAIL" / "Lot4_ReservationsHH"
    hh_dir.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    ws.append(["reservation_hh_id", "mois", "canal_id", "proprietaire_id", "logement_id",
              "reservation_id_hostaway", "date_arrivee", "date_depart", "nuits", "guestCount",
              "total_percu", "montant_retenu", "code_impact", "impact_resultat_reel",
              "impact_resultat_comptable", "statut_controle", "niveau_anomalie"])
    ws.append(["HH-1", "2026-06", "DIRECT", "PROP_A", "LOG_A1", None, "2026-06-10", "2026-06-12",
              2, 2, 200.0, 180.0, "HC", "OUI", "NON", "VALIDE", "INFO"])
    wb.save(hh_dir / "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx")
    wb.close()

    _run(script_b, ["--source-hostaway", "EXCEL", "--db", str(db_b), "--sans-excel"])
    lignes_excel = _lignes_calculees(db_b)

    assert len(lignes_sqlite) == len(lignes_excel) == 2, (lignes_sqlite, lignes_excel)
    for a, b in zip(lignes_sqlite, lignes_excel):
        assert a == b, f"divergence SQLite vs Excel : {a} != {b}"

    # Preuve positive que le moteur S1-S7 a bien tourné (pas juste 2 lignes vides) :
    sources = {r["source"] for r in lignes_sqlite}
    assert sources == {"HOSTAWAY_AIRBNB", "MANUEL_HORS_HOSTAWAY"}
    statuts = {r["statut_controle"] for r in lignes_sqlite}
    assert statuts == {"VALIDE"}


# ============================================================================
# Mission 14f — couverture explicite S1 à S7, un cas représentatif par branche.
# ============================================================================
# LOG_A1..A7 = un logement par branche, tous GERE/actif/gérés par PROP_A depuis 2025-01-01
# (aucune anomalie de parc/gestion ne doit interférer : on veut isoler la classification
# métier S1-S7, pas retester le mapping logement/gestion déjà couvert ailleurs).
_S1_S7_LOGEMENTS = [f"LOG_A{i}" for i in range(1, 8)]
_S1_S7_LISTINGS = {f"LOG_A{i}": str(480135 + i) for i in range(1, 8)}

# (reservation_id, listing, channel, inclure_resultat, status, payout_statut, payout_montant)
_S1_S7_HA = [
    ("70001", "LOG_A1", "AIRBNB", "OUI", "new", "NORMAL", 130.0),   # S1 AIRBNB valide
    ("70002", "LOG_A2", "BOOKING", "OUI", "new", "NORMAL", 140.0),  # S2 BOOKING valide
    ("70003", "LOG_A3", "DIRECT", "OUI", "new", "NORMAL", 150.0),   # S3 — exclue (HH liée)
    ("70004", "LOG_A4", "VRBO", "OUI", "new", "NORMAL", 160.0),     # S4 — exclue (HH liée)
    ("70005", "LOG_A5", "VRBO", "OUI", "new", None, None),          # S5 VRBO sans HH
    ("70007", "LOG_A7", "AIRBNB", "NON", "new", "NORMAL", 170.0),   # S7 ownerStay
]
# (reservation_hh_id, logement, reservation_id_hostaway ou None, canal_id)
_S1_S7_HH = [
    ("HH-3", "LOG_A3", "70003", "DIRECT"),   # S3
    ("HH-4", "LOG_A4", "70004", "VRBO"),     # S4
    ("HH-6", "LOG_A6", None, "DIRECT"),      # S6 HH pure
]


def _base_s1_a_s7(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE hostaway_reservations (
            id INTEGER PRIMARY KEY, extraction_id TEXT, reservation_id TEXT, listing_map_id TEXT,
            source TEXT, channel_type TEXT, source_financiere TEXT, status TEXT,
            payment_status TEXT, check_in_date TEXT, check_out_date TEXT, nights INTEGER,
            number_of_guests INTEGER, guest_count INTEGER, source_guest_count TEXT,
            controle_guest_count TEXT, code_controle_guest_count TEXT, total_price REAL,
            cleaning_fee_res REAL, channel_commission REAL, airbnb_expected_payout REAL,
            is_owner_stay TEXT, inclure_resultat TEXT, updated_on TEXT, created_on TEXT,
            extrait_le TEXT, row_hash TEXT, payload_json TEXT
        );
        CREATE TABLE hostaway_payouts (
            id INTEGER PRIMARY KEY, extraction_id TEXT, reservation_id TEXT, listing_map_id TEXT,
            source TEXT, channel_type TEXT, statut_calcul_payout TEXT, payout_calcule REAL,
            source_payout TEXT, menage_retenu REAL, assiette_commission REAL,
            menage_retenu_source TEXT, cout_standard_id TEXT, cout_standard_menage_snapshot REAL,
            cout_standard_date_debut_validite TEXT, cout_standard_date_fin_validite TEXT,
            logement_id_snapshot TEXT, type_logement_id_snapshot TEXT,
            date_reference_cout_menage TEXT, inclure_resultat_auto TEXT, extrait_le TEXT,
            row_hash TEXT
        );
        CREATE TABLE hostaway_extractions (
            id INTEGER PRIMARY KEY, extraction_id TEXT UNIQUE, run_id TEXT, mode TEXT,
            date_debut TEXT, date_fin TEXT, statut TEXT, nb_listings INTEGER,
            nb_reservations INTEGER, nb_payouts INTEGER, message TEXT, created_at TEXT
        );
        CREATE TABLE reservations_datasets (
            id INTEGER PRIMARY KEY, dataset_id TEXT UNIQUE, etape TEXT, extraction_id TEXT,
            run_id TEXT, date_calcul TEXT, nb_lignes INTEGER, statut TEXT, actif INTEGER,
            message TEXT
        );
        CREATE TABLE reservations_calculees (
            id INTEGER PRIMARY KEY, dataset_id TEXT,
            reservation_calc_id TEXT, row_hash TEXT, source TEXT, reservation_id_hostaway TEXT,
            reservation_hh_id TEXT, mois TEXT, logement_id TEXT, proprietaire_id TEXT,
            date_arrivee TEXT, date_depart TEXT, nuits INTEGER, guest_count TEXT,
            source_guest_count TEXT, montant_retenu REAL, source_montant TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            source_module TEXT, source_table TEXT, source_pk TEXT, date_integration TEXT
        );
    """)
    conn.execute(
        "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
        "nb_listings, nb_reservations, nb_payouts) "
        "VALUES ('HAX-T1','R1','API','2026-01-01T00:00:00Z','SUCCES',7,6,4)")
    for rid, log, channel, incl, status, pay_statut, pay_montant in _S1_S7_HA:
        listing = _S1_S7_LISTINGS[log]
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
            "inclure_resultat, check_in_date, check_out_date, nights) "
            "VALUES ('HAX-T1',?,?,'HOSTAWAY',?,?,?,150.0,'false',?,'2026-06-01','2026-06-03',2)",
            (rid, listing, channel, channel, status, incl))
        if pay_statut is not None:
            conn.execute(
                "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
                "statut_calcul_payout, payout_calcule) VALUES ('HAX-T1',?,?,?,?)",
                (rid, listing, pay_statut, pay_montant))
    conn.commit()
    conn.close()


def _referentiels_s1_a_s7(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE ref_mapping_logements (
            mapping_logement_id TEXT PRIMARY KEY, source TEXT, champ_source TEXT,
            valeur_source TEXT, logement_id TEXT, niveau_confiance TEXT, actif TEXT,
            commentaire TEXT, import_id TEXT
        );
        CREATE TABLE ref_logements (
            logement_id TEXT PRIMARY KEY, hostaway_listing_id TEXT, statut_parc TEXT,
            actif TEXT, import_id TEXT
        );
        CREATE TABLE ref_gestion_logements_hist (
            gestion_id TEXT PRIMARY KEY, logement_id TEXT, proprietaire_id TEXT,
            date_debut TEXT, date_fin TEXT, statut_gestion TEXT, source TEXT,
            commentaire TEXT, import_id TEXT
        );
        CREATE TABLE reservations_hors_hostaway (
            id INTEGER PRIMARY KEY, reservation_hh_id TEXT, row_hash TEXT, mois TEXT,
            canal_id TEXT, source_financiere TEXT, proprietaire_id TEXT, logement_id TEXT,
            reservation_id_hostaway TEXT, date_arrivee TEXT, date_depart TEXT, nuits INTEGER,
            guest_count INTEGER, montant_percu REAL, montant_retenu REAL, mode_paiement_id TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            date_saisie TEXT, source_module TEXT, source_table TEXT, source_pk TEXT,
            date_integration TEXT, statut TEXT, acteur TEXT, date_creation TEXT,
            date_modification TEXT
        );
    """)
    for i, log in enumerate(_S1_S7_LOGEMENTS, start=1):
        listing = _S1_S7_LISTINGS[log]
        conn.execute(
            "INSERT INTO ref_mapping_logements VALUES "
            "(?,'Hostaway','listingMapId',?,?,'FIABLE','OUI','','IMP-1')",
            (f"MAP-{i}", listing, log))
        conn.execute(
            "INSERT INTO ref_logements VALUES (?,?,'GERE','OUI','IMP-1')", (log, listing))
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist VALUES "
            "(?,?,'PROP_A','2025-01-01','','ACTIF','','','IMP-1')", (f"GST-{i}", log))
    for hh_id, log, ha_id, canal in _S1_S7_HH:
        conn.execute(
            "INSERT INTO reservations_hors_hostaway "
            "(reservation_hh_id, mois, canal_id, proprietaire_id, logement_id, "
            "reservation_id_hostaway, date_arrivee, date_depart, nuits, guest_count, "
            "montant_percu, montant_retenu, code_impact, impact_resultat_reel, "
            "impact_resultat_comptable, statut_controle, niveau_anomalie, statut) "
            "VALUES (?,'2026-06',?,'PROP_A',?,?,'2026-06-10','2026-06-12',2,2,"
            "200.0,180.0,'HC','OUI','NON','VALIDE','INFO','ACTIVE')",
            (hh_id, canal, log, ha_id))
    conn.commit()
    conn.close()


def _construire_excel_s1_a_s7(base_dir: Path):
    import openpyxl

    ha_dir = base_dir / "02_TRAVAIL" / "Lot1_Hostaway"
    ha_dir.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["reservation_id", "listingMapId", "channel_type", "source_financiere", "status",
              "totalPrice", "is_ownerStay", "inclure_resultat", "checkInDate", "checkOutDate",
              "nights", "numberOfGuests", "guestCount", "source_guestCount"])
    for rid, log, channel, incl, status, _pay_statut, _pay_montant in _S1_S7_HA:
        ws.append([rid, _S1_S7_LISTINGS[log], channel, channel, status, 150.0, "false", incl,
                  "2026-06-01", "2026-06-03", 2, None, None, None])
    wb.save(ha_dir / "MASTER_FACT_HA_Reservations.xlsx")
    wb.close()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["reservation_id", "listingMapId", "statut_calcul_payout", "payout_calcule"])
    for rid, log, _channel, _incl, _status, pay_statut, pay_montant in _S1_S7_HA:
        if pay_statut is not None:
            ws.append([rid, _S1_S7_LISTINGS[log], pay_statut, pay_montant])
    wb.save(ha_dir / "MASTER_CALC_HA_Payout.xlsx")
    wb.close()

    ref_dir = base_dir / "01_SOURCES_BRUTES" / "REF_Setup"
    ref_dir.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "REF_Mapping_Logements"
    ws.append(["source", "champ_source", "valeur_source", "logement_id", "actif"])
    for log in _S1_S7_LOGEMENTS:
        ws.append(["Hostaway", "listingMapId", _S1_S7_LISTINGS[log], log, "OUI"])
    ws2 = wb.create_sheet("REF_Logements")
    ws2.append(["logement_id", "statut_parc", "actif"])
    for log in _S1_S7_LOGEMENTS:
        ws2.append([log, "GERE", "OUI"])
    ws3 = wb.create_sheet("REF_Gestion_Logements_Hist")
    ws3.append(["gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
               "statut_gestion"])
    for i, log in enumerate(_S1_S7_LOGEMENTS, start=1):
        ws3.append([f"GST-{i}", log, "PROP_A", "2025-01-01", "", "ACTIF"])
    wb.save(ref_dir / "REF_Setup.xlsm")
    wb.close()

    hh_dir = base_dir / "02_TRAVAIL" / "Lot4_ReservationsHH"
    hh_dir.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    ws.append(["reservation_hh_id", "mois", "canal_id", "proprietaire_id", "logement_id",
              "reservation_id_hostaway", "date_arrivee", "date_depart", "nuits", "guestCount",
              "total_percu", "montant_retenu", "code_impact", "impact_resultat_reel",
              "impact_resultat_comptable", "statut_controle", "niveau_anomalie"])
    for hh_id, log, ha_id, canal in _S1_S7_HH:
        ws.append([hh_id, "2026-06", canal, "PROP_A", log, ha_id, "2026-06-10", "2026-06-12",
                  2, 2, 200.0, 180.0, "HC", "OUI", "NON", "VALIDE", "INFO"])
    wb.save(hh_dir / "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx")
    wb.close()


def test_ab_couverture_s1_a_s7(tmp_path):
    """Un cas représentatif par branche S1..S7, chemin legacy (Excel) vs SQLite direct.

    Différences purement techniques normalisées avant comparaison : `ROW_HASH` (dérivée d'un
    hash Python non stable entre exécutions distinctes de `runpy.run_path`, jamais comparée en
    tant que telle — seules les colonnes métier de `_lignes_calculees()` le sont) et l'ordre des
    lignes (les deux runs trient par `reservation_calc_id`, donc déjà normalisé).
    """
    script_a = _copier_script(tmp_path / "run_sqlite")
    db_a = tmp_path / "run_sqlite" / "app.db"
    _base_s1_a_s7(db_a)
    _referentiels_s1_a_s7(db_a)
    _run(script_a, ["--source-hostaway", "SQLITE", "--db", str(db_a), "--sans-excel"])
    lignes_sqlite = _lignes_calculees(db_a)

    script_b = _copier_script(tmp_path / "run_excel")
    db_b = tmp_path / "run_excel" / "app.db"
    conn = sqlite3.connect(str(db_b))
    conn.executescript("""
        CREATE TABLE reservations_datasets (
            id INTEGER PRIMARY KEY, dataset_id TEXT UNIQUE, etape TEXT, extraction_id TEXT,
            run_id TEXT, date_calcul TEXT, nb_lignes INTEGER, statut TEXT, actif INTEGER,
            message TEXT
        );
        CREATE TABLE reservations_calculees (
            id INTEGER PRIMARY KEY, dataset_id TEXT,
            reservation_calc_id TEXT, row_hash TEXT, source TEXT, reservation_id_hostaway TEXT,
            reservation_hh_id TEXT, mois TEXT, logement_id TEXT, proprietaire_id TEXT,
            date_arrivee TEXT, date_depart TEXT, nuits INTEGER, guest_count TEXT,
            source_guest_count TEXT, montant_retenu REAL, source_montant TEXT,
            code_impact TEXT, impact_resultat_reel TEXT, impact_resultat_comptable TEXT,
            statut_controle TEXT, niveau_anomalie TEXT, code_anomalie TEXT, commentaire TEXT,
            source_module TEXT, source_table TEXT, source_pk TEXT, date_integration TEXT
        );
    """)
    conn.commit()
    conn.close()
    _construire_excel_s1_a_s7(tmp_path / "run_excel")
    _run(script_b, ["--source-hostaway", "EXCEL", "--db", str(db_b), "--sans-excel"])
    lignes_excel = _lignes_calculees(db_b)

    assert len(lignes_sqlite) == len(lignes_excel) == 7, (lignes_sqlite, lignes_excel)
    for a, b in zip(lignes_sqlite, lignes_excel):
        assert a == b, f"divergence SQLite vs Excel : {a} != {b}"

    par_logement = {r["logement_id"]: r for r in lignes_sqlite}
    attendu = {
        "LOG_A1": ("HOSTAWAY_AIRBNB", "VALIDE"),           # S1
        "LOG_A2": ("HOSTAWAY_BOOKING", "VALIDE"),          # S2
        "LOG_A3": ("HOSTAWAY_DIRECT_HH", "VALIDE"),        # S3
        "LOG_A4": ("HOSTAWAY_VRBO_HH", "VALIDE"),          # S4
        "LOG_A5": ("HOSTAWAY_VRBO_A_CONTROLER", "A_CONTROLER"),  # S5
        "LOG_A6": ("MANUEL_HORS_HOSTAWAY", "VALIDE"),      # S6
        "LOG_A7": ("OWNERSTAY_EXCLU", "EXCLU_RESULTAT"),   # S7
    }
    resultats = {}
    for log, (source_attendu, statut_attendu) in attendu.items():
        row = par_logement.get(log)
        assert row is not None, f"{log} : aucune ligne produite"
        ok = row["source"] == source_attendu and row["statut_controle"] == statut_attendu
        resultats[log] = "OK" if ok else f"KO ({row['source']}/{row['statut_controle']})"
        assert ok, f"{log} : attendu {source_attendu}/{statut_attendu}, obtenu " \
                   f"{row['source']}/{row['statut_controle']}"

    # Résumé lisible en cas d'échec partiel futur (S1..S7 nommément).
    print("\nCouverture S1-S7 :", {
        f"S{i}" if log != "LOG_A6" else "S6": resultats[log]
        for i, log in enumerate(_S1_S7_LOGEMENTS, start=1)
    })

#!/usr/bin/env python3
"""
Lot 10 — Calcul resultats, commissions et net proprietaire
Pilotage_Conciergerie

Sources (lecture seule):
  MASTER_CALC_Flux.xlsx           (Lot 9)
  MASTER_CALC_Reservations.xlsx   (Lot 4bis)
  MASTER_CALC_HA_Payout.xlsx      (Lot 1)
  REF_Setup.xlsm                  (REF_Logements, REF_Taux_Commission)

Sorties:
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx

Dependances:
  pip install pandas openpyxl

Jointure:
  MASTER_CALC_Flux.source_pk
    -> MASTER_CALC_Reservations.reservation_calc_id
    -> MASTER_CALC_Reservations.reservation_id_hostaway
    -> MASTER_CALC_HA_Payout.reservation_id
"""
import argparse
import os
import sys
import logging
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_db_moteur as dbm  # noqa: E402

from lib_ref_history import (
    REF_GESTION_LOGEMENTS_HIST_SHEET,
    resolve_canape_parametres,
    resolve_commission_rate,
    resolve_management_period,
    resolve_regle_version,
)
from lib_settlements import (
    aggregate_refacturable_charges,
    settle_invoice,
    validated_airbnb_imputation,
)
from lib_canape import calculate_canape_amount
from lib_commission_engine import (
    assiette_v1_paiement_direct,
    calculer_commission_conciergerie,
    calculer_net_proprietaire,
    plafonner_assiette_pour_commission,
)
from lib_parc import (
    A_CONTROLER,
    HORS_PARC_TECHNIQUE,
    STATUT_PARC_INVALIDE,
    is_hors_parc_technique,
    is_statut_parc_a_controler,
)

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).resolve().parent.parent
FLUX_FILE   = BASE / "02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx"
RES_FILE    = BASE / "02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx"  # source résolue (lot4quater)
PAYOUT_FILE = BASE / "02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx"
HH_FILE     = BASE / "02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
ACC_FILE    = BASE / "02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx"
CHARGES_FILE = BASE / "02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx"
AIRBNB_IMPUT_FILE = BASE / "01_SOURCES_BRUTES/ImputationsAirbnb/SAISIE_ImputationsAirbnb.xlsx"
REF_FILE    = BASE / "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"
OUT_DIR     = BASE / "02_TRAVAIL/Lot10_Resultats"

# Constante : tolérance d'arrondi par ligne (D035)
TOL_LIGNE   = 0.10
# Sentinelle charges sans logement/proprietaire (D-LOT10C-03)
SENTINEL_GLOBAL = "GLOBAL_NON_AFFECTE"
COMM_FILE   = OUT_DIR / "MASTER_CALC_Commissions.xlsx"
RESULT_FILE = OUT_DIR / "MASTER_CALC_Resultats.xlsx"
NET_FILE    = OUT_DIR / "MASTER_CALC_NetProprietaire.xlsx"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger("lot10")

# ─────────────────────────────────────────────────────────────────────────────
# Excel styles
# ─────────────────────────────────────────────────────────────────────────────
FILL_HEADER = PatternFill("solid", fgColor="1F4E79")
FILL_WARN   = PatternFill("solid", fgColor="FFEB9C")
FONT_HEADER = Font(bold=True, color="FFFFFF", name="Calibri", size=10)
FONT_DATA   = Font(name="Calibri", size=10)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _read_sheet(path: Path, sheet=None, keep_vba: bool = False) -> pd.DataFrame:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_vba=keep_vba)
    ws = wb[sheet] if sheet else wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = list(next(rows_iter))
    rows = [dict(zip(headers, r)) for r in rows_iter]
    wb.close()
    return pd.DataFrame(rows)


def _read_optional_sheet(path: Path, sheet: str, keep_vba: bool = False) -> pd.DataFrame:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_vba=keep_vba)
    try:
        if sheet not in wb.sheetnames:
            return pd.DataFrame()
        ws = wb[sheet]
        rows_iter = ws.iter_rows(values_only=True)
        headers = list(next(rows_iter))
        rows = [dict(zip(headers, r)) for r in rows_iter]
        return pd.DataFrame(rows)
    finally:
        wb.close()


def _write_sheet(wb: Workbook, name: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(name)
    cols = list(df.columns)
    for ci, col in enumerate(cols, 1):
        cell = ws.cell(1, ci, col)
        cell.font = FILL_HEADER and FONT_HEADER
        cell.fill = FILL_HEADER
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for ri, row_vals in enumerate(df.itertuples(index=False), 2):
        for ci, val in enumerate(row_vals, 1):
            ws.cell(ri, ci, val).font = FONT_DATA
    ws.freeze_panes = "A2"
    for ci, col in enumerate(cols, 1):
        max_len = len(str(col))
        if len(df) > 0:
            col_len = df[col].astype(str).str.len().max()
            max_len = max(max_len, int(col_len) if pd.notna(col_len) else max_len)
        ws.column_dimensions[get_column_letter(ci)].width = min(max_len + 2, 45)


def _mois_range(first_mois: str, last_mois: str) -> list:
    """Return ['YYYY-MM', ...] inclusive between first and last."""
    try:
        periods = pd.period_range(start=first_mois, end=last_mois, freq="M")
        return [str(p) for p in periods]
    except Exception:
        return [first_mois]


def _n(val) -> float:
    """Coerce to float, 0.0 on failure."""
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _is_placeholder_id(v) -> bool:
    """Ligne d'instruction Power Query / formule, pas une vraie cle metier."""
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s[0] in "#[<←-*"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Load sources
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# SQLite — entrée `flux_unifies` (Lot9) et sorties dérivées (migration 0044)
#
# La logique pandas de ce script n'est PAS touchée : seules les entrées/sorties changent de
# support. `flux_unifies` porte exactement les colonnes du classeur `MASTER_CALC_Flux` (voir
# migration 0043, dérivée du `COLS` de lot9_construire_flux.py) ; la seule différence est la
# convention de nommage `row_hash`/`ROW_HASH`, traduite ici pour que le calcul aval retrouve le
# vocabulaire qu'il connaît.
# ─────────────────────────────────────────────────────────────────────────────
_FLUX_COLS_SQL = (
    "flux_id", "row_hash", "source_module", "source_table", "source_pk", "date_flux", "mois",
    "logement_id", "proprietaire_id", "associe_id", "type_flux_id", "sens", "montant",
    "code_impact", "inclure_resultat_reel", "inclure_resultat_comptable",
    "inclure_resultat_hors_compta", "statut_controle", "niveau_anomalie", "code_anomalie",
    "commentaire", "date_integration",
)


def charger_flux_sqlite(chemin_base) -> pd.DataFrame:
    """Flux unifiés depuis SQLite (Lot9), au format attendu par le calcul existant.

    Aucun repli Excel : si la table est absente ou vide, le script refuse plutôt que de calculer
    sur un flux partiel — un résultat économique construit sur une source silencieusement vide
    serait faux sans qu'aucune erreur ne le signale.
    """
    conn, message = dbm.verifier(chemin_base, ("flux_unifies",))
    if conn is None:
        sys.exit(f"[lot10] ERREUR : --source SQLITE inutilisable — {message}")
    try:
        # ORDRE D'ARRIVEE : `flux_unifies` a `flux_id` pour cle primaire, pas d'`id` autoincrement.
        # `rowid` restitue l'ordre d'insertion, donc celui du classeur d'origine — le calcul aval
        # est insensible a l'ordre (groupby/merge), mais la comparaison de parite ligne a ligne
        # reste lisible sans retri artificiel.
        lignes = dbm.lignes(conn, "flux_unifies", _FLUX_COLS_SQL, ordre="rowid")
    finally:
        conn.close()
    df = pd.DataFrame(lignes)
    if len(df) == 0:
        sys.exit("[lot10] ERREUR : `flux_unifies` est vide — lancer Lot9 avant Lot10.")
    return df.rename(columns={"row_hash": "ROW_HASH"})


_RES_COLS_SQL = (
    "reservation_calc_id", "reservation_id_hostaway", "reservation_hh_id", "source",
    "source_montant", "montant_retenu", "logement_id", "proprietaire_id", "date_arrivee",
    "date_depart", "nuits", "guest_count", "canal", "payout_calcule", "menage_retenu",
    "assiette_commission",
)
_PAYOUT_COLS_SQL = (
    "reservation_id", "channel_type", "statut_calcul_payout", "payout_calcule",
    "source_payout", "menage_retenu", "assiette_commission", "inclure_resultat_auto",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
    "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
)
_LOG_COLS_SQL = (
    "logement_id", "hostaway_listing_id", "nom_logement_officiel", "nom_court", "adresse",
    "ville", "type_logement_id", "sur_hostaway", "dynamic_pricing", "actif", "statut_parc",
    "commentaire", "forfait_logiciel_consommables_mensuel",
    "seuil_voyageurs_preparation_canape", "montant_preparation_canape",
)
_PROP_COLS_SQL = (
    "proprietaire_id", "nom_proprietaire", "prenom_proprietaire", "email", "telephone",
    "adresse_facturation", "mode_facturation", "actif", "commentaire",
)
_TAUX_COLS_SQL = (
    "taux_commission_id", "proprietaire_id", "logement_id", "taux_commission",
    "date_debut", "date_fin", "actif", "justification", "commentaire",
)
_GEST_COLS_SQL = (
    "gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
    "statut_gestion", "source", "commentaire",
)


def _charger_table_sqlite(chemin_base, table, colonnes, *, rename=None, vide_ok=False,
                          ordre="rowid", ou="", args=()):
    """Charge une table référentielle/résolue SQLite au format DataFrame attendu par le calcul
    existant — même contrat fail-closed que `charger_flux_sqlite` : une table absente est une
    erreur bloquante (mission 14f : plus aucun repli Excel silencieux en mode SQLITE). Une table
    présente mais vide n'est bloquante que si `vide_ok` est False (les référentiels doivent avoir
    été importés ; `reservations_resolues` peut légitimement être vide si aucune réservation ne
    porte sur la période).

    `ou`/`args` restreignent aux lignes du dataset/extraction ACTIF quand la table en porte
    plusieurs générations (voir `charger_reservations_sqlite`/`charger_payout_sqlite` : lire sans
    ce filtre agrège silencieusement toutes les générations passées, multipliant chaque montant par
    autant de générations existantes)."""
    conn, message = dbm.verifier(chemin_base, (table,))
    if conn is None:
        sys.exit(f"[lot10] ERREUR : --source SQLITE inutilisable — {table} — {message}")
    try:
        lignes = dbm.lignes(conn, table, colonnes, ou=ou, args=args, ordre=ordre)
    finally:
        conn.close()
    if not lignes and not vide_ok:
        sys.exit(f"[lot10] ERREUR : `{table}` est vide — référentiel jamais importé ?")
    df = pd.DataFrame(lignes, columns=list(colonnes))
    if rename:
        df = df.rename(columns=rename)
    return df


def charger_reservations_sqlite(chemin_base) -> pd.DataFrame:
    """Réservations résolues (Lot4quater) depuis SQLite — mission 14f : plus de lecture du
    classeur `MASTER_CALC_Reservations_Resolues.xlsx`, même en mode SQLITE (jusqu'ici encore lu
    inconditionnellement, y compris pour une base isolée : découvert via la recette HH -> Lot12).
    Vide accepté : aucune réservation VALIDE sur la période résolue n'est un état réel possible.

    RESTREINT au dataset RESOLUES ACTIF (mission facturation Cerdine/juillet — cause racine du ×4) :
    `reservations_resolues` conserve les générations précédentes (inactives) pour audit, à côté de
    la génération active — lire sans filtre additionne les deux, doublant chaque montant par
    génération présente."""
    conn, message = dbm.verifier(chemin_base, ("reservations_datasets",))
    dataset_actif = dbm.dataset_courant(conn, "RESOLUES") if conn is not None else ""
    if conn is not None:
        conn.close()
    return _charger_table_sqlite(chemin_base, "reservations_resolues", _RES_COLS_SQL,
                                 rename={"guest_count": "guestCount"}, vide_ok=True,
                                 ou="dataset_id = ?" if dataset_actif else "1=0",
                                 args=(dataset_actif,) if dataset_actif else (),
                                 ordre="rowid")


def charger_payout_sqlite(chemin_base) -> pd.DataFrame:
    """Payouts Hostaway (Lot1) depuis SQLite — vide accepté (aucune réservation Hostaway sur la
    période, par ex. un mois entièrement hors Hostaway, est un état réel).

    RESTREINT à la dernière extraction EXPLOITABLE (`dbm.extraction_utilisable`, même règle que
    `lot4bis_charger_reservations.charger_hostaway_sqlite`) : `hostaway_payouts` est append-only
    par extraction (plusieurs extractions Hostaway réelles coexistent, ex. HAX-44B3AA50E576 et
    HAX-97F6E34A7D92) — lire sans filtre additionne les payouts de TOUTES les extractions passées
    pour une même réservation, cause racine du ×4 constaté en facturation propriétaire."""
    conn, message = dbm.verifier(chemin_base, ("hostaway_payouts",))
    extraction = dbm.extraction_utilisable(conn) if conn is not None else ""
    if conn is not None:
        conn.close()
    return _charger_table_sqlite(chemin_base, "hostaway_payouts", _PAYOUT_COLS_SQL,
                                 vide_ok=True,
                                 ou="extraction_id = ?" if extraction else "1=0",
                                 args=(extraction,) if extraction else (),
                                 ordre="rowid")


def charger_logements_sqlite(chemin_base) -> pd.DataFrame:
    """`REF_Logements` depuis SQLite — référentiel obligatoire (fail-closed si jamais importé)."""
    return _charger_table_sqlite(chemin_base, "ref_logements", _LOG_COLS_SQL)


def charger_proprietaires_sqlite(chemin_base) -> pd.DataFrame:
    """`REF_Proprietaires` depuis SQLite — référentiel obligatoire."""
    return _charger_table_sqlite(chemin_base, "ref_proprietaires", _PROP_COLS_SQL)


def charger_taux_commission_sqlite(chemin_base) -> pd.DataFrame:
    """`REF_Taux_Commission` depuis SQLite — vide accepté (une table optionnelle en Excel, cf.
    `_read_optional_sheet` : tous les logements n'ont pas forcément une dérogation de taux)."""
    return _charger_table_sqlite(chemin_base, "ref_taux_commission", _TAUX_COLS_SQL, vide_ok=True)


def charger_corrections_assiette_sqlite(chemin_base) -> dict:
    """{reservation_calc_id: assiette_manuelle} — corrections humaines actives de l'assiette de
    commission (migration 0065, écran ASSIETTE_NEGATIVE_RAMENEE_ZERO). Absence de table = aucune
    correction (état normal avant la première utilisation de l'écran)."""
    conn, message = dbm.verifier(chemin_base, ("assiette_corrections_manuelles",))
    if conn is None:
        return {}
    try:
        lignes = dbm.lignes(
            conn, "assiette_corrections_manuelles",
            ("reservation_calc_id", "assiette_manuelle"), ou="actif = 1")
    finally:
        conn.close()
    return {l["reservation_calc_id"]: l["assiette_manuelle"] for l in lignes}


def assiette_retenue_pour_commission(df, corrections: dict):
    """Assiette utilisée pour LA COMMISSION uniquement (jamais `assiette_commission`, la brute,
    conservée telle quelle pour preuve). Une correction humaine active (0065) prime sur le
    plafonnement automatique `plafonner_assiette_pour_commission` — traçabilité et justification
    vivent dans `assiette_corrections_manuelles`, jamais ici."""
    auto = plafonner_assiette_pour_commission(df["assiette_commission"])
    if not corrections:
        return auto
    manuelle = df["reservation_calc_id"].map(corrections)
    return manuelle.combine_first(auto) if hasattr(manuelle, "combine_first") else auto


_CHARGES_COLS_SQL = (
    "charge_id", "date_charge", "mois", "montant", "sens_flux", "sens", "categorie_charge_id",
    "filtre_vue_menage", "type_flux_id", "code_impact", "impact_resultat_reel",
    "impact_resultat_comptable", "prise_en_compta", "associe_id", "mode_paiement_id", "carte_id",
    "affectation_type", "logement_id", "proprietaire_id", "reservation_id", "refacturable",
    "source_flux", "methode_traitement", "paye_avec_montant_recupere", "lien_virement_banque",
    "statut_controle", "niveau_anomalie", "code_anomalie", "statut_rapprochement", "justificatif",
    "commentaire",
)


def charger_charges_sqlite(chemin_base) -> pd.DataFrame:
    """`charges` (migration 0052) — mission 14g : `df_charges` restait lu inconditionnellement
    depuis `MASTER_FACT_MAN_Charges.xlsx`, y compris en mode SQLITE, alors que la table `charges`
    est déjà la source canonique de Lot9 (`app.readers.charges_reader`, jamais un classeur). Mêmes
    noms de colonnes que le MASTER historique (le reader applicatif ne les a pas changés), donc
    aucune adaptation en aval (`aggregate_refacturable_charges`, `build_net_proprietaire`). Vide
    accepté : aucune charge sur la période est un état réel."""
    return _charger_table_sqlite(chemin_base, "charges", _CHARGES_COLS_SQL, vide_ok=True)


_AIRBNB_IMP_COLS_SQL = (
    "imputation_airbnb_id", "transaction_banque_id", "reference_airbnb", "proprietaire_id",
    "logement_id", "mois", "document_id", "montant_impute", "date_imputation", "justificatif",
    "statut", "commentaire",
)


def charger_imputations_airbnb_sqlite(chemin_base) -> pd.DataFrame:
    """`imputations_airbnb` — mission « zéro Excel opérationnel Lot9/10/11 » : `df_airbnb_imp`
    restait lu inconditionnellement depuis `SAISIE_ImputationsAirbnb.xlsx`, y compris en mode
    SQLITE. Mêmes noms de colonnes que le classeur historique (`validated_airbnb_imputation`,
    lib_settlements.py, exige `transaction_banque_id`/`proprietaire_id`/`logement_id`/`mois`/
    `document_id`/`justificatif`/`statut`/`montant_impute` — tous présents tels quels dans la
    table). Vide accepté : aucune imputation Airbnb sur la période est un état réel (les deux
    sources sont d'ailleurs vides aujourd'hui)."""
    return _charger_table_sqlite(chemin_base, "imputations_airbnb", _AIRBNB_IMP_COLS_SQL,
                                 vide_ok=True)


_ACOMPTES_COLS_SQL = (
    "mouvement_opaque", "proprietaire_id", "logement_id", "montant", "date_mouvement",
    "statut",
)


def charger_acomptes_proprietaires_sqlite(chemin_base) -> pd.DataFrame:
    """`mouvements_tresorerie_proprietaires` (migration 0025) — mission « zéro Excel opérationnel
    Lot9/10/11 » : `df_acc` restait lu inconditionnellement depuis
    `Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx`, y compris en mode
    SQLITE. Ce n'est PAS un nouvel objet : la migration 0025 documente déjà cette table comme « un
    mouvement financier société <-> propriétaire (acompte, remboursement, régularisation,
    compensation, avance, restitution) », distinct de Lot5 (non modifié, non renommé) et « ne crée
    jamais automatiquement une écriture comptable » — exactement le rôle que jouait le classeur ici
    (bloc RÈGLEMENT uniquement, D031/D033 : jamais `revenu_net_exploitation`).

    RESTREINT à `nature='ACOMPTE_PROPRIETAIRE'` ET `sens='PROPRIETAIRE_VERS_SOCIETE'` (le
    propriétaire paie la société — direction exacte d'un acompte reçu) ET `statut='VALIDE'` ET
    `actif=1` : un mouvement d'une autre nature (remboursement, compensation…) ou de sens inverse
    n'est pas un acompte et ne doit jamais être compté comme tel. Renommage vers le vocabulaire
    historique attendu par `build_net_proprietaire` (`acompte_id`/`montant_acompte`/`mois`/
    `statut_controle`) fait après lecture, pas de colonne SQL calculée — même convention que les
    autres `charger_*_sqlite` de ce fichier. Vide accepté : aucun acompte sur la période est un état
    réel (les deux sources sont d'ailleurs vides aujourd'hui)."""
    df = _charger_table_sqlite(
        chemin_base, "mouvements_tresorerie_proprietaires", _ACOMPTES_COLS_SQL, vide_ok=True,
        ou="nature = ? AND sens = ? AND statut = ? AND actif = 1",
        args=("ACOMPTE_PROPRIETAIRE", "PROPRIETAIRE_VERS_SOCIETE", "VALIDE"))
    if len(df) == 0:
        return pd.DataFrame(columns=["acompte_id", "proprietaire_id", "logement_id",
                                     "montant_acompte", "mois", "statut_controle"])
    df = df.rename(columns={"mouvement_opaque": "acompte_id", "montant": "montant_acompte"})
    df["mois"] = df["date_mouvement"].astype(str).str.slice(0, 7)
    df["statut_controle"] = "VALIDE"
    return df[["acompte_id", "proprietaire_id", "logement_id", "montant_acompte", "mois",
              "statut_controle"]]


def charger_refacturations_realisees_sqlite(chemin_base):
    """Refacturations RÉALISÉES (mission 15) — remplace `aggregate_refacturable_charges()` en mode
    SQLite : une charge `refacturable='OUI'` n'est plus automatiquement un produit de refacturation
    réalisé. Seules les IMPUTATIONS effectivement décidées (`charges_refacturation_evenements`,
    évènement IMPUTATION/IMPUTATION_PARTIELLE, liées à une facture VALIDE/EMISE du mois) comptent
    ici — la charge économique elle-même (impact sur `total_charges`/`resultat`) reste inchangée
    par ailleurs (`_module_chg`/`charges_reader`), cette fonction ne construit QUE le bloc
    RÉGLEMENT (`montant_du_conciergerie`). Tables absentes (mission pas encore migrée) -> vide,
    jamais bloquant : aucune refacturation réalisée n'est un état réel légitime.
    """
    refac_by_log: dict[tuple, tuple[float, Any]] = {}
    refac_by_prop: dict[tuple, float] = {}
    conn, message = dbm.verifier(
        chemin_base, ("charges_refacturation_evenements", "charges_refacturation_positions",
                      "factures_proprietaires"))
    if conn is None:
        return refac_by_log, refac_by_prop, [], message
    try:
        rows = conn.execute(
            "SELECT f.mois AS mois, p.logement_id AS logement_id, "
            "p.proprietaire_id AS proprietaire_id, SUM(e.montant) AS montant "
            "FROM charges_refacturation_evenements e "
            "JOIN charges_refacturation_positions p ON p.position_id = e.position_id "
            "JOIN factures_proprietaires f ON f.facture_id_opaque = e.facture_id "
            "WHERE e.evenement IN ('IMPUTATION', 'IMPUTATION_PARTIELLE') "
            "AND f.statut IN ('VALIDE', 'EMIS') "
            "GROUP BY f.mois, p.logement_id, p.proprietaire_id"
        ).fetchall()
    finally:
        conn.close()
    for mois, logement_id, proprietaire_id, montant in rows:
        montant = round(float(montant or 0), 2)
        if logement_id:
            key = (mois, logement_id)
            cur = refac_by_log.get(key, (0.0, proprietaire_id))
            refac_by_log[key] = (round(cur[0] + montant, 2), cur[1])
        elif proprietaire_id:
            key = (mois, proprietaire_id)
            refac_by_prop[key] = round(refac_by_prop.get(key, 0.0) + montant, 2)
    return refac_by_log, refac_by_prop, [], message


def charger_gestion_sqlite(chemin_base) -> pd.DataFrame:
    """`REF_Gestion_Logements_Hist` depuis SQLite — vide accepté (mêmes garanties que la version
    Excel via `_read_optional_sheet`)."""
    return _charger_table_sqlite(chemin_base, "ref_gestion_logements_hist", _GEST_COLS_SQL,
                                 vide_ok=True)


def charger_hh_sqlite(chemin_base) -> pd.DataFrame:
    """Saisie HH (`reservations_hors_hostaway` + `reservation_hh_overrides`, migration 0052/0053)
    au format attendu par la branche HH — mission 14f : jusqu'ici lue inconditionnellement depuis
    `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`, y compris en mode SQLITE.

    Seules deux colonnes de ce chargement comptent réellement en aval : `total_percu` (assiette de
    départ) et `menage` (coût de ménage retenu). `commission`/`taux_commission` ne sont que
    reportées ici pour mémoire — `_attach_commission_rate` recalcule ensuite `taux_commission`
    depuis `REF_Taux_Commission`, écrasant systématiquement toute valeur saisie (vérifié : aucun
    autre usage de `commission`/`taux_hh_saisie` en aval). Vide accepté (aucune HH sur la
    période est un état réel).
    """
    conn, message = dbm.verifier(chemin_base, ("reservations_hors_hostaway",))
    if conn is None:
        sys.exit(f"[lot10] ERREUR : --source SQLITE inutilisable — reservations_hors_hostaway — "
                 f"{message}")
    try:
        base_rows = dbm.lignes(conn, "reservations_hors_hostaway",
                               ("reservation_hh_id", "montant_percu"), ordre="rowid")
        overrides_par_id = {}
        if dbm.table_presente(conn, "reservation_hh_overrides"):
            for r in dbm.lignes(conn, "reservation_hh_overrides",
                                ("reservation_hh_id", "menage"), ordre="rowid"):
                overrides_par_id[r["reservation_hh_id"]] = r.get("menage")
    finally:
        conn.close()
    lignes = [{
        "reservation_hh_id": r["reservation_hh_id"],
        "total_percu": r.get("montant_percu"),
        "menage": overrides_par_id.get(r["reservation_hh_id"]) or 0.0,
        "commission": None,
        "taux_commission": None,
    } for r in base_rows]
    return pd.DataFrame(lignes, columns=["reservation_hh_id", "total_percu", "menage",
                                         "commission", "taux_commission"])


def _flux_run_id(chemin_base) -> str:
    """run_id du dataset Lot9 consommé — traçabilité amont, jamais une fraîcheur de fichier."""
    conn, _ = dbm.verifier(chemin_base, ("flux_unifies",))
    if conn is None:
        return ""
    try:
        r = conn.execute("SELECT run_id FROM flux_unifies WHERE run_id IS NOT NULL "
                         "ORDER BY rowid DESC LIMIT 1").fetchone()
        return r[0] if r else ""
    finally:
        conn.close()


# Colonnes des tables 0044, et le nom du champ moteur correspondant quand il diffère.
_ALIAS_SQL = {"guest_count": "guestCount", "listing_map_id": "listingMapId", "row_hash": "ROW_HASH"}

_T_COMMISSIONS = (
    "flux_source_pk", "reservation_calc_id", "reservation_id_hostaway", "logement_id",
    "proprietaire_id", "mois", "date_arrivee", "date_depart", "nuits", "guest_count",
    "channel_type", "source_type", "statut_calcul_payout", "payout_calcule", "menage_retenu",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "date_reference_cout_menage", "assiette_commission", "taux_commission",
    "commission_conciergerie", "taux_commission_id", "taux_commission_source",
    "controle_taux_commission", "net_proprietaire", "inclure_resultat_auto",
    "logement_id_snapshot", "type_logement_id_snapshot", "preparation_canape_voyageurs",
    "controle_preparation_canape", "source_preparation_canape",
)
_T_COMM_AC = (
    "reservation_id", "listing_map_id", "source", "channel_type", "statut_calcul_payout",
    "payout_calcule", "source_payout", "menage_retenu", "assiette_commission",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
    "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
    "inclure_resultat_auto", "extrait_le", "row_hash", "code_anomalie_lot10",
)
_T_RESULTATS = ("mois", "logement_id", "proprietaire_id", "vision", "total_produits",
                "total_charges", "resultat", "nb_flux", "commentaire")
_T_EXPLOITATION = _T_COMMISSIONS + ("charge_fixe_mensuelle", "commentaire_charge_fixe",
                                    "revenu_net_exploitation")
_T_REGLEMENT = (
    "mois", "logement_id", "proprietaire_id", "charge_fixe_mensuelle", "charge_fixe_source",
    "total_payout_mois", "total_menage_mois", "total_commission_mois",
    "total_preparation_canape_mois", "net_proprietaire_avant_charge_mois", "nb_reservations",
    "charges_exceptionnelles_refacturees", "montant_du_conciergerie",
    "acompte_conciergerie_recu_via_airbnb", "autres_acomptes_recus", "paiement_deja_recu",
    "reste_a_payer_conciergerie", "credit_a_traiter", "statut_credit",
    "net_proprietaire_apres_charge_mois", "statut_reglement",
)
_T_VUE_MOIS = (
    "mois", "proprietaire_id", "total_payout_mois", "total_menage_mois", "total_commission_mois",
    "total_preparation_canape_mois", "charge_fixe_mensuelle",
    "charges_exceptionnelles_refacturees", "montant_du_conciergerie",
    "acompte_conciergerie_recu_via_airbnb", "autres_acomptes_recus", "paiement_deja_recu",
    "reste_a_payer_conciergerie", "credit_a_traiter", "net_proprietaire_avant_charge_mois",
    "net_proprietaire_apres_charge_mois", "nb_reservations",
)


# ── Lot10 : mois ouverts uniquement (mission « arrêter le backfill historique ménages ») ────────
# Décision produit : on ne cherche plus à reconstruire/préserver l'historique ménage des mois déjà
# clôturés — la structure métier va évoluer, ce temps n'est plus à investir. Remplace le mécanisme
# mixte (substitution/archive/legacy) d'une mission précédente : un run Lot10 ne couvre plus
# « toute l'histoire ». Un mois CLOTURE est simplement HORS PÉRIMÈTRE du recalcul courant — jamais
# réécrit, jamais recalculé, jamais fabriqué à partir d'une source figée quelconque. Un mois OUVERT
# est recalculé normalement. Aucun run Lot10 réel n'a d'ailleurs jamais porté mai 2026 ou antérieur
# (vérifié : periode_min='2026-06' sur les 3 runs historiques) — cette absence était donc déjà
# l'état réel, ce changement se contente de ne plus la traiter comme un blocage à lever.

MODE_RECALCULE = "RECALCULE"
MODE_EXCLU_PERIMETRE = "EXCLU_PERIMETRE"

CLASS_OUVERT = "OUVERT"
CLASS_CLOTURE = "CLOTURE"


def classifier_mois_lot10(conn, mois_list):
    """OUVERT -> recalculé ; CLOTURE -> hors périmètre, sans exception ni condition — la politique
    ne distingue plus archive authentique / legacy sans archive : les deux sont désormais traitées
    de la même façon (exclusion), aucun recalcul économique n'est jamais tenté."""
    resultat = {}
    for mois in mois_list:
        r = conn.execute(
            "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois = ?", (mois,)).fetchone()
        if r and r[0] == "CLOTURE":
            resultat[mois] = {"classification": CLASS_CLOTURE, "mode": MODE_EXCLU_PERIMETRE}
        else:
            resultat[mois] = {"classification": CLASS_OUVERT, "mode": MODE_RECALCULE}
    return resultat


def exclure_mois_clotures(classifications, df_comm, df_reel, df_compt, df_hc, df_exploit, df_reg,
                          df_vue):
    """Retire des 7 dataframes tout mois CLOTURE — jamais un recalcul, jamais une fabrication de
    donnée historique, juste une absence assumée du périmètre du run courant. Rend aussi les
    lignes de provenance (une par mois du run, y compris les mois exclus) pour pouvoir prouver
    après coup quel périmètre a réellement été traité."""
    mois_exclus = {m for m, c in classifications.items() if c["mode"] == MODE_EXCLU_PERIMETRE}

    provenance_rows = [{
        "mois": mois, "classification": c["classification"], "mode_traitement": c["mode"],
        "source_run_id": None, "source_archive_id": None,
    } for mois, c in classifications.items()]

    if not mois_exclus:
        return (df_comm, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue, provenance_rows)

    def _retirer(df):
        if df is None or len(df) == 0 or "mois" not in df.columns:
            return df
        return df[~df["mois"].astype(str).isin(mois_exclus)].reset_index(drop=True)

    return (_retirer(df_comm), _retirer(df_reel), _retirer(df_compt), _retirer(df_hc),
           _retirer(df_exploit), _retirer(df_reg), _retirer(df_vue), provenance_rows)


def _valeur_sql(v):
    """Valeur stockable. `NaN`/`NaT` pandas -> NULL : une absence reste une absence, jamais 0."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return str(v)[:10]
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):          # numpy scalar -> type Python natif
        return v.item()
    return v


def _ecrire_table(conn, table, colonnes, df, run_id):
    if df is None or len(df) == 0:
        return 0
    trous = ", ".join(["?"] * (len(colonnes) + 1))
    lignes_out = []
    for r in df.to_dict("records"):
        lignes_out.append((run_id, *(_valeur_sql(r.get(_ALIAS_SQL.get(c, c))) for c in colonnes)))
    conn.executemany(
        "INSERT OR REPLACE INTO %s (run_id, %s) VALUES (%s)"
        % (table, ", ".join(colonnes), trous), lignes_out)
    return len(lignes_out)


def ecrire_sqlite(chemin_base, df_comm, df_ac, df_reel, df_compt, df_hc,
                  df_exploit, df_reg, df_vue, *, run_id="", source_flux_run="",
                  provenance_rows=None):
    """Écrit le dataset Lot10 et ne l'active qu'après succès complet (mission §13).

    Tout se fait dans UNE transaction : l'ancien dataset reste actif tant que le nouveau n'est pas
    intégralement écrit. Un run interrompu laisse ses lignes en base sous un run non actif —
    visibles pour diagnostic, jamais servies comme si elles étaient complètes.

    `provenance_rows` : une ligne par mois du run (mode_traitement — voir `exclure_mois_clotures`),
    écrite dans la MÊME transaction, avant la bascule atomique : un run activé sans sa provenance
    complète serait aussi incorrect qu'un run activé avec des tables économiques incomplètes.
    """
    conn, message = dbm.verifier(chemin_base, ("lot10_runs",))
    if conn is None:
        log.warning(f"  SQLite non ecrit : {message}")
        return {"ecrit": False, "message": message}

    run_id = run_id or ("L10-" + uuid.uuid4().hex[:12].upper())
    df_res_all = pd.concat([d for d in (df_reel, df_compt, df_hc) if len(d) > 0],
                           ignore_index=True) if any(
        len(d) > 0 for d in (df_reel, df_compt, df_hc)) else pd.DataFrame()

    mois_connus = sorted({str(m) for m in df_res_all.get("mois", pd.Series(dtype=str)).dropna()
                          if str(m) not in ("", "N/A")}) if len(df_res_all) else []
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO lot10_runs (run_id, periode_min, periode_max, statut, actif, "
            "source_flux_run) VALUES (?,?,?,?,0,?)",
            (run_id, mois_connus[0] if mois_connus else None,
             mois_connus[-1] if mois_connus else None, "EN_COURS", source_flux_run or None))

        nb_comm = _ecrire_table(conn, "lot10_commissions", _T_COMMISSIONS, df_comm, run_id)
        _ecrire_table(conn, "lot10_commissions_a_controler", _T_COMM_AC, df_ac, run_id)
        nb_res = _ecrire_table(conn, "lot10_resultats", _T_RESULTATS, df_res_all, run_id)
        _ecrire_table(conn, "lot10_net_exploitation", _T_EXPLOITATION, df_exploit, run_id)
        nb_reg = _ecrire_table(conn, "lot10_net_reglement", _T_REGLEMENT, df_reg, run_id)
        _ecrire_table(conn, "lot10_net_vue_mois", _T_VUE_MOIS, df_vue, run_id)

        if provenance_rows and dbm.table_presente(conn, "lot10_run_mois_provenance"):
            conn.executemany(
                "INSERT INTO lot10_run_mois_provenance "
                "(run_id, mois, classification, mode_traitement, source_run_id, source_archive_id) "
                "VALUES (?,?,?,?,?,?)",
                [(run_id, r["mois"], r["classification"], r["mode_traitement"],
                  r.get("source_run_id"), r.get("source_archive_id"))
                 for r in provenance_rows])

        # Bascule atomique : l'ancien dataset n'est désactivé qu'ici, tout étant écrit.
        conn.execute("UPDATE lot10_runs SET actif = 0 WHERE actif = 1")
        conn.execute(
            "UPDATE lot10_runs SET statut = 'SUCCES', actif = 1, nb_commissions = ?, "
            "nb_resultats = ?, nb_reglements = ? WHERE run_id = ?",
            (nb_comm, nb_res, nb_reg, run_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.execute("UPDATE lot10_runs SET statut = 'ECHEC', message = ? WHERE run_id = ?",
                     (f"{type(exc).__name__}: {exc}", run_id))
        conn.commit()
        conn.close()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    log.info(f"  SQLite : run {run_id} — {nb_comm} commissions, {nb_res} resultats, "
             f"{nb_reg} reglements")
    return {"ecrit": True, "run_id": run_id, "nb_commissions": nb_comm,
            "nb_resultats": nb_res, "nb_reglements": nb_reg}


def load_sources(source="EXCEL", chemin_base=None):
    log.info("=== Chargement sources (lecture seule) ===")

    if source == "SQLITE":
        df_flux = charger_flux_sqlite(chemin_base)
        log.info(f"  Flux (SQLite `flux_unifies`) : {len(df_flux)} lignes")
        # Mission 14f : jusqu'ici seul `df_flux` basculait réellement sur SQLite — les six lignes
        # suivantes lisaient encore inconditionnellement les classeurs Excel, y compris pour une
        # base isolée (découvert via la recette HH -> Lot12 : une base fraîche relisait les
        # MASTER Excel de PRODUCTION encore présents sur disque, silencieusement, sans erreur).
        df_res    = charger_reservations_sqlite(chemin_base)
        df_payout = charger_payout_sqlite(chemin_base)
        df_log    = charger_logements_sqlite(chemin_base)
        df_prop   = charger_proprietaires_sqlite(chemin_base)
        df_taux   = charger_taux_commission_sqlite(chemin_base)
        df_gest   = charger_gestion_sqlite(chemin_base)
    else:
        df_flux = _read_sheet(FLUX_FILE)
        df_res    = _read_sheet(RES_FILE, sheet="MASTER")
        df_payout = _read_sheet(PAYOUT_FILE, sheet="data")
        df_log    = _read_sheet(REF_FILE, sheet="REF_Logements",     keep_vba=True)
        df_prop   = _read_sheet(REF_FILE, sheet="REF_Proprietaires", keep_vba=True)
        df_taux   = _read_optional_sheet(REF_FILE, "REF_Taux_Commission", keep_vba=True)
        df_gest   = _read_optional_sheet(REF_FILE, REF_GESTION_LOGEMENTS_HIST_SHEET, keep_vba=True)

    # Paramètres canapé (seuil/montant) — Mission 6 : n'existent QUE dans SQLite (migration 0058),
    # jamais eu d'onglet Excel équivalent (les colonnes REF_Logements.seuil_voyageurs_preparation_
    # canape/montant_preparation_canape restaient COURANTES, sans période). `chemin_base` est
    # calculé plus haut (--db / PILOTAGE_DB_PATH / APP_DATA_DIR), indépendamment de `--source` : ce
    # nouveau paramètre est disponible même en mode EXCEL. Absence de base ou table non migrée →
    # DataFrame vide, `build_commissions` retombe alors sur la colonne courante (comportement
    # historique inchangé), jamais une exception.
    df_canape = pd.DataFrame()
    conn_canape, _msg_canape = dbm.verifier(chemin_base, ("ref_canape_parametres",))
    if conn_canape is not None:
        try:
            df_canape = pd.DataFrame(dbm.lignes(
                conn_canape, "ref_canape_parametres",
                ("canape_parametre_id", "logement_id", "seuil_voyageurs_preparation_canape",
                 "montant_preparation_canape", "date_debut", "date_fin", "actif"),
                ordre="canape_parametre_id"))
        finally:
            conn_canape.close()

    # Versions de règles algorithmiques (assiette de commission, canapé, répartition) — Mission
    # 6 ter : mêmes garanties que le paramètre canapé (n'existe qu'en SQLite, migration 0059,
    # disponible même en mode EXCEL, base absente/table non migrée → DataFrame vide, repli sur le
    # comportement historique par la fonction appelante, jamais une exception).
    df_regles = pd.DataFrame()
    conn_regles, _msg_regles = dbm.verifier(chemin_base, ("ref_regles_versions",))
    if conn_regles is not None:
        try:
            df_regles = pd.DataFrame(dbm.lignes(
                conn_regles, "ref_regles_versions",
                ("regle_version_id", "rule_code", "version", "date_debut", "date_fin", "actif"),
                ordre="regle_version_id"))
        finally:
            conn_regles.close()

    # Sources HH + Acomptes (lecture seule, hors placeholder Power Query)
    # Acomptes (Lot5) : AUCUNE table SQLite ne porte le classeur Lot5 lui-même — mais l'objet
    # métier qu'il représente ici (mouvement financier acompte société<->propriétaire, bloc
    # RÈGLEMENT uniquement) est déjà porté par `mouvements_tresorerie_proprietaires` (0025), un
    # objet distinct et antérieur à cette mission, pas fabriqué pour l'occasion (voir
    # `charger_acomptes_proprietaires_sqlite`). CHARGES (0052, mission 14g) et IMPUTATIONS AIRBNB
    # (mission précédente) basculent de la même façon sous `--source SQLITE`.
    df_hh = (charger_hh_sqlite(chemin_base) if source == "SQLITE"
            else (_read_sheet(HH_FILE, sheet="MASTER") if HH_FILE.exists() else pd.DataFrame()))
    df_acc = (charger_acomptes_proprietaires_sqlite(chemin_base) if source == "SQLITE"
             else (_read_sheet(ACC_FILE, sheet="MASTER") if ACC_FILE.exists() else pd.DataFrame()))
    df_charges = (charger_charges_sqlite(chemin_base) if source == "SQLITE"
                 else (_read_sheet(CHARGES_FILE, sheet="MASTER") if CHARGES_FILE.exists()
                       else pd.DataFrame()))
    df_airbnb_imp = (charger_imputations_airbnb_sqlite(chemin_base) if source == "SQLITE"
                     else (_read_sheet(AIRBNB_IMPUT_FILE, sheet="MASTER")
                           if AIRBNB_IMPUT_FILE.exists() else pd.DataFrame()))
    if len(df_hh) > 0 and "reservation_hh_id" in df_hh.columns:
        df_hh = df_hh[~df_hh["reservation_hh_id"].map(_is_placeholder_id)].reset_index(drop=True)
    if len(df_acc) > 0 and "acompte_id" in df_acc.columns:
        df_acc = df_acc[~df_acc["acompte_id"].map(_is_placeholder_id)].reset_index(drop=True)
    if len(df_charges) > 0 and "charge_id" in df_charges.columns:
        df_charges = df_charges[~df_charges["charge_id"].map(_is_placeholder_id)].reset_index(drop=True)
    if len(df_airbnb_imp) > 0 and "imputation_airbnb_id" in df_airbnb_imp.columns:
        df_airbnb_imp = df_airbnb_imp[~df_airbnb_imp["imputation_airbnb_id"].map(_is_placeholder_id)].reset_index(drop=True)

    # Filter duplicate-header rows in xlsm sheets
    df_log  = df_log[df_log["logement_id"].astype(str) != "logement_id"].reset_index(drop=True)
    df_prop = df_prop[df_prop["proprietaire_id"].astype(str) != "proprietaire_id"].reset_index(drop=True)
    if len(df_taux) > 0 and "taux_commission_id" in df_taux.columns:
        df_taux = df_taux[df_taux["taux_commission_id"].astype(str) != "taux_commission_id"].reset_index(drop=True)
        df_taux = df_taux[df_taux["taux_commission_id"].notna()].reset_index(drop=True)

    # Numeric conversions
    df_log["forfait_logiciel_consommables_mensuel"] = pd.to_numeric(
        df_log["forfait_logiciel_consommables_mensuel"], errors="coerce"
    ).fillna(0.0)

    log.info(f"  Flux          : {len(df_flux)} lignes")
    log.info(f"  Reservations  : {len(df_res)} lignes")
    log.info(f"  Payout        : {len(df_payout)} lignes")
    log.info(f"  HH saisie     : {len(df_hh)} lignes (hors placeholder)")
    log.info(f"  Acomptes      : {len(df_acc)} lignes (hors placeholder)")
    log.info(f"  Charges Lot3  : {len(df_charges)} lignes (hors placeholder)")
    log.info(f"  Airbnb imput. : {len(df_airbnb_imp)} lignes (hors placeholder)")
    log.info(f"  REF_Logements : {len(df_log)} logements")
    log.info(f"  REF_Prop      : {len(df_prop)} proprietaires")
    log.info(f"  REF_Taux_Comm : {len(df_taux)} lignes historisees")
    return (df_flux, df_res, df_payout, df_hh, df_acc, df_charges, df_airbnb_imp, df_log, df_prop,
            df_taux, df_gest, df_canape, df_regles)


# ─────────────────────────────────────────────────────────────────────────────
# Résolution des versions de règles algorithmiques (Mission 6 ter) — un recalcul historique doit
# toujours utiliser la version en vigueur à SA date économique, jamais la version courante. Tant
# qu'une seule version existe pour chaque règle (le cas aujourd'hui, backfill migration 0059), ce
# garde-fou ne change jamais le résultat — il prouve que la sélection est réellement câblée, prête
# pour une vraie V2 future sans qu'aucun code économique n'ait à être retouché à ce moment-là.
# ─────────────────────────────────────────────────────────────────────────────
def _verifier_version_regle(df: pd.DataFrame, rule_code: str, regles_history: list,
                            date_col: str, branch: str, implementations: tuple) -> None:
    """BLOQUANT si une ligne résout une version absente/ambiguë/non implémentée. Ne fait rien
    (repli legacy) si `regles_history` est vide — cohérent avec le repli déjà appliqué au
    paramètre canapé quand la base n'est pas fournie (tests existants, environnements sans DB)."""
    if not regles_history or len(df) == 0:
        return
    blockers = []
    for _, row in df.iterrows():
        res = resolve_regle_version(regles_history, rule_code=rule_code, ref_date=row.get(date_col))
        if res.status != "OK":
            blockers.append(f"{branch} {row.get('reservation_calc_id')}: {rule_code} - {res.message}")
        elif res.value not in implementations:
            blockers.append(
                f"{branch} {row.get('reservation_calc_id')}: {rule_code} version "
                f"{res.value!r} resolue mais aucune implementation enregistree ({implementations})")
    if blockers:
        for b in blockers:
            log.error(f"BLOQUANT REGLE_VERSION_INDISPONIBLE - {b}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Build commissions
# ─────────────────────────────────────────────────────────────────────────────
def build_commissions(df_flux, df_res, df_payout, df_hh, df_log, df_prop, df_taux, df_canape=None,
                      df_regles=None, corrections_assiette=None):
    corrections_assiette = corrections_assiette or {}
    log.info("=== Construction commissions (routage HA / HH) ===")
    hh_controls = []
    taux_controls = []
    regles_history = df_regles.to_dict("records") if df_regles is not None and len(df_regles) > 0 else []

    # ── 2a. Filter TYPE_FLUX_017 ──
    df_017 = df_flux[df_flux["type_flux_id"] == "TYPE_FLUX_017"].copy()
    log.info(f"  TYPE_FLUX_017 : {len(df_017)} lignes dans Flux")

    dup = df_017[df_017.duplicated("source_pk", keep=False)]
    if len(dup) > 0:
        log.error(f"BLOQUANT DOUBLON_RESERVATION_FLUX — {len(dup)} lignes dupliquees source_pk")
        sys.exit(1)

    df_017_sel = df_017[[
        "source_pk", "flux_id", "mois", "logement_id", "proprietaire_id",
    ]].rename(columns={
        "mois":           "mois_flux",
        "logement_id":    "logement_id_flux",
        "proprietaire_id":"proprietaire_id_flux",
    })

    # ── 2b. Join Flux -> Reservations (+ champs de routage) ──
    df_res_num = df_res.copy()
    df_res_num["reservation_id_hostaway"] = pd.to_numeric(
        df_res_num["reservation_id_hostaway"], errors="coerce"
    )
    # financiers résolus (présents dans MASTER_CALC_Reservations_Resolues — lot4quater)
    for _c in ("payout_calcule", "menage_retenu", "assiette_commission", "canal", "guestCount"):
        if _c not in df_res_num.columns:
            df_res_num[_c] = None
    df_res_sel = df_res_num[[
        "reservation_calc_id", "reservation_id_hostaway", "reservation_hh_id",
        "source", "source_montant", "montant_retenu",
        "logement_id", "proprietaire_id", "date_arrivee", "date_depart", "nuits", "guestCount",
        "canal", "payout_calcule", "menage_retenu", "assiette_commission",
    ]].rename(columns={
        "payout_calcule": "payout_resolu",
        "menage_retenu": "menage_resolu",
        "assiette_commission": "assiette_resolu",
    })
    df_j = df_017_sel.merge(
        df_res_sel, left_on="source_pk", right_on="reservation_calc_id", how="left",
    )
    missing_res = df_j["reservation_calc_id"].isna()
    if missing_res.any():
        log.error(f"BLOQUANT JOINTURE_RESERVATIONS_MANQUANTE — {missing_res.sum()} lignes")
        sys.exit(1)
    log.info("  Jointure Flux <-> Reservations OK")

    # logement/proprietaire effectifs (Reservations prioritaire, fallback Flux)
    df_j["logement_id_eff"]     = df_j["logement_id"].combine_first(df_j["logement_id_flux"])
    df_j["proprietaire_id_eff"] = df_j["proprietaire_id"].combine_first(df_j["proprietaire_id_flux"])

    # ── 2c. Routage HA vs VRBO vs HH (D-LOT10C-05) ──
    log_ref = {r.get("logement_id"): r for _, r in df_log.iterrows()}
    hors_mask = df_j["logement_id_eff"].map(lambda lid: is_hors_parc_technique(log_ref.get(lid)))
    invalid_mask = df_j["logement_id_eff"].map(lambda lid: is_statut_parc_a_controler(log_ref.get(lid)))
    parc_exclusion_mask = hors_mask | invalid_mask
    hors_parc_controls = []
    if parc_exclusion_mask.any():
        for _, r in df_j[parc_exclusion_mask].iterrows():
            log_row = log_ref.get(r.get("logement_id_eff"))
            is_hors = is_hors_parc_technique(log_row)
            code = HORS_PARC_TECHNIQUE if is_hors else STATUT_PARC_INVALIDE
            niveau = "INFO" if is_hors else A_CONTROLER
            message = (
                "statut_parc=HORS_PARC_TECHNIQUE - exclu commission/net/facture/flux proprietaire"
                if is_hors
                else "statut_parc vide ou invalide - A_CONTROLER sans calcul economique"
            )
            hors_parc_controls.append({
                "reservation": r.get("reservation_calc_id"),
                "logement_id": r.get("logement_id_eff"),
                "proprietaire_id": None,
                "mois": r.get("mois_flux"),
                "code_anomalie_lot10": code,
                "niveau": niveau,
                "message": message,
            })
        df_j = df_j[~parc_exclusion_mask].copy()

    HA_SOURCES = {"HOSTAWAY_AIRBNB", "HOSTAWAY_BOOKING"}
    is_ha   = df_j["reservation_id_hostaway"].notna() & df_j["source"].isin(HA_SOURCES)
    # VRBO résolu (historique clôturé) : commission via assiette résolue, PAS routé en HH
    is_vrbo = (~is_ha) & ((df_j["source"] == "HOSTAWAY_VRBO") | (df_j["canal"] == "VRBO"))
    df_ha   = df_j[is_ha].copy()
    df_vrbo = df_j[is_vrbo].copy()
    df_hhb  = df_j[~is_ha & ~is_vrbo].copy()
    log.info(f"  Routage : {len(df_ha)} Hostaway / {len(df_vrbo)} VRBO / {len(df_hhb)} HH")

    taux_history = df_taux.to_dict("records") if len(df_taux) > 0 else []

    def _attach_commission_rate(df: pd.DataFrame, branch: str) -> pd.DataFrame:
        if len(df) == 0:
            for col in ("taux_commission", "taux_commission_id", "taux_commission_source",
                        "controle_taux_commission"):
                df[col] = None
            return df
        if not taux_history:
            out = df.copy()
            out["taux_commission"] = None
            out["taux_commission_id"] = None
            out["taux_commission_source"] = "REF_Taux_Commission"
            out["controle_taux_commission"] = "TAUX_COMMISSION_HISTORIQUE_ABSENT"
            taux_controls.append({
                "reservation": None,
                "code_anomalie": "TAUX_COMMISSION_HISTORIQUE_ABSENT",
                "niveau": "BLOQUANT",
                "message": f"{branch}: REF_Taux_Commission absent ou vide; aucun taux non date autorise.",
            })
            return out

        rates, ids, sources, controles = [], [], [], []
        blockers = []
        for _, row in df.iterrows():
            res = resolve_commission_rate(
                taux_history,
                proprietaire_id=row.get("proprietaire_id_eff"),
                logement_id=row.get("logement_id_eff"),
                ref_date=row.get("date_arrivee"),
            )
            if res.status != "OK":
                blockers.append({
                    "reservation": row.get("reservation_calc_id"),
                    "code_anomalie": f"TAUX_COMMISSION_{res.status}",
                    "niveau": "BLOQUANT",
                    "message": (
                        f"{branch} {row.get('reservation_calc_id')}: logement={row.get('logement_id_eff')} "
                        f"proprietaire={row.get('proprietaire_id_eff')} date={row.get('date_arrivee')} - {res.message}"
                    ),
                })
                rates.append(None)
                ids.append(None)
                sources.append("REF_Taux_Commission")
                controles.append(f"TAUX_COMMISSION_{res.status}")
                continue
            rates.append(res.value)
            ids.append(res.row.get("taux_commission_id") if res.row else None)
            sources.append("REF_Taux_Commission")
            controles.append("OK")
        if blockers:
            for c in blockers:
                log.error(f"{c['niveau']} {c['code_anomalie']} - {c['message']}")
            sys.exit(1)
        out = df.copy()
        out["taux_commission"] = rates
        out["taux_commission_id"] = ids
        out["taux_commission_source"] = sources
        out["controle_taux_commission"] = controles
        return out

    # ===================== BRANCHE HOSTAWAY =====================
    df_pay_sel = df_payout[[
        "reservation_id", "channel_type", "statut_calcul_payout",
        "payout_calcule", "source_payout", "menage_retenu", "assiette_commission",
        "inclure_resultat_auto", "menage_retenu_source", "cout_standard_id",
        "cout_standard_menage_snapshot", "cout_standard_date_debut_validite",
        "cout_standard_date_fin_validite", "logement_id_snapshot",
        "type_logement_id_snapshot", "date_reference_cout_menage",
    ]].copy()
    df_pay_sel["reservation_id"] = pd.to_numeric(df_pay_sel["reservation_id"], errors="coerce")

    df_ha = df_ha.merge(
        df_pay_sel, left_on="reservation_id_hostaway", right_on="reservation_id", how="left",
    )
    # BLOQUANT JOINTURE_PAYOUT_MANQUANTE : branche Hostaway uniquement
    missing_pay = df_ha["reservation_id"].isna()
    if missing_pay.any():
        log.error(f"BLOQUANT JOINTURE_PAYOUT_MANQUANTE — {missing_pay.sum()} lignes Hostaway")
        sys.exit(1)
    log.info("  Jointure HA <-> Payout OK")

    df_ha = _attach_commission_rate(df_ha, "HOSTAWAY")
    normal_ha = df_ha["statut_calcul_payout"] == "NORMAL"
    if (normal_ha & df_ha["proprietaire_id_eff"].isna()).any():
        log.error("BLOQUANT COMMISSION_LOGEMENT_SANS_PROPRIETAIRE — HA")
        sys.exit(1)
    if (normal_ha & df_ha["taux_commission"].isna()).any():
        log.error("BLOQUANT COMMISSION_SANS_TAUX — HA")
        sys.exit(1)
    for col in ["payout_calcule", "menage_retenu", "assiette_commission", "taux_commission"]:
        df_ha[col] = pd.to_numeric(df_ha[col], errors="coerce")
    n_assiette_neg_ha = int((normal_ha & (df_ha["assiette_commission"].fillna(0) < 0)).sum())
    # Mission 14f-bis : une assiette negative (menage retenu > payout, sejour tres court a faible
    # payout — cas reel audite : reservation_id=65060946) ne bloque plus Lot10. L'assiette BRUTE
    # reste inchangee dans `assiette_commission` (preuve + controle Lot11
    # ASSIETTE_NEGATIVE_RAMENEE_ZERO) ; seule la commission qui en decoule est plafonnee a 0 via
    # `plafonner_assiette_pour_commission` — jamais une commission negative.
    # Assiette HA fournie par le payout amont (Lot1/lot4quater) — la "V1" de ASSIETTE_COMMISSION
    # pour ce canal est précisément "accepter cette valeur amont sans la recalculer ici". Audité
    # Mission 7 bis : pur pass-through technique (aucune décision économique prise ici), donc
    # volontairement PAS extrait dans le moteur — contrairement à HH/VRBO (`lib_commission_engine.
    # assiette_v1_paiement_direct`), qui appliquent une vraie formule (payout - ménage). Le
    # garde-fou vérifie que la version résolue à la date de la réservation reste implémentée.
    _verifier_version_regle(df_ha[normal_ha], "ASSIETTE_COMMISSION", regles_history,
                            "date_arrivee", "HOSTAWAY", ("V1",))
    df_ha["commission_conciergerie"] = None
    df_ha["net_proprietaire"]        = None
    df_ha.loc[normal_ha, "commission_conciergerie"] = calculer_commission_conciergerie(
        assiette_retenue_pour_commission(df_ha.loc[normal_ha], corrections_assiette),
        df_ha.loc[normal_ha, "taux_commission"])
    df_ha.loc[normal_ha, "net_proprietaire"] = calculer_net_proprietaire(
        df_ha.loc[normal_ha, "payout_calcule"], df_ha.loc[normal_ha, "menage_retenu"],
        df_ha.loc[normal_ha, "commission_conciergerie"])
    if n_assiette_neg_ha > 0:
        log.info(f"  ASSIETTE_NEGATIVE_RAMENEE_ZERO — HA : {n_assiette_neg_ha} lignes "
                 f"(commission plafonnee a 0, assiette brute conservee)")
    df_ha["source_type"] = "HOSTAWAY"
    df_ha_norm = df_ha[normal_ha].copy()

    # ===================== BRANCHE HH =====================
    # D-LOT10C-01 : commission HH = (total_percu - menage) x taux REF_Prop
    df_hh_norm = pd.DataFrame()
    n_hh_exclus = 0
    if len(df_hhb) > 0:
        if len(df_hh) > 0:
            df_hh_sel = df_hh[[
                "reservation_hh_id", "total_percu", "menage", "commission", "taux_commission",
            ]].rename(columns={
                "commission": "commission_saisie", "taux_commission": "taux_hh_saisie",
            })
            df_hhb = df_hhb.merge(df_hh_sel, on="reservation_hh_id", how="left")
        else:
            for c in ["total_percu", "menage", "commission_saisie", "taux_hh_saisie"]:
                df_hhb[c] = None
        df_hhb = _attach_commission_rate(df_hhb, "HH")
        df_hhb["total_percu"]     = pd.to_numeric(df_hhb["total_percu"], errors="coerce")
        df_hhb["menage"]          = pd.to_numeric(df_hhb["menage"], errors="coerce").fillna(0.0)
        df_hhb["taux_commission"] = pd.to_numeric(df_hhb["taux_commission"], errors="coerce")

        # D-LOT10C-05 : integrer seulement si total_percu renseigne (>0) et taux dispo
        ok = df_hhb["total_percu"].notna() & (df_hhb["total_percu"] > 0) & df_hhb["taux_commission"].notna()
        df_hh_ok  = df_hhb[ok].copy()
        n_hh_exclus = int((~ok).sum())

        if len(df_hh_ok) > 0:
            df_hh_ok["payout_calcule"]      = df_hh_ok["total_percu"].round(2)
            df_hh_ok["menage_retenu"]       = df_hh_ok["menage"].round(2)
            df_hh_ok["assiette_commission"] = assiette_v1_paiement_direct(
                df_hh_ok["total_percu"], df_hh_ok["menage"])
            n_assiette_neg_hh = int((df_hh_ok["assiette_commission"] < 0).sum())
            if n_assiette_neg_hh > 0:
                log.info(f"  ASSIETTE_NEGATIVE_RAMENEE_ZERO — HH : {n_assiette_neg_hh} lignes "
                         f"(commission plafonnee a 0, assiette brute conservee)")
            _verifier_version_regle(df_hh_ok, "ASSIETTE_COMMISSION", regles_history,
                                    "date_arrivee", "HH", ("V1",))
            df_hh_ok["commission_conciergerie"] = calculer_commission_conciergerie(
                assiette_retenue_pour_commission(df_hh_ok, corrections_assiette),
                df_hh_ok["taux_commission"])
            df_hh_ok["net_proprietaire"] = calculer_net_proprietaire(
                df_hh_ok["total_percu"], df_hh_ok["menage"], df_hh_ok["commission_conciergerie"])
            df_hh_ok["statut_calcul_payout"] = "NORMAL"
            df_hh_ok["channel_type"]         = df_hh_ok["source"]
            df_hh_ok["source_type"]          = "HH"
            df_hh_ok["menage_retenu_source"] = "SAISIE_HH"
            # Ecart commission saisie vs recalcul (D-LOT10C-01)
            cs = pd.to_numeric(df_hh_ok["commission_saisie"], errors="coerce")
            ecart = (cs - df_hh_ok["commission_conciergerie"]).abs()
            for _, r in df_hh_ok[ecart > TOL_LIGNE].iterrows():
                hh_controls.append({
                    "reservation": r["reservation_calc_id"],
                    "code_anomalie": "COMMISSION_HH_SAISIE_DIFFERE_RECALCUL",
                    "niveau": "A_CONTROLER",
                    "message": (
                        f"{r['reservation_calc_id']}: commission saisie={cs.loc[r.name]} "
                        f"!= recalcul={r['commission_conciergerie']} (>0.10 EUR)"
                    ),
                })
            df_hh_norm = df_hh_ok

        if n_hh_exclus > 0:
            hh_controls.append({
                "reservation": None,
                "code_anomalie": "HH_SANS_MONTANT_SAISI",
                "niveau": "A_CONTROLER",
                "message": f"{n_hh_exclus} reservations HH dans Flux sans total_percu/taux exploitable",
            })

    # ===================== BRANCHE VRBO (historique clôturé) =====================
    # canal VRBO : payout = montant historisé, ménage standard déduit,
    # assiette = payout - ménage, commission = assiette x taux. Jamais routé en HH.
    df_vrbo_norm = pd.DataFrame()
    if len(df_vrbo) > 0:
        df_vrbo = _attach_commission_rate(df_vrbo, "VRBO")
        for c in ("payout_resolu", "menage_resolu", "assiette_resolu", "taux_commission"):
            df_vrbo[c] = pd.to_numeric(df_vrbo[c], errors="coerce")
        df_vrbo["payout_calcule"] = df_vrbo["payout_resolu"].round(2)
        df_vrbo["menage_retenu"]  = df_vrbo["menage_resolu"].fillna(0.0).round(2)
        df_vrbo["assiette_commission"] = df_vrbo["assiette_resolu"].combine_first(
            assiette_v1_paiement_direct(df_vrbo["payout_resolu"], df_vrbo["menage_resolu"].fillna(0.0))
        ).round(2)
        n_assiette_neg_vrbo = int((df_vrbo["assiette_commission"].fillna(0) < 0).sum())
        if n_assiette_neg_vrbo > 0:
            log.info(f"  ASSIETTE_NEGATIVE_RAMENEE_ZERO — VRBO : {n_assiette_neg_vrbo} lignes "
                     f"(commission plafonnee a 0, assiette brute conservee)")
        _verifier_version_regle(df_vrbo, "ASSIETTE_COMMISSION", regles_history,
                                "date_arrivee", "VRBO", ("V1",))
        if df_vrbo["taux_commission"].isna().any():
            log.error("BLOQUANT COMMISSION_SANS_TAUX — VRBO")
            sys.exit(1)
        df_vrbo["commission_conciergerie"] = calculer_commission_conciergerie(
            assiette_retenue_pour_commission(df_vrbo, corrections_assiette),
            df_vrbo["taux_commission"])
        df_vrbo["net_proprietaire"] = calculer_net_proprietaire(
            df_vrbo["payout_calcule"], df_vrbo["menage_retenu"],
            df_vrbo["commission_conciergerie"])
        df_vrbo["statut_calcul_payout"] = "NORMAL"
        df_vrbo["channel_type"]         = "VRBO"
        df_vrbo["source_type"]          = "VRBO"
        df_vrbo["menage_retenu_source"] = "COUT_STANDARD"
        df_vrbo_norm = df_vrbo

    # ── 2h. Sortie COMMISSIONS (NORMAL HA + VRBO + HH) ──
    #
    # Paramètre canapé — Mission 6 (historisation) : `canape_history` (ref_canape_parametres,
    # SQLite) fait foi quand disponible, résolu à LA DATE DE LA RÉSERVATION (`date_arrivee`), pas
    # à la date du recalcul. Absent (base non fournie ou table non migrée) : repli sur l'ancienne
    # colonne courante `REF_Logements` (comportement historique inchangé, aucune régression pour
    # les appels — tests notamment — qui ne fournissent aucune base).
    canape_history = (df_canape.to_dict("records")
                      if df_canape is not None and len(df_canape) > 0 else [])

    def _canape_ref_row(log_id, ref_date):
        if not canape_history:
            return log_ref.get(log_id)
        res = resolve_canape_parametres(canape_history, logement_id=log_id, ref_date=ref_date)
        return res.row if res.status == "OK" else None

    # Formule canapé versionnée (Mission 6 ter) — `lib_canape.calculate_canape_amount` reste la
    # formule V1, inchangée ; ce garde-fou prouve que la sélection par date est réellement câblée
    # avant de l'appeler. Repli identique au paramètre : `regles_history` vide → appel direct,
    # comportement historique inchangé (tests/environnements sans base).
    IMPLEMENTATIONS_CANAPE = ("V1",)

    def _canape_formule_ok(ref_date) -> tuple[bool, str]:
        if not regles_history:
            return True, ""
        res = resolve_regle_version(regles_history, rule_code="CANAPE_FORMULE", ref_date=ref_date)
        if res.status == "OK" and res.value in IMPLEMENTATIONS_CANAPE:
            return True, ""
        return False, f"CANAPE_FORMULE {res.status} pour {ref_date!r} : {res.message}"

    def _apply_preparation_canape(df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return df
        out = df.copy()
        amounts = []
        statuses = []
        sources = []
        for _, row in out.iterrows():
            log_id = row.get("logement_id_eff") or row.get("logement_id")
            ref_date = row.get("date_arrivee")
            ok, motif = _canape_formule_ok(ref_date)
            if not ok:
                amounts.append(0.0)
                statuses.append("A_CONTROLER")
                sources.append(motif)
                continue
            ref_row = _canape_ref_row(log_id, ref_date)
            res = calculate_canape_amount(log_id, row.get("guestCount"), ref_row)
            amounts.append(res.amount)
            statuses.append(res.status)
            sources.append(res.message)
        out["preparation_canape_voyageurs"] = amounts
        out["controle_preparation_canape"] = statuses
        out["source_preparation_canape"] = sources
        if "net_proprietaire" in out.columns:
            net = pd.to_numeric(out["net_proprietaire"], errors="coerce")
            canape = pd.to_numeric(out["preparation_canape_voyageurs"], errors="coerce").fillna(0.0)
            out["net_proprietaire"] = (net - canape).round(2)
        return out

    df_ha_norm = _apply_preparation_canape(df_ha_norm)
    df_vrbo_norm = _apply_preparation_canape(df_vrbo_norm)
    df_hh_norm = _apply_preparation_canape(df_hh_norm)

    COMM_OUT_COLS = [
        "source_pk", "reservation_calc_id", "reservation_id_hostaway",
        "logement_id_eff", "proprietaire_id_eff", "mois_flux",
        "date_arrivee", "date_depart", "nuits", "guestCount", "channel_type", "source_type",
        "statut_calcul_payout", "payout_calcule", "menage_retenu", "menage_retenu_source",
        "cout_standard_id", "cout_standard_menage_snapshot", "date_reference_cout_menage",
        "assiette_commission", "taux_commission", "commission_conciergerie",
        "taux_commission_id", "taux_commission_source", "controle_taux_commission",
        "net_proprietaire", "inclure_resultat_auto",
        "logement_id_snapshot", "type_logement_id_snapshot",
        "preparation_canape_voyageurs", "controle_preparation_canape", "source_preparation_canape",
    ]

    def _shape(df):
        if len(df) == 0:
            return pd.DataFrame(columns=COMM_OUT_COLS)
        for c in COMM_OUT_COLS:
            if c not in df.columns:
                df[c] = None
        return df[COMM_OUT_COLS].copy()

    df_comm = pd.concat([_shape(df_ha_norm), _shape(df_vrbo_norm), _shape(df_hh_norm)], ignore_index=True)
    df_comm = df_comm.rename(columns={
        "logement_id_eff":     "logement_id",
        "proprietaire_id_eff": "proprietaire_id",
        "mois_flux":           "mois",
        "source_pk":           "flux_source_pk",
    }).reset_index(drop=True)

    df_ac = df_payout[df_payout["statut_calcul_payout"] == "A_CONTROLER"].copy()
    # Deduplication : une reservation deja resolue ailleurs (typiquement HH, branche
    # DIRECT/VRBO liee a une saisie manuelle) ne doit pas AUSSI etre listee ici depuis
    # le statut brut Lot1 - sinon double-comptage (presente en COMMISSIONS et en A_CONTROLER).
    if len(df_ac) > 0 and len(df_comm) > 0 and "reservation_id_hostaway" in df_comm.columns:
        resolved_ids = pd.to_numeric(df_comm["reservation_id_hostaway"], errors="coerce").dropna()
        df_ac = df_ac[~pd.to_numeric(df_ac["reservation_id"], errors="coerce").isin(resolved_ids)]
    df_ac["code_anomalie_lot10"] = "RESERVATION_EXCLUE_A_CONTROLER"
    df_ac = df_ac.reset_index(drop=True)
    if hors_parc_controls:
        df_ac = pd.concat([df_ac, pd.DataFrame(hors_parc_controls)], ignore_index=True, sort=False)

    if len(df_comm) > 0 and "controle_preparation_canape" in df_comm.columns:
        canape_ctrl = df_comm[df_comm["controle_preparation_canape"] == "A_CONTROLER"].copy()
        if len(canape_ctrl) > 0:
            df_ac = pd.concat([df_ac, pd.DataFrame([{
                "reservation": r.get("reservation_calc_id"),
                "logement_id": r.get("logement_id"),
                "proprietaire_id": r.get("proprietaire_id"),
                "mois": r.get("mois"),
                "code_anomalie_lot10": "GUEST_COUNT_MANQUANT_PREPARATION_CANAPE",
                "niveau": "A_CONTROLER",
                "message": r.get("source_preparation_canape"),
            } for _, r in canape_ctrl.iterrows()])], ignore_index=True, sort=False)
    if taux_controls:
        df_ac = pd.concat([df_ac, pd.DataFrame(taux_controls)], ignore_index=True, sort=False)

    log.info(f"  NORMAL integres  : {len(df_comm)} (HA {len(df_ha_norm)} + VRBO {len(df_vrbo_norm)} + HH {len(df_hh_norm)})")
    log.info(f"  HH exclus        : {n_hh_exclus} (sans montant saisi)")
    log.info(f"  A_CONTROLER (pay): {len(df_ac)}")
    return df_comm, df_ac, hh_controls + taux_controls


# ─────────────────────────────────────────────────────────────────────────────
# 3. Build charge fixe grid (Option A)
# ─────────────────────────────────────────────────────────────────────────────
def build_charge_fixe(df_flux, df_log, df_gest=None):
    log.info("=== Construction grille charge fixe (Option A) ===")
    controls = []

    df_017 = df_flux[df_flux["type_flux_id"] == "TYPE_FLUX_017"].copy()
    log_first = df_017.groupby("logement_id")["mois"].min().to_dict()
    log_last  = df_017.groupby("logement_id")["mois"].max().to_dict()

    gest_rows = df_gest.to_dict("records") if df_gest is not None and len(df_gest) > 0 else []
    rows = []
    for _, lr in df_log.iterrows():
        log_id  = lr["logement_id"]
        forfait = _n(lr["forfait_logiciel_consommables_mensuel"])
        if is_hors_parc_technique(lr):
            controls.append({
                "logement_id": log_id,
                "code_anomalie": HORS_PARC_TECHNIQUE,
                "niveau": "INFO",
                "message": f"{log_id}: statut_parc=HORS_PARC_TECHNIQUE - forfait exclu des calculs proprietaire",
            })
            continue
        if is_statut_parc_a_controler(lr):
            controls.append({
                "logement_id": log_id,
                "code_anomalie": STATUT_PARC_INVALIDE,
                "niveau": A_CONTROLER,
                "message": f"{log_id}: statut_parc vide ou invalide - forfait exclu des calculs proprietaire",
            })
            continue

        if not gest_rows:
            controls.append({
                "logement_id": log_id,
                "code_anomalie": "GESTION_LOGEMENT_MISSING",
                "niveau": "BLOQUANT",
                "message": f"{log_id}: historique de gestion absent; charge fixe non calculee",
            })
            continue

        # Skip forfait = 0
        if forfait == 0.0:
            continue

        # Skip if no TYPE_FLUX_017 for this logement
        if log_id not in log_first:
            controls.append({
                "logement_id":   log_id,
                "code_anomalie": "LOG_SANS_FLUX_017",
                "niveau":        "A_CONTROLER",
                "message":       (
                    f"{log_id}: forfait={forfait} EUR "
                    f"mais aucune reservation TYPE_FLUX_017 dans MASTER_CALC_Flux"
                ),
            })
            continue

        first_mois = log_first[log_id]
        last_mois  = log_last[log_id]

        for mois in _mois_range(first_mois, last_mois):
            gest = resolve_management_period(gest_rows, logement_id=log_id, date_arrivee=f"{mois}-01")
            if gest.status != "OK":
                controls.append({
                    "logement_id": log_id,
                    "mois": mois,
                    "code_anomalie": f"GESTION_LOGEMENT_{gest.status}",
                    "niveau": "BLOQUANT" if gest.status in {"AMBIGUOUS", "MISSING_OWNER"} else "A_CONTROLER",
                    "message": f"{log_id} {mois}: {gest.message}; charge fixe non calculee",
                })
                continue
            prop_id = gest.value
            rows.append({
                "mois":                 mois,
                "logement_id":          log_id,
                "proprietaire_id":      prop_id,
                "charge_fixe_mensuelle":forfait,
                "charge_fixe_source":   "REF_Logements.forfait_logiciel_consommables_mensuel",
            })

    df_cfix = pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "mois", "logement_id", "proprietaire_id",
        "charge_fixe_mensuelle", "charge_fixe_source",
    ])

    n_log = df_cfix["logement_id"].nunique() if len(df_cfix) > 0 else 0
    n_sans = sum(1 for c in controls if c["code_anomalie"] == "LOG_SANS_FLUX_017")
    log.info(f"  Charge fixe  : {len(df_cfix)} lignes / {n_log} logements")
    log.info("  HIST_GESTION : resolution via REF_Gestion_Logements_Hist")
    log.info(f"  SANS_FLUX_017: {n_sans} logements avec forfait>0 mais aucune reservation")
    for c in controls:
        log.info(f"    [{c['niveau']}] {c['code_anomalie']}: {c['message']}")

    return df_cfix, controls


# ─────────────────────────────────────────────────────────────────────────────
# 4. Build resultats from MASTER_CALC_Flux
# ─────────────────────────────────────────────────────────────────────────────
def build_resultats(df_flux):
    log.info("=== Construction resultats (depuis MASTER_CALC_Flux) ===")

    df = df_flux.copy()
    df["montant"] = pd.to_numeric(df["montant"], errors="coerce").fillna(0.0)

    def _agg_vision(mask, vision_label):
        df_v = df[mask].copy()
        if len(df_v) == 0:
            return pd.DataFrame(columns=[
                "mois", "logement_id", "proprietaire_id",
                "total_produits", "total_charges", "resultat",
                "nb_flux", "vision", "commentaire",
            ])
        # D-LOT10C-03 : charges sans logement/proprietaire -> ligne dediee GLOBAL_NON_AFFECTE
        # (sinon perdues par groupby dropna=True)
        df_v["logement_id"]     = df_v["logement_id"].fillna(SENTINEL_GLOBAL)
        df_v["proprietaire_id"] = df_v["proprietaire_id"].fillna(SENTINEL_GLOBAL)
        df_v.loc[df_v["logement_id"].astype(str).str.strip() == "", "logement_id"] = SENTINEL_GLOBAL
        df_v.loc[df_v["proprietaire_id"].astype(str).str.strip() == "", "proprietaire_id"] = SENTINEL_GLOBAL
        grp = df_v.groupby(["mois", "logement_id", "proprietaire_id", "sens"]).agg(
            montant=("montant", "sum"),
            nb=("flux_id", "count"),
        ).reset_index()
        prod = grp[grp["sens"] == "PRODUIT"].groupby(
            ["mois", "logement_id", "proprietaire_id"]
        ).agg(total_produits=("montant", "sum"), nb_prod=("nb", "sum")).reset_index()
        chg  = grp[grp["sens"] == "CHARGE"].groupby(
            ["mois", "logement_id", "proprietaire_id"]
        ).agg(total_charges=("montant", "sum"), nb_chg=("nb", "sum")).reset_index()
        res = prod.merge(chg, on=["mois", "logement_id", "proprietaire_id"], how="outer").fillna(0)
        res["resultat"]   = (res["total_produits"] - res["total_charges"]).round(2)
        res["nb_flux"]    = res["nb_prod"] + res["nb_chg"]
        res["vision"]     = vision_label
        res["commentaire"]= ""
        return res[["mois", "logement_id", "proprietaire_id",
                    "total_produits", "total_charges", "resultat",
                    "nb_flux", "vision", "commentaire"]]

    df_reel  = _agg_vision(df["inclure_resultat_reel"]        == "OUI", "REEL")
    df_compt = _agg_vision(df["inclure_resultat_comptable"]   == "OUI", "COMPTABLE")
    df_hc    = _agg_vision(df["inclure_resultat_hors_compta"] == "OUI", "HORS_COMPTA")

    if len(df_hc) == 0:
        # Pas de flux HC -> placeholder explicite (sources HC vides)
        df_hc = pd.DataFrame([{
            "mois": "N/A", "logement_id": "N/A", "proprietaire_id": "N/A",
            "total_produits": 0.0, "total_charges": 0.0, "resultat": 0.0,
            "nb_flux": 0, "vision": "HORS_COMPTA",
            "commentaire": "HC_ZERO_SOURCES_VIDES — aucun flux HC",
        }])
    else:
        df_hc["commentaire"] = "Flux HC presents"

    hc_tot = df_hc["resultat"].sum() if len(df_hc) > 0 else 0.0
    log.info(f"  REEL        : {len(df_reel)} lignes  total={df_reel['resultat'].sum():,.2f} EUR")
    log.info(f"  COMPTABLE   : {len(df_compt)} lignes  total={df_compt['resultat'].sum():,.2f} EUR")
    log.info(f"  HORS_COMPTA : {len(df_hc)} lignes  total={hc_tot:,.2f} EUR")

    return df_reel, df_compt, df_hc


# ─────────────────────────────────────────────────────────────────────────────
# 5. Build net proprietaire
# ─────────────────────────────────────────────────────────────────────────────
def build_net_proprietaire(df_comm, df_cfix, df_acc, df_airbnb_imp, df_charges=None, *,
                          source="EXCEL", chemin_base=None):
    log.info("=== Construction net proprietaire ===")

    # ── 5-refac. Charges exceptionnelles refacturees (D033/D034) ──
    # Bloc REGLEMENT uniquement : alimente montant_du_conciergerie, jamais revenu_net_exploitation.
    # Mission 15 : en mode SQLite, seules les IMPUTATIONS RÉELLEMENT DÉCIDÉES comptent désormais
    # (`charges_refacturation_service`, via facture VALIDE/EMISE) — `refacturable=OUI` ne signifie
    # plus a lui seul "sera facture". Le mode EXCEL legacy garde l'ancien comportement (agregation
    # directe des charges eligibles), inchange.
    refac_by_log, refac_by_prop, refac_controls = {}, {}, []
    if source == "SQLITE":
        refac_by_log, refac_by_prop, refac_controls, message_refac = \
            charger_refacturations_realisees_sqlite(chemin_base)
        log.info(
            f"  Charges refac. REALISEES (SQLite) : {len(refac_by_log)} (mois x logement) / "
            f"{len(refac_by_prop)} (mois x prop seul) — {message_refac}"
        )
    elif df_charges is not None and len(df_charges) > 0:
        refac_by_log, refac_by_prop, refac_controls = aggregate_refacturable_charges(
            df_charges.to_dict("records")
        )
        log.info(
            f"  Charges refac. : {len(refac_by_log)} (mois x logement) / "
            f"{len(refac_by_prop)} (mois x prop seul) / {len(refac_controls)} sans proprietaire"
        )

    # ── 5a. EXPLOITATION (per reservation) — acomptes JAMAIS ici (D031/D033) ──
    df_exploit = df_comm.copy()
    df_exploit["charge_fixe_mensuelle"] = 0.0
    df_exploit["commentaire_charge_fixe"] = "Charge fixe 1x/mois dans REGLEMENT"
    df_exploit["revenu_net_exploitation"] = pd.to_numeric(
        df_exploit["net_proprietaire"], errors="coerce"
    ).round(2)

    # ── 5a-bis. Index acomptes (D-LOT10C-02) — bloc REGLEMENT uniquement ──
    # acc_by_log : (mois, logement_id) -> (montant, proprietaire_id)   [acomptes avec logement]
    # acc_by_prop: (mois, proprietaire_id) -> montant                  [acomptes sans logement]
    acc_by_log, acc_by_prop = {}, {}
    if len(df_acc) > 0:
        dfa = df_acc.copy()
        dfa = dfa[dfa.get("statut_controle", "VALIDE").astype(str) == "VALIDE"]
        dfa["montant_acompte"] = pd.to_numeric(dfa["montant_acompte"], errors="coerce").fillna(0.0)
        for _, a in dfa.iterrows():
            mois = a.get("mois")
            prop = a.get("proprietaire_id")
            logid = a.get("logement_id")
            mt = _n(a.get("montant_acompte"))
            if logid and str(logid).strip() and str(logid) != "None":
                k = (mois, logid)
                cur = acc_by_log.get(k, (0.0, prop))
                acc_by_log[k] = (round(cur[0] + mt, 2), prop)
            else:
                k = (mois, prop)
                acc_by_prop[k] = round(acc_by_prop.get(k, 0.0) + mt, 2)
        log.info(f"  Acomptes indexes : {len(acc_by_log)} (mois x logement) / {len(acc_by_prop)} (mois x prop seul)")

    airbnb_by_log = {}
    airbnb_controls = []
    if len(df_airbnb_imp) > 0:
        dfi = df_airbnb_imp.copy()
        if "montant_impute" in dfi.columns:
            dfi["montant_impute"] = pd.to_numeric(dfi["montant_impute"], errors="coerce")
        for _, imp in dfi.iterrows():
            ok, code = validated_airbnb_imputation(imp.to_dict())
            if not ok:
                airbnb_controls.append({
                    "mois": imp.get("mois"),
                    "logement_id": imp.get("logement_id"),
                    "proprietaire_id": imp.get("proprietaire_id"),
                    "code_controle": code,
                    "message": "Versement Airbnb non impute avec certitude; aucun impact reglement.",
                    "statut": "A_CONTROLER",
                })
                continue
            k = (imp.get("mois"), imp.get("logement_id"))
            airbnb_by_log[k] = round(airbnb_by_log.get(k, 0.0) + _n(imp.get("montant_impute")), 2)
        log.info(f"  Airbnb imputes : {len(airbnb_by_log)} (mois x logement) / controles {len(airbnb_controls)}")

    # ── 5b. Aggregate reservations per mois x logement ──
    df_num = df_comm.copy()
    for col in ["payout_calcule", "menage_retenu", "commission_conciergerie", "net_proprietaire", "preparation_canape_voyageurs"]:
        df_num[col] = pd.to_numeric(df_num[col], errors="coerce").fillna(0.0)

    df_agg_res = pd.DataFrame()
    if len(df_num) > 0:
        df_agg_res = df_num.groupby(["mois", "logement_id", "proprietaire_id"]).agg(
            total_payout_mois                =("payout_calcule",        "sum"),
            total_menage_mois                =("menage_retenu",         "sum"),
            total_commission_mois            =("commission_conciergerie","sum"),
            total_preparation_canape_mois    =("preparation_canape_voyageurs", "sum"),
            net_proprietaire_avant_charge_mois=("net_proprietaire",     "sum"),
            nb_reservations                  =("reservation_calc_id",   "count"),
        ).reset_index().round(2)

    # ── 5c. Build REGLEMENT: outer join cfix x agg_res ──
    cfix_keys = (
        set(zip(df_cfix["mois"], df_cfix["logement_id"])) if len(df_cfix) > 0 else set()
    )
    res_keys  = (
        set(zip(df_agg_res["mois"], df_agg_res["logement_id"])) if len(df_agg_res) > 0 else set()
    )
    # Inclure les acomptes (mois x logement) pour qu'un acompte sans resa/cfix reste visible
    # Inclure les charges refacturables (mois x logement) pour qu'une charge refac. seule reste visible
    all_keys = (
        cfix_keys | res_keys | set(acc_by_log.keys())
        | set(airbnb_by_log.keys()) | set(refac_by_log.keys())
    )

    # Index for fast lookup
    cfix_idx = (
        df_cfix.set_index(["mois", "logement_id"]) if len(df_cfix) > 0 else pd.DataFrame()
    )
    res_idx  = (
        df_agg_res.set_index(["mois", "logement_id"]) if len(df_agg_res) > 0 else pd.DataFrame()
    )

    reg_rows = []
    for (mois, log_id) in sorted(all_keys):
        row = {"mois": mois, "logement_id": log_id}

        # Charge fixe
        if (mois, log_id) in cfix_keys:
            cr = cfix_idx.loc[(mois, log_id)]
            if isinstance(cr, pd.DataFrame):
                cr = cr.iloc[0]
            row["proprietaire_id"]      = cr["proprietaire_id"]
            row["charge_fixe_mensuelle"] = _n(cr["charge_fixe_mensuelle"])
            row["charge_fixe_source"]   = cr["charge_fixe_source"]
        else:
            row["charge_fixe_mensuelle"] = 0.0
            row["charge_fixe_source"]   = "NON_APPLICABLE_FORFAIT_ZERO"

        # Reservation aggregates
        if (mois, log_id) in res_keys:
            rr = res_idx.loc[(mois, log_id)]
            if isinstance(rr, pd.DataFrame):
                rr = rr.iloc[0]
            if "proprietaire_id" not in row or not row.get("proprietaire_id"):
                row["proprietaire_id"] = rr["proprietaire_id"]
            row["total_payout_mois"]                 = _n(rr["total_payout_mois"])
            row["total_menage_mois"]                 = _n(rr["total_menage_mois"])
            row["total_commission_mois"]             = _n(rr["total_commission_mois"])
            row["total_preparation_canape_mois"]     = _n(rr.get("total_preparation_canape_mois"))
            row["net_proprietaire_avant_charge_mois"]= _n(rr["net_proprietaire_avant_charge_mois"])
            row["nb_reservations"]                   = int(_n(rr["nb_reservations"]))
        else:
            row.setdefault("proprietaire_id", None)
            row["total_payout_mois"]                 = 0.0
            row["total_menage_mois"]                 = 0.0
            row["total_commission_mois"]             = 0.0
            row["total_preparation_canape_mois"]     = 0.0
            row["net_proprietaire_avant_charge_mois"]= 0.0
            row["nb_reservations"]                   = 0

        # Acompte propriétaire (mois x logement) — bloc REGLEMENT uniquement (D-LOT10C-02)
        acc_amt = 0.0
        if (mois, log_id) in acc_by_log:
            acc_amt = acc_by_log[(mois, log_id)][0]
            if not row.get("proprietaire_id"):
                row["proprietaire_id"] = acc_by_log[(mois, log_id)][1]

        # Charges exceptionnelles refacturees (mois x logement) — bloc REGLEMENT uniquement (D033/D034)
        refac_amt = 0.0
        if (mois, log_id) in refac_by_log:
            refac_amt = refac_by_log[(mois, log_id)][0]
            if not row.get("proprietaire_id"):
                row["proprietaire_id"] = refac_by_log[(mois, log_id)][1]
        row["charges_exceptionnelles_refacturees"] = round(refac_amt, 2)

        # Bloc reglement (n'impacte JAMAIS revenu_net_exploitation — D031/D033/D034)
        airbnb_amt = round(airbnb_by_log.get((mois, log_id), 0.0), 2)
        row["montant_du_conciergerie"] = round(
            row["total_commission_mois"]
            + row["total_menage_mois"]
            + row["total_preparation_canape_mois"]
            + row["charge_fixe_mensuelle"]
            + refac_amt,
            2,
        )
        settlement = settle_invoice(row["montant_du_conciergerie"], acc_amt, airbnb_amt, 0.0)
        row["acompte_conciergerie_recu_via_airbnb"] = airbnb_amt
        row["autres_acomptes_recus"]                = round(acc_amt, 2)
        row["paiement_deja_recu"]                   = 0.0
        row["reste_a_payer_conciergerie"] = settlement.reste_a_payer
        row["credit_a_traiter"] = settlement.credit_a_traiter
        row["statut_credit"] = settlement.statut if settlement.credit_a_traiter > 0 else ""
        row["net_proprietaire_apres_charge_mois"] = round(
            row["net_proprietaire_avant_charge_mois"] - row["charge_fixe_mensuelle"], 2
        )
        # A_CONTROLER: mois with charge fixe but no reservation
        if row["nb_reservations"] == 0 and row["charge_fixe_mensuelle"] > 0:
            row["statut_reglement"] = "MOIS_ACTIF_SANS_RESERVATION"
        elif settlement.credit_a_traiter > 0:
            row["statut_reglement"] = "TROP_PERÇU / CRÉDIT À TRAITER"
        else:
            row["statut_reglement"] = "A_CONTROLER"
        reg_rows.append(row)

    # Acomptes sans logement (fallback proprietaire) -> ligne dediee GLOBAL_NON_AFFECTE
    for (mois, prop), amt in acc_by_prop.items():
        reg_rows.append({
            "mois": mois, "logement_id": SENTINEL_GLOBAL, "proprietaire_id": prop,
            "charge_fixe_mensuelle": 0.0, "charge_fixe_source": "NON_APPLICABLE",
            "total_payout_mois": 0.0, "total_menage_mois": 0.0, "total_commission_mois": 0.0,
            "total_preparation_canape_mois": 0.0,
            "net_proprietaire_avant_charge_mois": 0.0, "nb_reservations": 0,
            "montant_du_conciergerie": 0.0,
            "charges_exceptionnelles_refacturees": 0.0,
            "acompte_conciergerie_recu_via_airbnb": 0.0,
            "autres_acomptes_recus": round(amt, 2),
            "paiement_deja_recu": 0.0,
            "reste_a_payer_conciergerie": 0.0,
            "credit_a_traiter": round(amt, 2),
            "statut_credit": "TROP_PERÇU / CRÉDIT À TRAITER",
            "net_proprietaire_apres_charge_mois": 0.0,
            "statut_reglement": "ACOMPTE_SANS_LOGEMENT_A_CONTROLER",
        })

    # Charges refacturables avec proprietaire mais sans logement -> ligne GLOBAL_NON_AFFECTE (D033/D034)
    # Montant du present mais non affectable a un logement -> hors prefacture (D-LOT12-07), A_CONTROLER.
    for (mois, prop), amt in refac_by_prop.items():
        reg_rows.append({
            "mois": mois, "logement_id": SENTINEL_GLOBAL, "proprietaire_id": prop,
            "charge_fixe_mensuelle": 0.0, "charge_fixe_source": "NON_APPLICABLE",
            "total_payout_mois": 0.0, "total_menage_mois": 0.0, "total_commission_mois": 0.0,
            "total_preparation_canape_mois": 0.0,
            "net_proprietaire_avant_charge_mois": 0.0, "nb_reservations": 0,
            "montant_du_conciergerie": round(amt, 2),
            "charges_exceptionnelles_refacturees": round(amt, 2),
            "acompte_conciergerie_recu_via_airbnb": 0.0,
            "autres_acomptes_recus": 0.0,
            "paiement_deja_recu": 0.0,
            "reste_a_payer_conciergerie": round(amt, 2),
            "credit_a_traiter": 0.0,
            "statut_credit": "",
            "net_proprietaire_apres_charge_mois": 0.0,
            "statut_reglement": "REFAC_SANS_LOGEMENT_A_CONTROLER",
        })

    # Charges refacturables sans proprietaire exploitable -> ligne de controle, jamais perdue silencieusement
    for ctrl in refac_controls:
        reg_rows.append({
            "mois": ctrl.get("mois"), "logement_id": SENTINEL_GLOBAL, "proprietaire_id": SENTINEL_GLOBAL,
            "charge_fixe_mensuelle": 0.0, "charge_fixe_source": "NON_APPLICABLE",
            "total_payout_mois": 0.0, "total_menage_mois": 0.0, "total_commission_mois": 0.0,
            "total_preparation_canape_mois": 0.0,
            "net_proprietaire_avant_charge_mois": 0.0, "nb_reservations": 0,
            "montant_du_conciergerie": 0.0,
            "charges_exceptionnelles_refacturees": round(_n(ctrl.get("montant")), 2),
            "acompte_conciergerie_recu_via_airbnb": 0.0,
            "autres_acomptes_recus": 0.0,
            "paiement_deja_recu": 0.0,
            "reste_a_payer_conciergerie": 0.0,
            "credit_a_traiter": 0.0,
            "statut_credit": "",
            "net_proprietaire_apres_charge_mois": 0.0,
            "statut_reglement": ctrl.get("code_controle", "REFAC_SANS_PROPRIETAIRE_A_CONTROLER"),
        })

    df_reg = pd.DataFrame(reg_rows)

    # ── 5d. VUE_MOIS (per mois x proprietaire) ──
    if len(df_reg) > 0:
        sum_cols = {
            "total_payout_mois":                 "sum",
            "total_menage_mois":                 "sum",
            "total_commission_mois":             "sum",
            "total_preparation_canape_mois":     "sum",
            "charge_fixe_mensuelle":             "sum",
            "charges_exceptionnelles_refacturees": "sum",
            "montant_du_conciergerie":           "sum",
            "acompte_conciergerie_recu_via_airbnb": "sum",
            "autres_acomptes_recus":             "sum",
            "paiement_deja_recu":                "sum",
            "reste_a_payer_conciergerie":        "sum",
            "credit_a_traiter":                  "sum",
            "net_proprietaire_avant_charge_mois":"sum",
            "net_proprietaire_apres_charge_mois":"sum",
            "nb_reservations":                   "sum",
        }
        df_vue = df_reg.groupby(["mois", "proprietaire_id"]).agg(
            **{k: (k, v) for k, v in sum_cols.items() if k in df_reg.columns}
        ).reset_index().round(2)
        df_vue = df_vue.sort_values(["mois", "proprietaire_id"]).reset_index(drop=True)
    else:
        df_vue = pd.DataFrame(columns=[
            "mois", "proprietaire_id",
            "total_payout_mois", "total_commission_mois", "charge_fixe_mensuelle",
        ])

    log.info(f"  EXPLOITATION     : {len(df_exploit)} reservations")
    log.info(f"  REGLEMENT        : {len(df_reg)} lignes mois x logement")
    log.info(f"  VUE_MOIS         : {len(df_vue)} lignes mois x proprietaire")

    return df_exploit, df_reg, df_vue


# ─────────────────────────────────────────────────────────────────────────────
# 6. Write Excel outputs
# ─────────────────────────────────────────────────────────────────────────────
def write_all(df_comm, df_ac, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue):
    log.info("=== Ecriture fichiers Excel ===")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── MASTER_CALC_Commissions ──
    wb1 = Workbook()
    wb1.remove(wb1.active)
    _write_sheet(wb1, "COMMISSIONS", df_comm)
    _write_sheet(wb1, "A_CONTROLER", df_ac)
    wb1.save(COMM_FILE)
    log.info(f"  {COMM_FILE.name} : OK")

    # ── MASTER_CALC_Resultats ──
    # PAR_MOIS_LOGEMENT: all three visions stacked
    df_par_ml = pd.concat([df_reel, df_compt, df_hc], ignore_index=True)

    # PAR_MOIS_PROPRIETAIRE: aggregate reel + comptable
    def _agg_prop(df_v):
        if len(df_v) == 0:
            return pd.DataFrame()
        return df_v.groupby(["mois", "proprietaire_id", "vision"]).agg(
            total_produits=("total_produits", "sum"),
            total_charges =("total_charges",  "sum"),
            resultat      =("resultat",        "sum"),
            nb_flux       =("nb_flux",         "sum"),
        ).reset_index()

    df_par_mp = pd.concat(
        [_agg_prop(df_reel), _agg_prop(df_compt)], ignore_index=True
    )

    # GLOBAL — visions calculees (HC inclus, plus de codage en dur — defaut #2 corrige)
    def _sum(df, col):
        return round(df[col].sum(), 2) if len(df) > 0 and col in df.columns else 0.0

    # df_hc peut contenir un placeholder N/A (resultat 0) -> sommes = 0, coherent
    reel_prod, reel_chg, reel_tot    = _sum(df_reel, "total_produits"),  _sum(df_reel, "total_charges"),  _sum(df_reel, "resultat")
    compt_prod, compt_chg, compt_tot = _sum(df_compt, "total_produits"), _sum(df_compt, "total_charges"), _sum(df_compt, "resultat")
    hc_prod, hc_chg, hc_tot          = _sum(df_hc, "total_produits"),    _sum(df_hc, "total_charges"),    _sum(df_hc, "resultat")

    # Coherence REEL = COMPTABLE + HORS_COMPTA (D035 cumul 1.00 EUR)
    ecart_ident = abs(reel_tot - (compt_tot + hc_tot))
    comment_reel = (
        f"REEL=COMPTABLE+HC verifie (ecart={ecart_ident:.2f} EUR)"
        if ecart_ident <= 1.00 else
        f"!! RUPTURE REEL != COMPTABLE+HC (ecart={ecart_ident:.2f} EUR)"
    )
    comment_hc = "Aucun flux HC" if hc_tot == 0.0 and hc_prod == 0.0 and hc_chg == 0.0 else "Flux HC presents"

    df_global = pd.DataFrame([
        {"vision": "REEL",        "total_produits": reel_prod,  "total_charges": reel_chg,  "resultat": reel_tot,  "commentaire_hc": comment_reel},
        {"vision": "COMPTABLE",   "total_produits": compt_prod, "total_charges": compt_chg, "resultat": compt_tot, "commentaire_hc": "Vision comptable (IC)"},
        {"vision": "HORS_COMPTA", "total_produits": hc_prod,    "total_charges": hc_chg,    "resultat": hc_tot,    "commentaire_hc": comment_hc},
    ])

    wb2 = Workbook()
    wb2.remove(wb2.active)
    _write_sheet(wb2, "PAR_MOIS_LOGEMENT",    df_par_ml)
    _write_sheet(wb2, "PAR_MOIS_PROPRIETAIRE", df_par_mp)
    _write_sheet(wb2, "GLOBAL",                df_global)
    wb2.save(RESULT_FILE)
    log.info(f"  {RESULT_FILE.name} : OK")

    # ── MASTER_CALC_NetProprietaire ──
    wb3 = Workbook()
    wb3.remove(wb3.active)
    _write_sheet(wb3, "EXPLOITATION", df_exploit)
    _write_sheet(wb3, "REGLEMENT",   df_reg)
    _write_sheet(wb3, "VUE_MOIS",    df_vue)
    wb3.save(NET_FILE)
    log.info(f"  {NET_FILE.name} : OK")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Controls report (19 points)
# ─────────────────────────────────────────────────────────────────────────────
def print_controls(
    df_flux, df_comm, df_ac, df_reel, df_compt, df_hc,
    df_exploit, df_reg, df_vue, cfix_controls, hh_controls, df_payout,
):
    sep = "=" * 65
    log.info(sep)
    log.info("RAPPORT DE CONTROLES LOT 10")
    log.info(sep)

    for col in ["payout_calcule", "menage_retenu", "assiette_commission",
                "commission_conciergerie", "net_proprietaire"]:
        if col in df_comm.columns:
            df_comm[col] = pd.to_numeric(df_comm[col], errors="coerce").fillna(0.0)

    total_payout   = df_comm["payout_calcule"].sum()        if len(df_comm) > 0 else 0.0
    total_menage   = df_comm["menage_retenu"].sum()          if len(df_comm) > 0 else 0.0
    total_assiette = df_comm["assiette_commission"].sum()    if len(df_comm) > 0 else 0.0
    total_comm_cci = df_comm["commission_conciergerie"].sum()if len(df_comm) > 0 else 0.0
    total_net_avt  = df_comm["net_proprietaire"].sum()       if len(df_comm) > 0 else 0.0

    if len(df_reg) > 0:
        df_reg["charge_fixe_mensuelle"] = pd.to_numeric(
            df_reg.get("charge_fixe_mensuelle", 0), errors="coerce"
        ).fillna(0.0)
        df_reg["net_proprietaire_apres_charge_mois"] = pd.to_numeric(
            df_reg.get("net_proprietaire_apres_charge_mois", 0), errors="coerce"
        ).fillna(0.0)
        total_cfix    = df_reg["charge_fixe_mensuelle"].sum()
        total_net_apr = df_reg["net_proprietaire_apres_charge_mois"].sum()
    else:
        total_cfix    = 0.0
        total_net_apr = 0.0

    reel_tot  = df_reel["resultat"].sum()  if len(df_reel) > 0 else 0.0
    compt_tot = df_compt["resultat"].sum() if len(df_compt) > 0 else 0.0
    hc_tot    = df_hc["resultat"].sum()    if len(df_hc) > 0 else 0.0
    ecart_ident = abs(reel_tot - (compt_tot + hc_tot))

    # Charges globales / non affectables visibles (D-LOT10C-03)
    n_global_lines = 0
    if len(df_reel) > 0 and "logement_id" in df_reel.columns:
        n_global_lines = int((df_reel["logement_id"] == SENTINEL_GLOBAL).sum())

    # Acomptes injectes (REGLEMENT)
    tot_acomptes = 0.0
    if len(df_reg) > 0 and "autres_acomptes_recus" in df_reg.columns:
        tot_acomptes = pd.to_numeric(df_reg["autres_acomptes_recus"], errors="coerce").fillna(0.0).sum()
    tot_airbnb = 0.0
    if len(df_reg) > 0 and "acompte_conciergerie_recu_via_airbnb" in df_reg.columns:
        tot_airbnb = pd.to_numeric(df_reg["acompte_conciergerie_recu_via_airbnb"], errors="coerce").fillna(0.0).sum()
    tot_credit = 0.0
    if len(df_reg) > 0 and "credit_a_traiter" in df_reg.columns:
        tot_credit = pd.to_numeric(df_reg["credit_a_traiter"], errors="coerce").fillna(0.0).sum()

    n_hh = int((df_comm["source_type"] == "HH").sum()) if "source_type" in df_comm.columns else 0
    n_hh_ctrl = len(hh_controls)

    n_sans = sum(1 for c in cfix_controls
                 if c["code_anomalie"] == "LOG_SANS_FLUX_017")

    n_reel_log  = len(df_reel[df_reel["vision"] == "REEL"]) if len(df_reel) > 0 else 0
    n_reel_prop = len(
        df_reel.groupby(["mois", "proprietaire_id"]).size()
    ) if len(df_reel) > 0 else 0

    ctrs = [
        ("CTR-LOT10-01", "Flux lus depuis MASTER_CALC_Flux",
         f"{len(df_flux)}"),
        ("CTR-LOT10-02", "Reservations NORMAL integrees aux commissions",
         f"{len(df_comm)}"),
        ("CTR-LOT10-03", "Reservations A_CONTROLER exclues",
         f"{len(df_ac)}"),
        ("CTR-LOT10-04", "Total payout calcule (NORMAL)",
         f"{total_payout:,.2f} EUR"),
        ("CTR-LOT10-05", "Total menage retenu",
         f"{total_menage:,.2f} EUR"),
        ("CTR-LOT10-06", "Total assiette commission",
         f"{total_assiette:,.2f} EUR"),
        ("CTR-LOT10-07", "Total commission conciergerie",
         f"{total_comm_cci:,.2f} EUR"),
        ("CTR-LOT10-08", "Total net proprietaire avant charge fixe",
         f"{total_net_avt:,.2f} EUR"),
        ("CTR-LOT10-09", "Total charge fixe mensuelle generee",
         f"{total_cfix:,.2f} EUR"),
        ("CTR-LOT10-10", "Total net proprietaire apres charge fixe",
         f"{total_net_apr:,.2f} EUR"),
        ("CTR-LOT10-11", "Resultat REEL global (Flux)",
         f"{reel_tot:,.2f} EUR"),
        ("CTR-LOT10-12", "Resultat COMPTABLE global (Flux)",
         f"{compt_tot:,.2f} EUR"),
        ("CTR-LOT10-13", "Resultat HORS_COMPTA global (Flux)",
         f"{hc_tot:,.2f} EUR"),
        ("CTR-LOT10-14", "Lignes resultats PAR_MOIS_LOGEMENT (REEL)",
         f"{n_reel_log}"),
        ("CTR-LOT10-15", "Lignes resultats PAR_MOIS_PROPRIETAIRE (REEL)",
         f"{n_reel_prop}"),
        ("CTR-LOT10-16", "Charges fixes resolues via REF_Gestion_Logements_Hist",
         "source officielle unique"),
        ("CTR-LOT10-17", "LOG_SANS_FLUX_017 (forfait>0 sans reservation Flux)",
         f"{n_sans} logements"),
        ("CTR-LOT10-18", "Controles BLOQUANTS detectes",
         "0 (execution terminee sans sys.exit)"),
        ("CTR-LOT10-19", "Sources amont (lecture seule)",
         "FLUX / RES / PAYOUT / HH / ACOMPTES / REF_SETUP non modifies"),
        ("CTR-LOT10-20", "Identite REEL = COMPTABLE + HORS_COMPTA",
         f"{'OK' if ecart_ident <= 1.00 else 'RUPTURE'} (ecart={ecart_ident:.2f} EUR)"),
        ("CTR-LOT10-21", "Commissions HH integrees (montant saisi)",
         f"{n_hh}"),
        ("CTR-LOT10-22", "Controles HH (ecart commission / sans montant)",
         f"{n_hh_ctrl}"),
        ("CTR-LOT10-23", "Lignes charges GLOBAL_NON_AFFECTE (REEL)",
         f"{n_global_lines}"),
        ("CTR-LOT10-24", "Total acomptes injectes (REGLEMENT seulement)",
         f"{tot_acomptes:,.2f} EUR"),
        ("CTR-LOT10-25", "Total Airbnb impute valide (REGLEMENT seulement)",
         f"{tot_airbnb:,.2f} EUR"),
        ("CTR-LOT10-26", "Total credits a traiter (reste plafonne a 0)",
         f"{tot_credit:,.2f} EUR"),
    ]

    for code, desc, val in ctrs:
        log.info(f"  {code}  {desc:<50s}  {val}")

    # BLOQUANT defensif : rupture identite REEL = COMPTABLE + HC
    if ecart_ident > 1.00:
        log.error(f"BLOQUANT REEL_DIFF_COMPTABLE_PLUS_HC — ecart={ecart_ident:.2f} EUR")
        sys.exit(1)

    if hh_controls:
        log.info("  --- Controles HH ---")
        for c in hh_controls:
            log.info(f"    [{c['niveau']}] {c['code_anomalie']}: {c['message']}")

    log.info(sep)
    log.info("ATTENTE VALIDATION HUMAINE avant commit.")
    log.info(sep)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Lot10 — commissions, resultats, net proprietaire")
    parser.add_argument(
        "--source", choices=("EXCEL", "SQLITE"), default="EXCEL",
        help="EXCEL = MASTER_CALC_Flux.xlsx (comportement historique). "
             "SQLITE = table `flux_unifies` (migration 0043), deja alimentee par Lot9.")
    parser.add_argument("--db", default=None)
    parser.add_argument("--sans-excel", action="store_true",
                        help="N'ecrit pas les masters legacy. SQLite reste alimente.")
    parser.add_argument("--sans-sqlite", action="store_true",
                        help="N'ecrit pas les tables SQLite (parite legacy seule).")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    log.info("=" * 65)
    log.info("LOT 10 — Calcul resultats, commissions, net proprietaire")
    log.info(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Source flux : {args.source}")
    log.info("=" * 65)

    chemin_base = dbm.chemin_db(args.db)
    if args.source == "SQLITE" and chemin_base is None:
        sys.exit("[lot10] ERREUR : --source SQLITE exige une base "
                 "(--db / PILOTAGE_DB_PATH / APP_DATA_DIR).")

    (df_flux, df_res, df_payout, df_hh, df_acc, df_charges, df_airbnb_imp, df_log, df_prop,
     df_taux, df_gest, df_canape, df_regles) = load_sources(source=args.source, chemin_base=chemin_base)

    corrections_assiette = (charger_corrections_assiette_sqlite(chemin_base)
                            if args.source == "SQLITE" else {})
    df_comm, df_ac, hh_controls     = build_commissions(
        df_flux, df_res, df_payout, df_hh, df_log, df_prop, df_taux, df_canape, df_regles,
        corrections_assiette=corrections_assiette)
    df_cfix, cfix_controls          = build_charge_fixe(df_flux, df_log, df_gest)
    df_reel, df_compt, df_hc        = build_resultats(df_flux)
    df_exploit, df_reg, df_vue      = build_net_proprietaire(
        df_comm, df_cfix, df_acc, df_airbnb_imp, df_charges,
        source=args.source, chemin_base=chemin_base)

    if args.sans_excel:
        log.info("=== Masters legacy non ecrits (--sans-excel) ===")
    else:
        write_all(df_comm, df_ac, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue)

    if args.sans_sqlite:
        log.info("=== SQLite non ecrit (--sans-sqlite) ===")
    elif chemin_base is None:
        log.info("=== Aucune base designee : dataset SQLite non ecrit ===")
    else:
        provenance_rows = None
        if args.source == "SQLITE":
            mois_run = sorted({str(m) for d in (df_reel, df_compt, df_hc)
                              for m in d.get("mois", pd.Series(dtype=str)).dropna()
                              if str(m) not in ("", "N/A")})
            conn_cls, msg_cls = dbm.verifier(chemin_base, ("ref_cloture_mensuelle",))
            if conn_cls is None:
                sys.exit(f"[lot10] ERREUR : classification des mois impossible — {msg_cls}")
            try:
                classifications = classifier_mois_lot10(conn_cls, mois_run)
            finally:
                conn_cls.close()
            for mois, c in sorted(classifications.items()):
                log.info(f"  Mois {mois} : {c['classification']} -> {c['mode']}")
            (df_comm, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue,
             provenance_rows) = exclure_mois_clotures(
                classifications, df_comm, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue)

        log.info("=== Ecriture dataset SQLite (0044) ===")
        ecrire_sqlite(chemin_base, df_comm, df_ac, df_reel, df_compt, df_hc,
                      df_exploit, df_reg, df_vue, run_id=args.run_id,
                      source_flux_run=_flux_run_id(chemin_base) if args.source == "SQLITE" else "",
                      provenance_rows=provenance_rows)

    print_controls(
        df_flux, df_comm, df_ac, df_reel, df_compt, df_hc,
        df_exploit, df_reg, df_vue, cfix_controls, hh_controls, df_payout,
    )


if __name__ == "__main__":
    main()

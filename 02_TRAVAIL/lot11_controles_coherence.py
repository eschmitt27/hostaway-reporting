#!/usr/bin/env python3
"""
lot11_controles_coherence.py
Lot 11 - Controles de coherence globaux (Module 9)

Produit:
    02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx
    Onglets: MASTER / BLOQUANTS_OUVERTS / A_CONTROLER_OUVERTS /
             DASHBOARD_MOIS / RAPPROCHEMENT_PAYOUT_BANQUE / CAISSE_THEORIQUE

Decisions validees:
    D-LOT11-01 Banque adaptative (Option C)
    D-LOT11-02 REF_Cloture_Mensuelle non modifiee (dashboard manuel)
    D-LOT11-03 Rapprochement payout/banque indicatif, non bloquant
    D-LOT11-04 Re-verification independante des controles Lot 10
    D-LOT11-05 CAISSE_THEORIQUE produit meme si sources vides

Sources amont: toutes en lecture seule, aucune modification.

Dependances:
    pip install pandas openpyxl
"""

from pathlib import Path
from datetime import date
from collections import Counter
import re
import openpyxl
import pandas as pd

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------
BASE = Path(__file__).resolve().parent.parent

FLUX_FILE   = BASE / "02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx"
RES_FILE    = BASE / "02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx"  # source résolue (lot4quater)
PAY_FILE    = BASE / "02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx"
COM_FILE    = BASE / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx"
NET_FILE    = BASE / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx"
RSLT_FILE   = BASE / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx"
HA_ANO_FILE = BASE / "02_TRAVAIL/Lot1_Hostaway/MASTER_CTRL_HA_Anomalies.xlsx"
CHG_FILE    = BASE / "02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx"
HH_FILE     = BASE / "02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
ACC_FILE    = BASE / "02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx"
MEX_FILE    = BASE / "02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx"
M04_FILE    = BASE / "02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx"
IK_FILE     = BASE / "02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx"
REF_FILE    = BASE / "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"
BNQ_FILE    = BASE / "02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx"

OUT_DIR     = BASE / "02_TRAVAIL/Lot11_Controles"
OUT_FILE    = OUT_DIR / "MASTER_CTRL_Coherence.xlsx"

TODAY       = str(date.today())
TOLERANCE   = 0.10   # D035

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_sheet(path, sheet=None, keep_vba=False):
    """Lecteur a iterateur unique (evite le bug double-header xlsm)."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_vba=keep_vba)
    ws = wb[sheet] if sheet else wb.active
    it = ws.iter_rows(values_only=True)
    headers = list(next(it))
    rows = [dict(zip(headers, r)) for r in it]
    wb.close()
    return pd.DataFrame(rows)


def _read_ref_sheet(path, sheet, id_col):
    """Lecture REF_Setup.xlsm avec filtre anti-doublon-header."""
    df = _read_sheet(path, sheet=sheet, keep_vba=True)
    df = df[df[id_col].astype(str) != id_col].reset_index(drop=True)
    df = df[df[id_col].notna()].reset_index(drop=True)
    return df


def _ctrl(ctrl_rows, source_module, source_table, source_pk,
          code_controle, severity, message,
          mois=None, logement_id=None, proprietaire_id=None, commentaire=None):
    pk = f"{source_pk or source_module}||{code_controle}"
    ctrl_rows.append({
        "ctrl_pk":         pk,
        "source_module":   source_module,
        "source_table":    source_table,
        "source_pk":       str(source_pk) if source_pk else None,
        "code_controle":   code_controle,
        "severity":        severity,
        "message":         message,
        "mois":            mois,
        "logement_id":     logement_id,
        "proprietaire_id": proprietaire_id,
        "statut_resolution": "OUVERT",
        "commentaire":     commentaire,
        "date_detection":  TODAY,
    })


# ---------------------------------------------------------------------------
# Chargement des sources
# ---------------------------------------------------------------------------

print("Chargement sources...")

df_flux    = _read_sheet(FLUX_FILE, sheet="MASTER")
df_res     = _read_sheet(RES_FILE,  sheet="MASTER")
df_pay     = _read_sheet(PAY_FILE,  sheet="data")
df_com     = _read_sheet(COM_FILE,  sheet="COMMISSIONS")
df_com_ac  = _read_sheet(COM_FILE,  sheet="A_CONTROLER")
df_exploit = _read_sheet(NET_FILE,  sheet="EXPLOITATION")
df_regl    = _read_sheet(NET_FILE,  sheet="REGLEMENT")
df_rslt_g  = _read_sheet(RSLT_FILE, sheet="GLOBAL")
df_rslt_m  = _read_sheet(RSLT_FILE, sheet="PAR_MOIS_LOGEMENT")
df_ha_ano  = _read_sheet(HA_ANO_FILE)
df_chg     = _read_sheet(CHG_FILE,  sheet="MASTER")
df_hh      = _read_sheet(HH_FILE,   sheet="MASTER")
df_acc     = _read_sheet(ACC_FILE,  sheet="MASTER")
df_mex     = _read_sheet(MEX_FILE,  sheet="MASTER")
df_m04     = _read_sheet(M04_FILE,  sheet="MASTER")
df_ik      = _read_sheet(IK_FILE,   sheet="MASTER_CALC_AVANTAGES")

df_log  = _read_ref_sheet(REF_FILE, "REF_Logements",     "logement_id")
df_prop = _read_ref_sheet(REF_FILE, "REF_Proprietaires", "proprietaire_id")
df_map  = _read_ref_sheet(REF_FILE, "REF_Mapping_Logements", "mapping_logement_id")

# Sources vides — vide MÉTIER (F2 corrigé)
# Une source ne contenant que des lignes placeholder Power Query / formule / sans
# vraie clé métier doit être considérée comme VIDE.
_PLACEHOLDER_MARKERS = (
    "power query", "aliment", "charge par", "formule", "requete", "requête",
    "a coller", "à coller", "source_raw", "collage", "remplir", "exemple",
)

def _is_placeholder_value(val):
    """True si la valeur d'une clé n'est pas une vraie clé métier."""
    if val is None:
        return True
    s = str(val).strip()
    if s == "":
        return True
    low = s.lower()
    # Marqueurs techniques explicites
    if any(mk in low for mk in _PLACEHOLDER_MARKERS):
        return True
    # Préfixe technique : commence par #, [, <, ←, -, *
    if s[0] in "#[<←-*":
        return True
    return False

def _is_empty(df, id_col):
    """Vide métier : 0 ligne, ou la colonne clé est absente, ou toutes les
    lignes ont une clé placeholder (header dupliqué, instruction PQ, formule)."""
    if len(df) == 0:
        return True
    if id_col not in df.columns:
        # Pas de colonne clé exploitable -> on regarde si au moins une ligne
        # porte une donnée non triviale
        return df.dropna(how="all").shape[0] == 0
    keys = df[id_col].tolist()
    for k in keys:
        # Ligne header dupliquée -> placeholder
        if str(k) == id_col:
            continue
        if not _is_placeholder_value(k):
            return False  # au moins une vraie clé métier -> non vide
    return True

SOURCES_VIDES = {
    "CHARGES": _is_empty(df_chg, "charge_id"),
    "HH":      _is_empty(df_hh,  "reservation_hh_id"),
    "ACOMPTES":_is_empty(df_acc, "acompte_id"),
    "M04":     _is_empty(df_m04, "menage_calc_id"),
    "IK":      _is_empty(df_ik,  "pk_id"),
}

# Banque adaptative (D-LOT11-01 Option C)
BANQUE_DISPO = BNQ_FILE.exists()
if BANQUE_DISPO:
    try:
        df_bnq      = _read_sheet(BNQ_FILE, sheet="NORM_Banque")
        df_bnq_ctrl = _read_sheet(BNQ_FILE, sheet="CTRL_A_CONTROLER")
        df_cloture  = _read_sheet(BNQ_FILE, sheet="REF_Cloture_Mensuelle")
        df_rapproch_airbnb = _read_sheet(BNQ_FILE, sheet="RAPPROCH_AIRBNB_ATTENTE")
        print(f"  Banque: {len(df_bnq)} lignes NORM chargees.")
    except Exception as e:
        BANQUE_DISPO = False
        print(f"  Banque: erreur lecture -> {e} -> BANQUE_NON_DISPONIBLE_GIT")
else:
    print("  Banque: fichier absent -> BANQUE_NON_DISPONIBLE_GIT")

print(f"  Flux: {len(df_flux)} / Reservations: {len(df_res)} / Payout: {len(df_pay)}")
print(f"  Commissions: {len(df_com)} NORMAL / {len(df_com_ac)} A_CONTROLER")
print(f"  EXPLOITATION: {len(df_exploit)} / REGLEMENT: {len(df_regl)}")
print(f"  Sources vides: {SOURCES_VIDES}")


# ---------------------------------------------------------------------------
# Registre des controles
# ---------------------------------------------------------------------------

ctrl_rows = []

# ==========================================================================
# GROUPE 1 - PK DOUBLONS (transverse)
# ==========================================================================
print("CTR: PK_MANQUANTE_OU_DOUBLONNEE...")

tables_pk = [
    ("FLUX",         df_flux,    "flux_id",            "MASTER_CALC_Flux"),
    ("RESERVATIONS", df_res,     "reservation_calc_id","MASTER_CALC_Reservations"),
    ("PAYOUT",       df_pay,     "reservation_id",     "MASTER_CALC_HA_Payout"),
    ("COMMISSIONS",  df_com,     "flux_source_pk",     "MASTER_CALC_Commissions"),
]

for module, df, pk_col, tbl in tables_pk:
    if pk_col not in df.columns:
        _ctrl(ctrl_rows, module, tbl, None,
              "PK_COLONNE_ABSENTE", "BLOQUANT",
              f"Colonne PK '{pk_col}' absente de {tbl}.")
        continue
    nulls = df[pk_col].isna().sum()
    if nulls > 0:
        _ctrl(ctrl_rows, module, tbl, None,
              "PK_MANQUANTE_OU_DOUBLONNEE", "BLOQUANT",
              f"{tbl}: {nulls} lignes sans PK '{pk_col}'.")
    dupes = df[pk_col].dropna().duplicated()
    n_dupes = dupes.sum()
    if n_dupes > 0:
        examples = df.loc[dupes, pk_col].head(3).tolist()
        _ctrl(ctrl_rows, module, tbl, None,
              "PK_MANQUANTE_OU_DOUBLONNEE", "BLOQUANT",
              f"{tbl}: {n_dupes} PK dupliquees (ex: {examples}).")


# ==========================================================================
# GROUPE 2 - JOINTURES (re-verification independante depuis sources)
# ==========================================================================
print("CTR: Jointures...")

# 2a - Doublon reservation dans Flux TYPE_FLUX_017
flux017 = df_flux[df_flux["type_flux_id"] == "TYPE_FLUX_017"].copy()
dupes_017 = flux017["source_pk"].dropna().duplicated()
n_dupes_017 = dupes_017.sum()
if n_dupes_017 > 0:
    examples = flux017.loc[dupes_017, "source_pk"].head(3).tolist()
    _ctrl(ctrl_rows, "FLUX", "MASTER_CALC_Flux", None,
          "DOUBLON_RESERVATION_FLUX", "BLOQUANT",
          f"{n_dupes_017} source_pk TYPE_FLUX_017 dupliquees (ex: {examples}).")
else:
    print(f"  DOUBLON_RESERVATION_FLUX: 0 (OK)")

# 2b - Jointure Flux TYPE_FLUX_017 -> Reservations.reservation_calc_id
res_ids = set(df_res["reservation_calc_id"].dropna())
flux017_pks = set(flux017["source_pk"].dropna())
manquants_res = flux017_pks - res_ids
if manquants_res:
    for pk in sorted(manquants_res):
        row = flux017[flux017["source_pk"] == pk].iloc[0]
        _ctrl(ctrl_rows, "FLUX", "MASTER_CALC_Flux", pk,
              "JOINTURE_RESERVATIONS_MANQUANTE", "BLOQUANT",
              f"source_pk '{pk}' TYPE_FLUX_017 absent de MASTER_CALC_Reservations.",
              mois=row.get("mois"), logement_id=row.get("logement_id"))
    print(f"  JOINTURE_RESERVATIONS_MANQUANTE: {len(manquants_res)} BLOQUANT")
else:
    print(f"  JOINTURE_RESERVATIONS_MANQUANTE: 0 (OK)")

# 2c - Jointure Reservations (NORMAL pour Flux) -> Payout.reservation_id
res_normal = df_res[df_res["source"].isin(["HOSTAWAY_AIRBNB", "HOSTAWAY_BOOKING"])].copy()
pay_ids = set(df_pay["reservation_id"].dropna().astype(str))

# reservation_id_hostaway de type int ou float dans df_res
res_normal = res_normal.copy()
res_normal["_rid_str"] = res_normal["reservation_id_hostaway"].astype(str).str.split(".").str[0]
manquants_pay = res_normal[~res_normal["_rid_str"].isin(pay_ids)].copy()

if len(manquants_pay) > 0:
    for _, row in manquants_pay.iterrows():
        _ctrl(ctrl_rows, "RESERVATIONS", "MASTER_CALC_Reservations",
              row.get("reservation_calc_id"),
              "JOINTURE_PAYOUT_MANQUANTE", "BLOQUANT",
              f"reservation_id_hostaway '{row['reservation_id_hostaway']}' absent de MASTER_CALC_HA_Payout.",
              mois=row.get("mois"), logement_id=row.get("logement_id"))
    print(f"  JOINTURE_PAYOUT_MANQUANTE: {len(manquants_pay)} BLOQUANT")
else:
    print(f"  JOINTURE_PAYOUT_MANQUANTE: 0 (OK)")

# 2d - Doublon HH dans Reservations (HH vide -> informatif)
if SOURCES_VIDES["HH"]:
    _ctrl(ctrl_rows, "HH", "MASTER_FACT_MAN_ReservationsHorsHostaway", None,
          "HC_ZERO_SOURCES_VIDES", "INFO",
          "MASTER_FACT_MAN_ReservationsHorsHostaway vide. Controles HH non representatifs.",
          commentaire="Sources non encore saisies.")
else:
    hh_res_ids = set(df_hh["reservation_id_hostaway"].dropna().astype(str))
    res_ha_ids = set(df_res["reservation_id_hostaway"].dropna().astype(str).str.split(".").str[0])
    doublons_hh = hh_res_ids & res_ha_ids
    if doublons_hh:
        for rid in sorted(doublons_hh):
            _ctrl(ctrl_rows, "HH", "MASTER_FACT_MAN_ReservationsHorsHostaway", rid,
                  "RESERVATION_DOUBLON_HOSTAWAY_HH", "BLOQUANT",
                  f"reservation_id_hostaway '{rid}' present dans HH ET Reservations HA.")


# ==========================================================================
# GROUPE 3 - COHERENCE EXPLOITATION / REGLEMENT
# ==========================================================================
print("CTR: Coherence exploitation/reglement...")

# 3a - Verifier formule revenu_net_exploitation (payout - menage - commission)
# charge_fixe_mensuelle = 0 par reservation (D-LOT10-01)
df_e = df_exploit.copy()
for col in ["payout_calcule", "menage_retenu", "commission_conciergerie",
            "charge_fixe_mensuelle", "revenu_net_exploitation"]:
    if col not in df_e.columns:
        df_e[col] = 0.0
    df_e[col] = pd.to_numeric(df_e[col], errors="coerce").fillna(0.0)

df_e["_net_calcule"] = (df_e["payout_calcule"]
                        - df_e["menage_retenu"]
                        - df_e["commission_conciergerie"]
                        - df_e["charge_fixe_mensuelle"]).round(2)
df_e["_ecart_net"] = (df_e["revenu_net_exploitation"] - df_e["_net_calcule"]).abs()

incoherents_net = df_e[df_e["_ecart_net"] > TOLERANCE]
if len(incoherents_net) > 0:
    for _, row in incoherents_net.head(10).iterrows():
        _ctrl(ctrl_rows, "EXPLOITATION", "MASTER_CALC_NetProprietaire",
              row.get("flux_source_pk"),
              "REVENU_NET_EXPLOITATION_INCOHERENT", "BLOQUANT",
              f"Ecart {row['_ecart_net']:.4f}E: revenu_net={row['revenu_net_exploitation']}"
              f" != calcule={row['_net_calcule']}",
              mois=row.get("mois"), logement_id=row.get("logement_id"),
              proprietaire_id=row.get("proprietaire_id"))
    print(f"  REVENU_NET_EXPLOITATION_INCOHERENT: {len(incoherents_net)} BLOQUANT")
else:
    print(f"  REVENU_NET_EXPLOITATION_INCOHERENT: 0 (OK)")

# 3b - ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION
# Verifier: revenu_net != reste_a_payer. Si equals -> confusion possible.
df_r2 = df_regl.copy()
for col in ["net_proprietaire_apres_charge_mois", "reste_a_payer_conciergerie"]:
    if col not in df_r2.columns:
        df_r2[col] = 0.0
    df_r2[col] = pd.to_numeric(df_r2[col], errors="coerce").fillna(0.0)

if "acompte_conciergerie_recu_via_airbnb" in df_r2.columns:
    df_r2["_acompte"] = pd.to_numeric(df_r2["acompte_conciergerie_recu_via_airbnb"],
                                       errors="coerce").fillna(0.0)
    lignes_avec_acompte = df_r2[df_r2["_acompte"] > 0]
    for _, row in lignes_avec_acompte.iterrows():
        net = row.get("net_proprietaire_apres_charge_mois", 0)
        rp  = row.get("reste_a_payer_conciergerie", 0)
        if abs(net - rp) < TOLERANCE:
            _ctrl(ctrl_rows, "EXPLOITATION", "MASTER_CALC_NetProprietaire",
                  f"{row.get('mois')}|{row.get('logement_id')}",
                  "ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION", "BLOQUANT",
                  f"net_proprietaire_apres_charge={net} == reste_a_payer={rp} "
                  f"alors qu'un acompte de {row['_acompte']} est present -> confusion possible.",
                  mois=row.get("mois"), logement_id=row.get("logement_id"),
                  proprietaire_id=row.get("proprietaire_id"))

# 3c - CONFUSION_PAYOUT_SOLDE_FACTURE (net != reste_a_payer attendu)
# Structure OK: net_prop = resultat exploitation, reste_a_payer = bloc reglement
# Les deux ne doivent pas etre identiques sauf si commission = 0 et charge = 0
df_r3 = df_regl.copy()
for col in ["montant_du_conciergerie", "net_proprietaire_apres_charge_mois",
            "reste_a_payer_conciergerie", "total_payout_mois"]:
    if col not in df_r3.columns:
        df_r3[col] = 0.0
    df_r3[col] = pd.to_numeric(df_r3[col], errors="coerce").fillna(0.0)

# Verifie que total_payout n'est pas confondu avec reste_a_payer
confusion = df_r3[
    (df_r3["total_payout_mois"] > 0) &
    (df_r3["reste_a_payer_conciergerie"] == df_r3["total_payout_mois"])
]
if len(confusion) > 0:
    for _, row in confusion.iterrows():
        _ctrl(ctrl_rows, "EXPLOITATION", "MASTER_CALC_NetProprietaire",
              f"{row.get('mois')}|{row.get('logement_id')}",
              "CONFUSION_PAYOUT_SOLDE_FACTURE", "BLOQUANT",
              f"reste_a_payer={row['reste_a_payer_conciergerie']} == total_payout={row['total_payout_mois']}"
              f" -> commission/charge fixe non deduits du solde.",
              mois=row.get("mois"), logement_id=row.get("logement_id"),
              proprietaire_id=row.get("proprietaire_id"))
    print(f"  CONFUSION_PAYOUT_SOLDE_FACTURE: {len(confusion)} BLOQUANT")
else:
    print(f"  CONFUSION_PAYOUT_SOLDE_FACTURE: 0 (OK)")

# 3d - CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE
# Verifier que charge_fixe_mensuelle dans REGLEMENT = forfait REF_Logements
forfait_ref = {r["logement_id"]: float(r.get("forfait_logiciel_consommables_mensuel") or 0)
               for _, r in df_log.iterrows()}

df_r4 = df_regl.copy()
df_r4["_cf"] = pd.to_numeric(df_r4["charge_fixe_mensuelle"], errors="coerce").fillna(0.0)
for _, row in df_r4.iterrows():
    lid = row.get("logement_id")
    cf_val = row["_cf"]
    ref_val = forfait_ref.get(lid, 0.0)
    if lid and abs(cf_val - ref_val) > TOLERANCE:
        _ctrl(ctrl_rows, "EXPLOITATION", "MASTER_CALC_NetProprietaire",
              f"{row.get('mois')}|{lid}",
              "CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE", "BLOQUANT",
              f"Logement {lid} mois {row.get('mois')}: charge_fixe={cf_val} "
              f"!= REF_Logements.forfait={ref_val}.",
              mois=row.get("mois"), logement_id=lid,
              proprietaire_id=row.get("proprietaire_id"))

# 3e - PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT
# paiement_deja_recu ne doit pas etre soustrait du payout; seulement de reste_a_payer
if "paiement_deja_recu" in df_r2.columns:
    df_r2["_pdr"] = pd.to_numeric(df_r2["paiement_deja_recu"], errors="coerce").fillna(0.0)
    # Le signe attendu: total_payout ne doit pas diminuer de paiement_deja_recu
    # Impossible a verifier avec les colonnes disponibles -> check heuristique:
    # si paiement_deja_recu > 0 et total_payout_mois < payout attendu (pas de ref directe) -> skip
    # Controle formel: paiement_deja_recu in REGLEMENT n'est pas soustrait du payout
    # -> structure vérifiée: total_payout_mois = somme payout_calcule des reservations du mois
    #    et paiement_deja_recu reduit seulement reste_a_payer_conciergerie
    # -> verifier: total_payout != total_payout - paiement (i.e. paiement pas deduit du payout)
    # Heuristique: si paiement > 0 et reste_a_payer = montant_du - acompte - paiement (OK)
    #              vs si reste_a_payer = total_payout - menage - com - acompte - paiement (ERREUR)
    # Formule attendue: reste_a_payer = montant_du - acomptes - paiements
    for col in ["montant_du_conciergerie", "acompte_conciergerie_recu_via_airbnb",
                "autres_acomptes_recus", "reste_a_payer_conciergerie"]:
        if col not in df_r2.columns:
            df_r2[col] = 0.0
        df_r2[col] = pd.to_numeric(df_r2[col], errors="coerce").fillna(0.0)

    df_r2["_rp_calcule"] = (df_r2["montant_du_conciergerie"]
                            - df_r2["acompte_conciergerie_recu_via_airbnb"]
                            - df_r2["autres_acomptes_recus"]
                            - df_r2["_pdr"]).round(2)
    df_r2["_ecart_rp"] = (df_r2["reste_a_payer_conciergerie"] - df_r2["_rp_calcule"]).abs()
    incoherents_rp = df_r2[df_r2["_ecart_rp"] > TOLERANCE]
    if len(incoherents_rp) > 0:
        for _, row in incoherents_rp.iterrows():
            _ctrl(ctrl_rows, "EXPLOITATION", "MASTER_CALC_NetProprietaire",
                  f"{row.get('mois')}|{row.get('logement_id')}",
                  "PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT", "BLOQUANT",
                  f"Ecart {row['_ecart_rp']:.4f}E dans formule reste_a_payer.",
                  mois=row.get("mois"), logement_id=row.get("logement_id"),
                  proprietaire_id=row.get("proprietaire_id"))
        print(f"  PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT: {len(incoherents_rp)} BLOQUANT")
    else:
        print(f"  PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT: 0 (OK)")


# ==========================================================================
# GROUPE 4 - COHERENCE ARITHMETIQUE COMMISSIONS
# ==========================================================================
print("CTR: Coherence arithmetique commissions...")

df_c = df_com.copy()
for col in ["assiette_commission", "taux_commission", "commission_conciergerie",
            "payout_calcule", "menage_retenu", "net_proprietaire"]:
    if col not in df_c.columns:
        df_c[col] = 0.0
    df_c[col] = pd.to_numeric(df_c[col], errors="coerce").fillna(0.0)

# Verifier commission = round(assiette * taux, 2)
mask_normal = df_c["taux_commission"] > 0
df_cn = df_c[mask_normal].copy()
df_cn["_com_calcule"] = (df_cn["assiette_commission"] * df_cn["taux_commission"]).round(2)
df_cn["_ecart_com"]   = (df_cn["commission_conciergerie"] - df_cn["_com_calcule"]).abs()

incoherents_com = df_cn[df_cn["_ecart_com"] > TOLERANCE]
if len(incoherents_com) > 0:
    for _, row in incoherents_com.head(10).iterrows():
        _ctrl(ctrl_rows, "COMMISSIONS", "MASTER_CALC_Commissions",
              row.get("flux_source_pk"),
              "COMMISSION_INCOHERENTE", "BLOQUANT",
              f"commission={row['commission_conciergerie']} != "
              f"assiette({row['assiette_commission']})*taux({row['taux_commission']})="
              f"{row['_com_calcule']} (ecart={row['_ecart_com']:.4f}E)",
              mois=row.get("mois"), logement_id=row.get("logement_id"),
              proprietaire_id=row.get("proprietaire_id"))
    print(f"  COMMISSION_INCOHERENTE: {len(incoherents_com)} BLOQUANT")
else:
    print(f"  COMMISSION_INCOHERENTE: 0 (OK)")

# Verifier assiette = payout - menage
df_cn2 = df_c.copy()
df_cn2["_assiette_calcule"] = (df_cn2["payout_calcule"] - df_cn2["menage_retenu"]).round(2)
df_cn2["_ecart_assiette"]   = (df_cn2["assiette_commission"] - df_cn2["_assiette_calcule"]).abs()
incoherents_ass = df_cn2[df_cn2["_ecart_assiette"] > TOLERANCE]
if len(incoherents_ass) > 0:
    for _, row in incoherents_ass.head(10).iterrows():
        _ctrl(ctrl_rows, "COMMISSIONS", "MASTER_CALC_Commissions",
              row.get("flux_source_pk"),
              "ASSIETTE_COMMISSION_INCOHERENTE", "BLOQUANT",
              f"assiette={row['assiette_commission']} != payout({row['payout_calcule']})"
              f"-menage({row['menage_retenu']})={row['_assiette_calcule']}",
              mois=row.get("mois"), logement_id=row.get("logement_id"),
              proprietaire_id=row.get("proprietaire_id"))
    print(f"  ASSIETTE_COMMISSION_INCOHERENTE: {len(incoherents_ass)} BLOQUANT")
else:
    print(f"  ASSIETTE_COMMISSION_INCOHERENTE: 0 (OK)")

# Verifier REEL = COMPTABLE + HORS_COMPTA depuis GLOBAL
rslt_global = {}
for _, row in df_rslt_g.iterrows():
    rslt_global[row.get("vision")] = float(row.get("resultat") or 0)

reel_val = rslt_global.get("REEL", None)
comp_val = rslt_global.get("COMPTABLE", None)
hc_val   = rslt_global.get("HORS_COMPTA", None)

if reel_val is not None and comp_val is not None and hc_val is not None:
    expected_reel = round(comp_val + hc_val, 2)
    if abs(reel_val - expected_reel) > 1.00:  # tolerance D035 cumul 1€
        _ctrl(ctrl_rows, "RESULTATS", "MASTER_CALC_Resultats", "GLOBAL",
              "REEL_INCOHERENT_VS_COMPTABLE_PLUS_HC", "BLOQUANT",
              f"REEL={reel_val} != COMPTABLE({comp_val})+HC({hc_val})={expected_reel}")
        print(f"  REEL_INCOHERENT: BLOQUANT")
    else:
        print(f"  REEL=COMPTABLE+HC: OK ({reel_val} = {comp_val}+{hc_val})")
else:
    print(f"  REEL check: donnees GLOBAL manquantes")


# ==========================================================================
# GROUPE 5 - SOURCES VIDES (documentes comme INFO)
# ==========================================================================
print("CTR: Sources vides...")

if SOURCES_VIDES["CHARGES"]:
    _ctrl(ctrl_rows, "CHARGES", "MASTER_FACT_MAN_Charges", None,
          "HC_ZERO_SOURCES_VIDES", "INFO",
          "MASTER_FACT_MAN_Charges vide. Controles CHARGE_LOGEMENT_SANS_LOGEMENT_ID / "
          "CHARGE_PERSO_SANS_ASSOCIE non representatifs.",
          commentaire="SAISIE_Charges_Flux.xlsx non peuplee.")

if SOURCES_VIDES["M04"]:
    _ctrl(ctrl_rows, "M04", "M04_MENAGES_PowerQuery", None,
          "HC_ZERO_SOURCES_VIDES", "INFO",
          "M04_MENAGES_PowerQuery SOURCE_RAW vide. Controles M04 non representatifs. "
          "MENAGE_INTERNE_CODE_IMPACT_NON_HC / ACHATS_DEJA_EN_SAISIE_CHARGES = 0 (faux negatif).",
          commentaire="Google Sheet Suivi menage non colle.")

if SOURCES_VIDES["ACOMPTES"]:
    _ctrl(ctrl_rows, "ACOMPTES", "MASTER_FACT_MAN_AcomptesProprietaires", None,
          "HC_ZERO_SOURCES_VIDES", "INFO",
          "MASTER_FACT_MAN_AcomptesProprietaires vide. Controle ACOMPTE_NON_RATTACHE_FACTURE = 0.",
          commentaire="Acomptes non encore saisis.")

if SOURCES_VIDES["IK"]:
    _ctrl(ctrl_rows, "IK", "MASTER_FACT_MAN_IK_Avantages", None,
          "HC_ZERO_SOURCES_VIDES", "INFO",
          "MASTER_FACT_MAN_IK_Avantages vide. Controle MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES = 0.",
          commentaire="IK et avantages non encore saisis.")

if SOURCES_VIDES["HH"]:
    pass  # Deja traite en groupe 2

# INFO transverse: HC = 0 toutes sources
_ctrl(ctrl_rows, "TRANSVERSE", "MASTER_CALC_Resultats", "GLOBAL",
      "HC_ZERO_SOURCES_VIDES", "INFO",
      f"HORS_COMPTA = {hc_val} EUR. M04/Charges/IK non alimentes. "
      "Resultat HC = 0 attendu dans l'etat actuel.",
      commentaire="Normal jusqu'a alimentation M04, Charges, IK, Acomptes.")


# ==========================================================================
# GROUPE 6 - ANOMALIES CONNUES (re-verification independante)
# ==========================================================================
print("CTR: Anomalies connues...")

# 6a - LOG_SANS_FLUX_017 (re-detecte independamment)
logements_avec_forfait = {lid: v for lid, v in forfait_ref.items() if v > 0}
logements_en_flux017 = set(
    df_flux[df_flux["type_flux_id"] == "TYPE_FLUX_017"]["logement_id"].dropna()
)
for lid, forfait in logements_avec_forfait.items():
    if lid not in logements_en_flux017:
        row_log = df_log[df_log["logement_id"] == lid]
        actif = row_log.iloc[0]["actif"] if len(row_log) > 0 else "?"
        _ctrl(ctrl_rows, "RESULTATS", "MASTER_CALC_Flux", lid,
              "LOG_SANS_FLUX_017", "A_CONTROLER",
              f"Logement {lid} a forfait={forfait}E mais 0 flux TYPE_FLUX_017 dans MASTER_CALC_Flux. "
              f"actif={actif}.",
              logement_id=lid,
              commentaire="Confirmer si logement actif. Correction: REF_Setup ou saisie reservations.")

# 6b - CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE (re-detecte)
flux017_first = (
    df_flux[df_flux["type_flux_id"] == "TYPE_FLUX_017"]
    .groupby("logement_id")["mois"].min()
    .to_dict()
)
for _, row_log in df_log.iterrows():
    lid = row_log.get("logement_id")
    if lid not in flux017_first:
        continue
    first_mois = flux017_first[lid]
    date_entree = row_log.get("date_entree_gestion")
    if date_entree is None:
        continue
    if hasattr(date_entree, "strftime"):
        date_entree_mois = date_entree.strftime("%Y-%m")
    else:
        date_entree_mois = str(date_entree)[:7]
    if first_mois < date_entree_mois:
        _ctrl(ctrl_rows, "RESERVATIONS", "MASTER_CALC_Reservations", lid,
              "CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE", "A_CONTROLER",
              f"Logement {lid}: premier mois Flux={first_mois} < date_entree_gestion={date_entree_mois}. "
              "Utilisation Option A (premier mois Flux) conforme a D-LOT10-04.",
              logement_id=lid,
              commentaire="Correction: mettre a jour date_entree_gestion dans REF_Logements (lot separate).")

# 6c - LISTING_ORPHELIN_A_CONTROLER (depuis MASTER_CTRL_HA_Anomalies)
# Correctif faux positif : l'anomalie Lot 1 est figee avant le mapping/alias Lot 2.
# On ne re-emet l'orphelin que si le listing n'est PAS resolvable apres mapping
# (REF_Logements.hostaway_listing_id U REF_Mapping_Logements) ET qu'aucune
# reservation correspondante n'a de logement_id resolu dans MASTER_CALC_Reservations.
def _norm_id(v):
    if v is None:
        return ""
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s

# Listings resolvables directement (REF_Logements.hostaway_listing_id)
_resolved_listings = set()
if "hostaway_listing_id" in df_log.columns:
    _resolved_listings |= {_norm_id(v) for v in df_log["hostaway_listing_id"].dropna()}
# + resolvables via REF_Mapping_Logements (champ listingMapId, actif)
if {"champ_source", "valeur_source"}.issubset(df_map.columns):
    _map_act = df_map
    if "actif" in df_map.columns:
        _map_act = df_map[df_map["actif"].astype(str).str.upper() == "OUI"]
    _map_list = _map_act[_map_act["champ_source"].astype(str).str.contains("listingMapId", case=False, na=False)]
    _resolved_listings |= {_norm_id(v) for v in _map_list["valeur_source"].dropna()}
_resolved_listings.discard("")

# reservation_id_hostaway ayant un logement_id resolu dans la table commune
_resolved_res = set()
if {"reservation_id_hostaway", "logement_id"}.issubset(df_res.columns):
    _res_ok = df_res[df_res["logement_id"].notna()]
    _resolved_res = {_norm_id(v) for v in _res_ok["reservation_id_hostaway"].dropna()}
_resolved_res.discard("")

if "code_anomalie" in df_ha_ano.columns:
    orphelins = df_ha_ano[df_ha_ano["code_anomalie"] == "LISTING_ORPHELIN_A_CONTROLER"]
    for _, row in orphelins.iterrows():
        desc = str(row.get("description", "") or "")
        # listingMapId = premier entier >= 4 chiffres dans la description
        m = re.search(r"\d{4,}", desc)
        listing_id = m.group(0) if m else ""
        res_id = _norm_id(row.get("reservation_id"))
        # Resolu si listing connu OU reservation rattachee a un logement
        if (listing_id and listing_id in _resolved_listings) or (res_id and res_id in _resolved_res):
            continue  # faux positif : listing resolvable via mapping/alias
        _ctrl(ctrl_rows, "HOSTAWAY", "MASTER_CTRL_HA_Anomalies",
              str(row.get("reservation_id") or "listing"),
              "LISTING_ORPHELIN_A_CONTROLER", "A_CONTROLER",
              desc,
              commentaire="Resolution: ajouter listing au REF_Logements ou confirmer inactif.")

# 6d - VRBO_MONTANT_NON_RENSEIGNE (re-detecte depuis Reservations)
n_vrbo = df_res[df_res["source"] == "HOSTAWAY_VRBO_A_CONTROLER"].shape[0]
if n_vrbo > 0:
    _ctrl(ctrl_rows, "RESERVATIONS", "MASTER_CALC_Reservations", None,
          "VRBO_MONTANT_NON_RENSEIGNE", "A_CONTROLER",
          f"{n_vrbo} reservations HOSTAWAY_VRBO_A_CONTROLER sans montant valide. "
          "Saisie manuelle requise au Lot 4.",
          commentaire="Lots 4/4bis: saisir montants VRBO dans SAISIE_ReservationsHorsHostaway.xlsx")

# 6e - RESERVATION_A_CONTROLER_SANS_COMMISSION
n_ac = len(df_com_ac)
if n_ac > 0:
    _ctrl(ctrl_rows, "COMMISSIONS", "MASTER_CALC_Commissions", None,
          "RESERVATION_A_CONTROLER_SANS_COMMISSION", "A_CONTROLER",
          f"{n_ac} reservations A_CONTROLER exclues du calcul automatique des commissions "
          f"(VRBO + Direct). Net proprietaire incomplet pour ces {n_ac} reservations.",
          commentaire="Requiert saisie manuelle ou decision par reservation. Lots 4/4bis.")

# 6f - Rapprochement menages externes <-> Hostaway (depuis Lot 6c VUE_ECART_HOSTAWAY)
#      Remplace l'ancien controle bloquant "DATE_ABSENTE" : la date jour absente sur
#      facture mensuelle ne bloque plus seule. Le controle reel = ecart de volume
#      mensuel (mois x logement). Codes NEUTRES (jamais accusatoires).
try:
    df_ecart = _read_sheet(MEX_FILE, sheet="VUE_ECART_HOSTAWAY")
except Exception:
    df_ecart = pd.DataFrame()

if "code_controle" in df_ecart.columns:
    def _ecart_logs(code):
        g = df_ecart[df_ecart["code_controle"].astype(str) == code]
        logs = ", ".join(str(x) for x in g["logement_id"].dropna())
        return g, logs

    g, logs = _ecart_logs("MENAGE_EXTERNE_ECART_HOSTAWAY")
    if len(g) > 0:
        _ctrl(ctrl_rows, "MENAGES_EXT", "VUE_ECART_HOSTAWAY", None,
              "MENAGE_EXTERNE_ECART_HOSTAWAY", "A_CONTROLER",
              f"{len(g)} logement(s) avec ecart volume facture vs Hostaway: {logs}.",
              commentaire="Ecart a valider (interne, hors HA, decalage mois ou saisie). Pas une accusation prestataire.")

    g, logs = _ecart_logs("MENAGE_EXTERNE_LOGEMENT_HORS_HA")
    if len(g) > 0:
        _ctrl(ctrl_rows, "MENAGES_EXT", "VUE_ECART_HOSTAWAY", None,
              "MENAGE_EXTERNE_LOGEMENT_HORS_HA", "A_CONTROLER",
              f"{len(g)} logement(s) facture(s) absent(s) du comptage Hostaway: {logs}.",
              commentaire="Logement archive/hors HA/mapping. A valider, pas une erreur prestataire.")

    g, logs = _ecart_logs("MENAGE_HA_SANS_FACTURE_EXTERNE")
    if len(g) > 0:
        _ctrl(ctrl_rows, "MENAGES_EXT", "VUE_ECART_HOSTAWAY", None,
              "MENAGE_HA_SANS_FACTURE_EXTERNE", "INFO",
              f"{len(g)} logement(s) avec menages Hostaway sans facture externe: {logs}.",
              commentaire="Probable menage interne / a croiser avec M04.")

    g, logs = _ecart_logs("MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY")
    if len(g) > 0:
        _ctrl(ctrl_rows, "MENAGES_EXT", "VUE_ECART_HOSTAWAY", None,
              "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY", "INFO",
              f"{len(g)} logement(s) rapproche(s) facture = Hostaway.",
              commentaire="Volume mensuel coherent ; date jour non requise.")


# ==========================================================================
# GROUPE 7 - REFERENTIEL
# ==========================================================================
print("CTR: Referentiel...")

# 7a - MODE_FACTURATION_A_DEFINIR
n_adef = (df_prop["mode_facturation"].astype(str) == "A_DEFINIR").sum()
if n_adef > 0:
    _ctrl(ctrl_rows, "TRANSVERSE", "REF_Proprietaires", None,
          "MODE_FACTURATION_A_DEFINIR", "INFO",
          f"{n_adef} proprietaires avec mode_facturation=A_DEFINIR. "
          "Bloquant pour Lot 12 (facturation). Non bloquant pour Lots 9-11.",
          commentaire="Definir mode_facturation avant Lot 12.")

# 7b - Verifier taux_commission present pour proprietaires actifs
if "taux_commission" in df_prop.columns and "actif" in df_prop.columns:
    prop_actifs_sans_taux = df_prop[
        (df_prop["actif"].astype(str).str.upper() == "OUI") &
        (df_prop["taux_commission"].isna() | (df_prop["taux_commission"].astype(str) == ""))
    ]
    if len(prop_actifs_sans_taux) > 0:
        for _, row in prop_actifs_sans_taux.iterrows():
            _ctrl(ctrl_rows, "TRANSVERSE", "REF_Proprietaires",
                  row.get("proprietaire_id"),
                  "COMMISSION_SANS_TAUX", "BLOQUANT",
                  f"Proprietaire {row.get('proprietaire_id')} actif sans taux_commission.",
                  proprietaire_id=row.get("proprietaire_id"))
    else:
        print(f"  COMMISSION_SANS_TAUX: 0 (OK)")


# ==========================================================================
# GROUPE 8 - BANQUE (adaptatif D-LOT11-01 Option C)
# ==========================================================================
print("CTR: Banque...")

if not BANQUE_DISPO:
    _ctrl(ctrl_rows, "BANQUE", "Lot8_Banque", None,
          "BANQUE_NON_DISPONIBLE_GIT", "INFO",
          "Fichier 02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx absent ou illisible. "
          "Controles banque non executes. Fichier ignore par .gitignore (donnees sensibles).",
          commentaire="Executer le script localement avec fichiers Lot 8 presents.")
else:
    # 8a - BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY
    # Verifier qu'aucune ligne CREDIT PAYOUT_PLATEFORME n'est aussi dans Flux en PRODUIT
    flux_from_banque = df_flux[
        df_flux["source_table"].astype(str).str.contains("BANQUE", na=False) &
        (df_flux["sens"] == "PRODUIT")
    ]
    if len(flux_from_banque) > 0:
        for _, row in flux_from_banque.iterrows():
            _ctrl(ctrl_rows, "BANQUE", "MASTER_CALC_Flux", row.get("flux_id"),
                  "BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY", "BLOQUANT",
                  f"Flux PRODUIT source banque detecte: {row.get('source_pk')} type={row.get('type_flux_id')}. "
                  "Un payout banque ne doit pas creer un PRODUIT en double avec Hostaway.",
                  mois=row.get("mois"), logement_id=row.get("logement_id"))
        print(f"  BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY: {len(flux_from_banque)} BLOQUANT")
    else:
        print(f"  BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY: 0 (OK)")

    # 8b - CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE
    # Verifier mois OUVERT avec lignes non classees (RAPPROCHEMENT_REQUIS)
    n_rapproch_requis = (df_bnq["statut_classification"].astype(str) == "RAPPROCHEMENT_REQUIS").sum() \
                        if "statut_classification" in df_bnq.columns else 0

    for _, row_clo in df_cloture.iterrows():
        mois_clo = row_clo.get("mois")
        statut_clo = row_clo.get("statut_mois")
        if mois_clo is None:
            continue
        mois_str = str(mois_clo)[:7]

        # Compter lignes non finalisees pour ce mois
        if "date_operation" in df_bnq.columns:
            df_bnq["_mois"] = df_bnq["date_operation"].astype(str).str[:7]
            n_non_classe_mois = (
                (df_bnq["_mois"] == mois_str) &
                (df_bnq["statut_classification"].astype(str) == "RAPPROCHEMENT_REQUIS")
            ).sum() if "statut_classification" in df_bnq.columns else 0
        else:
            n_non_classe_mois = 0

        if n_non_classe_mois > 0:
            _ctrl(ctrl_rows, "BANQUE", "BANQUE_LOT8_IMPORT_REF_Cloture", mois_str,
                  "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE", "A_CONTROLER",
                  f"Mois {mois_str} statut={statut_clo}: {n_non_classe_mois} lignes RAPPROCHEMENT_REQUIS. "
                  "Cloture impossible tant que rapprochements non resolus.",
                  mois=mois_str,
                  commentaire="Exporter donnees Airbnb et rapprocher RAPPROCH_AIRBNB_ATTENTE (Lot 8c).")

    n_mois_clo = len(df_cloture[df_cloture["mois"].notna()]) if "mois" in df_cloture.columns else 0
    print(f"  CLOTURE: {n_rapproch_requis} lignes RAPPROCHEMENT_REQUIS sur {n_mois_clo} mois banque.")


# ==========================================================================
# DASHBOARD MOIS
# ==========================================================================
print("Construction DASHBOARD_MOIS...")

df_ctrl = pd.DataFrame(ctrl_rows)
mois_avec_ctrl = set(df_ctrl["mois"].dropna().astype(str).unique())

# Mois banque (mois ayant un enregistrement de clôture = mois pilotables)
mois_banque = []
if BANQUE_DISPO and "mois" in df_cloture.columns:
    mois_banque = sorted(set(df_cloture["mois"].dropna().astype(str).str[:7]))

# DASHBOARD = uniquement les mois porteurs de contrôles + mois banque pilotés
# + ligne TRANSVERSE. Pas de mois vides arbitraires (F3).
mois_all = sorted(mois_avec_ctrl | set(mois_banque) | {"TRANSVERSE"})

dashboard = []
for m in mois_all:
    if m == "TRANSVERSE" or m == "nan":
        subset = df_ctrl[df_ctrl["mois"].isna()]
    else:
        subset = df_ctrl[df_ctrl["mois"].astype(str) == m]

    nb_bl  = (subset["severity"] == "BLOQUANT").sum()
    nb_ac  = (subset["severity"] == "A_CONTROLER").sum()
    nb_inf = (subset["severity"] == "INFO").sum()

    # Statut cloture banque (REF_Cloture_Mensuelle du fichier Lot 8)
    statut_clo = "OUVERT"
    a_enregistrement_cloture = False
    if BANQUE_DISPO and len(df_cloture) > 0 and "mois" in df_cloture.columns:
        row_clo = df_cloture[df_cloture["mois"].astype(str).str[:7] == m]
        if len(row_clo) > 0:
            a_enregistrement_cloture = True
            statut_clo = row_clo.iloc[0].get("statut_mois", "OUVERT") or "OUVERT"

    # Banque compatible facturation uniquement si mois explicitement CLOTURE
    banque_cloturee = (statut_clo == "CLOTURE")

    # F1 — facturation Lot 12 : OUI seulement si 0 BLOQUANT ET 0 A_CONTROLER
    #      ET clôture banque réellement validée (statut CLOTURE).
    if m == "TRANSVERSE":
        cloture_possible = "NON_DONNEES_INCOMPLETES"
        facturation_lot12 = "NON_SOUS_RESERVE_A_CONTROLER" if (nb_bl + nb_ac) > 0 else "NON_DONNEES_INCOMPLETES"
    elif nb_bl > 0:
        cloture_possible = "NON"
        facturation_lot12 = "NON_BLOQUANT_OUVERT"
    elif nb_ac > 0:
        cloture_possible = "NON_A_CONTROLER_OUVERTS"
        facturation_lot12 = "NON_SOUS_RESERVE_A_CONTROLER"
    elif not banque_cloturee:
        cloture_possible = "NON_CLOTURE_INCOMPLETE"
        facturation_lot12 = "NON_CLOTURE_INCOMPLETE"
    else:
        cloture_possible = "OUI"
        facturation_lot12 = "OUI"

    if facturation_lot12 == "OUI":
        commentaire = "0 BLOQUANT, 0 A_CONTROLER, mois CLOTURE -> facturation Lot 12 possible."
    elif facturation_lot12 == "NON_BLOQUANT_OUVERT":
        commentaire = f"{nb_bl} BLOQUANT(s) ouvert(s) - Lot 12 bloque."
    elif facturation_lot12 == "NON_SOUS_RESERVE_A_CONTROLER":
        commentaire = f"{nb_ac} A_CONTROLER a traiter/valider humainement avant facturation."
    elif facturation_lot12 == "NON_CLOTURE_INCOMPLETE":
        sc = statut_clo if a_enregistrement_cloture else "AUCUN_ENREGISTREMENT_CLOTURE"
        commentaire = f"0 anomalie mais cloture banque non validee (statut={sc}) - facturation impossible."
    else:  # NON_DONNEES_INCOMPLETES
        commentaire = "Controles transverses / sources incompletes - non facturable en l'etat."

    dashboard.append({
        "mois":                    m,
        "nb_bloquants_ouverts":    int(nb_bl),
        "nb_a_controler_ouverts":  int(nb_ac),
        "nb_info":                 int(nb_inf),
        "statut_mois_banque":      statut_clo,
        "cloture_possible":        cloture_possible,
        "facturation_lot12_ok":    facturation_lot12,
        "commentaire":             commentaire,
    })

df_dashboard = pd.DataFrame(dashboard)


# ==========================================================================
# RAPPROCHEMENT PAYOUT / BANQUE (indicatif D-LOT11-03)
# ==========================================================================
print("Construction RAPPROCHEMENT_PAYOUT_BANQUE...")

rapproch_rows = []
BANQUE_STATUT_RAPPROCH = "BANQUE_NON_DISPONIBLE_GIT"

if BANQUE_DISPO:
    BANQUE_STATUT_RAPPROCH = "BANQUE_DISPONIBLE"
    # Mois banque disponibles
    mois_bnq_list = sorted(set(df_bnq["date_operation"].astype(str).str[:7].dropna())
                           if "date_operation" in df_bnq.columns else [])

    # Cote banque: CREDIT PAYOUT_PLATEFORME par mois
    if "categorie" in df_bnq.columns and "sens" in df_bnq.columns:
        df_bnq["_m"] = df_bnq["date_operation"].astype(str).str[:7]
        df_bnq["_montant_n"] = pd.to_numeric(df_bnq["montant"], errors="coerce").fillna(0.0)
        bnq_payout = (
            df_bnq[
                (df_bnq["categorie"] == "PAYOUT_PLATEFORME") &
                (df_bnq["sens"] == "CREDIT")
            ]
            .groupby("_m")["_montant_n"].sum()
            .to_dict()
        )
    else:
        bnq_payout = {}
        BANQUE_STATUT_RAPPROCH = "BANQUE_CLASSIFICATION_INSUFFISANTE"

    # Cote Hostaway: payout_calcule par mois (check-in month)
    df_c2 = df_com.copy()
    df_c2["_pay"] = pd.to_numeric(df_c2["payout_calcule"], errors="coerce").fillna(0.0)
    ha_payout_by_mois = df_c2.groupby("mois")["_pay"].sum().to_dict()

    # Combine pour mois banque disponibles
    for m_b in sorted(set(bnq_payout) | set(mois_bnq_list)):
        bank_total = round(bnq_payout.get(m_b, 0.0), 2)
        ha_total   = round(ha_payout_by_mois.get(m_b, 0.0), 2)
        ecart      = round(bank_total - ha_total, 2)
        rapproch_rows.append({
            "mois":                    m_b,
            "payout_banque_plateforme": bank_total,
            "payout_hostaway_mois":    ha_total,
            "ecart":                   ecart,
            "statut_rapprochement":    "INFORMATIF",
            "commentaire":             (
                "Ecart attendu: mois Hostaway = check-in date, "
                "mois banque = date virement reel (decalage de 1-2 mois). "
                "Rapprochement indicatif uniquement (D-LOT11-03). "
                "RAPPROCH_AIRBNB_ATTENTE contient les details par virement."
            ),
        })
    if not rapproch_rows:
        BANQUE_STATUT_RAPPROCH = "BANQUE_CLASSIFICATION_INSUFFISANTE"
else:
    rapproch_rows.append({
        "mois": None,
        "payout_banque_plateforme": 0,
        "payout_hostaway_mois": 0,
        "ecart": 0,
        "statut_rapprochement": "BANQUE_NON_DISPONIBLE_GIT",
        "commentaire": (
            "Fichier BANQUE_LOT8_IMPORT.xlsx absent (ignore git). "
            "Executer localement avec fichiers Lot 8 presents."
        ),
    })

df_rapproch = pd.DataFrame(rapproch_rows)


# ==========================================================================
# CAISSE THEORIQUE (D-LOT11-05)
# ==========================================================================
print("Construction CAISSE_THEORIQUE...")

# Structure: liquide_recupere - liquide_reverse - liquide_charge - liquide_acompte = solde
# HH vide -> 0 / Charges vide -> 0 / Acomptes vide -> 0
caisse_src = {
    "liquide_recupere_hh":   0.0,
    "liquide_verse_proprietaires": 0.0,
    "liquide_utilise_charges": 0.0,
    "liquide_affecte_acomptes": 0.0,
}

# HH: liquide recupere depuis montant_retenu ou montant_percu
if not SOURCES_VIDES["HH"] and "mode_paiement_id" in df_hh.columns:
    mask_liquide_hh = df_hh["mode_paiement_id"].astype(str).str.upper().isin(
        ["ESPECES", "LIQUIDE", "CASH"]
    )
    if mask_liquide_hh.any():
        caisse_src["liquide_recupere_hh"] = pd.to_numeric(
            df_hh.loc[mask_liquide_hh, "montant_retenu"], errors="coerce"
        ).fillna(0.0).sum()

# Charges: liquide utilise
if not SOURCES_VIDES["CHARGES"] and "mode_paiement_id" in df_chg.columns:
    mask_liquide_chg = df_chg["mode_paiement_id"].astype(str).str.upper().isin(
        ["ESPECES", "LIQUIDE", "CASH"]
    )
    if mask_liquide_chg.any():
        caisse_src["liquide_utilise_charges"] = pd.to_numeric(
            df_chg.loc[mask_liquide_chg, "montant"], errors="coerce"
        ).fillna(0.0).sum()

solde_theorique = (
    caisse_src["liquide_recupere_hh"]
    - caisse_src["liquide_verse_proprietaires"]
    - caisse_src["liquide_utilise_charges"]
    - caisse_src["liquide_affecte_acomptes"]
)

caisse_rows = [
    {"poste": "Liquide recupere (HH/direct)",
     "montant": caisse_src["liquide_recupere_hh"],
     "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if SOURCES_VIDES["HH"] else "OK"},
    {"poste": "Liquide verse proprietaires",
     "montant": -caisse_src["liquide_verse_proprietaires"],
     "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if SOURCES_VIDES["HH"] else "OK"},
    {"poste": "Liquide utilise charges",
     "montant": -caisse_src["liquide_utilise_charges"],
     "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if SOURCES_VIDES["CHARGES"] else "OK"},
    {"poste": "Liquide affecte acomptes",
     "montant": -caisse_src["liquide_affecte_acomptes"],
     "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES" if SOURCES_VIDES["ACOMPTES"] else "OK"},
    {"poste": "=== SOLDE THEORIQUE ===",
     "montant": round(solde_theorique, 2),
     "statut": "CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES"},
    {"poste": "NOTE",
     "montant": None,
     "statut": "HH/Charges/Acomptes vides. Solde = 0 attendu. Structure stable pour recalculs futurs."},
]
df_caisse = pd.DataFrame(caisse_rows)


# ==========================================================================
# ECRITURE MASTER_CTRL_Coherence.xlsx
# ==========================================================================
print("Ecriture MASTER_CTRL_Coherence.xlsx...")

df_master       = pd.DataFrame(ctrl_rows)
df_bloquants    = df_master[
    (df_master["severity"] == "BLOQUANT") &
    (df_master["statut_resolution"] == "OUVERT")
].reset_index(drop=True)
df_ac_ouverts   = df_master[
    (df_master["severity"] == "A_CONTROLER") &
    (df_master["statut_resolution"] == "OUVERT")
].reset_index(drop=True)

# Statistiques
n_total    = len(df_master)
n_bloquant = len(df_bloquants)
n_ac       = len(df_ac_ouverts)
n_info     = (df_master["severity"] == "INFO").sum()

print(f"\n  === RAPPORT CTR-2026-06-020 ===")
print(f"  Total controles generes : {n_total}")
print(f"  BLOQUANTS ouverts       : {n_bloquant}")
print(f"  A_CONTROLER ouverts     : {n_ac}")
print(f"  INFO                    : {n_info}")
print(f"  Banque statut           : {BANQUE_STATUT_RAPPROCH}")
print(f"  Caisse solde theorique  : {round(solde_theorique, 2)} EUR")
print(f"  Lot 12 facturation      : {'BLOQUE' if n_bloquant > 0 else 'OK_SOUS_RESERVE_A_CONTROLER'}")
print()

with pd.ExcelWriter(str(OUT_FILE), engine="openpyxl") as writer:
    df_master.to_excel(writer, sheet_name="MASTER", index=False)
    df_bloquants.to_excel(writer, sheet_name="BLOQUANTS_OUVERTS", index=False)
    df_ac_ouverts.to_excel(writer, sheet_name="A_CONTROLER_OUVERTS", index=False)
    df_dashboard.to_excel(writer, sheet_name="DASHBOARD_MOIS", index=False)
    df_rapproch.to_excel(writer, sheet_name="RAPPROCHEMENT_PAYOUT_BANQUE", index=False)
    df_caisse.to_excel(writer, sheet_name="CAISSE_THEORIQUE", index=False)

print(f"  Fichier ecrit: {OUT_FILE}")
print(f"\nSources amont : AUCUNE modification. Toutes lues en read_only=True.")
print("Lot 11 termine. Attente validation humaine avant commit.")

"""LOT4A — Bibliotheque pure partagee (comparateur + transformateur dry-run).

Regles UNIQUES du flux SAISIE_ReservationsHorsHostaway -> MASTER_FACT_MAN_ReservationsHorsHostaway.
Aucune ecriture metier ici. Lecture seule des classeurs. Arrondi = primitive numpy identique lot10.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from pathlib import Path

import numpy as np
import openpyxl

from lib_ref_history import resolve_commission_rate

# ── Racines projet (parametrees) ────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAISIE_PATH = PROJECT_ROOT / "01_SOURCES_BRUTES" / "ReservationsHH" / "SAISIE_ReservationsHorsHostaway.xlsx"
MASTER_PATH = PROJECT_ROOT / "02_TRAVAIL" / "Lot4_ReservationsHH" / "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
REF_SETUP_PATH = PROJECT_ROOT / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm"

FORBIDDEN_WRITE_ROOTS = [
    PROJECT_ROOT / "01_SOURCES_BRUTES",
    PROJECT_ROOT / "02_TRAVAIL",
    PROJECT_ROOT / "03_EXPORTS",
    PROJECT_ROOT / "05_APPLICATION",
]

PK_PREFIX = "RESHH-"
SHEET_SAISIE = "SAISIE"
SHEET_MASTER = "MASTER"
SHEET_VUE_ACTIVE = "VUE_ACTIVE"
SHEET_TAUX = "REF_Taux_Commission"

SYS_SOURCE_MODULE = "LOT4_HH"
SYS_SOURCE_TABLE = "SAISIE_ReservationsHorsHostaway"

# Schema canonique : 34 colonnes historiques, puis champs APP-2b de tracabilite.
MASTER_COLUMNS = [
    "reservation_hh_id", "ROW_HASH", "mois", "canal_id", "source_financiere",
    "proprietaire_id", "logement_id", "reservation_id_hostaway", "date_arrivee",
    "date_depart", "nuits", "total_percu", "menage", "taux_commission",
    "taux_commission_source", "commentaire_taux_commission", "commission",
    "montant_recupere", "associe_id_recuperateur", "montant_reverse_proprietaire",
    "mode_paiement_id", "acompte_facture", "code_impact", "comptabilisation",
    "impact_resultat_reel", "impact_resultat_comptable", "statut_controle",
    "niveau_anomalie", "code_anomalie", "commentaire",
    "source_module", "source_table", "source_pk", "date_integration",
    "taux_commission_override", "motif_override_taux_commission",
    "confirmation_override_taux_commission", "menage_override",
    "motif_override_menage", "confirmation_override_menage",
    "source_acompte_facture",
]

DERIVED_FIELDS = {
    "ROW_HASH", "mois", "nuits", "taux_commission", "taux_commission_source",
    "commission", "acompte_facture", "impact_resultat_reel", "impact_resultat_comptable",
}
SYSTEM_FIELDS = {"source_module", "source_table", "source_pk"}
METADATA_FIELDS = {"date_integration"}
TAUX_DEPENDENT = {"taux_commission", "taux_commission_source", "commission", "acompte_facture"}

# Champs manuels dont l'absence bloque la generation
REQUIRED_MANUAL = ["reservation_hh_id", "canal_id", "source_financiere",
                   "proprietaire_id", "logement_id", "date_arrivee", "date_depart",
                   "total_percu", "code_impact", "comptabilisation", "statut_controle"]
NUMERIC_FIELDS = ["total_percu", "menage", "montant_recupere", "montant_reverse_proprietaire"]
VALID_CODE_IMPACT = {"IC", "HC", "HR"}
VALID_STATUT_CONTROLE = {"VALIDE", "A_CONTROLER", "EXCLU_RESULTAT", "A_VENTILER"}

IMPACT_REEL = {"IC": "OUI", "HC": "OUI", "HR": "NON"}
IMPACT_COMPTA = {"IC": "OUI", "HC": "NON", "HR": "NON"}

MODE_BANQUE_PRO = {"PAY_001", "BANQUE_PRO"}
MODE_ESPECES = {"PAY_002", "ESPECES_CAISSE", "ESPECES"}
MODE_CARTE_ASSOCIEE = {"PAY_003", "CARTE_ASSOCIEE"}
MODE_COMPTE_ASSOCIE = {"PAY_004", "COMPTE_PERSO_ASSOCIEE"}
MODE_DIRECT_PROPRIETAIRE = {"PAY_006", "DIRECT_PROPRIETAIRE"}


# ── Primitive d'arrondi : identique lot10 (numpy float64), jamais round() builtin ──
def round2(value):
    if value is None or value == "":
        return None
    return float(np.round(np.float64(value), 2))


def _num(value) -> float:
    if value is None or value == "":
        return 0.0
    return float(np.float64(value))


def _norm(v) -> str:
    return "" if v is None else str(v).strip()


def _confirmed(value) -> bool:
    return _norm(value).lower() in {"1", "true", "on", "oui", "yes"}


def _override_decimal(value, confirmation) -> float | None:
    if not _confirmed(confirmation) or _norm(value) == "":
        return None
    return _num(value)


def _effective_taux(saisie: dict, taux_ref: float) -> tuple[float, str]:
    override = _override_decimal(
        saisie.get("taux_commission_override"),
        saisie.get("confirmation_override_taux_commission"),
    )
    if override is None:
        return taux_ref, "REF_HISTORIQUE"
    return (override / 100.0 if override > 1 else override), "OVERRIDE_CONFIRME"


def _effective_menage(saisie: dict) -> tuple[float, str]:
    override = _override_decimal(
        saisie.get("menage_override"),
        saisie.get("confirmation_override_menage"),
    )
    if override is None:
        return _num(saisie.get("menage")), "STANDARD"
    return override, "OVERRIDE_CONFIRME"


def compute_acompte_facture(saisie: dict) -> tuple[float, str]:
    mode = _norm(saisie.get("mode_paiement_id"))
    if mode in MODE_BANQUE_PRO or mode == "":
        return _num(saisie.get("total_percu")), "TOTAL_PERCU"
    if mode in MODE_COMPTE_ASSOCIE or mode in MODE_CARTE_ASSOCIEE:
        return _num(saisie.get("montant_recupere")), "MONTANT_RECUPERE_ASSOCIE"
    if mode in MODE_ESPECES:
        return _num(saisie.get("montant_reverse_proprietaire")), "MONTANT_REVERSE_PROPRIETAIRE"
    if mode in MODE_DIRECT_PROPRIETAIRE:
        return 0.0, "DIRECT_PROPRIETAIRE"
    return _num(saisie.get("total_percu")), "MODE_INCONNU_TOTAL_PERCU"


def to_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return _dt.datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    return None


# ── Garde de chemin generique (chemins resolus) ─────────────────────────────
def assert_output_under(path: Path, allowed_root: Path) -> Path:
    p = Path(path).resolve()
    allowed = Path(allowed_root).resolve()
    if p != allowed and allowed not in p.parents:
        raise RuntimeError(f"Ecriture refusee hors dossier autorise: {p}")
    for forbidden in FORBIDDEN_WRITE_ROOTS:
        fr = forbidden.resolve()
        if p == fr or fr in p.parents:
            raise RuntimeError(f"Ecriture refusee vers racine metier: {p}")
    return p


# ── Empreintes ──────────────────────────────────────────────────────────────
def file_fingerprint(path: Path) -> dict:
    p = Path(path)
    if not p.exists():
        return {"exists": False}
    data = p.read_bytes()
    st = p.stat()
    return {"exists": True, "sha256": hashlib.sha256(data).hexdigest(),
            "size": st.st_size, "mtime_ns": st.st_mtime_ns}


# ── Lectures classeurs (read-only) ──────────────────────────────────────────
def read_saisie_values(path: Path = SAISIE_PATH) -> list[dict]:
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        rows = list(wb[SHEET_SAISIE].iter_rows(values_only=True))
    finally:
        wb.close()
    header = [_norm(h) for h in rows[0]]
    out = []
    for r in rows[1:]:
        rec = dict(zip(header, r))
        if _norm(rec.get("reservation_hh_id")).startswith(PK_PREFIX):
            out.append(rec)
    return out


def check_saisie_formulas(path: Path = SAISIE_PATH) -> dict:
    wb = openpyxl.load_workbook(str(path), read_only=False, data_only=False)
    try:
        ws = wb[SHEET_SAISIE]
        header = [_norm(c.value) for c in ws[1]]
        idx = {n: i for i, n in enumerate(header)}
        res = {}
        for f in DERIVED_FIELDS:
            if f in idx:
                cell = ws.cell(row=2, column=idx[f] + 1).value
                res[f] = isinstance(cell, str) and cell.startswith("=")
            else:
                res[f] = False
    finally:
        wb.close()
    return res


def read_master_values(path: Path = MASTER_PATH) -> dict[str, dict]:
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        rows = list(wb[SHEET_MASTER].iter_rows(values_only=True))
    finally:
        wb.close()
    header = [_norm(h) for h in rows[0]]
    out = {}
    for r in rows[1:]:
        rec = dict(zip(header, r))
        pk = _norm(rec.get("reservation_hh_id"))
        if pk.startswith(PK_PREFIX):
            out[pk] = rec
    return out


def read_taux_rows(path: Path = REF_SETUP_PATH) -> list[dict]:
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        rows = list(wb[SHEET_TAUX].iter_rows(values_only=True))
    finally:
        wb.close()
    header = [_norm(h) for h in rows[0]]
    return [dict(zip(header, r)) for r in rows[1:] if _norm(r[0])]


# ── Validation metier d'une ligne manuelle ──────────────────────────────────
def validate_manual(saisie: dict, seen_pks: set) -> list[str]:
    """Retourne la liste des codes d'anomalie DONNEE (vide si ligne saine)."""
    errs = []
    pk = _norm(saisie.get("reservation_hh_id"))
    if not pk.startswith(PK_PREFIX):
        errs.append("HH_ID_INVALIDE")
    if pk in seen_pks:
        errs.append("HH_ID_DUPLIQUE")
    for f in REQUIRED_MANUAL:
        if _norm(saisie.get(f)) == "":
            errs.append(f"CHAMP_OBLIGATOIRE_MANQUANT:{f}")
    d_arr = to_date(saisie.get("date_arrivee"))
    d_dep = to_date(saisie.get("date_depart"))
    if _norm(saisie.get("date_arrivee")) and d_arr is None:
        errs.append("DATE_ARRIVEE_INVALIDE")
    if _norm(saisie.get("date_depart")) and d_dep is None:
        errs.append("DATE_DEPART_INVALIDE")
    if d_arr and d_dep and d_dep <= d_arr:
        errs.append("DATE_DEPART_AVANT_OU_EGALE_ARRIVEE")
    for f in NUMERIC_FIELDS:
        v = saisie.get(f)
        if _norm(v) != "":
            try:
                float(np.float64(v))
            except (TypeError, ValueError):
                errs.append(f"MONTANT_NON_NUMERIQUE:{f}")
    ci = _norm(saisie.get("code_impact"))
    if ci and ci not in VALID_CODE_IMPACT:
        errs.append("CODE_IMPACT_INCONNU")
    sc = _norm(saisie.get("statut_controle"))
    if sc and sc not in VALID_STATUT_CONTROLE:
        errs.append("STATUT_CONTROLE_INVALIDE")
    return errs


# ── Recalcul des champs derives (Python, jamais le cache Excel) ─────────────
def recompute(saisie: dict, taux_rows: list[dict]) -> dict:
    pk = _norm(saisie.get("reservation_hh_id"))
    canal = _norm(saisie.get("canal_id"))
    prop = _norm(saisie.get("proprietaire_id"))
    log = _norm(saisie.get("logement_id"))
    d_arr = to_date(saisie.get("date_arrivee"))
    d_dep = to_date(saisie.get("date_depart"))
    total = saisie.get("total_percu")
    menage = saisie.get("menage")
    reverse = saisie.get("montant_reverse_proprietaire")

    d = {}
    if pk == "":
        d["ROW_HASH"] = ""
    else:
        arr_txt = d_arr.strftime("%Y%m%d") if d_arr else ""
        total_txt = f"{_num(total):.2f}" if _norm(total) != "" else ""
        d["ROW_HASH"] = f"{pk}|{canal}|{prop}|{log}|{arr_txt}|{total_txt}"

    d["mois"] = d_arr.strftime("%Y-%m") if d_arr else ""
    d["nuits"] = (d_dep - d_arr).days if (d_arr and d_dep) else ""

    code_impact = _norm(saisie.get("code_impact"))
    d["impact_resultat_reel"] = IMPACT_REEL.get(code_impact, "A_CONTROLER")
    d["impact_resultat_comptable"] = IMPACT_COMPTA.get(code_impact, "A_CONTROLER")

    d["source_module"] = SYS_SOURCE_MODULE
    d["source_table"] = SYS_SOURCE_TABLE
    d["source_pk"] = pk

    res = resolve_commission_rate(taux_rows, proprietaire_id=prop, logement_id=log, ref_date=d_arr)
    taux_status = res.status
    if res.status == "OK":
        taux, taux_source = _effective_taux(saisie, res.value)
        row = res.row or {}
        d["taux_commission"] = taux
        if taux_source == "OVERRIDE_CONFIRME":
            d["taux_commission_source"] = taux_source
        else:
            d["taux_commission_source"] = "REF_LOGEMENT" if _norm(row.get("logement_id")) else "REF_PROPRIETAIRE"
        if _norm(total) != "":
            menage_effectif, menage_source = _effective_menage(saisie)
            acompte, source_acompte = compute_acompte_facture(saisie)
            assiette = round2(_num(total) - menage_effectif)
            commission = round2(assiette * taux)
            d["commission"] = commission
            d["acompte_facture"] = round2(acompte)
            d["menage_effectif"] = round2(menage_effectif)
            d["menage_effectif_source"] = menage_source
            d["source_acompte_facture"] = source_acompte
        else:
            d["commission"] = ""
            d["acompte_facture"] = ""
            d["menage_effectif"] = ""
            d["menage_effectif_source"] = ""
            d["source_acompte_facture"] = ""
    else:
        for f in TAUX_DEPENDENT:
            d[f] = None
    return {"derived": d, "taux_status": taux_status, "taux_message": res.message}


# ── Construction d'un enregistrement MASTER 34 colonnes (ordre canonique) ────
def _fmt_date(v) -> str:
    d = to_date(v)
    return d.strftime("%Y-%m-%d") if d else ""


def build_master_record(saisie: dict, taux_rows: list[dict], as_of_iso: str) -> dict | None:
    """Retourne un dict 34 colonnes, ou None si taux bloquant (rien a publier)."""
    rc = recompute(saisie, taux_rows)
    if rc["taux_status"] != "OK":
        return None
    d = rc["derived"]

    def money(x):
        r = round2(x) if _norm(x) != "" else None
        return r if r is not None else ""

    rec = {
        "reservation_hh_id": _norm(saisie.get("reservation_hh_id")),
        "ROW_HASH": d["ROW_HASH"],
        "mois": d["mois"],
        "canal_id": _norm(saisie.get("canal_id")),
        "source_financiere": _norm(saisie.get("source_financiere")),
        "proprietaire_id": _norm(saisie.get("proprietaire_id")),
        "logement_id": _norm(saisie.get("logement_id")),
        "reservation_id_hostaway": saisie.get("reservation_id_hostaway"),
        "date_arrivee": _fmt_date(saisie.get("date_arrivee")),
        "date_depart": _fmt_date(saisie.get("date_depart")),
        "nuits": d["nuits"],
        "total_percu": money(saisie.get("total_percu")),
        "menage": money(d.get("menage_effectif", saisie.get("menage"))),
        "taux_commission": d["taux_commission"],
        "taux_commission_source": d["taux_commission_source"],
        "commentaire_taux_commission": _norm(saisie.get("commentaire_taux_commission")),
        "commission": d["commission"],
        "montant_recupere": money(saisie.get("montant_recupere")),
        "associe_id_recuperateur": _norm(saisie.get("associe_id_recuperateur")),
        "montant_reverse_proprietaire": money(saisie.get("montant_reverse_proprietaire")),
        "mode_paiement_id": _norm(saisie.get("mode_paiement_id")),
        "acompte_facture": d["acompte_facture"],
        "code_impact": _norm(saisie.get("code_impact")),
        "comptabilisation": _norm(saisie.get("comptabilisation")),
        "impact_resultat_reel": d["impact_resultat_reel"],
        "impact_resultat_comptable": d["impact_resultat_comptable"],
        "statut_controle": _norm(saisie.get("statut_controle")),
        "niveau_anomalie": _norm(saisie.get("niveau_anomalie")),
        "code_anomalie": _norm(saisie.get("code_anomalie")),
        "commentaire": _norm(saisie.get("commentaire")),
        "source_module": d["source_module"],
        "source_table": d["source_table"],
        "source_pk": d["source_pk"],
        "date_integration": as_of_iso,  # chaine ISO UTC canonique, jamais date locale/serial
        "taux_commission_override": money(saisie.get("taux_commission_override")),
        "motif_override_taux_commission": _norm(saisie.get("motif_override_taux_commission")),
        "confirmation_override_taux_commission": _norm(saisie.get("confirmation_override_taux_commission")),
        "menage_override": money(saisie.get("menage_override")),
        "motif_override_menage": _norm(saisie.get("motif_override_menage")),
        "confirmation_override_menage": _norm(saisie.get("confirmation_override_menage")),
        "source_acompte_facture": d.get("source_acompte_facture", ""),
    }
    return rec


def build_master(saisie_list: list[dict], taux_rows: list[dict], as_of_iso: str) -> dict:
    """Construit toutes les lignes MASTER + VUE_ACTIVE, ou signale un blocage.

    Retour : {statut, master_rows, vue_active_rows, anomalies_donnees, anomalies_taux}.
    """
    anomalies_donnees = []
    anomalies_taux = []
    master_rows = []
    seen = set()

    for saisie in saisie_list:
        pk = _norm(saisie.get("reservation_hh_id"))
        errs = validate_manual(saisie, seen)
        seen.add(pk)
        if errs:
            anomalies_donnees.append({"reservation_hh_id": pk, "codes": errs})
        rc = recompute(saisie, taux_rows)
        if rc["taux_status"] in ("MISSING", "AMBIGUOUS"):
            anomalies_taux.append({"reservation_hh_id": pk, "statut": rc["taux_status"],
                                   "message": rc["taux_message"]})

    # blocage : taux ou donnees invalides sur ligne utile
    if anomalies_taux:
        statut = "ANALYSE_BLOQUEE_TAUX"
        return {"statut": statut, "master_rows": [], "vue_active_rows": [],
                "anomalies_donnees": anomalies_donnees, "anomalies_taux": anomalies_taux,
                "motif_blocage": "TAUX_" + anomalies_taux[0]["statut"]}
    if anomalies_donnees:
        statut = "ANALYSE_BLOQUEE_DONNEES"
        return {"statut": statut, "master_rows": [], "vue_active_rows": [],
                "anomalies_donnees": anomalies_donnees, "anomalies_taux": [],
                "motif_blocage": "DONNEES_INVALIDES"}

    for saisie in saisie_list:
        rec = build_master_record(saisie, taux_rows, as_of_iso)
        if rec is not None:
            master_rows.append(rec)

    vue_active_rows = [r for r in master_rows if _norm(r.get("statut_controle")) == "VALIDE"]
    return {"statut": "ANALYSE_TERMINEE", "master_rows": master_rows, "vue_active_rows": vue_active_rows,
            "anomalies_donnees": [], "anomalies_taux": [], "motif_blocage": None}

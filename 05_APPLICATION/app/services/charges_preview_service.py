"""APP-3b-1 - Prévisualisation de saisie charge sur copie.

Aucune écriture dans les sources réelles.
SAISIE_Charges_Flux.xlsx n'est jamais modifié.
Les copies ne sont créées que sous DRYRUNS_DIR.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg
from app.readers.saisie_charges_reader import (
    FORMULA_COL_INDICES,
    MANUAL_COL_MAP,
    _col_index,
    count_charges_with_prefix,
    find_model_row,
    read_ref_assoc_mode,
    read_ref_associes,
    read_ref_cartes_paiement,
    read_ref_categories_charges,
    read_ref_cloture,
    read_ref_codes_impact,
    read_ref_couts_standards_menage,
    read_ref_gestion_logements,
    read_ref_intervenants,
    read_ref_logements,
    read_ref_modes_paiement,
    read_ref_proprietaires,
    read_ref_statuts,
    read_ref_types_affectation,
    read_ref_types_flux,
    reservation_id_exists,
)
from app.services import charges_impact_service as impact
from app.services import charges_impacts_persist_service as persist

DRYRUNS_DIR = cfg.DRYRUNS_DIR
SAISIE_COPY_NAME = "SAISIE_Charges_Flux_copie.xlsx"
MANIFEST_NAME = "manifest.json"
IMPACTS_COPY_NAME = "SAISIE_Charges_Impacts_copie.xlsx"

# Champs du manifest qui portent la DÉCISION (ce qui sera écrit). Leur empreinte est scellée dans
# `integrite` : la confirmation la recalcule et refuse d'écrire si elle diffère. Ce n'est pas un
# dispositif anti-intrusion (le fichier est local et réinscriptible) mais une détection de
# corruption / d'altération accidentelle entre la prévisualisation et la confirmation.
CHAMPS_SCELLES: tuple[str, ...] = (
    "token", "status", "charge_id", "mois_charge", "target_row", "type_flux_id", "profil_impact",
    "assoc_mode", "form_data", "persistable", "source_hash_avant", "impacts_hash_avant",
)


def sceller_manifest(manifest: dict[str, Any]) -> str:
    """Empreinte des champs de décision du manifest (ordre stable, indépendant de l'insertion)."""
    charge = {k: manifest.get(k) for k in CHAMPS_SCELLES}
    payload = json.dumps(charge, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

CATEGORIE_REQUIRES_RESERVATION: frozenset[str] = frozenset({"CHG_021"})
MODES_REQUIRES_ASSOCIE: frozenset[str] = frozenset({"PAY_003", "PAY_004"})
MODE_CARTE = "PAY_003"

# ── Alignement référentiels Excel (Commit 1 — APP aligne saisie charges) ──
# Formulaire standard : IC et HC seulement ; HR hors parcours Nouvelle charge.
STANDARD_CODES_IMPACT: frozenset[str] = frozenset({"IC", "HC"})
# Valeurs canoniques SAISIE Excel (REF_LOCALE) — jamais CHARGE/PRODUIT ni AFF_*.
CANONICAL_SENS_FLUX: frozenset[str] = frozenset(
    {"DEPENSE", "RECUPERATION", "REMBOURSEMENT", "REFACTURATION", "NEUTRE"}
)
CANONICAL_AFFECTATION: frozenset[str] = frozenset(
    {"LOGEMENT", "PROPRIETAIRE", "GLOBAL", "NON_AFFECTABLE"}
)
DEFAULT_SENS_FLUX = "DEPENSE"
# Valeurs injectées automatiquement (jamais saisies par l'utilisateur).
AUTO_STATUT_CONTROLE = "A_CONTROLER"
AUTO_NIVEAU_ANOMALIE = "INFO"
# prise_en_compta dérivée du code_impact (IC=OUI, HC=NON).
PRISE_EN_COMPTA_BY_IMPACT: dict[str, str] = {"IC": "OUI", "HC": "NON"}

# ── Profils d'impact + familles éligibles (Commit 3) ──
FAMILLE_MENAGE = "MENAGE"
FAMILLE_PARCOURS_DEDIE = "PARCOURS_DEDIE"
# Familles previsualisables au formulaire standard.
FAMILLES_STANDARD: frozenset[str] = frozenset({"GLOBAL", "LOGEMENT_DIRECT"})
PROFIL_BY_FAMILLE: dict[str, str] = {"GLOBAL": "GLOBAL", "LOGEMENT_DIRECT": "LOGEMENT_DIRECT"}
CATEGORIE_PERSONNALISEE = "CHG_024"

# ── Dérivation type_flux_id (D-CHG-TYPEFLUX-01) — jamais choisi par l'utilisateur ──
# Types spécifiques prioritaires par catégorie.
TYPE_FLUX_BY_CATEGORIE: dict[str, str] = {
    "CHG_016": "TYPE_FLUX_012",
    "CHG_010": "TYPE_FLUX_016",
}
# Catégories → TYPE_FLUX_011 si refacturable=OUI.
CATEGORIES_REFAC_TF011: frozenset[str] = frozenset({"CHG_008", "CHG_011"})
TYPE_FLUX_REFAC = "TYPE_FLUX_011"
# Modes de paiement interdits en Nouvelle charge standard.
MODES_INTERDITS_STANDARD: frozenset[str] = frozenset({"PAY_005", "PAY_006"})
MODE_ESPECES = "PAY_002"
# Sinon, type déterminé par le règlement.
TYPE_FLUX_BY_MODE: dict[str, str] = {
    "PAY_001": "TYPE_FLUX_020",
    "PAY_003": "TYPE_FLUX_004",
    "PAY_004": "TYPE_FLUX_004",
}
TYPE_FLUX_ESPECES_RECUPERE = "TYPE_FLUX_008"
TYPE_FLUX_ESPECES = "TYPE_FLUX_004"


def derive_type_flux(
    categorie_id: str,
    mode_paiement_id: str,
    refacturable: str | None,
    paye_avec_montant_recupere: str | None,
) -> str | None:
    """Déduit type_flux_id selon la matrice validée (D-CHG-TYPEFLUX-01).

    Ordre : type spécifique catégorie > refacturable TF011 > règlement.
    Retourne None si non déductible (mode interdit / inconnu) — refusé en amont.
    CHG_024 : jamais TYPE_FLUX_011 (traité comme paiement-driven pur).
    """
    cat = str(categorie_id or "").strip()
    mode = str(mode_paiement_id or "").strip()

    # 1. Type spécifique catégorie (prioritaire)
    if cat in TYPE_FLUX_BY_CATEGORIE:
        return TYPE_FLUX_BY_CATEGORIE[cat]
    # 2. Refacturable → TF011 (jamais pour CHG_024)
    if (
        cat in CATEGORIES_REFAC_TF011
        and cat != CATEGORIE_PERSONNALISEE
        and str(refacturable or "").strip().upper() == "OUI"
    ):
        return TYPE_FLUX_REFAC
    # 3. Déterminé par le règlement
    if mode in MODES_INTERDITS_STANDARD:
        return None
    if mode == MODE_ESPECES:
        if str(paye_avec_montant_recupere or "").strip().upper() == "OUI":
            return TYPE_FLUX_ESPECES_RECUPERE
        return TYPE_FLUX_ESPECES
    return TYPE_FLUX_BY_MODE.get(mode)


def _getlist(form_data: dict[str, Any], key: str) -> list[str]:
    """Récupère une valeur multi-champ (liste ou str) en liste de str non vides."""
    v = form_data.get(key)
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []


def resolve_impact_menage(categorie_id: str, form_data: dict[str, Any]) -> tuple[bool, str]:
    """Résout l'impact ménage effectif + le comportement catalogue.

    FORCE → toujours Oui. INTERDIT → toujours Non. CHOIX → valeur formulaire.
    """
    comportement = impact.menage_comportement(categorie_id)
    if comportement == impact.MENAGE_FORCE:
        return True, comportement
    if comportement == impact.MENAGE_INTERDIT:
        return False, comportement
    val = str(form_data.get("impact_menage", "")).strip().upper()
    return (val == "OUI"), comportement


def compute_guidee(
    form_data: dict[str, Any],
    refs: dict[str, Any],
    mois: str,
) -> dict[str, Any]:
    """Valide et calcule les impacts guidés (périmètre / ménage / réserve / avantage / effet).

    Retourne {errors, impact_menage, perimetre, menage, reserve, avantage, effet, affectation}.
    Aucune écriture réelle : structures de prévisualisation uniquement.
    """
    errors: list[dict[str, str]] = []

    def err(code: str, message: str) -> None:
        errors.append({"code": code, "message": message})

    categorie_id = str(form_data.get("categorie_charge_id", "")).strip()
    code_impact = str(form_data.get("code_impact", "")).strip().upper()
    try:
        montant = float(str(form_data.get("montant", "0")).strip().replace(",", "."))
    except ValueError:
        montant = 0.0

    impact_menage, comportement = resolve_impact_menage(categorie_id, form_data)

    # Cohérence impact ménage vs catalogue
    form_menage = str(form_data.get("impact_menage", "")).strip().upper()
    if comportement == impact.MENAGE_INTERDIT and form_menage == "OUI":
        err("V20_IMPACT_MENAGE_INTERDIT",
            f"La catégorie {categorie_id} ne peut pas impacter le coût ménage.")

    gestion_rows = refs.get("gestion_logements", [])
    perimetre = None
    menage = None
    reserve = None
    refacturable_effectif = False
    avantage_associe = False
    # Avantage associé : champ DISTINCT du mode de paiement (associe_id du paiement).
    avantage_assoc_id = str(form_data.get("avantage_associe_id", "")).strip() or None

    if impact_menage:
        # ── Parcours ménage (analytique) ──
        mode = str(form_data.get("menage_mode", "")).strip().upper()
        menage_mois = str(form_data.get("menage_mois", "")).strip() or mois
        if mode not in (impact.MENAGE_MODE_INTERVENANT, impact.MENAGE_MODE_LOGEMENT):
            err("V16_MENAGE_PARCOURS_DEDIE",
                "Parcours ménage requis : choisir la répartition par intervenant(s) OU par logement(s).")
        else:
            menage = impact.menage_perimetre(
                mode,
                _getlist(form_data, "menage_intervenants"),
                _getlist(form_data, "menage_logements"),
                _getlist(form_data, "menage_proprietaires"),
                menage_mois,
                gestion_rows,
            )
            if menage["nb"] == 0:
                err("V21_MENAGE_PERIMETRE_VIDE",
                    "Le périmètre ménage est vide : sélectionner au moins un intervenant ou un logement.")
        # Charge ménage jamais refacturable
        if str(form_data.get("refacturable", "")).strip().upper() == "OUI":
            err("V22_MENAGE_NON_REFACTURABLE",
                "Une charge ménage ne peut jamais être refacturable.")
    else:
        # ── Périmètre analytique non ménage ──
        logements_directs = _getlist(form_data, "logements")
        proprietaires = _getlist(form_data, "proprietaires")
        # Validation existence
        valid_logs = {str(l.get("logement_id", "")).strip() for l in refs.get("logements", [])}
        for lid in logements_directs:
            if lid not in valid_logs:
                err("V23_LOGEMENT_INCONNU", f"Logement inconnu : {lid!r}.")
        perimetre = impact.compute_perimetre_logements(
            logements_directs, proprietaires, mois, gestion_rows
        )
        # Refacturable : seulement si ≥1 logement final
        if str(form_data.get("refacturable", "")).strip().upper() == "OUI":
            if perimetre["nb_logements_finaux"] < 1:
                err("V24_REFAC_SANS_LOGEMENT",
                    "Refacturable impossible : le périmètre final ne comprend aucun logement cible.")
            else:
                refacturable_effectif = True

    # ── Avantage associé ──
    form_avantage = str(form_data.get("avantage_associe", "")).strip().upper()
    if form_avantage == "OUI":
        if not impact.avantage_possible(categorie_id):
            err("V25_AVANTAGE_INTERDIT",
                f"L'avantage associé n'est pas applicable à la catégorie {categorie_id}.")
        elif not avantage_assoc_id:
            err("V26_AVANTAGE_SANS_ASSOCIE",
                "Avantage associé = Oui : la sélection de l'associé est obligatoire.")
        else:
            valid_assoc = {str(a.get("personne_id", "")).strip() for a in refs.get("associes", [])}
            if avantage_assoc_id not in valid_assoc:
                err("V26_AVANTAGE_ASSOCIE_INVALIDE", f"Associé inconnu : {avantage_assoc_id!r}.")
            else:
                avantage_associe = True

    # ── Réserve de facturation (si refacturable effectif, non ménage) ──
    if refacturable_effectif and perimetre:
        quotes = impact.repartir_egal(montant, perimetre["logements_finaux"])
        prop_par_log = {}
        for r in gestion_rows:
            if impact.gestion_active_pour_mois(r, mois):
                prop_par_log.setdefault(
                    str(r.get("logement_id", "")).strip(), str(r.get("proprietaire_id", "")).strip()
                )
        reserve = impact.build_reserve_refacturation(
            charge_id="(prévisualisation)",
            mois=mois,
            libelle=str(form_data.get("commentaire", "")).strip()
            or impact.catalog_entry(categorie_id).get("label", categorie_id) if impact.catalog_entry(categorie_id) else categorie_id,
            justificatif=str(form_data.get("justificatif", "")).strip() or None,
            quotes_parts=quotes,
            proprietaire_par_logement=prop_par_log,
        )

    effet = impact.build_effet_saisie(
        code_impact=code_impact,
        impact_menage=impact_menage,
        perimetre=perimetre,
        menage=menage,
        refacturable=refacturable_effectif,
        reserve=reserve,
        avantage_associe=avantage_associe,
        associe_id=avantage_assoc_id,
    )

    return {
        "errors": errors,
        "impact_menage": impact_menage,
        "menage_comportement": comportement,
        "perimetre": perimetre,
        "menage": menage,
        "reserve": reserve,
        "refacturable_effectif": refacturable_effectif,
        "avantage_associe": avantage_associe,
        "associe_id": avantage_assoc_id if avantage_associe else None,
        "effet": effet,
    }


class ChargesPreviewError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _token() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"CHG_{stamp}_{uuid.uuid4().hex[:12]}"


def _safe_token(token: str) -> str:
    clean = str(token).strip()
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-TZ")
    if not clean or any(ch not in allowed for ch in clean):
        raise ChargesPreviewError("Identifiant de prévisualisation invalide")
    return clean


def _assert_under(path: Path, allowed_root: Path) -> Path:
    resolved = Path(path).resolve()
    root = Path(allowed_root).resolve()
    if resolved != root and root not in resolved.parents:
        raise ChargesPreviewError(f"Chemin refusé hors dry-run : {resolved}")
    return resolved


def load_form_refs(ref_path: Path | None = None) -> dict[str, Any]:
    """Charge toutes les données dropdown nécessaires au formulaire."""
    p = ref_path or cfg.REF_SETUP
    categories = read_ref_categories_charges(p)
    types_flux = read_ref_types_flux(p)
    codes_impact = read_ref_codes_impact(p)
    modes_paiement = read_ref_modes_paiement(p)
    associes = read_ref_associes(p)
    cartes = read_ref_cartes_paiement(p)
    logements = read_ref_logements(p)
    cloture = read_ref_cloture(p)
    assoc_mode_rows = read_ref_assoc_mode(p)
    affectation_types = read_ref_types_affectation(p)
    statuts = read_ref_statuts(p)
    gestion_rows = read_ref_gestion_logements(p)
    intervenants = read_ref_intervenants(p)
    couts_standards = read_ref_couts_standards_menage(p)
    proprietaires = read_ref_proprietaires(p)

    def is_active(row: dict[str, Any]) -> bool:
        return str(row.get("actif", "")).upper() == "OUI"

    # Famille statut_controle (jamais famille import) — sert au contrôle défensif V14.
    statuts_controle = [
        s for s in statuts
        if s.get("famille_statut") == "statut_controle" and is_active(s)
    ]
    mois_ouverts = [
        m for m in cloture
        if str(m.get("statut_mois", "")).upper() != "CLOTURE"
    ]

    categories_actives = [c for c in categories if is_active(c)]

    def famille(c: dict[str, Any]) -> str:
        return str(c.get("famille_impact_categorie", "")).strip()

    def visible_form(c: dict[str, Any]) -> bool:
        cid = str(c.get("categorie_charge_id", "")).strip()
        # Visible = présente au catalogue métier, hors exclusions explicites.
        return (
            cid in impact.CATEGORY_CATALOG
            and cid not in impact.CATEGORIES_HORS_FORMULAIRE_EXPLICITE
        )

    # Enrichit chaque catégorie visible du libellé + comportement métier (catalogue).
    def enrichie(c: dict[str, Any]) -> dict[str, Any]:
        cid = str(c.get("categorie_charge_id", "")).strip()
        e = impact.CATEGORY_CATALOG.get(cid, {})
        d = dict(c)
        d["label_metier"] = e.get("label", cid)
        d["groupe_metier"] = e.get("groupe", "Autre")
        d["menage_comportement"] = e.get("menage", impact.MENAGE_INTERDIT)
        d["avantage_possible"] = bool(e.get("avantage", False))
        return d

    return {
        # Dropdown : catégories du catalogue métier (inclut ménage → parcours ménage).
        "categories": [enrichie(c) for c in categories_actives if visible_form(c)],
        # Toutes les catégories actives (famille intacte) — validation défensive côté serveur.
        "categories_all": categories_actives,
        "gestion_logements": gestion_rows,
        "intervenants": [i for i in intervenants if is_active(i)],
        "couts_standards": couts_standards,
        "proprietaires": [pr for pr in proprietaires if is_active(pr)],
        # types_flux conservé pour lookup code_impact_defaut (dérivation), pas pour dropdown.
        "types_flux": [t for t in types_flux if is_active(t)],
        # Code impact : IC et HC seulement (HR hors formulaire standard).
        "codes_impact": [
            c for c in codes_impact
            if is_active(c) and str(c.get("code_impact", "")).strip() in STANDARD_CODES_IMPACT
        ],
        # Modes de paiement : hors modes interdits en standard (PAY_005, PAY_006).
        "modes_paiement": [
            m for m in modes_paiement
            if is_active(m) and str(m.get("mode_paiement_id", "")).strip() not in MODES_INTERDITS_STANDARD
        ],
        "associes": [a for a in associes if is_active(a)],
        "cartes": [c for c in cartes if is_active(c)],
        "logements": [l for l in logements if is_active(l)],
        "assoc_mode": assoc_mode_rows,
        # Non filtré par actif : la valeur canonique (LOGEMENT/PROPRIETAIRE/GLOBAL/NON_AFFECTABLE)
        # est la vérité (REF_LOCALE). V11 valide contre CANONICAL_AFFECTATION.
        "affectation_types": affectation_types,
        "statuts_controle": statuts_controle,
        "mois_ouverts": mois_ouverts,
        "cloture": cloture,
    }


def _type_flux_impact_defaut(type_flux_id: str, refs: dict[str, Any]) -> str:
    """code_impact_defaut d'un type_flux (ou '' si inconnu)."""
    tid = str(type_flux_id or "").strip()
    for t in refs.get("types_flux", []):
        if str(t.get("type_flux_id", "")).strip() == tid:
            return str(t.get("code_impact_defaut", "")).strip().upper()
    return ""


def category_famille(categorie_id: str, refs: dict[str, Any]) -> str:
    """Retourne famille_impact_categorie de la catégorie (ou '' si inconnue).

    Cherche dans categories_all (toutes actives) pour permettre la validation défensive
    des familles MENAGE / PARCOURS_DEDIE absentes du dropdown.
    """
    cid = str(categorie_id or "").strip()
    pool = refs.get("categories_all") or refs.get("categories", [])
    for c in pool:
        if str(c.get("categorie_charge_id", "")).strip() == cid:
            return str(c.get("famille_impact_categorie", "")).strip()
    return ""


def resolve_profil_impact(categorie_id: str, refs: dict[str, Any]) -> str:
    """Profil_impact_charge final pour une charge previsualisable.

    CHG_024 → GLOBAL forcé. Sinon dérivé de la famille (GLOBAL / LOGEMENT_DIRECT).
    Les familles MENAGE et PARCOURS_DEDIE ne sont pas previsualisables (bloquées en amont).
    """
    if str(categorie_id or "").strip() == CATEGORIE_PERSONNALISEE:
        return "GLOBAL"
    famille = category_famille(categorie_id, refs)
    return PROFIL_BY_FAMILLE.get(famille, "GLOBAL")


def resolve_assoc_mode(
    mode_paiement_id: str,
    associe_id: str | None,
    assoc_mode_rows: list[dict[str, Any]],
) -> str | None:
    """Résout ASSOC_MODE depuis REF_Assoc_Mode.

    Retourne None si introuvable (ASSOC_MODE_NON_RESOLVABLE).
    """
    mode = str(mode_paiement_id or "").strip()
    assoc = str(associe_id or "").strip()
    for row in assoc_mode_rows:
        row_mode = str(row.get("mode_paiement_id", "") or "").strip()
        row_assoc = str(row.get("associe_id", "") or "").strip()
        row_actif = str(row.get("actif", "") or "").upper()
        if row_mode == mode and row_assoc == assoc and row_actif == "OUI":
            return str(row.get("assoc_mode", "")).strip()
    return None


def generate_charge_id(
    date_charge: date,
    code_impact: str,
    assoc_mode: str,
    saisie_path: Path | None = None,
) -> str:
    """Génère CHG-{AAAA}-{MM}-{IMPACT}-{ASSOC_MODE}-{NNN}."""
    aaaa = date_charge.strftime("%Y")
    mm = date_charge.strftime("%m")
    prefix = f"CHG-{aaaa}-{mm}-{code_impact}-{assoc_mode}"
    count = count_charges_with_prefix(prefix, saisie_path)
    nnn = str(count + 1).zfill(3)
    return f"{prefix}-{nnn}"


def validate_charge(
    form_data: dict[str, str],
    refs: dict[str, Any],
    saisie_path: Path | None = None,
    resolues_path: Path | None = None,
) -> list[dict[str, str]]:
    """Applique toutes les règles de validation bloquantes.

    Retourne une liste d'erreurs [{code, message}].
    """
    errors: list[dict[str, str]] = []

    def err(code: str, message: str) -> None:
        errors.append({"code": code, "message": message})

    def has_err(*prefixes: str) -> bool:
        return any(e["code"].startswith(p) for e in errors for p in prefixes)

    # V01 — date_charge obligatoire et valide
    date_charge_raw = str(form_data.get("date_charge", "")).strip()
    date_charge: date | None = None
    if not date_charge_raw:
        err("V01_DATE_MANQUANTE", "La date de la charge est obligatoire.")
    else:
        try:
            date_charge = date.fromisoformat(date_charge_raw)
        except ValueError:
            err("V01_DATE_INVALIDE", f"Format de date invalide : {date_charge_raw!r} (attendu : YYYY-MM-DD).")

    # V02 — mois non clôturé
    if date_charge is not None:
        mois_charge = date_charge.strftime("%Y-%m")
        cloture_map = {
            str(r.get("mois", "")).strip(): str(r.get("statut_mois", "")).strip().upper()
            for r in refs["cloture"]
        }
        statut_mois = cloture_map.get(mois_charge)
        if statut_mois == "CLOTURE":
            err("V02_MOIS_CLOTURE", f"Le mois {mois_charge} est clôturé — saisie impossible.")

    # V03 — montant obligatoire et > 0
    montant_raw = str(form_data.get("montant", "")).strip().replace(",", ".")
    if not montant_raw:
        err("V03_MONTANT_MANQUANT", "Le montant est obligatoire.")
    else:
        try:
            montant_val = float(montant_raw)
            if montant_val <= 0:
                err("V03_MONTANT_INVALIDE", f"Le montant doit être strictement positif (saisi : {montant_raw}).")
        except ValueError:
            err("V03_MONTANT_NON_NUMERIQUE", f"Montant non convertible : {montant_raw!r}.")

    # V04 — categorie_charge_id obligatoire, valide, visible au catalogue Nouvelle charge.
    categorie_id = str(form_data.get("categorie_charge_id", "")).strip()
    valid_categories_all = {str(c.get("categorie_charge_id", "")).strip() for c in refs.get("categories_all", refs["categories"])}
    famille = category_famille(categorie_id, refs) if categorie_id else ""
    if not categorie_id:
        err("V04_CATEGORIE_MANQUANTE", "La catégorie de charge est obligatoire.")
    elif categorie_id not in valid_categories_all:
        err("V04_CATEGORIE_INVALIDE", f"Catégorie inconnue : {categorie_id!r}.")
    elif famille == FAMILLE_PARCOURS_DEDIE:
        err("V04_CATEGORIE_HORS_FORMULAIRE",
            f"La catégorie {categorie_id} relève d'un parcours dédié — hors formulaire Nouvelle charge standard.")
    elif (categorie_id not in impact.CATEGORY_CATALOG
          or categorie_id in impact.CATEGORIES_HORS_FORMULAIRE_EXPLICITE):
        err("V04_CATEGORIE_HORS_FORMULAIRE",
            f"La catégorie {categorie_id} n'est pas saisissable en Nouvelle charge "
            f"(forfait client / récurrente / parcours dédié).")

    # Mode guidé (nouveau formulaire) = absence du champ legacy affectation_type.
    # Le formulaire guidé pilote l'affectation par périmètre/ménage (pas de affectation_type).
    guided = "affectation_type" not in form_data

    # V05 — type_flux_id N'EST PLUS saisi : dérivé côté serveur (D-CHG-TYPEFLUX-01).
    # Toute valeur type_flux_id envoyée par le navigateur est ignorée.

    # V06 — code_impact obligatoire ; formulaire standard = IC ou HC seulement (HR exclu)
    code_impact = str(form_data.get("code_impact", "")).strip().upper()
    if not code_impact:
        err("V06_CODE_IMPACT_MANQUANT", "Le code d'impact est obligatoire.")
    elif code_impact not in STANDARD_CODES_IMPACT:
        err("V06_CODE_IMPACT_HORS_STANDARD",
            f"Code impact {code_impact!r} hors formulaire standard : seuls IC (résultat réel et comptable) "
            f"et HC (résultat réel, hors compta) sont autorisés. HR relève d'un parcours dédié.")

    # V07 — mode_paiement_id obligatoire, valide, et autorisé en standard (PAY_005/PAY_006 interdits)
    mode_paiement_id = str(form_data.get("mode_paiement_id", "")).strip()
    valid_modes = {str(m.get("mode_paiement_id", "")).strip() for m in refs["modes_paiement"]}
    if not mode_paiement_id:
        err("V07_MODE_PAIEMENT_MANQUANT", "Le mode de paiement est obligatoire.")
    elif mode_paiement_id in MODES_INTERDITS_STANDARD:
        err("V07_MODE_PAIEMENT_INTERDIT",
            f"Le mode {mode_paiement_id} est interdit en Nouvelle charge standard "
            f"(PAY_005 A_DEFINIR, PAY_006 DIRECT_PROPRIETAIRE — aucune sortie d'argent conciergerie).")
    elif mode_paiement_id not in valid_modes:
        err("V07_MODE_PAIEMENT_INVALIDE", f"Mode de paiement inconnu : {mode_paiement_id!r}.")

    # V08 — associe_id requis si PAY_003 ou PAY_004
    associe_id = str(form_data.get("associe_id", "")).strip() or None
    if mode_paiement_id in MODES_REQUIRES_ASSOCIE:
        valid_associes = {str(a.get("personne_id", "")).strip() for a in refs["associes"]}
        if not associe_id:
            err("V08_ASSOCIE_MANQUANT",
                f"L'associé(e) est obligatoire pour le mode {mode_paiement_id}.")
        elif associe_id not in valid_associes:
            err("V08_ASSOCIE_INVALIDE", f"Associé(e) inconnu(e) : {associe_id!r}.")

    # V09 — carte_id requis si PAY_003, interdit sinon, cohérence carte/associe
    carte_id = str(form_data.get("carte_id", "")).strip() or None
    if mode_paiement_id == MODE_CARTE:
        valid_cartes = {str(c.get("carte_id", "")).strip() for c in refs["cartes"]}
        if not carte_id:
            err("V09_CARTE_MANQUANTE",
                "La carte est obligatoire pour le mode CARTE_ASSOCIEE (PAY_003).")
        elif carte_id not in valid_cartes:
            err("V09_CARTE_INVALIDE", f"Carte inconnue : {carte_id!r}.")
        elif associe_id:
            cartes_map = {str(c.get("carte_id", "")): c for c in refs["cartes"]}
            carte_row = cartes_map.get(carte_id or "")
            if carte_row:
                carte_personne = str(carte_row.get("personne_id", "") or "").strip()
                if carte_personne != associe_id:
                    err("V09_CARTE_MAUVAIS_ASSOCIE",
                        f"La carte {carte_id} appartient à {carte_personne}, pas à {associe_id}.")
    elif carte_id:
        err("V09_CARTE_INTERDITE",
            f"La carte ne doit pas être saisie pour le mode {mode_paiement_id}.")

    # V10 — ASSOC_MODE résolvable
    if not has_err("V07_MODE_PAIEMENT", "V08_ASSOCIE") and mode_paiement_id:
        am = resolve_assoc_mode(mode_paiement_id, associe_id, refs["assoc_mode"])
        if am is None:
            err("V10_ASSOC_MODE_NON_RESOLVABLE",
                f"Aucune correspondance REF_Assoc_Mode pour mode={mode_paiement_id}, "
                f"associe={associe_id!r}.")

    # V11/V12/V12b — affectation legacy (mono-champ). Ignorées en mode guidé (périmètre/ménage).
    affectation_type = str(form_data.get("affectation_type", "")).strip().upper()
    logement_id = str(form_data.get("logement_id", "")).strip() or None
    proprietaire_id = str(form_data.get("proprietaire_id", "")).strip() or None
    if not guided:
        if not affectation_type:
            err("V11_AFFECTATION_MANQUANTE", "Le type d'affectation est obligatoire.")
        elif affectation_type not in CANONICAL_AFFECTATION:
            err("V11_AFFECTATION_INVALIDE",
                f"Type d'affectation invalide : {affectation_type!r} "
                f"(attendu : LOGEMENT / PROPRIETAIRE / GLOBAL / NON_AFFECTABLE).")
        if affectation_type == "LOGEMENT":
            valid_logements = {str(l.get("logement_id", "")).strip() for l in refs["logements"]}
            if not logement_id:
                err("V12_LOGEMENT_MANQUANT",
                    "Le logement est obligatoire pour l'affectation LOGEMENT.")
            elif logement_id not in valid_logements:
                err("V12_LOGEMENT_INVALIDE", f"Logement inconnu : {logement_id!r}.")
        if affectation_type == "PROPRIETAIRE" and not proprietaire_id:
            err("V12B_PROPRIETAIRE_MANQUANT",
                "Le propriétaire est obligatoire pour l'affectation PROPRIETAIRE.")

    # V17/V18 — Catégorie personnalisée CHG_024 : GLOBAL forcé + verrous anti-contournement
    if categorie_id == CATEGORIE_PERSONNALISEE:
        if not str(form_data.get("libelle_categorie_personnalise", "")).strip():
            err("V17_LIBELLE_PERSONNALISE_MANQUANT",
                "Un libellé de catégorie personnalisée est obligatoire pour CHG_024.")
        if affectation_type in ("LOGEMENT", "PROPRIETAIRE") or logement_id or proprietaire_id:
            err("V18_PERSONNALISE_NON_GLOBAL",
                "Une catégorie personnalisée est toujours GLOBAL : ni logement ni propriétaire.")
        if str(form_data.get("reservation_id", "")).strip():
            err("V18_PERSONNALISE_RESERVATION_INTERDITE",
                "Réservation interdite pour une catégorie personnalisée (jamais incident voyageur).")
        if str(form_data.get("intervenant_concerne", "")).strip():
            err("V18_PERSONNALISE_INTERVENANT_INTERDIT",
                "Intervenant interdit pour une catégorie personnalisée (jamais ménage).")
        if str(form_data.get("refacturable", "")).strip().upper() == "OUI":
            err("V18_PERSONNALISE_REFAC_INTERDITE",
                "Refacturable interdit pour une catégorie personnalisée.")

    # V13 — reservation_id requis si CHG_021, et doit exister dans le dataset RESOLU courant
    reservation_id = str(form_data.get("reservation_id", "")).strip() or None
    if categorie_id in CATEGORIE_REQUIRES_RESERVATION:
        if not reservation_id:
            err("V13_RESERVATION_MANQUANTE",
                f"La catégorie {categorie_id} (Incident voyageur) requiert un reservation_id (D041).")
        else:
            if not reservation_id_exists(reservation_id, resolues_path):
                err("V13_RESERVATION_INCONNUE",
                    f"Réservation introuvable dans les réservations résolues : "
                    f"{reservation_id!r}.")

    # V14 — statut_controle injecté automatiquement (A_CONTROLER) ; contrôle défensif famille
    # statut_controle (jamais famille import). Le statut n'est jamais saisi par l'utilisateur.
    valid_statuts_controle = {str(s.get("statut", "")).strip() for s in refs["statuts_controle"]}
    if AUTO_STATUT_CONTROLE not in valid_statuts_controle:
        err("V14_STATUT_CONTROLE_REF_ABSENT",
            f"Statut auto {AUTO_STATUT_CONTROLE} absent de la famille statut_controle du référentiel.")

    # V15 — sens_flux injecté serveur (DEPENSE) : jamais saisi. Valeur navigateur ignorée. Aucune validation.

    # V05d — type_flux_id dérivé côté serveur : doit être déductible (D-CHG-TYPEFLUX-01)
    type_flux_derive: str | None = None
    if not has_err("V04_CATEGORIE", "V07_MODE_PAIEMENT") and categorie_id and mode_paiement_id:
        type_flux_derive = derive_type_flux(
            categorie_id,
            mode_paiement_id,
            form_data.get("refacturable"),
            form_data.get("paye_avec_montant_recupere"),
        )
        if type_flux_derive is None:
            err("V05_TYPE_FLUX_NON_DERIVABLE",
                f"Impossible de déduire le type de flux pour catégorie={categorie_id}, "
                f"mode={mode_paiement_id}.")

    # V19 — commentaire de justification obligatoire si code_impact ≠ code_impact_defaut du type
    if type_flux_derive and code_impact in STANDARD_CODES_IMPACT:
        impact_defaut = _type_flux_impact_defaut(type_flux_derive, refs)
        if impact_defaut and code_impact != impact_defaut:
            if not str(form_data.get("commentaire", "")).strip():
                err("V19_COMMENTAIRE_JUSTIFICATION_REQUIS",
                    f"L'impact choisi ({code_impact}) diffère du défaut du type {type_flux_derive} "
                    f"({impact_defaut}) — un commentaire de justification est obligatoire.")

    # V16/V20-V26 — validation guidée (ménage, périmètre, refacturable, avantage)
    mois_guide = date_charge.strftime("%Y-%m") if date_charge is not None else ""
    guide = compute_guidee(form_data, refs, mois_guide)
    errors.extend(guide["errors"])

    return errors


def _build_row_data(
    form_data: dict[str, str],
    charge_id: str,
    profil_impact: str | None = None,
    type_flux_id: str | None = None,
    guide: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Construit le dictionnaire de données à injecter dans la copie SAISIE.

    La ligne SAISIE reste UNE charge économique unique. Les ventilations analytiques
    multi-logements / ménage sont portées par le manifest (jamais concaténées en cellule).
    Valeurs techniques injectées côté serveur (jamais du navigateur) : sens_flux=DEPENSE,
    statut_controle=A_CONTROLER, niveau_anomalie=INFO, prise_en_compta dérivée, type_flux_id dérivé.
    CHG_024 : valeurs métier forcées. Charge ménage : affectable_menage=OUI, refacturable=NON.
    """
    date_charge_raw = str(form_data.get("date_charge", "")).strip()
    date_charge_val: date | None = None
    try:
        date_charge_val = date.fromisoformat(date_charge_raw)
    except (ValueError, TypeError):
        pass

    montant_raw = str(form_data.get("montant", "0")).strip().replace(",", ".")
    try:
        montant_val: float | None = float(montant_raw)
    except ValueError:
        montant_val = None

    # code_impact standard (IC/HC) → prise_en_compta dérivée (IC=OUI, HC=NON)
    code_impact = str(form_data.get("code_impact", "")).strip().upper() or None
    prise_en_compta = PRISE_EN_COMPTA_BY_IMPACT.get(code_impact) if code_impact else None

    categorie_id = str(form_data.get("categorie_charge_id", "")).strip() or None
    is_perso = categorie_id == CATEGORIE_PERSONNALISEE
    g = guide or {}
    impact_menage = bool(g.get("impact_menage"))
    perimetre = g.get("perimetre") or {}

    if is_perso:
        # CHG_024 : valeurs métier FORCÉES côté serveur (ignore toute valeur navigateur contradictoire)
        affectation_type = "GLOBAL"
        affectable_menage = "NON"
        refacturable = "NON"
        logement_id = None
        proprietaire_id = None
        reservation_id = None
        intervenant_concerne = None
    elif impact_menage:
        # Charge ménage : analytique. Ligne = charge économique unique, affectation GLOBAL,
        # affectable_menage=OUI, jamais refacturable. Ventilation ménage portée par le manifest.
        affectation_type = "GLOBAL"
        affectable_menage = "OUI"
        refacturable = "NON"
        logement_id = None
        proprietaire_id = None
        reservation_id = None
        intervenant_concerne = None
    elif g:
        # Charge non ménage guidée : affectation dérivée du périmètre déterministe.
        finaux = perimetre.get("logements_finaux", [])
        # Le périmètre a DÉJÀ résolu le propriétaire de façon historisée (gestion active du mois).
        # On le MATÉRIALISE sur la ligne : Lot10 n'infère jamais le propriétaire, donc sans cette
        # valeur une charge refacturable ne rejoint ni la préfacture ni le net propriétaire.
        prop_par_log: dict[str, str] = perimetre.get("proprietaire_par_logement", {}) or {}
        if len(finaux) == 1:
            affectation_type = "LOGEMENT"
            logement_id = finaux[0]
            proprietaire_id = (prop_par_log.get(logement_id) or "").strip() or None
        else:
            # 0 ou >1 logements → charge économique GLOBAL (ventilation multi en manifest)
            affectation_type = "GLOBAL"
            logement_id = None
            props = perimetre.get("proprietaires", [])
            if len(props) == 1 and not finaux:
                proprietaire_id = props[0]
            else:
                # Plusieurs logements : propriétaire matérialisé UNIQUEMENT s'il est unique
                # (aucune ambiguïté). Sinon None — jamais d'attribution arbitraire.
                proprios_finaux = {
                    (prop_par_log.get(lg) or "").strip()
                    for lg in finaux
                    if (prop_par_log.get(lg) or "").strip()
                }
                proprietaire_id = proprios_finaux.pop() if len(proprios_finaux) == 1 else None
        affectable_menage = "NON"
        refacturable = "OUI" if g.get("refacturable_effectif") else "NON"
        reservation_id = None
        intervenant_concerne = None
    else:
        # Legacy mono-affectation
        affectation_type = str(form_data.get("affectation_type", "")).strip().upper() or None
        affectable_menage = str(form_data.get("affectable_menage", "")).strip() or None
        refacturable = str(form_data.get("refacturable", "")).strip() or None
        logement_id = str(form_data.get("logement_id", "")).strip() or None
        proprietaire_id = str(form_data.get("proprietaire_id", "")).strip() or None
        reservation_id = str(form_data.get("reservation_id", "")).strip() or None
        intervenant_concerne = str(form_data.get("intervenant_concerne", "")).strip() or None

    return {
        "charge_id": charge_id,
        "date_charge": date_charge_val.isoformat() if date_charge_val else date_charge_raw,
        "montant": montant_val,
        # sens_flux injecté serveur (jamais saisi)
        "sens_flux": DEFAULT_SENS_FLUX,
        "categorie_charge_id": categorie_id,
        # type_flux_id dérivé serveur (jamais saisi)
        "type_flux_id": type_flux_id,
        "code_impact": code_impact,
        # Dérivée du code_impact (D-APP-2B-REV1, D012) — jamais saisie directement
        "prise_en_compta": prise_en_compta,
        "associe_id": str(form_data.get("associe_id", "")).strip() or None,
        "mode_paiement_id": str(form_data.get("mode_paiement_id", "")).strip() or None,
        "carte_id": str(form_data.get("carte_id", "")).strip() or None,
        "affectation_type": affectation_type,
        "logement_id": logement_id,
        "proprietaire_id": proprietaire_id,
        "reservation_id": reservation_id,
        "refacturable": refacturable,
        "source_flux": "SAISIE_MANUELLE",
        "methode_traitement": str(form_data.get("methode_traitement", "")).strip() or None,
        "paye_avec_montant_recupere": str(form_data.get("paye_avec_montant_recupere", "")).strip() or None,
        "lien_virement_banque": str(form_data.get("lien_virement_banque", "")).strip() or None,
        # Injectés automatiquement — jamais saisis par l'utilisateur
        "statut_controle": AUTO_STATUT_CONTROLE,
        "niveau_anomalie": AUTO_NIVEAU_ANOMALIE,
        "code_anomalie": str(form_data.get("code_anomalie", "")).strip() or None,
        "statut_rapprochement": "NON_RAPPROCHE",
        "justificatif": str(form_data.get("justificatif", "")).strip() or None,
        "commentaire": str(form_data.get("commentaire", "")).strip() or None,
        "date_saisie": date.today().isoformat(),
        "affectable_menage": affectable_menage,
        "intervenant_concerne": intervenant_concerne,
        # Profil d'impact final + libellé personnalisé (CHG_024)
        "profil_impact_charge": profil_impact,
        "libelle_categorie_personnalise": str(form_data.get("libelle_categorie_personnalise", "")).strip() or None,
        # Avantage associé porté par la charge (bénéficiaire, distinct du paiement) — agrégé par Lot7
        "avantage_associe_id": (guide or {}).get("associe_id") if (guide or {}).get("avantage_associe") else None,
    }


def _inject_row(copy_path: Path, target_row: int, row_data: dict[str, Any]) -> None:
    """Injecte row_data dans la copie SAISIE à target_row.

    Ne touche jamais aux colonnes formule (C, I, J, AD).
    """
    wb = openpyxl.load_workbook(str(copy_path), data_only=False)
    try:
        ws = wb["SAISIE"]
        for col_letter, field_name in MANUAL_COL_MAP.items():
            col_idx = _col_index(col_letter)
            if col_idx in FORMULA_COL_INDICES:
                continue
            value = row_data.get(field_name)
            ws.cell(row=target_row, column=col_idx, value=value)
        wb.calculation.fullCalcOnLoad = True
        wb.save(str(copy_path))
    finally:
        wb.close()


def previsualiser(
    form_data: dict[str, str],
    *,
    saisie_source: Path | None = None,
    ref_path: Path | None = None,
    dryruns_root: Path | None = None,
    resolues_path: Path | None = None,
) -> dict[str, Any]:
    """Valide et crée une copie prévisualisée.

    N'écrit jamais dans la source réelle SAISIE_Charges_Flux.xlsx.
    Toutes les copies restent sous DRYRUNS_DIR.
    """
    source = Path(saisie_source or cfg.SAISIE_CHARGES)
    root = Path(dryruns_root or DRYRUNS_DIR)
    token = _token()
    run_dir = root / token
    run_dir.mkdir(parents=True, exist_ok=False)

    source_hash_avant = _sha256(source)

    refs = load_form_refs(ref_path)
    errors = validate_charge(form_data, refs, saisie_path=source, resolues_path=resolues_path)

    if errors:
        manifest: dict[str, Any] = {
            "token": token,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "app_version": "APP-3b-1",
            "charges_real_write_enabled": cfg.CHARGES_REAL_WRITE_ENABLED,
            "status": "VALIDATION_REFUSEE",
            "errors": errors,
            "form_data": form_data,
            "source_hash_avant": source_hash_avant,
            "source_inchangee": True,
        }
        (run_dir / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"ok": False, "token": token, "run_dir": run_dir, "manifest": manifest}

    # Résolution ASSOC_MODE
    mode_paiement_id = str(form_data.get("mode_paiement_id", "")).strip()
    associe_id = str(form_data.get("associe_id", "")).strip() or None
    code_impact = str(form_data.get("code_impact", "")).strip()
    date_charge_raw = str(form_data.get("date_charge", "")).strip()
    date_charge = date.fromisoformat(date_charge_raw)

    assoc_mode = resolve_assoc_mode(mode_paiement_id, associe_id, refs["assoc_mode"])
    charge_id = generate_charge_id(date_charge, code_impact, assoc_mode, source)

    # Copie source
    copy_path = run_dir / SAISIE_COPY_NAME
    shutil.copy2(source, copy_path)
    _assert_under(copy_path, root)

    target_row = find_model_row(copy_path)
    if target_row is None:
        manifest = {
            "token": token,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "app_version": "APP-3b-1",
            "charges_real_write_enabled": cfg.CHARGES_REAL_WRITE_ENABLED,
            "status": "ERREUR_LIGNE_MODELE",
            "errors": [{"code": "E_MODELE", "message": "Aucune ligne modèle trouvée dans SAISIE_Charges_Flux.xlsx."}],
            "form_data": form_data,
            "source_hash_avant": source_hash_avant,
            "source_inchangee": True,
        }
        (run_dir / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"ok": False, "token": token, "run_dir": run_dir, "manifest": manifest}

    categorie_id = str(form_data.get("categorie_charge_id", "")).strip()
    profil_impact = resolve_profil_impact(categorie_id, refs)
    # Calculs guidés (périmètre / ménage / réserve / avantage / effet)
    guide = compute_guidee(form_data, refs, mois_charge_pre := date_charge.strftime("%Y-%m"))
    # type_flux_id dérivé serveur. Pour CHG_024 refacturable forcé NON (jamais TF011).
    refac_for_derive = None if categorie_id == CATEGORIE_PERSONNALISEE else form_data.get("refacturable")
    type_flux_id = derive_type_flux(
        categorie_id,
        mode_paiement_id,
        refac_for_derive,
        form_data.get("paye_avec_montant_recupere"),
    )
    row_data = _build_row_data(
        form_data, charge_id, profil_impact=profil_impact, type_flux_id=type_flux_id, guide=guide
    )
    _inject_row(copy_path, target_row, row_data)

    # ── Persistance durable (sur COPIE contrôlée, jamais le fichier réel — flags off) ──
    try:
        montant_pre = float(str(form_data.get("montant", "0")).strip().replace(",", "."))
    except ValueError:
        montant_pre = 0.0
    persistable = persist.build_persistable(charge_id, mois_charge_pre, montant_pre, guide, form_data)
    impacts_copy = run_dir / IMPACTS_COPY_NAME
    persist_report = None
    # Empreinte du fichier d'impacts RÉEL au moment de la prévisualisation : la confirmation la
    # revérifiera pour refuser d'écrire sur une base qui a bougé entre-temps.
    impacts_hash_avant = (
        _sha256(cfg.SAISIE_CHARGES_IMPACTS) if cfg.SAISIE_CHARGES_IMPACTS.exists() else None
    )
    if cfg.SAISIE_CHARGES_IMPACTS.exists():
        shutil.copy2(cfg.SAISIE_CHARGES_IMPACTS, impacts_copy)
        _assert_under(impacts_copy, root)
        # Avantage porté par la ligne charge (colonne avantage_associe_id) — pas d'écriture Lot7 ici.
        persist_report = persist.persister_sur_copie(persistable, impacts_copy)

    source_hash_apres = _sha256(source)
    source_inchangee = source_hash_avant == source_hash_apres
    copy_hash = _sha256(copy_path)
    mois_charge = date_charge.strftime("%Y-%m")

    manifest = {
        "token": token,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "app_version": "APP-3b-1",
        "charges_real_write_enabled": cfg.CHARGES_REAL_WRITE_ENABLED,
        "status": "OK",
        "errors": [],
        "form_data": form_data,
        "charge_id": charge_id,
        "mois_charge": mois_charge,
        "assoc_mode": assoc_mode,
        "profil_impact": profil_impact,
        "type_flux_id": type_flux_id,
        "target_row": target_row,
        # Impacts guidés (prévisualisation uniquement — aucune écriture réelle)
        "impact_menage": guide["impact_menage"],
        "perimetre": guide["perimetre"],
        "menage": guide["menage"],
        "reserve_refacturation": guide["reserve"],
        "refacturable_effectif": guide["refacturable_effectif"],
        "avantage_associe": guide["avantage_associe"],
        "avantage_associe_id": guide["associe_id"],
        "effet_saisie": guide["effet"],
        # Persistance durable (aperçu de ce qui serait écrit dans SAISIE_Charges_Impacts / Lot7)
        "persistable": persistable,
        "persist_report": persist_report,
        "source_hash_avant": source_hash_avant,
        "source_hash_apres": source_hash_apres,
        "source_inchangee": source_inchangee,
        "copy_hash": copy_hash,
        # Empreinte du SAISIE_Charges_Impacts réel (vérifiée à la confirmation).
        "impacts_hash_avant": impacts_hash_avant,
        "paths": {
            "run_dir": str(run_dir),
            "saisie_copy": str(copy_path),
        },
    }
    # Sceau des champs de décision — recalculé et comparé à la confirmation (détection d'altération).
    manifest["integrite"] = sceller_manifest(manifest)
    # Réserve de facturation : fichier séparé (traçabilité), jamais en cellule métier concaténée.
    if guide["reserve"]:
        (run_dir / "reserve_refacturation.json").write_text(
            json.dumps(guide["reserve"], ensure_ascii=False, indent=2), encoding="utf-8"
        )
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {"ok": True, "token": token, "run_dir": run_dir, "manifest": manifest}


def load_previsualisation(
    token: str,
    *,
    dryruns_root: Path | None = None,
) -> dict[str, Any]:
    """Charge une prévisualisation existante par token."""
    simulation_id = _safe_token(token)
    root = Path(dryruns_root or DRYRUNS_DIR)
    run_dir = root / simulation_id
    manifest_path = run_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise ChargesPreviewError("Prévisualisation introuvable.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "token": simulation_id,
        "run_dir": run_dir,
        "manifest": manifest,
    }

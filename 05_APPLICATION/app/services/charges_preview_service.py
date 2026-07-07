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
    read_ref_logements,
    read_ref_modes_paiement,
    read_ref_statuts,
    read_ref_types_affectation,
    read_ref_types_flux,
    reservation_id_exists,
)

DRYRUNS_DIR = cfg.DRYRUNS_DIR
SAISIE_COPY_NAME = "SAISIE_Charges_Flux_copie.xlsx"
MANIFEST_NAME = "manifest.json"

CATEGORIE_REQUIRES_RESERVATION: frozenset[str] = frozenset({"CHG_021"})
MODES_REQUIRES_ASSOCIE: frozenset[str] = frozenset({"PAY_003", "PAY_004"})
MODE_CARTE = "PAY_003"

# ── Alignement référentiels Excel (Commit 1 — APP aligne saisie charges) ──
# Catégories hors formulaire « Nouvelle charge » standard (parcours dédiés).
FORM_EXCLUDED_CATEGORIES: frozenset[str] = frozenset(
    {"CHG_001", "CHG_002", "CHG_014", "CHG_020", "CHG_021", "CHG_022"}
)
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

    return {
        # Catégories du formulaire standard : hors parcours dédiés (CHG_001/002/014/020/021/022).
        "categories": [
            c for c in categories
            if is_active(c)
            and str(c.get("categorie_charge_id", "")).strip() not in FORM_EXCLUDED_CATEGORIES
        ],
        "types_flux": [t for t in types_flux if is_active(t)],
        # Code impact : IC et HC seulement (HR hors formulaire standard).
        "codes_impact": [
            c for c in codes_impact
            if is_active(c) and str(c.get("code_impact", "")).strip() in STANDARD_CODES_IMPACT
        ],
        "modes_paiement": [m for m in modes_paiement if is_active(m)],
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

    # V04 — categorie_charge_id obligatoire, valide, et éligible au formulaire standard
    categorie_id = str(form_data.get("categorie_charge_id", "")).strip()
    valid_categories = {str(c.get("categorie_charge_id", "")).strip() for c in refs["categories"]}
    if not categorie_id:
        err("V04_CATEGORIE_MANQUANTE", "La catégorie de charge est obligatoire.")
    elif categorie_id in FORM_EXCLUDED_CATEGORIES:
        err("V04_CATEGORIE_HORS_FORMULAIRE",
            f"La catégorie {categorie_id} relève d'un parcours dédié — hors formulaire Nouvelle charge standard.")
    elif categorie_id not in valid_categories:
        err("V04_CATEGORIE_INVALIDE", f"Catégorie inconnue : {categorie_id!r}.")

    # V05 — type_flux_id obligatoire et valide
    type_flux_id = str(form_data.get("type_flux_id", "")).strip()
    valid_types_flux = {str(t.get("type_flux_id", "")).strip() for t in refs["types_flux"]}
    if not type_flux_id:
        err("V05_TYPE_FLUX_MANQUANT", "Le type de flux est obligatoire.")
    elif type_flux_id not in valid_types_flux:
        err("V05_TYPE_FLUX_INVALIDE", f"Type de flux inconnu : {type_flux_id!r}.")

    # V06 — code_impact obligatoire ; formulaire standard = IC ou HC seulement (HR exclu)
    code_impact = str(form_data.get("code_impact", "")).strip().upper()
    if not code_impact:
        err("V06_CODE_IMPACT_MANQUANT", "Le code d'impact est obligatoire.")
    elif code_impact not in STANDARD_CODES_IMPACT:
        err("V06_CODE_IMPACT_HORS_STANDARD",
            f"Code impact {code_impact!r} hors formulaire standard : seuls IC (résultat réel et comptable) "
            f"et HC (résultat réel, hors compta) sont autorisés. HR relève d'un parcours dédié.")

    # V07 — mode_paiement_id obligatoire et valide
    mode_paiement_id = str(form_data.get("mode_paiement_id", "")).strip()
    valid_modes = {str(m.get("mode_paiement_id", "")).strip() for m in refs["modes_paiement"]}
    if not mode_paiement_id:
        err("V07_MODE_PAIEMENT_MANQUANT", "Le mode de paiement est obligatoire.")
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

    # V11 — affectation_type obligatoire et valide (valeurs canoniques REF_LOCALE, jamais AFF_*)
    affectation_type = str(form_data.get("affectation_type", "")).strip().upper()
    if not affectation_type:
        err("V11_AFFECTATION_MANQUANTE", "Le type d'affectation est obligatoire.")
    elif affectation_type not in CANONICAL_AFFECTATION:
        err("V11_AFFECTATION_INVALIDE",
            f"Type d'affectation invalide : {affectation_type!r} "
            f"(attendu : LOGEMENT / PROPRIETAIRE / GLOBAL / NON_AFFECTABLE).")

    # V12 — logement_id requis si affectation LOGEMENT
    logement_id = str(form_data.get("logement_id", "")).strip() or None
    if affectation_type == "LOGEMENT":
        valid_logements = {str(l.get("logement_id", "")).strip() for l in refs["logements"]}
        if not logement_id:
            err("V12_LOGEMENT_MANQUANT",
                "Le logement est obligatoire pour l'affectation LOGEMENT.")
        elif logement_id not in valid_logements:
            err("V12_LOGEMENT_INVALIDE", f"Logement inconnu : {logement_id!r}.")

    # V12b — proprietaire_id requis si affectation PROPRIETAIRE
    proprietaire_id = str(form_data.get("proprietaire_id", "")).strip() or None
    if affectation_type == "PROPRIETAIRE" and not proprietaire_id:
        err("V12B_PROPRIETAIRE_MANQUANT",
            "Le propriétaire est obligatoire pour l'affectation PROPRIETAIRE.")

    # V13 — reservation_id requis si CHG_021 et doit exister dans MASTER_CALC_Reservations_Resolues
    reservation_id = str(form_data.get("reservation_id", "")).strip() or None
    if categorie_id in CATEGORIE_REQUIRES_RESERVATION:
        if not reservation_id:
            err("V13_RESERVATION_MANQUANTE",
                f"La catégorie {categorie_id} (Incident voyageur) requiert un reservation_id (D041).")
        else:
            if not reservation_id_exists(reservation_id, resolues_path):
                err("V13_RESERVATION_INCONNUE",
                    f"Réservation introuvable dans MASTER_CALC_Reservations_Resolues : "
                    f"{reservation_id!r}.")

    # V14 — statut_controle injecté automatiquement (A_CONTROLER) ; contrôle défensif famille
    # statut_controle (jamais famille import). Le statut n'est jamais saisi par l'utilisateur.
    valid_statuts_controle = {str(s.get("statut", "")).strip() for s in refs["statuts_controle"]}
    if AUTO_STATUT_CONTROLE not in valid_statuts_controle:
        err("V14_STATUT_CONTROLE_REF_ABSENT",
            f"Statut auto {AUTO_STATUT_CONTROLE} absent de la famille statut_controle du référentiel.")

    # V15 — sens_flux valeurs canoniques SAISIE Excel si fourni (défaut vide → DEPENSE)
    sens_flux = str(form_data.get("sens_flux", "")).strip().upper()
    if sens_flux and sens_flux not in CANONICAL_SENS_FLUX:
        err("V15_SENS_FLUX_INVALIDE",
            f"Sens flux invalide : {sens_flux!r} "
            f"(attendu : DEPENSE / RECUPERATION / REMBOURSEMENT / REFACTURATION / NEUTRE).")

    return errors


def _build_row_data(form_data: dict[str, str], charge_id: str) -> dict[str, Any]:
    """Construit le dictionnaire de données à injecter dans la copie SAISIE."""
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

    # sens_flux : valeur canonique SAISIE, défaut DEPENSE (jamais CHARGE/PRODUIT)
    sens_flux = str(form_data.get("sens_flux", "")).strip().upper() or DEFAULT_SENS_FLUX
    # affectation_type : valeur canonique REF_LOCALE (jamais AFF_*)
    affectation_type = str(form_data.get("affectation_type", "")).strip().upper() or None
    # code_impact standard (IC/HC) → prise_en_compta dérivée (IC=OUI, HC=NON)
    code_impact = str(form_data.get("code_impact", "")).strip().upper() or None
    prise_en_compta = PRISE_EN_COMPTA_BY_IMPACT.get(code_impact) if code_impact else None

    return {
        "charge_id": charge_id,
        "date_charge": date_charge_val.isoformat() if date_charge_val else date_charge_raw,
        "montant": montant_val,
        "sens_flux": sens_flux,
        "categorie_charge_id": str(form_data.get("categorie_charge_id", "")).strip() or None,
        "type_flux_id": str(form_data.get("type_flux_id", "")).strip() or None,
        "code_impact": code_impact,
        # Dérivée du code_impact (D-APP-2B-REV1, D012) — jamais saisie directement
        "prise_en_compta": prise_en_compta,
        "associe_id": str(form_data.get("associe_id", "")).strip() or None,
        "mode_paiement_id": str(form_data.get("mode_paiement_id", "")).strip() or None,
        "carte_id": str(form_data.get("carte_id", "")).strip() or None,
        "affectation_type": affectation_type,
        "logement_id": str(form_data.get("logement_id", "")).strip() or None,
        "proprietaire_id": str(form_data.get("proprietaire_id", "")).strip() or None,
        "reservation_id": str(form_data.get("reservation_id", "")).strip() or None,
        "refacturable": str(form_data.get("refacturable", "")).strip() or None,
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
        "affectable_menage": str(form_data.get("affectable_menage", "")).strip() or None,
        "intervenant_concerne": str(form_data.get("intervenant_concerne", "")).strip() or None,
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

    row_data = _build_row_data(form_data, charge_id)
    _inject_row(copy_path, target_row, row_data)

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
        "target_row": target_row,
        "source_hash_avant": source_hash_avant,
        "source_hash_apres": source_hash_apres,
        "source_inchangee": source_inchangee,
        "copy_hash": copy_hash,
        "paths": {
            "run_dir": str(run_dir),
            "saisie_copy": str(copy_path),
        },
    }
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

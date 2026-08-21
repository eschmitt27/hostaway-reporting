"""Service de validation saisie HH contrôlée — APP-2b.

Règles D1–D11. Aucun calcul métier, aucune écriture.
Utilise Decimal pour les montants (pas float) — rejet explicite >2 décimales.
Toute erreur de lecture REF_Setup bloque la validation (pas de pass silencieux).
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg
from app.readers.saisie_hh_reader import (
    MANUAL_COL_MAP,
    FORCED_VALUES,
    SaisieHHReadError,
    read_ref_locale,
    read_existing_pks,
    generate_pk,
)
from app.readers.ref_setup_hh_reader import (
    get_all_logements_hh as get_all_logements,
    get_gestion_hist,
    get_all_proprietaires_hh as get_all_proprietaires,
    get_all_associes,
    get_canaux,
    get_modes_paiement,
    get_codes_impact,
    get_taux_commission,
    get_couts_standards_menage,
    get_cloture_mois,
)
from app.services.saisie_hh_schema_migration import NEW_SAISIE_FIELDS

_MODE_DIRECT_PROPRIETAIRE_ID = "PAY_006"
_MODE_DIRECT_PROPRIETAIRE_CODE = "DIRECT_PROPRIETAIRE"
_MODE_ASSOCIE_CODES = {"COMPTE_PERSO_ASSOCIEE", "CARTE_ASSOCIEE"}
_MODE_ESPECES_CODES = {"ESPECES_CAISSE", "ESPECES"}

_LABEL_FIELDS = (
    "label", "libelle", "libellé", "nom_affichage", "display_name",
    "nom", "name", "designation", "description",
)
_PROPRIETAIRE_LABEL_FIELDS = (
    "prenom", "prénom", "nom_proprietaire", "nom_propriétaire",
    "proprietaire", "propriétaire", "nom", "libelle", "label",
)
_LOGEMENT_LABEL_FIELDS = (
    "nom_logement", "logement_nom", "logement", "nom", "libelle",
    "label", "adresse", "ville",
)
_REF_ID_FIELDS = (
    "id", "code", "valeur", "value", "identifiant",
)
_CANAL_ID_FIELDS = ("canal_id", "id_canal", "code_canal") + _REF_ID_FIELDS
_SOURCE_ID_FIELDS = (
    "source_financiere", "source_financiere_id", "id_source_financiere",
    "code_source_financiere", "code_source",
) + _REF_ID_FIELDS
_MODE_PAIEMENT_ID_FIELDS = (
    "mode_paiement_id", "id_mode_paiement", "code_mode_paiement",
    "paiement_id", "code_paiement",
) + _REF_ID_FIELDS
_CODE_IMPACT_ID_FIELDS = ("code_impact", "impact_id", "code") + _REF_ID_FIELDS
_COMPTABILISATION_ID_FIELDS = (
    "comptabilisation", "comptabilisation_id", "code_comptabilisation",
) + _REF_ID_FIELDS
_TECHNICAL_LOGEMENTS = {"APPARTEMENT_DIVERS", "LOGEMENT_DIVERS"}

# Vocabulaires fixes (pas des référentiels administrables — mêmes valeurs que l'ancien onglet
# REF_LOCALE de SAISIE_ReservationsHorsHostaway.xlsx, lues une seule fois avant migration).
LST_SOURCE_FINANCIERE = ["SAISIE_MANUELLE", "HOSTAWAY_REFERENCE", "VRBO_UNKNOWN",
                        "DIRECT_HA_PAYANT", "A_CONTROLER"]
LST_TAUX_COMMISSION_SOURCE = ["REF_PROPRIETAIRE", "REF_LOGEMENT", "SAISIE_MANUELLE", "A_CONTROLER"]
LST_COMPTABILISATION = ["OUI", "NON"]
LST_STATUTS_CONTROLE = ["VALIDE", "A_CONTROLER", "EXCLU_RESULTAT", "A_VENTILER"]
LST_NIVEAU_ANOMALIE = ["INFO", "A_CONTROLER", "BLOQUANT"]
_MONTH_LABELS = {
    "01": "janvier",
    "02": "février",
    "03": "mars",
    "04": "avril",
    "05": "mai",
    "06": "juin",
    "07": "juillet",
    "08": "août",
    "09": "septembre",
    "10": "octobre",
    "11": "novembre",
    "12": "décembre",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _row_value(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    lower = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        value = lower.get(key.lower())
        if _text(value):
            return _text(value)
    return ""


def _label_from_row(row: dict[str, Any], value: str, keys: tuple[str, ...]) -> str:
    label = _row_value(row, keys)
    return label if label and label != value else value


def _option(value: str, label: str | None = None, **extra: Any) -> dict[str, Any]:
    option = {"value": value, "label": label or value}
    option.update(extra)
    return option


def _month_label(mois: str) -> str:
    raw = _text(mois)
    if len(raw) >= 7 and raw[4] == "-":
        year = raw[:4]
        month = raw[5:7]
        label = _MONTH_LABELS.get(month)
        if label:
            return f"{label} {year}"
    return raw


def _cloture_ui_state(ref_setup_path: Path | None = None, *, db_path=None) -> dict[str, Any]:
    """Expose les mois ouverts/indisponibles pour l'ergonomie du formulaire.

    `ref_setup_path` conservé pour compatibilité d'appel ; ignoré — la source est la table SQLite
    `ref_cloture_mensuelle` (migration 0029), déjà lue en SQLite par `get_cloture_mois` ailleurs dans
    ce module. Cette fonction en était la seule ouverture directe de classeur restante.
    """
    from app.services import referentiel_admin_service as ref_admin

    rows: list[dict[str, Any]] = []
    try:
        cloture_rows = ref_admin.lignes("ref_cloture_mensuelle", db_path=db_path)
    except Exception:
        cloture_rows = []
    for rec in cloture_rows:
        mois = _text(rec.get("mois"))
        if not mois:
            continue
        statut = _text(rec.get("statut_mois")).upper()
        rows.append({"mois": mois, "label": _month_label(mois), "statut_mois": statut})
    rows.sort(key=lambda r: r["mois"])
    open_rows = [row for row in rows if row["statut_mois"] == "OUVERT"]
    return {
        "mois_ouverts": [row["mois"] for row in open_rows],
        "mois_ouverts_labels": [row["label"] for row in open_rows],
        "cloture_mois_status": {
            row["mois"]: {"statut_mois": row["statut_mois"], "label": row["label"]}
            for row in rows
        },
    }


def _label_map(rows: list[dict[str, Any]], id_key: str, label_keys: tuple[str, ...]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        value = _text(row.get(id_key))
        if value:
            result[value] = _label_from_row(row, value, label_keys)
    return result


def _options_from_values(
    values: list[str],
    labels: dict[str, str],
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for value in values:
        label = labels.get(value)
        options.append(_option(value, label or value))
    return options


def _label_map_by_keys(
    rows: list[dict[str, Any]],
    id_keys: tuple[str, ...],
    label_keys: tuple[str, ...] = _LABEL_FIELDS,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        value = _row_value(row, id_keys)
        if value:
            result[value] = _label_from_row(row, value, label_keys)
    return result


def _logement_labels(rows: list[dict[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for row in rows:
        logement_id = _text(row.get("logement_id"))
        label = _text(row.get("nom_court"))
        if logement_id and label:
            labels[logement_id] = label
    return labels


def _proprietaire_labels(rows: list[dict[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for row in rows:
        proprietaire_id = _text(row.get("proprietaire_id"))
        prenom = _text(row.get("prenom_proprietaire"))
        nom = _text(row.get("nom_proprietaire"))
        label = " ".join(part for part in (prenom, nom) if part)
        if proprietaire_id and label:
            labels[proprietaire_id] = label
    return labels


def _taux_commission_history_for_ui(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    history: list[dict[str, str]] = []
    for row in rows:
        if _text(row.get("actif")).upper() != "OUI":
            continue
        taux = _text(row.get("taux_commission"))
        if not taux:
            continue
        history.append({
            "proprietaire_id": _text(row.get("proprietaire_id")),
            "logement_id": _text(row.get("logement_id")),
            "taux_commission": taux,
            "date_debut": _date_text_for_ui(row.get("date_debut")),
            "date_fin": _date_text_for_ui(row.get("date_fin")),
        })
    return history


def _menage_standard_history_for_ui(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    history: list[dict[str, str]] = []
    for row in rows:
        if _text(row.get("actif")).upper() not in ("", "OUI"):
            continue
        cout = _text(row.get("cout_standard_menage"))
        type_logement_id = _text(row.get("type_logement_id"))
        if not cout or not type_logement_id:
            continue
        history.append({
            "type_logement_id": type_logement_id,
            "cout_standard_menage": cout,
            "date_debut": _date_text_for_ui(row.get("date_debut_validite")),
            "date_fin": _date_text_for_ui(row.get("date_fin_validite")),
        })
    return history


def _logement_type_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {
        _text(row.get("logement_id")): _text(row.get("type_logement_id"))
        for row in rows
        if _text(row.get("logement_id")) and _text(row.get("type_logement_id"))
    }


def _mode_code_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    result = {
        _text(row.get("mode_paiement_id")): _text(row.get("mode_paiement"))
        for row in rows
        if _text(row.get("mode_paiement_id")) and _text(row.get("mode_paiement"))
    }
    result[_MODE_DIRECT_PROPRIETAIRE_ID] = _MODE_DIRECT_PROPRIETAIRE_CODE
    return result


def _impact_comptabilisation_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        code = _text(row.get("code_impact"))
        compta = _text(row.get("impact_resultat_comptable")).upper()
        if code and compta in ("OUI", "NON"):
            result[code] = compta
    return result


def resolve_taux_commission(
    taux_rows: list[dict[str, Any]],
    logement_id: str,
    proprietaire_id: str,
    date_arrivee_str: str,
) -> dict[str, str] | None:
    target = _parse_date(date_arrivee_str)
    if target is None:
        return None

    active_rows: list[dict[str, Any]] = []
    for row in taux_rows:
        if _text(row.get("proprietaire_id")) != proprietaire_id:
            continue
        if _text(row.get("actif")).upper() not in ("", "OUI"):
            continue
        debut = _parse_date(str(row.get("date_debut", "")).strip())
        if debut is None or debut > target:
            continue
        fin_str = str(row.get("date_fin", "") or "").strip()
        if fin_str and fin_str.upper() not in ("NONE", "NULL"):
            fin = _parse_date(fin_str)
            if fin is None or fin < target:
                continue
        active_rows.append(row)

    logement_row = next(
        (row for row in active_rows if _text(row.get("logement_id")) == logement_id),
        None,
    )
    proprietaire_row = next(
        (row for row in active_rows if not _text(row.get("logement_id"))),
        None,
    )
    selected = logement_row or proprietaire_row
    if selected is None:
        return None
    return {
        "taux_commission": _text(selected.get("taux_commission")),
        "source": "taux_logement" if logement_row else "taux_proprietaire",
    }


def resolve_prix_menage_standard(
    cout_rows: list[dict[str, Any]],
    logement_rows: list[dict[str, Any]],
    logement_id: str,
    date_arrivee_str: str,
) -> dict[str, str] | None:
    target = _parse_date(date_arrivee_str)
    if target is None:
        return None
    logement = next(
        (row for row in logement_rows if _text(row.get("logement_id")) == logement_id),
        None,
    )
    if logement is None:
        return None
    type_logement_id = _text(logement.get("type_logement_id"))
    if not type_logement_id:
        return None

    for row in cout_rows:
        if _text(row.get("type_logement_id")) != type_logement_id:
            continue
        if _text(row.get("actif")).upper() not in ("", "OUI"):
            continue
        debut = _parse_date(str(row.get("date_debut_validite", "")).strip())
        if debut is None or debut > target:
            continue
        fin_str = str(row.get("date_fin_validite", "") or "").strip()
        if fin_str and fin_str.upper() not in ("NONE", "NULL"):
            fin = _parse_date(fin_str)
            if fin is None or fin < target:
                continue
        cout = _text(row.get("cout_standard_menage"))
        if cout:
            return {
                "menage": cout,
                "source": "REF_Couts_Standards_Menage",
                "type_logement_id": type_logement_id,
            }
    return None


def _is_managed_active_logement(row: dict[str, Any]) -> bool:
    logement_id = _text(row.get("logement_id")).upper()
    if logement_id in _TECHNICAL_LOGEMENTS:
        return False
    if _text(row.get("statut_parc")).upper() != "GERE":
        return False
    if _text(row.get("actif")).upper() != "OUI":
        return False
    return True


def _date_in_gestion(row: dict[str, Any], target: date) -> bool:
    if _text(row.get("statut_gestion")).upper() != "ACTIF":
        return False
    debut = _parse_date(str(row.get("date_debut", "")).strip())
    if debut is None or debut > target:
        return False
    fin_str = str(row.get("date_fin", "") or "").strip()
    if fin_str == "" or fin_str.upper() in ("NONE", "NULL"):
        return True
    fin = _parse_date(fin_str)
    return fin is not None and fin >= target


def _owners_for_logement_at_date(
    gestion_rows: list[dict[str, Any]],
    logement_id: str,
    target: date,
) -> list[str]:
    owners: set[str] = set()
    for row in gestion_rows:
        if _text(row.get("logement_id")) != logement_id:
            continue
        if not _date_in_gestion(row, target):
            continue
        proprietaire_id = _text(row.get("proprietaire_id"))
        if proprietaire_id:
            owners.add(proprietaire_id)
    return sorted(owners)


def _date_text_for_ui(value: Any) -> str:
    parsed = _parse_date(str(value or "").strip())
    return parsed.isoformat() if parsed else ""


def _gestion_history_for_ui(
    gestion_rows: list[dict[str, Any]],
    proprietaire_labels: dict[str, str],
) -> dict[str, list[dict[str, str]]]:
    history: dict[str, list[dict[str, str]]] = {}
    for row in gestion_rows:
        if _text(row.get("statut_gestion")).upper() != "ACTIF":
            continue
        logement_id = _text(row.get("logement_id"))
        proprietaire_id = _text(row.get("proprietaire_id"))
        if not logement_id or not proprietaire_id:
            continue
        history.setdefault(logement_id, []).append({
            "owner_id": proprietaire_id,
            "owner_label": proprietaire_labels.get(proprietaire_id, proprietaire_id),
            "date_debut": _date_text_for_ui(row.get("date_debut")),
            "date_fin": _date_text_for_ui(row.get("date_fin")),
        })
    return history


def load_form_refs(
    saisie_path: Path | None = None,
    ref_setup_path: Path | None = None,
    *,
    db_path=None,
) -> dict[str, Any]:
    # `saisie_path`/`ref_setup_path` conservés pour compatibilité d'appel ; ignorés — les listes
    # lst_* sont désormais construites depuis les MÊMES lignes SQLite typées que le reste de cette
    # fonction (pas une seconde lecture de REF_LOCALE) ; `read_ref_locale` reste importé (inerte)
    # pour ne pas casser les mocks existants qui le ciblent encore.

    logements_rows: list[dict[str, Any]] = []
    proprietaires_rows: list[dict[str, Any]] = []
    associes_rows: list[dict[str, Any]] = []
    gestion_rows: list[dict[str, Any]] = []
    canaux_sheet = modes_sheet = impacts_sheet = None
    canaux_rows: list[dict[str, Any]] = []
    modes_rows: list[dict[str, Any]] = []
    impacts_rows: list[dict[str, Any]] = []
    taux_commission_rows: list[dict[str, Any]] = []
    couts_menage_rows: list[dict[str, Any]] = []
    try:
        logements_rows = get_all_logements(db_path=db_path)
        proprietaires_rows = get_all_proprietaires(db_path=db_path)
        associes_rows = get_all_associes(db_path=db_path)
        gestion_rows = get_gestion_hist(db_path=db_path)
    except Exception:
        # Les libelles enrichis sont une aide UI ; les controles backend restent fail-closed.
        pass

    logement_labels = _logement_labels(logements_rows)
    proprietaire_labels = _proprietaire_labels(proprietaires_rows)
    associe_labels = _label_map(associes_rows, "associe_id", _LABEL_FIELDS)
    try:
        canaux_sheet, canaux_rows = get_canaux(db_path=db_path)
    except Exception:
        pass
    try:
        modes_sheet, modes_rows = get_modes_paiement(db_path=db_path)
    except Exception:
        pass
    try:
        impacts_sheet, impacts_rows = get_codes_impact(db_path=db_path)
    except Exception:
        pass
    try:
        taux_commission_rows = get_taux_commission(db_path=db_path)
    except Exception:
        pass
    try:
        couts_menage_rows = get_couts_standards_menage(db_path=db_path)
    except Exception:
        pass
    cloture_ui = _cloture_ui_state(db_path=db_path)

    refs: dict[str, list[str]] = {
        "lst_Logements": [str(r.get("logement_id", "")).strip() for r in logements_rows
                         if str(r.get("logement_id", "")).strip()],
        "lst_Proprietaires": [str(r.get("proprietaire_id", "")).strip() for r in proprietaires_rows
                             if str(r.get("proprietaire_id", "")).strip()],
        "lst_Canaux": [str(r.get("canal_id", "")).strip() for r in canaux_rows
                      if str(r.get("canal_id", "")).strip()],
        "lst_Associes": [str(r.get("associe_id", "")).strip() for r in associes_rows
                        if str(r.get("associe_id", "")).strip()],
        "lst_ModesPaiement": [str(r.get("mode_paiement_id", "")).strip() for r in modes_rows
                             if str(r.get("mode_paiement_id", "")).strip()],
        "lst_Codes_Impact": [str(r.get("code_impact", "")).strip() for r in impacts_rows
                            if str(r.get("code_impact", "")).strip()],
        "lst_Source_Financiere": list(LST_SOURCE_FINANCIERE),
        "lst_TauxCommissionSource": list(LST_TAUX_COMMISSION_SOURCE),
        "lst_Comptabilisation": list(LST_COMPTABILISATION),
        "lst_Statuts_Controle": list(LST_STATUTS_CONTROLE),
        "lst_Niveau_Anomalie": list(LST_NIVEAU_ANOMALIE),
    }

    canal_labels = _label_map(canaux_rows, "canal_id", ("canal",))
    mode_labels = _label_map(modes_rows, "mode_paiement_id", ("mode_paiement",))
    mode_labels.setdefault(_MODE_DIRECT_PROPRIETAIRE_ID, "Direct proprietaire")
    impact_labels = _label_map(impacts_rows, "code_impact", ("libelle",))
    eligible_logements = {
        _text(row.get("logement_id"))
        for row in logements_rows
        if _text(row.get("logement_id")) and _is_managed_active_logement(row)
    }

    logement_options: list[dict[str, Any]] = []
    for logement_id in refs.get("lst_Logements", []):
        if logement_id.upper() in _TECHNICAL_LOGEMENTS:
            continue
        if eligible_logements and logement_id not in eligible_logements:
            continue
        logement_options.append(_option(
            logement_id,
            logement_labels.get(logement_id, logement_id),
        ))

    logement_owner_history = _gestion_history_for_ui(gestion_rows, proprietaire_labels)
    mode_options = _options_from_values(refs.get("lst_ModesPaiement", []), mode_labels)
    if all(opt["value"] != _MODE_DIRECT_PROPRIETAIRE_ID for opt in mode_options):
        mode_options.append(_option(_MODE_DIRECT_PROPRIETAIRE_ID, "Direct proprietaire"))

    refs.update({
        "options_logements": logement_options,
        "options_proprietaires": _options_from_values(
            refs.get("lst_Proprietaires", []), proprietaire_labels
        ),
        "options_canaux": _options_from_values(
            refs.get("lst_Canaux", []), canal_labels
        ),
        "options_sources_financieres": _options_from_values(
            refs.get("lst_Source_Financiere", []), {}
        ),
        "options_associes": _options_from_values(refs.get("lst_Associes", []), associe_labels),
        "options_modes_paiement": mode_options,
        "options_codes_impact": _options_from_values(
            refs.get("lst_Codes_Impact", []), impact_labels
        ),
        "options_comptabilisation": _options_from_values(
            refs.get("lst_Comptabilisation", []), {}
        ),
        "proprietaire_labels": proprietaire_labels,
        "logement_owner_history": logement_owner_history,
        "logement_type_map": _logement_type_map(logements_rows),
        "mode_code_map": _mode_code_map(modes_rows),
        "impact_comptabilisation_map": _impact_comptabilisation_map(impacts_rows),
        "taux_commission_history": _taux_commission_history_for_ui(taux_commission_rows),
        "menage_standard_history": _menage_standard_history_for_ui(couts_menage_rows),
        "mois_ouverts": cloture_ui["mois_ouverts"],
        "mois_ouverts_labels": cloture_ui["mois_ouverts_labels"],
        "cloture_mois_status": cloture_ui["cloture_mois_status"],
        "label_sources": {
            "canaux": canaux_sheet,
            "sources_financieres": None,
            "modes_paiement": modes_sheet,
            "codes_impact": impacts_sheet,
            "comptabilisation": None,
            "taux_commission": "REF_Taux_Commission" if taux_commission_rows else None,
            "menage_standard": "REF_Couts_Standards_Menage" if couts_menage_rows else None,
            "cloture_mensuelle": "REF_Cloture_Mensuelle",
        },
    })
    return refs


# ── Parseurs ───────────────────────────────────────────────────────────────────

def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    s = str(value).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    raw = (
        str(value).strip()
        .replace(",", ".")
        .replace(" ", "")
        .replace(" ", "")
        .replace(" ", "")
    )
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _more_than_2_decimals(d: Decimal) -> bool:
    try:
        n = d.normalize()
        sign, digits, exp = n.as_tuple()
        return exp < -2
    except Exception:
        return False


def _mois_from_date(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


# ── Validation ─────────────────────────────────────────────────────────────────

def valider(
    form_data: dict[str, str],
    saisie_path: Path | None = None,
    ref_setup_path: Path | None = None,
    *,
    db_path=None,
) -> dict[str, Any]:
    """Valide D1–D11. Retourne {ok, erreurs, preview, pk}."""
    # `saisie_path`/`ref_setup_path` conservés pour compatibilité d'appel ; ignorés — SQLite (`db_path`).

    erreurs: list[dict[str, str]] = []

    def err(champ: str, code: str, message: str) -> None:
        erreurs.append({"champ": champ, "code": code, "message": message})

    # ── Champs obligatoires basiques ──────────────────────────────────────
    canal_id = str(form_data.get("canal_id", "")).strip()
    source_financiere = str(form_data.get("source_financiere", "")).strip()
    proprietaire_id_envoye = str(form_data.get("proprietaire_id", "")).strip()
    proprietaire_id = proprietaire_id_envoye
    logement_id = str(form_data.get("logement_id", "")).strip()
    reservation_id_ha_raw = str(form_data.get("reservation_id_hostaway", "")).strip()
    date_arrivee_str = str(form_data.get("date_arrivee", "")).strip()
    date_depart_str = str(form_data.get("date_depart", "")).strip()
    total_percu_raw = str(form_data.get("total_percu", "")).strip()
    menage_raw = str(form_data.get("menage", "")).strip()
    taux_override_pct_raw = str(form_data.get("taux_commission_override_pct", "")).strip()
    taux_override_motif = str(form_data.get("motif_override_taux_commission", "")).strip()
    taux_override_conf = str(form_data.get("confirmation_override_taux_commission", "")).strip()
    menage_override_raw = str(form_data.get("menage_override", "")).strip()
    menage_override_motif = str(form_data.get("motif_override_menage", "")).strip()
    menage_override_conf = str(form_data.get("confirmation_override_menage", "")).strip()
    montant_recupere_raw = str(form_data.get("montant_recupere", "")).strip()
    associe_id_recuperateur = str(form_data.get("associe_id_recuperateur", "")).strip()
    code_impact = str(form_data.get("code_impact", "")).strip()
    comptabilisation_envoyee = str(form_data.get("comptabilisation", "")).strip()
    comptabilisation = ""
    mode_paiement_id = str(form_data.get("mode_paiement_id", "")).strip()
    commentaire_taux = str(form_data.get("commentaire_taux_commission", "")).strip()
    montant_reverse_raw = str(form_data.get("montant_reverse_proprietaire", "")).strip()
    code_anomalie = str(form_data.get("code_anomalie", "")).strip()
    commentaire = str(form_data.get("commentaire", "")).strip()

    if not canal_id:
        err("canal_id", "CHAMP_OBLIGATOIRE", "Canal requis")
    if not source_financiere:
        err("source_financiere", "CHAMP_OBLIGATOIRE", "Source financière requise")
    if not logement_id:
        err("logement_id", "CHAMP_OBLIGATOIRE", "Logement requis")
    if not code_impact:
        err("code_impact", "CHAMP_OBLIGATOIRE", "Code impact requis")
    if reservation_id_ha_raw:
        err("reservation_id_hostaway", "RESERVATION_ID_HOSTAWAY_INTERDIT",
            "APP-2b hors Hostaway ne doit pas recevoir reservation_id_hostaway")

    # ── D1 — reservation_id_hostaway ──────────────────────────────────────
    reservation_id_ha = None

    # ── D2/D3 — total_percu obligatoire, Decimal, ≤2 décimales ──────────
    total_percu = _parse_decimal(total_percu_raw)
    if total_percu is None:
        err("total_percu", "TOTAL_PERCU_OBLIGATOIRE", "total_percu requis (D2/D3)")
    elif _more_than_2_decimals(total_percu):
        err("total_percu", "MONTANT_TROP_DE_DECIMALES",
            "total_percu : maximum 2 décimales autorisées (pas d'arrondi silencieux)")

    # ── Montants optionnels Decimal ≤2 décimales ──────────────────────────
    menage = _parse_decimal(menage_raw)
    if menage is not None and _more_than_2_decimals(menage):
        err("menage", "MONTANT_TROP_DE_DECIMALES", "menage : maximum 2 décimales")

    montant_recupere = _parse_decimal(montant_recupere_raw)
    if montant_recupere is not None and _more_than_2_decimals(montant_recupere):
        err("montant_recupere", "MONTANT_TROP_DE_DECIMALES",
            "montant_recupere : maximum 2 décimales")

    montant_reverse = _parse_decimal(montant_reverse_raw)
    if montant_reverse is not None and _more_than_2_decimals(montant_reverse):
        err("montant_reverse_proprietaire", "MONTANT_TROP_DE_DECIMALES",
            "montant_reverse_proprietaire : maximum 2 décimales")

    # ── Dates ─────────────────────────────────────────────────────────────
    date_arrivee = _parse_date(date_arrivee_str)
    date_depart = _parse_date(date_depart_str)

    if date_arrivee is None:
        err("date_arrivee", "DATE_ARRIVEE_INVALIDE",
            "Date d'arrivée invalide (format AAAA-MM-JJ)")
    if date_depart is None:
        err("date_depart", "DATE_DEPART_INVALIDE",
            "Date de départ invalide (format AAAA-MM-JJ)")
    if date_arrivee and date_depart and date_depart <= date_arrivee:
        err("date_depart", "DATE_DEPART_AVANT_ARRIVEE",
            "La date de départ doit être postérieure à la date d'arrivée")

    # ── D10 — clôture mois — fail-closed sur REF_Setup inaccessible ───────
    # Le proprietaire est derive cote backend depuis logement + date d'arrivee.
    if logement_id and date_arrivee:
        try:
            gestion = get_gestion_hist(db_path=db_path)
            owners = _owners_for_logement_at_date(gestion, logement_id, date_arrivee)
            if len(owners) == 0:
                proprietaire_id = ""
                err("proprietaire_id", "PROPRIETAIRE_LOGEMENT_ABSENT_A_DATE",
                    f"Aucun proprietaire actif trouve pour {logement_id} a la date {date_arrivee_str}")
                err("proprietaire_id", "PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE",
                    f"Aucune gestion active pour {logement_id} a la date {date_arrivee_str} (D7/D8)")
            elif len(owners) > 1:
                proprietaire_id = ""
                err("proprietaire_id", "PROPRIETAIRE_LOGEMENT_MULTIPLE_A_DATE",
                    f"Plusieurs proprietaires actifs trouves pour {logement_id} a la date {date_arrivee_str}")
            else:
                proprietaire_id = owners[0]
                if proprietaire_id_envoye and proprietaire_id_envoye != proprietaire_id:
                    err("proprietaire_id", "PROPRIETAIRE_HIDDEN_INCOHERENT",
                        "Le proprietaire envoye ne correspond pas au proprietaire calcule")
        except Exception as exc:
            proprietaire_id = ""
            err("proprietaire_id", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour derivation proprietaire (D7/D8) : {exc}")
    elif not proprietaire_id_envoye:
        err("proprietaire_id", "CHAMP_OBLIGATOIRE",
            "Proprietaire requis apres selection du logement et de la date d'arrivee")

    mois: str | None = None
    if date_arrivee:
        mois = _mois_from_date(date_arrivee)
        try:
            cloture_row = get_cloture_mois(mois, db_path=db_path)
            if cloture_row is None:
                err("date_arrivee", "MOIS_HORS_REFERENTIEL_CLOTURE",
                    "Le mois sélectionné n'est pas ouvert dans le référentiel de clôture. "
                    "Un commentaire ne permet pas de créer une réservation sur un mois non ouvert.")
            elif str(cloture_row.get("statut_mois", "")).strip().upper() == "CLOTURE":
                err("date_arrivee", "MOIS_CLOTURE",
                    f"Mois {mois} clôturé (statut = CLOTURE)")
        except Exception as exc:
            err("date_arrivee", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour vérification clôture (D10) : {exc}")

    # ── D8/D7 — éligibilité logement — fail-closed sur REF_Setup ──────────
    if logement_id and date_arrivee:
        try:
            logements = get_all_logements(db_path=db_path)
            log_row = next(
                (r for r in logements
                 if str(r.get("logement_id", "")).strip() == logement_id),
                None,
            )
            if log_row is None:
                err("logement_id", "LOGEMENT_HORS_PARC_TECHNIQUE",
                    f"Logement {logement_id} absent de REF_Logements dans REF_Setup.xlsm")
            else:
                if str(log_row.get("actif", "")).strip().upper() != "OUI":
                    err("logement_id", "LOGEMENT_HORS_PARC_TECHNIQUE",
                        f"Logement {logement_id} inactif (actif ≠ OUI)")
                if str(log_row.get("statut_parc", "")).strip().upper() != "GERE":
                    err("logement_id", "LOGEMENT_HORS_PARC_TECHNIQUE",
                        f"Logement {logement_id} hors parc géré (statut_parc ≠ GERE)")

            gestion = get_gestion_hist(db_path=db_path)
            gestion_active = False
            for g in gestion:
                if (str(g.get("logement_id", "")).strip() != logement_id
                        or str(g.get("proprietaire_id", "")).strip() != proprietaire_id):
                    continue
                if str(g.get("statut_gestion", "")).strip().upper() != "ACTIF":
                    continue
                debut = _parse_date(str(g.get("date_debut", "")).strip())
                if debut is None or debut > date_arrivee:
                    continue
                fin_str = str(g.get("date_fin", "") or "").strip()
                if fin_str == "" or fin_str.upper() in ("NONE", "NULL"):
                    gestion_active = True
                    break
                fin = _parse_date(fin_str)
                if fin is not None and fin >= date_arrivee:  # date_fin inclusive (D7)
                    gestion_active = True
                    break

            if not gestion_active:
                err("proprietaire_id", "PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE",
                    f"Aucune gestion active {proprietaire_id} ↔ {logement_id} "
                    f"à la date {date_arrivee_str} (D7/D8)")

        except Exception as exc:
            err("logement_id", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour vérification logement/gestion (D7/D8) : {exc}")

    # D9 (divergence REF_LOCALE vs REF_Setup) supprimée : les deux étaient déjà, depuis la
    # migration des référentiels, la MÊME table SQLite lue deux fois — un contrôle qui comparait
    # un ensemble à lui-même. Source unique désormais, la divergence est structurellement impossible.

    # ── D11 — associé récupérateur ────────────────────────────────────────
    # Comptabilisation derivee du referentiel des codes impact.
    impact_map: dict[str, str] = {}
    try:
        _, impact_rows = get_codes_impact(db_path=db_path)
        impact_map = _impact_comptabilisation_map(impact_rows)
        if code_impact:
            comptabilisation = impact_map.get(code_impact, "")
            if not comptabilisation:
                err("code_impact", "COMPTABILISATION_IMPACT_ABSENTE",
                    f"Code impact {code_impact} sans impact_resultat_comptable exploitable")
            elif comptabilisation_envoyee and comptabilisation_envoyee != comptabilisation:
                err("comptabilisation", "COMPTABILISATION_POSTEE_INCOHERENTE",
                    "La comptabilisation postee ne correspond pas au code impact")
    except Exception as exc:
        err("code_impact", "REF_SETUP_INDISPONIBLE",
            f"REF_Setup inaccessible pour derivation comptabilisation : {exc}")

    taux_auto: Decimal | None = None
    taux_auto_source = ""
    taux_override: Decimal | None = None
    taux_override_requested = bool(taux_override_pct_raw)
    if logement_id and proprietaire_id and date_arrivee:
        try:
            taux_result = resolve_taux_commission(
                get_taux_commission(db_path=db_path),
                logement_id,
                proprietaire_id,
                date_arrivee_str,
            )
            if taux_result is None:
                err("taux_commission", "TAUX_COMMISSION_ABSENT",
                    "Aucun taux de commission applicable dans REF_Taux_Commission")
            else:
                taux_auto = _parse_decimal(taux_result["taux_commission"])
                taux_auto_source = taux_result["source"]
        except Exception as exc:
            err("taux_commission", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour taux commission : {exc}")
    if taux_override_requested:
        parsed = _parse_decimal(taux_override_pct_raw)
        if parsed is None:
            err("taux_commission_override_pct", "TAUX_OVERRIDE_INVALIDE",
                "Taux derogatoire requis au format pourcentage")
        elif parsed < Decimal("0") or parsed > Decimal("100"):
            err("taux_commission_override_pct", "TAUX_OVERRIDE_HORS_BORNES",
                "Taux derogatoire : valeur attendue entre 0 et 100 pour cent")
        elif _more_than_2_decimals(parsed):
            err("taux_commission_override_pct", "TAUX_OVERRIDE_TROP_DE_DECIMALES",
                "Taux derogatoire : maximum 2 decimales")
        else:
            candidate = parsed / Decimal("100")
            if taux_auto is not None and candidate == taux_auto:
                taux_override = None
                taux_override_motif = ""
                taux_override_conf = ""
            else:
                taux_override = candidate
                if not taux_override_motif:
                    err("motif_override_taux_commission", "MOTIF_OVERRIDE_TAUX_MANQUANT",
                        "Motif obligatoire pour derogation de taux")
                if taux_override_conf.lower() not in ("1", "true", "on", "oui"):
                    err("confirmation_override_taux_commission", "CONFIRMATION_OVERRIDE_TAUX_MANQUANTE",
                        "Confirmation obligatoire pour derogation de taux")
    else:
        taux_override_motif = ""
        taux_override_conf = ""

    menage_standard: Decimal | None = None
    menage_standard_source = ""
    menage_override: Decimal | None = None
    menage_override_requested = bool(menage_override_raw)
    if logement_id and date_arrivee:
        try:
            menage_result = resolve_prix_menage_standard(
                get_couts_standards_menage(db_path=db_path),
                get_all_logements(db_path=db_path),
                logement_id,
                date_arrivee_str,
            )
            if menage_result is None:
                err("menage", "COUT_MENAGE_STANDARD_ABSENT",
                    "Aucun prix menage standard applicable dans REF_Couts_Standards_Menage")
            else:
                menage_standard = _parse_decimal(menage_result["menage"])
                menage_standard_source = menage_result["source"]
        except Exception as exc:
            err("menage", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour cout menage standard : {exc}")
    if menage_override_requested:
        parsed = _parse_decimal(menage_override_raw)
        if parsed is None:
            err("menage_override", "MENAGE_OVERRIDE_INVALIDE",
                "Montant menage derogatoire requis")
        elif _more_than_2_decimals(parsed):
            err("menage_override", "MONTANT_TROP_DE_DECIMALES",
                "menage_override : maximum 2 decimales")
        else:
            if menage_standard is not None and parsed == menage_standard:
                menage_override = None
                menage_override_motif = ""
                menage_override_conf = ""
            else:
                menage_override = parsed
                if not menage_override_motif:
                    err("motif_override_menage", "MOTIF_OVERRIDE_MENAGE_MANQUANT",
                        "Motif obligatoire pour derogation de menage")
                if menage_override_conf.lower() not in ("1", "true", "on", "oui"):
                    err("confirmation_override_menage", "CONFIRMATION_OVERRIDE_MENAGE_MANQUANTE",
                        "Confirmation obligatoire pour derogation de menage")
    else:
        menage_override_motif = ""
        menage_override_conf = ""
    menage = menage_override if menage_override is not None else menage_standard

    try:
        _, mode_rows = get_modes_paiement(db_path=db_path)
        mode_code = _mode_code_map(mode_rows).get(mode_paiement_id, mode_paiement_id)
    except Exception as exc:
        mode_code = ""
        if mode_paiement_id:
            err("mode_paiement_id", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour mode de paiement : {exc}")
    if mode_code not in _MODE_ASSOCIE_CODES:
        montant_recupere = None
        associe_id_recuperateur = ""
    if mode_code not in _MODE_ESPECES_CODES:
        montant_reverse = None

    if mode_code in _MODE_ASSOCIE_CODES:
        if montant_recupere is None:
            err("montant_recupere", "MONTANT_RECUPERE_MANQUANT",
                "Montant recupere par l'associe obligatoire pour ce mode de paiement")
        if not associe_id_recuperateur:
            err("associe_id_recuperateur", "ASSOCIE_MANQUANT",
                "Associe recuperateur obligatoire pour ce mode de paiement")
    if associe_id_recuperateur:
        try:
            associes = {
                str(r.get("associe_id", "") or r.get("personne_id", "") or "").strip()
                for r in get_all_associes(db_path=db_path)
            }
            if associe_id_recuperateur not in associes:
                err("associe_id_recuperateur", "ASSOCIE_INCONNU",
                    f"associe_id_recuperateur {associe_id_recuperateur} absent de REF_Associes")
        except Exception as exc:
            err("associe_id_recuperateur", "REF_SETUP_INDISPONIBLE",
                f"REF_Setup inaccessible pour verification associe recuperateur (D11) : {exc}")

    # ── Génération PK ─────────────────────────────────────────────────────
    pk: str = ""
    if date_arrivee and not any(e["code"] in (
        "DATE_ARRIVEE_INVALIDE", "MOIS_CLOTURE",
        "MOIS_HORS_REFERENTIEL_CLOTURE", "REF_SETUP_INDISPONIBLE",
    ) for e in erreurs):
        try:
            from app.readers import reservations_hh_reader
            existing_pks = [
                str(r.get("reservation_hh_id", "")).strip()
                for r in reservations_hh_reader.read_reservations(db_path=db_path)
                if str(r.get("reservation_hh_id", "")).strip()
            ]
            pk, pk_err = generate_pk(date_arrivee_str, existing_pks)
            if pk_err:
                err("date_arrivee", pk_err, f"Cle primaire impossible : {pk_err}")
                pk = ""
        except Exception as exc:
            err("date_arrivee", "SAISIE_INDISPONIBLE_PK",
                f"Lecture des reservations existantes impossible pour generation PK : {exc}")

    # ── Preview ───────────────────────────────────────────────────────────
    preview: dict[str, Any] = {
        "reservation_hh_id":           pk,
        "canal_id":                     canal_id,
        "source_financiere":            source_financiere,
        "proprietaire_id":              proprietaire_id,
        "logement_id":                  logement_id,
        "reservation_id_hostaway":      reservation_id_ha,
        "date_arrivee":                 date_arrivee_str,
        "date_depart":                  date_depart_str,
        "total_percu":                  total_percu,
        "menage":                       menage,
        "menage_standard":              menage_standard,
        "menage_standard_source":       menage_standard_source or None,
        "menage_override":              menage_override,
        "motif_override_menage":        menage_override_motif or None,
        "confirmation_override_menage": bool(menage_override_conf),
        "taux_commission_standard":     taux_auto,
        "taux_commission_standard_source": taux_auto_source or None,
        "taux_commission_override":     taux_override,
        "motif_override_taux_commission": taux_override_motif or None,
        "confirmation_override_taux_commission": bool(taux_override_conf),
        "commentaire_taux_commission":  commentaire_taux or None,
        "montant_recupere":             montant_recupere,
        "associe_id_recuperateur":      associe_id_recuperateur or None,
        "montant_reverse_proprietaire": montant_reverse,
        "mode_paiement_id":             mode_paiement_id or None,
        "code_impact":                  code_impact,
        "comptabilisation":             comptabilisation,
        "statut_controle":              FORCED_VALUES["statut_controle"],
        "niveau_anomalie":              FORCED_VALUES["niveau_anomalie"],
        "code_anomalie":                code_anomalie or None,
        "commentaire":                  commentaire or None,
        "mois":                         mois,
    }

    return {
        "ok": len(erreurs) == 0,
        "erreurs": erreurs,
        "preview": preview,
        "pk": pk,
    }


def build_row_data(preview: dict[str, Any]) -> dict[str, Any]:
    """Filtre la preview → champs manuels SAISIE + forcés. Decimal → float pour openpyxl."""
    field_names = set(MANUAL_COL_MAP.values()) | set(NEW_SAISIE_FIELDS)
    row: dict[str, Any] = {}
    for k, v in preview.items():
        if k not in field_names:
            continue
        if isinstance(v, Decimal):
            v = float(v)
        elif isinstance(v, bool):
            v = "OUI" if v else ""
        row[k] = v
    row.update(FORCED_VALUES)
    return row

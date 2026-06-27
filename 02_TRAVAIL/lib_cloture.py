"""
Regles pures de cloture mensuelle et corrections post-cloture.

Ce module ne lit ni n'ecrit de fichier. Les scripts pipeline s'en servent pour
eviter les statuts libres et les validations automatiques de corrections.
"""

import hashlib

STATUT_OUVERT = "OUVERT"
STATUT_EN_CONTROLE = "EN_CONTROLE"
STATUT_CLOTURE = "CLOTURE"

ALLOWED_CLOTURE_STATUSES = {STATUT_OUVERT, STATUT_EN_CONTROLE, STATUT_CLOTURE}

REQUIRED_AJUSTEMENT_COLUMNS = {
    "ajustement_id",
    "mois_origine",
    "mois_effet",
    "source_module",
    "source_pk",
    "logement_id",
    "proprietaire_id",
    "type_ajustement",
    "montant",
    "sens",
    "impact_reel",
    "impact_comptable",
    "motif",
    "justificatif",
    "auteur",
    "date_saisie",
    "statut_validation",
}

VALIDATION_ALLOWED = {"A_VALIDER", "REJETE", "VALIDE"}
SENS_ALLOWED = {"PRODUIT", "CHARGE", "AJUSTEMENT"}
IMPACT_ALLOWED = {"OUI", "NON"}


def normalise_cloture_status(value):
    status = str(value or "").strip().upper()
    return status if status in ALLOWED_CLOTURE_STATUSES else None


def validate_cloture_status(value):
    status = normalise_cloture_status(value)
    if status is None:
        return False, "STATUT_CLOTURE_INTERDIT"
    return True, status


def validate_adjustment(row):
    """Valide une correction post-cloture sans jamais la valider metier."""
    missing = [c for c in sorted(REQUIRED_AJUSTEMENT_COLUMNS) if row.get(c) in (None, "")]
    if missing:
        return False, "AJUSTEMENT_SCHEMA_OU_DONNEE_INCOMPLETE", f"Colonnes/champs manquants: {missing}"

    try:
        float(row.get("montant"))
    except (TypeError, ValueError):
        return False, "AJUSTEMENT_MONTANT_INVALIDE", "Montant non numerique"

    statut = str(row.get("statut_validation") or "").strip().upper()
    if statut not in VALIDATION_ALLOWED:
        return False, "AJUSTEMENT_STATUT_VALIDATION_INTERDIT", "Statut validation non autorise"
    if statut == "VALIDE":
        # Une ligne peut etre validee dans la saisie, mais jamais par defaut.
        if str(row.get("justificatif") or "").strip() == "":
            return False, "AJUSTEMENT_VALIDE_SANS_JUSTIFICATIF", "Validation sans justificatif"

    sens = str(row.get("sens") or "").strip().upper()
    if sens not in SENS_ALLOWED:
        return False, "AJUSTEMENT_SENS_INTERDIT", "Sens non autorise"

    for field in ("impact_reel", "impact_comptable"):
        impact = str(row.get(field) or "").strip().upper()
        if impact not in IMPACT_ALLOWED:
            return False, "AJUSTEMENT_IMPACT_INTERDIT", f"{field} non autorise"

    return True, "OK", "Correction tracee, a integrer comme ligne separee"


def adjustment_hash(row):
    fields = [
        "ajustement_id", "mois_origine", "mois_effet", "source_module",
        "source_pk", "logement_id", "proprietaire_id", "type_ajustement",
        "montant", "sens", "impact_reel", "impact_comptable", "motif",
        "justificatif", "auteur", "date_saisie", "statut_validation",
    ]
    payload = "|".join("" if row.get(f) is None else str(row.get(f)) for f in fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

"""
Helpers purs pour qualifier les controles qui bloquent une facture.

Les scripts pandas peuvent s'en servir sans embarquer la logique de ciblage
dans la generation Excel, et les tests unitaires restent sans dependance xlsx.
"""

IMPACT_BLOQUANT_FACTURE = "BLOQUANT_FACTURE"
IMPACT_NON_BLOQUANT_FACTURE = "NON_BLOQUANT_FACTURE"
IMPACT_A_DECIDER = "A_DECIDER"

VALID_IMPACTS_FACTURE = {
    IMPACT_BLOQUANT_FACTURE,
    IMPACT_NON_BLOQUANT_FACTURE,
    IMPACT_A_DECIDER,
}


def default_impact_facture(severity, impact_facture=None):
    """Retourne un impact facture ferme et stable."""
    if impact_facture:
        value = str(impact_facture).strip().upper()
        if value in VALID_IMPACTS_FACTURE:
            return value
        return IMPACT_A_DECIDER

    sev = str(severity or "").strip().upper()
    if sev == "BLOQUANT":
        return IMPACT_BLOQUANT_FACTURE
    if sev == "INFO":
        return IMPACT_NON_BLOQUANT_FACTURE
    return IMPACT_A_DECIDER


def _same_or_empty(control_value, target_value):
    if target_value in (None, ""):
        return True
    if control_value in (None, ""):
        return True
    return str(control_value) == str(target_value)


def control_targets_invoice(control, mois, logement_id=None, proprietaire_id=None, document_id=None):
    """Indique si un controle concerne la facture demandee."""
    impact = default_impact_facture(control.get("severity"), control.get("impact_facture"))
    ctrl_mois = control.get("mois")

    if ctrl_mois in (None, "", "nan"):
        if impact != IMPACT_BLOQUANT_FACTURE:
            return False
    elif str(ctrl_mois) != str(mois):
        return False

    if not _same_or_empty(control.get("logement_id"), logement_id):
        return False
    if not _same_or_empty(control.get("proprietaire_id"), proprietaire_id):
        return False
    if not _same_or_empty(control.get("document_id"), document_id):
        return False
    return True


def facture_control_counts(controls, mois, logement_id=None, proprietaire_id=None, document_id=None):
    subset = [
        c for c in controls
        if control_targets_invoice(c, mois, logement_id, proprietaire_id, document_id)
    ]
    return {
        "nb_bloquants": sum(
            1 for c in subset
            if str(c.get("severity")).upper() == "BLOQUANT"
            and default_impact_facture(c.get("severity"), c.get("impact_facture")) == IMPACT_BLOQUANT_FACTURE
        ),
        "nb_a_controler": sum(
            1 for c in subset
            if str(c.get("severity")).upper() == "A_CONTROLER"
            and default_impact_facture(c.get("severity"), c.get("impact_facture")) != IMPACT_NON_BLOQUANT_FACTURE
        ),
        "nb_info": sum(1 for c in subset if str(c.get("severity")).upper() == "INFO"),
        "controls": subset,
    }

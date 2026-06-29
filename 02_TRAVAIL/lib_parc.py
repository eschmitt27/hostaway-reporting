HORS_PARC_TECHNIQUE = "HORS_PARC_TECHNIQUE"
GERE = "GERE"
A_CONTROLER = "A_CONTROLER"
STATUT_PARC_INVALIDE = "STATUT_PARC_INVALIDE"

VALID_STATUTS_PARC = {GERE, HORS_PARC_TECHNIQUE}


def normalise_statut_parc(value):
    if value is None:
        return ""
    return str(value).strip().upper()


def _get_statut_parc(ref_row):
    if ref_row is None:
        return None
    getter = ref_row.get if hasattr(ref_row, "get") else lambda key, default=None: default
    return getter("statut_parc")


def statut_parc_traitement(ref_row):
    statut = normalise_statut_parc(_get_statut_parc(ref_row))
    if statut in VALID_STATUTS_PARC:
        return statut
    return A_CONTROLER


def code_anomalie_statut_parc(ref_row):
    return STATUT_PARC_INVALIDE if statut_parc_traitement(ref_row) == A_CONTROLER else None


def is_hors_parc_technique(ref_row):
    return statut_parc_traitement(ref_row) == HORS_PARC_TECHNIQUE


def is_gere(ref_row):
    return statut_parc_traitement(ref_row) == GERE


def is_statut_parc_a_controler(ref_row):
    return statut_parc_traitement(ref_row) == A_CONTROLER

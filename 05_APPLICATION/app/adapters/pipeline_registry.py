from pathlib import Path
from app.config import PIPELINE_SCRIPTS_ROOT

# Registre des scripts confirmés — nom logique → fichier .py réel dans PIPELINE_SCRIPTS_ROOT
# Vérifié le 2026-07-01 : tous les chemins pointent vers des fichiers existants à la racine de 02_TRAVAIL/.
# Absent du registre : lot3 (aucun lot3_*.py trouvé dans 02_TRAVAIL).
_REGISTRY: dict[str, str] = {
    "lot1_hostaway_extract":        "lot1_hostaway_extract.py",
    "lot4bis_charger_reservations": "lot4bis_charger_reservations.py",
    "lot4ter_historiser":           "lot4ter_historiser_reservations_cloturees.py",
    "lot4quater_resoudre_source":   "lot4quater_resoudre_source_reservations.py",
    "lot5_acomptes":                "lot5_master_acomptes_proprietaires.py",
    "lot6a_cleaning_tasks":         "lot6a_cleaning_tasks_comptage.py",
    "lot6b_m04_internes":           "lot6b_m04_menages_internes.py",
    "lot6c_menages_externes":       "lot6c_menages_externes.py",
    "lot6d_rapprochement":          "lot6d_rapprochement_menages.py",
    "lot6e_gainperte":              "lot6e_gainperte_menages.py",
    "lot6f_cout_complet":           "lot6f_cout_complet_menages.py",
    "lot7_ik_avantages":            "lot7_ik_avantages.py",
    "lot8a_banque_import":          "lot8a_banque_import.py",
    "lot8b_banque_regles":          "lot8b_banque_regles.py",
    "lot8c_rapprochement_banque":   "lot8c_rapprochement_banque.py",
    "lot9_construire_flux":         "lot9_construire_flux.py",
    "lot10_calculer_resultats":     "lot10_calculer_resultats.py",
    "lot11_controles_coherence":    "lot11_controles_coherence.py",
    "lot12_generer_factures":       "lot12_generer_factures.py",
    "lot13_export_powerbi":         "lot13_export_powerbi.py",
    "run_menages_pipeline":         "run_menages_pipeline.py",
}

# Scripts non référencés : aucun fichier .py confirmé dans 02_TRAVAIL/
NON_REFERENCES = {
    "lot3_charges": "Aucun lot3_*.py trouvé dans 02_TRAVAIL/ — script non référencé au Lot APP-0.",
}


def list_scripts() -> dict[str, dict]:
    result = {}
    for name, filename in _REGISTRY.items():
        abs_path = PIPELINE_SCRIPTS_ROOT / filename
        result[name] = {
            "name": name,
            "path": str(abs_path),
            "exists": abs_path.exists(),
        }
    return result


def get_script_path(name: str) -> Path | None:
    filename = _REGISTRY.get(name)
    if not filename:
        return None
    p = PIPELINE_SCRIPTS_ROOT / filename
    return p if p.exists() else None


def validate_registry() -> list[str]:
    """Retourne la liste des entrées du registre dont le fichier n'existe pas."""
    missing = []
    for name, filename in _REGISTRY.items():
        p = PIPELINE_SCRIPTS_ROOT / filename
        if not p.exists():
            missing.append(f"{name} → {filename}")
    return missing

"""APP-0 — Registre pipeline : chaque entrée active pointe vers un fichier réel."""
from app.adapters.pipeline_registry import _REGISTRY, validate_registry, NON_REFERENCES
from app.config import PIPELINE_SCRIPTS_ROOT


def test_all_registry_entries_point_to_existing_files():
    missing = validate_registry()
    assert missing == [], (
        f"Entrées du registre avec fichier absent :\n" + "\n".join(missing)
    )


def test_registry_uses_py_files_not_folders():
    for name, filename in _REGISTRY.items():
        assert filename.endswith(".py"), (
            f"Entrée '{name}' : '{filename}' n'est pas un fichier .py. "
            "Le registre doit pointer vers des scripts, pas des dossiers."
        )


def test_no_lot_folder_paths_in_registry():
    """Les anciens chemins vers dossiers Lot* ne doivent plus exister."""
    old_folder_patterns = [
        "Lot1_Hostaway", "Lot3_Charges", "Lot4_ReservationsHH",
        "Lot4bis_TableCommune", "Lot6b_DeclarationsInternes",
        "Lot6d_Rapprochement_Menages", "Lot6e_GainPerte_Menages",
        "Lot6f_CoutComplet_Menages", "Lot8_Banque", "Lot9_FluxUnifie",
        "Lot10_Resultats", "Lot11_Controles", "Lot12_Factures",
    ]
    for name, filename in _REGISTRY.items():
        for pattern in old_folder_patterns:
            assert pattern not in filename, (
                f"Entrée '{name}' contient encore un chemin dossier : '{filename}'"
            )


def test_non_references_documented():
    """lot3 est absent du registre actif et documenté dans NON_REFERENCES."""
    assert "lot3_charges" in NON_REFERENCES


def test_pipeline_scripts_root_exists():
    assert PIPELINE_SCRIPTS_ROOT.exists(), (
        f"PIPELINE_SCRIPTS_ROOT introuvable : {PIPELINE_SCRIPTS_ROOT}"
    )

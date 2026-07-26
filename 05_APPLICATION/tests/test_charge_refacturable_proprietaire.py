"""Charge refacturable sur un logement : le propriétaire DOIT être matérialisé dans la ligne SAISIE.

Défaut corrigé (test de non-régression) : `_build_row_data` écrivait `proprietaire_id = None` quand
la charge visait un seul logement, alors que le périmètre avait déjà résolu le propriétaire de
façon historisée (`proprietaire_par_logement`, via `gestion_active_pour_mois`).

Conséquence du défaut : Lot10 n'infère JAMAIS le propriétaire (règle explicite de non-inférence) ;
la charge refacturable ne rejoignait donc ni la préfacture ni le net propriétaire — le résultat du
logement baissait, mais le net du propriétaire restait inchangé.
"""
from app.services.charges_preview_service import _build_row_data

FORM = {
    "date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008",
    "code_impact": "IC", "mode_paiement_id": "PAY_001",
}


def _guide(logements, proprietaire_par_logement, refacturable=True, proprietaires=()):
    return {
        "impact_menage": False,
        "refacturable_effectif": refacturable,
        "perimetre": {
            "logements_directs": list(logements),
            "proprietaires": list(proprietaires),
            "logements_finaux": list(logements),
            "nb_logements_finaux": len(logements),
            "proprietaire_par_logement": dict(proprietaire_par_logement),
            "global_conciergerie": False,
        },
    }


def test_logement_unique_materialise_le_proprietaire():
    """Cœur du correctif : LOG_A1 → la ligne SAISIE porte PROP_A (et non None)."""
    row = _build_row_data(dict(FORM), "CHG-TEST-001",
                          guide=_guide(["LOG_A1"], {"LOG_A1": "PROP_A"}))
    assert row["logement_id"] == "LOG_A1"
    assert row["proprietaire_id"] == "PROP_A", "propriétaire perdu : Lot10 ne refacturera pas"
    assert row["refacturable"] == "OUI"


def test_logement_unique_non_refacturable_materialise_aussi():
    """La matérialisation ne dépend pas de la refacturation (traçabilité analytique)."""
    row = _build_row_data(dict(FORM), "CHG-TEST-002",
                          guide=_guide(["LOG_A1"], {"LOG_A1": "PROP_A"}, refacturable=False))
    assert row["proprietaire_id"] == "PROP_A"
    assert row["refacturable"] == "NON"


def test_proprietaire_inconnu_reste_none_jamais_invente():
    """Aucun propriétaire résolu pour le logement → None (jamais une valeur inventée)."""
    row = _build_row_data(dict(FORM), "CHG-TEST-003",
                          guide=_guide(["LOG_ZZ"], {}))
    assert row["logement_id"] == "LOG_ZZ"
    assert row["proprietaire_id"] is None


def test_multi_logements_meme_proprietaire_materialise_le_proprietaire():
    """2 logements du MÊME propriétaire : affectation GLOBAL mais propriétaire connu → matérialisé."""
    row = _build_row_data(dict(FORM), "CHG-TEST-004",
                          guide=_guide(["LOG_A1", "LOG_A2"],
                                       {"LOG_A1": "PROP_A", "LOG_A2": "PROP_A"}))
    assert row["proprietaire_id"] == "PROP_A"


def test_multi_proprietaires_ne_materialise_aucun_proprietaire():
    """2 propriétaires différents : ambiguïté → aucun propriétaire sur la ligne unique."""
    row = _build_row_data(dict(FORM), "CHG-TEST-005",
                          guide=_guide(["LOG_A1", "LOG_B1"],
                                       {"LOG_A1": "PROP_A", "LOG_B1": "PROP_B"}))
    assert row["proprietaire_id"] is None


def test_charge_menage_ne_porte_jamais_de_proprietaire():
    """Charge ménage : analytique, jamais refacturable, aucun propriétaire sur la ligne."""
    guide = _guide(["LOG_A1"], {"LOG_A1": "PROP_A"})
    guide["impact_menage"] = True
    row = _build_row_data(dict(FORM), "CHG-TEST-006", guide=guide)
    assert row["proprietaire_id"] is None
    assert row["refacturable"] == "NON"

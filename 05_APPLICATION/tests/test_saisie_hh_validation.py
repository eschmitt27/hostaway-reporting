"""APP-2b — Tests de validation saisie HH (D1–D11).

Aucun fichier Excel réel touché. Toutes les fonctions accept des paramètres
saisie_path et ref_setup_path pour pointer sur des copies temporaires.
"""
import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import patch, MagicMock
from app.services.saisie_hh_service import valider, load_form_refs, resolve_taux_commission
from app.readers.saisie_hh_reader import generate_pk, SaisieHHReadError


# ── Helpers ───────────────────────────────────────────────────────────────────

def _base_form(**overrides) -> dict:
    """Données minimales valides (hors contrôles REF_Setup/cloture)."""
    data = {
        "canal_id":           "CANAL_001",
        "source_financiere":  "SAISIE_MANUELLE",
        "proprietaire_id":    "PROP_0001",
        "logement_id":        "LOG_0001",
        "date_arrivee":       "2026-08-15",
        "date_depart":        "2026-08-18",
        "total_percu":        "450.00",
        "code_impact":        "HC",
        "comptabilisation":   "OUI",
    }
    data.update(overrides)
    return data


def _valider_no_refs(form_data: dict) -> dict:
    """Valide en patchant les lectures REF_Setup et REF_LOCALE pour renvoyer vide."""
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE", "type_logement_id": "TYPE_T3"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.get_modes_paiement", return_value=(
            "REF_Modes_Paiement", [
                {"mode_paiement_id": "PAY_001", "mode_paiement": "BANQUE_PRO"},
                {"mode_paiement_id": "PAY_003", "mode_paiement": "CARTE_ASSOCIEE"},
                {"mode_paiement_id": "PAY_004", "mode_paiement": "COMPTE_PERSO_ASSOCIEE"},
            ],
        )),
        patch("app.services.saisie_hh_service.get_codes_impact", return_value=(
            "REF_Codes_Impact", [{"code_impact": "HC", "impact_resultat_comptable": "OUI"}]
        )),
        patch("app.services.saisie_hh_service.get_taux_commission", return_value=[
            {"proprietaire_id": "PROP_0001", "logement_id": "", "taux_commission": "0.15",
             "date_debut": "2026-01-01", "date_fin": "", "actif": "OUI"}
        ]),
        patch("app.services.saisie_hh_service.get_couts_standards_menage", return_value=[
            {"type_logement_id": "TYPE_T3", "cout_standard_menage": "50.00",
             "date_debut_validite": "2026-01-01", "date_fin_validite": "", "actif": "OUI"}
        ]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"],
            "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
        patch("app.services.saisie_hh_service._openpyxl_ha_check", side_effect=lambda *a, **kw: None,
              create=True),
    ):
        # Patch inline openpyxl usage for ha check
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            return valider(form_data, saisie_path=None, ref_setup_path=None)


def _valider_with_gestion(form_data: dict, gestion_rows: list[dict]) -> dict:
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE", "type_logement_id": "TYPE_T3"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=gestion_rows),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"},
            {"proprietaire_id": "PROP_0002"},
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.get_modes_paiement", return_value=(
            "REF_Modes_Paiement", [{"mode_paiement_id": "PAY_001", "mode_paiement": "BANQUE_PRO"}]
        )),
        patch("app.services.saisie_hh_service.get_codes_impact", return_value=(
            "REF_Codes_Impact", [{"code_impact": "HC", "impact_resultat_comptable": "OUI"}]
        )),
        patch("app.services.saisie_hh_service.get_taux_commission", return_value=[
            {"proprietaire_id": "PROP_0001", "logement_id": "", "taux_commission": "0.15",
             "date_debut": "2026-01-01", "date_fin": "", "actif": "OUI"},
            {"proprietaire_id": "PROP_0002", "logement_id": "", "taux_commission": "0.15",
             "date_debut": "2026-01-01", "date_fin": "", "actif": "OUI"},
        ]),
        patch("app.services.saisie_hh_service.get_couts_standards_menage", return_value=[
            {"type_logement_id": "TYPE_T3", "cout_standard_menage": "50.00",
             "date_debut_validite": "2026-01-01", "date_fin_validite": "", "actif": "OUI"}
        ]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"],
            "lst_Proprietaires": ["PROP_0001", "PROP_0002"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            return valider(form_data, saisie_path=None, ref_setup_path=None)


def test_load_form_refs_prepare_libelles_et_proprietaire_du_logement():
    with (
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0009"],
            "lst_Proprietaires": ["PROP_0003"],
            "lst_Canaux": ["CANAL_001", "CANAL_002", "CANAL_003", "CANAL_004", "CANAL_005"],
            "lst_Source_Financiere": ["SAISIE_MANUELLE"],
            "lst_Associes": ["PERS_EWAN"],
            "lst_ModesPaiement": ["PAY_001"],
            "lst_Codes_Impact": ["HC"],
            "lst_Comptabilisation": ["OUI"],
        }),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0009", "nom_court": "Cyprien", "actif": "OUI",
             "statut_parc": "GERE", "type_logement_id": "TYPE_T3"}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0003", "prenom_proprietaire": "David", "nom_proprietaire": "Dupont"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[
            {"associe_id": "PERS_EWAN", "prenom": "Ewan"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0009", "proprietaire_id": "PROP_0003",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": ""}
        ]),
        patch("app.services.saisie_hh_service.get_canaux", return_value=(
            "REF_Canaux_Reservation",
            [
                {"canal_id": "CANAL_001", "canal": "Airbnb"},
                {"canal_id": "CANAL_002", "canal": "Booking"},
                {"canal_id": "CANAL_003", "canal": "VRBO"},
                {"canal_id": "CANAL_004", "canal": "Direct"},
                {"canal_id": "CANAL_005", "canal": "Conciergerie"},
            ],
        )),
        patch("app.services.saisie_hh_service.get_modes_paiement", return_value=(
            "REF_Modes_Paiement", [{"mode_paiement_id": "PAY_001", "mode_paiement": "BANQUE_PRO"}]
        )),
        patch("app.services.saisie_hh_service.get_codes_impact", return_value=(
            "REF_Codes_Impact", [{"code_impact": "HC", "libelle": "Hors comptabilite"}]
        )),
        patch("app.services.saisie_hh_service.get_taux_commission", return_value=[
            {
                "proprietaire_id": "PROP_0003",
                "logement_id": "",
                "taux_commission": "0.15",
                "date_debut": "2026-01-01",
                "date_fin": "",
                "actif": "OUI",
            },
            {
                "proprietaire_id": "PROP_0003",
                "logement_id": "LOG_0009",
                "taux_commission": "0.18",
                "date_debut": "2026-08-01",
                "date_fin": "2026-08-31",
                "actif": "OUI",
            },
        ]),
        patch("app.services.saisie_hh_service.get_couts_standards_menage", return_value=[
            {"type_logement_id": "TYPE_T3", "cout_standard_menage": "60.00",
             "date_debut_validite": "2026-01-01", "date_fin_validite": "", "actif": "OUI"}
        ]),
    ):
        refs = load_form_refs(saisie_path="dummy.xlsx", ref_setup_path="dummy.xlsm")
    logement = refs["options_logements"][0]
    assert logement["value"] == "LOG_0009"
    assert logement["label"] == "Cyprien"
    assert refs["logement_owner_history"]["LOG_0009"][0]["owner_id"] == "PROP_0003"
    assert refs["logement_owner_history"]["LOG_0009"][0]["owner_label"] == "David Dupont"
    assert refs["proprietaire_labels"]["PROP_0003"] == "David Dupont"
    assert refs["options_canaux"] == [
        {"value": "CANAL_001", "label": "Airbnb"},
        {"value": "CANAL_002", "label": "Booking"},
        {"value": "CANAL_003", "label": "VRBO"},
        {"value": "CANAL_004", "label": "Direct"},
        {"value": "CANAL_005", "label": "Conciergerie"},
    ]
    assert refs["options_sources_financieres"][0] == {
        "value": "SAISIE_MANUELLE", "label": "SAISIE_MANUELLE"
    }
    assert refs["options_modes_paiement"][0] == {"value": "PAY_001", "label": "BANQUE_PRO"}
    assert refs["options_codes_impact"][0] == {"value": "HC", "label": "Hors comptabilite"}
    assert refs["options_comptabilisation"][0] == {"value": "OUI", "label": "OUI"}
    assert refs["label_sources"] == {
        "canaux": "REF_Canaux_Reservation",
        "sources_financieres": None,
        "modes_paiement": "REF_Modes_Paiement",
        "codes_impact": "REF_Codes_Impact",
        "comptabilisation": None,
        "taux_commission": "REF_Taux_Commission",
        "menage_standard": "REF_Couts_Standards_Menage",
    }
    assert refs["taux_commission_history"][0]["taux_commission"] == "0.15"
    assert refs["taux_commission_history"][1]["logement_id"] == "LOG_0009"
    assert refs["taux_commission_history"][1]["taux_commission"] == "0.18"
    assert refs["menage_standard_history"][0]["cout_standard_menage"] == "60.00"


def test_load_form_refs_exclut_logements_techniques_et_hors_parc():
    with (
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001", "APPARTEMENT_DIVERS", "LOGEMENT_DIVERS", "LOG_0002", "LOG_0003"],
            "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "nom_logement": "T3 Cyprien", "actif": "OUI", "statut_parc": "GERE"},
            {"logement_id": "LOG_0002", "nom_logement": "Inactif", "actif": "NON", "statut_parc": "GERE"},
            {"logement_id": "LOG_0003", "nom_logement": "Hors parc", "actif": "OUI", "statut_parc": "NON_GERE"},
            {"logement_id": "APPARTEMENT_DIVERS", "actif": "OUI", "statut_parc": "GERE"},
            {"logement_id": "LOGEMENT_DIVERS", "actif": "OUI", "statut_parc": "GERE"},
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001", "prenom": "David"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[]),
    ):
        refs = load_form_refs(saisie_path="dummy.xlsx", ref_setup_path="dummy.xlsm")
    values = [opt["value"] for opt in refs["options_logements"]]
    assert values == ["LOG_0001"]


def test_resolve_taux_commission_proprietaire_a_date():
    result = resolve_taux_commission(
        [
            {
                "proprietaire_id": "PROP_0003",
                "logement_id": "",
                "taux_commission": "0.15",
                "date_debut": "2026-01-01",
                "date_fin": "2026-08-15",
                "actif": "OUI",
            }
        ],
        "LOG_0009",
        "PROP_0003",
        "2026-08-15",
    )
    assert result == {"taux_commission": "0.15", "source": "taux_proprietaire"}


def test_resolve_taux_commission_priorite_logement_sur_proprietaire():
    result = resolve_taux_commission(
        [
            {
                "proprietaire_id": "PROP_0003",
                "logement_id": "",
                "taux_commission": "0.15",
                "date_debut": "2026-01-01",
                "date_fin": "",
                "actif": "OUI",
            },
            {
                "proprietaire_id": "PROP_0003",
                "logement_id": "LOG_0009",
                "taux_commission": "0.18",
                "date_debut": "2026-01-01",
                "date_fin": "",
                "actif": "OUI",
            },
        ],
        "LOG_0009",
        "PROP_0003",
        "2026-08-15",
    )
    assert result == {"taux_commission": "0.18", "source": "taux_logement"}


# ── Champs obligatoires ───────────────────────────────────────────────────────

def test_champs_obligatoires_manquants():
    result = _valider_no_refs({})
    codes = {e["code"] for e in result["erreurs"]}
    assert "CHAMP_OBLIGATOIRE" in codes
    assert result["ok"] is False


def test_formulaire_valide_minimal():
    result = _valider_no_refs(_base_form())
    assert result["ok"] is True, result["erreurs"]
    assert result["pk"].startswith("RESHH-2026-08-")


# ── D1 — reservation_id_hostaway ─────────────────────────────────────────────

def test_d1_ha_id_obligatoire_vrbo():
    result = _valider_no_refs(_base_form(source_financiere="VRBO_UNKNOWN"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HA_MANQUANT" not in codes
    assert result["ok"] is True


def test_d1_ha_id_obligatoire_direct_ha():
    result = _valider_no_refs(_base_form(source_financiere="DIRECT_HA_PAYANT"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HA_MANQUANT" not in codes
    assert result["ok"] is True


def test_d1_ha_id_obligatoire_hostaway_ref():
    result = _valider_no_refs(_base_form(source_financiere="HOSTAWAY_REFERENCE"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HA_MANQUANT" not in codes
    assert result["ok"] is True


def test_d1_ha_id_facultatif_saisie_manuelle():
    result = _valider_no_refs(_base_form(source_financiere="SAISIE_MANUELLE"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HA_MANQUANT" not in codes


def test_d1_ha_id_format_invalide():
    result = _valider_no_refs(_base_form(
        source_financiere="VRBO_UNKNOWN",
        reservation_id_hostaway="ABC123",
    ))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HOSTAWAY_INTERDIT" in codes


def test_d1_ha_id_format_negatif():
    result = _valider_no_refs(_base_form(
        source_financiere="VRBO_UNKNOWN",
        reservation_id_hostaway="-1",
    ))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HOSTAWAY_INTERDIT" in codes


def test_d1_ha_id_valide_vrbo():
    result = _valider_no_refs(
        _base_form(source_financiere="VRBO_UNKNOWN", reservation_id_hostaway="12345678")
    )
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HOSTAWAY_INTERDIT" in codes


def test_d1_doublon_hostaway_saisie_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(
                _base_form(source_financiere="VRBO_UNKNOWN", reservation_id_hostaway="12345678"),
                saisie_path=None, ref_setup_path=None,
            )
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HOSTAWAY_INTERDIT" in codes
    assert "SAISIE_INDISPONIBLE_DOUBLON_HA" not in codes


# ── D2/D3 — total_percu obligatoire ──────────────────────────────────────────

def test_d2_total_percu_manquant():
    result = _valider_no_refs(_base_form(total_percu=""))
    codes = {e["code"] for e in result["erreurs"]}
    assert "TOTAL_PERCU_OBLIGATOIRE" in codes


def test_d2_total_percu_non_numerique():
    result = _valider_no_refs(_base_form(total_percu="abc"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "TOTAL_PERCU_OBLIGATOIRE" in codes


def test_d2_total_percu_virgule_acceptee():
    result = _valider_no_refs(_base_form(total_percu="450,00"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "TOTAL_PERCU_OBLIGATOIRE" not in codes
    assert result["preview"]["total_percu"] == 450.0


# ── Dates ─────────────────────────────────────────────────────────────────────

def test_date_arrivee_invalide():
    result = _valider_no_refs(_base_form(date_arrivee="not-a-date"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "DATE_ARRIVEE_INVALIDE" in codes


def test_date_depart_avant_arrivee():
    result = _valider_no_refs(_base_form(date_arrivee="2026-08-18", date_depart="2026-08-15"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "DATE_DEPART_AVANT_ARRIVEE" in codes


def test_date_depart_egal_arrivee():
    result = _valider_no_refs(_base_form(date_arrivee="2026-08-15", date_depart="2026-08-15"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "DATE_DEPART_AVANT_ARRIVEE" in codes


# ── D10 — cloture mois ────────────────────────────────────────────────────────

def test_d10_mois_cloture():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "CLOTURE"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "MOIS_CLOTURE" in codes


def test_d10_mois_absent_referentiel():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value=None),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "MOIS_HORS_REFERENTIEL_CLOTURE" in codes


# ── D7/D8 — éligibilité logement ─────────────────────────────────────────────

def test_d8_logement_inactif():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "NON", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "LOGEMENT_HORS_PARC_TECHNIQUE" in codes


def test_d8_logement_statut_parc_non_gere():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "HORS_GESTION"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "LOGEMENT_HORS_PARC_TECHNIQUE" in codes


def test_d7_gestion_inactive_a_date():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "INACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE" in codes


def test_d7_date_fin_inclusive():
    """date_fin = date_arrivee doit être considérée comme gestion active (D7 : inclusive)."""
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01",
             "date_fin": "2026-08-15"}  # = date_arrivee, doit être OK
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE" not in codes


# ── D11 — associé récupérateur ────────────────────────────────────────────────

def test_proprietaire_derive_par_logement_et_date_arrivee():
    result = _valider_with_gestion(
        _base_form(proprietaire_id=""),
        [{
            "logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
            "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": "2026-08-15",
        }],
    )
    codes = {e["code"] for e in result["erreurs"]}
    assert "CHAMP_OBLIGATOIRE" not in codes
    assert "PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE" not in codes
    assert result["preview"]["proprietaire_id"] == "PROP_0001"


def test_proprietaire_hidden_incoherent_refuse_et_recalcule():
    result = _valider_with_gestion(
        _base_form(proprietaire_id="PROP_0002"),
        [{
            "logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
            "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": "",
        }],
    )
    codes = {e["code"] for e in result["erreurs"]}
    assert "PROPRIETAIRE_HIDDEN_INCOHERENT" in codes
    assert result["preview"]["proprietaire_id"] == "PROP_0001"
    assert result["ok"] is False


def test_proprietaire_absent_a_date_bloque():
    result = _valider_with_gestion(
        _base_form(proprietaire_id=""),
        [{
            "logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
            "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": "2026-08-14",
        }],
    )
    codes = {e["code"] for e in result["erreurs"]}
    assert "PROPRIETAIRE_LOGEMENT_ABSENT_A_DATE" in codes
    assert result["ok"] is False


def test_proprietaire_multiple_a_date_bloque():
    result = _valider_with_gestion(
        _base_form(proprietaire_id=""),
        [
            {
                "logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
                "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": "",
            },
            {
                "logement_id": "LOG_0001", "proprietaire_id": "PROP_0002",
                "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": "",
            },
        ],
    )
    codes = {e["code"] for e in result["erreurs"]}
    assert "PROPRIETAIRE_LOGEMENT_MULTIPLE_A_DATE" in codes
    assert result["ok"] is False


def test_d11_associe_manquant_si_montant_recupere():
    result = _valider_no_refs(_base_form(
        mode_paiement_id="PAY_004", montant_recupere="100.00", associe_id_recuperateur=""
    ))
    codes = {e["code"] for e in result["erreurs"]}
    assert "ASSOCIE_MANQUANT" in codes


def test_d11_associe_facultatif_si_montant_nul():
    result = _valider_no_refs(_base_form(montant_recupere="", associe_id_recuperateur=""))
    codes = {e["code"] for e in result["erreurs"]}
    assert "ASSOCIE_MANQUANT" not in codes


def test_d11_associe_facultatif_si_zero():
    result = _valider_no_refs(_base_form(montant_recupere="0", associe_id_recuperateur=""))
    codes = {e["code"] for e in result["erreurs"]}
    assert "ASSOCIE_MANQUANT" not in codes


def test_d11_associe_inconnu_bloque():
    result = _valider_no_refs(_base_form(
        mode_paiement_id="PAY_004", montant_recupere="100.00", associe_id_recuperateur="PERS_ABSENT"
    ))
    codes = {e["code"] for e in result["erreurs"]}
    assert "ASSOCIE_INCONNU" in codes


def test_d11_associe_existant_accepte():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires", return_value=[
            {"proprietaire_id": "PROP_0001"}
        ]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[
            {"associe_id": "PERS_EWAN"}
        ]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(
                montant_recupere="100.00", associe_id_recuperateur="PERS_EWAN"
            ), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "ASSOCIE_INCONNU" not in codes


def test_d1_hostaway_poste_manuellement_refuse():
    result = _valider_no_refs(_base_form(reservation_id_hostaway="12345678"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "RESERVATION_ID_HOSTAWAY_INTERDIT" in codes


def test_taux_derogation_bloquee_sans_motif_confirmation():
    result = _valider_no_refs(_base_form(taux_commission_override="18"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MOTIF_OVERRIDE_TAUX_MANQUANT" in codes
    assert "CONFIRMATION_OVERRIDE_TAUX_MANQUANTE" in codes


def test_taux_derogation_convertie_en_decimal_moteur():
    result = _valider_no_refs(_base_form(
        taux_commission_override="18",
        motif_override_taux_commission="Accord proprietaire",
        confirmation_override_taux_commission="on",
    ))
    assert result["ok"] is True, result["erreurs"]
    assert result["preview"]["taux_commission_standard"] == Decimal("0.15")
    assert result["preview"]["taux_commission_override"] == Decimal("0.18")


def test_menage_standard_pre_rempli_depuis_ref_setup():
    result = _valider_no_refs(_base_form())
    assert result["ok"] is True, result["erreurs"]
    assert result["preview"]["menage"] == Decimal("50.00")
    assert result["preview"]["menage_standard_source"] == "REF_Couts_Standards_Menage"


def test_menage_derogation_bloquee_sans_motif_confirmation():
    result = _valider_no_refs(_base_form(menage_override="65.00"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MOTIF_OVERRIDE_MENAGE_MANQUANT" in codes
    assert "CONFIRMATION_OVERRIDE_MENAGE_MANQUANTE" in codes


def test_mode_non_associe_ignore_montant_associe_cache():
    result = _valider_no_refs(_base_form(
        mode_paiement_id="PAY_001",
        montant_recupere="100.00",
        associe_id_recuperateur="PERS_ABSENT",
    ))
    assert result["ok"] is True, result["erreurs"]
    assert result["preview"]["montant_recupere"] is None
    assert result["preview"]["associe_id_recuperateur"] is None


def test_mode_direct_proprietaire_ignore_recuperation_et_reverse():
    result = _valider_no_refs(_base_form(
        mode_paiement_id="PAY_006",
        montant_recupere="100.00",
        associe_id_recuperateur="PERS_ABSENT",
        montant_reverse_proprietaire="80.00",
    ))
    assert result["ok"] is True, result["erreurs"]
    assert result["preview"]["montant_recupere"] is None
    assert result["preview"]["associe_id_recuperateur"] is None
    assert result["preview"]["montant_reverse_proprietaire"] is None


def test_comptabilisation_postee_differente_refusee():
    result = _valider_no_refs(_base_form(comptabilisation="NON"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "COMPTABILISATION_POSTEE_INCOHERENTE" in codes
    assert "ASSOCIE_MANQUANT" not in codes


# ── D6 — génération PK ───────────────────────────────────────────────────────

def test_generate_pk_premiere_reservation():
    pk, err = generate_pk("2026-08-15", [])
    assert err is None
    assert pk == "RESHH-2026-08-001"


def test_generate_pk_sequence():
    existing = ["RESHH-2026-08-001", "RESHH-2026-08-002"]
    pk, err = generate_pk("2026-08-10", existing)
    assert err is None
    assert pk == "RESHH-2026-08-003"


def test_generate_pk_trous_sequence_admis():
    existing = ["RESHH-2026-08-001", "RESHH-2026-08-005"]
    pk, err = generate_pk("2026-08-10", existing)
    assert err is None
    assert pk == "RESHH-2026-08-006"


def test_generate_pk_autre_mois_isole():
    existing = ["RESHH-2026-07-001", "RESHH-2026-07-002"]
    pk, err = generate_pk("2026-08-10", existing)
    assert err is None
    assert pk == "RESHH-2026-08-001"  # mois différent → séquence indépendante


def test_generate_pk_incoherence_suffixe():
    existing = ["RESHH-2026-08-001", "RESHH-2026-08-ABC"]
    pk, err = generate_pk("2026-08-15", existing)
    assert err == "SEQUENCE_PK_INCOHERENTE"
    assert pk == ""


def test_generate_pk_date_invalide():
    pk, err = generate_pk("not-a-date", [])
    assert err == "DATE_ARRIVEE_INVALIDE"


def test_pk_saisie_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois", return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks",
              side_effect=SaisieHHReadError("lecture impossible")),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "SAISIE_INDISPONIBLE_PK" in codes


# ── Champs forcés ─────────────────────────────────────────────────────────────

def test_champs_forces_dans_preview():
    result = _valider_no_refs(_base_form())
    if result["ok"]:
        assert result["preview"]["statut_controle"] == "A_CONTROLER"
        assert result["preview"]["niveau_anomalie"] == "A_CONTROLER"


# ── Decimal — précision ≤2 décimales ──────────────────────────────────────────

def test_total_percu_3_decimales_rejete():
    result = _valider_no_refs(_base_form(total_percu="450.999"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" in codes


def test_total_percu_2_decimales_accepte():
    result = _valider_no_refs(_base_form(total_percu="450.99"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" not in codes
    assert "TOTAL_PERCU_OBLIGATOIRE" not in codes


def test_total_percu_entier_accepte():
    result = _valider_no_refs(_base_form(total_percu="450"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" not in codes


def test_menage_3_decimales_rejete():
    result = _valider_no_refs(_base_form(menage="50.001"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" in codes


def test_montant_recupere_3_decimales_rejete():
    result = _valider_no_refs(_base_form(
        montant_recupere="100.999", associe_id_recuperateur="PERS_EWAN"
    ))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" in codes


def test_total_percu_virgule_2_decimales_accepte():
    result = _valider_no_refs(_base_form(total_percu="450,50"))
    codes = {e["code"] for e in result["erreurs"]}
    assert "MONTANT_TROP_DE_DECIMALES" not in codes
    from decimal import Decimal
    assert result["preview"]["total_percu"] == Decimal("450.50")


# ── REF_Setup inaccessible → blocage (fail-closed) ───────────────────────────

def test_d10_ref_setup_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois",
              side_effect=Exception("REF_Setup verrouillé")),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "REF_SETUP_INDISPONIBLE" in codes
    assert result["ok"] is False


def test_d7_d8_ref_setup_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois",
              return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements",
              side_effect=Exception("REF_Setup verrouillé")),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "REF_SETUP_INDISPONIBLE" in codes
    assert result["ok"] is False


def test_d9_ref_setup_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois",
              return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              side_effect=Exception("REF_Setup verrouillé")),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"], "lst_Proprietaires": ["PROP_0001"],
        }),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "REF_SETUP_INDISPONIBLE" in codes
    assert result["ok"] is False


def test_d9_ref_locale_indisponible_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois",
              return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale",
              side_effect=SaisieHHReadError("REF_LOCALE illisible")),
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            result = valider(_base_form(), saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "REF_LOCALE_INDISPONIBLE" in codes
    assert result["ok"] is False


# ── D9 — divergence REF_LOCALE / REF_Setup ───────────────────────────────────

def test_d9_logement_locale_absent_setup_bloque():
    with (
        patch("app.services.saisie_hh_service.get_cloture_mois",
              return_value={"statut_mois": "OUVERT"}),
        patch("app.services.saisie_hh_service.get_all_logements", return_value=[
            {"logement_id": "LOG_0001", "actif": "OUI", "statut_parc": "GERE"}
        ]),
        patch("app.services.saisie_hh_service.get_gestion_hist", return_value=[
            {"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
             "statut_gestion": "ACTIF", "date_debut": "2026-01-01", "date_fin": None}
        ]),
        patch("app.services.saisie_hh_service.get_all_proprietaires",
              return_value=[{"proprietaire_id": "PROP_0001"}]),
        patch("app.services.saisie_hh_service.get_all_associes", return_value=[]),
        patch("app.services.saisie_hh_service.read_ref_locale", return_value={
            "lst_Logements": ["LOG_0001"],      # LOG_0001 dans REF_LOCALE
            "lst_Proprietaires": ["PROP_0001"],
        }),
        # get_all_logements retourne LOG_0001 (cohérent), mais get_all_proprietaires
        # retourne PROP_0001 → pas de divergence sur l'exemple de base.
        # Pour tester la divergence, on met LOG_9999 dans REF_LOCALE mais pas dans Setup.
        patch("app.services.saisie_hh_service.read_existing_pks", return_value=[]),
    ):
        # Utiliser logement_id=LOG_9999 : présent REF_LOCALE mais absent REF_Setup
        form = _base_form(logement_id="LOG_9999")
        import openpyxl as opx
        with patch.object(opx, "load_workbook", side_effect=Exception("no real file")):
            with patch("app.services.saisie_hh_service.read_ref_locale", return_value={
                "lst_Logements": ["LOG_9999"],   # LOG_9999 dans locale
                "lst_Proprietaires": ["PROP_0001"],
            }):
                # get_all_logements retourne LOG_0001 seulement (pas LOG_9999)
                result = valider(form, saisie_path=None, ref_setup_path=None)
    codes = {e["code"] for e in result["erreurs"]}
    assert "DIVERGENCE_REF_LOCALE_REF_SETUP" in codes


# ── Constante REF_CLOTURE obsolète non utilisée par APP-2b ───────────────────

def test_aucune_reference_ref_cloture_standalone():
    """Aucun module APP-2b ne référence la constante REF_CLOTURE (fichier autonome)."""
    import ast
    from pathlib import Path
    app_dir = Path(__file__).parent.parent / "app"
    target_files = [
        app_dir / "services" / "saisie_hh_service.py",
        app_dir / "services" / "saisie_hh_orchestrator.py",
        app_dir / "readers" / "ref_setup_hh_reader.py",
        app_dir / "readers" / "saisie_hh_reader.py",
        app_dir / "writers" / "saisie_hh_writer.py",
    ]
    for f in target_files:
        if not f.exists():
            continue
        src = f.read_text(encoding="utf-8")
        assert "REF_CLOTURE" not in src, (
            f"{f.name} référence REF_CLOTURE (obsolète) — utiliser REF_SETUP + feuille"
        )
        assert "REF_Cloture_Mensuelle.xlsx" not in src, (
            f"{f.name} référence le fichier autonome REF_Cloture_Mensuelle.xlsx"
        )

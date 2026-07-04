"""APP-2b — Tests routes saisie HH.

Routes testées :
- GET  /reservations/nouvelle
- POST /reservations/nouvelle/verifier
- POST /reservations/nouvelle/confirmer

Aucun fichier Excel réel touché. Les services sont patchés.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client(tmp_db):
    with TestClient(app) as c:
        yield c


def _refs_data():
    return {
            "lst_Canaux":          ["CANAL_001"],
            "lst_Source_Financiere": ["SAISIE_MANUELLE"],
            "lst_Proprietaires":   ["PROP_0001"],
            "lst_Logements":       ["LOG_0001"],
            "lst_Associes":        ["PERS_EWAN"],
            "lst_ModesPaiement":   ["PAY_001", "PAY_002", "PAY_003", "PAY_004", "PAY_006"],
            "lst_Codes_Impact":    ["HC", "IC", "HR"],
            "lst_Comptabilisation": ["OUI", "NON"],
            "options_logements": [
                {
                    "value": "LOG_0001",
                    "label": "Cyprien",
                }
            ],
            "options_canaux": [
                {"value": "CANAL_001", "label": "Airbnb"},
                {"value": "CANAL_002", "label": "Booking"},
                {"value": "CANAL_003", "label": "VRBO"},
                {"value": "CANAL_004", "label": "Direct"},
                {"value": "CANAL_005", "label": "Conciergerie"},
            ],
            "options_sources_financieres": [
                {"value": "SAISIE_MANUELLE", "label": "Saisie manuelle"},
                {"value": "VRBO_UNKNOWN", "label": "VRBO sans reference"},
            ],
            "options_proprietaires": [{"value": "PROP_0001", "label": "David Dupont"}],
            "options_associes": [{"value": "PERS_EWAN", "label": "Ewan"}],
            "options_modes_paiement": [
                {"value": "PAY_001", "label": "Banque pro"},
                {"value": "PAY_002", "label": "Especes"},
                {"value": "PAY_003", "label": "Carte associee"},
                {"value": "PAY_004", "label": "Compte perso associe"},
                {"value": "PAY_006", "label": "Direct proprietaire"},
            ],
            "options_codes_impact": [
                {"value": "HC", "label": "Hors comptabilite"},
                {"value": "IC", "label": "Impact comptable"},
                {"value": "HR", "label": "Hors resultat"},
            ],
            "options_comptabilisation": [
                {"value": "OUI", "label": "A comptabiliser"},
                {"value": "NON", "label": "Ne pas comptabiliser"},
            ],
            "proprietaire_labels": {"PROP_0001": "David Dupont"},
            "logement_owner_history": {
                "LOG_0001": [
                    {
                        "owner_id": "PROP_0001",
                        "owner_label": "David Dupont",
                        "date_debut": "2026-01-01",
                        "date_fin": "",
                    }
                ]
            },
            "logement_type_map": {"LOG_0001": "TYPE_T3"},
            "mode_code_map": {
                "PAY_001": "BANQUE_PRO",
                "PAY_002": "ESPECES_CAISSE",
                "PAY_003": "CARTE_ASSOCIEE",
                "PAY_004": "COMPTE_PERSO_ASSOCIEE",
                "PAY_006": "DIRECT_PROPRIETAIRE",
            },
            "impact_comptabilisation_map": {"HC": "NON", "IC": "OUI", "HR": "NON"},
            "taux_commission_history": [
                {
                    "proprietaire_id": "PROP_0001",
                    "logement_id": "",
                    "taux_commission": "0.15",
                    "date_debut": "2026-01-01",
                    "date_fin": "",
                }
            ],
            "menage_standard_history": [
                {
                    "type_logement_id": "TYPE_T3",
                    "cout_standard_menage": "60.00",
                    "date_debut": "2026-01-01",
                    "date_fin": "",
                }
            ],
            "mois_ouverts": ["2026-06", "2026-07"],
            "mois_ouverts_labels": ["juin 2026", "juillet 2026"],
            "cloture_mois_status": {
                "2026-05": {"statut_mois": "CLOTURE", "label": "mai 2026"},
                "2026-06": {"statut_mois": "OUVERT", "label": "juin 2026"},
                "2026-07": {"statut_mois": "OUVERT", "label": "juillet 2026"},
            },
        }


def _patch_refs():
    """Patch load_form_refs pour retourner des listes minimales."""
    return patch(
        "app.routes.reservations.saisie_svc.load_form_refs",
        return_value=_refs_data(),
    )


def _patch_valider_ok():
    """Patch valider pour retourner une validation réussie."""
    return patch(
        "app.routes.reservations.saisie_svc.valider",
        return_value={
            "ok": True,
            "erreurs": [],
            "pk": "RESHH-2026-08-001",
            "preview": {
                "reservation_hh_id":          "RESHH-2026-08-001",
                "canal_id":                    "CANAL_001",
                "source_financiere":           "SAISIE_MANUELLE",
                "proprietaire_id":             "PROP_0001",
                "logement_id":                 "LOG_0001",
                "reservation_id_hostaway":     None,
                "date_arrivee":                "2026-08-15",
                "date_depart":                 "2026-08-18",
                "total_percu":                 450.0,
                "menage":                      None,
                "commentaire_taux_commission": None,
                "montant_recupere":            None,
                "associe_id_recuperateur":     None,
                "montant_reverse_proprietaire": None,
                "mode_paiement_id":            None,
                "code_impact":                 "HC",
                "comptabilisation":            "OUI",
                "statut_controle":             "A_CONTROLER",
                "niveau_anomalie":             "A_CONTROLER",
                "code_anomalie":               None,
                "commentaire":                 None,
                "mois":                        "2026-08",
            },
        },
    )


def _patch_valider_erreurs():
    """Patch valider pour retourner des erreurs."""
    return patch(
        "app.routes.reservations.saisie_svc.valider",
        return_value={
            "ok": False,
            "erreurs": [{"champ": "total_percu", "code": "TOTAL_PERCU_OBLIGATOIRE",
                          "message": "total_percu requis"}],
            "pk": "",
            "preview": {},
        },
    )


# ── Route GET /reservations/nouvelle ─────────────────────────────────────────

def test_get_nouvelle_200(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert resp.status_code == 200
    assert "Nouvelle réservation" in resp.text
    assert "HH_REAL_WRITE_ENABLED" in resp.text


def test_get_nouvelle_contient_selects(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "CANAL_001" in resp.text
    assert "SAISIE_MANUELLE" in resp.text
    assert "LOG_0001" in resp.text


def test_get_nouvelle_source_saisie_manuelle_preselectionnee(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert '<option value="SAISIE_MANUELLE" selected' in resp.text


def test_get_nouvelle_ne_rend_pas_id_hostaway(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "reservation_id_hostaway" not in resp.text


def test_get_nouvelle_affiche_libelles_lisibles_et_garde_ids(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert resp.status_code == 200
    assert '<option value="LOG_0001"' in resp.text
    assert "Cyprien" in resp.text
    assert '"owner_id": "PROP_0001"' in resp.text
    assert '"owner_label": "David Dupont"' in resp.text
    assert '<option value="CANAL_001"' in resp.text and ">Airbnb</option>" in resp.text
    assert '<option value="PAY_001"' in resp.text and ">Banque pro</option>" in resp.text
    assert '<option value="HC"' in resp.text and ">Hors comptabilite</option>" in resp.text
    assert 'id="comptabilisation_display"' in resp.text
    assert 'id="comptabilisation" name="comptabilisation"' in resp.text
    assert "Libelle absent -" not in resp.text


def test_get_nouvelle_affiche_les_cinq_canaux_reels(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    for canal_id, label in [
        ("CANAL_001", "Airbnb"),
        ("CANAL_002", "Booking"),
        ("CANAL_003", "VRBO"),
        ("CANAL_004", "Direct"),
        ("CANAL_005", "Conciergerie"),
    ]:
        assert f'<option value="{canal_id}"' in resp.text
        assert f">{label}</option>" in resp.text


def test_get_nouvelle_proprietaire_derive_non_libre(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'id="proprietaire_display"' in resp.text
    assert 'readonly aria-readonly="true"' in resp.text
    assert 'type="hidden" id="proprietaire_id" name="proprietaire_id"' in resp.text
    assert 'select id="proprietaire_id"' not in resp.text
    assert "David Dupont" in resp.text


def test_get_nouvelle_logement_sans_proprietaire_bloquant(client):
    refs = _refs_data()
    refs["options_logements"] = [{"value": "LOG_9999", "label": "Logement sans proprietaire"}]
    refs["logement_owner_history"] = {}
    with patch("app.routes.reservations.saisie_svc.load_form_refs", return_value=refs):
        resp = client.get("/reservations/nouvelle")
    assert "Aucun proprietaire trouve pour ce logement a cette date" in resp.text


def test_get_nouvelle_logement_plusieurs_proprietaires_bloquant(client):
    refs = _refs_data()
    refs["options_logements"] = [{"value": "LOG_8888", "label": "Logement ambigu"}]
    refs["logement_owner_history"] = {
        "LOG_8888": [
            {"owner_id": "PROP_0001", "owner_label": "David", "date_debut": "2026-01-01", "date_fin": ""},
            {"owner_id": "PROP_0002", "owner_label": "Marie", "date_debut": "2026-01-01", "date_fin": ""},
        ]
    }
    with patch("app.routes.reservations.saisie_svc.load_form_refs", return_value=refs):
        resp = client.get("/reservations/nouvelle")
    assert '"owner_id": "PROP_0001"' in resp.text
    assert '"owner_id": "PROP_0002"' in resp.text
    assert "Plusieurs proprietaires trouves pour ce logement a cette date" in resp.text


def test_get_nouvelle_obligations_dynamiques_presentes(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'id="associe_required_marker"' in resp.text
    assert 'id="montant_recupere_required_marker"' in resp.text
    assert "COMPTE_PERSO_ASSOCIEE" in resp.text
    assert "CARTE_ASSOCIEE" in resp.text
    assert 'id="hostaway_required_marker"' not in resp.text
    assert "form.checkValidity()" in resp.text
    assert "form.reportValidity()" in resp.text
    assert "novalidate" not in resp.text


def test_get_nouvelle_affiche_mois_ouverts_ref_cloture(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "Mois ouverts pour saisie : juin 2026, juillet 2026" in resp.text
    assert "const openMonths = new Set" in resp.text
    assert '"2026-06"' in resp.text
    assert '"2026-07"' in resp.text


def test_get_nouvelle_mois_cloture_indisponible_cote_interface(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert '"2026-05"' in resp.text
    assert '"statut_mois": "CLOTURE"' in resp.text
    assert "aucune nouvelle réservation ne peut être saisie sur ce mois" in resp.text


def test_get_nouvelle_mois_absent_indisponible_cote_interface(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "n'est pas ouvert dans le référentiel de clôture" in resp.text
    assert "clotureMonthStatus[monthId] || null" in resp.text


def test_get_nouvelle_bouton_desactive_pour_mois_indisponible(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'id="verify_submit"' in resp.text
    assert "verifySubmit.disabled = true" in resp.text
    assert "if (!validateClotureMonth())" in resp.text


def test_get_nouvelle_bouton_reactive_pour_mois_ouvert(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "openMonths.has(monthId)" in resp.text
    assert "verifySubmit.disabled = false" in resp.text


def test_get_nouvelle_aucun_mois_ouvert_message_explicite(client):
    refs = _refs_data()
    refs["mois_ouverts"] = []
    refs["mois_ouverts_labels"] = []
    with patch("app.routes.reservations.saisie_svc.load_form_refs", return_value=refs):
        resp = client.get("/reservations/nouvelle")
    assert "Aucun mois ouvert actuellement dans le référentiel." in resp.text


def test_get_nouvelle_derogation_menage_et_paiement_conditionnel(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'name="menage_override"' in resp.text
    assert 'name="motif_override_menage"' in resp.text
    assert "confirmation_override_menage" in resp.text
    assert "conditional-associated" in resp.text
    assert "conditional-cash" in resp.text
    assert "modeCodeMap" in resp.text
    assert "menageHistory" in resp.text
    assert 'id="menage_derogation_toggle"' in resp.text
    assert "Modifier le prix menage" in resp.text
    assert 'id="menage_derogation_reset"' in resp.text
    assert 'id="override_modal_menage"' in resp.text


def test_get_nouvelle_affiche_taux_commission_et_derogation_non_persistante(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'id="taux_commission_message"' in resp.text
    assert "Taux applicable" in resp.text
    assert "tauxHistory" in resp.text
    assert "taux logement" in resp.text
    assert "taux proprietaire" in resp.text
    assert "Modifier le taux" in resp.text
    assert 'id="taux_derogation_reset"' in resp.text
    assert 'for="taux_commission_override_pct">Taux derogatoire (%)' in resp.text
    assert 'name="taux_commission_override_pct"' in resp.text
    assert 'name="taux_commission_override"' not in resp.text
    assert 'name="motif_override_taux_commission"' in resp.text
    assert 'name="taux_commission"' not in resp.text


def test_get_nouvelle_derogations_modale_sans_bloc_jaune_ni_checkbox(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "alert-warning" not in resp.text
    assert 'type="checkbox"' not in resp.text
    assert "Je confirme remplacer" not in resp.text
    assert 'id="override_modal"' in resp.text
    assert "Annuler" in resp.text
    assert "Confirmer la derogation" in resp.text
    assert "modalCancel.focus()" in resp.text
    assert "form.requestSubmit()" in resp.text
    assert "Motif obligatoire pour la derogation de taux" in resp.text
    assert "Motif obligatoire pour la derogation de menage" in resp.text


def test_get_nouvelle_parse_number_vide_ne_vaut_pas_zero(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "function parseNumber(value)" in resp.text
    assert "const raw = String(value ?? \"\")" in resp.text
    assert "if (!raw) {" in resp.text
    assert "return null;" in resp.text
    assert 'Number(String(value || "")' not in resp.text


def test_get_nouvelle_derogation_requise_seulement_si_panneau_ouvert_et_valeur(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "function isOverrideRequested(editPanel, field, automaticValue)" in resp.text
    assert "if (editPanel.hidden || !raw)" in resp.text
    assert "taux: isOverrideRequested(tauxDerogationInfo, tauxOverride, tauxAuto)" in resp.text
    assert "menage: isOverrideRequested(menageDerogationInfo, menageOverride, menageAuto)" in resp.text
    assert "modalTaux.hidden = !state.taux" in resp.text
    assert "modalMenage.hidden = !state.menage" in resp.text
    assert "clearOverride(tauxOverride, tauxOverrideMotif, tauxOverrideConf)" in resp.text
    assert "clearOverride(menageOverride, menageOverrideMotif, menageOverrideConf)" in resp.text


def test_get_nouvelle_reset_taux_et_menage_independants(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "resetOverride(tauxDerogationInfo, tauxOverride, tauxOverrideMotif, tauxOverrideConf)" in resp.text
    assert "resetOverride(menageDerogationInfo, menageOverride, menageOverrideMotif, menageOverrideConf)" in resp.text
    assert "editPanel.hidden = true" in resp.text


def test_get_nouvelle_modale_css_opaque_zindex_scroll(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert "z-index: 2147483647" in resp.text
    assert "background: rgba(0, 0, 0, 0.45)" in resp.text
    assert "background: #ffffff" in resp.text
    assert "opacity: 1" in resp.text
    assert "max-height: calc(100vh - 48px)" in resp.text
    assert "overflow-y: auto" in resp.text
    assert "isolation: isolate" in resp.text
    assert "width: min(560px, 100%)" in resp.text
    assert ".modal-section textarea" in resp.text
    assert "width: 100%" in resp.text
    assert resp.text.index("</form>") < resp.text.index('<div id="override_modal"')


def test_get_nouvelle_modale_annulation_ne_soumet_pas(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    assert 'modalCancel.addEventListener("click"' in resp.text
    assert "allowConfirmedSubmit = false" in resp.text
    assert "modal.hidden = true" in resp.text
    assert "openOverrideModal(state)" in resp.text


def test_get_nouvelle_visibilite_paiement_navigateur(client):
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    for mode, label in [
        ("PAY_001", "Banque pro"),
        ("PAY_002", "Especes"),
        ("PAY_003", "Carte associee"),
        ("PAY_004", "Compte perso associe"),
        ("PAY_006", "Direct proprietaire"),
    ]:
        assert f'<option value="{mode}"' in resp.text
        assert f">{label}</option>" in resp.text
    assert 'class="form-group conditional-associated"' in resp.text
    assert 'class="form-group conditional-cash"' in resp.text
    assert 'container.hidden = !associeMode' in resp.text
    assert 'container.hidden = !especesMode' in resp.text
    assert 'montantRecupere.required = associeMode' in resp.text
    assert 'associe.required = associeMode' in resp.text
    assert 'montantRecupere.value = ""' in resp.text
    assert 'associe.value = ""' in resp.text
    assert 'montantReverse.value = ""' in resp.text
    assert '[hidden] { display: none !important; }' in resp.text


def test_nouvelle_avant_detail_pas_de_conflit(client):
    """GET /reservations/nouvelle ne doit pas matcher {reservation_hh_id}."""
    with _patch_refs():
        resp = client.get("/reservations/nouvelle")
    # Doit retourner le formulaire, pas la page détail
    assert resp.status_code == 200
    assert "Prévisualisation" not in resp.text
    assert "Nouvelle réservation" in resp.text


# ── Route POST /reservations/nouvelle/verifier ────────────────────────────────

def test_post_verifier_validation_ok_affiche_preview(client):
    with _patch_refs(), _patch_valider_ok():
        resp = client.post("/reservations/nouvelle/verifier", data={
            "canal_id": "CANAL_001",
            "source_financiere": "SAISIE_MANUELLE",
            "proprietaire_id": "PROP_0001",
            "logement_id": "LOG_0001",
            "date_arrivee": "2026-08-15",
            "date_depart": "2026-08-18",
            "total_percu": "450.00",
            "code_impact": "HC",
            "comptabilisation": "OUI",
        })
    assert resp.status_code == 200
    assert "RESHH-2026-08-001" in resp.text
    assert "Prévisualisation" in resp.text


def test_post_verifier_validation_ko_affiche_erreurs(client):
    with _patch_refs(), _patch_valider_erreurs():
        resp = client.post("/reservations/nouvelle/verifier", data={})
    assert resp.status_code == 422
    assert "TOTAL_PERCU_OBLIGATOIRE" in resp.text


def test_post_verifier_preview_contient_champs_forces(client):
    with _patch_refs(), _patch_valider_ok():
        resp = client.post("/reservations/nouvelle/verifier", data={
            "canal_id": "CANAL_001",
            "total_percu": "450.00",
        })
    assert resp.status_code == 200
    assert "A_CONTROLER" in resp.text  # statut_controle + niveau_anomalie


# ── Route POST /reservations/nouvelle/confirmer ───────────────────────────────

def test_post_confirmer_guard_securite(client):
    """Confirmer doit renvoyer GARDE_SECURITE quand HH_REAL_WRITE_ENABLED=False."""
    garde_result = {
        "statut": "GARDE_SECURITE",
        "pk": "RESHH-2026-08-001",
        "mois": "2026-08",
        "ligne_cible": None,
        "details": "HH_REAL_WRITE_ENABLED = False",
    }
    with (
        _patch_refs(),
        patch("app.routes.reservations.saisie_svc.valider") as valider,
        patch("app.routes.reservations.saisie_svc.build_row_data", return_value={}),
        patch("app.routes.reservations.hh_orchestrator.confirm_write",
              return_value=garde_result),
    ):
        resp = client.post("/reservations/nouvelle/confirmer", data={
            "canal_id": "CANAL_001",
            "total_percu": "450.00",
        })
    assert resp.status_code == 200
    assert 'data-statut="GARDE_SECURITE"' in resp.text
    assert "Écriture désactivée" in resp.text
    valider.assert_not_called()


def test_post_confirmer_revalide_avant_ecriture(client):
    """Si la revalidation échoue lors du confirmer, on n'appelle pas le writer."""
    write_called = []

    def fake_write(**kwargs):
        write_called.append(True)
        return {"statut": "OK"}

    with (
        _patch_refs(),
        _patch_valider_erreurs(),
        patch("app.routes.reservations.cfg.HH_REAL_WRITE_ENABLED", True),
        patch("app.routes.reservations.hh_orchestrator.confirm_write", side_effect=fake_write),
    ):
        resp = client.post("/reservations/nouvelle/confirmer", data={})

    assert resp.status_code == 422
    assert len(write_called) == 0, "Le writer ne doit pas être appelé si validation échoue"


# ── Vérification que /reservations/nouvelle précède {reservation_hh_id} ──────

def test_route_ordering_nouvelle_avant_parametre(client):
    """'nouvelle' est un mot-clé statique — ne doit jamais matcher {reservation_hh_id}."""
    # Mock le détail pour simuler une potentielle collision de route
    with patch("app.routes.reservations.svc.load_detail") as mock_detail:
        mock_detail.return_value = None
        with _patch_refs():
            resp = client.get("/reservations/nouvelle")
        # load_detail ne doit PAS avoir été appelé
        mock_detail.assert_not_called()
    assert resp.status_code == 200

"""Cross-module : un logement archivé via `logements_gestion_service` ne doit plus être
sélectionnable dans les NOUVEAUX traitements (réservations, charges) mais doit rester visible dans
l'historique.

Ces modules lisent tous le référentiel `ref_logements` / `ref_gestion_logements_hist` — les mêmes
tables que `logements_gestion_service` écrit depuis la migration 0051. Pas de cache intermédiaire à
invalider : l'exclusion est immédiate, sans attendre un cycle de pipeline (Lot13/PBI),
contrairement à la vue fiche/liste (`logements_service.py` — voir `28_CYCLE_DE_VIE_LOGEMENT.md`).

PÉRIMÈTRE : l'éligibilité côté CHARGES est ici évaluée sur le référentiel SQLite. Le lecteur de
SAISIE des charges (`saisie_charges_reader`) lit encore le classeur — sa migration relève du
chantier Charges, pas de celui du référentiel. La RÈGLE testée (`actif == OUI`) est la même dans
les deux cas ; c'est elle que ce test protège.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.readers.ref_setup_hh_reader import get_all_logements_hh, get_gestion_hist
from app.services import logements_gestion_service as svc
from app.services import referentiel_service as ref_svc
from app.services.saisie_hh_service import _is_managed_active_logement


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer(
        tmp_db,
        logements=[{"logement_id": "LOG_A1", "hostaway_listing_id": "900001",
                    "nom_logement_officiel": "Fictif A1", "nom_court": "A1", "adresse": "1 rue",
                    "ville": "RECETTE", "type_logement_id": "TYPE_001", "sur_hostaway": "OUI",
                    "actif": "OUI", "statut_parc": "GERE", "commentaire": "",
                    "forfait_logiciel_consommables_mensuel": "0"}],
        gestion=[{"gestion_id": "GST_1", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
                  "date_debut": "2026-01-01", "date_fin": "", "statut_gestion": "ACTIF",
                  "source": "FICTIF", "commentaire": ""}],
        proprietaires=[{"proprietaire_id": "PROP_A", "nom_proprietaire": "Nom PROP_A",
                        "actif": "OUI"}],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
    )
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _eligible_charges(db_path) -> set[str]:
    """Reproduit le filtre réel de `charges_preview_service.load_form_refs` :
    `[l for l in logements if is_active(l)]` avec `is_active = actif == 'OUI'`."""
    return {str(l.get("logement_id", "")).strip()
            for l in ref_svc.logements(db_path=db_path)
            if str(l.get("actif", "")).upper() == "OUI"}


def _eligible_reservations(db_path) -> set[str]:
    """Reproduit le filtre réel de `saisie_hh_service.load_form_refs` :
    `_is_managed_active_logement` (actif=OUI ET statut_parc=GERE)."""
    logs = get_all_logements_hh(db_path=db_path)
    return {str(l.get("logement_id", "")).strip() for l in logs
            if str(l.get("logement_id", "")).strip() and _is_managed_active_logement(l)}


def test_logement_actif_est_selectionnable_partout(ref):
    assert "LOG_A1" in _eligible_charges(ref)
    assert "LOG_A1" in _eligible_reservations(ref)


def test_archiver_exclut_immediatement_des_nouveaux_traitements(ref):
    res = svc.archiver("LOG_A1", "2026-06-30", db_path=ref)
    assert res["ok"], res

    assert "LOG_A1" not in _eligible_charges(ref)
    assert "LOG_A1" not in _eligible_reservations(ref)


def test_archiver_reste_visible_dans_lhistorique(ref):
    svc.archiver("LOG_A1", "2026-06-30", db_path=ref)

    # Le logement reste lisible : la ligne existe toujours, jamais supprimée.
    assert any(str(l.get("logement_id")) == "LOG_A1" for l in ref_svc.logements(db_path=ref))

    # Le rattachement de gestion clôturé reste dans l'historique (jamais supprimé).
    gest = get_gestion_hist(db_path=ref)
    ligne = next(g for g in gest if str(g.get("logement_id")) == "LOG_A1")
    assert ligne["date_fin"] == "2026-06-30"
    assert ligne["statut_gestion"] == "RETIRE"

    # Historique applicatif (gestion_svc.historique) le retrouve aussi.
    histo = svc.historique("LOG_A1", db_path=ref)
    assert any(g["logement_id"] == "LOG_A1" for g in histo["gestion"])


def test_reactiver_rend_de_nouveau_selectionnable(ref):
    svc.archiver("LOG_A1", "2026-06-30", db_path=ref)
    assert "LOG_A1" not in _eligible_charges(ref)

    res = svc.reactiver("LOG_A1", "2026-07-01", "PROP_A", db_path=ref)
    assert res["ok"], res

    assert "LOG_A1" in _eligible_charges(ref)
    assert "LOG_A1" in _eligible_reservations(ref)


def test_changer_taux_resolu_correctement_a_toutes_les_dates(ref):
    """Le taux de commission historisé doit se résoudre correctement à n'importe quelle date,
    y compris avant/après un changement — cohérence avec `resolve_commission_rate` (Lot10)."""
    svc.changer_taux_commission("LOG_A1", 0.19, "2026-01-01", "PROP_A", db_path=ref)
    svc.changer_taux_commission("LOG_A1", 0.15, "2026-03-01", "PROP_A", db_path=ref)

    histo = svc.historique("LOG_A1", db_path=ref)["taux"]
    par_debut = sorted(histo, key=lambda t: t["date_debut"])
    assert par_debut[0]["date_debut"] == "2026-01-01" and par_debut[0]["date_fin"] == "2026-02-28"
    assert float(par_debut[0]["taux_commission"]) == 0.19
    # Période courante : `date_fin` vide (le référentiel SQLite rend une chaîne, jamais None).
    assert par_debut[1]["date_debut"] == "2026-03-01" and not par_debut[1]["date_fin"]
    assert float(par_debut[1]["taux_commission"]) == 0.15

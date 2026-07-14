"""APP-3b — Route de confirmation et écrans associés.

Aucune écriture réelle : les flags restent False dans tous ces tests (sauf mention), donc la route
refuse. On vérifie surtout les propriétés de la ROUTE elle-même : POST-Redirect-Get, protection
contre la double soumission, aucun payload métier accepté du navigateur, et restitution lisible.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.main import app
from app.routes import fournisseurs as route
from app.services import charges_confirmation_service as confirmation
from app.services import charges_preview_service as prev

REELS = [cfg.SAISIE_CHARGES, cfg.SAISIE_CHARGES_IMPACTS, cfg.MASTER_CHARGES, cfg.SAISIE_IK_AVANTAGES]


def _sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


@pytest.fixture
def client(tmp_db):
    with TestClient(app) as c:
        yield c


@pytest.fixture
def dryruns(tmp_path: Path, monkeypatch) -> Path:
    """Les dry-runs de ces tests vivent sous tmp_path, jamais dans data/dryruns réel."""
    d = tmp_path / "dryruns"
    monkeypatch.setattr(prev, "DRYRUNS_DIR", d)
    monkeypatch.setattr(cfg, "DRYRUNS_DIR", d)
    return d


def _previsualiser(dryruns: Path) -> str:
    """Prévisualisation réelle (lecture seule sur les vrais classeurs) → token."""
    form = {
        "date_charge": "2026-06-15", "montant": "100.00", "categorie_charge_id": "CHG_025",
        "code_impact": "IC", "mode_paiement_id": "PAY_001",
        "impact_menage": "NON", "refacturable": "NON",
    }
    res = prev.previsualiser(form, dryruns_root=dryruns)
    assert res["ok"], res["manifest"].get("errors")
    return res["token"]


# ── Écran de prévisualisation ────────────────────────────────────────────────

def test_previsualisation_affiche_le_bouton_desactive_flags_off(client, dryruns):
    token = _previsualiser(dryruns)
    html = client.get(f"/fournisseurs/nouvelle/previsualisation/{token}").text

    assert 'data-testid="bloc-confirmation"' in html
    assert 'data-testid="ecriture-desactivee"' in html
    assert "disabled" in html                                  # bouton présent mais inactif
    assert "n'est pas activée" in html
    assert 'action="/fournisseurs/nouvelle/confirmer/' not in html   # aucun formulaire d'écriture
    assert "Revenir modifier" in html


def test_previsualisation_affiche_leffet_prevu_sur_les_fichiers(client, dryruns):
    token = _previsualiser(dryruns)
    html = client.get(f"/fournisseurs/nouvelle/previsualisation/{token}").text

    assert 'data-testid="effet-fichiers"' in html
    assert "SAISIE_Charges_Flux.xlsx" in html
    assert "SAISIE_Charges_Impacts.xlsx" in html
    assert "MASTER_FACT_MAN_Charges" in html                   # Lot3 annoncé, hors transaction


def test_previsualisation_affiche_le_formulaire_quand_les_flags_sont_actifs(
    client, dryruns, monkeypatch
):
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    token = _previsualiser(dryruns)
    html = client.get(f"/fournisseurs/nouvelle/previsualisation/{token}").text

    assert f'action="/fournisseurs/nouvelle/confirmer/{token}"' in html
    assert 'method="post"' in html
    assert 'data-testid="avertissement-ecriture"' in html
    assert "écrit réellement" in html


def test_un_seul_flag_ne_suffit_pas_a_activer_le_bouton(client, dryruns, monkeypatch):
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)   # le second reste False
    token = _previsualiser(dryruns)
    html = client.get(f"/fournisseurs/nouvelle/previsualisation/{token}").text
    assert 'data-testid="ecriture-desactivee"' in html
    assert 'action="/fournisseurs/nouvelle/confirmer/' not in html


# ── Route de confirmation ────────────────────────────────────────────────────

def test_confirmation_flags_off_refuse_et_ne_touche_rien(client, dryruns):
    """Le POST est possible (curl, rejeu), mais l'écriture est refusée en aval."""
    empreintes = {p: _sha(p) for p in REELS if p.exists()}
    token = _previsualiser(dryruns)

    r = client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    assert r.status_code == 303                                # POST-Redirect-Get
    assert r.headers["location"] == f"/fournisseurs/nouvelle/resultat/{token}"

    html = client.get(f"/fournisseurs/nouvelle/resultat/{token}").text
    assert 'data-statut="REFUSE"' in html
    assert "E_FLAGS_DESACTIVES" in html
    assert "pas activée" in html

    assert {p: _sha(p) for p in REELS if p.exists()} == empreintes


def test_le_navigateur_ne_peut_pas_injecter_de_payload_metier(client, dryruns):
    """Un POST chargé de champs métier ne change RIEN : la route ne lit que le token."""
    token = _previsualiser(dryruns)
    r = client.post(
        f"/fournisseurs/nouvelle/confirmer/{token}",
        data={"montant": "999999", "charge_id": "CHG-PIRATE", "categorie_charge_id": "CHG_001"},
        follow_redirects=False,
    )
    assert r.status_code == 303

    resultat = confirmation.charger_resultat(token, dryruns)
    assert resultat["code"] == "E_FLAGS_DESACTIVES"            # refusé, et surtout :
    assert "PIRATE" not in str(resultat)                       # le payload client n'a laissé aucune trace


def test_rafraichir_la_page_de_resultat_ne_rejoue_jamais_lecriture(client, dryruns, monkeypatch):
    """La page de résultat est un GET : la rafraîchir ne peut pas réécrire."""
    appels: list[str] = []
    vrai = confirmation.confirmer
    monkeypatch.setattr(confirmation, "confirmer",
                        lambda t, **kw: appels.append(t) or vrai(t, **kw))
    monkeypatch.setattr(route.confirmation, "confirmer", confirmation.confirmer)

    token = _previsualiser(dryruns)
    client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    assert len(appels) == 1

    for _ in range(3):
        assert client.get(f"/fournisseurs/nouvelle/resultat/{token}").status_code == 200
    assert len(appels) == 1                                    # aucun nouvel appel d'écriture


def test_double_soumission_ne_reexecute_pas_la_confirmation(client, dryruns, monkeypatch):
    """Re-POSTer le même token : la route redirige sans relancer la confirmation."""
    appels: list[str] = []
    vrai = confirmation.confirmer

    def espion(t, **kw):
        appels.append(t)
        return vrai(t, **kw)

    monkeypatch.setattr(route.confirmation, "confirmer", espion)

    token = _previsualiser(dryruns)
    client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)

    assert len(appels) == 1                                    # une seule exécution réelle


def test_resultat_introuvable(client, dryruns):
    r = client.get("/fournisseurs/nouvelle/resultat/20260101T000000Z_inexistant00")
    assert r.status_code == 404
    assert 'data-testid="resultat-introuvable"' in r.text


def test_confirmer_un_token_inconnu_est_refuse_proprement(client, dryruns, monkeypatch):
    """Flags actifs, sinon c'est le refus de garde qui primerait (et masquerait le token inconnu)."""
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    token = "20260101T000000Z_inexistant00"
    r = client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    # Aucun dossier de prévisualisation où déposer un résultat : le refus est rendu directement.
    assert r.status_code == 404
    assert 'data-statut="REFUSE"' in r.text
    assert "E_TOKEN_INCONNU" in r.text
    assert "introuvable" in r.text          # l'expiration a désormais son propre code/message


def test_la_previsualisation_reste_en_lecture_seule(client, dryruns):
    """La route GET de prévisualisation n'écrit jamais dans les fichiers réels."""
    empreintes = {p: _sha(p) for p in REELS if p.exists()}
    token = _previsualiser(dryruns)
    client.get(f"/fournisseurs/nouvelle/previsualisation/{token}")
    assert {p: _sha(p) for p in REELS if p.exists()} == empreintes


def test_aucun_chemin_interne_exposé(client, dryruns):
    """Les écrans ne divulguent aucun chemin de fichier local."""
    token = _previsualiser(dryruns)
    client.post(f"/fournisseurs/nouvelle/confirmer/{token}", follow_redirects=False)
    html = client.get(f"/fournisseurs/nouvelle/resultat/{token}").text
    assert "C:\\Users" not in html
    assert "OneDrive" not in html

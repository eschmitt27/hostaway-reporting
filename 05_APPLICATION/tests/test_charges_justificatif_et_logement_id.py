"""Numérotation attribuée, jamais saisie — recette 4 lot 2 §24-§25, §29-§30.

Deux endroits où l'utilisateur devait inventer un identifiant :
· la référence du justificatif archivé d'une charge → JUS-AAAA-NNNN, attribué par une séquence
  qui ne recycle jamais un numéro consommé ;
· l'identifiant technique d'un logement → LOG_nnnn, à la suite du dernier attribué.
"""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.db.connection import get_db
from app.services import charges_saisie_service as charges
from app.services import logements_creation_service as logements

CHARGE = {"date_charge": "2026-03-15", "montant": 120.0, "categorie_charge_id": "CAT_001",
          "sens_flux": "SORTIE", "code_impact": "HC"}


def _creer(db, **extra):
    donnees = dict(CHARGE, **extra)
    res = charges.creer(donnees, acteur="test", db_path=db)
    assert res["ok"], res
    return charges.lire(res["charge_id"], db_path=db)


# ── §24/§29 — référence du justificatif archivé ─────────────────────────────────────────────────

def test_justificatif_archive_recoit_une_reference_sequentielle(tmp_db):
    premiere = _creer(tmp_db, justificatif_archive="OUI")
    seconde = _creer(tmp_db, justificatif_archive="OUI")
    assert premiere["justificatif_reference"] == "JUS-2026-0001"
    assert seconde["justificatif_reference"] == "JUS-2026-0002"
    assert premiere["justificatif_archive"] == "OUI"


def test_sans_justificatif_archive_aucune_reference(tmp_db):
    sans = _creer(tmp_db)
    non = _creer(tmp_db, justificatif_archive="NON")
    assert sans["justificatif_reference"] is None and sans["justificatif_archive"] is None
    assert non["justificatif_archive"] == "NON" and non["justificatif_reference"] is None


def test_un_numero_consomme_n_est_jamais_recycle(tmp_db):
    """La charge annulée garde son numéro, et la suivante prend le rang d'après."""
    premiere = _creer(tmp_db, justificatif_archive="OUI")
    _creer(tmp_db, justificatif_archive="OUI")
    annulation = charges.annuler(premiere["charge_id"], acteur="test", motif="erreur de saisie",
                                 db_path=tmp_db)
    assert annulation["ok"], annulation

    troisieme = _creer(tmp_db, justificatif_archive="OUI")
    assert troisieme["justificatif_reference"] == "JUS-2026-0003"
    assert charges.lire(premiere["charge_id"], db_path=tmp_db)["justificatif_reference"] == \
        "JUS-2026-0001"


def test_la_reference_ne_change_plus_une_fois_attribuee(tmp_db):
    charge = _creer(tmp_db, justificatif_archive="OUI")
    res = charges.modifier(charge["charge_id"], dict(CHARGE, justificatif_archive="OUI"),
                           acteur="test", motif="correction", db_path=tmp_db)
    assert res["ok"], res
    assert charges.lire(charge["charge_id"], db_path=tmp_db)["justificatif_reference"] == \
        "JUS-2026-0001"


def test_repasser_a_non_archive_efface_la_reference_sans_la_rendre(tmp_db):
    charge = _creer(tmp_db, justificatif_archive="OUI")
    charges.modifier(charge["charge_id"], dict(CHARGE, justificatif_archive="NON"),
                     acteur="test", motif="pièce non classée", db_path=tmp_db)
    assert charges.lire(charge["charge_id"], db_path=tmp_db)["justificatif_reference"] is None
    suivante = _creer(tmp_db, justificatif_archive="OUI")
    assert suivante["justificatif_reference"] == "JUS-2026-0002", "le 0001 reste consommé"


def test_la_serie_suit_l_annee_de_la_charge(tmp_db):
    a = _creer(tmp_db, justificatif_archive="OUI")
    b = _creer(tmp_db, date_charge="2027-01-04", justificatif_archive="OUI")
    assert (a["justificatif_reference"], b["justificatif_reference"]) == \
        ("JUS-2026-0001", "JUS-2027-0001")


def test_deux_attributions_concurrentes_ne_donnent_pas_le_meme_numero(tmp_db):
    """Double clic : la séquence est sérialisée, jamais deux fois le même numéro."""
    conn = get_db(tmp_db)
    try:
        numeros = {charges.attribuer_reference_justificatif(conn, "2026") for _ in range(5)}
        conn.commit()
    finally:
        conn.close()
    assert numeros == {f"JUS-2026-{i:04d}" for i in range(1, 6)}


# ── §25/§30 — identifiant de logement ───────────────────────────────────────────────────────────

FORM_LOGEMENT = {"nom_logement_officiel": "Fictif Nouveau", "nom_court": "NEW",
                 "adresse": "1 rue Test", "ville": "RECETTE", "type_logement_id": "TYPE_001",
                 "proprietaire_id": "PROP_A", "date_debut": "2026-06-01", "actif": "OUI"}


@pytest.fixture
def parc(tmp_db, monkeypatch):
    """Dernier identifiant attribué : LOG_0012."""
    fx.semer(
        tmp_db,
        logements=[{"logement_id": f"LOG_{i:04d}", "nom_logement_officiel": f"Fictif {i}",
                    "nom_court": f"F{i}", "adresse": "", "ville": "RECETTE",
                    "type_logement_id": "TYPE_001", "sur_hostaway": "NON", "actif": "OUI",
                    "statut_parc": "GERE", "commentaire": "",
                    "forfait_logiciel_consommables_mensuel": "0"} for i in (1, 7, 12)],
        gestion=[],
        proprietaires=[{"proprietaire_id": "PROP_A", "nom_proprietaire": "Nom A", "actif": "OUI"}],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
    )
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def test_le_prochain_identifiant_suit_le_dernier_attribue(parc):
    assert logements.prochain_identifiant(db_path=parc) == "LOG_0013"


def test_creation_sans_identifiant_saisi(parc):
    res = logements.creer(dict(FORM_LOGEMENT), acteur="test", db_path=parc)
    assert res["ok"], res
    assert res["logement_id"] == "LOG_0013"


def test_deux_creations_successives_ne_produisent_pas_de_doublon(parc):
    premier = logements.creer(dict(FORM_LOGEMENT), acteur="test", db_path=parc)
    second = logements.creer(dict(FORM_LOGEMENT, nom_court="NEW2"), acteur="test", db_path=parc)
    assert premier["ok"] and second["ok"], (premier, second)
    assert premier["logement_id"] != second["logement_id"]
    assert {premier["logement_id"], second["logement_id"]} == {"LOG_0013", "LOG_0014"}


def test_un_identifiant_libere_n_est_pas_reattribue(parc):
    """Le rang suit le plus GRAND numéro connu : les trous (LOG_0002…) ne se recyclent pas."""
    assert logements.prochain_identifiant(db_path=parc) == "LOG_0013"
    conn = get_db(parc)
    try:
        conn.execute("DELETE FROM ref_logements WHERE logement_id = 'LOG_0007'")
        conn.commit()
    finally:
        conn.close()
    assert logements.prochain_identifiant(db_path=parc) == "LOG_0013"


def test_l_ecran_n_exige_plus_de_saisir_l_identifiant(client, parc):
    page = client.get("/logements/nouveau").text
    assert 'data-testid="prochain-identifiant"' in page and "LOG_0013" in page
    assert 'name="logement_id"' not in page, "le champ n'est plus soumis par l'utilisateur"

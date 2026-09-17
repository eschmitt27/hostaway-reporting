"""E2E B et F — une facture réelle portant les trois catégories, jusqu'à ses effets comptables.

Document utilisé : 08-26-Aissata.pdf, la facture du dossier qui contient réellement les trois cas
de la recette (§9 à §13) — des ménages courants, deux remises en état, un achat de consommables,
et une ligne dont le libellé n'a été compris par personne (« solde dû suite aux prestations du
mois de juillet »).

Chaîne prouvée : extraction → lignes en base → classement → validation impossible tant qu'une
ligne n'est pas classée → validation → écriture ACHATS et dette fournisseur.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import facture_lignes_menage_service as flm
from app.services import facture_menage_pdf_service as pdf_svc
from app.services import factures_service as fact

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("fitz")
pdfex = pytest.importorskip("lib_menages_externes_pdf")

FACTURE_REELLE = cfg.MENAGES_PDF_DIR / "08-26-Aissata.pdf"
pdf_reel = pytest.mark.skipif(not FACTURE_REELLE.exists(),
                              reason="PDF réels absents d'un checkout propre")

_TYPES_CANONIQUES = [
    ("TLM_001", "MENAGE_STANDARD", "OUI"), ("TLM_002", "REMISE_EN_ETAT", "OUI"),
    ("TLM_003", "FRAIS_DEPLACEMENT", "NON"), ("TLM_004", "LINGE", "NON"),
    ("TLM_005", "ACHAT_PRODUIT", "NON"), ("TLM_006", "AUTRE", "NON"),
]


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED", "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_db):
    """Nomenclature des natures (référentiel) + un logement par ligne, pour le rapprochement."""
    fac = pdfex.extraire_pdf(FACTURE_REELLE)
    conn = get_db(tmp_db)
    try:
        for tid, libelle, compte in _TYPES_CANONIQUES:
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        for i, ligne in enumerate(fac.lignes, 1):
            if not ligne.logement_source:
                continue
            conn.execute(
                "INSERT INTO ref_logements (logement_id, nom_court, statut_parc, actif, import_id)"
                " VALUES (?,?,'GERE','OUI','IMP-T')", (f"LOG_E{i:02d}", ligne.logement_source))
            conn.execute(
                "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
                "valeur_source, logement_id, actif, import_id) "
                "VALUES (?,'FACTURE_MENAGE','libelle',?,?,'OUI','IMP-T')",
                (f"MAP-E{i:02d}", ligne.logement_source, f"LOG_E{i:02d}"))
        conn.commit()
    finally:
        conn.close()
    return tmp_db


@pytest.fixture()
def facture_importee(base, tmp_path):
    copie = tmp_path / FACTURE_REELLE.name
    shutil.copy2(FACTURE_REELLE, copie)
    resultat = pdf_svc.importer(copie, db_path=base, acteur="e2e")
    assert resultat["ok"] is True, resultat
    return resultat["facture_id_opaque"]


def _lignes(base, facture_id):
    return fact.lignes(facture_id, db_path=base)


def _controle_humain(base, facture_id):
    """Le geste de l'utilisateur : classer ce que le logiciel n'a pas compris, et désigner les
    logements que le rapprochement n'a pas su proposer. C'est exactement ce que §15 exige avant
    qu'une facture soit validable."""
    for ligne in _lignes(base, facture_id):
        if ligne["type_ligne_menage_confiance"] == "AUCUN":
            flm.corriger_classification(ligne["ligne_id_opaque"], categorie=flm.CAT_AUTRE,
                                        acteur="ewan", db_path=base)
        elif ligne["categorie_compte_menage"] and not ligne["logement_id"]:
            flm.corriger_classification(ligne["ligne_id_opaque"], logement_id="LOG_E01",
                                        acteur="ewan", db_path=base)


# ── E2E B : les trois catégories arrivent en base ───────────────────────────────────────────────

@pdf_reel
def test_toutes_les_lignes_du_document_sont_en_base_avec_leur_nature(base, facture_importee):
    lignes = _lignes(base, facture_importee)
    assert len(lignes) == 15, "les quinze lignes du document, aucune perdue"

    par_categorie: dict[str, int] = {}
    for l in lignes:
        par_categorie[l["categorie"] or "SANS"] = par_categorie.get(l["categorie"] or "SANS", 0) + 1
    assert par_categorie == {"MENAGE_STANDARD": 11, "REMISE_EN_ETAT": 2, "ACHAT_PRODUIT": 1,
                             "AUTRE": 1}, par_categorie

    # La somme des lignes reconstitue le total du document, au centime.
    total = round(sum(l["montant_ttc"] for l in lignes), 2)
    assert total == pytest.approx(2790.0)


@pdf_reel
def test_le_libelle_du_document_est_conserve_pour_chaque_ligne(base, facture_importee):
    lignes = _lignes(base, facture_importee)
    assert all(l["libelle_source"] for l in lignes)
    solde = next(l for l in lignes if "solde" in (l["libelle_source"] or "").lower())
    assert solde["libelle_source"].startswith("15.")
    assert solde["description"] != solde["libelle_source"], \
        "le libellé métier est une forme de travail, pas la reproduction de la pièce"


# ── E2E F : une remise en état compte un ménage, à son coût réel ────────────────────────────────

@pdf_reel
def test_une_remise_en_etat_compte_un_menage_a_son_cout_reel(base, facture_importee):
    remises = [l for l in _lignes(base, facture_importee)
               if l["categorie"] == flm.CAT_REMISE_EN_ETAT]
    assert len(remises) == 2
    # Elle compte comme un ménage…
    assert all(l["categorie_compte_menage"] is True for l in remises)
    # …mais à son montant réel, qui n'a rien d'un tarif standard, et qui varie d'une ligne à l'autre.
    assert sorted(l["montant_ttc"] for l in remises) == [49.0, 150.0]
    assert all(l["montant_ttc"] == l["montant_ttc_source"] for l in remises)


@pdf_reel
def test_une_autre_prestation_ne_compte_pas_comme_un_menage(base, facture_importee):
    courses = next(l for l in _lignes(base, facture_importee)
                   if l["categorie"] == flm.CAT_ACHAT_PRODUIT)
    assert courses["categorie_compte_menage"] is False
    assert courses["montant_ttc"] == pytest.approx(85.0)


# ── E2E C : la ligne incomprise bloque, puis débloque ───────────────────────────────────────────

@pdf_reel
def test_la_ligne_incomprise_bloque_la_validation_puis_la_libere(base, facture_importee):
    inconnue = next(l for l in _lignes(base, facture_importee)
                    if l["type_ligne_menage_confiance"] == "AUCUN")

    refus = fact.changer_statut(facture_importee, fact.ST_VALIDEE, acteur="e2e", db_path=base)
    assert refus["ok"] is False and refus["code"] == fact.E_LIGNE_NON_CLASSEE
    assert "solde" in refus["detail"].lower()

    _controle_humain(base, facture_importee)
    valide = fact.changer_statut(facture_importee, fact.ST_VALIDEE, acteur="e2e", db_path=base)
    assert valide["ok"] is True, valide
    assert inconnue["ligne_id_opaque"]


# ── E2E B (suite) : les effets de la validation ─────────────────────────────────────────────────

@pdf_reel
def test_la_validation_produit_une_seule_depense_ecriture_et_dette(base, facture_importee):
    _controle_humain(base, facture_importee)
    assert fact.changer_statut(facture_importee, fact.ST_VALIDEE, acteur="e2e",
                               db_path=base)["ok"] is True

    consequences = fact.consequences_constatees(facture_importee, db_path=base)
    assert len(consequences["ecritures"]) == 1, "une seule dépense, jamais deux"
    ecriture = consequences["ecritures"][0]
    assert ecriture["journal"] == "ACHATS"

    compta.valider(ecriture["ecriture_id_opaque"], acteur="e2e", db_path=base)
    dette = compta.solde_compte(compta.COMPTE_FOURNISSEURS, db_path=base)
    assert dette["credit"] == pytest.approx(2790.0), "la dette fournisseur est le total du document"

    # Idempotence : revalider ne crée pas une seconde écriture.
    fact.changer_statut(facture_importee, fact.ST_A_CONTROLER, acteur="e2e", db_path=base)
    fact.changer_statut(facture_importee, fact.ST_VALIDEE, acteur="e2e", db_path=base)
    assert len(fact.consequences_constatees(facture_importee, db_path=base)["ecritures"]) == 1

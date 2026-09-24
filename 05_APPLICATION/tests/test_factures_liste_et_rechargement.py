"""Liste des factures fournisseurs et rechargement depuis le dossier — recette 4 lot 2 §5-§16, §27.

CE QUI MANQUAIT
· L'écart entre le total du document et la somme des lignes ne se voyait qu'en ouvrant la fiche.
· Le filtre Fournisseur ne proposait que le référentiel Fournisseurs : les prestataires de ménage
  (qui vivent dans `ref_intervenants`) n'y étaient pas, donc infiltrables.
· Rien ne reliait une facture au PDF dont elle vient, ni ne resynchronisait la liste avec le
  dossier de dépôt : une facture à contrôler survivait au retrait de son PDF.

RÈGLE DE SYNCHRONISATION : le dossier fait foi pour ce qui n'est PAS validé. Une facture validée,
elle, porte une dette et des écritures — elle reste quoi qu'il arrive au fichier.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_lignes_menage_service as flm
from app.services import facture_menage_pdf_service as imp
from app.services import factures_service as fact
from app.services import menages_pdf_import_service as pdf_import

pytest.importorskip("fitz")
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))

SOURCE = cfg.MENAGES_PDF_DIR / "08-26-Aissata.pdf"
AUTRE = cfg.MENAGES_PDF_DIR / "07-26-Mounir.pdf"
reels = pytest.mark.skipif(not (SOURCE.exists() and AUTRE.exists()),
                           reason="PDF réels absents d'un checkout propre")


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_path):
    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        for tid, libelle, compte in (("TLM_001", "MENAGE_STANDARD", "OUI"),
                                     ("TLM_002", "REMISE_EN_ETAT", "OUI"),
                                     ("TLM_005", "ACHAT_PRODUIT", "NON"),
                                     ("TLM_006", "AUTRE", "NON")):
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        conn.commit()
    finally:
        conn.close()
    return db


@pytest.fixture()
def dossier(tmp_path):
    d = tmp_path / "depot"
    d.mkdir()
    return d


# ── §5-§8 : ce que la liste doit dire ───────────────────────────────────────────────────────────

@reels
def test_la_liste_porte_fichier_source_reference_imprimee_mois_et_ecart(base, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    f = fact.lister(db_path=base)[0]
    assert f["fichier_source"] == "08-26-Aissata.pdf"
    assert f["facture_ref_source"] == "2026-41"
    assert f["mois_concerne"] == "2026-08"
    assert f["montant_ttc"] == pytest.approx(2790.0)
    assert f["montant_lignes_ttc"] == pytest.approx(2790.0)
    assert f["ecart_lignes"] == pytest.approx(0.0)


@reels
def test_l_ecart_de_la_liste_est_celui_de_la_fiche_et_du_controle(base, dossier):
    """UNE vérité : liste, fiche et contrôle V11 lisent la même somme canonique."""
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = fact.lister(db_path=base)[0]["facture_id_opaque"]

    ligne = flm.lignes(opaque, db_path=base)[0]
    flm.marquer_extraction_incorrecte(ligne["ligne_id_opaque"], motif="test écart",
                                      acteur="test", db_path=base)

    liste = fact.lister(db_path=base)[0]
    fiche = fact.charger(opaque, db_path=base)
    controle = flm.controler_total(opaque, db_path=base)
    assert liste["montant_lignes_ttc"] == fiche["montant_lignes_ttc"] == controle["montant_lignes"]
    assert liste["ecart_lignes"] == pytest.approx(controle["ecart"])
    assert liste["ecart_lignes"] != 0.0


# ── §9 : les filtres ────────────────────────────────────────────────────────────────────────────

@reels
def test_filtres_fournisseur_mois_et_statut_combinables(base, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    shutil.copy2(AUTRE, dossier / AUTRE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    assert len(fact.lister(db_path=base)) == 2

    aissata = [f for f in fact.lister(db_path=base) if f["fichier_source"] == SOURCE.name][0]
    seul = fact.lister(fournisseur=aissata["fournisseur_id_opaque"], db_path=base)
    assert [f["fichier_source"] for f in seul] == [SOURCE.name]

    assert [f["fichier_source"] for f in fact.lister(mois="2026-08", db_path=base)] == [SOURCE.name]
    assert [f["fichier_source"] for f in fact.lister(mois="2026-07", db_path=base)] == [AUTRE.name]

    combine = fact.lister(fournisseur=aissata["fournisseur_id_opaque"], mois="2026-08",
                          statut=fact.ST_A_CONTROLER, db_path=base)
    assert [f["fichier_source"] for f in combine] == [SOURCE.name]
    assert fact.lister(fournisseur=aissata["fournisseur_id_opaque"], mois="2026-07",
                       db_path=base) == []


@reels
def test_l_ecran_propose_les_fournisseurs_reellement_presents(client, tmp_db, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=tmp_db)
    page = client.get("/factures").text
    assert 'data-testid="filtre-fournisseur"' in page
    assert 'data-testid="filtre-mois"' in page and "Août 2026" in page
    assert 'data-testid="actualiser-factures"' in page     # Mission 3 : bouton unique
    assert "08-26-Aissata.pdf" in page and "2026-41" in page
    assert 'data-testid="menu-actions"' in page


# ── §10-§15, §27 : le dossier fait foi pour ce qui n'est pas validé ─────────────────────────────

@reels
def test_recharger_est_idempotent(base, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    premier = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    second = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    assert premier["nb_importees"] == 1 and second["nb_importees"] == 0
    assert len(fact.lister(db_path=base)) == 1, "un second clic ne recrée pas la facture"


@reels
def test_pdf_retire_du_dossier_la_facture_a_controler_disparait(base, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    shutil.copy2(AUTRE, dossier / AUTRE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = [f for f in fact.lister(db_path=base) if f["fichier_source"] == AUTRE.name][0][
        "facture_id_opaque"]
    assert flm.lignes(opaque, db_path=base), "la facture a bien des lignes provisoires"

    (dossier / AUTRE.name).unlink()
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert res["nb_supprimees"] == 1
    assert [f["fichier_source"] for f in fact.lister(db_path=base)] == [SOURCE.name]
    assert fact.charger(opaque, db_path=base) is None, "aucune facture fantôme"
    assert flm.lignes(opaque, db_path=base) == [], "lignes provisoires supprimées"


@reels
def test_une_facture_validee_survit_a_la_disparition_de_son_pdf(base, dossier):
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    opaque = fact.lister(db_path=base)[0]["facture_id_opaque"]
    # La facture est passée VALIDÉE directement : les portes de validation (logement de chaque
    # ligne, natures à classer) sont éprouvées ailleurs ; ce qui est testé ICI est la règle de
    # rechargement, qui ne dépend que du STATUT.
    conn = get_db(base)
    try:
        conn.execute("UPDATE factures SET statut = ? WHERE facture_id_opaque = ?",
                     (fact.ST_VALIDEE, opaque))
        conn.commit()
    finally:
        conn.close()

    (dossier / SOURCE.name).unlink()
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert res["nb_supprimees"] == 0 and res["nb_conservees"] == 1
    facture = fact.charger(opaque, db_path=base)
    assert facture is not None and facture["statut"] == fact.ST_VALIDEE
    assert flm.lignes(opaque, db_path=base), "ses lignes restent"


@reels
def test_deux_fichiers_identiques_sont_signales_sans_arbitrage(base, dossier):
    shutil.copy2(SOURCE, dossier / "0005-A.pdf")
    shutil.copy2(SOURCE, dossier / "0005-B.pdf")
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)

    assert list(res["doublons"].values()) == [["0005-A.pdf", "0005-B.pdf"]]
    assert (dossier / "0005-A.pdf").exists() and (dossier / "0005-B.pdf").exists(), \
        "le logiciel ne supprime aucun fichier du dossier"
    assert len(fact.lister(db_path=base)) == 1, "un seul document, donc une seule facture"

    # L'utilisateur tranche lui-même, puis recharge : le doublon disparaît.
    (dossier / "0005-B.pdf").unlink()
    apres = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    assert apres["doublons"] == {}
    assert len(fact.lister(db_path=base)) == 1
    assert apres["nb_supprimees"] == 0, "la facture restante garde son fichier"


@reels
def test_le_meme_pdf_redepose_sous_un_autre_nom_reprend_la_facture(base, dossier):
    """Idempotence de PRÉSENCE : renommer le fichier ne doit pas créer une seconde dépense."""
    shutil.copy2(SOURCE, dossier / SOURCE.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    (dossier / SOURCE.name).rename(dossier / "renomme.pdf")
    res = pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    assert len(fact.lister(db_path=base)) == 1, "même document : une seule facture"
    assert res["nb_importees"] == 0


@reels
def test_une_date_aberrante_ne_deplace_pas_le_mois_concerne(base, dossier):
    """Cas réel : une facture de février 2026 dont UNE ligne porte « le 8 février 2028 » (coquille
    du fournisseur). Le mois majoritaire ne l'emporte que s'il porte au moins la moitié des lignes."""
    fevrier = cfg.MENAGES_PDF_DIR / "02-26-Aissata.pdf"
    if not fevrier.exists():
        pytest.skip("PDF de février absent")
    shutil.copy2(fevrier, dossier / fevrier.name)
    pdf_import.recharger(acteur="test", dossier=dossier, db_path=base)
    f = fact.lister(db_path=base)[0]
    assert f["mois_concerne"] == "2026-02", "la date du document fait foi, pas une ligne isolée"


def test_le_mois_concerne_suit_les_prestations_quand_elles_le_disent(base):
    """Facture émise le 1er septembre pour des ménages d'août : le mois concerné est août."""
    res = fact.creer({"fournisseur_id_opaque": "FRS-T", "facture_ref": "R-9",
                      "date_facture": "2026-09-01", "montant_ttc": 100.0}, db_path=base)
    opaque = res["facture_id_opaque"]
    for jour in ("2026-08-03", "2026-08-11"):
        flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A",
                          montant_ttc=50.0, description="ménage", date_menage=jour,
                          source=flm.SOURCE_PDF, db_path=base)
    assert fact.periode_metier(opaque, date_facture="2026-09-01", db_path=base) == "2026-08"

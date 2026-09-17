"""Les huit factures réelles du dossier, figées ligne par ligne — recette utilisateur n°4, §6-§7.

Exigence : TOUTES les lignes réellement présentes dans le PDF doivent avoir une représentation,
même si le logiciel ne comprend pas le libellé, et la somme des lignes doit égaler le total du
document — ou bien le logiciel doit dire explicitement ce qui reste à contrôler.

Avant cette mission, 49 lignes étaient extraites sur 59, et deux factures affichaient un total faux
(89,00 € au lieu de 2 234,00 €, 150,00 € au lieu de 2 790,00 €) parce que le total était pris pour
« le plus grand montant de la page 1 » — or leur pavé récapitulatif est en page 2. Un troisième
prestataire n'était pas lu du tout.

Ces attendus sont figés sur les documents réels : ils échouent si une évolution du parseur reperd
une ligne, réintroduit un écart, ou change une catégorie.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg

pytest.importorskip("fitz")
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pdfex = pytest.importorskip("lib_menages_externes_pdf")

DOSSIER = cfg.MENAGES_PDF_DIR

# fichier : (format, nb lignes, somme des lignes = total du document)
ATTENDUS = {
    "03-26-Aissata.pdf": ("AISSATA", 13, 2234.00),
    "03-26-Imrane.pdf": ("PRIVADOM", 3, 228.00),
    "03-26-Mounir.pdf": ("MOUNIR", 3, 218.00),
    "05-26-Aissata.pdf": ("AISSATA", 8, 1439.00),
    "05-26-Mounir.pdf": ("MOUNIR", 4, 942.00),
    "07-26-Aissata.pdf": ("AISSATA", 8, 1056.00),
    "07-26-Mounir.pdf": ("MOUNIR", 5, 520.00),
    "08-26-Aissata.pdf": ("AISSATA", 15, 2790.00),
}

presents = pytest.mark.skipif(
    not all((DOSSIER / nom).exists() for nom in ATTENDUS),
    reason="PDF réels absents d'un checkout propre")


def _extraire(nom: str):
    return pdfex.extraire_pdf(DOSSIER / nom)


@presents
@pytest.mark.parametrize("nom", sorted(ATTENDUS))
def test_chaque_facture_reelle_est_lue_en_entier_et_reconciliee(nom):
    format_attendu, nb_attendu, total_attendu = ATTENDUS[nom]
    fac = _extraire(nom)

    assert fac.statut_extraction == pdfex.EX_OK, fac.anomalies
    assert fac.format_detecte == format_attendu
    assert len(fac.lignes) == nb_attendu, [l.libelle_source for l in fac.lignes]
    assert fac.montant_total_facture == pytest.approx(total_attendu), \
        "le total doit être lu par son libellé, jamais pris pour le plus grand montant de la page"
    somme = round(sum(l.montant_ligne or 0 for l in fac.lignes), 2)
    assert somme == pytest.approx(total_attendu)
    assert fac.ecart_reconciliation == pytest.approx(0.0)
    assert fac.numero_facture, "une facture sans référence ne peut pas être dédoublonnée"


@presents
def test_aucune_ligne_ne_disparait_sur_l_ensemble_du_dossier():
    """59 lignes réelles dans les huit documents. Le compte total est le garde-fou."""
    total = sum(len(_extraire(nom).lignes) for nom in ATTENDUS)
    assert total == 59


@presents
def test_les_lignes_de_la_page_2_sont_lues():
    """Deux factures tiennent sur deux pages ; trois lignes y vivaient sans être vues."""
    fac = _extraire("08-26-Aissata.pdf")
    page2 = [l for l in fac.lignes if l.source_page == 2]
    assert len(page2) == 3
    libelles = " ".join(l.libelle_source.lower() for l in page2)
    assert "frais de courses" in libelles and "solde" in libelles


@presents
def test_les_remises_en_etat_sont_reconnues_et_comptent_comme_un_menage():
    """Quatre remises en état dans le dossier, toutes invisibles avant cette mission."""
    remises = [(nom, l) for nom in ATTENDUS for l in _extraire(nom).lignes
               if l.categorie == pdfex.CAT_REMISE_EN_ETAT]
    assert len(remises) == 4, [(n, l.libelle_source) for n, l in remises]
    # Leur coût est celui de la ligne, jamais un tarif standard : 89, 89, 49 et 150 €.
    assert sorted(l.montant_ligne for _n, l in remises) == [49.0, 89.0, 89.0, 150.0]
    assert pdfex.CAT_REMISE_EN_ETAT in pdfex.CATEGORIES_COMPTANT_UN_MENAGE


@presents
def test_une_prestation_inconnue_existe_quand_meme_et_attend_un_classement_humain():
    """« solde dû suite aux prestations du mois de juillet » n'est pas un ménage, et le logiciel
    ne prétend pas savoir ce que c'est : la ligne existe, avec son libellé intact."""
    lignes = _extraire("08-26-Aissata.pdf").lignes
    inconnues = [l for l in lignes if l.categorie_confiance == pdfex.CONF_AUCUN]
    assert len(inconnues) == 1
    assert "solde" in inconnues[0].libelle_source.lower()
    assert inconnues[0].categorie == pdfex.CAT_AUTRE
    assert inconnues[0].montant_ligne == pytest.approx(50.0)


@presents
def test_les_frais_ne_sont_pas_comptes_comme_un_menage():
    ligne = next(l for l in _extraire("08-26-Aissata.pdf").lignes
                 if "courses" in l.libelle_source.lower())
    assert ligne.categorie == pdfex.CAT_ACHAT_PRODUIT
    assert ligne.categorie not in pdfex.CATEGORIES_COMPTANT_UN_MENAGE
    assert ligne.quantite == 1 and ligne.quantite_deduite is True, \
        "quantité non écrite : déduite à 1, et dite comme telle"


@presents
def test_le_libelle_source_reproduit_le_document():
    """Le libellé de rapprochement est nettoyé ; celui du document, jamais."""
    ligne = next(l for l in _extraire("03-26-Aissata.pdf").lignes
                 if l.categorie == pdfex.CAT_REMISE_EN_ETAT)
    assert ligne.libelle_source.startswith("6.")
    assert "310 avenue de muret" in ligne.libelle_source.lower()
    assert "26 mars 2026" in ligne.libelle_source
    # Le libellé de rapprochement, lui, ne garde que ce qui désigne le logement.
    assert pdfex.normaliser_libelle(ligne.logement_source) == "t3 310 avenue de muret"
    assert ligne.date_menage == "2026-03-26"


@presents
def test_le_troisieme_prestataire_est_lu():
    """PrivaDom ressortait NON_SUPPORTE : 100 % de la facture était perdue."""
    fac = _extraire("03-26-Imrane.pdf")
    assert fac.format_detecte == "PRIVADOM"
    assert fac.numero_facture == "2025-017"
    assert fac.date_facture == "2026-04-21"
    assert [l.quantite for l in fac.lignes] == [2, 2, 1]
    assert [l.montant_ligne for l in fac.lignes] == [78.0, 110.0, 40.0]
    assert all(l.categorie == pdfex.CAT_MENAGE_STANDARD for l in fac.lignes)


@presents
def test_un_format_vraiment_inconnu_reste_non_supporte():
    """Ajouter un extracteur ne doit pas transformer l'inconnu en extraction devinée."""
    assert pdfex.detecter_format("Facture ACME Corp — prestation diverse") == pdfex.EX_NON_SUPPORTE


def test_les_dates_courtes_sont_lues_avec_l_annee_de_la_facture():
    """Une facture entière écrit ses dates en JJ/MM : ses dix lignes ressortaient sans date."""
    ligne = pdfex.LigneFacture("x", 1, 1.0, 1.0)
    pdfex._dater_ligne(ligne, "service de nettoyage T3 le 30/08", "2026-08")
    assert ligne.date_menage == "2026-08-30" and ligne.precision_date == "DATE_PRECISE"

    multi = pdfex.LigneFacture("x", 1, 1.0, 1.0)
    pdfex._dater_ligne(multi, "x 3 passages (06/08,08/08,10/08)", "2026-08")
    assert multi.precision_date == "MULTI_DATES"

    # Une facture de janvier qui cite un 30/12 parle de l'année précédente.
    bascule = pdfex.LigneFacture("x", 1, 1.0, 1.0)
    pdfex._dater_ligne(bascule, "ménage le 30/12", "2026-01")
    assert bascule.date_menage == "2025-12-30"


def test_les_variantes_reelles_de_remise_en_etat_sont_reconnues():
    """Formulations relevées dans les factures reçues, y compris l'apostrophe typographique."""
    for libelle in ("6. service remise en état le 26 mars 2026 T3 310 avenue de muret",
                    "5. service de remise en l’état T3 4 rue engalières",
                    "11. service de remise en état studio 51 rue d’Alsace Lorraine",
                    "grand nettoyage après départ",
                    "nettoyage exceptionnel T2"):
        assert pdfex.detecter_categorie(libelle)[0] == pdfex.CAT_REMISE_EN_ETAT, libelle

    assert pdfex.detecter_categorie("14. Frais de courses produits consommables")[0] == \
        pdfex.CAT_ACHAT_PRODUIT
    assert pdfex.detecter_categorie("1. service de nettoyage T3 4 rue Bardou")[0] == \
        pdfex.CAT_MENAGE_STANDARD
    # Ce que le logiciel ne comprend pas, il ne le classe pas d'autorité.
    assert pdfex.detecter_categorie("15. solde dû suite aux prestations du mois de juillet") == \
        (pdfex.CAT_AUTRE, pdfex.CONF_AUCUN)

"""Golden tests — les quinze factures fournisseur réelles, relues ligne par ligne sur le document.

LA RÉFÉRENCE N'EST PAS LE PARSEUR. Chaque attendu ci-dessous a été lu sur le RENDU VISUEL de la
page (ce qu'un humain voit), puis comparé à l'extraction. Si le parseur change de lecture, c'est lui
qui a tort jusqu'à preuve du contraire : ces attendus ne se « mettent pas à jour » pour faire passer
un test.

Familles : M = ménage, R = remise en état, A = autre prestation reconnue (frais, achats,
déplacement…), X = prestation non comprise, à classer par un humain (jamais supprimée).
Chaque ligne : (numéro ou None, quantité, prix unitaire, montant, famille, extrait du libellé).
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

M, R, A, X = "M", "R", "A", "X"

GOLDEN = {
    "02-26-Aissata.pdf": {
        "numero": "2025-370", "date": "2026-02-28", "total": 1702.00, "pages": 1, "lignes": [
            (1, 6, 55, 330, M, "T3 4 rue Bardou"),
            (2, 1, 39, 39, M, "65 rue Lucien Cassagne"),
            (3, 3, 29, 87, M, "6 impasse Duroux"),
            (4, 6, 55, 330, M, "4 rue Engali"),
            (5, 1, 15, 15, A, "Frais d"),
            (6, 5, 69, 345, M, "T4 blagnac"),
            (7, 8, 55, 440, M, "310 avenue de Muret"),
            (8, 1, 29, 29, M, "76 all"),
            (9, 1, 29, 29, M, "14 Place Saint Pierre"),
            (10, 2, 29, 58, M, "4 rue du Puits Verts"),
        ]},
    "02-26-Imrane.pdf": {
        # Le document IMPRIME « 2 × 30 € = 30 € » alors que son sous-total (205 €) compte 60 € :
        # le montant imprimé est conservé tel quel, l'écart source (−30 €) reste visible, et le
        # document — pas le parseur — porte l'anomalie.
        "numero": "2025-016", "date": "2026-03-02", "total": 205.00, "pages": 1,
        "somme_source": 175.00, "lignes": [
            (None, 5, 29, 145, M, "T1/ 1lits"),
            (None, 2, 30, 30, M, "T1/2 lits"),
        ]},
    "03-26-Aissata.pdf": {
        "numero": "2025-369", "date": "2026-03-31", "total": 2234.00, "pages": 2, "lignes": [
            (1, 5, 55, 275, M, "4 rue Bardou"),
            (2, 4, 29, 116, M, "76 all"),
            (3, 5, 39, 195, M, "65 rue Lucien Cassagne"),
            (4, 4, 29, 116, M, "6 impasse Duroux"),
            (5, 1, 39, 39, M, "amiral Galache"),
            (6, 1, 89, 89, R, "310 avenue de muret"),
            (7, 7, 69, 483, M, "T4 blagnac"),
            (8, 7, 29, 203, M, "puits vert"),
            (9, 2, 55, 110, M, "18 rue de Cugnaux"),
            (10, 7, 55, 385, M, "310 avenue de Muret"),     # libellé : « x 6 passages »
            (11, 1, 29, 29, M, "Charles de Fitte"),
            (12, 4, 29, 116, M, "14 place Saint Pierre"),
            (13, 2, 39, 78, M, "9 rue du Toul"),
        ]},
    "03-26-Imrane.pdf": {
        "numero": "2025-017", "date": "2026-04-21", "total": 228.00, "pages": 1, "lignes": [
            (None, 2, 39, 78, M, "T2"),
            (None, 2, 55, 110, M, "T3"),
            (None, 1, 40, 40, M, "T2/2 lits"),
        ]},
    "03-26-Mounir.pdf": {
        "numero": "0001", "date": "2026-03-31", "total": 218.00, "pages": 1, "lignes": [
            (None, 2, 65, 130, M, "T.4"),
            (None, 1, 52, 52, M, "T.3"),
            (None, 1, 36, 36, M, "T.2"),
        ]},
    "04-26-Aissata_1.pdf": {
        "numero": "2026-36", "date": "2026-04-30", "total": 2313.00, "pages": 1, "lignes": [
            (1, 11, 55, 605, M, "sept deniers"),
            (2, 5, 29, 145, M, "studio 76 (Dureuil)"),
            (3, 4, 39, 156, M, "T2 65 (Gabriel)"),
            (4, 8, 29, 232, M, "cote pav"),
            (5, 1, 69, 69, M, "90 blagnac"),
            (6, 6, 29, 174, M, "Puits verts"),
            (7, 1, 55, 55, M, "18 rue de Cugnaux"),
            (8, 8, 55, 440, M, "310 muret"),
            (9, 7, 29, 203, M, "st Pierre"),
            (10, 6, 39, 234, M, "9 rue du Toul"),
        ]},
    "04-26-Aissata_2.pdf": {
        "numero": "2026-37", "date": "2026-04-30", "total": 15.00, "pages": 1, "lignes": [
            (1, 1, 15, 15, A, "debarras matelas"),
        ]},
    "04-26-Mounir.pdf": {
        "numero": "0002", "date": "2026-04-30", "total": 491.00, "pages": 1, "lignes": [
            (None, 7, 65, 455, M, "T.4-90 Blagnac"),
            (None, 0, 52, 0, M, "T.3"),
            (None, 1, 36, 36, M, "T.2-65"),
        ]},
    "05-26-Aissata.pdf": {
        "numero": "2026-37", "date": "2026-05-31", "total": 1439.00, "pages": 1, "lignes": [
            (1, 1, 29, 29, M, "studio 76"),
            (2, 5, 29, 145, M, "cote pav"),
            (3, 10, 29, 290, M, "Puits verts"),
            (4, 8, 55, 440, M, "310 muret"),
            (5, 2, 29, 58, M, "st Pierre"),
            (6, 8, 39, 312, M, "9 rue du Toul"),
            (7, 1, 55, 55, M, "4 rue engali"),
            (8, 2, 55, 110, M, "Amiral Galache"),
        ]},
    "05-26-Mounir.pdf": {
        "numero": "0003", "date": "2026-05-31", "total": 942.00, "pages": 1, "lignes": [
            (None, 6, 65, 390, M, "T.4-90 Blagnac"),
            (None, 10, 52, 520, M, "Sept Deniers"),
            (None, 0, 36, 0, M, "T.2-65"),
            (None, 1, 32, 32, M, "Puits vert"),
        ]},
    "06-26-Aissata.pdf": {
        "numero": "2026-38", "date": "2026-06-30", "total": 1016.00, "pages": 1, "lignes": [
            (1, 1, 29, 29, M, "studio 76"),
            (2, 6, 29, 174, M, "cote pav"),
            (3, 4, 55, 220, M, "310 muret"),
            (4, 2, 7.5, 15, A, "Incident"),
            (5, 9, 29, 261, M, "Puits verts"),
            (6, 2, 29, 58, M, "st Pierre"),
            (7, 1, 39, 39, M, "9 rue du Toul"),
            (8, 4, 55, 220, M, "Amiral Galache"),
        ]},
    "06-26-Mounir.pdf": {
        "numero": "0004", "date": "2026-06-30", "total": 741.00, "pages": 1, "lignes": [
            (None, 5, 65, 325, M, "T.4-90 Blagnac"),
            (None, 8, 52, 416, M, "Sept Deniers"),
            (None, 0, 36, 0, M, "T.2-65"),
            (None, 0, 32, 0, M, "Puits vert"),
        ]},
    "07-26-Aissata.pdf": {
        "numero": "2026-40", "date": "2026-07-31", "total": 1056.00, "pages": 1, "lignes": [
            (1, 3, 29, 87, M, "cote pav"),
            (2, 7, 55, 385, M, "310 muret"),
            (3, 8, 29, 232, M, "Puits verts"),
            (4, 1, 29, 29, M, "st Pierre"),
            (5, 1, 89, 89, R, "4 rue engali"),
            (6, 1, 55, 55, M, "18 rue de Cugnaux"),
            (7, 1, 69, 69, M, "90 Blagnac"),
            (8, 2, 55, 110, M, "sept deniers"),
        ]},
    "07-26-Mounir.pdf": {
        "numero": "0005", "date": "2026-07-27", "total": 520.00, "pages": 1, "lignes": [
            (None, 4, 65, 260, M, "T.4-90 Blagnac"),
            (None, 1, 52, 52, M, "310 Muret"),
            (None, 4, 52, 208, M, "Sept Deniers"),
            (None, 0, 36, 0, M, "T.2-65"),
            (None, 0, 32, 0, M, "Puits vert"),
        ]},
    "08-26-Aissata.pdf": {
        "numero": "2026-41", "date": "2026-08-31", "total": 2790.00, "pages": 2, "lignes": [
            (1, 8, 29, 232, M, "studio - cote pav"),
            (2, 9, 55, 495, M, "T3 310 muret (David)"),
            (3, 5, 29, 145, M, "studio Puits verts (Caroline)"),
            (4, 4, 55, 220, M, "T3 4 rue engali"),
            (5, 1, 55, 55, M, "T3 18 rue de cugnaux"),
            (6, 2, 55, 110, M, "T3 20 rue de l"),
            (7, 8, 69, 552, M, "T4 90 Blagnac"),
            (8, 10, 55, 550, M, "T3 sept deniers"),
            (9, 1, 29, 29, M, "studio 46 all"),
            (10, 1, 29, 29, M, "studio 51 rue d"),
            (11, 1, 49, 49, R, "studio 51 rue d"),
            (12, 1, 150, 150, R, "T2 9 rue de Toul"),
            (13, 1, 39, 39, M, "T2 9 rue de Toul"),
            (14, 1, 85, 85, A, "Frais de courses produits consommables"),
            (15, 1, 50, 50, X, "solde d"),
        ]},
}

FAMILLE = {
    pdfex.CAT_MENAGE_STANDARD: M, pdfex.CAT_REMISE_EN_ETAT: R, pdfex.CAT_FRAIS_DEPLACEMENT: A,
    pdfex.CAT_ACHAT_PRODUIT: A, pdfex.CAT_LINGE: A,
}


def _famille(ligne) -> str:
    if ligne.categorie_confiance == pdfex.CONF_AUCUN:
        return X
    return FAMILLE.get(ligne.categorie, A)


presents = pytest.mark.skipif(not all((DOSSIER / n).exists() for n in GOLDEN),
                              reason="PDF réels absents d'un checkout propre")

_cache: dict[str, object] = {}


def _extraire(nom: str):
    if nom not in _cache:
        _cache[nom] = pdfex.extraire_pdf(DOSSIER / nom)
    return _cache[nom]


@presents
@pytest.mark.parametrize("nom", sorted(GOLDEN))
def test_facture_reelle_conforme_au_document(nom):
    attendu = GOLDEN[nom]
    fac = _extraire(nom)
    assert fac.statut_extraction == pdfex.EX_OK, fac.anomalies
    assert fac.numero_facture == attendu["numero"]
    assert fac.date_facture == attendu["date"]
    assert fac.source_pages == attendu["pages"]
    assert fac.montant_total_facture == pytest.approx(attendu["total"])

    lus = [(l.numero_ligne, l.quantite, l.prix_unitaire, l.montant_ligne, _famille(l))
           for l in fac.lignes]
    voulus = [(n, q, float(pu), float(mt), fam) for n, q, pu, mt, fam, _ in attendu["lignes"]]
    assert lus == voulus
    for ligne, (*_, extrait) in zip(fac.lignes, attendu["lignes"]):
        assert extrait.lower() in ligne.libelle_source.lower(), (extrait, ligne.libelle_source)

    somme_source = attendu.get("somme_source", attendu["total"])
    assert fac.somme_lignes == pytest.approx(somme_source)
    assert fac.ecart_reconciliation == pytest.approx(somme_source - attendu["total"])
    assert fac.anomalies_parseur == [], "chaque document est lu fidèlement"


@presents
@pytest.mark.parametrize("nom", sorted(GOLDEN))
def test_le_libelle_ne_contient_jamais_la_colonne_des_prix(nom):
    """La colonne des prix est lue à part : « 29,00 € x 8 » ne se mêle plus au libellé."""
    for l in _extraire(nom).lignes:
        assert "€" not in l.libelle_source, l.libelle_source
        assert "€" not in l.logement_source, l.logement_source


@presents
def test_gold_aissata_aout_quinze_lignes_2790_euros_ecart_nul():
    """Critère de fin du chantier, tel qu'écrit dans la mission."""
    fac = _extraire("08-26-Aissata.pdf")
    assert len(fac.lignes) == 15
    assert fac.somme_lignes == pytest.approx(2790.0)
    assert fac.ecart_reconciliation == pytest.approx(0.0)
    assert fac.net_a_payer == pytest.approx(2790.0)
    assert fac.total_ttc == pytest.approx(2790.0)
    assert fac.base_ht == pytest.approx(2790.0)
    l = {x.numero_ligne: x for x in fac.lignes}
    assert (l[11].categorie, l[11].montant_ligne) == (pdfex.CAT_REMISE_EN_ETAT, 49.0)
    assert (l[12].categorie, l[12].montant_ligne) == (pdfex.CAT_REMISE_EN_ETAT, 150.0)
    assert (l[13].categorie, l[13].montant_ligne) == (pdfex.CAT_MENAGE_STANDARD, 39.0)
    assert l[14].montant_ligne == 85.0 and l[14].categorie not in pdfex.CATEGORIES_COMPTANT_UN_MENAGE
    assert l[15].montant_ligne == 50.0 and l[15].categorie_confiance == pdfex.CONF_AUCUN
    # Page 2 : lignes 13, 14, 15 — 174 €.
    page2 = [x for x in fac.lignes if x.source_page == 2]
    assert [x.numero_ligne for x in page2] == [13, 14, 15]
    assert sum(x.montant_ligne for x in page2) == pytest.approx(174.0)


@presents
def test_le_libelle_de_rapprochement_est_propre():
    """Plus de résidus de dates ou de quantités : ils brouillaient la reconnaissance du logement."""
    libelles = {l.numero_ligne: l.logement_source for l in _extraire("08-26-Aissata.pdf").lignes}
    assert libelles[7] == "T4 90 Blagnac (Cédrine)"
    assert libelles[8] == "T3 sept deniers (François) (4 rue Bardou)"
    assert libelles[1] == "studio - cote pavé (François) (6 impasse Duroux)"
    assert libelles[2] == "T3 310 muret (David)"
    assert libelles[15] == "solde dû suite aux prestations du mois de juillet"
    avril = {l.numero_ligne: l.logement_source for l in _extraire("04-26-Aissata_1.pdf").lignes}
    assert avril[5] == "T4 90 blagnac (Cédrine)", "« le 8 avril et 17 avril » entièrement retiré"
    juillet = {l.numero_ligne: l.logement_source for l in _extraire("07-26-Aissata.pdf").lignes}
    assert juillet[8] == "T3 sept deniers (François) (4 rue Bardou)"


@presents
def test_une_quantite_de_libelle_contredite_par_la_facturation_est_signalee():
    """03-26, ligne 10 : « x 6 passages » au libellé, « 55,00 € x 7 » facturé. Le facturé fait foi
    (le total du document le confirme) ; la divergence est dite, pas effacée."""
    l10 = next(l for l in _extraire("03-26-Aissata.pdf").lignes if l.numero_ligne == 10)
    assert (l10.quantite, l10.quantite_libelle, l10.montant_ligne) == (7, 6, 385.0)
    assert "QUANTITE_LIBELLE_DIFFERENTE" in l10.code_anomalie


@presents
def test_un_montant_imprime_faux_n_est_jamais_ecrase():
    """Le calcul (60 €) est une SUGGESTION ; le montant imprimé (30 €) reste la donnée source."""
    fac = _extraire("02-26-Imrane.pdf")
    ligne = fac.lignes[1]
    assert (ligne.montant_ligne, ligne.montant_calcule, ligne.ecart_arithmetique) == (30.0, 60.0, 30.0)
    assert fac.sous_total == pytest.approx(205.0)
    assert (fac.somme_lignes, fac.somme_theorique, fac.correction_suggeree) == (175.0, 205.0, 30.0)
    assert any(a.startswith("INCOHERENCE_ARITHMETIQUE_DOCUMENT") for a in fac.anomalies_document)


@presents
def test_corpus_complet_aucune_ligne_perdue_aucun_ecart():
    total_lignes = 0
    incoherents = []
    for nom, attendu in GOLDEN.items():
        fac = _extraire(nom)
        total_lignes += len(fac.lignes)
        assert fac.anomalies_parseur == [], nom
        if fac.anomalies_document:
            incoherents.append(nom)
        else:
            assert fac.ecart_reconciliation == pytest.approx(0.0), nom
    assert incoherents == ["02-26-Imrane.pdf"], "un seul document mathématiquement faux"
    assert total_lignes == sum(len(a["lignes"]) for a in GOLDEN.values()) == 97

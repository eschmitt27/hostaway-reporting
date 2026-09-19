"""Reconstruction géométrique des factures — cas fabriqués, indépendants de tout fournisseur réel.

Les factures réelles sont couvertes par `test_factures_menage_corpus_golden`. Ici, des PDF
construits pour isoler UNE difficulté chacun : prix au-dessus de leur libellé, libellé sur deux
lignes, facture sur deux pages, tableau sans numérotation, montant sans libellé. L'identité du
fournisseur (sa raison sociale) est la seule chose empruntée au réel, parce que c'est elle qui
autorise l'import ; la MISE EN PAGE, elle, n'a rien de commun avec ses vraies factures — c'est ce
qui prouve que la lecture ne dépend pas du fournisseur.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

import app.config as cfg

fitz = pytest.importorskip("fitz")
_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pdfex = pytest.importorskip("lib_menages_externes_pdf")
geo = pytest.importorskip("lib_factures_geometrie")

IDENTITE = "Rends-moi un service"


def _pdf(tmp_path, pages: list[list[tuple[float, float, str]]], nom="f.pdf") -> Path:
    doc = fitz.open()
    for textes in pages:
        page = doc.new_page(width=595, height=842)
        for x, y, t in textes:
            page.insert_text((x, y), t, fontsize=10)
    p = tmp_path / nom
    doc.save(p)
    doc.close()
    return p


def _entete(numero="2099-1", date="31 mai 2026"):
    return [(60, 60, IDENTITE), (380, 100, f"Facture n°{numero}"),
            (60, 140, f"Date de la facture : {date}"),
            (60, 200, "Description"), (440, 200, "Montant HT")]


def test_prix_rendus_au_dessus_de_leur_libelle(tmp_path):
    """Chaque prix est 9 points AU-DESSUS de sa ligne, et les lignes sont serrées : « le plus
    proche » attribuait le prix n à la ligne n-1. L'alignement ordonné ne peut pas se tromper."""
    t = _entete()
    y = 240
    for i, (lib, prix) in enumerate([("service de nettoyage T3 4 rue A x 2 passages", "55,00 € x 2"),
                                     ("service de nettoyage studio 6 rue B x 1 passage", "29,00 € x 1"),
                                     ("service de remise en état T2 9 rue C", "150,00 €")], 1):
        t += [(20, y, f"{i}. {lib}"), (440, y - 9, prix)]
        y += 26
    t += [(40, 400, "Base HT"), (200, 400, "Total TTC"), (440, 400, "NET A PAYER"),
          (40, 414, "289,00"), (200, 414, "289,00"), (440, 414, "289,00 €")]
    fac = pdfex.extraire_pdf(_pdf(tmp_path, [t]))
    assert [(l.numero_ligne, l.quantite, l.montant_ligne) for l in fac.lignes] == \
        [(1, 2, 110.0), (2, 1, 29.0), (3, 1, 150.0)]
    assert fac.lignes[2].categorie == pdfex.CAT_REMISE_EN_ETAT
    assert fac.ecart_reconciliation == 0.0


def test_libelle_sur_deux_lignes_et_facture_sur_deux_pages(tmp_path):
    p1 = _entete() + [
        (20, 240, "1. service de nettoyage T4 90 Blagnac (Cédrine) x 3"), (440, 245, "69,00 € x 3"),
        (20, 254, "passages le 03/08,07/08,13/08"),
        (20, 290, "2. service de nettoyage T3 20 rue X x 1 passage"), (440, 290, "55,00 € x 1"),
    ]
    p2 = [(20, 40, "3. Frais de courses produits consommables"), (440, 40, "12,50 €"),
          (20, 80, "4. régularisation diverse"), (440, 80, "10,00 €"),
          (40, 140, "Base HT"), (440, 140, "NET A PAYER"), (40, 154, "284,50"), (440, 154, "284,50 €")]
    fac = pdfex.extraire_pdf(_pdf(tmp_path, [p1, p2]))
    assert len(fac.lignes) == 4
    assert [l.source_page for l in fac.lignes] == [1, 1, 2, 2]
    assert fac.lignes[0].logement_source == "T4 90 Blagnac (Cédrine)"
    assert fac.lignes[0].precision_date == "MULTI_DATES"
    assert fac.lignes[3].categorie_confiance == pdfex.CONF_AUCUN, "non comprise : à classer"
    assert fac.net_a_payer == 284.5 and fac.ecart_reconciliation == 0.0


def test_tableau_sans_numerotation_avec_colonnes_quantite_prix_total(tmp_path):
    """Mise en page « tableau » : QUANTITE | DESCRIPTION | PRIX UNITAIRE | TOTAL."""
    t = [(60, 60, IDENTITE), (380, 100, "Facture n°2099-7"), (60, 140, "Date de la facture : 30 juin 2026"),
         (40, 200, "QUANTITE"), (160, 200, "DESCRIPTION"), (330, 200, "PRIX UNITAIRE"),
         (450, 200, "TOTAL DE LA LIGNE"),
         (40, 230, "3"), (160, 230, "Nettoyage T2 rue A"), (350, 230, "39,00"), (470, 230, "117,00"),
         (40, 256, "1"), (160, 256, "Livraison de linge"), (350, 256, "20,00"), (470, 256, "20,00"),
         (400, 300, "Sous-total"), (500, 300, "137,00"), (400, 316, "Total TTC"), (500, 316, "137,00")]
    fac = pdfex.extraire_pdf(_pdf(tmp_path, [t]))
    assert fac.mode_lecture == "LIBELLES"
    assert [(l.quantite, l.prix_unitaire, l.montant_ligne) for l in fac.lignes] == \
        [(3, 39.0, 117.0), (1, 20.0, 20.0)]
    assert fac.lignes[1].categorie == pdfex.CAT_LINGE
    assert fac.total_ttc == 137.0 and fac.ecart_reconciliation == 0.0


def test_un_montant_sans_libelle_devient_une_ligne_a_classer(tmp_path):
    """Jamais perdre de l'argent : un prix que rien ne réclame reste une ligne, visible."""
    t = _entete() + [
        (20, 240, "1. service de nettoyage T3 4 rue A"), (440, 240, "55,00 €"),
        (440, 330, "40,00 €"),
        (40, 400, "Total TTC"), (40, 414, "95,00"),
    ]
    fac = pdfex.extraire_pdf(_pdf(tmp_path, [t]))
    assert len(fac.lignes) == 2
    assert "MONTANT_SANS_LIBELLE" in fac.lignes[1].code_anomalie
    assert fac.lignes[1].montant_ligne == 40.0
    assert fac.ecart_reconciliation == 0.0


def test_une_ligne_sans_prix_existe_quand_meme(tmp_path):
    t = _entete() + [
        (20, 240, "1. service de nettoyage T3 4 rue A"), (440, 240, "55,00 €"),
        (20, 300, "2. service de nettoyage studio 6 rue B"),
        (40, 400, "Total TTC"), (40, 414, "84,00"),
    ]
    fac = pdfex.extraire_pdf(_pdf(tmp_path, [t]))
    assert len(fac.lignes) == 2
    assert fac.lignes[1].montant_ligne is None and "MONTANT_ABSENT" in fac.lignes[1].code_anomalie
    assert fac.ecart_reconciliation == -29.0, "l'écart reste visible, rien n'est inventé"


def test_les_totaux_sont_lus_par_leur_libelle_jamais_par_un_maximum(tmp_path):
    lignes = geo.grouper_lignes(geo.lire_mots(fitz.open(_pdf(tmp_path, [[
        (40, 100, "Base HT"), (130, 100, "%TVA"), (330, 100, "Total TTC"), (450, 100, "NET A PAYER"),
        (40, 114, "2 790"), (130, 114, "0"), (330, 114, "2 790,00"), (450, 114, "2 790,00 €"),
        (40, 200, "Acompte déjà versé 9 999,00 €")]]))))
    assert geo.lire_recapitulatif(lignes) == {"base_ht": 2790.0, "total_ttc": 2790.0,
                                              "net_a_payer": 2790.0}


def test_aucune_regle_de_lecture_ne_depend_du_fournisseur():
    """Garde : le nom d'un fournisseur n'apparaît que dans la table d'IDENTITÉ, jamais dans une
    condition de lecture (pas de « if fournisseur == … »), et le module géométrique n'en connaît
    aucun."""
    source = Path(pdfex.__file__).read_text(encoding="utf-8")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert not re.search(r"(==|!=)\s*[\"'](AISSATA|MOUNIR|PRIVADOM)[\"']", code)
    assert not re.search(r"numero_facture\s*==", code)
    geo_src = Path(geo.__file__).read_text(encoding="utf-8").lower()
    for nom in ("aissata", "mounir", "privadom", "rends-moi", "mh entreprise", "imrane"):
        assert nom not in geo_src

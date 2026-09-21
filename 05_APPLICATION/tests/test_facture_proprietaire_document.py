"""§39-50 — ce que le propriétaire reçoit réellement entre les mains.

Une facture propriétaire est le SEUL document que l'application envoie à un tiers. Tout ce qui y
figure l'engage, et tout ce qui s'y lit mal lui revient en question. Ces tests portent sur les
octets du PDF, pas sur les services qui le préparent : c'est le document qui compte.

CE QUE LES PREMIERS RENDUS RÉELS ONT MONTRÉ, ET QUE CES TESTS EMPÊCHENT DE REVENIR
  · « 465,88 EUR » au lieu de « 465,88 € », et les tirets et apostrophes typographiques aplatis —
    non par contrainte du format PDF, mais parce que l'encodage retenu (latin-1) était plus étroit
    que l'encodage standard des polices de base (WinAnsiEncoding / cp1252).
  · « Logement : LOG_0001 » : un identifiant interne, sorti de l'application vers un tiers.
  · « Arrivee », « Depart », « Detail des frais », « NET A PAYER » : des libellés sans accents,
    séquelles de la même limitation d'encodage.
  · La référence de réservation Hostaway (« 63200085 »), qui ne dit rien au propriétaire.
  · Une page 2 ne portant que quatre lignes de « Règlement » et le pied de page.
"""
from __future__ import annotations

import pytest

from app.services import factures_proprietaires_pdf as pdfsvc

ENC = pdfsvc.ENCODAGE_POLICES_BASE


def _snapshot(**extra):
    """Snapshot minimal mais réaliste — même forme que celui figé à l'émission."""
    base = {
        "numero_facture": "F-TEST/0-000001",
        "type_document": "FACTURE",
        "date_facture": "2026-09-11",
        "mois": "2026-08",
        "devise": "EUR",
        "proprietaire_id": "PROP_0001",
        "logement_id": "LOG_0001",
        "montant_total": 465.88, "total_ht": 465.88, "total_ttc": 465.88, "total_tva": 0.0,
        "emetteur": {"denomination": "CHOUETTE PATRIMOINE", "forme_juridique": "SAS",
                     "capital": "200,00 €", "adresse_siege": "48E Route de Larnavey, 33650 Saint-Selve",
                     "siren": "109624767", "rcs": "R.C.S. Bordeaux"},
        "destinataire": {"nom": "Didier UZON", "adresse": "46 allées Charles de Fitte"},
        "conformite": {
            "client": {"denomination": "Didier UZON", "adresse": "46 allées Charles de Fitte",
                       "type_client": "PARTICULIER"},
            "logements": ["Studio - 46"],
            "representants": "Wafa Souci et Ewan Schmitt",
            "nature_operation": "PRESTATION_DE_SERVICES",
            "conditions_paiement": "Paiement à réception",
            "mention_tva": "TVA non applicable, art. 293 B du CGI",
        },
        "reservations": [
            {"reservation_id": "63200085", "check_in": "2026-08-02", "check_out": "2026-08-06",
             "nights": 4, "guest_count": 2, "plateforme": "AIRBNB", "payout": 171.75,
             "assiette_commission": 142.75, "taux_commission": 0.18, "commission": 25.70},
        ],
        "lignes": [
            {"numero_ligne": 1, "libelle": "Commission de conciergerie",
             "type_ligne": "COMMISSION_CONCIERGERIE", "montant": 169.88},
            {"numero_ligne": 2, "libelle": "Prestations de ménage",
             "type_ligne": "MENAGE_FACTURE", "montant": 261.00},
            {"numero_ligne": 3, "libelle": "Charge fixe mensuelle",
             "type_ligne": "CHARGE_FIXE", "montant": 35.00},
        ],
        "decomposition": {
            "montant_du": 465.88, "devise": "EUR", "acomptes": [],
            "postes": [
                {"cle": "commissions", "libelle": "Commissions de conciergerie",
                 "montant": 169.88, "nb": 1},
                {"cle": "menages", "libelle": "Prestations de ménage", "montant": 261.0, "nb": 1},
                {"cle": "forfait", "libelle": "Forfait logiciel et consommables",
                 "montant": 35.0, "nb": 1},
            ],
        },
    }
    base.update(extra)
    return base


@pytest.fixture
def octets():
    return pdfsvc.rendre(_snapshot())


@pytest.fixture
def texte(octets):
    pymupdf = pytest.importorskip("pymupdf")
    import io
    doc = pymupdf.open(stream=io.BytesIO(octets), filetype="pdf")
    return "\n".join(p.get_text() for p in doc), doc.page_count


# ── §49 — le symbole € et la typographie française ──────────────────────────────────────────────

def test_le_symbole_euro_est_imprime(octets):
    """Aucune police téléchargée : cp1252 (WinAnsiEncoding) contient « € ».

    Le montant attendu est demandé au formateur plutôt que réécrit à la main : il pose une espace
    INSÉCABLE avant le symbole (usage français), et une espace ordinaire dans le test ferait
    échouer une mise en forme pourtant correcte.
    """
    assert pdfsvc._montant(465.88).endswith("€")
    assert pdfsvc._montant(465.88).encode(ENC) in octets
    assert pdfsvc._montant(169.88).encode(ENC) in octets


def test_aucun_montant_libelle_en_eur(texte):
    contenu, _ = texte
    assert "EUR" not in contenu.replace("ÉMETTEUR", ""), \
        "les montants portent le symbole, pas le code devise"


def test_la_typographie_francaise_est_conservee(octets):
    """Tiret cadratin et apostrophe typographique existent en cp1252 : rien à aplatir."""
    assert "—".encode(ENC) in octets


@pytest.mark.parametrize("libelle", [
    "Arrivée", "Départ", "Détail des frais", "Désignation", "NET À PAYER",
    "Conciergerie de location courte durée",
])
def test_les_libelles_portent_leurs_accents(texte, libelle):
    """La propriété vérifiée est l'ACCENT, pas la casse.

    Les titres de section et le sous-titre de marque se composent en capitales depuis la refonte
    de présentation : « DÉTAIL DES FRAIS » plutôt que « Détail des frais ». Comparer sans tenir
    compte de la casse garde intact ce que ce test protège — qu'aucun « é » ne soit aplati en
    « e » par l'encodage — tout en laissant la mise en page évoluer. Les capitales accentuées
    sont d'ailleurs le cas le plus exposé : É, È et À doivent survivre au cp1252 comme les
    minuscules.
    """
    contenu, _ = texte
    assert libelle.upper() in contenu.upper()
    assert any(c in contenu for c in "ÉÈÀÊÔÇéèàêôç"), "l'encodage aplatit les accents"


def test_aucun_caractere_de_remplacement(texte):
    contenu, _ = texte
    assert "?" not in contenu.replace("N°", ""), "aucun caractère non encodable ne doit subsister"


# ── §43 — ce qui ne doit PAS figurer sur un document remis à un tiers ───────────────────────────

def test_aucun_identifiant_technique(texte):
    """« LOG_0001 » n'apprend rien au propriétaire et fait sortir un code interne."""
    contenu, _ = texte
    for motif in ("LOG_0001", "PROP_0001", "FPR-", "MTP-"):
        assert motif not in contenu, f"{motif} ne doit pas figurer sur la facture"
    assert "Logement : Studio - 46" in contenu, "le logement est désigné par son NOM"


def test_la_reference_de_reservation_hostaway_est_absente(texte):
    contenu, _ = texte
    assert "63200085" not in contenu


def test_le_nombre_de_voyageurs_remplace_la_reference(texte):
    contenu, _ = texte
    assert "Voyageurs" in contenu
    assert "Référence" not in contenu.split("Détail des frais")[0], \
        "la table des séjours n'a plus de colonne Référence"


def test_aucune_donnee_personnelle_du_voyageur(texte):
    """Le nom du voyageur est dans le snapshot mais jamais imprimé : la facture doit circuler."""
    octets = pdfsvc.rendre(_snapshot(reservations=[
        {"reservation_id": "1", "check_in": "2026-08-02", "check_out": "2026-08-06", "nights": 4,
         "guest_count": 2, "guest_name": "Jean DUPONT", "plateforme": "AIRBNB",
         "assiette_commission": 142.75, "taux_commission": 0.18, "commission": 25.70}]))
    assert "Jean DUPONT".encode(ENC) not in octets


# ── §45 — l'identité de l'émetteur ──────────────────────────────────────────────────────────────

def test_les_representants_sont_imprimes(texte):
    contenu, _ = texte
    assert "Représentée par Wafa Souci et Ewan Schmitt" in contenu


def test_les_mentions_legales_sont_completes(texte):
    contenu, _ = texte
    for mention in ("CHOUETTE PATRIMOINE", "SAS au capital de 200,00 €",
                    "48E Route de Larnavey", "33650 Saint-Selve", "109 624 767",
                    "R.C.S. Bordeaux", "TVA non applicable, art. 293 B du CGI"):
        assert mention in contenu, f"{mention!r} absent"


# ── §44 — l'ordre des blocs, et l'absence de page orpheline ─────────────────────────────────────

def test_une_facture_ordinaire_tient_sur_une_page(texte):
    """Elle en occupait deux, la seconde ne portant que « Règlement » et le pied de page."""
    _, pages = texte
    assert pages == 1


def test_le_document_se_termine_sur_le_net(texte):
    """Le récapitulatif clôt la facture : c'est sa conclusion, et cela évite le bloc orphelin.

    Depuis la refonte, règlement et récapitulatif se composent en DEUX COLONNES à partir de la
    même ordonnée — mais l'ordre de lecture ne change pas : le règlement d'abord, le net en
    dernier et plus bas. Comparaison insensible à la casse, les titres étant désormais en
    capitales (cf. `test_les_libelles_portent_leurs_accents`).
    """
    contenu, _ = texte
    haut = contenu.upper()
    assert haut.index("RÈGLEMENT") < haut.index("NET À PAYER"), \
        "les conditions de règlement viennent AVANT le récapitulatif"
    assert haut.index("DÉTAIL DES FRAIS") < haut.index("RÈGLEMENT")


def test_un_document_long_pagine_toujours(octets):
    long = pdfsvc.rendre(_snapshot(reservations=[
        {"reservation_id": str(i), "check_in": "2026-08-02", "check_out": "2026-08-06",
         "nights": 4, "guest_count": 2, "plateforme": "AIRBNB", "assiette_commission": 142.75,
         "taux_commission": 0.18, "commission": 25.70} for i in range(40)]))
    assert long.count(b"/Type /Page\n") >= 2


# ── Déterminisme : à snapshot identique, document identique ─────────────────────────────────────

def test_deux_rendus_du_meme_snapshot_sont_identiques():
    """C'est ce qui rend le hash du document opposable des mois plus tard."""
    assert pdfsvc.rendre(_snapshot()) == pdfsvc.rendre(_snapshot())


# ── §46 — un avoir se distingue au premier coup d'œil ───────────────────────────────────────────

def test_un_avoir_s_annonce_comme_tel(texte):
    contenu, _ = texte
    assert "FACTURE" in contenu
    pymupdf = pytest.importorskip("pymupdf")
    import io
    avoir = pdfsvc.rendre(_snapshot(type_document="AVOIR"))
    doc = pymupdf.open(stream=io.BytesIO(avoir), filetype="pdf")
    assert "AVOIR" in "\n".join(p.get_text() for p in doc)

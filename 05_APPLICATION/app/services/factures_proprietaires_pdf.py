"""Génération du document PDF d'une facture propriétaire.

Le document est produit UNIQUEMENT depuis le snapshot figé à l'émission, jamais depuis une lecture
live des sources : c'est ce qui permet de le regénérer à l'identique des mois plus tard, même si
les calculs, les taux ou les référentiels ont changé entre-temps.

Le PDF est déterministe : à snapshot identique, hash identique. Les métadonnées horodatées de
fpdf sont donc neutralisées explicitement.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fpdf import FPDF
from fpdf.enums import XPos, YPos


#: Encodage des polices de base du PDF (§49).
#
# Ce module translittérait « € » en « EUR », les tirets cadratins en traits d'union et les
# apostrophes typographiques en apostrophes droites, au motif que « les polices de base sont
# encodées en latin-1 ». C'était vrai du RÉGLAGE, pas du FORMAT : l'encodage standard des polices
# de base d'un PDF est WinAnsiEncoding, c'est-à-dire cp1252 — et cp1252 contient « € » (0x80), le
# tiret cadratin, les apostrophes et les guillemets typographiques. latin-1 était un choix
# inutilement restrictif, pas une contrainte du format.
#
# Conséquence : la facture porte « 465,88 € » sans qu'AUCUNE police n'ait à être téléchargée,
# copiée depuis le poste, ni redistribuée. Helvetica suffit.
ENCODAGE_POLICES_BASE = "cp1252"

#: Ce que cp1252 ne contient réellement pas, et qui s'imprimerait en caractère de remplacement.
#: Espaces fines et insécables étroites : raffinements typographiques que l'espace ordinaire
#: remplace sans perte de sens.
_TRANSLITTERATION = str.maketrans({
    "…": "...", " ": " ", " ": " ", " ": " ",
    "‑": "-", "−": "-",
})


def _t(v: Any) -> str:
    """Texte prêt pour une police de base, sans caractère de remplacement visible."""
    s = "" if v is None else str(v)
    return (s.translate(_TRANSLITTERATION)
            .encode(ENCODAGE_POLICES_BASE, "replace").decode(ENCODAGE_POLICES_BASE))


def _montant(v: Any) -> str:
    """« 1 234,56 € » — espace insécable avant le symbole, comme l'exige l'usage français."""
    return f"{float(v or 0):,.2f}".replace(",", " ").replace(".", ",") + " €"


def _siren_lisible(v: Any) -> str:
    """`109624767` → `109 624 767`. Présentation usuelle d'un SIREN, en trois groupes de trois.

    Le numéro est STOCKÉ brut (c'est l'identifiant) et formaté seulement à l'affichage : une valeur
    déjà espacée en configuration est donc rendue à l'identique, et rien n'est ajouté ni retiré au
    numéro lui-même. Une valeur qui n'a pas 9 chiffres est rendue telle quelle — mieux vaut afficher
    ce qui a été saisi qu'un regroupement inventé sur un numéro inattendu.
    """
    brut = "".join(str(v or "").split())
    if len(brut) == 9 and brut.isdigit():
        return f"{brut[:3]} {brut[3:6]} {brut[6:]}"
    return str(v or "").strip()


def _taux(v: Any) -> str:
    """`0.19` → `19,00 %`. Les taux sont stockés en fraction par Lot10 ; les afficher tels quels
    ferait lire « 0,19 % » sur la facture."""
    if v in (None, ""):
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    pourcent = f * 100 if abs(f) <= 1 else f
    return f"{pourcent:.2f}".replace(".", ",") + " %"


_LIBELLES_NATURE = {
    "PRESTATION_DE_SERVICES": "Gestion de location courte durée",
    "NON_APPLICABLE": "Non applicable",
}


def _libelle_nature(code: Any) -> str:
    """Code technique → libellé imprimable. `PRESTATION_DE_SERVICES` décrit une catégorie fiscale,
    pas ce que le propriétaire a acheté. Le code reste stocké ; seul l'affichage change.

    Un code inconnu est rendu lisible (soulignés retirés) plutôt que masqué : mieux vaut un libellé
    approximatif qu'une mention obligatoire absente."""
    texte = str(code or "").strip()
    if not texte:
        return ""
    return _LIBELLES_NATURE.get(texte.upper(), texte.replace("_", " ").capitalize())


def _telephone_lisible(v: Any) -> str:
    """`+33610190367` → `06 10 19 03 67` (§45). Un numéro se stocke canonique et se lit en paires.

    L'import est local : ce module doit rester capable de rendre un document archivé sans dépendre
    des services applicatifs, et un référentiel indisponible ne doit pas empêcher une facture de
    s'imprimer — la valeur brute est alors rendue telle quelle.
    """
    try:
        from app.services import telephone_service as tel
        return tel.afficher(v)
    except Exception:      # noqa: BLE001
        return str(v or "")


def _date_fr(v: Any) -> str:
    """`2026-07-31` → `31/07/2026`. Renvoie la valeur telle quelle si le format est inattendu."""
    s = "" if v is None else str(v)[:10]
    parties = s.split("-")
    return f"{parties[2]}/{parties[1]}/{parties[0]}" if len(parties) == 3 else s


# ── Direction artistique Chouette Patrimoine ────────────────────────────────────────────────────
# Valeurs reprises TELLES QUELLES du site (`src/app/globals.css`, bloc `@theme inline`) : ce sont
# les jetons de marque en production, pas une interprétation. Les redéfinir « à peu près » ferait
# diverger la facture du reste de la marque au premier changement de charte.
BRIQUE = (0x8C, 0x43, 0x36)      # --color-brick     : accent profond, titres et bandeau du total
TERRACOTTA = (0xB6, 0x5E, 0x4B)  # --color-terracotta : accent, en-têtes de tableau
CREME = (0xF7, 0xF1, 0xEB)       # --color-cream      : fond des blocs et lignes alternées
SABLE = (0xE8, 0xDD, 0xD2)       # --color-sand       : filets et séparateurs
ESPRESSO = (0x2E, 0x21, 0x1D)    # --color-espresso   : texte principal
PIERRE = (0x6F, 0x64, 0x5E)      # --color-stone      : texte secondaire
SAGE = (0x6D, 0x76, 0x62)        # --color-sage       : bandeau d'un net EN FAVEUR du propriétaire
BLANC = (0xFF, 0xFF, 0xFF)

# Le logo officiel du site, copié dans les assets de l'application (`static/img/brand/`). Aucun
# logo n'est redessiné : si le fichier manque, le document porte le nom en toutes lettres plutôt
# qu'un substitut graphique inventé.
LOGO = Path(__file__).resolve().parent.parent / "static" / "img" / "brand" / "chouette-logo.png"

MARGE = 15.0
LARGEUR_UTILE = 210.0 - 2 * MARGE

# ── Grille et respiration ───────────────────────────────────────────────────────────────────────
# Une facture est un document, pas un tableur : la lisibilité vient d'abord des blancs. Ces
# constantes remplacent les valeurs dispersées dans le code de rendu, pour qu'un ajustement de
# densité se fasse en un seul endroit et reste cohérent d'un bloc à l'autre.
RAYON = 1.6            # arrondi des cartes — franc mais sobre, jamais une pastille
PADDING = 3.5          # air intérieur d'une carte
GOUTTIERE = 5.0        # espace entre les deux cartes ÉMETTEUR / FACTURÉ À
LARGEUR_CARTE = (LARGEUR_UTILE - GOUTTIERE) / 2
HAUTEUR_LIGNE = 6.5    # hauteur d'une ligne de tableau : +0,3 mm sur l'ancienne, l'air se voit
BAS_PIED = 22.0        # hauteur réservée au pied de page

#: Fond très clair des lignes alternées. CRÈME pleine faisait des bandes trop marquées une fois
#: les bordures retirées : la teinte est éclaircie pour rester un repère, pas une trame.
CREME_CLAIR = (0xFB, 0xF8, 0xF5)

# Regroupement des types de ligne, identique à celui du service de composition. Redéfini ici plutôt
# qu'importé pour garder ce module SANS dépendance applicative : il ne reçoit qu'un snapshot et doit
# rester capable de rendre un document archivé, même si les services évoluent. Le test
# `test_pdf_groupes_alignes_sur_le_service` verrouille l'égalité des deux tables.
_GROUPES_PDF: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("commissions", "Commissions", ("COMMISSION_CONCIERGERIE",)),
    ("menages", "Ménages", ("MENAGE_FACTURE",)),
    ("canape", "Canapé", ("PREPARATION_CANAPE",)),
    ("forfait", "Forfait", ("CHARGE_FIXE",)),
    ("refacturations", "Refacturation", ("CHARGES_EXCEPT_REFAC", "CHARGE_REFACTUREE")),
    ("extras", "Extra", ("EXTRA",)),
    ("reductions", "Réduction", ("REDUCTION",)),
)


class _Facture(FPDF):
    def __init__(self, snapshot: dict[str, Any]):
        super().__init__(orientation="P", unit="mm", format="A4")
        # WinAnsiEncoding plutôt que latin-1 : c'est l'encodage standard des polices de base d'un
        # PDF, et c'est lui qui permet d'imprimer « € » sans embarquer de police (cf.
        # ENCODAGE_POLICES_BASE).
        self.core_fonts_encoding = ENCODAGE_POLICES_BASE
        self.snapshot = snapshot
        self.set_margins(MARGE, MARGE, MARGE)
        # 26 mm : la hauteur réelle du pied de page de marque, mesurée. Une marge plus courte
        # laisserait une ligne de tableau chevaucher les mentions légales sur les documents longs.
        self.set_auto_page_break(auto=True, margin=26)
        self.set_compression(False)   # sortie stable, indépendante de la version de zlib
        self._entete_tableau: tuple | None = None

    # ── En-tête de marque ───────────────────────────────────────────────────────────────────────

    def header(self):
        snap = self.snapshot
        est_avoir = snap.get("type_document") == "AVOIR"
        premiere = self.page_no() == 1

        if premiere:
            self._bandeau_marque(est_avoir)
        else:
            # Pages suivantes : rappel discret, pas une seconde page de garde.
            self.set_y(8)
            self.set_font("Helvetica", "", 8)
            self.set_text_color(*PIERRE)
            self.cell(0, 5, _t(f"{'Avoir' if est_avoir else 'Facture'} "
                               f"{snap.get('numero_facture') or ''} — suite"),
                      0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
            self.set_text_color(*ESPRESSO)
            self.ln(2)
            if self._entete_tableau:
                self._ligne_entete(*self._entete_tableau)

    def _bandeau_marque(self, est_avoir: bool):
        """Identité à gauche, nature du document à droite, sur une même ligne de base.

        L'ancienne version empilait le titre et quatre lignes de référence en corps 9, toutes de
        même poids : le numéro, la période et le statut se lisaient d'un bloc. Ici la hiérarchie
        est explicite — le mot FACTURE domine, les références sont secondaires, et le statut d'un
        document non émis devient un badge plutôt qu'une ligne de texte perdue au milieu des
        autres.
        """
        snap = self.snapshot
        haut = 13.0
        if LOGO.exists():
            # Le logo est plus haut que large (256x384) : on borne la HAUTEUR et laissons fpdf
            # déduire la largeur, sinon il serait étiré.
            try:
                self.image(str(LOGO), x=MARGE, y=haut, h=21)
            except Exception:      # noqa: BLE001 — un logo illisible ne doit pas empêcher la facture
                pass
        self.set_xy(MARGE + 19, haut + 2.5)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*BRIQUE)
        self.cell(76, 7, _t(snap.get("emetteur", {}).get("nom") or "Chouette Patrimoine"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(MARGE + 19)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*PIERRE)
        self.set_char_spacing(0.4)
        self.cell(76, 4, _t("CONCIERGERIE DE LOCATION COURTE DURÉE"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_char_spacing(0)

        # Pavé titre, aligné à droite. La colonne commence à 112 mm : assez large pour que
        # « Période des prestations : du 01/09/2026 au 13/09/2026 » tienne sur UNE ligne — elle
        # débordait sous l'ancienne largeur et repassait à la ligne sans indentation.
        colonne, largeur = 112.0, 210 - MARGE - 112.0
        self.set_xy(colonne, haut)
        self.set_font("Helvetica", "B", 24)
        self.set_text_color(*ESPRESSO)
        self.set_char_spacing(1.2)
        self.cell(largeur, 11, _t("AVOIR" if est_avoir else "FACTURE"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
        self.set_char_spacing(0)
        self.ln(0.8)
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(*PIERRE)
        for texte in self._references():
            self.set_x(colonne)
            self.cell(largeur, 4.4, _t(texte), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

        statut = snap.get("statut")
        if statut and statut != "EMIS":
            self.ln(1.4)
            self._badge(str(statut), droite=210 - MARGE)
            self.ln(2.2)      # le badge ne doit pas venir s'asseoir sur le filet de marque

        self.set_y(max(self.get_y(), haut + 23))
        self.set_draw_color(*TERRACOTTA)
        self.set_line_width(0.7)
        self.line(MARGE, self.get_y(), 210 - MARGE, self.get_y())
        self.set_line_width(0.2)
        self.set_text_color(*ESPRESSO)
        self.ln(4.5)

    def _badge(self, texte: str, *, droite: float):
        """Pastille discrète, calée sur son bord droit. Sert au statut d'un document non émis.

        Contraste volontairement doux (fond sable, texte brique) : un BROUILLON doit se remarquer
        sans faire croire à un tampon officiel.
        """
        self.set_font("Helvetica", "B", 7)
        self.set_char_spacing(0.5)
        largeur = self.get_string_width(_t(texte.upper())) + 7
        hauteur = 5.0
        x, y = droite - largeur, self.get_y()
        self.set_fill_color(*SABLE)
        self.rect(x, y, largeur, hauteur, style="F", round_corners=True,
                  corner_radius=hauteur / 2)
        self.set_xy(x, y)
        self.set_text_color(*BRIQUE)
        self.cell(largeur, hauteur, _t(texte.upper()), 0,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.set_char_spacing(0)
        self.set_text_color(*ESPRESSO)

    def _references(self) -> list[str]:
        snap = self.snapshot
        conf = snap.get("conformite") or {}
        out = []
        if snap.get("numero_facture"):
            out.append(f"N° {snap['numero_facture']}")
        if snap.get("date_facture"):
            out.append(f"Emise le {_date_fr(snap['date_facture'])}")
        # Libellé RÉGLEMENTAIRE, à conserver tel quel : « période des prestations » est la mention
        # attendue sur une facture de services, pas un simple intitulé de mise en page.
        debut, fin = conf.get("periode_debut"), conf.get("periode_fin")
        out.append(f"Période des prestations : du {_date_fr(debut)} au {_date_fr(fin)}"
                   if debut and fin else f"Période des prestations : {snap.get('mois', '')}")
        if snap.get("type_document") == "AVOIR" and snap.get("facture_origine"):
            out.append(f"Avoir sur : {snap['facture_origine']}")
        return out

    # ── Pied de page ────────────────────────────────────────────────────────────────────────────

    def footer(self):
        """Ancré en bas, jamais superposé au contenu : `BAS_PIED` lui est réservé, et
        `_place_restante` interdit d'écrire au-delà.

        Le filet est plus court que la largeur utile et centré : il pose le bloc sans redessiner
        une bordure de page. Le numéro de page est séparé des mentions légales par un demi-interligne
        — c'est une information de navigation, pas une mention légale de plus.
        """
        self.set_y(-BAS_PIED)
        self.set_draw_color(*SABLE)
        self.set_line_width(0.2)
        self.line(70, self.get_y(), 140, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "", 6.6)
        self.set_text_color(*PIERRE)
        for ligne in self._mentions_pied():
            self.cell(0, 3.1, _t(ligne), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.ln(1)
        self.set_font("Helvetica", "", 6.4)
        self.cell(0, 3.1, _t(f"Page {self.page_no()} / {{nb}}"), 0,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.set_text_color(*ESPRESSO)

    def _mentions_pied(self) -> list[str]:
        """Bloc légal du pied de page, dans la présentation usuelle d'une facture française.

        Trois lignes, chacune omise si elle serait vide — rien n'est complété par défaut : une
        mention légale absente doit se voir, pas être remplacée par une formule plausible.

            CHOUETTE PATRIMOINE — SAS au capital de 200,00 EUR
            48E Route de Larnavey — 33650 Saint-Selve
            109 624 767 R.C.S. Bordeaux

        La 3ᵉ ligne est la mention d'immatriculation normalisée : le SIREN est suivi du greffe,
        sans étiquette « SIREN : », parce que c'est sous cette forme qu'elle est opposable.
        SIRET et TVA intracommunautaire ne s'ajoutent que s'ils sont RÉELLEMENT connus, chacun sous
        sa propre étiquette — un SIREN présenté comme un SIRET serait un numéro faux, et les
        5 chiffres du NIC ne sont jamais fabriqués.
        """
        snap = self.snapshot
        em = (snap.get("conformite") or {}).get("emetteur") or {}
        base = snap.get("emetteur", {})

        def champ(*noms: str) -> str:
            for source in (em, base):
                for nom in noms:
                    valeur = str(source.get(nom) or "").strip()
                    if valeur:
                        return valeur
            return ""

        denomination = champ("denomination", "nom")
        forme = champ("forme_juridique")
        capital = champ("capital")
        adresse = champ("adresse_siege", "adresse")
        siren = champ("siren")
        siret = champ("siret")
        rcs = champ("rcs")
        tva = champ("tva_intra")

        # « SAS au capital de 200,00 EUR » : la forme et le capital forment une seule mention.
        # Séparés, on lirait « SAS — 200,00 EUR », qui ne veut rien dire.
        if forme and capital:
            entite = f"{denomination} — {forme} au capital de {capital}"
        elif forme:
            entite = f"{denomination} — {forme}"
        elif capital:
            entite = f"{denomination} — capital de {capital}"
        else:
            entite = denomination

        immatriculation = " ".join(x for x in (_siren_lisible(siren), rcs) if x)
        complements = [f"SIRET {siret}" if siret else None,
                       f"TVA {tva}" if tva else None]
        complement = " · ".join(x for x in complements if x)
        if complement:
            immatriculation = f"{immatriculation} · {complement}".strip(" ·")

        lignes = [entite, adresse.replace(", ", " — "), immatriculation]
        mention = (snap.get("conformite") or {}).get("mention_tva")
        if mention:
            lignes.append(mention)
        return [l for l in lignes if l]

    # ── Briques de mise en page ─────────────────────────────────────────────────────────────────

    # Hauteur utile d'une page : 297 mm moins la marge basse réservée au pied de marque, plus
    # 4 mm de garde. Le pied ne doit jamais toucher la dernière ligne écrite.
    BAS_UTILE = 297 - BAS_PIED - 4

    def _place_restante(self) -> float:
        """Millimètres encore disponibles avant le pied de page."""
        return self.BAS_UTILE - self.get_y()

    def _reserver(self, hauteur: float) -> None:
        """Garantit `hauteur` mm d'un seul tenant : coupe la page AVANT d'écrire si besoin.

        Sans cette réservation, le saut automatique de fpdf coupe là où il se trouve et laisse des
        blocs orphelins — la facture de recette finissait avec une page 2 portant trois lignes et
        un pied de page. Un bloc se déplace ENTIER, il ne se scinde pas (§30).
        """
        if hauteur > 0 and self._place_restante() < hauteur:
            self.add_page()

    def _titre_section(self, texte: str, hauteur_bloc: float = 22.0,
                       largeur: float | None = None):
        """Un titre ne doit jamais rester seul en bas de page : on force la coupe s'il ne reste pas
        de quoi afficher le bloc qu'il annonce.

        `hauteur_bloc` est la place nécessaire au titre ET à son contenu. La valeur par défaut
        couvre un en-tête de tableau plus une ligne ; les appelants qui connaissent leur hauteur
        réelle la passent, pour que le bloc migre en entier plutôt qu'à moitié.
        """
        self._reserver(hauteur_bloc)
        # Le titre porte sa propre hiérarchie : capitales espacées en corps 9, soulignées d'un
        # filet sable traversant, sur lequel un court segment brique marque le début de section.
        # Un titre en corps 10 gras sur filet plein se lisait comme une ligne de tableau de plus.
        self.ln(1.8)
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*BRIQUE)
        self.set_char_spacing(0.8)
        self.cell(0, 5.2, _t(texte.upper()), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_char_spacing(0)
        self.set_text_color(*ESPRESSO)
        y = self.get_y() + 0.4
        self.set_draw_color(*SABLE)
        self.set_line_width(0.2)
        self.line(MARGE, y, MARGE + (largeur or LARGEUR_UTILE), y)
        self.set_draw_color(*BRIQUE)
        self.set_line_width(0.7)
        self.line(MARGE, y, MARGE + 14, y)
        self.set_line_width(0.2)
        self.set_draw_color(*SABLE)
        self.ln(2.0)

    def _ligne_entete(self, colonnes: tuple, hauteur: float = 7.0):
        """En-tête de tableau. Mémorisé pour être RÉPÉTÉ automatiquement en haut de chaque page
        suivante (cf. `header`) — sans quoi un tableau long deviendrait illisible dès la page 2.

        Bandeau sable + libellés brique, plutôt que terracotta plein + blanc : à l'impression,
        un aplat saturé sur toute la largeur écrasait le reste du document et bavait sur les
        imprimantes laser d'entrée de gamme. Le contraste sable/brique reste franc, et le coin
        supérieur du bandeau est arrondi pour que le tableau lise comme un bloc, pas comme une
        grille.
        """
        largeur_totale = sum(c[0] for c in colonnes)
        y = self.get_y()
        self.set_fill_color(*SABLE)
        # Arrondi sur les seuls coins hauts : le bas du bandeau touche la première ligne.
        self.rect(MARGE, y, largeur_totale, hauteur, style="F", round_corners=("TOP_LEFT",
                                                                               "TOP_RIGHT"),
                  corner_radius=RAYON)
        self.set_xy(MARGE, y)
        self.set_font("Helvetica", "B", 7.6)
        self.set_text_color(*BRIQUE)
        self.set_char_spacing(0.3)
        for i, (largeur, titre, align) in enumerate(colonnes):
            dernier = i == len(colonnes) - 1
            self.cell(largeur, hauteur, _t(titre), 0,
                      new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                      new_y=YPos.NEXT if dernier else YPos.TOP, align=align, fill=False)
        self.set_char_spacing(0)
        self.set_text_color(*ESPRESSO)
        self.set_draw_color(*SABLE)

    @staticmethod
    def _hauteur_note(texte: str) -> float:
        """Hauteur qu'occupera `_note`. Calculée, pas devinée : une marge de sécurité arbitraire
        pousse des blocs à la page suivante alors qu'ils tenaient (constaté à 0,2 mm près)."""
        return max(1, len(texte) // 110 + 1) * 3.6 + 4

    def _note(self, texte: str):
        """Note explicative sous un tableau. Coupe la page AVANT d'écrire si la place manque —
        une note tronquée par le saut automatique perdrait justement l'explication qu'elle porte
        (constaté sur le premier rendu : la phrase distinguant acompte et réduction était coupée)."""
        self._reserver(self._hauteur_note(texte))
        self.ln(1)
        self.set_font("Helvetica", "I", 7.5)
        self.set_text_color(*PIERRE)
        self.multi_cell(0, 3.6, _t(texte))
        self.set_text_color(*ESPRESSO)

    def _ligne_tableau(self, colonnes: tuple, valeurs: tuple, pair: bool,
                       hauteur: float = HAUTEUR_LIGNE):
        """Une ligne, SANS bordure de cellule.

        L'ancienne version bordait chaque cellule par le bas (`"B"`) : quinze colonnes de traits
        verticaux implicites et un filet sous chaque cellule donnaient l'aspect d'une feuille de
        calcul exportée. Ici la séparation est portée par une alternance de fond très claire, et
        rien d'autre — l'alignement des montants suffit à guider l'oeil.
        """
        self.set_font("Helvetica", "", 8)
        self.set_fill_color(*(CREME_CLAIR if pair else BLANC))
        for i, ((largeur, _, align), valeur) in enumerate(zip(colonnes, valeurs)):
            dernier = i == len(colonnes) - 1
            self.cell(largeur, hauteur, _t(valeur), 0,
                      new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                      new_y=YPos.NEXT if dernier else YPos.TOP, align=align, fill=True)

    def _fin_tableau(self, colonnes: tuple):
        """Filet de clôture d'un tableau : il FERME le bloc, là où l'absence de bordure le
        laisserait flotter."""
        y = self.get_y()
        self.set_draw_color(*SABLE)
        self.set_line_width(0.3)
        self.line(MARGE, y, MARGE + sum(c[0] for c in colonnes), y)
        self.set_line_width(0.2)

    def _bloc_parties(self):
        snap = self.snapshot
        conf = snap.get("conformite") or {}
        em, cl = conf.get("emetteur") or {}, conf.get("client") or {}
        base, dest = snap.get("emetteur", {}), snap.get("destinataire", {})

        # « Représentée par … » : mention demandée explicitement, sans aucun titre juridique ajouté
        # (ni « gérant », ni « président ») — le Kbis n'en documente pas, et en inventer un sur une
        # facture engagerait la société sur une qualité non vérifiée.
        representants = str(conf.get("representants") or base.get("representants") or "").strip()
        gauche = [x for x in (
            f"Représentée par {representants}" if representants else None,
            em.get("adresse_siege") or base.get("adresse"),
            # Aucun contact tant qu'il n'est pas renseigné : jamais de courriel d'exemple.
            em.get("contact") or base.get("contact") or None,
        ) if x]

        # Les identifiants du CLIENT sont obligatoires dès qu'il est professionnel — les omettre
        # rendrait la facture non conforme. Ils ne s'impriment que s'ils sont renseignés, et sous
        # leur propre étiquette (un SIREN client n'est pas davantage un SIRET que celui de
        # l'émetteur).
        #
        # `Référence : PROP_0001` a été RETIRÉE : un identifiant interne n'apprend rien au
        # destinataire et n'a pas sa place sur un document qui sort de l'application. Le logement
        # est désigné par son NOM ; plusieurs logements sont listés quand il y en a plusieurs.
        logements = conf.get("logements") or snap.get("logements") or []
        if not logements and snap.get("logement_id"):
            logements = [snap.get("logement_nom") or snap.get("logement_id")]
        # UN IDENTIFIANT N'EST PAS UN NOM (§102). `_noms_logements` se replie volontairement sur
        # l'identifiant quand le référentiel ne connaît pas le logement — c'est le bon choix côté
        # DONNÉES, où une ligne vide serait une perte d'information. Sur le DOCUMENT, c'est
        # l'inverse : « Logement : LOG_0001 » n'apprend rien au propriétaire et fait sortir un code
        # interne de l'application. Les snapshots anciens en portent encore un, faute de
        # `conformite.logements` à l'époque de leur émission — et ils sont figés, donc on les filtre
        # à l'affichage plutôt que de les réécrire. Le repère est structurel : une valeur qui EST
        # l'identifiant n'est pas un nom.
        identifiants = {str(i) for i in
                        ([snap.get("logement_id")] + list(snap.get("logements") or [])) if i}
        logements = [l for l in logements if str(l) not in identifiants]
        lignes_logement: list[str] = []
        if len(logements) == 1:
            lignes_logement = [f"Logement : {logements[0]}"]
        elif logements:
            lignes_logement = ["Logements concernés :"] + [f"  {l}" for l in logements]

        droite = [x for x in (cl.get("denomination") or dest.get("nom"),
                              cl.get("adresse_facturation") or cl.get("adresse")
                              or dest.get("adresse"),
                              # Contact du client : téléphone d'abord, courriel à défaut. Rien si
                              # le référentiel n'en connaît aucun — on n'invente pas un contact.
                              _telephone_lisible(cl.get("telephone")) or cl.get("email") or None,
                              f"SIREN {cl['siren']}" if cl.get("siren") else None,
                              f"SIRET {cl['siret']}" if cl.get("siret") else None,
                              f"TVA {cl['tva_intra']}" if cl.get("tva_intra") else None,
                              *lignes_logement) if x]
        # La DÉNOMINATION est la première ligne de chaque partie et se compose en gras : sans
        # elle, les deux colonnes s'ouvraient l'une sur « Représentée par… » et l'autre sur un nom,
        # de même graisse — on ne voyait plus qui facturait qui.
        titre_gauche = str(em.get("denomination") or base.get("nom") or "").strip()
        gauche = ([titre_gauche] if titre_gauche else []) + gauche
        self._cartes_parties(gauche, droite)

    def _cartes_parties(self, gauche: list[str], droite: list[str]):
        """Les deux parties dans deux cartes alignées, de MÊME hauteur.

        Avant, c'étaient deux colonnes de texte nu posées côte à côte : rien ne les délimitait, et
        la partie la plus courte laissait un vide qu'on lisait comme une erreur de composition. Un
        fond crème et un arrondi franc suffisent à en faire deux blocs — aucune bordure, qui
        alourdirait sans rien ajouter.

        La hauteur est calculée sur le plus long des deux contenus, donc les deux cartes se
        terminent au même millimètre.
        """
        interligne = 4.4
        nb = max(len(gauche), len(droite))
        hauteur = PADDING + 4.6 + nb * interligne + PADDING - 1
        self._reserver(hauteur + 2)
        y = self.get_y()

        self.set_fill_color(*CREME)
        for x in (MARGE, MARGE + LARGEUR_CARTE + GOUTTIERE):
            self.rect(x, y, LARGEUR_CARTE, hauteur, style="F", round_corners=True,
                      corner_radius=RAYON)

        for x, etiquette, contenu in ((MARGE, "ÉMETTEUR", gauche),
                                      (MARGE + LARGEUR_CARTE + GOUTTIERE, "FACTURÉ À", droite)):
            self.set_xy(x + PADDING, y + PADDING - 1)
            self.set_font("Helvetica", "B", 6.8)
            self.set_text_color(*PIERRE)
            self.set_char_spacing(0.7)
            self.cell(LARGEUR_CARTE - 2 * PADDING, 4.6, _t(etiquette), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_char_spacing(0)
            self.set_text_color(*ESPRESSO)
            for i, ligne in enumerate(contenu):
                self.set_x(x + PADDING)
                # Première ligne = la dénomination : c'est elle qui identifie la partie.
                self.set_font("Helvetica", "B" if i == 0 else "", 8.5)
                self.cell(LARGEUR_CARTE - 2 * PADDING, interligne, _t(ligne), 0,
                          new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_font("Helvetica", "", 8.5)
        self.set_y(y + hauteur)
        self.ln(2.0)

    def _tableau_sejours(self):
        """Séjours de la période et commission de chacun.

        AUCUNE donnée personnelle du voyageur n'est imprimée : ni nom, ni e-mail, ni téléphone. La
        facture doit pouvoir circuler (comptable, propriétaire, archivage) sans emporter de PII.

        LA RÉFÉRENCE DE RÉSERVATION A ÉTÉ RETIRÉE (§43). « 63200085 » est l'identifiant Hostaway :
        il ne dit rien au propriétaire, qui n'a pas accès à Hostaway, et une facture n'est pas un
        écran d'application. Le séjour est déjà identifié sans ambiguïté par ses dates et son canal.
        Le nombre de VOYAGEURS le remplace : c'est une information que le propriétaire comprend, et
        qui éclaire la préparation du logement — sans nommer personne.
        """
        reservations = self.snapshot.get("reservations") or []
        if not reservations:
            return
        self._titre_section("Séjours de la période et commission")
        # Assiette x Taux = Commission, colonne par colonne : le propriétaire doit pouvoir refaire
        # l'opération de tête pour chaque séjour, sans avoir à nous croire sur parole.
        colonnes = ((22, "Arrivée", "C"), (22, "Départ", "C"), (12, "Nuits", "C"),
                    (19, "Voyageurs", "C"), (26, "Canal", "L"), (25, "Total perçu", "R"),
                    (15, "Taux", "R"), (24, "Commission", "R"))
        self._entete_tableau = (colonnes,)
        self._ligne_entete(colonnes)
        total_commission = 0.0
        for i, r in enumerate(reservations):
            taux = r.get("taux_commission")
            commission = r.get("commission")
            total_commission += float(commission or 0)
            self._ligne_tableau(colonnes, (
                _date_fr(r.get("check_in")), _date_fr(r.get("check_out")),
                r.get("nights") if r.get("nights") is not None else "",
                # Le nombre de voyageurs vient du séjour. Absent, la case reste vide : « 0 » se
                # lirait comme « personne n'est venu », ce que la donnée ne dit pas.
                r.get("guest_count") if r.get("guest_count") is not None else "",
                r.get("plateforme") or "",
                _montant(r.get("assiette_commission")) if r.get("assiette_commission") is not None
                else (_montant(r.get("payout")) if r.get("payout") is not None else ""),
                _taux(taux), _montant(commission) if commission is not None else "",
            ), pair=(i % 2 == 0))
        self._entete_tableau = None
        self._fin_tableau(colonnes)
        if total_commission:
            self.ln(0.6)
            self.set_font("Helvetica", "B", 8.5)
            self.set_text_color(*BRIQUE)
            self.cell(sum(c[0] for c in colonnes[:-1]), 6, _t("Total commissions"), 0,
                      new_x=XPos.RIGHT, new_y=YPos.TOP, align="R")
            self.cell(colonnes[-1][0], 6, _t(_montant(total_commission)), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
            self.set_text_color(*ESPRESSO)
        # L'explication du calcul est CONSERVÉE : c'est elle qui rend la colonne vérifiable à la
        # main. Seul son vocabulaire suit le nouveau libellé de colonne.
        self._note(f"{len(reservations)} séjour(s). Total perçu = versement de la plateforme "
                   "diminué du ménage ; commission = total perçu x taux. Ces montants proviennent "
                   "du calcul mensuel, ils ne sont pas recalculés sur la facture.")

    def _tableau_prestations(self):
        """Détail facturé, groupé par poste. C'est la lecture « d'où vient le montant ».

        Le RÉCAPITULATIF ne se rend plus ici : il clôt le document (cf. `corps`, §44).
        """
        snap = self.snapshot
        self._titre_section("Détail des frais")
        colonnes = ((12, "N", "C"), (108, "Désignation", "L"), (30, "Poste", "L"),
                    (30, "Montant", "R"))
        self._entete_tableau = (colonnes,)
        self._ligne_entete(colonnes)
        libelles = {c: lib for c, lib, _ in _GROUPES_PDF}
        appartenance = {t: c for c, _, types in _GROUPES_PDF for t in types}
        for i, l in enumerate(snap.get("lignes", [])):
            poste = libelles.get(appartenance.get(l.get("type_ligne"), ""), "Autre")
            self._ligne_tableau(colonnes, (
                l.get("numero_ligne"), l.get("libelle"), poste,
                _montant(l.get("montant")),
            ), pair=(i % 2 == 0))
        self._entete_tableau = None
        self._fin_tableau(colonnes)

    #: Hauteur d'une ligne du récapitulatif. Toutes les lignes en font exactement autant : c'est
    #: ce qui permet de CALCULER la hauteur de la carte avant de l'écrire, au lieu de la deviner.
    H_LIGNE_RECAP = 5.2

    def _hauteur_recapitulatif(self, deco: dict[str, Any]) -> tuple[float, float]:
        """(hauteur du contenu, hauteur du bandeau du net), en millimètres.

        fpdf n'a pas de calques : un rectangle dessiné après le texte le recouvre. Le fond de la
        carte doit donc être posé AVANT, ce qui suppose de connaître sa hauteur à l'avance. Toutes
        les lignes ayant la même hauteur, ce calcul est exact et non une marge de sécurité.
        """
        h = self.H_LIGNE_RECAP
        nb_postes = len([p for p in deco.get("postes", [])
                         if p.get("cle") != "reductions" and p.get("nb")])
        nb_reglements = sum(1 for v in (deco.get("total_acomptes"),
                                        deco.get("total_reversements_airbnb")) if v)
        contenu = (PADDING - 1)                       # air haut
        contenu += h * nb_postes                      # postes
        contenu += 0.8 + 1.8                          # filet + interligne
        contenu += h                                  # sous-total
        contenu += h if deco.get("total_reductions") else 0
        contenu += h * 3                              # TOTAL HT / TVA / TOTAL FACTURE
        if nb_reglements:
            contenu += 1 + 4.5 + h * nb_reglements    # intertitre + lignes
        contenu += PADDING - 1                        # air bas
        return contenu, 12.0

    def _recapitulatif(self, deco: dict[str, Any], *, y_depart: float | None = None):
        """Le récapitulatif EST la formule. Chaque poste apparaît même à zéro dès qu'il porte une
        ligne, pour qu'on puisse suivre l'addition sans deviner ce qui a été omis.

        PRÉSENTATION. Une carte crème occupant la moitié droite, alignée sur la carte « FACTURÉ À »
        de l'en-tête : deux blocs de même largeur ouvrent et ferment le document. Le net se détache
        dessous, en aplat plein — c'est le seul du document, et le seul chiffre composé en grand.
        """
        hauteur_carte, hauteur_badge = self._hauteur_recapitulatif(deco)
        if y_depart is None:
            self.ln(3)
            # Le récapitulatif EST la conclusion du document : le scinder entre deux pages
            # séparerait le total facturé de son net. On mesure le bloc réel avant de l'écrire.
            self._reserver(hauteur_carte + hauteur_badge + 4)
        else:
            # Place déjà réservée par l'appelant, qui aligne cette carte sur la colonne de gauche.
            self.set_y(y_depart)

        largeur = LARGEUR_CARTE
        gauche = 210 - MARGE - largeur
        interne = largeur - 2 * PADDING
        y_carte = self.get_y()
        self.set_fill_color(*CREME)
        self.rect(gauche, y_carte, largeur, hauteur_carte, style="F", round_corners=True,
                  corner_radius=RAYON)
        self.set_y(y_carte + PADDING - 1)

        def ligne(libelle: str, valeur: Any, gras: bool = False, couleur=None):
            self.set_x(gauche + PADDING)
            self.set_font("Helvetica", "B" if gras else "", 9 if gras else 8.5)
            self.set_text_color(*(couleur or ESPRESSO))
            self.cell(interne * 0.58, self.H_LIGNE_RECAP, _t(libelle), 0,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(interne * 0.42, self.H_LIGNE_RECAP, _t(_montant(valeur)), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
            self.set_text_color(*ESPRESSO)

        for poste in deco.get("postes", []):
            if poste.get("cle") == "reductions" or not poste.get("nb"):
                continue
            ligne(poste["libelle"], poste["montant"], couleur=PIERRE)
        self.set_draw_color(*SABLE)
        self.set_line_width(0.2)
        y_filet = self.get_y() + 0.8
        self.line(gauche + PADDING, y_filet, gauche + largeur - PADDING, y_filet)
        self.ln(1.8)
        ligne("Sous-total", deco.get("sous_total"))
        if deco.get("total_reductions"):
            ligne("Reductions", -float(deco["total_reductions"]))

        # TOTAL HT / TVA / TOTAL TTC : mentions OBLIGATOIRES sur une facture de services, même
        # sous un régime de franchise où la TVA vaut zéro. Elles viennent du bloc de conformité
        # figé, pas d'un calcul refait ici. `montant_total` sert de repli quand ce bloc est absent
        # (prévisualisation d'un brouillon dont la conformité n'a pas encore été construite).
        conf = self.snapshot.get("conformite") or {}
        total_ttc = conf.get("total_ttc", deco.get("total_facture"))
        ligne("TOTAL HT", conf.get("total_ht", deco.get("total_facture")))
        ligne("TVA", conf.get("total_tva", 0.0))
        # « TOTAL TTC » est une mention OBLIGATOIRE : elle reste, même en franchise où elle égale le
        # HT. On lui adjoint « TOTAL FACTURE » plutôt que de la remplacer (§27) — c'est la même
        # somme, nommée dans les deux vocabulaires : celui de la loi et celui du propriétaire, qui
        # doit voir d'un coup d'œil ce qui sépare le montant facturé de ce qu'il lui reste à payer.
        ligne("TOTAL FACTURE (TTC)", total_ttc, gras=True)

        # ── Règlements et compensations (§20) ────────────────────────────────────────────────
        # Ces lignes viennent APRÈS le total facturé et ne le modifient jamais : un acompte est un
        # règlement déjà reçu, un reversement Airbnb une somme déjà détenue. Ni l'un ni l'autre ne
        # diminue le chiffre d'affaires facturé, ni le produit comptabilisé. C'est précisément
        # pourquoi ils sont présentés en dessous, et non fondus dans le total.
        acomptes = float(deco.get("total_acomptes") or 0)
        reversements = float(deco.get("total_reversements_airbnb") or 0)
        if acomptes or reversements:
            self.ln(1)
            self.set_x(gauche + PADDING)
            self.set_font("Helvetica", "B", 6.8)
            self.set_text_color(*PIERRE)
            self.set_char_spacing(0.7)
            self.cell(interne, 4.5, _t("RÈGLEMENTS ET COMPENSATIONS"), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_char_spacing(0)
            self.set_text_color(*ESPRESSO)
            # §42 — ORDRE DÉFINITIF : total facture → reversement Airbnb → acompte(s) → net. Le
            # reversement vient d'abord : c'est une somme que la plateforme a déjà versée pour ce
            # mois, avant tout paiement du propriétaire.
            if reversements:
                ligne("Reversement Airbnb du mois", -reversements)
            if acomptes:
                ligne("Acompte(s) déjà versé(s)", -acomptes)

        # Bandeau du net : le seul élément coloré plein du document, pour qu'il soit le premier
        # chiffre que l'oeil trouve. Un net négatif n'est PAS une facture négative : il se nomme,
        # « à reverser au propriétaire », plutôt que de s'afficher en montant dû négatif.
        a_reverser = str(deco.get("sens_net")) == "A_REVERSER"
        if a_reverser:
            titre, valeur = "NET À REVERSER AU PROPRIÉTAIRE", deco.get("montant_a_reverser")
        elif str(deco.get("sens_net")) == "SOLDE":
            titre, valeur = "SOLDE", 0.0
        else:
            titre, valeur = "NET À PAYER", deco.get("net", deco.get("montant_du"))

        # On repart du bas RÉEL de la carte, pas du curseur : les arrondis d'interligne feraient
        # sinon flotter le bandeau d'un dixième de millimètre selon le nombre de postes.
        self.set_y(y_carte + hauteur_carte + 2)
        y_badge = self.get_y()
        self.set_fill_color(*(SAGE if a_reverser else BRIQUE))
        # Coins franchement arrondis, mais plus une pilule : à 12 mm de haut, un rayon de moitié
        # aurait donné une gélule publicitaire.
        self.rect(gauche, y_badge, largeur, hauteur_badge, style="F", round_corners=True,
                  corner_radius=2.4)
        self.set_xy(gauche + PADDING, y_badge)
        self.set_text_color(*BLANC)
        # Le libellé reste discret, le MONTANT porte la taille : c'est le chiffre qu'on cherche.
        self.set_font("Helvetica", "B", 8)
        self.set_char_spacing(0.6)
        self.cell(interne * 0.54, hauteur_badge, _t(titre), 0,
                  new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.set_char_spacing(0)
        self.set_font("Helvetica", "B", 14)
        self.cell(interne * 0.46, hauteur_badge, _t(_montant(valeur)), 0,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
        self.set_text_color(*ESPRESSO)
        self.ln(2)

    # ── Corps ───────────────────────────────────────────────────────────────────────────────────

    def corps(self):
        snap = self.snapshot
        conf = snap.get("conformite") or {}

        self._bloc_parties()
        # Nature de l'opération et bon de commande : mentions réglementaires quand elles sont
        # renseignées. Elles étaient rendues avant la refonte et doivent le rester.
        self.set_font("Helvetica", "", 8.5)
        for libelle, valeur in (("Nature de l'opération", _libelle_nature(conf.get("nature_operation"))),
                                ("Bon de commande", conf.get("numero_bon_commande"))):
            if valeur:
                self.cell(0, 4.6, _t(f"{libelle} : {valeur}"), 0,
                          new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self._tableau_sejours()
        self._tableau_prestations()

        # §43 — le tableau détaillé « Acomptes déjà versés » (date, mode, référence, montant, puis
        # une note réexpliquant ce qu'est un acompte) est SUPPRIMÉ du rendu. Il redisait, en page 2,
        # ce que la ligne « Acompte(s) déjà versé(s) » du récapitulatif dit déjà sur la page
        # principale — et portait jusqu'à un identifiant technique de mouvement. Un document ÉMIS
        # n'est jamais régénéré : ce changement ne vaut que pour les nouveaux rendus.

        # ── Conditions de règlement et mentions ──────────────────────────────────────────────
        # Le libellé des conditions est DÉRIVÉ du délai (« Paiement à réception » quand il vaut 0),
        # jamais saisi en parallèle : un libellé indépendant finirait par contredire l'échéance
        # imprimée juste au-dessus.
        conditions = [
            ("Conditions de règlement", conf.get("conditions_paiement")),
            ("Échéance de paiement", _date_fr(conf.get("date_echeance"))),
        ]
        # Les clauses B2B ne s'impriment que face à un client explicitement PROFESSIONNEL. Les
        # afficher parce qu'elles sont configurées globalement mettrait une mention inadaptée sur
        # la facture d'un particulier — ou pire, une menace de recouvrement sans fondement sur
        # celle d'un client dont le type n'est même pas tranché.
        #
        # L'ESCOMPTE suit la même règle (§29) : la mention « escompte : néant » est une obligation
        # d'information ENTRE PROFESSIONNELS (art. L441-9 du code de commerce). Sur la facture d'un
        # particulier elle n'informe de rien et alourdit le document. On ne la supprime donc pas —
        # on la réserve au cas où elle est due.
        if (conf.get("client") or {}).get("type_client") == "PROFESSIONNEL":
            conditions += [
                ("Escompte", conf.get("conditions_escompte")),
                ("Pénalités de retard", conf.get("taux_penalites_retard")),
                ("Indemnité forfaitaire de recouvrement", conf.get("indemnite_recouvrement")),
            ]
        # ── LA CLÔTURE SE COMPOSE EN DEUX COLONNES (§44) ─────────────────────────────────────
        # Le règlement occupait toute la largeur, et le récapitulatif venait DESSOUS, calé à
        # droite : sur une facture courte, la moitié gauche de la page restait vide sur une
        # quinzaine de centimètres, et le document paraissait inachevé. Les deux blocs partent
        # désormais de la MÊME ordonnée — mentions à gauche, montants à droite. La page se remplit
        # d'elle-même, sans qu'aucun texte ait été rapproché ni aucune police réduite.
        #
        # L'ordre de LECTURE est préservé : le lecteur finit toujours sur le NET, qui reste le
        # bloc le plus bas et le seul en aplat plein.
        retenues = [(l, v) for l, v in conditions if v]
        deco = snap.get("decomposition") or {}

        hauteur_mentions = 4 * (2 if conf.get("mention_tva") else 0) + 16
        hauteur_gauche = 10 + 5 * len(retenues) + hauteur_mentions
        hauteur_droite = (sum(self._hauteur_recapitulatif(deco)) + 2) if deco else 0

        self.ln(2)
        # Le bloc de clôture se déplace ENTIER : le couper laissait une page 2 ne portant que deux
        # lignes et le pied de page.
        #
        # UN SEUL MILLIMÈTRE DE GARDE, et c'est délibéré. `_hauteur_recapitulatif` ne fait pas une
        # estimation : toutes ses lignes ont la même hauteur, donc le total est EXACT. Une garde
        # de 4 mm coûtait une page entière sur une facture qui tenait — mesurée à 0,1 mm près,
        # elle demandait 67,2 mm là où 67,1 restaient. Le millimètre conservé n'absorbe que les
        # arrondis d'interligne.
        self._reserver(max(hauteur_gauche, hauteur_droite) + 1)
        y_colonnes = self.get_y()

        # ── Colonne de gauche : conditions, mentions légales, renvoi au relevé ───────────────
        self._titre_section("Règlement", hauteur_bloc=0, largeur=LARGEUR_CARTE)
        self.set_font("Helvetica", "", 8.5)
        for libelle, valeur in retenues:
            # La mention légale d'escompte est une PHRASE COMPLÈTE (« Escompte pour paiement
            # anticipé : néant »). La préfixer de son propre libellé donnerait « Escompte :
            # Escompte pour… ». On n'ajoute donc le libellé que si la valeur ne le porte pas déjà.
            texte = (str(valeur) if str(valeur).lower().startswith(libelle.lower())
                     else f"{libelle} : {valeur}")
            self.multi_cell(LARGEUR_CARTE, 5, _t(texte), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1.5)

        self.set_font("Helvetica", "I", 7.6)
        self.set_text_color(*PIERRE)
        if conf.get("mention_tva"):
            self.multi_cell(LARGEUR_CARTE, 3.9, _t(conf["mention_tva"]))
            self.ln(1)
        self.multi_cell(LARGEUR_CARTE, 3.9, _t(
            "Cette facture ne reprend que les prestations facturées par la conciergerie. "
            "Le détail des revenus, des acomptes et du solde figure sur le relevé propriétaire "
            "de la même période, qui est un document distinct."))
        self.set_text_color(*ESPRESSO)
        bas_gauche = self.get_y()

        # ── Colonne de droite : le récapitulatif, aligné sur le haut du règlement ────────────
        if deco:
            self._recapitulatif(deco, y_depart=y_colonnes)
        self.set_y(max(bas_gauche, self.get_y()))


def _neutraliser_metadonnees(pdf: FPDF) -> None:
    """Sans cela, fpdf insère l'horodatage courant : deux générations du même snapshot
    produiraient des hash différents, ce qui rendrait le contrôle d'intégrité inutilisable.

    La date de création est donc figée à une constante (et non supprimée : fpdf2 exige une date
    horodatée). La date qui fait foi métier est `date_facture`, portée par le snapshot et imprimée
    sur le document.
    """
    pdf.creation_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
    for attr, valeur in (("title", "Facture proprietaire"), ("author", "Pilotage Conciergerie"),
                         ("subject", ""), ("keywords", "")):
        try:
            setattr(pdf, attr, valeur)
        except Exception:
            pass


def rendre(snapshot: dict[str, Any]) -> bytes:
    """Octets du PDF pour un snapshot donné. Déterministe : même snapshot → mêmes octets."""
    pdf = _Facture(snapshot)
    # « Page 1/3 » exige de connaître le nombre total de pages, donc un alias résolu à la fin.
    pdf.alias_nb_pages()
    _neutraliser_metadonnees(pdf)
    pdf.add_page()
    pdf.corps()
    sortie = pdf.output()
    return sortie.encode("latin-1") if isinstance(sortie, str) else bytes(sortie)


def ecrire(snapshot: dict[str, Any], repertoire: Path) -> tuple[str, str]:
    """Écrit le PDF sous `<repertoire>/<AAAA>/<MM>/` et renvoie (nom_fichier, sha256).

    Ne renvoie jamais de chemin absolu : le stockage est configurable, seul le nom est persisté.
    """
    octets = rendre(snapshot)
    empreinte = hashlib.sha256(octets).hexdigest()

    mois = str(snapshot.get("mois") or "0000-00")
    annee, _, mm = mois.partition("-")
    cible = Path(repertoire) / annee / (mm or "00")
    cible.mkdir(parents=True, exist_ok=True)

    numero = str(snapshot.get("numero_facture") or snapshot.get("facture_id_opaque") or "SANS_NUM")
    nom = f"{numero.replace('/', '-')}.pdf"
    (cible / nom).write_bytes(octets)
    return nom, empreinte


def fabrique(repertoire: Path):
    """Adaptateur pour `factures_proprietaires_service.emettre(generer_pdf=...)`."""
    def _generer(snapshot: dict[str, Any]) -> tuple[str, str]:
        return ecrire(snapshot, repertoire)
    return _generer

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


# Les polices de base (Helvetica) sont encodées en latin-1. Les caractères typographiques
# courants y sont absents et deviendraient des "?" sur le document : on les translittère vers
# leur équivalent ASCII avant l'encodage. Les accents, eux, existent en latin-1 et sont conservés.
_TRANSLITTERATION = str.maketrans({
    "—": "-", "–": "-", "’": "'", "‘": "'", "“": '"', "”": '"',
    "€": "EUR", "…": "...", " ": " ", " ": " ",
})


def _t(v: Any) -> str:
    """Texte prêt pour une police latin-1, sans caractère de remplacement visible."""
    s = "" if v is None else str(v)
    return s.translate(_TRANSLITTERATION).encode("latin-1", "replace").decode("latin-1")


def _montant(v: Any) -> str:
    return f"{float(v or 0):,.2f}".replace(",", " ").replace(".", ",") + " EUR"


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
BLANC = (0xFF, 0xFF, 0xFF)

# Le logo officiel du site, copié dans les assets de l'application (`static/img/brand/`). Aucun
# logo n'est redessiné : si le fichier manque, le document porte le nom en toutes lettres plutôt
# qu'un substitut graphique inventé.
LOGO = Path(__file__).resolve().parent.parent / "static" / "img" / "brand" / "chouette-logo.png"

MARGE = 15.0
LARGEUR_UTILE = 210.0 - 2 * MARGE

# Regroupement des types de ligne, identique à celui du service de composition. Redéfini ici plutôt
# qu'importé pour garder ce module SANS dépendance applicative : il ne reçoit qu'un snapshot et doit
# rester capable de rendre un document archivé, même si les services évoluent. Le test
# `test_pdf_groupes_alignes_sur_le_service` verrouille l'égalité des deux tables.
_GROUPES_PDF: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("commissions", "Commissions", ("COMMISSION_CONCIERGERIE",)),
    ("menages", "Menages", ("MENAGE_FACTURE",)),
    ("canape", "Canape", ("PREPARATION_CANAPE",)),
    ("forfait", "Forfait", ("CHARGE_FIXE",)),
    ("refacturations", "Refacturation", ("CHARGES_EXCEPT_REFAC", "CHARGE_REFACTUREE")),
    ("extras", "Extra", ("EXTRA",)),
    ("reductions", "Reduction", ("REDUCTION",)),
)


class _Facture(FPDF):
    def __init__(self, snapshot: dict[str, Any]):
        super().__init__(orientation="P", unit="mm", format="A4")
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
        snap = self.snapshot
        haut = 12.0
        if LOGO.exists():
            # Le logo est plus haut que large (256x384) : on borne la HAUTEUR et laissons fpdf
            # déduire la largeur, sinon il serait étiré.
            try:
                self.image(str(LOGO), x=MARGE, y=haut, h=22)
            except Exception:      # noqa: BLE001 — un logo illisible ne doit pas empêcher la facture
                pass
        self.set_xy(MARGE + 20, haut + 1)
        self.set_font("Helvetica", "B", 17)
        self.set_text_color(*BRIQUE)
        self.cell(70, 8, _t(snap.get("emetteur", {}).get("nom") or "Chouette Patrimoine"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(MARGE + 20)
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(*PIERRE)
        self.cell(70, 4, _t("Conciergerie de location courte duree"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Pavé titre, aligné à droite
        self.set_xy(120, haut)
        self.set_font("Helvetica", "B", 22)
        self.set_text_color(*ESPRESSO)
        self.cell(LARGEUR_UTILE - 105, 11, _t("AVOIR" if est_avoir else "FACTURE"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*PIERRE)
        for texte in self._references():
            self.set_x(120)
            self.cell(LARGEUR_UTILE - 105, 4.5, _t(texte), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

        self.set_y(max(self.get_y(), haut + 24))
        self.set_draw_color(*TERRACOTTA)
        self.set_line_width(0.8)
        self.line(MARGE, self.get_y(), 210 - MARGE, self.get_y())
        self.set_line_width(0.2)
        self.set_text_color(*ESPRESSO)
        self.ln(4)

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
        out.append(f"Periode des prestations : du {_date_fr(debut)} au {_date_fr(fin)}"
                   if debut and fin else f"Periode des prestations : {snap.get('mois', '')}")
        statut = snap.get("statut")
        if statut and statut != "EMIS":
            out.append(f"Statut : {statut}")
        if snap.get("type_document") == "AVOIR" and snap.get("facture_origine"):
            out.append(f"Avoir sur : {snap['facture_origine']}")
        return out

    # ── Pied de page ────────────────────────────────────────────────────────────────────────────

    def footer(self):
        self.set_y(-24)
        self.set_draw_color(*SABLE)
        self.line(MARGE, self.get_y(), 210 - MARGE, self.get_y())
        self.ln(1.5)
        self.set_font("Helvetica", "", 6.8)
        self.set_text_color(*PIERRE)
        for ligne in self._mentions_pied():
            self.cell(0, 3.2, _t(ligne), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.set_font("Helvetica", "", 6.5)
        self.cell(0, 3.2, _t(f"Page {self.page_no()}/{{nb}}"), 0,
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

    def _titre_section(self, texte: str):
        """Un titre ne doit jamais rester seul en bas de page : on force la coupe s'il ne reste pas
        de quoi afficher au moins l'en-tête du tableau et une ligne."""
        if self.get_y() > 297 - 26 - 22:
            self.add_page()
        self.ln(2)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*BRIQUE)
        self.cell(0, 6, _t(texte), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*ESPRESSO)
        self.set_draw_color(*SABLE)
        self.line(MARGE, self.get_y(), 210 - MARGE, self.get_y())
        self.ln(2)

    def _ligne_entete(self, colonnes: tuple, hauteur: float = 7.0):
        """En-tête de tableau. Mémorisé pour être RÉPÉTÉ automatiquement en haut de chaque page
        suivante (cf. `header`) — sans quoi un tableau long deviendrait illisible dès la page 2."""
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(*TERRACOTTA)
        self.set_text_color(*BLANC)
        self.set_draw_color(*TERRACOTTA)
        for i, (largeur, titre, align) in enumerate(colonnes):
            dernier = i == len(colonnes) - 1
            self.cell(largeur, hauteur, _t(titre), 0,
                      new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                      new_y=YPos.NEXT if dernier else YPos.TOP, align=align, fill=True)
        self.set_text_color(*ESPRESSO)
        self.set_draw_color(*SABLE)

    def _note(self, texte: str):
        """Note explicative sous un tableau. Coupe la page AVANT d'écrire si la place manque —
        une note tronquée par le saut automatique perdrait justement l'explication qu'elle porte
        (constaté sur le premier rendu : la phrase distinguant acompte et réduction était coupée)."""
        lignes_estimees = max(1, len(texte) // 110 + 1)
        if self.get_y() + lignes_estimees * 3.6 + 4 > 297 - 26:
            self.add_page()
        self.ln(1)
        self.set_font("Helvetica", "I", 7.5)
        self.set_text_color(*PIERRE)
        self.multi_cell(0, 3.6, _t(texte))
        self.set_text_color(*ESPRESSO)

    def _ligne_tableau(self, colonnes: tuple, valeurs: tuple, pair: bool, hauteur: float = 6.2):
        self.set_font("Helvetica", "", 8)
        self.set_fill_color(*(CREME if pair else BLANC))
        for i, ((largeur, _, align), valeur) in enumerate(zip(colonnes, valeurs)):
            dernier = i == len(colonnes) - 1
            self.cell(largeur, hauteur, _t(valeur), "B",
                      new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                      new_y=YPos.NEXT if dernier else YPos.TOP, align=align, fill=True)

    def _bloc_parties(self):
        snap = self.snapshot
        conf = snap.get("conformite") or {}
        em, cl = conf.get("emetteur") or {}, conf.get("client") or {}
        base, dest = snap.get("emetteur", {}), snap.get("destinataire", {})

        gauche = [x for x in (em.get("adresse_siege") or base.get("adresse"),
                              em.get("contact") or base.get("contact")) if x]
        # Les identifiants du CLIENT sont obligatoires dès qu'il est professionnel — les omettre
        # rendrait la facture non conforme. Ils ne s'impriment que s'ils sont renseignés, et sous
        # leur propre étiquette (un SIREN client n'est pas davantage un SIRET que celui de
        # l'émetteur).
        droite = [x for x in (cl.get("denomination") or dest.get("nom"),
                              cl.get("adresse_facturation") or cl.get("adresse")
                              or dest.get("adresse"),
                              f"SIREN {cl['siren']}" if cl.get("siren") else None,
                              f"SIRET {cl['siret']}" if cl.get("siret") else None,
                              f"TVA {cl['tva_intra']}" if cl.get("tva_intra") else None,
                              f"Reference : {snap.get('proprietaire_id', '')}",
                              f"Logement : {snap.get('logement_id', '')}") if x]
        depart = self.get_y()
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*PIERRE)
        self.cell(88, 4.5, _t("EMETTEUR"), 0, new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(0, 4.5, _t("FACTURE A"), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*ESPRESSO)
        self.set_font("Helvetica", "", 8.5)
        for i in range(max(len(gauche), len(droite))):
            self.cell(88, 4.4, _t(gauche[i] if i < len(gauche) else ""), 0,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(0, 4.4, _t(droite[i] if i < len(droite) else ""), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_y(max(self.get_y(), depart + 4.5))
        self.ln(1)

    def _tableau_sejours(self):
        """Séjours de la période et commission de chacun.

        AUCUNE donnée personnelle du voyageur n'est imprimée : ni nom, ni e-mail, ni téléphone. La
        facture doit pouvoir circuler (comptable, propriétaire, archivage) sans emporter de PII.
        La référence de réservation suffit à retrouver le séjour dans l'application.
        """
        reservations = self.snapshot.get("reservations") or []
        if not reservations:
            return
        self._titre_section("Sejours de la periode et commission")
        # Assiette x Taux = Commission, colonne par colonne : le propriétaire doit pouvoir refaire
        # l'operation de tete pour chaque sejour, sans avoir a nous croire sur parole.
        colonnes = ((21, "Arrivee", "C"), (21, "Depart", "C"), (11, "Nuits", "C"),
                    (22, "Canal", "L"), (28, "Reference", "L"), (24, "Assiette", "R"),
                    (15, "Taux", "R"), (23, "Commission", "R"))
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
                r.get("plateforme") or "", r.get("reservation_id") or "",
                _montant(r.get("assiette_commission")) if r.get("assiette_commission") is not None
                else (_montant(r.get("payout")) if r.get("payout") is not None else ""),
                _taux(taux), _montant(commission) if commission is not None else "",
            ), pair=(i % 2 == 0))
        self._entete_tableau = None
        if total_commission:
            self.set_font("Helvetica", "B", 8)
            self.cell(sum(c[0] for c in colonnes[:-1]), 6, _t("Total commissions"), 0,
                      new_x=XPos.RIGHT, new_y=YPos.TOP, align="R")
            self.cell(colonnes[-1][0], 6, _t(_montant(total_commission)), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
        self._note(f"{len(reservations)} sejour(s). Assiette = payout de la plateforme diminue du "
                   "menage ; commission = assiette x taux. Ces montants proviennent du calcul "
                   "mensuel, ils ne sont pas recalcules sur la facture.")

    def _tableau_prestations(self):
        """Détail facturé, groupé par poste. C'est la lecture « d'où vient le montant »."""
        snap = self.snapshot
        deco = snap.get("decomposition") or {}
        self._titre_section("Detail des frais")
        colonnes = ((12, "N", "C"), (108, "Designation", "L"), (30, "Poste", "L"),
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
        if deco:
            self._recapitulatif(deco)

    def _recapitulatif(self, deco: dict[str, Any]):
        """Le récapitulatif EST la formule. Chaque poste apparaît même à zéro dès qu'il porte une
        ligne, pour qu'on puisse suivre l'addition sans deviner ce qui a été omis."""
        self.ln(3)
        if self.get_y() > 297 - 26 - 46:
            self.add_page()
        gauche, largeur = 105.0, 75.0

        def ligne(libelle: str, valeur: Any, gras: bool = False, couleur=None):
            self.set_x(gauche)
            self.set_font("Helvetica", "B" if gras else "", 9 if gras else 8.5)
            if couleur:
                self.set_text_color(*couleur)
            self.cell(largeur * 0.6, 5.2, _t(libelle), 0, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(largeur * 0.4, 5.2, _t(_montant(valeur)), 0,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
            self.set_text_color(*ESPRESSO)

        for poste in deco.get("postes", []):
            if poste.get("cle") == "reductions" or not poste.get("nb"):
                continue
            ligne(poste["libelle"], poste["montant"])
        self.set_draw_color(*SABLE)
        self.line(gauche, self.get_y() + 0.5, gauche + largeur, self.get_y() + 0.5)
        self.ln(1.5)
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
        ligne("TOTAL TTC", total_ttc, gras=True)

        if deco.get("total_acomptes"):
            ligne("Acomptes deja verses", -float(deco["total_acomptes"]))

        # Bandeau du montant dû : le seul élément coloré plein du document, pour qu'il soit le
        # premier chiffre que l'oeil trouve.
        self.ln(1)
        self.set_x(gauche)
        self.set_fill_color(*BRIQUE)
        self.set_text_color(*BLANC)
        self.set_font("Helvetica", "B", 11)
        self.cell(largeur * 0.6, 9, _t("  MONTANT DU"), 0, new_x=XPos.RIGHT, new_y=YPos.TOP,
                  fill=True)
        self.cell(largeur * 0.4, 9, _t(_montant(deco.get("montant_du")) + "  "), 0,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R", fill=True)
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
        for libelle, valeur in (("Nature de l'operation", conf.get("nature_operation")),
                                ("Bon de commande", conf.get("numero_bon_commande"))):
            if valeur:
                self.cell(0, 4.6, _t(f"{libelle} : {valeur}"), 0,
                          new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self._tableau_sejours()
        self._tableau_prestations()

        acomptes = (snap.get("decomposition") or {}).get("acomptes") or []
        if acomptes:
            self._titre_section("Acomptes deja verses")
            colonnes = ((34, "Date", "C"), (36, "Mode", "L"), (80, "Reference", "L"),
                        (30, "Montant", "R"))
            self._entete_tableau = (colonnes,)
            self._ligne_entete(colonnes)
            for i, a in enumerate(acomptes):
                self._ligne_tableau(colonnes, (
                    _date_fr(a.get("date_mouvement")), a.get("mode_reglement") or "",
                    a.get("mouvement_opaque") or a.get("reference_metier") or "",
                    _montant(a.get("montant")),
                ), pair=(i % 2 == 0))
            self._entete_tableau = None
            self._note("Un acompte est un paiement deja recu : il reduit ce qui reste a payer, pas "
                       "le montant facture. Une reduction commerciale, elle, diminue le montant "
                       "facture et figure dans le detail des frais.")

        # ── Conditions de règlement et mentions ──────────────────────────────────────────────
        self.ln(2)
        self.set_font("Helvetica", "", 8.5)
        conditions = [
            ("Echeance de paiement", _date_fr(conf.get("date_echeance"))),
            ("Delai de paiement", (f"{conf['delai_paiement_jours']} jours"
                                   if conf.get("delai_paiement_jours") else "")),
            ("Conditions d'escompte", conf.get("conditions_escompte")),
        ]
        # Les clauses B2B ne s'impriment que face à un client explicitement PROFESSIONNEL. Les
        # afficher parce qu'elles sont configurées globalement mettrait une mention inadaptée sur
        # la facture d'un particulier — ou pire, sur celle d'un client dont le type n'est pas
        # encore tranché.
        if (conf.get("client") or {}).get("type_client") == "PROFESSIONNEL":
            conditions += [
                ("Penalites de retard", conf.get("taux_penalites_retard")),
                ("Indemnite forfaitaire de recouvrement", conf.get("indemnite_recouvrement")),
            ]
        for libelle, valeur in conditions:
            if valeur:
                self.cell(0, 5, _t(f"{libelle} : {valeur}"), new_x=XPos.LMARGIN,
                          new_y=YPos.NEXT)
        self.ln(2)

        self.set_font("Helvetica", "I", 8)
        if conf.get("mention_tva"):
            self.multi_cell(0, 4, _t(conf["mention_tva"]))
            self.ln(1)
        self.multi_cell(0, 4, _t(
            "Cette facture ne reprend que les prestations facturees par la conciergerie. "
            "Le detail des revenus, des acomptes et du solde figure sur le releve proprietaire "
            "de la meme periode, qui est un document distinct."))


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

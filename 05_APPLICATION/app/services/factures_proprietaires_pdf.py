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


class _Facture(FPDF):
    def __init__(self, snapshot: dict[str, Any]):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.snapshot = snapshot
        self.set_auto_page_break(auto=True, margin=20)
        self.set_compression(False)   # sortie stable, indépendante de la version de zlib

    def header(self):
        snap = self.snapshot
        emetteur = snap.get("emetteur", {})
        est_avoir = snap.get("type_document") == "AVOIR"

        self.set_font("Helvetica", "B", 16)
        self.cell(0, 10, _t("AVOIR" if est_avoir else "FACTURE"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, _t(f"Numero : {snap.get('numero_facture', '')}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.cell(0, 5, _t(f"Date : {snap.get('date_facture', '')}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if est_avoir and snap.get("facture_origine"):
            self.cell(0, 5, _t(f"Avoir sur la facture : {snap['facture_origine']}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(3)

        self.set_font("Helvetica", "B", 10)
        self.cell(95, 5, _t("Emetteur"), 0, new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(0, 5, _t("Destinataire"), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("Helvetica", "", 9)
        dest = snap.get("destinataire", {})
        gauche = [emetteur.get("nom"), emetteur.get("adresse"),
                  f"SIRET {emetteur.get('siret', '')}"]
        droite = [dest.get("nom"), dest.get("adresse"),
                  f"Reference proprietaire : {snap.get('proprietaire_id', '')}"]
        for g, d in zip(gauche, droite):
            self.cell(95, 5, _t(g or ""), 0, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(0, 5, _t(d or ""), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(4)

    def footer(self):
        self.set_y(-18)
        self.set_font("Helvetica", "I", 7)
        mention = ("TVA non applicable — regime declare par l'emetteur."
                   if self.snapshot.get("regime_tva", "").startswith("NON_ASSUJETTI")
                   else "")
        self.cell(0, 4, _t(mention), 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.cell(0, 4, _t(f"Document genere par Pilotage Conciergerie — "
                           f"reference interne {self.snapshot.get('facture_id_opaque', '')}"),
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")

    def corps(self):
        snap = self.snapshot
        self.set_font("Helvetica", "B", 10)
        self.cell(0, 6, _t(f"Periode : {snap.get('mois', '')}    "
                           f"Logement : {snap.get('logement_id', '')}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(235, 235, 235)
        self.cell(15, 7, _t("N"), 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align="C", fill=True)
        self.cell(120, 7, _t("Prestation"), 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align="L", fill=True)
        self.cell(45, 7, _t("Montant"), 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R", fill=True)

        self.set_font("Helvetica", "", 9)
        for l in snap.get("lignes", []):
            self.cell(15, 7, _t(l.get("numero_ligne")), 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align="C")
            self.cell(120, 7, _t(l.get("libelle")), 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align="L")
            self.cell(45, 7, _t(_montant(l.get("montant"))), 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

        self.set_font("Helvetica", "B", 10)
        self.cell(135, 8, _t("TOTAL"), 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align="R")
        self.cell(45, 8, _t(_montant(snap.get("montant_total"))), 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")
        self.ln(6)

        self.set_font("Helvetica", "I", 8)
        self.multi_cell(0, 4, _t(
            "Cette facture ne reprend que les prestations facturees par la conciergerie. "
            "Le detail des revenus, des acomptes et du solde figure sur le releve propriétaire "
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
    """Octets du PDF pour un snapshot donné. Déterministe."""
    pdf = _Facture(snapshot)
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

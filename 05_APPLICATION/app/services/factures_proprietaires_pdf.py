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


def _date_fr(v: Any) -> str:
    """`2026-07-31` → `31/07/2026`. Renvoie la valeur telle quelle si le format est inattendu."""
    s = "" if v is None else str(v)[:10]
    parties = s.split("-")
    return f"{parties[2]}/{parties[1]}/{parties[0]}" if len(parties) == 3 else s


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
        conf = snap.get("conformite") or {}
        em = conf.get("emetteur") or {}
        cl = conf.get("client") or {}

        # Seules les informations réellement renseignées sont imprimées : aucune ligne d'identité
        # n'est fabriquée pour « remplir » le document.
        gauche = [em.get("denomination") or emetteur.get("nom"),
                  em.get("forme_juridique"),
                  f"Capital : {em['capital']}" if em.get("capital") else None,
                  em.get("adresse_siege") or emetteur.get("adresse"),
                  f"SIREN {em['siren']}" if em.get("siren") else None,
                  f"RCS {em['rcs']}" if em.get("rcs") else None,
                  f"TVA {em['tva_intra']}" if em.get("tva_intra") else None,
                  em.get("contact") or None]
        droite = [cl.get("denomination") or dest.get("nom"),
                  cl.get("adresse_facturation") or cl.get("adresse") or dest.get("adresse"),
                  f"SIREN {cl['siren']}" if cl.get("siren") else None,
                  f"TVA {cl['tva_intra']}" if cl.get("tva_intra") else None,
                  f"Reference proprietaire : {snap.get('proprietaire_id', '')}"]
        gauche = [x for x in gauche if x]
        droite = [x for x in droite if x]
        while len(gauche) < len(droite):
            gauche.append("")
        while len(droite) < len(gauche):
            droite.append("")
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
        conf = snap.get("conformite") or {}

        self.set_font("Helvetica", "B", 10)
        debut, fin = conf.get("periode_debut"), conf.get("periode_fin")
        periode = (f"Periode des prestations : du {_date_fr(debut)} au {_date_fr(fin)}"
                   if debut and fin else f"Periode : {snap.get('mois', '')}")
        self.cell(0, 6, _t(periode), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, _t(f"Reference prestation : logement {snap.get('logement_id', '')}"),
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if conf.get("nature_operation"):
            self.cell(0, 5, _t(f"Nature de l'operation : {conf['nature_operation']}"),
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if conf.get("numero_bon_commande"):
            self.cell(0, 5, _t(f"Bon de commande : {conf['numero_bon_commande']}"),
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(3)

        # ── Tableau des prestations ──────────────────────────────────────────────────────────
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(235, 235, 235)
        for largeur, titre, align in ((10, "N", "C"), (78, "Designation", "L"),
                                      (16, "Qte", "R"), (26, "PU HT", "R"),
                                      (28, "Total HT", "R"), (22, "TVA", "R")):
            dernier = titre == "TVA"
            self.cell(largeur, 7, _t(titre), 1,
                      new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                      new_y=YPos.NEXT if dernier else YPos.TOP, align=align, fill=True)

        self.set_font("Helvetica", "", 9)
        taux = float(conf.get("taux_tva") or 0)
        for l in snap.get("lignes", []):
            montant = float(l.get("montant") or 0)
            qte = float(l.get("quantite") or 1)
            pu = float(l.get("prix_unitaire_ht") if l.get("prix_unitaire_ht") is not None
                       else (montant / qte if qte else montant))
            tva_ligne = round(montant * taux / 100.0, 2) if taux else 0.0
            for largeur, valeur, align in (
                    (10, l.get("numero_ligne"), "C"), (78, l.get("libelle"), "L"),
                    (16, f"{qte:g}", "R"), (26, _montant(pu), "R"),
                    (28, _montant(montant), "R"), (22, _montant(tva_ligne), "R")):
                dernier = largeur == 22
                self.cell(largeur, 7, _t(valeur), 1,
                          new_x=XPos.LMARGIN if dernier else XPos.RIGHT,
                          new_y=YPos.NEXT if dernier else YPos.TOP, align=align)

        # ── Totaux ───────────────────────────────────────────────────────────────────────────
        self.ln(2)
        total_ht = conf.get("total_ht", snap.get("montant_total"))
        total_tva = conf.get("total_tva", 0.0)
        total_ttc = conf.get("total_ttc", snap.get("montant_total"))
        for libelle, valeur, gras in (("TOTAL HT", total_ht, False), ("TVA", total_tva, False),
                                      ("TOTAL TTC", total_ttc, True)):
            self.set_font("Helvetica", "B" if gras else "", 10 if gras else 9)
            self.cell(130, 7, _t(libelle), 0, new_x=XPos.RIGHT, new_y=YPos.TOP, align="R")
            self.cell(50, 7, _t(_montant(valeur)), 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT,
                      align="R")
        self.ln(4)

        # ── Conditions de règlement et mentions ──────────────────────────────────────────────
        self.set_font("Helvetica", "", 9)
        for libelle, valeur in (
                ("Echeance de paiement", _date_fr(conf.get("date_echeance"))),
                ("Delai de paiement", (f"{conf['delai_paiement_jours']} jours"
                                       if conf.get("delai_paiement_jours") else "")),
                ("Conditions d'escompte", conf.get("conditions_escompte")),
                ("Penalites de retard", conf.get("taux_penalites_retard")),
                ("Indemnite forfaitaire de recouvrement", conf.get("indemnite_recouvrement"))):
            # Les clauses B2B ne sont imprimées que si elles ont été configurées : une facture
            # adressée a un particulier n'affiche pas de clause professionnelle inadaptée.
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

# -*- coding: utf-8 -*-
"""Reconstruction GÉOMÉTRIQUE d'une facture PDF à texte natif — indépendante du fournisseur.

POURQUOI UNE COUCHE GÉOMÉTRIQUE
Le flux texte d'un PDF ne suit pas l'ordre visuel : dans une même facture, le prix d'une ligne est
rendu tantôt au-dessus de sa description, tantôt à sa hauteur, tantôt en dessous, et un libellé
coupé sur deux lignes voit parfois son prix s'intercaler entre ses deux moitiés. Lire le texte « à
la suite » mélangeait donc les prix aux libellés et rattachait un montant à la ligne voisine.

Ce module ne lit que la GÉOMÉTRIE — mots, coordonnées, pages — et rend une structure :

    mots (toutes pages) → lignes visuelles → colonnes → enregistrements (une prestation chacun)
                                                      → montants rattachés à leur enregistrement
                                                      → pavé récapitulatif (Base HT, TTC, net)

Il ne connaît AUCUN fournisseur. Les colonnes se déduisent des couloirs blancs verticaux du
tableau ; leur rôle (libellé, quantité, prix, total) se déduit de leur contenu. Un enregistrement
se reconnaît à la numérotation du document quand elle existe (« 1. », « 2. »…), sinon à ses
lignes de libellé. Les montants sont appariés aux enregistrements par un ALIGNEMENT ORDONNÉ
(programmation dynamique sur la distance verticale) : un prix ne peut pas « sauter » une ligne,
et un prix sans ligne ou une ligne sans prix ressort comme tel — jamais absorbé en silence.

L'interprétation métier (nature de la prestation, dates, logement) est faite par l'appelant
(`lib_menages_externes_pdf`). Aucun OCR, aucune IA : PyMuPDF seulement.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

# Décalage vertical par page : les pages se suivent sur un axe unique sans jamais se chevaucher.
PAGE_OFFSET = 100000.0

_ESPACES = "".join(chr(c) for c in (0xA0, 0x202F, 0x2009, 0x2007))

# Vocabulaire d'EN-TÊTE de tableau de facture (pas d'un fournisseur) : il faut au moins deux de
# ces mots, distincts, sur une même ligne pour qu'elle soit prise pour l'en-tête.
_MOTS_ENTETE = {"description", "designation", "libelle", "prestation", "prestations", "montant",
                "total", "prix", "unitaire", "pu", "quantite", "qte", "nombres", "nombre", "ttc",
                "ht"}

# Début du pavé récapitulatif ou du pied de facture : le tableau des prestations s'arrête là.
_RE_FIN_TABLEAU = re.compile(
    r"sous[\s-]*total|total\s*(?:ttc|ht)\b|base\s*ht\b|net\s*a\s*payer|montant\s*tva"
    r"|\bt\s*\.?\s*v\s*\.?\s*a\s*\.|conditions\s+de\s+paiement|mode\s+de\s+paiement")

# Une ligne de pied de page (identifiants légaux, banque) n'est jamais le texte d'une prestation.
_RE_PIED = re.compile(r"\b(?:siret|siren|iban|rcs|bic)\b")

# Numéro de ligne du document : « 1. », « 12. », ou collé au mot suivant (« 4.service »).
_RE_NUMERO = re.compile(r"^(\d{1,2})\s*\.(.*)$")

# Mot « montant » : un nombre, éventuellement suivi de €, ou € seul, ou un multiplicateur.
_RE_MOT_CHIFFRE = re.compile(r"^(?:[x×]?\d[\d.,]*€?|€|[x×])$", re.IGNORECASE)

# Libellés du pavé récapitulatif, en suites de mots normalisés.
LIBELLES_RECAP = {
    "net_a_payer": (("net", "a", "payer"),),
    "total_ttc": (("total", "ttc"),),
    "sous_total": (("sous-total",), ("sous", "total")),
    "total_ht": (("total", "ht"),),
    "base_ht": (("base", "ht"),),
}


def normaliser_mot(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")
    for c in _ESPACES:
        s = s.replace(c, " ")
    return s.lower().replace("’", "'").strip(" :;")


@dataclass
class Mot:
    page: int
    x0: float
    y0: float
    x1: float
    y1: float
    texte: str

    @property
    def yc(self) -> float:
        return (self.page - 1) * PAGE_OFFSET + (self.y0 + self.y1) / 2

    @property
    def h(self) -> float:
        return self.y1 - self.y0

    @property
    def xc(self) -> float:
        return (self.x0 + self.x1) / 2


@dataclass
class LigneVisuelle:
    mots: list[Mot]

    @property
    def page(self) -> int:
        return self.mots[0].page

    @property
    def yc(self) -> float:
        return sum(m.yc for m in self.mots) / len(self.mots)

    @property
    def h(self) -> float:
        return max(m.h for m in self.mots)

    @property
    def haut(self) -> float:
        return self.yc - self.h / 2

    @property
    def bas(self) -> float:
        return self.yc + self.h / 2

    @property
    def x0(self) -> float:
        return min(m.x0 for m in self.mots)

    @property
    def texte(self) -> str:
        return " ".join(m.texte for m in self.mots)

    @property
    def texte_normalise(self) -> str:
        return " ".join(normaliser_mot(m.texte) for m in self.mots)


@dataclass
class Colonne:
    x0: float
    x1: float
    role: str = ""          # LIBELLE | QUANTITE | PRIX | TOTAL | MONTANT
    mots: list[Mot] = field(default_factory=list)


@dataclass
class Evenement:
    """Une rangée de valeurs chiffrées (hors libellé), lue à travers les colonnes non-libellé."""
    yc: float
    page: int
    valeurs: dict[str, str]           # rôle de colonne → texte


@dataclass
class Enregistrement:
    """Une prestation du document : ses lignes de libellé, ses quantités, ses montants."""
    numero: int | None
    lignes: list[LigneVisuelle]
    lignes_quantite: list[LigneVisuelle] = field(default_factory=list)
    evenements: list[Evenement] = field(default_factory=list)

    @property
    def page(self) -> int:
        return self.lignes[0].page if self.lignes else 1

    @property
    def haut(self) -> float:
        return min(l.haut for l in self.lignes + self.lignes_quantite)

    @property
    def bas(self) -> float:
        return max(l.bas for l in self.lignes + self.lignes_quantite)

    @property
    def texte(self) -> str:
        return " ".join(l.texte for l in self.lignes).strip()


@dataclass
class TableauReconstruit:
    mode: str                                     # NUMEROTE | LIBELLES | AUCUN
    enregistrements: list[Enregistrement] = field(default_factory=list)
    evenements_orphelins: list[Evenement] = field(default_factory=list)
    textes_non_rattaches: list[str] = field(default_factory=list)
    colonnes: list[Colonne] = field(default_factory=list)


# ── Lecture ──────────────────────────────────────────────────────────────────────────────────────

def lire_mots(doc) -> list[Mot]:
    """TOUS les mots de TOUTES les pages, avec leurs coordonnées."""
    mots = []
    for ip, page in enumerate(doc):
        for w in page.get_text("words"):
            texte = str(w[4])
            for c in _ESPACES:
                texte = texte.replace(c, " ")
            texte = texte.strip()
            if texte:
                mots.append(Mot(ip + 1, float(w[0]), float(w[1]), float(w[2]), float(w[3]), texte))
    return mots


def grouper_lignes(mots: list[Mot]) -> list[LigneVisuelle]:
    """Regroupe des mots en lignes visuelles : même page, centres verticaux proches.

    La tolérance est relative à la hauteur des mots (la moitié de la plus petite des deux), pour
    qu'un prix rendu 4 points plus haut que son libellé reste distinct d'une ligne de libellé.
    """
    lignes: list[list[Mot]] = []
    for m in sorted(mots, key=lambda m: (m.yc, m.x0)):
        if lignes:
            der = lignes[-1]
            yc = sum(x.yc for x in der) / len(der)
            tol = 0.5 * min(m.h, min(x.h for x in der))
            if der[0].page == m.page and abs(m.yc - yc) <= tol:
                der.append(m)
                continue
        lignes.append([m])
    return [LigneVisuelle(sorted(l, key=lambda m: m.x0)) for l in lignes]


def trouver_entete(lignes: list[LigneVisuelle]) -> int | None:
    for i, l in enumerate(lignes):
        mots = set(re.split(r"[^a-z]+", l.texte_normalise))
        if len(mots & _MOTS_ENTETE) >= 2:
            return i
    return None


def trouver_fin_tableau(lignes: list[LigneVisuelle], debut: int) -> int:
    for i in range(debut, len(lignes)):
        if _RE_FIN_TABLEAU.search(lignes[i].texte_normalise):
            return i
    return len(lignes)


# ── Colonnes ─────────────────────────────────────────────────────────────────────────────────────

def _est_chiffre(m: Mot) -> bool:
    return bool(_RE_MOT_CHIFFRE.match(m.texte.replace(" ", "")))


def detecter_colonnes(lignes: list[LigneVisuelle], ecart_min: float = 12.0) -> list[Colonne]:
    """Colonnes = intervalles horizontaux séparés par un couloir blanc vertical.

    On fusionne les intervalles [x0, x1] de tous les mots du corps du tableau ; un vide d'au moins
    `ecart_min` points sur TOUTE la hauteur du tableau sépare deux colonnes. Le rôle de chaque
    colonne se lit ensuite dans son contenu : la plus « littéraire » porte les libellés ; à sa
    droite, les colonnes chiffrées portent les prix (une seule : montant, parfois « PU x N » ;
    deux : prix unitaire puis total) ; à sa gauche, une colonne d'entiers porte les quantités.
    """
    mots = [m for l in lignes for m in l.mots]
    if not mots:
        return []
    intervalles = sorted((m.x0, m.x1) for m in mots)
    fusion = [list(intervalles[0])]
    for a, b in intervalles[1:]:
        if a - fusion[-1][1] < ecart_min:
            fusion[-1][1] = max(fusion[-1][1], b)
        else:
            fusion.append([a, b])
    colonnes = [Colonne(a, b) for a, b in fusion]
    for m in mots:
        for c in colonnes:
            if c.x0 - 0.1 <= m.x0 <= c.x1 + 0.1:
                c.mots.append(m)
                break

    def lettres(c: Colonne) -> int:
        return sum(1 for m in c.mots if not _est_chiffre(m) and re.search(r"[A-Za-zÀ-ÿ]", m.texte))

    i_lib = max(range(len(colonnes)), key=lambda i: lettres(colonnes[i]))
    colonnes[i_lib].role = "LIBELLE"
    a_droite = [c for c in colonnes[i_lib + 1:]
                if c.mots and sum(_est_chiffre(m) for m in c.mots) >= len(c.mots) / 2]
    if len(a_droite) == 1:
        a_droite[0].role = "MONTANT"
    elif len(a_droite) >= 2:
        a_droite[-2].role, a_droite[-1].role = "PRIX", "TOTAL"
        for c in a_droite[:-2]:
            c.role = "QUANTITE"
    for c in colonnes[:i_lib]:
        if c.mots and all(re.fullmatch(r"[x×]?\d+", m.texte, re.IGNORECASE) for m in c.mots):
            c.role = "QUANTITE"
    return [c for c in colonnes if c.role]


def _colonne_de(m: Mot, colonnes: list[Colonne]) -> Colonne | None:
    for c in colonnes:
        if m in c.mots:
            return c
    return None


# ── Enregistrements ──────────────────────────────────────────────────────────────────────────────

_RE_LIGNE_QUANTITE = re.compile(r"^[x×]?\s*\d{1,3}$", re.IGNORECASE)


def _enregistrements_numerotes(lignes_lib: list[LigneVisuelle]) -> tuple[list[Enregistrement], list[str]]:
    """Numérotation du document comme signal fort : 1, 2, 3… dans l'ordre visuel.

    Un numéro n'est retenu que s'il est le SUIVANT attendu : une ligne de continuation qui
    commencerait par « 2. » au milieu d'un libellé ne coupe rien.
    """
    enr: list[Enregistrement] = []
    avant: list[str] = []
    attendu = 1
    for l in lignes_lib:
        m = _RE_NUMERO.match(l.texte.strip())
        if m and int(m.group(1)) == attendu:
            enr.append(Enregistrement(attendu, [l]))
            attendu += 1
        elif enr:
            enr[-1].lignes.append(l)
        else:
            avant.append(l.texte)
    return enr, avant


def _enregistrements_libelles(lignes_lib: list[LigneVisuelle]) -> list[Enregistrement]:
    """Sans numérotation : une ligne de libellé ouvre une prestation ; une ligne qui ne porte
    qu'une quantité (« 6 », « X 4 ») appartient à la prestation au-dessus ; une ligne de libellé
    collée à la précédente (interligne serré) en est la suite."""
    enr: list[Enregistrement] = []
    for l in lignes_lib:
        if _RE_LIGNE_QUANTITE.match(l.texte.strip()):
            if enr:
                enr[-1].lignes_quantite.append(l)
            continue
        if enr and not enr[-1].lignes_quantite:
            prec = enr[-1].lignes[-1]
            if l.haut - prec.bas < 0.6 * min(l.h, prec.h):
                enr[-1].lignes.append(l)
                continue
        enr.append(Enregistrement(None, [l]))
    return enr


def _evenements(lignes_chiffres: list[Mot], colonnes: list[Colonne]) -> list[Evenement]:
    """Rangées de valeurs à travers les colonnes chiffrées (prix, total, quantité)."""
    evts: list[Evenement] = []
    for l in grouper_lignes(lignes_chiffres):
        valeurs: dict[str, list[str]] = {}
        for m in l.mots:
            c = _colonne_de(m, colonnes)
            if c is not None:
                valeurs.setdefault(c.role, []).append(m.texte)
        if valeurs:
            evts.append(Evenement(l.yc, l.page, {k: " ".join(v) for k, v in valeurs.items()}))
    return evts


def _distance(e: Evenement, r: Enregistrement) -> float:
    if r.haut <= e.yc <= r.bas:
        return 0.0
    return min(abs(e.yc - r.haut), abs(e.yc - r.bas))


def aligner(enregistrements: list[Enregistrement], evenements: list[Evenement],
            cout_saut: float = 30.0) -> list[Evenement]:
    """Apparie les rangées de valeurs aux prestations, dans l'ORDRE, par programmation dynamique.

    Coût d'un appariement = distance verticale entre la rangée et la prestation ; laisser une
    prestation sans valeur, ou une valeur sans prestation, coûte `cout_saut`. L'ordre est une
    contrainte : le 9e prix ne peut aller qu'à une prestation située après celle du 8e. C'est ce
    qui rend juste une facture dont tous les prix sont décalés d'une demi-ligne vers le haut, là
    où « le plus proche » se trompait dès que deux lignes se serraient.

    Rend les rangées restées sans prestation (orphelines).
    """
    n, m = len(enregistrements), len(evenements)
    inf = float("inf")
    cout = [[inf] * (m + 1) for _ in range(n + 1)]
    choix = [[None] * (m + 1) for _ in range(n + 1)]
    cout[0][0] = 0.0
    for i in range(n + 1):
        for j in range(m + 1):
            c = cout[i][j]
            if c == inf:
                continue
            if i < n and j < m:
                v = c + _distance(evenements[j], enregistrements[i])
                if v < cout[i + 1][j + 1]:
                    cout[i + 1][j + 1], choix[i + 1][j + 1] = v, "APPARIE"
            if i < n and c + cout_saut < cout[i + 1][j]:
                cout[i + 1][j], choix[i + 1][j] = c + cout_saut, "SANS_VALEUR"
            if j < m and c + cout_saut < cout[i][j + 1]:
                cout[i][j + 1], choix[i][j + 1] = c + cout_saut, "ORPHELIN"
    orphelins = []
    i, j = n, m
    while i or j:
        c = choix[i][j]
        if c == "APPARIE":
            enregistrements[i - 1].evenements.insert(0, evenements[j - 1])
            i, j = i - 1, j - 1
        elif c == "SANS_VALEUR":
            i -= 1
        else:
            orphelins.insert(0, evenements[j - 1])
            j -= 1
    return orphelins


def reconstruire_tableau(lignes: list[LigneVisuelle]) -> TableauReconstruit:
    """Du document entier au tableau des prestations, toutes pages comprises."""
    i_entete = trouver_entete(lignes)
    debut = (i_entete + 1) if i_entete is not None else 0
    fin = trouver_fin_tableau(lignes, debut)
    corps = [l for l in lignes[debut:fin] if not _RE_PIED.search(l.texte_normalise)]
    if not corps:
        return TableauReconstruit("AUCUN")
    colonnes = detecter_colonnes(corps)
    col_lib = next((c for c in colonnes if c.role == "LIBELLE"), None)
    if col_lib is None:
        return TableauReconstruit("AUCUN", colonnes=colonnes)

    mots_lib = [m for m in col_lib.mots]
    mots_chiffres = [m for c in colonnes if c.role != "LIBELLE" for m in c.mots]
    lignes_lib = grouper_lignes(mots_lib)

    enr, avant = _enregistrements_numerotes(lignes_lib)
    mode = "NUMEROTE"
    if not enr:
        enr, avant, mode = _enregistrements_libelles(lignes_lib), [], "LIBELLES"
    evts = _evenements(mots_chiffres, colonnes)
    orphelins = aligner(enr, evts)
    return TableauReconstruit(mode, enr, orphelins,
                              [t for t in avant if re.search(r"[A-Za-zÀ-ÿ]", t)], colonnes)


# ── Pavé récapitulatif ───────────────────────────────────────────────────────────────────────────

_RE_NOMBRE = re.compile(r"\d{1,3}(?: \d{3})+(?:,\d{1,2})?|\d+(?:[.,]\d{1,2})?")


def lire_nombre(texte: str) -> float | None:
    """« 2 790 », « 2 790,00 € », « 205€ », « 0,00 » → nombre. Aucun nombre → None."""
    t = str(texte or "")
    for c in _ESPACES:
        t = t.replace(c, " ")
    t = re.sub(r"\s+", " ", t.replace("€", " ")).strip()
    m = _RE_NOMBRE.search(t)
    if not m:
        return None
    try:
        return float(m.group(0).replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def _valeur_apres(mots: list[Mot]) -> float | None:
    chiffres = []
    for m in mots:
        if _est_chiffre(m):
            chiffres.append(m.texte)
        elif chiffres:
            break
    return lire_nombre(" ".join(chiffres)) if chiffres else None


def lire_recapitulatif(lignes: list[LigneVisuelle]) -> dict[str, float]:
    """Base HT, sous-total, total TTC, net à payer — chacun lu par son LIBELLÉ.

    La valeur est à droite du libellé sur la même ligne (« Sous-total 205 € »), sinon juste en
    dessous, dans la même colonne (pavé « Base HT | %TVA | Total TTC | NET A PAYER » sur une
    ligne, valeurs sur la suivante). Jamais « le plus grand montant de la page ».
    """
    out: dict[str, float] = {}
    for il, l in enumerate(lignes):
        norm = [normaliser_mot(m.texte) for m in l.mots]
        positions = []
        for cle, variantes in LIBELLES_RECAP.items():
            for suite in variantes:
                k = len(suite)
                for i in range(len(norm) - k + 1):
                    if tuple(norm[i:i + k]) == suite:
                        positions.append((i, i + k, cle))
        positions.sort()
        for idx, (i, j, cle) in enumerate(positions):
            if cle in out:
                continue
            borne = positions[idx + 1][0] if idx + 1 < len(positions) else len(l.mots)
            valeur = _valeur_apres(l.mots[j:borne])
            if valeur is None:
                x0, x1 = l.mots[i].x0 - 25, l.mots[j - 1].x1 + 25
                for dessous in lignes[il + 1:il + 3]:
                    if dessous.page != l.page or dessous.haut - l.bas > 2.5 * l.h:
                        break
                    dans = [m for m in dessous.mots if x0 <= m.xc <= x1 and _est_chiffre(m)]
                    if dans:
                        valeur = lire_nombre(" ".join(m.texte for m in dans))
                        break
            if valeur is not None:
                out[cle] = valeur
    return out

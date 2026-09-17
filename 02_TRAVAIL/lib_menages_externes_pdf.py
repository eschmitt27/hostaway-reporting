# -*- coding: utf-8 -*-
"""Extraction des factures de menage externes depuis les PDF (Lot6c).

Texte NATIF (PyMuPDF/fitz) - jamais d'OCR (les 2 factures connues sont du texte natif).
Detection du format par CONTENU (SIRET / RCS / nom legal), jamais par le nom du fichier.
Deux extracteurs dedies (Aissata << Rends-moi un service >>, Mounir << MH Entreprise >>) ;
tout autre format -> statut NON_SUPPORTE (aucune invention).

Aucune ecriture. Aucune valeur inventee : un champ obligatoire absent produit une anomalie.
Le mapping logement est EXPLICITE (referentiel controle), jamais approximatif.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

DEVISE = "EUR"

EX_OK = "OK"
EX_NON_SUPPORTE = "NON_SUPPORTE"
EX_VIDE = "VIDE"
EX_CORROMPU = "CORROMPU"
EX_ERREUR = "ERREUR"

MOIS_FR = {"janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
           "juillet": 7, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12}

# Nature de la prestation — vocabulaire de `ref_types_lignes_menage` (TLM_001..TLM_006), qui existe
# en base depuis le référentiel initial et porte déjà les drapeaux compte_comme_menage /
# repartissable_sur_menages / impact_cout_menage. On réutilise ces libellés tels quels : une
# seconde nomenclature parallèle aurait immédiatement divergé de celle du référentiel.
# Lecture métier : MÉNAGE = MENAGE_STANDARD ; REMISE EN ÉTAT = REMISE_EN_ETAT (compte aussi pour un
# ménage, mais au coût réel de la ligne) ; AUTRE PRESTATION = les quatre autres.
CAT_MENAGE_STANDARD = "MENAGE_STANDARD"
CAT_REMISE_EN_ETAT = "REMISE_EN_ETAT"
CAT_FRAIS_DEPLACEMENT = "FRAIS_DEPLACEMENT"
CAT_LINGE = "LINGE"
CAT_ACHAT_PRODUIT = "ACHAT_PRODUIT"
CAT_AUTRE = "AUTRE"
CATEGORIES = (CAT_MENAGE_STANDARD, CAT_REMISE_EN_ETAT, CAT_FRAIS_DEPLACEMENT, CAT_LINGE,
              CAT_ACHAT_PRODUIT, CAT_AUTRE)
CATEGORIES_COMPTANT_UN_MENAGE = (CAT_MENAGE_STANDARD, CAT_REMISE_EN_ETAT)

CONF_CERTAIN = "CERTAIN"
CONF_PROBABLE = "PROBABLE"
CONF_AUCUN = "AUCUN"

# Décalage vertical appliqué par page pour ordonner les bandes d'un document multi-pages sans
# jamais rendre deux bandes de pages différentes « voisines ».
_PAGE_OFFSET = 100000

# Tout caractere d'espace unicode (espace, nbsp, narrow nbsp) reduit a un espace simple.
_ESPACES = "".join(chr(c) for c in (0x20, 0xA0, 0x202F, 0x2009, 0x2007))

# Montant d'une ligne. La quantité (« x 5 ») est FACULTATIVE : une ligne qui n'en porte pas vaut 1.
_RE_MONTANT = re.compile(r"(\d{1,3}(?:[" + _ESPACES + r"]\d{3})*,\d{2})\s*€(?:\s*x\s*(\d+))?",
                         re.IGNORECASE)
# Montant écrit en euros entiers, sans décimales (formats Mounir et PrivaDom : « 520 € », « 78€ »).
_RE_ENTIER_EURO = re.compile(r"(\d{1,3}(?:[" + _ESPACES + r"]\d{3})*)\s*€")
# Libellé du total du document. C'est LUI qui fait foi, jamais le plus grand montant de la page.
_RE_LIBELLE_TOTAL = re.compile(r"net\s*a\s*payer|total\s*ttc", re.IGNORECASE)


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")


def _despace(s: str) -> str:
    for c in _ESPACES:
        s = s.replace(c, " ")
    return s


def normaliser_libelle(libelle: str) -> str:
    """Cle de mapping stable : minuscule, sans accent, parentheses-ADRESSE retirees."""
    s = _despace(_strip_accents(libelle)).lower()
    s = re.sub(r"\(\s*\d[^)]*\)", " ", s)          # (76 allee de barcelone) = adresse -> retiree
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s).strip(" .")
    return s


# CE MODULE NE RAPPROCHE PLUS LES LOGEMENTS (recette utilisateur n°3, §23-25)
#
# Il portait ici un dictionnaire de seize libelles ecrits a la main, chacun etant la transcription
# d'une facture vue un jour. Deux raisons de l'avoir retire :
#
#  1. CE N'EST PAS LE ROLE D'UN EXTRACTEUR. Lire un PDF est une operation hors-ligne et stable.
#     Decider quel logement du parc un libelle designe depend du REFERENTIEL, qui vit, change de
#     noms et gagne des logements. Figer ce lien dans un module d'extraction le condamnait a etre
#     perime le jour ou un logement entrait dans le parc.
#  2. LE REFERENTIEL SAVAIT DEJA. `ref_mapping_logements` porte 86 correspondances declarees et
#     `ref_logements` nom officiel, nom court et adresse. Les seize entrees recopiees ici etaient
#     un doublon appauvri de cette table : elles ne couvraient que 12 logements sur 17.
#
# Le rapprochement se fait desormais dans `app/services/logement_matching_service.py`, sur le
# referentiel vivant, avec un niveau de confiance et une raison explicable. `lot6c` n'est pas
# concerne : il a toujours consomme `logement_source` (le libelle brut), jamais `logement_id`.
#
# `logement_id` reste un champ de `LigneFacture`, renseigne PAR L'APPELANT apres rapprochement.
# L'extracteur, lui, ne rend que ce que le document porte : `logement_source` et sa forme
# normalisee.


@dataclass
class LigneFacture:
    logement_source: str
    quantite: int | None
    prix_unitaire: float | None
    montant_ligne: float | None
    date_menage: str | None = None
    precision_date: str = "MOIS_FACTURE"        # DATE_PRECISE | MOIS_FACTURE | MULTI_DATES
    source_page: int = 1
    logement_id: str | None = None
    libelle_normalise: str = ""
    code_anomalie: str = ""
    # Texte de la ligne EXACTEMENT tel qu'il est écrit dans le document, numéro compris. Immuable :
    # `logement_source` est une forme nettoyée, utile au rapprochement, mais ce n'est pas ce que dit
    # la pièce. Sans ce champ, l'adresse et les dates du libellé disparaissaient sans trace.
    libelle_source: str = ""
    # Nature de la prestation, dans le vocabulaire canonique de `ref_types_lignes_menage`.
    categorie: str = CAT_AUTRE
    categorie_confiance: str = CONF_AUCUN
    # Quantité non écrite dans le document et ramenée à 1 : une déduction, jamais une lecture.
    quantite_deduite: bool = False


@dataclass
class FactureExtraite:
    nom_fichier_source: str
    format_detecte: str
    prestataire_id: str | None = None
    nom_prestataire: str | None = None
    numero_facture: str | None = None
    date_facture: str | None = None
    periode_facture: str | None = None
    montant_total_facture: float | None = None
    devise: str = DEVISE
    statut_extraction: str = EX_OK
    sha256_pdf: str = ""
    lignes: list[LigneFacture] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    source_pages: int = 1
    somme_lignes: float | None = None
    ecart_reconciliation: float | None = None


def detecter_categorie(libelle: str) -> tuple[str, str]:
    """Nature de la prestation d'après son libellé. Rend (catégorie, confiance).

    Règles DÉTERMINISTES sur le texte normalisé (minuscules, sans accents) : les motifs viennent des
    factures réellement reçues, jamais d'un devinement. Une formulation inconnue rend
    (AUTRE, AUCUN) : la ligne EXISTE et attend un classement humain — c'est exactement ce qu'il ne
    faut pas confondre avec « autre prestation » décidé par le logiciel.

    L'ordre compte : « service de remise en état » contient « nettoyage » dans certaines factures,
    or c'est bien une remise en état. Le motif le plus spécifique est donc testé en premier.
    """
    t = _strip_accents(str(libelle or "")).lower().replace("’", "'").replace("`", "'")
    t = re.sub(r"\s+", " ", t)
    if not t.strip():
        return CAT_AUTRE, CONF_AUCUN
    if re.search(r"remise\s+(?:en\s+)?(?:l\s*')?\s*etat|grand\s+nettoyage|nettoyage\s+exceptionnel"
                 r"|remise\s+a\s+neuf", t):
        return CAT_REMISE_EN_ETAT, CONF_CERTAIN
    if re.search(r"\blinge\b|blanchisserie|draps|repassage", t):
        return CAT_LINGE, CONF_CERTAIN
    if re.search(r"\bcourses?\b|consommable|produit|achat", t):
        return CAT_ACHAT_PRODUIT, CONF_CERTAIN
    if re.search(r"deplacement|frais\s+de\s+route|kilometr", t):
        return CAT_FRAIS_DEPLACEMENT, CONF_CERTAIN
    if re.search(r"service\s+de\s+nettoyage|\bnettoyage\b|\bmenage\b", t):
        return CAT_MENAGE_STANDARD, CONF_CERTAIN
    return CAT_AUTRE, CONF_AUCUN


def _bandes_document(doc) -> list[tuple[int, int, str]]:
    """Toutes les bandes de texte du document, pages comprises : (page, y_global, texte).

    Les extracteurs ne lisaient que `doc[0]`. Or deux factures réelles tiennent sur deux pages, et
    c'est justement en page 2 que se trouvent le pavé « NET A PAYER » et les dernières prestations :
    le total du document était donc faux et trois lignes n'existaient pour personne.

    `y_global` décale chaque page de `_PAGE_OFFSET` pour que la distance verticale entre deux bandes
    reste comparable d'un bout à l'autre du document sans jamais confondre deux pages.
    """
    bandes: list[tuple[int, int, str]] = []
    for ip, page in enumerate(doc):
        for y, texte in _lignes_par_y(page):
            if texte.strip():
                bandes.append((ip + 1, ip * _PAGE_OFFSET + y, texte))
    return bandes


def _montants_dans(texte: str) -> list[tuple[float, int | None]]:
    """Montants d'une bande : (montant, quantité ou None quand elle n'est pas écrite).

    La quantité est FACULTATIVE. L'ancien motif exigeait « x N » et laissait donc échapper tout
    montant écrit seul — c'est le cas de toutes les remises en état et des frais de courses.
    """
    out: list[tuple[float, int | None]] = []
    for m in _RE_MONTANT.finditer(_despace(texte)):
        valeur = _to_float(m.group(1))
        if valeur is not None:
            out.append((valeur, int(m.group(2)) if m.group(2) else None))
    return out


def _total_par_libelle(bandes: list[tuple[int, int, str]]) -> float | None:
    """Total du document lu par son LIBELLÉ (« net à payer », « total TTC »), jamais par un maximum.

    L'ancienne règle prenait le plus grand montant de la page 1. Sur une facture à deux pages dont
    le pavé récapitulatif est en page 2, elle rendait 89,00 € au lieu de 2 234,00 € — et le
    diagnostic d'écart accusait alors les lignes d'être en trop. Un total introuvable reste None :
    une absence assumée vaut mieux qu'un nombre inventé.
    """
    index = {i: (texte, y) for i, (_p, y, texte) in enumerate(bandes)}
    for i, (texte, _y) in index.items():
        if not _RE_LIBELLE_TOTAL.search(_strip_accents(texte).lower()):
            continue
        # Le montant est sur la bande du libellé, ou sur une bande voisine : les colonnes d'un pavé
        # récapitulatif sont des blocs indépendants, rendus parfois juste au-dessus de leur titre.
        for j in (i, i + 1, i - 1, i + 2):
            voisine = index.get(j)
            if not voisine:
                continue
            montants = [v for v, _q in _montants_dans(voisine[0])]
            if not montants:
                montants = [v for v in (_to_float(x) for x in _RE_ENTIER_EURO.findall(voisine[0]))
                            if v is not None]
            if montants:
                return montants[-1]
    return None


def _nombres_virgule(texte: str) -> list[float]:
    """Montants X,dd avec separateur de milliers STRICT (groupes de 3) : evite de fusionner
    des colonnes voisines separees par des espaces (ex. << 1 439 0 0,00 >> -> 0 et non 143900)."""
    out = []
    for m in re.finditer(r"\d{1,3}(?:[" + _ESPACES + r"]\d{3})*,\d{2}", _despace(texte)):
        v = _to_float(m.group(0))
        if v is not None:
            out.append(v)
    return out


def _to_float(s: str) -> float | None:
    s = _despace(str(s)).replace(" ", "").replace("€", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def _lignes_par_y(page, tol: int = 3):
    bandes = defaultdict(list)
    for w in page.get_text("words"):
        bandes[round(w[1] / tol)].append(w)
    out = []
    for yb in sorted(bandes):
        mots = sorted(bandes[yb], key=lambda w: w[0])
        out.append((yb * tol, " ".join(m[4] for m in mots)))
    return out


def detecter_format(texte: str) -> str:
    t = _strip_accents(texte).lower()
    if "rends-moi un service" in t or "10147251200017" in t or "kandia diabate" in t:
        return "AISSATA"
    if "mh entreprise" in t or "792015919" in t:
        return "MOUNIR"
    if "privadom" in t or "privadonettoyage" in t or "9315724990016" in t:
        return "PRIVADOM"
    return EX_NON_SUPPORTE


# ── AISSATA ──────────────────────────────────────────────────────────────────
#
# Une ligne se reconnaît à sa STRUCTURE — un numéro, un point, un libellé — et non à son libellé.
# L'ancien motif exigeait littéralement « service de nettoyage » : toutes les autres prestations du
# même prestataire (remises en état, frais de courses, solde dû) étaient donc invisibles, sept
# lignes perdues sur quatre factures. Le point après le numéro est exigé : sans lui, « 31500
# TOULOUSE » ou « 2 234,00 » passeraient pour des lignes.
_RE_LIGNE_NUM = re.compile(r"^(\d{1,2})\s*\.\s*(.+)$")
_RE_DATE = re.compile(r"le\s+(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+(\d{4})", re.IGNORECASE)
_RE_MULTI = re.compile(r"le\s+(\d{1,2})\s+\w+\s+et\s+le\s+(\d{1,2})", re.IGNORECASE)
# Dates écrites en JJ/MM, seules ou en liste : « le 30/08 », « (06/08,08/08,10/08) ». L'année n'y
# figure pas — elle se déduit de la période de la facture, et cette déduction est signalée.
_RE_DATE_COURTE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b(?!/)")
# Préfixe qui dit la PRESTATION, pas le logement : « service de nettoyage », « service de remise en
# l'état », « service remise en état ». Il est retiré du libellé de rapprochement — la nature est
# désormais portée par `categorie`, et la répéter dans le libellé brouillerait la reconnaissance
# du logement, qui travaille sur les mots restants.
_RE_PREFIXE_PRESTATION = re.compile(
    r"^\s*(?:service\s*)?(?:de\s+)?(?:nettoyage|remise\s+(?:en\s+)?(?:l\s*['’]\s*)?[ée]tat|m[ée]nage)\s*",
    re.IGNORECASE)


# Distance verticale au-delà de laquelle une bande ne peut plus être la suite du libellé précédent.
# Deux lignes consécutives sont espacées d'une trentaine de points dans ces factures ; au-delà, la
# bande appartient à autre chose (pavé de totaux, conditions de paiement).
_DISTANCE_CONTINUATION = 40


def _dater_ligne(ligne: LigneFacture, texte: str, periode_facture: str | None) -> None:
    """Date du ménage, lue dans le libellé. Plusieurs dates = MULTI_DATES, aucune = MOIS_FACTURE.

    Le format JJ/MM est accepté : une facture entière l'utilise, et ses dix lignes ressortaient
    jusqu'ici sans aucune date. L'année n'y est pas écrite — elle est reprise de la période de la
    facture, seule source fiable à disposition.
    """
    t = _strip_accents(str(texte or "")).lower()
    if _RE_MULTI.search(t):
        ligne.precision_date = "MULTI_DATES"
        return
    pr = _RE_DATE.search(t)
    if pr:
        mo = MOIS_FR.get(pr.group(2))
        if mo:
            ligne.date_menage = f"{int(pr.group(3)):04d}-{mo:02d}-{int(pr.group(1)):02d}"
            ligne.precision_date = "DATE_PRECISE"
            return
    courtes = _RE_DATE_COURTE.findall(t)
    if not courtes:
        return
    if len(courtes) > 1:
        ligne.precision_date = "MULTI_DATES"
        return
    if not periode_facture or len(str(periode_facture)) < 7:
        return
    annee, mois_facture = int(str(periode_facture)[:4]), int(str(periode_facture)[5:7])
    jour, mois = int(courtes[0][0]), int(courtes[0][1])
    if not (1 <= mois <= 12 and 1 <= jour <= 31):
        return
    # Une facture de janvier qui cite un « 30/12 » parle de l'année précédente.
    if mois - mois_facture > 6:
        annee -= 1
    ligne.date_menage = f"{annee:04d}-{mois:02d}-{jour:02d}"
    ligne.precision_date = "DATE_PRECISE"


def _extraire_aissata(doc, nom: str) -> FactureExtraite:
    bandes = _bandes_document(doc)
    texte = _despace("".join(page.get_text() for page in doc))
    texte_na = _strip_accents(texte).lower()

    fac = FactureExtraite(nom, "AISSATA", "INT_0004", "Kandia DIABATE")
    m = re.search(r"n[°º]?\s*(\d{4}-\d+)", texte)
    fac.numero_facture = m.group(1) if m else None
    mdf = re.search(r"date de la facture\s*:\s*(\d{1,2})\s+(\w+)\s+(\d{4})", texte_na)
    if mdf:
        mo = MOIS_FR.get(mdf.group(2))
        if mo:
            fac.date_facture = f"{int(mdf.group(3)):04d}-{mo:02d}-{int(mdf.group(1)):02d}"
            fac.periode_facture = fac.date_facture[:7]
    fac.montant_total_facture = _total_par_libelle(bandes)
    if fac.montant_total_facture is None:
        fac.anomalies.append("TOTAL_DOCUMENT_NON_LU")

    # Repérage des lignes, des montants et des bandes de continuation (la suite d'un libellé coupé,
    # qui porte presque toujours les dates de passage).
    descriptions: list[tuple[int, int, int, str]] = []       # (y, page, numéro, texte de la bande)
    montants: list[tuple[int, float, int | None]] = []       # (y, montant, quantité)
    for page_no, y, t in bandes:
        for valeur, quantite in _montants_dans(t):
            montants.append((y, valeur, quantite))
        md2 = _RE_LIGNE_NUM.match(t.strip())
        if md2:
            descriptions.append((y, page_no, int(md2.group(1)), md2.group(2).strip()))

    ys_descriptions = [d[0] for d in descriptions]
    continuations: dict[int, list[str]] = defaultdict(list)
    for _page_no, y, t in bandes:
        ts = t.strip()
        if y in ys_descriptions or _RE_LIGNE_NUM.match(ts):
            continue
        # Une bande qui ne porte QUE des montants appartient à une ligne, pas à son libellé.
        if not _RE_MONTANT.sub("", ts).strip(" x€"):
            continue
        precedentes = [yd for yd in ys_descriptions if yd < y]
        if precedentes and (y - max(precedentes)) <= _DISTANCE_CONTINUATION:
            continuations[max(precedentes)].append(ts)

    used = set()
    for y, page_no, num, lib in sorted(descriptions, key=lambda d: (d[0])):
        best = None
        for idx, (ym, pu, qte) in enumerate(montants):
            if idx in used:
                continue
            dist = abs(ym - y)
            if best is None or dist < best[0]:
                best = (dist, idx, pu, qte)
        pu = qte = None
        if best is not None:
            used.add(best[1])
            pu, qte = best[2], best[3]
        # Libellé source : la ligne telle qu'elle est écrite, numéro et continuations compris.
        libelle_source = " ".join([f"{num}. {lib}"] + continuations.get(y, [])).strip()
        lib = " ".join([lib] + continuations.get(y, []))
        # Le libellé retenu pour le RAPPROCHEMENT ne garde que ce qui désigne un logement : la
        # nature de la prestation est désormais portée par `categorie`, et les dates par
        # `date_menage`. Les retirer ici, plutôt que de tronquer au premier « le 12 juillet »,
        # préserve le logement des lignes qui nomment leur date AVANT leur adresse — sur une
        # remise en état écrite « le 26 mars 2026 T3 310 avenue de muret », l'ancienne troncature
        # ne laissait rien.
        lib_clean = _RE_MONTANT.sub(" ", lib)
        lib_clean = _RE_PREFIXE_PRESTATION.sub(" ", lib_clean)
        lib_clean = re.sub(r"\bx\s*\d+\s*(?:passages?)?", " ", lib_clean, flags=re.IGNORECASE)
        lib_clean = re.sub(r"\(\s*\d{1,2}/\d{1,2}[^)]*\)", " ", lib_clean)
        lib_clean = re.sub(r"\ble\s+\d{1,2}\s+(?:" + "|".join(MOIS_FR) + r")(?:\s+\d{4})?",
                           " ", _strip_accents(lib_clean), flags=re.IGNORECASE)
        lib_clean = re.sub(r"\bet\s+le\s+\d{1,2}\s+\w+", " ", lib_clean, flags=re.IGNORECASE)
        lib_clean = re.sub(r"\ble\s+\d{1,2}/\d{1,2}", " ", lib_clean, flags=re.IGNORECASE)
        lib_clean = re.sub(r"\b\d{1,2}/\d{1,2}\b", " ", lib_clean)
        # Description wrappee sur 2 lignes -> fragment de parenthese non fermee / << x >> isole a nettoyer.
        lib_clean = re.sub(r"\([^)]*$", "", lib_clean)          # parenthese-adresse ouverte non fermee
        lib_clean = re.sub(r"\(\s*\d[^)]*\)", "", lib_clean)    # parenthese-ADRESSE fermee (commence par chiffre)
        lib_clean = re.sub(r"\s+x\s*$", "", lib_clean)          # << x >> orphelin de fin de ligne
        # « le 22 juillet et le 26 juillet » : les deux dates retirées laissent un « et » orphelin.
        lib_clean = re.sub(r"\s+(?:et|le|du|au)\s*$", "", lib_clean, flags=re.IGNORECASE)
        lib_clean = lib_clean.replace("’", "'").replace("`", "'")   # apostrophe typographique -> droite
        lib_clean = re.sub(r"\s+", " ", lib_clean).strip(" .")
        # Quantité non écrite = 1, et c'est DIT (`quantite_deduite`) : une remise en état facturée
        # « 89,00 € » sans « x N » est une prestation unique, pas une ligne sans montant.
        quantite_deduite = pu is not None and qte is None
        if quantite_deduite:
            qte = 1
        ligne = LigneFacture(lib_clean, qte, pu,
                             round(pu * qte, 2) if (pu is not None and qte is not None) else None,
                             source_page=page_no)
        ligne.libelle_source = libelle_source
        ligne.quantite_deduite = quantite_deduite
        ligne.categorie, ligne.categorie_confiance = detecter_categorie(libelle_source)
        _dater_ligne(ligne, lib, fac.periode_facture)
        ligne.libelle_normalise = normaliser_libelle(lib_clean)
        if pu is None:
            ligne.code_anomalie = (ligne.code_anomalie + " | MONTANT_ABSENT").strip(" |")
        fac.lignes.append(ligne)

    if not fac.lignes:
        fac.statut_extraction = EX_VIDE
        fac.anomalies.append("AUCUNE_LIGNE_EXTRACTE")
    return fac


# ── MOUNIR ───────────────────────────────────────────────────────────────────
_RE_APPART = re.compile(r"^(T[.\- ]|Studio)", re.IGNORECASE)
# Montant Mounir : entier SANS separateur (390, 520, 0, 32, 942) -> ne fusionne pas << 36 0 >>.
_RE_EURO = re.compile(r"(\d+)\s*€")


def _extraire_mounir(doc, nom: str) -> FactureExtraite:
    bandes = _bandes_document(doc)
    texte = _despace("".join(page.get_text() for page in doc))
    texte_na = _strip_accents(texte).lower()

    fac = FactureExtraite(nom, "MOUNIR", "INT_0003", "MH Entreprise")
    m = re.search(r"numero de facture\s*:\s*(\S+)", texte_na)
    fac.numero_facture = m.group(1) if m else None
    md = re.search(r"date de facturation\s*:\s*(\d{1,2})/(\d{1,2})/(\d{4})", texte_na)
    if md:
        fac.date_facture = f"{int(md.group(3)):04d}-{int(md.group(2)):02d}-{int(md.group(1)):02d}"
        fac.periode_facture = fac.date_facture[:7]
    mt = re.search(r"net a payer ttc\s*(\d+)\s*€", texte_na)
    fac.montant_total_facture = _to_float(mt.group(1)) if mt else _total_par_libelle(bandes)
    if fac.montant_total_facture is None:
        fac.anomalies.append("TOTAL_DOCUMENT_NON_LU")

    # Un appartement par ligne du tableau ; le libellé PUR (avant le premier montant) sert de
    # référence de position, et permet de retirer du segment les chiffres du nom (« T.2-65 »).
    appart_ys: list[tuple[int, int, str]] = []
    for page_no, y, t in bandes:
        ts = t.strip()
        if _RE_APPART.match(ts) and "wonderbnb" not in ts.lower():
            lib_pur = re.split(r"\d+\s*€", ts, 1)[0].strip()
            lib_pur = lib_pur.replace("’", "'").replace("`", "'")
            appart_ys.append((y, page_no, lib_pur))
    borne_fin = min([y for _p, y, t in bandes
                     if re.search(r"conditions de paiement|net a payer", _strip_accents(t).lower())]
                    + [_PAGE_OFFSET * (doc.page_count + 1)])

    # APPARIEMENT PAR PROXIMITÉ, ET NON PAR BANDE DESCENDANTE.
    # Les colonnes d'un PDF sont des blocs de texte indépendants : rien ne garantit que le bloc des
    # montants soit rendu APRÈS le libellé de sa ligne. Sur une facture réelle, « 32 € 0 € » est
    # écrit juste AU-DESSUS de « Studio Puits vert » : la découpe en intervalles [Y_i, Y_i+1)
    # attribuait donc ces montants à l'appartement précédent, qui recevait 36 € au lieu de 0 € et
    # faisait tomber la somme des lignes 36 € au-dessus du total du document.
    # La quantité, elle, est bien rendue SOUS sa ligne (« X 4 ») : elle reste prise dans
    # l'intervalle descendant.
    ys_appart = [y for y, _p, _l in appart_ys]
    for i, (y0, page_no, lib) in enumerate(appart_ys):
        y1 = appart_ys[i + 1][0] if i + 1 < len(appart_ys) else borne_fin
        euros: list[float] = []
        quantites: list[int] = []
        for _p, yy, t in bandes:
            if yy > borne_fin:
                continue
            texte_bande = t.replace(lib, " ", 1) if yy == y0 else t
            # Une bande de MONTANTS porte un €, une bande de QUANTITÉ n'en porte aucun. Ce seul
            # critère suffit et résiste aux irrégularités du document, qui écrit tantôt
            # « 65 € 390 € », tantôt « 36 0 € » — le prix unitaire y perd son symbole.
            if "€" in texte_bande:
                if min(ys_appart, key=lambda ya: abs(ya - yy)) == y0:
                    euros.extend(v for v in (_to_float(x) for x in
                                             re.findall(r"\d+(?:,\d{2})?", _despace(texte_bande)))
                                 if v is not None)
            elif y0 <= yy < y1:
                for tok in _despace(texte_bande).split():
                    if re.fullmatch(r"\d+", tok):
                        quantites.append(int(tok))
        # Colonnes du document : « Nombres d'appartements | PU | PU TTC € ». Le premier montant est
        # donc le prix unitaire, le second le total de la ligne.
        pu = euros[0] if euros else None
        total = euros[-1] if euros else None
        nb = quantites[0] if quantites else None
        ligne = LigneFacture(lib, nb, pu, total, precision_date="MOIS_FACTURE", source_page=page_no)
        ligne.libelle_source = lib
        ligne.categorie, ligne.categorie_confiance = detecter_categorie(lib)
        if ligne.categorie_confiance == CONF_AUCUN:
            # Ce format ne décrit que des appartements : la prestation est un ménage, le document
            # ne le réécrit pas à chaque ligne. PROBABLE, donc corrigeable sans discussion.
            ligne.categorie, ligne.categorie_confiance = CAT_MENAGE_STANDARD, CONF_PROBABLE
        ligne.libelle_normalise = normaliser_libelle(lib)
        if total is None:
            ligne.code_anomalie = (ligne.code_anomalie + " | MONTANT_ABSENT").strip(" |")
        # controle interne : total == nb x pu (tolerance 1 EUR)
        if None not in (nb, pu, total) and abs((nb * pu) - total) > 1.0:
            ligne.code_anomalie = (ligne.code_anomalie + " | LIGNE_NB_PU_TOTAL_INCOHERENT").strip(" |")
        fac.lignes.append(ligne)

    if not fac.lignes:
        fac.statut_extraction = EX_VIDE
        fac.anomalies.append("AUCUNE_LIGNE_EXTRACTE")
    return fac


# ── PRIVADOM ─────────────────────────────────────────────────────────────────
#
# Troisième prestataire, jusqu'ici NON_SUPPORTÉ : sa facture entière était perdue. Le document est
# un vrai tableau — QUANTITE | DESCRIPTION | PRIX UNITAIRE | TOTAL DE LA LIGNE — dont chaque ligne
# tient sur une seule bande. Ce format-là se lit sans heuristique.
_RE_LIGNE_PRIVADOM = re.compile(
    r"^(\d+)\s+(.+?)\s+(\d+(?:,\d{2})?)\s*€\s+(\d+(?:,\d{2})?)\s*€\s*$")


def _extraire_privadom(doc, nom: str) -> FactureExtraite:
    bandes = _bandes_document(doc)
    texte = _despace("".join(page.get_text() for page in doc))
    texte_na = _strip_accents(texte).lower()

    fac = FactureExtraite(nom, "PRIVADOM", "INT_PRIVADOM", "PrivaDom")
    # Le numéro de facture et la date sont rendus sur la même bande : « 21/04/2026 2025-017 ».
    md = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{4}-\d+)", texte)
    if md:
        fac.date_facture = f"{int(md.group(3)):04d}-{int(md.group(2)):02d}-{int(md.group(1)):02d}"
        fac.periode_facture = fac.date_facture[:7]
        fac.numero_facture = md.group(4)
    else:
        mn = re.search(r"\b(\d{4}-\d{3,})\b", texte)
        fac.numero_facture = mn.group(1) if mn else None
    fac.montant_total_facture = _total_par_libelle(bandes)
    if fac.montant_total_facture is None:
        fac.anomalies.append("TOTAL_DOCUMENT_NON_LU")

    for page_no, _y, t in bandes:
        m = _RE_LIGNE_PRIVADOM.match(t.strip())
        if not m:
            continue
        libelle = m.group(2).strip()
        if re.search(r"sous-total|total|t\.v\.a", _strip_accents(libelle).lower()):
            continue
        quantite = int(m.group(1))
        ligne = LigneFacture(libelle, quantite, _to_float(m.group(3)), _to_float(m.group(4)),
                             precision_date="MOIS_FACTURE", source_page=page_no)
        ligne.libelle_source = t.strip()
        ligne.categorie, ligne.categorie_confiance = detecter_categorie(libelle)
        ligne.libelle_normalise = normaliser_libelle(libelle)
        if ligne.montant_ligne is None:
            ligne.code_anomalie = "MONTANT_ABSENT"
        elif (ligne.prix_unitaire is not None
                and abs(quantite * ligne.prix_unitaire - ligne.montant_ligne) > 1.0):
            ligne.code_anomalie = "LIGNE_NB_PU_TOTAL_INCOHERENT"
        fac.lignes.append(ligne)

    if not fac.lignes:
        fac.statut_extraction = EX_VIDE
        fac.anomalies.append("AUCUNE_LIGNE_EXTRACTE")
    return fac


# ── §18 — le nom de fichier est une INDICATION, jamais la vérité ─────────────────────────────────
#
# Convention canonique : MM-YY-Prestataire.pdf, ou MM-YY-Prestataire_<suffixe>.pdf quand un même
# prestataire a plusieurs factures le même mois. L'année est TOUJOURS sur 2 chiffres.
_RE_NOM_CANONIQUE = re.compile(r"^(\d{2})-(\d{2})-([^_]+?)(?:_(.+))?\.pdf$", re.IGNORECASE)


def indication_nom_fichier(nom: str) -> dict:
    """Ce que le NOM suggère : période, prestataire, suffixe. Un nom hors convention ne suggère
    rien — et une indication absente ne peut rien contredire."""
    m = _RE_NOM_CANONIQUE.match(Path(str(nom)).name.strip())
    if not m or not 1 <= int(m.group(1)) <= 12:
        return {"conforme": False}
    return {"conforme": True, "mois": f"20{m.group(2)}-{m.group(1)}",
            "prestataire": m.group(3).strip(), "suffixe": (m.group(4) or "").strip()}


def _cle_comparaison(s) -> str:
    return re.sub(r"[^a-z0-9]", "", _strip_accents(str(s or "")).lower())


def controler_nom_fichier(fac: FactureExtraite) -> None:
    """Confronte l'indication du nom au CONTENU extrait ; une contradiction devient une anomalie.

    Le logiciel ne valide jamais une facture sur la foi de son nom : elle reste À CONTRÔLER, et
    l'anomalie dit à l'humain ce qu'il doit regarder.

    Période — le mois du nom doit être celui de la facture OU celui d'au moins un ménage facturé :
    une facture de fin de mois émise le 1er du mois suivant ne contredit rien ; un nom qui ne
    correspond à AUCUNE des dates du document, si.
    Prestataire — comparé seulement quand le document le nomme : sans nom lu, rien n'est affirmé.
    Le suffixe ne sert qu'à distinguer deux fichiers ; le dédoublonnage repose sur le contenu.
    """
    ind = indication_nom_fichier(fac.nom_fichier_source)
    if not ind["conforme"]:
        return
    mois_document = {m for m in [(fac.periode_facture or fac.date_facture or "")[:7]]
                     + [(l.date_menage or "")[:7] for l in fac.lignes] if m}
    if mois_document and ind["mois"] not in mois_document:
        fac.anomalies.append(f"NOM_FICHIER_PERIODE_CONTRADICTOIRE:{ind['mois']}")
    cle_nom = _cle_comparaison(ind["prestataire"])
    if fac.nom_prestataire and cle_nom and cle_nom not in _cle_comparaison(fac.nom_prestataire):
        fac.anomalies.append(
            "NOM_FICHIER_PRESTATAIRE_CONTRADICTOIRE:" + ind["prestataire"].replace(",", " "))


def extraire_pdf(path) -> FactureExtraite:
    p = Path(path)
    nom = p.name
    if fitz is None:
        return FactureExtraite(nom, EX_ERREUR, statut_extraction=EX_ERREUR, anomalies=["PYMUPDF_ABSENT"])
    try:
        sha = _sha256(p)
        doc = fitz.open(p)
    except Exception as exc:
        return FactureExtraite(nom, EX_CORROMPU, statut_extraction=EX_CORROMPU,
                               anomalies=[f"OUVERTURE:{type(exc).__name__}"])
    try:
        texte = "".join(page.get_text() for page in doc)
        pages = doc.page_count
        if not texte.strip():
            return FactureExtraite(nom, EX_VIDE, statut_extraction=EX_VIDE, sha256_pdf=sha,
                                   source_pages=pages, anomalies=["PDF_SANS_TEXTE_NATIF_OCR_REQUIS"])
        fmt = detecter_format(texte)
        if fmt == "AISSATA":
            fac = _extraire_aissata(doc, nom)
        elif fmt == "MOUNIR":
            fac = _extraire_mounir(doc, nom)
        elif fmt == "PRIVADOM":
            fac = _extraire_privadom(doc, nom)
        else:
            fac = FactureExtraite(nom, EX_NON_SUPPORTE, statut_extraction=EX_NON_SUPPORTE,
                                  anomalies=["FORMAT_FOURNISSEUR_NON_SUPPORTE"])
        fac.sha256_pdf = sha
        fac.source_pages = pages
        _controle_reconciliation(fac)
        return fac
    finally:
        doc.close()


def _controle_reconciliation(fac: FactureExtraite) -> None:
    if fac.statut_extraction != EX_OK or fac.montant_total_facture is None:
        return
    somme = round(sum(l.montant_ligne or 0 for l in fac.lignes), 2)
    fac.somme_lignes = somme
    fac.ecart_reconciliation = round(somme - fac.montant_total_facture, 2)
    if abs(fac.ecart_reconciliation) > 1.0:
        fac.anomalies.append(f"RECONCILIATION_ECART_{fac.ecart_reconciliation}")


def row_hash(fac: FactureExtraite, ligne: LigneFacture) -> str:
    base = "|".join(str(x) for x in [fac.sha256_pdf[:16], fac.numero_facture, fac.prestataire_id,
                                     ligne.logement_id or ligne.libelle_normalise,
                                     ligne.date_menage, ligne.montant_ligne])
    return hashlib.sha256(base.encode()).hexdigest()[:16]


def empreinte_facture(fac: FactureExtraite) -> str:
    base = "|".join(str(x) for x in [fac.sha256_pdf, fac.numero_facture, fac.prestataire_id,
                                     fac.date_facture, fac.montant_total_facture])
    return hashlib.sha256(base.encode()).hexdigest()

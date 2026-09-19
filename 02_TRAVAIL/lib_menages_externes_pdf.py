# -*- coding: utf-8 -*-
"""Extraction des factures de menage externes depuis les PDF (Lot6c).

Texte NATIF (PyMuPDF) avec sa GEOMETRIE - jamais d'OCR ni d'IA (toutes les factures reçues sont
du texte natif ; un PDF sans texte ressort VIDE, « OCR requis », au lieu d'etre devine).
Le fournisseur est reconnu par CONTENU (SIRET / RCS / nom legal), jamais par le nom du fichier ;
un fournisseur inconnu -> NON_SUPPORTE (aucune invention).

UNE SEULE LECTURE POUR TOUS LES FOURNISSEURS : `lib_factures_geometrie` reconstruit le tableau
des prestations (colonnes, lignes numerotees ou non, prix apparies dans l'ordre, pages suivantes,
pave recapitulatif) ; ce module interprete chaque prestation (quantites, montants, nature, dates,
libelle de rapprochement) et controle l'arithmetique ligne par ligne puis contre le total.

Aucune ecriture. Aucune valeur inventee : un champ obligatoire absent produit une anomalie.
Le mapping logement est EXPLICITE (referentiel controle), jamais approximatif.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

try:
    import pymupdf as fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    try:
        import fitz
    except ImportError:
        fitz = None

import lib_factures_geometrie as geo

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

# Tout caractere d'espace unicode (espace, nbsp, narrow nbsp) reduit a un espace simple.
_ESPACES = "".join(chr(c) for c in (0x20, 0xA0, 0x202F, 0x2009, 0x2007))


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
# d'une facture vue un jour. Le rapprochement se fait desormais dans
# `app/services/logement_matching_service.py`, sur le referentiel vivant, avec un niveau de
# confiance et une raison explicable. `logement_id` reste un champ de `LigneFacture`, renseigne PAR
# L'APPELANT apres rapprochement. L'extracteur, lui, ne rend que ce que le document porte.


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
    # Texte de la ligne EXACTEMENT tel qu'il est écrit dans la colonne des libellés, numéro compris
    # — et rien d'autre : ni le prix, ni la quantité de la colonne voisine.
    libelle_source: str = ""
    # Nature de la prestation, dans le vocabulaire canonique de `ref_types_lignes_menage`.
    categorie: str = CAT_AUTRE
    categorie_confiance: str = CONF_AUCUN
    # Quantité non écrite dans le document et ramenée à 1 : une déduction, jamais une lecture.
    quantite_deduite: bool = False
    # Numéro de la ligne dans le document (« 7. »), quand le document numérote ses lignes.
    numero_ligne: int | None = None
    # Quantité écrite dans le LIBELLÉ (« x 6 passages »), à comparer à la quantité facturée.
    quantite_libelle: int | None = None
    # Montant de ligne tel qu'il est ÉCRIT, quand il a dû être recalculé (quantité × prix).
    montant_lu: float | None = None


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
    # Les totaux du pavé récapitulatif, chacun lu par son propre libellé. `montant_total_facture`
    # est le NET À PAYER quand il existe, sinon le total TTC, sinon le sous-total / la base HT.
    base_ht: float | None = None
    sous_total: float | None = None
    total_ttc: float | None = None
    net_a_payer: float | None = None
    # NUMEROTE (lignes « 1. », « 2. »…) ou LIBELLES (une prestation par libellé du tableau).
    mode_lecture: str = ""


def detecter_categorie(libelle: str) -> tuple[str, str]:
    """Nature de la prestation d'après son libellé. Rend (catégorie, confiance).

    Règles DÉTERMINISTES sur le texte normalisé (minuscules, sans accents). Une formulation
    inconnue rend (AUTRE, AUCUN) : la ligne EXISTE et attend un classement humain — ce qu'il ne
    faut pas confondre avec « autre prestation » décidé par le logiciel (confiance PROBABLE).

    L'ordre compte : « service de remise en état » est une remise en état même s'il contient
    « nettoyage » ; un libellé qui ne nomme qu'un logement (« T.3 Sept Deniers », « studio 76 …
    x 1 passage ») est un ménage PROBABLE — le document ne réécrit pas la prestation à chaque ligne.
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
    if re.search(r"service\s+de\s+nettoyage|\bnettoyage|\bmenages?\b", t):
        return CAT_MENAGE_STANDARD, CONF_CERTAIN
    if re.search(r"\blivraison|\bintervention|\bdebarras", t):
        return CAT_AUTRE, CONF_PROBABLE
    if re.search(r"\bpassages?\b|\bt\s*\.?\s*\d\b|\bstudio\b", t):
        return CAT_MENAGE_STANDARD, CONF_PROBABLE
    return CAT_AUTRE, CONF_AUCUN


# ── Identité du fournisseur ──────────────────────────────────────────────────────────────────────
#
# La SEULE connaissance propre à un fournisseur : qui il est (SIRET, RCS, raison sociale) et sous
# quel identifiant le référentiel le connaît. La LECTURE des lignes, elle, est la même pour tous
# (`lib_factures_geometrie`) : aucune règle d'extraction ne dépend du fournisseur.
IDENTITES = {
    "AISSATA": ("INT_0004", "Kandia DIABATE",
                ("rends-moi un service", "10147251200017", "kandia diabate")),
    "MOUNIR": ("INT_0003", "MH Entreprise", ("mh entreprise", "792015919")),
    "PRIVADOM": ("INT_PRIVADOM", "PrivaDom", ("privadom", "privadonettoyage", "9315724990016")),
}


def detecter_format(texte: str) -> str:
    t = _strip_accents(texte).lower()
    for fmt, (_id, _nom, signes) in IDENTITES.items():
        if any(s in t for s in signes):
            return fmt
    return EX_NON_SUPPORTE


# ── En-tête : numéro et date ─────────────────────────────────────────────────────────────────────

_MOIS_RE = "|".join(MOIS_FR)


def _lire_entete(texte: str) -> tuple[str | None, str | None]:
    """(numéro, date ISO) lus par leurs libellés usuels ; à défaut, le couple « date numéro »
    qu'un en-tête sans libellé écrit côte à côte. Rien de lu → None, jamais une valeur inventée."""
    t = _despace(texte)
    tn = _strip_accents(t).lower()
    numero = None
    for motif in (r"facture\s*n\s*[°º]\s*:?\s*([0-9][\w-]*)",
                  r"num[eé]ro\s+de\s+facture\s*:?\s*([0-9A-Za-z][\w-]*)",
                  r"facture\s+n(?:o|umero)?\s*[:.]\s*([0-9][\w-]*)"):
        m = re.search(motif, t, re.IGNORECASE)
        if m:
            numero = m.group(1)
            break
    date = None
    m = re.search(r"date\s+de\s+(?:la\s+)?factur(?:e|ation)\s*:?\s*(\d{1,2})(?:er)?\s+(" + _MOIS_RE
                  + r")\s+(\d{4})", tn)
    if m:
        date = f"{int(m.group(3)):04d}-{MOIS_FR[m.group(2)]:02d}-{int(m.group(1)):02d}"
    if date is None:
        m = re.search(r"date\s+de\s+(?:la\s+)?factur(?:e|ation)\s*:?\s*(\d{1,2})/(\d{1,2})/(\d{4})", tn)
        if m:
            date = f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    if date is None or numero is None:
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{4}-\d+)", t)
        if m:
            date = date or f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
            numero = numero or m.group(4)
    if date is None:
        m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", t)
        if m:
            date = f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return numero, date


# ── Dates de passage ─────────────────────────────────────────────────────────────────────────────

_RE_DATE = re.compile(r"le\s+(\d{1,2})(?:er)?\s+(" + _MOIS_RE + r")\s+(\d{4})", re.IGNORECASE)
_RE_MULTI = re.compile(r"le\s+(\d{1,2})(?:er)?\s+\w+\s+et\s+(?:le\s+)?(\d{1,2})", re.IGNORECASE)
# Dates écrites en JJ/MM, seules ou en liste : « le 30/08 », « (06/08,08/08,10/08) ». L'année n'y
# figure pas — elle se déduit de la période de la facture, et cette déduction est signalée.
_RE_DATE_COURTE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b(?!/)")


def _dater_ligne(ligne: LigneFacture, texte: str, periode_facture: str | None) -> None:
    """Date du ménage, lue dans le libellé. Plusieurs dates = MULTI_DATES, aucune = MOIS_FACTURE.

    Le format JJ/MM est accepté ; l'année n'y est pas écrite — elle est reprise de la période de la
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


# ── Libellé de rapprochement ─────────────────────────────────────────────────────────────────────
#
# Le libellé transmis au rapprochement ne garde que ce qui DÉSIGNE un logement. La nature de la
# prestation est portée par `categorie`, les dates par `date_menage`, la quantité par `quantite` :
# les laisser dans le libellé brouillait la reconnaissance (« T3 sept deniers (François) et », ou
# « T4 90 Blagnac (Cédrine) , , , , , , , » après retrait incomplet d'une liste de dates).
# L'adresse entre parenthèses est CONSERVÉE : c'est le meilleur signal du logement.

_MOIS_ACCENTS = r"janvier|f[ée]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[ée]cembre"
_DATE_ATOME = (r"(?:\d{1,2}(?:er)?\s+(?:" + _MOIS_ACCENTS + r")(?:\s+\d{4})?"
               r"|(?<![\w/])\d{1,2}/\d{1,2}(?:/\d{2,4})?(?![\w/]))")
_DATE_LISTE = _DATE_ATOME + r"(?:\s*(?:,|et\s+le|et)\s*" + _DATE_ATOME + r")*"
_RE_DATES = [re.compile(p, re.IGNORECASE) for p in (
    r"\(\s*" + _DATE_LISTE + r"\s*\)",
    r"\b(?:le|du|au)\s+" + _DATE_LISTE + r"\.?",
    _DATE_LISTE + r"\.?",
)]
_UNITES = r"(?:passages?|m[ée]nages?|d[ée]placements?|fois)"
_RE_QUANTITES = [re.compile(p, re.IGNORECASE) for p in (
    r"\b[x×]\s*\d+\s*" + _UNITES + r"?\b",
    r"\b\d+\s+" + _UNITES + r"\b",
    r"\bpassages?\b",
)]
_RE_PREFIXE_PRESTATION = re.compile(
    r"^\s*(?:service\s*)?(?:de\s+)?(?:nettoyage|remise\s+(?:en\s+)?(?:l\s*['’]\s*)?[ée]tat|m[ée]nage)\s*",
    re.IGNORECASE)
_RE_NUMERO_LIGNE = re.compile(r"^\s*(\d{1,2})\s*\.\s*")


def libelle_logement(texte: str) -> str:
    """Du texte d'une ligne au libellé qui désigne son logement (numéro, prestation, dates et
    quantités retirés ; propriétaire et adresse entre parenthèses conservés)."""
    s = _despace(str(texte or "")).replace("’", "'").replace("`", "'")
    s = _RE_NUMERO_LIGNE.sub("", s)
    s = _RE_PREFIXE_PRESTATION.sub("", s)
    for r in _RE_DATES + _RE_QUANTITES:
        s = r.sub(" ", s)
    s = re.sub(r"\(\s*\)", " ", s)
    s = re.sub(r"(?:\s*[,;.]\s*)+(?=\s|$)", " ", s)
    s = re.sub(r"\s+", " ", s).strip(" .,;-")
    for _ in range(2):
        s = re.sub(r"\s+(?:et|le|du|au|x)$", "", s, flags=re.IGNORECASE).strip(" .,;-")
    return s


# ── Lecture des montants ─────────────────────────────────────────────────────────────────────────

_RE_MULTIPLICATEUR = re.compile(r"[x×]\s*(\d+)\s*$", re.IGNORECASE)
_RE_QTE_LIBELLE = re.compile(
    r"\b[x×]\s*(\d+)\s*(?:" + _UNITES + r")?\b|\b(\d+)\s+" + _UNITES + r"\b", re.IGNORECASE)


def _entier(texte: str | None) -> int | None:
    m = re.search(r"(\d+)", str(texte or ""))
    return int(m.group(1)) if m else None


def _quantite_libelle(texte: str) -> int | None:
    m = _RE_QTE_LIBELLE.search(_despace(str(texte or "")))
    if not m:
        return None
    return int(m.group(1) or m.group(2))


def _ajouter_anomalie(ligne: LigneFacture, code: str) -> None:
    ligne.code_anomalie = (ligne.code_anomalie + " | " + code).strip(" |")


def _ligne_depuis_enregistrement(enr, periode_facture: str | None) -> LigneFacture:
    """Une prestation reconstruite → une ligne de facture, chiffres contrôlés.

    Priorité des quantités : celle écrite À CÔTÉ DU PRIX (« 55,00 € x 7 ») ou dans la colonne des
    quantités fait foi — c'est elle qui est facturée. Celle du libellé (« x 6 passages ») est
    comparée, et une divergence est signalée plutôt que tranchée en silence.
    """
    valeurs = enr.evenements[0].valeurs if enr.evenements else {}
    texte = enr.texte
    pu = total = None
    qte = None
    if "MONTANT" in valeurs:
        pu = geo.lire_nombre(valeurs["MONTANT"])
        mm = _RE_MULTIPLICATEUR.search(valeurs["MONTANT"])
        qte = int(mm.group(1)) if mm else None
    if "PRIX" in valeurs:
        pu = geo.lire_nombre(valeurs["PRIX"])
    if "TOTAL" in valeurs:
        total = geo.lire_nombre(valeurs["TOTAL"])
    if qte is None and "QUANTITE" in valeurs:
        qte = _entier(valeurs["QUANTITE"])
    if qte is None and enr.lignes_quantite:
        qte = _entier(enr.lignes_quantite[0].texte)
    qte_libelle = _quantite_libelle(texte)

    deduite = False
    if total is None and pu is not None:
        if qte is None and qte_libelle and qte_libelle > 1:
            # Un montant seul face à « x N passages » : le document ne dit pas s'il est unitaire.
            # On garde ce qui est ÉCRIT comme montant de la ligne, et on le signale.
            total = pu
            qte = qte_libelle
            pu = round(total / qte, 2)
        elif qte is None:
            qte, deduite = 1, True
        total = round(pu * qte, 2) if total is None else total
    elif total is not None and qte is None and pu:
        ratio = total / pu
        if abs(ratio - round(ratio)) < 1e-6:
            qte, deduite = int(round(ratio)), True

    ligne = LigneFacture(libelle_logement(texte), qte, pu, total, source_page=enr.page)
    ligne.numero_ligne = enr.numero
    ligne.libelle_source = texte
    ligne.quantite_deduite = deduite
    ligne.quantite_libelle = qte_libelle
    ligne.categorie, ligne.categorie_confiance = detecter_categorie(texte)
    _dater_ligne(ligne, texte, periode_facture)
    ligne.libelle_normalise = normaliser_libelle(ligne.logement_source)
    if total is None:
        _ajouter_anomalie(ligne, "MONTANT_ABSENT")
    if None not in (qte, pu, total) and abs(qte * pu - total) > 0.005:
        _ajouter_anomalie(ligne, "LIGNE_NB_PU_TOTAL_INCOHERENT")
    if qte_libelle is not None and qte is not None and not deduite and qte_libelle != qte:
        _ajouter_anomalie(ligne, f"QUANTITE_LIBELLE_DIFFERENTE(libelle={qte_libelle},facturee={qte})")
    return ligne


def _extraire_generique(doc, nom: str, fmt: str) -> FactureExtraite:
    """Une seule lecture pour tous les fournisseurs : géométrie → tableau → lignes → totaux."""
    prestataire_id, nom_prestataire, _signes = IDENTITES[fmt]
    fac = FactureExtraite(nom, fmt, prestataire_id, nom_prestataire)
    texte = _despace("\n".join(page.get_text() for page in doc))
    fac.numero_facture, fac.date_facture = _lire_entete(texte)
    fac.periode_facture = fac.date_facture[:7] if fac.date_facture else None

    lignes = geo.grouper_lignes(geo.lire_mots(doc))
    recap = geo.lire_recapitulatif(lignes)
    fac.base_ht, fac.sous_total = recap.get("base_ht"), recap.get("sous_total")
    fac.total_ttc, fac.net_a_payer = recap.get("total_ttc"), recap.get("net_a_payer")
    for valeur in (fac.net_a_payer, fac.total_ttc, fac.sous_total, recap.get("total_ht"), fac.base_ht):
        if valeur is not None:
            fac.montant_total_facture = valeur
            break
    if fac.montant_total_facture is None:
        fac.anomalies.append("TOTAL_DOCUMENT_NON_LU")
    if fac.net_a_payer is not None and fac.total_ttc is not None \
            and abs(fac.net_a_payer - fac.total_ttc) > 0.005:
        fac.anomalies.append(f"TOTAUX_DOCUMENT_DIFFERENTS(ttc={fac.total_ttc},net={fac.net_a_payer})")

    tableau = geo.reconstruire_tableau(lignes)
    fac.mode_lecture = tableau.mode
    for enr in tableau.enregistrements:
        fac.lignes.append(_ligne_depuis_enregistrement(enr, fac.periode_facture))
    # JAMAIS perdre un montant : une valeur que rien n'a pu rattacher devient une ligne, à classer.
    for evt in tableau.evenements_orphelins:
        valeur = geo.lire_nombre(evt.valeurs.get("TOTAL") or evt.valeurs.get("MONTANT")
                                 or evt.valeurs.get("PRIX") or "")
        if not valeur:
            continue
        ligne = LigneFacture("", 1, valeur, valeur, source_page=evt.page, quantite_deduite=True)
        ligne.libelle_source = "(montant sans libellé dans le document) " + " ".join(evt.valeurs.values())
        _ajouter_anomalie(ligne, "MONTANT_SANS_LIBELLE")
        fac.lignes.append(ligne)
        fac.anomalies.append("MONTANT_SANS_LIBELLE")
    for t in tableau.textes_non_rattaches:
        fac.anomalies.append("TEXTE_NON_RATTACHE:" + t[:40].replace(",", " "))

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


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


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
        if fmt in IDENTITES:
            fac = _extraire_generique(doc, nom, fmt)
        else:
            fac = FactureExtraite(nom, EX_NON_SUPPORTE, statut_extraction=EX_NON_SUPPORTE,
                                  anomalies=["FORMAT_FOURNISSEUR_NON_SUPPORTE"])
        fac.sha256_pdf = sha
        fac.source_pages = pages
        _controle_reconciliation(fac)
        return fac
    finally:
        doc.close()


def _recalculer_lignes_incoherentes(fac: FactureExtraite) -> None:
    """Une ligne dont le montant écrit contredit « quantité × prix unitaire », sur une facture
    dont le total ne se retrouve PAS avec les montants écrits mais se retrouve EXACTEMENT avec
    quantité × prix : c'est le montant de ligne qui est faux (erreur de saisie du fournisseur),
    pas la quantité ni le prix. Il est recalculé, le montant lu est conservé (`montant_lu`) et la
    ligne comme la facture le disent. Dans tout autre cas, rien n'est touché : l'écart reste
    visible et c'est à l'humain de trancher.
    """
    total = fac.montant_total_facture
    incoherentes = [l for l in fac.lignes if "LIGNE_NB_PU_TOTAL_INCOHERENT" in (l.code_anomalie or "")
                    and None not in (l.quantite, l.prix_unitaire, l.montant_ligne)]
    if total is None or not incoherentes:
        return
    somme_lue = sum(l.montant_ligne or 0 for l in fac.lignes)
    somme_recalculee = somme_lue + sum(l.quantite * l.prix_unitaire - l.montant_ligne
                                       for l in incoherentes)
    if abs(somme_lue - total) <= 0.005 or abs(somme_recalculee - total) > 0.005:
        return
    for l in incoherentes:
        l.montant_lu = l.montant_ligne
        l.montant_ligne = round(l.quantite * l.prix_unitaire, 2)
        _ajouter_anomalie(l, f"MONTANT_LIGNE_RECALCULE(lu={l.montant_lu},calcule={l.montant_ligne})")
    fac.anomalies.append("MONTANTS_LIGNES_RECALCULES_QUANTITE_X_PRIX")


def _controle_reconciliation(fac: FactureExtraite) -> None:
    if fac.statut_extraction != EX_OK or fac.montant_total_facture is None:
        return
    _recalculer_lignes_incoherentes(fac)
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

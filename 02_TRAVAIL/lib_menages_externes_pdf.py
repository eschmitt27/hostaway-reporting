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


MAPPING_LOGEMENTS: dict[str, str] = {
    normaliser_libelle("studio 76 (Dureuil)"): "LOG_0014",
    normaliser_libelle("studio - cote pave (Francois)"): "LOG_0012",
    normaliser_libelle("studio Puits verts (Caroline)"): "LOG_0006",
    normaliser_libelle("studio Puits vert (Caroline)"): "LOG_0006",
    normaliser_libelle("T3 310 muret (David)"): "LOG_0011",
    normaliser_libelle("studio st Pierre (Florane)"): "LOG_0010",
    normaliser_libelle("T2 9 rue du Toul"): "LOG_0007",
    normaliser_libelle("T3 4 rue engalieres"): "LOG_0009",
    normaliser_libelle("T3 20 rue l'Amiral Galache"): "LOG_0016",
    normaliser_libelle("T.4-90 Blagnac (Cedrine)"): "LOG_0002",
    normaliser_libelle("T.3 Sept Deniers (Francois)"): "LOG_0013",
    normaliser_libelle("T.2-65 (Gabriel)"): "LOG_0003",
}


def mapper_logement(libelle_source: str) -> tuple[str | None, str]:
    cle = normaliser_libelle(libelle_source)
    return MAPPING_LOGEMENTS.get(cle), cle


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
    return EX_NON_SUPPORTE


# ── AISSATA ──────────────────────────────────────────────────────────────────
_RE_MONTANT = re.compile(r"(\d{1,3},\d{2})\s*€\s*x\s*(\d+)")
_RE_LIGNE_NUM = re.compile(r"^(\d+)\s*\.?\s*service de nettoyage\s*(.*)$", re.IGNORECASE)
_RE_DATE = re.compile(r"le\s+(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+(\d{4})", re.IGNORECASE)
_RE_MULTI = re.compile(r"le\s+(\d{1,2})\s+\w+\s+et\s+le\s+(\d{1,2})", re.IGNORECASE)


def _extraire_aissata(doc, nom: str) -> FactureExtraite:
    page = doc[0]
    lignes_y = _lignes_par_y(page)
    texte = _despace(page.get_text())
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
    # Total NET A PAYER = plus grand montant X,dd de la facture (Aissata : 1 439,00).
    tous = _nombres_virgule(texte)
    fac.montant_total_facture = max(tous) if tous else None

    descriptions, montants = [], []
    for y, t in lignes_y:
        for mm in _RE_MONTANT.finditer(t):
            montants.append((y, _to_float(mm.group(1)), int(mm.group(2))))
        md2 = _RE_LIGNE_NUM.match(t.strip())
        if md2:
            descriptions.append((y, int(md2.group(1)), md2.group(2).strip()))

    used = set()
    for y, num, lib in sorted(descriptions, key=lambda d: d[1]):
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
        lib_clean = _RE_MONTANT.sub("", lib)
        lib_clean = re.split(r"\bx\s*\d+\s*passages|\ble\s+\d", lib_clean, 1, flags=re.IGNORECASE)[0]
        # Description wrappee sur 2 lignes -> fragment de parenthese non fermee / << x >> isole a nettoyer.
        lib_clean = re.sub(r"\([^)]*$", "", lib_clean)          # parenthese-adresse ouverte non fermee
        lib_clean = re.sub(r"\(\s*\d[^)]*\)", "", lib_clean)    # parenthese-ADRESSE fermee (commence par chiffre)
        lib_clean = re.sub(r"\s+x\s*$", "", lib_clean)          # << x >> orphelin de fin de ligne
        lib_clean = lib_clean.replace("’", "'").replace("`", "'")   # apostrophe typographique -> droite
        lib_clean = re.sub(r"\s+", " ", lib_clean).strip(" .")
        ligne = LigneFacture(lib_clean, qte, pu,
                             round(pu * qte, 2) if (pu is not None and qte is not None) else None)
        if _RE_MULTI.search(_strip_accents(lib).lower()):
            ligne.precision_date = "MULTI_DATES"
        else:
            pr = _RE_DATE.search(_strip_accents(lib).lower())
            if pr:
                mo = MOIS_FR.get(pr.group(2))
                if mo:
                    ligne.date_menage = f"{int(pr.group(3)):04d}-{mo:02d}-{int(pr.group(1)):02d}"
                    ligne.precision_date = "DATE_PRECISE"
        lid, cle = mapper_logement(lib_clean)
        ligne.logement_id, ligne.libelle_normalise = lid, cle
        if lid is None:
            ligne.code_anomalie = "LOGEMENT_FACTURE_EXTERNE_NON_RECONNU"
        if pu is None or qte is None:
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
    page = doc[0]
    lignes_y = _lignes_par_y(page)
    texte = _despace(page.get_text())
    texte_na = _strip_accents(texte).lower()

    fac = FactureExtraite(nom, "MOUNIR", "INT_0003", "MH Entreprise")
    m = re.search(r"numero de facture\s*:\s*(\S+)", texte_na)
    fac.numero_facture = m.group(1) if m else None
    md = re.search(r"date de facturation\s*:\s*(\d{1,2})/(\d{1,2})/(\d{4})", texte_na)
    if md:
        fac.date_facture = f"{int(md.group(3)):04d}-{int(md.group(2)):02d}-{int(md.group(1)):02d}"
        fac.periode_facture = fac.date_facture[:7]
    mt = re.search(r"net a payer ttc\s*(\d+)\s*€", texte_na)
    fac.montant_total_facture = _to_float(mt.group(1)) if mt else None

    # Bornes verticales : chaque appartement occupe [Y_i, Y_{i+1}) ; le dernier s'arrete
    # a la zone "conditions/net a payer". On garde le libelle PUR (avant le 1er montant EUR)
    # pour pouvoir le retirer du segment : sinon les chiffres du nom (<< T.2-65 >>) polluent nb/PU.
    appart_ys = []
    for y, t in lignes_y:
        ts = t.strip()
        if _RE_APPART.match(ts) and "wonderbnb" not in ts.lower():
            lib_pur = re.split(r"\d+\s*€", ts, 1)[0].strip()          # nom sans montants
            lib_pur = lib_pur.replace("’", "'").replace("`", "'")
            appart_ys.append((y, lib_pur))
    borne_fin = min([y for y, t in lignes_y
                     if re.search(r"conditions de paiement|net a payer", _strip_accents(t).lower())]
                    + [10000])

    for i, (y0, lib) in enumerate(appart_ys):
        y1 = appart_ys[i + 1][0] if i + 1 < len(appart_ys) else borne_fin
        seg = " ".join(t for yy, t in lignes_y if y0 <= yy < y1)
        seg_valeurs = seg.replace(lib, " ", 1)                        # retire le nom (et ses chiffres)
        euros = [v for v in (_to_float(x) for x in _RE_EURO.findall(seg_valeurs)) if v is not None]
        tous = []
        for tok in _despace(seg_valeurs).split():
            tok2 = tok.replace("€", "")
            if re.fullmatch(r"\d+", tok2):
                tous.append(int(tok2))
        total = max(euros) if euros else None
        restants = list(tous)
        if total is not None and int(total) in restants:
            restants.remove(int(total))
        pu = float(max(restants)) if restants else None
        nb = min(restants) if restants else None
        ligne = LigneFacture(lib, nb, pu, total, precision_date="MOIS_FACTURE")
        lid, cle = mapper_logement(lib)
        ligne.logement_id, ligne.libelle_normalise = lid, cle
        if lid is None:
            ligne.code_anomalie = "LOGEMENT_FACTURE_EXTERNE_NON_RECONNU"
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

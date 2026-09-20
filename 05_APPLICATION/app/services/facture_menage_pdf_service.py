"""PDF facture prestataire ménage externe → SQLite (mission Ménages §8/§26 — pont Lot6c).

N'EXTRAIT RIEN ICI. L'extraction (texte natif PyMuPDF, formats AISSATA/MOUNIR, réconciliation,
empreinte) est `lib_menages_externes_pdf.extraire_pdf` (02_TRAVAIL), déjà construite et testée
(21 tests, `tests/test_menages_pdf_extraction.py`) — ce module adapte seulement sa SORTIE
(`FactureExtraite`) vers `factures` (0017) + `facture_lignes_menage` (0037), sans passer par un
Excel intermédiaire.

IDEMPOTENCE (mission §34)
Réutilise le mécanisme DÉJÀ existant de `factures_service.creer()` : une facture est identifiée par
(fournisseur, référence), et `E_DOUBLON_CERTAIN` refuse une seconde création. `numero_facture` et
`prestataire_id` sont dérivés du contenu du PDF par l'extracteur — un même PDF réimporté (même
contenu, même nom ou non) redonne la même paire (fournisseur, référence) et se fait donc refuser par
le mécanisme existant, sans détection ad hoc ici.

RAPPROCHEMENT DU LOGEMENT (§23-25)
L'extracteur ne devine plus de logement : il rend le libellé brut du document. C'est ce module qui
appelle `logement_matching_service` sur le RÉFÉRENTIEL VIVANT, ligne par ligne, et conserve avec la
ligne la confiance obtenue et la voie empruntée. Un libellé reconnu avec certitude affecte la
ligne ; un libellé probable l'affecte aussi, mais marqué à confirmer ; un libellé inconnu ne
l'affecte pas — il n'est jamais rattaché « au plus proche ».

VENTILATION
Chaque ligne SANS logement reconnu devient une ligne FRAIS_NON_AFFECTE, conservée telle quelle
(§32 : la ligne originale reste visible) ; sa répartition sur les autres logements de la facture
est déléguée à `facture_ventilation_menage_service`.
"""
from __future__ import annotations

import re
import sqlite3
import sys
import unicodedata
import uuid
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import facture_interpretation_service as interpretation
from app.services import facture_lignes_menage_service as flm
from app.services import facture_md_service as md_svc
from app.services import facture_ventilation_menage_service as vent
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs
from app.services import logement_matching_service as lms

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

E_EXTRACTION_ECHOUEE = "EXTRACTION_ECHOUEE"
#: Anomalie portée par une facture dont le numéro fournisseur est aussi celui d'une autre facture.
A_NUMERO_FACTURE_REUTILISE = "NUMERO_FACTURE_REUTILISE"
#: Anomalie du DOCUMENT (pas du parseur) : une ligne imprimée contredit quantité × prix unitaire.
A_INCOHERENCE_ARITHMETIQUE = "INCOHERENCE_ARITHMETIQUE_DOCUMENT"
#: Deux documents du même numéro, ni identiques ni clairement distincts : un humain tranche.
A_VERSION_A_CONFIRMER = "VERSION_A_CONFIRMER"


def _facture_active(fournisseur_id_opaque: str, facture_ref: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM factures WHERE fournisseur_id_opaque = ? AND facture_ref = ? "
            "AND statut <> ? ORDER BY id DESC LIMIT 1",
            (fournisseur_id_opaque, facture_ref, fact.ST_ANNULEE),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


MEME_NUMERO_DOUBLON = "DOUBLON_EXACT"
MEME_NUMERO_NOUVELLE_VERSION = "NOUVELLE_VERSION"
MEME_NUMERO_DISTINCTE = "FACTURE_DISTINCTE"
MEME_NUMERO_AMBIGU = "VERSION_AMBIGUE"

#: Recouvrement (part des lignes du plus court document retrouvées dans l'autre) au-delà duquel
#: les deux pièces racontent la même prestation.
SEUIL_RECOUVREMENT_VERSION = 0.80
#: En deçà, les deux documents ne parlent manifestement pas de la même chose.
SEUIL_RECOUVREMENT_DISTINCTE = 0.30
#: Différence de nombre de lignes encore compatible avec « un ménage ajouté ou retiré ».
ECART_LIGNES_VERSION = 0.20


def _sans_accent(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s))
                   if unicodedata.category(c) != "Mn")


def _cle_ligne(libelle: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _sans_accent(libelle).lower()).strip()


def _lignes_enregistrees(facture_id_opaque: str, db_path=None) -> list[tuple[str, float]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT COALESCE(libelle_source, description, '') AS texte, montant_ttc "
            "FROM facture_lignes_menage WHERE facture_id_opaque = ?",
            (facture_id_opaque,)).fetchall()
    finally:
        conn.close()
    return [(_cle_ligne(r["texte"]), round(float(r["montant_ttc"] or 0), 2)) for r in rows]


def _communs(a: list[tuple[str, float]], b: list[tuple[str, float]]) -> int:
    reste = list(b)
    n = 0
    for item in a:
        if item in reste:
            reste.remove(item)
            n += 1
    return n


def similarite_lignes(a: list[tuple[str, float]], b: list[tuple[str, float]]) -> float:
    """Part des prestations communes aux deux documents (Jaccard sur libellé + montant)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    communs = _communs(a, b)
    return communs / float(len(a) + len(b) - communs)


def recouvrement_lignes(a: list[tuple[str, float]], b: list[tuple[str, float]]) -> float:
    """Part des lignes du PLUS COURT document qui se retrouvent telles quelles dans l'autre.

    C'est ce qui distingue « un ménage a été ajouté » (recouvrement 1 : tout l'ancien est dans le
    nouveau) de « ce sont deux factures sans rapport » (recouvrement 0). Aucun modèle, aucun
    apprentissage : un comptage.
    """
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return _communs(a, b) / float(min(len(a), len(b)))


def _sha_connu(facture_id_opaque: str, db_path=None) -> str | None:
    """Empreinte du PDF qui a produit cette facture, si un import l'a tracée."""
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT sha256_pdf FROM facture_pdf_diagnostics WHERE facture_id_opaque = ? "
            "AND sha256_pdf IS NOT NULL ORDER BY id DESC LIMIT 1", (facture_id_opaque,)).fetchone()
        return r["sha256_pdf"] if r else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def nature_meme_numero(active: dict[str, Any], fac, *, db_path=None) -> str:
    """Un document arrive avec le numéro IMPRIMÉ d'une facture déjà active du même fournisseur.

    Le numéro ne dit pas s'il s'agit du même document — un fournisseur réutilise parfois le sien
    par mégarde. Ce qui décide, ce sont le fichier, la période et le CONTENU :

      · même empreinte de fichier               → DOUBLON_EXACT : une seule facture, rien à créer ;
      · autre période                           → FACTURE_DISTINCTE ;
      · même période, tout l'ancien se retrouve
        dans le nouveau à une ligne près        → NOUVELLE_VERSION (remplacement V1 → V2) ;
      · même période, lignes sans rapport        → FACTURE_DISTINCTE ;
      · entre les deux                          → VERSION_AMBIGUE : on n'annule RIEN, les deux
        factures sont conservées et un humain tranche.

    Ce sont les LIGNES qui décident, pas le montant : un ménage ajouté fait bouger le total sans
    faire de la pièce une autre facture.
    """
    sha_actif = _sha_connu(active["facture_id_opaque"], db_path=db_path)
    if sha_actif and getattr(fac, "sha256_pdf", None) and sha_actif == fac.sha256_pdf:
        return MEME_NUMERO_DOUBLON

    mois_actif = str(active.get("date_facture") or "")[:7]
    mois_nouveau = str(getattr(fac, "date_facture", None) or "")[:7]
    if mois_actif and mois_nouveau and mois_actif != mois_nouveau:
        return MEME_NUMERO_DISTINCTE

    anciennes = _lignes_enregistrees(active["facture_id_opaque"], db_path=db_path)
    nouvelles = [(_cle_ligne(getattr(l, "libelle_source", "") or l.logement_source),
                  round(float(l.montant_ligne or 0), 2)) for l in getattr(fac, "lignes", [])]
    recouvrement = recouvrement_lignes(anciennes, nouvelles)
    ecart_lignes = abs(len(anciennes) - len(nouvelles))
    marge = max(1, int(ECART_LIGNES_VERSION * max(len(anciennes), len(nouvelles))))

    ancien, nouveau = active.get("montant_ttc"), getattr(fac, "montant_total_facture", None)
    if (recouvrement >= 0.999 and ecart_lignes == 0 and ancien is not None and nouveau is not None
            and abs(float(nouveau) - float(ancien)) <= 0.005):
        return MEME_NUMERO_DOUBLON            # même contenu, fichier simplement réexporté
    if recouvrement >= SEUIL_RECOUVREMENT_VERSION and ecart_lignes <= marge:
        return MEME_NUMERO_NOUVELLE_VERSION
    if recouvrement < SEUIL_RECOUVREMENT_DISTINCTE:
        return MEME_NUMERO_DISTINCTE
    return MEME_NUMERO_AMBIGU


def _confronter(homonymes: list[dict[str, Any]], fac,
                *, db_path=None) -> tuple[dict[str, Any] | None, str | None]:
    """Face à PLUSIEURS factures du même numéro imprimé, celle à laquelle comparer le document.

    Le doublon exact l'emporte sur tout : si l'une d'elles vient du même fichier, il n'y a rien à
    créer. Sinon on compare à celle de la même période — c'est avec elle qu'une nouvelle version
    aurait un sens — à défaut à la plus récemment enregistrée.
    """
    if not homonymes:
        return None, None
    natures = [(f, nature_meme_numero(f, fac, db_path=db_path)) for f in homonymes]
    for f, nature in natures:
        if nature == MEME_NUMERO_DOUBLON:
            return f, nature
    mois = str(getattr(fac, "date_facture", None) or "")[:7]
    for f, nature in natures:
        if mois and str(f.get("date_facture") or "")[:7] == mois:
            return f, nature
    return natures[-1]


_SUFFIXES = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def factures_par_ref_source(fournisseur: str, ref_source: str, db_path=None) -> list[dict[str, Any]]:
    """Factures actives du fournisseur portant ce numéro IMPRIMÉ, quelle que soit leur référence
    interne."""
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM factures WHERE fournisseur_id_opaque = ? "
            "AND COALESCE(facture_ref_source, facture_ref) = ? AND statut <> ? ORDER BY id",
            (fournisseur, ref_source, fact.ST_ANNULEE)).fetchall()]
    finally:
        conn.close()


def _renommer_reference_interne(facture: dict[str, Any], nouvelle: str, *, acteur: str,
                                db_path=None) -> bool:
    """Donne son suffixe (« -A ») à la facture déjà enregistrée. Le numéro IMPRIMÉ n'est pas touché.

    Seule une facture encore en contrôle est renommée : une facture validée ou réglée est déjà
    citée ailleurs, on ne la renomme pas dans son dos — la nouvelle prend alors le suffixe suivant.
    """
    if facture["statut"] not in (fact.ST_BROUILLON, fact.ST_A_CONTROLER):
        return False
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE factures SET facture_ref = ?, date_modification = "
                     "strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE facture_id_opaque = ?",
                     (nouvelle, facture["facture_id_opaque"]))
        conn.execute(
            "INSERT INTO facture_evenements (facture_id_opaque, type_evenement, ancien_statut, "
            "nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
            (facture["facture_id_opaque"], "MODIFICATION", facture["statut"], facture["statut"],
             f"Référence interne « {facture['facture_ref']} » → « {nouvelle} » : le fournisseur a "
             f"réutilisé le numéro {facture.get('facture_ref_source') or facture['facture_ref']}. "
             f"Le numéro imprimé sur la pièce est inchangé.", acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    facture["facture_ref"] = nouvelle
    return True


def reference_interne_libre(fournisseur: str, ref_source: str, *, acteur: str = "",
                            db_path=None) -> str:
    """Référence INTERNE du document qui arrive, quand son numéro imprimé est déjà pris.

    La première facture reçoit « -A » (si elle est encore en contrôle), la suivante « -B », etc.
    Le numéro imprimé, lui, reste celui du fournisseur pour toutes.
    """
    existantes = factures_par_ref_source(fournisseur, ref_source, db_path=db_path)
    if not existantes:
        return ref_source
    if len(existantes) == 1 and existantes[0]["facture_ref"] == ref_source:
        _renommer_reference_interne(existantes[0], f"{ref_source}-{_SUFFIXES[0]}", acteur=acteur,
                                    db_path=db_path)
    prises = {f["facture_ref"] for f in
              factures_par_ref_source(fournisseur, ref_source, db_path=db_path)}
    for lettre in _SUFFIXES:
        candidat = f"{ref_source}-{lettre}"
        if candidat not in prises:
            return candidat
    return f"{ref_source}-{uuid.uuid4().hex[:4].upper()}"


def _remplacer(active: dict[str, Any], fac, form: dict[str, Any], *, acteur: str,
               db_path=None) -> dict[str, Any] | None:
    """V1 ANNULÉE (jamais supprimée, tracée), V2 reprend SA référence interne."""
    ancien_montant, nouveau_montant = active.get("montant_ttc"), fac.montant_total_facture
    annulation = fact.changer_statut(
        active["facture_id_opaque"], fact.ST_ANNULEE, acteur=acteur,
        commentaire=f"Remplacée automatiquement par une nouvelle version du PDF "
                   f"({fac.nom_fichier_source}) — montant {ancien_montant} -> {nouveau_montant}",
        db_path=db_path)
    if not annulation.get("ok"):
        return None
    resultat = fact.creer({**form, "facture_ref": active["facture_ref"]}, acteur=acteur,
                          db_path=db_path,
                          fournisseur_actif=_fournisseur_actif(fac.prestataire_id, db_path=db_path))
    if resultat.get("ok"):
        resultat["remplacement_de"] = active["facture_id_opaque"]
    return resultat


def _tenter_remplacement_v1_v2(fac, form: dict[str, Any], *, acteur: str,
                               db_path=None) -> dict[str, Any] | None:
    """Réémission corrigée d'un document déjà connu. Tout ce qui n'est PAS une nouvelle version
    (doublon exact, facture distincte, cas ambigu) rend None : rien n'est annulé."""
    active = _facture_active(fac.prestataire_id, fac.numero_facture, db_path=db_path)
    if active is None or nature_meme_numero(active, fac,
                                            db_path=db_path) != MEME_NUMERO_NOUVELLE_VERSION:
        return None
    return _remplacer(active, fac, form, acteur=acteur, db_path=db_path)


def _fournisseur_actif(prestataire_id: str, db_path=None) -> bool | None:
    """Statut du fournisseur si le référentiel le connaît, None sinon — jamais deviné."""
    f = frs.charger_par_opaque(prestataire_id, db_path=db_path)
    if f is None:
        return None
    return f.get("statut") == "ACTIF"


def _enregistrer_diagnostic(fac, *, facture_id_opaque: str | None, db_path=None) -> None:
    """Trace CHAQUE tentative d'import (succès ou échec) — même grain que l'onglet DIAGNOSTIC_PDF
    legacy (0040) : mode_extraction est toujours PDF_AUTOMATIQUE ici, ce module n'a pas de secours
    de saisie manuelle (celui-ci reste, s'il existe, un mécanisme applicatif distinct non retouché)."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO facture_pdf_diagnostics (nom_fichier, format_detecte, statut_extraction, "
            "numero_facture, montant_total, somme_lignes, ecart_reconciliation, nb_lignes, "
            "anomalies, mode_extraction, facture_id_opaque, sha256_pdf) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (fac.nom_fichier_source, fac.format_detecte, fac.statut_extraction, fac.numero_facture,
             fac.montant_total_facture, fac.somme_lignes, fac.ecart_reconciliation,
             len(fac.lignes), ",".join(fac.anomalies) or None, "PDF_AUTOMATIQUE",
             facture_id_opaque, getattr(fac, "sha256_pdf", None) or None))
        conn.commit()
    finally:
        conn.close()


PREFIXE_CONTRADICTION_NOM = "NOM_FICHIER_"


def contradictions_nom_fichier(facture_id_opaque: str, *, db_path=None) -> list[dict[str, str]]:
    """§18 — contradictions entre le NOM du PDF et son CONTENU, relevées au dernier import.

    Source unique de l'écran de la facture et du catalogue des contrôles. Chaque entrée porte son
    code (stable, pour les tests et l'observabilité) et une phrase lisible par l'utilisateur.
    """
    import sqlite3

    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT anomalies FROM facture_pdf_diagnostics WHERE facture_id_opaque = ? "
            "ORDER BY id DESC LIMIT 1", (facture_id_opaque,)).fetchone()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    sortie = []
    for code in ((r["anomalies"] or "").split(",") if r else []):
        if not code.startswith(PREFIXE_CONTRADICTION_NOM):
            continue
        nature, _, valeur = code.partition(":")
        if nature == "NOM_FICHIER_PERIODE_CONTRADICTOIRE":
            libelle = (f"Le nom du fichier indique la période {valeur}, qu'aucune date du document "
                       "ne porte : vérifier la période sur la facture elle-même.")
        elif nature == "NOM_FICHIER_PRESTATAIRE_CONTRADICTOIRE":
            libelle = (f"Le nom du fichier indique le prestataire « {valeur} », différent de celui "
                       "que porte le document : vérifier le prestataire sur la facture.")
        else:
            libelle = "Le nom du fichier contredit le contenu du document."
        sortie.append({"code": nature, "libelle": libelle})
    return sortie


def anomalies_document(facture_id_opaque: str, *, db_path=None) -> list[dict[str, str]]:
    """Anomalies portées par le DOCUMENT lui-même — pas par le parseur — à montrer à l'humain.

    · NUMERO_FACTURE_REUTILISE : calculée EN DIRECT (autres factures actives du même fournisseur
      sous le même numéro), donc visible des DEUX côtés sans jamais écrire sur la première.
    · INCOHERENCE_ARITHMETIQUE_DOCUMENT : relevée au dernier import (une ligne imprimée contredit
      quantité × prix unitaire). Le montant imprimé est conservé ; la correction n'est qu'une
      suggestion, à accepter ou refuser par l'humain.
    """
    import sqlite3

    conn = get_db(db_path)
    try:
        f = conn.execute(
            "SELECT fournisseur_id_opaque, facture_ref, "
            "COALESCE(facture_ref_source, facture_ref) AS ref_source FROM factures "
            "WHERE facture_id_opaque = ?", (facture_id_opaque,)).fetchone()
        if f is None:
            return []
        autres = conn.execute(
            "SELECT facture_id_opaque, facture_ref, date_facture, montant_ttc, statut "
            "FROM factures WHERE fournisseur_id_opaque = ? "
            "AND COALESCE(facture_ref_source, facture_ref) = ? AND facture_id_opaque <> ? "
            "AND statut <> ? ORDER BY date_facture, id",
            (f["fournisseur_id_opaque"], f["ref_source"], facture_id_opaque,
             fact.ST_ANNULEE)).fetchall()
        try:
            diag = conn.execute(
                "SELECT anomalies FROM facture_pdf_diagnostics WHERE facture_id_opaque = ? "
                "ORDER BY id DESC LIMIT 1", (facture_id_opaque,)).fetchone()
        except sqlite3.OperationalError:
            diag = None
    finally:
        conn.close()
    sortie = []
    for a in autres:
        sortie.append({"code": A_NUMERO_FACTURE_REUTILISE, "facture_id_opaque": a["facture_id_opaque"],
                       "libelle": (
                           f"Le fournisseur a réutilisé le numéro {f['ref_source']} : il porte aussi "
                           f"la facture {a['facture_ref']} ({a['facture_id_opaque']}, date "
                           f"{a['date_facture'] or '?'}, période {str(a['date_facture'] or '')[:7] or '?'}, "
                           f"total {a['montant_ttc']} €, statut {a['statut']}). Les deux documents "
                           f"sont conservés ; celui-ci est enregistré sous la référence interne "
                           f"{f['facture_ref']}. Le numéro imprimé, lui, n'est jamais modifié.")})
    for code in ((diag["anomalies"] or "").split(",") if diag else []):
        nature, _, detail = code.partition(":")
        if nature == A_VERSION_A_CONFIRMER:
            sortie.append({"code": nature, "facture_id_opaque": detail, "libelle": (
                f"Ce document ressemble en partie à la facture {detail} du même numéro sans lui "
                "être identique : nouvelle version ou facture distincte ? Rien n'a été annulé — "
                "à trancher.")})
        if nature == A_INCOHERENCE_ARITHMETIQUE:
            champs = dict(x.split("=", 1) for x in detail.split(":") if "=" in x)
            sortie.append({"code": nature, "libelle": (
                f"Ligne {champs.get('ligne', '?')} : le document imprime {champs.get('imprime', '?')} € "
                f"alors que quantité × prix unitaire = {champs.get('calcule', '?')} €. Le montant "
                "imprimé est conservé ; corriger la ligne, ajouter une ligne corrective ou demander "
                "une facture corrigée au fournisseur.")})
    return sortie


def importer(path, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Importe une facture PDF ménage externe : header + lignes + ventilation, aucune Charge créée.

    Statut de la facture créée : toujours `A_CONTROLER` (défaut de `factures_service.creer`) — une
    ventilation, même appliquée sans écart, ne valide jamais seule la facture (§31).
    """
    import lib_menages_externes_pdf as pdfex

    fac = pdfex.extraire_pdf(path)
    if fac.statut_extraction != "OK":
        _enregistrer_diagnostic(fac, facture_id_opaque=None, db_path=db_path)
        return {"ok": False, "code": E_EXTRACTION_ECHOUEE, "statut_extraction": fac.statut_extraction,
                "anomalies": fac.anomalies}

    form = {
        "fournisseur_id_opaque": fac.prestataire_id,
        # Référence INTERNE (désambiguïsée plus bas si le numéro imprimé est déjà pris) et numéro
        # IMPRIMÉ sur la pièce, conservé tel quel quoi qu'il arrive.
        "facture_ref": fac.numero_facture,
        "facture_ref_source": fac.numero_facture,
        "date_facture": fac.date_facture,
        "montant_ttc": fac.montant_total_facture,
        "devise": fac.devise,
        "source": "PDF_EXTRACTION",
        # Provenance canonique : le NOM du PDF déposé dans le dossier surveillé. C'est par lui que
        # l'écran, et le rechargement du dossier, retrouvent le document d'origine.
        "justificatif": fac.nom_fichier_source,
        "commentaire": fac.nom_fichier_source,
    }
    actif = _fournisseur_actif(fac.prestataire_id, db_path=db_path) if fac.prestataire_id else None
    autre_facture = None
    # Le numéro imprimé est-il DÉJÀ porté par une facture active de ce fournisseur ? C'est cette
    # question — et non l'échec d'une contrainte d'unicité — qui ouvre le tri : même fichier,
    # nouvelle version, ou deux factures réellement distinctes.
    homonymes = (factures_par_ref_source(fac.prestataire_id, fac.numero_facture, db_path=db_path)
                 if fac.prestataire_id and fac.numero_facture else [])
    candidate, nature = _confronter(homonymes, fac, db_path=db_path)
    if nature == MEME_NUMERO_DOUBLON:
        _enregistrer_diagnostic(fac, facture_id_opaque=None, db_path=db_path)
        return {"ok": False, "code": fact.E_DOUBLON_CERTAIN,
                "message": "Cette facture est déjà enregistrée.",
                "facture_existante": candidate["facture_id_opaque"]}
    if nature == MEME_NUMERO_NOUVELLE_VERSION:
        resultat = _remplacer(candidate, fac, form, acteur=acteur, db_path=db_path) or {
            "ok": False, "code": "REMPLACEMENT_IMPOSSIBLE"}
    else:
        if nature in (MEME_NUMERO_DISTINCTE, MEME_NUMERO_AMBIGU):
            # Le fournisseur a réutilisé son numéro : les deux factures existent, chacune sous une
            # référence interne désambiguïsée (« …-A », « …-B »). Le numéro imprimé reste commun,
            # aucune facture n'est annulée ni remplacée.
            form["facture_ref"] = reference_interne_libre(
                fac.prestataire_id, fac.numero_facture, acteur=acteur, db_path=db_path)
            autre_facture = candidate["facture_id_opaque"]
            fac.anomalies.append(f"{A_NUMERO_FACTURE_REUTILISE}:{autre_facture}")
            if nature == MEME_NUMERO_AMBIGU:
                fac.anomalies.append(f"{A_VERSION_A_CONFIRMER}:{autre_facture}")
        resultat = fact.creer(form, acteur=acteur, db_path=db_path, fournisseur_actif=actif)
    if not resultat.get("ok"):
        codes = [resultat.get("code")] + [e.get("code") for e in (resultat.get("erreurs") or [])]
        _enregistrer_diagnostic(fac, facture_id_opaque=None, db_path=db_path)
        # Un PDF déjà importé n'est pas une « saisie invalide » : c'est le cas nominal d'un
        # dossier rescanné. Le dire par son code propre évite de renvoyer l'utilisateur vers un
        # formulaire à corriger pour un fichier qui n'a rien d'anormal.
        if fact.E_DOUBLON_CERTAIN in codes:
            return {**resultat, "code": fact.E_DOUBLON_CERTAIN,
                    "message": "Cette facture est déjà enregistrée."}
        return resultat

    facture_id = resultat["facture_id_opaque"]
    _enregistrer_diagnostic(fac, facture_id_opaque=facture_id, db_path=db_path)

    # Le MD structuré, s'il est là et valide, PREND LA MAIN : les lignes viennent de lui, et
    # jamais un mélange des deux lectures (contrat FACTURE_FOURNISSEUR_MD_V1).
    analyse_md = md_svc.analyser(path, db_path=db_path, pdf_sha256=fac.sha256_pdf)
    if analyse_md["etat"] == md_svc.ETAT_VALIDE:
        from app.services import referentiel_logements_export_service as ref_export

        lignes_md = md_svc.lignes_canoniques(
            analyse_md["donnees"], logements_connus=ref_export.logements_connus(db_path=db_path))
        interpretation._poser_lignes_md(
            facture_id, lignes_md, acteur=acteur, nom_prestataire=fac.nom_prestataire or "",
            db_path=db_path)
        pose = interpretation.enregistrer_version_initiale(
            facture_id, source=interpretation.SOURCE_MD, nb_lignes=len(lignes_md), md=analyse_md,
            pdf_sha256=fac.sha256_pdf,
            motif=f"Interprétation structurée {analyse_md['md_nom_fichier']}",
            acteur=acteur, db_path=db_path)
        pose["anomalies"] = analyse_md["anomalies"]
        controle = flm.controler_total(facture_id, db_path=db_path)
        return {"ok": True, "facture_id_opaque": facture_id, "statut": resultat["statut"],
                "nb_lignes": len(lignes_md), "controle_total": controle, "ventilations": [],
                "anomalies_extraction": fac.anomalies + list(pose.get("anomalies") or []),
                "numero_reutilise_de": autre_facture,
                "remplacement_de": resultat.get("remplacement_de"),
                "source_interpretation": interpretation.SOURCE_MD,
                "md_etat": analyse_md["etat"], "md_nom_fichier": analyse_md["md_nom_fichier"],
                "mois_impacte": str(fac.date_facture or "")[:7] or None}

    ventilations = _poser_lignes_pdf(facture_id, fac, acteur=acteur, db_path=db_path)
    interpretation.enregistrer_version_initiale(
        facture_id, source=interpretation.SOURCE_PDF, nb_lignes=len(fac.lignes), md=analyse_md,
        pdf_sha256=fac.sha256_pdf, motif="Extraction du PDF par le moteur déterministe",
        acteur=acteur, db_path=db_path)
    controle = flm.controler_total(facture_id, db_path=db_path)
    return {"ok": True, "facture_id_opaque": facture_id, "statut": resultat["statut"],
            "nb_lignes": len(fac.lignes), "controle_total": controle, "ventilations": ventilations,
            "anomalies_extraction": fac.anomalies,
            "numero_reutilise_de": autre_facture,
            "remplacement_de": resultat.get("remplacement_de"),
            "source_interpretation": interpretation.SOURCE_PDF,
            "md_etat": analyse_md["etat"],
            "md_nom_fichier": analyse_md["md_nom_fichier"] if analyse_md["md_present"] else None,
            "mois_impacte": str(fac.date_facture or "")[:7] or None}


def _poser_lignes_pdf(facture_id: str, fac, *, acteur: str = "", db_path=None) -> list[dict[str, Any]]:
    """Écrit les lignes que le PARSEUR a lues, et ventile ce qui n'a pas de logement.

    Extrait de `importer()` pour être rejouable : quand un MD disparaît, la facture doit pouvoir
    revenir à cette lecture-ci sans repasser par la création de la facture.
    """
    # §23 — le rapprochement du logement se fait ICI, sur le référentiel vivant, et non plus dans
    # l'extracteur. `proposition` porte toujours sa confiance et sa raison : une ligne préremplie
    # « à confirmer » n'est pas une ligne certaine, et l'écran doit pouvoir le dire.
    referentiel = lms.charger_referentiel(db_path)
    for ligne in fac.lignes:
        proposition = lms.proposer(ligne.logement_source, referentiel)
        ligne.logement_id = proposition["logement_id"] if proposition["preremplir"] else None
        if not ligne.logement_id:
            ligne.code_anomalie = (
                (ligne.code_anomalie or "") + " | LOGEMENT_FACTURE_EXTERNE_NON_RECONNU"
            ).strip(" |")
        ecriture = flm.ajouter_ligne(
            facture_id,
            type_ligne=flm.TYPE_MENAGE_EXTERNE if ligne.logement_id else flm.TYPE_FRAIS_NON_AFFECTE,
            logement_id=ligne.logement_id or "",
            montant_ttc=ligne.montant_ligne or 0, description=ligne.logement_source,
            quantite=ligne.quantite, prix_unitaire=ligne.prix_unitaire,
            date_menage=ligne.date_menage or "", precision_date_menage=ligne.precision_date,
            nom_prestataire=fac.nom_prestataire or "",
            source=flm.SOURCE_PDF, acteur=acteur, db_path=db_path,
            logement_confiance=proposition["confiance"], logement_methode=proposition["methode"],
            # §8 et §9 — le texte du document, et la nature proposée par le parseur. La confiance
            # AUCUN se lit « le logiciel n'a pas compris ce libellé » : la ligne existe, et elle
            # attend un classement humain avant que la facture puisse être validée.
            libelle_source=getattr(ligne, "libelle_source", "") or "",
            categorie=getattr(ligne, "categorie", "") or "",
            categorie_confiance=getattr(ligne, "categorie_confiance", "") or "")
        # Un refus d'écriture ne doit JAMAIS passer inaperçu : c'est ainsi que deux lignes réelles
        # avaient disparu de la base tout en restant comptées dans le diagnostic d'extraction.
        if not ecriture.get("ok"):
            fac.anomalies.append(
                f"LIGNE_NON_ENREGISTREE:{ecriture.get('code')}:{ligne.logement_source[:40]}")

    ventilations = []
    non_affectees = [l for l in fac.lignes if not l.logement_id and (l.montant_ligne or 0) != 0]
    if non_affectees:
        base = flm.cout_menages_par_logement(facture_id, db_path=db_path)
        for ligne in non_affectees:
            ventilations.append(vent.ventiler_et_enregistrer(
                facture_id, ligne.montant_ligne or 0, base, acteur=acteur, db_path=db_path))
    return ventilations


def relire_pdf(facture_id_opaque: str, chemin_pdf, *, motif: str = "", acteur: str = "",
               db_path=None) -> dict[str, Any]:
    """Repose les lignes du PARSEUR sur une facture existante (retour au PDF après un MD retiré).

    Le document est relu par le moteur déterministe ; les lignes de la lecture précédente ont déjà
    été désactivées par `facture_interpretation_service.appliquer`.
    """
    import lib_menages_externes_pdf as pdfex

    fac = pdfex.extraire_pdf(chemin_pdf)
    if fac.statut_extraction != "OK":
        return {"ok": False, "code": E_EXTRACTION_ECHOUEE,
                "statut_extraction": fac.statut_extraction, "anomalies": fac.anomalies}
    resultat = interpretation.appliquer(
        facture_id_opaque, source=interpretation.SOURCE_PDF, lignes=list(fac.lignes),
        pdf_sha256=fac.sha256_pdf, motif=motif or "Retour à l'extraction PDF", acteur=acteur,
        db_path=db_path, poser=lambda: _poser_lignes_pdf(facture_id_opaque, fac, acteur=acteur,
                                                         db_path=db_path))
    return resultat

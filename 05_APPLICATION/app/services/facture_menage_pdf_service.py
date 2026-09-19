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

import sys
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import facture_lignes_menage_service as flm
from app.services import facture_ventilation_menage_service as vent
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs
from app.services import logement_matching_service as lms

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

E_EXTRACTION_ECHOUEE = "EXTRACTION_ECHOUEE"
E_NUMERO_FACTURE_REUTILISE = "NUMERO_FACTURE_REUTILISE"


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


def _tenter_remplacement_v1_v2(fac, form: dict[str, Any], *, acteur: str,
                               db_path=None) -> dict[str, Any] | None:
    """Réémission d'un PDF déjà connu (même fournisseur+référence) avec un CONTENU différent
    (montant TTC différent) : V1 est ANNULÉE (jamais supprimée, reste tracée via facture_evenements
    et facture_pdf_diagnostics), V2 devient la seule facture active et comptabilisable (§E3/test 14).
    Un même contenu (montant identique) reste un doublon CERTAIN, refusé comme avant."""
    active = _facture_active(fac.prestataire_id, fac.numero_facture, db_path=db_path)
    if active is None:
        return None
    ancien_montant = active.get("montant_ttc")
    nouveau_montant = fac.montant_total_facture
    if ancien_montant is not None and nouveau_montant is not None and round(float(ancien_montant), 2) == round(float(nouveau_montant), 2):
        return None  # contenu identique : vrai doublon, pas une nouvelle version
    # Une nouvelle VERSION porte sur la même période. Le même numéro sur un AUTRE mois est un
    # numéro réutilisé par le fournisseur (cas réel : 2026-37 au 30 avril, 15 €, et au 31 mai,
    # 1 439 €) : deux factures distinctes. Annuler l'une pour l'autre ferait disparaître une
    # facture valide ; on refuse l'import, on le dit, et l'humain décide.
    mois_actif = str(active.get("date_facture") or "")[:7]
    mois_nouveau = str(getattr(fac, "date_facture", None) or "")[:7]
    if mois_actif and mois_nouveau and mois_actif != mois_nouveau:
        return {"ok": False, "code": E_NUMERO_FACTURE_REUTILISE,
                "message": (f"Le numéro {fac.numero_facture} est déjà porté par la facture du "
                            f"{active.get('date_facture')} ({ancien_montant} €) : ce document du "
                            f"{fac.date_facture} ({nouveau_montant} €) est une autre facture. "
                            f"Rien n'a été annulé ; à traiter manuellement."),
                "facture_existante": active["facture_id_opaque"]}
    annulation = fact.changer_statut(
        active["facture_id_opaque"], fact.ST_ANNULEE, acteur=acteur,
        commentaire=f"Remplacée automatiquement par une nouvelle version du PDF "
                   f"({fac.nom_fichier_source}) — montant {ancien_montant} -> {nouveau_montant}",
        db_path=db_path)
    if not annulation.get("ok"):
        return None
    resultat = fact.creer(form, acteur=acteur, db_path=db_path,
                          fournisseur_actif=_fournisseur_actif(fac.prestataire_id, db_path=db_path))
    if resultat.get("ok"):
        resultat["remplacement_de"] = active["facture_id_opaque"]
    return resultat


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
            "anomalies, mode_extraction, facture_id_opaque) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (fac.nom_fichier_source, fac.format_detecte, fac.statut_extraction, fac.numero_facture,
             fac.montant_total_facture, fac.somme_lignes, fac.ecart_reconciliation,
             len(fac.lignes), ",".join(fac.anomalies) or None, "PDF_AUTOMATIQUE",
             facture_id_opaque))
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
        "facture_ref": fac.numero_facture,
        "date_facture": fac.date_facture,
        "montant_ttc": fac.montant_total_facture,
        "devise": fac.devise,
        "source": "PDF_EXTRACTION",
        "commentaire": fac.nom_fichier_source,
    }
    actif = _fournisseur_actif(fac.prestataire_id, db_path=db_path) if fac.prestataire_id else None
    resultat = fact.creer(form, acteur=acteur, db_path=db_path, fournisseur_actif=actif)
    if not resultat.get("ok"):
        codes = [resultat.get("code")] + [e.get("code") for e in (resultat.get("erreurs") or [])]
        if fact.E_DOUBLON_CERTAIN in codes and fac.prestataire_id and fac.numero_facture:
            remplacement = _tenter_remplacement_v1_v2(fac, form, acteur=acteur, db_path=db_path)
            if remplacement is not None:
                resultat = remplacement
        if not resultat.get("ok"):
            _enregistrer_diagnostic(fac, facture_id_opaque=None, db_path=db_path)
            if resultat.get("code") == E_NUMERO_FACTURE_REUTILISE:
                return resultat
            # Un PDF déjà importé n'est pas une « saisie invalide » : c'est le cas nominal d'un
            # dossier rescanné. Le dire par son code propre évite de renvoyer l'utilisateur vers un
            # formulaire à corriger pour un fichier qui n'a rien d'anormal.
            if fact.E_DOUBLON_CERTAIN in codes:
                return {**resultat, "code": fact.E_DOUBLON_CERTAIN,
                        "message": "Cette facture est déjà enregistrée."}
            return resultat

    facture_id = resultat["facture_id_opaque"]
    _enregistrer_diagnostic(fac, facture_id_opaque=facture_id, db_path=db_path)

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

    controle = flm.controler_total(facture_id, db_path=db_path)

    return {"ok": True, "facture_id_opaque": facture_id, "statut": resultat["statut"],
            "nb_lignes": len(fac.lignes), "controle_total": controle, "ventilations": ventilations,
            "anomalies_extraction": fac.anomalies,
            # Mois impacté = EXACTEMENT ce que lot6d lira (`str(date_facture or "")[:7]`, cf. son
            # bloc `ext`) — mission "recalcul mensuel ciblé" §8 : jamais un mois recalculé au hasard.
            "mois_impacte": str(fac.date_facture or "")[:7] or None}

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

VENTILATION
Chaque ligne EXTRAITE SANS logement (`ligne.logement_id is None`) devient une ligne
FRAIS_NON_AFFECTE, conservée telle quelle (§32 : la ligne originale reste visible) ; sa répartition
sur les autres logements de la facture est déléguée à `facture_ventilation_menage_service`.
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

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

E_EXTRACTION_ECHOUEE = "EXTRACTION_ECHOUEE"


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
        _enregistrer_diagnostic(fac, facture_id_opaque=None, db_path=db_path)
        return resultat

    facture_id = resultat["facture_id_opaque"]
    _enregistrer_diagnostic(fac, facture_id_opaque=facture_id, db_path=db_path)
    for ligne in fac.lignes:
        if ligne.logement_id:
            flm.ajouter_ligne(
                facture_id, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id=ligne.logement_id,
                montant_ttc=ligne.montant_ligne or 0, description=ligne.logement_source,
                quantite=ligne.quantite, prix_unitaire=ligne.prix_unitaire,
                date_menage=ligne.date_menage or "", precision_date_menage=ligne.precision_date,
                nom_prestataire=fac.nom_prestataire or "",
                source=flm.SOURCE_PDF, acteur=acteur, db_path=db_path)
        else:
            flm.ajouter_ligne(
                facture_id, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE,
                montant_ttc=ligne.montant_ligne or 0, description=ligne.logement_source,
                quantite=ligne.quantite, prix_unitaire=ligne.prix_unitaire,
                date_menage=ligne.date_menage or "", precision_date_menage=ligne.precision_date,
                nom_prestataire=fac.nom_prestataire or "",
                source=flm.SOURCE_PDF, acteur=acteur, db_path=db_path)

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
            "anomalies_extraction": fac.anomalies}

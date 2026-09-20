"""Quelle lecture d'un document fait foi : le parseur PDF, ou le MD structuré posé à côté.

UNE SEULE INTERPRÉTATION ACTIVE À LA FOIS
Une facture a des lignes canoniques, et elles viennent d'UNE source : `PDF` ou `MD`, jamais un
mélange des deux. Changer de source ne corrige pas les lignes en place : cela crée une NOUVELLE
VERSION d'interprétation — les lignes précédentes sont désactivées (conservées, lisibles, mais
elles ne comptent plus nulle part) et les nouvelles sont posées. Tout ce qui lit une facture —
l'écran, le contrôle de validation, le module Ménages — ne voit donc jamais deux lectures
superposées.

CE QUI DÉCLENCHE UN CHANGEMENT DE SOURCE
Le rechargement du dossier, et lui seul : un MD apparaît → il prend la main ; il disparaît, ou
devient invalide/obsolète → le parseur PDF reprend. Jamais en silence : chaque bascule laisse une
version tracée, avec son motif.

CE QUI N'EN DÉCLENCHE JAMAIS
Une facture VALIDÉE est figée. Ajouter, modifier ou retirer un MD à côté d'elle ne touche ni ses
lignes, ni sa dette, ni sa comptabilité, ni les ménages : elle se corrige par le workflow de
contrepassation, comme toute pièce engagée.

LE MODULE MÉNAGES NE LIT JAMAIS UN MD. Il lit les lignes canoniques de la facture. C'est pour cela
qu'un changement de source le fait changer de résultat — sans seconde interprétation nulle part.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.db.connection import get_db
from app.services import facture_lignes_menage_service as flm
from app.services import facture_md_service as md_svc
from app.services import factures_service as fact

SOURCE_PDF = "PDF"
SOURCE_MD = "MD"

#: Lignes écartées parce qu'une autre lecture du même document a pris la main. Conservées pour
#: l'audit : on doit pouvoir montrer ce que la lecture précédente disait.
STATUT_LIGNE_REMPLACEE = "REMPLACEE_PAR_INTERPRETATION"
#: Source portée par une ligne issue du MD — distincte de `PDF_EXTRACTION`, jamais confondue.
SOURCE_LIGNE_MD = "MD_STRUCTURE"

E_FACTURE_FIGEE = "FACTURE_FIGEE"
E_INTROUVABLE = "FACTURE_INTROUVABLE"

STATUTS_MODIFIABLES = (fact.ST_BROUILLON, fact.ST_A_CONTROLER)


def _txt(v: Any) -> str:
    return str(v or "").strip()


def etat(facture_id_opaque: str, *, db_path=None) -> dict[str, Any]:
    """Ce qui fait foi aujourd'hui pour cette facture, et l'état du MD qui l'accompagne."""
    conn = get_db(db_path)
    try:
        f = conn.execute(
            "SELECT source_interpretation, md_nom_fichier, md_sha256, md_referentiel_version, "
            "md_etat, statut, source FROM factures WHERE facture_id_opaque = ?",
            (facture_id_opaque,)).fetchone()
        if f is None:
            return {}
        # Une facture saisie à la main n'a ni PDF ni MD : elle n'a donc aucune interprétation de
        # document à afficher. Lui prêter « Extraction PDF » par défaut serait une affirmation
        # fausse — l'écran n'en dit rien plutôt que d'inventer une provenance.
        if not f["source_interpretation"] and str(f["source"] or "") != "PDF_EXTRACTION":
            return {}
        versions = [dict(r) for r in conn.execute(
            "SELECT version, source, md_nom_fichier, etat, nb_lignes, montant_lignes, active, "
            "motif, date_creation FROM facture_interpretations WHERE facture_id_opaque = ? "
            "ORDER BY version", (facture_id_opaque,))]
    finally:
        conn.close()
    return {
        "source_interpretation": f["source_interpretation"] or SOURCE_PDF,
        "md_nom_fichier": f["md_nom_fichier"], "md_sha256": f["md_sha256"],
        "md_referentiel_version": f["md_referentiel_version"],
        "md_etat": f["md_etat"] or md_svc.ETAT_ABSENT,
        "figee": f["statut"] not in STATUTS_MODIFIABLES,
        "versions": versions,
    }


def _prochaine_version(conn, facture_id_opaque: str) -> int:
    r = conn.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM facture_interpretations "
                     "WHERE facture_id_opaque = ?", (facture_id_opaque,)).fetchone()
    return int(r[0])


def _desactiver_lignes(conn, facture_id_opaque: str, motif: str) -> int:
    cur = conn.execute(
        "UPDATE facture_lignes_menage SET statut_ligne = ?, motif_correction = ? "
        "WHERE facture_id_opaque = ? AND COALESCE(statut_ligne,'ACTIVE') = 'ACTIVE'",
        (STATUT_LIGNE_REMPLACEE, motif, facture_id_opaque))
    return cur.rowcount


def _poser_lignes_md(facture_id_opaque: str, lignes: list[dict[str, Any]], *, acteur: str,
                     nom_prestataire: str = "", db_path=None) -> list[dict[str, Any]]:
    """Écrit les lignes du MD par le chemin canonique (`ajouter_ligne`), jamais par un INSERT."""
    from app.services import referentiel_logements_export_service as ref_export

    connus = ref_export.logements_connus(db_path=db_path)
    ecrites = []
    for ligne in lignes:
        logements = [l for l in ligne["logements"] if l["logement_id"] in connus]
        premier = logements[0] if logements else None
        res = flm.ajouter_ligne(
            facture_id_opaque,
            type_ligne=flm.TYPE_MENAGE_EXTERNE if premier else flm.TYPE_FRAIS_NON_AFFECTE,
            logement_id=premier["logement_id"] if premier else "",
            montant_ttc=ligne["montant_ttc"], description=ligne["description"],
            quantite=ligne["quantite"], prix_unitaire=ligne["prix_unitaire"],
            date_menage=ligne["date_menage"] or "",
            precision_date_menage=ligne["precision_date"],
            nom_prestataire=nom_prestataire,
            source=SOURCE_LIGNE_MD, acteur=acteur, db_path=db_path,
            logement_confiance=premier["confiance"] if premier else "AUCUN",
            logement_methode="MD_STRUCTURE" if premier else "",
            commentaire=" | ".join(ligne["anomalies"]) or "",
            libelle_source=ligne["libelle_source"],
            categorie=ligne["categorie"], categorie_confiance=ligne["categorie_confiance"])
        if not res.get("ok"):
            ecrites.append({"ok": False, "code": res.get("code"),
                            "libelle": ligne["libelle_source"][:60]})
            continue
        ecrites.append({"ok": True, "ligne_id_opaque": res["ligne_id_opaque"],
                        "logements": logements})
        # Plusieurs logements sur une même ligne : on réutilise la répartition canonique, qui
        # neutralise la ligne mère et crée ses parts — aucune règle de ventilation réécrite ici.
        if len(logements) > 1:
            total = sum(float(l.get("quote_part") or 0) for l in logements)
            parts = [{"logement_id": l["logement_id"],
                      "montant": round(ligne["montant_ttc"] * (float(l.get("quote_part") or 0) / total), 2)
                      if total > 0 else round(ligne["montant_ttc"] / len(logements), 2)}
                     for l in logements]
            flm.repartir_ligne(res["ligne_id_opaque"], parts,
                               motif="Répartition déclarée par le MD structuré",
                               acteur=acteur, db_path=db_path)
    return ecrites


def _enregistrer(facture_id_opaque: str, *, version: int, source: str, nb_lignes: int,
                 md: dict[str, Any] | None, pdf_sha256: str, motif: str, acteur: str,
                 db_path=None) -> float:
    """Grave la version d'interprétation et ce qui fait foi sur la facture. Rend la somme des lignes."""
    somme = flm.somme_lignes_effectives(facture_id_opaque, db_path=db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE facture_lignes_menage SET interpretation_version = ? "
            "WHERE facture_id_opaque = ? AND interpretation_version IS NULL",
            (version, facture_id_opaque))
        conn.execute(
            "INSERT INTO facture_interpretations (facture_id_opaque, version, source, "
            "md_nom_fichier, md_sha256, pdf_sha256, referentiel_version, etat, nb_lignes, "
            "montant_lignes, active, motif, anomalies, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?)",
            (facture_id_opaque, version, source,
             (md or {}).get("md_nom_fichier") if source == SOURCE_MD else None,
             (md or {}).get("md_sha256") if source == SOURCE_MD else None, pdf_sha256,
             (md or {}).get("referentiel_version") if source == SOURCE_MD else None,
             (md or {}).get("etat") or md_svc.ETAT_ABSENT, nb_lignes, somme, motif,
             json.dumps((md or {}).get("anomalies") or [], ensure_ascii=False), acteur or "local"))
        conn.execute(
            "UPDATE factures SET source_interpretation = ?, md_nom_fichier = ?, md_sha256 = ?, "
            "md_referentiel_version = ?, md_etat = ? WHERE facture_id_opaque = ?",
            (source, (md or {}).get("md_nom_fichier") if source == SOURCE_MD else None,
             (md or {}).get("md_sha256") if source == SOURCE_MD else None,
             (md or {}).get("referentiel_version") if source == SOURCE_MD else None,
             (md or {}).get("etat") or md_svc.ETAT_ABSENT, facture_id_opaque))
        conn.commit()
    finally:
        conn.close()
    return somme


def enregistrer_version_initiale(facture_id_opaque: str, *, source: str, nb_lignes: int,
                                 md: dict[str, Any] | None = None, pdf_sha256: str = "",
                                 motif: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Première interprétation d'une facture qui vient d'être créée : rien à désactiver."""
    conn = get_db(db_path)
    try:
        version = _prochaine_version(conn, facture_id_opaque)
    finally:
        conn.close()
    somme = _enregistrer(facture_id_opaque, version=version, source=source, nb_lignes=nb_lignes,
                         md=md, pdf_sha256=pdf_sha256, motif=motif, acteur=acteur, db_path=db_path)
    return {"ok": True, "version": version, "source": source, "montant_lignes": somme}


def appliquer(facture_id_opaque: str, *, source: str, lignes: list[dict[str, Any]],
              poser=None, md: dict[str, Any] | None = None, pdf_sha256: str = "",
              motif: str = "", acteur: str = "", nom_prestataire: str = "",
              db_path=None) -> dict[str, Any]:
    """Pose une NOUVELLE VERSION d'interprétation : anciennes lignes désactivées, nouvelles posées.

    `poser` permet à l'appelant d'écrire lui-même les lignes (retour au parseur PDF, dont le
    moteur vit hors de l'application) ; sans lui, les lignes du MD sont posées ici.

    Refuse sur une facture qui n'est plus modifiable — une pièce validée ne change pas de lecture
    sans passer par la contrepassation.
    """
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT statut, date_facture FROM factures WHERE facture_id_opaque = ?",
                         (facture_id_opaque,)).fetchone()
        if f is None:
            return {"ok": False, "code": E_INTROUVABLE}
        if f["statut"] not in STATUTS_MODIFIABLES:
            return {"ok": False, "code": E_FACTURE_FIGEE,
                    "message": (f"Facture {f['statut']} : sa lecture est figée. La corriger passe "
                                "par la contrepassation, jamais par un fichier déposé à côté.")}
        mois = _txt(f["date_facture"])[:7]
        version = _prochaine_version(conn, facture_id_opaque)
        _desactiver_lignes(conn, facture_id_opaque,
                           motif or f"Remplacée par l'interprétation {source}")
        conn.execute("UPDATE facture_interpretations SET active = 0 WHERE facture_id_opaque = ?",
                     (facture_id_opaque,))
        conn.commit()
    finally:
        conn.close()

    if poser is not None:
        poser()
        refus = []
    else:
        ecrites = _poser_lignes_md(facture_id_opaque, lignes, acteur=acteur,
                                   nom_prestataire=nom_prestataire, db_path=db_path)
        refus = [e for e in ecrites if e.get("ok") is False]

    somme = _enregistrer(facture_id_opaque, version=version, source=source, nb_lignes=len(lignes),
                         md=md, pdf_sha256=pdf_sha256, motif=motif, acteur=acteur, db_path=db_path)

    # Les lignes canoniques ont changé : le rapprochement Ménages doit suivre, tout de suite.
    recalcul = fact._recalculer_menages(mois, "INTERPRETATION_CHANGEE", db_path=db_path)
    return {"ok": not refus, "version": version, "source": source, "nb_lignes": len(lignes),
            "montant_lignes": somme, "refus": refus, "recalcul_menages": recalcul,
            "anomalies": (md or {}).get("anomalies") or []}


def synchroniser(facture_id_opaque: str, pdf_path, *, acteur: str = "", db_path=None,
                 lignes_pdf: list[dict[str, Any]] | None = None,
                 extraire_pdf=None) -> dict[str, Any]:
    """Aligne l'interprétation active d'une facture EXISTANTE sur ce que dit le dossier.

    Rend `{"change": False}` quand rien ne bouge : c'est le cas nominal d'un rechargement, et il
    ne doit produire ni version, ni ligne, ni recalcul.
    """
    courant = etat(facture_id_opaque, db_path=db_path)
    if not courant:
        return {"ok": False, "code": E_INTROUVABLE, "change": False}
    analyse = md_svc.analyser(pdf_path, db_path=db_path)

    if courant["figee"]:
        # Facture validée : on NOTE l'état du MD (pour l'écran), on ne change rien d'autre.
        conn = get_db(db_path)
        try:
            conn.execute("UPDATE factures SET md_etat = ? WHERE facture_id_opaque = ?",
                         (analyse["etat"], facture_id_opaque))
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "change": False, "figee": True, "md_etat": analyse["etat"]}

    cible = SOURCE_MD if analyse["etat"] == md_svc.ETAT_VALIDE else SOURCE_PDF
    inchange = (cible == courant["source_interpretation"]
                and (cible == SOURCE_PDF or analyse["md_sha256"] == courant["md_sha256"]))
    if inchange:
        conn = get_db(db_path)
        try:
            conn.execute("UPDATE factures SET md_etat = ? WHERE facture_id_opaque = ?",
                         (analyse["etat"], facture_id_opaque))
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "change": False, "source": cible, "md_etat": analyse["etat"]}

    if cible == SOURCE_MD:
        from app.services import referentiel_logements_export_service as ref_export
        lignes = md_svc.lignes_canoniques(analyse["donnees"],
                                          logements_connus=ref_export.logements_connus(db_path=db_path))
        motif = f"Interprétation structurée {analyse['md_nom_fichier']} déposée à côté du PDF"
        return {**appliquer(facture_id_opaque, source=SOURCE_MD, lignes=lignes, md=analyse,
                            motif=motif, acteur=acteur, db_path=db_path), "change": True,
                "md_etat": analyse["etat"]}

    # Retour au parseur PDF : c'est l'appelant qui sait extraire (le moteur vit dans 02_TRAVAIL).
    if extraire_pdf is None:
        return {"ok": False, "code": "EXTRACTION_INDISPONIBLE", "change": False}
    motif = {
        md_svc.ETAT_ABSENT: "MD retiré du dossier : retour à l'extraction PDF",
        md_svc.ETAT_INVALIDE: "MD non conforme au contrat : retour à l'extraction PDF",
        md_svc.ETAT_OBSOLETE: "MD écrit pour une autre version du PDF : retour à l'extraction PDF",
    }.get(analyse["etat"], "Retour à l'extraction PDF")
    resultat = extraire_pdf(facture_id_opaque, motif=motif)
    return {**resultat, "change": True, "source": SOURCE_PDF, "md_etat": analyse["etat"],
            "md_erreurs": analyse["erreurs"]}

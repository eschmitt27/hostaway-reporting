"""Export du référentiel des logements — le contexte à donner à l'outil qui rédige les MD.

À QUOI CELA SERT
La transformation PDF → MD se fait À L'EXTÉRIEUR de l'application (aucune IA n'est branchée ici).
Pour qu'un MD puisse nommer un logement par son identifiant canonique (`LOG_0007`) plutôt que par
une phrase, celui qui le rédige doit disposer du référentiel. Ce service l'exporte, et rien de
plus : ni téléphone, ni e-mail, ni donnée sans rapport avec la reconnaissance d'un logement.

LA VERSION EST UNE EMPREINTE, PAS UN COMPTEUR
`referentiel_version` est le SHA-256 du CONTENU canonique (mêmes données → même version, à la
seconde près comme au mois suivant). C'est ce qui permet à l'application de dire, en relisant un
MD : « ce MD a été écrit avec une autre version du référentiel ». Aucune date n'entre dans le
calcul — sinon deux exports identiques produiraient deux versions différentes, et la comparaison
ne voudrait plus rien dire.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.db.connection import get_db

SCHEMA = "REFERENTIEL_LOGEMENTS_V1"
NOM_FICHIER = "referentiel_logements.json"

#: Statut de parc des bacs techniques : exportés, mais signalés — un MD ne doit pas y ranger une
#: prestation faute de mieux.
STATUT_HORS_PARC = "HORS_PARC_TECHNIQUE"


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _logements(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            "SELECT logement_id, nom_logement_officiel, nom_court, adresse, ville, actif, "
            "statut_parc FROM ref_logements ORDER BY logement_id")]
        alias = [dict(r) for r in conn.execute(
            "SELECT valeur_source, logement_id, champ_source, niveau_confiance "
            "FROM ref_mapping_logements WHERE UPPER(COALESCE(actif,'OUI')) = 'OUI' "
            "ORDER BY logement_id, valeur_source")]
        try:
            gestions = [dict(r) for r in conn.execute(
                "SELECT logement_id, proprietaire_id, date_debut, date_fin, statut_gestion "
                "FROM ref_gestion_logements_hist ORDER BY logement_id, date_debut")]
            proprietaires = {r["proprietaire_id"]: _txt(r["nom_proprietaire"]) for r in conn.execute(
                "SELECT proprietaire_id, nom_proprietaire FROM ref_proprietaires")}
        except Exception:      # noqa: BLE001 — base partielle : on exporte ce qui existe
            gestions, proprietaires = [], {}
    finally:
        conn.close()

    par_logement: dict[str, dict[str, Any]] = {}
    for l in lignes:
        lid = _txt(l.get("logement_id"))
        if not lid:
            continue
        par_logement[lid] = {
            "logement_id": lid,
            "nom": _txt(l.get("nom_logement_officiel")) or _txt(l.get("nom_court")),
            "nom_court": _txt(l.get("nom_court")),
            "adresse": _txt(l.get("adresse")),
            "ville": _txt(l.get("ville")),
            "actif": _txt(l.get("actif")).upper() == "OUI",
            "statut_parc": _txt(l.get("statut_parc")),
            "hors_parc_technique": _txt(l.get("statut_parc")).upper() == STATUT_HORS_PARC,
            "aliases": [],
            "gestion": [],
        }
    for a in alias:
        cible = par_logement.get(_txt(a.get("logement_id")))
        valeur = _txt(a.get("valeur_source"))
        # Les identifiants techniques d'une plateforme (listingMapId) ne servent pas à reconnaître
        # un logement sur une facture papier : ils alourdiraient l'export sans rien y apporter.
        if cible is None or not valeur or _txt(a.get("champ_source")) == "listingMapId":
            continue
        cible["aliases"].append(valeur)
    for g in gestions:
        cible = par_logement.get(_txt(g.get("logement_id")))
        if cible is None:
            continue
        cible["gestion"].append({
            "proprietaire": proprietaires.get(_txt(g.get("proprietaire_id")), ""),
            "date_debut": _txt(g.get("date_debut")),
            "date_fin": _txt(g.get("date_fin")),
            "statut": _txt(g.get("statut_gestion")),
        })
    for cible in par_logement.values():
        cible["aliases"] = sorted(set(cible["aliases"]))
    return [par_logement[k] for k in sorted(par_logement)]


def _empreinte(logements: list[dict[str, Any]]) -> str:
    """SHA-256 du contenu canonique : mêmes données, même version — toujours."""
    canonique = json.dumps({"schema": SCHEMA, "logements": logements},
                           ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonique.encode("utf-8")).hexdigest()


def construire(db_path=None) -> dict[str, Any]:
    """Le document exporté : schéma, version (empreinte), logements."""
    logements = _logements(db_path=db_path)
    return {
        "schema": SCHEMA,
        "referentiel_version": _empreinte(logements),
        "nb_logements": len(logements),
        "logements": logements,
    }


def version(db_path=None) -> str:
    """Version du référentiel TEL QU'IL EST en base, sans produire le fichier."""
    return _empreinte(_logements(db_path=db_path))


def exporter_json(db_path=None) -> str:
    return json.dumps(construire(db_path=db_path), ensure_ascii=False, indent=2) + "\n"


def logements_connus(db_path=None) -> set[str]:
    """Identifiants que l'application accepte dans un MD. Elle n'en crée jamais d'autres."""
    conn = get_db(db_path)
    try:
        return {_txt(r["logement_id"]) for r in conn.execute(
            "SELECT logement_id FROM ref_logements") if _txt(r["logement_id"])}
    finally:
        conn.close()

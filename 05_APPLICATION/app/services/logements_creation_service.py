"""Création d'un logement au référentiel — écriture SQLite, validée et journalisée.

Manque historique : le parc était en lecture seule (aucune route, aucune fonction de création dans
toutes les branches). Ce service ajoute la création.

MIGRATION EXCEL → SQLITE
Ce service écrivait dans `REF_Setup.xlsm` (`keep_vba=True`, remplacement atomique du fichier). Il
écrit désormais dans les tables `ref_logements` / `ref_gestion_logements_hist` (0029) via
`referentiel_admin_service`. Aucune règle métier n'a changé : mêmes validations, mêmes codes
d'erreur, mêmes deux écritures cohérentes. Seule la destination change — et avec elle le fait que
le référentiel SQLite devient canonique plutôt qu'une copie du classeur.

Deux tables sont alimentées de façon cohérente :
- `ref_logements` : le bien lui-même ;
- `ref_gestion_logements_hist` : le rattachement au propriétaire, **daté** (c'est cette table que
  lisent le moteur de charges et Lot10 pour résoudre le propriétaire d'un logement à une période).

Aucune règle métier n'est recalculée : le service valide, puis écrit.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.services import referentiel_admin_service as adm

SH_LOG = adm.TABLE_LOGEMENTS
SH_GEST = adm.TABLE_GESTION
SH_PROP = adm.TABLE_PROPRIETAIRES

E_REFERENTIEL_ABSENT = adm.E_REFERENTIEL_ABSENT
E_ID_MANQUANT = "V01_ID_MANQUANT"
E_ID_EXISTANT = "V02_ID_DEJA_UTILISE"
E_NOM_MANQUANT = "V03_NOM_MANQUANT"
E_PROP_MANQUANT = "V04_PROPRIETAIRE_MANQUANT"
E_PROP_INCONNU = "V05_PROPRIETAIRE_INCONNU"
E_DATE_INVALIDE = "V06_DATE_DEBUT_INVALIDE"
E_TYPE_INCONNU = "V07_TYPE_LOGEMENT_INCONNU"
E_ECRITURE = adm.E_ECRITURE

MESSAGES = {
    E_REFERENTIEL_ABSENT: adm.MESSAGES[adm.E_REFERENTIEL_ABSENT],
    E_ID_MANQUANT: "L'identifiant du logement est obligatoire.",
    E_ID_EXISTANT: "Cet identifiant de logement existe déjà.",
    E_NOM_MANQUANT: "Le nom du logement est obligatoire.",
    E_PROP_MANQUANT: "Le propriétaire est obligatoire.",
    E_PROP_INCONNU: "Ce propriétaire n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "La date de début de gestion est invalide (format AAAA-MM-JJ attendu).",
    E_TYPE_INCONNU: "Ce type de logement n'existe pas dans le référentiel.",
    E_ECRITURE: "Écriture refusée.",
}


def _txt(v) -> str:
    return adm.txt(v)


def referentiels(*, db_path=None) -> dict[str, Any]:
    """Options nécessaires au formulaire (propriétaires actifs, types, logements existants).

    `SOURCE_ABSENTE` est conservé comme statut : l'écran sait déjà l'afficher. Il signifie
    désormais « référentiel SQLite non initialisé », jamais « classeur introuvable ».
    """
    if not adm.disponible(db_path=db_path):
        return {"status": "SOURCE_ABSENTE", "proprietaires": [], "types": [], "logements": []}

    props = adm.lignes(SH_PROP, db_path=db_path)
    types = adm.lignes(adm.TABLE_TYPES, db_path=db_path)
    logs = adm.lignes(SH_LOG, db_path=db_path)
    return {
        "status": "OK",
        "proprietaires": [{"proprietaire_id": _txt(r.get("proprietaire_id")),
                           "nom": _txt(r.get("nom_proprietaire"))}
                          for r in props if _txt(r.get("proprietaire_id"))
                          and _txt(r.get("actif")).upper() == "OUI"],
        "types": [{"type_logement_id": _txt(r.get("type_logement_id")),
                   "libelle": _txt(r.get("type_logement") or r.get("libelle"))}
                  for r in types if _txt(r.get("type_logement_id"))],
        "logements": [_txt(r.get("logement_id")) for r in logs if _txt(r.get("logement_id"))],
    }


def valider(form: dict[str, Any], *, db_path=None) -> list[dict[str, str]]:
    """Contrôles métier AVANT écriture. Retourne la liste des erreurs (vide = valide)."""
    erreurs: list[dict[str, str]] = []

    def err(code: str, detail: str = ""):
        erreurs.append({"code": code, "message": MESSAGES.get(code, code), "detail": detail})

    refs = referentiels(db_path=db_path)
    logement_id = _txt(form.get("logement_id"))
    if not logement_id:
        err(E_ID_MANQUANT)
    elif logement_id in refs["logements"]:
        err(E_ID_EXISTANT, logement_id)

    if not _txt(form.get("nom_logement_officiel")) and not _txt(form.get("nom_court")):
        err(E_NOM_MANQUANT)

    prop = _txt(form.get("proprietaire_id"))
    connus = {p["proprietaire_id"] for p in refs["proprietaires"]}
    if not prop:
        err(E_PROP_MANQUANT)
    elif connus and prop not in connus:
        err(E_PROP_INCONNU, prop)

    d = _txt(form.get("date_debut"))
    if not d:
        err(E_DATE_INVALIDE, "date manquante")
    else:
        try:
            date.fromisoformat(d)
        except ValueError:
            err(E_DATE_INVALIDE, d)

    t = _txt(form.get("type_logement_id"))
    types_connus = {x["type_logement_id"] for x in refs["types"]}
    if t and types_connus and t not in types_connus:
        err(E_TYPE_INCONNU, t)

    return erreurs


def creer(form: dict[str, Any], *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée le logement + son rattachement propriétaire daté. Jamais de modification d'une ligne
    existante : uniquement des ajouts."""
    if not adm.disponible(db_path=db_path):
        return {"ok": False, "code": E_REFERENTIEL_ABSENT,
                "message": MESSAGES[E_REFERENTIEL_ABSENT], "erreurs": []}

    erreurs = valider(form, db_path=db_path)
    if erreurs:
        return {"ok": False, "code": "V_INVALIDE", "message": "Saisie invalide.", "erreurs": erreurs}

    logement_id = _txt(form.get("logement_id"))
    prop = _txt(form.get("proprietaire_id"))
    actif = "OUI" if _txt(form.get("actif")).upper() != "NON" else "NON"
    date_debut = _txt(form.get("date_debut"))

    res = adm.inserer(SH_LOG, {
        "logement_id": logement_id,
        "hostaway_listing_id": _txt(form.get("hostaway_listing_id")),
        "nom_logement_officiel": (_txt(form.get("nom_logement_officiel"))
                                  or _txt(form.get("nom_court"))),
        "nom_court": _txt(form.get("nom_court")) or _txt(form.get("nom_logement_officiel")),
        "adresse": _txt(form.get("adresse")),
        "ville": _txt(form.get("ville")),
        "type_logement_id": _txt(form.get("type_logement_id")),
        "sur_hostaway": _txt(form.get("sur_hostaway")) or "NON",
        "actif": actif,
        "statut_parc": _txt(form.get("statut_parc")) or ("GERE" if actif == "OUI" else "RETIRE"),
        "commentaire": _txt(form.get("commentaire")),
        "forfait_logiciel_consommables_mensuel": form.get(
            "forfait_logiciel_consommables_mensuel") or 0,
    }, action="CREATION", acteur=acteur, db_path=db_path)
    if not res.get("ok"):
        return {**res, "erreurs": []}

    # Rattachement propriétaire DATÉ (source de résolution du propriétaire par période)
    res_gest = adm.inserer(SH_GEST, {
        "gestion_id": f"GST_{logement_id}_{prop}",
        "logement_id": logement_id,
        "proprietaire_id": prop,
        "date_debut": date_debut,
        "date_fin": _txt(form.get("date_fin")),
        "statut_gestion": "ACTIF" if actif == "OUI" else "RETIRE",
        "source": adm.SOURCE_APPLICATION,
        "commentaire": _txt(form.get("commentaire")),
    }, action="CREATION", acteur=acteur, db_path=db_path)
    if not res_gest.get("ok"):
        return {**res_gest, "erreurs": []}

    return {"ok": True, "logement_id": logement_id, "proprietaire_id": prop,
            "date_debut": date_debut, "erreurs": []}

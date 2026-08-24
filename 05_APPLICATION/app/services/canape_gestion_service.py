"""Cycle de vie des paramètres canapé (seuil de voyageurs / montant) par logement — Mission 6.

`ref_canape_parametres` est un référentiel HISTORISÉ (grain `logement_id`, `date_debut`/
`date_fin`, migration 0058) : jusqu'à cette mission, seuil et montant vivaient comme deux colonnes
COURANTES sur `ref_logements`, sans période — un recalcul futur d'une réservation passée aurait
utilisé le paramètre ACTUEL, jamais celui en vigueur à la date de la réservation (violation directe
du principe fondamental de la mission : « changer le futur sans réécrire le passé »).

Ce module ajoute la même discipline clôture/ouverture que `logements_gestion_service.
changer_taux_commission`/`couts_menage_gestion_service.changer_cout` : clôture la période active à
la veille de la nouvelle date d'effet, ouvre une nouvelle ligne, jamais de modification d'une ligne
close, le tout en une seule transaction (`referentiel_admin_service.transaction`).

Les colonnes `ref_logements.seuil_voyageurs_preparation_canape`/`montant_preparation_canape`
restent en place (parité classeur) mais sont désormais exclues de l'édition libre côté écran
générique (`referentiel_admin_service.COLONNES_LECTURE_SEULE`) : ce module est le seul chemin
d'écriture pour ce paramètre.
"""
from __future__ import annotations

from typing import Any

from app.services import referentiel_admin_service as adm

TABLE = "ref_canape_parametres"

E_REFERENTIEL_ABSENT = adm.E_REFERENTIEL_ABSENT
E_LOGEMENT_MANQUANT = "V01_LOGEMENT_MANQUANT"
E_LOGEMENT_INCONNU = "V02_LOGEMENT_INCONNU"
E_DATE_INVALIDE = adm.E_DATE_INVALIDE
E_SEUIL_INVALIDE = "V03_SEUIL_INVALIDE"
E_MONTANT_INVALIDE = "V04_MONTANT_INVALIDE"
E_PERIODE_INCOHERENTE = adm.E_PERIODE_INCOHERENTE
E_ECRITURE = adm.E_ECRITURE

MESSAGES = {
    E_REFERENTIEL_ABSENT: adm.MESSAGES[adm.E_REFERENTIEL_ABSENT],
    E_LOGEMENT_MANQUANT: "Le logement est obligatoire.",
    E_LOGEMENT_INCONNU: "Ce logement n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "La date est invalide (format AAAA-MM-JJ attendu).",
    E_SEUIL_INVALIDE: "Le seuil de voyageurs doit être un nombre entier positif ou nul.",
    E_MONTANT_INVALIDE: "Le montant doit être un nombre positif ou nul.",
    E_PERIODE_INCOHERENTE: adm.MESSAGES[adm.E_PERIODE_INCOHERENTE],
    E_ECRITURE: adm.MESSAGES[adm.E_ECRITURE],
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def historique(logement_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes (closes ou non) de ce logement, triées par début décroissant."""
    lid = adm.txt(logement_id)
    rows = [r for r in adm.lignes(TABLE, db_path=db_path) if adm.txt(r.get("logement_id")) == lid]
    rows.sort(key=lambda r: adm.txt(r.get("date_debut")), reverse=True)
    return rows


def changer_parametres(logement_id: str, seuil_voyageurs: Any, montant: Any, date_debut: str, *,
                       acteur: str = "", db_path=None) -> dict[str, Any]:
    """Change le seuil/montant canapé d'un logement à `date_debut` : clôture la période active à
    la veille et ouvre une nouvelle ligne. Aucune ligne close n'est jamais modifiée."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not adm.date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)
    try:
        seuil_i = int(str(seuil_voyageurs).strip())
    except (TypeError, ValueError):
        return _refus(E_SEUIL_INVALIDE, str(seuil_voyageurs))
    if seuil_i < 0:
        return _refus(E_SEUIL_INVALIDE, str(seuil_voyageurs))
    try:
        montant_f = float(str(montant).replace(",", "."))
    except (TypeError, ValueError):
        return _refus(E_MONTANT_INVALIDE, str(montant))
    if montant_f < 0:
        return _refus(E_MONTANT_INVALIDE, str(montant))

    lid = adm.txt(logement_id)
    if not lid:
        return _refus(E_LOGEMENT_MANQUANT)
    if adm.ligne("ref_logements", lid, db_path=db_path) is None:
        return _refus(E_LOGEMENT_INCONNU, lid)

    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(TABLE, lid, adm.veille(date_debut), acteur=acteur,
                                        conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)

            res = adm.inserer(TABLE, {
                "canape_parametre_id": f"CNP_{lid}_{adm.txt(date_debut)}",
                "logement_id": lid,
                "seuil_voyageurs_preparation_canape": seuil_i,
                "montant_preparation_canape": montant_f,
                "date_debut": adm.txt(date_debut),
                "date_fin": "",
                "actif": "OUI",
                "commentaire": "",
            }, action="CHANGEMENT_PARAMETRES_CANAPE", acteur=acteur, conn=conn, db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001 — panne DB imprévue : rollback déjà fait
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "logement_id": lid, "seuil_voyageurs_preparation_canape": seuil_i,
            "montant_preparation_canape": montant_f, "date_debut": adm.txt(date_debut)}

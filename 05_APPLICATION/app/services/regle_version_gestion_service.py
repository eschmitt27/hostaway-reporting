"""Cycle de vie des versions de règles ALGORITHMIQUES — Mission 6 bis.

`ref_regles_versions` (migration 0059) distingue une RÈGLE DE CALCUL (assiette de commission,
répartition des charges communes de facture) d'une simple VARIABLE (taux, montant) : elle ne
stocke jamais de formule, seulement un `rule_code` stable + une `version` stable + une période de
validité. L'implémentation de chaque version reste dans le code (`lot10_calculer_resultats.py`
pour l'assiette, `charges_impact_service.py` pour la répartition) — ce module ne fait
qu'administrer QUAND une version devient applicable, avec la même discipline clôture/ouverture que
`canape_gestion_service.py`/`couts_menage_gestion_service.py`.

Aujourd'hui seule V1 existe pour chaque règle (la formule actuelle, backfillée par la migration
0059) : ce module rend la capacité d'ajouter une V2 administrable, sans qu'aucune V2 réelle
n'existe — introduire une vraie V2 exige d'abord de développer et tester son implémentation dans
le code (§25 de la mission), seulement ENSUITE sa date d'effet devient administrable ici.
"""
from __future__ import annotations

from typing import Any

from app.services import referentiel_admin_service as adm

TABLE = "ref_regles_versions"

E_REFERENTIEL_ABSENT = adm.E_REFERENTIEL_ABSENT
E_RULE_CODE_MANQUANT = "V01_RULE_CODE_MANQUANT"
E_VERSION_MANQUANTE = "V02_VERSION_MANQUANTE"
E_DATE_INVALIDE = adm.E_DATE_INVALIDE
E_PERIODE_INCOHERENTE = adm.E_PERIODE_INCOHERENTE
E_ECRITURE = adm.E_ECRITURE

MESSAGES = {
    E_REFERENTIEL_ABSENT: adm.MESSAGES[adm.E_REFERENTIEL_ABSENT],
    E_RULE_CODE_MANQUANT: "L'identifiant de la règle (rule_code) est obligatoire.",
    E_VERSION_MANQUANTE: "La version est obligatoire.",
    E_DATE_INVALIDE: "La date est invalide (format AAAA-MM-JJ attendu).",
    E_PERIODE_INCOHERENTE: adm.MESSAGES[adm.E_PERIODE_INCOHERENTE],
    E_ECRITURE: adm.MESSAGES[adm.E_ECRITURE],
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def historique(rule_code: str, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les versions (closes ou non) de cette règle, triées par début décroissant."""
    code = adm.txt(rule_code)
    rows = [r for r in adm.lignes(TABLE, db_path=db_path) if adm.txt(r.get("rule_code")) == code]
    rows.sort(key=lambda r: adm.txt(r.get("date_debut")), reverse=True)
    return rows


def changer_version(rule_code: str, version: str, date_debut: str, *, parametres: str = "",
                    commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Introduit une nouvelle version d'une règle à `date_debut` : clôture la version active à la
    veille et ouvre la nouvelle. Aucune version close n'est jamais modifiée."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not adm.date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)

    code = adm.txt(rule_code)
    if not code:
        return _refus(E_RULE_CODE_MANQUANT)
    ver = adm.txt(version)
    if not ver:
        return _refus(E_VERSION_MANQUANTE)

    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(TABLE, code, adm.veille(date_debut), acteur=acteur,
                                        conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)

            res = adm.inserer(TABLE, {
                "regle_version_id": f"RGV_{code}_{ver}_{adm.txt(date_debut)}",
                "rule_code": code,
                "version": ver,
                "date_debut": adm.txt(date_debut),
                "date_fin": "",
                "actif": "OUI",
                "parametres": parametres,
                "commentaire": commentaire,
            }, action="CHANGEMENT_VERSION_REGLE", acteur=acteur, conn=conn, db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001 — panne DB imprévue : rollback déjà fait
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "rule_code": code, "version": ver, "date_debut": adm.txt(date_debut)}

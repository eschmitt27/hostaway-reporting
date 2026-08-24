"""Cycle de vie d'un coût standard de ménage : ouverture/clôture de période par type de logement.

`ref_couts_standards_menage` est DÉJÀ consommé de façon historisée en lecture par le moteur ménage
(`lot6f_cout_complet_menages.py::date_aware`, résolution par `type_logement_id` + date) — ce n'est
pas une règle nouvelle. Le seul manque était côté écriture : l'écran générique d'administration
pouvait modifier librement `date_debut_validite`/`date_fin_validite` d'une ligne déjà utilisée, sans
discipline de clôture/ouverture. Ce module ajoute exactement la même discipline que
`logements_gestion_service.changer_taux_commission` : clôture la période active puis ouvre une
nouvelle ligne, jamais de modification d'une ligne déjà close, le tout en une seule transaction
(`referentiel_admin_service.transaction`).
"""
from __future__ import annotations

from typing import Any

from app.services import referentiel_admin_service as adm

TABLE = "ref_couts_standards_menage"
TABLE_TYPES = "ref_types_logements"

E_REFERENTIEL_ABSENT = adm.E_REFERENTIEL_ABSENT
E_TYPE_MANQUANT = "V01_TYPE_LOGEMENT_MANQUANT"
E_TYPE_INCONNU = "V02_TYPE_LOGEMENT_INCONNU"
E_DATE_INVALIDE = adm.E_DATE_INVALIDE
E_MONTANT_INVALIDE = "V03_MONTANT_INVALIDE"
E_PERIODE_INCOHERENTE = adm.E_PERIODE_INCOHERENTE
E_ECRITURE = adm.E_ECRITURE

MESSAGES = {
    E_REFERENTIEL_ABSENT: adm.MESSAGES[adm.E_REFERENTIEL_ABSENT],
    E_TYPE_MANQUANT: "Le type de logement est obligatoire.",
    E_TYPE_INCONNU: "Ce type de logement n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "La date est invalide (format AAAA-MM-JJ attendu).",
    E_MONTANT_INVALIDE: "Le coût standard doit être un nombre positif ou nul.",
    E_PERIODE_INCOHERENTE: adm.MESSAGES[adm.E_PERIODE_INCOHERENTE],
    E_ECRITURE: adm.MESSAGES[adm.E_ECRITURE],
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def historique(type_logement_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes (closes ou non) de ce type de logement, triées par début décroissant."""
    tid = adm.txt(type_logement_id)
    rows = [r for r in adm.lignes(TABLE, db_path=db_path)
            if adm.txt(r.get("type_logement_id")) == tid]
    rows.sort(key=lambda r: adm.txt(r.get("date_debut_validite")), reverse=True)
    return rows


def changer_cout(type_logement_id: str, cout: Any, date_debut: str, *, acteur: str = "",
                 justification: str = "", db_path=None) -> dict[str, Any]:
    """Change le coût standard ménage d'un type de logement à `date_debut` : clôture la période
    active à la veille et ouvre une nouvelle ligne. Aucune ligne close n'est jamais modifiée."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not adm.date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)
    try:
        cout_f = float(str(cout).replace(",", "."))
    except (TypeError, ValueError):
        return _refus(E_MONTANT_INVALIDE, str(cout))
    if cout_f < 0:
        return _refus(E_MONTANT_INVALIDE, str(cout))

    tid = adm.txt(type_logement_id)
    if not tid:
        return _refus(E_TYPE_MANQUANT)
    types_connus = {adm.txt(t.get("type_logement_id"))
                    for t in adm.lignes(TABLE_TYPES, db_path=db_path)}
    if types_connus and tid not in types_connus:
        return _refus(E_TYPE_INCONNU, tid)

    action_ouverture = "CORRECTION_RETROACTIVE" if adm.est_retroactif(date_debut) \
        else "CHANGEMENT_COUT_MENAGE"
    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(TABLE, tid, adm.veille(date_debut), acteur=acteur,
                                        commentaire=justification, action=action_ouverture,
                                        conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)

            res = adm.inserer(TABLE, {
                "cout_standard_id": f"CSM_{tid}_{adm.txt(date_debut)}",
                "type_logement_id": tid,
                "cout_standard_menage": cout_f,
                "date_debut_validite": adm.txt(date_debut),
                "date_fin_validite": "",
                "actif": "OUI",
                "commentaire": justification,
            }, action=action_ouverture, acteur=acteur, commentaire=justification, conn=conn,
               db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001 — panne DB imprévue : rollback déjà fait
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "type_logement_id": tid, "cout_standard_menage": cout_f,
            "date_debut": adm.txt(date_debut)}

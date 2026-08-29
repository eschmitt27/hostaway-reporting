"""Régularisation des réservations Hostaway sans saisie HH — APP-5B → APP-2c.

CE QUE CE MODULE N'EST PAS : un second moteur de saisie hors Hostaway. Il réutilise
`reservations_hh_saisie_service` (table `reservations_hors_hostaway` / migration 0052) pour l'écriture,
et enchaîne, après saisie, les MÊMES étapes du DAG moteur déjà utilisées manuellement en Mission 17
(`orchestrateur_moteur.executer_reservations`, `flux_unifie_service.construire`,
`orchestrateur_moteur.executer_lot10`, `controles_lot11_service.construire`,
`lot12_prefactures_service.construire`) — aucun nouveau pipeline.

CE QU'IL RÉSOUT : le contrôle DIRECT_HORS_HOSTAWAY / VRBO_SANS_MONTANT (agrégat
RESERVATION_A_CONTROLER_SANS_COMMISSION, cf. `controles_detail_service._classer_commission`) ne pointait
vers aucun écran de correction. Ce module fournit le pont entre l'élément de contrôle (ctrl_opaque) et
la saisie HH existante : structure déjà connue = préremplie depuis l'élément lui-même (jamais depuis un
paramètre client non vérifié) ; le seul champ à saisir par l'utilisateur est le montant réellement perçu.

RÈGLE ABSOLUE (Mission 18) : `total_price`/prix affiché Hostaway n'est jamais copié dans
`montant_percu`. Ce module ne lit même pas ce champ.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import controles_actionnable_service as act
from app.services import reservations_hh_saisie_service as saisie
from app.services import saisie_hh_service as saisie_refs

CLASSIFICATIONS_ELIGIBLES = {"DIRECT_HORS_HOSTAWAY", "VRBO_SANS_MONTANT"}

_CANAL_LABEL_PAR_CLASSIFICATION = {
    "DIRECT_HORS_HOSTAWAY": "direct",
    "VRBO_SANS_MONTANT": "vrbo",
}

_SOURCE_FINANCIERE_PAR_CLASSIFICATION = {
    "DIRECT_HORS_HOSTAWAY": "DIRECT_HA_PAYANT",
    "VRBO_SANS_MONTANT": "VRBO_UNKNOWN",
}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _resoudre_canal_id(canal_label: str, *, db_path=None) -> str:
    """Résout `canal_id` depuis le libellé (ex. "direct"/"vrbo") — lecture pure du référentiel,
    jamais une valeur inventée. Chaîne vide si le référentiel ne contient pas ce libellé."""
    try:
        _, rows = saisie_refs.get_canaux(db_path=db_path)
    except Exception:
        return ""
    cible = canal_label.strip().lower()
    for row in rows:
        if _txt(row.get("canal")).lower() == cible:
            return _txt(row.get("canal_id"))
    return ""


def element_regularisable(ctrl_opaque: str, *, db_path=None) -> dict[str, Any] | None:
    """Charge l'élément de contrôle et vérifie qu'il relève bien d'une classification régularisable
    (DIRECT_HORS_HOSTAWAY / VRBO_SANS_MONTANT). Retourne None sinon — jamais une régularisation
    hors périmètre."""
    fiche = act.load_fiche(ctrl_opaque, db_path=db_path)
    if fiche is None:
        return None
    el = fiche["element"]
    if el.get("classification") not in CLASSIFICATIONS_ELIGIBLES:
        return None
    return el


def saisie_existante(reservation_id_hostaway: str, *, db_path=None) -> dict[str, Any] | None:
    """Retrouve une saisie HH déjà créée pour cette réservation Hostaway (dédoublonnage — un
    deuxième clic sur « Régulariser » retrouve la saisie existante, n'en crée jamais une seconde)."""
    rid = _txt(reservation_id_hostaway)
    if not rid:
        return None
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM reservations_hors_hostaway WHERE reservation_id_hostaway = ? "
            "ORDER BY date_saisie DESC LIMIT 1", (rid,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def preparer_formulaire(ctrl_opaque: str, *, db_path=None) -> dict[str, Any] | None:
    """Données de préremplissage : uniquement les champs STRUCTURELS certains (jamais un montant).

    Retourne None si l'élément est introuvable ou hors périmètre régularisable.
    """
    el = element_regularisable(ctrl_opaque, db_path=db_path)
    if el is None:
        return None
    d = el["donnees"]
    classification = el["classification"]
    canal_label = _CANAL_LABEL_PAR_CLASSIFICATION.get(classification, "")
    rid = _txt(d.get("reservation_id"))
    codes_impact = []
    try:
        _, impacts_rows = saisie_refs.get_codes_impact(db_path=db_path)
        codes_impact = [
            {"value": _txt(r.get("code_impact")), "label": _txt(r.get("libelle")) or _txt(r.get("code_impact"))}
            for r in impacts_rows if _txt(r.get("code_impact"))
        ]
    except Exception:
        pass
    existante = saisie_existante(rid, db_path=db_path)
    return {
        "ctrl_opaque": ctrl_opaque,
        "classification": classification,
        "classification_libelle": el.get("classification_libelle", ""),
        "reservation_id_hostaway": rid,
        "logement_id": _txt(d.get("logement")),
        "proprietaire_id": _txt(d.get("proprietaire")),
        "mois": _txt(d.get("mois")),
        "date_arrivee": _txt(d.get("date_arrivee")),
        "date_depart": _txt(d.get("date_depart")),
        "canal_id": _resoudre_canal_id(canal_label, db_path=db_path),
        "canal_label": canal_label.upper(),
        "source_financiere_defaut": _SOURCE_FINANCIERE_PAR_CLASSIFICATION.get(classification, ""),
        "codes_impact": codes_impact,
        "prix_affiche_hostaway_non_autoritaire": None,  # jamais peuplé depuis total_price (RÈGLE ABSOLUE)
        "saisie_existante": existante,
    }


def _resoudre_impact_comptable(code_impact: str, *, db_path=None) -> str | None:
    try:
        _, impacts_rows = saisie_refs.get_codes_impact(db_path=db_path)
    except Exception:
        return None
    return saisie_refs._impact_comptabilisation_map(impacts_rows).get(_txt(code_impact)) or None


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def regulariser(
    ctrl_opaque: str,
    *,
    montant_percu: str,
    menage: str = "",
    code_impact: str,
    commentaire: str = "",
    canal_id: str = "",
    source_financiere: str = "",
    acteur: str = "",
    db_path=None,
) -> dict[str, Any]:
    """Enregistre la saisie HH régularisant l'élément désigné, via le service de saisie EXISTANT.

    Idempotent par réservation Hostaway : une saisie déjà présente est MODIFIÉE, jamais dupliquée.
    """
    prep = preparer_formulaire(ctrl_opaque, db_path=db_path)
    if prep is None:
        return _refus("ELEMENT_INTROUVABLE_OU_HORS_PERIMETRE",
                      "Contrôle introuvable ou non régularisable depuis cet écran.")
    if not _txt(montant_percu):
        return _refus("MONTANT_PERCU_OBLIGATOIRE", "Montant réellement perçu obligatoire.")
    if not _txt(code_impact):
        return _refus("CODE_IMPACT_OBLIGATOIRE", "Code impact obligatoire.")

    canal_final = _txt(canal_id) or prep["canal_id"]
    source_finale = _txt(source_financiere) or prep["source_financiere_defaut"]
    donnees = {
        "mois": prep["mois"],
        "canal_id": canal_final,
        "source_financiere": source_finale,
        "proprietaire_id": prep["proprietaire_id"],
        "logement_id": prep["logement_id"],
        "reservation_id_hostaway": prep["reservation_id_hostaway"],
        "date_arrivee": prep["date_arrivee"],
        "date_depart": prep["date_depart"],
        "montant_percu": montant_percu,
        "code_impact": code_impact,
        "impact_resultat_comptable": _resoudre_impact_comptable(code_impact, db_path=db_path),
        "statut_controle": "VALIDE",
        "niveau_anomalie": "INFO",
        "code_anomalie": "",
        "commentaire": commentaire,
    }
    if _txt(menage):
        donnees["menage"] = menage

    existante = prep["saisie_existante"]
    if existante is not None:
        rid = existante["reservation_hh_id"]
        resultat = saisie.modifier(rid, donnees, acteur=acteur,
                                   motif="Régularisation depuis contrôle " + ctrl_opaque, db_path=db_path)
    else:
        resultat = saisie.creer(donnees, acteur=acteur, db_path=db_path)
    return resultat


def recap(ctrl_opaque: str, *, montant_percu: str, menage: str = "", code_impact: str = "",
         commentaire: str = "", canal_id: str = "", source_financiere: str = "",
         db_path=None) -> dict[str, Any] | None:
    """Résumé affiché avant validation humaine (aucune écriture)."""
    prep = preparer_formulaire(ctrl_opaque, db_path=db_path)
    if prep is None:
        return None
    prep["montant_percu_saisi"] = montant_percu
    prep["menage_saisi"] = menage
    prep["code_impact_saisi"] = code_impact
    prep["commentaire_saisi"] = commentaire
    prep["canal_id_saisi"] = _txt(canal_id) or prep["canal_id"]
    prep["source_financiere_saisie"] = _txt(source_financiere) or prep["source_financiere_defaut"]
    return prep


# ── Recalcul après régularisation — enchaînement du DAG EXISTANT ────────────────────────────────
# Mêmes appels que Mission 17 (préflight/premier run réel), dans le même ordre. Aucun second pipeline.

def recalculer(*, db_path=None) -> dict[str, Any]:
    from app.services import orchestrateur_moteur
    from app.services import flux_unifie_service
    from app.services import controles_lot11_service
    from app.services import lot12_prefactures_service

    etapes: list[dict[str, Any]] = []

    def _etape(nom: str, resultat: dict[str, Any]) -> bool:
        etapes.append({"etape": nom, "resultat": resultat})
        return bool(resultat.get("ok", True))

    if not _etape("RESERVATIONS", orchestrateur_moteur.executer_reservations(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("FLUX_UNIFIE", flux_unifie_service.construire(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("LOT10", orchestrateur_moteur.executer_lot10(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("LOT11", controles_lot11_service.construire(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("LOT12", lot12_prefactures_service.construire(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    return {"ok": True, "etapes": etapes}

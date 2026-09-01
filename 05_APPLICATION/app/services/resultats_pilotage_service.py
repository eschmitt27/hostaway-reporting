"""Pilotage — vue globale/propriétaire/logement/plateforme + série mensuelle (mission « comptabilité
+ résultats + graphiques »).

PRINCIPE : LE MOTEUR CALCULE, L'APPLICATION CONSTATE. Aucun calcul économique n'est refait ici —
uniquement des lectures et des SOMMES de lignes déjà calculées par Lot10 (`lot10_net_reglement` au
grain mois × logement × propriétaire, `lot10_commissions` au grain réservation pour la ventilation
par plateforme). Les deux lecteurs (`proprietaires_reglements_reader.net_reglement`/`commissions`)
filtrent déjà sur le run Lot10 ACTIF — jamais deux générations mélangées.

DÉFINITIONS (reprises telles quelles du moteur, jamais inventées) :
  TOTAL PAYOUT         = somme des payouts Hostaway/HH retenus (lot10_net_reglement.total_payout_mois)
  CA CONCIERGERIE       = lot10_net_reglement.montant_du_conciergerie = commission + ménage facturé
                          + préparation canapé + charge fixe + charges refacturées réalisées
                          (formule exacte de `lot10_calculer_resultats.build_net_proprietaire`,
                          D033/D034 — jamais recalculée ici)
  COMMISSION CONCIERGERIE = lot10_net_reglement.total_commission_mois
  NET PROPRIÉTAIRE      = lot10_net_reglement.net_proprietaire_apres_charge_mois (après charge fixe,
                          la valeur définitive facturée)

Quand un filtre PLATEFORME est actif, seuls payout/ménage/commission/nb_reservations sont
disponibles (grain réservation, `lot10_commissions.channel_type`) : la préparation canapé/charge
fixe/refacturations vivent au grain mois × logement, pas réservation — jamais ventilées par canal
ici plutôt que d'inventer une clé de répartition.
"""
from __future__ import annotations

from typing import Any

from app.readers import proprietaires_reglements_reader as reader
from app.services import referentiel_service as ref_svc

NON_DISPONIBLE = "NON_DISPONIBLE"
OK = "OK"


def _num(v: Any) -> float:
    return reader.to_nombre(v) or 0.0


def mois_disponibles(*, db_path=None) -> list[str]:
    """Mois réellement présents dans Lot10 (run actif) — jamais un calendrier inventé."""
    src = reader.net_reglement(db_path=db_path)
    if not src.etat.disponible:
        return []
    return sorted({reader.to_mois(r.get("mois")) for r in src.lignes if r.get("mois")})


def _lignes_net_reglement(*, mois: str = "", proprietaire_id: str = "",
                         logement_id: str = "", db_path=None) -> list[dict[str, Any]]:
    src = reader.net_reglement(db_path=db_path)
    if not src.etat.disponible:
        return []
    out = []
    for r in src.lignes:
        if mois and reader.to_mois(r.get("mois")) != mois:
            continue
        if proprietaire_id and reader.to_texte(r.get("proprietaire_id")) != proprietaire_id:
            continue
        if logement_id and reader.to_texte(r.get("logement_id")) != logement_id:
            continue
        out.append(r)
    return out


def _lignes_commissions(*, mois: str = "", proprietaire_id: str = "", logement_id: str = "",
                        canal: str = "", db_path=None) -> list[dict[str, Any]]:
    src = reader.commissions(db_path=db_path)
    if not src.etat.disponible:
        return []
    out = []
    for r in src.lignes:
        if mois and reader.to_mois(r.get("mois")) != mois:
            continue
        if proprietaire_id and reader.to_texte(r.get("proprietaire_id")) != proprietaire_id:
            continue
        if logement_id and reader.to_texte(r.get("logement_id")) != logement_id:
            continue
        if canal and reader.to_texte(r.get("channel_type")).upper() != canal.upper():
            continue
        out.append(r)
    return out


def _agreger_net_reglement(lignes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_payout": round(sum(_num(l.get("total_payout_mois")) for l in lignes), 2),
        "ca_conciergerie": round(sum(_num(l.get("montant_du_conciergerie")) for l in lignes), 2),
        "commission": round(sum(_num(l.get("total_commission_mois")) for l in lignes), 2),
        "menage": round(sum(_num(l.get("total_menage_mois")) for l in lignes), 2),
        "canape": round(sum(_num(l.get("total_preparation_canape_mois")) for l in lignes), 2),
        "charge_fixe": round(sum(_num(l.get("charge_fixe_mensuelle")) for l in lignes), 2),
        "refacturations": round(sum(_num(l.get("charges_exceptionnelles_refacturees")) for l in lignes), 2),
        "net_proprietaire": round(sum(_num(l.get("net_proprietaire_apres_charge_mois")) for l in lignes), 2),
        "nb_reservations": int(sum(_num(l.get("nb_reservations")) for l in lignes)),
    }


def _agreger_commissions(lignes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_payout": round(sum(_num(l.get("payout_calcule")) for l in lignes), 2),
        "commission": round(sum(_num(l.get("commission_conciergerie")) for l in lignes), 2),
        "menage": round(sum(_num(l.get("menage_retenu")) for l in lignes), 2),
        "nb_reservations": len(lignes),
        # Ambigu au grain réservation — jamais ventilé ici (voir docstring module).
        "ca_conciergerie": None,
        "canape": None,
        "charge_fixe": None,
        "refacturations": None,
        "net_proprietaire": None,
    }


def filtres_reference(*, db_path=None) -> dict[str, list[dict[str, str]]]:
    """Options réelles pour les filtres — vrais noms via les resolvers déjà construits."""
    src = reader.net_reglement(db_path=db_path)
    props: set[str] = set()
    logs: set[str] = set()
    if src.etat.disponible:
        for r in src.lignes:
            p = reader.to_texte(r.get("proprietaire_id"))
            l = reader.to_texte(r.get("logement_id"))
            if p:
                props.add(p)
            if l:
                logs.add(l)
    src_com = reader.commissions(db_path=db_path)
    canaux: set[str] = set()
    if src_com.etat.disponible:
        for r in src_com.lignes:
            c = reader.to_texte(r.get("channel_type"))
            if c:
                canaux.add(c.upper())
    return {
        "proprietaires": [{"id": p, "libelle": ref_svc.libelle_proprietaire(p, db_path=db_path)} for p in sorted(props)],
        "logements": [{"id": l, "libelle": ref_svc.libelle_logement(l, db_path=db_path)} for l in sorted(logs)],
        "canaux": [{"id": c, "libelle": c.capitalize()} for c in sorted(canaux)],
    }


def logements_du_proprietaire(proprietaire_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Logements réellement rattachés à ce propriétaire dans Lot10 — pour limiter le filtre logement
    quand un propriétaire est sélectionné (mission § 11)."""
    if not proprietaire_id:
        return filtres_reference(db_path=db_path)["logements"]
    src = reader.net_reglement(db_path=db_path)
    if not src.etat.disponible:
        return []
    logs = sorted({reader.to_texte(r.get("logement_id")) for r in src.lignes
                  if reader.to_texte(r.get("proprietaire_id")) == proprietaire_id
                  and reader.to_texte(r.get("logement_id"))})
    return [{"id": l, "libelle": ref_svc.libelle_logement(l, db_path=db_path)} for l in logs]


def vue(*, mois: str = "", proprietaire_id: str = "", logement_id: str = "",
       canal: str = "", db_path=None) -> dict[str, Any]:
    """KPI agrégés pour le périmètre demandé — jamais un second calcul économique, uniquement la
    somme des lignes Lot10 déjà calculées (run actif)."""
    if canal:
        lignes = _lignes_commissions(mois=mois, proprietaire_id=proprietaire_id,
                                     logement_id=logement_id, canal=canal, db_path=db_path)
        source_disponible = reader.commissions(db_path=db_path).etat.disponible
        agg = _agreger_commissions(lignes)
        ventilation_limitee = True
    else:
        lignes = _lignes_net_reglement(mois=mois, proprietaire_id=proprietaire_id,
                                       logement_id=logement_id, db_path=db_path)
        source_disponible = reader.net_reglement(db_path=db_path).etat.disponible
        agg = _agreger_net_reglement(lignes)
        ventilation_limitee = False

    if not source_disponible:
        return {"statut": NON_DISPONIBLE, "kpi": None, "ventilation_limitee": ventilation_limitee}
    if not lignes:
        return {"statut": "VIDE", "kpi": agg, "ventilation_limitee": ventilation_limitee}
    return {"statut": OK, "kpi": agg, "ventilation_limitee": ventilation_limitee}


def serie_mensuelle(*, proprietaire_id: str = "", logement_id: str = "",
                    canal: str = "", db_path=None) -> dict[str, Any]:
    """Une entrée par mois disponible — TOTAL PAYOUT / CA CONCIERGERIE / COMMISSION (graphique § 12).

    CA CONCIERGERIE reste None (jamais 0 inventé) quand un filtre plateforme est actif : le champ
    n'est pas ventilable au grain réservation (voir docstring module)."""
    mois_liste = mois_disponibles(db_path=db_path)
    points = []
    for m in mois_liste:
        v = vue(mois=m, proprietaire_id=proprietaire_id, logement_id=logement_id, canal=canal,
                db_path=db_path)
        kpi = v.get("kpi") or {}
        points.append({
            "mois": m,
            "total_payout": kpi.get("total_payout", 0.0),
            "ca_conciergerie": kpi.get("ca_conciergerie"),
            "commission": kpi.get("commission", 0.0),
        })
    return {"statut": OK if mois_liste else NON_DISPONIBLE, "points": points}

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


def _dans_periode(m: str, *, mois: str = "", du: str = "", au: str = "") -> bool:
    """`mois` = un mois exact ; `du`/`au` = bornes incluses (AAAA-MM, comparables en texte).
    Une borne vide ne restreint rien : sans aucune borne, toute la période disponible."""
    if mois and m != mois:
        return False
    if du and m < du:
        return False
    if au and m > au:
        return False
    return True


def _filtrer(lignes: list[dict[str, Any]], *, mois: str = "", du: str = "", au: str = "",
             proprietaire_id: str = "", logement_id: str = "", canal: str = "") -> list[dict[str, Any]]:
    out = []
    for r in lignes:
        if not _dans_periode(reader.to_mois(r.get("mois")), mois=mois, du=du, au=au):
            continue
        if proprietaire_id and reader.to_texte(r.get("proprietaire_id")) != proprietaire_id:
            continue
        if logement_id and reader.to_texte(r.get("logement_id")) != logement_id:
            continue
        if canal and reader.to_texte(r.get("channel_type")).upper() != canal.upper():
            continue
        out.append(r)
    return out


def _lignes_net_reglement(*, mois: str = "", du: str = "", au: str = "", proprietaire_id: str = "",
                         logement_id: str = "", db_path=None) -> list[dict[str, Any]]:
    src = reader.net_reglement(db_path=db_path)
    if not src.etat.disponible:
        return []
    return _filtrer(src.lignes, mois=mois, du=du, au=au, proprietaire_id=proprietaire_id,
                    logement_id=logement_id)


def _lignes_commissions(*, mois: str = "", du: str = "", au: str = "", proprietaire_id: str = "",
                        logement_id: str = "", canal: str = "", db_path=None) -> list[dict[str, Any]]:
    src = reader.commissions(db_path=db_path)
    if not src.etat.disponible:
        return []
    return _filtrer(src.lignes, mois=mois, du=du, au=au, proprietaire_id=proprietaire_id,
                    logement_id=logement_id, canal=canal)


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


#: Libellés d'affichage des canaux Hostaway (`channel_type`) — le code reste la valeur du filtre.
_LIBELLES_CANAUX = {
    "AIRBNB": "Airbnb", "AIRBNBOFFICIAL": "Airbnb",
    "BOOKING": "Booking.com", "BOOKINGCOM": "Booking.com",
    "VRBO": "Vrbo", "VRBOICAL": "Vrbo", "HOMEAWAY": "Vrbo",
    "DIRECT": "Direct", "EXPEDIA": "Expedia",
}


def libelle_canal(code: str) -> str:
    c = reader.to_texte(code).upper()
    return _LIBELLES_CANAUX.get(c, c.capitalize())


def filtres_reference(*, db_path=None) -> dict[str, list[dict[str, Any]]]:
    """Options réelles pour les filtres — vrais noms via les resolvers déjà construits.

    Chaque logement porte la liste des propriétaires auxquels Lot10 le rattache : l'écran s'en
    sert pour restreindre la liste des logements dès qu'un propriétaire est choisi, sans recharger."""
    src = reader.net_reglement(db_path=db_path)
    props: set[str] = set()
    logs: dict[str, set[str]] = {}
    if src.etat.disponible:
        for r in src.lignes:
            p = reader.to_texte(r.get("proprietaire_id"))
            l = reader.to_texte(r.get("logement_id"))
            if p:
                props.add(p)
            if l:
                logs.setdefault(l, set())
                if p:
                    logs[l].add(p)
    src_com = reader.commissions(db_path=db_path)
    canaux: set[str] = set()
    if src_com.etat.disponible:
        for r in src_com.lignes:
            c = reader.to_texte(r.get("channel_type"))
            if c:
                canaux.add(c.upper())
    logements = [{"id": l, "libelle": ref_svc.libelle_logement(l, db_path=db_path),
                  "proprietaires": sorted(logs[l])} for l in logs]
    return {
        "proprietaires": sorted(({"id": p, "libelle": ref_svc.libelle_proprietaire(p, db_path=db_path)}
                                 for p in props), key=lambda d: d["libelle"].lower()),
        "logements": sorted(logements, key=lambda d: d["libelle"].lower()),
        "canaux": [{"id": c, "libelle": libelle_canal(c)} for c in sorted(canaux)],
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


def _source_et_lignes(*, mois: str = "", du: str = "", au: str = "", proprietaire_id: str = "",
                      logement_id: str = "", canal: str = "", db_path=None):
    """(source disponible ?, lignes filtrées, agrégateur) — UNE lecture de la table du grain
    adapté : réservation si un canal est demandé, mois × logement sinon."""
    if canal:
        src = reader.commissions(db_path=db_path)
        agreger = _agreger_commissions
    else:
        src = reader.net_reglement(db_path=db_path)
        agreger = _agreger_net_reglement
    if not src.etat.disponible:
        return False, [], agreger
    return True, _filtrer(src.lignes, mois=mois, du=du, au=au, proprietaire_id=proprietaire_id,
                          logement_id=logement_id, canal=canal), agreger


def vue(*, mois: str = "", proprietaire_id: str = "", logement_id: str = "",
       canal: str = "", du: str = "", au: str = "", db_path=None) -> dict[str, Any]:
    """KPI agrégés pour le périmètre demandé — jamais un second calcul économique, uniquement la
    somme des lignes Lot10 déjà calculées (run actif). `du`/`au` : plage de mois incluse."""
    source_disponible, lignes, agreger = _source_et_lignes(
        mois=mois, du=du, au=au, proprietaire_id=proprietaire_id, logement_id=logement_id,
        canal=canal, db_path=db_path)
    ventilation_limitee = bool(canal)
    if not source_disponible:
        return {"statut": NON_DISPONIBLE, "kpi": None, "ventilation_limitee": ventilation_limitee}
    agg = agreger(lignes)
    if not lignes:
        return {"statut": "VIDE", "kpi": agg, "ventilation_limitee": ventilation_limitee}
    return {"statut": OK, "kpi": agg, "ventilation_limitee": ventilation_limitee}


def serie_mensuelle(*, proprietaire_id: str = "", logement_id: str = "",
                    canal: str = "", du: str = "", au: str = "", db_path=None) -> dict[str, Any]:
    """Une entrée par mois disponible — TOTAL PAYOUT / CA CONCIERGERIE / COMMISSION (graphique § 12).

    CA CONCIERGERIE reste None (jamais 0 inventé) quand un filtre plateforme est actif : le champ
    n'est pas ventilable au grain réservation (voir docstring module).

    Mêmes sommes que `vue(mois=m, …)` pour chaque mois — mais la table n'est lue qu'une fois puis
    répartie par mois, au lieu d'une lecture complète par mois affiché."""
    mois_liste = [m for m in mois_disponibles(db_path=db_path) if _dans_periode(m, du=du, au=au)]
    source_disponible, lignes, agreger = _source_et_lignes(
        du=du, au=au, proprietaire_id=proprietaire_id, logement_id=logement_id, canal=canal,
        db_path=db_path)
    par_mois: dict[str, list[dict[str, Any]]] = {}
    for l in lignes:
        par_mois.setdefault(reader.to_mois(l.get("mois")), []).append(l)
    points = []
    for m in mois_liste:
        kpi = agreger(par_mois.get(m, [])) if source_disponible else {}
        points.append({
            "mois": m,
            "total_payout": kpi.get("total_payout", 0.0),
            "ca_conciergerie": kpi.get("ca_conciergerie"),
            "commission": kpi.get("commission", 0.0),
        })
    return {"statut": OK if mois_liste else NON_DISPONIBLE, "points": points}


def par_logement(*, mois: str = "", du: str = "", au: str = "", proprietaire_id: str = "",
                 logement_id: str = "", canal: str = "", db_path=None) -> list[dict[str, Any]]:
    """Mêmes agrégats que `vue`, répartis par logement (même périmètre, même source) — du plus
    gros CA conciergerie au plus petit (commission quand un canal est filtré : le CA n'y est pas
    ventilé). Sert la comparaison « quels logements produisent le plus ? »."""
    source_disponible, lignes, agreger = _source_et_lignes(
        mois=mois, du=du, au=au, proprietaire_id=proprietaire_id, logement_id=logement_id,
        canal=canal, db_path=db_path)
    if not source_disponible:
        return []
    groupes: dict[str, list[dict[str, Any]]] = {}
    for l in lignes:
        groupes.setdefault(reader.to_texte(l.get("logement_id")), []).append(l)
    out = []
    for lid, rows in groupes.items():
        props = sorted({reader.to_texte(r.get("proprietaire_id")) for r in rows
                        if reader.to_texte(r.get("proprietaire_id"))})
        out.append({"logement_id": lid,
                    "libelle": ref_svc.libelle_logement(lid, db_path=db_path) if lid else "Sans logement",
                    "proprietaires": props, **agreger(rows)})
    cle = "commission" if canal else "ca_conciergerie"
    out.sort(key=lambda d: (-(d.get(cle) or 0.0), d["libelle"].lower()))
    return out


def par_canal(*, mois: str = "", du: str = "", au: str = "", proprietaire_id: str = "",
              logement_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Payout / commission / ménage / réservations par plateforme (`lot10_commissions.channel_type`,
    grain réservation). Le CA conciergerie n'y figure pas : il n'est pas ventilable par canal."""
    src = reader.commissions(db_path=db_path)
    if not src.etat.disponible:
        return []
    lignes = _filtrer(src.lignes, mois=mois, du=du, au=au, proprietaire_id=proprietaire_id,
                      logement_id=logement_id)
    groupes: dict[str, list[dict[str, Any]]] = {}
    for l in lignes:
        groupes.setdefault(reader.to_texte(l.get("channel_type")).upper(), []).append(l)
    out = [{"canal": c, "libelle": libelle_canal(c) if c else "Non renseignée",
            **_agreger_commissions(rows)} for c, rows in groupes.items()]
    out.sort(key=lambda d: (-d["total_payout"], d["libelle"].lower()))
    return out

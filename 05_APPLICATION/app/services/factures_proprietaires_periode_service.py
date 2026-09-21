"""Facture propriétaire sur une PÉRIODE LIBRE — parcours manuel, à côté du cycle mensuel.

CE QUE CELA RÉSOUT
Le cycle mensuel facture un mois entier, une fois le mois passé. Un propriétaire qui arrête son
contrat le 13 septembre devait donc attendre le 30 pour être facturé de ses treize jours. Ce module
ouvre un second parcours, volontaire : un propriétaire, un logement, deux dates, une
prévisualisation, un brouillon. Le cycle automatique, lui, n'est pas touché — il garde ses règles.

CE MODULE NE CALCULE AUCUN MONTANT
Les montants viennent tels quels du moteur canonique : `lot10_commissions` pour ce qui se calcule
PAR RÉSERVATION, `lot10_net_reglement` pour ce qui se calcule PAR MOIS. Ici, on ne fait que
SÉLECTIONNER et ADDITIONNER. Aucune règle de commission, de ménage ou de forfait n'est réécrite ;
si le moteur change, ce module suit sans être modifié.

── LES DEUX GRAINS DU MOTEUR, ET POURQUOI ILS NE SE DÉCOUPENT PAS PAREIL ────────────────────────
PAR RÉSERVATION  commission, ménage, préparation du canapé. Le moteur range chaque réservation dans
                 un mois d'après sa DATE D'ARRIVÉE ; une période libre applique exactement la même
                 règle, sur ses propres bornes. Douze factures de dix jours refont donc une année.

PAR MOIS         forfait (charge fixe) et charges exceptionnelles refacturées. Le moteur les pose
                 UNE FOIS PAR MOIS CIVIL de gestion, au montant plein
                 (`REF_Logements.forfait_logiciel_consommables_mensuel`, cf.
                 `lot10_calculer_resultats.build_charge_fixe`). Il n'existe AUCUNE règle de
                 proratisation dans le moteur, et en inventer une ici fabriquerait un montant que
                 rien d'autre dans l'application ne saurait reproduire.

D'où la seule règle possible sans invention : un élément mensuel est facturé quand la période
COUVRE LE MOIS CIVIL ENTIER, et écarté sinon. Un mois partiel ne porte donc pas de forfait — le
montant écarté est RENDU VISIBLE (`mensuels_ecartes`), jamais silencieusement perdu : l'utilisateur
l'ajoute au brouillon s'il estime qu'il est dû, par les parcours d'édition existants.

La création passe par `factures_proprietaires_service.creer_exceptionnelle` : même objet facture,
même numérotation, même conformité, même PDF, même émission. Rien n'est allégé.
"""
from __future__ import annotations

import calendar
from datetime import date
from typing import Any

from app.db.connection import get_db
from app.services import factures_proprietaires_service as svc

#: Lignes facturables reconstituées depuis le calcul PAR RÉSERVATION, et leur source dans
#: `lot10_commissions`. L'ordre est celui de la facture.
LIGNES_RESERVATION = (
    ("COMMISSION_CONCIERGERIE", "commission_conciergerie"),
    ("MENAGE_FACTURE", "menage_retenu"),
    ("PREPARATION_CANAPE", "preparation_canape_voyageurs"),
)

#: Lignes facturables au grain MENSUEL, et leur source dans `lot10_net_reglement`. Indivisibles.
LIGNES_MENSUELLES = (
    ("CHARGE_FIXE", "charge_fixe_mensuelle"),
    ("CHARGES_EXCEPT_REFAC", "charges_exceptionnelles_refacturees"),
)

#: Ordre d'apparition sur la facture, tous grains confondus.
ORDRE_LIGNES = [t for t, _ in LIGNES_RESERVATION] + [t for t, _ in LIGNES_MENSUELLES]

E_PERIODE_INVALIDE = "PERIODE_INVALIDE"
E_PROPRIETAIRE_MANQUANT = "PROPRIETAIRE_MANQUANT"
E_AUCUNE_RESERVATION = "AUCUNE_RESERVATION"


def _iso(valeur: Any) -> str:
    v = str(valeur or "").strip()
    try:
        return date.fromisoformat(v).isoformat()
    except ValueError:
        return ""


def _run_actif(conn) -> str:
    r = conn.execute("SELECT run_id FROM lot10_runs WHERE actif = 1 "
                     "ORDER BY id DESC LIMIT 1").fetchone()
    return r["run_id"] if r else ""


def _bornes_du_mois(mois: str) -> tuple[str, str]:
    a, m = int(mois[:4]), int(mois[5:7])
    return f"{mois}-01", f"{mois}-{calendar.monthrange(a, m)[1]:02d}"


def mois_traverses(debut: str, fin: str) -> list[str]:
    """Mois civils touchés par la période, du premier au dernier."""
    a, m = int(debut[:4]), int(debut[5:7])
    dernier = fin[:7]
    out = []
    while f"{a:04d}-{m:02d}" <= dernier:
        out.append(f"{a:04d}-{m:02d}")
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    return out


def _mois_entierement_couvert(mois: str, debut: str, fin: str) -> bool:
    """La période englobe-t-elle le mois civil du premier au dernier jour ?"""
    m_debut, m_fin = _bornes_du_mois(mois)
    return debut <= m_debut and fin >= m_fin


def reservations_periode(proprietaire_id: str, debut: str, fin: str,
                         db_path=None) -> list[dict[str, Any]]:
    """Réservations du propriétaire dont la DATE D'ARRIVÉE tombe dans la période.

    La date d'arrivée est le critère du moteur lui-même : c'est elle qui décide du `mois` d'une
    réservation dans `lot10_commissions`. Une période libre applique donc exactement la même règle
    qu'un mois entier — sans quoi douze factures de dix jours ne feraient pas une année.
    """
    conn = get_db(db_path)
    try:
        run = _run_actif(conn)
        if not run:
            return []
        return [dict(r) for r in conn.execute(
            "SELECT * FROM lot10_commissions WHERE run_id = ? AND proprietaire_id = ? "
            "AND date_arrivee >= ? AND date_arrivee <= ? "
            "ORDER BY logement_id, date_arrivee, reservation_calc_id",
            (run, proprietaire_id, debut, fin)).fetchall()]
    finally:
        conn.close()


def elements_mensuels(proprietaire_id: str, debut: str, fin: str,
                      db_path=None) -> list[dict[str, Any]]:
    """Forfaits et charges refacturées des mois traversés, avec leur grain intact.

    Chaque élément dit s'il est retenu (`retenu`) ou écarté, et pourquoi. On ne décide pas ici à la
    place de l'utilisateur : on rend la règle du moteur lisible.
    """
    conn = get_db(db_path)
    try:
        run = _run_actif(conn)
        if not run:
            return []
        mois = mois_traverses(debut, fin)
        if not mois:
            return []
        marques = ",".join("?" * len(mois))
        lignes = [dict(r) for r in conn.execute(
            "SELECT * FROM lot10_net_reglement WHERE run_id = ? AND proprietaire_id = ? "
            "AND mois IN (" + marques + ") ORDER BY logement_id, mois",
            (run, proprietaire_id, *mois)).fetchall()]
    finally:
        conn.close()

    out = []
    for r in lignes:
        entier = _mois_entierement_couvert(r["mois"], debut, fin)
        for type_ligne, champ in LIGNES_MENSUELLES:
            montant = round(float(r.get(champ) or 0), 2)
            if montant <= 0:
                continue
            out.append({
                "type_ligne": type_ligne, "libelle": svc.LIBELLES[type_ligne],
                "logement_id": str(r.get("logement_id") or ""), "mois": r["mois"],
                "montant": montant, "retenu": entier,
                "motif": "" if entier else (
                    "mois " + str(r["mois"]) + " seulement partiellement couvert — élément "
                    "mensuel non proratisé par le moteur, donc non facturé ici"),
            })
    return out


def _periode_facture(f: dict[str, Any]) -> tuple[str, str]:
    """Période couverte par une facture existante : la sienne si elle en porte une, sinon le mois."""
    debut, fin = _iso(f.get("periode_debut")), _iso(f.get("periode_fin"))
    if debut and fin:
        return debut, fin
    mois = str(f.get("mois") or "")
    if len(mois) != 7:
        return "", ""
    return _bornes_du_mois(mois)


def chevauchements(proprietaire_id: str, debut: str, fin: str, *, logement_id: str = "",
                   db_path=None) -> list[dict[str, Any]]:
    """Factures du propriétaire couvrant déjà tout ou partie de la période.

    Ne bloque rien : deux factures peuvent légitimement se chevaucher (une régularisation, un
    complément). Mais l'utilisateur doit le savoir AVANT d'émettre — une facture en double se
    répare beaucoup plus difficilement qu'elle ne s'évite.
    """
    out = []
    for f in svc.lister(proprietaire_id=proprietaire_id, db_path=db_path):
        if f["statut"] == svc.ST_ANNULE:
            continue
        if logement_id and f.get("logement_id") and f["logement_id"] != logement_id:
            continue
        f_debut, f_fin = _periode_facture(f)
        if not f_debut or not f_fin or f_fin < debut or f_debut > fin:
            continue
        out.append({**f, "periode_debut_effective": f_debut, "periode_fin_effective": f_fin})
    return out


def previsualiser(proprietaire_id: str, debut: str, fin: str, *, logement_id: str = "",
                  db_path=None) -> dict[str, Any]:
    """Ce que serait la facture, par logement, sans rien écrire."""
    proprietaire_id = str(proprietaire_id or "").strip()
    logement_id = str(logement_id or "").strip()
    debut, fin = _iso(debut), _iso(fin)
    if not proprietaire_id:
        return {"ok": False, "code": E_PROPRIETAIRE_MANQUANT,
                "message": "Choisir un propriétaire."}
    if not debut or not fin:
        return {"ok": False, "code": E_PERIODE_INVALIDE,
                "message": "Renseigner une date de début et une date de fin (AAAA-MM-JJ)."}
    if fin < debut:
        return {"ok": False, "code": E_PERIODE_INVALIDE,
                "message": "La date de fin précède la date de début."}

    reservations = reservations_periode(proprietaire_id, debut, fin, db_path=db_path)
    mensuels = elements_mensuels(proprietaire_id, debut, fin, db_path=db_path)
    if logement_id:
        reservations = [r for r in reservations if str(r.get("logement_id") or "") == logement_id]
        mensuels = [m for m in mensuels if m["logement_id"] == logement_id]

    par_logement: dict[str, list[dict[str, Any]]] = {}
    for r in reservations:
        par_logement.setdefault(str(r.get("logement_id") or ""), []).append(r)
    for m in mensuels:
        par_logement.setdefault(m["logement_id"], [])

    propositions = []
    for log_id, lignes_res in sorted(par_logement.items()):
        montants: dict[str, float] = {}
        for type_ligne, champ in LIGNES_RESERVATION:
            montant = round(sum(float(r.get(champ) or 0) for r in lignes_res), 2)
            if montant > 0:
                montants[type_ligne] = montant
        retenus = [m for m in mensuels if m["logement_id"] == log_id and m["retenu"]]
        ecartes = [m for m in mensuels if m["logement_id"] == log_id and not m["retenu"]]
        for m in retenus:
            montants[m["type_ligne"]] = round(montants.get(m["type_ligne"], 0.0) + m["montant"], 2)

        lignes = [{"numero_ligne": i, "type_ligne": t, "libelle": svc.LIBELLES[t],
                   "montant": montants[t]}
                  for i, t in enumerate([t for t in ORDRE_LIGNES if t in montants], start=1)]
        propositions.append({
            "logement_id": log_id,
            "lignes": lignes,
            "montant_total": round(sum(l["montant"] for l in lignes), 2),
            "nb_reservations": len(lignes_res),
            "net_proprietaire": round(sum(float(r.get("net_proprietaire") or 0) for r in lignes_res), 2),
            "payout": round(sum(float(r.get("payout_calcule") or 0) for r in lignes_res), 2),
            "mensuels_retenus": retenus,
            "mensuels_ecartes": ecartes,
            "reservations": [{
                "reservation": r.get("reservation_calc_id"),
                "date_arrivee": r.get("date_arrivee"), "date_depart": r.get("date_depart"),
                "nuits": r.get("nuits"), "payout": r.get("payout_calcule"),
                "commission": r.get("commission_conciergerie"), "menage": r.get("menage_retenu"),
                "canape": r.get("preparation_canape_voyageurs"),
                "net_proprietaire": r.get("net_proprietaire"),
            } for r in lignes_res],
            "facturable": bool(lignes),
        })
    return {
        "ok": True, "proprietaire_id": proprietaire_id, "logement_id": logement_id,
        "debut": debut, "fin": fin,
        "propositions": propositions,
        "montant_total": round(sum(p["montant_total"] for p in propositions), 2),
        "nb_reservations": len(reservations),
        "mois_traverses": mois_traverses(debut, fin),
        "mensuels_ecartes": [m for m in mensuels if not m["retenu"]],
        "chevauchements": chevauchements(proprietaire_id, debut, fin, logement_id=logement_id,
                                         db_path=db_path),
    }


def creer(proprietaire_id: str, debut: str, fin: str, *, logement_id: str = "",
          acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée le ou les BROUILLON de la période. Le chevauchement éventuel est RENDU, jamais bloquant."""
    apercu = previsualiser(proprietaire_id, debut, fin, logement_id=logement_id, db_path=db_path)
    if not apercu.get("ok"):
        return apercu
    retenues = [p for p in apercu["propositions"] if p["facturable"]]
    if not retenues:
        return {"ok": False, "code": E_AUCUNE_RESERVATION,
                "message": ("Aucun élément facturable sur cette période pour ce propriétaire "
                            "(source : calcul du moteur). Vérifiez les dates, ou le logement "
                            "choisi."),
                "apercu": apercu}

    creees = []
    for p in retenues:
        resultat = svc.creer_exceptionnelle(
            proprietaire_id=apercu["proprietaire_id"], logement_id=p["logement_id"],
            mois=apercu["debut"][:7], lignes=p["lignes"], acteur=acteur, db_path=db_path)
        conn = get_db(db_path)
        try:
            conn.execute(
                "UPDATE factures_proprietaires SET periode_debut = ?, periode_fin = ? "
                "WHERE facture_id_opaque = ?",
                (apercu["debut"], apercu["fin"], resultat["facture_id_opaque"]))
            conn.commit()
        finally:
            conn.close()
        creees.append({**resultat, "logement_id": p["logement_id"],
                       "periode_debut": apercu["debut"], "periode_fin": apercu["fin"]})
    return {"ok": True, "creees": creees, "nb": len(creees),
            "chevauchements": apercu["chevauchements"],
            "mensuels_ecartes": apercu["mensuels_ecartes"],
            "montant_total": round(sum(c["montant_total"] for c in creees), 2)}

"""Facture propriétaire sur une PÉRIODE choisie — parcours manuel, à côté du cycle mensuel.

CE QUE CELA RÉSOUT
Le cycle mensuel facture un mois entier, une fois le mois passé. Un propriétaire qui arrête son
contrat le 10 septembre devait donc attendre le 30 pour être facturé de ses dix jours. Ce module
ouvre un second parcours, volontaire : un propriétaire, deux dates, une prévisualisation, un
brouillon. Le cycle automatique, lui, n'est pas touché — il garde ses règles.

CE MODULE NE CALCULE AUCUNE ÉCONOMIE
Les montants viennent tels quels de `lot10_commissions`, le résultat du moteur canonique pour
CHAQUE réservation (assiette, taux, commission, ménage retenu). Ici, on ne fait que SÉLECTIONNER
les réservations dont la date d'arrivée tombe dans la période — la même date que celle qui range
une réservation dans son mois côté moteur — et ADDITIONNER ce que le moteur a déjà calculé.
Aucune règle de commission n'est réécrite ; si le moteur change, ce module suit sans être modifié.

CE QU'IL NE FAIT PAS
Les charges fixes et les charges exceptionnelles refacturées sont des éléments MENSUELS : les
découper au prorata serait une invention. Elles ne sont pas reprises ici ; le brouillon créé reste
modifiable et l'utilisateur les ajoute s'il y a lieu, par les parcours existants.

La création passe par `factures_proprietaires_service.creer_exceptionnelle` : même objet facture,
même numérotation, même conformité, même émission. Rien n'est allégé.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.db.connection import get_db
from app.services import factures_proprietaires_service as svc

#: Lignes facturables reconstituées depuis le calcul par réservation, et leur source dans
#: `lot10_commissions`. L'ordre est celui de la facture.
LIGNES_PERIODE = (
    ("COMMISSION_CONCIERGERIE", "commission_conciergerie"),
    ("MENAGE_FACTURE", "menage_retenu"),
)

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


def _periode_facture(f: dict[str, Any]) -> tuple[str, str]:
    """Période couverte par une facture existante : la sienne si elle en porte une, sinon le mois."""
    debut, fin = _iso(f.get("periode_debut")), _iso(f.get("periode_fin"))
    if debut and fin:
        return debut, fin
    mois = str(f.get("mois") or "")
    if len(mois) != 7:
        return "", ""
    annee, m = int(mois[:4]), int(mois[5:7])
    dernier = date(annee + (m == 12), 1 if m == 12 else m + 1, 1).toordinal() - 1
    return f"{mois}-01", date.fromordinal(dernier).isoformat()


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


def previsualiser(proprietaire_id: str, debut: str, fin: str, db_path=None) -> dict[str, Any]:
    """Ce que serait la facture, par logement, sans rien écrire."""
    proprietaire_id = str(proprietaire_id or "").strip()
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
    par_logement: dict[str, list[dict[str, Any]]] = {}
    for r in reservations:
        par_logement.setdefault(str(r.get("logement_id") or ""), []).append(r)

    propositions = []
    for logement_id, lignes_res in sorted(par_logement.items()):
        lignes = []
        for type_ligne, champ in LIGNES_PERIODE:
            montant = round(sum(float(r.get(champ) or 0) for r in lignes_res), 2)
            if montant <= 0:
                continue
            lignes.append({"numero_ligne": len(lignes) + 1, "type_ligne": type_ligne,
                           "libelle": svc.LIBELLES[type_ligne], "montant": montant})
        propositions.append({
            "logement_id": logement_id,
            "lignes": lignes,
            "montant_total": round(sum(l["montant"] for l in lignes), 2),
            "nb_reservations": len(lignes_res),
            "net_proprietaire": round(sum(float(r.get("net_proprietaire") or 0) for r in lignes_res), 2),
            "payout": round(sum(float(r.get("payout_calcule") or 0) for r in lignes_res), 2),
            "reservations": [{
                "reservation": r.get("reservation_calc_id"),
                "date_arrivee": r.get("date_arrivee"), "date_depart": r.get("date_depart"),
                "nuits": r.get("nuits"), "payout": r.get("payout_calcule"),
                "commission": r.get("commission_conciergerie"), "menage": r.get("menage_retenu"),
                "net_proprietaire": r.get("net_proprietaire"),
            } for r in lignes_res],
            "facturable": bool(lignes),
        })
    return {
        "ok": True, "proprietaire_id": proprietaire_id, "debut": debut, "fin": fin,
        "propositions": propositions,
        "montant_total": round(sum(p["montant_total"] for p in propositions), 2),
        "nb_reservations": len(reservations),
        "chevauchements": chevauchements(proprietaire_id, debut, fin, db_path=db_path),
    }


def creer(proprietaire_id: str, debut: str, fin: str, *, logement_id: str = "",
          acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée le ou les BROUILLON de la période. Le chevauchement éventuel est RENDU, jamais bloquant."""
    apercu = previsualiser(proprietaire_id, debut, fin, db_path=db_path)
    if not apercu.get("ok"):
        return apercu
    retenues = [p for p in apercu["propositions"] if p["facturable"]
                and (not logement_id or p["logement_id"] == logement_id)]
    if not retenues:
        return {"ok": False, "code": E_AUCUNE_RESERVATION,
                "message": ("Aucune réservation facturable sur cette période pour ce propriétaire "
                            "(source : calcul par réservation du moteur)."),
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
            "montant_total": round(sum(c["montant_total"] for c in creees), 2)}

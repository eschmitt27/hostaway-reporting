"""Catalogue de contrôles Ménages — même patron que `factures_controles_service.py`.

Ces contrôles portent sur le cycle de vie applicatif (`menages`, migration `0019`). Ils ne
dupliquent pas les contrôles moteur de lot6d/lot11 (rapprochement Hostaway vs déclaré, coût
standard/direct/complet, `CTR-9-*`) : ceux-là restent la vérité du comptage. Ici : cohérence des
ménages saisis individuellement et de leurs rattachements.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.db.connection import get_db
from app.services import menages_cycle_service as cycle

BLOQUANT = "BLOQUANT"
CRITIQUE = "CRITIQUE"
AVERTISSEMENT = "AVERTISSEMENT"
INFO = "INFO"
RESOLU = "RESOLU"
JUSTIFIE = "JUSTIFIE"

NIVEAUX = [BLOQUANT, CRITIQUE, AVERTISSEMENT, INFO, RESOLU, JUSTIFIE]
_ORDRE = {n: i for i, n in enumerate(NIVEAUX)}

M_SANS_RESERVATION = "CTRL_MEN_SANS_RESERVATION"
M_DOUBLON = "CTRL_MEN_DOUBLON"
M_PRESTATAIRE_ARCHIVE = "CTRL_MEN_PRESTATAIRE_ARCHIVE"
M_EXTERNE_SANS_FACTURE = "CTRL_MEN_EXTERNE_SANS_FACTURE"
M_DUREE_NEGATIVE = "CTRL_MEN_DUREE_NEGATIVE"
M_COUT_ABSENT = "CTRL_MEN_COUT_ABSENT"
M_ANNULE_FACTURE = "CTRL_MEN_ANNULE_FACTURE"
M_VALIDE_SANS_COUT = "CTRL_MEN_VALIDE_SANS_COUT"
M_CHARGE_DUPLIQUEE = "CTRL_MEN_CHARGE_DUPLIQUEE"
M_ECART_PREVU_REEL = "CTRL_MEN_ECART_COUT_PREVU_REEL"
M_FACTURE_SANS_REGLEMENT = "CTRL_MEN_FACTURE_SANS_REGLEMENT"

MESSAGES = {
    M_SANS_RESERVATION: "Ménage sans réservation associée.",
    M_DOUBLON: "Doublon probable (même logement, mois, type, prestataire).",
    M_PRESTATAIRE_ARCHIVE: "Prestataire affecté désormais archivé.",
    M_EXTERNE_SANS_FACTURE: "Ménage externe validé sans facture liée.",
    M_DUREE_NEGATIVE: "Durée réelle négative.",
    M_COUT_ABSENT: "Ménage validé sans coût réel.",
    M_ANNULE_FACTURE: "Ménage annulé mais encore lié à une facture.",
    M_VALIDE_SANS_COUT: "Ménage validé sans coût réel enregistré.",
    M_CHARGE_DUPLIQUEE: "Charge liée à plusieurs ménages.",
    M_ECART_PREVU_REEL: "Écart de coût prévu/réel non justifié.",
    M_FACTURE_SANS_REGLEMENT: "Facture liée déclarée réglée sans règlement retrouvé.",
}


def _anomalie(code: str, severite: str, identifiant: str, *, montant: Any = None,
             detail: str = "", action: str = "") -> dict[str, Any]:
    return {"code": code, "severite": severite, "objet": "MENAGE", "identifiant": identifiant,
           "message": MESSAGES.get(code, code), "montant": montant, "detail": detail,
           "action": action, "statut": "OUVERT", "justification": ""}


def _fournisseurs_actifs(db_path=None) -> set[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT fournisseur_id_opaque FROM fournisseurs WHERE actif=1 AND statut='ACTIF'"
        ).fetchall()
        return {r["fournisseur_id_opaque"] for r in rows}
    finally:
        conn.close()


def _controler_menage(m: dict[str, Any], actifs: set[str], charges_vues: dict[str, int],
                      db_path=None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    mid = m["menage_id_opaque"]

    if m["statut"] not in (cycle.ST_ANNULE,) and not m.get("reservation_id"):
        out.append(_anomalie(M_SANS_RESERVATION, INFO, mid,
                             detail="Ménage hors réservation (ex. sortie propriétaire) : "
                                    "informatif, pas nécessairement une anomalie."))

    if m["statut"] == cycle.ST_ANNULE and m.get("facture_id_opaque"):
        out.append(_anomalie(M_ANNULE_FACTURE, CRITIQUE, mid,
                             detail=f"facture {m['facture_id_opaque']}",
                             action="Vérifier si la facture doit être annulée/créditée."))

    if m.get("fournisseur_id_opaque") and m["fournisseur_id_opaque"] not in actifs \
            and m["statut"] not in (cycle.ST_ANNULE, cycle.ST_REMPLACE):
        out.append(_anomalie(M_PRESTATAIRE_ARCHIVE, AVERTISSEMENT, mid,
                             detail=m["fournisseur_id_opaque"],
                             action="Le prestataire est archivé ; vérifier la cohérence."))

    if m.get("duree_reelle_h") is not None and m["duree_reelle_h"] < 0:
        out.append(_anomalie(M_DUREE_NEGATIVE, BLOQUANT, mid, montant=m["duree_reelle_h"]))

    if m["statut"] == cycle.ST_VALIDE and m.get("cout_reel") is None:
        out.append(_anomalie(M_VALIDE_SANS_COUT, BLOQUANT, mid))

    if (m["type_menage"] == "EXTERNE" and m["statut"] in (cycle.ST_VALIDE, cycle.ST_FACTURE,
                                                          cycle.ST_REGLE)
            and not m.get("facture_id_opaque")):
        out.append(_anomalie(M_EXTERNE_SANS_FACTURE, CRITIQUE, mid,
                             action="Relier ou créer la facture fournisseur correspondante."))

    if (m.get("cout_prevu") is not None and m.get("cout_reel") is not None
            and abs(m["cout_reel"] - m["cout_prevu"]) > max(5.0, 0.10 * (m["cout_prevu"] or 1))
            and not m.get("ecart_justification")):
        out.append(_anomalie(M_ECART_PREVU_REEL, AVERTISSEMENT, mid,
                             montant=round(m["cout_reel"] - m["cout_prevu"], 2),
                             action="Justifier l'écart ou corriger la saisie."))

    if m.get("charge_id") and charges_vues.get(m["charge_id"], 0) > 1:
        out.append(_anomalie(M_CHARGE_DUPLIQUEE, BLOQUANT, mid, detail=m["charge_id"]))

    if m["statut"] == cycle.ST_REGLE and m.get("facture_id_opaque"):
        from app.services import factures_service as fact
        f = fact.charger(m["facture_id_opaque"], db_path)
        if f is not None and f.get("solde_restant", 0) > 0.005:
            out.append(_anomalie(M_FACTURE_SANS_REGLEMENT, CRITIQUE, mid,
                                 montant=f.get("solde_restant"),
                                 detail=m["facture_id_opaque"]))
    return out


def _doublons_probables(menages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Défense en profondeur : le doublon CERTAIN est déjà refusé par index unique (0019).
    Un doublon PROBABLE (même logement/mois/type, prestataires différents, dates proches) reste
    possible et n'est détecté qu'ici."""
    out = []
    vivants = [m for m in menages if m["statut"] != cycle.ST_ANNULE]
    for i, a in enumerate(vivants):
        for b in vivants[i + 1:]:
            if (a["logement_id"] == b["logement_id"] and a["mois"] == b["mois"]
                    and a["type_menage"] == b["type_menage"]
                    and a["menage_id_opaque"] != b["menage_id_opaque"]):
                da, db_ = a.get("date_prevue"), b.get("date_prevue")
                proche = True
                if da and db_:
                    try:
                        proche = abs((date.fromisoformat(da) - date.fromisoformat(db_)).days) <= 2
                    except ValueError:
                        proche = True
                if proche:
                    out.append(_anomalie(M_DOUBLON, AVERTISSEMENT, a["menage_id_opaque"],
                                         detail=f"proche de {b['menage_id_opaque']}",
                                         action="Vérifier qu'il ne s'agit pas du même ménage."))
    return out


def controler(db_path=None) -> dict[str, Any]:
    """Contrôle l'ensemble des ménages. Ne lève jamais."""
    try:
        menages = cycle.lister(db_path=db_path)
    except Exception as exc:
        return {"statut": "INDISPONIBLE", "anomalies": [], "detail": f"{type(exc).__name__}",
               "compteurs": {n: 0 for n in NIVEAUX}, "nb_bloquants": 0, "fiable": False}

    actifs = _fournisseurs_actifs(db_path)
    charges_vues: dict[str, int] = {}
    for m in menages:
        if m.get("charge_id"):
            charges_vues[m["charge_id"]] = charges_vues.get(m["charge_id"], 0) + 1

    anomalies: list[dict[str, Any]] = []
    for m in menages:
        anomalies.extend(_controler_menage(m, actifs, charges_vues, db_path))
    anomalies.extend(_doublons_probables(menages))

    anomalies.sort(key=lambda a: (_ORDRE.get(a["severite"], 9), a["code"]))
    compteurs = {n: sum(1 for a in anomalies if a["severite"] == n) for n in NIVEAUX}
    return {
        "statut": "OK", "anomalies": anomalies, "compteurs": compteurs,
        "nb_bloquants": compteurs[BLOQUANT], "fiable": compteurs[BLOQUANT] == 0,
    }

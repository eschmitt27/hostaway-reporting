"""Adaptateur VENTES ↔ Lot12 (cf. cadrage §4 de la mission Comptabilité).

Ne modifie jamais Lot12, ne duplique jamais son calcul : lit `montant_du_conciergerie`, déjà
calculé par le moteur (`proprietaires_reglements_service.load_owners`), et le transmet tel quel à
`comptabilite_ecritures_service.generer_ecriture_vente`. Source explicitement marquée
SOURCE_PROVISOIRE_LOT12 partout où elle apparaît — ceci n'est PAS une facture propriétaire émise
comme objet applicatif (décision explicite : hors périmètre de cette mission).
"""
from __future__ import annotations

from typing import Any

from app.services import proprietaires_reglements_service as lot12


def lignes_du_mois(mois: str) -> list[dict[str, Any]]:
    """Une ligne par propriétaire ayant une vue Lot12 ce mois-ci. Liste vide si la source Lot12
    est indisponible (ne lève jamais)."""
    page = 1
    out: list[dict[str, Any]] = []
    while True:
        res = lot12.load_owners(mois=mois, page=page)
        if res.get("status") != "OK":
            return []
        out.extend(res["rows"])
        if page >= res.get("pages", 1):
            break
        page += 1
    return [
        {"proprietaire_id": r["proprietaire_id"], "nom": r["nom"], "mois": r["mois"],
         "montant_du_conciergerie": r["montant_du_conciergerie"]}
        for r in out
    ]


def generer_ecritures_du_mois(mois: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Génère (ou no-op idempotent) l'écriture VENTES de chaque propriétaire du mois. Ne génère
    rien pour un montant nul ou absent — reflète §4 : « rien à constater » n'est pas une anomalie."""
    from app.services import comptabilite_ecritures_service as compta

    lignes = lignes_du_mois(mois)
    resultats = []
    for l in lignes:
        montant = l.get("montant_du_conciergerie")
        if montant is None or round(montant, 2) == 0:
            resultats.append({"proprietaire_id": l["proprietaire_id"], "ok": True,
                              "genere": False, "raison": "montant nul ou absent"})
            continue
        res = compta.generer_ecriture_vente(
            l["proprietaire_id"], l["mois"], montant, nom_proprietaire=l.get("nom", ""),
            acteur=acteur, db_path=db_path)
        resultats.append({"proprietaire_id": l["proprietaire_id"], "ok": res.get("ok"),
                          "genere": res.get("ok") and not res.get("deja_generee"),
                          "ecriture_id_opaque": res.get("ecriture_id_opaque"),
                          "erreur": None if res.get("ok") else res.get("message")})
    return {"mois": mois, "nb_lignes": len(lignes), "resultats": resultats,
            "source": "SOURCE_PROVISOIRE_LOT12"}

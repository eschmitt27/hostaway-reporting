"""Ventilation d'un montant non affecté (facture prestataire ménage externe) sur les logements
présents dans CETTE facture, au prorata du coût des ménages de chacun.

RÈGLE (confirmée, pas inventée ici — cf. mission Ménages §9-14)
Une ligne de facture qui désigne un frais/heures supplémentaires SANS logement est répartie sur les
AUTRES logements déjà mentionnés dans la même facture, au prorata de leur coût de ménages dans cette
facture — jamais sur l'historique complet du prestataire. Sans base exploitable (aucun logement/coût
ménage dans la facture), la ventilation n'a pas lieu : la facture reste A_CONTROLER, motif explicite.

CENTIMES DÉTERMINISTES
Même méthode que `lib_charges_menage.ventiler_charge_menage` (Lot6f) : conversion en centimes,
répartition proportionnelle au poids, le dernier logement (trié) absorbe le résidu d'arrondi. La
somme des parts est donc TOUJOURS exactement égale au montant à répartir, jamais 9,99€ / 10,01€ pour
une charge de 10,00€.

Cette fonction est un calcul, pas un enregistrement : `ventiler_et_enregistrer` fait le lien avec la
facture (persistance + statut).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

BASE_COUT_MENAGES_FACTURE = "COUT_MENAGES_FACTURE"

ST_APPLIQUEE = "APPLIQUEE"
ST_NON_EFFECTUEE = "NON_EFFECTUEE"

MOTIF_AUCUNE_BASE = "Aucun logement avec coût de ménages exploitable dans cette facture."


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ventiler(montant_non_affecte: float, cout_menages_par_logement: dict[str, float]) -> dict[str, Any]:
    """Calcule la ventilation. Fonction PURE, aucun accès base.

    `cout_menages_par_logement` : {logement_id -> coût total des ménages de ce logement DANS la
    facture}. Logements à coût nul ou négatif exclus de la base (poids nul).

    Retourne {statut, parts:[{logement_id, cout_menages, poids, part_montant}], somme, message}.
    """
    montant = round(float(montant_non_affecte or 0), 2)
    if montant == 0:
        return {"statut": ST_NON_EFFECTUEE, "parts": [], "somme": 0.0,
                "message": "Montant non affecté nul : rien à ventiler."}

    base = {str(k): float(v) for k, v in (cout_menages_par_logement or {}).items()
            if v and float(v) > 0}
    if not base:
        return {"statut": ST_NON_EFFECTUEE, "parts": [], "somme": 0.0, "message": MOTIF_AUCUNE_BASE}

    total_base = sum(base.values())
    cles = sorted(base.keys())
    total_cents = int(round(montant * 100))
    cumul = 0
    parts: list[dict[str, Any]] = []
    for i, logement_id in enumerate(cles):
        poids = base[logement_id] / total_base
        if i < len(cles) - 1:
            cents = int(round(total_cents * poids))
            cumul += cents
        else:
            cents = total_cents - cumul  # dernier logement (trié) absorbe le résidu de centimes
        parts.append({"logement_id": logement_id, "cout_menages": round(base[logement_id], 2),
                      "poids": round(poids, 6), "part_montant": round(cents / 100.0, 2)})

    somme = round(sum(p["part_montant"] for p in parts), 2)
    return {"statut": ST_APPLIQUEE, "parts": parts, "somme": somme,
            "message": f"Ventilé sur {len(parts)} logement(s) au prorata du coût des ménages."}


def ventiler_et_enregistrer(facture_id_opaque: str, montant_non_affecte: float,
                            cout_menages_par_logement: dict[str, float], *, acteur: str = "",
                            db_path=None) -> dict[str, Any]:
    """Ventile et trace la décision. N'active JAMAIS seule la facture (§31) : le statut appelant
    reste responsable — cette fonction ne fait que garantir que la ventilation, si elle a eu lieu,
    n'est pas silencieuse.
    """
    resultat = ventiler(montant_non_affecte, cout_menages_par_logement)

    ventilation_id = "VENT-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO facture_ventilations (ventilation_id_opaque, facture_id_opaque, "
            "montant_non_affecte, base_ponderation, statut, motif, acteur) VALUES (?,?,?,?,?,?,?)",
            (ventilation_id, facture_id_opaque, round(float(montant_non_affecte or 0), 2),
             BASE_COUT_MENAGES_FACTURE, resultat["statut"],
             None if resultat["statut"] == ST_APPLIQUEE else resultat["message"], acteur or None))
        for p in resultat["parts"]:
            conn.execute(
                "INSERT INTO facture_ventilation_parts (ventilation_id_opaque, logement_id, "
                "cout_menages_logement, poids, part_montant) VALUES (?,?,?,?,?)",
                (ventilation_id, p["logement_id"], p["cout_menages"], p["poids"],
                 p["part_montant"]))
        conn.commit()
    finally:
        conn.close()

    return {"ventilation_id_opaque": ventilation_id, **resultat}


def ventilations(facture_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    """Ventilations tracées d'une facture, parts incluses — pour affichage/audit (§32)."""
    conn = get_db(db_path)
    try:
        vents = conn.execute(
            "SELECT ventilation_id_opaque, montant_non_affecte, base_ponderation, statut, motif, "
            "date_creation, acteur FROM facture_ventilations WHERE facture_id_opaque = ? "
            "ORDER BY id", (facture_id_opaque,)).fetchall()
        out = []
        for v in vents:
            parts = conn.execute(
                "SELECT logement_id, cout_menages_logement, poids, part_montant "
                "FROM facture_ventilation_parts WHERE ventilation_id_opaque = ? ORDER BY logement_id",
                (v["ventilation_id_opaque"],)).fetchall()
            out.append({**dict(v), "parts": [dict(p) for p in parts]})
        return out
    finally:
        conn.close()

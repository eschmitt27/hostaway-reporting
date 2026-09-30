"""Barème kilométrique — montant INDICATIF d'une IK, pour contrôle seulement.

ALERTE, JAMAIS BLOCAGE
Ce service ne refuse rien et ne modifie rien : il calcule une référence à partir des kilomètres
du relevé et du véhicule, et dit si le montant saisi la dépasse. Création, modification,
validation, clôture et comptabilisation de l'IK n'en dépendent pas. Le montant comptable reste
celui de la charge, sous la responsabilité de l'utilisateur.

UN RÉFÉRENTIEL, PAS DES MONTANTS DANS LE CODE
Les tranches vivent dans `ref_bareme_ik` / `ref_bareme_ik_annees` (migration 0117), par année.
Mettre le barème à jour = ajouter une année. Pour une IK, on retient le barème de l'année de fin
de sa période ; à défaut, le plus récent antérieur ; à défaut, le plus récent configuré — et
l'écran dit toujours quelle année a servi.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

EPS = 0.005
TYPES_VEHICULE = {"AUTO": "Voiture", "MOTO": "Motocyclette (plus de 50 cm³)",
                  "CYCLO": "Cyclomoteur (50 cm³ et moins)"}
MOTORISATIONS = {"THERMIQUE": "Thermique / hybride", "ELECTRIQUE": "Électrique"}


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def annees(*, db_path=None) -> list[int]:
    conn = get_db(db_path)
    try:
        if "ref_bareme_ik_annees" not in _tables(conn):
            return []
        return [r[0] for r in conn.execute("SELECT annee FROM ref_bareme_ik_annees ORDER BY annee")]
    finally:
        conn.close()


def annee_applicable(annee: int, *, db_path=None) -> int | None:
    dispo = annees(db_path=db_path)
    if not dispo:
        return None
    anterieures = [a for a in dispo if a <= annee]
    return max(anterieures) if anterieures else max(dispo)


def montant_indicatif(km: float, type_vehicule: str, puissance: int | None, motorisation: str,
                      annee: int, *, db_path=None) -> dict[str, Any]:
    """{ok, montant, annee, tranche, …} ou {ok: False, manque: phrase} — jamais d'exception."""
    type_vehicule = (type_vehicule or "").upper()
    if not km or km <= 0:
        return {"ok": False, "manque": "Ajoutez les trajets : le calcul part des kilomètres du relevé."}
    if type_vehicule not in TYPES_VEHICULE:
        return {"ok": False, "manque": "Renseignez le véhicule (type, puissance fiscale, motorisation)."}
    if type_vehicule != "CYCLO" and not puissance:
        return {"ok": False, "manque": "Renseignez la puissance fiscale du véhicule."}
    retenue = annee_applicable(annee, db_path=db_path)
    if retenue is None:
        return {"ok": False, "manque": "Aucun barème kilométrique n'est configuré."}
    cv = int(puissance or 0)
    conn = get_db(db_path)
    try:
        maj = conn.execute("SELECT majoration_electrique, source FROM ref_bareme_ik_annees "
                           "WHERE annee = ?", (retenue,)).fetchone()
        tranche = conn.execute(
            "SELECT coefficient, forfait, km_min, km_max, cv_min, cv_max FROM ref_bareme_ik "
            "WHERE annee = ? AND type_vehicule = ? AND cv_min <= ? AND (cv_max IS NULL OR cv_max >= ?) "
            "AND km_min < ? AND (km_max IS NULL OR km_max >= ?) ORDER BY km_min DESC LIMIT 1",
            (retenue, type_vehicule, cv, cv, km, km)).fetchone()
    finally:
        conn.close()
    if tranche is None:
        return {"ok": False, "manque": "Aucune tranche du barème ne correspond à ce véhicule."}
    base = km * float(tranche["coefficient"]) + float(tranche["forfait"] or 0)
    electrique = (motorisation or "").upper() == "ELECTRIQUE"
    majoration = float(maj["majoration_electrique"] or 0) if electrique else 0.0
    montant = round(base * (1 + majoration), 2)
    formule = f"{km:g} km × {tranche['coefficient']:g}"
    if tranche["forfait"]:
        formule += f" + {tranche['forfait']:g} €"
    if majoration:
        formule = f"({formule}) × {1 + majoration:g} (électrique)"
    return {"ok": True, "montant": montant, "annee": retenue, "annee_demandee": annee,
            "formule": formule, "source": maj["source"] if maj else ""}


def controle(ik: dict[str, Any], *, db_path=None) -> dict[str, Any]:
    """Compare le montant de l'IK (celui de la charge) au montant indicatif. Lecture seule."""
    annee = int(str(ik.get("date_fin") or ik.get("date_debut") or "0")[:4] or 0)
    calc = montant_indicatif(float(ik.get("km_total") or 0), ik.get("type_vehicule") or "",
                             ik.get("puissance_fiscale"), ik.get("motorisation") or "",
                             annee, db_path=db_path)
    saisi = round(float(ik.get("montant") or 0), 2)
    out = {"montant_saisi": saisi, **calc}
    out["depassement"] = bool(calc.get("ok") and saisi > calc["montant"] + EPS)
    out["ecart"] = round(saisi - calc["montant"], 2) if calc.get("ok") else None
    return out

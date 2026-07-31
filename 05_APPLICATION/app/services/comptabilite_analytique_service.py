"""Moteur analytique — couche de LECTURE et d'explication (Phase 2, mission Analytique/Résultats).

Ne recalcule JAMAIS différemment Lot10 : les mesures REEL/COMPTABLE/HORS_COMPTA viennent telles
quelles de `MASTER_CALC_Resultats.xlsx` (`PAR_MOIS_LOGEMENT`/`PAR_MOIS_PROPRIETAIRE`/`GLOBAL`),
lu en lecture seule via `proprietaires_reglements_reader`. Lot10 reste l'unique moteur de calcul
métier ; la Comptabilité (SQLite) reste l'unique traduction en partie double ; ce service ne fait
qu'agréger et croiser les deux, jamais un troisième calcul indépendant.

Aucune donnée n'est dupliquée en base : ce service lit à la demande, jamais un job de synchronisation.
"""
from __future__ import annotations

from typing import Any

from app.readers import proprietaires_reglements_reader as reader

VISIONS = ("REEL", "COMPTABLE", "HORS_COMPTA")

NON_DISPONIBLE = "NON_DISPONIBLE"
NON_APPLICABLE = "NON_APPLICABLE"


def _num(v: Any) -> float:
    return reader.to_nombre(v) or 0.0


def source_disponible() -> bool:
    return reader.resultats_par_logement().etat.disponible


def mois_disponibles() -> list[str]:
    """Liste triée des mois présents dans `PAR_MOIS_LOGEMENT` — sert à choisir un mois par défaut
    et à construire les comparaisons (mois précédent, cumul annuel)."""
    src = reader.resultats_par_logement()
    if not src.etat.disponible:
        return []
    return sorted({reader.to_mois(r.get("mois")) for r in src.lignes if r.get("mois")})


def mesures_cumulees(*, vision: str = "REEL", annee: str = "") -> dict[str, Any]:
    """Somme de toutes les lignes `PAR_MOIS_LOGEMENT` disponibles pour cette vision — jamais
    présentée comme le cumul annuel officiel s'il manque des mois : `nb_mois_couverts` est
    toujours renvoyé pour que l'écran distingue un cumul complet d'un cumul partiel."""
    src = reader.resultats_par_logement()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "total_produits": None, "total_charges": None,
                "resultat": None, "nb_mois_couverts": 0, "mois_couverts": []}
    lignes = [r for r in src.lignes if reader.to_texte(r.get("vision")) == vision
             and (not annee or reader.to_mois(r.get("mois")).startswith(annee))]
    mois_couverts = sorted({reader.to_mois(r.get("mois")) for r in lignes})
    return {
        "statut": "OK" if lignes else NON_DISPONIBLE,
        "total_produits": round(sum(_num(r.get("total_produits")) for r in lignes), 2),
        "total_charges": round(sum(_num(r.get("total_charges")) for r in lignes), 2),
        "resultat": round(sum(_num(r.get("resultat")) for r in lignes), 2),
        "nb_mois_couverts": len(mois_couverts), "mois_couverts": mois_couverts,
    }


def mois_precedent(mois: str) -> str:
    if not mois or len(mois) != 7:
        return ""
    annee, m = int(mois[:4]), int(mois[5:7])
    return f"{annee-1}-12" if m == 1 else f"{annee}-{m-1:02d}"


def mesures_globales() -> dict[str, Any]:
    """Une entrée par vision, telle que Lot10 l'a déjà calculée et contrôlée (`commentaire_hc`
    porte déjà la vérification REEL=COMPTABLE+HC, jamais refaite ici)."""
    src = reader.resultats_global()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "visions": {}}
    out: dict[str, Any] = {}
    for r in src.lignes:
        vision = reader.to_texte(r.get("vision"))
        out[vision] = {
            "total_produits": _num(r.get("total_produits")),
            "total_charges": _num(r.get("total_charges")),
            "resultat": _num(r.get("resultat")),
            "commentaire": reader.to_texte(r.get("commentaire_hc")),
        }
    return {"statut": "OK", "visions": out}


def _filtrer(lignes: list[dict[str, Any]], *, mois: str = "", vision: str = "",
            cle_dimension: str = "", valeur_dimension: str = "") -> list[dict[str, Any]]:
    out = []
    for r in lignes:
        if mois and reader.to_mois(r.get("mois")) != mois:
            continue
        if vision and reader.to_texte(r.get("vision")) != vision:
            continue
        if cle_dimension and reader.to_texte(r.get(cle_dimension)) != valeur_dimension:
            continue
        out.append(r)
    return out


def mesures_par_logement(*, mois: str = "", vision: str = "REEL") -> dict[str, Any]:
    """Une ligne par (mois, logement, vision) — grain déjà fourni par Lot10, jamais recomposé."""
    src = reader.resultats_par_logement()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "lignes": []}
    lignes = _filtrer(src.lignes, mois=mois, vision=vision)
    out = [{
        "mois": reader.to_mois(r.get("mois")), "logement_id": reader.to_texte(r.get("logement_id")),
        "proprietaire_id": reader.to_texte(r.get("proprietaire_id")),
        "vision": reader.to_texte(r.get("vision")),
        "total_produits": _num(r.get("total_produits")), "total_charges": _num(r.get("total_charges")),
        "resultat": _num(r.get("resultat")), "nb_flux": int(_num(r.get("nb_flux"))),
    } for r in lignes]
    return {"statut": "OK", "lignes": out}


def mesures_par_proprietaire(*, mois: str = "", vision: str = "REEL") -> dict[str, Any]:
    src = reader.resultats()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "lignes": []}
    lignes = _filtrer(src.lignes, mois=mois, vision=vision)
    out = [{
        "mois": reader.to_mois(r.get("mois")), "proprietaire_id": reader.to_texte(r.get("proprietaire_id")),
        "vision": reader.to_texte(r.get("vision")),
        "total_produits": _num(r.get("total_produits")), "total_charges": _num(r.get("total_charges")),
        "resultat": _num(r.get("resultat")), "nb_flux": int(_num(r.get("nb_flux"))),
    } for r in lignes]
    return {"statut": "OK", "lignes": out}


def fiche_logement(logement_id: str, *, mois: str = "") -> dict[str, Any]:
    """Une entrée par vision pour CE logement — base du drill-down `/resultats/logements/{id}`."""
    src = reader.resultats_par_logement()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "visions": {}}
    lignes = _filtrer(src.lignes, mois=mois, cle_dimension="logement_id", valeur_dimension=logement_id)
    visions: dict[str, Any] = {}
    for r in lignes:
        v = reader.to_texte(r.get("vision"))
        visions.setdefault(v, {"total_produits": 0.0, "total_charges": 0.0, "resultat": 0.0, "nb_flux": 0})
        visions[v]["total_produits"] += _num(r.get("total_produits"))
        visions[v]["total_charges"] += _num(r.get("total_charges"))
        visions[v]["resultat"] += _num(r.get("resultat"))
        visions[v]["nb_flux"] += int(_num(r.get("nb_flux")))
    return {"statut": "OK" if lignes else NON_DISPONIBLE, "visions": visions}


def fiche_proprietaire(proprietaire_id: str, *, mois: str = "") -> dict[str, Any]:
    src = reader.resultats()
    if not src.etat.disponible:
        return {"statut": NON_DISPONIBLE, "visions": {}}
    lignes = _filtrer(src.lignes, mois=mois, cle_dimension="proprietaire_id", valeur_dimension=proprietaire_id)
    visions: dict[str, Any] = {}
    for r in lignes:
        v = reader.to_texte(r.get("vision"))
        visions.setdefault(v, {"total_produits": 0.0, "total_charges": 0.0, "resultat": 0.0, "nb_flux": 0})
        visions[v]["total_produits"] += _num(r.get("total_produits"))
        visions[v]["total_charges"] += _num(r.get("total_charges"))
        visions[v]["resultat"] += _num(r.get("resultat"))
        visions[v]["nb_flux"] += int(_num(r.get("nb_flux")))
    return {"statut": "OK" if lignes else NON_DISPONIBLE, "visions": visions}


def drill_down_logement(logement_id: str, *, mois: str = "", db_path=None) -> list[dict[str, Any]]:
    """résultat → ligne analytique → écriture : les écritures ACHATS/VENTES dont la ventilation
    porte ce logement (`ecriture_ligne_ventilation` + `ecriture_lignes`, Phase 1), explicables
    jusqu'à la pièce via la fiche écriture existante."""
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT e.ecriture_id_opaque, e.journal, e.date_ecriture, e.statut, e.periode, "
            "l.debit, l.credit, l.compte "
            "FROM ecriture_lignes l JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE l.logement_id=? " + ("AND e.periode=? " if mois else "") +
            "ORDER BY e.date_ecriture DESC",
            (logement_id, mois) if mois else (logement_id,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]

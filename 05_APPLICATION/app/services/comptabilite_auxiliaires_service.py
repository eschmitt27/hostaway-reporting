"""Auxiliaires Comptabilité — fournisseurs, propriétaires, associés (migration `0021`/`0023`).

Le solde par auxiliaire (`comptabilite_ecritures_service.solde_auxiliaire`) est déjà générique —
aucune table de correspondance n'est nécessaire, l'auxiliaire EST l'identifiant métier
(`fournisseur_id_opaque` / `proprietaire_id` / `associe_id`). Ce service n'ajoute que la vue
consolidée par famille : solde + éléments ouverts, jamais un second calcul du solde.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

FAMILLE_FOURNISSEUR = "FOURNISSEUR"
FAMILLE_PROPRIETAIRE = "PROPRIETAIRE"
FAMILLE_ASSOCIE = "ASSOCIE"
FAMILLES = (FAMILLE_FOURNISSEUR, FAMILLE_PROPRIETAIRE, FAMILLE_ASSOCIE)

COMPTE_PAR_FAMILLE = {
    FAMILLE_FOURNISSEUR: "401000",
    FAMILLE_PROPRIETAIRE: "411000",
    FAMILLE_ASSOCIE: "467000",
}


def _fournisseurs_avec_mouvement(db_path=None) -> list[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT auxiliaire FROM ecriture_lignes WHERE compte='401000' "
            "AND auxiliaire IS NOT NULL").fetchall()
    finally:
        conn.close()
    return [r["auxiliaire"] for r in rows]


def _proprietaires_avec_mouvement(db_path=None) -> list[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT auxiliaire FROM ecriture_lignes WHERE compte='411000' "
            "AND auxiliaire IS NOT NULL").fetchall()
    finally:
        conn.close()
    return [r["auxiliaire"] for r in rows]


def _associes_avec_mouvement(db_path=None) -> list[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT auxiliaire FROM ecriture_lignes WHERE compte='467000' "
            "AND auxiliaire IS NOT NULL").fetchall()
    finally:
        conn.close()
    return [r["auxiliaire"] for r in rows]


def lister_auxiliaires(famille: str, db_path=None) -> list[str]:
    if famille == FAMILLE_FOURNISSEUR:
        return sorted(_fournisseurs_avec_mouvement(db_path))
    if famille == FAMILLE_PROPRIETAIRE:
        return sorted(_proprietaires_avec_mouvement(db_path))
    if famille == FAMILLE_ASSOCIE:
        return sorted(_associes_avec_mouvement(db_path))
    return []


def fiche_auxiliaire(famille: str, auxiliaire: str, db_path=None) -> dict[str, Any]:
    """Solde + mouvements + éléments ouverts pour un auxiliaire d'une famille donnée.

    « Éléments ouverts » : pour un fournisseur, ses factures au statut ouvert (cf.
    `factures_service.STATUTS_OUVERTS`) ; pour un propriétaire/associé, les écritures VENTES/CAISSE/
    ODIVERSES encore `PROPOSEE` (pas encore validées) qui le concernent.
    """
    from app.services import comptabilite_ecritures_service as compta
    solde = compta.solde_auxiliaire(auxiliaire, db_path=db_path)

    conn = get_db(db_path)
    try:
        mouvements = conn.execute(
            "SELECT e.ecriture_id_opaque, e.journal, e.date_ecriture, e.statut, "
            "l.debit, l.credit, l.libelle "
            "FROM ecriture_lignes l JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE l.auxiliaire=? ORDER BY e.date_ecriture DESC, e.id DESC",
            (auxiliaire,)).fetchall()
    finally:
        conn.close()

    elements_ouverts: list[dict[str, Any]] = []
    if famille == FAMILLE_FOURNISSEUR:
        try:
            from app.services import factures_service as fact
            elements_ouverts = [
                {"type": "FACTURE", "identifiant": f["facture_ref"], "montant": f["solde_restant"],
                 "statut": f["statut"]}
                for f in fact.lister(fournisseur=auxiliaire, db_path=db_path)
                if f["statut"] in fact.STATUTS_OUVERTS and f["solde_restant"] > 0.005
            ]
        except Exception:
            elements_ouverts = []
    else:
        elements_ouverts = [
            {"type": m["journal"], "identifiant": m["ecriture_id_opaque"],
             "montant": m["debit"] or m["credit"], "statut": m["statut"]}
            for m in mouvements if m["statut"] == "PROPOSEE"
        ]

    return {
        "famille": famille, "auxiliaire": auxiliaire, "compte": COMPTE_PAR_FAMILLE.get(famille),
        "solde": solde, "mouvements": [dict(m) for m in mouvements],
        "elements_ouverts": elements_ouverts,
    }


def synthese(db_path=None) -> dict[str, Any]:
    """Une ligne par auxiliaire ayant au moins un mouvement, groupée par famille."""
    from app.services import comptabilite_ecritures_service as compta
    out: dict[str, list[dict[str, Any]]] = {}
    for famille in FAMILLES:
        lignes = []
        for aux in lister_auxiliaires(famille, db_path):
            solde = compta.solde_auxiliaire(aux, db_path=db_path)
            lignes.append({"auxiliaire": aux, **solde})
        out[famille] = lignes
    return out

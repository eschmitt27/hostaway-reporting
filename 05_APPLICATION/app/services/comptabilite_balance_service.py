"""Balance générale : totaux débit / crédit et solde par compte.

Lecture pure des écritures existantes, agrégées par compte. Ne génère, ne modifie et ne valide
aucune écriture — la balance est une vue, pas une source.

Une écriture contrepassée est exclue : elle a été annulée, la faire figurer fausserait les totaux
tout en restant consultable dans le journal, qui est le bon endroit pour l'historique.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as ecr

# Convention : un compte de classe 1 à 5 est un compte de bilan, 6 et 7 des comptes de gestion.
# Sert uniquement à regrouper l'affichage, jamais à modifier un montant.
CLASSES = {
    "1": "Capitaux", "2": "Immobilisations", "3": "Stocks", "4": "Tiers",
    "5": "Financier", "6": "Charges", "7": "Produits", "8": "Spéciaux",
}


def _classe(compte: str) -> str:
    return CLASSES.get(str(compte)[:1], "Autres")


def balance(*, periode_debut: str = "", periode_fin: str = "", journal: str = "",
            inclure_soldes_nuls: bool = False, db_path=None) -> dict[str, Any]:
    """Balance par compte sur une période (bornes incluses, sur `periode` au format AAAA-MM)."""
    conditions = ["e.statut <> ?"]
    params: list[Any] = [ecr.ST_CONTREPASSEE]
    if periode_debut:
        conditions.append("e.periode >= ?")
        params.append(periode_debut)
    if periode_fin:
        conditions.append("e.periode <= ?")
        params.append(periode_fin)
    if journal:
        conditions.append("e.journal = ?")
        params.append(journal)

    conn = get_db(db_path)
    try:
        lignes = conn.execute(
            "SELECT l.compte, COALESCE(SUM(l.debit), 0) AS debit, "
            "       COALESCE(SUM(l.credit), 0) AS credit, COUNT(*) AS nb_lignes "
            "FROM ecriture_lignes l JOIN ecritures e "
            "  ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            f"WHERE {' AND '.join(conditions)} "
            "GROUP BY l.compte ORDER BY l.compte", params).fetchall()
        libelles = {r["compte"]: r["libelle"]
                    for r in conn.execute("SELECT compte, libelle FROM plan_comptable").fetchall()}
    finally:
        conn.close()

    comptes = []
    for r in lignes:
        debit, credit = round(r["debit"], 2), round(r["credit"], 2)
        solde = round(debit - credit, 2)
        if not inclure_soldes_nuls and debit == 0 and credit == 0:
            continue
        comptes.append({
            "compte": r["compte"],
            "libelle": libelles.get(r["compte"], ""),
            "classe": _classe(r["compte"]),
            "debit": debit,
            "credit": credit,
            "solde": solde,
            # Un solde débiteur est présenté à gauche, créditeur à droite : c'est la lecture
            # attendue d'une balance, et elle évite d'afficher des montants négatifs.
            "solde_debiteur": solde if solde > 0 else 0.0,
            "solde_crediteur": -solde if solde < 0 else 0.0,
            "nb_lignes": r["nb_lignes"],
        })

    total_debit = round(sum(c["debit"] for c in comptes), 2)
    total_credit = round(sum(c["credit"] for c in comptes), 2)

    return {
        "comptes": comptes,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "total_solde_debiteur": round(sum(c["solde_debiteur"] for c in comptes), 2),
        "total_solde_crediteur": round(sum(c["solde_crediteur"] for c in comptes), 2),
        # Une balance déséquilibrée signale une écriture anormale : on l'expose plutôt que de la
        # masquer par un arrondi.
        "equilibree": abs(total_debit - total_credit) < 0.005,
        "ecart": round(total_debit - total_credit, 2),
        "filtres": {"periode_debut": periode_debut, "periode_fin": periode_fin,
                    "journal": journal},
    }


def periodes_disponibles(*, db_path=None) -> list[str]:
    conn = get_db(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT periode FROM ecritures WHERE statut <> ? ORDER BY periode DESC",
            (ecr.ST_CONTREPASSEE,)).fetchall() if r[0]]
    finally:
        conn.close()

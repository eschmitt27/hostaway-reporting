"""Périmètre analytique d'une charge : les N logements qu'elle concerne (migration 0074).

DEUX AXES, JAMAIS CONFONDUS
    ANALYTIQUE     — une charge de 700 € affectée à 2 logements pèse 350 € sur le résultat de
                     chacun. C'est ce que porte ce module.
    REFACTURATION  — la même charge crée UN élément à refacturer de 700 € au total, que
                     l'utilisateur récupère comme il veut (700/0, 350/350, 500/200…). C'est
                     `charges_refacturation_service` qui le porte.

La ventilation analytique NE DOIT PAS imposer la ventilation commerciale : ce sont deux questions
différentes (« combien cette dépense coûte-t-elle à ce logement ? » vs « combien je refacture, et
sur quelle facture ? »).

FIGÉ À LA CRÉATION
`proprietaire_id` est résolu par la gestion active du MOIS DE LA CHARGE et stocké tel quel. Le
réinférer à la lecture ferait glisser une charge d'août vers un propriétaire entré en septembre.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db


def _round(v: Any) -> float:
    return round(float(v or 0), 2)


def enregistrer(charge_id: str, entrees: list[dict[str, Any]], *, mois: str = "",
                acteur: str = "", conn=None, db_path=None) -> int:
    """Écrit le périmètre d'une charge. Remplace intégralement le périmètre existant.

    `entrees` vient du calcul déjà fait par `charges_impact_service` : chaque entrée porte
    `logement_id`, `proprietaire_id` et la quote-part analytique. Rien n'est recalculé ici — ce
    module persiste, il n'arbitre pas.

    Si `conn` est fourni, l'appelant gère la transaction (atomicité avec l'écriture de la charge).
    """
    charge_id = str(charge_id or "").strip()
    if not charge_id:
        return 0
    locale = conn is None
    if locale:
        conn = get_db(db_path)
    try:
        conn.execute("DELETE FROM charges_perimetre_analytique WHERE charge_id = ?", (charge_id,))
        n = 0
        for e in entrees:
            logement_id = str(e.get("logement_id") or "").strip()
            if not logement_id:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO charges_perimetre_analytique "
                "(charge_id, logement_id, proprietaire_id, mois, quote_part_montant, acteur) "
                "VALUES (?,?,?,?,?,?)",
                (charge_id, logement_id, str(e.get("proprietaire_id") or "").strip() or None,
                 str(e.get("mois") or mois or "").strip() or None,
                 _round(e.get("quote_part_montant", e.get("montant_refacturable"))),
                 acteur or None))
            n += 1
        if locale:
            conn.commit()
        return n
    except Exception:
        if locale:
            conn.rollback()
        raise
    finally:
        if locale:
            conn.close()


def enregistrer_menage(charge_id: str, mode: str, entrees: list[dict[str, Any]], *,
                       mois: str = "", acteur: str = "", conn=None, db_path=None) -> int:
    """Écrit le périmètre MÉNAGE d'une charge (migration 0076).

    Le parcours ménage ventile soit sur des INTERVENANTS, soit sur des LOGEMENTS — jamais les deux.
    Comme pour le périmètre analytique, ce calcul était fait à la prévisualisation puis jeté ;
    `lot6f_cout_complet_menages` en a besoin pour constituer ses pools de coût.
    """
    charge_id = str(charge_id or "").strip()
    mode = str(mode or "").strip().upper()
    if not charge_id or mode not in ("INTERVENANT", "LOGEMENT"):
        return 0
    locale = conn is None
    if locale:
        conn = get_db(db_path)
    try:
        conn.execute("DELETE FROM charges_perimetre_menage WHERE charge_id = ?", (charge_id,))
        n = 0
        for e in entrees:
            intervenant = str(e.get("intervenant_id") or "").strip() or None
            logement = str(e.get("logement_id") or "").strip() or None
            # La contrainte SQL exige exactement une dimension : on écarte ici ce qui n'en porte
            # aucune plutôt que de laisser la base refuser une transaction entière.
            if mode == "INTERVENANT":
                logement = None
                if not intervenant:
                    continue
            else:
                intervenant = None
                if not logement:
                    continue
            conn.execute(
                "INSERT OR REPLACE INTO charges_perimetre_menage "
                "(charge_id, mode, intervenant_id, logement_id, proprietaire_id, mois, "
                " quote_part_montant, acteur) VALUES (?,?,?,?,?,?,?,?)",
                (charge_id, mode, intervenant, logement,
                 str(e.get("proprietaire_id") or "").strip() or None,
                 str(e.get("mois") or mois or "").strip() or None,
                 _round(e.get("quote_part_montant")), acteur or None))
            n += 1
        if locale:
            conn.commit()
        return n
    except Exception:
        if locale:
            conn.rollback()
        raise
    finally:
        if locale:
            conn.close()


def lire_menage(charge_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Périmètre ménage d'une charge. Liste vide si la charge n'emprunte pas ce parcours."""
    charge_id = str(charge_id or "").strip()
    if not charge_id:
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM charges_perimetre_menage WHERE charge_id = ? ORDER BY id",
            (charge_id,))]
    except Exception:      # noqa: BLE001 — base antérieure à 0076
        return []
    finally:
        conn.close()


def lire(charge_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Le périmètre d'une charge, ordonné. Liste vide si la charge n'en a pas (charge globale)."""
    charge_id = str(charge_id or "").strip()
    if not charge_id:
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM charges_perimetre_analytique WHERE charge_id = ? "
            "ORDER BY logement_id", (charge_id,))]
    except Exception:      # noqa: BLE001 — base antérieure à 0074 : pas de périmètre, pas d'erreur
        return []
    finally:
        conn.close()


def logements(charge_id: str, *, db_path=None) -> list[str]:
    return [r["logement_id"] for r in lire(charge_id, db_path=db_path)]


def proprietaires(charge_id: str, *, db_path=None) -> list[str]:
    """Propriétaires distincts du périmètre, dans l'ordre de première apparition."""
    vus: list[str] = []
    for r in lire(charge_id, db_path=db_path):
        pid = str(r.get("proprietaire_id") or "").strip()
        if pid and pid not in vus:
            vus.append(pid)
    return vus


def resume(charge_id: str, montant_charge: Any = None, *, db_path=None) -> dict[str, Any]:
    """Vue d'affichage : combien de logements, quelle part analytique chacun.

    `quote_part_theorique` est recalculée pour l'affichage uniquement (montant / N) afin que
    l'écran puisse expliquer la règle ; les parts réellement stockées restent la référence.
    """
    lignes = lire(charge_id, db_path=db_path)
    n = len(lignes)
    total = _round(sum(_round(l.get("quote_part_montant")) for l in lignes))
    theorique = _round(_round(montant_charge) / n) if (n and montant_charge is not None) else None
    return {
        "nb_logements": n,
        "logements": [l["logement_id"] for l in lignes],
        "lignes": lignes,
        "total_reparti": total,
        "quote_part_theorique": theorique,
        "multi": n > 1,
    }

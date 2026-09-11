"""Origine des ménages attendus : Hostaway ou hors Hostaway (recette utilisateur n°2, §38-§39).

DEUX AXES, JAMAIS MÉLANGÉS
    ORIGINE — d'où vient le ménage attendu ?
              · ATTENDU HOSTAWAY      : la Cleaning Task Hostaway EST le ménage attendu ;
              · ATTENDU HORS HOSTAWAY : l'application le déduit d'une réservation saisie à la main.
    ÉTAT    — où en est son exécution ? (prévu, réalisé, annulé…)

L'ancienne présentation opposait « Ménage attendu » à « Hostaway réalisé » comme deux sources
CONCURRENTES, la première restant vide en permanence. Elle laissait croire à un écart alors que
les deux colonnes ne répondent pas à la même question : l'une désigne une origine, l'autre un
état d'avancement. Une tâche Hostaway réalisée n'est pas « un autre ménage » que le ménage
attendu — c'est LE MÊME, plus tard.

CE QUE CE MODULE NE FAIT PAS : créer un second ménage pour une réservation qui en a déjà un. Les
deux origines sont disjointes par construction — une réservation est sur Hostaway ou elle ne l'est
pas.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

ORIGINE_HOSTAWAY = "HOSTAWAY"
ORIGINE_HORS_HOSTAWAY = "HORS_HOSTAWAY"

# États d'exécution tels que Hostaway les renvoie, traduits sans être réinterprétés. Un statut
# inconnu est conservé tel quel plutôt que rangé d'office dans « autre » : mieux vaut afficher un
# état qu'on ne sait pas nommer que le faire disparaître dans une catégorie fourre-tout.
ETATS_HOSTAWAY = {
    "completed": "Réalisé",
    "confirmed": "Prévu",
    "pending": "En attente",
    "inProgress": "En cours",
    "cancelled": "Annulé",
}
# Seuls ces états comptent un ménage réellement ATTENDU : un ménage annulé n'est plus attendu.
ETATS_ATTENDUS = ("completed", "confirmed", "pending", "inProgress")


def _mois_de(valeur: Any) -> str:
    """`2026-08-14T09:00:00` → `2026-08`. Chaîne vide si la date est illisible."""
    texte = str(valeur or "").strip()
    return texte[:7] if len(texte) >= 7 and texte[4] == "-" else ""


def etat_libelle(status: str) -> str:
    return ETATS_HOSTAWAY.get(str(status or ""), str(status or "").strip() or "Inconnu")


def origines(mois: str = "", logement_id: str = "", *, db_path=None) -> dict[str, Any]:
    """Ménages attendus du périmètre, ventilés par ORIGINE puis par ÉTAT d'exécution.

    `logement_id` filtre via `listing_map_id` côté Hostaway : c'est la clé que porte la tâche.
    Sans référentiel de correspondance disponible, le filtre logement ne s'applique qu'au hors
    Hostaway — on préfère un total honnête à un filtrage approximatif.
    """
    conn = get_db(db_path)
    try:
        taches = [dict(r) for r in conn.execute(
            "SELECT task_id, status, can_start_from, listing_map_id, reservation_id "
            "FROM hostaway_cleaning_tasks")]
        clauses, args = [], []
        if mois:
            clauses.append("mois = ?")
            args.append(mois)
        if logement_id:
            clauses.append("logement_id = ?")
            args.append(logement_id)
        ou = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        hh = [dict(r) for r in conn.execute(
            "SELECT reservation_hh_id, mois, logement_id, date_depart, statut "
            f"FROM reservations_hors_hostaway{ou}", args)]
    except Exception:      # noqa: BLE001 — sources absentes : périmètre vide, jamais une erreur
        return _vide(mois, logement_id)
    finally:
        conn.close()

    if mois:
        taches = [t for t in taches if _mois_de(t.get("can_start_from")) == mois]

    par_etat: dict[str, int] = {}
    for t in taches:
        par_etat[str(t.get("status") or "")] = par_etat.get(str(t.get("status") or ""), 0) + 1
    attendus_hostaway = sum(n for s, n in par_etat.items() if s in ETATS_ATTENDUS)
    # Une réservation hors Hostaway annulée n'appelle plus de ménage.
    hh_actives = [r for r in hh if str(r.get("statut") or "ACTIVE").upper() != "ANNULEE"]

    return {
        "mois": mois,
        "logement_id": logement_id,
        "hostaway": {
            "origine": ORIGINE_HOSTAWAY,
            "libelle": "Attendu Hostaway",
            "source": "Tâches de ménage Hostaway",
            "attendus": attendus_hostaway,
            "total_taches": len(taches),
            "annules": par_etat.get("cancelled", 0),
            "etats": [{"code": s, "libelle": etat_libelle(s), "nb": n}
                      for s, n in sorted(par_etat.items(), key=lambda x: -x[1])],
        },
        "hors_hostaway": {
            "origine": ORIGINE_HORS_HOSTAWAY,
            "libelle": "Attendu hors Hostaway",
            "source": "Réservations saisies hors Hostaway",
            "attendus": len(hh_actives),
            "total_reservations": len(hh),
            "annulees": len(hh) - len(hh_actives),
            # Pas d'état d'exécution : ces ménages n'ont pas de tâche Hostaway pour en porter un.
            # Le dire explicitement vaut mieux qu'afficher « 0 réalisé », qui se lirait comme un
            # manquement alors que l'information n'existe simplement pas.
            "etats": [],
            "etat_indisponible": True,
        },
        "total_attendus": attendus_hostaway + len(hh_actives),
    }


def _vide(mois: str, logement_id: str) -> dict[str, Any]:
    return {
        "mois": mois, "logement_id": logement_id,
        "hostaway": {"origine": ORIGINE_HOSTAWAY, "libelle": "Attendu Hostaway",
                     "source": "Tâches de ménage Hostaway", "attendus": 0, "total_taches": 0,
                     "annules": 0, "etats": []},
        "hors_hostaway": {"origine": ORIGINE_HORS_HOSTAWAY, "libelle": "Attendu hors Hostaway",
                          "source": "Réservations saisies hors Hostaway", "attendus": 0,
                          "total_reservations": 0, "annulees": 0, "etats": [],
                          "etat_indisponible": True},
        "total_attendus": 0,
    }

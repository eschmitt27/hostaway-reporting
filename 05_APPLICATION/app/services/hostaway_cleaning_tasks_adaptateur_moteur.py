"""Classeur CleaningTasks jetable pour la recette Ménages (`menages_chaine_service`).

Même statut que `banque_adaptateur_moteur`/`reservations_adaptateur_moteur` (cf.
`adaptateur_workspace`) : le classeur `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` que
`stub_lot6a_hostaway.py`/lot6c/lot6d attendent dans le workspace n'est plus une copie du MASTER
permanent du projet — il est FABRIQUÉ ici depuis `menages_taches_enrichies` (0038, déjà le résultat
de l'enrichissement Lot6a, pas la couche RAW : reproduire ici la résolution logement/mois dupliquerait
une règle métier qui vit dans `lot6a_cleaning_tasks_comptage.py`).

Deux onglets seulement, ceux réellement lus en aval (vérifié par grep — `data` n'est consommé par
aucun script moteur de cette chaîne) :
  MASTER_ENRICHI — lue par lot6d en mode Excel (aucune base désignée dans son workspace).
  VUE_COMPTAGE   — lue par lot6c pour son onglet VUE_ECART_HOSTAWAY.

L'agrégation de VUE_COMPTAGE reprend la même règle que `menages_reader.hostaway_comptage()` (déjà
un port applicatif de `lot6a_cleaning_tasks_comptage.build_comptage_rows` — pas une seconde version
divergente, la même logique appliquée deux fois pour deux besoins différents : afficher, ou fabriquer
un classeur jetable).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from app.db.connection import get_db
from app.services.adaptateur_workspace import ecrire_classeur, refus

E_AUCUNE_DONNEE = "HOSTAWAY_CLEANING_TASKS_AUCUNE_DONNEE"

COLS_ENRICHI = (
    "task_id", "ROW_HASH", "mois", "logement_id", "proprietaire_id", "listingMapId",
    "reservation_id", "scheduled_date", "title", "status", "statut_menage",
    "type_ligne_menage_id", "type_ligne_menage_lib", "compte_comme_menage", "cost", "h6_note",
    "statut_controle", "niveau_anomalie", "code_anomalie", "extrait_le", "date_integration",
)
_ALIAS = {"row_hash": "ROW_HASH", "listing_map_id": "listingMapId"}

COLS_COMPTAGE = (
    "mois", "logement_id", "proprietaire_id", "nb_menages_realises", "nb_menages_confirmes",
    "nb_menages_pending", "nb_menages_annules", "nb_taches_total", "statut_controle",
    "niveau_anomalie", "code_anomalie",
)


def _lignes_enrichies(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='menages_taches_enrichies'"
        ).fetchone():
            return []
        rows = conn.execute("SELECT * FROM menages_taches_enrichies ORDER BY id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _comptage_depuis_enrichi(lignes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Même règle que `menages_reader.hostaway_comptage()` : compte_comme_menage=OUI pour
    réalisé/prévu/pending, les annulés comptent quel que soit compte_comme_menage."""

    def _txt(v):
        return "" if v is None else str(v).strip()

    def _cnt(groupe, statut_val, ccm_filter=True):
        return sum(1 for r in groupe if _txt(r.get("statut_menage")) == statut_val
                  and (not ccm_filter or _txt(r.get("compte_comme_menage")) == "OUI"))

    groupes: dict[tuple, list] = {}
    for r in lignes:
        cle = (_txt(r.get("mois")), _txt(r.get("logement_id")))
        groupes.setdefault(cle, []).append(r)

    out = []
    for (mois, logement_id), grp in sorted(groupes.items()):
        prop_id = next((r.get("proprietaire_id") for r in grp if r.get("proprietaire_id")), None)
        out.append({
            "mois": mois, "logement_id": logement_id, "proprietaire_id": prop_id,
            "nb_menages_realises": _cnt(grp, "réalisé"),
            "nb_menages_confirmes": _cnt(grp, "prévu"),
            "nb_menages_pending": _cnt(grp, "A_CONTROLER"),
            "nb_menages_annules": _cnt(grp, "annulé", ccm_filter=False),
            "nb_taches_total": len(grp),
            "statut_controle": "BLOQUANT" if not logement_id else "OK",
            "niveau_anomalie": "BLOQUANT" if not logement_id else "",
            "code_anomalie": "COMPTAGE_LOGEMENT_ABSENT" if not logement_id else "",
        })
    return out


def _valeur(ligne: dict[str, Any], colonne: str) -> Any:
    if colonne in ligne:
        return ligne[colonne]
    inverse = {v: k for k, v in _ALIAS.items()}
    return ligne.get(inverse.get(colonne, colonne))


def ecrire_classeur_moteur(dest_path: Path, *, db_path=None) -> dict[str, Any]:
    """Fabrique le classeur CleaningTasks jetable depuis SQLite. Refuse plutôt que produire un
    classeur vide : un classeur vide ferait conclure « aucune tâche » à lot6c/lot6d, silencieusement."""
    lignes = _lignes_enrichies(db_path=db_path)
    if not lignes:
        return refus(E_AUCUNE_DONNEE,
                    "menages_taches_enrichies vide ou absente : aucune tâche CleaningTask à fournir.")

    enrichi_rows = [{c: _valeur(l, c) for c in COLS_ENRICHI} for l in lignes]
    comptage_rows = _comptage_depuis_enrichi(lignes)

    resultat = ecrire_classeur(dest_path, [
        ("MASTER_ENRICHI", COLS_ENRICHI, enrichi_rows),
        ("VUE_COMPTAGE", COLS_COMPTAGE, comptage_rows),
    ])
    return resultat

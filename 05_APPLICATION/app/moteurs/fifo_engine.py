"""Moteur FIFO pur — allocation chronologique de sources financières sur des créances triées.

Extrait de `compte_proprietaire_service.py` (Mission 5, simplification architecture) : c'était déjà
une fonction pure, déjà réutilisée telle quelle par `intervenant_menage_compte_service.py` — un
domaine métier distinct (dette d'intervenant ménage, pas facture propriétaire). Ce couplage
cross-domaine (un module métier important son algorithme depuis le service d'un AUTRE domaine)
était le seul défaut réel ; la logique elle-même n'a pas changé d'une ligne (parité par
construction, testée en double : `tests/test_fifo_engine.py` importe ce module directement,
`tests/test_compte_proprietaire_fifo.py` continue de passer par le ré-export de
`compte_proprietaire_service`).

Ce module ne dépend de RIEN d'applicatif : pas de SQLite, pas de FastAPI, pas de filesystem, pas de
config, pas d'environnement, pas d'état global. Il ne journalise rien, ne committe rien.

CONTRAT
Générique sur les clés `facture_id_opaque`/`montant_total` (créances triées, plus ancienne
d'abord) et `source_type`/`source_ref`/`source_date`/`montant` (sources triées, plus ancienne
d'abord) : le TRI est la responsabilité de l'appelant — ce module n'a aucune notion de date
métier, seulement de l'ordre qu'on lui donne.
"""
from __future__ import annotations

from typing import Any

# Comparaison monétaire : au-delà de ce seuil, un écart est réel et non un artefact d'arrondi.
TOLERANCE = 0.005


def _round(x: float) -> float:
    return round(float(x or 0), 2)


def calculer_fifo(factures: list[dict[str, Any]],
                  sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fonction PURE : (factures triées, sources triées) → allocations. Aucun accès base.

    Isolée volontairement — c'est le cœur de la règle métier, testable sans base, sans facture
    réelle et sans propriétaire (ni intervenant, ni aucun autre domaine qui la réutilise).
    """
    restant_facture = {f["facture_id_opaque"]: f["montant_total"] for f in factures}
    allocations: list[dict[str, Any]] = []
    rang = 0

    for source in sources:
        disponible = source["montant"]
        for facture in factures:
            if disponible <= TOLERANCE:
                break
            du = restant_facture[facture["facture_id_opaque"]]
            if du <= TOLERANCE:
                continue
            montant = _round(min(disponible, du))
            if montant <= 0:
                continue
            rang += 1
            allocations.append({
                "source_type": source["source_type"], "source_ref": source["source_ref"],
                "source_date": source["source_date"],
                "facture_id_opaque": facture["facture_id_opaque"],
                "montant_alloue": montant, "rang_fifo": rang,
            })
            restant_facture[facture["facture_id_opaque"]] = _round(du - montant)
            disponible = _round(disponible - montant)
        # Le reliquat éventuel n'est pas perdu : il devient du crédit, calculé par différence.
    return allocations

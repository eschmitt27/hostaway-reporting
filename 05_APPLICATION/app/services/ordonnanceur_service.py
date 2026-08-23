"""Ordonnanceur — actualisation automatique Hostaway toutes les 5 heures (§39-44).

UN SEUL SERVICE, DEUX DÉCLENCHEURS
Le bouton manuel et l'ordonnanceur appellent le MÊME chemin : `orchestrateur_moteur.
importer_hostaway`, qui appelle lui-même `hostaway_actualisation_service.actualiser`. Il n'existe
pas de seconde implémentation de l'extraction, donc pas de risque qu'un chemin évolue sans l'autre.
Seul le `declencheur` change (AUTO vs MANUEL), ce qui reste visible dans `moteur_runs`.

CADENCES SÉPARÉES, PARCE QUE LES CONTRAINTES SONT DIFFÉRENTES
  · Réservations / payouts : toutes les 5 heures.
  · CleaningTasks (H6)     : PAS à cette cadence. Ce point d'API a rencontré des limites 429
    sévères, et les tâches de ménage ne bougent pas au même rythme que les réservations. La cadence
    par défaut est donc nettement plus lente, et reste une action ciblée. Décider l'inverse
    « parce que c'est plus simple » reviendrait à choisir une règle d'exploitation à la place du
    métier — ce que cette mission interdit explicitement.

LES 429 SONT DÉJÀ TRAITÉS EN AMONT
`lot1_hostaway_extract.py` gère `Retry-After`, un backoff exponentiel PLAFONNÉ, un budget de
tentatives et l'exception `RateLimitEpuise` qui termine le run en PARTIEL. L'ordonnanceur ne
réimplémente rien de tout cela : il se contente de ne pas relancer immédiatement un run qui vient
d'échouer pour cette raison.

MODE RÉEL NON ACTIVÉ (§44)
Ce module ne démarre RIEN de lui-même. `demarrer()` doit être appelé explicitement, et refuse tant
que `cfg.ORDONNANCEUR_ACTIF` est faux. La logique de cadence est testable sans horloge réelle :
toutes les décisions passent par `doit_declencher(maintenant=...)`, une fonction pure.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

# Délai minimal après un run terminé en PARTIEL/ÉCHEC (typiquement une limite d'API atteinte) :
# relancer tout de suite retomberait dans le même mur et consommerait le budget pour rien.
REPRISE_APRES_ECHEC_H = 1

TACHE_HOSTAWAY = "HOSTAWAY_RAW"
TACHE_CLEANING_TASKS = "HOSTAWAY_CLEANING_TASKS"


def cadences() -> dict[str, int]:
    """Lues à chaud depuis `cfg` (jamais figées à l'import) — configurables par variable
    d'environnement (`HOSTAWAY_REFRESH_INTERVAL_HOURS`/`HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS`),
    jamais un second 5 codé en dur ailleurs."""
    return {
        TACHE_HOSTAWAY: cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS,
        TACHE_CLEANING_TASKS: cfg.HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS,
    }

E_INACTIF = "ORDONNANCEUR_INACTIF"


def _maintenant() -> datetime:
    return datetime.now(timezone.utc)


def _parse(horodatage: str | None) -> datetime | None:
    if not horodatage:
        return None
    try:
        return datetime.strptime(horodatage, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def dernier_declenchement(tache: str, *, db_path=None) -> dict[str, Any] | None:
    """Dernier run AUTO ou MANUEL ayant réellement actualisé cette source.

    On regarde l'état du DATASET, pas un compteur interne à l'ordonnanceur : si quelqu'un vient de
    lancer l'extraction à la main, l'ordonnanceur n'a aucune raison de la relancer 5 minutes après.
    """
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT dataset, statut, calcule_le, maj_le, declencheur, erreur_code "
            "FROM orchestrateur_datasets WHERE dataset = ?", (tache,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def doit_declencher(tache: str, *, maintenant: datetime | None = None,
                    db_path=None) -> dict[str, Any]:
    """Décide si `tache` doit partir maintenant. FONCTION PURE vis-à-vis de l'horloge.

    `maintenant` est injectable : les tests pilotent le temps, sans attendre 5 heures et sans
    dépendre de l'heure réelle de la machine.
    """
    maintenant = maintenant or _maintenant()
    cadence_h = cadences()[tache]
    cadence = timedelta(hours=cadence_h)
    etat = dernier_declenchement(tache, db_path=db_path)

    if etat is None or not etat.get("calcule_le"):
        return {"declencher": True, "motif": "Jamais actualisé.", "tache": tache}

    if etat.get("statut") == orch.ST_EN_COURS:
        return {"declencher": False, "motif": "Actualisation déjà en cours.", "tache": tache}

    dernier = _parse(etat.get("calcule_le"))
    if dernier is None:
        return {"declencher": True, "motif": "Date de dernier calcul illisible.", "tache": tache}

    # Un échec récent (limite d'API, notamment) impose un palier avant de réessayer.
    if etat.get("statut") == orch.ST_ECHEC:
        derniere_tentative = _parse(etat.get("maj_le")) or dernier
        if maintenant - derniere_tentative < timedelta(hours=REPRISE_APRES_ECHEC_H):
            return {"declencher": False, "tache": tache,
                    "motif": f"Échec récent ({etat.get('erreur_code') or 'inconnu'}) : "
                             f"attente de {REPRISE_APRES_ECHEC_H}h avant nouvelle tentative."}

    ecoule = maintenant - dernier
    if ecoule >= cadence:
        return {"declencher": True, "tache": tache,
                "motif": f"Dernière actualisation il y a {ecoule}. Cadence : {cadence_h}h."}
    return {"declencher": False, "tache": tache,
            "motif": f"Actualisé il y a {ecoule} (cadence {cadence_h}h)."}


def tick(*, maintenant: datetime | None = None, db_path=None) -> dict[str, Any]:
    """Un battement de l'ordonnanceur : déclenche ce qui est dû, et rien d'autre.

    Après un import Hostaway réussi, les descendants nécessaires sont recalculés via le DAG (§40) —
    par le même orchestrateur que l'écran, sans seconde logique de propagation.
    """
    maintenant = maintenant or _maintenant()
    orch.marquer_runs_interrompus(db_path=db_path)

    decisions, lances = [], []
    for tache in (TACHE_HOSTAWAY, TACHE_CLEANING_TASKS):
        decision = doit_declencher(tache, maintenant=maintenant, db_path=db_path)
        decisions.append(decision)
        if not decision["declencher"]:
            continue
        if not dag.NOEUDS[tache].service:
            # H6 n'a pas encore de service d'import automatique : le dire, ne pas faire semblant.
            decision["motif"] += " (aucun service d'import automatisé pour cette source)"
            continue
        resultat = orch.actualiser(cibles=[tache], declencheur=orch.DECLENCHEUR_AUTO,
                                   inclure_imports_externes=True, db_path=db_path)
        lances.append({"tache": tache, "run_id": resultat.get("run_id"),
                       "statut": resultat.get("statut")})

    return {"horodatage": maintenant.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "decisions": decisions, "lances": lances}


# ── Démarrage explicite (jamais automatique) ────────────────────────────────────────────────────

_minuteur: threading.Timer | None = None


def actif() -> bool:
    return bool(getattr(cfg, "ORDONNANCEUR_ACTIF", False))


def demarrer(*, intervalle_s: int = 900, db_path=None) -> dict[str, Any]:
    """Démarre le battement périodique. REFUSE tant que l'ordonnanceur n'est pas activé.

    L'intervalle de battement (15 min) n'est pas la cadence métier : c'est la fréquence à laquelle
    on se demande « est-ce dû ? ». La cadence réelle reste celle de `cadences()`.
    """
    global _minuteur
    if not actif():
        return {"ok": False, "code": E_INACTIF,
                "message": "Ordonnanceur non activé (cfg.ORDONNANCEUR_ACTIF). "
                           "Aucune actualisation automatique ne sera déclenchée."}
    if _minuteur is not None:
        return {"ok": True, "deja_demarre": True}

    def _battement():
        global _minuteur
        try:
            tick(db_path=db_path)
        except Exception:   # noqa: BLE001 — un battement raté ne doit pas tuer l'ordonnanceur
            pass
        finally:
            _minuteur = threading.Timer(intervalle_s, _battement)
            _minuteur.daemon = True
            _minuteur.start()

    _minuteur = threading.Timer(intervalle_s, _battement)
    _minuteur.daemon = True
    _minuteur.start()
    return {"ok": True, "intervalle_s": intervalle_s, "cadences": cadences()}


def arreter() -> None:
    global _minuteur
    if _minuteur is not None:
        _minuteur.cancel()
        _minuteur = None


def etat(*, db_path=None) -> dict[str, Any]:
    """Ce que l'écran doit pouvoir dire de l'ordonnanceur, sans le démarrer."""
    cad = cadences()
    return {
        "actif": actif(),
        "cadences_h": cad,
        "taches": [dernier_declenchement(t, db_path=db_path) or {"dataset": t, "statut": "JAMAIS_CALCULE"}
                   for t in cad],
        "prochaines_decisions": [doit_declencher(t, db_path=db_path) for t in cad],
    }

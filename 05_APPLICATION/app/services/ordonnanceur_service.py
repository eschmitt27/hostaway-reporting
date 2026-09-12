"""Ordonnanceur — actualisation automatique Hostaway toutes les 5 heures (§39-44).

LE SCHEDULER N'EST QU'UN DÉCLENCHEUR
Il décide QUAND, jamais QUOI ni COMMENT. Un battement appelle `orchestrateur_service.actualiser(
cibles=[HOSTAWAY_RAW], declencheur=AUTO)` — l'appel exact du bouton « Actualiser » de l'écran
Pilotage sur cette source, au déclencheur près. Il ne connaît ni les Lots, ni les tables, ni l'API :
l'orchestrateur appelle le service canonique (`hostaway_depot_service.synchroniser`, le même que le
bouton de l'écran Hostaway), puis le DAG détermine les descendants à recalculer.

CADENCES SÉPARÉES, PARCE QUE LES CONTRAINTES SONT DIFFÉRENTES
  · Réservations / payouts : toutes les 5 heures, DÉCLENCHÉES automatiquement.
  · CleaningTasks (H6)     : cadence configurée (24 h) mais NON déclenchée automatiquement. Aucune
    règle validée ne fixe sa fréquence (CADENCE H6 À ARBITRER) ; ce point d'API a rencontré des
    limites 429 sévères, et il ne part que sur demande explicite (« Actualiser les ménages »).
    Décider l'inverse « parce que c'est plus simple » reviendrait à choisir une règle d'exploitation
    à la place du métier.

LES 429 NE SONT PAS TRAITÉS ICI
Le chemin canonique lit le dépôt publié par le pipeline GitHub : ce poste n'appelle plus l'API.
`Retry-After`, backoff plafonné, budget de tentatives et `RateLimitEpuise` vivent dans
`app/adapters/hostaway_client.py`. L'ordonnanceur ne réimplémente rien de tout cela : il se contente
de ne pas relancer immédiatement un run qui vient d'échouer (`REPRISE_APRES_ECHEC_H`).

MODE RÉEL NON ACTIVÉ (§44)
Ce module ne démarre RIEN de lui-même. `demarrer()` doit être appelé explicitement, et refuse tant
que `cfg.ORDONNANCEUR_ACTIF` est faux. La logique de cadence est testable sans horloge réelle :
toutes les décisions passent par `doit_declencher(maintenant=...)`.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch
from app.services.logging_config import get_logger
from app.services.path_sanitizer import sanitize_exception

# Délai minimal après un run terminé en ÉCHEC (typiquement une source indisponible) : relancer tout
# de suite retomberait dans le même mur et consommerait le budget pour rien.
REPRISE_APRES_ECHEC_H = 1

TACHE_HOSTAWAY = "HOSTAWAY_RAW"
TACHE_CLEANING_TASKS = "HOSTAWAY_CLEANING_TASKS"

# Sources que le battement DÉCLENCHE réellement. H6 n'y figure pas (CADENCE H6 À ARBITRER, cf.
# `SCHEDULER_HOSTAWAY.md`) : sa cadence reste configurée et son échéance affichée, rien de plus.
TACHES_AUTOMATIQUES = (TACHE_HOSTAWAY,)

# Nom du fil du minuteur : c'est ce qui permet de PROUVER, en comptant les fils vivants, qu'une
# application n'a jamais qu'un seul scheduler.
NOM_FIL = "ordonnanceur-hostaway"


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


def _decision_cadence(tache: str, maintenant: datetime, db_path) -> dict[str, Any]:
    cadence_h = cadences()[tache]
    cadence = timedelta(hours=cadence_h)
    etat = dernier_declenchement(tache, db_path=db_path)

    # Une synchronisation détient le verrou de cette source (clic manuel, autre poste) : refus propre
    # et immédiat. Rien n'attend — le battement suivant reconsidérera.
    if orch.verrou_actif(tache, db_path=db_path):
        return {"declencher": False, "motif": "Actualisation déjà en cours.", "tache": tache}

    if etat is None or not etat.get("calcule_le"):
        return {"declencher": True, "motif": "Jamais actualisé.", "tache": tache}

    if etat.get("statut") == orch.ST_EN_COURS:
        return {"declencher": False, "motif": "Actualisation déjà en cours.", "tache": tache}

    dernier = _parse(etat.get("calcule_le"))
    if dernier is None:
        return {"declencher": True, "motif": "Date de dernier calcul illisible.", "tache": tache}

    # Un échec récent (source indisponible, notamment) impose un palier avant de réessayer.
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


def doit_declencher(tache: str, *, maintenant: datetime | None = None,
                    db_path=None) -> dict[str, Any]:
    """Décide si `tache` doit partir maintenant. FONCTION PURE vis-à-vis de l'horloge.

    `maintenant` est injectable : les tests pilotent le temps, sans attendre 5 heures et sans
    dépendre de l'heure réelle de la machine.

    Une source hors `TACHES_AUTOMATIQUES` ne part JAMAIS d'ici, même échéance atteinte : la décision
    le dit (`automatique=False`) au lieu de laisser croire qu'elle partira au prochain battement.
    """
    maintenant = maintenant or _maintenant()
    decision = _decision_cadence(tache, maintenant, db_path)
    if tache in TACHES_AUTOMATIQUES:
        return {**decision, "automatique": True}
    return {**decision, "automatique": False, "echeance_atteinte": decision["declencher"],
            "declencher": False,
            "motif": "Non déclenchée automatiquement (cadence à arbitrer). " + decision["motif"]}


def tick(*, maintenant: datetime | None = None, db_path=None) -> dict[str, Any]:
    """Un battement de l'ordonnanceur : déclenche ce qui est dû, et rien d'autre.

    Après un import Hostaway réussi, les descendants nécessaires sont recalculés via le DAG (§40) —
    par le même orchestrateur que l'écran, sans seconde logique de propagation.
    """
    maintenant = maintenant or _maintenant()
    orch.marquer_runs_interrompus(db_path=db_path)

    decisions, lances = [], []
    for tache in cadences():
        decision = doit_declencher(tache, maintenant=maintenant, db_path=db_path)
        decisions.append(decision)
        if not decision["declencher"]:
            continue
        if not dag.NOEUDS[tache].service:
            # Une source sans service d'import : le dire, ne pas faire semblant.
            decision["motif"] += " (aucun service d'import automatisé pour cette source)"
            continue
        resultat = orch.actualiser(cibles=[tache], declencheur=orch.DECLENCHEUR_AUTO,
                                   inclure_imports_externes=True, db_path=db_path)
        lances.append({"tache": tache, "run_id": resultat.get("run_id"),
                       "statut": resultat.get("statut")})

    return {"horodatage": maintenant.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "decisions": decisions, "lances": lances}


# ── Démarrage explicite (jamais automatique) ────────────────────────────────────────────────────
#
# UN PROCESSUS, UN MINUTEUR, ET JAMAIS UN MINUTEUR RESSUSCITÉ
# `_verrou_cycle` sérialise démarrage, arrêt et réarmement. `_generation` numérote les cycles : un
# battement qui se termine APRÈS `arreter()` (ou après un redémarrage) appartient à un cycle révolu
# et ne réarme rien. Sans ce numéro, un arrêt demandé pendant un battement laissait le `finally`
# recréer un minuteur — un fil orphelin après l'arrêt, ou deux schedulers après un redémarrage.

_verrou_cycle = threading.Lock()
_minuteur: threading.Timer | None = None
_generation = 0


def actif() -> bool:
    """Le scheduler est-il AUTORISÉ ? (configuration — ne dit pas s'il tourne)."""
    return bool(getattr(cfg, "ORDONNANCEUR_ACTIF", False))


def en_marche() -> bool:
    """Un minuteur est-il armé dans CE processus ?"""
    return _minuteur is not None


def _battement_protege(db_path) -> None:
    try:
        tick(db_path=db_path)
    except Exception as exc:   # noqa: BLE001 — un battement raté ne doit pas tuer l'ordonnanceur
        # Tracé, jamais avalé en silence : sans cette ligne, un scheduler qui échoue à chaque
        # battement ressemble à un scheduler qui n'a simplement rien à faire.
        get_logger().error("Ordonnanceur : battement en échec (%s) — %s",
                           type(exc).__name__, sanitize_exception(exc))


def _armer(generation: int, intervalle_s: float, db_path) -> None:
    """Arme le prochain battement. Toujours appelé sous `_verrou_cycle`."""
    global _minuteur

    def _battement():
        _battement_protege(db_path)
        with _verrou_cycle:
            if generation == _generation and _minuteur is not None:
                _armer(generation, intervalle_s, db_path)

    minuteur = threading.Timer(intervalle_s, _battement)
    minuteur.daemon = True
    minuteur.name = NOM_FIL
    _minuteur = minuteur
    minuteur.start()


def demarrer(*, intervalle_s: float = 900, db_path=None) -> dict[str, Any]:
    """Démarre le battement périodique. REFUSE tant que l'ordonnanceur n'est pas activé.

    L'intervalle de battement (15 min) n'est pas la cadence métier : c'est la fréquence à laquelle
    on se demande « est-ce dû ? ». La cadence réelle reste celle de `cadences()`.
    """
    global _generation
    if not actif():
        return {"ok": False, "code": E_INACTIF,
                "message": "Ordonnanceur non activé (cfg.ORDONNANCEUR_ACTIF). "
                           "Aucune actualisation automatique ne sera déclenchée."}
    with _verrou_cycle:
        if _minuteur is not None:
            return {"ok": True, "deja_demarre": True}
        _generation += 1
        _armer(_generation, intervalle_s, db_path)
    return {"ok": True, "intervalle_s": intervalle_s, "cadences": cadences()}


def arreter() -> None:
    global _minuteur, _generation
    with _verrou_cycle:
        if _minuteur is not None:
            _minuteur.cancel()
            _minuteur = None
        _generation += 1


def etat(*, db_path=None) -> dict[str, Any]:
    """Ce que l'écran doit pouvoir dire de l'ordonnanceur, sans le démarrer."""
    cad = cadences()
    return {
        "actif": actif(),
        "demarre": en_marche(),
        "cadences_h": cad,
        "taches_automatiques": list(TACHES_AUTOMATIQUES),
        "taches": [dernier_declenchement(t, db_path=db_path) or {"dataset": t, "statut": "JAMAIS_CALCULE"}
                   for t in cad],
        "prochaines_decisions": [doit_declencher(t, db_path=db_path) for t in cad],
    }

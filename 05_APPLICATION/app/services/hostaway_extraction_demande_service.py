"""Extraction Hostaway À LA DEMANDE — le pipeline GitHub canonique, déclenché par un clic.

POURQUOI PASSER PAR GITHUB
Les identifiants Hostaway n'existent que dans les secrets du dépôt GitHub : ce poste n'en porte pas,
et n'a pas à en porter. Le moteur d'extraction canonique — `extract_reservations.py`,
`extract_finance_fields.py`, `extract_cleaning_tasks.py`, orchestrés par `pipeline.yml` — tourne
sur GitHub Actions trois fois par jour. Ce module ne réécrit RIEN de ce moteur : il le DÉCLENCHE
(`workflow_dispatch`, déjà prévu par le workflow), suit ses étapes en direct, et rend la main quand
le run a publié. L'import qui suit est la synchronisation atomique existante
(`hostaway_depot_service.synchroniser`) : le dépôt reste le transport, il ne borne plus la
fraîcheur d'une actualisation manuelle.

IDENTIFIER NOTRE RUN
`pipeline.yml` n'a pas d'entrée `request_id` (le modifier toucherait la branche publiée) : le run
est celui qui apparaît après le dispatch, déclenché par `workflow_dispatch`, absent de la liste
relevée juste avant. Les deux workflows Hostaway partagent un groupe de concurrence : si un run
planifié publie déjà, le nôtre attend son tour (« en file d'attente sur GitHub »), il ne le
double jamais.

AUCUNE ÉCRITURE ICI. Ni base, ni fichier : seulement des appels à l'API GitHub (un POST de dispatch,
des GET de suivi) et des rappels `suivi(sous_etapes)` pour l'écran.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.adapters import github_actions_client as gh

WORKFLOW_PIPELINE = "pipeline.yml"
ATTENTE_MAX_S = 20 * 60      # file d'attente derrière un run planifié + ~3 min d'extraction
INTERVALLE_S = 3

E_RUN_INTROUVABLE = "HOSTAWAY_DEMANDE_RUN_INTROUVABLE"
E_RUN_ECHEC = "HOSTAWAY_DEMANDE_RUN_ECHEC"
E_RUN_ANNULE = "HOSTAWAY_DEMANDE_RUN_ANNULE"
E_DELAI = "HOSTAWAY_DEMANDE_DELAI"

# Sous-étapes affichées, et les étapes du job GitHub qui les composent (noms de `pipeline.yml`).
SOUS_ETAPES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Hostaway — connexion", ("Set up job", "Checkout repository", "Set up Python",
                              "Install dependencies")),
    ("Hostaway — réservations", ("Run reservations extraction",)),
    ("Hostaway — données financières", ("Run finance fields extraction",)),
    ("Hostaway — tâches de ménage", ("Run cleaning tasks extraction",)),
    ("Hostaway — publication des données", ("Build final report", "Commit generated files")),
)
TACHES = "Hostaway — tâches de ménage"

CAUSES = {
    gh.E_CONFIGURATION: "Jeton GitHub absent du fichier « .env » (HOSTAWAY_GITHUB_TOKEN) : "
                        "l'extraction à la demande ne peut pas être lancée.",
    gh.E_AUTHENTIFICATION: "GitHub a refusé le jeton (expiré ou révoqué) : l'extraction à la "
                           "demande ne peut pas être lancée.",
    gh.E_INTERDIT: "Le jeton GitHub ne permet pas de lancer le pipeline Hostaway.",
    gh.E_LIMITE: "GitHub est momentanément saturé : réessayez dans quelques minutes.",
}


def _instant(texte: str | None) -> datetime | None:
    if not texte:
        return None
    try:
        return datetime.fromisoformat(str(texte).replace("Z", "+00:00"))
    except ValueError:
        return None


def sous_etapes_initiales() -> list[dict[str, Any]]:
    return [{"libelle": libelle, "etat": "attente", "message": "", "duree_s": None}
            for libelle, _ in SOUS_ETAPES]


def _sous_etapes(etapes_github: list[dict], *, file_attente: bool) -> list[dict[str, Any]]:
    """Traduit l'état RÉEL des étapes du job en sous-étapes lisibles."""
    par_nom = {e["nom"]: e for e in etapes_github}
    out = []
    for libelle, noms in SOUS_ETAPES:
        lues = [par_nom[n] for n in noms if n in par_nom]
        debuts = [d for d in (_instant(e.get("started_at")) for e in lues) if d]
        fins = [f for f in (_instant(e.get("completed_at")) for e in lues) if f]
        message = ""
        # GitHub annonce les étapes à venir « pending » (parfois « queued » ou « waiting ») : elles
        # n'ont pas commencé — les dire « en cours » ferait tout clignoter au démarrage du job.
        if not lues or all(e.get("status") in ("queued", "pending", "waiting") for e in lues):
            etat = "attente"
            if libelle == SOUS_ETAPES[0][0] and file_attente:
                etat, message = "en_cours", "En file d'attente sur GitHub…"
        elif any(e.get("conclusion") in ("failure", "timed_out", "cancelled") for e in lues):
            etat, message = "echec", "Étape en échec sur GitHub."
        elif all(e.get("status") == "completed" for e in lues) and len(lues) == len(noms):
            etat = "termine"
        else:
            etat = "en_cours"
        duree = None
        if debuts:
            fin = max(fins) if etat in ("termine", "echec") and fins else datetime.now(timezone.utc)
            duree = max((fin - min(debuts)).total_seconds(), 0.0)
        out.append({"libelle": libelle, "etat": etat, "message": message, "duree_s": duree})
    return out


def _trouver(client, avant: set, depuis: datetime) -> dict | None:
    nouveaux = [r for r in client.runs_workflow(WORKFLOW_PIPELINE, depuis=depuis)
                if r.get("id") not in avant]
    nouveaux.sort(key=lambda r: _instant(r.get("created_at"))
                  or datetime.max.replace(tzinfo=timezone.utc))
    return nouveaux[0] if nouveaux else None


def extraire_maintenant(*, suivi: Callable[[list[dict]], None] | None = None, client=None,
                        attente_max_s: float = ATTENTE_MAX_S, intervalle_s: float = INTERVALLE_S,
                        horloge: Callable[[], float] = time.monotonic,
                        dormir: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Lance le pipeline Hostaway canonique et attend qu'il ait publié.

    Rend `{"ok", "run_id", "debut", "fin", "sous_etapes", "taches_ok"}` ; en échec, `code` et un
    `message` court et lisible. Les données ne sont PAS importées ici.
    """
    signaler = suivi or (lambda _s: None)
    sous = sous_etapes_initiales()
    sous[0].update(etat="en_cours", message="Lancement de l'extraction sur GitHub…")
    signaler(sous)

    def echec(code: str, message: str, sous_etapes=None) -> dict[str, Any]:
        etapes = sous_etapes or sous
        if not any(s["etat"] == "echec" for s in etapes):
            courante = next((s for s in etapes if s["etat"] in ("en_cours", "attente")), etapes[0])
            courante.update(etat="echec", message=message)
        signaler(etapes)
        return {"ok": False, "code": code, "message": message, "sous_etapes": etapes}

    try:
        client = client or gh.ClientGitHubActions()
        depuis = datetime.now(timezone.utc) - timedelta(minutes=2)
        avant = {r.get("id") for r in client.runs_workflow(WORKFLOW_PIPELINE, depuis=depuis)}
        debut = datetime.now(timezone.utc)
        indice = client.dispatch_workflow(WORKFLOW_PIPELINE).get("workflow_run_id")
    except gh.ErreurGitHub as err:
        return echec(err.code, CAUSES.get(err.code, "GitHub est injoignable : l'extraction à la "
                                                    "demande n'a pas pu être lancée."))

    limite = horloge() + attente_max_s
    limite_apparition = horloge() + 120     # un dispatch accepté apparaît en quelques secondes
    run = None
    try:
        while True:
            if run is None:
                run = client.run(indice) if indice else _trouver(client, avant, depuis)
            if run is not None:
                run = client.run(run["id"])
                etapes = client.etapes_run(run["id"])
                sous = _sous_etapes(etapes, file_attente=run.get("status") in ("queued",
                                                                               "waiting",
                                                                               "pending"))
                signaler(sous)
                if run.get("status") == "completed":
                    break
            if run is None and horloge() > limite_apparition:
                return echec(E_RUN_INTROUVABLE, "GitHub a accepté la demande mais le run "
                                                "d'extraction n'est pas apparu : réessayez.")
            if horloge() > limite:
                return echec(E_DELAI, "L'extraction Hostaway est toujours en cours sur GitHub "
                                      "après 20 minutes : les données seront publiées à sa fin. "
                                      "Relancez l'actualisation ensuite.")
            dormir(intervalle_s)
    except gh.ErreurGitHub as err:
        return echec(err.code, CAUSES.get(err.code, "Le suivi de l'extraction sur GitHub a été "
                                                    "interrompu (réseau)."))

    conclusion = run.get("conclusion")
    if conclusion == "cancelled":
        return echec(E_RUN_ANNULE, "L'extraction Hostaway a été annulée sur GitHub. Les dernières "
                                   "données valides sont conservées.", sous)
    if conclusion != "success":
        fautive = next((s["libelle"] for s in sous if s["etat"] == "echec"), "")
        return echec(E_RUN_ECHEC, "L'extraction Hostaway a échoué sur GitHub"
                                  + (f" (« {fautive} »)" if fautive else "")
                                  + ". Les dernières données valides sont conservées.", sous)
    taches_ok = next(s for s in sous if s["libelle"] == TACHES)["etat"] != "echec"
    if not taches_ok:
        next(s for s in sous if s["libelle"] == TACHES)["message"] = (
            "Extraction des tâches en échec sur GitHub : les tâches précédentes restent publiées.")
    signaler(sous)
    return {"ok": True, "run_id": run["id"], "debut": debut.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fin": run.get("updated_at"), "sous_etapes": sous, "taches_ok": taches_ok}

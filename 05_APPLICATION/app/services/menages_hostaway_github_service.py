"""« Actualiser les ménages » — cycle complet, asynchrone, par le workflow GitHub des tâches.

    clic ─► ligne d'actualisation (LANCEMENT) ─► réponse immédiate ; l'écran interroge l'état
                  │  (travail de fond)
                  ▼
    dispatch du workflow avec request_id ─► EN_ATTENTE
    run retrouvé PAR SON TITRE « Hostaway Cleaning Tasks — <request_id> » ─► EXTRACTION
    run success ─► RECUPERATION : artifact « cleaning-tasks-output » DE CE RUN, TSV validé,
                   extraction préparée NON ACTIVE
               ─► RAPPROCHEMENT : chaîne Ménages canonique (`menages_actualisation_service`),
                   dont l'étape « tâches » active le jeu préparé puis lance lot6a
               ─► TERMINE
    failure / timed_out / erreur ─► ECHEC      cancelled ─► ANNULE      (aucun import)

ATOMICITÉ. Le jeu de tâches servi ne change que si TOUT a réussi : run success, artifact présent,
TSV conforme, écriture SQLite, comptage lot6a et recalcul des mois ciblés. Un échec avant
l'activation laisse le jeu précédent intact (l'extraction préparée est mise en ÉCHEC sans jamais
avoir été servie) ; un échec après l'activation la désactive et relance le comptage et le recalcul
sur le jeu précédent. Rien n'est effacé.

UNE SEULE ACTUALISATION ACTIVE. Garantie par la base (index unique partiel sur `active`), pas par
une vérification applicative : deux clics simultanés ne peuvent pas créer deux lignes actives, donc
jamais deux dispatch.

IDEMPOTENCE DU SUIVI. Chaque transition est une mise à jour CONDITIONNELLE (`WHERE etat = …`). Le
dispatch, la récupération de l'artifact et le rapprochement sont « réclamés » avant d'être faits :
un second suiveur (redémarrage, second onglet) lit l'état, il ne refait rien.
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from app.adapters import github_actions_client as gh
from app.db.connection import get_db

NOM_ARTIFACT = "cleaning-tasks-output"
NOM_FICHIER = "cleaning_tasks_hostaway.tsv"
PREFIXE_REQUEST_ID = "MEN-REFRESH-"

LANCEMENT = "LANCEMENT"
EN_ATTENTE = "EN_ATTENTE"
EXTRACTION = "EXTRACTION"
RECUPERATION = "RECUPERATION"
RAPPROCHEMENT = "RAPPROCHEMENT"
TERMINE = "TERMINE"
ECHEC = "ECHEC"
ANNULE = "ANNULE"
TERMINAUX = frozenset({TERMINE, ECHEC, ANNULE})

LIBELLES = {
    LANCEMENT: "Lancement", EN_ATTENTE: "En attente", EXTRACTION: "Extraction",
    RECUPERATION: "Récupération", RAPPROCHEMENT: "Rapprochement", TERMINE: "Terminé",
    ECHEC: "Échec", ANNULE: "Annulé",
}
MESSAGES = {
    TERMINE: "Données Hostaway actualisées.",
    ECHEC: ("L'actualisation Hostaway a échoué. Les dernières données valides ont été "
            "conservées."),
    ANNULE: ("L'actualisation Hostaway a été annulée. Les dernières données valides ont été "
             "conservées."),
}

# Une cause n'est ajoutée que lorsqu'elle appelle une action de l'exploitant : un accès non
# configuré ou refusé ne se résout pas en recliquant.
CAUSES_LISIBLES = {
    gh.E_CONFIGURATION: ("L'accès au service d'extraction Hostaway n'est pas configuré sur ce poste "
                         "(voir « .env.example », rubrique « Actualisation des ménages »)."),
    gh.E_AUTHENTIFICATION: ("L'accès au service d'extraction Hostaway a été refusé : l'autorisation "
                            "a expiré ou a été révoquée."),
    gh.E_INTERDIT: ("L'accès au service d'extraction Hostaway a été refusé : l'autorisation ne "
                    "couvre pas cette opération."),
    gh.E_LIMITE: "Le service d'extraction est momentanément saturé : réessayez plus tard.",
}

INTERVALLE_SUIVI_S = 4
# Délai pour que le run apparaisse après le dispatch. GitHub le crée en quelques secondes ; au-delà
# de 3 minutes, le dispatch n'a pas produit de run (ou un run concurrent l'a supplanté).
DELAI_RUN_INTROUVABLE_S = 180
# Le job est borné à 15 min (`timeout-minutes`), mais un run peut attendre son tour dans le groupe de
# concurrence `hostaway-data-publish` derrière le pipeline complet. 45 min couvrent les deux.
DELAI_RUN_MAX_S = 45 * 60
# Une étape réclamée (récupération, rapprochement) sans nouvelle depuis ce délai, et sans suiveur
# vivant dans ce processus, a été interrompue (arrêt du serveur).
DELAI_ETAPE_INTERROMPUE_S = 60 * 60

# Erreurs GitHub TRANSITOIRES pendant le suivi : on réessaie au tour suivant, jusqu'au délai max.
ERREURS_TRANSITOIRES = frozenset({gh.E_LIMITE, gh.E_DELAI, gh.E_RESEAU, gh.E_API})

CONCLUSION_ANNULEE = "cancelled"
CONCLUSION_SUCCES = "success"


def _maintenant() -> datetime:
    return datetime.now(timezone.utc)


def _iso(d: datetime) -> str:
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _lire_iso(texte: str | None) -> datetime | None:
    if not texte:
        return None
    try:
        return datetime.fromisoformat(str(texte).replace("Z", "+00:00"))
    except ValueError:
        return None


def nouveau_request_id(maintenant: datetime | None = None) -> str:
    """`MEN-REFRESH-20260924T215012Z-3f9a1c` — 36 caractères, alphabet `[A-Za-z0-9._-]`."""
    d = maintenant or _maintenant()
    rid = f"{PREFIXE_REQUEST_ID}{d.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    assert gh.REQUEST_ID_VALIDE.match(rid)
    return rid


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

def table_presente(db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='menages_actualisations_hostaway'"
        ).fetchone() is not None
    finally:
        conn.close()


def charger(refresh_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM menages_actualisations_hostaway WHERE refresh_id=?",
                         (refresh_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def active(*, db_path=None) -> dict[str, Any] | None:
    if not table_presente(db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM menages_actualisations_hostaway WHERE active=1").fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def derniere(*, db_path=None) -> dict[str, Any] | None:
    if not table_presente(db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM menages_actualisations_hostaway "
                         "ORDER BY id DESC LIMIT 1").fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def vue(ligne: dict[str, Any] | None) -> dict[str, Any] | None:
    """Ce que l'écran affiche : aucun vocabulaire GitHub, aucun identifiant technique superflu."""
    if not ligne:
        return None
    etat = ligne["etat"]
    resume = [p for p in (ligne.get("resume") or "").split(" · ") if p]
    cause = CAUSES_LISIBLES.get(ligne.get("code_erreur") or "") if etat == ECHEC else None
    if cause:
        resume.append(cause)
    return {
        "refresh_id": ligne["refresh_id"],
        "etat": etat,
        "libelle": LIBELLES.get(etat, etat),
        "termine": etat in TERMINAUX,
        "succes": etat == TERMINE,
        "message": MESSAGES.get(etat, ""),
        "resume": resume,
        "demande_le": ligne.get("demande_le"),
        "termine_le": ligne.get("termine_le"),
        "nb_taches": ligne.get("nb_taches"),
        "mois_affiche": ligne.get("mois_affiche"),
    }


# ── Écriture ────────────────────────────────────────────────────────────────────────────────────

def _maj(refresh_id: str, *, db_path=None, depuis: tuple[str, ...] | None = None,
         **colonnes) -> bool:
    """Mise à jour, CONDITIONNELLE si `depuis` est donné. Rend vrai si la ligne a été modifiée —
    c'est ce booléen qui décide qui, de deux suiveurs, fait le travail réclamé."""
    colonnes.setdefault("maj_le", _iso(_maintenant()))
    if colonnes.get("etat") in TERMINAUX:
        colonnes["active"] = None
        colonnes.setdefault("termine_le", colonnes["maj_le"])
    affect = ", ".join(f"{k} = ?" for k in colonnes)
    sql = f"UPDATE menages_actualisations_hostaway SET {affect} WHERE refresh_id = ?"
    args: list[Any] = [*colonnes.values(), refresh_id]
    if depuis:
        sql += f" AND etat IN ({', '.join('?' * len(depuis))})"
        args += list(depuis)
    conn = get_db(db_path)
    try:
        cur = conn.execute(sql, args)
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def _terminer(refresh_id: str, etat: str, *, code: str = "", erreur: str = "",
              resume: str = "", db_path=None, **autres) -> None:
    _maj(refresh_id, db_path=db_path, depuis=tuple(set(LIBELLES) - TERMINAUX), etat=etat,
         code_erreur=code or None, erreur=(erreur or None) and erreur[:500],
         resume=resume or MESSAGES.get(etat, ""), **autres)


def demarrer(*, acteur: str, mois_affiche: str = "", lancer_suivi: bool = True,
             db_path=None) -> dict[str, Any]:
    """Enregistre la demande et rend la main. N'appelle PAS GitHub : le dispatch est la première
    étape du suivi de fond. Une actualisation déjà active est RENDUE, jamais doublée."""
    existante = active(db_path=db_path)
    if existante:
        if lancer_suivi:
            assurer_suivi(existante["refresh_id"], db_path=db_path)
        return {"ok": True, "deja_en_cours": True, "refresh_id": existante["refresh_id"]}
    refresh_id = "MRH-" + uuid.uuid4().hex[:12].upper()
    request_id = nouveau_request_id()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_actualisations_hostaway (refresh_id, request_id, acteur, "
            "mois_affiche, demande_le, maj_le) VALUES (?,?,?,?,?,?)",
            (refresh_id, request_id, acteur or "ui:menages", mois_affiche or None,
             _iso(_maintenant()), _iso(_maintenant())))
        conn.commit()
    except sqlite3.IntegrityError:
        # Une autre requête a créé l'actualisation active entre notre lecture et notre écriture.
        conn.rollback()
        existante = active(db_path=db_path)
        return {"ok": True, "deja_en_cours": True,
                "refresh_id": existante["refresh_id"] if existante else ""}
    finally:
        conn.close()
    if lancer_suivi:
        assurer_suivi(refresh_id, db_path=db_path)
    return {"ok": True, "deja_en_cours": False, "refresh_id": refresh_id}


# ── Suivi ───────────────────────────────────────────────────────────────────────────────────────

def avancer(refresh_id: str, *, client_factory: Callable[[], Any] | None = None,
            chaine: Callable[..., dict] | None = None,
            maintenant: Callable[[], datetime] = _maintenant, db_path=None) -> dict[str, Any]:
    """UN pas du cycle. Sans effet sur une actualisation terminée. Rend la ligne à jour.

    `client_factory` / `chaine` sont injectables pour les tests : par défaut le vrai client GitHub et
    la vraie chaîne Ménages.
    """
    ligne = charger(refresh_id, db_path=db_path)
    if not ligne or ligne["etat"] in TERMINAUX:
        return ligne or {}
    fabrique = client_factory or gh.ClientGitHubActions
    etat = ligne["etat"]
    try:
        if etat == LANCEMENT:
            _pas_lancement(ligne, fabrique, maintenant, db_path)
        elif etat in (EN_ATTENTE, EXTRACTION):
            _pas_suivi_run(ligne, fabrique, maintenant, chaine, db_path)
        elif etat in (RECUPERATION, RAPPROCHEMENT):
            _pas_interrompu(ligne, maintenant, db_path)
    except gh.ErreurGitHub as exc:
        _erreur_github(ligne, exc, maintenant, db_path)
    return charger(refresh_id, db_path=db_path) or {}


def _ecoule(ligne: dict[str, Any], maintenant: Callable[[], datetime]) -> float:
    debut = _lire_iso(ligne.get("demande_le")) or maintenant()
    return (maintenant() - debut).total_seconds()


def _erreur_github(ligne, exc: gh.ErreurGitHub, maintenant, db_path) -> None:
    refresh_id = ligne["refresh_id"]
    if exc.code in ERREURS_TRANSITOIRES and ligne["etat"] in (EN_ATTENTE, EXTRACTION) \
            and _ecoule(ligne, maintenant) < DELAI_RUN_MAX_S:
        _maj(refresh_id, db_path=db_path, code_erreur=exc.code, erreur=str(exc)[:500])
        return
    _terminer(refresh_id, ECHEC, code=exc.code, erreur=str(exc), db_path=db_path)


def _pas_lancement(ligne, fabrique, maintenant, db_path) -> None:
    refresh_id = ligne["refresh_id"]
    # Réclamation du dispatch : un seul suiveur passe `github_statut` de NULL à « dispatch ».
    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "UPDATE menages_actualisations_hostaway SET github_statut='dispatch', maj_le=? "
            "WHERE refresh_id=? AND etat=? AND github_statut IS NULL",
            (_iso(maintenant()), refresh_id, LANCEMENT))
        conn.commit()
        reclame = cur.rowcount == 1
    finally:
        conn.close()
    if not reclame:
        # Dispatch réclamé par un suiveur disparu avant d'avoir conclu : on ne sait pas s'il est
        # parti. Le relancer risquerait deux runs ; au-delà du délai, on conclut à l'échec.
        if _ecoule(ligne, maintenant) > DELAI_RUN_INTROUVABLE_S:
            _terminer(refresh_id, ECHEC, code="LANCEMENT_INTERROMPU",
                      erreur="lancement interrompu avant confirmation", db_path=db_path)
        return
    try:
        client = fabrique()
        resultat = client.dispatch(ligne["request_id"])
    except gh.ErreurGitHub as exc:
        _terminer(refresh_id, ECHEC, code=exc.code, erreur=str(exc), db_path=db_path)
        return
    run_id = resultat.get("workflow_run_id")
    _maj(refresh_id, db_path=db_path, depuis=(LANCEMENT,), etat=EN_ATTENTE,
         github_statut="queued", github_run_id=str(run_id) if run_id else None)


def _trouver(client, ligne) -> dict | None:
    """Le run DE CETTE DEMANDE. L'identifiant rendu par le dispatch n'est qu'un raccourci : il est
    vérifié par le titre, et ignoré s'il ne correspond pas."""
    attendu = gh.titre_run(ligne["request_id"])
    if ligne.get("github_run_id"):
        run = client.run(ligne["github_run_id"])
        if run.get("display_title") == attendu:
            return run
    return client.trouver_run(ligne["request_id"], depuis=_lire_iso(ligne.get("demande_le")))


def _pas_suivi_run(ligne, fabrique, maintenant, chaine, db_path) -> None:
    refresh_id = ligne["refresh_id"]
    client = fabrique()
    run = _trouver(client, ligne)
    if run is None:
        if _ecoule(ligne, maintenant) > DELAI_RUN_INTROUVABLE_S:
            _terminer(refresh_id, ECHEC, code="RUN_INTROUVABLE",
                      erreur="aucun run portant le titre attendu", db_path=db_path)
        return
    statut, conclusion = run.get("status"), run.get("conclusion")
    commun = {"github_run_id": str(run.get("id")), "github_statut": statut,
              "github_conclusion": conclusion, "code_erreur": None, "erreur": None}
    if statut != "completed":
        if _ecoule(ligne, maintenant) > DELAI_RUN_MAX_S:
            _terminer(refresh_id, ECHEC, code=gh.E_DELAI,
                      erreur="le run n'a pas abouti dans le délai", db_path=db_path, **{
                          k: v for k, v in commun.items() if k.startswith("github")})
            return
        nouvel_etat = EXTRACTION if statut == "in_progress" else EN_ATTENTE
        _maj(refresh_id, db_path=db_path, depuis=(EN_ATTENTE, EXTRACTION), etat=nouvel_etat,
             **commun)
        return
    if conclusion == CONCLUSION_ANNULEE:
        _terminer(refresh_id, ANNULE, code="RUN_ANNULE", db_path=db_path,
                  **{k: v for k, v in commun.items() if k.startswith("github")})
        return
    if conclusion != CONCLUSION_SUCCES:
        _terminer(refresh_id, ECHEC, code=f"RUN_{str(conclusion or 'inconnu').upper()}",
                  erreur=f"run conclu « {conclusion} »", db_path=db_path,
                  **{k: v for k, v in commun.items() if k.startswith("github")})
        return
    # Succès : réclamation de la récupération — un seul suiveur télécharge et importe.
    if not _maj(refresh_id, db_path=db_path, depuis=(EN_ATTENTE, EXTRACTION), etat=RECUPERATION,
                **commun):
        return
    _recuperer_et_rapprocher(charger(refresh_id, db_path=db_path), client, chaine, db_path)


def _recuperer_et_rapprocher(ligne, client, chaine, db_path) -> None:
    from app.services import hostaway_cleaning_tasks_actualisation_service as taches_svc

    refresh_id = ligne["refresh_id"]
    try:
        artefact = client.telecharger_artifact(ligne["github_run_id"], NOM_ARTIFACT, NOM_FICHIER)
    except gh.ErreurGitHub as exc:
        _terminer(refresh_id, ECHEC, code=exc.code, erreur=str(exc), db_path=db_path)
        return
    prepare = taches_svc.preparer_depuis_artifact(
        artefact["octets"], request_id=ligne["request_id"], db_path=db_path)
    _maj(refresh_id, db_path=db_path, artifact_id=str(artefact.get("artifact_id") or ""),
         artifact_traite=1, extraction_id=prepare.get("extraction_id"),
         nb_taches=prepare.get("nb_taches"))
    if not prepare.get("ok"):
        _terminer(refresh_id, ECHEC, code=prepare.get("code", "ARTIFACT_INVALIDE"),
                  erreur=prepare.get("message", ""), db_path=db_path)
        return
    if not _maj(refresh_id, db_path=db_path, depuis=(RECUPERATION,), etat=RAPPROCHEMENT):
        return
    _rapprocher(charger(refresh_id, db_path=db_path), prepare, chaine, db_path)


def _rapprocher(ligne, prepare: dict[str, Any], chaine, db_path) -> None:
    """Chaîne Ménages CANONIQUE, dont l'étape « tâches » active le jeu préparé. Aucun moteur
    parallèle : `menages_actualisation_service.actualiser`, tel que le bouton l'a toujours lancé."""
    from app.services import hostaway_cleaning_tasks_actualisation_service as taches_svc
    from app.services import menages_actualisation_service as chaine_menages
    from app.services import orchestrateur_moteur as moteur
    from app.services import orchestrateur_service as orch

    refresh_id = ligne["refresh_id"]
    extraction_id = prepare.get("extraction_id")
    nouveau = bool(prepare.get("importe"))
    etat_etape: dict[str, Any] = {"active": False}

    def etape_taches() -> dict[str, Any]:
        if nouveau:
            taches_svc.activer(extraction_id, db_path=db_path,
                               message=f"Artifact du run {ligne.get('github_run_id')} "
                                       f"({ligne['request_id']})")
            etat_etape["active"] = True
        comptage = moteur.compter_taches_menage(db_path=db_path)
        orch.marquer_dataset(
            "HOSTAWAY_CLEANING_TASKS", orch.ST_A_JOUR if comptage.get("ok") else orch.ST_ECHEC,
            declencheur=orch.DECLENCHEUR_MANUEL, motif=f"ARTIFACT {ligne['request_id']}",
            erreur_code="" if comptage.get("ok") else comptage.get("code", "ECHEC"),
            erreur_message="" if comptage.get("ok") else comptage.get("message", ""),
            db_path=db_path)
        return comptage

    executer = chaine or chaine_menages.actualiser
    try:
        resultat = executer(mois_affiche=ligne.get("mois_affiche") or "",
                            acteur=ligne["acteur"], declencheur=orch.DECLENCHEUR_MANUEL,
                            etape_hostaway=etape_taches, db_path=db_path)
    except Exception as exc:   # noqa: BLE001 — toute panne devient un état lisible et réversible
        resultat = {"ok": False, "etapes": [], "echecs": [{"message": type(exc).__name__}],
                    "code": type(exc).__name__, "statistiques": []}

    etape_ha = next((e for e in resultat.get("etapes") or [] if e.get("etape") == "HOSTAWAY"),
                    None)
    reussi = bool(etape_ha and etape_ha.get("ok")) and not resultat.get("echecs")
    resume = " · ".join(resultat.get("statistiques") or [])
    if reussi:
        _terminer(refresh_id, TERMINE, db_path=db_path, synchronisation_faite=1,
                  resume=" · ".join([MESSAGES[TERMINE]] + [s for s in (resume,) if s]))
        return

    # Échec après (ou sans) activation : le jeu précédent redevient le jeu servi.
    motif = (etape_ha or {}).get("message") or resultat.get("code") or "rapprochement en échec"
    restauration = ""
    if nouveau and extraction_id:
        taches_svc.abandonner(extraction_id, motif=f"Rapprochement en échec : {motif}"[:500],
                              db_path=db_path)
        if etat_etape["active"]:
            retour = moteur.compter_taches_menage(db_path=db_path)
            for mois in resultat.get("mois_recalcules") or []:
                moteur.executer_menages_cible(mois=mois, declencheur=orch.DECLENCHEUR_MANUEL,
                                              db_path=db_path)
            restauration = "" if retour.get("ok") else " — restauration du comptage à vérifier"
    _terminer(refresh_id, ECHEC, code=resultat.get("code") or "RAPPROCHEMENT_ECHEC",
              erreur=f"{motif}{restauration}", db_path=db_path)


def _pas_interrompu(ligne, maintenant, db_path) -> None:
    """Une étape réclamée reste affichée tant que son suiveur travaille. Si plus aucun suiveur ne
    vit dans ce processus et que rien n'a bougé depuis longtemps, elle a été interrompue."""
    if suivi_vivant(ligne["refresh_id"]):
        return
    maj = _lire_iso(ligne.get("maj_le")) or _lire_iso(ligne.get("demande_le")) or maintenant()
    if (maintenant() - maj).total_seconds() < DELAI_ETAPE_INTERROMPUE_S:
        return
    from app.services import hostaway_cleaning_tasks_actualisation_service as taches_svc

    if ligne.get("extraction_id") and not ligne.get("synchronisation_faite"):
        from app.services import hostaway_cleaning_tasks_raw_service as raw

        if raw.derniere_extraction_utilisable(db_path=db_path) != ligne["extraction_id"]:
            taches_svc.abandonner(ligne["extraction_id"], motif="actualisation interrompue",
                                  db_path=db_path)
    _terminer(ligne["refresh_id"], ECHEC, code="ACTUALISATION_INTERROMPUE",
              erreur="étape interrompue (arrêt du serveur ?)", db_path=db_path)


# ── Suiveur de fond ─────────────────────────────────────────────────────────────────────────────

_SUIVEURS: dict[str, threading.Thread] = {}
_VERROU_SUIVEURS = threading.Lock()


def suivi_vivant(refresh_id: str) -> bool:
    with _VERROU_SUIVEURS:
        t = _SUIVEURS.get(refresh_id)
        return bool(t and t.is_alive())


def _boucle(refresh_id: str, db_path) -> None:
    while True:
        ligne = avancer(refresh_id, db_path=db_path)
        if not ligne or ligne.get("etat") in TERMINAUX:
            return
        time.sleep(INTERVALLE_SUIVI_S)


def assurer_suivi(refresh_id: str, *, db_path=None) -> bool:
    """Démarre le suiveur de fond s'il n'en existe pas de vivant. Appelé au clic ET à chaque
    interrogation de l'écran : après un redémarrage du serveur, le suivi reprend de lui-même."""
    with _VERROU_SUIVEURS:
        t = _SUIVEURS.get(refresh_id)
        if t and t.is_alive():
            return False
        t = threading.Thread(target=_boucle, args=(refresh_id, db_path),
                             name=f"menages-hostaway-{refresh_id}", daemon=True)
        _SUIVEURS[refresh_id] = t
        t.start()
        return True

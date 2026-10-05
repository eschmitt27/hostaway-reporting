"""Séjours hors périmètre de gestion — ce que le module Réservations sait, et ce qu'il permet de TRANCHER.

LE PROBLÈME. Le moteur range un séjour chez le propriétaire de la période de gestion qui couvre TOUTES ses
dates (`resolve_management_period`). Un séjour qui n'en trouve aucune — logement retiré, mandat terminé, séjour
à cheval sur la fin de gestion — n'a pas de propriétaire : il passe « à contrôler », sort du calcul et des
factures, et le reste tant que personne n'a tranché. Il n'y avait pourtant AUCUNE façon de trancher « ce séjour
n'est pas à nous » : seules les exclusions automatiques (séjour propriétaire, logement hors parc, statut
Hostaway) existaient. Un tel séjour bloquait donc chaque clôture mensuelle sans réponse possible.

LES RÉPONSES, selon ce que le séjour est réellement :

  · il RELEVAIT de notre gestion (il a commencé pendant la gestion et s'est terminé juste après : le ménage a
    été fait, facturé) → la période de gestion est trop courte : on la PROLONGE jusqu'à son départ, depuis la
    fiche du logement (`logements_gestion_service.prolonger_gestion`). Le séjour redevient un séjour géré ;
  · il ne relève PAS de notre gestion (réservation reçue d'Hostaway sur un logement qu'on ne gère plus) →
    on l'EXCLUT, ici, par une décision explicite : justification obligatoire, auteur, date et heure, journal en
    ajout seul, décision consultable et annulable (`exclure`, `reintegrer`) ;
  · la gestion du logement est incohérente (deux périodes simultanées, période sans propriétaire) → c'est une
    donnée à corriger, pas un séjour à trancher : on renvoie vers la fiche du logement.

CE QUE FAIT UNE EXCLUSION. Le séjour reste VISIBLE, avec son historique ; il ne produit ni commission, ni
facture, ni net propriétaire ; il cesse d'être un bloqueur de clôture. La décision est lue par le moteur
(`lot4bis`) qui classe alors le séjour « exclu » (`EXCLU_RESULTAT`, motif `EXCLUSION_DECIDEE`) — la même famille
que les autres exclusions — et par la clôture, qui ne fait que LIRE cette décision ici : ce n'est pas une case
propre à la clôture. Entre la décision et le recalcul, c'est cette lecture directe qui fait foi, pour que le
bloqueur disparaisse tout de suite.
"""
from __future__ import annotations

import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote

import app.config as cfg
from app.db.connection import get_db

# Les bibliothèques de règles vivent dans le 02_TRAVAIL du WORKTREE (cf. `controles_lot11_service`).
_TRAVAIL_DIR = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

from lib_ref_history import resolve_management_period  # noqa: E402

MISSING = "GESTION_LOGEMENT_MISSING"
OUT_OF_PERIOD = "GESTION_LOGEMENT_OUT_OF_PERIOD"
AMBIGUOUS = "GESTION_LOGEMENT_AMBIGUOUS"
MISSING_OWNER = "GESTION_LOGEMENT_MISSING_OWNER"
CODES_GESTION = (MISSING, OUT_OF_PERIOD, AMBIGUOUS, MISSING_OWNER)
#: Ce qu'on peut TRANCHER par une exclusion : les deux cas où le séjour est hors de nos périodes, pas ceux
#: où la gestion elle-même est à corriger.
CODES_DECIDABLES = (MISSING, OUT_OF_PERIOD)

DECISION_EXCLURE = "EXCLURE_PERIMETRE_GESTION"
MOTIF_MOTEUR = "EXCLUSION_DECIDEE"            # = lib_db_moteur.MOTIF_EXCLUSION_DECIDEE
ETAT_A_TRANCHER = "A_TRANCHER"
ETAT_EXCLU = "EXCLU_DECIDE"

#: Codes d'anomalie du moteur qui disent autre chose qu'un problème de gestion : le séjour n'est pas rangé
#: ici (annonce non rattachée → correspondances ; statut de parc → fiche du logement).
_CODES_AUTRES = ("LOGEMENT_NON_MAPPE", "STATUT_PARC_INVALIDE")

_MOIS_FR = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
            "octobre", "novembre", "décembre")
_CANAUX = {"AIRBNB": "Airbnb", "BOOKING": "Booking", "VRBO": "VRBO", "DIRECT": "Direct",
           "HH": "Saisie manuelle", "OWNERSTAY": "Séjour propriétaire"}


# ── Utilitaires ──────────────────────────────────────────────────────────────────────────────────

def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    """Heure LOCALE du poste, comme tout ce que la clôture trace : celle que l'utilisateur lit."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _mois(valeur: Any) -> str:
    return _txt(valeur)[:7]


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _date(iso: Any) -> date | None:
    try:
        return date.fromisoformat(_txt(iso)[:10])
    except ValueError:
        return None


def date_longue(iso: Any) -> str:
    """« 2026-09-01 » → « 1er septembre 2026 »."""
    d = _date(iso)
    if d is None:
        return _txt(iso)
    jour = "1er" if d.day == 1 else str(d.day)
    return f"{jour} {_MOIS_FR[d.month - 1]} {d.year}"


def periode_sejour(arrivee: Any, depart: Any) -> str:
    """« du 1er au 3 septembre 2026 », « du 30 octobre au 1er novembre 2026 »."""
    a, d = _date(arrivee), _date(depart)
    if a is None or d is None:
        return " → ".join(x for x in (_txt(arrivee), _txt(depart)) if x)
    jour_a = "1er" if a.day == 1 else str(a.day)
    jour_d = "1er" if d.day == 1 else str(d.day)
    if (a.year, a.month) == (d.year, d.month):
        return f"du {jour_a} au {jour_d} {_MOIS_FR[d.month - 1]} {d.year}"
    if a.year == d.year:
        return f"du {jour_a} {_MOIS_FR[a.month - 1]} au {jour_d} {_MOIS_FR[d.month - 1]} {d.year}"
    return f"du {date_longue(arrivee)} au {date_longue(depart)}"


def _noms(conn) -> tuple[dict[str, str], dict[str, str]]:
    t = _tables(conn)
    logements: dict[str, str] = {}
    proprietaires: dict[str, str] = {}
    if "ref_logements" in t:
        for r in conn.execute("SELECT logement_id, nom_court, nom_logement_officiel FROM ref_logements"):
            logements[r[0]] = _txt(r[1]) or _txt(r[2]) or "Logement"
    if "ref_proprietaires" in t:
        for r in conn.execute("SELECT proprietaire_id, prenom_proprietaire, nom_proprietaire "
                              "FROM ref_proprietaires"):
            proprietaires[r[0]] = " ".join(x for x in (_txt(r[1]), _txt(r[2])) if x) or "Propriétaire"
    return logements, proprietaires


def nom_logement(logement_id: str, *, db_path=None) -> str:
    """Nom lisible d'un logement (jamais son identifiant)."""
    conn = get_db(db_path)
    try:
        return _noms(conn)[0].get(_txt(logement_id), "Logement")
    finally:
        conn.close()


def _jeu_actif(conn) -> str | None:
    if "reservations_datasets" not in _tables(conn):
        return None
    r = conn.execute("SELECT dataset_id FROM reservations_datasets WHERE etape = 'RESOLUES' AND actif = 1 "
                     "ORDER BY rowid DESC LIMIT 1").fetchone()
    return r[0] if r else None


def _periodes(conn, logement_id: str) -> list[dict[str, str]]:
    """Périodes de gestion d'un logement, dans la forme que lit le moteur."""
    if "ref_gestion_logements_hist" not in _tables(conn):
        return []
    rows = [dict(r) for r in conn.execute(
        "SELECT gestion_id, logement_id, proprietaire_id, date_debut, date_fin, statut_gestion "
        "FROM ref_gestion_logements_hist WHERE logement_id = ?", (logement_id,))]
    rows.sort(key=lambda g: (_txt(g.get("date_debut")), _txt(g.get("date_fin")) or "9999-12-31"))
    return rows


def _statut_gestion(periodes: list[dict[str, str]], arrivee: str, depart: str, logement_id: str) -> str:
    """Ce que le moteur conclut pour ces dates : « OK », ou le code d'anomalie qu'il pose sur le séjour
    (GESTION_LOGEMENT_MISSING, _OUT_OF_PERIOD, _AMBIGUOUS, _MISSING_OWNER)."""
    statut = resolve_management_period(periodes, logement_id=logement_id, date_arrivee=arrivee,
                                       date_depart=depart).status
    return "OK" if statut == "OK" else f"GESTION_LOGEMENT_{statut}"


# ── Le constat : pourquoi ce séjour n'a pas de propriétaire ───────────────────────────────────────

def _gestion_lisible(periodes: list[dict[str, str]], proprietaires: dict[str, str]) -> list[dict[str, str]]:
    out = []
    for g in periodes:
        debut, fin = _txt(g.get("date_debut")), _txt(g.get("date_fin"))
        out.append({"debut": debut, "fin": fin,
                    "debut_fr": date_longue(debut) if debut else "origine",
                    "fin_fr": date_longue(fin) if fin else "en cours",
                    "proprietaire": proprietaires.get(_txt(g.get("proprietaire_id")), "Propriétaire non renseigné")
                    if _txt(g.get("proprietaire_id")) else "Propriétaire non renseigné"})
    return out


def _cause(statut: str, periodes: list[dict[str, str]], arrivee: str, depart: str,
           sejour_fr: str) -> tuple[str, str]:
    """(code de la cause, phrase pour l'utilisateur) — ce que le séjour a de particulier, dit clairement."""
    a = _date(arrivee)
    datees = [(_date(g.get("date_debut")), _date(g.get("date_fin"))) for g in periodes]
    if statut == OUT_OF_PERIOD:
        fins = [f for (d, f) in datees if f and a and (d is None or d <= a) and a <= f]
        fin = max(fins) if fins else None
        fin_txt = f" (qui s'arrête le {date_longue(fin)})" if fin else ""
        return OUT_OF_PERIOD, (
            f"Ce séjour {sejour_fr} commence pendant la gestion de ce logement{fin_txt} et se termine après : "
            "le propriétaire de ses dernières nuits n'est pas défini.")
    if statut == AMBIGUOUS:
        return AMBIGUOUS, (f"Deux périodes de gestion se chevauchent aux dates de ce séjour {sejour_fr} : on ne "
                           "sait pas à quel propriétaire l'attribuer.")
    if statut == MISSING_OWNER:
        return MISSING_OWNER, (f"La période de gestion des dates de ce séjour {sejour_fr} n'a pas de "
                               "propriétaire.")
    # MISSING : aucune période ne couvre l'arrivée — après la fin, avant le début, dans un trou, ou aucune période.
    if not periodes:
        return MISSING, "Aucune période de gestion n'est enregistrée pour ce logement."
    fins = [f for (_, f) in datees if f]
    debuts = [d for (d, _) in datees if d]
    if a and fins and not any(f is None for (_, f) in datees) and a > max(fins):
        return MISSING, (f"Ce séjour {sejour_fr} se situe après la fin de la gestion enregistrée pour ce "
                         f"logement ({date_longue(max(fins))}) : le logement n'est plus dans votre périmètre à "
                         "ces dates.")
    if a and debuts and a < min(debuts):
        return MISSING, (f"Ce séjour {sejour_fr} est antérieur au début de la gestion enregistrée pour ce "
                         f"logement ({date_longue(min(debuts))}).")
    return MISSING, (f"Ce séjour {sejour_fr} tombe dans un intervalle où aucune période de gestion n'est "
                     "enregistrée pour ce logement.")


def _prolongation(periodes: list[dict[str, str]], statut: str, arrivee: str, depart: str) -> str:
    """Date jusqu'à laquelle prolonger la gestion pour que ce séjour soit couvert, ou chaîne vide.

    Possible seulement si le séjour COMMENCE dans une période close (dernière du logement) et se termine
    après sa fin, sans qu'une autre période ne démarre avant son départ."""
    if statut != OUT_OF_PERIOD:
        return ""
    a, d = _date(arrivee), _date(depart)
    if a is None or d is None:
        return ""
    candidates = [g for g in periodes if _date(g.get("date_fin")) is not None
                  and (_date(g.get("date_debut")) is None or _date(g["date_debut"]) <= a) and a <= _date(g["date_fin"])]
    if len(candidates) != 1:
        return ""
    fin = _date(candidates[0]["date_fin"])
    if fin is None or d <= fin:
        return ""
    for g in periodes:
        debut_g = _date(g.get("date_debut"))
        if g is not candidates[0] and debut_g is not None and debut_g > fin and debut_g <= d:
            return ""                        # une autre période démarre avant le départ : changement de gestion
    return d.isoformat()


# ── Lecture ──────────────────────────────────────────────────────────────────────────────────────

def _decisions_actives(conn) -> dict[str, dict[str, Any]]:
    if "reservation_perimetre_decisions" not in _tables(conn):
        return {}
    return {r["reservation_id_hostaway"]: dict(r) for r in conn.execute(
        "SELECT * FROM reservation_perimetre_decisions WHERE statut = 'ACTIVE'")}


def _decision_lisible(d: dict[str, Any] | None) -> dict[str, Any] | None:
    if not d:
        return None
    return {**d, "date_decision_fr": _horodatage_fr(d.get("date_decision")),
            "date_annulation_fr": _horodatage_fr(d.get("date_annulation"))}


def _horodatage_fr(valeur: Any) -> str:
    s = _txt(valeur).replace("T", " ").replace("Z", "")
    if not s:
        return ""
    d = _date(s[:10])
    jour = d.strftime("%d/%m/%Y") if d else s[:10]
    heure = s[11:16]
    return f"{jour} à {heure.replace(':', 'h')}" if len(heure) == 5 else jour


def sejours_hors_gestion(mois: str = "", *, logement_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Séjours du jeu de calcul actif qui n'ont pas de propriétaire exploitable, ou qu'une décision a exclus.

    `mois` vide : tous les mois de la comptabilité V1. Chaque séjour dit son état — À TRANCHER, ou EXCLU PAR
    DÉCISION (la décision active, ou le motif posé par le moteur au dernier calcul) —, pourquoi il est là, et
    ce que l'utilisateur peut en faire. Lu dans les sources, jamais copié : une décision annulée, une période
    prolongée, un recalcul le font bouger tout seul."""
    from app.services import perimetre_v1_service as v1

    mois = _mois(mois)
    if mois and v1.est_anterieur(mois, db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        t = _tables(conn)
        if "reservations_resolues" not in t:
            return []
        jeu = _jeu_actif(conn)
        sql = ("SELECT reservation_calc_id, reservation_id_hostaway, reservation_hh_id, source, canal, mois, "
               "logement_id, proprietaire_id, date_arrivee, date_depart, nuits, guest_count, montant_retenu, "
               "statut_controle, code_anomalie, motif_exclusion FROM reservations_resolues WHERE "
               "((statut_controle = 'A_CONTROLER' AND COALESCE(proprietaire_id, '') = '' "
               "AND COALESCE(logement_id, '') <> '' "
               "AND COALESCE(code_anomalie, '') NOT IN ('LOGEMENT_NON_MAPPE', 'STATUT_PARC_INVALIDE')) "
               "OR motif_exclusion = ?)")
        params: list[Any] = [MOTIF_MOTEUR]
        if jeu:
            sql, params = sql + " AND dataset_id = ?", params + [jeu]
        if mois:
            sql, params = sql + " AND mois = ?", params + [mois]
        if _txt(logement_id):
            sql, params = sql + " AND logement_id = ?", params + [_txt(logement_id)]
        rows = [dict(r) for r in conn.execute(sql + " ORDER BY mois, logement_id, date_arrivee", params)]
        decisions = _decisions_actives(conn)
        noms_logements, noms_proprietaires = _noms(conn)
        cache: dict[str, list[dict[str, str]]] = {}
        etats_logement: dict[str, dict[str, str]] = {}
        if "ref_logements" in t:
            etats_logement = {r[0]: {"actif": _txt(r[1]).upper(), "statut_parc": _txt(r[2]).upper()}
                              for r in conn.execute("SELECT logement_id, actif, statut_parc FROM ref_logements")}
        premier = v1.premier_mois(db_path=db_path)
        sejours = []
        for r in rows:
            if not mois and premier and _txt(r["mois"]) < premier:
                continue
            lid = _txt(r["logement_id"])
            periodes = cache.setdefault(lid, _periodes(conn, lid))
            arrivee, depart = _txt(r["date_arrivee"])[:10], _txt(r["date_depart"])[:10]
            statut = _statut_gestion(periodes, arrivee, depart, lid)
            rid = _txt(r["reservation_id_hostaway"])
            decision = decisions.get(rid) if rid else None
            if statut == "OK" and decision is None:
                # Couvert par la gestion d'aujourd'hui et sans décision d'exclusion (jamais prise, ou annulée) : ce
                # séjour n'est plus hors gestion — le prochain calcul le rendra géré. Pas notre sujet.
                continue
            sejour_fr = periode_sejour(arrivee, depart)
            if statut == "OK":
                # La gestion couvre désormais ce séjour, mais une décision l'a exclu : c'est elle qui fait foi
                # tant qu'elle n'est pas annulée.
                code = _txt((decision or {}).get("code_constat")) or MISSING
                cause = (f"Ce séjour {sejour_fr} a été exclu du périmètre de gestion ; la gestion du logement "
                         "le couvre aujourd'hui, la décision d'exclusion reste prioritaire tant qu'elle n'est "
                         "pas annulée.")
            else:
                # Le dernier mot du moteur (le code posé sur le séjour) fait foi pour NOMMER la cause ; la lecture
                # des périodes d'aujourd'hui sert à savoir si le problème existe encore (une gestion prolongée
                # depuis le dernier calcul n'en est plus un) et à proposer l'action.
                code_moteur = _txt(r["code_anomalie"])
                code = code_moteur if code_moteur in CODES_GESTION else statut
                code, cause = _cause(code, periodes, arrivee, depart, sejour_fr)
            # Seul un séjour reçu d'Hostaway, sans saisie manuelle associée, se décide ici : c'est la branche
            # du moteur qui lit la décision.
            est_hostaway = (bool(rid) and not _txt(r["reservation_hh_id"])
                            and _txt(r["source"]).upper().startswith("HOSTAWAY"))
            decidable = est_hostaway and code in CODES_DECIDABLES
            jusqu_au = _prolongation(periodes, statut, arrivee, depart) if est_hostaway else ""
            exclu = decision is not None
            etat_log = etats_logement.get(lid, {})
            sejours.append({
                "cle": _txt(r["reservation_calc_id"]), "reservation_id": rid,
                "reservation_hh_id": _txt(r["reservation_hh_id"]),
                "est_hostaway": est_hostaway,
                "mois": _txt(r["mois"]), "logement_id": lid,
                "logement": noms_logements.get(lid, "Logement"),
                "arrivee": arrivee, "depart": depart, "nuits": r["nuits"],
                "periode_fr": sejour_fr, "voyageurs": r["guest_count"],
                "canal": _CANAUX.get(_txt(r["canal"]).upper(), _txt(r["canal"]).title()),
                "montant": r["montant_retenu"] if isinstance(r["montant_retenu"], (int, float)) else None,
                "code": code, "cause": cause,
                "decidable": decidable, "couvert": statut == "OK",
                "gestion": _gestion_lisible(periodes, noms_proprietaires),
                "prolongation_jusqu_au": jusqu_au,
                "prolongation_jusqu_au_fr": date_longue(jusqu_au) if jusqu_au else "",
                "logement_archive": etat_log.get("actif") == "NON",
                "etat": ETAT_EXCLU if exclu else ETAT_A_TRANCHER,
                "decision": _decision_lisible(decision),
            })
        return sejours
    finally:
        conn.close()


def a_trancher(mois: str = "", *, db_path=None) -> list[dict[str, Any]]:
    return [s for s in sejours_hors_gestion(mois, db_path=db_path) if s["etat"] == ETAT_A_TRANCHER]


def sejour(cle: str, *, db_path=None) -> dict[str, Any] | None:
    """Un séjour par sa clé économique (n'importe quel mois), ou None."""
    for s in sejours_hors_gestion("", db_path=db_path):
        if s["cle"] == _txt(cle):
            return s
    return None


def couverts_par_prolongation(logement_id: str, jusqu_au: str, *, db_path=None) -> list[dict[str, Any]]:
    """Séjours de ce logement qu'une prolongation de la gestion jusqu'à `jusqu_au` rendrait gérés : ceux qui
    commencent dans sa dernière période close et se terminent au plus tard à cette date."""
    fin = _date(jusqu_au)
    if fin is None:
        return []
    return [s for s in sejours_hors_gestion("", logement_id=logement_id, db_path=db_path)
            if s["prolongation_jusqu_au"] and (_date(s["depart"]) or date.max) <= fin]


def decision_active(reservation_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        return _decision_lisible(_decisions_actives(conn).get(_txt(reservation_id)))
    finally:
        conn.close()


def reservations_exclues(*, db_path=None) -> set[str]:
    """Identifiants Hostaway des séjours dont l'exclusion est décidée et ACTIVE — ce que lit le moteur."""
    conn = get_db(db_path)
    try:
        return set(_decisions_actives(conn))
    finally:
        conn.close()


def historique(reservation_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Journal des décisions d'un séjour, du plus récent au plus ancien."""
    conn = get_db(db_path)
    try:
        if "reservation_perimetre_evenements" not in _tables(conn):
            return []
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM reservation_perimetre_evenements WHERE reservation_id_hostaway = ? "
            "ORDER BY id DESC", (_txt(reservation_id),))]
    finally:
        conn.close()
    for r in rows:
        r["date_evenement_fr"] = _horodatage_fr(r.get("date_evenement"))
        r["libelle"] = "Exclu du périmètre de gestion" if r["type_evenement"] == "EXCLUSION" \
            else "Réintégré au périmètre de gestion"
    return rows


# ── Décision ─────────────────────────────────────────────────────────────────────────────────────

def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _refus_verrou(mois: str, *, db_path=None) -> str:
    from app.services import cloture_verrous_service as verrous
    return verrous.refus(mois, "RESERVATIONS", db_path=db_path)


def _invalider_calculs(*, db_path=None) -> None:
    """Marque les calculs périmés jusqu'à une actualisation (jamais recalculés ici). Même mécanisme que tout
    changement de référentiel."""
    from app.services import orchestrateur_dag as dag
    from app.services import orchestrateur_service as orch
    orch.invalider_descendants(dag.REF_SETUP, db_path=db_path)


# QUAND UNE DÉCISION PÉRIME LES CALCULS. Un séjour HORS de toute période de gestion n'a pas de propriétaire : il ne
# pèse déjà sur AUCUN chiffre (ni commission, ni net, ni facture). L'exclure — ou annuler cette exclusion — ne change
# donc aucun résultat : seule son étiquette (« à contrôler » → « exclu ») change, au prochain calcul, et la clôture
# comme la page des Réservations lisent la décision EN DIRECT. Imposer une actualisation complète pour cela
# ferait réapparaître, juste après la décision, des « calculs à actualiser » sans raison.
# Un séjour que la gestion COUVRE aujourd'hui (la décision prime sur la période) est autre chose : l'exclure le sort de
# la facturation, annuler l'exclusion l'y remet — les chiffres bougent, les calculs sont périmés.


def exclure(cle: str, *, justification: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Décide qu'un séjour ne relève PAS de la gestion : exclu du périmètre, sans facturation.

    Refusé — message métier, jamais une erreur SQL — si : la justification est vide ; le séjour est
    introuvable, déjà exclu, ou n'est pas un séjour Hostaway hors de nos périodes de gestion ; son mois
    précède la comptabilité V1 ; le module Réservations est clôturé pour son mois."""
    from app.services import perimetre_v1_service as v1

    justification = _txt(justification)
    if not justification:
        return _refus("JUSTIFICATION_OBLIGATOIRE",
                      "Une justification est obligatoire pour exclure un séjour du périmètre de gestion.")
    s = sejour(cle, db_path=db_path)
    if s is None:
        return _refus("SEJOUR_INTROUVABLE", "Ce séjour n'est plus à trancher : il n'est plus hors de la gestion.")
    if s["etat"] == ETAT_EXCLU:
        return _refus("DEJA_EXCLU", "Ce séjour est déjà exclu du périmètre de gestion.")
    if not s["decidable"]:
        return _refus("NON_DECIDABLE",
                      "Ce séjour ne s'exclut pas ici : sa période de gestion est à corriger depuis la fiche "
                      "du logement (ou, pour une saisie manuelle, depuis la réservation).")
    if v1.est_anterieur(s["mois"], db_path=db_path):
        return _refus("AVANT_V1", v1.message_cloture(s["mois"], db_path=db_path))
    verrou = _refus_verrou(s["mois"], db_path=db_path)
    if verrou:
        return _refus("MOIS_CLOTURE", verrou)

    maintenant = _now()
    decision_id = "DEC-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM reservation_perimetre_decisions WHERE reservation_id_hostaway = ? "
                        "AND statut = 'ACTIVE'", (s["reservation_id"],)).fetchone():
            conn.rollback()
            return _refus("DEJA_EXCLU", "Ce séjour est déjà exclu du périmètre de gestion.")
        conn.execute(
            "INSERT INTO reservation_perimetre_decisions (decision_id, reservation_calc_id, "
            "reservation_id_hostaway, logement_id, mois, date_arrivee, date_depart, canal, montant_retenu, "
            "code_constat, decision, statut, justification, acteur, date_decision) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?,?)",
            (decision_id, s["cle"], s["reservation_id"], s["logement_id"], s["mois"], s["arrivee"], s["depart"],
             s["canal"], s["montant"], s["code"], DECISION_EXCLURE, justification, acteur or None, maintenant))
        conn.execute(
            "INSERT INTO reservation_perimetre_evenements (decision_id, reservation_id_hostaway, mois, "
            "type_evenement, ancien_etat, nouvel_etat, justification, acteur, date_evenement) "
            "VALUES (?,?,?,'EXCLUSION',?,?,?,?,?)",
            (decision_id, s["reservation_id"], s["mois"], ETAT_A_TRANCHER, ETAT_EXCLU, justification,
             acteur or None, maintenant))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    if s["couvert"]:
        _invalider_calculs(db_path=db_path)
    return {"ok": True, "decision_id": decision_id, "mois": s["mois"], "reservation_id": s["reservation_id"]}


def reintegrer(cle: str, *, justification: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Annule une exclusion : le séjour redevient « à trancher ». Rien n'est effacé — la décision d'origine
    reste, marquée annulée, et le journal ajoute la réintégration."""
    justification = _txt(justification)
    if not justification:
        return _refus("JUSTIFICATION_OBLIGATOIRE",
                      "Une justification est obligatoire pour réintégrer un séjour au périmètre de gestion.")
    conn = get_db(db_path)
    try:
        t = _tables(conn)
        if "reservation_perimetre_decisions" not in t:
            return _refus("DECISION_INTROUVABLE", "Aucune décision d'exclusion pour ce séjour.")
        d = conn.execute("SELECT * FROM reservation_perimetre_decisions WHERE reservation_calc_id = ? "
                         "AND statut = 'ACTIVE'", (_txt(cle),)).fetchone()
    finally:
        conn.close()
    if d is None:
        return _refus("DECISION_INTROUVABLE", "Aucune décision d'exclusion active pour ce séjour.")
    d = dict(d)
    verrou = _refus_verrou(d["mois"], db_path=db_path)
    if verrou:
        return _refus("MOIS_CLOTURE", verrou)
    avant = sejour(cle, db_path=db_path)            # lu AVANT l'annulation : c'est elle qui peut le rendre facturable
    couvert = bool(avant and avant["couvert"])

    maintenant = _now()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "UPDATE reservation_perimetre_decisions SET statut = 'ANNULEE', annulee_par = ?, "
            "date_annulation = ?, justification_annulation = ? WHERE decision_id = ? AND statut = 'ACTIVE'",
            (acteur or None, maintenant, justification, d["decision_id"]))
        if cur.rowcount != 1:
            conn.rollback()
            return _refus("DECISION_INTROUVABLE", "Cette décision a déjà été annulée.")
        conn.execute(
            "INSERT INTO reservation_perimetre_evenements (decision_id, reservation_id_hostaway, mois, "
            "type_evenement, ancien_etat, nouvel_etat, justification, acteur, date_evenement) "
            "VALUES (?,?,?,'REINTEGRATION',?,?,?,?,?)",
            (d["decision_id"], d["reservation_id_hostaway"], d["mois"], ETAT_EXCLU, ETAT_A_TRANCHER,
             justification, acteur or None, maintenant))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    if couvert:
        _invalider_calculs(db_path=db_path)
    return {"ok": True, "decision_id": d["decision_id"], "mois": d["mois"]}


def lien_page(mois: str = "", ancre: str = "") -> str:
    """La page où se traitent ces séjours — là où mène « Traiter » depuis la clôture."""
    lien = "/reservations/hors-gestion" + (f"?mois={quote(_mois(mois))}" if _mois(mois) else "")
    return lien + (f"#{ancre}" if ancre else "")

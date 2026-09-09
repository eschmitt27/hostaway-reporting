"""« Actualiser le rapprochement des ménages » — LE workflow du bouton unique de l'écran /menages.

UN SEUL BOUTON, UN SEUL CHEMIN, SYNCHRONE
L'écran ne propose plus qu'une action. Elle exécute la chaîne COMPLÈTE, EN LIGNE, et ne rend la
main qu'une fois terminée : au retour de la requête, l'écran affiche l'état réel. Le fonctionnement
précédent (`BackgroundTasks`) rendait la main immédiatement et redirigeait vers un écran encore
périmé, ce qui obligeait l'utilisateur à deviner quand rafraîchir — et avait imposé un second bouton
« Actualiser l'affichage », depuis supprimé. Un traitement de fond sans écran de suivi est un
traitement dont l'utilisateur ne sait rien : ici, l'attente est assumée et lisible.

ORDRE IMPOSÉ (chaque étape alimente la suivante)
  1. PDF          — factures de ménage externes déposées (hash : nouveau / inchangé / V1→V2)
  2. GOOGLE SHEET — déclarations internes, via lot6b `--sans-excel` (SQLite -> Sheet -> SQLite)
  3. HOSTAWAY     — Cleaning Tasks, lecture seule de l'API
  4. CIBLAGE      — mois RÉELLEMENT impactés, déduits des diffs des trois sources
  5/6. RECALCUL   — mois OUVERTS uniquement ; un mois CLÔTURÉ est TRACÉ, jamais recalculé
  7. RAPPROCHEMENT— lot6d/6e/6f puis cascade Lot9→Lot12 pour les mois retenus

MOIS CLÔTURÉS
Un mois clôturé dont une source a changé n'est pas ignoré : il est inscrit dans
`menages_changements_mois_clotures`, et ses chiffres économiques restent rigoureusement inchangés.
Rouvrir la clôture reste une décision humaine explicite (`clotures_service.rouvrir`), jamais un
effet de bord d'un clic sur « Actualiser ».

ZÉRO EXCEL
Aucune étape n'ouvre de classeur : lot6b/6d/6e/6f sont invoqués en `--source SQLITE --sans-excel`,
l'import PDF écrit en base, et Hostaway est une API.
"""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

from app.db.connection import get_db

ORIGINE_PDF = "PDF"
ORIGINE_SHEET = "GOOGLE_SHEET"
ORIGINE_HOSTAWAY = "HOSTAWAY"

# Portée du verrou DB (réutilise `orchestrateur_service.prendre_verrou`/`liberer_verrou`, §36 — bail
# expirable en base, PAS un `threading.Lock` en mémoire : un verrou mémoire ne protégerait pas contre
# deux workers/process distincts, et ne survivrait pas à un crash. Portée DISTINCTE de
# `orch.PORTEE_GLOBALE` : l'étape 7 de ce workflow appelle `orch.actualiser(...)`, qui prend elle-même
# `PORTEE_GLOBALE` avec un `run_id` différent — les confondre ferait échouer cet appel imbriqué avec
# « verrou déjà pris » sur son propre appelant.
PORTEE_ACTUALISATION = "MENAGES_ACTUALISATION_BOUTON"

E_DEJA_EN_COURS = "MENAGES_ACTUALISATION_DEJA_EN_COURS"
E_HOSTAWAY_CREDENTIALS_ABSENTES = "MENAGES_HOSTAWAY_CREDENTIALS_ABSENTES"
E_GOOGLE_SHEET_CONFIG_ABSENTE = "MENAGES_GOOGLE_SHEET_CONFIG_ABSENTE"

STATUT_SUCCES = "SUCCES"
STATUT_PARTIEL = "PARTIEL"
STATUT_ECHEC = "ECHEC"


def _resultat_refus(mois_affiche: str, *, code: str, message: str) -> dict[str, Any]:
    """Forme commune d'un refus précoce (verrou pris / préflight KO) — AVANT toute étape.

    Toujours le même contrat que le workflow complet (`ok`, `statut`, `etapes`, ...) : la route et les
    gabarits n'ont jamais besoin de distinguer « refus précoce » de « échec en cours de route »."""
    return {"ok": False, "statut": STATUT_ECHEC, "mois_affiche": mois_affiche, "etapes": [],
            "mois_recalcules": [], "mois_clotures_signales": [], "echecs": [], "code": code,
            "message": message, "statistiques": [message]}


def _google_sheet_config_ok(*, db_path=None) -> bool:
    """Préflight SANS sous-processus : même critère que `lot6b_m04_menages_internes.py`
    (`_url_src011_depuis_sqlite`), vérifié ici en SQLite pur pour éviter de lancer le script pour
    échouer immédiatement dessus (mission « préflight avant le sous-processus Google Sheet »)."""
    conn = get_db(db_path)
    try:
        if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='ref_sources_systeme'"
        ).fetchone() is None:
            return False
        r = conn.execute(
            "SELECT dossier_source FROM ref_sources_systeme "
            "WHERE nom_source='GOOGLE_SHEET_M04_DECLARATIONS' AND actif='OUI'").fetchone()
    except Exception:  # noqa: BLE001
        return False
    finally:
        conn.close()
    if r is None:
        return False
    return str(r["dossier_source"] or "").strip().startswith("http")


def _statut_global(etapes: list[dict[str, Any]], echecs: list[dict[str, Any]]) -> str:
    """SUCCÈS seulement si les TROIS étapes sources ont réussi ET aucun mois n'a échoué au recalcul
    (mission §8 : « Sheet OK, PDF OK, Hostaway KO -> PAS SUCCÈS »). ÉCHEC seulement si AUCUNE étape
    source n'a abouti — sinon PARTIEL, jamais présenté comme un succès."""
    if all(e["ok"] for e in etapes) and not echecs:
        return STATUT_SUCCES
    if etapes and not any(e["ok"] for e in etapes):
        return STATUT_ECHEC
    return STATUT_PARTIEL


def _source_en_echec(etapes: list[dict[str, Any]], echecs: list[dict[str, Any]]) -> str:
    sources = [e["etape"] for e in etapes if not e["ok"]]
    if echecs:
        sources.append("RAPPROCHEMENT")
    return ",".join(sources) or "inconnue"


# ── Empreintes par mois : ce qui permet de savoir ce qui a RÉELLEMENT changé ─────────────────────
# Comparer un avant/après par mois est ce qui distingue « une source a été relue » de « une source a
# changé ». Sans cela, chaque clic recalculerait tous les mois — y compris ceux que personne n'a
# touchés — et l'idempotence serait perdue.

_EMPREINTES = {
    ORIGINE_SHEET: (
        "SELECT mois AS mois, COUNT(*) AS n, GROUP_CONCAT(row_hash) AS h "
        "FROM menages_declarations_internes GROUP BY mois"),
    ORIGINE_HOSTAWAY: (
        "SELECT substr(can_start_from, 1, 7) AS mois, COUNT(*) AS n, GROUP_CONCAT(row_hash) AS h "
        "FROM hostaway_cleaning_tasks WHERE can_start_from IS NOT NULL "
        "GROUP BY substr(can_start_from, 1, 7)"),
}


def empreintes(origine: str, *, db_path=None) -> dict[str, str]:
    """Empreinte stable par mois. Une table absente rend un dictionnaire vide, pas une erreur."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(_EMPREINTES[origine]).fetchall()
    except Exception:  # noqa: BLE001
        return {}
    finally:
        conn.close()
    out: dict[str, str] = {}
    for r in rows:
        mois = str(r["mois"] or "").strip()
        if len(mois) != 7:
            continue
        brut = f"{r['n']}|{r['h'] or ''}"
        out[mois] = hashlib.sha256(brut.encode("utf-8")).hexdigest()[:16]
    return out


def _mois_modifies(avant: dict[str, str], apres: dict[str, str]) -> list[str]:
    """Mois dont l'empreinte a bougé, apparu ou disparu."""
    return sorted({m for m in set(avant) | set(apres) if avant.get(m) != apres.get(m)})


def signaler_mois_cloture(mois: str, origine: str, detail: str, *, acteur: str = "",
                          db_path=None) -> None:
    """Trace un changement détecté sur un mois clôturé. AUCUN recalcul n'en découle."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_changements_mois_clotures (mois, origine, detail, acteur) "
            "VALUES (?,?,?,?)", (mois, origine, detail, acteur or "ui:menages"))
        conn.commit()
    finally:
        conn.close()


def changements_mois_clotures(*, statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        sql = "SELECT * FROM menages_changements_mois_clotures"
        params: list[Any] = []
        if statut:
            sql += " WHERE statut=?"
            params.append(statut)
        sql += " ORDER BY id DESC"
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception:  # noqa: BLE001
        return []
    finally:
        conn.close()


# ── Le workflow ─────────────────────────────────────────────────────────────────────────────────

def actualiser(*, mois_affiche: str = "", acteur: str = "ui:menages",
               declencheur: str = "MANUEL", db_path=None) -> dict[str, Any]:
    """Exécute la chaîne complète, SYNCHRONE, et rend des statistiques RÉELLES.

    Les compteurs rendus sont ceux des opérations effectivement réalisées — jamais une estimation,
    jamais une valeur figée dans le gabarit : un message qui annonce « 1 nouvelle facture » quand
    rien n'a été importé est pire qu'une absence de message.
    """
    from app.services import hostaway_cleaning_tasks_actualisation_service as hostaway_ct
    from app.services import menages_declarations_service as decl
    from app.services import menages_pdf_import_service as pdf_import
    from app.services import menages_service as svc
    from app.services import orchestrateur_moteur as moteur
    from app.services import orchestrateur_service as orch
    from app.services import run_history_service as history

    mois_affiche = str(mois_affiche or "").strip() or svc.periode_par_defaut()

    # ── Verrou (§36, réutilisé) — un clic pendant qu'une actualisation tourne déjà ne relance PAS
    # une seconde chaîne : refus propre, jamais deux Hostaway/Sheet concurrents sur la même base.
    # Verrou EN BASE (pas un `threading.Lock`) : protège aussi contre deux workers/process distincts,
    # et un bail expirable évite qu'un crash laisse un verrou orphelin pour toujours.
    run_id = uuid.uuid4().hex[:12]
    verrou = orch.prendre_verrou(PORTEE_ACTUALISATION, run_id, db_path=db_path)
    if not verrou.get("ok"):
        return _resultat_refus(
            mois_affiche, code=E_DEJA_EN_COURS,
            message="Une actualisation des ménages est déjà en cours. Réessayez dans un instant.")

    try:
        history_run_id = history.demarrer("MENAGES_ACTUALISATION", acteur=acteur, db_path=db_path)

        # ── Préflight Hostaway — ARRÊT IMMÉDIAT avant même le PDF/Sheet si les identifiants sont
        # absents (mission §3 : « Hostaway non configuré -> arrêt immédiat », pas un échec tardif
        # après un PDF+Sheet inutile). Défense en profondeur gardée : `hostaway_ct.actualiser()`
        # revérifie la même chose en interne, à l'étape 3 ci-dessous.
        if not hostaway_ct.credentials_disponibles():
            message = ("Actualisation impossible : " + hostaway_ct.MESSAGES[
                hostaway_ct.E_CREDENTIALS_ABSENTES])
            if history_run_id:
                history.marquer_echec(
                    history_run_id,
                    erreur=f"{STATUT_ECHEC}: préflight HOSTAWAY credentials absentes", db_path=db_path)
            return _resultat_refus(mois_affiche, code=hostaway_ct.E_CREDENTIALS_ABSENTES,
                                   message=message)

        etapes: list[dict[str, Any]] = []

        # ── 1. PDF ──────────────────────────────────────────────────────────────────────────
        # Déjà idempotent et sans IA : le hash décide seul si un fichier est nouveau, inchangé, ou
        # une nouvelle version d'une facture déjà connue.
        pdf = pdf_import.importer_nouveaux(acteur=acteur, db_path=db_path)
        etapes.append({"etape": "PDF", "ok": bool(pdf.get("ok")),
                       "nb_importees": pdf.get("nb_importees", 0),
                       "nb_remplacees": pdf.get("nb_remplacees", 0),
                       "nb_deja_importees": pdf.get("nb_deja_importees", 0),
                       "mois_impactes": pdf.get("mois_impactes", [])})

        # ── 2. Google Sheet (lot6b) ─────────────────────────────────────────────────────────
        # Préflight SANS sous-processus (mission §3) : une config SRC_011 absente/mal formée est
        # détectée en SQLite pur, AVANT de lancer lot6b pour échouer immédiatement dessus.
        if not _google_sheet_config_ok(db_path=db_path):
            mois_sheet: list[str] = []
            etapes.append({
                "etape": "GOOGLE_SHEET", "ok": False, "mois_impactes": [],
                "message": "Configuration Google Sheet indisponible (SRC_011 absent/invalide dans "
                           "ref_sources_systeme) : sous-processus non lancé."})
        else:
            sheet_avant = empreintes(ORIGINE_SHEET, db_path=db_path)
            sheet = moteur.executer_declarations_internes(db_path=db_path)
            sheet_apres = empreintes(ORIGINE_SHEET, db_path=db_path)
            mois_sheet = _mois_modifies(sheet_avant, sheet_apres)
            etapes.append({"etape": "GOOGLE_SHEET", "ok": bool(sheet.get("ok")),
                           "message": sheet.get("message", ""), "mois_impactes": mois_sheet})

        # ── 3. Hostaway Cleaning Tasks (lecture seule) ─────────────────────────────────────────
        hostaway_avant = empreintes(ORIGINE_HOSTAWAY, db_path=db_path)
        hostaway = orch.recalculer_dataset("HOSTAWAY_CLEANING_TASKS", declencheur=declencheur,
                                           db_path=db_path)
        hostaway_apres = empreintes(ORIGINE_HOSTAWAY, db_path=db_path)
        mois_hostaway = _mois_modifies(hostaway_avant, hostaway_apres)
        etapes.append({"etape": "HOSTAWAY", "ok": bool((hostaway or {}).get("ok", True)),
                       "mois_impactes": mois_hostaway})

        # ── 4. Ciblage ──────────────────────────────────────────────────────────────────────
        # Le mois affiché est TOUJOURS retenu : l'utilisateur qui clique sur cet écran demande
        # d'abord que CE mois-là soit à jour, même si aucune source n'a bougé ailleurs.
        candidats = sorted(
            {mois_affiche, *pdf.get("mois_impactes", []), *mois_sheet, *mois_hostaway} - {""})

        # ── 5/6. Recalcul des mois OUVERTS, traçage des mois CLÔTURÉS ─────────────────────────
        mois_recalcules: list[str] = []
        mois_clotures: list[str] = []
        echecs: list[dict[str, Any]] = []
        for mois in candidats:
            if decl.mois_cloture(mois, db_path=db_path):
                mois_clotures.append(mois)
                origines = [o for o, liste in ((ORIGINE_PDF, pdf.get("mois_impactes", [])),
                                               (ORIGINE_SHEET, mois_sheet),
                                               (ORIGINE_HOSTAWAY, mois_hostaway)) if mois in liste]
                for origine in origines:
                    signaler_mois_cloture(
                        mois, origine,
                        "changement détecté sur un mois clôturé : aucun recalcul économique effectué",
                        acteur=acteur, db_path=db_path)
                continue

            resultat = moteur.executer_menages_cible(mois=mois, declencheur=declencheur,
                                                     db_path=db_path)
            if resultat.get("ok"):
                mois_recalcules.append(mois)
                orch.marquer_dataset("MENAGES", orch.ST_A_JOUR, declencheur=declencheur,
                                     detail=resultat, motif=f"RECALCUL_CIBLE mois={mois}",
                                     db_path=db_path)
            else:
                echecs.append({"mois": mois, "code": resultat.get("code"),
                               "message": resultat.get("message", "")})
                orch.marquer_dataset("MENAGES", orch.ST_ECHEC, declencheur=declencheur,
                                     erreur_code=resultat.get("code", "ECHEC"),
                                     erreur_message=resultat.get("message", ""),
                                     motif=f"RECALCUL_CIBLE mois={mois}", db_path=db_path)

        # ── 7. Rapprochement aval ─────────────────────────────────────────────────────────────
        # lot6d a déjà reconstruit le rapprochement ; la cascade propage aux résultats économiques.
        # Elle n'est déclenchée que si un mois a réellement été recalculé : sans changement, aucun
        # recalcul aval — c'est ce qui rend l'action idempotente.
        if mois_recalcules:
            orch.actualiser(cibles=["FLUX_LOT9"], declencheur=declencheur,
                            inclure_imports_externes=True, db_path=db_path)
        svc.invalidate_menages_cache()

        statut = _statut_global(etapes, echecs)
        resultat_final = {
            "ok": statut == STATUT_SUCCES,
            "statut": statut,
            "mois_affiche": mois_affiche,
            "etapes": etapes,
            "mois_recalcules": mois_recalcules,
            "mois_clotures_signales": mois_clotures,
            "echecs": echecs,
            "statistiques": _statistiques(pdf, mois_recalcules, mois_clotures, statut=statut,
                                          etapes=etapes, echecs=echecs),
        }
        if history_run_id:
            if statut == STATUT_SUCCES:
                history.marquer_succes(history_run_id, db_path=db_path)
            else:
                source = _source_en_echec(etapes, echecs)
                history.marquer_echec(history_run_id, erreur=f"{statut}: {source}", db_path=db_path)
        return resultat_final
    finally:
        orch.liberer_verrou(PORTEE_ACTUALISATION, run_id, db_path=db_path)


def _statistiques(pdf: dict[str, Any], mois_recalcules: list[str],
                  mois_clotures: list[str], *, statut: str = STATUT_SUCCES,
                  etapes: list[dict[str, Any]] | None = None,
                  echecs: list[dict[str, Any]] | None = None) -> list[str]:
    """1 à 3 phrases courtes, TOUTES dérivées de ce que le workflow a réellement fait.

    Aucune ligne n'est produite pour un compteur à zéro : « 0 nouvelle facture » n'apprend rien et
    noie la seule information utile.

    Si `statut` n'est pas SUCCÈS, la PREMIÈRE ligne annonce honnêtement l'échec/le partiel (mission
    §8) — jamais un résumé qui ressemble à un succès quand une source a échoué.
    """
    lignes: list[str] = []
    if statut != STATUT_SUCCES:
        source = _source_en_echec(etapes or [], echecs or [])
        if statut == STATUT_ECHEC:
            lignes.append(f"Actualisation en échec : {source} n'a pas pu être actualisé.")
        else:
            lignes.append(f"Actualisation partielle : {source} en échec, le reste a été actualisé.")
    nouvelles = int(pdf.get("nb_importees", 0) or 0)
    remplacees = int(pdf.get("nb_remplacees", 0) or 0)
    if nouvelles:
        lignes.append(f"{nouvelles} nouvelle facture" + ("s" if nouvelles > 1 else ""))
    if remplacees:
        lignes.append(f"{remplacees} facture" + ("s" if remplacees > 1 else "")
                      + " remplacée" + ("s" if remplacees > 1 else ""))
    if mois_recalcules:
        n = len(mois_recalcules)
        lignes.append(f"{n} mois mis à jour ({', '.join(mois_recalcules)})")
    if mois_clotures:
        n = len(mois_clotures)
        lignes.append(f"{n} mois clôturé" + ("s" if n > 1 else "")
                      + f" signalé{'s' if n > 1 else ''} sans recalcul "
                      f"({', '.join(mois_clotures)})")
    if not lignes:
        lignes.append("Aucun changement détecté sur les sources.")
    return lignes

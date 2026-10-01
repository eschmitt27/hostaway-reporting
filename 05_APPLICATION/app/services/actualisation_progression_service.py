"""Progression d'une actualisation, lisible par un humain — LECTURE SEULE.

Tout ce que l'écran affiche vient du run réel : `moteur_runs` (statut, début, fin) et
`moteur_run_etapes` (une ligne par étape, planifiée EN_ATTENTE dès l'ouverture du run, passée
EN_COURS au démarrage, puis à son statut final). Aucun minuteur, aucune estimation : une barre qui
avance toute seule mentirait précisément au moment où l'utilisateur en a besoin — quand une étape
est lente ou bloquée.

Ce module ne fait que TRADUIRE : codes de datasets → libellés du DAG (`Noeud.nom_affiche`),
statuts → états (en attente, en cours, terminée, non configurée, ignorée, non exécutée, échec),
résultats de service → volumes (« 1 651 réservations »). Il ne décide rien.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

ATTENTE = "attente"
EN_COURS = "en_cours"
TERMINE = "termine"
NON_CONFIGURE = "non_configure"
IGNORE = "ignore"
BLOQUE = "bloque"
ECHEC = "echec"

ICONES = {ATTENTE: "⏳", EN_COURS: "🔄", TERMINE: "✅", NON_CONFIGURE: "⚠️", IGNORE: "⚠️",
          BLOQUE: "⛔", ECHEC: "❌"}
LIBELLES_ETAT = {ATTENTE: "En attente", EN_COURS: "En cours", TERMINE: "Terminée",
                 NON_CONFIGURE: "Non configurée", IGNORE: "Ignorée", BLOQUE: "Non exécutée",
                 ECHEC: "Échec"}
LIBELLE_SAUVEGARDE = "Sauvegarde de la base"

LIBELLES_RUN = {orch.RUN_EN_COURS: "Actualisation en cours…",
                orch.RUN_SUCCES: "Actualisation terminée",
                orch.RUN_PARTIEL: "Actualisation terminée avec des erreurs",
                orch.RUN_ECHEC: "Actualisation en échec",
                orch.RUN_INTERROMPU: "Actualisation interrompue",
                "DRY_RUN": "Simulation (aucune donnée modifiée)"}

MESSAGES_NATURE = {
    orch.NATURE_SANS_SERVICE: "Import manuel : rien à interroger automatiquement.",
    orch.NATURE_NON_DECLENCHEE: "Non incluse dans cette actualisation.",
    orch.NATURE_INCHANGEE: "Données d'entrée inchangées : résultat conservé.",
    orch.NATURE_NON_EXECUTEE: "Non exécutée : l'actualisation s'est arrêtée avant.",
}


# ── Formats ─────────────────────────────────────────────────────────────────────────────────────

def _nombre(n: Any) -> str:
    try:
        return f"{int(n):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(n)


def _pluriel(n: Any, singulier: str, pluriel: str | None = None) -> str:
    try:
        un = int(n) <= 1
    except (TypeError, ValueError):
        un = False
    return f"{_nombre(n)} {singulier if un else (pluriel or singulier + 's')}"


def _instant(texte: str | None) -> datetime | None:
    if not texte:
        return None
    try:
        return datetime.fromisoformat(str(texte).replace("Z", "+00:00"))
    except ValueError:
        return None


def _heure(texte: str | None) -> str:
    """Même convention que le reste de l'application (`datetime_fr`) : l'heure écrite dans la
    donnée, sans conversion de fuseau."""
    dt = _instant(texte)
    return f"{dt:%H:%M:%S}" if dt else ""


def _date_heure(texte: str | None) -> str:
    dt = _instant(texte)
    return f"{dt:%d/%m/%Y} à {dt:%H:%M:%S}" if dt else ""


def duree_lisible(secondes: float | None) -> str:
    if secondes is None:
        return ""
    if secondes < 1:
        # Horodatages à la seconde : « 0,0 s » laisserait croire que l'étape n'a rien fait.
        return "< 1 s"
    if secondes < 60:
        return f"{secondes:.1f} s".replace(".", ",")
    minutes, reste = divmod(int(round(secondes)), 60)
    return f"{minutes} min {reste:02d} s"


def _publie_le(resultat: dict) -> str:
    horodatage = ((resultat.get("publie") or {}).get("source_horodatage")
                  or resultat.get("source_horodatage") or "")
    dt = _instant(horodatage)
    return f"{dt:%d/%m à %H:%M}" if dt else ""


# ── Volumes (appelé par l'orchestrateur après une étape réussie) ───────────────────────────────

def volume(dataset: str, resultat: dict, *, db_path=None) -> str | None:
    """Ce qu'une étape réussie a réellement traité, en mots. None quand rien d'utile n'est connu."""
    if dataset == dag.HOSTAWAY_RAW:
        return _volume_hostaway(resultat, db_path)
    if dataset == dag.HOSTAWAY_CLEANING_TASKS:
        base = _pluriel(resultat["nb_taches"], "tâche") if "nb_taches" in resultat else ""
        return " — ".join(x for x in (base, _etat_publication(resultat)) if x)
    if dataset == dag.BANQUE_QONTO:
        if "vues" not in resultat:
            return None
        return (f"{_pluriel(resultat['vues'], 'mouvement')} lus sur "
                f"{_pluriel(resultat.get('comptes', 0), 'compte')}, "
                f"{_pluriel(resultat.get('creees', 0), 'nouveau', 'nouveaux')}")
    if dataset == dag.MENAGES_PDF:
        if "nb_detectes" not in resultat:
            return None
        return (f"{_pluriel(resultat['nb_detectes'], 'PDF examiné', 'PDF examinés')}, "
                f"{_pluriel(resultat.get('nb_importees', 0), 'nouvelle facture', 'nouvelles factures')}")
    if dataset == dag.MENAGES_DECLARATIONS:
        return "Google Sheet relue"
    if dataset == dag.LOT12 and resultat.get("nb_entetes") is not None:
        return _pluriel(resultat["nb_entetes"], "préfacture")
    if dataset == dag.LOT11 and resultat.get("nb_constats") is not None:
        return _pluriel(resultat["nb_constats"], "constat")
    return None


def _volume_hostaway(resultat: dict, db_path) -> str | None:
    texte = _volume_hostaway_donnees(resultat, db_path)
    demande = resultat.get("extraction_demande")
    if not demande:
        return texte
    debut = _instant(demande.get("debut"))
    entete = f"Extraction Hostaway lancée à {debut:%H:%M:%S}" if debut else "Extraction Hostaway"
    if not resultat.get("importe"):
        # Le run a extrait, mais rien n'avait changé depuis la publication précédente : son
        # commit est vide, le dépôt garde le même état. C'est une vraie vérification, pas un
        # « déjà à jour » par défaut.
        return f"{entete} — aucune nouvelle publication (rien n'a changé) — {texte}"
    return f"{entete} — {texte}"


def _volume_hostaway_donnees(resultat: dict, db_path) -> str | None:
    extraction = resultat.get("extraction_id")
    ligne = None
    if extraction:
        conn = get_db(db_path)
        try:
            ligne = conn.execute(
                "SELECT nb_reservations, nb_payouts, nb_listings FROM hostaway_extractions "
                "WHERE extraction_id = ?", (extraction,)).fetchone()
        finally:
            conn.close()
    morceaux = []
    if ligne is not None:
        morceaux.append(f"{_pluriel(ligne['nb_reservations'], 'réservation')}, "
                        f"{_pluriel(ligne['nb_payouts'], 'paiement')}, "
                        f"{_pluriel(ligne['nb_listings'], 'logement')}")
    morceaux.append(_etat_publication(resultat))
    return " — ".join(morceaux)


def _etat_publication(resultat: dict) -> str:
    """Ce qui a été importé, sans surpromettre : une publication importée n'est pas forcément un
    contenu modifié (le pipeline réécrit parfois les mêmes lignes dans un autre ordre)."""
    publie = _publie_le(resultat)
    if resultat.get("importe"):
        return f"publication du {publie} importée" if publie else "nouvelle publication importée"
    return f"déjà à jour (publication du {publie})" if publie else "déjà à jour"


# ── Messages d'échec ────────────────────────────────────────────────────────────────────────────

def _message_echec(dataset: str, code: str | None, erreur: str) -> str:
    """Explication COURTE. Le détail technique reste dans Observabilité."""
    if dataset == orch.ETAPE_SAUVEGARDE:
        return ("La copie de sécurité de la base n'a pas pu être faite : par prudence, rien n'a "
                "été actualisé.")
    if code in ("HOSTAWAY_DEMANDE_RUN_ECHEC", "HOSTAWAY_DEMANDE_RUN_ANNULE",
                "HOSTAWAY_DEMANDE_DELAI", "HOSTAWAY_DEMANDE_RUN_INTROUVABLE",
                "HOSTAWAY_TACHES_NON_EXTRAITES") or (code or "").startswith("GITHUB_"):
        return (erreur or "").strip()           # messages déjà écrits pour l'utilisateur
    if dataset in (dag.HOSTAWAY_RAW, dag.HOSTAWAY_CLEANING_TASKS) and (
            code == "MOTEUR_CODE_RETOUR" or "rc=" in (erreur or "")):
        return ("L'import des données Hostaway s'est arrêté en erreur. Les dernières données "
                "valides sont conservées.")
    texte = (erreur or "").strip()
    if not texte:
        return "L'étape a échoué sans message. Les dernières données valides sont conservées."
    premiere = texte.split(" | ")[0]
    return premiere if len(premiere) <= 220 else premiere[:217] + "…"


# ── Lecture d'un run ────────────────────────────────────────────────────────────────────────────

def _libelle(etape: str) -> str:
    if etape == orch.ETAPE_SAUVEGARDE:
        return LIBELLE_SAUVEGARDE
    noeud = dag.NOEUDS.get(etape)
    return noeud.nom_affiche if noeud else etape


def _etat(ligne: dict, detail: dict) -> tuple[str, str]:
    statut = ligne.get("statut")
    erreur = ligne.get("erreur") or ""
    if statut == orch.ETAPE_EN_ATTENTE:
        return ATTENTE, ""
    if statut == orch.ETAPE_EN_COURS:
        return EN_COURS, ""
    if statut == orch.ETAPE_SUCCES:
        return TERMINE, detail.get("volume") or ""
    if statut == orch.ETAPE_NON_CONFIGUREE:
        return NON_CONFIGURE, erreur
    if statut == orch.ETAPE_ECHEC:
        return ECHEC, _message_echec(ligne["etape"], detail.get("code"), erreur)
    # IGNOREE : blocage par un amont, ou saut légitime.
    nature = detail.get("nature")
    if nature == orch.NATURE_BLOQUEE or (nature is None and erreur.startswith("Amont(s) en échec")):
        amonts = detail.get("amonts") or []
        noms = ", ".join(f"« {_libelle(a)} »" for a in amonts)
        return BLOQUE, (f"Non exécutée : {noms} en échec." if noms
                        else "Non exécutée : une étape précédente a échoué.")
    if nature in MESSAGES_NATURE:
        return (BLOQUE if nature == orch.NATURE_NON_EXECUTEE else IGNORE), MESSAGES_NATURE[nature]
    return IGNORE, "Ignorée."


def _sous_etapes(detail: dict) -> list[dict]:
    out = []
    for s in detail.get("sous_etapes") or []:
        etat = s.get("etat") if s.get("etat") in ICONES else ATTENTE
        out.append({"libelle": s.get("libelle", ""), "etat": etat, "icone": ICONES[etat],
                    "etat_libelle": LIBELLES_ETAT[etat], "message": s.get("message") or "",
                    "duree": duree_lisible(s.get("duree_s"))})
    return out


def _duree(ligne: dict, etat: str, maintenant: datetime) -> float | None:
    debut = _instant(ligne.get("started_at"))
    if etat == ATTENTE or debut is None:
        return None
    fin = maintenant if etat == EN_COURS else _instant(ligne.get("ended_at"))
    return max((fin - debut).total_seconds(), 0.0) if fin else None


def _run(run_id: str | None, db_path) -> dict | None:
    if run_id is None:
        return orch.dernier_run(db_path=db_path)
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM moteur_runs WHERE run_id = ? AND lot = ?",
                         (run_id, orch.LOT_ORCHESTRATEUR)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def progression(*, run_id: str | None = None, db_path=None) -> dict[str, Any]:
    """État d'avancement du run demandé — par défaut le dernier run de l'orchestrateur, en cours
    ou terminé. C'est ce qui permet de retrouver un run en cours après un rechargement de page."""
    run = _run(run_id, db_path)
    if run is None:
        return {"run": None, "en_cours": False, "etapes": []}
    maintenant = datetime.now(timezone.utc)
    etapes = []
    for ligne in orch.etapes_run(run["run_id"], db_path=db_path):
        try:
            detail = json.loads(ligne.get("detail") or "null") or {}
        except (TypeError, ValueError):
            detail = {}
        etat, message = _etat(ligne, detail)
        duree = _duree(ligne, etat, maintenant)
        etapes.append({"cle": ligne["etape"], "libelle": _libelle(ligne["etape"]), "etat": etat,
                       "icone": ICONES[etat], "etat_libelle": LIBELLES_ETAT[etat],
                       "message": message, "duree_s": duree, "duree": duree_lisible(duree),
                       "sous_etapes": _sous_etapes(detail)})

    total = len(etapes)
    finies = sum(1 for e in etapes if e["etat"] not in (ATTENTE, EN_COURS))
    en_cours = run["statut"] == orch.RUN_EN_COURS
    debut, fin = _instant(run.get("started_at")), _instant(run.get("ended_at"))
    duree = ((maintenant if en_cours else fin) - debut).total_seconds() if debut and (
        en_cours or fin) else None
    compteurs = {k: sum(1 for e in etapes if e["etat"] == k) for k in ICONES}
    echec = next((e for e in etapes if e["etat"] == ECHEC), None)
    courante = next((e for e in etapes if e["etat"] == EN_COURS), None)
    return {
        "run": {"run_id": run["run_id"], "statut": run["statut"],
                "titre": LIBELLES_RUN.get(run["statut"], run["statut"]),
                "global": run.get("arguments") == "TOUT",
                "debut": _date_heure(run.get("started_at")), "fin": _date_heure(run.get("ended_at")),
                "heure_debut": _heure(run.get("started_at")), "heure_fin": _heure(run.get("ended_at")),
                "duree": duree_lisible(duree)},
        "en_cours": en_cours,
        "total": total,
        "terminees": finies,
        "pourcentage": int(round(100 * finies / total)) if total else 0,
        "courante": courante,
        "compteurs": compteurs,
        "echec": ({"libelle": echec["libelle"], "titre": f"Échec lors de l'étape « {echec['libelle']} »",
                   "message": echec["message"]} if echec else None),
        "etapes": etapes,
    }

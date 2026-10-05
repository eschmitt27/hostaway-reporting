"""Service clôture mensuelle (APP-5C) — préparation, validation humaine, clôture définitive.

Deux décisions humaines distinctes, jamais enchaînées en un clic :
  · VALIDEE  = la préparation du mois est contrôlée et validée par un humain. Le mois n'est PAS
               gelé : ses opérations restent modifiables (une réouverture VALIDEE → ROUVERTE existe).
  · ARCHIVEE = la clôture DÉFINITIVE (mission 15, exposée par la mission 33) : `archiver()` fige
               l'archive économique du mois, la vérifie, marque `ref_cloture_mensuelle` à CLOTURE
               — le mois est alors fermé, et chaque module qui lit ce statut refuse ses écritures —
               puis passe la clôture à ARCHIVEE, le tout dans UNE transaction.
Les contrôles sont composés exclusivement à partir des services APP-5B existants (aucune
duplication de la logique de contrôle).

MISSION 32 — la clôture lit Flux financiers. Les bloqueurs d'un mois = contrôles moteur bloquants
(APP-5B) + bloqueurs financiers calculés EN DIRECT par `cloture_flux_service` (mouvements Qonto et
caisse non traités, écritures proposées, comptes comptables à définir). Et le calendrier s'impose :
seul un mois TERMINÉ se valide ou s'archive — le mois courant se prépare et se contrôle, jamais il ne
se clôture ; un mois futur non plus. Les deux gardes sont refaites par le serveur à chaque tentative,
sous verrou d'écriture, quelle que soit l'interface.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import controles_actionnable_service as act

_RE_MOIS = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# ── Automate ─────────────────────────────────────────────────────────────────
ST_NON_DEMARREE = "NON_DEMARREE"
ST_EN_PREPARATION = "EN_PREPARATION"
ST_A_VALIDER = "A_VALIDER"
ST_VALIDEE = "VALIDEE"
ST_ROUVERTE = "ROUVERTE"
ST_ARCHIVEE = "ARCHIVEE"

STATUTS = {ST_NON_DEMARREE, ST_EN_PREPARATION, ST_A_VALIDER, ST_VALIDEE, ST_ROUVERTE, ST_ARCHIVEE}
STATUTS_LIBELLES = {
    ST_NON_DEMARREE: "Non démarrée", ST_EN_PREPARATION: "En préparation", ST_A_VALIDER: "À valider",
    ST_VALIDEE: "Validée", ST_ROUVERTE: "Rouverte", ST_ARCHIVEE: "Clôturée définitivement",
}
TRANSITIONS: dict[str, set[str]] = {
    ST_NON_DEMARREE: {ST_EN_PREPARATION},
    ST_EN_PREPARATION: {ST_A_VALIDER},
    ST_A_VALIDER: {ST_EN_PREPARATION, ST_VALIDEE},
    ST_VALIDEE: {ST_ROUVERTE, ST_ARCHIVEE},
    ST_ROUVERTE: {ST_EN_PREPARATION},
    ST_ARCHIVEE: set(),
}


class ClotureRefusee(Exception):
    """Action de clôture refusée (transition invalide, bloqueur présent, justification manquante)."""


# ── Calendrier : seul un mois terminé se clôture ──────────────────────────────────────────────
T_PASSE = "PASSE"
T_COURANT = "COURANT"
T_FUTUR = "FUTUR"
_MOIS_FR = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
            "octobre", "novembre", "décembre")

E_BLOQUE = "BLOQUE"
E_FUTUR = "MOIS_FUTUR"
E_PRET_EN_COURS = "PRET_MOIS_EN_COURS"
E_PRET = "PRET_A_CLOTURER"


def aujourdhui() -> date:
    """La date du jour — une fonction, pour que les tests puissent la fixer."""
    return date.today()


def mois_fr(mois: str) -> str:
    """« 2026-09 » → « septembre 2026 »."""
    return f"{_MOIS_FR[int(mois[5:7]) - 1]} {mois[:4]}" if mois_valide(mois) else _txt(mois)


def _du_mois(mois: str) -> str:
    """« de septembre 2026 », « d'août 2026 »."""
    texte = mois_fr(mois)
    return f"d'{texte}" if texte[:1] in "aeiouéâ" else f"de {texte}"


def du_mois(mois: str) -> str:
    """Le mois précédé de sa préposition, élision comprise : « de septembre 2026 », « d'octobre 2026 »."""
    return _du_mois(mois)


def temporalite(mois: str) -> str:
    courant = aujourdhui().strftime("%Y-%m")
    return T_PASSE if mois < courant else (T_COURANT if mois == courant else T_FUTUR)


def refus_temporel(mois: str) -> str:
    """Motif de refus lié au calendrier, ou chaîne vide pour un mois terminé.

    Le premier motif est le périmètre V1 : un mois antérieur au début de la comptabilité V1
    n'appartient plus à la comptabilité applicative, il ne se clôture donc plus (cutover)."""
    from app.services import perimetre_v1_service as v1
    if v1.est_anterieur(mois):
        return v1.message_cloture(mois)
    t = temporalite(mois)
    if t == T_COURANT:
        return f"Le mois {_du_mois(mois)} est encore en cours et ne peut pas être clôturé."
    if t == T_FUTUR:
        return (f"Le mois {_du_mois(mois)} n'est pas commencé : un mois futur ne peut jamais "
                "être clôturé.")
    return ""


def refus_demarrage(mois: str) -> str:
    """Motif pour lequel la clôture de ce mois ne peut pas être DÉMARRÉE, ou chaîne vide.

    Le mois courant et les mois futurs n'ont rien à démarrer : la page du mois montre dès maintenant, recalculé à
    chaque affichage, tout ce qui resterait à traiter — sans clôture démarrée, sans rien figer. Démarrer n'y
    apporterait rien, et une clôture « en préparation » sur un mois qui court est un état que rien ne sait
    défendre (instantané de contrôles périmés, validation d'un mois inachevé). Elle ne se démarre donc qu'une
    fois le mois terminé — jamais avant la comptabilité V1, jamais pour un mois futur."""
    motif = refus_temporel(mois)
    if motif and temporalite(mois) == T_COURANT:
        return (f"Le mois {_du_mois(mois)} est encore en cours : ses points à traiter se consultent dès maintenant, "
                "mais sa clôture ne peut être démarrée qu'une fois le mois terminé.")
    return motif


MSG_NON_VALIDEE = ("Le mois doit d'abord être validé avant de pouvoir être clôturé "
                   "définitivement.")
MSG_DEJA_ARCHIVEE = "Ce mois est déjà clôturé définitivement."
MSG_ETAT_PERIME = ("La clôture a changé depuis l'affichage de la confirmation : rechargez la page "
                   "et vérifiez-la de nouveau.")


def message_bloquants(nb: int) -> str:
    pluriel = nb > 1
    return (f"Ce mois ne peut pas être clôturé : {nb} contrôle{'s' if pluriel else ''} "
            f"bloquant{'s' if pluriel else ''} reste{'nt' if pluriel else ''} à traiter.")


def message_modules_ouverts(cles: list[str]) -> str:
    """« Tous les modules doivent être clôturés… » — dit lesquels restent ouverts, par leur nom."""
    from app.services import cloture_modules_service as cm
    noms = ", ".join(f"« {cm.PAR_CLE[c].libelle} »" for c in cles if c in cm.PAR_CLE)
    return ("Le mois ne peut pas être clôturé : tous les modules doivent d'abord l'être. "
            f"Il reste à clôturer : {noms}.")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Identifiants opaques ──────────────────────────────────────────────────────

def id_opaque(prefixe: str, valeur: str) -> str:
    base = (cfg.CLOTURE_OPAQUE_SALT + "|" + prefixe + "|" + _txt(valeur)).encode("utf-8")
    return prefixe + "-" + hashlib.sha256(base).hexdigest()[:10]


def cloture_id_opaque(mois: str) -> str:
    return id_opaque("CLO", mois)


def mois_valide(mois: str) -> bool:
    """Format strict AAAA-MM (janvier=01 à décembre=12). Aucune autre forme acceptée."""
    return bool(_RE_MOIS.match(_txt(mois)))


# ── Lecture ──────────────────────────────────────────────────────────────────

def _row(r) -> dict[str, Any]:
    return dict(r) if r is not None else None


def charger_par_mois(mois: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE mois=? AND actif=1", (mois,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def charger_par_opaque(cloture_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def lister(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE actif=1 ORDER BY mois DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def historique(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_evenements WHERE cloture_id_opaque=? ORDER BY id DESC",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Progression / bloqueurs (composé depuis APP-5B, aucune duplication) ──────

def elements_du_mois(mois: str, db_path=None) -> list[dict[str, Any]]:
    tous = act._tous_les_elements(db_path)
    return [e for e in tous if e["mois"] == mois]


def calcul_progression(mois: str, db_path=None, *,
                       contexte_flux: dict | None = None) -> dict[str, Any]:
    """Progression du mois : contrôles moteur (APP-5B) + bloqueurs Flux financiers, recalculés à
    chaque appel. `contexte_flux` évite de relire Flux pour chaque mois d'une liste."""
    from app.services import cloture_flux_service as cf
    from app.services import cloture_modules_service as cm

    els = elements_du_mois(mois, db_path)
    anomalies = [e for e in els if not e["est_info"]]
    # Un constat que la lecture DIRECTE d'un module remplace (charge non validée, écart de ménages) n'est pas un
    # bloqueur du moteur : c'est le module qui le juge — et qui le lève quand la décision est prise. Le compter ici
    # ferait dire à la trace de clôture « 5 bloquants moteur » pour un mois sans aucun bloqueur.
    bloqueurs = [e for e in anomalies
                if e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]
                and e["code"] not in cm.CODES_LUS_EN_DIRECT]
    financier = cf.analyser(mois, contexte_flux=contexte_flux, db_path=db_path)
    # UNE seule vérité des bloqueurs : ceux de chaque MODULE (contrôles du moteur + Flux + lectures
    # directes des vrais modules). `bloqueurs` / `financier` restent rendus tels quels pour les
    # écrans et les journaux qui les lisent encore.
    modules = cm.analyser(mois, elements=els, flux=financier, db_path=db_path)
    nb_bloqueurs = modules["nb_bloqueurs"]
    temporel = temporalite(mois) if mois_valide(mois) else T_FUTUR
    pluriel = "s" if nb_bloqueurs > 1 else ""
    if temporel == T_FUTUR:
        etat, etat_libelle = E_FUTUR, "Mois futur — non clôturable"
    elif nb_bloqueurs:
        etat = E_BLOQUE
        etat_libelle = f"Clôture impossible — {nb_bloqueurs} élément{pluriel} bloquant{pluriel}"
    elif temporel == T_COURANT:
        etat, etat_libelle = E_PRET_EN_COURS, "Prêt techniquement — mois en cours"
    else:
        etat, etat_libelle = E_PRET, "Prêt à clôturer"
    return {
        "mois": mois,
        "mois_fr": mois_fr(mois),
        "nb_total": len(els),
        "nb_anomalies": len(anomalies),
        "nb_bloqueurs": nb_bloqueurs,
        "nb_bloqueurs_moteur": len(bloqueurs),
        "nb_bloqueurs_flux": financier["nb_bloquants"],
        "nb_informatifs_flux": financier["nb_informatifs"],
        "flux": financier,
        "modules": modules,
        "temporalite": temporel,
        "refus_temporel": refus_temporel(mois) if mois_valide(mois) else "",
        "etat": etat,
        "etat_libelle": etat_libelle,
        # « Prêt » = tous les contrôles automatiques satisfaits ; « autorisée » = prêt ET terminé.
        "pret_a_cloturer": nb_bloqueurs == 0,
        "cloture_autorisee": nb_bloqueurs == 0 and temporel == T_PASSE,
        "nb_a_traiter": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "OUVERT"),
        "nb_en_cours": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "EN_COURS"),
        "nb_resolus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "RESOLU"),
        "nb_exceptions": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "ACCEPTE_AVEC_JUSTIFICATION"),
        "nb_reapparus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "ROUVERT"),
        "nb_informatifs": sum(1 for e in els if e["est_info"]),
        "bloqueurs": bloqueurs,
        "cloturable": nb_bloqueurs == 0,
    }


def _resume_controles(progression: dict) -> str:
    return (f"Contrôles recalculés : {progression['nb_bloqueurs_moteur']} bloquant(s) moteur, "
            f"{progression['nb_bloqueurs_flux']} bloquant(s) Flux financiers, "
            f"{progression['nb_informatifs_flux']} informatif(s) Flux financiers.")


def _garde_cloture(cloture: dict, db_path=None) -> dict[str, Any]:
    """Les deux gardes d'une clôture, refaites par le serveur : calendrier, puis bloqueurs."""
    refus = refus_temporel(cloture["mois"])
    if refus:
        raise ClotureRefusee(refus)
    progression = calcul_progression(cloture["mois"], db_path)
    if not progression["cloturable"]:
        raise ClotureRefusee(message_bloquants(progression["nb_bloqueurs"]))
    return progression


# ── Création / transition ────────────────────────────────────────────────────

def _journaliser_evenement(conn, cloture_opaque, type_evt, ancien, nouveau, commentaire="",
                           preuve="", acteur="", date_evenement=None):
    """Ajoute une ligne au journal de la clôture. L'heure écrite est l'heure LOCALE du poste (`_now()`), celle
    que l'utilisateur lit et rapproche de ses gestes : la valeur par défaut de la colonne, elle, est en UTC
    et ferait apparaître un mois « clôturé » avant les modules qu'il vient de clôturer."""
    conn.execute(
        "INSERT INTO cloture_evenements (cloture_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, commentaire, preuve, acteur, date_evenement) VALUES (?,?,?,?,?,?,?,?)",
        (cloture_opaque, type_evt, ancien, nouveau, commentaire, preuve, acteur, date_evenement or _now()))


def creer_ou_charger(mois: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée la clôture du mois si absente (idempotent), sinon retourne l'existante. Unicité mois.

    Refuse tout mois hors format strict AAAA-MM (jamais de ligne créée pour une valeur malformée —
    évite aussi toute injection dans le nom de fichier d'export ou les en-têtes HTTP)."""
    if not mois_valide(mois):
        raise ClotureRefusee(f"Mois invalide : « {mois} ». Format attendu AAAA-MM.")
    mois = _txt(mois)
    from app.services import perimetre_v1_service as v1
    if v1.est_anterieur(mois, db_path=db_path):
        raise ClotureRefusee(v1.message_cloture(mois, db_path=db_path))
    existante = charger_par_mois(mois, db_path)
    if existante:
        return existante
    opaque = cloture_id_opaque(mois)
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                "VALUES (?,?,?,?)", (opaque, mois, ST_NON_DEMARREE, acteur))
            _journaliser_evenement(conn, opaque, "CREATION", None, ST_NON_DEMARREE, acteur=acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            # Course gagnée par une requête concurrente qui a créé le même mois entretemps
            # (deux clics simultanés) — la contrainte UNIQUE de la base tranche, pas Python.
            conn.rollback()
    finally:
        conn.close()
    return charger_par_mois(mois, db_path)


def _transition(cloture: dict, nouveau_statut: str, *, acteur: str = "", commentaire: str = "",
                justification: str = "", version_attendue: int | None = None, db_path=None,
                extra_cols: dict | None = None, conn=None,
                permis_depuis_archivee: bool = False) -> dict[str, Any]:
    """Transition atomique : la garde de version est appliquée par SQL (`WHERE ... AND version=?`),
    pas seulement vérifiée côté Python — élimine la fenêtre de course (TOCTOU) entre deux requêtes
    concurrentes qui liraient le même état avant d'écrire (double clic, requêtes simultanées).

    `version_attendue` explicite (si fourni) doit correspondre à la version chargée par l'appelant ;
    la garde SQL utilise toujours `cloture["version"]` (la version réellement lue), jamais une valeur
    non vérifiée.
    """
    ancien = cloture["statut"]
    # Un mois clôturé définitivement n'a AUCUNE sortie dans l'automate : la seule exception est la réouverture
    # exceptionnelle (`rouvrir_exceptionnellement`), qui le demande explicitement et traite tout le reste.
    exception = permis_depuis_archivee and ancien == ST_ARCHIVEE and nouveau_statut == ST_ROUVERTE
    if nouveau_statut not in TRANSITIONS.get(ancien, set()) and not exception:
        raise ClotureRefusee(f"Transition {ancien} → {nouveau_statut} interdite.")
    version_lue = cloture["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise ClotureRefusee(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    opaque = cloture["cloture_id_opaque"]
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        cols = ["statut = ?", "version = version + 1"]
        vals: list[Any] = [nouveau_statut]
        extra_cols = extra_cols or {}
        for k, v in extra_cols.items():
            cols.append(f"{k} = ?")
            vals.append(v)
        vals.append(opaque)
        vals.append(version_lue)
        cur = conn.execute(
            f"UPDATE clotures_mensuelles SET {', '.join(cols)} "
            f"WHERE cloture_id_opaque = ? AND version = ?", vals)
        if cur.rowcount == 0:
            # Personne d'autre n'a pu écrire entre notre lecture et notre écriture SANS que ce
            # contrôle l'attrape : conflit réel (rejeu, double clic concurrent, écriture parallèle).
            if connexion_locale:
                conn.rollback()
            raise ClotureRefusee(
                f"Conflit de version — la clôture a été modifiée entretemps (version {version_lue} "
                "attendue). Rechargez la page et réessayez.")
        _journaliser_evenement(conn, opaque, "TRANSITION", ancien, nouveau_statut,
                               commentaire=commentaire or justification, acteur=acteur)
        if connexion_locale:
            conn.commit()
            resultat = charger_par_opaque(opaque, db_path)
        else:
            # Connexion partagée (transaction non encore commitée par l'appelant) : relire via la
            # MÊME connexion — une connexion séparée ne verrait pas l'écriture non commitée (WAL).
            r = conn.execute(
                "SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=?", (opaque,)).fetchone()
            resultat = _row(r)
    finally:
        if connexion_locale:
            conn.close()
    return resultat


def demarrer_preparation(cloture: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    motif = refus_demarrage(cloture["mois"])
    if motif:
        raise ClotureRefusee(motif)
    return _transition(cloture, ST_EN_PREPARATION, acteur=acteur, version_attendue=version_attendue,
                       db_path=db_path, extra_cols={"date_preparation": _now()})


def demarrer(mois: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ouvre la clôture d'un mois TERMINÉ et la démarre — le point d'entrée de « Démarrer la clôture ».

    Refus (mois courant, mois futur, mois antérieur à la comptabilité V1, format invalide) AVANT toute écriture :
    un refus ne laisse aucune clôture « non démarrée » derrière lui. Une clôture déjà démarrée est rendue
    telle quelle, jamais redémarrée."""
    if not mois_valide(mois):
        raise ClotureRefusee(f"Mois invalide : « {mois} ». Format attendu AAAA-MM.")
    existante = charger_par_mois(_txt(mois), db_path)
    if existante is not None and existante["statut"] != ST_NON_DEMARREE:
        return existante
    motif = refus_demarrage(_txt(mois))
    if motif:
        raise ClotureRefusee(motif)
    c = creer_ou_charger(mois, acteur=acteur, db_path=db_path)
    if c["statut"] == ST_NON_DEMARREE:
        c = demarrer_preparation(c, acteur=acteur, version_attendue=c["version"], db_path=db_path)
    return c


def passer_a_valider(cloture: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    """Passage à A_VALIDER + snapshot — ATOMIQUE (une seule transaction SQL, un seul commit).

    Sans cela, un arrêt du processus entre la transition et le snapshot laisserait une clôture en
    A_VALIDER sans instantané figé. La garde de version protège aussi ce passage contre un double
    déclenchement concurrent."""
    motif = refus_temporel(cloture["mois"])
    if motif:
        raise ClotureRefusee(motif)         # jamais d'instantané de contrôles « à valider » sur un mois qui court
    progression = calcul_progression(cloture["mois"], db_path)
    conn = get_db(db_path)
    try:
        res = _transition(cloture, ST_A_VALIDER, acteur=acteur, version_attendue=version_attendue,
                          db_path=db_path, conn=conn)
        snapshot(res, db_path=db_path, conn=conn)
        _journaliser_evenement(conn, cloture["cloture_id_opaque"], "CONTROLES", None, None,
                               commentaire=_resume_controles(progression), acteur=acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(cloture["cloture_id_opaque"], db_path)


def valider(cloture: dict, *, acteur: str = "", commentaire: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    """Valide la clôture d'un mois TERMINÉ sans aucun bloqueur (jamais d'écriture réelle).

    Transactionnel : le verrou d'écriture (`BEGIN IMMEDIATE`) est pris AVANT le recalcul des
    bloqueurs, si bien qu'aucune écriture concurrente ne peut s'intercaler entre le contrôle et la
    validation ; la transition et la trace des contrôles partent dans le même commit, ou rien."""
    if not commentaire.strip():
        raise ClotureRefusee("Commentaire de validation requis.")
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        progression = _garde_cloture(cloture, db_path)
        _transition(cloture, ST_VALIDEE, acteur=acteur, commentaire=commentaire,
                    version_attendue=version_attendue, db_path=db_path, conn=conn,
                    extra_cols={"date_validation": _now(), "commentaire_validation": commentaire,
                                "valide_par": acteur})
        _journaliser_evenement(conn, cloture["cloture_id_opaque"], "CONTROLES", None, None,
                               commentaire=_resume_controles(progression), acteur=acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(cloture["cloture_id_opaque"], db_path)


def rouvrir(cloture: dict, *, acteur: str = "", justification: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    if not justification.strip():
        raise ClotureRefusee("Justification requise pour rouvrir une clôture validée.")
    return _transition(cloture, ST_ROUVERTE, acteur=acteur, justification=justification,
                       version_attendue=version_attendue, db_path=db_path,
                       extra_cols={"date_reouverture": _now(), "justification_reouverture": justification})


def archiver(cloture: dict, *, acteur: str = "", commentaire: str = "", version_attendue=None,
             db_path=None):
    """VALIDEE -> ARCHIVEE — la clôture DÉFINITIVE (mission 15, exposée en mission 33).

    Sous verrou d'écriture (`BEGIN IMMEDIATE`), dans cet ordre : calendrier (mois terminé), état
    RELU en base (VALIDEE, et inchangé depuis `version_attendue` si l'appelant l'a affiché),
    bloqueurs moteur et Flux recalculés ; puis archive économique, `CLOTURE`, transition et trace
    des contrôles — tout ou rien. Si l'archivage échoue (`ArchivageRefuse`), rien n'est appliqué :
    jamais `mois=CLOTURE` sans archive complète.
    """
    from app.services import cloture_archivage_service as arch

    mois = cloture["mois"]
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        refus = refus_temporel(mois)
        if refus:
            raise ClotureRefusee(refus)
        actuelle = _row(conn.execute("SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=? "
                                     "AND actif=1", (cloture["cloture_id_opaque"],)).fetchone())
        if actuelle is None:
            raise ClotureRefusee("Clôture introuvable.")
        if actuelle["statut"] == ST_ARCHIVEE:
            raise ClotureRefusee(MSG_DEJA_ARCHIVEE)
        if actuelle["statut"] != ST_VALIDEE:
            raise ClotureRefusee(MSG_NON_VALIDEE)
        if version_attendue is not None and int(version_attendue) != actuelle["version"]:
            raise ClotureRefusee(MSG_ETAT_PERIME)
        _exiger_modules_clos(mois, db_path)
        progression = calcul_progression(mois, db_path)
        if not progression["cloturable"]:
            raise ClotureRefusee(message_bloquants(progression["nb_bloqueurs"]))
        cloture = actuelle
        arch.archiver_mois(mois, acteur=acteur, conn=conn)
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?,?,?) "
            "ON CONFLICT(mois) DO UPDATE SET statut_mois='CLOTURE'",
            (mois, "CLOTURE", f"CLOTURE_APP-{acteur or 'SYSTEME'}"))
        resultat = _transition(cloture, ST_ARCHIVEE, acteur=acteur, commentaire=commentaire,
                               conn=conn)
        _journaliser_evenement(conn, cloture["cloture_id_opaque"], "CONTROLES", None, None,
                               commentaire=_resume_controles(progression), acteur=acteur)
        conn.commit()
        return resultat
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


MSG_REOUVERTURE_JUSTIFICATION = ("Une justification est obligatoire pour rouvrir exceptionnellement un mois "
                                 "clôturé.")
MSG_REOUVERTURE_NON_CLOTURE = ("Ce mois n'est pas clôturé définitivement : il n'y a rien à rouvrir "
                               "exceptionnellement. Un module clôturé se rouvre depuis la clôture du mois.")


def mois_posterieurs_figes(mois: str, *, conn=None, db_path=None) -> list[str]:
    """Mois POSTÉRIEURS qui reposent sur celui-ci : clôturés définitivement, ou dont au moins un module est
    clôturé. Un mois dont les chiffres sont arrêtés a été arrêté sur les chiffres du mois précédent : rouvrir
    ce dernier changerait, en silence, ce sur quoi le suivant s'appuie."""
    locale = conn is None
    if locale:
        conn = get_db(db_path)
    try:
        figes = {r[0] for r in conn.execute(
            "SELECT mois FROM clotures_mensuelles WHERE actif = 1 AND statut = ? AND mois > ?",
            (ST_ARCHIVEE, mois))}
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cloture_modules'").fetchone():
            figes |= {r[0] for r in conn.execute(
                "SELECT DISTINCT mois FROM cloture_modules WHERE statut = 'CLOS' AND mois > ?", (mois,))}
        return sorted(figes)
    finally:
        if locale:
            conn.close()


def message_mois_posterieurs(mois: str, posterieurs: list[str]) -> str:
    """« …rouvrez d'abord novembre 2026, puis octobre 2026 » — du plus récent au plus ancien."""
    ordre = ", puis ".join(mois_fr(m) for m in sorted(posterieurs, reverse=True))
    return (f"Le mois {_du_mois(mois)} ne peut pas être rouvert tant que des mois postérieurs sont clôturés (ou "
            f"en partie) : rouvrez d'abord {ordre}. Les mois se rouvrent du plus récent au plus ancien, pour "
            "qu'aucun mois clôturé ne repose sur un mois modifié.")


def rouvrir_exceptionnellement(cloture: dict, *, acteur: str = "", justification: str = "",
                               version_attendue=None, db_path=None) -> dict[str, Any]:
    """RÉOUVERTURE EXCEPTIONNELLE d'un mois clôturé définitivement — contrôlée, justifiée, tracée, jamais une
    suppression de la clôture : rouvrir remet le mois dans un état où l'on peut RÉELLEMENT corriger ses données,
    puis le reclôturer.

    En UNE transaction, ou rien : (1) l'archive économique figée est retirée des tables vives — après avoir été
    copiée, ligne par ligne, dans `cloture_archives_retirees` — pour que la reclôture fige les valeurs corrigées ;
    (2) le mois redevient « en contrôle » pour tout ce qui lit `ref_cloture_mensuelle` (Banque, Charges, Ménages,
    les moteurs de calcul : plus aucun ne le tient pour clos) ; (3) les SEPT modules repassent à « rouvert » —
    un mois rouvert dont les domaines resteraient verrouillés ne corrigerait rien — et la période comptable est
    rouverte ; (4) la clôture passe de ARCHIVEE à ROUVERTE. Le journal montre : clôturé → rouvert → (reclôturé) ;
    la clôture d'origine (date, acteur) reste lisible, la réouverture s'y ajoute avec sa justification.

    Refusée — message métier — si : justification vide ; mois non clôturé définitivement ; état affiché périmé ;
    un mois POSTÉRIEUR est clôturé (ou en partie) : on rouvre du plus récent au plus ancien. Les factures déjà
    émises et les écritures validées ne sont jamais modifiées par la réouverture."""
    from app.services import cloture_archivage_service as arch
    from app.services import cloture_modules_service as cm
    from app.services import comptabilite_periodes_service as per

    justification = _txt(justification)
    if not justification:
        raise ClotureRefusee(MSG_REOUVERTURE_JUSTIFICATION)
    mois = cloture["mois"]
    opaque = cloture["cloture_id_opaque"]
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        actuelle = _row(conn.execute("SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=? "
                                     "AND actif=1", (opaque,)).fetchone())
        if actuelle is None:
            raise ClotureRefusee("Clôture introuvable.")
        if actuelle["statut"] != ST_ARCHIVEE:
            raise ClotureRefusee(MSG_REOUVERTURE_NON_CLOTURE)
        if version_attendue is not None and int(version_attendue) != actuelle["version"]:
            raise ClotureRefusee(MSG_ETAT_PERIME)
        posterieurs = mois_posterieurs_figes(mois, conn=conn)
        if posterieurs:
            raise ClotureRefusee(message_mois_posterieurs(mois, posterieurs))

        maintenant = _now()
        retire = arch.retirer_archive(mois, cloture_id_opaque=opaque, justification=justification,
                                      acteur=acteur, conn=conn)
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?,?,?) "
            "ON CONFLICT(mois) DO UPDATE SET statut_mois='EN_CONTROLE', import_id=excluded.import_id",
            (mois, "EN_CONTROLE", f"REOUVERTURE_APP-{acteur or 'SYSTEME'}"))
        conn.execute(
            "INSERT INTO mois_reouvertures (mois, statut_avant, statut_apres, motif, acteur, reouvert_le, "
            "recalcul_statut) VALUES (?,?,?,?,?,?,?)",
            (mois, "CLOTURE", "EN_CONTROLE", justification, acteur or "local",
             datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "NON_LANCE"))
        rouverts = cm.rouvrir_tous_dans(conn, opaque, mois, acteur=acteur, justification=justification,
                                        maintenant=maintenant)
        periode = per.rouvrir_dans(conn, mois, justification=f"Réouverture exceptionnelle du mois : "
                                                              f"{justification}", acteur=acteur)
        if not periode.get("ok"):
            raise ClotureRefusee(periode.get("message") or "La période comptable n'a pas pu être rouverte.")

        resultat = _transition(actuelle, ST_ROUVERTE, acteur=acteur, justification=justification, db_path=db_path,
                               conn=conn, permis_depuis_archivee=True,
                               extra_cols={"date_reouverture": maintenant,
                                           "justification_reouverture": justification})
        n = len(rouverts)
        effets = (f"{n} module{'s' if n > 1 else ''} rouvert{'s' if n > 1 else ''} ; "
                  + ("période comptable rouverte ; " if periode.get("rouverte") else "")
                  + (f"archive économique retirée et conservée ({retire['nb_reservations']} réservation"
                     f"{'s' if retire['nb_reservations'] > 1 else ''}) ; "
                     if retire["nb_reservations"] or retire["nb_reglement"] else "")
                  + "rien n'est supprimé, la clôture d'origine reste dans l'historique.")
        _journaliser_evenement(conn, opaque, "REOUVERTURE_EXCEPTIONNELLE", ST_ARCHIVEE, ST_ROUVERTE,
                               commentaire=effets, acteur=acteur, date_evenement=maintenant)
        conn.commit()
        return resultat
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _exiger_modules_clos(mois: str, db_path=None) -> None:
    """Le mois entier ne se clôture que si TOUS ses modules l'ont été — règle posée par la clôture
    par modules, tenue ici pour TOUS les chemins qui mènent à la clôture définitive."""
    from app.services import cloture_modules_service as cm
    restants = cm.modules_non_clos(mois, db_path=db_path)
    if restants:
        raise ClotureRefusee(message_modules_ouverts(restants))


def cloturer_mois(cloture: dict, *, acteur: str = "", commentaire: str = "", version_attendue=None,
                  db_path=None):
    """CLÔTURE DU MOIS ENTIER — la décision finale de la clôture par modules.

    Exige, sous verrou d'écriture et dans cet ordre : un mois terminé (le courant et les futurs ne se
    clôturent jamais), une clôture démarrée et non déjà définitive, l'état affiché encore actuel,
    TOUS les modules clôturés, puis aucun bloqueur réel (recalculés, jamais copiés). Alors seulement,
    en UNE transaction : les étapes d'automate qui restent (à valider, validée — la validation est ici
    portée par la clôture de chaque module), l'archive économique du mois, `ref_cloture_mensuelle` à
    CLOTURE, la clôture à ARCHIVEE et la trace des contrôles. Au moindre refus : rien n'est appliqué.

    Cette décision est une décision humaine unique et confirmée (page de confirmation à case cochée) :
    chaque module a déjà été clôturé par un geste distinct, c'est ce qui remplace la validation
    intermédiaire des clôtures d'avant (VALIDEE), conservée pour celles qui y sont déjà."""
    from app.services import cloture_archivage_service as arch

    mois = cloture["mois"]
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        refus = refus_temporel(mois)
        if refus:
            raise ClotureRefusee(refus)
        actuelle = _row(conn.execute("SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=? "
                                     "AND actif=1", (cloture["cloture_id_opaque"],)).fetchone())
        if actuelle is None:
            raise ClotureRefusee("Clôture introuvable.")
        if actuelle["statut"] == ST_ARCHIVEE:
            raise ClotureRefusee(MSG_DEJA_ARCHIVEE)
        if actuelle["statut"] == ST_NON_DEMARREE:
            raise ClotureRefusee("Démarrez d'abord la clôture du mois.")
        if version_attendue is not None and int(version_attendue) != actuelle["version"]:
            raise ClotureRefusee(MSG_ETAT_PERIME)
        _exiger_modules_clos(mois, db_path)
        progression = calcul_progression(mois, db_path)
        if not progression["cloturable"]:
            raise ClotureRefusee(message_bloquants(progression["nb_bloqueurs"]))

        courante = actuelle
        if courante["statut"] == ST_ROUVERTE:
            courante = _transition(courante, ST_EN_PREPARATION, acteur=acteur, db_path=db_path,
                                   conn=conn, commentaire="Clôture du mois")
        if courante["statut"] == ST_EN_PREPARATION:
            courante = _transition(courante, ST_A_VALIDER, acteur=acteur, db_path=db_path, conn=conn)
            snapshot(courante, db_path=db_path, conn=conn)
        if courante["statut"] == ST_A_VALIDER:
            validation = commentaire.strip() or "Tous les modules sont clôturés : mois validé et clôturé."
            courante = _transition(courante, ST_VALIDEE, acteur=acteur, commentaire=validation,
                                   db_path=db_path, conn=conn,
                                   extra_cols={"date_validation": _now(),
                                               "commentaire_validation": validation,
                                               "valide_par": acteur})
        arch.archiver_mois(mois, acteur=acteur, conn=conn)
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?,?,?) "
            "ON CONFLICT(mois) DO UPDATE SET statut_mois='CLOTURE'",
            (mois, "CLOTURE", f"CLOTURE_APP-{acteur or 'SYSTEME'}"))
        resultat = _transition(courante, ST_ARCHIVEE, acteur=acteur, commentaire=commentaire,
                               db_path=db_path, conn=conn)
        _journaliser_evenement(conn, cloture["cloture_id_opaque"], "CONTROLES", None, None,
                               commentaire=_resume_controles(progression), acteur=acteur)
        conn.commit()
        return resultat
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Snapshot ──────────────────────────────────────────────────────────────────

def snapshot(cloture: dict, db_path=None, conn=None) -> int:
    """Persiste un instantané logique FIGÉ des contrôles du mois (jamais de fichier métier réel).

    Écrit toujours (jamais recalculé silencieusement après coup) : `snapshot_actif` relit ensuite
    ces lignes, pas l'état courant du moteur. `conn` optionnel permet de partager la transaction avec
    la transition qui déclenche ce snapshot (atomicité passer_a_valider)."""
    opaque = cloture["cloture_id_opaque"]
    els = elements_du_mois(cloture["mois"], db_path)
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        conn.execute("UPDATE cloture_elements SET actif=0 WHERE cloture_id_opaque=? AND actif=1",
                    (opaque,))
        for e in els:
            conn.execute(
                "INSERT INTO cloture_elements (cloture_id_opaque, ctrl_opaque, code_controle, "
                "entite_type, entite_opaque, severite, statut_moteur, statut_humain, bloque_cloture) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (opaque, e["ctrl_opaque"], e["code"], e["module"], e["entite_id"], e["niveau"],
                 "PRESENTE" if e["etat"]["anomalie_moteur_presente"] else "ABSENTE",
                 e["etat"]["statut_suivi"],
                 1 if (e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]
                       and not e["est_info"]) else 0))
        if connexion_locale:
            conn.commit()
        return len(els)
    finally:
        if connexion_locale:
            conn.close()


def snapshot_actif(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_elements WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Documents (preuves) ───────────────────────────────────────────────────────

def ajouter_document(cloture_opaque: str, nom_logique: str, type_document: str = "PREUVE",
                     chemin_logique: str = "", db_path=None) -> str:
    from app.services.path_sanitizer import sanitize_text
    doc_opaque = id_opaque("DOC", cloture_opaque + nom_logique + _now())
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO cloture_documents (document_id_opaque, cloture_id_opaque, nom_logique, "
            "type_document, chemin_logique) VALUES (?,?,?,?,?)",
            (doc_opaque, cloture_opaque, nom_logique, type_document, sanitize_text(chemin_logique)))
        conn.execute(
            "INSERT INTO cloture_evenements (cloture_id_opaque, type_evenement, commentaire, preuve, "
            "date_evenement) VALUES (?,?,?,?,?)",
            (cloture_opaque, "PREUVE_AJOUTEE", nom_logique, doc_opaque, _now()))
        conn.commit()
    finally:
        conn.close()
    return doc_opaque


def documents(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_documents WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

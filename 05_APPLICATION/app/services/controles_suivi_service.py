"""Service SUIVI HUMAIN des contrôles (APP-5B) — journal applicatif isolé.

Le moteur (Lot11) reste la vérité de l'anomalie. Ce service journalise UNIQUEMENT le suivi humain
(prise en charge, décision, exception) dans SQLite ; il ne masque JAMAIS une anomalie moteur encore
présente. Quatre dimensions distinctes et non confondues :

  1. anomalie moteur       : PRÉSENTE / ABSENTE            (fournie par le moteur, jamais par le suivi)
  2. prise en charge       : NON_TRAITÉ / EN_COURS / TRAITÉ
  3. résultat humain       : CORRIGE / EXCEPTION_ACCEPTEE / NON_CORRIGE / A_REVOIR
  4. statut de suivi       : OUVERT / EN_COURS / RESOLU / ACCEPTE_AVEC_JUSTIFICATION / ROUVERT

Règles dures : une seule décision ACTIVE par contrôle ; historique append-only ; version optimiste
(conflit refusé) ; « Corrigé » exige une preuve ; « Exception » exige une justification ; « Résolu »
interdit si l'anomalie moteur est présente sauf exception acceptée ; réapparition => réouverture auto.
Les flags d'écriture réelle restent False : rien n'est écrit hors de l'app.db isolée.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

# ── Statuts de suivi ─────────────────────────────────────────────────────────
ST_OUVERT = "OUVERT"
ST_EN_COURS = "EN_COURS"
ST_RESOLU = "RESOLU"
ST_ACCEPTE = "ACCEPTE_AVEC_JUSTIFICATION"
ST_ROUVERT = "ROUVERT"
STATUTS_SUIVI = {ST_OUVERT, ST_EN_COURS, ST_RESOLU, ST_ACCEPTE, ST_ROUVERT}
STATUTS_LIBELLES = {
    ST_OUVERT: "Ouvert", ST_EN_COURS: "En cours", ST_RESOLU: "Résolu",
    ST_ACCEPTE: "Accepté avec justification", ST_ROUVERT: "Rouvert",
}

# ── Résultats humains ────────────────────────────────────────────────────────
RES_CORRIGE = "CORRIGE"
RES_EXCEPTION = "EXCEPTION_ACCEPTEE"
RES_NON_CORRIGE = "NON_CORRIGE"
RES_A_REVOIR = "A_REVOIR"


class SuiviRefuse(Exception):
    """Action de suivi refusée (transition invalide, preuve/justification/motif absent, conflit version)."""


# ── Lecture ──────────────────────────────────────────────────────────────────

def suivi_actif(ctrl_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM controles_suivi WHERE controle_id_opaque=? AND actif=1 "
            "ORDER BY version DESC LIMIT 1", (str(ctrl_opaque),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def historique(ctrl_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM controles_suivi_historique WHERE controle_id_opaque=? "
            "ORDER BY id DESC", (str(ctrl_opaque),)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def index_suivis_actifs(db_path=None) -> dict[str, dict[str, Any]]:
    """{ctrl_opaque: suivi actif} — pour recomposer l'état des listes sans N requêtes."""
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM controles_suivi WHERE actif=1").fetchall()
        return {r["controle_id_opaque"]: dict(r) for r in rows}
    finally:
        conn.close()


# ── Écriture (nouvelle version active + historique) ──────────────────────────

def _ecrire_version(el: dict[str, Any], *, statut_suivi: str, resultat_humain: str | None,
                    action: str, anomalie_moteur_presente: bool, responsable: str = "",
                    commentaire: str = "", justification: str = "", preuve_reference: str = "",
                    exception_portee: str = "", exception_expiration: str = "",
                    version_attendue: int | None, db_path=None) -> dict[str, Any]:
    ctrl_opaque = el["ctrl_opaque"]
    courant = suivi_actif(ctrl_opaque, db_path)
    version_courante = int((courant or {}).get("version") or 0)
    if version_attendue is not None and version_attendue != version_courante:
        raise SuiviRefuse(f"Conflit de version (attendu {version_attendue}, courant {version_courante}).")
    prise_en_charge = (courant or {}).get("date_prise_en_charge") or ""
    if action == "PRISE_EN_CHARGE" and not prise_en_charge:
        prise_en_charge = "now"
    date_resolution = "now" if statut_suivi in (ST_RESOLU, ST_ACCEPTE) else ""
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE controles_suivi SET actif=0 WHERE controle_id_opaque=? AND actif=1",
                     (ctrl_opaque,))
        cur = conn.execute(
            "INSERT INTO controles_suivi (controle_id_opaque, ctrl_pk_moteur, code_controle, module, "
            "entite_id, mois, statut_suivi, resultat_humain, responsable, commentaire, justification, "
            "preuve_reference, exception_portee, exception_expiration, date_prise_en_charge, "
            "date_resolution, anomalie_moteur_presente, version, actif, auteur, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
            "CASE WHEN ?='now' THEN strftime('%Y-%m-%dT%H:%M:%SZ','now') ELSE ? END,"
            "CASE WHEN ?='now' THEN strftime('%Y-%m-%dT%H:%M:%SZ','now') ELSE ? END,"
            "?,?,1,?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))",
            (ctrl_opaque, el.get("ctrl_pk_moteur", ""), el.get("code", ""), el.get("module", ""),
             el.get("entite_id", ""), el.get("mois", ""), statut_suivi, resultat_humain, responsable,
             commentaire, justification, preuve_reference, exception_portee, exception_expiration,
             prise_en_charge, prise_en_charge, date_resolution, date_resolution,
             1 if anomalie_moteur_presente else 0, version_courante + 1, responsable))
        new_id = cur.lastrowid
        conn.execute(
            "INSERT INTO controles_suivi_historique (controle_id_opaque, action, ancienne_valeur, "
            "nouvelle_valeur, auteur, version) VALUES (?,?,?,?,?,?)",
            (ctrl_opaque, action, (courant or {}).get("statut_suivi", ""), statut_suivi,
             responsable, version_courante + 1))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "suivi_id": new_id, "version": version_courante + 1, "statut_suivi": statut_suivi}


# ── Actions métier ───────────────────────────────────────────────────────────

def prendre_en_charge(el, *, responsable="", commentaire="", version_attendue=None, db_path=None):
    if not responsable:
        raise SuiviRefuse("Responsable requis pour la prise en charge.")
    return _ecrire_version(el, statut_suivi=ST_EN_COURS, resultat_humain=RES_A_REVOIR,
                           action="PRISE_EN_CHARGE", anomalie_moteur_presente=True,
                           responsable=responsable, commentaire=commentaire,
                           version_attendue=version_attendue, db_path=db_path)


def commenter(el, *, responsable="", commentaire="", version_attendue=None, db_path=None):
    if not commentaire.strip():
        raise SuiviRefuse("Commentaire vide.")
    courant = suivi_actif(el["ctrl_opaque"], db_path)
    statut = (courant or {}).get("statut_suivi") or ST_OUVERT
    return _ecrire_version(el, statut_suivi=statut if statut != ST_OUVERT else ST_EN_COURS,
                           resultat_humain=(courant or {}).get("resultat_humain"),
                           action="COMMENTAIRE",
                           anomalie_moteur_presente=bool((courant or {}).get("anomalie_moteur_presente", 1)),
                           responsable=responsable, commentaire=commentaire,
                           version_attendue=version_attendue, db_path=db_path)


def marquer_corrige(el, *, responsable="", preuve_reference="", anomalie_moteur_presente=True,
                    commentaire="", version_attendue=None, db_path=None):
    """« Corrigé » : exige une preuve/recalcul. Interdit si l'anomalie moteur est toujours présente."""
    if not preuve_reference.strip():
        raise SuiviRefuse("Preuve ou recalcul moteur requis pour marquer « Corrigé ».")
    if anomalie_moteur_presente:
        raise SuiviRefuse("Anomalie moteur toujours présente : « Résolu » interdit (utiliser une exception).")
    return _ecrire_version(el, statut_suivi=ST_RESOLU, resultat_humain=RES_CORRIGE,
                           action="CORRIGE", anomalie_moteur_presente=False, responsable=responsable,
                           preuve_reference=preuve_reference, commentaire=commentaire,
                           version_attendue=version_attendue, db_path=db_path)


def accepter_exception(el, *, responsable="", justification="", portee="ENTITE", expiration="",
                       preuve_reference="", anomalie_moteur_presente=True, version_attendue=None, db_path=None):
    """Exception acceptée : exige une justification. L'anomalie moteur reste affichée présente."""
    if not justification.strip():
        raise SuiviRefuse("Justification requise pour accepter une exception.")
    return _ecrire_version(el, statut_suivi=ST_ACCEPTE, resultat_humain=RES_EXCEPTION,
                           action="EXCEPTION", anomalie_moteur_presente=anomalie_moteur_presente,
                           responsable=responsable, justification=justification, exception_portee=portee,
                           exception_expiration=expiration, preuve_reference=preuve_reference,
                           version_attendue=version_attendue, db_path=db_path)


def rouvrir(el, *, responsable="", motif="", version_attendue=None, db_path=None):
    if not motif.strip():
        raise SuiviRefuse("Motif requis pour rouvrir un contrôle.")
    return _ecrire_version(el, statut_suivi=ST_ROUVERT, resultat_humain=RES_A_REVOIR,
                           action="ROUVERT", anomalie_moteur_presente=True, responsable=responsable,
                           commentaire=motif, version_attendue=version_attendue, db_path=db_path)


def annuler_decision(el, *, responsable="", version_attendue=None, db_path=None):
    """Annule la décision courante : repasse le suivi à OUVERT (historique conservé)."""
    return _ecrire_version(el, statut_suivi=ST_OUVERT, resultat_humain=None, action="ANNULATION",
                           anomalie_moteur_presente=True, responsable=responsable,
                           version_attendue=version_attendue, db_path=db_path)


def reouvrir_auto_si_reapparu(el, *, anomalie_moteur_presente: bool, db_path=None) -> dict[str, Any] | None:
    """Réapparition : contrôle marqué RÉSOLU mais anomalie moteur de nouveau présente => ROUVERT auto.

    Ne crée aucun doublon (même clé stable). Conserve l'ancienne résolution en historique. Retourne
    le résultat de réouverture, ou None si aucune réouverture nécessaire.
    """
    courant = suivi_actif(el["ctrl_opaque"], db_path)
    if not courant:
        return None
    if anomalie_moteur_presente and courant.get("statut_suivi") == ST_RESOLU:
        return _ecrire_version(el, statut_suivi=ST_ROUVERT, resultat_humain=RES_A_REVOIR,
                               action="REOUVERT_AUTO", anomalie_moteur_presente=True,
                               responsable="moteur", commentaire="Réapparu au recalcul moteur",
                               version_attendue=None, db_path=db_path)
    return None


# ── Composition état effectif (moteur + suivi) ───────────────────────────────

def etat_effectif(el: dict[str, Any], suivi: dict[str, Any] | None, anomalie_moteur_presente: bool) -> dict[str, Any]:
    """Combine anomalie moteur + suivi humain sans jamais masquer une anomalie présente."""
    statut_suivi = (suivi or {}).get("statut_suivi") or ST_OUVERT
    resultat = (suivi or {}).get("resultat_humain") or ""
    incoherence = anomalie_moteur_presente and statut_suivi == ST_RESOLU  # résolu mais anomalie présente
    a_cloturer_suivi = (not anomalie_moteur_presente) and statut_suivi in (ST_EN_COURS, ST_ROUVERT, ST_OUVERT)
    exception_active = statut_suivi == ST_ACCEPTE
    return {
        "anomalie_moteur_presente": anomalie_moteur_presente,
        "statut_suivi": statut_suivi,
        "statut_suivi_libelle": STATUTS_LIBELLES.get(statut_suivi, statut_suivi),
        "resultat_humain": resultat,
        "prise_en_charge": "TRAITE" if statut_suivi in (ST_RESOLU, ST_ACCEPTE)
                           else ("EN_COURS" if statut_suivi in (ST_EN_COURS, ST_ROUVERT) else "NON_TRAITE"),
        "incoherence_suivi": incoherence,
        "suivi_a_cloturer": a_cloturer_suivi,
        "exception_active": exception_active,
    }

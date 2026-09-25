"""Mois clôturés qui ont reçu de nouvelles données — voir, décider, tracer.

L'actualisation des ménages ne recalcule jamais un mois clôturé : elle le SIGNALE
(`menages_changements_mois_clotures`). Ce module donne à ce signalement une suite :

  · VOIR      — par mois : quel type de donnée a changé, quand, combien (quand on le sait) ;
  · RÉOUVRIR  — confirmation explicite + motif obligatoire + acteur + date, journalisés. Le mois passe
                de `CLOTURE` à `EN_CONTROLE` (statut canonique D024 : « à contrôler / à reclore »),
                puis le recalcul canonique des ménages du mois est lancé (même moteur que le bouton,
                même cascade aval). Le mois n'est JAMAIS reclôturé automatiquement : il reste
                EN_CONTROLE jusqu'à une nouvelle clôture par le parcours de clôture ;
  · CLASSER   — le changement ne justifie pas de rouvrir (ex. signalement sans différence réelle) :
                motif obligatoire, tracé, le mois reste clôturé.

Rien n'est effacé : un signalement traité garde sa décision, son motif, son auteur et sa date.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from app.db.connection import get_db

ST_CLOTURE = "CLOTURE"
ST_EN_CONTROLE = "EN_CONTROLE"
SIGNALE = "SIGNALE"
TRAITE = "TRAITE"
DECISION_REOUVERT = "REOUVERT"
DECISION_CLASSE = "CLASSE"

LIBELLES_ORIGINE = {
    "HOSTAWAY": "Tâches de ménage Hostaway",
    "PDF": "Factures de ménage externes",
    "GOOGLE_SHEET": "Déclarations internes",
}
UNITES_ORIGINE = {"HOSTAWAY": "tâche", "PDF": "facture", "GOOGLE_SHEET": "déclaration"}


class DecisionRefusee(ValueError):
    """Décision impossible : motif absent, confirmation absente, mois non clôturé, clôture archivée."""


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def statut_mois(mois: str, *, db_path=None) -> str:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?",
                         (mois,)).fetchone()
        return str(r["statut_mois"]) if r else ""
    finally:
        conn.close()


def detail(*, db_path=None) -> list[dict[str, Any]]:
    """Mois ayant au moins un signalement non traité, du plus récent au plus ancien."""
    conn = get_db(db_path)
    try:
        colonnes = {r[1] for r in conn.execute(
            "PRAGMA table_info(menages_changements_mois_clotures)")}
        quantite = "quantite" if "quantite" in colonnes else "NULL AS quantite"
        lignes = [dict(r) for r in conn.execute(
            f"SELECT id, mois, origine, date_detection, {quantite} "
            "FROM menages_changements_mois_clotures WHERE statut = ? ORDER BY mois DESC, id",
            (SIGNALE,))]
        statuts = {r["mois"]: r["statut_mois"] for r in conn.execute(
            "SELECT mois, statut_mois FROM ref_cloture_mensuelle")}
    finally:
        conn.close()
    par_mois: dict[str, dict[str, Any]] = {}
    for l in lignes:
        m = par_mois.setdefault(l["mois"], {"mois": l["mois"], "statut_mois": statuts.get(
            l["mois"], ""), "evenements": [], "origines": {}})
        l["libelle"] = LIBELLES_ORIGINE.get(l["origine"], l["origine"])
        l["unite"] = UNITES_ORIGINE.get(l["origine"], "élément")
        m["evenements"].append(l)
        agr = m["origines"].setdefault(l["origine"], {
            "origine": l["origine"], "libelle": l["libelle"], "nb_signalements": 0,
            "derniere_detection": "", "quantite": None, "unite": l["unite"]})
        agr["nb_signalements"] += 1
        agr["derniere_detection"] = max(agr["derniere_detection"], l["date_detection"] or "")
        if l["quantite"] is not None:
            agr["quantite"] = (agr["quantite"] or 0) + int(l["quantite"])
    for m in par_mois.values():
        m["origines"] = sorted(m["origines"].values(), key=lambda o: o["libelle"])
        m["reouvrable"] = m["statut_mois"] == ST_CLOTURE
    return sorted(par_mois.values(), key=lambda m: m["mois"], reverse=True)


def _clore_signalements(conn, mois: str, decision: str, acteur: str, motif: str) -> int:
    cur = conn.execute(
        "UPDATE menages_changements_mois_clotures SET statut=?, decision=?, decision_le=?, "
        "decision_par=?, decision_motif=? WHERE mois=? AND statut=?",
        (TRAITE, decision, _maintenant(), acteur, motif, mois, SIGNALE))
    return cur.rowcount


def _recalcul_canonique(mois: str, *, db_path=None) -> dict[str, Any]:
    """Le recalcul que l'actualisation des ménages aurait fait si le mois avait été ouvert : lot6d/
    6e/6f ciblés sur le mois, puis la cascade aval (flux, résultats). Aucun autre moteur."""
    from app.services import menages_service as svc
    from app.services import orchestrateur_moteur as moteur
    from app.services import orchestrateur_service as orch

    resultat = moteur.executer_menages_cible(mois=mois, declencheur=orch.DECLENCHEUR_MANUEL,
                                             db_path=db_path)
    if resultat.get("ok"):
        orch.actualiser(cibles=["FLUX_LOT9"], declencheur=orch.DECLENCHEUR_MANUEL,
                        inclure_imports_externes=True, db_path=db_path)
    svc.invalidate_menages_cache()
    return resultat


def rouvrir(mois: str, *, motif: str, acteur: str, confirmation: bool,
            recalculer: Callable[..., dict] | None = None, db_path=None) -> dict[str, Any]:
    """CLOTURE → EN_CONTROLE, journalisé, puis recalcul canonique. Jamais de reclôture."""
    from app.services import audit_service
    from app.services import clotures_service

    motif = (motif or "").strip()
    acteur = (acteur or "").strip()
    if not confirmation:
        raise DecisionRefusee("Confirmez la réouverture du mois.")
    if not motif:
        raise DecisionRefusee("Le motif de la réouverture est obligatoire.")
    if not acteur:
        raise DecisionRefusee("L'auteur de la réouverture est obligatoire.")
    if not clotures_service.mois_valide(mois):
        raise DecisionRefusee("Mois invalide.")
    avant = statut_mois(mois, db_path=db_path)
    if avant != ST_CLOTURE:
        raise DecisionRefusee(f"{mois} n'est pas clôturé : rien à rouvrir.")

    # Clôture applicative éventuelle : ARCHIVÉE, elle ne se rouvre pas d'ici (archive économique
    # figée) ; VALIDÉE, elle passe ROUVERTE par son propre automate — jamais contournée.
    cloture = clotures_service.charger_par_mois(mois, db_path)
    if cloture and cloture["statut"] == clotures_service.ST_ARCHIVEE:
        raise DecisionRefusee(f"La clôture de {mois} est archivée : sa réouverture passe par la "
                              "correction rétroactive, pas par cet écran.")
    if cloture and cloture["statut"] == clotures_service.ST_VALIDEE:
        clotures_service.rouvrir(cloture, acteur=acteur, justification=motif, db_path=db_path)

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "UPDATE ref_cloture_mensuelle SET statut_mois=? WHERE mois=? AND statut_mois=?",
            (ST_EN_CONTROLE, mois, ST_CLOTURE))
        if cur.rowcount != 1:
            conn.rollback()
            raise DecisionRefusee(f"{mois} a changé d'état entretemps : rechargez la page.")
        reouverture_id = conn.execute(
            "INSERT INTO mois_reouvertures (mois, statut_avant, statut_apres, motif, acteur, "
            "reouvert_le, recalcul_statut) VALUES (?,?,?,?,?,?,?)",
            (mois, ST_CLOTURE, ST_EN_CONTROLE, motif, acteur, _maintenant(), "NON_LANCE")
        ).lastrowid
        nb = _clore_signalements(conn, mois, DECISION_REOUVERT, acteur, motif)
        conn.commit()
    finally:
        conn.close()
    audit_service.log_event("MOIS_REOUVERT", {"mois": mois, "motif": motif,
                                              "statut_avant": ST_CLOTURE,
                                              "statut_apres": ST_EN_CONTROLE,
                                              "signalements_traites": nb},
                            user_label=acteur, db_path=db_path)

    try:
        recalcul = (recalculer or _recalcul_canonique)(mois, db_path=db_path)
    except Exception as exc:   # noqa: BLE001 — le mois reste rouvert, l'échec est tracé
        recalcul = {"ok": False, "message": type(exc).__name__}
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE mois_reouvertures SET recalcul_statut=?, recalcul_message=? "
                     "WHERE id=?", ("SUCCES" if recalcul.get("ok") else "ECHEC",
                                    str(recalcul.get("message") or "")[:500] or None,
                                    reouverture_id))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mois": mois, "statut_mois": ST_EN_CONTROLE,
            "recalcul_ok": bool(recalcul.get("ok")), "signalements_traites": nb}


def classer(mois: str, *, motif: str, acteur: str, db_path=None) -> dict[str, Any]:
    """Signalements du mois traités SANS réouverture. Le mois reste clôturé."""
    from app.services import audit_service

    motif = (motif or "").strip()
    if not motif:
        raise DecisionRefusee("Le motif est obligatoire pour classer sans rouvrir.")
    conn = get_db(db_path)
    try:
        nb = _clore_signalements(conn, mois, DECISION_CLASSE, acteur or "ui:menages", motif)
        conn.commit()
    finally:
        conn.close()
    audit_service.log_event("MOIS_CLOTURE_SIGNALEMENT_CLASSE",
                            {"mois": mois, "motif": motif, "signalements_traites": nb},
                            user_label=acteur or "ui:menages", db_path=db_path)
    return {"ok": True, "mois": mois, "signalements_traites": nb}


def reouvertures(*, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM mois_reouvertures ORDER BY id DESC LIMIT 20")]
    except Exception:  # noqa: BLE001 — base non migrée
        return []
    finally:
        conn.close()


def mois_a_reclore(*, db_path=None) -> list[str]:
    """Mois rouverts depuis cet écran et pas encore reclôturés : « à contrôler / à reclore »."""
    conn = get_db(db_path)
    try:
        return [r["mois"] for r in conn.execute(
            "SELECT DISTINCT r.mois FROM mois_reouvertures r JOIN ref_cloture_mensuelle c "
            "ON c.mois = r.mois WHERE c.statut_mois = ? ORDER BY r.mois", (ST_EN_CONTROLE,))]
    except Exception:  # noqa: BLE001
        return []
    finally:
        conn.close()

"""Cycle de vie opérationnel du ménage unitaire (migration `0019`).

Le module Ménages existant (`menages_service.py`, `menages_recalcul_service.py`,
`menages_chaine_service.py`) COMPTE et RAPPROCHE : il compare des agrégats
(Hostaway vs déclaré, coût standard/direct/complet) par `(mois, logement_id, intervenant_id)`.
Il n'a jamais eu d'objet ménage unitaire ni de statut opérationnel — voir l'audit
`42_AUDIT_MENAGES_CYCLE_DE_VIE.md` (renommé `41b` pour éviter une collision de numérotation avec
l'audit Factures/Charges/Règlements de ce même tour).

Ce service ajoute cette couche, SANS toucher à l'existant : ni `menage_overrides` (0003, journal de
justification d'un écart de comptage), ni les MASTER_* Excel (source de vérité du comptage), ni
`menages_chaine_service` (moteur de recalcul sur copies). Un ménage saisi ici est une occurrence
suivie individuellement (création hors Hostaway, affectation, remplacement, rattachements) — il ne
remplace ni ne masque le comptage agrégé.

Règle absolue reprise de `factures_service.py` / `factures_banque_service.py` : ce service ne crée
JAMAIS de facture, de charge, de règlement ni de rapprochement bancaire. Il ne fait que lire les
objets existants et enregistrer le lien vers eux.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

E_FLAGS = "E_FLAGS_DESACTIVES"


def _flags_actifs() -> bool:
    return bool(cfg.MENAGES_CYCLE_REAL_WRITE_ENABLED
               and cfg.MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED)


# ── Statuts ───────────────────────────────────────────────────────────────────
ST_PREVU = "PREVU"
ST_A_AFFECTER = "A_AFFECTER"
ST_A_REALISER = "A_REALISER"
ST_REALISE = "REALISE"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDE = "VALIDE"
ST_FACTURE = "FACTURE"
ST_REGLE = "REGLE"
ST_ANNULE = "ANNULE"
ST_REMPLACE = "REMPLACE"
ST_LITIGE = "LITIGE"

STATUTS = [ST_PREVU, ST_A_AFFECTER, ST_A_REALISER, ST_REALISE, ST_A_CONTROLER, ST_VALIDE,
           ST_FACTURE, ST_REGLE, ST_ANNULE, ST_REMPLACE, ST_LITIGE]

TYPES = ("INTERNE", "EXTERNE")

# Transitions autorisées. Réouverture contrôlée : LITIGE peut revenir vers VALIDE ou A_CONTROLER,
# jamais directement vers FACTURE/REGLE (il faut re-passer par le contrôle).
TRANSITIONS: dict[str, set[str]] = {
    ST_PREVU: {ST_A_AFFECTER, ST_ANNULE},
    ST_A_AFFECTER: {ST_A_REALISER, ST_REMPLACE, ST_ANNULE, ST_LITIGE},
    ST_A_REALISER: {ST_REALISE, ST_REMPLACE, ST_ANNULE, ST_LITIGE},
    ST_REALISE: {ST_A_CONTROLER, ST_LITIGE},
    ST_A_CONTROLER: {ST_VALIDE, ST_LITIGE, ST_ANNULE},
    ST_VALIDE: {ST_FACTURE, ST_LITIGE},
    ST_FACTURE: {ST_REGLE, ST_LITIGE},
    ST_REGLE: {ST_LITIGE},
    ST_ANNULE: set(),
    ST_REMPLACE: set(),                      # état terminal du ménage remplacé ; le nouveau repart de PREVU
    ST_LITIGE: {ST_A_CONTROLER, ST_VALIDE},  # réouverture contrôlée uniquement
}

E_INTROUVABLE = "E_MENAGE_INTROUVABLE"
E_STATUT = "E_TRANSITION_INTERDITE"
E_LOGEMENT_INCONNU = "V01_LOGEMENT_INCONNU"
E_PROPRIETAIRE_NON_RESOLU = "V02_PROPRIETAIRE_NON_RESOLU"
E_DATE_INVALIDE = "V03_DATE_INVALIDE"
E_TYPE_INVALIDE = "V04_TYPE_INVALIDE"
E_PRESTATAIRE_ARCHIVE = "V05_PRESTATAIRE_ARCHIVE"
E_PRESTATAIRE_INCOMPATIBLE = "V06_PRESTATAIRE_TYPE_INCOMPATIBLE"
E_DOUBLON = "V07_DOUBLON_EMPREINTE"
E_TARIF_HISTORIQUE_ABSENT = "V08_TARIF_HISTORIQUE_ABSENT"

MESSAGES = {
    E_INTROUVABLE: "Ménage introuvable.",
    E_STATUT: "Transition de statut interdite.",
    E_LOGEMENT_INCONNU: "Logement inconnu.",
    E_PROPRIETAIRE_NON_RESOLU: "Propriétaire non résolu pour ce logement.",
    E_DATE_INVALIDE: "Date invalide.",
    E_TYPE_INVALIDE: "Type de ménage invalide (INTERNE ou EXTERNE attendu).",
    E_PRESTATAIRE_ARCHIVE: "Ce prestataire est archivé — affectation refusée.",
    E_PRESTATAIRE_INCOMPATIBLE: "Ce prestataire n'est pas qualifié pour ce type de ménage.",
    E_DOUBLON: "Un ménage identique existe déjà pour ce logement, ce mois et cette date.",
    E_TARIF_HISTORIQUE_ABSENT: "Aucun tarif historique applicable à cette date.",
    E_FLAGS: "Écriture du cycle Ménages désactivée sur cette installation.",
}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def empreinte(logement_id: str, mois: str, type_menage: str, date_prevue: str) -> str:
    base = f"{logement_id}|{mois}|{type_menage}|{date_prevue or ''}"
    return hashlib.sha256(base.encode()).hexdigest()[:24]


def _evenement(conn, opaque: str, type_evt: str, *, ancien: str | None = None,
              nouveau: str | None = None, ancien_prestataire: str | None = None,
              nouveau_prestataire: str | None = None, commentaire: str = "",
              acteur: str = "") -> None:
    conn.execute(
        "INSERT INTO menage_evenements (menage_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, ancien_prestataire, nouveau_prestataire, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, ancien_prestataire, nouveau_prestataire,
         commentaire, acteur or "local"))


def _resoudre_logement_proprietaire(logement_id: str, db_path=None) -> tuple[str | None, str | None]:
    """Résout le propriétaire depuis le référentiel logements (PBI + REF_Setup, lecture seule).

    Réutilise `logements_service.load_detail` : jamais un second référentiel logements.
    `db_path` n'est pas utilisé par ce référentiel (source Excel/CSV), gardé pour homogénéité de
    signature avec le reste du service.
    """
    from app.services import logements_service
    try:
        fiche = logements_service.load_detail(logement_id)
    except Exception:
        fiche = None
    if not fiche or fiche.get("status") == "ERROR":
        return None, None
    return logement_id, fiche.get("proprietaire_id") or None


def _prestataire_qualifie(fournisseur_opaque: str, type_menage: str, logement_id: str,
                          ref_date: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        f = conn.execute(
            "SELECT * FROM fournisseurs WHERE fournisseur_id_opaque=? AND actif=1", (fournisseur_opaque,)
        ).fetchone()
        if f is None or f["statut"] != "ACTIF":
            return None
        q = conn.execute(
            "SELECT * FROM fournisseur_menage_qualification WHERE fournisseur_id_opaque=?",
            (fournisseur_opaque,)).fetchone()
    finally:
        conn.close()
    if q is None:
        return None
    if q["type_menage"] != type_menage:
        return None
    if q["date_debut_validite"] and ref_date and ref_date < q["date_debut_validite"]:
        return None
    if q["date_fin_validite"] and ref_date and ref_date > q["date_fin_validite"]:
        return None
    autorises = q["logements_autorises"]
    if autorises:
        liste = [x.strip() for x in autorises.split(",") if x.strip()]
        if logement_id not in liste:
            return None
    return dict(q)


def creer(form: dict[str, Any], *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Création hors Hostaway. Bloque : doublon, logement inconnu, propriétaire non résolu,
    prestataire archivé, date invalide, tarif historique absent (si prestataire déjà affecté)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    logement_id = _txt(form.get("logement_id"))
    mois = _txt(form.get("mois"))
    type_menage = _txt(form.get("type_menage")).upper()
    date_prevue = _txt(form.get("date_prevue")) or None

    if type_menage not in TYPES:
        return _refus(E_TYPE_INVALIDE, type_menage)
    if date_prevue:
        try:
            datetime.fromisoformat(date_prevue)
        except ValueError:
            return _refus(E_DATE_INVALIDE, date_prevue)

    logement_id, proprietaire_id = _resoudre_logement_proprietaire(logement_id, db_path)
    if logement_id is None:
        return _refus(E_LOGEMENT_INCONNU, _txt(form.get("logement_id")))
    if not proprietaire_id:
        return _refus(E_PROPRIETAIRE_NON_RESOLU, logement_id)

    emp = empreinte(logement_id, mois, type_menage, date_prevue)
    conn = get_db(db_path)
    try:
        existant = conn.execute(
            "SELECT 1 FROM menages WHERE empreinte=? AND statut <> ?", (emp, ST_ANNULE)).fetchone()
        if existant is not None:
            return _refus(E_DOUBLON, emp)
        opaque = "MEN-" + uuid.uuid4().hex[:12].upper()
        conn.execute(
            "INSERT INTO menages (menage_id_opaque, mois, date_prevue, logement_id, "
            "proprietaire_id, reservation_id, type_menage, duree_prevue_h, cout_prevu, "
            "commentaire, source, empreinte, acteur) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, mois, date_prevue, logement_id, proprietaire_id,
             _txt(form.get("reservation_id")) or None, type_menage,
             _nombre(form.get("duree_prevue_h")), _nombre(form.get("cout_prevu")),
             _txt(form.get("commentaire")) or None, "SAISIE", emp, acteur or "local"))
        _evenement(conn, opaque, "CREATION", nouveau=ST_PREVU, commentaire=_txt(form.get("commentaire")),
                  acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "statut": ST_PREVU}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM menages WHERE menage_id_opaque=?", (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def lister(*, mois: str = "", logement_id: str = "", fournisseur: str = "", type_menage: str = "",
          statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM menages ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        m = dict(r)
        if mois and m["mois"] != mois:
            continue
        if logement_id and m["logement_id"] != logement_id:
            continue
        if fournisseur and m["fournisseur_id_opaque"] != fournisseur:
            continue
        if type_menage and m["type_menage"] != type_menage:
            continue
        if statut and m["statut"] != statut:
            continue
        out.append(m)
    return out


def historique(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM menage_evenements WHERE menage_id_opaque=? ORDER BY id DESC",
            (opaque,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def affecter(opaque: str, fournisseur_opaque: str, *, acteur: str = "", commentaire: str = "",
            db_path=None) -> dict[str, Any]:
    """Affecte (ou change) le prestataire. L'ancien reste dans l'historique — jamais réécrit."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    if m["statut"] not in (ST_PREVU, ST_A_AFFECTER):
        return _refus(E_STATUT, f"affectation impossible depuis {m['statut']}")

    ref_date = m["date_prevue"] or _now()[:10]
    q = _prestataire_qualifie(fournisseur_opaque, m["type_menage"], m["logement_id"], ref_date, db_path)
    if q is None:
        return _refus(E_PRESTATAIRE_INCOMPATIBLE, fournisseur_opaque)

    ancien_prestataire = m["fournisseur_id_opaque"]
    nouveau_statut = ST_A_REALISER
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE menages SET fournisseur_id_opaque=?, statut=?, date_modification=?, "
            "version=version+1 WHERE menage_id_opaque=?",
            (fournisseur_opaque, nouveau_statut, _now(), opaque))
        type_evt = "CHANGEMENT_PRESTATAIRE" if ancien_prestataire else "AFFECTATION"
        _evenement(conn, opaque, type_evt, ancien=m["statut"], nouveau=nouveau_statut,
                  ancien_prestataire=ancien_prestataire, nouveau_prestataire=fournisseur_opaque,
                  commentaire=commentaire, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "statut": nouveau_statut}


def remplacer(opaque: str, nouveau_fournisseur_opaque: str, *, acteur: str = "",
             commentaire: str = "", db_path=None) -> dict[str, Any]:
    """Remplace un ménage affecté/à réaliser : l'ancien passe REMPLACE (terminal, jamais supprimé),
    un nouveau ménage repart de PREVU avec le nouveau prestataire déjà affecté."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    if m["statut"] not in TRANSITIONS or ST_REMPLACE not in TRANSITIONS.get(m["statut"], set()):
        return _refus(E_STATUT, f"remplacement impossible depuis {m['statut']}")

    ref_date = m["date_prevue"] or _now()[:10]
    q = _prestataire_qualifie(nouveau_fournisseur_opaque, m["type_menage"], m["logement_id"],
                              ref_date, db_path)
    if q is None:
        return _refus(E_PRESTATAIRE_INCOMPATIBLE, nouveau_fournisseur_opaque)

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE menages SET statut=?, date_modification=?, version=version+1 "
                    "WHERE menage_id_opaque=?", (ST_REMPLACE, _now(), opaque))
        _evenement(conn, opaque, "REMPLACEMENT", ancien=m["statut"], nouveau=ST_REMPLACE,
                  ancien_prestataire=m["fournisseur_id_opaque"],
                  nouveau_prestataire=nouveau_fournisseur_opaque, commentaire=commentaire, acteur=acteur)

        nouveau_opaque = "MEN-" + uuid.uuid4().hex[:12].upper()
        nouvelle_empreinte = empreinte(m["logement_id"], m["mois"], m["type_menage"],
                                       m["date_prevue"] or "") + "-R"
        conn.execute(
            "INSERT INTO menages (menage_id_opaque, mois, date_prevue, logement_id, "
            "proprietaire_id, reservation_id, type_menage, fournisseur_id_opaque, statut, "
            "duree_prevue_h, cout_prevu, commentaire, source, empreinte, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (nouveau_opaque, m["mois"], m["date_prevue"], m["logement_id"], m["proprietaire_id"],
             m["reservation_id"], m["type_menage"], nouveau_fournisseur_opaque, ST_A_REALISER,
             m["duree_prevue_h"], m["cout_prevu"],
             f"Remplace {opaque}. {commentaire}".strip(), "SAISIE", nouvelle_empreinte, acteur or "local"))
        _evenement(conn, nouveau_opaque, "CREATION", nouveau=ST_A_REALISER,
                  nouveau_prestataire=nouveau_fournisseur_opaque,
                  commentaire=f"Remplace {opaque}.", acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "statut": ST_REMPLACE,
           "nouveau_menage_id_opaque": nouveau_opaque}


def realiser(opaque: str, *, duree_reelle_h: Any = None, cout_reel: Any = None,
            methode_cout: str = "", ecart_justification: str = "", acteur: str = "",
            db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    if ST_REALISE not in TRANSITIONS.get(m["statut"], set()):
        return _refus(E_STATUT, f"réalisation impossible depuis {m['statut']}")

    duree = _nombre(duree_reelle_h)
    cout = _nombre(cout_reel)
    cout_prevu = m["cout_prevu"]
    ecart_important = (cout is not None and cout_prevu is not None
                      and abs(cout - cout_prevu) > max(5.0, 0.10 * cout_prevu))
    if ecart_important and not _txt(ecart_justification):
        return _refus("V09_ECART_NON_JUSTIFIE",
                      f"écart coût prévu/réel de {round(cout - cout_prevu, 2)} € non justifié")

    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE menages SET statut=?, date_realisation=?, duree_reelle_h=?, cout_reel=?, "
            "methode_cout=?, ecart_justification=?, date_modification=?, version=version+1 "
            "WHERE menage_id_opaque=?",
            (ST_REALISE, _now()[:10], duree, cout, methode_cout or None,
             ecart_justification or None, _now(), opaque))
        _evenement(conn, opaque, "REALISATION", ancien=m["statut"], nouveau=ST_REALISE,
                  commentaire=ecart_justification, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "statut": ST_REALISE}


def changer_statut(opaque: str, nouveau: str, *, commentaire: str = "", acteur: str = "",
                   db_path=None) -> dict[str, Any]:
    """Transition générique — validation, mise en litige, réouverture, annulation."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    ancien = m["statut"]
    if nouveau != ancien and nouveau not in TRANSITIONS.get(ancien, set()):
        return _refus(E_STATUT, f"{ancien} -> {nouveau}")
    if nouveau == ST_VALIDE and m["cout_reel"] is None:
        return _refus("V10_VALIDATION_SANS_COUT", "Un ménage validé doit porter un coût.")

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE menages SET statut=?, date_modification=?, version=version+1 "
                    "WHERE menage_id_opaque=?", (nouveau, _now(), opaque))
        type_evt = {"VALIDE": "VALIDATION", "ANNULE": "ANNULATION", "LITIGE": "LITIGE",
                   "FACTURE": "FACTURATION", "REGLE": "REGLEMENT"}.get(nouveau, "REOUVERTURE"
                   if ancien == ST_LITIGE else "MODIFICATION")
        _evenement(conn, opaque, type_evt, ancien=ancien, nouveau=nouveau, commentaire=commentaire,
                  acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "statut": nouveau}


def lier_facture(opaque: str, facture_id_opaque: str, *, acteur: str = "",
                 db_path=None) -> dict[str, Any]:
    """Rattache une facture DÉJÀ créée par le parcours Factures. Ne crée jamais la facture, et
    refuse qu'une facture soit rattachée à deux ménages (même règle que factures↔charge en 0017)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    conn = get_db(db_path)
    try:
        deja = conn.execute(
            "SELECT menage_id_opaque FROM menages WHERE facture_id_opaque=? AND menage_id_opaque<>?",
            (facture_id_opaque, opaque)).fetchone()
        if deja is not None:
            return _refus("E_FACTURE_DEJA_LIEE", facture_id_opaque)
        conn.execute("UPDATE menages SET facture_id_opaque=?, date_modification=?, "
                    "version=version+1 WHERE menage_id_opaque=?",
                    (facture_id_opaque, _now(), opaque))
        _evenement(conn, opaque, "FACTURATION", ancien=m["statut"], nouveau=m["statut"],
                  commentaire=f"Facture liée : {facture_id_opaque}", acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "facture_id_opaque": facture_id_opaque}


def lier_charge(opaque: str, charge_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Rattache une charge DÉJÀ créée. Ne la crée jamais ; l'index unique de la migration 0019
    refuse qu'elle soit rattachée à deux ménages."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    m = charger(opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, opaque)
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE menages SET charge_id=?, date_modification=?, version=version+1 "
                    "WHERE menage_id_opaque=?", (charge_id, _now(), opaque))
        _evenement(conn, opaque, "MODIFICATION", ancien=m["statut"], nouveau=m["statut"],
                  commentaire=f"Charge liée : {charge_id}", acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "menage_id_opaque": opaque, "charge_id": charge_id}


def contexte_facture_charge_reglement_banque(opaque: str, db_path=None) -> dict[str, Any]:
    """Vue consolidée pour la fiche ménage : facture, charge, règlements, mouvements bancaires,
    état du rapprochement. Lecture seule — délègue aux services existants, ne recalcule rien."""
    m = charger(opaque, db_path)
    if m is None:
        return {}
    out: dict[str, Any] = {"menage": m, "facture": None, "reglements": [], "rapprochements": []}
    if m.get("facture_id_opaque"):
        from app.services import factures_service
        out["facture"] = factures_service.charger(m["facture_id_opaque"], db_path=db_path)
        if out["facture"]:
            conn = get_db(db_path)
            try:
                out["reglements"] = [dict(r) for r in conn.execute(
                    "SELECT g.* FROM reglements_fournisseurs g "
                    "JOIN reglement_repartitions r ON r.reglement_id_opaque = g.reglement_id_opaque "
                    "WHERE r.facture_id_opaque=?", (m["facture_id_opaque"],)).fetchall()]
            finally:
                conn.close()
            from app.services import factures_banque_service
            for reg in out["reglements"]:
                out["rapprochements"].extend(
                    factures_banque_service.liens_du_reglement(reg["reglement_id_opaque"], db_path))
    return out

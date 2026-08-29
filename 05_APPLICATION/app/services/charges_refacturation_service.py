"""Positions de refacturation des charges (mission 15, migration 0062).

POINT D'ENTRÉE UNIQUE : la saisie/modification d'une charge (`charges_saisie_service.creer`/
`modifier`), jamais un second flux de création. `synchroniser_depuis_charge()` est appelée par ces
deux fonctions juste après l'écriture en base ; ce module ne doit JAMAIS être appelé pour créer une
position autrement qu'à partir d'une charge existante.

Trois notions distinctes, jamais confondues :
  CHARGE (table `charges`)               — l'événement économique d'origine, inchangé.
  POSITION (`charges_refacturation_positions`) — montant disponible à récupérer, créée
                                            automatiquement dès `refacturable='OUI'`.
  IMPUTATION (événement `IMPUTATION`/`IMPUTATION_PARTIELLE`) — décision humaine réalisée au moment
                                            de la VALIDATION effective d'une facture, jamais à
                                            l'aperçu/recalcul (`imputer()` n'est appelée que par la
                                            validation de facture, dans SA transaction).

Une case `refacturable='OUI'` ne signifie plus "cette charge sera automatiquement facturée" mais
"cette charge est disponible pour être refacturée" — la décision reste humaine.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

_TRAVAIL_DIR = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

from lib_ref_history import resolve_management_period  # noqa: E402

STATUT_A_TRAITER = "A_TRAITER"
STATUT_DISPONIBLE = "DISPONIBLE"
STATUT_REPORTEE = "REPORTEE"
STATUT_PARTIELLEMENT_IMPUTEE = "PARTIELLEMENT_IMPUTEE"
STATUT_IMPUTEE = "IMPUTEE"
STATUT_NON_REFACTUREE = "NON_REFACTUREE"

STATUTS_OUVERTS = (STATUT_A_TRAITER, STATUT_DISPONIBLE, STATUT_REPORTEE,
                  STATUT_PARTIELLEMENT_IMPUTEE)
STATUTS_CLOS = (STATUT_IMPUTEE, STATUT_NON_REFACTUREE)

EVT_CREATION = "CREATION_POSITION"
EVT_MISE_A_JOUR = "MISE_A_JOUR_CHARGE"
EVT_LIBERATION = "LIBERATION"
EVT_REPORT = "REPORT"
EVT_IMPUTATION = "IMPUTATION"
EVT_IMPUTATION_PARTIELLE = "IMPUTATION_PARTIELLE"
EVT_NON_REFACTURATION = "NON_REFACTURATION"

E_INTROUVABLE = "POSREF_INTROUVABLE"
E_STATUT_CLOS = "POSREF_STATUT_CLOS"
E_MONTANT_INVALIDE = "POSREF_MONTANT_INVALIDE"
E_JUSTIFICATION_MANQUANTE = "POSREF_JUSTIFICATION_MANQUANTE"
E_MONTANT_DEPASSE = "POSREF_MONTANT_DEPASSE"

TOLERANCE = 0.005


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _round(v: Any) -> float:
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return 0.0


def _position_par_charge(conn, charge_id: str) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM charges_refacturation_positions WHERE charge_id = ?", (charge_id,)
    ).fetchone()
    return dict(r) if r else None


def _position(conn, position_id: str) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM charges_refacturation_positions WHERE position_id = ?", (position_id,)
    ).fetchone()
    return dict(r) if r else None


def _journaliser(conn, position_id: str, evenement: str, *, montant=None, facture_id=None,
                 acteur: str = "", motif: str = "", avant=None, apres=None) -> None:
    import json
    conn.execute(
        "INSERT INTO charges_refacturation_evenements (position_id, evenement, montant, "
        "facture_id, acteur, motif, avant_json, apres_json) VALUES (?,?,?,?,?,?,?,?)",
        (position_id, evenement, montant, facture_id, acteur or None, motif or None,
         json.dumps(avant, default=str) if avant is not None else None,
         json.dumps(apres, default=str) if apres is not None else None))


def _resoudre_proprietaire(conn, logement_id: str, date_charge: str) -> str | None:
    """Propriétaire du logement à la date de la charge, uniquement si résolu SANS ambiguïté
    (`resolve_management_period`, même mécanisme que partout ailleurs) — jamais inventé."""
    rows = [dict(r) for r in conn.execute(
        "SELECT gestion_id, logement_id, proprietaire_id, date_debut, date_fin, statut_gestion "
        "FROM ref_gestion_logements_hist WHERE logement_id = ?", (logement_id,))]
    if not rows:
        return None
    res = resolve_management_period(rows, logement_id=logement_id, date_arrivee=date_charge)
    return res.value if res.status == "OK" else None


def synchroniser_depuis_charge(charge_id: str, *, acteur: str = "", conn=None,
                               db_path=None) -> None:
    """Appelée par `charges_saisie_service.creer()`/`modifier()` juste après l'écriture de la
    charge — POINT D'ENTRÉE UNIQUE. Ne lève jamais d'exception métier (silencieuse sur les cas
    normaux), la création/modification de la charge elle-même ne doit jamais échouer à cause de la
    synchronisation de sa position."""
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        charge = conn.execute("SELECT * FROM charges WHERE charge_id = ?", (charge_id,)).fetchone()
        if charge is None:
            return
        charge = dict(charge)
        existante = _position_par_charge(conn, charge_id)
        refacturable = str(charge.get("refacturable") or "").strip().upper() == "OUI"

        if not refacturable:
            if existante and existante["actif"] and existante["statut"] not in STATUTS_CLOS:
                conn.execute(
                    "UPDATE charges_refacturation_positions SET actif=0, statut=?, "
                    "date_derniere_decision=?, acteur_derniere_decision=? WHERE position_id=?",
                    (STATUT_NON_REFACTUREE, _maintenant(), acteur or None,
                     existante["position_id"]))
                _journaliser(conn, existante["position_id"], EVT_LIBERATION, acteur=acteur,
                            motif="Charge repassee refacturable=NON",
                            avant={"statut": existante["statut"]},
                            apres={"statut": STATUT_NON_REFACTUREE, "actif": 0})
            if connexion_locale:
                conn.commit()
            return

        proprietaire_id = charge.get("proprietaire_id") or None
        logement_id = charge.get("logement_id") or None
        if not proprietaire_id and logement_id:
            proprietaire_id = _resoudre_proprietaire(conn, logement_id, charge.get("date_charge"))

        if existante is None:
            statut = STATUT_DISPONIBLE if (proprietaire_id or logement_id) else STATUT_A_TRAITER
            position_id = "POSREF-" + uuid.uuid4().hex[:12].upper()
            montant = _round(charge.get("montant"))
            conn.execute(
                "INSERT INTO charges_refacturation_positions (position_id, charge_id, "
                "montant_origine, montant_eligible, proprietaire_id, logement_id, statut, "
                "date_disponibilite) VALUES (?,?,?,?,?,?,?,?)",
                (position_id, charge_id, montant, montant, proprietaire_id, logement_id, statut,
                 charge.get("date_charge")))
            _journaliser(conn, position_id, EVT_CREATION, montant=montant, acteur=acteur,
                        apres={"statut": statut, "proprietaire_id": proprietaire_id,
                               "logement_id": logement_id})
        else:
            # Charge modifiée : ne resynchronise le montant que si rien n'a encore été imputé —
            # une position déjà (partiellement) imputée ne doit jamais voir son montant éligible
            # changer sous elle (l'invariant montant_impute_total <= montant_eligible prime).
            if _round(existante["montant_impute_total"]) == 0:
                montant = _round(charge.get("montant"))
                nouveau_statut = existante["statut"]
                if existante["statut"] == STATUT_A_TRAITER and (proprietaire_id or logement_id):
                    nouveau_statut = STATUT_DISPONIBLE
                conn.execute(
                    "UPDATE charges_refacturation_positions SET montant_origine=?, "
                    "montant_eligible=?, proprietaire_id=?, logement_id=?, statut=?, actif=1 "
                    "WHERE position_id=?",
                    (montant, montant, proprietaire_id, logement_id, nouveau_statut,
                     existante["position_id"]))
                _journaliser(conn, existante["position_id"], EVT_MISE_A_JOUR, montant=montant,
                            acteur=acteur, avant={"montant": existante["montant_origine"],
                                                  "statut": existante["statut"]},
                            apres={"montant": montant, "statut": nouveau_statut})
        if connexion_locale:
            conn.commit()
    finally:
        if connexion_locale:
            conn.close()


def _enrichir(conn, pos: dict[str, Any]) -> dict[str, Any]:
    charge = conn.execute(
        "SELECT date_charge, categorie_charge_id, associe_id, commentaire, justificatif "
        "FROM charges WHERE charge_id = ?", (pos["charge_id"],)).fetchone()
    charge = dict(charge) if charge else {}
    restant = _round(pos["montant_eligible"] - pos["montant_impute_total"])
    return {
        **pos,
        "date_charge": charge.get("date_charge"),
        "fournisseur": charge.get("associe_id"),
        "description": charge.get("commentaire"),
        "justificatif": charge.get("justificatif"),
        "montant_restant": restant,
        "proposable": (pos["statut"] in (STATUT_DISPONIBLE, STATUT_REPORTEE,
                                        STATUT_PARTIELLEMENT_IMPUTEE)
                      and pos["proprietaire_id"] is not None and restant > TOLERANCE),
    }


def lister(*, statut: str | None = None, proprietaire_id: str | None = None,
          logement_id: str | None = None, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        clauses, args = ["actif = 1"], []
        if statut == "PROPOSABLES":
            clauses.append("statut IN (?,?,?)")
            args += [STATUT_DISPONIBLE, STATUT_REPORTEE, STATUT_PARTIELLEMENT_IMPUTEE]
        elif statut:
            clauses.append("statut = ?")
            args.append(statut)
        if proprietaire_id:
            clauses.append("proprietaire_id = ?")
            args.append(proprietaire_id)
        if logement_id:
            clauses.append("logement_id = ?")
            args.append(logement_id)
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM charges_refacturation_positions WHERE {' AND '.join(clauses)} "
            "ORDER BY date_creation", args)]
        return [_enrichir(conn, r) for r in rows]
    finally:
        conn.close()


def proposer_pour_facture(proprietaire_id: str, logement_id: str | None = None,
                         db_path=None) -> list[dict[str, Any]]:
    """Lecture seule : ne consomme rien. Les positions disponibles/reportées/partiellement
    imputées pour ce propriétaire (et ce logement si fourni), avec solde > 0."""
    positions = lister(statut="PROPOSABLES", proprietaire_id=proprietaire_id,
                       logement_id=logement_id, db_path=db_path)
    return [p for p in positions if p["montant_restant"] > TOLERANCE]


def imputer(position_id: str, montant: float, *, facture_id: str, acteur: str = "",
           justification: str | None = None, conn=None, db_path=None) -> dict[str, Any]:
    """Consomme un montant sur la position. N'est appelée QUE par la validation effective d'une
    facture, dans SA transaction (`conn` partagé) — jamais à l'aperçu. Si `conn` est fourni,
    l'appelant gère le commit/rollback (atomicité avec la création de la facture)."""
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        pos = _position(conn, position_id)
        if pos is None:
            return _refus(E_INTROUVABLE, f"Position introuvable : {position_id}.")
        if pos["statut"] in STATUTS_CLOS:
            return _refus(E_STATUT_CLOS, f"Position deja cloturee ({pos['statut']}).")
        montant = _round(montant)
        if montant <= 0:
            return _refus(E_MONTANT_INVALIDE, "Le montant impute doit etre positif.")
        restant_avant = _round(pos["montant_eligible"] - pos["montant_impute_total"])
        if montant - restant_avant > TOLERANCE:
            return _refus(E_MONTANT_DEPASSE,
                         f"Montant {montant} superieur au solde disponible {restant_avant}.")
        if abs(montant - restant_avant) > TOLERANCE and not justification:
            return _refus(E_JUSTIFICATION_MANQUANTE,
                         "Justification obligatoire : montant impute different du solde propose "
                         f"({restant_avant}).")
        nouveau_total = _round(pos["montant_impute_total"] + montant)
        complet = abs(nouveau_total - pos["montant_eligible"]) <= TOLERANCE
        nouveau_statut = STATUT_IMPUTEE if complet else STATUT_PARTIELLEMENT_IMPUTEE
        conn.execute(
            "UPDATE charges_refacturation_positions SET montant_impute_total=?, statut=?, "
            "derniere_decision=?, derniere_justification=?, date_derniere_decision=?, "
            "acteur_derniere_decision=? WHERE position_id=?",
            (nouveau_total, nouveau_statut,
             EVT_IMPUTATION if complet else EVT_IMPUTATION_PARTIELLE, justification, _maintenant(),
             acteur or None, position_id))
        _journaliser(conn, position_id,
                    EVT_IMPUTATION if complet else EVT_IMPUTATION_PARTIELLE,
                    montant=montant, facture_id=facture_id, acteur=acteur, motif=justification,
                    avant={"montant_impute_total": pos["montant_impute_total"],
                           "statut": pos["statut"]},
                    apres={"montant_impute_total": nouveau_total, "statut": nouveau_statut})
        if connexion_locale:
            conn.commit()
        return {"ok": True, "position_id": position_id, "montant_impute": montant,
               "montant_restant": _round(pos["montant_eligible"] - nouveau_total),
               "statut": nouveau_statut}
    except Exception:
        if connexion_locale:
            conn.rollback()
        raise
    finally:
        if connexion_locale:
            conn.close()


def reporter(position_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ne consomme rien : la position reste disponible pour une facture ulterieure."""
    conn = get_db(db_path)
    try:
        pos = _position(conn, position_id)
        if pos is None:
            return _refus(E_INTROUVABLE, f"Position introuvable : {position_id}.")
        if pos["statut"] in STATUTS_CLOS:
            return _refus(E_STATUT_CLOS, f"Position deja cloturee ({pos['statut']}).")
        conn.execute(
            "UPDATE charges_refacturation_positions SET statut=?, derniere_decision=?, "
            "date_derniere_decision=?, acteur_derniere_decision=? WHERE position_id=?",
            (STATUT_REPORTEE, EVT_REPORT, _maintenant(), acteur or None, position_id))
        _journaliser(conn, position_id, EVT_REPORT, acteur=acteur,
                    avant={"statut": pos["statut"]}, apres={"statut": STATUT_REPORTEE})
        conn.commit()
        return {"ok": True, "position_id": position_id, "statut": STATUT_REPORTEE}
    finally:
        conn.close()


def ne_pas_refacturer(position_id: str, *, acteur: str = "", justification: str,
                      db_path=None) -> dict[str, Any]:
    """Cloture definitivement la possibilite de refacturation. Justification obligatoire."""
    if not (justification or "").strip():
        return _refus(E_JUSTIFICATION_MANQUANTE, "Justification obligatoire.")
    conn = get_db(db_path)
    try:
        pos = _position(conn, position_id)
        if pos is None:
            return _refus(E_INTROUVABLE, f"Position introuvable : {position_id}.")
        if pos["statut"] in STATUTS_CLOS:
            return _refus(E_STATUT_CLOS, f"Position deja cloturee ({pos['statut']}).")
        conn.execute(
            "UPDATE charges_refacturation_positions SET statut=?, derniere_decision=?, "
            "derniere_justification=?, date_derniere_decision=?, acteur_derniere_decision=? "
            "WHERE position_id=?",
            (STATUT_NON_REFACTUREE, EVT_NON_REFACTURATION, justification, _maintenant(),
             acteur or None, position_id))
        _journaliser(conn, position_id, EVT_NON_REFACTURATION, acteur=acteur, motif=justification,
                    avant={"statut": pos["statut"]}, apres={"statut": STATUT_NON_REFACTUREE})
        conn.commit()
        return {"ok": True, "position_id": position_id, "statut": STATUT_NON_REFACTUREE}
    finally:
        conn.close()


def historique(position_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM charges_refacturation_evenements WHERE position_id = ? "
            "ORDER BY id", (position_id,))]
    finally:
        conn.close()


def montant_realise(mois: str, logement_id: str | None = None, proprietaire_id: str | None = None,
                    db_path=None) -> float:
    """Total réellement imputé pour un mois (et logement/propriétaire) — via les factures dont le
    mois correspond. Consommé par Lot10 pour `charges_exceptionnelles_refacturees` : SEULES les
    imputations réellement décidées comptent, jamais les positions simplement disponibles."""
    conn = get_db(db_path)
    try:
        clauses, args = ["e.evenement IN (?,?)", "f.mois = ?"], \
            [EVT_IMPUTATION, EVT_IMPUTATION_PARTIELLE, mois]
        if logement_id:
            clauses.append("p.logement_id = ?")
            args.append(logement_id)
        if proprietaire_id:
            clauses.append("p.proprietaire_id = ?")
            args.append(proprietaire_id)
        if not conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='factures_proprietaires'"
        ).fetchone():
            return 0.0
        row = conn.execute(
            "SELECT ROUND(SUM(e.montant), 2) FROM charges_refacturation_evenements e "
            "JOIN charges_refacturation_positions p ON p.position_id = e.position_id "
            "JOIN factures_proprietaires f ON f.facture_id_opaque = e.facture_id "
            f"WHERE {' AND '.join(clauses)}", args).fetchone()
        return _round(row[0]) if row and row[0] is not None else 0.0
    finally:
        conn.close()

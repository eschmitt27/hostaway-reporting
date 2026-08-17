"""Files d'attente de rapprochement bancaire — lit SQLite, écrit SQLite (Lot 8c).

    banque_mouvements + banque_classifications → banque_rapprochements (statut PROPOSE)

CE QUE CE SERVICE FAIT, ET SURTOUT PAS
Il constitue des **files d'attente** : des mouvements identifiés comme relevant d'un type d'objet,
mais qu'aucune preuve ne permet encore de rattacher. Il ne crée aucun produit économique, ne
confirme aucun rapprochement, et n'invente aucun objet.

BANQUE ≠ RÉSERVATION — RÈGLE ABSOLUE
Un virement de plateforme n'est JAMAIS rapproché d'une réservation individuelle. `PAYOUT_PLATEFORME`
qualifie l'origine du mouvement ; les réservations sont suivies séparément. Le rapprochement
mouvement → réservation reste impossible sans export détaillé de la plateforme, et son absence est
une information, pas un manque à combler par déduction.

RÉUTILISATION, PAS SECOND MODÈLE
Rien de nouveau n'est créé : `banque_rapprochements` existe depuis la migration 0015, son
`type_objet` couvre déjà `PAYOUT_PLATEFORME` et `REVERSEMENT_PROPRIETAIRE`, et son `objet_id` est
nullable — ce qui exprime exactement « en attente, rien à rattacher pour l'instant ». Le statut
reste `PROPOSE` : seule une décision humaine peut le confirmer.
"""
from __future__ import annotations

import json
import re
import uuid
from typing import Any

from app.db.connection import get_db

TYPE_PAYOUT_PLATEFORME = "PAYOUT_PLATEFORME"
TYPE_REVERSEMENT_PROPRIETAIRE = "REVERSEMENT_PROPRIETAIRE"

ST_PROPOSE = "PROPOSE"
SOURCE_AUTO = "AUTO"

# Motifs d'attente, reprenant le vocabulaire de lot8c.
ATTENTE_EXPORT_PLATEFORME = "EN_ATTENTE_EXPORT_AIRBNB"
ATTENTE_AJUSTEMENT = "AJUSTEMENT_AIRBNB_A_CONTROLER"
ATTENTE_SAISIE_ACOMPTE = "EN_ATTENTE_SAISIE_ACOMPTE"

# Seuil sous lequel un versement de plateforme est traité comme un ajustement plutôt qu'un payout.
# Repris à l'identique de lot8c (D-8c-06) : ce n'est pas une règle inventée ici.
MONTANT_AJUSTEMENT = 4.97
TOLERANCE_AJUSTEMENT = 0.01

TIERS_PLATEFORME = "AIRBNB"

_GCODE = re.compile(r"\bG[0-9A-Z]{6,}\b")


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _extraire_reference(libelle: str) -> str:
    """Référence de versement présente dans le libellé, si la banque la transmet.

    Elle sert à retrouver le versement dans l'export de la plateforme — jamais à deviner une
    réservation.
    """
    m = _GCODE.search(_txt(libelle).upper())
    return m.group(0) if m else ""


def _mouvements_classes(*, bank_account_id: str = "", classification_run_id: str = "",
                        db_path=None) -> list[dict[str, Any]]:
    """Mouvements joints à leur classification courante. Jointure en base, pas en Python."""
    from app.services import banque_classification_service as cls

    run = classification_run_id or cls.derniere_execution(db_path=db_path)
    if not run:
        return []

    sql = (
        "SELECT m.mouvement_id_opaque, m.bank_account_id, m.date_operation, m.date_valeur, "
        "       m.montant, m.sens, m.libelle_brut, m.ligne_source, "
        "       c.tiers_detecte, c.categorie, c.regle_id, c.statut_classification "
        "FROM banque_mouvements m "
        "JOIN banque_classifications c "
        "  ON c.mouvement_id_opaque = m.mouvement_id_opaque "
        " AND c.classification_run_id = ? "
    )
    args: list = [run]
    if bank_account_id:
        sql += "WHERE m.bank_account_id = ? "
        args.append(bank_account_id)
    sql += "ORDER BY m.date_operation, m.ligne_source"

    cols = ("mouvement_id_opaque", "bank_account_id", "date_operation", "date_valeur",
            "montant", "sens", "libelle_brut", "ligne_source", "tiers_detecte", "categorie",
            "regle_id", "statut_classification")
    conn = get_db(db_path)
    try:
        return [dict(zip(cols, r)) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def proprietaires_connus(*, db_path=None) -> set[str]:
    """Identifiants propriétaires du référentiel.

    lot8c portait une liste fermée codée en dur. La lire du référentiel évite qu'elle se périme en
    silence à chaque propriétaire ajouté — et couvre du même coup le libellé de regroupement
    historique, qui n'est pas un propriétaire mais reste un tiers à traiter comme tel.
    """
    conn = get_db(db_path)
    try:
        noms = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "ref_proprietaires" not in noms:
            return set()
        return {_txt(r[0]) for r in conn.execute(
            "SELECT proprietaire_id FROM ref_proprietaires") if _txt(r[0])}
    finally:
        conn.close()


def _tiers_a_traiter(tiers: str, proprietaires: set[str]) -> bool:
    """Un tiers propriétaire, connu du référentiel ou signalé comme à identifier."""
    t = _txt(tiers)
    return bool(t) and (t in proprietaires or t.endswith("_A_CONTROLER"))


def construire(*, bank_account_id: str = "", classification_run_id: str = "",
               db_path=None) -> dict[str, Any]:
    """Construit les files d'attente. Remplace celles de l'exécution précédente.

    Les rapprochements CONFIRMÉS ou REFUSÉS par un humain ne sont jamais touchés : ce service ne
    produit que des propositions automatiques, et il ne défait pas une décision.
    """
    lignes = _mouvements_classes(bank_account_id=bank_account_id,
                                 classification_run_id=classification_run_id, db_path=db_path)
    if not lignes:
        return {"ok": False, "code": "BANQUE_AUCUNE_CLASSIFICATION",
                "message": "Aucun mouvement classé : lancez la classification avant."}

    proprietaires = proprietaires_connus(db_path=db_path)
    attentes: list[tuple] = []
    stats = {ATTENTE_EXPORT_PLATEFORME: {"nb": 0, "total": 0.0},
             ATTENTE_AJUSTEMENT: {"nb": 0, "total": 0.0},
             ATTENTE_SAISIE_ACOMPTE: {"nb": 0, "total": 0.0}}

    for l in lignes:
        tiers = _txt(l.get("tiers_detecte"))
        montant = round(float(l.get("montant") or 0), 2)

        if tiers == TIERS_PLATEFORME:
            if abs(montant - MONTANT_AJUSTEMENT) < TOLERANCE_AJUSTEMENT:
                motif, type_objet = ATTENTE_AJUSTEMENT, TYPE_PAYOUT_PLATEFORME
                commentaire = (f"Montant anormalement faible ({montant:.2f} EUR) — ajustement ou "
                               "correction de plateforme possible. Aucun produit créé.")
            else:
                motif, type_objet = ATTENTE_EXPORT_PLATEFORME, TYPE_PAYOUT_PLATEFORME
                ref = _extraire_reference(l.get("libelle_brut"))
                commentaire = (
                    f"En attente de l'export détaillé de la plateforme (référence {ref or '?'}). "
                    "Aucun rapprochement à une réservation individuelle n'est possible ni "
                    "recherché : la Banque catégorise l'origine, les réservations sont suivies "
                    "séparément.")
        elif _tiers_a_traiter(tiers, proprietaires):
            motif, type_objet = ATTENTE_SAISIE_ACOMPTE, TYPE_REVERSEMENT_PROPRIETAIRE
            commentaire = ("En attente d'un mouvement de trésorerie propriétaire déclaré. "
                           "Aucun encaissement n'est validé automatiquement.")
        else:
            continue

        stats[motif]["nb"] += 1
        stats[motif]["total"] = round(stats[motif]["total"] + montant, 2)
        attentes.append((
            "BRP-" + uuid.uuid4().hex[:12].upper(), l["mouvement_id_opaque"], type_objet,
            None,  # objet_id : rien à rattacher, c'est tout l'objet de l'attente
            montant, ST_PROPOSE, SOURCE_AUTO,
            json.dumps({"motif_attente": motif, "tiers_detecte": tiers,
                        "regle_id": l.get("regle_id"),
                        "reference": _extraire_reference(l.get("libelle_brut"))},
                       ensure_ascii=False),
            commentaire,
        ))

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Ne remplacer que les propositions AUTOMATIQUES non décidées : une décision humaine
            # doit survivre à un recalcul.
            conn.execute(
                "DELETE FROM banque_rapprochements "
                "WHERE source = ? AND statut = ?", (SOURCE_AUTO, ST_PROPOSE))
            conn.executemany(
                "INSERT INTO banque_rapprochements (rapprochement_id_opaque, "
                "mouvement_id_opaque, type_objet, objet_id, montant_rapproche, statut, source, "
                "criteres_json, commentaire) VALUES (?,?,?,?,?,?,?,?,?)", attentes)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "nb_attentes": len(attentes),
            "plateforme": stats[ATTENTE_EXPORT_PLATEFORME],
            "ajustement": stats[ATTENTE_AJUSTEMENT],
            "proprietaires": stats[ATTENTE_SAISIE_ACOMPTE],
            "produit_economique_cree": 0}


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

_COLS = ("rapprochement_id_opaque", "mouvement_id_opaque", "type_objet", "objet_id",
         "montant_rapproche", "statut", "source", "criteres_json", "commentaire", "date_creation")


def attentes(*, motif: str = "", db_path=None) -> list[dict[str, Any]]:
    """Files d'attente courantes, éventuellement filtrées par motif."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            f"SELECT {', '.join(_COLS)} FROM banque_rapprochements "
            "WHERE source = ? AND statut = ? ORDER BY date_creation, rapprochement_id_opaque",
            (SOURCE_AUTO, ST_PROPOSE)).fetchall()
    finally:
        conn.close()
    out = [dict(zip(_COLS, r)) for r in rows]
    for o in out:
        try:
            o["criteres"] = json.loads(o.get("criteres_json") or "{}")
        except ValueError:
            o["criteres"] = {}
    if motif:
        out = [o for o in out if o["criteres"].get("motif_attente") == motif]
    return out


def synthese(*, db_path=None) -> dict[str, Any]:
    """Compteurs et totaux par motif — de quoi comparer deux exécutions."""
    resultat: dict[str, Any] = {}
    for motif in (ATTENTE_EXPORT_PLATEFORME, ATTENTE_AJUSTEMENT, ATTENTE_SAISIE_ACOMPTE):
        lignes = attentes(motif=motif, db_path=db_path)
        resultat[motif] = {"nb": len(lignes),
                           "total": round(sum(l["montant_rapproche"] for l in lignes), 2)}
    return resultat

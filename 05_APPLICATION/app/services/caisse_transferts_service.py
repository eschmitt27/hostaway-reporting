"""Retrait d'espèces = transfert interne Banque → Caisse. Aucune charge, aucun résultat.

Quand on retire 20 € au distributeur, rien n'est dépensé : l'argent passe du compte au tiroir.
La banque baisse de 20 €, la caisse monte de 20 €, le résultat ne bouge pas d'un centime. C'est
pourquoi ce mouvement ne demande aucune décision humaine sur sa nature — contrairement à un
paiement par carte, dont il faut savoir à quoi il correspond.

TROIS ÉTATS, ET UN SEUL COMPTE DANS L'ENCAISSE :

  · PROVISOIRE — Qonto donne encore l'opération `pending`. Le retrait est identifié, affiché,
    mais l'encaisse ne bouge pas : une autorisation de carte peut changer de montant ou ne jamais
    être débitée. Faire monter la caisse maintenant créerait de l'argent qui n'existe pas.
  · CONFIRME  — l'opération est `completed`. Le transfert compte.
  · ANNULE    — Qonto dit explicitement `declined` ou `reversed`. L'encaisse redescend, sans
    qu'aucune ligne ne soit supprimée : on garde la trace de ce qui a été cru, puis corrigé.

PAS DE DOUBLE COMPTAGE, PAR LE SCHÉMA. `qonto_transaction_uuid` est UNIQUE en base : un même
retrait ne peut pas produire deux transferts, même si la synchronisation est rejouée cent fois.
Ce n'est pas une précaution de code qu'on pourrait oublier, c'est une contrainte.

AUCUNE ÉCRITURE COMPTABLE. La caisse comptable (`operations_caisse`) génère une écriture au 530000
à sa validation ; y déverser automatiquement un retrait reviendrait à comptabiliser sans contrôle.
Ce module enregistre le FAIT DE TRÉSORERIE. La comptabilisation reste une décision humaine.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.db.connection import get_db
from app.services import qonto_classification_service as classif

PROVISOIRE = "PROVISOIRE"
CONFIRME = "CONFIRME"
ANNULE = "ANNULE"

LIBELLES_ETAT = {
    PROVISOIRE: "Provisoire (en attente Qonto)",
    CONFIRME: "Confirmé",
    ANNULE: "Annulé par la banque",
}

# Les états Qonto qui annulent explicitement une opération. `pending` n'en fait PAS partie : une
# opération en attente n'est pas annulée, elle n'est pas encore tranchée.
ETATS_ANNULES = ("declined", "reversed")

MOTIFS = {
    PROVISOIRE: "Opération encore en attente chez Qonto : l'encaisse ne bouge pas",
    CONFIRME: "Opération réglée par la banque",
    ANNULE: "Opération refusée ou contrepassée par la banque",
}


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_presente(*, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name='caisse_transferts_banque'").fetchone() is not None
    finally:
        conn.close()


def _etat_pour(statut_qonto: str) -> tuple[str, str]:
    statut = (statut_qonto or "").lower()
    if statut in ETATS_ANNULES:
        return ANNULE, MOTIFS[ANNULE]
    if statut == "completed":
        return CONFIRME, MOTIFS[CONFIRME]
    return PROVISOIRE, MOTIFS[PROVISOIRE]


def synchroniser(*, db_path=None) -> dict:
    """Crée, confirme ou annule les transferts, d'après l'état courant des retraits Qonto.

    Idempotent : rejouer sans changement côté banque ne touche aucune ligne. Un transfert n'est
    jamais supprimé — il change d'état, et l'historique reste lisible.
    """
    if not table_presente(db_path=db_path):
        return {"crees": 0, "confirmes": 0, "annules": 0, "inchanges": 0}

    horodatage = _maintenant()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        retraits = list(conn.execute(
            "SELECT t.qonto_transaction_uuid, t.montant, t.devise, t.statut, t.libelle, "
            "       t.regle_le, t.emis_le, c.etat AS etat_actuel "
            "  FROM qonto_transactions_raw t "
            "  JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid "
            "  LEFT JOIN caisse_transferts_banque c "
            "    ON c.qonto_transaction_uuid = t.qonto_transaction_uuid "
            " WHERE s.nature = ?", (classif.RETRAIT_ESPECES,)))

        bilan = {"crees": 0, "confirmes": 0, "annules": 0, "inchanges": 0}
        for retrait in retraits:
            etat, motif = _etat_pour(retrait["statut"])
            if retrait["etat_actuel"] is None:
                conn.execute(
                    "INSERT INTO caisse_transferts_banque (transfert_id, qonto_transaction_uuid, "
                    "montant, devise, date_operation, etat, motif_etat, libelle_source, cree_le, "
                    "maj_le, confirme_le, annule_le) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"TRF-{uuid.uuid4().hex[:8].upper()}", retrait["qonto_transaction_uuid"],
                     retrait["montant"], retrait["devise"] or "EUR",
                     (retrait["regle_le"] or retrait["emis_le"] or "")[:10], etat, motif,
                     retrait["libelle"], horodatage, horodatage,
                     horodatage if etat == CONFIRME else None,
                     horodatage if etat == ANNULE else None))
                bilan["crees"] += 1
                if etat == CONFIRME:
                    bilan["confirmes"] += 1
                elif etat == ANNULE:
                    bilan["annules"] += 1
            elif retrait["etat_actuel"] != etat:
                conn.execute(
                    "UPDATE caisse_transferts_banque SET etat=?, motif_etat=?, montant=?, "
                    "date_operation=?, maj_le=?, "
                    "confirme_le=CASE WHEN ?='CONFIRME' THEN ? ELSE confirme_le END, "
                    "annule_le=CASE WHEN ?='ANNULE' THEN ? ELSE annule_le END "
                    "WHERE qonto_transaction_uuid=?",
                    (etat, motif, retrait["montant"],
                     (retrait["regle_le"] or retrait["emis_le"] or "")[:10], horodatage,
                     etat, horodatage, etat, horodatage, retrait["qonto_transaction_uuid"]))
                bilan["confirmes" if etat == CONFIRME else
                      "annules" if etat == ANNULE else "inchanges"] += 1
            else:
                bilan["inchanges"] += 1
        conn.commit()
        return bilan
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def lister(*, db_path=None) -> list[dict]:
    if not table_presente(db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM caisse_transferts_banque ORDER BY date_operation DESC, cree_le DESC")]
    finally:
        conn.close()


def totaux(*, db_path=None) -> dict:
    """Ce que les retraits apportent à l'encaisse. Seul le CONFIRMÉ compte."""
    if not table_presente(db_path=db_path):
        return {"confirme": 0.0, "provisoire": 0.0, "nb_confirmes": 0, "nb_provisoires": 0}
    conn = get_db(db_path)
    try:
        ligne = conn.execute(
            "SELECT COALESCE(SUM(CASE WHEN etat=? THEN montant END), 0) AS confirme, "
            "       COALESCE(SUM(CASE WHEN etat=? THEN montant END), 0) AS provisoire, "
            "       COUNT(CASE WHEN etat=? THEN 1 END) AS nb_confirmes, "
            "       COUNT(CASE WHEN etat=? THEN 1 END) AS nb_provisoires "
            "  FROM caisse_transferts_banque",
            (CONFIRME, PROVISOIRE, CONFIRME, PROVISOIRE)).fetchone()
        return {"confirme": round(ligne["confirme"], 2),
                "provisoire": round(ligne["provisoire"], 2),
                "nb_confirmes": ligne["nb_confirmes"],
                "nb_provisoires": ligne["nb_provisoires"]}
    finally:
        conn.close()

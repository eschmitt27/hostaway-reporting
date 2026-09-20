"""Statut applicatif des mouvements Qonto — une file d'attente visible, sans effet comptable.

Chaque mouvement importé reçoit `A_RAPPROCHER`. Ce statut ne crée aucun règlement, ne rapproche
rien, ne modifie aucune dette ni créance : il dit seulement « ce mouvement attend qu'un humain
décide de quoi il s'agit ». Le rapprochement, quand il existera, LIRA ce statut ; il n'est pas ici.

DÉFINITIF OU NON. Une opération `pending` est une autorisation (typiquement une carte) : le montant
peut encore bouger, ou ne jamais être débité. Elle doit apparaître — la cacher donnerait une image
fausse du solde — mais elle est marquée `comptabilisable = 0` avec le motif en clair. C'est le
garde-fou qui empêche, plus tard, qu'un rapprochement s'appuie sur une opération qui n'existe pas
encore vraiment.
"""
from __future__ import annotations

from datetime import datetime, timezone

from app.db.connection import get_db

A_RAPPROCHER = "A_RAPPROCHER"

LIBELLES = {A_RAPPROCHER: "À rapprocher"}

# Seul `completed` est un mouvement acquis. Les trois autres états que Qonto expose ne le sont pas,
# chacun pour une raison différente — le motif est conservé et affiché, pas résumé en « non ».
MOTIFS_NON_DEFINITIFS = {
    "pending": "En attente chez Qonto — montant non définitif, peut encore changer ou ne pas être débité",
    "declined": "Opération refusée par la banque — aucun mouvement d'argent",
    "reversed": "Opération contrepassée — annulée après coup",
}


def _mouvement_opaque(transaction_id: str) -> str:
    """Identifiant de mouvement stable (cf. `qonto_validation_service.mouvement_opaque`)."""
    from app.services.qonto_validation_service import mouvement_opaque
    return mouvement_opaque(transaction_id)


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_presente(*, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name='qonto_transactions_statut_local'").fetchone() is not None
    finally:
        conn.close()


def synchroniser(*, db_path=None) -> dict:
    """Pose `A_RAPPROCHER` sur les mouvements qui n'ont pas encore de statut, et rafraîchit le
    caractère définitif de tous.

    Idempotent : rejouer ne change rien tant que Qonto n'a rien changé. Le statut lui-même n'est
    JAMAIS réécrit une fois posé — seul `comptabilisable` suit l'état de la banque, parce qu'une
    opération en attente qui se règle devient acquise sans que notre décision ait changé.
    """
    if not table_presente(db_path=db_path):
        return {"poses": 0, "rafraichis": 0}

    horodatage = _maintenant()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        mouvements = list(conn.execute(
            "SELECT t.qonto_transaction_uuid, t.transaction_id, t.statut, "
            "       s.comptabilisable, s.mouvement_id_opaque "
            "FROM qonto_transactions_raw t "
            "LEFT JOIN qonto_transactions_statut_local s "
            "  ON s.qonto_transaction_uuid = t.qonto_transaction_uuid"))

        poses = rafraichis = 0
        for ligne in mouvements:
            statut_qonto = (ligne["statut"] or "").lower()
            motif = MOTIFS_NON_DEFINITIFS.get(statut_qonto)
            definitif = 1 if statut_qonto == "completed" else 0
            if ligne["comptabilisable"] is None:
                conn.execute(
                    "INSERT INTO qonto_transactions_statut_local (qonto_transaction_uuid, "
                    "statut_local, comptabilisable, motif_non_comptabilisable, pose_le, maj_le, "
                    "mouvement_id_opaque) VALUES (?,?,?,?,?,?,?)",
                    (ligne["qonto_transaction_uuid"], A_RAPPROCHER, definitif, motif,
                     horodatage, horodatage, _mouvement_opaque(ligne["transaction_id"])))
                poses += 1
            elif not ligne["mouvement_id_opaque"]:
                # Ligne posée avant que l'identifiant de mouvement existe : on le complète sans
                # toucher au reste.
                conn.execute(
                    "UPDATE qonto_transactions_statut_local SET mouvement_id_opaque=?, "
                    "comptabilisable=?, motif_non_comptabilisable=?, maj_le=? "
                    "WHERE qonto_transaction_uuid=?",
                    (_mouvement_opaque(ligne["transaction_id"]), definitif, motif, horodatage,
                     ligne["qonto_transaction_uuid"]))
                rafraichis += 1
            elif ligne["comptabilisable"] != definitif:
                conn.execute(
                    "UPDATE qonto_transactions_statut_local SET comptabilisable=?, "
                    "motif_non_comptabilisable=?, maj_le=? WHERE qonto_transaction_uuid=?",
                    (definitif, motif, horodatage, ligne["qonto_transaction_uuid"]))
                rafraichis += 1
        conn.commit()
        return {"poses": poses, "rafraichis": rafraichis}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def resume(*, db_path=None) -> dict:
    """Compteurs pour l'écran : combien attendent, combien ne sont pas définitifs."""
    if not table_presente(db_path=db_path):
        return {"a_rapprocher": 0, "non_definitifs": 0}
    conn = get_db(db_path)
    try:
        return {
            "a_rapprocher": conn.execute(
                "SELECT COUNT(*) FROM qonto_transactions_statut_local WHERE statut_local=?",
                (A_RAPPROCHER,)).fetchone()[0],
            "non_definitifs": conn.execute(
                "SELECT COUNT(*) FROM qonto_transactions_statut_local "
                "WHERE comptabilisable=0").fetchone()[0],
        }
    finally:
        conn.close()

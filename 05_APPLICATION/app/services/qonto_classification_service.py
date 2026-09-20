"""Nature d'une transaction Qonto, et état de son traitement. Déterministe et explicable.

DEUX QUESTIONS DISTINCTES, DEUX RÉPONSES SÉPARÉES :

  · la NATURE — de quoi s'agit-il ? Déduite de ce que Qonto affirme, jamais d'une intuition. Elle
    est stockée avec son motif : on doit pouvoir dire POURQUOI une transaction a été classée.
  · le TRAITEMENT — où en est-on ? Calculé à la volée, parce qu'il dépend de l'état courant de la
    banque et des pièces disponibles. Le figer en base le rendrait faux dès le lendemain.

LE SEUL AUTOMATISME EST LE RETRAIT D'ESPÈCES. Tout le reste va au contrôle humain.
"""
from __future__ import annotations

from datetime import datetime, timezone

from app.db.connection import get_db

# ── Natures ───────────────────────────────────────────────────────────────────────────────────
RETRAIT_ESPECES = "RETRAIT_ESPECES"
APPORT_ASSOCIE = "APPORT_ASSOCIE"
FRAIS_BANCAIRES = "FRAIS_BANCAIRES"
INCONNU = "INCONNU"

LIBELLES_NATURE = {
    RETRAIT_ESPECES: "Retrait espèces → Caisse",
    APPORT_ASSOCIE: "Apport en compte courant d'associé",
    FRAIS_BANCAIRES: "Frais bancaires",
    INCONNU: "Nature à déterminer",
}

# ── Traitement ────────────────────────────────────────────────────────────────────────────────
EN_ATTENTE_QONTO = "EN_ATTENTE_QONTO"
TRANSFERE_CAISSE = "TRANSFERE_CAISSE"
A_RAPPROCHER = "A_RAPPROCHER"
A_CONTROLER = "A_CONTROLER"
RAPPROCHE = "RAPPROCHE"

LIBELLES_TRAITEMENT = {
    EN_ATTENTE_QONTO: "En attente Qonto",
    TRANSFERE_CAISSE: "Transféré en caisse",
    A_RAPPROCHER: "À rapprocher",
    A_CONTROLER: "À contrôler",
    RAPPROCHE: "Rapproché",
}

# Ordre d'affichage des filtres : ce qui demande une action d'abord.
TRAITEMENTS = (A_CONTROLER, A_RAPPROCHER, EN_ATTENTE_QONTO, TRANSFERE_CAISSE, RAPPROCHE)

# `category = "atm"` est le champ documenté par Qonto pour « retrait d'espèces au distributeur ».
# Qonto le signale comme déprécié au profit de `cashflow_category`, MAIS cette dernière est une
# étiquette libre posée par le titulaire : elle ne peut pas servir de preuve automatique. Tant que
# `category` est servi, il reste le seul signal fiable et non ambigu.
#
# LE DÉFAUT VA DANS LE BON SENS : si Qonto cessait d'envoyer ce champ, la transaction ne serait
# PAS classée en retrait — elle partirait au contrôle humain. Une classification automatique qui
# disparaît fait perdre du confort ; une qui se déclenche à tort ferait bouger de l'argent.
CATEGORIE_RETRAIT_ESPECES = "atm"


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def determiner_nature(mouvement: dict) -> tuple[str, str]:
    """Retourne (nature, motif). Le motif est une phrase lisible, jamais un code nu."""
    categorie = (mouvement.get("categorie") or "").strip().lower()
    type_operation = (mouvement.get("type_operation") or "").strip().lower()
    sens = (mouvement.get("sens") or "").strip().lower()
    categorie_flux = (mouvement.get("categorie_flux") or "").strip()

    if categorie == CATEGORIE_RETRAIT_ESPECES and sens == "debit":
        return RETRAIT_ESPECES, "Qonto classe cette opération en retrait au distributeur (atm)"

    if type_operation == "qonto_fee":
        return FRAIS_BANCAIRES, "Opération de type « frais Qonto »"

    # Étiquette posée à la main dans Qonto par le titulaire : c'est SA lecture, pas la nôtre. On
    # la reprend telle quelle en le disant, au lieu de deviner à partir du libellé.
    normalisee = categorie_flux.lower()
    if "apport" in normalisee and "compte courant" in normalisee:
        return APPORT_ASSOCIE, f"Catégorie renseignée dans Qonto : « {categorie_flux} »"

    return INCONNU, "Aucun signal fiable dans les données Qonto"


def determiner_traitement(mouvement: dict, niveau_suggestion: str = "") -> str:
    """État courant du traitement. Calculé, jamais stocké : il dépend de l'instant.

    `niveau_suggestion` vient du moteur de suggestions. Sans candidat solide, la transaction part
    À CONTRÔLER — même quand sa nature est connue. Connaître la nature d'un encaissement ne dit
    pas à QUOI il s'impute : affirmer « à rapprocher » sans avoir trouvé de document laisserait
    croire qu'il ne reste qu'un clic à donner.
    """
    if (mouvement.get("statut") or "").lower() != "completed":
        return EN_ATTENTE_QONTO
    if mouvement.get("statut_local") == RAPPROCHE:
        return RAPPROCHE
    if mouvement.get("nature") == RETRAIT_ESPECES:
        return TRANSFERE_CAISSE
    if niveau_suggestion in ("FORT", "MOYEN"):
        return A_RAPPROCHER
    return A_CONTROLER


def synchroniser(*, db_path=None) -> dict:
    """Pose ou met à jour la nature de chaque mouvement importé.

    Idempotent : une nature déjà posée avec le même motif n'est pas réécrite. Elle EST recalculée
    quand les données Qonto changent — un retrait reste un retrait, mais une transaction dont la
    catégorie arrive plus tard doit pouvoir être reclassée.
    """
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lignes = list(conn.execute(
            "SELECT t.qonto_transaction_uuid, t.categorie, t.categorie_flux, t.type_operation, "
            "       t.sens, s.nature, s.nature_motif "
            "  FROM qonto_transactions_raw t "
            "  JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid"))
        poses = 0
        for ligne in lignes:
            nature, motif = determiner_nature(dict(ligne))
            if ligne["nature"] == nature and ligne["nature_motif"] == motif:
                continue
            conn.execute(
                "UPDATE qonto_transactions_statut_local SET nature=?, nature_motif=?, maj_le=? "
                "WHERE qonto_transaction_uuid=?",
                (nature, motif, _maintenant(), ligne["qonto_transaction_uuid"]))
            poses += 1
        conn.commit()
        return {"classees": poses}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

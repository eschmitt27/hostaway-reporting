"""Lecteur read-only charges fournisseurs — Lot APP-3a, SQLite (migration 0052).

Source unique : la table `charges`. `MASTER_FACT_MAN_Charges.xlsx` n'est plus lu.

POURQUOI LA BASCULE
Ce lecteur était la dernière ENTRÉE ÉCONOMIQUE de l'application encore servie par un classeur : il
alimente le module CHG de `flux_unifie_service` (donc Lot9 → Lot10 → Lot11 → Lot12), l'écran
Résultats, l'analytique et les candidats bancaires. Tant qu'il lisait Excel, le pipeline ne pouvait
pas démarrer sans classeur, même quand il n'y avait aucune charge à lire.

Au moment de la migration, la chaîne Charges ne portait AUCUNE donnée métier : la SAISIE comme le
MASTER ne contenaient que des lignes de gabarit Power Query, déjà écartées par l'ancien
`_is_real_row`. Il n'y avait donc rien à reprendre — l'absence de charges est désormais un état
SQLite vide et propre, pas un classeur vide qu'il faudrait maintenir pour que le moteur accepte de
tourner.

Règles gravées, inchangées :
- D026 : source unique = les charges du système, jamais `MASTER_CALC_Flux` ni une saisie parallèle.
- D025 : IK et virements associés exclus du périmètre Fournisseurs.
- D044 : `statut_controle` affiché tel quel, jamais recalculé ici.

Les noms de colonnes rendus sont ceux du MASTER historique : les consommateurs n'ont rien à changer.
"""
from typing import Any

from app.db.connection import get_db

SHEET_MASTER = "MASTER"           # conservé : libellé d'origine, encore affiché dans les diagnostics
SOURCE_MASTER = "SQLite (charges)"
SOURCE_SAISIE = "SQLite (charges)"

_EXCLUDED_TYPE_FLUX = {"IK", "VIREMENT_ASSOCIE"}

# Colonnes rendues, dans l'ordre du MASTER historique. `row_hash` est réexposé sous son nom moteur
# `ROW_HASH` : c'est sous ce nom que les consommateurs le connaissent.
_COLONNES = (
    "charge_id", "date_charge", "mois", "montant", "sens_flux", "sens", "categorie_charge_id",
    "filtre_vue_menage", "type_flux_id", "code_impact", "impact_resultat_reel",
    "impact_resultat_comptable", "prise_en_compta", "associe_id", "mode_paiement_id", "carte_id",
    "affectation_type", "logement_id", "proprietaire_id", "reservation_id", "refacturable",
    "source_flux", "methode_traitement", "paye_avec_montant_recupere", "lien_virement_banque",
    "statut_controle", "niveau_anomalie", "code_anomalie", "statut_rapprochement", "justificatif",
    "commentaire", "row_hash", "date_saisie", "source_module", "source_table", "source_pk",
    "date_integration",
)


def _table_presente(db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='charges'"
        ).fetchone() is not None
    finally:
        conn.close()


def master_available(db_path=None) -> bool:
    """La source charges est-elle exploitable ? (table présente, pas « fichier présent »)

    Une base non migrée rend False — un état affiché, jamais une exception. Aucune charge saisie
    n'est en revanche un état NORMAL : la table existe, elle est simplement vide.
    """
    return _table_presente(db_path)


def saisie_available(db_path=None) -> bool:
    """La saisie et le référentiel de charges vivent désormais dans la même table."""
    return _table_presente(db_path)


def _is_fournisseur_row(row: dict[str, Any]) -> bool:
    """Exclut IK et virements associés — D025."""
    tfi = str(row.get("type_flux_id") or "").strip().upper()
    return tfi not in _EXCLUDED_TYPE_FLUX


def read_charges(db_path=None) -> list[dict[str, Any]]:
    """Toutes les charges fournisseurs ACTIVES (hors IK/virements associés).

    Les charges ANNULÉES sont exclues : elles restent en base pour la traçabilité, mais ne sont plus
    une charge du point de vue économique. Le filtre des lignes de gabarit Power Query n'a plus
    d'objet : une ligne en base est une charge réellement saisie.
    """
    if not _table_presente(db_path):
        return []
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            f"SELECT {', '.join(_COLONNES)} FROM charges "
            "WHERE statut = 'ACTIVE' ORDER BY date_charge, charge_id")]
    finally:
        conn.close()
    for l in lignes:
        l["ROW_HASH"] = l.pop("row_hash", None)
    return [l for l in lignes if _is_fournisseur_row(l)]


def find_charge(charge_id: str, db_path=None) -> dict[str, Any] | None:
    """Ligne unique par `charge_id`. None si absente."""
    cid = str(charge_id).strip()
    for row in read_charges(db_path=db_path):
        if str(row.get("charge_id") or "").strip() == cid:
            return row
    return None

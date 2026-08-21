"""Lecteur read-only des réservations hors Hostaway — Lot APP-2a, SQLite (migration 0052).

Source : la table `reservations_hors_hostaway`.
`MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` n'est plus lu.

POURQUOI LA BASCULE
Ce lecteur alimente l'écran Réservations et le pilotage mensuel. Il lisait un master produit par
Power Query depuis une saisie Excel : trois artefacts et deux rafraîchissements manuels pour une
poignée de réservations saisies à la main. La saisie passe désormais par l'application
(`reservations_hh_saisie_service`), et la table est la seule vérité.

`reservation_hh_id` reste la clé métier : c'est d'elle que le moteur dérive `reservation_calc_id`
(`lib_db_moteur.cle_reservation_hh`). La conserver telle quelle est ce qui garantit qu'une
réservation saisie ici reste la même réservation économique en aval.

Règles inchangées :
- Lecture seule stricte, aucune transformation, aucun recalcul.
- Ne lit JAMAIS une saisie parallèle ni le référentiel.
"""
from typing import Any

from app.db.connection import get_db

SHEET_MASTER = "MASTER"           # conservé : libellé d'origine, encore affiché dans les diagnostics
PK_PREFIX = "RESHH-"

_COLONNES = (
    "reservation_hh_id", "row_hash", "mois", "canal_id", "source_financiere", "proprietaire_id",
    "logement_id", "reservation_id_hostaway", "date_arrivee", "date_depart", "nuits",
    "guest_count", "montant_percu", "montant_retenu", "mode_paiement_id", "code_impact",
    "impact_resultat_reel", "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
    "code_anomalie", "commentaire", "date_saisie", "source_module", "source_table", "source_pk",
    "date_integration",
)


def _table_presente(db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='reservations_hors_hostaway'").fetchone() is not None
    finally:
        conn.close()


def master_available(db_path=None) -> bool:
    """La source est-elle exploitable ? (table présente, pas « fichier présent »)"""
    return _table_presente(db_path)


def read_reservations(db_path=None) -> list[dict[str, Any]]:
    """Réservations hors Hostaway ACTIVES.

    Les réservations ANNULÉES restent en base pour la traçabilité mais ne sont plus servies : une
    réservation annulée n'a pas d'effet économique. Le filtre de la ligne-placeholder Power Query
    n'a plus d'objet — une ligne en base est une réservation réellement saisie.
    """
    if not _table_presente(db_path):
        return []
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            f"SELECT {', '.join(_COLONNES)} FROM reservations_hors_hostaway "
            "WHERE statut = 'ACTIVE' ORDER BY mois, reservation_hh_id")]
    finally:
        conn.close()
    for l in lignes:
        # `ROW_HASH` et `guestCount` : noms attendus par les consommateurs, hérités du moteur.
        l["ROW_HASH"] = l.pop("row_hash", None)
        l["guestCount"] = l.get("guest_count")
    return lignes


def find_reservation(reservation_hh_id: str, db_path=None) -> dict[str, Any] | None:
    target = str(reservation_hh_id).strip()
    for r in read_reservations(db_path=db_path):
        if str(r.get("reservation_hh_id") or "").strip() == target:
            return r
    return None

"""Constante NEW_SAISIE_FIELDS — champs override HH (dérogations menage/commission).

L'outil de migration de schéma APP-2b (`migrate_saisie_copy`/`migrate_ref_setup_copy`,
`_assert_copy_path`) qui écrivait ces colonnes dans une copie de classeur a été supprimé
(0 appelant réel, la migration SQLite 0053 a repris son rôle) — cf. mission nettoyage legacy
2026-08-22. Seule la liste des champs reste utilisée, par `saisie_hh_service.py`
(reservation_hh_overrides).
"""
from __future__ import annotations

NEW_SAISIE_FIELDS = [
    "taux_commission_override",
    "motif_override_taux_commission",
    "confirmation_override_taux_commission",
    "menage_override",
    "motif_override_menage",
    "confirmation_override_menage",
    "source_acompte_facture",
]

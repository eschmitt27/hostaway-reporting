-- Un numéro de facture FOURNISSEUR n'est pas une clé unique absolue.
--
-- Cas réel : le même prestataire a émis « 2026-37 » le 30 avril (15 €) ET le 31 mai (1 439 €),
-- deux documents différents. L'index (fournisseur, référence) interdisait la seconde ; le service
-- d'import finissait alors par annuler l'une pour l'autre, ou par refuser un document réel.
--
-- L'identité interne d'une facture reste `facture_id_opaque`. Le doublon CERTAIN interdit au
-- niveau du schéma devient : même fournisseur + même référence + même MOIS de facture. Deux
-- factures de même numéro sur deux mois différents peuvent coexister — l'import les signale
-- (anomalie NUMERO_FACTURE_REUTILISE) et la saisie manuelle continue de refuser tout numéro déjà
-- connu (contrôle applicatif `factures_service._doublon_certain`, inchangé sans option explicite).
-- Aucune donnée n'est modifiée : seul l'index change.
DROP INDEX IF EXISTS idx_factures_fournisseur_ref;
CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_fournisseur_ref_mois
    ON factures(fournisseur_id_opaque, facture_ref, substr(COALESCE(date_facture, ''), 1, 7))
    WHERE statut <> 'ANNULEE';

-- « Justificatif archivé » : la charge a-t-elle une pièce rangée, et sous quelle référence.
--
-- La référence n'est JAMAIS saisie : elle est attribuée par une séquence, pour qu'elle soit unique
-- et qu'elle ne dépende pas de ce que l'utilisateur croit être le dernier numéro. Format
-- JUS-AAAA-NNNN (JUS-2026-0001), remis à 1 par année.
--
-- La séquence ne connaît QUE son dernier numéro : elle ne recycle donc jamais un numéro libéré par
-- une charge annulée ou supprimée — un justificatif classé sous JUS-2026-0001 doit rester
-- retrouvable même si la charge qui l'a consommé n'existe plus.
ALTER TABLE charges ADD COLUMN justificatif_archive TEXT;      -- OUI | NON | NULL (non renseigné)
ALTER TABLE charges ADD COLUMN justificatif_reference TEXT;    -- JUS-AAAA-NNNN, attribué, jamais saisi

CREATE UNIQUE INDEX IF NOT EXISTS idx_charges_justificatif_reference
    ON charges(justificatif_reference) WHERE justificatif_reference IS NOT NULL;

CREATE TABLE IF NOT EXISTS charges_justificatif_sequence (
    serie             TEXT PRIMARY KEY,          -- l'année : « 2026 »
    dernier_numero    INTEGER NOT NULL DEFAULT 0,
    date_modification TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

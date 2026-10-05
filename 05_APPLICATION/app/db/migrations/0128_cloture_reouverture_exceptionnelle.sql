-- 0128 — Réouverture EXCEPTIONNELLE d'un mois clôturé définitivement : conservation de l'archive retirée.
--
-- Un mois clôturé définitivement est figé : son archive économique (`reservations_historique_cloture`,
-- `mois_archive_reglement`) porte les valeurs de la clôture, et `ref_cloture_mensuelle` = CLOTURE.
-- Rouvrir exceptionnellement ce mois (justification obligatoire) le rend de nouveau modifiable ; pour que
-- sa RE-clôture fige les valeurs corrigées — et non les anciennes, qu'une réservation « archivée une fois
-- pour toutes » ferait ignorer —, l'archive d'origine est RETIRÉE des tables vives.
--
-- AUCUNE TRACE N'EST PERDUE : tout ce qui est retiré est copié, ligne par ligne, dans CETTE table avant
-- d'être retiré, avec le mois, la clôture, l'acteur, la date et la justification. La table est en ajout
-- seul (déclencheurs ci-dessous). Le journal de la clôture, lui, montre : clôturé → rouvert → (reclôturé).
--
-- Additive : aucune table existante modifiée, aucune donnée existante touchée.

CREATE TABLE IF NOT EXISTS cloture_archives_retirees (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                TEXT NOT NULL,                 -- AAAA-MM
    cloture_id_opaque   TEXT NOT NULL,                 -- clôture mensuelle concernée (CLO-xxxx)
    archive_ids         TEXT,                          -- ARC-xxxx retirés (séparés par des virgules)
    nb_reservations     INTEGER NOT NULL DEFAULT 0,
    nb_reglement        INTEGER NOT NULL DEFAULT 0,
    contenu_json        TEXT NOT NULL,                 -- {"reservations": [...], "reglement": [...]} tels que figés
    justification       TEXT NOT NULL CHECK (length(trim(justification)) > 0),
    acteur              TEXT,
    date_retrait        TEXT NOT NULL                  -- heure locale du poste
);
CREATE INDEX IF NOT EXISTS idx_cloture_archives_retirees_mois ON cloture_archives_retirees(mois);

CREATE TRIGGER IF NOT EXISTS trg_cloture_archives_retirees_no_update
BEFORE UPDATE ON cloture_archives_retirees
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_TRACE : une archive retirée est conservée en ajout seul.');
END;

CREATE TRIGGER IF NOT EXISTS trg_cloture_archives_retirees_no_delete
BEFORE DELETE ON cloture_archives_retirees
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_TRACE : une archive retirée ne s''efface pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0128');

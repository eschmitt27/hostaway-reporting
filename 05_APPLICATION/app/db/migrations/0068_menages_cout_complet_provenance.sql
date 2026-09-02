-- Migration 0068 — Provenance des lignes `menages_cout_complet` : calcul courant vs historique figé
-- (mission « backfill historique ménages figé + zéro Excel runtime »).
--
-- POURQUOI
-- Lot9 construisait TYPE_FLUX_018/019 en lisant le classeur `MASTER_CALC_CoutComplet_Menages.xlsx`.
-- Le basculer vers SQLite butait sur un écart de périmètre réel : le classeur ne contient que
-- 2026-05 (16 lignes déjà arrêtées), tandis que `menages_cout_complet` ne contenait que
-- 2026-06/2026-07. Basculer sans rien faire aurait supprimé le flux économique d'un mois CLOTURÉ —
-- une modification économique silencieuse sur de l'historique, refusée.
--
-- La décision produit est un BACKFILL HISTORIQUE FIGÉ, jamais un recalcul : les valeurs déjà
-- arrêtées de 2026-05 sont reprises telles quelles depuis le classeur, sans rejouer une seule
-- formule (ni lot6a, ni lot6d, ni lot6e, ni lot6f), sans rouvrir le mois et sans toucher
-- `ref_cloture_mensuelle`. Le mois reste CLOTURE.
--
-- TABLE COMPAGNON, PAS `ALTER TABLE`
-- Les migrations sont rejouées à chaque démarrage et SQLite n'a pas d'`ADD COLUMN IF NOT EXISTS` :
-- un ALTER planterait au second passage (« duplicate column name »). Même motif que 0012/0017/0019/
-- 0020/0028/0032, et même choix que 0066 (`menages_declarations_extra`) : la provenance vit dans sa
-- propre table, jointe à la demande sur la clé métier.
--
-- CETTE TABLE EST DE LA PROVENANCE, PAS DE L'ÉCONOMIE
-- Aucune de ses colonnes ne participe à un calcul. Elle permet seulement de distinguer, à la
-- lecture, une ligne recalculée par le moteur d'une ligne d'historique gelée reprise d'un classeur
-- legacy — distinction impossible autrement une fois les deux dans la même table. Lot9 ne la lit
-- pas et consomme les deux origines par la même interface (§3/§6) : c'est justement l'intérêt de
-- les faire cohabiter plutôt que de créer un second moteur.
--
-- Une ligne de `menages_cout_complet` SANS entrée ici = calcul courant du moteur (comportement
-- historique inchangé pour toutes les lignes déjà présentes).

CREATE TABLE IF NOT EXISTS menages_cout_complet_provenance (
    mois           TEXT NOT NULL,
    logement_id    TEXT NOT NULL,
    intervenant_id TEXT NOT NULL,
    source_type    TEXT NOT NULL,          -- LEGACY_IMPORT_FIGE
    source_fichier TEXT,
    source_hash    TEXT,
    date_import    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (mois, logement_id, intervenant_id)
);

-- Recherche par origine et par mois : sert l'audit et les contrôles de non-régression, pas le
-- chemin chaud (Lot9 lit `menages_cout_complet` sans jointure).
CREATE INDEX IF NOT EXISTS idx_menages_cout_complet_provenance_mois
    ON menages_cout_complet_provenance(mois, source_type);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0068');

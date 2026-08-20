-- Migration 0048 — Lot12 : correspondance identifiant stable ↔ identifiant legacy positionnel.
--
-- Additive comme 0017→0047. Aucun ALTER TABLE (extension 1-1 de `lot12_prefactures_entete`, même
-- procédé que `facture_lignes_menage_detail`/`controles_lot11_constats_champs`).
--
-- POURQUOI
-- `facture_id` valait `PREF-{mois}-{prop}-{log}-{NNN}` où `NNN` était un compteur POSITIONNEL par
-- mois (ordre de parcours des lignes). Un simple changement d'ordre de lecture — ou l'ajout d'une
-- préfacture sur un mois — décalait le suffixe de toutes les suivantes : l'identifiant désignait
-- une position, pas une préfacture. C'est ce qui produisait 102/285 identifiants différents entre
-- le moteur legacy (ordre pandas) et le service SQLite (ORDER BY id), à données économiques
-- rigoureusement identiques.
--
-- `facture_id` dérive désormais du GRAIN CANONIQUE de Lot12 (D-LOT12-01) : mois × propriétaire ×
-- logement. Deux exécutions sur les mêmes données produisent le même identifiant, quel que soit
-- l'ordre ; l'ajout d'une préfacture ne renomme aucune autre.
--
-- Cette table conserve l'identifiant positionnel tel que le legacy l'aurait produit, pour retrouver
-- une préfacture citée sous son ancien numéro (rapport, échange, capture d'écran). Elle est une
-- correspondance de traçabilité, jamais une clé de calcul : aucun montant n'en dépend.
CREATE TABLE IF NOT EXISTS lot12_prefactures_id_legacy (
    run_id            TEXT NOT NULL,
    facture_id        TEXT NOT NULL,   -- identifiant stable (grain métier)
    facture_id_legacy TEXT NOT NULL,   -- identifiant positionnel historique (PREF-...-NNN)
    PRIMARY KEY (run_id, facture_id)
);
CREATE INDEX IF NOT EXISTS idx_lot12_id_legacy ON lot12_prefactures_id_legacy(facture_id_legacy);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0048');

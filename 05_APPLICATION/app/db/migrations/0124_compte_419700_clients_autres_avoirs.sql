-- Migration 0124 — 419700 « Clients – autres avoirs » (2026-10-03).
--
-- Le PCG réserve 4191 aux avances et acomptes reçus. Un crédit client qui n'est ni une avance ni
-- un acompte — solde créditeur repris de l'ancienne structure — relève de 4197 (« Clients, autres
-- avoirs »). Subdivision 419700, auxiliaire client obligatoire, comme 419100.

INSERT OR IGNORE INTO plan_comptable (compte, libelle, type_compte, actif, auxiliaire_autorise,
                                      auxiliaire_mode, auxiliaire_type, commentaire)
VALUES ('419700', 'Clients - autres avoirs (crédits clients)', 'PASSIF', 1, 1, 'REQUIRED',
        'CLIENT', 'Crédits clients hors avances et acomptes (migration 0124)');

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0124');

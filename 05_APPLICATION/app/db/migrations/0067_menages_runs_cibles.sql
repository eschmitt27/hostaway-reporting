-- Migration 0067 — Trace des recalculs MENAGES ciblés par mois (mission « rendre le recalcul
-- ménages réellement mensuel et ciblé »).
--
-- Permet de prouver, pour chaque déclenchement du bouton "Actualiser <mois>" : le mois demandé par
-- l'utilisateur == le mois réellement traité par lot6d/6e/6f. Distinct de `moteur_runs` (trace les
-- runs Hostaway lot1) et de `orchestrateur_dataset_evenements` (trace les transitions de statut par
-- DATASET, pas par MOIS) : aucune table existante ne portait cette grain-là.

CREATE TABLE IF NOT EXISTS menages_runs_cibles (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    mois_demande   TEXT NOT NULL,
    mois_traite    TEXT,
    declencheur    TEXT NOT NULL DEFAULT 'MANUEL',
    statut         TEXT NOT NULL,   -- SUCCES|ECHEC|REFUSE_MOIS_CLOTURE
    horodatage     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_menages_runs_cibles_mois ON menages_runs_cibles(mois_demande);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0067');

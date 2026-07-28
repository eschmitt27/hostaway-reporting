-- Migration 0022 — Lignes de facture (multi-charges / multi-logements).
--
-- Gap identifié dans `48_ROADMAP_RESTANTE_PROJET.md` : `factures.charge_id` (0017) impose UNE
-- charge par facture, via un index unique. Une facture qui couvre plusieurs charges ou plusieurs
-- logements (ex. facture ménage groupée sur 3 appartements) n'a pas de représentation.
--
-- Ajout ADDITIF, rétrocompatible (décision utilisateur explicite) : `factures.charge_id` reste
-- inchangé et continue de porter le cas mono-charge historique. `facture_lignes` porte le cas
-- multi-charges/multi-logements pour les nouvelles factures qui en ont besoin. Une facture n'utilise
-- JAMAIS les deux mécanismes à la fois (contrôle applicatif dans factures_service.ajouter_ligne) :
-- soit `charge_id` seul (mono-charge, comme avant), soit une ou plusieurs `facture_lignes`.

CREATE TABLE IF NOT EXISTS facture_lignes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque     TEXT NOT NULL UNIQUE,        -- FACL-xxxx
    facture_id_opaque   TEXT NOT NULL,
    charge_id           TEXT NOT NULL,                -- lien vers la charge économique (jamais créée ici)
    logement_id         TEXT,                         -- logement concerné par cette ligne, si connu
    montant_ht          REAL,
    montant_tva         REAL,
    montant_ttc         REAL NOT NULL,
    commentaire         TEXT,
    date_creation       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur              TEXT
);
CREATE INDEX IF NOT EXISTS idx_facture_lignes_facture ON facture_lignes(facture_id_opaque);
-- Même règle que `factures.charge_id` (0017) : une charge n'est jamais rattachée deux fois.
CREATE UNIQUE INDEX IF NOT EXISTS idx_facture_lignes_charge ON facture_lignes(charge_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0022');

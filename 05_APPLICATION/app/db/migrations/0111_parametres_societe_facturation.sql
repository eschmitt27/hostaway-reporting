-- Migration 0111 — Paramètres société & facturation : SQLite devient la source canonique.
--
-- AVANT : l'identité de la société et les conditions de facturation vivaient dans des variables
-- d'environnement, lues à DEUX endroits différents (`app.config` pour la route d'émission,
-- `facturation_config_service` pour le contrôle de conformité). Le `.env` réel les laissait vides ;
-- les vraies valeurs (Kbis du 2026-09-10) n'étaient passées que sur la ligne de commande d'un
-- lancement de recette. Résultat : « émetteur incomplet » dès que l'application était lancée
-- normalement.
--
-- Ces données ne sont pas des secrets : elles s'impriment sur chaque facture. Elles ont donc leur
-- place dans la base, administrables et historisées. Les secrets restent dans `.env`.
--
-- UNE LIGNE PAR PARAMÈTRE. `valeur` NULL = « non configuré », distinct d'une valeur saisie : pour le
-- délai de paiement, '0' (paiement à réception) n'est pas NULL (non configuré).
CREATE TABLE IF NOT EXISTS parametres_societe_facturation (
    cle            TEXT PRIMARY KEY,
    valeur         TEXT,
    maj_le         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    maj_par        TEXT NOT NULL,
    origine        TEXT NOT NULL DEFAULT 'SAISIE'      -- SAISIE | REPRISE_ENVIRONNEMENT | REPRISE_DOCUMENT
);

-- Chaque changement est conservé : une facture émise garde son propre instantané (snapshot), mais
-- savoir QUAND un paramètre a changé, et par qui, reste une question légitime.
CREATE TABLE IF NOT EXISTS parametres_societe_facturation_historique (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    cle               TEXT NOT NULL,
    ancienne_valeur   TEXT,
    nouvelle_valeur   TEXT,
    modifie_le        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    modifie_par       TEXT NOT NULL,
    motif             TEXT
);
CREATE INDEX IF NOT EXISTS idx_param_societe_hist_cle
    ON parametres_societe_facturation_historique(cle, id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0111');

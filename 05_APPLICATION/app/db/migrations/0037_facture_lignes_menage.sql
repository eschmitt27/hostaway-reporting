-- Migration 0037 — Lignes de facture prestataire ménage externe, sans passer par le module Charges.
--
-- Additive comme 0017→0036. Aucun ALTER TABLE.
--
-- POURQUOI PAS `facture_lignes` (0022)
-- `facture_lignes.charge_id` est NOT NULL : chaque ligne y suppose une Charge déjà créée par le
-- parcours Charges existant. Ce parcours reste, à ce jour, un écrivain Excel
-- (`saisie_charges_writer.py` → `SAISIE_Charges_Flux.xlsx`) — hors périmètre de cette mission
-- (Ménages/Lot6). Router les lignes de factures ménage externes par `facture_lignes` rattacherait
-- donc chaque ligne à une donnée qui reste, en bout de chaîne, Excel — l'inverse de l'objectif
-- « zéro Excel runtime » pour Ménages. `facture_lignes_menage` est un satellite : même racine
-- (`factures`, 0017 — pas de table `facture_menage_*` concurrente pour l'EN-TÊTE), mais une ligne
-- porte directement `logement_id`/`montant_ttc`, sans intermédiaire Charge.
--
-- LIGNE SANS LOGEMENT
-- `logement_id` NULL = frais/heures supplémentaires non affecté à l'extraction PDF (mission §9).
-- La ligne reste telle quelle (montant conservé dans le total de la facture, §32 : la ligne
-- originale doit rester visible) ; c'est `facture_ventilations`/`facture_ventilation_parts`
-- (migration 0036) qui explique, séparément, comment ce montant a été réparti — jamais une
-- réécriture de cette ligne.
CREATE TABLE IF NOT EXISTS facture_lignes_menage (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque     TEXT NOT NULL UNIQUE,        -- FLM-xxxx
    facture_id_opaque   TEXT NOT NULL,               -- factures.facture_id_opaque (0017)
    type_ligne          TEXT NOT NULL,
        -- MENAGE_INTERNE|MENAGE_EXTERNE|FRAIS_NON_AFFECTE|AUTRE
    logement_id         TEXT,                        -- NULL si frais non affecté (voir ci-dessus)
    menage_id_opaque    TEXT,                        -- prestation rattachée (menages, 0019) si connue
    description         TEXT,                        -- texte brut de la ligne PDF, pour audit
    montant_ht          REAL,
    montant_tva         REAL,
    montant_ttc         REAL NOT NULL,
    source               TEXT NOT NULL DEFAULT 'PDF_EXTRACTION',   -- PDF_EXTRACTION|SAISIE
    commentaire         TEXT,
    date_creation       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur              TEXT
);
CREATE INDEX IF NOT EXISTS idx_flm_facture ON facture_lignes_menage(facture_id_opaque);
CREATE INDEX IF NOT EXISTS idx_flm_logement ON facture_lignes_menage(logement_id);
CREATE INDEX IF NOT EXISTS idx_flm_menage ON facture_lignes_menage(menage_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0037');

-- Migration 0040 — Champs PDF manquants sur les lignes ménage externe + diagnostics d'import.
--
-- Additive comme 0017→0039. Aucun ALTER TABLE : `facture_lignes_menage` (0037) et
-- `facture_lignes_menage_detail` (0039) sont déjà en usage, table compagnon plutôt que colonne
-- ajoutée après coup (même raison que 0039).
--
-- CE QUI MANQUAIT (mission Ménages, Bloc A §3-4)
-- `menages_reader.externes()` (legacy, `MASTER_FACT_MEN_MenagesExternes.xlsx` onglet MASTER) exposait
-- `date_menage`/`precision_date_menage`/`nom_prestataire` par ligne. Ces trois champs viennent du
-- moteur d'extraction (`lib_menages_externes_pdf.LigneFacture.date_menage`/`.precision_date`,
-- `FactureExtraite.nom_prestataire`), jamais recalculés ni devinés ici — le pont
-- `facture_menage_pdf_service.importer()` ne les persistait pas avant cette migration ; il le fait
-- désormais lors de l'import (§5 : aucun second passage Excel).
CREATE TABLE IF NOT EXISTS facture_lignes_menage_pdf (
    ligne_id_opaque      TEXT PRIMARY KEY,     -- facture_lignes_menage.ligne_id_opaque
    date_menage          TEXT,                 -- NULL = DATE_MENAGE_ABSENTE, jamais complétée d'office
    precision_date_menage TEXT,                -- DATE_PRECISE|MOIS_FACTURE|MOIS_SEUL|MULTI_DATES (moteur)
    nom_prestataire      TEXT                  -- tel que lu par l'extracteur pour CETTE ligne
);

-- Diagnostic d'extraction PDF, un enregistrement par tentative d'import (succès ou échec) — même
-- grain que l'onglet DIAGNOSTIC_PDF legacy. `mode_extraction` distingue PDF_AUTOMATIQUE (extraction
-- réelle) de SAISIE_MANUELLE_SECOURS (transcription figée) : l'application ne parse jamais de PDF
-- elle-même, elle reflète ce que le moteur d'extraction a produit.
CREATE TABLE IF NOT EXISTS facture_pdf_diagnostics (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    nom_fichier          TEXT NOT NULL,
    format_detecte       TEXT,
    statut_extraction    TEXT NOT NULL,        -- OK|NON_SUPPORTE|VIDE|CORROMPU
    numero_facture       TEXT,
    montant_total        REAL,
    somme_lignes         REAL,
    ecart_reconciliation REAL,
    nb_lignes            INTEGER,
    doublon_de           TEXT,
    anomalies            TEXT,                 -- CSV des codes d'anomalie de l'extracteur
    mode_extraction      TEXT NOT NULL DEFAULT 'PDF_AUTOMATIQUE',  -- PDF_AUTOMATIQUE|SAISIE_MANUELLE_SECOURS
    facture_id_opaque    TEXT,                 -- NULL si l'extraction n'a pas produit de facture
    date_creation        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_facture_pdf_diag_fichier ON facture_pdf_diagnostics(nom_fichier);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0040');

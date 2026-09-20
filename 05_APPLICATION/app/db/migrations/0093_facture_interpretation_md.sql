-- Une facture fournisseur a UNE interprétation active, et une seule : PDF ou MD, jamais les deux.
--
-- Le PDF reste la pièce originale et la preuve. Un fichier `.md` de même nom de base peut en
-- donner l'interprétation structurée (contrat FACTURE_FOURNISSEUR_MD_V1) ; quand il est présent
-- ET valide, c'est LUI qui fait les lignes. Les lignes des deux sources ne se mélangent jamais :
-- changer de source crée une NOUVELLE VERSION d'interprétation, désactive les lignes précédentes
-- (conservées pour l'audit) et pose les nouvelles.
--
-- `factures.source_interpretation` dit ce qui fait foi AUJOURD'HUI ; `facture_interpretations`
-- garde l'historique des versions, avec l'empreinte du PDF et la version du référentiel logements
-- utilisées — c'est ce qui permet de dire qu'un MD est devenu obsolète.
ALTER TABLE factures ADD COLUMN source_interpretation TEXT;      -- PDF | MD
ALTER TABLE factures ADD COLUMN md_nom_fichier TEXT;
ALTER TABLE factures ADD COLUMN md_sha256 TEXT;
ALTER TABLE factures ADD COLUMN md_referentiel_version TEXT;
ALTER TABLE factures ADD COLUMN md_etat TEXT;                    -- VALIDE | ABSENT | INVALIDE | OBSOLETE

-- Les lignes portent la version d'interprétation qui les a produites : une ligne désactivée reste
-- lisible, et on sait de quelle lecture du document elle venait.
ALTER TABLE facture_lignes_menage ADD COLUMN interpretation_version INTEGER;

CREATE TABLE IF NOT EXISTS facture_interpretations (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque      TEXT NOT NULL,
    version                INTEGER NOT NULL,
    source                 TEXT NOT NULL,          -- PDF | MD
    md_nom_fichier         TEXT,
    md_sha256              TEXT,
    pdf_sha256             TEXT,
    referentiel_version    TEXT,
    etat                   TEXT,                   -- VALIDE | INVALIDE | OBSOLETE | ABSENT
    nb_lignes              INTEGER,
    montant_lignes         REAL,
    active                 INTEGER NOT NULL DEFAULT 1,
    motif                  TEXT,
    anomalies              TEXT,
    acteur                 TEXT,
    date_creation          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_facture_interpretation_version
    ON facture_interpretations(facture_id_opaque, version);
CREATE INDEX IF NOT EXISTS idx_facture_interpretation_active
    ON facture_interpretations(facture_id_opaque, active);

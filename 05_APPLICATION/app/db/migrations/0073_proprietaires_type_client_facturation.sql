-- Migration 0073 — Type de client de facturation, par propriétaire.
--
-- ADDITIVE. Aucune table existante modifiée, aucune donnée réécrite.
--
-- ── LE BESOIN ─────────────────────────────────────────────────────────────────────────────────
-- Les mentions légales d'une facture diffèrent selon que le client est un PARTICULIER ou un
-- PROFESSIONNEL : pénalités de retard et indemnité forfaitaire de recouvrement sont obligatoires
-- en B2B et déplacées, voire trompeuses, sur la facture d'un particulier.
-- `factures_proprietaires_conformite_service.client()` lit déjà `type_client` et laisse
-- `A_CONTROLER` faute d'information — l'information n'existait simplement nulle part.
--
-- ── POURQUOI UNE TABLE COMPAGNE, ET PAS UNE COLONNE DANS `ref_proprietaires` ───────────────────
-- `ref_proprietaires` est RECONSTRUITE à chaque import de `REF_Setup.xlsm` :
-- `ref_setup_import_service` supprime les lignes dont l'`import_id` n'est pas celui de
-- l'application, puis réinsère le contenu du classeur. Les 12 propriétaires actuels viennent du
-- classeur : une colonne ajoutée là serait donc VIDÉE au prochain import, sans prévenir.
-- Une table compagne survit à l'import — c'est exactement le raisonnement déjà tenu par la
-- migration 0071 pour `factures_proprietaires_meta`.
--
-- ── POURQUOI `A_CONTROLER` N'EST PAS UNE VALEUR STOCKABLE ─────────────────────────────────────
-- Le CHECK n'autorise que PARTICULIER et PROFESSIONNEL. « À contrôler » n'est pas un choix du
-- métier, c'est l'ABSENCE de choix : elle se lit à l'absence de ligne. Stocker `A_CONTROLER`
-- reviendrait à enregistrer qu'on ne sait pas, et rendrait indiscernables « jamais renseigné » et
-- « renseigné comme inconnu ».
--
-- Le type n'est JAMAIS déduit d'un nom, d'une adresse ou d'un SIREN : il est saisi par
-- l'utilisateur, et tracé (date + acteur).

CREATE TABLE IF NOT EXISTS proprietaires_facturation (
    proprietaire_id          TEXT PRIMARY KEY,
    type_client_facturation  TEXT NOT NULL
        CHECK (type_client_facturation IN ('PARTICULIER', 'PROFESSIONNEL')),
    -- Identifiants du client professionnel. Facultatifs : un professionnel peut être facturé sans
    -- que son SIREN soit connu, et rien ne doit être fabriqué pour combler la case.
    siren_client             TEXT,
    tva_intra_client         TEXT,
    date_creation            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification        TEXT,
    acteur                   TEXT
);

-- Journal des changements : qui a classé ce propriétaire, quand, et depuis quelle valeur. Un
-- reclassement change les mentions légales des factures futures ; il doit rester explicable.
CREATE TABLE IF NOT EXISTS proprietaires_facturation_evenements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    proprietaire_id   TEXT NOT NULL,
    valeur_avant      TEXT,
    valeur_apres      TEXT NOT NULL,
    motif             TEXT,
    acteur            TEXT,
    date_evenement    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_pfe_proprietaire
    ON proprietaires_facturation_evenements(proprietaire_id, date_evenement);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0073');

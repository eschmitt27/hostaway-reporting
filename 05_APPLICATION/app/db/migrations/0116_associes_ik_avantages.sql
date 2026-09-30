-- Associés : avantage associé conservé sur la charge, IK (indemnités kilométriques) justifiées.
--
-- ADDITIVE ET RÉTROCOMPATIBLE. Aucune table existante n'est réécrite, aucune ligne n'est modifiée
-- ni supprimée. Les anciennes charges gardent `avantage_associe` NULL (= non), comme avant.
--
-- 1. L'AVANTAGE ASSOCIÉ ÉTAIT DEMANDÉ PUIS PERDU. Le formulaire « Nouvelle charge » propose déjà
--    « Cette charge constitue-t-elle un avantage associé ? » et l'associé bénéficiaire (contrôlés
--    par V25/V26), mais la ligne enregistrée n'en gardait rien : `associe_id` est l'associé qui a
--    PAYÉ, pas celui qui en bénéficie. La charge reste la source de vérité : on y conserve le choix.
ALTER TABLE charges ADD COLUMN avantage_associe TEXT;      -- 'OUI' | NULL (non)
ALTER TABLE charges ADD COLUMN avantage_associe_id TEXT;   -- ref_associes.personne_id

-- 2. IK. Une IK EST une charge comptable (la ligne `charges` qu'elle référence) : son montant, son
--    compte et son écriture restent ceux de la charge, jamais recopiés ici. Cette table ne porte
--    que ce que la charge ne dit pas : l'associé, la période couverte, le statut de contrôle.
--    Aucune IK n'est attendue chaque mois : l'absence d'IK n'est jamais une anomalie.
CREATE TABLE IF NOT EXISTS ik (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ik_id_opaque    TEXT NOT NULL UNIQUE,
    charge_id       TEXT NOT NULL UNIQUE REFERENCES charges(charge_id),
    associe_id      TEXT NOT NULL,
    date_debut      TEXT NOT NULL,
    date_fin        TEXT NOT NULL,
    statut          TEXT NOT NULL DEFAULT 'BROUILLON'
                    CHECK (statut IN ('BROUILLON', 'A_CONTROLER', 'VALIDEE')),
    commentaire     TEXT,
    cree_par        TEXT,
    cree_le         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version         INTEGER NOT NULL DEFAULT 1,
    CHECK (date_fin >= date_debut)
);
CREATE INDEX IF NOT EXISTS idx_ik_associe ON ik(associe_id);

-- Relevé de trajets : autant de lignes que nécessaire. Justification, pas calcul : le montant de
-- l'IK reste celui de la charge (aucun barème n'est inventé ici).
CREATE TABLE IF NOT EXISTS ik_trajets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ik_id_opaque    TEXT NOT NULL REFERENCES ik(ik_id_opaque),
    date_trajet     TEXT NOT NULL,
    motif           TEXT NOT NULL CHECK (motif IN ('ACHATS', 'LOGEMENT', 'RDV_PROPRIETAIRE',
                        'RDV_CLIENT', 'ADMINISTRATIF', 'INTERVENTION', 'AUTRE')),
    depart          TEXT,
    destination     TEXT,
    km              REAL NOT NULL CHECK (km > 0),
    vehicule        TEXT,
    commentaire     TEXT,
    actif           INTEGER NOT NULL DEFAULT 1 CHECK (actif IN (0, 1)),
    cree_par        TEXT,
    cree_le         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_ik_trajets_ik ON ik_trajets(ik_id_opaque);

-- Part de l'IK engagée pour l'activité (dépense payée ensuite par l'associé sur son compte
-- personnel). ANALYTIQUE UNIQUEMENT : aucune écriture, la charge comptable reste entière. Datée du
-- débit réel sur le compte personnel — jamais proratisée.
CREATE TABLE IF NOT EXISTS ik_depenses_activite (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    depense_id_opaque       TEXT NOT NULL UNIQUE,
    ik_id_opaque            TEXT NOT NULL REFERENCES ik(ik_id_opaque),
    montant                 REAL NOT NULL CHECK (montant > 0),
    date_debit              TEXT NOT NULL,
    nature                  TEXT NOT NULL CHECK (nature IN ('MENAGE_PRESTATAIRE_INTERNE',
                                'FONCTIONNEMENT', 'FONCTIONNEMENT_NON_AFFECTABLE')),
    commentaire             TEXT,
    intervenant_id          TEXT,
    prestation_ref          TEXT,
    justificatif_reference  TEXT,
    actif                   INTEGER NOT NULL DEFAULT 1 CHECK (actif IN (0, 1)),
    cree_par                TEXT,
    cree_le                 TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_ik_depenses_ik ON ik_depenses_activite(ik_id_opaque);

-- Contrôle serveur, doublé en base : la part engagée ne dépasse JAMAIS le montant de l'IK.
CREATE TRIGGER IF NOT EXISTS trg_ik_depenses_plafond
BEFORE INSERT ON ik_depenses_activite
WHEN NEW.actif = 1 AND
     (SELECT COALESCE(SUM(d.montant), 0) FROM ik_depenses_activite d
       WHERE d.ik_id_opaque = NEW.ik_id_opaque AND d.actif = 1) + NEW.montant
     > (SELECT COALESCE(c.montant, 0) FROM ik JOIN charges c ON c.charge_id = ik.charge_id
         WHERE ik.ik_id_opaque = NEW.ik_id_opaque) + 0.005
BEGIN
    SELECT RAISE(ABORT, 'IK_DEPENSES_SUPERIEURES_AU_MONTANT');
END;

-- Une ligne se retire (actif 1 → 0), elle ne se réécrit pas et ne se réactive pas.
CREATE TRIGGER IF NOT EXISTS trg_ik_depenses_immuables
BEFORE UPDATE ON ik_depenses_activite
WHEN NEW.montant <> OLD.montant OR NEW.ik_id_opaque <> OLD.ik_id_opaque OR NEW.actif > OLD.actif
BEGIN
    SELECT RAISE(ABORT, 'IK_DEPENSE_IMMUABLE');
END;

CREATE TABLE IF NOT EXISTS ik_evenements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ik_id_opaque    TEXT NOT NULL,
    evenement       TEXT NOT NULL,
    detail          TEXT,
    acteur          TEXT,
    horodatage      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_ik_evenements_ik ON ik_evenements(ik_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0116');

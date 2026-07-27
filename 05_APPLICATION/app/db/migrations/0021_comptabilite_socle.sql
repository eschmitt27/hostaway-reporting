-- Migration 0021 — premier socle Comptabilité (cadrage `43_CADRAGE_COMPTABILITE_APPLICATION.md`).
--
-- Couche d'écritures en partie double, dérivée des objets déjà existants (facture, règlement,
-- rapprochement bancaire). Ne recalcule AUCUN résultat de gestion — Lot10 reste la seule vérité du
-- résultat économique. Seuls les journaux ACHATS et BANQUE sont câblés ce tour (verticale de
-- recette du brief) ; VENTES/CAISSE/ODIVERSES sont déclarés, pas générés.

CREATE TABLE IF NOT EXISTS plan_comptable (
    compte           TEXT PRIMARY KEY,
    libelle          TEXT NOT NULL,
    type_compte      TEXT NOT NULL,        -- ACTIF|PASSIF|CHARGE|PRODUIT
    actif            INTEGER NOT NULL DEFAULT 1,
    date_debut       TEXT,
    date_fin         TEXT,
    auxiliaire_autorise INTEGER NOT NULL DEFAULT 0,
    analytique_obligatoire INTEGER NOT NULL DEFAULT 0,
    commentaire      TEXT
);

-- Seed provisoire — décision NON définitive (cf. cadrage 43). Un mapping catégorie->compte fin
-- reste à arbitrer métier ; 606000 sert de compte générique par défaut.
INSERT OR IGNORE INTO plan_comptable (compte, libelle, type_compte, auxiliaire_autorise, commentaire) VALUES
    ('401000', 'Fournisseurs',                       'PASSIF',  1, 'Auxiliaire = fournisseur_id_opaque'),
    ('411000', 'Propriétaires (créance/compensation)', 'ACTIF', 1, 'Auxiliaire = proprietaire_id ; non généré ce tour'),
    ('512000', 'Banque',                              'ACTIF',  0, NULL),
    ('606000', 'Achats et charges externes (générique)', 'CHARGE', 0, 'Compte générique par défaut — mapping fin non arbitré');

CREATE TABLE IF NOT EXISTS ecritures (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ecriture_id_opaque  TEXT NOT NULL UNIQUE,     -- ECR-xxxx
    journal             TEXT NOT NULL,            -- ACHATS|VENTES|BANQUE|CAISSE|ODIVERSES
    date_ecriture       TEXT NOT NULL,
    periode             TEXT NOT NULL,             -- AAAA-MM
    piece                TEXT,                      -- référence de la pièce justificative
    libelle              TEXT NOT NULL,
    origine_type         TEXT NOT NULL,             -- FACTURE|REGLEMENT|RAPPROCHEMENT|AVOIR|MANUEL
    origine_id_opaque    TEXT,
    statut               TEXT NOT NULL DEFAULT 'PROPOSEE',
        -- PROPOSEE|VALIDEE|CONTREPASSEE
    total_debit          REAL NOT NULL DEFAULT 0,
    total_credit         REAL NOT NULL DEFAULT 0,
    contrepasse_de        TEXT,                     -- ecriture_id_opaque de l'écriture annulée, si contrepassation
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_ecritures_journal ON ecritures(journal);
CREATE INDEX IF NOT EXISTS idx_ecritures_periode ON ecritures(periode);
-- Idempotence : une seule écriture par (journal, origine). Regénérer pour la même origine est un
-- no-op explicite (détecté avant tentative d'insertion), jamais un doublon.
CREATE UNIQUE INDEX IF NOT EXISTS idx_ecritures_origine
    ON ecritures(journal, origine_type, origine_id_opaque)
    WHERE origine_id_opaque IS NOT NULL AND statut <> 'CONTREPASSEE';

CREATE TABLE IF NOT EXISTS ecriture_lignes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ecriture_id_opaque   TEXT NOT NULL,
    ligne_num            INTEGER NOT NULL,
    compte               TEXT NOT NULL,
    auxiliaire           TEXT,                      -- fournisseur_id_opaque / proprietaire_id / associe_id
    debit                REAL NOT NULL DEFAULT 0,
    credit               REAL NOT NULL DEFAULT 0,
    logement_id          TEXT,
    proprietaire_id      TEXT,
    reservation_id       TEXT,
    libelle              TEXT
);
CREATE INDEX IF NOT EXISTS idx_ecriture_lignes_ecriture ON ecriture_lignes(ecriture_id_opaque);
CREATE INDEX IF NOT EXISTS idx_ecriture_lignes_compte ON ecriture_lignes(compte);

CREATE TABLE IF NOT EXISTS ecriture_evenements (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ecriture_id_opaque TEXT NOT NULL,
    type_evenement     TEXT NOT NULL,      -- GENERATION|VALIDATION|CONTREPASSATION
    ancien_statut      TEXT,
    nouveau_statut     TEXT,
    commentaire        TEXT,
    date_evenement     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur             TEXT
);
CREATE INDEX IF NOT EXISTS idx_ecriture_evenements_ecriture ON ecriture_evenements(ecriture_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0021');

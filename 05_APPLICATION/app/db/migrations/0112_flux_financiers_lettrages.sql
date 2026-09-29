-- Flux financiers : le LETTRAGE, décision humaine qui relie N mouvements à M objets métier.
--
-- AUCUNE TABLE MÉTIER N'EST FUSIONNÉE. Mouvements bancaires (`qonto_*`), opérations de caisse,
-- charges, factures, règlements, rapprochements et écritures restent dans leurs tables. Ce que
-- cette migration ajoute, c'est l'ACTE qui manquait : « ces mouvements-là règlent ces objets-là,
-- et voici l'écriture qui le constate », validé en une seule fois, par une personne nommée.
--
-- `banque_rapprochements` reste la vérité sur « ce mouvement est-il rapproché ? ». Un lettrage
-- validé y dépose ses liens mouvement ↔ objet (statut CONFIRME), marqués de son identifiant :
-- les écrans existants continuent de lire le même journal, et rien n'a deux sources.

-- ── 1. Le lettrage ────────────────────────────────────────────────────────────────────────────
-- Un lettrage n'existe qu'une fois VALIDÉ : les propositions du moteur ne sont jamais écrites
-- (elles se recalculent), si bien qu'une proposition ne peut pas « traîner » en base et passer
-- pour une décision. Seul un refus humain laisse une trace (§3).
CREATE TABLE IF NOT EXISTS flux_lettrages (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    lettrage_id_opaque      TEXT NOT NULL UNIQUE,                 -- LET-xxxxxxxxxxxx
    statut                  TEXT NOT NULL DEFAULT 'VALIDE'
        CHECK (statut IN ('VALIDE', 'ANNULE')),
    source                  TEXT NOT NULL DEFAULT 'MANUEL'
        CHECK (source IN ('PROPOSITION', 'MANUEL')),
    confiance               TEXT,                                  -- FORTE | MOYENNE | FAIBLE
    explication             TEXT,
    -- Empreinte de la combinaison (mouvements, objets, montants). Sert l'idempotence : un double
    -- clic ne peut pas valider deux fois la même décision (index unique partiel ci-dessous).
    empreinte               TEXT NOT NULL,
    total_mouvements        REAL NOT NULL,
    total_objets            REAL NOT NULL,
    ecart                   REAL NOT NULL DEFAULT 0,
    traitement_ecart        TEXT NOT NULL DEFAULT 'AUCUN'
        CHECK (traitement_ecart IN ('AUCUN', 'SOLDE_OUVERT', 'COMPTABILISE')),
    compte_ecart            TEXT,
    ecriture_id_opaque      TEXT,                                  -- ECR-… produite dans la même transaction
    ecriture_modifiee       INTEGER NOT NULL DEFAULT 0,            -- 1 = proposition corrigée à la main
    justification           TEXT,
    acteur                  TEXT NOT NULL,
    cree_le                 TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    annule_le               TEXT,
    annule_par              TEXT,
    motif_annulation        TEXT,
    version                 INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_flux_lettrage_empreinte_valide
    ON flux_lettrages(empreinte) WHERE statut = 'VALIDE';

-- ── 2. Les deux côtés du lettrage ─────────────────────────────────────────────────────────────
-- `cote` = MOUVEMENT (banque ou caisse) ou OBJET (charge, facture, règlement…). `montant` est la
-- part de l'élément réellement engagée dans CE lettrage — une facture de 1 000 € réglée en trois
-- virements apparaît une fois côté OBJET (1 000) et trois fois côté MOUVEMENT (300, 300, 400).
CREATE TABLE IF NOT EXISTS flux_lettrage_lignes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    lettrage_id_opaque      TEXT NOT NULL REFERENCES flux_lettrages(lettrage_id_opaque),
    cote                    TEXT NOT NULL CHECK (cote IN ('MOUVEMENT', 'OBJET')),
    type_element            TEXT NOT NULL,       -- BANQUE | CAISSE | CHARGE | FACTURE_FOURNISSEUR | …
    element_id              TEXT NOT NULL,
    montant                 REAL NOT NULL CHECK (montant > 0),
    libelle                 TEXT                 -- libellé lisible figé au moment de la décision
);

CREATE INDEX IF NOT EXISTS idx_flux_lettrage_lignes_element
    ON flux_lettrage_lignes(type_element, element_id);
CREATE INDEX IF NOT EXISTS idx_flux_lettrage_lignes_lettrage
    ON flux_lettrage_lignes(lettrage_id_opaque);

-- ── 3. Journal append-only ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS flux_lettrage_evenements (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    lettrage_id_opaque      TEXT NOT NULL,
    type_evenement          TEXT NOT NULL,        -- VALIDATION | ANNULATION
    detail_json             TEXT,
    acteur                  TEXT NOT NULL,
    horodatage              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_flux_lettrage_evenements_lettrage
    ON flux_lettrage_evenements(lettrage_id_opaque);

-- ── 4. Refus humain d'une proposition ─────────────────────────────────────────────────────────
-- Refuser ne change RIEN aux objets ni aux mouvements : la seule trace est ce refus, qui empêche
-- le moteur de reproposer exactement la même combinaison. `actif = 0` la rend à nouveau
-- proposable sans effacer l'historique.
CREATE TABLE IF NOT EXISTS flux_propositions_refusees (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    empreinte               TEXT NOT NULL,
    mouvements_json         TEXT NOT NULL,
    objets_json             TEXT NOT NULL,
    motif                   TEXT,
    acteur                  TEXT NOT NULL,
    refuse_le               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    actif                   INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_flux_refus_empreinte_actif
    ON flux_propositions_refusees(empreinte) WHERE actif = 1;

-- ── 5. Rattachement des liens de rapprochement à leur lettrage ────────────────────────────────
-- Additif : les liens historiques gardent NULL, ils restent lus exactement comme avant.
ALTER TABLE banque_rapprochements ADD COLUMN lettrage_id_opaque TEXT;

CREATE INDEX IF NOT EXISTS idx_banque_rapprochements_lettrage
    ON banque_rapprochements(lettrage_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0112');

-- Plan comptable et mappings administrables depuis l'application (Mission 31).
--
-- ADDITIVE. Aucune ligne existante n'est modifiée, aucun compte n'est créé, aucune règle n'est
-- ajoutée : le plan reste celui que l'utilisateur a (8 comptes, un seul de charge), et les 27
-- catégories restent sans règle validée. Cette migration ne fait que rendre ce paramétrage
-- administrable ET traçable.

-- ── 1. Plan comptable : horodatages et journal ───────────────────────────────────────────────
-- NULL pour les comptes antérieurs : leur date de création réelle est inconnue, on ne l'invente
-- pas (ils viennent des migrations 0021/0023/0099).
ALTER TABLE plan_comptable ADD COLUMN date_creation TEXT;
ALTER TABLE plan_comptable ADD COLUMN date_modification TEXT;

CREATE TABLE IF NOT EXISTS plan_comptable_evenements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    compte          TEXT NOT NULL,
    type_evenement  TEXT NOT NULL
        CHECK (type_evenement IN ('CREATION', 'MODIFICATION', 'DESACTIVATION', 'REACTIVATION')),
    avant_json      TEXT,
    apres_json      TEXT,
    motif           TEXT,
    acteur          TEXT NOT NULL,
    horodatage      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_plan_comptable_evenements_compte
    ON plan_comptable_evenements(compte);

-- Un compte ayant existé ne disparaît jamais : des écritures le citent par son numéro. On le
-- désactive. Le garde-fou est dans le schéma, pas seulement dans le code.
CREATE TRIGGER IF NOT EXISTS trg_plan_comptable_sans_suppression
BEFORE DELETE ON plan_comptable
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : un compte se désactive, il ne se supprime pas.');
END;

-- Le numéro EST l'identité du compte (les lignes d'écriture le portent par valeur) : le renommer
-- rendrait l'historique incohérent. Pour un autre numéro, on crée un autre compte.
CREATE TRIGGER IF NOT EXISTS trg_plan_comptable_numero_immuable
BEFORE UPDATE OF compte ON plan_comptable
WHEN NEW.compte <> OLD.compte
BEGIN
    SELECT RAISE(ABORT, 'Le numéro d''un compte ne se modifie pas.');
END;

-- ── 2. Règles de mapping : désactivation et journal ─────────────────────────────────────────
-- `actif` = la règle participe à la résolution. Une règle désactivée reste lisible (historique),
-- elle ne propose plus rien. Défaut 1 : les règles existantes gardent leur effet.
ALTER TABLE mapping_comptable_regles ADD COLUMN actif INTEGER NOT NULL DEFAULT 1;
ALTER TABLE mapping_comptable_regles ADD COLUMN date_modification TEXT;

CREATE TABLE IF NOT EXISTS mapping_regle_evenements (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    regle_id_opaque  TEXT NOT NULL,
    type_evenement   TEXT NOT NULL
        CHECK (type_evenement IN ('CREATION', 'MODIFICATION', 'VALIDATION', 'PASSAGE_PROVISOIRE',
                                  'DESACTIVATION', 'REACTIVATION')),
    avant_json       TEXT,
    apres_json       TEXT,
    motif            TEXT,
    acteur           TEXT NOT NULL,
    horodatage       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mapping_regle_evenements_regle
    ON mapping_regle_evenements(regle_id_opaque);

CREATE TRIGGER IF NOT EXISTS trg_mapping_regles_sans_suppression
BEFORE DELETE ON mapping_comptable_regles
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : une règle se désactive, elle ne se supprime pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0113');

-- Migration 0007 — journal du SUIVI HUMAIN des contrôles détaillés APP-5B.
-- Le moteur (Lot11) reste la vérité de l'anomalie. Ce journal ne fait JAMAIS autorité et ne masque
-- JAMAIS une anomalie moteur : il consigne uniquement la prise en charge, la décision humaine et les
-- exceptions. Quatre dimensions distinctes : anomalie moteur (présente/absente), prise en charge,
-- résultat humain, statut de suivi. Une seule décision ACTIVE par contrôle détaillé ; historique
-- append-only ; contrôle de version optimiste ; jamais de suppression physique.

CREATE TABLE IF NOT EXISTS controles_suivi (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    controle_id_opaque       TEXT NOT NULL,      -- identifiant public opaque CTRL-xxxx (aucune donnée sensible)
    ctrl_pk_moteur           TEXT,               -- correspondance interne vers le contrôle moteur (Lot11)
    code_controle            TEXT NOT NULL,
    module                   TEXT,
    entite_id                TEXT,               -- entité détaillée (réservation, logement, mouvement opaque…)
    mois                     TEXT,
    statut_suivi             TEXT NOT NULL,      -- OUVERT|EN_COURS|RESOLU|ACCEPTE_AVEC_JUSTIFICATION|ROUVERT
    resultat_humain          TEXT,               -- CORRIGE|EXCEPTION_ACCEPTEE|NON_CORRIGE|A_REVOIR
    responsable              TEXT,
    commentaire              TEXT,
    justification            TEXT,               -- obligatoire pour EXCEPTION / RESOLU sous exception
    preuve_reference         TEXT,               -- réf. preuve / recalcul moteur ayant démontré la disparition
    exception_portee         TEXT,               -- ENTITE|MOIS|PERIODE
    exception_expiration     TEXT,               -- date d'expiration facultative de l'exception
    date_prise_en_charge     TEXT,
    date_resolution          TEXT,
    anomalie_moteur_presente INTEGER NOT NULL DEFAULT 1,  -- 1 présente au moment de la décision, 0 absente
    version                  INTEGER NOT NULL DEFAULT 1,
    actif                    INTEGER NOT NULL DEFAULT 1,   -- 1 = décision courante, 0 = historisée
    auteur                   TEXT,
    created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_controles_suivi_ctrl ON controles_suivi(controle_id_opaque, actif);

-- Historique append-only : une ligne par transition (jamais de suppression physique).
CREATE TABLE IF NOT EXISTS controles_suivi_historique (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    controle_id_opaque  TEXT NOT NULL,
    action              TEXT NOT NULL,          -- PRISE_EN_CHARGE|COMMENTAIRE|CORRIGE|EXCEPTION|ROUVERT|ANNULATION|REOUVERT_AUTO
    ancienne_valeur     TEXT,
    nouvelle_valeur     TEXT,
    auteur              TEXT,
    version             INTEGER NOT NULL,
    date_action         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_controles_suivi_hist ON controles_suivi_historique(controle_id_opaque);

-- Runs de recalcul moteur sur COPIES (Lot8c/Lot11) — traçabilité, jamais sur le réel.
CREATE TABLE IF NOT EXISTS controles_runs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    type_action          TEXT NOT NULL,         -- RECALCUL_COPIE_LOT8C_LOT11 | PREFLIGHT | COMPARAISON
    statut               TEXT NOT NULL,         -- SUCCES|ECHEC|BLOQUE|VERROUILLE
    nb_elements          INTEGER,
    workspace_path       TEXT,
    reel_intact          INTEGER,
    avant_json           TEXT,                  -- contrôles avant recalcul (copie)
    apres_json           TEXT,                  -- contrôles après recalcul (copie)
    erreur_code          TEXT,
    erreur_resume        TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0007');

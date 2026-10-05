-- 0127 — Décision explicite « exclure du périmètre de gestion » d'un séjour (module Réservations).
--
-- POURQUOI. Un séjour Hostaway dont les dates ne tombent dans aucune période de gestion de son logement
-- (logement retiré, mandat terminé…) n'a pas de propriétaire : le moteur le laisse « à contrôler », hors du
-- calcul et des factures, tant que personne n'a tranché. Jusqu'ici RIEN ne permettait de trancher « ce
-- séjour n'est pas à nous » : seuls les exclusions automatiques (séjour propriétaire, logement hors parc,
-- statut Hostaway) existaient. Le séjour restait donc « à contrôler » pour toujours, et bloquait chaque
-- clôture mensuelle sans réponse possible.
--
-- CE QUI EST STOCKÉ : la décision (qui, quand, pourquoi, sur quel séjour) et son journal. Rien d'autre —
-- le séjour lui-même reste dans `reservations_resolues`, visible, avec son historique ; le moteur lit
-- cette table pour le classer « exclu » (`EXCLU_RESULTAT`, motif `EXCLUSION_DECIDEE`) au prochain calcul.
-- Ce n'est PAS une case propre à la clôture : c'est une décision métier du module Réservations, que la
-- clôture se contente de lire.
--
-- `reservation_perimetre_decisions` : une décision par séjour ET par période de validité (une seule
-- décision ACTIVE à la fois) ; annuler une décision ne l'efface pas, elle passe à ANNULEE.
-- `reservation_perimetre_evenements` : journal APPEND-ONLY (exclusion, réintégration).
--
-- Additive : aucune table existante modifiée, aucune donnée existante touchée.

CREATE TABLE IF NOT EXISTS reservation_perimetre_decisions (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id               TEXT NOT NULL UNIQUE,          -- DEC-xxxx (opaque)
    reservation_calc_id       TEXT NOT NULL,                 -- RES-HA-<id Hostaway> : clé économique stable
    reservation_id_hostaway   TEXT NOT NULL,
    logement_id               TEXT,
    mois                      TEXT NOT NULL,                 -- AAAA-MM d'arrivée du séjour
    date_arrivee              TEXT,
    date_depart               TEXT,
    canal                     TEXT,
    montant_retenu            REAL,                          -- tel qu'affiché au moment de la décision
    code_constat              TEXT NOT NULL,                 -- ce que le moteur disait (GESTION_LOGEMENT_*)
    decision                  TEXT NOT NULL DEFAULT 'EXCLURE_PERIMETRE_GESTION'
                              CHECK (decision IN ('EXCLURE_PERIMETRE_GESTION')),
    statut                    TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (statut IN ('ACTIVE', 'ANNULEE')),
    justification             TEXT NOT NULL CHECK (length(trim(justification)) > 0),
    acteur                    TEXT,
    date_decision             TEXT NOT NULL,                 -- heure locale du poste
    annulee_par               TEXT,
    date_annulation           TEXT,
    justification_annulation  TEXT
);
-- Une seule décision ACTIVE par séjour : une décision annulée (ANNULEE) laisse la place à une nouvelle.
CREATE UNIQUE INDEX IF NOT EXISTS idx_perimetre_decision_active
    ON reservation_perimetre_decisions(reservation_id_hostaway) WHERE statut = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_perimetre_decision_mois ON reservation_perimetre_decisions(mois);
CREATE INDEX IF NOT EXISTS idx_perimetre_decision_logement ON reservation_perimetre_decisions(logement_id);

CREATE TABLE IF NOT EXISTS reservation_perimetre_evenements (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id               TEXT NOT NULL,
    reservation_id_hostaway   TEXT NOT NULL,
    mois                      TEXT NOT NULL,
    type_evenement            TEXT NOT NULL CHECK (type_evenement IN ('EXCLUSION', 'REINTEGRATION')),
    ancien_etat               TEXT,
    nouvel_etat               TEXT,
    justification             TEXT NOT NULL,
    acteur                    TEXT,
    date_evenement            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_perimetre_evt_decision ON reservation_perimetre_evenements(decision_id);
CREATE INDEX IF NOT EXISTS idx_perimetre_evt_resa ON reservation_perimetre_evenements(reservation_id_hostaway);

-- Journal APPEND-ONLY : aucune trace de décision ne se corrige ni ne s'efface.
CREATE TRIGGER IF NOT EXISTS trg_perimetre_evt_no_update
BEFORE UPDATE ON reservation_perimetre_evenements
BEGIN
    SELECT RAISE(ABORT, 'DECISION_PERIMETRE_TRACE : l''historique des décisions est en ajout seul.');
END;

CREATE TRIGGER IF NOT EXISTS trg_perimetre_evt_no_delete
BEFORE DELETE ON reservation_perimetre_evenements
BEGIN
    SELECT RAISE(ABORT, 'DECISION_PERIMETRE_TRACE : l''historique des décisions ne s''efface pas.');
END;

-- Une décision ne se supprime jamais ; la seule modification admise est son ANNULATION (ACTIVE → ANNULEE),
-- qui laisse intacts le séjour, la justification, l'auteur et la date de la décision d'origine.
CREATE TRIGGER IF NOT EXISTS trg_perimetre_decision_no_delete
BEFORE DELETE ON reservation_perimetre_decisions
BEGIN
    SELECT RAISE(ABORT, 'DECISION_PERIMETRE_TRACE : une décision ne se supprime pas, elle s''annule.');
END;

CREATE TRIGGER IF NOT EXISTS trg_perimetre_decision_annulation_seule
BEFORE UPDATE ON reservation_perimetre_decisions
WHEN NOT (OLD.statut = 'ACTIVE' AND NEW.statut = 'ANNULEE'
          AND NEW.decision_id = OLD.decision_id
          AND NEW.reservation_calc_id = OLD.reservation_calc_id
          AND NEW.reservation_id_hostaway = OLD.reservation_id_hostaway
          AND NEW.mois = OLD.mois
          AND NEW.justification = OLD.justification
          AND NEW.acteur IS OLD.acteur
          AND NEW.date_decision = OLD.date_decision
          AND NEW.code_constat = OLD.code_constat)
BEGIN
    SELECT RAISE(ABORT, 'DECISION_PERIMETRE_TRACE : seule l''annulation d''une décision active est possible.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0127');

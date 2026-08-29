-- Migration 0062 — Positions de refacturation des charges (mission 15).
--
-- Sépare explicitement trois notions jusqu'ici confondues :
--   CHARGE (table `charges`, 0052)      = l'événement économique d'origine, inchangé.
--   POSITION DE REFACTURATION (ici)     = montant disponible à récupérer auprès d'un propriétaire,
--                                         créée AUTOMATIQUEMENT dès qu'une charge porte
--                                         refacturable='OUI' (point d'entrée unique : la saisie de
--                                         charge elle-même, jamais un second flux de création).
--   IMPUTATION (charges_refacturation_evenements, evenement='IMPUTATION'/'IMPUTATION_PARTIELLE')
--                                        = décision humaine réalisée, au moment de la VALIDATION
--                                          effective d'une facture propriétaire — jamais à
--                                          l'aperçu/recalcul.
--
-- Avant cette migration, `refacturable='OUI'` suffisait à ce qu'`aggregate_refacturable_charges()`
-- (lib_settlements.py, appelé par Lot10) transforme directement la charge en produit de
-- refacturation réalisé, sans aucune décision humaine ni possibilité de report/imputation
-- partielle/non-refacturation — une case cochée à la saisie valait facturation automatique.
--
-- Une position par charge (pas par ventilation) : le multi-logements est déjà porté par `charges`
-- elle-même (une ligne par logement, même `justificatif` partagé, mission 14g) — chaque ligne
-- charge devient donc naturellement sa propre position, traçable indépendamment.
CREATE TABLE IF NOT EXISTS charges_refacturation_positions (
    position_id           TEXT PRIMARY KEY,
    charge_id             TEXT NOT NULL UNIQUE REFERENCES charges(charge_id),
    montant_origine       REAL NOT NULL,
    montant_eligible       REAL NOT NULL,
    proprietaire_id       TEXT,
    logement_id           TEXT,
    statut                TEXT NOT NULL,
    -- Total imputé : source de vérité pour l'invariant montant_impute_total <= montant_eligible,
    -- maintenu transactionnellement par le service (jamais par un trigger : la décision humaine et
    -- son audit doivent rester dans la même transaction applicative que la mise à jour du total).
    montant_impute_total  REAL NOT NULL DEFAULT 0,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_disponibilite    TEXT,
    derniere_decision     TEXT,
    derniere_justification TEXT,
    date_derniere_decision TEXT,
    acteur_derniere_decision TEXT,
    actif                 INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_charges_refac_pos_statut
    ON charges_refacturation_positions(statut);
CREATE INDEX IF NOT EXISTS idx_charges_refac_pos_prop_log
    ON charges_refacturation_positions(proprietaire_id, logement_id);

-- Journal append-only : chaque transition (création, proposition, report, imputation totale ou
-- partielle, modification de montant, non-refacturation, libération) — jamais de suppression, une
-- charge ne doit jamais disparaître silencieusement (même garantie que `charge_evenements`).
CREATE TABLE IF NOT EXISTS charges_refacturation_evenements (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id    TEXT NOT NULL REFERENCES charges_refacturation_positions(position_id),
    evenement      TEXT NOT NULL,
    montant        REAL,
    facture_id     TEXT,
    acteur         TEXT,
    motif          TEXT,
    avant_json     TEXT,
    apres_json     TEXT,
    date_evenement TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_charges_refac_evt_position
    ON charges_refacturation_evenements(position_id);
CREATE INDEX IF NOT EXISTS idx_charges_refac_evt_facture
    ON charges_refacturation_evenements(facture_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0062');

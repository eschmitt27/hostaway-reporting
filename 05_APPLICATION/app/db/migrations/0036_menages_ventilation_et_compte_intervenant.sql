-- Migration 0036 — Ventilation facture externe ménage + compte FIFO intervenant interne.
--
-- Additive comme 0017→0035. Aucun ALTER TABLE.
--
-- CE QUI N'EST PAS CRÉÉ ICI, ET POURQUOI
--   Pas de nouvelle table `facture_menage_*` : une facture prestataire ménage externe est une
--   `facture` (0017) comme une autre, ses lignes vivent dans `facture_lignes` (0022), chaque ligne
--   pointe une `charge_id` déjà créée par le module Charges existant. Dupliquer ce mécanisme
--   compterait la même opération deux fois (règle explicite de la mission).
--   Pas de FIFO propriétaire réutilisé tel quel : `proprietaire_allocations` (0030) reste au
--   propriétaire. Le compte intervenant ménage interne est un objet métier différent — même
--   algorithme (factorisé côté Python, `compte_proprietaire_service.calculer_fifo`), tables
--   séparées.
--
-- VENTILATION D'UNE LIGNE SANS LOGEMENT (facture externe)
-- Une ligne de facture prestataire ménage qui ne désigne aucun logement (frais/heures
-- supplémentaires) est répartie au prorata du coût des ménages des AUTRES logements déjà présents
-- dans la même facture — jamais sur l'historique complet du prestataire. `facture_ventilations`
-- trace CETTE décision : montant d'origine non affecté, base de pondération, et le détail par
-- logement (via `facture_ventilation_parts`), pour qu'un utilisateur revoie exactement le calcul
-- (§32 de la mission : traçabilité de la règle utilisée, pas seulement du résultat).
CREATE TABLE IF NOT EXISTS facture_ventilations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ventilation_id_opaque TEXT NOT NULL UNIQUE,       -- VENT-xxxx
    facture_id_opaque     TEXT NOT NULL,
    montant_non_affecte   REAL NOT NULL,
    base_ponderation      TEXT NOT NULL,               -- COUT_MENAGES_FACTURE (seule règle confirmée)
    statut                TEXT NOT NULL,               -- APPLIQUEE|NON_EFFECTUEE
    motif                 TEXT,                        -- obligatoire si NON_EFFECTUEE
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT
);
CREATE INDEX IF NOT EXISTS idx_facture_ventilations_facture
    ON facture_ventilations(facture_id_opaque);

CREATE TABLE IF NOT EXISTS facture_ventilation_parts (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ventilation_id_opaque TEXT NOT NULL,
    logement_id           TEXT NOT NULL,
    cout_menages_logement REAL NOT NULL,               -- base servant au poids, conservée pour audit
    poids                 REAL NOT NULL,
    part_montant          REAL NOT NULL,
    FOREIGN KEY (ventilation_id_opaque)
        REFERENCES facture_ventilations(ventilation_id_opaque)
);
CREATE INDEX IF NOT EXISTS idx_facture_ventilation_parts_vent
    ON facture_ventilation_parts(ventilation_id_opaque);

-- ═══ COMPTE INTERVENANT MÉNAGE INTERNE (FIFO) ════════════════════════════════════════════════════
--
-- Une dette est une créance UNITAIRE : un ménage interne VALIDÉ, valorisé au tarif standard
-- applicable à sa date. Ce grain (1 dette = 1 ménage) donne la traçabilité la plus fine possible
-- (§23) sans rien perdre du calcul global — la somme des dettes d'un intervenant est identique à un
-- calcul par lot, et le FIFO consomme les dettes dans l'ordre, quel que soit leur découpage.
CREATE TABLE IF NOT EXISTS intervenant_menage_dettes (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    dette_id_opaque       TEXT NOT NULL UNIQUE,        -- DIM-xxxx
    intervenant_id        TEXT NOT NULL,               -- fournisseur_id_opaque (0010), type INTERNE
    menage_id_opaque      TEXT NOT NULL UNIQUE,        -- menages.menage_id_opaque (0019) — 1 dette / ménage
    date_validation        TEXT NOT NULL,              -- date de référence du tarif (date_realisation)
    tarif_unitaire        REAL NOT NULL,
    tarif_ref_id          TEXT,                        -- identifiant REF_Couts_Menage_Interne utilisé
    montant                REAL NOT NULL,
    statut                TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE|ANNULEE
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_dim_intervenant ON intervenant_menage_dettes(intervenant_id);

CREATE TABLE IF NOT EXISTS intervenant_menage_paiements (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    paiement_id_opaque    TEXT NOT NULL UNIQUE,        -- PIM-xxxx
    intervenant_id        TEXT NOT NULL,
    date_paiement         TEXT NOT NULL,
    montant                REAL NOT NULL,
    moyen                 TEXT,                        -- ESPECES|VIREMENT|CHEQUE...
    mouvement_banque_ref  TEXT,                        -- preuve éventuelle (jamais une création automatique)
    statut                TEXT NOT NULL DEFAULT 'VALIDE',  -- VALIDE|ANNULE
    commentaire           TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT
);
CREATE INDEX IF NOT EXISTS idx_pim_intervenant ON intervenant_menage_paiements(intervenant_id);

CREATE TABLE IF NOT EXISTS intervenant_menage_allocations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    allocation_id_opaque  TEXT NOT NULL UNIQUE,        -- AIM-xxxx
    intervenant_id        TEXT NOT NULL,
    recalcul_id           TEXT NOT NULL,
    paiement_id_opaque    TEXT NOT NULL,
    dette_id_opaque       TEXT NOT NULL,
    montant_alloue        REAL NOT NULL,
    rang_fifo             INTEGER NOT NULL,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_aim_intervenant ON intervenant_menage_allocations(intervenant_id);
CREATE INDEX IF NOT EXISTS idx_aim_dette ON intervenant_menage_allocations(dette_id_opaque);
CREATE INDEX IF NOT EXISTS idx_aim_recalcul ON intervenant_menage_allocations(recalcul_id);

CREATE TABLE IF NOT EXISTS intervenant_menage_recalculs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    recalcul_id           TEXT NOT NULL UNIQUE,        -- RCM-xxxx
    intervenant_id        TEXT NOT NULL,
    horodatage            TEXT NOT NULL,
    empreinte_entrees     TEXT NOT NULL,
    nb_dettes             INTEGER NOT NULL DEFAULT 0,
    nb_paiements          INTEGER NOT NULL DEFAULT 0,
    nb_allocations        INTEGER NOT NULL DEFAULT 0,
    montant_alloue        REAL NOT NULL DEFAULT 0,
    dette_restante        REAL NOT NULL DEFAULT 0,
    declencheur           TEXT NOT NULL DEFAULT 'MANUEL'
);
CREATE INDEX IF NOT EXISTS idx_rcm_intervenant
    ON intervenant_menage_recalculs(intervenant_id, horodatage);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0036');

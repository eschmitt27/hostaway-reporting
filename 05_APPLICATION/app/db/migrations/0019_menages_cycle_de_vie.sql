-- Migration 0019 — cycle de vie opérationnel des ménages (module Ménages, tour du 2026-07-27).
--
-- Le module existant COMPTE et RAPPROCHE (Hostaway vs déclaré, coût standard/direct/complet) : il
-- compare des agrégats par (mois, logement_id, intervenant_id). Il n'a JAMAIS eu d'objet ménage
-- unitaire ni de statut opérationnel (audit `42_AUDIT_MENAGES_CYCLE_DE_VIE.md`).
--
-- `menages` introduit ce grain. Il ne remplace ni ne masque `menage_overrides` (0003, journal de
-- justification d'écart de comptage) ni les MASTER_* Excel (source de vérité du comptage) : c'est
-- une couche opérationnelle au-dessus, pour les ménages saisis ou suivis individuellement
-- (création hors Hostaway, affectation, remplacement, rattachements).
--
-- Statuts : PREVU|A_AFFECTER|A_REALISER|REALISE|A_CONTROLER|VALIDE|FACTURE|REGLE|ANNULE|LITIGE.
-- Aucune suppression physique — désactivation logique par statut ANNULE, comme fournisseurs/factures.

CREATE TABLE IF NOT EXISTS menages (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    menage_id_opaque      TEXT NOT NULL UNIQUE,      -- MEN-xxxx
    mois                  TEXT NOT NULL,             -- AAAA-MM, dérivé de la date
    date_prevue           TEXT,
    date_realisation      TEXT,
    logement_id           TEXT NOT NULL,
    proprietaire_id       TEXT NOT NULL,             -- résolu à la création, jamais recalculé après
    reservation_id        TEXT,                      -- NULL si ménage hors réservation (ex. sortie propriétaire)
    type_menage           TEXT NOT NULL,             -- INTERNE|EXTERNE
    fournisseur_id_opaque TEXT,                      -- prestataire affecté (référentiel Fournisseurs)
    duree_prevue_h        REAL,
    duree_reelle_h        REAL,
    cout_prevu            REAL,
    cout_reel             REAL,
    methode_cout          TEXT,                      -- INTERNE_HEURES_M04|INTERNE_STANDARD_PARAMETRE|EXTERNE_FACTURE (D101/D103)
    ecart_justification    TEXT,                      -- obligatoire si cout_reel diffère de cout_prevu au-delà d'un seuil
    facture_id_opaque     TEXT,                      -- lien vers factures (0017) — jamais une seconde facture créée ici
    charge_id             TEXT,                      -- lien vers la charge économique — jamais recréée ici
    statut                TEXT NOT NULL DEFAULT 'PREVU',
        -- PREVU|A_AFFECTER|A_REALISER|REALISE|A_CONTROLER|VALIDE|FACTURE|REGLE|ANNULE|LITIGE
    source                TEXT NOT NULL DEFAULT 'SAISIE',  -- SAISIE|HOSTAWAY|IMPORT
    commentaire           TEXT,
    empreinte             TEXT,                      -- anti-doublon (logement+mois+type+prestataire+date)
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_menages_mois_logement ON menages(mois, logement_id);
CREATE INDEX IF NOT EXISTS idx_menages_statut ON menages(statut);
CREATE INDEX IF NOT EXISTS idx_menages_fournisseur ON menages(fournisseur_id_opaque);
-- Doublon CERTAIN : même logement, même mois, même type, même prestataire, même date prévue.
CREATE UNIQUE INDEX IF NOT EXISTS idx_menages_empreinte
    ON menages(empreinte) WHERE statut <> 'ANNULE' AND empreinte IS NOT NULL;
-- Une charge n'est jamais rattachée qu'à un seul ménage (même règle que factures 0017).
CREATE UNIQUE INDEX IF NOT EXISTS idx_menages_charge
    ON menages(charge_id) WHERE charge_id IS NOT NULL AND statut <> 'ANNULE';

-- Historique append-only des transitions et affectations — jamais de réécriture silencieuse.
CREATE TABLE IF NOT EXISTS menage_evenements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    menage_id_opaque  TEXT NOT NULL,
    type_evenement    TEXT NOT NULL,
        -- CREATION|AFFECTATION|CHANGEMENT_PRESTATAIRE|REALISATION|VALIDATION|FACTURATION|
        -- REGLEMENT|ANNULATION|REMPLACEMENT|LITIGE|REOUVERTURE
    ancien_statut     TEXT,
    nouveau_statut    TEXT,
    ancien_prestataire TEXT,                          -- conserve l'historique lors d'un changement
    nouveau_prestataire TEXT,
    commentaire       TEXT,
    date_evenement    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur            TEXT
);
CREATE INDEX IF NOT EXISTS idx_menage_evenements_menage ON menage_evenements(menage_id_opaque);

-- ── Qualification ménage du référentiel Fournisseurs ────────────────────────
-- Extension, PAS un référentiel concurrent (0010 reste la table fournisseurs). Table séparée : les
-- migrations sont rejouées à chaque démarrage et SQLite n'a pas d'ADD COLUMN IF NOT EXISTS.
-- `fournisseur_details.prestataire_menage` (0017) est un simple booléen jamais lu par le code ;
-- cette table le remplace pour l'usage réel, sans le supprimer (compat lecture).
CREATE TABLE IF NOT EXISTS fournisseur_menage_qualification (
    fournisseur_id_opaque TEXT PRIMARY KEY,
    type_menage           TEXT NOT NULL,             -- INTERNE|EXTERNE
    date_debut_validite   TEXT,
    date_fin_validite     TEXT,
    tarif_horaire         REAL,                      -- pertinent si INTERNE, méthode heures (D101)
    logements_autorises   TEXT,                      -- CSV de logement_id, NULL = tous autorisés
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0019');

-- Migration 0027 — Factures propriétaires ÉMISES par la conciergerie.
--
-- Objet distinct des factures fournisseurs (0017/0022), qui sont des pièces REÇUES : elles ont un
-- fournisseur (`fournisseur_id_opaque NOT NULL`), une référence portée par le document du
-- fournisseur, et chaque ligne pointe une charge (`facture_lignes.charge_id NOT NULL`). Une facture
-- propriétaire n'a rien de tout cela : elle est créée par l'application, sa référence est un numéro
-- que NOUS attribuons, et ses lignes sont des prestations (commission, ménage, canapé, charge fixe,
-- refacturation) et non des charges fournisseur. Réutiliser `factures` aurait imposé un fournisseur
-- fictif et une charge fictive par ligne — table séparée, comme 0017 et 0020 l'ont déjà fait pour
-- des raisons analogues.
--
-- Ce n'est pas non plus le RELEVÉ propriétaire : le relevé (Lot 12, 12/13 lignes) explique le revenu
-- du propriétaire et son solde ; la facture ne porte QUE ce que la société facture réellement. Le
-- total de la facture réconcilie `montant_du_conciergerie` (Lot 10), rien d'autre.
--
-- Une facture ÉMISE est immutable : son contenu est figé dans `snapshot_json` à l'émission, et le
-- document PDF est identifié par son hash. Ni un recalcul Lot 10, ni un changement de taux, ni une
-- correction de charge ne peuvent la modifier après coup. Une correction passe par un AVOIR lié.

CREATE TABLE IF NOT EXISTS factures_proprietaires (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque     TEXT NOT NULL UNIQUE,      -- FPR-xxxx (interne, stable dès le brouillon)
    numero_facture        TEXT,                      -- attribué à l'émission, jamais réutilisé
    type_document         TEXT NOT NULL DEFAULT 'FACTURE',
        -- FACTURE|AVOIR
    facture_origine       TEXT,                      -- si AVOIR : facture_id_opaque corrigée
    proprietaire_id       TEXT NOT NULL,             -- id métier (référentiel Excel, non FK SQL)
    logement_id           TEXT NOT NULL,             -- grain actuel : mois x proprietaire x logement
    mois                  TEXT NOT NULL,             -- AAAA-MM
    devise                TEXT NOT NULL DEFAULT 'EUR',
    montant_total         REAL NOT NULL DEFAULT 0,   -- somme des lignes, jamais saisi a la main
    statut                TEXT NOT NULL DEFAULT 'BROUILLON',
        -- BROUILLON|VALIDE|EMIS|ANNULE
    source_calcul         TEXT,                      -- reference du calcul source (ex. id prefacture Lot12)
    snapshot_json         TEXT,                      -- fige a l'emission : contenu integral reproductible
    snapshot_hash         TEXT,                      -- sha256 du snapshot
    document_nom          TEXT,                      -- nom de fichier seul, jamais un chemin absolu
    document_hash         TEXT,                      -- sha256 du PDF emis
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_facture          TEXT,
    date_validation       TEXT,
    date_emission         TEXT,
    date_annulation       TEXT,
    motif_annulation      TEXT,
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);

-- Anti-doublon metier : un seul document vivant par (mois, proprietaire, logement, type).
-- Les ANNULE sortent de la contrainte : l'historique conserve tout, mais ne bloque pas un remplacant.
CREATE UNIQUE INDEX IF NOT EXISTS idx_fpr_grain
    ON factures_proprietaires(mois, proprietaire_id, logement_id, type_document)
    WHERE statut <> 'ANNULE';
-- Un numero emis n'est jamais reutilise, meme apres annulation : pas de clause WHERE ici.
CREATE UNIQUE INDEX IF NOT EXISTS idx_fpr_numero
    ON factures_proprietaires(numero_facture) WHERE numero_facture IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fpr_statut ON factures_proprietaires(statut);
CREATE INDEX IF NOT EXISTS idx_fpr_proprietaire ON factures_proprietaires(proprietaire_id, mois);

CREATE TABLE IF NOT EXISTS factures_proprietaires_lignes (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque       TEXT NOT NULL UNIQUE,      -- FPRL-xxxx
    facture_id_opaque     TEXT NOT NULL,
    numero_ligne          INTEGER NOT NULL,
    type_ligne            TEXT NOT NULL,
        -- COMMISSION_CONCIERGERIE|MENAGE_FACTURE|PREPARATION_CANAPE|CHARGE_FIXE|CHARGES_EXCEPT_REFAC
    libelle               TEXT NOT NULL,
    montant               REAL NOT NULL,
    objet_source_type     TEXT,                      -- ex. LOT10_NET_PROPRIETAIRE
    objet_source_ref      TEXT,                      -- reference de l'objet de calcul d'origine
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_fprl_facture ON factures_proprietaires_lignes(facture_id_opaque);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprl_ordre
    ON factures_proprietaires_lignes(facture_id_opaque, numero_ligne);

-- Journal append-only : aucune suppression, aucune mise a jour.
CREATE TABLE IF NOT EXISTS factures_proprietaires_evenements (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque     TEXT NOT NULL,
    type_evenement        TEXT NOT NULL,
        -- CREATION|REGENERATION|VALIDATION|EMISSION|ANNULATION|AVOIR_CREE
    ancien_statut         TEXT,
    nouveau_statut        TEXT,
    commentaire           TEXT,
    date_evenement        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT
);
CREATE INDEX IF NOT EXISTS idx_fpr_evt ON factures_proprietaires_evenements(facture_id_opaque);

-- Sequence de numerotation, une ligne par serie. Le compteur est incremente sous transaction
-- immediate (voir service) : deux emissions concurrentes ne peuvent pas obtenir le meme numero.
CREATE TABLE IF NOT EXISTS factures_proprietaires_sequence (
    serie                 TEXT PRIMARY KEY,          -- ex. FPR-2026 ou RECETTE-2026
    dernier_numero        INTEGER NOT NULL DEFAULT 0,
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0027');

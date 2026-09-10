-- Migration 0072 — Facture propriétaire : EXTRA, RÉDUCTION, et lien vers l'écriture comptable.
--
-- ADDITIVE. Aucune donnée supprimée, aucune valeur existante réécrite. `0071` n'est pas touchée.
--
-- ── POURQUOI DEUX TYPES DE LIGNE DE PLUS, ET PAS DAVANTAGE ─────────────────────────────────────
-- Le contrat de `type_ligne` (0055, resserré depuis 0027) énumérait les composants de
-- `montant_du_conciergerie` (Lot10) plus `CHARGE_REFACTUREE`. Deux besoins métier n'y entraient
-- dans AUCUN type existant, et les forcer dans un type voisin aurait rendu la facture illisible :
--
--   EXTRA      prestation ponctuelle facturée au propriétaire qui n'est ni une commission, ni un
--              ménage, ni un forfait, ni la refacturation d'une charge déjà engagée. Montant
--              POSITIF : il augmente ce qui est dû.
--   REDUCTION  remise commerciale consentie sur cette facture. Montant NÉGATIF : il diminue ce qui
--              est facturé. Ce n'est PAS un paiement reçu.
--
-- ── CE QUI N'EST DÉLIBÉRÉMENT PAS AJOUTÉ ICI : L'ACOMPTE ───────────────────────────────────────
-- Un acompte est un PAIEMENT DÉJÀ REÇU, pas une ligne de facture. Le projet le modélise déjà comme
-- un objet à part entière : `mouvements_tresorerie_proprietaires` (0025), rattaché à la facture par
-- `reference_metier`, créé ET validé par `factures_proprietaires_edition_service.ajouter_acompte`.
-- Lui donner EN PLUS une ligne de facture le compterait deux fois : une fois dans le total facturé,
-- une fois dans le solde de règlement. `factures_proprietaires_service.TYPES_NON_FACTURABLES` liste
-- d'ailleurs `ACOMPTES_PROPRIETAIRES`/`PAIEMENT_DEJA_RECU` comme non facturables depuis l'origine.
-- L'acompte reste donc HORS `montant_total` et n'apparaît qu'en DÉDUCTION du montant dû, calculé.
--
-- Conséquence, et c'est la distinction que cette migration matérialise :
--     montant_total  = commissions + ménages + canapé + forfait + refacturations + extras - réductions
--     montant dû     = montant_total - acomptes déjà versés          (dérivé, jamais stocké)
-- RÉDUCTION et ACOMPTE ne sont donc jamais traités de la même façon : l'un est une ligne de la
-- facture, l'autre un encaissement antérieur.
--
-- ── LIEN FACTURE ↔ ÉCRITURE COMPTABLE ─────────────────────────────────────────────────────────
-- AUCUNE table de liaison n'est créée : `ecritures` (0021) porte déjà `origine_type` +
-- `origine_id_opaque`. Une écriture de vente issue d'une facture propriétaire s'identifie donc par
-- `origine_type = 'FACTURE_PROPRIETAIRE'` et `origine_id_opaque = <facture_id_opaque>`. C'est ce
-- couple qui rend la comptabilisation IDEMPOTENTE : émettre deux fois ne peut pas créer deux
-- écritures, puisque la seconde tentative retrouve la première. L'index ci-dessous rend cette
-- recherche immédiate et sert de garde-fou de performance, pas de contrainte d'unicité — une
-- contrepassation légitime (`contrepasse_de`) partage la même origine.

PRAGMA foreign_keys=OFF;

-- ── 1. type_ligne : ouverture du domaine à EXTRA et REDUCTION ──────────────────────────────────
-- SQLite ne sait pas modifier un CHECK : reconstruction, exactement l'idiome de 0055 §4 et 0060.
-- Rejouable : à ce point de la migration la table source existe toujours, la reconstruction est
-- donc reproductible à l'identique (vérifié par un second passage).

CREATE TABLE factures_proprietaires_lignes_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque       TEXT NOT NULL UNIQUE,
    facture_id_opaque     TEXT NOT NULL,
    numero_ligne          INTEGER NOT NULL,
    type_ligne            TEXT NOT NULL CHECK (type_ligne IN (
        'COMMISSION_CONCIERGERIE', 'MENAGE_FACTURE', 'PREPARATION_CANAPE',
        'CHARGE_FIXE', 'CHARGES_EXCEPT_REFAC', 'CHARGE_REFACTUREE',
        'EXTRA', 'REDUCTION')),
    libelle               TEXT NOT NULL,
    montant               REAL NOT NULL,
    objet_source_type     TEXT,
    objet_source_ref      TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (facture_id_opaque) REFERENCES factures_proprietaires(facture_id_opaque)
);
INSERT INTO factures_proprietaires_lignes_new (
    id, ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant,
    objet_source_type, objet_source_ref, date_creation)
SELECT id, ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant,
       objet_source_type, objet_source_ref, date_creation
FROM factures_proprietaires_lignes;
DROP TABLE factures_proprietaires_lignes;
ALTER TABLE factures_proprietaires_lignes_new RENAME TO factures_proprietaires_lignes;

CREATE INDEX IF NOT EXISTS idx_fprl_facture ON factures_proprietaires_lignes(facture_id_opaque);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprl_ordre
    ON factures_proprietaires_lignes(facture_id_opaque, numero_ligne);

-- ── 2. Recherche immédiate de l'écriture comptable d'une facture ───────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ecritures_origine ON ecritures(origine_type, origine_id_opaque);

-- ── 3. Charge refacturable RATTACHÉE (et non recréée) ──────────────────────────────────────────
-- `factures_proprietaires_lignes_charge` (0071) enregistrait le lien d'une charge CRÉÉE depuis le
-- brouillon. Le même lien sert maintenant aussi à RATTACHER une charge qui existe déjà : c'est le
-- lien qui porte l'anti-doublon, indépendamment de qui a créé la charge. Un index unique sur
-- `charge_id` interdit STRUCTURELLEMENT qu'une même charge soit portée par deux lignes de facture
-- — la garde applicative (`charge_deja_facturee`) devient une vérification confortable, pas la
-- seule protection.
--
-- Aucune donnée existante ne viole cette contrainte : le lien était écrit une seule fois par charge
-- créée depuis un brouillon (une charge neuve ne peut pas déjà être liée ailleurs).
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprlc_charge_unique
    ON factures_proprietaires_lignes_charge(charge_id);

PRAGMA foreign_keys=ON;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0072');

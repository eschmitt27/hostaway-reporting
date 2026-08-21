-- Migration 0051 — Référentiel administrable depuis l'application.
--
-- Additive comme 0017→0050. Aucun ALTER TABLE : les tables `ref_*` de 0029 sont réutilisées telles
-- quelles, on ajoute seulement un journal.
--
-- POURQUOI CETTE MIGRATION
-- Jusqu'ici le CRUD logements écrivait dans `REF_Setup.xlsm` (`keep_vba=True`). Le référentiel
-- SQLite (0029) n'était donc qu'une COPIE de lecture : la vérité restait dans le classeur. Pour que
-- SQLite devienne canonique — condition pour que le classeur ne soit plus qu'un import initial —
-- l'application doit écrire en base, et ces écritures doivent laisser une trace.
--
-- LES LIGNES SAISIES DANS L'APPLICATION NE SONT PAS DES LIGNES IMPORTÉES
-- `ref_setup_import_service` remplace intégralement le contenu importé (`DELETE FROM` puis
-- réinsertion). Une ligne créée ou modifiée dans l'application porte donc
-- `import_id = 'SAISIE_APPLICATION'`, et l'import préserve ces lignes-là en signalant tout conflit
-- de clé. Sans cela, réimporter le classeur effacerait silencieusement tout ce qui a été saisi
-- depuis — et SQLite ne serait pas canonique.

-- ── Journal des modifications du référentiel ────────────────────────────────────────────────────
--
-- Append-only, comme `cloture_evenements` (0008) ou `proprietaire_recalculs` (0030). Un référentiel
-- administrable sans trace ne permet pas de répondre à « qui a changé ce taux, et quand ». Les
-- valeurs avant/après sont stockées en JSON : le journal ne doit pas avoir à connaître le schéma
-- de chaque table qu'il trace.
CREATE TABLE IF NOT EXISTS ref_admin_evenements (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    table_cible   TEXT NOT NULL,      -- ref_logements, ref_gestion_logements_hist, ...
    cle           TEXT NOT NULL,      -- valeur de la clé métier concernée
    action        TEXT NOT NULL,      -- CREATION|MODIFICATION|ARCHIVAGE|REACTIVATION|
                                      -- CHANGEMENT_PROPRIETAIRE|CHANGEMENT_TAUX|CLOTURE_PERIODE|
                                      -- ACTIVATION|DESACTIVATION
    avant_json    TEXT,
    apres_json    TEXT,
    acteur        TEXT,
    commentaire   TEXT,
    horodatage    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_ref_admin_evt_cible
    ON ref_admin_evenements(table_cible, cle, id);

-- ── Pourquoi PAS d'index unique sur « une seule période ouverte » ───────────────────────────────
--
-- Un `CREATE UNIQUE INDEX ... WHERE date_fin IS NULL OR date_fin = ''` sur
-- `ref_gestion_logements_hist` a été écrit, testé, puis RETIRÉ délibérément.
--
-- Il tenait sur les données réelles (aucun logement n'a deux périodes ouvertes aujourd'hui) et il
-- empêchait bien l'application de créer l'ambiguïté. Mais il avait deux effets non voulus :
--
--   1. Un référentiel ambigu venu de l'EXTÉRIEUR (classeur importé, restauration, correction
--      manuelle en base) devenait irreprésentable : l'import échouait sur une `IntegrityError`
--      brute, au lieu d'être chargé puis SIGNALÉ.
--   2. Il court-circuitait une garde de lecture déjà en place : `logements_service` détecte
--      justement ce cas et rend `gestion_statut = A_CONTROLER` sans jamais choisir un propriétaire
--      au hasard. Rendre l'état impossible en base rendait cette garde intestable, donc fragile.
--
-- L'invariant est donc défendu là où il doit l'être : par le SERVICE, qui clôt toujours la période
-- courante avant d'en ouvrir une nouvelle et refuse explicitement une seconde période ouverte
-- (`referentiel_admin_service`). Une ambiguïté introduite hors de l'application reste visible et
-- signalée, plutôt que masquée par un refus d'écriture opaque.

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0051');

-- Migration 0066 — Supplément/justification sur les déclarations internes, audit trail, conflits
-- Google Sheet / Application (mission Ménages « FINALISER LE VRAI WORKFLOW »).
--
-- Additive, sans ALTER TABLE (même convention que 0038→0065) : `menages_declarations_internes`
-- (0038) est déjà en usage réel (lot6b + saisie UI), on ajoute une table compagnon plutôt que des
-- colonnes après coup, exactement le motif documenté par 0039/0040.
--
-- POURQUOI PAS UN ALTER TABLE
-- `menages_declarations_internes` est régénérée par un DELETE + INSERT complet à chaque run lot6b
-- (0038, commentaire "chaque table ci-dessous est un calcul dérivé, intégralement remplacé à chaque
-- run"). Un ALTER TABLE ajouterait des colonnes qu'un DELETE+INSERT ferait disparaître à la valeur
-- NULL par défaut au run suivant si lot6b ne les alimente pas explicitement. La table compagnon
-- `menages_declarations_extra` porte les champs propres à l'APPLICATION (supplément, justification,
-- source, coûts détaillés) sur une clé stable (mois, logement_id, intervenant_id) qui survit à la
-- régénération de la table calculée, tant que lot6b conserve le même grain.

CREATE TABLE IF NOT EXISTS menages_declarations_extra (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                   TEXT NOT NULL,
    logement_id            TEXT NOT NULL,
    intervenant_id         TEXT NOT NULL,
    supplement             REAL NOT NULL DEFAULT 0,
    justification_supplement TEXT,
    cout_standard_calcule  REAL,
    cout_final             REAL,
    source                 TEXT NOT NULL DEFAULT 'APPLICATION',  -- GOOGLE_SHEET|APPLICATION
    derniere_valeur_sheet_nb_menages INTEGER,
    derniere_synchro_sheet TEXT,
    derniere_modification_app TEXT,
    date_creation          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mde_cle
    ON menages_declarations_extra(mois, logement_id, intervenant_id);

-- Audit : une ligne par changement de champ, jamais réécrite (§A1 : "jamais silencieusement
-- écraser l'historique"). Grain = un champ modifié, pas une ligne par sauvegarde de formulaire :
-- une modification qui touche nb_menages ET supplément produit deux lignes distinctes, chacune
-- comparable indépendamment.
CREATE TABLE IF NOT EXISTS menages_declarations_historique (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    mois              TEXT NOT NULL,
    logement_id       TEXT NOT NULL,
    intervenant_id    TEXT NOT NULL,
    champ             TEXT NOT NULL,
    ancienne_valeur   TEXT,
    nouvelle_valeur   TEXT,
    justification     TEXT,
    auteur            TEXT NOT NULL DEFAULT '',
    source             TEXT NOT NULL DEFAULT 'APPLICATION',
    date_heure        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mdh_cle ON menages_declarations_historique(mois, logement_id, intervenant_id);

-- Conflit détecté au réimport Google Sheet : la valeur Sheet et la valeur SQLite actuelle
-- divergent ET une modification APPLICATION a eu lieu depuis la dernière synchro. Jamais fusionné
-- silencieusement (§B2) : la ligne reste OUVERTE tant qu'un humain n'a pas choisi.
CREATE TABLE IF NOT EXISTS menages_declarations_conflits (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    mois               TEXT NOT NULL,
    logement_id        TEXT NOT NULL,
    intervenant_id     TEXT NOT NULL,
    champ              TEXT NOT NULL DEFAULT 'nb_menages',
    valeur_application TEXT,
    valeur_sheet       TEXT,
    statut             TEXT NOT NULL DEFAULT 'OUVERT',   -- OUVERT|RESOLU_GARDE_APPLICATION|RESOLU_REPRIS_SHEET
    resolu_par         TEXT,
    resolu_le          TEXT,
    date_detection     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mdc_cle ON menages_declarations_conflits(mois, logement_id, intervenant_id);
CREATE INDEX IF NOT EXISTS idx_mdc_statut ON menages_declarations_conflits(statut);

-- Dernier hash connu par fichier PDF surveillé (`menages_pdf_import_service`) : permet de
-- distinguer un PDF « déjà traité, inchangé » d'un PDF « même nom, contenu remplacé » (§E3) sans
-- confondre les deux avec un sha256 de fichier générique (déjà écarté ailleurs, cf. commentaire
-- `_deja_traite` — ici la question posée est différente : pas « ce fichier a-t-il déjà produit une
-- facture ? » mais « ce fichier a-t-il changé depuis la dernière fois ? »).
CREATE TABLE IF NOT EXISTS menages_pdf_fichiers_hash (
    nom_fichier TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    date_maj    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0066');

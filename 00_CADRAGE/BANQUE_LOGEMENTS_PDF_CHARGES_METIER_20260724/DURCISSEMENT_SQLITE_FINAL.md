# Dernier durcissement SQLite ciblé — Mission 12 (2026-08-27)

## Leçon de la migration 0056, appliquée dès l'audit

`banque_mouvements.sens` avait été fermé par CHECK en 0055, puis rouvert en 0056 : les contrôles
(`banques_controles_catalogue_service.py`) doivent pouvoir voir un `sens` hors domaine importé
d'une source externe corrompue. Avant toute nouvelle contrainte, la question posée pour CHAQUE
candidat a été : *cette table est-elle alimentée par un import brut externe, ou entièrement générée
par l'application ?* Les 4 CHECK retenus (§ci-dessous) ne portent que sur des tables du second type.

## Matrice d'audit

| Table | Risque actuel | FK utile ? | UNIQUE utile ? | CHECK utile ? | NOT NULL utile ? | Index utile ? | Transaction ? | Verdict |
|---|---|---|---|---|---|---|---|---|
| `charges` | statut non fermé (ACTIVE/ANNULEE, exhaustif en code) | NON (décision historique 0025/0027/0052, confirmée) | déjà (`charge_id` UNIQUE) | OUI | NON (`logement_id` doit rester nullable — charge commune) | déjà (mois/logement/statut/proprietaire) | déjà (1 INSERT + 1 journal, atomique par construction sqlite3) | **C — CHECK(statut) ajouté** |
| `reservations_hors_hostaway` | statut non fermé (exhaustif en code) | NON (même décision historique) | déjà | OUI | NON (`montant_retenu` doit rester nullable — placeholder) | déjà | déjà | **C — CHECK(statut) ajouté** |
| `banque_mouvements` | déjà durci (0055/0056) | déjà (banque_classifications→ici) | déjà | déjà (sens retiré à raison) | déjà | déjà | déjà | **A — rien à faire, non-régression vérifiée** |
| `mouvements_tresorerie_proprietaires` | sens/nature/statut non fermés (exhaustifs en code, table 100% app-générée) | NON (proprietaire_id référentiel Excel, décision historique) | déjà (`mouvement_opaque` UNIQUE) | OUI ×3 | NON (`logement_id` facultatif, voulu) | déjà | déjà (1 INSERT, atomique) | **C — CHECK(sens), CHECK(nature), CHECK(statut) ajoutés** |
| `factures_proprietaires` / lignes | déjà durci (0055) | déjà | déjà | déjà | déjà | déjà | déjà | **A — rien à faire** |
| `factures` (fournisseurs) | statut non fermé (7 valeurs, `TRANSITIONS` exhaustif en code) | NON (fournisseur_id_opaque/charge_id, décisions historiques 0017) | déjà (fournisseur+ref, charge_id) | OUI | déjà (montant_ttc NOT NULL) | déjà | déjà | **C — CHECK(statut) ajouté** |
| `facture_lignes` | non auditée en détail (hors périmètre prioritaire, pas de signal de risque trouvé) | — | — | — | — | — | — | **B — pas de signal, non traité** |
| référentiels propriétaires/logements | non modifiés (référentiel Excel historique, décision figée) | NON | — | — | — | déjà | — | **A — inchangé** |
| règles temporelles (`ref_regles_versions`, `ref_canape_parametres`, `ref_couts_standards_menage`) | résolveurs chargent la table entière en mémoire (Python), pas de requête SQL filtrée à indexer | — | — | déjà couverts par les contrats/garde-fous applicatifs (Missions 6-9) | — | AUCUN bénéfice démontré (pas de scan SQL à accélérer) | — | **B — aucun index utile, résolution faite en Python** |
| ménages (`menages`, `menages_cout_complet`) | aucune incohérence structurelle évidente trouvée lors de l'audit | — | — | — | — | — | — | **A — rien à faire** |

## Contraintes ajoutées (migration 0060)

- `charges.statut` : `CHECK (statut IN ('ACTIVE', 'ANNULEE'))`.
- `reservations_hors_hostaway.statut` : `CHECK (statut IN ('ACTIVE', 'ANNULEE'))`.
- `mouvements_tresorerie_proprietaires.sens` : `CHECK (sens IN ('PROPRIETAIRE_VERS_SOCIETE', 'SOCIETE_VERS_PROPRIETAIRE'))`.
- `mouvements_tresorerie_proprietaires.nature` : `CHECK (nature IN (7 valeurs, dont le catch-all
  déclaré `AUTRE_A_CONTROLER`))`.
- `mouvements_tresorerie_proprietaires.statut` : `CHECK (statut IN ('BROUILLON', 'A_CONTROLER',
  'VALIDE', 'ANNULE'))`.
- `factures.statut` : `CHECK (statut IN (7 valeurs))`, domaine confirmé exhaustif par le dict
  `TRANSITIONS` du service (n'utilise jamais d'autre valeur).

Chaque domaine a été vérifié EXHAUSTIF par lecture directe du code (constantes `STATUT_*`/`ST_*`,
tuples `SENS`/`NATURES`/`STATUTS`/`TRANSITIONS`) avant fermeture — jamais deviné.

## Contraintes refusées

- **FK sur `charges.proprietaire_id`/`logement_id`, `reservations_hors_hostaway.proprietaire_id`/
  `logement_id`, `mouvements_tresorerie_proprietaires.proprietaire_id`** : décisions historiques
  (référentiel Excel, migrations 0025/0027/0052) maintenues — aucun élément nouveau ne les remet en
  cause (mission §8).
- **NOT NULL sur `charges.logement_id`** : casserait le cas métier valide « charge commune sans
  logement direct » (périmètre défini autrement, cf. Mission 8 `charges_impact_service`).
- **NOT NULL sur `reservations_hors_hostaway.montant_retenu`** : casserait le placeholder réel
  confirmé par Mission 11 (réservation en attente de saisie complète).
- **CHECK/UNIQUE supplémentaire sur `banque_mouvements.sens`** : répéterait exactement l'erreur de
  la migration 0056 (les contrôles doivent voir une valeur anormale).
- **Index sur les référentiels temporels** (`ref_regles_versions`, etc.) : les résolveurs
  (`resolve_regle_version`, `resolve_canape_parametres`, `resolve_fixed_internal_cost`…) chargent
  la table entière en mémoire Python (`.to_dict("records")` ou équivalent) puis filtrent en Python
  — aucune requête SQL avec `WHERE` à accélérer, donc aucun bénéfice démontré.

## FK au runtime

`app/db/connection.py::get_db()` exécute `PRAGMA foreign_keys=ON` sur CHAQUE connexion — vérifié
par test dédié (`test_pragma_foreign_keys_actif_sur_connexion_applicative`). Les FK déjà présentes
(0055 : `banque_classifications`, `factures_proprietaires_lignes`) restent actives.

## Transactions

Aucune nouvelle transaction créée — audit des écritures multi-étapes (charges + journal,
réservations HH + overrides + journal, trésorerie + événement) confirme qu'elles sont déjà
atomiques par construction : le module `sqlite3` de Python groupe implicitement toutes les
instructions entre deux `commit()` dans la même transaction ; chaque service appelle `conn.commit()`
une seule fois après toutes ses écritures, dans un bloc `try/finally`. Aucune moitié d'objet
possible avec le code existant.

## Régression trouvée et corrigée pendant la campagne (pas un bug pré-existant)

Le premier jet de la migration 0060 recréait la table `factures` (motif : ajouter `CHECK(statut)`,
SQLite exigeant une reconstruction de table) sans recréer le trigger `trg_facture_classification_
defaut` (migration 0020, `AFTER INSERT ON factures`) — SQLite supprime les triggers d'une table
droppée, ils ne survivent jamais à un `DROP TABLE`/`RENAME TO`. La campagne complète a immédiatement
révélé la régression (`tests/test_facture_classification.py`, 3 échecs : une facture créée après
la migration ne recevait plus sa classification par défaut). Corrigé en recréant le trigger à
l'identique juste après le renommage de la table, dans la même migration — jamais commité en l'état
cassé.

## Tests sur copie réelle

`app.db` réelle copiée (jamais modifiée), migrations 0016→HEAD appliquées sur la copie :
`integrity_check` OK, `foreign_key_check` vide, aucune ligne perdue/modifiée sur les tables
existantes (seuls `schema_migrations`/`sqlite_sequence` grossissent, comme attendu). Rejoué une
seconde fois (replay ×2) sur la même copie : identique, aucune collision.

Les 4 tables durcies (`charges`, `reservations_hors_hostaway`, `mouvements_tresorerie_
proprietaires`, `factures`) n'existent PAS encore dans la vraie `app.db` (toujours en migration
0016) — aucune donnée réelle à auditer pour ces tables spécifiquement ; le domaine exhaustif de
chaque CHECK a été validé par lecture du code applicatif (seule source de vérité disponible).

## Tests

Nouveaux : 13 (`tests/test_durcissement_sqlite_final.py`) — 4 CHECK négatifs, 3 cas « toutes
valeurs valides acceptées », 2 stop-gates explicites (§32/§33 : réservation sans montant,
charge sans logement, toujours valides), 1 stop-gate banque (§31 : `sens` hors domaine toujours
accepté, non-régression 0056), 1 PRAGMA foreign_keys. Régression ciblée : 141 tests existants
(contraintes 0055, contrats Mission 11, charges, réservations HH, trésorerie, factures, banque)
tous verts. Campagne complète : moteur **397 passed / 0 failed** (inchangé, mission hors
`02_TRAVAIL`), application **2791 passed / 0 failed** (10 lots, 191 fichiers).

## Migration

`0060_durcissement_sqlite_final.sql` — additive, idempotente (comportement standard du projet),
aucune migration passée modifiée, aucune donnée économique réécrite. Vraie app.db (0016) jamais
migrée.

## Limites

- `facture_lignes`/`facture_ligne_charges` non auditées en détail (hors périmètre prioritaire de
  la mission, aucun signal de risque identifié en passant).
- Les 4 tables durcies n'ont pas encore de données réelles à auditer (app.db réelle toujours en
  0016) — la preuve d'exhaustivité repose sur la lecture du code, seule source disponible.

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer la recette navigateur sans
validation du rapport Mission 12.

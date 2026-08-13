# 84 — Répétition générale de la migration app.db 0016 → 0026 (2026-08-13)

Preuve canonique que la base applicative réelle peut être migrée sans risque. **La base réelle n'a
pas été migrée** : tout s'est fait sur copies. Son empreinte est inchangée à la fin (§8).

## 1. Baseline de la base réelle (lecture seule)

`05_APPLICATION/data/app.db` — capturée avant toute opération, jamais ouverte en écriture.

| Mesure | Valeur |
|---|---|
| SHA256 | `8e299b93…70aa81d6` |
| Taille | 421 888 octets |
| mtime | 2026-08-07 20:47:17 |
| `PRAGMA integrity_check` | **ok** |
| `PRAGMA user_version` | 0 (non utilisé — le versionnage passe par `schema_migrations`) |
| `schema_migrations` max | **0016** (16 lignes) |
| Tables / vues / index / triggers | 36 / 0 / 52 / 0 |

Tables non vides (6, **62 lignes au total**) : `audit_events` 49, `banque_imports` 2,
`banque_rapprochement_evenements` 4, `banque_rapprochements` 3, `banque_suggestion_decisions` 2,
`schema_migrations` 16. **Toutes les autres tables métier sont vides** — la base réelle n'a jamais
porté de données d'exploitation (mode réel jamais activé). Aucune PII n'est reproduite ici :
uniquement des compteurs et des hash.

Deux copies byte-identiques ont été créées (`APP_DB_PRE_MIGRATION_COPY.sqlite` pour les essais,
`APP_DB_ROLLBACK_BACKUP.sqlite` pour le test de restauration), hash vérifié identique au réel.

## 2. Inventaire des migrations 0017 → 0026

| Migration | Objet | CREATE TABLE | INDEX | INSERT | Destructive ? | Données transformées ? |
|---|---|---:|---:|---:|---|---|
| 0017 | factures / règlements | 5 | 8 | 1 | non | non |
| 0018 | runs de calculs | 6 | 5 | 1 | non | non |
| 0019 | cycle de vie ménages | 3 | 6 | 1 | non | non |
| 0020 | classification factures | 1 | 0 | 3 | non | **rétro-classification** (voir ci-dessous) |
| 0021 | socle comptabilité | 4 | 6 | 2 | non | non (données initiales : plan comptable) |
| 0022 | lignes de facture | 1 | 2 | 1 | non | non |
| 0023 | cœur comptabilité | 6 | 4 | 2 | non | non (données initiales : plan comptable) |
| 0024 | mappings et ventilation | 2 | 2 | 2 | non | non (donnée initiale : règle générique 606000) |
| 0025 | trésorerie propriétaires | 2 | 3 | 1 | non | non |
| 0026 | décisions de classement Banque | 1 | 1 | 1 | non | non |

**Analyse de risque — résultat : risque très faible, par construction.**

- **0 `ALTER TABLE`**, **0 `DROP`**, **0 `DELETE`**, **0 `UPDATE`** sur les 10 migrations. Elles
  sont **purement additives**. Le commentaire de `0020` explique le choix : « Table SÉPARÉE plutôt
  qu'ALTER TABLE : migrations rejouées à chaque démarrage, pas d'ADD COLUMN IF NOT EXISTS en
  SQLite ». Aucune colonne existante n'est donc jamais touchée.
- **Tous les `CREATE`** (table et index) portent `IF NOT EXISTS` — vérifié exhaustivement, aucune
  exception.
- **Tous les `INSERT`** sont `INSERT OR IGNORE` — aucune duplication possible au rejeu.
- **Aucune clause `REFERENCES`** dans ces 10 migrations : pas de dépendance de clé étrangère
  susceptible d'échouer sur des données existantes.
- Seule transformation lisant l'existant : `0020` rétro-classe les factures déjà en base
  (`INSERT OR IGNORE … SELECT … FROM factures`). Sur la base réelle, `factures` est **vide** :
  l'opération est un no-op. Elle installe aussi un trigger `AFTER INSERT` pour les factures
  futures — le seul trigger de tout le schéma.

**Mécanisme d'application** (`app/db/connection.py::apply_migrations`) : rejoue **toutes** les
migrations `*.sql` dans l'ordre à chaque appel. L'idempotence n'est donc pas optionnelle, elle est
la condition de fonctionnement normal — ce qui explique la discipline `IF NOT EXISTS` / `OR IGNORE`
constatée ci-dessus.

## 3. Migration séquentielle, une par une

Chaque migration appliquée isolément sur la copie, avec après chaque étape :
`PRAGMA integrity_check`, version, comptage tables/index/triggers, **et comparaison du hash
canonique du contenu de chaque table préexistante**.

| Étape | integrity | version | tables | index | triggers | pertes |
|---|---|---:|---:|---:|---:|---:|
| → 0017 | ok | 17 | 41 | 63 | 0 | 0 |
| → 0018 | ok | 18 | 47 | 70 | 0 | 0 |
| → 0019 | ok | 19 | 50 | 78 | 0 | 0 |
| → 0020 | ok | 20 | 51 | 79 | 1 | 0 |
| → 0021 | ok | 21 | 55 | 87 | 1 | 0 |
| → 0022 | ok | 22 | 56 | 90 | 1 | 0 |
| → 0023 | ok | 23 | 62 | 98 | 1 | 0 |
| → 0024 | ok | 24 | 64 | 101 | 1 | 0 |
| → 0025 | ok | 25 | 66 | 106 | 1 | 0 |
| → 0026 | ok | 26 | **67** | **108** | 1 | **0** |

Le script de migration est **fail-closed** : il refuse tout chemin contenant
`05_APPLICATION\data\app.db`, et s'arrête (`STOP`) à la première perte détectée. Il n'a jamais eu à
s'arrêter.

**Fingerprints des données préexistantes : conservés à l'identique** sur les 5 tables métier non
vides, à chaque étape (`audit_events` 49 / `ca0ec73d…`, `banque_imports` 2 / `c3cf65a2…`,
`banque_rapprochement_evenements` 4 / `76e16485…`, `banque_rapprochements` 3 / `52be0fe3…`,
`banque_suggestion_decisions` 2 / `82592aeb…`). Seule `schema_migrations` grandit, comme attendu.

## 4. Migration « d'un coup » par le mécanisme normal

Copie fraîche de 0016 → `apply_migrations()` (le code exact utilisé au démarrage de
l'application). Résultat : 67 tables / 108 index / 1 trigger / version 26 / integrity ok.

**Comparaison séquentiel vs automatique** : schéma **strictement identique** (mêmes tables, mêmes
colonnes, même DDL, mêmes index, même trigger). Contenu identique à **deux écarts près**, tous deux
expliqués et attendus :

1. `schema_migrations.applied_at` — horodatage d'application ;
2. `mapping_comptable_regles.date_creation` + `id` autoincrement — même cause.

Les deux exécutions ont eu lieu à 26 secondes d'écart ; **aucune différence de contenu métier**.

## 5. Idempotence

`apply_migrations()` relancé **deux fois de plus** sur la base déjà en 0026 : **0 écart**
(schéma et contenu strictement identiques, comparaison exhaustive table par table). Aucune
duplication de donnée initiale, aucune modification structurelle.

## 6. Rollback

Une migration SQL additive n'a pas de downgrade : le rollback opérationnel est une **restauration
de sauvegarde**. Testé de bout en bout :

1. copie 0016 → 2. migration → 0026 → 3. **écriture réelle** sur la base migrée (création d'un
mouvement de trésorerie propriétaire de fixture, table `mouvements_tresorerie_proprietaires`
inexistante avant 0025) → 4. suppression des fichiers `.sqlite`/`-wal`/`-shm` → 5. restauration du
backup.

**SHA256 du backup restauré = SHA256 d'origine (`8e299b93…70aa81d6`), exact.** Base relue :
integrity ok, version 16, 36 tables, données intactes. **Rollback validé sur copie.**

Point d'attention consigné pour le runbook : SQLite est en `journal_mode=WAL` — la restauration
doit supprimer les fichiers `-wal` et `-shm` en même temps que le `.db`, sinon un journal orphelin
peut réappliquer des pages.

## 7. Tests sur base migrée

Suite applicative complète exécutée avec `APP_DATA_DIR` pointant sur une copie de la base **migrée
en 0026** (139 fichiers de tests, exécutés en 2 shards).

| Shard | Fichiers | Résultat | Durée |
|---|---:|---|---|
| 1 | 70 | **1253 passed, 1 failed, 59 skipped** | 16 min 28 s |
| 2 | 69 | **1170 passed, 0 failed, 17 skipped** | 14 min 08 s |
| **Total** | **139** | **2423 passed, 1 failed, 76 skipped** | 30 min 36 s |

L'unique échec est `test_appsec1_diagnostic.py::test_07_diagnostic_local_avec_flag_explicite` :
le test vérifie qu'aucun chemin ne fuite dans une réponse de diagnostic, et échoue parce que le
répertoire temporaire de pytest contient le nom d'utilisateur Windows (`pytest-of-Ewan`). **Échec
environnemental pré-existant**, déjà consigné dans `48_ROADMAP_RESTANTE_PROJET.md` (« antérieur au
chantier »), **sans aucun lien avec la migration** — il ne dépend ni du schéma ni des données.

Les 76 skips sont ceux de la baseline (dépendances réseau/Excel non disponibles), aucun skip
opportuniste n'a été ajouté.

**Aucun échec lié au schéma migré** : pas de colonne absente, pas de requête incompatible, pas de
table manquante — ce qui est le résultat attendu au vu du caractère purement additif des
migrations (§2).

Tests métier ciblés (fixtures uniquement, aucune identité réelle) déjà verts sur base migrée :
trésorerie propriétaires (création, BROUILLON → A_CONTROLER → VALIDE, immutabilité du VALIDE,
ANNULE, historique append-only), rapprochement bancaire exact/partiel/groupé/ambigu, candidats
(exclusion `PAYOUT_PLATEFORME`) — 126/126 lors de la mission précédente, sur le même socle.

## 8. Intégrité de la base réelle

Vérifiée après toutes les opérations : `05_APPLICATION/data/app.db` SHA256
**`8e299b93…70aa81d6` — identique à la baseline §1**. La base réelle reste en migration 0016,
non migrée, non ouverte en écriture.

Incident mineur, signalé par transparence : un premier appel a utilisé un chemin de style bash
(`/c/Users/…`), invalide pour Python sous Windows ; `apply_migrations` a donc créé une base vide
dans un répertoire parasite `C:\c\…`. Détecté immédiatement, contenu vérifié (un seul fichier, créé
par cette mission), **répertoire supprimé**. Aucune donnée réelle concernée. Leçon consignée :
toujours passer des chemins Windows natifs aux interpréteurs Windows.

## 9. Verdict migration

| Critère | État |
|---|---|
| Migration séquentielle verte | OUI — 10/10, integrity ok à chaque étape |
| Fingerprints des données conservés | OUI — 5 tables métier, hash identiques |
| Migration automatique = séquentielle | OUI — schéma identique, contenu identique hors horodatages |
| Idempotence | OUI — 2 rejeux supplémentaires, 0 écart |
| Rollback restauré | OUI — hash exact |
| Anomalie de schéma | AUCUNE |
| Protocole documenté | OUI — `85_RUNBOOK_MIGRATION_APP_DB_REELLE.md` |
| Application complète verte sur base migrée | voir §7 / rapport de mission |

**MIGRATION APP.DB 0016 → 0026 : PRÊTE ET RÉPÉTÉE.**

Migration prête ≠ activation autorisée. Le mode réel reste **NO GO — NON ACTIVÉ**, et la migration
réelle elle-même n'a pas été exécutée : elle attend une décision explicite.

## 10. Extension a 0027 (2026-08-13)

La migration `0027` (factures proprietaires emises) a ete ajoutee apres la redaction de ce
document. La repetition a ete rejouee de bout en bout jusqu'a elle, meme protocole :

| Etape | integrity | version | tables | index | triggers | pertes |
|---|---|---:|---:|---:|---:|---:|
| ... 0026 | ok | 26 | 67 | 108 | 1 | 0 |
| **-> 0027** | **ok** | **27** | **71** | **118** | **1** | **0** |

`0027` est additive comme les precedentes (4 CREATE TABLE, 4 index, 1 INSERT OR IGNORE de version ;
0 ALTER, 0 DROP, 0 DELETE, 0 UPDATE). Migration automatique via `apply_migrations()` : schema
strictement identique au sequentiel, contenu identique hors horodatages et id autoincrement.
Idempotence : 3 passages consecutifs, 0 ecart. Rollback par restauration de backup : hash exact
restitue (`8e299b93...70aa81d6`), base relisible en 0016, apres avoir ecrit dans
`factures_proprietaires` sur la base migree.

**MIGRATION APP.DB 0016 -> 0027 : PRETE ET REPETEE.** La base reelle n'a toujours pas ete migree.

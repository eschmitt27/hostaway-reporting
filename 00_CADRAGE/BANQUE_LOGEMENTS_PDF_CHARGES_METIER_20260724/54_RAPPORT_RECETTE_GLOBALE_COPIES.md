# 54 — Rapport de recette globale sur copies contrôlées des données réelles

Mission : exécuter une recette globale de l'application sur des copies des structures réelles,
corriger uniquement les défauts reproduits, produire un verdict GO/NO GO. Aucune donnée réelle
modifiée à aucun moment (vérifié par hash avant/après, cf. section 2).

## 1. Environnement de copies

Chemin (hors Git, hors `data_recette/`) :
```
C:\Users\Ewan\OneDrive\Documents\Conciergerie\_RECETTES_GLOBALES\RECETTE_GLOBALE_20260801_004232\
├── SOURCES_COPIEES\   (copie de 01_SOURCES_BRUTES/ + 02_TRAVAIL/, 85 fichiers)
├── APP_DATA\          (copie de 05_APPLICATION/data/app.db, renommée app.db)
├── SORTIES\           (non utilisé — le pipeline écrit dans SOURCES_COPIEES/02_TRAVAIL, comme en
│                        recette fictive ; pas de changement d'architecture pour en faire une copie
│                        séparée, décision assumée)
├── LOGS_SANITISES\    (extraits stdout/stderr des runs, vérifiés sans PII/chemin)
├── PREUVES\           (captures texte des écrans consultés)
├── HASHES\            (BASELINE_SOURCES_REELLES_SHA256.txt, MANIFEST_COPIES_SHA256.txt)
└── RAPPORTS\          (ce document et les 6 autres)
```

Serveur de recette lancé sur port 8030, `PROJECT_ROOT=SOURCES_COPIEES`, `APP_DATA_DIR=APP_DATA`,
tous les flags d'écriture réelle activés **sur la copie uniquement** (aucun impact possible sur le
réel — le write-guard applicatif borne toute écriture à `RECETTE_ROOT=PROJECT_ROOT`).

## 2. Intégrité des sources réelles

85 fichiers réels hashés (SHA256) avant toute opération (`01_SOURCES_BRUTES/`, `02_TRAVAIL/`,
`05_APPLICATION/data/app.db`). Copiés avec vérification hash source=copie (0 écart). Après
l'ensemble des opérations (pipeline lancé deux fois, migrations testées, recette navigateur),
**re-hashés un par un contre la baseline : 85/85 identiques, 0 écart**. Aucun fichier réel modifié,
aucun fichier réel créé, aucun verrou laissé dans le worktree (`git status` propre après coup).

## 3. Migrations sur base de recette

Base testée : copie de l'`app.db` réel (24 migrations déjà appliquées historiquement) + une base
vierge neuve. Les deux : `PRAGMA integrity_check` → `ok` ; `PRAGMA foreign_key_check` → 0 violation ;
`schema_migrations` → 24 lignes dans les deux cas (cohérent). Ré-application des migrations sur la
copie réelle : aucune ligne dupliquée dans aucune table (comptage table par table avant/après
identique) — le hash brut du fichier `.db` change (pages SQLite réallouées par `executescript` même
sur des `INSERT OR IGNORE`/`CREATE TABLE IF NOT EXISTS` no-op), mais le contenu logique est
rigoureusement identique. Idempotence confirmée au niveau qui compte (lignes, pas octets bruts).

## 4. Exécution du pipeline sur copies

**Chaîne aval (lot4quater→lot9→lot10→lot11→lot12→lot13), mois 2026-06** : lot4quater SUCCÈS,
**lot9 ÉCHEC** — `BLOQUANT [CTR-9-001] Source manquante : BANQUE_LOT8_IMPORT`. lot10-13 IGNORÉS
(non lancés, dépendance en échec — comportement correct, aucun faux succès). Sorties restaurées
(`Restaurer les sorties d'avant ce run`, 7 fichiers, hash de restauration vérifié identique à
l'original copié). Détail de cette anomalie réelle : `55_MATRICE_ECARTS_CONTRATS_REELS.md`.

**Chaîne charges (lot3), mois 2026-06** : SUCCÈS (2,5 s). Relancée une seconde fois : SUCCÈS,
mêmes totaux exposés par `/resultats/categories` (`NON_DISPONIBLE` les deux fois — la `SAISIE`
réelle contient 0 ligne actuellement, ce n'est pas un défaut applicatif, c'est l'état réel des
données : aucune charge n'a encore été saisie côté réel dans ce module). Aucun double comptage.

## 5. Lecture des modules (readers)

Tous les fichiers listés dans `app/config.py` vérifiés présents/lisibles sur les copies, sauf :
- `MASTER_BANQUE` (`BANQUE_LOT8_IMPORT.xlsx`) — **absent**, dossier `Lot8_Banque/` inexistant dans
  le réel (Banque n'a jamais été exécuté en réel, seulement en `data_recette` fictif).
- `PBI_LOGEMENTS` (export Power BI) — **absent**, `lot13` n'a jamais tourné en réel non plus.

Les deux absences sont correctement traitées comme `SOURCE_ABSENTE`/`NON_DISPONIBLE` par
l'application — jamais transformées en zéro fabriqué (`/health/diagnostic` et `/resultats/*`
vérifiés).

## 6. Ce qui n'a pas pu être testé, honnêtement

- Régénération complète du pipeline aval sur données réelles courantes (bloquée par l'absence
  réelle de `BANQUE_LOT8_IMPORT.xlsx`, cf. `55`). Les sorties Lot9-13 utilisées pour les
  réconciliations sont celles déjà présentes dans le réel (dernière exécution connue,
  2026-07-24), pas régénérées ce tour. Réconciliation A confirme leur cohérence mutuelle
  (Lot9↔Lot10, écart 0,00€), ce qui est une preuve partielle mais réelle, pas une preuve de
  fraîcheur par rapport à l'état actuel exact de `01_SOURCES_BRUTES`.
- Idempotence de la chaîne aval complète (lot9-13) — non testable pour la même raison. Testée à
  la place sur la chaîne `charges` (lot3), qui ne dépend pas de Banque.
- Corrections de données réelles : aucune n'a été appliquée (interdit par la mission) ; celles
  identifiées sont **proposées** dans `55`, pas exécutées.

## 7. Corrections de code apportées

**Aucune.** L'unique anomalie réelle trouvée (source Banque manquante bloquant lot9) est un
**écart de données/processus réel**, pas un défaut de code — le contrôle `CTR-9-001` du moteur
fonctionne exactement comme conçu (refuse de produire un résultat sur une source absente plutôt
que de fabriquer un flux à partir de rien). Aucune règle métier modifiée, aucune source réelle
touchée, aucun moteur altéré pour masquer l'absence de données.

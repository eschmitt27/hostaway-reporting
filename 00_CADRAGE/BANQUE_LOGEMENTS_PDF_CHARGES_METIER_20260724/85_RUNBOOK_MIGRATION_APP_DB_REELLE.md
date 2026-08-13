# 85 — Runbook : migration de la base applicative réelle 0016 → 0026

Procédure exacte, **non exécutée**. Elle attend une décision explicite. Répétition générale et
preuves : `84_REPETITION_MIGRATION_DB_0016_VERS_0026.md`.

Base concernée : `05_APPLICATION/data/app.db` (actuellement migration **0016**, 421 888 octets,
SHA256 `8e299b93…70aa81d6`).

## Avant de commencer — conditions d'entrée

| Condition | Comment vérifier |
|---|---|
| Aucune instance applicative en cours | `Get-NetTCPConnection -LocalPort 8000` et tout autre port de recette ; `Get-Process python` |
| Aucun writer actif | mode réel OFF, `CALCULS_REAL_RUN_ENABLED` non positionné |
| Espace disque | > 10 Mo (la base fait < 1 Mo) |
| Branche et HEAD attendus | `git rev-parse HEAD`, `git status` propre |

Si une instance tourne : l'**arrêter proprement** (pas de kill brutal — SQLite en WAL doit
pouvoir écrire son checkpoint).

## Procédure

```powershell
# --- 0. Se placer dans le worktree ---
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"

# --- 1. Empreinte AVANT (à conserver) ---
$db = "05_APPLICATION\data\app.db"
Get-FileHash $db -Algorithm SHA256
(Get-Item $db).Length
(Get-Item $db).LastWriteTime

# --- 2. Backup horodaté + copie de rollback ---
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$bak = "99_ARCHIVES\APP_DB\app_db_PRE_MIGRATION_0026_$ts.sqlite"
New-Item -ItemType Directory -Force "99_ARCHIVES\APP_DB" | Out-Null
Copy-Item $db $bak
# Vérifier que le backup est identique à l'original AVANT d'aller plus loin
(Get-FileHash $db).Hash -eq (Get-FileHash $bak).Hash    # doit afficher True
```

**Si cette comparaison n'affiche pas `True`, ARRÊTER ici.** Ne jamais migrer sans backup vérifié.

Attention WAL : si `app.db-wal` / `app.db-shm` existent, les copier aussi, ou faire un checkpoint
propre au préalable. Un backup du seul `.db` avec un `-wal` non intégré est incomplet.

```powershell
# --- 3. Migration (mécanisme applicatif normal, aucun script ad hoc) ---
cd 05_APPLICATION
python -c "from pathlib import Path; import sys; sys.path.insert(0,'.'); from app.db.connection import apply_migrations; apply_migrations(Path(r'data\app.db')); print('migrations appliquees')"
cd ..
```

Le chemin passé doit être un **chemin Windows natif**. Un chemin de style `/c/Users/...` crée
silencieusement une base parasite ailleurs (incident constaté et documenté en `84` §8).

```powershell
# --- 4. Vérifications post-migration ---
python -c "import sqlite3; c=sqlite3.connect(r'05_APPLICATION\data\app.db'); print('integrity:', c.execute('PRAGMA integrity_check').fetchone()[0]); print('version:', c.execute('SELECT MAX(CAST(version AS INTEGER)) FROM schema_migrations').fetchone()[0]); print('tables:', c.execute(\"SELECT COUNT(*) FROM sqlite_master WHERE type='table'\").fetchone()[0])"
```

Attendu, **exactement** :

| Contrôle | Valeur attendue |
|---|---|
| `integrity_check` | `ok` |
| `schema_migrations` max | `26` |
| tables | **67** (hors tables internes SQLite) |
| index | **108** |
| triggers | **1** (`trg_facture_classification_defaut`) |

```powershell
# --- 5. Fingerprints : les données existantes doivent être intactes ---
# (script de la répétition générale, réutilisable tel quel)
python "<scratchpad>\MIGRATION_20260813\fingerprint_db.py" "05_APPLICATION\data\app.db"
```

Attendu — **inchangés** par rapport à l'avant-migration :

| Table | Lignes | Hash (préfixe) |
|---|---:|---|
| `audit_events` | 49 | `ca0ec73de4dfb285` |
| `banque_imports` | 2 | `c3cf65a2ff45cb51` |
| `banque_rapprochement_evenements` | 4 | `76e164856c8d02e9` |
| `banque_rapprochements` | 3 | `52be0fe31caf9320` |
| `banque_suggestion_decisions` | 2 | `82592aeb6161da09` |

Seule `schema_migrations` doit avoir changé (16 → 26 lignes).

```powershell
# --- 6. Smoke tests lecture (instance de recette, JAMAIS le port 8000) ---
cd 05_APPLICATION
$env:RECETTE_MODE = "1"
python -m uvicorn app.main:app --port 8042
# Parcourir : dashboard, logements, propriétaires, réservations, ménages, fournisseurs,
# factures, règlements, Banque, trésorerie propriétaires, comptabilité, analytique,
# résultats, contrôles, clôture, exports. Chercher 500 / colonne absente / template cassé.
# Puis arrêter cette instance (Ctrl+C), et elle seule.
```

## Critère GO / rollback

**GO** si et seulement si les 5 conditions sont réunies : `integrity_check = ok`, version `26`,
67 tables / 108 index / 1 trigger, **fingerprints des 5 tables métier inchangés**, smoke tests sans
erreur 500.

**Rollback** (si un seul critère échoue) :

```powershell
Remove-Item "05_APPLICATION\data\app.db","05_APPLICATION\data\app.db-wal","05_APPLICATION\data\app.db-shm" -ErrorAction SilentlyContinue
Copy-Item $bak "05_APPLICATION\data\app.db"
(Get-FileHash "05_APPLICATION\data\app.db").Hash    # doit correspondre à l'empreinte de l'étape 1
```

Supprimer `-wal` et `-shm` est **obligatoire** : un journal orphelin peut réappliquer des pages
par-dessus la base restaurée. Rollback validé sur copie avec restitution du hash exact (`84` §6).

## Après migration

- La table `mouvements_tresorerie_proprietaires` devient disponible : la trésorerie propriétaires
  est alors techniquement exploitable (elle ne l'est pas aujourd'hui).
- **La migration n'active rien.** Mode réel, writers réels et pipeline réel restent OFF. Les
  activer relève d'une décision séparée, avec sa propre checklist
  (`72_CHECKLIST_GO_NO_GO_MODE_REEL.md`).
- Consigner l'opération dans `JOURNAL_CONTROLES.md` : date, hash avant, hash après, chemin du
  backup, résultat des 5 critères.

## Ce que ce runbook ne fait pas

- Il ne migre **aucune donnée métier** (les migrations 0017→0026 sont purement additives : 0 ALTER,
  0 DROP, 0 DELETE, 0 UPDATE).
- Il ne remplit ni Lot 5, ni la trésorerie propriétaires, ni la Banque.
- Il n'active pas le mode réel.

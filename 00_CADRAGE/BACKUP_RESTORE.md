# Sauvegarde et restauration des données — `app.db`

Mis en place le 2026-10-03 (mission « Fiabiliser les sauvegardes automatiques »).
Code : `05_APPLICATION/app/services/backup_service.py`, `migration_service.py`,
`ordonnanceur_service.py`. Paramètres : `05_APPLICATION/app/config.py` (section « Politique de
sauvegarde »). Tests : `tests/test_sauvegardes_politique.py`, `tests/test_backup_service.py`.

On sauvegarde **les données** : la base SQLite `app.db` et les métadonnées qui prouvent qu'une copie
est saine et restaurable. Ni le code, ni Git, ni `.venv`, ni les PDF, ni les logs.

## 1. Architecture

```
app.db  (C:\Users\Ewans\PilotageConciergerie\data\app.db, WAL)
  │
  ├── transactions / datasets candidats / activation atomique   ← protection au quotidien
  │     (Hostaway, imports, recalculs ciblés : aucune copie complète)
  │
  ├── copie automatique avant opération risquée
  │     ├── BEFORE_MIGRATION       au démarrage, seulement si une migration est en attente
  │     └── BEFORE_GLOBAL_REFRESH  avant « Actualiser toute l'activité »
  │
  └── copie quotidienne (DAILY), vers 03:00, même sans aucune activité
           │
           ├── rôle quotidien   : 7 jours
           ├── rôle hebdomadaire : 4 semaines   } mêmes fichiers, aucune copie en plus
           ├── rôle mensuel     : 6 mois        }
           └── archives protégées (ARCHIVE / MANUAL protégées) : illimité
```

Toutes les copies vont dans `BACKUPS_DIR` = `<APP_DATA_DIR>\backups`
(`C:\Users\Ewans\PilotageConciergerie\data\backups`).

### Une copie = deux fichiers

- `<AAAAMMJJ_HHMMSS>_<OPERATION>_<BCK-…>.db` : la base, copiée par **l'API de backup SQLite** (état
  cohérent même avec des écritures encore dans le `-wal`), écrite sous un nom provisoire `.partiel`
  puis renommée, et passée en `journal_mode=DELETE` (un seul fichier autonome, empreinte figée).
- `<…>.db.meta.json` (sidecar) : identifiant, catégorie, raison, date, SHA256, taille, version de
  schéma avant / cible, nombre de tables et de lignes, `integrity_check`, `foreign_key_check` (copie
  et source), durée, statut, protection, commit Git, résultat de la copie secondaire.

**Le catalogue qui fait foi, ce sont les sidecars**, pas la table `sauvegardes_base` : cette table vit
dans la base qu'elle décrit et disparaît ou recule à chaque restauration. Elle reste alimentée en
plus, pour l'écran.

### Copie VALIDE

Une copie n'est `VALIDE` que si elle s'ouvre, que `PRAGMA integrity_check` = `ok` et que
`PRAGMA foreign_key_check` donne le même nombre de violations que la source au moment de la copie
(0 sur la base réelle). Sinon `CORROMPU` : elle est gardée comme preuve, jamais utilisée.

## 2. Sauvegarde quotidienne

- Activée par `BACKUP_DAILY_ENABLED=true` dans le `.env` de l'instance réelle (désactivée par défaut
  dans le code : un test ou une instance de recette n'arme jamais rien).
- Portée par **le minuteur existant** de l'ordonnanceur (un seul fil, `ordonnanceur-hostaway`,
  battement toutes les 15 min). Le minuteur démarre si l'actualisation Hostaway automatique
  (`ORDONNANCEUR_ACTIF`) **ou** la sauvegarde quotidienne est activée ; chaque tâche ne part que si
  elle-même est activée. Activer la sauvegarde **ne déclenche jamais Hostaway**.
- Due à partir de `BACKUP_DAILY_HOUR` (3 → 03:00, heure locale). PC éteint à 03:00 : elle part au
  premier battement suivant (≤ 15 min après le démarrage de l'application).
- **Une seule par jour** : la décision est revérifiée sous verrou ; redémarrages et battements
  suivants répondent « Sauvegarde du jour déjà faite. ». Après 3 échecs le même jour, reprise le
  lendemain (pas une copie illisible tous les quarts d'heure).
- Après une quotidienne valide, la rotation (§5) s'applique aux sauvegardes gérées.

## 3. Sauvegarde avant migration

Au démarrage, `main.lifespan` appelle `migration_service.migrer_au_demarrage()` :

```
migrations en attente ? (lecture seule, sans toucher la base)
  NON  → démarrage normal, AUCUNE copie
  OUI  → copie BEFORE_MIGRATION (raison AVANT_MIGRATION, version avant, version cible)
         copie non valide ou impossible → migration NON lancée, démarrage refusé
         → migrations
         → integrity_check + foreign_key_check (pas de nouvelle violation)
         → succès : démarrage ; échec : restauration de la copie, démarrage refusé
base neuve (aucune version) → migrations directes, rien à protéger
```

La prochaine vraie migration (0122…) passera par ce chemin sans rien faire de plus.
`migrer_avec_sauvegarde()` reste disponible pour une migration lancée à la main.

## 4. Actualisation globale

`orchestrateur_service.actualiser(cibles=None)` (bouton « Actualiser toute l'activité ») prend une
copie `BEFORE_GLOBAL_REFRESH` avant de commencer ; si la copie est impossible, l'actualisation est
refusée. La base n'est restaurée automatiquement que si elle est **corrompue** après le run
(`integrity_check`) — jamais pour une simple étape en échec.

**Pas de copie complète** pour une actualisation ciblée, un appel Hostaway (manuel ou scheduler),
un import PDF ou un recalcul ciblé : ces opérations reposent sur les transactions et l'activation
atomique. Hostaway : chaque import crée une nouvelle extraction (`extraction_id`) dans une transaction
`BEGIN IMMEDIATE` ; un échec (timeout, 429) laisse l'extraction précédente intacte et active.

## 5. Rotation

`backup_service.purger(dry_run=True)` (défaut) montre exactement ce qui serait supprimé
(nom, catégorie, date, taille, octets libérés) sans rien effacer.

| Catégorie | Conservé |
|---|---|
| DAILY | la plus récente de chacun des 7 derniers jours, des 4 dernières semaines ISO, des 6 derniers mois (rôles cumulables sur un même fichier) |
| BEFORE_MIGRATION | les 5 plus récentes + toutes celles de moins de 30 jours |
| BEFORE_GLOBAL_REFRESH | les 5 plus récentes |
| MANUAL | toujours (jamais supprimée automatiquement) |
| ARCHIVE / protégée | toujours |

Jamais supprimées : une sauvegarde protégée, un fichier rangé dans un dossier `archive_*`, une copie
non valide (preuve d'incident), la dernière sauvegarde saine.

**Sauvegardes héritées** (créées avant le 2026-10-03, sans la marque `politique`) : hors rotation. Leur
nettoyage se fait **sur validation séparée** :

```python
backup_service.purger(dry_run=True, inclure_heritees=True)                       # voir
backup_service.purger(dry_run=False, inclure_heritees=True, confirmer=True)      # appliquer
```

Les fichiers `.db` sans sidecar (copies d'outils : `app_avant_*.db`) ne sont jamais touchés.

## 6. Sauvegardes protégées et archive V1

- Protéger une copie : `protegee=True` à la création, ou catégorie `ARCHIVE`, ou rangement dans un
  dossier `archive_*`.
- `backup_service.enregistrer_archive(chemin, libelle=…, sha256_attendu=…)` inscrit une copie
  existante comme archive protégée **sans toucher au fichier** (seul un sidecar est ajouté ; l'empreinte
  est vérifiée avant et après).
- **ARCHIVE DE RÉFÉRENCE V1 — 03/10/2026** :
  `backups\archive_v1_2026-10-03\app_data_v1_2026-10-03_020652.db`,
  SHA256 `fb0170a13cb9f8ecefa83315f1e8f023a3ba59612e05dffe093f8a5685394cff`, lecture seule,
  manifeste `…_020652.manifest.json`, inscrite comme `ARCHIVE` protégée.

## 7. Restauration

- **Essai, sans risque** : `backup_service.tester_restauration(sauvegarde_id)` restaure dans un
  dossier temporaire isolé, inspecte, compare aux métadonnées, supprime l'essai.
- **Réelle** : `backup_service.restaurer(sauvegarde_id, confirmer=True)`. Refusée sans `confirmer`,
  refusée si la copie n'est pas valide (empreinte, intégrité, FK). Écrit **par l'API SQLite** dans la
  base cible quand elle est lisible (le `-wal` et les connexions restent cohérents) ; base détruite :
  remplacement du fichier après retrait des résidus `-wal`/`-shm`.
- Aucune erreur métier ne déclenche de restauration. Automatique seulement : migration en échec au
  démarrage, base corrompue après une actualisation globale.

## 8. Procédure d'urgence

1. Arrêter l'application (port 8000).
2. Mettre de côté la base actuelle : copier `app.db`, `app.db-wal`, `app.db-shm` dans un dossier
   `incident_<date>` (ne rien supprimer).
3. Choisir la copie : écran Observabilité ou `backup_service.catalogue()` ; vérifier avec
   `backup_service.verifier(id)` puis `tester_restauration(id)`.
4. `backup_service.restaurer(id, confirmer=True)` (application arrêtée).
5. Relancer l'application ; contrôler `integrity_check`, `foreign_key_check`, les écrans clés.
6. Noter l'incident dans `JOURNAL_CONTROLES.md`.

Sans l'application : une copie est un `.db` autonome ; la copier sous le nom `app.db` (application
arrêtée, après avoir retiré `app.db-wal` et `app.db-shm`) suffit.

## 9. Destination secondaire

`BACKUP_SECONDARY_DIR` (`.env`) : si renseigné, chaque copie valide y est recopiée (fichier fermé,
vérifié par SHA256, + sidecar) ; la rotation y supprime aussi les copies purgées. Une erreur
(dossier absent, disque débranché) est tracée dans le sidecar et ne touche jamais la copie locale.

**BACKUP_SECONDAIRE : NON CONFIGURÉ.** Base et copies sont sur le même disque `C:`. Le choix d'un
second emplacement (disque externe, NAS, dossier synchronisé…) appartient à l'utilisateur.

## 10. Concurrence

Une seule copie à la fois : verrou de processus + fichier `backups\.sauvegarde.lock` (couvre un
outil lancé à côté). Avant migration et avant actualisation globale : attente bornée (300 s / 60 s)
puis refus propre ; quotidienne : aucune attente, elle passe son tour. Un verrou de plus de 30 min
(processus tué) est réputé abandonné.

## 11. Observabilité

- Chaque copie écrit une ligne `run_history` (opération `SAUVEGARDE`, acteur = catégorie, début,
  fin, durée, statut, erreur) et une ligne `sauvegardes_base`.
- Écran **Observabilité › Runs** : dernière sauvegarde réussie (date, type, taille), nombre de
  sauvegardes et rôles, espace disque, sauvegarde quotidienne (active / prochaine), sauvegarde
  secondaire configurée. Noms de fichiers seulement, jamais de chemin.

## 12. Paramètres (`.env`)

| Variable | Défaut | Réel |
|---|---|---|
| `BACKUP_DAILY_ENABLED` | false | **true** |
| `BACKUP_DAILY_HOUR` | 3 | 3 |
| `BACKUP_RETENTION_DAILY` / `_WEEKLY` / `_MONTHLY` | 7 / 4 / 6 | idem |
| `BACKUP_RETENTION_MIGRATION` / `_MIGRATION_DAYS` | 5 / 30 | idem |
| `BACKUP_RETENTION_GLOBAL_REFRESH` | 5 | idem |
| `BACKUP_SECONDARY_DIR` | vide | vide (non configuré) |
| `BACKUP_LOCK_STALE_SECONDS` | 1800 | idem |

# APP-3b — Protocole d'écriture réelle contrôlée (Nouvelle charge guidée)

> **STATUT : EXÉCUTÉ SUR COPIE ISOLÉE (2026-07-13) — ÉCRITURE RÉELLE NON ACTIVÉE.** Les flags
> `CHARGES_REAL_WRITE_ENABLED` et `CHARGES_REAL_WRITE_CONFIRMATION_ENABLED` restent **False**
> (`05_APPLICATION/app/config.py`). Toute activation fera l'objet d'une décision explicite séparée.
>
> Exécution : `04_LOGS/APP3B_ECRITURE_REELLE/executer_protocole_copie.py` — 3 cas (avantage associé,
> refacturable, ménage), **59/59 contrôles verts**, SHA256 des 3 fichiers réels inchangés.
> Rapport : `04_LOGS/APP3B_ECRITURE_REELLE/RAPPORT_PROTOCOLE_COPIE_<TS>.md` ; trace :
> `CTR-APP3B-ECRITURE-REELLE-01`. **4 limites bloquantes** restent à lever avant §8 (voir §7 du rapport) :
> writer réel inexistant (`persister_reel()` = stub), `charge_id` non réservé, verrou classeur ouvert
> non testé, cache des formules `C/I/J/AD` non recalculé hors Excel.

## 0. Contexte

- Aujourd'hui : `SAISIE_Charges_Flux.xlsx` = **0 charge réelle** ; `SAISIE_Charges_Impacts.xlsx`
  (AFFECTATIONS/MENAGE/RESERVE) vide ; `MASTER_CALC_AVANTAGES` non régénéré sur données réelles.
- `persister_reel()` lève `PermissionError` tant que le flag est False. Seul `persister_sur_copie()`
  (dry-run) écrit, et uniquement sur copie.
- Objectif du protocole : prouver, **sur copie isolée**, qu'une charge réelle unique s'écrit
  correctement, sans casser les formules/validations/macros, sans double comptage, avec régénération
  Lot7 et contrôles Lot11 propres — **avant** d'envisager l'activation du flag.

## 1. Pré-requis

- [ ] Recette verte : `tests/` (Python312 + PYTHONPATH miniconda) et `05_APPLICATION/tests` (miniconda).
- [ ] Branche dédiée (pas `main`), worktree propre (`git status --short` sans WIP hors périmètre).
- [ ] Aucune clôture en cours sur le mois de test ; mois de test **OUVERT** dans `REF_Cloture_Mensuelle`.

## 2. Sauvegarde (avant toute écriture)

- [ ] Copier les fichiers réels cibles vers `99_ARCHIVES/APP3B_ECRITURE_REELLE_<TS>/` :
  - `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx`
  - `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Impacts.xlsx`
  - `02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx`
- [ ] Journaliser les empreintes (SHA256) des 3 fichiers avant écriture.
- [ ] Vérifier que `99_ARCHIVES/APP3B_ECRITURE_REELLE_*` est **ignoré** (`.gitignore`) — backups non versionnés.

## 3. Copie isolée de test

- [ ] Créer un dossier scratch hors dépôt (`$env:TEMP/app3b_ecriture_<TS>/`).
- [ ] Y copier les 3 fichiers. **Toute l'étape 4-6 s'exécute sur ces copies**, jamais sur le réel.
- [ ] Confirmer par empreinte que les fichiers réels ne changent pas pendant tout le protocole.

## 4. Charge test minimale (spécification)

Une **seule** charge, déterministe, traçable :

| Champ | Valeur test |
|---|---|
| `charge_id` | `CHG-TEST-<TS>` (préfixe dédié, jamais réutilisé) |
| `date_charge` | date d'un mois OUVERT (ex. `2026-06-15`) |
| `montant` | `100.00` |
| `categorie_charge_id` | catégorie avec avantage possible (ex. `CHG_025` Repas) |
| `mode_paiement_id` | `PAY_001` (banque pro) |
| `avantage_associe_id` | `PERS_EWAN` (pour exercer la régénération Lot7) |
| `affectation_type` / `refacturable` / `affectable_menage` | selon le cas testé (1 seul à la fois) |

> Variante ménage (test séparé) : catégorie ménage (`CHG_027`), `affectable_menage`, sans réserve.

## 5. Écriture contrôlée (sur copie)

- [ ] Exécuter le writer réel **contre les copies** (chemins injectés), flag activé **uniquement dans
      l'environnement de test isolé** (jamais commité). Écrit : ligne SAISIE_Charges_Flux + lignes
      normalisées AFFECTATIONS/MENAGE/RESERVE (selon le cas) via `build_persistable`.
- [ ] Idempotence : réexécuter la **même** charge → aucune ligne dupliquée (remplacement par `charge_id`).

## 6. Vérifications (toutes obligatoires, sur copie)

### 6.1 Colonnes écrites dans SAISIE_Charges_Flux
- [ ] Colonnes **manuelles** écrites correctement (charge_id, date_charge, montant, categorie,
      mode_paiement, associe/avantage, affectation…).
- [ ] Colonnes **formules NON écrasées** : `C` (mois), `I` (impact_resultat_reel), `J`
      (impact_resultat_comptable), `AD` (ROW_HASH) — restent des formules Excel vivantes.

### 6.2 Tables d'impacts (si concernées)
- [ ] `AFFECTATIONS` : 1 ligne par quote-part ; **Σ quote_part = montant** ; `charge_id` valide ;
      aucun identifiant concaténé dans une cellule.
- [ ] `MENAGE` : 1 ligne par intervenant **ou** logement (jamais les deux) ; **jamais** de ligne RESERVE
      pour une charge ménage.
- [ ] `RESERVE_REFACTURATION` : statut `EN_ATTENTE` ; **Σ montant_refacturable = montant refacturable** ;
      aucune application automatique en préfacture.

### 6.3 Intégrité Excel
- [ ] Formules, plages nommées, **validations de données** et **macros VBA** intactes (comparaison
      structure avant/après ; le classeur reste ouvrable sans réparation Excel).
- [ ] `keep_vba` respecté pour les `.xlsm` (aucune perte de projet VBA).

### 6.4 Non-duplication
- [ ] `charge_id` **unique** dans SAISIE_Charges_Flux (aucun doublon après 1re et 2e écriture).
- [ ] Aucune ligne SOURCE_SAISIE Lot7 créée pour cette charge (avantage porté par la charge).

### 6.5 Régénération Lot7 (si avantage associé)
- [ ] Régénérer `MASTER_CALC_AVANTAGES` via `lot7_generateur_avantages.generer(copie…)`.
- [ ] La charge test ressort pour le **bon associé/mois**, `code_impact = HR`, `avantage_net` cohérent,
      **exactement une fois** ; 2e génération identique (jamais ×2).

### 6.6 Contrôles Lot11 (pas de faux positif)
- [ ] `controles_suivi_associe(...)` sur les copies : **0 anomalie** attendue sur une charge cohérente.
- [ ] Les 2 cross-contrôles (`AVANTAGE_ABSENT_DU_SUIVI`, `CHARGE_PAYEE_NON_REPRISE`) **s'activent**
      (calc régénéré, non vide) et **ne remontent pas** de faux positif.

## 7. Rollback

- [ ] En cas d'échec d'une vérification : **ne rien conserver**. Supprimer les copies scratch.
- [ ] Le réel n'a jamais été touché (empreintes §2 inchangées) → aucun rollback réel nécessaire.
- [ ] Si (hypothèse) une écriture réelle avait eu lieu : restaurer depuis `99_ARCHIVES/APP3B_ECRITURE_REELLE_<TS>/`
      (copie octet-à-octet), revérifier empreintes, journaliser l'incident.

## 8. Critères pour envisager `CHARGES_REAL_WRITE_ENABLED = True`

Toutes ces conditions réunies, **puis discussion explicite** :
1. §6.1 à §6.6 **toutes vertes** sur copie, pour les cas : non-ménage refacturable, ménage, avantage associé.
2. Idempotence prouvée (double écriture = pas de doublon).
3. Intégrité Excel/VBA/validations prouvée.
4. Régénération Lot7 + contrôles Lot11 verts sur données de test réelles.
5. Sauvegarde + rollback documentés et testés.
6. `CHARGES_REAL_WRITE_CONFIRMATION_ENABLED` prévu comme **second garde-fou** (double confirmation).
7. Décision humaine tracée (JOURNAL_CONTROLES) avant activation ; activation réversible (flag).

## 9. Journalisation

- [ ] Entrée `JOURNAL_CONTROLES.md` : `CTR-APP3B-ECRITURE-REELLE-XX` (résultats, empreintes, verdict).
- [ ] Aucune donnée réelle modifiée tant que les critères §8 ne sont pas validés et le flag non activé.

---

> **Rappel final** : ce document est un **protocole**. Il ne s'exécute pas ici. Les avantages associés
> restent **HR** (hors résultat/propriétaire). Aucun règlement, aucun mouvement de trésorerie.

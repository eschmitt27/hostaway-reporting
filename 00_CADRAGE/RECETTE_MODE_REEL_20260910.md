# Recette utilisateur — mode réel — 2026-09-10

Instance réelle, données réelles, **writers métier activés**. Le scheduler Hostaway reste **OFF** :
chaque actualisation est un geste utilisateur identifiable.

## Accès

| | |
|---|---|
| URL | **http://127.0.0.1:8000** |
| Port | 8000, **127.0.0.1 uniquement** (aucune exposition réseau) |
| Projet | `C:\Users\Ewans\.devswarm\repos\1\9537f8ef\resume-pilotage-conciergerie-20260909` |
| Base | `05_APPLICATION\data\app.db` — schéma **0071**, `integrity_check` **ok**, `foreign_key_check` **ok** |
| Mode réel | **ON** — `RECETTE_MODE=0` (pas de bandeau « données fictives »), `MODE_REEL_ECRITURES=1` |
| Scheduler Hostaway | **OFF** · CleaningTasks auto **OFF** |

## Comment relancer l'instance avec la même configuration

Depuis `05_APPLICATION\`, avec le venv du projet. Aucun secret, rien à committer :

```
set PROJECT_ROOT=C:\Users\Ewans\.devswarm\repos\1\9537f8ef\resume-pilotage-conciergerie-20260909
set APP_DATA_DIR=%PROJECT_ROOT%\05_APPLICATION\data
set PORT=8000
set LOT4A_ENGINE_PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe
set MENAGES_ENGINE_PYTHON=%LOT4A_ENGINE_PYTHON%
set PILOTAGE_ENGINE_PYTHON=%LOT4A_ENGINE_PYTHON%
set RECETTE_MODE=0
set MODE_REEL_ECRITURES=1
set CHARGES_REAL_WRITE_ENABLED=1&  set CHARGES_REAL_WRITE_CONFIRMATION_ENABLED=1
set FACTURES_REAL_WRITE_ENABLED=1& set FACTURES_REAL_WRITE_CONFIRMATION_ENABLED=1
set MENAGES_CYCLE_REAL_WRITE_ENABLED=1& set MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED=1
set COMPTABILITE_REAL_WRITE_ENABLED=1&  set COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED=1
set BANQUE_REAL_WRITE_ENABLED=1&  set BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=1
%LOT4A_ENGINE_PYTHON% run_app.py
```

`LOT4A_ENGINE_PYTHON` est indispensable : le défaut codé (`C:\Program Files\Python312\python.exe`)
n'existe pas sur cette machine, et sans lui tout lot moteur répond « Interpréteur des lots
introuvable ».

## Writers — état exact

| Writer | État | Portée d'écriture |
|---|---|---|
| Écritures opérationnelles (NIVEAU A) | **ACTIF** (par conception) | déclarations ménage, facture À CONTRÔLER depuis PDF, saisies, commentaires, suivi humain, administration des référentiels |
| `CHARGES_REAL_WRITE_*` | **ACTIF** | SQLite seul (`charges`, `charge_evenements`) |
| `FACTURES_REAL_WRITE_*` | **ACTIF** | SQLite seul (validation facture, ventilation, règlements fournisseurs) |
| `MENAGES_CYCLE_REAL_WRITE_*` | **ACTIF** | SQLite seul (cycle de vie ménage) |
| `COMPTABILITE_REAL_WRITE_*` | **ACTIF** | SQLite seul (écritures, périodes, OD, caisse) |
| `BANQUE_REAL_WRITE_*` | **ACTIF** | SQLite seul — **aucun mouvement en base** (décision connue) |
| `CALCULS_REAL_RUN_*` | **OFF — volontaire** | réécrirait des Excel/CSV **réels suivis par Git** (`02_TRAVAIL\Lot9\|Lot10\|Lot11\|Lot12\MASTER_*.xlsx`, `03_EXPORTS\PowerBI\*.csv`). Le recalcul économique s'obtient sans lui via `/actualisation` (orchestrateur SQLite, zéro Excel) |
| `MENAGES_REAL_RECALC_*` | **OFF — volontaire** | chemin LEGACY qui réécrit `M04_MENAGES` et `MASTER_NORM_Declarations_Internes` réels. Le bouton unique « Actualiser le rapprochement » ne l'utilise pas |
| `HH_REAL_WRITE_*` | **GELÉ** | flag mort : plus aucun service ne le lit (la saisie HH vit en SQLite, NIVEAU A) |
| `REF_ASSOC_MODE_REAL_WRITE_*` | **GELÉ** | migration one-shot qui écrirait dans `REF_Setup.xlsm` (source brute) |
| `CONTROLES_REAL_WRITE_*` | **GELÉ** | interlock **inverse** : s'il était actif, `controles_runner_service` **refuserait** le recalcul sur copie. Le moteur reste la vérité de l'anomalie |
| `ORDONNANCEUR_ACTIF` | **OFF** | demandé pour la recette |

Deux leviers simultanés sont exigés pour tout writer NIVEAU B : un contexte
(`RECETTE_MODE` **ou** `MODE_REEL_ECRITURES`) **et** la variable dédiée. Sans variable
d'environnement, tout est OFF — le défaut d'installation est inchangé.

## Matrice de recette

Remplacez `À TESTER` par **OK** · **BUG** · **INCOMPLET** · **NON TESTÉ**.

| Module | Lecture réelle | Écriture réelle | Données présentes | Testable | Résultat | Limitation |
|---|---|---|---|---|---|---|
| Accueil `/` | OUI | — | synthèse | COMPLETEMENT TESTABLE | À TESTER | — |
| Logements `/logements` | OUI | NIVEAU A | 19 `ref_logements` | COMPLETEMENT TESTABLE | À TESTER | — |
| Propriétaires `/proprietaires-reglements` | OUI | NIVEAU A | 12 propriétaires | PARTIELLEMENT TESTABLE | À TESTER | règlement réel dépend d'une facture validée |
| Comptes propriétaires `/comptes-proprietaires` | OUI | dérivé | FIFO **global** (vérifié : imputation par propriétaire, jamais par logement) | COMPLETEMENT TESTABLE | À TESTER | — |
| Réservations `/reservations` | OUI | saisie HH (NIVEAU A) | 1585 lignes actives · 3 saisies HH | COMPLETEMENT TESTABLE | À TESTER | lecture seule pour les réservations Hostaway (par conception) |
| Hostaway `/hostaway` | OUI | — | 4739 réservations, 4664 payouts, 3711 cleaning tasks | **BLOQUE PAR SOURCE EXTERNE** | À TESTER | `HOSTAWAY_LIVE_BLOQUE_PAR_IDENTIFIANTS` — aucun `.env` dans les emplacements configurés du projet |
| Ménages `/menages` | OUI | cycle NIVEAU B **ACTIF** | 33 rapprochements, 29 coûts complets, 727 tâches | **BLOQUE PAR SOURCE EXTERNE** (bouton unique) | À TESTER | « Actualiser le rapprochement » s'arrête au préflight Hostaway (par conception). Les écrans et le cycle de vie restent testables |
| Fournisseurs `/fournisseurs` | OUI | NIVEAU A | **0 fournisseur** | NON TESTABLE SANS DONNEES | À TESTER | à créer par l'utilisateur |
| Factures fournisseurs `/factures` | OUI | **ACTIF** | 2 factures | PARTIELLEMENT TESTABLE | À TESTER | import PDF possible ; aucun PDF source déposé |
| Factures propriétaires `/factures-proprietaires` | OUI | **ACTIF** | **13 BROUILLON** (2026-08, montants réels) | COMPLETEMENT TESTABLE | À TESTER | émission = geste définitif, à faire en connaissance de cause |
| Charges | OUI | **ACTIF** (prouvé) | 1 charge (test annulé) | PARTIELLEMENT TESTABLE | À TESTER | **pas d'écran de saisie dédié** : une charge se crée par une ligne CHARGE sur un BROUILLON propriétaire |
| Créances `/creances` | OUI | dérivé | dérivé | COMPLETEMENT TESTABLE | À TESTER | — |
| Calculs `/calculs` | OUI | OFF volontaire | runs historisés | PARTIELLEMENT TESTABLE | À TESTER | dry-run OK ; run réel volontairement désactivé (réécrit des Excel réels) |
| Banque `/banques-caisse` | OUI | **ACTIF** | **0 mouvement** | **PARTIELLEMENT TESTABLE — AUCUNE DONNÉE BANCAIRE DISPONIBLE** | À TESTER | décision connue : aucune donnée bancaire attendue pour cette phase |
| Référentiels `/administration/referentiels` | OUI | NIVEAU A | 6 tables administrables | COMPLETEMENT TESTABLE | À TESTER | historisation vérifiée (`date_debut_validite`/`date_fin_validite`, `date_debut`/`date_fin`) |
| Référentiel Setup `/referentiel-setup` | OUI | GELÉ | REF_Setup réel | LECTURE SEULE PAR CONCEPTION | À TESTER | écriture dans `REF_Setup.xlsm` gelée |
| Sources & calculs `/sources-calculs` | OUI | dry-run | inventaire réel | COMPLETEMENT TESTABLE | À TESTER | — |
| Contrôles `/controles-cloture` | OUI | suivi NIVEAU A | 19 constats Lot11 | PARTIELLEMENT TESTABLE | À TESTER | écriture moteur gelée par conception (interlock) |
| Clôtures `/clotures` | OUI | suivi NIVEAU A | 2 clôtures | PARTIELLEMENT TESTABLE | À TESTER | ne pas clôturer un mois réel juste pour tester un bouton |
| Pilotage mensuel `/pilotage-mensuel` | OUI | — | réel | COMPLETEMENT TESTABLE | À TESTER | — |
| Comptabilité `/comptabilite` (+ 9 sous-écrans) | OUI | **ACTIF** | 0 écriture, plan comptable 7 comptes | PARTIELLEMENT TESTABLE | À TESTER | génération journal Ventes disponible sur 2026-06 ; caisse/banque nécessitent des mouvements |
| Résultats `/resultats` | OUI | — | 2026-06/07/08 réels | COMPLETEMENT TESTABLE | À TESTER | — |
| Actualisation `/actualisation` | OUI | orchestrateur | DAG + historique | PARTIELLEMENT TESTABLE | À TESTER | dry-run OK ; **DAG rouge à sa racine** car `HOSTAWAY_RAW` ne peut pas être rafraîchi (identifiants absents) → tout l'aval est `A_RECALCULER`. Les données affichées ailleurs restent justes (cf. anomalie 3) |
| Observabilité `/observabilite/runs` | OUI | — | 10 runs, 18 sauvegardes | COMPLETEMENT TESTABLE | À TESTER | `/observabilite` nu = 404 (normal) |

Écrans techniques : `/health` 200 · `/health/diagnostic` 404 **par conception**.

## Ce qui a été prouvé en réel pendant la mise en service

1. **Charges** — création d'une charge réelle via la route HTTP `/factures-proprietaires/{id}/lignes/ajouter`
   (type CHARGE) : charge `CHG-a1db33663116` créée par le service canonique, liée à la facture,
   `mois`/`logement_id`/`proprietaire_id` correctement dérivés. Suppression de la ligne → charge
   **ANNULEE** (jamais supprimée), journal `charge_evenements` CREATION puis ANNULATION avec
   l'état avant. Réversibilité complète.
2. **Chaîne moteur complète** — rattrapage VRBO exécuté par le service canonique
   `regularisation_hh_service`, puis DAG RESERVATIONS → FLUX_UNIFIE → LOT10 → LOT11 → LOT12,
   toutes étapes `ok=True`, 1585 lignes / 1585 clés distinctes (aucun doublon).
3. **Intégrité** — `integrity_check` ok et `foreign_key_check` ok après chaque opération.

## Anomalies relevées pendant la recette

| # | Module | Route | Action | Résultat observé | Message (sanitisé) | Sévérité |
|--:|---|---|---|---|---|---|
| 1 | Réservations | — | rattrapage VRBO | `reservations_resolues.menage_retenu = 0` sur les 2 lignes issues d'une saisie HH, alors que `reservation_hh_overrides.menage = 55` et que **Lot10 compte bien 55 €** (assiette 130,99 / 316,97, commission 24,89 / 60,22 — exactes). Colonne d'affichage non alimentée pour les lignes HH ; même comportement sur la ligne HH héritée `RESHH-2025-02-001`. Économie juste. | — | FAIBLE (affichage) |
| 2 | Charges | — | — | Aucun écran de saisie de charge autonome : la seule voie de création est une ligne CHARGE sur un BROUILLON propriétaire. | — | MOYENNE (ergonomie) |
| 3 | Actualisation | `/actualisation` | affichage | **Le DAG est rouge à sa racine** : `HOSTAWAY_RAW` en ÉCHEC, donc RESERVATIONS / FLUX_LOT9 / LOT10 / LOT11 / LOT12 en `A_RECALCULER` (« amont en échec »). Cause réelle : **identifiants Hostaway absents** — la source ne peut pas être rafraîchie. **Les données affichées restent justes et à jour** (le rattrapage VRBO a rejoué toute la chaîne avec succès) ; c'est le registre de fraîcheur qui est bloqué, pas le calcul. | `MOTEUR_CODE_RETOUR / lot1_hostaway_extract rc=1` | MOYENNE — se résout en fournissant le `.env` Hostaway |
| 4 | Actualisation | `/actualisation/tout/dry-run` | dry-run | Fonctionne et produit un plan honnête (chaque étape « serait exécuté » / « serait ignoré (amont en échec) »). À noter : le dry-run **écrit** un run dans `moteur_runs`/`moteur_run_etapes` — l'empreinte de `app.db` change donc, sans qu'aucune donnée métier ne bouge. | — | INFO |
| 5 | | | | | | |

> **Note sur l'anomalie 3.** L'état stocké avant cette mission attribuait le blocage à
> `AttributeError: 'CompletedProcess' object has no attribute 'pid'` — un bug applicatif corrigé
> depuis (mission 14b). Une tentative réelle relancée pendant la mise en service a remplacé ce
> message périmé par la cause véritable (`rc=1`, identifiants absents). Le bug de la mission 14b est
> donc bien corrigé : **il ne se reproduit plus**.

## Actions à ne pas déclencher sans intention claire

Émettre une facture propriétaire · valider définitivement · clôturer un mois · régler ·
contrepasser · restaurer / rollback · « Actualiser toute l'activité » en réel.

## Arrêt / rollback

- Arrêt : stopper le process (PID donné dans le rapport).
- Rollback données : `backup_service.restaurer("BCK-F59FB3D90764")` (point d'avant-activation) ou
  `BCK-FA840A6A5752` (avant rattrapage VRBO). Copies externes dans
  `..\REAL_DATA_BACKUP_AVANT_RECETTE_20260910\`.
- Rollback code : copie propre `..\resume-pilotage-conciergerie-20260909_SAUVEGARDE_AVANT_MODE_REEL_20260910\`
  (intacte, jamais utilisée).

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
rem --- Identite legale (Kbis du 2026-09-10). SIRET et TVA intra volontairement absents. ---
set SOCIETE_NOM=CHOUETTE PATRIMOINE
set SOCIETE_FORME_JURIDIQUE=SAS
set SOCIETE_CAPITAL=200,00 €
set SOCIETE_ADRESSE=48E Route de Larnavey, 33650 Saint-Selve
set SOCIETE_SIREN=109624767
set SOCIETE_RCS=R.C.S. Bordeaux
set SOCIETE_REPRESENTANTS=Wafa Souci et Ewan Schmitt
rem --- Regime de TVA et conditions de reglement (confirmes le 2026-09-10) ---
set FACTURATION_REGIME_TVA=FRANCHISE_TVA
set FACTURATION_MENTION_FRANCHISE_TVA=TVA non applicable, art. 293 B du CGI
set FACTURATION_DELAI_PAIEMENT_JOURS=0
rem --- Clauses B2B : exigees uniquement face a un client PROFESSIONNEL ---
set FACTURATION_TAUX_PENALITES_RETARD=3 fois le taux d'interet legal (art. L441-10 du code de commerce)
set FACTURATION_INDEMNITE_RECOUVREMENT=40,00 € (art. D441-5 du code de commerce)
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
| Factures propriétaires `/factures-proprietaires` | OUI | **ACTIF** | **13 BROUILLON** (2026-08, montants réels) | **COMPLETEMENT TESTABLE** | À TESTER | parcours complet livré le 2026-09-10 (voir §Factures propriétaires ci-dessous). Émission = geste définitif, à faire en connaissance de cause |
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

## Factures propriétaires — parcours complet (livré le 2026-09-10)

Depuis la fiche d'un BROUILLON (`/factures-proprietaires/{id}`) :

| Étape | Où | À vérifier |
|---|---|---|
| Voir les séjours de la période | bloc « Séjours de la période et commission » | dates, canal, référence, **Assiette × Taux = Commission**. Aucune donnée personnelle du voyageur. |
| Voir la formule | bloc « Montant dû » | commissions + ménages + canapé + forfait + refacturations + extras − réductions − acomptes |
| Ajouter une **charge refacturable** | « Ajouter au brouillon » | seules les charges ACTIVE, refacturables, du bon propriétaire/logement et **non déjà facturées** sont proposées. Le montant vient de la charge. |
| Retirer une charge | bouton sur la ligne | la charge **redevient sélectionnable** et n'est **pas** annulée |
| Ajouter un **extra** | formulaire | montant strictement positif (un extra négatif est refusé) |
| Ajouter une **réduction** | formulaire | saisie en positif, déduite ; ne peut pas rendre la facture négative |
| Ajouter un **acompte** | bloc Règlement | diminue le **reste à payer**, jamais le montant facturé |
| **Prévisualiser** | bouton « Prévisualiser la facture (PDF) » | c'est le **PDF réel**, même moteur que le document final |
| Valider puis **émettre** | boutons de la fiche | numéro définitif, PDF figé, écriture comptable |
| Retrouver en **comptabilité** | bloc Comptabilité de la fiche + `/comptabilite/ecritures` | 411000 débit / 706000 crédit, liée à la facture |

**Forfait logiciel et consommables : UNE seule ligne.** Le référentiel
(`REF_Charges_Recurrentes.REC_001`) porte « Forfait client logiciel et consommables » d'un seul
tenant, calculé par Lot10 depuis `REF_Logements.forfait_logiciel_consommables_mensuel`. Les
scinder en deux postes inventerait une répartition que le référentiel ne porte pas.

**Acompte ≠ réduction.** Une réduction diminue ce qui est **facturé** (elle est une ligne de la
facture) ; un acompte est un **paiement déjà reçu** (mouvement de trésorerie, hors total facturé).
Les deux sont affichés séparément et comptabilisés différemment.

### Identité légale — renseignée depuis le Kbis (2026-09-10)

| Information | État | Variable |
|---|---|---|
| Dénomination | **CHOUETTE PATRIMOINE** | `SOCIETE_NOM` |
| Forme juridique | **SAS** (société par actions simplifiée) | `SOCIETE_FORME_JURIDIQUE` |
| Capital social | **200,00 €** | `SOCIETE_CAPITAL` |
| Siège social | **48E Route de Larnavey, 33650 Saint-Selve** | `SOCIETE_ADRESSE` |
| **SIREN** | **109 624 767** — affiché en trois groupes, **jamais** comme SIRET | `SOCIETE_SIREN` |
| Immatriculation | **R.C.S. Bordeaux** (immatriculée le 08/09/2026, activité depuis le 01/09/2026) | `SOCIETE_RCS` |
| **SIRET (14 chiffres)** | **NON FOURNI** — absent du Kbis, et **non déductible** du SIREN (il faut le NIC de l'établissement). **Jamais fabriqué.** | `SOCIETE_SIRET` (vide) |
| **TVA intracommunautaire** | **NON FOURNIE** — absente du Kbis. **Jamais inventée.** | `SOCIETE_TVA_INTRA` (vide) |

**SIRET et TVA intracommunautaire sont facultatifs** : leur absence ne bloque pas l'émission.
`EMETTEUR_REQUIS` porte sur dénomination + adresse du siège + SIREN — les trois sont renseignés,
l'identité de l'émetteur est donc **complète**. L'omission est conditionnelle, pas codée en dur :
le jour où ces numéros existent, ils s'impriment automatiquement, chacun sous sa propre étiquette.

Pied de page imprimé sur chaque facture :

```
CHOUETTE PATRIMOINE — SAS au capital de 200,00 EUR
48E Route de Larnavey — 33650 Saint-Selve
109 624 767 R.C.S. Bordeaux
```

### Régime de TVA et conditions de règlement — tranchés le 2026-09-10

| Décision | Valeur | Variable |
|---|---|---|
| Régime de TVA | **franchise en base** | `FACTURATION_REGIME_TVA=FRANCHISE_TVA` |
| Mention légale | « TVA non applicable, art. 293 B du CGI » | `FACTURATION_MENTION_FRANCHISE_TVA` |
| Conditions de règlement | **paiement à réception** — échéance = date d'émission | `FACTURATION_DELAI_PAIEMENT_JOURS=0` |
| Escompte | « Escompte pour paiement anticipé : néant » | `FACTURATION_CONDITIONS_ESCOMPTE` (défaut) |
| Pénalités de retard (B2B) | à renseigner — **aucun défaut dans le code** | `FACTURATION_TAUX_PENALITES_RETARD` |
| Indemnité de recouvrement (B2B) | à renseigner — **aucun défaut dans le code** | `FACTURATION_INDEMNITE_RECOUVREMENT` |

Les factures sont émises **sans TVA**, et **HT = TTC est garanti par construction** : hors
assujettissement, `taux_tva_applicable()` renvoie `0.0`, donc aucun arrondi ne peut faire diverger
les deux totaux. Ni la mention ni l'échéance ne sont écrites dans le gabarit PDF — toutes deux
viennent du service canonique, sans quoi un changement de régime laisserait la facture affirmer un
fondement juridique devenu faux.

`FACTURE_REGIME_TVA_NON_CONFIRME` et `FACTURE_ECHEANCE_NON_CONFIGUREE` sont **levés**.

**Reste à faire avant la première émission réelle** : **classer chaque propriétaire**
(particulier ou professionnel). L'application ne le devine pas — ni depuis le nom, ni depuis
l'adresse, ni depuis la présence d'un SIREN — et bloque l'émission tant que le type n'est pas
saisi, avec un message explicite sur la fiche. Les mentions légales obligatoires diffèrent entre
les deux, et l'indemnité forfaitaire de 40 € n'a pas à figurer sur la facture d'un particulier.

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

---

## Mise à jour du 2026-09-11 — après la recette utilisateur n°2

### Schéma et configuration

| Élément | Valeur |
|---|---|
| Schéma | **0075** (0074 périmètre analytique, 0075 refacturation partagée) |
| Numérotation des factures | `AAAA-MM-NNN` sur le **mois de prestation** (`2026-08-001`) |
| Accueil | supprimé — `/` redirige vers `/logements` |
| Module « Fournisseurs » | renommé **Charges** (l'entité fournisseur est conservée) |

### Nouvelles variables

```
SOCIETE_REPRESENTANTS=Wafa Souci et Ewan Schmitt
```
Imprimée « Représentée par … » sur les factures, **sans aucun titre juridique** : le Kbis n'en
documente pas, et en inventer un engagerait la société sur une qualité non vérifiée.

### Hostaway — emplacement exact des identifiants

Fichier **`.env` à la racine du projet**, modèle `.env.example` (également à la racine, créé par
cette mission). Variables : `HOSTAWAY_CLIENT_ID`, `HOSTAWAY_CLIENT_SECRET`, `HOSTAWAY_ACCOUNT_ID`,
`HOSTAWAY_BASE_URL`. Le fichier est gitignoré — il n'est donc jamais copié dans un nouveau
worktree, ce qui explique l'absence constatée en recette. L'écran `/hostaway` indique désormais
quelles variables manquent et où les déposer, sans jamais afficher de valeur.

### Ce que la recette a réellement corrigé

1. le périmètre analytique d'une charge est **persisté** (il était calculé puis jeté) ;
2. une charge multi-logements est **refacturable** (sa position naissait `A_TRAITER`, invisible) ;
3. une charge **peut être validée** (`A_CONTROLER` → `CONFORME`) ;
4. la numérotation ne dépend plus d'une date saisie (`F-11/0-000001` ne peut plus se reproduire) ;
5. Créances : `Total − Réglé − Compensé = Solde`, plus aucun montant invisible ;
6. une facture **VALIDE** peut repasser en brouillon ; une facture **ÉMISE** ne le peut pas ;
7. `apply_migrations` ne rejoue plus tout l'historique — il empêchait le démarrage dès l'ajout
   d'une migration.

### Ce qui reste à la main de l'utilisateur

- **classer chaque propriétaire** (particulier / professionnel) avant émission définitive ;
- décider du sort de la facture `F-11/0-000001`, émise sous l'ancien format (avoir + réémission,
  ou conservation en l'état) — aucune modification automatique n'a été faite ;
- renseigner le `.env` Hostaway pour réactiver l'actualisation.

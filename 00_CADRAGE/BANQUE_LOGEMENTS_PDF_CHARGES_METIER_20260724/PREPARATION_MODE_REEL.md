# Préparation technique du mode réel (2026-08-10)

> **MISE À JOUR (2026-08-10, suite) — LE VERDICT REPASSE EN NO GO.**
> La clarification des contrôles bloquants (cf. `RECETTE_FONCTIONNELLE_GLOBALE.md`) établit que
> **2357 lignes empêchent réellement la clôture** (959 de sévérité BLOQUANT + 1399 A_CONTROLER,
> toutes porteuses de `impact_cloture = "Bloque la clôture"`). Ce ne sont pas des défauts
> applicatifs mais des **lacunes de données métier** ; elles n'en bloquent pas moins toute clôture.
> Basculer un writer en écriture réelle sur un périmètre dont aucun mois ne peut être clôturé
> serait prématuré. **Préparation mode réel : NO GO** jusqu'à traitement de ces familles.
>
> **Second constat de cet audit : le contrat de sécurité demandé existe déjà pour l'essentiel.**
> L'audit des 9 writers (ci-dessous, section 2bis) montre que la double garde, le défaut
> fail-closed, le write-guard de chemin, le backup pré-écriture, la prévisualisation et la
> confirmation sont **déjà implémentés**. Construire un second mécanisme de permission
> contreviendrait à l'instruction explicite de la mission (« ne crée pas un troisième système de
> permission parallèle », « n'invente pas si l'architecture actuelle peut être étendue »).
> **Aucune modification de `app/config.py` n'a donc été faite** — ce qui manque n'est pas un
> mécanisme de sécurité, c'est l'**état « écriture réelle »** lui-même, délibérément jamais
> construit, dont la création doit rester une décision humaine explicite et revue.

**Ce document ne déclenche aucune activation.** Il documente ce qui existe, dans quel ordre une
activation future devrait se faire, et ce qui la bloque aujourd'hui. Aucun flag n'a été modifié,
aucun writer n'a été activé, le mode réel reste NO GO.

## 1. Découverte structurante — l'activation n'est PAS un simple changement de variable

Audit direct de `05_APPLICATION/app/config.py` : **tous les writers sont doublement verrouillés**,
et la majorité sont conditionnés par `RECETTE_MODE` :

```python
CHARGES_REAL_WRITE_ENABLED       = RECETTE_MODE and _env_flag("CHARGES_REAL_WRITE_ENABLED")
BANQUE_REAL_WRITE_ENABLED        = RECETTE_MODE and _env_flag("BANQUE_REAL_WRITE_ENABLED")
FACTURES_REAL_WRITE_ENABLED      = RECETTE_MODE and _env_flag("FACTURES_REAL_WRITE_ENABLED")
MENAGES_CYCLE_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("MENAGES_CYCLE_REAL_WRITE_ENABLED")
COMPTABILITE_REAL_WRITE_ENABLED  = RECETTE_MODE and _env_flag("COMPTABILITE_REAL_WRITE_ENABLED")
CALCULS_REAL_RUN_ENABLED         = RECETTE_MODE and _env_flag("CALCULS_REAL_RUN_ENABLED")
MENAGES_REAL_RECALC_ENABLED      = RECETTE_MODE and _env_flag("MENAGES_REAL_RECALC_ENABLED")
```

**Conséquence directe** : dans une instance NON recette (`RECETTE_MODE=0`, c'est-à-dire une
instance « réelle »), **aucun de ces writers ne peut être activé**, quelles que soient les
variables d'environnement positionnées. Positionner `CHARGES_REAL_WRITE_ENABLED=1` sur une
instance réelle ne produit rien : l'expression retombe sur `False`.

Trois writers sont même **codés en dur à `False`**, sans aucun mécanisme d'activation :

```python
HH_REAL_WRITE_ENABLED            = False   # réservations hors Hostaway
REF_ASSOC_MODE_REAL_WRITE_ENABLED = False  # migration REF_Assoc_Mode
CONTROLES_REAL_WRITE_ENABLED     = False   # suivi humain des contrôles
```

**L'activation du mode réel exige donc une modification délibérée de `app/config.py`** — ce n'est
pas une opération de configuration, c'est un changement de code, à faire sous revue, writer par
writer. C'est un choix de conception protecteur (impossible d'activer le réel par accident ou par
une variable d'environnement mal placée), pas un défaut. Il doit être connu avant toute
planification de bascule.

## 2. Inventaire des writers réellement existants

| Writer | Fonction | Flag | Risque | Prérequis | Test avant | Test après | Rollback |
|---|---|---|---|---|---|---|---|
| Charges | Écrit `SAISIE_Charges_Flux.xlsx` (source métier Excel) | `CHARGES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` (recette-gated) | **ÉLEVÉ** — touche une source métier réelle | Backup + hash `SAISIE_Charges_Flux.xlsx` ; mois ouvert | `test_charges_confirmation_e2e/route/audit` verts | Relire la charge créée, vérifier montant/catégorie/impact, relancer Lot3 | Restaurer le `.xlsx` depuis le backup horodaté |
| Réservations hors Hostaway | Écrit `SAISIE_ReservationsHorsHostaway.xlsx` | `HH_REAL_WRITE_ENABLED` — **codé en dur `False`** | **ÉLEVÉ** — source métier réelle | Modification de `config.py` obligatoire ; backup + hash | `test_reservations_hh`, `test_saisie_hh_*` verts | Relire la réservation, vérifier propriétaire/taux/ménage résolus, relancer lot4quater→lot13 | Restaurer le `.xlsx` |
| Factures / Règlements | SQLite applicatif (`factures`, `reglements`) | `FACTURES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul, aucun fichier métier | Backup `app.db` | `test_factures*`, `test_reglements*` verts | Vérifier solde/statut dérivé/historique | Restaurer `app.db` |
| Ménages (cycle) | SQLite applicatif (`menages_cycle`) | `MENAGES_CYCLE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul | Backup `app.db` | `test_menages_cycle*` verts | Vérifier statut/historique | Restaurer `app.db` |
| Comptabilité | SQLite applicatif (écritures, périodes) | `COMPTABILITE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul ; **mappings comptables encore PROVISOIRES** | Backup `app.db` ; **arbitrage des comptes (`70`) fortement recommandé avant** | `-k comptabilite` vert (142) | Vérifier équilibre débit=crédit, balance, période | Contrepassation (jamais de suppression) + restauration `app.db` |
| Banque (contrôle/catégorisation) | Écrit un onglet `OVERRIDE_APP4B` dans une **copie** de `BANQUE_LOT8_IMPORT.xlsx` (workspace isolé sous `data/`) | `BANQUE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | FAIBLE — travaille sur copie par conception, jamais sur les onglets moteur | Backup `app.db` | `-k banque` vert (612) | Vérifier décision + historique append-only | Restaurer `app.db` ; la copie est régénérable |
| Calculs (runs pipeline) | Exécute les lots sur l'arborescence métier | `CALCULS_REAL_RUN_ENABLED` + `_CONFIRMATION_` | **ÉLEVÉ** — régénère les MASTER réels | Backup complet `02_TRAVAIL` + `03_EXPORTS` | `test_calculs*` verts ; run identique sur copies | Comparer avant/après, vérifier idempotence | **Rollback natif intégré** : bouton « Restaurer les sorties d'avant ce run » (exercé ce tour : 8 fichiers restaurés) |
| Ménages (recalcul sources) | Régénère Lot6b/6c (atteint le réseau : feuille Google) | `MENAGES_REAL_RECALC_ENABLED` | **ÉLEVÉ** — accès réseau + données réelles | Workspace contrôlé obligatoire (`menages_chaine_service`) | Chaîne ménages verte sur copies | Vérifier 4 flux séparés | Restaurer les MASTER ménages |
| Suivi des contrôles | SQLite applicatif (suivi humain) | `CONTROLES_REAL_WRITE_ENABLED` — **codé en dur `False`** | FAIBLE — journalise seulement, ne masque jamais le moteur | Modification de `config.py` obligatoire | `-k controle` vert | Vérifier que l'anomalie moteur reste visible | Restaurer `app.db` |

**Aucun writer n'a été créé, modifié ou activé par cette mission.**

## 2bis. Audit détaillé des 9 writers (2026-08-10) — le contrat de sécurité est déjà en place

| Writer | Module | Cible réelle | Flag | RECETTE_MODE requis ? | Hardcodé ? | Risque |
|---|---|---|---|---|---|---|
| Charges | Charges | `SAISIE_Charges_Flux.xlsx` (fichier métier) | `CHARGES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | ÉLEVÉ |
| Réservations HH | Réservations | `SAISIE_ReservationsHorsHostaway.xlsx` (fichier métier) | `HH_REAL_WRITE_ENABLED` | — | **oui, `False`** | ÉLEVÉ |
| Banque contrôle | Banque | **copie** de `BANQUE_LOT8_IMPORT.xlsx` (onglet `OVERRIDE_APP4B`, workspace isolé sous `data/`) | `BANQUE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | FAIBLE (copie par conception) |
| Banque import | Banque | `NORM_Banque` via fichier temporaire | `BANQUE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | ÉLEVÉ |
| Factures / Règlements | Factures | SQLite applicatif uniquement | `FACTURES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | MOYEN |
| Ménages (cycle) | Ménages | SQLite applicatif uniquement | `MENAGES_CYCLE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | MOYEN |
| Comptabilité | Comptabilité | SQLite applicatif uniquement | `COMPTABILITE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | **oui** | non | MOYEN |
| Calculs (runs) | Calculs | MASTER de `02_TRAVAIL` + `03_EXPORTS` | `CALCULS_REAL_RUN_ENABLED` + `_CONFIRMATION_` | **oui** | non | ÉLEVÉ |
| Suivi contrôles | Contrôles | SQLite applicatif uniquement | `CONTROLES_REAL_WRITE_ENABLED` | — | **oui, `False`** | FAIBLE |
| *(bonus)* REF_Assoc_Mode | Référentiel | `REF_Setup.xlsm` | `REF_ASSOC_MODE_REAL_WRITE_ENABLED` | — | **oui, `False`** | ÉLEVÉ |

### Ce qui existe déjà (vérifié dans le code, pas déduit)

- **Double garde** : `X_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("X_REAL_WRITE_ENABLED")` —
  contexte **et** flag individuel requis, exactement le contrat demandé (§4/§5 de la mission).
- **Défaut fail-closed** : `_env_flag()` retourne `False` pour toute variable absente ; aucun
  writer ne s'active par omission, ni en développement, ni via une route HTTP.
- **Second verrou de confirmation** : chaque writer a un flag `_CONFIRMATION_ENABLED` distinct.
- **Write-guard de chemin** (`app/recette_guard.py`) : barrière indépendante des flags, explicitement
  « fail-closed », qui refuse toute écriture hors `RECETTE_ROOT` en mode recette et interdit en dur
  les segments `01_SOURCES_BRUTES`, `02_TRAVAIL`, `03_EXPORTS`.
- **Backup pré-écriture** : présent (sauvegardes horodatées, vérifiées lors des missions Banque et
  Calculs — « sera sauvegardé avant écrasement » affiché en prévisualisation).
- **Prévisualisation + confirmation explicite** : exercées réellement sur Charges, Réservations HH,
  Trésorerie, Banque, Comptabilité, Calculs.
- **Écriture transactionnelle et journalisée** : historiques append-only vérifiés (factures,
  trésorerie, classement bancaire, périodes comptables).
- **Rollback** : natif et exercé pour Calculs (8 fichiers restaurés) ; par restauration `app.db`
  pour les writers SQLite ; par backup horodaté pour les writers fichier.

### Ce qui n'existe pas — et pourquoi ce n'est pas un défaut

Il n'existe **aucun état « écriture réelle »**. Ce n'est pas un oubli : c'est la conséquence directe
du choix `RECETTE_MODE and ...`. Les trois états prévus par la mission se lisent ainsi :

| État | Existe ? | Comportement |
|---|---|---|
| A — RECETTE | **oui** | Writers activables, cibles exclusivement sous `RECETTE_ROOT`, write-guard actif |
| B — LECTURE RÉELLE | **oui** (état par défaut aujourd'hui) | Tous les writers `False`, aucune écriture possible |
| C — ÉCRITURE RÉELLE | **non, jamais construit** | Exigerait de modifier `app/config.py` |

**Recommandation : ne pas créer l'état C tant que le verdict est NO GO.** Le créer maintenant
reviendrait à retirer la protection qui garantit aujourd'hui qu'aucune écriture réelle accidentelle
n'est possible, sans qu'aucune écriture réelle ne soit encore souhaitée ni sûre (2357 bloqueurs de
clôture ouverts, mappings comptables non arbitrés, validation humaine partielle).

### Les 3 hardcodes à `False` — analyse individuelle

| Writer | Cause réelle | Verdict |
|---|---|---|
| `HH_REAL_WRITE_ENABLED` | Écrit une **source métier Excel**. Le module est fonctionnellement validé (LOT A ACCEPTE_AVEC_RESERVE) et couvert par des tests verts. Le hardcode est une **protection volontaire**, pas une fonction inachevée | **PRÊT fonctionnellement, MAIS laisser `False`** — son activation relève de l'état C, à créer sous revue |
| `REF_ASSOC_MODE_REAL_WRITE_ENABLED` | Migration ponctuelle d'un référentiel, écrit `REF_Setup.xlsm`. Opération one-shot, jamais exercée en réel | **NON_ACTIVABLE** — laisser `False` |
| `CONTROLES_REAL_WRITE_ENABLED` | Journalise le suivi humain (SQLite seul, ne masque jamais le moteur). Fonctionnellement exercé en recette | **PRÊT fonctionnellement, MAIS laisser `False`** — même raison que HH |

Aucun hardcode n'a été levé par cette mission. Conformément à l'instruction : « ne rends jamais
activable un writer incomplet juste pour uniformiser la config » — et, ici, ne pas lever non plus
un writer *complet* tant que le contexte global est NO GO.

## 3. Ordre d'activation proposé (technique, non exécuté)

Ordre dérivé des dépendances réelles constatées, du risque le plus faible au plus élevé — et non
de la simple existence d'un writer. Un writer ne doit être activé que s'il correspond à un besoin
métier réel exprimé.

0. **Backup complet + manifest SHA256** (cf. §4). Point de restauration nommé et daté.
1. **Contrôle de santé en lecture seule** : instance réelle démarrée sans aucun writer, parcours de
   consultation des 18 modules, aucune erreur.
2. **Contrôle des migrations** : `test_sqlite_migrations` vert, migrations idempotentes vérifiées.
3. **Writers SQLite purs d'abord** (aucun fichier métier touché, rollback = restaurer `app.db`) :
   a. Suivi des contrôles (le plus inoffensif : journalise, ne décide rien) ;
   b. Banque contrôle/catégorisation (travaille sur copie par conception) ;
   c. Factures / Règlements ;
   d. Ménages (cycle).
4. **Comptabilité** — **seulement après arbitrage des mappings comptables** (`70`), sinon les
   écritures produites porteraient des comptes provisoires en production.
5. **Writers touchant des sources métier Excel** (risque élevé, un seul à la fois, avec backup
   dédié avant chacun) : Charges, puis Réservations hors Hostaway.
6. **Calculs (runs réels)** en dernier : il régénère tout l'aval. Son rollback natif est déjà
   éprouvé, mais il a le plus large rayon d'action.
7. **Ménages (recalcul sources)** : à traiter séparément, car il atteint le réseau — exige le
   workspace contrôlé, jamais une exécution directe.

Entre chaque activation : une seule opération réelle de faible montant, contrôle, puis décision
explicite de continuer ou de restaurer.

## 4. Procédure backup / rollback (commandes préparées, non exécutées sur le réel)

Périmètre à sauvegarder avant toute activation :

- `05_APPLICATION/data/app.db` (base applicative réelle) ;
- `01_SOURCES_BRUTES/` en entier (REF_Setup, Banque, Charges, ReservationsHH, AcomptesProprietaires,
  AirCover, ImputationsAirbnb, AjustementsPostCloture, VRBO, MenagesExternes) ;
- `02_TRAVAIL/` (tous les MASTER : Lot1, Lot3, Lot4*, Lot5, Lot6*, Lot7, Lot8, Lot9, Lot10, Lot11,
  Lot12) ;
- `02_DONNEES_NORMALISEES/` ;
- `03_EXPORTS/` ;
- manifest SHA256 de l'ensemble.

Le script de baseline utilisé par cette mission est réutilisable tel quel comme générateur de
manifest (950 fichiers couverts, SHA256 + taille + mtime, sortie TSV hors arbre métier).

Procédure :

1. **Point de restauration** : copier l'arborescence ci-dessus vers `_BACKUPS_MODE_REEL/<horodatage>/`,
   produire le manifest SHA256, vérifier que le manifest est relisible.
2. **Arrêt** : arrêter l'instance applicative réelle (le port 8000/PID 21136 actuel est un processus
   historique de l'utilisateur — ne jamais le tuer sans son accord explicite).
3. **Restauration** : recopier les fichiers depuis le backup, restaurer `app.db`, revérifier le
   manifest SHA256 (comparaison stricte, 0 différence attendue).
4. **Contrôle post-restauration** : relancer l'application en lecture seule, vérifier les totaux
   de référence (REEL = COMPTABLE + HORS_COMPTA, écart 0,00 €) et l'absence d'objet orphelin.
5. **Reprise** : ne réactiver aucun writer tant que la cause de la restauration n'est pas comprise
   et documentée.

## 5. Conditions GO / NO GO

**Conditions GO (toutes requises) :**
- fiche de validation humaine remplie et signée par l'utilisateur pour les 16 modules ;
- arbitrages comptables rendus (`70` : 606000, frais bancaires, trésorerie propriétaires,
  associés/IK) — au minimum ceux qui concernent les écritures qui seront réellement produites ;
- modification revue de `app/config.py` levant le verrou `RECETTE_MODE` pour le seul writer visé ;
- backup complet + manifest SHA256 réalisés et vérifiés ;
- procédure de rollback testée au moins une fois de bout en bout ;
- campagne de tests verte au moment de la bascule.

**Conditions NO GO (une seule suffit) :**
- validation humaine non signée ;
- mappings comptables non arbitrés si le writer Comptabilité est concerné ;
- backup absent, incomplet ou non vérifié ;
- rollback non testé ;
- anomalie BLOQUANT ouverte sur le périmètre concerné ;
- activation de plusieurs writers simultanément.

**Statut actuel : NO GO** — trois causes cumulatives, chacune suffisante :
1. **2357 lignes bloquent réellement la clôture** (lacunes de données métier, 9 familles de
   codes, aucune résolue automatiquement) — aucun mois n'est clôturable aujourd'hui ;
2. arbitrages comptables non rendus (`70` : 606000, frais bancaires, trésorerie propriétaires,
   associés/IK) ;
3. l'état « écriture réelle » n'existe pas et ne doit pas être créé tant que 1 et 2 tiennent.

Validation humaine : LOT A à E désormais tous signés par l'utilisateur (2026-08-10) — ce point
n'est plus bloquant, les deux autres le restent.

**Mise à jour 2026-08-10 (suite)** : première écriture réelle contrôlée de la mission, exécutée
strictement selon la procédure décrite en §4 (backup, SHA256, prévisualisation, écriture, relecture,
contrôle d'intégrité global). `REF_Setup.xlsm` (`REF_Gestion_Logements_Hist`) modifié sur décision
utilisateur explicite : 14 logements, `date_debut` prolongée à 2025-08-01. Intégrité vérifiée :
1 seul fichier modifié sur 950, exactement celui attendu. `GESTION_LOGEMENT_MISSING` réduit de
838 à 472 lignes (mesuré sur copies avec un diff identique, non recalculé sur le réel — le writer
Calculs reste désactivé). **Cause 1 du NO GO partiellement traitée, causes 1 (reste 77 couples)
et 2 (arbitrages comptables) demeurent → préparation mode réel reste NO GO.**

**Mise à jour 2026-08-10 (suite, RESERVATION_A_CONTROLER_SANS_COMMISSION)** : audit complet, aucune
écriture nécessaire — le référentiel `REF_Taux_Commission` couvre déjà exactement la règle utilisateur
(2025-01-01→2026-01-31 à 15 % pour tous, taux spécifique dès le 01/02/2026 quand il diffère). Les
612 lignes bloquantes sont causées par `GUEST_COUNT_MANQUANT` (553) et un statut de payout non
résolu pour des réservations VRBO/Direct (59) — deux causes indépendantes du taux, non traitées
ici. Aucun impact sur le statut NO GO.


**Mise a jour 2026-08-10 (suite, GUEST_COUNT_MANQUANT_PREPARATION_CANAPE)** : audit complet, code
deja correct (correctif du 20/06/2026, numberOfGuests). Cause reelle : aucune re-extraction
Hostaway reelle depuis ce correctif (le fichier source a guestCount vide sur 100% des
reservations et ne porte pas la colonne d'audit ajoutee par le correctif). Aucune source locale
fiable pour reconstruire - la seule voie est une re-extraction Hostaway reelle (writer reel,
acces reseau externe), non entreprise, question posee a l'utilisateur. 553 inchange. Aucun
impact sur le statut NO GO.

**Mise a jour 2026-08-10 (suite, re-extraction Hostaway reelle executee)** : autorisation
utilisateur explicite et strictement scopee (lecture API reelle + nouveau
MASTER_FACT_HA_Reservations.xlsx + remplacement controle de ce seul fichier - PAS une activation
generale du mode reel, PAS d'autorisation Banque, PAS de pipeline aval reel). Deuxieme ecriture
reelle controlee de la mission, procedure identique a la premiere (backup, hash, comparaison
exhaustive verifiee y compris par appel API direct sur les 17 reservations disparues, simulation
sur copie integrale du projet, gate, remplacement atomique, relecture, integrite globale).
Resultat mesure en simulation : GUEST_COUNT_MANQUANT 553->506 (-47), mecanisme verifie a 100%
(les 506 restantes sont 100% en mois cloture/gele par design, resolution complete de ce qui etait
techniquement atteignable par API). Master reel remplace (1391->1527 reservations, hash relu
identique a la source). Integrite : 950/950 fichiers, 2 diffs attendus. Port 8000/PID 21136
intact. Tests cibles : 62 passed, 0 nouvel echec. **Le pipeline aval reel n'a pas ete relance : le
chiffre de cloture officiel reste 553 tant que ce run n'est pas rejoue. Aucun flag app/config.py
modifie. Statut NO GO inchange.**

**Mise a jour 2026-08-11 (suite, correctif lot4ter + correction reelle des 506 guestCount
historiques cloturees)** : decision utilisateur, correction retroactive CIBLEE (pas de reouverture
globale, pas de recalcul aveugle depuis le live, pas de synchronisation LIVE->HIST generale, pas
d'autorisation Banque, pas d'autorisation mode reel general). Bug reel trouve pendant l'audit
d'impact : `lot4ter_historiser_reservations_cloturees.py` reconstruisait HIST depuis une liste
`COLS` fixe sans `guestCount` — toute correction de ce champ etait effacee au run normal suivant.
Corrige minimalement (2 lignes), test rouge->vert
(`tests/test_lot4ter_guestcount_persistence.py`), regression complete 272+432 passed, 0 nouvel
echec. **Committe (`9a0a6aa`) avant toute donnee reelle**, conformement a la regle. Troisieme
ecriture reelle controlee de la mission (HIST_Reservations_Cloturees.xlsx), procedure identique
aux precedentes : backup horodate + hash verifie, script de correction dedie fail-closed (refuse
tout champ hors `guestCount`, toute reservation hors liste explicite, toute valeur invalide,
toute ecriture reelle sans variable d'environnement dediee explicite), 506 valeurs ajoutees, 1269
lignes inchangees, 0 diff sur les 28 colonnes existantes (verifie programmatiquement), relecture
confirmee. Journal append-only cree. Preuve de persistance sur copie (run normal de lot4ter
corrige apres correction : guestCount survit), idempotence et rollback verifies avant l'ecriture
reelle. Integrite : 950/950, 3 diffs attendus. Port 8000/PID 21136 intact. **Pipeline aval reel
non relance (interdit explicitement) : les resultats de cloture reels n'integrent pas encore la
correction. Aucun flag `app/config.py` modifie. Statut NO GO inchange.**

**Mise a jour 2026-08-11/12 (suite, audit des 70 RESERVATION_EXCLUE_A_CONTROLER VRBO/Direct)** :
bug reel trouve (pas une donnee reelle a corriger cette fois) : `lot10_calculer_resultats.py`
construisait l'onglet A_CONTROLER directement depuis le statut brut Lot1, sans verifier si la
reservation avait deja ete resolue via un mecanisme existant (backfill VRBO, saisie HH Direct
deja validee D054). 28 reservations (27 VRBO + 1 Direct) etaient deja comptees dans COMMISSIONS
mais listees en double dans A_CONTROLER. Corrige minimalement (5 lignes), test rouge->vert
(`tests/test_lot10_reservation_exclue_dedup.py`), regression complete 274 passed, 0 nouvel echec.
**AUCUNE ecriture reelle cette fois** : le bug etait dans la construction d'un onglet de
reporting, pas dans une donnee source — aucune correction de donnee n'etait eligible (0
PREUVE_A sur les 42 restantes, qui necessitent une saisie manuelle humaine, pas une correction de
champ existant). Simulation : `RESERVATION_A_CONTROLER` 70->42, delta resultat societe 0,00 EUR
(les 28 etaient deja comptees). Port 8000/PID 21136 intact. Statut NO GO inchange.

**Mise a jour 2026-08-12 (mission de nuit — baseline canonique fraiche avec Banque reelle)** :
simulation reconstruite depuis HEAD `6b60577` avec Banque reelle regeneree sur copie (pipeline
`lot8a/8b/8c` deja valide rejoue, 0 stub, 0 reaudition metier, 0 matching Banque-Reservation).
**Decouverte determinante : les 541 BLOQUANT actuels sont exactement et uniquement
GESTION_LOGEMENT_MISSING (472) + CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE (69, verifie
programmatiquement comme sous-ensemble strict a 100% des 77 couples gestion, meme cause
racine).** Aucun autre BLOQUANT n'existe dans le systeme. Audit complet des residuels : 0
nouveau bug technique trouve, 0 code modifie cette nuit. 42 Direct/VRBO : requete API Hostaway
en direct refaite, 0 nouvelle preuve (paymentStatus=Unknown confirme frais). Banque fraiche :
541 mouvements, 236 classes, 222 rapprochement humain (166 Airbnb + 56 proprietaires), 83 file
assistee, statut Lot11 passe a BANQUE_DISPONIBLE. REEL/COMPTABLE/HORS_COMPTA avec Banque reelle :
313756,48/303232,32/10524,16 EUR, ecart 0,00. Idempotence verifiee. Integrite : 950/950, 3 diffs
deja committes, **0 nouvelle modification reelle cette nuit**. Port 8000/PID 21136 intact. **3
decisions humaines ferment tout ce qui reste : historique gestion 2025 (resout les 541 BLOQUANT
d'un coup), 42 saisies Direct/VRBO, 222+83 mouvements bancaires. Statut NO GO inchange — toutes
les corrections deterministes de code sont epuisees.**

## Mise a jour 2026-08-12 — decision (1) appliquee, 541 BLOQUANT -> 0

Decision utilisateur recue et appliquee : meme proprietaire par logement sur toute 2025.
REF_Gestion_Logements_Hist prolonge 2025-08-01 -> 2025-01-01 (14 logements). Simulation
canonique fraiche (Banque reelle incluse) : GESTION_LOGEMENT_MISSING 472->0,
CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE 69->0, BLOQUANT total 541->0. Delta economique 0,00 EUR.
Reel modifie (14 cellules date_debut, backup+SHA256 verifies), pipeline reel non relance,
integrite 3/950 diffs (attendus), 24/24 tests cibles verts.

**Restes : 2 decisions humaines** (au lieu de 3) : 42 saisies Direct/VRBO, 222+83 mouvements
bancaires. Statut **NO GO inchange** — corrections deterministes de code toujours epuisees.

## Mise a jour 2026-08-12 (2) — vraie file humaine finale

Ventilation Banque exacte : 236 CLASSE (0 action), 166 PAYOUT_PLATEFORME (0 decision, categorie
deja correcte, exclue du rapprochement reservation dans l'app, conforme 78877da), 56 proprietaires
(1 prerequis: alimenter Lot 5), 82 A_ENVOYER_IA distincts (83 lignes physiques, 1 doublon
mouvement_id verifie conforme). Vrai total decisions Banque : 138 (pas 305). Reservations : 42
(38 Direct + 4 VRBO), mecanisme existant audite + preuve fixtures 58/58 verts. 14 A_CONTROLER : 3
seulement nouveaux (Menages/provenance), 11 = reformulations des blocs Reservations/Banque deja
comptes. Mappings comptables (606000 generique) : non bloquants techniquement, reserve acceptable.
0 code modifie, 0 bug trouve. Detail complet : 82_PACK_FINAL_ACTIONS_HUMAINES.md.

**PREPARATION MODE REEL : NO GO** — 138 decisions Banque + 42 saisies Reservations restent a
traiter humainement, rollback reel jamais exerce hors recette. **MODE REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 — audit Lot 5, file humaine finale

Lot 5 FONCTIONNEL mais NON ALIMENTE : 0 ligne dans SAISIE_AcomptesProprietaires.xlsx, 0 dans sa
sortie, 0 dans mouvements_tresorerie_proprietaires. Les 56 mouvements proprietaires sont donc tous
sans preuve (PREUVE_A=0, ABSENT=56), aucun rapprochement possible faute d'objet a rapprocher.
56 -> 56 decisions humaines (plancher 7 si nature uniforme par proprietaire, a confirmer).
82 A_ENVOYER_IA -> 37 decisions si reponse par regle (12 regles candidates couvrant 57 mouvements),
0 correspondance deterministe avec les referentiels existants.

File humaine finale dedupliquee : 183 objets economiques, **89 a 183 decisions humaines** selon
les reponses de compression. Detail et questions : 82_PACK_FINAL_ACTIONS_HUMAINES.md.

Nouveau prerequis technique : migrations 0017->0026 a appliquer a la base reelle (actuellement
0016) avant exploitation de la tresorerie proprietaires.

**PREPARATION MODE REEL : NO GO. MODE REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 (2) — migration DB repetee, chaine Lot 5 fiabilisee

Le principal risque technique restant est leve : la migration app.db 0016->0026 a ete **repetee
integralement sur copies** (sequentielle une par une, automatique, idempotence, rollback), avec
0 perte de donnees et hash exact restitue au rollback. La base reelle n'a **pas** ete migree
(hash identique, toujours 0016). Runbook pret et non execute : 85_RUNBOOK_MIGRATION_APP_DB_REELLE.md.

Un bug reel a ete trouve et corrige au passage (commit 1759ce0) : lot8c n'ouvrait jamais le master
Lot 5 tout en affirmant qu'il etait vide — alimenter Lot 5 puis relancer n'avait aucun effet
observable. Corrige de facon minimale, sans inventer de regle de rapprochement.

Reste apres cette mission : uniquement des **decisions metier** (nature des encaissements
proprietaires, identite FAMILLE_UZON, classification des 82, montants des 42) et deux arbitrages
(regles de rapprochement Lot5<->Banque, mappings comptables fins). Plus aucune question technique
ouverte sur la migration ni sur l'exploitabilite de Lot 5.

**PREPARATION MODE REEL : NO GO** (donnees humaines non traitees). **MODE REEL : NO GO — NON ACTIVE.**

# Recette utilisateur n°3 — continuation finale

**Architecture Hostaway, relevé propriétaire, correspondances logement, pricing dynamique**

Date : 2026-09-12 · Branche : `resume/pilotage-conciergerie-20260909`

---

## A. L'architecture Hostaway réelle

Le rapport précédent classait `lot6a` en « NON — identifiants Hostaway locaux absents ». Ce
diagnostic était faux par insuffisance : il décrivait un manque sans regarder ce qui existait.

Il existe un pipeline GitHub Actions qui **fonctionne**.

| Élément | Valeur constatée |
|---|---|
| Dépôt | `eschmitt27/hostaway-reporting` — qui est **aussi `origin` du projet** |
| Workflow | `.github/workflows/pipeline.yml`, nom « Hostaway Pipeline », branche `main` |
| Déclenchement | `workflow_dispatch` **et** `schedule: cron '0 6,12,18 * * *'` — 3 fois par jour |
| Secret consommé | **un seul** : `secrets.HOSTAWAY_TOKEN` (Bearer) |
| Scripts | `extract_reservations.py` → `/v1/reservations` · `extract_finance_fields.py` → `/v1/financeField/{id}` · `build_final_report.py` (jointure) |
| Permissions | `contents: write` |
| Dernier succès | commit `ab52ab4`, **2026-09-12 14:35:42 UTC** |

**Le secret n'est pas celui que le poste réclamait.** Le pipeline utilise un jeton unique
(`HOSTAWAY_TOKEN`) ; le `.env.example` local demandait le triplet OAuth2
`CLIENT_ID`/`CLIENT_SECRET`/`ACCOUNT_ID`. Deux modèles d'authentification différents pour la même
plateforme — ce qui explique qu'aucune recopie de secret n'aurait de toute façon suffi.

---

## B. Comment les données sont publiées — réponse factuelle

Sur les six hypothèses posées (artifact, branche, release, storage, autre) : **c'est A — commitées
dans le repository.**

L'étape finale du workflow est littéralement :

```
git add -f *.tsv
git diff --cached --quiet || git commit -m "Automated data refresh"
git push
```

**Conséquence directe, et c'est la plus importante : aucun jeton GitHub supplémentaire n'est
nécessaire.** Le dépôt de données EST le dépôt du projet. L'authentification qui fonctionne déjà
pour le code fonctionne pour la donnée — `git fetch origin main` s'exécute sans invite. Il n'y a
donc rien à demander à l'utilisateur au titre du §9.

Historique observé, pour montrer que le pipeline n'est pas dormant :

```
ab52ab4  2026-09-12T14:35:42Z   003fb48  2026-09-12T09:45:16Z
b334fdf  2026-09-11T20:12:11Z   65c9d1c  2026-09-11T15:28:13Z
8ea3b9f  2026-09-11T10:10:33Z   a238c6c  2026-09-10T20:13:02Z
```

---

## C. Matrice de couverture Hostaway

| Donnée | GitHub la produit ? | API locale la produisait ? | Nécessaire ? | Source canonique retenue |
|---|---|---|---|---|
| listings (`/v1/listings`) | non — **déduits** des réservations | oui | non | dépôt, champs inconnus laissés **NULL** |
| réservations | **oui** — 1 962 | oui — 1 585 | oui | **dépôt GitHub** |
| détails réservation | non | oui (appel conditionnel) | non — **0 appel** nécessaire depuis le dépôt | dépôt |
| finance fields | **oui** — 16 540 | oui — 2 996 | oui | **dépôt GitHub** |
| fees (`reservationFees`) | non | oui | non — 0 fee sur l'import réel | — |
| payouts | calculés par le pipeline (`TotalPayout`) | calculés par le moteur | oui | **moteur local** (règle métier) |
| calendrier | non | non | non demandé | — |
| **Cleaning Tasks** (`/v1/tasks`) | **non** | oui | **oui** | **trou réel — §G** |
| guests / nb voyageurs | oui (`numberOfGuests`) | oui | oui | dépôt |
| champs nécessaires aux ménages | via cleaning tasks | oui | oui | idem trou |
| champs nécessaires aux canapés | n/a — référentiel local | n/a | oui | `ref_logements` |
| champs factures / relevés | dérivés du payout + référentiels | idem | oui | moteur + référentiels |

### Ce que le dépôt publie et que l'application **refuse de lire**

`listing_constants.csv` porte `CoutMenage` et `TauxCommission`. `hostaway_reporting_final.tsv`
porte un `TotalPayout` déjà calculé. Ce sont des **décisions de gestion** et des **conclusions**,
pas des faits Hostaway. Elles vivent dans `ref_couts_standards_menage` et `ref_taux_commission`,
datés et versionnés, et le payout est calculé par le moteur.

Les importer créerait une seconde vérité — muette, plus récente en apparence, et fausse le jour où
l'une des deux change. Un test le verrouille :
`test_le_cout_de_menage_et_la_commission_publies_ne_sont_jamais_lus`.

*(Au passage : `listing_constants.csv` contient déjà des valeurs qui divergent du référentiel
local — p. ex. `CoutMenage = 35` pour `481998` là où le coût standard daté dit autre chose. C'est
exactement le risque décrit.)*

---

## D. La décision architecturale

**Une seule chaîne d'ingestion.**

```
GitHub Actions + Secrets → API Hostaway → TSV commités dans le dépôt
                                               ↓  git fetch + git show
                                    lot1_hostaway_extract --source DEPOT_GITHUB
                                               ↓  (moteur INCHANGÉ)
                                       couche RAW SQLite → moteurs → application
```

**Ce n'est pas un second extracteur.** `lib_hostaway_depot.SourceDepotGitHub` présente exactement
la même surface que `HostawayClient` (`count_reservations`, `get_reservations_page`,
`get_reservation_detail`, `get_listings`, `get_tasks`). `lot1_hostaway_extract.py` reçoit
`--source API|DEPOT_GITHUB` et construit l'un ou l'autre. **Tout le reste est le même code** :
normalisation des canaux, payout H1/H2/H3, coût de ménage daté, détection d'anomalies, écriture RAW.

### Preuve que le transport ne change rien au résultat

Import du dépôt comparé à la dernière extraction API réelle (`HAX-4BDCFBF77A21`, 2026-09-02),
sur les **1 536 réservations communes** :

| Champ comparé | Écarts |
|---|---|
| `channel_type` | **0** |
| `statut_calcul_payout` | **0** |
| `payout_calcule` | **0** |
| `menage_retenu` | **0** |
| `assiette_commission` | **0** |
| `cout_standard_id` | **0** |

Les seuls écarts portent sur **9 réservations réellement modifiées chez Hostaway** entre le 2 et le
12 septembre (statut 9, montant 7, voyageurs 2, nuits 1, date de départ 1) — p. ex. la réservation
`61737735` passée de 10 à 3 nuits. C'est la fraîcheur, pas une divergence de calcul.

### Ce qui a été refusé, et pourquoi

`SourceDepotGitHub.get_tasks()` **lève** au lieu de rendre une liste vide. « Non fourni par cette
source » n'est pas « aucune tâche » : une liste vide rendue par erreur ferait disparaître tous les
ménages d'un mois sans la moindre alerte. Même principe que `RateLimitEpuise`, déjà en place.

De même, une annonce déduite des réservations ne prétend pas être active : `ville`, `actif`,
`specialStatus` restent **NULL**. Écrire « actif = OUI » faute de mieux transformerait une
ignorance en affirmation.

---

## E. Fraîcheur : deux dates, jamais une seule

Migration **0086** : `hostaway_extractions` porte désormais `source_ref` (la version exacte
importée — le SHA du commit) et `source_horodatage` (quand la source a été produite).

| Notion | Colonne | Exemple réel |
|---|---|---|
| données **produites** le | `source_horodatage` | 2026-09-12 **14:35:42Z** |
| **synchronisées ici** le | `date_fin` | 2026-09-12 **15:49:20Z** |

Confondre les deux, c'est mentir : afficher l'heure de l'import ferait passer pour « données de
17 h » un jeu produit à 14 h 35 ; afficher l'heure du pipeline ferait passer pour « à jour » un
dépôt jamais synchronisé.

**« À jour » se juge sur l'IDENTITÉ de la version, jamais sur l'ancienneté.** Trois états :
`A_JOUR` (même `source_ref`), `RETARD` (une version plus récente est publiée), `JAMAIS_IMPORTE`.
Un dépôt illisible donne `INDETERMINE` — on ne peut alors affirmer ni le retard ni son contraire.

`source_ref` rend aussi la synchronisation **idempotente** : réimporter le même état ne recrée
rien. Sans cela, chaque clic aurait fabriqué une extraction de plus, identique à la précédente, et
la comparaison d'une extraction à l'autre — qui sert à repérer les mois impactés — n'aurait plus
rien voulu dire.

---

## F. Le bouton « Actualiser » — ce qu'il fait réellement

Une **étape 0** a été ajoutée en tête du workflow « Actualiser le rapprochement » :

| # | Étape | Ce qu'elle fait |
|---|---|---|
| **0** | **HOSTAWAY_DEPOT** | `git fetch` → dernier jeu publié → import SQLite **si nouveau** |
| 1 | PDF | factures de ménage déposées |
| 2 | GOOGLE_SHEET | déclarations internes |
| 3 | HOSTAWAY | tâches de ménage (API — voir §G) |
| 4 | CIBLAGE | mois réellement impactés |
| 5/6 | RECALCUL | mois OUVERTS uniquement |
| 7 | RAPPROCHEMENT | lot6d/6e/6f puis cascade Lot9→Lot12 |

L'étape 0 **ne déclenche aucune seconde extraction Hostaway** : si l'état publié est déjà en base,
elle ne fait rien et le dit. Le mois sélectionné est toujours retenu au ciblage, et l'écran affiche
l'état réel du run — un run PARTIEL reste PARTIEL.

Le bouton « Actualiser Hostaway » de l'écran Réservations a été **redirigé vers le dépôt** lui
aussi : il ne restait sinon qu'une seconde chaîne d'ingestion, celle qui échoue faute de secret,
sans que l'écran dise laquelle avait produit ce qu'il affichait.

---

## G. Le trou de couverture, nommé

Le pipeline GitHub **n'extrait pas** `/v1/tasks`. Les **tâches de ménage** restent donc sur
l'ancien chemin — appel API local, identifiants absents sur cette installation.

Conséquence réelle et bornée : l'étape 3 du bouton est marquée en échec, le run devient PARTIEL,
l'écran le dit, et le comptage des ménages s'appuie sur la dernière extraction obtenue
(2026-09-07, 746 tâches). Les réservations, elles, sont à jour.

**La correction n'est pas de recopier un secret Hostaway ici** — ce serait rétablir la seconde
chaîne qu'on vient de supprimer. C'est d'ajouter un quatrième script au pipeline GitHub, qui
détient déjà le jeton. C'est une modification du dépôt de données, hors du périmètre de cette
mission, et elle est signalée comme telle à l'écran.

---

## H. Deux défauts réels corrigés au passage

### Le bouton d'actualisation Hostaway était mort depuis deux jours

Un run `RUN-lot1_hostaway_extract-20260910_152129` était resté `EN_COURS` : son sous-processus
s'était arrêté sans écrire de statut. `actualisation_en_cours()` refusant tout lancement tant qu'un
run est ouvert, **toute actualisation était devenue impossible** — en répondant « une actualisation
est déjà en cours », c'est-à-dire en désignant la mauvaise cause.

Aucun verrou ne protège ce lot. Le critère retenu est donc le temps écoulé : au-delà de deux heures
(une extraction dure des minutes, des secondes depuis le dépôt), le run est requalifié
**INTERROMPU**, avec sa raison. Il n'est pas effacé : un échec doit rester visible, sinon on ne
comprend plus pourquoi la donnée est ancienne.

### Les anomalies perdaient leur numéro de réservation

`AnomalyDetector.to_df()` construit un DataFrame. Dès qu'une seule anomalie porte
`reservation_id = None` — le cas de `check_listing`, qui vise une annonce — pandas type la colonne
en flottant et réécrit tous les autres identifiants : « 66096017 » devient « 66096017.0 ». Ces
anomalies ne se rattachent alors **plus à aucune réservation**.

Le défaut est resté invisible tant que toutes les annonces étaient connues. Il est apparu à la
première annonce absente du parc. Corrigé à l'écriture, seul endroit où la valeur est encore
sûrement un identifiant.

---

## I. Ce que la synchronisation a révélé

### Une annonce Hostaway absente du parc, avec 940,40 € de séjours

L'annonce **`590757` — « 4 rue engalière »** existe chez Hostaway avec quatre réservations en
canal DIRECT :

| Réservation | Arrivée | Départ | Nuits | Montant |
|---|---|---|---|---|
| 66095878 | 2026-07-29 | 2026-08-01 | 3 | 241,58 € |
| 66095934 | 2026-08-07 | 2026-08-09 | 2 | 214,01 € |
| 66095980 | 2026-08-10 | 2026-08-17 | 7 | 393,23 € |
| 66096017 | 2026-08-27 | 2026-08-28 | 1 | 91,58 € |

**940,40 € sur juillet et août, absents du pilotage.** Le moteur refuse de calculer les
réservations : `[BLOQUANT] LOGEMENT_NON_MAPPE — listingMapId=590757`. C'est le bon comportement —
il ne bascule pas en `LOGEMENT_DIVERS` pour faire passer le calcul. Le jeu de données dérivé reste
donc celui d'avant (1 585 lignes) : aucun état à moitié mis à jour.

**Décision humaine requise** — depuis l'écran **Correspondances logement**, qui remonte
précisément ce cas.

### Un logement retiré du parc qui produit toujours des recettes

`PROP_0002` / `LOG_0002` « T4 - 90 Blagnac » : période de gestion close au **2026-01-01**, statut
`RETIRE`. Et pourtant **7 réservations, 21 nuits et 2 107,33 €** en août 2026, avec une commission
calculée par le moteur.

Le relevé ne fabrique pas de dénominateur : il affiche le taux de remplissage comme non calculable
et **dit pourquoi**. À trancher : la date de fin de gestion est-elle fausse, ou ces réservations
sont-elles mal rattachées ?

---

## J. Relevé propriétaire — écran économique

`/releves-proprietaires`. Il répond à une seule question : **ce parc a-t-il bien travaillé ce
mois-ci ?**

Ni créance, ni règlement, ni compensation, ni geste de trésorerie — un test interdit littéralement
ces notions dans le résultat du service. L'écran qui portait ces sujets porte enfin son vrai nom :
**Règlements propriétaires**.

### Ce qu'il affiche

Réservations · nuits occupées · durée moyenne · voyageurs moyens · répartition par canal (et nuits
par canal) · taux de remplissage · total perçu · commission · net propriétaire · ADR · ADR net
propriétaire. Puis le détail par logement.

### Les formules, affichées à l'écran

```
durée moyenne         = Σ nuits ÷ nombre de réservations
voyageurs moyens      = Σ voyageurs connus ÷ nb de réservations PORTANT la donnée
taux de remplissage   = nuits occupées ÷ nuits commercialisables
ADR                   = Σ total perçu ÷ Σ nuits occupées
ADR net propriétaire  = Σ net propriétaire ÷ Σ nuits occupées
```

**Deux pièges, chacun évité et testé :**

- **voyageurs moyens** — le dénominateur ne compte que les réservations qui *portent* la donnée.
  Diviser par le total ferait d'une donnée absente un séjour à zéro voyageur, et la moyenne
  baisserait à chaque trou. L'écran dit sur combien de réservations la moyenne est établie.
- **nuits commercialisables** — les jours **réellement sous gestion**, logement par logement, pas
  les jours du mois × nombre de logements. Un logement entré en gestion le 16 avril compte 15
  jours, pas 30.

### Une erreur que seule l'invraisemblance a trahie

Ma première version lisait `lot10_net_reglement` directement. **Six runs du moteur coexistent en
base** — la lecture les additionnait tous. Résultat : **12 046 €** là où le mois vaut **1 204 €**,
et des ADR à **1 003 €/nuit** pour un T4 en périphérie toulousaine. Aucun test ne l'aurait vu ;
c'est l'invraisemblance du chiffre qui a alerté.

La lecture passe désormais par `proprietaires_reglements_reader`, qui filtre sur le **run actif**
(`lot10_runs.actif`). C'est le **même run** que les Créances et les Comptes propriétaires : cet
écran ne peut donc pas diverger d'eux. Un test le verrouille en plaçant délibérément le même mois
dans deux runs.

### Design

Synthèse KPI en haut, puis détail par logement, puis répartition canal. Aucun code `PROP_`/`LOG_`
n'atteint l'écran ni l'export. Quantités entières. Période affichée en clair. Badge **PROVISOIRE**
sur le mois courant.

---

## K. Export utilisateur

Deux parcours, là où il n'y en avait aucun.

**`/releves-proprietaires/export.csv`** — le relevé du mois au grain logement. Décimale française
(un tableur configuré en français lit « 44.62 » comme du texte), BOM utf-8 (sans lui les accents
sont illisibles dans Excel FR), et `provisoire` comme **colonne** — un fichier se découpe, se trie
et se recolle, une mention d'en-tête se perdrait au premier filtre.

**`/exports`** — les 13 jeux de données complets. Le moteur savait déjà les produire ; rien ne
permettait de les **demander** ni de les **récupérer**. L'écran génère, liste (nom, lignes, taille,
date), et télécharge fichier par fichier ou en archive. Vérifié : 13 fichiers + le dictionnaire de
colonnes, archive de 55 Ko, et un nom de fichier venu de l'URL ne peut pas désigner un fichier
ailleurs sur le disque.

L'archive s'appelle `exports_AAAAMMJJ_PROVISOIRE.zip` : elle couvre tous les mois, donc le mois en
cours, qui est incomplet. La mention est dans le **nom**, parce qu'un fichier téléchargé puis
transmis n'emporte pas les avertissements d'un écran.

---

## L. « Démarrer le suivi » — but métier et emplacement final

**Audit.** `POST /proprietaires-reglements/demarrer` → `proprietaires_suivi_service.creer_ou_charger`
crée la ligne `(propriétaire, mois)` dont le `statut_facturation` pilote
`NON_CONCERNE → A_FACTURER → FACTURE → AVOIR_A_EMETTRE → AVOIR_EMIS`.

C'est donc un **suivi de facturation mensuel** : un avancement, pas une performance.

**Emplacement final.** Le bouton n'a jamais été sur le relevé économique (qui vient d'être créé) ;
il vit sur l'écran de règlements, désormais nommé pour ce qu'il est. Et le **parcours** est
maintenant offert depuis la **préparation de clôture**, qui affiche l'avancement du mois et
signale les propriétaires ayant une activité et **aucun suivi ouvert**.

Le moteur n'a pas bougé — il est transactionnel, idempotent, verrouillé optimiste, journalisé.

**Ce que ce nouvel affichage révèle immédiatement** :

| Mois | Propriétaires avec activité | Suivis ouverts | Sans suivi |
|---|---|---|---|
| 2026-08 | 9 | 2 | **7** |
| 2026-07 | 9 | 0 | **9** |
| 2026-06 | 10 | 0 | **10** |

Un suivi non ouvert n'est pas « à jour » : il est **invisible**. Clôturer sur cette base fermerait
le mois sur une facturation que plus aucun écran ne réclame.

---

## M. Correspondances logement — parcours dédié

`ref_mapping_logements` était classée `EDITABLE`. **Décision appliquée : `DEDICATED_WORKFLOW`.**

Le besoin — pouvoir corriger une correspondance fausse — était juste ; le moyen ne l'était pas.
Éditer la ligne brute demande de connaître `source`, `champ_source`, `valeur_source` et
l'identifiant technique du logement, et n'enregistre ni qui a tranché, ni contre quelle proposition.

`/correspondances-logement` pose la seule question qui compte, avec sous les yeux : la valeur telle
que la source l'écrit · le logement actuellement rattaché · celui que le moteur **propose**, avec
son degré de certitude · le choix humain, tracé.

**Il remonte ce qu'aucun écran ne montrait.** Les correspondances *déclarées* sont faciles à
lister ; les correspondances *manquantes* ne l'étaient pas — elles ne se signalaient qu'au moment
où un moteur s'arrêtait dessus. L'écran liste désormais les annonces Hostaway vues dans la dernière
extraction et rattachées à aucun logement, et les libellés de facture de ménage sans
correspondance, avec la conséquence de chacun.

Garde-fous : `LOGEMENT_DIVERS` n'est **jamais** proposé comme réponse · l'ancienne correspondance
est désactivée, **jamais supprimée** · rattacher une annonce écrit **les deux** chemins de lecture
(fiche logement *et* règle de correspondance), sinon l'autre resterait bloqué avec le même message.

L'écran d'administration **conduit** au parcours au lieu de seulement le nommer.

---

## N. Pricing dynamique normalisé

`dynamic_pricing` mélangeait deux questions : *activé ?* et *quel moteur ?* Tant qu'il n'existe
qu'un fournisseur, la confusion est invisible ; elle apparaît le jour où l'on en change.

Migration **0087** :

| `dynamic_pricing` (brut) | `dynamic_pricing_enabled` | `dynamic_pricing_provider` | Logements |
|---|---|---|---|
| `hostdynamic` | `OUI` | `hostdynamic` | 14 |
| `non` | `NON` | `NULL` | 5 |

**« hostdynamic » n'est pas perdu.**

La paire **dérive par déclencheur SQL**, pas par le code applicatif : elle est donc exacte quel que
soit le chemin d'écriture — import du classeur compris. L'écran modifie la paire lisible
(« Pricing dynamique : Oui/Non », puis « Moteur ») et le service recompose la valeur brute. La
correspondance est **bijective**, donc les trois colonnes ne peuvent pas se contredire.

Un « oui » sans moteur nommé reste « oui » : lui attribuer d'office « hostdynamic » affirmerait un
fournisseur que personne n'a désigné.

---

## O. `menages_cout_complet` — la variation expliquée

État courant : **39 lignes / 7 655,00 €**.

| Mois | Lignes | Montant |
|---|---|---|
| 2026-03 | 5 | 1 084,00 € |
| 2026-04 | 8 | 1 844,00 € |
| 2026-05 | 5 | 1 322,00 € |
| 2026-06 | 6 | 1 025,00 € |
| 2026-07 | 7 | 1 470,00 € |
| 2026-08 | 8 | 910,00 € |
| **Total** | **39** | **7 655,00 €** |

Les deux factures de juillet sont repassées en `A_CONTROLER` :

| Facture | Lignes ménage | Montant |
|---|---|---|
| `2026-40` | 7 | 967,00 € |
| `0005` | 4 | 556,00 € |
| **Total** | **11** | **1 523,00 €** |

**39 + 11 = 50** · **7 655,00 + 1 523,00 = 9 178,00 €** — exactement l'écart annoncé au §20.

**La règle qui le produit**, dans `lot6f_cout_complet_menages.py` :

> *« Seules les factures VALIDEES entrent dans le coût complet : une facture A_CONTROLER est un
> document reçu, pas une charge acceptée. Sans ce filtre, son montant remontait jusqu'à
> `menages_cout_complet` puis TYPE_FLUX_018/019 dans lot9. »*

La variation est donc **attendue**, et elle disparaîtra d'elle-même quand l'utilisateur aura résolu
les deux factures depuis l'écran.

---

## P. `TYPE_FLUX_005` — contrat confirmé

État constaté dans `ref_types_flux` :

```
TYPE_FLUX_005 · REMBOURSEMENT_ASSOCIE · code_impact_defaut = NULL
avantage_brut_defaut = NON · deduit_avantage_defaut = NON · comptabilisable_defaut = NON
```

Le test de non-régression demandé **existe et passe** : `test_type_flux_005_contract.py`,
**8 tests verts**, couvrant :

- `code_impact_defaut` est **NULL**, et non la chaîne vide ni le mot « None » ;
- le lookup rend une chaîne vide, **jamais** le mot « None » ;
- **aucun** `IC`/`HC` n'est inventé par défaut ;
- `sens_flux = REMBOURSEMENT` neutralise indépendamment du code d'impact ;
- la validation V19 (justification si code ≠ défaut) ne se déclenche **jamais** pour ce type ;
- vérification sur le référentiel **réel**, pas seulement sur une fixture.

**Contrat confirmé. Rien à modifier.**

---

## Q. Factures de juillet — état volontairement conservé

Elles ne sont pas corrigées à la place de l'utilisateur. Les deux restent `A_CONTROLER` avec leurs
diagnostics **distincts** :

| Facture | Diagnostic | Geste attendu |
|---|---|---|
| `0005` | `LIGNES_INCOHERENTES` — une ligne extraite est fausse (qté 0 × 32,00 € = 36,00 €) | « Écarter : extraction incorrecte » |
| `2026-40` | `LIGNE_MANQUANTE` — lignes cohérentes, 89 € manquants | « Ajouter une ligne manquante » |

Le logiciel rend ces deux parcours fiables ; il ne choisit pas à la place de l'utilisateur.

---

## R. Qonto

**Hors scope. Rien n'a été commencé.** Aucun fichier, aucune table, aucune route.

---

## S. Reset

**Aucun reset n'a été exécuté.** La base n'a été ni réinitialisée, ni nettoyée, ni renumérotée.
`F-11/0-000001` est intacte (EMIS, 465,88 €). Les séquences de numérotation n'ont pas été touchées.

Sauvegarde prise avant les travaux : **`BCK-FF4136EB8006`** (schéma 0085, validation VALIDE).

---

## T. Ce qui reste ouvert, et pourquoi

| Sujet | État | Raison |
|---|---|---|
| Tâches de ménage par le dépôt | **ouvert** | Le pipeline GitHub n'extrait pas `/v1/tasks`. Correction = 4e script dans le dépôt de données, hors périmètre. Jamais contourné par un secret local. |
| Annonce `590757` « 4 rue engalière » | **décision utilisateur** | 4 réservations, 940,40 €. À rattacher depuis Correspondances logement, ou à déclarer hors parc. |
| `LOG_0002` gestion close mais recettes | **décision utilisateur** | Date de fin de gestion fausse, ou rattachement erroné. |
| Factures `0005` / `2026-40` | **décision utilisateur** | Deux gestes distincts, décrits au §Q. |
| Qonto | **hors scope** | Consigne explicite. |


---

# Second tour — points fermés avant la référence finale

## U. Périmètre économique : deux règles rétablies dans le moteur

### U.1 — une annonce non rattachée signale, elle ne bloque plus

`lot4bis` faisait `abort()` sur `LOGEMENT_NON_MAPPE` : une seule annonce apparue chez Hostaway et
non encore rattachée arrêtait le calcul des **1 600 autres** réservations, parfaitement mappées. Le
remède était pire que le mal.

Désormais la réservation est **conservée, tracée, exclue de l'économie** et signalée comme
correspondance à établir. Ni fourre-tout, ni logement créé à la volée, ni rattachement inventé —
les trois feraient disparaître l'anomalie en fabriquant une réponse.

Sur la base réelle, après recalcul complet (1 613 lignes) :

| Motif d'exclusion | Lignes |
|---|---|
| (inclus dans l'économie) | 293 |
| `LEGACY_SANS_ARCHIVE_ORIGINE` | 1 260 |
| `HORS_PERIODE_GESTION` | 31 |
| `OWNERSTAY` | 25 |
| **`LOGEMENT_NON_MAPPE`** | **4** |

Les quatre séjours de `590757` sont là, avec leurs nuits, **toujours non mappés** :

```
RES-HA-66095878  2026-07  3 nuits   LOGEMENT_NON_MAPPE  impact réel = NON
RES-HA-66096017  2026-08  1 nuit    LOGEMENT_NON_MAPPE  impact réel = NON
RES-HA-66095980  2026-08  7 nuits   LOGEMENT_NON_MAPPE  impact réel = NON
RES-HA-66095934  2026-08  2 nuits   LOGEMENT_NON_MAPPE  impact réel = NON
```

### U.2 — LOG_0002 : la règle ne demandait pas d'arbitrage, et le moteur ne l'appliquait pas

Il n'y avait effectivement rien à arbitrer. Une réservation postérieure à la fin de gestion se
conserve, se consulte, se signale — et ne contribue à **rien**. Le moteur ne le faisait pas, et la
cause tenait en deux défauts distincts :

1. **`lib_parc` ne connaissait pas `RETIRE`.** Ce statut figure pourtant dans la liste proposée à
   l'administration et quatre logements le portent. Un statut inconnu retombe sur `A_CONTROLER` :
   tous les séjours d'un logement retiré étaient donc classés `STATUT_PARC_INVALIDE` — « statut de
   parc vide ou invalide ». Le motif était faux et, surtout, il **masquait le vrai**.
2. **Les branches S1/S2 jetaient l'anomalie de gestion.** Dès que le payout était `NORMAL`, la
   ligne était construite « VALIDE / INFO / aucune anomalie » — et `ano_code` était perdu en
   chemin. Une réservation hors de toute période de gestion ressortait donc **valide**.

Effet mesuré sur la base réelle, run Lot10 actif :

| | Lignes | Perçu | Commission | Net propriétaire |
|---|---|---|---|---|
| Avant | 39 | 48 252,15 € | 6 859,81 € | 31 863,34 € |
| Après | 37 | 41 751,24 € | 6 070,97 € | 27 393,27 € |
| **Écart** | **−2** | **−6 500,91 €** | **−788,84 €** | **−4 470,07 €** |

Cet écart est **entièrement** imputable à `LOG_0002`, dont la gestion s'arrête au 2026-01-01 :
6 500,91 € de recettes et 788,84 € de commission étaient attribués à un propriétaire sans mandat.

La donnée source, elle, est intacte — mois par mois :

```
2025-10   3 résa · 3 dans l'économie ·  6 898,86 €
2025-12   1 résa · 1 dans l'économie ·    371,18 €      <- avant la fin de gestion
--------------------------------------------------------
2026-01   5 résa · 0 dans l'économie ·  1 075,29 € conservés
2026-02   6 résa · 0 dans l'économie ·  2 169,46 € conservés
2026-03   9 résa · 0 dans l'économie ·  2 305,28 € conservés
2026-04   7 résa · 0 dans l'économie ·  2 110,27 € conservés
2026-05   6 résa · 0 dans l'économie ·  2 096,81 € conservés
2026-06   5 résa · 0 dans l'économie ·  1 620,30 € conservés
2026-07   6 résa · 0 dans l'économie ·  2 773,28 € conservés
2026-08   7 résa · 0 dans l'économie ·  2 107,33 € conservés
```

Si la date de fin est erronée, la corriger depuis **Historique logement** suffira : tout se
recalculera. Le moteur ne contourne rien de lui-même.

---

## V. Tests : ce qui a été renforcé, et ce qui a été retiré

### V.1 — l'export n'est plus comparé à lui-même

Le contrôle de valeur de l'export a bien failli devenir tautologique. Comparer le CSV à la table
SQLite dont il est issu vérifie que **SQLite ressemble à SQLite** : ça reste vert devant une
colonne décalée, un montant divisé par cent, un identifiant qui fuit.

Le classeur `MASTER_CALC_Commissions.xlsx` ne pouvait pas servir de référence non plus : figé au
2026-09-09, ses clés suivent l'ancien schéma (`RES-2025-01-HA-001`) quand `reservation_calc_id`
vaut désormais `RES-HA-53441757`. **1 476 lignes d'un côté, 217 de l'autre, zéro en commun.** La
comparaison était impossible bien avant cette mission ; personne ne le voyait parce que le test
était *ignoré*, faute d'export sur la racine.

Il est remplacé par `test_export_snapshot_canonique.py` : un jeu **minimal, écrit à la main** —
1 propriétaire, 2 logements, 3 réservations, nuits, perçu, commission, net, ménage — et les valeurs
exportées attendues **énoncées en toutes lettres dans le fichier de test**. 10 contrôles, dont :

- chaque montant du détail, colonne par colonne, valeur par valeur ;
- la concordance détail ↔ total du mois (172,00 € de commission, 1 000,00 € perçus, 3 réservations) ;
- la forme des nombres (« 300 » et non « 300.0 ») — un rendu qui casse Power BI sans qu'un chiffre
  bouge en base ;
- le séparateur `;` et le BOM utf-8 ;
- l'absence de coordonnées dans le référentiel propriétaires ;
- le fait qu'un run **inactif** n'est jamais exporté (la faute qui a produit 12 046 € pour un mois
  à 1 204 €).

### V.2 — la suite automatisée ne dépend plus de la base réelle

`test_regularisation_hh.py` clonait l'`app.db` de production. C'est précieux — et c'est
précisément pourquoi ce n'est pas un test automatisé : le jour où le parc a reçu une annonce non
rattachée, la suite est devenue rouge pour une raison qui ne concernait pas le code.

Séparation appliquée :

| | Périmètre | Déclenchement |
|---|---|---|
| **A. Suite automatisée** | bases temporaires, fixtures déterministes | à chaque exécution |
| **B. Contrôle de recette réelle** | clone de la vraie base | `RECETTE_REELLE=1` |

C'est le **seul** fichier de la suite qui clonait la base réelle — vérifié.

En remplacement, `test_perimetre_gestion_et_mapping.py` prouve les mêmes règles **sans aucune
donnée réelle** : 22 contrôles déterministes sur la période de gestion, le statut de parc, le
rattachement d'annonce et le cycle de vie des runs.

### V.3 — trois corrections d'assertions, et ce qu'elles valaient

Les trois tests touchés au premier tour affirmaient un **littéral** là où les changements étendent
légitimement l'ensemble (liste de colonnes verrouillées, nom d'un maillon de la chaîne
d'ingestion). Les assertions disent désormais l'**intention**. Aucune ne masque une anomalie : la
seule qui en masquait une — celle de l'export — a été retirée et remplacée par une preuve plus
forte.

---

## W. Runs orphelins : le contrat

Trois issues, et **aucune présomption de mort** :

| État | Signification |
|---|---|
| `SUCCES` / `PARTIEL` / `ECHEC` | le sous-processus a conclu lui-même |
| `INTERROMPU` | on a **établi** qu'il ne tourne plus |
| `EN_COURS` | il tourne, ou on ne peut pas prouver le contraire |

Deux preuves sont acceptées, dans cet ordre :

1. **Le PID.** Le journal porte le numéro de processus et le nom de la machine. Si le run a démarré
   sur *cette* machine et que ce PID n'existe plus, la conclusion est immédiate — inutile
   d'attendre le bail. S'il vit encore, le run est en cours **même s'il dépasse le bail** : le tuer
   serait pire que l'attendre.
2. **Le temps écoulé**, en dernier recours seulement, quand le PID ne répond pas de la question —
   absent, illisible, ou enregistré sur une autre machine. On ne conclut jamais sur un processus
   qu'on ne peut pas voir : ce serait tuer un run réellement en cours sur un autre poste.

Sous Windows, l'existence se teste par `tasklist` et **jamais** par `os.kill(pid, 0)` — qui, sur
cette plateforme, ne teste rien : il termine le processus.

Le run n'est jamais effacé : il est marqué `INTERROMPU` avec la preuve retenue.

Sept contrôles déterministes couvrent ce contrat, y compris les deux cas qu'on rate le plus
facilement : un processus vivant au-delà du bail (jamais tué) et un run venu d'une autre machine
(jamais jugé sur son PID).

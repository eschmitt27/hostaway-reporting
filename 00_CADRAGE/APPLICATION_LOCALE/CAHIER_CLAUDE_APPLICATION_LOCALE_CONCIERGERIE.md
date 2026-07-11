# CAHIER_CLAUDE_APPLICATION_LOCALE_CONCIERGERIE

## 0. But du document

Construire une application locale de pilotage pour la conciergerie, sans remplacer ni casser le système de calcul existant.

L’application doit devenir une couche de saisie, de contrôle, de suivi, de rapprochement et de visualisation au-dessus des règles métier, exports, référentiels et scripts déjà présents dans le dépôt.

Elle est utilisée par un seul administrateur au départ.

Ne pas développer de gestion de comptes, rôles, permissions ou accès dans cette version.

---

## 1. Documents à lire avant toute modification

Lire intégralement, dans cet ordre, les documents déjà présents dans le dépôt :

1. `CLAUDE.md`
2. `ARCHITECTURE_DONNEES.md`
3. `DECISIONS_METIER.md`
4. `ETAT_AVANCEMENT.md`
5. `JOURNAL_ANOMALIES.md`
6. `JOURNAL_CONTROLES.md`
7. `OBJECTIF_PROJET_PILOTAGE_CONCIERGERIE_V3.md`
8. `00_CADRAGE/APPLICATION_LOCALE/Catalogue_operations_navigation_conciergerie.xlsx`
9. `00_CADRAGE/APPLICATION_LOCALE/BRIEF_STITCH_DESIGN_CONCIERGERIE.md`
10. le présent document.

Avant de créer une table, une fonction, une règle de calcul ou un écran, vérifier si la règle existe déjà dans les référentiels, les scripts ou les documents de cadrage.

En cas de contradiction :
- ne pas deviner ;
- ne pas écraser une règle existante ;
- documenter la contradiction ;
- proposer une décision explicite.

---

## 2. Objectif fonctionnel

L’application locale doit permettre à une personne de gérer, suivre, contrôler et clôturer l’activité de conciergerie.

Elle couvre :

```text
Référentiels
Réservations hors Hostaway
Ménages
Relevés propriétaires
Factures clients
Fournisseurs et charges
Banque
Caisse
Prévisions de trésorerie
Associés, avances et avantages
Précomptabilité
Clôture mensuelle
Sauvegardes
Connecteur Hostaway et anomalies source
```

Le catalogue Excel est la liste des opérations attendues.

Toute ligne du catalogue avec `Retirer ? (X)` renseigné par `X` est hors périmètre de l’application.

---

## 3. Principes non négociables

### 3.1 Une seule source de vérité par donnée

- Les données brutes Hostaway restent identifiables et ne sont pas réécrites silencieusement.
- Les données bancaires importées restent identifiables et ne sont pas réécrites silencieusement.
- Les corrections doivent être des ajustements, correspondances ou modifications explicitement tracées.
- Ne jamais créer une seconde source libre d’une donnée déjà structurée.

### 3.2 Ne pas dupliquer les calculs existants

Les scripts Python et les tables masters existants restent la base des calculs déjà validés.

L’application doit :
- lire et afficher les résultats ;
- permettre les saisies contrôlées ;
- déclencher un recalcul ou un pipeline existant si nécessaire ;
- stocker les actions manuelles dans des tables structurées ;
- ne pas réimplémenter discrètement des règles métier dans le front-end.

### 3.3 Données structurées

Utiliser :
- listes contrôlées ;
- identifiants stables ;
- relations explicites ;
- périodes de validité ;
- statuts fermés ;
- motifs obligatoires quand une action a un impact de contrôle.

Éviter les champs texte libres, sauf pour :
- commentaires ;
- notes ;
- motifs ;
- libellés importés ;
- pièces ou documents.

### 3.4 Archivage plutôt que suppression

- Un acteur, logement, paramètre, facture ou opération historique ne doit pas être supprimé définitivement en usage normal.
- Utiliser `actif/inactif`, `archivé`, ou une date de fin.
- Une suppression exceptionnelle doit être réservée aux données de test ou aux doublons non utilisés.

### 3.5 Historisation des paramètres sensibles

Les éléments suivants doivent avoir une période de validité :

```text
Taux de commission
Rattachement propriétaire-logement
Coût standard de ménage
Tarif prestataire externe
Taux horaire interne
Paramètres contractuels particuliers
```

Les calculs passés ne doivent jamais être recalculés avec le paramètre actuel par erreur.

### 3.6 Résultat réel et résultat comptable

Chaque flux sensible doit pouvoir indiquer :

```text
impact_resultat_reel
impact_resultat_comptable
```

Ces deux notions ne doivent jamais être confondues.

---

## 4. Règles métier prioritaires

### 4.1 Acteurs

Types d’acteurs :
- propriétaire ;
- intervenant ménage interne ;
- prestataire externe ;
- associé.

Un propriétaire peut être créé sans logement.

Le RIB ne bloque pas la création d’un propriétaire, mais doit bloquer ou alerter avant un reversement bancaire.

### 4.2 Logements

Un logement doit être relié à :
- un ou plusieurs propriétaires selon période ;
- un taux de commission ;
- un coût standard de ménage par type de ménage et période ;
- une liaison Hostaway si elle existe ;
- des paramètres spécifiques si nécessaire.

Le coût standard de ménage appartient au logement, au type de ménage et à la période. Il ne doit pas dépendre uniquement de la personne qui fait le ménage.

### 4.3 Personnel et prestataires ménage

Le taux horaire appartient à l’intervenant interne.

Le tarif externe dépend de :

```text
prestataire + logement + type de prestation + période
```

### 4.4 Réservations hors Hostaway

Les réservations hors Hostaway sont saisies dans l’application.

Les réservations déjà présentes dans Hostaway ne doivent pas être ressaisies.

Lors de la saisie :
- sélectionner le logement ;
- préremplir le propriétaire et les paramètres du logement ;
- saisir les dates, voyageur, montants et acompte ;
- créer des liens vers les paiements bancaires et documents si disponibles.

### 4.5 Factures et ajustements

Une facture ou un document émis ne doit jamais être modifié silencieusement.

Une correction doit passer par :
- un avoir ;
- une facture ou document rectificatif ;
- un ajustement tracé ;
- une nouvelle révision explicitement liée.

Chaque ajustement doit avoir au minimum :

```text
type
montant
sens
période ou réservation concernée
logement
propriétaire
source
justificatif si disponible
motif
impact réel
impact comptable
```

Les reversements Airbnb, AirCover, acomptes, extras et corrections suivent cette règle.

### 4.6 Banque

Une ligne bancaire importée ne crée jamais automatiquement une charge définitive.

Elle doit être :
- rattachée à une facture existante ;
- classée ;
- transformée en charge après validation ;
- identifiée comme encaissement, reversement, transfert, avance associé, mouvement de caisse ou anomalie.

Les doublons bancaires doivent être détectés.

### 4.7 Ménages

Le contrôle compare :

```text
Ménages attendus
Ménages réalisés Hostaway
Ménages déclarés internes
Ménages facturés externes
```

Le matching doit pouvoir être relancé après correction.

Un outrepassage est autorisé seulement avec :
- motif ;
- date ;
- éléments concernés ;
- trace de l’action.

### 4.8 Clôture

Statuts de période :

```text
OUVERT
EN_CONTROLE
PRET_A_CLOTURER
CLOTURE
REOUVERT
```

Un mois clôturé ne doit pas être modifié directement.

Pour modifier une période clôturée :
- créer un ajustement post-clôture ;
- ou réouvrir explicitement le mois ;
- créer un snapshot avant réouverture ;
- exiger un motif ;
- créer une nouvelle révision.

### 4.9 Sauvegardes

Prévoir :
- snapshot complet tous les 14 jours ;
- snapshot avant clôture ;
- snapshot après clôture ;
- snapshot avant réouverture ;
- restauration uniquement dans une copie de travail, jamais par écrasement silencieux de la base active.

---

## 5. Navigation et structure des écrans

La structure de navigation est fixée par le brief Stitch.

```text
Accueil

Activité
  Réservations hors Hostaway
  Ménages

Facturation & paiements
  Relevés propriétaires
  Factures clients
  Fournisseurs & charges

Banque & trésorerie
  Banque
  Caisse
  Prévisions de trésorerie
  Associés / avances

Pilotage & clôture
  À traiter
  Précomptabilité
  Clôture mensuelle
  Sauvegardes & archives

Référentiels
  Acteurs
  Logements
  Paramètres

Outils & sources
  Hostaway & calculs
  Anomalies source
```

Règles d’ergonomie :
- deux niveaux de navigation maximum ;
- une recherche globale fixe ;
- une période active visible partout ;
- une action primaire par écran ;
- liste → fiche détaillée ;
- actions sensibles seulement dans `Actions avancées` ;
- le front-end doit respecter les écrans fournis par Stitch lorsqu’ils sont disponibles.

---

## 6. Architecture technique recommandée

### 6.1 Cible locale

Déployer localement avec Docker Compose.

Architecture recommandée :

```text
Navigateur local
  ↓
Frontend React + TypeScript
  ↓ API locale
Backend FastAPI + Python
  ↓
Base PostgreSQL locale
  ↓
Volumes Docker persistants
  ↓
Scripts / exports / documents existants du projet
```

Raisons :
- cohérence avec les scripts Python existants ;
- interface moderne et compatible avec le design Stitch ;
- API claire entre interface, règles métier et données ;
- base robuste pour relations, clôtures, historique et sauvegardes ;
- déploiement local reproductible dans WSL/Docker.

Ne pas démarrer avec une architecture microservices.

Construire un monolithe modulaire :
- un front-end ;
- un back-end ;
- une base de données ;
- un worker ou système de tâches local uniquement si nécessaire pour les imports Hostaway et recalculs.

### 6.2 Séparation obligatoire

```text
frontend/
  pages
  composants
  formulaires
  tables
  design system
  appels API

backend/
  api
  services
  repositories
  schemas
  validations
  jobs
  adaptateurs vers scripts existants

database/
  migrations
  modèles
  seeds de développement

integration/
  hostaway
  import banque
  exports existants
  fichiers et justificatifs

tests/
  unitaires
  intégration
  non-régression métier
```

Le front-end ne porte aucune règle de calcul sensible.

Le backend ne doit pas se connecter directement à une feuille Excel arbitraire comme base de données opérationnelle. Les imports et exports Excel restent possibles, mais la base relationnelle devient la source opérationnelle de l’application.

### 6.3 Répertoire d’application recommandé

Créer l’application sans modifier le fonctionnement des scripts actuels :

```text
01_APPLICATION/
  docker-compose.yml
  README.md
  .env.example
  frontend/
  backend/
  database/
  docs/
  scripts/
```

Les scripts existants de calcul restent dans leur emplacement actuel, notamment `02_TRAVAIL/`, et sont appelés par un adaptateur explicite lorsque nécessaire.

Ne pas déplacer ni renommer massivement les fichiers existants sans plan de migration et sans tests.

---

## 7. Modèle de données minimal

Utiliser des identifiants techniques stables, non modifiables et distincts des libellés visibles.

### 7.1 Référentiels

```text
actors
actor_contacts
actor_bank_accounts
actor_documents
properties
property_owner_assignments
commission_rates
cleaning_standard_costs
external_service_rates
internal_hourly_rates
parameters
parameter_values
```

### 7.2 Opérations

```text
manual_reservations
manual_reservation_payments
supplier_invoices
supplier_invoice_allocations
customer_invoices
customer_invoice_lines
owner_statements
owner_statement_lines
adjustments
bank_transactions
bank_reconciliations
cash_movements
partner_movements
cleaning_expected
cleaning_actual
cleaning_declarations
cleaning_supplier_invoice_lines
cleaning_matches
cleaning_overrides
```

### 7.3 Contrôle et clôture

```text
anomalies
supporting_documents
period_closures
period_closure_modules
period_closure_revisions
snapshot_registry
audit_events
calculation_runs
import_runs
```

Ne créer une table que lorsqu’elle porte une entité ou une relation claire. Ne pas multiplier les tables de paramètres sans besoin réel.

---

## 8. États, statuts et validations

Les statuts doivent être centralisés et fermés.

Exemples :

```text
VALIDE
A_CONTROLER
EXCLU_RESULTAT
A_VENTILER
A_PAYER
PAYE
PARTIELLEMENT_PAYE
IMPAYE
A_COMPTABILISER
COMPTABILISE
OUVERT
EN_CONTROLE
PRET_A_CLOTURER
CLOTURE
REOUVERT
ARCHIVE
```

Chaque action qui crée ou modifie une donnée doit passer par une validation backend.

Les formulaires doivent :
- préremplir les valeurs déterminables ;
- utiliser des listes ;
- bloquer seulement les champs réellement nécessaires ;
- expliquer clairement la raison d’un blocage ;
- permettre l’enregistrement en brouillon lorsque la règle métier le permet.

---

## 9. Modules à construire et ordre de développement

### Phase 0 — Audit et socle

Avant de coder :
1. auditer les scripts, fichiers de référence, masters et exports existants ;
2. produire une carte des flux de données ;
3. identifier les tables importées, générées et saisies ;
4. proposer une migration progressive ;
5. créer le Docker Compose, la base, les migrations et le squelette front-end ;
6. ajouter une documentation de lancement local ;
7. mettre en place des jeux de données de démonstration séparés des données réelles.

Ne pas développer d’écran métier avant d’avoir validé le modèle de données minimal et la stratégie de synchronisation avec les scripts existants.

### Phase 1 — Référentiels et navigation

Construire :
- navigation globale ;
- page Accueil minimaliste ;
- Acteurs ;
- Logements ;
- Paramètres ;
- documents joints ;
- dates de validité ;
- statuts actif/inactif/archivé.

Objectif : pouvoir créer proprement les acteurs et logements nécessaires aux opérations suivantes.

### Phase 2 — Réservations hors Hostaway et ménage

Construire :
- saisie et suivi des réservations hors Hostaway ;
- lien logement/propriétaire ;
- acomptes et solde ;
- module ménages ;
- matching attendu/réalisé/déclaré/facturé ;
- correction structurée ;
- relance du matching ;
- outrepassage motivé.

Objectif : fiabiliser les opérations sources.

### Phase 3 — Facturation, charges et paiements

Construire :
- relevés propriétaires ;
- ajustements structurés ;
- factures clients, paiements et relances ;
- fournisseurs, charges, échéances et ventilation ;
- détection des pièces manquantes ;
- suivi des créances, dettes et reversements.

Objectif : produire et suivre les documents financiers.

### Phase 4 — Banque, caisse et trésorerie

Construire :
- import bancaire ;
- détection de doublons ;
- classification ;
- rapprochement ;
- caisse ;
- associés et avances ;
- prévisions de trésorerie à 13 semaines.

Objectif : visualiser la trésorerie réelle et prévisionnelle.

### Phase 5 — Précomptabilité, clôture et sauvegardes

Construire :
- centre `À traiter` ;
- précomptabilité ;
- checklist de clôture ;
- dérogations ;
- clôture ;
- réouverture motivée ;
- snapshots ;
- restauration en copie de travail ;
- historique des révisions.

Objectif : sécuriser la fin de mois.

### Phase 6 — Intégrations et robustesse

Construire :
- adaptateur Hostaway ;
- affichage des runs et anomalies source ;
- déclenchement contrôlé des scripts ;
- exports comptables et Power BI si prévus ;
- tests de non-régression métier ;
- sauvegarde et restauration vérifiées ;
- documentation complète.

---

## 10. Livrables attendus à chaque phase

À chaque phase, livrer :

1. code fonctionnel ;
2. migrations ;
3. tests ;
4. documentation de lancement ;
5. documentation de la structure de données ;
6. liste des opérations du catalogue couvertes ;
7. liste des opérations non couvertes ;
8. décisions prises et zones à confirmer ;
9. démonstration avec données de test ;
10. commit Git propre, sans fichiers temporaires, exports bruts ou secrets.

Ne pas mélanger dans un même commit :
- refonte large ;
- changement métier ;
- correction de bug ;
- fichiers de test temporaires ;
- exports de production.

---

## 11. Tests obligatoires

Tester au minimum :

### Référentiels
- création d’un propriétaire sans logement ;
- blocage/alerte d’un reversement sans RIB ;
- historique des taux de commission ;
- historique des coûts de ménage ;
- archivage sans disparition de l’historique.

### Réservations et facturation
- réservation hors Hostaway liée au bon logement et propriétaire ;
- acompte correctement déduit du solde ;
- facture émise non modifiable silencieusement ;
- ajustement justifié et relié au document ;
- pas de doublon lors d’un second clic ou d’un nouvel import.

### Banque
- une ligne bancaire ne crée pas une charge sans validation ;
- dédoublonnage ;
- rapprochement facture client ;
- rapprochement facture fournisseur ;
- transfert banque/caisse ;
- mouvement associé.

### Ménages
- matching ;
- relance après correction ;
- outrepassage motivé ;
- écarts visibles en clôture.

### Clôture et sauvegarde
- mois clôturé protégé ;
- réouverture motivée ;
- snapshot avant réouverture ;
- restauration dans copie de travail ;
- modification source post-clôture détectée.

---

## 12. Écrans prioritaires

Créer et valider rapidement, avec le design Stitch lorsqu’il sera fourni :

1. Accueil
2. Liste et fiche Acteur
3. Liste et fiche Logement
4. Réservation hors Hostaway
5. Ménages
6. Relevé propriétaire
7. Facture fournisseur / charge
8. Banque et rapprochement
9. Prévisions de trésorerie
10. Clôture mensuelle

---

## 13. Critères de réussite

L’application est considérée prête pour une première utilisation réelle lorsque :

- les référentiels peuvent être gérés sans dépendre d’Excel pour chaque modification ;
- les réservations hors Hostaway sont saisissables et exploitables ;
- les ménages peuvent être rapprochés et contrôlés ;
- les factures, charges, paiements, créances, dettes et reversements sont suivis ;
- la banque et la caisse sont rapprochables sans doublon ;
- la trésorerie est lisible ;
- les règles de clôture sont respectées ;
- les sauvegardes sont testées ;
- les calculs métier existants ne sont pas altérés ;
- chaque opération retenue du catalogue est soit disponible, soit explicitement planifiée et documentée ;
- l’application se lance localement avec une procédure reproductible.

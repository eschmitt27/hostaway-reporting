# Objectif du projet — Système de pilotage conciergerie

## 1. Objectif général

Le projet vise à construire un système de pilotage pour une activité de conciergerie. Le système doit permettre de suivre les réservations, les encaissements, les charges, les ménages, les commissions, les montants reversés aux propriétaires, les avantages des associés et les résultats de l’activité.

L’objectif n’est pas seulement de produire un tableau de bord. L’objectif est de construire un système fiable qui relie correctement plusieurs sources différentes, sans double comptage et sans mélanger les flux comptables, hors compta et personnels.

Le système doit permettre de répondre simplement à ces questions :

- quel chiffre d’affaires a été généré ;
- quel montant doit être reversé à chaque propriétaire ;
- quelle commission revient à la société ;
- quelles charges ont été payées ;
- quelles charges ont été payées avec un compte personnel ou en liquide ;
- quels montants constituent réellement un avantage pour un associé ;
- quel est le résultat comptable ;
- quel est le résultat réel ;
- quelles lignes doivent être contrôlées avant validation.

Le système doit être automatisé au maximum. Les saisies manuelles doivent être limitées aux informations qui ne peuvent pas être récupérées automatiquement de manière fiable.

## 2. Sources d’entrée du système

Le système doit être construit à partir de plusieurs sources. Chaque source doit rester identifiable afin de pouvoir contrôler l’origine d’une donnée.

### 2.1 API Hostaway

Hostaway est la source principale pour les réservations présentes dans Hostaway.

Les données Hostaway doivent être extraites automatiquement via GitHub Actions et stockées dans le dépôt du projet.

Les exports Hostaway doivent alimenter notamment :

- les logements Hostaway ;
- les réservations ;
- les détails de réservation ;
- les champs financiers détaillés ;
- les frais associés aux réservations ;
- les payouts calculés ;
- les anomalies Hostaway ;
- éventuellement les tâches de ménage, si elles sont disponibles et utiles.

Le dossier cible attendu est :

```text
exports/hostaway/master/
```

Les fichiers attendus dans ce dossier sont notamment :

```text
MASTER_REF_HA_Listings.csv
MASTER_FACT_HA_Reservations.csv
MASTER_FACT_HA_ReservationDetails.csv
MASTER_FACT_HA_ReservationFinanceFields.csv
MASTER_FACT_HA_ReservationFees.csv
MASTER_CALC_HA_Payout.csv
MASTER_CTRL_HA_Anomalies.csv
MASTER_RUN_Log.csv
```

Les fichiers Hostaway doivent être considérés comme des sources techniques. Les calculs finaux doivent utiliser les tables propres, pas directement les fichiers bruts JSON.

### 2.2 Référentiel interne `REF_Setup.xlsm`

Le fichier `REF_Setup.xlsm` est la source interne de référence.

Il doit contenir ou centraliser les informations stables utilisées par tout le système :

- les logements internes ;
- les identifiants Hostaway associés aux logements ;
- les propriétaires ;
- les taux de commission ;
- les types de logements ;
- les coûts de ménage de référence ;
- les associés ;
- les règles d’impact comptable / hors compta / hors résultat ;
- les catégories de charges ;
- les règles de rapprochement ou de classification si nécessaire.

Ce fichier est essentiel parce que Hostaway ne doit pas décider seul des règles internes de gestion. Par exemple, le coût de ménage de référence doit venir du référentiel interne, pas du prix de ménage Hostaway.

### 2.3 Exports bancaires du compte professionnel

Les exports bancaires du compte professionnel doivent servir à suivre :

- les encaissements ;
- les virements plateformes ;
- les dépenses professionnelles ;
- les dépenses personnelles payées avec le compte professionnel ;
- les virements vers les associés ;
- les remboursements ;
- les paiements fournisseurs ;
- les mouvements à rapprocher.

Le rapprochement bancaire complet sera construit dans un second temps. La logique cible est : import bancaire, normalisation, règles manuelles, lignes à envoyer à l’IA, retour classification, contrôle manuel des lignes incertaines.

### 2.4 Réservations hors Hostaway

Les réservations hors Hostaway sont minoritaires, mais elles doivent être suivies dans une table manuelle dédiée.

Cette table doit contenir au minimum :

- un identifiant unique de réservation hors Hostaway ;
- le mois ;
- le logement ;
- le propriétaire ;
- le total perçu ;
- le montant récupéré par un associé ;
- l’associé qui a récupéré le montant ;
- le montant reversé au propriétaire ;
- le ménage ;
- la commission ;
- l’acompte à reporter sur facture ;
- le statut ;
- l’indication éventuelle de comptabilisation.

Ces réservations ne doivent pas entrer automatiquement en produit comptable. Elles alimentent principalement le résultat réel ou extra-comptable, sauf si une colonne indique explicitement qu’elles doivent être comptabilisées.

### 2.5 Charges payées avec compte personnel ou liquide

Les charges payées avec un compte personnel ou avec du liquide doivent être saisies dans une table dédiée.

Cette table doit contenir au minimum :

- un identifiant unique de charge ;
- la date ;
- le mois de rattachement ;
- l’associé concerné ;
- le mode de paiement ;
- le montant ;
- la catégorie ;
- le logement ou le propriétaire si applicable ;
- le statut ;
- l’indication de prise en compte comptable ou non ;
- un justificatif ou une référence si disponible.

Ces charges doivent réduire le résultat réel. Elles doivent aussi réduire les avantages nets de l’associé concerné, car une charge payée pour la société n’est pas un avantage réel.

Elles ne doivent impacter le résultat comptable que si la colonne dédiée indique explicitement qu’elles doivent être prises en comptabilité.

### 2.6 IK et avantages associés

Un fichier ou module dédié doit permettre de suivre les avantages et IK par associé.

Un avantage peut venir uniquement de trois sources principales :

- un virement reçu sur le compte personnel ;
- une dépense personnelle passée sur le compte professionnel ;
- un montant récupéré dans une réservation hors Hostaway.

Le calcul attendu est :

```text
Avantage net = Avantages bruts + IK - Charges payées pour la société par l’associé
```

Le détail peut être utile pour les contrôles, mais l’attendu principal est d’obtenir un total fiable par associé.

### 2.7 Factures propriétaires et acomptes

Le système doit permettre de préparer, contrôler et produire les factures propriétaires.

La facture propriétaire est structurée en **deux blocs strictement séparés** (D033) :

**Bloc exploitation** (performance économique — ne varie qu'avec le séjour et les tarifs) :

- total payout ;
- ménage facturé ;
- commission conciergerie = (total payout − ménage) × taux ;
- charge fixe mensuelle (forfait récurrent contractuel, paramétrable par propriétaire/logement dans le référentiel — jamais une charge exceptionnelle) ;
- **revenu net d'exploitation propriétaire** = total payout − ménage − commission − charge fixe mensuelle.

Ce bloc n'inclut **jamais** : avances, acomptes Airbnb reçus par la conciergerie, paiements déjà reçus, remboursements, régularisations de trésorerie, achats ou charges exceptionnels.

**Bloc règlement / trésorerie** (ne modifie jamais le bloc exploitation) :

- montant total dû à la conciergerie ;
- acompte reçu via Airbnb (versé à la conciergerie uniquement, jamais traité comme payout propriétaire) ;
- autres paiements déjà reçus ;
- reste à payer à la conciergerie ;
- charges ou achats exceptionnels refacturés (hors revenu net d'exploitation) ;
- acomptes issus des réservations hors Hostaway.

La facture produit : sortie Excel de contrôle, table d'en-tête facture, table de lignes facture, statut de génération. Les champs nécessaires à une future sortie PDF sont préparés dès maintenant dans les tables, mais **aucun PDF propriétaire n'est livré au démarrage** (D040/P11). La mise en forme PDF sera décidée si besoin.

Pour les réservations annulées avec indemnité : commission sur le montant d'indemnité uniquement, sans déduction de ménage (D030).

### 2.8 Factures ménage, fournisseurs et justificatifs

Les factures de ménage, fournisseurs et justificatifs doivent servir aux contrôles et au suivi des charges.

Le système doit pouvoir comparer, à terme :

- les ménages attendus ;
- les ménages déclarés ou facturés ;
- les coûts standards ;
- les coûts réels ;
- les écarts par logement, propriétaire ou intervenant.

Ce module est important, mais il peut être construit après le socle réservations / payouts / résultats.

## 3. Attendus finaux du système

Le système doit produire des sorties simples, contrôlables et exploitables.

### 3.1 Tables master propres

Le système doit produire des tables propres, durables et mises à jour automatiquement.

Les tables Hostaway attendues sont :

```text
MASTER_REF_HA_Listings.csv
MASTER_FACT_HA_Reservations.csv
MASTER_FACT_HA_ReservationDetails.csv
MASTER_FACT_HA_ReservationFinanceFields.csv
MASTER_FACT_HA_ReservationFees.csv
MASTER_CALC_HA_Payout.csv
MASTER_CTRL_HA_Anomalies.csv
MASTER_RUN_Log.csv
```

Ces tables doivent pouvoir être consommées par Excel, Power Query ou Power BI.

### 3.2 Résultat propriétaire

Le système doit permettre de calculer, par propriétaire et par logement, les éléments suivants — en distinguant strictement le bloc exploitation du bloc règlement.

**Bloc exploitation (performance économique) :**

- total payout ;
- ménage facturé ;
- base de commission = total payout − ménage ;
- taux de commission (issu du référentiel) ;
- commission conciergerie = base × taux ;
- charge fixe mensuelle (paramétrable par propriétaire/logement, valeur 0 si non définie) ;
- **revenu net d'exploitation propriétaire** = total payout − ménage − commission − charge fixe mensuelle ;
- anomalies à corriger.

**Bloc règlement / trésorerie :**

- montant total dû à la conciergerie ;
- acompte reçu via Airbnb (réduit uniquement le reste à payer) ;
- autres paiements déjà reçus ;
- reste à payer à la conciergerie ;
- charges ou achats exceptionnels refacturés (hors revenu net d'exploitation) ;
- statut de règlement.

**Formules verrouillées :**

```text
base_commission                      = TotalPayout - MenageFacture
commission_conciergerie              = base_commission × TauxCommission
revenu_net_exploitation_proprietaire = TotalPayout - MenageFacture - commission_conciergerie - charge_fixe_mensuelle

montant_du_conciergerie = commission_conciergerie + MenageFacture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees
reste_a_payer           = montant_du_conciergerie - acompte_airbnb_recu - autres_acomptes - paiements_deja_recus
```

Cas annulation avec indemnité (D030) : `base_commission = CancellationPayout`, aucun ménage déduit.

`charge_fixe_mensuelle` vaut 0 si aucun forfait fixe n'est défini pour ce propriétaire/logement dans le référentiel.

### 3.3 Résultat réel

Le résultat réel doit donner la vision économique complète de l’activité.

Il doit intégrer :

- les réservations Hostaway ;
- les réservations hors Hostaway ;
- les charges professionnelles ;
- les charges payées personnellement ;
- les charges payées en liquide ;
- les ménages ;
- les commissions ;
- les flux hors compta qui ont un impact économique réel.

Le résultat réel doit pouvoir être consulté :

- par mois ;
- par logement ;
- par propriétaire ;
- au global.

### 3.4 Résultat comptable

Le résultat comptable doit rester distinct du résultat réel.

Il ne doit intégrer que les flux qui doivent être considérés comme comptables selon les règles internes.

Les réservations hors Hostaway, charges personnelles ou flux en liquide ne doivent pas impacter le résultat comptable par défaut. Ils ne doivent le faire que si une colonne de comptabilisation le prévoit explicitement.

### 3.5 Résultat hors compta / extra-comptable

Le système doit aussi permettre de suivre les flux hors compta ou extra-comptables.

Cela concerne notamment :

- certaines réservations hors Hostaway ;
- certains paiements en liquide ;
- certains montants récupérés par un associé ;
- certaines charges payées hors compte professionnel.

Ces flux doivent être visibles et contrôlés, même s’ils ne sont pas intégrés au résultat comptable.

### 3.6 Suivi des avantages associés

Le système doit produire une vision claire par associé :

- avantages bruts ;
- IK ;
- charges payées pour la société ;
- avantages nets ;
- solde à régulariser si nécessaire.

Les avantages associés doivent être séparés du résultat global. Le résultat global ne doit pas être ventilé par associé, mais les avantages doivent pouvoir l’être.

### 3.7 Contrôles et anomalies

Le système doit produire une table de contrôles.

Les contrôles doivent aider à détecter les incohérences avant validation.

Les contrôles prioritaires sont :

- logement Hostaway non relié au référentiel interne ;
- réservation sans logement ;
- réservation Airbnb active sans payout ;
- réservation Booking active sans payout calculable ;
- réservation annulée avec montant à contrôler ;
- réservation hors Hostaway non reprise dans les avantages ;
- montant récupéré hors Hostaway non repris dans les avantages ;
- acompte non rattaché à une facture ;
- charge personnelle sans statut clair ;
- charge personnelle sans associé ;
- charge personnelle non rattachée lorsque le rattachement est nécessaire ;
- écart entre ménage attendu et ménage facturé ou payé ;
- ligne sensible sans statut.

Certaines alertes doivent être bloquantes, notamment :

- acompte non rattaché à facture ;
- montant récupéré hors Hostaway non repris dans les avantages.

### 3.8 Sorties de consultation

Le système doit permettre de consulter :

- les résultats mensuels ;
- les résultats par logement ;
- les résultats par propriétaire ;
- l’activité globale ;
- les avantages par associé ;
- les contrôles bloquants ;
- les lignes à corriger.

La consultation pourra se faire dans Excel, Power Query, Power BI ou un autre outil selon l’évolution du projet.

## 4. Principes structurants

Le système doit distinguer trois niveaux :

1. **Résultat comptable** : les flux qui doivent être pris en compte dans la logique comptable classique.
2. **Résultat réel** : la vision économique réelle de l’activité, incluant certains flux hors compta ou payés hors compte professionnel.
3. **Avantages et IK associés** : ce que chaque associé récupère réellement, après déduction des charges payées pour la société.

Chaque ligne importante doit avoir :

- une source ;
- un identifiant unique ;
- une date ou un mois de rattachement ;
- un logement ou un propriétaire si applicable ;
- un associé si applicable ;
- un statut ;
- une règle d’impact ;
- un indicateur de contrôle si nécessaire.

Le système doit éviter :

- le double comptage d’un revenu ;
- l’oubli d’une charge ;
- l’intégration à tort d’un flux hors compta dans le résultat comptable ;
- la confusion entre un avantage réel et un remboursement de charge ;
- l’oubli d’un acompte propriétaire ;
- la perte d’une réservation ou d’un flux déjà connu.

## 5. Règle de conservation des données

Les données déjà connues ne doivent pas disparaître automatiquement.

Pour les données extraites via API, le système doit fonctionner selon une logique de mise à jour par clé :

- si la clé n’existe pas encore, la ligne est ajoutée ;
- si la clé existe et que la donnée a changé, la ligne est mise à jour ;
- si la clé existe et que la donnée n’a pas changé, la ligne est conservée ;
- si une ligne connue n’apparaît plus dans une extraction future, elle reste conservée dans le master.

Cette règle est indispensable pour ne pas perdre une réservation ou une information supprimée, archivée ou absente d’un extract futur.

Il n’est pas nécessaire de conserver un historique complet des versions. Le système doit conserver la dernière version connue de chaque ligne.

## 6. Clés techniques par table

Chaque table doit avoir une clé stable permettant l’upsert.

Clés attendues :

```text
REF_HA_Listings = listingMapId
FACT_HA_Reservations = reservation_id
FACT_HA_ReservationDetails = reservation_id
FACT_HA_ReservationFinanceFields = reservation_id + financeField_name
FACT_HA_ReservationFees = reservation_id + fee_id
CALC_HA_Payout = reservation_id
FACT_HA_Calendar = listingMapId + date
FACT_HA_CleaningTasks_Discovery = task_id si disponible, sinon clé composée
CTRL_HA_Anomalies = reservation_id + code
```

Chaque CSV technique doit contenir au minimum :

```text
PK
ROW_HASH
```

La clé `PK` permet d’identifier la ligne. Le `ROW_HASH` permet de savoir si la ligne a changé.

## 7. Règles Hostaway

Hostaway est la source principale pour les réservations plateformes.

Chaque réservation Hostaway doit être identifiée par son identifiant de réservation Hostaway, utilisé comme clé principale.

Hostaway doit permettre de récupérer :

- le logement ;
- le canal de réservation ;
- les dates de séjour ;
- le statut ;
- le prix total canal ;
- les frais ;
- les champs financiers détaillés ;
- les éléments nécessaires au calcul du payout ;
- les éventuelles anomalies.

Les réservations Hostaway servent à construire les résultats plateforme, les commissions propriétaires, le suivi des ménages et les contrôles.

## 8. Règles de payout plateformes

Le système doit distinguer le prix total canal du montant réellement reversé.

Le prix total canal peut inclure l’hébergement, le ménage, les taxes, les remises et les frais liés au canal.

Le payout plateforme correspond au montant utile pour calculer la commission de gestion et le net propriétaire.

### Airbnb

Pour Airbnb, le payout retenu est :

```text
Payout Airbnb = airbnbExpectedPayoutAmount
```

Si ce champ n’est pas disponible, le système peut utiliser en secours le champ financier détaillé correspondant à `airbnbPayoutSum`.

### Booking.com

Pour Booking.com, le payout retenu est calculé à partir des champs financiers détaillés de la réservation :

```text
Payout Booking = Total price from channel - City / Tourism tax - OTA payment processing fee - Host channel fee
```

Exemple validé :

```text
360,36 - 6,26 - 5,22 - 60,20 = 288,68 €
```

La source prioritaire est le détail financier de la réservation Hostaway, pas la liste simple des réservations.

En secours seulement, si les champs financiers détaillés sont absents, le système peut utiliser :

```text
totalPrice - taxe de séjour - payment charge extrait de la note Booking - channelCommissionAmount
```

### Direct / hors Hostaway

Les réservations directes ou hors Hostaway ne doivent pas utiliser Hostaway comme source financière principale.

Hostaway peut servir au planning ou au suivi, mais les montants réels doivent venir de la table manuelle des réservations hors Hostaway.

## 9. Réservations hors Hostaway

Les réservations hors Hostaway sont minoritaires, mais elles doivent être suivies proprement.

Elles ne doivent pas entrer automatiquement en produit comptable.

Elles doivent alimenter :

- le résultat réel ou extra-comptable ;
- le suivi propriétaire ;
- les acomptes sur facture ;
- les avantages associés lorsqu’un associé récupère une partie du montant.

Pour chaque réservation hors Hostaway, le système doit permettre de suivre :

- le propriétaire ;
- le logement ;
- le mois ;
- le montant total perçu ;
- le montant récupéré par un associé ;
- le montant reversé au propriétaire ;
- le ménage ;
- la commission ;
- l’acompte à reporter sur facture si nécessaire ;
- l’indication éventuelle de comptabilisation.

La formule d’acompte à contrôler est :

```text
Acompte facture = total perçu - ménage - commission - montant reversé propriétaire
```

L’acompte doit apparaître sur la facture propriétaire sans forcément détailler toute la réservation hors Hostaway.

## 10. Charges payées personnellement ou en liquide

Certaines charges peuvent être payées par un associé avec son compte personnel ou avec du liquide récupéré.

Ces charges doivent :

- réduire le résultat réel ;
- diminuer les avantages ou IK de l’associé concerné ;
- impacter le résultat comptable seulement si une colonne indique explicitement qu’elles doivent être comptabilisées.

Une charge payée personnellement pour la société n’est pas un avantage. Elle vient au contraire réduire l’avantage net de l’associé.

## 11. Avantages et IK associés

Le système doit suivre les avantages par associé séparément du résultat global.

Un avantage peut venir uniquement de trois sources principales :

- un virement reçu sur le compte personnel ;
- une dépense personnelle passée sur le compte professionnel ;
- un montant récupéré dans une réservation hors Hostaway.

La règle de calcul est :

```text
Avantage net = avantages bruts + IK - charges payées pour la société par l’associé
```

Le détail des avantages est utile pour le contrôle, mais le système doit surtout permettre d’obtenir un total net fiable par associé.

## 12. Ménages

Le système doit suivre les frais de ménage et, si possible, les tâches de ménage Hostaway.

Il faut distinguer :

- le frais de ménage facturé ou visible dans une réservation ;
- le coût de ménage de référence défini dans les fichiers internes ;
- les prestations réellement réalisées par les intervenants ;
- les éventuels écarts entre coût standard et coût réel.

Les coûts de ménage de référence doivent venir du référentiel interne, pas du prix de ménage Hostaway.

La découverte des tâches ménage Hostaway est utile, mais secondaire. Elle ne doit pas ralentir inutilement les extractions quotidiennes si elle n’est pas nécessaire au calcul principal.

## 13. Modules à construire

Le système doit être construit progressivement autour de ces blocs :

1. Référentiel interne des logements, propriétaires, associés, coûts de ménage et règles d’impact.
2. Extraction Hostaway automatisée.
3. Master Hostaway avec réservations, détails financiers, frais, payouts et anomalies.
4. Table manuelle des réservations hors Hostaway.
5. Table des charges payées personnellement ou en liquide.
6. Table des avantages et IK associés.
7. Module de résultat comptable.
8. Module de résultat réel.
9. Module de facturation propriétaire et acomptes.
10. Module de contrôle et anomalies.
11. Module de rapprochement bancaire, à construire ensuite.

## 14. Priorité de construction

Le système doit être construit dans l’ordre suivant :

1. Stabiliser les référentiels.
2. Stabiliser l’extraction Hostaway et les payouts plateforme.
3. Construire les tables manuelles hors Hostaway et charges personnelles.
4. Construire le suivi avantages / IK.
5. Construire le résultat réel et le résultat comptable.
6. Construire les contrôles bloquants.
7. Construire les sorties propriétaires et factures.
8. Ajouter ensuite les rapprochements bancaires et automatisations avancées.

## 15. Objectif d’usage

Le système doit être utilisable régulièrement avec un minimum de saisie manuelle.

L’utilisateur doit pouvoir :

- déposer ou actualiser les sources ;
- lancer une mise à jour ;
- voir les résultats ;
- corriger uniquement les lignes à contrôler ;
- éviter de retraiter manuellement les réservations ou flux déjà connus.

Le système ne doit pas chercher à être parfait dès le départ. Il doit d’abord être fiable, contrôlable et modulable.

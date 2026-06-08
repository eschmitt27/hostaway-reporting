# ARCHITECTURE_DONNEES.md

> **Version définitive consolidée.** Socle ancré dans les fichiers réels (19 onglets `REF_Setup`, 9 CSV master du run `20260523_005752`), enrichi des raffinements d'implémentation.
> Ce document **n'implémente rien**. Il sert de référence pour construire le système lot par lot. Il doit être assez clair pour qu'un autre développeur ou assistant puisse construire le système.
>
> **Décisions métier validées :**
> - Assiette commission = **payout plateforme encaissé − frais de ménage** (le payout inclut le ménage facturé au voyageur).
> - Réservations hors Hostaway = **une ligne par réservation**.
> - Séjours `ownerStay` = **exclus totalement du résultat**.
> - Convention table de flux = **montant positif + colonne `sens` (PRODUIT / CHARGE)**.
> - Identifiants manuels = **lisibles, stables, non déductibles d'un montant seul** ; fichiers de saisie **contrôlés** (listes déroulantes, colonnes obligatoires, statut, alertes doublons).

---

## Table des matières (lecture ciblée)

> Ce fichier est long (~18 k tokens). **Ne pas le lire en entier par défaut.** Voir `CLAUDE.md §5.bis` pour la matrice « lot → sections ». Sauter directement à la section utile.

| § | Section | Lire surtout pour |
|---|---|---|
| §1 | Objectif du système (+ §1.1 contexte juridique) | Onboarding |
| §2 | Principes structurants (upsert, flux, banque, familles de tables) | Tous lots |
| §3 | Vue d'ensemble des modules | Onboarding |
| §4 | Sources d'entrée (+ §4.1 GitHub/OneDrive, §4.2 fichiers saisie) | Lots 0, 2, 3 |
| §5 | Tables de sortie (existantes / à construire / consultables) | Lots 1, 9, 10 |
| §6 | Architecture Hostaway (tables, statuts, perf) | Lot 1 |
| §7 | Règles de payout Hostaway (Airbnb / Booking / Direct) | Lots 1, 10 |
| §8 | Commission & net propriétaire (+ §8.3 ménage par canal) | Lot 10 |
| §9 | Réservations hors Hostaway (+ §9.5 table commune) | Lots 4, 4 bis |
| §10 | Charges perso/liquide (+ §10.4 acomptes, §10.5 liquide, §10.6 corrections) | Lots 3, 5 |
| §11 | Ménages (+ §11.4 M04, §11.5 ménages externes) | Lot 6 |
| §12 | IK & avantages associés | Lot 7 |
| §13 | Banque & rapprochement (+ §13.6 source réelle CM) | Lot 8 |
| §14 | Table de flux unifiée `MASTER_CALC_Flux` | Lot 9 |
| §15 | Résultats réel / comptable / hors compta | Lot 10 |
| §16 | Clés de liaison (+ §16.2 nomenclature, §16.3 rapprochements) | Lots 0, 2 |
| §17 | Livrables propriétaires | Lot 12 |
| §18 | Contrôles de cohérence (+ §18.5 contrôles de saisie) | Lots 9, 11 |
| §19 | Ordre de construction recommandé | Planification |
| §20 | Points de vigilance | Tous lots |
| §21 | Arbitrages non bloquants restants | Décisions ouvertes |
| §22 | Résultat du contrôle de cohérence | — |
| §23 | Conventions transverses (statuts, clôture, arrondi, `REF_Statuts_Payout`) | Lots 0, 8, 10, 11 |

---

## 1. Objectif du système

Le système doit produire une vision financière et opérationnelle fiable d'une conciergerie courte durée gérant ~16 logements (référentiel : 17 logements dont 1 hors Hostaway), majoritairement à Toulouse et Blagnac.

Il concilie plusieurs réalités qui ne vivent pas dans le même outil :

- les **réservations Hostaway** (Airbnb, Booking, VRBO) issues de l'API ;
- les **réservations hors Hostaway** (canal `direct`), minoritaires, encaissées en liquide ou virement ;
- les **flux bancaires** du compte professionnel ;
- les **acomptes** sur factures propriétaires ;
- les **charges** payées avec comptes personnels ou en liquide ;
- les **IK et avantages** des associés ;
- trois lectures de résultat : **réel**, **comptable**, **hors compta** ;
- les **contrôles de cohérence** entre sources.

Automatisation maximale ; les fichiers manuels ne couvrent que ce qu'aucune source automatique ne fournit. Le système doit rester **robuste, modulaire, maintenable** et **prioritairement exploitable dans Excel / Power Query** (livrable initial). Les tables et CSV sont **conçus pour être directement exploitables dans Power BI** par l'utilisateur lui-même, mais **aucun lot ne livre un dashboard Power BI** (D043).

### 1.1 Contexte juridique et conséquence sur le résultat comptable

L'activité est **opérationnelle** mais la **SAS porteuse est nouvelle** et son enregistrement n'est pas encore complètement stabilisé. Le système prépare les flux, les contrôles et la distinction IC / HC / HR pour la future exploitation comptable, mais **ne suppose pas d'historique comptable existant**. Aucune écriture comptable passée n'est à rechercher. Le résultat comptable (somme des flux `IC`) démarre à partir des flux validés une fois la comptabilité opérationnelle ; les flux antérieurs restent visibles dans le résultat réel (`IC + HC`) sans rétroactivité comptable forcée.

---

## 2. Principes structurants

### 2.1 Ne jamais supprimer une donnée connue (upsert par clé stable)

Chaque table a une `PK` et un `ROW_HASH`.

| Cas | Traitement |
|---|---|
| `PK` nouvelle | Ajouter la ligne |
| `PK` existante + `ROW_HASH` modifié | Remplacer / mettre à jour |
| `PK` existante + `ROW_HASH` identique | Ne rien modifier |
| Ancienne `PK` absente du nouvel extract | **Conserver** la ligne |

Jamais de suppression automatique.

### 2.2 Séparer source brute / normalisé / calcul / contrôle

Pour chaque module : données sources → tables normalisées → tables calculées → tables de contrôle. Exemple Hostaway : JSON API → réservations/listings/finance fields/fees → payout → anomalies.

### 2.3 La table de flux unifiée est la colonne vertébrale

`MASTER_CALC_Flux` reçoit **tous** les événements économiques sous une forme commune (un produit, une charge, un acompte, un avantage, une déduction, un remboursement, un ménage, une commission = une ligne). Les trois résultats deviennent alors de **simples filtres** sur le code impact. C'est ce qui garantit la cohérence entre résultat réel, comptable et hors compta.

### 2.4 Les contrôles bloquent les zones dangereuses

Le système est bâti avec des contrôles, pas seulement des calculs. Sont bloquants uniquement les cas qui rendent un résultat ou une facture faux ; le reste est « à contrôler ».

### 2.5 Les fichiers manuels doivent empêcher les erreurs simples

Toute table saisie à la main utilise des listes déroulantes issues du référentiel, des colonnes obligatoires mises en évidence si vides, un statut de contrôle, et des alertes visuelles (mise en forme conditionnelle) sur les doublons et incohérences de montant. Ces garde-fous sont faits en Excel / Power Query, sans macro complexe tant que des règles simples suffisent (détail §10.5 et §18.5).

### 2.6 La banque ne doit pas doubler les flux métier

La banque constate les encaissements et décaissements réels, mais ne recrée pas automatiquement un produit ou une charge déjà porté par une source métier.

| Cas | Source économique dans `MASTER_CALC_Flux` | Rôle de la banque |
|---|---|---|
| Payout Airbnb / Booking | Hostaway (`MASTER_CALC_HA_Payout`) | Rapprochement / contrôle |
| Réservation hors Hostaway liquide/virement | Table HH manuelle | Rapprochement caisse / banque |
| Charge payée par compte perso/liquide | Table charges manuelle | Rapprochement si remboursement associé |
| Dépense passée directement sur compte pro | Banque | Source économique |
| Virement associé | Banque | Source avantages / IK |
| Abonnement logiciel prélevé | Banque ou règle récurrente validée | Source économique + contrôle anti-doublon |

Règle générale : **source métier structurée prioritaire ; banque en contrôle**, sauf quand la banque est la seule source disponible.

### 2.7 Familles de tables

| Préfixe | Rôle | Alimentation |
|---|---|---|
| `REF_*` | Référentiels | Manuelle (`REF_Setup`) |
| `MASTER_REF_HA_*`, `MASTER_FACT_HA_*` | Faits/dimensions Hostaway | Automatique |
| `MASTER_FACT_MAN_*` | Faits saisis manuellement | Manuelle / semi-auto |
| `BRUT_Banque`, `NORM_Banque`, `IA_Classification` | Pipeline bancaire (brut, normalisé, classification) | Semi-auto |
| `MASTER_CALC_*` | Tables calculées (payout, flux, résultats) | Dérivées |
| `MASTER_CTRL_*` | Contrôles de cohérence | Dérivées |

---

## 3. Vue d'ensemble des modules

```text
MODULE 0  - Référentiels (REF_Setup)           [À PRÉPARER]
MODULE 1  - Hostaway API                        [extraction existante, non validée]
MODULE 2  - Réservations hors Hostaway
MODULE 3  - Charges perso / liquide / compte pro + acomptes
MODULE 4  - Ménages
MODULE 5  - IK & avantages associés
MODULE 6  - Banque & rapprochement bancaire
MODULE 7  - Table de flux unifiée (MASTER_CALC_Flux)
MODULE 8  - Résultats réel / comptable / hors compta
MODULE 9  - Contrôles de cohérence
MODULE 10 - Livrables propriétaires / exports Excel (Power BI = utilisateur, hors lots)
```

Le Module 1 (Hostaway) est déjà construit. Tous les autres dépendent du Module 0 et convergent vers le Module 7.

---

## 4. Sources d'entrée

| Source | Type | Alimentation | État |
|---|---|---|---|
| API Hostaway | API | Automatique | **Opérationnelle** (`hostaway_master_upsert_fast.py`) |
| `REF_Setup` | Excel maître (19 onglets) | Manuelle | **Opérationnel** |
| Crédit Mutuel (banque pro) | Export Excel | Manuelle → auto | À brancher (Module 6) |
| Factures fournisseurs | PDF → Excel | Semi-auto | À brancher |
| Factures ménage externe | Excel standardisé | Semi-auto | À brancher |
| Suivi ménage interne | Excel | Manuelle | À brancher |
| `M04_MENAGES_PowerQuery.xlsx` | Excel + Power Query actualisable | Semi-auto (refresh requis avant lecture) | En cours — produit `tbl_MASTER_FACT_MEN_Menages` (§11.4) |
| `2026_03_BRUT_Banque_CreditMutuel.xlsx` | Export bancaire brut Crédit Mutuel | Manuel → pipeline banque | À brancher (Module 6) — détail §13.6 |
| Dépenses terrain | Formulaire mobile | Manuelle | À brancher |
| Caisse espèces | Excel caisse | Manuelle | À brancher |
| `SAISIE_Charges_Flux.xlsx` | **Source unique** des achats, charges, consommables, produits ménage, linge, lavage, matériel, charges perso/liquide, dépenses perso sur compte pro | Manuelle | À construire (Lot 3) |
| Facturation propriétaires | Excel / génération | Semi-auto | À brancher (livrable) |

**Sources manuelles strictement nécessaires** : réservations hors Hostaway (montant réellement encaissé/reversé), charges perso/liquide (invisibles dans le compte pro), IK/avantages (décision métier), acomptes propriétaires (rattachement facture), caisse espèces, corrections manuelles.

**Référentiel central `REF_Setup`** : source de vérité pour logements (dont `charge_fixe_mensuelle` par logement — D039), propriétaires, associés, types de logement, taux de commission, coûts standards ménage (exécution, D037), types de flux, catégories de charges, codes impact (`IC`/`HC`/`HR`), **statuts de contrôle (`REF_Statuts`, valeurs fermées — §23.1)**, modes de paiement, cartes/personnes, et mappings libellés→logements.

### 4.1 Synchronisation GitHub → OneDrive

Le dépôt GitHub automatise les extractions Hostaway, mais Excel / Power Query doit lire les CSV depuis le dossier local OneDrive synchronisé, **pas** depuis les artefacts temporaires de GitHub Actions.

```text
GitHub Actions met à jour exports/hostaway/master/
→ le dépôt local (dans OneDrive) est mis à jour par git pull
→ Excel / Power Query lit les CSV locaux dans exports/hostaway/master/tables/
```

Le `git pull` peut être manuel au départ, puis automatisé (tâche planifiée Windows ou script PowerShell). Les requêtes Power Query pointent vers le dossier local synchronisé pour rester utilisables sans ouvrir GitHub.

### 4.2 Règles minimales des fichiers de saisie manuelle

| Règle | Application attendue |
|---|---|
| Identifiant stable | une colonne `*_id` lisible, non recalculée à chaque ouverture |
| Listes déroulantes | valeurs issues du `REF_Setup` : logement, propriétaire, associé, type de flux, mode de paiement, statut |
| Colonnes obligatoires | mises en évidence si vides |
| Statut de contrôle | au minimum : à contrôler / validé / bloquant / ignoré |
| Détection doublons | mise en forme conditionnelle sur les clés ou quasi-clés |
| Contrôle montants | alertes si total perçu, reversé, acompte, charge ou avantage incohérents |
| Justificatif | référence ou lien pour les charges et remboursements |

S'applique en priorité aux réservations hors Hostaway, charges perso/liquide, acomptes propriétaires, IK/avantages et caisse espèces.

---

## 5. Tables de sortie attendues

### 5.1 Tables produites — Module Hostaway (run `20260523_005752` — **non validées sur données réelles**)

| Table | Lignes | PK |
|---|---|---|
| `MASTER_REF_HA_Listings` | 16 | `listingMapId` |
| `MASTER_FACT_HA_Reservations` | 1505 | `reservation_id` |
| `MASTER_FACT_HA_ReservationDetails` | 1505 | `reservation_id` |
| `MASTER_FACT_HA_ReservationFinanceFields` | 13367 | `reservation_id + financeField_name` |
| `MASTER_FACT_HA_ReservationFees` | 614 | `reservation_id + fee_id` |
| `MASTER_CALC_HA_Payout` | 1505 | `reservation_id` |
| `MASTER_CTRL_HA_Anomalies` | 28 | `reservation_id + code` |
| `MASTER_FACT_HA_CleaningTasks_Discovery` | 451 | `task_id` |
| `MASTER_RUN_Log` | 1 | `run_id` |

### 5.2 Tables à construire

| Table | PK | Module |
|---|---|---|
| `MASTER_FACT_MAN_ReservationsHorsHostaway` | `reservation_hh_id` | 2 |
| `MASTER_CALC_Reservations` | `reservation_calc_id` | 2/7 (table commune) |
| `MASTER_FACT_MAN_Charges` | `charge_id` | 3 |
| `MASTER_FACT_MAN_AcomptesProprietaires` | `acompte_id` | 3 |
| `MASTER_FACT_MEN_Menages` | `task_id` / composite | 4 |
| `MASTER_FACT_MEN_MenagesExternes` | `menage_externe_id` | 4 (Lot 6c) |
| `MASTER_FACT_MAN_IK_Avantages` | `avantage_id` (saisie des flux associés uniquement) | 5 |
| `MASTER_CALC_AvantagesAssocies` | `personne_id + mois` (calculée) | 5 |
| `MASTER_FACT_MAN_Corrections` | `correction_id` | transverse |
| `MASTER_CALC_Flux` | `flux_id` | 7 |
| `MASTER_CALC_Resultats` | `mois + périmètre + vision` | 8 |
| `MASTER_CALC_NetProprietaire` | `proprietaire_id + logement_id + mois` ou `facture_id` | 10 |
| `MASTER_CALC_Commissions` | `reservation_id` / composite | 8 |
| `MASTER_CTRL_Coherence` | `source_pk + code_controle` | 9 |
| `FACT_FACTURE_ENTETE` | `facture_id` | 12 (D040) |
| `FACT_FACTURE_LIGNES` | `facture_id + ligne_num` | 12 (D040) |
| `BRUT_Banque` | `import_id + ligne_source` | 6 |
| `NORM_Banque` | `mouvement_id` | 6 |
| `IA_Classification` | `mouvement_id` | 6 |
| `CTRL_A_CONTROLER` | `mouvement_id + code` | 6 |
| `LOG_Traitement` | `run_id + étape` | 6 |
| `REF_Cloture_Mensuelle` | `mois` | **0** (structure créée au Lot 0 ; exploitée au Lot 8) |
| `REF_Statuts_Payout` | `statut_calcul_payout` | **0** (créée au Lot 0 — voir D021) |

> Les tables bancaires (`BRUT_Banque`, `NORM_Banque`, `IA_Classification`, `CTRL_A_CONTROLER`, `LOG_Traitement`) gardent les noms déjà prévus pour le pipeline banque afin de rester compatibles avec les macros / scripts futurs (détail §13).

### 5.3 Résultats consultables attendus

Résultat réel/comptable/hors compta par mois ; résultat par logement, par propriétaire, global ; net propriétaire ; commission ; ménages retenus ; acomptes ; avantages bruts/nets par associé ; charges perso/liquide ; anomalies bloquantes et à contrôler.

---

## 6. Architecture Hostaway (Module 1 — extraction existante, non validée)

### 6.1 Tables et clés

| Table | Clé | Rôle |
|---|---|---|
| `MASTER_REF_HA_Listings` | `listingMapId` | Logements Hostaway |
| `MASTER_FACT_HA_Reservations` | `reservation_id` | Réservations |
| `MASTER_FACT_HA_ReservationDetails` | `reservation_id` | Détail JSON |
| `MASTER_FACT_HA_ReservationFinanceFields` | `reservation_id + financeField_name` | Champs financiers détaillés |
| `MASTER_FACT_HA_ReservationFees` | `reservation_id + fee_id` (fallback si absent) | Frais |
| `MASTER_CALC_HA_Payout` | `reservation_id` | Payout calculé |
| `MASTER_CTRL_HA_Anomalies` | `reservation_id + code` | Anomalies |
| `MASTER_FACT_HA_CleaningTasks_Discovery` | `task_id` | Tâches ménage |

### 6.2 Données réelles observées

Répartition par canal : `airbnbOfficial` 1335, `bookingcom` 110, `direct` 31, `vrboical` 29.
Champ déjà calculé `source_financiere_prevue` : HOSTAWAY_AIRBNB / HOSTAWAY_BOOKING / MANUEL_HORS_HOSTAWAY / A_CONTROLER.
Finance fields les plus fréquents : `baseRate`, `totalPriceFromChannel`, `hostChannelFee`, `cleaningFee`, `totalPaid`, `airbnbPayoutSum`, `cityTax`, `otaPaymentProcessingFee`, `vat`.
Reservation fees : tous les `fee_id` renseignés au dernier run (fallback non sollicité mais à conserver).

### 6.3 Règle de performance (extraction incrémentale)

```text
si reservation_id inconnue                                   → récupérer le détail API
si reservation_id connue mais updatedOn/latestActivityOn changé → récupérer le détail API
si reservation_id connue et inchangée                        → réutiliser le détail stocké
```

Même résultat final avec moins d'appels API.

### 6.4 Traitement des statuts

| Statut | Traitement | Volume réel |
|---|---|---|
| `new` | Inclus | 1269 |
| `modified` | Inclus | 62 |
| `cancelled` | Exclu, **sauf contrôle si montant/payout présent** | 77 |
| `ownerStay` | **Exclu du résultat**, éventuellement tracé pour l'occupation | 9 |
| `inquiry`, `declined`, `expired`, `inquiryPreapproved`, `inquiryNotPossible` | Exclus | 88 |

---

## 7. Règles de payout Hostaway

### 7.1 Airbnb (vérifié)

```text
PayoutPlateforme = airbnbExpectedPayoutAmount
   fallback : financeField[airbnbPayoutSum]
```

### 7.2 Booking (vérifié)

```text
PayoutPlateforme = financeField[totalPriceFromChannel]
                 - financeField[cityTax]
                 - financeField[otaPaymentProcessingFee]
                 - financeField[hostChannelFee]

fallback si financeField absents (marqué moins fiable) :
PayoutPlateforme = totalPrice - taxe de séjour (reservationFees)
                 - payment charge (guestNote) - channelCommissionAmount
```

### 7.3 Direct / hors Hostaway

Hostaway n'est jamais la source financière. Il sert d'existence/planning ; le montant vient de la table manuelle (§9).

`InclureResultatAuto` (=1 si OK) et `StatutCalculPayout` pilotent l'inclusion automatique au résultat.

### 7.4 Statuts de calcul payout — valeurs fermées (D021)

Le champ `statut_calcul_payout` dans `MASTER_CALC_HA_Payout` utilise les valeurs ci-dessous (référentiel `REF_Statuts_Payout`, créé au Lot 0).

| `statut_calcul_payout` | Signification |
|---|---|
| `NORMAL` | Payout calculé, réservation active |
| `ANNULE_SANS_PAYOUT` | Annulée, aucun montant |
| `ANNULE_AVEC_PAYOUT` | Annulée avec indemnité → règle D030 s'applique |
| `PAYOUT_ABSENT` | Réservation active sans payout calculable → **BLOQUANT** |
| `PAYOUT_INCOMPLET` | Champs financiers partiels → `A_CONTROLER` |
| `A_CONTROLER` | Cas non résolu (VRBO Unknown, direct sans montant) |

---

## 8. Commission et net propriétaire

### 8.1 Assiette et commission (validé)

Le `PayoutPlateforme` **inclut le ménage facturé au voyageur**. L'assiette de commission s'obtient donc en retirant le ménage :

```text
Assiette       = PayoutPlateforme - MenageRetenu
CommissionGestion = Assiette × TauxCommission        (REF_Proprietaires, 0,12 à 0,19)
```

### 8.2 Net propriétaire

```text
NetProprietaire = PayoutPlateforme - MenageRetenu - CommissionGestion
                = (PayoutPlateforme - MenageRetenu) × (1 - TauxCommission)
```

Exemple vérifié (Booking, taux 15 % illustratif) : payout 257,12 − ménage 55 = assiette 202,12 → commission 30,32 → net propriétaire 171,80.

### 8.3 Source du ménage retenu — POINT TECHNIQUE CRITIQUE

Le ménage à soustraire **ne se trouve pas au même endroit selon le canal** :

| Canal | Où lire le ménage | Constat sur les données |
|---|---|---|
| **Airbnb** (1335) | `MASTER_FACT_HA_ReservationFinanceFields` → `financeField_name = cleaningFee` (1210/1252 renseignés) | La colonne `CleaningFee` de la table payout est **toujours vide** pour Airbnb |
| **Booking** (110) | Colonne `CleaningFee` de la table payout (95/110) | Utilisable directement |
| **VRBO / Direct** | Finance fields ou saisie manuelle | Ménage non isolé |

> ⚠️ **Sans cette logique par canal, la commission Airbnb serait calculée sur une assiette incluant le ménage → surcommission systématique sur ~88 % du volume.** À implémenter avec une récupération du ménage canal par canal.

Distinction à garder : le **ménage retenu** (facturé au voyageur) sert au calcul propriétaire ; le **coût réel ménage** (facture prestataire) sert au résultat réel et au contrôle d'écart (§11).

### 8.4 Réservation annulée avec indemnité — `CancellationPayout` (D030)

Pour toute réservation avec `statut_calcul_payout = ANNULE_AVEC_PAYOUT` (`CancellationPayout > 0`) :

```text
BaseCommission         = CancellationPayout
CommissionConciergerie = CancellationPayout × TauxCommission
NetProprietaire        = CancellationPayout − CommissionConciergerie
```

**Aucun ménage n'est déduit** (pas de prestation réalisée). L'ancienne anomalie `CANCELLED_AVEC_MONTANT` (à contrôler) devient une règle active via ce statut.

### 8.5 `revenu_net_exploitation_proprietaire` (D031)

Indicateur économique pur. **Exclut impérativement** : avances, acomptes Airbnb versés à la conciergerie, paiements déjà reçus, montants réglés par le propriétaire, remboursements, régularisations de trésorerie, achats exceptionnels, matériel exceptionnel, charges exceptionnelles non récurrentes, ajustements ponctuels.

```text
CommissionConciergerie               = (TotalPayout − MenageFacture) × TauxCommission
revenu_net_exploitation_proprietaire = TotalPayout − MenageFacture − CommissionConciergerie − charge_fixe_mensuelle
```

`charge_fixe_mensuelle` = montant récurrent facturé contractuellement chaque mois (forfait logiciel, forfait consommables récurrent, forfait contractuel fixe). **Paramétrable par propriétaire/logement dans `REF_Logements`** (D039). Valeur = 0 si aucun forfait défini. Jamais une charge exceptionnelle.

> Relation avec §8.2 : `revenu_net_exploitation = NetProprietaire − charge_fixe_mensuelle`. Les deux indicateurs sont distincts et doivent coexister.

### 8.6 Séparation exploitation / règlement — deux blocs non communicants (D033)

**Bloc exploitation** (performance économique — ne varie qu'avec le séjour et les tarifs) :

| Champ | Formule |
|---|---|
| `total_payout` | PayoutPlateforme |
| `menage_facture` | Ménage retenu (§8.3) |
| `base_commission` | `total_payout − menage_facture` |
| `taux_commission` | Taux `REF_Proprietaires` |
| `commission_conciergerie` | `base_commission × taux_commission` |
| `charge_fixe_mensuelle` | Forfait fixe contractuel |
| `revenu_net_exploitation_proprietaire` | `total_payout − menage_facture − commission_conciergerie − charge_fixe_mensuelle` |

**Bloc règlement / trésorerie** (mouvements de cash — ne modifie jamais le bloc exploitation) :

| Champ | Formule / source |
|---|---|
| `montant_du_conciergerie` | `commission_conciergerie + menage_facture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees` |
| `aircover_recu_par_proprietaire_montant` | **Information uniquement** (D042/AC2) — montant AirCover perçu directement par le propriétaire. Ne modifie ni revenu net ni règlement. |
| `aircover_recu_par_proprietaire_date` | Date à laquelle le remboursement AirCover a été reçu par le propriétaire. |
| `aircover_recu_par_proprietaire_motif` | Motif du remboursement AirCover (description libre). |
| `acompte_conciergerie_recu_via_airbnb` | Versement Airbnb → conciergerie uniquement (D032) |
| `autres_acomptes_conciergerie_recus` | Autres avances reçues |
| `paiement_deja_recu` | Règlements directs reçus |
| `reste_a_payer_conciergerie` | `montant_du_conciergerie − acomptes − paiements` |
| `statut_reglement_conciergerie` | REF_Statuts (VALIDE / A_CONTROLER / BLOQUANT) |

`charges_exceptionnelles_refacturees` : modifie `montant_du_conciergerie` uniquement, **jamais** `revenu_net_exploitation_proprietaire` (D034).

---

## 9. Réservations hors Hostaway (Module 2)

**Besoin confirmé** : sur 31 `direct`, seules **13 sont des réservations payantes** (`new`), 9 `ownerStay` (montant 0, exclus), 7 `cancelled`, 2 `modified`. Les 29 VRBO sont en `paymentStatus = Unknown` (flag `A_CONTROLER`). **Granularité retenue : une ligne par réservation.**

### 9.1 Cas de figure réels

Une réservation hors Hostaway peut combiner : l'associé récupère du liquide/virement ; il en reverse une partie au propriétaire ; il garde la part commission + ménage ; l'argent peut servir à payer un prestataire ; le solde devient un acompte sur facture.

### 9.2 Table `MASTER_FACT_MAN_ReservationsHorsHostaway`

Type de flux `RESERVATION_HORS_HOSTAWAY` (code impact défaut `HC`, sauf `comptabilisation` explicite).

| Colonne | Rôle |
|---|---|
| `reservation_hh_id` (PK) | Clé séquentielle (`RESHH_0001`) |
| `ROW_HASH` | Hash de ligne |
| `mois` | Mois de rattachement |
| `proprietaire_id`, `logement_id` | Affectation (`REF_Proprietaires` / `REF_Logements`) |
| `reservation_id_hostaway` | Lien optionnel si la résa existe dans Hostaway |
| `date_arrivee`, `date_depart`, `nuits` | Séjour |
| `total_percu` | Total réellement encaissé |
| `menage` | Ménage retenu |
| `taux_commission`, `commission` | Taux et commission calculée |
| `montant_recupere`, `associe_id_recuperateur` | Montant récupéré et associé concerné (`REF_Associes`) |
| `montant_reverse_proprietaire` | Reversé au propriétaire |
| `acompte_facture` | À reprendre sur facture propriétaire |
| `mode_paiement_id` | Espèces / virement / autre (`REF_Modes_Paiement`) |
| `code_impact` | `HC` par défaut |
| `comptabilisation` | OUI/NON (passage en compta) |
| `statut_controle` | OK / à contrôler / bloquant |
| `commentaire` | Note libre |

### 9.3 Formule d'acompte (validée, périmètre limité)

```text
AcompteFacture = TotalPercu - Menage - Commission - MontantReverseProprietaire
```

> Applicable **uniquement** aux réservations hors Hostaway, où chaque composant est saisi manuellement. **Ne pas réutiliser comme calcul d'acompte générique** : sur Hostaway, ménage et commission ne sont pas isolés de la même façon (§8.3). Cette formule doit être contrôlée automatiquement.

### 9.4 Avantage associé lié

Le `montant_recupere` alimente les avantages bruts de l'associé. Si ce montant sert à payer une charge, celle-ci réduit l'avantage net :

```text
Montant récupéré = 100   →  avantage brut +100
Charge payée avec  = 100   →  déduction      -100
Avantage net       =   0
```

### 9.5 Table commune des réservations — `MASTER_CALC_Reservations`

**Objectif.** Consolider **toutes** les réservations (Hostaway, hors Hostaway, VRBO manuelles, manuelles hors plateforme) sous un schéma unique, pour empêcher tout double comptage avant déversement dans `MASTER_CALC_Flux`.

Cette table ne crée pas de données par elle-même : elle réconcilie les sources existantes.

| Colonne | Rôle |
|---|---|
| `reservation_calc_id` (PK) | Identifiant consolidé (`RES-AAAA-MM-{SOURCE}-{COMPTEUR}`) |
| `ROW_HASH` | Hash de ligne |
| `source` | `HOSTAWAY_AIRBNB` / `HOSTAWAY_BOOKING` / `HOSTAWAY_VRBO` / `HOSTAWAY_DIRECT` / `MANUEL_HORS_HOSTAWAY` |
| `reservation_id_hostaway` | Lien Hostaway si applicable |
| `reservation_hh_id` | Lien table manuelle si applicable |
| `logement_id`, `proprietaire_id` | Affectation |
| `date_arrivee`, `date_depart`, `nuits` | Séjour |
| `montant_retenu` | Montant qui alimentera `MASTER_CALC_Flux` (un seul par réservation) |
| `source_montant` | `HOSTAWAY` / `MANUEL` / `MANUEL_VRBO` |
| `code_impact` | `IC` / `HC` selon la source |
| `statut_controle` | Validé / à contrôler / bloquant |
| `commentaire` | |

**Règles de réconciliation.**
- Une réservation `direct` Hostaway avec `totalPrice > 0` **et** une ligne hors Hostaway liée par `reservation_id_hostaway` : la table commune retient une seule ligne, `source_montant = MANUEL`.
- Une réservation VRBO `paymentStatus = Unknown` : tant que le montant n'est pas renseigné manuellement, la ligne est `statut_controle = A_CONTROLER` et n'alimente pas `MASTER_CALC_Flux`.
- Une réservation Hostaway Airbnb / Booking sans contrepartie manuelle : `source_montant = HOSTAWAY`, alimentation directe.

**Contrôles dédiés.**
- `RESERVATION_DOUBLON_HOSTAWAY_HH` (bloquant) : `reservation_id_hostaway` rattaché à 2+ lignes sans lien explicite.
- `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` (à contrôler) : `direct` Hostaway avec `totalPrice > 0` mais aucune ligne hors Hostaway → vérifier si saisie manquante.
- `RESERVATION_VRBO_MONTANT_NON_RENSEIGNE` (à contrôler) : VRBO `Unknown` sans saisie manuelle.

**Position dans le pipeline.** `MASTER_FACT_HA_Reservations` + `MASTER_FACT_MAN_ReservationsHorsHostaway` → `MASTER_CALC_Reservations` → `MASTER_CALC_Flux`. La table commune est le seul point d'entrée des réservations dans le flux.

---

## 10. Charges perso / liquide / compte pro (Module 3)

### 10.1 Fichier de saisie `SAISIE_Charges_Flux.xlsx` → table `MASTER_FACT_MAN_Charges`

> `SAISIE_Charges_Flux.xlsx` est la **source unique** de toutes les charges, achats, consommables, produits ménage, linge/lavage, matériel, charges perso/liquide, dépenses perso sur compte pro (D026). Ce fichier exclut IK et virements associés.

Types de flux : `DEPENSE_PERSO_COMPTE_PRO`, `CHARGE_PAYEE_PERSO_OU_LIQUIDE`, `PAIEMENT_PRESTATAIRE_LIQUIDE`, `ACHAT_MENAGE`, `FRAIS_LOCAL`, `CHARGE_EXCEPTIONNELLE_REFACTURABLE`, **`INCIDENT_VOYAGEUR`** (D041 — `reservation_id` obligatoire), **`PRESTATION_AIRCOVER_REFACTUREE`** (D042 — gestion sinistre facturée au propriétaire).

| Colonne | Rôle |
|---|---|
| `charge_id` (PK) | Identifiant parlant, nomenclature §16.2 (`CHG-2026-04-HC-WAFA-CB-001`) |
| `ROW_HASH` | Hash de ligne |
| `date_charge`, `mois` | Date et mois |
| `associe_id` | Associé (`REF_Associes`) |
| `montant` | Montant TTC |
| `categorie_charge_id` | `REF_Categories_Charges` (20 catégories) |
| `type_flux_id` | `REF_Types_Flux` |
| `mode_paiement_id` | Banque pro / espèces / carte / compte perso (`REF_Modes_Paiement`) |
| `carte_id` | `REF_Cartes_Paiement` si applicable |
| `logement_id`, `proprietaire_id` | Affectation |
| `affectation_type` | Logement / propriétaire / global / non affectable (`REF_Types_Affectation`) |
| `code_impact` | `IC` / `HC` / `HR` |
| `prise_en_compta` | OUI / NON |
| `paye_avec_montant_recupere` | Lien réservation HH si l'argent vient d'un montant récupéré |
| `lien_virement_banque` | Lien optionnel avec un mouvement bancaire |
| `refacturable` | Défaut `REF_Categories_Charges`, surchargeable |
| `justificatif` | Oui / non / lien |
| `statut_controle` | OK / à contrôler / bloquant |
| `commentaire` | Note |

`REF_Categories_Charges` pré-remplit via `impact_resultat`, `refacturable_defaut`, `hors_compta_defaut`.

### 10.2 Effets métier

| Cas | Résultat réel | Résultat comptable | Avantages |
|---|---|---|---|
| Charge société payée par compte perso | Diminue | Non, sauf `prise_en_compta = OUI` | Réduit l'avantage net |
| Paiement prestataire en liquide | Diminue | Non, sauf instruction contraire | Réduit l'avantage net si payé avec argent récupéré |
| Dépense perso sur compte pro | Diminue le résultat concerné | Oui (banque pro) | Augmente l'avantage brut |
| Remboursement associé | Neutralise selon lien d'origine | Selon code impact | Peut neutraliser avantage/charge |

### 10.3 Clé

Identifiant parlant généré à la saisie, suivant la nomenclature unique du §16.2 : `CHG-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR` (ex. `CHG-2026-04-HC-WAFA-CB-001`). **Éviter** une clé date+montant+catégorie (deux charges identiques peuvent exister le même jour).

### 10.4 Acomptes propriétaires — `MASTER_FACT_MAN_AcomptesProprietaires`

Type de flux `ACOMPTE_FACTURE_PROPRIETAIRE` (`HC`). Règles : rattachement à une facture **obligatoire** ; report au mois suivant si excédentaire.

| Colonne | Rôle |
|---|---|
| `acompte_id` (PK) / `ROW_HASH` | |
| `mois`, `proprietaire_id`, `logement_id` | Rattachement |
| `facture_id` | Facture rattachée (obligatoire) |
| `montant_acompte`, `report_mois_suivant` | Montant et report éventuel |
| `source_pk` | Réservation HH ou autre origine |
| `mode_paiement_id`, `statut_controle` | |

Granularité recommandée : `proprietaire_id + logement_id + mois + facture_ref` (un acompte peut devoir être ventilé par appartement quand les factures sont émises par logement).

### 10.5 Traçabilité du liquide et caisse théorique

Le liquide n'est pas un avantage par défaut : il doit être traçable par **origine** et par **usage**.

| Origine | Usage | Impact |
|---|---|---|
| Réservation hors Hostaway (paiement espèces) | Montant conservé par associé | Avantage brut |
| Remboursement / ajustement documenté | Paiement prestataire | Charge HC ou comptabilisable selon `prise_en_compta`, déduit de l'avantage net |
| | Reversement propriétaire | Diminue le montant conservé / impacte acompte ou net propriétaire |
| | Acompte facture | Rattaché à une facture propriétaire |
| | Solde non utilisé | Reste en caisse théorique, à contrôler |

Contrôle minimal :

```text
Solde liquide théorique = liquide récupéré − liquide reversé − liquide utilisé en charge − liquide affecté en acompte
```

Toute ligne de liquide porte un lien d'origine (`reservation_hh_id` si possible) et un usage. Une ligne sans usage est tolérée temporairement mais apparaît en contrôle.

---

### 10.6 Corrections manuelles — `MASTER_FACT_MAN_Corrections`

Table transverse pour les ajustements validés qui ne relèvent d'aucune autre table : régularisation d'un écart constaté, neutralisation, correction d'imputation. Chaque correction est tracée et porte un code impact.

| Colonne | Rôle |
|---|---|
| `correction_id` (PK) / `ROW_HASH` | Nomenclature §16.2 |
| `mois` | Mois de rattachement |
| `cible_module`, `cible_pk` | Ligne ou périmètre corrigé |
| `montant`, `sens` | Montant positif + PRODUIT / CHARGE / NEUTRALISATION |
| `code_impact` | `IC` / `HC` / `HR` |
| `motif` | Justification obligatoire |
| `statut_controle` | Validé / à contrôler |

Une correction n'est jamais silencieuse : motif obligatoire et visible en contrôle.

---

## 11. Ménages (Module 4)

### 11.1 Trois notions distinctes

| Notion | Source | Usage |
|---|---|---|
| Ménage facturé au voyageur (retenu) | Hostaway / hors Hostaway | Calcul propriétaire & payout |
| Coût standard ménage (exécution) | `REF_Couts_Standards_Menage` — **standards rebasés sur l'exécution uniquement (D037)**. Valeurs actuelles (Studio 29, T2 39, T3 55, T4 69, T6/Duo 110 €) à revalider au Lot 0 | Contrôle d'écart exécution vs standard |
| Coût réel ménage | Facture prestataire / suivi interne | Résultat réel & écart |

Le prix de ménage Hostaway **n'est pas** le coût réel prestataire.

### 11.2 Deux sources et logique de comptage

Tâches Hostaway (451, endpoint `/tasks`) : 450/451 ont un `reservationId`, 451/451 un `listingMapId`, mais **22/451 seulement ont un `cost`**. Statuts : `completed` 280, `cancelled` 72, `confirmed` 72, `pending` 27.

`REF_Types_Lignes_Menage` : `MENAGE_STANDARD` et `REMISE_EN_ETAT` comptent comme ménage ; `FRAIS_DEPLACEMENT` jamais (mais répartissable) ; `LINGE` et `ACHAT_PRODUIT` non comptés.

> **Décision (validée par les données)** : le `cost` Hostaway étant vide à 95 %, **Hostaway sert au comptage, pas à la valorisation**. Le coût réel vient des factures / suivi interne.

### 11.3 Table `MASTER_FACT_MEN_Menages`

PK = `task_id` (Hostaway), sinon composite `logement_id + date_menage + intervenant_id` (+ suffixe si collision). Colonnes : `reservation_id`, `logement_id`, `proprietaire_id`, `intervenant_id` (`REF_Intervenants`), `date_menage`, `type_ligne_menage_id`, `compte_comme_menage`, `cout_reel`, `cout_standard`, `ecart_cout`, `source`, `statut_controle`.

### 11.4 Fichier de production `M04_MENAGES_PowerQuery.xlsx`

**Emplacement**

```text
C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_DONNEES_NORMALISEES\menages\M04_MENAGES_PowerQuery.xlsx
```

**Périmètre après décision D027 — IRRÉVOCABLE.** M04 traite uniquement :

- la main-d'œuvre de ménage directe (heures × taux horaire) ;
- le rangement (main-d'œuvre opérationnelle uniquement — D038 : si le rangement inclut un achat, du linge, du matériel, des consommables ou un coût exceptionnel, il sort de M04 vers `SAISIE_Charges_Flux.xlsx`) ;
- la comparaison avec le coût standard d'exécution ménage (`REF_Couts_Standards_Menage`).

M04 **ne contient plus** : onglet `achats`, colonne `Coût du lavage`, colonne `Courses`, heures de courses, consommables, linge, matériel, forfait local 50 €. Ces postes passent par `SAISIE_Charges_Flux.xlsx`. Le coût complet ménage (exécution + charges) est reconstruit hors M04 via `VUE_ACHATS_MENAGE_VALIDES` (§11.6, D028).

**Statut.** Ce classeur n'est pas une source brute. C'est un fichier de transformation actualisable. La requête Power Query `tbl_MASTER_FACT_MEN_Menages` produit la table métier exploitable. Les lignes ne se modifient pas à la main. La requête doit être actualisée avant toute exploitation.

**Granularité.** Table mensuelle agrégée par `mois × intervenant × appartement`, alimentée par le Google Sheet `Suivi ménage` (main-d'œuvre). Niveau distinct du comptage Hostaway (par tâche, §11.2).

**Sources amont (simplifiées).** Google Sheet `Suivi ménage` — onglet principal uniquement : heures, nombre de ménages, intervenant, mois, appartement, Rangement. L'onglet `achats` du Google Sheet est **retiré** (D027).

**Schéma attendu du Google Sheet `Suivi ménage` (onglet principal).**

| Colonne | Type | Obligatoire | Notes |
|---|---|---|---|
| `Mois` | texte ou numérique | OUI | Power Query convertit en numérique |
| `Année` | numérique | OUI | |
| `Intervenant` | texte | OUI | Résolu via `REF_Intervenants` (`INTERNE` attendu) |
| Une colonne par appartement | numérique (nb ménages) | au moins une | Dépivotées. Libellés mappés dans `REF_Mapping_Logements` (Lot 2). |
| `Rangement` | numérique | NON | Devient une ligne `Type = Rangement` après dépivotage |
| `Nombre d'heures` | numérique | OUI | Heures de main-d'œuvre directe |

> `Courses` et `Coût du lavage` sont **supprimés** du schéma source. Si ces colonnes subsistent dans le Google Sheet, elles sont ignorées à l'import et ne participent à aucun calcul M04.

**Comportement si le schéma amont change.**
- Colonne appartement renommée → `MENAGE_SANS_LOGEMENT_ID` (bloquant Lot 11).
- Colonne obligatoire absente → `M04_SCHEMA_SOURCE_INVALIDE` (bloquant).
- Nouvelle colonne appartement → ajouter à `REF_Mapping_Logements` avant actualisation.

**Formules métier (simplifiées, M04 = exécution uniquement).**

```text
Coût d'exécution ménage  = Nombre d'heures × TAUX_HORAIRE_MENAGE_INTERNE
Prix d'exécution unitaire = Coût d'exécution ménage / Nombre de ménages
TotalRangement           = Σ coût d'exécution des lignes Type = Rangement
Ecart_standard           = REF_Couts_Standards_Menage − Prix d'exécution unitaire   (informatif)
Total_execution          = Nombre de ménages × Prix d'exécution unitaire
```

`TAUX_HORAIRE_MENAGE_INTERNE` : actuellement 10 €/h (codé en dur, à migrer vers `REF_Parametres_Generaux`).

> **Plus de quote-part Courses / forfait local dans M04.** La quote-part par ménage et le coût complet sont calculés hors M04 (§11.6).

**Colonnes attendues en sortie.**

| Colonne | Source |
|---|---|
| `hostaway_listing_id` | Via `REF_Mapping_Logements` |
| `Mois`, `Année` | Google Sheet |
| `Intervenant`, `Appartement`, `Type` | Google Sheet dépivotage |
| `Nombre de ménages`, `Nombre d'heures` | Google Sheet |
| `cout_execution_unitaire` | Coût exécution / Nb ménages |
| `cout_standard` | `REF_Couts_Standards_Menage` |
| `ecart_execution_vs_standard` | `cout_standard − cout_execution_unitaire` (informatif) |
| `total_execution` | `Nombre de ménages × cout_execution_unitaire` |
| `menage_calc_id` | Clé composite |
| `statut_controle`, `ROW_HASH` | Contrôle et upsert |

**Clé et upsert.**

```text
menage_calc_id = MEN-{AAAA-MM}-{hostaway_listing_id ou APP_SANITIZED}-{INTERVENANT}-{compteur}
ex. MEN-2026-04-480140-IMENE-001
```

**Alimentation de `MASTER_CALC_Flux`** (Lot 9) :

| Champ `MASTER_CALC_Flux` | Valeur |
|---|---|
| `source_module` | `MENAGES_INTERNES` |
| `source_table` | `tbl_MASTER_FACT_MEN_Menages` |
| `source_pk` | `menage_calc_id` |
| `date_flux` | Dernier jour du mois |
| `mois` | `Année` + `Mois` |
| `logement_id` | Via `REF_Mapping_Logements` |
| `type_flux_id` | `COUT_EXECUTION_MENAGE_INTERNE` |
| `sens` | `CHARGE` |
| `montant` | `total_execution` |
| `code_impact` | `HC` **obligatoire** |

**Contrôle dédié.** `MENAGE_INTERNE_CODE_IMPACT_NON_HC` (bloquant). `M04_SCHEMA_SOURCE_INVALIDE` (bloquant).

### 11.5 Ménages externes — `MASTER_FACT_MEN_MenagesExternes`

**Statut.** Module **à construire** au Lot 6c. La source initiale est constituée des **factures PDF des prestataires de ménage externes**, transformées par IA dans le format structuré ci-dessous. La saisie ligne par ligne par l'humain n'est pas prévue ; l'IA produit la table.

**Granularité obligatoire.** 1 ligne = 1 ménage × 1 appartement × 1 date × 1 prestataire. La référence facture est conservée, mais la table de travail est détaillée — jamais agrégée à la facture.

**Schéma cible.**

| Colonne | Rôle |
|---|---|
| `menage_externe_id` (PK) | Nomenclature : `MENEXT-{AAAA-MM}-{prestataire}-{COMPTEUR}` |
| `ROW_HASH` | Hash de ligne |
| `facture_id` | Identifiant de la facture d'origine (groupe de lignes) |
| `date_facture` | **Date administrative/comptable** de la facture prestataire (suivi fournisseur, compta). |
| `date_menage` | **Date de prestation** (date réelle d'exécution du ménage) — **pilote le rattachement économique** (mois/logement/réservation, ME5). Peut être différente de `date_facture`. |
| `mois`, `annee` | Mois de rattachement (`date_menage` fait foi, jamais `date_facture`) |
| `prestataire_id`, `nom_prestataire` | `REF_Intervenants` |
| `type_intervenant` | `EXTERNE` obligatoire ici |
| `logement_id`, `hostaway_listing_id` | Via `REF_Mapping_Logements` |
| `appartement_source` | Libellé brut tel qu'il apparaît sur la facture |
| `type_ligne_menage_id` | `REF_Types_Lignes_Menage` |
| `nombre_menages` | Compteur |
| `montant_ligne_ht`, `montant_ligne_ttc` | Montant unitaire ligne |
| `montant_facture_total_ht`, `montant_facture_total_ttc` | Montant total facture (sert au contrôle de réconciliation) |
| `code_impact` | **`IC` par défaut**, sélectionnable `IC` / `HC` / `HR` |
| `prise_en_compta` | `OUI` / `NON` |
| `statut_controle` | Validé / à contrôler / bloquant |
| `source_document`, `nom_fichier_source` | Trace du PDF d'origine |
| `commentaire` | |

**Règles.**
- Code impact : **`IC` par défaut**. Modifiable ligne par ligne (jamais par défaut à `HC`).
- `type_intervenant` doit être `EXTERNE` sur toute ligne de cette table. Une ligne avec `type_intervenant = INTERNE` doit aller dans M04, pas ici.
- Réconciliation : `Σ(montant_ligne_ttc) ≈ montant_facture_total_ttc` (tolérance arrondis), sinon `MENAGE_EXTERNE_FACTURE_NON_RECONCILIEE`.

**Alimentation de `MASTER_CALC_Flux`.**

| Champ `MASTER_CALC_Flux` | Valeur issue de la table |
|---|---|
| `source_module` | `MENAGES_EXTERNES` |
| `source_table` | `MASTER_FACT_MEN_MenagesExternes` |
| `source_pk` | `menage_externe_id` |
| `date_flux` | `date_menage` |
| `mois` | Dérivé de `date_menage` |
| `logement_id` | Via `REF_Mapping_Logements` |
| `type_flux_id` | `COUT_REEL_MENAGE_EXTERNE` |
| `sens` | `CHARGE` |
| `montant` | `montant_ligne_ttc` |
| `code_impact` | Selon la ligne (`IC` par défaut) |

**Contrôles dédiés** (cf. `PLAN_CONSTRUCTION.md` Lot 6c) : `MENAGE_EXTERNE_LOGEMENT_ABSENT`, `MENAGE_EXTERNE_DATE_ABSENTE`, `MENAGE_EXTERNE_PRESTATAIRE_INCONNU`, `MENAGE_EXTERNE_CODE_IMPACT_ABSENT`, `MENAGE_EXTERNE_FACTURE_NON_RECONCILIEE`, `MENAGE_EXTERNE_A_VENTILER`.

> **À confirmer avant Lot 6c.** Format réel des factures PDF prestataires (1 à 2 factures anonymisées suffisent à figer le pipeline d'extraction IA).

### 11.6 Coût complet ménage hors M04 — `VUE_ACHATS_MENAGE_VALIDES` (D028)

> M04 produit uniquement le coût d'exécution (main-d'œuvre). Le coût complet ménage est reconstruit dans le flux analytique global à partir de deux sources distinctes. Aucun double comptage n'est possible car les sources sont exclusives.

**`VUE_ACHATS_MENAGE_VALIDES`**

Vue dérivée de `MASTER_FACT_MAN_Charges` (alimentée par `SAISIE_Charges_Flux.xlsx`). Filtre les lignes où :

```text
type_charge IN ('LINGE', 'CONSOMMABLE_MENAGE', 'PRODUIT_MENAGE', 'MATERIEL_MENAGE', 'FRAIS_LOCAL')
ET statut_controle = 'VALIDE'
```

Dimensions disponibles : `logement_id`, `proprietaire_id`, `mois`, `montant`, `code_impact`, `associe_id`.

Cette vue ne remplace pas M04. Elle complète le coût analytique au niveau du flux unifié (`MASTER_CALC_Flux`, Lot 9).

**Reconstruction du coût complet analytique (Lot 9 / Lot 10)**

```text
Coût complet ménage interne (analytique) par logement × mois :
  = Σ total_execution de tbl_MASTER_FACT_MEN_Menages (M04)
  + Σ montant de VUE_ACHATS_MENAGE_VALIDES pour le même logement × mois
```

Cette reconstruction est un calcul de lecture/reporting (Power BI / Power Query). Elle ne modifie pas `MASTER_CALC_Flux` directement.

**Règle de cohérence.**

Contrôle `ACHATS_DEJA_EN_SAISIE_CHARGES` (remplace `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL`) : si un poste de charge présent dans `SAISIE_Charges_Flux.xlsx` est aussi injecté depuis M04, il est signalé en doublon.

---

## 12. IK & avantages (Module 5)

### 12.1 Logique

Chaque type de flux porte des drapeaux (`avantage_brut_defaut`, `deduit_avantage_defaut`) :

| Type de flux | Avantage brut | Déduit avantage |
|---|---|---|
| `VIREMENT_ASSOCIE` | OUI | NON |
| `DEPENSE_PERSO_COMPTE_PRO` | OUI | NON |
| `RESERVATION_HORS_HOSTAWAY` (montant récupéré) | OUI | NON |
| `CHARGE_PAYEE_PERSO_OU_LIQUIDE` | NON | OUI |
| `REMBOURSEMENT_ASSOCIE` | NON (neutralise, `HR`) | NON |
| `PAIEMENT_PRESTATAIRE_LIQUIDE` | NON | OUI |

Trois sources d'avantage brut : virement reçu sur compte perso ; dépense perso sur compte pro ; `montant_recupere` des réservations hors Hostaway. Une partie est **dérivée** des modules 2 et 3 (pas de double saisie). Le strict manuel : virements associés sans détail (« seul le total compte ») et IK kilométriques.

### 12.2 Séparation saisie / calcul (deux tables)

Il ne faut pas mélanger les lignes saisies et le résultat calculé, sous peine de double comptage. Deux tables distinctes.

**Table de saisie `MASTER_FACT_MAN_IK_Avantages`** — uniquement les flux associés non disponibles ailleurs : virements associés sans détail, IK en montant direct, avances, corrections validées. Les dépenses perso sur compte pro, les charges payées pour la société et les montants récupérés hors Hostaway **ne sont pas ressaisis ici** s'ils existent déjà dans leurs tables sources.

| Colonne | Rôle |
|---|---|
| `avantage_id` (PK) / `ROW_HASH` | Identifiant stable (nomenclature §16.2) |
| `mois` | Mois de rattachement |
| `associe_id` | Associé (`REF_Associes`) |
| `type_flux` | IK / virement associé / avance / correction |
| `nature` | Description courte (ex. trajet, avance, virement mensuel) |
| `montant` | Montant saisi directement — pas de barème auto (D036) |
| `code_impact` | `IC` / `HC` / `HR` |
| `impact_resultat_reel` | OUI / NON |
| `impact_resultat_comptable` | OUI / NON |
| `commentaire` | Note libre |
| `lien_origine` | Lien optionnel vers banque ou justificatif |
| `statut_controle` | REF_Statuts (VALIDE / A_CONTROLER / BLOQUANT / IGNORE_JUSTIFIE) |

**Table calculée `MASTER_CALC_AvantagesAssocies`** — consolide par associé et par mois en empilant les sources (virements, dépenses perso compte pro, montants récupérés HH, IK), puis déduit les charges payées pour la société.

| Colonne | Rôle |
|---|---|
| `PK` = `personne_id + mois` | Clé de synthèse |
| `avantages_bruts` | Virements + dépenses perso compte pro + montants récupérés HH + IK |
| `charges_payees_pour_societe` | Charges payées par l'associé pour la société |
| `avantages_nets` | `avantages_bruts − charges_payees_pour_societe` |
| `detail_sources` | Liste ou lien vers les lignes sources |

```text
Avantage net = Avantages bruts (+ IK) − Charges payées pour la société − Remboursements neutralisateurs
```

IK : montant direct (D036). Barème kilométrique pourra être ajouté plus tard — non bloquant.

---

## 13. Banque & rapprochement bancaire (Module 6)

### 13.1 Objectif

Rapprocher les mouvements du compte pro avec : payouts plateformes, virements associés, dépenses perso sur compte pro, remboursements, abonnements logiciels, factures fournisseurs, autres charges.

### 13.2 Pipeline cible (stack réelle)

```text
1. VBA importe + normalise + applique les règles manuelles
2. Les lignes A_ENVOYER_IA restent dans NORM_Banque
3. Un script/macro exporte ces lignes en JSON/CSV propre
4. Claude Code lit ce fichier localement
5. Claude Code génère un fichier résultat JSON/Excel
6. Une macro importe le résultat dans IA_Classification et CTRL_A_CONTROLER
```

### 13.3 Tables du pipeline bancaire

| Table | Rôle | Clé |
|---|---|---|
| `BRUT_Banque` | Copie brute de l'export importé | `import_id + ligne_source` |
| `NORM_Banque` | Mouvements normalisés (date, libellé nettoyé, montant, sens, compte, empreinte) | `mouvement_id` |
| `IA_Classification` | Classification IA des lignes non reconnues | `mouvement_id` |
| `CTRL_A_CONTROLER` | Lignes douteuses à valider | `mouvement_id + code` |
| `LOG_Traitement` | Historique des imports et traitements | `run_id + étape` |

Colonnes principales de `NORM_Banque` : `mouvement_id` (PK), `ROW_HASH`, `date_operation`, `date_valeur`, `libelle`, `montant`, `sens` (débit/crédit), `compte_id`, `tiers_detecte`, `categorie`, `type_flux_id`, `code_impact`, `source_classification` (règle/IA/manuel), `statut_controle`.

### 13.4 Logique anti-doublon

Chaque mouvement reçoit une empreinte technique pour éviter les doublons entre exports qui se chevauchent. Si la banque fournit un identifiant stable, il prime ; sinon l'empreinte sert de clé de déduplication :

```text
empreinte = date_operation + date_valeur + montant + sens + libellé_normalisé + compte
```

**Distinction BRUT vs NORM (période).** `BRUT_Banque` contient **toutes** les lignes importées depuis le fichier brut, telles quelles, sans filtre temporel — même celles qui débordent du mois nominal. `NORM_Banque` ne contient que les **lignes rattachées à la période réelle** concernée (mois courant), normalisées et filtrées. Le rattachement temporel se fait **toujours par les colonnes `Date` ou `Valeur`**, jamais par le nom du fichier. Toutes les lignes de la période doivent être validées avant clôture du mois.

### 13.4 bis Banque et table de flux : éviter le double comptage

`NORM_Banque` n'alimente `MASTER_CALC_Flux` comme produit ou charge que lorsque la banque est la **source économique principale**. Quand un flux est déjà porté par Hostaway, une réservation hors Hostaway, une charge manuelle ou une règle récurrente, la banque sert au rapprochement.

| Mouvement bancaire | Traitement dans `MASTER_CALC_Flux` |
|---|---|
| Virement Airbnb/Booking rapproché à `MASTER_CALC_HA_Payout` | pas de produit bancaire ; rapprochement uniquement |
| Dépense fournisseur inconnue du système | crée une charge si validée |
| Dépense perso sur compte pro | crée un avantage brut + charge selon catégorie |
| Virement associé | crée une ligne d'avantage / IK si non déjà saisie |
| Remboursement associé | neutralisation ou contrôle selon lien d'origine |
| Paiement d'une charge déjà saisie manuellement | rapprochement uniquement, sauf si la ligne manuelle était prévisionnelle |

Règle obligatoire pour empêcher le double comptage des produits et charges.

### 13.5 Place de l'IA

L'IA ne traite que les lignes restantes après les règles déterministes. Elle **n'écrase jamais** une règle manuelle validée ; une correction humaine peut au contraire **devenir** une nouvelle règle déterministe.

> Module autonome : il ne bloque pas le cœur du système, mais devient prioritaire dès que les tables Charges, IK/Avantages et Payout sont stabilisées, car il permet de vérifier versements plateformes, dépenses perso sur compte pro et virements associés.

### 13.6 Source brute observée — `2026_03_BRUT_Banque_CreditMutuel.xlsx`

**Emplacement officiel**

```text
C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\01_SOURCES_BRUTES\Banque\2026_03_BRUT_Banque_CreditMutuel.xlsx
```

**Statut.** Source brute bancaire Crédit Mutuel. **Ne jamais modifier le fichier.** Toutes les transformations produisent des tables dérivées (`BRUT_Banque`, `NORM_Banque`, `IA_Classification`, `CTRL_A_CONTROLER`, `LOG_Traitement`).

**Compte concerné**

| Élément | Valeur |
|---|---|
| Compte | C/C EUROCOMPTE PRO WONDERBNB |
| RIB | 10278 02211 00021321603 |
| `compte_id` retenu | `CM_02211_00021321603` |
| Devise | EUR |

**Feuilles du classeur**

| Feuille | Rôle | Utilisable pour les calculs ? |
|---|---|---|
| `Vos comptes` | Synthèse (RIB, solde, situation au 25/04/2026) | Non — identification uniquement |
| `Cpt 02211 00021321603` | **Mouvements bancaires exploitables** | **Oui — feuille métier** |
| `hidden_data` | Métadonnées techniques de l'export (`Range A6:E137`) | Non |
| `hidden` | Résidu de modèle d'export | Non |

**Structure de la feuille métier `Cpt 02211 00021321603`**

| Élément | Valeur observée |
|---|---|
| Ligne d'en-tête | Ligne 5 |
| Première ligne de mouvements | Ligne 6 |
| Colonnes brutes | `Date`, `Valeur`, `Libellé`, `Débit`, `Crédit`, `Solde`, `Dev` |
| Nombre de mouvements exploitables | 132 |
| Première date observée | 25/02/2026 |
| Dernière date observée | 25/04/2026 |
| Total débits observés | 11 055,83 € |
| Total crédits observés | 11 955,26 € |

> **Règle nom de fichier ≠ période réelle.** Le fichier s'appelle `2026_03_...` mais couvre du **25/02/2026 au 25/04/2026**. Le rattachement temporel doit **toujours** se faire par les colonnes `Date` ou `Valeur`, jamais par le nom du fichier. Ce constat motive le contrôle `BANQUE_FICHIER_PERIODE_INCOHERENTE` (§18.3).

**Lignes à ignorer à l'import** : lignes d'introduction de compte, en-têtes non mouvement, lignes totalement vides, ligne de solde final, ligne "Liste de vos comptes", feuilles `hidden` / `hidden_data`. Une ligne bancaire exploitable doit avoir au minimum : `Date` non vide, `Libellé` non vide, `Débit` ou `Crédit` renseigné, devise renseignée ou déductible.

**Identifiants recommandés pour ce fichier**

```text
import_id     = IMP-BQ-CM-2026-03-001
compte_id     = CM_02211_00021321603
mouvement_id  = MVT-{compte_id}-{YYYYMMDD}-{sens}-{montant_centimes}-{hash_court}
ex.           = MVT-CM_02211_00021321603-20260315-DEBIT-2590-A1B2C3
```

Le `2026-03` de l'`import_id` correspond au mois nominal du fichier, **pas** à la période réelle des mouvements.

**Conventions de normalisation** (cohérentes avec §13.2 et §14.3)

```text
Si Débit renseigné  : sens = DEBIT  ; montant = |Débit|
Si Crédit renseigné : sens = CREDIT ; montant = Crédit
```

Cas à contrôler : Débit et Crédit simultanés, les deux vides, montant nul, montant non numérique (cf. §18.3).

**Familles de libellés observées et règle de classification**

| Famille de libellé | Usage probable | Règle |
|---|---|---|
| `VIR AIRBNB PAYMENTS LUXEMBOU...` | Versement Airbnb | **Rapprochement Hostaway, jamais nouveau produit** (§13.4 bis) |
| `PAIEMENT CB ... CARTE 8259` | Carte compte pro | Charge ou dépense perso compte pro selon tiers |
| `PRLV SEPA ...` | Prélèvement SEPA | Charge récurrente à classifier |
| `VIR INST WAFA SOUCI...` | Virement instantané associé | Avantage / IK / remboursement à classifier |
| `VIR INST ...` (reçu) | Encaissement non plateforme | À classifier (caution, propriétaire, associé, autre) |
| `FRAIS ...` | Frais bancaires | Charge bancaire si validée |
| `IMPAYE ...` | Impayé / retour | À contrôler |

Aucune règle définitive n'est déduite du seul texte tant qu'elle n'est pas inscrite dans un référentiel de classification bancaire.

---

## 14. Table de flux unifiée (Module 7)

### 14.1 Rôle

`MASTER_CALC_Flux` empile toutes les lignes économiques : Hostaway, réservations hors Hostaway, banque, charges perso/liquide, ménages, IK & avantages, acomptes, corrections.

### 14.2 Structure

| Colonne | Rôle |
|---|---|
| `flux_id` (PK) / `ROW_HASH` | |
| `source_module`, `source_table`, `source_pk` | Traçabilité vers la ligne d'origine |
| `date_flux`, `mois` | |
| `logement_id`, `proprietaire_id`, `associe_id` | Dimensions d'affectation |
| `type_flux_id`, `categorie` | Nature |
| `sens` | **PRODUIT / CHARGE / NEUTRALISATION** |
| `montant` | **Toujours positif** |
| `code_impact` | `IC` / `HC` / `HR` |
| `inclure_resultat_comptable` | OUI/NON (dérivé du code impact) |
| `inclure_resultat_hors_compta` | OUI/NON (dérivé) |
| `inclure_resultat_reel` | OUI/NON (dérivé) |
| `statut_controle`, `commentaire` | |

### 14.3 Convention de calcul (retenue : la plus robuste)

```text
montant = toujours positif
sens    = PRODUIT | CHARGE | NEUTRALISATION
Resultat = Somme(PRODUITS) - Somme(CHARGES)
```

Montant positif + sens explicite rend les contrôles plus lisibles et évite les erreurs de signe. Les trois colonnes `inclure_resultat_*` sont pré-calculées depuis `code_impact` pour faciliter l'exploitation Power BI.

---

## 15. Résultats réel / comptable / hors compta (Module 8)

### 15.1 Codes impact (pivot)

| Code | Signification | Comptable | Hors compta | Réel |
|---|---|---:|---:|---:|
| `IC` | Intra-comptable | Oui | Non | Oui |
| `HC` | Hors compta / extra | Non | Oui | Oui |
| `HR` | Hors résultat | Non | Non | Non |

- **Comptable** = flux `IC`.
- **Hors compta** = flux `HC`.
- **Réel (pilotage)** = `IC` + `HC` (tout sauf `HR`). Vision par défaut (`REF_Parametres_Generaux` → `resultat_par_defaut = PILOTAGE`). Démarrage `2026-03`.

### 15.2 Table `MASTER_CALC_Resultats`

Dimensions : mois, logement, propriétaire, activité globale, vision (réel/comptable/hors compta). Mesures : produits, charges, résultat, commission, ménage, net propriétaire, avantages associés, anomalies bloquantes.

### 15.3 Vision par associé

Le résultat global n'est **pas** découpé par associé. Les avantages sont consultables par associé dans une vue dédiée. Sont exclus du résultat : statuts non productifs et `ownerStay`.

---

## 16. Clés de liaison

### 16.1 Clés primaires

| Table | PK | Vérifié |
|---|---|---|
| Listings Hostaway | `listingMapId` | OUI |
| Réservations / Détail / Payout | `reservation_id` | OUI |
| Finance fields | `reservation_id + financeField_name` | OUI |
| Reservation fees | `reservation_id + fee_id` (fallback) | OUI (0 fallback au dernier run) |
| Anomalies | `reservation_id + code` | OUI |
| Tâches ménage | `task_id` | OUI |
| Calendrier | `listingMapId + date` | (non fourni) |
| Réservations hors Hostaway | `reservation_hh_id` | À créer |
| Charges | `charge_id` | À créer |
| Acomptes | `acompte_id` | À créer |
| IK & avantages saisis | `avantage_id` | À créer |
| Avantages associés calculés | `personne_id + mois` | À créer |
| Net propriétaire | `proprietaire_id + logement_id + mois` ou `facture_id` | À créer |
| Banque normalisée | `mouvement_id` (empreinte si pas d'ID stable) | À créer |
| Flux unifié | `flux_id` | À créer |

### 16.2 Nomenclature des identifiants manuels

Les identifiants manuels sont lisibles et stables, et ne dépendent pas du seul montant (deux flux identiques peuvent exister le même jour).

```text
TYPE-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR
```

Exemples : `FLUX-2026-04-HC-EWAN-LIQ-001`, `CHG-2026-04-HC-WAFA-CB-001`, `RESHH-2026-04-HC-DIRECT-001`, `ACOMPTE-2026-04-PROP001-APP002-001`.

| Élément | Exemple | Rôle |
|---|---|---|
| `TYPE` | `FLUX`, `CHG`, `RESHH`, `ACOMPTE`, `AVTG` | nature de la ligne |
| `AAAA-MM` | `2026-04` | période de rattachement |
| `IMPACT` | `IC`, `HC`, `HR` | impact comptable / hors compta / hors résultat |
| associé / mode | `EWAN`, `WAFA`, `LIQ`, `CB` | lecture rapide |
| compteur | `001`, `002` | évite les collisions |

Obligatoire pour les tables manuelles ; les tables Hostaway conservent les IDs API comme clés.

### 16.3 Rapprochements

```text
REF_Logements.hostaway_listing_id ──► MASTER_REF_HA_Listings.listingMapId
REF_Logements.logement_id ──────────► toutes les tables MAN_* (affectation)
REF_Logements.proprietaire_id ──────► REF_Proprietaires.proprietaire_id
REF_Logements.type_logement_id ─────► REF_Types_Logements / REF_Couts_Standards_Menage
REF_Mapping_Logements ──────────────► résolution libellés sources → logement_id
toute charge/flux ──► REF_Categories_Charges / REF_Types_Flux / REF_Codes_Impact
```

`REF_Mapping_Logements` (81 lignes) relie les libellés hétérogènes (Hostaway `listingMapId`/`listingName`, libellés factures ménage, noms courts, adresses) à un `logement_id` unique avec un `niveau_confiance`. C'est le maillon qui rattache une facture ménage au bon logement.

---

## 17. Livrables propriétaires (Module 10)

> Règle centrale : **Revenu net d'exploitation = performance économique du logement.** **Solde à payer = règlement réel.** Ces deux notions sont **strictement séparées** (D033).

### 17.1 Bloc exploitation (par réservation ou agrégé par mois)

```text
base_commission                      = total_payout − menage_facture
commission_conciergerie              = base_commission × taux_commission
revenu_net_exploitation_proprietaire = total_payout − menage_facture − commission_conciergerie − charge_fixe_mensuelle
```

Cas particulier — annulation avec indemnité (D030) :
```text
base_commission         = CancellationPayout
commission_conciergerie = CancellationPayout × taux_commission
revenu_net_exploitation = CancellationPayout − commission_conciergerie   (charge_fixe_mensuelle non déduite par réservation)
```

### 17.2 Bloc règlement / trésorerie

```text
montant_du_conciergerie = commission_conciergerie + menage_facture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees
reste_a_payer           = montant_du_conciergerie − acompte_conciergerie_recu_via_airbnb − autres_acomptes_recus − paiement_deja_recu
```

`charges_exceptionnelles_refacturees` impacte le `montant_du_conciergerie` uniquement — **jamais** `revenu_net_exploitation_proprietaire`.

### 17.3 Format de la facture propriétaire (12 lignes obligatoires)

La facture affiche séparément et dans cet ordre :

| Ligne | Champ | Bloc |
|---|---|---|
| 1 | Total payout | Exploitation |
| 2 | Ménage facturé | Exploitation |
| 3 | Commission conciergerie | Exploitation |
| 4 | Charge fixe mensuelle | Exploitation |
| 5 | **Revenu net d'exploitation propriétaire** | Exploitation |
| 6 | Montant total dû à la conciergerie | Règlement |
| 7 | Acompte reçu via Airbnb | Règlement |
| 8 | Autres paiements déjà reçus | Règlement |
| 9 | **Reste à payer à la conciergerie** | Règlement |
| 10 | Charges / achats exceptionnels refacturés (hors revenu net) | Règlement |
| 11 | Acomptes propriétaires (réservations hors Hostaway) | Règlement |
| 12 | Statut règlement | Règlement |

L'acompte issu des réservations hors Hostaway (§9.3) apparaît en ligne 11, **sans détailler toute l'origine**, mais le lien de contrôle est conservé en interne.

**Note D042 (AirCover) — non inscrite comme ligne de facture** : les éventuels champs `aircover_recu_par_proprietaire_montant`, `_date` et `_motif` apparaissent en encadré d'information sur l'Excel de contrôle uniquement (pas dans les 12 lignes), pour rappeler au propriétaire qu'un remboursement plateforme lui a été versé directement. Il ne modifie ni le bloc exploitation ni le bloc règlement (AC2).

### 17.4 Structure de sortie facture (D040)

La facture propriétaire produit les sorties logiques suivantes (mise en forme visuelle décidée au Lot 12) :

| Table / sortie | Rôle |
|---|---|
| Excel de contrôle | Feuille récapitulative par mois / propriétaire / logement |
| `FACT_FACTURE_ENTETE` | Identifiants, `proprietaire_id`, `logement_id`, `mois`, `statut_generation`, dates, totaux blocs exploitation et règlement |
| `FACT_FACTURE_LIGNES` | Les 12 lignes de §17.3 avec `type_ligne`, libellé, montant, `bloc` (EXPLOITATION / REGLEMENT) |
| `statut_generation` | BROUILLON / VALIDE / EMIS / ANNULE (REF_Statuts) |
| Future sortie PDF | **Aucun PDF propriétaire produit au démarrage** (D040/P11). Les champs et tables sont conçus dès maintenant pour qu'un PDF puisse être généré au Lot 12 sans refactoring, mais la production PDF n'est pas un livrable des lots initiaux. |

La structure logique des 12 lignes (§17.3) est verrouillée et doit être respectée avant toute mise en forme visuelle.

---

## 18. Contrôles de cohérence (Module 9)

### 18.1 En place (Hostaway)

`MASTER_CTRL_HA_Anomalies` : `CANCELLED_AVEC_MONTANT` (règle active via D030 — `statut_calcul_payout = ANNULE_AVEC_PAYOUT`), `BOOKING_PAYOUT_INCOMPLET` (bloquant).

### 18.2 Bloquants

> Codes à l'identique de `JOURNAL_CONTROLES.md` (registre faisant foi).

| Code | Module | Pourquoi |
|---|---|---|
| `PK_MANQUANTE_OU_DOUBLONNEE` | Transverse | Upsert impossible |
| `BOOKING_PAYOUT_INCOMPLET` | Hostaway | Réservation Booking active sans payout calculable |
| `ACOMPTE_NON_RATTACHE_FACTURE` | Acomptes | Acompte sans `facture_ref` |
| `MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES` | HH / Avantages | `montant_recupere` non reflété dans les avantages |
| `CHARGE_LOGEMENT_SANS_LOGEMENT_ID` | Charges | Charge affectation LOGEMENT sans `logement_id` |
| `CHARGE_PERSO_SANS_ASSOCIE` | Charges | Charge perso/liquide sans `associe_id` |
| `RESERVATION_HH_SANS_PROPRIETAIRE` | HH | Réservation HH sans `proprietaire_id` |
| `ACOMPTE_HH_INCOHERENT` | HH | Acompte ≠ Total − Ménage − Commission − Reversé |
| `MENAGE_SANS_LOGEMENT_ID` | M04 / Ménages | Appartement ménage non rattaché à un logement |
| `MENAGE_INTERNE_CODE_IMPACT_NON_HC` | M04 | Ligne M04 avec `code_impact` ≠ `HC` |
| `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY` | Banque | Tentative double comptage banque ↔ Hostaway (§13.4 bis) |
| `BANQUE_DATE_INEXPLOITABLE` | Banque | **Date ET Valeur absentes ou toutes deux non parsables** → ligne inutilisable |
| `BANQUE_DEBIT_CREDIT_VIDES` | Banque | Débit et Crédit simultanément vides |
| `BANQUE_DEBIT_CREDIT_DOUBLES` | Banque | Débit et Crédit simultanément renseignés |
| `BANQUE_MONTANT_NON_NUMERIQUE` | Banque | Montant non convertible |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Ligne non classée ouverte → mois non clôturable |
| `M04_SCHEMA_SOURCE_INVALIDE` | M04 | Colonne obligatoire absente / requête PQ échoue |
| `RESERVATION_DOUBLON_HOSTAWAY_HH` | Table commune | `reservation_id_hostaway` rattaché à 2+ lignes |
| `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL` | Obsolète | **Remplacé par `ACHATS_DEJA_EN_SAISIE_CHARGES`** |
| `ACHATS_DEJA_EN_SAISIE_CHARGES` | M04 / Charges | Charge présente dans `SAISIE_Charges_Flux` aussi injectée depuis M04 |
| `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION` | Exploitation | `acompte_conciergerie_recu_via_airbnb` comptabilisé dans le revenu net |
| `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION` | Exploitation | Achat ou charge exceptionnelle inclus dans `revenu_net_exploitation` |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | Exploitation | Charge non récurrente dans `charge_fixe_mensuelle` |
| `PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT` | Exploitation | Paiement déduit du `total_payout` au lieu du `reste_a_payer` |
| `CONFUSION_PAYOUT_SOLDE_FACTURE` | Exploitation | Confusion entre `total_payout`, `montant_du_conciergerie`, `reste_a_payer` |
| `INCIDENT_VOYAGEUR_SANS_RESERVATION` | Incidents | Ligne `INCIDENT_VOYAGEUR` sans `reservation_id` (D041/IV2) |
| `AIRCOVER_CONFONDU_AVEC_PAYOUT` | AirCover | Montant AirCover apparaît dans `total_payout` (D042/AC5) |

### 18.3 À contrôler (non bloquants)

| Code | Module | Cas réel / raison |
|---|---|---|
| `LISTING_ORPHELIN_A_CONTROLER` | Hostaway | `listingMapId 515523` dans l'export, absent du REF |
| `REFERENTIEL_ORPHELIN` | REF | Logement `sur_hostaway=OUI` absent de l'export |
| `VRBO_MONTANT_NON_RENSEIGNE` | VRBO | Réservation VRBO Unknown sans saisie manuelle |
| `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` | Table commune | `direct` Hostaway avec `totalPrice > 0`, pas de ligne HH |
| `CANCELLED_AVEC_MONTANT` | Hostaway | Réservation annulée avec montant → règle D030 s'applique |
| `BANQUE_LIGNE_SANS_DATE` | Banque | **Colonne Date vide** mais colonne Valeur présente et parsable → non bloquant |
| `BANQUE_FICHIER_PERIODE_INCOHERENTE` | Banque | Dates réelles hors période nominale du nom de fichier |
| `BANQUE_DEVISE_NON_EUR` | Banque | Devise ≠ EUR |
| `DOUBLON_BANCAIRE_POTENTIEL` | Banque | Empreinte bancaire déjà connue |
| `LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Aucune règle ni classification fiable |
| `IA_CONFIANCE_INSUFFISANTE` | Banque | Score de confiance IA sous le seuil |
| `MENAGE_SANS_COUT_STANDARD` | Ménages | Type ou coût standard absent → contrôle écart impossible |
| `MENAGE_ECART_NEGATIF_IMPORTANT` | Ménages | Écart coût réel vs standard > seuil |
| `MENAGE_DOUBLON_POTENTIEL` | Ménages | Même mois × logement × intervenant en double |
| `MENAGE_STATUT_NON_VALIDE` | M04 | Ligne non validée, exclue du calcul |
| `TYPE_INTERVENANT_ABSENT` | Ménages | Intervenant sans type (prioritaire sur identité exacte) |
| `AIRCOVER_NON_TRACE` | AirCover | Événement AirCover documenté sans ligne associée (D042/AC5) |

### 18.4 Table `MASTER_CTRL_Coherence`

Colonnes : `PK` (`source_pk + code_controle`), `source_module`, `source_pk`, `code_controle`, `severity` (bloquant/à contrôler/information), `message`, `statut_resolution` (ouvert/corrigé/ignoré justifié), `commentaire`.

### 18.5 Contrôles de saisie dans les fichiers manuels

Faits directement en Excel (formules, mise en forme conditionnelle, validation de données, Power Query), sans macro complexe tant que des règles simples suffisent.

| Fichier | Contrôles minimums |
|---|---|
| Réservations hors Hostaway | doublon `logement_id + date_arrivee + total_percu` ; acompte incohérent ; propriétaire manquant ; montant récupéré sans associé |
| Charges perso/liquide | doublon `date + montant + tiers + personne` ; charge sans affectation ; charge sans justificatif ; `prise_en_compta` vide |
| IK & avantages | associé manquant ; mois manquant ; montant négatif non justifié ; lien origine absent si avantage dérivé |
| Acomptes propriétaires | facture absente ; logement absent ; montant reporté incohérent |
| Caisse / liquide | origine absente ; usage absent ; solde théorique négatif |

### 18.6 Contrôle structurel permanent

À chaque run : `PK` unique, `ROW_HASH` présent, compteurs dans `MASTER_RUN_Log`.

---

## 19. Ordre de construction recommandé

```
Lot 0  │ Stabiliser REF_Setup                                   [À PRÉPARER]
Lot 1  │ Module Hostaway (extraction + payout + anomalies)      [extraction existante, non validée]
Lot 2  │ Réconciliation logements (orphelin 515523, encodage, dates)
Lot 3  │ Charges perso/liquide (Module 3)
Lot 4  │ Réservations hors Hostaway (Module 2) — 1 ligne/résa, acompte
Lot 5  │ Acomptes propriétaires (Module 3)
Lot 6  │ Ménages (Module 4) — tasks (comptage) + factures (valorisation)
Lot 7  │ IK & Avantages (Module 5) — dérivé des Lots 3 et 4
Lot 8  │ Banque / rapprochement (Module 6) — autonome
Lot 9  │ Table de flux unifiée MASTER_CALC_Flux (Module 7)
Lot 10 │ Résultats + Commissions (Module 8) — filtres IC/HC/HR
Lot 11 │ Contrôles globaux (Module 9)
Lot 12 │ Livrables propriétaires Excel / données prêtes Power BI (Module 10)
```

Justification : tout dépend du référentiel (Lot 0) et de la réconciliation logements (Lot 2). Les charges (Lot 3) précèdent les IK (Lot 7), qui s'en déduisent. La banque (Lot 8) est autonome et peut être menée en parallèle dès que les charges sont stabilisées. La table de flux (Lot 9) précède les résultats (Lot 10), simples agrégations filtrées. Les contrôles (Lot 11) consomment toutes les tables.

---

## 20. Points de vigilance

1. **Assiette commission par canal** (§8.3). Le ménage à soustraire n'est pas au même endroit selon Airbnb / Booking / VRBO / Direct. **Risque de surcommission Airbnb (~88 % du volume) si mal géré. Point n°1 à l'implémentation.**
2. **Décalage référentiel ↔ Hostaway.** `515523` dans l'export, absent du REF ; `480780` (LOG_0016, `sur_hostaway=NON`, cohérent) et `497801` du REF absents de l'export. À trancher avant de fiabiliser les jointures.
3. **Encodage du référentiel.** `REF_Associes`, `REF_Codes_Impact`, `REF_Types_Flux` contiennent des caractères cassés (« associÃ© »). À corriger à la source.
4. **Dates en série Excel** (ex. `46023`). À normaliser à l'import.
5. **Ne pas confondre prix total canal et payout réel.**
6. **Hostaway ≠ source financière hors Hostaway.** La table manuelle est la vérité.
7. **Prix ménage Hostaway ≠ coût réel ménage.** Coût réel via factures/suivi.
8. **Pas de double saisie** : une charge qui alimente déjà les avantages n'est pas ressaisie.
9. **Ne jamais supprimer** une ligne absente d'un extract.
10. **Sévérité juste** : ne rendre bloquant que ce qui rend un résultat ou une facture faux.
11. **Formule d'acompte HH non générique** (§9.3).
12. **Clés sur identifiant stable, pas sur montant** (collisions).
13. **Construire lot par lot** : ne pas demander à un outil IA de tout construire en une passe. Les règles restent dans les fichiers `.md` de référence.
14. **Synchronisation GitHub / OneDrive** (§4.1) : Power Query doit pointer vers le dépôt local synchronisé, pas vers les artefacts GitHub temporaires. Le `git pull` local doit devenir une étape maîtrisée.
15. **Fichiers manuels contrôlés** (§4.2, §18.5) : pas de tableaux libres. Listes déroulantes, colonnes obligatoires, statuts, alertes doublons.
16. **Traçabilité du liquide** (§10.5) : tout liquide récupéré a une origine et un usage. Un solde théorique non expliqué apparaît en contrôle.
17. **Table de flux = colonne vertébrale** : c'est elle qui évite les incohérences entre les trois résultats.

---

## 21. Décisions verrouillées et points non bloquants restants

### 21.1 Décisions verrouillées (rappel — ne pas rouvrir sans nouvelle décision explicite)

| Point | Décision verrouillée |
|---|---|
| Cancellation payout | **D030 / EP4** — BaseCommission = CancellationPayout, pas de ménage déduit |
| Seuil tolérance arrondi | **D035** — 0,10 €/ligne, 1,00 € cumulé/facture |
| Barème IK | **D036** — montant direct au démarrage |
| Périmètre `REF_Couts_Standards_Menage` | **D037** — exécution seule. Valeurs à revalider au Lot 0 |
| `charge_fixe_mensuelle` | **D039** — paramétrable dans `REF_Logements`, valeur 0 si absent |
| Structure facture | **D040** — `FACT_FACTURE_ENTETE` + `FACT_FACTURE_LIGNES` |

### 21.2 Points réellement non bloquants restants

| Point | Solution provisoire / report |
|---|---|
| Mise en forme visuelle PDF facture | Préparée par la structure des données (tables et champs conformes), mais **non livrée au démarrage** (D040/P11). Décision future si besoin — non bloquante pour tous les lots. |
| VRBO / iCal | « à contrôler » tant que la source financière n'est pas validée |
| Remboursements associés | Lier à une charge si possible, sinon « à contrôler » |
| Caisse espèces | Suivi manuel au départ |
| Périodicité de validation manuelle | hebdo / mensuelle / avant facturation — à confirmer à l'usage |

---

## 22. Résultat du contrôle de cohérence

L'architecture est **cohérente et constructible** sous réserve de respecter les règles anti-casse ci-dessus. Chaque donnée utile est rattachée à une source, une clé, un hash, un statut de contrôle et, si nécessaire, une ligne de rapprochement.

| Axe | Évaluation |
|---|---|
| Constructibilité | Oui, par lots successifs |
| Résistance aux doublons | Oui, si `PK` / `ROW_HASH` / empreinte bancaire appliqués |
| Résistance au double comptage | Oui, grâce à la règle source métier prioritaire / banque rapprochement (§2.6, §13.4 bis) |
| Vérification des montants | Oui, via `MASTER_CTRL_*`, `CTRL_A_CONTROLER`, caisse théorique, rapprochement bancaire |
| Maintenabilité | Oui, si les règles métier restent dans les `.md` et référentiels, pas dans du code dispersé |

---

## 23. Conventions transverses (statuts, clôture, arrondi)

> Section ajoutée pour combler trois manques verrouillés ailleurs en règle mais sans support structurel : un référentiel de statuts fermé, une table de clôture mensuelle, et une convention d'arrondi unique. Les conventions ci-dessous sont **verrouillées** ; les seuls points non bloquants restants sont listés au §21.

### 23.1 Référentiel des statuts — `REF_Statuts` (onglet `REF_Setup`)

Le champ `statut_controle` est présent dans toutes les tables de saisie et de calcul. Pour éviter la dérive de valeurs (`OK`, `Validé`, `VALIDE`, `À contrôler`, `A_CONTROLER`…), les valeurs autorisées sont **fermées** et viennent d'un onglet `REF_Statuts` dans `REF_Setup` (à créer au Lot 0).

| `statut_id` | Libellé | Effet |
|---|---|---|
| `VALIDE` | Validé | Ligne intégrée au calcul |
| `A_CONTROLER` | À contrôler | Visible en contrôle, intégrée sauf règle contraire du module |
| `BLOQUANT` | Bloquant | Exclue du calcul, bloque la clôture / la facturation |
| `IGNORE_JUSTIFIE` | Ignoré justifié | Exclue volontairement, motif obligatoire |

Toute table manuelle (`MASTER_FACT_MAN_*`), calculée et de contrôle utilise **uniquement** ces valeurs. Les listes déroulantes des fichiers de saisie (§4.2) pointent vers `REF_Statuts`.

> **D044 (2026-06-08) — Lot 3+ :** `statut_controle` = `VALIDE` / `A_CONTROLER` / `EXCLU_RESULTAT` / `A_VENTILER`.
> Nouvelle colonne `niveau_anomalie` (famille `niveau_anomalie` dans `REF_Statuts`) = `INFO` / `A_CONTROLER` / `BLOQUANT`.
> `BLOQUANT` n'est plus un statut de ligne — c'est un niveau d'anomalie. `IGNORE_JUSTIFIE` reste valide pour Lots 0-2.
> `REF_Statuts` étendu : STAT_022 désactivé ; STAT_024-026 (statut_controle) + STAT_027-029 (niveau_anomalie) ajoutés.

> **À ne pas confondre** avec les statuts de réservation Hostaway (`new`/`cancelled`/`ownerStay`… §6.4) ni avec les statuts de mois (§23.2), qui sont des familles distinctes.

### 23.2 Clôture mensuelle — `REF_Cloture_Mensuelle`

La règle de clôture (REGLES §11, C1-C7 ; PLAN Lot 8) impose trois états de mois. La table de stockage est créée **au Lot 0** (structure vide dans `REF_Setup.xlsm` ou CSV dédié) et exploitée/alimentée **au Lot 8**.

| Colonne | Rôle |
|---|---|
| `mois` (PK) | `AAAA-MM` |
| `statut_mois` | `OUVERT` / `EN_CONTROLE` / `CLOTURE` |
| `date_passage_controle`, `date_cloture` | Traçabilité |
| `nb_lignes_bancaires_non_classees` | Compteur ; > 0 ⇒ `CLOTURE` impossible (REGLES C5/C6) |
| `nb_controles_bloquants_ouverts` | Compteur de bloquants ouverts |
| `commentaire` | |

Règles : un mois ne peut passer à `CLOTURE` que si `nb_lignes_bancaires_non_classees = 0` **et** `nb_controles_bloquants_ouverts = 0`. La facturation propriétaire (§17, Lot 12) n'est émise qu'après `CLOTURE`. Contrôle associé : `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` (§18.2, PLAN Lot 8). Un calcul provisoire reste possible sur un mois `OUVERT` / `EN_CONTROLE`.

### 23.2bis `REF_Statuts_Payout` — à distinguer de `REF_Statuts`

Deux référentiels de statuts distincts, créés **tous les deux au Lot 0** :

| Référentiel | Onglet REF_Setup | Champ cible | Valeurs |
|---|---|---|---|
| `REF_Statuts` | `REF_Statuts` | `statut_controle` (toutes tables) | VALIDE / A_CONTROLER / BLOQUANT / IGNORE_JUSTIFIE |
| `REF_Statuts_Payout` | `REF_Statuts_Payout` | `statut_calcul_payout` (`MASTER_CALC_HA_Payout`) | NORMAL / ANNULE_SANS_PAYOUT / ANNULE_AVEC_PAYOUT / PAYOUT_ABSENT / PAYOUT_INCOMPLET / A_CONTROLER |

Ne jamais mélanger les deux. `REF_Statuts` = traitement/contrôle. `REF_Statuts_Payout` = statut de calcul payout.

### 23.3 Convention d'arrondi et double seuil de tolérance (D035)

Pour éviter les écarts entre Excel, Power Query, Python et Power BI :

```text
Calcul interne     : pleine précision disponible — jamais d'arrondi intermédiaire.
Stockage/affichage : 2 décimales, arrondi demi-vers-le-haut (ROUND).
Tolérance ligne    : 0,10 € par ligne.
Tolérance cumulée  : 1,00 € par facture / propriétaire / mois.
Écart ≤ seuil      : acceptable, traçable si nécessaire.
Écart > seuil ligne   → ECART_ARRONDI_LIGNE_SUPERIEUR_TOLERANCE (à contrôler).
Écart > seuil cumulé  → ECART_ARRONDI_FACTURE_SUPERIEUR_TOLERANCE (à contrôler).
```

Paramètres externalisés dans `REF_Parametres_Generaux` (avec dates de validité) :

| Paramètre | Valeur | Type |
|---|---|---|
| `ARRONDI_DECIMALES` | 2 | ENTIER |
| `TOLERANCE_ARRONDI_LIGNE_EUR` | 0.10 | MONTANT |
| `TOLERANCE_ARRONDI_CUMUL_EUR` | 1.00 | MONTANT |

---

## Vision proposée par Claude

Je m'appuierais sur **une table de flux unifiée comme colonne vertébrale**, tout le reste étant alimentation ou lecture de cette table.

**Principe central.** Les données vivent aujourd'hui en silos (Hostaway, charges, ménages, acomptes, banque) avec des schémas différents. Calculer les résultats depuis chaque silo multiplierait les règles et rendrait les trois visions incohérentes. `MASTER_CALC_Flux` normalise tout : un événement économique = une ligne, mêmes colonnes, montant positif + sens. Les trois résultats deviennent **trois filtres** sur `code_impact` (REEL = IC+HC, COMPTABLE = IC, EXTRA = HC). Robuste, vérifiable, exploitable en Power BI comme schéma en étoile (une table de faits, les `REF_*` en dimensions).

**Lecture en trois couches.** (1) **Automatique** : Hostaway, puis banque — tables propres, stables, indépendantes des saisies manuelles. (2) **Métier** : `REF_Setup`, réservations hors Hostaway, charges perso/liquide, avantages — complète ce que les API ne fournissent pas. (3) **Pilotage** : flux unifié, résultats, commissions, factures propriétaires, contrôles.

**Priorités et dépendances.** (1) Fiabiliser la réconciliation logements (Lot 2), maillon le plus risqué : une facture ménage mal rattachée fausse tout le résultat par logement. (2) Charges (Lot 3) avant IK (Lot 7), car les avantages s'en déduisent. (3) Réservations hors Hostaway (Lot 4), porteuses de la logique la plus subtile (montant récupéré → avantage → charge payée avec → déduction). (4) Table de flux (Lot 9) avant résultats (Lot 10). (5) Contrôles (Lot 11) en filet de sécurité avant les livrables.

**Garder simple.** Pas de base de données ni d'ETL lourd : scripts Python d'upsert + CSV master + Power Query/Power BI suffisent, déjà en place pour Hostaway. La complexité reste dans les **règles métier** (le référentiel), pas dans la plomberie. Les fichiers manuels se limitent à ce qui n'existe nulle part ailleurs.

**Point d'attention n°1 à l'implémentation** : l'assiette de commission par canal (§8.3), la règle validée la plus piégeuse techniquement, parce que le ménage à soustraire se cache à un endroit différent selon Airbnb, Booking, VRBO ou Direct.

**En une phrase.** Un référentiel solide + une table de flux unifiée alimentée par tous les modules + trois lectures par simple filtre sur le code impact = un système modulaire, maintenable et directement branchable sur Power BI.

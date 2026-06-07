# REGLES_METIER.md

> **Version V2 — cadrage de lancement.** À compléter lot par lot sans modifier les décisions verrouillées.
> Ce document liste uniquement les **règles métier déjà validées**. Aucune invention.
> Pour la structure des tables, la modélisation et les clés, voir `ARCHITECTURE_DONNEES.md`.
> Pour l'ordre des étapes de construction, voir `PLAN_CONSTRUCTION.md`.

## Contexte juridique

L'activité est **opérationnelle** mais la **SAS porteuse est nouvelle** et son enregistrement n'est pas encore complètement stabilisé.

**Conséquence pour le système** : il prépare les flux, les contrôles et la distinction IC / HC / HR pour la future exploitation comptable, mais **ne suppose pas d'historique comptable existant**. Aucune écriture comptable passée n'est à rechercher. Le résultat comptable démarre à partir des flux validés une fois la compta opérationnelle.

---

## 1. Règles structurantes (non négociables)

| # | Règle | Référence |
|---|---|---|
| R1 | Une table ne supprime jamais une donnée connue. PK + ROW_HASH, upsert non destructif. | Archi §2.1 |
| R2 | La table de flux unifiée `MASTER_CALC_Flux` est la colonne vertébrale. Les trois résultats (réel / comptable / hors compta) en sont des filtres sur `code_impact`. | Archi §2.3, §14, §15 |
| R3 | Source métier prioritaire. La banque rapproche, elle ne recrée pas un flux déjà porté par Hostaway ou une saisie manuelle. | Archi §2.6, §13.4 bis |
| R4 | Hostaway n'est jamais la source financière des réservations hors plateforme. | Archi §7.3, §9 |
| R5 | Hostaway n'est pas la source de valorisation du coût réel ménage. Il sert au comptage et à la comparaison. | Archi §11.2 |

---

## 2. Hostaway

| # | Règle |
|---|---|
| H1 | Payout Airbnb = `airbnbExpectedPayoutAmount` (fallback `airbnbPayoutSum`). |
| H2 | Payout Booking = `totalPriceFromChannel − cityTax − otaPaymentProcessingFee − hostChannelFee`. |
| H3 | Direct / hors Hostaway : aucune valorisation financière depuis Hostaway. |
| H4 | Assiette commission = **payout encaissé − frais de ménage**, ménage récupéré canal par canal (Airbnb via finance fields, Booking via colonne payout). |
| H5 | `ownerStay` : exclus du résultat. |
| H6 | Tâches ménage Hostaway : comptage et comparaison réalisé / déclaré / facturé. Aucune valorisation. |
| H7 | VRBO en `paymentStatus = Unknown` : `A_CONTROLER`, pas d'inclusion automatique au résultat. |

---

## 3. Ménages internes (`M04_MENAGES_PowerQuery.xlsx`)

| # | Règle |
|---|---|
| M1 | M04 traite **uniquement** les ménages internes. |
| M2 | Code impact M04 = **HC obligatoire** (résultat réel oui, résultat comptable non). |
| M3 | `type_flux_id` = `COUT_EXECUTION_MENAGE_INTERNE` (renommé), `sens` = `CHARGE`. Alimente uniquement le coût de main-d'œuvre d'exécution. |
| M4 | **SUPPRIMÉ** — la règle « statut_controle VALIDE requis pour les achats » s'applique désormais dans `SAISIE_Charges_Flux.xlsx`, pas dans M04. |
| M5 | **SUPPRIMÉ — IRRÉVOCABLE** — la quote-part Courses + achats n'existe plus dans M04. Voir D027. |
| M6 | **SUPPRIMÉ** — le forfait local 50 € quitte M04 (D016-REV). Il est saisi dans `SAISIE_Charges_Flux.xlsx` comme charge HC. |
| M7 | **SUPPRIMÉ** — `cout_complet_analytique` / `montant_injectable_flux` et `LOCAL_50_INJECTABLE_DANS_M04` sont obsolètes (D016-REV, D028). |
| M8 | Taux horaire `10 €/h` codé en dur dans M04 ; à migrer vers `REF_Parametres_Generaux` (non bloquant). |
| M9 | M04 est la **source officielle du coût d'exécution ménage interne**. Le CSV exporté est un export technique, jamais l'autorité. |
| M10 | Mapping appartement absent ou ambigu → contrôle, pas d'affectation logement arbitraire. |
| M11 | `APPARTEMENT_DIVERS` / `LOGEMENT_DIVERS` autorisés pour les cas hors parc réels uniquement, jamais pour masquer un mauvais mapping. |
| M12 | **NOUVEAU — IRRÉVOCABLE** : M04 **ne contient pas** : onglet `achats`, colonne `Coût du lavage`, colonne `Courses`, heures de courses, consommables, linge, matériel, forfait local. |
| M14 | **NOUVEAU (D038)** : `Rangement` reste dans M04 uniquement si c'est du temps de main-d'œuvre opérationnelle. Si le rangement inclut un achat, du linge, du matériel, des consommables, un déplacement ou un coût exceptionnel → `SAISIE_Charges_Flux.xlsx`. |
| M13 | **NOUVEAU** : Le coût complet ménage (exécution + charges) est reconstruit dans le flux analytique global via `VUE_ACHATS_MENAGE_VALIDES` (D028). M04 ne calcule que le coût d'exécution (main-d'œuvre) et l'écart vs coût standard d'exécution. |

---

## 3.bis Source unique des achats et charges : `SAISIE_Charges_Flux.xlsx`

| # | Règle |
|---|---|
| SC1 | `SAISIE_Charges_Flux.xlsx` est la **source unique** de toutes les saisies de charges, achats, consommables, produits ménage, linge, lavage, matériel, charges perso/liquide, dépenses perso sur compte pro. |
| SC2 | Ce fichier est **interdit** pour : IK, virements associés, avances associés → `MASTER_FACT_MAN_IK_Avantages` (Lot 7, D025). |
| SC3 | Toute ligne de `SAISIE_Charges_Flux.xlsx` porte : `charge_id` (nomenclature §16.2), `date_charge`, `mois`, `associe_id` si applicable, `categorie_charge_id`, `code_impact`, `statut_controle` (valeur fermée REF_Statuts). |
| SC4 | `VUE_ACHATS_MENAGE_VALIDES` filtre les lignes de `SAISIE_Charges_Flux.xlsx` liées aux ménages (`type_charge IN LINGE / CONSOMMABLE_MENAGE / PRODUIT_MENAGE / MATERIEL_MENAGE / FRAIS_LOCAL`) avec `statut_controle = VALIDE`. |
| SC5 | Toute charge présente dans `SAISIE_Charges_Flux.xlsx` ne doit pas réapparaître dans M04 (anti-double-comptage). |

---

## 3.ter Règles exploitation / règlement propriétaire

| # | Règle |
|---|---|
| EP1 | Le **bloc exploitation** (`total_payout`, `menage_facture`, `commission_conciergerie`, `charge_fixe_mensuelle`, `revenu_net_exploitation_proprietaire`) ne doit jamais être modifié par des éléments de trésorerie. |
| EP2 | `revenu_net_exploitation_proprietaire = TotalPayout − MenageFacture − CommissionConciergerie − charge_fixe_mensuelle`. |
| EP3 | `charge_fixe_mensuelle` = montant facturé contractuellement chaque mois (forfait logiciel, forfait fixe). Jamais une charge exceptionnelle. Contrôle : `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE`. |
| EP4 | Pour une réservation annulée avec `CancellationPayout > 0` : `BaseCommission = CancellationPayout` ; `CommissionConciergerie = CancellationPayout × TauxCommission` ; `NetProprietaire = CancellationPayout − Commission`. Aucun ménage déduit (D030). |
| EP4b | `charge_fixe_mensuelle` paramétrable par propriétaire/logement dans `REF_Logements` (D039). Valeur = 0 si absent. Jamais une charge exceptionnelle. |
| EP5 | Le **bloc règlement** (`montant_du_conciergerie`, `acompte_conciergerie_recu_via_airbnb`, `autres_acomptes_conciergerie_recus`, `paiement_deja_recu`, `reste_a_payer_conciergerie`) ne modifie **jamais** le bloc exploitation. |
| EP6 | `acompte_conciergerie_recu_via_airbnb` : réduit uniquement le reste_a_payer. Ne touche ni au payout propriétaire ni au revenu net d'exploitation. Contrôle : `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION`. |
| EP7 | `charges_exceptionnelles_refacturees` modifie `montant_du_conciergerie` mais **jamais** `revenu_net_exploitation_proprietaire`. Le propriétaire ne doit **jamais voir** ces charges exceptionnelles dans son résultat opérationnel : elles apparaissent uniquement dans le bloc règlement / montant dû, séparément de la performance d'exploitation. Elles peuvent augmenter le reste à payer sans modifier artificiellement le revenu net d'exploitation. Contrôle : `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION`. |

---

## 3.quater Incidents voyageurs (D041 — P02)

**Définition.** Un « incident voyageur » est une situation exceptionnelle liée à un séjour nécessitant un suivi financier ou opérationnel. Le périmètre couvre :

- problèmes d'accès au logement ;
- dégradations constatées ;
- réclamations du voyageur ;
- compensations versées au voyageur ;
- interventions urgentes (serrurier, dépannage, etc.) ;
- tout problème de séjour générant un coût ou un suivi.

| # | Règle |
|---|---|
| IV1 | Toute charge liée à un incident voyageur est saisie dans `SAISIE_Charges_Flux.xlsx` avec `categorie_charge_id = INCIDENT_VOYAGEUR`. |
| IV2 | Le champ `reservation_id` est **obligatoire** sur une ligne `INCIDENT_VOYAGEUR` (lien Hostaway ou hors Hostaway). Contrôle : `INCIDENT_VOYAGEUR_SANS_RESERVATION` (bloquant). |
| IV3 | `categorie_charge_id = INCIDENT_VOYAGEUR` est conservée dans tous les cas pour traçabilité — la catégorie ne change pas. `refacturable = OUI/NON` indique si le propriétaire est refacturé. Si `refacturable = OUI`, la ligne alimente `charges_exceptionnelles_refacturees` et suit EP7 (bloc règlement, jamais bloc exploitation). La ligne reste identifiable comme incident voyageur même quand refacturée. |
| IV4 | Le `code_impact` (`IC` / `HC` / `HR`) se décide ligne par ligne selon la nature de la dépense. |
| IV5 | Aucune nouvelle table dédiée au démarrage. `MASTER_FACT_MAN_Charges` suffit avec la nouvelle catégorie. Une table dédiée pourra être créée plus tard si le volume le justifie. |

---

## 3.quinquies AirCover et réclamations plateformes (D042 — P03)

| # | Règle |
|---|---|
| AC1 | **Trois flux distincts à ne jamais confondre** : (a) remboursement plateforme perçu par le propriétaire ; (b) prestation facturée par la conciergerie ; (c) impact sur le résultat opérationnel. |
| AC2 | **Flux (a) — Remboursement AirCover perçu directement par le propriétaire** : hors comptes conciergerie. N'entre **pas** dans `MASTER_CALC_Flux`. Tracé en information dans `MASTER_CALC_NetProprietaire` via trois champs séparés : `aircover_recu_par_proprietaire_montant`, `aircover_recu_par_proprietaire_date`, `aircover_recu_par_proprietaire_motif`. Ne modifie ni revenu net d'exploitation ni règlement conciergerie. |
| AC3 | **Flux (b) — Prestation facturée par la conciergerie** (gestion sinistre, intervention, suivi) : saisie dans `SAISIE_Charges_Flux.xlsx` avec catégorie `PRESTATION_AIRCOVER_REFACTUREE`. Refacturable → entre dans `charges_exceptionnelles_refacturees` (EP7). N'impacte **jamais** `revenu_net_exploitation_proprietaire`. |
| AC4 | **Flux (c) — Impact sur le résultat conciergerie** : selon le `code_impact` (`IC` / `HC` / `HR`) de la ligne saisie. Décidé ligne par ligne. |
| AC5 | Contrôles : `AIRCOVER_NON_TRACE` (à contrôler, si un événement AirCover documenté n'a pas de ligne associée) ; `AIRCOVER_CONFONDU_AVEC_PAYOUT` (bloquant, si un montant AirCover apparaît dans `total_payout`). |

---

## 4. Ménages externes (futur fichier)

| # | Règle |
|---|---|
| ME1 | Source = factures prestataires PDF, transformées par IA dans le format défini. |
| ME2 | Granularité : **1 ligne = 1 ménage × 1 appartement × 1 date × 1 prestataire**. La référence facture est conservée mais la table de travail est détaillée. |
| ME3 | Code impact = **IC par défaut**, sélectionnable ligne par ligne (IC / HC / HR). |
| ME4 | `type_intervenant` retenu : `INTERNE` / `EXTERNE` / `AUTRE` / `A_CONTROLER`. Ce type compte plus que le nom exact de l'intervenant. |
| ME5 | **(P17)** Distinguer strictement deux dates : `date_menage` = **date de prestation** (date réelle d'exécution du ménage) — pilote le **rattachement économique** (mois/logement/réservation) ; `date_facture` = **date administrative/comptable** de la facture prestataire — utile pour le suivi fournisseur et la comptabilité. La date de prestation fait foi pour l'imputation économique ; la date de facture peut être différente et **doit être conservée** pour la traçabilité comptable. |

---

## 5. Intervenants

| # | Règle |
|---|---|
| I1 | `type_intervenant = INTERNE` → code impact `HC`. |
| I2 | `type_intervenant = EXTERNE` → code impact selon facture, `IC` par défaut. |
| I3 | `type_intervenant` absent → `A_CONTROLER`. Contrôle `TYPE_INTERVENANT_ABSENT`, prioritaire sur l'identité exacte. |

---

## 6. Banque

| # | Règle |
|---|---|
| B1 | Le nom du fichier bancaire ne sert **jamais** au rattachement temporel. Rattachement par colonne `Date` ou `Valeur`. |
| B2 | Le fichier brut n'est **jamais** modifié. Toutes les transformations produisent des tables dérivées. |
| B3 | Pipeline obligatoire : règles déterministes (`REF_Banque_Regles`) → IA sur le reste → contrôle humain si doute. |
| B4 | L'IA ne valide **jamais définitivement** une ligne sensible (virement, remboursement, libellé ambigu). |
| B5 | L'IA n'écrase jamais une règle déterministe validée. Une correction humaine récurrente peut **devenir** une règle. |
| B6 | Prudence maximale sur les virements. Si le libellé n'est pas suffisamment discriminant → contrôle. |
| B7 | Niveau de risque `ELEVE` → pas de validation automatique. |
| B8 | `source_economique = HOSTAWAY_PRIORITAIRE` → la banque sert au rapprochement, jamais à créer un produit dans `MASTER_CALC_Flux`. |
| B9 | Empreinte anti-doublon : `compte_id + date_op + date_valeur + sens + montant + libellé_normalisé + devise`. |
| B10 | `BANQUE_FICHIER_PERIODE_INCOHERENTE` : non bloquant tant qu'au moins une date exploitable existe. `BANQUE_DATE_INEXPLOITABLE` : bloquant. |

---

## 7. Banque — clôture (rappel court)

Le détail de la clôture mensuelle est traité en §11 (Clôture mensuelle et périodicité). Règle banque associée :

| # | Règle |
|---|---|
| B-C1 | Tant qu'une ligne `LIGNE_BANCAIRE_NON_CLASSEE` reste ouverte, le mois ne peut pas être marqué `CLOTURE` (cf. §11). |

---

## 8. Anti-double-comptage (récapitulatif transverse)

| # | Règle |
|---|---|
| DC1 | Payout Airbnb / Booking : porté par Hostaway. La banque rapproche, ne crée pas de produit. |
| DC2 | Charge déjà saisie manuellement (M04, charges perso/liquide) : la banque rapproche, ne recrée pas de charge. |
| DC3 | Forfait local 50 € : ne doit pas apparaître à la fois dans le coût analytique M04 **et** dans `MASTER_CALC_Flux` en charge bancaire ou facture. |
| DC4 | Réservation `direct` Hostaway avec `totalPrice` non nul : risque de doublon avec saisie hors Hostaway. |
| DC5 | Contrôle bloquant : `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY`. |

---

## 9. Mapping logements (transverse)

| # | Règle |
|---|---|
| MAP1 | `REF_Mapping_Logements` couvre les libellés venant de Hostaway, du Google Sheet ménage, des factures prestataires, des fichiers bancaires utiles, et des noms courts internes. Libellés normalisés pour rapprochement. |
| MAP2 | Niveau de confiance obligatoire sur chaque ligne de mapping. |
| MAP3 | Mapping clair → intégration au résultat par logement. Mapping ambigu / absent → statut `LOGEMENT_ORPHELIN_A_CONTROLER`, pas d'intégration au résultat par logement. |
| MAP4 | `APPARTEMENT_DIVERS` / `LOGEMENT_DIVERS` réservés aux cas hors parc réels. Jamais utilisés pour masquer un mauvais mapping. |
| MAP5 | `LOGEMENT_ORPHELIN` (technique) sert uniquement à éviter la casse dans les jointures en attendant validation. Ne masque pas l'absence de mapping. |
| MAP6 | `listingMapId 515523` = vrai logement ancien / désactivé. Statut `LOGEMENT_ORPHELIN_A_CONTROLER` en attendant ajout au référentiel. Ne pas ignorer définitivement, ne pas intégrer comme logement actif. |

---

## 10. Table commune des réservations

| # | Règle |
|---|---|
| TC1 | Une **table commune** consolide toutes les réservations (Hostaway, hors Hostaway, VRBO renseignées manuellement, manuelles non présentes dans Hostaway) pour éviter les doubles comptages. |
| TC2 | Une même réservation n'alimente `MASTER_CALC_Flux` qu'**une seule fois**. |
| TC3 | Si une réservation `direct` Hostaway est reprise dans la table hors Hostaway, elle doit être **liée explicitement** via `reservation_id_hostaway`. La source financière reste la table hors Hostaway. |
| TC4 | VRBO sans montant fiable dans Hostaway → montant **renseigné manuellement**, jamais assimilé automatiquement à une réservation directe. |
| TC5 | La table commune ne crée pas de flux par elle-même : elle réconcilie les sources existantes. |

---

## 11. Clôture mensuelle et périodicité

| # | Règle |
|---|---|
| C1 | Statuts mois : `OUVERT` / `EN_CONTROLE` / `CLOTURE`. |
| C2 | Contrôles opérationnels : au fil de l'eau autorisés. |
| C3 | **Validation officielle = mensuelle**, avant clôture et avant facturation propriétaire. |
| C4 | Toutes les lignes bancaires de la période doivent être validées avant clôture. |
| C5 | Une seule ligne `LIGNE_BANCAIRE_NON_CLASSEE` ouverte → mois `CLOTURE` impossible. Calcul provisoire possible. |
| C6 | Contrôle dédié : `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE`. |
| C7 | Facturation propriétaire : seulement après clôture validée. |

---

## 12. Caisse et comptes annexes

| # | Règle |
|---|---|
| CA1 | **Caisse théorique uniquement** au départ. Pas de compte caisse séparé. |
| CA2 | Pas de livret ni compte épargne intégré au départ. |
| CA3 | Solde liquide théorique = liquide récupéré − liquide reversé − liquide utilisé en charge − liquide affecté en acompte. |

---

## 13. Identifiants

| # | Règle |
|---|---|
| ID1 | Nomenclature manuelle générale : `TYPE-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR` (cf. Archi §16.2). |
| ID2 | Clé M04 spécifique : `MEN-{AAAA-MM}-{listing ou APP}-{INTERVENANT}-{compteur}`. Ne contient pas le code impact car il est porté au moment du déversement dans le flux. |
| ID3 | Identifiants bancaires : `import_id`, `mouvement_id`, `compte_id` (cf. Archi §13.6). |

---

## À compléter ultérieurement

- Règles ménages externes affinées une fois le futur fichier construit (champs définitifs, contrôles).
- Règles de facturation propriétaire détaillées (Lot 12).
- IK → **VERROUILLÉ D036** : montant direct au démarrage. Barème kilométrique ajoutable plus tard si besoin.
- Règles précises de transition automatique entre statuts mois. États stockés dans `REF_Cloture_Mensuelle` (Archi §23.2).
- **Cancellation payout → VERROUILLÉE : voir D030, EP4.**
- **Convention d'arrondi → VERROUILLÉE D035** : calcul pleine précision, 2 décimales, tolérance 0,10 €/ligne, 1,00 € cumulé/facture.
- **REF_Couts_Standards_Menage → VERROUILLÉ D037** : standard = exécution seule. Valeurs à revalider au Lot 0.

## Statuts de contrôle (rappel)

Valeurs de `statut_controle` **fermées** : `VALIDE`, `A_CONTROLER`, `BLOQUANT`, `IGNORE_JUSTIFIE` (référentiel `REF_Statuts`, Archi §23.1). Pas de valeurs libres (`OK`, `Validé`, etc.) dans les fichiers de saisie.

---

## Principe de priorité Excel avant Power BI (D043 — P32)

| # | Règle |
|---|---|
| PBI1 | L'objectif initial est de produire un **Excel propre, automatisé, structuré et fiable**. C'est le livrable prioritaire des lots initiaux. |
| PBI2 | **Aucun lot ne livre un dashboard Power BI** ni un fichier `.pbix`. Power BI sera construit ensuite par l'utilisateur lui-même. |
| PBI3 | Les tables et CSV produits par le système doivent être **structurellement compatibles Power BI** (schéma en étoile : `MASTER_CALC_Flux` en faits, `REF_*` en dimensions). C'est une conception, pas une livraison. |
| PBI4 | Le Lot 12 produit Excel + tables + données prêtes à l'emploi — **pas le dashboard lui-même**. |

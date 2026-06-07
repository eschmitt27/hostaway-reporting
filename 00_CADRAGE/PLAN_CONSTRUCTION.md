# PLAN_CONSTRUCTION.md

> Plan de construction complet du système de pilotage conciergerie, de A à Z, lot par lot.
> Ce document décrit **quoi** produire et **dans quel ordre**, avec les entrées, les sorties, les critères de validation et les dépendances.
> Il ne prescrit **aucune méthode technique** : le choix des outils, du langage, de la structure du code et de l'approche est laissé libre à l'exécutant.
> Référence d'architecture : `ARCHITECTURE_DONNEES.md`.

---

## Conventions communes à tous les lots

Ces règles s'appliquent à chaque lot, sans être répétées à chaque fois.

| Règle | Détail |
|---|---|
| Clé + hash | Chaque table a une `PK` stable et un `ROW_HASH`. |
| Upsert non destructif | Ajout si `PK` nouvelle ; remplacement si `ROW_HASH` change ; conservation si `PK` disparue d'un extract. Jamais de suppression automatique. |
| Référentiel d'abord | Toute valeur typée (logement, propriétaire, associé, type de flux, mode de paiement, catégorie, code impact) vient de `REF_Setup`, jamais en dur. |
| Statut de contrôle | Chaque table de saisie ou de calcul porte un `statut_controle` (validé / à contrôler / bloquant / ignoré justifié). |
| Code impact | Chaque ligne à effet financier porte un `code_impact` (`IC` / `HC` / `HR`). |
| Source métier prioritaire | La banque rapproche, elle ne recrée pas un flux déjà porté par une source métier (§2.6, §13.4 bis de l'architecture). |
| Définition de terminé (DoD) | Un lot est terminé quand ses tables existent, sont peuplées, passent leurs contrôles, sont documentées, et sont lisibles depuis Excel/Power BI local. |
| Lecture ciblée par lot | Pour savoir **quelles sections lire / quels fichiers ouvrir / quoi éviter** pour un lot donné, voir la matrice `CLAUDE.md §5.bis` (source unique, non dupliquée ici pour éviter la dérive). |

Chaque lot ci-dessous précise : **Objectif · Pré-requis · Entrées · Sorties (tables) · Travail attendu · Contrôles de validation · Livrable · Risques.**

---

## PHASE A — FONDATIONS (lots 0 à 2)

### Lot 0 — Stabiliser le référentiel `REF_Setup`  `[À PRÉPARER / audit requis / non démarré]`

**Objectif.** Figer les clés, les libellés et les valeurs par défaut de tous les référentiels.

**Pré-requis.** Aucun.

**Entrées.** `REF_Setup` (19 onglets).

**Sorties.** Référentiel propre et stable, exploitable comme dimensions Power BI.

**Travail attendu.**
- Corriger l'encodage des onglets touchés (`REF_Associes`, `REF_Codes_Impact`, `REF_Types_Flux` : « associÃ© »).
- Normaliser les dates stockées en numéro de série Excel (ex. `46023`).
- Vérifier l'unicité des clés de chaque onglet (`logement_id`, `proprietaire_id`, `type_flux_id`, etc.).
- Revalider les valeurs de `REF_Couts_Standards_Menage` : elles doivent représenter le coût d'exécution ménage uniquement (D037), pas un coût complet.
- Vérifier que chaque type de flux porte bien ses drapeaux (`avantage_brut_defaut`, `deduit_avantage_defaut`, code impact par défaut).
- Vérifier que chaque catégorie de charge porte ses défauts (`impact_resultat`, `refacturable_defaut`, `hors_compta_defaut`).
- **Préparer `REF_Parametres_Generaux`** pour accueillir le taux horaire ménage interne (`TAUX_HORAIRE_MENAGE_INTERNE = 10 EUR_HEURE`), la convention d'arrondi (`ARRONDI_DECIMALES = 2`, `TOLERANCE_ARRONDI_LIGNE_EUR = 0.10`, `TOLERANCE_ARRONDI_CUMUL_EUR = 1.00` — D035), avec dates de validité et drapeau `actif`. Le drapeau `LOCAL_50_INJECTABLE_DANS_M04` est **obsolète (D016-REV)** — ne pas créer, ou créer avec statut `OBSOLETE` pour traçabilité.
- **Compléter `REF_Intervenants`** avec au minimum `intervenant_id`, `nom_intervenant`, `nom_normalise`, `type_intervenant` (INTERNE / EXTERNE / AUTRE / A_CONTROLER), `actif`, dates de validité.
- **Créer `REF_Statuts`** (onglet `REF_Setup`) avec les valeurs fermées de `statut_controle` : `VALIDE`, `A_CONTROLER`, `BLOQUANT`, `IGNORE_JUSTIFIE` (cf. Archi §23.1). Toutes les listes déroulantes de statut des fichiers de saisie pointeront vers ce référentiel — pas de valeurs libres (`OK`, `Validé`, etc.).
- **Créer `REF_Statuts_Payout`** (onglet `REF_Setup`) avec les valeurs fermées de `statut_calcul_payout` : `NORMAL`, `ANNULE_SANS_PAYOUT`, `ANNULE_AVEC_PAYOUT`, `PAYOUT_ABSENT`, `PAYOUT_INCOMPLET`, `A_CONTROLER` (cf. Archi §23.2bis, D021). Distinct de `REF_Statuts`.
- **Créer `REF_Cloture_Mensuelle`** : structure vide (onglet `REF_Setup` ou CSV dédié). Colonnes : `mois`, `statut_mois`, `date_passage_controle`, `date_cloture`, `nb_lignes_bancaires_non_classees`, `nb_controles_bloquants_ouverts`, `commentaire`. Elle sera alimentée au Lot 8 (D024).
- **Supprimer / marquer obsolète le paramètre `LOCAL_50_INJECTABLE_DANS_M04`** dans `REF_Parametres_Generaux` (D016-REV). Conserver pour traçabilité avec date de clôture.
- **Vérifier la présence d'un code `APPARTEMENT_DIVERS` / `LOGEMENT_DIVERS`** dans `REF_Logements` pour les cas hors parc réels (jamais utilisé pour masquer un mauvais mapping).

**Contrôles de validation.** Aucune clé dupliquée ; aucune date en série brute ; aucun caractère mal encodé ; tous les taux de commission renseignés.

**Livrable.** `REF_Setup` validé + courte note des corrections faites.

**Risques.** Un défaut de référentiel se propage à tout le système → ce lot conditionne tout.

---

### Lot 1 — Module Hostaway  `[extraction existante éventuelle — non validée sur données réelles]`

**Objectif.** Extraire et structurer toutes les données Hostaway, calculer le payout, détecter les anomalies.

**Pré-requis.** Lot 0.

**Entrées.** API Hostaway (Listings, Reservations, Details, Finance fields, Fees, Tasks).

**Sorties.** `MASTER_REF_HA_Listings`, `MASTER_FACT_HA_Reservations`, `…ReservationDetails`, `…ReservationFinanceFields`, `…ReservationFees`, `MASTER_CALC_HA_Payout`, `MASTER_CTRL_HA_Anomalies`, `MASTER_FACT_HA_CleaningTasks_Discovery`, `MASTER_RUN_Log`.

**Travail existant à contrôler / reprendre si nécessaire.** Extraction incrémentale, upsert, calcul payout Airbnb/Booking, anomalies — produit par le run précédent mais **non validé sur données réelles** (D029). À recontrôler avant tout passage au Lot 10.

**Contrôles de validation.** Compteurs cohérents dans `MASTER_RUN_Log` ; payout présent pour toute réservation active Airbnb/Booking ; anomalies bloquantes traitées.

**Livrable.** Tables `*_HA_*` à jour. `[Existant]`

**Risques.** Variables de payout manquantes (anomalie `BOOKING_PAYOUT_INCOMPLET`).

---

### Lot 2 — Réconciliation logements (toutes sources)

**Objectif.** Garantir que **chaque libellé de logement** issu de **toute source du projet** se rattache à un `logement_id` unique du référentiel.

**Pré-requis.** Lots 0 et 1.

**Entrées.**
- `MASTER_REF_HA_Listings` (libellés Hostaway).
- `REF_Logements`, `REF_Mapping_Logements` (état actuel : 81 lignes).
- **Libellés du Google Sheet `Suivi ménage`** (colonnes appartement utilisées par M04). Ex. observés sur le run actuel : `T3 - 18 cugnaux (David)`.
- Libellés `Rangement` du Google Sheet → code technique `RANGEMENT` (main-d'œuvre, reste dans M04). Libellés `Courses` → **ne pas mapper comme source économique** : la colonne `Courses` est héritée et doit être ignorée à l'import M04 (D027). Si elle subsiste dans le Google Sheet, noter sa présence sans lui attribuer de logement ni de code économique.
- Libellés des **futures factures prestataires externes** (Lot 6c).
- Libellés bancaires utiles (préparation Lot 8 : tiers récurrents reliés à un logement, ex. fournisseur d'eau d'un appartement précis).

**Sorties.** `REF_Mapping_Logements` complété et fiabilisé ; table/vue de contrôle des orphelins.

**Travail attendu.**
- Résoudre les orphelins Hostaway connus :
  - `listingMapId 515523` → statut `LOGEMENT_ORPHELIN_A_CONTROLER`, **vrai logement ancien / désactivé**, à ajouter plus tard dans `REF_Setup`, **ne pas ignorer définitivement, ne pas intégrer comme actif maintenant**.
  - `480780` / LOG_0016 : `sur_hostaway = NON`, confirmer comme normal.
  - `497801` : à arbitrer.
- Vérifier que chaque libellé de mapping a un `niveau_confiance`.
- **Mapper explicitement les libellés appartement du Google Sheet `Suivi ménage`** vers `logement_id` (pré-requis du Lot 6b — sans ça, M04 ne peut pas produire un résultat par logement).
- Créer / vérifier les codes techniques pour les cas hors parc : `LOGEMENT_ORPHELIN`, `APPARTEMENT_DIVERS`, `LOGEMENT_DIVERS`.
- Préparer la résolution des libellés futurs factures prestataires (Lot 6c) et libellés bancaires tiers (Lot 8).

**Contrôles de validation.**
- Zéro `listingMapId` actif non mappé ;
- zéro logement `sur_hostaway = OUI` absent de l'export non justifié ;
- **zéro libellé Google Sheet `Suivi ménage` non mappé** (sauf cas justifiés `LOGEMENT_ORPHELIN_A_CONTROLER` ou techniques `RANGEMENT` / `COURSES`).

**Livrable.** Mapping logements complet + liste des orphelins résolus/justifiés + couverture explicite des libellés Google Sheet.

**Risques.** **Maillon le plus risqué du projet** : un mauvais rattachement fausse tout le résultat par logement. Si les libellés Google Sheet ne sont pas couverts ici, le Lot 6b cassera en cascade (`MENAGE_SANS_LOGEMENT_ID` bloquant).

---

## PHASE B — SAISIES MÉTIER (lots 3 à 7)

### Lot 3 — `SAISIE_Charges_Flux.xlsx` : source unique des achats et charges

**Objectif.** Construire `SAISIE_Charges_Flux.xlsx`, source unique de toutes les charges, achats, consommables, produits ménage, linge/lavage, matériel, charges perso/liquide/compte pro (D026). Inclut les nouvelles catégories **`INCIDENT_VOYAGEUR`** (D041 — `reservation_id` obligatoire) et **`PRESTATION_AIRCOVER_REFACTUREE`** (D042 — gestion sinistre facturée au propriétaire, alimente `charges_exceptionnelles_refacturees`).

**Périmètre inclus.** Achats, charges, consommables, produits ménage, linge, lavage, matériel, charges perso/liquide/compte pro, dépenses perso sur compte pro, forfait local mensuel, charges exceptionnelles refacturables.
**Périmètre exclu (interdit dans ce fichier).** IK, virements associés, avances associés → `MASTER_FACT_MAN_IK_Avantages` (Lot 7, D025).

**Pré-requis.** Lots 0 et 2.

**Entrées.** Saisie manuelle ; défauts de `REF_Categories_Charges`, `REF_Types_Flux`, `REF_Modes_Paiement`, `REF_Cartes_Paiement`, `REF_Associes`.

**Sorties.** `SAISIE_Charges_Flux.xlsx` (saisie) → `MASTER_FACT_MAN_Charges` (table normalisée) → `VUE_ACHATS_MENAGE_VALIDES` (vue dérivée filtrée pour M04/ménages).

**Travail attendu.**
- Construire `SAISIE_Charges_Flux.xlsx` avec listes déroulantes (dont `REF_Statuts`, `REF_Categories_Charges`), colonnes obligatoires, statut, alertes doublons (§4.2, SC1-SC5).
- Définir les catégories de charge liées aux ménages (`LINGE`, `CONSOMMABLE_MENAGE`, `PRODUIT_MENAGE`, `MATERIEL_MENAGE`, `FRAIS_LOCAL`) pour alimenter `VUE_ACHATS_MENAGE_VALIDES`.
- Définir la catégorie `CHARGE_EXCEPTIONNELLE_REFACTURABLE` (impacte `montant_du_conciergerie` uniquement, pas `revenu_net_exploitation`).
- Implémenter les effets métier (§10.2) : impact résultat réel/comptable, lien avantage associé, refacturable.
- Identifiant suivant la nomenclature §16.2 (`CHG-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR`).

**Contrôles de validation.** Aucune charge `LOGEMENT` sans `logement_id` ; aucune charge perso sans associé ; aucun IK ni virement associé présent dans ce fichier ; `prise_en_compta` toujours renseigné ; `VUE_ACHATS_MENAGE_VALIDES` filtre correctement.

**Livrable.** `SAISIE_Charges_Flux.xlsx` + `MASTER_FACT_MAN_Charges` + `VUE_ACHATS_MENAGE_VALIDES`.

**Risques.** Double comptage avec la banque (Lot 8) → respecter source métier prioritaire. Double comptage M04/charges si un poste ménage est saisi ici ET injecté depuis M04 → contrôle `ACHATS_DEJA_EN_SAISIE_CHARGES`.

---

### Lot 4 — Réservations hors Hostaway

**Objectif.** Porter la vérité financière des réservations directes (une ligne par réservation).

**Pré-requis.** Lots 0, 2, 3.

**Entrées.** Saisie manuelle ; `REF_Proprietaires`, `REF_Logements`, `REF_Canaux_Reservation`, `REF_Associes`, `REF_Modes_Paiement`. Lien optionnel vers `reservation_id` Hostaway.

**Sorties.** `MASTER_FACT_MAN_ReservationsHorsHostaway`.

**Travail attendu.**
- Construire le fichier de saisie contrôlé.
- Implémenter la formule d'acompte (§9.3) : `Acompte = Total perçu − Ménage − Commission − Reversé propriétaire`.
- Implémenter la commission HH : `(Total perçu − Ménage) × taux`.
- Relier le `montant_recupere` aux avantages associés (Lot 7).
- Exclure les `ownerStay`.
- **Saisie manuelle obligatoire pour VRBO `paymentStatus = Unknown`** dont le montant n'apparaît pas clairement dans Hostaway.

**Contrôles de validation.** Aucune réservation sans propriétaire ; acompte cohérent avec la formule ; montant récupéré toujours associé à un associé ; pas de doublon avec une réservation `direct` Hostaway portant un montant.

**Livrable.** Table HH peuplée + fichier de saisie contrôlé.

**Risques.** Logique la plus subtile (récupéré → reversé → acompte → avantage → charge payée avec).

---

### Lot 4 bis — Table commune des réservations

**Objectif.** Réconcilier toutes les réservations (Hostaway, hors Hostaway, VRBO manuelles, manuelles hors plateforme) sous un schéma unique, pour empêcher tout double comptage en amont de `MASTER_CALC_Flux`.

**Pré-requis.** Lots 1 (Hostaway) et 4 (hors Hostaway).

**Entrées.** `MASTER_FACT_HA_Reservations`, `MASTER_CALC_HA_Payout`, `MASTER_FACT_MAN_ReservationsHorsHostaway`.

**Sorties.** `MASTER_CALC_Reservations` (cf. Archi §9.5).

**Travail attendu.**
- Empiler les sources sous le schéma commun (`source`, `reservation_id_hostaway`, `reservation_hh_id`, `montant_retenu`, `source_montant`, `code_impact`).
- Implémenter les règles de réconciliation :
  - réservation `direct` Hostaway avec `totalPrice > 0` + ligne HH liée → retenir une seule ligne, `source_montant = MANUEL` ;
  - VRBO `Unknown` sans saisie manuelle → `statut_controle = A_CONTROLER`, pas de montant retenu ;
  - Hostaway Airbnb / Booking sans contrepartie manuelle → `source_montant = HOSTAWAY`.
- Contrôles : `RESERVATION_DOUBLON_HOSTAWAY_HH`, `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH`, `RESERVATION_VRBO_MONTANT_NON_RENSEIGNE`.

**Contrôles de validation.** Aucune réservation comptée deux fois ; cohérence `source_montant` ↔ `code_impact` ; toute réservation déversée dans `MASTER_CALC_Flux` (Lot 9) passe par cette table.

**Livrable.** Table consolidée + vue de contrôle des doublons potentiels.

**Risques.** Sans cette étape, risque de double comptage sur les `direct` Hostaway avec saisie HH parallèle.

---

### Lot 5 — Acomptes propriétaires

**Objectif.** Suivre les acomptes rattachés aux factures propriétaires, avec report.

**Pré-requis.** Lots 0, 2, 4.

**Entrées.** Saisie manuelle ; sortie de Lot 4 (acomptes issus des réservations HH) ; `REF_Proprietaires`, `REF_Logements`.

**Sorties.** `MASTER_FACT_MAN_AcomptesProprietaires`.

**Travail attendu.**
- Rattachement obligatoire à une facture (`facture_ref`).
- Gestion du report au mois suivant si excédentaire.
- Granularité `proprietaire_id + logement_id + mois + facture_ref`.

**Contrôles de validation.** Aucun acompte sans facture ; report cohérent ; logement présent si facturation par appartement.

**Livrable.** Table acomptes peuplée.

**Risques.** Acompte non rattaché → facturation propriétaire fausse.

---

### Lot 6 — Ménages (trois sources distinctes)

Le Module 4 Ménages combine **trois sources** qui n'ont pas le même statut. Elles doivent rester séparées dans la construction.

#### Lot 6a — Hostaway : comparaison ménages réalisés / attendus

**Objectif.** Utiliser `MASTER_FACT_HA_CleaningTasks_Discovery` comme **point de comparaison**, pas comme source de coût.

**Pré-requis.** Lot 1 (déjà fait : 451 tâches extraites).

**Entrées.** Tâches Hostaway ; `REF_Types_Lignes_Menage` ; statuts (completed / confirmed / pending / cancelled).

**Sorties.** Vue de comparaison réalisé / déclaré / facturé (sans table dédiée nouvelle ; alimente les contrôles).

**Travail attendu.**
- Compter les ménages par logement × mois selon `REF_Types_Lignes_Menage`.
- Comparer avec les ménages déclarés (Lot 6b) et facturés (Lot 6c).
- **Ne jamais utiliser** le `cost` Hostaway (vide à 95 %) comme valorisation.

**Contrôles de validation.** Aucun coût Hostaway intégré au résultat ; vue de comparaison disponible.

**Risques.** Tentation d'utiliser le `cost` Hostaway → règle explicite à interdire.

---

#### Lot 6b — Ménages internes (`M04_MENAGES_PowerQuery.xlsx`)

**Objectif.** Produire la table normalisée mensuelle du **coût d'exécution ménage interne** (main-d'œuvre uniquement, toujours `HC`). M04 ne contient plus d'achats ni de coût de lavage (D027).

**Pré-requis.** Lots 0, 2 **et 3** (obligatoire : `SAISIE_Charges_Flux.xlsx` doit exister pour que la séparation charges/exécution soit opérationnelle).

**Entrées.** Google Sheet `Suivi ménage` (heures, nb ménages, intervenant, mois, appartement, Rangement — **sans onglet `achats`**, sans colonne `Courses`, sans `Coût du lavage`) ; `REF_Logements`, `REF_Couts_Standards_Menage`, `REF_Intervenants`, `REF_Mapping_Logements`.

**Sorties.** Requête Power Query `tbl_MASTER_FACT_MEN_Menages` dans `M04_MENAGES_PowerQuery.xlsx`. Export CSV optionnel (technique, jamais source officielle).

**Travail attendu.**
- Implémenter les formules simplifiées de §11.4 de l'architecture (exécution uniquement) :
  `Coût d'exécution = heures × TAUX_HORAIRE_MENAGE_INTERNE` ; `Ecart_standard = standard − cout_execution_unitaire`.
- Garder `10 €/h` codé en dur ; prévoir migration vers `REF_Parametres_Generaux`.
- **Supprimer toute logique** `Courses`, `TotalCourses`, `Coût du lavage`, `Quote-part achats`, `cout_complet_analytique`, `montant_injectable_flux`, `LOCAL_50_INJECTABLE_DANS_M04`.
- Code impact = **HC obligatoire** sur toute ligne M04.
- Clé : `menage_calc_id = MEN-{AAAA-MM}-{listing ou APP_SANITIZED}-{INTERVENANT}-{compteur}`.
- Alimenter `MASTER_CALC_Flux` (Lot 9) avec `type_flux_id = COUT_EXECUTION_MENAGE_INTERNE`, `sens = CHARGE`, `code_impact = HC`.

**Contrôles de validation.** Aucune ligne M04 hors `HC` ; aucune colonne `Courses`/`Coût du lavage` dans la sortie ; aucun achat ou consommable dans le flux M04 ; aucun appartement non mappé ne contribue au résultat.

**Livrable.** Classeur Power Query actualisé (main-d'œuvre seule) + export CSV optionnel.

**Risques.** Si `Courses` ou `Coût du lavage` subsistent dans le Google Sheet → les ignorer à l'import (ne pas les calculer).

---

#### Lot 6c — Ménages externes (futur fichier)

**Objectif.** Construire la table des coûts ménage externes à partir des factures PDF de prestataires, transformées par IA.

**Pré-requis.** Lots 0 et 2 ; Lot 8 partiel (règles déterministes de classification) ; cadrage IA distinct.

**Entrées.** Factures PDF prestataires ; `REF_Intervenants`, `REF_Logements`, `REF_Mapping_Logements`.

**Sorties.** Table `MASTER_FACT_MEN_MenagesExternes` (nom indicatif).

**Travail attendu.**
- **Granularité obligatoire** : 1 ligne = 1 ménage × 1 appartement × 1 date × 1 prestataire.
- Conserver la référence facture, mais la table de travail est détaillée.
- Schéma minimal cible : `menage_externe_id`, `facture_id`, `date_facture`, `date_menage`, `mois`, `annee`, `prestataire_id`, `nom_prestataire`, `type_intervenant`, `logement_id`, `hostaway_listing_id`, `appartement_source`, `type_ligne_menage_id`, `nombre_menages`, `montant_ligne_ht`, `montant_ligne_ttc`, `montant_facture_total_ht`, `montant_facture_total_ttc`, `code_impact`, `prise_en_compta`, `statut_controle`, `source_document`, `nom_fichier_source`, `commentaire`, `ROW_HASH`.
- Code impact = **IC par défaut**, sélectionnable ligne par ligne (IC / HC / HR).
- Contrôles : `MENAGE_EXTERNE_LOGEMENT_ABSENT`, `MENAGE_EXTERNE_DATE_ABSENTE`, `MENAGE_EXTERNE_PRESTATAIRE_INCONNU`, `MENAGE_EXTERNE_CODE_IMPACT_ABSENT`, `MENAGE_EXTERNE_FACTURE_NON_RECONCILIEE`, `MENAGE_EXTERNE_A_VENTILER`.

**Contrôles de validation.** Total ligne × nombre = total ligne TTC cohérent ; somme lignes = total facture (ou ventilation justifiée) ; code impact présent sur toutes les lignes.

**Livrable.** Table peuplée à partir des factures PDF disponibles.

**Risques.** Mauvaise extraction IA des PDF ; mauvaise réconciliation ligne ↔ facture totale ; mauvaise attribution prestataire / logement.

---

### Lot 7 — IK & avantages associés

**Objectif.** Consolider les avantages par associé et par mois, sans double saisie.

**Pré-requis.** Lots 3 et 4 (les avantages s'en déduisent en partie).

**Entrées.** Saisie pure (virements associés, IK, avances, corrections) ; dérivés des Lots 3 et 4 ; `REF_Associes`, `REF_Types_Flux`.

**Sorties.** `MASTER_FACT_MAN_IK_Avantages` (saisie) + `MASTER_CALC_AvantagesAssocies` (calcul).

**Travail attendu.**
- Table de saisie limitée aux flux non disponibles ailleurs.
- Table calculée : `avantages_bruts − charges_payees_pour_societe = avantages_nets`.
- Empiler les trois sources d'avantage brut (virement, dépense perso compte pro, montant récupéré HH).
- IK en montant direct (D036). Schéma minimal obligatoire : `associe_id`, `mois`, `type_flux`, `nature`, `montant`, `commentaire`, `statut_controle`, `impact_resultat_reel`, `impact_resultat_comptable`.

**Contrôles de validation.** Aucun montant récupéré HH non reflété dans les avantages ; pas de double comptage entre saisie et dérivé.

**Livrable.** Deux tables avantages (saisie + calcul).

**Risques.** Double comptage si une charge déjà saisie est ressaisie ici.

---

## PHASE C — CONSOLIDATION (lots 8 à 11)

### Lot 8 — Banque & rapprochement bancaire

**Objectif.** Constater les flux réels du compte pro et rapprocher sans doubler les flux métier.

**Pré-requis.** Lots 3 et 7 stabilisés (pour rapprocher), Lot 1 (payouts).

**Entrées.** Export Crédit Mutuel (ex. `2026_03_BRUT_Banque_CreditMutuel.xlsx`) ; `REF_Banque_Regles` (à créer) ; `REF_Cartes_Paiement`, `REF_Types_Flux`, `REF_Categories_Charges`.

**Sorties.** `BRUT_Banque`, `NORM_Banque`, `IA_Classification`, `CTRL_A_CONTROLER`, `LOG_Traitement`, `REF_Cloture_Mensuelle` (états de mois, Archi §23.2), et `REF_Banque_Regles` peuplé.

**Travail attendu.**
- Import brut → normalisation (date, libellé, montant, sens, compte, empreinte).
- **Rattachement temporel** : toujours par colonnes `Date` ou `Valeur`, jamais par le nom du fichier (un fichier nommé `2026_03_...` peut couvrir 25/02 → 25/04).
- Empreinte anti-doublon : `compte_id + date_op + date_valeur + sens + montant + libellé_normalisé + devise` (Archi §13.4).
- **Créer `REF_Banque_Regles`** comme référentiel de règles déterministes (pas seulement classification). Colonnes minimales : `regle_id`, `priorite`, `actif`, `compte_id`, `type_match` (CONTIENT / COMMENCE_PAR / REGEX / MONTANT_EXACT / etc.), `champ_cible`, `motif`, `tiers_detecte`, `categorie`, `type_flux_id`, `code_impact`, `source_economique` (BANQUE_SOURCE / BANQUE_RAPPROCHEMENT / HOSTAWAY_PRIORITAIRE / MANUEL_PRIORITAIRE / A_CONTROLER), `rapprochement_requis`, `validation_automatique`, `niveau_risque` (FAIBLE / MOYEN / ELEVE), `statut_controle_defaut`, dates de validité, `commentaire`.
- Pipeline obligatoire : **règles déterministes d'abord, IA sur le reste, contrôle humain si doute**.
- L'IA n'écrase **jamais** une règle déterministe validée. Une correction humaine récurrente peut devenir une nouvelle règle.
- **Prudence maximale sur les virements** (`VIR INST`, `VIREMENT RECU/EMIS`, `REMBOURSEMENT`, `IMPAYE`, `TRANSFERT`, `VIR ASSOCIE`, `VIR PROPRIETAIRE`, libellé incomplet, tiers inconnu) : si libellé non discriminant → contrôle obligatoire, jamais de classification automatique définitive.
- `niveau_risque = ELEVE` → pas de validation automatique.
- Appliquer la règle anti-double-comptage (Archi §13.4 bis) : `source_economique = HOSTAWAY_PRIORITAIRE` → banque sert au rapprochement, jamais à créer un produit bancaire dans `MASTER_CALC_Flux`.

**Statuts mois et clôture.**
- Statuts mois : `OUVERT` / `EN_CONTROLE` / `CLOTURE`, stockés dans la table **`REF_Cloture_Mensuelle`** (PK `mois`, cf. Archi §23.2) — c'est elle qui matérialise officiellement l'état du mois.
- **Toute ligne `LIGNE_BANCAIRE_NON_CLASSEE` ouverte → mois non clôturable** (un calcul provisoire reste possible mais le mois ne peut pas être marqué fiable).
- Contrôle dédié : `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE`.

**Contrôles spécifiques (Archi §18.2 / §18.3).**
- Bloquants : `BANQUE_LIGNE_SANS_DATE` (aucune date ni `Date` ni `Valeur` dans la ligne), `BANQUE_DATE_INEXPLOITABLE` (date présente mais non interprétable / non convertible — distinct du précédent), `BANQUE_LIGNE_SANS_LIBELLE`, `BANQUE_DEBIT_CREDIT_VIDES`, `BANQUE_DEBIT_CREDIT_DOUBLES`, `BANQUE_MONTANT_NON_NUMERIQUE`, `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY`.
- À contrôler : `BANQUE_FICHIER_PERIODE_INCOHERENTE` (non bloquant tant qu'au moins une date exploitable existe), `BANQUE_DEVISE_NON_EUR`, `DOUBLON_BANCAIRE_POTENTIEL`, `LIGNE_BANCAIRE_NON_CLASSEE`, `IA_CONFIANCE_INSUFFISANTE`, `VIREMENT_BANCAIRE_AMBIGU`, `REMBOURSEMENT_BANCAIRE_AMBIGU`.

**Contrôles de validation.** Aucun doublon de mouvement ; aucun produit bancaire en double d'un payout Hostaway ; aucune ligne sensible classée automatiquement ; aucune ligne non classée ouverte sur un mois marqué `CLOTURE`.

**Livrable.** Tables bancaires + rapprochements + `REF_Banque_Regles` peuplé pour les libellés récurrents.

**Risques.** Double comptage (payout vu par Hostaway ET par la banque, charge déjà saisie ET re-vue côté banque) ; mauvaise classification automatique d'un virement → conséquences sur les avantages associés.

---

### Lot 9 — Table de flux unifiée

**Objectif.** Empiler tous les événements économiques sous un schéma commun.

**Pré-requis.** Lots 1, 3, 4, 4 bis, 5, 6, 7, 8 (toutes les sources de flux).

**Entrées.** `MASTER_CALC_Reservations` (table commune, point d'entrée unique des réservations), autres tables `MASTER_FACT_*`, avantages, acomptes, ménages, banque (source seulement si applicable).

**Sorties.** `MASTER_CALC_Flux`.

**Travail attendu.**
- Normaliser chaque source vers le schéma commun (montant positif + `sens` PRODUIT/CHARGE/NEUTRALISATION).
- Affecter `code_impact` et pré-calculer `inclure_resultat_comptable/hors_compta/reel`.
- Conserver la traçabilité (`source_module`, `source_table`, `source_pk`).
- Appliquer la règle anti-double-comptage banque/métier.
- **Les réservations entrent exclusivement via `MASTER_CALC_Reservations`** : ni `MASTER_FACT_HA_Reservations`, ni `MASTER_FACT_MAN_ReservationsHorsHostaway` n'alimentent directement le flux.

**Contrôles de validation.** Toute ligne tracée vers sa source ; somme des flux par source = somme des tables sources ; aucun double comptage.

**Livrable.** Table de flux unifiée.

**Risques.** C'est la pièce centrale : une erreur ici fausse les trois résultats.

---

### Lot 10 — Résultats, commissions & livrables propriétaires (exploitation)

**Objectif.** Produire résultat réel / comptable / hors compta, net propriétaire, `revenu_net_exploitation_proprietaire` et les deux blocs propriétaire (exploitation + règlement).

**Pré-requis.** Lot 9.

**Entrées.** `MASTER_CALC_Flux`, `REF_Codes_Impact`, `REF_Proprietaires` (taux, `charge_fixe_mensuelle`), `MASTER_CALC_HA_Payout` (dont `statut_calcul_payout`).

**Sorties.** `MASTER_CALC_Resultats`, `MASTER_CALC_Commissions`, `MASTER_CALC_NetProprietaire` (avec blocs exploitation + règlement).

**Travail attendu.**
- Trois résultats = filtres sur `code_impact` (REEL = IC+HC, COMPTABLE = IC, EXTRA = HC).
- Commission = (payout − ménage) × taux, **avec récupération du ménage par canal** (§8.3 — point critique Airbnb).
- Cancellation payout (D030) : `BaseCommission = CancellationPayout`, pas de ménage déduit.
- Calculer `revenu_net_exploitation_proprietaire = TotalPayout − MenageFacture − CommissionConciergerie − charge_fixe_mensuelle` (D031, §8.5).
- Construire le **bloc règlement** : `montant_du_conciergerie`, `acompte_conciergerie_recu_via_airbnb`, `reste_a_payer_conciergerie` (D032/D033, §8.6).
- Garantir la séparation exploitation / règlement : le bloc règlement ne modifie jamais `revenu_net_exploitation_proprietaire`.
- Agrégations par mois, logement, propriétaire, global.

**Contrôles de validation.** Réel = comptable + hors compta ; commission Airbnb sur assiette hors ménage ; `revenu_net_exploitation` ne contient aucun acompte/avance/paiement ; `charge_fixe_mensuelle` sans charge exceptionnelle ; `CONFUSION_PAYOUT_SOLDE_FACTURE` absent ; seuils arrondi D035 respectés (0,10 €/ligne, 1,00 €/cumul).

**Livrable.** Tables de résultats + `MASTER_CALC_NetProprietaire` avec les deux blocs.

**Risques.** Assiette commission par canal (§8.3). Confusion revenu net / règlement si blocs mélangés.

---

### Lot 11 — Contrôles de cohérence globaux

**Objectif.** Centraliser tous les contrôles inter-modules.

**Pré-requis.** Tous les lots produisant des tables.

**Entrées.** Toutes les tables.

**Sorties.** `MASTER_CTRL_Coherence`.

**Travail attendu.**
- Implémenter les contrôles bloquants et non bloquants (§18.2, §18.3).
- Caisse théorique liquide (§10.5).
- Rapprochement payout vs banque.
- Sévérité juste (bloquant seulement si un résultat/facture est faux).
- Contrôle `ACHATS_DEJA_EN_SAISIE_CHARGES` (remplace `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL` — obsolète D016-REV).
- Contrôle clôture mois `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE`.
- Contrôle `TYPE_INTERVENANT_ABSENT` (prioritaire sur `INTERVENANT_MENAGE_INCONNU`).
- Contrôles exploitation/règlement : `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION`, `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION`, `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE`, `PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT`, `CONFUSION_PAYOUT_SOLDE_FACTURE`.

**Contrôles de validation.** Tous les contrôles tournent ; les bloquants ouverts sont visibles ; les ignorés sont justifiés.

**Livrable.** Table de contrôle + tableau de bord d'anomalies.

**Risques.** Trop de bloquants → paralysie ; trop peu → erreurs silencieuses.

---

## PHASE D — EXPLOITATION (lot 12)

### Lot 12 — Livrables propriétaires Excel (préparation données Power BI)

**Objectif.** Produire les factures propriétaires au format verrouillé (§17.3) et les vues de pilotage.

**Pré-requis.** Lots 10 et 11.

**Entrées.** `MASTER_CALC_NetProprietaire` (blocs exploitation + règlement), `MASTER_CALC_Resultats`, acomptes, charges refacturables, `SAISIE_Charges_Flux.xlsx` (charges exceptionnelles refacturables).

**Sorties.** Factures propriétaires Excel (par logement/mois) au format §17.3 (12 lignes) ; **données structurées prêtes à l'emploi pour Power BI** (schéma étoile : `MASTER_CALC_Flux` en faits, `REF_*` en dimensions). **Aucun dashboard Power BI livré** (D043/PBI2).

**Travail attendu.**
- Facture affichant séparément les 12 lignes de §17.3 (voir Archi §17).
- Bloc exploitation : total payout, ménage facturé, commission, charge fixe mensuelle, **revenu net d'exploitation**.
- Bloc règlement : montant dû conciergerie, acompte Airbnb reçu, autres paiements, **reste à payer**, charges exceptionnelles refacturées.
- Produire `FACT_FACTURE_ENTETE` et `FACT_FACTURE_LIGNES` (Archi §17.4, D040).
- Excel de contrôle par mois / propriétaire / logement.
- Vérifier que revenu_net_exploitation ≠ solde à payer dans toutes les sorties.
- Vues Excel et CSV exploitables par mois, logement, propriétaire, associé (avantages). Tables conçues pour être directement utilisables dans Power BI par l'utilisateur lui-même.

**Contrôles de validation.** Facture = somme des lignes incluses ; blocs séparés ; vues cohérentes avec `MASTER_CALC_Resultats` ; aucun contrôle de §18.2 actif pour ce mois.

**Livrable.** Excel propre, automatisé, structuré, contrôlable. Données prêtes pour un dashboard Power BI ultérieur — **non livré par le projet** (D043).

**Risques.** Mise en forme visuelle de la facture à valider avec les propriétaires.

---

## Graphe de dépendances (synthèse)

```text
Lot 0 ──► Lot 1 ──► Lot 2 ──┬─► Lot 3 ──► Lot 4 ──► Lot 5
                            │      │         │
                            ├─► Lot 6        └──► Lot 7 ◄── (dérive de 3 et 4)
                            │                         │
                  Lot 8 (banque, autonome) ───────────┤
                            │                         │
                            └──────────► Lot 9 (flux) ◄┘
                                            │
                                         Lot 10 (résultats)
                                            │
                                         Lot 11 (contrôles)
                                            │
                                         Lot 12 (livrables)
```

**Chemin critique** : 0 → 1 → 2 → 3 → 4 → 4 bis → 7 → 9 → 10 → 11 → 12.
**Parallélisable** : Lot 6 (ménages) après Lot 2 ; Lot 8 (banque) dès que Lot 3 est stable.

---

## Jalons de validation

| Jalon | Atteint quand | Vérifiable par |
|---|---|---|
| J1 — Fondations | Lots 0-2 terminés | Tout logement Hostaway mappé |
| J2 — Saisies métier | Lots 3-7 terminés | Charges, HH, acomptes, ménages, avantages peuplés et contrôlés |
| J3 — Consolidation | Lots 8-11 terminés | Les trois résultats sont produits et réconciliés, sans double comptage |
| J4 — Exploitation | Lot 12 terminé | Une facture propriétaire mensuelle est générée et vérifiée |

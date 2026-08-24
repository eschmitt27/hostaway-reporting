# DECISIONS_METIER.md
> Registre des décisions métier validées + décisions ouvertes.
> Toute nouvelle décision = nouvelle entrée datée. Ne jamais modifier une décision existante : ajouter une révision.
> Source de vérité : REGLES_METIER.md (métier) et ARCHITECTURE_DONNEES.md (structure).

---

## DÉCISIONS VALIDÉES

### D001 — Assiette de commission
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Assiette = PayoutPlateforme − MenageRetenu. Commission = Assiette × TauxCommission.
⚠ Point critique n°1 : le ménage n'est pas au même endroit selon le canal (voir D005).
Tables : MASTER_CALC_HA_Payout, MASTER_CALC_Commissions

### D002 — Net propriétaire
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : NetProprietaire = (PayoutPlateforme − MenageRetenu) × (1 − TauxCommission)
Tables : MASTER_CALC_NetProprietaire

### D003 — Payout Airbnb
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : PayoutAirbnb = airbnbExpectedPayoutAmount. Fallback : financeField[airbnbPayoutSum].
Tables : MASTER_CALC_HA_Payout

### D004 — Payout Booking
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : PayoutBooking = totalPriceFromChannel − cityTax − otaPaymentProcessingFee − hostChannelFee (finance fields). Fallback moins fiable si finance fields absents.
Exemple validé : 360,36 − 6,26 − 5,22 − 60,20 = 288,68 €
Tables : MASTER_CALC_HA_Payout, MASTER_FACT_HA_ReservationFinanceFields

### D005 — Source du ménage retenu par canal ⚠ CRITIQUE
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision :
  Airbnb → financeField[cleaningFee] (colonne CleaningFee payout TOUJOURS vide pour Airbnb)
  Booking → colonne CleaningFee de la table payout (95/110 renseignés)
  VRBO/Direct → finance fields ou saisie manuelle
Tables : MASTER_FACT_HA_ReservationFinanceFields, MASTER_CALC_HA_Payout

### D006 — Hostaway hors plateforme
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Hostaway n'est JAMAIS source financière des réservations hors plateforme.
Source = MASTER_FACT_MAN_ReservationsHorsHostaway.
Tables : MASTER_FACT_MAN_ReservationsHorsHostaway

### D007 — Coût ménage
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Coût réel = factures prestataires / suivi interne.
Coût standard = REF_Couts_Standards_Menage (Studio 29€, T2 39€, T3 55€, T4 69€, T6/Duo 110€).
Prix ménage Hostaway ≠ coût réel (interdit en valorisation).

### D008 — ownerStay
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Séjours ownerStay exclus totalement du résultat.
Tables : MASTER_FACT_HA_Reservations

### D009 — Granularité réservations hors Hostaway
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : 1 ligne par réservation.
Tables : MASTER_FACT_MAN_ReservationsHorsHostaway

### D010 — Formule acompte hors Hostaway
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : AcompteFacture = TotalPercu − Menage − Commission − MontantReverseProprietaire.
⚠ Valide UNIQUEMENT pour les réservations hors Hostaway. Non générique.
Tables : MASTER_FACT_MAN_ReservationsHorsHostaway, MASTER_FACT_MAN_AcomptesProprietaires

### D011 — Avantage net associé
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : AvantageNet = AvantagesBruts + IK − ChargesPayeesPourSociété.
Sources avantage brut : virement perso, dépense perso sur compte pro, montant récupéré HH.
Tables : MASTER_CALC_AvantagesAssocies

### D012 — Codes impact
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision :
  IC = intra-comptable (comptable OUI, réel OUI)
  HC = hors compta (comptable NON, réel OUI)
  HR = hors résultat (comptable NON, réel NON)
  Résultat réel = IC + HC. Résultat comptable = IC uniquement.
Tables : MASTER_CALC_Flux (colonne code_impact)

### D013 — Convention table de flux
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : montant toujours positif + colonne sens (PRODUIT / CHARGE / NEUTRALISATION).
Résultat = Σ(PRODUITS) − Σ(CHARGES).
Tables : MASTER_CALC_Flux

### D014 — Upsert non destructif
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : PK nouvelle → ajout. PK existante + ROW_HASH changé → mise à jour. PK disparue d'un extract → conserver. Jamais de suppression automatique.
S'applique à TOUTES les tables.

### D015 — Ménages internes M04
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : M04 traite UNIQUEMENT les ménages internes. Code impact HC OBLIGATOIRE.
Source officielle = requête Power Query actualisée (pas le CSV export).
Tables : tbl_MASTER_FACT_MEN_Menages (agrégé mensuel) / MASTER_FACT_MEN_Menages (conceptuel granulaire)
⚠ Les deux niveaux coexistent légitimement (Archi §11.4). Ne pas les confondre.

### D016 — Forfait local 50€ dans M04 *(⚠️ RÉVISÉ par D016-REV — voir ci-dessous)*
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : LOCAL_50_INJECTABLE_DANS_M04 = NON par défaut.
Le 50€ reste composante analytique M04 mais n'est PAS injecté dans MASTER_CALC_Flux depuis M04.
Contrôle : LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL si OUI + charge locale déjà dans flux.
Tables : tbl_MASTER_FACT_MEN_Menages, REF_Parametres_Generaux

### D017 — Clôture mensuelle
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Statuts mois = OUVERT / EN_CONTROLE / CLOTURE.
Une ligne LIGNE_BANCAIRE_NON_CLASSEE ouverte = mois non clôturable.
Facturation propriétaire uniquement après clôture validée.
Voir D024 pour la table de stockage.

### D018 — Banque : source vs rapprochement
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Banque = source économique principale SEULEMENT si aucune source métier ne porte déjà le flux.
Payout Airbnb/Booking = rapprochement uniquement.
Règles déterministes avant IA. IA ne valide jamais définitivement une ligne sensible.
Tables : NORM_Banque, MASTER_CALC_Flux

### D019 — VRBO paymentStatus Unknown
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : 32 réservations VRBO en statut Unknown → flag A_CONTROLER.
Montant renseigné manuellement dans MASTER_FACT_MAN_ReservationsHorsHostaway.
Jamais assimilé automatiquement à une réservation directe.
Tables : MASTER_CALC_Reservations

### D020 — Résultat par défaut
Date : antérieur 2026-06-04 | Statut : VALIDÉ
Décision : Vision par défaut = PILOTAGE (réel = IC + HC). Démarrage 2026-03.
Tables : REF_Parametres_Generaux

### D021 — Statuts de payout (valeurs fermées)
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Le statut de calcul payout dans MASTER_CALC_HA_Payout utilise des valeurs fermées.

| statut_calcul_payout | Signification |
|---|---|
| NORMAL | Payout calculé, réservation active |
| ANNULE_SANS_PAYOUT | Annulée, aucun montant |
| ANNULE_AVEC_PAYOUT | Annulée avec indemnité → règle active **D030**, aucun ménage déduit. Contrôle `CANCELLED_AVEC_MONTANT` conservé pour traçabilité. |
| PAYOUT_ABSENT | Réservation active sans payout calculable → BLOQUANT |
| PAYOUT_INCOMPLET | Champs financiers partiels → A_CONTROLER |
| A_CONTROLER | Cas non résolu (VRBO Unknown, direct sans montant) |

Tables : MASTER_CALC_HA_Payout, REF_Setup (onglet REF_Statuts_Payout à créer au Lot 0)

### D022 — REF_Statuts : valeurs de statut_controle fermées
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Le champ statut_controle dans TOUTES les tables de saisie et de calcul utilise UNIQUEMENT ces valeurs.
Aucune valeur libre (OK, Validé, A contrôler, etc.) autorisée dans les fichiers de saisie.

| statut_id | Libellé | Effet |
|---|---|---|
| VALIDE | Validé | Ligne intégrée au calcul |
| A_CONTROLER | À contrôler | Visible en contrôle, intégrée sauf règle contraire |
| BLOQUANT | Bloquant | Exclue du calcul, bloque clôture/facturation |
| IGNORE_JUSTIFIE | Ignoré justifié | Exclue, motif obligatoire |

Onglet REF_Statuts à créer dans REF_Setup.xlsm au Lot 0.
Tables : tous les MASTER_FACT_MAN_*, MASTER_CALC_*, listes déroulantes fichiers saisie

### D023 — Convention d'arrondi et tolérance — **OBSOLÈTE, remplacée par D035**

> Cette décision a été remplacée intégralement par D035 (double seuil 0,10 €/ligne et 1,00 €/cumulé). Ne pas appliquer le contenu de D023. Conservée pour traçabilité historique.

### D024 — Table REF_Cloture_Mensuelle
Date : 2026-06-04 | Statut : VALIDÉ
Décision : La règle de clôture mensuelle (REGLES §11) nécessite une table de stockage dédiée.
Table : REF_Cloture_Mensuelle (PK = mois AAAA-MM)
Colonnes : mois, statut_mois, date_passage_controle, date_cloture, nb_lignes_bancaires_non_classees,
           nb_controles_bloquants_ouverts, commentaire
Règle : passage à CLOTURE seulement si nb_lignes_non_classees=0 ET nb_bloquants_ouverts=0.
À créer au Lot 0 (onglet dans REF_Setup.xlsm ou table CSV dédiée).

### D025 — Frontière Lot 3 / Lot 7 : IK et avantages
Date : 2026-06-04 | Statut : VALIDÉ
Contexte : Risque de double saisie entre Lot 3 (charges) et Lot 7 (IK/avantages).
Décision — frontière stricte :
  MASTER_FACT_MAN_Charges (Lot 3) : charges payées pour la société (perso/liquide/compte pro).
    Inclut les dépenses perso sur compte pro (source avantage brut dérivée, pas ressaisie).
  MASTER_FACT_MAN_IK_Avantages (Lot 7) : UNIQUEMENT les flux non disponibles ailleurs =
    virements associés sans détail, IK en montant direct, avances, corrections.
  ⚠ Ne JAMAIS ressaisir au Lot 7 ce qui existe déjà dans Lot 3 ou Lot 4.
Tables : MASTER_FACT_MAN_Charges, MASTER_FACT_MAN_IK_Avantages, MASTER_CALC_AvantagesAssocies

### D026 — Source unique des achats et charges : `SAISIE_Charges_Flux.xlsx`
Date : 2026-06-04 | Statut : VALIDÉ
Décision : `SAISIE_Charges_Flux.xlsx` est la **source unique** de saisie pour toutes les lignes économiques non portées par Hostaway, les réservations hors Hostaway, ou les IK/avantages associés.
Périmètre inclus : achats, charges, consommables, produits ménage, linge / lavage, matériel, charges payées perso ou liquide, dépenses perso sur compte pro.
Périmètre exclu (interdit dans ce fichier) : IK kilométriques, virements associés, avances associés → restent dans `MASTER_FACT_MAN_IK_Avantages` (Lot 7, D025).
Ce fichier alimente la table normalisée `MASTER_FACT_MAN_Charges`.
Aucun fichier consommables ou achats séparé ne doit exister.
Tables : MASTER_FACT_MAN_Charges, MASTER_CALC_Flux

### D027 — Suppression définitive de la logique `Courses` et `Coût du lavage` dans M04
Date : 2026-06-04 | Statut : VALIDÉ — IRRÉVOCABLE
Décision : M04 (`M04_MENAGES_PowerQuery.xlsx`) ne doit plus contenir :
  - onglet `achats` ou toute source d'achats consommables ;
  - colonne `Coût du lavage` (linge) ;
  - colonne `Courses` ou `heures de courses` ;
  - calcul de TotalCourses ou de Quote-part incluant des achats ;
  - tout montant de consommable, matériel, produit ménage ou linge.
Si des heures de courses doivent être valorisées, elles passent par `SAISIE_Charges_Flux.xlsx` comme charge/flux analytique traçable.
M04 se limite à : main-d'œuvre ménage directe (heures × taux), Rangement (main-d'œuvre), comparaison avec coût standard d'exécution ménage.
Conséquence : le forfait local 50 € quitte aussi M04 (c'est une charge, pas de la main-d'œuvre) → D016 révisé.
Tables : tbl_MASTER_FACT_MEN_Menages, M04_MENAGES_PowerQuery.xlsx, SAISIE_Charges_Flux.xlsx

### D016-REV — Révision D016 (forfait local 50 €)
Date : 2026-06-04 | Statut : VALIDÉ (révision de D016 du 2026-06-04)
Décision : Le forfait local mensuel 50 € **quitte M04**. Il est traité comme une charge dans `SAISIE_Charges_Flux.xlsx` (HC, catégorie FRAIS_LOCAL). Le paramètre LOCAL_50_INJECTABLE_DANS_M04 devient obsolète et est retiré de REF_Parametres_Generaux. Le contrôle LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL est remplacé par ACHATS_DEJA_EN_SAISIE_CHARGES (voir §18).
Tables : REF_Parametres_Generaux, SAISIE_Charges_Flux.xlsx, MASTER_CALC_Flux

### D028 — Coût complet ménage reconstruit hors M04 + `VUE_ACHATS_MENAGE_VALIDES`
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Le coût complet ménage n'est **plus calculé dans M04**.
Il est reconstruit dans le flux analytique global à partir de deux sources :
  1. Coût d'exécution ménage issu de M04 (main-d'œuvre).
  2. Charges validées dans `SAISIE_Charges_Flux.xlsx` affectées au ménage (linge, consommables, produits, matériel, forfait local), via la vue `VUE_ACHATS_MENAGE_VALIDES`.
`VUE_ACHATS_MENAGE_VALIDES` : filtre les lignes de MASTER_FACT_MAN_Charges où `type_charge IN ('LINGE','CONSOMMABLE_MENAGE','PRODUIT_MENAGE','MATERIEL_MENAGE','FRAIS_LOCAL')` ET `statut_controle = VALIDE`. Elle ne remplace pas M04 mais alimente le coût analytique complet hors M04. Aucun double comptage possible (sources distinctes).
Le type_flux_id M04 devient `COUT_EXECUTION_MENAGE_INTERNE` (renommé depuis COUT_REEL_MENAGE_INTERNE).
Tables : tbl_MASTER_FACT_MEN_Menages, MASTER_FACT_MAN_Charges, VUE_ACHATS_MENAGE_VALIDES, MASTER_CALC_Flux

### D029 — Statuts de lots : aucun lot n'est FAIT avant contrôle documenté
Date : 2026-06-04 | Statut : VALIDÉ — IRRÉVOCABLE
Décision : Aucun lot ne peut être marqué `FAIT` tant qu'aucune entrée n'existe dans JOURNAL_CONTROLES pour ce lot.
  Lot 0 → `À PRÉPARER / audit requis / non démarré`
  Lot 1 → `extraction existante éventuelle, non validée sur données réelles`
Tous les fichiers de cadrage doivent refléter ce statut.
Tables : JOURNAL_CONTROLES (registre de référence)

### D030 — Cancellation payout : règle de calcul
Date : 2026-06-04 | Statut : VALIDÉ (clôture de DO-01)
Décision : Pour une réservation annulée avec CancellationPayout > 0 :
  BaseCommission            = CancellationPayout
  CommissionConciergerie    = CancellationPayout × TauxCommission
  NetProprietaire           = CancellationPayout − CommissionConciergerie
Aucun ménage ne doit être déduit (pas de prestation réalisée).
Contrôle : CANCELLED_AVEC_MONTANT passe de A_CONTROLER à règle active ; statut_calcul_payout = ANNULE_AVEC_PAYOUT.
Tables : MASTER_CALC_HA_Payout, MASTER_CALC_Commissions, MASTER_CALC_NetProprietaire

### D031 — `revenu_net_exploitation_proprietaire` (indicateur économique pur)
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Indicateur économique verrouillé. Ne prend JAMAIS en compte : avances, acomptes Airbnb, paiements déjà reçus, montants réglés par le propriétaire, remboursements, régularisations de trésorerie, achats exceptionnels, matériel exceptionnel, charges exceptionnelles non récurrentes, ajustements.
Formule :
  CommissionConciergerie              = (TotalPayout − MenageFacture) × TauxCommission
  revenu_net_exploitation_proprietaire = TotalPayout − MenageFacture − CommissionConciergerie − charge_fixe_mensuelle
charge_fixe_mensuelle = montant facturé chaque mois au propriétaire (forfait logiciel, forfait fixe contractuel uniquement — aucune charge exceptionnelle).
Tables : MASTER_CALC_NetProprietaire, MASTER_CALC_Commissions

### D032 — `acompte_conciergerie_recu_via_airbnb`
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Colonne distincte pour les cas où Airbnb verse un acompte à la conciergerie.
Ce montant ne modifie JAMAIS : payout propriétaire, revenu net d'exploitation, résultat global.
Rôle unique : réduire le reste à payer à la conciergerie.
Tables : MASTER_CALC_NetProprietaire (bloc règlement)

### D033 — Séparation exploitation / règlement : deux blocs distincts et non communicants
Date : 2026-06-04 | Statut : VALIDÉ
Bloc exploitation (performance économique) :
  total_payout, menage_facture, base_commission, taux_commission, commission_conciergerie,
  charge_fixe_mensuelle, revenu_net_exploitation_proprietaire.
  base_commission = total_payout − menage_facture
  commission_conciergerie = base_commission × taux_commission
  revenu_net_exploitation = total_payout − menage_facture − commission_conciergerie − charge_fixe_mensuelle
Bloc règlement / trésorerie :
  montant_du_conciergerie, acompte_conciergerie_recu_via_airbnb, autres_acomptes_conciergerie_recus,
  paiement_deja_recu, reste_a_payer_conciergerie, statut_reglement_conciergerie.
  montant_du_conciergerie = commission_conciergerie + menage_facture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees
  reste_a_payer_conciergerie = montant_du_conciergerie − acompte_conciergerie_recu_via_airbnb − autres_acomptes_conciergerie_recus − paiement_deja_recu
Règle absolue : le bloc règlement ne modifie JAMAIS le bloc exploitation.
Tables : MASTER_CALC_NetProprietaire

### D034 — `charges_exceptionnelles_refacturees` hors revenu net d'exploitation
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Les charges exceptionnelles refacturées au propriétaire modifient le montant_du_conciergerie (et donc le reste_a_payer), mais ne modifient JAMAIS le revenu_net_exploitation_proprietaire.
Elles apparaissent sur la facture propriétaire comme ligne séparée, en dehors du bloc exploitation.
Tables : MASTER_CALC_NetProprietaire, SAISIE_Charges_Flux.xlsx (catégorie CHARGE_EXCEPTIONNELLE_REFACTURABLE)

---

## Historique des décisions ouvertes désormais fermées

> **Aucune décision ouverte bloquante à ce jour.** Toutes les questions initialement ouvertes ont été tranchées et verrouillées (D030, D035, D036). Cette section est conservée pour traçabilité.
>
> - **DO-01 Cancellation payout → FERMÉE, voir D030.**
> - **DO-02 Seuil tolérance arrondi → FERMÉE, voir D035.**
> - **DO-03 Barème IK → FERMÉE, voir D036.**

---

## DÉCISIONS DV1-DV6 VERROUILLÉES

### D035 — Convention d'arrondi et double seuil de tolérance (clôture DO-02)
Date : 2026-06-04 | Statut : VALIDÉ
Décision :
  Calcul : pleine précision disponible, jamais d'arrondi intermédiaire.
  Stockage / affichage : 2 décimales, arrondi demi-vers-le-haut (ROUND).
  Tolérance ligne    : 0,10 € maximum par ligne.
  Tolérance cumulée  : 1,00 € maximum par facture / propriétaire / mois.
  Écart ≤ seuil : acceptable, traçable si nécessaire.
  Écart > seuil ligne   → anomalie `ECART_ARRONDI_LIGNE_SUPERIEUR_TOLERANCE`.
  Écart > seuil cumulé  → anomalie `ECART_ARRONDI_FACTURE_SUPERIEUR_TOLERANCE`.
Paramètres dans REF_Parametres_Generaux :
  ARRONDI_DECIMALES = 2
  TOLERANCE_ARRONDI_LIGNE_EUR = 0.10
  TOLERANCE_ARRONDI_CUMUL_EUR = 1.00
Tables : MASTER_CALC_Commissions, MASTER_CALC_NetProprietaire, MASTER_CTRL_Coherence

### D036 — IK et avantages : saisie en montant direct (clôture DO-03)
Date : 2026-06-04 | Statut : VALIDÉ
Décision : Pas de calcul automatique au barème kilométrique au démarrage. L'utilisateur renseigne directement le montant IK ou avantage. Le barème kilométrique pourra être ajouté plus tard.
`MASTER_FACT_MAN_IK_Avantages` doit au minimum contenir :
  associe_id, mois, type_flux, nature, montant, commentaire, statut_controle,
  impact_resultat_reel, impact_resultat_comptable.
Chaque ligne doit rester traçable avec justificatif si nécessaire.
Tables : MASTER_FACT_MAN_IK_Avantages, MASTER_CALC_AvantagesAssocies

### D037 — REF_Couts_Standards_Menage rebasé sur l'exécution ménage uniquement
Date : 2026-06-04 | Statut : VALIDÉ
Décision : `REF_Couts_Standards_Menage` doit représenter le coût d'exécution (main-d'œuvre) uniquement.
M04 compare son coût d'exécution à ce standard. Le coût complet ménage (exécution + charges) est comparé au standard complet uniquement dans la vue analytique hors M04 (§11.6, D028).
Ne pas comparer un standard complet à un coût M04 limité à l'exécution : l'écart serait faux.
Les valeurs actuelles (Studio 29 €, T2 39 €, T3 55 €, T4 69 €, T6/Duo 110 €) sont à revalider au Lot 0 selon leur périmètre réel (exécution seule ou complet).
Tables : REF_Couts_Standards_Menage, tbl_MASTER_FACT_MEN_Menages

### D038 — Rangement dans M04 : main-d'œuvre opérationnelle uniquement
Date : 2026-06-04 | Statut : VALIDÉ
Décision : `Rangement` reste dans M04 uniquement s'il correspond à du temps de main-d'œuvre opérationnelle lié au ménage.
Si le Rangement inclut : achat, déplacement, linge, matériel, consommables ou coût exceptionnel → sort de M04 et passe par `SAISIE_Charges_Flux.xlsx`.
Règle pratique : si la ligne est saisie en heures travaillées → M04. Si elle est saisie en montant d'achat ou de déplacement → SAISIE_Charges_Flux.
Tables : tbl_MASTER_FACT_MEN_Menages, SAISIE_Charges_Flux.xlsx

### D039 — `charge_fixe_mensuelle` : paramétrable dans le référentiel par propriétaire/logement
Date : 2026-06-04 | Statut : VALIDÉ
Décision : `charge_fixe_mensuelle` est paramétrable par propriétaire/logement dans `REF_Logements` (champ dédié).
Si aucun forfait fixe n'est défini : valeur = 0.
Aucun montant en dur dans les règles de calcul.
Scope : forfait logiciel, forfait consommables récurrent, forfait contractuel fixe uniquement. Jamais une charge exceptionnelle.
Tables : REF_Logements, MASTER_CALC_NetProprietaire

### D040 — Structure de sortie facture propriétaire
Date : 2026-06-04 | Statut : VALIDÉ | Renforcé : 2026-06-05 (P11)
Décision : La facture propriétaire produit les sorties logiques suivantes :
  - **Excel de contrôle propre et exploitable** (par mois / propriétaire / logement) — c'est l'objectif initial verrouillé ;
  - Table `FACT_FACTURE_ENTETE` : identifiants, propriétaire, logement, mois, statut_generation, dates, totaux blocs exploitation et règlement ;
  - Table `FACT_FACTURE_LIGNES` : les lignes de §17.3, avec type_ligne, libellé, montant, bloc (exploitation/règlement) — ordre et comptage révisés par D-PREF-ORDRE-01 (12 lignes sans canapé, 13 avec) ;
  - Champ `statut_generation` : BROUILLON / VALIDE / EMIS / ANNULE.
  - **Aucun PDF propriétaire produit au démarrage.** Les champs et tables sont conçus dès maintenant pour qu'un PDF puisse être généré au Lot 12 sans refactoring — mais la production PDF n'est pas prioritaire et n'est pas un livrable des lots initiaux.
La structure logique des 12 lignes (§17.3) est verrouillée et doit être respectée avant toute mise en forme visuelle.
Tables : FACT_FACTURE_ENTETE, FACT_FACTURE_LIGNES (nouvelles tables, Module 10)

### D041 — Incidents voyageurs (P02)
Date : 2026-06-05 | Statut : VALIDÉ
Décision : **Définition.** Un « incident voyageur » est une situation exceptionnelle liée à un séjour qui nécessite un suivi financier ou opérationnel. Le périmètre couvre :
  - problèmes d'accès au logement ;
  - dégradations constatées ;
  - réclamations du voyageur ;
  - compensations versées au voyageur ;
  - interventions urgentes (serrurier, dépannage, etc.) ;
  - tout problème de séjour générant un coût ou un suivi.
**Traitement.** Les incidents voyageurs sont saisis dans `SAISIE_Charges_Flux.xlsx` avec :
  - `categorie_charge_id = INCIDENT_VOYAGEUR` (nouvelle catégorie) ;
  - `reservation_id` renseigné (lien avec la réservation Hostaway ou hors Hostaway concernée) ;
  - `refacturable` selon la nature (refacturable au propriétaire ou non — décidé ligne par ligne) ;
  - `code_impact` selon les règles standard (`IC` / `HC` / `HR`).
Si `refacturable = OUI`, la ligne alimente `charges_exceptionnelles_refacturees` et suit EP7/D034 (bloc règlement uniquement, jamais bloc exploitation). La `categorie_charge_id = INCIDENT_VOYAGEUR` est **conservée** — elle ne change pas. La ligne reste identifiable comme incident voyageur même quand refacturée.
**Pas de nouvelle table dédiée** au démarrage : `MASTER_FACT_MAN_Charges` (alimentée par `SAISIE_Charges_Flux.xlsx`) suffit avec la nouvelle catégorie et le champ `reservation_id`.
Tables : SAISIE_Charges_Flux.xlsx, MASTER_FACT_MAN_Charges, MASTER_CALC_Flux

### D042 — AirCover et réclamations plateformes (P03)
Date : 2026-06-05 | Statut : VALIDÉ
Décision : Trois flux distincts à ne JAMAIS confondre :

**Flux 1 — Remboursement plateforme perçu par le propriétaire (AirCover ou équivalent).**
  - Versé directement par la plateforme au propriétaire.
  - **Hors comptes de la conciergerie.** N'entre pas dans `MASTER_CALC_Flux`.
  - Tracé en information dans `MASTER_CALC_NetProprietaire` via trois champs séparés : `aircover_recu_par_proprietaire_montant`, `aircover_recu_par_proprietaire_date`, `aircover_recu_par_proprietaire_motif`. Ces champs **ne modifient ni le revenu net d'exploitation ni le règlement conciergerie**.

**Flux 2 — Prestation facturée par la conciergerie suite à l'incident** (gestion de sinistre, intervention, suivi).
  - Saisie dans `SAISIE_Charges_Flux.xlsx` avec catégorie `PRESTATION_AIRCOVER_REFACTUREE`.
  - **Refacturable au propriétaire** → entre dans `charges_exceptionnelles_refacturees` (bloc règlement, D034).
  - Augmente `montant_du_conciergerie` et donc `reste_a_payer_conciergerie`.
  - **N'impacte JAMAIS `revenu_net_exploitation_proprietaire`** (D034).

**Flux 3 — Impact sur le résultat opérationnel de la conciergerie.**
  - Selon le `code_impact` de la ligne saisie (`IC` / `HC` / `HR`).
  - Décidé ligne par ligne lors de la saisie.

Contrôles dédiés : `AIRCOVER_NON_TRACE` (à contrôler), `AIRCOVER_CONFONDU_AVEC_PAYOUT` (bloquant — si un montant AirCover apparaît dans `total_payout`).
Tables : SAISIE_Charges_Flux.xlsx, MASTER_CALC_NetProprietaire, MASTER_CALC_Flux

### D043 — Priorité Excel avant Power BI (P32)
Date : 2026-06-05 | Statut : VALIDÉ
Décision : L'objectif prioritaire est de produire un **Excel propre, automatisé, structuré et fiable**.
**Power BI n'est pas un livrable des lots initiaux.** L'utilisateur réalisera le dashboard Power BI lui-même, plus tard, à partir des tables et CSV produits.
Conséquence pour le cadrage :
  - Les fichiers et tables doivent être **conçus pour être directement exploitables dans Power BI** (schéma en étoile, `MASTER_CALC_Flux` en table de faits, `REF_*` en dimensions).
  - **Aucun lot ne livre un fichier `.pbix`** ni un dashboard Power BI. Le Lot 12 produit Excel, tables et données prêtes pour Power BI — pas le dashboard lui-même.
Tables : aucune nouvelle table — règle de priorité applicable à tous les lots.

### D044 — Séparation statut_controle / niveau_anomalie (DM-L3-01)
Date : 2026-06-08 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  `statut_controle` (état de la ligne) : `VALIDE` / `A_CONTROLER` / `EXCLU_RESULTAT` / `A_VENTILER`
  `niveau_anomalie` (sévérité de l'anomalie) : `INFO` / `A_CONTROLER` / `BLOQUANT`
  `code_anomalie` : code technique du contrôle détecté (ex. `CHARGE_LOGEMENT_SANS_LOGEMENT_ID`)
  `BLOQUANT` n'est plus un statut de ligne métier — c'est un niveau d'anomalie.
  `EXCLU_RESULTAT` remplace `IGNORE_JUSTIFIE` pour les tables Lot 3+.
  `IGNORE_JUSTIFIE` reste valide pour Lots 0-2 (compatibilité ascendante).
REF_Statuts :
  - STAT_022 (BLOQUANT / statut_controle) → désactivé
  - STAT_024 (A_CONTROLER / statut_controle) + STAT_025 (EXCLU_RESULTAT) + STAT_026 (A_VENTILER) → ajoutés
  - STAT_027 (INFO / niveau_anomalie) + STAT_028 (A_CONTROLER) + STAT_029 (BLOQUANT) → ajoutés
Périmètre : SAISIE_Charges_Flux.xlsx et toutes tables MASTER_FACT_MAN_* du Lot 3+.
Tables : REF_Statuts, SAISIE_Charges_Flux.xlsx, MASTER_FACT_MAN_Charges

### D045 — REF_Charges_Recurrentes : table des montants récurrents paramétrables
Date : 2026-06-08 | Statut : VALIDÉ — VERROUILLÉ
Décision : Les charges récurrentes (forfaits, loyers, abonnements) sont portées par `REF_Charges_Recurrentes`
  (nouvel onglet REF_Setup). Aucun montant fixe ne peut être codé en dur dans les formules Excel,
  Power Query, descriptions de catégories ou scripts.
  Colonnes clés : `charge_recurrente_id`, `montant_ttc`, `periodicite`, `cle_repartition`,
  `date_debut_validite`, `date_fin_validite`.
  Premières entrées : REC_001 (Forfait client, CHG_016, TYPE_FLUX_012, IC, refacturable=OUI) ;
  REC_002 (Forfait local cave, CHG_023, TYPE_FLUX_010, HC, clé=NOMBRE_MENAGES, 50 €).
Tables : REF_Charges_Recurrentes (REF_Setup.xlsm)

### D046 — canal_id obligatoire dans SAISIE_ReservationsHorsHostaway (QM-L4-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `canal_id` est obligatoire dans la table `MASTER_FACT_MAN_ReservationsHorsHostaway`.
Source : `REF_Canaux_Reservation`. Objectif : distinguer DIRECT, VRBO, Autre hors Hostaway
sans déduire le canal depuis un commentaire libre.
Contrôle associé : `RESH_CANAL_MANQUANT` (BLOQUANT).
Tables : SAISIE_ReservationsHorsHostaway.xlsx, MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx

### D047 ? taux_commission depuis r?f?rentiel dat? unique (QM-L4-02)
Date : 2026-06-09 | Statut : REMPLAC? PAR SOURCE UNIQUE 2026-06-28
D?cision : `taux_commission` est r?solu uniquement depuis `REF_Taux_Commission`, selon propri?taire/logement et date de r?servation.
  - Si un taux unique est applicable ? `taux_commission_source = REF_Taux_Commission`.
  - Si aucun taux dat? n'est applicable ? contr?le BLOQUANT, aucune facture finale.
  - Si plusieurs taux sont applicables ? contr?le BLOQUANT, aucune facture finale.
Aucun taux non dat?, manuel, devin? ou cod? en dur ne peut alimenter un calcul r?el.
Tables : REF_Taux_Commission (REF_Setup.xlsm), MASTER_CALC_Reservations.xlsx

### D048 — VRBO Unknown dans SAISIE_ReservationsHorsHostaway (QM-L4-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Les 32 réservations VRBO `paymentStatus = Unknown` (ANO-004) sont saisies dans
`SAISIE_ReservationsHorsHostaway.xlsx`, pas dans un fichier séparé.
  - `canal_id = CANAL_003` (VRBO)
  - `source_financiere = VRBO_UNKNOWN`
  - `reservation_id_hostaway` renseigné si l'ID Hostaway existe (obligatoire — D049)
  - La vérité financière vient de la saisie Lot 4, jamais du montant Hostaway.
  - Si `total_percu` vide → `A_CONTROLER` (pas BLOQUANT). Si renseigné → ligne valide.
Tables : SAISIE_ReservationsHorsHostaway.xlsx, MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx

### D049 — Séparation réservation / charge, lien via champ Lot 3 existant (QM-L4-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Dans `SAISIE_ReservationsHorsHostaway.xlsx`, on saisit uniquement la réservation,
l'encaissement, le montant récupéré, l'associé récupérateur, le montant reversé propriétaire
et l'acompte facture.
Si une charge est payée avec le montant récupéré, elle est saisie dans `SAISIE_Charges_Flux.xlsx`
(Lot 3 — champ `paye_avec_montant_recupere` = `reservation_hh_id`).
Ce champ existe déjà dans Lot 3. Aucune modification de `SAISIE_Charges_Flux.xlsx` au Lot 4.
Tables : SAISIE_ReservationsHorsHostaway.xlsx, SAISIE_Charges_Flux.xlsx (lien, non modifié)

### D050 — Nomenclature PK réservations hors Hostaway (QM-L4-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : PK = `RESHH-AAAA-MM-NNN`. Compteur NNN repart à `001` chaque mois.
  Exemple : RESHH-2026-05-001 / RESHH-2026-05-002 / RESHH-2026-06-001.
  La clé est saisie comme valeur figée (pas de formule volatile). Elle ne doit jamais être
  régénérée automatiquement après la saisie initiale.
Tables : SAISIE_ReservationsHorsHostaway.xlsx, MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx

### D051 — Chemins officiels Lot 4 (QM-L4-06)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  Source saisie  : `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`
  Master transformé : `02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`
Tables : SAISIE_ReservationsHorsHostaway.xlsx, MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx

### D052 — Mapping logement_id depuis listingMapId pour branche HA (QM-L4b-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
D?cision : Pour la branche HA, `logement_id` est obtenu via JOIN sur `REF_Mapping_Logements` (colonne `listingMapId`), actifs uniquement. `proprietaire_id` est r?solu uniquement depuis `REF_Gestion_Logements_Hist` selon `logement_id` et dates de s?jour. Pour la branche HH, `logement_id` vient de la saisie et `proprietaire_id` est ?galement r?solu depuis `REF_Gestion_Logements_Hist`.
  Contrôle `RESERVATION_LOGEMENT_NON_MAPPE` (A_CONTROLER) : `listingMapId` absent de `REF_Mapping_Logements` actifs.
  Contrôle `RESERVATION_MAPPING_MULTIPLE` (A_CONTROLER) : plusieurs lignes actives pour un même `listingMapId` dans `REF_Mapping_Logements`.
Tables : MASTER_CALC_Reservations, REF_Mapping_Logements, REF_Gestion_Logements_Hist

### D053 — Structure 24 colonnes et valeurs fermées source / source_montant (QM-L4b-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `MASTER_CALC_Reservations` contient 24 colonnes (22 base + `niveau_anomalie` + `code_anomalie` en bloc statut, après `statut_controle` et avant `commentaire`).
  `source` fermé — 7 valeurs : HOSTAWAY_AIRBNB / HOSTAWAY_BOOKING / HOSTAWAY_DIRECT_HH / HOSTAWAY_VRBO_HH / HOSTAWAY_VRBO_A_CONTROLER / MANUEL_HORS_HOSTAWAY / OWNERSTAY_EXCLU.
  `source_montant` fermé — 5 valeurs : HOSTAWAY_PAYOUT / MANUEL_HH / MANUEL_VRBO / NON_CONCERNE / A_CONTROLER.
  `impact_resultat_reel` calculé : IC/HC→OUI, HR→NON, vide→A_CONTROLER.
  `impact_resultat_comptable` calculé : IC→OUI, HC/HR→NON, vide→A_CONTROLER.
  Contrôle `RESERVATION_HH_NON_VALIDE` (A_CONTROLER) : ligne HH utilisée dans table commune avec `statut_controle ≠ VALIDE`.
Tables : MASTER_CALC_Reservations

### D054 — Logique anti-double-comptage et scénarios (QM-L4b-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : 7 scénarios anti-double-comptage. Règle centrale : si `reservation_id` HA existe dans `reservation_id_hostaway` HH → ligne HA exclue avant empilement, représentée uniquement par la ligne HH.
  S1 Airbnb HA pur → HOSTAWAY_AIRBNB, HOSTAWAY_PAYOUT, IC, VALIDE.
  S2 Booking HA pur → HOSTAWAY_BOOKING, HOSTAWAY_PAYOUT, IC, VALIDE.
  S3 Direct HA + HH liée → HOSTAWAY_DIRECT_HH, MANUEL_HH, code_impact HH, statut HH — ligne HA exclue.
  S4 VRBO HA + HH renseignée → HOSTAWAY_VRBO_HH, MANUEL_VRBO, HC, statut HH — ligne HA exclue.
  S5 VRBO HA sans HH → HOSTAWAY_VRBO_A_CONTROLER, A_CONTROLER, montant null, HC, A_CONTROLER.
  S6 HH pure → MANUEL_HORS_HOSTAWAY, MANUEL_HH, code_impact HH.
  S7 OwnerStay → OWNERSTAY_EXCLU, NON_CONCERNE, montant 0, HR, EXCLU_RESULTAT.
  Contrôle BLOQUANT `RESERVATION_DOUBLON_HOSTAWAY_HH` : `reservation_id_hostaway` **non vide** ET rattaché à 2+ lignes actives (`statut_controle ≠ EXCLU_RESULTAT`) dans la table commune.
  Contrôle BLOQUANT `RESERVATION_CALC_ID_DUPLIQUE` : `reservation_calc_id` dupliqué.
Tables : MASTER_CALC_Reservations

### D055 — Champ mois : TEXT YYYY-MM depuis checkInDate (QM-L4b-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `mois` dans `MASTER_CALC_Reservations` est calculé en Power Query depuis `checkInDate` (branche HA) : `Date.ToText([checkInDate], "yyyy-MM")`. Pour la branche HH, `mois` vient directement de la colonne `mois` de `SAISIE_ReservationsHorsHostaway.xlsx`.
Tables : MASTER_CALC_Reservations

### D056 — Nomenclature PK MASTER_CALC_Reservations (QM-L4b-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : PK = `RES-AAAA-MM-HA-NNN` (branche HA) ou `RES-AAAA-MM-HH-NNN` (branche HH). Compteur NNN repart à `001` chaque mois, par branche. Généré en Power Query via `Table.Group + Table.AddIndexColumn`. Contrôle `RESERVATION_CALC_ID_DUPLIQUE` (BLOQUANT) détecte tout doublon post-empilement.
Tables : MASTER_CALC_Reservations

### D057 — Chemins officiels Lot 4bis (QM-L4b-06)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  Script    : `02_TRAVAIL/lot4bis_master_calc_reservations.py`
  Master    : `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
  Onglets   : MASTER (24 cols) / VUE_FLUX (filtre VALIDE+OUI+montant≠0) / POWER_QUERY_CODE (7 requêtes)
  Sources   : HA → MASTER_FACT_HA_Reservations.xlsx + MASTER_CALC_HA_Payout.xlsx (Lot 1)
              HH → SAISIE_ReservationsHorsHostaway.xlsx (Lot 4, lu directement)
              REF → REF_Setup.xlsm (REF_Mapping_Logements + REF_Logements)
  Ne pas toucher : REF_Setup.xlsm / Lots 1, 3, 4
Tables : MASTER_CALC_Reservations

### D058 — Périmètre sources acomptes Lot 5 (QM-L5-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Le Lot 5 couvre trois sources d'acomptes propriétaires : (1) acomptes issus des réservations hors Hostaway (`acompte_facture` du Lot 4) ; (2) virements directs propriétaires ; (3) acomptes manuels ou exceptionnels sans réservation HH.
  `source_acompte` fermé — 3 valeurs : `HH_RESERVATION` / `VIREMENT_DIRECT` / `AUTRE`.
  Toutes les lignes passent par `SAISIE_AcomptesProprietaires.xlsx`. La validation croisée HH est effectuée en PQ (non en saisie brute).
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D059 — Format facture_ref et règle de rattachement (QM-L5-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `facture_ref` est une référence provisoire de rattachement, format `FAC-AAAA-MM-PROP-NNN`. Obligatoire pour les lignes `VALIDE`. Absente → contrôle `ACOMPTE_NON_RATTACHE_FACTURE` (BLOQUANT). La vraie facture est créée au Lot 12 ; `facture_ref` peut être remplacée ou rapprochée à ce moment.
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D060 — Granularité et structure 22 colonnes (QM-L5-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Granularité `proprietaire_id + logement_id + mois + facture_ref + source_acompte`. Une ligne par acompte ; pas de regroupement au niveau propriétaire seul. Structure finale : 18 colonnes SAISIE + 4 colonnes PQ = 22 colonnes MASTER.
  Blocs : identification (2) / rattachement (6) / financier (2) / mode (1) / impact (3) / statut (4) / système PQ (4).
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D061 — Suppression report_mois_suivant — Lot 5 = table de faits pure (QM-L5-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `report_mois_suivant` supprimé du Lot 5. Le montant réel de la facture n'est pas connu au Lot 5 ; le calcul du report définitif est différé au Lot 10/12. `report_mois_precedent` est conservé comme information non liquidée : saisi manuellement, non calculé, contrôlé si renseigné sans commentaire (`ACOMPTE_REPORT_INCOHERENT` A_CONTROLER).
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D062 — Structure fichiers Lot 5 (QM-L5-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  Script   : `02_TRAVAIL/lot5_master_acomptes_proprietaires.py`
  SAISIE   : `01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx` (18 cols, 4 onglets : SAISIE / REF_LOCALE / CONTROLES_SAISIE / README)
  MASTER   : `02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx` (22 cols, 3 onglets : MASTER / VUE_ACTIVE / POWER_QUERY_CODE)
  Type de flux : `TYPE_FLUX_006 = ACOMPTE_FACTURE_PROPRIETAIRE` (déjà présent dans REF_Setup.xlsm — aucune modification REF_Setup nécessaire)
  Sources PQ : SAISIE_AcomptesProprietaires + MASTER_FACT_MAN_ReservationsHorsHostaway (ref croisée HH)
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D063 — Nomenclature PK acompte_id (QM-L5-06)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : PK = `ACC-AAAA-MM-NNN`. Compteur reset à `001` chaque mois. Clé stable — jamais régénérée automatiquement. Saisie manuelle par l'utilisateur dans `SAISIE_AcomptesProprietaires.xlsx`. Contrôle `ACOMPTE_CALC_ID_DUPLIQUE` (BLOQUANT) détecte tout doublon.
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D064 — Séparation source_pk / source_hh_id (QM-L5-07)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Deux colonnes distinctes, deux rôles non interchangeables.
  `source_table` = `SAISIE_AcomptesProprietaires` (toujours — unique source du MASTER Lot 5).
  `source_pk`    = `acompte_id` (toujours — trace la ligne d'acompte).
  `source_hh_id` = `reservation_hh_id` si `source_acompte = HH_RESERVATION` / `null` sinon (lien métier vers Lot 4).
  Contrôle `ACOMPTE_SOURCE_HH_INTROUVABLE` (A_CONTROLER) : `source_hh_id` renseigné mais absent de `MASTER_FACT_MAN_ReservationsHorsHostaway` VALIDE + acompte_facture > 0.
  Contrôle `ACOMPTE_HH_INCOHERENT` (BLOQUANT) : `source_acompte = HH_RESERVATION` ET `montant_acompte ≠ acompte_facture` dans MASTER HH.
Tables : SAISIE_AcomptesProprietaires, MASTER_FACT_MAN_AcomptesProprietaires

### D065 — API Hostaway /v1/tasks : extraction segmentée par listingMapId (QM-L6a-API)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : L'API Hostaway `/v1/tasks` retourne 500 tâches maximum par appel. Le paramètre `offset` est ignoré (réponse identique pour offset=0 et offset=100). Le paramètre `dateFrom+dateTo` est aussi ignoré (toujours 500 résultats indifférents de la plage). Seul le filtre `listingMapId=XXX` est efficace : il retourne uniquement les tâches du listing demandé.
  Méthode fiable : un appel par listing connu dans REF_Logements (actifs + inactifs). Déduplication par `task_id` après agrégation. Contrôle anti-plafond obligatoire : si un segment retourne >= 500 → BLOQUANT.
  Un seul appel global sans filtre est insuffisant si le résultat = 500 (troncature probable). Ne jamais utiliser le single call comme seule méthode.
  Résultat 2026-06-09 : 17 requêtes (1/listing), 0 segment plafonné, 500 tâches uniques, 0 doublon.
Tables : MASTER_FACT_HA_CleaningTasks_Discovery (data)

### D066 — Janvier 2026 absent des CleaningTasks (QM-L6a-Jan)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Aucune tâche retournée pour 2026-01. Normal : la SAS est nouvelle, les opérations ont démarré en février 2026. Aucun contrôle BLOQUANT sur l'absence de janvier.
Tables : MASTER_FACT_HA_CleaningTasks_Discovery (VUE_COMPTAGE)

### D067 — statut_menage : confirmed ≠ réalisé (QM-L6a-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Mapping des statuts Hostaway → statut_menage interne.
  `completed` → `réalisé` ; `confirmed` → `prévu` ; `pending` → `A_CONTROLER` ; `cancelled` → `annulé`.
  Tâche confirmed = planifiée mais pas encore exécutée : non comptée comme ménage réalisé.
Tables : MASTER_FACT_HA_CleaningTasks_Discovery (MASTER_ENRICHI)

### D068 — Logement inactif : A_CONTROLER, pas BLOQUANT (QM-L6a-inactif)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : LOG_0003 (listingMapId=485104) a `actif='NON'` dans REF_Logements mais possède 22 tâches historiques. Ces tâches sont mappées (logement_id connu) et flaggées `TASK_LOGEMENT_INACTIF` (A_CONTROLER). Non BLOQUANT : l'identité du logement est connue, le ménage a eu lieu. Contrôle différent de TASK_LOGEMENT_ABSENT (listingMapId inconnu → BLOQUANT).
Tables : MASTER_FACT_HA_CleaningTasks_Discovery (MASTER_ENRICHI)

### D069 — type_ligne_menage default = TLM_001 (QM-L6a-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Toutes les tâches Hostaway ont titre "Ménage XXX" → classées TLM_001 MENAGE_STANDARD par défaut. `compte_comme_menage = OUI`. Correction manuelle possible pour cas REMISE_EN_ETAT ou autres. Coût = NULL partout (H6 irrévocable).
Tables : MASTER_FACT_HA_CleaningTasks_Discovery (MASTER_ENRICHI)

### D070 — Source données M04 : squelette Power Query + collage GSheet (QM-L6b-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : M04 est construit à partir d'un squelette Excel + Power Query. L'utilisateur colle les données du Google Sheet `Suivi ménage` dans l'onglet `SOURCE_RAW`. Le script Python crée le squelette ; aucune extraction automatique depuis le GSheet. Colonnes SOURCE_RAW attendues : `mois_saisie | appartement | intervenant | type_menage | nb_menages | nb_heures | commentaire`. Les noms doivent correspondre exactement aux en-têtes du GSheet réel.
Tables : M04_MENAGES_PowerQuery.xlsx (SOURCE_RAW)

### D071 — Intervenants EXTERNE dans M04 : importés, flaggés, hors VUE_ACTIVE (QM-L6b-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Les intervenants EXTERNE présents dans le GSheet `Suivi ménage` sont importés dans le MASTER M04 avec contrôle `MENAGE_EXTERNE_DANS_M04` (A_CONTROLER). Ils sont exclus de `VUE_ACTIVE` (filtre type_intervenant = INTERNE). Objectif : traçabilité complète, pas d'exclusion silencieuse. Les ménages EXTERNE sont traités dans le Lot 6c (factures PDF) — jamais valorisés dans M04.
Tables : M04_MENAGES_PowerQuery.xlsx (MASTER, VUE_ACTIVE)

### D072 — Rangement dans M04 : importé si MO pure, contrôle A_CONTROLER (QM-L6b-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Les lignes de type `RANGEMENT` sont importées dans M04 si elles correspondent à de la main-d'œuvre pure (heures × taux). Elles déclenchent le contrôle `MENAGE_RANGEMENT_A_CONTROLER` (A_CONTROLER) pour vérification humaine. Si une ligne Rangement inclut du matériel ou des achats, elle doit être ventilée vers `SAISIE_Charges_Flux.xlsx`.
Tables : M04_MENAGES_PowerQuery.xlsx (MASTER)

### D073 — VUE_ECART_HOSTAWAY dans M04 (QM-L6b-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : L'onglet `VUE_ECART_HOSTAWAY` est intégré dans M04. Il compare `nb_menages_m04` (M04 VALIDE INTERNE, par mois × logement) vs `nb_menages_realises` (Lot 6a `VUE_COMPTAGE`). Contrôle `MENAGE_ECART_HOSTAWAY_M04` (A_CONTROLER) si écart ≠ 0. Implémenté via Power Query Q9 — jointure full outer mois × logement_id.
Tables : M04_MENAGES_PowerQuery.xlsx (VUE_ECART_HOSTAWAY), MASTER_FACT_HA_CleaningTasks_Discovery (VUE_COMPTAGE)

### D074 — Seuil écart main-d'œuvre vs standard : 10 €, paramétrable (QM-L6b-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Seuil initial = 10 €/ménage. Stocké dans `PARAMETRES_M04` (colonne `SEUIL_ECART_STANDARD_MENAGE`). Power Query lit ce seuil dynamiquement — aucune valeur codée en dur. Déclencheur : `ecart_main_oeuvre_vs_standard < −SEUIL` → `MENAGE_ECART_NEGATIF_IMPORTANT` (A_CONTROLER). Rappel sens : `ecart = cout_standard − cout_execution_unitaire` (positif = MO favorable).
Tables : M04_MENAGES_PowerQuery.xlsx (PARAMETRES_M04, MASTER)

### D075 — Taux horaire par intervenant : PARAM_TAUX_INTERVENANTS (non global)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Le taux horaire n'est pas un paramètre global unique. Il est défini par intervenant dans la table `PARAM_TAUX_INTERVENANTS` (8 colonnes : `intervenant_id | nom_intervenant | type_intervenant | taux_horaire | date_debut_validite | date_fin_validite | actif | commentaire`). Power Query récupère le taux actif pour la période via jointure sur `intervenant_id` et plage de dates. Valeurs initiales : INT_0001 Imène 10 €/h, INT_0002 Kheira 10 €/h. Intervenants EXTERNE présents dans la table (taux null — non requis dans M04). Aucune valeur codée en dur.
  Contrôle `TAUX_ABSENT_INTERVENANT_INTERNE` (A_CONTROLER) : 0 taux actif pour l'intervenant à la période.
  Contrôle `TAUX_MULTIPLE_INTERVENANT` (A_CONTROLER) : ≥2 taux actifs pour le même intervenant à la même période.
Tables : M04_MENAGES_PowerQuery.xlsx (PARAM_TAUX_INTERVENANTS)

### D076 — Clé de répartition des charges affectables = COUT_STANDARD_MENAGES_MOIS
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : La clé de répartition des charges affectables aux ménages (consommables, produits, location cave, etc.) est `COUT_STANDARD_MENAGES_MOIS` et non `NOMBRE_MENAGES`. Formule : `quote_part_charge = charge_affectable × (cout_standard_total_ligne / Σ cout_standard_total_ligne du périmètre)`. Cette répartition s'effectue dans une couche de coût complet ultérieure — pas dans M04.
  `cout_standard_total_ligne = nb_menages × cout_standard` : colonne M04, base de pondération uniquement. Non comptable. Non injectée dans `MASTER_CALC_Flux`.
  Périmètres possibles : mois | logement | intervenant | prestataire | tous ménages du mois selon nature de la charge.
  ✓ Révise D045 pour REC_002 : la `cle_repartition` de REC_002 (Forfait local cave, TYPE_FLUX_010) a été mise à jour de `NOMBRE_MENAGES` vers `COUT_STANDARD_MENAGES_MOIS` dans `REF_Charges_Recurrentes` (REF_Setup.xlsm) le 2026-06-09 — validation humaine Lot 6b.
Tables : M04_MENAGES_PowerQuery.xlsx (MASTER — colonne cout_standard_total_ligne), SAISIE_Charges_Flux.xlsx, REF_Charges_Recurrentes

### D077 — TYPE_FLUX_013 = COUT_MO_INTERNE_MENAGE (révision D028 sur le type_flux_id)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Le type flux M04 est `TYPE_FLUX_013 = COUT_MO_INTERNE_MENAGE`. Révise D028 qui mentionnait `COUT_EXECUTION_MENAGE_INTERNE` comme libellé (ce libellé était provisoire). Clé technique : `TYPE_FLUX_013`. Colonnes fixes dans MASTER M04 : `type_flux_id = TYPE_FLUX_013 | sens = CHARGE | code_impact = HC | impact_resultat_reel = OUI | impact_resultat_comptable = NON`. TYPE_FLUX_013 intégrera `MASTER_CALC_Flux` au Lot 9 — MO interne ménage contribue au résultat réel HC.
  Ajouté dans `REF_Types_Flux` (REF_Setup.xlsm) le 2026-06-09 — backup : `99_ARCHIVES/LOT6B_Menages/REF_Setup_BACKUP_20260609_152826.xlsm`.
Tables : REF_Types_Flux (REF_Setup.xlsm), M04_MENAGES_PowerQuery.xlsx (MASTER), MASTER_CALC_Flux

### D078 — Structure et chemins officiels Lot 6b
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  Script : `02_TRAVAIL/lot6b_m04_menages_internes.py`
  M04    : `02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx`
  Onglets (8) : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 / MASTER (34 cols) / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE (10 requêtes) / README
  MASTER (34 cols) : 2 IDENT + 8 RATT + 2 INTERV + 10 CALCUL + 5 FLUX + 3 STATUT + 4 SYSTEME
  Tables PQ (10) : Q1_SOURCE_RAW / Q2_PARAM_TAUX / Q3_PARAMETRES_M04 / Q4_REF_MAPPING / Q5_REF_LOGEMENTS / Q6_REF_COUTS_STANDARDS / Q6B_REF_INTERVENANTS / Q7_MASTER / Q8_VUE_ACTIVE / Q9_VUE_ECART_HOSTAWAY
  Adaptation requise : remplacer `C:\CHEMIN_A_ADAPTER\` dans PQ (Q4/Q5/Q6/Q6B/Q9) par chemins absolus locaux.
  source_table = SOURCE_RAW (toujours) — source_pk = menage_calc_id (toujours, pas de clé GSheet fiable).
Tables : M04_MENAGES_PowerQuery.xlsx, lot6b_m04_menages_internes.py

---

## Lot 6c — Ménages externes (D079–D088)

### D079 — Format réel des factures PDF prestataires confirmé (D-6c-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : 2 factures PDF de mai 2026 fournies et analysées : Aissata (Kandia DIABATE / Rends-moi un service, n°2026-37, 1 439 € TTC) et Mounir (MH Entreprise, n°0003, 942 € TTC). Les deux prestataires sont en franchise TVA (art.293B CGI). Format extraction structurée validé. Pipeline d'extraction IA non nécessaire pour les données mai 2026 — peuplement direct SOURCE_RAW depuis analyse visuelle PDF.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx (SOURCE_RAW)

### D080 — Architecture fichier unique master Lot 6c (D-6c-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Option B retenue — fichier unique MASTER_FACT_MEN_MenagesExternes.xlsx (7 onglets : SOURCE_RAW / PARAMETRES / MASTER / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE / README). Pas de fichier SAISIE séparé. Source = extraction IA depuis PDF ou collage structuré. Chemin : `02_TRAVAIL/Lot6c_MenagesExternes/`. Script : `02_TRAVAIL/lot6c_menages_externes.py`.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx

### D081 — Code impact selon mode de paiement facture externe (D-6c-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision :
  Cas 1 (normal) : code_impact=IC, prise_en_compta=OUI, impact_reel=OUI, impact_compta=OUI.
  Cas 2 (liquide/perso) : code_impact=HC, prise_en_compta=NON, mode_paiement=LIQUIDE/PERSO, associe_payeur obligatoire — contrôle MENAGE_EXTERNE_PAIEMENT_PERSO_SANS_ASSOCIE si absent.
  Cas 3 (hors résultat) : code_impact=HR, justification obligatoire.
  Anti-double-comptage : facture saisie en Lot 6c ≠ charge en SAISIE_Charges_Flux.xlsx.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx (MASTER — col code_impact, mode_paiement, associe_payeur)

### D082 — Facture globale sans détail logement = BLOQUANT (D-6c-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Option C retenue — une facture sans détail par logement est BLOQUANTE. Pas de répartition proportionnelle automatique depuis Hostaway. Pas de APPARTEMENT_DIVERS par défaut. Contrôle : MENAGE_EXTERNE_FACTURE_GLOBALE_NON_DETAILLEE (BLOQUANT).
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx

### D083 — TVA prestataires — 3 valeurs (D-6c-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Colonne regime_tva_prestataire : FRANCHISE_TVA / ASSUJETTI_TVA / A_CONTROLER. Si FRANCHISE_TVA : taux_tva=0, montant_tva=0, montant_ht=montant_ttc. Si ASSUJETTI_TVA : récupérer taux, montant_ht, montant_tva. Contrôle MENAGE_EXTERNE_TVA_A_CONTROLER si A_CONTROLER. Confirmé mai 2026 : Aissata = FRANCHISE_TVA, Mounir = FRANCHISE_TVA.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx (colonnes taux_tva, montant_tva, regime_tva_prestataire)

### D084 — Fournitures incluses dans la prestation : lignes séparées (D-6c-06)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Si la facture prestataire détaille des fournitures, linge, produits, frais annexes — les lignes doivent être séparées dans Lot 6c avec type_ligne_menage_id : TLM_001 (ménage standard) / TLM_002 (remise en état) / TLM_003 (déplacement) / TLM_004 (linge) / TLM_005 (achat/produit) / TLM_006 (autre). Ce qui est sur la facture prestataire ≠ SAISIE_Charges_Flux (sources exclusives). Contrôle MENAGE_EXTERNE_FOURNITURE_A_RECLASSER pour TLM_004/005.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx (col type_ligne_menage_id, fournitures_incluses)

### D085 — Chemin officiel Lot 6c (D-6c-07)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Chemin master = `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`. Script = `02_TRAVAIL/lot6c_menages_externes.py`. PDFs sources = `01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/`.
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx

### D086 — Identité légale des prestataires externes (D-6c-08)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Kandia DIABATE / Rends-moi un service (SIRET 10147251200017) = INT_0004 (Aissata). MH Entreprise (RCS 792015919) = INT_0003 (Mounir). REF_Intervenants mis à jour avec 3 nouvelles colonnes : nom_legal, siret_rcs, email_facturation. nom_intervenant non modifié. Backup : `99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm`.
Tables : REF_Intervenants (REF_Setup.xlsm — +3 colonnes INT_0003/INT_0004)

### D087 — Absence de date_menage individuelle : Option B (D-6c-09)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Option B — date_menage=null si absente sur facture. mois déduit depuis date_facture. precision_date_menage=MOIS_FACTURE. statut_controle=A_CONTROLER. code_anomalie=MENAGE_EXTERNE_DATE_ABSENTE. Interdiction absolue : ne jamais inventer une date, ne jamais compléter depuis Hostaway ou CleaningTasks. Cas mai 2026 : 5 lignes Aissata + 4 lignes Mounir sans date précise → A_CONTROLER. 4 lignes Aissata avec date précise → VALIDE. Ligne 8 originale splittée en 8a/8b (2 dates distinctes sur 1 ligne facture).
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx (col date_menage, precision_date_menage)

### D088 — Ligne 0€ / 0 ménage : conservation MASTER, exclusion VUE_ACTIVE (D-6c-10)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Ligne Mounir T2-65 (Gabriel) à 0 ménage / 0€ (logement inactif depuis 2026-04-26) conservée dans MASTER pour traçabilité. statut_controle=A_CONTROLER, code_anomalie=MENAGE_EXTERNE_MONTANT_NUL + MENAGE_EXTERNE_LOGEMENT_INACTIF. Exclue de VUE_ACTIVE via filtre montant_ligne_ttc>0. Règle générale : 0€ = A_CONTROLER traçable ; montant<0 = BLOQUANT (MENAGE_EXTERNE_MONTANT_INVALIDE).
Tables : MASTER_FACT_MEN_MenagesExternes.xlsx

---

### D089 — TYPE_FLUX_015 = INDEMNITE_KILOMETRIQUE (D-7-01)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Créer TYPE_FLUX_015 = INDEMNITE_KILOMETRIQUE dans REF_Types_Flux. IK ≠ virement associé sur le plan juridique et comptable. Doit être identifiable séparément dans les avantages, avec justificatif ou commentaire obligatoire. code_impact_defaut=IC, avantage_brut_defaut=OUI, deduit_avantage_defaut=NON, comptabilisable_defaut=OUI.
Tables : REF_Setup.xlsm (REF_Types_Flux), MASTER_FACT_MAN_IK_Avantages.xlsx

### D090 — Structure fichier IK & Avantages : 1 fichier 6 onglets (D-7-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Un seul fichier MASTER_FACT_MAN_IK_Avantages.xlsx avec 6 onglets : SOURCE_SAISIE / PARAMETRES / MASTER_SAISIE / MASTER_CALC_AVANTAGES / POWER_QUERY_CODE / README. Cohérent avec Lots 5 et 6c. PQ peut joindre les sources internes.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx

### D091 — MASTER_CALC_AVANTAGES multi-mois, granularité mois × associe_id (D-7-03)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : La table calculée est multi-mois dès la construction. Une ligne par combinaison mois × associe_id. Structure stable, évite toute reconstruction mensuelle.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx (onglet MASTER_CALC_AVANTAGES)

### D092 — Structure vide, aucune donnée fictive (D-7-04)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Le fichier Lot 7 est construit avec structure vide. Aucune donnée saisie de mai 2026 (banque non traitée, montants virements non vérifiés). Les données seront saisies via SOURCE_SAISIE au fur et à mesure. Les virements associés seront rapprochés sans ressaisie au Lot 8.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx

### D093 — montant_recupere HH dérivé via reservation_hh_id, jamais ressaisi (D-7-05)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Le montant_recupere de MASTER_FACT_MAN_ReservationsHorsHostaway est dérivé dans MASTER_CALC_AVANTAGES par référence FK reservation_hh_id. Jamais copie de montant, jamais ressaisi dans MASTER_SAISIE. Contrôle bloquant MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES si montant_recupere > 0 non reflété.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx, MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx

### D094 — Format avantage_id : préfixe VIR / IK / REM / AJU (D-7-06)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Format {PREFIXE}-AAAA-MM-{ASSOCIE_ID}-{NNN}. Préfixes : VIR (TYPE_FLUX_001), IK (TYPE_FLUX_015), REM (TYPE_FLUX_005), AJU (ajustements/avances/corrections). Uniformité PK, sémantique portée par type_flux_id.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx (onglet MASTER_SAISIE, colonne avantage_id)

### D095 — Avances via TYPE_FLUX_001 + nature="AVANCE" (D-7-07)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : Les avances associés sont captées via TYPE_FLUX_001 = VIREMENT_ASSOCIE avec nature="AVANCE". Pas de nouveau type_flux dédié. Pas de besoin différencié spécifique à ce stade.
Tables : MASTER_FACT_MAN_IK_Avantages.xlsx

---

### D096 — Correction type logement LOG_0016 : T3 au lieu de T2 (Cyprien / Clarisse)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : LOG_0016 (T3 20 rue de l'Amiral Galache, Clarisse / PROP_0012) était classé TYPE_002 (T2) par erreur. Correction en TYPE_003 (T3). Coût standard applicable : 55€ (COUT_MEN_003) au lieu de 39€ (COUT_MEN_002). Logement archivé dans Hostaway (actif=NON). L'internalName Hostaway "T2 - Cyprien (Clarisse)" n'est pas modifié (donnée source externe). MAP_LOG_0075 et MAP_LOG_0076 mis à jour vers "T3". Lots 6a, 6b, 6c : aucun recalcul — LOG_0016 absent de M04 et CleaningTasks ; montants Lot 6c déjà à 55€ (tarif T3) ; type_logement_id non stocké dans MASTER Lot 6c.
Tables : REF_Setup.xlsm (REF_Logements, REF_Mapping_Logements)

---

### D-LOT-PROD-01 — Bascule nouvelle société et conservation de l'historique de performance
Date : 2026-06-15 | Statut : VALIDÉ
Contexte : La SAS porteuse est **en cours d'immatriculation**. L'activité tourne déjà, mais la structure juridique définitive n'est pas encore opérationnelle. Avant la mise en production comptable, on veut d'abord **vérifier que tout le pipeline fonctionne intégralement** (Lot 0 → Lot 12) sur les données actuelles.

Décision :
- Société **en cours d'immatriculation** ; le **point de bascule** (passage à l'exploitation comptable de la nouvelle structure) sera **paramétré plus tard**.
- **L'historique de performance financière est conservé** : réservations, payouts, commissions, net propriétaire, résultats par logement / propriétaire / mois, référentiels. Cet historique n'est jamais purgé.
- Les **données bancaires et comptables rattachées à l'ancienne structure** (imports bancaires, rapprochements, clôtures banque, soldes, charges comptables, acomptes / règlements bancaires, justificatifs sensibles) sont **purgeables / réinitialisables au moment de la mise en production** de la nouvelle société.
- **Aucune purge ni réinitialisation sans validation humaine explicite.** La purge est une action manuelle déclenchée par l'utilisateur, jamais automatique.
- Purge autorisée **uniquement après backup / snapshot** complet vérifié.
- Le **solde initial de banque** de la nouvelle structure sera **fourni plus tard** par l'utilisateur.
- Les **coordonnées définitives de la société** (nom légal, SIRET, RCS, adresse, TVA intracom, IBAN, logo) seront **fournies plus tard**.

Conséquences :
- Aucune action de purge, suppression, modification de REF_Setup ou modification banque/compta n'est entreprise tant que la bascule n'est pas décidée et validée.
- La séparation **historique de performance / comptabilité de production** est documentée dans ARCHITECTURE_DONNEES.md (section « Bascule société et séparation historique / production »).
- Futurs paramètres à prévoir dans `REF_Parametres_Generaux` au moment de la bascule : `DATE_BASCULE_SOCIETE`, `SOLDE_INITIAL_BANQUE`, `STATUT_PERIODE` (= `AVANT_BASCULE` / `APRES_BASCULE`).
- Interdiction de mélanger les données bancaires / comptables de l'ancienne structure avec la nouvelle société.

Tables : REF_Parametres_Generaux (paramètres futurs), pipeline banque (NORM_Banque, REF_Cloture_Mensuelle), MASTER_CALC_* (historique conservé)

---

### DO-03 — Barème IK kilométrique
> **FERMÉE, voir D036.** Montant direct retenu au démarrage, barème optionnel plus tard.

---

### D097 — Historique des réservations clôturées prime l'extract pour les mois clôturés
Date : 2026-06-17 | Statut : VALIDÉ
Contexte : Ne pas supposer que l'API Hostaway fournira toujours l'historique complet (réservations passées non archivées au fil de l'eau, payouts renseignés tardivement). Besoin d'une source de vérité figée pour les mois clôturés.

Décision — logique **générique** (tous canaux, pas spécifique Hostaway ni VRBO) :
- Une **table historique unique des réservations clôturées** (`HIST_Reservations_Cloturees`) archive toutes les réservations validées des mois clôturés (Airbnb, Booking, VRBO, Direct, hors Hostaway).
- **Clôture pilotée uniquement par `REF_Cloture_Mensuelle.statut_mois = CLOTURE`.** Aucune règle automatique « mois < mois courant ». REF vide ⇒ 0 mois clôturé ⇒ tout reste live.
- Pour un mois clôturé, **HIST prime l'extract live**. Pour un mois ouvert / absent / `EN_CONTROLE`, la source reste live (API Hostaway + `SAISIE_ReservationsHorsHostaway`).
- **Upsert sans suppression** sur clé stable (`reservation_id_hostaway`, sinon `reservation_hh_id`) : une réservation disparue de l'API après clôture est conservée et réinjectée.
- Toute différence live vs HIST sur mois clôturé ⇒ **alerte / ligne d'ajustement**, jamais écrasement silencieux.
- Date de référence métier = **check-in** (mois dérivé du check-in).
- **Clôture initiale technique** (réservations) ≠ clôture comptable finale banque (commentaire obligatoire dans `REF_Cloture_Mensuelle`).
- Architecture : `lot4bis` reste live-only ; `lot4ter_historiser_reservations_cloturees.py` archive ; `lot4quater_resoudre_source_reservations.py` résout open/closed → `MASTER_CALC_Reservations_Resolues.xlsx` (consommé par lot9/10/11/12, qui ne portent plus la bascule).
- Valeurs référentielles : `source_ligne`/`source_montant = HIST_RESERVATIONS_CLOTUREES`, `methode = HIST_PRIME_MOIS_CLOTURE`, `origine_initiale ∈ {API_HOSTAWAY, SAISIE_HH, BACKFILL_VRBO, CORRECTION_VALIDEE}`, `canal ∈ {AIRBNB, BOOKING, VRBO, DIRECT, HH}`.
Tables : HIST_Reservations_Cloturees.xlsx, MASTER_CALC_Reservations_Resolues.xlsx, REF_Cloture_Mensuelle, REF_Sources_Systeme (SRC_010)

---

### D098 — Backfill VRBO historique + commission VRBO
Date : 2026-06-17 | Statut : VALIDÉ
Contexte : Les payouts VRBO des réservations passées sont absents de l'API (paymentStatus=Unknown). Un extract VRBO historique ponctuel (`01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv`) fournit les montants nets manquants.

Décision :
- Le **backfill VRBO** est une **origine de correction ponctuelle** (`origine_initiale = BACKFILL_VRBO`) intégrée dans `HIST_Reservations_Cloturees`. **Ce n'est PAS une table VRBO ni une branche structurante.** Pas de pipeline VRBO récurrent : les futurs payouts VRBO arriveront naturellement via Hostaway.
- Réconciliation csv → réservation par `logement_id` + check-in. 27 réservations, net total 11 883,67 € (montant = « Montant du paiement » net après déductions).
- Une fois corrigée, une VRBO est traitée comme une **réservation clôturée normale**, `canal = VRBO`, statut VALIDE.
- **Commission VRBO** : `assiette_commission = payout − coût ménage standard du logement` ; `commission = assiette × taux propriétaire` ; `net_proprietaire = payout − ménage − commission`. VRBO **n'est jamais routé en HH** dans lot10 (branche VRBO dédiée).
- Les VRBO futures sans payout réel restent `A_CONTROLER` (mois ouvert), hors VUE_FLUX.
Résultats vérifiés (clôture 2025-01→2026-05) : ménage VRBO 1 485,00 € ; assiette 10 398,67 € ; commission 1 975,75 € ; net propriétaire 8 422,92 €.
Tables : HIST_Reservations_Cloturees.xlsx, MASTER_CALC_Reservations_Resolues.xlsx, MASTER_CALC_Commissions.xlsx

---

## Module ménages — refonte (D099-D106, 2026-06-18)

### D099 — Rapprochement volumes ménages
Date : 2026-06-18 | Statut : VALIDÉ
Comparer, **sans aucun coût**, le NOMBRE de ménages : Hostaway Tasks réalisés + réservations HH (logements hors Hostaway) vs factures externes + déclarations internes M04. Sortie `MASTER_CTRL_Rapprochement_Menages`. Un écart = à expliquer, jamais une accusation. Lot 6d.

### D100 — Source Hostaway des ménages
Date : 2026-06-18 | Statut : VALIDÉ
Utiliser **Hostaway Tasks API**, pas les réservations. Compter uniquement les tasks `completed` / réalisées (date réf = date task / canStartFrom). HH : 1 réservation validée = 1 ménage attendu (checkout), **uniquement** logements sans listingMapId Hostaway actif (anti double comptage tasks).

### D101 — Méthode interne selon période
Date : 2026-06-18 | Statut : VALIDÉ
Pivot **2026-06**. ≤ 2026-05 : `INTERNE_HEURES_M04` = nb_heures × taux horaire (PARAM_004). ≥ 2026-06 : `INTERNE_STANDARD_PARAMETRE` = nb_menages × forfait `REF_Couts_Menage_Interne` (Studio 30 / T2 35 / T3 45, date-aware). Type absent → A_CONTROLER `COUT_INTERNE_TYPE_LOGEMENT_ABSENT`. Montants jamais codés en dur.

### D102 — Écart vs coût standard
Date : 2026-06-18 | Statut : VALIDÉ
Référence permanente = **coût standard facturé** (`REF_Couts_Standards_Menage`, date-aware). `ecart = cout_standard_total − cout_reel/complet_total` ; >0 GAIN, <0 PERTE, =0 EQUILIBRE. Méthodes : EXTERNE_FACTURE / INTERNE_HEURES_M04 / INTERNE_STANDARD_PARAMETRE / NON_CALCULABLE (donnée absente, jamais d'invention).

### D103 — Répartition des charges communes ménage
Date : 2026-06-18 | Statut : VALIDÉ
Clé unique de ventilation = `poids = nb_menages × cout_standard_menage_appartement`. Pas de répartition au nombre simple. `quote_part = montant_pool × poids_ligne / Σ poids_pool`.

### D104 — Mapping intervenants Hostaway
Date : 2026-06-18 | Statut : VALIDÉ
`assigneeUserId` **fait foi**, mappé dans **`REF_Intervenants`** (colonne `hostaway_assigneeUserId`, pas de table séparée). 1059650→INT_0001 Imène, 1059682→INT_0002 Kheira, 1061546→INT_0003 Mounir, 1059064→INT_0004 Aissata, 1061542→INT_0005 Imrane. `None`/`0` non mappés. `title` = contrôle secondaire (`CONFLIT_TITLE_ASSIGNEE`, A_CONTROLER non bloquant). Non assigné : historique = INFO `TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE` ; futur/mois ouvert = A_CONTROLER `TASK_FUTURE_SANS_INTERVENANT_ASSIGNE`. Kira (Google Sheet) = Kheira INT_0002.

### D105 — Écart analytique ménage en HORS_COMPTA (révisée 2026-06-18)
Date : 2026-06-18 | Statut : VALIDÉ (révise la version initiale « vue analytique uniquement »)
Le gain/perte ménage **impacte le résultat HORS_COMPTA** par une logique analytique. Seul l'**écart** est injecté, jamais le coût complet entier.
- `ecart_analytique = cout_standard_total − cout_complet_reel_total` (lot6f). >0 GAIN → augmente HC ; <0 PERTE → diminue HC ; =0 aucun impact.
- Injection en **deux niveaux** (mois × logement × intervenant), HC analytique, `impact_resultat_reel=OUI`, `impact_resultat_comptable=NON` :
  - **`TYPE_FLUX_019` MENAGE_STANDARD_ANALYTIQUE** = coût standard ménage par appartement, toujours **CHARGE HC**.
  - **`TYPE_FLUX_018` GAIN_PERTE_MENAGE_ANALYTIQUE** = écart : gain→**PRODUIT HC**, perte→**CHARGE HC**.
  - Net HC ménage = 019 (charge standard) + 018 (écart) = −coût complet réel. Jamais le coût complet brut injecté directement.
- **`TYPE_FLUX_013` (M04 MO interne réel) = analytique SEUL : JAMAIS injecté dans MASTER_CALC_Flux** comme charge résultat/compta. Il ne sert qu'au calcul du coût complet (lot6f). Évite le double comptage (le coût MO est déjà une composante du coût complet référencé par l'écart).
- REEL = COMPTABLE + HORS_COMPTA reste cohérent. **HORS_COMPTA bouge du standard analytique (TYPE_FLUX_019) plus l'écart (TYPE_FLUX_018), soit le coût complet analytique ménage.** TYPE_FLUX_013 reste analytique seul et non injecté. **Aucun impact** comptable, commissions, net propriétaire ou factures propriétaires.
- Restitution analytique (nb ménages, coût standard total, coût complet réel, écart, statut GAIN/PERTE/EQUILIBRE) par mois × appartement : `MASTER_CALC_CoutComplet_Menages` (lot6f).

### D106 — Coût complet ménage avancé
Date : 2026-06-18 | Statut : VALIDÉ
- **Cave/local** (REC_002) = ventilée **uniquement sur les ménages INTERNES** (jamais les externes), clé D103, date-aware. Contrôle `REC_002_LOCAL_CAVE_INTERNE_ONLY`.
- **Lavage interne** = source **Google Sheet / M04** (payé par les intervenantes internes). **Jamais exporté** vers SAISIE_Charges_Flux. Si saisi un jour dans SAISIE → `affectable_menage = NON` (sauf décision contraire). Contrôle `DOUBLE_SOURCE_LAVAGE_A_CONTROLER`.
- **Courses / consommables / achats** = `SAISIE_Charges_Flux` **uniquement si `affectable_menage = OUI`**. Affectation via `intervenant_concerne` (MENAGE_INTERNE / COMMUN_MENAGE / INT_xxxx / multi `INT_x;INT_y`). SAISIE vide → pools = 0 `POOL_VIDE_NON_SAISI` (non saisi ≠ inexistant).
- **Abandon** de la logique `fournitures_incluses`.
- Colonnes ajoutées à SAISIE_Charges_Flux : `affectable_menage`, `intervenant_concerne`.
Tables : REF_Couts_Menage_Interne, REF_Intervenants (mapping Hostaway), SAISIE_Charges_Flux, MASTER_CTRL_Rapprochement_Menages, MASTER_CALC_GainPerte_Menages, MASTER_CALC_CoutComplet_Menages. Lots 6d/6e/6f.

### D-HORS-PARC-TECHNIQUE-01 - Separation actif / statut_parc
Date : 2026-06-29 | Statut : VALIDE
Decision : `REF_Logements.actif` decrit la disponibilite technique d'un code referentiel. `REF_Logements.statut_parc` decrit l'eligibilite du code au parc de logements geres.

Valeurs autorisees de `statut_parc` :
- `GERE` : logement reellement gere et eligible aux calculs metier.
- `HORS_PARC_TECHNIQUE` : code conserve pour controle ou anti-mauvais-mapping, exclu explicitement de tout calcul economique et operationnel.

Toute valeur vide, invalide ou inconnue de `statut_parc` produit `A_CONTROLER` avec code anomalie `STATUT_PARC_INVALIDE` et ne doit produire aucune commission, facture/prefacture, net proprietaire, menage, flux proprietaire ou resultat par logement.
Tables : REF_Logements, MASTER_CALC_Reservations, MASTER_CALC_Commissions, MASTER_CALC_NetProprietaire, MASTER_FACT_Proprietaires

---

### D-LOT4A-01 — Statuts de run LOT4A : cinq valeurs officielles dont ANALYSE_BLOQUEE_DONNEES
Date : 2026-07-02 | Statut : VALIDÉ
Contexte : LOT4A est un moteur Python déterministe (SAISIE_ReservationsHorsHostaway → MASTER de test).
  La découverte du préflight (aucun Power Query réel dans SAISIE ni MASTER) a conduit à reformuler
  D-APP-05A comme un dry-run Python pur sans Excel COM. Les statuts de sortie doivent être exhaustifs
  et mutuellement exclusifs. Le 5e statut ANALYSE_BLOQUEE_DONNEES a été introduit en cours de
  développement et validé explicitement par l'utilisateur le 2026-07-02.

Cinq statuts officiels du run LOT4A :
  `ANALYSE_TERMINEE`
      Run terminé sans anomalie bloquante. MASTER de test généré sous 04_LOGS/LOT4A_DRY_RUN/.

  `ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES`
      S'applique au comparateur (--compare-existing) : run terminé ; des champs NULL dans
      le MASTER réel existant ont déclenché la catégorie ECART_HISTORIQUE_ATTENDU
      (MASTER legacy incomplet — jamais une erreur).
      Le dry-run transformateur ne compare pas le MASTER legacy existant : il produit
      uniquement ANALYSE_TERMINEE, ANALYSE_BLOQUEE_TAUX, ANALYSE_BLOQUEE_DONNEES
      ou ERREUR_TECHNIQUE — jamais ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES.

  `ANALYSE_BLOQUEE_TAUX`
      Taux de commission MISSING (aucun taux applicable pour la date de check-in et le propriétaire)
      ou AMBIGUOUS (plusieurs taux actifs, résolution impossible) pour au moins une ligne SAISIE.
      Aucun MASTER de test généré. Aucun fichier .xlsx partiel. manifest.json + rapport_anomalies.md.

  `ANALYSE_BLOQUEE_DONNEES`
      Au moins une ligne de saisie manuelle invalide ou incohérente (voir codes ci-dessous).
      Aucun MASTER de test généré. Aucun fichier .xlsx partiel. manifest.json + rapport_anomalies.md.

  `ERREUR_TECHNIQUE`
      Exception Python, fichier illisible, schéma inaccessible ou défaut logiciel non prévu.
      Aucun MASTER de test généré. Message sur stderr, code retour 2.

Codes de blocage données (ANALYSE_BLOQUEE_DONNEES) :
  PK_HH_INVALIDE              : reservation_hh_id absent, vide ou format invalide
  PK_HH_DOUBLON               : reservation_hh_id dupliqué dans la SAISIE
  DATE_ARRIVEE_INVALIDE       : date_arrivee absente ou non parseable
  DATE_DEPART_INVALIDE        : date_depart absente ou non parseable
  DATES_SEJOUR_INCOHERENTES   : date_depart <= date_arrivee
  MONTANT_INVALIDE            : total_percu ou menage non numérique
  CHAMP_OBLIGATOIRE_MANQUANT  : champ obligatoire vide (logement_id, proprietaire_id, etc.)
  CODE_IMPACT_INVALIDE        : code_impact hors valeurs fermées (IC/HC/HR)
  STATUT_CONTROLE_INVALIDE    : statut_controle hors valeurs fermées

Règle manifest — pour tout statut bloquant :
  `master_test_genere = false` (toujours) et `motif_blocage = <code précis>` (toujours renseigné).

Périmètre : exclusivement LOT4A. Ces statuts ne s'appliquent pas aux autres lots ni à l'application.
Tables : manifest.json (04_LOGS/LOT4A_DRY_RUN/), rapport.md, rapport_anomalies.md

---

### D-APP-05C — Formules Excel B (ROW_HASH) et C (mois) : indépendance de la langue et du séparateur
Date : 2026-07-02 | Statut : VALIDÉ
Contexte : Les formules originales de SAISIE_ReservationsHorsHostaway.xlsx utilisaient TEXT() avec des masques
  de date (YYYY, DD) et décimal ("0.00") — tokens de la locale EN, illisibles sous Excel FR.
  Symptômes : colonne C affichait "YYYY-05" au lieu de "2026-05" ; colonne B affichait "YYYY05DD|23.43".
  Anomalie préexistante (classeur créé par openpyxl), non introduite par l'application.
  Correction appliquée atomiquement le 2026-07-02 via D-APP-05D. Backup conservé.

Décision permanente :
  Les formules Excel B (ROW_HASH) et C (mois) de SAISIE_ReservationsHorsHostaway.xlsx doivent
  rester indépendantes de la langue Excel et des séparateurs locaux.

Interdits dans ces formules :
  YYYY, DD, AAAA, JJ — masques de date localisés
  TEXT() avec un masque de date ou décimal ("0.00", "#,##0.00", etc.)

Formules canoniques validées (locale-agnostiques), N = numéro de ligne (2 à 501) :
  C (mois) :
    =IF(IN="","",YEAR(IN)&"-"&RIGHT("0"&MONTH(IN),2))

  B (ROW_HASH) :
    =IF(AN="","",AN&"|"&DN&"|"&FN&"|"&GN
       &"|"&IF(IN="","",YEAR(IN)&RIGHT("0"&MONTH(IN),2)&RIGHT("0"&DAY(IN),2))
       &"|"&IF(LN="","",IF(ROUND(LN*100,0)<0,"-","")
               &INT(ABS(ROUND(LN*100,0))/100)&"."
               &RIGHT("0"&MOD(ABS(ROUND(LN*100,0)),100),2)))

  Fonctions autorisées : YEAR, MONTH, DAY, RIGHT, ROUND, INT, ABS, MOD.
  Le point décimal est un littéral chaîne, jamais un masque TEXT.

Cohérence Python ↔ Excel (contrat recompute()) :
  mois      = d_arr.strftime("%Y-%m")
  arr_txt   = d_arr.strftime("%Y%m%d") if d_arr else ""
  total_txt = f"{_num(total):.2f}" if total != "" else ""
  ROW_HASH  = f"{pk}|{canal}|{prop}|{log}|{arr_txt}|{total_txt}"
  Point à traiter avant l'écriture applicative APP-2b :
  un contrôle futur devra décider du traitement des montants ayant plus de deux décimales.
  Aucun blocage MONTANT_PRECISION_INVALIDE n'est implémenté ni actif à ce stade.

Tables : SAISIE_ReservationsHorsHostaway.xlsx (colonnes B, C — lignes 2 à 501)

---

### D-APP-2B — Cadrage fonctionnel verrouillé de la saisie HH contrôlée (D1 à D11)
Date : 2026-07-03 | Statut : VALIDÉ — cadrage verrouillé, implémentation NON démarrée
Portée : décisions fonctionnelles APP-2b (création contrôlée de réservations hors Hostaway).
  Aucune route d'écriture, aucun writer, aucune modification Excel active à ce stade.
  saisie_writer.py reste stub (NotImplementedError). Les codes de blocage listés ne sont pas encore implémentés.

D1 — reservation_id_hostaway (champ H)
  - Obligatoire pour VRBO_UNKNOWN, DIRECT_HA_PAYANT, HOSTAWAY_REFERENCE.
  - Facultatif pour les autres sources financières.
  - Format : entier positif, chiffres uniquement.
  - Si renseigné : unicité obligatoire parmi les réservations HH actives.
  - Doublon : blocage RESERVATION_DOUBLON_HOSTAWAY_HH.
  - La règle README devient la règle APP-2b, même si l'ancien moteur ne l'applique pas encore.

D2/D3 — total_percu
  - Obligatoire pour toute création APP-2b, y compris VRBO_UNKNOWN.
  - La tolérance historique du contrôle Excel CTR-L4-13 n'est PAS reprise par l'application
    (le moteur Lot4A bloque ensuite une ligne sans montant).

D4 — canal et source financière
  - Aucun mapping canal <-> source financière imposé par APP-2b à ce stade.
  - L'application peut suggérer une valeur cohérente à l'interface, mais :
    * l'utilisateur choisit un canal_id valide ;
    * l'application valide seulement l'appartenance aux listes référentielles ;
    * aucune combinaison valide n'est rejetée uniquement sur ce mapping ;
    * aucune valeur n'est corrigée automatiquement.

D5 — codes de blocage APP-2b (validés, non implémentés)
  MOIS_HORS_REFERENTIEL_CLOTURE
  MOIS_CLOTURE
  LOGEMENT_SANS_GESTION_ACTIVE
  PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE
  LOGEMENT_HORS_PARC_TECHNIQUE
  DIVERGENCE_REF_LOCALE_REF_SETUP
  RESERVATION_HH_ID_DUPLIQUE
  RESERVATION_DOUBLON_HOSTAWAY_HH
  SEQUENCE_PK_INCOHERENTE

D6 — génération de reservation_hh_id
  - Format : RESHH-AAAA-MM-NNN.
  - AAAA-MM = mois de date_arrivee.
  - NNN = maximum numérique existant du mois + 1.
  - Les trous de séquence sont admis.
  - Toute clé du même mois avec suffixe non numérique ou format incompatible : blocage SEQUENCE_PK_INCOHERENTE.
  - Toute collision bloque.
  - Aucun identifiant généré si le mois est clôturé ou absent du référentiel.

D7 — historique de gestion logement (REF_Gestion_Logements_Hist)
  - date_fin inclusive.
  - date_fin vide = période de gestion ouverte.
  - Cohérence propriétaire/logement contrôlée à la date_arrivee.

D8 — éligibilité du logement
  Une création APP-2b exige simultanément :
    * relation de gestion active propriétaire <-> logement à la date d'arrivée ;
    * statut_parc = GERE ;
    * actif = OUI.
  Un logement technique, hors parc ou non géré est bloqué.
  L'application ne corrige jamais le propriétaire ou le logement.

D9 — divergence référentiels
  - REF_LOCALE garantit la compatibilité avec les validations Excel.
  - REF_Setup.xlsm est autoritaire pour activité, parc et historique de gestion.
  - Toute divergence pertinente bloque avec DIVERGENCE_REF_LOCALE_REF_SETUP.

D10 — mois de juillet 2026
  - L'application ne crée jamais un mois dans REF_Cloture_Mensuelle.
  - Ouverture de 2026-07 / OUVERT = opération manuelle hors application dans REF_Setup.xlsm,
    documentée et contrôlée.
  - Tant que la ligne manque : blocage MOIS_HORS_REFERENTIEL_CLOTURE.

D11 — associé récupérateur
  - associe_id_recuperateur obligatoire seulement si montant_recupere > 0.
  - Montant vide ou nul : associé récupérateur facultatif, laissé vide par défaut.

Statut APP-2b : cadrage fonctionnel verrouillé. Implémentation non démarrée.
Tables : SAISIE_ReservationsHorsHostaway.xlsx, REF_Setup.xlsm (REF_Cloture_Mensuelle,
  REF_Gestion_Logements_Hist, REF_Logements, REF_Associes), REF_LOCALE.
### D-APP-2B-REV1 — Règles paiement, acomptes et dérogations saisie HH

**Date** : 2026-07-03
**Statut** : VALIDÉ
**Périmètre** : APP-2b, SAISIE_ReservationsHorsHostaway sur migration contrôlée, Lot4A.

Décisions validées :
- APP-2b saisit exclusivement des réservations hors Hostaway : `reservation_id_hostaway` n'est plus demandé, plus affiché et plus requis. Toute valeur postée manuellement est refusée ou ignorée de façon traçable. La colonne physique existante peut rester vide jusqu'à migration contrôlée.
- `source_financiere = SAISIE_MANUELLE` est pré-sélectionnée sur nouveau formulaire. Une valeur déjà choisie reste conservée lors d'un retour de validation.
- Le taux de commission standard est résolu par `logement_id + proprietaire_id + date_arrivee` depuis `REF_Setup.xlsm -> REF_Taux_Commission`, périodes inclusives, priorité au taux logement puis taux propriétaire. Une dérogation exige taux, motif et confirmation explicite ; elle est tracée séparément et ne remplace pas silencieusement l'historique.
- Le prix ménage standard est résolu après logement + date depuis `REF_Setup.xlsm -> REF_Couts_Standards_Menage`. Une dérogation exige montant, motif et confirmation explicite ; elle est tracée séparément.
- Le mode de paiement `Direct propriétaire` est ajouté au schéma cible sous l'identifiant technique cohérent `PAY_006` / `DIRECT_PROPRIETAIRE`, via migration sur copie uniquement avant activation réelle.
- L'acompte facture propriétaire est rattaché au mois de `date_arrivee` et dépend du mode de paiement : banque pro = `total_percu`, compte perso associée = `montant_recupere_associe`, carte associée = `montant_recupere_associe`, espèces = `montant_reverse_proprietaire`, direct propriétaire = `0`.
- La comptabilisation n'est plus une liste libre : elle est dérivée automatiquement du code impact depuis `REF_Setup.xlsm -> REF_Codes_Impact.impact_resultat_comptable`.
- L'écriture réelle APP-2b reste désactivée tant que le classeur source réel et les dépendances MASTER/Lot4A/lot5/lot10/lot12 n'ont pas été migrés et contrôlés sur copies.

### D-APP-2B-REV2 — Confirmation ergonomique des dérogations et acompte propriétaire

**Date** : 2026-07-03
**Statut** : VALIDÉ
**Périmètre** : APP-2b, Lot4A, documentation métier.

Décisions validées :
- Les dérogations de taux et de ménage ne sont plus présentées par défaut dans un bloc d'alerte ou par une case à cocher. Le formulaire affiche la valeur automatique, sa source et un bouton compact `Modifier`. La saisie d'une valeur différente ouvre, au clic sur `Vérifier avant validation`, une modale locale de confirmation avec valeur automatique, valeur demandée, motif obligatoire distinct et bouton `Annuler` focalisé par défaut.
- Une dérogation identique à la valeur automatique est traitée comme une absence de dérogation. Une dérogation différente reste bloquée côté backend sans motif et confirmation.
- Les champs `Montant récupéré par l'associé (€)` et `Associé récupérateur` sont visibles et obligatoires uniquement pour `CARTE_ASSOCIEE` et `COMPTE_PERSO_ASSOCIEE`. Les valeurs cachées sont vidées dans le navigateur et ignorées côté backend pour les autres modes.
- `Montant reversé propriétaire (€)` est visible uniquement pour le mode espèces. Les valeurs cachées sont vidées dans le navigateur et ignorées côté backend pour les autres modes.
- L'acompte facture propriétaire reste rattaché au mois de `date_arrivee`. La règle officielle devient : banque pro = `total_percu`, carte associée = `total_percu`, compte perso associé = `total_percu`, espèces = `total_percu - montant_reverse_proprietaire`, direct propriétaire = `0`. Le `montant_recupere` reste une information de contrôle/traçabilité et ne réduit jamais l'acompte.
- Les sources Lot4A associées sont `TOTAL_PERCU`, `TOTAL_PERCU_ASSOCIE`, `TOTAL_PERCU_MOINS_REVERSE_ESPECES`, `DIRECT_PROPRIETAIRE`.
- Le mode cible `PAY_006` / `DIRECT_PROPRIETAIRE` et son libellé `Direct propriétaire` sont conservés dans le schéma cible et les migrations sur copie.
- L'écriture réelle APP-2b reste désactivée (`HH_REAL_WRITE_ENABLED=False`) et aucun fichier Excel réel n'est modifié par REV2.

### D-APP-2B-REV3 — Taux dérogatoire canonique

**Date** : 2026-07-04
**Statut** : VALIDÉ
**Périmètre** : APP-2b, Lot4A, schéma cible de traçabilité.

Décisions validées :
- L'interface saisit un pourcentage utilisateur dans `taux_commission_override_pct` : `18` = 18 %, `0,5` = 0,5 %, `0` = 0 %.
- Le backend APP-2b convertit ce pourcentage avec `Decimal`, accepte la virgule française, borne la valeur entre 0 et 100 inclus, limite à deux décimales utilisateur et stocke uniquement le taux décimal canonique dans `taux_commission_override`.
- `0 %` est autorisé. Si le taux automatique est déjà 0 %, une saisie `0` est neutralisée comme absence de dérogation ; si le taux automatique est non nul, `0` est une vraie dérogation qui exige motif et confirmation.
- Lot4A reçoit uniquement `taux_commission_override` en décimal canonique entre 0 et 1 inclus. Toute valeur confirmée hors bornes bloque explicitement ; aucune division implicite par 100 ni heuristique `override > 1` n'est autorisée.
- Le writer futur et la colonne cible SAISIE HH ne doivent jamais persister le pourcentage brut navigateur.
- `HH_REAL_WRITE_ENABLED` reste `False` et aucun fichier Excel réel n'est modifié par REV3.

### D-APP-2C - Previsualisation complete sur copies avant activation d'ecriture reelle

**Date** : 2026-07-04
**Statut** : VALIDE
**Perimetre** : APP-2c, saisie HH, migration de schema sur copie, Lot4A dry-run.

Decisions validees :
- Apres validation APP-2b, l'utilisateur passe par `Previsualiser l'enregistrement` avant toute activation future d'ecriture reelle.
- APP-2c execute le scenario de bout en bout sur copies isolees : payload canonique, copie de SAISIE HH, copie REF_Setup si necessaire, migration cible sur copie, injection simulee, execution Lot4A sur copie, controles de coherence et resume avant/apres.
- Les resultats affiches proviennent du backend et de Lot4A, jamais d'une estimation JavaScript.
- Le dossier dry-run applicatif contient un sous-dossier unique avec `manifest.json`, `SAISIE_ReservationsHorsHostaway_copie.xlsx`, `REF_Setup_copie.xlsm`, `MASTER_FACT_MAN_ReservationsHorsHostaway_simule.xlsx` et `resultat_lot4a.json`.
- Le manifest trace l'horodatage UTC, l'identifiant de simulation, les hashes des sources lues, les hashes des copies, le resume du payload, le statut et les erreurs.
- La simulation ne contourne pas `HH_REAL_WRITE_ENABLED=False` : le writer reel reste garde et l'ecran APP-2c ne propose aucun bouton de sauvegarde reelle.
- L'activation de l'ecriture reelle reste une etape separee, necessitant migration controlee du classeur source reel, validation humaine et controles aval.

### D-APP-2D - Activation controlee de l'ecriture reelle HH

**Date** : 2026-07-04
**Statut** : VALIDE
**Perimetre** : APP-2d, activation preparee de l'ecriture reelle des reservations hors Hostaway.

Decisions validees :
- L'ecriture reelle reste desactivee par defaut. Deux flags independants sont obligatoires : `HH_REAL_WRITE_ENABLED=True` et `HH_REAL_WRITE_CONFIRMATION_ENABLED=True`. Aucun ecran applicatif ne permet de modifier ces flags.
- Une ecriture reelle ne peut etre tentee qu'a partir d'une simulation APP-2c recente, agee de moins de 30 minutes, au statut `OK`, avec `lot4a_status = ANALYSE_TERMINEE`, sans erreur bloquante.
- Les hashes reels de `REF_Setup.xlsm` et `SAISIE_ReservationsHorsHostaway.xlsx` doivent etre strictement identiques aux hashes sources conserves dans le manifest APP-2c.
- La reservation simulee ne doit pas deja exister dans SAISIE HH, le mois doit rester `OUVERT` dans `REF_Cloture_Mensuelle`, et le schema reel doit deja contenir les champs cibles requis.
- APP-2d ne migre jamais le fichier reel. Si des colonnes cibles SAISIE ou le mode `PAY_006 / DIRECT_PROPRIETAIRE` manquent, le diagnostic `SCHEMA_REEL_NON_PREPARE` bloque l'ecriture.
- Avant toute ecriture, une copie de rollback est creee. Toute erreur apres debut d'ecriture restaure automatiquement cette copie et journalise `ECRITURE_REELLE_ANNULEE_ET_RESTAUREE` avec hashes avant/apres.
- Apres ecriture, Lot4A est execute sur une copie post-ecriture pour verifier que la reservation apparait avec les memes calculs que la simulation.
- Meme avec tous les prerequis valides, l'utilisateur doit saisir exactement `ENREGISTRER RESHH-AAAA-MM-NNN` dans une confirmation finale.
- Aucune ecriture reelle n'est realisee pendant APP-2d ; les tests d'ecriture utilisent uniquement des copies temporaires.

### D-APP-2E - Preparation schema reel HH et recette integrale sur copies

**Date** : 2026-07-04
**Statut** : VALIDE
**Perimetre** : APP-2e, diagnostic schema HH, migration future controlee, recette ecriture reelle sur copies.

Decisions validees :
- APP-2e prepare la migration du schema reel sans l'executer. Les fichiers reels `REF_Setup.xlsm` et `SAISIE_ReservationsHorsHostaway.xlsx` ne sont pas modifies pendant le developpement ni pendant les tests.
- Le diagnostic schema HH est idempotent et signale les colonnes cible manquantes dans SAISIE, la presence du mode `PAY_006 / DIRECT_PROPRIETAIRE`, les formules critiques, validations, tables, plages nommees, MFC, VBA et hashes avant operation.
- La migration de schema sur copies ajoute uniquement les colonnes APP-2b/APP-2c manquantes et le mode `PAY_006 / DIRECT_PROPRIETAIRE` s'il est absent. Elle ne deplace pas les colonnes historiques et n'ecrase jamais les formules critiques B/C/K/N/O/Q/V/Y/Z.
- La future migration reelle est reservee a une commande locale explicite, avec confirmation exacte `MIGRER_SCHEMA_HH_REELLE`, snapshots des deux classeurs, validation sur temporaires, remplacement atomique et rollback des deux fichiers si un remplacement echoue.
- Aucune migration reelle n'est lancee depuis l'interface applicative. L'ecran APP-2c/APP-2d affiche seulement le diagnostic `SCHEMA_REEL_NON_PREPARE` et les elements manquants.
- La recette APP-2e execute sur copies le chemin complet APP-2c puis APP-2d : validation, migration copie, simulation, ecriture controlee sur copie, Lot4A post-ecriture, comparaison stricte simulation/ecriture et rollback force.
- Les champs de derogation `taux_commission_override`, `motif_override_taux_commission`, `confirmation_override_taux_commission`, `menage_override`, `motif_override_menage`, `confirmation_override_menage` sont propages jusqu'aux colonnes cible lorsqu'elles existent. Les taux `0.005` et `0.00` restent des valeurs tracees et ne sont jamais traites comme vides.
- `HH_REAL_WRITE_ENABLED=False` et `HH_REAL_WRITE_CONFIRMATION_ENABLED=False` restent les valeurs par defaut. La premiere activation reelle reste une etape separee apres migration reelle controlee et validation humaine.

### D-APP-2E-VBA - Intégrité binaire VBA et parties ZIP sensibles (APP-2e durcissement)

**Date** : 2026-07-05
**Statut** : VALIDE
**Perimetre** : APP-2e, controle VBA post-migration, protection package Excel.

Decisions validees :
- La presence du VBA (`has_vba=True`) n'est pas suffisante. La migration sur copies doit verifier l'integrite binaire octet pour octet de `xl/vbaProject.bin` via SHA-256 avant et apres migration. Toute difference produit le code bloquant `VBA_PRESERVATION_ECHEC`.
- Si `xl/vbaProjectSignature.bin` est present avant migration, il doit etre present et identique apres. Toute disparition ou modification produit `VBA_PRESERVATION_ECHEC`.
- `[Content_Types].xml` doit toujours declarer un classeur macro-enabled (`macroEnabled` dans le ContentType du workbook) apres migration d'un classeur `.xlsm` avec VBA.
- `xl/_rels/workbook.xml.rels` doit toujours contenir une relation de type `relationships/vbaProject` apres migration d'un classeur avec VBA.
- Les parties ZIP sensibles (`xl/activeX/`, `xl/ctrlProps/`, `xl/embeddings/`, `xl/externalLinks/`, `xl/connections.xml`, `customUI/`, `docProps/custom.xml`, `xl/printerSettings/`) doivent rester presentes et identiques si elles existent avant migration. Toute alteration non autorisee produit `PACKAGE_SENSIBLE_PRESERVATION_ECHEC`.
- Ces controles ne s'appliquent qu'a la comparaison copie-reference/copie-travail ; ils ne touchent jamais les fichiers sources reels.
- Le manifest `preparer_migration_hh_sur_copies` inclut desormais `vba_snapshots` avec les empreintes avant/apres pour audit.
- Les tests de ces controles utilisent des fichiers ZIP `.xlsm` synthetiques construits en Python ; ils ne dependent pas du vrai `REF_Setup.xlsm`.

### D-APP-2E-MIGREE - Migration réelle schéma HH exécutée (2026-07-05)

**Date execution** : 2026-07-05T00:17:47Z
**Operateur** : Ewan Schmitt — confirmation manuelle `MIGRER_SCHEMA_HH_REELLE`
**Statut** : `real_status = OK`

Decisions validees :
- La migration reelle APP-2e a ete executee le 05/07/2026 via `executer_migration_hh_reelle()` avec confirmation humaine explicite.
- Les deux fichiers sources reels ont ete migres en transaction atomique avec backup preablable et rollback double-fichier disponible.
- SAISIE : 7 colonnes ajoutees en fin de schema (colonnes AE-AK), colonnes historiques intactes, formules B/C/K/N/O/Q/V/Y/Z preservees.
- REF_Setup : ligne PAY_006 / DIRECT_PROPRIETAIRE ajoutee une seule fois dans REF_Modes_Paiement. VBA preserve (`xl/vbaProject.bin` SHA-256 identique avant/apres : `09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2`).
- Les flags `HH_REAL_WRITE_ENABLED = False` et `HH_REAL_WRITE_CONFIRMATION_ENABLED = False` restent a False. L'activation de l'ecriture reelle des reservations est une etape separee independante.
- Sauvegarde manuelle pre-migration : `99_ARCHIVES\APP2E_SCHEMA_HH_20260705_020543\` (hors staging). Hashes pre-migration verifies : SAISIE `c3c00e73...`, REF `6d9f21de...`.
- Hashes post-migration : SAISIE `60b7bc85f7d59530e0a0fcdb9596162012db44611aeefaa3b1f0a97d32b18943`, REF `3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8`.

---

### D-APP-2-MENAGES — Module Ménages APP-2 : rapprochement + outrepassage

- **Date** : 2026-07-05
- **Lot** : APP-2 (ménages — sous-lot de Lot APP-2)
- **Statut** : ACTÉ

**Décisions :**
- D-M1 : Source de liste = `MASTER_CTRL_Rapprochement_Menages.xlsx`, onglet `TABLEAU_COMPARAISON`. Lecture seule, aucun recalcul. Aucun fallback, aucune reconstruction par jointure.
- D-M2 : Les 3 flux (tâches Hostaway completed / déclarés M04 internes / déclarés externes) restent **TOUJOURS** dans des champs séparés (`nb_menages_tasks_hostaway_completed`, `nb_menages_declares_interne_m04`, `nb_menages_declares_externe`). Jamais fusionnés, jamais agrégés dans un champ unique.
- D-M3 : Aucune valorisation ne s'appuie sur les données Hostaway. Le coût Hostaway (H6 cost=NULL) n'est jamais lu ni exposé. Coût réel = `MASTER_CALC_GainPerte_Menages.xlsx` uniquement.
- D-M4 : Enrichissement fiche = `MASTER_CALC_GainPerte_Menages.xlsx` (DETAIL_ECART_COUT). Coût complet (`MASTER_CALC_CoutComplet_Menages.xlsx`) = non chargé au MVP (lecture disponible à APP-3+ si besoin).
- D-M5 : Outrepassage = écriture SQLite uniquement (`menage_overrides` migration 0003 + `audit_events`). Jamais d'écriture dans un fichier Excel ou MASTER. Motif obligatoire. Contrainte UNIQUE(mois, logement_id, intervenant_id) — 2e outrepassage = mise à jour, pas doublon.
- D-M6 : Statut effectif = `JUSTIFIE` si override enregistré, sinon `statut_controle` du MASTER. L'affichage reflète l'override sans modifier la source.


## D-APP-3A-FOURNISSEURS — Décisions APP-3a : charges fournisseurs lecture seule

> Décisions de conception pour le module Fournisseurs (APP-3a), lot APP-3.
> Statut : VALIDÉES — 2026-07-05. Implémentées.

- D-C1 : Source unique de la liste = MASTER_FACT_MAN_Charges.xlsx onglet MASTER. Jamais SAISIE_Charges_Flux ni MASTER_CALC_Flux. Conforme D026.
- D-C2 : Lignes placeholder Power Query (charge_id commence par [) filtrées avant affichage. Aucune erreur levée — comportement normal si MASTER non rafraîchi.
- D-C3 : IK (	ype_flux_id=IK) et virements associés (	ype_flux_id=VIREMENT_ASSOCIE) exclus du périmètre Fournisseurs. Conforme D025.
- D-C4 : statut_controle affiché tel quel sans recalcul. Aucun calcul de métrique dans service ou reader. Conforme D044.
- D-C5 : status=OK si liste vide (MASTER non rafraîchi = comportement normal). status=ERROR uniquement si MASTER absent ou illisible.
- D-C6 : Aucun accès SQLite, aucune migration. Aucune écriture Excel. Aucune route POST.
- D-C7 : Identifiant de route = charge_id. Unicité non vérifiable empiriquement (0 données au moment de l'implémentation) — schéma prévoit charge_id comme clé intentionnelle.
- D-C8 : Filtres disponibles : mois, logement_id, categorie_charge_id, code_impact, statut_controle, associe_id.

### D-APP-3C-PROPRIETAIRES — Propriétaires & règlements, relevés, préfacture

> Statut : VALIDÉES — 2026-07-05. Implémentées.

- D-P1 : Sources de lecture : REF_Setup.xlsm/REF_Proprietaires (liste), MASTER_CALC_NetProprietaire.xlsx onglets REGLEMENT + VUE_MOIS (relevés), MASTER_FACT_Proprietaires.xlsx onglets FACT_FACTURE_ENTETE + FACT_FACTURE_LIGNES (préfacture). Aucun fallback source brute.
- D-P2 : Bloc EXPLOITATION et bloc REGLEMENT non mélangés dans le service ni dans les templates. Séparation structurelle (D033, EP1-EP7).
- D-P3 : revenu_net_exploitation lu depuis MASTER, jamais recalculé par le service.
- D-P4 : AirCover affiché uniquement comme information séparée : ligne ACOMPTE_AIRBNB dans le bloc règlement de la préfacture.
- D-P5 : Les lignes de préfacture sont structurellement présentes (EXPLOITATION + REGLEMENT séparés) pour chacune des 270 factures. Affiché tel quel. Ordre et comptage révisés par D-PREF-ORDRE-01 (12 lignes sans canapé, 13 avec ; CHARGES_EXCEPT_REFAC avant MONTANT_DU).
- D-P6 : Lot12 absent → préfacture status=UNAVAILABLE, sans exception, sans bloquer le relevé.
- D-P7 : Aucun accès SQLite, aucune écriture Excel. Routes GET uniquement.
- D-P8 : Clé relevé = prop_id × mois (VUE_MOIS) ; lignes par logement dans REGLEMENT (clé prop×log×mois unique — vérifié 270/270).

---

### D-APP-3B-0 — Référentiel ASSOC_MODE (QM-APP-3B-0)

> Statut : VALIDÉES — 2026-07-05. Implémentées.

- D-AM-1 : ASSOC_MODE n'est jamais un champ libre saisi par l'utilisateur. Il est généré déterministiquement depuis le référentiel fermé REF_Assoc_Mode.
- D-AM-2 : Table REF_Assoc_Mode créée dans REF_Setup.xlsm (migration contrôlée, copie uniquement en APP-3b-0). Colonnes : assoc_mode_id, mode_paiement_id, associe_id, assoc_mode, actif, commentaire.
- D-AM-3 : Lignes initiales : AM_001 PAY_001/BANQUE, AM_002 PAY_002/LIQ, AM_003 PAY_003+PERS_EWAN/EWAN-CB, AM_004 PAY_003+PERS_WAFA/WAFA-CB, AM_005 PAY_004+PERS_EWAN/EWAN-PERSO, AM_006 PAY_004+PERS_WAFA/WAFA-PERSO, AM_007 PAY_005/ADEF.
- D-AM-4 : Contraintes métier PAY_001 / PAY_002 : associe_id et carte_id interdits. PAY_003 : associe_id et carte_id obligatoires, carte cohérente avec associé. PAY_004 : associe_id obligatoire, carte_id interdit. PAY_005 : associe_id et carte_id interdits, statut_controle=A_CONTROLER obligatoire. PAY_006 : hors périmètre APP-3b.
- D-AM-5 : Avance liquide personnelle (PAY_002 + associe) non représentable dans REF_Assoc_Mode à ce stade — jamais détourner PAY_002 pour ce cas.
- D-AM-6 : Unicité garantie : assoc_mode_id, (mode_paiement_id, associe_id), assoc_mode — trois clés distinctes.
- D-AM-7 : Migration réelle gated par cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED = False. Copie de diagnostic disponible dans DATA_DIR/dryruns ou $env:TEMP.
- D-AM-8 : La migration réelle (--execute --confirm MIGRER_REF_ASSOC_MODE) est préparée mais non exécutée en APP-3b-0. Activation explicite du flag requise.

---

### D-APP-3B-0-BIS -- Migration reelle REF_Assoc_Mode executee (2026-07-05)

Statut : APPLIQUEE -- 2026-07-05.

- D-AM-R1 : Migration reelle executee manuellement. Resultat verifie par diagnostiquer() : feuille_presente=true, feuille_coherente=true, migration_necessaire=false.
- D-AM-R2 : Hash avant (sauvegarde) : 3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8
- D-AM-R3 : Hash apres (REF_Setup.xlsm) : c5a544e6a73f2815fbbec7ee0b2777c230085086d3f417bf42c4747ce9a78d9a
- D-AM-R4 : VBA vbaProject.bin inchange : 09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2
- D-AM-R5 : Sauvegarde conservee dans 99_ARCHIVES/APP3B0_REF_ASSOC_MODE_20260705_182335/ -- jamais modifier ni stager.
- D-AM-R6 : flag REF_ASSOC_MODE_REAL_WRITE_ENABLED -- active temporairement et explicitement pour l'execution unique de la migration reelle, puis remis a False apres controle post-migration.
- D-AM-R7 : Aucun pipeline Lot3/9/10/11/12 lance, aucune charge creee.
- D-AM-R8 : 27 feuilles historiques preservees + REF_Assoc_Mode (28 total). Table tblRefAssocMode presente.


### D-APP3B1-01 — Prévisualisation charge : copie uniquement sous DRYRUNS_DIR
Date : 2026-07-05 | Statut : VALIDÉ | Lot : APP-3b-1
Décision : La prévisualisation de saisie charge ne touche jamais SAISIE_Charges_Flux.xlsx. Elle crée
une copie sous 05_APPLICATION/data/dryruns/{token}/. Aucune route de confirmation réelle n'existe.
Le flag CHARGES_REAL_WRITE_ENABLED reste False tant que APP-3b-2 (écriture réelle) n'est pas validé.

### D-APP3B1-02 — ASSOC_MODE résolu exclusivement depuis REF_Assoc_Mode (table fermée)
Date : 2026-07-05 | Statut : VALIDÉ | Lot : APP-3b-1
Décision : L'ASSOC_MODE entrant dans le charge_id est résolu par lookup (mode_paiement_id, associe_id) →
REF_Assoc_Mode. Si aucune correspondance : erreur V10_ASSOC_MODE_NON_RESOLVABLE, saisie refusée.
Jamais de saisie libre de l'ASSOC_MODE.

### D-APP3B1-03 — Séquence NNN calculée sur préfixe CHG-{AAAA}-{MM}-{IMPACT}-{ASSOC_MODE}
Date : 2026-07-05 | Statut : VALIDÉ | Lot : APP-3b-1
Décision : NNN = (count des charge_id existants commençant par {prefix}-) + 1, sur 3 chiffres.
Calculé en lecture seule sur SAISIE_Charges_Flux.xlsx à l'instant de la prévisualisation.

---

## MODÈLE CHARGES & RÈGLEMENTS (préparation APP-3b-2, décisions cadre)

> Décisions issues de l'audit de confrontation modèle charges vs pipeline réel (2026-07-06).
> Elles cadrent la refonte APP-3b à venir. Aucune n'est implémentée dans ce commit documentaire.

### D-CHG-MODELE-01 — Une charge est toujours une dépense réelle ou une dette à régler
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Une charge manuelle représente toujours soit une dépense réellement engagée, soit une
dette fournisseur réelle à régler. Elle doit pouvoir être rapprochée avec : un débit bancaire (Lot 8),
une sortie de caisse, une dette envers associé puis son remboursement, ou un paiement fournisseur ultérieur.
Le rapprochement ne crée JAMAIS une seconde charge — il lettre un mouvement de trésorerie existant à la charge.
Ne sont pas des charges standard (parcours dédiés) : transfert banque↔caisse, avance associé et son
remboursement, acompte propriétaire (Lot 5), paiement direct propriétaire, main-d'œuvre interne M04 (Lot 6b),
AirCover (D042), remboursement voyageur.

### D-CHG-MODELE-02 — Identité de la charge découplée de son règlement futur
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : L'identité d'une charge (categorie, montant, impact, périmètre) doit être découplée de son
règlement futur (qui paie, quand, par quel moyen). Une charge peut exister avant d'être payée (cas 4 :
facture fournisseur non encore réglée). Une charge peut recevoir plusieurs règlements ; un règlement peut
couvrir plusieurs charges (cas 5). La nomenclature actuelle `charge_id = CHG-{AAAA}-{MM}-{IMPACT}-{ASSOC_MODE}-{NNN}`
encode l'ASSOC_MODE (donc le mode de paiement) dans l'identité : c'est une contrainte à lever pour supporter
la dette fournisseur. Le futur objet Règlement et le lettrage sont distincts de l'objet Charge.

### D-CHG-MODELE-03 — PAY_006 reste DIRECT_PROPRIETAIRE — jamais réutilisé pour une dette fournisseur
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Le mode de paiement PAY_006 = DIRECT_PROPRIETAIRE est réservé au paiement direct propriétaire.
Il ne doit JAMAIS être détourné pour représenter une dette fournisseur « à payer ». Une future dette
fournisseur nécessitera un nouveau mode de paiement distinct, à nommer après audit dédié (ne pas anticiper
le code ni le libellé dans ce commit). Interdiction de recycler un mode existant pour changer sa sémantique.

### D-CHG-MODELE-04 — Lettrage charge ↔ règlement = source métier contrôlée, pas SQLite seul
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Le futur lettrage charge ↔ règlement (table de liaison N-N permettant paiements partiels,
règlements groupés, remboursements d'associés et sorties de caisse) doit être une source métier contrôlée
et traçable (fichier / MASTER Excel versionné, comme les autres sources du pipeline), et non une vérité
stockée uniquement dans SQLite. SQLite reste réservé à l'audit applicatif et aux outrepassages, jamais au
calcul métier ni à la vérité de rapprochement.

### D-CHG-MODELE-05 — Clé de répartition ménages inchangée = COUT_STANDARD_MENAGES_MOIS
Date : 2026-07-06 | Statut : VALIDÉ (rappel D076/D103) | Lot : cadre APP-3b
Décision : La clé de répartition des charges affectables aux ménages reste `COUT_STANDARD_MENAGES_MOIS` :
`poids_ligne = nb_menages × cout_standard_menage(type_logement)`, puis
`quote_part = montant_pool × poids_ligne / Σ poids_pool`. Aucune nouvelle clé n'est créée. La refonte APP-3b
réutilise strictement cette clé existante (implémentée lot6f_cout_complet_menages.py). Une charge ménage
utilise un seul mode : soit MENAGE_INTERVENANT, soit MENAGE_LOGEMENTS, jamais les deux (anti double-allocation).
Les heures de ménage interne (M04, TYPE_FLUX_013) ne sont JAMAIS saisies comme charge manuelle (D027/D105).
Le ménage externe garde son circuit unique Lot6c (facture prestataire) — pas de second circuit Charges.

### D-CHG-MODELE-06 — Profils d'impact des catégories de charge
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Chaque catégorie de charge porte un profil d'impact qui déclenche un comportement réel (et non
un simple libellé) : GLOBAL (aucun logement ciblé, aucune répartition), LOGEMENT_DIRECT (un logement précis,
pas de clé ménage), MENAGE_INTERVENANT (réparti sur les ménages d'un intervenant via la clé D-CHG-MODELE-05),
MENAGE_LOGEMENTS (réparti sur un ou plusieurs logements sélectionnés via la même clé), PARCOURS_DEDIE
(hors formulaire standard : AirCover, incident voyageur, avance, acompte…). Multi-logements = N lignes filles
ventilées, jamais une cellule multi-logements (cohérence grain lot9/lot10 : une ligne = un logement).

### D-CHG-MODELE-07 — Catégorie personnalisée = profil GLOBAL forcé, libellé libre séparé
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : `categorie_charge_id` reste une valeur fermée (préserve les listes déroulantes Excel, la validation
V04, le routage lot6f et les analyses). Pour une charge sans catégorie existante adaptée, un futur code fermé
dédié (pressenti CHG_024 — AUTRE_PERSONNALISEE) sera utilisé, avec le libellé personnalisé stocké dans un champ
séparé (jamais un libellé libre dans `categorie_charge_id`, qui casserait la validation et le routage ménage).
Une catégorie personnalisée est TOUJOURS traitée en profil GLOBAL forcé : jamais ménage, logement direct,
incident voyageur, AirCover, avance associé ni aucun parcours spécialisé. Verrous d'anti-contournement :
`affectable_menage=NON`, `reservation_id` interdit, `refacturable=NON` forcés sur ce chemin.

### D-CHG-MODELE-08 — HR retiré de la saisie charge standard
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Le formulaire « Nouvelle charge » standard n'expose que deux impacts métier : « Impacte le résultat
réel et comptable » (→ IC → prise_en_compta = OUI) et « Impacte le résultat réel, hors compta »
(→ HC → prise_en_compta = NON). `HR` (hors résultat) est retiré du formulaire standard : un flux sans impact
résultat n'est pas une charge dans ce modèle. Les rares cas exigeant HR (ex. CHG_014 Avance, impact_resultat=NON)
relèvent d'un parcours dédié, hors formulaire standard. `prise_en_compta` n'est jamais saisi directement :
il est dérivé de l'impact choisi (D-APP-2B-REV1, D012).

### D-CHG-MODELE-09 — Charges refacturables bloquées tant que Lots 10/12 non corrigés
Date : 2026-07-06 | Statut : VALIDÉ | Lot : cadre APP-3b
Décision : Aucune écriture réelle de charge marquée `refacturable=OUI` n'est autorisée tant que la chaîne de
refacturation propriétaire n'est pas corrigée de bout en bout (Lot 10 : terme `charges_exceptionnelles_refacturees`
dans `montant_du_conciergerie` ; Lot 12 : ligne préfacture CHARGES_EXCEPT_REFAC, affichée avant MONTANT_DU). La
correction pipeline lot10/lot12 (formule D033/D034) précède l'ouverture de la refacturation à la saisie réelle.
Voir trace de correction associée dans JOURNAL_CONTROLES (CTR-REFAC-LOT10-12) et l'ordre d'affichage D-PREF-ORDRE-01.

### D-PREF-ORDRE-01 — Ordre d'affichage préfacture propriétaire (supersède le verrou « 12 lignes fixes »)
Date : 2026-07-06 | Statut : VALIDÉ | Lot : Lot 12 / APP-3b
Décision : La préfacture propriétaire (FACT_FACTURE_LIGNES) n'a plus une numérotation fixe à 12 lignes. L'ordre
suit la lecture propriétaire et la numérotation est séquentielle : **12 lignes sans supplément canapé, 13 avec**.
Ordre : TOTAL_PAYOUT, MENAGE_FACTURE, COMMISSION_CONCIERGERIE, [PREPARATION_CANAPE si applicable], CHARGE_FIXE,
REVENU_NET_EXPLOITATION (bloc EXPLOITATION) ; puis CHARGES_EXCEPT_REFAC, MONTANT_DU, ACOMPTE_AIRBNB,
PAIEMENT_DEJA_RECU, ACOMPTES_PROPRIETAIRES, RESTE_A_PAYER, STATUT_REGLEMENT (bloc REGLEMENT).
Règles : `CHARGES_EXCEPT_REFAC` est affichée AVANT `MONTANT_DU` car elle explique ce montant ; `PREPARATION_CANAPE`
apparaît une seule fois (montant déjà inclus une seule fois dans `MONTANT_DU`, calculé par lot10) ;
`RESTE_A_PAYER` est toujours après tous les acomptes/paiements ; `STATUT_REGLEMENT` est toujours la dernière ligne.
Cette décision révise le libellé « 12 lignes fixes / §17.3 » de D040 et D-P5 : la cohérence de lecture propriétaire
prime sur l'ancien verrou artificiel de numérotation. Les blocs EXPLOITATION / REGLEMENT restent séparés (D033).
Périmètre : `02_TRAVAIL/lot12_generer_factures.py` (build_facture_lignes). Lot 10, Excel, flags, APP-3b inchangés.

### D-CHG-TYPEFLUX-01 — Dérivation type_flux_id (jamais choisi par l'utilisateur) + TYPE_FLUX_020
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : L'utilisateur ne choisit jamais `type_flux_id`. Le service le déduit ainsi (matrice validée) :
- **Types spécifiques prioritaires** : CHG_016 → TYPE_FLUX_012 ; CHG_010 → TYPE_FLUX_016 ;
  CHG_008 ou CHG_011 avec `refacturable=OUI` → TYPE_FLUX_011.
- **Sinon, déterminé par le règlement** : PAY_001 → **TYPE_FLUX_020** ;
  PAY_002 + montant récupéré=OUI → TYPE_FLUX_008 ; PAY_002 sinon → TYPE_FLUX_004 ;
  PAY_003 → TYPE_FLUX_004 ; PAY_004 → TYPE_FLUX_004.
- **Interdits en Nouvelle charge standard** : PAY_005 (A_DEFINIR), PAY_006 (DIRECT_PROPRIETAIRE — aucune sortie
  d'argent conciergerie).
`TYPE_FLUX_002` (DEPENSE_PERSO_COMPTE_PRO) n'est jamais utilisé pour une charge normale payée banque pro : sa
sémantique est une dépense **personnelle**. Nouveau **TYPE_FLUX_020 = CHARGE_SOCIETE_COMPTE_PRO** (code_impact_defaut
IC, comptabilisable_defaut OUI, actif OUI) créé pour ce cas.
`CHG_024` : `type_flux_id` déterminé uniquement par le règlement ; **jamais TYPE_FLUX_011**.
Impact résultat : l'utilisateur choisit IC ou HC, prioritaire sur `code_impact_defaut` du type. Si le choix diffère
du défaut du type, un **commentaire de justification devient obligatoire**.

### D-CHG-DEDIE-01 — Reclassement de 4 catégories en PARCOURS_DEDIE
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : CHG_012 (remboursement voyageur), CHG_013 (salaire associée), CHG_015 (remboursement frais associée) et
CHG_019 (sinistre / dégât logement) passent `famille_impact_categorie = PARCOURS_DEDIE` (profils_impact_autorises =
PARCOURS_DEDIE). Ils rejoignent CHG_001/002/014/020/021/022 hors formulaire Nouvelle charge standard. CHG_003/004/018/023
restent famille MENAGE (parcours ménage dédié). Catégories GLOBAL standard restantes : CHG_005/006/007/008/009/010/011/016/017/024.
Migration `lst_TypesFlux_Lot3` (SAISIE) : ajout de TYPE_FLUX_016 et TYPE_FLUX_020.
Périmètre : migration contrôlée (REF_Setup.xlsm + SAISIE_Charges_Flux.xlsx) avec backup/hash/VBA préservé.
Voir trace JOURNAL_CONTROLES (CTR-TYPEFLUX-MIGRATION-01).

---

## MODULE NOUVELLE CHARGE GUIDÉE (saisie assistée déterministe)

### D-CHG-GUIDE-01 — Une charge = une seule charge économique, N impacts analytiques
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Toute charge saisie représente une seule dépense économique réelle. Elle n'est JAMAIS dupliquée
parce qu'elle est ventilée analytiquement. Une charge de 100 € reste une charge de 100 € : elle peut porter
plusieurs impacts analytiques (multi-logements, multi-propriétaires, ménage) mais jamais plusieurs dépenses
réelles de 100 €. La ligne SAISIE reste unique ; les ventilations (quotes-parts, périmètre, réserve) sont des
structures analytiques portées par le manifest de prévisualisation, **jamais** une liste d'identifiants
concaténée dans une cellule métier.

### D-CHG-GUIDE-02 — Périmètre analytique non ménage (déterministe)
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Périmètre final logements = logements sélectionnés directement + logements ACTIFS des propriétaires
sélectionnés (source `REF_Gestion_Logements_Hist`, statut ACTIF + dates englobant le mois) − doublons. Sans
ciblage : charge globale conciergerie. Le montant est réparti **également** entre les logements finaux avec
gestion déterministe des centimes (les premiers logements triés reçoivent le centime résiduel) ; la somme des
quotes-parts est TOUJOURS égale au montant réel. Le montant n'est jamais répliqué intégralement sur chaque
logement. Toute autre clé de répartition non ménage devrait être documentée explicitement.

### D-CHG-GUIDE-03 — Impact ménage (analytique, jamais seconde charge)
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Une charge ménage est une vraie charge économique ET porte un second effet **exclusivement
analytique** : elle alimente le coût complet ménage et l'analyse gain/perte ménage. Elle ne crée jamais une
seconde charge réelle ni comptable, ne double jamais le coût payé, et n'est jamais refacturable. La répartition
ménage se fait soit par un ou plusieurs **intervenants**, soit par un ou plusieurs **logements** (propriétaires
utilisables en filtre), **jamais les deux à la fois**. La clé de référence reste `COUT_STANDARD_MENAGES_MOIS`
(D076/D103). La ventilation quantitative réelle relève de Lot6f : la prévisualisation établit le périmètre et
l'intention, marque la charge à contrôler, et ne répartit jamais arbitrairement des montants ménage.
Catégories : impact ménage FORCÉ pour Achat ménage (CHG_004), Blanchisserie (CHG_003), Supplément ménage
(CHG_027) ; au CHOIX pour Déplacement (CHG_009), Repas (CHG_025), Achat divers (CHG_018), Prestation diverse
(CHG_026) ; INTERDIT pour logiciels, frais bancaires, assurance, charge générale, maintenance, forfait client.
M04 reste intégralement automatique/analytique (jamais charge manuelle). Le ménage externe garde son circuit
Lot6c ; ce parcours ne recrée jamais une facture prestataire déjà traitée.

### D-CHG-GUIDE-04 — Réserve de facturation (refacturable)
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : `refacturable = OUI` ne facture jamais automatiquement. Une charge non ménage n'est refacturable que
si le périmètre final comprend au moins un logement cible. Cocher refacturable crée une **réserve de
facturation** : une entrée par quote-part (jamais dupliquée), portant charge, montant refacturable, propriétaire,
logement, mois, libellé, justificatif, statut EN_ATTENTE et décisions futures possibles (APPLIQUER / REPORTER /
IGNORER). La somme des quotes-parts réservées = montant total réellement refacturable. Une charge répartie sur
plusieurs logements ne reporte jamais son montant entier sur chaque préfacture. La mise en œuvre de la
facturation (application/report/ignore) est préparée mais non exécutée : aucune préfacture n'est modifiée du seul
fait qu'une charge devient refacturable. Une charge ménage n'est jamais refacturable.

### D-CHG-GUIDE-05 — Avantage associé distinct du moyen de paiement
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Le choix « Avantage associé ? » (Oui/Non) apparaît pour Déplacement, Repas, Achat divers, Prestation
diverse (IK reste hors formulaire — circuit Lot7, D026 préservé). Si Oui : sélection d'un associé bénéficiaire
**obligatoire**, dans un champ DISTINCT du mode de paiement (`avantage_associe_id` ≠ `associe_id` du paiement).
Un paiement par carte/compte personnel associé n'implique pas un avantage ; une charge banque pro peut en
constituer un. L'avantage alimente le suivi des avantages associés (traçable), cohérent avec Lot7.

### D-CHG-GUIDE-06 — Catégories visibles + Forfait client hors saisie
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Le formulaire Nouvelle charge n'affiche que des libellés métier (jamais type_flux_id, sens_flux,
statut_controle, niveau_anomalie, prise_en_compta). Groupes : Logiciels (CHG_005/006/007) ; Charges courantes
(CHG_010 Frais bancaires, CHG_011 Assurance, CHG_009 Déplacement, CHG_025 Repas, CHG_018 Achat divers,
CHG_026 Prestation diverse, CHG_008 Maintenance, CHG_017 Charge générale) ; Ménages (CHG_004 Achat ménage,
CHG_003 Blanchisserie, CHG_027 Supplément ménage) ; Autre (CHG_024 personnalisée). **CHG_016 Forfait client
logiciel/consommables N'EST PLUS une charge saisissable** : c'est une ligne de facturation propriétaire
(déjà portée par la préfacture Lot12, ligne applicable ; référentiel `forfait_logiciel_consommables_mensuel`
dans REF_Logements + REF_Charges_Recurrentes REC_001). CHG_023 (forfait cave récurrent) reste hors saisie
(REF_Charges_Recurrentes). Les catégories PARCOURS_DEDIE et ménage externe / M04 restent hors formulaire.
Reclassement documenté : CHG_018 (Petit équipement) passe MENAGE→GLOBAL (« Achat divers », impact ménage au
choix). Nouvelles catégories CHG_025 (Repas), CHG_026 (Prestation diverse), CHG_027 (Supplément ménage) créées
par migration contrôlée (voir CTR-CHG-GUIDE-MIGRATION-01).

### D-CHG-GUIDE-07 — IK hors Nouvelle charge (circuit Lot7 préservé)
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Les indemnités kilométriques (IK) NE sont PAS ajoutées à Nouvelle charge. Elles restent traitées par
leur circuit dédié Lot7 (`MASTER_FACT_MAN_IK_Avantages`), conformément à l'exclusion D026 (SAISIE_Charges_Flux
exclut IK et virements associés). L'avantage associé du formulaire couvre les autres catégories éligibles.
Cette décision évite tout double comptage IK et préserve D026.

### D-CHG-GUIDE-08 — Persistance durable des impacts (source de vérité SAISIE_Charges_Impacts.xlsx)
Date : 2026-07-07 | Statut : VALIDÉ | Lot : APP-3b
Décision : Les impacts analytiques d'une charge ne vivent plus seulement en prévisualisation (manifest/JSON) :
ils ont une **source de vérité Excel durable**, `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Impacts.xlsx`, source de
SAISIE distincte des masters de calcul (jamais régénérée par un pipeline). Trois onglets normalisés liés par
`charge_id`, chacun avec identifiant unique, mois, périmètre, montant/quote-part, statut, origine, commentaire,
ROW_HASH :
- **AFFECTATIONS** : ventilation analytique multi-logements/propriétaires (une ligne par quote-part ; somme = montant).
- **MENAGE** : sélections/impacts ménage (mode INTERVENANT ou LOGEMENT, jamais les deux), pour Lot6f.
- **RESERVE_REFACTURATION** : réserve de charges refacturables en attente (statut EN_ATTENTE), lue plus tard par Lot12.
Les **avantages associés** sont PORTÉS PAR LA CHARGE (colonne `avantage_associe_id` de
`SAISIE_Charges_Flux`, distincte du paiement) et **JAMAIS ressaisis dans Lot7 SOURCE_SAISIE** (qui est
strictement résiduelle — « NE PAS RESSAISIR » les charges Lot3). L'agrégation Lot7 (générateur Python
`lot7_generateur_avantages.py`, logique `lib_avantages`) attribue l'avantage par bénéficiaire directement
depuis SAISIE_Charges_Flux — voir D-CHG-GUIDE-09.
Règles : une charge économique reste unique (Lot3) ; les ventilations ne sont jamais des doubles charges ; une
charge ménage n'a jamais de ligne RESERVE ; un avantage n'est écrit qu'une fois par charge (dédup `lien_origine`) ;
jamais de liste d'identifiants concaténée. **Écriture réelle interdite tant que CHARGES_REAL_WRITE_ENABLED = False** :
la persistance s'effectue sur COPIE contrôlée (dry-run), idempotente par `charge_id` (réécrire remplace, ne duplique
jamais). L'application/report/ignore d'une réserve en préfacture reste préparée mais NON automatique (Lot12 futur).
Voir CTR-CHG-PERSIST-01.

### D-CHG-GUIDE-09 — Avantage porté par la charge + Power Query Lot7 étendu (verrou résolu)
Date : 2026-07-08 | Statut : VALIDÉ | Lot : APP-3b / Lot7
Décision (corrige D-CHG-GUIDE-08 sur les avantages) : **aucune écriture dans SOURCE_SAISIE Lot7 pour une charge
existant déjà dans SAISIE_Charges_Flux** (SOURCE_SAISIE est strictement résiduelle). L'avantage est porté
uniquement par la charge via `avantage_associe_id` :
- `avantage_associe_id` vide = pas d'avantage ;
- `avantage_associe_id` renseigné = **totalité du montant de la charge** attribuée à cet associé ;
- PAY_003 / PAY_004 ne créent JAMAIS automatiquement un avantage ;
- PAY_001 peut créer un avantage si `avantage_associe_id` est renseigné.
Le générateur **Python `lot7_generateur_avantages.py`** (Option A) produit `avantage_brut_depenses_perso`
depuis SAISIE_Charges_Flux (Lot3) avec priorité déterministe :
1. si `avantage_associe_id` renseigné → ce bénéficiaire (tout moyen de paiement) ;
2. sinon → règle historique `TYPE_FLUX_002` par `associe_id` (traitement inchangé) ;
jamais les deux voies pour une même charge ; une même `charge_id` jamais comptée deux fois ; génération
idempotente. `TYPE_FLUX_004` / `TYPE_FLUX_008` (charges_payees_pour_societe) inchangés. L'onglet
POWER_QUERY_CODE reste **documentaire** (aucun Power Query vivant : ni connections.xml, ni DataMashup).
Preuve principale : `tests/test_lot7_generateur_avantages.py` (5 cas + idempotence, commit d5da30d) —
charge PAY_001 100 € + avantage_associe_id → 100 € exactement une fois pour le bon associé ; seconde
exécution → toujours 100 €, jamais 200 €. Preuve complémentaire OPTIONNELLE (non nécessaire au pipeline) :
`tests/test_pq_avantage_lot7_reel.py` (Excel COM, skip si Excel indisponible) ; M-code documentaire
`02_TRAVAIL/lot7_pq_avantages.py`. Voir CTR-CHG-AVANTAGE-PQ-01.

### D-CHG-GUIDE-10 — Suivi associé HR (Lot7B, compte courant associé)
Date : 2026-07-11 | Statut : VALIDÉ | Lot : Lot7B
Décision : `MASTER_CALC_AVANTAGES` (clé `associe_id` + `mois`) EST le **suivi associé**. Il porte
`AvantageNet = AvantagesBruts + IK − ChargesPayéesPourSociété` (D011) et 4 colonnes de suivi :
`code_impact = HR` (strict), `source_calcul = LOT7`, `sens_suivi`, `associe_nom`.
- **Dimension ASSOCIÉ uniquement.** N'impacte JAMAIS : net propriétaire, résultat conciergerie
  (réel ni comptable), préfactures. **Interdiction de brancher dans Lot10 / Lot12.**
- **Pas un règlement** : aucun virement, aucun mouvement de trésorerie, aucun solde réel. Suivi
  *calculé* uniquement. Le solde par virements/remboursements associés + le lettrage = **futur** circuit.
- `sens_suivi` ∈ {A_CONTROLER_POSITIF, A_CONTROLER_NEGATIF, SOLDE_NUL} — convention PRUDENTE : le sens
  « à payer / à rembourser » n'est PAS tranché ici.
- Un avantage associé n'est jamais transformé en charge ni en produit ; un remboursement associé n'a
  jamais d'impact résultat (HR/neutralise).
Contrôles : `02_TRAVAIL/lib_controles_avantages.py` (Lot11). Voir CTR-CHG-SUIVI-ASSOCIE-01.

### D-REF-HIST-01 — Groupes de logements historisés : concept absent ; corrigé par la vraie règle (répartition par facture)
Date : 2026-08-23, complétée 2026-08-24 | Statut : VALIDÉ | Lot : Mission 6 / Mission 6 bis (règles/variables temporelles)

**Complément 2026-08-24 (Mission 6 bis)** : la formulation initiale ci-dessous documentait
correctement l'absence de groupe PERMANENT, mais laissait à tort penser qu'un mécanisme de
répartition manquait. Un mécanisme réel existe et a été retrouvé et prouvé par des tests
(`charges_impact_service.py`, préexistant, non modifié) : le périmètre d'une charge non
directement attribuable vient des logements sélectionnés à SA création — sélection directe et/ou
logements ACTIFS d'un propriétaire sélectionné, résolus au MOIS de la charge via `gestion_active_
pour_mois` (déjà daté, réutilise `ref_gestion_logements_hist`). La répartition elle-même est
`repartir_egal` : parts strictement égales entre les logements du périmètre, arrondi au centime,
reliquat aux premiers logements triés. Aucun groupe mémorisé nulle part — chaque charge porte son
propre périmètre. Voir `REGLES_METIER_TEMPORELLES.md` §9.1 pour le détail complet et les tests
(`test_repartition_charge_commune_facture.py`).

Décision : la mission « règles et variables métier historisées » demandait de prouver l'historisation
de la composition de « groupes de logements » (exemple donné : GROUPE A passant de 3 à 4 appartements
entre 2026 et 2027, une charge devant toujours utiliser la composition en vigueur à sa date). L'audit
n'a trouvé **aucune trace** de ce concept dans le code actuel — ni table `ref_groupes*`, ni notion de
« groupe nommé à membership persistant » dans `02_TRAVAIL/` ni `05_APPLICATION/`. Le mécanisme le plus
proche (`lot6f_cout_complet_menages.py`, POOLS `LOCAL_CAVE`/`COURSES`/`CONSOMMABLES`) n'est PAS un
groupe à composition versionnée : il ventile sur les ménages RÉELLEMENT effectués le mois calculé —
déjà daté par construction, sans risque de réécriture du passé, mais ce n'est pas la même chose qu'un
groupe nommé dont la liste de membres change dans le temps.

**Décision : ne pas inventer ce concept.** Construire un schéma « groupes de logements historisés »
sans qu'aucune règle métier réelle ne l'utilise aujourd'hui créerait une structure spéculative — exactement
ce que la mission interdit (§15/§25 : ne pas deviner une réponse, ne pas construire de framework
abstrait sans besoin réel identifié). Si un vrai besoin métier de « groupe de logements » existe
(quelle charge, quelle règle de répartition, quel périmètre exact), il doit être cadré explicitement
avant tout développement — cette décision documente l'absence, pas un refus définitif.

Ce qui A été traité dans la même mission (référentiels réellement présents avec le même défaut
générique — valeur courante sans période) : le paramètre canapé (`ref_canape_parametres`, migration
0058) et l'audit confirme que taux de commission / gestion logement↔propriétaire / coût ménage
standard étaient déjà historisés (missions précédentes). Voir `REGLES_METIER_TEMPORELLES.md`.

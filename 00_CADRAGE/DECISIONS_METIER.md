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
  - Table `FACT_FACTURE_LIGNES` : les 12 lignes de §17.3, avec type_ligne, libellé, montant, bloc (exploitation/règlement) ;
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

### D047 — taux_commission depuis référentiel prioritaire, fallback contrôlé (QM-L4-02)
Date : 2026-06-09 | Statut : VALIDÉ — VERROUILLÉ
Décision : `taux_commission` est pré-rempli par VLOOKUP depuis `REF_Proprietaires.taux_commission`.
  - Si trouvé → `taux_commission_source = REF_PROPRIETAIRE`.
  - Si absent du référentiel → formule vide, `taux_commission_source = A_CONTROLER`.
  - Saisie manuelle autorisée en fallback : `taux_commission_source = SAISIE_MANUELLE`.
Aucun taux ne peut être deviné ou codé en dur. Colonne `commentaire_taux_commission` obligatoire
si `taux_commission_source = SAISIE_MANUELLE`.
Tables : SAISIE_ReservationsHorsHostaway.xlsx, REF_Proprietaires (REF_Setup.xlsm)

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
Décision : Pour la branche HA, `logement_id` est obtenu via JOIN sur `REF_Mapping_Logements` (colonne `listingMapId`), actifs uniquement. `proprietaire_id` est obtenu via JOIN sur `REF_Logements` depuis `logement_id`. Pour la branche HH, les deux champs viennent directement de la saisie (Lot 4).
  Contrôle `RESERVATION_LOGEMENT_NON_MAPPE` (A_CONTROLER) : `listingMapId` absent de `REF_Mapping_Logements` actifs.
  Contrôle `RESERVATION_MAPPING_MULTIPLE` (A_CONTROLER) : plusieurs lignes actives pour un même `listingMapId` dans `REF_Mapping_Logements`.
Tables : MASTER_CALC_Reservations, REF_Mapping_Logements, REF_Logements

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

### D105 — Vue analytique uniquement
Date : 2026-06-18 | Statut : VALIDÉ
Les vues gain/perte (Lot 6e) et coût complet (Lot 6f) sont **analytiques** : ne réinjectent RIEN dans MASTER_CALC_Flux / Resultats / Commissions / NetProprietaire / Factures. Lecture seule des charges déjà comptées une fois.

### D106 — Coût complet ménage avancé
Date : 2026-06-18 | Statut : VALIDÉ
- **Cave/local** (REC_002) = ventilée **uniquement sur les ménages INTERNES** (jamais les externes), clé D103, date-aware. Contrôle `REC_002_LOCAL_CAVE_INTERNE_ONLY`.
- **Lavage interne** = source **Google Sheet / M04** (payé par les intervenantes internes). **Jamais exporté** vers SAISIE_Charges_Flux. Si saisi un jour dans SAISIE → `affectable_menage = NON` (sauf décision contraire). Contrôle `DOUBLE_SOURCE_LAVAGE_A_CONTROLER`.
- **Courses / consommables / achats** = `SAISIE_Charges_Flux` **uniquement si `affectable_menage = OUI`**. Affectation via `intervenant_concerne` (MENAGE_INTERNE / COMMUN_MENAGE / INT_xxxx / multi `INT_x;INT_y`). SAISIE vide → pools = 0 `POOL_VIDE_NON_SAISI` (non saisi ≠ inexistant).
- **Abandon** de la logique `fournitures_incluses`.
- Colonnes ajoutées à SAISIE_Charges_Flux : `affectable_menage`, `intervenant_concerne`.
Tables : REF_Couts_Menage_Interne, REF_Intervenants (mapping Hostaway), SAISIE_Charges_Flux, MASTER_CTRL_Rapprochement_Menages, MASTER_CALC_GainPerte_Menages, MASTER_CALC_CoutComplet_Menages. Lots 6d/6e/6f.

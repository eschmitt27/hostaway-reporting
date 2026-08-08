# 70 — Matrice d'arbitrages comptables

Aucune règle comptable modifiée par cette mission. Décisions distinguées explicitement par nature
(technique / métier / comptable / fiscale) — Claude ne fournit aucun conseil fiscal définitif.

> **Mise à jour 2026-08-08** — cette mission joue le rôle de `MATRICE_ARBITRAGES_COMPTABLES.md`
> demandée : la matrice ci-dessous a été reconstruite de façon exhaustive (catalogue réel
> `REF_Categories_Charges`, 27 catégories, + catégories bancaires + trésorerie propriétaires),
> avec séparation explicite RÈGLE MÉTIER / NUMÉRO DE COMPTE / RÈGLE FISCALE. Constat de fond :
> **exactement 7 comptes existent dans le plan comptable applicatif** (`401000`, `411000`,
> `512000`, `530000`, `467000`, `606000`, `706000` — vérifié directement dans les migrations
> `0021`/`0023`, aucun autre). Aucune règle `mapping_comptable_regles` de portée `CATEGORIE` ou
> `TYPE_FLUX` n'est actuellement `VALIDE` en base (une seule ligne seedée : le filet générique
> `606000` en portée `PROVISOIRE_GENERIQUE`). Conséquence : **toute charge, quelle que soit sa
> catégorie, résout aujourd'hui sur `606000` PROVISOIRE** — ce n'est pas un défaut du mécanisme
> (`resoudre_compte()` est fonctionnel et testé), c'est l'absence totale de règles arbitrées.

## A. Catégories de charges (`REF_Categories_Charges`, 27 lignes réelles) → 606000

Toutes résolvent aujourd'hui sur `606000` (PROVISOIRE, filet générique — aucune règle `VALIDE`
seedée pour aucune catégorie). Aucun compte plus précis n'existe dans le référentiel pour aucune
d'entre elles (7 comptes au total, tous déjà listés plus haut, aucun n'est une charge sectorielle).

| Code | Catégorie | Impact résultat (référentiel) | Compte actuel | Statut |
|---|---|---|---|---|
| CHG_001 | Ménage externe | OUI | 606000 | PROVISOIRE |
| CHG_002 | Ménage interne | OUI | 606000 | PROVISOIRE |
| CHG_003 | Blanchisserie | OUI | 606000 | PROVISOIRE |
| CHG_004 | Consommables / produits logement | OUI | 606000 | PROVISOIRE |
| CHG_005 | Logiciel — Hostaway | OUI | 606000 | PROVISOIRE |
| CHG_006 | Logiciel — Dynamic pricing | OUI | 606000 | PROVISOIRE |
| CHG_007 | Logiciel — Autres | OUI | 606000 | PROVISOIRE |
| CHG_008 | Maintenance / réparation logement | OUI | 606000 | PROVISOIRE |
| CHG_009 | Déplacement | OUI | 606000 | PROVISOIRE |
| CHG_010 | **Frais bancaires** | OUI | 606000 | PROVISOIRE (voir section C) |
| CHG_011 | Assurance | OUI | 606000 | PROVISOIRE |
| CHG_012 | Remboursement / geste commercial voyageur | OUI | 606000 | PROVISOIRE |
| CHG_013 | Associées — Salaire | OUI | 606000 | PROVISOIRE |
| CHG_014 | Associées — Avance | NON (hors résultat) | 606000 si généré | PROVISOIRE — voir aussi 467000, section D |
| CHG_015 | Associées — Remboursement frais | OUI | 606000 | PROVISOIRE — voir aussi 467000, section D |
| CHG_016 | Forfait client logiciel/consommables | OUI | 606000 | PROVISOIRE |
| CHG_017 | Charge générale non affectée | OUI | 606000 | PROVISOIRE |
| CHG_018 | Achat petit équipement | OUI | 606000 | PROVISOIRE |
| CHG_019 | Sinistre / dégât logement | OUI | 606000 | PROVISOIRE |
| CHG_020 | À contrôler (provisoire par nature) | A_DEFINIR | 606000 | A_ARBITRER (catégorie elle-même non stabilisée) |
| CHG_021 | Incident voyageur | OUI | 606000 | PROVISOIRE |
| CHG_022 | Prestation AirCover refacturée | OUI | 606000 | PROVISOIRE |
| CHG_023 | Forfait local cave (ménage) | OUI | 606000 | PROVISOIRE |
| CHG_024 | Catégorie personnalisée (saisie libre) | OUI | 606000 | PROVISOIRE (catégorie libre par nature, jamais un compte dédié possible) |
| CHG_025 | Repas professionnel | OUI | 606000 | PROVISOIRE |
| CHG_026 | Prestation diverse | OUI | 606000 | PROVISOIRE |
| CHG_027 | Supplément ménage (analytique) | OUI | 606000 | PROVISOIRE |

**Volume réel** : non recalculé ce tour (audit documentaire, pas d'exécution pipeline) — cf.
`13_RESULTATS_CHIFFRES_AVANT_APRES.md` et rapports Charges antérieurs pour les volumes déjà
mesurés par catégorie.

## B. Catégories bancaires (moteur `lot8b`, 17 codes déterministes) — AUCUN pont comptable

Distinctes des `CHG_XXX` : ce sont des catégorisations de mouvement bancaire, jamais transformées
en `Charge` applicative aujourd'hui. Un mouvement classé `HONORAIRES_COMPTABLES` reste un mouvement
bancaire classifié, sans facture/charge/écriture générée — vérifié (`banques_classement_service.
decider()` : « Aucun — BANQUE_LOT8_IMPORT.xlsx n'est jamais modifié », « effet_reel: Aucun »).

| Code moteur | Nature métier connue | Pont vers Charge/Écriture | Statut |
|---|---|---|---|
| FRAIS_BANCAIRES | Charge professionnelle (CHG_010 confirme la nature) | Aucun | A_ARBITRER (compte + construction du pont) |
| HONORAIRES_COMPTABLES | Charge professionnelle (classification métier déjà **APPROUVÉE**, DEC-001) | Aucun | A_ARBITRER (compte + pont) |
| COTISATIONS_SOCIALES | Charge professionnelle (classification déjà **APPROUVÉE**, DEC-001) | Aucun | A_ARBITRER (compte + pont) |
| COTISATION_PREVOYANCE | Charge professionnelle (classification déjà **APPROUVÉE**, DEC-001) | Aucun | A_ARBITRER (compte + pont) |
| LOGICIEL_GESTION | Charge professionnelle (classification déjà **APPROUVÉE**, DEC-001 — proche CHG_005/006/007) | Aucun | A_ARBITRER |
| ACHAT_PERSO_CB | Dépense personnelle par carte — **jamais** classée charge professionnelle (DEC-001) | Compte 467000 documenté pour cet usage (voir section D) | A_ARBITRER (rattachement associé nominatif requis avant tout compte) |
| PAYOUT_PLATEFORME | Versement plateforme catégorisé (règle Banque définitive, cf. mission précédente) | Hors périmètre — jamais rapproché objet économique individuel | N/A (déjà tranché) |
| VIR_ASSOCIE | Virement associé (sens à démontrer avant toute nature) | Compte 467000 si nature confirmée | A_ARBITRER par mouvement (déjà en cours, groupes 4/5 Banque) |
| Autres (9 codes : ABONNEMENT_MOBILITE, DEPENSE_CB_A_CLASSIFIER, EFFET_DOMICILIE, FACTURE_PRESTATAIRE, IMPAYE, LOYER_LOCAL_A_CONTROLER, SANTE_A_CONTROLER, VIREMENT_PROPRIETAIRE_A_RAPPROCHER, VIREMENT_SORTANT_A_CONTROLER) | Classification bancaire seule, nature économique non tranchée en masse | Aucun | A_CONTROLER (déjà le statut assigné, cf. `73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md`) |

**Distinction importante** : « les frais bancaires sont une charge professionnelle » est une RÈGLE
MÉTIER déjà connue (CHG_010 + TYPE_FLUX_016 le prouvent). « Quel compte utiliser (ex. 627000) » est
une DÉCISION COMPTABLE non prise — aucun numéro n'a été choisi par cette mission.

## C. Frais bancaires (détail, TYPE_FLUX_016)

- Règle métier : **connue** — CHG_010 (charges) et TYPE_FLUX_016 (`FRAIS_BANCAIRES`, flux Lot9)
  prouvent la nature charge professionnelle. Confirmé au dernier cycle réel : 24 lignes TYPE_FLUX_016
  dans `MASTER_CALC_Flux` (cf. `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`).
- Numéro de compte : **A_ARBITRER**. Le plan comptable applicatif n'a aucun compte `627xxx` ou
  équivalent — ni proposé ni créé par cette mission.
- Construction du pont Banque→Charge→Écriture : **non construite** (aucun générateur consomme
  `TYPE_FLUX_016` aujourd'hui côté Comptabilité).

## D. Trésorerie propriétaires — sémantique des natures (aucun compte choisi)

| Nature | Effet économique connu | Résultat (réel/comptable) ? | Dette/créance ? | Compte nécessaire ? | Statut |
|---|---|---|---|---|---|
| ACOMPTE_PROPRIETAIRE | Sens déclaré manuellement (PROPRIETAIRE_VERS_SOCIETE ou inverse) — direction connue, qualification économique fine non documentée | A_CONTROLER | Non documenté explicitement | Candidat : `411000` (« créance/compensation », déjà commenté en ce sens dans la migration `0021`, jamais arbitré comme définitif pour cet objet précis) | A_ARBITRER |
| REMBOURSEMENT_PROPRIETAIRE | Idem | A_CONTROLER | Non documenté | Candidat : `411000` | A_ARBITRER |
| REGULARISATION_PROPRIETAIRE | Idem | A_CONTROLER | Non documenté | Candidat : `411000` | A_ARBITRER |
| COMPENSATION_PROPRIETAIRE | Idem — nom suggère un lien direct avec `411000` (« créance/compensation ») | A_CONTROLER | Non documenté | Candidat : `411000` | A_ARBITRER |
| AVANCE_PROPRIETAIRE | Idem | A_CONTROLER | Non documenté | Candidat : `411000` | A_ARBITRER |
| RESTITUTION_PROPRIETAIRE | Idem | A_CONTROLER | Non documenté | Candidat : `411000` | A_ARBITRER |
| AUTRE_A_CONTROLER | Nature non qualifiée par construction | A_CONTROLER | Non documenté | Aucun | A_ARBITRER |

Constat structurel (`proprietaires_tresorerie_service.py`, docstring) : ces mouvements sont
explicitement définis comme « jamais une réservation, une charge, une facture, une écriture
bancaire ou comptable » — l'absence de pont comptable est un choix de conception assumé à ce jour,
pas un oubli technique. Le compte `411000` existe déjà dans le plan comptable avec le commentaire
« Propriétaires (créance/compensation) ; non généré ce tour » (migration `0021`) — c'est un indice
disponible dans le schéma, pas une décision prise : aucune règle `mapping_comptable_regles` ne
relie aujourd'hui `411000` à cet objet, et la distinction dette/créance par nature n'est pas
tranchée.

## E. Associés / IK / remboursements — ce qui est connu vs inconnu

- **Compte** : `467000` (« Associés — comptes courants ») existe et porte, dans son commentaire de
  schéma (migration `0023`), l'usage explicite « avances/dépenses personnelles/remboursements ».
  C'est le seul compte du référentiel dont le libellé documente directement cet usage.
- **Dépense personnelle associé** : règle métier confirmée par le contexte fourni cette mission —
  jamais affectée automatiquement à un associé, doit être explicitement rattachée, reste
  A_CONTROLER si l'associé n'est pas démontré. Le moteur Banque a déjà posé la catégorie
  `DEPENSE_PERSONNELLE_ASSOCIE` en overlay (DEC-001, 2026-08-02) sans jamais choisir de compte ni
  rattacher un associé nominatif automatiquement.
- **IK (indemnité kilométrique)** : aucune catégorie `CHG_XXX` ni code bancaire dédié « IK »
  n'existe dans le référentiel actuel (le plus proche : `CHG_015`, « Remboursement frais avancés » —
  non confirmé comme identique à une IK). Règle métier de cette mission : ne jamais valider une IK
  sur le seul libellé bancaire, exiger l'objet métier justificatif (distance, campagne,
  bénéficiaire) — cet objet n'existe pas encore dans l'application. **A_ARBITRER** (à la fois la
  catégorie exacte et le compte).
- **Gestes commerciaux** (CHG_012, `REMBOURSEMENT_A_TRAITER` bancaire) : contrat métier non trouvé
  dans les documents de cadrage au-delà de la catégorie CHG_012 elle-même (« remboursement ou
  geste commercial voyageur », impact résultat OUI). Aucun compte ni traitement comptable distinct
  documenté. **A_ARBITRER**.

## F. Refacturations et avoirs

- Refacturations : `CATEGORIES_REFAC_TF011` (`CHG_008`, `CHG_011`) + `CHG_022` (AirCover refacturée)
  — mécanisme de refacturation propriétaire déjà modélisé côté Charges/Factures (`facture_lignes`,
  `charges_exceptionnelles_refacturees`), mais le mapping comptable de la refacturation elle-même
  suit la même résolution générique que toute charge → `606000` PROVISOIRE, rien de spécifique
  arbitré.
- Avoirs : `comptabilite_ecritures_service.generer_ecriture_avoir()` existe et fonctionne
  (contrepassation tracée) — **VALIDE techniquement**, aucun arbitrage compte requis au-delà de
  celui de la facture d'origine qu'il contrepasse.

## G. Ventes / commissions

Inchangé depuis `49` : `706000`, source `SOURCE_PROVISOIRE_LOT12` — voir tableau ci-dessous.

## Tableau de synthèse (structure demandée : Nature métier | Traitement métier connu | Impact réel | Impact comptable | Compte actuel | Compte définitif | Auxiliaire | TVA | Statut)

| Nature métier | Traitement métier connu | Impact réel | Impact comptable | Compte actuel | Compte définitif | Auxiliaire | TVA | Statut |
|---|---|---|---|---|---|---|---|---|
| Achats fournisseurs (générique, 27 catégories CHG) | Charge professionnelle, nature variable par catégorie | OUI (par catégorie, cf. référentiel) | OUI | `606000` | — | Fournisseur (401000) déjà fonctionnel | TVA_A_CONTROLER | PROVISOIRE |
| Frais bancaires (CHG_010/TYPE_FLUX_016) | Charge professionnelle — connu | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | A_ARBITRER (compte) |
| Honoraires comptables | Charge professionnelle — connu (DEC-001) | OUI | OUI | Aucun (pas de pont) | — | — | TVA_A_CONTROLER | A_ARBITRER (compte + pont) |
| Cotisations sociales | Charge professionnelle — connu (DEC-001) | OUI | OUI | Aucun | — | — | Hors champ probable (non tranché) | A_ARBITRER |
| Prévoyance | Charge professionnelle — connu (DEC-001) | OUI | OUI | Aucun | — | — | Hors champ probable (non tranché) | A_ARBITRER |
| Logiciels (CHG_005/006/007, LOGICIEL_GESTION) | Charge professionnelle | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Assurance (CHG_011) | Charge professionnelle, refacturable si sinistre | OUI | OUI | `606000` | — | Fournisseur/Propriétaire selon cas | TVA_A_CONTROLER | PROVISOIRE |
| Déplacements (CHG_009) | Charge professionnelle | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Repas (CHG_025) | Charge, avantage associé possible (à documenter au cas par cas) | OUI | OUI | `606000` | — | Associé (467000) si avantage confirmé | TVA_A_CONTROLER | PROVISOIRE |
| Achats divers (CHG_018) | Charge professionnelle | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Prestations diverses (CHG_026) | Charge, avantage associé possible | OUI | OUI | `606000` | — | Associé si avantage confirmé | TVA_A_CONTROLER | PROVISOIRE |
| Charges générales (CHG_017/CHG_020) | Charge non affectée / à contrôler | OUI / A_DEFINIR | OUI / A_DEFINIR | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE / A_ARBITRER |
| Achats ménage (CHG_001/002/004) | Charge ménage, coût complet suivi analytiquement | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Blanchisserie (CHG_003) | Charge ménage | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Supplément ménage (CHG_027) | Charge analytique, jamais refacturable | OUI | OUI | `606000` | — | — | TVA_A_CONTROLER | PROVISOIRE |
| Dépenses personnelles associés (ACHAT_PERSO_CB / DEPENSE_PERSONNELLE_ASSOCIE) | **Jamais** charge professionnelle — connu | NON (par nature) | NON (compte courant associé) | Aucun | Candidat `467000` (usage documenté en schéma) | Associé (nominatif requis) | Hors champ | A_ARBITRER (rattachement + compte définitif) |
| Remboursements associés (CHG_015) | Remboursement de frais avancés par un associé | OUI probable | OUI | `606000` | Candidat `467000` | Associé | TVA_A_CONTROLER | A_ARBITRER |
| IK | Remboursement associé si justificatif démontré — jamais déduit du seul libellé | OUI si justifié | OUI si justifié | Aucun objet dédié | Candidat `467000` | Associé | Hors champ probable | A_ARBITRER (catégorie + compte + objet justificatif absent) |
| Gestes commerciaux (CHG_012) | Remboursement/geste commercial voyageur | OUI | OUI | `606000` | — | — | Hors champ probable | A_ARBITRER (traitement distinct non documenté) |
| Trésorerie propriétaires (7 natures, migration 0025) | Mouvement financier société↔propriétaire, sens déclaré manuellement — jamais un objet comptable par conception actuelle | A_CONTROLER | A_CONTROLER | Aucun pont | Candidat `411000` (usage documenté en schéma) | Propriétaire | Hors champ (mouvement de trésorerie) | A_ARBITRER |
| Refacturations (CHG_008/011/022) | Charge refacturée au propriétaire, modélisée côté Factures | OUI | OUI | `606000` (charge d'origine) | — | Propriétaire (via facture) | TVA_A_CONTROLER | PROVISOIRE |
| Avoirs | Contrepassation d'une facture validée | OUI | OUI | Suit le compte de la facture d'origine | — | Fournisseur | Suit la facture d'origine | VALIDE (mécanisme) |
| Ventes / commissions (adaptateur Lot12) | Commission conciergerie, montant déjà calculé par Lot12, jamais recalculé | OUI | OUI | `706000` | — | Propriétaire | Hors champ probable (prestation de service, à confirmer) | PROVISOIRE |
| Ventilation charge multi-logements (pool) | Aucune clé de poids définie | — | — | — | — | — | — | GAP CONNU (`41` §7bis), pas un sujet de compte |
| TVA (transverse) | Voir section dédiée ci-dessous | — | — | — | — | — | — | TVA_A_CONTROLER (partout) |

## Décisions techniques déjà prises (rappel, pas des arbitrages en attente)

- Le mécanisme de résolution de compte (`mapping_comptable_regles`, historisé par date de
  validité) est fonctionnel et testé — ce n'est pas la mécanique qui manque, seul le contenu des
  règles `VALIDE` reste à arbitrer.
- La ventilation analytique (`ecriture_ligne_ventilation`) trace déjà la méthode utilisée par
  ligne (affectation directe, facture multi-lignes, ménage, sans dimension) — aucun nouveau
  mécanisme à construire pour cette partie.
- Les statuts `PROVISOIRE`/`A_ARBITRER` n'empêchent ni la consultation Comptabilité, ni les
  Résultats, ni l'Analytique, ni le Journal, ni les Contrôles, ni la recette fonctionnelle — ils
  empêchent uniquement la **validation comptable définitive** des écritures concernées (contrôle
  `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE`, déjà existant).

## Ce qui n'a jamais été traité par aucun chantier (à ne pas confondre avec un oubli de cette mission)

- Toute question fiscale (TVA, régime d'imposition, obligations déclaratives) — nécessite un
  professionnel qualifié, jamais une réponse générée automatiquement.
- Le plan de comptes complet au sens d'un plan comptable général français détaillé (le système
  actuel couvre les comptes strictement nécessaires au fonctionnement applicatif, pas un plan
  comptable exhaustif).

## TVA — ce que l'application sait / ne sait pas

- **Sait** : afficher en permanence un contrôle `CTRL_CPT_TVA_NON_ARBITREE` (niveau INFO, jamais
  masqué) sur toute écriture générée — visibilité honnête, aucune TVA calculée ni déduite.
- **Ne sait pas / A_CONTROLER** : régime applicable (franchise en base ou non), assujettissement de
  chaque fournisseur, TVA déductible/collectée par nature de charge, traitement des factures
  concernées.
- **Nécessaire pour passer à VALIDE** : décision fiscale formelle (professionnel qualifié), puis
  compte(s) TVA dédié(s) créés dans le plan comptable (aucun aujourd'hui) et règles de rattachement
  par catégorie/fournisseur.
- Aucune hypothèse TVA n'a été transformée en règle automatique par cette mission.

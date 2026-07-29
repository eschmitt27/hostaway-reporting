# 46 — Plan comptable et mappings : état

## Plan de comptes actuel — provisoire

| Compte | Libellé | Type | Auxiliaire | Usage |
|---|---|---|---|---|
| 401000 | Fournisseurs | PASSIF | OUI | crédité par ACHATS, débité par BANQUE |
| 411000 | Propriétaires (créance/compensation) | ACTIF | OUI | déclaré, **non généré** ce tour |
| 512000 | Banque | ACTIF | NON | crédité par BANQUE |
| 606000 | Achats et charges externes (générique) | CHARGE | NON | débité par ACHATS, seul compte de charge existant |

**Ce plan n'est pas définitif.** Aucune décision métier consultée ne fixe un plan comptable
complet. `606000` sert de compte générique unique : toute charge, quelle que soit sa catégorie
(`CHG_001`…`CHG_024`, cf. `REF_Categories_Charges`), est imputée dessus. C'est délibérément
minimal — le mapping fin catégorie → compte reste à arbitrer.

## Mapping catégorie de charge → compte : NON ARBITRÉ

`REF_Categories_Charges` distingue déjà des familles (`Ménage`, `Linge`, `Logiciel`, `Maintenance`,
`Déplacement`, `Banque`, `Assurance`, `Remboursement`, `Associées`…). Un plan comptable réel
distinguerait probablement au moins :

- 606100 — Ménage externe
- 606200 — Maintenance / réparation
- 606300 — Logiciels et abonnements
- 627000 — Frais bancaires
- 641000 — Rémunération associés (si applicable en salaire)

**Aucun de ces comptes n'est créé.** Les créer sans arbitrage métier fixerait une classification
non validée dans un schéma difficile à faire évoluer proprement (les migrations sont additives,
jamais destructives). La table `plan_comptable` est conçue pour recevoir ce mapping le jour où il
est tranché : ajouter une ligne ne casse rien.

## Mappings fournisseur → auxiliaire, banque/caisse → compte

- **Fournisseur → auxiliaire** : direct, `fournisseur_id_opaque` porté par `ecriture_lignes.auxiliaire`
  — aucune table de correspondance nécessaire.
- **Banque/caisse → compte** : `512000` unique pour l'instant. Aucune distinction compte par
  compte bancaire (l'application ne gère qu'un compte fictif de recette,
  `CM_02211_00021321603`).
- **Propriétaire → auxiliaire, associé → auxiliaire** : colonnes prévues (`411000` déclaré), aucune
  génération câblée.

## Suite (2026-07-29, migration `0023`) — comptes ajoutés pour VENTES/CAISSE/OD

| Compte | Libellé | Type | Auxiliaire | Usage |
|---|---|---|---|---|
| 530000 | Caisse | ACTIF | NON | crédité/débité par CAISSE — compte de caisse unique (recette : un seul compte fictif, comme 512000 pour la banque) |
| 467000 | Associés — comptes courants | PASSIF | OUI | avances, dépenses personnelles, remboursements associés — CAISSE et OD |
| 706000 | Prestations de services (commissions) | PRODUIT | NON | crédité par VENTES — montant `montant_du_conciergerie` de Lot12, mapping non arbitré au-delà de ce compte générique |

Toujours **provisoire** : ces trois comptes couvrent le besoin technique minimal pour que les trois
journaux fonctionnent, ils ne préjugent d'aucun plan comptable détaillé futur.

## Table `mapping_categorie_compte` (migration `0023`) — infrastructure créée, non reliée

Une ligne par catégorie de charge, `compte` par défaut `606000`, `statut` `A_CONTROLER` tant qu'un
arbitrage métier ne la fait pas passer `VALIDE`. Le contrôle `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE`
signale chaque ligne `A_CONTROLER`.

**Non fait, honnêtement** : `generer_ecriture_achat` (journal ACHATS) continue d'utiliser `606000`
en dur, comme avant cette mission. Relier le mapping demanderait de résoudre, au moment de la
génération, la catégorie de la charge liée à la facture (`SAISIE_Charges_Flux.categorie_charge_id`,
un fichier Excel) — un second changement, non entrepris ce tour faute de temps, à traiter avant que
cette table serve à autre chose qu'à afficher un contrôle A_CONTROLER.

## Ce qui reste à faire

1. Décision métier sur le plan de comptes détaillé (hors périmètre de ce tour, aucune source ne le
   fixait).
2. Relier `mapping_categorie_compte` à la résolution réelle du compte dans `generer_ecriture_achat`
   (lecture de la catégorie de charge depuis l'Excel), une fois le plan détaillé arbitré.
3. Génération d'écritures pour le circuit propriétaire COMME OBJET APPLICATIF (facture propriétaire
   émise), si la décision `44` de ne pas migrer lot12 est un jour révisée — l'adaptateur VENTES
   actuel reste une lecture, pas une migration.

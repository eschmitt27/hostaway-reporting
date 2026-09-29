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

> **SUPERSÉDÉ (Mission 31, 2026-09-28)** : la règle appliquée vit dans `mapping_comptable_regles`
> (`0024`, étendue par `0113`), seule source canonique. `mapping_categorie_compte` est vide et n'est plus
> lue que par le contrôle `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE` (0 signalement). Voir la section
> « Mission 31 » ci-dessous.

Une ligne par catégorie de charge, `compte` par défaut `606000`, `statut` `A_CONTROLER` tant qu'un
arbitrage métier ne la fait pas passer `VALIDE`. Le contrôle `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE`
signale chaque ligne `A_CONTROLER`.

**Non fait, honnêtement** : `generer_ecriture_achat` (journal ACHATS) continue d'utiliser `606000`
en dur, comme avant cette mission. Relier le mapping demanderait de résoudre, au moment de la
génération, la catégorie de la charge liée à la facture (`SAISIE_Charges_Flux.categorie_charge_id`,
un fichier Excel) — un second changement, non entrepris ce tour faute de temps, à traiter avant que
cette table serve à autre chose qu'à afficher un contrôle A_CONTROLER.

## Ce qui reste à faire

> **État au 2026-09-28 (Mission 31)** : le point 2 est fait depuis `50` (résolution par
> `mapping_comptable_regles`) ; le point 1 est désormais réalisable par l'utilisateur depuis l'application
> (plan comptable administrable), il n'est pas tranché à sa place.

1. Décision métier sur le plan de comptes détaillé (hors périmètre de ce tour, aucune source ne le
   fixait).
2. Relier `mapping_categorie_compte` à la résolution réelle du compte dans `generer_ecriture_achat`
   (lecture de la catégorie de charge depuis l'Excel), une fois le plan détaillé arbitré.
3. Génération d'écritures pour le circuit propriétaire COMME OBJET APPLICATIF (facture propriétaire
   émise), si la décision `44` de ne pas migrer lot12 est un jour révisée — l'adaptateur VENTES
   actuel reste une lecture, pas une migration.

## Mission 31 (2026-09-28, migration `0113`) — plan comptable et mappings administrables

- **Plan comptable administrable** (Comptabilité › Plan comptable) : ajout manuel d'un compte par
  l'utilisateur (numéro en chiffres, libellé, type ; charge ⇔ classe 6, produit ⇔ classe 7 ; doublon refusé ;
  aucun numéro généré), modification du libellé et du commentaire seulement, désactivation / réactivation
  avec motif et auteur, historique `plan_comptable_evenements`. Les comptes utilisés par les générateurs
  (`401000`, `411000`, `455100`, `512000`, `530000`, `606000`, `706000`) ne se désactivent pas. **Aucune
  suppression** : le schéma la refuse (trigger), le numéro est immuable (trigger).
- **Mappings** : règle catégorie → compte choisie dans des listes (catégories du référentiel, comptes de
  charge actifs), période de validité, statut provisoire ou validé, prévisualisation d'impact avant
  enregistrement ou validation, historique `mapping_regle_evenements`, désactivation au lieu de suppression.
- **Aucun repli silencieux** : le repli absolu `606000` du résolveur est supprimé ; Flux financiers ne
  propose qu'une règle validée vers un compte actif de classe 6, sinon « Compte comptable à définir ».
- **Limite** : le journal Achats des factures fournisseurs garde le filet `MAP-GENERIQUE-606000`
  PROVISOIRE (lecture seule dans l'écran) tant qu'aucune règle validée ne couvre la catégorie.
- Le plan de comptes détaillé reste un **choix de l'utilisateur** : aucun compte ni aucune règle n'a été créé
  dans la base réelle.

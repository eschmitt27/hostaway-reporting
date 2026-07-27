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

## Ce qui reste à faire

1. Décision métier sur le plan de comptes détaillé (hors périmètre de ce tour, aucune source ne le
   fixait).
2. Mapping catégorie de charge → compte, une fois le plan validé.
3. Génération d'écritures pour le circuit propriétaire, si la décision `44` de ne pas migrer lot12
   est un jour révisée.

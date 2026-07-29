# 45 — Modèle des écritures comptables : état

Suite du cadrage `43`. Ce document dit ce qui est prouvé.

## Statut : première verticale construite et prouvée en recette navigateur

Migration `0021` (`plan_comptable`, `ecritures`, `ecriture_lignes`, `ecriture_evenements`),
service `comptabilite_ecritures_service.py`, 8 routes sous `/comptabilite` + 1 route de génération
depuis la fiche facture.

## Recette navigateur réelle (port 8080)

Sur `FA-MEN-2026-06` (facture réelle du jeu de recette, `PARTIELLEMENT_REGLEE`, charge
`CHG_SEED_001` déjà liée) :

1. **Génération** de l'écriture ACHATS depuis la fiche facture → `ECR-697E8F682426`, journal
   `ACHATS`, débit `606000` = crédit `401000` = **120,00 €**, auxiliaire = fournisseur, origine
   tracée (`FACTURE FAC-D256FCE7A321`).
2. **Validation** → statut `VALIDEE`.
3. **Solde fournisseur** consulté sur `/comptabilite/auxiliaires` : débit 0, crédit 120, solde
   **−120,00 €** (dette, cohérent avec un compte de passif).
4. **Contrepassation** (avoir) → écriture miroir `ECR-C84C016DF481` générée, lignes inversées,
   origine `contrepasse_de = ECR-697E8F682426`. L'écriture d'origine passe `CONTREPASSEE`, **jamais
   supprimée**.
5. Solde fournisseur après contrepassation : débit 120, crédit 120, **solde 0,00 €** — l'avoir
   compense exactement l'achat.
6. **Redémarrage du serveur** → l'écriture reste `CONTREPASSEE`. **Persistance prouvée.**
7. **Relance idempotente** : regénérer l'écriture ACHATS pour la même facture, deux fois de suite,
   rend **le même** `ecriture_id_opaque` (`ECR-8595B1575B6E`) — aucun doublon, même après que
   l'écriture précédente sur la même origine ait été contrepassée puis qu'une nouvelle ait pris sa
   place.

## Défaut de conception trouvé et corrigé pendant l'écriture des tests

`solde_compte()`/`solde_auxiliaire()` ne comptaient à l'origine que les écritures `VALIDEE`. Après
contrepassation, l'écriture d'origine passe `CONTREPASSEE` et sortait donc du calcul — alors que son
miroir (`VALIDEE`) y restait. Résultat : le solde ne revenait jamais à zéro après une
contrepassation, l'inverse de ce qu'une contrepassation doit garantir.

**Correction** : les deux fonctions comptent `VALIDEE` **et** `CONTREPASSEE` — une écriture
contrepassée reste une écriture historiquement postée, compensée par son miroir, pas par son
absence. Seule `PROPOSEE` (jamais validée) reste exclue. Preuve : `test_contrepasser_cree_une_ecriture_miroir` et la recette navigateur ci-dessus (solde exactement 0,00 € après avoir).

## Garanties prouvées par les tests (28 au total : 15 service + 9 routes + 4 migrations/flags)

- **Équilibre imposé** : `test_ecriture_desequilibree_refusee` — une écriture dont débit ≠ crédit
  est refusée avant toute écriture en base.
- **Compte inconnu refusé** : `test_compte_inconnu_refuse`.
- **Idempotence** : `test_idempotence_generation_achat` + défense en profondeur par index unique
  (`test_idempotence_impossible_a_contourner_en_sql_direct`, `IntegrityError`).
- **Origine invalide refusée** : une facture non validée ne génère aucune écriture
  (`test_facture_non_validee_refusee`).
- **Aucune suppression** : `test_ecriture_jamais_supprimee_apres_contrepassation`.
- **Double verrou** : `test_generer_refuse_sans_flags`, flag `COMPTABILITE_REAL_WRITE_*` figé par
  `test_flags_inventaire.py` (32 tests, catégorie « double verrou »).

## Suite (2026-07-29) — cœur Comptabilité complet : VENTES, CAISSE, OD, périodes, clôture

Détail complet dans `49_COEUR_COMPTABILITE_ETAT_FINAL.md`. Résumé des ajouts (migration `0023`) :

- **VENTES** : adaptateur `ventes_lot12_adapter_service.py`, lit `montant_du_conciergerie` déjà
  calculé par Lot12 (jamais recalculé), génère une écriture par propriétaire/mois, marquée
  `SOURCE_PROVISOIRE_LOT12` partout où elle apparaît. Ce n'est PAS une facture propriétaire émise
  comme objet applicatif (décision explicite, hors périmètre).
- **CAISSE** : `generer_ecriture_caisse_reglement` (règlement fournisseur moyen=CAISSE, source déjà
  réelle) + `operations_caisse_service.py` (encaissement, remboursement associé — objet neuf, aucune
  source préexistante pour ces cas).
- **ODIVERSES** : `operations_diverses_service.py` — objet BROUILLON avec ses propres lignes
  équilibrées (`od_lignes`), ne devient écriture qu'après validation explicite.
- **Périodes comptables** (`comptabilite_periodes_service.py`) : `OUVERTE → EN_CONTROLE → VALIDEE →
  CLOTUREE → ROUVERTE`, distinctes de la clôture applicative du pilotage des calculs (`0008`). Une
  période `CLOTUREE` fait refuser toute nouvelle écriture par `_inserer_ecriture` (code
  `E_PERIODE_CLOTUREE`) — vérifié y compris par contournement SQL direct dans les tests.
- **Contrôles comptables** (`comptabilite_controles_service.py`) : catalogue de 15 codes, utilisé
  comme garde de clôture (`cloturer()` refuse si un `BLOQUANT` subsiste).
- **Auxiliaires** (`comptabilite_auxiliaires_service.py`) : vue consolidée fournisseurs/
  propriétaires/associés, solde + éléments ouverts, aucun second calcul de solde.
- Preuve : parcours complet ACHATS→BANQUE→VENTES→CAISSE→OD→contrepassation→période→clôture→
  écriture refusée→réouverture, via les routes HTTP réelles (`test_comptabilite_coeur_recette.py`),
  plus une passe navigateur réel sur les écrans CAISSE/OD/Périodes/Auxiliaires/VENTES.

## Ce qui n'est pas fait

| Sujet | État |
|---|---|
| Mapping catégorie de charge → compte fin | ⚠️ table `mapping_categorie_compte` créée (statut `A_CONTROLER`), **pas encore reliée** à la résolution du compte dans `generer_ecriture_achat` (nécessiterait de lire la catégorie de charge depuis l'Excel `SAISIE_Charges_Flux` au moment de la génération — non traité, gap honnêtement documenté) |
| Dimensions analytiques exploitées | ⚠️ colonnes présentes sur chaque ligne, aucun tableau de bord — hors périmètre explicite |
| Écrans Résultats | ⛔ hors périmètre de cette mission |
| Contrôle TVA | ⛔ non construit — signalé en INFO permanent par le contrôle `CTRL_CPT_TVA_NON_ARBITREE` |
| Circuit propriétaire en écritures | ⚠️ VENTES généré via adaptateur Lot12 (lecture seule) ; facture propriétaire comme objet applicatif reste hors périmètre (décision `44`) |
| Ventilation pools de courses en recette | ⛔ gap distinct, cf. `41` §7bis — indépendant de ce tour |

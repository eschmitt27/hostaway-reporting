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

## Ce qui n'est pas fait

| Sujet | État |
|---|---|
| Journal VENTES/CAISSE/ODIVERSES | ⛔ déclarés, non générés |
| Mapping catégorie de charge → compte fin | ⛔ `606000` générique par défaut, non arbitré |
| Dimensions analytiques exploitées | ⚠️ colonnes présentes sur chaque ligne, aucun tableau de bord |
| Écrans Résultats | ⛔ hors périmètre de cette mission |
| Clôture comptable | ⛔ distincte de la clôture applicative du pilotage des calculs, non construite |
| Contrôle TVA | ⛔ non construit |
| Circuit propriétaire en écritures | ⛔ dépend de la décision `44` de ne pas migrer lot12 vers SQLite |

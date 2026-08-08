# 72 — Checklist GO/NO-GO mode réel

Verdicts possibles (un seul choisi ci-dessous) : `NO GO — VALIDATION HUMAINE REQUISE` /
`NO GO — ARBITRAGES MÉTIER REQUIS` / `NO GO — CORRECTION TECHNIQUE REQUISE` /
`GO POUR PRÉPARATION DU MODE RÉEL`. Jamais « GO pour mode réel » — cette checklist ne déclenche
aucune activation.

| # | Item | État constaté | Coché |
|---|---|---|---|
| 1 | Validation humaine signée (`67`, fiche ci-dessous) | colonnes laissées vides, jamais remplies par Claude | ☐ |
| 2 | Règles Banque validées (classification) | 517/541 encore `A_CONTROLER`, arbitrage non fait (`68`) | ☐ |
| 3 | Rapprochements prioritaires revus | 166 Airbnb **catégorisés** (PAYOUT_PLATEFORME, jamais rapprochés à une réservation — cadrage corrigé 2026-08-08, cf. `76`) ; 56 propriétaires en attente, 0 confirmé | ☐ |
| 4 | Mappings comptables validés | filet provisoire `606000` partout, aucune règle `VALIDE` arbitrée (`70`) | ☐ |
| 5 | Fonctions différées arbitrées | décisions déjà explicitées en `67` (la plupart « non nécessaire »), 2 recommandées avant activation Comptabilité | ☐ (partiel, voir `67`) |
| 6 | Sécurité validée | leak chemin absolu corrigé et testé ce tour (`c65f891`) ; 2 notes de sécurité pré-existantes restent ouvertes (voir `60`) | ☐ |
| 7 | Sauvegardes testées | procédure décrite (`71`), pratiquée en routine pour les recettes globales — jamais testée en conditions réelles avec writers actifs | ☐ |
| 8 | Rollback testé | procédure décrite (`71`), jamais déclenchée réellement (aucun incident à ce jour) | ☐ |
| 9 | Performance acceptée | non mesurée formellement ce tour — pipeline complet (6 lots) exécuté sans lenteur perçue sur volumes réels de recette | ☐ |
| 10 | Campagne de tests verte | 132 fichiers, 2284 passed / 75 skipped / 1 échec pré-existant connu (`test_appsec1_diagnostic.py`, non-bloquant, indépendant du code métier) | ☑ |
| 11 | Intégrité des sources vérifiée | 88/88 fichiers réels conformes au hash baseline (à reconfirmer une dernière fois avant commit — voir ci-dessous) | ☑ |
| 12 | Responsables identifiés (qui décide quoi) | non désigné par ce document — décision organisationnelle de l'utilisateur | ☐ |
| 13 | Fenêtre de bascule définie | non définie par ce document — décision de planning de l'utilisateur | ☐ |

## Fiche de signature (acceptation) — à remplir exclusivement par l'utilisateur

| Domaine | Accepté | Accepté avec réserve | Refusé | Commentaire |
|---|---|---|---|---|
| Données / référentiels | | | | |
| Réservations | | | | |
| Ménages | | | | |
| Charges | | | | |
| Factures / Règlements | | | | |
| Banque | | | | |
| Résultats / Réconciliations | | | | |
| Propriétaires | | | | |
| Comptabilité | | | | |
| Exports (Power BI, CSV) | | | | |
| Sécurité | | | | |
| Performances | | | | |

Colonnes intentionnellement vides. Claude ne coche, ne remplit, ni n'infère aucune de ces cases.

## Verdict

**NO GO — VALIDATION HUMAINE REQUISE.**

Justification : items 1, 2, 3, 4, 12, 13 dépendent exclusivement d'une décision humaine non encore
rendue ; item 5 partiellement tranché mais deux points restent recommandés avant l'activation
Comptabilité ; items 6-9 sont techniquement prêts mais non éprouvés en conditions réelles (sécurité
corrigée mais 2 notes ouvertes, sauvegardes/rollback jamais exercés hors recette). Aucun blocage
technique de fond (item 10-11 verts) — le socle technique est prêt à recevoir la décision humaine,
mais celle-ci n'a pas eu lieu.

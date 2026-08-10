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

## Mise à jour 2026-08-10 (suite) — clarification des bloqueurs de clôture

**Correction majeure d'un chiffre de mes rapports précédents.** Comptage exact sur l'export
applicatif : **2420 lignes** au total — sévérité BLOQUANT **959**, A_CONTROLER **1399**, INFO **62** ;
**2357 lignes portent `impact_cloture = "Bloque la clôture"`**. Mes rapports écrivaient « 62 INFO
séparés des 2358 bloquants », ce qui conflatait « bloque la clôture » et « sévérité BLOQUANT ».

**Ce ne sont pas des défauts applicatifs mais des lacunes de données métier** (9 familles de codes :
périodes de gestion absentes 838, réservations sans commission 612, guest count manquant 553, lignes
bancaires non classées 221, charges exceptionnelles mal rangées 121, 12 résiduelles). Aucune n'a été
résolue, masquée ni transformée en exception.

**Conséquence : aucun mois n'est clôturable aujourd'hui → préparation mode réel = NO GO.**

| Item | Statut |
|---|---|
| Bloqueurs de clôture identifiés et catégorisés | VALIDE |
| Bloqueurs de clôture traités | **BLOQUE (partiel)** — `GESTION_LOGEMENT_MISSING` : 838→472 lignes (137→77 couples) sur décision utilisateur explicite (couverture prolongée au 01/08/2025, réel modifié) ; 8 autres familles (1519 lignes) + reste de cette famille (77 couples, jan-juil 2025) encore ouverts |
| Contrat de sécurité des writers (double garde, fail-closed, write-guard, backup, prévisualisation) | VALIDE — **déjà implémenté**, aucun mécanisme parallèle créé |
| État « écriture réelle » (état C) | **NON CONSTRUIT — délibérément** ; ne doit pas l'être tant que le verdict est NO GO |
| 3 hardcodes analysés individuellement | VALIDE — 2 prêts fonctionnellement mais maintenus `False`, 1 NON_ACTIVABLE |

## Mise à jour 2026-08-10 — checklist restructurée par domaine

Statuts : `VALIDE` / `VALIDE_AVEC_RESERVE` / `BLOQUE` / `NON_TESTE` / `A_SIGNER_UTILISATEUR`.
Aucune case signée par Claude.

### A. Prêt techniquement

| Item | Statut | Preuve |
|---|---|---|
| 18 modules répondent, aucune erreur 500/404 | VALIDE | Smoke HTTP + recette navigateur (`RECETTE_FONCTIONNELLE_GLOBALE.md`) |
| Chaîne E2E Fournisseur→Facture→Règlements→Comptabilité | VALIDE | LOT D, écritures réelles générées et équilibrées |
| Équilibre comptable imposé (déséquilibre refusé) | VALIDE | OD 100/60 refusée, message exact |
| Période clôturée verrouillée + réouverture justifiée | VALIDE | LOT D, cycle complet exercé |
| Invariant REEL = COMPTABLE + HORS_COMPTA | VALIDE | Écart 0,00 € vérifié en direct |
| Pipeline aval 6/6 depuis l'interface | VALIDE | RUN-430E7833235E, 74,4 s |
| Idempotence | VALIDE | 2ᵉ run identique au centime |
| Rollback pipeline | VALIDE | 8 fichiers restaurés, exercé |
| Campagne de tests | VALIDE | 591 passés / 37 ignorés / 0 échec (périmètre modifié), plus campagnes antérieures |
| Intégrité des sources réelles | VALIDE | 950/950 fichiers identiques au baseline, 0 modification |
| Confidentialité (PII, IBAN, chemins) | VALIDE | Scan sur tous les exports : aucune fuite |
| Séparation Charge/Facture/Règlement/Banque/Écriture | VALIDE | Aucun objet recréé, aucun double comptage |

### B. Validation humaine

| Item | Statut |
|---|---|
| LOT A (Navigation, Logements, Propriétaires, Réservations) | VALIDE_AVEC_RESERVE — signé utilisateur 2026-08-10 |
| LOT B (Ménages, Fournisseurs, Charges, Factures, Règlements) | VALIDE_AVEC_RESERVE — signé utilisateur 2026-08-10 |
| LOT C (Banque/Caisse, Trésorerie propriétaires) | VALIDE_AVEC_RESERVE — signé utilisateur 2026-08-10 |
| LOT D (Comptabilité, Analytique, Résultats) | **A_SIGNER_UTILISATEUR** |
| LOT E (Contrôles/Clôture, Calculs/Exports) | **A_SIGNER_UTILISATEUR** |
| Responsables désignés (qui décide quoi) | A_SIGNER_UTILISATEUR |
| Fenêtre de bascule définie | A_SIGNER_UTILISATEUR |

### C. Arbitrages comptables

| Item | Statut |
|---|---|
| 27 catégories `CHG_XXX` → comptes définitifs | **BLOQUE** (tout sur `606000` provisoire) |
| Frais bancaires → compte définitif | **BLOQUE** |
| Trésorerie propriétaires → compte/circuit | **BLOQUE** (`411000` candidat non validé) |
| Associés / IK / remboursements | **BLOQUE** (`467000` candidat non validé) |
| TVA | VALIDE — utilisateur : aucune TVA applicable actuellement (2026-08-08) |

### D. Fonctionnalités différées

| Élément | Statut |
|---|---|
| Écran dédié de consultation Hostaway | A_ARBITRER (réserve LOT A) |
| Factures propriétaires émises | A_ARBITRER |
| Factures voyageurs / tiers | A_ARBITRER |
| Avoir autonome | A_ARBITRER |
| Pool multi-logements (clé de répartition) | A_ARBITRER |
| Caisse (aucune donnée de recette disponible) | A_ARBITRER (réserve LOT C) |
| Mappings comptables définitifs | A_ARBITRER (cf. section C) |

### E. Rollback

| Item | Statut |
|---|---|
| Rollback pipeline (natif, bouton applicatif) | VALIDE — exercé |
| Rollback base applicative (`app.db`) | VALIDE — procédure documentée (`PREPARATION_MODE_REEL.md`) |
| Backup complet + manifest SHA256 | VALIDE — générateur éprouvé (950 fichiers) |
| Rollback testé de bout en bout en conditions réelles | NON_TESTE |

### F. Mode réel

| Item | Statut |
|---|---|
| Inventaire des writers | VALIDE — 9 writers documentés (`PREPARATION_MODE_REEL.md`) |
| Ordre d'activation proposé | VALIDE — 7 étapes, du risque le plus faible au plus élevé |
| Verrou technique | **BLOQUE par conception** — tous les writers gatés par `RECETTE_MODE`, 3 codés en dur à `False`. L'activation exige une modification revue de `app/config.py`, pas un changement de variable |
| Activation | **NON — aucun flag modifié, mode réel jamais activé** |

## Verdict

**NO GO — VALIDATION HUMAINE REQUISE.**

Justification : items 1, 2, 3, 4, 12, 13 dépendent exclusivement d'une décision humaine non encore
rendue ; item 5 partiellement tranché mais deux points restent recommandés avant l'activation
Comptabilité ; items 6-9 sont techniquement prêts mais non éprouvés en conditions réelles (sécurité
corrigée mais 2 notes ouvertes, sauvegardes/rollback jamais exercés hors recette). Aucun blocage
technique de fond (item 10-11 verts) — le socle technique est prêt à recevoir la décision humaine,
mais celle-ci n'a pas eu lieu.

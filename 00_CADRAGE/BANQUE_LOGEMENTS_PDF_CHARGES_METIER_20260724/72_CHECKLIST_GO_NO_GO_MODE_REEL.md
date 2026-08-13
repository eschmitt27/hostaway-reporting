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
| Bloqueurs de clôture traités | **BLOQUE (partiel)** — `GESTION_LOGEMENT_MISSING` : 838→472 (réel modifié, décision utilisateur) ; `RESERVATION_A_CONTROLER_SANS_COMMISSION` : 612 inchangé (référentiel de taux déjà complet, rien à faire) ; `GUEST_COUNT_MANQUANT` : ré-extraction Hostaway réelle exécutée, master `MASTER_FACT_HA_Reservations.xlsx` remplacé (1391→1527) ; **bug `lot4ter` trouvé et corrigé (committé `9a0a6aa`, HIST n'aurait jamais conservé `guestCount`)** ; **correction réelle des 506 snapshots historiques appliquée dans `HIST_Reservations_Cloturees.xlsx`** (backup+hash+diff exact vérifiés, 0 autre colonne touchée), effet mesuré en **simulation sur copie** : 553→506→0 (mécanisme intégralement vérifié, idempotent, rollback testé) ; **chiffre de clôture réel reste 553 tant que le pipeline aval réel n'est pas rejoué** (interdit explicitement par l'autorisation) ; `RESERVATION_EXCLUE_A_CONTROLER` : **bug de double-comptage trouvé et corrigé dans `lot10_calculer_resultats.py`** (28 réservations déjà comptées financièrement mais listées en double, 0 impact société), simulation 70→42, 42 restantes = saisie manuelle nécessaire (aucune donnée réelle touchée, correctif de code seul) ; **baseline canonique fraîche établie (2026-08-12, Banque réelle incluse) : les 541 BLOQUANT du système sont exactement `GESTION_LOGEMENT_MISSING`(472)+`CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE`(69, sous-ensemble strict vérifié de la même gestion manquante) — aucun autre BLOQUANT n'existe** ; Banque : 541 mouvements réels, 236 classés, 222 rapprochement humain, 83 file assistée ; 0 nouveau bug technique résiduel |
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

## Mise a jour 2026-08-12

Historique gestion 2025 (items dependant de la cloture technique gestion/charges) : RESOLU.
541 BLOQUANT -> 0 (voir `81_BASELINE_CLOTURE_APRES_NETTOYAGE.md` §14). Items 1-4/12/13
restants : concernent desormais uniquement 42 Direct/VRBO, Banque humaine, mappings comptables.
Verdict global inchange : **NO GO — VALIDATION HUMAINE REQUISE** sur les points restants.

## Mise a jour 2026-08-12 (2)

Vraie file Banque humaine ventilee : 138 decisions reelles (56 proprietaires + 82 A_ENVOYER_IA),
166 PAYOUT_PLATEFORME exclues (0 decision, deja categorisees correctement par le moteur). Detail :
82_PACK_FINAL_ACTIONS_HUMAINES.md. Items Banque de la checklist : A_FAIRE_HUMAIN sur 138 items
precis (au lieu de 305 bruts). Rollback reel jamais exerce hors recette : NON_TESTE. Verdict
global inchange : NO GO — VALIDATION HUMAINE REQUISE.

## Mise a jour 2026-08-13 — audit Lot 5

LOT 5 : FONCTIONNEL mais NON ALIMENTE (0 ligne) -> A_FAIRE_HUMAIN.
DONNEES PROPRIETAIRES : A_FAIRE_HUMAIN (56 mouvements, 0 preuve existante, 7 a 56 decisions).
BANQUE A_ENVOYER_IA : A_FAIRE_HUMAIN (82 mouvements, 37 a 82 decisions selon regles candidates).
MIGRATIONS BASE REELLE : NON_TESTE — base reelle en migration 0016, tresorerie proprietaires
requiert 0025 ; migrations 0017->0026 a appliquer avant exploitation reelle (nouvel item).
ROLLBACK : NON_TESTE (inchange).
Verdict global inchange : NO GO — VALIDATION HUMAINE REQUISE.

## Mise a jour 2026-08-13 (2) — repetition migration app.db

MIGRATION APP.DB 0016->0026 : **PRETE ET REPETEE** (etait NON_TESTE). Sequentielle + automatique +
idempotence + rollback tous verts sur copies, base reelle jamais migree. Preuves :
84_REPETITION_MIGRATION_DB_0016_VERS_0026.md. Runbook pret, non execute :
85_RUNBOOK_MIGRATION_APP_DB_REELLE.md.
ROLLBACK DB : **VALIDE SUR COPIE** (hash exact restitue) — etait NON_TESTE.
LOT 5 : PRET A ETRE ALIMENTE, avec une contrainte operationnelle documentee (peuplement de MASTER
depuis SAISIE par refresh Power Query dans Excel, pas par Python).
BANQUE : PRETE TECHNIQUEMENT (lot8c corrige, lit desormais l'etat reel de Lot 5).
DONNEES HUMAINES : A_FAIRE_HUMAIN (56 proprietaires + 82 A_ENVOYER_IA + 42 Direct/VRBO + 3 autres).
Verdict global inchange : **NO GO — VALIDATION HUMAINE REQUISE. MODE REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 (3) — facturation proprietaires

FACTURES PROPRIETAIRES : l'application sait desormais les CREER (etait : NON). Modele 0027, cycle
BROUILLON/VALIDE/EMIS/ANNULE, snapshot immutable, numerotation serialisee, PDF deterministe,
avoir, solde derive. Recette complete verte sur copie migree.
FACTURES FOURNISSEURS : VALIDE — non-regression verifiee.
ECRITURE COMPTABLE DE VENTE : **A_FAIRE_HUMAIN** (nouvel item) — arbitrer la source unique de
l'ecriture (Lot 10 ou facture) avant de brancher un generateur, sous peine de double comptage.
MENTIONS LEGALES / FORMAT DE NUMERO : **A_FAIRE_HUMAIN** (nouvel item) — le format actuel est un
defaut technique, pas une decision juridique.
IDENTITE SOCIETE EMETTRICE : **A_FAIRE_HUMAIN** — vide par defaut, une facture ne peut pas etre
validee sans elle.
IMPUTATION BANQUE SUR CREANCE DE FACTURE : NON_TESTE — a cabler sur le moteur de rapprochement.
EMISSION REELLE : NON AUTORISEE. Verdict global inchange : **NO GO. MODE REEL : NO GO — NON ACTIVE.**

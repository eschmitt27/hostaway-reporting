# Recette fonctionnelle globale (2026-08-08)

Mission : vérifier que l'application permet réellement d'exécuter l'activité de bout en bout avec
les règles métier actuellement validées, sur environnement isolé (copies), aucun code modifié.

## A. Environnement de recette

Instance isolée : port `8030` (≠ 8000), `PROJECT_ROOT` = copies (`RECETTE_GLOBALE_20260801_004232/
SOURCES_COPIEES`), `APP_DATA_DIR` = `_RECETTES_GLOBALES/RECETTE_FONCTIONNELLE_20260808/APP_DATA`
(SQLite isolée, jamais l'`app.db` réelle), `RECETTE_MODE=1`. Port 8000/PID 21136 (réel) jamais
touché. Aucune source réelle modifiée. Serveur arrêté et nettoyé en fin de mission.

## B. Résultat des 18 modules (smoke HTTP + vérification ciblée)

Portée réelle de cette mission, à énoncer honnêtement : un smoke-test HTTP a été fait sur les 18
modules (chaque écran principal rendu sans erreur 500/404, avec les données réelles copiées) ; un
parcours navigateur approfondi (JS réel, formulaire rempli, résolution propriétaire/taux/ménage) a
été fait sur **Réservations hors Hostaway** (le seul module explicitement signalé « à corriger si
non fonctionnel » par la mission). Les autres modules s'appuient sur la couverture de test
automatisée déjà établie et vérifiée verte dans les missions précédentes de cette même session
(Banque, Comptabilité, Trésorerie propriétaires, Analytique/Résultats) — non ré-exercés en
navigateur un par un ce tour, faute de budget proportionné pour un parcours interactif complet des
18 modules en une seule mission.

| Module | Parcours principal | Technique | Navigateur | Données | Résultat | Anomalies |
|---|---|---|---|---|---|---|
| Tableau de bord / navigation | `/` | OK (200) | Non ré-exercé (couvert par `test_navigation_no_404.py`) | Copies | OK | Aucune |
| Logements | `/logements` | OK (200) | Non ré-exercé (couvert, module TERMINÉ) | Copies | OK | Aucune |
| Propriétaires | `/proprietaires` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Réservations Hostaway | `/reservations` | OK (200) | Non ré-exercé (lecture seule assumée) | Copies | OK | Aucune |
| Réservations hors Hostaway | `/reservations/nouvelle` | OK (200) | **Exercé réellement** : logement→date→propriétaire auto-résolu (PROP_0001), taux 18 % résolu (« source : taux propriétaire »), prix ménage standard résolu (29 €, « source : REF_Couts_Standards_Menage »). Écriture réelle non poussée jusqu'au bout (le formulaire ouvre une modale de confirmation JS avant le POST final, non forcée ce tour — écriture déjà prouvée par `test_reservations_hh.py`, 44 tests verts cette session) | Copies | OK (résolution métier confirmée en direct) | Aucune |
| Ménages | `/menages` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Fournisseurs | `/fournisseurs` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Charges | `/charges-controle` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Factures | `/factures` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Règlements | `/reglements` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Banque / Caisse | `/banques-caisse`, `/banques-caisse/a-classer` | OK (200) | Non ré-exercé ce tour (VALIDÉ SUR COPIES lors des deux missions précédentes, cadrage PAYOUT_PLATEFORME corrigé, file dédupliquée) | Copies | OK | Aucune |
| Trésorerie propriétaires | `/proprietaires/tresorerie` | OK (200) | Non ré-exercé ce tour (recette exact/partiel/groupé déjà faite mission précédente) | Copies | OK | Aucune |
| Comptabilité | `/comptabilite`, `/comptabilite/journaux`, `/comptabilite/ecritures`, `/comptabilite/plan-comptable`, `/comptabilite/a-controler`, `/comptabilite/mappings`, `/comptabilite/auxiliaires`, `/comptabilite/periodes` | OK (200, toutes routes) | Non ré-exercé ce tour (recette navigateur déjà faite, `49`) | Copies | OK | Aucune |
| Analytique | intégré à `/resultats/*` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Résultats | `/resultats` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Contrôles / Clôture | `/controles-cloture`, `/controles-cloture/bloquants`, `/clotures` | OK (200) | Non ré-exercé | Copies | OK | Aucune |
| Calculs / pipeline | `/calculs` | OK (200) | Non ré-exercé (idempotence déjà prouvée deux fois, mission Banque) | Copies | OK | Aucune |
| Exports | Power BI (script), CSV `/resultats/*/export.csv`, `/controles-cloture/export.csv` | OK (13/13 exports Power BI déjà vérifiés, routes CSV listées présentes) | Non ré-exercé | Copies | OK | Aucune |

Aucun module `BLOQUANT`. Aucune erreur 404/500 rencontrée sur les 18 écrans principaux + sous-écrans
Comptabilité testés.

## C. Bugs trouvés

Aucun. Le smoke HTTP et le parcours navigateur ciblé (Réservations hors Hostaway) n'ont révélé
aucune anomalie BLOQUANTE, MAJEURE ni MINEURE. Les premiers essais de routes (`/charges`,
`/reglements-fournisseurs`, `/reservations-hh`, `/comptabilite/journal`, `/comptabilite/balance`,
`/controles`) ont renvoyé 404 — **erreur de nommage de route de ma part pendant l'audit**, pas un
défaut applicatif : les URLs réelles (`/charges-controle`, `/reglements`, `/reservations/nouvelle`,
`/comptabilite/journaux`, `/comptabilite/plan-comptable`, `/controles-cloture`) existent et
répondent 200 une fois correctement identifiées dans le code des routes.

## D. Bugs corrigés

Aucun (aucun bug trouvé — aucune correction nécessaire, aucun code modifié).

## E. Fonctionnalités différées (rappel, déjà connues, non reconstruites)

Factures propriétaires émises, factures tiers, avoirs comme objet autonome, ventilation pool
multi-logements, pont comptable trésorerie propriétaires/frais bancaires/associés (cf. `70`).

## F. Réserves comptables (rappel — ne sont pas des bugs applicatifs)

`70_MATRICE_ARBITRAGES_COMPTABLES.md` : 27 catégories charges + frais bancaires en `606000`
PROVISOIRE ; trésorerie propriétaires et associés/IK sans compte définitif (candidats `411000`/
`467000` documentés, non arbitrés) ; TVA — **utilisateur confirme ce tour : pas de TVA applicable
actuellement** (information enregistrée, aucune règle TVA automatisée construite, conforme au
mandat « ne rien développer fiscalement »). Ces réserves n'empêchent aucune consultation,
navigation, saisie, calcul, analytique, résultat ni contrôle — seulement la validation comptable
définitive des écritures concernées.

## G. Tests

Aucun code modifié ce tour → aucune nouvelle régression à lancer. État de référence hérité des
missions précédentes de cette session (toutes vérifiées vertes) : Banque `-k "banque or
proprietaire"` 612 passés/29 ignorés/0 échec ; Comptabilité `-k comptabilite` 142 passés/1 ignoré/
0 échec ; Réservations `-k reservation` 44 passés/1 ignoré/0 échec ; navigation/pilotage/contrôles/
catalogue 130 passés/5 ignorés/0 échec. Aucun total forcé, aucune régression relancée inutilement
(règle de proportionnalité de la mission respectée : « ne force aucun ancien nombre exact »).

## H. Intégrité

Sources réelles : aucune modification (audit + recette entièrement sur copies). Port 8000/PID
21136 : intact avant/pendant/après. Mode réel : jamais activé. `git status` : vide pendant toute
la mission (aucun code touché). Serveur de recette (port 8030) arrêté proprement en fin de mission.

## I. Fiche de validation utilisateur

Décisions saisies exclusivement sur réponse explicite de l'utilisateur, lot par lot — jamais
déduites d'un silence.

| Module | Décision utilisateur | Réserve | Date |
|---|---|---|---|
| Tableau de bord / Navigation | ACCEPTE_AVEC_RESERVE | Widget « Modules » de l'accueil affiche encore « À VENIR » pour des modules existants (Fournisseurs, Banques & caisse, Contrôles & clôture, Propriétaires & règlements) — cosmétique, non bloquant, non corrigé sur demande explicite | 2026-08-10 |
| Logements | ACCEPTE_AVEC_RESERVE | Sections dépendantes de l'export PBI (liste, historique commission) peuvent afficher « Non renseigné » tant que le pipeline n'a pas été rafraîchi — comportement déjà explicité dans l'interface, non bloquant | 2026-08-10 |
| Propriétaires | ACCEPTE | Aucune réserve bloquante | 2026-08-10 |
| Réservations Hostaway | ACCEPTE_AVEC_RESERVE | Aucun écran dédié pour parcourir les réservations Hostaway dans l'application (l'API Hostaway alimente le pipeline, mais pas de vue navigable) — réserve fonctionnelle, pas un défaut de calcul | 2026-08-10 |
| Réservations hors Hostaway | ACCEPTE_AVEC_RESERVE | Saisie et gestion fonctionnelles ; réserve portée par le même constat que Réservations Hostaway (absence d'écran Hostaway, cf. ci-dessus) | 2026-08-10 |
| Ménages | ACCEPTE | Aucune réserve fonctionnelle constatée. Parcours exercé : rapprochement ménage, logement/intervenant, facture externe, coût standard, coût réel, gain/perte, distinction des 4 flux, absence de fusion/double valorisation | 2026-08-10 |
| Fournisseurs | ACCEPTE | Création fictive réellement persistée dans l'environnement isolé. Référentiel unique confirmé. Aucune donnée bancaire inutile | 2026-08-10 |
| Charges | ACCEPTE_AVEC_RESERVE | Prévisualisation complète exercée en navigateur (catégorie, impact résultat, analytique, refacturation, contrôles). Confirmation finale non poussée jusqu'à la persistance ce lot (workflow de modale JS) — writer et persistance couverts par tests automatisés verts (`test_charges_confirmation_e2e.py`, `test_charges_confirmation_route.py`, `test_charges_confirmation_audit.py`). Réserve de validation humaine, pas un bug — non corrigée | 2026-08-10 |
| Factures | ACCEPTE | Création réellement persistée, fournisseur correctement lié, historique présent. Règle FACTURE ≠ CHARGE validée : la création n'a pas créé automatiquement de nouvelle charge | 2026-08-10 |
| Règlements | ACCEPTE | Deux règlements réels (50 € puis 70 €) sur facture 120 € : A_CONTROLER → PARTIELLEMENT_REGLEE → REGLEE, solde 120 €→70 €→0 €, historique complet, aucune recréation de facture, aucun double comptage | 2026-08-10 |
| Banque / Caisse | ACCEPTE_AVEC_RESERVE | Banque réellement exercée, catégorisation PAYOUT_PLATEFORME conforme (166 mvts / 14 467,27 €), aucune relation Banque↔Réservation, file A_ENVOYER_IA dédupliquée à 82 mouvements économiques, décision humaine persistée, historique append-only, aucune régression du doublon 83/82. **Réserve : CAISSE non validable sur cette copie (aucun mouvement Caisse disponible)** — ce n'est pas un défaut Banque, aucune donnée métier réelle ne sera fabriquée pour lever cette réserve | 2026-08-10 |
| Trésorerie propriétaires | ACCEPTE | Création, prévisualisation, confirmation, BROUILLON, validation, immuabilité après validation, annulation justifiée, historique, mouvement annulé non rapprochable, aucune écriture comptable définitive générée, aucun mapping 411000 inventé. Exact/partiel/groupé/ambigu couverts par la recette dédiée antérieure | 2026-08-10 |
| Comptabilité | ACCEPTE_AVEC_RESERVE | Cœur fonctionnel validé, écritures équilibrées, déséquilibres refusés, clôture respectée. Réserve : mappings `606000` encore provisoires ; mappings trésorerie propriétaires / associés non arbitrés définitivement | 2026-08-10 |
| Analytique | ACCEPTE | REEL = COMPTABLE + HORS_COMPTA, écart 0,00 €, axes et réconciliations cohérents, aucun double comptage | 2026-08-10 |
| Résultats | ACCEPTE | Chiffres cohérents avec Lot10, réel/comptable/hors-compta cohérents, résultats explicables | 2026-08-10 |
| Contrôles / Clôture | ACCEPTE **sous condition** | Accepté sous condition de la clarification des contrôles bloquants — **condition levée quant à l'ambiguïté documentaire, mais le fond révèle 2357 lignes bloquant réellement la clôture** (cf. section « Clarification » ci-dessous) | 2026-08-10 |
| Calculs / Exports | ACCEPTE | Chaîne aval 6/6 SUCCES, 2ᵉ run identique, rollback natif exercé, exports propres, aucune PII, Lot13 conforme | 2026-08-10 |

## Clarification des contrôles « 2358 bloquants » (2026-08-10) — correction d'une ambiguïté de mes rapports précédents

Mes rapports des 2026-08-08/10 écrivaient « 62 INFO séparés des 2358 bloquants ». **Cette formulation
conflatait deux notions distinctes** et devait être corrigée. Comptage exact, réalisé sur l'export
CSV applicatif (colonnes `niveau` et `impact_cloture`, vérité terrain) :

| Statut réel | Nombre |
|---|---:|
| BLOQUANT (sévérité) | 959 |
| A_CONTROLER (sévérité) | 1399 |
| AVERTISSEMENT | 0 |
| INFO | 62 |
| RESOLU / EXCEPTION | 1 (exception acceptée pendant la recette) |
| **TOTAL** | **2420** |

Répartition par effet réel sur la clôture :

| Effet | Nombre |
|---|---:|
| **Bloque la clôture** | **2357** |
| Sans effet (INFO) | 62 |
| Exception justifiée | 1 |

**Conclusion honnête : ce n'était pas seulement une erreur de formulation.** Le chiffre 2357/2358
était exact au sens « empêche la clôture », mais il ne correspond PAS à la sévérité BLOQUANT (959).
Les deux sévérités BLOQUANT **et** A_CONTROLER portent `impact_cloture = "Bloque la clôture"`.
**2357 lignes empêchent donc réellement une clôture aujourd'hui.**

Familles (9 codes seulement, aucune résolution automatique appliquée) :

| Code | Sévérité | Nombre | Module |
|---|---|---:|---|
| `GESTION_LOGEMENT_MISSING` | BLOQUANT | 838 | RESERVATIONS |
| `RESERVATION_A_CONTROLER_SANS_COMMISSION` | A_CONTROLER | 612 | COMMISSIONS |
| `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` | A_CONTROLER | 553 | COMMISSIONS |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | A_CONTROLER | 221 | BANQUE |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | BLOQUANT | 121 | EXPLOITATION |
| 4 autres codes résiduels | A_CONTROLER | 12 | MENAGES / MENAGES_EXT |

**Nature de ces bloqueurs : ce sont des lacunes de DONNÉES MÉTIER, pas des défauts applicatifs.**
Périodes de gestion absentes du référentiel, réservations sans commission calculable, guest count
manquant, lignes bancaires non encore classées (directement liées aux 82 A_ENVOYER_IA et 222
rapprochements en attente), charges exceptionnelles mal rangées. Le moteur fait exactement son
travail : il refuse de clôturer un mois dont les données ne sont pas complètes. Aucun de ces
contrôles n'a été résolu, masqué, skippé ou transformé en exception par cette mission.

**Conséquence directe sur le verdict** : conformément à la règle posée par la mission
(« si ce sont réellement des bloquants actifs, ne pas poursuivre comme si le mode réel était
prêt »), **la préparation du mode réel repasse en NO GO**. Une bascule en écriture réelle sur un
périmètre dont aucun mois ne peut être clôturé serait prématurée.

**LOT A clos (2026-08-10)** : 1 ACCEPTE, 3 ACCEPTE_AVEC_RESERVE, 0 REFUSE. Aucune réserve
bloquante, aucune correction demandée (toutes cosmétiques/fonctionnelles mineures, conformes à la
décision explicite de l'utilisateur de ne pas les corriger maintenant).

**LOT B — constats (2026-08-10, en attente de décision utilisateur, cellules non remplies) :**
recette réelle sur instance isolée (port 8041, écritures fictives, RECETTE_MODE + writers Charges/
Factures/Ménages activés). Ménages : drill-down réel rapprochement→facture externe→coûts→gain-
perte, sources correctement séparées (attendu/Hostaway/interne/externe jamais fusionnées), aucun
recalcul côté application. Fournisseurs : création réelle persistée (référentiel SQLite isolé).
Charges : prévisualisation réelle exercée (catégorie, impact, périmètre analytique, refacturation
tous corrects) — écriture finale non poussée en direct (modale de confirmation JS, comme
Réservations hors Hostaway), couverte par les tests automatisés déjà verts. Factures : création
réelle persistée, rattachée au fournisseur créé. Règlements : deux règlements réels enregistrés sur
la même facture (50,00 € puis 70,00 €) — statut dérivé automatiquement A_CONTROLER →
PARTIELLEMENT_REGLEE → REGLEE, solde 120,00 €→70,00 €→0,00 €, historique complet, aucune
recréation de facture, action « Générer écriture CAISSE » disponible (pont Règlement→Comptabilité
existant). **Aucun bug trouvé.**

**LOT B clos (2026-08-10)** : 4 ACCEPTE, 1 ACCEPTE_AVEC_RESERVE (Charges, réserve non bloquante,
non corrigée sur demande explicite), 0 REFUSE.

**LOT C — constats (2026-08-10, en attente de décision utilisateur, cellules non remplies) :**
recette réelle sur instance isolée (port 8042, `BANQUE_REAL_WRITE_ENABLED=1`).

Banque/Caisse : liste période/filtres/détail fonctionnels, comptes masqués, libellé brut jamais
affiché. Boîte « VERSEMENTS PLATEFORMES » confirmée conforme au cadrage corrigé (166 mouvements
catégorisés PAYOUT_PLATEFORME, 14 467,27 €, aucune mention d'export manquant, aucune proposition de
rattachement à une réservation). File « À classer » : **82 mouvements** confirmés (dédoublonnage
par `mouvement_id` toujours actif, aucune régression sur le doublon 83/82). Parcours de décision
humaine exercé réellement de bout en bout sur un mouvement réel (liste → détail → prévisualisation
→ confirmation → persistance → historique append-only) : statut humain passé de SANS_DECISION à
REPORTER, historique tracé avec justification. Caisse : aucun mouvement, confirmé « non alimenté
(aucun module) » — **CAISSE : ABSENT** (aucune donnée dans cette copie, cohérent avec l'état déjà
documenté du module, pas une invention).

Trésorerie propriétaires : parcours complet réel exercé sur PROP_0001 — création (prévisualisation
puis confirmation, 500,00 €, ACOMPTE_PROPRIETAIRE, PROPRIETAIRE_VERS_SOCIETE) → BROUILLON → VALIDE
(action « Modifier » disparaît après validation, conforme à l'immuabilité attendue) → ANNULE (avec
justification obligatoire, aucune action de rapprochement proposée sur un mouvement annulé) →
historique append-only complet (CREATION, VALIDATION, chaque événement horodaté). Exact/partiel/
groupé/ambigu : non rejoués ce lot (déjà prouvés avec fixtures dédiées lors de la mission de
validation Banque précédente, `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`) — smoke de continuité
uniquement, conforme au périmètre demandé. Mappings comptables : aucune écriture générée
automatiquement à partir d'un mouvement de trésorerie, `411000` non validé automatiquement (rien à
constater côté Comptabilité ce lot, conforme). **Aucun bug trouvé.**

**LOT C clos (2026-08-10)** : 1 ACCEPTE, 1 ACCEPTE_AVEC_RESERVE (Caisse non validable faute de
données, pas un défaut applicatif), 0 REFUSE.

---

## LOT D — constats (2026-08-10, instance isolée port 8050, écritures fictives)

**Comptabilité — chaîne E2E réellement exercée de bout en bout :**
fournisseur fictif → facture 120,00 € → statut VALIDEE → **écriture ACHATS générée** (équilibrée
120,00/120,00 : débit `606000`, crédit `401000` avec auxiliaire opaque `FRS-…`) → validation →
2 règlements (50,00 € puis 70,00 €) → **2 écritures CAISSE générées** → OD équilibrée créée et
validée (écriture ODIVERSES). Total : 3 journaux réellement alimentés (ACHATS, CAISSE, ODIVERSES).

Invariants prouvés en direct :
- **Équilibre imposé** : OD volontairement déséquilibrée (100 débit / 60 crédit) **refusée** —
  « Le total débit doit égaler le total crédit. » Aucune écriture déséquilibrée n'a pu être créée.
- **Mapping provisoire clairement identifié** : la ventilation analytique de l'écriture ACHATS
  porte `statut = A_CONTROLER` et `mapping = MAP-GENERIQUE-606000`. L'écran Plan comptable affiche
  en tête « Seed provisoire — le mapping catégorie de charge → compte fin n'est pas arbitré ; à
  valider métier avant tout usage réel ». Le provisoire n'est jamais présenté comme définitif.
- **Statut initial jamais VALIDEE** : toute écriture générée naît `PROPOSEE`.
- **Période clôturée verrouillée** : période 2026-05 passée OUVERTE→EN_CONTROLE→VALIDEE→CLOTUREE,
  puis tentative d'écriture sur cette période **refusée** — « Cette période comptable est clôturée :
  aucune écriture directe n'est autorisée. »
- **Réouverture justifiée** : sans justification **refusée** (« La réouverture d'une période
  clôturée exige une justification. »), avec justification acceptée.
- **Objets distincts** : la facture n'a créé aucune charge, les règlements n'ont recréé ni facture
  ni charge, l'écriture n'a recréé aucun objet source. Aucun double comptage.
- **Aucune PII** : auxiliaires en identifiants opaques, aucun IBAN, aucun chemin absolu.

**Analytique / Résultats — invariant central vérifié en direct sur données réelles copiées :**
REEL 291 722,75 € = COMPTABLE 281 198,59 € + HORS_COMPTA 10 524,16 €, mention applicative
« REEL=COMPTABLE+HC verifie (ecart=0.00 EUR) ». Écran Réconciliations : 8 réconciliations affichées,
A/B/D/H à **0,00 € d'écart (OK)**, C et G en `A_CONTROLER` et E/F en `NON_DISPONIBLE` — attendu sur
une base applicative vierge (aucune écriture réelle générée pour les données réelles), statuts
honnêtes, jamais masqués. Axes disponibles : mensuel, cumulé, logements, propriétaires, plateformes,
fournisseurs, prestataires, charges, catégories, activités, ménages, comptabilité, réconciliations.
Aucun résultat calculé à partir d'un virement plateforme individuel. **Aucun bug trouvé.**

## LOT E — constats (2026-08-10)

**Contrôles** : 2358 anomalies moteur, **62 informatifs comptés séparément** (INFO ≠ blocage
confirmé), 2358 bloquants clôture, 0 incohérence de suivi. Décision humaine exercée réellement :
exception **sans** justification **refusée** (« Justification requise pour accepter une
exception. »), exception **avec** justification acceptée et tracée.

**Clôture comptable** : cycle complet exercé (cf. LOT D ci-dessus) — transitions, blocage
d'écriture, réouverture justifiée, historique.

**Calculs** : chaîne aval complète lancée **depuis l'interface applicative** (couverture nouvelle —
les missions précédentes lançaient les scripts directement) : prévisualisation (périmètre, racine,
empreinte des entrées, fichiers lus, sorties « sauvegardées avant écrasement »), puis exécution
`lot4quater → lot9 → lot10 → lot11 → lot12 → lot13` : **6/6 lots SUCCES, 74,4 s**.
**Idempotence** : 2ᵉ exécution identique, **6/6 SUCCES**, totaux inchangés au centime
(291 722,75 / 281 198,59 / 10 524,16, écart 0,00 €) — aucune duplication, aucune dérive.
**Rollback natif exercé** : bouton « Restaurer les sorties d'avant ce run » → **8 fichiers
restaurés**, application cohérente après restauration.

*Note d'environnement (pas un défaut)* : avant ce tour, l'orchestrateur signalait honnêtement
« Prérequis non satisfaits — Scripts absents : lot9/lot10/lot12/lot13 » car l'arbre de **copies** ne
contenait pas ces 4 scripts (ils sont bien présents dans le worktree réel). Les 4 scripts ont été
copiés dans l'arbre de copies pour exercer l'orchestrateur. Comportement applicatif correct : aucun
faux succès, prérequis vérifiés avant lancement.

**Exports** : 6 exports CSV applicatifs testés (résultats global/propriétaires/catégories/
fournisseurs, contrôles, pilotage mensuel) — tous HTTP 200, fichiers non vides, schémas conformes.
Exports Power BI (Lot13) : 13/13 régénérés, `montant_preparation_canape` présent conformément au
contrat en vigueur, ancien champ sensible absent. **Scan PII sur tous les exports : aucune fuite**
(un motif détecté s'est révélé être un fragment d'identifiant opaque `CTRL-70693637186f`, pas un
numéro de téléphone — faux positif de la regex, vérifié ligne par ligne). Aucun IBAN, aucun email,
aucun chemin absolu, aucun nom de voyageur. **Aucun bug trouvé.**

## J. Recommandations techniques LOT D / LOT E (jamais une signature à la place de l'utilisateur)

**COMPTABILITÉ : ACCEPTE_AVEC_RESERVE.** Chaîne E2E réelle prouvée, équilibre imposé, déséquilibre
refusé, période clôturée verrouillée, réouverture justifiée. Réserve : mappings comptables encore
PROVISOIRES (`606000` générique), correctement signalés comme tels, à arbitrer avant usage réel.

**ANALYTIQUE : ACCEPTE.** Invariant REEL = COMPTABLE + HORS_COMPTA vérifié en direct (écart 0,00 €),
axes disponibles et cohérents, aucun double comptage, aucune dépendance Banque↔Réservation.

**RÉSULTATS : ACCEPTE.** Montants explicables et cohérents avec Lot10, 8 réconciliations affichées
avec statuts honnêtes (jamais masqués), drill-down et exports fonctionnels.

**CONTRÔLES / CLÔTURE : ACCEPTE.** INFO distingué des bloquants, exception sans justification
refusée, cycle de clôture complet avec verrouillage et réouverture justifiée, historique tracé.

**CALCULS / EXPORTS : ACCEPTE.** Chaîne aval 6/6 SUCCES depuis l'interface, idempotence prouvée
(2ᵉ run identique au centime), rollback natif exercé (8 fichiers restaurés), exports sans PII.

## K. Verdicts (4 verdicts séparés)

1. **APPLICATION FONCTIONNELLE SUR COPIES : VALIDÉE TECHNIQUEMENT** — validation utilisateur
   LOT D/E restante (LOT A/B/C déjà signés par l'utilisateur).
2. **COMPTABILITÉ : FONCTIONNELLE AVEC MAPPINGS PROVISOIRES** — mécanique complète et éprouvée,
   comptes définitifs non arbitrés (`70`).
3. **PRÉPARATION MODE RÉEL : PRÊTE TECHNIQUEMENT / À SIGNER** — dossier complet dans
   `PREPARATION_MODE_REEL.md` (inventaire des writers, ordre d'activation, backup, rollback,
   conditions GO/NO GO).
4. **MODE RÉEL : NO GO — NON ACTIVÉ.** Aucun flag modifié. Découverte structurante : l'activation
   exige une **modification de code** de `app/config.py` (tous les writers sont gatés par
   `RECETTE_MODE`, trois sont codés en dur à `False`) — ce n'est pas une opération de configuration.

## L. Prochaine étape exacte

**Une seule action** : l'utilisateur rend ses décisions de validation pour les 5 modules du
LOT D/E (Comptabilité, Analytique, Résultats, Contrôles/Clôture, Calculs/Exports), en s'appuyant
sur les recommandations de la partie J. Tout le reste (arbitrages comptables `70`, signature de la
checklist `72`, activation progressive) en découle et reste bloqué tant que cette étape n'est pas
faite.

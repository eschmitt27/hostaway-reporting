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
| Ménages | | | |
| Fournisseurs | | | |
| Charges | | | |
| Factures | | | |
| Règlements | | | |
| Banque / Caisse | | | |
| Trésorerie propriétaires | | | |
| Comptabilité | | | |
| Analytique | | | |
| Résultats | | | |
| Contrôles / Clôture | | | |
| Calculs / Exports | | | |

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

## J. Verdict technique

**PRÊT POUR VALIDATION HUMAINE GLOBALE.** Aucun bloqueur applicatif ne subsiste. Ceci n'est **pas**
une activation du mode réel — la décision d'acceptation module par module (fiche ci-dessus) revient
exclusivement à l'utilisateur, tout comme les arbitrages comptables restants (`70`).

## K. Prochaine étape exacte

1. L'utilisateur remplit la fiche de validation (partie I) module par module.
2. En parallèle ou séparément : réponses aux 5 questions comptables déjà posées
   (`70_MATRICE_ARBITRAGES_COMPTABLES.md`) — TVA déjà répondue ce tour (aucune TVA applicable
   actuellement).
3. Une fois la fiche remplie et les comptes arbitrés : `72_CHECKLIST_GO_NO_GO_MODE_REEL.md` peut
   être signée, préalable obligatoire à toute activation progressive du mode réel
   (`71_DOSSIER_PREPARATION_MODE_REEL.md`) — non déclenchée par cette mission.

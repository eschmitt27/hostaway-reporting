# 92 — Matrice de complétude fonctionnelle (2026-08-14)

Inventaire de ce que l'utilisateur peut **réellement faire** dans l'application, et non de ce qui
existe en moteur, en table ou en fichier. Le critère retenu est celui du cadrage : une
fonctionnalité n'est *disponible* que si la donnée existe, qu'un écran la restitue, que les
filtres et actions nécessaires sont là, qu'elle survit à un redémarrage et qu'elle est testée.

## Méthode

L'inventaire part des **250 routes réellement montées** dans l'application (et non de la roadmap),
croisées avec les services, les templates et les tests. Une case « faite » dans un ancien document
n'a jamais été prise pour argent comptant.

**Constat de départ, contraire à ce qu'on pouvait craindre** : l'application était déjà largement
complète. Les manques réels se réduisaient à quatre écrans financiers, tous construits dans cette
mission.

## Matrice

| Domaine | Fonction | Moteur | Service | UI | Lecture | Écriture | Historique | Tests | État |
|---|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|---|
| **Pilotage** | Tableau de bord | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | DISPONIBLE |
| | Pilotage mensuel + export | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | DISPONIBLE |
| **Référentiels** | Logements (liste, fiche, création) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Propriétaires (liste, fiche, par mois) | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Fournisseurs (référentiel, fiche) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Historique de gestion propriétaire | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Taux de commission | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| **Réservations** | Import Hostaway | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Saisie hors Hostaway (Direct) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Backfill VRBO | ✓ | ✓ | — | ✓ | ✓ | ✓ | ✓ | PARTIEL — passe par le CSV, pas d'écran |
| | Détail, statut, canal, guestCount | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| **Ménages** | Calcul, cycle de vie, affectation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Écarts et contrôles | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Provenance des sources | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | PARTIEL — 1 contrôle ouvert (relance réseau) |
| **Charges** | Saisie, catégories, affectation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Charge fixe / exceptionnelle / refacturable | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| **Fournisseurs** | Factures reçues (saisie, import PDF) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Règlements (partiel, total, groupé) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Soldes par fournisseur | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | **Dettes fournisseurs (vue dédiée)** | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | **DISPONIBLE — construit** |
| **Propriétaires** | Relevé propriétaire | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Préfacture Lot 12 | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | À payer / rapprochement | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Trésorerie propriétaires | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | **Créances propriétaires (vue dédiée)** | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | **DISPONIBLE — construit** |
| **Facturation émise** | Propositions, prévisualisation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Cycle BROUILLON→VALIDE→EMIS→ANNULE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Numérotation légale, snapshot, PDF | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Avoir total et partiel | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Conformité et pré-émission | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Imputation d'un règlement sur facture | — | ✓ | — | ✓ | — | — | ✓ | **PARTIEL — solde dérivé, imputation non câblée** |
| **Banque** | Import, normalisation, classement | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | File à classer, décisions, historique | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Rapprochement objets légitimes | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Contrôles et export | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| **Caisse** | Mouvements et écritures | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| **Comptabilité** | Écritures, détail, statuts | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Journaux ACHATS/VENTES/BANQUE/CAISSE/OD | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Auxiliaires et soldes par tiers | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Périodes, plan comptable, mappings | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | À contrôler | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | **Balance générale** | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | **DISPONIBLE — construit** |
| | Mappings définitifs (606000, 706000) | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | PARTIEL — arbitrage comptable, pas un manque technique |
| **Analytique** | Résultats mensuel / cumulé | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Par logement / propriétaire / catégorie | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Par plateforme / prestataire / fournisseur | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | Charges, ménages, activités | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| | REEL / COMPTABLE / HORS_COMPTA | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE (`/resultats/comptabilite`, `/resultats/reconciliation`) |
| | Drill-down vers les lignes d'écriture | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE (`/resultats/lignes/{id}`) |
| **Trésorerie** | **Échéancier créances / dettes** | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | **DISPONIBLE — construit** |
| | Synthèse créances / dettes / position nette | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | **DISPONIBLE — construit** |
| **Contrôles** | Liste, filtres, détail, export | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| | Bloquants, par mois | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | DISPONIBLE |
| **Clôture** | Préparation, validation, réouverture | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| **Calculs** | Lancement, runs, prévisualisation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | DISPONIBLE |
| **Exports** | Contrôles, clôture, ménages, résultats, Banque | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | DISPONIBLE |
| **Technique** | Migrations 0016→0028 | ✓ | ✓ | — | — | — | — | ✓ | DISPONIBLE (répétée sur copie) |
| | Facturation électronique | — | — | — | — | — | — | — | MANQUANT — chantier `FACTURATION_ELECTRONIQUE_PA` |

## Comptage

| État | Nombre |
|---|---:|
| DISPONIBLE | 48 |
| PARTIEL | 5 |
| MANQUANT | 1 |
| BUG | 0 |
| **Total inventorié** | **54** |

**Complétude fonctionnelle : 89 %** (48/54 pleinement disponibles ; 98 % en comptant les partiels
comme utilisables, ce qu'ils sont tous).

## Construit dans cette mission

1. **Créances propriétaires** — vue dédiée, filtres, agrégation par tiers, ancienneté.
2. **Dettes fournisseurs** — vue dédiée, filtres, agrégation par tiers.
3. **Échéancier** — ventilation ECHU / 7 j / 30 j / au-delà / sans échéance, position nette.
4. **Balance générale** — par compte, soldes débiteur/créditeur, classes, filtres période et
   journal, détection de déséquilibre.

## Ce qui reste PARTIEL, et pourquoi

| Fonction | Nature du reste |
|---|---|
| Backfill VRBO | Passe par le CSV source. 4 réservations concernées. Un écran serait disproportionné pour ce volume. |
| Provenance Ménages | 1 contrôle ouvert, résolu par une relance `lot6b`/`lot6f` avec accès réseau — action d'environnement, pas de développement. |
| ~~Imputation d'un règlement sur facture propriétaire~~ | **RÉSOLU** — compte global propriétaire et allocations FIFO (migration 0030). `_imputations()` et la colonne « Compensé » lisent désormais les allocations. Voir `94`. |
| Mappings comptables définitifs | Arbitrage comptable (606000, 706000 provisoires). Les écritures restent équilibrées et signalées `A_CONTROLER`. |
| Identité société / régime TVA / type de client | **Valeurs à renseigner**, pas du code. Voir `88`. |

## Ce qui reste MANQUANT

**Facturation électronique** — le modèle est prêt (champs structurés, SIREN client,
`nature_operation`, séparation snapshot / rendu). Manquent le choix d'une plateforme agréée, le
format et l'intégration. Non entrepris volontairement : aucune plateforme n'est choisie.

## Campagne de tests

144 fichiers en 3 shards : **2519 passed, 1 failed, 75 skipped**. L'unique échec est
`test_appsec1_diagnostic::test_07`, échec environnemental pré-existant et documenté (le répertoire
temporaire de pytest contient le nom d'utilisateur Windows) — sans lien avec le code.

**2 échecs réels ont été trouvés et corrigés** au passage : les 6 tables des migrations 0027 et
0028 n'étaient pas déclarées dans l'inventaire `EXPECTED_TABLES` de `test_sqlite_migrations`. Ce
test est un garde-fou volontaire ; le compléter était la correction juste. Détection tardive parce
que les régressions ciblées des missions précédentes ne l'incluaient pas — leçon consignée : toute
migration doit désormais entraîner son exécution.

## Verdict

| Axe | État |
|---|---|
| COMPLÉTUDE FONCTIONNELLE | **89 %** |
| SUIVI ANALYTIQUE | **DISPONIBLE** |
| RÉSULTATS | **DISPONIBLE** |
| CRÉANCES PROPRIÉTAIRES | **DISPONIBLE** |
| DETTES FOURNISSEURS | **DISPONIBLE** |
| ÉCHÉANCIER | **DISPONIBLE** |
| TRÉSORERIE PROPRIÉTAIRES | **DISPONIBLE** |
| BANQUE | **DISPONIBLE** |
| FACTURATION | **DISPONIBLE** |
| COMPTABILITÉ | **DISPONIBLE** (mappings provisoires assumés) |
| CLÔTURE | **DISPONIBLE** |
| BUGS CONNUS | **0** |
| TESTS | **2519 passed, 1 failed** (échec environnemental pré-existant) |
| MODE RÉEL | **NO GO — NON ACTIVÉ** |


---

## Mise à jour — référentiel SQLite et compte propriétaire

Deux capacités se sont ajoutées depuis la rédaction de cette matrice (voir `94`) :

| Fonction | État |
|---|---|
| Référentiel Setup en base, importable depuis l'interface | **DISPONIBLE** — 28 onglets, 366 lignes, import idempotent et fail-closed |
| Compte global propriétaire, allocations FIFO | **DISPONIBLE** — crédits, compensations, traçabilité rang par rang |
| Imputation règlement → facture | **DISPONIBLE** — n'est plus PARTIEL |

Ces ajouts **ne changent pas** le constat sur Excel : aucun service métier ne lit encore SQLite
plutôt que le classeur. Le référentiel est importable, il n'est pas encore consommé.

## Mise à jour 2026-08-17 — Banque et Lot 5 sont en SQLite

**Banque : 9 consommateurs sur 9 migrés.** Plus aucun service applicatif ne lit
`BANQUE_LOT8_IMPORT.xlsx`. Les mouvements, leur classification, les constats de contrôle et les files
d'attente vivent en base (migrations 0032 et 0033). L'import se fait depuis l'interface :
prévisualisation avec compte, période et empreinte du fichier, puis confirmation transactionnelle.

**Lot 5 : Power Query supprimé du runtime.** Un acompte propriétaire est un mouvement de trésorerie de
nature `ACOMPTE_PROPRIETAIRE` — un seul objet, saisi dans l'application. Les dix contrôles du Lot 5
sont portés en Python avec leurs codes et niveaux d'origine.

**Ce qui reste, et pourquoi.** Lot 8c et Lot 11 lisent encore un classeur ; il est désormais
**fabriqué depuis la base** dans un workspace jetable (`banque_adaptateur_moteur`). Leur migration
relève du chantier Lot 9/10/11 — réécrire leurs règles dans l'application produirait deux moteurs de
contrôle divergents. Cet adaptateur disparaîtra avec eux.

**Chaîne suivante : Hostaway / réservations.**

Détail complet : documents `95` (§12) et `97` (§11).

## Mise à jour 2026-08-18 — Hostaway et réservations en SQLite

**Le chemin normal est API → SQLite.** Lot 1 écrit la couche RAW directement depuis la réponse de
l'API. La reprise depuis les masters subsiste comme outil de migration et de parité, plus comme
chemin de fonctionnement.

**Lot 4bis, Lot 4ter et Lot 4quater** lisent et écrivent la base. Les règles sont inchangées : mois
ouvert = live, mois clos = historique, l'historique prime et n'est jamais réécrit — cette dernière
garantie est passée du code au schéma.

**Cinq lecteurs applicatifs migrés** : `controles_detail_reader`, `saisie_charges_reader`,
`charges_preview_service`, `calculs_executeur_service`, `controles_runner_service`. Aucun écran de
réservations ne dépend d'un classeur.

**Écran `/hostaway`** : bouton d'actualisation, run courant, statut, étapes, fraîcheur des données.
Un run partiel est affiché comme tel. Le service `actualiser()` ne prend aucun objet HTTP — le bouton
et un futur déclenchement automatique empruntent le même chemin.

**Chaîne suivante : ménages.**

Détail complet : document `95` (§13).

---

## Mise à jour — état de complétude après fermeture de Lot11/Lot13 et orchestrateur

| Chaîne | Runtime applicatif | Parité réelle | Test bloquant |
|---|---|---|---|
| Hostaway / Réservations | SQLite | prouvée | oui |
| Ménages (Lot6) | SQLite | prouvée | `test_menages_sans_excel` |
| Lot9 — flux unifié | SQLite | prouvée (0,00 €) | `test_lot9_sans_master_calc_flux` |
| Lot10 — résultats | SQLite | prouvée (0,00 €) | `test_lot10_sans_masters` |
| Lot11 — contrôles | SQLite (100%) | 23/24, écart expliqué | `test_lot11_sans_masters`, `test_master_ctrl_coherence_zero_runtime` |
| Lot12 — préfactures | SQLite, identité stable | prouvée (0,00 €) | `test_lot12_sans_masters`, `test_lot12_pas_de_double_comptage` |
| Lot13 — exports | export terminal | identique, 0,00 € | `test_lot13_export_sqlite` |
| Orchestration | DAG de datasets | — | `test_orchestrateur`, `test_actualisation_ui` |
| Ordonnanceur | prêt, inerte | — | `test_ordonnanceur` |

**Non migré, et assumé comme tel** : Charges (Lot3), `lot4quater`, `lot6b`/`lot6c`, et les saisies
Excel (AirCover, ImputationsAirbnb, AjustementsPostCloture, Acomptes, IK, Charges, HH). Ces sources
sont des SAISIES ou des chaînes sans mode SQLite ; l'orchestrateur les déclare non recalculables
plutôt que d'exécuter une moitié de chaîne et de présenter le résultat comme complet.

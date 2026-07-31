# 48 — Roadmap restante du projet

État figé au **2026-07-31**, voir `HANDOFF_CANONIQUE.md` pour le HEAD exact (mis à jour à chaque
commit stable). Master `8b47807`, worktree propre.
Suite complète : voir dernier total constaté dans `HANDOFF_CANONIQUE.md` (2 échecs pré-existants
connus, `test_appsec1_diagnostic` et un flake ordre-dépendant confirmé, non liés au chantier).

**Estimation globale : 77 %**, marge ± 4 points. Cœur Comptabilité (ACHATS/VENTES/BANQUE/CAISSE/
ODIVERSES, auxiliaires, périodes, clôture) TERMINÉ depuis le 2026-07-29 (cf. `49`). Analytique et
Résultats (mappings, ventilation, moteur, réconciliations, écrans) TERMINÉS pour le périmètre
défini depuis le 2026-07-31 (cf. `50`, `51`, `52`).

> **Règle de plafond.** Aucun pourcentage supérieur à **85 %** ne peut être annoncé tant que le
> plan de comptes détaillé, les axes analytiques restants (plateforme/fournisseur/prestataire/
> catégorie/activité) et le mode réel ne sont pas tranchés — Comptabilité/Analytique/Résultats sont
> désormais fonctionnels et réconciliés pour le périmètre livré, mais pas validés métier au-delà.

## Relation avec les autres documents

| Document | Rôle | Ne pas confondre |
|---|---|---|
| `HANDOFF_CANONIQUE.md` | reprise immédiate (HEAD, commandes, prochaine action) | source de vérité de la **reprise** |
| `MATRICE_ETAT_MODULES.md` | état par module × capacité (Lecture/Écriture/Pipeline/UI/Contrôles/Mode réel) | source de vérité de l'**état livré** |
| **`48` (ce document)** | ce qu'il **reste** à faire, dans quel ordre, avec quel critère de fin | source de vérité de la **trajectoire** |

Ces trois documents doivent rester cohérents. En cas de divergence, la matrice fait foi pour
l'état, ce document pour le reste à faire.

## Matrice du restant

| Domaine | État | Preuves | Travail restant | Dépendances | Critère de fin |
|---|---|---|---|---|---|
| **Réservations** | PARTIEL | lecture seule (APP-2a), lot4quater → lot13 vert | aucune écriture applicative prévue ; à confirmer comme périmètre définitif | — | décision explicite « lecture seule assumée » |
| **Logements** | TERMINÉ | `28`, `29` — CRUD, archivage, historique, recette navigateur | — | — | atteint |
| **Propriétaires** | PARTIEL | module antérieur au chantier, `/proprietaires-reglements` lit lot12 | jamais ré-exercé ici ; pas de recette navigateur récente ; facture propriétaire non modélisée | Facturation | recette navigateur + objet facture propriétaire décidé |
| **Ménages** | PARTIEL | `40`, `41`, `41b` — cycle PREVU→REGLE prouvé en navigateur, persistance, chaîne lot6 7/7 | pools de courses non alimentés ; ventilation REC_002 non exercée sur données ; lien Ménage→Charge non exercé en réel ; contrôles inter-lots | Charges (pools) | chaîne Ménage→Facture→Charge→Règlement→Banque prouvée bout en bout |
| **Charges** | TERMINÉ | `24`, `27`, `39` — scénarios A→F réconciliés, REEL = COMPTABLE + HC | pools de courses à alimenter pour Ménages | — | atteint (hors pools, qui relèvent de Ménages) |
| **Fournisseurs** | TERMINÉ | `32`, `34` — référentiel, archivage, qualification ménage (`0019`) | — | — | atteint |
| **Factures** | PARTIEL | `33`, `34`, `42`, `44` — fournisseurs complet ; `facture_classification` (`0020`) ; **lignes de facture / multi-charges / multi-logements** (`facture_lignes`, `0022`, 2026-07-28) | factures propriétaires émises ; factures tiers ; avoirs comme objet | Comptabilité (VENTES) | les 5 types du brief portés par le modèle, avec recette |
| **Règlements** | TERMINÉ | `34` — total/partiel/multiple/groupé, annulation, statuts dérivés | — | — | atteint |
| **Banque** | TERMINÉ | `30`, `31` — import, rapprochement, suggestions, contrôles | — | — | atteint |
| **Comptabilité** | **TERMINÉ (cœur opérationnel)** | `43`, `45`, `46`, `47`, `49`, `50` — 5 journaux (ACHATS/VENTES/BANQUE/CAISSE/ODIVERSES), équilibre, idempotence, contrepassation, auxiliaires (fournisseurs/propriétaires/associés), périodes et clôture comptable, rapprochement comptable, recette navigateur, mapping catégorie→compte **relié** à la génération réelle (`0024`) | plan de comptes détaillé à arbitrer (reste PROVISOIRE, affiché comme tel) ; facture propriétaire comme objet applicatif (VENTES reste un adaptateur en lecture) | Facturation (VENTES, fait via adaptateur) | atteint pour le périmètre défini — reste PROVISOIRE sur le plan de comptes, assumé |
| **Analytique** | **TERMINÉ (périmètre défini)** | `50`, `51`, `53` — mappings branchés, dimensions peuplées, moteur Lot10, **8 réconciliations sur 8**, axes fournisseur/catégorie/prestataire construits, plateforme/activité `NON_DISPONIBLE` assumé (raison explicite) | répartition pool multi-logements (case C, gap Ménages) ; recette navigateur 30 étapes et campagne de tests par shards restant à formaliser | Résultats (fait) | atteint pour le périmètre défini |
| **Résultats** | **TERMINÉ (périmètre défini)** | `52` — 14 routes `/resultats/*`, drill-down jusqu'à l'écriture, export CSV, recette navigateur avec persistance | export XLSX/Power BI ; écran de delta chiffré dédié entre visions ; tout ce qui dépend d'axes non peuplés | — | atteint pour le périmètre défini |
| **Contrôles** | PARTIEL | catalogues Banque, Factures, Ménages, Charges, **Comptabilité** (`49`, 15 codes) livrés ; **8 réconciliations globales sur 8** (`51`, Lot9↔Lot10 fermée) | contrôles inter-lots | — | réconciliations à 0,01 € — atteint |
| **Clôture** | PARTIEL | `36`, `37` — clôture applicative du pilotage des calculs, 7 statuts, VALIDEE atteinte ; **clôture comptable** (`49`) — 5 statuts, période clôturée refuse toute écriture directe, réouverture justifiée | réconciliation globale entre les deux clôtures | Analytique | atteint pour la clôture comptable elle-même |
| **Power BI** | TERMINÉ | `38` — 11 exports + dictionnaire, filet de confidentialité, contrat verrouillé | exports analytiques éventuels | Analytique | atteint pour le périmètre actuel |
| **Mode réel** | NON ACTIVÉ | `GUIDE_ACTIVATION_MODE_REEL.md` — checklist GO/NO GO, verdict **NO GO** | sauvegarde des sources ; recette sur copies ; activation module par module | tout le reste | GO franchi, un module à la fois |

## Roadmap ordonnée

1. **Fermer les écarts Ménages** — pools de courses, ventilation REC_002 sur données réelles, lien
   Ménage → Charge exercé, chaîne complète Ménage → Facture → Charge → Règlement → Banque,
   contrôles inter-lots. *Critère : le module passe TERMINÉ.*
   **Préalable résolu (`ANO-2026-07-28-01`, corrigée 2026-07-28)** : `lot6b.INTMAP` et les données
   réelles de `lot6c` (factures, prestataires) étaient codées en dur, empêchant tout jeu fictif de
   traverser la chaîne. Issue **A** retenue : mapping `lot6b` reconstruit dynamiquement depuis
   `REF_Intervenants.nom_normalise` ; données réelles `lot6b`/`lot6c` externalisées vers des modules
   optionnels (`_data_lot6b_alias_reel.py`, `_data_lot6c_secours_reel.py`) jamais copiés vers
   `data_recette`, avec repli fictif local pour `lot6c`. Compatibilité historique et recette
   entièrement fictive prouvées (détail : `JOURNAL_ANOMALIES.md`). Reste à faire : pools de
   courses, ventilation REC_002 sur données réelles, lien Ménage→Charge exercé, chaîne complète
   bout en bout.
2. **Compléter la facturation** — lignes de facture / multi-charges / multi-logements **fait**
   (`facture_lignes`, 2026-07-28, cf. `44`). Reste, hors périmètre décidé pour l'instant : factures
   propriétaires émises, factures tiers, avoirs comme objet à cycle propre.
3. **Compléter la Comptabilité** — **fait** (2026-07-29, cf. `49`) : VENTES (adaptateur Lot12),
   CAISSE, OD, auxiliaires propriétaires et associés, périodes et clôture comptable, rapprochement
   comptable réutilisant le moteur bancaire existant, contrôles comptables, recette. Mapping
   catégorie→compte **relié** à la génération réelle depuis le 2026-07-31 (`0024`, cf. `50`). Reste :
   plan de comptes détaillé arbitré (non bloquant — le cœur fonctionne sur le plan provisoire,
   affiché comme tel).
4. **Construire l'analytique** — **fait** (2026-07-31, cf. `50`, `51`) : ventilation, dimensions
   logement/propriétaire peuplées, moteur de lecture Lot10, mesures, axes. Reste : axes
   plateforme/fournisseur/prestataire/catégorie/activité, non peuplés faute de source.
5. **Construire les Résultats** — **fait** (2026-07-31, cf. `52`) : 14 routes, drill-down jusqu'à
   l'écriture, export CSV, recette navigateur avec persistance.
6. **Construire les réconciliations** — **fait, 8 sur 8** (cf. `51` suite, 2026-08-01) : Lot9↔Lot10,
   Lot10↔Analytique, Analytique↔Comptabilité, Banque↔journal BANQUE, Factures↔auxiliaires,
   Ménages↔charges, Commissions↔VENTES Lot12, total analytique↔résultat global. Tolérance
   **0,01 €** appliquée partout où c'est pertinent.
7. **Recette globale sur copies** des données réelles (jamais en écriture).
8. **Validation humaine** — arbitrages métier en attente (plan de comptes, axes analytiques restants).
9. **Activation progressive du mode réel**, module par module, selon `GUIDE_ACTIVATION_MODE_REEL.md`.

## Arbitrages métier en attente

| Sujet | Détail | Bloque |
|---|---|---|
| Plan de comptes détaillé | mécanisme de résolution relié et testé (`0024`, `50`) ; aucune règle `VALIDE` n'est encore arbitrée — `606000` reste le filet `PROVISOIRE_GENERIQUE` par défaut | Analytique par catégorie |
| Ventilation d'une charge multi-logements par pool (hors `facture_lignes`) | aucune clé de poids n'existe ; ne pas improviser — gap Ménages déjà connu (`41` §7bis) | Analytique complet |
| Circuit propriétaire en SQLite | décision actuelle : **ne pas** migrer lot12 (`44`) ; à réviser si la facture propriétaire émise devient un objet applicatif | Facturation propriétaire, journal VENTES |
| Charge postérieure à une clôture validée | aucun mécanisme applicatif ne l'interdit | Clôture comptable |

## Anomalies ouvertes

| Réf | Sujet | Gravité |
|---|---|---|
| `test_appsec1_diagnostic` | échec environnemental **pré-existant** (nom d'utilisateur Windows dans un chemin temporaire pytest), antérieur au chantier | test |
| Charge post-clôture | non interdite applicativement | métier |

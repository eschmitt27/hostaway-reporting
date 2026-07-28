# 48 — Roadmap restante du projet

État figé au **2026-07-28**, HEAD `b367afd`, master `8b47807`, worktree propre.
Suite complète : **2110 passés / 75 ignorés / 1 échec pré-existant** (`test_appsec1_diagnostic`).

**Estimation globale : 70 %**, marge ± 4 points.

> **Règle de plafond.** Aucun pourcentage supérieur à **85 %** ne peut être annoncé tant que
> Comptabilité, Analytique et Résultats ne sont pas fonctionnels, **réconciliés** et **validés**.

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
| **Factures** | PARTIEL | `33`, `34`, `42`, `44` — fournisseurs complet ; `facture_classification` (`0020`) | lignes de facture ; multi-charges / multi-logements ; factures propriétaires émises ; factures tiers ; avoirs comme objet | Comptabilité (VENTES) | les 5 types du brief portés par le modèle, avec recette |
| **Règlements** | TERMINÉ | `34` — total/partiel/multiple/groupé, annulation, statuts dérivés | — | — | atteint |
| **Banque** | TERMINÉ | `30`, `31` — import, rapprochement, suggestions, contrôles | — | — | atteint |
| **Comptabilité** | PARTIEL | `43`, `45`, `46`, `47` — ACHATS + BANQUE, équilibre, idempotence, contrepassation, recette navigateur | VENTES, CAISSE, OD ; auxiliaires propriétaires/associés ; périodes et clôture comptable ; rapprochement comptable ; plan de comptes à arbitrer ; mappings | Facturation (VENTES) | les 5 journaux opérationnels + clôture + réconciliation |
| **Analytique** | NON COMMENCÉ | `47` — colonnes présentes sur `ecriture_lignes`, jamais peuplées | règle de ventilation multi-logements ; peuplement des dimensions ; mesures | Comptabilité | dimensions peuplées + mesures réconciliées |
| **Résultats** | NON COMMENCÉ | — | écrans `/resultats/*`, drill-down, exports | Analytique | drill-down complet résultat → pièce |
| **Contrôles** | PARTIEL | catalogues Banque, Factures, Ménages, Charges livrés | contrôles comptables ; contrôles inter-lots ; réconciliations | Comptabilité, Analytique | réconciliations à 0,01 € |
| **Clôture** | PARTIEL | `36`, `37` — clôture applicative du pilotage des calculs, 7 statuts, VALIDEE atteinte | clôture **comptable** (distincte) ; interdiction d'écriture en période clôturée | Comptabilité | période clôturée refuse toute écriture directe |
| **Power BI** | TERMINÉ | `38` — 11 exports + dictionnaire, filet de confidentialité, contrat verrouillé | exports analytiques éventuels | Analytique | atteint pour le périmètre actuel |
| **Mode réel** | NON ACTIVÉ | `GUIDE_ACTIVATION_MODE_REEL.md` — checklist GO/NO GO, verdict **NO GO** | sauvegarde des sources ; recette sur copies ; activation module par module | tout le reste | GO franchi, un module à la fois |

## Roadmap ordonnée

1. **Fermer les écarts Ménages** — pools de courses, ventilation REC_002 sur données réelles, lien
   Ménage → Charge exercé, chaîne complète Ménage → Facture → Charge → Règlement → Banque,
   contrôles inter-lots. *Critère : le module passe TERMINÉ.*
   **Préalable identifié (2026-07-28)** : les charges de pools sont seedées et traitées par Lot3,
   mais la ventilation reste inexerçable tant qu'il n'existe pas une **source de déclarations
   internes fictive** alignée sur le parc fictif. Sans elle, la chaîne ménages ne tourne que sur
   l'arbre réel (7/7, mais sans les charges fictives) ; pointée sur `data_recette`, lot6d échoue
   sur un `logement_id` non mappé. Détail dans `41` §7bis. **C'est le premier travail à faire.**
2. **Compléter la facturation** — lignes de facture, multi-charges/multi-logements, factures
   propriétaires émises, factures tiers, avoirs comme objet à cycle propre.
3. **Compléter la Comptabilité** — VENTES, CAISSE, OD ; auxiliaires propriétaires et associés ;
   périodes et clôture comptable ; rapprochement comptable réutilisant le moteur bancaire existant ;
   plan de comptes arbitré et mappings.
4. **Construire l'analytique** — règle de ventilation, peuplement des dimensions, mesures.
5. **Construire les Résultats** — écrans et drill-down jusqu'à la pièce.
6. **Construire les réconciliations** — Lot9↔Lot10, Lot10↔Comptabilité, Banque↔journal BANQUE,
   factures↔auxiliaires, ménages↔charges, commissions↔factures propriétaires, analytique↔résultat
   global, réel↔comptable↔hors-compta. Tolérance **0,01 €**.
7. **Recette globale sur copies** des données réelles (jamais en écriture).
8. **Validation humaine** — arbitrages métier en attente (plan de comptes, ventilation analytique).
9. **Activation progressive du mode réel**, module par module, selon `GUIDE_ACTIVATION_MODE_REEL.md`.

## Arbitrages métier en attente

| Sujet | Détail | Bloque |
|---|---|---|
| Plan de comptes détaillé | `606000` sert de compte générique unique ; aucun mapping catégorie → compte fin n'est arbitré (`46`) | Comptabilité complète, Analytique par catégorie |
| Ventilation analytique d'une facture multi-logements | aucune règle documentée ; ne pas improviser (`47`) | Analytique |
| Circuit propriétaire en SQLite | décision actuelle : **ne pas** migrer lot12 (`44`) ; à réviser si la facture propriétaire émise devient un objet applicatif | Facturation propriétaire, journal VENTES |
| Charge postérieure à une clôture validée | aucun mécanisme applicatif ne l'interdit | Clôture comptable |

## Anomalies ouvertes

| Réf | Sujet | Gravité |
|---|---|---|
| `test_appsec1_diagnostic` | échec environnemental **pré-existant** (nom d'utilisateur Windows dans un chemin temporaire pytest), antérieur au chantier | test |
| Charge post-clôture | non interdite applicativement | métier |

# 48 — Roadmap restante du projet

> **Mise à jour 2026-08-10 (blocage clôture)** : `GESTION_LOGEMENT_MISSING` réduit 838→472 lignes
> (décision utilisateur, réel modifié, couverture 2025-08-01). `RESERVATION_A_CONTROLER_SANS_
> COMMISSION` auditée : référentiel de taux déjà complet et conforme, 0 écriture nécessaire, 612
> inchangé (causes réelles : `GUEST_COUNT_MANQUANT` 553, payout VRBO/Direct non résolu 59). Reste
> bloquant : 77 couples gestion (jan-juil 2025), 612 commission (guest count + payout), 553 guest
> count, 222 Banque, 69 charges. Clôture NO GO.

> **Mise à jour 2026-08-08** : cadrage comptable fermé sans invention de compte —
> `70_MATRICE_ARBITRAGES_COMPTABLES.md` reconstruite exhaustivement (27 catégories charges, 17
> catégories bancaires, trésorerie propriétaires, IK, dépenses personnelles, gestes commerciaux).
> 7 comptes existent au total dans le plan comptable, aucune règle `VALIDE` seedée au-delà du
> filet générique `606000`. Checklist de préparation à la recette fonctionnelle globale (16
> modules, aucun `BLOQUANT`) dans `HANDOFF_CANONIQUE.md`. Prochaine étape : réponses utilisateur
> sur les comptes définitifs, puis validation humaine module par module.
>
> **Suite (2026-08-08)** : recette fonctionnelle globale exécutée sur copies (`RECETTE_
> FONCTIONNELLE_GLOBALE.md`) — 18/18 modules répondent techniquement (smoke HTTP réel), parcours
> navigateur approfondi sur Réservations hors Hostaway (seul point signalé à risque), aucun bug
> trouvé, aucun code modifié. TVA : utilisateur confirme aucune TVA applicable actuellement.
> **Verdict technique : PRÊT POUR VALIDATION HUMAINE GLOBALE** — fiche de validation vierge livrée,
> à remplir par l'utilisateur module par module, avant toute activation de mode réel.
>
> **Suite (2026-08-10)** : validation humaine LOT A/B/C signée ; LOT D/E exercés en profondeur
> (chaîne E2E comptable réelle, pipeline aval 6/6 depuis l'interface, idempotence, rollback natif,
> exports sans PII, intégrité 950/950 fichiers identiques, 591 tests verts, **aucun bug**).
> Préparation du mode réel documentée (`PREPARATION_MODE_REEL.md`, 9 writers, ordre d'activation,
> backup/rollback). **Blocage restant identifié : l'activation du mode réel exige une modification
> revue de `app/config.py`** (tous les writers gatés par `RECETTE_MODE`, 3 codés en dur à `False`)
> — protection par conception, pas un défaut. Reste : décisions utilisateur LOT D/E, arbitrages
> comptables (`70`), signature de la checklist (`72`).

État figé au **2026-07-31**, voir `HANDOFF_CANONIQUE.md` pour le HEAD exact (mis à jour à chaque
commit stable). Master `8b47807`, worktree propre.
Suite complète : voir dernier total constaté dans `HANDOFF_CANONIQUE.md` (2 échecs pré-existants
connus, `test_appsec1_diagnostic` et un flake ordre-dépendant confirmé, non liés au chantier).

**Estimation globale : 82 %**, marge ± 4 points. Cœur Comptabilité (ACHATS/VENTES/BANQUE/CAISSE/
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
| **Analytique** | **TERMINÉ (périmètre défini)** | `50`, `51`, `53` — mappings branchés, dimensions peuplées, moteur Lot10, **8 réconciliations sur 8**, axes fournisseur/catégorie/prestataire construits, plateforme/activité `NON_DISPONIBLE` assumé (raison explicite), drill-down vérifié, exports par axe livrés, recette navigateur 30 étapes faite, campagne de tests par shards faite (2281 passés, 1 échec pré-existant) | répartition pool multi-logements (case C, gap Ménages) | Résultats (fait) | atteint pour le périmètre défini |
| **Résultats** | **TERMINÉ (périmètre défini)** | `52`, `53` — 22 routes `/resultats/*`, drill-down jusqu'à l'écriture vérifié sans 404, 7 exports CSV par axe, recette navigateur avec persistance et idempotence vérifiées | export XLSX/Power BI ; écran de delta chiffré dédié entre visions ; tout ce qui dépend d'axes non peuplés | — | atteint pour le périmètre défini |
| **Contrôles** | PARTIEL | catalogues Banque, Factures, Ménages, Charges, **Comptabilité** (`49`, 15 codes) livrés ; **8 réconciliations globales sur 8** (`51`, Lot9↔Lot10 fermée) | contrôles inter-lots | — | réconciliations à 0,01 € — atteint |
| **Clôture** | PARTIEL | `36`, `37` — clôture applicative du pilotage des calculs, 7 statuts, VALIDEE atteinte ; **clôture comptable** (`49`) — 5 statuts, période clôturée refuse toute écriture directe, réouverture justifiée | réconciliation globale entre les deux clôtures | Analytique | atteint pour la clôture comptable elle-même |
| **Power BI** | TERMINÉ | `38` — 11 exports + dictionnaire, filet de confidentialité, contrat verrouillé | exports analytiques éventuels | Analytique | atteint pour le périmètre actuel |
| **Mode réel** | NON ACTIVÉ | `GUIDE_ACTIVATION_MODE_REEL.md` — checklist GO/NO GO, verdict **NO GO** ; recette globale sur copies **faite, complète** (`54`-`65`, 2026-08-01/02, verdict final **GO POUR VALIDATION HUMAINE COMPLÈTE**, jamais GO mode réel) | arbitrage métier classification Banque ; activation module par module | tout le reste | GO franchi, un module à la fois |

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
7. **Recette globale sur copies** des données réelles (jamais en écriture) — **faite, complète**
   (2026-08-01/02, cf. `54`-`65`) : verdict final **GO POUR VALIDATION HUMAINE COMPLÈTE**. Lot8
   accepte le format « relevé consolidé » **en plus** du format natif historique (`63`). Cycle
   Banque complet Lot8a→8b→8c exercé (`65`) : classification + rapprochement, 0 confirmation
   automatique. L'anomalie `lot4quater`/`CTR-9-003` (mission précédente) s'est révélée être un
   artefact de l'environnement de copies, pas un défaut (`64`) — confirmé, `/calculs` produit le
   même résultat que les moteurs directs (6/6 lots, idempotent, deux fois). Deux mois Lot10
   manquants **expliqués** (`62`) : filtrage upstream cohérent. Campagne de tests complète rejouée
   (2281/75/1, identique à la référence).
8. **Validation humaine** — **dossier complet préparé** (2026-08-02, cf. `66`-`70`) : baseline
   figée, plan de validation par module/scénario, matrice d'arbitrage Banque (517 `A_CONTROLER`,
   166+56 rapprochements en attente), matrice d'arbitrage comptable (comptes provisoires), guide de
   recette utilisateur. Les décisions elles-mêmes restent en attente (plan de comptes, axes
   analytiques restants, complément de saisie Hors-Hostaway pour LOG_0015/PROP_0011 si les mois
   2026-11/2027-01/2026-12 doivent être complétés, règles de classification Banque fines, fiche de
   signature `72`).
9. **Activation progressive du mode réel** — **dossier de préparation livré** (2026-08-02, cf. `71`-
   `72`) : sauvegardes, flags, ordre d'activation en 5 phases, procédure de rollback documentés,
   **aucune phase exécutée, aucun flag activé**. Checklist GO/NO-GO formalisée, verdict actuel
   **NO GO — VALIDATION HUMAINE REQUISE**. Reste : décision humaine puis activation module par
   module selon `GUIDE_ACTIVATION_MODE_REEL.md` et `71_DOSSIER_PREPARATION_MODE_REEL.md`.

## Arbitrages métier en attente

| Sujet | Détail | Bloque |
|---|---|---|
| Plan de comptes détaillé | mécanisme de résolution relié et testé (`0024`, `50`) ; aucune règle `VALIDE` n'est encore arbitrée — `606000` reste le filet `PROVISOIRE_GENERIQUE` par défaut | Analytique par catégorie |
| Ventilation d'une charge multi-logements par pool (hors `facture_lignes`) | aucune clé de poids n'existe ; ne pas improviser — gap Ménages déjà connu (`41` §7bis) | Analytique complet |
| Circuit propriétaire en SQLite | décision actuelle : **ne pas** migrer lot12 (`44`) ; à réviser si la facture propriétaire émise devient un objet applicatif | Facturation propriétaire, journal VENTES |
| Charge postérieure à une clôture validée | aucun mécanisme applicatif ne l'interdit | Clôture comptable |
| Source Banque réelle — format consolidé accepté | **RÉSOLU (code)** : Lot8 accepte désormais le format consolidé en plus du natif (`63`), `BANQUE_LOT8_IMPORT.xlsx` produit réellement (541 mouvements, 0 BLOQUANT) ; cycle complet Lot8a/8b/8c exercé (`65`) | levé |
| Deux mois absents de la série réelle Lot10 (2026-11, 2027-01) | **RÉSOLU (expliqué)** : filtrage upstream cohérent d'une réservation `A_CONTROLER`/`DIRECT_SANS_SAISIE_HH` sans montant (LOG_0015/PROP_0011), seule ligne de ces mois — cf. `62` | Fraîcheur des Résultats réels, sauf complément de saisie Hors-Hostaway |
| Règles de classification Banque (`lot8b`) | génériques/seed uniquement — la plupart des mouvements réels resteront `A_CONTROLER` sans arbitrage métier fin | Précision de la classification Banque |

## Anomalies ouvertes

| Réf | Sujet | Gravité |
|---|---|---|
| `test_appsec1_diagnostic` | échec environnemental **pré-existant** (nom d'utilisateur Windows dans un chemin temporaire pytest), antérieur au chantier | test |
| Charge post-clôture | non interdite applicativement | métier |
| Bandeau `MODE RECETTE` (chemin absolu) | pré-existant, hors mandat de la mission Banque, non corrigé — cf. `65` §Sécurité | affichage, RECETTE_MODE uniquement |

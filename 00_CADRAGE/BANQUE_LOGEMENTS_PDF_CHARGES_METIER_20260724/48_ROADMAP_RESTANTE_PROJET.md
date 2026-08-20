# 48 — Roadmap restante du projet

> **Mise à jour 2026-08-12 (mission de nuit — baseline canonique après nettoyage)** : simulation
> fraîche avec Banque réelle régénérée sur copie (pipeline déjà validé rejoué, 0 stub). **Les
> 541 BLOQUANT du système sont exactement `GESTION_LOGEMENT_MISSING` (472) + `CHARGE_
> EXCEPTIONNELLE_DANS_CHARGE_FIXE` (69, sous-ensemble strict des 77 couples gestion, même
> cause)** — aucun autre BLOQUANT n'existe. 0 nouveau bug de code trouvé après audit complet.
> Banque : 541 mouvements, 236 classés, 222 rapprochement humain, 83 file assistée, statut
> `BANQUE_DISPONIBLE`. 42 Direct/VRBO : 0 nouvelle preuve (API vérifiée en direct cette nuit).
> Résiduels Ménages classifiés, hors périmètre. Idempotence + intégrité vérifiées, 0 donnée
> réelle modifiée. **3 décisions humaines ferment tout : historique gestion 2025, saisies
> Direct/VRBO, classification Banque.** Détail : `81_BASELINE_CLOTURE_APRES_NETTOYAGE.md`.

> **Mise à jour 2026-08-11/12 (audit des 70 RESERVATION_EXCLUE_A_CONTROLER VRBO/Direct)** : bug
> réel de double-comptage trouvé et corrigé dans `lot10_calculer_resultats.py` — 28 réservations
> (27 VRBO résolues via backfill CSV historique, 1 Direct résolue via saisie HH déjà validée
> D054) étaient déjà comptées dans `COMMISSIONS` mais listées en double dans `A_CONTROLER`. Test
> rouge→vert, fix minimal (5 lignes), régression 274 passed, 0 échec. **Aucune écriture réelle**
> (bug de reporting, pas de donnée source à corriger). Simulation : `RESERVATION_A_CONTROLER`
> 70→42, delta résultat société 0,00 €. 42 restantes = donnée absente (38 Direct sans saisie HH +
> 4 VRBO sans backfill), nécessitent une saisie manuelle humaine, pas une décision de règle
> métier. Clôture NO GO, mode réel NO GO — non activé.

> **Mise à jour 2026-08-11 (correctif lot4ter + correction réelle des 506 guestCount clôturés)** :
> décision utilisateur, correction rétroactive ciblée (pas de réouverture globale, pas de
> synchronisation LIVE→HIST générale). Audit d'impact préalable : 397/506 sans effet canapé, 109/
> 506 avec correction nécessaire (+1 090 € canapé) ; résultat société global inchangé. **Bug réel
> trouvé et corrigé** : `lot4ter` ne conservait jamais `guestCount` dans HIST (colonne absente du
> schéma depuis l'origine) — toute correction était effacée au run normal suivant. Test rouge→vert,
> fix minimal, régression 272+432 passed, 0 échec. Committé (`9a0a6aa`) avant toute donnée réelle.
> Correction réelle des 506 appliquée dans `HIST_Reservations_Cloturees.xlsx` (backup+hash+diff
> exact vérifiés, 0 autre colonne touchée), persistance/idempotence/rollback prouvés sur copies
> avant écriture réelle. Intégrité 950/950, 3 diffs attendus. Port 8000/PID 21136 intact.
> **Pipeline aval réel non relancé : le chiffre de clôture officiel reste 553** jusqu'au prochain
> run autorisé. Clôture NO GO, mode réel NO GO — non activé.

> **Mise à jour 2026-08-10 (ré-extraction Hostaway réelle exécutée)** : autorisation utilisateur
> scopée strictement (lecture API + nouveau `MASTER_FACT_HA_Reservations.xlsx` + remplacement
> contrôlé de ce seul fichier). Backup vérifié, extraction 1391→1527 réservations (guestCount
> 0→100 %), comparaison exhaustive concluante (17 disparues vérifiées en direct via l'API =
> annulées sans payout, 153 nouvelles = activité normale, 1 seul écart économique réel =
> réservation prolongée, cohérent). Simulation complète sur copie intégrale (jamais sur le réel) :
> `lot4bis→lot4quater→lot9→lot10→lot11`, 0 bloquant partout. **Résultat mesuré :
> `GUEST_COUNT_MANQUANT` 553→506 (-47)**, mécanisme vérifié à 100 % (506 restantes = 100 % en
> mois clôturé, gelées par conception, résolution complète de ce qui était atteignable par API).
> Master réel remplacé (hash relu identique). Intégrité 950/950, 2 diffs attendus. Port
> 8000/PID 21136 intact. **Pipeline aval réel non relancé : le chiffre de clôture officiel reste
> 553 tant que ce run n'est pas rejoué.** Clôture NO GO, mode réel NO GO — non activé.

> **Mise à jour 2026-08-10 (blocage clôture)** : `GESTION_LOGEMENT_MISSING` réduit 838→472 lignes
> (décision utilisateur, réel modifié, couverture 2025-08-01). `RESERVATION_A_CONTROLER_SANS_
> COMMISSION` auditée : référentiel de taux déjà complet et conforme, 0 écriture nécessaire, 612
> inchangé (causes réelles : `GUEST_COUNT_MANQUANT` 553, payout VRBO/Direct non résolu 59). Reste
> bloquant : 77 couples gestion (jan-juil 2025), 612 commission (guest count + payout), 553 guest
> count, 222 Banque, 69 charges. Clôture NO GO.
>
> **Suite (2026-08-10)** : `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` auditée — code déjà correct
> (correctif du 20/06/2026), cause réelle = aucune ré-extraction Hostaway réelle depuis. Aucune
> source locale fiable, 553 inchangé, aucune écriture. Question posée : autoriser une ré-extraction
> Hostaway réelle (hors périmètre technique). Clôture NO GO.

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

## Mise a jour 2026-08-12

Historique gestion 2025 (14 logements, prolongation date_debut au 01/01/2025, decision
utilisateur) applique reel + verifie. 541 BLOQUANT (GESTION_LOGEMENT_MISSING +
CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE) -> 0 sur simulation canonique fraiche, delta
economique 0,00 EUR. Reste roadmap inchange pour le reste (42 Direct/VRBO, Banque humaine,
mappings comptables, anomalies ci-dessus).

## Mise a jour 2026-08-12 (2)

Vraie file humaine finale etablie : 42 saisies Reservations (Direct/VRBO) + 138 decisions Banque
(56 proprietaires + 82 A_ENVOYER_IA) + 3 residuels Menages/provenance + arbitrage comptable fin
non bloquant. 166 PAYOUT_PLATEFORME Airbnb definitivement exclues du travail humain (categorie
moteur deja correcte). Detail et priorisation : 82_PACK_FINAL_ACTIONS_HUMAINES.md.

## Mise a jour 2026-08-13 — audit Lot 5

Lot 5 audite : fonctionnel, non alimente. Les 56 mouvements proprietaires restent 56 decisions
humaines (0 preuve existante, plancher 7 si nature uniforme). 82 A_ENVOYER_IA : 12 regles
candidates preparees (couvrent 57/82), compression possible a 37 decisions. File humaine finale :
89 a 183 decisions. Nouveau prerequis : migrations 0017->0026 sur la base reelle avant
exploitation tresorerie proprietaires. Detail : 83_AUDIT_LOT5_RAPPROCHEMENT_PROPRIETAIRES.md et
82_PACK_FINAL_ACTIONS_HUMAINES.md.

## Mise a jour 2026-08-13 (2)

Migration app.db 0016->0026 repetee et prete (base reelle non migree) ; rollback valide sur copie ;
runbook 85 pret a executer sur decision. Bug lot8c/Lot5 corrige (1759ce0). Contrainte Lot 5
documentee : peuplement MASTER par refresh Power Query dans Excel. 12 regles candidates sur les 82
validees techniquement. Il ne reste aucune question technique sur la migration ou l'exploitation de
Lot 5 — uniquement des decisions metier et deux arbitrages (regles Lot5<->Banque, mappings
comptables fins).

## Mise a jour 2026-08-13 (3)

Facturation proprietaires construite et recettee (migration 0027, docs 86 et 87). Nouveaux points
ouverts, tous metier : source unique de l'ecriture comptable de vente (anti double comptage),
format de numero et mentions legales, identite de la societe emettrice, imputation d'un mouvement
bancaire sur une creance de facture. Factures voyageurs/tiers : hors perimetre, modele extensible.

## Mise a jour 2026-08-13 (4)

Chaine comptable fermee : facture EMIS = source unique de vente, double comptage impossible par
construction. Migration repetee jusqu'a 0027. Restent : format de numero et mentions legales,
identite societe, mapping compte produit definitif (706000 provisoire), imputation Banque sur
creance de facture.

## Mise a jour 2026-08-13 (5)

Module facture **termine** (doc 88). Restent uniquement des valeurs a renseigner : identite
societe, regime TVA + mention, conditions de reglement, penalites/indemnite B2B, type de client par
proprietaire.

**Nouveau chantier identifie : FACTURATION_ELECTRONIQUE_PA** — raccordement a une plateforme
agreee. Le modele est deja prepare (champs electronic_invoice_*, SIREN client structure,
nature_operation, separation snapshot / rendu PDF) ; restent le choix du fournisseur, le format
(Factur-X ou autre), l'integration et le e-reporting. Non entrepris volontairement : aucune
plateforme n'est choisie a ce jour.

## Mise a jour 2026-08-14 — completude fonctionnelle

Quatre ecrans financiers construits (creances, dettes, echeancier, balance). Matrice complete :
`92_MATRICE_COMPLETUDE_FONCTIONNELLE.md` — 48 DISPONIBLE / 5 PARTIEL / 1 MANQUANT / 0 BUG,
completude 89 %.

Ordre du projet rappele : completude -> corrections -> consolidation -> tests -> **recette manuelle
utilisateur** -> preparation cut-over -> donnees nouvelle structure -> bascule -> mode reel. La
recette manuelle (`89`) est prete a executer ; le cut-over n'est pas la prochaine etape.

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

## Mise à jour migration SQLite — Ménages, Lot9, Lot10 TERMINÉS

Ménages fermé sans master permanent (10/10 sources). Lot9 (`flux_unifies`) et Lot10 (`lot10_*`)
migrés en SQLite, parité réelle 0,00 € d'écart, tests bloquants verts, campagne complète verte
(moteur 345/345, application 2856/2856). Détail : document `95` (§15-16).

**Chaîne suivante : Lot11 (contrôles → SQLite), puis conditionnellement Lot12.**

## Mise à jour migration SQLite — Lot11 TERMINÉ (groupes couverts)

`controles_lot11_service.py` (SQLite natif) couvre les groupes de contrôle dont les sources sont
déjà migrées ; parité réelle prouvée (10/11 constats en accord exact, écart restant expliqué). Test
bloquant vert. Détail : document `95` (§17).

**Chaîne suivante : conditionnellement Lot12, gate ouvert (Lot10 fermé + Lot11 validé).**

## Mise à jour migration SQLite — Lot12 TERMINÉ, mission large close

`lot12_prefactures_service.py` (SQLite natif) : préfactures propriétaires, parité réelle prouvée
sur l'intégralité du jeu réel (285/285, 3481/3481 lignes). Règle préfactures-uniquement vérifiée
par test dédié. Détail : document `95` (§18).

**Lot9/10/11(couvert)/12 clos. Arrêt volontaire : Lot13/export final/orchestrateur = mission
suivante, non commencée.**

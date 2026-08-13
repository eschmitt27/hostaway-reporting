# 60 — Verdict GO / NO GO (recette globale sur copies, 2026-08-01, mis à jour le 2026-08-02)

> **Suite 18 (2026-08-12)** - Mission de nuit autonome : baseline canonique fraiche avec Banque
> reelle (pipeline lot8a/8b/8c deja valide rejoue sur copie, 0 stub, 0 reaudition metier, 0
> matching Banque-Reservation). **Decouverte : les 541 BLOQUANT du systeme sont exactement et
> uniquement GESTION_LOGEMENT_MISSING (472) + CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE (69, sous-
> ensemble strict verifie a 100% des 77 couples gestion, meme cause racine).** Aucun autre
> BLOQUANT n'existe. Audit complet des residuels : 0 nouveau bug technique trouve, 0 code
> modifie cette nuit. Banque fraiche : 541 mouvements, 236 classes, 222 rapprochement humain,
> 83 file assistee, statut BANQUE_DISPONIBLE. 42 Direct/VRBO : requete API Hostaway en direct
> refaite cette nuit, 0 nouvelle preuve (paymentStatus=Unknown confirme frais sur les 42).
> REEL/COMPTABLE/HORS_COMPTA avec Banque reelle : 313756,48/303232,32/10524,16 EUR, ecart 0,00.
> Idempotence verifiee (2 runs identiques). Integrite : 950/950, 3 diffs deja committes, 0
> nouvelle modification reelle cette nuit. Port 8000/PID 21136 intact.
> **3 decisions humaines ferment tout : historique gestion 2025 (resout 541 BLOQUANT d'un coup),
> 42 saisies Direct/VRBO, 222 mouvements bancaires + 83 file assistee.**
> **CLOTURE NO GO. PREPARATION MODE REEL NO GO. MODE REEL NO GO - NON ACTIVE.**

> **Suite 17 (2026-08-11/12)** - Audit des 70 RESERVATION_EXCLUE_A_CONTROLER (VRBO/Direct).
> Bug reel trouve : `lot10_calculer_resultats.py` listait en double des reservations DEJA
> comptees dans COMMISSIONS (28 = 27 VRBO resolues via backfill CSV historique + 1 Direct
> resolue via saisie HH deja validee D054), a partir du statut brut Lot1 sans verifier la
> resolution aval. Preuve programmatique : 100% des 28 supprimees etaient reellement dans
> COMMISSIONS (0 suppression injustifiee). Test rouge->vert
> (`tests/test_lot10_reservation_exclue_dedup.py`), fix minimal (5 lignes), regression 274
> passed, 0 nouvel echec. **AUCUNE ecriture reelle** (bug de reporting, pas de donnee source -
> 0 PREUVE_A eligible sur les 42 restantes). Simulation : `RESERVATION_A_CONTROLER` 70->42,
> delta resultat societe **0,00 EUR** (les 28 deja comptees). 42 restantes = 38 Direct sans
> saisie HH + 4 VRBO sans backfill, donnee genuinement absente, saisie manuelle necessaire (pas
> une decision de regle metier). Port 8000/PID 21136 intact.
> **CLOTURE NO GO. PREPARATION MODE REEL NO GO. MODE REEL NO GO - NON ACTIVE.**

> **Suite 16 (2026-08-11)** - Correction retroactive CIBLEE des 506 GUEST_COUNT_MANQUANT clotures
> (decision utilisateur : pas de reouverture globale, pas de recalcul aveugle depuis le live, pas
> de sync LIVE->HIST generale). Audit d'impact prealable (copies) : 397/506 sans impact canape
> (delta 0), 109/506 avec correction necessaire (+1090 EUR canape). Resultat societe global
> INCHANGE (REEL/COMPTABLE/HORS_COMPTA identiques au centime). Bug reel trouve et corrige :
> `lot4ter_historiser_reservations_cloturees.py` reecrivait HIST sans jamais conserver `guestCount`
> (colonne absente du schema depuis l'origine, pas juste vide) - toute correction etait effacee au
> run normal suivant. Test rouge->vert, fix minimal (2 lignes), regression 272+432 passed, 0 nouvel
> echec. **Committe (`9a0a6aa`) avant toute donnee reelle.** Correction reelle appliquee : backup
> horodate + hash verifie, ecriture reelle des 506 guestCount (colonne ajoutee, 0 diff sur les 28
> colonnes existantes, verifie), journal append-only cree. Preuve de persistance/idempotence/
> rollback verifiees sur copies avant l'ecriture reelle. Integrite : 950/950, 3 diffs attendus.
> Port 8000/PID 21136 intact. **Pipeline aval reel NON relance (interdit explicitement) : les
> resultats de cloture reels n'integrent pas encore la correction, le chiffre officiel reste 553.**
> **CLOTURE NO GO. PREPARATION MODE REEL NO GO. MODE REEL NO GO - NON ACTIVE.**

> **Suite 15 (2026-08-10)** - Re-extraction Hostaway reelle AUTORISEE et EXECUTEE (perimetre
> strict : lecture API + nouveau MASTER_FACT_HA_Reservations.xlsx + remplacement controle de ce
> seul fichier - pas d activation generale du mode reel, pas d autorisation Banque, pas de
> pipeline aval reel relance). Backup verifie, extraction 1391->1527 reservations (guestCount
> 0%->100%), comparaison exhaustive (17 disparues verifiees EN DIRECT via l API = cancelled sans
> payout, 153 nouvelles = activite normale, 1 seul ecart economique reel = resa prolongee,
> coherent). Simulation complete sur copie integrale (jamais sur le reel) : lot4bis->lot4quater->
> lot9->lot10->lot11, 0 bloquant, REEL=COMPTABLE+HC verifie a l euro. RESULTAT MESURE :
> GUEST_COUNT_MANQUANT 553->506 (-47), mecanisme verifie a 100% par jointure (506 restantes =
> 100% mois cloture/gele par design, resolution complete de ce qui etait atteignable par API).
> Master reel remplace (hash relu identique). Integrite 950/950, 2 diffs attendus. Port
> 8000/PID 21136 intact. Tests cibles 62 passed, 0 nouvel echec. **Pipeline aval reel NON
> relance : le chiffre de cloture officiel reste 553 tant que ce run n est pas rejoue.**
> **CLOTURE NO GO. PREPARATION MODE REEL NO GO. MODE REEL NO GO - NON ACTIVE.**

> **Suite 14 (2026-08-10)** - GUEST_COUNT_MANQUANT_PREPARATION_CANAPE audite. 553 lignes
> = 553 reservations distinctes, 4 logements avec regle canape configuree. Code deja correct
> (correctif 20/06/2026, numberOfGuests). Cause reelle : aucune re-extraction Hostaway reelle
> depuis ce correctif (fichier source 100% vide, colonne d audit du correctif absente - preuve
> d une extraction anterieure). Aucune source locale fiable. PREUVE_A=0. Aucune ecriture, aucun
> code modifie. 553 inchange, 612 inchange, TOTAL 2002 inchange. Question posee : autoriser une
> re-extraction Hostaway reelle (hors perimetre technique de cette mission).
> **CLOTURE NO GO. PREPARATION MODE REEL NO GO. MODE REEL NO GO - NON ACTIVE.**

> **Suite 13 (2026-08-10)** — `RESERVATION_A_CONTROLER_SANS_COMMISSION` auditée. Baseline
> contrôles corrigée à 2002 (exact, écart de −52 sur `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE`
> expliqué — effet aval de la prolongation gestion). **Découverte : `REF_Taux_Commission` existe
> déjà, conforme exactement à la règle utilisateur (15 % 2025-01→2026-01, taux spécifique dès
> 2026-02 si différent), 12/12 propriétaires couverts.** 0 des 612 lignes causé par un taux
> manquant — causes réelles : `GUEST_COUNT_MANQUANT` (553) et payout VRBO/Direct non résolu (59).
> **Aucune écriture nécessaire, aucune faite. 612 inchangé. CLÔTURE NO GO. PRÉPARATION MODE RÉEL
> NO GO. MODE RÉEL NO GO — NON ACTIVÉ.**

> **Suite 12 (2026-08-10)** — Première écriture réelle contrôlée de la mission. Sur décision
> utilisateur explicite, `REF_Gestion_Logements_Hist` (14 logements) modifié : couverture
> propriétaire prolongée au 01/08/2025 (procédure backup+SHA256+prévisualisation+écriture+relecture
> respectée, intégrité vérifiée : 1/950 fichiers modifiés, exactement celui attendu).
> `GESTION_LOGEMENT_MISSING` : 838→472 lignes, 137→77 couples (mesuré sur copies, diff identique
> au réel, non recalculé sur le réel). **77 couples restent (jan-juil 2025), 8 autres familles
> bloquantes (1519 lignes) intactes. CLÔTURE NO GO. PRÉPARATION MODE RÉEL NO GO. MODE RÉEL
> NO GO — NON ACTIVÉ.**

> **Suite 11 (2026-08-10) — LA PRÉPARATION DU MODE RÉEL REPASSE EN NO GO.**
> Validation humaine LOT D/E signée (Comptabilité ACCEPTE_AVEC_RESERVE, Analytique/Résultats/
> Calculs ACCEPTE, Contrôles/Clôture ACCEPTE sous condition). La condition a été instruite et
> **révèle un fait qui change le verdict** : **2357 lignes de contrôle empêchent réellement toute
> clôture** (959 BLOQUANT + 1399 A_CONTROLER, toutes `impact_cloture = "Bloque la clôture"`). Mes
> rapports précédents écrivaient « 2358 bloquants » en conflatant sévérité et effet — chiffre exact,
> lecture ambiguë, **corrigée**. Ce sont des **lacunes de données métier** (9 familles), pas des
> défauts applicatifs, et aucune n'a été résolue automatiquement.
>
> Audit des 9 writers : **le contrat de sécurité demandé (double garde, défaut fail-closed,
> write-guard de chemin, backup pré-écriture, prévisualisation/confirmation) est déjà implémenté**.
> Aucun second mécanisme n'a été construit, aucune ligne de `app/config.py` modifiée. Ce qui manque
> est l'état « écriture réelle », délibérément jamais construit — et qui ne doit pas l'être tant que
> le verdict est NO GO.
>
> **Verdicts : Application VALIDÉE SUR COPIES (LOT A→E signés) ; Configuration writers PRÊTE
> (déjà fail-closed) ; Writers 0/9 activables aujourd'hui, par conception ; Préparation mode réel
> NO GO (bloqueurs de clôture + arbitrages comptables) ; Mode réel NO GO — NON ACTIVÉ.**

> **Suite 10 (2026-08-10) — 4 verdicts séparés.** Validation humaine LOT A/B/C signée par
> l'utilisateur ; LOT D/E exercés en profondeur (chaîne E2E comptable réelle, équilibre imposé,
> période clôturée verrouillée, invariant REEL=COMPTABLE+HC à 0,00 €, pipeline 6/6 depuis
> l'interface, idempotence, rollback natif, exports sans PII). **950/950 fichiers réels identiques
> au baseline, 0 modification. 591 passés / 37 ignorés / 0 échec. Aucun bug trouvé.**
>
> 1. **APPLICATION FONCTIONNELLE SUR COPIES : VALIDÉE TECHNIQUEMENT** — validation utilisateur
>    LOT D/E restante.
> 2. **COMPTABILITÉ : FONCTIONNELLE AVEC MAPPINGS PROVISOIRES** (`606000` générique, à arbitrer).
> 3. **PRÉPARATION MODE RÉEL : PRÊTE TECHNIQUEMENT / À SIGNER** — `PREPARATION_MODE_REEL.md`.
> 4. **MODE RÉEL : NO GO — NON ACTIVÉ.** Aucun flag modifié. L'activation exige une modification
>    revue de `app/config.py` (writers gatés par `RECETTE_MODE`, 3 codés en dur à `False`), pas un
>    changement de variable d'environnement.

> **Suite 9 (2026-08-08)** : recette fonctionnelle globale exécutée sur copies (port isolé 8030,
> `RECETTE_MODE=1`, port 8000/PID 21136 jamais touché). 18/18 modules techniquement fonctionnels
> (smoke HTTP réel), parcours navigateur approfondi sur Réservations hors Hostaway (résolution
> propriétaire/taux/ménage confirmée en direct). **Aucun bug trouvé, aucun code modifié.** TVA :
> utilisateur confirme qu'aucune TVA n'est applicable actuellement. Détail : `RECETTE_
> FONCTIONNELLE_GLOBALE.md`.
> **VERDICT TECHNIQUE : PRÊT POUR VALIDATION HUMAINE GLOBALE** (fiche vierge livrée, à remplir par
> l'utilisateur module par module) — ceci n'est PAS une activation du mode réel.

> **Suite 8 (2026-08-08)** : cadrage comptable fermé sans invention de compte. Matrice exhaustive
> (`70_MATRICE_ARBITRAGES_COMPTABLES.md`) : 7 comptes existent dans le plan comptable applicatif,
> aucune règle `mapping_comptable_regles` `VALIDE` seedée au-delà du filet générique `606000`.
> Aucun numéro de compte choisi par cette mission (dont deux candidats identifiés dans les
> commentaires de schéma : `467000` pour associés, `411000` pour propriétaires — jamais arbitrés
> comme définitifs). **Le cœur Comptabilité est techniquement fonctionnel. Les comptes/mappings
> non validés restent PROVISOIRES/A_ARBITRER et ne doivent pas empêcher la recette fonctionnelle
> des autres modules.** Checklist de préparation recette globale (16 modules, 0 BLOQUANT) dans
> `HANDOFF_CANONIQUE.md`.

> **Suite 7 (2026-08-08)** : correction de cadrage métier — un virement plateforme reçu n'est
> JAMAIS rapproché d'une réservation individuelle (règle métier définitive). Fausse logique
> supprimée : `banques_candidats_service._reservations()` générait des candidats RESERVATION pour
> tout mouvement CREDIT (Hostaway et hors Hostaway), exposés à exact/partiel/groupé — supprimé,
> exact/partiel/groupé restent intacts pour charges et trésorerie propriétaires. UI Airbnb
> (`SOURCE_AIRBNB_DETAILLEE_ABSENTE`, "en attente d'export pour rapprochement") remplacée par une
> catégorisation neutre (`VERSEMENTS PLATEFORMES`, 166 mouvements PAYOUT_PLATEFORME identifiés,
> 14 467,27 €, aucun export requis). Doublon mouvement_id (83 lignes/82 mouvements) : garantie de
> traitement unique absente, corrigée dans `banques_classement_service` (déduplication par
> `mouvement_id`, 4 tests). Régression ciblée verte (612+304+44+130 passés, 0 échec). Détail
> complet : `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md` (section G), `74_CONTRAT_SOURCE_AIRBNB_
> RAPPROCHEMENT.md` (marqué SUPERCÉDÉ).
> **Verdict séparé : Bloc Banque/Trésorerie VALIDÉ SUR COPIES (cadrage corrigé) ; Airbnb —
> catégorisation fonctionnelle, plus de blocage lié à un export absent (le besoin n'existe plus) ;
> Mode réel NO GO (validation humaine toujours non rendue).**

> **Suite 6 (2026-08-07)** : validation finale ciblée du bloc Banque/Trésorerie — recette
> navigateur complète (trésorerie, exact/partiel/groupé/ambigu/partiel-puis-groupé, file
> A_ENVOYER_IA, statut Airbnb), pipeline Lot8a→Lot13 exécuté deux fois à l'identique (idempotence
> prouvée), rollback exercé, campagne complète (2500 tests, 0 échec nouveau, 1 échec pré-existant
> reproduit et confirmé antérieur), intégrité avant/après vérifiée (85/88 identiques, 3 écarts tous
> classés ATTENDU). Détail complet : `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`.
> **Verdict séparé : Bloc Banque/Trésorerie VALIDÉ SUR COPIES ; Airbnb NO GO (source absente) ;
> Mode réel NO GO (validation humaine toujours non rendue).** Verdict global inchangé, ce tour ne **Suite 6 (2026-08-07)** : validation finale ciblée du bloc Banque/Trésorerie — recette
> navigateur complète (trésorerie, exact/partiel/groupé/ambigu/partiel-puis-groupé, file
> A_ENVOYER_IA, statut Airbnb), pipeline Lot8a→Lot13 exécuté deux fois à l'identique (idempotence
> prouvée), rollback exercé, campagne complète (2500 tests, 0 échec nouveau, 1 échec pré-existant
> reproduit et confirmé antérieur), intégrité avant/après vérifiée (85/88 identiques, 3 écarts tous
> classés ATTENDU). Détail complet : `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`.
> **Verdict séparé : Bloc Banque/Trésorerie VALIDÉ SUR COPIES ; Airbnb NO GO (source absente) ;
> Mode réel NO GO (validation humaine toujours non rendue).** Verdict global inchangé, ce tour ne
> le remplace pas — il le confirme sur le périmètre Banque/Trésorerie.

> Re-vérifié le 2026-08-02 (suite 1) : une mission a annoncé un relevé Crédit Mutuel « nouvellement
> fourni » — vérifié absent en pratique. Verdict inchangé à cette étape.
>
> **Suite 2 (2026-08-02)** : le fichier a réellement été déposé
> (`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`, SHA256
> `a84c9b51b1c0eb50d17216272bd3c6cf2669d159bf7e1299c2b762face0ca4a8`, copié et hash vérifié
> identique dans l'environnement de copies). Exécution réelle de `lot8a_banque_import.py` sur la
> copie : **ÉCHEC reproduit, code retour 1** — `[ERREUR BLOQUANT] Feuille "Cpt 02211 00021321603"
> absente. Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']`.
> Le fichier fourni est un **rapport consolidé** (titre interne *« Relevé bancaire consolidé —
> WONDERBNB »*), pas l'export brut attendu — même compte (RIB identique), mais 12 colonnes au lieu
> de 7 et une logique de fusion entre deux sources. **`BANQUE_LOT8_IMPORT.xlsx` n'a pas été
> produit.** Détail complet : `61_CONTRAT_SOURCE_BANQUE_LOT8.md` (section « Suite »).

## Verdict final (mis à jour 2026-08-02, suite 5 — dossier de validation humaine et de préparation du mode réel livré)

**NO GO — VALIDATION HUMAINE REQUISE**

Le socle technique est prêt (recette globale complète, cycle Banque exercé, sécurité corrigée,
campagne de tests verte) mais **aucune décision humaine n'a encore été rendue**. Package complet
livré ce tour : `66_BASELINE_VALIDATION_HUMAINE.md` (photographie figée de l'état technique),
`67_PLAN_VALIDATION_HUMAINE.md` (matrice module × scénario, décisions volontairement laissées
vides), `68_MATRICE_ARBITRAGES_BANQUE.md` (synthèse des 517 mouvements `A_CONTROLER`, 166+56
propositions de rapprochement en attente), `69_GUIDE_RECETTE_UTILISATEUR.md` (parcours pas-à-pas),
`70_MATRICE_ARBITRAGES_COMPTABLES.md` (comptes provisoires à arbitrer), `71_DOSSIER_PREPARATION_
MODE_REEL.md` (sauvegardes, flags, 5 phases documentées, **aucune exécutée**), `72_CHECKLIST_GO_NO_
GO_MODE_REEL.md` (checklist + fiche de signature vierge).

**Sécurité** : une fuite réelle de chemin absolu (incluant le nom d'utilisateur Windows) dans le
bandeau `MODE RECETTE` de chaque page a été trouvée et corrigée ce tour (`c65f891`), procédure
test-rouge→correction→test-vert respectée (`test_securite_bandeau_recette.py`, 3 tests). Campagne
complète rejouée après correction : **2284 passés / 75 ignorés / 1 échec pré-existant** (3 tests de
plus que la référence, aucune régression).

Aucun rapprochement bancaire confirmé, aucune règle métier décidée, aucun flag de mode réel activé
par cette mission. Le paragraphe ci-dessous (verdict précédent, 2026-08-02 suite 4) reste valide
techniquement mais est remplacé par ce verdict, plus prudent, tant que la fiche de signature
(`72`) n'est pas remplie par l'utilisateur.

### Verdict précédent (2026-08-02, suite 4 — anomalie lot4quater expliquée, cycle Banque complet)

**GO POUR VALIDATION HUMAINE COMPLÈTE**

L'anomalie `lot4quater`/`CTR-9-003` qui limitait le verdict précédent à « partielle » s'est révélée
être **un artefact de mon propre environnement de copies** (fichier réel `HIST_Reservations_
Cloturees.xlsx` pas encore copié à ce moment précis), **pas un défaut d'orchestration** — audit
complet, 6 tests de contrat, détail dans `64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md`. Preuve :
`/calculs` relancé deux fois pour le mois 2026-06 depuis l'écran applicatif lui-même (pas les
scripts moteur) → **6/6 lots SUCCÈS les deux fois**, totaux identiques au centime près à
l'exécution moteur directe (291 852,76 € REEL), idempotent. **Aucune correction de code
appliquée** : le contrat était déjà correct.

**Cycle Banque complet exécuté** (Lot8a → Lot8b → Lot8c, cf. `65_RAPPORT_CYCLE_BANQUE_COMPLET.md`) :
classification (24 VALIDE/517 A_CONTROLER, 30 règles seed), rapprochement (166 Airbnb + 56
propriétaires en attente, **0 confirmation automatique** — conforme au mandat). Effet mesuré sur
la chaîne aval : Lot9 intègre désormais 24 flux de frais bancaires (`TYPE_FLUX_016`), Lot11 passe
de `BANQUE_NON_DISPONIBLE_GIT` à `BANQUE_DISPONIBLE`. Nouveaux totaux (REEL 291 722,75 € = COMPTABLE
281 198,59 € + HORS_COMPTA 10 524,16 €, écart 0,00 €) réconciliés et idempotents.

**Campagne complète rejouée** (`TEST_SHARDS_RECETTE_GLOBALE.txt`, 131 fichiers, 6 shards, codes
retour réels) : **2281 passés / 75 ignorés / 1 échec pré-existant** — identique au caractère près
à la référence, aucune régression sur l'ensemble de la série de missions Banque.

**Recette navigateur** sur les écrans Banque (`/banques-caisse`, fiche mouvement) : drill-down sans
404, comptes masqués, classification et rapprochement affichés fidèlement, aucune confirmation
automatique visible. Sécurité : 0 fuite dans cette documentation (deux points pré-existants notés,
non introduits par cette mission, hors mandat de correction — cf. `65` §Sécurité).

Sources réelles : **88/88 intactes** (le seul écart reste la modification intentionnelle et
committée de `lot8a_banque_import.py`, du code, faite lors d'une mission antérieure).

Le second écart historique (deux mois Lot10 manquants) reste **résolu et expliqué**
(`62_RAPPORT_MOIS_LOT10_MANQUANTS.md`) : filtrage upstream cohérent, pas un défaut.

**Reste hors du périmètre de cette mission** (arbitrages métier, pas des défauts) : plan de comptes
détaillé toujours provisoire ; règles de classification Banque (`lot8b`) toujours génériques/seed,
pas encore affinées métier ; import du relevé consolidé dans le SQLite applicatif (écran `/banques-
caisse/importer`) non exercé — la lecture directe de `BANQUE_LOT8_IMPORT.xlsx` par l'application
(`banques_reader.py`) suffit à la consultation, l'import CSV est un mécanisme distinct et optionnel.

## Modules validés (sur copies réelles)

- Lecture de toutes les sources réelles configurées, sauf 2 absences documentées (Banque, export
  Power BI) — jamais transformées en zéro.
- Migrations : `integrity_check` ok, 0 violation de clé étrangère, idempotentes (comptage de
  lignes), sur une copie de l'`app.db` réel ET sur une base vierge.
- Pipeline `charges` (lot3) : exécuté deux fois sur copies, succès, idempotent, 0 doublon.
- Réconciliations A, B, D, H : **OK** sur 24 mois de données réelles (0,00€ d'écart). B confirme en
  conditions réelles le correctif du tour précédent (grains incompatibles).
- Invariant REEL = COMPTABLE + HORS_COMPTA : vérifié sur le total réel (291 779,67€), écart 0,00€.
- Sécurité : 0 fuite (chemins, PII, secrets) sur les pages et exports scannés.
- Performance : toutes les pages < 1 s, rien à corriger (pas de problème mesuré).
- Intégrité des sources réelles : 85/85 fichiers identiques avant/après (aucune écriture réelle,
  aucun résidu).

## Modules non validés / non exercés en réel

- Comptabilité (0 écriture réelle), Factures (0), Règlements (0), Ménages (0) — jamais alimentés en
  réel, réconciliations C/E/F/G correctement `A_CONTROLER`/`NON_DISPONIBLE`, pas un défaut.
- Import du relevé consolidé dans le SQLite applicatif via `/banques-caisse/importer` — mécanisme
  distinct de la lecture directe de `BANQUE_LOT8_IMPORT.xlsx` (déjà exercée), non nécessaire pour
  la consultation, non exercé ce tour.

## Anomalies

- **CORRIGÉ (code, mission précédente)** : format Banque consolidé désormais accepté par Lot8,
  sans casser le format natif historique (11 tests de régression/nouveaux, tous verts). Cf. `63`.
- **EXPLIQUÉ (pas un défaut) ce tour** : `lot4quater_resoudre_source_reservations.py`, soupçonné de
  régénérer `VUE_FLUX` scopé au mois via `/calculs`, s'est révélé se comporter correctement (contrat
  global, jamais mensuel) — la cause réelle était un fichier réel (`HIST_Reservations_Cloturees.
  xlsx`) pas encore copié dans l'environnement au moment du run défaillant. Reproduit, confirmé,
  6 tests de contrat ajoutés, **aucune correction de code nécessaire**. Cf. `64`.
- **RÉSOLU (expliqué)** : deux mois absents de la série réelle Lot10 (2026-11, 2027-01) — cause
  identifiée (filtrage upstream cohérent d'une réservation placeholder sans montant), pas un
  défaut. Cf. `62_RAPPORT_MOIS_LOT10_MANQUANTS.md`.
- **NOTÉ, PRÉ-EXISTANT, HORS MANDAT** : le bandeau `MODE RECETTE` de certains écrans affiche un
  chemin absolu de la racine de recette ; les libellés bancaires affichés peuvent contenir des
  fragments de compte tiers (texte de virement, inhérent aux relevés bancaires). Ni l'un ni l'autre
  introduit par cette mission, ni corrigé (hors mandat). Cf. `65` §Sécurité.
- **MINEUR** : `réconciliation /resultats/reconciliation` à ~1 s — acceptable, surveiller si le
  volume augmente.

Suite de tests : **268 passés** dans `tests/` (moteur, dont 6 nouveaux pour lot4quater), 0 échec.
Campagne complète par shards de l'application (`TEST_SHARDS_RECETTE_GLOBALE.txt`, 131 fichiers,
6 shards, codes retour réels) : **2281 passés / 75 ignorés / 1 échec pré-existant** — identique au
caractère près à la référence, aucune régression.

## Risques

1. Les règles de classification Banque (`lot8b`) restent génériques/seed — la plupart des
   mouvements réels resteront `A_CONTROLER` tant qu'un arbitrage métier fin n'est pas fait.
2. Comptabilité/Factures/Ménages restent à zéro en réel — tout arbitrage sur leur activation en
   mode réel reste entièrement en attente (`GUIDE_ACTIVATION_MODE_REEL.md`, verdict NO GO inchangé
   pour le mode réel lui-même).
3. L'environnement de copies contient désormais un cycle Banque complet (mouvements classifiés,
   rapprochements en attente) — à nettoyer avant tout archivage/partage.

## Prochaines actions

1. Arbitrage métier sur les règles de classification Banque (`REF_Banque_Regles`) si une
   classification plus fine que le seed générique est souhaitée.
2. Mois 2026-11/2027-01 : compléter la saisie Hors-Hostaway pour `LOG_0015`/`PROP_0011` si
   l'activité de ces mois doit apparaître dans les Résultats — sinon, aucune action requise (état
   correct).
3. Nettoyer l'environnement de copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) une
   fois ce rapport validé — il contient une copie de l'`app.db` réel et le relevé bancaire réel, à
   ne jamais committer ni partager tel quel (hors du dépôt Git par construction, mais toujours des
   données sensibles locales).
4. Décision humaine sur l'activation du mode réel, module par module (`GUIDE_ACTIVATION_MODE_REEL.
   md`), maintenant que Banque/Analytique/Résultats sont validés sur copies.

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current   # feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"
git status --porcelain      # doit être vide après le commit de ce tour
```

Ne relance pas le mode réel. Ne modifie aucune donnée réelle. Ne déclare pas le projet terminé.

## Mise a jour 2026-08-12 — decision gestion 2025 appliquee

REF_Gestion_Logements_Hist prolonge au 01/01/2025 pour 14 logements (decision utilisateur :
meme proprietaire toute la periode). BLOQUANT 541 -> 0 sur simulation canonique fraiche
(GESTION_LOGEMENT_MISSING + CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE, cause unique resolue).
Delta economique 0,00 EUR. Voir `77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md` §13,
`81_BASELINE_CLOTURE_APRES_NETTOYAGE.md` §14.

**CLOTURE TECHNIQUE (gestion+charges) : GO. PREPARATION MODE REEL : NO GO** (42 Direct/VRBO,
Banque humaine, 14 A_CONTROLER residuels, mappings comptables restent ouverts). **MODE REEL :
NO GO — NON ACTIVE.**

## Mise a jour 2026-08-12 (2) — vraie file humaine finale, 138 decisions Banque

Detail complet : `82_PACK_FINAL_ACTIONS_HUMAINES.md`. APPLICATION : VALIDEE. CLOTURE TECHNIQUE :
GO. BLOQUANTS : 0. SAISIES HUMAINES RESERVATIONS : 42. DECISIONS HUMAINES BANQUE : 138 (au lieu
de 305 brut — 166 PAYOUT_PLATEFORME exclues, 0 decision). A_CONTROLER non bloquants : 14 (3
reellement nouveaux). COMPTABILITE : PRETE AVEC RESERVES. **PREPARATION MODE REEL : NO GO. MODE
REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 — audit Lot 5

APPLICATION : VALIDEE. CLOTURE TECHNIQUE : GO. BLOQUANTS : 0. LOT 5 : FONCTIONNEL mais NON
ALIMENTE. MOUVEMENTS PROPRIETAIRES : 56 -> 56 decisions humaines (0 resoluble par preuve ;
plancher 7). A_ENVOYER_IA : 82 -> 37 a 82 decisions. DIRECT/VRBO : 42. AUTRES A_CONTROLER : 3.
TOTAL DECISIONS HUMAINES : 89 a 183. COMPTABILITE : PRETE AVEC RESERVES (les 56 proprietaires ne
creent aucune question comptable nouvelle). **PREPARATION MODE REEL : NO GO. MODE REEL : NO GO —
NON ACTIVE.**

## Mise a jour 2026-08-13 (2) — repetition migration DB

APPLICATION : VALIDEE. CLOTURE TECHNIQUE : GO. BLOQUANTS : 0.
MIGRATION APP.DB 0016->0026 : **PRETE ET REPETEE**. ROLLBACK DB : **VALIDE SUR COPIE**.
LOT 5 : PRET A ETRE ALIMENTE (contrainte : refresh Power Query dans Excel pour peupler MASTER).
BANQUE : PRETE TECHNIQUEMENT. 56 proprietaires -> 7 a 56 decisions. 82 IA -> 37 a 82 decisions.
42 Direct/VRBO. 3 autres A_CONTROLER. COMPTABILITE : PRETE AVEC RESERVES.
**PREPARATION MODE REEL : NO GO. MODE REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 (3) — facturation proprietaires

FACTURES FOURNISSEURS : VALIDEES. FACTURES PROPRIETAIRES : **APPLICATION CAPABLE DE LES CREER**.
PDF : GENERE. NUMEROTATION : PRETE (format juridique a arbitrer). SNAPSHOT : VALIDE (immutabilite
prouvee). REGLEMENT : solde derive valide, imputation Banque a cabler. COMPTABILITE : **ecriture de
vente non branchee — arbitrage requis pour eviter le double comptage**.
**EMISSION REELLE : NON AUTORISEE. APP.DB REELLE : NON MIGREE. MODE REEL : NO GO — NON ACTIVE.**

## Mise a jour 2026-08-13 (4) — source comptable unique

FACTURE PROPRIETAIRE : **SOURCE UNIQUE DE VENTE**. DOUBLE COMPTAGE : **IMPOSSIBLE PAR
CONSTRUCTION**. ECRITURE VENTES : **VALIDEE** (PROPOSEE, mapping provisoire). REGLEMENT : VALIDE.
COMPENSATION : VALIDEE. AVOIR : VALIDE (total et partiel). MIGRATION DB JUSQU'A 0027 : PRETE SUR
COPIE. IDENTITE SOCIETE : A COMPLETER. NUMEROTATION : MECANIQUE VALIDEE, FORMAT A VALIDER.
MENTIONS : A COMPLETER. **EMISSION REELLE : NON AUTORISEE. MODE REEL : NO GO — NON ACTIVE.**

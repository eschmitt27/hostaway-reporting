# 60 — Verdict GO / NO GO (recette globale sur copies, 2026-08-01, mis à jour le 2026-08-02)

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

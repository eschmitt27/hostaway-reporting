# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| État figé le | **2026-08-02** — Dossier de préparation de la validation humaine et du mode réel **LIVRÉ, mode réel NON activé**. Sécurité : fuite réelle de chemin absolu dans le bandeau MODE RECETTE trouvée et corrigée (`c65f891`, test rouge→vert). Package complet de validation humaine créé (`66`-`72` : baseline figée, plan de validation, matrices d'arbitrage Banque et Comptabilité, guide utilisateur, dossier de préparation du mode réel en 5 phases documentées mais non exécutées, checklist GO/NO-GO). Verdict : **NO GO — VALIDATION HUMAINE REQUISE** |
| Dernier commit stable avant ce tour | `f77aad9` — `docs: handoff - interface rapprochements groupes livree et testee` |
| Clarification des commits (demandée explicitement) | `dff5177` = code+tests Phase 1 (routes+5 templates trésorerie propriétaires) ; `efcf64c` = handoff seul (aucun code) ; `38042ef` = code+tests Phase 2 (interface rapprochement groupé, `banques_mouvement.html` + `routes/banques.py`) ; `f77aad9` = handoff seul (aucun code). Aucune incohérence — alternance systématique 1 commit code+tests puis 1 commit handoff, comme pour tous les tours précédents. |
| Interface trésorerie propriétaires (2026-08-04) | Phase 1 livrée et testée : routes+templates complets (`app/routes/proprietaires_tresorerie.py`, 5 templates), cycle création→prévisualisation→confirmation→validation→annulation, historique. Commit `dff5177`. |
| Interface rapprochements groupés (2026-08-05/06) | Phase 2 livrée et testée. Bug réel corrigé : recherche groupée scindée par `type_objet` (pool hétérogène épuisait la limite d'itérations). Commit `38042ef`. |
| File humaine de classement Banque (2026-08-06) | Phase 3 livrée et testée : migration `0026` (`banque_classement_decisions`, append-only), service `banques_classement_service.py`, routes+4 templates `/banques-caisse/a-classer*`. **83 mouvements réels confirmés** (vérifié sur copie : tous `statut_controle=A_CONTROLER`, `niveau_risque=MOYEN`, `categorie=NON_CLASSE`, motif catch-all `R_099`, aucune proposition IA réelle dans `IA_Classification` — 83 lignes présentes mais `categorie_proposee` toujours vide). Décisions : `CATEGORISER` (catalogue fermé curé + catégories réellement présentes), `MAINTENIR_A_CONTROLER`, `NON_CLASSE`, `REPORTER`. Aucune IA externe, aucun réseau, aucune écriture Excel/réelle. **1 bug trouvé et corrigé** : `categories_disponibles()` ne retenait que les catégories du fichier courant, refusant à tort une catégorie valide du catalogue moteur non encore présente sur cette source précise — unifié avec `LIBELLES_CATEGORIE`. 71 tests. Commit `4b51718`. Régression `-k "banque or proprietaire or migration"` : 642 passés/29 ignorés/2 échecs → corrigés (liste de tables figée dans `test_sqlite_migrations.py`) → 5/5 après correction. |
| Statut source Airbnb absente (2026-08-06) | Phase 4 livrée et testée : `banques_service.statut_source_airbnb()` compte les propositions réellement bloquées (166 `EN_ATTENTE_EXPORT_AIRBNB` sur la source réelle, vérifié), bloc `SOURCE_AIRBNB_DETAILLEE_ABSENTE` sur `/banques-caisse` avec données minimales attendues et référence au contrat `74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md`, bouton d'import désactivé (aucun import spéculatif). 7 tests. Commit `c3acedb`. Régression `-k "banque or proprietaire or migration"` : 651 passés/29 ignorés/0 échec. |
| **Phases 1-4 toutes livrées et testées (2026-08-06)** | **Non fait, scope explicite, budget de session déjà substantiel** : Phase 5 (recette navigateur réelle sur environnement isolé port≠8000), Phase 6 (pipeline Lot8a→Lot13 rejoué), Phase 7 (campagne déterministe complète par shards, ~2300+ tests), Phase 8 (comparaison hashes avant/après). Verdict : **NO GO — RECETTE NAVIGATEUR ET PIPELINE COMPLET RESTENT À FAIRE**. Aucune application réelle, aucun rapprochement confirmé, aucune règle Banque modifiée, port 8000/PID 21136 intact, master `8b47807` intact. |
| Interface rapprochements groupés (2026-08-05/06) | Phase 2 livrée et testée : `banques_mouvement.html` affiche les propositions de rapprochement groupé (≥2 objets), délègue entièrement à `banques_rapprochement_service.proposer_groupes()`/`confirmer_groupe()` (aucun second moteur). **Bug réel trouvé et corrigé** : la recherche groupée sur un pool hétérogène (réservations + trésorerie propriétaires mélangées, triées par date) épuisait la limite d'itérations bornée avant d'atteindre les objets pertinents — corrigé en scindant la recherche par `type_objet` dans `_groupes()` (routes/banques.py). Statut `A_CONTROLER` si ambiguïté, `PROPOSE` sinon, jamais de confirmation automatique ni en masse. 5 tests HTTP. Commit `38042ef`. Régression `-k "banque or proprietaire"` : 560 passés/29 ignorés/0 échec. **Non fait ce tour, scope explicite** : file A_ENVOYER_IA (Phase 3), statut "source Airbnb absente" dans l'UI (Phase 4), recette navigateur réelle (Phase 5), suite complète (2284/75/1) et pipeline Lot8a→Lot13 rejoués intégralement (Phase 6). Verdict : **NO GO — TRAVAIL RESTANT SUBSTANTIEL**. Aucune application réelle, aucun rapprochement confirmé, aucune règle Banque modifiée, port 8000/PID 21136 intact. |
| Rapprochement groupé borné (2026-08-03) | Détail complet : `75_CONTRAT_TRESORERIE_PROPRIETAIRES.md`. `banques_rapprochement_service.proposer_groupes()`/`confirmer_groupe()` ajoutés (aucun second moteur) : recherche par sous-ensembles bornée (fenêtre ±30j, 20 candidats max, groupe 5 objets max, tolérance 0,01 €, 20000 itérations max), tri déterministe, statut `A_CONTROLER` si plusieurs combinaisons valides (ambiguïté), transaction atomique tout-ou-rien à la confirmation. Scénario "partiel puis groupé" vérifié de bout en bout. 25 tests, régression `-k "banque or proprietaire"` : 527 passés/29 ignorés/0 échec. Commits de ce
tour (2, vérifié via `git log --oneline`) : `513f400` (code+tests) et `5e2b5d8` (ce fichier —
un commit qui met à jour le handoff ne peut jamais porter son propre SHA, cf. ligne "HEAD"
ci-dessus). **Non fait ce tour (scope explicite, budget de session épuisé avant de les aborder)** : routes/écrans propriétaires-trésorerie (Phase 1), file A_ENVOYER_IA (Phase 3), statut "source Airbnb absente" dans l'UI (Phase 4), recette navigateur (Phase 5), suite complète (2284/75/1) et pipeline Lot8a→Lot13 rejoués intégralement (Phase 6). Verdict : **NO GO — TRAVAIL RESTANT SUBSTANTIEL**. Aucune application réelle, aucun rapprochement confirmé, aucune règle Banque modifiée, port 8000/PID 21136 intact. |
| Trésorerie propriétaires — décision produit exécutée (2026-08-03) | Détail complet : `75_CONTRAT_TRESORERIE_PROPRIETAIRES.md`. Objet `MOUVEMENT_TRESORERIE_PROPRIETAIRE` créé (migration `0025`, 2 tables, style journal déclaratif append-only), distinct de Lot5 (inchangé, non renommé). Service `proprietaires_tresorerie_service.py` (créer/valider/annuler/solde/reste_a_rapprocher/objets_rapprochables, 20 tests). Intégré au moteur de rapprochement **existant** (`banques_candidats_service._reversements_proprietaires()`, type `REVERSEMENT_PROPRIETAIRE` déjà déclaré migration 0015, aucun second moteur, partiel/multiple hérités gratuitement de `banques_rapprochement_service`, 6 tests). PII supprimée : `PROP_LABELS` (noms réels en dur) retiré de `lot8c_rapprochement_banque.py`, vérifié sur copies. Régression ciblée (`banque or proprietaire or migration`) : 544 passés/29 ignorés/0 échec (a nécessité une correction de `test_sqlite_migrations.py`, liste de tables attendues mise à jour). Commits : `b447dd1`, `5966097`, `f71c566`. **Non fait ce tour, scope explicite** : routes/écrans propriétaires-trésorerie, algorithme de rapprochement groupé (subset-sum borné), file A_ENVOYER_IA (83 mouvements), statut "source Airbnb absente" dans l'UI, suite complète (2284/75/1) et pipeline Lot8a→Lot13 rejoués intégralement, recette navigateur. Verdict : **NO GO — TRAVAIL RESTANT SUBSTANTIEL, SOCLE TECHNIQUE POSÉ ET TESTÉ**. Aucune application réelle, aucun rapprochement confirmé, aucune règle Banque modifiée. |
| Audit prérequis rapprochement bancaire (2026-08-03) | Mission ciblée (audit Airbnb/Hostaway/Lot5/Lot8c, 45 min max) — détail complet : `74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md`. **Bug réel corrigé et testé** : `app/services/banques_candidats_service.py::_reservations()` lisait des colonnes inexistantes (`montant_paye`/`montant_total`) au lieu du schéma réel de `MASTER_CALC_Reservations_Resolues.xlsx` (`montant_retenu`/`date_arrivee`) — retournait 0 candidat sur 1391 réservations réelles, silencieusement. Test rouge→vert (`tests/test_banques_candidats_service.py`, 7 tests). Régression `pytest -k banque` : 219 passés/29 ignorés/0 échec. **3 blocages restants, non résolus ce tour, chacun nécessitant une source externe ou une décision produit** (pas un bug) : (1) aucun export Airbnb détaillé n'existe (G-code extrait du seul libellé bancaire) — contrat documenté, import non codé sans fichier réel pour le tester ; (2) les paiements Airbnb sont groupés (1 virement = plusieurs réservations), aucun algorithme de somme partielle/tolérance n'existe ; (3) Lot5 (`SAISIE_AcomptesProprietaires.xlsx`) a un contrat métier différent de ce qu'attend Lot8c (acompte facture réservation hors-Hostaway ≠ mouvement de trésorerie propriétaire↔société) — aucun objet applicatif "acompte/remboursement propriétaire" n'existe, décision produit requise avant toute migration. Verdict : **NO GO — SOURCE MÉTIER ET DÉCISION PRODUIT REQUISES**. Aucune règle moteur Banque touchée, aucune migration SQL créée, aucune application réelle. |
| Session interactive de validation humaine (2026-08-03, en cours) | **Groupe 4 CLOS (55/55, 38585,85 €, synthèse par statut corrigée le 2026-08-02 : 0 PREUVE_TROUVEE / 34 CANDIDAT_PARTIEL (24311,04 €) / 2 AMBIGU (3330,00 €) / 19 PIECE_ABSENTE (10944,81 €)).** **Groupe 5 CLOS (19/19, 9678,52 €)** : 15 crédits de tiers à identifier (`TIERS_CREDIT_A/B/C/D`, 6473,16 €), 3 crédits liés à ASSOCIE_A (2900,00 €, dont 1 anomalie de détection moteur enregistrée `BANQUE_NORMALISATION_TIERS_INCOHERENTE`), 1 impayé prioritaire (305,36 €). Toutes les décisions : `REPORTER AVEC CATÉGORIE CANDIDATE`, statut `A_CONTROLER`, aucune validée définitivement. Journal détaillé : `73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md` (DEC-001 à DEC-005, tous groupes 1-5 clos). Overlay hors Git (`_RECETTES_GLOBALES/.../RAPPORTS/OVERLAY_DECISIONS_BANQUE_GROUPES_1A5.csv`, 212 lignes, 74 avec catégorie candidate : 55 groupe 4 + 19 groupe 5). **Aucune règle moteur modifiée, aucune écriture dans `BANQUE_LOT8_IMPORT.xlsx` ni `app.db`, aucune application réelle, aucun rapprochement confirmé.** Prochaine action : les 222 propositions de rapprochement (166 Airbnb + 56 propriétaires), présentées par groupes de fiabilité mais aucune décision encore enregistrée ; puis les 83 mouvements A_ENVOYER_IA. Process historique du port 8000 (PID 21136, antérieur à cette série de missions) laissé intact sur demande explicite de l'utilisateur. |
| HEAD | vérifier avec `git log -1` — ce fichier est mis à jour par le commit qui le porte, dont le SHA ne peut donc pas y figurer |
| git status | propre avant ce tour ; ce tour ajoute uniquement des documents `00_CADRAGE` (`66`-`72`), aucun code |
| master / canonique | **intacts, jamais touchés** (`master` = `8b47807`) |
| Sources réelles | inchangées ce tour. **88/88 fichiers réels re-vérifiés identiques** au hash baseline, **une seule exception assumée et déjà documentée** : `02_TRAVAIL/lot8a_banque_import.py` (code, mission `da76d0f`, format bancaire consolidé), jamais une donnée. Relevé Banque réel jamais rouvert en écriture |
| Suite complète (dernier total constaté) | **2284 passés / 75 ignorés / 1 échec pré-existant** (`test_appsec1_diagnostic.py`, indépendant), campagne rejouée après le fix sécurité (`TEST_SHARDS_RECETTE_GLOBALE.txt`, 132 fichiers) — 3 tests de plus que la référence (`test_securite_bandeau_recette.py`), aucune régression |
| Documents transverses | `MATRICE_ETAT_MODULES.md` · `48_ROADMAP_RESTANTE_PROJET.md` · `GUIDE_ACTIVATION_MODE_REEL.md` · `60_VERDICT_GO_NO_GO.md` (verdict **NO GO — VALIDATION HUMAINE REQUISE**) · `61`-`65` (contrats/rapports Banque et lot4quater) · `66_BASELINE_VALIDATION_HUMAINE.md` (nouveau) · `67_PLAN_VALIDATION_HUMAINE.md` (nouveau) · `68_MATRICE_ARBITRAGES_BANQUE.md` (nouveau) · `69_GUIDE_RECETTE_UTILISATEUR.md` (nouveau) · `70_MATRICE_ARBITRAGES_COMPTABLES.md` (nouveau) · `71_DOSSIER_PREPARATION_MODE_REEL.md` (nouveau) · `72_CHECKLIST_GO_NO_GO_MODE_REEL.md` (nouveau, verdict + fiche de signature vierge) |
| **Avancement global estimé** | **83 %**, marge ± 4 points (+1 point : fuite de sécurité réelle corrigée et testée ce tour). Plafond inchangé : aucun pourcentage > 85 % tant que le périmètre restant (plan de comptes détaillé, facturation propriétaire/tiers, arbitrages Banque, mode réel) n'est pas tranché. Le socle technique est prêt à recevoir la décision humaine ; celle-ci n'a pas eu lieu (`72`). |

## Périmètre restant avant achèvement

Détail complet, avec dépendances et critères de fin : **`48_ROADMAP_RESTANTE_PROJET.md`**.

Ordre arrêté : (1) fermer les écarts Ménages · (2) compléter la facturation · (3) compléter la
Comptabilité · (4) analytique · (5) Résultats · (6) réconciliations · (7) recette globale sur
copies · (8) validation humaine · (9) activation progressive du mode réel.

**(3) Comptabilité : FAIT** (2026-07-29, cf. `49`). **(4) analytique : FAIT** pour le périmètre
défini, **(5) Résultats : FAIT** pour le périmètre défini, **(6) réconciliations : FAIT (8/8)**
(2026-07-31, cf. `53`). **(7) recette globale sur copies : FAITE, COMPLÈTE** (2026-08-01/02, cf. `54`-`65`) — cycle Banque
Lot8a/8b/8c exécuté avec la source réelle, chaîne aval rejouée avec succès depuis l'application.
**(8) validation humaine : PRÉPARÉE, PAS ENCORE RENDUE** (2026-08-02, cf. `66`-`70`) — dossier complet
livré (baseline, plan de validation par module, matrices d'arbitrage Banque et Comptabilité, guide
utilisateur) ; les décisions elles-mêmes (arbitrages Banque, plan de comptes, fiche de signature)
restent à rendre par l'utilisateur, jamais par Claude. **(9) activation progressive du mode réel :
PRÉPARÉE, NON DÉMARRÉE** (cf. `71`-`72`) — dossier de rollout en 5 phases et checklist GO/NO-GO
documentés, aucune phase exécutée, aucun flag activé. Verdict global : **NO GO — VALIDATION HUMAINE
REQUISE**.

## Arbitrages métier en attente (cf. `48`)

1. **Plan de comptes détaillé** — `606000`/`530000`/`467000`/`706000` génériques, mapping
   catégorie → compte créé (`mapping_categorie_compte`) mais non relié à la génération réelle.
2. **Ventilation analytique** d'une facture multi-logements — aucune règle documentée.
3. **Circuit propriétaire en SQLite** — décision actuelle : ne pas migrer lot12 ; VENTES le lit
   en adaptateur (`SOURCE_PROVISOIRE_LOT12`), ne le remplace pas.
4. **Charge postérieure à une clôture validée** — non interdite applicativement (clôture du
   pilotage des calculs ; la clôture **comptable**, elle, refuse toute écriture directe, cf. `49`).

## Modules

| Module | Statut | Documents |
|---|---|---|
| Logements | **TERMINÉ** | `28`, `29` |
| Banque (import, rapprochement, suggestions, contrôles) | **TERMINÉ** | `30`, `31` |
| Fournisseurs / Factures / Règlements | **TERMINÉ** (circuit fournisseur ; lignes multi-charges/multi-logements ajoutées `0022`) | `32`, `33`, `34`, `44` |
| Pilotage des calculs & clôture | **TERMINÉ** — chaîne aval complète `lot4quater → lot13` | `35`, `36`, `37`, `38` |
| Charges | **chaîne exercée depuis `/calculs`**, scénarios A→F réconciliés | `27`, `39` |
| Ménages | **PARTIEL** — chaîne 7/7 verte, cycle de vie construit et prouvé en recette navigateur (persistance incluse) ; `ANO-2026-07-28-01` (données réelles en dur lot6b/lot6c) **corrigée** ; pools de courses et rattachement charge encore non exercés en recette (gap distinct : appartements fictifs à aligner sur le parc) | `40`, `41`, `41b`, `JOURNAL_ANOMALIES.md` |
| Comptabilité (cœur : ACHATS/VENTES/BANQUE/CAISSE/ODIVERSES, auxiliaires, périodes, clôture) | **TERMINÉ** — plan de comptes reste PROVISOIRE (assumé, affiché comme tel) | `43`, `45`, `46`, `47`, `49` |
| Analytique (mappings, ventilation, moteur, réconciliations 8/8, axes) | **TERMINÉ pour le périmètre défini** — plateforme/activité `NON_DISPONIBLE` assumé | `50`, `51`, `53` |
| Résultats (22 routes, drill-down, exports par axe) | **TERMINÉ pour le périmètre défini** | `52`, `53` |

## ⚠️ Correction importante d'une limite documentée à tort

Les tours 1-3 affirmaient que « les moteurs Lot9/Lot10 ne sont pas exécutables ici (pandas absent) ».
**C'est faux.** Deux interpréteurs coexistent :

| Interpréteur | Version | pandas | Rôle |
|---|---|:--:|---|
| `C:\Users\Ewan\miniconda3\python.exe` | 3.12.9 | ❌ | application FastAPI + tests |
| `C:\Program Files\Python312\python.exe` | 3.12.3 | ✅ | **moteurs (lots)** — `cfg.LOT4A_ENGINE_PYTHON` |

Les lots **s'exécutent réellement** avec le second (prouvé en recette navigateur, cf. `36`).
Ne plus propager l'ancienne affirmation.

## Limites persistantes (volontaires)

1. **Forfait logiciel/consommables historisé** — nécessite de modifier `build_charge_fixe()` dans
   `lot10_calculer_resultats.py`. Jamais commencé, volontairement.
2. **Mode réel du pilotage des calculs** — garde-fous construits (`CALCULS_REAL_RUN_ENABLED`),
   jamais activé.
3. **OFX (Banque) / import CSV-XLSX de factures / caisse** — aucun besoin métier démontré.
4. Contrôles inter-lots (`total Lot8 vs Lot9`, double comptage payout) — non construits.
5. Chaînes `charges` (lot3) et `menages` (lot6b→lot6f) déclarées et lançables, **jamais exercées**
   en recette.
6. Aucune **durée estimée a priori** n'est affichée : volontaire, pas d'estimation inventée. Les
   durées réelles sont mesurées et journalisées à chaque run.

## Limite de méthode de recette navigateur

Les clics de l'outil d'automatisation sur les **cases à cocher** et sur certains boutons de
soumission ne se propagent pas au DOM (même défaillance qu'aux tours précédents sur `<summary>`).
Contournement utilisé : `element.click()` puis `form.requestSubmit()` dans le moteur JS de la page
— vrais événements DOM, vraie soumission HTTP, mais le geste physique n'est pas reproduit. Le reste
du parcours (navigation, lecture, vérification, redémarrage) est réel.

## Piège d'exploitation : verrou d'exécution périmé

`test_menages_chaine.py::test_chaine_e2e_reelle_sur_copies` a échoué sur
`KeyError: 'reel_intact'` : `executer_chaine` sortait avant terme parce que
`05_APPLICATION/data/.menages_chaine.lock` traînait, laissé par un run interrompu. Le pid inscrit
dans le verrou n'existait plus.

Le verrou n'est **pas invalidé automatiquement quand son pid est mort**. Après toute interruption
brutale d'un run ménages :

```
rm -f 05_APPLICATION/data/.menages_chaine.lock
```

Ce fichier est désormais dans `.gitignore` (il porte un pid et un nom de machine) ; il y avait été
oublié, contrairement à son équivalent `.saisie_charges_write.lock`.

## Anomalie de test connue (pré-existante, hors périmètre)

`tests/test_appsec1_diagnostic.py::test_07_diagnostic_local_avec_flag_explicite` : le nom
d'utilisateur Windows apparaît dans un chemin temporaire pytest, ce que le test interdit.
**Antérieure à tout ce chantier.** Ne pas la confondre avec une régression.

## Blocs 1 à 4 : faits — voir `37_CHAINE_AVAL_RECETTE_ET_DEFAUT_LOT13.md`

| Bloc | État | Preuve |
|---|---|---|
| 1 — chaîne aval verte en recette | ✅ **fait** (hors lot13) | `RUN-27CA69FD8D87` : lot4quater → lot12 **SUCCES**, 20,4 s |
| 2 — comparaison avant/après | ✅ **fait** | deux runs réussis, tous écarts 0,00 (prouve aussi l'idempotence) |
| 3 — clôture `VALIDEE` | ✅ **fait** | `OUVERTE → EN_CALCUL → A_CONTROLER → VALIDEE`, persiste au redémarrage |
| 4 — rejouer un lot | ✅ **fait** | case à cocher par lot dans le formulaire de lancement, 4 tests |

Correction de raisonnement du handoff précédent : il ne fallait **pas** seeder
`MASTER_CALC_Reservations_Resolues.xlsx` — c'est la *sortie* de lot4quater, la seeder aurait
fabriqué un faux succès. Ce qui manquait, ce sont les **entrées** (table live lot4bis, payout, et
7 sources que lot11 charge inconditionnellement, recopiées en en-tête seul).

## ✅ Défaut lot13 : corrigé (phase 1 du tour du 2026-07-27)

Décision utilisateur : **renommer à la frontière d'export**, sans supprimer la donnée ni affaiblir
le filet. `preparation_canape_voyageurs` (nom interne, inchangé dans les `MASTER_*`) est exporté
sous `montant_preparation_canape` dans `PBI_Commissions.csv`.

Seul point de code modifié : `RENOMMAGES_EXPORT` dans `lot13_export_powerbi.py`, et le filet qui
contrôle désormais les noms **réellement exportés**. Aucun calcul métier touché.

Preuves : 11 tests (le `xfail(strict)` est **remplacé par la preuve**, pas supprimé) ;
`RUN-64BF7084CBB0` = `lot4quater → lot13` en **6/6 SUCCES** ; second run `RUN-048B5CF57C3F` avec
tous écarts à 0,00. Détail dans `38_LOT13_CONTRAT_EXPORT_POWERBI.md`.

**À répercuter côté utilisateur** : le rapport Power BI doit référencer
`montant_preparation_canape`. C'est le seul consommateur externe du CSV.

## Module actif / module terminé

| | |
|---|---|
| Modules terminés | Logements, Banque, Fournisseurs-Factures-Règlements, Pilotage des calculs (chaîne aval complète), Charges exercée, Comptabilité (cœur), Analytique (périmètre défini), Résultats (périmètre défini) |
| Module actif | *(Aucun — recette globale COMPLÈTE, verdict GO validation humaine complète. Prochaine étape réelle non entreprise : arbitrage métier sur les règles de classification Banque, ou décision sur l'activation progressive du mode réel, ou tout autre chantier explicitement demandé. Ne pas démarrer le mode réel ni une autre mission sans instruction explicite.)* |

## ✅ Mission 16 de ce tour — Anomalie lot4quater expliquée (pas un bug), cycle Banque complet, campagne rejouée : GO validation humaine complète

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `6d80612`
avant ce tour, master `8b47807`, git status propre, 88/88 hashes réels re-vérifiés identiques).

**Audit de l'anomalie `lot4quater`/`CTR-9-003`** : lecture complète de `lot4quater_resoudre_
source_reservations.py` — **aucun paramètre mois n'existe dans ce script**, il reconstruit
toujours l'intégralité de l'historique (mois ouverts depuis le live, mois clôturés depuis HIST).
Exécution directe sur la copie actuelle : 1391 MASTER / 1349 VUE_FLUX, correct. La cause du run
défaillant précédent (104 lignes) : `HIST_Reservations_Cloturees.xlsx` n'était pas encore copié
dans l'environnement à ce moment précis (copié depuis, mission antérieure, pour débloquer
`lot11`) — lot4quater a appliqué son propre repli déjà documenté (`CLOTURE_SANS_HIST` →
`A_CONTROLER`), jamais un crash. **Preuve définitive** : `/calculs` relancé deux fois pour le mois
2026-06 **depuis l'écran applicatif** (pas les scripts moteur) → 6/6 lots SUCCÈS les deux fois,
totaux identiques au centime près à l'exécution moteur directe (REEL 291 852,76 €), idempotent.
**Aucune correction de code appliquée** — le contrat était déjà correct. 6 tests nouveaux
(`tests/test_lot4quater_resoudre_source_reservations.py`) fixant ce contrat. Détail :
`64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md`.

**Cycle Banque complet** (Lot8a→8b→8c, scripts moteur, copie) : Lot8b (classification, 24 VALIDE/
517 A_CONTROLER, 30 règles seed) et Lot8c (rapprochement, 166 Airbnb + 56 propriétaires en attente,
**0 confirmation automatique**) tous deux **SUCCÈS**, tous deux **applicables** au format
consolidé (aucun `NON_APPLICABLE` nécessaire — Lot8b/8c ne lisent que `NORM_Banque`, structure
canonique identique quel que soit le format d'entrée). Effet mesuré : Lot9 intègre 24 flux de
frais bancaires (`TYPE_FLUX_016`, auparavant 0), Lot11 passe de `BANQUE_NON_DISPONIBLE_GIT` à
`BANQUE_DISPONIBLE`. Nouveaux totaux réconciliés (REEL 291 722,75 € = COMPTABLE 281 198,59 € +
HORS_COMPTA 10 524,16 €, écart 0,00 €), idempotents (lot9/lot10 relancés). Réconciliations
rejouées via l'application : A/B/D/H **OK**. Recette navigateur sur les écrans Banque
(`/banques-caisse`, fiche mouvement) : drill-down sans 404, comptes masqués, classification et
rapprochement affichés fidèlement. Détail : `65_RAPPORT_CYCLE_BANQUE_COMPLET.md`.

**Deux points de sécurité pré-existants notés, hors mandat, non corrigés** : bandeau `MODE
RECETTE` affichant un chemin absolu (comportement de template antérieur à cette mission) ;
libellés bancaires pouvant contenir des fragments de compte tiers (inhérent au texte des relevés,
affichage nécessaire à la classification humaine). Ni l'un ni l'autre introduit par cette mission.

**Campagne complète rejouée** (`TEST_SHARDS_RECETTE_GLOBALE.txt`, 131 fichiers, 6 shards, codes
retour réels) : **2281 passés / 75 ignorés / 1 échec pré-existant** (`test_appsec1_diagnostic`) —
identique au caractère près à la référence, aucune régression sur toute la série de missions
Banque. `tests/` racine (moteur) : 268 passés, 0 échec.

88/88 fichiers réels de données re-vérifiés identiques après le cycle complet (seul écart :
`lot8a_banque_import.py`, code déjà commité lors d'une mission antérieure).

**Verdict : GO POUR VALIDATION HUMAINE COMPLÈTE** (jamais GO mode réel). Tous les critères de fin
du bloc Banque/chaîne applicative sont atteints : Lot8a natif et consolidé verts, Lot8b/8c
exercés, `/calculs` produit le même résultat que les moteurs directs, `CTR-9-003` expliqué sans
désactivation, Lot9→Lot13 verts, réconciliations rejouées, recette navigateur, idempotence,
campagne complète déterministe, 88/88 sources intactes, aucune anomalie bloquante inexpliquée.
Détail complet : `60_VERDICT_GO_NO_GO.md` (mis à jour), `64`, `65`.

## ✅ Mission 15 de ce tour — Lot8 accepte le format consolidé, chaîne aval rejouée avec succès, verdict GO validation humaine partielle

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `35a04b1`,
master `8b47807`, git status propre, 86/86 hashes réels re-vérifiés identiques).

**Audit de provenance** (30 min) : le relevé consolidé fourni fusionne 3 exports bruts Crédit
Mutuel successifs (`comptes_4_complet_lignes_releve(1).xlsx`, `comptes (6).xlsx`,
`comptes (7).xlsx`, cités dans son propre onglet `Sources`) — **non circulaire**, ne provient
d'aucune sortie Lot8/Lot9+. Méthode de fusion et contrôles déjà documentés par le fichier lui-même
(103 doublons retirés, solde raccordé sans écart). Poursuite autorisée.

**Implémentation (`02_TRAVAIL/lot8a_banque_import.py`)** : détection de format par présence de
feuilles (`FORMAT_CREDIT_MUTUEL_NATIF`/`FORMAT_RELEVE_CONSOLIDE`/`FORMAT_INCONNU`, jamais par nom
de fichier), deux adaptateurs (`lire_natif`/`lire_consolide`) convergeant vers le même `raw_rows`
canonique (7 colonnes) avant de rejoindre la normalisation/dédoublonnage/écriture déjà existante —
aucune duplication de moteur. Point critique corrigé dans l'adaptateur consolidé : conversion
`0 → None` sur Débit/Crédit (le format consolidé renseigne toujours les deux colonnes, contrairement
au natif) — sans elle, chaque ligne aurait été signalée à tort `BANQUE_DEBIT_CREDIT_DOUBLES`.
Nouvelle colonne `format_source` en fin de `LOG_Traitement` (position 14, aucun consommateur
`lot8b`/`lot8c` cassé, ils lisent par position ≤13). Colonne `commentaire` existante réutilisée
pour la provenance par ligne — aucune colonne inventée.

**11 tests nouveaux** (`tests/test_lot8a_banque_import.py`, fixtures entièrement fictives) :
régression format natif, détection/adaptateur consolidé, conversion 0→None, totaux, multi-mois
`A_CONTROLER` non bloquant, doublon détecté non masqué, idempotence, consolidé incomplet →
`INCONNU`, fichier absent, confidentialité stdout. Suite complète `tests/` (moteur) : **262 passés,
0 échec**. Suite ciblée Banque application : **58 passés, 17 ignorés, 0 échec**.

**Exécution réelle sur copie** : `lot8a` produit `BANQUE_LOT8_IMPORT.xlsx` (541 mouvements, 0
BLOQUANT, 1 `A_CONTROLER` pour la période multi-mois — non bloquant). Totaux **identiques au
centime près** à ceux de la feuille `Synthese` du fichier (51 744,37 € débit / 52 148,21 € crédit).
Idempotent (2 exécutions, mêmes totaux, mêmes identifiants). Fichier original jamais modifié (hash
inchangé, vérifié).

**Chaîne aval rejouée avec succès** (`lot9`→`lot10`→`lot11`→`lot12`→`lot13`, scripts moteur
directs — cf. ci-dessous pour la raison) : tous contrôles bloquants OK, REEL=COMPTABLE+HC vérifié
(291 852,76 = 281 328,60 + 10 524,16, écart 0,00 €), idempotence confirmée (lot9/lot10 relancés,
mêmes totaux). Réconciliations rejouées via l'application sur les sorties fraîches : A/B/D/H
**OK** (écart 0,00 €), C/E/F/G toujours `A_CONTROLER`/`NON_DISPONIBLE` (attendu). Sécurité : 0
fuite sur les pages et les 13 exports Power BI.

**Écart de copie corrigé en cours de route** : `02_DONNEES_NORMALISEES/` (requis par `lot11`)
n'avait pas été copié dans l'environnement de recette globale — complété, hash vérifié, ajouté au
manifeste (88 fichiers réels de données suivis désormais).

**Anomalie nouvelle trouvée, hors mandat, non corrigée** : en tentant de rejouer la chaîne via
l'écran `/calculs` de l'application (plutôt que les scripts moteur directs), `lot9` échoue —
`lot4quater_resoudre_source_reservations.py` régénère `VUE_FLUX` scopé au seul mois demandé (104
lignes) au lieu de l'historique complet (1349 lignes), déclenchant `CTR-9-003` (volume suspect).
Sans rapport avec le format Banque. Sorties restaurées, aucune correction appliquée (« ne commence
aucune nouvelle fonctionnalité »). Consigné comme anomalie ouverte distincte, mission future.

**Verdict : GO POUR VALIDATION HUMAINE PARTIELLE** (jamais GO mode réel) — « partielle » car la
chaîne n'a pas encore été rejouée depuis l'écran applicatif lui-même (anomalie `lot4quater`
ci-dessus), et Lot8b/8c (classification/rapprochement) restent à exécuter pour un cycle Banque
complet. Détail complet : `63_CONTRAT_FORMAT_RELEVE_BANCAIRE_CONSOLIDE.md`,
`60_VERDICT_GO_NO_GO.md` (mis à jour).

## ✅ Mission 14 de ce tour — Relevé Banque fourni, Lot8 exécuté sur copie : contrat incompatible, verdict NO GO — SOURCE BANQUE INCOMPATIBLE

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `1ff9e52`,
master `8b47807`, git status propre). Cette fois, `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_
CreditMutuel.xlsx` **existe réellement** (141 986 octets, SHA256 `a84c9b51b1c0eb50d17216272bd3c6cf
2669d159bf7e1299c2b762face0ca4a8`) — vérifié avant toute lecture métier.

**Copie contrôlée** : fichier copié dans `_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/
SOURCES_COPIEES/01_SOURCES_BRUTES/Banque/`, hash source=copie vérifié identique. Fichier original
jamais ouvert en écriture, jamais renommé, jamais modifié.

**Lot8 exécuté sur la copie uniquement** (jamais sur le réel) :
```
[OK] Source brute : ...SOURCES_COPIEES\01_SOURCES_BRUTES\Banque\BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx
[ERREUR BLOQUANT] Feuille "Cpt 02211 00021321603" absente.
  Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']
EXITCODE=1
```
`BANQUE_LOT8_IMPORT.xlsx` **non produit** (le script sort avant l'écriture — aucun fichier vide,
aucune ancienne sortie réutilisée, dossier `Lot8_Banque/` créé vide comme simple effet de bord).

**Analyse du fichier fourni** : c'est un **rapport consolidé** (titre interne *« Relevé bancaire
consolidé — WONDERBNB »*, note *« ancien consolidé retenu jusqu'au 31/05/2026, puis relevé du
01/08/2026 prioritaire »*), pas l'export brut Crédit Mutuel que le script attend. Même compte (RIB
`10278 02211 00021321603` identique à `CM_02211_00021321603`), mais feuille `Mouvements` à
**12 colonnes** (`N°`, `Date opération`, `Date de valeur`, `Libellé`, `Débit`, `Crédit`, `Montant
net`, `Solde consolidé`, `Devise`, `Source du relevé`, `Mois`, `Ligne source`) contre les 7
attendues, et une période de 9 mois (03/11/2025→01/08/2026) au lieu du mois nominal `2026-03`.

**Aucune correction appliquée** : ni renommage de feuille, ni adaptation des colonnes lues par
`lot8a_banque_import.py`, ni conversion du fichier — cela aurait changé le contrat métier
unilatéralement pour accepter un format non prévu, explicitement hors mandat. 85/85 hashes réels
historiques re-vérifiés identiques ; le nouveau relevé ajouté au manifeste d'intégrité, jamais
modifié (hash inchangé après traitement).

**Aucune correction de code, aucun commit de correction applicative.** Suite de tests inchangée
(2281/75/1).

**Verdict : NO GO — SOURCE BANQUE INCOMPATIBLE.** Action requise, décision humaine : fournir
l'export brut natif Crédit Mutuel (feuille `Cpt 02211 00021321603`, 7 colonnes), ou décider
explicitement d'adapter `lot8a_banque_import.py` pour consommer ce format consolidé — un
changement de contrat, pas une correction de bug. Détail complet :
`61_CONTRAT_SOURCE_BANQUE_LOT8.md` (section « Suite »), `60_VERDICT_GO_NO_GO.md` (mis à jour).

## ✅ Mission 13 de ce tour — Vérification du relevé Banque annoncé : toujours absent, aucune action possible

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `893c630`,
master `8b47807`, git status propre, 85/85 hashes réels re-vérifiés identiques).

La mission reçue annonçait un relevé Crédit Mutuel « nouvellement fourni » sous
`01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx`. Conformément à l'étape 1 de la
mission (« contrôler le fichier fourni avant toute lecture métier »), vérification directe avant
tout traitement : **le dossier `01_SOURCES_BRUTES/Banque/` n'existe toujours pas** sur disque, dans
le réel. Recherche large complémentaire (motifs `*BRUT_Banque*`, `*CreditMutuel*`, tout `.xlsx`
modifié depuis le dernier verdict) sur l'ensemble du worktree ET sur l'environnement de copies
(`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) : **aucune trace du fichier annoncé**.

**Aucune action tentée sur cette base** : ni copie, ni exécution de Lot8, ni pipeline, ni
réconciliation, ni recette navigateur — la prémisse de la mission (fichier fourni) ne correspondait
pas à l'état observé du disque, et fabriquer un résultat sur cette base aurait été le type même de
faux succès explicitement interdit par les missions précédentes. Signalé à l'utilisateur plutôt que
supposé résolu silencieusement.

**Aucune correction de code, aucun commit de correction.** Suite de tests inchangée (2281/75/1).
85/85 hashes réels re-vérifiés identiques (aucun ajout, aucune modification).

**Verdict inchangé : NO GO — SOURCE BANQUE REQUISE.** Détail : `61_CONTRAT_SOURCE_BANQUE_LOT8.md`
(contrat, toujours d'actualité), `JOURNAL_ANOMALIES.md` (entrée « VÉRIFICATION 2026-08-02 »).

## ✅ Mission 12 de ce tour — Contrat Banque Lot8 résolu à la racine, mois Lot10 expliqués, verdict final NO GO — SOURCE BANQUE REQUISE

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `e3b9b1f`,
master `8b47807`, git status propre, 85/85 hashes réels re-vérifiés identiques, environnement de
copies `_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/` toujours présent, lecture intégrale de
`HANDOFF_CANONIQUE.md`/`48`/`54`-`60`).

**Contrat Lot8 (audit ciblé, `61_CONTRAT_SOURCE_BANQUE_LOT8.md`)** : lecture complète de
`lot8a_banque_import.py`. `BANQUE_LOT8_IMPORT.xlsx` est une **sortie** de Lot8 (jamais une source
utilisateur), produite depuis `01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx`
(export brut Crédit Mutuel, compte `02211 00021321603`, feuille `Cpt 02211 00021321603`, en-tête
ligne 5, données ligne 6). Ce fichier brut **n'existe nulle part** — le dossier
`01_SOURCES_BRUTES/Banque/` n'existe pas physiquement sur disque, ni dans le réel ni dans les
copies (rien à copier). Lot8 est 100 % exécutable hors réseau (openpyxl pur) dès que la source est
fournie. Contrat Lot8→Lot9 vérifié cohérent (`SRC_BNQ` de `lot9_construire_flux.py` pointe
exactement vers `OUT_FILE` de `lot8a`), aucune divergence code/documentation.

**Cas B confirmé** (aucune source bancaire brute exploitable) : aucun contournement codé, aucune
donnée fabriquée, aucune source réelle modifiée. Action précisément documentée pour l'utilisateur :
exporter le relevé, le déposer sous `01_SOURCES_BRUTES/Banque/` (réel), exécuter `lot8a` avant toute
nouvelle recette pipeline complète.

**Deux mois Lot10 manquants, cause trouvée (`62_RAPPORT_MOIS_LOT10_MANQUANTS.md`)** : remontée de
Lot10 → Lot9 → Lot4quater. Les deux mois (2026-11, 2027-01) sont déjà absents du flux Lot9 ; en
remontant à `MASTER_CALC_Reservations_Resolues.xlsx`, l'onglet `VUE_FLUX` (celui que Lot9 consomme
réellement) les exclut déjà, alors que l'onglet `MASTER` (historique complet) les contient : chacun
de ces deux mois ne porte qu'**une seule réservation**, un placeholder `A_CONTROLER`/
`DIRECT_SANS_SAISIE_HH` à montant 0 (LOG_0015/PROP_0011, réservation directe sans saisie
Hors-Hostaway) — la même ligne existe aussi en 2026-12 (qui, elle, apparaît en Lot9/Lot10 grâce à
une autre réservation validée ce mois-là). Lot4quater exclut légitimement ces placeholders de sa
vue financière (`VUE_FLUX`) ; pour ces deux mois précis, c'était la seule ligne, donc le mois entier
disparaît en aval. **Filtrage upstream cohérent, pas un bug** — statut VIDE_VALIDE/NON_APPLICABLE.
Aucune correction de code nécessaire, aucun test rouge/vert requis.

**Aucune correction de code appliquée ce tour non plus** — audit et remontée de cause racine
uniquement. **0 commit de correction**, suite de tests inchangée (2281/75/1).

**Verdict final : NO GO — SOURCE BANQUE REQUISE** (porte sur la ré-exécution du pipeline aval, pas
sur la validité applicative des modules déjà alimentés — validation humaine du périmètre alimenté
reste AUTORISÉE ; préparation du mode réel reste NO GO). Détail complet :
`60_VERDICT_GO_NO_GO.md` (mis à jour), `61_CONTRAT_SOURCE_BANQUE_LOT8.md`,
`62_RAPPORT_MOIS_LOT10_MANQUANTS.md`.

## ✅ Mission 11 de ce tour — Recette globale sur copies contrôlées des données réelles : verdict GO pour validation humaine

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `ede8c52`,
master `8b47807`, git status propre, lecture intégrale de `HANDOFF_CANONIQUE.md`/`48`/`49`/
documents Analytique/Résultats/`TEST_SHARDS_ANALYTIQUE_RESULTATS.txt`).

**Environnement de copies** créé hors Git (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) :
85 fichiers réels (`01_SOURCES_BRUTES/`, `02_TRAVAIL/`, plus une copie de l'`app.db` réel) hashés
(SHA256), copiés, re-vérifiés identiques source=copie, puis **re-hashés après toutes les
opérations : 85/85 identiques, 0 écart**. Aucune donnée réelle jamais modifiée.

**Migrations** testées sur une copie de l'`app.db` réel ET sur une base vierge : `integrity_check`
ok, 0 violation de clé étrangère, idempotentes au niveau des lignes (24 migrations, comptages de
tables identiques après une deuxième application, même si le hash brut du fichier change —
réallocation de pages SQLite, pas une duplication logique).

**Pipeline sur copies** : chaîne aval (`lot4quater→lot9→lot10→lot11→lot12→lot13`) lancée pour
2026-06 — **lot4quater SUCCÈS, lot9 ÉCHEC** (`BLOQUANT [CTR-9-001] Source manquante :
BANQUE_LOT8_IMPORT`, dossier `Lot8_Banque/` inexistant dans le réel — Banque n'a jamais été
exécuté en réel, seulement en `data_recette` fictif). Lots 10-13 correctement IGNORÉS (dépendance
en échec, aucun faux succès). Sorties restaurées (7 fichiers, hash de restauration vérifié
identique à l'original copié). Chaîne `charges` (lot3) lancée deux fois : SUCCÈS les deux fois,
idempotente (0 charge réelle saisie actuellement — `SAISIE` réelle à 0 ligne, état réel, pas un
défaut).

**Réconciliations sur copies réelles (24 mois, 2025-01→2027-02, hors 2026-11/2027-01 absents)** :
A, B, D, H **OK** (écart 0,00€) — B confirme en conditions réelles le correctif de grains
incompatibles du tour précédent. C, G `A_CONTROLER` et E, F `NON_DISPONIBLE` : attendus, 0
écriture/facture/ménage réels enregistrés dans l'app (module jamais exercé en réel). Invariant
REEL=COMPTABLE+HORS_COMPTA vérifié sur le total réel (291 779,67€), écart 0,00€.

**Sécurité** : 0 fuite (chemins absolus, username, email, téléphone, IBAN, token) sur les pages et
exports scannés. **Performance** : toutes les pages < 1 s, rien à corriger (pas de problème
mesuré). **Double comptage** : aucun trouvé sur les axes exercés ; contrôles Factures/Ménages/
Comptabilité sans objet (0 ligne réelle).

**Aucune correction de code appliquée** : l'unique anomalie trouvée (source Banque manquante) est
un écart de données/processus réel, pas un défaut applicatif — le contrôle `CTR-9-001` fonctionne
exactement comme conçu. **0 commit de correction**, suite de tests inchangée (2281/75/1, campagne
du tour précédent toujours valide).

**Verdict : GO POUR VALIDATION HUMAINE** (jamais GO pour mode réel). Détail complet :
`54_RAPPORT_RECETTE_GLOBALE_COPIES.md`, `55_MATRICE_ECARTS_CONTRATS_REELS.md`,
`56_RAPPORT_RECONCILIATIONS_GLOBALES.md`, `57_RAPPORT_DOUBLE_COMPTAGE.md`,
`58_RAPPORT_SECURITE_RECETTE_GLOBALE.md`, `59_RAPPORT_PERFORMANCE.md`, `60_VERDICT_GO_NO_GO.md`.

`ETAT_AVANCEMENT.md`/`ARCHITECTURE_DONNEES.md` non modifiés ce tour non plus : confirmé via
`git log` qu'ils appartiennent à une couche de documentation antérieure à ce chantier (dernière
modification `053f195`, avant les missions Comptabilité/Analytique/Résultats/recette globale) —
même jugement assumé que le tour précédent, pas un oubli.

## ✅ Mission 10 de ce tour — Fermeture Analytique/Résultats : Lot9↔Lot10 (8/8), axes fournisseur/catégorie/prestataire, plateforme/activité assumés

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `bef5609`,
master `8b47807`, git status propre, lecture intégrale de `HANDOFF_CANONIQUE.md`/`48`/`49`/`50`/
`51`/`52`). Audit ciblé (Bloc 1) : `MASTER_CALC_Flux.xlsx` (Lot9, 22 colonnes documentées dans
`lot9_construire_flux.py`) porte déjà `mois`/`logement_id`/`sens`/`montant`/
`inclure_resultat_<vision>` — exactement les colonnes que `lot10_calculer_resultats.build_resultats`
lit pour produire `PAR_MOIS_LOGEMENT`. Aucun lecteur applicatif n'existait pour ce fichier — seul
manque réel identifié pour fermer la réconciliation A.

**Bloc 2 — Réconciliation Lot9↔Lot10 construite** : `app/readers/lot9_flux_reader.py` (lecture
seule) + `cfg.MASTER_CALC_FLUX` (additif) ; `comptabilite_reconciliations_service.lot9_vs_lot10`
réimplémentée (l'ancienne version renvoyait toujours `NON_DISPONIBLE`). Grain `(mois, logement_id)`,
même sentinelle `GLOBAL_NON_AFFECTE` que Lot10, `NEUTRALISATION` exclue, doublons de `ROW_HASH`
détectés et signalés (jamais masqués même si les totaux coïncident). 11 tests dédiés. Un test
existant (`test_lot9_vs_lot10_non_disponible`) supposait à tort une absence permanente de source —
corrigé pour isoler `cfg.MASTER_CALC_FLUX` explicitement (le vrai worktree porte un vrai fichier
Lot9, lisible en lecture seule, ce qui rendait le test non déterministe sans ce monkeypatch — même
catégorie de correction que pour `test_resultats_routes.py`).

**Les 8 réconciliations sont désormais toutes implémentées.** Suite ciblée Comptabilité+Résultats
rejouée : 195 passés, 0 échec.

**Bloc 3/4 — Axes analytiques.** Audit : `menages.fournisseur_id_opaque`/`cout_prevu`/`cout_reel`
(`0019`) — source réelle jamais exploitée comme axe — et `charges_reader.categorie_charge_id`
(déjà lu par `/resultats/charges`) sont des sources fiables et complètes pour prestataire et
catégorie ; `canal_id` n'existe que côté réservations hors Hostaway (fraction non représentative)
et `type_flux_id` est une classification technique fine, jamais organisée en taxonomie d'activité
dans les décisions métier.

Nouveau `comptabilite_axes_service.py` : fournisseur (relit `comptabilite_auxiliaires_service`,
`49`, aucun second calcul), catégorie (relit `charges_reader`, ajoute le détail par catégorie),
prestataire (agrège `menages` directement — **≠ fournisseur même si même tiers**, chaque fiche
prestataire lie explicitement vers la fiche fournisseur du même tiers). Plateforme et activité :
`NON_DISPONIBLE` avec raison explicite, jamais une taxonomie inventée.

8 routes ajoutées : `/resultats/fournisseurs/{id}` (nouveau — la liste existait), `/resultats/
prestataires(+{id})`, `/resultats/categories(+{id})`, `/resultats/plateformes/{id}`, `/resultats/
activites(+{id})`. 23 tests ajoutés (`test_comptabilite_axes.py` 13, `test_resultats_axes_routes.py`
10). Suite ciblée Comptabilité+Résultats+Axes rejouée : **246 passés, 0 échec**.

**Bloc 5 — Drill-down.** L'écriture comptable affichait son origine (`FACTURE`/`REGLEMENT`/
`LOT12_PROPRIETAIRE_MOIS`/`RAPPROCHEMENT`) en texte brut — la chaîne axe → mesure → écriture
s'arrêtait là. Liens ajoutés par `origine_type` vers l'objet réel (aucun lien ajouté là où aucune
route de détail n'existe, ex. `CHARGE`/`FACTURE_LIGNE` au niveau ventilation). Fiche prestataire :
`menage_id_opaque` rendu cliquable. 6 tests (`test_resultats_drilldown.py`), vérifiant aussi les
404 propres sur identifiant inconnu.

**Bloc 6 — Exports par axe.** CSV ajoutés : dashboard (global), propriétaire, fournisseur,
prestataire, catégorie, réconciliation (logement existait déjà). Bug attrapé par les tests : les
routes `export.csv` étaient déclarées après les routes `/{id}` correspondantes — FastAPI matchait
`"export.csv"` comme identifiant opaque. Corrigé par réordonnancement. Plateforme/activité : pas
d'export, cohérent avec `NON_DISPONIBLE`. 8 tests (`test_resultats_exports_axes.py`).

**Bloc 7 — Recette navigateur réelle (30 étapes, pas seulement TestClient).** `build_data_recette.py`
rejoué, serveur lancé (port 8020, `PROJECT_ROOT` et `APP_DATA_DIR` isolés, tous les flags d'écriture
activés), pipeline aval lancé pour 2026-06 (6/6 lots OK). **Anomalie réelle trouvée, absente des
tests unitaires** : la réconciliation B (Lot10↔Analytique) comparait `Lot10 GLOBAL` (tout le jeu de
données, sans grain mensuel) à l'Analytique filtrée sur le mois choisi — écart artificiel de
10 035,00 € dès qu'un mois était sélectionné. Corrigé (`app/routes/resultats.py`) : B ignore
désormais le filtre mois, comme son alias H. Test de non-régression à deux mois ajouté. Vérifié en
navigateur réel : dashboard, 3 visions, tous les écrans par axe, 8 réconciliations, tous les exports,
cumul, comparaison mois précédent, redémarrage serveur (persistance confirmée), relance du pipeline
(idempotence confirmée, écarts 0,00), aucun double comptage.

**Bloc 8 — Campagne de tests par shards.** Manifeste `TEST_SHARDS_ANALYTIQUE_RESULTATS.txt`
(05_APPLICATION/, temporaire, hors documentation métier) : 131 fichiers de tests, 6 shards
consécutifs, exécution séquentielle avec `--basetemp` distinct, codes retour réels à chaque fois.
**Anomalie nouvelle trouvée et corrigée** : `test_no_metier_calc.py::test_no_import_of_travail_
modules` — faux positif sur `lot9_flux_reader.py` (le nom contenait littéralement "import lot9...").
Renommé en `flux_unifie_reader.py` (seul importeur : `comptabilite_reconciliations_service.py`).
**Total : 2281 passés, 75 ignorés, 1 échec** (`test_appsec1_diagnostic`, pré-existant, connu). Le
flake `test_proprietaires_reglements` n'est pas apparu (fichiers déclencheurs isolés du shard
contenant `test_proprietaires.py`). Aucun run tué par l'environnement.

**Bloc 9 — Documentation.** `53_AXES_ANALYTIQUES_ETAT_FINAL.md` complété (drill-down, exports,
recette, anomalie B). `48_ROADMAP_RESTANTE_PROJET.md` : Analytique et Résultats passés
**TERMINÉ (périmètre défini)**. Ce document (`HANDOFF_CANONIQUE.md`) mis à jour. `JOURNAL_
ANOMALIES.md`/`ETAT_AVANCEMENT.md`/`ARCHITECTURE_DONNEES.md` jugés hors périmètre de ce chantier
SQLite-app (couche documentation antérieure, même jugement que les tours précédents) — non modifiés.

**Critère de fin atteint pour le périmètre défini** : réconciliations 8/8 ✓, axes demandés
construits ou `NON_DISPONIBLE` assumé avec raison explicite ✓, drill-down vérifié sans 404 ni id
SQLite brut ✓, exports par axe livrés ✓, absence de double comptage vérifiée en recette réelle ✓,
campagne de tests complète ✓. Analytique et Résultats passent **TERMINÉ (périmètre défini)** —
jamais présenté comme validé métier au-delà (plan de comptes toujours PROVISOIRE, pools
multi-logements toujours un gap Ménages connu).

Détail complet : `51_MOTEUR_ANALYTIQUE_ET_RECONCILIATIONS.md` (section « Suite »),
`53_AXES_ANALYTIQUES_ETAT_FINAL.md`.

## ✅ Mission 9 de ce tour — Phase 3 Analytique/Résultats : écrans Résultats — mission complète

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `fd2845f`,
master `8b47807`, git status propre, lecture intégrale de `HANDOFF_CANONIQUE.md`/`48`/`49`/`50`/
`51`). Phase 3 sur 3 — clôt la mission Analytique/Résultats démarrée en Mission 7.

**14 routes livrées** sous `/resultats/*` (dashboard, mensuel, cumulé, logements + détail,
propriétaires + détail, plateformes, fournisseurs, charges, ménages, comptabilité, réconciliation,
lignes/{id}, export CSV) — toutes lisent `comptabilite_analytique_service.py`/
`comptabilite_reconciliations_service.py` (Phase 2) ou les services Comptabilité déjà livrés
(`49`), aucun recalcul. Fiche propriétaire distingue explicitement résultat conciergerie (Lot10) et
net propriétaire (auxiliaire/Lot12) — jamais additionnés. Page plateformes : `NON_DISPONIBLE`
assumé, aucune dimension n'existe pour cet axe. Aucun calendrier construit : les mois viennent de
`ana.mois_disponibles()`.

**Preuves** : 18 tests (`test_resultats_routes.py`). Recette navigateur réelle (Chrome, port 8092,
`data_recette` avec `PROJECT_ROOT` correctement isolé dès le départ) : dashboard sur données
fictives sans sorties Lot10 → `NON_DISPONIBLE` partout, aucun zéro fabriqué ; génération réelle
d'une écriture ACHATS depuis une facture fictive de `data_recette` → validation → apparition
immédiate dans `/resultats/fournisseurs` et `/resultats/comptabilite` ; `/resultats/charges` agrège
5 catégories réelles du MASTER Lot3 de `data_recette` ; `/resultats/reconciliation` rend les 8
lignes (D confirmée `OK`, E affiche un montant réel avec droit `NON_DISPONIBLE` en mode agrégé,
comportement attendu) ; **persistance** vérifiée par redémarrage du serveur (écriture toujours
`VALIDEE`). `data_recette/app_data/app.db` régénéré après la recette, aucune trace résiduelle.

Suite ciblée Comptabilité+Résultats rejouée : **184 passés, 0 échec**.

**Décision explicite, non entreprise ce tour** : export XLSX/Power BI (seul CSV construit) ; écran
de delta chiffré dédié entre visions (l'information existe déjà dans le tableau GLOBAL) ; tout ce
qui dépend d'axes non peuplés (plateforme, fournisseur analytique, prestataire, catégorie, activité).

Détail complet : `52_ECRANS_RESULTATS_ETAT_FINAL.md`.

## ✅ Mission 8 de ce tour — Phase 2 Analytique : moteur analytique et réconciliations

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `29b6e50`,
master `8b47807`, git status propre, lecture intégrale de `HANDOFF_CANONIQUE.md`/`48`/`49`/`50`/
`43`-`47`). Phase 2 sur 3 — Phase 3 (écrans Résultats) non commencée, sur instruction de stabiliser
chaque bloc avant d'enchaîner.

**Principe tenu** : aucun recalcul différent de Lot10, aucune nouvelle table de grain persistée
(risque de dérive vs Lot10/écritures jugé pire que l'absence de copie). `comptabilite_analytique_
service.py` lit `MASTER_CALC_Resultats.xlsx` (`PAR_MOIS_LOGEMENT`/`PAR_MOIS_PROPRIETAIRE`/`GLOBAL`)
via deux fonctions additives du reader existant (`resultats_par_logement()`, `resultats_global()`).
Mesures par vision/mois/logement/propriétaire, fiches drill-down, `drill_down_logement()` descend
jusqu'aux écritures dont la ventilation (Phase 1) porte le logement.

**Réconciliations (`comptabilite_reconciliations_service.py`, 8 fonctions)** : A (Lot9↔Lot10)
**non disponible, assumé** (aucun lecteur Lot9 applicatif, le moteur contrôle déjà ce point,
`CTR-LOT10-*`) ; B, C, D, E, F, G, H **faites** — chacune renvoie un statut parmi
`OK|ECART_TOLERE|A_CONTROLER|BLOQUANT|NON_DISPONIBLE`, jamais une exception. Notable : la
réconciliation C (Analytique↔Comptabilité) documente qu'un écart est **attendu** tant que toutes
les factures n'ont pas leur écriture générée — classé `A_CONTROLER`, jamais présenté comme un bug.

**Preuves** : 20 tests ajoutés (analytique 8, réconciliations 12), fixtures Excel isolées. Suite
ciblée Comptabilité+Propriétaires rejouée : 144 passés, 1 échec (le flake déjà documenté,
sans lien).

**Décision explicite, non entreprise ce tour** : axes plateforme/réservation/fournisseur/
prestataire/catégorie/activité non peuplés (Phase 1 n'a peuplé que logement/propriétaire) ;
réconciliation Lot9↔Lot10 réelle non construite (redondante avec le moteur).

Détail complet : `51_MOTEUR_ANALYTIQUE_ET_RECONCILIATIONS.md`.

## ✅ Mission 7 de ce tour — Phase 1 Analytique : mappings comptables branchés, ventilation analytique (migration `0024`)

Continuation autonome après vérification préalable complète (worktree, branche, HEAD `4e0b159`,
master `8b47807`, git status propre, absence de processus/verrou, lecture intégrale de
`HANDOFF_CANONIQUE.md`/`48`/`49`/`43`-`47`). Phase 1 des trois phases demandées (mappings/
ventilation → moteur analytique → Résultats) — les deux suivantes non commencées, sur instruction
de ne pas enchaîner sans stabiliser d'abord chaque bloc.

**Audit ciblé** : `categorie_charge_id`, `type_flux_id`, `logement_id`, `proprietaire_id` existent
déjà dans le MASTER Lot3 (jamais lus par la Comptabilité) ; `facture_lignes.logement_id` existe
déjà (`0022`, jamais transmis à l'écriture) ; `menages.logement_id`/`proprietaire_id` (NOT NULL,
`0019`) résolvent une charge liée à un ménage sans dimension propre. Aucune clé de poids n'existe
pour une charge multi-logements hors `facture_lignes` (pools/`REC_002`, gap déjà connu `41` §7bis) —
resté explicitement non traité, pas improvisé.

**Ajouté (migration `0024`, additive)** : `mapping_comptable_regles` (résolution historisée
CATEGORIE→TYPE_FLUX→PROVISOIRE_GENERIQUE, jamais un mapping expiré ou pas encore actif) ;
`ecriture_ligne_ventilation` (traçabilité méthode/statut/origine/mapping par ligne). `generer_ecriture_achat`
refondu : une ligne de débit par charge source (mono-charge ou `facture_lignes`), compte résolu via
mapping, dimensions peuplées selon 4 cas (A affectation directe, D facture multi-lignes, E ménage,
F sans dimension → `A_CONTROLER`, jamais bloquant). VENTES : `proprietaire_id` posé sur les deux
lignes (déjà en paramètre, jamais transmis avant). Écran `/comptabilite/mappings` (liste + création
de règle) ; bloc « Ventilation analytique » sur la fiche écriture.

**Preuves** : 22 tests ajoutés (mappings 10, routes 3, ventilation 9), suite ciblée Comptabilité
rejouée — **169 passés, 0 échec**. Réconciliation montant source = somme ventilations garantie
structurellement (aucun recalcul, seule redistribution de montants déjà exacts).

**Décision explicite, non entreprise ce tour** : case C (répartition multi-logements par pool) reste
non traitée — gap Ménages déjà connu, pas improvisé ici. Plan de comptes détaillé toujours non
arbitré (le mécanisme est prêt, aucune règle `VALIDE` n'est créée par défaut).

Détail complet : `50_MODELE_MAPPINGS_ET_VENTILATION_ANALYTIQUE.md`.

## ✅ Mission 6 de ce tour — Cœur Comptabilité complet : VENTES, CAISSE, OD, auxiliaires, périodes, clôture (migration `0023`)

Continuation autonome sur instruction explicite, après vérification préalable complète (worktree,
branche, HEAD `9bb24a7`, master `8b47807`, git status propre, absence de verrou périmé, intégrité
des sources réelles, lecture intégrale de `HANDOFF_CANONIQUE.md`/`48`/`43`-`47`/
`DECISIONS_METIER.md`/`ARCHITECTURE_DONNEES.md`/`JOURNAL_CONTROLES.md`/`JOURNAL_ANOMALIES.md`).
Audit ciblé (45 min) : ACHATS/BANQUE déjà solides (mission 3), VENTES/CAISSE/OD/périodes/clôture/
auxiliaires propriétaires-associés absents comme annoncé par `45`.

**Ajouté (additif, aucune table `0021`/`0022` modifiée)** :
- Plan comptable étendu : `530000` Caisse, `467000` Associés, `706000` Ventes (commissions).
- Table `mapping_categorie_compte` (catégorie de charge → compte, statut `A_CONTROLER`) —
  infrastructure créée, **pas reliée** à `generer_ecriture_achat` (gap honnêtement documenté, `46`).
- **VENTES** : `ventes_lot12_adapter_service.py`, lit `montant_du_conciergerie` (Lot12, jamais
  recalculé), génère une écriture par propriétaire/mois, libellé `SOURCE_PROVISOIRE_LOT12`.
- **CAISSE** : règlement fournisseur (moyen CAISSE) + `operations_caisse_service.py` (encaissement,
  remboursement associé — objets neufs).
- **ODIVERSES** : `operations_diverses_service.py`, objet BROUILLON à lignes équilibrées, écriture
  générée seulement à la validation.
- **Périodes comptables** (`comptabilite_periodes_service.py`) : `OUVERTE→EN_CONTROLE→VALIDEE→
  CLOTUREE→ROUVERTE`, contrôle de fermeture câblé dans `_inserer_ecriture` (partagé par tous les
  générateurs, pas dupliqué par journal) — une période clôturée refuse toute écriture directe,
  vérifié y compris par contournement SQL direct. Réouverture exige une justification non vide.
- **Contrôles comptables** (`comptabilite_controles_service.py`) : 15 codes, garde de clôture
  (refuse si un `BLOQUANT` subsiste).
- **Auxiliaires** (`comptabilite_auxiliaires_service.py`) : vue fournisseurs/propriétaires/associés,
  solde + éléments ouverts, aucun second calcul de solde.
- 12 nouvelles routes, 7 nouveaux templates.

**Preuves** : 104 tests ajoutés (36+9+7+4+4+1 nouveaux fichiers, 100 % verts) ; parcours complet
ACHATS→BANQUE→VENTES→CAISSE→OD→contrepassation→période→clôture→écriture refusée→réouverture via
HTTP réel (`test_comptabilite_coeur_recette.py`) ; passe navigateur Chrome réelle (port 8091,
`data_recette` isolé) sur CAISSE/OD/Périodes/Auxiliaires/VENTES — toutes les transitions et calculs
observés correspondent exactement au comportement attendu. Suite complète rejouée en tranches après
le changement, sans régression (seul l'échec pré-existant documenté subsiste).

**Incident détecté et corrigé pendant la recette** : un premier essai navigateur a démarré le
serveur avec `APP_DATA_DIR` seul (sans `PROJECT_ROOT`), ce qui a fait lire les VRAIS fichiers Lot12
(lecture seule, aucune source modifiée) et écrire de vrais noms de propriétaires dans le libellé
d'écritures VENTES de `data_recette/app_data/app.db`. Détecté immédiatement, corrigé en supprimant
et régénérant ce fichier (idempotent), vérifié sans PII résiduelle. Leçon consignée dans
`JOURNAL_ANOMALIES.md` : toujours fixer `PROJECT_ROOT=<data_recette>` en plus de `APP_DATA_DIR`
pour tout écran qui touche un adaptateur lisant l'arbre réel (Lot9/Lot10/Lot12).

**Décision explicite, non entreprise ce tour** : plan de comptes détaillé toujours PROVISOIRE ;
mapping catégorie→compte non relié à la génération réelle ; facture propriétaire/tiers/avoir comme
objets applicatifs restent hors périmètre (décision `44` non révisée) ; Analytique et Résultats non
commencés, sur instruction explicite.

Détail complet : `49_COEUR_COMPTABILITE_ETAT_FINAL.md`.

## ✅ Mission 5 de ce tour — Factures : lignes de facture, multi-charges / multi-logements (migration `0022`)

Gap du roadmap `48` : `factures.charge_id` (0017) impose une charge unique par facture (index
unique) — aucune facture ne pouvait couvrir plusieurs charges ou plusieurs logements. Décision
utilisateur explicite (choix recommandé) : ajout **additif**, `charge_id` reste pour le cas
mono-charge historique, nouvelle table `facture_lignes` pour le cas multi-charges/multi-logements.
Décision utilisateur explicite n°2 : ne pas construire factures propriétaires émises / factures
tiers / avoir-objet ce tour — la décision `44` de ne pas migrer lot12 vers SQLite n'est pas remise
en cause.

Ajouté : migration `0022_facture_lignes.sql` (table + index unique sur `charge_id`, même règle
qu'avant : une charge n'est jamais rattachée deux fois) ; `factures_service.ajouter_ligne()` /
`lignes()` ; les deux mécanismes (mono-charge / lignes) sont mutuellement exclusifs par facture,
contrôlé dans les deux sens (`lier_charge` refuse si des lignes existent déjà, `ajouter_ligne`
refuse si `charge_id` est déjà posé) — code `E04_FACTURE_DEJA_MONO_CHARGE`. Route
`POST /factures/{opaque}/lignes` et UI (carte « Lignes de facture », formulaire d'ajout,
alerte non bloquante si le total des lignes diverge du montant TTC).

18 tests ajoutés (`test_factures.py` : 10 service ; `test_factures_routes.py` : 1 HTTP ; correction
de `test_sqlite_migrations.py` pour inclure `facture_lignes` dans les tables attendues).

**Suite complète rejouée intégralement après ce changement** (4 tranches, par sous-lots de fichiers
pour éviter les coupures de l'environnement d'exécution en tâche de fond) : tous les tests passent,
seul le même échec pré-existant documenté (`test_appsec1_diagnostic`) subsiste. Aucune régression.

Détail : `44_MODELE_FACTURES_CHARGES_REGLEMENTS.md`, `48_ROADMAP_RESTANTE_PROJET.md`.

## ✅ Mission 4 de ce tour — `ANO-2026-07-28-01` corrigée : plus de données réelles en dur dans lot6b/lot6c

Choix utilisateur explicite (« Choix A ») : ne pas reconstruire APP-5C (déjà livrée à l'identique
du brief reçu, devenu obsolète face à l'état réel du worktree), reprendre la roadmap réelle.
Priorité imposée : corriger complètement cette anomalie avant tout autre travail.

**lot6b_m04_menages_internes.py** : `INTMAP` (dict figé, prénoms réels d'intervenantes) supprimé.
Mapping reconstruit dynamiquement depuis `REF_Intervenants.nom_normalise` (D104) — un référentiel
fictif définit ses propres `nom_normalise`, donc un jeu de recette fictif traverse désormais le
moteur sans aucun `intervenant_id` nul. L'alias réel documenté (D104, deux prénoms d'intervenante
liés au même identifiant) est externalisé dans `02_TRAVAIL/_data_lot6b_alias_reel.py`, module
optionnel jamais copié vers
`data_recette`.

**lot6c_menages_externes.py** : `RAW_MANUEL` (transcription de factures réelles mai 2026),
`PREST_MAP`, `LOG_MAP`, `PREST_BRUT`, ainsi que les mappings pcode/mode-paiement basés sur des
`intervenant_id` réels codés en dur, externalisés dans `02_TRAVAIL/_data_lot6c_secours_reel.py`.
Repli fictif local (2 lignes, identifiants `INT_B`/`LOG_A1`/`LOG_B1` alignés sur
`build_data_recette.py`) si ce module est absent. Textes du README généré (section SOURCES,
décision D086) rendus conditionnels au mode réellement actif.

`recette/build_data_recette.py` exclut désormais explicitement ces deux modules de la copie des
scripts moteur vers `data_recette/02_TRAVAIL` (`EXCLUS_DONNEES_REELLES`).

**Preuves** : compatibilité historique lot6b (3/3 clés réelles résolvent au même `intervenant_id`
qu'avant) ; compatibilité historique lot6c (rerun réel : 13 lignes, 12 VALIDE/1 BLOQUANT,
réconciliation exacte 1439€/942€, identique à l'ancien script — le seul écart constaté sur la ligne
T.2-65/Gabriel est **préexistant**, reproduit à l'identique avec l'ancien script non modifié, donc
sans lien avec ce refactor) ; recette entièrement fictive (lot6b + lot6c exécutés sur
`data_recette`, module réel absent, `ImportError` attendue, zéro `intervenant_id` nul, zéro nom
réel dans l'export généré — scanné) ; non-régression (`test_menages*.py` +
`test_charges_pipeline.py` : 148 passed, 22 skipped, 0 échec).

Détail complet : `JOURNAL_ANOMALIES.md` (`ANO-2026-07-28-01`), `41_MODULE_MENAGES_ETAT_FINAL.md`
§7bis, `48_ROADMAP_RESTANTE_PROJET.md`.

**Gap restant, distinct de cette anomalie** : la ventilation des pools de courses sur données
fictives nécessite encore une source de déclarations internes fictive dont les noms
d'appartements se mappent sur le parc fictif (`REF_Mapping_Logements` n'a que des `listingMapId`
aujourd'hui) — non traité ce tour, hors périmètre de la priorité demandée.

## ✅ Mission 3 de ce tour — premier socle Comptabilité construit et prouvé (docs `43`, `45`, `46`, `47`)

Migration `0021` : `plan_comptable` (seed provisoire 401000/411000/512000/606000), `ecritures`,
`ecriture_lignes`, `ecriture_evenements`. Service `comptabilite_ecritures_service.py` : deux
journaux câblés (**ACHATS**, **BANQUE** — les seuls exigés par la verticale de recette du brief),
équilibre débit/crédit imposé en code, idempotence par index unique
`(journal, origine_type, origine_id_opaque)`, contrepassation en écriture miroir (jamais de
suppression), double verrou `COMPTABILITE_REAL_WRITE_*`.

**Défaut trouvé et corrigé en écrivant les tests** : `solde_compte`/`solde_auxiliaire` excluaient
les écritures `CONTREPASSEE`, ce qui empêchait le solde de revenir à zéro après un avoir (le miroir
compensait une ligne qui n'était plus comptée). Corrigé : `VALIDEE` et `CONTREPASSEE` comptent
toutes deux, seule `PROPOSEE` reste exclue.

**Recette navigateur réelle** (port 8080, sur une vraie facture du jeu de recette,
`FA-MEN-2026-06`) : génération de l'écriture ACHATS (120,00 € équilibrés) → validation → solde
fournisseur −120,00 € → contrepassation (avoir) → solde revenu à **0,00 €** → redémarrage du
serveur → écriture toujours `CONTREPASSEE` (persistance) → régénération deux fois de suite →
**même** `ecriture_id_opaque` (idempotence).

37 tests ajoutés (15 service + 9 routes + migrations/flags). Suite complète : 2110 passés.

**Décision explicite, non entreprise ce tour** : le circuit propriétaire (lot12, Excel) n'est pas
migré vers SQLite (doc `44`) ; le plan comptable détaillé et le mapping catégorie→compte ne sont
pas arbitrés (doc `46`, `606000` générique par défaut) ; l'analytique est préparé (colonnes) mais
non exploité (doc `47`) ; aucun écran Résultats, aucune clôture comptable, aucun contrôle TVA.

## ✅ Mission 2 de ce tour — audit Factures/Charges/Règlements (doc `42`, `44`)

Audit systématique des 9 incohérences citées par le brief : **une seule confirmée** — facture
fournisseur (SQLite) et facture propriétaire (Excel, lot12) n'ont aucun modèle commun. Ce n'est pas
un bug, c'est une absence de modèle, sans conséquence tant qu'aucune comptabilité applicative
n'existait. Les 13 contrôles demandés étaient déjà couverts à 9/13, 3/13 structurellement
impossibles (index uniques), 1/13 hors périmètre SQLite (créance propriétaire).

**Décision explicite** : le circuit propriétaire n'est **pas** migré vers SQLite (lot12 reste seul
maître). Correction additive : migration `0020`, table `facture_classification` (`sens`,
`type_facture`), trigger auto-peuplé, rétro-classification. Aucun code de service modifié. 8 tests.

## ✅ Mission 1 de ce tour — cycle de vie Ménages construit et prouvé

Modèle SQLite (migration `0019`, patron de `factures_service.py`), service `menages_cycle_service.py`
(11 statuts, transitions du brief, qualification prestataire, historique jamais réécrit),
`menages_controles_service.py` (11 codes), 11 routes + 4 écrans sous `/menages/cycle`.

**Défaut de confidentialité corrigé en premier** (`ANO-2026-07-27-01`) : `lot6b`, lancé directement
depuis `/calculs` au tour précédent, interrogeait réellement la feuille Google des déclarations
internes et avait fait entrer de vraies données (prénoms d'intervenantes) dans `data_recette`.
Corrigé par `Lot.exige_workspace_controle` : refus avant tout lancement, chaîne retirée de
`/calculs`. `data_recette` purgé et vérifié vierge.

**Correction de mon propre diagnostic précédent** : « lot6d ECHEC » n'était pas un défaut de lot6d
ni du jeu de recette — c'était une conséquence de l'exécution illégitime ci-dessus (M04 pollué,
`logement_id` nul partout). Prouvé : via l'orchestrateur légitime, la chaîne complète passe 7/7,
`reel_intact=True`.

**Recette navigateur réelle** : cycle complet `PREVU → A_REALISER → REALISE → A_CONTROLER → VALIDE
→ FACTURE → REGLE` sur un ménage fictif, contrôles déclenchés en conditions réelles (pas seulement
en test), rattachement d'une facture existante avec contexte règlement affiché, second ménage
annulé, **persistance prouvée après redémarrage du serveur**.

82 tests ajoutés. Détail complet : doc `41`.

## ✅ Phase 2 — chaîne Charges exercée (doc `39`)

Deux défauts trouvés **en exécutant**, pas en relisant :

1. **La chaîne `charges` ne pouvait pas fonctionner.** `lot3_generateur_charges.py` est une
   bibliothèque (aucun `__main__`) : lancé comme script il rend `EXIT=0` sans rien produire. Le
   garde-fou « jamais de faux succès » le rattrapait, mais la chaîne restait inutilisable.
   Corrigé par `Lot.runner` → `charges_post_write_runner.py`, l'orchestrateur **déjà existant**.
   Point critique : ce runner rend **toujours 0** et porte l'échec dans son JSON ; `_verdict_runner`
   le lit, sans quoi une étape en échec passerait pour un succès.
2. **Double comptage de 8,90 €** : le frais bancaire était à la fois charge manuelle et flux
   bancaire injecté par Lot9. **Préexistant, non détecté.** Charge retirée + contrôle ajouté.

Le MASTER charges n'est plus seedé à la main (seconde vérité) : les charges vivent dans la SAISIE,
et Lot3 produit lui-même le MASTER.

Réconciliation : REEL 558,90 = COMPTABLE 493,90 + HC 65,00. Net propriétaire **inchangé**
(scénario A), somme à payer **+150,00 exactement** (scénario B). Idempotence et rollback prouvés.

## Phase 3 — Ménages : audit fait, chaîne partiellement exercée (doc `40`)

**Le module Ménages existe déjà** : 3 services, 13 routes, 7 écrans, 6 fichiers de tests, 2 exports
Power BI. Ce n'est pas une construction mais une **extension**. Ce qui manque est le **cycle de vie
opérationnel** (statuts, création hors Hostaway, affectation d'un prestataire, rattachements
facture/charge/règlement/banque, prestataires qualifiés, catalogue de contrôles) — un chantier
comparable à Factures.

Trois corrections livrées, chacune trouvée en exerçant :

1. `MENAGES_REAL_RECALC_ENABLED` était le **dernier verrou codé en dur** → aligné sur le double
   verrou. Reste False ; mode réel non activé.
2. Les lots Ménages ne déclaraient **aucune sortie** : la garantie « jamais de faux succès » ne
   s'appliquait donc **pas** à eux. Sorties déclarées + test de non-régression.
3. `lot6c` **manquait** dans la chaîne du pilotage alors que lot6d consomme sa sortie. Ajouté ; le
   test d'ordre compare désormais à `menages_chaine_service.STEPS_CHAINE` au lieu d'une liste
   recopiée.

État réel : **lot6b ✅, lot6c ✅, lot6d ⛔**. Le premier blocage (source Hostaway absente) est levé
par `build_menages_hostaway()`. Le second ne l'est pas :

```
lot6d_rapprochement_menages.py:220
TypeError: '<' not supported between instances of 'NoneType' and 'str'
```

Une des sources agrégées porte un `logement_id` ou `proprietaire_id` nul.

## ✅ Ménages — chaîne verte, cycle de vie non construit (doc `41`)

`lot6b → lot6c → lot6d → lot6e → lot6f → lot11` + étape Hostaway : **7/7 OK**, `SUCCES`,
`reel_intact = True`. Lancement par **`/menages/chaine`** uniquement.

**Trois corrections de mes propres affirmations précédentes :**

1. **lot6d n'a jamais eu de défaut.** Le `TypeError` venait d'un M04 pollué par une exécution
   illégitime (voir ci-dessous), pas du jeu de recette.
2. **Le pivot du coût interne était conforme** à D101, pas en dérive.
3. **La règle « cave 50 € » existe** : je l'avais déclarée introuvable après une recherche limitée
   à `lib_menage_costs`. Elle est dans `REC_002` + D103 + `lot6f`.

**Défaut de confidentialité corrigé (ANO-2026-07-27-01)** : `lot6b` interroge réellement la feuille
Google des déclarations internes. Exécuté directement via `/calculs`, il a fait entrer de vraies
données (prénoms d'intervenantes) dans `data_recette`. Rien n'a été écrit côté réel ni committé.
`Lot.exige_workspace_controle` + refus dans `executer_lot` + retrait de `/calculs`.

**Verrou périmé** : `recuperer_verrou_perime()` écarte par renommage atomique un verrou dont le PID
est mort, jamais celui d'un processus vivant ; journalisé sans le nom de machine.

**Cave (REC_002)** : forfait mensuel de `REF_Charges_Recurrentes`, ventilé au prorata du **coût
standard** (D103, révise D045) sur les **seuls ménages internes**. Ce n'est pas « +50 € par
ménage ». **Aucun arbitrage nécessaire** — le grain est documenté.

## ✅ FAIT VALIDÉ — pivot du coût de ménage interne (D101)

Tranché : **D101 reste la règle.** Ce n'est plus une anomalie ouverte.

Une consigne du 2026-07-27 avait demandé de déplacer le pivot au 1er mai 2026, au motif d'une
dérive du moteur. Vérification faite : **le moteur était conforme**, pas en dérive.

`DECISIONS_METIER.md` → **D101 — Méthode interne selon période**, VALIDÉ le 2026-06-18 :

> Pivot **2026-06**. **Jusqu'à mai 2026 inclus** : `INTERNE_HEURES_M04` = nb_heures × taux horaire
> (PARAM_004). **À compter de juin 2026** : `INTERNE_STANDARD_PARAMETRE` = nb_menages × forfait
> `REF_Couts_Menage_Interne`.

`lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01` applique exactement D101. **Le pivot n'a pas été
modifié** — l'avancer au 1er mai aurait recalculé mai 2026, le mois qui porte les données réelles
(factures et heures de deux prestataires/intervenantes externes réels), avec l'autre méthode.

Comportement verrouillé par `tests/test_menages_pivot_historique.py` (9 tests, dont les 4
frontières 30/04, 01/05, 31/05, 01/06 — cf. Mission 1 §6 de ce tour pour la ré-vérification
demandée).

Tant que ce n'est pas tranché, aucun écran ni calcul ne doit présenter une règle contredisant D101.

## Mission d'intégration : parties bornées livrées, mission NON terminée

Livré : `MATRICE_ETAT_MODULES.md` (13 modules classés avec preuve — 8 TERMINÉ, 4 PARTIEL, mode réel
à l'arrêt partout), audit des verrous (`test_flags_inventaire.py`, 28 tests),
`GUIDE_ACTIVATION_MODE_REEL.md` avec checklist GO/NO GO.

**Non fait** : recette sur copies contrôlées des données réelles (§16), mesures de performance
(§18), revue de sécurité complète (§19), `RAPPORT_RECETTE_GLOBALE.md`.

**Correction d'une affirmation précédente** : `MENAGES_REAL_RECALC_ENABLED` n'était pas « la
dernière garde codée en dur ». Cinq gardes **gelées** subsistent (`HH_*` ×2,
`REF_ASSOC_MODE_REAL_WRITE_ENABLED`, `CONTROLES_*` ×2). Ce n'est pas une dérive : les geler est plus
sûr que de leur ouvrir un chemin d'activation. Les deux catégories sont désormais figées par tests.

## Prochaine action précise

**⛔ Un arbitrage bloque le premier point de la roadmap.**

1. **TRANCHER : données réelles en dur dans les moteurs ménages.** `lot6b.INTMAP` (prénoms
   d'intervenantes) et `lot6c` (références de factures, noms de prestataires) sont codés dans le
   source. Conséquence : **aucun jeu de recette fictif ne peut traverser la chaîne ménages** — un
   prénom fictif donne `intervenant_id = None` et lot6d échoue sur `TypeError: NoneType < str`,
   et utiliser les vrais prénoms réinjecterait de la PII en recette (défaut déjà corrigé,
   `ANO-2026-07-27-01`). Trois issues dans `41` §7bis :
   **A** externaliser vers les référentiels (modification de moteur) ·
   **B** n'exercer la chaîne que sur l'arbre réel en copies (aujourd'hui 7/7, `reel_intact=True`) ·
   **C** impossible en l'état (`INTMAP` est indexé par prénom, pas par identifiant).
   Tant que ce n'est pas tranché : pools de courses, ventilation REC_002 et chaîne Ménage complète
   restent non exerçables.
2. **Puis** le reste de la roadmap `48` : compléter la facturation → compléter la Comptabilité
   (VENTES/CAISSE/OD, auxiliaires, périodes, clôture) → analytique → Résultats → réconciliations.
3. Le **mode réel** reste à activer sur décision explicite — garde-fous en place, jamais activés.
4. Non construit, signalé : « charge postérieure à une clôture validée » n'est interdit par aucun
   mécanisme applicatif.

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current          # feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"        # docs(handoff): ... ; le commit precedent est 7775097
git status --porcelain             # doit être vide

# Régénérer le jeu de recette (idempotent, écrase data_recette/) :
python recette/build_data_recette.py

# Tests ciblés :
cd 05_APPLICATION
python -m pytest -q tests/ -k "banque"                        # 171 passés
python -m pytest -q tests/ -k "facture or reglement"          # 120 passés
python -m pytest -q tests/ -k "calculs or lot13"               # exécuteur, pipeline, routes, contrat lot13
python -m pytest -q tests/test_charges_pipeline.py             # 19 passés (chaîne Charges)
python -m pytest -q tests/test_recette_scenarios.py            # scénarios charges bout en bout

# Reprendre le blocage lot6d (prochaine action n°1) :
cd 05_APPLICATION
APP_DATA_DIR="<wt>/data_recette/app_data" PROJECT_ROOT="<wt>/data_recette" RECETTE_MODE=1 \
  LOT4A_ENGINE_PYTHON="C:\Program Files\Python312\python.exe" \
  python -c "from app.services import calculs_executeur_service as ex; \
r=ex.executer_lot('lot6d'); print(r.statut, r.message); print(r.stderr[-800:])"
python -m pytest -q                                            # suite complète (~32 min)

# Lancer un lot moteur à la main (interpréteur AVEC pandas) :
PROJECT_ROOT="<worktree>/data_recette" PYTHONIOENCODING=utf-8 \
  "C:/Program Files/Python312/python.exe" data_recette/02_TRAVAIL/lot9_construire_flux.py

# Serveur de recette complet (APP_DATA_DIR isole aussi la base SQLite ; LOT4A_ENGINE_PYTHON donne
# l'interpréteur des lots au pilotage des calculs) :
cd 05_APPLICATION
APP_DATA_DIR="<worktree>/data_recette/app_data" \
  PROJECT_ROOT="<worktree>/data_recette" RECETTE_MODE=1 \
  LOT4A_ENGINE_PYTHON="C:\Program Files\Python312\python.exe" \
  BANQUE_REAL_WRITE_ENABLED=1 BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  CHARGES_REAL_WRITE_ENABLED=1 CHARGES_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  FACTURES_REAL_WRITE_ENABLED=1 FACTURES_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  python -m uvicorn app.main:app --port 8020 --host 127.0.0.1
```

## Décisions structurantes du chantier

1. **Une seule norme bancaire** : l'import applicatif reproduit exactement le schéma et les formules
   de `lot8a` (26 colonnes, `ROW_HASH`, `mouvement_id`).
2. **Un seul moteur de rapprochement** : `banques_rapprochement_service`. Factures/règlements y
   accèdent via un *pont* (`factures_banque_service`), jamais par un second moteur.
3. **Excel reste la vérité métier ; SQLite journalise.** Aucune anomalie moteur masquée ni réécrite.
4. **Aucune validation silencieuse** : une suggestion, même EXACT, produit un statut `PROPOSE`.
5. **Statuts dérivés recalculés dans les deux sens** (leçon d'un bug réel : une facture restait
   REGLEE avec un solde non nul après annulation d'un règlement).
6. **Jamais de faux succès de pipeline** : un lot n'est SUCCES que si code retour 0 **et** sorties
   présentes ; un pipeline partiel est un échec.
7. **Le besoin de pandas est une propriété du lot**, pas du pilotage.
8. **Double verrou d'écriture partout** : `RECETTE_MODE` + variable dédiée + write-guard de chemin.
9. **Quatre objets distincts** : facture (dette) / charge (impact économique) / règlement (paiement)
   / rapprochement (lien bancaire). Un rapprochement ne crée jamais de charge.
10. **Réutiliser les orchestrateurs existants** : l'ordre des lots vient de
    `run_regression_pipeline.py` et `run_menages_pipeline.py`, jamais réinventé.
11. **Seeder des entrées, jamais des sorties de lot.** Seeder une sortie court-circuite le moteur
    et fabrique un faux succès. Corollaire : un jeu de recette se rend *conforme aux contrôles*,
    on ne contourne jamais un contrôle.
12. **Un défaut moteur se documente, il ne se contourne pas.** Le défaut lot13 est tenu par un test
    `xfail(strict=True)` côté application ; ni le moteur réel ni sa copie de recette n'ont été
    retouchés.
13. **Un nom de colonne d'indicateur se relève sur la sortie réelle, jamais par déduction.** Une
    colonne mal nommée ne lève aucune erreur : elle rend la valeur silencieusement absente. Un test
    compare désormais la déclaration aux en-têtes réels.

## Scope explicite : `ETAT_AVANCEMENT.md` / `ARCHITECTURE_DONNEES.md` (00_CADRAGE racine)

Décision réaffirmée explicitement ce tour (2026-08-02), déjà jugée ainsi lors de missions
antérieures : ces deux documents appartiennent à une couche de documentation historique/legacy du
projet, antérieure au chantier `BANQUE_LOGEMENTS_PDF_CHARGES_METIER_20260724`. Ils restent **hors
périmètre** de ce chantier et ne sont ni mis à jour ni remplacés par les documents `54`-`72` — pas
un oubli, une décision de scope assumée pour ne pas dupliquer/écraser une documentation dont la
structure n'est pas celle de ce chantier.

## Validation finale Banque/Trésorerie (2026-08-07)

Mission de validation dédiée : recette navigateur complète, pipeline Lot8a→Lot13 exécuté deux fois
sur copies (idempotence prouvée), rollback exercé, campagne complète (2500 tests, 0 échec nouveau,
1 échec pré-existant reproduit et confirmé antérieur), intégrité avant/après vérifiée (85/88
identiques, 3 écarts tous ATTENDUS). Aucun bug applicatif trouvé — aucune correction de code
nécessaire. Détail complet : `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`. Verdict séparé : Bloc
Banque/Trésorerie **VALIDÉ SUR COPIES** ; Airbnb et Mode réel **NO GO** (inchangés).

## Correction de cadrage métier — versements plateformes ↔ réservations (2026-08-08)

**Règle métier définitive** : un virement entrant de plateforme (Airbnb ou autre) n'est jamais
rapproché d'une réservation individuelle (montant agrégé, versements groupés, plusieurs
plateformes — aucune correspondance fiable montant↔réservation). La Banque catégorise l'origine du
flux (catégorie moteur déterministe `PAYOUT_PLATEFORME`) ; les réservations restent gérées
indépendamment (Hostaway API ou saisie manuelle hors Hostaway).

**Fausse logique supprimée** : `banques_candidats_service._reservations()` générait des candidats
`RESERVATION` pour tout mouvement CREDIT et les exposait à exact/partiel/groupé — supprimé
entièrement (fonction + câblage dans `candidats_pour()`/`compter_sources()`). Exact/partiel/groupé
restent pleinement fonctionnels pour charges (`_charges()`) et trésorerie propriétaires
(`_reversements_proprietaires()`) — aucune régression sur ces objets.

**UI Airbnb corrigée** : `SOURCE_AIRBNB_DETAILLEE_ABSENTE` (alerte bloquante impliquant qu'un export
serait nécessaire pour rapprocher à une réservation) remplacée par `categorisation_versements_
airbnb()` / bloc `VERSEMENTS PLATEFORMES` — 166 mouvements catégorisés `PAYOUT_PLATEFORME`,
14 467,27 €, aucun export requis, aucune tentative de rattachement à une réservation.

**Doublon 83 lignes/82 mouvements** : `banques_classement_service.lister()`/`compter()` ne
dédoublonnaient pas par `mouvement_id` — le mouvement dupliqué en amont (ligne_source 149/151)
apparaissait deux fois dans la file A_ENVOYER_IA sous le même `id_opaque`. Corrigé (déduplication
par `mouvement_id`, ligne source non modifiée, 4 tests). `compter()` retourne désormais 82, pas 83.

**56 propriétaires** : inchangé, toujours en attente (56 virements, 27 069,18 €, 0 confirmé),
hors périmètre de cette correction (objet de rapprochement légitime, `REVERSEMENT_PROPRIETAIRE`).

Détail complet, tests, régression : `76_VALIDATION_FINALE_BANQUE_TRESORERIE.md` (section G).
`74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md` marqué **SUPERCÉDÉ** en tête de document. Commit
ciblé : "Correction cadrage Banque - categorisation versements plateformes".

Reste à faire (prochaine action) : les 83 A_ENVOYER_IA (classification humaine progressive, hors
mandat de cette correction — mission antérieure interrompue à 10/82 lignes présentées, non
reprise ici) et les 56 propriétaires (identité univoque/ambiguë, objet trésorerie existant/absent)
restent à traiter progressivement dans l'application, comme prévu. Checklist GO/NO-GO mode réel
(`72`) toujours à signer par l'utilisateur.

## Audit ciblé Comptabilité (2026-08-08) — aucun code modifié

Mission « finaliser le bloc Comptabilité ». Audit confirme que le cœur Comptabilité est **déjà
construit et testé** par des missions antérieures (`43`/`45`/`46`/`47`/`49`/`50`) : migrations
`0021`-`0024`, 8 services (`comptabilite_ecritures_service.py` — 7 générateurs ACHATS/BANQUE/
AVOIR/VENTES/CAISSE×2/OD —, `comptabilite_mappings_service.py`, `comptabilite_auxiliaires_service.py`,
`comptabilite_periodes_service.py`, `comptabilite_controles_service.py`,
`comptabilite_analytique_service.py`, `comptabilite_axes_service.py`,
`comptabilite_reconciliations_service.py`), 32 routes, 15 templates, équilibre imposé en code,
idempotence, périodes/clôture avec réouverture tracée, 15 contrôles (4 niveaux). Régression
ciblée `-k comptabilite` ce tour : **142 passés / 1 ignoré / 0 échec** — aucune régression après la
correction Banque du tour précédent (`78877da`).

**Écarts confirmés, tous déjà documentés (`70_MATRICE_ARBITRAGES_COMPTABLES.md`, inchangée et
toujours exacte)**, aucun nouveau code écrit pour ne pas inventer une règle comptable :
- `606000` générique (achats fournisseurs) — mécanisme de résolution fonctionnel, aucune règle
  `VALIDE` arbitrée par catégorie de charge ;
- VENTES — adaptateur Lot12 provisoire (`SOURCE_PROVISOIRE_LOT12`), jamais recalculé ;
- Frais bancaires (`TYPE_FLUX_016`, 24 lignes Lot9) — aucun générateur d'écriture Comptabilité
  dédié, compte cible (ex. `627000`) non arbitré ;
- TVA — non traitée par aucun chantier, nécessite un avis fiscal, hors périmètre technique ;
- **Trésorerie propriétaires** (migration `0025`, natures ACOMPTE/REMBOURSEMENT/REGULARISATION/
  COMPENSATION/AVANCE/RESTITUTION/AUTRE_A_CONTROLER) — **aucun générateur d'écriture comptable
  n'existe** pour cet objet (absent de `comptabilite_ecritures_service.py`), et aucun compte n'est
  documenté nulle part (`DECISIONS_METIER.md`, `ARCHITECTURE_DONNEES.md` : recherche directe, 0
  résultat) — **A_ARBITRER**, décision utilisateur requise avant tout code ;
- Dépenses personnelles associés / IK / gestes commerciaux (identifiés dans `73_JOURNAL_DECISIONS_
  VALIDATION_HUMAINE.md`, groupes Banque 1-5) — classification métier faite, mapping comptable
  explicitement laissé `A_CONTROLER` par l'utilisateur à l'époque, toujours non tranché.

**Documents métier externes cités par la mission** (« Système compta et réservations.txt », « Résumé
règles métier.txt », « Application gestion acteurs factures.txt », « Fusion de prompts.txt »)
introuvables dans le dépôt (`find` sur tout le worktree, 0 résultat) — probablement des fichiers
locaux à l'utilisateur, jamais versés au repo. Aucune tentative de deviner leur contenu.

Décision de cette mission : ne construire aucun lien comptable pour les objets dont le traitement
n'est pas documenté (règle absolue « ne rien inventer »), plutôt que de fabriquer un mapping
compte/nature non validé. Le cœur déjà livré reste la source de vérité ; rien n'a été rouvert côté
Banque (règles de cadrage du tour précédent non retouchées).

## Fermeture du cadrage comptable + checklist recette globale (2026-08-08, suite)

`70_MATRICE_ARBITRAGES_COMPTABLES.md` reconstruite exhaustivement (27 catégories `CHG_XXX`, 17
catégories bancaires, 7 natures trésorerie propriétaires, IK, dépenses personnelles associés,
gestes commerciaux, refacturations, avoirs, ventes). Constat central, vérifié directement dans les
migrations SQL : **7 comptes existent au total** dans le plan comptable applicatif (`401000`,
`411000`, `512000`, `530000`, `467000`, `606000`, `706000`) ; aucune règle `mapping_comptable_
regles` `VALIDE` n'est seedée pour aucune catégorie ou type de flux — seul le filet générique
`606000` PROVISOIRE existe. Deux comptes déjà existants portent un usage documenté en commentaire
de schéma, jamais exploité : `467000` (« avances/dépenses personnelles/remboursements » —
associés) et `411000` (« créance/compensation » — propriétaires, trésorerie propriétaires). Aucun
numéro de compte n'a été choisi ou inventé par cette mission — chaque écart reste `PROVISOIRE` ou
`A_ARBITRER`, jamais transformé silencieusement en `VALIDE`. Aucun code modifié (documentation
uniquement).

**Le cœur Comptabilité est techniquement fonctionnel. Les comptes/mappings non validés restent
PROVISOIRES/A_ARBITRER et ne doivent pas empêcher la recette fonctionnelle des autres modules.**

État des modules pour préparation de la recette fonctionnelle globale (technique/tests connus par
cette session et les précédentes, aucune recette relancée ce tour) :

| Module | Technique | Tests | Recette sur copies | Validation humaine |
|---|---|---|---|---|
| Logements | OK | OK | Faite (`28`/`29`) | NON_TESTE |
| Réservations | OK (lecture seule assumée) | OK | Faite (lecture) | NON_TESTE |
| Propriétaires | A_VALIDER | OK | Ancienne, non rejouée récemment | NON_TESTE |
| Ménages | A_VALIDER (pools de courses non alimentés) | OK | Partielle (cycle prouvé) | NON_TESTE |
| Charges | OK (hors pools) | OK | Faite | NON_TESTE |
| Fournisseurs | OK | OK | Faite | NON_TESTE |
| Factures | A_VALIDER (factures propriétaires émises absentes) | OK | Faite (fournisseurs) | NON_TESTE |
| Règlements | OK | OK | Faite | NON_TESTE |
| Banque | OK (cadrage corrigé 2026-08-08) | OK (612+ passés) | **VALIDÉ SUR COPIES** | Partielle (groupes 1-5 décidés) |
| Trésorerie propriétaires | OK (module) / A_ARBITRER (pont comptable) | OK | Faite | NON_TESTE |
| Comptabilité | OK (cœur) / PROVISOIRE (mappings) | OK (142 passés) | Faite (`49`) | NON_TESTE |
| Analytique | OK (périmètre défini) | OK | Faite | NON_TESTE |
| Résultats | OK (périmètre défini) | OK | Faite | NON_TESTE |
| Contrôles | A_VALIDER (contrôles inter-lots restants) | OK | Partielle | NON_TESTE |
| Clôture | A_VALIDER (réconciliation clôture applicative ↔ clôture comptable) | OK | Faite (comptable) | NON_TESTE |
| Exports | OK (Power BI, 13 exports + dictionnaire) | OK | Faite | NON_TESTE |

Aucun module `BLOQUANT`. Le point commun à tous : aucune **validation humaine** globale n'a
encore eu lieu (seule la Banque a une validation humaine partielle, groupes 1-5). C'est la
prochaine étape naturelle du projet, pas un nouveau développement.

## Recette fonctionnelle globale (2026-08-08, suite) — aucun code modifié

Smoke HTTP réel sur les 18 modules (instance isolée port 8030, copies, `RECETTE_MODE=1`,
`APP_DATA_DIR` isolé, port 8000/PID 21136 jamais touché) : **18/18 écrans principaux + 8
sous-écrans Comptabilité répondent 200, aucun 404/500**. Parcours navigateur réel approfondi sur
Réservations hors Hostaway (seul module explicitement signalé à corriger si non fonctionnel) :
résolution automatique propriétaire (logement+date → PROP_0001), taux commission (18 %, source
« taux propriétaire »), prix ménage standard (29 €, source `REF_Couts_Standards_Menage`) — tous
confirmés fonctionnels en direct contre les données copiées. Écriture finale non poussée jusqu'au
bout en navigateur (le formulaire ouvre une modale de confirmation JS avant le POST — non forcée,
écriture déjà prouvée par `test_reservations_hh.py`, vert cette session). **Aucun bug trouvé,
aucune correction nécessaire.** TVA : l'utilisateur confirme qu'aucune TVA n'est applicable
actuellement — information enregistrée dans `70_MATRICE_ARBITRAGES_COMPTABLES.md`, aucune règle
fiscale automatisée construite. Détail complet, fiche de validation vierge, verdict par module :
`RECETTE_FONCTIONNELLE_GLOBALE.md`.

**VERDICT TECHNIQUE : PRÊT POUR VALIDATION HUMAINE GLOBALE** (pas une activation de mode réel —
la fiche de validation attend les réponses de l'utilisateur, module par module).

## Validation humaine globale — en cours (2026-08-10)

Recette par lots, décisions utilisateur explicites, jamais déduites. **LOT A clos** : Tableau de
bord/Navigation ACCEPTE_AVEC_RESERVE (widget « Modules » accueil obsolète, cosmétique), Logements
ACCEPTE_AVEC_RESERVE (sections PBI « Non renseigné » tant que pipeline non rafraîchi, déjà
explicité en UI), Propriétaires ACCEPTE, Réservations (Hostaway + hors Hostaway)
ACCEPTE_AVEC_RESERVE (aucun écran dédié de parcours des réservations Hostaway — réserve
fonctionnelle, pas un défaut de calcul). Aucune correction demandée par l'utilisateur (réserves
toutes non bloquantes, corrections explicitement refusées pour l'instant). Détail : `RECETTE_
FONCTIONNELLE_GLOBALE.md` section I. **LOT B exercé (2026-08-10), en attente de décision
utilisateur** : recette réelle sur instance isolée (port 8041, écritures fictives). Chaîne
Fournisseur→Facture→Règlements×2 entièrement exercée en écriture réelle (persistée, isolée) :
création fournisseur, facture 120,00 €, deux règlements (50,00 € puis 70,00 €), statut dérivé
automatiquement A_CONTROLER→PARTIELLEMENT_REGLEE→REGLEE, solde à 0, historique complet, aucune
recréation de facture. Ménages : drill-down réel rapprochement→facture→coûts confirmé, sources
jamais fusionnées. Charges : prévisualisation réelle correcte, écriture finale non poussée en
direct (modale JS, couverte par tests automatisés verts). **Aucun bug trouvé dans le LOT B.**

**LOT B clos (2026-08-10)** — décisions utilisateur : Ménages ACCEPTE, Fournisseurs ACCEPTE,
Charges ACCEPTE_AVEC_RESERVE (réserve non bloquante, non corrigée sur demande explicite), Factures
ACCEPTE, Règlements ACCEPTE. **LOT C exercé (2026-08-10), en attente de décision utilisateur** :
Banque/Caisse — 82 mouvements confirmés dans la file (dédoublonnage toujours actif), catégorisation
PAYOUT_PLATEFORME conforme au cadrage corrigé (aucun export manquant, aucun rattachement
réservation), décision humaine réelle exercée bout en bout (liste→détail→confirmation→historique).
Caisse : ABSENT (aucun mouvement dans cette copie, état déjà connu). Trésorerie propriétaires :
parcours réel création→BROUILLON→VALIDE (immuabilité confirmée)→ANNULE→historique, tous corrects.
**Aucun bug trouvé dans le LOT C.**

## LOT C signé + LOT D/E exercés (2026-08-10) — aucun code modifié

**LOT C — décisions utilisateur** : Banque/Caisse ACCEPTE_AVEC_RESERVE (réserve : Caisse non
validable, aucun mouvement disponible sur la copie — pas un défaut Banque, aucune donnée métier
fabriquée pour lever la réserve), Trésorerie propriétaires ACCEPTE.

**LOT D/E exercés en profondeur** (instance isolée port 8050, écritures fictives, baseline
d'intégrité 950 fichiers prise avant) :
- **Comptabilité** : chaîne E2E réelle fournisseur→facture 120 €→écriture ACHATS équilibrée
  (606000/401000, auxiliaire opaque)→validation→2 règlements→2 écritures CAISSE→OD validée
  (ODIVERSES). Déséquilibre 100/60 **refusé**. Période 2026-05 clôturée → écriture **refusée** ;
  réouverture sans justification **refusée**, avec justification acceptée. Mapping provisoire
  explicitement marqué (`A_CONTROLER` + `MAP-GENERIQUE-606000`, bandeau « Seed provisoire »).
- **Analytique/Résultats** : REEL 291 722,75 = COMPTABLE 281 198,59 + HC 10 524,16, écart 0,00 €.
  8 réconciliations, statuts honnêtes.
- **Contrôles** : 62 INFO comptés séparément des 2358 bloquants ; exception sans justification
  refusée, avec justification acceptée.
- **Calculs** : chaîne aval **6/6 SUCCES depuis l'interface** (74,4 s) — couverture nouvelle ;
  **idempotence** prouvée (2ᵉ run identique au centime) ; **rollback natif exercé** (8 fichiers
  restaurés).
- **Exports** : 6 CSV applicatifs + 13 Power BI, aucune PII (un motif suspect s'est révélé être un
  fragment d'ID opaque, vérifié).
- **Intégrité finale** : **950/950 fichiers réels identiques au baseline, 0 modification.**
- Régression : **591 passés / 37 ignorés / 0 échec**. **Aucun bug trouvé — aucune correction
  nécessaire.**

**Découverte structurante pour le mode réel** : tous les writers sont gatés par `RECETTE_MODE`
(`X_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag(...)`) et trois sont codés en dur à `False`
(HH, REF_ASSOC_MODE, CONTROLES). **Une instance réelle ne peut activer aucun writer par variable
d'environnement** — l'activation exige une modification revue de `app/config.py`. Protection par
conception, à connaître avant toute planification. Dossier complet : `PREPARATION_MODE_REEL.md`
(9 writers inventoriés, ordre d'activation en 7 étapes, backup/rollback, conditions GO/NO GO).

## Validation LOT D/E signée + clarification des bloqueurs de clôture (2026-08-10, suite)

**LOT D/E — décisions utilisateur enregistrées** : Comptabilité ACCEPTE_AVEC_RESERVE (mappings
`606000` / trésorerie / associés encore provisoires), Analytique ACCEPTE, Résultats ACCEPTE,
Contrôles/Clôture ACCEPTE **sous condition** de clarification, Calculs/Exports ACCEPTE.
**Les 16 modules ont désormais une décision humaine.**

**La condition a été instruite — et change le verdict.** Comptage exact sur l'export applicatif :
2420 lignes, sévérité BLOQUANT **959**, A_CONTROLER **1399**, INFO **62** ; **2357 lignes portent
`impact_cloture = "Bloque la clôture"`**. Mes rapports précédents écrivaient « 2358 bloquants » en
conflatant sévérité et effet : chiffre exact, lecture ambiguë, **corrigée dans tous les documents**.
Ces 2357 lignes se répartissent en **9 familles de codes** et sont des **lacunes de données métier**
(périodes de gestion absentes 838, réservations sans commission 612, guest count manquant 553,
lignes bancaires non classées 221, charges exceptionnelles mal rangées 121, 12 résiduelles) — pas
des défauts applicatifs. **Aucune n'a été résolue, masquée ni transformée en exception.**
**Conséquence : aucun mois n'est clôturable → préparation mode réel NO GO.**

**Audit des 9 writers — le contrat de sécurité demandé existe déjà.** Double garde
(`RECETTE_MODE and _env_flag(...)`), défaut fail-closed, second flag de confirmation, write-guard
de chemin indépendant (`app/recette_guard.py`, interdit en dur `01_SOURCES_BRUTES`/`02_TRAVAIL`/
`03_EXPORTS`), backup pré-écriture, prévisualisation + confirmation, historiques append-only,
rollback : **tout est en place et vérifié dans le code**. Construire un second mécanisme aurait
contrevenu à l'instruction « ne crée pas un troisième système de permission parallèle ».
**Aucune ligne de `app/config.py` n'a été modifiée.** Ce qui manque est l'état « écriture réelle »
(état C), délibérément jamais construit : le créer maintenant retirerait la protection qui garantit
qu'aucune écriture réelle accidentelle n'est possible, alors qu'aucune n'est encore souhaitable.
Les 3 hardcodes ont été analysés individuellement : HH et CONTROLES sont **prêts fonctionnellement
mais maintenus `False`** ; REF_ASSOC_MODE est **NON_ACTIVABLE**.

## GESTION_LOGEMENT_MISSING — diagnostic complet (2026-08-10, suite) — aucune donnée inventée

Détail complet : `77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md`.

**Le volume était trompeur : 838 lignes = 137 couples logement×mois distincts, sur 14 logements et
la seule année 2025** (ratio 6,12 — une ligne par réservation concernée).

**Cause unique et uniforme (137/137 en catégorie A, ABSENCE_REELLE_HISTORIQUE)** : les 17 lignes de
`REF_Gestion_Logements_Hist` ont **toutes** `date_debut = 2026-01-01`, toutes sourcées
« Confirmation opérateur 28/06/2026 ». Le référentiel est une **photographie de l'état au
01/01/2026**, il n'a jamais contenu d'historique antérieur.

**Aucun bug moteur — vérifié, pas supposé.** `lib_ref_history.applies_on()` : début inclusif, fin
inclusive, période ouverte si `date_fin` vide. Vérification empirique exécutée (6 cas de bornes,
tous conformes) et confirmation croisée par les données : aucun mois de 2026 n'apparaît parmi les
couples manquants. **0 des 838 lignes n'est imputable à un défaut de résolution.**

**Recherche de preuves historiques : résultat négatif.** Les 5 archives datées de `REF_Setup`
**ne contiennent pas l'onglet** (créé après) ; `REF_Logements` n'a aucune colonne de date. Bilan :
**PREUVE_A = 0, PREUVE_B = 0, AMBIGU = 0, ABSENT = 137.** Aucune reconstruction déterministe n'est
donc possible, aucun overlay n'a été construit, aucune simulation n'avait d'objet.

**Rien n'a été inventé** : ni date d'entrée en gestion, ni propriétaire historique. L'existence
d'une réservation en 2025 et l'identité du propriétaire actuel ont été explicitement écartées comme
preuves insuffisantes.

**Question posée à l'utilisateur** (une seule, le motif étant uniforme) : les 14 logements
étaient-ils (a) déjà en gestion en 2025 avec les mêmes propriétaires — auquel cas il faut la date
d'entrée réelle de chacun ; (b) en gestion avec d'autres propriétaires/dates ; ou (c) hors
périmètre de gestion en 2025 — auquel cas c'est une règle de périmètre à écrire, pas une donnée à
reconstruire.

## GESTION_LOGEMENT_MISSING — décision appliquée au réel (2026-08-10, suite)

Décision utilisateur reçue : couverture propriétaire (même `proprietaire_id` qu'au 01/01/2026)
prolongée jusqu'au **01/08/2025** pour les 14 logements concernés — date de couverture décidée,
pas affirmée comme date commerciale réelle. Janvier→juillet 2025 explicitement hors périmètre,
non reconstruits.

**Appliqué d'abord sur copie** : 14 lignes de `REF_Gestion_Logements_Hist` modifiées
(`date_debut` uniquement), contrôles structurels verts (0 chevauchement, 0 doublon, propriétaire/
statut/`date_fin` inchangés, 3 autres logements intacts). Mesure : **GESTION_LOGEMENT_MISSING
838→472 lignes, 137→77 couples**, les 77 restants tous en janvier-juillet 2025 sur les mêmes 14
logements. Effets aval : **REEL/COMPTABLE/HORS_COMPTA strictement identiques** (291 722,75 /
281 198,59 / 10 524,16, écart 0,00 €) — explicable : une seule ligne de gestion par logement,
aucune ambiguïté de propriétaire n'a jamais existé pour le calcul.

**Appliqué ensuite au référentiel réel**, dans l'ordre imposé : backup
(`99_ARCHIVES/LOT0_REF_Setup/REF_Setup_PRE_PROLONGATION_GESTION_20260810.xlsm`) → SHA256 vérifié
identique avant écriture → prévisualisation exacte du diff (seule `date_debut` de 14 lignes) →
écriture → relecture (14 lignes conformes, 3 autres logements intacts, 17 lignes au total comme
avant) → **intégrité globale : 1 seul fichier modifié sur 950 (`REF_Setup.xlsm`), exactement
celui attendu.**

**Recalcul réel non effectué, délibérément** : régénérer Lot9→Lot13 sur le réel exige le writer
Calculs (`CALCULS_REAL_RUN_ENABLED`), désactivé conformément au NO GO mode réel en vigueur. La
mesure 838→472/137→77 a été faite sur copies avec un diff strictement identique à celui appliqué
au réel — représentative, mais non recalculée sur le réel lui-même. *(Incident mineur autocorrigé :
une instance de lecture a été démarrée sans `PROJECT_ROOT` explicite puis arrêtée immédiatement,
aucun writer actif à aucun moment, aucune donnée touchée — cf. `JOURNAL_ANOMALIES.md`.)*

Cette famille (`GESTION_LOGEMENT_MISSING`) reste ouverte à **77 couples** (janvier→juillet 2025).
Les 8 autres familles bloquantes (1519 lignes) restent intactes — en particulier les 612 contrôles
de commission, référentiel **distinct** (taux), non traités ici.

## RESERVATION_A_CONTROLER_SANS_COMMISSION — constat : rien à construire (2026-08-10, suite)

Détail complet : `78_RECONSTRUCTION_HISTORIQUE_COMMISSIONS.md`.

**Gate baseline corrigé** : le total de contrôles après prolongation gestion est **2002**, vérifié
exact (pas une erreur). L'hypothèse « 2054 » supposait que seule `GESTION_LOGEMENT_MISSING` avait
varié ; en réalité `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` a aussi varié de −52 (121→69), effet
aval réel et explicable (ce contrôle dépend lui aussi de `REF_Gestion_Logements_Hist`). Tous les
autres codes strictement inchangés, vérifiés un à un.

**Audit des 612** : source directe (`MASTER_CALC_Commissions.xlsx`, onglet `A_CONTROLER`) — 612
réservations distinctes, causes **553 `GUEST_COUNT_MANQUANT`** + **59 `RESERVATION_EXCLUE_A_
CONTROLER`** (VRBO/Direct, statut de payout non résolu, indépendant du taux). **0 causée par un
taux manquant.**

**Découverte** : `REF_Taux_Commission` **existe déjà**, 19 lignes, 12/12 propriétaires couverts,
structure **exactement conforme** à la règle utilisateur (2025-01-01→2026-01-31 à 15 % pour tous,
puis taux spécifique à partir du 01/02/2026 quand il diffère — construit le même jour que
`REF_Gestion_Logements_Hist`, 28/06/2026). Vérifié dans le code (`lot10_calculer_resultats.py`) :
un taux manquant provoque `sys.exit(1)` (arrêt dur, pas un A_CONTROLER par ligne) — puisque tous
les runs ont toujours abouti à `SUCCES`, la preuve directe est qu'aucune réservation n'a jamais
manqué de taux.

**Conséquence : aucune reconstruction, aucune simulation, aucune écriture réelle.** Le référentiel
cible et le référentiel actuel sont identiques. `RESERVATION_A_CONTROLER_SANS_COMMISSION` reste à
**612, inchangé** — pour des causes totalement indépendantes du taux, hors périmètre de cette
mission (guest count, payout VRBO/Direct).

## GUEST_COUNT_MANQUANT_PREPARATION_CANAPE — constat, aucune action possible (2026-08-10, suite)

Détail complet : `79_RECONSTRUCTION_GUEST_COUNT.md`.

**553 lignes = 553 réservations distinctes**, sur les **4 seuls logements du parc** ayant une règle
de préparation canapé configurée (`LOG_0006/0008/0011/0013`), 23 mois.

**Le code est déjà correct** — commit `719169d` (« Correctif Hostaway - nombre voyageurs pour
preparation canape », **2026-06-20**) : `res.get("numberOfGuests")` remplace
`res.get("guestCount")` (champ API inexistant). **Constat déterminant** : le fichier réel
`MASTER_FACT_HA_Reservations.xlsx` a `guestCount` vide sur 1391/1391 réservations (100 %) **et ne
porte pas la colonne `numberOfGuests`** que le correctif ajoute — preuve qu'il provient d'une
extraction **antérieure au correctif**, jamais régénérée depuis. **Ce n'est pas un bug de code,
c'est une absence de ré-extraction Hostaway réelle.**

Recherche de source locale fiable : négative (aucun cache de payload API, aucune archive Lot1,
aucune autre source). **PREUVE_A = 0 sur 553.** Aucune correction de code, aucune donnée écrite,
aucune simulation possible — rien à simuler puisque rien ne change.

`RESERVATION_A_CONTROLER_SANS_COMMISSION` reste à **612** (553 guest count + 59 VRBO/Direct,
inchangé). TOTAL contrôles reste à **2002**.

**Question utilisateur unique, posée, en attente** : autoriser une ré-extraction Hostaway réelle
(accès API réseau réel, writer touchant des sources métier réelles, résultat non garanti à 100 %
si l'API elle-même ne fournit pas `numberOfGuests` pour d'anciennes réservations) ? Mission dédiée
et distincte si oui.

## Ré-extraction Hostaway réelle exécutée — GUEST_COUNT_MANQUANT 553→506 (2026-08-10, suite)

Détail complet : `79_RECONSTRUCTION_GUEST_COUNT.md` §11. **Ré-extraction Hostaway réelle
autorisée par l'utilisateur pour rafraîchir le master après correction `numberOfGuests`. Cette
autorisation ne constitue pas une activation générale du mode réel.**

Backup réel préalable (`99_ARCHIVES/LOT1_MASTER_HA_Reservations/…PRE_REEXTRACT_20260810_184807.xlsx`,
hash vérifié). Extraction API réelle en zone temporaire isolée (jamais d'écriture directe dans
`02_TRAVAIL` réel avant validation) : 1391→1527 réservations, `guestCount` 0 %→100 % rempli, 4
colonnes ajoutées (`numberOfGuests` + audit). Comparaison exhaustive ancien/nouveau : 17 disparues
vérifiées en direct via l'API (`status=cancelled`, sans payout — comportement conforme au code
existant, inchangé), 153 nouvelles (activité normale), 1 seul écart économique réel (résa
prolongée, cohérent). Simulation complète sur copie intégrale du projet (jamais sur le réel) :
lot4bis→lot4quater→lot9→lot10→lot11, 0 bloquant partout, REEL=COMPTABLE+HC vérifié à l'euro.

**Résultat mesuré : `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` 553→506 (-47).** Mécanisme
intégralement vérifié par jointure : les 506 restantes sont **100 % en mois clôturé** (historique
gelé par conception, jamais réécrit même par ce correctif) — la résolution a atteint 100 % de ce
qui était techniquement atteignable par API (mois ouverts). `RESERVATION_EXCLUE_A_CONTROLER`
59→70 (base élargie, explicable). Total A_CONTROLER Lot10 612→576.

**Seul `MASTER_FACT_HA_Reservations.xlsx` remplacé dans le réel** (hash relu identique à la
source, 1527 lignes/1527 `reservation_id` distincts/26 colonnes confirmés). Les autres fichiers
`Lot1_Hostaway` (Payout, Details, Fees, FinanceFields, Listings, Anomalies) restent les anciens —
désynchronisation partielle **délibérée**, le pipeline aval réel n'a **pas** été relancé (interdit
explicitement par l'autorisation). Intégrité globale : 950/950 fichiers réels contrôlés, 2 diffs,
tous deux attendus et documentés (`REF_Setup.xlsm` déjà committé + ce nouveau master). Port 8000/
PID 21136 intact. Tests ciblés : 62 passed, 0 nouvel échec.

**Aucun chiffre de clôture réel n'a changé** (pipeline aval réel non relancé). **CLÔTURE : NO GO.
PRÉPARATION MODE RÉEL : NO GO. MODE RÉEL : NO GO — NON ACTIVÉ**, aucun flag `app/config.py`
touché.

## Audit impact 506 guest count clôturés + correctif lot4ter (2026-08-11, suite)

Détail complet : `79_RECONSTRUCTION_GUEST_COUNT.md` (à compléter §12). Deux missions d'audit/
correction enchaînées, **aucune donnée réelle modifiée par ce tour — uniquement du code, committé
séparément.**

**Audit d'impact (copies)** : les 506 `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` restants sont
100% en mois clôturé, guestCount désormais connu pour 100% d'entre elles (0 cas encore
indéterminé). 397 sans impact canapé (delta 0), 109 avec correction nécessaire (+1 090,00 €
canapé). Ces 506 sont actuellement **exclues** de Commissions/NetProprietaire (statut
A_CONTROLER) — les y réintégrer ajouterait 14 302,91 € de commission et 75 412,65 € de net
propriétaire jamais formellement reconnus. **Résultat société global : delta 0,00 €** (REEL/
COMPTABLE/HORS_COMPTA déjà comptés via le Flux Lot9, indépendant du contrôle canapé Lot10) —
invariant vérifié à l'identique sur toutes les simulations de ce tour. 0 relevé propriétaire
existant pour ces 3 propriétaires/17 mois (vérifié en lecture seule sur `app.db` réelle).

**Découverte critique** : `lot4ter_historiser_reservations_cloturees.py` réécrivait HIST depuis
une liste `COLS` fixe de 28 colonnes n'incluant pas `guestCount` — toute correction ciblée de ce
champ était **silencieusement effacée** au run normal suivant (nouvelle clôture). Bug reproduit
par un test rouge (`tests/test_lot4ter_guestcount_persistence.py`, 4 cas), corrigé par l'ajout
minimal de `"guestCount"` à `COLS` + capture dans la construction de nouvelle ligne (2 lignes de
code modifiées). Test rouge→vert. Régression complète : 272 passed (moteur, root `tests/`) + 432
passed/36 skipped (app, `05_APPLICATION/tests`), 0 nouvel échec.

**Preuve de persistance** : correction fail-closed des 506 guestCount appliquée sur copie de HIST
(script dédié `correction_guestcount_hist_ciblee.py`, refuse tout champ hors guestCount, toute
réservation hors liste, toute valeur invalide, toute écriture réelle sans
`AUTORISATION_ECRITURE_REELLE=1` explicite), puis run **normal** de `lot4ter` (le fix appliqué) :
guestCount survit (506/506 conservés, 1269 lignes inchangées, 17 mois toujours CLOTURE). Chaîne
aval rejouée (lot4quater→lot9→lot10→lot11) : GUEST_COUNT 506→0, A_CONTROLER 576→70 (VRBO/Direct
seuls), résultat société identique au centime. Idempotence du cycle complet vérifiée (2e passage
correction+lot4ter+aval : mêmes compteurs exacts). Rollback vérifié (hash restauré exact,
compteurs reviennent à 576/506/70 avant correction).

**Code committé (`9a0a6aa`), puis donnée réelle appliquée.** Backup réel préalable
(`99_ARCHIVES/HIST_Reservations_Cloturees/…PRE_CORRECTION_GUESTCOUNT_20260811_144118.xlsx`, hash
vérifié). Écriture réelle : colonne `guestCount` ajoutée (29e colonne), 506 valeurs renseignées,
1269 lignes inchangées, **0 diff sur les 28 colonnes existantes** (vérifié programmatiquement
contre le backup). Journal append-only des 506 corrections :
`99_ARCHIVES/JOURNAL_CORRECTIONS/journal_correction_guestcount_hist_20260811.json`. Intégrité
globale : 950/950 fichiers baseline contrôlés, **3 diffs, tous attendus et documentés**
(`REF_Setup.xlsm`, `MASTER_FACT_HA_Reservations.xlsx` déjà committés + `HIST_Reservations_
Cloturees.xlsx` ce tour). Port 8000/PID 21136 intact.

**Pipeline aval réel non relancé** (interdit explicitement par l'autorisation, `CALCULS_REAL_
RUN_ENABLED` jamais touché). Les sorties `MASTER_CTRL_Coherence.xlsx`/export de clôture réels ne
reflètent pas encore la correction — cela nécessitera un futur run du pipeline aval autorisé
séparément. Le HIST réel est corrigé et durable (fix `lot4ter` committé), mais les résultats de
clôture affichés restent ceux d'avant jusqu'à ce run futur.

## Audit des 70 RESERVATION_EXCLUE_A_CONTROLER (VRBO/Direct) — bug corrigé (2026-08-11/12)

Détail complet : `80_AUDIT_RESERVATIONS_VRBO_DIRECT_A_CONTROLER.md`. Sur les 70 (39 Direct + 31
VRBO), toutes connues de Hostaway mais sans payout plateforme calculable par design
(`DIRECT_HORS_HOSTAWAY`/`VRBO_UNKNOWN`, comportement voulu de `lot1_hostaway_extract.py`).

**Bug réel trouvé et corrigé** : `lot10_calculer_resultats.py` construisait l'onglet A_CONTROLER
directement depuis le statut brut Lot1, sans vérifier si la réservation avait déjà été résolue
ailleurs (VRBO via backfill CSV historique déjà en place, Direct via saisie HH déjà existante,
décision D054). **28 réservations (27 VRBO + 1 Direct) étaient déjà correctement comptées dans
COMMISSIONS mais listées une seconde fois par erreur** — vérifié programmatiquement (0 suppression
injustifiée). Test rouge→vert (`tests/test_lot10_reservation_exclue_dedup.py`), fix minimal (5
lignes), régression complète 274 passed (moteur) + app en cours, 0 nouvel échec attendu.

**Résultat simulation (idempotent, moteur réel)** : `RESERVATION_A_CONTROLER` 70→42.
**Delta résultat société : 0,00 €** (les 28 étaient déjà comptées financièrement, seul un
doublon d'affichage/exclusion corrigé). 42 restantes (38 Direct sans saisie HH + 4 VRBO sans
backfill) = donnée absente, nécessitent une saisie manuelle humaine, pas une décision de règle
métier (la règle existe déjà et fonctionne). **Aucune donnée réelle modifiée** (correctif de code
uniquement, aucune écriture de donnée n'était éligible).

## Mission de nuit — baseline de clôture canonique après nettoyage complet (2026-08-12)

Détail complet : `81_BASELINE_CLOTURE_APRES_NETTOYAGE.md`. Simulation canonique fraîche
reconstruite depuis HEAD `6b60577` (sources actuelles + **Banque réelle régénérée sur copie**,
pipeline `lot8a→lot8b→lot8c` déjà validé rejoué, aucun stub). **Aucune donnée réelle modifiée,
aucun code modifié — 0 nouveau bug trouvé après audit complet.**

**Découverte majeure : les 541 BLOQUANT du dernier run réel sont EXACTEMENT et UNIQUEMENT
`GESTION_LOGEMENT_MISSING` (472) + `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` (69).** Vérifié
programmatiquement : les 69 charges sont un sous-ensemble strict à 100 % des 77 couples gestion
— même cause racine, pas un bug séparé. Aucun autre BLOQUANT n'existe dans le système.

Banque fraîche : 541 mouvements, 236 classés déterministement, 222 en rapprochement humain
(166 Airbnb + 56 propriétaires), 83 en file `A_ENVOYER_IA`. Statut Lot11 passe de
`BANQUE_NON_DISPONIBLE_GIT` à `BANQUE_DISPONIBLE`. 0 produit économique créé, 0 matching
Banque↔Réservation (interdit, respecté).

Les 42 Direct/VRBO : nouvelle vérification API Hostaway en direct cette nuit, 0 nouvelle preuve
(100 % `paymentStatus=Unknown` confirmé fraîchement). Résiduels (Ménages, provenance) audités,
classifiés, 0 bug technique, hors périmètre d'action immédiate.

REEL/COMPTABLE/HORS_COMPTA avec Banque réelle : 313 756,48 / 303 232,32 / 10 524,16 € (écart
0,00 €). Idempotence vérifiée (2 runs identiques). Intégrité : 950/950, 3 diffs déjà committés,
0 nouvelle modification réelle. Port 8000/PID 21136 intact.

## État de reprise

**3 décisions/actions humaines distinctes ferment tout le reste** (voir `81_BASELINE_CLOTURE_
APRES_NETTOYAGE.md` §13) : (1) historique gestion 14 logements jan-juil 2025 (résout 472+69=541
BLOQUANT) ; (2) 42 saisies manuelles Direct/VRBO ; (3) 222 mouvements bancaires humains (166
Airbnb export détaillé absent + 56 acomptes) + 83 file assistée. Aucune autre correction
technique possible sans invention de règle/donnée. Toutes les corrections déterministes de code
sont épuisées. **Prochaine action : décision utilisateur sur l'historique de gestion 2025
(impact le plus large, résout deux familles BLOQUANT d'un coup) ; en parallèle, saisies humaines
Direct/VRBO et classification Banque peuvent avancer indépendamment.**

Commandes de reprise : `cd <worktree> && git status && git log --oneline -5` ; instance de recette
type : `RECETTE_MODE=1 PROJECT_ROOT=<copies> APP_DATA_DIR=<recette>/APP_DATA python -m uvicorn
app.main:app --port 80XX` depuis `05_APPLICATION` (jamais le port 8000).

## Prolongation gestion au 01/01/2025 — 541 BLOQUANT → 0 (2026-08-12)

Décision utilisateur reçue : « le propriétaire a toujours été le même pour chaque logement ».
Couverture `REF_Gestion_Logements_Hist` prolongée de 2025-08-01 à **2025-01-01** pour les 14
logements déjà prolongés en amont. Détail complet : `77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md`
§13.

Procédure identique aux corrections précédentes : copie d'abord, script fail-closed (refuse tout
`gestion_id`/valeur de départ hors liste autorisée), simulation canonique fraîche rejouée sur
`SIMULATION_CANONIQUE_HEAD_6b60577` (Banque réelle déjà incluse) via `run_regression_pipeline.py`,
2 runs identiques (idempotence), puis backup réel + SHA256 + écriture gatée
(`AUTORISATION_ECRITURE_REELLE=1`) + relecture immédiate.

**Résultat simulation (avant → après)** : `GESTION_LOGEMENT_MISSING` 472→**0**,
`CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` 69→**0**, BLOQUANT total 541→**0**. A_CONTROLER (14) et
INFO (10) strictement inchangés, code par code. REEL/COMPTABLE/HORS_COMPTA : **0,00 € de delta**
(chaque logement n'a qu'une ligne de gestion — prolonger `date_debut` ne change ni propriétaire ni
taux déjà utilisés par le calcul).

Réel modifié : 14 cellules `date_debut` uniquement dans `REF_Setup.xlsm`, backup horodaté
disponible dans `99_ARCHIVES/REF_Setup/`. Intégrité globale : 3/950 diffs, exactement les 3
attendus (ce changement + les 2 déjà committés). Pipeline réel **non relancé**
(`CALCULS_REAL_RUN_ENABLED` OFF). Tests ciblés (0 code modifié) : 24/24 verts.

**CLÔTURE TECHNIQUE (gestion + charges dépendantes) : GO.** Restes humains inchangés (42
Direct/VRBO, Banque humaine 222+83, 14 A_CONTROLER résiduels, mappings comptables provisoires).
**PRÉPARATION MODE RÉEL : NO GO. MODE RÉEL : NO GO — NON ACTIVÉ.**

## Pack final actions humaines — vraie file Banque = 138, pas 305 (2026-08-12)

Détail complet : `82_PACK_FINAL_ACTIONS_HUMAINES.md`. Les « 222 mouvements Banque humains »
ci-dessus étaient un amalgame de reporting (pas un bug de code) : 166 sont `PAYOUT_PLATEFORME`
(Airbnb) — catégorie moteur déjà correcte, déjà exclue du rapprochement réservation dans
l'application (`banques_candidats_service.py`, conforme au commit `78877da`), **0 décision
humaine**, informatif, en attente de l'export Airbnb détaillé. Les 56 restants sont de vrais
candidats propriétaires (`VIREMENT_PROPRIETAIRE_A_RAPPROCHER`), bloqués par un seul prérequis :
alimenter `SAISIE_AcomptesProprietaires.xlsx` (Lot 5). Les 83 lignes `A_ENVOYER_IA` se ramènent à
**82 décisions distinctes** (1 doublon `mouvement_id` vérifié conforme, dédupliqué proprement,
traçabilité conservée).

**Vraie baseline opérationnelle Banque : 56 + 82 = 138 décisions humaines** (au lieu de 222+83=305
brut). Réservations : 42 (38 Direct + 4 VRBO) inchangé, mécanisme de saisie existant audité et
prouvé sur fixtures (58/58 tests verts, aucune vraie réservation touchée). Des 14 A_CONTROLER,
seuls 3 sont un travail réellement nouveau (Ménages : écart Hostaway 4 logements, hors HA
2 logements, provenance informative) — les 11 autres reformulent en agrégat les blocs
Réservations/Banque déjà comptés. Mappings comptables (606000 générique) confirmés non bloquants
techniquement — réserve acceptable, écritures restent équilibrées.

**0 code modifié, 0 bug trouvé, mission docs-only.** CLÔTURE TECHNIQUE : GO. PRÉPARATION MODE
RÉEL : NO GO (138 décisions Banque + 42 saisies Réservations + rollback jamais exercé hors
recette). MODE RÉEL : NO GO — NON ACTIVÉ.

## Audit Lot 5 — les 56 proprietaires ne sont pas reductibles par preuve (2026-08-13)

Detail complet : `83_AUDIT_LOT5_RAPPROCHEMENT_PROPRIETAIRES.md`. Question posee : quelle preuve
Lot 5 existe deja permettant de resoudre tout ou partie des 56 mouvements proprietaires Banque ?
**Reponse mesuree : aucune.** SAISIE_AcomptesProprietaires.xlsx = 0 ligne, sortie Lot 5 = 0 ligne,
table mouvements_tresorerie_proprietaires = 0 ligne (base recette) et absente de la base reelle
(migration 0016 alors que la table est creee en 0025). Aucun objet metier proprietaire n'existe
nulle part dans le depot.

PREUVE_A=0, PREUVE_B=0, AMBIGU=0, ABSENT=56. Rapprochement EXACT/PARTIEL/GROUPE/AMBIGU = 0,
AUCUN = 56 — non par echec moteur mais faute d'objet a rapprocher. Les 56 sont 100% CREDIT,
27069,18 EUR, 2025-11 -> 2026-07, **aucun montant repete** (0 recurrence exploitable), reparties
sur 7 identites dont une explicitement non resolue (`FAMILLE_UZON_A_CONTROLER`, 7 mvts). Les
proprietaire_id proviennent de regles de libelle bancaire : IDENTITE_CANDIDATE, jamais prouvee.

**56 -> 56 decisions humaines**, compressibles a **7** si l'utilisateur confirme une nature
uniforme par proprietaire (non deduit ici). Les 82 A_ENVOYER_IA : 0 correspondance deterministe
avec les referentiels existants, 12 regles candidates preparees couvrant 57 mouvements sur 82
(compression possible 82 -> 37, jamais par validation inventee). Prerequis technique consigne :
migrations 0017->0026 a appliquer a la base reelle avant exploitation de la tresorerie
proprietaires.

Tests fixtures : 126/126 verts (tresorerie proprietaires, rapprochement exact/partiel/groupe/
ambigu, candidats). Idempotence Banque reverifiee (lot8a/8b/8c relances : 236/222/83 identiques,
0 doublon). Integrite : 3/950 diffs, tous deja committes, **0 nouvelle modification reelle**.
0 code modifie, 0 bug trouve. Port 8000 constate libre, non manipule. Mode reel OFF.

**CLOTURE TECHNIQUE : GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO — NON ACTIVE.**

## Repetition migration app.db 0016 -> 0026 + bug lot8c/Lot5 (2026-08-13)

Preuves : `84_REPETITION_MIGRATION_DB_0016_VERS_0026.md`, runbook :
`85_RUNBOOK_MIGRATION_APP_DB_REELLE.md`.

**Migration repetee sur copies, base reelle JAMAIS migree** (hash inchange, toujours 0016). Les 10
migrations 0017->0026 sont **purement additives** : 0 ALTER, 0 DROP, 0 DELETE, 0 UPDATE, tous les
CREATE en IF NOT EXISTS, tous les INSERT en OR IGNORE, 0 clause REFERENCES. Sequentielle une par
une : integrity ok aux 10 etapes, 36->67 tables, 52->108 index, 0 trigger -> 1, **0 perte** (hash
canonique des 5 tables metier non vides conserve a chaque etape). Migration automatique via
`apply_migrations()` : schema strictement identique au sequentiel, contenu identique hors
horodatages (`applied_at`, `date_creation`, id autoincrement). Idempotence : 2 rejeux
supplementaires, 0 ecart. Rollback par restauration de backup : **hash exact restitue**, base
relisible en 0016. **MIGRATION PRETE ET REPETEE** (prete != autorisee).

**Bug reel trouve et corrige (commit `1759ce0`)** : `lot8c` affirmait en dur "MASTER_FACT_MAN_
AcomptesProprietaires vide - attendre saisie Lot 5" sans jamais ouvrir ce fichier. Prouve
empiriquement : alimenter Lot 5 puis relancer lot8c ne changeait rien et ne le signalait pas.
Correction minimale (lecture reelle de l'onglet MASTER ; 0 objet -> comportement inchange, >0 ->
controle `LOT5_REGLES_RAPPROCHEMENT_A_ARBITRER`). **Aucune regle de rapprochement Lot5<->Banque
n'a ete definie** : tolerance, fenetre de date et groupement restent un arbitrage metier ouvert.
4 tests (1 non-regression + 3 rouges avant fix).

**Contrainte Lot 5 documentee** : `lot5_master_acomptes_proprietaires.py` est un generateur de
template ; `build_master()` ne lit pas SAISIE. Le peuplement de MASTER depuis SAISIE se fait par
**refresh Power Query dans Excel** (M-code embarque : 5 controles BLOQUANT + 5 A_CONTROLER +
validation croisee HH + detection de doublons). Reimplementer cette ingestion en Python serait une
vraie feature, volontairement non entreprise. Consequence pratique : apres saisie, l'utilisateur
doit ouvrir le classeur dans Excel et actualiser les requetes avant de relancer lot8c.

12 regles candidates sur les 82 A_ENVOYER_IA **validees techniquement** (pas metier) : 0 collision,
0 PAYOUT_PLATEFORME, 0 mouvement proprietaire, 0 mouvement deja classe, 57 couverts + 25 isoles =
82 exact -> compression possible 82 -> 37 decisions.

Baseline de controles inchangee (0 BLOQUANT / 14 A_CONTROLER / 10 INFO / 24) et invariants
economiques identiques (313 756,48 = 303 232,32 + 10 524,16, ecart 0,00 EUR) apres le fix.
Integrite : app.db reelle hash identique, 3/950 diffs deja committes, 0 nouvelle modification
reelle. Port 8000 constate libre, non manipule. **MODE REEL : NO GO — NON ACTIVE.**

## Facturation proprietaires : l'application cree desormais ses factures (2026-08-13)

Detail : `86_FACTURATION_PROPRIETAIRES_APPLICATION.md`, recette :
`87_RECETTE_FACTURES_PROPRIETAIRES.md`.

**L'application ne se contente pas de calculer une prefacture proprietaire. Elle genere desormais
une facture proprietaire reelle dans son modele applicatif, avec snapshot, numero, document PDF,
statuts, historique et suivi du reglement.**

**Les factures fournisseurs recues restent des documents externes importes/enregistres.**

**Facture, reglement, Banque, reservation et charge restent des objets distincts.**

RELEVE != FACTURE. Le releve Lot 12 (12/13 lignes) explique au proprietaire son revenu et son
solde. La facture ne porte QUE les 5 composants de `montant_du_conciergerie` : commission, menage,
preparation canape, charge fixe, charges exceptionnelles refacturees. Les 7 autres types (payout,
revenu net, acomptes, paiements recus, reste a payer, statut) sont listes explicitement comme non
facturables dans le code. Grain conserve : mois x proprietaire x logement.

Modele : migration **0027**, table separee de `factures` (0017/0022) qui est entierement orientee
fournisseur (`fournisseur_id_opaque NOT NULL`, index unique fournisseur+reference,
`facture_lignes.charge_id NOT NULL`) -- la reutiliser aurait impose un fournisseur fictif et une
charge fictive par ligne.

Cycle : BROUILLON -> VALIDE -> EMIS (+ ANNULE). Une facture EMIS est **immutable** : contenu fige
dans un snapshot JSON hashe, relu depuis ce snapshot et jamais depuis les sources -- un recalcul
Lot 10 ou un changement de taux ne peut plus la modifier. Correction par AVOIR lie, jamais par
edition ni suppression. Numerotation serialisee (BEGIN IMMEDIATE), numero jamais reutilise. PDF
deterministe (meme snapshot = meme hash), servi fige et jamais reconstruit. Pas de TVA (decision
utilisateur). Identite emetteur incomplete -> validation refusee, facture reste BROUILLON.

Recette complete sur copie migree 0027, port 8042 (jamais 8000) : facture emise
`RECETTE-2026-00001`, PDF telecharge avec hash identique au hash fige, anti-doublon, avoir, solde
derive, immutabilite -- tous verifies sur instance vivante. 89 tests cibles verts (dont
non-regression fournisseurs).

**Point ouvert assume, non contourne** : l'ecriture comptable de VENTES n'est pas branchee. Decider
quelle est la source unique de l'ecriture (Lot 10 ou la facture) est un arbitrage metier ; brancher
un generateur sans cet arbitrage creerait le double comptage que le cadrage interdit. De meme,
l'imputation d'un mouvement bancaire sur une creance de facture reste a cabler sur le moteur de
rapprochement generique existant (le solde, lui, se calcule deja).

**EMISSION REELLE : NON AUTORISEE. APP.DB REELLE : NON MIGREE. MODE REEL : NO GO — NON ACTIVE.**

## La facture emise est la source unique de la vente (2026-08-13, suite)

Detail : `86_FACTURATION_PROPRIETAIRES_APPLICATION.md` §14. Le point ouvert de la mission
precedente (ecriture comptable non branchee, risque de double comptage) est **ferme**.

**Decision utilisateur appliquee : la facture propriétaire au statut EMIS materialise la vente.**
Chaine : Lot 10 calcule -> Lot 12 prepare le releve -> la FACTURE constate -> le REGLEMENT eteint
la creance -> la BANQUE prouve le mouvement. Aucun de ces objets n'en cree un autre.

Source VENTES avant : `ventes_lot12_adapter_service`, origine `LOT12_PROPRIETAIRE_MOIS`, grain
proprietaire x mois (agrege), deja marque SOURCE_PROVISOIRE_LOT12. Source apres :
`generer_ecriture_vente_facture`, origine `FACTURE_PROPRIETAIRE`, origine_id = facture_id_opaque,
montant pris sur le total FIGE de la facture (jamais recalcule).

BROUILLON : 0 ecriture. VALIDE : 0 ecriture. EMIS : 1 ecriture VENTES, 411000 debit / 706000
credit, statut PROPOSEE (mapping provisoire assume).

**Double comptage impossible par construction** : garde bidirectionnelle, code stable
`FACTURE_PROPRIETAIRE_DOUBLE_SOURCE_COMPTABLE`. Une facture refuse de constater si l'ancien
mecanisme a deja comptabilise ce proprietaire/mois ; l'ancien generateur refuse si une facture a
deja constate. Conflit signale, jamais resolu en silence. Verifie sur instance vivante : apres
emission, rejouer le generateur Lot 12 renvoie le refus en citant la facture, total VENTES reste
500,00 EUR.

**Frontiere historique/futur = `origine_type`**, reference source explicite et non une date de
bascule. Aucune ecriture historique supprimee ni regeneree.

Migration repetee **jusqu'a 0027** sur copie : sequentielle verte (71 tables, 118 index, 0 perte),
automatique identique au sequentiel hors horodatages, idempotence (3 passages, 0 ecart), rollback
hash exact. **La base reelle reste en 0016, non migree, hash inchange.**

Restent avant emission reelle : format de numero et mentions legales (arbitrage juridique),
identite societe a renseigner, imputation d'un mouvement bancaire sur une creance de facture a
cabler, et le mapping de compte produit (706000) toujours provisoire.

**EMISSION REELLE : NON AUTORISEE. MODE REEL : NO GO — NON ACTIVE.**

## Conformite des factures proprietaires (2026-08-13, suite)

Detail : `88_CONFORMITE_FACTURES_PROPRIETAIRES.md`. Le module facture est desormais **termine**.
Ce qui reste n'est plus de l'architecture : ce sont des **valeurs a renseigner** et une
**obligation future de transmission electronique**.

Numerotation legale : **F-AAAA-NNNNNN** (factures) et **A-AAAA-NNNNNN** (avoirs), deux series
independantes portees par le compteur existant. Numero consomme uniquement a l'emission (un
brouillon abandonne ne cree aucun trou), fige, jamais reutilise, concurrence protegee. Chaque
annee ouvre une serie explicite.

Migration **0028** additive : `factures_proprietaires_conformite` (identites completes figees,
type de client, nature d'operation, periode de prestation, regime TVA + mention, HT/TVA/TTC,
conditions de reglement, champs electronic_invoice_* neutres) et
`factures_proprietaires_lignes_detail` (quantite, prix unitaire).

Configuration unique `facturation_config_service` : **aucune valeur juridique inventee**, tout vide
par defaut. Regime TVA = A_CONTROLER tant qu'il n'est pas declare (la decision "pas de TVA" dit
qu'aucune TVA n'est facturee, pas pourquoi). Taux de penalites et indemnite forfaitaire sans
defaut : ils bloquent l'emission professionnelle. Vocabulaire TVA aligne sur D083.

Controle de pre-emission unique : PRETE_A_EMETTRE / BLOQUEE + liste des manques nommes par code
stable. Toujours affiche, ne bloque que si l'emission reelle est ouverte — la recette peut exercer
le parcours avec une configuration incomplete sans creer de chemin permissif en production.

Type de client jamais devine : sans information explicite il vaut A_CONTROLER et bloque. Les
clauses B2B sont exigees ET imprimees uniquement face a un PROFESSIONNEL.

PDF refondu : periode de prestation distincte de la date d'emission, tableau
Designation/Qte/PU HT/Total HT/TVA, totaux HT/TVA/TTC, echeance, conditions, mentions configurees.
Determinisme conserve. Le PDF est un **rendu derive** du snapshot, jamais la source de verite —
une future facture electronique sera un autre rendu du meme snapshot, sans recalcul metier.

Facturation electronique : **architecture prete, rien de branche** (aucune plateforme choisie,
aucune API, aucun envoi, pas de Factur-X). Chantier futur : FACTURATION_ELECTRONIQUE_PA.

Recette E2E (port 8044, base copie 0028) : F-2026-000001 et F-2026-000002 emises, conformite figee
(periode 01/07->31/07, echeance 31/08, franchise TVA, HT=TTC=500), ventes comptables generees,
PDF complet verifie, avoir cree. 1 defaut trouve et corrige pendant la recette (clauses B2B
imprimees sans regarder le type de client) + test renforce.

Migration repetee 0016->0028 : sequentielle (73 tables, 122 index, 0 perte), automatique
identique, idempotence, rollback hash exact. **Base reelle toujours 0016.**

**IDENTITE SOCIETE : DONNEES A FOURNIR. REGIME TVA : A CONFIRMER. EMISSION REELLE : NON AUTORISEE.
MODE REEL : NO GO — NON ACTIVE.**

## Completude fonctionnelle : inventaire, construction des manques (2026-08-14)

Matrice : `92_MATRICE_COMPLETUDE_FONCTIONNELLE.md`. Recette utilisateur :
`89_RECETTE_MANUELLE_AVANT_BASCULE.md` (**PRETE A EXECUTER**, jamais validee sans retour explicite).

Inventaire fait a partir des **250 routes reellement montees**, pas de la roadmap. Constat
contraire a ce qu'on pouvait craindre : l'application etait deja largement complete. Les manques
reels se reduisaient a **quatre ecrans financiers**, tous construits :

1. **Creances proprietaires** (`/creances`) — factures emises seulement (un brouillon n'est pas une
   creance), avoirs inclus en negatif, filtres, agregation par tiers, anciennete.
2. **Dettes fournisseurs** (`/dettes`) — factures ouvertes a solde non nul, solde delegue au
   service factures.
3. **Echeancier** (`/echeancier`) — ECHU / 7 j / 30 j / au-dela + tranche SANS_ECHEANCE isolee,
   position nette avec la reserve explicite que ce n'est pas une tresorerie disponible.
4. **Balance generale** (`/comptabilite/balance`) — soldes debiteur/crediteur, classes, filtres
   periode et journal, contrepassees exclues, desequilibre affiche et non masque.

Vocabulaire clarifie : **trois soldes distincts** coexistent et ne doivent pas etre confondus —
solde de facture (traite par ces vues), solde de tresorerie proprietaire (objet separe), net
d'exploitation (resultat Lot 10, qui n'est pas une creance).

Comptage : 54 fonctions inventoriees, **48 DISPONIBLE, 5 PARTIEL, 1 MANQUANT, 0 BUG** —
**completude 89 %**. Les 5 partiels sont utilisables ; aucun n'est un trou d'architecture :
backfill VRBO par CSV (4 reservations), provenance Menages (relance reseau), imputation d'un
reglement sur facture proprietaire (solde derive correct, cablage restant), mappings comptables
definitifs (arbitrage), valeurs d'identite/TVA (doc 88). Le seul MANQUANT est la facturation
electronique, chantier `FACTURATION_ELECTRONIQUE_PA`, modele deja pret.

22 nouveaux tests dont la persistance apres redemarrage (creances et dettes relues depuis la base,
aucune dependance a un etat memoire).

**Prochaine etape : la recette manuelle par l'utilisateur.** Le cut-over n'est pas la prochaine
etape. MODE REEL : NO GO — NON ACTIVE.

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

## Mise à jour 2026-08-18 (suite) — Ménages : moteur en SQLite, application pas encore

**Lot6a→6f acceptent `--source SQLITE`** en plus du chemin Excel historique (conservé, sert encore
Lot9-12 non migrés). Sorties : `menages_taches_enrichies`/`menages_declarations_internes`/
`menages_rapprochement`/`menages_gainperte`/`menages_cout_complet` (migration 0038). Formules et clé
de ventilation inchangées.

**Facture PDF ménage externe → SQLite direct** (mission précédente, non retouché) : PDF → `factures`/
`facture_lignes_menage` (0037/0039), sans passer par le module Charges (encore Excel). Ventilation
des frais sans logement, contrôle facture, dette intervenant interne FIFO : construits, testés.

**`menages_reader` : 6/8 sources en SQLite** (`hostaway_taches`/`hostaway_comptage`/`internes`/
`rapprochement`/`gainperte`/`cout_complet`) — plus de repli Excel sur ces fonctions, testé (101
tests verts sur les fichiers de test concernés). Restent Excel : `externes()` (exigerait d'étendre
`facture_lignes_menage`), `controles_rapprochement()`/`controles_lot11()`/`pools_charges()`.

**Trois services NON migrés** (`menages_chaine_service`, `menages_recalcul_service`,
`controles_runner_service`) — orchestrent la chaîne Excel jusqu'à Lot9-12 (hors périmètre) ; les
migrer isolément aurait exigé de commencer Lot9-12. `controles_detail_reader` dépend de la même
limite que `externes()`. Excel entre Lots6 : toujours présent (M04, ménages externes, Lot6d/e/f
legacy, consommés par ces 3 services). Excel application Ménages : réduit, pas éliminé.

**Chaîne suivante : `externes()` (étendre `facture_lignes_menage`), puis les 3 services restants,
puis Lot9 → SQLite.**

Détail complet : document `95` (§14).

## ✅ Mise à jour — Ménages fermé sans master permanent, Lot9 et Lot10 TERMINÉS

Depuis le paragraphe ci-dessus (obsolète) : Ménages fermé à 100% (10/10 sources du lecteur
migrées, `externes()` inclus, plus aucun master Ménages permanent requis) — test bloquant
`test_menages_sans_excel.py` vert. Lot9 (`flux_unifies`, migration 0043) et Lot10 (`lot10_*`,
migration 0044) migrés en SQLite avec parité financière réelle prouvée à 0,00 € d'écart (détail
dans le document `95`, §15-16). Tests bloquants `test_lot9_sans_master_calc_flux.py` et
`test_lot10_sans_masters.py` verts. Campagne complète post-fermeture Lot10 : moteur 345/345,
application 2856/2856, 0 échec.

**Chaîne suivante : Lot11 (contrôles → SQLite), puis conditionnellement Lot12 (préfacture/relevés
propriétaires → SQLite).**

Détail complet : document `95` (§15-16).

## ✅ Mise à jour — Lot11 (contrôles transverses) TERMINÉ pour les groupes couverts

Nouveau service `controles_lot11_service.py` (SQLite natif, migration 0045) : port fidèle des
groupes de contrôle Lot11 dont les sources sont déjà SQLite (réservations, payouts/anomalies
Hostaway, Lot9, Lot10, référentiel, banque). Parité réelle prouvée sur données réelles 2026-08-17 :
10/11 constats en accord exact, 0 BLOQUANT des deux côtés, écart restant expliqué (divergence de
fraîcheur entre deux copies réelles de REF_Cloture_Mensuelle). Test bloquant
`test_lot11_sans_masters.py` vert. Groupes non couverts (AirCover, ajustements post-clôture,
sources vides, Lot7C avantages, ménages externes 6f, caisse théorique) documentés, non fabriqués —
servis par le moteur legacy tant que leurs sources restent Excel.

**Chaîne suivante : conditionnellement Lot12 (préfacture/relevés propriétaires → SQLite), sous
réserve — Lot10 formellement fermé ET Lot11 réellement validé (les deux le sont désormais).**

Détail complet : document `95` (§17).

## ✅ Mise à jour — Lot12 (préfactures propriétaires) TERMINÉ — MISSION LARGE CLOSE

Nouveau service `lot12_prefactures_service.py` (SQLite natif, migration 0047 + 0046 pour le
dashboard Lot11 persisté) : port fidèle des préfactures propriétaires. Règle fondamentale
inchangée : préfactures uniquement, jamais une facture émise — la facturation-propriétaire réelle
reste `ventes_lot12_adapter_service`/`comptabilite_ecritures_service`, chemin séparé, vérifié par
test dédié. Parité réelle prouvée sur données réelles 2026-08-17 : 285/285 préfactures, 3481/3481
lignes, tous montants identiques sur l'intégralité du jeu (pas un échantillon). Tests bloquants
`test_lot12_sans_masters.py`/`test_lot12_pas_de_double_comptage.py` verts.

**Lot9, Lot10, Lot11 (groupes couverts), Lot12 : tous clos dans cette mission.** Arrêt volontaire
ici, conformément à la mission — Lot13/export final/orchestrateur restent hors périmètre, à traiter
dans une mission dédiée ultérieure.

Détail complet : document `95` (§18).

## ✅ Mise à jour — Lot11 fermé à 100%, Lot13 export-only, orchestrateur et ordonnanceur

Lot11 est entièrement SQLite : les 6 derniers groupes legacy sont portés (AirCover, ajustements
post-clôture, sources vides, Lot7C, ménages externes 6f, provenance, caisse théorique). Parité
réelle complète : 23/24 constats identiques sur six champs, le seul écart étant une divergence de
fraîcheur entre deux copies réelles du même référentiel, prouvée côté données.

`MASTER_CTRL_Coherence.xlsx` n'est plus lu nulle part au runtime : le runner recalcule via le
service SQLite sur une copie de la base (fin de la boucle SQLite → XLSX → moteur → SQLite,
`controles_lot11_adapter` supprimé), et `controles_cloture_reader` lit les constats en base — il
servait jusque-là aux écrans les contrôles du dernier calcul LEGACY.

Lot12 : `facture_id` dérive du grain métier, plus d'un rang. Lot13 : export terminal, parité
identique fichier par fichier, écart 0,00 €, exports reconstructibles.

Orchestrateur (migration 0050) : DAG de datasets, fraîcheur fondée sur les runs, propagation aux
descendants, atomicité, verrou à bail, reprise après crash, écran `Pilotage / Actualisation`.
Ordonnanceur Hostaway 5 h prêt mais INERTE (`ORDONNANCEUR_ACTIF` faux), même service que le bouton
manuel, H6 sur cadence distincte.

**Prochaine mission : administration REF_Setup SQLite, analytique 3 niveaux, recette manuelle
UI-only, préparation du cut-over.**

Détail complet : document `95` (§19).

---

## Clôture 2026-08-22 — ZERO EXCEL OPÉRATIONNEL = OUI

Les saisies HH et les 4 familles extras propriétaires (Acomptes, AirCover, Imputations Airbnb,
Ajustements post-clôture) sont désormais SQLite. Inventaire Excel runtime refait de zéro (58
fichiers `app/`, appelants vérifiés par grep) : les 5 critères mission sont à 0
(`SAISIE_EXCEL_OBLIGATOIRE`, `REF_SETUP_RUNTIME`, `EXCEL_ENTRE_MOTEURS`, `MASTER_CALCULE_REQUIS`,
`BUG_RUNTIME_EXCEL`). Campagne complète rejouée après clôture (moteur 345/345, application
~2900 tests, 0 échec hors 4 défauts pré-existants confirmés sans rapport avec Excel). Détail
complet : documents `95` (§20) et `97` (§4).

**Prochaine mission** : administration REF_Setup SQLite, analytique 3 niveaux, recette manuelle
UI-only, préparation du cut-over.

**Mis à jour 2026-08-22** : la dette technique listée ci-dessus (code mort des anciens
orchestrateurs Excel) a été traitée — `saisie_charges_transaction_service.py`,
`writers/saisie_charges_writer.py`, `writers/saisie_hh_writer.py`, l'essentiel de
`services/saisie_hh_schema_migration.py` et 15 fonctions mortes de
`readers/saisie_charges_reader.py` supprimés, 6 fichiers de test obsolètes retirés avec eux. 0
nouvelle régression (campagne complète rejouée : moteur 345/345, application ~2593 passed).
Détail : `NETTOYAGE_LEGACY_POST_SQLITE.md`.

**Mis à jour 2026-08-22 (fermeture technique)** : les adaptateurs SQLite→Excel restants
(Lot1/4quater/6b/6c/8/10) audités un par un — 0 appelé par « Actualiser toute l'activité »
(`orchestrateur_dag.NOEUDS` inspecté directement) ; ceux qui produisent réellement un XLSX
(`reservations_adaptateur_moteur`, `banque_adaptateur_moteur`,
`hostaway_cleaning_tasks_adaptateur_moteur`) ne sont atteignables que via la route recette
isolée `menages_chaine_service` (copies, mode réel refusé) — `LEGACY_PARITE` confirmé, pas un
défaut runtime. Test bloquant renforcé pour intercepter aussi l'ÉCRITURE (`Workbook.save`), pas
seulement la lecture. Les 4 défauts pré-existants ont été corrigés (causes réelles isolées, aucune
règle économique modifiée). **Baseline désormais 0 failed** : moteur 345/345, application
~2599 passed. Détail : `JOURNAL_CONTROLES.md` et `JOURNAL_ANOMALIES.md`.

**Mis à jour 2026-08-22 (fiabilisation phase 1)** : tag local `ZERO_EXCEL_SQLITE_BASELINE_2026-08-22`
posé sur `4dfd3db` (non poussé). Migrations 0055/0056 : FK réelles (`banque_classifications`→
`banque_mouvements`, `factures_proprietaires_lignes`→`factures_proprietaires`) et CHECK de domaine
fermé sur `factures_proprietaires`/`factures_proprietaires_lignes`, ajoutés via recréation de
table (SQLite n'autorise pas `ALTER TABLE ADD CONSTRAINT`). Une erreur d'audit initiale (CHECK sur
`banque_mouvements.sens` cassant la détection volontaire d'anomalie
`test_sens_incoherent_detecte`) trouvée par la campagne complète et corrigée par une migration de
correction (0056), 0055 non modifiée. 4 contrats de données typés (dataclasses)
`app/contrats_donnees.py` : `Charge`, `ReservationHH`, `MouvementBanque`,
`MouvementTresorerieProprietaire` — définis et testés, délibérément non câblés dans les services
de saisie existants (risque de régression identifié, câblage différé). Aucune règle métier
modifiée, aucun écart financier. Baseline confirmée 0 failed après correctifs. Détail complet :
`DURCISSEMENT_SQLITE_CONTRATS_DONNEES.md`.

**Mis à jour 2026-08-22 (industrialisation socle technique)** : migration 0057 —
`sauvegardes_base` (sauvegarde de `app.db` elle-même : checkpoint WAL, copie, sha256,
`git_commit`, `integrity_check`) et `run_history` (historique centralisé, statuts STARTED/
VALIDATING/SUCCESS/FAILED/ROLLED_BACK). Nouveaux services `backup_service.py`,
`run_history_service.py`, `migration_service.py` (point d'entrée protégé
`migrer_avec_sauvegarde()` : sauvegarde → migration → contrôle d'intégrité → validation, ou
restauration automatique en cas d'échec). `apply_migrations()` elle-même non modifiée (appelée
par des centaines de tests, y ajouter une sauvegarde automatique aurait ralenti toute la suite
sans bénéfice sur une base jetable). Écran observabilité `/observabilite/runs` (lecture seule).
Le mécanisme CURRENT/CANDIDATE (dataset actif) existait déjà (`lot10_runs`/`lot12_runs`), non
reconstruit. Détail complet : `INDUSTRIALISATION_SOCLE_TECHNIQUE.md`.

**Mis à jour 2026-08-23 (orchestrateur global)** : `orchestrateur_service.actualiser()` (déjà
existant — DAG, propagation de fraîcheur, `moteur_runs`/`moteur_run_etapes`) câblé au socle de la
mission précédente. Sur une actualisation GLOBALE réelle (`cibles=None`, jamais sur une cible
unique) : `backup_service.sauvegarder()` avant le premier dataset, run journalisé en parallèle
dans `run_history` (registres existants non remplacés). Après le run, `PRAGMA integrity_check` —
seule panne qu'aucun état de dataset ne représente honnêtement ; si elle échoue, restauration
automatique de la sauvegarde prise au départ et `ROLLED_BACK`. Un échec PARTIEL normal (un dataset
en erreur, les autres réussissent) ne déclenche jamais de restauration — les données précédentes
restent en place par construction, aucun dataset n'étant écrasé avant son propre succès. Mode
`dry_run=True` ajouté : calcule le même plan, n'exécute et n'active rien, aucune sauvegarde
prise — nouvelle route `/actualisation/tout/dry-run` + bouton « Simuler (dry-run) ». Isolation de
test : `cfg.BACKUPS_DIR` désormais patché par le fixture `tmp_db` partagé (comme `cfg.DB_PATH`) —
tout test appelant une actualisation globale est protégé sans configuration supplémentaire.
Détail complet : `ORCHESTRATEUR_GLOBAL_ACTUALISATION.md`.

**Mis à jour 2026-08-23 (scheduler Hostaway)** : `ordonnanceur_service.py` (cadence 5h Hostaway /
24h CleaningTasks, verrou/état partagés avec l'orchestrateur, appelle le MÊME service que le
bouton manuel) **existait déjà**, tout comme `hostaway_actualisation_service.py::actualiser()`
(point d'entrée unique). Trois manques comblés, sans rien reconstruire : câblage démarrage/arrêt
avec `app/main.py::lifespan` (le service n'était jamais lancé), `run_history` pour Hostaway
(chemin synchrone uniquement), cadence rendue configurable (`cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS`/
`cfg.HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS`). Aucune sauvegarde `app.db` avant un tick de routine
(déjà le comportement de l'orchestrateur pour une cible unique — panne API = non-activation du
dataset, jamais une restauration complète). Scheduler réel toujours INACTIF
(`ORDONNANCEUR_ACTIF=False` par défaut, non modifié). Détail complet : `SCHEDULER_HOSTAWAY.md`.

**Mis à jour 2026-08-23 (référentiels administrables)** : l'administration des référentiels
(`referentiel_admin_service.py`, `logements_gestion_service.py`, `fournisseurs_referentiel_
service.py`, écran `/administration/referentiels`) **existait déjà**, historisée et journalisée
(`ref_admin_evenements`). Cinq manques comblés, sans deuxième système créé : (1) `referentiel_
admin_service.transaction()` rend atomiques les séquences clôture+ouverture (`archiver`/
`reactiver`/`changer_proprietaire`/`changer_taux_commission`) — un échec de l'ouverture annule
désormais la clôture déjà faite ; (2) `ref_couts_standards_menage` a rejoint les tables historisées
(nouveau `couts_menage_gestion_service.py`, grain `type_logement_id`, colonnes `*_validite` — le
moteur `lot6f_cout_complet_menages.py` les consommait déjà de façon historisée en lecture, seule
l'écriture manquait de discipline) ; (3) refus d'une nouvelle période dont le début chevauche une
période déjà close du même grain ; (4) désactiver un propriétaire encore rattaché à un logement
actif est refusé (`V10_PROPRIETAIRE_LOGEMENT_ACTIF`) ; (5) lien de navigation ajouté vers
`/referentiel-fournisseurs`, jusque-là sans accès menu. Aucune migration, aucune règle de calcul
modifiée. Détail complet : `REFERENTIELS_ADMIN_SQLITE.md`.

**Mis à jour 2026-08-23 (moteurs métier purs, phase 1)** : expérience contrôlée sur UN SEUL moteur
pilote, `compte_proprietaire_service.calculer_fifo` — déjà pure, déjà testée, mais réutilisée par
un AUTRE domaine (`intervenant_menage_compte_service.py`) via un import direct depuis le service
propriétaire (couplage cross-domaine réel). Déplacée telle quelle (0 ligne de logique modifiée)
vers `app/moteurs/fifo_engine.py`, module sans aucune dépendance applicative (pas de SQLite, pas
de FastAPI, pas de filesystem, pas d'environnement) ; `compte_proprietaire_service.py` ré-exporte
`calculer_fifo`/`TOLERANCE` pour compatibilité (0 régression) ; `intervenant_menage_compte_
service.py` importe désormais depuis le moteur neutre, plus depuis le service d'un autre domaine.
9 nouveaux tests unitaires purs (`test_fifo_engine.py`, sans DB ni fixture) en plus des 24 tests
existants (`test_compte_proprietaire_fifo.py`, inchangés, verts via le ré-export). Aucune règle
métier modifiée, aucune migration. Lot10/Lot11 (candidats C, trop couplés pandas+SQL+calcul) non
touchés. Détail complet : `MOTEURS_METIER_PURS_PHASE1.md`.

**Mis à jour 2026-08-23 (règles métier temporelles)** : audit confirme taux commission/gestion
logement↔propriétaire/coût ménage standard déjà historisés et résolus par date (`lib_ref_history.
resolve_commission_rate`/`resolve_management_period`, `lot6f::date_aware`), fail-closed. Manque
réel comblé : le paramètre canapé (seuil/montant), jusque-là valeur COURANTE sur `ref_logements`
sans période, lu sans filtre de date par `lot10_calculer_resultats.py`. Nouvelle table historisée
`ref_canape_parametres` (migration 0058, backfill préservant le comportement actuel), résolveur
`lib_ref_history.resolve_canape_parametres` (fail-closed), service `canape_gestion_service.py`
(clôture+ouverture atomique), écran dédié sur `/administration/referentiels` existant. `Lot10`
résout désormais le paramètre canapé à la date de la réservation (repli sur la colonne courante
si aucune base fournie — zéro régression pour les appels existants). **Correction de cadrage
(voir mise à jour Mission 6 bis ci-dessous)** : il n'existe pas de « groupe de logements »
permanent — la vraie règle (répartition de charge par périmètre de facture) est documentée plus
bas. Assiette de commission et formule canapé : non versionnées (une seule implémentation a
toujours existé). Détail complet : `REGLES_METIER_TEMPORELLES.md`.

**Mis à jour 2026-08-24 (Mission 6 bis — finalisation socle temporel)** : **correction de cadrage
importante** — la mention « groupes de logements historisés » des entrées précédentes était
trompeuse. Il n'existe AUCUN groupe permanent de logements. La vraie règle, retrouvée dans
`charges_impact_service.py` (préexistant, non modifié) : le périmètre d'une charge non directement
attribuable vient des logements sélectionnés à SA création (sélection directe et/ou logements actifs
d'un propriétaire, résolus au mois de la charge via `gestion_active_pour_mois`, déjà daté) ;
répartition `repartir_egal` (parts égales, arrondi centime déterministe). Nouveau : versionnement des
RÈGLES algorithmiques (`ref_regles_versions`, migration 0059) — distinct d'une simple variable :
`rule_code`+`version`+période, backfill V1 pour `ASSIETTE_COMMISSION`/`REGLE_REPARTITION_CHARGE_
COMMUNE`/`CANAPE_FORMULE`, aucune V2 réelle introduite. `resolve_regle_version`/`resolve_parametre_
general` ajoutés à `lib_ref_history.py`. `TAUX_HORAIRE_MENAGE_INTERNE` (`ref_parametres_generaux`,
seul consommateur réel trouvé, `lot6e_gainperte_menages.py`) résout désormais par date au lieu du
premier match par nom. Bug trouvé et corrigé pendant la campagne : backfill 0059 utilisait `INSERT`
littéral (pas conditionné par `SELECT` comme 0058) — rejeu brut de la migration violait la contrainte
PRIMARY KEY ; corrigé en `INSERT OR IGNORE` avant tout commit. Campagne finale : moteur 362/362,
application 2717/0 failed (10 lots). Limites : invalidation DAG non revalidée pour ce référentiel,
impact preview non construit, bandeau correction-rétroactive dédié non construit (mécanisme
générique existant suffit pour empêcher l'écrasement silencieux). Détail complet :
`REGLES_METIER_TEMPORELLES.md` §9.

**Mis à jour 2026-08-24 (Mission 6 ter — fermeture socle temporel en production)** : les 3 règles
versionnées (Mission 6 bis) étaient déclarées mais jamais consommées. Câblé maintenant : `lot10_
calculer_resultats.py` résout `ASSIETTE_COMMISSION`/`CANAPE_FORMULE` à la date de chaque réservation
avant calcul (BLOQUANT/`A_CONTROLER` si version indisponible, formule V1 strictement inchangée) ;
`charges_preview_service.compute_guidee` résout `REGLE_REPARTITION_CHARGE_COMMUNE` au mois de la
charge avant `repartir_egal` (refus `V27` sinon). Vérification stricte du périmètre facture (§9/§10
de la mission) : confirmé que le mode « propriétaire » du formulaire « Nouvelle charge » est un
raccourci de sélection intentionnel (deux champs multi-select sur le même formulaire), pas une
fuite de périmètre — aucune correction nécessaire. Invalidation DAG enfin câblée :
`orchestrateur_service.invalider_descendants()` existait sans aucun appelant ; nouveau
`referentiel_admin_service.invalider_dag_referentiel()` appelé par les 4 services d'écriture
temporelle, prouvé par test (modification → descendants `A_RECALCULER`, `LOT13_EXPORT` jamais
touché, aucun recalcul réel déclenché). Non construits, déclarés honnêtement : impact preview,
bandeau de correction rétroactive avec justification obligatoire (tenté puis écarté — risque de
régression sur des tests existants utilisant des dates de fixture passées sans justification).
Campagne : moteur 369/369, application 2725/0 failed. app.db réelle inchangée. Détail complet :
`REGLES_METIER_TEMPORELLES.md` §10.

**Mis à jour 2026-08-25 (Mission 6 quater — administration temporelle finalisée)** : les deux
manques de la mission 6 ter comblés. Correction rétroactive (date passée/aujourd'hui) distinguée du
changement normal (date future) : justification obligatoire, contrôlée côté route (backend),
journalisée sous l'action `CORRECTION_RETROACTIVE` — bandeau `⚠ MODIFICATION RÉTROACTIVE` en JS de
confort uniquement. Nouveau `impact_preview_service.py` : comptages structurels (réservations/
factures concernées + datasets aval du DAG existant), jamais un montant financier inventé. Bug
journal trouvé et corrigé : `inserer()` a un paramètre `commentaire=` distinct de la clé
`valeurs["commentaire"]`, les deux doivent être renseignés pour que la justification apparaisse dans
`ref_admin_evenements`. Aucune migration nouvelle. Campagne : moteur 369/369, application 2757/0
failed. app.db réelle inchangée. Détail complet : `REGLES_METIER_TEMPORELLES.md` §11. Mission 6
quater stoppe ici explicitement — pas de moteur Commission pur commencé.

**Mis à jour 2026-08-25 (Mission 7 — moteur Commission pur)** : extraction réussie. Nouveau
`02_TRAVAIL/lib_commission_engine.py` (2 fonctions pures, aucun import pandas/sqlite3/fastapi) :
`calculer_commission_conciergerie(assiette, taux)` et `calculer_net_proprietaire(payout, menage,
commission)` — formule inchangée, dupliquée 3 fois avant (HOSTAWAY/HH/VRBO), maintenant une seule
source appelée 3 fois. Résolution taux/assiette/version par date économique reste dans Lot10
(inchangée, Mission 6 ter). Parité prouvée par construction + tests ciblés (14 nouveaux :
`test_commission_engine.py`, `test_lot10_commission_moteur_pur.py`) — pas rejouée sur copie
complète du pipeline réel, jugé non nécessaire (extraction strictement mécanique). Écart
économique 0,00€. Campagne : moteur 383/383, application 2758/0 failed. app.db réelle inchangée.
Détail complet : `MOTEUR_COMMISSION.md`. Mission 7 stoppe ici explicitement — pas de moteur Charges
commencé.

**Mis à jour 2026-08-25 (Mission 7 bis — fermeture moteur Commission)** : audit de l'assiette
restante dans Lot10 → verdict **MIXTE**. HOSTAWAY = pur pass-through technique (0 décision, reste
dans Lot10). HH/VRBO = vraie formule métier (`payout - menage`), désormais extraite dans
`lib_commission_engine.assiette_v1_paiement_direct` (partagée par les deux canaux, même règle
économique). Préférence de source VRBO (historique résolu vs calculé) reste une décision technique
de réconciliation dans Lot10, documentée comme telle. Preuve A/B réelle ajoutée (15 réservations,
3 canaux, 2 taux, `test_ab_moteur_commission.py`) : 0 diff ligne à ligne, 0 diff agrégat, écart
0,00€. Piège de rounding `round()` vs `pandas.Series.round()` découvert et documenté (comportement
préexistant, pas un bug introduit). Campagne : moteur 392/0 failed, application 2758/0 failed.
Détail complet : `MOTEUR_COMMISSION.md`. Mission 7 bis stoppe ici explicitement — pas de moteur
Charges commencé.

**Mis à jour 2026-08-26 (Mission 8 — moteur Charges pur)** : audit de la chaîne Charges →
`app/services/charges_impact_service.py` était DÉJÀ un moteur pur (0 sqlite3/FastAPI/pandas/
fichier, un seul appelant en production, déjà validé Mission 6 bis/6 ter). Relocalisé vers
`app/moteurs/charges_engine.py` (même pattern que `fifo_engine.py`, Mission 5) — aucune ligne de
logique modifiée, `charges_impact_service.py` devient un ré-export. Règle métier confirmée
inchangée : pas de groupe permanent, périmètre = la charge, affectation directe = cas n=1 de
`repartir_egal` (pas un second mécanisme). Preuve A/B (fixtures représentatives, 8 tests) : 0 diff
production vs moteur direct. Temporalité V1/V2 testée. Campagne : moteur 392/0 failed (inchangé,
mission hors 02_TRAVAIL), application 2766/0 failed. Détail complet : `MOTEUR_CHARGES.md`. Mission
8 stoppe ici explicitement — pas d'autre extraction de moteur commencée.

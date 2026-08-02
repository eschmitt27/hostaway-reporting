# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| État figé le | **2026-08-02** — Dossier de préparation de la validation humaine et du mode réel **LIVRÉ, mode réel NON activé**. Sécurité : fuite réelle de chemin absolu dans le bandeau MODE RECETTE trouvée et corrigée (`c65f891`, test rouge→vert). Package complet de validation humaine créé (`66`-`72` : baseline figée, plan de validation, matrices d'arbitrage Banque et Comptabilité, guide utilisateur, dossier de préparation du mode réel en 5 phases documentées mais non exécutées, checklist GO/NO-GO). Verdict : **NO GO — VALIDATION HUMAINE REQUISE** |
| Dernier commit stable avant ce tour | `dbab903` — `Validation humaine Banque - decisions groupe 4 lignes 31 a 40` |
| Session interactive de validation humaine (2026-08-02, en cours) | Conduite mouvement par mouvement du groupe 4 Banque (55 mouvements CLASSE/risque ÉLEVÉ/DEBIT, sous-ensemble des 517 `A_CONTROLER`). **Groupe 4 CLOS : 55/55 lignes traitées**, toutes `REPORTER AVEC CATÉGORIE CANDIDATE`, toutes restent `A_CONTROLER`, aucune validée définitivement. Synthèse par famille : ASSOCIE_A (12, 8719,00 €), IK (6, 3100,00 €), effets domiciliés (4, 5166,59 €), factures fournisseurs (10, 3921,00 €), gestes commerciaux (3, 988,14 €), ménages candidat (7, 5681,75 €), rémunération à identifier (3, 1144,30 €), remboursement associé (3, 3000,00 €), flux mixtes à décomposer (2, 4259,00 €), autres non identifiés (5, 2606,07 €) — total 55 lignes, 38585,85 €, au centime près identique au total engine. Journal détaillé : `73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md` (DEC-001 à DEC-005). Overlay de contrôle hors Git (`_RECETTES_GLOBALES/.../RAPPORTS/OVERLAY_DECISIONS_BANQUE_GROUPES_1A5.csv`, 212 lignes, 55/55 lignes du groupe 4 avec catégorie candidate). **Aucune règle moteur modifiée, aucune écriture dans `BANQUE_LOT8_IMPORT.xlsx` ni `app.db`, aucune application réelle, aucun rapprochement confirmé.** Prochaine action : groupe 5 (19 mouvements CLASSE/risque ÉLEVÉ/CREDIT), puis les 305 mouvements restants (222 RAPPROCHEMENT_REQUIS + 83 A_ENVOYER_IA). Process historique du port 8000 (PID 21136, antérieur à cette série de missions) laissé intact sur demande explicite de l'utilisateur. |
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
[OK] Source brute : ...SOURCES_COPIEES\01_SOURCES_BRUTES\Banque\2026_03_BRUT_Banque_CreditMutuel.xlsx
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
`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`. Conformément à l'étape 1 de la
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
utilisateur), produite depuis `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`
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
moteur sans aucun `intervenant_id` nul. L'alias réel documenté « Kira = Kheira » (D104) est
externalisé dans `02_TRAVAIL/_data_lot6b_alias_reel.py`, module optionnel jamais copié vers
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
(factures Aissata / Mounir, heures Imène / Kheira), avec l'autre méthode.

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

## État de reprise

Worktree propre, suite complète verte (hors flake pré-existant), quatre modules documentés. La
prochaine session peut démarrer directement sur le Bloc 1 ci-dessus.

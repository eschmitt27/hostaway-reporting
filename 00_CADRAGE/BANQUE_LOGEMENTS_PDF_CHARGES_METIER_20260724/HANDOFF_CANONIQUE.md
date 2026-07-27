# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| Dernier commit stable | `25bde6d` — `fix(menages): sorties declarees, lot6c ajoute, dernier verrou aligne` |
| Commits de ce tour | `053f195` (lot13) · `88f2337` (Charges) · `25bde6d` (Ménages) |
| HEAD | vérifier avec `git log -1` — ce fichier est mis à jour par le commit qui le porte |
| git status | propre (`data_recette/` ignoré, régénérable) |
| master / canonique | **intacts, jamais touchés** (`master` = `8b47807`) |
| Sources réelles | inchangées, **une exception assumée** : `02_TRAVAIL/lot13_export_powerbi.py`, sur décision utilisateur explicite (renommage de la colonne d'export). Aucune donnée réelle touchée ; toutes les écritures de recette restent sous `data_recette/` |
| Campagnes ciblées de ce tour | `test_lot13_filet_anti_sensible` **11 passés** · `test_charges_pipeline` **19 passés** · `test_calculs_executeur` **21 passés** · `-k "calculs or lot13 or charges_pipeline or menages_chaine"` **103 passés / 10 skipés** · `test_recette_scenarios` **1 passé** · tests moteur racine (interpréteur pandas) **31 passés** |
| Suite complète | **1967 passés / 75 skipés / 1 échec pré-existant** (`test_appsec1_diagnostic`). Exécutée en **4 tranches** (507+17 · 508+8 · 578+6 · 374+44) car la suite d'un seul tenant dépasse le délai d'exécution disponible : `python -m pytest -q -p no:cacheprovider $(ls tests/test_*.py \| awk 'NR%4==1')`, puis `NR%4==2`, `==3`, `==0` |

## Modules

| Module | Statut | Documents |
|---|---|---|
| Logements | **TERMINÉ** | `28`, `29` |
| Banque (import, rapprochement, suggestions, contrôles) | **TERMINÉ** | `30`, `31` |
| Fournisseurs / Factures / Règlements | **TERMINÉ** | `32`, `33`, `34` |
| Pilotage des calculs & clôture | **TERMINÉ** — chaîne aval complète `lot4quater → lot13` | `35`, `36`, `37`, `38` |
| Charges | **chaîne exercée depuis `/calculs`**, scénarios A→F réconciliés | `27`, `39` |
| Ménages | **PARTIEL** — chaîne 7/7 verte ; cycle de vie opérationnel non construit | `40`, `41` |

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
| Modules terminés | Logements, Banque, Fournisseurs-Factures-Règlements, Pilotage des calculs (chaîne aval complète), **chaîne Charges exercée** |
| Module actif | *(phase 3 : module Ménages)* |

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

## ⛔ ARBITRAGE EN ATTENTE — pivot du coût de ménage interne

Une consigne du 2026-07-27 demandait de déplacer le pivot au **1er mai 2026**, au motif que le
moteur aurait dérivé au 1er juin. **Vérification faite : le moteur est conforme.**

`DECISIONS_METIER.md` → **D101 — Méthode interne selon période**, VALIDÉ le 2026-06-18 :

> Pivot **2026-06**. ≤ 2026-05 : `INTERNE_HEURES_M04` = nb_heures × taux horaire (PARAM_004).
> ≥ 2026-06 : `INTERNE_STANDARD_PARAMETRE` = nb_menages × forfait `REF_Couts_Menage_Interne`.

`lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01` applique exactement D101.

**Le pivot n'a donc PAS été modifié.** Avancer au 1er mai recalculerait **mai 2026** — le mois qui
porte les données réelles (factures Aissata / Mounir, heures Imène / Kheira) — avec l'autre méthode,
ce que la consigne elle-même interdit (« vérifier qu'aucun mois passé n'est recalculé avec une
mauvaise règle »).

Deux issues possibles, à trancher explicitement :
1. **D101 reste la règle** → rien à faire, le comportement est déjà juste et désormais verrouillé
   par `tests/test_menages_pivot_historique.py` (9 tests, dont les 4 frontières demandées).
2. **D101 est révisée** → il faut modifier `PIVOT_FIXED_COST`, amender D101 (nouvelle décision
   datée, pas une réécriture), recalculer mai 2026 et mesurer l'écart avant/après.

Tant que ce n'est pas tranché, aucun écran ni calcul ne doit présenter une règle contredisant D101.

## Prochaine action précise

1. **Trancher l'arbitrage du pivot** ci-dessus. C'est bloquant pour tout écran de tarif ménage.
2. **Cycle de vie opérationnel Ménages** — le gros du travail restant, dans cet ordre :
   modèle SQLite du ménage unitaire + statuts (auditer d'abord les statuts réellement utilisés,
   ne pas créer une seconde norme) → qualification prestataires **sur le référentiel Fournisseurs**
   (jamais une table concurrente) → services → écrans et rattachements facture/charge/règlement/
   banque → catalogue de contrôles sur le modèle de `/factures/controles` → jeu de recette →
   recette navigateur → pipeline.
3. **Alimenter les pools de courses** du jeu de recette, pour exercer réellement la quote-part
   (mécanisme présent dans lot6f, pools vides aujourd'hui). Garder distinguables : pool vide valide,
   source absente, source illisible, courses non ventilées.
4. Le **mode réel** reste à activer sur décision explicite (`CALCULS_REAL_RUN_ENABLED`,
   `MENAGES_REAL_RECALC_ENABLED`) — garde-fous en place, jamais activés.
5. Non construit, signalé : « charge postérieure à une clôture validée » n'est interdit par aucun
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

## État de reprise

Worktree propre, suite complète verte (hors flake pré-existant), quatre modules documentés. La
prochaine session peut démarrer directement sur le Bloc 1 ci-dessus.

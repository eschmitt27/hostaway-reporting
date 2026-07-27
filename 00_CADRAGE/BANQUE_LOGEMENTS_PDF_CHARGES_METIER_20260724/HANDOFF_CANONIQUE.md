# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| Dernier commit de contenu | `7775097` — `docs(calculs): chaine aval executee de bout en bout, et defaut moteur lot13` |
| HEAD | ce fichier est mis à jour par le commit **suivant** (`docs(handoff): …`), dont le SHA ne peut pas figurer dans son propre contenu — vérifier avec `git log -1` |
| git status | propre (`data_recette/` ignoré, régénérable) |
| master / canonique | **intacts, jamais touchés** (`master` = `8b47807`) |
| Sources réelles | **jamais modifiées** — toutes les écritures de recette sous `data_recette/` |
| Suite complète | **1948 passés / 65 skipés / 1 xfail attendu / 1 échec pré-existant** (21 min 43) |

## Modules

| Module | Statut | Documents |
|---|---|---|
| Logements | **TERMINÉ** | `28`, `29` |
| Banque (import, rapprochement, suggestions, contrôles) | **TERMINÉ** | `30`, `31` |
| Fournisseurs / Factures / Règlements | **TERMINÉ** | `32`, `33`, `34` |
| Pilotage des calculs & clôture | **TERMINÉ côté applicatif** — reste un défaut *moteur* sur lot13 | `35`, `36`, `37` |
| Charges | Antérieur, non retouché | `27` |

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

## ⛔ Défaut moteur ouvert : lot13 échoue systématiquement

Première exécution de la chaîne assez loin pour atteindre lot13 :

```
[BLOQUANT lot13] colonnes sensibles dans des exports :
   PBI_Commissions: colonne sensible détectée ['preparation_canape_voyageurs']
```

`lot13_export_powerbi.py` se contredit : sa whitelist `PBI_Commissions` contient
`preparation_canape_voyageurs`, que son propre filet anti-sensible interdit (motif `voyageur`).
**Statique, donc valable aussi en mode réel.** Postérieur au commit `8763676`.

Moteur **non modifié** (règle du chantier). Le défaut est tenu par
`tests/test_lot13_filet_anti_sensible.py` en `xfail(strict=True)` : le test échouera dès la
correction du moteur, forçant à retirer le garde-fou.

Décision métier requise, au choix : retirer la colonne de la whitelist / la renommer (elle ne porte
qu'un montant) / restreindre le motif `voyageur` aux colonnes nominatives.

## Prochaine action précise

1. **Trancher le défaut lot13** ci-dessus (hors périmètre applicatif — décision sur le moteur).
   Une fois corrigé : relancer la chaîne complète avec lot13 coché, le run doit passer SUCCES et
   le test `xfail` doit être supprimé.
2. Exercer les chaînes **`charges`** et **`menages`** en recette (déclarées et lançables, jamais
   exécutées).
3. Le **mode réel** du pilotage reste à activer sur décision explicite
   (`CALCULS_REAL_RUN_ENABLED`) — garde-fous en place, jamais activé.

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
python -m pytest -q tests/ -k "calculs or lot13"               # 69 passés + 1 xfail attendu
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

# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| HEAD | `685b38a` — `feat(calculs): routes, interface et recette de pilotage` |
| git status | propre (`data_recette/` ignoré, régénérable) |
| master / canonique | **intacts, jamais touchés** (`master` = `8b47807`) |
| Sources réelles | **jamais modifiées** — toutes les écritures de recette sous `data_recette/` |
| Suite complète | **1939 passés / 65 skipés / 1 échec pré-existant** (~32 min) |

## Modules

| Module | Statut | Documents |
|---|---|---|
| Logements | **TERMINÉ** | `28`, `29` |
| Banque (import, rapprochement, suggestions, contrôles) | **TERMINÉ** | `30`, `31` |
| Fournisseurs / Factures / Règlements | **TERMINÉ** | `32`, `33`, `34` |
| Pilotage des calculs & clôture | **PARTIEL** | `35`, `36` |
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

## Anomalie de test connue (pré-existante, hors périmètre)

`tests/test_appsec1_diagnostic.py::test_07_diagnostic_local_avec_flag_explicite` : le nom
d'utilisateur Windows apparaît dans un chemin temporaire pytest, ce que le test interdit.
**Antérieure à tout ce chantier.** Ne pas la confondre avec une régression.

## Prochaine action précise

**Bloc 1 — rendre la chaîne aval verte en recette** (condition nécessaire au reste) :
`lot4quater` échoue (code 1) faute de sources de réservations dans `data_recette`. Il manque :
- `02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx`
- `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`

`recette/build_reservations_recette.py` existe déjà — vérifier ce qu'il produit et le brancher dans
`build_data_recette.py`, ou seeder directement ces deux fichiers. Objectif : un run
`lot4quater → … → lot13` en **SUCCES**, ce qui débloque ensuite :

**Bloc 2** — comparaison avant/après alimentée par deux runs réussis (le mécanisme est construit et
testé, jamais exercé sur de vraies données).

**Bloc 3** — atteindre la clôture `VALIDEE` de bout en bout (aujourd'hui refusée à juste titre).

**Bloc 4** — bouton « rejouer un lot » isolé (`executer_lot()` le permet déjà côté service, aucun
écran ne l'expose).

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current          # feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"        # doit afficher 685b38a ...
git status --porcelain             # doit être vide

# Régénérer le jeu de recette (idempotent, écrase data_recette/) :
python recette/build_data_recette.py

# Tests ciblés :
cd 05_APPLICATION
python -m pytest -q tests/ -k "banque"                        # 171 passés
python -m pytest -q tests/ -k "facture or reglement"          # 120 passés
python -m pytest -q tests/ -k "calculs"                       # 58 passés
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

## État de reprise

Worktree propre, suite complète verte (hors flake pré-existant), quatre modules documentés. La
prochaine session peut démarrer directement sur le Bloc 1 ci-dessus.

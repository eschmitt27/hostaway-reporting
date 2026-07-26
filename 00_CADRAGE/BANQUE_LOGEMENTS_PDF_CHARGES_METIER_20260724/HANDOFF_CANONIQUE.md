# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| HEAD | `7d2e0a5` — `docs: etat du module Factures, handoff canonique et avancement global` |
| Dernier commit utile | idem |
| git status | propre (`data_recette/` ignoré, régénérable) |
| master / canonique | **intacts, jamais touchés** (`master` = `8b47807`) |
| Sources réelles | **jamais modifiées** — toutes les écritures de recette sont sous `data_recette/` |

## Modules

| Module | Statut | Document de référence |
|---|---|---|
| Logements | **TERMINÉ** | `28_CYCLE_DE_VIE_LOGEMENT.md`, `29_MODULE_LOGEMENTS_COUCHE_HTTP.md` |
| Banque | **TERMINÉ** | `30_MODULE_BANQUE_IMPORT_RAPPROCHEMENT.md`, `31_MODULE_BANQUE_ETAT_FINAL.md` |
| Charges | Antérieur, non retouché ce chantier | `27_VALIDATION_CHARGES_ET_CONTROLES.md` |
| Fournisseurs / Factures / Règlements | **PARTIEL** — cœur utilisable et prouvé | `32_AUDIT_FOURNISSEURS_FACTURES.md`, `33_MODULE_FACTURES_ETAT.md` |

## Limites persistantes (volontaires, à ne pas re-auditer)

1. **Forfait logiciel/consommables historisé** — nécessite de modifier `build_charge_fixe()` dans
   `02_TRAVAIL/lot10_calculer_resultats.py` (moteur, hors `app/`). Signalé depuis
   `AUDIT_CIBLE_FORFAITS_ET_TAUX_LOGEMENTS.md`. **Jamais commencé, volontairement.**
2. **Pilotage applicatif complet de Lot9/Lot10** — `/sources-calculs` est en dry-run par conception
   (débloqué au Lot APP-5). De plus **`pandas` est absent de cet environnement** : les moteurs
   complets ne peuvent pas être exécutés ici. Les vérifications d'impact se font en reproduisant
   *verbatim* les filtres réels dans des tests (cf. `test_banques_impact_lot9.py`).
3. **OFX / caisse (Banque)** — non construits, aucun besoin métier démontré.
4. **Contrôles inter-lots** (`total Lot8 vs Lot9`, double comptage payout) — non construits :
   supposent de rejouer Lot9/Lot10, impossible ici. Documenté dans `31_MODULE_BANQUE_ETAT_FINAL.md`.

## Anomalie de test connue (pré-existante, hors périmètre)

`tests/test_appsec1_diagnostic.py::test_07_diagnostic_local_avec_flag_explicite` échoue :
le nom d'utilisateur Windows (« Ewan ») apparaît dans un chemin temporaire pytest, ce que le test
interdit. **Antérieure à ce chantier**, indépendante des modules construits. Ne pas la confondre
avec une régression.

## Mission active

**Mission 2 — Fournisseurs, Factures et Règlements** : audit fait, modèle + services + couche HTTP
construits et prouvés en navigateur. Statut **PARTIEL** (détail exhaustif dans
`33_MODULE_FACTURES_ETAT.md`, section « Ce qui N'EST PAS construit »).

### Prochaine action précise (par ordre de valeur)

1. **Brancher le rapprochement bancaire dans les deux sens** — c'est le chaînon manquant le plus
   structurant. Tout existe déjà côté service :
   - `banques_rapprochement_service.enregistrer(...)` accepte déjà le type d'objet
     `REGLEMENT_CHARGE` ;
   - `reglements_fournisseurs_service.marquer_rapproche(reglement_opaque, mvt_opaque)` existe et est
     testé, mais n'est appelé par **aucune route**.
   À faire : bouton « Rapprocher » sur la fiche facture/règlement → réutiliser
   `banques_candidats_service` pour proposer les mouvements, puis appeler le service Banque.
   **Ne jamais créer un second moteur de rapprochement.**
2. **Fiche fournisseur enrichie** — `factures_service.solde_fournisseur()` existe et est testé,
   mais aucun écran ne l'expose. Ajouter une fiche `/referentiel-fournisseurs/{opaque}` avec
   total facturé / réglé / solde / échues / litiges + liste des factures et règlements.
3. **Import PDF de facture** — réutiliser `02_TRAVAIL/lib_menages_externes_pdf.py`
   (`extraire_pdf()`, `FactureExtraite`, `empreinte_facture()`). PyMuPDF est disponible ici.
   Attention : l'extracteur ne connaît que 2 formats fournisseur ; prévoir un repli en saisie
   manuelle pré-remplie plutôt qu'un échec.
4. **Page `/factures/controles`** sur le modèle de `/banques-caisse/controles` (le service
   `banques_controles_catalogue_service.py` est un bon patron à copier).
5. **Seed du jeu de recette** — ajouter fournisseurs + factures fictives dans
   `recette/build_data_recette.py` (aujourd'hui il ne seed que charges/banque/logements).

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current          # doit afficher feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"        # doit afficher 7d2e0a5 ...
git status --porcelain             # doit être vide

# Régénérer le jeu de recette fictif (idempotent, écrase data_recette/) :
python recette/build_data_recette.py

# Tests ciblés :
cd 05_APPLICATION
python -m pytest -q tests/ -k "banque"                    # 171 passés, 29 skipés
python -m pytest -q tests/ -k "facture or reglement"      # 49 passés
python -m pytest -q                                        # suite complète (~28 min)

# Serveur de recette (données fictives isolées, écritures autorisées uniquement sous data_recette).
# APP_DATA_DIR isole aussi la base SQLite : sans lui, les factures de recette iraient dans la vraie
# base applicative 05_APPLICATION/data/app.db.
cd 05_APPLICATION
APP_DATA_DIR="<worktree>/data_recette/app_data" \
  PROJECT_ROOT="<worktree>/data_recette" RECETTE_MODE=1 \
  BANQUE_REAL_WRITE_ENABLED=1 BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  CHARGES_REAL_WRITE_ENABLED=1 CHARGES_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  FACTURES_REAL_WRITE_ENABLED=1 FACTURES_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  python -m uvicorn app.main:app --port 8020 --host 127.0.0.1
```

## Décisions structurantes prises pendant ce chantier

1. **Une seule norme bancaire.** L'import applicatif reproduit *exactement* le schéma et les
   formules de `lot8a_banque_import.py` (26 colonnes, `ROW_HASH`, `mouvement_id`). Jamais de
   seconde norme concurrente.
2. **Excel reste la vérité métier ; SQLite journalise.** Les rapprochements, décisions de
   catégorisation et décisions sur suggestions vivent en SQLite et ne font jamais autorité sur le
   moteur. Aucune anomalie moteur n'est masquée ni réécrite.
3. **Aucune validation silencieuse.** Une suggestion, même de niveau EXACT, produit au mieux un
   rapprochement `PROPOSE` ; la confirmation reste un geste humain.
4. **Pas de relation 1-1 forcée** entre mouvement et objet : le contrôle porte sur la somme des
   liens actifs (partiel et multiple supportés nativement).
5. **Double verrou d'écriture** : `RECETTE_MODE` ET variable d'environnement dédiée, plus le
   write-guard de chemin. Une instance non-recette ne peut jamais écrire.
6. **Fiche minimale plutôt que 404** (Logements) : un objet créé par l'application mais pas encore
   repris par l'export PBI reste consultable, avec un bandeau explicite.

## État de reprise

Worktree propre, tous les tests ciblés verts, module Banque clos. Rien n'est laissé partiellement
modifié. La prochaine session peut démarrer directement sur l'audit ciblé Fournisseurs/Factures.

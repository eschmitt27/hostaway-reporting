# HANDOFF CANONIQUE — reprise immédiate

Document unique de reprise. Toute nouvelle session lit CE fichier en premier.
Mis à jour à chaque fin de phase. Ne jamais dupliquer : mettre à jour, jamais recréer à côté.

## Contexte technique

| Élément | Valeur |
|---|---|
| Worktree | `C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER` |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| HEAD | `a4d5cb4` — `feat(banque): suggestions branchees a l'interface et page de controles` |
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
| Fournisseurs / Factures / Règlements | **NON COMMENCÉ** (mission suivante) | — |

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

**Mission 2 — Fournisseurs, Factures et Règlements** (non commencée).
Prochaine action précise : **audit ciblé §7** (45 min max) → produire le tableau
`| Objet | Source de vérité actuelle | Manque | Cible |` dans un nouveau document
`32_AUDIT_FOURNISSEURS_FACTURES.md` du même dossier, puis enchaîner sur le modèle de données.

Points déjà repérés pour cet audit (à confirmer, pas à re-chercher) :
- `app/services/fournisseurs_referentiel_service.py` existe déjà et couvre **une grande partie du
  §8** : lister / charger / rechercher doublons / créer / modifier / désactiver / réactiver /
  historique, avec journal SQLite (`fournisseurs`, `fournisseur_evenements`, migration 0010).
- `app/services/fournisseur_rattachements_service.py` + migration 0013 : association historisée
  fournisseur↔logement.
- `app/services/charges_affectations_service.py` + migration 0011, et
  `proprietaires_paiement_service.py` + migration 0012 : briques de règlement déjà présentes.
- Migration 0014 (`rapprochements_reglements`) : rapprochement déclaratif règlement↔mouvement, à
  **réutiliser** plutôt que dupliquer.
- Le module Banque expose déjà `banques_rapprochement_service` (générique, multi-objets) : le
  rapprochement facture/règlement doit **réutiliser ce service**, jamais en créer un second.

## Commandes exactes de reprise

```
cd "C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Worktrees\BANQUE_LOGEMENTS_PDF_CHARGES_METIER"
git branch --show-current          # doit afficher feature/banque-logements-pdf-charges-metier
git log -1 --format="%H %s"        # doit afficher a4d5cb4 ...
git status --porcelain             # doit être vide

# Régénérer le jeu de recette fictif (idempotent, écrase data_recette/) :
python recette/build_data_recette.py

# Tests ciblés :
cd 05_APPLICATION
python -m pytest -q tests/ -k "banque"        # 171 passés, 29 skipés
python -m pytest -q                            # suite complète (~21 min)

# Serveur de recette (données fictives isolées, écritures autorisées uniquement sous data_recette) :
cd 05_APPLICATION
PROJECT_ROOT="<worktree>/data_recette" RECETTE_MODE=1 \
  BANQUE_REAL_WRITE_ENABLED=1 BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=1 \
  CHARGES_REAL_WRITE_ENABLED=1 CHARGES_REAL_WRITE_CONFIRMATION_ENABLED=1 \
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

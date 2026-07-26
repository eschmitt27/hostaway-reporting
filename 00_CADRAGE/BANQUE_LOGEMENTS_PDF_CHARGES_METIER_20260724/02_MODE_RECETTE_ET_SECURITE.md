# 02 — Mode recette et sécurité (conception)

## Principe

Un **MODE RECETTE — DONNÉES FICTIVES** activé par variable d'environnement, qui redirige TOUS les
chemins (sources Excel/CSV, base SQLite, sorties MASTER, dossier PDF) vers un dossier isolé
`data_recette/`, sans jamais pointer vers le réel.

## Leviers déjà présents (à réutiliser, pas à réinventer)

- `PROJECT_ROOT`, `APP_DATA_DIR` sont déjà surchargeables par variable d'environnement
  (`app/config.py` lit `os.environ`). Le recalcul ménages tourne déjà « sur copies » sous `data/`
  avec `MENAGES_REAL_RECALC_ENABLED=False`.
- Les flags writers (`CHARGES_/BANQUE_/CONTROLES_/MENAGES_*_REAL_*`) existent déjà et gardent toute
  écriture réelle.

## Garde-fou à ajouter (write-guard)

Un contrôle unique, appelé avant toute écriture de fichier, qui **refuse** si :
1. le mode recette n'est pas explicitement activé (`RECETTE_MODE=1`) ; **et/ou**
2. le chemin cible n'est pas sous `data_recette/` (résolution `Path.resolve()` + `is_relative_to`) ;
3. le chemin résout vers un fichier réel connu (liste noire : `01_SOURCES_BRUTES`, `02_TRAVAIL`
   réels du dépôt) ;
4. le MASTER réel risque d'être touché.

En mode recette, l'interface affiche en bandeau : **« MODE RECETTE — AUCUNE DONNÉE RÉELLE
MODIFIÉE »**.

## Où le mettre

Worktree `BANQUE_LOGEMENTS_PDF_CHARGES_METIER` uniquement (jamais canonique/master). Base SQLite
`data_recette/app.db`. Writers autorisés **exclusivement** vers `data_recette/`.

## Statut : **conçu, non implémenté** (phase build — voir checkpoint).

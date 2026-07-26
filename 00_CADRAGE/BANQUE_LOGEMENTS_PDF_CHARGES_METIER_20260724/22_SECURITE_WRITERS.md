# 22 — Sécurité des writers

## Double verrou d'activation
Les flags d'écriture charges ne peuvent être `True` que si **RECETTE_MODE est actif ET** la variable
dédiée est positionnée. Vérifié (3 modes) :
- défaut (rien) → `CHARGES_REAL_WRITE_ENABLED = False`
- **non-recette** + `CHARGES_REAL_WRITE_ENABLED=1` → **False** (le mode recette manque : refus)
- recette + `CHARGES_REAL_WRITE_ENABLED=1` → True

Donc une instance normale (canonique / master) **ne peut jamais** écrire, même variables posées.

## Write-guard (barrière de dernière ligne)
`app/recette_guard.py` appelé au **seul point de remplacement définitif** de fichier
(`_remplacer_fichier`). En mode recette, refuse toute cible hors `RECETTE_ROOT` (résolution
absolue `Path.resolve` + `relative_to`), et refuse les segments réels connus (01_SOURCES_BRUTES,
02_TRAVAIL, 03_EXPORTS) situés hors racine recette. Tests : 5/5 dédiés + 53/53 non-régression charges.

## Preuve en conditions réelles
Charge confirmée sur l'instance recette (8018) → écriture effective dans
`data_recette/01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` et régénération
`data_recette/02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx`. **`git status` sur les sources
réelles du worktree = vide** : aucune écriture n'a atteint un fichier réel.

## Moteur aval
Exécuté par un interpréteur séparé porteur de pandas (`C:\Program Files\Python312\python.exe`),
en sous-processus, `cwd = data_recette`, sys.path = `data_recette/02_TRAVAIL` (scripts copiés).

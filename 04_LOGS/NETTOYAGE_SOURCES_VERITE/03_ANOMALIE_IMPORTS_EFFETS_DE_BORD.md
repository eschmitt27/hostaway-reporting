# Anomalie imports avec effets de bord

Date : 2026-06-28 22:15:06

## Modules concernes

- `02_TRAVAIL/lot4bis_charger_reservations.py`
- `02_TRAVAIL/lot11_controles_coherence.py`
- `02_TRAVAIL/lot12_generer_factures.py`

Modules audites et deja ou rendus neutres :

- `02_TRAVAIL/lot10_calculer_resultats.py`
- `02_TRAVAIL/lot13_export_powerbi.py`

## Cause exacte

Certains scripts contenaient le corps du lot au niveau module. Un simple `import` executait donc la lecture des sources et l'ecriture des fichiers de sortie. L'anomalie a ete observee lors de l'import de `lot4bis`, `lot11` et `lot12`, qui a reecrit des sorties de travail.

## Correction appliquee

- Encapsulation des corps executables dans `main()`.
- Ajout ou conservation du garde `if __name__ == "__main__": main()`.
- Deplacement des effets d'initialisation non metier derriere `main()` lorsque necessaire (`stdout.reconfigure`, `logging.basicConfig`, warnings/stdout Lot13).
- Aucune logique metier de calcul n'a ete transformee en bibliotheque complexe.

## Sorties restaurees

Restauration depuis `HEAD` uniquement pour :

- `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx`
- `02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx`

## Tests ajoutes ou adaptes

- Ajout de `tests/test_import_side_effects.py`.
- Verification statique : presence de `main()`, garde `__main__`, absence d'appel `main()` hors garde, absence d'ecriture Excel top-level.

## Preuve qu'aucun lot ne demarre a l'import

- Compilation syntaxique des cinq scripts : OK.
- Suite unittest : `Ran 51 tests ... OK`.
- Controle d'import dans une copie temporaire isolee : les cinq modules s'importent, sortie `XLSX_CREATED []`, aucun fichier Excel cree.

## Risques restants

- Les scripts restent des scripts de lot ; l'execution directe lance toujours le traitement complet, ce qui est le comportement attendu.
- Les imports ont ete verifies sur les modules listes, pas sur tous les scripts historiques du projet.

# Revue finale avant commit - sources uniques et imports neutres

Date : 2026-06-28 22:19:15

## Perimetre

Lot couvert : suppression des doubles sources de verite et suppression des effets de bord a l'import.

## Resultats controles

- Sorties generees modifiees : aucune sortie generee suivie n'est modifiee dans `git status`.
- References actives `REF_Logements.proprietaire_id` : aucune.
- References actives `REF_Proprietaires.taux_commission` : aucune.
- Fallbacks legacy (`taux_commission_TRANSITOIRE`, `PROPRIETAIRE_ACTIF_SANS_TAUX`, `df_prop_sel`) : aucun.
- Import des lots `lot4bis`, `lot10`, `lot11`, `lot12`, `lot13` en copie temporaire : OK, aucun `.xlsx` cree.
- Formules `#REF!` dans `REF_Setup.xlsm` : aucune detectee.
- `xl/vbaProject.bin` present : OK.
- `REF_Setup.xlsm` lisible avec `keep_vba=True` : OK.
- Documents de cadrage : sources uniques documentees.

## Excel modifies

### `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`

- Feuille modifiee : `README`.
- Cellule modifiee : `A19`.
- Avant : `5. taux_commission : pre-rempli via VLOOKUP depuis REF_Proprietaires (col N).`
- Apres : `5. taux_commission : resolu en aval depuis REF_Taux_Commission ; ne pas saisir de taux non date.`
- Donnee metier modifiee : non.
- Formule/table/validation/structure modifiee : non.

### `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`

- Sortie restauree depuis `HEAD` apres l'anomalie d'import.
- Etat final : non modifiee, exclue du commit.

## Tests

Commande : `"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"`

Resultat : `Ran 51 tests in 0.241s - OK`.

## Git checks

- `git diff --check` : aucune erreur. Avertissements CRLF/LF uniquement.
- `git diff --stat` : coh?rent avec le perimetre code, tests, referentiel, documentation et README Excel.

## Fichiers a committer

- `00_CADRAGE/ARCHITECTURE_DONNEES.md`
- `00_CADRAGE/DECISIONS_METIER.md`
- `00_CADRAGE/ETAT_AVANCEMENT.md`
- `00_CADRAGE/JOURNAL_ANOMALIES.md`
- `00_CADRAGE/JOURNAL_CONTROLES.md`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
- `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`
- `02_TRAVAIL/lib_ref_history.py`
- `02_TRAVAIL/lot4bis_charger_reservations.py`
- `02_TRAVAIL/lot10_calculer_resultats.py`
- `02_TRAVAIL/lot11_controles_coherence.py`
- `02_TRAVAIL/lot12_generer_factures.py`
- `02_TRAVAIL/lot13_export_powerbi.py`
- `tests/test_ref_history.py`
- `tests/test_import_side_effects.py`

## Fichiers explicitement exclus

- `.gitignore`
- `04_LOGS/` hors rapports de travail non destines au commit
- `Conciergerie app/`
- `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx`
- `02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx`
- caches Python et fichiers temporaires eventuels

## Anomalies restantes

Aucune anomalie bloquante detectee dans le perimetre de revue. Point de vigilance : plusieurs fichiers affichent des avertissements CRLF/LF lors des commandes Git, sans erreur `diff --check`.

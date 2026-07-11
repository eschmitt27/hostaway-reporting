# Rapport suppression doubles sources de verite

Date : 2026-06-28 21:57:33
Branche cible : master

## Objectif

- `REF_Gestion_Logements_Hist` devient l'unique source officielle du proprietaire applicable a une date donnee.
- `REF_Taux_Commission` devient l'unique source officielle du taux de commission applicable a une date donnee.
- Toute valeur affichee doit etre derivee, jamais ressaisie ou stockee comme seconde source.

## Sauvegarde

- Backup avant suppression : `04_LOGS/NETTOYAGE_SOURCES_VERITE/BACKUP_AVANT_SUPPRESSION/REF_Setup_BACKUP_AVANT_SUPPRESSION_20260628_214929.xlsm`

## References cachees detectees et adaptees

- `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`, onglet `README`, cellule `A19` : ancienne mention du VLOOKUP depuis `REF_Proprietaires` remplacee par une resolution aval depuis `REF_Taux_Commission`.
- `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`, onglet `POWER_QUERY_CODE`, cellule `C278` : commentaire Power Query remplace pour indiquer `REF_Mapping_Logements` + `REF_Gestion_Logements_Hist`.
- `00_CADRAGE/ARCHITECTURE_DONNEES.md` : formules et schema mis a jour vers `REF_Taux_Commission` et `REF_Gestion_Logements_Hist`.
- `00_CADRAGE/DECISIONS_METIER.md` : D047 et D052 remplacees par les sources uniques datees.
- `00_CADRAGE/ETAT_AVANCEMENT.md` : D052 mise a jour.
- `00_CADRAGE/JOURNAL_CONTROLES.md` : ancien controle VLOOKUP non date decommissionne.
- `00_CADRAGE/JOURNAL_ANOMALIES.md` : note source de verite ajoutee.

## Colonnes supprimees

- `REF_Setup.xlsm` / feuille `REF_Logements` : suppression physique de `proprietaire_id`.
- `REF_Setup.xlsm` / feuille `REF_Proprietaires` : suppression physique de `taux_commission`.

## Adaptations code et exports

- `02_TRAVAIL/lot4bis_charger_reservations.py` : resolution proprietaire via `REF_Gestion_Logements_Hist`, sans fallback `REF_Logements`.
- `02_TRAVAIL/lot10_calculer_resultats.py` : taux resolu via `REF_Taux_Commission`, sans fallback non date `REF_Proprietaires`.
- `02_TRAVAIL/lot11_controles_coherence.py` : controles mis a jour pour bloquer les referentiels historiques absents/vides.
- `02_TRAVAIL/lot13_export_powerbi.py` : exports Power BI ajustes pour ne plus exposer les deux anciennes colonnes et ajouter les referentiels historiques.

## Controles apres suppression

- `REF_Setup.xlsm` lisible avec `keep_vba=True` : OK.
- `xl/vbaProject.bin` present : OK.
- Hash VBA conserve pendant la suppression : OK.
- Nombre de feuilles apres suppression : 27.
- `REF_Logements.proprietaire_id` absent des en-tetes et de `tbl_REF_Logements` : OK.
- `REF_Proprietaires.taux_commission` absent des en-tetes et de `tbl_REF_Proprietaires` : OK.
- Aucune formule `#REF!` detectee : OK.
- Scan texte actif : aucune reference restante aux anciennes sources exactes.
- Scan Excel actif, formules, tables, validations, noms definis : aucune reference restante aux anciennes sources exactes.

## Tests

Commande executee :

```bash
"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"
```

Resultat : `Ran 48 tests in 0.039s - OK`.

## Points de vigilance

- Les archives sous `99_ARCHIVES/` n'ont pas ete modifiees. Deux references historiques restantes ont ete identifiees : `99_ARCHIVES/AUDIT_CLEANUP_20260618/lot4bis_master_calc_reservations.py` et `99_ARCHIVES/AUDIT_CLEANUP_20260618/lot4_saisie_template.py`.
- Les dossiers proteges `.gitignore`, `Conciergerie app/` et les autres contenus de `04_LOGS/` n'ont pas ete modifies hors sauvegarde et present rapport.
- `git diff --check` : aucune erreur de whitespace ; avertissements CRLF/LF sur `JOURNAL_ANOMALIES.md` et `JOURNAL_CONTROLES.md`.

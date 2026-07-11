# 10 - Audit dates de gestion logements

Date audit : 2026-06-28 22:41:48

Aucune suppression, aucune modification de code, Excel, tests, Power Query ou cadrage. Rapport uniquement.

## Question

Comparer `REF_Logements.date_entree_gestion` / `date_sortie_gestion` avec `REF_Gestion_Logements_Hist.date_debut` / `date_fin`.

## Comparaison par logement

| Logement | REF entr?e | REF sortie | P?riodes historiques | Propri?taire(s) | Statut(s) gestion | ?cart / verdict |
|---|---|---|---|---|---|---|
| `LOG_0001` | 2026-01-01 |  | GST_LOG_0001_PROP_0001:2026-01-01?(ouverte) / PROP_0001 / ACTIF | PROP_0001 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0002` | 2026-01-01 |  | GST_LOG_0002_PROP_0002:2026-01-01?(ouverte) / PROP_0002 / ACTIF | PROP_0002 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0003` | 2026-01-01 | 2026-04-26 | GST_LOG_0003_PROP_0003:2026-01-01?2026-04-26 / PROP_0003 / INACTIF | PROP_0003 | INACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0004` | 2026-01-01 |  | GST_LOG_0004_PROP_0004:2026-01-01?(ouverte) / PROP_0004 / ACTIF | PROP_0004 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0005` | 2026-01-01 |  | GST_LOG_0005_PROP_0005:2026-01-01?(ouverte) / PROP_0005 / ACTIF | PROP_0005 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0006` | 2026-01-01 |  | GST_LOG_0006_PROP_0006:2026-01-01?(ouverte) / PROP_0006 / ACTIF | PROP_0006 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0007` | 2026-01-01 |  | GST_LOG_0007_PROP_0007:2026-01-01?(ouverte) / PROP_0007 / ACTIF | PROP_0007 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0008` | 2026-01-01 |  | GST_LOG_0008_PROP_0008:2026-01-01?(ouverte) / PROP_0008 / ACTIF | PROP_0008 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0009` | 2026-01-01 |  | GST_LOG_0009_PROP_0003:2026-01-01?(ouverte) / PROP_0003 / ACTIF | PROP_0003 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0010` | 2026-01-01 |  | GST_LOG_0010_PROP_0005:2026-01-01?(ouverte) / PROP_0005 / ACTIF | PROP_0005 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0011` | 2026-01-01 |  | GST_LOG_0011_PROP_0008:2026-01-01?(ouverte) / PROP_0008 / ACTIF | PROP_0008 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0012` | 2026-01-01 |  | GST_LOG_0012_PROP_0009:2026-01-01?(ouverte) / PROP_0009 / ACTIF | PROP_0009 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0013` | 2026-01-01 |  | GST_LOG_0013_PROP_0009:2026-01-01?(ouverte) / PROP_0009 / ACTIF | PROP_0009 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0014` | 2026-01-01 |  | GST_LOG_0014_PROP_0010:2026-01-01?(ouverte) / PROP_0010 / ACTIF | PROP_0010 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0015` | 2026-01-01 |  | GST_LOG_0015_PROP_0011:2026-01-01?(ouverte) / PROP_0011 / ACTIF | PROP_0011 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0016` | 2026-01-01 |  | GST_LOG_0016_PROP_0012:2026-01-01?(ouverte) / PROP_0012 / ACTIF | PROP_0012 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `LOG_0017` | 2026-01-01 |  | GST_LOG_0017_PROP_0008:2026-01-01?(ouverte) / PROP_0008 / ACTIF | PROP_0008 | ACTIF | OK_EQUIVALENT_OU_NON_RENSEIGNE |
| `APPARTEMENT_DIVERS` |  |  | AUCUNE |  |  | AUCUN_HISTORIQUE |
| `LOGEMENT_DIVERS` |  |  | AUCUNE |  |  | AUCUN_HISTORIQUE |

## Consommateurs d?tect?s

### R?f?rences texte

- `date_entree_gestion` :
  - `02_TRAVAIL/lot10_calculer_resultats.py:552,586,600,622`
  - `02_TRAVAIL/lot11_controles_coherence.py:804,816,819`
  - `02_TRAVAIL/lot13_export_powerbi.py:76`
  - `00_CADRAGE/ETAT_AVANCEMENT.md:515,549`
  - `00_CADRAGE/JOURNAL_CONTROLES.md:249,357,359,394`
- `date_sortie_gestion` :
  - `02_TRAVAIL/lot10_calculer_resultats.py:551,579`
  - `02_TRAVAIL/lot13_export_powerbi.py:76`
  - `02_TRAVAIL/lot4bis_charger_reservations.py:210,225,254,256`
  - `02_TRAVAIL/lot6c_menages_externes.py:77`
  - `00_CADRAGE/ETAT_AVANCEMENT.md:25`
  - `00_CADRAGE/JOURNAL_CONTROLES.md:373,1036,1038`

### R?f?rences Excel / formules / tables / validations / noms d?finis

- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm::REF_Logements!H1=date_entree_gestion`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm::REF_Logements!I1=date_sortie_gestion`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm::REF_Logements table tbl_REF_Logements col date_entree_gestion`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm::REF_Logements table tbl_REF_Logements col date_sortie_gestion`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G6=Logement LOG_0001: premier mois Flux=2025-07 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L6=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G7=Logement LOG_0002: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L7=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G8=Logement LOG_0003: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L8=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G9=Logement LOG_0004: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L9=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G10=Logement LOG_0006: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L10=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G11=Logement LOG_0007: premier mois Flux=2025-03 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L11=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G12=Logement LOG_0008: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L12=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G13=Logement LOG_0010: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L13=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G14=Logement LOG_0011: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L14=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G15=Logement LOG_0012: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L15=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G16=Logement LOG_0013: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L16=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G17=Logement LOG_0014: premier mois Flux=2025-01 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L17=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G18=Logement LOG_0015: premier mois Flux=2025-02 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L18=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!G19=Logement LOG_0016: premier mois Flux=2025-05 < date_entree_gestion=2026-01. Utilisation Option A (premier mois Flux) con`
- `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx::MASTER!L19=INFO justifiee — date_entree_gestion referentielle incoherente, mais calcul forfait securise par D-LOT10-04 via premier `

## R?solution des r?servations avec historique seul

- `02_TRAVAIL/lib_ref_history.py:resolve_management_period:130`
- `02_TRAVAIL/lib_ref_history.py:date_debut:103,156,161,179`
- `02_TRAVAIL/lib_ref_history.py:date_fin:103,156,158`
- `02_TRAVAIL/lot4bis_charger_reservations.py:REF_Gestion_Logements_Hist:`
- `02_TRAVAIL/lot4bis_charger_reservations.py:resolve_management_period:43,259,338`

Constat : la r?solution propri?taire/p?riode utilis?e par les scripts r?cents passe par `REF_Gestion_Logements_Hist` et `resolve_management_period`. La r?gle date_debut vide non bloquante est port?e par cette biblioth?que.

## ?carts mat?riels

- `APPARTEMENT_DIVERS` : AUCUN_HISTORIQUE
- `LOGEMENT_DIVERS` : AUCUN_HISTORIQUE

## Option retenue

`OPTION_C_MIGRER_AVANT_SUPPRESSION`

Motif : des consommateurs ou ?carts doivent ?tre trait?s avant toute suppression physique.

## ?l?ments ? adapter avant suppression ?ventuelle

| Domaine | ?l?ment ? adapter / v?rifier |
|---|---|
| Scripts | V?rifier/remplacer toute lecture r?siduelle de `REF_Logements.date_entree_gestion` et `date_sortie_gestion`; `lot4bis` et les calculs doivent rester sur `REF_Gestion_Logements_Hist`. |
| Excel | Supprimer uniquement apr?s contr?le des tables, formules, validations et noms d?finis dans `REF_Setup.xlsm`; sauvegarde xlsm avec macros obligatoire. |
| Power Query / Power BI | Rechercher les deux colonnes dans requ?tes et exports avant suppression. |
| Tests | Ajouter tests de sch?ma interdisant la r?apparition comme sources officielles et testant date_debut vide non bloquante. |
| Cadrage | Documenter que ces colonnes sont d?commissionn?es ou indicatives selon d?cision humaine finale. |
| Sauvegarde | Backup horodat? de `REF_Setup.xlsm` avant toute suppression. |
| Contr?les apr?s suppression | Tests unitaires, scan r?f?rences, keep_vba=True, pr?sence `xl/vbaProject.bin`, absence `#REF!`, r?solution r?servations anciennes. |

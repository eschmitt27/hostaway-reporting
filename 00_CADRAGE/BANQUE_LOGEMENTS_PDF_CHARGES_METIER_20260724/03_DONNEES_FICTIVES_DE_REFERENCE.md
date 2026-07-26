# 03 — Données fictives de référence

## Emplacement
`…\BANQUE_LOGEMENTS_PDF_CHARGES_METIER\data_recette\` (dossier isolé, hors sources réelles).
Généré par `recette\build_data_recette.py` (idempotent, réinitialisable).

## Contenu (aucune PII réelle — vérifié 0 occurrence)
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` : feuilles config recopiées en valeurs ; feuilles
  parc/personnes **remplacées** par du fictif ; feuille banque (PII) vidée ; `REF_Assoc_Mode`
  remappée (PERS_X/PERS_Y).
- `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` + `SAISIE_Charges_Impacts.xlsx` : templates
  vides copiés verbatim (formules + ligne modèle préservées ; 0 charge ; 0 PII).
- `02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` : vide (régénéré par Lot3).
- `02_TRAVAIL/*.py` : 46 scripts moteur copiés (code, requis par le runner aval).
- `03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv` : liste logements fictive.
- `data/` : base SQLite isolée.

## Parc fictif
| Propriétaire | Logements | Taux commission |
|---|---|---|
| PROP_A (Alpha) | LOG_A1, LOG_A2 | A1 : 19 % avant 2026-03 → **15 % à partir de 2026-03** ; A2 : 19 % |
| PROP_B (Beta) | LOG_B1 | 15 % |
| PROP_C (Gamma) | LOG_C1, LOG_INACTIF | C1 : 19 % ; LOG_INACTIF : inactif (exclu des listes) |

Intervenants fictifs : INT_A (interne), INT_B (externe). Associés : PERS_X, PERS_Y.
Période de recette ouverte : **2026-06** (seule non clôturée). Emails fictifs `@example.test`.

## Vérifié
Le formulaire Charges lit ces données : propriétaires = [PROP_A, PROP_B, PROP_C], logements =
[LOG_A1, LOG_A2, LOG_B1, LOG_C1] (LOG_INACTIF exclu), codes impact = [IC, HC], mois ouverts =
[2026-06]. Changement de taux LOG_A1 présent pour tester la période applicable.

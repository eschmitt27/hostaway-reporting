# LOT4A dry-run — Transformateur SAISIE ReservationsHH -> MASTER de test

**Statut** : `ANALYSE_TERMINEE`  
**as-of (date_integration)** : `2026-07-02T00:00:00Z`  
**MASTER de test genere** : True  
**Interpreteur** : `C:\Users\Ewan\AppData\Local\Programs\Python\Python313\python.exe` (Python 3.13.2)  
**pandas** 3.0.3 · **numpy** 2.4.6 · **openpyxl** 3.1.5

- lignes SAISIE utiles : 1
- lignes MASTER generees : 1
- lignes VUE_ACTIVE (statut_controle=VALIDE) : 1

## Oracle RESHH-2026-05-001

- taux_commission = `0.15`
- commission = `343.27`
- acompte_facture = `1945.21`
- date_integration = `2026-07-02T00:00:00Z`

## Invariance des sources reelles (lecture seule)

- `SAISIE_ReservationsHorsHostaway.xlsx` : INCHANGE (sha256 e4591912a6b0…)
- `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` : INCHANGE (sha256 c0e4434c3479…)
- `REF_Setup.xlsm` : INCHANGE (sha256 f45f4feadcbe…)

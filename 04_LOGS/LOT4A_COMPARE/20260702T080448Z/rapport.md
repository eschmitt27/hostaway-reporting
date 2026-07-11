# LOT4A — Comparateur SAISIE ReservationsHH vs MASTER (lecture seule)

**Statut** : `ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES`  
**Horodatage UTC** : 20260702T080448Z  
**as-of** : 2026-07-02T00:00:00Z  
**Interpreteur** : `C:\Users\Ewan\AppData\Local\Programs\Python\Python313\python.exe` (Python 3.13.2)  
**pandas** 3.0.3 · **numpy** 2.4.6 · **openpyxl** 3.1.5

## Compteurs par categorie

- `MANUEL_IDENTIQUE` : 21
- `MANUEL_DIFFERENT` : 0
- `DERIVE_COHERENT` : 6
- `ECART_HISTORIQUE_ATTENDU` : 6
- `METADONNEE_NON_COMPARABLE` : 1
- `TAUX_BLOQUANT` : 0

## Ecarts manuels (SAISIE ≠ MASTER)

_Aucun._

## Ecarts historiques legacy (MASTER incomplet/divergent)

- RESHH-2026-05-001 · ROW_HASH : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique
- RESHH-2026-05-001 · commission : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique
- RESHH-2026-05-001 · acompte_facture : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique
- RESHH-2026-05-001 · source_module : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique
- RESHH-2026-05-001 · source_table : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique
- RESHH-2026-05-001 · source_pk : MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique

## Reservations a taux bloquant

_Aucune._

## Acomptes positifs a analyser (impact lot5/lot12)

- RESHH-2026-05-001 · acompte_facture recalc=`1945.21`

## Oracle RESHH-2026-05-001

- taux_commission = `0.15` (master=`0.15`, DERIVE_COHERENT)
- commission = `343.27` (master=``, ECART_HISTORIQUE_ATTENDU)
- acompte_facture = `1945.21` (master=``, ECART_HISTORIQUE_ATTENDU)

## Invariance des sources (lecture seule)

- `SAISIE_ReservationsHorsHostaway.xlsx` : INCHANGE (sha256 e4591912a6b0…)
- `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` : INCHANGE (sha256 c0e4434c3479…)
- `REF_Setup.xlsm` : INCHANGE (sha256 f45f4feadcbe…)

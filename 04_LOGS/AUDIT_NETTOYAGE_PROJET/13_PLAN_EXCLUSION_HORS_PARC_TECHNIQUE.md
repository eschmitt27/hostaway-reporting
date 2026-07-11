# 13 - Plan exclusion hors parc technique

Date : 2026-06-28 23:01:39

## D?cision

Ajouter `statut_parc` dans `REF_Logements`.

Valeurs autoris?es :

- `GERE`
- `HORS_PARC_TECHNIQUE`

`actif` reste la disponibilit? technique du code r?f?rentiel. `statut_parc` porte l'?ligibilit? aux calculs de conciergerie.

## R?gles appliqu?es

- Tous les logements r?ellement g?r?s : `GERE`.
- `APPARTEMENT_DIVERS` et `LOGEMENT_DIVERS` : `HORS_PARC_TECHNIQUE`.
- Les codes hors parc restent techniquement pr?sents mais sont exclus des calculs op?rationnels.
- Le code de contr?le explicite est `HORS_PARC_TECHNIQUE`.

## Scripts ? adapter

- `lot4bis_charger_reservations.py` : exclure les r?servations mapp?es sur hors parc.
- `lot10_calculer_resultats.py` : exclure commissions, charge fixe, nets et r?sultats par logement hors parc.
- `lot11_controles_coherence.py` : ne pas produire de contr?le ambigu propri?taire/taux pour hors parc.
- `lot12_generer_factures.py` : ne jamais produire pr?facture hors parc.
- `lot13_export_powerbi.py` : exposer `statut_parc` comme dimension, sans calcul.
- `lot6c_menages_externes.py` : exclure les m?nages externes hors parc.

## Contr?les attendus

- Tests unitaires hors parc.
- Suite unittest compl?te.
- Aucune sortie g?n?r?e modifi?e.
- `REF_Setup.xlsm` lisible avec `keep_vba=True`.
- `xl/vbaProject.bin` pr?sent.
- Aucune formule `#REF!`.
- Scan des r?f?rences aux deux codes techniques.

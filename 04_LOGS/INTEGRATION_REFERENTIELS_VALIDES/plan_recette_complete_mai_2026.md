# Pr?paration recette compl?te mai 2026

Aucune cl?ture r?elle et aucune facture finale l?gale ne doivent ?tre lanc?es.

## Cha?ne cible isol?e

- Lot1 ou snapshot Hostaway local : utiliser un snapshot fig?, sans appel API vivant.
- Lot4bis : charger les r?servations normalis?es depuis le snapshot/local.
- Lot4quater : r?soudre les sources de r?servations.
- Lot5 : int?grer les acomptes si un fichier de saisie r?el existe.
- Lot6b ? Lot6f : rejouer m?nages internes, externes, rapprochement, gain/perte et co?t complet.
- Lot8 : utiliser l'import bancaire local, sans banque en ligne.
- Lot9 : produire flux d?terministes.
- Lot10 : produire r?sultats.
- Lot11 : produire contr?les.
- Lot12 : pr?factures uniquement, pas de facture finale l?gale.
- Lot13 : exports Power BI.

## Comparaisons run 1 / run 2

Comparer nombre de lignes, identifiants m?tier, montants, contr?les ouverts, r?sultat r?el, r?sultat comptable et r?sultat hors compta. Ne pas comparer les timestamps fichiers.

## Pr?requis bloquants constat?s

L'int?gration des r?f?rentiels valid?s est bloqu?e tant que `REF_Setup.xlsm` est verrouill? ou que les lignes du fichier de pr?paration restent ? confirmer/incompl?tes.

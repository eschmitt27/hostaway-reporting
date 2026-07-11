# ?tat pr?t recette

Statuts possibles : `PR?T`, `? RENSEIGNER`, `NON APPLICABLE CE MOIS`.

| Domaine | Statut initial | Donn?es ? v?rifier avant recette | Blocage possible |
|---|---|---|---|
| Taux de commission dat?s | ? RENSEIGNER | Compl?ter `TAUX_COMMISSION` pour les propri?taires/logements du mois; 12 lignes ? confirmer | Oui, facture finale bloqu?e si absent/ambigu/non dat? |
| Historique propri?taire / logement | ? RENSEIGNER | Compl?ter `GESTION_LOGEMENTS`; 19 lignes ? confirmer | Oui, facture finale bloqu?e si propri?taire ou p?riode non applicable |
| Taux horaires internes avant juin | ? RENSEIGNER | Compl?ter si le mois test? contient des m?nages internes avant juin; 5 lignes reprises de M04 | Oui pour les mois avant juin avec m?nage interne |
| Co?ts internes fixes depuis juin | ? RENSEIGNER | Confirmer les co?ts existants et compl?ter les types/logements manquants; 5 lignes ? confirmer | Oui pour les mois depuis juin avec m?nage interne |
| Banque du mois | ? RENSEIGNER | V?rifier import Lot8, rapprochement et statut de cl?ture bancaire du mois | Oui |
| Charges du mois | ? RENSEIGNER | V?rifier `SAISIE_Charges_Flux.xlsx` et int?gration des charges du mois | Oui si charges obligatoires absentes ou incoh?rentes |
| M?nages internes et externes | ? RENSEIGNER | V?rifier volumes Hostaway/hors Hostaway, d?clarations internes, factures prestataires et rapprochement | Oui si ?cart bloquant ou co?t absent |
| Acomptes | NON APPLICABLE CE MOIS | Renseigner seulement s'il existe des acomptes propri?taire imputables | Bloque uniquement si acompte attendu mais non rattach?/ambigu |
| Versements Airbnb | NON APPLICABLE CE MOIS | Renseigner `IMPUTATIONS_AIRBNB` seulement si un versement Airbnb doit r?duire une facture | Bloque ou contr?le si virement bancaire Airbnb non imput? |
| AirCover | NON APPLICABLE CE MOIS | Renseigner `AIRCOVER` uniquement si remboursement r?el document? | Contr?le si b?n?ficiaire/traitement/justificatif absent; pas d'impact automatique |
| Contr?les ouverts | ? RENSEIGNER | Examiner `MASTER_CTRL_Coherence.xlsx`, surtout les contr?les `BLOQUANT_FACTURE` du mois/document | Oui |
| Pr?factures | PR?T | G?n?rer uniquement des pr?factures de contr?le, jamais facture finale l?gale sans cl?ture | Oui pour ?mission finale |

## Donn?es ? ne jamais inventer

- Dates de d?but ou fin de gestion.
- Dates d'effet des taux de commission.
- Taux de commission contractuel.
- Taux horaire intervenant.
- Co?t fixe m?nage interne.
- B?n?ficiaire r?el AirCover.
- Justificatif AirCover, Airbnb, acompte ou ajustement.
- Rattachement banque vers propri?taire/logement/mois/document.

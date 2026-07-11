# Guide de renseignement avant premi?re cl?ture

Ce dossier sert ? pr?parer les d?cisions m?tier avant de modifier les r?f?rentiels de production. Ne renseigner que des informations prouv?es par contrat, relev?, saisie op?rateur ou justificatif.

## R?gle g?n?rale

- Ne pas inventer de date de d?but, date de fin, taux, co?t, propri?taire, montant ou justificatif.
- Laisser vide toute information inconnue.
- Utiliser la colonne `source_preuve` pour noter le fichier, la feuille, la colonne ou le document justificatif.
- Utiliser `a_confirmer_par_operateur = OUI` tant que la ligne n'a pas ?t? valid?e par une personne responsable.

## TAUX_COMMISSION

? renseigner pour chaque propri?taire ou logement concern? par le mois de cl?ture. Les taux repris viennent de `REF_Proprietaires.taux_commission`, mais ils ne sont pas encore historis?s dans `REF_Taux_Commission`.

? compl?ter : `taux_commission_id`, `date_debut`, `date_fin` si applicable, `justification`, confirmation du taux. Un taux logement peut ?tre ajout? si le logement d?roge au taux g?n?ral propri?taire.

Contr?le si manquant : taux absent, taux non dat? ou taux concurrent. Une facture finale est bloqu?e pour le document concern?.

## GESTION_LOGEMENTS

? renseigner pour chaque logement qui porte une r?servation, charge, m?nage, acompte, contr?le ou facture du mois. Les lignes proviennent de `REF_Logements` et doivent ?tre confirm?es comme historique r?el.

? compl?ter : `gestion_id`, propri?taire applicable, dates r?elles de d?but/fin de gestion, statut. Ne pas transformer un statut actuel en historique si la date n'est pas prouv?e.

Contr?le si manquant : gestion absente, propri?taire absent ou r?servation hors p?riode. La facture finale du logement concern? est bloqu?e.

## TAUX_HORAIRES_AVANT_JUIN

? renseigner seulement pour les m?nages internes ant?rieurs au 1er juin 2026. Les lignes proviennent de `M04_MENAGES_PowerQuery.xlsx / PARAM_TAUX_INTERVENANTS`.

? compl?ter : `taux_horaire_id`, confirmation du taux par intervenant, dates de validit? et source de preuve.

Contr?le si manquant : taux horaire absent avant juin. Aucun co?t ne doit ?tre remplac? par 0 ou par un co?t fixe.

## COUTS_INTERNES_DEPUIS_JUIN

? renseigner pour les m?nages internes ? partir du 1er juin 2026. Les co?ts existants de `REF_Couts_Menage_Interne` ont ?t? repris; les types de logement sans co?t apparaissent comme lignes ? compl?ter.

Hi?rarchie ? respecter : intervenant + logement, logement, intervenant + type logement, type logement, d?faut global explicitement param?tr?.

Contr?le si manquant : co?t fixe absent ou ambigu apr?s juin. Aucun co?t ne doit ?tre fix? ? 0 automatiquement.

## AIRCOVER

Peut rester vide tant qu'aucun remboursement AirCover r?el n'existe. Si un cas existe, renseigner b?n?ficiaire r?el, montant, logement/propri?taire/r?servation si connus, justificatif et traitement explicite.

Contr?le si manquant ou ambigu : `A_CONTROLER`. AirCover ne modifie jamais automatiquement payout, commission, net propri?taire, facture ou r?glement.

## IMPUTATIONS_AIRBNB

Peut rester vide tant qu'aucun versement Airbnb ne doit r?duire un reste ? r?gler. Si un versement doit ?tre imput?, il faut rattacher avec certitude transaction bancaire, propri?taire, logement, mois, document et montant.

Contr?le si ambigu : versement Airbnb non imput?. Aucun impact sur payout, commission, m?nage, forfait ou net propri?taire.

## AJUSTEMENTS_POST_CLOTURE

Peut rester vide avant la premi?re cl?ture r?elle. ? utiliser uniquement pour une correction append-only apr?s cl?ture ou un ajustement valid? avec justificatif.

Contr?le si incomplet : ajustement sans justificatif, rattachement ou validation. Aucun mois cl?tur? ne doit ?tre ?cras?.

## Ce qui bloque une facture finale

- Taux de commission applicable absent, ambigu ou non dat?.
- Gestion logement/propri?taire applicable absente ou incoh?rente.
- R?servation hors p?riode de gestion.
- Co?t m?nage interne applicable absent ou ambigu lorsque le mois et le cas l'exigent.
- Banque du mois non cl?tur?e.
- Contr?le bloquant facture ouvert sur le document, logement, propri?taire ou mois concern?.

## Ce qui ne bloque pas tant qu'aucun cas r?el n'existe

- Onglet AirCover vide si aucun remboursement AirCover r?el n'existe.
- Onglet Imputations Airbnb vide si aucun versement Airbnb ne doit ?tre imput? ? une facture.
- Onglet Ajustements post-cl?ture vide avant la premi?re cl?ture, hors correction d?j? d?cid?e.

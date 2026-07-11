# 18 - Revue finale suppression dates gestion

Date: 2026-06-29

## 1. Logements GERE couverts

Controle structurel de REF_Gestion_Logements_Hist:
- LOG_0001 -> PROP_0001 / GST_LOG_0001_PROP_0001
- LOG_0002 -> PROP_0002 / GST_LOG_0002_PROP_0002
- LOG_0003 -> PROP_0003 / GST_LOG_0003_PROP_0003
- LOG_0004 -> PROP_0004 / GST_LOG_0004_PROP_0004
- LOG_0005 -> PROP_0005 / GST_LOG_0005_PROP_0005
- LOG_0006 -> PROP_0006 / GST_LOG_0006_PROP_0006
- LOG_0007 -> PROP_0007 / GST_LOG_0007_PROP_0007
- LOG_0008 -> PROP_0008 / GST_LOG_0008_PROP_0008
- LOG_0009 -> PROP_0003 / GST_LOG_0009_PROP_0003
- LOG_0010 -> PROP_0005 / GST_LOG_0010_PROP_0005
- LOG_0011 -> PROP_0008 / GST_LOG_0011_PROP_0008
- LOG_0012 -> PROP_0009 / GST_LOG_0012_PROP_0009
- LOG_0013 -> PROP_0009 / GST_LOG_0013_PROP_0009
- LOG_0014 -> PROP_0010 / GST_LOG_0014_PROP_0010
- LOG_0015 -> PROP_0011 / GST_LOG_0015_PROP_0011
- LOG_0016 -> PROP_0012 / GST_LOG_0016_PROP_0012
- LOG_0017 -> PROP_0008 / GST_LOG_0017_PROP_0008

Cas bloquants detectes: 0.
Codes HORS_PARC_TECHNIQUE sans historique acceptes: APPARTEMENT_DIVERS, LOGEMENT_DIVERS.

## 2. References actives adaptees

- lot4bis_charger_reservations.py: suppression du fallback REF_Logements.date_sortie_gestion; resolution uniquement via resolve_management_period / REF_Gestion_Logements_Hist.
- lot6c_menages_externes.py: proprietaire resolu via REF_Gestion_Logements_Hist; suppression de l'usage date_sortie_gestion.
- lot10_calculer_resultats.py: build_charge_fixe utilise REF_Gestion_Logements_Hist; retrait des controles date_entree_gestion/date_sortie_gestion.
- lot11_controles_coherence.py: retrait du re-controle CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE base sur REF_Logements.
- lot13_export_powerbi.py: retrait des deux colonnes de l'export PBI_Referentiel_Logements.

Scan actif 02_TRAVAIL/*.py: aucune reference restante a date_entree_gestion/date_sortie_gestion.

## 3. Colonnes supprimees

Supprimees physiquement de REF_Logements:
- date_entree_gestion
- date_sortie_gestion

Etat Excel apres suppression:
- REF_Logements headers: logement_id, hostaway_listing_id, nom_logement_officiel, nom_court, adresse, ville, type_logement_id, sur_hostaway, dynamic_pricing, actif, statut_parc, commentaire, forfait_logiciel_consommables_mensuel, seuil_voyageurs_preparation_canape, montant_preparation_canape.
- tbl_REF_Logements: A1:O20.
- Scan zip xlsm: aucune occurrence date_entree_gestion/date_sortie_gestion/#REF!.

## 4. Tests ajoutes et resultat

Tests ajoutes/adaptes:
- tests/test_ref_history.py: periode explicite GERE, date debut vide deja couverte, periode ambigue deja couverte, hors parc sans historique explicite.
- tests/test_hors_parc_technique.py: absence des deux anciennes colonnes dans REF_Logements, absence de fallback dans les scripts actifs, adaptation charge fixe a REF_Gestion_Logements_Hist.

Commande:
"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"

Resultat:
.................C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_TRAVAIL\lot10_calculer_resultats.py:523: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  df_comm = pd.concat([_shape(df_ha_norm), _shape(df_vrbo_norm), _shape(df_hh_norm)], ignore_index=True)
................................................
----------------------------------------------------------------------
Ran 65 tests in 0.630s

OK

## 5. Controles Excel, macros et Power Query

- keep_vba=True: OK.
- xl/vbaProject.bin present: oui.
- Formules #REF!: 0.
- Anciennes colonnes dans contenu xlsm: 0 occurrence.
- Table REF_Logements coherente: tbl_REF_Logements A1:O20.
- Validations presentes: 2 validations sur REF_Logements, non cassees par le scan.
- Noms definis: aucun nom defini detecte avant/apres.
- Power Query / connexions: scan zip xlsm sans occurrence des anciennes colonnes.

## 6. Sorties generees

Aucun lot metier complet ni pipeline lance.
Aucune sortie MASTER_* suivie par Git modifiee.
Fichiers modifies suivis: REF_Setup.xlsm, scripts actifs, tests.

## 7. Fichiers candidats futur commit

- 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
- 02_TRAVAIL/lot4bis_charger_reservations.py
- 02_TRAVAIL/lot6c_menages_externes.py
- 02_TRAVAIL/lot10_calculer_resultats.py
- 02_TRAVAIL/lot11_controles_coherence.py
- 02_TRAVAIL/lot13_export_powerbi.py
- tests/test_hors_parc_technique.py
- tests/test_ref_history.py

Rapports et sauvegarde sous 04_LOGS/AUDIT_NETTOYAGE_PROJET crees mais non candidats par defaut si 04_LOGS reste exclu.

## 8. Controles Git

- git diff --check: OK, aucun probleme; avertissements LF/CRLF uniquement.
- git diff --stat: 8 fichiers suivis modifies, 92 insertions, 121 suppressions, REF_Setup.xlsm 86514 -> 86345 bytes.

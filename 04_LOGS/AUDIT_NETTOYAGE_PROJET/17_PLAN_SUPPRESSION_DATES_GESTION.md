# 17 - Plan suppression dates gestion

Date: 2026-06-29

Decision appliquee: REF_Gestion_Logements_Hist est l'unique source officielle des periodes de gestion. REF_Logements.date_entree_gestion et REF_Logements.date_sortie_gestion sont des doubles sources a supprimer.

## Inventaire consommateurs actifs

References actives detectees avant correction:
- 02_TRAVAIL/lot4bis_charger_reservations.py: fallback date_sortie_gestion dans resolve_logement.
- 02_TRAVAIL/lot6c_menages_externes.py: enrichissement log_info avec date_sortie_gestion.
- 02_TRAVAIL/lot10_calculer_resultats.py: build_charge_fixe utilise date_entree_gestion/date_sortie_gestion.
- 02_TRAVAIL/lot11_controles_coherence.py: controle date_entree_gestion incoherente.
- 02_TRAVAIL/lot13_export_powerbi.py: export PBI_Referentiel_Logements inclut les deux colonnes.

References Excel detectees avant suppression:
- REF_Logements!H1 = date_entree_gestion.
- REF_Logements!I1 = date_sortie_gestion.
- Table Excel: tbl_REF_Logements.
- Aucune formule, validation, nom defini ou commentaire actif referencant ces deux champs detecte hors en-tetes.

## Plan d'execution

1. Sauvegarder REF_Setup.xlsm dans 04_LOGS/AUDIT_NETTOYAGE_PROJET.
2. Adapter les scripts actifs pour utiliser REF_Gestion_Logements_Hist via lib_ref_history.py, sans fallback date_entree_gestion/date_sortie_gestion.
3. Adapter l'export Power BI pour retirer les deux colonnes de REF_Logements et conserver REF_Gestion_Logements_Hist comme source datee.
4. Supprimer physiquement les deux colonnes de REF_Logements dans REF_Setup.xlsm en conservant VBA.
5. Ajouter les tests de resolution historique, hors parc sans historique, absence des colonnes et absence de fallback.
6. Verifier: unittest, git diff --check, Excel keep_vba, vbaProject.bin, absence #REF!, scan actif, aucune sortie MASTER_* suivie modifiee.

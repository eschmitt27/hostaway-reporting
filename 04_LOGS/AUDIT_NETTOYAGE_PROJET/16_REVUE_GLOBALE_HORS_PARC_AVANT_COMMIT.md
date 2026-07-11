# 16 - Revue globale HORS_PARC avant commit

Date: 2026-06-29
Projet: C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie
Base: 3e19606 Supprime doublons sources verite et securise imports

## 1. Regle centrale unique

Recherche texte dans 02_TRAVAIL/*.py hors lib_parc.py:
- Aucune occurrence de APPARTEMENT_DIVERS ou LOGEMENT_DIVERS dans les scripts actifs.
- Les lots consommateurs utilisent les fonctions centrales de lib_parc.py: is_hors_parc_technique, is_statut_parc_a_controler, is_gere selon le cas.
- Des occurrences HORS_PARC_TECHNIQUE / GERE restent dans des imports, messages, codes anomalies ou libelles explicatifs.

Analyse AST des comparaisons directes hors lib_parc.py:
- 02_TRAVAIL/lot4bis_charger_reservations.py:471: ano_code in (HORS_PARC_TECHNIQUE, STATUT_PARC_INVALIDE)
- 02_TRAVAIL/lot4bis_charger_reservations.py:472: ano_code == HORS_PARC_TECHNIQUE
- 02_TRAVAIL/lot4bis_charger_reservations.py:473: ano_code == HORS_PARC_TECHNIQUE
- 02_TRAVAIL/lot4bis_charger_reservations.py:474: ano_code == HORS_PARC_TECHNIQUE

Interpretation: ces comparaisons portent sur le code d'anomalie retourne en aval de la regle centrale, pas sur des codes logement ni sur des chaines brutes. Elles restent neanmoins les seules comparaisons directes residuelles hors lib_parc.py.

## 2. Contrat lib_parc.py

lib_parc.py distingue explicitement:
- GERE
- HORS_PARC_TECHNIQUE
- A_CONTROLER / STATUT_PARC_INVALIDE

Comportement constate:
- statut_parc=GERE -> is_gere True.
- statut_parc=HORS_PARC_TECHNIQUE -> is_hors_parc_technique True.
- statut_parc vide, None, absent ou inconnu -> statut_parc_traitement A_CONTROLER, code_anomalie_statut_parc STATUT_PARC_INVALIDE.
- Une valeur inconnue ne retourne pas simplement False comme decision finale; elle est routable explicitement via is_statut_parc_a_controler.

## 3. REF_Setup.xlsm

Inspection read-only openpyxl + zip:
- xl/vbaProject.bin present: oui.
- Chargement keep_vba=True: OK.
- Onglet REF_Logements: colonne statut_parc presente.
- Table Excel: tbl_REF_Logements presente.
- Lignes existantes: 19.
- statut_parc renseigne: 19/19.
- Valeurs presentes: GERE, HORS_PARC_TECHNIQUE.
- Lignes statut_parc vides: 0.
- Validation de donnees: liste sur M2:M20 avec "GERE,HORS_PARC_TECHNIQUE".
- Formules contenant #REF!: 0.
- Aucune casse de macro detectee par presence xl/vbaProject.bin et ouverture keep_vba=True.

## 4. Documentation mise a jour

Documents modifies uniquement dans 00_CADRAGE:
- ARCHITECTURE_DONNEES.md
- DECISIONS_METIER.md
- ETAT_AVANCEMENT.md
- JOURNAL_CONTROLES.md
- JOURNAL_ANOMALIES.md

Contenu ajoute:
- actif = disponibilite technique d'un code referentiel.
- statut_parc = eligibilite du code au parc de logements geres.
- GERE = logement reellement gere et eligible aux calculs metier.
- HORS_PARC_TECHNIQUE = code conserve pour controle ou anti-mauvais-mapping, exclu explicitement de tout calcul economique et operationnel.
- statut_parc vide, invalide ou inconnu = A_CONTROLER, code anomalie STATUT_PARC_INVALIDE, sans calcul economique.

## 5. Controles

### unittest

Commande:
"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"

Sortie:
.................C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_TRAVAIL\lot10_calculer_resultats.py:522: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  df_comm = pd.concat([_shape(df_ha_norm), _shape(df_vrbo_norm), _shape(df_hh_norm)], ignore_index=True)
............................................
----------------------------------------------------------------------
Ran 61 tests in 0.997s

OK

### git diff --check

Resultat: aucune erreur.
Avertissements CRLF/LF sur les fichiers modifies, sans anomalie diff --check.

### git diff --stat

 00_CADRAGE/ARCHITECTURE_DONNEES.md         |   14 +
 00_CADRAGE/DECISIONS_METIER.md             |   11 +
 00_CADRAGE/ETAT_AVANCEMENT.md              |   15 +
 00_CADRAGE/JOURNAL_ANOMALIES.md            |   11 +
 00_CADRAGE/JOURNAL_CONTROLES.md            |   18 +
 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm |  Bin 86364 -> 86514 bytes
 02_TRAVAIL/lot10_calculer_resultats.py     |   78 +-
 02_TRAVAIL/lot11_controles_coherence.py    |   16 +-
 02_TRAVAIL/lot12_generer_factures.py       |   10 +
 02_TRAVAIL/lot13_export_powerbi.py         |    2 +-
 02_TRAVAIL/lot4bis_charger_reservations.py |   54 +-
 02_TRAVAIL/lot6c_menages_externes.py       | 1359 ++++++++++++++--------------
 tests/test_import_side_effects.py          |   68 +-
 13 files changed, 955 insertions(+), 701 deletions(-)

Note: 02_TRAVAIL/lib_parc.py, tests/test_hors_parc_technique.py et les rapports 04_LOGS non suivis ne sont pas inclus dans git diff --stat.

## 6. Fichiers candidats futur commit

- 00_CADRAGE/ARCHITECTURE_DONNEES.md
- 00_CADRAGE/DECISIONS_METIER.md
- 00_CADRAGE/ETAT_AVANCEMENT.md
- 00_CADRAGE/JOURNAL_ANOMALIES.md
- 00_CADRAGE/JOURNAL_CONTROLES.md
- 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
- 02_TRAVAIL/lib_parc.py
- 02_TRAVAIL/lot4bis_charger_reservations.py
- 02_TRAVAIL/lot6c_menages_externes.py
- 02_TRAVAIL/lot10_calculer_resultats.py
- 02_TRAVAIL/lot11_controles_coherence.py
- 02_TRAVAIL/lot12_generer_factures.py
- 02_TRAVAIL/lot13_export_powerbi.py
- tests/test_hors_parc_technique.py
- tests/test_import_side_effects.py
- 04_LOGS/AUDIT_NETTOYAGE_PROJET/14_REVUE_FINALE_HORS_PARC_TECHNIQUE.md
- 04_LOGS/AUDIT_NETTOYAGE_PROJET/15_CORRECTION_STATUT_PARC_INVALIDE.md
- 04_LOGS/AUDIT_NETTOYAGE_PROJET/16_REVUE_GLOBALE_HORS_PARC_AVANT_COMMIT.md

## 7. Fichiers explicitement exclus

- .gitignore
- 04_LOGS/ hors rapports AUDIT_NETTOYAGE_PROJET explicitement crees
- Conciergerie app/
- 99_ARCHIVES/

## Conclusion

La validation referentielle statut_parc est conforme. La documentation demandee est mise a jour. Les tests passent. Les seules comparaisons directes residuelles detectees hors lib_parc.py sont dans lot4bis sur le code d'anomalie deja produit par la regle centrale.

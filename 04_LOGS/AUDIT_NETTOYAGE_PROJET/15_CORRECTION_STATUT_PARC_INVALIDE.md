# 15 - Correction STATUT_PARC_INVALIDE

Date: 2026-06-29
Projet: C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie
Base: 3e19606 Supprime doublons sources verite et securise imports

## Corrections appliquees

- 02_TRAVAIL/lib_parc.py
  - Ajout d'une decision centrale tri-etat: GERE, HORS_PARC_TECHNIQUE, A_CONTROLER.
  - Ajout de STATUT_PARC_INVALIDE.
  - statut_parc vide, manquant, None ou inconnu retourne A_CONTROLER et non GERE.
  - is_gere ne retourne True que pour statut_parc=GERE.
  - is_hors_parc_technique ne retourne True que pour statut_parc=HORS_PARC_TECHNIQUE.

- 02_TRAVAIL/lot4bis_charger_reservations.py
  - Les statuts invalides produisent A_CONTROLER / STATUT_PARC_INVALIDE.
  - Aucun montant n'est retenu pour ces lignes.
  - Les lignes HORS_PARC_TECHNIQUE restent explicitement exclues.

- 02_TRAVAIL/lot10_calculer_resultats.py
  - build_commissions exclut avant calcul les logements HORS_PARC_TECHNIQUE et les statuts invalides.
  - Les statuts invalides alimentent A_CONTROLER avec code STATUT_PARC_INVALIDE.
  - build_charge_fixe bloque les statuts invalides avant tout calcul proprietaire.

- 02_TRAVAIL/lot11_controles_coherence.py
  - Les statuts invalides REF_Logements sont signales en A_CONTROLER / STATUT_PARC_INVALIDE.
  - Les controles economiques de logements geres ne prennent plus les statuts invalides.

- 02_TRAVAIL/lot12_generer_factures.py
  - Protection avant creation de facture: HORS_PARC_TECHNIQUE et STATUT_PARC_INVALIDE ne produisent pas de facture.

- 02_TRAVAIL/lot6c_menages_externes.py
  - Import de HORS_PARC_TECHNIQUE et is_hors_parc_technique depuis lib_parc.
  - Encapsulation du traitement dans main().
  - Ajout du guard if __name__ == "__main__": main().
  - L'import du module ne lit/ecrit plus de fichier metier et ne cree plus de sortie.

## Tests ajoutes ou modifies

- tests/test_hors_parc_technique.py
  - GERE conserve le chemin normal de commission quand le taux existe.
  - HORS_PARC_TECHNIQUE ne produit pas de commission/net.
  - statut_parc vide produit A_CONTROLER / STATUT_PARC_INVALIDE sans calcul economique.
  - statut_parc inconnu produit A_CONTROLER / STATUT_PARC_INVALIDE sans calcul economique.
  - charge fixe bloque STATUT_PARC_INVALIDE.
  - logement GERE sans proprietaire reste BLOQUANT.
  - Lot12 contient les protections hors parc et statut invalide avant facture.

- tests/test_import_side_effects.py
  - Ajout de lot6c_menages_externes.py au perimetre.
  - Import isole en copie temporaire pour lots 4bis, 6c, 10, 11, 12 et 13.
  - Patch des acces fichiers metier et openpyxl.load_workbook pendant l'import.
  - Verification qu'aucun fichier n'est cree dans la copie temporaire.

## Resultat des tests

Commande:
"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"

Sortie:
.................C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_TRAVAIL\lot10_calculer_resultats.py:522: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  df_comm = pd.concat([_shape(df_ha_norm), _shape(df_vrbo_norm), _shape(df_hh_norm)], ignore_index=True)
............................................
----------------------------------------------------------------------
Ran 61 tests in 0.585s

OK

## Controles Git

### git diff --check

Resultat: aucune erreur.
Avertissements CRLF/LF sur:
- 02_TRAVAIL/lot10_calculer_resultats.py
- 02_TRAVAIL/lot11_controles_coherence.py
- 02_TRAVAIL/lot12_generer_factures.py
- 02_TRAVAIL/lot13_export_powerbi.py
- 02_TRAVAIL/lot4bis_charger_reservations.py
- 02_TRAVAIL/lot6c_menages_externes.py
- tests/test_import_side_effects.py

### git diff --stat

 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm |  Bin 86364 -> 86514 bytes
 02_TRAVAIL/lot10_calculer_resultats.py     |   78 +-
 02_TRAVAIL/lot11_controles_coherence.py    |   16 +-
 02_TRAVAIL/lot12_generer_factures.py       |   10 +
 02_TRAVAIL/lot13_export_powerbi.py         |    2 +-
 02_TRAVAIL/lot4bis_charger_reservations.py |   54 +-
 02_TRAVAIL/lot6c_menages_externes.py       | 1359 ++++++++++++++--------------
 tests/test_import_side_effects.py          |   68 +-
 8 files changed, 886 insertions(+), 701 deletions(-)

Note: 02_TRAVAIL/lib_parc.py et tests/test_hors_parc_technique.py sont non suivis; ils ne sont donc pas inclus dans git diff --stat.

## Sorties generees

Aucun lot metier ni pipeline n'a ete lance.
Aucune sortie generee suivie par Git n'apparait modifiee par les controles executes.
REF_Setup.xlsm etait deja modifie dans le lot en cours et n'a pas ete modifie pendant cette correction.

## Conclusion

Le comportement interdit est corrige: un statut_parc vide, manquant ou inconnu n'est plus assimile a GERE.
Il devient A_CONTROLER avec code STATUT_PARC_INVALIDE et les chemins economiques controles ne poursuivent pas le calcul.
Les imports des lots 4bis, 6c, 10, 11, 12 et 13 sont couverts en copie temporaire isolee et restent sans effet de bord metier.

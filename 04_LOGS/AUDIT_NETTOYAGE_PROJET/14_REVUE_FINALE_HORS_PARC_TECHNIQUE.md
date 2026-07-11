# 14 - Revue finale HORS_PARC_TECHNIQUE

Date: 2026-06-29
Projet: C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie
Commit de base: 3e19606 Supprime doublons sources verite et securise imports

## Perimetre revu

Fichiers attendus:
- 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
- 02_TRAVAIL/lib_parc.py
- 02_TRAVAIL/lot4bis_charger_reservations.py
- 02_TRAVAIL/lot6c_menages_externes.py
- 02_TRAVAIL/lot10_calculer_resultats.py
- 02_TRAVAIL/lot11_controles_coherence.py
- 02_TRAVAIL/lot12_generer_factures.py
- 02_TRAVAIL/lot13_export_powerbi.py
- tests/test_hors_parc_technique.py

## Constats

### lib_parc.py

- Import sans lecture/ecriture fichier constatee: le fichier ne contient que constantes et fonctions.
- Regle centrale presente: HORS_PARC_TECHNIQUE, GERE, normalise_statut_parc, is_hors_parc_technique, is_gere.
- Ecart bloquant: normalise_statut_parc(None) retourne GERE.
- Ecart bloquant: statut_parc vide est donc assimile a GERE.
- Ecart bloquant: statut_parc inconnu/invalide n'est pas bloque par la regle centrale; is_hors_parc_technique retourne False et is_gere retourne True.

### Exclusions HORS_PARC_TECHNIQUE

- Les lots modifies appellent majoritairement is_hors_parc_technique au lieu de comparer APPARTEMENT_DIVERS / LOGEMENT_DIVERS.
- Les comparaisons directes APPARTEMENT_DIVERS / LOGEMENT_DIVERS trouvees sont dans tests/test_hors_parc_technique.py pour verifier le referentiel.
- Lot4bis: les reservations HA/HH hors parc sont marquees HORS_PARC_TECHNIQUE, montant 0, HR, EXCLU_RESULTAT.
- Lot10: les reservations hors parc sont retirees de build_commissions et ajoutees en controle INFO; build_charge_fixe exclut aussi ces logements.
- Lot11: les controles de forfait et proprietaire ignorent les logements hors parc.
- Lot12: les factures proprietaires sautent les logements hors parc avant creation facture.
- Lot13: statut_parc est ajoute a l'export referentiel logements.

### Ecarts et risques

- Ecart bloquant de regle metier: statut_parc vide ou inconnu est assimile de fait a GERE, contrairement a la regle attendue.
- Les tests actuels consacrent ce comportement non conforme: test_helper_defaults_missing_statut_to_gere attend explicitement normalise_statut_parc(None) == GERE.
- lot6c_menages_externes.py utilise is_hors_parc_technique mais l'import correspondant n'est pas present dans l'en-tete lu; risque NameError a l'execution.
- lot6c_menages_externes.py conserve du code execute a l'import: os.makedirs(L6C_DIR, exist_ok=True), lecture REF_Setup.xlsm et construction de donnees globales. Ce point est hors lib_parc mais reste contraire au principe de lots proteges par main guard.
- La chaine HORS_PARC_TECHNIQUE est dispersee comme code anomalie/message, mais la decision d'exclusion passe par la regle centrale.

### REF_Setup.xlsm

Inspection read-only openpyxl keep_vba=True:
- Fichier lisible avec keep_vba=True: oui.
- xl/vbaProject.bin present: oui.
- REF_Logements contient statut_parc: oui.
- APPARTEMENT_DIVERS = HORS_PARC_TECHNIQUE.
- LOGEMENT_DIVERS = HORS_PARC_TECHNIQUE.
- Valeurs statut_parc invalides dans REF_Logements: 0.
- Formules contenant #REF!: 0.

### Sorties generees suivies par Git

- git diff --name-only ne remonte aucune sortie generee suivie dans 02_TRAVAIL/Lot*/ ni 03_EXPORTS/.
- Les modifications suivies sont limitees a REF_Setup.xlsm et aux scripts du lot.

## Commandes executees

### unittest

Commande:
"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"

Resultat:
........................................................
----------------------------------------------------------------------
Ran 56 tests in 0.599s

OK

### git diff --check

Resultat: pas d'erreur de whitespace. Avertissements CRLF/LF sur les fichiers Python modifies:
- 02_TRAVAIL/lot11_controles_coherence.py
- 02_TRAVAIL/lot12_generer_factures.py
- 02_TRAVAIL/lot13_export_powerbi.py
- 02_TRAVAIL/lot4bis_charger_reservations.py
- 02_TRAVAIL/lot6c_menages_externes.py

### git diff --stat

 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm | Bin 86364 -> 86514 bytes
 02_TRAVAIL/lot10_calculer_resultats.py     |  52 +++++++++++++++++++----------
 02_TRAVAIL/lot11_controles_coherence.py    |   9 ++++-
 02_TRAVAIL/lot12_generer_factures.py       |   4 +++
 02_TRAVAIL/lot13_export_powerbi.py         |   2 +-
 02_TRAVAIL/lot4bis_charger_reservations.py |  30 ++++++++++++++++-
 02_TRAVAIL/lot6c_menages_externes.py       |   6 ++--
 7 files changed, 81 insertions(+), 22 deletions(-)

### git status --short

 M 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
 M 02_TRAVAIL/lot10_calculer_resultats.py
 M 02_TRAVAIL/lot11_controles_coherence.py
 M 02_TRAVAIL/lot12_generer_factures.py
 M 02_TRAVAIL/lot13_export_powerbi.py
 M 02_TRAVAIL/lot4bis_charger_reservations.py
 M 02_TRAVAIL/lot6c_menages_externes.py
?? 02_TRAVAIL/lib_parc.py
?? 04_LOGS/
?? "Conciergerie app/"
?? tests/test_hors_parc_technique.py

## Conclusion

Lot present mais non validable en l'etat.
Les exclusions HORS_PARC_TECHNIQUE sont amorcees et centralisees, REF_Setup.xlsm est coherent, et les tests passent.
Cependant la regle centrale assimile statut_parc vide a GERE et ne bloque pas les valeurs inconnues; les tests actuels valident meme cet ecart. lot6c presente aussi un risque d'execution par import manquant et code top-level.

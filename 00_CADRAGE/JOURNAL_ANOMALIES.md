# JOURNAL_ANOMALIES.md
> Anomalies détectées et leur statut de résolution. Chaque anomalie = une entrée unique.

---

## Format d'entrée

```
ID         : ANO-AAAA-MM-XXX
Date       : AAAA-MM-JJ
Lot        : Lot X — Nom
Code       : CODE_CONTROLE (cf. JOURNAL_CONTROLES.md)
Sévérité   : BLOQUANT | A_CONTROLER | INFO
Source     : table ou fichier concerné
PK         : clé de la ligne concernée si applicable
Description: description précise
Statut     : OUVERT | EN_COURS | CORRIGÉ | IGNORE_JUSTIFIE
Résolution : action prise ou raison d'ignorer
```

---

## Anomalies ouvertes

Note source de v?rit? 2026-06-28 : `REF_Gestion_Logements_Hist` est l'unique source officielle propri?taire/logement dat?e et `REF_Taux_Commission` l'unique source officielle des taux de commission. Toute valeur affich?e doit ?tre d?riv?e, jamais ressaisie comme seconde source.


### ANO-2026-06-001
Date : 2026-06-04 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-08
Code : LISTING_ORPHELIN_A_CONTROLER
Source : MASTER_REF_HA_Listings
PK : listingMapId = 515523
Description : Logement présent dans l'export Hostaway, ABSENT de REF_Logements.
Cas : vrai logement ancien ou désactivé (pas une erreur de saisie).
Traitement attendu : ajouter au REF_Setup.xlsm au Lot 2 avec statut explicite.
⚠ Ne pas ignorer définitivement. Ne pas intégrer comme logement actif avant arbitrage.
Résolution : CORRIGÉ — 515523 est l'identifiant Hostaway actuel (archivé) de LOG_0016 (T2 Cyprien / Clarisse).
Le logement n'était pas absent du REF : il y était via l'ancien ID 480780. Recréation Hostaway → nouvel ID.
Action : 515523 ajouté comme alias Hostaway dans REF_Mapping_Logements (MAP_LOG_0082) → LOG_0016.
LOG_0016.hostaway_listing_id mis à jour : 480780 → 515523. LOG_0016.sur_hostaway : NON → OUI.
Audit Lot 2 : 17/17 listings mappés, 0 orphelin résiduel.

---

### ANO-2026-06-002
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-07
Code : ENCODAGE_CASSE
Source : REF_Setup.xlsm — onglets REF_Associes, REF_Codes_Impact, REF_Types_Flux, REF_Types_Affectation
PK : multiple (26 cellules touchées)
Description : Caractères mal encodés (mojibake UTF-8→latin-1, ex. « associÃ© » au lieu de « associé »).
Onglet REF_Types_Affectation ajouté à la liste — absent du signalement initial.
Traitement appliqué le 2026-06-07 : correction par script Python (lot0_corrections.py), traitement paire par paire (U+00C3+U+00xx → codepoint UTF-8 correct).
Résolution : CORRIGÉ — 0 cellule résiduelle confirmée par audit post-correction CTR-2026-06-003.

---

### ANO-2026-06-003
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ / SANS OBJET
Code : DATES_SERIE_EXCEL
Source : REF_Setup.xlsm — onglets avec colonnes date
PK : multiple
Description : Signalement préventif de dates potentiellement stockées en numéro de série Excel.
Audit réel du 2026-06-07 : AUCUNE date série brute détectée. Toutes les dates sont correctement
stockées comme objets datetime par Excel. Anomalie sans objet sur données réelles.
Résolution : SANS OBJET — confirmé par audit CTR-2026-06-001.

---

### ANO-2026-06-004
Date : 2026-06-04 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : OUVERT
Code : VRBO_MONTANT_NON_RENSEIGNE
Source : MASTER_FACT_HA_Reservations
PK : 32 réservations vrboical — paymentStatus = Unknown (29 au run 20260523, 32 au run 20260608 — +3 nouvelles réservations VRBO)
Description : Montant financier indisponible dans Hostaway pour ces 32 réservations.
Traitement attendu : saisie manuelle dans MASTER_FACT_MAN_ReservationsHorsHostaway au Lot 4.
Résolution : En attente — Lot 4

---

### ANO-2026-06-005
Date : 2026-06-04 | Lot : 2 — Réconciliation | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-08
Code : REFERENTIEL_ORPHELIN
Source : REF_Setup.xlsm — REF_Logements
PK : listingMapId = 497801
Description : listingMapId 497801 présent dans REF_Logements (sur_hostaway = OUI ?) mais ABSENT de l'export Hostaway.
Cas : logement désactivé, archivé, ou erreur de saisie dans le référentiel.
⚠ Cas DIFFÉRENT de ANO-001 : ici c'est le référentiel qui pointe vers Hostaway, pas l'inverse.
Traitement attendu : trancher au Lot 2 — désactiver dans REF_Logements ou vérifier si listingMapId est correct.
Résolution : CORRIGÉ — 497801 est l'ancien identifiant Hostaway de LOG_0009 (T3 Montaudran).
Le logement a été retiré puis recréé dans Hostaway → nouvel ID = 556954.
Action : LOG_0009.hostaway_listing_id mis à jour : 497801 → 556954. Nom mis à jour : "T3 Montaudran".
497801 conservé comme alias historique actif dans REF_Mapping_Logements (MAP_LOG_0039, actif=OUI).
556954 ajouté comme nouvel alias Hostaway (MAP_LOG_0084/0085/0086).

---

### ANO-2026-06-006
Date : 2026-06-04 | Lot : 2 — Réconciliation | Sévérité : INFO | Statut : CORRIGÉ 2026-06-08
Code : LISTING_CONFIRME_HORS_HOSTAWAY
Source : REF_Setup.xlsm — REF_Logements
PK : logement_id = LOG_0016, ancien listingMapId = 480780
Description : Logement avec sur_hostaway = NON dans REF_Logements. Cohérent architecturalement (logement géré sans Hostaway).
⚠ Cas DIFFÉRENT de ANO-001 et ANO-005 : ce n'est pas un orphelin, c'est un logement volontairement hors Hostaway.
Traitement attendu : confirmer au Lot 2 que sur_hostaway = NON est bien intentionnel et documenter.
Résolution : CORRIGÉ — le sur_hostaway=NON était incorrect. 480780 est l'ancien ID Hostaway de LOG_0016.
Le logement a été recréé dans Hostaway sous le nouvel ID 515523 (archivé).
LOG_0016.sur_hostaway corrigé NON → OUI. LOG_0016.hostaway_listing_id : 480780 → 515523.
480780 conservé comme alias historique actif dans REF_Mapping_Logements (MAP_LOG_0073, actif=OUI).

---

## Note — les 4 cas listingMapId — RÉSOLUS au Lot 2 (2026-06-08)

| PK | Nature | Résolution |
|---|---|---|
| 515523 | Nouvel ID Hostaway de LOG_0016 (remplace 480780) | Alias actif MAP_LOG_0082 → LOG_0016. LOG_0016.hid=515523, sur_hostaway=OUI |
| 556954 | Nouvel ID Hostaway de LOG_0009 (remplace 497801) | Alias actif MAP_LOG_0084/0085/0086 → LOG_0009. LOG_0009.hid=556954 |
| 497801 | Ancien ID Hostaway de LOG_0009 | Alias historique actif MAP_LOG_0039. Conservé pour résolution des anciennes réservations |
| 480780 | Ancien ID Hostaway de LOG_0016 | Alias historique actif MAP_LOG_0073. Conservé pour résolution des anciennes réservations |

---

---

### ANO-2026-06-007
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_STATUTS_VALEURS_CONTROLE_MANQUANTES
Source : REF_Setup.xlsm — REF_Statuts
PK : famille statut_controle
Description : Valeurs fermées de statut_controle manquantes dans REF_Statuts : VALIDE, BLOQUANT, IGNORE_JUSTIFIE.
A_CONTROLER présent (famille import) mais les 3 autres requises par PLAN Lot 0 absentes.
Sans ces valeurs, les listes déroulantes de saisie ne peuvent pas pointer vers le référentiel.
Résolution : CORRIGÉ — VALIDE (STAT_021), BLOQUANT (STAT_022), IGNORE_JUSTIFIE (STAT_023) ajoutés en famille statut_controle.

---

### ANO-2026-06-008
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_STATUTS_PAYOUT_ABSENT
Source : REF_Setup.xlsm
PK : onglet REF_Statuts_Payout
Description : Onglet REF_Statuts_Payout requis par PLAN Lot 0 (Archi §23.2bis, D021) absent du fichier.
Sans cet onglet, le Lot 1 (Hostaway) ne peut pas affecter les statuts de calcul payout.
Résolution : CORRIGÉ — onglet créé avec 6 valeurs (NORMAL, ANNULE_SANS_PAYOUT, ANNULE_AVEC_PAYOUT, PAYOUT_ABSENT, PAYOUT_INCOMPLET, A_CONTROLER).

---

### ANO-2026-06-009
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_CLOTURE_MENSUELLE_ABSENTE
Source : REF_Setup.xlsm
PK : onglet REF_Cloture_Mensuelle
Description : Onglet REF_Cloture_Mensuelle requis par PLAN Lot 0 (D024) absent. Structure vide requise
pour que le Lot 8 puisse l'alimenter (statuts mois OUVERT/EN_CONTROLE/CLOTURE).
Résolution : CORRIGÉ — onglet créé (structure vide, 7 colonnes : mois, statut_mois, date_passage_controle, date_cloture, nb_lignes_bancaires_non_classees, nb_controles_bloquants_ouverts, commentaire).

---

### ANO-2026-06-010
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_PARAMETRES_GENERAUX_INCOMPLETS
Source : REF_Setup.xlsm — REF_Parametres_Generaux
PK : TAUX_HORAIRE_MENAGE_INTERNE, ARRONDI_DECIMALES, TOLERANCE_ARRONDI_LIGNE_EUR, TOLERANCE_ARRONDI_CUMUL_EUR
Description : 4 paramètres requis par PLAN Lot 0 absents. TAUX_HORAIRE_MENAGE_INTERNE requis pour Lot 6b
(M04 code le taux en dur à 10 EUR/h et doit migrer vers ce référentiel). Les 3 paramètres d'arrondi (D035)
requis pour les contrôles d'arrondi des Lots 10/11.
Résolution : CORRIGÉ — 4 params ajoutés (PARAM_004 à PARAM_007) avec valeurs 10 / 2 / 0.10 / 1.00.

---

### ANO-2026-06-011
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_INTERVENANTS_SCHEMA_INCOMPLET
Source : REF_Setup.xlsm — REF_Intervenants
PK : INT_0001 à INT_0005
Description : 3 colonnes requises absentes (nom_normalise, date_debut_validite, date_fin_validite).
type_intervenant en minuscules ('Interne'/'Externe') au lieu de MAJUSCULES ('INTERNE'/'EXTERNE').
Sans nom_normalise, le mapping Google Sheet → M04 (Lot 6b) est impossible.
Résolution : CORRIGÉ — 3 colonnes ajoutées, type_intervenant normalisé en MAJUSCULES,
nom_normalise calculé sans accents (IMENE, KHEIRA, MOUNIR, AISSATA, IMRANE).

---

### ANO-2026-06-012
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-07
Code : REF_LOGEMENTS_CODES_HORS_PARC_ABSENTS
Source : REF_Setup.xlsm — REF_Logements
PK : APPARTEMENT_DIVERS, LOGEMENT_DIVERS
Description : Codes techniques hors parc requis par PLAN Lot 0 absents. Nécessaires pour Lots 2 et 6b
(cas mapping orphelin, appartements hors parc réel). Sans eux, le système ne peut pas affecter un logement_id
aux flux hors parc sans violer la règle interdisant APPARTEMENT_DIVERS pour masquer un mauvais mapping.
Résolution : CORRIGÉ — 2 logements techniques ajoutés (actif=OUI, sur_hostaway=NON, forfait=0,
commentaire explicite "jamais utilisé pour masquer un mauvais mapping").

---

## Anomalies corrigées

*(voir statut CORRIGÉ sur ANO-002, ANO-003, ANO-007 à ANO-012 ci-dessus)*

---

### ANO-2026-06-014
Date : 2026-06-08 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-08
Code : LISTING_ORPHELIN_A_CONTROLER
Source : MASTER_REF_HA_Listings / MASTER_CTRL_HA_Anomalies
PK : listingMapId = 556954
Description : Listing 556954 (T3 Montaudran, Toulouse) présent dans l'export Hostaway (specialStatus=archived, actif=NON),
ABSENT de REF_Logements. Détecté au run 20260608_134253 (CTR-2026-06-004).
22 réservations historiques attachées — incluses dans les 23 LISTING_ORPHELIN_A_CONTROLER du run.
Cas analogue à ANO-001 (515523) : ancien logement archivé, jamais saisi dans le référentiel.
⚠ Ne pas intégrer comme logement actif avant arbitrage.
Traitement attendu : ajouter dans REF_Setup.xlsm / REF_Logements au Lot 2 avec statut ORPHELIN_A_CONTROLER.
Résolution : CORRIGÉ — 556954 est le nouvel identifiant Hostaway de LOG_0009 (T3 Montaudran).
Le logement avait pour ancien ID 497801 (absent de l'export). Recréation Hostaway → nouvel ID.
Action : 556954 ajouté comme alias Hostaway de LOG_0009 (MAP_LOG_0084/0085/0086).
LOG_0009.hostaway_listing_id : 497801 → 556954. Nom mis à jour : "T3 Montaudran".
Audit Lot 2 : 17/17 listings mappés, 0 orphelin résiduel.

---

## Anomalies ignorées justifiées

### ANO-2026-06-013
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : IGNORE_JUSTIFIE
Code : REF_CARTES_PAIEMENT_SUFFIXE_MANQUANT
Source : REF_Setup.xlsm — REF_Cartes_Paiement
PK : CARTE_002 (Ewan)
Description : suffixe_carte = 'XXXX' (placeholder). Le vrai suffixe de la carte d'Ewan n'est pas renseigné.
Justification : Non bloquant avant Lot 8. Le suffixe sert uniquement au rapprochement bancaire (Lot 8).
Action à faire au Lot 8 : remplacer 'XXXX' par le vrai suffixe avant traitement des exports bancaires.

---

### ANO-2026-06-029
Date : 2026-06-29 | Lot : Transverse - HORS_PARC_TECHNIQUE | Severite : A_CONTROLER | Statut : REGLE_DE_CONTROLE_A_APPLIQUER
Code : STATUT_PARC_INVALIDE
Source : REF_Logements.statut_parc
PK : logement_id
Description : `statut_parc` vide, invalide ou inconnu. Cette situation ne doit jamais etre assimilee a `GERE`.
Traitement attendu : produire `A_CONTROLER` avec code anomalie `STATUT_PARC_INVALIDE` et interrompre tout calcul economique pour le logement concerne.
Resolution : Regle centrale implementee dans `lib_parc.py`; referentiel actuel renseigne uniquement `GERE` et `HORS_PARC_TECHNIQUE`.

### ANO-2026-07-030
Date : 2026-07-07 | Lot : APP-3b - Nouvelle charge guidee | Severite : INFO | Statut : RESOLU
Code : CHG_GUIDE_CONFLITS_MODELE
Source : Mission module Nouvelle charge guidee vs referentiels/circuits existants.
Description : Trois conflits potentiels identifies et tranches (sans contradiction residuelle) :
  1. IK demande visible dans Nouvelle charge mais deja gere par Lot7 (D026 exclut IK de SAISIE_Charges_Flux).
     -> IK reste hors formulaire (circuit Lot7 preserve, D026 intact). D-CHG-GUIDE-07.
  2. Forfait client logiciel/consommables (CHG_016) saisi comme charge alors que c'est une ligne de
     facturation proprietaire. -> CHG_016 retire de la saisie ; reste porte par la prefacture Lot12 et
     REF_Charges_Recurrentes/REF_Logements. D-CHG-GUIDE-06.
  3. Achat divers (CHG_018) classe MENAGE mais devant proposer impact menage au choix.
     -> CHG_018 reclasse GLOBAL (impact menage au choix). Migration controlee documentee.
Resolution : Decisions D-CHG-GUIDE-01 a 07 ; aucune ecriture reelle ; circuits Lot6c/Lot7/Lot12 non doubles.


## ANO-2026-07-27-01 — Lot6b atteint le reseau et fait entrer des donnees reelles en recette

GRAVITE : HAUTE (confidentialite + integrite du jeu de recette).
STATUT : CORRIGE.

CONTEXTE : au tour precedent, la chaine Menages (lot6b..lot6f) a ete ajoutee aux chaines lancables
depuis /calculs, qui execute chaque lot par appel DIRECT du script avec PROJECT_ROOT=data_recette.

CONSTAT : lot6b_m04_menages_internes.py resout une URL depuis REF_Sources_Systeme (SRC_011) et
interroge REELLEMENT la feuille Google des declarations internes. Lance ainsi il a rapatrie
40 lignes de donnees reelles :
  [lot6b] URL REF OK (SRC_011) | CSV 40 lignes | normalisees 24 | M04 MASTER 24 lignes
  [lot6b] mai 2026 par intervenant : {('INT_0002', 'Kheira'), ('INT_0001', 'Imene')}
Des prenoms reels d'intervenantes ont ete ecrits dans data_recette :
  02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx (SOURCE_RAW, MASTER)
  02_TRAVAIL/Lot6b_DeclarationsInternes/MASTER_NORM_Declarations_Internes.xlsx

PORTEE : aucune donnee reelle n'a ete ECRITE (les sources reelles n'ont ete que lues) ; data_recette
est gitignore, donc rien n'a ete commite. L'exposition est restee locale.

EFFET DE BORD DIAGNOSTIQUE : le mapping des noms d'appartements reels vers le parc FICTIF echouait
(24 lignes A_CONTROLER), donc M04 sortait avec logement_id = None sur toutes ses lignes. C'est CE
M04 pollue qui faisait ensuite echouer lot6d :
  TypeError: '<' not supported between instances of 'NoneType' and 'str'
Le rapport precedent attribuait cet echec a lot6d ou au jeu de recette. C'ETAIT FAUX : lot6d n'a
aucun defaut, et build_data_recette non plus. La cause etait le CHEMIN D'EXECUTION.

CAUSE RACINE : menages_chaine_service existe precisement pour cela — il copie les sources dans un
workspace isole et substitue stub_lib_sheet_source.py a l'acces reseau. Le chemin /calculs
contournait cette protection.

CORRECTION :
- Lot.exige_workspace_controle marque toute la chaine Menages ;
- executer_lot() refuse ces lots AVANT tout lancement, avec un message qui renvoie vers
  /menages/chaine ; aucun processus n'est demarre ;
- la chaine menages n'est plus proposee dans les chaines de /calculs ;
- data_recette regenere : plus aucune donnee reelle (verifie par balayage des classeurs) ;
- 4 tests de non-regression, dont un qui verifie que le garde-fou ne deborde pas sur les chaines
  aval et charges.

PREUVE DE BON FONCTIONNEMENT : via l'orchestrateur legitime, la chaine complete passe —
hostaway_stub, lot6b, lot6c, lot6d, lot6e, lot6f, lot11 tous OK, statut SUCCES, reel_intact=True
(sources reelles verifiees inchangees par sha256).

LECON : un lot qui atteint le reseau ou des sources hors racine ne doit jamais etre expose a une
execution directe. Le pilotage doit passer par l'orchestrateur qui porte les garde-fous — meme
lecon que lot3, sur un risque plus grave.


## ANO-2026-07-28-01 — Donnees reelles codees en dur dans les moteurs menages

GRAVITE : MOYENNE (confidentialite + testabilite).
STATUT : CORRIGEE (2026-07-28) — issue A retenue et implementee.

CONSTAT : deux moteurs de la chaine menages embarquent des donnees reelles dans leur CODE SOURCE,
versionne :
- lot6b_m04_menages_internes.py ligne 37 :
    INTMAP = {"imene": ("INT_0001","Imene"), "kira": ("INT_0002","Kheira"), ...}
  prenoms reels d'intervenantes, servant de cle de mapping ;
- lot6c_menages_externes.py lignes 4-5 et 120+ : references de factures reelles
  (FAC-2026-05-AISSATA-001), noms de prestataires (Kandia DIABATE, MH Entreprise), montants.

CONSEQUENCE 1 (confidentialite) : ces donnees sortent du perimetre des sources et vivent dans le
depot. Elles ne transitent pas par data_recette, mais elles sont dans le code.

CONSEQUENCE 2 (testabilite) : AUCUN jeu de recette fictif ne peut traverser la chaine menages.
INTMAP etant indexe par prenom normalise, un intervenant fictif donne intervenant_id = None, et
lot6d echoue sur sorted() -> TypeError: '<' not supported between NoneType and str. Utiliser les
vrais prenoms pour contourner reinjecterait de la PII en recette : ecarte (cf. ANO-2026-07-27-01).

IMPACT : pools de courses, ventilation REC_002 et chaine Menage complete restent non exercables
sur donnees fictives. La chaine reste exercable sur l'arbre REEL en copies (7/7, reel_intact=True).

ISSUES POSSIBLES (a arbitrer) :
A. Externaliser INTMAP et les donnees de lot6c vers REF_Intervenants / REF_Setup. Rend les moteurs
   pilotables par referentiel et la recette fictive possible. Modification de moteur.
B. Accepter que la chaine ne soit exercable que sur l'arbre reel, en copies et en lecture seule.
C. Enrichir le jeu de recette d'intervenants correspondant aux cles d'INTMAP sans reprendre les
   prenoms reels : IMPOSSIBLE en l'etat, INTMAP est indexe par prenom.

ISSUE A RETENUE ET IMPLEMENTEE (2026-07-28) :

lot6b_m04_menages_internes.py : INTMAP (dict fige de prenoms reels) supprime. Le mapping
prenom -> intervenant_id est desormais construit dynamiquement depuis
REF_Intervenants.nom_normalise (D104) — un referentiel fictif definit ses propres nom_normalise et
le mapping fonctionne identiquement, sans aucune donnee reelle requise. L'alias orthographique reel
("Kira" = Kheira, D104) est externalise dans un nouveau module optionnel
`02_TRAVAIL/_data_lot6b_alias_reel.py`, importe via try/except ImportError (absence sans effet).

lot6c_menages_externes.py : RAW_MANUEL (transcription des factures reelles mai 2026), PREST_MAP,
LOG_MAP, PREST_BRUT, ainsi que les mappings pcode/mode_paiement bases sur des intervenant_id reels
en dur, sont externalises dans un nouveau module optionnel
`02_TRAVAIL/_data_lot6c_secours_reel.py`. En son absence, le moteur bascule sur un repli fictif
local (RAW_FICTIF, 2 lignes, aucun nom reel) aligne sur les identifiants FICTIF de
`recette/build_data_recette.py` (INT_B, LOG_A1/LOG_B1). Les textes du README genere (section
SOURCES, decision D086) sont rendus conditionnels au mode reellement actif — plus de nom reel
injecte dans un artefact de recette.

`recette/build_data_recette.py` exclut explicitement ces deux modules de la copie des scripts
moteur vers `data_recette/02_TRAVAIL` (`EXCLUS_DONNEES_REELLES`).

PREUVES :
- Compatibilite historique lot6b : intmap reconstruit depuis REF_Intervenants reel resout les
  3 cles (imene, kira, kheira) vers les memes intervenant_id que l'ancien INTMAP (verifie).
- Compatibilite historique lot6c : rerun sur l'arbre reel (module _data_lot6c_secours_reel.py
  present) — 13 lignes SOURCE_RAW, 12 VALIDE / 1 BLOQUANT, reconciliation exacte (1439€ / 942€),
  identique au comportement de l'ancien script (verifie par diff colonne-a-colonne du MASTER,
  hors horodatage/hash — un seul ecart preexistant sur la ligne T.2-65/Gabriel, confirme
  independant du refactor par rerun de l'ancien script tel quel).
- Recette entierement fictive : lot6b et lot6c executes sur `data_recette` (module reel absent,
  ImportError attendue) produisent des lignes VALIDE avec intervenant_id/logement_id fictifs
  (INT_A/INT_B, LOG_A1/LOG_B1), sans aucun intervenant_id nul — la chaine ne bloque plus lot6d.
  Scan de l'export lot6c genere sur data_recette : 0 occurrence des noms/references reels
  (Aissata, Mounir, Kandia, DIABATE, MH Entreprise, prenoms proprietaires).
- Non-regression : suites test_menages*.py + test_charges_pipeline.py — 148 passed, 22 skipped,
  0 echec.

Detail (a mettre a jour) : 41_MODULE_MENAGES_ETAT_FINAL.md section 7bis, 48_ROADMAP_RESTANTE_PROJET.md.


## LECON (2026-07-29) — recette navigateur ad hoc sur le coeur Comptabilite : PROJECT_ROOT oublie

Pendant la recette navigateur du nouveau journal VENTES (adaptateur Lot12, mission Comptabilite
coeur), un serveur de recette a ete demarre avec seulement `APP_DATA_DIR=<data_recette>/app_data`,
sans `PROJECT_ROOT=<data_recette>`. Consequence : la base SQLite (ecritures, factures) etait bien
isolee dans data_recette, mais les chemins Excel Lot12 (`cfg.MASTER_NET_PROPRIETAIRE`,
`MASTER_FACT_PROPRIETAIRES`, etc.) sont derives de `PROJECT_ROOT`, pas de `APP_DATA_DIR` — ils
pointaient donc vers l'arbre REEL. Generer les ecritures VENTES a lu les VRAIS noms et montants de
10 proprietaires reels depuis Lot12 (lecture seule, aucune source reelle modifiee) et les a ecrits
dans le libelle des ecritures de `data_recette/app_data/app.db` — une PII reelle dans un artefact de
recette cense etre entierement fictif.

DETECTION : immediate, en relisant le texte de la page apres generation (noms reels visibles).
CORRECTION : `data_recette/app_data/app.db` supprime et regenere via `recette/build_data_recette.py`
(idempotent) ; verifie par scan de tokens PII sur la base regeneree — 0 occurrence. Aucune source
reelle modifiee a aucun moment (Lot12 est lu, jamais ecrit, par ce parcours).

LECON : demarrer un serveur de recette qui touche a un moteur lisant l'arbre reel (Lot9/Lot10/Lot12,
comme ici) exige `PROJECT_ROOT=<data_recette>` en plus de `APP_DATA_DIR` — sinon seule la base
applicative est isolee, pas les sources Excel lues par les adaptateurs. Meme categorie de risque que
la lecon deja consignee plus haut sur les executions directes hors orchestrateur : un point d'entree
qui lit l'arbre reel doit toujours etre lance avec la racine de recette explicitement forcee, jamais
suppose par defaut.


## ANOMALIE DE TEST (2026-07-29) — flake pre-existant, ordre-dependant, confirme non lie au chantier

Suite complete rejouee en tranches apres la mission Comptabilite coeur : en plus de l'echec connu
`test_appsec1_diagnostic`, un second echec est apparu dans une tranche (decoupage ad hoc, pas le
decoupage habituel a 4 voies) :

`tests/test_proprietaires_reglements.py::test_route_dashboard_200` echoue quand precede, dans le
meme processus pytest, par `test_menages_cave_et_pools.py`, `test_menages_cycle_routes.py`,
`test_menages_recalcul.py`, `test_no_metier_calc.py`, `test_pilotage_mensuel_resilience.py`,
`test_proprietaires.py` (dans cet ordre) : le rendu de `/proprietaires-reglements` contient alors un
bandeau de diagnostic avec chemin Windows complet et donnees non masquees (adresse `SECRETVILLE` de
fixture), au lieu du rendu normal masque. Passe seul, ou avec seulement `test_proprietaires.py`
avant : 100% vert.

VERIFICATION : reproduit a l'identique sur le commit `9bb24a7` (avant tout changement de ce tour,
via `git stash`), confirmant que ce n'est PAS une regression introduite par la mission Comptabilite
coeur (aucun fichier de ce tour ne touche `proprietaires_reglements*`, `base.html`, ou les fichiers
menages listes ci-dessus). Deterministe (reproduit 2 fois de suite avec le meme ordre), donc un vrai
probleme d'isolation entre tests (etat global — probablement un flag de diagnostic ou `RECETTE_MODE`
mute par un test amont sans passer par `monkeypatch`, a investiguer), pas un flake aleatoire.

STATUT : OUVERT, ordre-dependant, non bloquant (n'affecte que cette combinaison precise de fichiers,
jamais rencontree dans le decoupage a 4 tranches habituel qui a servi de base a tous les totaux
« suite complete » consignes dans `HANDOFF_CANONIQUE.md`). A investiguer si ce decoupage devient la
norme, ou en le bisectant explicitement (retirer un fichier a la fois du groupe amont) — non fait ce
tour faute de temps, le probleme n'appartenant pas au perimetre de cette mission.

Confirme a nouveau le 2026-07-31 (mission fermeture Analytique/Resultats, Bloc 8) : le decoupage a
6 shards utilise pour la campagne de tests complete separe deliberement les fichiers declencheurs
(shard 03) de `test_proprietaires.py`/`test_proprietaires_reglements.py` (shard 04) — le flake n'est
pas apparu, coherent avec l'analyse ci-dessus. Toujours OUVERT, toujours non lie a ce chantier.


## ANOMALIE REELLE TROUVEE EN RECETTE NAVIGATEUR (2026-07-31) — reconciliation B grains incompatibles

Trouvee pendant la recette navigateur reelle (Bloc 7, mission fermeture Analytique/Resultats) :
pipeline aval lance sur donnees neuves (mois 2026-06), la reconciliation **B — Lot10 <-> Analytique**
affichait un statut `A_CONTROLER` avec un ecart de 10 035,00 EUR des qu'un mois etait selectionne
dans l'ecran `/resultats/reconciliation`, alors que toutes les autres verifications de coherence
etaient au vert.

CAUSE : `Lot10 GLOBAL` (feuille `GLOBAL` de `MASTER_CALC_Resultats.xlsx`) est un total unique sur
TOUT le jeu de donnees, sans grain mensuel. La route `/resultats/reconciliation` (et son export CSV)
appelait `recon.lot10_vs_analytique(mois=mois)`, qui filtrait le cote Analytique sur le mois
selectionne avant de le comparer au total global non filtre — une comparaison de grains
incompatibles, exactement le type d'erreur que la mission interdisait explicitement. Les tests
unitaires ne l'avaient jamais detecte car leur fixture ne portait qu'un seul mois (le total global
coincidait par hasard avec le total mensuel).

CORRECTION : `app/routes/resultats.py` — B ignore desormais le filtre mois (route HTML et export
CSV), coherent avec H (dont B est l'alias interne) : elle est par construction a l'echelle du jeu de
donnees complet. Note explicative ajoutee au template `resultats_reconciliation.html`. Test de
non-regression avec fixture Excel a deux mois
(`test_reconciliation_b_reste_ok_quel_que_soit_le_mois_filtre`, `tests/test_resultats_routes.py`).

LECON : une reconciliation contre un total agrege sans dimension temporelle ne doit jamais recevoir
un filtre temporel provenant de l'ecran — verifier, pour chaque reconciliation, que le grain des deux
cotes est explicite et compatible AVANT de brancher un filtre d'ecran dessus, pas seulement sur des
donnees de test a un seul mois.


## ANOMALIE DE TEST TROUVEE EN CAMPAGNE PAR SHARDS (2026-07-31) — faux positif garde APP-0

Trouvee en executant la suite complete par shards (Bloc 8, jamais rejouee jusque-la avec le fichier
`lot9_flux_reader.py` present, cree lors du Bloc 2 de la meme mission) :
`test_no_metier_calc.py::test_no_import_of_travail_modules` echouait, signalant un import direct
d'un module 02_TRAVAIL dans `comptabilite_reconciliations_service.py`.

CAUSE : le garde APP-0 detecte toute occurrence litterale du texte `"import lot"` dans le code de
`app/` pour interdire un import direct des scripts moteur `02_TRAVAIL/lotN_*.py`. Le nom du reader
`lot9_flux_reader.py` declenchait un faux positif : la ligne `from app.readers import
lot9_flux_reader as lot9` contient litteralement `"import lot9..."`. Le reader lui-meme est un
module `app/readers/` legitime (lecture seule d'un fichier Excel Lot9), pas un import du moteur.

CORRECTION : renomme en `flux_unifie_reader.py` — coherent avec la convention deja suivie par tous
les autres lecteurs (`charges_reader.py`, `proprietaires_reglements_reader.py`,
`controles_cloture_reader.py` : jamais de prefixe `lotN`). Seul importeur (`comptabilite_
reconciliations_service.py`) mis a jour ; aucun test ne referencait le module par son nom.

LECON : ne jamais nommer un module `app/` avec un prefixe `lotN_` meme s'il ne fait que LIRE une
sortie Lot9/Lot10/etc. — le garde APP-0 le traite comme un import moteur interdit. Suivre la
convention descriptive deja en place pour tous les autres lecteurs.


## ANOMALIE REELLE TROUVEE EN RECETTE GLOBALE SUR COPIES (2026-08-01) — source Banque reelle absente

Trouvee pendant la recette globale sur copies controlees des donnees reelles (mission dediee,
HEAD `ede8c52`) : lancement du pipeline aval (`lot4quater->lot9->lot10->lot11->lot12->lot13`) sur
une copie fidele de `01_SOURCES_BRUTES/`+`02_TRAVAIL/` pour verifier que la chaine reste rejouable
sur donnees reelles. `lot4quater` SUCCES, **`lot9` ECHEC** : `BLOQUANT [CTR-9-001] Source
manquante : BANQUE_LOT8_IMPORT`.

CAUSE : `lot9_construire_flux.py` exige trois sources en dur (`MASTER_CALC_Reservations`,
`MASTER_FACT_MEN_MenagesExternes`, `BANQUE_LOT8_IMPORT`) via son controle `CTR-9-001`. Verification
sur le reel : le dossier `02_TRAVAIL/Lot8_Banque/` **n'existe pas du tout** dans l'arbre reel — le
module Banque n'a jamais ete execute en reel, uniquement dans `data_recette` fictif. Les sorties
Lot9-13 reelles actuellement presentes (datees du 2026-07-24) proviennent donc d'une execution
anterieure a l'ajout de ce controle, ou d'une source Banque qui existait alors et a disparu depuis.

CE N'EST PAS UN DEFAUT DE CODE : le controle `CTR-9-001` fonctionne exactement comme concu (refuse
de produire un resultat sur une source metier absente plutot que de fabriquer un flux a partir de
rien). Aucune correction de code appliquee. Aucune donnee reelle modifiee ou fabriquee pour
satisfaire ce controle (interdit par la mission).

VERIFICATION DE NON-REGRESSION : la reconciliation Lot9<->Lot10 (A) sur les sorties Lot9-13
existantes confirme leur coherence mutuelle (ecart 0,00 EUR sur 24 mois reels) — ces sorties ne
sont pas cassees, seulement non regenerables aujourd'hui faute de source Banque.

STATUT : OUVERT, decision humaine requise (alimenter la source Banque reelle avant toute nouvelle
execution complete du pipeline aval, ou accepter que les sorties actuelles restent figees).
Detail complet : `55_MATRICE_ECARTS_CONTRATS_REELS.md`, `60_VERDICT_GO_NO_GO.md`.


## SUITE (2026-08-02) — contrat Lot8 remonte a la cause racine : sortie, pas source, jamais produite

Audit ciblé de `lot8a_banque_import.py` (mission dediee) : `BANQUE_LOT8_IMPORT.xlsx` est une
**sortie** de Lot8, jamais une source deposable directement. Sa source brute d'entree est
`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` (export Credit Mutuel, compte
`02211 00021321603`, feuille `Cpt 02211 00021321603`, en-tete ligne 5, donnees ligne 6). Ce fichier
brut **n'existe nulle part** — le dossier `01_SOURCES_BRUTES/Banque/` n'existe pas physiquement sur
disque, ni dans le reel ni dans les copies. Lot8 est 100% executable hors reseau (openpyxl pur),
mais rien a traiter sans ce depot humain.

Contrat Lot8->Lot9 verifie coherent : `SRC_BNQ` de `lot9_construire_flux.py` pointe exactement vers
`OUT_FILE` de `lot8a`, aucune divergence code/documentation.

CAS B CONFIRME (aucune source bancaire brute exploitable) : aucun contournement code, aucune
donnee fabriquee. Action precise documentee pour l'utilisateur dans
`61_CONTRAT_SOURCE_BANQUE_LOT8.md` : exporter le releve, le deposer sous
`01_SOURCES_BRUTES/Banque/` (reel), executer `lot8a` avant toute nouvelle recette pipeline
complete.

STATUT : OUVERT, decision et geste humains requis. Verdict formalise :
`60_VERDICT_GO_NO_GO.md` -> **NO GO — SOURCE BANQUE REQUISE**.


## VERIFICATION (2026-08-02, suite) — fichier annonce comme fourni, toujours absent en pratique

Mission recue annoncant un releve Credit Mutuel "nouvellement fourni" sous
`01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx`. Verification directe avant toute
lecture metier (regle de la mission : controler le fichier avant tout traitement) :

- `01_SOURCES_BRUTES/Banque/` **n'existe toujours pas** sur disque, dans le reel ;
- recherche large (nom de fichier, motif `*BRUT_Banque*`, `*CreditMutuel*`) sur tout le worktree,
  sur l'environnement de copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/`) et sur
  tout fichier `.xlsx` modifie depuis le dernier verdict : **aucune trace du fichier annonce** ;
- 85/85 hashes des sources reelles reference re-verifies identiques a la baseline (aucun
  changement, ni ajout, ni modification).

CONCLUSION : la premisse de la mission ("fichier nouvellement fourni") ne correspond pas a l'etat
observe du disque. Aucun contournement tente, aucune donnee fabriquee, aucune execution de Lot8
lancee (rien a traiter). Signale a l'utilisateur plutot que suppose resolu.

STATUT : INCHANGE. Verdict toujours `60_VERDICT_GO_NO_GO.md` -> **NO GO — SOURCE BANQUE REQUISE**.


## ANOMALIE REELLE TROUVEE EN RECETTE (2026-08-02, suite) — releve fourni, contrat incompatible

Le fichier `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` a ete reellement depose
(141986 octets, SHA256 `a84c9b51b1c0eb50d17216272bd3c6cf2669d159bf7e1299c2b762face0ca4a8`).
Verification avant tout traitement metier : taille > 0, extension correcte, 5 feuilles
(`Synthese`, `Mouvements`, `Mensuel`, `Controles`, `Sources`).

Copie controlee vers l'environnement de copies (hash source=copie verifie identique). Execution
REELLE de `lot8a_banque_import.py` sur la copie (jamais sur le reel) :

```
[OK] Source brute : ...SOURCES_COPIEES\01_SOURCES_BRUTES\Banque\2026_03_BRUT_Banque_CreditMutuel.xlsx
[ERREUR BLOQUANT] Feuille "Cpt 02211 00021321603" absente.
  Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']
EXITCODE=1
```

CAUSE : le fichier fourni est un **rapport consolide** (titre interne "Releve bancaire consolide -
WONDERBNB", note "ancien consolide retenu jusqu'au 31/05/2026, puis releve du 01/08/2026
prioritaire"), pas l'export brut Credit Mutuel que `lot8a_banque_import.py` attend. Meme compte
(RIB `10278 02211 00021321603` = `CM_02211_00021321603`), mais feuille `Mouvements` a 12 colonnes
(`N°`, `Date operation`, `Date de valeur`, `Libelle`, `Debit`, `Credit`, `Montant net`, `Solde
consolide`, `Devise`, `Source du releve`, `Mois`, `Ligne source`) contre les 7 attendues
(Date|Valeur|Libelle|Debit|Credit|Solde|Devise), et couvre 9 mois (03/11/2025->01/08/2026) au lieu
du mois nominal 2026-03.

CE N'EST PAS UNE CORRECTION A APPLIQUER SILENCIEUSEMENT : aucun renommage de feuille, aucune
adaptation des colonnes lues par le script, aucune conversion du fichier n'a ete tentee — cela
aurait change le contrat metier de Lot8 unilateralement pour accepter un format non prevu.
`BANQUE_LOT8_IMPORT.xlsx` n'a pas ete produit. Fichier original jamais modifie (hash inchange
apres traitement, verifie). 85/85 hashes reels historiques re-verifies identiques ; le nouveau
releve ajoute au manifeste d'integrite (86e fichier suivi).

STATUT : OUVERT, decision humaine requise entre deux options : (1) fournir l'export brut natif
Credit Mutuel (feuille `Cpt 02211 00021321603`, 7 colonnes), ou (2) decider explicitement d'adapter
`lot8a_banque_import.py` pour consommer ce format consolide — un changement de contrat, pas une
correction de bug. Verdict formalise : `60_VERDICT_GO_NO_GO.md` -> **NO GO — SOURCE BANQUE
INCOMPATIBLE**. Detail complet : `61_CONTRAT_SOURCE_BANQUE_LOT8.md` (section « Suite »).


## RESOLU (2026-08-02, suite) — Lot8 accepte le format consolide, chaine aval rejouee avec succes

Decision prise et implementee : `lot8a_banque_import.py` accepte desormais le format consolide EN
PLUS du format natif Credit Mutuel historique (contrat non remplace). Detection par presence de
feuilles (`FORMAT_CREDIT_MUTUEL_NATIF`/`FORMAT_RELEVE_CONSOLIDE`/`FORMAT_INCONNU`, jamais par nom
de fichier). Deux adaptateurs (`lire_natif`/`lire_consolide`) convergent vers le meme `raw_rows`
canonique avant de rejoindre la normalisation deja existante.

PROVENANCE AUDITEE (30 min, avant toute correction) : le releve consolide fusionne 3 exports bruts
successifs du meme compte (`comptes_4_complet_lignes_releve(1).xlsx`, `comptes (6).xlsx`,
`comptes (7).xlsx`, cites dans son propre onglet `Sources`) — NON CIRCULAIRE, ne provient d'aucune
sortie Lot8/Lot9+. Methode de fusion et controles deja documentes par le fichier lui-meme (103
doublons retires, solde raccorde sans ecart).

POINT CRITIQUE CORRIGE DANS L'ADAPTATEUR : le format consolide renseigne toujours Debit ET Credit
(0 cote inactif, jamais vide), contrairement au natif ou le cote inactif est une cellule vide —
sans conversion `0 -> None`, chaque ligne aurait ete signalee a tort `BANQUE_DEBIT_CREDIT_DOUBLES`.

EXECUTION REELLE SUR COPIE : `lot8a` produit `BANQUE_LOT8_IMPORT.xlsx` (541 mouvements, 0
BLOQUANT, 1 A_CONTROLER pour la periode multi-mois). Totaux identiques au centime pres a ceux de
la feuille `Synthese` du fichier (51744,37 EUR debit / 52148,21 EUR credit). Idempotent (2
executions, memes totaux, memes identifiants mouvement_id). Fichier original jamais modifie (hash
inchange, verifie).

CHAINE AVAL REJOUEE AVEC SUCCES (lot9->lot10->lot11->lot12->lot13, scripts moteur directs) : tous
controles bloquants OK, REEL=COMPTABLE+HC verifie (291852,76 = 281328,60+10524,16, ecart 0,00 EUR),
idempotence confirmee sur un second passage lot9/lot10. Reconciliations rejouees via l'application
sur les sorties fraiches : A/B/D/H OK (ecart 0,00 EUR), C/E/F/G toujours A_CONTROLER/NON_DISPONIBLE
(attendu, 0 ecriture Comptabilite reelle). Securite : 0 fuite sur les pages et les 13 exports Power
BI.

ECART DE COPIE CORRIGE EN COURS DE ROUTE : `02_DONNEES_NORMALISEES/` (requis par lot11 pour
M04_MENAGES_PowerQuery.xlsx et l'historique cloture) n'avait pas ete copie dans l'environnement de
recette globale lors de sa creation initiale (2026-08-01) — complete ce tour, hash source=copie
verifie identique, ajoute au manifeste (88 fichiers reels de donnees suivis au total).

11 TESTS NOUVEAUX (`tests/test_lot8a_banque_import.py`, fixtures fictives, aucune donnee bancaire
reelle) : regression format natif, format consolide (detection, conversion 0->None, totaux,
multi-mois non bloquant, doublon detecte non masque, idempotence, consolide incomplet -> INCONNU),
format inconnu, fichier absent, confidentialite stdout. Suite complete tests/ (moteur) : 262
passes, 0 echec. Suite ciblee Banque application : 58 passes, 17 ignores, 0 echec.

STATUT : RESOLU (code). Verdict formalise : `60_VERDICT_GO_NO_GO.md` -> **GO POUR VALIDATION
HUMAINE PARTIELLE**. Detail complet : `63_CONTRAT_FORMAT_RELEVE_BANCAIRE_CONSOLIDE.md`.


## ANOMALIE NOUVELLE TROUVEE (2026-08-02, suite), HORS MANDAT, NON CORRIGEE — lot4quater scope au mois

En tentant de rejouer la chaine aval via l'ecran `/calculs` de l'application (plutot que les
scripts moteur directs), `lot9` echoue : `lot4quater_resoudre_source_reservations.py`, invoque
pour le mois 2026-06, regenere `VUE_FLUX` avec seulement 104 lignes (les mois a partir de
2026-06) au lieu de l'historique complet (1349 lignes, 2025-01->2027-02) — declenche `BLOQUANT
[CTR-9-003] VUE_FLUX volume suspect : 104 lignes (attendu >= 1000)`.

CAUSE : comportement de regeneration scope au mois de `lot4quater` quand invoque via le pipeline
applicatif avec un parametre `mois` — sans aucun rapport avec le format Banque (le controle qui
bloquait avant cette mission, CTR-9-001, est bien resolu ; c'est un AUTRE controle, CTR-9-003, qui
bloque maintenant, pour une raison entierement differente).

CE N'EST PAS CORRIGE CE TOUR : hors mandat strict de la mission Banque (« ne commence aucune
nouvelle fonctionnalite »). Sorties restaurees immediatement apres la tentative (7 fichiers, hash
verifie identique a l'original copie). La reprise de la chaine aval documentee ci-dessus a donc ete
faite en executant les scripts moteur directement — un contournement legitime pour isoler le sujet
Banque, pas une correction du probleme lot4quater lui-meme.

STATUT : OUVERT, mission dediee future necessaire pour permettre la re-execution de la chaine aval
depuis l'ecran applicatif `/calculs` pour un mois donne sans perdre l'historique complet.


## RESOLU (2026-08-02) — deux mois Lot10 manquants (2026-11, 2027-01) : cause identifiee, pas un bug

Remontee de la chaine Lot10 -> Lot9 -> Lot4quater (mission recette globale, suite). Les deux mois
sont deja absents du flux Lot9 (`MASTER_CALC_Flux.xlsx`). En remontant a `MASTER_CALC_Reservations_
Resolues.xlsx` : l'onglet `VUE_FLUX` (celui que `lot9_construire_flux.py` consomme reellement, `SRC_
RES`) les exclut deja (1349 lignes), alors que l'onglet `MASTER` (historique complet, 1391 lignes)
les contient.

CAUSE : chacun de ces deux mois ne porte qu'**une seule reservation** dans `MASTER` :
`RES-2026-11-HA-001` et `RES-2027-01-HA-001`, meme logement/proprietaire (LOG_0015/PROP_0011), meme
profil — `montant_retenu=0`, `statut_controle=A_CONTROLER`, `code_anomalie=DIRECT_SANS_SAISIE_HH`,
canal DIRECT (reservation directe sans saisie Hors-Hostaway). Lot4quater exclut legitimement ces
placeholders sans montant de sa vue financiere (`VUE_FLUX`) — comportement deja verifie coherent :
la meme ligne existe en 2026-12 (`RES-2026-12-HA-002`, meme profil exact) mais ce mois-la porte
aussi une reservation validee (`RES-2026-12-HA-003`, 317,77 EUR), donc le mois n'est pas vide et
apparait normalement en Lot9/Lot10.

CE N'EST PAS UN DEFAUT : filtrage upstream coherent et deja documente dans Lot4quater. Aucune
divergence entre Lot9 et Lot10 (Lot10 reflete fidelement ce que Lot9 recoit). Aucune correction de
code necessaire, aucun test rouge/vert requis.

STATUT : RESOLU (explique). Statut retenu : VIDE_VALIDE/NON_APPLICABLE, jamais un zero fabrique.
Reste ouvert cote metier : completer la saisie Hors-Hostaway pour LOG_0015/PROP_0011 (trois mois
consecutifs concernes : 2026-11, 2026-12, 2027-01) si l'activite reelle doit apparaitre dans les
Resultats. Detail complet : `62_RAPPORT_MOIS_LOT10_MANQUANTS.md`.


## RESOLU (2026-08-02, suite) — anomalie lot4quater/CTR-9-003 : artefact d'environnement, pas un bug

Suite a l'anomalie consignee precedemment ("lot4quater regenere VUE_FLUX scope au mois via
/calculs"), audit complet (45 min) : lecture integrale de `lot4quater_resoudre_source_
reservations.py` confirme qu'**aucun parametre mois n'existe dans ce script** — il reconstruit
TOUJOURS l'integralite de l'historique (mois ouverts depuis le live, mois clotures depuis HIST),
sans aucune notion de "mois demande".

CAUSE REELLE ETABLIE : execution directe sur la copie actuelle -> 1391 MASTER / 1349 VUE_FLUX,
correct. La difference avec le run defaillant precedent (104 lignes) tient entierement a
l'absence, A CE MOMENT PRECIS, de `02_DONNEES_NORMALISEES/historique_reservations/HIST_
Reservations_Cloturees.xlsx` dans l'environnement de copies (copie depuis, lors d'une mission
anterieure, pour debloquer lot11 — mais APRES le run /calculs qui avait echoue). Lot4quater a
applique son propre mecanisme de repli DEJA DOCUMENTE (CLOTURE_SANS_HIST -> A_CONTROLER, alerte
explicite dans les logs), jamais un crash ni une donnee fabriquee.

PREUVE DEFINITIVE : `/calculs` relance deux fois pour le mois 2026-06 DEPUIS L'ECRAN APPLICATIF
lui-meme (pas les scripts moteur) -> 6/6 lots SUCCES les deux fois, totaux identiques au centime
pres a l'execution moteur directe (REEL 291852,76 EUR), idempotent (ecart 0,00 EUR au 2e run).
Equivalence moteur direct <-> application confirmee.

AUCUNE CORRECTION DE CODE APPLIQUEE : le contrat etait deja correct. Aucune regle metier modifiee,
aucun contournement de CTR-9-003, aucune donnee fabriquee.

6 tests nouveaux (tests/test_lot4quater_resoudre_source_reservations.py, fixtures fictives,
script reel execute via runpy) fixant ce contrat : aucun filtre mois, HIST prime sur le live pour
les mois clotures, repli documente et signale si HIST absent, reinjection, idempotence, filtre
VUE_FLUX. Suite complete tests/ (moteur) : 268 passes, 0 echec.

STATUT : RESOLU (explique, pas un defaut). Detail complet :
`64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md`.


## CYCLE BANQUE COMPLET EXECUTE (2026-08-02, suite) — Lot8a/8b/8c, aucune anomalie bloquante

Lot8b (classification, 24 VALIDE/517 A_CONTROLER, 30 regles seed) et Lot8c (rapprochement, 166
Airbnb + 56 proprietaires en attente, 0 confirmation automatique) executes sur copie apres Lot8a
(format consolide). Les deux SUCCES, les deux applicables au format consolide sans adaptation
(Lot8b/8c ne lisent que NORM_Banque, structure canonique identique quel que soit le format
d'entree — aucun NON_APPLICABLE necessaire).

Effet mesure sur la chaine aval : Lot9 integre 24 flux de frais bancaires (TYPE_FLUX_016,
auparavant 0), Lot11 passe de BANQUE_NON_DISPONIBLE_GIT a BANQUE_DISPONIBLE. Nouveaux totaux
reconcilies et idempotents (REEL 291722,75 = COMPTABLE 281198,59 + HORS_COMPTA 10524,16, ecart
0,00). Reconciliations rejouees via l'application : A/B/D/H OK.

Deux points de securite pre-existants notes (hors mandat, non corriges) : bandeau MODE RECETTE
affichant un chemin absolu (comportement de template anterieur a cette mission) ; libelles
bancaires pouvant contenir des fragments de compte tiers (inherent au texte des releves, jamais
reproduit dans la documentation).

Campagne complete rejouee (TEST_SHARDS_RECETTE_GLOBALE.txt, 131 fichiers, 6 shards) : 2281 passes
/ 75 ignores / 1 echec pre-existant — identique a la reference, aucune regression.

STATUT : TERMINE pour le perimetre defini. Verdict formalise : `60_VERDICT_GO_NO_GO.md` ->
**GO POUR VALIDATION HUMAINE COMPLETE**. Detail complet : `65_RAPPORT_CYCLE_BANQUE_COMPLET.md`.


## FUITE DE CHEMIN ABSOLU DANS LE BANDEAU MODE RECETTE (2026-08-02, suite) — CORRIGEE

Un des deux points de securite pre-existants notes ci-dessus (bandeau MODE RECETTE) a ete pris en
charge explicitement par le mandat de cette mission (contrairement aux missions precedentes ou il
etait note "hors mandat"). Reproduit : `app.main._recette_globals` exposait `str(_cfg.RECETTE_ROOT)`
et `str(_cfg.DB_PATH)` bruts (chemin Windows complet, incluant le nom d'utilisateur reel) dans les
globals Jinja pousses sur chaque page rendue en RECETTE_MODE.

PROCEDURE RESPECTEE : test rouge d'abord — `tests/test_securite_bandeau_recette.py` (3 tests)
ecrit et execute AVANT la correction, confirmant la presence litterale du chemin/nom d'utilisateur
dans `app.main._recette_globals`. Correction ensuite : fonction `_nom_logique()` (nom de fichier
seul, jamais le chemin complet) appliquee a `RECETTE_ROOT`/`RECETTE_DB` ; `base.html` : libelle
"racine :" -> "environnement :" (coherent avec la nouvelle semantique masquee). Tests relances :
3/3 verts. Regression : 81 tests (sous-ensemble cible), puis campagne complete par shards
(132 fichiers) : 2284 passes / 75 ignores / 1 echec pre-existant — 3 tests de plus que la
reference du 2026-07-31, aucune regression.

Le second point de securite pre-existant (libelles bancaires avec fragments de compte tiers)
reste NOTE, NON CORRIGE — inherent au texte brut des releves bancaires reels, hors mandat de cette
mission, jamais reproduit dans la documentation Git.

STATUT : CORRIGE ET TESTE (commit `c65f891`). Detail complet : `HANDOFF_CANONIQUE.md`,
`JOURNAL_CONTROLES.md` (entree mission 2026-08-02 suite).


## BANQUE_NORMALISATION_TIERS_INCOHERENTE (2026-08-03) — DETECTION MOTEUR DIFFEREE, NON CORRIGEE

Constat pendant la validation humaine du groupe 5 Banque (19 credits CLASSE/risque eleve) : un
mouvement credit de 1300,00 EUR (2026-01-31) porte une identite textuelle candidate identique a
deux autres mouvements de la meme famille (credits 1000,00 EUR et 600,00 EUR, tous deux tagues
`VIR_ASSOCIE` par le moteur, regle R_020), mais ce mouvement precis est classe `VIR_INST_GENERIQUE`
(regle R_090) — le moteur ne l'a pas rattache au meme tiers associe.

Cause probable identifiee (sans PII) : un caractere anormal dans le libelle bancaire source (un
symbole non alphabetique insere au milieu du texte normalement attendu) casse le pattern-matching
de la regle de detection R_020/R_021. Le meme type d'incoherence de detection a ete observe a
plusieurs reprises pendant la validation humaine du groupe 4 (lignes portant une identite reelle
non detectee par le moteur malgre sa presence dans le libelle).

Aucune correction de regle appliquee — categorie moteur inchangee, aucune donnee source modifiee.
Categorie candidate humaine enregistree en overlay uniquement (hors Git) :
`APPORT_OU_REMBOURSEMENT_ASSOCIE_A_IDENTIFIER_ANOMALIE_DETECTION`. Arbitrage requis : affiner la
regle de detection de tiers (R_020/R_021) pour tolerer les caracteres anormaux de saisie bancaire,
ou accepter le residu comme limite connue du moteur de classification.

STATUT : DIFFEREE (categorie candidate seulement, aucune correction technique). Detail complet :
`73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md` (DEC-005).


## NOMS REELS EN DUR DANS LOT8C — CORRIGEE (2026-08-03)

Constat pendant l'audit des prerequis du rapprochement bancaire : `02_TRAVAIL/lot8c_rapprochement_
banque.py` contenait un dictionnaire `PROP_LABELS` codant en dur des prenoms et noms reels de
proprietaires (associes a des identifiants `PROP_00XX`), utilise uniquement pour enrichir un
commentaire genere dans l'onglet `RAPPROCH_PROPRIETAIRES_ATTENTE` de `BANQUE_LOT8_IMPORT.xlsx`.

Correction : dictionnaire supprime, le commentaire genere n'utilise plus que l'identifiant opaque
`PROP_00XX` deja disponible (aucune information necessaire perdue — le commentaire restait
exploitable avec le seul identifiant). Aucune donnee deplacee vers un autre fichier suivi. Verifie
par relecture directe de la sortie regeneree sur l'environnement de copies (aucune donnee reelle
modifiee) : plus aucun nom reel dans les 56 lignes de l'onglet concerne.

STATUT : CORRIGE. Detail complet : `74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md` (mission trésorerie
propriétaires du 2026-08-03).


## LOT8A CIBLE PAR ERREUR SUR LE WORKTREE REEL — CORRIGEE (2026-08-07)

Constat pendant l'execution du pipeline Lot8a->Lot13 sur copies (mission de validation finale
Banque/Tresorerie) : `lot8a_banque_import.py` a ete invoque avec `--project-root` en supposant ce
flag supporte par tous les scripts lot8, comme lot8c. Le script l'a silencieusement ignore (il ne
supporte que les variables d'environnement `LOT8A_BRUT_FILE_OVERRIDE`/`LOT8A_OUT_FILE_OVERRIDE`,
pas d'argparse) et a ecrit dans le worktree reel :
`02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` (fichier gitignore, jamais suivi par git).

Detection immediate via le chemin "Sortie" imprime par le script lui-meme. Impact verifie par
lecture directe du code : `01_SOURCES_BRUTES` n'est jamais ecrit par ce script (lecture seule).
Aucune sauvegarde automatique n'a ete creee dans `99_ARCHIVES` avec la date du jour, preuve que le
fichier n'existait pas avant cette ecriture (premiere creation, pas d'ecrasement d'un etat
anterieur a restaurer).

Correction : fichier cree supprime immediatement (`rm`), `git status --short` verifie vide.
Relance correcte avec les variables d'environnement adequates, pointant vers l'environnement de
copies. Lecon retenue et appliquee pour la suite du pipeline (lot8b/lot9/lot10/lot12/lot13 :
mecanisme de redirection de chaque script verifie par lecture du code AVANT tout lancement, technique
de copie temporaire du script utilisee la ou aucune redirection n'existe).


## FAUSSE LOGIQUE METIER — RAPPROCHEMENT VIREMENT PLATEFORME <-> RESERVATION — CORRIGEE (2026-08-08)

GRAVITE : MAJEURE (regle metier fausse construite dans le moteur applicatif, pas seulement documentee).
STATUT : CORRIGE.

Constat (signale par l'utilisateur) : `app/services/banques_candidats_service.py::_reservations()`
generait des candidats de type `RESERVATION` a partir de `MASTER_CALC_Reservations_Resolues.xlsx`
(Hostaway ET hors Hostaway) pour tout mouvement bancaire CREDIT, exposes ensuite au moteur generique
exact/partiel/groupe (`banques_rapprochement_service`, routes `_suggestions`/`_groupes`). Cette
fonction avait ete corrigee (pas creee) le 2026-08-03 (`74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md`)
en pensant reparer un bug de lecture de colonnes — mais la logique elle-meme (chercher une reservation
candidate a partir d'un montant bancaire recu) est fausse pour cette activite : un versement de
plateforme n'a pas de correspondance fiable avec une reservation individuelle (commissions/frais
agreges, versements groupes, plusieurs plateformes).

L'interface Banque (`banques_list.html`) portait la meme hypothese : bloc `SOURCE_AIRBNB_DETAILLEE_
ABSENTE` presentant les 166 virements Airbnb comme "bloques en attente d'export pour etre rapproches",
avec une liste de donnees minimales attendues incluant "reservation associee (lorsque disponible)".

Regle metier definitive enregistree : « Les virements entrants provenant des plateformes ne sont pas
rapproches des reservations individuelles. Ils sont categorises par origine lorsque celle-ci est
identifiable. Les reservations proviennent soit de l'API Hostaway, soit d'une saisie manuelle hors
Hostaway, independamment des virements recus. »

Correction : `_reservations()` supprimee entierement (fonction + cablage dans `candidats_pour()`/
`compter_sources()`) — aucun candidat `RESERVATION` n'est plus jamais genere. Exact/partiel/groupe
restent intacts pour charges (`_charges()`) et tresorerie proprietaires (`_reversements_
proprietaires()`), aucune regression. UI Airbnb remplacee par une categorisation neutre
(`categorisation_versements_airbnb()`, bloc `VERSEMENTS PLATEFORMES`) : 166 mouvements categorises
`PAYOUT_PLATEFORME` (categorie moteur deterministe deja existante, `tiers_detecte=AIRBNB`),
14467,27 EUR, aucun export requis. `74_CONTRAT_SOURCE_AIRBNB_RAPPROCHEMENT.md` marque SUPERCEDE.
lot8c_rapprochement_banque.py (script moteur) non modifie — il n'a jamais construit de lien
virement->reservation, seulement un statut d'attente desormais reinterprete cote application.

Effet secondaire trouve et corrige au meme tour (mission section 9) : `banques_classement_service.
lister()`/`compter()` ne dedoublonnaient pas par `mouvement_id`, exposant deux fois la meme ligne
(meme `id_opaque`) dans la file A_ENVOYER_IA pour le doublon physique deja connu
(`MVT-CM_02211_00021321603-20260115-DEBIT-12000-0FB68A`, ligne_source 149/151). Corrige : deduplication
par `mouvement_id`, ligne source non modifiee, 4 tests ajoutes.

Regression ciblee verte : `-k "banque or proprietaire"` 612 passes/29 ignores/0 echec ; `-k "banque or
classement"` 304 passes/29 ignores/0 echec ; `-k reservation` 44 passes/1 ignore/0 echec ; navigation/
pilotage/controles/catalogue 130 passes/5 ignores/0 echec. Aucune source reelle modifiee, port 8000/
PID 21136 intact, mode reel jamais active. Detail complet : `76_VALIDATION_FINALE_BANQUE_
TRESORERIE.md` (section G).

STATUT : CORRIGE, aucune donnee reelle perdue ni modifiee. Detail complet :
`76_VALIDATION_FINALE_BANQUE_TRESORERIE.md`.


## RECETTE LOT D/E (2026-08-10) — AUCUNE ANOMALIE APPLICATIVE TROUVEE

Recette fonctionnelle approfondie des lots D (Comptabilite, Analytique, Resultats) et E (Controles/
Cloture, Calculs, Exports) sur instance isolee : aucun defaut BLOQUANT, MAJEUR, MINEUR ni
COSMETIQUE nouveau. Aucune correction appliquee, aucun code modifie.

Deux constats a ne PAS confondre avec des anomalies :

1. ORCHESTRATEUR CALCULS - "Prerequis non satisfaits : scripts absents (lot9/lot10/lot12/lot13)"
   Artefact de l'arbre de COPIES uniquement : ces 4 scripts existent bien dans le worktree reel
   mais n'etaient pas presents dans l'arbre de copies (technique de securite des missions
   anterieures : copie temporaire du script puis suppression). Le comportement applicatif est
   CORRECT - l'orchestrateur verifie ses prerequis et refuse de lancer plutot que de produire un
   faux succes. Les 4 scripts ont ete copies dans l'arbre de copies pour exercer l'orchestrateur.
   STATUT : NON-ANOMALIE (artefact d'environnement de recette).

2. RECONCILIATIONS C et G en A_CONTROLER, E et F en NON_DISPONIBLE
   Attendu sur une base applicative vierge : aucune ecriture comptable n'a ete generee pour les
   donnees reelles dans cet environnement isole (seules des ecritures fictives ont ete creees).
   Les statuts sont honnetes et jamais masques. STATUT : NON-ANOMALIE (etat de donnees attendu).

DECOUVERTE STRUCTURANTE (pas une anomalie, une propriete de conception a documenter) :
tous les writers sont gates par RECETTE_MODE (X_REAL_WRITE_ENABLED = RECETTE_MODE and
_env_flag(...)), et trois sont codes en dur a False (HH_REAL_WRITE_ENABLED,
REF_ASSOC_MODE_REAL_WRITE_ENABLED, CONTROLES_REAL_WRITE_ENABLED). Une instance NON recette ne peut
donc activer AUCUN writer par variable d'environnement : l'activation du mode reel exige une
modification delibaree et revue de app/config.py. Protection par conception (impossible d'activer
le reel par accident), a connaitre avant toute planification de bascule.
Detail complet : PREPARATION_MODE_REEL.md.


## AMBIGUITE DOCUMENTAIRE "2358 BLOQUANTS" — CORRIGEE (2026-08-10)

GRAVITE : MOYENNE (erreur de restitution dans mes propres rapports, pas un defaut applicatif).
STATUT : CORRIGE.

Constat (souleve par l'utilisateur) : mes rapports des 2026-08-08/10 ecrivaient "62 INFO separes
des 2358 bloquants", formulation qui conflatait deux notions distinctes :
  - la SEVERITE du controle (BLOQUANT / A_CONTROLER / INFO) ;
  - l'EFFET du controle sur la cloture (impact_cloture).

Comptage exact realise sur l'export CSV applicatif : TOTAL 2420 lignes ; severite BLOQUANT 959,
A_CONTROLER 1399, INFO 62 ; 2357 lignes portent impact_cloture = "Bloque la cloture" ; 1 exception.

Le chiffre 2357/2358 etait donc EXACT au sens "empeche la cloture", mais sa lecture laissait
croire a 2358 controles de severite BLOQUANT alors qu'ils ne sont que 959. Les deux severites
BLOQUANT et A_CONTROLER bloquent la cloture.

Correction appliquee : tous les documents concernes (RECETTE_FONCTIONNELLE_GLOBALE.md,
PREPARATION_MODE_REEL.md, 72_CHECKLIST_GO_NO_GO_MODE_REEL.md, 60_VERDICT_GO_NO_GO.md,
HANDOFF_CANONIQUE.md, JOURNAL_CONTROLES.md) portent desormais les compteurs exacts et la
distinction severite / effet.

CONSEQUENCE DE FOND, plus importante que l'erreur elle-meme : 2357 lignes empechent REELLEMENT
toute cloture aujourd'hui. Ce sont des lacunes de donnees metier (9 familles), pas des defauts
applicatifs, mais elles rendent prematuree toute bascule en mode reel. Le verdict de preparation
du mode reel repasse en NO GO. Aucun controle n'a ete resolu automatiquement.


## INSTANCE DE RECETTE SANS PROJECT_ROOT EXPLICITE — AUTOCORRIGEE (2026-08-10)

GRAVITE : MINEURE. STATUT : CORRIGE, aucun impact.

Constat : apres application de la decision GESTION_LOGEMENT_MISSING au referentiel reel, une
instance applicative a ete demarree (port 8070) avec RECETTE_MODE=1 mais sans PROJECT_ROOT
explicitement positionne (variable non "unset" correctement dans l'environnement du shell
precedent), risquant une resolution par defaut sur l'arborescence reelle du worktree au lieu d'une
copie.

Detection immediate avant toute action de l'instance. Verification : CALCULS_REAL_RUN_ENABLED
n'a jamais ete positionne pour cette instance, donc aucun writer de calcul n'etait actif - meme si
l'instance avait servi une page en lecture sur donnees reelles, aucune ecriture n'etait possible.
Instance arretee immediatement (taskkill), avant toute requete HTTP traitee.

Impact reel : AUCUN. Confirme par le controle d'integrite final : 949/950 fichiers identiques,
seul REF_Setup.xlsm modifie (modification volontaire et documentee de cette meme mission).

Lecon retenue : toujours verifier explicitement PROJECT_ROOT (echo $PROJECT_ROOT) avant de
demarrer une instance de recette, particulierement apres une sequence de commandes multiples dans
la meme session shell.

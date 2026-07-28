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
STATUT : OUVERTE — arbitrage requis.

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

ISSUES POSSIBLES (a arbitrer, aucune engagee) :
A. Externaliser INTMAP et les donnees de lot6c vers REF_Intervenants / REF_Setup. Rend les moteurs
   pilotables par referentiel et la recette fictive possible. Modification de moteur.
B. Accepter que la chaine ne soit exercable que sur l'arbre reel, en copies et en lecture seule.
C. Enrichir le jeu de recette d'intervenants correspondant aux cles d'INTMAP sans reprendre les
   prenoms reels : IMPOSSIBLE en l'etat, INTMAP est indexe par prenom.

AUCUNE MODIFICATION DE MOTEUR N'A ETE FAITE. Detail : 41_MODULE_MENAGES_ETAT_FINAL.md section 7bis.

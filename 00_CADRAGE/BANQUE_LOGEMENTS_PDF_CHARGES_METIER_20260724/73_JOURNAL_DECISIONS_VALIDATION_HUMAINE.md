# 73 — Journal des décisions de validation humaine

Journal vivant, alimenté au fil de la session interactive de validation humaine. Chaque décision
n'existe que si l'utilisateur l'a rendue explicitement (APPROUVER / REFUSER / MODIFIER / REPORTER /
A_CONTROLER). Aucune décision n'est déduite du silence. `application_reelle` reste **NON** pour
toute la durée de cette mission — aucun flag de mode réel n'est activé.

## Format d'une entrée

```
### DEC-<NNN>

- date : AAAA-MM-JJ
- domaine : Banque | Rapprochements | Comptabilité | Fonctions différées | Sécurité | Autre
- objet_ou_groupe : <description du groupe/objet concerné>
- contexte : <chiffres factuels>
- proposition_claude : <recommandation faite>
- decision_utilisateur : APPROUVER | REFUSER | MODIFIER | REPORTER | A_CONTROLER
- justification_utilisateur : <verbatim ou résumé fidèle>
- portee : <ce que la décision couvre exactement>
- date_effet : <à partir de quand la décision s'applique>
- application_copies : OUI | NON
- application_reelle : NON (fixe pour cette mission)
- tests_associes : <tests créés/relancés>
- statut : ENREGISTRÉE | APPLIQUÉE SUR COPIES | EN ATTENTE D'APPLICATION
```

## Correction de comptage (avant toute décision)

Les 5 groupes de plus forte confiance présentés (tous `statut_classification = CLASSE`, tous
`statut_controle = A_CONTROLER`) totalisent **212** mouvements, pas 236. `236` est le total *tous
statuts confondus* de `CLASSE` (212 encore `A_CONTROLER` + 24 déjà `VALIDE`). Les mouvements
`A_CONTROLER` non couverts par les groupes 1-5 sont donc **305**, pas 281 :
222 `RAPPROCHEMENT_REQUIS` + 83 `A_ENVOYER_IA` = 305. Vérification : 212 + 305 = 517. Les valeurs
236 et 281 ne sont pas reprises ci-dessous.

## Décisions

### DEC-001 (corrigée)

- date : 2026-08-02 (correction du même jour — le sous-groupe "autres" ne peut pas rester approuvé
  en bloc puisqu'il contenait des achats personnels par carte)
- domaine : Banque — classification
- objet_ou_groupe : Groupe 1 (CLASSE / risque faible / DEBIT, 58 mouvements, 4390,21 €), sous-groupe
  "autres" (36, 491,07 €) scindé en 4
- contexte réel (vérifié sur les catégories moteur, aucune catégorie résiduelle non couverte) :
  - logiciel de gestion identifiable : `LOGICIEL_GESTION`, 5 mouvements, montants inclus dans les
    491,07 € du sous-groupe "autres"
  - autres abonnements professionnels identifiables : **0 mouvement** — aucune catégorie moteur
    distincte de ce type trouvée dans le groupe 1
  - achats personnels CB : `ACHAT_PERSO_CB`, 31 mouvements
  - prélèvements récurrents non identifiés : **0 mouvement** — 5 + 31 = 36, aucun résidu
- proposition_claude (initiale, corrigée par l'utilisateur) : approbation groupée des 36 lignes du
  sous-groupe "autres" — **rejetée par l'utilisateur, à raison**
- decision_utilisateur : MODIFIER (correction du sous-groupe "autres")
  - honoraires comptables (9, 2026,80 €) : classification métier **APPROUVÉE**, mapping comptable
    **A_CONTROLER**
  - cotisations sociales (7, 1549,00 €) : classification métier **APPROUVÉE**, mapping comptable
    **A_CONTROLER**
  - prévoyance (6, 323,34 €) : classification métier **APPROUVÉE**, mapping comptable
    **A_CONTROLER**
  - logiciel de gestion identifiable (5, catégorie `LOGICIEL_GESTION`) : classification métier
    **APPROUVÉE**, mapping comptable **A_CONTROLER**
  - autres abonnements professionnels identifiables (0 mouvement présent) : classification
    **APPROUVÉE uniquement si caractère professionnel démontré** — règle enregistrée, aucun
    mouvement à appliquer actuellement
  - achats personnels CB (31, catégorie `ACHAT_PERSO_CB`) : classification métier =
    **DEPENSE_PERSONNELLE_ASSOCIE** (catégorie équivalente la plus proche déjà existante côté
    moteur : aucune n'existe littéralement sous ce nom — décision enregistrée comme intention
    métier, **aucune catégorie moteur renommée**, aucun code modifié) ; **jamais** classée en
    charge professionnelle ; impact comptable via auxiliaire associé, après arbitrage ; **reste
    A_CONTROLER** tant que l'associé concerné et le traitement comptable ne sont pas validés
  - prélèvements récurrents non identifiés (0 mouvement présent) : maintenir A_CONTROLER — règle
    enregistrée pour un usage futur si un tel résidu apparaît
- justification_utilisateur : le sous-groupe "autres" mélangeait des catégories de nature
  différente (dépense professionnelle récurrente vs dépense strictement personnelle) ; les
  approuver ensemble aurait risqué de faire passer une dépense personnelle pour une charge
  professionnelle
- portee : 58 mouvements du groupe 1, classification uniquement, par 6 sous-groupes (dont 2 vides
  actuellement)
- date_effet : 2026-08-02
- application_copies : OUI pour honoraires comptables (9), cotisations sociales (7), prévoyance
  (6), logiciel de gestion (5) — classification uniquement, mapping comptable non touché. **NON**
  pour achats personnels CB (31) au-delà de l'étiquette `DEPENSE_PERSONNELLE_ASSOCIE` — reste
  A_CONTROLER tant que l'associé et le traitement comptable ne sont pas validés
- application_reelle : NON
- tests_associes : aucun (aucun code/règle moteur modifié — décision de labellisation humaine sur
  overlay de copie, pas une nouvelle règle déterministe, aucune catégorie moteur renommée)
- statut : APPLIQUÉE SUR COPIES (27/58 : honoraires + cotisations + prévoyance + logiciel gestion,
  classification uniquement) — EN ATTENTE (31/58 : achats personnels CB, classification
  informative seulement, comptable et associé non tranchés)

### DEC-002

- date : 2026-08-02
- domaine : Banque — classification
- objet_ou_groupe : Groupe 2 (CLASSE / risque faible / CREDIT, 4 mouvements, 41,87 €)
- contexte : 4 crédits `PAIEMENT CB` (Vinted x2, Uber Eats x2) ; recherche d'un débit d'origine de
  même montant identifiée sur les copies : 1 correspondance exacte (10,28 €, même date 2026-04-14,
  `ACHAT_PERSO_CB` débit vs crédit), 3 sans correspondance identifiable
- proposition_claude : approbation avec règle (jamais chiffre d'affaires, contrepasse si origine
  identifiable, sinon reste A_CONTROLER)
- decision_utilisateur : APPROUVER AVEC RÈGLE PRÉCISE
- justification_utilisateur : remboursement/annulation de dépense carte, jamais un produit
  commercial ; contrepasser la charge d'origine si identifiable, sinon rester A_CONTROLER
- portee : 4 mouvements du groupe 2
- date_effet : 2026-08-02
- application_copies : OUI — 1/4 mouvement marqué contrepassé sur copie (origine identifiée), 3/4
  restent A_CONTROLER (aucune origine fiable trouvée)
- application_reelle : NON
- tests_associes : aucun (rapprochement manuel montant/date sur l'export copie, pas un nouveau
  moteur de règle)
- statut : APPLIQUÉE SUR COPIES (1/4), EN ATTENTE (3/4)

### DEC-003

- date : 2026-08-02
- domaine : Banque — classification
- objet_ou_groupe : Groupe 3 (CLASSE / risque moyen / DEBIT, 76 mouvements, 6204,88 €)
- contexte : sous-groupes réels — DEPENSE_CB_A_CLASSIFIER (47, 3676,55 €), factures prestataires
  (9, 415,67 €), abonnement mobilité (6, 1299,92 €), santé (5, 272,74 €), loyer local (1, 60,00 €),
  virements sortants génériques (8, 480,00 €)
- proposition_claude : traitement différencié par sous-groupe, aucune approbation en masse
- decision_utilisateur : MODIFIER
- justification_utilisateur : DEPENSE_CB_A_CLASSIFIER maintenu A_CONTROLER en attendant des
  sous-règles par enseigne/motif (sans donnée personnelle) ; factures prestataires validables
  uniquement avec facture/fournisseur identifié ; abonnement mobilité validable après vérification
  du caractère professionnel ; santé jamais en masse (risque dépense personnelle) ; loyer local
  validable après rattachement contrat/justificatif ; virements sortants génériques en contrôle
  individuel
- portee : 76 mouvements du groupe 3, aucun sous-groupe validé en masse
- date_effet : 2026-08-02
- application_copies : NON — tous les sous-groupes nécessitent un justificatif individuel ou une
  sous-règle non encore construite ; aucun n'est éligible à une application sans justificatif
- application_reelle : NON
- tests_associes : aucun
- statut : EN ATTENTE (tous sous-groupes)

### DEC-004

- date : 2026-08-02
- domaine : Banque — classification
- objet_ou_groupe : Groupe 4 (CLASSE / risque élevé / DEBIT, 55 mouvements, 38585,85 €)
- contexte : virements instantanés génériques (34), virements associés (12), virements génériques
  (5), effets domiciliés (4)
- proposition_claude : contrôle individuel obligatoire, aucune validation groupée
- decision_utilisateur : REPORTER / CONTRÔLE INDIVIDUEL OBLIGATOIRE
- justification_utilisateur : montants élevés + motif générique = risque de faux positif métier
  réel (personnel/professionnel/associé) ; aucune confirmation automatique
- portee : 55 mouvements, traitement ligne par ligne à venir
- date_effet : 2026-08-02
- application_copies : NON
- application_reelle : NON
- tests_associes : aucun
- statut : EN ATTENTE — aucune règle modifiée

**Lignes 1 à 10 du groupe 4 — sous-décisions détaillées (aucune validation définitive, catégorie
candidate enregistrée pour faciliter le contrôle humain) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Justificatif requis | Confiance |
|---|---|---|---|---|---|
| 1 | `...C5BC18` (485,00 €) | FOURNISSEUR / DEPENSE_PROFESSIONNELLE | A_CONTROLER | Facture + identité prestataire | MOYENNE |
| 2 | `...973A2A` (400,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE | A_CONTROLER | Relevé IK, associé bénéficiaire, période/déplacements — **jamais** `AVANCE_ASSOCIE` tant que le sens n'est pas démontré | MOYENNE |
| 3 | `...A254FC` (469,00 €) | FOURNISSEUR / PAIEMENT_FACTURE_FRACTIONNE | A_CONTROLER | Facture portant la mention 1/2 et son éventuel second paiement | MOYENNE |
| 4 | `...3DDF74` (900,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE | A_CONTROLER | Idem ligne 2 ; vérifier campagne IK commune avec lignes 2 et 9 | MOYENNE |
| 5 | `...3F28C2` (200,00 €) | MOUVEMENT_TIERS_DOUBLE_QUALITE | A_CONTROLER | Rôle du mouvement (associé/propriétaire/homonyme/erreur de mapping) à établir avant tout traitement comptable | MOYENNE |
| 6 | `...D308BC` (562,85 €) | REMUNERATION_OU_PAIEMENT_PERSONNE_A_CONTROLER | A_CONTROLER | Contrat, bulletin, facture, remboursement, bénéficiaire — **jamais** classée automatiquement en salaire | FAIBLE |
| 7 | `...7921CE` (516,07 €) | MOUVEMENT_ASSOCIE_POTENTIEL | A_CONTROLER | Rôle réel et objet du paiement — le nom présent dans le libellé n'est pas une preuve suffisante, aucune règle automatique fondée dessus | FAIBLE |
| 8 | `...A914A0` (630,00 €) | FOURNISSEUR / DEPENSE_PROFESSIONNELLE | A_CONTROLER | Facture + fournisseur ; rapprochement possible avec les autres mouvements motif FACTURE | MOYENNE |
| 9 | `...EA9572` (200,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE | A_CONTROLER | Idem lignes 2/4, même campagne IK potentielle. Catégorie moteur réelle = `VIR_INST_GENERIQUE` (**pas** `VIR_ASSOCIE` — correction maintenue) | MOYENNE |
| 10 | `...B6888D` (590,00 €) | FOURNISSEUR_001 / DEPENSE_PROFESSIONNELLE | A_CONTROLER | 3 factures/justificatifs candidats (590 €, 165 €, 270 €) à rapprocher avant toute validation | MOYENNE À HAUTE (identité), BASSE (validité comptable) |

Aucune ligne validée définitivement. Toutes restent `A_CONTROLER`. Aucune écriture comptable, aucun
rapprochement, aucune confirmation, aucune modification moteur.

- application_copies : **OUI, uniquement pour l'overlay de contrôle** (catégorie candidate ajoutée
  en colonne `categorie_candidate` dans le fichier de recette, hors Git)
- application_dans_BANQUE_LOT8_IMPORT : **NON**
- application_dans_app.db : **NON**
- application_reelle : **NON**

**Lignes 11 à 20 du groupe 4 — sous-décisions détaillées (REPORTER AVEC CATÉGORIE CANDIDATE) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Confiance |
|---|---|---|---|---|
| 11 | `...C73548` (1072,14 €) | CONTRAT_RECURRENT_A_IDENTIFIER / EFFET_DOMICILIE | A_CONTROLER | Faible (nature), moyenne (existence du contrat) |
| 12 | `...AC9B00` (50,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE | A_CONTROLER | Moyenne |
| 13 | `...C660AC` (416,88 €) | REMUNERATION_OU_PAIEMENT_PERSONNE_A_IDENTIFIER | A_CONTROLER | Faible |
| 14 | `...B5E94D` (900,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE | A_CONTROLER | Moyenne |
| 15 | `...BEE17F` (331,50 €) | GESTE_COMMERCIAL / REMBOURSEMENT_CLIENT / DEDOMMAGEMENT_A_IDENTIFIER | A_CONTROLER | Faible |
| 16 | `...CA9195` (165,00 €) | FOURNISSEUR_001 / DEPENSE_PROFESSIONNELLE | A_CONTROLER (facture obligatoire) | Identité moyenne-haute, comptable non validée |
| 17 | `...9E07BD` (275,00 €) | FOURNISSEUR_INCONNU / PAIEMENT_FACTURE | A_CONTROLER | Faible |
| 18 | `...F20BEB` (1259,93 €) | CONTRAT_RECURRENT_A_IDENTIFIER / EFFET_DOMICILIE | A_CONTROLER | Faible (nature), moyenne (échéancier commun avec ligne 11) |
| 19 | `...0C2445` (299,64 €) | GESTE_COMMERCIAL / REMBOURSEMENT_CLIENT / DEDOMMAGEMENT_A_IDENTIFIER | A_CONTROLER | Faible |
| 20 | `...45558F` (250,00 €) | PEAGE / DEPLACEMENT_PROFESSIONNEL_POTENTIEL | A_CONTROLER (à défaut de preuve : envisager DEPENSE_PERSONNELLE_ASSOCIE) | Faible |

Toutes restent `A_CONTROLER`. Aucune écriture comptable, aucun rapprochement confirmé, aucun
mapping comptable validé, aucune règle moteur modifiée.

- application_copies : **OUI, uniquement dans l'overlay de contrôle** (colonne `categorie_candidate`)
- application_dans_BANQUE_LOT8_IMPORT : **NON**
- application_dans_app.db : **NON**
- application_reelle : **NON**

**Lignes 21 à 30 du groupe 4 — sous-décisions détaillées (REPORTER AVEC CATÉGORIE CANDIDATE) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Confiance |
|---|---|---|---|---|
| 21 | `...787D76` (164,57 €) | REMUNERATION_OU_PAIEMENT_PERSONNE_A_IDENTIFIER (rattaché analytiquement aux lignes 6 et 13, même série candidate) | A_CONTROLER | Faible |
| 22 | `...F62DFB` (1500,00 €) | CONTRAT_OU_PRESTATION_A_IDENTIFIER ("IL" non interprété comme IK/indemnité/loyer) | A_CONTROLER | Faible |
| 23 | `...2605E7` (370,00 €) | FOURNISSEUR_INCONNU / PAIEMENT_FACTURE | A_CONTROLER | Faible |
| 24 | `...D08174` (462,00 €) | FOURNISSEUR / PAIEMENT_FACTURE_FRACTIONNE (candidat textuel ligne 3, écart 7 € non expliqué) | A_CONTROLER | Moyenne |
| 25 | `...8DC75E` (270,00 €) | FOURNISSEUR_001 / DEPENSE_PROFESSIONNELLE (série complète avec 590 €/165 €) | A_CONTROLER (facture requise) | Identité moyenne-haute, comptable non validée |
| 26 | `...A37649` (1464,56 €) | CONTRAT_RECURRENT_A_IDENTIFIER / EFFET_DOMICILIE (série avec lignes 11/18) | A_CONTROLER | Moyenne (récurrence), faible (nature) |
| 27 | `...6D5255` (357,00 €) | GESTE_COMMERCIAL / REMBOURSEMENT_CLIENT / DEDOMMAGEMENT_A_IDENTIFIER (série avec lignes 15/19) | A_CONTROLER | Faible |
| 28 | `...401E78` (1185,50 €) | FOURNISSEUR_MENAGE_A_IDENTIFIER (non rattaché à la facture Ménages de mai 2026 déjà connue) | A_CONTROLER | Faible-moyenne (nature), faible (tiers/pièce) |
| 29 | `...798B4A` (650,00 €) | INDEMNITE_KILOMETRIQUE / REMBOURSEMENT_ASSOCIE (campagne candidate lignes 2/4/9/12/14) | A_CONTROLER | Moyenne |
| 30 | `...2CB810` (600,00 €) | REMBOURSEMENT_ASSOCIE (terme "actionnaire", pas une identité) | A_CONTROLER | Moyenne (famille métier) |

Toutes restent `A_CONTROLER`. Aucune écriture comptable, aucun rapprochement confirmé, aucun
mapping comptable validé, aucune règle moteur modifiée.

- application_copies : **OUI, uniquement dans l'overlay de contrôle** (colonne `categorie_candidate`)
- application_dans_BANQUE_LOT8_IMPORT : **NON**
- application_dans_app.db : **NON**
- application_reelle : **NON**

**Vérification de comptage (groupe 4)** : total groupe 4 = 55 ; lignes détaillées après ce tour =
30 ; restant = 25. Groupes 1-5 = 212, autres statuts (RAPPROCHEMENT_REQUIS 222 + A_ENVOYER_IA 83)
= 305. Total 212+305 = 517. Cohérent, aucune correction nécessaire.

**Lignes 31 à 40 du groupe 4 — sous-décisions détaillées (REPORTER AVEC CATÉGORIE CANDIDATE) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Confiance |
|---|---|---|---|---|
| 31 | `...F18C69` (120,00 €) | CONTRAT_OU_PRESTATION_A_IDENTIFIER (rattaché ligne 22, sigle "IL" non interprété) | A_CONTROLER | Faible |
| 32 | `...0A9AE5` (220,00 €) | FOURNISSEUR_002 / DEPENSE_PROFESSIONNELLE_POTENTIELLE | A_CONTROLER | Moyenne (identité), non validée (nature/comptable) |
| 33 | `...907C08` (1369,96 €) | CONTRAT_RECURRENT_A_IDENTIFIER / EFFET_DOMICILIE (série avec lignes 11/18/26) | A_CONTROLER | Moyenne (récurrence), faible (nature) |
| 34 | `...0748EC` (750,00 €) | PRESTATAIRE_MENAGE_A / PAIEMENT_PRESTATION_MENAGE_POTENTIEL | A_CONTROLER | Moyenne (tiers candidat), non validée (comptable) |
| 35 | `...A898C6` (205,00 €) | FOURNISSEUR_INCONNU / PAIEMENT_FACTURE (pattern lignes 17/23) | A_CONTROLER | Faible |
| 36 | `...317412` (600,00 €) | REMBOURSEMENT_ASSOCIE (famille lignes 30/38) | A_CONTROLER | Moyenne (famille métier) |
| 37 | `...C5212A` (3130,00 €) | FLUX_MIXTE_MENAGE_IK_A_DECOMPOSER | A_CONTROLER | Faible ; **risque de double comptage : ÉLEVÉ** |
| 38 | `...3140EA` (1800,00 €) | REMBOURSEMENT_ASSOCIE (famille lignes 30/36) | A_CONTROLER | Moyenne (famille métier) |
| 39 | `...6AC67A` (270,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée entre avance/remboursement |
| 40 | `...11DDFF` (300,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée entre avance/remboursement |

Toutes restent `A_CONTROLER`. Aucune écriture comptable, aucun rapprochement confirmé, aucun
mapping comptable validé, aucune règle moteur modifiée.

- application_copies : **OUI, uniquement dans l'overlay de contrôle** (colonne `categorie_candidate`)
- application_dans_BANQUE_LOT8_IMPORT : **NON**
- application_dans_app.db : **NON**
- application_reelle : **NON**

**Vérification de comptage (groupe 4)** : total = 55 ; lignes détaillées après ce tour = 40 ;
restant = 15. Groupes 1-5 = 212, autres statuts = 305. Total 517. Cohérent.

**Lignes 41 à 50 du groupe 4 — sous-décisions détaillées (REPORTER AVEC CATÉGORIE CANDIDATE) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Confiance |
|---|---|---|---|---|
| 41 | `...678C9D` (250,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Moyenne (tiers), faible (nature) |
| 42 | `...B1DBDC` (2879,00 €) | MOUVEMENT_ASSOCIE_A_MONTANT_ATYPIQUE | A_CONTROLER — contrôle individuel prioritaire | Faible (nature), risque financier ÉLEVÉ |
| 43 | `...E60B8C` (600,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |
| 44 | `...EBCA46` (970,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |
| 45 | `...743E86` (1129,00 €) | FLUX_MIXTE_LINGE_FRAIS_PLATEFORME_A_DECOMPOSER | A_CONTROLER | Faible ; **risque de double comptage : ÉLEVÉ** |
| 46 | `...E376D3` (315,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |
| 47 | `...87A060` (1444,00 €) | MOUVEMENT_ASSOCIE_A_MONTANT_ATYPIQUE | A_CONTROLER — contrôle individuel prioritaire | Faible |
| 48 | `...D7A176` (716,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |
| 49 | `...BF95CA` (900,00 €) | PRESTATAIRE_MENAGE_A / PAIEMENT_PRESTATION_MENAGE_POTENTIEL | A_CONTROLER | Moyenne-haute (tiers), non validée (comptable) |
| 50 | `...77B5D1` (400,00 €) | PRESTATAIRE_MENAGE_A / PAIEMENT_PRESTATION_MENAGE_POTENTIEL | A_CONTROLER | Moyenne-haute (tiers), non validée (comptable) |

Toutes restent `A_CONTROLER`. Aucune écriture comptable, aucun rapprochement confirmé, aucun
mapping comptable validé, aucune règle moteur modifiée.

- application_copies : **OUI, uniquement dans l'overlay de contrôle**
- application_dans_BANQUE_LOT8_IMPORT : **NON**
- application_dans_app.db : **NON**
- application_reelle : **NON**

**Synthèse agrégée série ASSOCIE_A (identifiant technique du tiers détecté par le moteur, masqué
`ASSOCIE_A` dans toute cette session ; toutes catégories, tous statuts) :**

| Indicateur | Valeur |
|---|---:|
| Nombre total de mouvements (toutes catégories) | 61 |
| Dont `VIR_ASSOCIE` (famille stricte) | 14 (12 dans le groupe 4 + 2 hors groupe 4/hors périmètre de cette session) |
| Total débit (`VIR_ASSOCIE`) | 8719,00 € |
| Total crédit (`VIR_ASSOCIE`) | 1600,00 € |
| Solde net (crédit − débit) | -7119,00 € (net versé à l'associé sur la période) |
| Nombre sans justificatif applicatif | 61 (aucun module de compte courant associé alimenté) |
| Nombre avec objet candidat (masqué ASSOCIE_A) | 61 |
| Montants atypiques (> 1000 €) | 2 : 2879,00 € (2026-05-10), 1444,00 € (2026-06-11) |

Aucune identité reproduite. Aucune conclusion sur le sens économique (avance/remboursement) prise à
la place de l'utilisateur.

**Vérification de comptage (groupe 4)** : total = 55 ; lignes détaillées après ce tour = 50 ;
restant = 5. Groupes 1-5 = 212, autres statuts = 305. Total 517. Cohérent.

**Lignes 51 à 55 du groupe 4 — finalisation (REPORTER AVEC CATÉGORIE CANDIDATE, aucune preuve
applicative fiable trouvée pour aucune des cinq lignes) :**

| # | ID opaque (suffixe) | Catégorie candidate | Statut | Confiance |
|---|---|---|---|---|
| 51 | `...5EBCFC` (160,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |
| 52 | `...8C4BA9` (815,00 €) | PRESTATAIRE_MENAGE_B / PAIEMENT_PRESTATION_MENAGE_POTENTIEL | A_CONTROLER | Moyenne (tiers référencé), non validée (comptable) |
| 53 | `...C8869F` (615,25 €) | TIERS_NON_REFERENCE / PAIEMENT_A_IDENTIFIER | A_CONTROLER | Faible (tiers non trouvé au référentiel) |
| 54 | `...4647CE` (1016,00 €) | PRESTATAIRE_MENAGE_A / PAIEMENT_PRESTATION_MENAGE_POTENTIEL | A_CONTROLER | Moyenne-haute (tiers), non validée (comptable) |
| 55 | `...3823E7` (615,00 €) | MOUVEMENT_ASSOCIE_A_A_QUALIFIER | A_CONTROLER | Non tranchée |

Groupe 4 **clos à 55/55 lignes analysées** — toutes restent `A_CONTROLER`, aucune validée
définitivement, aucune règle moteur modifiée, aucune application réelle.

## Correction — synthèse par statut du groupe 4 (2026-08-02, suite)

La synthèse publiée initialement (Candidat partiel 33926,71 € + Ambigu 4259,00 € + Pièce absente
4620,07 € = 42805,78 €) était **incohérente** avec le total réel du groupe (38585,85 €) — la
répartition avait été faite par famille métier (avec chevauchements de lecture) et non par statut
de recherche exclusif ligne par ligne. Corrigée par recalcul programmatique direct sur les 55
lignes sources (script Python, une seule exécution, un seul statut par ligne, aucune saisie
manuelle du total) :

| Statut | Nombre | Montant absolu |
|---|---:|---:|
| PREUVE_TROUVEE_A_VALIDER | 0 | 0,00 € |
| CANDIDAT_PARTIEL | 34 | 24 311,04 € |
| AMBIGU | 2 | 3 330,00 € |
| PIECE_ABSENTE | 19 | 10 944,81 € |
| **Total** | **55** | **38 585,85 €** |

| Contrôle | Attendu | Obtenu | Statut |
|---|---:|---:|---|
| Nombre de lignes | 55 | 55 | OK |
| Doublon de ligne | 0 | 0 | OK |
| Ligne oubliée | 0 | 0 | OK |
| Somme des 4 statuts | 38 585,85 € | 38 585,85 € | OK |
| Somme des familles (déjà publiée) | 38 585,85 € | 38 585,85 € | OK (inchangée, correcte) |
| Somme moteur (NORM_Banque, 55 lignes) | 38 585,85 € | 38 585,85 € | OK |

Les 2 lignes `AMBIGU` sont la ligne 5 (chevauchement de tag ASSOCIE_A/PROP_0008, nature non
tranchable) et la ligne 37 (flux mixte ménage/IK, décomposition non vérifiable). Les 19 lignes
`PIECE_ABSENTE` sont celles pour lesquelles aucun indice exploitable n'a été trouvé dans les
copies (aucune facture, relevé, référentiel ou pièce candidate, même partielle). Les 34 lignes
`CANDIDAT_PARTIEL` disposent d'au moins un indice (récurrence, identité référentielle, motif
textuel) mais aucune pièce applicative complète. **Aucune ligne `PREUVE_TROUVEE_A_VALIDER`** —
confirmé, cohérent avec le fait qu'aucune décision de ce tour n'a validé quoi que ce soit
définitivement.

## Audit et purge de confidentialité (2026-08-02, suite)

Le compte-rendu oral (chat) d'un tour antérieur avait fait apparaître une fois la valeur brute
d'un champ moteur (`tiers_detecte=PERS_WAFA`) au lieu du masquage `ASSOCIE_A` utilisé partout
ailleurs. Audit effectué :

- **Dans ce fichier et dans `HANDOFF_CANONIQUE.md` (version committée, `d61c277`)** : recherche
  du prénom et de tout autre prénom/nom réel — **absence confirmée** par recherche directe sur le
  contenu du commit (`git show d61c277:<fichier>`), aucune occurrence.
- **Dans l'historique Git complet de `73_JOURNAL_DECISIONS_VALIDATION_HUMAINE.md`** (tous
  commits) : aucune occurrence à aucun moment — le correctif appliqué avant le premier commit de
  ce fichier a fonctionné, rien à purger côté Git.
- **Dans `HANDOFF_CANONIQUE.md`, deux occurrences pré-existantes trouvées**, non liées à cette
  session, datant de missions antérieures à ce chantier de validation humaine (commit
  `5d90739`, mission Ménages, antérieure de plusieurs semaines) : un alias documenté (D104) et une
  référence à des prestataires réels dans une note sur le pivot de coût ménage. **Corrigées ce
  tour** (masquage générique, sens inchangé) car ce fichier fait partie du périmètre explicite de
  purge demandé.
- **Constat plus large, hors périmètre de correction immédiate** : un audit élargi à tout
  `00_CADRAGE/` a trouvé des occurrences de prénoms réels dans 8 fichiers historiques
  (`JOURNAL_ANOMALIES.md`, `JOURNAL_CONTROLES.md`, `41_MODULE_MENAGES_ETAT_FINAL.md`,
  `ARCHITECTURE_DONNEES.md`, `ETAT_AVANCEMENT.md`, `DECISIONS_METIER.md`, et deux documents dans
  `APPLICATION_LOCALE/`), **toutes pré-existantes**, introduites par des missions antérieures à ce
  chantier de validation humaine Banque (vérifié par `git log -S` sur chaque fichier — aucune ne
  provient d'un commit de cette série). **Non corrigées ce tour** : purger 8 documents historiques
  d'autres chantiers dépasse le périmètre de cette mission (validation Banque) et constituerait une
  nouvelle mission de fond (audit de confidentialité documentaire global), à traiter séparément si
  l'utilisateur le demande explicitement.
- **L'overlay hors Git** ne contient et n'a jamais contenu que des alias (`ASSOCIE_A`,
  `PRESTATAIRE_MENAGE_A`, `PRESTATAIRE_MENAGE_B`, `TIERS_NON_REFERENCE`, `FOURNISSEUR_00X`) —
  vérifié, aucune identité réelle n'y a été écrite à aucun moment de la session.

**Aucune anomalie à consigner dans `JOURNAL_ANOMALIES.md` pour le périmètre de cette session** — la
fuite ponctuelle du chat n'a jamais atteint Git (corrigée avant le premier commit du journal
humain). L'audit élargi (8 fichiers pré-existants) est noté ci-dessus pour information, pas comme
une anomalie de ce tour.

### DEC-005

- date : 2026-08-02
- domaine : Banque — classification
- objet_ou_groupe : Groupe 5 (CLASSE / risque élevé / CREDIT, 19 mouvements, 9678,52 €)
- contexte : virements instantanés génériques (15), virements associés (2), virement générique (1),
  1 ligne `IMPAYE` isolée
- proposition_claude : contrôle individuel obligatoire, isolement immédiat de l'impayé
- decision_utilisateur : REPORTER / CONTRÔLE INDIVIDUEL OBLIGATOIRE
- justification_utilisateur : virements entrants génériques restent A_CONTROLER jusqu'à
  identification de l'origine ; l'impayé doit être isolé et traité séparément (opération initiale,
  rejet/annulation/retour, impact règlement, impact créance/dette, nécessité de contrepassation)
- portee : 19 mouvements, la ligne IMPAYE isolée dans un sous-groupe distinct
- date_effet : 2026-08-02
- application_copies : NON
- application_reelle : NON
- tests_associes : aucun
- statut : EN ATTENTE — aucune règle modifiée, IMPAYE isolé pour traitement dédié

## Overlay d'application sur copies (fichier généré)

`_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/RAPPORTS/OVERLAY_DECISIONS_BANQUE_GROUPES_1A5.csv`
(212 lignes, une par mouvement des groupes 1 à 5, colonnes : mouvement_id, groupe, sous_groupe,
montant, date_operation, categorie_moteur, decision_classification, statut_copie, decision_id).
Fichier hors dépôt Git (dossier de recette), jamais une modification de `BANQUE_LOT8_IMPORT.xlsx`
ni de l'`app.db` réel ou de copie. Aucun code ni règle moteur modifié — ces décisions sont des
étiquettes de validation humaine superposées aux données, pas une nouvelle règle déterministe.

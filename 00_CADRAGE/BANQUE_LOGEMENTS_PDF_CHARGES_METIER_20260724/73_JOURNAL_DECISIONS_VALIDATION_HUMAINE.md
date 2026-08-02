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

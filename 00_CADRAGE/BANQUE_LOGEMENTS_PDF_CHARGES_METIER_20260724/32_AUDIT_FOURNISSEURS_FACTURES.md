# 32 — Audit ciblé : Fournisseurs, Factures et Règlements

Audit ciblé (≤45 min), pas un audit général. Objectif : savoir ce qui existe déjà pour **ne rien
dupliquer**, et identifier ce qui manque réellement.

## Tableau de cadrage

| Objet | Source de vérité actuelle | Manque | Cible |
|---|---|---|---|
| **Fournisseur** | SQLite `fournisseurs` + `fournisseur_evenements` (migration 0010), service `fournisseurs_referentiel_service.py` : lister/charger/doublons/créer/modifier/désactiver/réactiver/historique. Route `/referentiel-fournisseurs`. | Champs métier étendus (raison sociale, contact, email, téléphone, adresse, TVA, conditions de paiement, prestataire ménage) ; **interdiction d'utiliser un fournisseur archivé** pour une nouvelle facture ; solde fournisseur. | Compléter la table existante (colonnes additionnelles) ; **jamais** un second référentiel. |
| **Facture** | **N'existe pas.** `/fournisseurs` gère des CHARGES, pas des factures (confirmé par le commentaire de la migration 0010 elle-même). `facture_ref` n'existe que comme colonne libre côté charges. `MASTER_FACT_MEN_MenagesExternes` (Lot6c) contient des factures de ménage, mais c'est une sortie moteur, pas un objet applicatif. | Tout : modèle, statuts, import/saisie, justificatif, solde. | Nouvelle table `factures` + `facture_evenements` (SQLite, journal applicatif — la vérité économique reste la charge). |
| **Charge** | `SAISIE_Charges_Flux.xlsx` (vérité) → Lot3 → `MASTER_FACT_MAN_Charges.xlsx`. Parcours applicatif complet : `charges_preview_service`, `charges_confirmation_service`, `charges_validation_service` (A_CONTROLER→VALIDE), contrôles d'intégrité. | Lien **facture → charge** (une charge par facture, jamais deux). | Réutiliser le parcours Charges existant **sans le contourner** ; ajouter uniquement le lien `facture_id`/`facture_ref`. |
| **Règlement** | Partiellement : `proprietaires_paiement_service.py` + migration 0012 (règlements **propriétaires**, pas fournisseurs). `charges_affectations_service.py` + migration 0011. | Règlement **fournisseur** : total/partiel/multiple/groupé, moyens (banque, caisse, personnel associé), avoir, remboursement. | Nouvelle table `reglements_fournisseurs` + répartition par facture (paiement groupé). |
| **Rapprochement** | **Existe et est générique** : `banques_rapprochement_service.py` + migration 0015 (multi-objets, partiel, multiple, contrôles de dépassement/double). Types déjà prévus : `CHARGE_FOURNISSEUR`, `REGLEMENT_CHARGE`. Également migration 0014 `rapprochements_reglements` (règlement propriétaire ↔ mouvement, déclaratif). | Rien de structurel — juste le branchement depuis une facture/un règlement. | **Réutiliser `banques_rapprochement_service`**, jamais un second moteur (règle explicite de la consigne §13). |
| **Solde fournisseur** | N'existe pas. | Total facturé / réglé / solde / échues / à venir / litige / ancienneté. | Vue calculée à la demande depuis `factures` + `reglements_fournisseurs` (jamais une colonne dénormalisée qui dériverait). |

## Extraction PDF

- Extracteur **existant et réutilisable** : `02_TRAVAIL/lib_menages_externes_pdf.py`
  (`extraire_pdf()`, `detecter_format()`, `FactureExtraite`, `LigneFacture`, `empreinte_facture()`,
  `row_hash()`).
- **Texte natif uniquement (PyMuPDF/fitz), jamais d'OCR** — décision déjà prise et documentée en
  tête du fichier. `fitz` est **disponible dans cet environnement** (PyMuPDF 1.27.2), contrairement
  à `pandas`.
- Limite réelle : l'extracteur ne connaît que **deux formats de fournisseur de ménage**
  (`_extraire_aissata`, `_extraire_mounir`) et retombe sinon sur un format inconnu. Il est donc
  réutilisable tel quel pour ces deux cas, et il faut un **chemin de saisie manuelle** pour tout le
  reste — c'est la cible retenue, pas un nouvel extracteur générique.

## Principe structurant (rappel de la consigne, retenu tel quel)

Quatre objets **distincts**, jamais confondus :

| Objet | Rôle |
|---|---|
| Facture | dette et pièce fournisseur |
| Charge | impact économique / analytique |
| Règlement | paiement effectué |
| Rapprochement | lien règlement ↔ mouvement bancaire |

**Une charge n'est jamais créée lors d'un rapprochement bancaire.** Le rapprochement ne fait que
relier un règlement existant à un mouvement existant.

## Décisions d'architecture pour la suite

1. **Étendre** `fournisseurs` (migration additive), ne pas créer de second référentiel.
2. **Nouvelles tables** : `factures`, `facture_evenements`, `reglements_fournisseurs`,
   `reglement_repartitions` (pour le paiement groupé).
3. **Réutiliser** `banques_rapprochement_service` pour le lien bancaire — types `CHARGE_FOURNISSEUR`
   et `REGLEMENT_CHARGE` déjà prévus dans son énumération.
4. **Flags dédiés** `FACTURES_REAL_WRITE_ENABLED` / `_CONFIRMATION_ENABLED`, même double verrou
   `RECETTE_MODE` que Charges/Banque/Logements.
5. Le lien facture → charge **passe par le parcours Charges existant**, jamais en écrivant
   directement dans `SAISIE_Charges_Flux.xlsx` depuis le module Factures.

## Statut de l'audit

Terminé. Prochaine étape : modèle de données (migration) + service Factures, puis Règlements, puis
branchement Banque, puis contrôles/interface/recette.

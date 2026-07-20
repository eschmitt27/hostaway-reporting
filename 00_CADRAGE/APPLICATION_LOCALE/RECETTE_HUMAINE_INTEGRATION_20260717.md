# Recette humaine — intégration application locale — 2026-07-17

Validation visuelle menée sur l'instance de démonstration (worktree d'intégration, données sur copie
isolée, réel intact). **La validation humaine n'est PAS acquise.** Anomalies et fonctions manquantes
consignées ci-dessous, à traiter avant toute validation ou commit.

## Anomalies constatées

1. **500 Ménages** — `/menages/recalculer/confirmer` renvoie *Internal Server Error*.
   Cause : `_construire_workspace` copie les scripts moteur (lot6d/lot6e) sans vérifier leur présence ;
   sur un PROJECT_ROOT qui ne les contient pas → `FileNotFoundError` non capturée → 500.
2. **Écran chaîne faux** — `/menages/chaine` affiche encore *« Factures externes — extraction manuelle.
   lot6c ne lit pas le contenu des PDF »*. FAUX depuis l'extraction PDF réelle (Aissata/Mounir).
3. **Banque non actionnable** — les mouvements sont lisibles mais aucun ne peut être catégorisé,
   rattaché (propriétaire/logement/réservation/facture) ni contrôlé. Module en lecture seule.
4. **Filtre logement Propriétaires** — affiche `LOG_XXXX` au lieu des noms officiels de logement.
5. **Périodes futures** — le filtre période propose des mois futurs (jusqu'à 2027-02).
6. **Propriétaires non gérables** — aucun propriétaire ne peut être créé ni modifié.
7. **Fin de contrat impossible** — aucune sortie de gestion propriétaire ne peut être déclarée.
8. **Factures propriétaires figées** — aucune facture ne peut être modifiée, générée en PDF, ni
   changer de statut.
9. **Contrôles non actionnables** — les contrôles sont lisibles mais sans workflow de traitement
   (prise en charge, résolution, justification).
10. **Clôture absente** — aucun mois ne peut être clôturé ni rouvert.

## Classement par lot proposé (voir Phase 12)
- **HOTFIX-1** : #1 (500 Ménages) + #2 (texte chaîne) + #4 (filtre logement) + #5 (périodes futures).
- **APP-4B** : #3 (contrôle & catégorisation bancaire sur copies).
- **APP-3D** : #6 + #7 (gestion propriétaires & contrats sur copies).
- **APP-3E** : #8 (factures propriétaires : statuts, PDF, sur copies).
- **APP-5B** : #9 (contrôles actionnables sur copies).
- **APP-5C** : #10 (clôture mensuelle sur copies).

## Garde-fous (inchangés)
Aucun commit. Aucun fichier réel modifié. Aucune requête Hostaway. Aucun MASTER réel régénéré.
Tous les flags d'écriture restent False (existants + nouveaux à créer, jamais activés).

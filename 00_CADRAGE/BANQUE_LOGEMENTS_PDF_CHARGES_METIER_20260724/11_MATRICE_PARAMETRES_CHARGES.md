# 11 — Matrice des paramètres de charge

Valeurs **réellement présentes dans le projet** (aucune inventée). Références : R1-R13 (doc 10).

| Paramètre | Valeurs possibles | Effet attendu |
|---|---|---|
| Catégorie | 15 (CHG_003..027) + 2 hors-form (CHG_016 forfait client, CHG_023 forfait cave) | Détermine famille ménage (FORCE/CHOIX/INTERDIT) et avantage possible |
| Code impact | **IC** / **HC** (form standard) ; HR (parcours dédié) | IC = réel+comptable ; HC = réel, hors compta ; HR = hors résultat |
| prise_en_compta | OUI (si IC) / NON (si HC) | Dérivée du code impact, jamais saisie |
| Sens du flux | DEPENSE / RECUPERATION / REMBOURSEMENT / REFACTURATION / NEUTRE | DEPENSE par défaut ; pilote remboursement/récupération |
| Type d'affectation | LOGEMENT / PROPRIETAIRE / GLOBAL / NON_AFFECTABLE | Périmètre analytique |
| Supporteur | Conciergerie (défaut) / Propriétaire (si refacturable) | Qui porte la charge |
| Refacturable | Oui / Non | Oui → réserve de facturation → préfacture propriétaire |
| Propriétaire(s) | PROP_xxxx (0..n) | Élargit aux logements actifs du propriétaire |
| Logement(s) | LOG_xxxx (0..n) | Périmètre restreint + répartition |
| Répartition | égale (repartir_egal) | Somme quotes-parts = montant, 0 centime perdu |
| Mode de paiement | PAY_001 BANQUE_PRO / PAY_002 ESPECES_CAISSE / PAY_003 CARTE_ASSOCIEE / PAY_004 COMPTE_PERSO_ASSOCIEE | Qui a payé ; lien banque si BANQUE_PRO |
| Liée à un ménage | Forcé (cat FORCE) / Choix Oui-Non (cat CHOIX) / Interdit | Alimente coût ménage + gain/perte, jamais refacturable |
| Ménage interne/externe | mode INTERVENANT / LOGEMENT + source coût | Traitements distincts |
| Avantage associé | Oui (+associe_id) / Non | Flèche la charge comme avantage d'un associé (cat CHOIX) |
| Hors comptabilité | via code impact HC | Exclu du résultat comptable, gardé en réel/analytique |
| ASSOC_MODE | BANQUE / GLOBAL / … (REF_Assoc_Mode) | Trace dans charge_id CHG-AAAA-MM-IMPACT-ASSOC-NNN |
| Période / mois | mois ouvert (non clôturé) | V-garde : refus si mois clôturé |
| Statut | A_CONTROLER (auto) | Injecté, jamais saisi |
| Niveau anomalie | INFO (auto) | Injecté |

## Champs qui déterminent chaque impact (rappel synthèse doc 10)
- **Résultat conciergerie ↓** : IC/HC réel + supporteur conciergerie + non refacturable.
- **Net propriétaire ↓** : refacturable=Oui (ou affectation propriétaire).
- **Comptabilité** : IC seulement.
- **Préfacture** : refacturable=Oui (réserve).
- **Coût ménages** : catégorie FORCE ou CHOIX+ménage=Oui.
- **Aucun impact** : HR / catégorie neutralisée / informative.

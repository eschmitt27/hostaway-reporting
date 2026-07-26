# 20 — Verdict sur les règles métier des charges

Preuves : confirmation réelle en recette (6 scénarios) + lecture des chiffres écrits + test
automatisé `test_recette_scenarios.py`.

| Règle métier | Code présent | Test auto | Résultat chiffré | Verdict |
|---|---|---|---|---|
| Une charge = une seule charge économique (somme quotes-parts = montant) | oui | oui | H/I : 50+50 = 100 | **Conservée et correcte** |
| IC = résultat réel + comptable | oui | oui | A : prise_en_compta OUI | **Conservée** |
| HC = réel, HORS comptabilité | oui | oui | D : prise_en_compta NON | **Conservée** |
| HR = hors résultat (parcours dédié) | oui | — | non exposé au formulaire standard (V06) | **Conservée** (hors Nouvelle charge, par conception) |
| Refacturable → réserve de facturation → préfacture | oui | oui | B : préfacture PROP_A = 100 € | **Conservée** |
| Charge conciergerie ≠ charge propriétaire | oui | oui | A (pas de préfacture) vs B (préfacture 100) | **Conservée** |
| Répartition égale sans perte de centime | oui | oui | H : 50/50 | **Conservée** |
| Répartition multi-propriétaires distincte | oui | oui | I : PROP_A 50 / PROP_B 50 | **Conservée** |
| Paiement compte perso → ASSOC_MODE distinct, pas de remboursement indu | oui | oui | C : charge_id …FICTIF…, 0 préfacture | **Conservée** |
| Logement inactif exclu | oui | — | LOG_INACTIF absent du formulaire | **Conservée** |
| Mois clôturé refusé | oui | — | seul 2026-06 ouvert | **Conservée** |
| Impact net propriétaire / résultat conciergerie (valeur dashboard) | oui (drapeaux) | — | déterminé par drapeaux ; recalcul dashboard bloqué (réservations absentes) | **Conservée au niveau charge ; mesure dashboard = bloc suivant** |

## Conclusion
Les règles métier différenciantes des charges sont **conservées et correctes** : deux charges de
100 € avec des paramètres différents produisent des effets **distincts et chiffrés** (comptabilité
IC/HC, préfacture refacturable, ventilation multi-logements/propriétaires, ASSOC_MODE). Aucune règle
perdue détectée. Seule la matérialisation du net propriétaire dans le tableau de bord reste à
brancher (pipeline réservations, hors vertical charges).

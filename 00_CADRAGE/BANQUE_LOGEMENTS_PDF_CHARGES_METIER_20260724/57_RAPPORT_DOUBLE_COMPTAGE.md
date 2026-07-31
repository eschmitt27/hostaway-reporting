# 57 — Rapport double comptage (recette globale, copies de données réelles)

Contrôles explicitement recherchés sur les copies (mois 2026-06 et cumul 24 mois réels) :

| Contrôle | Méthode | Résultat |
|---|---|---|
| REEL + COMPTABLE + HORS_COMPTA additionnés | vérifié que l'écran Résultats et l'export CSV présentent 3 lignes distinctes, jamais une somme | Aucune addition trouvée — `HORS_COMPTA` est *inclus* dans REEL (REEL=COMPTABLE+HC), jamais un 4e total qui les cumulerait tous les trois |
| Même réservation/ménage compté deux fois | Lot9→Lot10 (réconciliation A) : 11 856,22 = 11 856,22, aucun doublon `ROW_HASH` détecté par `lot9_vs_lot10` | 0 doublon |
| Facture et charge comptées comme deux coûts distincts | sans objet : 0 facture réelle enregistrée | sans objet ce tour |
| Règlement compté comme charge | sans objet : 0 règlement réel enregistré | sans objet ce tour |
| Mouvement bancaire compté comme charge | réconciliation D : Banque=0,00 / journal BANQUE=0,00, aucun rapprochement réel | 0 |
| Même écriture comptable générée deux fois | 0 écriture réelle dans l'`app.db` copié — pipeline `charges` (lot3) relancé deux fois de suite sans passer par la Comptabilité | sans objet ce tour |
| Ventilation multi-logements supérieure à la source | sans objet : 0 facture multi-lignes réelle | sans objet ce tour |
| Même ligne présente dans plusieurs visions | vérifié sur `/resultats` : REEL/COMPTABLE/HORS_COMPTA affichés comme 3 lignes séparées avec des totaux différents, jamais la même ligne dupliquée entre visions | 0 |
| Avoir non déduit | sans objet : 0 avoir réel | sans objet ce tour |
| Contrepassation comptée avec l'écriture initiale | sans objet : 0 écriture réelle | sans objet ce tour |
| **Idempotence pipeline (lot3 charges)** | relancé deux fois sur les copies : `/resultats/categories` identique (`NON_DISPONIBLE` les deux fois, 0 charge réelle saisie), page HTML octet-identique après le 2e run | 0 doublon, écart 0,00 |
| **Idempotence migrations** | migrations rejouées deux fois sur la copie de l'`app.db` réel : comptage de lignes identique table par table (24 migrations, 42 `audit_events`, etc. inchangés) | 0 doublon |

## Constat

Aucun double comptage détecté sur les axes réellement exercés (Analytique/Résultats/Lot9-10,
migrations, pipeline charges). Les contrôles portant sur Factures/Règlements/Ménages/Comptabilité
sont **sans objet** ce tour car ces modules n'ont jamais été alimentés en réel (0 ligne) — pas une
absence de vérification, une absence de matière à vérifier, honnêtement documentée plutôt que
simulée.

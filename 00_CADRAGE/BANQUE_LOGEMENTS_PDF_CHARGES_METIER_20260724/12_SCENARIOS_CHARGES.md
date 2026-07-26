# 12 — Scénarios de charges (conception)

Conception des scénarios A→J, chacun mappé aux **paramètres réels** (doc 11) et à l'**effet attendu**
calculé par `build_effet_saisie` (doc 10). Montant = 100 € (fictif) pour tous, afin de prouver que
les paramètres — pas le montant — font la différence. Exécution chiffrée = phase build (non faite).

| Scénario | Paramètres réels | Effet attendu (moteur) |
|---|---|---|
| **A — Conciergerie** | cat CHG_008 (maintenance), impact **IC**, logement LOG_A1, non refacturable, payé BANQUE_PRO | Résultat conciergerie −100 ; net propriétaire inchangé ; préfacture aucune ; charge logement +100 (analytique) ; comptable Oui |
| **B — Propriétaire (refacturable)** | cat CHG_008, IC, propriétaire A + logement LOG_A1, **refacturable=Oui** | Réserve de facturation 100 € → préfacture ligne 100 ; net propriétaire −100 ; résultat conciergerie neutre (créance) ; charge logement +100 |
| **C — Payée directement par propriétaire** | sens_flux NEUTRE / affectation logement, supporteur propriétaire, payé COMPTE_PERSO | Trésorerie conciergerie 0 ; somme à payer 0 ; résultat conciergerie 0 ; résultat économique logement −100 selon règle |
| **D — Hors comptabilité** | impact **HC** (prise_en_compta=NON) | Résultat comptable inchangé ; résultat réel/analytique −100 ; identifiable HORS_COMPTA |
| **E — Ménage externe** | cat CHG_003/004 (FORCE), fournisseur externe, lié logement + réservation | Coût ménages +100 ; alimente gain/perte + rapprochement ménage ; jamais refacturable ; jamais 2ᵉ charge réelle |
| **F — Ménage interne** | cat FORCE, mode INTERVENANT, répartition interne | Traitement distinct de E (source coût interne) ; coût ménages +100 ; analytique |
| **G — Répartition multi-logements** | logements LOG_A1 60 % / LOG_A2 40 % | A1 +60, A2 +40, somme 100, 0 centime perdu ; impact propriétaire A unique |
| **G2 — Répartition multi-propriétaires** | LOG_A1 (prop A) + LOG_B1 (prop B) | Ventilation par propriétaire distincte, sommes correctes |
| **H — Informative sans impact** | impact HR / catégorie neutralisée | Visible, aucun indicateur modifié |
| **I — Remboursement / récupérable** | charge initiale + sens REMBOURSEMENT | Solde final net ; pas de double comptage ; bonne période |
| **J — Charge issue d'un PDF** | mêmes params qu'une charge manuelle équivalente | Résultat identique à la charge manuelle (règles communes) |

## Données fictives de référence nécessaires (doc 03, phase build)
- Propriétaires A/B/C ; logements LOG_A1, LOG_A2 (prop A), LOG_B1 (prop B), LOG_C1 (prop C), 1 inactif.
- Taux commission 15 % et 19 % (période historique distincte pour vérifier la reprise du taux).
- Réservations à montants ronds → CA/commission/net connus AVANT charges.

## Preuve visée
Pour chaque scénario : tableau AVANT/APRÈS (doc 13) constaté **dans l'application après recalcul**,
pas seulement en test unitaire. L'effet de saisie (prévisualisation) est déjà exerçable en direct
(prouvé mission précédente : charge 100 € → affectation LOG_0001, impact réel+comptable Oui).

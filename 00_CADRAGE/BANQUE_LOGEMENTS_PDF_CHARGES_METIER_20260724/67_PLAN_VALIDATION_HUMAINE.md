# 67 — Plan de validation humaine

Matrice de scénarios représentatifs, sur les copies (`_RECETTES_GLOBALES/RECETTE_GLOBALE_
20260801_004232`), avec preuve déjà obtenue lors des missions de recette globale. Identifiants
opaques uniquement — aucune PII. La colonne **Décision humaine** est à remplir par l'utilisateur,
jamais par Claude.

| Module | Scénario | Données utilisées | Résultat attendu | Preuve | Décision humaine | Statut |
|---|---|---|---|---|---|---|
| Logements | cas normal | `LOG_A1`…`LOG_C1` (parc réel, 19 logements) | fiche complète, historique | `/logements/{id}` — vérifié missions antérieures | — | à valider |
| Propriétaires | cas normal | `PROP_0001`…`PROP_0012` | fiche + net Lot12 séparé du résultat conciergerie | `/resultats/proprietaires/{id}` — vérifié, non-confusion prouvée | — | à valider |
| Réservations | cas A_CONTROLER | réservations `DIRECT_SANS_SAISIE_HH`, ex. `LOG_0015`/`PROP_0011` sur 3 mois | statut `A_CONTROLER`, jamais fabriqué à 0 | `62_RAPPORT_MOIS_LOT10_MANQUANTS.md` | — | à valider |
| Réservations | cas historique (clôturé) | mois clôturés, source `HIST_Reservations_Cloturees` | HIST prime sur le live | `64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md` §preuve | — | à valider |
| Ménages | cas normal | `MEN-xxxx` liés à `menages.fournisseur_id_opaque` | coût prévu/réel, prestataire ≠ fournisseur | `/resultats/prestataires/{id}` | — | à valider |
| Charges | cas sans donnée | `SAISIE_Charges_Flux` réelle, onglet `SAISIE` = 0 ligne | `NON_DISPONIBLE`, jamais un tableau à zéro | `/resultats/categories` → `NON_DISPONIBLE` constaté | — | à valider |
| Fournisseurs | cas normal | référentiel fournisseurs existant | fiche + solde auxiliaire | `/resultats/fournisseurs/{id}` | — | à valider |
| Factures | cas incomplet | 0 facture réelle enregistrée | réconciliation E = `NON_DISPONIBLE`, jamais 0 fabriqué | `56_RAPPORT_RECONCILIATIONS_GLOBALES.md` | — | à valider |
| Règlements | cas sans donnée | 0 règlement réel | cohérent avec Factures ci-dessus | idem | — | à valider |
| Banque | cas normal (VALIDE) | 24 mouvements `VALIDE` (Lot8b) | classification affichée, compte masqué | `/banques-caisse` — vérifié, `CM ••••1603` | — | à valider |
| Banque | cas A_CONTROLER (majorité) | 517 mouvements | synthèse par motif, cf. `68` | `/banques-caisse?statut=A_CONTROLER` | **arbitrage requis** | à trancher |
| Banque | cas doublon probable | 1 doublon détecté (Lot8a) | signalé, jamais masqué, montant non soustrait deux fois | `65_RAPPORT_CYCLE_BANQUE_COMPLET.md` | — | à valider |
| Banque | cas multi-mois | fichier consolidé couvrant 03/11/2025→01/08/2026 | `BANQUE_FICHIER_PERIODE_INCOHERENTE`, `A_CONTROLER` non bloquant | `63_CONTRAT_FORMAT_RELEVE_BANCAIRE_CONSOLIDE.md` | — | à valider |
| Calculs | cas normal | pipeline aval complet, mois 2026-06 | 6/6 lots SUCCÈS, idempotent | `64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md` | — | à valider |
| Calculs | cas historique multi-mois | 24 mois (2025-01→2027-02) | reconstruction globale, jamais partielle | idem | — | à valider |
| Comptabilité | cas sans donnée | 0 écriture réelle | `A_CONTROLER`/`NON_DISPONIBLE`, jamais un faux total | `56_RAPPORT_RECONCILIATIONS_GLOBALES.md` (C, G) | **arbitrage requis** (plan de comptes) | à trancher |
| Analytique | cas normal (drill-down) | logement → écriture → facture | aucune 404 sur objet existant | mission Bloc 5 antérieure, `/resultats/logements/{id}` | — | à valider |
| Résultats | cas avec drill-down | prestataire → ménage → facture | chaîne complète sans 404 | test dédié `test_resultats_drilldown.py` | — | à valider |
| Résultats | cas annulé | rôle prestataire vs fournisseur, même tiers | jamais additionnés, lien croisé affiché | `53_AXES_ANALYTIQUES_ETAT_FINAL.md` | — | à valider |
| Contrôles | cas normal | 1536 contrôles générés (Lot11) | 959 BLOQUANTS + 567 A_CONTROLER, Lot12 correctement `BLOQUE` | `65_RAPPORT_CYCLE_BANQUE_COMPLET.md` | **arbitrage requis** (volume élevé, à trier) | à trancher |
| Clôture | cas normal | période comptable | refuse toute écriture directe si clôturée | `49_COEUR_COMPTABILITE_ETAT_FINAL.md` | — | à valider |
| Power BI | cas normal | 13 exports + dictionnaire | 0 PII (email/téléphone/IBAN/libellés bancaires/noms voyageurs exclus) | `65_RAPPORT_CYCLE_BANQUE_COMPLET.md` | — | à valider |

## Fonctions différées — statut et nécessité avant mode réel

| Fonction | Besoin réel | Contournement actuel | Risque | Nécessaire avant mode réel ? |
|---|---|---|---|---|
| Factures propriétaires émises | objet applicatif distinct, jamais construit | Lot12 (préfactures) reste la source ; Comptabilité VENTES lit Lot12 en adaptateur | faible — le flux actuel produit déjà des préfactures cohérentes | **Non** — décision explicite : le circuit actuel (Lot12 + adaptateur VENTES) couvre l'usage initial |
| Factures voyageurs/tiers | jamais construites | hors périmètre du chantier actuel | nul pour l'usage conciergerie interne | **Non**, sauf besoin explicite futur |
| Avoirs comme objet autonome | Lot8a génère `AVOIR` en contrepassation d'une écriture ACHATS existante, pas un objet à cycle propre | contrepassation déjà fonctionnelle et testée | faible — couvre le cas d'usage réel (annulation de facture fournisseur) | **Non** |
| Lot8b/8c classification fine | règles seed génériques, 517/541 mouvements encore `A_CONTROLER` | classification manuelle via `/banques-caisse/mouvements/{id}` (déjà fonctionnelle) | **moyen** — beaucoup de contrôle manuel au démarrage réel | **Recommandé mais pas bloquant** — la validation manuelle mouvement par mouvement est déjà opérationnelle |
| Plan de comptes détaillé | mécanisme relié, aucune règle `VALIDE` arbitrée | filet `PROVISOIRE_GENERIQUE` (`606000`) partout | **moyen** — comptabilité provisoire tant que non arbitré | **Recommandé avant activation du module Comptabilité en réel** |

Aucune de ces fonctions n'a été construite dans cette mission — décision de les laisser en l'état,
conformément au mandat (« ne pas construire sauf si l'absence empêche réellement l'usage initial »).

## Guide utilisateur court

Voir `69_GUIDE_RECETTE_UTILISATEUR.md` pour un parcours pas-à-pas reproductible.

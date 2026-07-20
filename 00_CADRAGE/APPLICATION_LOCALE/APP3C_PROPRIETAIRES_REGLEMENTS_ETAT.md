# APP-3C — Propriétaires & règlements : lecture & pilotage (lecture seule)

**Statut : `APP-3C_PROPRIETAIRES_REGLEMENTS_LECTURE_EN_ATTENTE_VALIDATION`**

Écran consolidé de pilotage propriétaire, alimenté par les sorties moteur (Lot10 / Lot12). Aucune
écriture, aucun virement, aucune génération de facture, aucun déclenchement Lot12, **aucun calcul de
commission dans FastAPI**. Développé dans un worktree détaché (HEAD `8b47807`). **Aucun commit.**

Le module `/proprietaires` existant (fiche par propriétaire) reste inchangé ; APP-3C ajoute l'écran
consolidé `/proprietaires-reglements` (grain **mois × propriétaire**).

## 1. Sources (carte source → producteur → onglet → grain → clé → statut → consommateur)

| source | producteur | onglet | grain | clé | statut | consommateur |
|---|---|---|---|---|---|---|
| MASTER_CALC_NetProprietaire.xlsx | Lot10 | VUE_MOIS | mois × propriétaire | mois+proprietaire_id | (net calculé) | APP-3C dashboard |
| MASTER_CALC_NetProprietaire.xlsx | Lot10 | REGLEMENT | mois × logement × prop | mois+logement+prop | statut_reglement | APP-3C (acomptes, reste) |
| MASTER_CALC_Commissions.xlsx | Lot10 | COMMISSIONS | réservation | reservation_calc_id | statut_calcul_payout | APP-3C (taux, assiette) |
| MASTER_CALC_Resultats.xlsx | Lot10 | PAR_MOIS_PROPRIETAIRE | mois × propriétaire × vision | — | — | (résultat) |
| MASTER_FACT_Proprietaires.xlsx | Lot12 | FACT_FACTURE_ENTETE | mois × logement × prop | facture_id | statut_facture | APP-3C (facture) |
| MASTER_FACT_Proprietaires.xlsx | Lot12 | DASHBOARD_FACTURATION | mois × propriétaire | — | statut_facture, balises | APP-3C (anomalies) |
| MASTER_FACT_Proprietaires.xlsx | Lot12 | A_CONTROLER | contrôle | — | severite | APP-3C (à contrôler) |

Volumétrie réelle : VUE_MOIS 221, REGLEMENT 270, COMMISSIONS 1349, FACT_ENTETE 270, DASHBOARD 221.

## 2. Écrans livrés

- **`/proprietaires-reglements`** — période, fraîcheur, 6 cartes (propriétaires, logements, commission
  totale, net total, reste à régler, factures/à-contrôler), filtres (période, propriétaire, logement,
  facturé/non, réglé/non, avec anomalie, avec reste), tableau paginé (propriétaire, logements, CA retenu,
  ménage, base commission, commission, autres impacts, net, facture, règlement, reste, statut, Voir),
  export CSV.
- **`/proprietaires-reglements/{proprietaire_id}`** — résultat de période (CA/ménage/base/commission/
  net distincts), logements & règlements (acomptes ≠ paiement ≠ reste), **taux historiques** (moteur),
  factures, anomalies, traçabilité. 404 propre si inconnu.
- **`/proprietaires-reglements/a-controler`** — propriétaires signalés (bloquants, à contrôler, reste,
  balises) + contrôles facturation. Bouton **« Préparer un règlement » désactivé** (prochain lot).

## 3. Règles respectées

- **commission jamais recalculée** : `commission = total_commission_mois` (moteur) ; le taux affiché
  vient de `COMMISSIONS.taux_commission` (15 % jusqu'au 31/01/2026, puis taux propre au
  propriétaire/logement) — jamais recomposé ;
- **CA retenu ≠ commission ≠ net** ; préfacture ≠ facture ≠ paiement ≠ résultat ; acompte ≠ règlement
  final — champs distincts ;
- **aucun statut de paiement inventé** (statut_reglement / statut_facture repris du moteur) ;
- **adresse propriétaire et données bancaires jamais affichées** ; aucun chemin absolu ;
- SQLite non utilisé (module strictement lecture) ; **aucun MASTER écrit, aucun Lot12 déclenché**.

## 4. Limites & données manquantes

- Pas de règlement / virement (bouton désactivé — lot ultérieur).
- Pas de génération de facture réelle.
- « Base commission » = somme de `assiette_commission` (COMMISSIONS) — indicative ; la commission
  autoritaire reste `total_commission_mois`.
- « Reste » = `reste_a_payer_conciergerie` (moteur) ; aucun statut de paiement inventé.
- PAR_MOIS_PROPRIETAIRE (Resultats) lu mais non exploité en profondeur (double vision produits/charges).

## 5. Prochaine étape

Préparation de règlement (rapprochement acompte ↔ virement, marquage réglé), génération de facture —
lots ultérieurs, avec gardes d'écriture dédiées. Non livrés ici.

## 6. Tests

`05_APPLICATION/tests/test_proprietaires_reglements.py` — ~30 tests sur fixtures isolées : lecture &
états (alimentée/absente/onglet absent/vide), propriétaire conforme, plusieurs logements, taux
historique (15 % puis propre), **commission reprise sans recalcul**, base commission, net, acompte
distinct du paiement, règlement complet/partiel, reste & à-contrôler, facture absente, filtres
(période/propriétaire/reste/non-facturé), pagination, détail/404, export CSV, **adresse jamais
exposée**, aucun chemin absolu, aucune source réelle modifiée, aucun import moteur ni écriture
openpyxl ni calcul de commission dans `app/`, routes (dashboard, détail 200/404, à-contrôler, export),
aucune route de paiement, nav sidebar.

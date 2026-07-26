# Audit ciblé — forfait logiciel/consommables et taux de commission

Fait avant toute écriture, conformément à la consigne. Lecture seule.

## 3.1 — Forfait logiciel et consommables

| Élément | Mécanisme actuel |
|---|---|
| Paramètre source | `REF_Logements.forfait_logiciel_consommables_mensuel` — **une seule valeur courante, non historisée** |
| Feuille / colonne | `REF_Setup.xlsm` → `REF_Logements` |
| Producteur mensuel | `lot10_calculer_resultats.py::build_charge_fixe()` (« Option A ») — **interne à Lot10**, aucun fichier/service séparé |
| Fichier de sortie | Aucun fichier dédié : DataFrame en mémoire, fusionné dans `MASTER_CALC_NetProprietaire.xlsx` |
| Consommateur Lot9 | **Aucun** — le forfait ne passe jamais par Lot9 ni par `SAISIE_Charges` |
| Consommateur Lot10 | `build_net_proprietaire()` : `net_proprietaire_apres_charge_mois = net_avant_charge − charge_fixe_mensuelle` |
| Impact résultat conciergerie | Aucun (le forfait ne touche pas `MASTER_CALC_Resultats` / `GLOBAL`) |
| Impact propriétaire | **Direct sur le net**, jamais via préfacture/charge refacturable |
| Impact préfacture | Aucun — circuit **totalement séparé** du mécanisme `charges_exceptionnelles_refacturees` construit pour le scénario B |
| Protection contre doublon | Une ligne par (mois × logement) générée par boucle sur `_mois_range(first_mois, last_mois)` — pas de clé stable persistée, recalculée à chaque run |
| Règle d'archivage | Le forfait n'est généré que pour les mois où `TYPE_FLUX_017` (réservation) existe ; un logement sans réservation dans le mois n'a pas de ligne |
| Règle de prorata | **Mois entier**, pas de prorata : la boucle génère une ligne à taux plein pour chaque mois entre première et dernière réservation |

### Risque identifié — pas de double comptage, mais pas d'historisation

Le mécanisme actuel et celui construit pour les charges (Lot3/SAISIE/préfacture, scénario B) sont
**deux circuits disjoints** : le forfait n'entre jamais dans `SAISIE_Charges`, donc **aucun risque
de double comptage entre les deux** dans l'état actuel.

En revanche, `REF_Logements.forfait_logiciel_consommables_mensuel` est une **valeur unique sans
date de début/fin**. `build_charge_fixe()` relit cette valeur courante pour **chaque mois passé**
(via `log_first`→`log_last`). Conséquence : **modifier le forfait aujourd'hui change
rétroactivement le calcul de tous les mois déjà produits**, dès que Lot10 est relancé — à l'exact
opposé de la règle demandée (« ne jamais modifier les mois passés »).

### Réconciliation attendue pour 35 €

| Effet | Montant |
|---|--:|
| Ligne mensuelle générée (charge_fixe_mensuelle) | 35 |
| Charge conciergerie | 0 (aucune) |
| Créance propriétaire | 0 (pas de préfacture) |
| Préfacture | 0 (circuit non emprunté) |
| Variation somme à payer (`reste_a_payer_conciergerie`) | 0 |
| Variation net propriétaire | **−35** (déduction directe) |
| Effet économique final conciergerie | 0 |
| Effet économique final propriétaire | **−35** |

### Recommandation d'architecture

**Ne pas créer un second mécanisme.** Compléter l'existant :
1. Historiser `forfait_logiciel_consommables_mensuel` dans une feuille dédiée (ex.
   `REF_Forfait_Logiciel_Hist`, même patron que `REF_Taux_Commission` : `logement_id`, `montant`,
   `date_debut`, `date_fin`, `actif`).
2. Modifier `build_charge_fixe()` (Lot10) pour résoudre le montant **par mois**, via une fonction de
   résolution historisée (même famille que `resolve_management_period`/`resolve_commission_rate`
   déjà utilisées dans ce fichier), au lieu de lire la valeur courante unique.
3. C'est une **modification du moteur Lot10** (fichier `.py` hors `app/`), pas seulement du service
   applicatif — à traiter avec le même soin que le correctif `proprietaire_id` (test rouge → fix →
   preuve chiffrée AVANT/APRÈS sur un mois déjà « passé »).

**Non fait ce tour** : la modification de `build_charge_fixe()` elle-même (risque de régression sur
un calcul déjà validé — 14 060 € de résultat global — non pris à la légère avec le budget restant).

## 3.2 — Taux de commission : contradiction structurelle

Les données réelles (`REF_Setup.xlsm` du dépôt réel, vues lors d'un audit antérieur de cette
mission, non reproduites ici pour éviter toute PII) montrent des lignes `REF_Taux_Commission` avec
**`logement_id = None`** : le taux y est aujourd'hui rattaché au **propriétaire**, pas au logement.
Une ligne observée avait `date_fin = 2026-01-31`, ce qui est **cohérent** avec la règle annoncée
(« 15 % jusqu'au 31/01/2026 »).

| Point | Constat |
|---|---|
| Règle annoncée (§4.2) | Taux attaché au **logement** (`logement_id` obligatoire) |
| Données réelles observées | Taux attaché au **propriétaire** (`logement_id = None`) |
| Contradiction | **Oui — structurelle**, pas seulement une date : c'est la clé de résolution qui change de grain |
| Jeu de recette fictif construit cette mission | `logement_id` renseigné (LOG_A1, LOG_A2…) — conforme à la **nouvelle** règle, car construit pour la démonstration, pas depuis les données réelles |

### Arbitrage nécessaire — ne pas trancher seul

`resolve_commission_rate()` (Lot10, `lib_ref_history.py`) doit être vérifiée : accepte-t-elle déjà
une résolution par logement, ou suppose-t-elle `logement_id=None` (résolution par propriétaire
uniquement) ? **Non vérifié ce tour.** Si la fonction ne gère que le grain propriétaire, migrer les
données réelles vers un grain logement est un changement de modèle qui **doit être validé avant
toute écriture sur les données réelles** — hors périmètre recette de toute façon (jamais de données
réelles touchées ici).

## Ce qui peut être implémenté immédiatement, sans arbitrage

- `changer_taux_commission()` côté service applicatif, **au grain logement**, dans le jeu de recette
  fictif (qui utilise déjà ce grain) — sans toucher Lot10 ni aux données réelles.
- `modifier()`, `archiver()`, `reactiver()`, `changer_proprietaire()` — aucune dépendance au forfait
  ni à l'arbitrage taux.
- Le forfait **historisé côté saisie** (nouvelle feuille + service applicatif) peut être construit ;
  seule sa **lecture** par `build_charge_fixe()` (Lot10) reste à corriger séparément pour que
  l'historisation ait un effet réel sur le calcul.

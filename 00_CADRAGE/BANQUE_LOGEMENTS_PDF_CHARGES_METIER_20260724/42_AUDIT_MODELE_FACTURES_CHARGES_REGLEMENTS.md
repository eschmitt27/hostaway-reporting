# 42 — Audit transverse : modèle Factures / Charges / Règlements

Mission 2. Principe vérifié : facture ≠ charge ≠ dette/créance ≠ règlement ≠ mouvement bancaire ≠
écriture comptable.

## 1. Cartographie des trois circuits du brief

### A. Fournisseur — circuit SQLite complet, déjà séparé et testé

```
Facture reçue (table factures, migration 0017)
  → dette (solde recalculé, jamais stocké — factures_service.solde())
  → charge (charge_id, lien vers Excel, jamais créée par ce module)
  → règlement (reglements_fournisseurs + reglement_repartitions)
  → banque (factures_banque_service, pont vers banques_rapprochement_service)
  → écriture comptable : INEXISTANT (objet du tout premier périmètre de Mission 3)
```

| Objet | Source de vérité | Grain | Table | Producteur | Consommateur |
|---|---|---|---|---|---|
| Facture | SQLite | 1 facture fournisseur | `factures` | `factures_service.creer` | `factures_controles_service`, `reglements_fournisseurs_service` |
| Charge | Excel (`SAISIE_Charges_Flux`) | 1 charge économique | classeur, pas SQLite | parcours Charges | Lot9, `factures_service` (lien seul) |
| Règlement | SQLite | 1 paiement | `reglements_fournisseurs` | `reglements_fournisseurs_service` | `factures_banque_service` |
| Rapprochement | SQLite | 1 lien règlement↔mouvement | `banque_rapprochements` (0015) | `banques_rapprochement_service` | `factures_banque_service` (lecture) |

Ce circuit est **déjà correctement séparé**. Confirmé par les tours précédents : `test_verite_du_lien_reste_dans_banque_rapprochements`, index unique `idx_factures_charge` (une charge = une facture), `idx_repartitions_reglement`/`facture` (un règlement peut couvrir plusieurs factures, une facture peut recevoir plusieurs règlements).

### B. Propriétaire — circuit ENTIÈREMENT SÉPARÉ, jamais dans SQLite

```
Commission et frais → FACT_FACTURE_ENTETE / FACT_FACTURE_LIGNES (Excel, lot12_generer_factures.py)
  → montant_du_conciergerie / reste_a_payer_conciergerie (calculés dans le classeur)
  → règlement/reversement : suivi via /proprietaires-reglements (lecture de l'Excel + statuts SQLite
    de suivi humain, migration antérieure)
  → banque : lecture seule
  → écriture comptable : INEXISTANT
```

**Aucune table SQLite ne modélise une facture propriétaire.** L'écran `/proprietaires-reglements`
lit directement les onglets Excel produits par lot12. C'est un circuit de **lecture et de suivi**,
pas un objet applicatif au même sens que les factures fournisseurs.

### C. Voyageur / tiers — NON CONSTRUIT

Aucune table, aucun service, aucun écran. Le brief le note lui-même : « si applicables ». Aucune
donnée de ce type n'a été rencontrée dans le jeu de recette ni dans les décisions métier
consultées.

## 2. Incohérences recherchées, résultat

| Recherche demandée | Constat |
|---|---|
| Charges créées par plusieurs circuits | Non trouvé — le parcours Charges reste le seul producteur, `charge_id` unique par facture |
| Facture créée manuellement et automatiquement (même pièce, deux fois) | Non trouvé — `idx_factures_fournisseur_ref` interdit le doublon certain ; `doublons_probables()` détecte le doublon probable |
| Règlement dupliqué | Non trouvé — chaque règlement a un `reglement_id_opaque` unique, la répartition est une table séparée |
| Statut dupliqué | Non trouvé — un seul champ `statut` par facture/règlement, recalculé nulle part ailleurs |
| Rapprochement dupliqué | Non trouvé — `banques_rapprochement_service` reste la seule source, le pont ne fait que déléguer |
| Paiement stocké comme caractéristique d'une charge | Non trouvé — la charge Excel ne porte aucune colonne de règlement |
| **Facture fournisseur confondue avec facture propriétaire** | **CONFIRMÉ — pas une confusion de code, une ABSENCE de modèle commun.** Voir §3. |
| Mouvement bancaire pris pour un revenu ou une charge | Non trouvé — Lot9 les classe explicitement par `type_flux_id`/`sens`, jamais un statut de charge |
| Net propriétaire calculé plusieurs fois | Non trouvé — un seul calcul, dans lot10 (`MASTER_CALC_NetProprietaire`), lot12 le lit sans le recalculer |

**Une seule incohérence réelle trouvée** : l'absence de modèle commun entre factures fournisseurs
(SQLite) et factures propriétaires (Excel). Ce n'est pas un bug — c'est une dette de modélisation,
qui n'empêchait rien tant qu'aucune comptabilité applicative n'existait. Elle devient bloquante pour
Mission 3 : un journal ACHATS ne peut référencer que des factures qui existent quelque part sous une
forme interrogeable.

## 3. Décision de modélisation

**Ne pas fusionner les deux circuits.** Le circuit propriétaire reste piloté par lot12 (règle
métier lourde : commissions, forfaits, refacturations, compensations) — le recréer en SQLite serait
dupliquer un moteur existant, exactement ce que ce chantier interdit partout ailleurs.

**Étendre la table `factures` avec deux colonnes de classification**, pour qu'elle puisse à terme
porter plusieurs types sans ambiguïté :

- `sens` : `RECUE` | `EMISE` — toutes les factures actuelles sont `RECUE` (fournisseur) ;
- `type_facture` : `FOURNISSEUR` | `PROPRIETAIRE` | `VOYAGEUR` | `AVOIR_RECU` | `AVOIR_EMIS` —
  toutes les factures actuelles sont `FOURNISSEUR`.

Migration additive (`0020`), colonnes `NOT NULL DEFAULT` pour ne rien casser côté existant.
**Aucune facture propriétaire n'est créée en SQLite par ce tour** : les deux colonnes préparent le
terrain (et permettent au premier socle Comptabilité de Mission 3 de savoir de quoi il parle), sans
prétendre migrer lot12.

## 4. Contrôles du brief §14 — déjà couverts, vérifiés un par un

| Contrôle demandé | Code existant | Statut |
|---|---|---|
| Facture en doublon | `F_DOUBLON_CERTAIN` (index unique) + `F_DOUBLON_PROBABLE` | ✅ déjà couvert |
| Facture sans tiers | `F_FOURNISSEUR_ABSENT` | ✅ |
| Facture fournisseur sans dette | solde toujours recalculé depuis les règlements ; une facture `VALIDEE` a par construction un solde | ✅ structurel |
| Facture propriétaire sans créance | ⛔ hors périmètre SQLite — la créance vit dans `reste_a_payer_conciergerie` (Excel) |
| Facture validée sans lignes | ⚠️ absent — `factures` n'a pas de table de lignes (une facture fournisseur est un montant global, pas structurée en lignes) |
| Charge liée deux fois | `idx_factures_charge` (index unique) | ✅ structurel |
| Facture liée deux fois à la même charge | même index, sens inverse | ✅ structurel |
| Règlement supérieur au solde | contrôle applicatif dans `reglements_fournisseurs_service` | ✅ |
| Statut REGLEE avec solde non nul | `F_REGLEE_SOLDE_NON_NUL` | ✅ (bug réel trouvé et corrigé au tour Factures) |
| Banque rapprochée sans règlement | `R_BANQUE_SANS_MOUVEMENT` | ✅ |
| Règlement sans facture (si obligatoire) | `R_FACTURE_INEXISTANTE` | ✅ |
| Paiement compté comme charge | structurellement impossible — `reglements_fournisseurs` n'a pas de `code_impact`, jamais lu par Lot9 | ✅ structurel |
| Facture et charge comptées deux fois | même index unique `idx_factures_charge` | ✅ structurel |

**Conclusion de l'audit** : le modèle Factures/Règlements est déjà solide. Le vrai travail de
Mission 2 est l'extension `sens`/`type_facture`, pas une correction de bug.

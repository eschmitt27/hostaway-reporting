# 42 — Audit ciblé : cycle de vie opérationnel Ménages

Suite de `40` et `41`. Ce document répond au §2 du brief : inventaire précis avant construction,
pour ne rien recréer de ce qui existe déjà.

## 1. Inventaire

| Fonction | Complète | Partielle | Absente | Fichier |
|---|:--:|:--:|:--:|---|
| Comptage attendu/réalisé (Hostaway vs déclaré) | ✅ | | | `menages_service.load_reconciliation_rows`, `load_dashboard` |
| Rapprochement chiffré (coût standard/direct/complet) | ✅ | | | `lot6d`→`lot6f`, `load_reconciliation_detail` |
| Recalcul sur copies (chaîne lot6 complète) | ✅ | | | `menages_chaine_service.py` |
| Justification humaine d'un écart (`outrepasser`) | ✅ | | | route `/menages/{mois}/{logement_id}/{intervenant_id}/outrepasser`, table `menage_overrides` |
| **Objet ménage unitaire** (un ménage = une ligne, un id) | | | ⛔ | rien — le grain est `(mois, logement_id, intervenant_id)`, jamais l'occurrence |
| Statuts opérationnels | | | ⛔ | aucune table ne porte de statut de cycle de vie |
| Création hors Hostaway | | | ⛔ | — |
| Affectation / changement de prestataire | | | ⛔ | — |
| Annulation / remplacement / litige | | | ⛔ | — |
| Qualification prestataire (interne/externe, tarif, logements autorisés) | | ⚠️ | | `fournisseur_details.prestataire_menage` **existe mais n'est lu ni écrit nulle part** — colonne morte |
| Lien ménage → facture | | | ⛔ | — |
| Lien ménage → charge | | | ⛔ | — |
| Lien ménage → règlement | | | ⛔ | — |
| Lien ménage → mouvement bancaire | | | ⛔ | — |
| Catalogue de contrôles applicatif (type `/factures/controles`) | | | ⛔ | — |
| Filtres mois/logement/prestataire/type | ✅ | | | `menages_service.load_filter_options` |
| Filtre par statut | | | ⛔ | pas de statut à filtrer |

## 2. Tables SQLite existantes touchant Ménages

| Table | Grain | Rôle réel |
|---|---|---|
| `menage_overrides` (migration `0003`) | `(mois, logement_id, intervenant_id)` | journal de justification humaine d'un écart de comptage — **pas** un objet ménage |
| `fournisseurs` (`0010`) | fournisseur | `type` inclut déjà `MENAGE` |
| `fournisseur_details` (`0017`) | fournisseur | `prestataire_menage INTEGER` déclaré, **jamais utilisé** dans le code |
| `menages_recalcul_runs` (`0005`) | run de recalcul | traçabilité des recalculs sur copies, pas des ménages eux-mêmes |

**Aucune table ne porte un ménage comme entité.** Le module actuel compare des comptages ; il ne
gère pas d'occurrences.

## 3. Référentiel Fournisseurs — extension à faire, pas de table concurrente

`reglements_fournisseurs`, `factures`, `facture_evenements` suivent déjà le patron
(table métier + table d'événements append-only, `id_opaque` public, `version` optimiste,
`date_modification`, jamais de suppression physique). Le modèle du ménage unitaire doit reprendre
ce patron à l'identique.

`fournisseur_details.prestataire_menage` est insuffisant : il ne distingue pas interne/externe, ne
porte ni tarif historique ni logements autorisés. Une table `fournisseur_menage_qualification`
séparée s'impose (même raison que `0017` : pas d'`ALTER TABLE` sûr, les migrations sont rejouées à
chaque démarrage).

## 4. Pont vers Factures — patron `factures_banque_service.py`

Le module Factures/Banque a déjà résolu exactement le problème que Ménages doit résoudre : relier
un objet métier à des factures/règlements **sans dupliquer les contrôles**. `factures_banque_service`
ne fait que déléguer et lire (`rapprocher()` appelle `banques_rapprochement_service.enregistrer`,
jamais de logique dupliquée). Le service Ménages devra faire de même : lire les factures/charges/
règlements/mouvements existants, jamais les recréer.

## 5. Conclusion d'audit

Le cycle de vie manque en totalité. Ce n'est pas une lacune ponctuelle : c'est une couche entière
absente, comparable en volume à ce qu'a demandé le module Factures. Construction nécessaire, dans
l'ordre déjà arrêté au tour précédent :

1. modèle SQLite du ménage unitaire + statuts ;
2. qualification prestataires (extension du référentiel Fournisseurs) ;
3. services ;
4. écrans et rattachements ;
5. catalogue de contrôles ;
6. jeu de recette, recette navigateur, pipeline.

# 27 — Validation des charges et contrôles d'intégrité

## Pourquoi une charge doit être validée
Une charge saisie est créée en `A_CONTROLER`. **Lot9 n'ingère que les charges `VALIDE`** : tant
qu'elle n'est pas validée, la charge n'a **aucun impact financier** (ni résultat, ni net
propriétaire, ni préfacture). Message affiché : « Cette charge n'aura aucun impact financier tant
qu'elle ne sera pas validée. »

## Parcours implémenté — `app/services/charges_validation_service.py`
- `lister(statut, refacturable, sans_proprietaire, code_impact)` : charges de la SAISIE + compteurs
  (total, A_CONTROLER, VALIDE, REJETE, refacturables, sans propriétaire) et filtres.
- `valider(charge_id, acteur, commentaire)` : `A_CONTROLER → VALIDE`, journalisé, renvoie
  `recalcul_requis=True`.
- `rejeter(charge_id, commentaire)` : `A_CONTROLER → REJETE`, **commentaire obligatoire**, ligne
  **conservée** (jamais supprimée), jamais ingérée par Lot9.
- `historique(charge_id)` : journal SQLite (`charges_validation_journal` : charge, ancien/nouveau
  statut, acteur, commentaire, horodatage UTC).

### Sécurité
Écriture sur la **SAISIE** (vérité) par remplacement atomique passant par le **write-guard**
(refus hors `data_recette` en mode recette) ; flags `CHARGES_REAL_WRITE_*` requis ; en cas de refus,
le fichier temporaire est supprimé et **le statut reste inchangé** (vérifié par test).

## Preuve end-to-end (parcours applicatif, plus aucune modification manuelle du MASTER)
| Étape | reste à payer PROP_A | préfacture PROP_A |
|---|--:|--:|
| Baseline | 340 | 0 |
| Charge refacturable 100 € confirmée (A_CONTROLER) | 340 | 0 |
| **`valider()` → VALIDE** (journalisé, acteur=recette) + Lot3/Lot9/Lot10 | **440** | **100** |

L'impact financier est donc **déclenché par la validation applicative**, jamais avant.

## Contrôles d'intégrité — `app/services/charges_controles_integrite_service.py`
| Contrôle | Code | Niveau |
|---|---|---|
| Charge refacturable VALIDE sans propriétaire | `CTRL_CHG_REFAC_SANS_PROPRIETAIRE` | **BLOQUANT** |
| Propriétaire inconnu du référentiel | `CTRL_CHG_PROPRIETAIRE_INCONNU` | **BLOQUANT** |
| Propriétaire incohérent avec le logement à la période | `CTRL_CHG_PROPRIETAIRE_INCOHERENT_LOGEMENT` | **BLOQUANT** |
| Statut non ingérable | `CTRL_CHG_STATUT_NON_INGERABLE` | INFO |

Message bloquant : « Charge refacturable validée sans propriétaire déterminé : impossible de calculer
la préfacture et le montant dû. » Sortie : `statut`, `nb_bloquants`, `recalcul_fiable`, et pour chaque
anomalie charge_id / logement / propriétaire / mois / montant / détail.

Une charge refacturable **non validée** sans propriétaire n'est PAS bloquante (rien n'est encore
calculé) — vérifié par test.

## Tests — `tests/test_charges_validation_et_controles.py` (14 tests, tous verts)
Liste + filtres, validation (statut + persistance disque + journal), rejet (commentaire obligatoire,
ligne conservée), double validation refusée, charge inexistante, flags désactivés, **write-guard hors
racine → refus sans modification**, contrôle orphelin bloquant, contrôle OK quand propriétaire
matérialisé, propriétaire inconnu, propriétaire incohérent, charge globale non refacturable ignorée,
charge non validée ignorée.

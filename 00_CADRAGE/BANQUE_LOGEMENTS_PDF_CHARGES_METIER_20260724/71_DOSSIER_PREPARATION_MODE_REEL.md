# 71 — Dossier de préparation du mode réel (technique uniquement, mode réel NON activé)

Ce document prépare le passage éventuel au mode réel. **Aucun flag n'est activé par ce document.
Aucune phase n'est exécutée dans cette mission.**

## Sauvegardes préalables (avant toute activation)

| Élément | Procédure | Vérification |
|---|---|---|
| Sources réelles (85 fichiers historiques + relevé Banque) | copie intégrale hors Git, hash SHA256 avant/après chaque opération | déjà pratiqué à chaque mission de recette globale — procédure éprouvée |
| `app.db` réel | copie avant toute activation d'un writer réel | déjà pratiqué (copie utilisée comme base des environnements de copies) |
| Sorties pipeline (Lot9-13) | copie avant tout nouveau run réel | le mécanisme « sauvegarde avant écrasement » existe déjà dans `/calculs` (vérifié à chaque run) |
| Point de restauration Git | `git log -1` avant activation, tag ou simple SHA noté | `HEAD` actuel : voir `HANDOFF_CANONIQUE.md` |

## Flags nécessaires (déjà existants dans le code, tous à `False` par défaut)

| Flag | Rôle | État actuel |
|---|---|---|
| `RECETTE_MODE` | doit passer à `0`/absent pour sortir du mode recette | actuellement `1` en recette |
| `BANQUE_REAL_WRITE_ENABLED` / `..._CONFIRMATION_ENABLED` | écriture réelle Banque | `False` par défaut, jamais activé en réel |
| `CHARGES_REAL_WRITE_ENABLED` / `..._CONFIRMATION_ENABLED` | écriture réelle Charges | idem |
| `FACTURES_REAL_WRITE_ENABLED` / `..._CONFIRMATION_ENABLED` | écriture réelle Factures | idem |
| `COMPTABILITE_REAL_WRITE_ENABLED` / `..._CONFIRMATION_ENABLED` | écriture réelle Comptabilité | idem |
| `MENAGES_CYCLE_REAL_WRITE_ENABLED` / `..._CONFIRMATION_ENABLED` | écriture réelle cycle Ménages | idem |
| `CALCULS_REAL_RUN_ENABLED` / `..._CONFIRMATION_ENABLED` | exécution réelle du pipeline | idem |
| `MENAGES_REAL_RECALC_ENABLED` | recalcul réel Ménages (Excel) | idem |

Double verrou déjà en place partout : `RECETTE_MODE` doit être cohérent avec chaque flag
individuel — un flag seul ne suffit jamais à activer une écriture.

## Ordre d'activation proposé (aucune phase exécutée ici)

**PHASE 0 — Lecture seule.** Aucun flag activé. Valider que l'application lit correctement les
sources réelles sans écrire nulle part (déjà largement prouvé par les recettes globales — cette
phase est de facto déjà couverte).

**PHASE 1 — Écritures sur copie de production.** Réactiver l'environnement de copies actuel
(ou un nouveau, hash-vérifié) avec les writers activés, comme fait dans cette série de missions —
**déjà accompli**, pas une nouvelle étape à faire.

**PHASE 2 — Un seul module writer en réel.** Choisir le module le moins risqué en premier
(candidat naturel : Charges, déjà « TERMINÉ, chaîne exercée » selon `48_ROADMAP_RESTANTE_PROJET.
md`). Activer uniquement `CHARGES_REAL_WRITE_ENABLED`+`CONFIRMATION`, `RECETTE_MODE=0`. Contrôler
après : aucune autre écriture possible (double verrou), sources réelles inchangées ailleurs.

**PHASE 3 — Writers suivants, un par un.** Ordre proposé selon la maturité documentée
(`48_ROADMAP_RESTANTE_PROJET.md`) : Banque (import) → Factures/Règlements → Comptabilité →
Ménages (cycle applicatif) → pipeline (`CALCULS_REAL_RUN_ENABLED`, en dernier, car il dépend de
tous les autres).

**PHASE 4 — Clôture et contrôle.** Activer la clôture comptable réelle uniquement après qu'au
moins un cycle complet (Achats/Ventes/Banque/Caisse) ait tourné en réel sans anomalie bloquante.

**PHASE 5 — Exploitation normale.** Retirer les bandeaux/diagnostics de recette, documenter la
date de bascule effective.

## Contrôles après chaque activation (identiques quel que soit le module)

1. Hash des sources réelles inchangé (sauf le fichier que le module vient précisément de créer/
   modifier, attendu).
2. `git status` propre côté code (aucune activation de flag ne doit toucher au dépôt Git).
3. Tests ciblés du module rejoués en conditions réelles (subset pertinent).
4. Recette navigateur du module activé, sur les vraies données.
5. Vérification qu'aucun autre writer n'a été activé implicitement (double verrou).

## Procédure d'arrêt / rollback

- Couper le flag concerné (retour à `False`) — l'application refuse alors immédiatement toute
  nouvelle écriture de ce type (vérifié par les tests `test_recette_guard.py` et équivalents).
- Restaurer les sources réelles depuis la sauvegarde si une écriture s'est produite de façon non
  souhaitée (ne devrait jamais arriver si le double verrou fonctionne — mécanisme de dernier
  recours, jamais utilisé à ce jour dans aucune mission).
- Restaurer `app.db` depuis la copie de sauvegarde si l'état applicatif doit être réinitialisé.
- Consigner l'incident dans `JOURNAL_ANOMALIES.md`, jamais silencieusement.

## Journal d'audit

Chaque génération d'écriture comptable/bancaire/facture porte déjà un acteur, un horodatage et un
identifiant opaque (`audit_events`, déjà en place, 42 lignes dans l'`app.db` réel actuel). Aucun
nouveau mécanisme d'audit à construire pour le passage au mode réel — celui-ci existe déjà et a
été exercé dans toutes les missions de recette globale.

## Ce dossier NE constitue PAS une autorisation

Aucune phase ci-dessus n'a été exécutée par cette mission. Le mode réel reste désactivé
(`RECETTE_MODE=1` partout où ce chantier a tourné). Voir `72_CHECKLIST_GO_NO_GO_MODE_REEL.md` pour
le verdict formel.

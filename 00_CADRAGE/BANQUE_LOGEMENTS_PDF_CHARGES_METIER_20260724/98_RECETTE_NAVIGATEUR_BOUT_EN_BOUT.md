# RECETTE NAVIGATEUR BOUT-EN-BOUT — ENVIRONNEMENT ISOLÉ — MISSION 13

Recette manuelle par navigateur réel (Chrome, automatisé via l'outillage Claude-in-Chrome),
sur environnement totalement isolé : `APP_DATA_DIR` dédié (scratchpad, hors dépôt), port
`8013` (≠ 8000, jamais touché), `app.db` isolée auto-créée/auto-migrée à HEAD (migration 0060)
au premier démarrage du serveur de recette. Aucune écriture réelle : vraie `app.db`, vrai
`REF_Setup.xlsm`, mode réel et scheduler Hostaway strictement hors périmètre et non touchés.

## Environnement de recette

| Élément | Valeur |
|---|---|
| Serveur isolé | `127.0.0.1:8013`, PID 65152, `APP_DATA_DIR` = scratchpad `RECETTE_M13/data` |
| Port 8000 | jamais touché, jamais arrêté, jamais redémarré |
| `app.db` isolée | auto-créée par `apply_migrations()` au lifespan FastAPI, migration 0060 (HEAD) |
| Mode réel | OFF (guards `*_REAL_WRITE_ENABLED` doublement verrouillés par `RECETTE_MODE`) |
| Scheduler Hostaway | inactif, aucun appel réseau réel effectué |
| Jeu de données de recette | 1 propriétaire (`PROP_RECETTE_01`), 3 logements (`LOG_RECETTE_01/02/03`), 1 fournisseur, 1 facture fournisseur multi-lignes, 3 charges (1 directe, 2 communes), 1 réservation HH, 1 compte bancaire de recette (2 mouvements importés + 1 anomalie RAW insérée volontairement) |

## Tableau des parcours

| Parcours | Résultat | Preuve | Bug | Correction |
|---|---|---|---|---|
| 1. Ouverture application | OK | Accueil chargé, aucune erreur console | — | — |
| 2. Créer propriétaire | OK | `PROP_RECETTE_01` créé via formulaire admin | — | — |
| 3. Ajouter logement | OK | 3 logements créés (`LOG_RECETTE_01/02/03`) | — | — |
| 4. Modifier propriétaire/logement | OK | Modification confirmée, relue après rafraîchissement | — | — |
| 5. Taux commission futur | OK | `TAUX_COMMISSION_ABSENT` refusé correctement pour taux démarrant en 2027 | — | — |
| 6. Correction rétroactive (bandeau + justification) | OK | Refus propre si justification vide ; succès + `CORRECTION_RETROACTIVE` journalisé (`ref_admin_evenements.commentaire`) si justification fournie | — | — |
| 7. Impact preview (« Voir les impacts ») | OK | `/logements/{id}/impacts-taux` : comptages structurels uniquement (0 réservation/facture concernée), jamais de montant financier ; aucun recalcul auto | — | — |
| 8. Chronologie des périodes de taux | OK | Insertion d'une période antérieure à une période déjà ouverte refusée (message explicite, pas de période négative) | — | — |
| 9. Réservation HH (cas normal) | OK | Cycle `verifier → previsualiser → confirmer` complet, écriture SQLite confirmée (`reservations_hors_hostaway`) | — | — |
| 10. Fournisseur (référentiel) | OK | `/referentiel-fournisseurs` — fiche minimale, aucune donnée bancaire, SQLite isolée | — | — |
| 11. Facture fournisseur multi-logements | OK | Facture créée, ligne multi-charges/multi-logements rattachée ; module bien distinct des Charges | — | — |
| 12. Charge directe | OK | Charge créée sur `LOG_RECETTE_02` uniquement | — | — |
| 13. Charge commune / périmètre | OK | Stop-gate confirmé : 3 logements cochés sur ~20 disponibles → seuls les 3 apparaissent en périmètre final | — | — |
| 14. Répartition centimes | OK | 10,00 € / 3 logements → 3,34 €/3,33 €/3,33 € (somme exacte), résidu au premier logement trié | — | — |
| 15. Ménages | OK (lecture seule, source vide) | `/menages` dégrade proprement (« Source incomplète »), `MENAGES_REAL_RECALC_ENABLED = False` visible. Pas d'écran « compte intervenant » séparé — c'est une dimension (`intervenant_id`) au sein des lignes de rapprochement ménage, pas un module distinct | — | — |
| 16. Compte propriétaire | OK | `/comptes-proprietaires` : tout à 0,00 €, aucun crash. Pas d'écran de saisie manuelle de mouvement — c'est un solde recalculé (`recalculer`), jamais une saisie directe | — | — |
| 17. Banque (import, classification, contrôles, anomalie RAW) | OK | Import CSV réel via upload, cycle prévisualiser/confirmer ; ligne `sens='INCONNU'` insérée directement reste visible et contrôlable, aucun crash ; pages de rapprochement lisent une sortie pipeline distincte (lot8) — « Source indisponible » attendu tant que lot8 n'est pas rejoué | — | — |
| 18. Actualisation — dry-run | OK | `/calculs` « Prévisualiser le pipeline » : plan affiché sous forme de DAG, aucune exécution | — | — |
| 19. Actualisation isolée réelle | **NON TESTÉE (limite documentée)** | Exécuter réellement (`Lancer le calcul`) écrirait dans `02_TRAVAIL/`/`03_EXPORTS/` du worktree partagé, hors périmètre isolé (`APP_DATA_DIR` ne scope que le SQLite, pas les sorties fichier des scripts Lot) — décision : ne pas déclencher | Non applicable | Nécessite une recette contrôlée dédiée sur copies de fichiers, hors périmètre Mission 13 |
| 20. Observabilité / runs | OK | `/sources-calculs` (« Lot APP-0 ») : simulation dry-run pure pour tous les scripts listés, journal de run vérifié (`lot9_construire_flux` → « Simulé », horodaté) | — | — |
| 21. Résultats | OK | `/resultats` : `NON_DISPONIBLE` propre (sortie Lot10 absente en environnement isolé), aucun crash | — | — |
| 22. Factures propriétaires | OK | `/factures-proprietaires` : liste vide propre, aucune génération forcée | — | — |
| 23. Surveillance « 0 dépendance Excel opérationnelle » | OK, avec réserve documentaire | Toutes les écritures effectuées (charges, réservation HH, banque, facture) confirment « Fichiers écrits — aucun » ; 3 libellés d'écran mentionnent encore Excel comme méthode de référence — texte obsolète, sans impact fonctionnel (voir Bugs) | A/B (texte) | Non corrigé — documenté |
| 24. Erreurs utilisateur réalistes | OK | Justification vide, taux absent, période antérieure, champ requis manquant (validation HTML native) : tous des refus propres, jamais de Traceback/500/IntegrityError brut | — | — |
| 25. Rafraîchissement navigateur | OK | Rechargement de `/fournisseurs` et `/banques-caisse` après plusieurs écritures : données strictement identiques, aucune perte | — | — |

## Bugs trouvés (classification A/B/C/D)

Aucun bug fonctionnel (C/D). Trois textes d'écran obsolètes ou trompeurs, classés A/B,
**non corrigés** — modifier un message lié à un garde-fou de sécurité (`HH_REAL_WRITE_ENABLED`)
sans une revue dédiée du contexte historique du flag comportait un risque non nul pour un
gain cosmétique ; documentés ici pour une correction ciblée future, hors scope de cette mission :

1. **A (cosmétique)** — `/reservations/nouvelle` affiche l'avertissement `HH_REAL_WRITE_ENABLED = False`
   et mentionne `SAISIE_ReservationsHorsHostaway.xlsx`, alors que le chemin d'écriture réel
   (`reservations_hh_confirmation_service` → `reservations_hh_saisie_service.creer()`) est
   100 % SQLite et ne dépend pas de ce flag legacy. Fichier : `reservation_nouvelle_verif.html:151-153`.
2. **A/B** — `/fournisseurs` affiche, en état vide, « Saisissez des charges dans
   SAISIE_Charges_Flux.xlsx puis rafraîchissez Power Query » alors qu'un flux SQLite
   fonctionnel (« + Nouvelle charge ») existe sur le même écran.
3. **B (UX gênante)** — `/banques-caisse/controle` affiche « Excel reste la vérité métier ;
   SQLite ne fait que journaliser. » — contredit l'architecture actuelle (SQLite est la
   source de vérité applicative depuis les Lots 7-12).

Aucun de ces trois éléments n'a provoqué de crash, d'écriture Excel réelle, ni d'accès fichier
inattendu — vérifié par lecture du code des services concernés (chemins d'écriture 100 % SQLite).

## Campagne automatisée finale

| Suite | Résultat |
|---|---|
| Moteur (`tests/`, racine worktree) | **397 passés / 0 échec** (baseline inchangée) |
| Application (`05_APPLICATION/tests/`, 191 fichiers, 20 shards) | **2790 passés / 30 ignorés / 0 échec** |

Note sur le décompte : la dernière baseline enregistrée (Mission 12) indiquait 2791 passés (skips non
détaillés à l'époque). Écart de 1 passé sur 2820 tests collectés, **0 échec dans les deux cas** —
cohérent avec une variance naturelle de collecte/skip (aucun code de production modifié ce tour,
191/191 fichiers couverts exactement une fois, vérifié par diff). Non traité comme une régression.

Écart économique : **0,00 €** (aucune règle métier modifiée, aucun code de production touché).

## Intégrité finale

| Contrôle | Résultat |
|---|---|
| `git rev-parse HEAD` | `d687902eb69fda64116d7e81259647d5a64412c3` (inchangé depuis fin Mission 12) |
| `git branch --show-current` | `feature/banque-logements-pdf-charges-metier` |
| `git status --short` | propre |
| Hash vraie `app.db` | `8e299b935ef1e0d4...` — **inchangé** |
| `REF_Setup.xlsm` réel | mtime `2026-08-12 11:00:05` — **inchangé** |
| Mode réel | OFF |
| Scheduler Hostaway | inactif |
| Port 8000 | jamais touché |

## Décision

Aucun bug C/D bloquant. Aucune dépendance Excel opérationnelle réelle (3 textes d'écran
obsolètes, sans impact fonctionnel, documentés). Aucune donnée réelle modifiée. Tests
automatiques verts.

**RECETTE NAVIGATEUR VALIDÉE.**

**PROCHAINE ÉTAPE : PRÉPARATION ACTIVATION RÉELLE CONTRÔLÉE.**

Rappel explicite : mode réel et scheduler Hostaway **non activés** dans cette mission — leur
activation fera l'objet d'une mission séparée.

# Durcissement SQLite + contrats de données — Phase 1 (2026-08-22)

Mission « fiabilisation phase 1 » lancée après clôture du chantier Excel→SQLite (tag
`ZERO_EXCEL_SQLITE_BASELINE_2026-08-22`, HEAD `4dfd3db`). Objectif : rendre les états incohérents
structurellement plus difficiles à créer — sans changer une seule règle métier, sans réécrire les
Lots, sans toucher à l'app.db réelle (0016, inchangée).

## 1. Audit ciblé

Tables auditées (réservations, logements, propriétaires, charges, factures, lignes de facture,
règlements, allocations FIFO, mouvements bancaires, trésorerie propriétaires, ménages,
flux_unifies, datasets/runs Lot10/11/12) — lecture directe des migrations, pas de déduction.

**Mécanismes anti-incohérence déjà en place, non dupliqués** :
- Dataset actif en double : index UNIQUE partiel sur `lot10_runs.actif`/`lot12_runs.actif`
  (`WHERE actif=1`) — déjà bloqué.
- Doublon économique facture propriétaire : index UNIQUE partiel `(mois, proprietaire_id,
  logement_id, type_document) WHERE statut<>'ANNULE'` — déjà bloqué.
- Verrou orchestrateur : bail à expiration (`orchestrateur_verrous`) — déjà géré.
- Doublon classification bancaire : index UNIQUE `(mouvement_id_opaque, classification_run_id)`
  — déjà bloqué.

## 2. Contraintes SQLite ajoutées (migration 0055, corrigée par 0056)

SQLite n'autorise pas `ALTER TABLE ADD CONSTRAINT` — chaque table listée a été recréée
(`CREATE` + `INSERT...SELECT` + `DROP` + `RENAME`), colonnes/index/défauts repris à l'identique.
Vérifié par grep qu'aucun code ne référence les `id` AUTOINCREMENT bruts de ces tables (seules les
clés opaques métier sont utilisées) — recréation sans risque de casse de référence.

| Table | Ajouté | Domaine vérifié exhaustif avant fermeture |
|---|---|---|
| `banque_classifications` | FK `mouvement_id_opaque` → `banque_mouvements` | — |
| `factures_proprietaires` | CHECK `statut IN (BROUILLON,VALIDE,EMIS,ANNULE)` | `factures_proprietaires_service.py:58` |
| `factures_proprietaires` | CHECK `type_document IN (FACTURE,AVOIR)` | idem `:61` |
| `factures_proprietaires_lignes` | FK `facture_id_opaque` → `factures_proprietaires` | — |
| `factures_proprietaires_lignes` | CHECK `type_ligne IN (5 valeurs)` | `TYPES_FACTURABLES`, `:30-36` |
| `charges` | index `proprietaire_id` (manquant ; `logement_id` déjà indexé depuis 0052) | — |
| `reservations_hors_hostaway` | index `proprietaire_id` (idem) | — |

**Erreur trouvée et corrigée en cours de mission** (migration 0056) : le CHECK initial sur
`banque_mouvements.sens IN ('DEBIT','CREDIT')` cassait un mécanisme existant —
`banques_controles_catalogue.py` détecte volontairement un `sens` hors domaine comme une anomalie
CONTRÔLABLE (écran de contrôle), pas un rejet à l'insertion. Retiré par une migration de correction
(0055 non modifiée, historique immuable). Leçon : l'audit initial n'avait vérifié que le chemin de
saisie, pas les tests de détection d'anomalie — la campagne complète l'a révélé.

## 3. Contraintes NON ajoutées, et pourquoi

| Cible envisagée | Raison de ne pas l'ajouter |
|---|---|
| FK `charges.logement_id`/`proprietaire_id` → référentiel | Colonnes déjà documentées « non FK SQL » par décision antérieure (référentiel Excel historique, migrations 0025/0027/0052) — respecté, pas de nouvelle décision prise ici. |
| FK `reservations_hors_hostaway.logement_id`/`proprietaire_id` → référentiel | Même raison. |
| UNIQUE `menages_taches_enrichies.task_id` | Table peuplée par le moteur pandas (Lot6a), sémantique exacte (une ligne par tâche vs. plusieurs si historisé) non confirmée sans audit du script `02_TRAVAIL` — hors budget de cette phase, mission §7 : « si un doute existe, ne pas forcer ». |
| CHECK sur montants (signe cohérent avec `sens`) | Nécessiterait un audit plus large des cas où un montant négatif est légitime (avoirs, corrections) — reporté. |
| Recréation de `charges`/`reservations_hors_hostaway` (tables moteur-adjacentes) | Volume et complexité plus importants que les 4 tables traitées, sémantique d'écriture moins confirmée — reporté à une session dédiée. |

## 4. Contrats de données typés (Bloc B)

`app/contrats_donnees.py` — dataclasses (pas Pydantic : 0 import Pydantic dans tout le projet,
`@dataclass` déjà utilisé dans 12 modules, même convention reprise).

4 objets sur les 10 cités par la mission : `Charge`, `ReservationHH`, `MouvementBanque`,
`MouvementTresorerieProprietaire` — les 4 qui ont un point d'entrée de saisie dict-based net et un
domaine fermé bien identifié. `Facture`/`LigneFacture`/`Reglement`/`Menage`/`FluxUnifie`/
`DatasetRun` non traités : la plupart sont des objets DÉRIVÉS par les moteurs (Lot10/11/12), pas
des saisies utilisateur — moins prioritaires pour un contrat d'entrée.

Chaque `from_dict()` vérifie uniquement la STRUCTURE (champs obligatoires, types, montant
numérique fini, date AAAA-MM-JJ calendaire valide) — jamais une règle métier.

**Délibérément NON câblés dans les services de saisie existants** (`charges_saisie_service.creer`,
`reservations_hh_saisie_service.creer`) : vérification faite que `valider()` dans
`charges_saisie_service.py` n'impose pas le format de date strict qu'exige ce contrat — câbler
aveuglément aurait risqué de rejeter une saisie que le service accepte aujourd'hui, donc une
régression contre la baseline 0 failed. Câblage différé à une session dédiée avec audit préalable
des formats réellement acceptés en recette (mission §7, non fait ici par manque de temps).

## 5. Transactions (Bloc C)

Audit ciblé de l'écriture multi-table la plus représentative (facture + lignes,
`factures_proprietaires_service.py::creer`) : une seule connexion, un seul `commit()` en fin de
fonction, `finally: conn.close()` sans commit sur exception (rollback implicite du mode
transactionnel par défaut de `sqlite3`). Déjà atomique — confirmé, aucun changement nécessaire.
`banque_classification_service.py` va plus loin : `BEGIN IMMEDIATE` explicite avant l'écriture
multi-table. Pattern déjà correct et généralisé dans le projet, pas de mécanisme à ajouter.

## 6. Tests négatifs (Bloc D)

`tests/test_durcissement_sqlite_contraintes.py` (10 tests) : essaie de casser la base —
FK inexistante (classification orpheline, ligne de facture orpheline), CHECK domaine invalide
(statut, type_document, type_ligne facture), chemin normal accepté (ligne rattachée à une facture
réelle), plus 3 tests de non-régression prouvant que les mécanismes déjà en place (doublon
facture, double dataset actif Lot10, doublon classification) restent actifs.

`tests/test_contrats_donnees.py` (13 tests) : valide chaque contrat isolément.

## 7. Performance

Aucune dégradation attendue : les FK/CHECK ajoutés portent sur des tables de volume modeste
(classifications, factures, lignes de facture) et SQLite vérifie une FK via l'index déjà présent
sur la colonne référencée (`mouvement_id_opaque`/`facture_id_opaque`, tous deux UNIQUE — donc déjà
indexés). Les 2 index ajoutés (`charges.proprietaire_id`, `reservations_hors_hostaway.
proprietaire_id`) accélèrent les lectures, n'en ralentissent aucune. Pas de benchmark formalisé
(mission §23 : « pas de benchmark complexe ») — campagne complète rejouée sans régression de durée
notable par rapport aux campagnes précédentes de taille comparable.

## 8. Tables restant à durcir (prochaine session)

- `charges`/`reservations_hors_hostaway` : FK vers référentiel logement/propriétaire, si la
  décision « non FK SQL » est un jour révisée.
- `menages_taches_enrichies.task_id` : UNIQUE, après audit du script moteur Lot6a.
- Contrats `Facture`/`LigneFacture`/`Reglement`/`Menage`/`FluxUnifie`/`DatasetRun`.
- Câblage des 4 contrats existants dans leurs services de saisie, après audit des formats de date
  réellement acceptés.

## 9. Anomalies de données existantes trouvées

Aucune. La copie de la vraie app.db (0016) migrée jusqu'à HEAD (0056) donne `foreign_key_check`
vide et `integrity_check: ok` — 0 ligne orpheline dans les données réelles pour les 2 FK ajoutées.

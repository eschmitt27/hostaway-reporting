# Préparation technique du mode réel (2026-08-10)

**Ce document ne déclenche aucune activation.** Il documente ce qui existe, dans quel ordre une
activation future devrait se faire, et ce qui la bloque aujourd'hui. Aucun flag n'a été modifié,
aucun writer n'a été activé, le mode réel reste NO GO.

## 1. Découverte structurante — l'activation n'est PAS un simple changement de variable

Audit direct de `05_APPLICATION/app/config.py` : **tous les writers sont doublement verrouillés**,
et la majorité sont conditionnés par `RECETTE_MODE` :

```python
CHARGES_REAL_WRITE_ENABLED       = RECETTE_MODE and _env_flag("CHARGES_REAL_WRITE_ENABLED")
BANQUE_REAL_WRITE_ENABLED        = RECETTE_MODE and _env_flag("BANQUE_REAL_WRITE_ENABLED")
FACTURES_REAL_WRITE_ENABLED      = RECETTE_MODE and _env_flag("FACTURES_REAL_WRITE_ENABLED")
MENAGES_CYCLE_REAL_WRITE_ENABLED = RECETTE_MODE and _env_flag("MENAGES_CYCLE_REAL_WRITE_ENABLED")
COMPTABILITE_REAL_WRITE_ENABLED  = RECETTE_MODE and _env_flag("COMPTABILITE_REAL_WRITE_ENABLED")
CALCULS_REAL_RUN_ENABLED         = RECETTE_MODE and _env_flag("CALCULS_REAL_RUN_ENABLED")
MENAGES_REAL_RECALC_ENABLED      = RECETTE_MODE and _env_flag("MENAGES_REAL_RECALC_ENABLED")
```

**Conséquence directe** : dans une instance NON recette (`RECETTE_MODE=0`, c'est-à-dire une
instance « réelle »), **aucun de ces writers ne peut être activé**, quelles que soient les
variables d'environnement positionnées. Positionner `CHARGES_REAL_WRITE_ENABLED=1` sur une
instance réelle ne produit rien : l'expression retombe sur `False`.

Trois writers sont même **codés en dur à `False`**, sans aucun mécanisme d'activation :

```python
HH_REAL_WRITE_ENABLED            = False   # réservations hors Hostaway
REF_ASSOC_MODE_REAL_WRITE_ENABLED = False  # migration REF_Assoc_Mode
CONTROLES_REAL_WRITE_ENABLED     = False   # suivi humain des contrôles
```

**L'activation du mode réel exige donc une modification délibérée de `app/config.py`** — ce n'est
pas une opération de configuration, c'est un changement de code, à faire sous revue, writer par
writer. C'est un choix de conception protecteur (impossible d'activer le réel par accident ou par
une variable d'environnement mal placée), pas un défaut. Il doit être connu avant toute
planification de bascule.

## 2. Inventaire des writers réellement existants

| Writer | Fonction | Flag | Risque | Prérequis | Test avant | Test après | Rollback |
|---|---|---|---|---|---|---|---|
| Charges | Écrit `SAISIE_Charges_Flux.xlsx` (source métier Excel) | `CHARGES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` (recette-gated) | **ÉLEVÉ** — touche une source métier réelle | Backup + hash `SAISIE_Charges_Flux.xlsx` ; mois ouvert | `test_charges_confirmation_e2e/route/audit` verts | Relire la charge créée, vérifier montant/catégorie/impact, relancer Lot3 | Restaurer le `.xlsx` depuis le backup horodaté |
| Réservations hors Hostaway | Écrit `SAISIE_ReservationsHorsHostaway.xlsx` | `HH_REAL_WRITE_ENABLED` — **codé en dur `False`** | **ÉLEVÉ** — source métier réelle | Modification de `config.py` obligatoire ; backup + hash | `test_reservations_hh`, `test_saisie_hh_*` verts | Relire la réservation, vérifier propriétaire/taux/ménage résolus, relancer lot4quater→lot13 | Restaurer le `.xlsx` |
| Factures / Règlements | SQLite applicatif (`factures`, `reglements`) | `FACTURES_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul, aucun fichier métier | Backup `app.db` | `test_factures*`, `test_reglements*` verts | Vérifier solde/statut dérivé/historique | Restaurer `app.db` |
| Ménages (cycle) | SQLite applicatif (`menages_cycle`) | `MENAGES_CYCLE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul | Backup `app.db` | `test_menages_cycle*` verts | Vérifier statut/historique | Restaurer `app.db` |
| Comptabilité | SQLite applicatif (écritures, périodes) | `COMPTABILITE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | MOYEN — SQLite seul ; **mappings comptables encore PROVISOIRES** | Backup `app.db` ; **arbitrage des comptes (`70`) fortement recommandé avant** | `-k comptabilite` vert (142) | Vérifier équilibre débit=crédit, balance, période | Contrepassation (jamais de suppression) + restauration `app.db` |
| Banque (contrôle/catégorisation) | Écrit un onglet `OVERRIDE_APP4B` dans une **copie** de `BANQUE_LOT8_IMPORT.xlsx` (workspace isolé sous `data/`) | `BANQUE_REAL_WRITE_ENABLED` + `_CONFIRMATION_` | FAIBLE — travaille sur copie par conception, jamais sur les onglets moteur | Backup `app.db` | `-k banque` vert (612) | Vérifier décision + historique append-only | Restaurer `app.db` ; la copie est régénérable |
| Calculs (runs pipeline) | Exécute les lots sur l'arborescence métier | `CALCULS_REAL_RUN_ENABLED` + `_CONFIRMATION_` | **ÉLEVÉ** — régénère les MASTER réels | Backup complet `02_TRAVAIL` + `03_EXPORTS` | `test_calculs*` verts ; run identique sur copies | Comparer avant/après, vérifier idempotence | **Rollback natif intégré** : bouton « Restaurer les sorties d'avant ce run » (exercé ce tour : 8 fichiers restaurés) |
| Ménages (recalcul sources) | Régénère Lot6b/6c (atteint le réseau : feuille Google) | `MENAGES_REAL_RECALC_ENABLED` | **ÉLEVÉ** — accès réseau + données réelles | Workspace contrôlé obligatoire (`menages_chaine_service`) | Chaîne ménages verte sur copies | Vérifier 4 flux séparés | Restaurer les MASTER ménages |
| Suivi des contrôles | SQLite applicatif (suivi humain) | `CONTROLES_REAL_WRITE_ENABLED` — **codé en dur `False`** | FAIBLE — journalise seulement, ne masque jamais le moteur | Modification de `config.py` obligatoire | `-k controle` vert | Vérifier que l'anomalie moteur reste visible | Restaurer `app.db` |

**Aucun writer n'a été créé, modifié ou activé par cette mission.**

## 3. Ordre d'activation proposé (technique, non exécuté)

Ordre dérivé des dépendances réelles constatées, du risque le plus faible au plus élevé — et non
de la simple existence d'un writer. Un writer ne doit être activé que s'il correspond à un besoin
métier réel exprimé.

0. **Backup complet + manifest SHA256** (cf. §4). Point de restauration nommé et daté.
1. **Contrôle de santé en lecture seule** : instance réelle démarrée sans aucun writer, parcours de
   consultation des 18 modules, aucune erreur.
2. **Contrôle des migrations** : `test_sqlite_migrations` vert, migrations idempotentes vérifiées.
3. **Writers SQLite purs d'abord** (aucun fichier métier touché, rollback = restaurer `app.db`) :
   a. Suivi des contrôles (le plus inoffensif : journalise, ne décide rien) ;
   b. Banque contrôle/catégorisation (travaille sur copie par conception) ;
   c. Factures / Règlements ;
   d. Ménages (cycle).
4. **Comptabilité** — **seulement après arbitrage des mappings comptables** (`70`), sinon les
   écritures produites porteraient des comptes provisoires en production.
5. **Writers touchant des sources métier Excel** (risque élevé, un seul à la fois, avec backup
   dédié avant chacun) : Charges, puis Réservations hors Hostaway.
6. **Calculs (runs réels)** en dernier : il régénère tout l'aval. Son rollback natif est déjà
   éprouvé, mais il a le plus large rayon d'action.
7. **Ménages (recalcul sources)** : à traiter séparément, car il atteint le réseau — exige le
   workspace contrôlé, jamais une exécution directe.

Entre chaque activation : une seule opération réelle de faible montant, contrôle, puis décision
explicite de continuer ou de restaurer.

## 4. Procédure backup / rollback (commandes préparées, non exécutées sur le réel)

Périmètre à sauvegarder avant toute activation :

- `05_APPLICATION/data/app.db` (base applicative réelle) ;
- `01_SOURCES_BRUTES/` en entier (REF_Setup, Banque, Charges, ReservationsHH, AcomptesProprietaires,
  AirCover, ImputationsAirbnb, AjustementsPostCloture, VRBO, MenagesExternes) ;
- `02_TRAVAIL/` (tous les MASTER : Lot1, Lot3, Lot4*, Lot5, Lot6*, Lot7, Lot8, Lot9, Lot10, Lot11,
  Lot12) ;
- `02_DONNEES_NORMALISEES/` ;
- `03_EXPORTS/` ;
- manifest SHA256 de l'ensemble.

Le script de baseline utilisé par cette mission est réutilisable tel quel comme générateur de
manifest (950 fichiers couverts, SHA256 + taille + mtime, sortie TSV hors arbre métier).

Procédure :

1. **Point de restauration** : copier l'arborescence ci-dessus vers `_BACKUPS_MODE_REEL/<horodatage>/`,
   produire le manifest SHA256, vérifier que le manifest est relisible.
2. **Arrêt** : arrêter l'instance applicative réelle (le port 8000/PID 21136 actuel est un processus
   historique de l'utilisateur — ne jamais le tuer sans son accord explicite).
3. **Restauration** : recopier les fichiers depuis le backup, restaurer `app.db`, revérifier le
   manifest SHA256 (comparaison stricte, 0 différence attendue).
4. **Contrôle post-restauration** : relancer l'application en lecture seule, vérifier les totaux
   de référence (REEL = COMPTABLE + HORS_COMPTA, écart 0,00 €) et l'absence d'objet orphelin.
5. **Reprise** : ne réactiver aucun writer tant que la cause de la restauration n'est pas comprise
   et documentée.

## 5. Conditions GO / NO GO

**Conditions GO (toutes requises) :**
- fiche de validation humaine remplie et signée par l'utilisateur pour les 16 modules ;
- arbitrages comptables rendus (`70` : 606000, frais bancaires, trésorerie propriétaires,
  associés/IK) — au minimum ceux qui concernent les écritures qui seront réellement produites ;
- modification revue de `app/config.py` levant le verrou `RECETTE_MODE` pour le seul writer visé ;
- backup complet + manifest SHA256 réalisés et vérifiés ;
- procédure de rollback testée au moins une fois de bout en bout ;
- campagne de tests verte au moment de la bascule.

**Conditions NO GO (une seule suffit) :**
- validation humaine non signée ;
- mappings comptables non arbitrés si le writer Comptabilité est concerné ;
- backup absent, incomplet ou non vérifié ;
- rollback non testé ;
- anomalie BLOQUANT ouverte sur le périmètre concerné ;
- activation de plusieurs writers simultanément.

**Statut actuel : NO GO** — validation humaine LOT D/E non rendue, arbitrages comptables non
rendus, aucune modification de `config.py` faite ni souhaitée à ce stade.

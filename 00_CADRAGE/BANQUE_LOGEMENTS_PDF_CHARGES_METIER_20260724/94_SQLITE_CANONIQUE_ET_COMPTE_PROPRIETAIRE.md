# 94 — SQLite canonique et compte propriétaire FIFO

Ce document couvre deux constructions livrées ensemble : le **référentiel Setup en SQLite**
(migration 0029) et le **compte global propriétaire avec allocations FIFO** (migration 0030).

Il décrit ce qui existe. Ce qui reste à construire est listé au §7 sans embellissement.

---

## 1. Référentiel Setup en SQLite

### 1.1 Audit préalable

Les 73 tables présentes en migration 0028 ont été inventoriées avant d'écrire quoi que ce soit :
toutes sont transactionnelles (factures, écritures, mouvements) ou des journaux. **Aucune table de
référentiel n'existait.** Les 28 tables créées ne dupliquent donc rien.

Deux ensembles ont en revanche été identifiés comme réutilisables et n'ont **pas** été recréés :

| Besoin | Table réutilisée |
|---|---|
| Registre des traitements (§9 de la mission) | `calculs_runs`, `calculs_run_lots`, `calculs_indicateurs`, `calculs_sauvegardes`, `pipeline_runs` |
| Paiements et reversements propriétaires | `mouvements_tresorerie_proprietaires` (0025) |
| Créances propriétaires | `factures_proprietaires` (0027) |

### 1.2 Correspondance onglet → table

28 onglets, 258 colonnes, 366 lignes dans le référentiel réel. Le nom de table dérive
mécaniquement de l'onglet : `REF_Taux_Commission` → `ref_taux_commission`. La clé primaire est la
première colonne partout, sauf `REF_Cloture_Mensuelle` qui n'a pas de colonne `*_id` et se lit par
`mois`.

Le schéma a été **généré depuis le classeur réel**, pas recopié. Le catalogue déclaratif
`app/services/ref_setup_catalogue.py` est la source unique : importateur, repository et
`test_sqlite_migrations` le lisent tous. Ils ne peuvent donc pas diverger entre eux.

### 1.3 Pourquoi toutes les colonnes sont TEXT

Décision assumée, pour trois raisons :

1. **Lossless.** Une cellule Excel hétérogène est conservée telle qu'elle est saisie, et l'import
   reste comparable d'une exécution à l'autre — condition de l'idempotence et des empreintes.
2. **Cohérent avec l'existant.** `readers/csv_reader.py` rend déjà des chaînes et les services
   convertissent au moment de l'usage. Typer ici créerait deux conventions selon la source d'une
   même donnée.
3. **Ce n'est pas à une migration de trancher.** Décider colonne par colonne ce qu'est un nombre ou
   une date est une interprétation métier.

Les contrôles de type vivent dans l'importateur, qui **refuse** plutôt que de convertir en silence.

### 1.4 Contrôles de l'import

**Bloquants** — rien n'est écrit, l'import est intégral ou inexistant (une seule transaction pour
les 28 tables) :

| Code | Constat |
|---|---|
| `REF_SETUP_SOURCE_ABSENTE` | classeur introuvable |
| `REF_SETUP_SOURCE_ILLISIBLE` | classeur non ouvrable |
| `REF_SETUP_ONGLET_MANQUANT` | un onglet du catalogue est absent |
| `REF_SETUP_COLONNES_INATTENDUES` | colonnes renommées, ajoutées ou supprimées |
| `REF_SETUP_CLE_VIDE` | une ligne sans clé |
| `REF_SETUP_CLE_DUPLIQUEE` | une clé présente deux fois |
| `REF_SETUP_SCHEMA_ABSENT` | migration 0029 non appliquée |

**Non bloquants** — signalés, jamais arbitrés :

| Code | Constat |
|---|---|
| `REF_SETUP_ONGLET_HORS_CATALOGUE` | onglet présent dans Excel mais inconnu — non importé |
| `REF_SETUP_RELATION_ORPHELINE` | un `logement_id` / `proprietaire_id` sans cible |
| `REF_SETUP_HISTORIQUE_CHEVAUCHANT` | deux périodes qui se recouvrent sur la même clé |

**Pourquoi cette frontière.** Les anomalies métier existent peut-être déjà dans le référentiel
réel. Bloquer sur elles reviendrait à exiger que les données soient propres avant de pouvoir les
regarder, et les rendrait invisibles au lieu de les exposer.

### 1.5 Déterminisme

La normalisation des valeurs Excel est explicite, sans quoi l'empreinte de contenu changerait sans
qu'aucune donnée n'ait bougé :

| Valeur Excel | Stockée |
|---|---|
| `None` | `""` |
| `datetime(2025,1,1)` (minuit) | `2025-01-01` |
| `datetime(2025,1,1,14,30)` | `2025-01-01 14:30:00` |
| `35.0` | `35` |
| `0.15` | `0.15` |
| `True` / `False` | `OUI` / `NON` |

L'empreinte par onglet est calculée sur les lignes **triées** : réorganiser des lignes dans Excel
ne change pas le référentiel, cela ne doit donc pas faire croire à une modification.

### 1.6 Résultat mesuré

Sur le classeur réel : **28 onglets, 366 lignes, 0 avertissement**. Import rejoué : mêmes
empreintes de contenu sur les 28 onglets, mêmes compteurs. Le journal, lui, conserve les deux
passages — y compris les tentatives refusées.

Un défaut a été trouvé et corrigé pendant la mise au point : le contrôle de chevauchement groupait
sur `logement_id` seul, alors que `REF_Taux_Commission` s'historise au niveau **propriétaire**
(`logement_id` vide). Il ignorait donc la totalité des lignes réelles.

### 1.7 Écran

`/referentiel-setup` : état, contenu par référentiel, historique des imports. Bouton
*Prévisualiser* → page d'effet détaillée → *Confirmer l'import*. Aucun bouton ne déclenche un
import sans avoir montré son effet.

---

## 2. Compte global propriétaire

### 2.1 Principe

Un propriétaire a **un** compte, tous logements confondus. L'argent disponible solde ses factures
de la plus ancienne à la plus récente. **L'utilisateur ne choisit jamais** quelle facture un
paiement solde.

### 2.2 Ce qui reste distinct

Le compte calcule une position ; il ne fusionne aucun objet.

| Objet | Où il vit |
|---|---|
| Facture propriétaire | `factures_proprietaires` (0027) |
| Paiement reçu | `mouvements_tresorerie_proprietaires`, sens `PROPRIETAIRE_VERS_SOCIETE` |
| Somme à reverser | `mouvements_tresorerie_proprietaires`, sens `SOCIETE_VERS_PROPRIETAIRE` |
| Compensation | allocation de source `REVERSEMENT` |
| Crédit / acompte | **aucun objet — dérivé** |

### 2.3 Le crédit n'est pas un objet

Un excédent de paiement ne crée ni facture fictive ni ligne de crédit : c'est la part d'une source
qui n'a trouvé aucune facture à solder. Il se déduit.

Conséquence directe et voulue : quand une facture est émise plus tard, le crédit existant la solde
au recalcul suivant, **sans qu'aucune écriture de report n'ait à être faite**.

### 2.4 Ordre de consommation

Deux sources : `PAIEMENT` et `REVERSEMENT`. Elles sont consommées dans l'ordre **chronologique**,
sans priorité de type — un ordre par type aurait demandé de décider laquelle prime, décision que
rien dans le métier ne tranche.

Les factures sont triées par **date d'émission**, départagées par numéro de facture puis par
identifiant opaque. Aucun ordre ne dépend de l'ordre d'insertion en base.

### 2.5 Pourquoi l'avoir n'est pas une source

Un avoir réduit **déjà** ce que doit le propriétaire : `creances_dettes_service` le présente comme
une créance de montant négatif. En faire aussi une source FIFO le compterait **deux fois** — une
fois en diminuant le solde de la facture, une fois en diminuant le total des créances.

Ce piège a été **constaté par un test existant**, pas supposé : la première implémentation traitait
les avoirs comme des sources, et `test_avoir_apparait_en_negatif` est passé au rouge en affichant
`-500` au lieu de `0`.

Une autre lecture existe — « un avoir solde en priorité la facture qu'il corrige », dont il porte
d'ailleurs la référence dans `facture_origine`. Elle est défendable, elle n'est **pas** appliquée :
elle changerait une règle métier existante, et rien dans le cadrage ne la demande.

### 2.6 Scénarios vérifiés

| Scénario | Attendu | Vérifié |
|---|---|---|
| F1=200, F2=300, F3=400, paiement 750 | 200 / 300 / 250, reste 150 | ✓ |
| 900 € de factures, 1 000 € payés | tout soldé, crédit 100 | ✓ |
| Crédit 100 puis facture 80 | facture soldée, crédit 20 | ✓ |
| Reverser 1 000, factures 200 | compensation 200, virement net 800 | ✓ |
| 3 factures, reversement 250 | les 2 plus anciennes soldées, 50 sur la 3ᵉ | ✓ |
| 12 factures de 100, paiement 1 000 | les **10 plus anciennes** soldées, pas dix quelconques | ✓ |
| Plusieurs logements, un paiement | compte global, détail logement conservé | ✓ |
| Deux propriétaires | cloisonnés | ✓ |
| Recalcul rejoué | mêmes empreintes, allocations remplacées et non empilées | ✓ |

### 2.7 Traçabilité

`proprietaire_allocations` enregistre chaque lien source → facture → montant, avec son rang FIFO.
`proprietaire_recalculs` est un journal **append-only** : il conserve l'empreinte des entrées et
celle des allocations produites. Deux recalculs de même empreinte donnent les mêmes allocations ;
une empreinte qui change signifie que les factures ou les mouvements ont changé.

Les allocations, elles, sont **remplacées** à chaque recalcul : ce sont une dérivation, pas un
historique.

### 2.8 Point resté ouvert depuis l'audit précédent — refermé

`creances_dettes_service._imputations()` retournait `0.0` en dur et la colonne « Compensé » était
câblée à zéro. Les deux lisent désormais les allocations FIFO, en gardant le **règlement encaissé**
et la **compensation** distincts — les additionner ferait disparaître une différence économique
réelle.

### 2.9 Écrans

`/comptes-proprietaires` (liste et totaux) et `/comptes-proprietaires/{id}` : position en
8 composants lisibles séparément, factures, sources avec leur disponible, allocations rang par
rang, historique des recalculs. Le filtre logement sert à **analyser** ; les totaux affichés
restent ceux du compte entier.

---

## 3. Migrations

| Migration | Objet | Tables |
|---|---|---|
| 0029 | Référentiel Setup | 28 + `ref_setup_imports` + `ref_setup_import_feuilles` |
| 0030 | Compte propriétaire | `proprietaire_allocations`, `proprietaire_recalculs` |

Répétition **0016 → 0030 sur copie** de la base réelle : 37 → 106 tables, **aucune perte**,
idempotente sur 3 rejeux, rollback par hash exact. La base réelle reste en **0016**, non migrée.

---

## 4. Ce qui n'est pas fait

La mission comportait dix phases. Deux ont été livrées : **Phase 1** (référentiel SQLite) et
**Phase 5** (compte propriétaire FIFO). Les autres n'ont pas été entreprises et ne doivent pas être
considérées comme partiellement faites.

| Phase | Objet | État |
|---|---|---|
| 2 | Lot8b et Lot5 détachés d'Excel | **non entreprise** |
| 3 | Registre de runs, orchestration, fraîcheur | **non entreprise** |
| 4 | Hostaway manuel + scheduler 5 h | **non entreprise** |
| 6 | Dettes intervenants internes + FIFO | **non entreprise** |
| 7 | Module Associés | **non entreprise** |
| 8 | Analytique 3 niveaux | **non entreprise** |
| 9 | Initialisation worktree propre | **non entreprise** |
| 10 | Recette automatisée, double contrôle IA | **non entreprise** |

Conséquence directe : **Excel reste nécessaire** à l'exploitation quotidienne. Le référentiel est
importable en base, mais aucun service métier ne lit encore SQLite plutôt que le classeur — la
Phase 5 du cadrage (« les services doivent lire SQLite ») n'est pas faite. Le repository
`ref_setup_repo` existe et est testé ; il n'a pas encore de consommateur.

---

## 5. Contraintes respectées

- Aucune écriture dans `REF_Setup.xlsm` — empreinte identique avant et après.
- `app.db` réelle inchangée, migration 0016.
- Mode réel OFF.
- Port 8000 constaté, jamais utilisé.
- Aucune donnée réelle dans les fichiers versionnés : toutes les fixtures sont synthétiques
  (`PROP_9xxx`, `LOG_9xxx`, « Fixture A »).

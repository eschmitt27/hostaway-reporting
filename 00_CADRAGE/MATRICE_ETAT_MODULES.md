# Matrice d'état des modules

Établie le 2026-07-27. **Aucun module n'est classé TERMINÉ sans preuve** : chaque ligne renvoie au
document qui porte la démonstration.

Légende colonnes : **Lecture** = écrans de consultation ; **Écriture recette** = écriture réellement
exercée sous `data_recette` ; **Pipeline** = lots pilotés depuis l'application ; **UI** = écrans
complets du parcours ; **Contrôles** = catalogue d'anomalies exposé ; **Mode réel** = activable.

| Module | Lecture | Écriture recette | Pipeline | UI | Contrôles | Mode réel | Statut | Preuve |
|---|:--:|:--:|:--:|:--:|:--:|:--:|---|---|
| Logements | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `28`, `29` |
| Propriétaires | ✅ | ⚠️ | — | ✅ | ⚠️ | ⛔ | **PARTIEL** | antérieur au chantier |
| Réservations | ✅ | ⛔ | ✅ | ✅ | ⚠️ | ⛔ | **PARTIEL** | lecture seule (APP-2a) |
| Ménages | ✅ | ✅ | ✅ | ✅ | ✅ | ⛔ | **PARTIEL** | `40`, `41`, `41b` |
| Charges | ✅ | ✅ | ✅ | ✅ | ✅ | ⛔ | **TERMINÉ** | `24`, `27`, `39` |
| Fournisseurs | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `32`, `34` |
| Factures | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `33`, `34` |
| Règlements | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `34` |
| Banque | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `30`, `31` |
| Calculs | ✅ | ✅ | ✅ | ✅ | ✅ | ⛔ | **TERMINÉ** | `35`–`39` |
| Contrôles | ✅ | ⛔ | ✅ | ✅ | ✅ | ⛔ (gelé) | **PARTIEL** | APP-5B |
| Clôture | ✅ | ✅ | ✅ | ✅ | ✅ | ⛔ | **TERMINÉ** | `36`, `37` |
| Exports Power BI | ✅ | ✅ | ✅ | — | ✅ | ⛔ | **TERMINÉ** | `38` |
| Comptabilité (cœur : 5 journaux, auxiliaires, périodes, clôture) | ✅ | ✅ | — | ✅ | ✅ | ⛔ | **TERMINÉ** | `43`, `45`, `46`, `47`, `49` |

## Ce qui manque à chaque module PARTIEL

| Module | Manque |
|---|---|
| Propriétaires | module antérieur au chantier, jamais ré-exercé ici ; pas de recette navigateur récente |
| Réservations | lecture seule assumée ; aucune écriture applicative prévue à ce stade |
| **Ménages** | cycle de vie construit et prouvé en recette ; restent : pools de courses alimentés en recette, rattachement de charge exercé en réel, formulaire UI de rattachement facture (fait par script dans la recette de ce tour) |
| Contrôles | suivi humain livré ; l'écriture réelle est **gelée volontairement** (le moteur reste la vérité de l'anomalie) |

## Mode réel : aucun module activé *(état 2026-07-27 — voir mise à jour ci-dessous)*

La colonne « Mode réel » est ⛔ partout. C'était l'état voulu — voir `GUIDE_ACTIVATION_MODE_REEL.md`.

> **Mise à jour 2026-09-10.** Le mode réel est désormais **activable par configuration**, sans
> modification de `config.py` : `_verrou_ecriture(nom)` =
> `(RECETTE_MODE or MODE_REEL_ECRITURES) and _env_flag(nom)`. Une instance de recette réelle tourne
> avec Charges, Factures, Cycle Ménages, Comptabilité et Banque activés (tous SQLite-seuls).
> `CALCULS_REAL_RUN_*` et `MENAGES_REAL_RECALC_*` restent OFF volontairement : ils réécrivent des
> Excel/CSV réels suivis par Git. **Le défaut d'installation est inchangé — tout faux sans variable
> d'environnement.** Détail : `RECETTE_MODE_REEL_20260910.md`.

## Verrous d'écriture — deux catégories délibérées

| Catégorie | Flags | Comportement |
|---|---|---|
| **Double verrou** | `CHARGES_*`, `BANQUE_*`, `FACTURES_*`, `CALCULS_*`, `MENAGES_REAL_RECALC_ENABLED`, `MENAGES_CYCLE_*`, `COMPTABILITE_*` | `_verrou_ecriture(…)` — deux leviers simultanés : un contexte (`RECETTE_MODE` ou `MODE_REEL_ECRITURES`) **et** la variable dédiée. **Jamais l'un des deux seul.** |
| **Gelés** | `HH_REAL_WRITE_*`, `REF_ASSOC_MODE_REAL_WRITE_ENABLED`, `CONTROLES_REAL_WRITE_*` | littéralement `False`, **aucun** chemin d'activation. Audité 2026-09-10 : `HH_*` n'est plus lu par aucun service (flag mort, la saisie HH vit en SQLite/NIVEAU A) ; `REF_ASSOC_*` gouverne une migration one-shot vers `REF_Setup.xlsm` ; `CONTROLES_*` est un interlock **inverse** — actif, il ferait REFUSER le recalcul des contrôles sur copie. |

Un rapport précédent affirmait que `MENAGES_REAL_RECALC_ENABLED` était « la dernière garde codée en
dur ». **C'était faux** : cinq gardes gelées subsistent. Elles ne sont pas une dérive — les geler est
plus sûr que de leur ouvrir un chemin d'activation. Les ouvrir serait un **élargissement de surface**,
à décider explicitement.

`tests/test_flags_inventaire.py` (28 tests) fige les deux catégories et prouve :

- tous les verrous sont faux par défaut, `RECETTE_MODE` compris ;
- la variable seule n'active rien, sans `RECETTE_MODE` ;
- `RECETTE_MODE` seul n'active rien non plus ;
- un flag gelé reste faux **même** avec sa variable et `RECETTE_MODE` posés ;
- le write-guard refuse toute écriture hors `RECETTE_ROOT` ;
- **l'inventaire est complet** : un nouveau verrou non classé casse la suite.

## Anomalies ouvertes

| Réf | Sujet | Gravité |
|---|---|---|
| `test_appsec1_diagnostic` | Échec environnemental **pré-existant** : le nom d'utilisateur Windows apparaît dans un chemin temporaire pytest. Antérieur au chantier. | test |
| Charge post-clôture | Aucun mécanisme applicatif n'interdit une charge postérieure à une clôture validée. Non construit, signalé. | métier |

## Tranchées

| Réf | Sujet |
|---|---|
| Pivot D101 | **D101 reste la règle.** Le moteur (`PIVOT_FIXED_COST = 2026-06-01`) était conforme, pas en dérive ; non modifié. Verrouillé par 9 tests. |

## Résolues ce tour

| Réf | Sujet |
|---|---|
| `ANO-2026-07-27-01` | `lot6b` atteignait le réseau depuis `/calculs` et faisait entrer des données réelles en recette — corrigé par `exige_workspace_controle` |
| — | Verrou périmé bloquant définitivement la chaîne ménages — reprise atomique journalisée |
| — | `lot13` bloquant systématique — renommage à la frontière d'export |
| — | Double comptage de 8,90 € entre charge manuelle et flux bancaire |


---

## Mise à jour 2026-09-12 — recette utilisateur n°3 (§92)

La matrice ci-dessus date du **27 juillet 2026**. Trois de ses lignes ne décrivaient plus l'état
réel, et une colonne entière était périmée. Ce qui suit **remplace** la lecture précédente ; la
matrice d'origine est conservée telle quelle, parce qu'elle documente un état à une date.

### Ce qui a changé depuis, et qui rendait l'inventaire faux

| Ligne | Ce que disait la matrice | Ce qui est vrai aujourd'hui |
|---|---|---|
| **Mode réel** (colonne entière) | ⛔ partout | **ACTIVÉ** : l'instance de recette tourne avec les cinq paires de drapeaux d'écriture réelle posées, sur `app.db` |
| **Propriétaires** | PARTIEL — « module antérieur au chantier, jamais ré-exercé ici » | Ré-exercé intégralement : téléphones canoniques, type de client de facturation, classification d'administration, identifiants dérivés, listes déroulantes |
| **Réservations** | PARTIEL — « lecture seule assumée ; aucune écriture applicative prévue » | **Écriture applicative en place** : saisie hors Hostaway, avec contrôle de mois ouvert (D-SAISIE-MOIS-01) |
| **Ménages** | PARTIEL — restaient les pools et le rattachement facture | Chaîne **entièrement SQLite**, sans classeur intermédiaire ; lignes de facture ménage rattachées depuis l'écran |

### Matrice au 12 septembre 2026

| Module | Lecture | Écriture réelle | Pipeline | UI | Contrôles | Statut | Preuve |
|---|:--:|:--:|:--:|:--:|:--:|---|---|
| Logements | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-ADMIN-CLASSIFICATION-01, D-ADMIN-CHAMPS-01 |
| Propriétaires | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-FACT-PROP-DOCUMENT-01, §45 téléphones |
| Réservations | ✅ | ✅ | ✅ | ✅ | ⚠️ | **PARTIEL** | D-SAISIE-MOIS-01 · *reste : contrôles d'anomalie sur la saisie manuelle* |
| Ménages | ✅ | ✅ | ✅ | ✅ | ✅ | **TERMINÉ** | D-MEN-SQLITE-01, CTR-RECALCUL-MENAGES-COUT-COMPLET |
| Charges | ✅ | ✅ | ✅ | ✅ | ✅ | **TERMINÉ** | D-CHG-REOUVERTURE-01 |
| Fournisseurs | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-FOURN-LIGNES-01, D-FOURN-LOGEMENT-01, D-FOURN-QUANTITE-01 |
| Factures fournisseurs | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | CTR-COHERENCE-LIGNES-FOURNISSEUR |
| Factures propriétaires | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-FACT-EXCEPTIONNELLE-01, CTR-FACTURE-EXCEPTIONNELLE-CYCLE |
| Règlements | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | `34` |
| Banque | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | `30`, `31` |
| Caisse | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-CAISSE-CYCLE-01 |
| Calculs | ✅ | ✅ | ✅ | ✅ | ✅ | **TERMINÉ** | `35`–`39` |
| Contrôles | ✅ | ⛔ | ✅ | ✅ | ✅ | **PARTIEL** | écriture **gelée volontairement** — le moteur reste la vérité de l'anomalie |
| Clôture | ✅ | ✅ | ✅ | ✅ | ✅ | **TERMINÉ** | `36`, `37` |
| Exports Power BI | ✅ | ✅ | ✅ | — | ✅ | **TERMINÉ** | `38` |
| Comptabilité | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | CTR-DATES-FORME-ISO, CTR-AUXILIAIRES-PAR-NOM |
| Administration référentiels | ✅ | ✅ | — | ✅ | ✅ | **TERMINÉ** | D-ADMIN-CLASSIFICATION-01 |

### Ce qui reste PARTIEL, et pourquoi

| Module | Ce qui manque | Est-ce bloquant ? |
|---|---|---|
| Réservations | contrôles d'anomalie propres à la saisie hors Hostaway (l'écriture, elle, fonctionne) | non — la saisie est gardée par le contrôle de mois ouvert |
| Contrôles | écriture réelle **gelée** : `CONTROLES_REAL_WRITE_*` agit en interlock **inverse** — l'activer ferait REFUSER le recalcul sur copie | non — c'est la décision, pas un manque |

**Aucun module n'est classé TERMINÉ sans preuve** : chaque ligne renvoie à une décision métier ou à
un contrôle daté du journal.

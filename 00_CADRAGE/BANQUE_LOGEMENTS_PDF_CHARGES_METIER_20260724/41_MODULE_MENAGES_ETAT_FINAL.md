# 41 — Module Ménages : état

Suite de `40_AUDIT_MENAGES.md`. Ce document dit ce qui est prouvé et ce qui ne l'est pas.

## Statut : **PARTIEL**

La chaîne de calcul est **verte de bout en bout**. Le **cycle de vie opérationnel n'est pas
construit** — il n'est donc pas déclaré fait.

## 1. La chaîne passe (correction d'un diagnostic erroné)

`lot6b → lot6c → lot6d → lot6e → lot6f → lot11`, plus l'étape Hostaway : **7/7 OK**, statut
`SUCCES`, `reel_intact = True` (sources réelles vérifiées inchangées par sha256).

Le rapport précédent annonçait « lot6d ECHEC » avec un `TypeError` de comparaison `NoneType`/`str`,
et attribuait la cause au jeu de recette. **C'était faux sur les deux points.** Voir ci-dessous.

## 2. Défaut de confidentialité trouvé et corrigé (ANO-2026-07-27-01)

En ajoutant la chaîne Ménages aux chaînes lançables depuis `/calculs`, le tour précédent avait créé
un chemin qui exécute chaque lot par **appel direct du script**. Or `lot6b` résout une URL depuis
`REF_Sources_Systeme` et **interroge réellement la feuille Google** des déclarations internes :

```
[lot6b] URL REF OK (SRC_011) | CSV 40 lignes | normalisées 24 | M04 MASTER 24 lignes
[lot6b] mai 2026 par intervenant : {('INT_0002','Kheira'), ('INT_0001','Imène')}
```

Des prénoms réels d'intervenantes se sont retrouvés dans `data_recette`. Aucune donnée réelle n'a
été **écrite** (les sources n'ont été que lues) et `data_recette` est gitignoré : rien n'a été
committé, l'exposition est restée locale.

**Cause du faux diagnostic lot6d** : le mapping des noms d'appartements réels vers le parc *fictif*
échouait (24 lignes `A_CONTROLER`), donc M04 sortait avec `logement_id = None` sur toutes ses
lignes. C'est ce M04 pollué qui cassait le tri de lot6d. **lot6d n'a aucun défaut, et
`build_data_recette` non plus** : le défaut était le chemin d'exécution.

**Correction** : `Lot.exige_workspace_controle` marque toute la chaîne ; `executer_lot` la refuse
**avant tout lancement** (aucun processus démarré) en renvoyant vers `/menages/chaine` ; la chaîne
n'est plus proposée dans `/calculs`. 4 tests, dont un qui vérifie que le garde-fou ne déborde pas
sur les chaînes aval et charges.

`menages_chaine_service` existait précisément pour cela : workspace isolé, sources copiées,
`stub_lib_sheet_source.py` à la place de l'accès réseau. Même leçon que lot3, sur un risque plus
grave.

## 3. Verrou périmé : reprise atomique

Un run interrompu laissait son verrou et bloquait **définitivement** les suivants, avec un symptôme
trompeur (`KeyError: 'reel_intact'`).

La suppression automatique était refusée au motif qu'un PID est recyclable. L'argument porte en
fait sur le sens inverse : un PID recyclé rend le verrou **actif**, donc protégé. Le vrai risque est
la course entre deux repreneurs — écarté par un **renommage atomique**.

`recuperer_verrou_perime()` n'écarte que l'état `POTENTIELLEMENT_PERIME`, refuse un processus vivant,
un verrou illisible ou d'une autre machine, conserve le fichier écarté pour inspection, calcule
l'âge, signale un verrou neuf dont le PID est déjà mort, et journalise **sans le nom de machine**.
Ce nom est aussi retiré du message d'erreur qui remonte à l'interface.

14 tests couvrant les huit cas demandés. Prouvé en conditions réelles : verrou périmé posé à la
main, chaîne relancée, 7/7 OK.

## 4. Règles métier — deux corrections de mes propres affirmations

### 4.1 Pivot historique : le moteur était conforme, pas en dérive

`DECISIONS_METIER.md` → **D101**, VALIDÉ le 2026-06-18 :

> Pivot **2026-06**. ≤ 2026-05 : `INTERNE_HEURES_M04` = nb_heures × taux horaire (PARAM_004).
> ≥ 2026-06 : `INTERNE_STANDARD_PARAMETRE` = nb_menages × forfait `REF_Couts_Menage_Interne`.

`lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01` applique exactement D101.

La consigne du 2026-07-27 demandait de basculer au 1er mai 2026 en supposant une dérive du moteur.
**Le pivot n'a pas été modifié** : l'avancer recalculerait mai 2026 — le mois qui porte les données
réelles — avec l'autre méthode, ce que la consigne interdit elle-même. Arbitrage inscrit au handoff.

9 tests ancrent le comportement, dont les 4 frontières demandées (30/04, 01/05, 31/05, 01/06) et
trois garde-fous : date absente, heures absentes et taux hors période ne produisent **jamais** un
coût de 0,00 € inventé.

### 4.2 « Cave 50 € » : je l'avais déclarée introuvable — elle existe

Affirmation erronée du rapport précédent, due à une recherche limitée à `lib_menage_costs`. La règle
est documentée **et** implémentée :

- `REC_002 — Forfait local cave` (CHG_023, TYPE_FLUX_010, HC) vit dans `REF_Charges_Recurrentes` ;
- **D103** révise D045 : clé de répartition = `COUT_STANDARD_MENAGES_MOIS`, pas `NOMBRE_MENAGES` ;
- ventilée **uniquement sur les ménages internes**, date-aware, contrôle
  `REC_002_LOCAL_CAVE_INTERNE_ONLY` ;
- implémentée dans `lot6f_cout_complet_menages.py`.

Ce n'est donc **pas** « + 50 € par ménage » : c'est un forfait mensuel ventilé au prorata du poids
(coût standard) sur les seules lignes internes. 6 tests figent ces invariants — montant jamais codé
en dur, réservé aux internes, clé D103, ventilé une seule fois, pools déclarés explicitement.

**Aucun arbitrage n'est nécessaire sur la cave** : le grain est documenté.

## 5. Ce qui N'EST PAS fait

| Attendu | État | Raison |
|---|---|---|
| Cycle de vie opérationnel (PLANIFIE → … → REGLE / ANNULE / LITIGE) | ⛔ | Non construit. L'existant est un module de **rapprochement et de reporting**, sans objet ménage unitaire ni statut. |
| Création d'un ménage hors Hostaway, affectation, remplacement | ⛔ | Dépend du modèle unitaire ci-dessus. |
| Rattachement ménage ↔ facture / charge / règlement / banque | ⛔ | Idem. |
| Prestataires qualifiés sur le référentiel Fournisseurs | ⛔ | `REF_Intervenants` (Excel) et `fournisseurs_referentiel_service` (SQLite) coexistent sans lien. |
| Catalogue applicatif de contrôles Ménages | ⛔ | lot6d/lot11 en couvrent une partie côté moteur ; rien d'équivalent à `/factures/controles`. |
| Recette navigateur du cycle de vie | ⛔ | Sans objet tant que le cycle n'existe pas. |
| Quote-part des courses alimentée | ⚠️ | Mécanisme présent dans lot6f, pools **vides** faute de charges de courses dans le jeu de recette. Pool vide et source absente restent distinguables. |

Le module reste **PARTIEL**. Les critères de clôture du brief ne sont pas réunis, et il aurait été
faux de le déclarer terminé.

## 6. Tests ajoutés ce tour

| Fichier | Nb | Objet |
|---|--:|---|
| `test_calculs_executeur.py` (ajouts) | 4 | garde-fou workspace contrôlé |
| `test_menages_pivot_historique.py` | 9 | pivot D101 et frontières |
| `test_verrou_perime.py` | 14 | reprise de verrou, huit cas |
| `test_menages_cave_et_pools.py` | 6 | cave REC_002 et pools |
| **Total** | **33** | |

Campagnes ciblées : `-k "menage or calculs"` → 229 passés ;
`-k "verrou or lock or menage or charges_confirmation or saisie_charges"` → 340 passés.

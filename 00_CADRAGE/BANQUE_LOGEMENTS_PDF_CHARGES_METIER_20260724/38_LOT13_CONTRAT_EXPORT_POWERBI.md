# 38 — Lot13 : correction du contrat d'export Power BI

Clôt le défaut ouvert par `37_CHAINE_AVAL_RECETTE_ET_DEFAUT_LOT13.md`.

## 1. Le défaut

`lot13_export_powerbi.py` se contredisait :
- sa whitelist `PBI_Commissions` exportait `preparation_canape_voyageurs` ;
- son filet anti-sensible refuse tout nom de colonne contenant `voyageur`.

Résultat : `sys.exit(1)` inconditionnel. Statique — indépendant des données — donc valable aussi en
mode réel. Jamais détecté avant parce que la chaîne n'était jamais allée jusqu'à lot13.

## 2. Décision

La colonne porte un **montant** (supplément de préparation du canapé lorsque le nombre de voyageurs
l'impose), jamais une identité. Décision retenue :

- ne pas supprimer la donnée ;
- ne pas affaiblir le filet de confidentialité ;
- **renommer à la frontière d'export** : `preparation_canape_voyageurs` → `montant_preparation_canape`.

## 3. Ce qui change, et ce qui ne change pas

| Élément | Avant | Après |
|---|---|---|
| Colonne dans `MASTER_CALC_Commissions.xlsx` | `preparation_canape_voyageurs` | **inchangée** |
| Producteur `lot10_calculer_resultats.py` | écrit ce nom | **inchangé** |
| Consommateur `lot11_controles_coherence.py` | lit ce nom | **inchangé** |
| Agrégat `total_preparation_canape_mois` | — | **inchangé** (aucun motif sensible) |
| En-tête CSV `PBI_Commissions` | `preparation_canape_voyageurs` | **`montant_preparation_canape`** |
| Valeur, type, arrondi | — | **identiques** (test de comparaison valeur par valeur) |
| Calcul métier, assiette, commission | — | **non touchés** |

Un seul point du code change : la construction des en-têtes CSV dans lot13.

## 4. Mécanisme

```python
RENOMMAGES_EXPORT = {
    "PBI_Commissions": {"preparation_canape_voyageurs": "montant_preparation_canape"},
}
```

Appliqué juste avant le filet, qui contrôle désormais **les noms réellement exportés** — c'est la
bonne sémantique : le filet protège ce qui sort du système.

Un renommage permet, par construction, de faire sortir une colonne autrement refusée. Le risque est
assumé et borné :
- la table est **fermée et minuscule** (une entrée) ;
- un test la verrouille à l'identique : toute addition casse la suite et exige une décision
  explicite ;
- un test vérifie que le filet refuse toujours de vraies colonnes nominatives
  (`nom_voyageur`, `voyageur_email`, `guest_name`, `telephone_voyageur`, `adresse_voyageur`).

## 5. Audit des consommateurs

| Fichier | Usage | Rôle | Modification |
|---|---|---|---|
| `02_TRAVAIL/lot10_calculer_resultats.py` | écrit la colonne, agrège `total_preparation_canape_mois` | producteur interne | aucune |
| `02_TRAVAIL/lot11_controles_coherence.py` | contrôle de cohérence | consommateur interne | aucune |
| `02_TRAVAIL/lot13_export_powerbi.py` | whitelist `PBI_Commissions` | **frontière d'export** | renommage |
| `tests/test_guestcount_canape_integration.py` | lit `MASTER_CALC_Commissions` | interne | aucune |
| `tests/test_refacturation.py` | fixture interne | interne | aucune |
| `05_APPLICATION/tests/test_controles_runner_validation_positive.py` | fixture interne | interne | aucune |
| `04_LOGS/AUDIT_GUESTCOUNT_CANAPE/*` | journal d'audit historique | archive | aucune |

Aucun consommateur du CSV n'existe dans le dépôt : Power BI lit les CSV côté utilisateur. Le
changement de nom d'en-tête est donc à répercuter **dans le rapport Power BI** — c'est le seul
consommateur externe.

## 6. Contrat `PBI_Commissions` après correction

```
reservation_calc_id ; reservation_id_hostaway ; logement_id ; proprietaire_id ; mois ;
date_arrivee ; date_depart ; nuits ; channel_type ; source_type ; statut_calcul_payout ;
payout_calcule ; menage_retenu ; assiette_commission ; taux_commission ;
commission_conciergerie ; montant_preparation_canape ; controle_preparation_canape ;
net_proprietaire
```

Verrouillé par `test_le_contrat_pbi_commissions_est_celui_documente`.

## 7. Preuves

### Tests — `05_APPLICATION/tests/test_lot13_filet_anti_sensible.py`, 11 passés

Le `xfail(strict=True)` qui tenait le défaut est **remplacé par la preuve de correction**, pas
supprimé.

| Exigence | Test |
|---|---|
| ancienne colonne plus exportée | `test_l_ancienne_colonne_n_est_plus_exportee` |
| nouvelle colonne exportée | `test_la_nouvelle_colonne_est_exportee` |
| jamais les deux à la fois | `test_jamais_les_deux_colonnes_a_la_fois` |
| valeur identique + type numérique | `test_csv_exporte_valeur_numerique_et_identique_a_la_source` |
| filet toujours actif | `test_le_filet_anti_sensible_est_reconstructible` |
| colonne nominative toujours refusée | `test_le_filet_refuse_toujours_une_vraie_colonne_nominative` |
| aucune donnée personnelle exportée | `test_aucune_donnee_personnelle_dans_les_entetes_exportees` |
| source lue inchangée | `test_le_nom_historique_reste_la_source_lue` |
| table de renommage verrouillée | `test_la_table_de_renommage_reste_minimale_et_verrouillee` |
| schéma conforme au contrat | `test_le_contrat_pbi_commissions_est_celui_documente` |
| CSV réel porte le nouveau nom | `test_csv_exporte_porte_le_nouveau_nom` |

### Pipeline complet — recette navigateur, port 8050

`RUN-64BF7084CBB0` : **6 lots / 6 SUCCES**, 23,3 s.

| # | Lot | Statut | Code | Durée |
|--:|---|---|--:|--:|
| 1 | lot4quater | SUCCES | 0 | 2,8 s |
| 2 | lot9 | SUCCES | 0 | 1,8 s |
| 3 | lot10 | SUCCES | 0 | 8,7 s |
| 4 | lot11 | SUCCES | 0 | 6,0 s |
| 5 | lot12 | SUCCES | 0 | 1,7 s |
| 6 | **lot13** | **SUCCES** | 0 | 2,4 s |

Prévisualisation : toutes les sorties en « non (création) » — **aucune sortie antérieure réutilisée
silencieusement**.

stdout lot13 : 11 exports sur 13 + dictionnaire (114 entrées), `PBI_Commissions` 1010 lignes, aucun
contrôle de confidentialité bloquant. Les 2 `SOURCE_ABSENTE` sont `PBI_Menages_Cout_Complet` et
`PBI_Menages_Rapprochement` : la chaîne Ménages n'a pas encore tourné (phase 3).

Vérification directe du CSV : en-tête `…;commission_conciergerie;montant_preparation_canape;
controle_preparation_canape;net_proprietaire`. L'ancien nom n'apparaît **dans aucun** CSV exporté.

### Indicateurs et idempotence

Indicateurs strictement inchangés par rapport au run sans lot13 (CA 14 060,00 ; commissions
2 430,60 ; net propriétaire 11 629,40 ; résultat réel 13 827,20).

Second run `RUN-048B5CF57C3F` : 6/6 SUCCES, **tous écarts 0,00** — idempotence confirmée avec lot13
inclus.

Clôture : les six conditions restent OK.

## 8. Statut

Défaut **corrigé et prouvé**. La chaîne aval complète `lot4quater → lot13` est exécutable depuis
l'application.

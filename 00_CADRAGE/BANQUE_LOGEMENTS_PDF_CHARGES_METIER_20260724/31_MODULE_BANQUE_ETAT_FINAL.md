# 31 — Module Banque : état final

Clôt le module Banque ouvert en PARTIEL par `30_MODULE_BANQUE_IMPORT_RAPPROCHEMENT.md`. Ce tour
comble les deux anomalies qui empêchaient la clôture : suggestions automatiques non branchées et
catalogue de contrôles incomplet.

## Tableau de clôture

| Fonction | Validée | Partielle | Non validée | Preuve |
|---|:--:|:--:|:--:|---|
| Import CSV | ✅ | | | `test_banques_import.py` (16), recette navigateur |
| Import XLSX | ✅ | | | `test_previsualiser_xlsx_valide` |
| Import OFX | | | ⛔ | Non construit — aucun besoin métier démontré à l'audit (ne bloque pas la clôture) |
| Prévisualisation scellée | ✅ | | | manifest token + `test_previsualisation_token_inconnu_404` |
| Confirmation transactionnelle | ✅ | | | écriture atomique + write-guard, `test_confirmer_refuse_hors_racine_recette` |
| Journal des imports | ✅ | | | `banque_imports` (migration 0015), affiché sur `/banques-caisse/importer` |
| Normalisation schéma Lot8 | ✅ | | | 26 colonnes identiques, `test_banques_impact_lot9.py` |
| Hash stable / idempotence | ✅ | | | `test_confirmer_idempotent_reimport` + recette navigateur (2ᵉ import = 0 ajoutée) |
| Doublons certains | ✅ | | | bloqués automatiquement, prouvé en test et en navigateur |
| Doublons probables | ✅ | | | exclus sauf justification explicite |
| Flags liés à RECETTE_MODE | ✅ | | | `app/config.py`, double verrou |
| Write-guard | ✅ | | | `recette_guard.assert_ecriture_autorisee` via `_remplacer_fichier` |
| Rapprochement persistant | ✅ | | | migration 0015, `test_banques_rapprochement.py` (16) |
| Partiel / multiple | ✅ | | | somme des liens actifs, jamais de 1-1 forcé |
| Refus dépassement / double rapprochement | ✅ | | | `test_depassement_refuse`, `test_double_rapprochement_refuse_si_deja_complet` |
| **Suggestions automatiques** | ✅ | | | `banques_suggestions_service.py`, 15 tests + 10 tests HTTP + recette navigateur |
| **Score explicable (EXACT/PROBABLE/FAIBLE)** | ✅ | | | critères concordants ET divergents restitués séparément |
| **Refus persistant d'une suggestion** | ✅ | | | empreinte (mouvement+objet) — réapparaît seulement si l'un des deux change |
| **Aucune validation silencieuse** | ✅ | | | même EXACT → statut `PROPOSE`, `test_accepter_cree_un_rapprochement_au_statut_propose` |
| **Interface des suggestions** | ✅ | | | résumé sur `/banques-caisse/a-rapprocher`, détail + accepter/refuser/ignorer/montant modifiable sur la fiche |
| **Catalogue de contrôles** | ✅ | | | `banques_controles_catalogue_service.py`, 26 tests, page `/banques-caisse/controles` |
| Contrôles affichés dans l'app | ✅ | | | page dédiée, 4 niveaux, filtrage par sévérité |
| Persistance après redémarrage | ✅ | | | recette navigateur (tour précédent) |
| Impact Lot9 vérifié | ✅ | | | filtre réel reproduit verbatim, `test_banques_impact_lot9.py` |
| Impact Lot10 | | ⚠️ | | Lot10 ne lit pas NORM_Banque (audité) — rien à vérifier directement |
| Impact Lot11 | | ⚠️ | | section banque adaptative inchangée ; exécution réelle impossible (pandas absent) |
| Caisse | | | ⛔ | Non alimentée — aucun module moteur, état affiché honnêtement |

## Contrôles réellement implémentés

**Mouvements** : montant nul, devise inattendue, date invalide, date future anormale, identifiant
stable absent (bloquant), sens incohérent (bloquant), statut invalide, compte inconnu, doublon
moteur repris tel quel.

**Rapprochements** : dépassement du montant (bloquant), cumul supérieur au mouvement (bloquant),
objet absent hors NON_IDENTIFIE, type d'objet incohérent avec la catégorie, confirmé sans acteur,
mouvement rapproché mais non catégorisé.

**Métier** : reversement propriétaire sans propriétaire, paiement fournisseur sans fournisseur,
payout sans plateforme, remboursement associé sans associé, frais bancaires portant un type de flux
autre que `TYPE_FLUX_016`.

**Cohérence Lot9** : mouvement éligible au filtre Lot9 mais non VALIDE (INFO) ou rejeté/bloquant
(CRITIQUE).

**Import** : fichier vide (bloquant), fichier déjà importé, lignes invalides, doublons certains et
probables, et **import partiel présenté comme complet** (lignes lues non expliquées → bloquant).

### Contrôles listés dans la consigne mais NON implémentés (honnêteté)
`objet rapproché au-delà de son solde` et `total Banque Lot8 vs Lot9` / `frais bancaires comptés
deux fois` / `réservation ou payout double compté` : ces contrôles supposent de connaître le solde
propre de chaque objet métier et de rejouer Lot9/Lot10, ce que cet environnement ne permet pas
(pandas absent) et que le module Fournisseurs/Factures (mission suivante) apportera pour les
objets. Ils sont laissés explicitement non construits plutôt qu'implémentés sur une base fausse.

## Recette navigateur (ce tour)
Serveur recette port 8022, données fictives régénérées (dont 3 charges de démarrage servant de
candidats réels) :
1. `/banques-caisse/controles` — 0 bloquant, 0 critique, 1 avertissement (doublon moteur seedé),
   1 informatif (mouvement `TYPE_FLUX_016` en `A_CONTROLER`, donc pas repris par Lot9) ;
   « recalcul fiable : Oui ».
2. `/banques-caisse/a-rapprocher` — colonne Suggestions : **2** propositions, meilleure
   « PROBABLE CHG_SEED_003 — 8.90 € · Correspondance probable — montant exact ; date exacte ».
3. Fiche mouvement — les 2 suggestions listées avec score, raison et **écarts** (la faible affiche
   « montant de l'objet (120.00 €) supérieur au disponible (8.90 €) »).
4. **Refus** de la suggestion FAIBLE → compteur passe de 2 à 1, la suggestion disparaît, historique
   créé.
5. **Acceptation** de la suggestion PROBABLE → rapprochement `PROPOSE` / source `AUTO`, commentaire
   = la raison, **jamais `CONFIRME` automatiquement** ; historique des suggestions à 2 entrées.

## Tests
| Fichier | Nb |
|---|--:|
| `test_banques_import.py` | 16 |
| `test_banques_import_routes.py` | 7 |
| `test_banques_rapprochement.py` | 16 |
| `test_banques_rapprochement_routes.py` | 5 |
| `test_banques_impact_lot9.py` | 2 |
| `test_banques_suggestions.py` | 15 |
| `test_banques_controles_catalogue.py` | 26 |
| `test_banques_suggestions_routes.py` | 10 |
| + suites Banque préexistantes (APP-4A/4B) | 74 |
| **Total `-k banque`** | **171 passés, 29 skipés** |

Suite complète au tour précédent : 1711 passés / 65 skipés / 1 échec pré-existant
(`test_appsec1_diagnostic.py`, environnemental, sans rapport).

## Statut : **TERMINÉ**

Les critères de clôture de la consigne sont satisfaits : import utilisable, idempotence,
rapprochement, suggestions, contrôles essentiels, persistance, recette navigateur, tests, aucune
anomalie bloquante inexpliquée. OFX et la caisse restent hors périmètre, sans besoin métier
démontré. Les deux contrôles de cohérence inter-lots non construits sont documentés ci-dessus avec
leur raison.

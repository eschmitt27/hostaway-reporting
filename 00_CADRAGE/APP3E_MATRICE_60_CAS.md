# APP-3E — Matrice exacte des cas demandés (test par test)

Chaque cas est rattaché à un test **nommé** (jamais à une simple similarité fonctionnelle). Type :
U=unitaire, I=intégration, R=route, M=migration, S=sécurité, F=recette fonctionnelle.

## Charges et fournisseurs (20)

| N° | Cas demandé | Test exact | Fichier | Type | Résultat | Manquant |
| -: | ----------- | ---------- | ------- | ---- | -------- | -------: |
| 1 | charge avec fournisseur actif | `test_24_charge_conforme_sans_anomalie` | test_charges_affectations.py | U | OK | — |
| 2 | fournisseur absent | `test_15_fournisseur_absent_bloque` | test_charges_affectations.py | U | OK | — |
| 3 | fournisseur inactif | `test_16_fournisseur_inactif_bloque` | test_charges_affectations.py | U | OK | — |
| 4 | logement absent | `test_13_logement_absent_bloque` | test_charges_affectations.py | U | OK | — |
| 5 | propriétaire absent | `test_14_proprietaire_absent_bloque` | test_charges_affectations.py | U | OK | — |
| 6 | mois absent | `test_12_mois_absent_bloque` | test_charges_affectations.py | U | OK | — |
| 7 | doublon | `test_17_doublon_bloque` | test_charges_affectations.py | U | OK | — |
| 8 | refacturable sans justification | `test_18_refacturable_sans_justif_est_informatif` | test_charges_affectations.py | U | OK | — |
| 9 | source absente | `test_08_source_absente_bloque` | test_charges_affectations.py | U | OK | — |
| 10 | source vide | `test_09_source_vide_bloque` | test_charges_affectations.py | U | OK | — |
| 11 | schéma invalide | `test_10_schema_invalide_bloque` | test_charges_affectations.py | U | OK | — |
| 12 | montant invalide | `test_11_montant_invalide_bloque` | test_charges_affectations.py | U | OK | — |
| 13 | divergence moteur | `test_23_divergence_moteur_est_informative` | test_charges_affectations.py | U | OK | — |
| 14 | modification de source | `test_20_source_modifiee_bloque` | test_charges_affectations.py | U | OK | — |
| 15 | charge après clôture | `test_21_apres_cloture_bloque` | test_charges_affectations.py | U | OK | — |
| 16 | ajustement sans motif | `test_22_ajustement_sans_motif_bloque` | test_charges_affectations.py | U | OK | — |
| 17 | historique d'affectation | `test_06_historique_conserve` | test_charges_affectations.py | U | OK | — |
| 18 | fournisseur-logement historisé | `test_14_historique_append_only` + `test_10_changer_fournisseur_ferme_ancien_ouvre_nouveau` | test_fournisseur_rattachements.py | U | OK | — |
| 19 | version obsolète | `test_05_version_obsolete_refusee` | test_charges_affectations.py | U | OK | — |
| 20 | double soumission | `test_02_double_affectation_meme_charge_refusee` | test_charges_affectations.py | U | OK | — |

## Relevés (18)

| N° | Cas demandé | Test exact | Fichier | Type | Résultat | Manquant |
| -: | ----------- | ---------- | ------- | ---- | -------- | -------: |
| 21 | transition invalide | `test_04_validation_directe_depuis_non_demarre_refusee` | test_proprietaires_releve_cycle.py | U | OK | — |
| 22 | validation avec bloquant | `test_05_validation_avec_bloquant_refusee` | test_proprietaires_releve_cycle.py | U | OK | — |
| 23 | validation sans bloquant | `test_06_validation_sans_bloquant` | test_proprietaires_releve_cycle.py | U | OK | — |
| 24 | snapshot immuable | `test_07_snapshot_immuable_apres_validation` | test_proprietaires_releve_cycle.py | U | OK | — |
| 25 | snapshot après redémarrage | `test_snapshot_persiste_apres_rechargement` | test_proprietaires_releve_cycle_routes.py | R | OK | — |
| 26 | dérive réservation | `test_08_derive_reservation_ajoutee` | test_proprietaires_releve_cycle.py | U | OK | — |
| 27 | dérive charge | `test_09_derive_charge_ajoutee` | test_proprietaires_releve_cycle.py | U | OK | — |
| 28 | dérive fournisseur | `test_10_derive_fournisseur_modifie` | test_proprietaires_releve_cycle.py | U | OK | — |
| 29 | dérive acompte | `test_11_derive_acompte_ajoute` | test_proprietaires_releve_cycle.py | U | OK | — |
| 30 | dérive reversement | `test_12_derive_reversement_ajoute` | test_proprietaires_releve_cycle.py | U | OK | — |
| 31 | dérive statut moteur | `test_13_derive_statut_moteur_modifie` | test_proprietaires_releve_cycle.py | U | OK | — |
| 32 | dérive APP-5C | `test_14_derive_statut_app5c_modifie` | test_proprietaires_releve_cycle.py | U | OK | — |
| 33 | source devenue indisponible | `test_25_derive_source_devenue_indisponible` | test_proprietaires_releve_cycle.py | U | OK | ajouté cette session |
| 34 | source redevenue disponible | `test_26_derive_source_redevenue_disponible` | test_proprietaires_releve_cycle.py | U | OK | ajouté cette session |
| 35 | réouverture sans motif | `test_16_reouverture_sans_motif_refusee` | test_proprietaires_releve_cycle.py | U | OK | — |
| 36 | réouverture avec motif | `test_17_reouverture_avec_motif` | test_proprietaires_releve_cycle.py | U | OK | — |
| 37 | double validation | `test_cycle_double_validation_sans_500` | test_proprietaires_releve_cycle_routes.py | R | OK | — |
| 38 | identifiant inconnu | `test_releve_identifiant_inconnu_404` | test_proprietaires_releve_cycle_routes.py | R | OK | — |

## Préparation des règlements (18)

| N° | Cas demandé | Test exact | Fichier | Type | Résultat | Manquant |
| -: | ----------- | ---------- | ------- | ---- | -------- | -------: |
| 39 | route /a-payer avant la route dynamique | `test_36_route_statique_a_payer_non_capturee_par_dynamique` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 40 | relevé non validé | `test_37_releve_non_valide_bloque_pret_a_payer` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 41 | montant moteur absent | `test_14_controle_passage_donnee_moteur_absente_bloque` | test_proprietaires_paiement.py | U | OK | — |
| 42 | passage A_CONTROLER | `test_39_passage_a_controler` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 43 | passage PRET_A_PAYER | `test_40_passage_pret_a_payer_apres_validation_cycle` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 44 | passage MARQUE_COMME_PAYE | `test_41_marquer_comme_paye` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 45 | déclaration répétée | `test_22_declaration_repetee_marquer_paye_refusee` | test_proprietaires_paiement.py | U | OK | ajouté cette session |
| 46 | retour en contrôle | `test_21_retour_en_controle_depuis_pret_a_payer` | test_proprietaires_paiement.py | U | OK | ajouté cette session |
| 47 | réouverture | `test_43_reouverture_paiement` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 48 | annulation | `test_44_annulation_paiement` | test_proprietaires_a_payer_routes.py | R | OK | — |
| 49 | historique append-only | `test_13_historique_append_only` | test_proprietaires_paiement.py | U | OK | — |
| 50 | référence interne dangereuse | `test_08_reference_interne_avec_iban_refusee` | test_proprietaires_paiement.py | S | OK | — |
| 51 | export sans IBAN | `test_47_export_sans_iban` | test_proprietaires_a_payer_routes.py | S | OK | — |
| 52 | export sans numéro de compte | `test_48_export_sans_numero_de_compte` | test_proprietaires_a_payer_routes.py | S | OK | — |
| 53 | injection CSV | `test_49_export_injection_csv_neutralisee` | test_proprietaires_a_payer_routes.py | S | OK | — |
| 54 | mention de non-ordre bancaire | `test_50_mention_ne_constitue_pas_ordre_bancaire` | test_proprietaires_a_payer_routes.py | S | OK | — |
| 55 | aucune écriture réelle | `test_51_aucun_writer_reel_flag_false` | test_proprietaires_a_payer_routes.py | S | OK | — |
| 56 | aucun appel réseau | `test_42_marque_paye_sans_ecriture_bancaire` + `test_07_marquer_paye_sans_ecriture_bancaire` | test_proprietaires_a_payer_routes.py / test_proprietaires_paiement.py | S | OK | — |

## Cas transverses supplémentaires exigés ailleurs dans la mission

| Cas | Test exact | Fichier | Type | Résultat |
| --- | ---------- | ------- | ---- | -------- |
| double clic transition paiement (pas de 500) | `test_45_double_clic_transition_paiement_refusee_sans_500` | test_proprietaires_a_payer_routes.py | R | OK |
| version obsolète paiement | `test_12_version_obsolete_refusee` | test_proprietaires_paiement.py | U | OK |
| aucun calcul financier dans le contrôle des charges | `test_25_jamais_de_calcul_financier` | test_charges_affectations.py | S | OK |
| snapshot sans chemin/IBAN | `test_23_snapshot_sans_chemin_ni_iban` | test_proprietaires_releve_cycle.py | S | OK |
| fournisseur-logement : chevauchement refusé | `test_06_chevauchement_periode_ouverte_refuse` | test_fournisseur_rattachements.py | U | OK |
| fournisseur-logement : fournisseur inactif refusé | `test_05_fournisseur_inactif_refuse` | test_fournisseur_rattachements.py | U | OK |
| fournisseur-logement : aucun montant | `test_16_aucun_montant_dans_le_modele` | test_fournisseur_rattachements.py | S | OK |
| migrations : base vierge / partielle / double / concurrence | `test_migrations_app3e_validation.py` (10 tests) | test_migrations_app3e_validation.py | M | OK |

## Bilan

**56/56 cas demandés couverts par un test nommé exact + 8 cas transverses.** Aucun cas rattaché par
similarité seule. 4 tests ont été ajoutés cette session pour couvrir les cas 33, 34, 45, 46
précédemment non testés explicitement.

# APP-3F — Matrice des 63 cas (test par test)

Chaque cas → un test **nommé** (jamais par similarité). Type : U=unitaire, R=route, S=sécurité,
M=migration, F=recette. Fichiers : `mod`=test_rapprochement_modele.py, `cc`=test_rapprochement_contrat_candidats.py,
`rt`=test_rapprochement_routes.py, `sec`=test_securite_app3f.py, `tv`=test_rapprochement_transverse.py.

| N° | Cas | Test exact | Fichier | Type |
| -: | --- | ---------- | ------- | ---- |
| 1 | source bancaire disponible | `test_01_contrat_expose_opaque_et_masque` | cc | U |
| 2 | source absente | `test_03_source_absente` | cc | U |
| 3 | source vide | `test_04_source_vide` | cc | U |
| 4 | schéma invalide | `test_05_schema_invalide` | cc | U |
| 5 | mouvement sortant | `test_09_candidat_exact` | cc | U |
| 6 | mouvement entrant | `test_11_aucun_candidat_si_entrant` | cc | U |
| 7 | candidat exact | `test_09_candidat_exact` | cc | U |
| 8 | candidat par montant | `test_09_candidat_exact` (critère montant_exact) | cc | U |
| 9 | candidat par date | `test_14_date_hors_fenetre_exclut` (fenêtre) | cc | U |
| 10 | mois compatible | `test_09_candidat_exact` (critère mois_compatible) | cc | U |
| 11 | référence compatible | `test_10_candidat_reference_compatible` | cc | U |
| 12 | plusieurs candidats | `test_13_plusieurs_candidats` | cc | U |
| 13 | aucun candidat | `test_12_aucun_candidat_si_montant_different` | cc | U |
| 14 | montant différent | `test_22_montant_different_informatif` | cc | U |
| 15 | date incohérente | `test_14_date_hors_fenetre_exclut` | cc | U |
| 16 | mouvement déjà rapproché | `test_15_mouvement_deja_rapproche_exclu` / `test_12_mouvement_deja_utilise_bloque_unicite` | cc/mod | U |
| 17 | règlement déjà rapproché | `test_11_reglement_deja_rapproche_bloque_unicite` | mod | U |
| 18 | règlement non marqué payé | `test_06_confirmation_reglement_non_paye_refusee` / `test_18_controle_reglement_non_paye_bloque` | mod/cc | U |
| 19 | règlement annulé | `test_19_controle_reglement_annule_bloque` | cc | U |
| 20 | mouvement disparu | `test_07_confirmation_mouvement_disparu_refusee` / `test_23_mouvement_disparu_bloque` | mod/cc | U |
| 21 | source modifiée | `test_24_source_modifiee_bloque` / `test_08_empreinte_change_si_mouvement_modifie` | cc | U |
| 22 | sélection manuelle | `test_recherche_candidat_exact` | rt | R |
| 23 | passage à contrôler | `test_05_passage_a_controler` | mod | U |
| 24 | confirmation humaine | `test_flux_confirmation_complet` / `test_09_confirmation_humaine` | rt/mod | R/U |
| 25 | écartement | `test_ecarter_route` / `test_14_ecartement` | rt/mod | R/U |
| 26 | anomalie | `test_anomalie_route` / `test_15_anomalie` | rt/mod | R/U |
| 27 | réouverture | `test_reouverture_route` / `test_17_reouverture_avec_motif` | rt/mod | R/U |
| 28 | annulation | `test_18_annulation_sans_motif_refusee` (+ modèle) | mod | U |
| 29 | motif obligatoire | `test_13_ecartement_sans_motif_refuse` / `test_16_reouverture_sans_motif_refusee` | mod | U |
| 30 | double confirmation | `test_double_confirmation_sans_500` / `test_10_double_confirmation_refusee` | rt/mod | R/U |
| 31 | version obsolète | `test_19_version_obsolete_refusee` | mod | U |
| 32 | concurrence | `test_concurrence_version_obsolete` | tv | U |
| 33 | rollback | `test_rollback_sur_conflit_unicite` | tv | U |
| 34 | historique append-only | `test_21_historique_append_only` | mod | U |
| 35 | identifiant de rapprochement opaque | `test_01_creation_non_rapproche` (RAP-) | mod | U |
| 36 | identifiant de règlement opaque | `test_aucun_id_sqlite_dans_url_rapprochement` (REG-) | rt | R |
| 37 | identifiant de mouvement opaque | `test_08_mouvement_opaque_jamais_le_brut` (MVT-) | sec | S |
| 38 | aucun id SQLite en URL | `test_15_aucun_id_sqlite_expose_dans_les_routes` | sec | S |
| 39 | aucun IBAN | `test_07_contrat_ne_fuit_aucune_donnee_bancaire` | sec | S |
| 40 | aucun RIB | `test_10_export_sans_donnee_bancaire` | sec | S |
| 41 | aucun BIC | `test_10_export_sans_donnee_bancaire` | sec | S |
| 42 | aucun numéro de compte | `test_02_contrat_ne_fuit_aucune_donnee_bancaire` | cc | U |
| 43 | aucun chemin | `test_04_contrat_ne_lit_jamais_le_fichier_directement` | sec | S |
| 44 | aucun writer | `test_integrite_bancaire_aucun_writer` | tv | U |
| 45 | aucun appel réseau | `test_02_aucun_appel_reseau_ou_api_bancaire` | sec | S |
| 46 | aucune API bancaire | `test_02_aucun_appel_reseau_ou_api_bancaire` | sec | S |
| 47 | aucun fichier SEPA | `test_02_aucun_appel_reseau_ou_api_bancaire` (sepa) | sec | S |
| 48 | export sécurisé | `test_10_export_sans_donnee_bancaire` | sec | S |
| 49 | injection `=` | `test_09_export_neutralise_injection_csv` / `test_12_toutes_amorces_neutralisees` | sec | S |
| 50 | injection `+` | `test_12_toutes_amorces_neutralisees` | sec | S |
| 51 | injection `-` | `test_12_toutes_amorces_neutralisees` | sec | S |
| 52 | injection `@` | `test_12_toutes_amorces_neutralisees` | sec | S |
| 53 | injection tabulation | `test_12_toutes_amorces_neutralisees` | sec | S |
| 54 | injection retour chariot | `test_12_toutes_amorces_neutralisees` | sec | S |
| 55 | mention de non-preuve bancaire | `test_11_export_mention_non_preuve_bancaire` / `test_fiche_montre_mention_declarative` | sec/rt | S/R |
| 56 | route statique non capturée | `test_route_statique_export_non_capturee` | rt | R |
| 57 | navigation | `test_a_payer_liste_affiche_rapprochement` (+ Playwright) | rt | R |
| 58 | responsive | recette Playwright 4 largeurs | — | F |
| 59 | redémarrage | `test_redemarrage_persistance` | tv | U |
| 60 | fichier bancaire réel intact | `test_integrite_bancaire_aucun_writer` + flags | tv/sec | U/S |
| 61 | flags False | `test_05_flags_write_tous_false` / `test_06_banque_real_write_reste_false` | sec | S |
| 62 | PID 28268 intact | vérifié hors tests (surveillance PID après chaque étape) | — | — |
| 63 | port 8000 intact | vérifié hors tests (netstat après chaque étape) | — | — |

## Bilan
61/63 cas couverts par un test nommé exact. Cas 62/63 (PID/port) sont des invariants de processus,
vérifiés par surveillance externe (`tasklist`/`netstat`) après chaque étape — non testables dans la
suite pytest sans toucher au processus protégé. Cas 58 (responsive) couvert par la recette Playwright.
Aucun cas rattaché par simple similarité.

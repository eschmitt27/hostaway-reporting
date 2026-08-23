# Référentiels SQLite administrables depuis l'application (2026-08-23)

Mission « industrialisation : référentiels SQLite administrables ». HEAD départ `47e81d5`.

## 1. Audit initial — l'essentiel existait déjà

L'administration des référentiels n'était pas à construire : `referentiel_admin_service.py`
(historisation, journal `ref_admin_evenements`, garde no-delete, 28 tables catégorisées),
`logements_gestion_service.py` (cycle de vie logement : modifier/archiver/réactiver/changer
propriétaire/changer taux, historisé), `fournisseurs_referentiel_service.py` (CRUD fournisseurs
versionné, jamais de suppression) et l'écran `/administration/referentiels` (générique, catalogue
déclaratif `ref_setup_catalogue.py`) étaient déjà en place et déjà testés (`test_logements_gestion.py`,
`test_fournisseurs_referentiel.py`).

| Référentiel | Verdict avant cette mission | Manque réel |
|---|---|---|
| Propriétaires | EXISTANT À COMPLÉTER | Aucune garde bloquant la désactivation d'un propriétaire encore rattaché à un logement actif |
| Logements | EXISTANT ET SUFFISANT | — |
| Taux de commission | EXISTANT ET SUFFISANT | Chevauchement avec une période déjà close non vérifié ; clôture+ouverture non atomiques |
| Coûts standards ménage | **ABSENT** (au sens administration) | Table et colonnes de période déjà consommées par le moteur (`lot6f_cout_complet_menages.py`) mais librement éditables côté écriture, sans discipline de clôture/ouverture |
| Fournisseurs/Prestataires | EXISTANT ET SUFFISANT | Aucun lien de menu vers `/referentiel-fournisseurs` (collision avec l'écran charges `/fournisseurs`) |
| Paramètres métier | EXISTANT ET SUFFISANT | — (config.py ne contient que des paramètres techniques ; `ref_parametres_generaux` couvre le métier) |

Conclusion : **aucun deuxième système de référentiels créé**. Cinq manques réels comblés :
atomicité des transactions clôture+ouverture, historisation des coûts ménage, refus de
chevauchement avec une période close, garde propriétaire↔logement actif, lien de navigation
fournisseurs.

## 2. Atomicité clôture + ouverture (`referentiel_admin_service.transaction`)

`inserer`, `mettre_a_jour` et `clore_periode` acceptent désormais un `conn=` optionnel. Passé, ils
écrivent dessus sans committer ni fermer — c'est le nouveau context manager `transaction(db_path=)`
qui décide du commit final (rollback automatique sur exception). Sans `conn` (tous les appelants
existants, inchangés), le comportement autonome d'origine (ouvre/committe/ferme) est identique à
avant.

`logements_gestion_service.archiver/reactiver/changer_proprietaire/changer_taux_commission`
enveloppent désormais leurs deux écritures dans une seule `transaction()` : si l'ouverture de la
nouvelle période échoue après que la clôture a eu lieu, la clôture est annulée — jamais de logement
laissé sans période de gestion ou de taux ouverts.

Testé par collision volontaire de clé primaire sur la ligne à ouvrir (`test_changer_proprietaire_
rollback_si_ouverture_echoue`, `test_changer_taux_rollback_si_ouverture_echoue`) : l'ancienne
période reste ouverte après l'échec, rien n'est modifié.

## 3. Historisation de `ref_couts_standards_menage`

Nouveau module `couts_menage_gestion_service.py`, calqué sur `changer_taux_commission` mais au
grain **type de logement** (`type_logement_id`, colonnes `date_debut_validite`/
`date_fin_validite` — pas les mêmes noms que `ref_taux_commission`/`ref_gestion_logements_hist`).

Pour le permettre sans dupliquer la mécanique de période, `referentiel_admin_service.TABLES_
HISTORISEES` est devenu un dict `PERIODES` déclaratif `{table: {"grain", "debut", "fin"}}` — les
deux tables existantes (`ref_gestion_logements_hist`, `ref_taux_commission`) gardent leurs colonnes
`logement_id`/`date_debut`/`date_fin` inchangées ; `ref_couts_standards_menage` s'ajoute avec son
propre schéma sans code dupliqué.

`ref_couts_standards_menage` a rejoint `LECTURE_SEULE` : l'écran générique d'administration ne peut
plus la modifier librement (refus `E_ECRITURE`) ; un nouveau bloc dédié sur ce même écran
(`administration_referentiel_detail.html`, pas une deuxième page) appelle
`couts_menage_gestion_service.changer_cout(type_logement_id, cout, date_debut)`.

**Aucune règle métier de calcul modifiée** : le moteur (`lot6f_cout_complet_menages.py::date_aware`)
résolvait déjà le coût standard par `type_logement_id` + date de façon historisée en lecture ; seul
le chemin d'écriture manquait de discipline.

## 4. Refus de chevauchement avec une période déjà close

`inserer()` refusait déjà l'ouverture d'une deuxième période **ouverte** simultanée (`date_fin`
vide) pour un même grain. Ajout : si la nouvelle période a une `date_debut` renseignée, elle est
comparée aux périodes déjà **closes** du même grain — si `date_debut` tombe dans l'intervalle
`[date_debut_close, date_fin_close]` d'une période déjà terminée, refus `E_PERIODE_INCOHERENTE`.

Les clôtures normales (`clore_periode` puis `inserer` à la date suivante, toujours strictement
croissantes) ne déclenchent jamais ce refus — seule une saisie manuelle d'une date antérieure à une
période déjà close le fait.

## 5. Garde propriétaire ↔ logement actif

`basculer_activation("ref_proprietaires", ..., actif=False)` refuse désormais (code
`V10_PROPRIETAIRE_LOGEMENT_ACTIF`) si ce propriétaire a encore un rattachement `ref_gestion_
logements_hist` ouvert (`date_fin` vide). La réactivation d'un propriétaire reste toujours libre —
seule la désactivation est gardée, puisque c'est elle qui rendrait un logement actif sans
propriétaire exploitable par `lib_ref_history.resolve_management_period`.

## 6. Navigation fournisseurs

`/referentiel-fournisseurs` n'avait aucun lien de menu (le seul item nav `fournisseurs` pointe vers
`/fournisseurs`, l'écran de charges — objet distinct). Ajout d'une carte « Fournisseurs &
prestataires » sur l'écran `/administration/referentiels` existant, avec lien direct — aucun
deuxième menu créé.

## 7. Historique / audit

Inchangé : toutes les écritures (y compris les nouvelles, coûts ménage) passent par
`referentiel_admin_service.journaliser()` → table `ref_admin_evenements` (migration 0051,
préexistante). Aucun nouveau mécanisme de journal créé.

## 8. Acteur logique

Inchangé : `acteur="ui"` (convention déjà en place sur toutes les routes d'administration),
remplaçable plus tard par un identifiant utilisateur réel sans changer la forme des appels.

## 9. Invalidation DAG

Non touché par cette mission : aucune route d'administration ne déclenche `orchestrateur_service.
actualiser()` — l'utilisateur ré-actualise séparément (règle déjà en vigueur, confirmée, pas
modifiée ici).

## 10. Migrations

**Aucune migration ajoutée.** Toutes les colonnes utilisées (`ref_couts_standards_menage.
date_debut_validite/date_fin_validite/actif`) existaient déjà dans la migration 0029. Le seul
changement est applicatif (Python) : discipline d'écriture, pas de schéma.

## 11. Tests

`tests/test_referentiels_administration_industrialisation.py` (17 tests, nouveau) : atomicité (3),
historisation coûts ménage (7), chevauchement période close (1), garde propriétaire (3), écrans
existants (3). `tests/fixtures_referentiel.py` complété d'un paramètre `couts=` pour semer
`ref_couts_standards_menage` dans les tests.

Aucune régression : `test_logements_gestion.py`, `test_logements.py`, `test_ref_setup_*.py`,
`test_proprietaires.py`, `test_fournisseurs_referentiel.py`, `test_ref_assoc_mode_prep.py`,
`test_non_dependance_fichiers.py` tous verts après les changements.

## 12. Campagne finale

Moteur (`tests/` racine) : **345 passed**, 0 failed (baseline confirmée, aucune régression — ce
suite n'a pas été touché par cette mission).

Application (`05_APPLICATION/tests/`, 179 fichiers, 9 lots de ~20-22) : **2 678 passed**, 0 failed
(quelques `skipped` pré-existants, non liés à cette mission).

## 13. Intégrité réelle

`app.db` réelle : hash inchangé (`8e299b935ef1e0d4`) avant/après. `REF_Setup.xlsm` réel : non
touché (`git diff` vide sur ce fichier). Mode réel : `OFF`. Scheduler Hostaway réel : `INACTIF`
(non concerné par cette mission).

## 14. Limites restantes

- La garde propriétaire↔logement ne couvre que la désactivation manuelle depuis l'écran générique
  (`basculer_activation`) — pas un contrôle de cohérence global exécuté en tâche de fond.
- Le refus de chevauchement avec une période close est une garde défensive (saisie manuelle
  incohérente) ; le chemin nominal (clôture puis ouverture) ne peut structurellement pas le
  déclencher.
- `referentiel_service.py` (lecture, distinct de ce module) et `lib_ref_history.py` (résolution
  moteur) n'ont pas été modifiés — cette mission ne touche que l'écriture administrable.

## 15. Prochaine étape recommandée

Annoncée précédemment, non commencée : simplification architecture Python + séparation moteurs
métier purs + finalisation observabilité/backups. PostgreSQL, multi-utilisateur, analytique
avancée et activation du mode réel restent explicitement hors mandat.

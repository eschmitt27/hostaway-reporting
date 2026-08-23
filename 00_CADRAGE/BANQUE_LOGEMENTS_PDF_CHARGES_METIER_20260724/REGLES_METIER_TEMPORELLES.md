# Règles et variables métier historisées — administration temporelle (2026-08-23)

Mission « historiser règles et variables métier + administration temporelle depuis l'application ».
HEAD départ `3bffb69`.

## 1. Principe fondamental

Un moteur ne demande jamais « quel est le taux actuel ? » mais « quel était le taux applicable à la
date économique de cette opération ? ». Un recalcul futur d'une réservation passée doit utiliser les
règles en vigueur À SA DATE, jamais les règles courantes ; et il ne doit jamais dépendre d'une
sauvegarde SQLite (la sauvegarde reste un filet de sécurité — rollback, incident — jamais une source
de règles historiques).

## 2. Audit (fork lecture seule) — l'essentiel existait déjà

| Variable/règle | Historisée ? | Résolution par date ? | Verdict |
|---|---|---|---|
| Taux de commission (`ref_taux_commission`) | Oui | Oui — `lib_ref_history.resolve_commission_rate`, fail-closed | **A** |
| Gestion logement↔propriétaire (`ref_gestion_logements_hist`) | Oui | Oui — `resolve_management_period`, fail-closed | **A** |
| Coût ménage standard (`ref_couts_standards_menage`) | Oui | Oui — `lot6f_cout_complet_menages.py::date_aware` | **A** |
| Coût ménage interne (`ref_couts_menage_interne`/`ref_taux_heures_menage`) | Oui (colonnes) | Oui en lecture (`lib_menage_costs.py`) | **B** — écriture encore libre, non traitée cette mission (hors périmètre canapé) |
| **Paramètre canapé** (seuil/montant) | **Non** — valeur courante sur `ref_logements` | **Non** — `lot10_calculer_resultats.py` lisait la ligne courante sans filtre de date | **C — le manque le plus critique** |
| Assiette de commission (formule) | N/A (formule) | N/A | **D formel, non versionné** — une seule implémentation a toujours existé par canal ; versionner maintenant serait spéculatif (§25) |
| Règle canapé (formule seuil→montant) | N/A (formule) | N/A | **D formel, non versionné** — même raison |
| Groupes de logements (composition) | **Concept absent** | — | Voir `DECISIONS_METIER.md` D-REF-HIST-01 — non inventé |
| Paramètres métier généraux (`ref_parametres_generaux`) | Colonnes prêtes, jamais exploitées | Non | **B/C mélangé**, non traité cette mission (seul consommateur connu, `TAUX_HORAIRE_MENAGE_INTERNE`, probablement obsolète depuis le pivot coût fixe 2026-06-01) |

Conclusion : **aucun système d'historisation parallèle créé**. Le manque réel et concret comblé cette
mission est le paramètre canapé — exactement l'exemple donné par la mission elle-même (§0).

## 3. Convention des périodes (auditée, inchangée)

Fin **incluse** partout où une historisation existe réellement (`lib_ref_history.applies_on` :
`d > end → hors période`, donc `end` est le dernier jour valide). Cohérent avec `logements_gestion_
service`/`couts_menage_gestion_service`/`canape_gestion_service` qui clôturent toujours à la
**veille** de la nouvelle date d'effet. Aucun changement de convention — documentée, pas réinventée.

## 4. Paramètre canapé — historisation complète

### 4.1 Schéma

Migration additive `0058_canape_parametres_historises.sql` : nouvelle table
`ref_canape_parametres` (grain `logement_id`, `date_debut`/`date_fin`, `seuil_voyageurs_
preparation_canape`, `montant_preparation_canape`, `actif`). Backfill : une ligne par logement déjà
configuré sur `ref_logements`, période **ouverte depuis l'origine** (`date_debut`/`date_fin` vides =
valide depuis toujours) — préserve exactement le comportement actuel, écart 0,00 € garanti par
construction. `ref_logements.seuil_voyageurs_preparation_canape`/`montant_preparation_canape`
restent en place (parité classeur) mais deviennent vestigiales pour le calcul.

**Cette table n'est volontairement PAS dans `ref_setup_catalogue.FEUILLES`** (contrairement à
`ref_couts_standards_menage`) : elle n'a jamais eu d'onglet Excel, et l'y ajouter aurait fait
échouer `ref_setup_import_service._lire_classeur` (qui cherche chaque onglet déclaré dans le
classeur réel). Un petit catalogue séparé, `referentiel_admin_service.TABLES_NATIVES`, décrit cette
table pour que `_colonnes`/`_cle`/`lignes`/`ligne`/`decrire_table`/`categories` la traitent comme
n'importe quelle autre table administrable, sans toucher au contrat strict de l'import Excel. Deux
tests d'import réel (`test_banque_attentes_sqlite.py`, `test_banque_classification_sqlite.py`,
marqués `@reel_requis`) ont d'abord cassé en ajoutant la table au catalogue Excel — corrigé en la
retirant et en créant ce catalogue natif à la place.

### 4.2 Résolution par date

`lib_ref_history.resolve_canape_parametres(rows, logement_id, ref_date)` — même forme que
`resolve_commission_rate`/`resolve_management_period` : `MISSING` (aucune règle, cas normal — un
logement peut légitimement ne jamais facturer de canapé), `AMBIGUOUS` (chevauchement, erreur de
données), `OK` (`res.row` porte seuil/montant). Ne retombe JAMAIS sur une valeur « courante ».

### 4.3 Écriture administrable

`app/services/canape_gestion_service.py` (nouveau) : `changer_parametres(logement_id, seuil,
montant, date_debut)` — clôture la période active à la veille, ouvre une nouvelle ligne, jamais de
modification d'une ligne close, une seule transaction (`referentiel_admin_service.transaction`).
`ref_canape_parametres` rejoint `LECTURE_SEULE` (écran générique) ; un nouveau bloc dédié sur
l'écran existant `/administration/referentiels/ref_canape_parametres` (pas une deuxième page)
appelle ce service.

### 4.4 Verrou sur `ref_logements`

Nouveau `referentiel_admin_service.COLONNES_LECTURE_SEULE` : les deux colonnes vestigiales
(`seuil_voyageurs_preparation_canape`/`montant_preparation_canape`) sont exclues de l'édition libre
via l'écran générique `ref_logements` (qui reste éditable pour ses autres champs descriptifs). Le
formulaire les affiche en lecture seule, avec renvoi vers l'écran dédié.

### 4.5 Résolution effective dans le moteur (Lot10)

`lot10_calculer_resultats.py::load_sources` charge `ref_canape_parametres` depuis SQLite (`lib_db_
moteur.verifier`/`lignes`, base résolue via `--db`/`PILOTAGE_DB_PATH`/`APP_DATA_DIR` — **disponible
même en mode `--source EXCEL`**, car c'est un paramètre indépendant du flux). `build_commissions`
résout désormais le paramètre canapé À LA DATE DE LA RÉSERVATION (`date_arrivee`) via `resolve_
canape_parametres`, au lieu de lire la ligne courante de `REF_Logements`. **Repli explicite** :
si aucune base n'est fournie (tests existants, environnements sans SQLite), le comportement
historique (colonne courante) est préservé à l'identique — zéro régression pour les appelants
existants qui n'ont jamais fourni de base.

`lib_canape.py` (calcul pur seuil→montant) n'a **pas été modifié** : il reçoit toujours un simple
dict `ref_row`, qu'il vienne de la colonne courante ou de la résolution datée — le changement est
entièrement dans QUI construit ce dict, pas dans la formule elle-même.

## 5. Découverte annexe (non demandée, documentée par honnêteté)

Lot10, tel qu'invoqué par `calculs_executeur_service.executer_lot` (le chemin réel de l'application,
bouton « Actualiser »), ne passe **aucun** argument `--source`/`--db` — il tourne donc par défaut en
`--source EXCEL`, lisant les référentiels (logements, taux, gestion) directement depuis `REF_Setup.
xlsm`, jamais depuis les tables `ref_*` administrées par l'application. **`chemin_base` est
néanmoins résolu indépendamment** (via `PILOTAGE_DB_PATH`/`APP_DATA_DIR`, hérité de l'environnement
du process) — c'est ce mécanisme, déjà existant (`lib_db_moteur.chemin_db`), qui permet au
paramètre canapé (une donnée qui n'a jamais existé dans le classeur) d'être lu depuis SQLite même
en mode EXCEL, sans toucher au reste du pipeline. Cette découverte n'est **pas** traitée ici (câbler
`--source SQLITE` réellement dans `calculs_executeur_service` est un chantier séparé, touchant un
fichier classé C — 1533 lignes — hors mandat de cette mission) ; elle est documentée pour la
prochaine mission qui s'attaquerait à un moteur temporel pilote plus large (commission).

## 6. Tests

- `tests/test_ref_history.py` (racine) : 6 tests nouveaux, `CanapeParametresHistoryTests` — évolution
  future sans réécriture du passé, `MISSING`/`AMBIGUOUS`, période ouverte depuis l'origine, ligne
  inactive ignorée, logements distincts jamais confondus.
- `tests/test_canape_historise.py` (racine, nouveau) : 4 tests d'intégration `build_commissions`
  (Lot10) — preuve que la résolution datée fonctionne réellement dans le moteur (pas seulement le
  résolveur isolé), et que l'absence de base préserve le comportement historique.
- `05_APPLICATION/tests/test_canape_parametres_historises.py` (nouveau, 15 tests) : cycle de vie
  admin (`canape_gestion_service`), verrou colonnes (`COLONNES_LECTURE_SEULE`), écran générique.
- `05_APPLICATION/tests/test_sqlite_migrations.py` : `EXPECTED_TABLES` complété (`ref_canape_
  parametres`).
- `05_APPLICATION/tests/fixtures_referentiel.py` : paramètre `canape=` ajouté à `semer()`.

Aucune régression : campagne complète moteur 355/355 passed (racine `tests/`), application 2702
passed (05_APPLICATION, 10 lots) — 0 failed.

## 7. Intégrité réelle

`app.db` réelle : hash inchangé (`8e299b935ef1e0d4`) avant/après. Migration 0058 testée sur base
vierge ET copie de l'app.db réelle (jamais l'original), replay ×2, `PRAGMA integrity_check` = `ok`,
`PRAGMA foreign_key_check` = vide. `REF_Setup.xlsm` réel : non touché. Mode réel : `OFF`. Scheduler
Hostaway réel : `INACTIF` (non concerné par cette mission).

## 8. Limites restantes

- Groupes de logements historisés : concept absent du code, explicitement non inventé (voir
  `DECISIONS_METIER.md` D-REF-HIST-01) — nécessite un cadrage métier avant tout développement.
- Assiette de commission et formule canapé : non versionnées (RULE_ID/VERSION) — une seule
  implémentation a toujours existé, versionner maintenant serait spéculatif (§25 de la mission).
- Coût ménage interne (`ref_couts_menage_interne`) : historisé et résolu en lecture, mais toujours
  librement éditable en écriture (même défaut que `ref_couts_standards_menage` avant sa correction
  en mission précédente) — non traité ici, hors périmètre du paramètre canapé.
- `ref_parametres_generaux` : schéma de période déjà présent, jamais exploité par aucun consommateur
  connu (`TAUX_HORAIRE_MENAGE_INTERNE`, probablement obsolète) — non traité, décision différée.
- Lot10 (application réelle) lit toujours les référentiels historisés (taux, gestion) depuis
  `REF_Setup.xlsm`, pas depuis SQLite — découverte documentée en §5, non corrigée (hors mandat).

## 9. Prochaine étape recommandée

Annoncée par la mission : extraction d'un moteur temporel pilote, probablement **commission**
(déjà historisée et résolue par date — le candidat le plus proche d'être un moteur pur complet).
Le socle de résolution centralisée (`lib_ref_history.py`, déjà pur, déjà réutilisé par deux
domaines) est prêt à servir de base à ce prochain moteur, sans reconstruction.

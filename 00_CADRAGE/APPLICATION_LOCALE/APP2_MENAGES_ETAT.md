# APP-2a — Module Ménages : rapprochement en lecture

**Statut : `APP-2A_RAPPROCHEMENT_MENAGES_LECTURE_EN_ATTENTE_VALIDATION`**

> **APP-2b ajouté (recalcul sur copies) — statut `APP-2B_RECALCUL_COPIES_EN_ATTENTE_VALIDATION`.**
> Voir §9 à §12. Le mode réel reste **désactivé** (`MENAGES_REAL_RECALC_ENABLED = False`).
> Aucun fichier métier réel n'est modifié ; aucun commit tant que la validation humaine n'est pas donnée.

Le module est **visible, navigable et alimenté par les données réelles**. Il ne
produit encore **aucune écriture métier** et ne relance **aucun calcul réel**.

---

## 1. Ce qui est affiché

### `/menages` — écran principal

- période active, état global (**Conforme** / **À contrôler** / **Source incomplète**),
  date de dernière génération des sources ;
- six cartes de synthèse : ménages attendus, Hostaway réalisés, internes déclarés,
  externes facturés, écarts à contrôler, coût complet total (avec le coût réel en sous-titre) ;
- tableau de rapprochement au grain du moteur (**mois × logement × intervenant**),
  paginé (25 lignes) et trié — anomalies en premier par défaut ;
- filtres : période, logement, propriétaire, intervenant, type, statut, tri,
  « avec écart uniquement », « identification incomplète » ;
- bloc **Dernier calcul Ménages** : date, nombre d'anomalies, état de chaque source ;
- états vides, source absente et erreur lisible, tous distincts.

### `/menages/{mois}/{logement}/{intervenant}` — fiche de rapprochement

Sept blocs : résumé des 4 flux · ménages attendus · tâches Hostaway ·
internes M04 · externes facturés · coûts · anomalies · traçabilité.

### `/menages/a-controler` — écran de contrôle

Lignes à contrôler, contrôles du moteur (Lot6d) et contrôles de cohérence ménages
(Lot11). Chaque anomalie affiche code, niveau, résumé et lien vers la fiche.

### `/menages/diagnostic` — diagnostic du pipeline

État de chaque source, dernière génération, étapes du pipeline, commande serveur.
**Le bouton de recalcul est présent mais désactivé** : il n'existe aucune route
capable de lancer le pipeline sur les fichiers réels.

---

## 2. Sources utilisées (toutes en lecture seule)

| Bloc | Fichier | Onglet | État |
|---|---|---|---|
| A — Ménages attendus | — | — | **NON ALIMENTÉ** (voir §3) |
| B — Hostaway réalisé | `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` | `MASTER_ENRICHI`, `VUE_COMPTAGE` | Alimentée (500 tâches) |
| C — Interne déclaré | `MASTER_NORM_Declarations_Internes.xlsx` | `MASTER_NORMALISE` | Alimentée (20 lignes) |
| D — Externe facturé | `MASTER_FACT_MEN_MenagesExternes.xlsx` | `MASTER` | Alimentée (13 lignes) |
| Rapprochement | `MASTER_CTRL_Rapprochement_Menages.xlsx` | `TABLEAU_COMPARAISON`, `CONTROLES` | Alimentée (18 lignes) |
| Gain / perte | `MASTER_CALC_GainPerte_Menages.xlsx` | `DETAIL_ECART_COUT` | Alimentée (16 lignes) |
| Coût complet | `MASTER_CALC_CoutComplet_Menages.xlsx` | `DETAIL_COUT_COMPLET` | Alimentée (16 lignes) |
| Contrôles transverses | `MASTER_CTRL_Coherence.xlsx` | `MASTER` | Alimentée (4 contrôles ménages) |

Le reader distingue quatre états dégradés — **fichier absent**, **onglet absent**,
**source vide**, **source illisible** — et ne les confond jamais avec « zéro ménage ».

---

## 3. Le flux « ménages attendus » n'existe pas

C'est le constat central de cet audit.

`DECISIONS_METIER.md` (D090) énonce la règle : « 1 réservation validée = 1 ménage
attendu (check-out) ». **Aucun script du moteur ne matérialise cette colonne.**
Lot6d compare trois flux — Hostaway réalisé, interne déclaré, externe facturé — et
son `ecart` vaut *Hostaway − déclarés*. Il n'existe pas d'attendu indépendant.

L'application **ne le fabrique pas**. Le déduire des réservations reviendrait à
créer une règle de rapprochement dans FastAPI, ce que le cadrage interdit. La carte
et la colonne « Attendu » affichent donc `—` avec la mention **« Non alimenté par le
moteur »**.

À titre indicatif, la fiche montre le comptage Hostaway produit par Lot6a
(planifiées / réalisées / en attente / annulées), **explicitement nommé comme un
planifié Hostaway, pas comme un attendu métier**.

**Prochaine étape possible** : produire l'attendu dans le moteur (Lot6a ou un Lot6g),
au grain réservation × check-out, et l'exposer comme quatrième colonne du
rapprochement. C'est une décision métier, pas un chantier applicatif.

---

## 4. Ce qui n'est volontairement pas fait

- **Aucune écriture métier.** Ni MASTER, ni SAISIE, ni source brute.
- **Aucun recalcul du pipeline.** `/menages/diagnostic` affiche la commande ;
  il ne l'exécute pas. Aucune route n'importe `run_pipeline` ni `subprocess`.
- **Aucun matching applicatif.** Le rattachement d'une tâche Hostaway à un
  intervenant repose sur le mapping `assigneeUserId` de `REF_Intervenants`, fait par
  le moteur. La fiche liste donc les tâches **du logement et du mois**, toutes
  assignations confondues, et le dit.
- **Aucune valorisation Hostaway.** La colonne `cost` des tâches n'est ni lue, ni
  exposée. Le coût interne vient des heures et taux M04 ; le coût externe vient de la
  facture. Un test le vérifie en injectant un coût Hostaway de 999 € et en s'assurant
  qu'il n'apparaît jamais.
- **Aucun statut inventé.** `statut_controle` et `code_controle` viennent du moteur.
- **Aucune vérité métier parallèle en SQLite.** Voir §5.

### Deux notions de coût, jamais confondues

Le moteur produit deux coûts différents, que l'application nomme séparément :

| Notion | Source | Contenu |
|---|---|---|
| **Coût réel** | Lot6e `cout_reel_total` | heures et taux internes M04, **ou** montant facturé par le prestataire |
| **Coût complet** | Lot6f `cout_complet_total` | coût réel **+** quotes-parts des charges ménage (lavage, local, courses, consommables) |

La colonne du tableau et la carte de synthèse portent le **coût complet** ; le coût réel
apparaît en sous-titre de la carte et dans le bloc « Coûts » de la fiche. Un test verrouille
la distinction.

### Un seul critère « à contrôler »

La carte « Écarts à contrôler » et l'écran `/menages/a-controler` comptent **exactement le
même ensemble** : statut moteur `A_CONTROLER` ou `BLOQUANT`, **ou** identification incomplète
(logement / intervenant que le moteur n'a pas su rattacher — marqueurs `NON_ATTRIBUE`,
`ASSIGNEE_NON_MAPPE`). Le critère est un seul prédicat partagé (`menages_service.a_controler`),
et un test vérifie que les deux nombres coïncident.

---

## 5. Outrepassage — ce qu'il est et ce qu'il n'est pas

L'outrepassage existait avant ce lot (table `menage_overrides`). Il est **conservé
tel quel** et n'a pas été étendu.

Il enregistre un **motif horodaté** dans SQLite et affiche la ligne comme `JUSTIFIE`.
Le statut du moteur reste lu, affiché et interrogeable (`statut_moteur` est distinct
de `statut_effectif` dans le service et dans la fiche). Aucune source n'est modifiée.

**Point à trancher** : un statut effectif stocké en SQLite est à la limite de la règle
« ne jamais stocker une vérité métier parallèle ». Le compromis retenu ici — annotation
tracée, moteur toujours visible à côté — est explicite et testé, mais mérite une
décision formelle si le module doit aller plus loin.

---

## 6. Limites des données réelles

- **Un seul mois rapproché : `2026-05`.** Lot6d fixe `MONTH = "2026-05"` en dur.
  Le sélecteur de période n'affichera d'autres mois que lorsque le moteur les aura
  produits.
- Les déclarations internes portent aussi 2026-03, 2026-04 et 2026-11, mais ces mois
  ne sont pas rapprochés : ils n'apparaissent donc pas dans le tableau.
- 18 lignes de rapprochement, dont **7 avec écart** et **6 à contrôler**.
- Le contrôle `CONFLIT_TITLE_ASSIGNEE` compte 15 occurrences : le titre de la tâche
  Hostaway et l'assignation ne concordent pas. À instruire côté moteur.

---

## 7. Ce qui reste à faire

1. **Produire l'attendu métier** dans le moteur (décision à prendre — §3).
2. **Rendre le mois paramétrable** dans Lot6d (aujourd'hui figé à `2026-05`).
3. **Matching relançable** : route de recalcul avec instantané préalable, exécution en
   sous-processus, confirmation explicite — sur le modèle de la chaîne APP-3b.
4. **Outrepassage motivé** : trancher le point §5, puis, si le principe est retenu,
   journaliser l'outrepassage comme une écriture tracée à part entière.
5. Vendorer HTMX si l'on veut du rafraîchissement partiel : la bibliothèque n'est pas
   présente dans le dépôt et aucun CDN n'est autorisé. Les filtres fonctionnent
   aujourd'hui en formulaire GET, sans JavaScript, et le tableau est déjà isolé dans
   `partials/menages_table.html` pour être servi en fragment le jour venu.

---

## 7 bis. Défaut trouvé et corrigé pendant ce lot

`get_db(db_path=DB_PATH)` fige son défaut **à l'import**. Le service ménages appelait
`get_db()` sans argument : il ouvrait donc la **vraie** `app.db`, même quand un test
isolait `cfg.DB_PATH`. Aucune donnée n'était écrite (toutes les tables restent à zéro,
intégrité `ok`), mais `PRAGMA journal_mode=WAL` écrit dans l'en-tête du fichier — les
octets et le mtime bougeaient. La garde APP-3b « la base réelle n'a pas bougé »
(`test_charges_confirmation_e2e`) le détectait, mais seulement dans la suite complète.

Le défaut préexistait dans l'ancien service Ménages ; les nouvelles routes l'ont rendu
systématique. **Correctif** : le service lit `cfg.DB_PATH` à chaud
(`conn = get_db(cfg.DB_PATH)`), jamais le défaut figé. **Régression ajoutée** :
`test_route_ne_touche_jamais_la_base_reelle` compare SHA256 + mtime de la base réelle
avant/après trois pages Ménages. Correctif contenu dans le module — APP-3b non rouvert.

Méthode : ce défaut n'apparaît que suite complète. Éditer un gabarit **pendant** qu'une
suite tourne la fausse (Jinja recharge les templates à chaud) : les suites de mesure ont
donc été relancées sur code figé.

## 8. Tests

`05_APPLICATION/tests/test_menages_rapprochement.py` — 20 scénarios sur fixtures Excel
isolées (rapprochement conforme, Hostaway sans déclaration, déclaration sans tâche,
interne rapproché, externe rapproché, interne et externe simultanés mais distincts,
coût Hostaway jamais utilisé, source interne vide, source externe vide, fichier absent,
onglet absent, date externe absente, logement inconnu, anomalie bloquante, filtres
période / logement / écarts / intervenant / statut, pagination, fiche détail, aucune
source modifiée), plus les tests de route et les gardes structurelles.

`05_APPLICATION/tests/test_menages.py` (APP-2 d'origine) reste vert et inchangé.

---

# APP-2b — Recalcul du rapprochement (sur copies, mode réel gardé)

**Statut : `APP-2B_RECALCUL_COPIES_EN_ATTENTE_VALIDATION`** — implémenté, testé, non commité.

## 9. Ce que fait le recalcul

Le bouton **« Simuler le recalcul (copies)… »** (écran principal `/menages`) et
**« Tester le rapprochement sur copies (simulation)… »** (`/menages/diagnostic`) ouvre un parcours contrôlé : `GET /menages/recalculer` (préparation) → `POST …/preparer`
(revérification) → `POST …/confirmer` (exécution, **POST-Redirect-GET**) →
`GET …/runs/{id}` (résultat). Un rafraîchissement de la page résultat ne relance jamais le calcul.

Deux modes, portés par le flag `config.MENAGES_REAL_RECALC_ENABLED` (**False**) :

| Mode | Ce qu'il fait | État |
|---|---|---|
| **COPIES** | Copie le sous-arbre nécessaire dans un workspace sous `data/menages_recalc/`, y exécute **lot6d puis lot6e** via un runner hors paquet, compare l'état produit à l'état réel. **Aucun fichier réel touché.** | Actif |
| **RÉEL** | Régénérerait les MASTER ménages **en place** (via `run_menages_pipeline`, qui commence par lot6b → Google Sheet + réécriture M04/MASTER_NORM). | **Bloqué** (flag False) — `confirmer(REEL)` refuse et trace un run `BLOQUE`, sans rien exécuter |

**lot6f est exclu du recalcul sur copies** : il lit la Google Sheet (réseau, `fetch_sheet_csv`) et
n'est donc pas déterministe hors ligne. Le recalcul rafraîchit le rapprochement (lot6d) et la vue
gain/perte (lot6e), tous deux hors ligne.

### Architecture (pourquoi « sur copies » et pas « chemins injectés »)

Les lots 6d/6e ne sont pas des fonctions à chemins injectables : ce sont des **scripts** qui
dérivent tous leurs chemins de `ROOT = dirname(dirname(abspath(__file__)))`. On ne les réécrit pas
(règle métier intouchée) et on ne les importe pas (garde `test_no_import_of_travail_modules`). On les
**exécute dans un arbre miroir** — le workspace copié — où `ROOT` résout vers la copie. Le moteur y
lit et y écrit ; le réel n'est jamais atteint. Le runner `runners/menages_recalcul_runner.py` vit
hors du paquet `app/`, comme `charges_post_write_runner` (APP-3b) et `lot4a_dryrun_runner` (APP-2c),
et est lancé en sous-processus avec l'interpréteur moteur.

### Garde-fous (repris de la chaîne charges APP-3b)

- **Verrou interprocessus** atomique (`os.O_EXCL`, token de propriété), fichier dédié
  `data/.menages_recalcul.lock`, **libéré dans un `finally`** quoi qu'il arrive ;
- **Préflight bloquant** : Excel ouvert (fichiers `~$`) → `E_EXCEL_OUVERT` ; source absente →
  `E_SOURCE_ABSENTE` ; verrou déjà pris → `E_VERROU` ;
- **Snapshot** horodaté + manifeste sha256 des sources réelles **avant** tout (traçabilité) ;
- un run n'est **`SUCCES`** que si chaque étape rend `rc==0` **ET** produit ses sorties — jamais sur
  le seul code retour 0 ; codes d'échec fermés (`E_RUNNER`, `E_SORTIE_ABSENTE`, `E_TIMEOUT`,
  `E_REPONSE_INVALIDE`, `E_CONTROLE_ECHEC`, `E_REEL_MODIFIE`…) ;
- **contrôle post-run** : les sha256 des sources réelles sont recomparés avant/après ; toute
  divergence force `ECHEC` `E_REEL_MODIFIE` (ne peut pas arriver en mode copies — garde de dernier ressort).

### Historique et suivi

Chaque run est journalisé dans `menages_recalcul_runs` (migration `0005`, SQLite applicatif
uniquement) : mode, période, statut, dates, durée, `snapshot_id`, `git_head`, sha256 sources/sorties
avant-après, étapes, code/résumé d'erreur, comparaison avant/après. L'écran résultat montre le statut,
les étapes, la comparaison (lignes / écarts) et les empreintes. Les cinq derniers runs sont listés sur
l'écran de préparation.

**Suivi « en direct » (HTMX)** : non branché. La recette sur copies s'exécute en quelques secondes,
de façon **synchrone** dans le POST `confirmer`, et la page résultat rend l'exécution complète. Le
polling HTMX n'a de sens que pour un mode réel long et asynchrone ; il attend la vendorisation d'HTMX
(absent du dépôt, aucun CDN autorisé) et l'activation du mode réel. Documenté comme prochaine étape.

## 10. Attendu métier — décision requise (ne pas fabriquer)

L'audit a corrigé une imprécision : la règle « 1 réservation validée = 1 ménage attendu » **n'est pas
D090** (qui porte sur la structure du fichier IK/Avantages). Elle vient de **D100** et **D099** :

- **D100** : « 1 réservation validée = 1 ménage attendu (checkout), **uniquement logements sans
  `listingMapId` Hostaway actif** » — l'attendu ne concerne **que les logements hors Hostaway**, pour
  ne pas doubler le comptage des tâches Hostaway. Un attendu universel serait **faux**.
- **D099** : le rapprochement (Lot 6d) doit comparer « Hostaway Tasks réalisés **+ réservations HH
  (logements hors Hostaway)** vs factures externes + déclarations internes M04 ».

**Gap décision ↔ code constaté** : le `lot6d` réellement exécuté ne compte que les tâches Hostaway,
les externes et les internes ; **il n'ajoute pas les réservations HH** prescrites par D099. Le bloc
« attendu » n'existe donc ni comme colonne moteur, ni pour les logements hors Hostaway.

La matière première existe (`MASTER_CALC_Reservations_Resolues.xlsx` : 1391 lignes, colonnes `source`,
`menage_retenu`, dates, `statut_controle`). Un **Lot6g_Attendus_Menages** est donc *faisable*, mais les
exceptions ne sont **pas toutes décidées** : réservation annulée, owner stay, séjour sans ménage,
ménage intermédiaire, doublon Hostaway/HH, logement sorti de gestion, réservation clôturée. Conformément
à la consigne (« ne devine aucune exception »), **cette sous-partie est documentée et arrêtée** : aucun
Lot6g n'est créé, l'application ne fabrique pas l'attendu (ce serait une règle de rapprochement dans
FastAPI). Décisions humaines à trancher avant implémentation — voir §7 (liste) et le rapport final.

## 11. Grain — deux niveaux à prévoir (proposition, non implémentée)

Le grain actuel `mois × logement × intervenant` est **financier** : il agrège bien déclarations,
factures, coût réel et coût complet, mais n'explique pas une **réservation** précise. Un second grain
**opérationnel** `réservation / date de sortie / logement` serait nécessaire pour l'attendu et la tâche
Hostaway. Recommandation : ne pas tout forcer dans une table ; garder le grain financier existant pour
le rapprochement de valorisation, et introduire (avec le Lot6g) un grain opérationnel dédié à
l'attendu, relié par `logement_id + mois`. Non implémenté ici — dépend de §10.

## 12. Outrepassage — clé de stabilité (revue, non migrée)

L'outrepassage (`menage_overrides`, clé `UNIQUE(mois, logement_id, intervenant_id)`) est **inchangé**.
Revue : la clé est stable tant que le moteur conserve ce grain, mais un changement de grain (§11)
rendrait une justification orpheline. Amélioration possible **sans deuxième vérité métier** : stocker le
`ROW_HASH` de la ligne justifiée (le moteur le produit déjà, `rowhash(mois, lg, iid, nb_t, tot_dec)`)
pour détecter une justification périmée après recalcul, et afficher « un nouveau rapprochement peut
rendre cette justification obsolète ». Non migré ici (aucune nécessité démontrée à ce lot) — proposé.

## 13. Améliorations d'affichage livrées (APP-2b)

- Écran principal : boutons **« Simuler le recalcul (copies)… »** et **« Export CSV »** ; **indicateur de
  fraîcheur** (mtime des sources vs master rapprochement : « à jour » / « sources modifiées depuis le
  dernier rapprochement, relance recommandée ») — présenté comme un simple signal, pas une preuve comptable.
- Fiche : bloc **« Pourquoi cette ligne est à contrôler ? »** dérivé d'un dictionnaire d'explications
  lisibles (`EXPLICATIONS_CONTROLE`) — affichage seul, le code moteur reste autoritaire.
- **Export CSV** (`GET /menages/export.csv`) : vue filtrée, générée **en mémoire** (aucun fichier
  écrit), colonnes métier uniquement (aucun chemin interne, aucune donnée bancaire/voyageur). Filtres
  partagés avec la liste via un prédicat unique (`_match_vue`) pour qu'ils ne divergent jamais.

## 14. Tests APP-2b

`05_APPLICATION/tests/test_menages_recalcul.py` — 23 tests : préparation (copies / réel bloqué / Excel
ouvert), gardes de `confirmer` (mode réel refusé sans exécution, Excel ouvert, source absente, verrou
déjà pris, runner absent), logique via **runner stub** (succès, échec, réponse absente), deux runs
distincts, aucune source réelle modifiée, routes (formulaire, run inconnu 404, PRG réel→run bloqué,
diagnostic lie le recalcul, la base réelle n'est jamais touchée), export CSV + explication + fraîcheur,
et une **recette E2E réelle** qui exécute lot6d+lot6e sur copies des vraies sources et **prouve par
sha256 que le MASTER réel ne bouge pas** (ignorée si l'interpréteur moteur est absent).

## 15. Correctif transversal — isolation de la vraie app.db

**Cause** : `get_db(db_path=DB_PATH)` et `apply_migrations(db_path=DB_PATH)` figeaient `DB_PATH` **à
l'import** (valeur capturée dans le défaut d'argument). Le `lifespan` FastAPI appelle
`apply_migrations()` sans argument au démarrage du TestClient ; il visait donc la **vraie** app.db
même lorsqu'un test monkeypatchait `cfg.DB_PATH`. Chaque test montant un TestClient réappliquait ainsi
les migrations sur la vraie base (en-tête WAL modifié ; et, avec la migration 0005, une table vide y
apparaissait).

**Correction (7 fichiers, aucun défaut figé restant)** : `db_path: Path | None = None` + résolution
`Path(db_path) if db_path is not None else cfg.DB_PATH` **au moment de l'appel** (lecture à chaud).
Traité dans `connection.py` (get_db, apply_migrations), `pipeline_runner.py`, `audit_service.py`,
`snapshot_service.py`, `routes/sources_calculs.py`, `routes/health.py`. `DATA_DIR` devient isolable
par env `APP_DATA_DIR` (permet de lancer une instance de validation hors de la vraie base). Compatibilité
des appelants préservée (ceux qui passent un `db_path` explicite sont inchangés).

**Preuve** : `tests/test_db_isolation.py` — monkeypatcher `cfg.DB_PATH` redirige réellement `get_db()`
et `apply_migrations()` ; monter un TestClient laisse la vraie app.db **inchangée** (sha256 + mtime +
schéma identiques).

**Restauration de la vraie app.db** : aucune sauvegarde pré-mission vérifiée n'existait (base
gitignorée). Retrait contrôlé de la migration 0005 (transaction explicite : `DROP TABLE
menages_recalcul_runs` + `DELETE FROM schema_migrations WHERE version='0005'`, `integrity_check=ok`,
`foreign_key_check` vide, checkpoint WAL). État logique restauré : migrations **0001-0004**, aucune
table `menages_recalcul_runs`, **0 ligne métier**. Le sha256 diffère de l'original (`34fb43c8…` →
`7d237a64…`) car l'organisation interne SQLite après `DROP TABLE` ne restitue pas les octets d'origine —
l'état **logique** est identique. La migration **0005 reste dans le diff** et ne s'appliquera à la vraie
base qu'au futur déploiement validé.

## 16. Boutons — simulation vs réel (jamais confondus)

Tant que `MENAGES_REAL_RECALC_ENABLED = False`, l'interface ne présente **jamais** la simulation comme
une mise à jour effective :
- bouton principal : **« Simuler le recalcul sur copies »** + badge **« Aucun fichier réel ne sera
  modifié »** ;
- action réelle **visible mais désactivée** : **« Relancer le rapprochement réel »** —
  *« Non activé — nécessite une recette contrôlée »* ;
- page résultat (MODE COPIES) : bandeau explicite « les MASTER réels n'ont pas changé ; les cartes de
  `/menages` restent celles du dernier calcul réel ; cette simulation ne remplace pas le rapprochement
  affiché » ;
- un POST forcé `mode=REEL` retourne/trace **`E_MODE_REEL_DESACTIVE`** (run `BLOQUE`), sans verrou, sans
  snapshot, sans workspace, sans script lancé — la garde du flag est évaluée **en premier**.

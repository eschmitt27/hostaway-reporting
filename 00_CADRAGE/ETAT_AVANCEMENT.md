# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.
> Deux pistes distinctes : **MOTEUR** (pipeline de calcul Lot 0→13, sections ci-dessous) et **APPLICATION LOCALE** (section dédiée juste après). Ne pas mélanger.

---

## APPLICATION LOCALE — Suivi dédié

> Piste de l'application locale de pilotage. Plan complet : `00_CADRAGE/APPLICATION_LOCALE/PLAN_CONSTRUCTION_APPLICATION_LOCALE.md` (ne pas le dupliquer ici).
>
> **Règle de tenue permanente** : après chaque lot app réellement démarré / modifié / testé / validé / bloqué / terminé, ajouter une entrée datée avec : statut, lot concerné, fichiers touchés, tests réalisés, résultat, anomalies/risques, décisions attendues, prochaine action. Ne jamais marquer un lot **FAIT** sans les contrôles, tests et preuves prévus par le cadrage. Édition ciblée, historique conservé.

### Statut global app
**APP-2a VALIDÉ — D-APP-05A/B/C/D VALIDÉES — APP-2b IMPLÉMENTÉE + AUDITÉE + CORRECTIFS APPLIQUÉS — VALIDATION HUMAINE RÉELLE REQUISE AVANT ACTIVATION HH_REAL_WRITE_ENABLED.**
Application : 181/181 tests verts. Lot4A : 42/42 tests verts.
Suite moteur exhaustive : non exécutable intégralement dans l'environnement local (88 tests verts ; 1 module non chargeable faute de `requests`/`python-dotenv` — anomalie antérieure à Lot4A, hors périmètre Lot4A).

### Journal app

#### 2026-07-03 — APP-2b — Saisie HH contrôlée — IMPLÉMENTÉE + AUDITÉE + CORRECTIFS APPLIQUÉS — VALIDATION HUMAINE REQUISE
- **Statut** : IMPLÉMENTÉE TECHNIQUEMENT avec 9 correctifs post-audit appliqués. `HH_REAL_WRITE_ENABLED = False` — aucune écriture réelle possible jusqu'à validation humaine et activation explicite.
- **Portée** : formulaire de saisie guidée + validation D1–D11 + écriture atomique + garde + rollback. Aucun Excel réel modifié. `SAISIE_ReservationsHorsHostaway.xlsx` sha256 = `c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c` INCHANGÉ.
- **Routes créées** :
  - `GET /reservations/nouvelle` — formulaire (listes REF_LOCALE)
  - `POST /reservations/nouvelle/verifier` — validation D1–D11 + prévisualisation PK
  - `POST /reservations/nouvelle/confirmer` — écriture atomique (bloquée par garde)
- **Architecture writer/orchestrateur (séparation stricte)** :
  - `saisie_hh_writer.py` — opérations fichier pures, aucun SQLite. ~$ + verrou + préflight formules + structure + delta + remplacement atomique. 14 étapes.
  - `saisie_hh_orchestrator.py` — guard + snapshot + SQLite + rollback. Aucun openpyxl/save().
  - Séparation vérifiée par `test_no_bidirectional_sync` (détection import DB réel, pas mention textuelle).
- **Fichiers créés (12)** :
  1. `app/db/migrations/0002_saisie_hh.sql` — table `saisie_hh_writes` (journal applicatif)
  2. `app/readers/saisie_hh_reader.py` — lecture REF_LOCALE, PKs existants, génération RESHH-AAAA-MM-NNN
  3. `app/readers/ref_setup_hh_reader.py` — REF_Setup pour APP-2b (cloture, gestion hist, associes, logements, proprietaires)
  4. `app/services/saisie_hh_service.py` — validation D1–D11 + Decimal + fail-closed REF_Setup
  5. `app/services/saisie_hh_orchestrator.py` — guard + snapshot + SQLite + rollback atomique
  6. `app/writers/saisie_hh_writer.py` — écriture atomique pure (14 étapes), aucun SQLite
  7. `app/templates/reservation_nouvelle_form.html`
  8. `app/templates/reservation_nouvelle_verif.html`
  9. `tests/test_saisie_hh_validation.py` — D1–D11 + Decimal + fail-closed + D9 divergence
  10. `tests/test_saisie_hh_writer.py` — ~$, verrou, préflight, structure, delta, fullCalcOnLoad
  11. `tests/test_saisie_hh_orchestrator.py` — guard, SQLite, rollback
  12. `tests/test_saisie_hh_routes.py` — routes GET/POST
- **Fichiers modifiés (6)** :
  1. `app/config.py` — `HH_REAL_WRITE_ENABLED = False` ; `_REF_CLOTURE_OBSOLETE = None` (neutralise chemin autonome)
  2. `app/db/connection.py` — `apply_migrations` applique tous les *.sql en ordre
  3. `app/readers/ref_setup_reader.py` — import Path + note module séparé APP-2b
  4. `app/routes/reservations.py` — 3 nouvelles routes ; import `saisie_hh_orchestrator` (remplace writer direct)
  5. `tests/test_no_metier_calc.py` — détection sync bidirectionnel par import DB réel (pas mention textuelle)
  6. `tests/test_sqlite_migrations.py` — `saisie_hh_writes` dans EXPECTED_TABLES
- **Invariants APP-1 préservés** : `test_code_ne_lit_jamais_proprietaires_ni_gestion` toujours vert.
- **Correctifs post-audit (9)** :
  - C1 : `REF_CLOTURE` path autonome neutralisé → `_REF_CLOTURE_OBSOLETE = None`
  - C2 : writer = pur fichier (0 sqlite) ; orchestrateur = pur SQLite/guard/rollback (0 openpyxl save)
  - C3 : ~$ détection + verrou O_CREAT|O_EXCL avant toute écriture
  - C4 : préflight formules (figée → ERREUR), volume check
  - C5 : préservation structurelle (sheets/DV/MFC/named_ranges/fullCalcOnLoad) avant os.replace
  - C6 : rollback automatique sur échec journalisation post-write
  - C7 : Decimal (pas float) pour montants ; rejet >2 décimales ; fail-closed REF_Setup D7/D8/D9/D10
  - C8 : guard à l'entrée de l'orchestrateur (fail-closed HH_REAL_WRITE_ENABLED)
  - C9 : test_no_bidirectional_sync affiné + tests writer/orchestrateur/validation ajoutés
- **Tests** : `pytest tests/ -q` → **181/181 PASSED**. Toutes suites précédentes vertes.
- **Garde d'écriture** : `HH_REAL_WRITE_ENABLED = False` dans orchestrateur. Toute activation nécessite modification manuelle explicite de `config.py` + validation humaine.
- **Prochaine action** : validation humaine de l'interface, activation volontaire de `HH_REAL_WRITE_ENABLED = True`, puis première saisie de validation.
- **Entrées JOURNAL_CONTROLES** : CTR-DAPP2B-IMPL-2026-07-03 ; CTR-DAPP2B-AUDIT-2026-07-03.

#### 2026-07-03 — D-APP-2B-CADRAGE — Décisions fonctionnelles verrouillées (D1 à D11) — VALIDÉ
- **Statut** : CADRAGE FONCTIONNEL VERROUILLÉ. Implémentation APP-2b NON démarrée.
- **Portée** : décisions D1 à D11 pour la création contrôlée de réservations hors Hostaway. Détail dans DECISIONS_METIER (D-APP-2B).
- **D1** : reservation_id_hostaway obligatoire pour VRBO_UNKNOWN / DIRECT_HA_PAYANT / HOSTAWAY_REFERENCE, facultatif sinon ; entier positif ; unicité si renseigné ; doublon → RESERVATION_DOUBLON_HOSTAWAY_HH.
- **D2/D3** : total_percu obligatoire pour toute création, y compris VRBO_UNKNOWN. Tolérance Excel CTR-L4-13 non reprise.
- **D4** : aucun mapping canal ↔ source financière imposé ; validation d'appartenance aux listes seulement ; aucune correction auto.
- **D5** : codes de blocage APP-2b validés (non implémentés) — voir DECISIONS_METIER.
- **D6** : reservation_hh_id = RESHH-AAAA-MM-NNN sur mois de date_arrivee ; max+1 ; trous admis ; suffixe non numérique → SEQUENCE_PK_INCOHERENTE ; pas de génération si mois clôturé/absent.
- **D7** : REF_Gestion_Logements_Hist — date_fin inclusive, vide = ouverte ; cohérence prop/logement à la date_arrivee.
- **D8** : éligibilité logement = gestion active à date_arrivee + statut_parc GERE + actif OUI ; sinon bloqué ; aucune correction auto.
- **D9** : REF_LOCALE compatible Excel, REF_Setup autoritaire ; divergence → DIVERGENCE_REF_LOCALE_REF_SETUP.
- **D10** : l'application ne crée jamais de mois ; ouverture 2026-07/OUVERT manuelle dans REF_Setup.xlsm ; sinon MOIS_HORS_REFERENTIEL_CLOTURE.
- **D11** : associe_id_recuperateur obligatoire seulement si montant_recupere > 0.
- **APP-2b** : cadrage verrouillé. `saisie_writer.py` reste stub (`NotImplementedError`). Aucune route d'écriture, aucun writer, aucun fichier Excel modifié. Implémentation sur feu vert humain explicite.
- **Entrée JOURNAL_CONTROLES** : CTR-DAPP2B-CADRAGE-2026-07-03.

#### 2026-07-02 — D-APP-05B / D-APP-05C / D-APP-05D — Preuve écriture + correction formules SAISIE HH — VALIDÉ HUMAINEMENT
- **Statut** : VALIDÉ HUMAINEMENT — Formules B/C corrigées sur fichier réel. APP-2b débloquée techniquement.
- **D-APP-05A** : VALIDÉE — Comparateur et dry-run LOT4A terminés techniquement (voir CTR-LOT4A-2026-07-02).
- **D-APP-05B** : VALIDÉE — Preuve d'écriture openpyxl sur copie isolée `05_APPLICATION/data/dapp05b_proof/20260702T095519Z/`. 12/12 contrôles automatisés verts. Validation humaine Excel réussie : formules, validations, plages nommées, MFC, feuilles, ligne historique préservées ; fichier réel inchangé pendant la preuve.
- **D-APP-05C** : VALIDÉE — Correction des formules B (ROW_HASH) et C (mois) sur copie isolée `05_APPLICATION/data/dapp05c_formules/20260702T143503Z/`. Anomalie préexistante : tokens `TEXT`/`YYYY`/`DD` localisés, incompatibles Excel FR. Nouvelles formules : `YEAR`/`MONTH`/`DAY` + `RIGHT("0"&…,2)` + `INT`/`ABS`/`ROUND`/`MOD` pour les montants — voir D-APP-05C (DECISIONS_METIER). 14/14 contrôles verts. Validation humaine Excel réussie.
- **D-APP-05D** : VALIDÉE — Correction atomique réelle sur `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`. Périmètre : B2:B501 et C2:C501 uniquement. 11/11 contrôles structurels verts. Remplacement atomique (`os.replace`). Backup conservé : `05_APPLICATION/data/dapp05d_reel/20260702T155817Z/backup_avant/SAISIE_AVANT.xlsx` (sha256 origine vérifié). État intermédiaire post-`os.replace` : sha256=`b4165ca9…87b101`, taille 85696 o — Excel réécrit le fichier lors de la validation humaine (sérialisation ZIP différente). État final après ouverture/sauvegarde Excel : sha256=`c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c`, taille 49717 o. Vérification finale lecture seule : 17/17 contrôles CONFORME (voir `verification_finale.json`). Validation humaine Excel réussie : C2=`2026-05`, B2=`RESHH-2026-05-001|CANAL_004|PROP_0003|LOG_0009|20260525|2343.48`, aucune alerte de réparation Excel.
- **APP-2b** : DÉBLOQUÉE TECHNIQUEMENT. `saisie_writer.py` reste stub (`NotImplementedError`). Aucune route d'écriture créée. Démarrage APP-2b uniquement sur feu vert humain explicite.
- **Entrée JOURNAL_CONTROLES** : CTR-DAPP05BCD-2026-07-02.

#### 2026-07-02 — LOT4A — Comparateur + Dry-run — D-APP-05A TERMINÉ TECHNIQUEMENT
- **Statut** : TERMINÉ EN DRY-RUN — 42/42 tests LOT4A verts (22 comparateur + 20 transformateur). Sources réelles inchangées.
- **Contexte** : réponse à la découverte preflight (aucun Power Query réel). LOT4A est un moteur Python déterministe SAISIE → MASTER de test, remplacement du mécanisme PQ inexistant.
- **Fichiers créés (3)** :
  1. `02_TRAVAIL/lib_lot4a_reservations_hh.py` — bibliothèque pure partagée (règles uniques : schéma 34 col, lecture SAISIE/MASTER/REF, `round2` numpy, `recompute`, résolution taux, validation métier, `build_master`, VUE_ACTIVE, gardes de chemin, empreintes).
  2. `02_TRAVAIL/lot4a_transform_reservations_hh.py` — transformateur mode unique `--dry-run --as-of ISO-8601 UTC`.
  3. `tests/test_lot4a_transform_reservations_hh.py` (20 tests).
- **Fichier modifié (1)** :
  - `02_TRAVAIL/lot4a_compare_reservations_hh.py` — refactorisé pour utiliser la bibliothèque commune ; comportement inchangé.
- **Fichier déjà existant, inchangé** :
  - `tests/test_lot4a_compare_reservations_hh.py` (22 tests — déjà vert avant Lot4A).
- **Run dry-run réel** (commande : `py -3 lot4a_transform_reservations_hh.py --dry-run --as-of 2026-07-02T00:00:00Z`) :
  - Statut : `ANALYSE_TERMINEE`, `master_test_genere = true`.
  - MASTER de test : 2 feuilles MASTER + VUE_ACTIVE, 34 colonnes, aucun POWER_QUERY_CODE.
  - Oracle RESHH-2026-05-001 : `taux=0.15`, `commission=343.27`, `acompte_facture=1945.21` (arrondi numpy, jamais builtin).
  - Écrit uniquement sous `04_LOGS/LOT4A_DRY_RUN/20260702T085200Z/`.
- **Invariance sources réelles** :
  - SAISIE  sha256 `e4591912a6b0f1ca`… taille 75924 mtime_ns 1782676150164898800 INCHANGÉ
  - MASTER  sha256 `c0e4434c347987d2`… taille 10829 mtime_ns 1781524006555932300 INCHANGÉ
  - REF_Setup sha256 `f45f4feadcbebd04`… taille 86345 mtime_ns 1782721518446925000 INCHANGÉ
- **Gardes permanents** : chemins `01_SOURCES_BRUTES`, `02_TRAVAIL`, `03_EXPORTS`, `05_APPLICATION` refusés (RuntimeError). Écriture atomique temp → validate → `os.replace`. AST : aucun `subprocess`, `win32com`, `saisie_writer`, ni argument `--write-master`. Aucun `--write-master` dans le CLI.
- **Statuts LOT4A** : 5 statuts validés — `ANALYSE_TERMINEE`, `ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES`, `ANALYSE_BLOQUEE_TAUX`, `ANALYSE_BLOQUEE_DONNEES` (5e statut, validé explicitement le 2026-07-02), `ERREUR_TECHNIQUE`. Voir D-LOT4A-01.
- **Compatibilité aval** : lot4bis lit onglet MASTER par nom ✓ ; lot5 lit `acompte_facture` ✓ ; `date_integration` ISO UTC texte non consommée par lot4bis/lot5/lot9 ✓ ; aucun consommateur n'exige POWER_QUERY_CODE ✓.
- **Données réelles** : Aucune modification sous `01_SOURCES_BRUTES/` ni `03_EXPORTS/`. Les seules modifications sous `02_TRAVAIL/` sont les ajouts et le refactor Lot4A explicitement documentés ci-dessus.
- **Entrée JOURNAL_CONTROLES** : CTR-LOT4A-2026-07-02.
- **D-APP-05A** : TERMINÉ TECHNIQUEMENT EN DRY-RUN. Aucun `--write-master` créé ni autorisé. MASTER réel jamais touché.
- **D-APP-05B** : NON DÉMARRÉ. Preuve d'écriture dans une copie isolée de SAISIE (preservation formules/DV/plages/MFC) — prérequis restant avant APP-2b.
- **APP-2b** : TOUJOURS BLOQUÉE. Requiert D-APP-05B validé + validation humaine finale LOT4A avant toute écriture.
- **Prochaine action** : D-APP-05B (preuve copie isolée SAISIE), sur feu vert humain.

#### 2026-07-02 — D-APP-05 — Protocole de preuve technique — BLOCAGE AU PRÉFLIGHT
- **Statut** : BLOQUÉ au préflight (étape 1). **Aucune ligne de test écrite. Aucune copie créée. Aucun fichier réel modifié.**
- **Contexte** : exécution du protocole de preuve D-APP-05 (écriture contrôlée future dans `SAISIE_ReservationsHorsHostaway.xlsx`). APP-2b reste bloquée.
- **Préflight réalisé (lecture seule uniquement)** :
  - Excel COM **disponible** (win32com OK, Excel 16.0).
  - Inspection interne des classeurs par décompression (copies extraites en scratchpad, originaux jamais ouverts en écriture).
- **Découverte bloquante** : **aucun Power Query vivant** dans les fichiers réels.
  - `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` : généré par **openpyxl 3.1.5** (creator=openpyxl, créé 2026-06-09). **Aucun `connections.xml`, aucune DataMashup, aucune requête PQ.** Fichier statique (3 feuilles + texte).
  - `SAISIE_ReservationsHorsHostaway.xlsx` : généré par **openpyxl 3.1.5**. **Aucune connexion PQ.** Contient bien 12 plages nommées `lst_*`, validations de données, mise en forme conditionnelle, formules (ROW_HASH, mois, nuits, taux, commission, acompte, impacts).
  - **Aucun script actuel ne régénère le MASTER depuis la SAISIE** : lot4bis/lot10/lot11 **lisent** le MASTER ; lot12_seed/remove injectent/retirent des données fictives ; le vrai build SAISIE→MASTER n'existe pas dans l'arbre courant (généré une fois par openpyxl).
  - La feuille `POWER_QUERY_CODE` et les lignes `[Chargé par Power Query…]` sont **documentaires**, pas des requêtes réelles.
- **Conséquence** : le mécanisme central du protocole (« refresh Power Query manuel → MASTER de test reçoit la ligne ») **n'est pas exécutable** : il n'y a pas de Power Query à rafraîchir. Par la règle §1 du protocole, arrêt avant toute écriture.
- **Hash SHA-256 originaux (référence, inchangés)** :
  - SAISIE : `e4591912a6b0f1cad69775c2c0554bc603e6ea43097133e46c21878807414190`
  - MASTER : `c0e4434c347987d21bac52d7df6def4633fb949ffe3267fb203252a362300390`
- **Données réelles** : `git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/` → vide.
- **Solution minimale proposée** (à valider) : reformuler D-APP-05 sans Power Query — (a) preuve Excel COM d'écriture d'une ligne dans une copie isolée de la SAISIE en préservant formules/DV/plages/MFC ; (b) identifier ou reconstruire le générateur réel SAISIE→MASTER (script openpyxl) et le rejouer sur les copies isolées pour prouver la propagation de la ligne. Décision humaine requise avant toute écriture.
- **Entrée JOURNAL_CONTROLES** : CTR-DAPP05-PREFLIGHT-2026-07-02.
- **D-APP-05** : reste OUVERTE. **Non validée.** APP-2b bloquée.

#### 2026-07-01 — Lot APP-2a — Réservations hors Hostaway (lecture seule) — TERMINÉ
- **Statut** : TERMINÉ — 96/96 tests pytest verts (18 nouveaux tests réservations + 3 nav).
- **Lot concerné** : APP-2a — consultation lecture seule des réservations hors Hostaway. **APP-2b (écriture) reste bloquée.**
- **Source de lecture (unique)** : `02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`, onglet **MASTER** (sortie générée par Power Query, consommée telle quelle par lot4bis). Ligne-placeholder Power Query filtrée (garde `reservation_hh_id` commençant par `RESHH-`). VUE_ACTIVE écartée (cache PQ vide).
- **SAISIE non lue** : `SAISIE_ReservationsHorsHostaway.xlsx` seulement nommée pour l'origine, jamais lue pour construire la liste.
- **Arbitrages appliqués** :
  - Aucune route POST/PUT/PATCH/DELETE ; `GET /reservations` + `GET /reservations/{reservation_hh_id}` uniquement (POST → 405 vérifié).
  - Aucun formulaire, brouillon, modification, annulation, génération de PK, appel `saisie_writer`, snapshot déclenché par l'UI, exécution PQ/pipeline.
  - Montants dérivés (taux, commission, acompte, impacts) **affichés tels quels, signalés « issus du moteur »**, jamais recalculés (badge `moteur` + note explicite).
  - Filtres réels : mois, logement, propriétaire, canal, source financière, statut contrôle, code impact, comptabilisation. Recherche texte. États vide / EMPTY (cache PQ vide) / erreur source absente / réservation inconnue (404 propre).
  - Bloc fraîcheur : fichier source + dernière modification MASTER + « Actualisation Power Query manuelle requise après toute future saisie ».
  - Aucune donnée personnelle : la structure ne contient aucun nom/email voyageur.
- **Fichiers créés (5)** :
  1. `app/readers/reservations_hh_reader.py` (lecture read-only MASTER, filtre placeholder PQ)
  2. `app/services/reservations_hh_service.py` (liste/détail, états d'erreur, zéro calcul)
  3. `app/routes/reservations.py` (2 routes GET)
  4. `app/templates/reservations_list.html`
  5. `app/templates/reservations_detail.html`
  6. `tests/test_reservations_hh.py` (18 tests)
  > Rectif décompte : **6 fichiers créés** (les 5 ci-dessus + le fichier de tests).
- **Fichiers modifiés (4)** :
  1. `app/config.py` (constantes `MASTER_RESERVATIONS_HH`, `SAISIE_RESERVATIONS_HH`)
  2. `app/main.py` (router reservations)
  3. `app/templates/base.html` (menu Réservations cliquable)
  4. `app/routes/home.py` (module Réservations → disponible)
  5. `tests/test_navigation_no_404.py` (/reservations live)
  > Rectif décompte : **5 fichiers modifiés**. `home.html` non modifié (bascule via `home.py`).
- **Tests** : `pytest tests/ -q` → **96/96 PASSED** (9.89 s). Aucun skip (données réelles présentes : 1 réservation RESHH-2026-05-001).
- **Lancement réel** : `/reservations` 200 · détail `RESHH-2026-05-001` 200 · inconnu 404 propre · `POST /reservations` 405 · `/fournisseurs` 404 (à venir). Liste = 1 réservation, note fraîcheur affichée.
- **Données réelles** : `git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/` → vide. Empreinte MASTER et SAISIE inchangée après consultation (contrôlé en test).
- **Entrée JOURNAL_CONTROLES** : CTR-APP2a-2026-07-01.
- **D-APP-05** : **TOUJOURS OUVERTE**. Protocole de preuve APP-2b sur copie isolée préparé (voir journal contrôles), non exécuté.
- **Prochaine action** : validation humaine APP-2a puis, séparément, exécution du protocole de preuve D-APP-05 avant tout déblocage APP-2b. Aucun module APP-2b/APP-3+ démarré.

#### 2026-07-01 — Lot APP-1 — Module Logements (lecture seule) — TERMINÉ
- **Statut** : TERMINÉ — 76/76 tests pytest verts (dont 21 tests logements + 3 nav mis à jour).
- **Lot concerné** : APP-1 — premier module métier `Logements`, strictement lecture seule.
- **Arbitrages appliqués (feu vert humain)** :
  - **Source liste = unique** : `03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv` (propriétaire + dates déjà résolus par le moteur). Aucune reconstruction par jointure `REF_Logements × REF_Gestion_Logements_Hist × REF_Proprietaires`. Aucun fallback Excel si absent/vide/illisible → état d'erreur clair.
  - **Enrichissement fiche** : `REF_Setup.xlsm` en lecture seule, par clé directe `logement_id` (+ `proprietaire_id` pour le seul historique commission, `type_logement_id` pour décodage type/coût standard). Jamais `REF_Gestion` ni `REF_Proprietaires`.
  - **Commission (D-APP-04 validée)** : bloc `Historique des taux de commission` — lignes brutes (taux, date début, date fin, actif brut, origine). Aucun « taux actuel », aucune sélection, aucune comparaison de dates, `date_fin` vide → `Date de fin non renseignée`.
  - **Lignes techniques** (`APPARTEMENT_DIVERS`, `LOGEMENT_DIVERS`) : exclues par défaut, jamais masquées ; toggle `Inclure les lignes techniques (2)` ; badge `HORS_PARC_TECHNIQUE` ; non comptées dans le parc (17 logements réels).
  - **Affichage** : liste = ville (pas adresse), propriétaire résolu, type, statut, Hostaway, forfait ; pas de `hostaway_listing_id` ni contact propriétaire en liste. Fiche = adresse autorisée, `hostaway_listing_id` dans bloc `Informations techniques`, canaux limités à `Présent dans Hostaway : Oui/Non`, contact propriétaire interdit, absent = `Non renseigné`.
  - **Traçabilité** : bloc `Origine des données` par fiche (source liste, référentiel, date/heure lecture, identifiant d'enrichissement, message si enrichissement absent).
- **Correction APP-0 intégrée** : `config.py` — chemin réel `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` (sous-dossier). `/health` remonte désormais `ref_setup exists: true`, status OK.
- **Fichiers créés (6)** :
  1. `app/readers/ref_setup_reader.py` (lecteur read-only, clés directes, jamais gestion/propriétaires)
  2. `app/services/logements_service.py` (assemblage, états d'erreur, zéro calcul)
  3. `app/routes/logements.py` (GET /logements, GET /logements/{logement_id})
  4. `app/templates/logements_list.html`
  5. `app/templates/logements_detail.html`
  6. `tests/test_logements.py` (21 tests)
- **Fichiers modifiés (6)** :
  1. `app/config.py` (chemin REF_Setup corrigé + constante PBI_LOGEMENTS)
  2. `app/main.py` (router logements inclus)
  3. `app/templates/base.html` (menu Logements cliquable)
  4. `app/routes/home.py` (module Logements → disponible)
  5. `app/static/css/app.css` (styles module logements)
  6. `tests/test_navigation_no_404.py` (/logements live)
  > `app/templates/home.html` n'a PAS été modifié au Lot APP-1 : la bascule du module se fait par `home.py` seul (le template portait déjà la branche « disponible » depuis les corrections APP-0).
- **Tests** : `pytest tests/ -v` → **76/76 PASSED** (7.10 s). Aucun skip (données réelles présentes).
- **Lancement réel** : `/` 200 · `/logements` 200 · `/logements/LOG_0001` 200 · `/logements/LOG_9999` 404 propre · `/sources-calculs` 200 · `/proprietaires` 404 (module à venir). `/health` status OK, `ref_setup exists: true`. Liste = 17 logements parc, techniques exclues par défaut, toggle OK, recherche Blagnac → LOG_0002.
- **Données réelles** : `git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/` → vide. Aucune source modifiée (contrôlé par empreinte taille+mtime en test).
- **Entrée JOURNAL_CONTROLES** : CTR-APP1-2026-07-01 (voir ci-dessous).
- **Décisions** : D-APP-04 CLÔTURÉE (affichage commission datée brute, sans calcul). D-APP-05 (contrat écriture SAISIE) toujours OUVERTE, pré-requis APP-2.
- **Limites documentées** : canaux par logement inexistants en référentiel (seul `sur_hostaway`) → pas d'invention. Coût standard ménage rattaché par type du logement (référence brute), coût interne par `logement_id`.
- **Validation humaine** : ACCORDÉE le 2026-07-01 — contrôles visuels conformes (liste, recherche/filtres, séparation lignes techniques, fiche détail, historique commission sans calcul, `/health`).
- **Prochaine action** : cadrage APP-2 avant tout démarrage. Aucun module APP-2+ démarré.

#### 2026-07-01 — Lot APP-0 — Corrections post-build avant validation humaine — TERMINÉ
- **Statut** : TERMINÉ — 53/53 tests verts (11 nouveaux tests ajoutés).
- **Lot concerné** : APP-0 — corrections ciblées (aucun nouveau module, aucune route métier).
- **Corrections appliquées** :
  1. **Registre pipeline** : chemins corrigés — pointaient vers dossiers `LotX_*/` (MASTER Excel), désormais vers fichiers `.py` réels à la racine de `02_TRAVAIL/`. Lot3 (aucun script confirmé) → `NON_REFERENCES` documenté. 21 scripts actifs confirmés.
  2. **HTMX supprimé** : stub `htmx.2.0.4.min.js` supprimé ; balise `<script>` retirée de `base.html` ; instructions téléchargement retirées de `LANCEMENT_LOCAL.md`. APP-0 fonctionne en HTML classique full-page-reload. HTMX réintégré uniquement quand fichier local validé fourni.
  3. **Navigation sidebar** : 7 menus non construits convertis de `<a href=...>` en `<span class="nav-item--future">` non cliquables avec badge lot cible. Aucune route fantôme.
  4. **Modules accueil** : cartes `a_venir` converties en `<div>` sans `href` — aucun lien cassé.
  5. **CSS** : styles `.nav-item--future` + `.nav-badge-future` ajoutés.
  6. **requirements.txt** : versions figées exactes (fastapi==0.138.2, uvicorn==0.49.0, jinja2==3.1.6, openpyxl==3.1.5, python-multipart==0.0.32, pytest==9.1.1, httpx==0.28.1).
  7. **Statut global** : unifié — suppression contradictions "en attente Stitch" / "code non démarré".
- **Tests ajoutés** :
  - `test_pipeline_registry_paths.py` (5 tests) : tous scripts registre → fichiers existants.
  - `test_navigation_no_404.py` (6 tests) : sidebar sans href cassé, routes futures → 404, routes réelles → 200.
- **Résultats tests** : `pytest tests/ -v` → **53/53 PASSED** (6.46 s).
- **Lancement réel vérifié** : `http://localhost:8000` → 200 ; `/health` → 200 OK ; `/sources-calculs` → 200 ; `/logements` → 404 (attendu).
- **Données réelles** : `git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/` → `nothing to commit`.
- **Entrée JOURNAL_CONTROLES** : CTR-APP0-CORR-2026-07-01 (voir ci-dessous).
- **Prochaine action** : validation humaine explicite avant démarrage APP-1.

#### 2026-07-01 — Lot APP-0 — Socle technique — TERMINÉ
- **Statut** : TERMINÉ / VALIDÉ — 42/42 tests verts.
- **Lot** : APP-0 — Socle technique.
- **Fichiers créés** : `05_APPLICATION/` — 44 fichiers + arborescence complète.
  - `app/main.py` · `app/config.py` · `app/db/connection.py` · `app/db/migrations/0001_init.sql`
  - `app/adapters/pipeline_registry.py` · `app/adapters/pipeline_runner.py`
  - `app/readers/excel_reader.py` · `app/readers/csv_reader.py` · `app/readers/run_log_reader.py`
  - `app/writers/saisie_writer.py` (STUB)
  - `app/services/file_registry.py` · `app/services/snapshot_service.py` · `app/services/audit_service.py`
  - `app/routes/home.py` · `app/routes/sources_calculs.py` · `app/routes/health.py`
  - `app/templates/base.html` · `app/templates/home.html` · `app/templates/sources_calculs.html` · `app/templates/partials/`
  - `app/static/css/app.css` (tokens Stitch complets) · `app/static/img/logo-main.png` (aucun fichier JS tiers — APP-0 en HTML classique)
  - `docs/LANCEMENT_LOCAL.md` · `docs/CARTE_FLUX_DONNEES.md` · `docs/CONTRAT_FICHIERS.md`
  - `tests/conftest.py` + 6 fichiers de test
  - `run_app.py` · `requirements.txt` · `.env.example` · `.gitignore` · `README.md`
- **Tests réalisés** : `pytest tests/ -v` — **42/42 PASSED** (4.30 s).
  - test_boot (7) · test_no_metier_calc (4) · test_pipeline_runner_dryrun (5) · test_readonly_guarantee (13) · test_snapshot (5) · test_sqlite_migrations (5) · test_wal_mode (1) · test_periods_mirror (1)
- **Résultat** :
  - App boot sur `python run_app.py` (localhost:8000).
  - Layout `base.html` : sidebar 9 menus MVP, logo local, police `"Segoe UI"`, tokens CSS Stitch.
  - Page Accueil : 5 KPIs read-only, cartes modules.
  - Page Sources & calculs : dry-run simulé, historique SQLite, journaux MASTER_RUN_Log.
  - Page `/health` : diagnostic complet.
  - SQLite journal : 7 tables, migrations idempotentes, WAL mode.
  - Snapshot : copie + manifeste sha256 + SQLite + restauration en copie isolée.
  - Pipeline runner : dry-run forcé, exécution réelle bloquée par garde explicite.
  - File registry : `assert_writable` refuse tout chemin non-SAISIE.
  - Aucun calcul métier dans le code applicatif (scan vérifié).
- **Anomalies / corrections** : API Starlette 1.3.1 — `TemplateResponse(request, name, ctx)` (corrigé pendant le build).
- **Données réelles** : `git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/` → `nothing to commit`. Aucune source modifiée.
- **Entrée JOURNAL_CONTROLES** : CTR-APP0-2026-07-01 (voir ci-dessous).
- **Décisions restantes** : D-APP-04 (pré-requis APP-1) · D-APP-05 (pré-requis APP-2).
- **Prochaine action** : valider APP-0 → feu vert humain explicite avant démarrage APP-1.

#### 2026-07-01 — Précisions police, Banques & caisse, HTMX — INTÉGRÉES AU PLAN
- **Statut** : PLAN mis à jour (v2 précision) — aucun code démarré.
- **Lot concerné** : préparation APP-0 (§4.1, §4.2 V2, §6.1, §6.9, §8.3, §8.4 du plan).
- **Fichiers touchés** : `PLAN_CONSTRUCTION_APPLICATION_LOCALE.md` ; `ETAT_AVANCEMENT.md` (cette entrée).
- **Tests réalisés** : aucun (pas de code).
- **Résultat** :
  - **Police** : Plus Jakarta Sans retirée des pré-requis APP-0 (fichiers woff2 non fournis). Police effective APP-0 = `"Segoe UI", Arial, sans-serif`. Amélioration future uniquement si fichiers fournis légalement. Aucun téléchargement, aucun woff2 créé, aucun dossier `static/fonts/`.
  - **Banques & caisse** : périmètre MVP précisé — rapprochement bancaire inclus, lecture caisse existante possible si données présentes. Exclut : saisie caisse complète, comptage physique, écart théorique/constaté → V2. Aucun écran vide caisse dans le MVP.
  - **HTMX** : repoussé hors APP-0 — sera intégré uniquement quand un fichier local exact, vérifié et validé sera fourni. APP-0 fonctionne en HTML classique full-page-reload. Aucun CDN, aucun stub, aucun téléchargement demandé.
- **Décisions restantes** : D-APP-04 (pré-requis APP-1) · D-APP-05 (pré-requis APP-2).
- **Prochaine action** : valider → démarrer le code du Lot APP-0. Aucun code avant validation humaine explicite.

#### 2026-07-01 — Arbitrages Stitch, navigation et stack — INTÉGRÉS AU PLAN
- **Statut** : PLAN mis à jour (v2) — aucun code démarré.
- **Lot concerné** : préparation APP-0 (impacts sur §2/§4/§6/§8/§9 du plan).
- **Fichiers touchés** : `PLAN_CONSTRUCTION_APPLICATION_LOCALE.md` (v2) ; `ETAT_AVANCEMENT.md` (cette entrée).
- **Tests réalisés** : aucun (pas de code).
- **Résultat** :
  - Dossier `stitch/` analysé : 12 écrans HTML+PNG, 2 DESIGN.md, 1 logo PNG.
  - Design system Chouette Patrimoine retenu comme référence visuelle locale (police Plus Jakarta Sans, palette teal #003441, tokens couleur complets).
  - **Calendrier retiré du MVP** et hors V2 planifiée — idée future non cadrée, aucun point d'extension.
  - **Navigation MVP figée à 9 menus** : Accueil · Logements · Propriétaires & règlements · Réservations · Fournisseurs · Banques & caisse · Ménages · Sources & calculs · Contrôles & clôture.
  - Stack confirmé : FastAPI · Jinja2 · HTMX · SQLite. **Pas de Tailwind, pas de React, pas de Node, pas de Docker, pas de PostgreSQL, pas de CDN réseau** (ni Google Fonts, ni Material Symbols, ni Tailwind CDN).
  - Police APP-0 = fallback système (`"Segoe UI", Arial`). Plus Jakarta Sans = cible future si fichiers fournis. Icônes SVG locales. CSS pur `app.css`.
  - Logo : `logo-main.png` local + texte « CHOUETTE » / « PATRIMOINE ».
  - 8 corrections Stitch documentées (§8.6 plan) — à appliquer dans le code, pas dans Stitch.
  - Corrections majeures : couleur active sidebar (teal, pas violet) ; clôture avec guard obligatoire ; progression clôture 9 blocs ; logs pipeline depuis SQLite.
- **Décisions actées** : D-APP-01 ✓ · D-APP-02 ✓ · D-APP-03 ✓ · D-APP-06 ✓ · D-STITCH-01 ✓ (Calendrier hors MVP) · D-STITCH-02 ✓ (fallback système APP-0 ; Plus Jakarta Sans si fichiers fournis) · D-STITCH-03 ✓ (pas de CDN) · D-STITCH-04 ✓ (tokens sémantiques) · D-STITCH-05 ✓ (logo local).
- **Décisions restantes** : D-APP-04 (source unique 3 doublons, pré-requis APP-1) · D-APP-05 (contrat écriture SAISIE_*, pré-requis APP-2).
- **Anomalies / risques** : MCP Stitch auth KO — non bloquant. Écrans Stitch absents pour fiches détail métier (à créer ex nihilo aux lots APP-1→3).
- **Prochaine action** : valider ce résumé → démarrer le code du Lot APP-0. **Aucun code avant validation humaine explicite.**

#### 2026-06-30 — Lot APP-0 (Socle technique) — EN ATTENTE STITCH
- **Statut** : PLANIFIÉ / EN_ATTENTE_PRODUCTIONS_STITCH (non démarré côté code).
- **Lot concerné** : APP-0 — Socle technique (FastAPI · Jinja2/HTMX · SQLite · `05_APPLICATION/` · sans Docker/PostgreSQL/worker).
- **Fichiers touchés** : `00_CADRAGE/APPLICATION_LOCALE/PLAN_CONSTRUCTION_APPLICATION_LOCALE.md` (créé) ; `00_CADRAGE/ETAT_AVANCEMENT.md` (cette section). Aucun fichier technique/applicatif.
- **Tests réalisés** : aucun (pas de code à ce stade).
- **Résultat** : plan rédigé et validé ; A1-A7 actés ; MVP 9 modules + V2 figés (sans table ni écran V2 avant validation moteurs) ; APP-0→APP-6 définis.
- **Décisions actées** : D-APP-01 `05_APPLICATION/` ✓ · D-APP-02 scope snapshot ✓ · D-APP-03 dry-run Lot 0 / exécution réelle à terme ✓ · D-APP-06 Stitch manuel, MCP optionnel ✓.
- **Décisions restantes** : D-APP-04 (source unique 3 doublons, pré-requis APP-1) · D-APP-05 (contrat écriture SAISIE_*, pré-requis APP-2).
- **Anomalies / risques** : MCP Stitch auth KO — non bloquant. Doc moteur périmée — hors périmètre app.
- **Prochaine action** : analyser productions Stitch → relever contradictions → adapter plan → validation humaine → démarrer code Lot APP-0.

---

## Dernière mise à jour
Date : 2026-07-03
Session : Session 28 — APP-2b implémentée techniquement — 149 tests verts — HH_REAL_WRITE_ENABLED = False
Agent : Claude Code (claude-sonnet-4-6)

---

## Lot en cours
Lot : 11 — Contrôles de cohérence globaux
Statut : **EN_ATTENTE_VALIDATION_HUMAINE** — 51 contrôles générés (CTR-2026-06-020), commit non fait

> Contrôles inscrits :
> - Lot 0 : CTR-2026-06-001 (audit initial), CTR-2026-06-002 (corrections), CTR-2026-06-003 (post-correction — tout vert)
> - Lot 1 : CTR-2026-06-004 (extraction + payout validés — 2026-06-08)
> - Lot 2 : CTR-2026-06-005 (mapping logements — 17/17 OK — validé humainement 2026-06-08)
> - Lot 11 : CTR-2026-06-020 (contrôles globaux — 0 BLOQUANT / 44 A_CONTROLER / 7 INFO — après audit F1/F2)
> - Lot 3 FAIT (2026-06-08) : REF_Setup.xlsm mis à jour (5 onglets). SAISIE_Charges_Flux.xlsx créé (4 onglets, 31 cols, 18 DV, 13 contrôles). MASTER_FACT_MAN_Charges.xlsx créé (37 cols, 4 requêtes PQ). CTR-2026-06-006 inscrit.
> - Lot 4 (2026-06-09) : SAISIE_ReservationsHorsHostaway.xlsx créé (4 onglets, 30 cols, 11 DV, 13 contrôles, VLOOKUP taux). MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx créé (34 cols, 3 requêtes PQ). CTR-2026-06-007 inscrit. Décisions D046–D051 verrouillées.
> - Lot 4bis squelette (2026-06-09) : MASTER_CALC_Reservations.xlsx créé (3 onglets, 24 cols, 7 requêtes PQ, anti-double-comptage 7 scénarios, 2 BLOQUANTS + 6 A_CONTROLER). CTR-2026-06-008 inscrit. Décisions D052–D057 verrouillées.
> - Lot 4bis correctif FAIT (2026-06-11) : MASTER_CALC_Reservations.xlsx peupl? par script Python (1 391 lignes MASTER / 1 321 VUE_FLUX). CTR-2026-06-016 inscrit. Commit 3835f21. R?gle date_sortie_gestion valid?e ? cette date, puis d?commissionn?e le 2026-06-29 au profit exclusif de REF_Gestion_Logements_Hist.
> - Lot 5 (2026-06-09) : SAISIE_AcomptesProprietaires.xlsx créé (4 onglets, 18 cols, 5 DV, 10 contrôles). MASTER_FACT_MAN_AcomptesProprietaires.xlsx créé (22 cols, 5 requêtes PQ). REF_Setup.xlsm non modifié (TYPE_FLUX_006 déjà présent). CTR-2026-06-009 inscrit. Décisions D058–D064 verrouillées.
> - Lot 6a (2026-06-09) : MASTER_FACT_HA_CleaningTasks_Discovery.xlsx peuplé (4 onglets : data 500 tâches / MASTER_ENRICHI 21 cols / VUE_COMPTAGE 11 cols / POWER_QUERY_CODE). 0 BLOQUANT, 325 ménages réalisés. CTR-2026-06-010 inscrit. Décisions D065–D069 verrouillées.
> - Lot 9 (2026-06-11) : MASTER_CALC_Flux.xlsx créé (1 333 flux, 22 cols). TYPE_FLUX_017 créé. CTR-2026-06-017 inscrit. EN_ATTENTE_VALIDATION_HUMAINE.

**Points résiduels non bloquants à traiter dans les lots suivants :**
- `CARTE_002` suffixe `XXXX` (carte Ewan) → à renseigner au **Lot 8** avant traitement des exports bancaires.
- `mode_facturation = A_DEFINIR` pour tous les propriétaires → à définir au **Lot 12** avant facturation.
- CleaningTasks FAIT au Lot 6a (500 tâches, 325 réalisés, 0 BLOQUANT).
- 59 réservations A_CONTROLER (32 VRBO + 27 DIRECT) → saisie manuelle aux **Lots 4 / 4bis**.

---

## Note d'état — Bascule nouvelle société (D-LOT-PROD-01, 2026-06-15)

- Pipeline Lot 0 → Lot 12 construit et commité, dernier commit validé : `94dc959 — Lot 12 FAIT - Prefactures proprietaires`.
- **Nouvelle société en cours d'immatriculation.**
- **Point de bascule société non encore défini.**
- **Priorité actuelle** : vérifier que tout le système fonctionne **avant** toute purge.
- **Plus tard** : conservation de l'historique de performance financière (réservations, payouts, commissions, net propriétaire, résultats par logement / propriétaire / mois, référentiels).
- **Plus tard** : remise à zéro / purge contrôlée des données bancaires et comptables de l'ancienne structure (imports bancaires, rapprochements, clôtures banque, soldes, charges comptables, acomptes / règlements bancaires, justificatifs sensibles).
- **À fournir plus tard par l'utilisateur** : date de bascule (`DATE_BASCULE_SOCIETE`), solde initial banque (`SOLDE_INITIAL_BANQUE`), coordonnées définitives société (nom légal, SIRET, RCS, adresse, TVA intracom, IBAN, logo).
- **Aucune purge autorisée maintenant.** Toute purge future : uniquement après backup / snapshot vérifié + validation humaine explicite.
- Détail : voir D-LOT-PROD-01 (DECISIONS_METIER.md) et ARCHITECTURE_DONNEES.md §1.2.

---

## Ce qui est terminé

- **Fichiers de cadrage mis à jour et cohérents (Session 2, pack V2)** :
  - CLAUDE.md, README_PROJET.md, REGLES_METIER.md, ARCHITECTURE_DONNEES.md, PLAN_CONSTRUCTION.md
  - ETAT_AVANCEMENT.md, DECISIONS_METIER.md, JOURNAL_CONTROLES.md, JOURNAL_ANOMALIES.md
- **Lot 0 — REF_Setup FAIT (2026-06-07)** :
  - Audit réel exécuté sur `REF_Setup.xlsm` (19 onglets, 0 doublon clé, 0 date série brute).
  - 6 corrections bloquantes appliquées (B1 mojibake, B2 statuts, B3 REF_Statuts_Payout, B4 REF_Cloture_Mensuelle, B5 paramètres, B6 intervenants).
  - Codes hors-parc APPARTEMENT_DIVERS + LOGEMENT_DIVERS ajoutés dans REF_Logements.
  - Fichier déplacé vers chemin canonique : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`.
  - Sauvegarde : `99_ARCHIVES/LOT0_REF_Setup/REF_Setup_BACKUP_20260607_113223.xlsx.xlsm`.
  - Contrôle inscrit : CTR-2026-06-003 (audit post-correction tout vert).
  - Décisions métier tranchées : QM1 (coûts standards = exécution seule, D037 confirmé), QM2 (A_DEFINIR OK), QM3 (CHG_012 reste A_CONTROLER).
- **Lot 1 — Module Hostaway FAIT (2026-06-08)** : extraction validée (run 20260608_134253 — 1391 réservations traitées, 17 listings, 55 anomalies A_CONTROLER, 0 BLOQUANT). Fix financeField appliqué (`_ff_from_res`). Payout : 1321 NORMAL, 59 A_CONTROLER (32 VRBO + 27 DIRECT). Contrôle inscrit : CTR-2026-06-004. CleaningTasks SKIPPED → Lot 6a.
- **Lot 2 — Mapping logements FAIT (2026-06-08)** : réconciliation REF_Logements ↔ MASTER_REF_HA_Listings. 17/17 listings mappés, 0 orphelin. ANO-001/005/006/014 CORRIGÉES par correction de mapping (pas de création de logement). LOG_0009 : hid 497801→556954, nom "T3 Montaudran". LOG_0016 : hid 480780→515523, sur_hostaway NON→OUI. 5 nouvelles lignes REF_Mapping (MAP_LOG_0082–0086, total 86 lignes). Anciens IDs 480780 et 497801 conservés comme alias historiques actifs (actif=OUI). Contrôle inscrit : CTR-2026-06-005. Validé humainement 2026-06-08.
- **Lot 3 — SAISIE_Charges_Flux FAIT (2026-06-08)** :
  - REF_Setup.xlsm mis à jour : REF_Categories_Charges (+filtre_vue_menage +CHG_021/022/023), REF_Types_Flux (+TYPE_FLUX_009-012), REF_Types_Affectation (+AFF_GLOBAL/NON_AFFECTABLE), REF_Statuts (D044 : STAT_022 désactivé, STAT_024-029), REF_Charges_Recurrentes (nouvel onglet, REC_001/002). Backup : 99_ARCHIVES/LOT3_Charges/REF_Setup_BACKUP_20260608_191019.xlsm.
  - SAISIE_Charges_Flux.xlsx créé (01_SOURCES_BRUTES/Charges/) : 31 colonnes, 18 listes déroulantes, formules calculées, 13 contrôles, MFC niveau_anomalie.
  - MASTER_FACT_MAN_Charges.xlsx créé (02_TRAVAIL/Lot3_Charges/) : 37 colonnes (31 SAISIE + sens/filtre_vue_menage/source_module/source_table/source_pk/date_integration). VUE_MENAGE = filtre filtre_vue_menage=OUI AND statut_controle=VALIDE. 4 requêtes M-code Power Query.
  - Décisions verrouillées : D044 (séparation statut_controle/niveau_anomalie), D045 (REF_Charges_Recurrentes paramétrable). REFACTURATION→sens=CHARGE validé.
  - Contrôle inscrit : CTR-2026-06-006 (21 points, 0 FAIL). Validé humainement 2026-06-08.
- **Lot 4 — Réservations hors Hostaway FAIT (2026-06-09)** :
  - SAISIE_ReservationsHorsHostaway.xlsx créé (4 onglets, 30 cols, 11 DV, 13 contrôles, VLOOKUP taux_commission). Chemin : `01_SOURCES_BRUTES/ReservationsHH/`.
  - MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx créé (3 onglets, 34 cols, 3 requêtes PQ). Chemin : `02_TRAVAIL/Lot4_ReservationsHH/`.
  - REF_Setup.xlsm non modifié (V1+V2 OK). Contrôle inscrit : CTR-2026-06-007. Décisions D046–D051.
- **Lot 4bis — Table commune réservations FAIT (2026-06-09)** :
  - MASTER_CALC_Reservations.xlsx créé (3 onglets, 24 cols, 7 requêtes PQ). Chemin : `02_TRAVAIL/Lot4bis_TableCommune/`.
  - Anti-double-comptage 7 scénarios. 2 BLOQUANTS + 6 A_CONTROLER détectés en PQ. VUE_FLUX filtre VALIDE+OUI+montant≠0.
  - Contrôle inscrit : CTR-2026-06-008. Décisions D052–D057.
- **Lot 5 — Acomptes propriétaires FAIT (2026-06-09)** :
  - SAISIE_AcomptesProprietaires.xlsx créé (4 onglets, 18 cols, 5 DV, 10 contrôles). Chemin : `01_SOURCES_BRUTES/AcomptesProprietaires/`.
  - MASTER_FACT_MAN_AcomptesProprietaires.xlsx créé (3 onglets, 22 cols, 5 requêtes PQ). Chemin : `02_TRAVAIL/Lot5_AcomptesProprietaires/`.
  - REF_Setup.xlsm non modifié : TYPE_FLUX_006 (ACOMPTE_FACTURE_PROPRIETAIRE) déjà présent.
  - report_mois_suivant supprimé (D061). source_pk = acompte_id toujours (D064). TYPE_FLUX_013 non créé.
  - Contrôle inscrit : CTR-2026-06-009. Décisions D058–D064.
- **Lot 6a — Hostaway CleaningTasks comptage ménages FAIT (2026-06-09)** :
  - MASTER_FACT_HA_CleaningTasks_Discovery.xlsx peuplé (4 onglets). Chemin : `02_TRAVAIL/Lot1_Hostaway/`.
  - data (11 cols, 500 tâches brutes, H6 cost=NULL). MASTER_ENRICHI (21 cols, 0 BLOQUANT, 39 A_CONTROLER).
  - VUE_COMPTAGE (11 cols, 95 lignes mois×logement, 325 ménages réalisés). POWER_QUERY_CODE (Q1–Q4).
  - D065 : API /v1/tasks = 500 max, single call. D066 : Jan 2026 absent = normal.
  - D067 : confirmed=prévu. D068 : logement inactif=A_CONTROLER. D069 : TLM_001 par défaut.
  - Contrôle inscrit : CTR-2026-06-010. Décisions D065–D069.
- **Lot 6c — Ménages externes FAIT (2026-06-09)** :
  - MASTER_FACT_MEN_MenagesExternes.xlsx créé (7 onglets, 49 cols). Chemin : `02_TRAVAIL/Lot6c_MenagesExternes/`.
  - SOURCE_RAW : 13 lignes (9 Aissata fac.2026-37 / 4 Mounir fac.0003 — mai 2026). Ligne 8 splittée en 8a/8b.
  - MASTER : 13 lignes — 4 VALIDE / 9 A_CONTROLER / 0 BLOQUANT.
  - VUE_ACTIVE : 4 lignes (lignes avec date_menage précise). 9 lignes A_CONTROLER = MENAGE_EXTERNE_DATE_ABSENTE.
  - VUE_ECART_HOSTAWAY : 16 logements mois=2026-05.
  - Réconciliation : FAC-2026-05-AISSATA-001 écart=0,00€ / FAC-2026-05-MOUNIR-001 écart=0,00€ — VALIDE.
  - REF_Setup.xlsm : TYPE_FLUX_014 (COUT_REEL_MENAGE_EXTERNE) ajouté. REF_Intervenants +3 colonnes (nom_legal, siret_rcs, email_facturation).
  - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm.
  - FRANCHISE_TVA confirmé : Aissata et Mounir. taux_tva=0, HT=TTC.
  - Kandia DIABATE = INT_0004 (Aissata) / MH Entreprise = INT_0003 (Mounir).
  - CTR-2026-06-012 inscrit. Décisions D079–D088.
- **Lot 6b — M04 Ménages internes FAIT (2026-06-09)** :
  - M04_MENAGES_PowerQuery.xlsx créé (squelette). Chemin : `02_DONNEES_NORMALISEES/menages/`.
  - 8 onglets : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 / MASTER (34 cols) / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE (10 requêtes) / README.
  - PARAM_TAUX : INT_0001 Imène 10€, INT_0002 Kheira 10€, INT_0003/0004/0005 EXTERNE.
  - PARAMETRES_M04 : SEUIL_ECART_STANDARD_MENAGE=10 paramétrable.
  - REF_Setup.xlsm : TYPE_FLUX_013 (COUT_MO_INTERNE_MENAGE) ajouté. Backup : 99_ARCHIVES/LOT6B_Menages/REF_Setup_BACKUP_20260609_152826.xlsm.
  - Clé répartition future : COUT_STANDARD_MENAGES_MOIS (non NOMBRE_MENAGES). cout_standard_total_ligne = base pondération, non comptable (D076).
  - Contrôle inscrit : CTR-2026-06-011. Décisions D070–D078.
  - SOURCE_RAW vide — coller données GSheet + adapter chemins PQ avant premier run.
  - REC_002 cle_repartition mise à jour : COUT_STANDARD_MENAGES_MOIS (ancienne valeur : NOMBRE_MENAGES, D076, validation humaine Lot 6b).

> **Règle D029 — IRRÉVOCABLE** : aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES.

---

## Ce qui a été modifié (session 22 — Lot 12 Préfactures propriétaires, 2026-06-14)

**Objectif :** produire les préfactures propriétaires (12 lignes §17.3) depuis les sorties Lot 10/11.
Lot 12 = lot **terminal** : lit seulement les sorties Lot 10/11, ne recalcule rien, n'écrit que dans Lot12_Factures/.

- Créé : `02_TRAVAIL/lot12_generer_factures.py`
- Créé : `02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx` (5 onglets)
  - FACT_FACTURE_ENTETE / FACT_FACTURE_LIGNES / CONTROLE_MENSUEL / DASHBOARD_FACTURATION / A_CONTROLER
- Modifié : `00_CADRAGE/JOURNAL_CONTROLES.md` (CTR-2026-06-023)
- Modifié : `00_CADRAGE/ETAT_AVANCEMENT.md` (ce fichier — session 22)

**Résultat :**
- 269 préfactures / 3 228 lignes (12 lignes par préfacture)
- 0 facture finale ; statut_generation PREFACTURE_CONTROLE ; statut_facture NON_FACTURABLE_A_CONTROLER (269/269)
- Balises visibles si données manquantes (logo/SIRET/adresse société/mode_facturation/banque/...)
- GLOBAL_NON_AFFECTE exclu des factures propriétaires (frais bancaires société, hors REGLEMENT)
- Granularité prop × logement × mois ; PREF-AAAA-MM-PROP-LOG-NNN
- 0 BLOQUANT / 60 A_CONTROLER
- Test fictif complet validé (HH visibles, ACOMPTE_001 en règlement, ACOMPTE_002 A_CONTROLER exclu, 0 finale)
- 0 fictif résiduel ; 11 xlsx données restaurés à l'état commité ; sources intactes

**Décisions appliquées :** D-LOT12-01 à D-LOT12-08.

**Limites (préfactures uniquement — aucune finale tant que facturation_lot12_ok ≠ OUI) :**
44 A_CONTROLER Lot 11 · 12 modes facturation à définir · banque non clôturée ·
sources réelles M04/Charges/IK/Acomptes partielles ou vides · coords société/logo/SIRET sous balises.

**Fichiers à ne SURTOUT PAS committer :**
- `99_ARCHIVES/LOT12_TEST_DATA/` (backups fictif — ignoré git)
- Sources brutes · fichiers banque · `REF_Setup.xlsm` · tout xlsx de données amont

---

## Ce qui a été modifié (session 21 — Correctif Lot 10 HH / HC / charges globales / acomptes, 2026-06-14)

**Objectif :** corriger les 4 défauts Lot 10 révélés par les tests fictifs (CTR-2026-06-021),
pour que le pipeline supporte les nouveaux flux (HH, HC, charges globales, acomptes).

- Modifié : `02_TRAVAIL/lot10_calculer_resultats.py`
  - Défaut #1 HH : routage HA/HH ; HH via montant saisi (total_percu/menage/taux), commission `(total_percu−menage)×taux` ; `JOINTURE_PAYOUT_MANQUANTE` limité à Hostaway
  - Défaut #2 HC : GLOBAL HORS_COMPTA calculé depuis df_hc (plus codé à 0) ; contrôle REEL=COMPTABLE+HC
  - Défaut #3 charges globales : sentinelle GLOBAL_NON_AFFECTE (plus de drop silencieux)
  - Défaut #4 acomptes : lus depuis Lot 5, injectés REGLEMENT uniquement (jamais exploitation)
  - 2 nouvelles sources lues : MASTER_FACT_MAN_ReservationsHorsHostaway, MASTER_FACT_MAN_AcomptesProprietaires
  - Contrôles ajoutés : CTR-LOT10-20 à 24
- Modifié : `02_TRAVAIL/lot12_seed_donnees_fictives.py` (HH réactivé + acomptes ajoutés)
- Modifié : `02_TRAVAIL/lot12_remove_donnees_fictives.py` (cible Acomptes ajoutée)
- Modifié : `00_CADRAGE/JOURNAL_CONTROLES.md` (CTR-2026-06-022)
- Modifié : `00_CADRAGE/ETAT_AVANCEMENT.md` (ce fichier — session 21)

**Décisions appliquées :** D-LOT10C-01 à D-LOT10C-05 (validées 2026-06-14).

**CHANGEMENT DE BASELINE (justifié — correction du défaut #3) :**
- Ancien REEL : 283 515,60 € / Nouveau REEL : 283 442,51 € / Écart : 73,09 €
- 8 frais bancaires (TYPE_FLUX_016, sans logement/proprietaire) auparavant perdus silencieusement,
  désormais visibles dans GLOBAL_NON_AFFECTE. PAS une régression.
- Correction doc : l'ancien « 194 € = 4 ménages + 8 frais bancaires » de CTR-2026-06-019 était faux ;
  194 € = 4 ménages seuls ; 73,09 € de frais bancaires étaient perdus.

**Résultats audit :**
- Changement de baseline justifié (73,09 € = 8 frais bancaires droppés)
- Non-régression Hostaway confirmée (1321 NORMAL, payout 283 709,60, ménage 55 619,00,
  assiette 228 090,60, commission 39 167,42, net 188 923,18 — inchangés)
- HH fictives testées sans blocage payout (NORMAL 1323 = HA 1321 + HH 2)
- HC global corrigé (HORS_COMPTA −444,44 avec fictif, REEL=COMPTABLE+HC OK)
- Charges globales visibles (GLOBAL_NON_AFFECTE)
- Acomptes uniquement dans REGLEMENT (222,22 € ; ACOMPTE_002 A_CONTROLER exclu)
- 0 fictif résiduel (scan 30 xlsx suivis)
- 0 Excel suivi modifié (11 xlsx données restaurés à l'état commité)

**Fichiers à ne SURTOUT PAS committer :**
- `99_ARCHIVES/LOT12_TEST_DATA/` (backups fictif — ignoré git)
- Tous les fichiers Excel de données (sorties Lot 10 régénérées seulement après validation/commit)

**À retenir :** les sorties Excel Lot 10 commitées portent encore l'ancien REEL 283 515,60 (buggé) ;
elles seront régénérées avec 283 442,51 quand le lot10 corrigé sera exécuté après commit.

---

## Ce qui a été modifié (session 20 — Correctif Lot 9 ingestion Charges/M04 + infra test Lot 12, 2026-06-14)

**Objectif :** câbler dans le Flux les sources jusqu'ici absentes (Charges, M04) + créer une
infrastructure de données fictives pour tester le pipeline jusqu'au Lot 12.

- Modifié : `02_TRAVAIL/lot9_construire_flux.py`
  - Ingestion Charges (Lot 3, VALIDE) — sens/code_impact/type portés par la ligne
  - Ingestion M04 ménages internes (Lot 6b, VALIDE, HC, TYPE_FLUX_013)
  - IK exclu du Flux (décision « IK hors Flux / vue dédiée », §15.3)
  - Acomptes différés (passe Lot 10)
  - Garde-fou CTR-9-011 : portion RES (TYPE_FLUX_017) inchangée
- Créé : `02_TRAVAIL/lot12_seed_donnees_fictives.py` (seed idempotent + backup + tag obligatoire)
- Créé : `02_TRAVAIL/lot12_remove_donnees_fictives.py` (suppression par ID ZZ_TEST_ / tag + vérif 0 résiduel)
- Modifié : `.gitignore` (ignore `99_ARCHIVES/LOT12_TEST_DATA/` — backups fictif, règle #10)
- Modifié : `00_CADRAGE/JOURNAL_CONTROLES.md` (CTR-2026-06-021)
- Modifié : `00_CADRAGE/ETAT_AVANCEMENT.md` (ce fichier — session 20)

**Fichiers à ne SURTOUT PAS committer :**
- `99_ARCHIVES/LOT12_TEST_DATA/` (backups contenant du fictif — déjà ignoré git)
- Tous les fichiers Excel de données (Flux, Resultats, Commissions, NetProprietaire, CTRL_Coherence,
  Charges, M04, MenExt, ReservationsHH, Reservations4bis, REF_Setup.xlsm, banque)

**Résultat audit (avant commit) :**
- 0 donnée fictive résiduelle (scan ZZ_TEST_ / tag sur 30 Excel suivis)
- 0 Excel suivi modifié (git diff = uniquement .gitignore + lot9_construire_flux.py + docs)
- Sources amont non modifiées (lecture seule)
- Correctif Lot 9 **neutre** tant que Charges/M04 sont vides (baseline 1 333 inchangée)

**Test fictif réalisé :** baseline 1 333 → avec fictif 1 340 → après suppression 1 333 → 0 résiduel.

**Findings Lot 10 à traiter ensuite (passe Lot 10) :**
1. HH sans payout Hostaway → gérer par montant saisi (pas payout)
2. Lot 10 GLOBAL HORS_COMPTA reste 0 même avec flux HC → casse REEL = COMPTABLE + HC
3. Charges globales / non affectables (sans logement_id) perdues dans l'agrégation
4. Acomptes uniquement dans le bloc REGLEMENT, jamais dans l'exploitation (D031/D032/D033)

**PRÉREQUIS BLOQUANT :** ne pas peupler Charges/M04 avec des données réelles HC avant correctif Lot 10.

---

## Ce qui a été modifié (session 19 — Lot 11 Contrôles de cohérence globaux, 2026-06-14)

- Script créé : `02_TRAVAIL/lot11_controles_coherence.py`
  - Banque adaptative (D-LOT11-01 Option C) — 132 lignes NORM_Banque lues localement
  - Re-vérification indépendante (D-LOT11-04) — pas de reprise Lot 10
  - 6 onglets produits : MASTER / BLOQUANTS_OUVERTS / A_CONTROLER_OUVERTS / DASHBOARD_MOIS / RAPPROCHEMENT_PAYOUT_BANQUE / CAISSE_THEORIQUE
- Créé : `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx`
  - MASTER : 51 contrôles générés
  - BLOQUANTS_OUVERTS : **0 ligne** — 0 bloquant
  - A_CONTROLER_OUVERTS : 44 lignes
  - DASHBOARD_MOIS : 4 lignes (2026-02/03/04 + TRANSVERSE) — mois vides arbitraires supprimés (F3)
  - RAPPROCHEMENT_PAYOUT_BANQUE : 3 mois (indicatif — écarts normaux)
  - CAISSE_THEORIQUE : 6 lignes avec flag CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES
- Corrections post-audit (2026-06-14) :
  - F1 : `facturation_lot12_ok` = OUI uniquement si 0 BLOQUANT + 0 A_CONTROLER + mois banque CLOTURE.
    Résultat : AUCUN mois facturable (tout NON_SOUS_RESERVE_A_CONTROLER).
  - F2 : détection placeholder Power Query → CHARGES/M04/IK/HH correctement vides → INFO 3 → 7.
  - F3 : code mort supprimé, print dynamique, DASHBOARD sans mois vides.
- Chiffres clés :
  - 0 BLOQUANT — aucun blocage structurel
  - 44 A_CONTROLER (23 LISTING_ORPHELIN + 14 CHARGE_FIXE_DATE + 3 CLOTURE_BANQUE + 4 autres)
  - 7 INFO (HC_ZERO_SOURCES_VIDES ×6 [CHARGES/M04/ACOMPTES/IK/HH/TRANSVERSE] + MODE_FACTURATION_A_DEFINIR)
  - REEL = COMPTABLE + HORS_COMPTA = 283 515.60 + 0 = 283 515.60 € ✓
  - Banque : BANQUE_DISPONIBLE — 52 lignes RAPPROCHEMENT_REQUIS sur 3 mois
  - Caisse théorique : 0 € (sources HH/Charges/Acomptes vides)
  - facturation_lot12_ok : NON sur tous les mois (0 OUI)
- Sources amont : aucune modification (toutes read_only=True)
- Commit non fait — EN_ATTENTE_VALIDATION_HUMAINE

---

## Ce qui a été modifié (session 18 — Lot 10 Résultats / commissions / net propriétaire, 2026-06-13)

- Script créé : `02_TRAVAIL/lot10_calculer_resultats.py`
  - Jointure confirmée : Flux.source_pk → Reservations.reservation_calc_id → reservation_id_hostaway → Payout.reservation_id
  - 6 contrôles BLOQUANTS / 4 contrôles A_CONTROLER / 19 points CTR rapport
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx`
  - Onglet COMMISSIONS : 1 321 réservations NORMAL (Airbnb + Booking)
  - Onglet A_CONTROLER : 59 réservations exclues (VRBO + Direct)
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx`
  - PAR_MOIS_LOGEMENT : 497 lignes total (248 REEL + 248 COMPTABLE + 1 HORS_COMPTA placeholder)
  - PAR_MOIS_PROPRIETAIRE : 404 lignes total (202 REEL + 202 COMPTABLE)
  - GLOBAL : 3 lignes (1 REEL + 1 COMPTABLE + 1 HORS_COMPTA) — REEL = COMPTABLE = 283 515.60 € / HC = 0 (sources vides)
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx`
  - EXPLOITATION : 1 321 réservations, charge_fixe=0 par réservation
  - REGLEMENT : 269 lignes mois × logement, charge fixe Option A
  - VUE_MOIS : 220 lignes mois × propriétaire
- Charge fixe mensuelle (Option A, D-LOT10-04 validé) :
  - 233 lignes générées / 13 logements avec forfait > 0
  - 12 logements flagg?s CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE (trace historique : ancien contr?le bas? sur date_entree_gestion, supprim? le 2026-06-29 avec les colonnes doublons)
  - 1 logement flaggé LOG_SANS_FLUX_017 : LOG_0009 (forfait=40€ mais 0 réservation TYPE_FLUX_017)
  - Total charge fixe générée : 7 645.00 €
- Chiffres clés :
  - Total payout NORMAL : 283 709.60 €
  - Total ménage retenu : 55 619.00 €
  - Total assiette commission : 228 090.60 €
  - Total commission conciergerie : 39 167.42 €
  - Total net proprio avant charge fixe : 188 923.18 €
  - Total net proprio après charge fixe : 181 278.18 €
  - Résultat REEL global (Flux) : 283 515.60 €
  - Résultat HORS_COMPTA : 0 € (sources vides — M04, Charges, IK non alimentés)
- 0 BLOQUANT détecté — CTR-2026-06-019 inscrit
- Sources amont non modifiées (Flux, Reservations, Payout, REF_Setup — lecture seule)
- Commit non fait — EN_ATTENTE_VALIDATION_HUMAINE

---

## Ce qui a été modifié (session 17 — Correctif Lot 1 Payout final, 2026-06-13)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-018 inscrit — correctif final date-aware + REF historique)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 17, correctif Lot 1 final)
- Cadrage : `.gitignore` (99_ARCHIVES/LOT1_Hostaway/ ajouté)
- Référentiel : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  - REF_Couts_Standards_Menage : +5 lignes historiques 2025 (COUT_STD_2025_TYPE_001–005)
  - Période : 2025-01-01 → 2025-12-31 / Montants identiques 2026 (29/39/55/69/110€)
  - Lignes 2026 (COUT_MEN_001–005) inchangées. Aucun chevauchement.
  - Backup : `99_ARCHIVES/REF_Setup_BACKUP_20260613_175507.xlsm`
- Script refactoré : `02_TRAVAIL/lot1_hostaway_extract.py`
  - `_build_cost_ref_df()` : colonnes date_debut_validite + date_fin_validite + cout_standard_id
  - `load_menage_cost_ref()` : retourne DataFrame (plus dict simple)
  - `_lookup_menage_by_date()` : sélection date-aware + détection doublons BLOQUANT
  - `PayoutCalculator` : refactoring complet → 5-tuples + meta_dict traçabilité
  - `_META_NON_APPLICABLE` : traçabilité annulations / VRBO / Direct
  - `recalc_payout_only()` : date-aware par ligne via checkInDate + 8 colonnes tracabilité
  - Anomalies : COUT_STANDARD_MENAGE_ABSENT, COUT_STANDARD_MENAGE_DOUBLON_VALIDITE (BLOQUANT)
  - Mode `--recalc-payout-only --payout-source <path>` conservé
- Recalculé (correctif final) : `02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx`
  - Source : backup propre MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
  - 1 235 lignes Airbnb NORMAL + 86 lignes Booking NORMAL corrigées
  - menage_retenu Airbnb : 0.00 € → 51 727.00 € (REF_Setup date-aware)
  - menage_retenu Booking : 3 810.00 € → 3 892.00 € (REF_Setup date-aware, delta +82€)
  - assiette Airbnb : 263 043.22 € → 211 316.22 €
  - assiette Booking : 16 856.38 € → 16 774.38 €
  - 8 colonnes traçabilité ajoutées (menage_retenu_source, cout_standard_id, snapshot, dates, logement_id, type_id, date_reference)
  - CTR-15 doublons validité : 0 / CTR-16 Airbnb avec cout_std_id : 1235/1235 / CTR-17 Booking : 86/86
  - CTR-18 snapshot==menage_retenu : 1321/1321 / CTR-19 date_reference : 1321/1321
  - 0 ligne sans cout_standard [OK] / Annulations intactes [OK] (D030)
  - Impact estimé commissions (~15%) : −7 771.35 € (taux réels à confirmer Lot 10)
- Backup pré-correctif : `99_ARCHIVES/LOT1_Hostaway/MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx`

Note progression correctifs :
  #1 cleaningFee_res : invalidé (prix voyageur ≠ coût standard).
  #2 REF_Setup sans dates : insuffisant (814 réservations 2025 sans cout_standard).
  Final : date-aware + REF historique 2025 → 0 manquant, traçabilité complète.

---

## Ce qui a été modifié (session 16 — Lot 9, 2026-06-11)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-017 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 16, Lot 9)
- Cadrage : `.gitignore` (ajout `99_ARCHIVES/LOT9_FluxUnifie/`)
- Référentiel : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  (TYPE_FLUX_017 = REVENU_RESERVATION_HOSTAWAY ajouté — onglet REF_Types_Flux)
- Backup REF_Setup : `99_ARCHIVES/LOT9_FluxUnifie/REF_Setup_backup_lot9_20260611_213323.xlsm`
- Script créé : `02_TRAVAIL/lot9_construire_flux.py`
  (construit MASTER_CALC_Flux depuis 3 sources : RES 1321 + MEN 4 + BNQ 8 = 1333 flux)
- Créé : `02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx`
  (22 colonnes, 1333 flux, tous VALIDE, tous IC, 10 contrôles BLOQUANTS passés)

Volumes produits :
  - RES : 1321 flux (TYPE_FLUX_017, PRODUIT, IC — réservations Airbnb/Booking)
  - MEN : 4 flux (TYPE_FLUX_014, CHARGE, IC — ménages externes Aissata, 2026-05)
  - BNQ : 8 flux (TYPE_FLUX_016, CHARGE, IC — frais bancaires, commentaire générique)
  - Total : 1333 flux VALIDE / 0 doublon / 0 montant négatif

Sécurité bancaire : aucune donnée brute (libellé, compte, IBAN) dans MASTER_CALC_Flux.xlsx [OK]
Statut : EN_ATTENTE_VALIDATION_HUMAINE — commit non fait
Fichiers sources NON modifiés (HA_Reservations, Payout, HH, Banque brute)

## Ce qui a été modifié (session 15 — Lot 4bis correctif, 2026-06-11)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-016 inscrit)
- Script créé : `02_TRAVAIL/lot4bis_charger_reservations.py`
- Modifié : `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
  (MASTER 1 391 lignes / VUE_FLUX 1 321 lignes / POWER_QUERY_CODE conservé)
- Backup : `99_ARCHIVES/LOT4BIS_TableCommune/MASTER_CALC_Reservations_BACKUP_20260611_200317.xlsx`
- Commit 3835f21 (5 fichiers : .gitignore, ETAT_AVANCEMENT, JOURNAL_CONTROLES, script, xlsx)

---

## Ce qui a été modifié (cette session — Lot 6c)
- Cadrage : `DECISIONS_METIER.md` (D079–D088 ajoutés — D-6c-01 à D-6c-10)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-012 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 13, Lot 6c)
- Script : `02_TRAVAIL/lot6c_menages_externes.py` (créé + exécuté)
- Créé : `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`
  (7 onglets : SOURCE_RAW 13L / PARAMETRES / MASTER 49cols / VUE_ACTIVE 4L / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE 7Q / README)
- Modifié : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  (REF_Intervenants +3 cols INT_0003/0004 ; REF_Types_Flux +TYPE_FLUX_014)
- Backup : `99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm`
- Fichiers Lots 3, 4, 4bis, 5, 6a, 6b : NON modifiés

## Ce qui a été modifié (cette session — Lot 6a)
- Cadrage : `DECISIONS_METIER.md` (D065–D069 ajoutés — QM-L6a-API/Jan/02/inactif/04)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-010 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 11, Lot 6a)
- Script : `02_TRAVAIL/lot6a_cleaning_tasks_comptage.py` (créé + exécuté)
- Modifié : `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`
  (4 onglets : data 500 tâches 11 cols, MASTER_ENRICHI 21 cols, VUE_COMPTAGE 11 cols, POWER_QUERY_CODE)
- REF_Setup.xlsm : NON modifié
- Fichier CleaningTasks issu du Lot 1 enrichi au Lot 6a : MODIFIÉ (`02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`)
- REF_Setup.xlsm : NON modifié
- Fichiers Lots 3, 4, 4bis, 5 : NON modifiés

---

## Ce qui a été testé sur données réelles
- Test : Audit Lot 0 complet (pre + post correction) sur `REF_Setup.xlsm` réel
- Statut : VALIDÉ — CTR-2026-06-003
- Test : Extraction Lot 1 Hostaway complète post-fix financeField (run 20260608_134253, 121.8 s)
- Statut : VALIDÉ — CTR-2026-06-004

---

## Anomalies connues (résumé — détail dans JOURNAL_ANOMALIES.md)

| ID | Code | Sévérité | Statut |
|---|---|---|---|
| ANO-001 | LISTING_ORPHELIN (515523) | A_CONTROLER | **CORRIGÉ 2026-06-08** — alias → LOG_0016 |
| ANO-002 | ENCODAGE_CASSE | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-003 | DATES_SERIE_EXCEL | A_CONTROLER | **SANS OBJET — aucune série brute** |
| ANO-004 | VRBO_MONTANT_NON_RENSEIGNE (×32) | A_CONTROLER | OUVERT — Lot 4 |
| ANO-005 | REFERENTIEL_ORPHELIN (497801) | A_CONTROLER | **CORRIGÉ 2026-06-08** — ancien ID LOG_0009 |
| ANO-006 | LISTING_CONFIRME_HORS_HOSTAWAY (480780) | INFO | **CORRIGÉ 2026-06-08** — ancien ID LOG_0016 |
| ANO-007 | REF_STATUTS_VALEURS_CONTROLE_MANQUANTES | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-008 | REF_STATUTS_PAYOUT_ABSENT | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-009 | REF_CLOTURE_MENSUELLE_ABSENTE | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-010 | REF_PARAMETRES_GENERAUX_INCOMPLETS | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-011 | REF_INTERVENANTS_SCHEMA_INCOMPLET | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-012 | REF_LOGEMENTS_CODES_HORS_PARC_ABSENTS | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-013 | REF_CARTES_PAIEMENT_SUFFIXE_MANQUANT | A_CONTROLER | IGNORE_JUSTIFIE — Lot 8 |
| ANO-014 | LISTING_ORPHELIN_A_CONTROLER (556954) | A_CONTROLER | **CORRIGÉ 2026-06-08** — alias → LOG_0009 |

> **4 cas listingMapId résolus au Lot 2** — voir JOURNAL_ANOMALIES.md (note synthèse mise à jour).

---

## Décisions prises (résumé — détail dans DECISIONS_METIER.md)
- D001 à D020 : décisions architecture validées (payout, commission, codes impact, upsert…)
- D021 : Statuts payout fermés — `REF_Statuts_Payout` à créer au Lot 0
- D022 : `REF_Statuts` fermé — à créer au Lot 0
- D023 : obsolète — remplacée par D035 (double seuil 0,10 €/1,00 €)
- D024 : `REF_Cloture_Mensuelle` — structure créée au Lot 0, exploitée au Lot 8
- D025 : Frontière Lot 3 / Lot 7 — IK uniquement dans `MASTER_FACT_MAN_IK_Avantages`
- D026 : `SAISIE_Charges_Flux.xlsx` = source unique achats/charges (exclut IK)
- D027 : Suppression définitive `Courses` / `Coût du lavage` / `achats` de M04 — IRRÉVOCABLE
- D016-REV : Forfait local 50 € quitte M04 → `SAISIE_Charges_Flux.xlsx`
- D028 : Coût complet ménage hors M04 via `VUE_ACHATS_MENAGE_VALIDES`
- D029 : Aucun lot ne peut être FAIT sans contrôle dans JOURNAL_CONTROLES — IRRÉVOCABLE
- D030 : Cancellation payout — `BaseCommission = CancellationPayout`, pas de ménage déduit
- D031 : `revenu_net_exploitation_proprietaire` — indicateur économique pur, formule verrouillée
- D032 : `acompte_conciergerie_recu_via_airbnb` — bloc règlement uniquement, jamais exploitation
- D033 : Séparation exploitation / règlement — deux blocs non communicants
- D034 : `charges_exceptionnelles_refacturees` — bloc règlement uniquement
- D035 : Convention d'arrondi — double seuil 0,10 €/ligne / 1,00 €/cumulé — VERROUILLÉ
- D036 : IK en montant direct (pas de barème auto au démarrage) — VERROUILLÉ
- D037 : REF_Couts_Standards_Menage = exécution seule, valeurs à revalider Lot 0 — VERROUILLÉ
- D038 : Rangement dans M04 = main-d'œuvre uniquement — VERROUILLÉ
- D039 : charge_fixe_mensuelle paramétrable dans REF_Logements, 0 si absent — VERROUILLÉ
- D040 : Structure sortie facture (FACT_FACTURE_ENTETE/LIGNES, Excel contrôle, **aucun PDF au démarrage**) — VERROUILLÉ
- D041 : Incidents voyageurs — catégorie `CHG_021` dans `SAISIE_Charges_Flux.xlsx`, `reservation_id` obligatoire — VERROUILLÉ (P02)
- D042 : AirCover — 3 flux distincts (remboursement propriétaire hors comptes / prestation `CHG_022` facturée bloc règlement / impact résultat ligne par ligne) — VERROUILLÉ (P03)
- D043 : Priorité Excel avant Power BI — aucun dashboard `.pbix` livré par les lots — VERROUILLÉ (P32)
- D044 : Séparation statut_controle / niveau_anomalie — `statut_controle` : VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER (Lot 3+) ; `niveau_anomalie` : INFO/A_CONTROLER/BLOQUANT — VERROUILLÉ (DM-L3-01)
- D045 : REF_Charges_Recurrentes — table des montants paramétrables (forfaits, loyers) ; aucun montant fixe codé en dur dans formules/PQ/scripts — VERROUILLÉ
- D052 : logement_id via JOIN REF_Mapping_Logements (listingMapId, HA) ; proprietaire_id r?solu via REF_Gestion_Logements_Hist ; anomalies LOGEMENT_NON_MAPPE + MAPPING_MULTIPLE + GESTION_LOGEMENT_MISSING/AMBIGUE ? VERROUILL? (QM-L4b-01)
- D053 : MASTER_CALC_Reservations 24 cols ; source 7 valeurs ; source_montant 5 valeurs ; +niveau_anomalie +code_anomalie ; contrôle RESERVATION_HH_NON_VALIDE — VERROUILLÉ (QM-L4b-02)
- D054 : Anti-double-comptage 7 scénarios ; DOUBLON HA-HH BLOQUANT ; CALC_ID_DUPLIQUE BLOQUANT — VERROUILLÉ (QM-L4b-03)
- D055 : mois = TEXT YYYY-MM depuis checkInDate (HA) / direct (HH) — VERROUILLÉ (QM-L4b-04)
- D056 : PK RES-AAAA-MM-HA-NNN / RES-AAAA-MM-HH-NNN ; compteur reset 001/mois/branche — VERROUILLÉ (QM-L4b-05)
- D057 : chemins 02_TRAVAIL/Lot4bis_TableCommune/ ; 3 onglets ; 7 requêtes PQ — VERROUILLÉ (QM-L4b-06)
- D058 : périmètre sources Lot 5 — HH_RESERVATION / VIREMENT_DIRECT / AUTRE — VERROUILLÉ (QM-L5-01)
- D059 : facture_ref = FAC-AAAA-MM-PROP-NNN, provisoire, BLOQUANT si absente — VERROUILLÉ (QM-L5-02)
- D060 : granularité proprietaire_id+logement_id+mois+facture_ref+source ; 22 cols MASTER — VERROUILLÉ (QM-L5-03)
- D061 : report_mois_suivant supprimé Lot 5 ; report_mois_precedent informatif, déféré Lot 10/12 — VERROUILLÉ (QM-L5-04)
- D062 : chemins Lot 5 ; TYPE_FLUX_006 existant, aucune modif REF_Setup — VERROUILLÉ (QM-L5-05)
- D063 : PK ACC-AAAA-MM-NNN, reset 001/mois, stable, saisi manuellement — VERROUILLÉ (QM-L5-06)
- D064 : source_pk = acompte_id toujours ; source_hh_id = reservation_hh_id si HH — VERROUILLÉ (QM-L5-07)

---

## Prochaine action obligatoire
```
Lot 10 — 2026-06-13 — CTR-2026-06-019 — EN_ATTENTE_VALIDATION_HUMAINE
Action : valider les 19 contrôles CTR-LOT10-01 à CTR-LOT10-19, puis git add + commit.

Fichiers à committer (6) :
  00_CADRAGE/ETAT_AVANCEMENT.md
  00_CADRAGE/JOURNAL_CONTROLES.md
  02_TRAVAIL/lot10_calculer_resultats.py
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx

19 contrôles à valider :
  CTR-LOT10-01  Flux lus                                : 1 333
  CTR-LOT10-02  Reservations NORMAL commissions         : 1 321
  CTR-LOT10-03  A_CONTROLER exclus                      : 59
  CTR-LOT10-04  Total payout NORMAL                     : 283 709.60 €
  CTR-LOT10-05  Total ménage retenu                     : 55 619.00 €
  CTR-LOT10-06  Total assiette commission                : 228 090.60 €
  CTR-LOT10-07  Total commission conciergerie            : 39 167.42 €
  CTR-LOT10-08  Total net proprio avant charge fixe      : 188 923.18 €
  CTR-LOT10-09  Total charge fixe générée                : 7 645.00 €
  CTR-LOT10-10  Total net proprio après charge fixe      : 181 278.18 €
  CTR-LOT10-11  Résultat REEL global (Flux)             : 283 515.60 €
  CTR-LOT10-12  Résultat COMPTABLE global               : 283 515.60 €
  CTR-LOT10-13  Résultat HORS_COMPTA                    : 0.00 € [HC_ZERO_SOURCES_VIDES]
  CTR-LOT10-14  PAR_MOIS_LOGEMENT total / dont REEL      : 497 lignes / 248 REEL
  CTR-LOT10-15  PAR_MOIS_PROPRIETAIRE total / dont REEL : 404 lignes / 202 REEL
  CTR-LOT10-16  CHARGE_FIXE_DATE_ENTREE_GESTION_INCO.   : 12 logements (trace historique ; contr?le d?commissionn? le 2026-06-29, REF_Gestion_Logements_Hist source unique)
  CTR-LOT10-17  LOG_SANS_FLUX_017                       : 1 (LOG_0009 — forfait=40€, 0 réservation Flux)
  CTR-LOT10-18  Contrôles BLOQUANTS                     : 0
  CTR-LOT10-19  Sources amont lecture seule              : OK

Points à décider après Lot 10 (ne bloquent pas le commit) :
  - LOG_0009 : forfait=40€ mais 0 réservation Flux → confirmer si logement actif / en gestion
  - date_entree_gestion REF_Logements : trace historique ; colonne supprim?e le 2026-06-29, remplac?e par REF_Gestion_Logements_Hist comme source officielle unique
  - mode_facturation = A_DEFINIR (12 propriétaires) → Lot 12
```

---

## Prochaine action obligatoire
```
Lot 11 — 2026-06-14 — CTR-2026-06-020 — EN_ATTENTE_VALIDATION_HUMAINE
Action : valider les 12 contrôles CTR-LOT11 ci-dessous, puis git add + commit.

Fichiers à committer (4) :
  00_CADRAGE/ETAT_AVANCEMENT.md
  00_CADRAGE/JOURNAL_CONTROLES.md
  02_TRAVAIL/lot11_controles_coherence.py
  02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx

12 contrôles à valider (après corrections audit F1/F2/F3) :
  CTR-LOT11-01  Total contrôles générés               : 51
  CTR-LOT11-02  BLOQUANTS ouverts                     : 0
  CTR-LOT11-03  A_CONTROLER ouverts                   : 44
  CTR-LOT11-04  INFO                                  : 7
  CTR-LOT11-05  LISTING_ORPHELIN_A_CONTROLER           : 23 (réservations listingMapId 515523/556954)
  CTR-LOT11-06  CHARGE_FIXE_DATE_ENTREE_GESTION_INCO. : 14 logements (trace historique ; contr?le supprim? le 2026-06-29 avec les colonnes doublons)
  CTR-LOT11-07  CLOTURE_IMPOSSIBLE_LIGNE_BANC.         : 3 mois (2026-02/03/04 RAPPROCHEMENT_REQUIS)
  CTR-LOT11-08  REEL = COMPTABLE + HORS_COMPTA         : 283515.60 = 283515.60 + 0 ✓
  CTR-LOT11-09  Banque statut                         : BANQUE_DISPONIBLE
  CTR-LOT11-10  Caisse théorique                      : 0.00 EUR (CAISSE_NON_REPRESENTATIVE)
  CTR-LOT11-11  Sources amont lecture seule            : OK (aucune modification)
  CTR-LOT11-12  facturation_lot12_ok                  : NON sur tous les mois (0 OUI) — F1 appliqué

Points résiduels (non bloquants pour commit) :
  - 44 A_CONTROLER à traiter/valider avant Lot 12 (priorité : VRBO, HH, acomptes, clôture banque)
  - LOG_0009 : investigation séparée
  - date_entree_gestion : trace historique ; correctif r?alis? le 2026-06-29 par suppression des colonnes doublons et usage exclusif de REF_Gestion_Logements_Hist
  - mode_facturation : Lot 12
  - Banque : export Airbnb requis pour finaliser RAPPROCH_AIRBNB_ATTENTE
```

---

## Lecture prochaine session (discipline contextuelle)

Si la prochaine session est **validation + commit Lot 11** :
```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 11
- ARCHITECTURE_DONNEES.md → §18
- REGLES_METIER.md → §1, §6, §8, §11

À NE PAS OUVRIR :
- README_PROJET.md (sauf onboarding)
- Sources brutes PDF/API
```

Cette discipline est appliquée à chaque nouveau lot, en s'appuyant sur la matrice `CLAUDE.md §5.bis`.

---

## Interdictions / points sensibles
- Ne pas modifier : sources brutes (Banque, Hostaway, PDF)
- Ne pas utiliser : REF_Setup.xlsx (nom incorrect — le fichier est REF_Setup.xlsm)
- Ne pas passer au Lot 2 sans audit Lot 0 validé sur fichier réel
- **Ne jamais marquer un lot FAIT sans entrée dans JOURNAL_CONTROLES (D029)**
- Ne pas écraser : tables master existantes sans sauvegarde
- Ne pas fusionner ménages internes / externes / tâches Hostaway
- Ne pas saisir IK/virements associés dans SAISIE_Charges_Flux.xlsx (appartient au Lot 7)
- **Ne jamais réintroduire Courses, Coût du lavage, onglet achats dans M04 (D027 — irrévocable)**
- Ne pas mettre achats/consommables/linge/matériel dans M04 (appartient à SAISIE_Charges_Flux)
- Ne pas modifier revenu_net_exploitation via des acomptes ou paiements reçus (D031/D033)
- Ne jamais intégrer une charge exceptionnelle dans charge_fixe_mensuelle (D039/EP3)
- Ne jamais comparer un coût standard complet à un coût d'exécution M04 (D037)
- Ne jamais inclure une charge exceptionnelle refacturée dans `revenu_net_exploitation_proprietaire` (D034/EP7/P21)
- Ne jamais traiter un remboursement AirCover perçu par le propriétaire comme un payout (D042/AC5)
- Ne jamais livrer un dashboard Power BI dans un lot (D043/PBI2)

---

## Note d'etat - Lot HORS_PARC_TECHNIQUE (2026-06-29)

Le lot `HORS_PARC_TECHNIQUE` est en revue avant commit.

Regle consolidee :
- `actif` = disponibilite technique du code dans le referentiel.
- `statut_parc` = eligibilite du code au parc de logements geres.
- `GERE` = logement reellement gere et eligible aux calculs metier.
- `HORS_PARC_TECHNIQUE` = code conserve pour controle ou anti-mauvais-mapping, exclu explicitement de tout calcul economique et operationnel.
- `statut_parc` vide, invalide ou inconnu = `A_CONTROLER`, code anomalie `STATUT_PARC_INVALIDE`, sans calcul economique.

Tests de regression ajoutes : `tests/test_hors_parc_technique.py` et extension de `tests/test_import_side_effects.py` aux imports des lots 4bis, 6c, 10, 11, 12 et 13 en copie temporaire isolee.
## Note d'état - APP-2b règles paiement, acomptes et dérogations (2026-07-03)

Statut : implémentation contrôlée sur code et copies temporaires, écriture réelle toujours désactivée.

Éléments réalisés :
- suppression fonctionnelle de `reservation_id_hostaway` dans APP-2b ;
- `SAISIE_MANUELLE` pré-sélectionné sur nouveau formulaire ;
- propriétaire dérivé par logement + date d'arrivée ;
- taux de commission standard affiché depuis `REF_Taux_Commission` avec dérogation réelle tracée ;
- prix ménage standard affiché depuis `REF_Couts_Standards_Menage` avec dérogation réelle tracée ;
- mode cible `PAY_006` / `DIRECT_PROPRIETAIRE` préparé par migration sur copie ;
- acompte propriétaire Lot4A recalculé par mode de paiement, mois = mois de check-in ;
- comptabilisation dérivée de `REF_Codes_Impact.impact_resultat_comptable` ;
- migration de schéma APP-2b testée sur copie D-APP-05C uniquement.

Activation restante :
- migration contrôlée du classeur source réel et des dépendances aval avant toute écriture réelle ;
- maintien de `HH_REAL_WRITE_ENABLED=False` jusqu'à validation humaine et contrôle complet.

## Note d'état - D-APP-2B-REV2 (2026-07-03)

Statut : correctif métier/ergonomie en cours de contrôle avant commit sélectif.

Éléments REV2 :
- dérogations taux/ménage confirmées en modale locale au moment de la vérification, avec `Annuler` focalisé par défaut, motif obligatoire et confirmation cachée uniquement après validation de la modale ;
- suppression du rendu permanent en bloc jaune/case à cocher pour les dérogations ;
- champs associés visibles uniquement pour `CARTE_ASSOCIEE` / `COMPTE_PERSO_ASSOCIEE`, reverse propriétaire visible uniquement pour espèces, valeurs masquées vidées/ignorées ;
- acompte propriétaire Lot4A : banque pro = `total_percu`, carte associée = `total_percu`, compte perso associé = `total_percu`, espèces = `total_percu - montant_reverse_proprietaire`, direct propriétaire = `0`, mois = `date_arrivee` ;
- aucune modification des fichiers Excel réels, écriture réelle APP-2b toujours désactivée.

Contrôles requis avant commit :
- tests application APP-2b Miniconda ;
- tests Lot4A ciblés avec dépendances NumPy/pandas ;
- `git diff --check` ;
- staging sélectif REV2 uniquement.

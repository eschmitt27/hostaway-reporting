# JOURNAL_CONTROLES.md
> Un contrôle exécuté sur données réelles = une entrée. Vide tant qu'aucun lot n'a tourné sur fichier réel.

---

## Format d'entrée

```
Date       : AAAA-MM-JJ
Lot        : Lot X — Nom
Code       : CODE_CONTROLE (convention : voir §Référence ci-dessous)
Sévérité   : BLOQUANT | A_CONTROLER | INFO
Fichier    : fichier testé
Résultat   : description précise
Statut     : OUVERT | CORRIGÉ | IGNORE_JUSTIFIE
Commentaire: note
```

---

## Contrôles exécutés

---

### CTR-DAPP2B-IMPL-2026-07-03

```
Date       : 2026-07-03
Lot        : APP-2b — Saisie HH contrôlée — Implémentation technique
Code       : DAPP2B_IMPL_TECHNIQUE_GUARD_ACTIF
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (10 fichiers créés, 6 fichiers modifiés)
             01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
Résultat   : APP-2b implémentée techniquement. 149/149 tests verts.
             HH_REAL_WRITE_ENABLED = False — aucune écriture réelle effectuée.
             SAISIE sha256 = c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c INCHANGÉ.
             Routes créées :
               GET  /reservations/nouvelle     → formulaire (listes REF_LOCALE)
               POST /reservations/nouvelle/verifier  → validation D1–D11 + PK + preview
               POST /reservations/nouvelle/confirmer → écriture atomique (bloquée garde)
             Modules créés :
               saisie_hh_reader.py  — lecture SAISIE, PKs, génération RESHH-AAAA-MM-NNN
               ref_setup_hh_reader.py — cloture, gestion hist, associes (module séparé — préserve invariant APP-1)
               saisie_hh_service.py — validation D1–D11
               saisie_hh_writer.py  — écriture atomique 11 étapes + garde HH_REAL_WRITE_ENABLED
               0002_saisie_hh.sql   — table saisie_hh_writes (journal applicatif)
             Tests :
               test_saisie_hh_validation.py (27 tests — D1–D11 complets)
               test_saisie_hh_writer.py      (6 tests — garde, SQLite, formules protégées)
               test_saisie_hh_routes.py      (8 tests — 3 routes + ordering conflit statique/param)
               test_sqlite_migrations.py mis à jour (saisie_hh_writes dans EXPECTED_TABLES)
               test_no_metier_calc.py mis à jour (writers/ exclus du check sync bidirectionnel)
             Invariants APP-1 préservés :
               test_code_ne_lit_jamais_proprietaires_ni_gestion : VERT
               (fonctions APP-2b dans ref_setup_hh_reader.py, hors ref_setup_reader.py)
Statut     : INFO — IMPLÉMENTÉE. Validation humaine réelle requise avant activation HH_REAL_WRITE_ENABLED.
Commentaire: Le writer est fonctionnel mais bloqué par garde. La première écriture réelle nécessite :
             (1) validation visuelle interface (formulaire + prévisualisation)
             (2) modification manuelle de HH_REAL_WRITE_ENABLED = True dans config.py
             (3) saisie de test humaine contrôlée
             (4) vérification Excel post-écriture
             L'activation de HH_REAL_WRITE_ENABLED ne doit jamais être faite par l'agent sans feu vert explicite.
```

---

### CTR-DAPP2B-AUDIT-2026-07-03

```
Date       : 2026-07-03
Lot        : APP-2b — Audit technique + 9 correctifs post-audit
Code       : DAPP2B_AUDIT_CORRECTIFS_SECURITE
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (2 fichiers réécrits, 1 nouveau, 4 modifiés)
             01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
Résultat   : Audit technique APP-2b (lecture seule) — 5 domaines, 9 NON_CONFORME identifiés.
             9 correctifs appliqués. 181/181 tests verts.
             SAISIE sha256 = c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c INCHANGÉ.

             AUDIT — Résultats (avant correctifs) :
               A1 Source clôture  : NON_CONFORME — REF_CLOTURE pointait fichier autonome inexistant
               A2 Test anti-sync  : NON_CONFORME — exclusion writers/ masquait faux positif potentiel
               A3 Writer contract : NON_CONFORME — writer contenait accès SQLite et garde guard
               A4 D7/D8/D9/D10   : NON_CONFORME — except: pass silencieux sur REF_Setup inaccessible
               A5 Decimal/précis. : NON_CONFORME — float acceptait >2 décimales sans rejet
               A6 Rollback        : ABSENT — aucun mécanisme de restauration post-écriture
               A7 ~$/verrou       : ABSENT — écriture sans détection Excel ouvert ni verrou exclusif
               A8 Préservation    : ABSENT — aucune vérification structure avant os.replace
               A9 Routes sécurité : NON_CONFORME — route appelait writer direct (pas orchestrateur)

             CORRECTIFS APPLIQUÉS :
               C1 config.py : _REF_CLOTURE_OBSOLETE = None — chemin autonome neutralisé
               C2 saisie_hh_writer.py (réécrit) : writer pur fichier, ZERO accès SQLite
                  saisie_hh_orchestrator.py (nouveau) : guard + snapshot + SQLite + rollback,
                  ZERO openpyxl/save()
               C3 write_row étape 1 : détection ~$<fichier>
                  write_row étape 2 : verrou O_CREAT|O_EXCL|O_WRONLY (atomique Windows/Unix)
               C4 write_row étape 5 : préflight formules (figée ≠ None ≠ = → ERREUR)
                  write_row étape 7 : vérification même volume (atomicité garantie)
               C5 write_row étape 6+11 : mesure structure avant / vérification après
                  (sheets/DV/MFC/named_ranges/fullCalcOnLoad — comparaison relative)
                  fullCalcOnLoad forcé à True avant save
               C6 orchestrateur : copie .rollback.xlsx avant write_row
                  rollback atomique (os.replace) si journalisation SQLite échoue post-write
               C7 saisie_hh_service.py : Decimal (pas float) pour tous les montants
                  rejet explicite >2 décimales (MONTANT_TROP_DE_DECIMALES)
                  fail-closed : except Exception as exc → REF_SETUP_INDISPONIBLE (D7/D8/D9/D10)
               C8 orchestrateur : guard HH_REAL_WRITE_ENABLED en premier (avant assert_writable)
               C9 test_no_bidirectional_sync : détection par import DB réel (get_db / from app.db)
                  test_saisie_hh_writer.py : 20 tests (~$, verrou, préflight, structure, delta)
                  test_saisie_hh_orchestrator.py : 8 tests (guard, SQLite, rollback)
                  test_saisie_hh_validation.py : +15 tests (Decimal, fail-closed, D9 divergence)
                  routes mock mis à jour : hh_orchestrator.confirm_write

             Séparation vérifiée par test :
               saisie_hh_writer.py  : has save() → True ; has DB access → False ✓
               saisie_hh_orchestrator.py : has save() → False ; has DB access → True ✓
               test_no_bidirectional_sync → aucune violation ✓

Statut     : INFO — CORRECTIFS APPLIQUÉS. Validation humaine réelle requise avant activation.
Commentaire: HH_REAL_WRITE_ENABLED = False. Aucune écriture réelle possible.
             Activation : modification manuelle de config.py + validation humaine explicite.
             Ne jamais activer HH_REAL_WRITE_ENABLED sans feu vert humain.
```

---

### CTR-DAPP2B-CADRAGE-2026-07-03

```
Date       : 2026-07-03
Lot        : D-APP-2B-CADRAGE — Décisions fonctionnelles verrouillées (D1 à D11)
Code       : DAPP2B_CADRAGE_FONCTIONNEL_VERROUILLE
Sévérité   : INFO
Fichier    : 00_CADRAGE/DECISIONS_METIER.md (D-APP-2B) — aucun fichier Excel, code ou test touché
Résultat   : Cadrage fonctionnel APP-2b verrouillé avant implémentation. 11 décisions actées :
               D1  reservation_id_hostaway : obligatoire VRBO_UNKNOWN/DIRECT_HA_PAYANT/HOSTAWAY_REFERENCE,
                   facultatif sinon ; entier positif ; unicité si renseigné ; doublon → RESERVATION_DOUBLON_HOSTAWAY_HH.
               D2/D3 total_percu obligatoire pour toute création (y compris VRBO_UNKNOWN) ;
                   tolérance CTR-L4-13 non reprise par l'application.
               D4  aucun mapping canal ↔ source financière imposé ; validation d'appartenance seulement ; aucune correction auto.
               D5  codes de blocage APP-2b validés (non implémentés) : MOIS_HORS_REFERENTIEL_CLOTURE, MOIS_CLOTURE,
                   LOGEMENT_SANS_GESTION_ACTIVE, PROPRIETAIRE_LOGEMENT_INCOHERENT_A_DATE, LOGEMENT_HORS_PARC_TECHNIQUE,
                   DIVERGENCE_REF_LOCALE_REF_SETUP, RESERVATION_HH_ID_DUPLIQUE, RESERVATION_DOUBLON_HOSTAWAY_HH,
                   SEQUENCE_PK_INCOHERENTE.
               D6  reservation_hh_id = RESHH-AAAA-MM-NNN (mois de date_arrivee) ; max numérique + 1 ; trous admis ;
                   suffixe non numérique/format incompatible → SEQUENCE_PK_INCOHERENTE ; collision bloque ;
                   pas de génération si mois clôturé ou absent.
               D7  REF_Gestion_Logements_Hist : date_fin inclusive ; vide = période ouverte ;
                   cohérence propriétaire/logement contrôlée à la date_arrivee.
               D8  éligibilité logement : gestion active à date_arrivee + statut_parc=GERE + actif=OUI ;
                   sinon bloqué ; aucune correction auto du propriétaire/logement.
               D9  REF_LOCALE compatible Excel ; REF_Setup autoritaire (activité/parc/historique) ;
                   divergence → DIVERGENCE_REF_LOCALE_REF_SETUP.
               D10 l'application ne crée jamais de mois ; ouverture 2026-07/OUVERT = opération manuelle
                   dans REF_Setup.xlsm ; sinon MOIS_HORS_REFERENTIEL_CLOTURE.
               D11 associe_id_recuperateur obligatoire seulement si montant_recupere > 0.
Statut     : CADRAGE VERROUILLÉ — implémentation APP-2b NON démarrée.
Commentaire: Aucun code APP-2b, aucune route d'écriture, aucun writer, aucun fichier Excel modifié.
             saisie_writer.py reste stub (NotImplementedError). Codes de blocage non encore implémentés.
             Démarrage de l'implémentation uniquement sur feu vert humain explicite.
```

---
### CTR-DAPP05BCD-2026-07-02

```
Date       : 2026-07-02
Lot        : D-APP-05B / D-APP-05C / D-APP-05D — Preuve écriture + correction formules + correction réelle SAISIE HH
Code       : DAPP05BCD_FORMULES_SAISIE_HH
Sévérité   : INFO
Fichier    : (D-APP-05B) 05_APPLICATION/data/dapp05b_proof/20260702T095519Z/SAISIE_copie.xlsx
             (D-APP-05C) 05_APPLICATION/data/dapp05c_formules/20260702T143503Z/SAISIE_copie.xlsx
             (D-APP-05D) 01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
             (backup)    05_APPLICATION/data/dapp05d_reel/20260702T155817Z/backup_avant/SAISIE_AVANT.xlsx
Résultat   : D-APP-05B — Preuve openpyxl sur copie isolée. 12/12 contrôles automatisés verts.
               - Formules B/C/K/N/O/Q/V/Y/Z intactes sur la ligne modèle après écriture manuelles.
               - 12 plages nommées, 11 validations, 2 MFC, 4 feuilles : identiques au réel.
               - fullCalcOnLoad="1" préservé. Aucun temp résiduel. Fichier réel invariant.
               - Validation humaine Excel réussie : DV actives, MFC fonctionnelle, ligne historique intacte.
               - Anomalie détectée : formules B/C utilisaient TEXT(YYYY/DD/"0.00") — tokens localisés,
                 incompatibles Excel FR. Affichage "YYYY-05" / "YYYY05DD|23.43".

             D-APP-05C — Correction formules B/C sur copie isolée. 14/14 contrôles automatisés verts.
               - Nouvelles formules : YEAR/MONTH/DAY + RIGHT("0"&…,2) pour date ;
                 INT/ABS/ROUND/MOD + point décimal littéral pour montants. Aucun token localisé.
               - Delta borné à {B2:B501, C2:C501}, 0 hors périmètre. K/N/O/Q/V/Y/Z inchangées.
               - 12 plages nommées, 11 validations, 2 MFC, 4 feuilles identiques.
               - Validation humaine Excel réussie : C2=2026-05, C3=2099-12 ;
                 B3=RESHH-2099-12-999|CANAL_001|PROP_0001|LOG_0001|20991201|1000.00.

             D-APP-05D — Correction atomique réelle. 11/11 contrôles structurels verts.
               - Préflight : sha256=e4591912… taille=75924 mtime_ns=1782676150164898800 (== référence).
               - Même volume Windows vérifié. Temporaire : SAISIE_APRES.tmp.xlsx.
               - Delta réel : 1000 cellules, toutes dans {B2:B501, C2:C501}, 0 hors périmètre.
               - État intermédiaire post-os.replace : sha256=b4165ca9…87b101, taille=85696 o.
                 calcPr fullCalcOnLoad="1" + calcId="124519" préservés.
               - Excel a réécrit le fichier lors de la validation humaine (sérialisation ZIP différente).
               - État final (après ouverture/sauvegarde Excel) :
                 sha256=c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c, taille=49717 o.
               - Vérification finale lecture seule : 17/17 contrôles CONFORME.
                 Référence : 05_APPLICATION/data/dapp05d_reel/20260702T155817Z/verification_finale.json
               - Backup immuable conservé (sha256 origine vérifié).
               - Validation humaine finale : C2=2026-05,
                 B2=RESHH-2026-05-001|CANAL_004|PROP_0003|LOG_0009|20260525|2343.48,
                 aucune alerte de réparation Excel, aucune ligne test parasite.
Statut     : CORRIGÉ — D-APP-05A/B/C/D toutes validées. APP-2b débloquée techniquement.
Commentaire: saisie_writer.py reste stub (NotImplementedError). Aucune route d'écriture créée.
             APP-2b ne démarre que sur feu vert humain explicite.
```

---

### CTR-LOT4A-2026-07-02

```
Date       : 2026-07-02
Lot        : LOT4A — Comparateur + Transformateur dry-run (SAISIE ReservationsHH → MASTER de test)
Code       : LOT4A_COMPARATEUR_DRYRUN
Sévérité   : INFO
Fichier    : Créés :
               02_TRAVAIL/lib_lot4a_reservations_hh.py (bibliothèque partagée)
               02_TRAVAIL/lot4a_transform_reservations_hh.py (transformateur --dry-run)
               tests/test_lot4a_transform_reservations_hh.py (20 tests)
             Modifié :
               02_TRAVAIL/lot4a_compare_reservations_hh.py (refactorisé — lib commune, comportement inchangé)
             Déjà existant, inchangé :
               tests/test_lot4a_compare_reservations_hh.py (22 tests)
             Run réel :
               04_LOGS/LOT4A_DRY_RUN/20260702T085200Z/
Résultat   : D-APP-05A terminé techniquement en dry-run. Sources réelles inchangées.

             Comparateur lecture seule — 22/22 tests OK :
             - 7 catégories : MANUEL_IDENTIQUE, MANUEL_DIFFERENT, DERIVE_COHERENT,
               ECART_HISTORIQUE_ATTENDU, METADONNEE_NON_COMPARABLE, TAUX_BLOQUANT
             - MASTER legacy (commission=NULL) → ECART_HISTORIQUE_ATTENDU (jamais erreur)
             - Oracle RESHH-2026-05-001 : taux=0.15, commission=343.27, acompte=1945.21
             - Garde de chemin : RuntimeError si cible hors 04_LOGS/LOT4A_COMPARE
             - AST : aucun wb.save/to_excel, aucun subprocess/win32com/saisie_writer

             Transformateur dry-run — 20/20 tests OK :
             - Mode unique --dry-run --as-of ISO-8601 UTC obligatoire (naïf/non-UTC refusé)
             - MASTER de test : 2 feuilles MASTER+VUE_ACTIVE, 34 colonnes, aucun POWER_QUERY_CODE
             - Écrit uniquement sous 04_LOGS/LOT4A_DRY_RUN/<horodatage-UTC>/
             - Écriture atomique temp → validate (_validate_workbook) → os.replace → delete temp
             - Blocage ANALYSE_BLOQUEE_TAUX ou ANALYSE_BLOQUEE_DONNEES → aucun .xlsx ;
               seulement rapport_anomalies.md + manifest.json
             - Garde de chemin : RuntimeError pour 01_SOURCES_BRUTES, 02_TRAVAIL,
               03_EXPORTS, 05_APPLICATION (vérification avant chaque écriture)
             - AST : aucun argument --write-master déclaré, aucun args.write_master,
               aucun subprocess/win32com/saisie_writer

             Run réel (2026-07-02T08:52:00Z) :
             - Statut : ANALYSE_TERMINEE, master_test_genere = true
             - Oracle RESHH-2026-05-001 : taux=0.15, commission=343.27, acompte=1945.21
             - Équilibre : round2(55 + 343.27 + 0 + 1945.21) = 2343.48 OK
             - date_integration = '2026-07-02T00:00:00Z' (chaîne ISO UTC = --as-of)

             Invariance sources réelles (SHA-256 + taille + mtime_ns, avant = après) :
               SAISIE    e4591912a6b0f1cad69775c2… taille 75924 mtime_ns 1782676150164898800 INCHANGÉ
               MASTER    c0e4434c347987d21bac52d7… taille 10829 mtime_ns 1781524006555932300 INCHANGÉ
               REF_Setup f45f4feadcbebd04f78c4e4a… taille 86345 mtime_ns 1782721518446925000 INCHANGÉ

             Compatibilité aval confirmée (lecture seule) :
               lot4bis lit onglet "MASTER" par nom → compatible
               lot5 lit colonne acompte_facture → compatible
               date_integration ISO UTC texte non consommée par lot4bis/lot5/lot9 → compatible
               Aucun consommateur aval n'exige POWER_QUERY_CODE → absence sûre

             Aucune modification sous 01_SOURCES_BRUTES/ ni 03_EXPORTS/.
             Les seules modifications sous 02_TRAVAIL/ sont les ajouts et le refactor Lot4A explicitement documentés.
Statut     : INFO — D-APP-05A TERMINÉ TECHNIQUEMENT EN DRY-RUN
Commentaire: Aucun --write-master créé ni autorisé. MASTER réel jamais touché.
             D-APP-05B non démarré (preuve écriture SAISIE sur copie isolée).
             APP-2b toujours BLOQUÉE jusqu'à D-APP-05B validé + validation humaine finale LOT4A.
             Statut ANALYSE_BLOQUEE_DONNEES (5e statut LOT4A) validé le 2026-07-02 — voir D-LOT4A-01.
```

---

### CTR-DAPP05-PREFLIGHT-2026-07-02

```
Date       : 2026-07-02
Lot        : D-APP-05 — Protocole de preuve technique (préflight)
Code       : DAPP05_PREFLIGHT_POWER_QUERY_ABSENT
Sévérité   : BLOQUANT (pour le protocole tel qu'écrit)
Fichier    : 01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx (lecture seule)
             02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx (lecture seule)
Résultat   : Préflight bloquant — aucune écriture, aucune copie, aucune ligne de test.
             - Excel COM disponible (win32com OK, Excel 16.0).
             - MASTER généré par openpyxl 3.1.5 (creator=openpyxl, 2026-06-09) : aucun connections.xml,
               aucune DataMashup, aucune requête Power Query. Fichier statique.
             - SAISIE générée par openpyxl 3.1.5 : aucune connexion PQ. Contient 12 plages nommées lst_*,
               validations de données, mise en forme conditionnelle, formules (ROW_HASH/mois/nuits/taux/
               commission/acompte/impacts).
             - Aucun script courant ne régénère MASTER depuis SAISIE (lecture seule côté lot4bis/10/11 ;
               lot12_seed/remove = données fictives). Feuille POWER_QUERY_CODE = documentaire.
             - Conséquence : le refresh Power Query du protocole (§5) n'est pas exécutable (pas de PQ à
               rafraîchir). Arrêt avant écriture conformément à §1 du protocole.
             - SHA-256 originaux (inchangés) :
               SAISIE  e4591912a6b0f1cad69775c2c0554bc603e6ea43097133e46c21878807414190
               MASTER  c0e4434c347987d21bac52d7df6def4633fb949ffe3267fb203252a362300390
             git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/ → vide.
Statut     : OUVERT (bloqué — décision humaine requise)
Commentaire: D-APP-05 NON validée. Solution minimale proposée : reformuler la preuve sans Power Query
             (écriture COM sur copie isolée + rejeu du générateur openpyxl SAISIE→MASTER à identifier/
             reconstruire). Aucune écriture réelle, aucune route POST, saisie_writer reste stub. APP-2b bloquée.
```

---

### CTR-APP2a-2026-07-01

```
Date       : 2026-07-01
Lot        : Lot APP-2a — Réservations hors Hostaway (lecture seule)
Code       : APP2A_RESERVATIONS_HH_LECTURE
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (6 fichiers créés, 5 modifiés)
             tests/ (96 tests pytest, dont 18 réservations HH)
Résultat   : 96/96 tests pytest PASSED (9.89 s) — aucun skip
             Contrôles ciblés vérifiés :
             - Liste lue depuis MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx onglet MASTER (généré par PQ)
             - Ligne-placeholder Power Query exclue (filtre reservation_hh_id commence par RESHH-)
             - SAISIE_ReservationsHorsHostaway.xlsx jamais lue pour construire la liste (scan AST)
             - Aucune route POST/PUT/PATCH/DELETE ; POST /reservations -> 405
             - Recherche + filtres réels (mois, logement, propriétaire, canal, source_financiere, statut, impact, compta)
             - États : OK, EMPTY (cache PQ vide), erreur source absente, réservation inconnue -> 404 propre
             - Montants dérivés signalés « issus du moteur », jamais recalculés (CHAMPS_MOTEUR)
             - Lecture read_only ; MASTER et SAISIE inchangés (empreinte taille+mtime) après consultation
             - Aucun appel/import de saisie_writer ; aucune écriture SAISIE/MASTER
             - Aucune réservation métier en SQLite ; service n'importe ni get_db ni sqlite
             - Bloc fraîcheur : « Actualisation Power Query manuelle requise après toute future saisie »
             git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/ → vide (aucune donnée réelle modifiée)
Statut     : CORRIGÉ
Commentaire: Consultation lecture seule uniquement. APP-2b (écriture) reste BLOQUÉE.
             D-APP-05 TOUJOURS OUVERTE — aucune écriture SAISIE tant que la preuve technique
             sur copie isolée n'est pas réalisée et validée (protocole ci-dessous, non exécuté).
```

**D-APP-05 — Protocole de preuve APP-2b (préparé, NON exécuté au Lot APP-2a)**

À exécuter uniquement sur une COPIE temporaire isolée du classeur réel, jamais sur `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx`. Étapes obligatoires :

```
1.  Copie temporaire exacte du classeur réel vers un dossier de test isolé (hors 01/02/03).
2.  Ajout d'UNE SEULE ligne de test (colonnes manuelles uniquement) dans l'onglet SAISIE.
3.  Contrôle des formules pré-remplies (ROW_HASH, mois, nuits, taux_commission,
    taux_commission_source, commission, acompte_facture, impact_resultat_reel/comptable)
    → intactes et présentes sur la ligne ajoutée.
4.  Contrôle des 11 validations de données (listes déroulantes lst_*) → préservées.
5.  Contrôle des plages nommées (lst_Canaux … lst_PropTauxLookup) → préservées.
6.  Contrôle de la mise en forme conditionnelle (rouge=BLOQUANT / orange=A_CONTROLER) → préservée.
7.  Ouverture manuelle du fichier test dans Excel (recalcul des formules).
8.  Refresh Power Query manuel DANS CETTE COPIE uniquement (onglet MASTER).
9.  Contrôle que le MASTER de test reçoit la ligne attendue (30 col + 4 PQ, valeurs calculées correctes).
10. Suppression complète de l'environnement de test après preuve.
```

Tant que ces 10 points ne sont pas prouvés et validés humainement, `app/writers/saisie_writer.py` reste un stub (`NotImplementedError`) et aucune route d'écriture n'est créée.

---

### CTR-APP1-2026-07-01

```
Date       : 2026-07-01
Lot        : Lot APP-1 — Module Logements (lecture seule)
Code       : APP1_MODULE_LOGEMENTS
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (6 fichiers créés, 6 modifiés)
             tests/ (76 tests pytest, dont 21 logements)
Résultat   : 76/76 tests pytest PASSED (7.10 s) — aucun skip
             Contrôles ciblés vérifiés :
             - Liste depuis PBI_Referentiel_Logements.csv uniquement ; PBI manquant → erreur lisible, sans fallback Excel
             - Aucune reconstruction de jointure REF_Logements × REF_Gestion_Logements_Hist × REF_Proprietaires (scan AST)
             - Commission : bloc « Historique des taux de commission », aucune notion de taux actuel / en vigueur (rendu vérifié)
             - Lignes techniques (APPARTEMENT_DIVERS, LOGEMENT_DIVERS) exclues par défaut, présentes si toggle, badge HORS_PARC_TECHNIQUE, hors compteur parc (17)
             - Recherche + filtre ville testés ; état vide propre
             - Fiche détail existante 200 ; logement inconnu → 404 propre
             - REF_Setup lu en read_only ; aucune écriture source (empreinte taille+mtime PBI et REF_Setup inchangée après consultation)
             - Contact propriétaire (email/téléphone/adresse_facturation) absent de liste et fiche ; aucun « @ » dans le rendu
             - Aucune donnée métier logement en SQLite ; service n'importe ni get_db ni sqlite
             - /health confirme le vrai chemin REF_Setup (01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm), status OK
             git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/ → vide (aucune donnée réelle modifiée)
Statut     : VALIDÉ
Commentaire: Premier module métier de l'application, strictement lecture seule.
             Correction APP-0 intégrée : chemin REF_Setup (sous-dossier) — /health passe de DEGRADED à OK.
             D-APP-04 clôturée (affichage commission datée brute, sans calcul).
             VALIDATION HUMAINE ACCORDÉE le 2026-07-01 (contrôles visuels conformes).
             Décompte exact : 6 fichiers créés + 6 modifiés (home.html non modifié — bascule via home.py seul).
             Aucun module APP-2 ou suivant démarré. En attente de cadrage APP-2.
```

---

### CTR-APP0-CORR-2026-07-01

```
Date       : 2026-07-01
Lot        : Lot APP-0 — Corrections post-build avant validation humaine
Code       : APP0_CORRECTIONS_PRE_VALIDATION
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (7 fichiers modifiés, 2 fichiers créés, 1 fichier supprimé)
             tests/ (53 tests pytest)
Résultat   : 53/53 tests pytest PASSED (6.46 s)
             Nouvelles batteries :
             - test_pipeline_registry_paths (5) : 21 scripts confirmés → fichiers .py existants, aucun dossier Lot*, lot3 NON_REFERENCES documenté
             - test_navigation_no_404 (6) : sidebar sans href cassé, / et /sources-calculs → 200, routes futures → 404, badges APP-N présents
             Corrections appliquées :
             1. pipeline_registry.py — 13 chemins dossiers → 21 chemins fichiers .py réels ; lot3 exclus (aucun script)
             2. base.html — HTMX <script> retiré ; 7 menus non construits → <span class="nav-item--future"> non cliquable
             3. home.html — cartes modules a_venir → <div> sans href
             4. app.css — styles .nav-item--future et .nav-badge-future ajoutés
             5. htmx.2.0.4.min.js — stub supprimé ; LANCEMENT_LOCAL.md nettoyé
             6. requirements.txt — versions >= remplacées par versions figées exactes
             7. Statut global ETAT_AVANCEMENT.md unifié (suppression contradictions)
             Vérification lancement réel : / → 200, /health → 200 OK, /sources-calculs → 200, /logements → 404
             git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/ → nothing to commit
Statut     : CORRIGÉ
Commentaire: Corrections pré-validation demandées par utilisateur avant démarrage APP-1.
             Aucun module métier créé. Aucune donnée réelle modifiée.
             APP-0 en attente de validation humaine explicite avant tout démarrage APP-1.
```

---

### CTR-APP0-2026-07-01

```
Date       : 2026-07-01
Lot        : Lot APP-0 — Socle technique application locale
Code       : APP0_SOCLE_TECHNIQUE
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (44 fichiers créés)
             tests/ (42 tests pytest)
Résultat   : 42/42 tests pytest PASSED (4.30 s)
             - test_boot (7) : boot FastAPI, routes /, /sources-calculs, /health, CSS, logo, .gitignore OK
             - test_no_metier_calc (4) : aucun calcul métier dans app/, aucun import 02_TRAVAIL, pas de sync bidirectionnelle, syntaxe valide
             - test_pipeline_runner_dryrun (5) : dry-run forcé, subprocess non appelé, exécution réelle bloquée, journalisation SQLite OK
             - test_readonly_guarantee (13) : file_registry refuse 8 chemins non-SAISIE, accepte 4 SAISIE_*, PermissionError sur REF/MASTER, excel_reader read_only=True vérifié, saisie_writer stub OK
             - test_snapshot (5) : copie + manifeste sha256 + SQLite + détection corruption + restauration copie isolée
             - test_sqlite_migrations (5) : 7 tables créées, idempotence, version 0001, WAL mode, periods miroir OK
             git status 01_SOURCES_BRUTES/ 02_TRAVAIL/ 03_EXPORTS/ → nothing to commit (aucune donnée réelle modifiée)
Statut     : CORRIGÉ
Commentaire: Anomalie détectée et corrigée pendant le build : API Starlette 1.3.1 utilise
             TemplateResponse(request, name, context) et non TemplateResponse(name, context).
             Corrigé dans home.py et sources_calculs.py avant passage des tests.
             HTMX : repoussé hors APP-0 (stub supprimé en CTR-APP0-CORR-2026-07-01).
             Lot APP-0 conforme au DoD § 6.9 du plan.
```

---

### CTR-2026-06-023

```
Date       : 2026-06-14
Lot        : Lot 12 — Préfactures propriétaires
Code       : LOT12_PREFACTURES_PROPRIETAIRES
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot12_generer_factures.py
             02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx
Résultat   : Module Lot 12 créé — préfactures propriétaires (lecture seule sorties Lot 10/11).
             5 onglets : FACT_FACTURE_ENTETE / FACT_FACTURE_LIGNES / CONTROLE_MENSUEL /
                         DASHBOARD_FACTURATION / A_CONTROLER
             269 préfactures générées / 3 228 lignes facture (12 lignes par préfacture, §17.3)
             0 facture finale générée
             statut_generation : PREFACTURE_CONTROLE (269/269)
             statut_facture    : NON_FACTURABLE_A_CONTROLER (269/269)
             Balises visibles si données manquantes (logo/SIRET/adresse/mode/banque/...)
             GLOBAL_NON_AFFECTE exclu des factures propriétaires (frais bancaires société, hors REGLEMENT)
             Granularité prop × logement × mois ; numérotation PREF-AAAA-MM-PROP-LOG-NNN
             Contrôles : 0 BLOQUANT / 60 A_CONTROLER (59 réservations exclues + 1 MODE_FACTURATION_A_DEFINIR)
             0 donnée fictive résiduelle ; sources amont non modifiées
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Décisions appliquées (validées 2026-06-15) :** D-LOT12-01 à D-LOT12-08.

**Test fictif complet (cycle seed → pipeline → lot12 → remove) :**
- seed exécuté (Charges + M04 + MenExt + HH + Acomptes)
- pipeline relancé jusqu'à Lot 12 (lot4bis → lot9 → lot10 → lot11 → lot12)
- HH fictives visibles dans les préfactures (PROP_0001/LOG_0001/2026-03 : L1 payout 1 388,02 incluant HH_001, nb_resa 10)
- acompte fictif `ZZ_TEST_ACOMPTE_001` (VALIDE) visible en règlement : L11 = 222,22 € ; reste_à_payer 367,75 = 589,97 − 222,22
- acompte fictif `ZZ_TEST_ACOMPTE_002` (A_CONTROLER) exclu du règlement : L11 = 0
- flux HC fictifs : statut non final maintenu ; REEL = COMPTABLE + HC OK (282 775,85 = 283 220,29 + (−444,44)) ; lot11 0 BLOQUANT
- 0 facture finale même avec fictif ; 12 lignes par préfacture ; GLOBAL_NON_AFFECTE hors facture
- L5 (revenu net exploitation) ≠ L9 (reste à payer) — pas de confusion exploitation/règlement (D033)
- remove exécuté ; retour baseline réel (1333 flux / REEL 283 442,51 / lot11 51 contrôles 0 BLOQUANT / lot12 269 préfactures)
- 0 `ZZ_TEST_` / 0 `__TEST_FICTIF_LOT12_A_SUPPRIMER__` ; 11 xlsx données restaurés à l'état commité

**Limites actuelles (Lot 12 = préfactures uniquement) :**
Aucune facture finale tant que `facturation_lot12_ok != OUI` (DASHBOARD_MOIS, Lot 11). Blocages :
- 44 A_CONTROLER ouverts Lot 11
- modes de facturation propriétaires à définir (12 props `mode_facturation = A_DEFINIR`)
- banque non clôturée (52 lignes RAPPROCHEMENT_REQUIS)
- sources réelles M04 / Charges / IK / Acomptes encore partielles ou vides (HORS_COMPTA réel = 0 → balise DONNEES_PARTIELLES)
- coordonnées société / logo / SIRET sous balises (absents de REF_Parametres_Generaux)

---

### CTR-2026-06-022

```
Date       : 2026-06-14
Lot        : Lot 10 — Correctif HH / HC / charges globales / acomptes
Code       : CORRECTIF_LOT10_HH_HC_GLOBAL_ACOMPTES
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot10_calculer_resultats.py
             02_TRAVAIL/lot12_seed_donnees_fictives.py
             02_TRAVAIL/lot12_remove_donnees_fictives.py
Résultat   : 4 défauts révélés par les tests fictifs (CTR-2026-06-021) corrigés :
               1. HH / VRBO ne cherchent plus de payout Hostaway.
                  - branche HH : montant depuis la saisie (total_percu, menage, taux_commission)
                  - commission HH recalculée : (total_percu - menage) × taux_commission
                  - JOINTURE_PAYOUT_MANQUANTE limité à la branche Hostaway
                  - source_type = HH ; integration seulement si total_percu renseigné + VALIDE (D-LOT10C-05)
               2. GLOBAL HORS_COMPTA n'est plus forcé à 0.
                  - vision HC calculée depuis df_hc (flux inclure_resultat_hors_compta=OUI)
                  - contrôle ajouté CTR-LOT10-20 : REEL = COMPTABLE + HORS_COMPTA
                    (BLOQUANT REEL_DIFF_COMPTABLE_PLUS_HC si écart > 1,00 EUR)
               3. Charges sans logement/proprietaire visibles en GLOBAL_NON_AFFECTE.
                  - sentinelle (plus de groupby dropna) ; pas de ventilation arbitraire
               4. Acomptes lus depuis MASTER_FACT_MAN_AcomptesProprietaires (VALIDE).
                  - intégrés UNIQUEMENT dans REGLEMENT (autres_acomptes_recus)
                  - n'impactent jamais revenu_net_exploitation / commission / net / REEL / COMPTABLE
                  - granularité mois × logement × proprietaire, fallback proprietaire (D-LOT10C-02)
             Scripts fictifs Lot 12 mis à jour (HH réactivé + acomptes ajoutés).
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Décisions appliquées (validées 2026-06-14) :**
- D-LOT10C-01 : commission HH recalculée `(total_percu − menage) × taux` ; flag A_CONTROLER si écart saisie/recalcul > 0,10 €
- D-LOT10C-02 : acomptes granularité mois × logement × proprietaire (fallback proprietaire), REGLEMENT seulement
- D-LOT10C-03 : charges globales/non-affectables → ligne dédiée GLOBAL_NON_AFFECTE (pas de ventilation)
- D-LOT10C-04 : charges Lot 3 n'impactent PAS revenu_net_exploitation (formule fermée D031), seulement REEL/COMPTABLE
- D-LOT10C-05 : HH/VRBO intégrées seulement si total_percu renseigné + VALIDE, sinon A_CONTROLER

**Contrôles ajoutés :** CTR-LOT10-20 (REEL=COMPTABLE+HC), CTR-LOT10-21 (commissions HH), CTR-LOT10-22 (contrôles HH), CTR-LOT10-23 (lignes GLOBAL_NON_AFFECTE), CTR-LOT10-24 (acomptes REGLEMENT).

**CHANGEMENT DE BASELINE — justifié (correction, pas régression) :**
```
Ancien REEL : 283 515,60 €
Nouveau REEL : 283 442,51 €
Écart        : 73,09 €
```
Justification : 8 frais bancaires (`TYPE_FLUX_016`, `sens=CHARGE`, sans `logement_id` ni `proprietaire_id`,
source `BANQUE_LOT8_IMPORT_NORM_Banque`) étaient **perdus silencieusement** par l'agrégation
(`groupby dropna=True`). Désormais visibles dans `GLOBAL_NON_AFFECTE` et inclus au total.
Les 8 lignes : 9,00 + 18,00 + 0,54 + 9,00 + 9,00 + 9,00 + 18,00 + 0,55 = **73,09 €**.
Égalité prouvée : `283 515,60 − 283 442,51 = 73,09 €`.
**Ce n'est PAS une régression** : c'est la correction du drop silencieux (défaut #3).
Produits RES Hostaway inchangés (283 709,60 €) — garde-fou respecté.

**Non-régression Hostaway confirmée (audit) :**
1321 NORMAL / payout 283 709,60 / ménage 55 619,00 / assiette 228 090,60 /
commission 39 167,42 / net avant charge 188 923,18 € — tous inchangés.

**NOTE DE CORRECTION DOCUMENTAIRE (CTR-2026-06-019) :**
L'ancien commentaire CTR-2026-06-019 indiquait à tort que les `194 €` de charges incluaient
« 4 ménages externes + 8 frais bancaires ». **Correction** : les `194 €` correspondaient aux
**4 ménages externes seulement** ; les `73,09 €` de frais bancaires étaient **perdus** avant ce
correctif. Total charges REEL réel = 194,00 + 73,09 = 267,09 €.

**Résultats test fictif (cycle complet, avant restauration) :**
- baseline 1333 flux / REEL 283 442,51 / REEL=COMPTABLE+HC OK / 0 BLOQUANT
- avec fictif 1342 flux / NORMAL 1323 (HA 1321 + HH 2) / REEL 282 775,85 / COMPTABLE 283 220,29 /
  HORS_COMPTA −444,44 / REEL=COMPTABLE+HC OK (écart 0) / GLOBAL_NON_AFFECTE 3 / acomptes 222,22 REGLEMENT / lot11 0 BLOQUANT
- après suppression : retour 1333 / REEL 283 442,51 / 0 fictif résiduel
- 11 xlsx données restaurés à l'état commité (0 Excel suivi modifié)

---

### CTR-2026-06-021

```
Date       : 2026-06-14
Lot        : Lot 9 — Correctif ingestion Charges + M04 + outils test Lot 12
Code       : CORRECTIF_LOT9_INGESTION_CHARGES_M04
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot9_construire_flux.py
             02_TRAVAIL/lot12_seed_donnees_fictives.py
             02_TRAVAIL/lot12_remove_donnees_fictives.py
Résultat   : Lot 9 étend l'ingestion du Flux avec deux sources jusqu'ici non câblées :
               - Charges (Lot 3, MASTER_FACT_MAN_Charges) — statut_controle = VALIDE
                 sens / code_impact / type_flux portés par la ligne
               - M04 ménages internes (Lot 6b, M04_MENAGES_PowerQuery) — statut_controle = VALIDE
                 CHARGE, code_impact = HC (M2 verrouillé), TYPE_FLUX_013
             IK exclu du Flux (conforme décision « IK hors Flux / vue dédiée », §15.3).
             Acomptes différés à la passe Lot 10.
             Garde-fou ajouté : CTR-9-011 — portion RES (TYPE_FLUX_017) inchangée (BLOQUANT si écart).
             Test fictif réalisé (seed / pipeline / remove) :
               - baseline               : 1 333 flux
               - avec données fictives   : 1 340 flux (5 CHG + 1 M04 + 1 MenExt + 2 HH différées exclues)
               - après suppression       : retour 1 333 flux
               - 0 donnée fictive résiduelle (scan ZZ_TEST_ / tag sur 30 Excel suivis)
             Sources amont en lecture seule.
             Données Excel restaurées à l'état commité (git checkout après test).
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Outils de test fictif Lot 12 :**
- `lot12_seed_donnees_fictives.py` : insertion idempotente, backup avant écriture (99_ARCHIVES/LOT12_TEST_DATA/), tag obligatoire, append-only (aucune ligne réelle modifiée).
- `lot12_remove_donnees_fictives.py` : suppression par ID `ZZ_TEST_` OU tag `__TEST_FICTIF_LOT12_A_SUPPRIMER__`, backup avant suppression, vérification 0 résiduel.
- IDs fictifs : préfixe `ZZ_TEST_`. Tag : `__TEST_FICTIF_LOT12_A_SUPPRIMER__`. Montants reconnaissables : 111.11 / 222.22 / 333.33.
- Backups fictif (`99_ARCHIVES/LOT12_TEST_DATA/`) ignorés git (règle #10 — jamais versionnés).

**PRÉREQUIS BLOQUANT — ne PAS peupler Charges/M04 avec des données réelles HC avant le correctif Lot 10.**
Les tests fictifs ont révélé 4 points à traiter dans la passe Lot 10 :
1. Lot 10 `GLOBAL HORS_COMPTA` reste à 0 même si des flux HC existent → casse l'égalité REEL = COMPTABLE + HC (capté en BLOQUANT par CTR-LOT11 — preuve que les contrôles fonctionnent).
2. Certaines charges globales / non affectables (sans `logement_id`) sont perdues dans l'agrégation des résultats Lot 10.
3. HH (réservations hors Hostaway) doit être géré par le montant saisi (total_perçu − ménage − commission), pas par le payout Hostaway (absent → JOINTURE_PAYOUT_MANQUANTE).
4. Acomptes doivent aller uniquement dans le bloc REGLEMENT, jamais dans l'exploitation (D031/D032/D033).

Tant que Charges/M04 restent vides, le correctif Lot 9 est **neutre** (baseline 1 333 inchangée, vérifié).

---

### CTR-2026-06-020

```
Date       : 2026-06-14
Lot        : Lot 11 — Contrôles de cohérence globaux
Code       : CONSTRUCTION_LOT11_CONTROLES_COHERENCE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx
             02_TRAVAIL/lot11_controles_coherence.py
Résultat   : Script exécuté. 51 contrôles générés. 0 BLOQUANT. 44 A_CONTROLER. 7 INFO.
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Résultats partiels — M04/Charges/IK/HH/Acomptes non encore alimentés.
             0 BLOQUANT : aucun blocage structurel.
             44 A_CONTROLER à traiter/valider humainement avant facturation Lot 12.
             AUCUN mois facturable (facturation_lot12_ok = NON_SOUS_RESERVE_A_CONTROLER partout).
             Banque disponible localement (BANQUE_DISPONIBLE).
```

**Corrections appliquées après audit (2026-06-14) :**
- F1 (obligatoire) : `facturation_lot12_ok` = OUI uniquement si 0 BLOQUANT ET 0 A_CONTROLER ET mois banque CLOTURE.
  Sinon valeur prudente (NON_BLOQUANT_OUVERT / NON_SOUS_RESERVE_A_CONTROLER / NON_CLOTURE_INCOMPLETE / NON_DONNEES_INCOMPLETES).
- F2 (obligatoire) : `_is_empty()` détecte désormais les lignes placeholder Power Query
  ("Alimenté par Power Query", "Charge par Power Query", "# formule", préfixes techniques).
  CHARGES/M04/IK/HH désormais correctement détectées VIDES → INFO 3 → 7.
- F3 (cosmétique) : code mort `vrbo_ac` supprimé ; print "sur N mois banque" dynamique ;
  DASHBOARD limité aux mois porteurs de contrôles + mois banque + TRANSVERSE (plus de mois vides arbitraires).

**Décisions appliquées :**
- D-LOT11-01 Banque : Option C adaptative — fichier Lot 8 lu en local, contrôles banque exécutés
- D-LOT11-02 REF_Cloture_Mensuelle : non modifiée (dashboard dans DASHBOARD_MOIS)
- D-LOT11-03 Rapprochement payout/banque : indicatif, non bloquant (statut INFORMATIF)
- D-LOT11-04 Re-vérification indépendante depuis tables sources (pas de reprise Lot 10)
- D-LOT11-05 CAISSE_THEORIQUE : produite avec 0 + flag CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES

**Contrôles BLOQUANTS : 0**
```
CTR-LOT11-BL-01  PK_MANQUANTE_OU_DOUBLONNEE                    : 0 — OK (Flux/Res/Payout/Commissions)
CTR-LOT11-BL-02  DOUBLON_RESERVATION_FLUX                      : 0 — OK
CTR-LOT11-BL-03  JOINTURE_RESERVATIONS_MANQUANTE               : 0 — OK
CTR-LOT11-BL-04  JOINTURE_PAYOUT_MANQUANTE                     : 0 — OK
CTR-LOT11-BL-05  REVENU_NET_EXPLOITATION_INCOHERENT            : 0 — OK (formule payout-menage-com-cf)
CTR-LOT11-BL-06  CONFUSION_PAYOUT_SOLDE_FACTURE                : 0 — OK
CTR-LOT11-BL-07  PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT          : 0 — OK
CTR-LOT11-BL-08  COMMISSION_INCOHERENTE                        : 0 — OK
CTR-LOT11-BL-09  ASSIETTE_COMMISSION_INCOHERENTE               : 0 — OK
CTR-LOT11-BL-10  REEL_INCOHERENT_VS_COMPTABLE_PLUS_HC         : 0 — OK (283515.60 = 283515.60 + 0)
CTR-LOT11-BL-11  CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE       : 0 — OK (forfait REF = charge_fixe REGLEMENT)
CTR-LOT11-BL-12  BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY        : 0 — OK (0 flux PRODUIT banque)
CTR-LOT11-BL-13  COMMISSION_SANS_TAUX                         : 0 — OK (tous taux renseignés)
```

**Contrôles A_CONTROLER : 44 (répartition)**
```
CTR-LOT11-AC-01  LISTING_ORPHELIN_A_CONTROLER        : 23 lignes (reservations listingMapId 515523 / 556954)
                 → Source : MASTER_CTRL_HA_Anomalies — depuis Lot 1, statut OUVERT
                 → Action : confirmer logement inactif + supprimer ou mapper dans REF_Logements
CTR-LOT11-AC-02  CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE : 14 logements (trace historique ; contr?le d?commissionn? le 2026-06-29)
                 → 14 vs 12 en Lot 10 : Lot 11 vérifie TOUS les logements avec TYPE_FLUX_017
                   (Lot 10 vérifiait seulement logements avec forfait>0)
                 ? Action historique : corriger date_entree_gestion dans REF_Logements. D?commissionn? le 2026-06-29 : colonnes supprim?es, REF_Gestion_Logements_Hist source unique.
CTR-LOT11-AC-03  CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE : 3 (mois 2026-02/03/04)
                 → 52 lignes RAPPROCHEMENT_REQUIS dans NORM_Banque
                 → Action : exporter données Airbnb, rapprocher RAPPROCH_AIRBNB_ATTENTE (Lot 8c)
CTR-LOT11-AC-04  LOG_SANS_FLUX_017                   : 1 (LOG_0009 — forfait=40€, 0 réservation Flux)
                 → Re-détecté indépendamment — confirmé
                 → Action : vérifier si LOG_0009 actif / a eu des réservations
CTR-LOT11-AC-05  VRBO_MONTANT_NON_RENSEIGNE          : 1 entrée agrégée (32 réservations VRBO)
                 → Action : saisie manuelle dans SAISIE_ReservationsHorsHostaway.xlsx (Lots 4/4bis)
CTR-LOT11-AC-06  RESERVATION_A_CONTROLER_SANS_COMMISSION : 1 entrée agrégée (59 réservations)
                 → Net propriétaire incomplet pour 4% des réservations
                 → Action : Lots 4/4bis + décision par réservation
CTR-LOT11-AC-07  MENAGE_EXTERNE_DATE_ABSENTE         : 1 (9 lignes Aissata fac.2026-37)
                 → Action : préciser date_menage auprès du prestataire
```

**Contrôles INFO : 7**
```
CTR-LOT11-IN-01  HC_ZERO_SOURCES_VIDES (CHARGES)     : SAISIE_Charges_Flux non peuplée — placeholder PQ
CTR-LOT11-IN-02  HC_ZERO_SOURCES_VIDES (M04)         : M04 SOURCE_RAW vide — placeholder PQ
CTR-LOT11-IN-03  HC_ZERO_SOURCES_VIDES (ACOMPTES)    : acomptes non saisis — 0 attendu
CTR-LOT11-IN-04  HC_ZERO_SOURCES_VIDES (IK)          : IK/avantages non saisis — placeholder formule
CTR-LOT11-IN-05  HC_ZERO_SOURCES_VIDES (HH)          : ReservationsHorsHostaway vide — placeholder PQ
CTR-LOT11-IN-06  HC_ZERO_SOURCES_VIDES (TRANSVERSE)  : HORS_COMPTA = 0 global — M04/Charges/IK vides
CTR-LOT11-IN-07  MODE_FACTURATION_A_DEFINIR          : 12 propriétaires — bloquant pour Lot 12
```
> Note F2 : INFO 3 → 7 après correction de la détection placeholder Power Query.
> CHARGES/M04/IK/HH ne contenaient qu'une ligne d'instruction technique (pas de donnée métier).

**DASHBOARD_MOIS (4 lignes — F1/F3 appliqués)**
```
mois        BL  AC INFO  statut_banque  facturation_lot12_ok
2026-02      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
2026-03      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
2026-04      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
TRANSVERSE   0  41   7    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
```
> AUCUN mois marqué OUI. Facturation Lot 12 interdite tant que A_CONTROLER non traités
> et clôture banque non validée (REGLES §11 C5/C6/C7).

**Rapprochement payout / banque (indicatif)**
```
Banque statut : BANQUE_DISPONIBLE (fichier Lot 8 présent localement)
Mois 2026-02  : banque=197.03€ / HA=14020.13€ / écart=-13823.10€
Mois 2026-03  : banque=1983.79€ / HA=16752.21€ / écart=-14768.42€
Mois 2026-04  : banque=1341.36€ / HA=18888.65€ / écart=-17547.29€
NOTE : écarts normaux — mois Hostaway = check-in date, banque = date virement réel
       (décalage 1-2 mois). 39 virements Airbnb EN_ATTENTE_EXPORT_AIRBNB dans RAPPROCH_AIRBNB_ATTENTE.
```

**Caisse théorique**
```
Solde : 0.00 EUR — CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES
HH/Charges/Acomptes vides → structure stable pour recalculs futurs.
```

**Sources amont :** AUCUNE modification (toutes read_only=True)
- MASTER_CALC_Flux.xlsx ✓   MASTER_CALC_Reservations.xlsx ✓   MASTER_CALC_HA_Payout.xlsx ✓
- MASTER_CALC_Commissions.xlsx ✓   MASTER_CALC_Resultats.xlsx ✓   MASTER_CALC_NetProprietaire.xlsx ✓
- REF_Setup.xlsm ✓   Lot8_Banque/ ✓   Fichiers bancaires bruts ✓

---

### CTR-2026-06-019

```
Date       : 2026-06-13
Lot        : Lot 10 — Résultats, commissions & net propriétaire
Code       : CONSTRUCTION_LOT10_COMMISSIONS_RESULTATS_NET
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
             02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
             02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx

Script     : 02_TRAVAIL/lot10_calculer_resultats.py

Jointure confirmée par test valeurs réelles :
  MASTER_CALC_Flux.source_pk         = "RES-2025-01-HA-001" (reservation_calc_id)
    -> MASTER_CALC_Reservations.reservation_calc_id
    -> MASTER_CALC_Reservations.reservation_id_hostaway (entier, ex. 60504160)
    -> MASTER_CALC_HA_Payout.reservation_id             (même format entier)

Contrôles exécutés (19 points) :
  CTR-LOT10-01  Flux lus depuis MASTER_CALC_Flux              : 1 333 [OK]
  CTR-LOT10-02  Reservations NORMAL intégrées aux commissions  : 1 321 [OK]
  CTR-LOT10-03  Reservations A_CONTROLER exclues               : 59 [OK]
  CTR-LOT10-04  Total payout calculé (NORMAL)                  : 283 709.60 €
  CTR-LOT10-05  Total ménage retenu                            : 55 619.00 €
  CTR-LOT10-06  Total assiette commission                       : 228 090.60 €
                Vérification : 283 709.60 - 55 619.00 = 228 090.60 [OK]
  CTR-LOT10-07  Total commission conciergerie                   : 39 167.42 €
  CTR-LOT10-08  Total net propriétaire avant charge fixe        : 188 923.18 €
                Vérification : 228 090.60 - 39 167.42 = 188 923.18 [OK]
  CTR-LOT10-09  Total charge fixe mensuelle générée             : 7 645.00 €
                (233 lignes mois x logement / 13 logements avec forfait > 0)
  CTR-LOT10-10  Total net propriétaire après charge fixe        : 181 278.18 €
                Vérification : 188 923.18 - 7 645.00 = 181 278.18 [OK]
  CTR-LOT10-11  Résultat REEL global (Flux)                    : 283 515.60 €
                Note : différence avec payout (194.00 €) = charges Flux (4 ménages + 8 frais bancaires)
  CTR-LOT10-12  Résultat COMPTABLE global (Flux)               : 283 515.60 €
                REEL = COMPTABLE [attendu — tout IC à ce stade]
  CTR-LOT10-13  Résultat HORS_COMPTA global (Flux)             : 0.00 € [HC_ZERO_SOURCES_VIDES]
                Sources vides : M04 / Charges / IK / Acomptes non alimentés
  CTR-LOT10-14  PAR_MOIS_LOGEMENT total / dont REEL            : 497 lignes / 248 REEL [OK]
                (248 REEL + 248 COMPTABLE + 1 HORS_COMPTA placeholder)
  CTR-LOT10-15  PAR_MOIS_PROPRIETAIRE total / dont REEL        : 404 lignes / 202 REEL [OK]
                (202 REEL + 202 COMPTABLE)
  CTR-LOT10-16  CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE    : 12 logements [trace historique ; contr?le d?commissionn? le 2026-06-29]
                Cause historique : date_entree_gestion REF = 2026-01-01 pour tous logements,
                        mais réservations Flux remontent à 2025.
                        date_entree_gestion = date cr?ation SAS/r?f?rentiel, pas d?but r?el de gestion. D?commissionn? le 2026-06-29 : colonne supprim?e.
                        Décision : ne pas corriger REF_Setup maintenant — lot séparé si nécessaire.
  CTR-LOT10-17  LOG_SANS_FLUX_017                              : 1 logement [A_CONTROLER]
                LOG_0009 (T3 Montaudran) : forfait=40€ mais aucune réservation TYPE_FLUX_017 dans Flux.
                A vérifier : logement effectivement en gestion ? Données manquantes ?
  CTR-LOT10-18  Contrôles BLOQUANTS détectés                   : 0 [OK]
  CTR-LOT10-19  Sources amont (lecture seule)                  : [OK]
                MASTER_CALC_Flux / MASTER_CALC_Reservations / MASTER_CALC_HA_Payout / REF_Setup.xlsm
                non modifiés — vérifiés par git status (0 fichier M)

Charge fixe mensuelle — règle appliquée (Option A, D-LOT10-04) :
  - Source : REF_Logements.forfait_logiciel_consommables_mensuel
  - Exposé : charge_fixe_mensuelle
  - Début  : premier mois TYPE_FLUX_017 dans MASTER_CALC_Flux par logement
  - Fin    : trace historique date_sortie_gestion ; r?gle d?commissionn?e le 2026-06-29, fin de gestion r?solue par REF_Gestion_Logements_Hist
  - Cas LOG_0003 (actif=NON, sortie=2026-04-26) : charge fixe arrêtée à 2026-04 [OK]
  - Logements à forfait=0 : aucune ligne générée (LOG_0004, LOG_0016, LOG_0017, divers)

Séparation exploitation / règlement :
  - EXPLOITATION : charge_fixe=0 par réservation (non proratisée — D-LOT10-01)
  - REGLEMENT    : charge_fixe 1x par mois x logement actif
  - Acomptes     : 0 (sources vides — A_CONTROLER sur tous les reste_a_payer)

Fichiers créés :
  - 02_TRAVAIL/lot10_calculer_resultats.py
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
    (onglets : COMMISSIONS 1321L + A_CONTROLER 59L)
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
    (onglets : PAR_MOIS_LOGEMENT + PAR_MOIS_PROPRIETAIRE + GLOBAL)
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx
    (onglets : EXPLOITATION + REGLEMENT 269L + VUE_MOIS 220L)

Fichiers non modifiés : tout le reste (Flux, Reservations, Payout, REF_Setup, banque, Lot 9)
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 11 ne peut pas démarrer avant validation + commit Lot 10 (D029).
             LOG_0009 et date_entree_gestion : trace historique ; lot r?alis? le 2026-06-29 par suppression des colonnes doublons.
```

---

### CTR-2026-06-017

```
Date       : 2026-06-11
Lot        : Lot 9 — Table de flux unifiée MASTER_CALC_Flux
Code       : CONSTRUCTION_MASTER_CALC_FLUX_LOT9
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx
Résultat   : Construction sur données réelles. Script lot9_construire_flux.py exécuté.
             10 contrôles BLOQUANTS tous OK.

             Volumes produits :
               - RES : 1 321 flux (TYPE_FLUX_017, PRODUIT, IC — VUE_FLUX MASTER_CALC_Reservations)
               - MEN : 4 flux (TYPE_FLUX_014, CHARGE, IC — MenagesExternes VALIDE 2026-05)
               - BNQ : 8 flux (TYPE_FLUX_016, CHARGE, IC — frais bancaires Lot 8 VALIDE)
               - TOTAL : 1 333 flux / 22 colonnes / 0 doublon / 0 montant négatif

             Sources :
               - MASTER_CALC_Reservations VUE_FLUX : 1 321 lignes VALIDE
                 (AIRBNB 1235 / BOOKING 86 — tous IC, PRODUIT, TYPE_FLUX_017)
               - MASTER_FACT_MEN_MenagesExternes MASTER VALIDE : 4 lignes
                 (MENEXT-2026-05-AISSATA-001/007/008/009 — IC, CHARGE, 29+55+55+55=194€)
               - BANQUE_LOT8_IMPORT NORM_Banque TYPE_FLUX_016 VALIDE : 8 lignes
                 (frais bancaires — IC, CHARGE, commentaire générique sans libellé brut)

             Contrôles BLOQUANTS :
               CTR-9-001 : 3 fichiers sources présents [OK]
               CTR-9-002 : VUE_FLUX non vide [OK]
               CTR-9-003 : VUE_FLUX volume >= 1000 (1321) [OK]
               CTR-9-004 : 0 montant négatif [OK]
               CTR-9-005 : tous sens valides (PRODUIT/CHARGE) [OK]
               CTR-9-006 : tous code_impact valides (IC uniquement) [OK]
               CTR-9-007 : 1333 flux_id uniques [OK]
               CTR-9-008 : 0 doublon source technique [OK]
               CTR-9-009 : volume = 1321+4+8 = 1333 [OK]
               CTR-9-010 : aucune colonne bancaire sensible dans schema [OK]

             Sécurité bancaire : libelle, libelle_brut, compte_id absents de MASTER_CALC_Flux.xlsx [OK]
             Commentaire frais bancaires : "Frais bancaires validés Lot 8" (générique) [OK]

             TYPE_FLUX_017 = REVENU_RESERVATION_HOSTAWAY créé dans REF_Setup.xlsm.
             Backup REF_Setup : 99_ARCHIVES/LOT9_FluxUnifie/REF_Setup_backup_lot9_20260611_213323.xlsm

             Répartition par mois : 2025-01 (7) à 2027-02 (2) — 23 mois couverts
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Sources vides à ce stade : IK (0), Acomptes (0), Charges (0), M04 (0).
             Ces sources alimenteront MASTER_CALC_Flux lors de leur remplissage — re-run idempotent.
```

---

### CTR-2026-06-018

```
Date       : 2026-06-13
Lot        : Lot 1 — Correctif Payout final : menage_retenu date-aware + REF historique 2025
Code       : CORRECTIF_LOT1_PAYOUT_MENAGE_REF_SETUP
Sévérité   : BLOQUANT (bug bloquant Lot 10)
Fichier    : 02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx

--- Diagnostic initial ---
Bug identifié : menage_retenu = 0 pour 1 235 réservations Airbnb NORMAL.
Cause : _airbnb() lisait ffd.get("cleaningFee") — finance fields Airbnb vides → 0.0.

--- Correctif #1 (invalidé) ---
Tentative : menage_retenu = cleaningFee_res (champ API Hostaway, prix plateforme voyageur).
Résultat diagnostic : cleaningFee_res = prix facturé au voyageur (Studio 40€ vs std 29€, etc.)
                       ≠ coût standard conciergerie. 1 233 / 1 235 lignes avec écart individuel.
Décision humaine : cleaningFee_res non valide comme source de menage_retenu.
                   Règle métier : menage_retenu = REF_Couts_Standards_Menage uniquement.

--- Correctif #2 (intermédiaire, insuffisant) ---
Source : REF_Couts_Standards_Menage sans contrôle de validité temporelle.
Problème : 814 réservations Airbnb NORMAL avec checkInDate < 2026-01-01 tombaient à
           menage_retenu = 0 dès qu'un lookup date-aware était appliqué.
           Coût standard REF_Setup en vigueur depuis 2026-01-01 uniquement → trou historique 2025.
Supplanté par correctif final.

--- Correctif final — RETENU (2026-06-13) ---
Approche en deux volets :
  1. Lookup date-aware : menage_retenu sélectionné selon
       date_debut_validite <= checkInDate AND (date_fin_validite IS NULL OR checkInDate <= date_fin_validite)
       Date de référence = checkInDate de la réservation (jamais date du jour / date recalcul).
  2. Extension REF_Couts_Standards_Menage avec 5 lignes historiques 2025 :
       COUT_STD_2025_TYPE_001 | TYPE_001 | 29€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_002 | TYPE_002 | 39€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_003 | TYPE_003 | 55€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_004 | TYPE_004 | 69€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_005 | TYPE_005 | 110€ | 2025-01-01 → 2025-12-31
     Mêmes montants que 2026 — aucun tarif différent connu pour 2025.
     Lignes 2026 (COUT_MEN_001 à COUT_MEN_005 — date_fin_validite NULL) inchangées.
     Aucun chevauchement : 2025-12-31 < 2026-01-01.

Périmètre : AIRBNB NORMAL + BOOKING NORMAL (règle homogène toutes plateformes).
Annulations avec payout : menage_retenu = 0 conservé (D030 irrévocable).
Script : lot1_hostaway_extract.py — PayoutCalculator utilise _lookup_menage_by_date()
         (DataFrame date-aware remplace dict simple). Retourne 5-tuple + meta_dict.
Mode : --recalc-payout-only --payout-source <backup> (sans relance API).
Backup source : 99_ARCHIVES/LOT1_Hostaway/MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
REF_Setup backup : 99_ARCHIVES/REF_Setup_BACKUP_20260613_175507.xlsm

8 colonnes de traçabilité ajoutées dans MASTER_CALC_HA_Payout.xlsx :
  menage_retenu_source, cout_standard_id, cout_standard_menage_snapshot,
  cout_standard_date_debut_validite, cout_standard_date_fin_validite,
  logement_id_snapshot, type_logement_id_snapshot, date_reference_cout_menage

19 contrôles obligatoires (correctif final) :
  CTR-1   Lignes Airbnb NORMAL traitées         : 1 235 [OK]
  CTR-2   Lignes Booking NORMAL traitées        : 86 [OK]
  CTR-3   menage_retenu Airbnb AVANT (bug=0)    : 0.00 €
  CTR-4   menage_retenu Airbnb APRÈS REF_Setup  : 51 727.00 €
  CTR-5   menage_retenu Booking AVANT           : 3 810.00 € (était cleaningFee_res)
  CTR-6   menage_retenu Booking APRÈS REF_Setup : 3 892.00 € (delta +82€)
  CTR-7   assiette Airbnb AVANT                 : 263 043.22 €
  CTR-8   assiette Airbnb APRÈS                 : 211 316.22 € (delta -51 727€)
  CTR-9   assiette Booking AVANT                : 16 856.38 €
  CTR-10  assiette Booking APRÈS                : 16 774.38 € (delta -82€)
  CTR-11  Écart cleaningFee_res vs cout_std AB  : -501.00 € (cout_std légèrement > cln_res 2026)
  CTR-12  Lignes NORMAL sans cout_standard      : 0 [OK]
  CTR-13  Annulations avec payout intactes      : OK (D030)
  CTR-14  Impact estimé commissions (~15%)      : -7 771.35 € (assiette réduite 51 809€)
           API non relancée                     : OUI
           Source utilisée                      : MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
  CTR-15  Doublons validité BLOQUANT            : 0 [OK]
  CTR-16  Airbnb NORMAL avec cout_standard_id   : 1 235 / 1 235 [OK]
  CTR-17  Booking NORMAL avec cout_standard_id  : 86 / 86 [OK]
  CTR-18  Snapshot == menage_retenu (NORMAL)    : 1 321 / 1 321 [OK]
  CTR-19  date_reference non vide (NORMAL)      : 1 321 / 1 321 [OK]
           git status                           : 6 fichiers M — aucun commit

Anomalie connexe (non bloquante ce lot) :
  LOG_0009 "T3 Montaudran" type = TYPE_002 (T2 — 39€) alors que nom dit T3.
  Impact nul sur correctif (listingMapId concerné bien couvert par cout_standard TYPE_002).
  À décider séparément (correction REF_Logements ou maintien).

Fichiers modifiés :
  - 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (+5 lignes historiques 2025 REF_Couts_Standards_Menage)
  - 02_TRAVAIL/lot1_hostaway_extract.py (refactor complet : _build_cost_ref_df dates,
    load_menage_cost_ref→DataFrame, _lookup_menage_by_date nouveau, PayoutCalculator 5-tuples,
    _META_NON_APPLICABLE, recalc_payout_only date-aware + 8 col, main 5-tuple + nouveaux champs)
  - 02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx (correctif final appliqué, 8 col tracabilité)
  - .gitignore (99_ARCHIVES/LOT1_Hostaway/ ajouté)
  - 00_CADRAGE/ETAT_AVANCEMENT.md (session 17)
  - 00_CADRAGE/JOURNAL_CONTROLES.md (ce fichier)

Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 10 reste bloqué jusqu'à validation humaine du correctif final.
             Impact réel commissions = taux propriétaire × (51 727 + 82) € — à confirmer Lot 10.
             Taux de commission dans REF_Proprietaires ; charges fixes mensuelles dans REF_Logements — non dans ce fichier.
```

---

### CTR-2026-06-001

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : AUDIT_REF_SETUP_LOT0_INITIAL
Sévérité   : BLOQUANT
Fichier    : 01_SOURCES_BRUTES/REF_Setup.xlsx.xlsm (chemin pré-correction)
Résultat   : Audit sur données réelles. 19 onglets présents, 0 doublon de clé, 0 date série brute.
             6 anomalies bloquantes détectées :
             B1 - Mojibake (REF_Associes, REF_Codes_Impact, REF_Types_Flux, REF_Types_Affectation — 26 cellules)
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE absents
             B3 - REF_Statuts_Payout : onglet absent
             B4 - REF_Cloture_Mensuelle : onglet absent
             B5 - REF_Parametres_Generaux : 4 params manquants (TAUX_HORAIRE_MENAGE_INTERNE,
                  ARRONDI_DECIMALES, TOLERANCE_ARRONDI_LIGNE_EUR, TOLERANCE_ARRONDI_CUMUL_EUR)
             B6 - REF_Intervenants : colonnes nom_normalise / date_debut_validite / date_fin_validite
                  absentes, type_intervenant en minuscules au lieu de MAJUSCULES
             Anomalie importante I1 : APPARTEMENT_DIVERS et LOGEMENT_DIVERS absents de REF_Logements
             ANO-2026-06-003 (dates série) : sans objet, aucune série brute détectée
Statut     : CORRIGÉ (voir CTR-2026-06-002)
Commentaire: Lot 0 non validable avant correction. 6 bloquants + 2 onglets à créer.
```

---

### CTR-2026-06-002

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : CORRECTIONS_REF_SETUP_LOT0
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (chemin canonique post-correction)
Résultat   : Corrections B1–B6 + I1 appliquées par script Python (lot0_corrections.py).
             Sauvegarde horodatée créée : 99_ARCHIVES/LOT0_REF_Setup/REF_Setup_BACKUP_20260607_113223.xlsx.xlsm
             Fichier déplacé vers chemin canonique : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
             B1 - Mojibake corrigé : 26 cellules (4 onglets)
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE ajoutés (famille statut_controle)
             B3 - REF_Statuts_Payout : créé (6 valeurs fermées)
             B4 - REF_Cloture_Mensuelle : créé (structure vide, 7 colonnes)
             B5 - REF_Parametres_Generaux : 4 params ajoutés (TAUX_HORAIRE_MENAGE_INTERNE=10,
                  ARRONDI_DECIMALES=2, TOLERANCE_ARRONDI_LIGNE_EUR=0.10, TOLERANCE_ARRONDI_CUMUL_EUR=1.00)
             B6 - REF_Intervenants : 3 colonnes ajoutées, type_intervenant normalisé en MAJUSCULES,
                  nom_normalise calculé (IMENE, KHEIRA, MOUNIR, AISSATA, IMRANE)
             I1 - REF_Logements : APPARTEMENT_DIVERS et LOGEMENT_DIVERS ajoutés
Statut     : CORRIGÉ (audit post-correction : CTR-2026-06-003)
Commentaire: Script reproductible conservé dans 02_TRAVAIL/lot0_corrections.py
```

---

### CTR-2026-06-003

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : AUDIT_REF_SETUP_LOT0_POST_CORRECTION
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Audit post-correction complet. 21 onglets (19 originaux + REF_Statuts_Payout + REF_Cloture_Mensuelle).
             B1 - Mojibake : 0 cellule résiduelle — OK
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE / A_CONTROLER présents — OK
             B3 - REF_Statuts_Payout : 6 valeurs fermées — OK
             B4 - REF_Cloture_Mensuelle : 7 colonnes, structure vide — OK
             B5 - REF_Parametres_Generaux : 7 params dont les 4 requis — OK
             B6 - REF_Intervenants : 11 colonnes, tous types MAJUSCULES, nom_normalise renseigné — OK
             I1 - REF_Logements : 19 logements dont APPARTEMENT_DIVERS et LOGEMENT_DIVERS — OK
             0 doublon de clé sur 8 onglets vérifiés — OK
             0 date série brute — OK
             Points restants non bloquants : CARTE_002 suffixe XXXX (Lot 8), mode_facturation A_DEFINIR (Lot 12).
             Questions métier tranchées : QM1 (coûts standards = exécution seule), QM2 (A_DEFINIR OK), QM3 (A_CONTROLER OK).
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 0 techniquement validable. Attente accord humain avant marquage FAIT dans ETAT_AVANCEMENT.md.
```

---

### CTR-2026-06-004

```
Date       : 2026-06-08
Lot        : Lot 1 — Module Hostaway (extraction + payout)
Code       : AUDIT_LOT1_HOSTAWAY_POST_FIX_FINANCEFIELD
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot1_Hostaway/ (run 20260608_134253)
Résultat   : Extraction complète Lot 1 sur données réelles. Fix financeField appliqué (1 ligne :
             _ff_from_res(detail) au lieu de parse_finance_fields(detail.get("money", {}))).
             17 listings (14 actifs, 3 archivés : 485104, 515523, 556954).
             1586 réservations API. 1391 traitées, 195 sautées (annulations sans montant — correct).
             86 appels détail Booking uniquement (6% — mode minimal validé).
             676 finance fields extraits (100% Booking — Airbnb via airbnbExpectedPayoutAmount).
             Payout : 1321 NORMAL, 59 A_CONTROLER (32 VRBO Unknown + 27 DIRECT), 0 INCOMPLET, 0 ABSENT.
             86 Booking 100% NORMAL via formule H2 (totalPriceFromChannel_formula) — fix confirmé.
             1235 Airbnb 100% NORMAL via airbnbExpectedPayoutAmount.
             55 anomalies A_CONTROLER (0 BLOQUANT) :
             - 32 VRBO_MONTANT_NON_RENSEIGNE (→ Lot 4)
             - 23 LISTING_ORPHELIN_A_CONTROLER (listings 515523 + 556954 absents REF — → Lot 2)
             CleaningTasks SKIPPED — à traiter Lot 6a via --only-cleaning-tasks.
             Points résiduels non bloquants :
             - ANO-004 mis à jour 29→32 VRBO
             - ANO-014 créée (listing 556954 T3 Montaudran, archivé, absent REF_Logements)
             - 59 A_CONTROLER à saisir manuellement (Lots 4 / 4bis)
Statut     : VALIDE
Commentaire: Lot 1 validé humainement le 2026-06-08. Script : 02_TRAVAIL/lot1_hostaway_extract.py.
```

---

### CTR-2026-06-005

```
Date       : 2026-06-08
Lot        : Lot 2 — Réconciliation logements
Code       : AUDIT_LOT2_MAPPING_LOGEMENTS
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (REF_Logements + REF_Mapping_Logements)
             02_TRAVAIL/Lot1_Hostaway/MASTER_REF_HA_Listings.xlsx
Résultat   : Réconciliation REF_Logements ↔ MASTER_REF_HA_Listings.
             17 listings Hostaway — 17/17 mappés, 0 orphelin résiduel.
             4 anomalies ANO-001/005/006/014 résolues par correction de mapping (pas de création de logement).
             Cause commune : deux logements (LOG_0009, LOG_0016) avaient été retirés puis recréés
             dans Hostaway → nouveaux listingMapId (556954, 515523) non encore reflétés dans le REF.
             Corrections appliquées :
             - LOG_0009 : hostaway_listing_id 497801 → 556954 ; nom "T3 Montaudran" ; commentaire ancien nom.
             - LOG_0016 : hostaway_listing_id 480780 → 515523 ; sur_hostaway NON → OUI.
             - REF_Mapping_Logements : 5 lignes ajoutées (MAP_LOG_0082 à 0086), total 86 lignes.
             - Anciens IDs 480780 et 497801 conservés comme alias historiques actifs (MAP_LOG_0039/0073).
             Contrôles vérifiés :
             - 0 listingMapId actif Hostaway non mappé.
             - Anciens IDs 480780 / 497801 résolvables → logement_id correct.
             - LOG_0009 et LOG_0016 : hostaway_listing_id, sur_hostaway, nom conformes post-correction.
             Google Sheet Suivi ménage : reporté au Lot 6b (M04) — non traité ici.
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 2 techniquement validé. Attente accord humain avant marquage FAIT dans ETAT_AVANCEMENT.md.
             Backup : 99_ARCHIVES/LOT2_Mapping/REF_Setup_BACKUP_20260608_153507.xlsm
```

---

### CTR-2026-06-006

```
Date       : 2026-06-08
Lot        : Lot 3 — SAISIE_Charges_Flux.xlsx
Code       : AUDIT_LOT3_SAISIE_MASTER_CHARGES
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx
             02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx
Résultat   : Contrôle final Lot 3 — 21 points vérifiés, 0 FAIL.
             REF_Setup.xlsm :
             - REF_Categories_Charges : 23 lignes, col 9 filtre_vue_menage présente.
             - CHG_016 filtre_vue_menage=NON ; CHG_021/022/023 présents ; CHG_023 filtre_vue_menage=OUI.
             - REF_Types_Flux : TYPE_FLUX_009 (ACHAT_MENAGE), 010 (FRAIS_LOCAL),
               011 (CHARGE_EXCEPTIONNELLE_REFACTURABLE), 012 (CHARGE_RECURRENTE_REFACTURABLE) présents.
             - REF_Charges_Recurrentes : REC_001 → TYPE_FLUX_012, REC_002 → TYPE_FLUX_010.
             SAISIE_Charges_Flux.xlsx :
             - 4 onglets (SAISIE, REF_LOCALE, CONTROLES_SAISIE, README).
             - 31 colonnes SAISIE conformes au cadrage validé (groupes 1 à 11).
             - 18 listes REF_LOCALE avec Named Ranges.
             - 13 contrôles CONTROLES_SAISIE.
             - Formules calculées sur 500 lignes : mois, impact_resultat_reel,
               impact_resultat_comptable, ROW_HASH.
             MASTER_FACT_MAN_Charges.xlsx :
             - 3 onglets (MASTER 37 cols, VUE_MENAGE 37 cols identiques, POWER_QUERY_CODE).
             - 6 colonnes PQ présentes : sens, filtre_vue_menage, source_module,
               source_table, source_pk, date_integration.
             - 4 requêtes M-code présentes dans POWER_QUERY_CODE.
             - Filtre VUE : [filtre_vue_menage]="OUI" AND [statut_controle]="VALIDE".
             Git status : 5 fichiers modifiés (tous Lot 3), 0 supprimé, 0 Lot 4.
Statut     : VALIDÉ
Commentaire: Lot 3 validé humainement le 2026-06-08. Marquage FAIT autorisé.
             Décisions D044 (statuts) et D045 (REF_Charges_Recurrentes) verrouillées.
             REFACTURATION → sens=CHARGE validé (charge avancée, récupérée sur propriétaire).
```

### CTR-2026-06-007

```
Date       : 2026-06-09
Lot        : Lot 4 — Réservations hors Hostaway
Code       : AUDIT_LOT4_SAISIE_MASTER_RESHH_STRUCTURE
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (lecture V1/V2)
             01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
             02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx
Résultat   : Audit structurel Lot 4 — 0 FAIL.
             V1 REF_Canaux_Reservation : CANAL_003 VRBO / CANAL_004 Direct / CANAL_005 Autre présents, actifs.
               Aucune modification REF_Setup.xlsm nécessaire.
             V2 remplac? : `REF_Taux_Commission` est la seule source officielle des taux dat?s.
               l'ancienne colonne non dat?e de `REF_Proprietaires` et tout VLOOKUP associ? sont d?commissionn?s.
             SAISIE_ReservationsHorsHostaway.xlsx :
               - 4 onglets (SAISIE, REF_LOCALE, CONTROLES_SAISIE, README).
               - 30 colonnes SAISIE conformes au cadrage validé (groupes 1 à 9).
               - les taux sont r?solus en aval depuis `REF_Taux_Commission`; aucun VLOOKUP taux non dat? ne doit alimenter les calculs r?els.
               - 11 DV (10 plan + niveau_anomalie pour cohérence Lot 3).
               - 13 contrôles CONTROLES_SAISIE (CTR-L4-01 à CTR-L4-13).
               - Formules calculées 500 lignes : mois, nuits, taux_commission, taux_commission_source,
                 commission, acompte_facture, impact_resultat_reel, impact_resultat_comptable, ROW_HASH.
               - MFC : rouge = BLOQUANT / orange = A_CONTROLER (sur colonne AB niveau_anomalie).
             MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx :
               - 3 onglets (MASTER 34 cols, VUE_ACTIVE 34 cols, POWER_QUERY_CODE).
               - 4 colonnes PQ : source_module, source_table, source_pk, date_integration.
               - 3 requêtes M-code : SAISIE_HH_Source, MASTER_FACT_MAN_ReservationsHorsHostaway, VUE_ACTIVE_ReservationsHH.
               - Filtre VUE_ACTIVE : [statut_controle]="VALIDE".
             Décisions verrouillées : D046–D051 (QM-L4-01 à QM-L4-06).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, saisie non encore effectuée.
             La saisie des 32 VRBO (ANO-004) et des réservations directes se fait manuellement.
             Lot 4 peut être marqué FAIT après validation humaine du fichier produit.
```

### CTR-2026-06-008

```
Date       : 2026-06-09
Lot        : Lot 4bis — Table commune des réservations
Code       : AUDIT_LOT4BIS_MASTER_CALC_RESERVATIONS_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx
             02_TRAVAIL/lot4bis_master_calc_reservations.py
Résultat   : Contrôle structurel Lot 4bis — 0 FAIL.
             Script Python exécuté avec succès.
             MASTER_CALC_Reservations.xlsx créé :
             - 3 onglets : MASTER (24 cols) / VUE_FLUX (24 cols) / POWER_QUERY_CODE.
             - 24 colonnes : 2 identification + 6 rattachement + 3 séjour + 2 financier
               + 3 impact + 4 statut (incl. niveau_anomalie + code_anomalie) + 4 système PQ.
             - 7 requêtes M-code : HA_Reservations_Source / HA_Payout_Source / HH_Source /
               REF_Mapping_Source / REF_Logements_Source / MASTER_CALC_Reservations /
               VUE_FLUX_Reservations.
             Anti-double-comptage : 7 scénarios (S1–S7) — lignes HA liées à HH exclues
               de la branche HA avant empilement.
             2 contrôles BLOQUANTS : RESERVATION_DOUBLON_HOSTAWAY_HH + RESERVATION_CALC_ID_DUPLIQUE.
             6 contrôles A_CONTROLER : RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH /
               RESERVATION_VRBO_MONTANT_NON_RENSEIGNE (ANO-004) / RESERVATION_PAYOUT_MANQUANT /
               RESERVATION_LOGEMENT_NON_MAPPE / RESERVATION_MAPPING_MULTIPLE /
               RESERVATION_HH_NON_VALIDE.
             VUE_FLUX : filtre VALIDE + impact_resultat_reel=OUI + montant_retenu≠0 + non nul.
             Décisions verrouillées : D052–D057 (QM-L4b-01 à QM-L4b-06).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, données non encore chargées via PQ.
             La saisie HH et le refresh PQ doivent être effectués avant Lot 9.
             Lot 4bis peut être marqué FAIT après validation humaine.
```

### CTR-2026-06-009

```
Date       : 2026-06-09
Lot        : Lot 5 — Acomptes propriétaires
Code       : AUDIT_LOT5_SAISIE_MASTER_ACOMPTES_STRUCTURE
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx
             02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx
             02_TRAVAIL/lot5_master_acomptes_proprietaires.py
Résultat   : Contrôle structurel Lot 5 — 0 FAIL.
             Script Python exécuté avec succès.
             REF_Setup.xlsm : non modifié — TYPE_FLUX_006 (ACOMPTE_FACTURE_PROPRIETAIRE)
               déjà présent, aucun ajout nécessaire.
             SAISIE_AcomptesProprietaires.xlsx :
               - 4 onglets : SAISIE (18 cols) / REF_LOCALE / CONTROLES_SAISIE / README.
               - 18 colonnes SAISIE conformes : 2 identification + 6 rattachement + 2 financier
                 + 1 mode + 3 impact + 4 statut.
               - 5 DV : source_acompte (liste fermée) / statut_controle (liste fermée) /
                 proprietaire_id (REF_LOCALE) / logement_id (REF_LOCALE) / mode_paiement_id (REF_LOCALE).
               - REF_LOCALE : 12 propriétaires actifs / 16 logements actifs avec proprietaire_id /
                 5 modes de paiement actifs.
               - 10 contrôles CONTROLES_SAISIE : 5 BLOQUANT + 5 A_CONTROLER.
             MASTER_FACT_MAN_AcomptesProprietaires.xlsx :
               - 3 onglets : MASTER (22 cols) / VUE_ACTIVE (22 cols) / POWER_QUERY_CODE.
               - 22 colonnes MASTER : 18 SAISIE + 4 PQ (source_module / source_table / source_pk / date_integration).
               - 5 requêtes M-code : Q1_SAISIE_ACC_Source / Q2_HH_Acomptes_Ref /
                 Q3_REF_Proprietaires_Source / Q4_MASTER_FACT_MAN_AcomptesProprietaires /
                 Q5_VUE_ACTIVE_AcomptesProprietaires.
               - source_table = SAISIE_AcomptesProprietaires (toujours) — D064.
               - source_pk = acompte_id (toujours) — D064.
               - source_hh_id = reservation_hh_id si HH_RESERVATION / null sinon — D064.
               - Filtre VUE_ACTIVE : statut_controle = VALIDE.
             Décisions verrouillées : D058–D064 (QM-L5-01 à QM-L5-07).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, saisie non encore effectuée.
             Aucun backup REF_Setup.xlsm nécessaire (aucune modification).
             report_mois_suivant supprimé (D061) — report_mois_precedent conservé informatif.
             Lot 5 peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-010

```
Date       : 2026-06-09
Lot        : Lot 6a — Hostaway CleaningTasks comptage ménages
Code       : AUDIT_LOT6A_CLEANING_TASKS_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx
             02_TRAVAIL/lot6a_cleaning_tasks_comptage.py
Résultat   : Contrôle structurel Lot 6a — 0 FAIL.
             Extraction segmentée par listingMapId (D065 — méthode fiable).
             Anti-plafond : 17 requêtes, 0 segment >= 500, exhaustivité prouvée.
             REF_Setup.xlsm : non modifié.

             Extraction :
               - Requêtes API      : 17 (1 par listing REF_Logements actifs + inactifs)
               - Tâches brutes     : 500
               - Tâches uniques    : 500 (après déduplication par task_id)
               - Doublons supprimés: 0
               - Segments plafonnés: aucun (max=51 pour listings 480139/480140)

             data (11 cols, 500 tâches brutes) :
               - H6 : cost=NULL sur 500/500 tâches dans onglets.
                 Note : 22/500 tâches avaient un coût dans l'API (50–70 EUR) — forcé NULL.
               - Dates : 2026-02 → 2027-02. Jan 2026 absent (D066 : normal).
               - 14 listings actifs, 3 listings avec 0 tâche (482324, 556954, 515523).
               - 1 tâche sans reservation_id, 0 sans listingMapId, 0 sans scheduled_date.
               - autoTaskId distincts : 15.

             MASTER_ENRICHI (21 cols, 500 lignes) :
               - BLOQUANT=0, A_CONTROLER=39, OK=461.
               - A_CONTROLER : 22 TASK_LOGEMENT_INACTIF (LOG_0003/485104, D068)
                 + 16 TASK_STATUT_PENDING (D067) + 1 TASK_SANS_RESERVATION.
               - Statuts : réalisé=325, prévu=74, A_CONTROLER=16, annulé=85.
               - type_ligne_menage_id=TLM_001 par défaut (D069).

             VUE_COMPTAGE (11 cols, 95 lignes mois×logement) :
               - BLOQUANT=0, 325 ménages réalisés (tous mois).
               - Mois couverts : 2026-02 → 2026-06 (complets), 2026-07+ (planifiés).

             POWER_QUERY_CODE : code M de référence (Q1 à Q4).
             Décisions verrouillées : D065–D069.
Statut     : VALIDÉ
Commentaire: Extraction segmentée remplace le single call défaillant (offset ignoré).
             Lot 6a remplace l'extraction --only-cleaning-tasks du Lot 1 (était vide).
             LOG_0003 (485104, actif=NON) : 22 tâches historiques mappées, A_CONTROLER.
             Lot 6a peut être marqué FAIT après validation humaine.
```

---

### CTR-2026-06-011

```
Date       : 2026-06-09
Lot        : Lot 6b — M04 Ménages internes main-d'œuvre
Code       : AUDIT_LOT6B_M04_MENAGES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx
             02_TRAVAIL/lot6b_m04_menages_internes.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (modification TYPE_FLUX_013)
Résultat   : Contrôle structurel Lot 6b — 0 FAIL.
             Script Python exécuté le 2026-06-09 (tag 20260609_152826).

             REF_Setup.xlsm :
               - Backup créé : 99_ARCHIVES/LOT6B_Menages/REF_Setup_BACKUP_20260609_152826.xlsm
               - TYPE_FLUX_013 (COUT_MO_INTERNE_MENAGE) ajouté dans REF_Types_Flux.
               - Colonnes : HC / NON / NON / NON / OUI confirmés.

             M04_MENAGES_PowerQuery.xlsx :
               - 8 onglets : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 /
                 MASTER / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE / README
               - MASTER : 34 colonnes (2 IDENT + 8 RATT + 2 INTERV + 10 CALCUL + 5 FLUX + 3 STATUT + 4 SYSTEME)
               - PARAM_TAUX_INTERVENANTS : 5 intervenants (INT_0001-005), taux 10€/h INTERNE.
               - PARAMETRES_M04 : SEUIL_ECART_STANDARD_MENAGE = 10, actif=OUI.
               - POWER_QUERY_CODE : 10 requêtes M (Q1..Q9 + Q6B).
               - SOURCE_RAW : squelette vide, colonnes attendues documentées (D070).
               - Chemins PQ placeholder C:\CHEMIN_A_ADAPTER\ — à adapter localement.

             Contrôles documentés :
               BLOQUANTS : MENAGE_SANS_LOGEMENT_ID / M04_SCHEMA_SOURCE_INVALIDE /
                           MENAGE_INTERNE_CODE_IMPACT_NON_HC
               A_CONTROLER : MENAGE_EXTERNE_DANS_M04 / TYPE_INTERVENANT_ABSENT /
                             TAUX_ABSENT_INTERVENANT_INTERNE / TAUX_MULTIPLE_INTERVENANT /
                             MENAGE_RANGEMENT_A_CONTROLER / MENAGE_DOUBLON_POTENTIEL /
                             MENAGE_ECART_NEGATIF_IMPORTANT / MENAGE_ECART_HOSTAWAY_M04

             Décisions verrouillées : D070–D078 (QM-L6b-01 à QM-L6b-05 + corrections).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — SOURCE_RAW vide, saisie GSheet non encore effectuée.
             Adaptation chemins PQ requise avant premier run Power Query.
             REC_002 cle_repartition mise à jour : COUT_STANDARD_MENAGES_MOIS (ancienne valeur : NOMBRE_MENAGES).
             Validation humaine Lot 6b — aucune action différée sur REC_002.
             Lot 6b peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-012

```
Date       : 2026-06-09
Lot        : Lot 6c — Ménages externes (factures PDF prestataires)
Code       : AUDIT_LOT6C_MENAGES_EXTERNES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx
             02_TRAVAIL/lot6c_menages_externes.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (modifications D086)
             01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/Facture mai Aissata.pdf
             01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/Facture mai Mounir.pdf
Résultat   : Contrôle structurel Lot 6c — 0 BLOQUANT, 9 A_CONTROLER, 4 VALIDE.
             Script Python exécuté le 2026-06-09 (tag 20260609_170606).

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm
               - REF_Intervenants : +3 colonnes (nom_legal, siret_rcs, email_facturation).
                 INT_0003 Mounir → MH Entreprise / RCS 792015919.
                 INT_0004 Aissata → Kandia DIABATE / SIRET 10147251200017.
               - REF_Types_Flux : TYPE_FLUX_014 (COUT_REEL_MENAGE_EXTERNE) ajouté.

             MASTER_FACT_MEN_MenagesExternes.xlsx :
               - 7 onglets : SOURCE_RAW / PARAMETRES / MASTER / VUE_ACTIVE /
                 VUE_ECART_HOSTAWAY / POWER_QUERY_CODE / README
               - SOURCE_RAW : 13 lignes (9 Aissata + 4 Mounir — ligne 8 splittée en 8a/8b)
               - MASTER : 13 lignes — 49 colonnes (11 blocs)
               - VUE_ACTIVE : 4 lignes VALIDE (lignes 1, 7, 8a, 8b Aissata — dates précises)
               - VUE_ECART_HOSTAWAY : 16 logements mois=2026-05

             Réconciliation factures :
               - FAC-2026-05-AISSATA-001 : somme=1 439,00€ = total — écart=0,00€ VALIDE
               - FAC-2026-05-MOUNIR-001  : somme=942,00€   = total — écart=0,00€ VALIDE

             Anomalies A_CONTROLER (9 lignes) :
               - 5 lignes Aissata : MENAGE_EXTERNE_DATE_ABSENTE (D087 Option B)
               - 3 lignes Mounir  : MENAGE_EXTERNE_DATE_ABSENTE
               - 1 ligne Mounir T2-65 Gabriel : MENAGE_EXTERNE_MONTANT_NUL +
                 MENAGE_EXTERNE_LOGEMENT_INACTIF (0 ménage, logement inactif 2026-04-26)

             Observations factures :
               - Studio Puits vert : présent chez Aissata (10 pass.) ET Mounir (1 pass.) — non doublon.
               - T3 20 rue Amiral Galache (LOG_0016) : facturé T3 par Aissata, REF = T2. Noté commentaire.
               - Prix Mounir (32/36/52/65€) différents des coûts standards REF — normal.
               - Prix Aissata (29/39/55€) = coûts standards REF — coïncidence, non bloquant.

             Décisions verrouillées : D079–D088 (D-6c-01 à D-6c-10).
             POWER_QUERY_CODE : 7 requêtes M (Q1–Q7).
Statut     : VALIDÉ
Commentaire: Contrôle structurel et données mai 2026 peuplées. 9 lignes A_CONTROLER en attente
             de dates précises de ménage auprès des prestataires (D087 Option B).
             REF_Mapping_Logements à compléter avec les noms factures prestataires pour futurs runs PQ.
             TYPE_FLUX_014 intégrera MASTER_CALC_Flux au Lot 9.
             Lot 6c peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-016

```
Date       : 2026-06-11
Lot        : Lot 4bis — Table commune réservations (correctif peuplement)
Code       : CORRECTIF_LOT4BIS_PEUPLEMENT_MASTER_CALC_RESERVATIONS
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot4bis_charger_reservations.py (créé)
             02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx (modifié)
             .gitignore (modifié — ajout 99_ARCHIVES/LOT4BIS_TableCommune/)
Résultat   : MASTER_CALC_Reservations.xlsx peuplé par script Python reproductible.
             Squelette Power Query (non exécutable) remplacé par approche script Python.
             Backup créé : 99_ARCHIVES/LOT4BIS_TableCommune/MASTER_CALC_Reservations_BACKUP_20260611_200317.xlsx

             Volumes produits :
               MASTER   : 1 391 lignes (24 colonnes)
               VUE_FLUX : 1 321 lignes (VALIDE + impact_resultat_reel=OUI + montant≠0)

             Répartition par source :
               HOSTAWAY_AIRBNB            : 1 235 (S1)
               HOSTAWAY_BOOKING           :    86 (S2)
               HOSTAWAY_DIRECT_HH         :    27 (cas non couvert D054 — validé 2026-06-11)
               HOSTAWAY_VRBO_A_CONTROLER  :    32 (S5)
               OWNERSTAY_EXCLU            :    11 (S7)
               HH (S3/S4/S6)             :     0 (SAISIE_ReservationsHorsHostaway vide)

             Répartition par statut_controle :
               VALIDE         : 1 321
               A_CONTROLER    :    59
               EXCLU_RESULTAT :    11

             Répartition par code_impact :
               IC : 1 321
               HC :    59
               HR :    11

             Anomalies A_CONTROLER :
               DIRECT_SANS_SAISIE_HH      : 27 — DIRECT Hostaway sans saisie HH, à saisir
               VRBO_MONTANT_NON_RENSEIGNE : 32 — VRBO sans montant, en attente saisie HH
               LOGEMENT_INACTIF           :  0 ? r?gle date_sortie_gestion appliqu?e (trace historique ; d?commissionn?e le 2026-06-29)

             R?gle date_sortie_gestion valid?e (2026-06-11) ? trace historique, d?commissionn?e le 2026-06-29 au profit de REF_Gestion_Logements_Hist :
               - date_arrivee < date_sortie ET date_depart <= date_sortie → VALIDE
               - date_arrivee < date_sortie ET date_depart > date_sortie  → A_CONTROLER (SEJOUR_CHEVAUCHE_SORTIE_GESTION)
               - date_arrivee >= date_sortie                              → A_CONTROLER (LOGEMENT_INACTIF)
               - date_sortie absente ET actif=NON                         → A_CONTROLER (LOGEMENT_INACTIF)

             LOG_0003 (T2 - 65 Gabriel, sorti gestion 2026-04-26) :
               76 réservations historiques — toutes VALIDE (arrivées avant date_sortie)
               0 SEJOUR_CHEVAUCHE_SORTIE_GESTION (tous départs avant 2026-04-26)

             Contrôles BLOQUANTS : 0 détecté
             Données personnelles voyageur dans MASTER/VUE_FLUX : AUCUNE (vérifié)
             Fichiers sources : NON modifiés
             HH : 0 ligne (SAISIE_ReservationsHorsHostaway vide — saisie manuelle à venir)
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Script reproductible. Relancer lot4bis_charger_reservations.py après toute
             modification de SAISIE_ReservationsHorsHostaway pour inclure les HH.
             Lot 9 reste BLOQUÉ jusqu'à validation et commit du correctif.
```

---

### CTR-2026-06-015

```
Date       : 2026-06-09
Lot        : Lot 7 — IK & Avantages associés
Code       : AUDIT_LOT7_IK_AVANTAGES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx
             02_TRAVAIL/lot7_ik_avantages.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Construction structurelle Lot 7 — structure vide, 0 donnée fictive.

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT7_IK_Avantages/REF_Setup_BACKUP_20260609_192200.xlsm
               - REF_Types_Flux : TYPE_FLUX_015 (INDEMNITE_KILOMETRIQUE) ajouté.
                 IC — avantage_brut=OUI — justificatif obligatoire.

             MASTER_FACT_MAN_IK_Avantages.xlsx :
               - 6 onglets : SOURCE_SAISIE / PARAMETRES / MASTER_SAISIE /
                 MASTER_CALC_AVANTAGES / POWER_QUERY_CODE / README
               - MASTER_SAISIE      : 22 colonnes — structure vide
               - MASTER_CALC_AVANTAGES : 15 colonnes — structure vide — multi-mois
               - SOURCE_SAISIE      : 9 colonnes saisie brute + ligne instructions
               - PARAMETRES         : REF_Associes (2 associés) + REF_Types_Flux
                 (7 types pertinents) + REF_Modes_Paiement (5 modes) +
                 valeurs fermées statuts / niveau_anomalie / type_remboursement
               - POWER_QUERY_CODE   : 5 requêtes M (Q1-Q5)

             Décisions verrouillées : D089–D095 (D-7-01 à D-7-07).
             Pré-correction : D096 (LOG_0016 T2→T3, CTR-2026-06-014).

             Contrôles implémentés dans Q3 :
               BLOQUANTS (6) : AVANTAGE_ASSOCIE_SANS_ASSOCIE_ID /
                 AVANTAGE_ASSOCIE_MONTANT_INVALIDE / AVANTAGE_SANS_TYPE_FLUX /
                 MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES /
                 DOUBLE_COMPTAGE_SAISIE_ET_DERIVE /
                 SAISIE_LOT7_SOURCE_DEJA_EXISTANTE
               A_CONTROLER (6) : REMBOURSEMENT_SANS_LIEN_ORIGINE /
                 REMBOURSEMENT_SENS_ABSENT / VIREMENT_ASSOCIE_SANS_LIEN_BANQUE /
                 IK_SANS_JUSTIFICATIF / AVANTAGE_NET_NEGATIF /
                 TYPE_FLUX_IMPACT_INCOHERENT

             Sources dérivées (à 0 jusqu au peuplement) :
               - avantage_brut_depenses_perso      : Lot 3 TYPE_FLUX_002
               - avantage_brut_montant_recupere_hh : Lot 4 via reservation_hh_id
               - charges_payees_pour_societe        : Lot 3 TYPE_FLUX_004+008

             Points résiduels non bloquants :
               - Q4/Q5 : jointures Lot 3 et Lot 4 codées mais
                 sources à 0 — à activer quand Lot 3 et Lot 4 peuplés.
               - Virements associés mai 2026 : à saisir manuellement
                 quand données disponibles (banque non traitée).

             Lot 7 structure validée. Données à peuplier par saisie.
Statut     : VALIDÉ
Commentaire: Aucune donnée fictive. Toutes les colonnes et contrôles sont en place.
             TYPE_FLUX_015 actif dans REF_Setup.xlsm.
             Lot 8 (banque) alimentera les virements associés sans ressaisie.
```

---

### CTR-2026-06-014

```
Date       : 2026-06-09
Lot        : Correction référentiel — pré-Lot 7
Code       : CORRECTION_REF_LOG0016_TYPE_LOGEMENT
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Correction type logement LOG_0016 (Cyprien / Clarisse).

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_REFCORR_LOG0016_20260609_190848.xlsm
               - REF_Logements LOG_0016 :
                   type_logement_id  : TYPE_002 → TYPE_003
                   nom_logement_officiel : T2 - Cyprien (Clarisse) → T3 - Cyprien (Clarisse)
                   nom_court         : T2 - Cyprien → T3 - Cyprien
               - REF_Mapping_Logements :
                   MAP_LOG_0075 (Facture ménage externe) : T2 - Cyprien (Clarisse) → T3 - Cyprien (Clarisse)
                   MAP_LOG_0076 (Nom court interne)      : T2 - Cyprien → T3 - Cyprien
                   MAP_LOG_0074 (Hostaway internalName)  : non modifié (donnée source externe)
                   MAP_LOG_0083 (Hostaway listing public): non modifié (pas de mention T2/T3)

             Impact lots commités :
               - Lot 6a (CleaningTasks) : LOG_0016 absent du MASTER_ENRICHI — aucun recalcul.
               - Lot 6b (M04)           : LOG_0016 absent de M04 — aucun recalcul.
               - Lot 6c (ménages ext.)  : LOG_0016 présent (MENEXT-2026-05-AISSATA-008/009).
                   type_logement_id absent du MASTER Lot 6c — structure non impactée.
                   montant_ligne_ttc = 55€ (tarif T3) déjà correct — aucun recalcul financier.
               - Lot 7                  : construction non démarrée — référentiel propre.

             Coût standard applicable post-correction :
               COUT_MEN_003 = 55€ (TYPE_003 T3) — au lieu de COUT_MEN_002 = 39€ (TYPE_002 T2).

             Décision verrouillée : D096.
Statut     : VALIDÉ
Commentaire: Correction purement référentielle. Aucun fichier MASTER à recalculer.
             L'internalName Hostaway "T2 - Cyprien (Clarisse)" reste à corriger côté Hostaway si nécessaire.
```

---

## Référence des codes de contrôle (source : ARCHITECTURE_DONNEES.md §18)

> Convention : codes tirés de l'architecture. Ne pas inventer de nouveaux codes sans les ajouter ici ET dans l'architecture.

### BLOQUANTS (calcul exclu, clôture / facturation impossible)

| Code | Module | Déclencheur |
|---|---|---|
| `BOOKING_PAYOUT_INCOMPLET` | Hostaway | Réservation Booking active sans payout calculable |
| `ACOMPTE_NON_RATTACHE_FACTURE` | Acomptes | Acompte sans facture_ref renseignée |
| `MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES` | HH / Avantages | montant_recupere HH non reflété dans les avantages associés |
| `CHARGE_LOGEMENT_SANS_LOGEMENT_ID` | Charges | Charge avec affectation LOGEMENT mais logement_id absent |
| `CHARGE_PERSO_SANS_ASSOCIE` | Charges | Charge perso/liquide sans associe_id |
| `RESERVATION_HH_SANS_PROPRIETAIRE` | HH | Réservation hors Hostaway sans proprietaire_id |
| `ACOMPTE_HH_INCOHERENT` | HH | Acompte ≠ Total − Ménage − Commission − Reversé |
| `MENAGE_SANS_LOGEMENT_ID` | M04 / Ménages | Appartement ménage non rattaché à un logement du référentiel |
| `MENAGE_INTERNE_CODE_IMPACT_NON_HC` | M04 | Ligne M04 avec code_impact ≠ HC |
| `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY` | Banque | Tentative double comptage banque ↔ Hostaway |
| `BANQUE_DATE_INEXPLOITABLE` | Banque | Ligne bancaire sans aucune date exploitable (Date ET Valeur absentes ou non parsables) |
| `BANQUE_DEBIT_CREDIT_VIDES` | Banque | Débit et Crédit simultanément vides |
| `BANQUE_DEBIT_CREDIT_DOUBLES` | Banque | Débit et Crédit simultanément renseignés |
| `BANQUE_MONTANT_NON_NUMERIQUE` | Banque | Montant non convertible en nombre |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Au moins une ligne non classée ouverte sur le mois |
| `M04_SCHEMA_SOURCE_INVALIDE` | M04 | Colonne obligatoire absente dans Google Sheet ou requête PQ échoue |
| `RESERVATION_DOUBLON_HOSTAWAY_HH` | Table commune | reservation_id_hostaway rattaché à 2+ lignes sans lien explicite |
| `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL` | Obsolète | **Remplacé par `ACHATS_DEJA_EN_SAISIE_CHARGES`** |
| `ACHATS_DEJA_EN_SAISIE_CHARGES` | M04 / Charges | Charge présente dans `SAISIE_Charges_Flux` aussi injectée depuis M04 |
| `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION` | Exploitation | `acompte_conciergerie_recu_via_airbnb` comptabilisé dans le revenu net — bloquant |
| `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION` | Exploitation | Achat ou charge exceptionnelle inclus dans `revenu_net_exploitation` — bloquant |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | Exploitation | Charge non récurrente dans `charge_fixe_mensuelle` — bloquant |
| `PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT` | Exploitation | Paiement déduit du `total_payout` au lieu du `reste_a_payer` — bloquant |
| `CONFUSION_PAYOUT_SOLDE_FACTURE` | Exploitation | Confusion entre `total_payout`, `montant_du_conciergerie`, `reste_a_payer` — bloquant |
| `PK_MANQUANTE_OU_DOUBLONNEE` | Transverse | PK absente ou en doublon dans une table master |

### À CONTRÔLER (intégrés au calcul provisoire, non bloquants)

| Code | Module | Déclencheur |
|---|---|---|
| `LISTING_ORPHELIN_A_CONTROLER` | Hostaway | listingMapId dans export, absent de REF_Logements |
| `REFERENTIEL_ORPHELIN` | REF | Logement sur_hostaway=OUI absent de l'export Hostaway |
| `VRBO_MONTANT_NON_RENSEIGNE` | VRBO | Réservation VRBO Unknown sans saisie manuelle |
| `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` | Table commune | direct Hostaway avec totalPrice > 0, pas de ligne HH |
| `CANCELLED_AVEC_MONTANT` | Hostaway | Réservation annulée avec montant → règle D030 s'applique automatiquement (statut ANNULE_AVEC_PAYOUT) |
| `BANQUE_FICHIER_PERIODE_INCOHERENTE` | Banque | Dates réelles du fichier hors période nominale du nom |
| `BANQUE_LIGNE_SANS_DATE` | Banque | Colonne Date vide mais Valeur présente (non bloquant si Valeur exploitable) |
| `BANQUE_DEVISE_NON_EUR` | Banque | Devise ≠ EUR |
| `DOUBLON_BANCAIRE_POTENTIEL` | Banque | Empreinte bancaire déjà connue |
| `LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Aucune règle ni classification fiable |
| `IA_CONFIANCE_INSUFFISANTE` | Banque | Score de confiance IA sous le seuil |
| `MENAGE_SANS_COUT_STANDARD` | Ménages | Type ou coût standard absent du référentiel |
| `ECART_ARRONDI_LIGNE_SUPERIEUR_TOLERANCE` | Transverse | Écart d'arrondi > 0,10 € sur une ligne (D035) |
| `ECART_ARRONDI_FACTURE_SUPERIEUR_TOLERANCE` | Transverse | Écart d'arrondi cumulé > 1,00 € sur une facture / propriétaire / mois (D035) |
| `MENAGE_ECART_NEGATIF_IMPORTANT` | Ménages | Écart coût réel vs standard > seuil |
| `MENAGE_DOUBLON_POTENTIEL` | Ménages | Même mois × logement × intervenant en double |
| `MENAGE_STATUT_NON_VALIDE` | M04 | Ligne ménage non validée, exclue du calcul |
| `TYPE_INTERVENANT_ABSENT` | Ménages | Intervenant sans type (prioritaire sur identité exacte) |

---

## Note sur BANQUE_LIGNE_SANS_DATE vs BANQUE_DATE_INEXPLOITABLE
- `BANQUE_LIGNE_SANS_DATE` : colonne Date vide mais colonne Valeur présente et parsable → **A_CONTROLER** (non bloquant)
- `BANQUE_DATE_INEXPLOITABLE` : Date ET Valeur absentes ou toutes deux non parsables → **BLOQUANT** (ligne inutilisable)

---

### CTR-2026-06-029

```
Date       : 2026-06-29
Lot        : Lot transverse - HORS_PARC_TECHNIQUE
Code       : STATUT_PARC_REFERENTIEL_ET_EXCLUSIONS
Severite   : INFO
Fichier    : REF_Setup.xlsm / lib_parc.py / lots consommateurs
Resultat   : REF_Logements.statut_parc present, renseigne sur toutes les lignes, limite aux valeurs GERE et HORS_PARC_TECHNIQUE.
             Validation de donnees presente sur la colonne statut_parc.
             HORS_PARC_TECHNIQUE exclu explicitement des calculs economiques et operationnels.
             statut_parc vide, invalide ou inconnu route en A_CONTROLER avec code STATUT_PARC_INVALIDE, sans calcul economique.
Statut     : OUVERT - EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: actif = disponibilite technique du code referentiel ; statut_parc = eligibilite au parc gere.
```
### CTR-2026-07-03-APP2B-REGLES

```
Date       : 2026-07-03
Lot        : APP-2b / Lot4A
Code       : APP2B_REGLES_PAIEMENT_ACOMPTES_DEROGATIONS
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, 02_TRAVAIL/Lot4A, tests Lot4A
Resultat   : Tests application Miniconda verts (224 passed) ; tests Lot4A cibles Miniconda verts (43 passed, 5 subtests).
             Migration APP-2b testee uniquement sur copie D-APP-05C.
             Ecriture reelle HH maintenue desactivee.
Statut     : OUVERT - EN_ATTENTE_CONTROLES_FINAUX_PRECOMMIT
Commentaire: Ne pas activer l'ecriture reelle avant migration controlee du classeur source et validation aval Lot4A/lot5/lot10/lot12.
```

### CTR-2026-07-03-APP2B-REV2

```
Date       : 2026-07-03
Lot        : APP-2b / Lot4A
Code       : APP2B_REV2_DEROGATIONS_ACOMPTES
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, 02_TRAVAIL/Lot4A, tests Lot4A, cadrage APP-2b
Resultat   : Tests application Miniconda verts (231 passed, 1 warning Starlette/httpx).
             Tests Lot4A cibles py -3 + PYTHONPATH Miniconda verts (48 passed, 9 subtests).
             REV2 remplace le rendu permanent des derogations par une modale locale de confirmation.
             REV2 applique l'acompte proprietaire officiel : banque pro/carte associee/compte perso associe = total_percu,
             especes = total_percu - montant_reverse_proprietaire, direct proprietaire = 0.
             Ecriture reelle HH maintenue desactivee ; aucun fichier Excel reel ne doit etre modifie.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Commit autorise seulement si git diff --check et controle staged restent verts.
```

### CTR-2026-07-04-APP2B-REV3

```
Date       : 2026-07-04
Lot        : APP-2b / Lot4A
Code       : APP2B_REV3_TAUX_OVERRIDE_CANONIQUE
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, 02_TRAVAIL/Lot4A, tests Lot4A, cadrage APP-2b
Resultat   : Tests application Miniconda verts (243 passed, 1 warning Starlette/httpx).
             Tests Lot4A cibles py -3 + PYTHONPATH Miniconda verts (53 passed, 21 subtests).
             REV3 separe le pourcentage utilisateur `taux_commission_override_pct` du taux metier canonique
             `taux_commission_override` stocke en decimal [0,1].
             Lot4A bloque les overrides confirmes hors [0,1] et ne divise plus implicitement par 100.
             Ecriture reelle HH maintenue desactivee ; aucun fichier Excel reel ne doit etre modifie.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Commit autorise seulement si git diff --check, hashes Excel et controle staged restent verts.
```

### CTR-2026-07-04-APP2B-MODALE-FIX

```
Date       : 2026-07-04
Lot        : APP-2b
Code       : APP2B_FIX_MODALE_DEROGATIONS_PAR_DEFAUT
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, cadrage etat/journal
Resultat   : Tests application Miniconda verts (250 passed, 1 warning Starlette/httpx).
             Tests Lot4A cibles py -3 + PYTHONPATH Miniconda verts (53 passed, 21 subtests).
             Correctif d'implementation REV2/REV3 : valeur vide non assimilee a 0,
             derogation uniquement si panneau ouvert + valeur non vide + difference reelle.
             Modale opaque au premier plan, scroll interne, sections taux/menage strictement conditionnelles.
             MOIS_HORS_REFERENTIEL_CLOTURE reste bloquant avec message utilisateur explicite.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Aucun changement metier acompte/taux/modes/cloture ; commit autorise seulement si controles Git et staged restent propres.
```
### CTR-2026-07-04-APP2C-DRYRUN

```
Date       : 2026-07-04
Lot        : APP-2c / Lot4A
Code       : APP2C_PREVISUALISATION_COPIES
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, cadrage APP-2c
Resultat   : Tests application Miniconda verts (261 passed, 1 warning Starlette/httpx).
             Tests Lot4A cibles py -3 + PYTHONPATH Miniconda verts (53 passed, 21 subtests).
             APP-2c simule l'enregistrement HH sur copies isolees, produit un manifest hashe,
             execute Lot4A sur copie et affiche un comparatif avant/apres sans ecriture reelle.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Aucun fichier Excel reel ne doit etre modifie ; `HH_REAL_WRITE_ENABLED=False` reste obligatoire.
```

### CTR-2026-07-04-APP2C-MOTEUR-REEL

```
Date       : 2026-07-04
Lot        : APP-2c / Lot4A
Code       : APP2C_EXECUTION_LOT4A_REEL_SUBPROCESS
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, cadrage APP-2c
Resultat   : Tests application Miniconda verts (268 passed, 1 warning Starlette/httpx).
             Tests Lot4A cibles py -3 + PYTHONPATH Miniconda verts (53 passed, 21 subtests).
             APP-2c execute Lot4A reel via Python systeme avec NumPy/pandas,
             sans import Lot4A dans FastAPI Miniconda et sans ecriture Excel reelle.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Correctif technique post APP-2c ; aucune regle metier APP-2b/APP-2c modifiee.
```

### CTR-2026-07-04-APP2D-ECRITURE-REELLE

```
Date       : 2026-07-04
Lot        : APP-2d
Code       : APP2D_ECRITURE_REELLE_CONTROLEE
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, cadrage APP-2d
Resultat   : APP-2d prepare l'activation reelle sans l'executer.
             Double flag obligatoire : HH_REAL_WRITE_ENABLED et HH_REAL_WRITE_CONFIRMATION_ENABLED.
             Ecriture autorisee seulement depuis une simulation APP-2c recente, OK, Lot4A terminee,
             hashes sources inchanges, PK absente, mois ouvert et schema reel deja prepare.
             Confirmation humaine exacte requise : ENREGISTRER RESHH-AAAA-MM-NNN.
             Rollback APP-2d hashé en cas d'erreur apres debut d'ecriture.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Aucun fichier Excel reel ne doit etre modifie ; migration reelle du schema reste une etape separee.
```

### CTR-2026-07-04-APP2E-SCHEMA-RECETTE

```
Date       : 2026-07-04
Lot        : APP-2e
Code       : APP2E_PREPARATION_SCHEMA_RECETTE_COPIES
Severite   : INFO
Fichiers   : 05_APPLICATION/app, 05_APPLICATION/tests, 05_APPLICATION/tools, cadrage APP-2e
Resultat   : APP-2e prepare le diagnostic et la migration future du schema reel sans l'executer.
             La migration de developpement fonctionne uniquement sur copies et produit un manifest.
             La recette sur copies controle APP-2c + APP-2d + writer + Lot4A post-ecriture,
             persiste les champs de derogation et teste le rollback hashe.
             Les flags HH_REAL_WRITE_ENABLED et HH_REAL_WRITE_CONFIRMATION_ENABLED restent False par defaut.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Aucun fichier Excel reel ne doit etre modifie ; la premiere migration reelle devra etre lancee par commande explicite avec confirmation humaine.
```

### CTR-2026-07-05-APP2E-MIGRATION-REELLE

```
Date       : 2026-07-05T00:17:47Z
Lot        : APP-2e migration reelle
Code       : APP2E_MIGRATION_REELLE_SCHEMA_HH
Severite   : CRITIQUE — MIGRATION IRREVERSIBLE EXECUTEE
Fichiers   : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
             01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
Resultat   : real_status = OK. Migration atomique avec backup preablable et validation sur temporaires.
             SAISIE : 7 colonnes ajoutees (AE-AK), colonnes historiques et formules intactes.
             REF_Setup : PAY_006 / DIRECT_PROPRIETAIRE ajoute dans REF_Modes_Paiement.
             VBA preserve : xl/vbaProject.bin SHA-256 = 09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2
             Diagnostic post-migration : migration_needed=false, errors=[], formula_violations=[].
             Flags HH_REAL_WRITE_ENABLED et HH_REAL_WRITE_CONFIRMATION_ENABLED restes a False.
Hashes     : avant  SAISIE=c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c
                    REF=6d9f21de919e80c1903ae5acdb2f64a3d776c858857dda52fb39b8335ab726da
             apres  SAISIE=60b7bc85f7d59530e0a0fcdb9596162012db44611aeefaa3b1f0a97d32b18943
                    REF=3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8
Sauvegarde : 99_ARCHIVES\APP2E_SCHEMA_HH_20260705_020543\ (hors staging)
Statut     : FERME — MIGRATION CONFIRMEE ET VALIDEE
```

### CTR-2026-07-05-APP2E-INTEGRITE-VBA

```
Date       : 2026-07-05
Lot        : APP-2e durcissement
Code       : APP2E_INTEGRITE_BINAIRE_VBA_PACKAGE_ZIP
Severite   : BLOQUANT
Fichiers   : saisie_hh_schema_real_prepare_service.py, test_saisie_hh_app2e.py
Resultat   : Controle binaire VBA implemente et valide sur copies synthetiques.
             xl/vbaProject.bin : present et SHA-256 identique avant/apres migration.
             xl/vbaProjectSignature.bin : present et identique si existait avant.
             [Content_Types].xml : declare classeur macro-enabled apres migration xlsm.
             xl/_rels/workbook.xml.rels : relation vbaProject presente apres migration.
             Parties ZIP sensibles (activeX, ctrlProps, embeddings, externalLinks,
             connections.xml, customUI, docProps/custom.xml, printerSettings) :
             presentes et identiques si existaient avant migration.
             Codes bloquants : VBA_PRESERVATION_ECHEC, PACKAGE_SENSIBLE_PRESERVATION_ECHEC.
             30 tests au total : 6 nouveaux (VBA binaire x5 + formule mutee x1).
             Hashes REF_Setup.xlsm et SAISIE inchanges confirmes.
Statut     : OUVERT - EN_ATTENTE_COMMIT_SELECTIF
Commentaire: Aucun fichier Excel reel modifie. Tests VBA utilisent des ZIPs synthetiques uniquement.
```

---

### CTR-2026-07-05-APP2-MENAGES — Lot APP-2 Ménages — Contrôle module rapprochement

- **Date** : 2026-07-05
- **Session** : APP-2 ménages — rapprochement, 3 flux, outrepassage

**Périmètre contrôlé :**
1. Sources MASTER présentes et lisibles (Lot6d, Lot6e)
2. 3 flux séparés dans la réponse service (HA tasks / M04 internes / externes)
3. Aucune valorisation Hostaway (scan code service + reader)
4. Outrepassage SQLite : motif obligatoire, UNIQUE constraint, audit_events
5. Sources non modifiées après lecture (sha256 stable)
6. Route /menages 200, /menages/{clé} 200, /menages/{clé inconnu} 404
7. POST outrepassage motif vide → 422
8. Migration 0003 : table menage_overrides créée, idempotente

**Résultat :** 16/16 tests ménages verts. Suite application complète : 335/335 verts. Lot4A : 53/53 verts.

**Sources Excel :**
- SAISIE sha256 = `60b7bc85f7d59530e0a0fcdb9596162012db44611aeefaa3b1f0a97d32b18943` INCHANGÉE
- REF_Setup sha256 = `3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8` INCHANGÉE

**Décision :** D-M1 à D-M6 appliquées (voir DECISIONS_METIER D-APP-2-MENAGES).

---

### CTR-2026-07-05-APP3A-FOURNISSEURS

```
Date       : 2026-07-05
Lot        : APP-3a — Fournisseurs / Charges lecture seule
Code       : APP3A_CHARGES_LECTURE_INTEGRITE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx
Résultat   : 19/21 tests verts, 2 skips légitimes (MASTER Power Query sans données, liste vide attendue)
Statut     : CONFORME
Commentaire: MASTER = Power Query, données présentes après refresh Excel + saisie dans SAISIE.
             Placeholder PQ filtré côté reader (charge_id commence par '[').
             D025 (pas IK), D026 (source=MASTER Lot3), D044 (statut tel quel) vérifiés.
             Aucun accès SQLite, aucune écriture Excel détectés (scan code).
```

**Périmètre contrôlé :**
1. Source unique MASTER Lot3 (pas SAISIE, pas MASTER_CALC_Flux)
2. Filtre placeholder PQ actif (charge_id starts with '[')
3. IK et virements associés absents (D025) — scan sur liste
4. statut_controle non recalculé (D044) — aucun calcul dans service/reader
5. Sources non modifiées après lecture (sha256 stable — MASTER et SAISIE)
6. Aucun import sqlite3 / get_db dans service et reader (scan code)
7. Route /fournisseurs 200, /fournisseurs/{inconnu} 404
8. Menu Fournisseurs actif dans sidebar

**Résultat :** 19/21 tests fournisseurs verts. 49/51 tests ciblés verts. Tous ménages et navigation verts.

**Sources Excel :**
- SAISIE_Charges_Flux sha256 = 82d133c3631608261b6b759ecaa48fbf6e50fe5e3bb26cc170ef4b11e0e50e8e INCHANGÉE
- MASTER_FACT_MAN_Charges sha256 = 90513baf64ca6a38b07b835c428ba6e6e3ba8a49c962f41b90... INCHANGÉ
- SAISIE_HH sha256 = 60b7bc85f7d59530e0a0fcdb9596162012db44611aeefaa3b1f0a97d32b18943 INCHANGÉE
- REF_Setup sha256 = 3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8 INCHANGÉ

---

### CTR-2026-07-05-APP3C-PROPRIETAIRES

```
Date       : 2026-07-05
Lot        : APP-3c — Propriétaires & règlements (lecture seule)
Code       : APP3C_LECTURE_PROPRIETAIRES_RELEVES
Sévérité   : INFO
Fichier    : 05_APPLICATION/ (8 fichiers créés, 4 modifiés)
             02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx
             02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : APP-3c implémentée. 401/401 tests verts. 53/53 tests Lot4a verts.
             Routes créées :
               GET /proprietaires                         → liste 12 propriétaires
               GET /proprietaires/{prop_id}               → fiche + mois disponibles
               GET /proprietaires/{prop_id}/{mois}        → relevé (blocs séparés)
               GET /proprietaires/{prop_id}/{mois}/prefacture → 12 lignes préfacture
             Diagnostic sources :
               MASTER_CALC_NetProprietaire.xlsx :
                 REGLEMENT : 270 lignes, clé prop×log×mois 100% unique. Aucun placeholder PQ.
                 VUE_MOIS  : 221 lignes agrégées par prop×mois.
               MASTER_FACT_Proprietaires.xlsx :
                 FACT_FACTURE_LIGNES : 3240 lignes = exactement 12/facture × 270 factures.
                 Blocs : EXPLOITATION (5 lignes), REGLEMENT (7 lignes).
               REF_Proprietaires : 12 propriétaires (PROP_0001–PROP_0012).
             Règles :
               D033/EP1-EP7 : blocs exploitation/règlement séparés structurellement.
               revenu_net_exploitation affiché tel quel, jamais recalculé.
               Lot12 absent → status=UNAVAILABLE, relevé non bloqué.
             Hashes sources (inchangées) :
               REF_Setup.xlsm                    : 3354CE22...C16149E8
               SAISIE_ReservationsHorsHostaway   : 60B7BC85...B18943
               SAISIE_Charges_Flux               : 82D133C3...E50E8E
               MASTER_FACT_MAN_Charges           : 90513BAF...41B90...
               MASTER_CALC_NetProprietaire       : F9E2666B...29B54...
               MASTER_FACT_Proprietaires         : 8CDCA80F...7FE9...
Statut     : INFO — TERMINÉ. Lecture seule. Aucun Excel modifié. Aucune écriture SQLite.
Commentaire: Aucune activation requise. Données disponibles immédiatement.
```

---

```
CTR-2026-07-05-APP3B0-REF-ASSOC-MODE
Date       : 2026-07-05
Lot        : APP-3b-0 — préparation REF_Assoc_Mode
Opération  : Dry-run — préparation REF_Assoc_Mode sur copie de REF_Setup.xlsm
Source     : REF_Setup.xlsm (lecture seule)
Copie      : REF_Setup_assoc_mode_prepare.xlsm (dans DATA_DIR ou tmpdir externe)
Résultats  :
             Suite complète : verts après ajout des tests test_ref_assoc_mode_prep.py
             Lot4A          : verts (inchangé)
             git diff --check : 0 whitespace error
             REF_Setup.xlsm hash inchangé (vérifié par test_execute_source_ref_inchangee_apres_refus_flag)
             VBA vbaProject.bin hash inchangé dans copie (test_preparer_vba_preserve)
             Package sensible préservé (test_preparer_package_sensible_preserve)
             Feuilles originales préservées (test_preparer_feuilles_originales_preservees)
             Idempotence vérifiée (test_preparer_idempotent)
             Feuille incoherente refusée (test_preparer_refuse_feuille_incoherente)
             cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED = False (test_flag_ref_assoc_mode_est_false)
Lignes     : 7 (AM_001..AM_007)
Table Excel: tblRefAssocMode (openpyxl ListObject)
Contrainte : aucun fichier réel modifié, aucun pipeline lot3/9/10/11/12 appelé
Statut     : INFO — TERMINÉ. Préparation copie validée. Migration réelle non exécutée.
Commentaire: cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED reste False. Activation requise avant APP-3b-2.
```

---

```
CTR-2026-07-05-APP3B0-MIGRATION-REELLE
Date       : 2026-07-05
Lot        : APP-3b-0 bis -- migration reelle REF_Assoc_Mode
Operation  : Application migration reelle REF_Assoc_Mode dans REF_Setup.xlsm
Hash avant : 3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8
Hash apres : c5a544e6a73f2815fbbec7ee0b2777c230085086d3f417bf42c4747ce9a78d9a
Hash VBA   : 09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2 (inchange)
Sauvegarde : 99_ARCHIVES/APP3B0_REF_ASSOC_MODE_20260705_182335/REF_Setup.xlsm
Resultats  :
             Diagnostic : status=OK, feuille_presente=true, feuille_coherente=true
             migration_necessaire=false, coherence_violations=[], errors=[]
             REF_Assoc_Mode occurrences : 1 (unique)
             Table tblRefAssocMode : presente
             Headers : 6 colonnes exactes
             Lignes : 7/7 correctes
             Unicite IDs, pairs, abbrevs : OK
             27 feuilles historiques preservees + REF_Assoc_Mode (28 total)
             VBA vbaProject.bin : hash inchange
             Package sensible : preserve
             flag REF_ASSOC_MODE_REAL_WRITE_ENABLED : active temporairement pour migration reelle, remis a False apres controle post-migration
             Suite 423 passed, 2 skipped -- apres fix fixtures post-migration
             Lot4A 53 passed
             git diff --check : 0 whitespace error
             Aucun pipeline Lot3/9/10/11/12 lance
             Aucune charge creee
Statut     : INFO -- TERMINE. Migration reelle validee et versionnee.
Commentaire: flag reste False. Sauvegarde dans 99_ARCHIVES jamais stager ni modifier.
```


## CTR-APP-3b-1-001 — Validation prévisualisation saisie charge

Date       : 2026-07-05
Lot        : APP-3b-1 — Prévisualisation saisie charge
Code       : CTR-APP-3B1-001
Sévérité   : INFO
Fichier    : 05_APPLICATION/tests/test_charges_preview.py
Résultat   : 61 tests passés (0 échec). 16 catégories couvertes :
             flags sécurité, hash source inchangé, validation V01-V15,
             mois clôturé, mode paiement + associé + carte, ASSOC_MODE,
             affectation + logement, réservation CHG_021, génération charge_id,
             token/manifest, copie créée, colonnes formule non écrites, référentiels.
             Suite complète : 484 passés, 0 régression.
Statut     : CORRIGÉ
Commentaire: SAISIE_Charges_Flux.xlsx inchangé confirmé par test (hash avant = hash après).
             Copie créée uniquement sous data/dryruns/. Aucune route de confirmation réelle.

---

Date       : 2026-07-06
Code       : CTR-REFAC-LOT10-12
Sévérité   : BLOQUANT (défaut métier corrigé)
Fichier    : 02_TRAVAIL/lot10_calculer_resultats.py, 02_TRAVAIL/lot12_generer_factures.py,
             02_TRAVAIL/lib_settlements.py
Défaut     : Le terme `charges_exceptionnelles_refacturees` de la formule verrouillée D033
             (montant_du_conciergerie = commission + menage + charge_fixe + charges_exceptionnelles_refacturees)
             était absent de lot10 (settle_invoice appelé avec 4e argument figé 0.0) et la ligne 11
             de préfacture CHARGES_EXCEPT_REFAC était codée en dur à 0.0 dans lot12.
             Conséquence : toute charge refacturable=OUI validée était perdue pour le bloc RÈGLEMENT
             propriétaire (montant dû, reste à payer, ligne 11 préfacture).
Correction : Fonctions pures `eligible_refacturable_charge` et `aggregate_refacturable_charges` ajoutées
             à lib_settlements. lot10.build_net_proprietaire agrège les charges refacturables validées
             par (mois × logement × propriétaire) et les intègre à `montant_du_conciergerie` + expose la
             colonne `charges_exceptionnelles_refacturees`. lot12 lit ce champ pour la ligne 11.
             Éligibilité : refacturable=OUI ET statut_controle=VALIDE ET propriétaire exploitable
             (jamais inféré depuis categorie ou type_flux). Charges refacturables sans logement/propriétaire
             routées en lignes sentinelles A_CONTROLER (jamais perdues silencieusement, jamais affectées
             arbitrairement). Jamais mélangé à revenu_net_exploitation (D034).
Périmètre  : Aucune source Excel réelle modifiée. Aucun MASTER métier régénéré. Correction code + tests
             sur fixtures synthétiques. Charge non refacturable et charge non validée inchangées (0.0).
Statut     : CORRIGÉ (tests non-régression verts)
Commentaire: Prérequis à l'ouverture de la refacturation en écriture réelle (D-CHG-MODELE-09).
             Réparation de la chaîne, indépendante de APP-3b-1 (non modifié).

---

Date       : 2026-07-07
Code       : CTR-TYPEFLUX-MIGRATION-01
Sévérité   : INFO
Fichier    : 05_APPLICATION/tools/migrer_profils_impact.py,
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm,
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx
Objet      : Migration contrôlée — TYPE_FLUX_020 (CHARGE_SOCIETE_COMPTE_PRO) + enrichissement
             lst_TypesFlux_Lot3 (TF016, TF020) + reclassement CHG_012/013/015/019 en PARCOURS_DEDIE.
Backup     : 99_ARCHIVES/TYPESFLUX_PARCOURS_20260707_125338/ (non commité)
Hash REF   : avant 70ce2eb1…b0cff60  →  après be45aa09…749b155
Hash SAISIE: avant 39d2bafe…c78409  →  après c9a58527…f74dcc
Contrôles  : VBA identique (sha 09eb44f9…) ; 16 tables préservées ; formules SAISIE (C/I/J/AD) intactes ;
             20 DV préservées ; TYPE_FLUX_020 unique (20 types) ; TF016+TF020 uniques dans REF_LOCALE ;
             CHG_024 unique ; 0 ligne de charge métier créée ou modifiée ; migration idempotente (2e passe = no-op).
Statut     : APPLIQUÉ (tests schéma verts)
Commentaire: Prérequis à la dérivation type_flux côté service (Commit 3, D-CHG-TYPEFLUX-01).

---

Date       : 2026-07-07
Code       : CTR-CHG-GUIDE-MIGRATION-01
Sévérité   : INFO
Fichier    : 05_APPLICATION/tools/migrer_profils_impact.py,
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm,
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx
Objet      : Nouvelles catégories CHG_025 (Repas), CHG_026 (Prestation diverse), CHG_027 (Supplément ménage) ;
             reclassement CHG_018 MENAGE→GLOBAL ; ajout des 3 catégories à lst_Categories (REF_LOCALE).
Backup     : 99_ARCHIVES/CATEGORIES_NOUVELLES_20260707_180659/ (non commité)
Hash REF   : avant be45aa09…749b155  →  après a0ae8fbd…cc6637a
Hash SAISIE: avant c9a58527…f74dcc  →  après 6fa963c2…4da2df6
Contrôles  : VBA identique (sha 09eb44f9…) ; 16 tables préservées ; formules SAISIE intactes ;
             CHG_025/026/027 uniques ; table catégories A1:K28 ; 0 ligne de charge métier créée/modifiée ;
             migration idempotente (2e passe = no-op).
Statut     : APPLIQUÉ (tests schéma + moteur verts)
Commentaire: Support du formulaire Nouvelle charge guidé (D-CHG-GUIDE-06).

---

Date       : 2026-07-07
Code       : CTR-CHG-GUIDE-MOTEUR-01
Sévérité   : INFO
Fichier    : 05_APPLICATION/app/services/charges_impact_service.py,
             05_APPLICATION/app/services/charges_preview_service.py
Objet      : Moteur d'impacts charges (prévisualisation, aucune écriture réelle).
Contrôles vérifiés par tests :
             - une charge réelle jamais comptée deux fois (ligne SAISIE unique, ventilations en manifest) ;
             - somme des quotes-parts = montant réel (répartition égale centimes déterministe) ;
             - une charge ménage jamais refacturable (V22) ; jamais seconde charge comptable (analytique) ;
             - périmètre : propriétaire élargit aux logements actifs, doublons éliminés (REF_Gestion) ;
             - ménage intervenant XOR logement (jamais les deux) ;
             - avantage associé distinct du moyen de paiement (champ dédié) ;
             - forfait client CHG_016 non saisissable (V04) ; IK hors formulaire (Lot7, D026) ;
             - champs cachés bloqués côté service ; requête navigateur manipulée neutralisée ;
             - réserve : une entrée par quote-part, pas de doublon, statut EN_ATTENTE ;
             - SAISIE_Charges_Flux.xlsx hash inchangé après prévisualisation.
Statut     : APPLIQUÉ (34 tests moteur + intégration verts)
Commentaire: Voir D-CHG-GUIDE-01 à 07. SQLite jamais vérité métier ; flags d'écriture inchangés.

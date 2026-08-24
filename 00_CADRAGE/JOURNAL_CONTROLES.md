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

---

Date       : 2026-07-07
Code       : CTR-CHG-PERSIST-01
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/Charges/SAISIE_Charges_Impacts.xlsx (nouveau),
             05_APPLICATION/tools/creer_saisie_charges_impacts.py,
             05_APPLICATION/app/services/charges_impacts_persist_service.py
Objet      : Source de vérité durable des impacts charges (affectations / ménage / réserve) + avantages Lot7.
Contrôles vérifiés par tests :
             - fichier source créé vide (0 ligne de charge métier), schéma conforme, générateur idempotent ;
             - somme des quotes-parts d'affectations = montant (jamais répliqué) ;
             - somme des quotes-parts de réserve = montant refacturable ;
             - charge ménage : aucune ligne RESERVE (jamais refacturable) ;
             - avantage associé : au plus une ligne par charge (dédup lien_origine=charge_id), source Lot7 ;
             - écriture sur COPIE contrôlée ; fichier source réel intouché (hash inchangé) ;
             - idempotence : réécrire un charge_id remplace ses lignes, jamais de doublon ;
             - persister_reel interdit (PermissionError) tant que CHARGES_REAL_WRITE_ENABLED = False ;
             - une charge économique reste unique (Lot3) ; aucune préfacture ni charge réelle écrite.
Statut     : APPLIQUÉ (12 tests persistance + recettes complètes vertes)
Commentaire: D-CHG-GUIDE-08. Consommateurs futurs : Lot9/Lot10 (affectations), Lot6f (ménage), Lot12 (réserve),
             Lot7 (avantages). Application/report/ignore non automatique.

---

Date       : 2026-07-08
Code       : CTR-CHG-BRANCHEMENT-LOTS-01
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lib_avantages.py, lib_charges_menage.py, lib_charges_affectation.py,
             lib_charges_reserve.py, lib_controles_impacts.py
Objet      : Preuve de LECTURE contrôlée des nouvelles sources d'impacts par chaque Lot (sur copies/fixtures,
             aucune écriture réelle). Table réelle vide → 0 ligne → pipeline réel inchangé (no-op).
Verrou Lot7 (levé) : SOURCE_SAISIE est strictement RÉSIDUELLE (« NE PAS RESSAISIR » les charges Lot3) et la
             transformation Lot7 est Power Query (non Python). Écrire un avantage de charge dans SOURCE_SAISIE
             = double comptage. Correction : l'avantage est PORTÉ PAR LA CHARGE (colonne avantage_associe_id de
             SAISIE_Charges_Flux), agrégé par `lib_avantages` (réplique testable du PQ) — jamais ressaisi en Lot7.
Preuves par Lot (tests) :
             - Lot7  : avantage attribué exactement une fois par bénéficiaire ; 2e exécution sans duplication ;
                       avantage distinct du paiement (banque pro) ; pas de double attribution (flag+TF002).
             - Lot6f : ventilation ménage par COUT_STANDARD_MENAGES_MOIS (poids=nb×cout_standard) ; intervenants ;
                       logements ; tous logements ; aucun ménage éligible → A_CONTROLER (jamais arbitraire) ;
                       intervenant+logement simultané refusé ; somme quotes = montant ; jamais 2e charge / réserve.
             - Lot9/Lot10 : 100 € sur 2 logements = 1 charge économique + 2 impacts totalisant 100 (jamais 200) ;
                       global ; 1/N propriétaires ; directs+propriétaires dédupliqués ; centimes déterministes.
             - Lot12 : propositions réserve EN_ATTENTE par propriétaire×mois ; préfacture inchangée ; aucune
                       application automatique ; pas de double proposition (dédup reserve_id) ; états futurs lus sans casser.
             - Lot11 : contrôles quotes=montant (affectations & réserve), ménage hors réserve, avantage unique par
                       charge, aucune charge analytique recréée comme charge réelle, charge_id valide.
Statut     : APPLIQUÉ (41 tests libs Lots verts + recettes complètes)
Commentaire: Lecture uniquement via charge_id valide. Sources réelles intouchées (tables vides). Flags off.
             Branchement effectif des pipelines (écriture des pools/résultats) déféré à l'ouverture de l'écriture réelle.

---

Date       : 2026-07-08 (reformulé 2026-07-11 — Option A)
Code       : CTR-CHG-AVANTAGE-PQ-01
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot7_generateur_avantages.py (moteur réel, commit d5da30d),
             tests/test_lot7_generateur_avantages.py (preuve principale),
             02_TRAVAIL/lot7_pq_avantages.py + tests/test_pq_avantage_lot7_reel.py (preuve complémentaire optionnelle),
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx (colonne avantage_associe_id)
Objet      : Attribution déterministe des avantages issus des charges — générateur Python Lot7 (Option A).
Constat    : le classeur Lot7 ne contient AUCUN Power Query vivant (pas de connections.xml / DataMashup) ; l'onglet
             POWER_QUERY_CODE est documentaire. Le moteur réel est Python : avantage_brut_depenses_perso est agrégé
             depuis SAISIE_Charges_Flux (Lot3) avec priorité déterministe (avantage_associe_id > TYPE_FLUX_002).
Preuve     : PRINCIPALE — tests/test_lot7_generateur_avantages.py (fixtures, aucune donnée métier réelle) :
             PAY_001 100 € + avantage_associe_id=PERS_EWAN → PERS_EWAN 100 € exactement une fois ; 2e exécution →
             toujours 100 € (jamais 200) ; TYPE_FLUX_002 historique préservé ; PAY_003/004 sans flag → 0.
             COMPLÉMENTAIRE OPTIONNELLE — test_pq_avantage_lot7_reel.py (Excel COM, scratch) : mêmes résultats sur le
             moteur Power Query Excel ; guardé (skip si Excel/win32com indisponible). Non nécessaire au pipeline.
Statut     : PROUVÉ (générateur Python idempotent ; cross-check Excel COM optionnel concordant)
Commentaire: D-CHG-GUIDE-09. Corrige les docs affirmant une écriture SOURCE_SAISIE ou un Power Query réel pour les
             avantages de charge. Aucune écriture dans les données métier réelles. Flags off.

---

Date       : 2026-07-11
Code       : CTR-CHG-SUIVI-ASSOCIE-01
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot7_generateur_avantages.py (suivi associé), 02_TRAVAIL/lib_controles_avantages.py,
             tests/test_lot7b_suivi_associes.py, tests/test_controles_avantages.py
Objet      : Suivi associé HR (Lot7B) — MASTER_CALC_AVANTAGES enrichi + contrôles Lot11.
Constat    : MASTER_CALC_AVANTAGES devient le suivi par associé/mois (code_impact=HR, source_calcul=LOT7,
             sens_suivi, associe_nom). AvantageNet = AvantagesBruts + IK − ChargesPayéesPourSociété (D011).
Preuve     : tests/test_lot7b_suivi_associes.py (Cas A–F + HR strict + idempotence) et
             tests/test_controles_avantages.py (9 contrôles + intégration : la sortie générateur passe tous
             les contrôles, 0 anomalie). Aucune donnée métier réelle (fixtures). 229 tests root verts.
Contrôles  : avantage absent du suivi ; charge_id doublé ; SOURCE_SAISIE lien déjà Lot3 ; code_impact non-HR ;
             colonne d'impact propriétaire/résultat interdite ; clé mois/associé manquante ; IK dans Lot3 ;
             charge payée société non reprise ; avantage_net incohérent.
Statut     : PROUVÉ (suivi HR, aucun impact résultat/propriétaire, aucun règlement)
Commentaire: D-CHG-GUIDE-10. Ce n'est PAS un règlement (aucun virement/trésorerie). Flags off. Fichiers réels intacts.

---

Date       : 2026-07-11
Code       : CTR-CHG-SUIVI-ASSOCIE-02
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot11_controles_coherence.py (fonction controles_suivi_associe),
             02_TRAVAIL/lib_controles_avantages.py, tests/test_lot11_avantages_integration.py
Objet      : Branchement des contrôles du suivi associé dans le pipeline Lot11 (lecture seule).
Constat    : Lot11 exécute désormais les contrôles avantages sur SAISIE_Charges_Flux + SOURCE_SAISIE +
             MASTER_CALC_AVANTAGES. 7 contrôles INTRINSÈQUES ACTIFS (code_impact HR, pas d'impact
             propriétaire, clé mois/associé, avantage_net cohérent, lien SOURCE_SAISIE déjà Lot3, charge_id
             doublé, IK hors Lot3). 2 cross-contrôles (AVANTAGE_ABSENT_DU_SUIVI, CHARGE_PAYEE_NON_REPRISE)
             PRÉPARÉS mais DIFFÉRÉS tant que MASTER_CALC_AVANTAGES n'est pas régénéré depuis la SAISIE
             (flags off) — sinon faux positifs. INFO SUIVI_ASSOCIE_NON_GENERE émise dans cet état.
Preuve     : sur fichiers réels (lecture seule) = 1 seule entrée INFO SUIVI_ASSOCIE_NON_GENERE, 0 anomalie.
             tests/test_lot11_avantages_integration.py : 6 cas déclencheurs + cas calc vide (différé). 236 root verts.
Statut     : BRANCHÉ (7 contrôles actifs, 2 préparés/différés). Aucune écriture ; aucun impact résultat/propriétaire.
Commentaire: Lot7C. Aucun fichier métier réel modifié. Flags off. Cross-contrôles activés quand le suivi sera généré.

---

Date       : 2026-07-13
Code       : CTR-APP3B-ECRITURE-REELLE-01
Sévérité   : INFO
Fichier    : 04_LOGS/APP3B_ECRITURE_REELLE/executer_protocole_copie.py (+ rapport horodaté),
             00_CADRAGE/PROTOCOLE_ECRITURE_REELLE_CHARGES.md, 02_TRAVAIL/lot7_generateur_avantages.py
Objet      : Exécution du protocole d'écriture réelle — UNIQUEMENT SUR COPIES ISOLÉES (hors dépôt).
Constat    : 3 cas écrits sur copies ($TEMP/app3b_ecriture_<TS>/) : A) charge simple non ménage avec
             avantage associé (CHG_025, 100 €, PAY_001, PERS_EWAN) ; B) charge refacturable (CHG_008,
             LOG_0001) ; C) charge ménage (CHG_004, INT_0001). 59/59 contrôles verts.
Preuve     : SHA256 des 3 fichiers réels IDENTIQUES avant/après (SAISIE_Charges_Flux, SAISIE_Charges_Impacts,
             MASTER_FACT_MAN_IK_Avantages) → aucun fichier métier réel modifié. Colonnes formule C/I/J/AD
             non écrasées (formules vivantes). 1 seule ligne charge après DOUBLE écriture (idempotence).
             AFFECTATIONS Σ quote_part = montant ; RESERVE statut EN_ATTENTE ; MENAGE intervenant XOR
             logement, jamais de réserve. Onglets/validations/tables Excel inchangés. Lot7 régénéré :
             avantage PERS_EWAN 2026-06, code_impact=HR, net=100.00, exactement 1 fois, 2e génération
             identique. Lot11 : 0 anomalie, les 2 cross-contrôles s'activent sans faux positif.
Correctif  : lot7_generateur_avantages._mois_valide — le gabarit d'instruction 'AAAA-MM' de SOURCE_SAISIE
             avait la FORME d'un mois (7 car., tiret en 5e) et franchissait le filtre. Rejeté désormais
             (chiffres exigés). Test de non-régression ajouté.
Statut     : MOTEUR PROUVÉ SUR COPIE — ÉCRITURE RÉELLE NON ACTIVÉE (flags restent False).
Limites    : 1) persister_reel() est un stub (NotImplementedError) : aucun writer réel n'existe, activer le
             flag ne suffirait pas ; 2) charge_id non réservé (collision possible en saisie concurrente) ;
             3) verrou classeur ouvert / OneDrive non testé ; 4) formules C/I/J/AD sans valeur en cache tant
             qu'Excel n'a pas rouvert le fichier (Lot7 immunisé, autres lecteurs à vérifier).
Commentaire: Les 4 limites doivent être levées avant toute discussion d'activation (§8 du protocole).
             Aucune copie Excel de test n'est versionnée.

---

Date       : 2026-07-13
Code       : CTR-LOT3-GENERATEUR-01
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot3_generateur_charges.py (créé), 02_TRAVAIL/lot6f_cout_complet_menages.py,
             tests/test_lot3_generateur_charges.py, 00_CADRAGE/ARCHITECTURE_DONNEES.md
Objet      : Lot3 — générateur réel du MASTER charges (Option A) + correctif du cache formules Lot6f.
Constat    : MASTER_FACT_MAN_Charges.xlsx ne contenait AUCUN Power Query vivant (pas de
             connections.xml / DataMashup / queryTables) et aucun script lot3_*.py n'existait. Rien ne
             l'alimentait : son onglet MASTER ne contenait qu'une ligne placeholder. Or Lot9, Lot10,
             Lot11, Lot12 et la page Charges de l'app lisent le MASTER, pas la SAISIE — une charge
             écrite réellement dans SAISIE_Charges_Flux serait restée invisible pour le résultat et le
             net propriétaire. Même situation que Lot7 avant Option A.
Correctif  : lot3_generateur_charges.py — lit SAISIE_Charges_Flux (vérité métier) + REF_Categories_Charges,
             écrit MASTER (37 colonnes, contrat identique au M-code documentaire) + VUE_MENAGE
             (filtre_vue_menage=OUI ET statut_controle=VALIDE, D028). Idempotent. Colonnes formule
             RECALCULÉES en Python, jamais lues depuis le cache Excel : mois (← date_charge),
             impact_resultat_reel / impact_resultat_comptable (← code_impact, D012), ROW_HASH.
             Lignes vides / gabarits / placeholders ignorées. date_charge inexploitable → anomalie
             DATE_CHARGE_INVALIDE (jamais un mois inventé).
             lot6f_cout_complet_menages.py — dérive désormais le mois des charges ménage depuis
             date_charge (helper mois_charge) au lieu de lire la colonne formule C en data_only=True.
             Sans ce correctif, une charge écrite par openpyxl sans réouverture Excel était
             silencieusement exclue des pools ménage, avec le contrôle POOL_VIDE_NON_SAISI affirmant à
             tort « aucune charge ménage saisie ». Nouveaux contrôles : CHARGE_MENAGE_DATE_INVALIDE
             (A_CONTROLER) et POOL_VIDE_HORS_MOIS (INFO, distingué de POOL_VIDE_NON_SAISI).
Preuve     : tests/test_lot3_generateur_charges.py — 13 tests sur fixtures (mois dérivé sans cache,
             gabarit ignoré, IC/HC/HR, charge ménage vue par Lot6f, idempotence, sources intactes,
             contrat 37 colonnes, une charge = une ligne, sens dérivé, date invalide, doublon,
             VUE_MENAGE). Exécution sur sources réelles avec sortie sur COPIE : 0 charge lue
             (SAISIE vide), 27 catégories REF, fichiers réels intacts (SHA256). 251 tests root verts.
Statut     : MAILLON LOT3 RÉTABLI — la SAISIE alimente enfin le MASTER, sans dépendance au cache Excel.
Limites    : le MASTER n'est pas régénéré automatiquement après une saisie ; la régénération reste une
             étape moteur explicite, hors transaction d'écriture app (file_registry interdit à juste
             titre l'écriture app dans 02_TRAVAIL / MASTER_*).
Commentaire: Prérequis au writer réel (APP-3b). Flags inchangés (CHARGES_REAL_WRITE_ENABLED = False).
             Aucun fichier métier réel modifié.

---

Date       : 2026-07-14
Code       : CTR-APP3B-CLOTURE-01
Sévérité   : INFO
Fichier    : 05_APPLICATION/app/services/charges_confirmation_service.py (créé),
             charges_post_write_service.py (créé), runners/charges_post_write_runner.py (créé),
             charges_impacts_persist_service.py (persister_reel branché), charges_preview_service.py
             (manifest scellé + empreinte impacts), routes/fournisseurs.py (POST confirmer,
             GET resultat), templates fournisseurs_previsualisation/resultat.html,
             tests test_charges_confirmation_{e2e,route,audit}.py
Objet      : APP-3b — Nouvelle charge guidée : chaîne complète saisie → confirmation → écriture
             transactionnelle → journal → recalculs aval. Clôture technique.
Constat    : persister_reel n'est plus un stub : il prend un token de prévisualisation et délègue à
             la chaîne sécurisée. Le navigateur ne fournit AUCUNE donnée métier (seul le token
             transite) ; tout est relu du manifest serveur. Le charge_id définitif reste résolu SOUS
             VERROU par l'orchestrateur. Lot3/Lot7/Lot11 sont déclenchés hors transaction, en
             sous-processus (interpréteur moteur) : l'app n'importe jamais le moteur.
Contrôles  : 0. flags (les DEUX) — refus avant tout, tracé ; 1. manifest serveur (corruption ≠ token
             inconnu) ; 2. sceau d'intégrité ; 3. fraîcheur (24 h) ; 4. token déjà écrit ;
             5. copie conforme ; 6. empreintes SHA256 des deux fichiers réels ; 7. REVALIDATION
             MÉTIER sur l'état actuel (mois clôturé, référentiels) ; 8. verrou, unicité charge_id,
             transaction deux fichiers, rollback vérifié, journal.
Défauts    : audit adversarial du 2026-07-14 — 4 défauts trouvés et corrigés :
  corrigés    1) la validation métier n'était PAS rejouée à la confirmation : une prévisualisation
                 faite AVANT une clôture restait confirmable APRÈS (les empreintes ne couvrent pas
                 REF_Setup). Correctif : validate_charge + compute_guidee rejoués sur l'état actuel
                 → E_VALIDATION_PERIMEE ;
              2) les flags étaient vérifiés en DERNIER : flags off + source modifiée renvoyait
                 « refaites la saisie » au lieu de « écriture non activée » (faux guidage).
                 Correctif : garde des flags en premier, refus tracé (REFUSE_FLAGS) ;
              3) un manifest corrompu était annoncé comme « token inconnu » → E_MANIFEST_ILLISIBLE ;
              4) aucune notion de fraîcheur → E_MANIFEST_EXPIRE. Durée de vie d'une
                 prévisualisation VALIDÉE À 24 H (décision humaine 2026-07-14) : au-delà, une
                 nouvelle prévisualisation est obligatoire. Une expiration ne signifie jamais
                 qu'une charge a été écrite (refus AVANT toute écriture) ; l'idempotence reste
                 fondée sur le token journalisé, consulté sous verrou.
Preuve     : 05_APPLICATION/tests — E2E 18 cas sur copies isolées (globale, multi-logements, ménage,
             réserve, avantage, double-clic, token inconnu, manifest altéré, source modifiée, flags
             off, verrou pris, échec Lot3 après écriture, rollback, panne journal) ; routes 12 ;
             audit 18 (revalidation, fraîcheur, manifest illisible, Lot7/Lot11 en échec, runner
             cassé, DEUX VRAIS PROCESSUS sur le même token, deux tokens → jamais le même charge_id,
             E2E HTTP complet formulaire→303→résultat). Suite applicative complète verte.
             Les quatre Excel réels et app.db sont INTACTS (SHA256 vérifiés avant/après).
Statut     : TERMINE_TECHNIQUEMENT_NON_ACTIVE — aucune écriture réelle n'a jamais eu lieu.
Limites    : atomicité inter-fichiers inexistante (pire cas choisi : impacts orphelins plutôt qu'une
             charge sans impacts) ; OneDrive non maîtrisable par un verrou applicatif ; un verrou
             périmé bloque jusqu'à décision humaine (aucune purge automatique — un PID est
             recyclable) ; le sceau du manifest détecte la corruption, pas un attaquant.
Commentaire: Flags inchangés (CHARGES_REAL_WRITE_ENABLED = False,
             CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = False). La première écriture réelle est une
             recette humaine distincte : 00_CADRAGE/APPLICATION_LOCALE/APP3B_RUNBOOK_PREMIERE_ACTIVATION.md.
             Documentation de clôture : APP3B_CHARGES_CLOTURE.md.

================================================================================
CTR-APP2A-MENAGES-01 — Module Ménages : rapprochement en lecture
================================================================================
Date       : 2026-07-14
Objet      : Rendre le rapprochement ménages visible, filtrable et explicable dans
             l'application, sans jamais rejouer une règle du moteur ni écrire une source.
Périmètre  : 05_APPLICATION — readers/menages_reader.py, services/menages_service.py,
             routes/menages.py, 4 gabarits + 1 partial, app.css.
Sources    : Lot6a (tâches Hostaway), Lot6b (internes M04), Lot6c (externes facturés),
             Lot6d (rapprochement), Lot6e (gain/perte), Lot6f (coût complet), Lot11 (cohérence).
             Toutes en lecture seule (openpyxl read_only=True).

Constat    : le flux « MÉNAGES ATTENDUS » n'est produit par AUCUN script du moteur.
principal    D090 énonce la règle (1 réservation validée = 1 ménage attendu au check-out) mais
             aucune colonne ne la matérialise. Lot6d compare trois flux : Hostaway réalisé,
             interne déclaré, externe facturé ; son `ecart` = Hostaway − déclarés.
             DÉCISION : l'application ne déduit PAS l'attendu. Le fabriquer reviendrait à
             créer une règle de matching dans FastAPI. Affiché « Non alimenté par le moteur ».
             À produire côté moteur si le besoin est confirmé (décision métier).

Règles     - les 4 flux restent des champs distincts ; jamais fusionnés ;
tenues     - le coût Hostaway (`cost`) n'est ni lu, ni exposé, ni nommé : un test injecte
             999 € dans la fixture et vérifie qu'il n'apparaît nulle part ;
           - coût interne = heures et taux M04 ; coût externe = facture ;
           - aucun statut inventé : statut_controle et code_controle viennent du moteur ;
           - aucune écriture Excel ; aucun import de module 02_TRAVAIL ;
           - aucune route ne lance le pipeline (ni run_pipeline, ni subprocess).

États      Le reader distingue FICHIER_ABSENT / ONGLET_ABSENT / VIDE / ILLISIBLE et ne les
sources    confond jamais avec « zéro ménage ». Une source manquante affiche
           « Source indisponible », jamais une valeur fabriquée.

Preuve     05_APPLICATION/tests/test_menages_rapprochement.py — 20 scénarios sur fixtures Excel
           isolées : conforme ; Hostaway sans déclaration ; déclaration sans tâche ; interne
           rapproché ; externe rapproché ; interne ET externe simultanés restant distincts ;
           coût Hostaway jamais utilisé ; source interne vide ; source externe vide ; fichier
           absent ; onglet absent ; date externe absente ; logement inconnu ; anomalie
           bloquante ; filtres période / logement / écarts / intervenant / type / statut ;
           pagination ; fiche détail ; aucune source modifiée (SHA256 avant/après).
           Plus 10 tests de route (200, cartes, filtres, à-contrôler, diagnostic désactivé,
           404, aucun chemin absolu exposé) et 5 gardes structurelles.
           test_menages.py (APP-2 d'origine) reste vert, inchangé.

Statut     : APP-2A_RAPPROCHEMENT_MENAGES_LECTURE_EN_ATTENTE_VALIDATION
Limites    : un seul mois rapproché (2026-05 — Lot6d fixe MONTH en dur) ; le rattachement d'une
             tâche Hostaway à un intervenant est fait par le moteur, donc la fiche liste les
             tâches du LOGEMENT et du MOIS, toutes assignations confondues (et le dit) ;
             CONFLIT_TITLE_ASSIGNEE : 15 occurrences à instruire côté moteur ; l'outrepassage
             SQLite préexistant affiche un statut effectif JUSTIFIE — le statut moteur reste
             affiché à côté, mais le principe mérite une décision formelle.
Commentaire: aucune écriture métier, aucun recalcul, aucun fichier réel modifié.
             Documentation : 00_CADRAGE/APPLICATION_LOCALE/APP2_MENAGES_ETAT.md

Défaut      DÉFAUT TROUVÉ ET CORRIGÉ (suite complète, 2026-07-15) : le service ménages appelait
trouvé      get_db() sans argument. Le défaut de get_db(db_path=DB_PATH) étant figé à l'import, la
            vraie app.db était ouverte même sous fixture isolée ; PRAGMA journal_mode=WAL écrit
            dans l'en-tête → le fichier réel bougeait (aucune donnée écrite, intégrité ok). Détecté
            par la garde APP-3b test_charges_confirmation_e2e::test_aucun_fichier_reel_ni_base_reelle
            _touches, uniquement en suite complète. Correctif : lecture de cfg.DB_PATH à chaud
            (get_db(cfg.DB_PATH)). Régression : test_route_ne_touche_jamais_la_base_reelle (SHA256 +
            mtime de la base réelle avant/après 3 pages). Correctif contenu dans le module ménages —
            APP-3b non rouvert.
Recette     823 passed, 2 skipped (suite applicative complète, code figé). Tests ciblés ménages 59.
finale      La contamination Jinja (édition d'un gabarit pendant qu'une suite tourne, templates
            rechargés à chaud) a faussé 3 suites intermédiaires ; mesure refaite sur code figé.

CTR-APP2B-RECALCUL-01 — Recalcul ménages sur copies (mode réel gardé)
Contexte    Bouton « Relancer le rapprochement » rendu réellement fonctionnel en mode COPIES ;
            mode RÉEL bloqué par MENAGES_REAL_RECALC_ENABLED=False.
Vérifs      (1) recette E2E : lot6d+lot6e exécutés sur copies des vraies sources → MASTER réel
            sha256 INCHANGÉ (0759fa8d… avant = après) ; sortie écrite dans le workspace, pas le réel.
            (2) confirmer(REEL) → run BLOQUE, aucune exécution, aucun workspace créé.
            (3) préflight : Excel ouvert (~$) → E_EXCEL_OUVERT ; source absente → E_SOURCE_ABSENTE ;
            verrou déjà pris → E_VERROU. (4) SUCCES seulement si rc==0 ET sorties présentes
            (runner stub : fail → ECHEC, réponse absente → E_REPONSE_INVALIDE). (5) verrou libéré
            en finally (état ABSENT après chaque cas). (6) routes : la vraie app.db jamais touchée
            (client isolé tmp_db). Résultat : 23 tests verts (dont E2E réel).
Attendu     Règle réelle = D100/D099 (attendu réservé aux logements hors Hostaway, anti double-
            comptage), PAS D090. Gap décision↔code : lot6d n'ajoute pas les réservations HH de D099.
            Sous-partie documentée et ARRÊTÉE (aucune exception devinée, aucun Lot6g créé).


## 2026-07-27 - Lot13 : filet anti-sensible et contrat d'export

CONTROLE : filet de confidentialite de lot13 (ABORT si une colonne sensible sort).
ETAT AVANT : BLOQUANT permanent. `PBI_Commissions` whitelistait `preparation_canape_voyageurs`,
refusee par le motif `voyageur` du filet. Defaut statique, donc valable aussi en mode reel ;
jamais vu avant parce que la chaine n'atteignait jamais lot13.
CORRECTION : renommage a la frontiere d'export vers `montant_preparation_canape`. Filet non
affaibli, donnee non supprimee, calcul metier non touche.

PREUVES :
- 11 tests dans `05_APPLICATION/tests/test_lot13_filet_anti_sensible.py`, dont la comparaison
  valeur par valeur entre `MASTER_CALC_Commissions.xlsx` et `PBI_Commissions.csv` ;
- le xfail(strict) qui tenait le defaut est remplace par la preuve de correction, pas supprime ;
- run RUN-64BF7084CBB0 : lot4quater -> lot13, 6/6 SUCCES, 23,3 s, aucun controle de
  confidentialite bloquant ; 11 exports sur 13 + dictionnaire (114 entrees) ;
- l'ancien nom n'apparait dans AUCUN CSV exporte (verifie par grep sur 03_EXPORTS/PowerBI) ;
- prevision : toutes les sorties en "non (creation)" -> aucune sortie anterieure reutilisee ;
- run RUN-048B5CF57C3F : 6/6 SUCCES, tous ecarts 0,00 -> idempotence avec lot13 inclus ;
- indicateurs inchanges (CA 14 060,00 ; commissions 2 430,60 ; net 11 629,40 ;
  resultat reel 13 827,20) ; six conditions de cloture toujours OK.

CONTROLES RESTES ACTIFS : le filet refuse toujours `nom_voyageur`, `voyageur_email`,
`guest_name`, `telephone_voyageur`, `adresse_voyageur` (test dedie). La table de renommage est
verrouillee a une seule entree : toute addition casse la suite et exige une decision explicite.
REFERENCE : 38_LOT13_CONTRAT_EXPORT_POWERBI.md


## 2026-07-27 - Chaine Charges exercee depuis l'application (phase 2)

CONTROLE 1 : "jamais de faux succes" du pilotage.
CONSTAT : la chaine `charges` de /calculs declarait lot3_generateur_charges.py comme script
autonome. Or ce moteur est une BIBLIOTHEQUE (aucun bloc __main__) : lance ainsi il rend EXIT=0 et
ne produit RIEN. Le garde-fou l'a bien rattrape ("Code retour 0 mais sortie absente"), mais la
chaine etait inutilisable.
CORRECTION : Lot.runner pointe sur runners/charges_post_write_runner.py, l'orchestrateur qui
existait deja (lot3 + lot7 + lot11). Aucun second moteur ecrit.
POINT CRITIQUE : ce runner rend TOUJOURS le code 0 et porte l'echec metier dans son JSON. Sans
lecture de ce JSON, une etape en echec passerait pour un succes des qu'une sortie d'un run
precedent traine sur le disque. _verdict_runner() refuse tout statut hors OK / NON_APPLICABLE.
Verifie en conditions reelles : ECHEC "lot3=ECHEC (FileNotFoundError)" avec code retour 0.

CONTROLE 2 : double comptage facture/charge/banque.
CONSTAT : le jeu de recette portait le frais bancaire de 8,90 EUR a la fois en charge manuelle
(CHG_SEED_003) et en flux bancaire injecte par Lot9 depuis NORM_Banque (MVT-SEED-006).
Total charges 567,80 au lieu de 558,90. Le defaut PREEXISTAIT (ancien charges_reel 232,80 =
223,90 + 8,90) et rien ne le detectait.
CORRECTION : charge manuelle retiree du jeu de recette ; nouveau controle
test_pas_de_double_comptage_frais_bancaire signalant tout couple mois+montant present a la fois
en flux bancaire et en charge manuelle.

CONTROLE 3 : le MASTER charges n'est plus seede a la main. Il etait une seconde verite que le
premier passage de Lot3 aurait ecrasee. Les charges vivent desormais dans la SAISIE et Lot3
produit lui-meme le MASTER.

PREUVES CHIFFREES (run RUN-C87382B8DA79, SUCCES, 3,0 s) :
- charges manuelles VALIDE 550,00 + frais bancaire 8,90 = REEL 558,90 ;
- dont hors comptabilite 65,00 -> COMPTABLE 493,90 ;
- invariant REEL = COMPTABLE + HORS_COMPTA verifie : 493,90 + 65,00 = 558,90 ;
- charges non validees (500 + 400 = 900) absentes du flux ;
- net_proprietaire 11 629,40 INCHANGE -> scenario A prouve ;
- somme_a_payer 2 430,60 -> 2 580,60, soit +150,00 exactement -> scenario B prouve ;
- resultat_hors_compta -65,00 -> scenarios C + D prouves ;
- second run RUN-653B6C4AE68F : tous ecarts 0,00 (idempotence) ;
- rollback : 1 fichier restaure, run repasse RESTAURE.

NON CONSTRUIT, SIGNALE : "charge posterieure a une cloture validee" n'est interdit par aucun
mecanisme applicatif. La cloture applicative est un suivi ; la cloture reelle reste portee par
REF_Cloture_Mensuelle.
REFERENCE : 39_CHAINE_CHARGES_EXERCEE.md


## 2026-07-27 - Menages : audit et trou dans la garantie "jamais de faux succes" (phase 3)

CONTROLE : sorties declarees des lots pilotes.
CONSTAT : les lots Menages etaient declares sorties=(). Or sorties_ok = all(...) if sorties else
True : SANS SORTIE DECLAREE, LA GARANTIE NE S'APPLIQUAIT PAS. Un lot6* sortant en code 0 sans rien
produire aurait ete annonce SUCCES. Le meme piege que lot3, sur une autre chaine.
CORRECTION : sorties declarees d'apres menages_chaine_service.SORTIES_CHAINE (seule cartographie
auditee) + test test_chaque_lot_menages_declare_ses_sorties.

CONTROLE : completude de la chaine Menages du pilotage.
CONSTAT : lot6c manquait, alors que lot6d consomme sa sortie. Le test d'ordre comparait la chaine
a une liste recopiee a la main, donc il ne pouvait pas detecter l'oubli.
CORRECTION : lot6c ajoute, dependance lot6d <- (lot6b, lot6c) ; le test compare desormais a
menages_chaine_service.STEPS_CHAINE.

CONTROLE : double verrou d'ecriture.
CONSTAT : MENAGES_REAL_RECALC_ENABLED etait la DERNIERE garde codee en dur (= False), ce qui
faisait mentir la regle "double verrou partout".
CORRECTION : aligne sur RECETTE_MODE and _env_flag(...). Reste False par defaut ; mode reel NON
active.

ETAT REEL DE LA CHAINE : lot6b SUCCES (2,6 s), lot6c SUCCES (1,7 s), lot6d ECHEC.
Blocage 1 (leve) : source Hostaway absente -> IndexError sur le glob de
MASTER_FACT_HA_CleaningTasks_Discovery.xlsx. build_menages_hostaway() la seede (4 taches).
Blocage 2 (NON leve) : lot6d_rapprochement_menages.py:220
TypeError: '<' not supported between instances of 'NoneType' and 'str' - une source agregee porte
un logement_id ou proprietaire_id nul. Rien n'est declare fonctionnel au-dela de lot6c.
REFERENCE : 40_AUDIT_MENAGES.md


## 2026-07-27 (soir) - Menages : chaine verte, trois corrections de diagnostic

CONTROLE : chaine Menages complete via son orchestrateur legitime.
RESULTAT : hostaway_stub, lot6b, lot6c, lot6d, lot6e, lot6f, lot11 -> 7/7 OK, statut SUCCES,
reel_intact=True (sources reelles verifiees inchangees par sha256).

CORRECTION 1 - lot6d n'a jamais eu de defaut. Le TypeError NoneType/str venait d'un M04 pollue
par une execution illegitime (lot6b lance en direct atteint le reseau et rapatrie des donnees
reelles dont le mapping vers le parc fictif echoue -> logement_id nul partout). Ni lot6d ni
build_data_recette n'etaient en cause. Voir ANO-2026-07-27-01.

CORRECTION 2 - pivot du cout interne. D101 (VALIDE 2026-06-18) fixe le pivot a 2026-06 ;
lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01 y est CONFORME. Une consigne demandait de le
deplacer au 1er mai en supposant une derive : le pivot n'a PAS ete modifie, l'avancer
recalculerait mai 2026 (mois portant les donnees reelles) avec l'autre methode. Arbitrage inscrit
au handoff. 9 tests dont les 4 frontieres demandees.

CORRECTION 3 - regle cave 50 EUR. Declaree introuvable au tour precedent : AFFIRMATION FAUSSE,
due a une recherche limitee a lib_menage_costs. La regle existe : REC_002 dans
REF_Charges_Recurrentes, D103 (cle COUT_STANDARD_MENAGES_MOIS, revise D045), ventilee UNIQUEMENT
sur les menages internes, implementee dans lot6f. Ce n'est pas "+50 EUR par menage" mais un
forfait mensuel ventile au prorata du poids. Aucun arbitrage necessaire. 6 tests.

CONTROLE : verrou perime. recuperer_verrou_perime() ecarte par renommage atomique un verrou dont
le PID est mort, jamais celui d'un processus vivant, jamais un verrou illisible ou d'une autre
machine. Fichier conserve pour inspection, age calcule, reprise journalisee SANS nom de machine.
14 tests (huit cas demandes). Preuve reelle : verrou perime pose, chaine relancee, 7/7 OK.

NON FAIT, DIT CLAIREMENT : le cycle de vie operationnel des menages (statuts, creation hors
Hostaway, affectation, rattachements facture/charge/reglement/banque, prestataires qualifies,
catalogue de controles) n'est PAS construit. Module PARTIEL.
REFERENCE : 41_MODULE_MENAGES_ETAT_FINAL.md


## 2026-07-27 (nuit) - Cycle de vie Menages construit et prouve en recette

CONTROLE : recette navigateur reelle du cycle complet (port 8070, double verrou
MENAGES_CYCLE_REAL_WRITE_*).
PARCOURS PROUVE : creation (propriétaire resolu automatiquement) -> PREVU -> affectation ->
A_REALISER -> realisation (ecart 45->46 sous seuil) -> REALISE -> A_CONTROLER -> VALIDE.
A ce stade, /menages/cycle/controles a REELLEMENT signale CTRL_MEN_EXTERNE_SANS_FACTURE
(CRITIQUE) et CTRL_MEN_SANS_RESERVATION (INFO) - les controles fonctionnent sur un cas reel, pas
seulement en test unitaire.
Rattachement d'une facture existante (FAC-477C2F7A50BB, REGLEE, solde 0) -> contexte
facture/reglement/banque correctement affiche (reglement REG-1ED3A40AA329, aucun mouvement
bancaire rapproche - exact, ce reglement n'etait pas rapproche dans le jeu de recette).
Transitions FACTURE -> REGLE, historique 7 evenements tous horodates.
Second menage cree puis ANNULE : etat terminal, aucune action restante, jamais supprime.
REDEMARRAGE DU SERVEUR -> GET renvoie toujours REGLE. PERSISTANCE PROUVEE.

82 tests ajoutes (modele+service 28, controles 8, routes HTTP 11, plus les 46 du bloc precedent
de ce meme tour : pivot 9, verrou perime 14, cave/pools 6, garde workspace 4, flags 2 + ajustements).
Campagne ciblee -k "menage" : 230 passes / 14 skipes.

STATUT MODULE : PARTIEL (pas TERMINE) - restent pools de courses non alimentes en recette,
rattachement de charge non exerce en reel, formulaire UI de rattachement facture absent (fait par
script dans cette recette).
REFERENCE : 41_MODULE_MENAGES_ETAT_FINAL.md, 41b_AUDIT_MENAGES_CYCLE_DE_VIE.md


## 2026-07-27 (Mission 3) - Premier socle Comptabilite : defaut de solde corrige, prouve en recette

CONTROLE : equilibre debit/credit et idempotence de la generation d'ecritures.
CONSTAT (trouve en ecrivant les tests) : solde_compte()/solde_auxiliaire() ne comptaient que les
ecritures VALIDEE. Apres contrepassation (avoir), l'ecriture d'origine passe CONTREPASSEE et
sortait du calcul, alors que son miroir (VALIDEE) y restait compte. Consequence : le solde ne
revenait JAMAIS a zero apres un avoir - l'inverse de ce qu'une contrepassation doit garantir.
CORRECTION : VALIDEE et CONTREPASSEE comptent toutes les deux (une ecriture contrepassee reste une
ecriture historiquement postee, compensee par son miroir) ; seule PROPOSEE reste exclue.

PREUVE REELLE (recette navigateur, port 8080, sur FA-MEN-2026-06, facture reelle du jeu de
recette) : generation ACHATS 120,00 EUR equilibres -> validation -> solde fournisseur -120,00 EUR
-> contrepassation -> solde EXACTEMENT 0,00 EUR -> redemarrage serveur -> ecriture toujours
CONTREPASSEE (persistance) -> regeneration deux fois de suite -> MEME ecriture_id_opaque
(idempotence).

37 tests ajoutes (15 service + 9 routes + migrations/flags). Suite complete : 2110 passes / 75
skipes / 1 echec pre-existant (test_appsec1_diagnostic, inchange).
REFERENCE : 43_CADRAGE_COMPTABILITE_APPLICATION.md, 45_MODELE_ECRITURES_COMPTABLES.md


## 2026-07-28 - Pools de courses menages : seedes, ventilation bloquee (cause identifiee)

FAIT : trois charges affectable_menage=OUI ajoutees au jeu de recette (CHG_G1_COURSES 80 EUR ->
pool COURSES, CHG_G2_CONSO 40 EUR -> pool CONSOMMABLES, CHG_G3_HORS_MOIS 25 EUR en avril ->
exclue du mois mais comptee dans nb_affectables). Colonne affectable_menage ajoutee au contrat de
seeding, avec valeur par defaut NON pour les scenarios existants.

RECONCILIATION VERIFIEE : REEL 703,90 = 558,90 (avant) + 145,00 (G1+G2+G3).
COMPTABLE 638,90. Invariant REEL = COMPTABLE + HORS_COMPTA respecte (638,90 + 65,00 = 703,90).
19 tests de test_charges_pipeline.py passent.

BLOQUE, CAUSE STRUCTURELLE IDENTIFIEE : la ventilation des pools n'est pas exercable.
menages_chaine_service copie les sources depuis PROJECT_ROOT puis execute dans un workspace isole
avec stub reseau. Deux cas, aucun exploitable :
- PROJECT_ROOT = arbre reel : chaine 7/7 OK, mais elle lit la SAISIE REELLE, qui ne contient pas
  les charges fictives (et il est exclu d'y ecrire) ;
- PROJECT_ROOT = data_recette : la SAISIE fictive est bien lue, mais la source des declarations
  internes porte des noms d'appartements REELS qui ne se mappent sur aucun logement_id du parc
  fictif -> lot6d echoue sur nom_app(lg) = None (TypeError NoneType < str).

CE N'EST NI UN DEFAUT MOTEUR NI UN DEFAUT DE build_data_recette : c'est une lacune du jeu de
recette, qui n'a pas de source de declarations internes fictive alignee sur le parc fictif.
DEBLOCAGE : fabriquer cette source fictive (equivalent de source_sheet_copiee.csv) mappee sur
LOG_A1/A2/B1/C1 via REF_Mapping_Logements. C'est le premier travail du prochain tour.
REFERENCE : 41_MODULE_MENAGES_ETAT_FINAL.md section 7bis, 48_ROADMAP_RESTANTE_PROJET.md


## 2026-07-31 - Fermeture Analytique/Resultats : 8/8 reconciliations, recette reelle, campagne complete

CONTROLE : coherence des 8 reconciliations globales (Lot9<->Lot10, Lot10<->Analytique,
Analytique<->Comptabilite, Banque<->journal BANQUE, Factures<->auxiliaires, Menages<->charges,
Commissions<->VENTES, total analytique<->resultat global) sur un jeu fictif neuf, pipeline aval
complet lance en reel (6/6 lots OK, mois 2026-06).

CONSTAT (trouve en recette navigateur, pas en test unitaire) : reconciliation B comparait
`Lot10 GLOBAL` (total sur tout le jeu de donnees, sans grain mensuel) a l'Analytique filtree sur
le mois selectionne - ecart artificiel de 10 035,00 EUR. Grains incompatibles. Corrige : B ignore
desormais le filtre mois (route + export CSV), coherent avec son alias H. Detail complet :
JOURNAL_ANOMALIES.md (2026-07-31).

PREUVE REELLE (recette navigateur, port 8020, PROJECT_ROOT et APP_DATA_DIR isoles) : dashboard,
3 visions, tous les ecrans par axe (logement/proprietaire/plateforme/fournisseur/prestataire/
categorie/activite/menages/comptabilite), 8 reconciliations, tous les exports CSV, cumul,
comparaison mois precedent -> redemarrage serveur (persistance confirmee, valeurs identiques) ->
pipeline relance sur le meme mois (sorties sauvegardees avant ecrasement) -> second run
IDEMPOTENT (tous les indicateurs de comparaison a ecart 0,00) -> totaux /resultats et export
dashboard identiques -> AUCUN DOUBLE COMPTAGE.

CAMPAGNE DE TESTS PAR SHARDS (manifeste 05_APPLICATION/TEST_SHARDS_ANALYTIQUE_RESULTATS.txt,
temporaire) : 131 fichiers, 6 shards, codes retour reels a chaque fois, aucun run tue par
l'environnement. Anomalie de test trouvee et corrigee (faux positif garde APP-0 sur
lot9_flux_reader.py, renomme flux_unifie_reader.py - detail JOURNAL_ANOMALIES.md). TOTAL :
2281 passes / 75 skipes / 1 echec pre-existant (test_appsec1_diagnostic, inchange). Flake
proprietaires_reglements non reproduit (fichiers declencheurs isoles du shard qui contient
test_proprietaires.py).

STATUT MODULE : Analytique et Resultats TERMINES pour le perimetre defini (plateforme/activite
NON_DISPONIBLE assumes, pools multi-logements toujours un gap Menages connu, plan de comptes
toujours PROVISOIRE).
REFERENCE : 53_AXES_ANALYTIQUES_ETAT_FINAL.md, 51_MOTEUR_ANALYTIQUE_ET_RECONCILIATIONS.md,
48_ROADMAP_RESTANTE_PROJET.md, HANDOFF_CANONIQUE.md


## 2026-08-01 - Recette globale sur copies controlees des donnees reelles : verdict GO validation humaine

CONTROLE : recette complete de l'application sur une copie fidele des structures reelles (85
fichiers hashes SHA256, jamais le reel touche), hors mandat de mode reel.

CONSTAT : migrations saines sur copie de l'app.db reel (integrity_check ok, 0 violation FK,
idempotentes) et sur base vierge. Pipeline aval BLOQUE des lot9 : source Banque reelle
(BANQUE_LOT8_IMPORT.xlsx) inexistante dans l'arbre reel (dossier Lot8_Banque/ absent) - controle
moteur CTR-9-001 fonctionne comme concu, pas un defaut de code. Chaine charges (lot3) : SUCCES,
idempotente, 0 charge reelle saisie actuellement (etat reel, pas un defaut).

RECONCILIATIONS SUR 24 MOIS REELS (2025-01 a 2027-02, deux mois absents 2026-11/2027-01) : A, B, D,
H OK (ecart 0,00 EUR) - B confirme en conditions reelles le correctif de grains incompatibles du
2026-07-31. C, G A_CONTROLER et E, F NON_DISPONIBLE : attendus, 0 ecriture/facture/menage reel
enregistre dans l'app. Invariant REEL=COMPTABLE+HORS_COMPTA verifie sur le total reel (291
779,67 EUR), ecart 0,00.

SECURITE : 0 fuite (chemins absolus, PII, secrets) sur les pages et exports scannes. PERFORMANCE :
toutes les pages < 1 s. DOUBLE COMPTAGE : aucun trouve sur les axes exerces.

AUCUNE CORRECTION DE CODE APPLIQUEE (l'unique anomalie est un ecart de donnees reelles, pas un
bug). Suite de tests inchangee (2281/75/1, campagne du 2026-07-31 toujours valide).

VERDICT : GO POUR VALIDATION HUMAINE (jamais GO mode reel).
REFERENCE : 54_RAPPORT_RECETTE_GLOBALE_COPIES.md, 55_MATRICE_ECARTS_CONTRATS_REELS.md,
56_RAPPORT_RECONCILIATIONS_GLOBALES.md, 57_RAPPORT_DOUBLE_COMPTAGE.md,
58_RAPPORT_SECURITE_RECETTE_GLOBALE.md, 59_RAPPORT_PERFORMANCE.md, 60_VERDICT_GO_NO_GO.md


## 2026-08-02 - Contrat Banque Lot8 remonte a la racine, mois Lot10 expliques, verdict final NO GO

CONTROLE : audit cible du producteur de BANQUE_LOT8_IMPORT.xlsx (lot8a_banque_import.py) et
remontee de cause des deux mois Lot10 manquants (2026-11, 2027-01).

CONSTAT BANQUE : BANQUE_LOT8_IMPORT.xlsx est une SORTIE de Lot8, jamais une source directe. Sa
source brute (01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx, export Credit
Mutuel compte 02211 00021321603) n'existe nulle part - dossier absent du disque, reel et copies.
Lot8 100% executable hors reseau des que la source est fournie. Contrat Lot8->Lot9 verifie
coherent, aucune divergence code/documentation. CAS B confirme : NO GO — SOURCE BANQUE REQUISE.

CONSTAT MOIS LOT10 : remontee Lot10->Lot9->Lot4quater. Les deux mois sont deja absents de VUE_FLUX
(Lot4quater, onglet consomme par Lot9) alors que presents dans l'onglet MASTER (historique complet)
- chacun ne porte qu'une seule reservation placeholder A_CONTROLER/DIRECT_SANS_SAISIE_HH a montant
0 (LOG_0015/PROP_0011), exclue legitimement de la vue financiere. Filtrage upstream coherent, pas
un bug. RESOLU (explique), statut VIDE_VALIDE/NON_APPLICABLE.

AUCUNE CORRECTION DE CODE APPLIQUEE. Suite de tests inchangee (2281/75/1).

VERDICT FINAL : NO GO — SOURCE BANQUE REQUISE (porte sur la re-execution du pipeline aval, pas sur
la validite applicative des modules alimentes - validation humaine du perimetre alimente reste
AUTORISEE ; preparation du mode reel reste NO GO).
REFERENCE : 60_VERDICT_GO_NO_GO.md (mis a jour), 61_CONTRAT_SOURCE_BANQUE_LOT8.md,
62_RAPPORT_MOIS_LOT10_MANQUANTS.md


## 2026-08-02 (suite) - Releve Banque fourni, Lot8 execute sur copie : contrat incompatible

CONTROLE : le releve Credit Mutuel a ete depose (01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_
CreditMutuel.xlsx, 141986 octets, SHA256 a84c9b51...). Copie controlee vers l'environnement de
copies (hash source=copie identique verifie). Lot8a execute REELLEMENT sur la copie.

CONSTAT : ECHEC reproduit, code retour 1 - "[ERREUR BLOQUANT] Feuille Cpt 02211 00021321603
absente. Feuilles disponibles : ['Synthese', 'Mouvements', 'Mensuel', 'Controles', 'Sources']".
BANQUE_LOT8_IMPORT.xlsx non produit. Le fichier fourni est un rapport CONSOLIDE ("Releve bancaire
consolide - WONDERBNB", fusion de deux sources avec logique de priorite), pas l'export brut CM
attendu : meme compte (RIB identique), mais 12 colonnes au lieu de 7, periode de 9 mois au lieu du
mois nominal.

AUCUNE CORRECTION APPLIQUEE : ni renommage de feuille, ni adaptation des colonnes du script, ni
conversion du fichier - changerait le contrat metier unilateralement. Fichier original jamais
modifie (hash inchange verifie). 85/85 hashes reels historiques re-verifies identiques ; nouveau
releve ajoute au manifeste (86e fichier suivi).

AUCUNE CORRECTION DE CODE APPLIQUEE. Suite de tests inchangee (2281/75/1).

VERDICT FINAL (mis a jour) : NO GO — SOURCE BANQUE INCOMPATIBLE. Decision humaine requise : export
brut natif, ou adaptation explicite du contrat Lot8 au format consolide.
REFERENCE : 60_VERDICT_GO_NO_GO.md (mis a jour), 61_CONTRAT_SOURCE_BANQUE_LOT8.md (section Suite)


## 2026-08-02 (suite) - Lot8 accepte le format consolide, chaine aval rejouee, GO validation partielle

CONTROLE : implementation de deux adaptateurs convergents (FORMAT_CREDIT_MUTUEL_NATIF /
FORMAT_RELEVE_CONSOLIDE) dans lot8a_banque_import.py, provenance du consolide auditee (non
circulaire, 3 exports bruts successifs du meme compte), execution reelle sur copie, reprise de la
chaine aval complete.

CONSTAT LOT8 : BANQUE_LOT8_IMPORT.xlsx produit reellement - 541 mouvements, 0 BLOQUANT, 1
A_CONTROLER (periode multi-mois, non bloquant). Totaux identiques au centime pres a la feuille
Synthese du fichier source (51744,37 debit / 52148,21 credit). Idempotent (2 executions, memes
totaux, memes mouvement_id). Point corrige dans l'adaptateur : conversion 0->None sur Debit/Credit
(le format consolide renseigne toujours les deux colonnes, contrairement au natif) - sans elle,
chaque ligne aurait declenche a tort BANQUE_DEBIT_CREDIT_DOUBLES.

CONSTAT CHAINE AVAL : lot9->lot10->lot11->lot12->lot13 executes directement (scripts moteur, sur
copie) : tous verts. REEL=COMPTABLE+HC verifie (291852,76 = 281328,60+10524,16, ecart 0,00).
Idempotence confirmee (lot9/lot10 relances, memes totaux). Reconciliations rejouees via
l'application sur les sorties fraiches : A/B/D/H OK (ecart 0,00), C/E/F/G attendus A_CONTROLER/
NON_DISPONIBLE (0 ecriture Comptabilite reelle). Securite : 0 fuite (pages + 13 exports Power BI).

ECART DE COPIE CORRIGE : 02_DONNEES_NORMALISEES/ (requis par lot11) manquait dans l'environnement
de copies - complete, hash verifie, 88 fichiers reels de donnees suivis desormais.

NOUVELLE ANOMALIE TROUVEE, HORS MANDAT, NON CORRIGEE : lot4quater regenere VUE_FLUX scope au seul
mois demande (104 lignes) quand invoque via l'ecran /calculs de l'application, au lieu de
l'historique complet (1349 lignes) - declenche CTR-9-003 (volume suspect), sans rapport avec la
Banque. Sorties restaurees, aucune correction appliquee (hors mandat : "ne commence aucune nouvelle
fonctionnalite"). C'est pourquoi la chaine aval a ete rejouee via les scripts moteur directs plutot
que via /calculs.

11 tests nouveaux (tests/test_lot8a_banque_import.py). Suite complete tests/ (moteur) : 262
passes, 0 echec. Suite ciblee Banque application : 58 passes, 17 ignores, 0 echec.

VERDICT FINAL : GO POUR VALIDATION HUMAINE PARTIELLE (jamais GO mode reel). "Partielle" car (1)
l'anomalie lot4quater/CTR-9-003 empeche la re-execution depuis l'ecran applicatif lui-meme, (2)
Lot8b/Lot8c (classification/rapprochement) non executes, non requis pour prouver la compatibilite
de format mais necessaires pour un cycle Banque complet.
REFERENCE : 60_VERDICT_GO_NO_GO.md (mis a jour), 63_CONTRAT_FORMAT_RELEVE_BANCAIRE_CONSOLIDE.md


## 2026-08-02 (suite) - Anomalie lot4quater expliquee, cycle Banque complet, GO validation complete

CONTROLE : audit de l'anomalie lot4quater/CTR-9-003 (soupconnee bug d'orchestration), puis
execution du cycle Banque complet Lot8a->8b->8c, puis chaine aval depuis /calculs.

CONSTAT LOT4QUATER : le script ne prend AUCUN parametre mois, reconstruit TOUJOURS l'integralite
de l'historique. Execution directe sur copie -> 1391 MASTER / 1349 VUE_FLUX, correct. Cause reelle
du run defaillant precedent : HIST_Reservations_Cloturees.xlsx pas encore copie a ce moment
precis (complete depuis, mission anterieure, pour lot11) - lot4quater a applique son repli deja
documente (CLOTURE_SANS_HIST -> A_CONTROLER), pas un crash. PREUVE : /calculs relance deux fois
depuis l'ecran applicatif -> 6/6 lots SUCCES les deux fois, totaux identiques au centime pres a
l'execution moteur directe, idempotent. AUCUNE CORRECTION DE CODE - le contrat etait deja correct.
6 tests nouveaux (test_lot4quater_resoudre_source_reservations.py).

CONSTAT CYCLE BANQUE : Lot8b (classification, 24 VALIDE/517 A_CONTROLER) et Lot8c (rapprochement,
166 Airbnb + 56 proprietaires en attente, 0 confirmation automatique) tous deux SUCCES, tous deux
applicables au format consolide. Effet : Lot9 integre 24 flux frais bancaires (auparavant 0),
Lot11 passe a BANQUE_DISPONIBLE. Nouveaux totaux reconcilies et idempotents (REEL 291722,75 =
COMPTABLE 281198,59 + HORS_COMPTA 10524,16). Reconciliations A/B/D/H OK.

SECURITE : deux points pre-existants notes (bandeau MODE RECETTE chemin absolu ; libelles
bancaires avec fragments de compte tiers), hors mandat, non corriges, jamais reproduits dans la
documentation.

CAMPAGNE COMPLETE REJOUEE (TEST_SHARDS_RECETTE_GLOBALE.txt, 131 fichiers, 6 shards) : 2281 passes
/ 75 ignores / 1 echec pre-existant - identique a la reference, aucune regression sur toute la
serie de missions Banque.

88/88 fichiers reels de donnees re-verifies identiques.

VERDICT FINAL : GO POUR VALIDATION HUMAINE COMPLETE (jamais GO mode reel). Tous les criteres de
fin du bloc Banque/chaine applicative sont atteints.
REFERENCE : 60_VERDICT_GO_NO_GO.md (mis a jour), 64_RAPPORT_ORCHESTRATION_LOT4QUATER_LOT9.md,
65_RAPPORT_CYCLE_BANQUE_COMPLET.md

---

MISSION 2026-08-02 (suite) : preparation de la validation humaine et du dossier de mode reel.

CORRECTION FACTUELLE : le rapport oral (chat) de la mission precedente annoncait "3 commits" entre
7ea5a81 et e5248fd. Verifie via git log --oneline 7ea5a81..e5248fd : exactement 2 commits
(6d80612, e5248fd). HANDOFF_CANONIQUE.md n'avait jamais porte l'erreur - seul le recapitulatif oral.
Note ajoutee dans HANDOFF_CANONIQUE.md.

SECURITE - FUITE REELLE TROUVEE ET CORRIGEE : app.main._recette_globals exposait le chemin absolu
complet (incluant le nom d'utilisateur Windows) via RECETTE_ROOT/RECETTE_DB sur chaque page rendue
en RECETTE_MODE. Procedure test-rouge->correction->test-vert respectee :
test_securite_bandeau_recette.py (3 tests, rouges avant fix, verts apres). Correction : fonction
_nom_logique() (masquage par nom de fichier, jamais le chemin complet), app/main.py + base.html
(libelle "racine :" -> "environnement :"). Regression : 81 passes (subset), puis campagne complete
rejouee : 2284 passes / 75 ignores / 1 echec pre-existant (3 tests de plus que la reference, aucune
regression). Deux notes de securite pre-existantes restent ouvertes (hors mandat de correction,
cf. 60/65).

PACKAGE DE VALIDATION HUMAINE LIVRE (documents 66 a 72, 00_CADRAGE/BANQUE_LOGEMENTS_PDF_CHARGES_
METIER_20260724/) :
- 66_BASELINE_VALIDATION_HUMAINE.md : photographie figee de l'etat technique (git, sources,
  migrations, pipeline, Banque, reconciliations, tests, parametres recette).
- 67_PLAN_VALIDATION_HUMAINE.md : matrice module x scenario (colonne decision humaine vide),
  table des fonctions differees.
- 68_MATRICE_ARBITRAGES_BANQUE.md : synthese des 517 mouvements A_CONTROLER par motif, 166
  propositions Airbnb + 56 propositions proprietaires, aucune confirmation automatique.
- 69_GUIDE_RECETTE_UTILISATEUR.md : parcours pas-a-pas reproductible, 17 etapes.
- 70_MATRICE_ARBITRAGES_COMPTABLES.md : comptes provisoires a arbitrer (mapping catégorie->compte,
  frais bancaires, TVA hors perimetre - fiscal, pas technique).
- 71_DOSSIER_PREPARATION_MODE_REEL.md : sauvegardes, flags, 5 phases d'activation documentees,
  AUCUNE EXECUTEE, procedure de rollback et d'audit.
- 72_CHECKLIST_GO_NO_GO_MODE_REEL.md : 13 items de checklist, fiche de signature vierge (jamais
  remplie par Claude), verdict.

VERIFICATION FINALE D'INTEGRITE : 88/88 fichiers reels de donnees re-controles identiques au hash
baseline apres ce tour (seul ecart : lot8a_banque_import.py, code, deja documente comme
modification intentionnelle d'une mission anterieure).

VERDICT FINAL DE CE TOUR : NO GO - VALIDATION HUMAINE REQUISE (remplace le GO POUR VALIDATION
HUMAINE COMPLETE precedent, plus prudent tant qu'aucune decision humaine n'est rendue). Aucune
confirmation de rapprochement, aucun arbitrage metier decide, aucun flag de mode reel active par
cette mission.
REFERENCE : 60_VERDICT_GO_NO_GO.md (mis a jour, suite 5), 66 a 72, HANDOFF_CANONIQUE.md,
48_ROADMAP_RESTANTE_PROJET.md (mis a jour)

---

## Validation finale Banque/Tresorerie (2026-08-07)

Mission dediee : recette navigateur complete (tresorerie proprietaires, rapprochements exact/
partiel/groupe/ambigu/partiel-puis-groupe, file A_ENVOYER_IA, statut Airbnb), pipeline Lot8a a
Lot13 execute deux fois integralement sur copies (idempotence prouvee, chiffres identiques au
chiffre pres entre les deux passages), rollback exerce (sauvegardes horodatees restaurees et
verifiees), campagne complete rejouee : 2500 tests collectes, 0 echec nouveau, 1 echec deja
documente comme anterieur (test_07_diagnostic_local_avec_flag_explicite, chemin pytest local
contenant le nom d'utilisateur).

Controle d'integrite avant/apres : 85/88 fichiers reels identiques au hash baseline ; 3 ecarts, tous
ATTENDUS (deux fichiers de code lot8a/lot8c correspondant exactement au commit HEAD courant, deja
landes via des commits anterieurs a cette mission ; app.db reelle, gitignoree, usage normal de
l'application via le port 8000 preexistant, jamais touchee par les environnements isoles de cette
mission). git status vide en permanence sur le worktree. Port 8000/PID 21136 intact. Mode reel
jamais active.

Incident autocorrige : un lancement de lot8a a ecrit par erreur dans le worktree reel (flag
--project-root suppose a tort supporte partout) ; detecte immediatement, fichier gitignore
supprime, git status verifie vide, relance correcte. Documente : ANO-2026-08-... (JOURNAL_
ANOMALIES.md, "LOT8A CIBLE PAR ERREUR SUR LE WORKTREE REEL").

VERDICT (separe) : Bloc Banque/Tresorerie VALIDE SUR COPIES. Airbnb : NO GO (source absente).
Mode reel : NO GO (validation humaine toujours non rendue).
REFERENCE : 76_VALIDATION_FINALE_BANQUE_TRESORERIE.md, 60_VERDICT_GO_NO_GO.md (suite 6).

---

## Recette fonctionnelle globale (2026-08-08)

Smoke HTTP reel sur instance isolee (port 8030, copies, RECETTE_MODE=1) : 18/18 modules + 8
sous-ecrans Comptabilite repondent 200, aucune erreur 500/404. Parcours navigateur reel approfondi
sur Reservations hors Hostaway (seul module signale a risque par la mission) : resolution
propriataire/taux/menage standard confirmee fonctionnelle contre donnees copiees reelles.
**Aucun bug trouve, aucune correction necessaire, aucun code modifie.**

TVA : utilisateur confirme aucune TVA applicable actuellement (reponse enregistree dans
70_MATRICE_ARBITRAGES_COMPTABLES.md, aucune regle fiscale automatisee construite).

VERDICT TECHNIQUE : PRET POUR VALIDATION HUMAINE GLOBALE (pas une activation de mode reel).
Fiche de validation vierge livree (16 modules), a remplir exclusivement par l'utilisateur.
REFERENCE : RECETTE_FONCTIONNELLE_GLOBALE.md.

---

## Validation humaine LOT C/D/E + preparation mode reel (2026-08-10)

LOT C signe par l'utilisateur : Banque/Caisse ACCEPTE_AVEC_RESERVE (Caisse non validable, aucun
mouvement disponible sur la copie - pas un defaut Banque), Tresorerie proprietaires ACCEPTE.

LOT D/E exerces en profondeur sur instance isolee (port 8050, ecritures fictives, baseline
d'integrite 950 fichiers prise avant) :
- Chaine E2E comptable reelle : fournisseur -> facture 120 EUR -> ecriture ACHATS equilibree
  (606000/401000, auxiliaire opaque) -> validation -> 2 reglements -> 2 ecritures CAISSE -> OD
  validee (ODIVERSES). 3 journaux reellement alimentes.
- Desequilibre 100/60 REFUSE ("Le total debit doit egaler le total credit.").
- Periode 2026-05 clOturee -> ecriture REFUSEE ; reouverture sans justification REFUSEE, avec
  justification acceptee.
- Mapping provisoire explicitement marque (A_CONTROLER + MAP-GENERIQUE-606000, bandeau "Seed
  provisoire" sur le plan comptable). Jamais presente comme definitif.
- Invariant REEL = COMPTABLE + HORS_COMPTA verifie en direct : 291722.75 = 281198.59 + 10524.16,
  ecart 0.00 EUR. 8 reconciliations affichees, statuts honnetes (A/B/D/H OK, C/G A_CONTROLER,
  E/F NON_DISPONIBLE - attendu sur base vierge).
- Controles : 62 INFO comptes separement des 2358 bloquants (INFO != blocage). Exception sans
  justification REFUSEE, avec justification acceptee.
- Calculs : chaine aval 6/6 SUCCES depuis l'interface applicative (74.4 s) - couverture nouvelle.
  Idempotence : 2e run identique au centime. Rollback natif exerce : 8 fichiers restaures.
- Exports : 6 CSV applicatifs + 13 Power BI. Scan PII : aucune fuite (un motif suspect s'est
  revele etre un fragment d'identifiant opaque CTRL-70693637186f, verifie ligne par ligne).

INTEGRITE FINALE : 950/950 fichiers reels identiques au baseline, 0 modification, 0 absent.
REGRESSION : 591 passes / 37 ignores / 0 echec (perimetre Comptabilite/Resultats/Controles/
Cloture/Calculs). AUCUN BUG TROUVE - aucune correction necessaire, aucun code modifie.

Port 8000/PID 21136 intact. Toutes les instances de recette arretees. Mode reel jamais active.

VERDICTS (4 separes) : Application VALIDEE TECHNIQUEMENT SUR COPIES (validation utilisateur LOT D/E
restante) ; Comptabilite FONCTIONNELLE AVEC MAPPINGS PROVISOIRES ; Preparation mode reel PRETE
TECHNIQUEMENT / A SIGNER ; Mode reel NO GO - NON ACTIVE.
REFERENCE : RECETTE_FONCTIONNELLE_GLOBALE.md, PREPARATION_MODE_REEL.md, 72_CHECKLIST_GO_NO_GO_
MODE_REEL.md (restructuree en 6 domaines A-F).

---

## Clarification des bloqueurs de cloture + audit des writers (2026-08-10, suite)

DECISIONS LOT D/E signees par l'utilisateur : Comptabilite ACCEPTE_AVEC_RESERVE, Analytique
ACCEPTE, Resultats ACCEPTE, Controles/Cloture ACCEPTE SOUS CONDITION, Calculs/Exports ACCEPTE.
Les 16 modules ont desormais une decision humaine.

CORRECTION D'UN CHIFFRE DE MES RAPPORTS PRECEDENTS. Comptage exact sur l'export applicatif
(colonnes niveau + impact_cloture) : TOTAL 2420 lignes ; severite BLOQUANT 959, A_CONTROLER 1399,
INFO 62 ; 2357 lignes portent impact_cloture = "Bloque la cloture" ; 1 exception justifiee.
Mes rapports ecrivaient "62 INFO separes des 2358 bloquants", ce qui conflatait l'effet sur la
cloture et la severite BLOQUANT. Chiffre exact au sens "bloque la cloture", lecture ambigue.
CORRIGE dans tous les documents.

9 familles de codes, toutes des LACUNES DE DONNEES METIER (pas des defauts applicatifs) :
  838  BLOQUANT     GESTION_LOGEMENT_MISSING                        (RESERVATIONS)
  612  A_CONTROLER  RESERVATION_A_CONTROLER_SANS_COMMISSION         (COMMISSIONS)
  553  A_CONTROLER  GUEST_COUNT_MANQUANT_PREPARATION_CANAPE         (COMMISSIONS)
  221  A_CONTROLER  CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE   (BANQUE)
  121  BLOQUANT     CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE          (EXPLOITATION)
   12  A_CONTROLER  4 codes residuels                               (MENAGES / MENAGES_EXT)
AUCUNE resolue automatiquement, masquee, skippee ni transformee en exception.

CONSEQUENCE : aucun mois n'est cloturable aujourd'hui -> PREPARATION MODE REEL = NO GO.

AUDIT DES 9 WRITERS : le contrat de securite demande par la mission (double garde, defaut
fail-closed, write-guard de chemin, backup pre-ecriture, previsualisation/confirmation,
historique append-only, rollback) EXISTE DEJA et a ete verifie dans le code. Aucun mecanisme
parallele construit, AUCUNE ligne de app/config.py modifiee. Ce qui manque est l'etat "ecriture
reelle" (etat C), delibarement jamais construit ; le creer maintenant retirerait la protection
qui garantit qu'aucune ecriture reelle accidentelle n'est possible.
3 hardcodes analyses : HH et CONTROLES prets fonctionnellement mais maintenus False ;
REF_ASSOC_MODE NON_ACTIVABLE.

Sources reelles intactes, port 8000/PID 21136 intact, mode reel jamais active, aucun writer active.
REFERENCE : RECETTE_FONCTIONNELLE_GLOBALE.md, PREPARATION_MODE_REEL.md (section 2bis),
72_CHECKLIST_GO_NO_GO_MODE_REEL.md.

---

## GESTION_LOGEMENT_MISSING — diagnostic (2026-08-10, suite)

DEDUPLICATION : 838 lignes de controle = 137 couples logement x mois distincts, sur 14 logements
et la seule annee 2025 (ratio 6.12 : une ligne par reservation concernee). Le volume apparent
etait un artefact de comptage.

CAUSE UNIQUE (137/137 categorie A, ABSENCE_REELLE_HISTORIQUE) : les 17 lignes de
REF_Gestion_Logements_Hist ont TOUTES date_debut = 2026-01-01, toutes sourcees "Confirmation
operateur 28/06/2026". Le referentiel est une photographie de l'etat au 01/01/2026 ; il n'a jamais
contenu d'historique anterieur. Aucune autre categorie (borne, trou, statut, alias, incoherence)
n'est representee.

MOTEUR NON EN CAUSE - verifie empiriquement, pas suppose. lib_ref_history.applies_on() : debut
inclusif, fin inclusive, periode ouverte si date_fin vide. 6 cas de bornes testes sur le moteur
reel, tous conformes. Confirmation croisee par les donnees : aucun mois de 2026 n'apparait parmi
les couples manquants alors que tous les mois de 2025 y sont. 0 des 838 lignes imputable a un bug.
Aucune correction moteur appliquee (il n'y en avait pas besoin).

PREUVES HISTORIQUES : recherche negative. Les 5 archives datees de REF_Setup (99_ARCHIVES, juin
2026) NE CONTIENNENT PAS l'onglet REF_Gestion_Logements_Hist (cree posterieurement).
REF_Logements n'a aucune colonne de date d'entree/sortie. Bilan : PREUVE_A=0, PREUVE_B=0,
AMBIGU=0, ABSENT=137.

CONSEQUENCE : aucune reconstruction deterministe possible. Aucun overlay construit, aucune
simulation d'injection (ensemble vide). AUCUNE donnee inventee : ni date d'entree en gestion, ni
proprietaire historique. L'existence d'une reservation en 2025 et l'identite du proprietaire actuel
ont ete explicitement ecartees comme preuves insuffisantes, conformement au cadrage.

QUESTION UTILISATEUR (une seule, motif uniforme) : les 14 logements etaient-ils (a) deja en gestion
en 2025 avec les memes proprietaires - date d'entree reelle a preciser ; (b) avec d'autres
proprietaires/dates ; ou (c) hors perimetre de gestion en 2025 - regle de perimetre a ecrire.

VERDICT : GESTION_LOGEMENT_MISSING reste a 838 lignes / 137 couples / 14 logements.
CLOTURE : NO GO (137 couples ouverts + 8 autres familles bloquantes, 1519 lignes, non traitees).
PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON ACTIVE.
Sources reelles intactes, port 8000/PID 21136 intact.
REFERENCE : 77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md

---

## GESTION_LOGEMENT_MISSING - decision utilisateur appliquee au reel (2026-08-10, suite)

DECISION UTILISATEUR : couverture proprietaire (meme proprietaire_id qu'au 01/01/2026) prolongee
jusqu'au 01/08/2025 pour les 14 logements concernes. Janvier-juillet 2025 explicitement hors
perimetre, non reconstruits, rien invente.

APPLIQUE SUR COPIE D'ABORD : 14 lignes de REF_Gestion_Logements_Hist modifiees (date_debut
uniquement). Controles structurels verts : 0 chevauchement, 0 doublon, proprietaire/statut/
date_fin inchanges, 3 autres logements (LOG_0005/0009/0017) strictement intacts.

MESURE : GESTION_LOGEMENT_MISSING 838 -> 472 lignes (-366) ; couples logement x mois 137 -> 77
(-60). Restes janvier-juillet 2025 (77 couples, memes 14 logements) : jan=7 fev=64 mar=79 avr=83
mai=73 jun=82 jul=84.

EFFETS AVAL (copies) : REEL/COMPTABLE/HORS_COMPTA strictement identiques (291722.75/281198.59/
10524.16, ecart 0.00). Explicable : chaque logement n'a qu'une seule ligne de gestion, aucune
ambiguite proprietaire n'a jamais existe pour le calcul de commission.

APPLIQUE AU REFERENTIEL REEL, ordre impose respecte :
  1. backup 99_ARCHIVES/LOT0_REF_Setup/REF_Setup_PRE_PROLONGATION_GESTION_20260810.xlsm
  2. SHA256 verifie identique avant ecriture (a0ae8fbd... 88670 octets)
  3. previsualisation exacte du diff (seule date_debut de 14 lignes)
  4. ecriture (14 lignes, conforme au diff previsualise)
  5. relecture post-ecriture (14 lignes conformes, 3 autres intacts, 17 lignes au total comme avant)
  6. integrite globale : 1 seul fichier modifie sur 950 (REF_Setup.xlsm), exactement celui attendu.

RECALCUL REEL NON EFFECTUE, DELIBEREMENT : regenerer Lot9->Lot13 sur le reel exige le writer
Calculs (CALCULS_REAL_RUN_ENABLED), desactive conformement au NO GO mode reel en vigueur. La
mesure 838->472/137->77 a ete faite sur copies avec un diff strictement identique au reel -
representative, non recalculee sur le reel lui-meme.

VERDICT : GESTION_LOGEMENT_MISSING reste ouvert a 77 couples (jan-juil 2025, memes 14 logements).
CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON ACTIVE.
Port 8000/PID 21136 intact tout du long.
REFERENCE : 77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md section 10.

---

## Correction baseline controles - 2002 confirme exact (2026-08-10, suite)

Verifie sur l'artefact reel controles-cloture (comparaison code par code, avant vs apres
prolongation gestion) : TOTAL apres = 2002, EXACT, pas une erreur.

L'hypothese "2420-366=2054" supposait que seule GESTION_LOGEMENT_MISSING avait varie. FAUX :
CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE a aussi varie de -52 (121->69), effet aval REEL et
EXPLICABLE : ce controle depend de REF_Gestion_Logements_Hist (charges fixes resolues via ce
referentiel, CTR-LOT10-16). Delta reel total = -366-52 = -418. 2420-418 = 2002.

Repartition exacte apres (2002 lignes, verifiee code par code, tous les autres codes a delta 0) :
  GESTION_LOGEMENT_MISSING                        472
  RESERVATION_A_CONTROLER_SANS_COMMISSION         612  (inchange)
  GUEST_COUNT_MANQUANT_PREPARATION_CANAPE         553  (inchange)
  CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE   222  (inchange)
  CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE           69  (etait 121, -52 explique ci-dessus)
  11 codes residuels mineurs                       74  (inchanges individuellement)
  TOTAL                                          2002

---

## RESERVATION_A_CONTROLER_SANS_COMMISSION - rien a construire (2026-08-10, suite)

Audit des 612 : source directe MASTER_CALC_Commissions.xlsx onglet A_CONTROLER, 612 reservations
distinctes (pas de duplication). Causes : GUEST_COUNT_MANQUANT_PREPARATION_CANAPE 553,
RESERVATION_EXCLUE_A_CONTROLER 59 (source VRBO 32 + Direct 27, statut_calcul_payout=A_CONTROLER,
assigne en amont dans lot1_hostaway_extract.py, independant de tout taux). 0 cause liee au taux.

Verifie dans le code (lot10_calculer_resultats.py, _attach_commission_rate) : un taux manquant
declenche sys.exit(1) - arret dur du pipeline, jamais un simple A_CONTROLER par ligne. Puisque
tous les runs de cette session ont toujours abouti a SUCCES, la preuve directe est qu'aucune
reservation n'a jamais manque de taux.

DECOUVERTE : REF_Taux_Commission EXISTE DEJA (19 lignes, 12/12 proprietaires couverts), construit
le meme jour que REF_Gestion_Logements_Hist (28/06/2026), structure EXACTEMENT conforme a la
regle utilisateur : 2025-01-01->2026-01-31 a 15% pour tous, puis taux specifique a partir du
01/02/2026 quand il differe (7 proprietaires), ligne unique continue a 15% sinon (5 proprietaires).
0 trou, 0 chevauchement, 0 doublon.

CONSEQUENCE : aucune reconstruction necessaire, aucune simulation, aucune ecriture reelle - le
referentiel cible et le referentiel actuel sont identiques. RESERVATION_A_CONTROLER_SANS_
COMMISSION reste a 612, INCHANGE, pour des causes totalement independantes du taux.

VERDICT : GESTION_LOGEMENT_MISSING 472 inchange. GUEST_COUNT 553 inchange. BANQUE 222 inchange.
CHARGES 69 inchange. CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON
ACTIVE. Aucun code ni donnee reelle modifie ce tour.
REFERENCE : 78_RECONSTRUCTION_HISTORIQUE_COMMISSIONS.md

---

## GUEST_COUNT_MANQUANT_PREPARATION_CANAPE - constat, aucune action possible (2026-08-10, suite)

Dedup : 553 lignes = 553 reservations distinctes (source Lot10, onglet A_CONTROLER), sur 4
logements ayant une regle de preparation canape configuree (LOG_0006/0008/0011/0013 - les seuls
du parc), 23 mois (2025-01 a 2027-02).

Chaine tracee : lib_canape.calculate_canape_amount() - si le logement a une regle configuree et
guestCount absent -> A_CONTROLER, montant 0, jamais de valeur inventee. Taux de commission non
implique a aucun moment.

AUDIT TECHNIQUE : le correctif necessaire EXISTE DEJA (commit 719169d, "Correctif Hostaway -
nombre voyageurs pour preparation canape", 2026-06-20) - res.get("numberOfGuests") au lieu de
res.get("guestCount") qui n'existe pas dans l'API. Code deja correct.

CONSTAT DETERMINANT : le fichier reel MASTER_FACT_HA_Reservations.xlsx (copies) a guestCount vide
sur 1391/1391 reservations (100%) ET NE PORTE PAS la colonne numberOfGuests que le correctif
ajoute - preuve qu'il a ete produit par une extraction ANTERIEURE au correctif, jamais regeneree
depuis. Pas un bug de code : un probleme de fraicheur de donnees (aucune re-extraction Hostaway
reelle depuis le 20/06/2026).

Recherche de source locale fiable : NEGATIVE. Aucun cache de payload API brut, aucune archive
Lot1, aucune autre source. PREUVE_A=0, PREUVE_B=0, AMBIGU=0, ABSENT=553.

CONSEQUENCE : aucune correction de code, aucune donnee ecrite, aucune simulation (rien a
simuler). 553 reste inchange. RESERVATION_A_CONTROLER_SANS_COMMISSION reste a 612. TOTAL
controles reste a 2002.

QUESTION UTILISATEUR UNIQUE : autoriser une re-extraction Hostaway reelle (acces API reseau reel,
writer touchant des sources metier reelles) ? Resultat non garanti a 100% si l'API elle-meme ne
fournit pas numberOfGuests pour d'anciennes reservations.

VERDICT : GESTION_LOGEMENT_MISSING 472 inchange. BANQUE 222 inchange. CHARGES 69 inchange.
CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON ACTIVE. Aucun code ni
donnee reelle modifie ce tour.
REFERENCE : 79_RECONSTRUCTION_GUEST_COUNT.md

---

## RE-EXTRACTION HOSTAWAY REELLE EXECUTEE - GUEST_COUNT 553->506 (2026-08-10, suite)

Autorisation utilisateur explicite et scopee : lecture API reelle + nouveau
MASTER_FACT_HA_Reservations.xlsx + remplacement controle de ce seul fichier. Pas d'activation
mode reel general, pas d'autorisation Banque, pas de modification REF_Setup, pas de pipeline aval
reel relance.

Backup reel prealable verifie (hash identique). Extraction API reelle en zone temporaire isolee
(jamais d'ecriture reelle avant validation) : 1391->1527 reservations, guestCount 0%->100%
rempli. Comparaison exhaustive ancien/nouveau par reservation_id : 17 disparues verifiees EN
DIRECT via l'API (status=cancelled, cancellationAmount=None - conforme au code existant, aucune
modification de regle), 153 nouvelles (activite normale), 1 seul ecart economique reel (resa
prolongee 5->7 nuits, prix coherent au prorata). 0 doublon reservation_id.

Simulation complete sur COPIE INTEGRALE du projet (jamais sur le reel) : lot4bis->lot4quater->
lot9->lot10->lot11 rejoues avec l'interpreteur des lots. Stub NORM_Banque vide (0 ligne, hors
perimetre Banque, le dossier Lot8_Banque reel est aussi vide independamment de cette mission) pour
satisfaire uniquement la dependance technique CTR-9-001. 0 bloquant sur toute la chaine,
REEL=COMPTABLE+HC verifie a l'euro pres.

RESULTAT MESURE : GUEST_COUNT_MANQUANT_PREPARATION_CANAPE 553->506 (-47). Mecanisme verifie par
jointure exhaustive avec MASTER_CALC_Reservations_Resolues : les 506 restantes sont 100% en
etat_mois=CLOTURE (historique gele par conception, jamais reecrit meme par ce correctif) - la
resolution a atteint 100% de ce qui etait techniquement atteignable par re-extraction API (mois
ouverts), 0% des mois clotures par construction deliberee (immutabilite de la cloture), pas une
limite du correctif. RESERVATION_EXCLUE_A_CONTROLER 59->70 (base elargie, explicable). Total
A_CONTROLER Lot10 612->576.

REMPLACEMENT REEL : uniquement MASTER_FACT_HA_Reservations.xlsx (hash relu identique a la source,
1527 lignes/1527 reservation_id distincts/26 colonnes confirmes). Les autres fichiers
Lot1_Hostaway (Payout, Details, Fees, FinanceFields, Listings, Anomalies) restent les anciens -
desynchronisation partielle DELIBEREE, pipeline aval reel non relance (interdit explicitement par
l'autorisation).

INTEGRITE : 950/950 fichiers reels controles apres operation. 2 diffs, tous deux attendus :
REF_Setup.xlsm (deja committe 4b49a9d) + MASTER_FACT_HA_Reservations.xlsx (ce tour). 0 diff
imprevu. Port 8000/PID 21136 intact. Tests cibles : 62 passed, 0 nouvel echec.

AUCUN CHIFFRE DE CLOTURE REEL N'A CHANGE (pipeline aval reel non relance, chiffre 553 reste le
chiffre de cloture officiel tant qu'un run reel n'est pas rejoue).

VERDICT : CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON ACTIVE. Aucun
flag app/config.py modifie.
REFERENCE : 79_RECONSTRUCTION_GUEST_COUNT.md section 11

---

## AUDIT IMPACT 506 + CORRECTIF LOT4TER + CORRECTION REELLE (2026-08-11, suite)

Decision utilisateur : correction retroactive CIBLEE des 506 GUEST_COUNT_MANQUANT clotures (pas
de reouverture globale, pas de recalcul aveugle depuis le live, pas de synchronisation LIVE->HIST
generale, pas d'activation mode reel).

AUDIT D'IMPACT (copies) : 397/506 sans impact canape (delta 0), 109/506 avec correction
necessaire (+1090,00 EUR canape). Les 506 exclues de Commissions/NetProprietaire (A_CONTROLER) -
les y reintegrer ajoute 14302,91 EUR de commission et 75412,65 EUR de net proprietaire jamais
formellement reconnus. RESULTAT SOCIETE GLOBAL INCHANGE (REEL/COMPTABLE/HORS_COMPTA identiques au
centime dans toutes les simulations - le Flux Lot9 compte deja ces revenus independamment du
controle canape Lot10). 0 releve proprietaire existant pour ces 3 proprietaires/17 mois (verifie
en lecture seule sur app.db reelle).

DECOUVERTE CRITIQUE ET CORRIGEE : lot4ter_historiser_reservations_cloturees.py reecrivait HIST
depuis une liste COLS fixe de 28 colonnes n'incluant pas guestCount - toute correction de ce
champ etait silencieusement effacee au run normal suivant (nouvelle cloture). Reproduit par un
test rouge (tests/test_lot4ter_guestcount_persistence.py, 4 cas), corrige par l'ajout minimal de
"guestCount" a COLS + capture dans la construction de nouvelle ligne (2 lignes de code
modifiees). Test rouge->vert. Regression complete : 272 passed (moteur, root tests/) + 432
passed/36 skipped (app, 05_APPLICATION/tests), 0 nouvel echec. COMMITTE (9a0a6aa) AVANT toute
donnee reelle.

PREUVE DE PERSISTANCE (copies) : correction fail-closed des 506 (script
correction_guestcount_hist_ciblee.py - refuse tout champ hors guestCount, toute reservation hors
liste, toute valeur invalide, toute ecriture reelle sans AUTORISATION_ECRITURE_REELLE=1
explicite) puis run NORMAL de lot4ter corrige : guestCount survit (506/506, 1269 lignes
inchangees, 17 mois toujours CLOTURE). Chaine aval rejouee : GUEST_COUNT 506->0, A_CONTROLER
576->70 (VRBO/Direct seuls), resultat societe identique. Idempotence du cycle complet verifiee
(2e passage correction+lot4ter+aval : memes compteurs exacts, 506/70/313886.49 EUR). Rollback
verifie (hash restaure exact, compteurs reviennent a 576/506/70).

CORRECTION REELLE APPLIQUEE : backup horodate
(99_ARCHIVES/HIST_Reservations_Cloturees/..._PRE_CORRECTION_GUESTCOUNT_20260811_144118.xlsx, hash
verifie). Ecriture reelle : colonne guestCount ajoutee (29e colonne), 506 valeurs renseignees,
1269 lignes inchangees, 0 diff sur les 28 colonnes existantes (verifie programmatiquement contre
le backup). Journal append-only :
99_ARCHIVES/JOURNAL_CORRECTIONS/journal_correction_guestcount_hist_20260811.json (506 entrees,
aucune PII - reservation_id_hostaway = ID numerique opaque). Integrite globale : 950/950 fichiers
baseline controles, 3 diffs tous attendus et documentes (REF_Setup.xlsm + MASTER_FACT_HA_
Reservations.xlsx deja committes + HIST_Reservations_Cloturees.xlsx ce tour). Port 8000/PID 21136
intact.

PIPELINE AVAL REEL NON RELANCE (interdit explicitement, CALCULS_REAL_RUN_ENABLED jamais touche).
Les sorties de cloture reelles (MASTER_CTRL_Coherence.xlsx, export applicatif) n'integrent pas
encore la correction - effet d'un futur run autorise separement.

VERDICT : SNAPSHOTS CIBLES 506/506. CHAMPS MODIFIES guestCount uniquement. MOIS 17/17 toujours
CLOTURE. GUEST_COUNT (simulation) 506->0. A_CONTROLER (simulation) 576->70. Canape +1090,00 EUR.
Commission +14302,91 EUR (debloquee). Net proprietaire +75412,65 EUR (debloque). Resultat
societe : delta 0,00 EUR. Rollback VALIDE. Idempotence VALIDEE. HIST reel modifie : OUI. Pipeline
reel relance : NON. CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON
ACTIVE.
REFERENCE : 79_RECONSTRUCTION_GUEST_COUNT.md section 12

---

## AUDIT DES 70 RESERVATION_EXCLUE_A_CONTROLER VRBO/DIRECT (2026-08-11/12)

70 reservations (39 Direct + 31 VRBO), toutes connues de Hostaway, sans payout plateforme
calculable par design (DIRECT_HORS_HOSTAWAY/VRBO_UNKNOWN, comportement voulu et documente de
lot1_hostaway_extract.py::PayoutCalculator, docstring "H3: Direct/VRBO-Unknown -> None, jamais
valorise depuis Hostaway").

BUG REEL TROUVE ET CORRIGE : lot10_calculer_resultats.py construisait l'onglet A_CONTROLER
directement depuis le statut brut Lot1 (statut_calcul_payout=A_CONTROLER), sans verifier si la
reservation avait deja ete resolue ailleurs dans le pipeline via un mecanisme legitime deja
existant : VRBO via backfill CSV historique (deja en place depuis debut aout 2026), Direct via
saisie HH existante (decision D054, validee 2026-06-15). 28 reservations (27 VRBO + 1 Direct,
reservation_id_hostaway=60559486) etaient DEJA correctement comptees dans COMMISSIONS mais
listees une seconde fois par erreur en A_CONTROLER - double-comptage, pas une perte financiere.

PREUVE VERIFIEE PROGRAMMATIQUEMENT (pas une supposition) : les 28 reservation_id supprimees de
A_CONTROLER apres correctif sont 100% presentes dans COMMISSIONS avec des montants reels (0
suppression injustifiee, verifie explicitement).

CORRECTION : test rouge (tests/test_lot10_reservation_exclue_dedup.py, 2 cas - reproduit le bug
+ prouve qu'une reservation genuinement non resolue reste A_CONTROLER) -> fix minimal (5 lignes,
exclusion des reservation_id deja presents dans df_comm avant construction de df_ac) -> test
vert. Regression complete : 274 passed (moteur), 0 nouvel echec. Aucune autre regle de canal
modifiee.

SIMULATION (copie fidele, jamais sur le reel, idempotent) : RESERVATION_A_CONTROLER 70->42.
DELTA RESULTAT SOCIETE : 0,00 EUR (commission 41208,44 EUR, net proprietaire 215397,04 EUR,
REEL 313886,49 EUR, COMPTABLE 303362,33 EUR, HORS_COMPTA 10524,16 EUR - tous identiques
avant/apres, les 28 etaient deja comptees).

LES 42 RESTANTES : 38 Direct sans saisie HH + 4 VRBO sans backfill disponible = donnee absente
(DONNEE_ABSENTE), 0 cas ambigu, 0 bug technique residuel. Necessitent une saisie manuelle
humaine (mecanisme HH deja existant), pas une nouvelle decision de regle metier - la regle D054
existe deja et fonctionne correctement quand la donnee est fournie. Aucun payout invente, aucune
donnee Banque consultee, aucun mois cloture reouvert (10 logements, 7 proprietaires, 27 en mois
OUVERT / 15 en mois CLOTURE parmi les 42, aucune correction HIST necessaire - donnee absente,
pas gelee).

AUCUNE DONNEE REELLE MODIFIEE (correctif de code uniquement - aucune ecriture de donnee n'etait
eligible : ni PREUVE_A sur les 42 restantes, ni besoin de correction HIST puisque le bug etait
purement dans la construction de l'onglet A_CONTROLER, pas dans une donnee source).

VERDICT : CLOTURE : NO GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON ACTIVE. Aucun
flag app/config.py modifie.
REFERENCE : 80_AUDIT_RESERVATIONS_VRBO_DIRECT_A_CONTROLER.md

---

## MISSION DE NUIT - BASELINE CANONIQUE FRAICHE AVEC BANQUE REELLE (2026-08-12)

Simulation canonique fraiche reconstruite depuis HEAD 6b60577 : sources actuelles (REF_Setup,
MASTER Hostaway, HIST corrige) + Banque reelle regeneree sur copie (pipeline lot8a->lot8b->lot8c
DEJA VALIDE rejoue, 0 stub, 0 reaudition metier, 0 matching Banque-Reservation, 0 classification
manuelle des mouvements humains). Chaine complete rejouee : lot1(recalc-payout)->lot4bis->
lot4quater->lot9->lot10->lot11. Idempotent (2 runs consecutifs, memes compteurs exacts).

DECOUVERTE DETERMINANTE : les 541 controles BLOQUANT du systeme sont EXACTEMENT et UNIQUEMENT
GESTION_LOGEMENT_MISSING (472) + CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE (69). Verifie
programmatiquement : les 69 couples logement x mois de CHARGE_EXCEPTIONNELLE sont un
sous-ensemble strict a 100% des 77 couples GESTION_LOGEMENT_MISSING (0 couple hors gestion) -
meme cause racine (absence de periode de gestion applicable avant le 01/08/2025), pas un bug
separe, pas une donnee independante a corriger. Aucun autre BLOQUANT n'existe.

BANQUE FRAICHE : source reelle 01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx
(format consolide, deja gere), 541 mouvements, 1 doublon detecte, 236 classes deterministement,
222 en rapprochement humain (166 Airbnb export detaille absent + 56 proprietaires attente
acompte), 83 en file A_ENVOYER_IA. 0 produit economique cree (rapprochement/categorisation
seulement). Statut Lot11 passe de BANQUE_NON_DISPONIBLE_GIT a BANQUE_DISPONIBLE. REEL/COMPTABLE/
HORS_COMPTA avec Banque reelle : 313756,48/303232,32/10524,16 EUR, ecart 0,00 EUR (delta vs
simulation stub anterieure : -130,01 EUR sur REEL/COMPTABLE, explique par 24 flux de frais
bancaires reels desormais comptes au lieu de 0).

AUDIT RESIDUELS (Phase 11-12) : 0 nouveau BUG_TECHNIQUE trouve. Codes residuels tous classifies :
CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE (9, A_CONTROLER, ACTION_HUMAINE) ;
RESERVATION_A_CONTROLER_SANS_COMMISSION (42 agregees, ACTION_HUMAINE, deja documente) ;
VRBO_MONTANT_NON_RENSEIGNE (4 agregees, ACTION_HUMAINE) ; MENAGE_EXTERNE_ECART_HOSTAWAY (4
logements) ; MENAGE_EXTERNE_LOGEMENT_HORS_HA (2 logements) ; SOURCE_SHEET_PROVENANCE_INCOMPLETE
(garde-fou existant) - ces 3 derniers hors perimetre Menages de cette mission, documentes non
traites. 10 lignes INFO_LEGITIME (sources optionnelles absentes, HC_ZERO_SOURCES_VIDES) ne
bloquent pas la cloture.

42 DIRECT/VRBO : nouvelle requete API Hostaway en direct (lecture seule) sur les 42
reservation_id cette nuit - 100% ont paymentStatus=Unknown, airbnbExpectedPayoutAmount=None,
cancellationAmount=None confirmes frais. totalPrice existe mais n'est pas un payout (utiliser ce
champ inventerait une formule prix->payout non validee, interdit). 0 nouvelle PREUVE_A trouvee.

0 CODE MODIFIE CETTE NUIT (audit complet, 0 bug technique residuel). AUCUNE DONNEE REELLE
MODIFIEE (950/950 fichiers baseline, 3 diffs deja committes lors des missions precedentes, 0
nouvelle modification). Port 8000/PID 21136 intact tout du long.

VERDICT : APPLICATION VALIDEE. CORRECTIONS DETERMINISTES TOUTES EPUISEES. CLOTURE : NO GO (541
BLOQUANT, cause unique = historique gestion 2025). PREPARATION MODE REEL : NO GO. MODE REEL :
NO GO - NON ACTIVE.

3 DECISIONS HUMAINES FERMENT TOUT LE RESTE : (1) historique gestion 14 logements jan-juil 2025
(resout 472+69=541 BLOQUANT d'un coup) ; (2) 42 saisies manuelles Direct/VRBO ; (3) 222
mouvements bancaires humains + 83 file assistee deja engagee.
REFERENCE : 81_BASELINE_CLOTURE_APRES_NETTOYAGE.md

--- Mission gestion 2025 finale (2026-08-12) ---
Decision utilisateur : proprietaire toujours identique par logement sur toute la periode 2025.
REF_Gestion_Logements_Hist prolonge 2025-08-01 -> 2025-01-01 pour les 14 logements deja
prolonges. Simulation canonique fraiche (Banque reelle incluse, run_regression_pipeline.py,
2 runs identiques) : GESTION_LOGEMENT_MISSING 472->0, CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE
69->0, BLOQUANT total 541->0. A_CONTROLER (14) et INFO (10) inchanges code par code.
REEL/COMPTABLE/HORS_COMPTA : delta 0,00 EUR (une seule ligne de gestion par logement, aucun
changement de proprietaire/taux possible).

Reel modifie : 14 cellules date_debut uniquement (REF_Setup.xlsm), backup + SHA256 verifies,
relecture 14/14 conforme. Integrite globale : 3/950 diffs, exactement attendus. Pipeline reel
NON relance (garde OFF). Tests cibles 24/24 verts (0 code modifie).

VERDICT : CLOTURE TECHNIQUE (gestion+charges) GO. PREPARATION MODE REEL : NO GO (42 Direct/VRBO,
Banque humaine, 14 A_CONTROLER residuels, mappings comptables restent). MODE REEL : NO GO -
NON ACTIVE.
REFERENCE : 77_RECONSTRUCTION_GESTION_LOGEMENTS_HIST.md §13, 81_BASELINE_CLOTURE_APRES_NETTOYAGE.md §14

--- Pack final actions humaines (2026-08-12) ---
Ventilation exacte des 541 mouvements Banque : 236 CLASSE (deterministe), 166 PAYOUT_PLATEFORME
(0 decision, deja exclues du rapprochement reservation dans l'app, conforme 78877da), 56
proprietaires (1 prerequis Lot 5), 82 A_ENVOYER_IA distincts (83 physiques, 1 doublon
mouvement_id verifie conforme, 0 bug). Vrai total decisions Banque humaines : 138 (pas 305 brut).
42 Direct/VRBO : mecanisme de saisie existant audite et prouve sur fixtures (58/58 tests verts,
0 vraie reservation touchee). 14 A_CONTROLER decomposes : 11 reformulent les blocs
Reservations/Banque deja comptes, 3 seulement nouveaux (Menages ecart/hors HA, provenance info).
Mappings comptables 606000 : non bloquants techniquement. 0 code modifie, 0 bug trouve (mission
docs-only). CLOTURE TECHNIQUE : GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO - NON
ACTIVE. Reference : 82_PACK_FINAL_ACTIONS_HUMAINES.md

--- Audit Lot 5 et 82 A_ENVOYER_IA (2026-08-13) ---
Lot 5 identifie (lot5_master_acomptes_proprietaires.py, SAISIE_AcomptesProprietaires.xlsx,
MASTER_FACT_MAN_AcomptesProprietaires.xlsx) : FONCTIONNEL mais 0 ligne. Table
mouvements_tresorerie_proprietaires (migration 0025) : 0 ligne en recette, absente de la base
reelle (migration 0016). Aucun objet metier proprietaire n'existe.

56 mouvements proprietaires : PREUVE_A=0, PREUVE_B=0, AMBIGU=0, ABSENT=56. Rapprochement
EXACT/PARTIEL/GROUPE/AMBIGU=0, AUCUN=56. 100% CREDIT, 27069,18 EUR, 0 montant repete, 7 identites
candidates (dont FAMILLE_UZON_A_CONTROLER non resolue). 56 -> 56 decisions humaines, plancher 7 si
nature uniforme par proprietaire (non deduit).

82 A_ENVOYER_IA (83 physiques, 1 doublon mouvement_id confirme) : 79 DEBIT / 3 CREDIT, 3204,79 EUR.
0 correspondance deterministe avec les referentiels (proprietaires/associes/intervenants). 12
regles candidates preparees couvrant 57 mouvements sur 82. 82 -> 37 decisions si l'utilisateur
repond par regle, jamais par validation inventee.

Tests fixtures 126/126 verts. Idempotence Banque revalidee (236/222/83 identiques apres relance
lot8a/8b/8c). Integrite 3/950 diffs deja committes, 0 nouvelle modification reelle. 0 code
modifie, 0 bug. CLOTURE TECHNIQUE : GO. PREPARATION MODE REEL : NO GO. MODE REEL : NO GO -
NON ACTIVE. Reference : 83_AUDIT_LOT5_RAPPROCHEMENT_PROPRIETAIRES.md

--- Repetition migration app.db 0016->0026 + chaine Lot 5 (2026-08-13) ---
Base reelle app.db : baseline capturee (SHA256 8e299b93...70aa81d6, 421888 octets, integrity ok,
migration 0016, 36 tables / 52 index / 0 trigger, 6 tables non vides = 62 lignes). 2 copies
byte-identiques creees. **Base reelle jamais migree ni ouverte en ecriture ; hash identique a la
fin.**

Migrations 0017->0026 : purement additives (0 ALTER, 0 DROP, 0 DELETE, 0 UPDATE ; tous CREATE en
IF NOT EXISTS ; tous INSERT en OR IGNORE ; 0 clause REFERENCES). Sequentielle une par une :
integrity ok aux 10 etapes, 36->67 tables, 52->108 index, 0->1 trigger, 0 perte (hash canonique
des 5 tables metier conserve a chaque etape). Automatique via apply_migrations() : schema
strictement identique, contenu identique hors horodatages. Idempotence : 2 rejeux, 0 ecart.
Rollback par restauration de backup : hash exact restitue, base relisible en 0016.
MIGRATION PRETE ET REPETEE.

BUG REEL corrige (commit 1759ce0) : lot8c affirmait en dur "MASTER_FACT_MAN_AcomptesProprietaires
vide" sans jamais ouvrir ce fichier ; alimenter Lot 5 puis relancer ne changeait rien et ne le
signalait pas (prouve empiriquement sur copie). Fix minimal : lecture reelle de l'onglet MASTER,
0 objet -> comportement inchange, >0 -> LOT5_REGLES_RAPPROCHEMENT_A_ARBITRER. Aucune regle de
rapprochement inventee. 4 tests (1 non-regression + 3 rouges avant fix) verts.

GAP 2 documente non corrige : lot5 ne peuple pas MASTER depuis SAISIE (refresh Power Query dans
Excel requis ; M-code = 5 controles BLOQUANT + 5 A_CONTROLER + validation croisee HH + doublons).
Reimplementation en Python volontairement non entreprise (vraie feature, risque de divergence
metier).

12 regles candidates sur les 82 A_ENVOYER_IA validees TECHNIQUEMENT (pas metier) : 0 collision,
0 PAYOUT_PLATEFORME, 0 mouvement proprietaire, 0 deja classe ; 57 couverts + 25 isoles = 82.

Baseline controles apres fix : 0 BLOQUANT / 14 A_CONTROLER / 10 INFO / 24 total (inchangee).
Invariants : REEL 313756,48 = COMPTABLE 303232,32 + HORS_COMPTA 10524,16, ecart 0,00 EUR.
Integrite : app.db reelle hash identique, 3/950 diffs deja committes, 0 nouvelle modification
reelle. Port 8000 constate libre, non manipule. MODE REEL : NO GO - NON ACTIVE.
Reference : 84_REPETITION_MIGRATION_DB_0016_VERS_0026.md, 85_RUNBOOK_MIGRATION_APP_DB_REELLE.md

Campagne de tests sur base migree 0026 (APP_DATA_DIR isole) : shard 1 = 1253 passed / 1 failed /
59 skipped (16m28), shard 2 = 1170 passed / 0 failed / 17 skipped (14m08). TOTAL 139 fichiers :
**2423 passed, 1 failed, 76 skipped**. L'unique echec est test_appsec1_diagnostic::test_07
(nom d'utilisateur Windows dans le chemin temporaire pytest) -- echec environnemental
PRE-EXISTANT deja consigne dans 48_ROADMAP §Anomalies, sans lien avec le schema migre.
0 nouvel echec, 0 skip opportuniste.

--- Decisions utilisateur + audit UZON (2026-08-13) ---
DECISION 1 (historique proprietaires) : pas de differenciation fine des types de reglements pour
l'historique ; les FUTURS reglements devront porter leur vraie nature a la saisie. Le traitement
simplifie de l'historique NE DEVIENT PAS le comportement futur. Aucune nature n'a ete inventee :
le contrat 0025 contient deja une valeur neutre existante (AUTRE_A_CONTROLER), presentee a
l'utilisateur avec les consequences de chaque option ; le choix lui reste.

DECISION 2 (FAMILLE_UZON dissocie) : Maryline UZON = PROP_0011 = LOG_0015 "Studio - 97" ;
Didier UZON = PROP_0001 = LOG_0001 "Studio - 46", 46 allee Charles de Fitte. Deux proprietaires
distincts, jamais fusionnes, jamais de tresorerie partagee.

AUDIT des 7 mouvements FAMILLE_UZON_A_CONTROLER : les 7 portent le libelle explicite
"VIR MLLE MARIE-LINE UZON". MARYLINE 7 / DIDIER 0 / AMBIGU 0 (total 7, 1161,69 EUR). Cause du
regroupement : la regle R_074 ne cherche que le nom de famille "UZON", partage par les deux
proprietaires, et ignore le prenom -- qui etait pourtant present et discriminant. Variante
orthographique signalee (banque "MARIE-LINE" vs referentiel "Maryline"), sans ambiguite entre les
deux Uzon.

AUCUNE ecriture reelle : ni Lot 5, ni Banque, ni referentiel, ni app.db. R_074 non modifiee
(classification Banque gelee jusqu'a reponse utilisateur sur les 12 regles). HEAD 727d073,
worktree propre, port 8000 constate libre, mode reel OFF.

--- Facturation proprietaires (2026-08-13) ---
Migration 0027 : 4 tables (factures_proprietaires, _lignes, _evenements, _sequence). Table separee
de `factures` (fournisseur) apres audit : fournisseur_id_opaque NOT NULL, index unique
fournisseur+reference, facture_lignes.charge_id NOT NULL -> reutilisation impossible sans donnees
fictives.

Contrat facturable etabli : 5 types (COMMISSION_CONCIERGERIE, MENAGE_FACTURE, PREPARATION_CANAPE,
CHARGE_FIXE, CHARGES_EXCEPT_REFAC) = exactement les composants de montant_du_conciergerie (Lot 10).
7 types Lot 12 explicitement non facturables (payout, revenu net, acomptes, paiements recus, reste
a payer, statut). Grain conserve : mois x proprietaire x logement.

Nouveaux codes de controle : FACTURE_PROPRIETAIRE_SOURCE_INCOMPLETE, _DOUBLON, _TOTAL_INCOHERENT,
_IDENTITE_INCOMPLETE, _PDF_ABSENT, _SNAPSHOT_INCOHERENT, _EMISE_MODIFIEE.

Recette sur copie migree 0027, port 8042 (8000 jamais touche) : 3 factures fictives, emission
RECETTE-2026-00001, PDF 3582 octets telecharge avec sha256 identique au hash fige, anti-doublon
refuse, avoir -500 EUR avec originale intacte, solde derive sur 4 paliers, immutabilite du snapshot
apres changement des sources. 1 defaut cosmetique trouve et corrige (tirets cadratins hors latin-1
sortaient en "?").

Tests cibles : 89 verts (25 modele + 6 routes + 58 non-regression fournisseurs/charges).
Integrite : app.db reelle non migree, hash inchange ; REF_Setup, MASTER Hostaway, HIST, Banque,
sources Lot 5 inchanges. EMISSION REELLE : NON AUTORISEE. MODE REEL : NO GO - NON ACTIVE.
Reference : 86_FACTURATION_PROPRIETAIRES_APPLICATION.md, 87_RECETTE_FACTURES_PROPRIETAIRES.md

Regression ciblee facturation (2026-08-13) : suites factures (fournisseurs + proprietaires),
reglements, comptabilite et proprietaires -- **555 passed, 0 echec** (6m20). Non-regression du
module factures fournisseurs confirmee : creation, import PDF, liens charges, reglements partiel /
total / groupe, rapprochement Banque, controles. La campagne complete de la suite applicative a ete
lancee mais interrompue par l'environnement (killed) ; la regression ciblee ci-dessus couvre les
modules touches et ceux exposes au risque.

--- Source comptable unique : facture proprietaire (2026-08-13) ---
Audit prealable : la source VENTES etait ventes_lot12_adapter_service, origine
LOT12_PROPRIETAIRE_MOIS, grain proprietaire x mois (agrege), deja marque SOURCE_PROVISOIRE_LOT12.
La facture est au grain proprietaire x mois x logement -> meme realite economique a deux grains,
d'ou le risque de double comptage.

Nouveau generateur generer_ecriture_vente_facture : origine FACTURE_PROPRIETAIRE, origine_id =
facture_id_opaque, montant pris sur le total FIGE de la facture. BROUILLON 0 ecriture, VALIDE 0,
EMIS 1 (411000 debit / 706000 credit, statut PROPOSEE).

Garde bidirectionnelle FACTURE_PROPRIETAIRE_DOUBLE_SOURCE_COMPTABLE : refus dans les deux sens,
conflit signale jamais resolu en silence. Frontiere historique/futur = origine_type (reference
source explicite, pas une date). Aucune ecriture historique supprimee ni regeneree.

Verifie sur instance vivante (port 8043, base copie 0027) : BROUILLON 0 -> VALIDE 0 -> EMIS 1
ecriture equilibree 500/500 ; rejeu du generateur Lot 12 sur le meme mois -> refus explicite
citant la facture, total VENTES reste 500,00 EUR. Reconciliation lignes facture (300+150+50) =
total facture = produit comptable = creance = 500,00 EUR, ecart 0,00.

Reglement 200 -> solde 300 ; 500 -> solde 0 REGLEE ; compensation traitee comme extinction ;
aucune vente supplementaire a aucune etape. Avoir total : net produit 0,00 EUR, originale intacte.
Avoir partiel 100 : net 400,00 EUR.

Migration repetee jusqu'a 0027 : sequentielle (71 tables, 118 index, 0 perte), automatique
identique, idempotence 3 passages, rollback hash exact. Base reelle toujours 0016, non migree.
Tests : 15 nouveaux (comptabilite) + 62 verts sur le bloc facturation/comptabilite.
Reference : 86_FACTURATION_PROPRIETAIRES_APPLICATION.md §14, 84 §10, 87 §7-8.

Regression ciblee apres branchement comptable (2026-08-13) : factures (fournisseurs +
proprietaires), comptabilite, reglements, proprietaires, rapprochement bancaire, clotures --
**706 passed, 0 echec** (6m17). Non-regression du module factures fournisseurs et du socle
comptable confirmee.

--- Conformite des factures proprietaires (2026-08-13) ---
Numerotation legale : F-AAAA-NNNNNN (factures), A-AAAA-NNNNNN (avoirs), series independantes
portees par le compteur existant. Numero consomme uniquement a l'emission (brouillon abandonne =
aucun trou, verifie), fige, jamais reutilise, concurrence protegee, nouvelle serie par annee.

Migration 0028 additive : factures_proprietaires_conformite (identites figees, type client,
nature operation, periode prestation, regime TVA + mention, HT/TVA/TTC, conditions reglement,
champs electronic_invoice_* neutres) + factures_proprietaires_lignes_detail (qte, PU HT).
Sequentielle 73 tables / 122 index / 0 perte, automatique identique, idempotence 3 passages,
rollback hash exact. Base reelle toujours 0016.

Configuration unique facturation_config_service : aucune valeur juridique inventee, tout vide par
defaut. Regime TVA A_CONTROLER tant que non declare. Taux penalites et indemnite forfaitaire sans
defaut (bloquent l'emission professionnelle). Vocabulaire TVA aligne sur D083.

Controle de pre-emission unique : PRETE_A_EMETTRE / BLOQUEE + manques nommes. 11 codes stables.
Toujours affiche, ne bloque que si l'emission reelle est ouverte.

Recette E2E (port 8044, copie 0028) : F-2026-000001 et 000002 emises, conformite figee (periode
01/07->31/07, echeance 31/08, FRANCHISE_TVA, HT=TTC=500), ventes comptables generees, PDF complet
verifie, avoir cree, checklist de conformite affichee sur la fiche.

Tests : 28 nouveaux (conformite) + mises a jour. 74 verts sur le bloc facturation complet.
EMISSION REELLE : NON AUTORISEE. MODE REEL : NO GO - NON ACTIVE.
Reference : 88_CONFORMITE_FACTURES_PROPRIETAIRES.md

Regression ciblee apres conformite (2026-08-13) : factures (fournisseurs + proprietaires),
comptabilite, reglements, proprietaires, clotures -- **688 passed, 0 echec** (7m10).
Non-regression des factures fournisseurs et du socle comptable confirmee.

--- Completude fonctionnelle (2026-08-14) ---
Inventaire a partir des 250 routes reellement montees (pas de la roadmap). 54 fonctions
inventoriees : 48 DISPONIBLE, 5 PARTIEL, 1 MANQUANT, 0 BUG -> completude 89 %.

Quatre manques reels construits : creances proprietaires, dettes fournisseurs, echeancier,
balance generale. 22 tests dont persistance apres redemarrage. Navigation ajoutee.

Les 5 partiels restants sont tous utilisables et aucun n'est un trou d'architecture. Le seul
MANQUANT est la facturation electronique (chantier FACTURATION_ELECTRONIQUE_PA, modele pret).

Recette manuelle utilisateur redigee (89_RECETTE_MANUELLE_AVANT_BASCULE.md) : 15 sections, ~100
points, PRETE A EXECUTER. **Non validee** — elle ne le sera qu'apres retour explicite de
l'utilisateur. Reference : 92_MATRICE_COMPLETUDE_FONCTIONNELLE.md

Campagne large complete (2026-08-14), 144 fichiers en 3 shards :
- shard 1 (70 fichiers) : 1286 passed, 1 failed, 44 skipped (15m46)
- shard 2a (37 fichiers) : 606 passed, 31 skipped (4m12)
- shard 2b (37 fichiers) : 627 passed, 2 failed -> corriges -> verts (9m38)
TOTAL : **2519 passed, 1 failed** (test_appsec1_diagnostic::test_07, echec environnemental
pre-existant documente : nom d'utilisateur Windows dans le chemin temporaire pytest), 75 skipped.
Les 2 echecs de shard 2b etaient reels (inventaire EXPECTED_TABLES non mis a jour apres les
migrations 0027/0028) et ont ete corriges : test_sqlite_migrations 5/5 vert.

## 2026-08-16 — Renommage de la source bancaire et revision du controle de periode

CONSTAT : le fichier `2026_03_BRUT_Banque_CreditMutuel.xlsx` ne contenait pas le mois de mars 2026.
Mesure sur son contenu : 541 mouvements du 2025-11-03 au 2026-08-01, soit dix mois, dont seulement
74 en mars. Le nom etait faux.

ACTION : renomme en `BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx`.
SHA256 identique avant et apres (`a84c9b51...face0ca4a8`) : le renommage n a pas touche au contenu.

CONTROLE `BANQUE_FICHIER_PERIODE_INCOHERENTE` : il comparait la periode reelle a un mois nominal
code en dur dans lot8a (NOM_ANNEE=2026, NOM_MOIS=3). Il se declenchait donc a chaque import sans
qu aucune anomalie n existe. Le nom DECLARE desormais la nature de la source (HISTORIQUE / MENSUEL
/ INDETERMINE) et le controle ne verifie que ce que cette declaration promet. Il reste entier pour
un fichier declare mensuel.

VERIFICATION : chaine Banque relancee apres renommage. 541 mouvements, 222 RAPPROCHEMENT_REQUIS,
236 CLASSE, 83 A_ENVOYER_IA, Airbnb 166 lignes / 14467,27 EUR, proprietaires 56 lignes /
27069,18 EUR — identiques a avant. Ecart economique : 0.

Detail complet : document 97.

## 2026-08-17 — Rafraichissement Hostaway : run partiellement interrompu

CONSTAT : Lot1 relance en lecture seule. L extraction des reservations et des payouts est allee a
son terme (1542 reservations traitees, 288 sautees, 7 tables ecrites a 07:26:53-55). L etape
suivante - taches menage H6 - a tourne plus d une heure en butant sur des HTTP 429 (limites de
debit Hostaway), puis le processus a ete interrompu par l environnement a 08:32.

CONSEQUENCES : MASTER_FACT_HA_CleaningTasks_Discovery reste date du 2026-06-09, et surtout
MASTER_RUN_Log ne porte AUCUNE entree pour ce run - le lot ecrit son journal apres l etape menages.
Sa derniere entree reste celle du 2026-06-08.

PORTEE : nulle sur la baseline. Les chiffres ont ete mesures directement dans les fichiers produits
(payouts 1518 lignes, extraction 2026-08-17) et confirmes fonctionnellement par la disparition des
128 controles JOINTURE_PAYOUT_MANQUANTE.

A RETENIR : le journal de run du moteur n est pas une preuve fiable de rafraichissement tant qu il
est ecrit en toute fin de parcours. Un run interrompu apres l ecriture des donnees ne s y voit pas.


## 2026-08-17 — Banque et Lot 5 en SQLite

### Banque, sur le relevé réel (541 mouvements)

| Contrôle | Attendu (classeur) | Obtenu (SQLite) | Écart |
|---|---|---|---|
| Mouvements importés | 541 | 541 | 0 |
| `RAPPROCHEMENT_REQUIS` / `CLASSE` / `A_ENVOYER_IA` | 222 / 236 / 83 | 222 / 236 / 83 | 0 |
| `VALIDE` / `A_CONTROLER` | 24 / 517 | 24 / 517 | 0 |
| Risques ÉLEVÉ / MOYEN / FAIBLE | 74 / 215 / 252 | 74 / 215 / 252 | 0 |
| Constats de contrôle | 169 | 169 | 0 |
| `niveau_anomalie` renseigné | 74 | 74 | 0 |
| `codes_anomalie` renseigné | 1 | 1 | 0 |
| Attente plateforme | 166 lignes / 14 467,27 € | 166 / 14 467,27 € | 0,00 € |
| Attente propriétaires | 56 lignes / 27 069,18 € | 56 / 27 069,18 € | 0,00 € |
| Rapprochements `RESERVATION` | 0 | 0 | 0 |
| Débit / Crédit | 51 744,37 / 52 148,21 € | idem | 0,00 € |

Codes de contrôle identiques un à un : `IA_CONFIANCE_INSUFFISANTE` 83, `VIREMENT_BANCAIRE_AMBIGU` 67,
`VIR_ASSOCIE_DETECTE` 14, `REMBOURSEMENT_BANCAIRE_AMBIGU` 3, `IMPAYE_DETECTE` 1,
`DOUBLON_BANCAIRE_POTENTIEL` 1.

### Banque sans classeur

Classeur rendu introuvable : écrans, classification, files d'attente, contrôles et détail des
contrôles fonctionnent. Sans donnée en base, chaque écran annonce un état nommé
(`BANQUE_NON_INITIALISEE`, `BANQUE_NON_CLASSEE`) au lieu de se rabattre sur le fichier.

### Lot 5

Dix contrôles portés, comparés à la règle telle que le Lot 5 la rédige, sur fixture synthétique :
même verdict, même code, même niveau pour les huit cas de ligne, plus le doublon métier et le montant
invalide. FIFO : acompte 100 € sans facture → 100 € de crédit ; facture 80 € → soldée, 20 € de crédit
restant ; recalcul idempotent.

### Migrations et isolation

Copie de la base réelle 0016 → HEAD : 33 versions appliquées, `integrity_check` **ok**,
`foreign_key_check` **ok**, rejeu deux fois sans erreur ni changement de versions. Base réelle
**inchangée**, toujours en 0016. Deux instances `APP_DATA_DIR` distinctes : aucune contamination, ni
côté Banque ni côté Lot 5.

## 2026-08-18 — Hostaway et réservations en SQLite

### Parité, sur données réelles

| Étape | Attendu (legacy) | Obtenu (SQLite) | Écart |
|---|---|---|---|
| Lot 1 — réservations | 1 542 | 1 542 | 0 |
| Lot 1 — payouts | 1 518 | 1 518 | 0 |
| Lot 1 — listings / frais / champs / anomalies | 17 / 644 / 892 / 31 | idem | 0 |
| Lot 4bis | 1 542 × 19 colonnes | idem | 0 |
| Lot 4ter | 1 269 × 26 colonnes | idem | 0 |
| Lot 4quater | 1 542 × 27 colonnes | idem | 0 |
| `montant_retenu` | 322 868,56 € | 322 868,56 € | 0,00 € |
| `payout_calcule` | 320 525,08 € | 320 525,08 € | 0,00 € |
| `assiette_commission` | 257 971,08 € | 257 971,08 € | 0,00 € |
| `guestCount` (4bis / 4quater) | 3 557 / 2 178 | idem | 0 |
| `etat_mois`, `methode`, `canal` | — | répartitions identiques | 0 |

### Chemin API direct

Extraction simulée : 7 groupes RAW écrits, extraction close, fraîcheur lue au journal. Rejeu de la
même extraction : aucun doublon. Deux extractions successives restent comparables ligne à ligne. Une
extraction échouée n'est pas retenue comme utilisable ; une extraction partielle l'est, et est
signalée comme incomplète.

### Sans masters réservations

Masters rendus introuvables : couche RAW, datasets, cinq lecteurs, adaptateur de workspace et écran
d'actualisation fonctionnent. `guestCount` conservé sur un payload de 6 131 caractères et sur un
payload tronqué à 4 000 dont le JSON est invalide ; non inventé quand la coupure emporte
l'information.

### Mois clos

Une ligne archivée n'est jamais réécrite, y compris si un appelant transmet des valeurs différentes :
la garantie est dans le schéma (`INSERT OR IGNORE` sur `cle_historisation`). Rejeu de
l'historisation : 1 269 déjà en base, 0 ajoutée. Deux recalculs successifs ne modifient pas
l'historique.

### Migrations et isolation

Copie de la base réelle 0016 → HEAD : 34 versions, `integrity_check` **ok**, `foreign_key_check`
**ok**, rejeu ×3 sans erreur. Base réelle **inchangée**, toujours en 0016. Deux instances
`APP_DATA_DIR` distinctes : aucune contamination.

### Tests

Moteur 318 passed. Application 2 809 passed / 27 skipped. **0 failed.**

## Migration Ménages / Lot6 (2026-08-18)

### Lot6a→6f — SQLite

Chaque Lot accepte `--source SQLITE` en plus du chemin Excel historique (inchangé, conservé pour
parité). Formules et clés de ventilation non réécrites : seules les entrées/sorties changent de
support.

- Lot6a : lit `hostaway_cleaning_tasks` (RAW, 0035) + référentiels déjà importés (0029) au lieu de
  l'API/REF_Setup.xlsm. Écrit `menages_taches_enrichies` (0038).
- Lot6b : écrit `menages_declarations_internes` (0038) en plus du classeur M04 (conservé, sert
  encore Lot9-12 non migrés).
- Lot6d : lit Tasks (6a SQLite), déclarations (6b SQLite), factures ménage externes
  (`facture_lignes_menage`, 0037/0039 — bridge Lot6c). Écrit `menages_rapprochement` (0038). Mois
  sans valeur codée en dur : `--mois` explicite, sinon dernier mois présent dans les tâches.
- Lot6e : mêmes sources d'entrée que 6d + référentiels de coûts (0029). Écrit `menages_gainperte`
  (0038). Formule inchangée (`ecart = cout_standard_total - cout_reel_total`).
- Lot6f : idem, plus la mécanique de quote-parts (cave/lavage/courses/conso) inchangée. Le fetch
  réseau Google Sheet (lavage) est remplacé par une lecture de `menages_declarations_internes`
  (déjà calculé par 6b) — plus d'appel réseau redondant sur ce chemin. `SAISIE_Charges_Flux.xlsx`
  (module Charges, hors périmètre) reste lu en Excel dans les deux chemins. Écrit
  `menages_cout_complet` (0038).

Vérifications ciblées par Lot : COUNT, quelques IDs, rejeu sans doublon (DELETE+INSERT par mois),
refus propre sans base désignée. Tous verts (voir commits `e790460`..`81f7ea3`).

### Pont Lot6c (PDF → SQLite)

Construit lors de la mission précédente (`facture_menage_pdf_service`), non retouché ici — Lot6d/e/f
lisent directement `facture_lignes_menage`/`facture_lignes_menage_detail` (0037/0039, `quantite`
ajoutée cette session : absente de 0037, nécessaire pour compter des ménages plutôt que des euros).

### Suite — `menages_reader` : 6/8 sources en SQLite

Contrairement à la première tentative (réécriture complète annulée, cf. ci-dessus/JOURNAL_ANOMALIES),
une seconde passe bascule source par source, chacune vérifiée avant la suivante :
`hostaway_taches`/`hostaway_comptage` (0038, `menages_taches_enrichies` — `hostaway_comptage()`
dérive désormais son agrégat des tâches au lieu d'une seconde source indépendante et potentiellement
incohérente comme dans le classeur legacy), `internes` (`menages_declarations_internes`),
`rapprochement`/`gainperte`/`cout_complet` (`menages_rapprochement`/`menages_gainperte`/
`menages_cout_complet`). `rapprochement_available()`/`gainperte_available()` basculées sur l'état
de la source plutôt que l'existence d'un fichier.

Régression corrigée au passage : les tests de route basés sur le seul fixture `client` (sans le
fixture `sources`, qui isole `cfg.MASTER_*`) lisaient sans le vouloir les classeurs Excel RÉELS du
projet via les chemins par défaut — invisible tant que le lecteur retombait sur Excel. Seed SQLite
minimal ajouté où nécessaire (`test_menages.py`, `test_menages_chaine.py`,
`test_menages_rapprochement.py`).

Restent Excel : `externes()` (Lot6c — exigerait d'étendre `facture_lignes_menage` avec
`date_menage`/`precision_date_menage`, absentes de 0037/0039), `controles_rapprochement()`/
`controles_lot11()`/`pools_charges()` (dérivation non triviale ou usage marginal, non vérifié
comme rentable dans le temps restant).

### Non couvert — 4 services/lecteurs restants

`menages_chaine_service`/`menages_recalcul_service`/`controles_runner_service` orchestrent la
chaîne Excel complète jusqu'à Lot9-12 (non migrés, explicitement hors périmètre) — les migrer
isolément aurait exigé de commencer Lot9-12. `controles_detail_reader` lit
`MASTER_MENAGES_EXTERNES` (même dépendance que `externes()` ci-dessus).

### Migrations et isolation

Migrations 0035→0039 additives. Copie de la base réelle 0016 → HEAD (0039) : `integrity_check`
**ok**, `foreign_key_check` **ok**, rejeu ×2 sans erreur. Base réelle **inchangée**. Deux instances
`APP_DATA_DIR` (A/B, données ménages différentes) : aucune contamination croisée constatée.

## Lot9/Lot10 — fermeture définitive

Migrations 0043 (`flux_unifies`) et 0044 (`lot10_*`) additives. Ménages fermé sans master
permanent (10/10 sources du lecteur, `externes()` inclus). Lot9 et Lot10 basculés en SQLite,
parité réelle prouvée 0,00 € d'écart sur les 5 composants + net propriétaire + résultats +
invariant REEL=COMPTABLE+HORS_COMPTA (4053 clés comparées ligne à ligne, 0 manquante, 0 écart).

Tests bloquants actifs par interception `openpyxl.load_workbook` : `test_menages_sans_excel.py`,
`test_lot9_sans_master_calc_flux.py`, `test_lot10_sans_masters.py`.

Copie de la base réelle 0016 → HEAD (0044) : `integrity_check` ok, `foreign_key_check` ok, rejeu
×2 sans erreur. Base réelle inchangée (hash identique). Campagne complète : moteur 345/345,
application 2856/2856, 0 échec.

## Lot11 — contrôles transverses, moteur SQLite natif

`controles_lot11_service.py` (migration 0045) recalcule directement en SQLite les groupes de
contrôle Lot11 dont les sources sont déjà migrées, écrit dans `controles_lot11_constats`/`_champs`
(mêmes tables que la reprise classeur legacy). Parité réelle prouvée le 2026-08-17 : 10/11
constats en accord exact avec `MASTER_CTRL_Coherence.xlsx`, 0 BLOQUANT des deux côtés. Test
bloquant `test_lot11_sans_masters.py` (interception globale openpyxl) actif.

## Lot12 — préfactures propriétaires, moteur SQLite natif

`lot12_prefactures_service.py` (migrations 0046/0047) recalcule directement en SQLite les
préfactures propriétaires depuis Lot10/Lot11/référentiel, écrit dans `lot12_*` (dataset versionné,
run actif). Parité réelle prouvée le 2026-08-17 : 285/285 préfactures, 3481/3481 lignes, 0 écart de
montant sur l'intégralité du jeu comparé à `MASTER_FACT_Proprietaires.xlsx`. Tests bloquants actifs
(sans masters + pas de double comptage).

## Lot11 fermé à 100% — MASTER_CTRL_Coherence hors runtime

Les 6 derniers groupes legacy sont portés dans `controles_lot11_service`. Parité réelle complète :
23/24 constats identiques sur `message`, `commentaire`, `source_module`, `severity`,
`impact_facture`, `statut_resolution` — listes de logements du groupe 6f comprises. Écart restant :
`CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` 9 vs 8, divergence de fraîcheur entre la copie de
`REF_Cloture_Mensuelle` du classeur Banque et celle de `REF_Setup` (prouvée côté données).

Deux chemins lisaient encore le classeur : le runner de recalcul (désormais SQLite sur copie de
base, sans sous-processus) et `controles_cloture_reader` (désormais lecture des constats en base).
Test bloquant `test_master_ctrl_coherence_zero_runtime.py` : toute ouverture du classeur permanent
fait échouer immédiatement, écrans de contrôles/clôture verts.

## 2026-08-22 — Clôture HH + extras propriétaires, ZERO EXCEL OPÉRATIONNEL = OUI

HH (réservations hors Hostaway) : parité OLD/NEW formelle prouvée par test dédié
(`test_reservations_hh_confirmation.py::test_parite_decision_persistance_tous_champs_economiques`)
comparant, champ par champ, la décision de prévisualisation à la ligne persistée en base
(réservation, logement, propriétaire, arrivée, départ, montant, ménage standard/override,
commission standard/override, montant récupéré, associé récupérateur, montant reversé
propriétaire, statut) — 0 différence économique inexpliquée. Le service Lot4A de comparaison
legacy (`lot4a_dryrun_runner.py`) confirmé 0-appelant, supprimé.

AirCover / Imputations Airbnb / Ajustements post-clôture (APP-3D) : migration 0054, 3 tables
créées avec les colonnes reprises verbatim des en-têtes réels observés. Les 3 fichiers Excel
réels ne contenaient qu'une ligne d'en-tête (0 donnée) — aucune reprise historique nécessaire.
Acomptes déjà SQLite (mouvement de trésorerie propriétaires + FIFO), aucun changement requis.

Bug trouvé et corrigé pendant la campagne de contrôle finale : migration 0054 n'inscrivait pas
sa propre version dans `schema_migrations` (contrairement à toutes les migrations précédentes),
révélé par `test_clotures.py::test_31_idempotence`. Corrigé par l'ajout de la ligne `INSERT OR
IGNORE INTO schema_migrations (version) VALUES ('0054')`.

Inventaire Excel runtime refait de zéro sur les 58 fichiers `app/` référençant Excel — 5 critères
mission tous à 0 (`SAISIE_EXCEL_OBLIGATOIRE`, `REF_SETUP_RUNTIME`, `EXCEL_ENTRE_MOTEURS`,
`MASTER_CALCULE_REQUIS`, `BUG_RUNTIME_EXCEL`). Campagne complète rejouée : moteur 345/345 passed,
application ~2900 tests passed hors 4 défauts pré-existants confirmés (par `git stash`) sans
rapport avec Excel : `test_banque_controle.py::test_07_08_rattacher_proprietaire_logement`,
`test_banque_controle_finition.py::test_11_type_flux_valeur_technique_conservee`,
`test_menages_chaine.py::test_chaine_e2e_reelle_sur_copies`,
`test_ventes_lot12_adapter.py::test_generer_ecritures_du_mois(_idempotent)`.

**VERDICT : ZERO EXCEL OPÉRATIONNEL = OUI.**

## 2026-08-22 (suite) — nettoyage contrôlé du legacy Excel devenu mort

Après le verdict ci-dessus, nettoyage ciblé (pas un audit général) des writers/readers/services
Excel devenus 0-appelant par la migration HH/Charges/extras : `saisie_hh_writer.py`,
`saisie_charges_writer.py`, `saisie_charges_transaction_service.py` (sauf `_remplacer_fichier`,
déplacée vers son seul appelant réel `calculs_pipeline_service.restaurer`),
`saisie_charges_journal_service.py`, l'essentiel de `saisie_hh_schema_migration.py` (constante
`NEW_SAISIE_FIELDS` conservée) et 15 fonctions mortes de `saisie_charges_reader.py`. 6 fichiers de
test devenus obsolètes supprimés avec eux. Détail complet, tableau d'inventaire et preuve
0-appelant : `NETTOYAGE_LEGACY_POST_SQLITE.md`.

Campagne complète rejouée après nettoyage : moteur 345/345 passed (inchangé), application
~2593 passed, 0 nouvelle régression — les 4 échecs pré-existants documentés ci-dessus reviennent
identiques (confirmés par `git stash` comme antérieurs à ce nettoyage).

## 2026-08-22 (suite) — clarification des adaptateurs SQLite→Excel restants, baseline 0 failed

**Audit des adaptateurs Lot1/Lot4quater/Lot6b/Lot6c/Lot8/Lot10** (mission de fermeture technique
post-legacy). Distinction retenue : pandas n'implique pas Excel — seul un adaptateur qui produit
réellement un classeur intermédiaire, ET que « Actualiser toute l'activité » atteint réellement,
viole `ZERO EXCEL OPÉRATIONNEL`.

- `orchestrateur_moteur.py::executer_lot10` (Lot10, dans `orchestrateur_dag.NOEUDS`) : appelle le
  script pandas legacy avec `--source SQLITE --db <base>` — SQLite→DataFrame→SQLite, aucun
  classeur. **CONSERVÉ, conforme.**
- `reservations_adaptateur_moteur.py` (Lot4quater→Lot11) : `ecrire_resolues`/`ecrire_payouts`/
  `ecrire_tout` produisent bien un XLSX, mais leur seul appelant réel est
  `menages_chaine_service.py` — non présent dans `orchestrateur_dag.NOEUDS`, atteignable
  uniquement via la route recette `POST /menages/chaine/executer` (`MODE_COPIES` forcé, mode réel
  explicitement refusé). Seule `_vue_flux` (fonction pure, 0 I/O) est réutilisée par
  `flux_unifie_service.py` (Lot9, DAG réel). **LEGACY_PARITE, 0 appelant runtime normal — prouvé.**
- `banque_adaptateur_moteur.py`, `hostaway_cleaning_tasks_adaptateur_moteur.py`,
  `adaptateur_workspace.py` : même chaîne isolée que ci-dessus, mêmes conclusions.
  **LEGACY_PARITE.**
- `hostaway_adaptateurs.py`/`hostaway_cleaning_tasks_adaptateur.py` (sens Excel→SQLite, reprise
  H6) : 0 appelant dans `app/` en dehors de leur propre test. **IMPORT_PONCTUEL, conservé.**

**Verdict : ADAPTATEURS XLSX RUNTIME = 0/9. EXCEL ENTRE MOTEURS = 0/9. ZERO EXCEL OPÉRATIONNEL
reste OUI**, formulation précisée : les classeurs produits par la chaîne `menages_chaine_service`
existent mais ne sont jamais atteints par le run automatique.

Test bloquant renforcé (`test_bootstrap_zero_excel.py`) : le garde n'interceptait que
`openpyxl.load_workbook` (lecture) ; `openpyxl.Workbook.save` (écriture) ajouté sur les mêmes
motifs interdits — toujours vert, confirme qu'aucun classeur interne n'est ni ouvert ni créé
pendant un run complet de l'orchestrateur.

**Les 4 défauts pré-existants ont été corrigés** (causes réelles : `db_path` non transmis et
cache non invalidé dans `banques_controle_service.py`, `SNAPSHOTS_DIR` non isolé dans
`test_menages_chaine.py`, fixture `db` pointant vers une base différente dans
`test_ventes_lot12_adapter.py` — détail `JOURNAL_ANOMALIES.md`). Campagne complète rejouée :
moteur 345/345 passed, application ~2599 passed. **0 failed.**

## 2026-08-22 (suite) — durcissement SQLite Phase 1 (fiabilisation)

Migrations 0055/0056 : FK réelles ajoutées (`banque_classifications.mouvement_id_opaque` →
`banque_mouvements`, `factures_proprietaires_lignes.facture_id_opaque` → `factures_proprietaires`)
et CHECK de domaine fermé (`factures_proprietaires.statut`/`type_document`,
`factures_proprietaires_lignes.type_ligne`) — toutes vérifiées exhaustives dans le code avant
fermeture, et vérifiées sans ligne orpheline sur une copie de la vraie app.db (0016) migrée
jusqu'à HEAD. Mécanismes déjà en place non dupliqués : dataset actif en double (Lot10/Lot12),
doublon facture, doublon classification — tous confirmés toujours actifs par test négatif.

**Erreur trouvée par la campagne complète, corrigée dans la même mission** : le CHECK initial sur
`banque_mouvements.sens` (migration 0055) cassait `test_sens_incoherent_detecte` —
`banques_controles_catalogue.py` détecte volontairement un `sens` hors domaine comme anomalie
CONTRÔLABLE, pas un rejet SQL. Migration 0055 non modifiée (immuable) ; correction par 0056.

4 contrats de données typés (`app/contrats_donnees.py`, dataclasses) créés et testés : `Charge`,
`ReservationHH`, `MouvementBanque`, `MouvementTresorerieProprietaire`. Délibérément non câblés
dans les services de saisie (risque de régression identifié sur le format de date accepté par
`charges_saisie_service.valider()` — câblage différé après audit).

Transactions multi-table (facture + lignes) auditées : déjà atomiques (connexion unique, commit
unique en fin de fonction, pas de commit sur exception). Aucun changement nécessaire.

Campagne finale : moteur 345/345 passed, application ~2621 passed (8 shards). **0 failed.**
Aucune règle métier modifiée, aucun écart financier. Détail complet :
`DURCISSEMENT_SQLITE_CONTRATS_DONNEES.md`.

## 2026-08-22 (suite) — industrialisation socle technique : sauvegarde/rollback/observabilité

Audit initial : l'essentiel du socle demandé existait déjà (`snapshot_service.py`,
`calculs_pipeline_service.py`/`calculs_runs`/`calculs_sauvegardes`, pattern CURRENT/CANDIDATE déjà
en place sur `lot10_runs`/`lot12_runs` via index UNIQUE PARTIEL `WHERE actif=1`). Deux manques
réels identifiés et comblés : (1) aucune sauvegarde de `app.db` elle-même n'existait avant une
migration de schéma ; (2) aucune vue centralisée des runs tous types confondus.

Migration 0057 : `sauvegardes_base` (git_commit, database_hash, validation_status) et
`run_history` (STARTED/VALIDATING/SUCCESS/FAILED/ROLLED_BACK). Services `backup_service.py`
(sauvegarder/vérifier/lister/restaurer/purger — purge jamais automatique, vérifié par test),
`run_history_service.py`, `migration_service.migrer_avec_sauvegarde()` (sauvegarde → migration →
`integrity_check` → validation, restauration automatique sinon).

Point technique trouvé en testant le rollback : `restaurer()` remplace tout le fichier cible, y
compris la ligne `run_history` du run en échec écrite juste avant — elle disparaît avec le fichier
remplacé. Corrigé : l'issue est rejournalisée sur le fichier restauré, dans une entrée fraîche,
après la restauration.

`apply_migrations()` (appelée par le fixture `tmp_db` de centaines de tests) volontairement NON
modifiée — `migrer_avec_sauvegarde()` est le nouveau point d'entrée pour une vraie migration.

Écran `/observabilite/runs` (lecture seule) ajouté. Tests : 18 (backup + run_history + migration
protégée) + 2 (route). Campagne ciblée verte. Détail complet :
`INDUSTRIALISATION_SOCLE_TECHNIQUE.md`.

## 2026-08-23 — orchestrateur global câblé au socle sauvegarde/rollback

Audit initial : `orchestrateur_dag.py`/`orchestrateur_service.py` couvraient déjà presque tout ce
que la mission « orchestrateur global » demandait (DAG déclaratif, propagation d'invalidation en
cascade, `moteur_runs`/`moteur_run_etapes`, écran `/actualisation` avec tâche de fond et reprise
après crash). Deux vrais manques comblés, sans rien reconstruire :

- `backup_service.sauvegarder()` appelé avant une actualisation GLOBALE réelle (`cibles=None`,
  jamais sur une cible unique) ; run journalisé en parallèle dans `run_history` (registres
  existants non remplacés) ; `PRAGMA integrity_check` après le run — restauration automatique
  + `ROLLED_BACK` UNIQUEMENT si elle échoue (un simple `PARTIEL` reste géré sans rollback, les
  données déjà recalculées restant valides par construction).
- Mode `dry_run=True` : calcule le plan de dépendances sans exécuter ni activer, route
  `POST /actualisation/tout/dry-run` + bouton dédié.

**Même défaut technique que la mission précédente, reproduit puis corrigé de la même façon** :
`backup_service.restaurer()` remplace tout le fichier cible, y compris la ligne `run_history` du
run en écriture — l'issue `ROLLED_BACK` est donc rejournalisée dans une entrée fraîche après
restauration, référençant le run d'origine.

**Défaut de test découvert et corrigé** : câbler `backup_service` a révélé que plusieurs fixtures
isolaient `cfg.DB_PATH` sans isoler `cfg.BACKUPS_DIR` — une actualisation globale dans un test
aurait écrit une vraie sauvegarde sous le `BACKUPS_DIR` réel. Corrigé à la source (fixture partagé
`tmp_db` + 2 fixtures locales), vérifié après coup (`data/backups` absent).

Tests : 7 nouveaux (`test_actualisation_backup_rollback.py`) + 1 (route dry-run). Campagne
complète rejouée : moteur 345/345 passed, application ~2653 passed (8 shards). **0 failed.**
Détail complet : `ORCHESTRATEUR_GLOBAL_ACTUALISATION.md`.

## 2026-08-23 — scheduler Hostaway câblé (audit : déjà largement construit, non reconstruit)

Audit initial de la mission « scheduler Hostaway 5h » : `ordonnanceur_service.py` (cadence 5h
réservations/payouts, 24h CleaningTasks, `doit_declencher()` pur/testable, `demarrer()`/`arreter()`
avec minuteur singleton, `tick()` appelant `orchestrateur_service.actualiser()`) et
`hostaway_actualisation_service.py::actualiser()` (point d'entrée unique, manuel et auto)
existaient déjà, avec 16 tests couvrant déjà la quasi-totalité des exigences (cadence, propagation,
concurrence via l'état du dataset, échec récent imposant un palier, H6 séparé, désactivé par
défaut). Rien reconstruit.

Trois manques réels comblés :
- `app/main.py::lifespan` appelle désormais `ordonnanceur_service.demarrer()`/`arreter()` — le
  service existait mais n'était jamais démarré avec l'application.
- `run_history_service` câblé dans `hostaway_actualisation_service.actualiser()`, uniquement sur
  le chemin synchrone (`attendre=True`, celui de l'orchestrateur/ordonnanceur) — seul chemin où
  l'issue réelle est connue avant la réponse.
- Cadences rendues configurables (`cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS`/
  `cfg.HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS`) au lieu de constantes codées en dur.

Aucune sauvegarde `app.db` avant un tick Hostaway de routine — décision explicite alignée sur la
mission (« backup ≠ restauration systématique ») : l'ordonnanceur appelle déjà
`actualiser(cibles=[...])`, qui (mission précédente) ne prend une sauvegarde que sur une
actualisation GLOBALE. Une panne API se traduit par une non-activation du dataset (l'ancien reste
actif), jamais une restauration de base.

Écran `/actualisation` étendu d'un bloc « Scheduler Hostaway » (statut, cadences, dernier état,
prochaine décision) — aucune deuxième page créée.

Tests : 10 nouveaux (`test_scheduler_hostaway_industrialisation.py`) + 1 (écran) +
`test_ordonnanceur.py` adapté (`cadences()` fonction au lieu de dict figé, 16 tests existants
toujours verts). Campagne complète : moteur 345/345 passed, application ~2660 passed (8 shards).
**0 failed.** Détail complet : `SCHEDULER_HOSTAWAY.md`.

## 2026-08-23 — référentiels SQLite administrables (audit : déjà largement construit, non reconstruit)

Audit initial de la mission « référentiels administrables » : `referentiel_admin_service.py`
(historisation, journal `ref_admin_evenements`, garde no-delete, 28 tables catégorisées),
`logements_gestion_service.py` (cycle de vie logement historisé) et `fournisseurs_referentiel_
service.py` (CRUD fournisseurs versionné) existaient déjà, avec l'écran `/administration/
referentiels`. Rien reconstruit.

Cinq manques réels comblés :
- `referentiel_admin_service.transaction()` (nouveau context manager) rend atomiques les séquences
  clôture+ouverture (`archiver`/`reactiver`/`changer_proprietaire`/`changer_taux_commission`) —
  auparavant deux connexions/commits séparés, un échec de la seconde écriture après la première
  déjà committée aurait laissé un logement sans période ouverte. Testé par collision de clé
  primaire volontaire sur la ligne à ouvrir : l'ancienne période reste ouverte après l'échec.
- `ref_couts_standards_menage` promue table historisée (nouveau `couts_menage_gestion_service.py`,
  grain `type_logement_id`) — le moteur (`lot6f_cout_complet_menages.py::date_aware`) la consommait
  déjà de façon historisée en lecture, seule l'écriture manquait de discipline clôture/ouverture ;
  l'écran générique ne peut plus la modifier librement.
- Refus d'une nouvelle période historisée dont le début chevauche une période déjà **close** du
  même grain (au-delà du refus déjà existant sur une période déjà ouverte).
- Désactiver un propriétaire encore rattaché à un logement actif est refusé
  (`V10_PROPRIETAIRE_LOGEMENT_ACTIF`) — réactiver reste toujours libre.
- Lien de navigation ajouté vers `/referentiel-fournisseurs`, jusque-là sans accès menu (collision
  avec l'écran charges `/fournisseurs`).

Aucune migration, aucune règle de calcul métier modifiée. Tests : 17 nouveaux
(`test_referentiels_administration_industrialisation.py`) + `fixtures_referentiel.py` complété
d'un paramètre `couts=`. Campagne complète : moteur 345/345 passed, application 2678 passed
(9 lots, quelques `skipped` pré-existants). **0 failed.** Détail complet :
`REFERENTIELS_ADMIN_SQLITE.md`.

## 2026-08-23 — moteurs métier purs, phase 1 (expérience contrôlée, un seul moteur pilote)

Audit ciblé (commissions, ménages, charges, facturation, trésorerie propriétaires, flux_unifié,
Lot10, Lot11, Lot12) : classement A/B/C, pilote choisi `compte_proprietaire_service.calculer_fifo`
— déjà 100 % pure (aucun accès base), déjà documentée comme telle, déjà couverte par 4 tests
unitaires sans DB, et surtout déjà réutilisée par un domaine métier distinct
(`intervenant_menage_compte_service.py`) via un import direct depuis le service d'un AUTRE domaine
— couplage cross-domaine réel et mesurable.

Extraction : nouveau package `app/moteurs/` (0 dépendance applicative). `calculer_fifo`/
`TOLERANCE` déplacés tels quels vers `app/moteurs/fifo_engine.py` — **0 ligne de logique
modifiée**, parité par construction (même objet Python importé depuis deux points d'entrée).
`compte_proprietaire_service.py` ré-exporte pour compatibilité (les 24 tests existants,
`test_compte_proprietaire_fifo.py`, restent verts sans modification). `intervenant_menage_compte_
service.py` importe désormais depuis le moteur neutre, plus depuis le service propriétaire.

9 nouveaux tests unitaires purs (`test_fifo_engine.py`) : reprennent les scénarios existants +
4 nouveaux cas limites (aucune source, montant exactement égal au dû, source infra-tolérance,
ordre de consommation des sources) + un test structurel vérifiant l'absence de toute dépendance
applicative (`sqlite3`/`fastapi`/`app.config`/`app.db`/`Path(`/`os.environ`) dans le fichier source
du moteur.

Lot10 (1533 lignes) et Lot11 (1461 lignes) classés C — pandas+SQL+calcul massivement imbriqués,
non touchés cette mission, conformément à la règle « expérience contrôlée, pas un chantier
général ». Aucune règle métier modifiée, aucune migration.

Campagne complète : moteur 345/345 passed, application 2661 passed (9 lots, quelques `skipped`
pré-existants). **0 failed.** app.db réelle inchangée (`8e299b935ef1e0d4`). Détail complet :
`MOTEURS_METIER_PURS_PHASE1.md`.

## 2026-08-23 — règles et variables métier historisées (paramètre canapé)

Audit ciblé (commission, assiette, canapé, coûts ménage, groupes de logements, paramètres
généraux) : taux de commission, gestion logement↔propriétaire et coût ménage standard confirmés
déjà historisés et résolus par date, fail-closed (missions précédentes, rien reconstruit). Manque
réel identifié et comblé : le paramètre canapé (seuil de voyageurs / montant) vivait comme deux
colonnes COURANTES sur `ref_logements`, sans période — `lot10_calculer_resultats.py` lisait la
ligne courante sans aucun filtre de date, violation directe du principe « changer le futur sans
réécrire le passé ».

Corrigé : nouvelle table historisée `ref_canape_parametres` (migration additive 0058, grain
`logement_id`, backfill préservant exactement le comportement actuel — écart 0,00€ garanti par
construction), résolveur `lib_ref_history.resolve_canape_parametres` (même forme que les
résolveurs existants, fail-closed), service `canape_gestion_service.py` (clôture+ouverture
atomique, même transaction que les référentiels existants), écran dédié sur l'administration
générique existante. `lot10_calculer_resultats.py` résout désormais le paramètre à la date
économique de la réservation, avec repli explicite sur l'ancien comportement si aucune base SQLite
n'est fournie (zéro régression pour les appelants existants, notamment les tests).

**Piège rencontré et corrigé** : la nouvelle table a d'abord été enregistrée dans
`ref_setup_catalogue.FEUILLES` (comme les autres référentiels historisés) — elle a aussitôt cassé
deux tests d'import réel (`@reel_requis`), car cette table n'a jamais eu d'onglet dans
`REF_Setup.xlsm` et `ref_setup_import_service._lire_classeur` refuse tout onglet déclaré-mais-
absent. Corrigé en la retirant du catalogue Excel et en créant un petit catalogue séparé
(`referentiel_admin_service.TABLES_NATIVES`) pour les tables SQLite natives qui réutilisent le
même CRUD générique sans prétendre venir du classeur.

**Correction de cadrage (Mission 6 bis, 2026-08-24)** : « groupes de logements » n'existe pas — la
vraie règle (périmètre venant de la facture, répartition `repartir_egal`) est documentée dans
l'entrée Mission 6 bis ci-dessous. Assiette de commission et formule canapé : non versionnées
(RULE_ID/VERSION) au moment de cette mission — une seule implémentation existait par canal ;
rendues versionnables en Mission 6 bis (voir plus bas).

Tests : 6 nouveaux (`tests/test_ref_history.py`, résolveur pur), 4 nouveaux (`tests/test_canape_
historise.py`, intégration Lot10 — preuve que la résolution datée s'applique réellement, pas
seulement le résolveur isolé), 15 nouveaux (`test_canape_parametres_historises.py`, cycle de vie
admin + verrou colonnes). Campagne complète : moteur 355/355 passed, application 2702 passed
(10 lots). **0 failed.** app.db réelle inchangée (`8e299b935ef1e0d4`). Détail complet :
`REGLES_METIER_TEMPORELLES.md`.

## 2026-08-24 — Mission 6 bis : versionnement des règles, répartition de charges, ménage interne

Correction de cadrage majeure : « groupes de logements » n'existe pas et n'a jamais été un concept
à construire. Retrouvé et prouvé (`charges_impact_service.py`, préexistant, non modifié) : le
périmètre d'une charge non directement attribuable vient des logements sélectionnés à SA création
(directs et/ou logements actifs d'un propriétaire, résolus au mois de la charge via
`gestion_active_pour_mois`, déjà daté via `ref_gestion_logements_hist`) — répartis à parts égales
(`repartir_egal`, arrondi centime déterministe). Aucun groupe mémorisé nulle part ; chaque
charge/facture porte son propre périmètre. Preuve : `test_repartition_charge_commune_facture.py`
(4 tests — une seconde facture A+D ne touche jamais B/C d'une première facture A+B+C ; recalcul
déterministe ; périmètre via propriétaire reste daté sur la gestion historique).

Versionnement des règles ALGORITHMIQUES (distinct d'une simple variable) : nouvelle table
`ref_regles_versions` (migration additive 0059, `rule_code`+`version`+période) — backfill V1 pour
`ASSIETTE_COMMISSION` (formule Lot10 actuelle par canal), `REGLE_REPARTITION_CHARGE_COMMUNE`
(`repartir_egal`), `CANAPE_FORMULE` (seuil→montant fixe, `lib_canape.py`). Aucune V2 réelle
introduite — capacité prouvée par une V2 de fixture dans les tests, jamais une vraie nouvelle
formule. Résolveur `lib_ref_history.resolve_regle_version` (fail-closed, même forme que les
résolveurs existants). Service `regle_version_gestion_service.py` (clôture+ouverture atomique).

`TAUX_HORAIRE_MENAGE_INTERNE` (`ref_parametres_generaux`, seul paramètre avec un consommateur réel
identifié — `lot6e_gainperte_menages.py`) résolu désormais par date via nouveau
`lib_ref_history.resolve_parametre_general`, réutilisant `DREF` (date de référence déjà établie
dans ce script pour d'autres filtres — pas devinée).

**Bug trouvé et corrigé pendant la campagne** : le backfill de la migration 0059 utilisait
`INSERT INTO` littéral (contrairement au backfill 0058, conditionné par un `SELECT` sur
`ref_logements` qui produit 0 ligne sur base vide) — un rejeu brut de tous les fichiers de migration
sur une connexion séparée (`test_migrations_app3e_validation.py::test_double_application_
concurrente_legere`) violait la contrainte `PRIMARY KEY`. Corrigé en `INSERT OR IGNORE` (migration
non encore committée à ce stade, éditée directement). Revérifié : fresh db + copie de l'app.db
réelle, replay ×2, `integrity_check`/`foreign_key_check` propres.

Campagne complète après correction : moteur **362/362 passed**, application **2717 passed**
(10 lots, `05_APPLICATION/tests/`, 183 fichiers). **0 failed.** app.db réelle inchangée
(`8e299b935ef1e0d4`), REF_Setup inchangé, mode réel OFF, scheduler INACTIF.

Limites assumées : aucun consommateur de production ne lit encore `ref_regles_versions` pour
choisir entre versions réelles (une seule existe) ; invalidation DAG non revalidée pour ce
référentiel ; pas d'impact preview dédié ; pas de bandeau "MODIFICATION RÉTROACTIVE" dédié (le
mécanisme générique clôture/ouverture + refus de chevauchement + journal protège déjà contre
l'écrasement silencieux). Détail complet : `REGLES_METIER_TEMPORELLES.md` §9.

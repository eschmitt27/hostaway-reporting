# PLAN_CONSTRUCTION_APPLICATION_LOCALE.md

> Plan de construction de **l'application locale de pilotage** de la conciergerie.
> Document de cadrage de l'app. Il ne remplace ni `PLAN_CONSTRUCTION.md` (moteur de calcul) ni les règles métier.
> Ordre de priorité inchangé : `REGLES_METIER.md` > `ARCHITECTURE_DONNEES.md` > moteur existant > documents `APPLICATION_LOCALE/`.
> Suivi d'exécution : section **APPLICATION LOCALE** de `ETAT_AVANCEMENT.md` (ne pas dupliquer ce plan là-bas).

---

## 0. Statut du document

| Élément | Valeur |
|---|---|
| Version | v2 — arbitrages Stitch, navigation et stack intégrés (2026-07-01) |
| Date de création | 2026-06-30 |
| Statut global app | **PLAN VALIDÉ — ARBITRAGES STITCH ET NAVIGATION INTÉGRÉS — EN_ATTENTE_DÉMARRAGE_LOT_APP-0** |
| Aucun code écrit | OUI (aucun dossier applicatif, aucune base SQLite, aucun fichier technique créé) |

---

## 1. Principe directeur (validé)

L'application **orchestre, saisit de façon contrôlée, affiche et trace**. Elle **ne recalcule jamais** les règles métier déjà portées par les scripts Python (`02_TRAVAIL/`), `REF_Setup.xlsm`, les fichiers `SAISIE_*.xlsx` et les tables `MASTER_*`.

Conséquences gravées :
- La **vérité de calcul** reste le moteur existant. L'app lit ses sorties et déclenche ses scripts ; elle ne réimplémente aucune règle (commission, payout, net propriétaire, résolution datée propriétaire/taux, flux, contrôles).
- **SQLite = journal applicatif uniquement** : statuts d'écran, brouillons de saisie, traces d'audit, registre des runs, registre des snapshots, miroir des statuts de période. Jamais une 2ᵉ source de calcul. Jamais de synchronisation bidirectionnelle fragile avec Excel.
- L'app n'écrit **jamais** dans une source brute, dans `REF_Setup.xlsm`, ni dans une sortie `MASTER_*`. Seules cibles writables (à partir des lots de saisie) : les fichiers `SAISIE_*.xlsx`, via écriture atomique précédée d'un snapshot.

Règles métier non négociables rappelées (elles ne sont jamais contournées par l'app) :
- Ne jamais modifier une source brute.
- Ne jamais réécrire silencieusement une donnée clôturée (HIST prime — D097).
- Toute dérogation / correction / réouverture / forçage est tracée.
- Ne jamais fusionner ménage interne / ménage externe / tâches Hostaway.
- Ne jamais utiliser Hostaway pour valoriser le coût réel ménage.
- Séparation stricte bloc exploitation / bloc règlement propriétaire (EP1/EP5).
- Clôture pilotée par `REF_Cloture_Mensuelle.statut_mois = CLOTURE` (seul déclencheur — D097).

---

## 2. Arbitrages validés (référence — ne pas rediscuter sans nouvelle décision)

| # | Arbitrage validé |
|---|---|
| A1 | Principe directeur : orchestrer / saisir / afficher / tracer ; jamais recalculer. |
| A2 | Source de vérité = sources métier actuelles. SQLite = journal applicatif seulement. |
| A3 | Stack MVP : Python unique · FastAPI · Jinja2 + HTMX · SQLite · lancement local direct · **sans Docker, sans PostgreSQL, sans Tailwind, sans React, sans Node, sans worker/jobs au Lot 0**. Scripts existants exécutés comme processus contrôlés. CSS pur local (`app.css`). Icônes SVG locales. Police Lot APP-0 : `"Segoe UI", Arial, sans-serif` (Plus Jakarta Sans = amélioration future si fichiers fournis légalement — voir §8.4). Aucune dépendance CDN réseau. |
| A4 | Stitch = référence visuelle **locale** (`00_CADRAGE/APPLICATION_LOCALE/stitch/` — analysé 2026-07-01). Sidebar, hiérarchie, tableaux, filtres, cartes, actions reproduits en **Jinja2 + HTMX + CSS local**. Pas de React, pas de Node, pas de Tailwind CDN, pas de Google Fonts CDN, pas de Material Symbols CDN. Icônes SVG locales ou inline. Police Lot APP-0 : `"Segoe UI", Arial, sans-serif` (Plus Jakarta Sans = amélioration future si fichiers fournis légalement — voir §8.4). Exports Stitch = guides d'adaptation, jamais copiés tels quels. |
| A5 | Périmètre MVP = modules réellement adossés au moteur (cf. §4). V2 = trésorerie avancée, caisse réelle complète, précompta avancée, facturation voyageurs, relances avancées. |
| A6 | « Factures clients » = propriétaire dans le MVP → bloc unique **« Propriétaires & règlements »**. Facturation voyageurs = module V2 distinct. |
| A7 | Fusion « Hostaway & calculs » + « Anomalies source » → écran unique **« Sources & calculs »** (onglets Lancer/historique · Anomalies · Journaux). Exploitation et règlement propriétaire restent strictement séparés. |

---

## 3. Architecture cible validée

```
Poste Windows (navigateur local)
        │  localhost:PORT — lancement python direct (pas de Docker)
Backend Python unique (FastAPI)
   ├── rendu HTML serveur (Jinja2 + HTMX)          ← CSS local (app.css), assets locaux, design Stitch adapté, 1 langage
   ├── SQLite (data/app.db)                          ← journal app : saisies/brouillons, statuts, traces, runs, snapshots
   ├── adapters/  → exécutent 02_TRAVAIL/*.py        ← processus externes, sur action utilisateur, jamais import
   └── readers/   → lisent MASTER_*.xlsx + CSV       ← restitution (jamais recalcul)
        │
Source de vérité INCHANGÉE : REF_Setup.xlsm + SAISIE_*.xlsx + sorties MASTER_*  (lecture seule par l'app, sauf SAISIE_* en écriture atomique aux lots dédiés)
Sauvegardes : snapshot = copie horodatée de fichiers (99_ARCHIVES + registre SQLite)
```

**Racine applicative** : `<APP_ROOT>` = **`05_APPLICATION/`** — décision **D-APP-01 ACTÉE**.

**Séparation des couches** :
- sources brutes immuables + saisies → `01_SOURCES_BRUTES/`
- moteur de calcul → `02_TRAVAIL/`
- données calculées → `MASTER_*.xlsx` + `03_EXPORTS/PowerBI/*.csv`
- application → `<APP_ROOT>/`
- documents/justificatifs → hors source brute
- sauvegardes/snapshots → `99_ARCHIVES/` + `<APP_ROOT>/data/snapshots/` + registre SQLite

---

## 4. Périmètre MVP et feuille de route V2

### 4.1 MVP — modules adossés au moteur existant

| Module MVP | Adossé à |
|---|---|
| Référentiels & logements | `REF_Setup.xlsm` (27 onglets), `REF_Gestion_Logements_Hist`, `REF_Taux_Commission`, `lib_ref_history`, `lib_parc` |
| Réservations hors Hostaway | `SAISIE_ReservationsHorsHostaway` → lot4bis/4ter/4quater, `MASTER_CALC_Reservations_Resolues`, `HIST_Reservations_Cloturees` |
| Charges, factures fournisseurs & suivi fournisseur | `SAISIE_Charges_Flux` → `MASTER_FACT_MAN_Charges`, lot3, lot9 |
| Ménages | lot6a/6b/6d/6e/6f, `MASTER_CTRL_Rapprochement_Menages`, `MASTER_CALC_GainPerte/CoutComplet_Menages`, `run_menages_pipeline` |
| Banques & caisse | lot8a/8b/8c, `BANQUE_LOT8_IMPORT`, `REF_Banque_Regles`. Périmètre MVP : import relevé, classement, rapprochement tracé, anti-doublon ; lecture des mouvements de caisse existants si données disponibles. **Exclut du MVP** : saisie complète de caisse réelle, comptage physique, écart théorique/constaté → V2. Pas de faux écran caisse vide. |
| Propriétaires & règlements (relevés, factures propriétaires, acomptes/ajustements, reversements, suivi paiements) | lot12 `MASTER_FACT_Proprietaires`, lot10 `MASTER_CALC_NetProprietaire`, `SAISIE_Acomptes`, `SAISIE_AirCover`, `SAISIE_ImputationsAirbnb`, `SAISIE_Ajustements_PostCloture`, `lib_settlements` |
| Sources & calculs Hostaway | lot1, `MASTER_RUN_Log`, `MASTER_CTRL_HA_Anomalies` |
| Contrôles, anomalies & clôture | lot11 `MASTER_CTRL_Coherence`, `lib_cloture`, `REF_Cloture_Mensuelle` |
| Sauvegardes minimales | `99_ARCHIVES/` + `snapshot_registry` (SQLite) |

### 4.2 V2 — conservés dans la feuille de route (pas de table métier, pas d'écran vide, pas de logique active)

| Module V2 | Point d'extension prévu (sans implémentation) |
|---|---|
| Prévision de trésorerie avancée (13 semaines) | Écran réservé sous « Banque & trésorerie » ; lira charges échéances + reversements + soldes. Aucune table créée au MVP. |
| Caisse réelle complète (comptage physique, écarts théorique/constaté, gestion complète) | Caisse théorique lisible via données existantes si présentes. Extension V2 : table `cash_movements`, saisie physique, écart constaté. Non créée au MVP. Aucun écran vide caisse dans le MVP. |
| Précomptabilité avancée | Statut `prise_en_compta` déjà porté par les saisies. Écran de transmission/lots = V2. |
| Facturation voyageurs | Module distinct V2 (le client MVP = propriétaire — A6). Aucun couplage avec le net propriétaire. |
| Relances avancées | Surcouche sur le suivi des paiements MVP. |
| Calendrier opérationnel | Idée future **non cadrée** — aucun lot, aucune date, aucun point d'extension obligatoire. Aucune table, route, écran, logique ou dépendance créée au MVP ni en V2 planifiée. |

Règle V2 : ces modules existent dans la **feuille de route** ; **aucune table métier, aucun écran fonctionnel, aucune logique active, aucun écran « à venir »** n'est construit tant qu'ils ne sont pas planifiés et validés.

**Calendrier** : retiré du MVP et hors V2 planifiée. Pas d'écran placeholder, pas de sous-menu, pas de route, pas de table, pas de dépendance. Idée future non cadrée sans engagement de développement.

---

## 5. Découpage complet par lots

> Convention commune à tous les lots : DoD = code + migrations éventuelles + tests + doc de lancement + liste des opérations catalogue couvertes/non couvertes + démo données fictives séparées du réel + commit propre (jamais `origin/main` — RUNBOOK §14). Principe transverse : orchestrer/afficher, jamais recalculer. Aucun lot ne démarre sur une hypothèse non validée.

### Lot APP-0 — Socle technique
- **Objectif** : squelette FastAPI + Jinja2/HTMX + SQLite + adaptateurs lecture/exécution + snapshot + écran de lancement, sans calcul métier ni modification de données réelles.
- **Dépendances** : décisions D-APP-01/02/03 (§9).
- **Fichiers touchés** : création de `<APP_ROOT>/` (cf. §6). Aucune modification de `01_SOURCES_BRUTES/`, `02_TRAVAIL/`, `REF_Setup.xlsm`, `MASTER_*`.
- **Tables** : SQLite journal (7 tables — §6.3). Aucune table métier.
- **Scripts moteur concernés** : lus via `pipeline_registry` ; exécution réelle **interdite** au Lot 0 (dry-run only).
- **Tests** : boot, garantie lecture seule, absence de calcul métier, migrations idempotentes, snapshot, runner dry-run, effets de bord à l'import.
- **Critère d'entrée** : arbitrages A1-A7 validés + D-APP-01 tranché.
- **Critère de sortie** : cf. §6.9 (DoD Lot 0).

### Lot APP-1 — Référentiels & navigation
- **Objectif** : shell de navigation complet + Accueil + écrans Référentiels (acteurs, logements, paramètres) en lecture/contrôle ; saisie référentiel maîtrisée.
- **Dépendances** : APP-0.
- **Fichiers touchés** : `<APP_ROOT>/app/routes/`, `templates/`, lecteurs `REF_*` ; (selon A2) écriture éventuelle vers REF via adaptateur dédié à arbitrer.
- **Tables** : SQLite (`screen_states`, `drafts`). Aucune table métier.
- **Scripts/sources** : `REF_Setup.xlsm`, `REF_Gestion_Logements_Hist`, `REF_Taux_Commission`, `lib_ref_history`, `lib_parc`.
- **Tests** : propriétaire sans logement ; alerte reversement sans RIB ; taux/coût **dérivé daté** (jamais ressaisi) ; archivage sans perte d'historique ; `STATUT_PARC_INVALIDE`.
- **Critère d'entrée** : APP-0 validé ; source unique tranchée sur les 3 doublons (D-APP-04).
- **Critère de sortie** : référentiels consultables/gérables sans ouvrir Excel ; intégration visuelle Stitch de la sidebar/cartes/tableaux.

### Lot APP-2 — Réservations hors Hostaway & Ménages
- **Objectif** : saisie HH (préremplissage logement→propriétaire) ; rapprochement ménages (attendu/réalisé HA/déclaré interne/facturé externe + écart) ; relance matching ; outrepassage motivé tracé.
- **Dépendances** : APP-0, APP-1.
- **Fichiers touchés** : écriture atomique `SAISIE_ReservationsHorsHostaway.xlsx` ; lecture `MASTER_CTRL_Rapprochement_Menages`, `MASTER_CALC_GainPerte/CoutComplet_Menages` ; déclenchement `run_menages_pipeline`.
- **Tests** : pas de doublon au 2ᵉ clic/import ; matching relançable ; **3 flux ménage jamais fusionnés** ; Hostaway jamais valorisation ménage ; outrepassage = motif+date+trace.
- **Critère d'entrée** : APP-1 validé ; contrat d'écriture SAISIE_* validé (D-APP-05).
- **Critère de sortie** : une réservation HH saisie ressort correctement dans la résolution ; un écart ménage est lisible et outrepassable avec trace.

### Lot APP-3 — Charges, fournisseurs & Propriétaires & règlements
- **Objectif** : saisie charges/fournisseurs (ventilation, code impact, refacturable) ; bloc « Propriétaires & règlements » (relevés/préfactures, factures propriétaires, acomptes/ajustements, reversements, suivi paiements).
- **Dépendances** : APP-0, APP-1, APP-2.
- **Fichiers touchés** : écriture atomique `SAISIE_Charges_Flux`, `SAISIE_Acomptes`, `SAISIE_AirCover`, `SAISIE_ImputationsAirbnb`, `SAISIE_Ajustements_PostCloture` ; lecture `MASTER_FACT_Proprietaires`, `MASTER_CALC_NetProprietaire`.
- **Tests** : facture non modifiable silencieusement ; `revenu_net_exploitation` jamais touché par acompte/charge exceptionnelle (EP1/EP6/EP7) ; ajustement justifié+lié ; incident voyageur sans `reservation_id` bloqué.
- **Critère d'entrée** : APP-2 validé.
- **Critère de sortie** : un relevé propriétaire s'affiche avec exploitation et règlement strictement séparés ; un ajustement structuré est tracé.

### Lot APP-4 — Banque & rapprochement
- **Objectif** : import bancaire (lecture lot8), classement, rapprochement tracé, détection doublons ; reste sans caisse réelle ni trésorerie (V2).
- **Dépendances** : APP-0, APP-1, APP-3.
- **Fichiers touchés** : lecture `BANQUE_LOT8_IMPORT`, `REF_Banque_Regles` ; déclenchement lot8a/8b/8c.
- **Tests** : ligne bancaire ne crée pas charge sans validation ; dédoublonnage ; pas de double comptage Hostaway/banque (`BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY`) ; virements jamais classés automatiquement.
- **Critère d'entrée** : APP-3 validé.
- **Critère de sortie** : un relevé importé est rapprochable sans doublon, traces complètes.

### Lot APP-5 — Contrôles, clôture & sauvegardes
- **Objectif** : centre « À traiter » ; écran « Sources & calculs » complété (exécution réelle snapshot-protégée) ; clôture multi-statuts ; réouverture motivée ; corrections post-clôture ; snapshots branchés ; restauration en copie de travail.
- **Dépendances** : APP-0 → APP-4.
- **Fichiers touchés** : lecture `MASTER_CTRL_Coherence`, `lib_cloture`, `REF_Cloture_Mensuelle` ; SQLite (`snapshots`, `audit_events`, `periods`).
- **Tests** : mois clôturé protégé ; réouverture = motif+snapshot+révision ; restauration en copie seulement ; modification source post-clôture détectée ; clôture impossible si ligne bancaire non classée.
- **Critère d'entrée** : APP-4 validé ; D-APP-03 (exécution réelle depuis l'app) tranché.
- **Critère de sortie** : un mois se clôture/réouvre avec traces et snapshots conformes ; `REEL = COMPTABLE + HORS_COMPTA` préservé.

### Lot APP-6 — Intégrations & robustesse
- **Objectif** : adaptateur Hostaway complet, anomalies source corrigeables→relance, exports, non-régression métier (réutilise `tests/` + `run_regression_pipeline`), sauvegarde/restauration vérifiées, documentation complète.
- **Dépendances** : APP-0 → APP-5.
- **Fichiers touchés** : lecture `MASTER_RUN_Log`, `MASTER_CTRL_HA_Anomalies` ; orchestration `02_TRAVAIL/`.
- **Tests** : scripts déclenchés sur action seulement ; suite pytest projet verte ; aucun effet de bord à l'import.
- **Critère d'entrée** : APP-5 validé.
- **Critère de sortie** : pipeline orchestrable depuis l'app sans régression ; doc de lancement et d'exploitation complètes.

---

## 6. Lot APP-0 — détail (à coder seulement après validation)

### 6.1 Architecture de dossiers
```
05_APPLICATION/                     (D-APP-01 ACTÉE)
├── README.md
├── requirements.txt                fastapi, uvicorn[standard], jinja2, python-multipart, openpyxl
├── .env.example                    PORT, PROJET_ROOT, chemins sources, READ_ONLY=1
├── .gitignore                      data/app.db, data/snapshots/, .env, __pycache__/
├── run_app.py                      point d'entrée (uvicorn, ouvre localhost:PORT)
├── app/
│   ├── main.py  config.py
│   ├── db/        connection.py · migrations/0001_init.sql
│   ├── adapters/  pipeline_registry.py · pipeline_runner.py
│   ├── readers/   excel_reader.py · csv_reader.py · run_log_reader.py
│   ├── writers/   saisie_writer.py        (STUB — contrat, inactif au Lot 0)
│   ├── services/  snapshot_service.py · audit_service.py · file_registry.py
│   ├── routes/    home.py · sources_calculs.py · health.py
│   ├── templates/ base.html · partials/ · home.html · sources_calculs.html
│   └── static/    css/app.css · js/htmx.X.X.X.min.js (local, versionné) · img/logo-main.png · icons/ (SVG)
├── data/          app.db · snapshots/      (gitignored)
├── seeds/demo/                              (jeux fictifs, séparés du réel)
├── docs/          LANCEMENT_LOCAL.md · CARTE_FLUX_DONNEES.md · CONTRAT_FICHIERS.md
└── tests/         test_boot · test_readonly_guarantee · test_no_metier_calc ·
                   test_sqlite_migrations · test_snapshot · test_pipeline_runner_dryrun
```
Règle d'isolement : l'app n'écrit qu'au sein de `<APP_ROOT>/data/`.

### 6.2 Fichiers précis à créer
Cf. arborescence §6.1. Livrables documentaires obligatoires : `docs/CARTE_FLUX_DONNEES.md` (importé/généré/saisi) et `docs/CONTRAT_FICHIERS.md` (chemins + writable/read-only).

### 6.3 Modèle SQLite minimal (journal — aucune valeur métier autoritaire)
Tables : `schema_migrations`, `audit_events`, `pipeline_runs`, `snapshots`, `screen_states`, `drafts`, `periods` (miroir applicatif ; la vérité de clôture reste `REF_Cloture_Mensuelle` — D097). Détail des colonnes figé au moment du build, conforme à la spécification de cadrage.

### 6.4 Conventions de lecture / écriture Excel
- **Lecture** (seule capacité active au Lot 0) : openpyxl `read_only=True`, jamais de handle d'écriture ; encodage utf-8 + repli cp1252 ; date de référence métier = check-in ; affichage tel quel, aucune transformation.
- **Écriture** (contrat défini, activé aux lots de saisie) : uniquement `SAISIE_*.xlsx` ; écriture atomique via fichier temporaire + validation de schéma + snapshot préalable + remplacement atomique ; cellules texte commençant par `=` préfixées par `'` ; `file_registry` refuse toute écriture vers un chemin read-only.
- **Synchronisation** : unidirectionnelle (Excel → app). SQLite ne réinjecte jamais dans Excel.

### 6.5 Écran de lancement minimal
Layout `base.html` unique : sidebar fixe **9 menus MVP** (Accueil · Logements · Propriétaires & règlements · Réservations · Fournisseurs · Banques & caisse · Ménages · Sources & calculs · Contrôles & clôture) + header (recherche globale placeholder, période active, indicateur Hostaway), sans profil, **sans menu Calendrier**. Logo `logo-main.png` en haut à gauche + texte « CHOUETTE » / « PATRIMOINE » sur deux lignes. Item actif : fond couleur primaire teal + bordure gauche — jamais violet. Sidebar et header identiques sur toutes les pages. Accueil : 5 indicateurs read-only (« — » si source absente), bloc « À traiter » lu dans `MASTER_CTRL_Coherence`, cartes des modules MVP (Disponible/À venir). Écran « Sources & calculs » : onglets Lancer/historique (boutons bridés en dry-run au Lot 0), Anomalies (lecture), Journaux (lecture). Page `/health` : diagnostic chemins/lecture seule/SQLite.

### 6.6 Tests
boot ; garantie lecture seule ; absence de calcul métier (scan du code app) ; migrations idempotentes ; snapshot (copie+manifeste vérifié, restauration en copie) ; runner dry-run (refuse exécution réelle sans garde) ; effets de bord à l'import nuls.

### 6.7 Risques (Lot 0)
Lancer un vrai script depuis le socle (→ dry-run + garde) ; SQLite devenant 2ᵉ source (→ test anti-calcul) ; écrasement source (→ file_registry + écriture atomique) ; encodage/mojibake (→ repli) ; perf openpyxl gros xlsx (→ read_only) ; dérive de chemins (→ config + /health) ; git (ne jamais committer `app.db`/snapshots/`.env` ; jamais `origin/main`) ; collision `01_APPLICATION` (→ `05_APPLICATION`).

### 6.8 Critères d'entrée
Arbitrages A1-A7 validés + D-APP-01 tranché.

### 6.9 Critères de sortie (DoD Lot 0)
App se lance via `python run_app.py` sans Docker ; lit en read-only ≥1 CSV + ≥1 onglet REF + `MASTER_RUN_Log` et affiche sans recalcul ; 0 écriture sur source/MASTER/REF (prouvé) ; SQLite journal + migrations idempotentes ; snapshot manuel vérifié + restauration en copie ; runner testé en dry-run avec garde ; `docs/CARTE_FLUX_DONNEES.md` + `CONTRAT_FICHIERS.md` produits ; pytest vert ; aucune donnée réelle modifiée ; **CSS design tokens produits + logo local intégré + police fallback `"Segoe UI", Arial, sans-serif` (pas de woff2 au Lot 0) + icônes SVG locales + HTMX local versionné + aucune dépendance CDN réseau active** ; sidebar 9 menus conforme à la navigation MVP (sans Calendrier) ; commit propre (jamais `origin/main`).

---

## 7. Règles de sauvegarde, restauration, clôture, réouverture, corrections post-clôture

### 7.1 Sauvegarde / snapshot
- Snapshot = copie horodatée d'un scope défini → `<APP_ROOT>/data/snapshots/<ts>_<type>/` + manifeste (liste + sha256) + ligne `snapshots` (vérifiée par re-hash).
- Types : `MANUEL`, `PERIODIQUE` (14 j), `AVANT_CLOTURE`, `APRES_CLOTURE`, `AVANT_REOUVERTURE`.
- Lot 0 : primitive + déclenchement manuel. Périodique 14 j et déclenchements clôture/réouverture : branchés au Lot APP-5 (pas de scheduler — A3 « sans worker »).
- Scope par défaut à confirmer (D-APP-02).

### 7.2 Restauration
Restauration **uniquement en copie de travail isolée** (`data/restore_workspace/`), jamais écrasement des sources actives (cahier §4.9). Comparaison avant toute réintégration validée par l'utilisateur. Flux complet : Lot APP-5.

### 7.3 Clôture
Statuts app : `OUVERT / EN_CONTROLE / PRET_A_CLOTURER / CLOTURE / REOUVERT`. `CLOTURE` réel = écriture `REF_Cloture_Mensuelle.statut_mois = CLOTURE` (seul déclencheur HIST — D097). Pré-requis : 0 BLOQUANT, 0 ligne bancaire non classée, A_CONTROLER traités/justifiés, pas de `SOURCE_SHEET_CACHE_UTILISE` ouvert.

### 7.4 Réouverture tracée
Réouverture = motif obligatoire + snapshot `AVANT_REOUVERTURE` + nouvelle révision (`period_closure_revisions` / `audit_events`) + statut `REOUVERT`. Jamais directe sur un mois `CLOTURE`.

### 7.5 Corrections post-clôture
Mois clôturé non modifiable directement → ligne d'ajustement post-clôture tracée (`SAISIE_Ajustements_PostCloture.xlsx`, déjà présent) **ou** réouverture tracée. Toute différence live/HIST sur mois clôturé = alerte/ligne d'ajustement, jamais écrasement silencieux (D097).

### 7.6 Bascule société
Aligné sur **D-LOT-PROD-01** : aucune purge sans backup/snapshot vérifié + validation humaine explicite ; jamais mélanger données bancaires/comptables ancienne et nouvelle structure. L'app n'effectue aucune purge ; elle prépare et trace.

---

## 8. Intégration des références visuelles Stitch

### 8.1 Statut et rôle de Stitch
Dossier analysé le 2026-07-01 : `00_CADRAGE/APPLICATION_LOCALE/stitch/` (12 écrans HTML+PNG + 2 DESIGN.md + 1 logo).

Stitch = **référence visuelle et ergonomique locale uniquement** (A4). Ne définit ni règle métier, ni structure de données, ni calcul, ni navigation au-delà du présent plan. Les exports HTML/PNG sont des **guides d'adaptation** pour Jinja2+HTMX+CSS. Jamais copiés tels quels. MCP Stitch non opérationnel (auth KO) — intégration **manuelle** depuis les exports locaux. MCP = accélérateur optionnel, jamais pré-requis d'un lot. (D-APP-06 ACTÉE)

### 8.2 Écrans disponibles et lot d'intégration

| Écran Stitch | Lot d'intégration |
|---|---|
| Accueil | APP-0 (squelette) + APP-1 (KPIs réels) |
| Référence états fonctionnels (vide/erreur/chargement/confirmation/V2) | APP-0 |
| Référence fiche détail générique (onglets/layout) | APP-1 (gabarit fiche) |
| Logements | APP-1 |
| Propriétaires & règlements | APP-3 |
| Réservations | APP-2 |
| Fournisseurs | APP-3 |
| Banques & caisse | APP-4 |
| Ménages | APP-2 |
| Sources & calculs | APP-0 (dry-run) + APP-5 (complet) |
| Contrôles & clôture | APP-5 |

Écrans absents de Stitch (à créer ex nihilo) : fiche propriétaire détail, fiche logement détail, formulaire réservation HH, fiche facture fournisseur, rapprochement bancaire ligne par ligne, relevé propriétaire complet.

### 8.3 Design system — tokens à transcrire dans `app.css`

Marque : **Chouette Patrimoine**. Police cible design : **Plus Jakarta Sans** (non fournie — voir §8.4). Police effective Lot APP-0 : `"Segoe UI", Arial, sans-serif`.

**Tokens couleur principaux (source : `stitch/DESIGN.md`) :**
```
--color-primary: #003441
--color-on-primary: #ffffff
--color-surface: #f9f9ff
--color-on-surface: #101b30
--color-secondary: #5b5f61
--color-on-surface-variant: #40484b
--color-outline-variant: #c0c8cb
--color-outline: #70787c
--color-error: #ba1a1a
--color-error-container: #ffdad6
--color-on-error-container: #93000a
--color-surface-container-low: #f1f3ff
--color-surface-container: #e8edff
--color-surface-container-high: #e0e8ff
--color-inverse-surface: #263046
--color-inverse-on-surface: #edf0ff
```

**Tokens sémantiques supplémentaires (absents de DESIGN.md — ajoutés au plan) :**
```
--color-success: #137333
--color-success-bg: #e6f4ea
--color-warning: #92400E
--color-warning-bg: #FEF3C7
--color-info: #1E40AF
--color-info-bg: #DBEAFE
```

**Espacement :** sidebar 260px · gutter 24px · container-padding 32px · stack-sm 8px · stack-md 16px · stack-lg 32px (rythme 8px strict).

**Radius :** cards 8px · inputs 4px · boutons 8px.

### 8.4 Règles de code impératives (applicables dès APP-0, jamais dérogeables)

**Logo et identité**
- Utiliser `static/img/logo-main.png` (PNG local, fond transparent).
- Afficher en haut à gauche de la sidebar, suivi du texte « CHOUETTE » / « PATRIMOINE » (deux lignes, gras, `--color-primary`).
- Jamais l'URL Google externe (`lh3.googleusercontent.com/aida-public/…`) présente dans les exports Stitch.
- Jamais de génération d'un autre logo ni d'icône de substitution au PNG.
- Jamais afficher « conciergerie de luxe ».

**Police**
- **Lot APP-0** : `font-family: "Segoe UI", Arial, sans-serif;` — fallback système uniquement.
- **Amélioration future uniquement** : Plus Jakarta Sans activable uniquement si fichiers woff2 400/500/600/700 fournis explicitement et légalement dans le projet. Aucun dossier `static/fonts/` créé, aucun fichier woff2 créé, aucun téléchargement, aucune récupération réseau.
- Pas de Google Fonts CDN — ni au Lot 0, ni ensuite.

**Icônes**
- SVG locales dans `static/icons/` ou inline dans les templates.
- Pas de Material Symbols CDN ni de font-icon externe.

**Framework CSS**
- Pas de Tailwind (ni CDN, ni build npm).
- Pas de framework CSS externe.
- CSS pur dans `static/css/app.css` (tokens → layout → sidebar → header → cards → tableaux → boutons → statuts → états fonctionnels).

**HTMX**
- Fichier local versionné : `static/js/htmx.X.X.X.min.js` (version figée au moment du build APP-0).
- Aucun CDN HTMX. `<script src="/static/js/htmx.X.X.X.min.js">` uniquement.
- Version choisie et provenance documentées dans `docs/LANCEMENT_LOCAL.md` (lien release officielle, hash vérifié).
- Mise à jour manuelle uniquement, sur décision explicite.

**Sidebar (navigation MVP)**
- Un `base.html` unique — sidebar et header identiques sur toutes les pages.
- **9 menus dans l'ordre obligatoire** : Accueil · Logements · Propriétaires & règlements · Réservations · Fournisseurs · Banques & caisse · Ménages · Sources & calculs · Contrôles & clôture.
- Aucun menu Calendrier.
- Item actif : fond `--color-primary` teinté + `border-left: 4px solid var(--color-primary)`. **Jamais `#8b5cf6`** (violet incohérent dans un export Stitch — bug à ne pas reproduire).
- Pas d'animation `scale-95 active:scale-100` (bug export Accueil Stitch).

**Header**
- Toujours visible. Hauteur 64px fixe.
- Recherche globale · sélecteur période · notifications · paramètres. Pas de profil utilisateur.

### 8.5 Adaptations HTMX (composants Stitch statiques → interactifs)

| Composant Stitch (statique) | Implémentation HTMX |
|---|---|
| Tabs (Synthèse/Détails/…) | `hx-get` + `hx-target="#tab-content"` |
| Filtres dropdowns | `<form hx-get hx-trigger="change">` |
| Pagination | liens `hx-get?page=N hx-target="#table"` |
| Sélecteur période header | `<select hx-trigger="change">` ou cookie serveur |
| Modale de confirmation | `hx-get="/dialog" hx-target="#modal-slot"` |
| Console logs / traitements | polling `hx-trigger="every 3s"` sur endpoint tail |
| Spinner traitement | `htmx:beforeRequest` / `htmx:afterRequest` |
| Badges sidebar (anomalies) | comptage depuis SQLite à chaque rendu layout |
| Actions avancées (clôture, réouverture…) | `<details>` ou HTMX swap — jamais bouton primaire |

### 8.6 Corrections Stitch à appliquer dans le code (pas dans Stitch)

| Code | Correction |
|---|---|
| C-01 | Couleur active sidebar → `--color-primary` partout |
| C-02 | Tokens succès/warning/info → définis §8.3 |
| C-03 | Logo → `static/img/logo-main.png` local |
| C-04 | Supprimer `scale-95 active:scale-100` du header |
| C-05 | « Synchronisation Hostaway » → « Dernier run pipeline » + lecture journal SQLite |
| C-06 | Clôture → guard obligatoire (snapshot + confirmation × 2 + `REF_Cloture_Mensuelle = CLOTURE`) |
| C-07 | Progression clôture → 9 blocs (plan prévaut sur 4 blocs Stitch) |
| C-08 | Bouton « Recalculer » ménages → appel pipeline avec snapshot + confirmation (D-APP-03 ACTÉE) |

---

## 9. Risques connus, arbitrages restants, décisions humaines

### 9.1 Risques connus
- Dérive entre journal SQLite et moteur Excel si une donnée métier autoritaire est stockée en base → interdit, testé.
- Recodage masqué d'une règle métier dans le front → interdit, testé (scan).
- Double source si l'app ressaisit une valeur dérivée (taux, propriétaire) → toujours dériver de `REF_Taux_Commission` / `REF_Gestion_Logements_Hist`.
- Écrasement d'une sortie ou d'un mois clôturé → file_registry read-only + écriture atomique + snapshot + HIST figé.
- Documentation moteur périmée (`ETAT_AVANCEMENT.md` pipeline, `PLAN_CONSTRUCTION.md`) → à mettre à jour côté moteur, hors périmètre app.

### 9.2 Décisions humaines (track application)
| Code | Décision | Statut |
|---|---|---|
| D-APP-01 | Nom du dossier racine | **ACTÉE — `05_APPLICATION/`** |
| D-APP-02 | Scope snapshot par défaut | **ACTÉE** — périmètre défini §7.1 |
| D-APP-03 | Exécution scripts depuis l'app | **ACTÉE** — dry-run Lot 0 ; exécution réelle avec snapshot+confirmation à partir APP-5 |
| D-APP-04 | Source unique sur 3 doublons (gestion logement / taux / clôture mensuelle) | **OUVERTE** (pré-requis APP-1) |
| D-APP-05 | Contrat écriture `SAISIE_*.xlsx` | **OUVERTE** (pré-requis APP-2) |
| D-APP-06 | Stitch MCP / intégration | **ACTÉE** — intégration manuelle depuis exports locaux ; MCP optionnel non bloquant |
| D-STITCH-01 | Calendrier MVP ou V2 | **ACTÉE** — retiré du MVP et hors V2 planifiée ; idée future non cadrée |
| D-STITCH-02 | Fonts : CDN ou fichiers locaux | **ACTÉE** — Lot APP-0 : `"Segoe UI", Arial, sans-serif` (fallback système). Plus Jakarta Sans = amélioration future uniquement si fichiers fournis légalement. Aucun `static/fonts/`, aucun woff2, aucun CDN. |
| D-STITCH-03 | Tailwind CDN ou CSS pur | **ACTÉE** — pas de CDN, pas de Tailwind ; CSS pur `app.css` |
| D-STITCH-04 | Couleurs sémantiques absentes de DESIGN.md | **ACTÉE** — tokens définis §8.3 |
| D-STITCH-05 | Logo sidebar | **ACTÉE** — `logo-main.png` local + texte « CHOUETTE » / « PATRIMOINE » |

### 9.3 Arbitrages et décisions déjà tranchés
A1-A7 (§2) — VALIDÉS.
D-APP-01/02/03/06 — ACTÉES.
D-STITCH-01/02/03/04/05 — ACTÉES.

---

## 10. Suivi d'exécution

Le suivi lot par lot est tenu dans la section **APPLICATION LOCALE** de `ETAT_AVANCEMENT.md`. Ce plan n'est pas dupliqué là-bas. Règle : après chaque lot démarré/modifié/testé/validé/bloqué/terminé, mettre à jour l'état (date, statut, lot, fichiers, tests, résultat, anomalies/risques, décisions attendues, prochaine action). Aucun lot marqué FAIT sans les contrôles/tests/preuves prévus par le cadrage.

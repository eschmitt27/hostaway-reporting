"""
lot6b_m04_menages_internes.py — Alimentation DÉTERMINISTE de M04 (sans Power Query)
================================================================================
Remplace la dépendance Power Query : le MASTER M04 est reconstruit DIRECTEMENT
par ce script à chaque run, depuis la Google Sheet "Suivi ménage".

D027 (M04 = MO interne HC, TYPE_FLUX_013) conservée. D106 (refonte ménages).

URL CSV : lue depuis SQLite `ref_sources_systeme` (SRC_011, nom_source =
          GOOGLE_SHEET_M04_DECLARATIONS, actif=OUI). PAS d'URL en dur, PAS de lecture Excel au
          runtime : fail-closed si la configuration SQLite manque. Repli classeur uniquement sur
          `--url-depuis-excel` (reprise legacy explicite).

SORTIE CANONIQUE : `menages_declarations_internes` (SQLite). C'est la SEULE sortie de ce script —
aucun classeur n'est écrit, en aucune circonstance.

EXPORT LEGACY SUPPRIMÉ (mission « lot6c vers SQLite » §9, une fois lot6c doté d'un mode SQLite et
la chaîne bout-en-bout prouvée sans classeur intermédiaire) : ce script écrivait auparavant, sous
`--export-legacy`,
  - 02_TRAVAIL/Lot6b_DeclarationsInternes/MASTER_NORM_Declarations_Internes.xlsx
  - 02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx
      (SOURCE_RAW/MASTER/VUE_ACTIVE) — POWER_QUERY_CODE conservé en documentation/archive, jamais lu.
Seul appelant : la recette de chaîne complète (/menages/chaine), tant que lot6c n'avait aucun mode
SQLite. Ce chemin a disparu avec le code qui l'exécutait — pas seulement désactivé par un flag.
Ces deux classeurs restent lus, gelés à leur dernier contenu, par certains chemins EXCEL non
encore migrés (lot6d/6e en `--source EXCEL`, deux contrôles lot11) — cf. RESTANT_EXCEL_OPERATIONNEL.md.

Contrôles BLOQUANTS : URL absente / inaccessible / structure Google Sheet inattendue.
Ne touche pas : banque, Hostaway, factures, résultats aval (lot9-12).
"""

import sys, os, io, csv, sqlite3, subprocess, hashlib, datetime, unicodedata, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import openpyxl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_db_moteur as dbm

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REF  = os.path.join(ROOT, "01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
NOW = datetime.datetime.now().isoformat(timespec="seconds")

SOURCE_GOOGLE_SHEET_LOT6B = "GOOGLE_SHEET"  # défaut si menages_declarations_extra n'existe pas encore

# ── SORTIE : SQLite, sans exception. ──────────────────────────────────────────────────────────
#
# Le parcours est GOOGLE SHEET -> normalisation Python -> SQLite, sans AUCUNE écriture de classeur
# — plus d'exception `--export-legacy` : lot6c a désormais son propre mode SQLite (mission « lot6c
# vers SQLite »), donc plus aucun appelant n'a besoin que lot6b produise un classeur pour que la
# suite de la chaîne ait quelque chose à lire. `--sans-excel` reste accepté, sans effet : c'est
# devenu le seul comportement possible, et des appelants le passent encore explicitement
# (orchestrateur_moteur, tests). Le retirer les casserait sans rien gagner.
SANS_EXCEL = True


def _arg_valeur(nom):
    """Valeur d'un argument `--nom VALEUR` sur la ligne de commande, ou None si absent."""
    if nom in sys.argv:
        i = sys.argv.index(nom)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None


# `--db <chemin>` — base applicative explicite. MANQUAIT ENTIÈREMENT : les quatre appels à
# `dbm.chemin_db(...)` du script passaient `None` en dur, jamais un argument reçu (ce script
# n'utilise pas `argparse`, contrairement à lot6d/6e/6f — l'option n'avait simplement jamais été
# câblée). Conséquence en production : `orchestrateur_moteur.executer()` passe pourtant `--db
# <chemin>` à CHAQUE appel de ce script, silencieusement ignoré — lot6b ne résolvait sa base QUE
# par variable d'environnement (`PILOTAGE_DB_PATH`/`APP_DATA_DIR`), et ne s'en sortait que parce
# que l'environnement du process héritait la bonne valeur. Un `--db` explicite pointant une base
# différente de l'environnement aurait silencieusement écrit au mauvais endroit ; l'absence des
# deux (cas d'une recette sur copies, sans variable d'environnement positionnée) faisait échouer
# le script avec un message qui semblait dire « aucune base désignée » alors qu'une l'était bel et
# bien, juste jamais lue. Trouvé en testant la chaîne ménages sans classeur intermédiaire.
ARG_DB = _arg_valeur("--db")

MOIS = {"janvier":"01","fevrier":"02","mars":"03","avril":"04","mai":"05","juin":"06","juillet":"07","aout":"08","septembre":"09","octobre":"10","novembre":"11","decembre":"12"}
REQUIRED_COLS = ["Prénom", "Mois des ménages", "Année des ménages", "Appartement"]

def abort(msg):
    print(f"\n[BLOQUANT lot6b] {msg}\nM04 NON modifié.")
    sys.exit(1)

def norm(s):
    s = str(s or "").strip().lower()
    return " ".join("".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").split())

def rh(*v): return hashlib.sha256("|".join("" if x is None else str(x) for x in v).encode()).hexdigest()[:16]
def fnum(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def to_d(v):
    try: return datetime.date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError): return None

def sh(p, s):
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True); ws = wb[s]
    rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]; wb.close()
    return [dict(zip([str(c) for c in rows[0]], r)) for r in rows[1:]]

# ── URL SRC_011 — configuration canonique SQLite, FAIL-CLOSED ────────────────
# `ref_sources_systeme` (SRC_011) est LA source de configuration runtime : la lire en base supprime
# la dernière LECTURE Excel du parcours d'import quotidien.
#
# Pourquoi plus AUCUN repli automatique sur REF_Setup.xlsm : le repli précédent a masqué une vraie
# panne. `dbm.lignes()` trie par défaut sur `ORDER BY id` — colonne inexistante ici, la clé est
# `source_id` — l'`OperationalError` était avalé par un `except Exception` nu, et le parcours
# retombait sur le classeur en affichant « EXCEL_REPLI » comme si c'était le fonctionnement normal.
# SRC_011 était pourtant bien en base. Un repli qui masque la panne qu'il est censé compenser ne
# protège personne : il transforme un bug réparable en dépendance Excel permanente et invisible.
#
# `--url-depuis-excel` reste disponible pour une reprise legacy/diagnostic explicite (base pas
# encore alimentée par l'import du référentiel), jamais pour le parcours quotidien.
URL_DEPUIS_EXCEL = "--url-depuis-excel" in sys.argv


def _url_src011_depuis_sqlite():
    """URL SRC_011 lue dans `ref_sources_systeme`. Rend (url, erreur) — aucune exception avalée."""
    chemin = dbm.chemin_db(ARG_DB)
    if chemin is None:
        return None, "aucune base applicative designee (--db / PILOTAGE_DB_PATH / APP_DATA_DIR)"
    if not chemin.exists():
        return None, "base applicative introuvable : %s" % chemin
    conn = dbm.ouvrir(chemin)
    try:
        if not dbm.table_presente(conn, "ref_sources_systeme"):
            return None, "table `ref_sources_systeme` absente (referentiel jamais importe)"
        # `ordre="source_id"` : clé de cette table, `dbm.lignes()` trierait sinon sur `id` inexistant.
        sources = dbm.lignes(conn, "ref_sources_systeme",
                             ("nom_source", "dossier_source", "actif"), ordre="source_id")
    finally:
        conn.close()
    for d in sources:
        if str(d.get("nom_source")) == "GOOGLE_SHEET_M04_DECLARATIONS" and str(d.get("actif")) == "OUI":
            valeur = str(d.get("dossier_source") or "").strip()
            if not valeur.startswith("http"):
                return None, "SRC_011 present mais `dossier_source` n'est pas une URL : %r" % valeur
            return valeur, ""
    return None, "SRC_011 (GOOGLE_SHEET_M04_DECLARATIONS, actif=OUI) absent de `ref_sources_systeme`"


url, _err_url = _url_src011_depuis_sqlite()
_origine_url = "SQLITE"
if url is None and URL_DEPUIS_EXCEL:
    _origine_url = "EXCEL_LEGACY_EXPLICITE"
    for d in sh(REF, "REF_Sources_Systeme"):
        if str(d.get("nom_source")) == "GOOGLE_SHEET_M04_DECLARATIONS" and str(d.get("actif")) == "OUI":
            url = str(d.get("dossier_source") or "").strip()
if not url or not url.startswith("http"):
    abort("URL GOOGLE_SHEET_M04_DECLARATIONS non resolue depuis SQLite : %s.\n"
          "  Source canonique attendue : table `ref_sources_systeme`, SRC_011, actif=OUI.\n"
          "  Aucun repli Excel automatique (fail-closed) : corriger le referentiel SQLite.\n"
          "  Reprise legacy explicite si necessaire : --url-depuis-excel."
          % (_err_url or "URL vide/invalide"))
print(f"[lot6b] URL SRC_011 lue depuis {_origine_url}")

# ── Fetch CSV via lib fiabilisée (retry + cache 72h traçable, DEF-1) ──────────
from lib_sheet_source import fetch_sheet_csv, begin_step, commit_step
CACHE_DIR = os.path.join(ROOT, "02_DONNEES_NORMALISEES", "menages", "_cache_google_sheet")
# fetch_sheet_csv lève SystemExit("SOURCE_SHEET_INDISPONIBLE") (rc!=0) si réseau KO + pas de cache <=72h
txt, prov = fetch_sheet_csv(url, CACHE_DIR, step="lot6b")
if prov["resolution_source"] == "CACHE":
    print(f"[lot6b] AVERTISSEMENT SOURCE_SHEET_CACHE_UTILISE : cache du {prov['cache_date_extraction_utc']} "
          f"(age {prov['cache_age_h']}h, sha {prov['sha256_csv'][:12]}) — source réseau indisponible. "
          f"Ne pas clôturer le mois tant que ce contrôle est ouvert.")
rows = list(csv.reader(io.StringIO(txt)))
if not rows: abort("CSV vide.")
hdr = rows[0]
missing = [c for c in REQUIRED_COLS if c not in hdr]
if missing: abort(f"Structure Google Sheet inattendue, colonnes manquantes : {missing}")

# ── Référentiels — SQLite canonique, FAIL-CLOSED ─────────────────────────────
# Ce script ne lit plus AUCUN classeur pour son fonctionnement normal : `ref_mapping_logements`,
# `ref_logements` et `ref_intervenants` sont les référentiels canoniques, et ce sont les trois
# seuls dont il a besoin — le bloc M04 legacy qui consommait `logtype`/`logprop`/`loghaid` et les
# barèmes a disparu (mission « lot6c vers SQLite » §9, cf. tête de fichier).
#
# NORMALISATION EXPLICITE (§10) : le classeur rend `hostaway_listing_id` en int (482204) et une
# cellule vide en None ; SQLite rend '482204' et ''. Comparés bruts, ces deux référentiels
# paraissent divergents alors que la donnée est la même. Tout passe donc par `_txt()` : un écart
# de TYPE ne doit jamais casser silencieusement un rattachement Hostaway.
def _txt(v):
    return "" if v is None else str(v).strip()


def _refs_sqlite():
    """Référentiels du parcours opérationnel, lus en SQLite. Fail-closed, jamais de repli Excel."""
    chemin = dbm.chemin_db(ARG_DB)
    if chemin is None:
        abort("Aucune base SQLite designee (PILOTAGE_DB_PATH / APP_DATA_DIR) : "
              "referentiels illisibles. Aucun repli classeur (§12).")
    if not os.path.exists(str(chemin)):
        abort(f"Base SQLite introuvable : {chemin}. Aucun repli classeur (§12).")
    conn = dbm.ouvrir(chemin)
    try:
        for table in ("ref_mapping_logements", "ref_logements", "ref_intervenants"):
            if not dbm.table_presente(conn, table):
                abort(f"Referentiel SQLite absent : {table}. Importer le referentiel avant "
                      "de lancer lot6b. Aucun repli classeur (§12).")
        _lmap = {}
        for vs, lid in conn.execute(
                "SELECT valeur_source, logement_id FROM ref_mapping_logements"):
            if _txt(vs):
                _lmap[norm(vs)] = lid
        _lognom = {}
        for lid, nom in conn.execute(
                "SELECT logement_id, nom_logement_officiel FROM ref_logements"):
            if _txt(lid):
                _lognom[lid] = nom
        _intmap = {}
        for iid, nom, normalise in conn.execute(
                "SELECT intervenant_id, nom_intervenant, nom_normalise FROM ref_intervenants"):
            if _txt(iid) and _txt(normalise):
                _intmap[norm(normalise)] = (iid, nom or iid)
        return _lmap, _lognom, _intmap
    finally:
        conn.close()


lmap, lognom, intmap_sqlite = _refs_sqlite()

# ── Mapping prénom Google Sheet -> intervenant ────────────────────────────────
# Construit DEPUIS REF_Intervenants.nom_normalise (D104) : ni prénom réel ni identifiant ne sont
# codés en dur ici. Corrige ANO-2026-07-28-01 — l'ancien mapping (dict `INTMAP` figé sur trois
# prénoms réels) empêchait tout jeu de données fictif de traverser ce moteur : un intervenant
# fictif n'y était jamais reconnu, et l'agrégation aval (lot6d) plantait sur un intervenant_id nul
# mêlé à des chaînes lors d'un tri. Un référentiel fictif définit ses propres nom_normalise et le
# mapping fonctionne alors identiquement, sans aucune donnée réelle requise.
# Lu en SQLite (`_refs_sqlite`) : même construction, même clé `norm(nom_normalise)`, aucune donnée
# réelle codée en dur. Parité vérifiée avec l'ancienne lecture classeur (5 entrées identiques).
intmap = dict(intmap_sqlite)

# Alias orthographiques Google Sheet (ex. D104 : « Kira » = Kheira). Exception nominative et
# documentée, PAS un mécanisme général : chargée depuis un module optionnel, absent du jeu de
# recette (jamais copié par `recette/build_data_recette.py`), donc sans effet sur un référentiel
# fictif. Son absence ne bloque rien — seule la résolution exacte par nom_normalise reste requise.
try:
    from _data_lot6b_alias_reel import ALIAS_PRENOMS_GOOGLE_SHEET as _alias
except ImportError:
    _alias = {}
for _brut, _canonique in _alias.items():
    _cible = intmap.get(norm(_canonique))
    if _cible:
        intmap[norm(_brut)] = _cible

i_pre = hdr.index("Prénom"); i_mois = hdr.index("Mois des ménages"); i_an = hdr.index("Année des ménages")
appcols = [i for i, h in enumerate(hdr) if h.strip() == "Appartement"]
i_lav_na = next((i for i, h in enumerate(hdr) if h.strip().startswith("Coûts de lavage du linge (hors")), None)

# ── Dépivot ──────────────────────────────────────────────────────────────────
norm_rows = []
for r in rows[1:]:
    if not any(c.strip() for c in r): continue
    pre = r[i_pre].strip(); mm = MOIS.get(norm(r[i_mois])); yr = r[i_an].strip()
    miso = f"{yr}-{mm}" if (mm and yr) else None
    iid, inom = intmap.get(norm(pre), (None, pre))
    lav_na = fnum(r[i_lav_na]) if (i_lav_na is not None and i_lav_na < len(r) and r[i_lav_na].strip()) else 0
    for ci in appcols:
        app = r[ci].strip() if ci < len(r) else ""
        if not app: continue
        nb = int(fnum(r[ci+1]) or 0) if (ci+1 < len(r) and r[ci+1].strip()) else 0
        nh = fnum(r[ci+2]) if (ci+2 < len(r) and r[ci+2].strip()) else None
        lav = fnum(r[ci+3]) if (ci+3 < len(r) and r[ci+3].strip()) else 0
        lid = lmap.get(norm(app))
        statut, code = ("VALIDE", "")
        if lid is None: statut, code = "A_CONTROLER", "LOGEMENT_NON_MAPPE"
        elif iid is None: statut, code = "A_CONTROLER", "INTERVENANT_NON_MAPPE"
        norm_rows.append({"mois": miso, "annee": yr, "mois_saisie": r[i_mois].strip(),
            "appartement_source": app, "nom_appartement": lognom.get(lid), "logement_id": lid,
            "intervenant_source": pre, "intervenant_id": iid, "nom_intervenant": inom, "type_intervenant": "INTERNE",
            "nb_menages": nb, "nb_heures": nh, "cout_lavage_attribue": lav, "lavage_non_attribuable_mois": lav_na,
            "statut_controle": statut, "code_controle": code, "source_url": url, "date_extraction": NOW,
            "ROW_HASH": rh(miso, lid, iid, nb)})

# ── Transaction provenance DEF-1 : marqueur PENDING AVANT remplacement sorties ─
# Tant que ce marqueur existe (et qu'une sortie existe), lot11 refuse de croire RESEAU.
begin_step(CACHE_DIR, "lot6b")

# NCOLS : colonnes de la ligne normalisée — sert de base aux colonnes SQL ci-dessous (`_sql_cols`).
# Ce nom (« 1) MASTER_NORM ») date de l'export classeur du même nom, supprimé (mission « lot6c
# vers SQLite » §9) : plus aucun classeur MASTER_NORM n'est écrit, ici ni ailleurs.
NCOLS = ["mois","annee","mois_saisie","appartement_source","nom_appartement","logement_id",
    "intervenant_source","intervenant_id","nom_intervenant","type_intervenant","nb_menages","nb_heures",
    "cout_lavage_attribue","lavage_non_attribuable_mois","statut_controle","code_controle","source_url","date_extraction","ROW_HASH"]

# ── SQLite : menages_declarations_internes (0038) — SEULE sortie de ce script ────────────────────
#
# RECONCILIATION SHEET <-> APPLICATION (migration 0066, mission "FINALISER LE VRAI WORKFLOW")
# Un DELETE FROM complet écraserait silencieusement toute déclaration créée/modifiée depuis
# l'application (menages_declarations_extra.source == 'APPLICATION') : remplacé par un upsert
# clé par clé (mois, logement_id, intervenant_id). Une ligne APPLICATION dont la valeur Sheet
# diverge n'est JAMAIS réécrite : un conflit est enregistré (menages_declarations_conflits),
# résolu uniquement par un humain via menages_declarations_service.resoudre_conflit().
_db = dbm.chemin_db(ARG_DB)
if _db is None:
    print("[lot6b] Aucune base designee (PILOTAGE_DB_PATH/APP_DATA_DIR) : SQLite non ecrit")
else:
    _sql_cols = [c for c in NCOLS if c != "ROW_HASH"]
    _conn = dbm.ouvrir(_db)
    # `row_factory` OBLIGATOIRE ici : les blocs de réconciliation ci-dessous indexent les lignes par
    # NOM (`r["mois"]`, `dict(r)`). Sans lui, `dbm.ouvrir` rend des tuples et l'accès lève
    # `TypeError: tuple indices must be integers`.
    # Ce défaut était latent et invisible : les compréhensions concernées itèrent sur les lignes
    # DÉJÀ en base, donc leur corps ne s'exécutait pas tant que `menages_declarations_internes`
    # était vide — exactement l'état du tout premier import réel. Toute RÉIMPORTATION (donc toute
    # synchronisation Google Sheet ultérieure, et avec elle toute la détection de conflits
    # Sheet/Application) plantait dès la deuxième exécution.
    _conn.row_factory = sqlite3.Row
    _run_id = os.environ.get("LOT6_RUN_ID", "")
    _nb_ecrites = _nb_conflits = _nb_inchangees = _nb_non_mappees = 0
    _mois_impactes = set()   # mission "recalcul mensuel cible" §7 : mois reellement changes/en
                              # conflit durant CETTE synchro, pour un recalcul cible optionnel —
                              # jamais tout l'historique.
    try:
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS menages_declarations_extra ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, mois TEXT NOT NULL, logement_id TEXT NOT NULL, "
            "intervenant_id TEXT NOT NULL, supplement REAL NOT NULL DEFAULT 0, "
            "justification_supplement TEXT, cout_standard_calcule REAL, cout_final REAL, "
            "source TEXT NOT NULL DEFAULT 'APPLICATION', derniere_valeur_sheet_nb_menages INTEGER, "
            "derniere_synchro_sheet TEXT, derniere_modification_app TEXT, "
            "date_creation TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), "
            "date_modification TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')))"
        )
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS menages_declarations_conflits ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, mois TEXT NOT NULL, logement_id TEXT NOT NULL, "
            "intervenant_id TEXT NOT NULL, champ TEXT NOT NULL DEFAULT 'nb_menages', "
            "valeur_application TEXT, valeur_sheet TEXT, statut TEXT NOT NULL DEFAULT 'OUVERT', "
            "resolu_par TEXT, resolu_le TEXT, "
            "date_detection TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')))"
        )
        # Lignes déjà en base, indexées par clé métier (nb_menages seul suffit au conflit §B2 —
        # les autres champs restent alignés sur la même source).
        _existantes = {
            (r["mois"], r["logement_id"], r["intervenant_id"]): dict(r)
            for r in _conn.execute(
                "SELECT mois, logement_id, intervenant_id, nb_menages FROM menages_declarations_internes"
            ).fetchall()
        }
        _extras = {
            (r["mois"], r["logement_id"], r["intervenant_id"]): dict(r)
            for r in _conn.execute("SELECT * FROM menages_declarations_extra").fetchall()
        }
        for d in norm_rows:
            # `logement_id`/`intervenant_id` non résolus (LOGEMENT_NON_MAPPE / INTERVENANT_NON_MAPPE,
            # cf. `statut_controle`/`code_controle` ci-dessus) : la ligne NE PEUT PAS entrer dans les
            # tables SQLite structurées — `menages_declarations_extra`/`menages_declarations_conflits`
            # portent ces deux colonnes en NOT NULL (ce sont des clés métier, pas des attributs
            # optionnels). Avant cette garde, une ligne non mappée provoquait un `IntegrityError` qui
            # faisait échouer TOUTE la synchronisation SQLite (y compris les lignes valides) : un
            # ÉLÉMENT À TRAITER ne doit jamais bloquer le reste.
            # Visibilité actuelle, LIMITÉE : uniquement le compte agrégé `_nb_non_mappees` ci-dessous
            # (stdout, capturé dans `stdout_tail` par le runner) — plus aucune trace ligne-à-ligne
            # depuis la suppression de MASTER_NORM (mission « lot6c vers SQLite » §9). C'était déjà le
            # cas en production avant cette mission (`--sans-excel` y était déjà le seul mode réel :
            # MASTER_NORM n'y était jamais écrit) ; ce n'est donc pas une régression introduite ici,
            # mais une lacune préexistante qui reste à combler par une vraie file À_TRAITER durable
            # (cf. RESTANT_EXCEL_OPERATIONNEL.md) si des Sheets réelles produisent un jour ce cas.
            if not d.get("logement_id") or not d.get("intervenant_id"):
                _nb_non_mappees += 1
                continue
            cle = (d["mois"], d["logement_id"], d["intervenant_id"])
            extra = _extras.get(cle)
            existante = _existantes.get(cle)
            source_actuelle = (extra or {}).get("source") or SOURCE_GOOGLE_SHEET_LOT6B
            if existante is not None and source_actuelle == "APPLICATION":
                if int(existante.get("nb_menages") or 0) != int(d["nb_menages"] or 0):
                    _conn.execute(
                        "INSERT INTO menages_declarations_conflits "
                        "(mois, logement_id, intervenant_id, champ, valeur_application, "
                        "valeur_sheet, statut) VALUES (?,?,?,?,?,?,'OUVERT')",
                        (cle[0], cle[1], cle[2], "nb_menages",
                         str(existante.get("nb_menages")), str(d["nb_menages"])))
                    _nb_conflits += 1
                    _mois_impactes.add(cle[0])
                else:
                    _nb_inchangees += 1
                _conn.execute(
                    "INSERT INTO menages_declarations_extra (mois, logement_id, intervenant_id, "
                    "source, derniere_valeur_sheet_nb_menages, derniere_synchro_sheet) "
                    "VALUES (?,?,?,'APPLICATION',?,?) "
                    "ON CONFLICT(mois, logement_id, intervenant_id) DO UPDATE SET "
                    "derniere_valeur_sheet_nb_menages=excluded.derniere_valeur_sheet_nb_menages, "
                    "derniere_synchro_sheet=excluded.derniere_synchro_sheet",
                    (cle[0], cle[1], cle[2], d["nb_menages"], NOW))
                continue
            _change = existante is None or int(existante.get("nb_menages") or 0) != int(d["nb_menages"] or 0)
            _conn.execute(
                "DELETE FROM menages_declarations_internes WHERE mois=? AND logement_id=? "
                "AND intervenant_id=?", cle)
            _conn.execute(
                f"INSERT INTO menages_declarations_internes "
                f"({', '.join(_sql_cols)}, row_hash, run_id) VALUES "
                f"({', '.join(['?'] * (len(_sql_cols) + 2))})",
                tuple(d.get(c) for c in _sql_cols) + (d.get("ROW_HASH"), _run_id))
            _conn.execute(
                "INSERT INTO menages_declarations_extra (mois, logement_id, intervenant_id, "
                "source, derniere_valeur_sheet_nb_menages, derniere_synchro_sheet) "
                "VALUES (?,?,?,'GOOGLE_SHEET',?,?) "
                "ON CONFLICT(mois, logement_id, intervenant_id) DO UPDATE SET "
                "source='GOOGLE_SHEET', derniere_valeur_sheet_nb_menages=excluded.derniere_valeur_sheet_nb_menages, "
                "derniere_synchro_sheet=excluded.derniere_synchro_sheet",
                (cle[0], cle[1], cle[2], d["nb_menages"], NOW))
            if _change:
                _nb_ecrites += 1
                _mois_impactes.add(cle[0])
            else:
                _nb_inchangees += 1
        _conn.commit()
    finally:
        _conn.close()
    print(f"[lot6b] SQLite : menages_declarations_internes — {_nb_ecrites} lignes ecrites/mises a jour, "
          f"{_nb_inchangees} inchangees (deja alignees), {_nb_conflits} conflits GOOGLE_SHEET/APPLICATION "
          f"detectes (menages_declarations_conflits, non ecrases), {_nb_non_mappees} ligne(s) non mappee(s) "
          f"(LOGEMENT_NON_MAPPE/INTERVENANT_NON_MAPPE, non ecrites, non tracees au-dela de ce compte)")
    print(f"[lot6b] MOIS_IMPACTES: {','.join(sorted(_mois_impactes)) or 'AUCUN'}")

# ── Fin de run — plus aucun classeur écrit (mission « lot6c vers SQLite » §9) ────────────────────
# Supprimait auparavant, sous `--export-legacy` : le classeur M04 (SOURCE_RAW/MASTER/VUE_ACTIVE,
# avec un `.BAK` avant chaque écriture) et les référentiels historisés (barèmes, propriétaire
# historisé via `ref_gestion_logements_hist`) chargés pour cette seule reconstruction. Ce chemin a
# disparu avec le code qui l'exécutait, pas seulement désactivé par un flag — voir le git log de
# ce fichier pour le code supprimé si une régression legacy devait un jour être investiguée.
nmap = sum(1 for d in norm_rows if d["code_controle"])
print("[lot6b] --sans-excel : classeur M04 NON modifié (aucun .BAK créé). "
      "Sortie canonique = menages_declarations_internes (SQLite).")
print(f"[lot6b] URL REF OK (SRC_011) | CSV {len(rows)-1} lignes | normalisées {len(norm_rows)} | SQLite uniquement")
print(f"[lot6b] lignes A_CONTROLER (mapping) : {nmap}")
commit_step(CACHE_DIR, "lot6b", prov)

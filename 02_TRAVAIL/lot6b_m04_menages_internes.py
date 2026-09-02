"""
lot6b_m04_menages_internes.py — Alimentation DÉTERMINISTE de M04 (sans Power Query)
================================================================================
Remplace la dépendance Power Query : le MASTER M04 est reconstruit DIRECTEMENT
par ce script à chaque run, depuis la Google Sheet "Suivi ménage".

D027 (M04 = MO interne HC, TYPE_FLUX_013) conservée. D106 (refonte ménages).

URL CSV : lue depuis REF_Setup.xlsm > REF_Sources_Systeme
          (nom_source = GOOGLE_SHEET_M04_DECLARATIONS, actif=OUI). PAS d'URL en dur.

Sorties écrites automatiquement :
  - 02_TRAVAIL/Lot6b_DeclarationsInternes/MASTER_NORM_Declarations_Internes.xlsx
  - 02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx :
        SOURCE_RAW (traçabilité), MASTER (calculé), VUE_ACTIVE (VALIDE)
  POWER_QUERY_CODE conservé en documentation/archive, non utilisé.

Contrôles BLOQUANTS : URL absente / inaccessible / structure Google Sheet inattendue.
Ne touche pas : banque, Hostaway, factures, résultats aval (lot9-12).
"""

import sys, os, io, csv, subprocess, shutil, hashlib, datetime, collections, unicodedata, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import openpyxl
from openpyxl.styles import Font, PatternFill
from lib_menage_costs import resolve_internal_cleaning_cost

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_db_moteur as dbm

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REF  = os.path.join(ROOT, "01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
M04  = os.path.join(ROOT, "02_DONNEES_NORMALISEES", "menages", "M04_MENAGES_PowerQuery.xlsx")
NORM_DIR = os.path.join(ROOT, "02_TRAVAIL", "Lot6b_DeclarationsInternes")
NORM_OUT = os.path.join(NORM_DIR, "MASTER_NORM_Declarations_Internes.xlsx")
NOW = datetime.datetime.now().isoformat(timespec="seconds")

SOURCE_GOOGLE_SHEET_LOT6B = "GOOGLE_SHEET"  # défaut si menages_declarations_extra n'existe pas encore

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

def sh_opt(p, s):
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    try:
        if s not in wb.sheetnames:
            return []
        ws = wb[s]
        rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
    finally:
        wb.close()
    if not rows:
        return []
    return [dict(zip([str(c) for c in rows[0]], r)) for r in rows[1:]]

# ── URL depuis REF (bloquant) ────────────────────────────────────────────────
url = None
for d in sh(REF, "REF_Sources_Systeme"):
    if str(d.get("nom_source")) == "GOOGLE_SHEET_M04_DECLARATIONS" and str(d.get("actif")) == "OUI":
        url = str(d.get("dossier_source") or "").strip()
if not url or not url.startswith("http"):
    abort("URL GOOGLE_SHEET_M04_DECLARATIONS absente/invalide dans REF_Sources_Systeme (SRC_011).")

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

# ── Référentiels ─────────────────────────────────────────────────────────────
lmap, lognom, logtype, logprop, loghaid = {}, {}, {}, {}, {}
for d in sh(REF, "REF_Mapping_Logements"):
    if d.get("valeur_source"): lmap[norm(d["valeur_source"])] = d.get("logement_id")
for d in sh(REF, "REF_Logements"):
    lid = d.get("logement_id")
    if lid and str(lid) != "logement_id":
        lognom[lid] = d.get("nom_logement_officiel"); logtype[lid] = d.get("type_logement_id")
        logprop[lid] = d.get("proprietaire_id"); loghaid[lid] = d.get("hostaway_listing_id")
std_ref = sh(REF, "REF_Couts_Standards_Menage")
hourly_ref = sh_opt(REF, "REF_Taux_Heures_Menage")
fixed_ref = sh_opt(REF, "REF_Couts_Menage_Interne")

# ── Mapping prénom Google Sheet -> intervenant ────────────────────────────────
# Construit DEPUIS REF_Intervenants.nom_normalise (D104) : ni prénom réel ni identifiant ne sont
# codés en dur ici. Corrige ANO-2026-07-28-01 — l'ancien mapping (dict `INTMAP` figé sur trois
# prénoms réels) empêchait tout jeu de données fictif de traverser ce moteur : un intervenant
# fictif n'y était jamais reconnu, et l'agrégation aval (lot6d) plantait sur un intervenant_id nul
# mêlé à des chaînes lors d'un tri. Un référentiel fictif définit ses propres nom_normalise et le
# mapping fonctionne alors identiquement, sans aucune donnée réelle requise.
intmap = {}
for d in sh(REF, "REF_Intervenants"):
    iid = d.get("intervenant_id")
    if not iid or str(iid) == "intervenant_id":
        continue
    cle = d.get("nom_normalise")
    if cle:
        intmap[norm(cle)] = (iid, d.get("nom_intervenant") or iid)

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
def std_unit(type_id, dref):
    best = None
    for d in std_ref:
        if d.get("type_logement_id") != type_id or str(d.get("actif")) != "OUI": continue
        deb, fin = to_d(d.get("date_debut_validite")), to_d(d.get("date_fin_validite"))
        if deb and dref < deb: continue
        if fin and dref > fin: continue
        best = fnum(d.get("cout_standard_menage"))
    return best

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

# ── 1) MASTER_NORM ───────────────────────────────────────────────────────────
os.makedirs(NORM_DIR, exist_ok=True)
NCOLS = ["mois","annee","mois_saisie","appartement_source","nom_appartement","logement_id",
    "intervenant_source","intervenant_id","nom_intervenant","type_intervenant","nb_menages","nb_heures",
    "cout_lavage_attribue","lavage_non_attribuable_mois","statut_controle","code_controle","source_url","date_extraction","ROW_HASH"]
wbn = openpyxl.Workbook(); wsn = wbn.active; wsn.title = "MASTER_NORMALISE"; wsn.append(NCOLS)
for c in wsn[1]: c.font = Font(bold=True); c.fill = PatternFill("solid", fgColor="DDDDDD")
for d in norm_rows: wsn.append([d.get(c) for c in NCOLS])
wbn.save(NORM_OUT)

# ── SQLite : menages_declarations_internes (0038) — sortie canonique pour Lot6d/6e ──────────────
# Le classeur M04 (ci-dessous) reste écrit pour Lot9-12, pas encore migrés (parité temporaire).
#
# RECONCILIATION SHEET <-> APPLICATION (migration 0066, mission "FINALISER LE VRAI WORKFLOW")
# Un DELETE FROM complet écraserait silencieusement toute déclaration créée/modifiée depuis
# l'application (menages_declarations_extra.source == 'APPLICATION') : remplacé par un upsert
# clé par clé (mois, logement_id, intervenant_id). Une ligne APPLICATION dont la valeur Sheet
# diverge n'est JAMAIS réécrite : un conflit est enregistré (menages_declarations_conflits),
# résolu uniquement par un humain via menages_declarations_service.resoudre_conflit().
_db = dbm.chemin_db(None)
if _db is None:
    print("[lot6b] Aucune base designee (PILOTAGE_DB_PATH/APP_DATA_DIR) : SQLite non ecrit")
else:
    _sql_cols = [c for c in NCOLS if c != "ROW_HASH"]
    _conn = dbm.ouvrir(_db)
    _run_id = os.environ.get("LOT6_RUN_ID", "")
    _nb_ecrites = _nb_conflits = _nb_inchangees = 0
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
          f"detectes (menages_declarations_conflits, non ecrases)")
    print(f"[lot6b] MOIS_IMPACTES: {','.join(sorted(_mois_impactes)) or 'AUCUN'}")

# ── 2) M04 SOURCE_RAW + MASTER + VUE_ACTIVE (autres onglets préservés) ────────
backup = M04 + ".BAK_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
shutil.copy(M04, backup)
wb = openpyxl.load_workbook(M04)
ws = wb["SOURCE_RAW"]
if ws.max_row > 1: ws.delete_rows(2, ws.max_row - 1)
for d in norm_rows:
    ws.append([d["mois"], d["appartement_source"], d["intervenant_source"], "MENAGE_STANDARD",
               d["nb_menages"], d["nb_heures"], f'lavage={d["cout_lavage_attribue"] or 0}; import lot6b'])
if "tbl_SOURCE_RAW" in ws.tables:
    ws.tables["tbl_SOURCE_RAW"].ref = f"A1:G{1+len(norm_rows)}"

MASTER_HEADERS = [c.value for c in wb["MASTER"][1]]
for extra_col in [
    "methode_cout_interne", "cout_interne_ref_id", "cout_interne_priorite",
    "controle_cout_interne",
]:
    if extra_col not in MASTER_HEADERS:
        MASTER_HEADERS.append(extra_col)
master_rows = []; cnt = collections.Counter()
for d in norm_rows:
    miso = d["mois"]; lid = d["logement_id"]; type_id = logtype.get(lid)
    dref = to_d((miso + "-01")) if miso else datetime.date.today()
    nb = d["nb_menages"]; nh = d["nb_heures"]
    su = std_unit(type_id, dref or datetime.date.today())
    cost = resolve_internal_cleaning_cost(
        ref_date=dref,
        intervenant_id=d["intervenant_id"],
        logement_id=lid,
        type_logement_id=type_id,
        nb_menages=nb,
        nb_heures=nh,
        hourly_rows=hourly_ref,
        fixed_rows=fixed_ref,
    )
    cet = cost.total if cost.status == "OK" else None
    ceu = round(cet / nb, 2) if (cet is not None and nb) else None
    cst = round((su or 0) * nb, 2) if su is not None else None
    ec = round(cst - cet, 2) if (cst is not None and cet is not None) else None
    statut_controle = d["statut_controle"]
    code_controle = d["code_controle"] or None
    if cost.status != "OK":
        statut_controle = "A_CONTROLER"
        code_controle = f"COUT_INTERNE_{cost.status}"
    cnt[miso] += 1
    mid = f"MEN-{miso or '0000-00'}-{cnt[miso]:03d}"
    master_rows.append({"menage_calc_id": mid, "ROW_HASH": d["ROW_HASH"], "mois": miso, "annee": d["annee"],
        "mois_num": (miso[5:7] if miso else None), "logement_id": lid, "proprietaire_id": logprop.get(lid),
        "hostaway_listing_id": loghaid.get(lid), "appartement_source": d["appartement_source"],
        "intervenant_id": d["intervenant_id"], "nom_intervenant": d["nom_intervenant"], "type_intervenant": "INTERNE",
        "type_menage": "MENAGE_STANDARD", "nb_menages": nb, "nb_heures": nh, "taux_horaire_intervenant": cost.rate,
        "cout_execution_total": cet, "cout_execution_unitaire": ceu, "cout_standard": su,
        "cout_standard_total_ligne": cst, "ecart_main_oeuvre_vs_standard": ec, "total_execution": cet,
        "methode_cout_interne": cost.method, "cout_interne_ref_id": cost.ref_id,
        "cout_interne_priorite": cost.priority, "controle_cout_interne": "OK" if cost.status == "OK" else cost.message,
        "type_flux_id": "TYPE_FLUX_013", "sens": "CHARGE", "code_impact": "HC",
        "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
        "statut_controle": statut_controle, "niveau_anomalie": ("INFO" if statut_controle == "VALIDE" else "A_CONTROLER"),
        "code_anomalie": code_controle, "source_module": "lot6b", "source_table": "SOURCE_RAW",
        "source_pk": mid, "date_integration": NOW})
for sheetname, only_valide in [("MASTER", False), ("VUE_ACTIVE", True)]:
    wsm = wb[sheetname]
    if wsm.max_row > 1: wsm.delete_rows(2, wsm.max_row - 1)
    for r in master_rows:
        if only_valide and r["statut_controle"] != "VALIDE": continue
        wsm.append([r.get(h) for h in MASTER_HEADERS])
wb.save(M04); wb.close()

# ── Sorties metier ecrites OK -> provenance officielle PUIS suppression PENDING ─
commit_step(CACHE_DIR, "lot6b", prov)

# ── Rapport ──────────────────────────────────────────────────────────────────
mai = [d for d in norm_rows if d["mois"] == "2026-05"]
ag = collections.Counter()
for d in mai: ag[(d["intervenant_id"], d["nom_intervenant"])] += d["nb_menages"]
nmap = sum(1 for d in norm_rows if d["code_controle"])
print(f"[lot6b] URL REF OK (SRC_011) | CSV {len(rows)-1} lignes | normalisées {len(norm_rows)} | M04 MASTER {len(master_rows)} lignes")
print(f"[lot6b] MASTER_NORM -> {NORM_OUT}")
print(f"[lot6b] M04 SOURCE_RAW+MASTER+VUE_ACTIVE reconstruit SANS Power Query | backup {os.path.basename(backup)}")
print(f"[lot6b] mai 2026 par intervenant : {dict(ag)}")
print(f"[lot6b] lignes A_CONTROLER (mapping) : {nmap}")

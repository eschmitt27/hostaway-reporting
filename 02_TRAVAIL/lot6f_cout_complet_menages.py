"""
lot6f_cout_complet_menages.py — Coût complet ménage AVANCÉ (vue analytique)
================================================================================
Vue ANALYTIQUE pure (D105). N'écrit RIEN dans Flux / Resultats / Commissions /
NetProprietaire / Factures. Ne modifie pas M04 / SAISIE_Charges_Flux / REF_Setup /
banque / VRBO. Ne relance pas lot9-12.

cout_complet_total = cout_direct_total + Σ quote_parts (local + lavage + courses + conso + autres)
ecart_vs_standard_total = cout_standard_total - cout_complet_total   ( >0 GAIN | <0 PERTE | =0 EQUILIBRE )

Périmètre ACCEPTÉ (§6) :
  - direct externe = montant facture prestataire (lot6c)
  - direct interne <=2026-05 = heures × taux PARAM_004 ; >=2026-06 = forfait REF_Couts_Menage_Interne
  - LOCAL_CAVE = REC_002 (date-aware) ventilé sur tous les ménages du mois
  - LAVAGE interne = Google Sheet / M04 (attribuable par appart + non-attribuable par intervenant)
  - COURSES/CONSO/ACHATS = SAISIE_Charges_Flux (affectable_menage=OUI) -> vide aujourd'hui => 0 POOL_VIDE_NON_SAISI
HORS périmètre (étape ultérieure) : heures cave, heures courses, consommables non saisis.
Pas de logique 'fournitures_incluses'. L'affectation dépend de affectable_menage + intervenant_concerne.

Clé de ventilation (D103) : poids = nb_menages × cout_standard_unitaire.

DRY-RUN : 02_TRAVAIL/Lot6_DryRun/DRYRUN_CoutComplet_Menages.xlsx — MOIS = 2026-05.
"""

import argparse, sys, os, io, csv, glob, subprocess, hashlib, datetime, collections, unicodedata, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import openpyxl
from openpyxl.styles import Font, PatternFill
from lib_menage_costs import resolve_internal_cleaning_cost
import lot3_generateur_charges as lot3   # mois dérivé de date_charge (jamais le cache formule)
import lib_db_moteur as dbm

_ap = argparse.ArgumentParser()
_ap.add_argument("--source", choices=("EXCEL", "SQLITE"), default="EXCEL",
                 help="SQLITE lit les declarations internes (avec lavage) depuis "
                      "menages_declarations_internes (Lot6b, deja alimente depuis la meme Google "
                      "Sheet) au lieu de refaire l'appel reseau ici.")
_ap.add_argument("--db", default=None)
_ap.add_argument("--mois", default=None,
                 help="AAAA-MM. Absent = dernier mois present dans menages_taches_enrichies.")
_ap.add_argument("--sans-excel", action="store_true")
_ap.add_argument("--sans-sqlite", action="store_true")
_ap.add_argument("--run-id", default="")
args = _ap.parse_args()
chemin_base = dbm.chemin_db(args.db)

# AUD-005 — mono-mois volontaire : l'extension multi-mois de l'écart analytique ménage est
# différée jusqu'à la mise en place d'un vrai processus de clôture mensuelle métier/comptable.
# Ne pas utiliser les clôtures techniques réservations/VRBO (REF_Cloture_Mensuelle) comme
# déclencheur de ce calcul. Statut registre : DIFFERE / BYPASS_PROVISOIRE.
PIVOT = "2026-06"
if args.source == "SQLITE":
    if chemin_base is None:
        sys.exit("[lot6f] ERREUR : --source SQLITE exige une base (--db / PILOTAGE_DB_PATH / "
                 "APP_DATA_DIR).")
    _conn0 = dbm.ouvrir(chemin_base)
    if args.mois:
        MONTH = args.mois
    else:
        r = _conn0.execute(
            "SELECT MAX(mois) FROM menages_taches_enrichies WHERE mois IS NOT NULL").fetchone()
        MONTH = r[0] if r and r[0] else datetime.date.today().strftime("%Y-%m")
    _conn0.close()
else:
    MONTH = args.mois or "2026-05"
DREF = datetime.date.fromisoformat(MONTH + "-01")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REF  = os.path.join(ROOT, "01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
# SAISIE_Charges_Flux (module Charges) reste Excel dans les deux chemins : hors perimetre de la
# migration Menages (le module Charges lui-meme n'est pas migre — cf mission, decision explicite).
SAISIE = os.path.join(ROOT, "01_SOURCES_BRUTES", "Charges", "SAISIE_Charges_Flux.xlsx")
OUTD = os.path.join(ROOT, "02_TRAVAIL", "Lot6f_CoutComplet_Menages")
OUT  = os.path.join(OUTD, "MASTER_CALC_CoutComplet_Menages.xlsx")
def _m04_url():
    """URL Google Sheet M04 lue depuis REF_Setup > REF_Sources_Systeme (pas d'URL en dur)."""
    wb = openpyxl.load_workbook(REF, read_only=True, data_only=True); ws = wb["REF_Sources_Systeme"]
    rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]; wb.close()
    H = [str(c) for c in rows[0]]
    for r in rows[1:]:
        d = dict(zip(H, r))
        if str(d.get("nom_source")) == "GOOGLE_SHEET_M04_DECLARATIONS" and str(d.get("actif")) == "OUI":
            u = str(d.get("dossier_source") or "").strip()
            if u.startswith("http"): return u
    raise SystemExit("[BLOQUANT lot6f] URL GOOGLE_SHEET_M04_DECLARATIONS absente de REF_Sources_Systeme (SRC_011).")
if args.source != "SQLITE":
    SHEET_URL = _m04_url()

def sh(p, s):
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    if s not in wb.sheetnames: wb.close(); return []
    ws = wb[s]; rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]; wb.close()
    return [dict(zip([str(c) for c in rows[0]], r)) for r in rows[1:]]

def mois_charge(d):
    """mois d'une charge SAISIE, dérivé de date_charge — JAMAIS la colonne formule C.

    La colonne `mois` de SAISIE_Charges_Flux est une formule Excel : openpyxl la préserve mais ne
    la recalcule pas. Lue en data_only=True après une écriture applicative, son cache est vide et
    la charge serait silencieusement exclue des pools ménage. On dérive donc depuis date_charge.
    Retourne "" si la date est inexploitable (l'appelant émet alors un contrôle explicite).
    """
    return lot3.mois_de(d.get("date_charge"))

def norm(s):
    s = str(s or "").strip().lower()
    return " ".join("".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").split())

def rh(*v): return hashlib.sha256("|".join("" if x is None else str(x) for x in v).encode()).hexdigest()[:16]
def to_d(v):
    if isinstance(v, datetime.datetime): return v.date()
    if isinstance(v, datetime.date): return v
    try: return datetime.date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError): return None
def f(x):
    try: return float(x)
    except (TypeError, ValueError): return None

# ── Référentiels ─────────────────────────────────────────────────────────────
def _num(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None

if args.source == "SQLITE":
    _conn = dbm.ouvrir(chemin_base)
    from lib_ref_history import resolve_management_period
    log_info_raw = {r["logement_id"]: r for r in dbm.lignes(
        _conn, "ref_logements", ("logement_id", "type_logement_id", "nom_logement_officiel"),
        ordre="logement_id")}
    gest_rows = dbm.lignes(_conn, "ref_gestion_logements_hist",
        ("gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
         "statut_gestion", "source", "commentaire"), ordre="gestion_id") \
        if dbm.table_presente(_conn, "ref_gestion_logements_hist") else []
    log_info = {}
    for lid, d in log_info_raw.items():
        prop_id = None
        if gest_rows:
            res = resolve_management_period(gest_rows, logement_id=lid, date_arrivee=MONTH + "-15")
            prop_id = res.value if res.status == "OK" else None
        log_info[lid] = {**d, "proprietaire_id": prop_id}
    typ_lib = {r["type_logement_id"]: r["type_logement"] for r in dbm.lignes(
        _conn, "ref_types_logements", ("type_logement_id", "type_logement"),
        ordre="type_logement_id")}
    int_info = {r["intervenant_id"]: r for r in dbm.lignes(
        _conn, "ref_intervenants", ("intervenant_id", "nom_intervenant"), ordre="intervenant_id")}
    std_ref = [{**r, "cout_standard_menage": _num(r.get("cout_standard_menage"))} for r in dbm.lignes(
        _conn, "ref_couts_standards_menage",
        ("type_logement_id", "cout_standard_menage", "actif", "date_debut_validite",
         "date_fin_validite"), ordre="cout_standard_id")]
    int_ref = [{**r, "montant_interne_standard": _num(r.get("montant_interne_standard"))}
              for r in dbm.lignes(_conn, "ref_couts_menage_interne",
                                  ("type_logement_id", "montant_interne_standard", "actif",
                                   "date_debut_validite", "date_fin_validite"),
                                  ordre="cout_menage_interne_id")] \
        if dbm.table_presente(_conn, "ref_couts_menage_interne") else []
    hourly_ref = [{**r, "taux_horaire": _num(r.get("taux_horaire"))} for r in dbm.lignes(
        _conn, "ref_taux_heures_menage",
        ("intervenant_id", "taux_horaire", "actif", "date_debut", "date_fin"),
        ordre="taux_horaire_id")] if dbm.table_presente(_conn, "ref_taux_heures_menage") else []
    rec_ref = [{**r, "montant_ttc": _num(r.get("montant_ttc"))} for r in dbm.lignes(
        _conn, "ref_charges_recurrentes",
        ("charge_recurrente_id", "montant_ttc", "actif", "date_debut_validite",
         "date_fin_validite"), ordre="charge_recurrente_id")]
else:
    log_info = {d["logement_id"]: d for d in sh(REF, "REF_Logements") if d.get("logement_id") and str(d["logement_id"]) != "logement_id"}
    typ_lib  = {d["type_logement_id"]: d.get("type_logement") for d in sh(REF, "REF_Types_Logements")}
    int_info = {d["intervenant_id"]: d for d in sh(REF, "REF_Intervenants")}
    std_ref  = sh(REF, "REF_Couts_Standards_Menage")
    int_ref  = sh(REF, "REF_Couts_Menage_Interne")
    hourly_ref = sh(REF, "REF_Taux_Heures_Menage")
    rec_ref  = sh(REF, "REF_Charges_Recurrentes")

def date_aware(rows, type_id, montant_field, type_field="type_logement_id"):
    best = None
    for d in rows:
        if d.get(type_field) != type_id or str(d.get("actif")) != "OUI": continue
        deb, fin = to_d(d.get("date_debut_validite")), to_d(d.get("date_fin_validite"))
        if deb and DREF < deb: continue
        if fin and DREF > fin: continue
        best = f(d.get(montant_field))
    return best
def std_unit(type_id): return date_aware(std_ref, type_id, "cout_standard_menage")
def int_unit(type_id): return date_aware(int_ref, type_id, "montant_interne_standard")

# REC_002 local cave date-aware
def rec_montant(rec_id):
    best = None
    for d in rec_ref:
        if d.get("charge_recurrente_id") != rec_id or str(d.get("actif")) != "OUI": continue
        deb, fin = to_d(d.get("date_debut_validite")), to_d(d.get("date_fin_validite"))
        if deb and DREF < deb: continue
        if fin and DREF > fin: continue
        best = f(d.get("montant_ttc"))
    return best
local_cave_montant = rec_montant("REC_002") or 0.0

controls = []

if args.source == "SQLITE":
    # ── Direct EXTERNE — facture_lignes_menage (bridge 6c) ────────────────────────────────────
    ext = collections.defaultdict(lambda: [0, 0.0])
    if dbm.table_presente(_conn, "facture_lignes_menage"):
        cur = _conn.execute(
            "SELECT l.logement_id, f.fournisseur_id_opaque, l.montant_ttc, d.quantite, "
            "f.date_facture, l.facture_id_opaque "
            "FROM facture_lignes_menage l "
            "JOIN factures f ON f.facture_id_opaque = l.facture_id_opaque "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            # Seules les factures VALIDEES entrent dans le cout complet : une facture A_CONTROLER
            # est un document recu, pas une charge acceptee. Sans ce filtre, son montant remontait
            # jusqu'a menages_cout_complet puis TYPE_FLUX_018/019 dans lot9.
            "WHERE l.type_ligne = 'MENAGE_EXTERNE' "
            f"AND {dbm.filtre_sql_factures_comptables('f')}")
        for lg, pid, mttc, qte, dfac, fid in cur.fetchall():
            if str(dfac or "")[:7] != MONTH: continue
            q = qte if qte is not None else 1
            if q == 0 and (mttc or 0) == 0:
                controls.append(("EXCLU_VOLUME", "INFO", f"facture {fid} 0€/q0")); continue
            e = ext[(lg, pid)]; e[0] += q; e[1] += (mttc or 0)

    # ── Interne + LAVAGE — menages_declarations_internes (Lot6b, deja resolu) ─────────────────
    interne = collections.defaultdict(lambda: [0, 0.0, 0.0])
    lav_na_by_int = collections.Counter()
    for d in dbm.lignes(_conn, "menages_declarations_internes",
                        ("mois", "logement_id", "intervenant_id", "nb_menages", "nb_heures",
                         "cout_lavage_attribue", "lavage_non_attribuable_mois"), ordre="id"):
        if str(d.get("mois"))[:7] != MONTH: continue
        lg, iid = d.get("logement_id"), d.get("intervenant_id")
        e = interne[(lg, iid)]
        e[0] += d.get("nb_menages") or 0
        e[1] += d.get("nb_heures") or 0
        e[2] += d.get("cout_lavage_attribue") or 0
        lav_na = d.get("lavage_non_attribuable_mois")
        if lav_na:
            lav_na_by_int[iid] += lav_na
    _conn.close()
else:
    # mapping libellé appart -> logement_id
    lmap = {}
    for d in sh(REF, "REF_Mapping_Logements"):
        if d.get("valeur_source"): lmap[norm(d["valeur_source"])] = d.get("logement_id")

    # ── Direct EXTERNE (lot6c) ───────────────────────────────────────────────────
    fc = glob.glob(os.path.join(ROOT, "02_TRAVAIL", "**", "MASTER_FACT_MEN_MenagesExternes.xlsx"), recursive=True)[0]
    ext = collections.defaultdict(lambda: [0, 0.0])
    for d in sh(fc, "MASTER"):
        if str(d.get("mois"))[:7] != MONTH: continue
        if str(d.get("type_ligne_menage_id")) not in ("TLM_001", "TLM_002"): continue
        q = d.get("nombre_menages") or 0; m = d.get("montant_ligne_ttc") or 0
        if (q or 0) == 0 and (m or 0) == 0:
            controls.append(("EXCLU_VOLUME", "INFO", f"facture {d.get('facture_id')} 0€/q0")); continue
        e = ext[(d.get("logement_id"), d.get("prestataire_id"))]; e[0] += q; e[1] += m

    # ── Interne + LAVAGE (Google Sheet) ──────────────────────────────────────────
    INTMAP = {"imene": "INT_0001", "kira": "INT_0002", "kheira": "INT_0002"}
    MOIS = {"janvier":"01","fevrier":"02","mars":"03","avril":"04","mai":"05","juin":"06","juillet":"07","aout":"08","septembre":"09","octobre":"10","novembre":"11","decembre":"12"}
    from lib_sheet_source import fetch_sheet_csv, begin_step, commit_step
    _CACHE_DIR = os.path.join(ROOT, "02_DONNEES_NORMALISEES", "menages", "_cache_google_sheet")
    txt, _prov = fetch_sheet_csv(SHEET_URL, _CACHE_DIR, step="lot6f")   # SystemExit si indisponible
    if _prov["resolution_source"] == "CACHE":
        print(f"[lot6f] AVERTISSEMENT SOURCE_SHEET_CACHE_UTILISE : cache du {_prov['cache_date_extraction_utc']} (age {_prov['cache_age_h']}h).")
    srows = list(csv.reader(io.StringIO(txt))); shdr = srows[0]
    i_pre = shdr.index("Prénom"); i_mois = shdr.index("Mois des ménages"); i_an = shdr.index("Année des ménages")
    appcols = [i for i, h in enumerate(shdr) if h.strip() == "Appartement"]
    # colonne lavage non-attribuable (queue)
    i_lav_na = next((i for i, h in enumerate(shdr) if h.strip().startswith("Coûts de lavage du linge (hors")), None)

    interne = collections.defaultdict(lambda: [0, 0.0, 0.0])   # (lg,iid) -> [nb, heures, lavage_attribuable]
    lav_na_by_int = collections.Counter()                       # iid -> lavage non-attribuable du mois
    for r in srows[1:]:
        if not any(c.strip() for c in r): continue
        pre = r[i_pre].strip(); mm = MOIS.get(norm(r[i_mois])); yr = r[i_an].strip()
        miso = f"{yr}-{mm}" if (mm and yr) else None
        if miso != MONTH: continue
        iid = INTMAP.get(norm(pre))
        for ci in appcols:
            app = r[ci].strip() if ci < len(r) else ""
            if not app: continue
            lg = lmap.get(norm(app))
            nb = int(f(r[ci+1]) or 0) if (ci+1 < len(r) and r[ci+1].strip()) else 0
            h  = f(r[ci+2]) if (ci+2 < len(r) and r[ci+2].strip()) else 0
            lav = f(r[ci+3]) if (ci+3 < len(r) and r[ci+3].strip()) else 0
            e = interne[(lg, iid)]; e[0] += nb; e[1] += (h or 0); e[2] += (lav or 0)
        if i_lav_na is not None and i_lav_na < len(r) and r[i_lav_na].strip():
            lav_na_by_int[iid] += f(r[i_lav_na]) or 0

# ── Construction lignes de base (direct + standard) ──────────────────────────
lines = []   # dict par (mois,lg,iid)
def base_line(lg, iid, typ_interv, nb, heures, methode, direct, lav_attr=0.0,
              cout_ref=None, cout_priority=None, cout_status="OK", cout_message=""):
    ti = log_info.get(lg) or {}; type_id = ti.get("type_logement_id")
    su = std_unit(type_id)
    poids = (nb * su) if (su is not None) else 0
    lines.append({"mois": MONTH, "logement_id": lg, "nom_appartement": ti.get("nom_logement_officiel"),
        "proprietaire_id": ti.get("proprietaire_id"),
        "type_logement_id": type_id, "type_logement_libelle": typ_lib.get(type_id),
        "intervenant_id": iid, "nom_intervenant": (int_info.get(iid) or {}).get("nom_intervenant"),
        "type_intervenant": typ_interv, "nb_menages": nb, "nb_heures": heures,
        "cout_standard_unitaire": su, "cout_standard_total": (su*nb if su is not None else None),
        "methode": methode, "cout_direct_total": direct, "cout_interne_ref_id": cout_ref,
        "cout_interne_priorite": cout_priority,
        "controle_cout_interne": "OK" if cout_status == "OK" else cout_message,
        "poids": poids,
        "lavage_attribuable": lav_attr})

for (lg, iid), (nb, mont) in ext.items():
    base_line(lg, iid, "EXTERNE", nb, None, "EXTERNE_FACTURE", round(mont, 2))
for (lg, iid), (nb, heures, lav) in interne.items():
    ti = (log_info.get(lg) or {}).get("type_logement_id")
    cost = resolve_internal_cleaning_cost(
        ref_date=DREF,
        intervenant_id=iid,
        logement_id=lg,
        type_logement_id=ti,
        nb_menages=nb,
        nb_heures=heures,
        hourly_rows=hourly_ref,
        fixed_rows=int_ref,
    )
    if cost.status != "OK":
        controls.append((f"COUT_INTERNE_{cost.status}", "BLOQUANT", f"{lg}/{iid}/{MONTH}: {cost.message}"))
    base_line(
        lg, iid, "INTERNE", nb, heures, cost.method, cost.total,
        lav_attr=round(lav, 2), cout_ref=cost.ref_id,
        cout_priority=cost.priority, cout_status=cost.status,
        cout_message=cost.message,
    )

# ── POOLS de charges communes ────────────────────────────────────────────────
sum_poids_all = sum(l["poids"] for l in lines) or 1
# La cave/local ne sert QU'AUX ménages internes -> ventilée sur le poids interne uniquement.
sum_poids_interne = sum(l["poids"] for l in lines if l["type_intervenant"] == "INTERNE") or 1
sum_poids_int = collections.Counter()
for l in lines:
    if l["type_intervenant"] == "INTERNE": sum_poids_int[l["intervenant_id"]] += l["poids"]

# SAISIE courses/conso/achats (affectable_menage=OUI) — vide aujourd'hui
saisie_rows = sh(SAISIE, "SAISIE")
saisie_has_col = bool(saisie_rows) and "affectable_menage" in saisie_rows[0]
pool_courses = pool_conso = pool_autres = 0.0
nb_affectables = nb_date_invalide = 0
if saisie_has_col:
    for d in saisie_rows:
        if str(d.get("affectable_menage")) != "OUI": continue
        nb_affectables += 1
        m_charge = mois_charge(d)
        if not m_charge:
            nb_date_invalide += 1
            continue
        if m_charge != MONTH: continue
        cat = str(d.get("categorie_charge_id")); m = f(d.get("montant")) or 0
        if cat == "CHG_004": pool_conso += m
        elif cat in ("CHG_018",): pool_autres += m
        else: pool_courses += m
if nb_date_invalide:
    controls.append(("CHARGE_MENAGE_DATE_INVALIDE", "A_CONTROLER",
                     f"{nb_date_invalide} charge(s) ménage affectable(s) sans date_charge exploitable — mois non dérivable, exclues des pools"))
if pool_courses == 0 and pool_conso == 0 and pool_autres == 0:
    if nb_affectables:
        controls.append(("POOL_VIDE_HORS_MOIS", "INFO",
                         f"{nb_affectables} charge(s) ménage affectable(s) en SAISIE, aucune sur {MONTH} (pools courses/conso=0)"))
    else:
        controls.append(("POOL_VIDE_NON_SAISI", "INFO", "Aucune charge ménage affectable saisie dans SAISIE_Charges_Flux (pools courses/conso=0)"))

# contrôle double source lavage
if saisie_has_col:
    lav_saisie = [d for d in saisie_rows if str(d.get("categorie_charge_id")) == "CHG_003" and str(d.get("affectable_menage")) == "OUI" and mois_charge(d) == MONTH]
    if lav_saisie and sum(l["lavage_attribuable"] for l in lines) > 0:
        controls.append(("DOUBLE_SOURCE_LAVAGE_A_CONTROLER", "A_CONTROLER", f"{len(lav_saisie)} lignes lavage SAISIE affectable=OUI + lavage Google Sheet présent"))

POOLS = {"LOCAL_CAVE": local_cave_montant, "COURSES": pool_courses, "CONSOMMABLES": pool_conso, "AUTRES": pool_autres}
# Règle figée : cave/local REC_002 ventilée UNIQUEMENT sur les ménages internes (jamais les externes).
controls.append(("REC_002_LOCAL_CAVE_INTERNE_ONLY", "INFO",
    f"Cave {local_cave_montant}€ ventilée sur poids internes ({round(sum_poids_interne,2)}) — externes exclus"))

# ── Quote-parts + coût complet par ligne ─────────────────────────────────────
ventil = []
for l in lines:
    w = l["poids"]
    # cave = ménages internes uniquement
    qp_local = round(local_cave_montant * w / sum_poids_interne, 2) if (local_cave_montant and l["type_intervenant"] == "INTERNE") else 0.0
    # lavage = attribuable (direct sheet) + non-attribuable ventilé (interne seulement, par poids intervenant)
    qp_lav_na = 0.0
    if l["type_intervenant"] == "INTERNE" and lav_na_by_int.get(l["intervenant_id"]):
        sp = sum_poids_int.get(l["intervenant_id"]) or 1
        qp_lav_na = round(lav_na_by_int[l["intervenant_id"]] * w / sp, 2)
    qp_lavage = round(l["lavage_attribuable"] + qp_lav_na, 2)
    qp_courses = round(pool_courses * w / sum_poids_all, 2) if pool_courses else 0.0
    qp_conso = round(pool_conso * w / sum_poids_all, 2) if pool_conso else 0.0
    qp_autres = round(pool_autres * w / sum_poids_all, 2) if pool_autres else 0.0
    cc = None if l["cout_direct_total"] is None else round(l["cout_direct_total"] + qp_local + qp_lavage + qp_courses + qp_conso + qp_autres, 2)
    st = l["cout_standard_total"]
    ecart = round(st - cc, 2) if (st is not None and cc is not None) else None
    statut_e = "NON_CALCULABLE" if ecart is None else ("GAIN" if ecart > 0 else "PERTE" if ecart < 0 else "EQUILIBRE")
    if l.get("controle_cout_interne") not in (None, "OK"):
        statut_c, code = "A_CONTROLER", "COUT_INTERNE_A_CONTROLER"
    else:
        statut_c, code = ("A_CONTROLER", "COUT_STANDARD_ABSENT") if st is None else ("VALIDE", "")
    l.update({"quote_part_local": qp_local, "quote_part_courses": qp_courses, "quote_part_lavage": qp_lavage,
        "quote_part_consommables": qp_conso, "quote_part_autres_charges_menage": qp_autres,
        "cout_complet_total": cc, "cout_complet_unitaire": round(cc/l["nb_menages"], 2) if (cc is not None and l["nb_menages"]) else None,
        "ecart_vs_standard_total": ecart,
        "ecart_unitaire": (round(ecart/l["nb_menages"], 2) if (ecart is not None and l["nb_menages"]) else None),
        "statut_ecart": statut_e, "statut_controle": statut_c,
        "code_controle": code, "commentaire": "", "ROW_HASH": rh(MONTH, l["logement_id"], l["intervenant_id"], cc)})
    for nm, val in [("LOCAL_CAVE", qp_local), ("LAVAGE", qp_lavage), ("COURSES", qp_courses), ("CONSOMMABLES", qp_conso), ("AUTRES", qp_autres)]:
        if val: ventil.append({"mois": MONTH, "pool": nm, "logement_id": l["logement_id"], "intervenant_id": l["intervenant_id"], "poids": round(w,2), "quote_part": val})

# ── Écriture ──────────────────────────────────────────────────────────────────
os.makedirs(OUTD, exist_ok=True)
wb = openpyxl.Workbook()
def wsheet(title, cols, rows, first=False):
    ws = wb.active if first else wb.create_sheet(title)
    if first: ws.title = title
    ws.append(cols)
    for c in ws[1]: c.font = Font(bold=True); c.fill = PatternFill("solid", fgColor="DDDDDD")
    for r in rows: ws.append([r.get(c) for c in cols])
DET = ["mois","logement_id","nom_appartement","proprietaire_id","type_logement_id","intervenant_id","nom_intervenant","type_intervenant",
    "nb_menages","cout_standard_total","cout_direct_total","quote_part_local","quote_part_courses","quote_part_lavage",
    "quote_part_consommables","quote_part_autres_charges_menage","cout_complet_total","cout_complet_unitaire",
    "ecart_vs_standard_total","ecart_unitaire","methode","cout_interne_ref_id","cout_interne_priorite",
    "controle_cout_interne","statut_ecart","statut_controle","code_controle","commentaire"]

# ── SQLite : menages_cout_complet (0038) — remplacement integral par mois ───────────────────────
if args.sans_sqlite:
    print("[lot6f] --sans-sqlite : menages_cout_complet non ecrit.")
elif chemin_base is None:
    print("[lot6f] Aucune base designee : menages_cout_complet non ecrit.")
else:
    _conn = dbm.ouvrir(chemin_base)
    try:
        _conn.execute("DELETE FROM menages_cout_complet WHERE mois = ?", (MONTH,))
        if lines:
            _trous = ", ".join(["?"] * (len(DET) + 1))
            _conn.executemany(
                f"INSERT INTO menages_cout_complet ({', '.join(DET)}, run_id) VALUES ({_trous})",
                [tuple(l.get(c) for c in DET) + (args.run_id or None,) for l in lines])
        _conn.commit()
    finally:
        _conn.close()
    print(f"[lot6f] SQLite : menages_cout_complet — {len(lines)} lignes (mois={MONTH})")

if args.sans_excel:
    print(f"[lot6f] --sans-excel : classeur legacy non ecrit. mois={MONTH} lignes={len(lines)}")
    sys.exit(0)

wsheet("DETAIL_COUT_COMPLET", DET, lines, first=True)
wsheet("POOLS_CHARGES_MENAGE", ["mois","pool","montant_total","source","cle_repartition"],
    [{"mois":MONTH,"pool":k,"montant_total":round(v,2),"source":("REC_002 (date-aware)" if k=="LOCAL_CAVE" else "Google Sheet" if k=="LAVAGE" else "SAISIE_Charges_Flux"),"cle_repartition":"poids=nb×cout_standard"} for k,v in {**POOLS,"LAVAGE":sum(l['lavage_attribuable'] for l in lines)+sum(lav_na_by_int.values())}.items()])
wsheet("VENTILATION_CHARGES", ["mois","pool","logement_id","intervenant_id","poids","quote_part"], ventil)
def resume(keyf):
    agg = collections.defaultdict(lambda: [0,0.0,0.0,0.0])
    for l in lines:
        k = keyf(l); a = agg[k]; a[0]+=l["nb_menages"]; a[1]+=l["cout_standard_total"] or 0; a[2]+=l["cout_complet_total"] or 0; a[3]+=l["ecart_vs_standard_total"] or 0
    return agg
wsheet("RESUME_LOGEMENT", ["mois","logement_id","nom_appartement","nb_menages","cout_standard_total","cout_complet_total","ecart_total","ecart_unitaire_moyen"],
    [{"mois":MONTH,"logement_id":k[0],"nom_appartement":k[1],"nb_menages":v[0],"cout_standard_total":round(v[1],2),"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2),"ecart_unitaire_moyen":(round(v[3]/v[0],2) if v[0] else None)} for k,v in sorted(resume(lambda l:(l["logement_id"],l["nom_appartement"])).items())])
wsheet("RESUME_INTERVENANT", ["mois","intervenant_id","nom_intervenant","type_intervenant","nb_menages","cout_standard_total","cout_complet_total","ecart_total"],
    [{"mois":MONTH,"intervenant_id":k[0],"nom_intervenant":k[1],"type_intervenant":k[2],"nb_menages":v[0],"cout_standard_total":round(v[1],2),"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2)} for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items())])
wsheet("RESUME_PRESTATAIRE", ["mois","intervenant_id","nom_intervenant","nb_menages","cout_complet_total","ecart_total"],
    [{"mois":MONTH,"intervenant_id":k[0],"nom_intervenant":k[1],"nb_menages":v[0],"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2)} for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items()) if k[2]=="EXTERNE"])
cc_ctrl = collections.Counter((c[0],c[1]) for c in controls)
# rappel charges déjà en Flux
cc_ctrl[("CHARGE_EXTERNE_DEJA_EN_FLUX_NON_REINJECTEE","INFO")] += 1
wsheet("CONTROLES_DOUBLE_COMPTAGE", ["code_controle","niveau","nb","exemple"],
    [{"code_controle":k[0],"niveau":k[1],"nb":n,"exemple":next((c[2] for c in controls if (c[0],c[1])==k),"ménage externe (TYPE_FLUX_014) déjà compté dans Flux ; coût complet = analytique, non réinjecté")} for k,n in cc_ctrl.most_common()])
# Transaction provenance DEF-1 : marqueur PENDING AVANT remplacement sortie metier.
begin_step(_CACHE_DIR, "lot6f")
_saved_official = False
try:
    wb.save(OUT); _saved_official = True
except PermissionError:
    OUT = OUT.replace(".xlsx","_MAJ.xlsx"); wb.save(OUT); print(f"[lot6f] original verrouillé -> {os.path.basename(OUT)}")
# Provenance officielle SEULEMENT si la sortie officielle (OUT reel) a ete ecrite.
# Si fallback _MAJ : sortie officielle stale -> PENDING reste -> lot11 SOURCE_SHEET_PROVENANCE_INCOMPLETE.
if _saved_official:
    commit_step(_CACHE_DIR, "lot6f", _prov)

# ── Rapport ──────────────────────────────────────────────────────────────────
ts = sum(l["cout_standard_total"] or 0 for l in lines); tc = sum(l["cout_complet_total"] or 0 for l in lines)
print(f"[lot6f] DRY-RUN mois={MONTH} | local_cave={local_cave_montant} -> {OUT}")
print(f"  lignes DETAIL : {len(lines)}")
print(f"  POOLS : LOCAL_CAVE={round(local_cave_montant,2)} LAVAGE={round(sum(l['lavage_attribuable'] for l in lines)+sum(lav_na_by_int.values()),2)} COURSES={pool_courses} CONSO={pool_conso}")
print(f"  COUT GLOBAL : standard={round(ts,2)} complet={round(tc,2)} ecart={round(ts-tc,2)} ({'GAIN' if ts-tc>0 else 'PERTE' if ts-tc<0 else 'EQ'})")
print("\n  par intervenant:")
for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items()):
    print(f"    {str(k[0]):10} {str(k[1])[:9]:9} {str(k[2]):8} nb={v[0]:3} std={round(v[1],2):8} complet={round(v[2],2):8} ecart={round(v[3],2):8}")
print("\n  CONTROLES:", dict(cc_ctrl))

"""
lot6d_rapprochement_menages.py — PARTIE 1 : rapprochement du NOMBRE de ménages
================================================================================
Compare, SANS AUCUN COÛT, par mois × logement × intervenant :
  ménages Hostaway Tasks réalisés (completed)
  vs ménages externes facturés (lot6c, lignes TLM compte_comme_menage=OUI)
  vs ménages internes M04 déclarés.

Décisions : D099, D100, D104 (révisée — mapping via REF_Intervenants.hostaway_assigneeUserId).

DRY-RUN : sortie de test 02_TRAVAIL/Lot6_DryRun/DRYRUN_Rapprochement_Menages_Complet.xlsx
  (non définitive). N'écrit RIEN dans Flux / résultats. Ne touche pas M04 / banque / VRBO.

Périmètre courant : MOIS = 2026-05.
"""

import argparse, sys, os, glob, hashlib, datetime, collections, unicodedata, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import openpyxl
from openpyxl.styles import Font, PatternFill

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_db_moteur as dbm

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REF  = os.path.join(ROOT, "01_SOURCES_BRUTES", "REF_Setup", "REF_Setup.xlsm")
OUTD = os.path.join(ROOT, "02_TRAVAIL", "Lot6d_Rapprochement_Menages")
OUT  = os.path.join(OUTD, "MASTER_CTRL_Rapprochement_Menages.xlsx")
NORM_DIR = os.path.join(ROOT, "02_TRAVAIL", "Lot6b_DeclarationsInternes")

def sh(p, s):
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True); ws = wb[s]
    rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]; wb.close()
    h = list(rows[0]); return h, [dict(zip(h, r)) for r in rows[1:]]

def norm(s):
    s = str(s or "").strip().lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def rowhash(*v):
    return hashlib.sha256("|".join("" if x is None else str(x) for x in v).encode()).hexdigest()[:16]

_ap = argparse.ArgumentParser()
_ap.add_argument("--source", choices=("EXCEL", "SQLITE"), default="EXCEL",
                 help="EXCEL = classeurs legacy Lot6a/6b/6c (comportement historique). "
                      "SQLITE = menages_taches_enrichies/menages_declarations_internes/"
                      "facture_lignes_menage (0038/0039), deja alimentes par ces Lots.")
_ap.add_argument("--db", default=None)
_ap.add_argument("--mois", default=None,
                 help="AAAA-MM. Absent = dernier mois present dans les Tasks CleaningTasks — "
                      "aucun mois n'est fige dans le code (mission §12).")
_ap.add_argument("--sans-excel", action="store_true")
_ap.add_argument("--sans-sqlite", action="store_true")
_ap.add_argument("--run-id", default="")
args = _ap.parse_args()
chemin_base = dbm.chemin_db(args.db)

if args.source == "SQLITE":
    if chemin_base is None:
        sys.exit("[lot6d] ERREUR : --source SQLITE exige une base (--db / PILOTAGE_DB_PATH / "
                 "APP_DATA_DIR).")
    _conn = dbm.ouvrir(chemin_base)
    from lib_ref_history import resolve_management_period

    # ── Mois par défaut : dernier mois présent dans les tâches enrichies (Lot6a SQLite) ──────────
    if args.mois:
        MONTH = args.mois
    else:
        r = _conn.execute(
            "SELECT MAX(mois) FROM menages_taches_enrichies WHERE mois IS NOT NULL").fetchone()
        MONTH = r[0] if r and r[0] else datetime.date.today().strftime("%Y-%m")

    # ── Référentiels (0029, déjà importés) ─────────────────────────────────────
    ref_int = dbm.lignes(_conn, "ref_intervenants",
        ("intervenant_id", "nom_intervenant", "type_intervenant", "nom_normalise",
         "hostaway_assigneeUserId", "hostaway_mapping_actif"), ordre="intervenant_id")
    ref_clo = dbm.lignes(_conn, "ref_cloture_mensuelle", ("mois", "statut_mois"), ordre="mois")
    cloture = {str(d["mois"])[:7] for d in ref_clo
              if str(d.get("statut_mois") or "").upper() == "CLOTURE" and d.get("mois")}
    mois_historique = MONTH in cloture

    assignee2int = {}    # assigneeUserId -> (intervenant_id, nom, type)
    int_by_id = {}
    for d in ref_int:
        int_by_id[d["intervenant_id"]] = d
        a = d.get("hostaway_assigneeUserId")
        if a is not None and str(d.get("hostaway_mapping_actif") or "").upper() == "OUI":
            assignee2int[a] = (d["intervenant_id"], d.get("nom_intervenant"), d.get("type_intervenant"))
    KNOWN = {norm(d["nom_normalise"]): d["intervenant_id"] for d in ref_int if d.get("nom_normalise")}

    ref_log = dbm.lignes(_conn, "ref_logements",
        ("logement_id", "nom_logement_officiel"), ordre="logement_id")
    gest_rows = dbm.lignes(_conn, "ref_gestion_logements_hist",
        ("gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
         "statut_gestion", "source", "commentaire"), ordre="gestion_id") \
        if dbm.table_presente(_conn, "ref_gestion_logements_hist") else []
    # proprietaire_id resolu par periode (comme lot6a) — REF_Logements n'a pas cette colonne sur le
    # classeur reel ; log_info["proprietaire_id"] doit donc etre calcule, jamais lu tel quel.
    log_info = {}
    for d in ref_log:
        lid = d["logement_id"]
        prop_id = None
        if gest_rows:
            res = resolve_management_period(gest_rows, logement_id=lid,
                                            date_arrivee=MONTH + "-15")
            prop_id = res.value if res.status == "OK" else None
        log_info[lid] = {"nom_logement_officiel": d.get("nom_logement_officiel"),
                         "proprietaire_id": prop_id}

    # ── A. Hostaway Tasks réalisés (completed) du mois — menages_taches_enrichies (0038) ─────────
    t_enr = dbm.lignes(_conn, "menages_taches_enrichies",
        ("task_id", "mois", "logement_id", "status", "statut_menage"), ordre="id")
    _raw_tasks = dbm.lignes(_conn, "hostaway_cleaning_tasks",
        ("task_id", "title", "assignee_user_id"), ordre="id")
    assignee_by_task = {r["task_id"]: r["assignee_user_id"] for r in _raw_tasks}
    title_by_task    = {r["task_id"]: r["title"] for r in _raw_tasks}

    # ── C. Ménages externes déclarés — facture_lignes_menage/detail (0037/0039) ───────────────────
    ext = []
    if dbm.table_presente(_conn, "facture_lignes_menage"):
        # Lot6d est l'ecran de CONTROLE : il COMPTE des menages, il n'additionne aucun montant.
        # Les factures A_CONTROLER sont donc volontairement chargees ici — les masquer ferait
        # disparaitre du rapprochement la facture meme qu'un humain doit examiner. Le filtre par
        # statut s'applique la ou des MONTANTS sont agreges (lot6e, lot6f), pas ici.
        # `statut_facture`/`economique` sont remontes pour que l'aval affiche explicitement
        # « impact economique retenu = 0 » sans redemander la base.
        cur = _conn.execute(
            "SELECT l.facture_id_opaque, l.logement_id, f.fournisseur_id_opaque AS prestataire_id, "
            "l.montant_ttc, d.quantite, f.date_facture, f.statut "
            "FROM facture_lignes_menage l "
            "JOIN factures f ON f.facture_id_opaque = l.facture_id_opaque "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.type_ligne = 'MENAGE_EXTERNE'")
        for facture_id, logement_id, prestataire_id, montant_ttc, quantite, date_facture, statut \
                in cur.fetchall():
            ext.append({
                "mois": str(date_facture or "")[:7], "logement_id": logement_id,
                "prestataire_id": prestataire_id,
                "type_ligne_menage_id": "TLM_001",   # équivalent SQLite : MENAGE_EXTERNE compte
                "nombre_menages": quantite if quantite is not None else 1,
                "montant_ligne_ttc": montant_ttc, "facture_id": facture_id,
                "statut_facture": statut,
                "economique": statut in dbm.STATUTS_FACTURE_COMPTABLES,
            })

    # ── D. Ménages internes déclarés — menages_declarations_internes (0038) ───────────────────────
    decl = dbm.lignes(_conn, "menages_declarations_internes",
        ("mois", "logement_id", "intervenant_id", "nb_menages"), ordre="id")
    m04_alimente = any(str(d.get("mois"))[:7] == MONTH for d in decl)
    src_interne = "SQLITE_menages_declarations_internes"
    _conn.close()

else:
    MONTH = args.mois or "2026-05"

    # ── Référentiels ────────────────────────────────────────────────────────────
    _, ref_int = sh(REF, "REF_Intervenants")
    assignee2int = {}    # assigneeUserId -> (intervenant_id, nom, type)
    int_by_id = {}
    for d in ref_int:
        int_by_id[d["intervenant_id"]] = d
        a = d.get("hostaway_assigneeUserId")
        if a is not None and str(d.get("hostaway_mapping_actif")).upper() == "OUI":
            assignee2int[a] = (d["intervenant_id"], d.get("nom_intervenant"), d.get("type_intervenant"))
    KNOWN = {norm(d["nom_normalise"]): d["intervenant_id"] for d in ref_int if d.get("nom_normalise")}

    _, ref_clo = sh(REF, "REF_Cloture_Mensuelle")
    cloture = {str(d["mois"])[:7] for d in ref_clo if str(d.get("statut_mois")).upper() == "CLOTURE" and d.get("mois")}
    mois_historique = MONTH in cloture          # mois clôturé => non-assignés = INFO historique

    _, ref_log = sh(REF, "REF_Logements")
    log_info = {d["logement_id"]: d for d in ref_log if d.get("logement_id") and str(d["logement_id"]) != "logement_id"}

    # ── A. Hostaway Tasks réalisés (completed) du mois ───────────────────────────
    tf = glob.glob(os.path.join(ROOT, "02_TRAVAIL", "**", "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"), recursive=True)[0]
    _, t_data = sh(tf, "data")
    _, t_enr  = sh(tf, "MASTER_ENRICHI")
    assignee_by_task = {d["task_id"]: d.get("assigneeUserId") for d in t_data}
    title_by_task    = {d["task_id"]: d.get("title") for d in t_data}

def title_names(t):
    n = norm(str(t).split(" - ")[0])
    import re
    return {k for k in KNOWN if re.search(r"\b" + re.escape(k) + r"\b", n)}

tasks = collections.Counter()       # (logement_id, intervenant_id) -> nb réalisés
controls = []
conflits = nonmap_hist = nonmap_futur = 0
for d in t_enr:
    if str(d.get("mois"))[:7] != MONTH:
        continue
    if d.get("statut_menage") != "réalisé":   # completed uniquement
        continue
    tid = d["task_id"]; a = assignee_by_task.get(tid)
    lg = d.get("logement_id")
    if a in assignee2int:
        iid, nom, typ = assignee2int[a]
        # contrôle secondaire title
        tn = title_names(title_by_task.get(tid))
        exp = norm(nom)
        others = {x for x in tn if x != exp}
        if others and exp not in tn:
            conflits += 1
            controls.append({"type": "CONFLIT_TITLE_ASSIGNEE", "niveau": "A_CONTROLER",
                "detail": f"task {tid} assignee->{iid}({nom}) title={sorted(tn)}"})
    elif a in (None, 0):
        if mois_historique:
            iid = "NON_ATTRIBUE"; nonmap_hist += 1
            controls.append({"type": "TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE", "niveau": "INFO",
                "detail": f"task {tid} logement {lg} — historique, ignorée"})
        else:
            iid = "NON_ATTRIBUE"; nonmap_futur += 1
            controls.append({"type": "TASK_FUTURE_SANS_INTERVENANT_ASSIGNE", "niveau": "A_CONTROLER",
                "detail": f"task {tid} logement {lg} — mois ouvert/futur, à assigner"})
    else:
        iid = "ASSIGNEE_NON_MAPPE"
        controls.append({"type": "MENAGE_ASSIGNEE_NON_MAPPE", "niveau": "A_CONTROLER",
            "detail": f"task {tid} assigneeUserId={a} inconnu du référentiel"})
    tasks[(lg, iid)] += 1

# ── C. Ménages externes déclarés (facturés) du mois ──────────────────────────
# `ext` est déjà construit par la branche SQLITE plus haut ; en EXCEL, il faut encore le lire.
if args.source != "SQLITE":
    fc = glob.glob(os.path.join(ROOT, "02_TRAVAIL", "**", "MASTER_FACT_MEN_MenagesExternes.xlsx"), recursive=True)[0]
    _, ext = sh(fc, "MASTER")
ext_cnt = collections.Counter()     # (logement_id, intervenant_id) -> nb ménages
for d in ext:
    if str(d.get("mois"))[:7] != MONTH:
        continue
    if str(d.get("type_ligne_menage_id")) not in ("TLM_001", "TLM_002"):   # compte_comme_menage=OUI
        continue
    q = d.get("nombre_menages") or 0
    m = d.get("montant_ligne_ttc") or 0
    if (q or 0) == 0 and (m or 0) == 0:   # ligne 0€/q0 exclue (INFO)
        controls.append({"type": "EXCLU_VOLUME", "niveau": "INFO",
            "detail": f"facture {d.get('facture_id')} ligne 0€/q0 exclue du comptage"})
        continue
    # Comptee dans le rapprochement (elle existe, un humain doit la voir), mais signalee comme
    # sans effet economique tant que la facture n'est pas validee : lot6e/lot6f l'ignorent.
    if d.get("statut_facture") is not None and not d.get("economique"):
        controls.append({"type": "FACTURE_NON_VALIDEE_HORS_ECONOMIQUE", "niveau": "A_CONTROLER",
            "detail": f"facture {d.get('facture_id')} statut={d.get('statut_facture')} — "
                      f"comptee au rapprochement, impact economique retenu 0 "
                      f"(montant presente {m})"})
    ext_cnt[(d.get("logement_id"), d.get("prestataire_id"))] += q

# ── D. Ménages internes déclarés du mois ─────────────────────────────────────
# En SQLITE, `decl`/`m04_alimente`/`src_interne` sont déjà construits par la branche plus haut.
# Priorité à la source normalisée DRY-RUN (Google Sheet) si présente, sinon M04 réel.
# (M04 réel jamais modifié ici — lecture seule.)
int_cnt = collections.Counter()     # (logement_id, intervenant_id) -> nb ménages
if args.source != "SQLITE":
    m04_alimente = False
    src_interne = "AUCUNE"
    DRY_M04 = os.path.join(NORM_DIR, "MASTER_NORM_Declarations_Internes.xlsx")
    if os.path.exists(DRY_M04):
        src_interne = "NORM_DECLARATIONS_INTERNES"
        _, decl = sh(DRY_M04, "MASTER_NORMALISE")
    else:
        src_interne = "M04_REEL"
        m04f = glob.glob(os.path.join(ROOT, "02_DONNEES_NORMALISEES", "menages", "M04_MENAGES_PowerQuery.xlsx"))[0]
        _, decl = sh(m04f, "MASTER")
for d in decl:
    if str(d.get("mois"))[:7] != MONTH:
        continue
    m04_alimente = True
    int_cnt[(d.get("logement_id"), d.get("intervenant_id"))] += (d.get("nb_menages") or 0)

# ── Tableau comparaison mois × logement × intervenant ────────────────────────
keys = set(tasks) | set(ext_cnt) | set(int_cnt)
def nom_app(lg): return (log_info.get(lg) or {}).get("nom_logement_officiel")
def prop(lg):    return (log_info.get(lg) or {}).get("proprietaire_id")
def i_nom(iid):  return (int_by_id.get(iid) or {}).get("nom_intervenant") or iid
def i_typ(iid):  return (int_by_id.get(iid) or {}).get("type_intervenant")

comp = []
cnt_statut = collections.Counter()
for (lg, iid) in sorted(keys, key=lambda x: (str(x[0]), str(x[1]))):
    nb_t = tasks.get((lg, iid), 0)
    nb_e = ext_cnt.get((lg, iid), 0)
    nb_i = int_cnt.get((lg, iid), 0)
    tot_dec = nb_e + nb_i
    ecart = tot_dec - nb_t
    typ = i_typ(iid)
    statut, code, comm = "VALIDE", "", ""
    if iid == "NON_ATTRIBUE":
        if mois_historique:
            statut, code, comm = "INFO", "TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE", "Tasks non assignées (historique), ignorées pour blocage"
        else:
            statut, code, comm = "A_CONTROLER", "TASK_FUTURE_SANS_INTERVENANT_ASSIGNE", "Tasks non assignées sur mois ouvert/futur"
    elif iid == "ASSIGNEE_NON_MAPPE":
        statut, code, comm = "A_CONTROLER", "MENAGE_ASSIGNEE_NON_MAPPE", "assigneeUserId inconnu du référentiel"
    elif typ == "INTERNE" and nb_t > 0 and nb_i == 0:
        statut, code, comm = "A_CONTROLER", "MENAGE_M04_NON_ALIMENTE", "Tasks Hostaway internes mais M04 vide"
    elif nb_t > 0 and tot_dec < nb_t:
        statut, code, comm = "A_CONTROLER", "MENAGE_TOTAL_ECART_HOSTAWAY", "Tasks Hostaway > déclaré total"
    elif typ == "EXTERNE" and nb_e > nb_t:
        statut, code, comm = "A_CONTROLER", "MENAGE_PRESTATAIRE_ECART_HOSTAWAY", "Facturé externe > tasks Hostaway"
    elif ecart != 0:
        statut, code, comm = "A_CONTROLER", "MENAGE_ECART_NOMBRE", "Écart à expliquer"
    cnt_statut[statut] += 1
    comp.append({"mois": MONTH, "nom_appartement": nom_app(lg), "logement_id": lg, "proprietaire_id": prop(lg),
        "intervenant_id": iid, "nom_intervenant": i_nom(iid), "type_intervenant": typ,
        "source_mapping_hostaway": "REF_Intervenants.hostaway_assigneeUserId",
        "nb_menages_tasks_hostaway_completed": nb_t,
        "nb_menages_declares_externe": nb_e, "nb_menages_declares_interne_m04": nb_i,
        "total_menages_declares": tot_dec, "ecart": ecart,
        "statut_controle": statut, "code_controle": code, "commentaire": comm,
        "ROW_HASH": rowhash(MONTH, lg, iid, nb_t, tot_dec)})

# ── Résumés ──────────────────────────────────────────────────────────────────
def resume(keyf):
    agg = collections.defaultdict(lambda: [0, 0, 0])
    for r in comp:
        k = keyf(r); agg[k][0] += r["nb_menages_tasks_hostaway_completed"]
        agg[k][1] += r["nb_menages_declares_externe"]; agg[k][2] += r["nb_menages_declares_interne_m04"]
    return agg
res_app = resume(lambda r: (r["logement_id"], r["nom_appartement"]))
res_int = resume(lambda r: (r["intervenant_id"], r["nom_intervenant"], r["type_intervenant"]))

# ── SQLite : menages_rapprochement (0038) — remplacement integral (cache derive) ────────────────
if args.sans_sqlite:
    print("[lot6d] --sans-sqlite : menages_rapprochement non ecrit.")
elif chemin_base is None:
    print("[lot6d] Aucune base designee : menages_rapprochement non ecrit.")
else:
    _sql_cols = ["mois", "nom_appartement", "logement_id", "proprietaire_id", "intervenant_id",
                "nom_intervenant", "type_intervenant", "source_mapping_hostaway",
                "nb_menages_tasks_hostaway_completed", "nb_menages_declares_externe",
                "nb_menages_declares_interne_m04", "total_menages_declares", "ecart",
                "statut_controle", "code_controle", "commentaire"]
    _conn = dbm.ouvrir(chemin_base)
    try:
        _conn.execute("DELETE FROM menages_rapprochement WHERE mois = ?", (MONTH,))
        if comp:
            _trous = ", ".join(["?"] * (len(_sql_cols) + 1))
            _conn.executemany(
                f"INSERT INTO menages_rapprochement ({', '.join(_sql_cols)}, run_id) "
                f"VALUES ({_trous})",
                [tuple(r.get(c) for c in _sql_cols) + (args.run_id or None,) for r in comp])
        _conn.commit()
    finally:
        _conn.close()
    print(f"[lot6d] SQLite : menages_rapprochement — {len(comp)} lignes (mois={MONTH})")

if args.sans_excel:
    print("[lot6d] --sans-excel : classeur legacy non ecrit.")
    print(f"[lot6d] mois={MONTH} (clôturé={mois_historique}) — {len(comp)} lignes, "
         f"statuts={dict(cnt_statut)}")
    sys.exit(0)

# ── Écriture ──────────────────────────────────────────────────────────────────
os.makedirs(OUTD, exist_ok=True)
wb = openpyxl.Workbook()
def write(ws, cols, rows):
    ws.append(cols)
    for c in ws[1]: c.font = Font(bold=True); c.fill = PatternFill("solid", fgColor="DDDDDD")
    for r in rows: ws.append([r.get(c) if isinstance(r, dict) else r[i] for i, c in enumerate(cols)])

ws1 = wb.active; ws1.title = "TABLEAU_COMPARAISON"
COLS1 = ["mois","nom_appartement","logement_id","proprietaire_id","intervenant_id","nom_intervenant",
    "type_intervenant","source_mapping_hostaway","nb_menages_tasks_hostaway_completed",
    "nb_menages_declares_externe","nb_menages_declares_interne_m04","total_menages_declares",
    "ecart","statut_controle","code_controle","commentaire"]
write(ws1, COLS1, comp)

ws2 = wb.create_sheet("RESUME_APPARTEMENT")
r2 = [{"mois":MONTH,"nom_appartement":k[1],"logement_id":k[0],"tasks_hostaway_total":v[0],
    "declares_externes_total":v[1],"declares_internes_m04_total":v[2],"total_declares":v[1]+v[2],
    "ecart_total":v[1]+v[2]-v[0],"statut_controle":"VALIDE" if v[1]+v[2]-v[0]==0 else "A_CONTROLER",
    "commentaire":""} for k,v in sorted(res_app.items())]
write(ws2, ["mois","nom_appartement","logement_id","tasks_hostaway_total","declares_externes_total",
    "declares_internes_m04_total","total_declares","ecart_total","statut_controle","commentaire"], r2)

ws3 = wb.create_sheet("RESUME_INTERVENANT")
r3 = [{"mois":MONTH,"intervenant_id":k[0],"nom_intervenant":k[1],"type_intervenant":k[2],
    "tasks_hostaway_total":v[0],"declares_externes_total":v[1],"declares_internes_m04_total":v[2],
    "total_declares":v[1]+v[2],"ecart_total":v[1]+v[2]-v[0],
    "statut_controle":"VALIDE" if v[1]+v[2]-v[0]==0 else "A_CONTROLER","commentaire":""} for k,v in sorted(res_int.items())]
write(ws3, ["mois","intervenant_id","nom_intervenant","type_intervenant","tasks_hostaway_total",
    "declares_externes_total","declares_internes_m04_total","total_declares","ecart_total","statut_controle","commentaire"], r3)

ws4 = wb.create_sheet("CONTROLES")
cc = collections.Counter(c["type"] for c in controls)
write(ws4, ["code_controle","niveau","nb","exemple"],
    [{"code_controle":k,"niveau":next(c["niveau"] for c in controls if c["type"]==k),"nb":n,
      "exemple":next(c["detail"] for c in controls if c["type"]==k)} for k,n in cc.most_common()])
try:
    wb.save(OUT)
except PermissionError:
    OUT = OUT.replace(".xlsx", "_MAJ.xlsx")
    wb.save(OUT)
    print(f"[lot6d] AVERTISSEMENT : sortie originale verrouillée (Excel ouvert) -> écrit dans {os.path.basename(OUT)}")

# ── Rapport console ──────────────────────────────────────────────────────────
print(f"[lot6d] DRY-RUN mois={MONTH} (clôturé={mois_historique}) -> {OUT}")
print(f"  TABLEAU_COMPARAISON : {len(comp)} lignes")
print(f"  statuts : {dict(cnt_statut)}")
print(f"  interne mai : alimenté={m04_alimente} source={src_interne}")
print(f"  conflits title : {conflits} | non assignés histo : {nonmap_hist} | futur : {nonmap_futur}")
print("\n  RESUME_APPARTEMENT:")
for r in r2: print(f"    {r['logement_id']:8} {str(r['nom_appartement'])[:26]:26} tasks={r['tasks_hostaway_total']:3} ext={r['declares_externes_total']:3} m04={r['declares_internes_m04_total']:3} ecart={r['ecart_total']:4} {r['statut_controle']}")
print("\n  RESUME_INTERVENANT:")
for r in r3: print(f"    {str(r['intervenant_id']):16} {str(r['nom_intervenant'])[:10]:10} {str(r['type_intervenant']):8} tasks={r['tasks_hostaway_total']:3} ext={r['declares_externes_total']:3} m04={r['declares_internes_m04_total']:3} ecart={r['ecart_total']:4} {r['statut_controle']}")
print("\n  CONTROLES:")
for k,n in cc.most_common(): print(f"    {k}: {n}")
"""END"""

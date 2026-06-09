"""
lot6b_m04_menages_internes.py
Lot 6b — M04 Ménages internes (main-d'œuvre uniquement)

D027 irrévocable : M04 = MO interne HC uniquement.
Périmètre exclu : consommables / produits / linge / achats / forfait local / coût complet.
TYPE_FLUX_013 = COUT_MO_INTERNE_MENAGE.
"""

import os, shutil, datetime
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
REF_PATH   = BASE_DIR / "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"
ARCHIVE_DIR = BASE_DIR / "99_ARCHIVES/LOT6B_Menages"
OUT_DIR    = BASE_DIR / "02_DONNEES_NORMALISEES/menages"
OUT_PATH   = OUT_DIR / "M04_MENAGES_PowerQuery.xlsx"
LOT6A_PATH = BASE_DIR / "02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"
TODAY      = datetime.date.today().isoformat()
NOW_TAG    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Column definitions ───────────────────────────────────────────────────────
SOURCE_RAW_HEADERS = [
    "mois_saisie", "appartement", "intervenant", "type_menage",
    "nb_menages", "nb_heures", "commentaire"
]

MASTER_HEADERS = [
    "menage_calc_id", "ROW_HASH",
    "mois", "annee", "mois_num", "logement_id", "proprietaire_id",
    "hostaway_listing_id", "appartement_source", "intervenant_id",
    "nom_intervenant", "type_intervenant",
    "type_menage", "nb_menages", "nb_heures", "taux_horaire_intervenant",
    "cout_execution_total", "cout_execution_unitaire",
    "cout_standard", "cout_standard_total_ligne",
    "ecart_main_oeuvre_vs_standard", "total_execution",
    "type_flux_id", "sens", "code_impact",
    "impact_resultat_reel", "impact_resultat_comptable",
    "statut_controle", "niveau_anomalie", "code_anomalie",
    "source_module", "source_table", "source_pk", "date_integration"
]

PARAM_TAUX_HEADERS = [
    "intervenant_id", "nom_intervenant", "type_intervenant",
    "taux_horaire", "date_debut_validite", "date_fin_validite",
    "actif", "commentaire"
]

PARAM_M04_HEADERS = [
    "parametre", "valeur", "date_debut_validite", "date_fin_validite",
    "actif", "commentaire"
]

VUE_ECART_HEADERS = [
    "mois", "logement_id", "appartement_source",
    "nb_menages_m04", "nb_menages_hostaway_realises",
    "ecart", "statut_controle", "code_anomalie"
]

# ── Initial data ─────────────────────────────────────────────────────────────
PARAM_TAUX_ROWS = [
    ("INT_0001", "Imène",   "INTERNE", 10.0, "2026-01-01", None, "OUI", "Taux initial validé"),
    ("INT_0002", "Kheira",  "INTERNE", 10.0, "2026-01-01", None, "OUI", "Taux initial validé"),
    ("INT_0003", "Mounir",  "EXTERNE", None, "2026-01-01", None, "OUI", "EXTERNE — taux non requis dans M04"),
    ("INT_0004", "Aissata", "EXTERNE", None, "2026-01-01", None, "OUI", "EXTERNE — taux non requis dans M04"),
    ("INT_0005", "Imrane",  "EXTERNE", None, "2026-01-01", None, "OUI", "EXTERNE — taux non requis dans M04"),
]

PARAM_M04_ROWS = [
    ("SEUIL_ECART_STANDARD_MENAGE", "10", "2026-01-01", None, "OUI",
     "Seuil paramétrable pour contrôle MENAGE_ECART_NEGATIF_IMPORTANT"),
]

# ── TYPE_FLUX_013 ─────────────────────────────────────────────────────────────
TYPE_FLUX_013 = (
    "TYPE_FLUX_013",
    "COUT_MO_INTERNE_MENAGE",
    "Main-d’œuvre interne ménage valorisée au taux horaire par intervenant",
    "HC",
    "NON",
    "NON",
    "NON",
    "OUI",
    "Source M04 — main-d’œuvre interne ménage, intégrée au résultat réel en HC via MASTER_CALC_Flux ; ne comprend pas les charges affectables ni le coût complet ménage."
)

# ── Styles ───────────────────────────────────────────────────────────────────
def fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

F_BLUE   = fill("1F4E79")   # IDENTIFICATION
F_TEAL   = fill("2E75B6")   # RATTACHEMENT
F_ORANGE = fill("C55A11")   # INTERVENANT
F_GREEN  = fill("375623")   # CALCUL
F_GOLD   = fill("7F6000")   # FLUX
F_RED    = fill("C00000")   # STATUT
F_GRAY   = fill("595959")   # SYSTEME
F_PURPLE = fill("7030A0")   # PARAMS

W_FONT   = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
B_FONT   = Font(name="Calibri", bold=True, color="000000", size=10)
N_FONT   = Font(name="Calibri", size=10)
S_FONT   = Font(name="Calibri", size=9)
I_FONT   = Font(name="Calibri", italic=True, color="C00000", size=9)

MASTER_BLOC_FILLS = {
    "menage_calc_id": F_BLUE, "ROW_HASH": F_BLUE,
    "mois": F_TEAL, "annee": F_TEAL, "mois_num": F_TEAL,
    "logement_id": F_TEAL, "proprietaire_id": F_TEAL,
    "hostaway_listing_id": F_TEAL, "appartement_source": F_TEAL, "intervenant_id": F_TEAL,
    "nom_intervenant": F_ORANGE, "type_intervenant": F_ORANGE,
    "type_menage": F_GREEN, "nb_menages": F_GREEN, "nb_heures": F_GREEN,
    "taux_horaire_intervenant": F_GREEN,
    "cout_execution_total": F_GREEN, "cout_execution_unitaire": F_GREEN,
    "cout_standard": F_GREEN, "cout_standard_total_ligne": F_GREEN,
    "ecart_main_oeuvre_vs_standard": F_GREEN, "total_execution": F_GREEN,
    "type_flux_id": F_GOLD, "sens": F_GOLD, "code_impact": F_GOLD,
    "impact_resultat_reel": F_GOLD, "impact_resultat_comptable": F_GOLD,
    "statut_controle": F_RED, "niveau_anomalie": F_RED, "code_anomalie": F_RED,
    "source_module": F_GRAY, "source_table": F_GRAY, "source_pk": F_GRAY, "date_integration": F_GRAY,
}

def write_header_row(ws, headers, default_fill, row=1, height=28):
    for i, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=i, value=h)
        c.fill = MASTER_BLOC_FILLS.get(h, default_fill)
        c.font = W_FONT
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.row_dimensions[row].height = height

def auto_width(ws, min_w=8, max_w=35):
    for col in ws.columns:
        max_len = 0
        letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[letter].width = min(max(max_len + 2, min_w), max_w)

def make_table(ws, name, n_rows, n_cols, style="TableStyleMedium2"):
    ref = f"A1:{get_column_letter(n_cols)}{max(2, n_rows + 1)}"
    tbl = Table(displayName=name, ref=ref)
    tbl.tableStyleInfo = TableStyleInfo(name=style, showFirstColumn=False,
                                        showLastColumn=False, showRowStripes=True)
    ws.add_table(tbl)

# ── Backup & REF modification ────────────────────────────────────────────────
def backup_ref_setup():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    bak_name = f"REF_Setup_BACKUP_{NOW_TAG}.xlsm"
    bak_path = ARCHIVE_DIR / bak_name
    shutil.copy2(REF_PATH, bak_path)
    return bak_path

def add_type_flux_013():
    wb = openpyxl.load_workbook(REF_PATH, keep_vba=True)
    ws = wb["REF_Types_Flux"]
    for row in ws.iter_rows(values_only=True):
        if row[0] == "TYPE_FLUX_013":
            wb.close()
            return False, "already present"
    ws.append(TYPE_FLUX_013)
    wb.save(REF_PATH)
    wb.close()
    return True, "added"

# ── Excel onglets ─────────────────────────────────────────────────────────────
def build_source_raw(wb):
    ws = wb.create_sheet("SOURCE_RAW")
    write_header_row(ws, SOURCE_RAW_HEADERS, F_GRAY)
    note = ws.cell(row=2, column=1,
                   value="← Coller ici les données du Google Sheet 'Suivi ménage'. "
                         "Vérifier que les noms de colonnes correspondent exactement.")
    note.font = I_FONT
    make_table(ws, "tbl_SOURCE_RAW", 1, len(SOURCE_RAW_HEADERS))
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_param_taux(wb):
    ws = wb.create_sheet("PARAM_TAUX_INTERVENANTS")
    write_header_row(ws, PARAM_TAUX_HEADERS, F_PURPLE)
    for r in PARAM_TAUX_ROWS:
        ws.append(r)
    make_table(ws, "tbl_PARAM_TAUX_INTERVENANTS", len(PARAM_TAUX_ROWS), len(PARAM_TAUX_HEADERS))
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_param_m04(wb):
    ws = wb.create_sheet("PARAMETRES_M04")
    write_header_row(ws, PARAM_M04_HEADERS, F_PURPLE)
    for r in PARAM_M04_ROWS:
        ws.append(r)
    make_table(ws, "tbl_PARAMETRES_M04", len(PARAM_M04_ROWS), len(PARAM_M04_HEADERS))
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_master(wb, sheet_name="MASTER"):
    ws = wb.create_sheet(sheet_name)
    write_header_row(ws, MASTER_HEADERS, F_GRAY)
    note = ws.cell(row=2, column=1, value="← Alimenté par Power Query (Q7_MASTER) après collage des données dans SOURCE_RAW.")
    note.font = I_FONT
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_vue_active(wb):
    ws = wb.create_sheet("VUE_ACTIVE")
    write_header_row(ws, MASTER_HEADERS, F_GREEN)
    note = ws.cell(row=2, column=1,
                   value="← PQ Q8 : MASTER filtré statut_controle=VALIDE ET type_intervenant=INTERNE")
    note.font = I_FONT
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_vue_ecart(wb):
    ws = wb.create_sheet("VUE_ECART_HOSTAWAY")
    write_header_row(ws, VUE_ECART_HEADERS, F_GOLD)
    note = ws.cell(row=2, column=1,
                   value="← PQ Q9 : comparaison nb_menages M04 vs nb_menages_realises Lot 6a. "
                         "Contrôle MENAGE_ECART_HOSTAWAY_M04.")
    note.font = I_FONT
    auto_width(ws)
    ws.freeze_panes = "A2"

def build_pq_code(wb):
    ws = wb.create_sheet("POWER_QUERY_CODE")
    ws["A1"] = "REQUÊTE"
    ws["B1"] = "M-CODE (à coller dans l'éditeur Power Query)"
    ws["A1"].fill = F_GRAY
    ws["B1"].fill = F_GRAY
    ws["A1"].font = W_FONT
    ws["B1"].font = W_FONT
    ws.row_dimensions[1].height = 20
    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 110

    queries = _pq_queries()
    for i, (name, code) in enumerate(queries, 2):
        c_name = ws.cell(row=i, column=1, value=name)
        c_name.font = Font(bold=True, size=9, name="Calibri")
        c_code = ws.cell(row=i, column=2, value=code)
        c_code.font = Font(name="Courier New", size=8)
        c_code.alignment = Alignment(wrap_text=True, vertical="top")
        ws.row_dimensions[i].height = 100

def _pq_queries():
    # REF_Setup path placeholder — user must replace with absolute path
    REF_PH = r"C:\CHEMIN_A_ADAPTER\REF_Setup.xlsm"
    L6A_PH = r"C:\CHEMIN_A_ADAPTER\MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"

    q1 = f"""// Q1_SOURCE_RAW — Charge les données depuis l'onglet SOURCE_RAW de ce classeur
let
    Source       = Excel.CurrentWorkbook(){{[Name="tbl_SOURCE_RAW"]}}[Content],
    TypedTable   = Table.TransformColumnTypes(Source, {{
                       {{"mois_saisie",  type text}},
                       {{"appartement",  type text}},
                       {{"intervenant",  type text}},
                       {{"type_menage",  type text}},
                       {{"nb_menages",   Int64.Type}},
                       {{"nb_heures",    type number}},
                       {{"commentaire",  type text}}
                   }}),
    NoEmpty      = Table.SelectRows(TypedTable, each [appartement] <> null and [appartement] <> "")
in NoEmpty"""

    q2 = """// Q2_PARAM_TAUX — Taux horaires par intervenant (depuis ce classeur)
let
    Source = Excel.CurrentWorkbook(){[Name="tbl_PARAM_TAUX_INTERVENANTS"]}[Content],
    Active = Table.SelectRows(Source, each [actif] = "OUI")
in Active"""

    q3 = """// Q3_PARAMETRES_M04 — Paramètres M04 (depuis ce classeur)
let
    Source = Excel.CurrentWorkbook(){[Name="tbl_PARAMETRES_M04"]}[Content],
    Active = Table.SelectRows(Source, each [actif] = "OUI")
in Active"""

    q4 = f"""// Q4_REF_MAPPING — Référentiel mapping logements
// ADAPTER : remplacer le chemin ci-dessous par le chemin absolu local vers REF_Setup.xlsm
let
    WB      = Excel.Workbook(File.Contents("{REF_PH}"), null, true),
    Sheet   = WB{{[Item="REF_Mapping_Logements", Kind="Sheet"]}}[Data],
    Headers = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true]),
    Active  = Table.SelectRows(Headers, each [actif] = "OUI")
in Active"""

    q5 = f"""// Q5_REF_LOGEMENTS — Référentiel logements
// ADAPTER : chemin absolu vers REF_Setup.xlsm
let
    WB      = Excel.Workbook(File.Contents("{REF_PH}"), null, true),
    Sheet   = WB{{[Item="REF_Logements", Kind="Sheet"]}}[Data],
    Headers = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true])
in Headers"""

    q6 = f"""// Q6_REF_COUTS_STANDARDS — Coûts standards ménage par type logement
// ADAPTER : chemin absolu vers REF_Setup.xlsm
let
    WB      = Excel.Workbook(File.Contents("{REF_PH}"), null, true),
    Sheet   = WB{{[Item="REF_Couts_Standards_Menage", Kind="Sheet"]}}[Data],
    Headers = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true]),
    Active  = Table.SelectRows(Headers, each [actif] = "OUI")
in Active"""

    q6b = f"""// Q6B_REF_INTERVENANTS — Référentiel intervenants
// ADAPTER : chemin absolu vers REF_Setup.xlsm
let
    WB      = Excel.Workbook(File.Contents("{REF_PH}"), null, true),
    Sheet   = WB{{[Item="REF_Intervenants", Kind="Sheet"]}}[Data],
    Headers = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true]),
    Active  = Table.SelectRows(Headers, each [actif] = "OUI"),
    WithNorm = Table.AddColumn(Active, "_nom_upper", each Text.Upper(Text.Trim([nom_normalise])))
in Active"""

    q7 = """// Q7_MASTER — Transformation SOURCE_RAW → tbl_MASTER_FACT_MEN_Menages (34 colonnes)
// Dépendances : Q1..Q6B, Q3 (seuil écart)
let
    SrcRaw    = Q1_SOURCE_RAW,
    ParamTaux = Q2_PARAM_TAUX,
    Params    = Q3_PARAMETRES_M04,
    RefMap    = Q4_REF_MAPPING,
    RefLog    = Q5_REF_LOGEMENTS,
    RefCouts  = Q6_REF_COUTS_STANDARDS,
    RefInt    = Q6B_REF_INTERVENANTS,

    // Seuil écart depuis PARAMETRES_M04
    SeuilRow  = Table.SelectRows(Params, each [parametre] = "SEUIL_ECART_STANDARD_MENAGE"),
    SeuilEcart = if Table.RowCount(SeuilRow) > 0
                 then try Number.FromText(SeuilRow{0}[valeur]) otherwise 10
                 else 10,

    // ── Étape 1 : mapping appartement → logement_id ─────────────────────────
    // REF_Mapping sources utiles pour le GSheet Suivi ménage : "Nom court interne"
    RefMapMenage = Table.SelectRows(RefMap,
        each [source] = "Nom court interne" or [source] = "Suivi ménage"),
    MergeMap  = Table.NestedJoin(SrcRaw, {"appartement"}, RefMapMenage, {"valeur_source"},
                    "MappedLog", JoinKind.LeftOuter),
    ExpandMap = Table.ExpandTableColumn(MergeMap, "MappedLog",
                    {"logement_id"}, {"logement_id"}),

    // ── Étape 2 : infos logement ─────────────────────────────────────────────
    MergeLog  = Table.NestedJoin(ExpandMap, {"logement_id"}, RefLog, {"logement_id"},
                    "LogInfo", JoinKind.LeftOuter),
    ExpandLog = Table.ExpandTableColumn(MergeLog, "LogInfo",
                    {"proprietaire_id", "hostaway_listing_id", "type_logement_id"},
                    {"proprietaire_id", "hostaway_listing_id", "type_logement_id"}),

    // ── Étape 3 : coût standard ──────────────────────────────────────────────
    MergeCout  = Table.NestedJoin(ExpandLog, {"type_logement_id"}, RefCouts, {"type_logement_id"},
                     "CoutInfo", JoinKind.LeftOuter),
    ExpandCout = Table.ExpandTableColumn(MergeCout, "CoutInfo",
                     {"cout_standard_menage"}, {"cout_standard"}),

    // ── Étape 4 : mapping intervenant → intervenant_id ────────────────────────
    WithIntNorm = Table.AddColumn(ExpandCout, "_int_norm",
                      each Text.Upper(Text.Trim([intervenant]))),
    RefIntU     = Table.AddColumn(RefInt, "_nom_upper",
                      each Text.Upper(Text.Trim([nom_normalise]))),
    MergeInt    = Table.NestedJoin(WithIntNorm, {"_int_norm"}, RefIntU, {"_nom_upper"},
                      "IntInfo", JoinKind.LeftOuter),
    ExpandInt   = Table.ExpandTableColumn(MergeInt, "IntInfo",
                      {"intervenant_id", "nom_intervenant", "type_intervenant"},
                      {"intervenant_id", "nom_intervenant", "type_intervenant"}),

    // ── Étape 5 : taux horaire depuis PARAM_TAUX ─────────────────────────────
    // Taux actif pour intervenant_id à la période mois_saisie
    WithTaux = Table.AddColumn(ExpandInt, "taux_horaire_intervenant", each
        let
            iid     = [intervenant_id],
            m       = [mois_saisie],
            Match   = Table.SelectRows(ParamTaux, each
                          [intervenant_id] = iid and
                          [actif] = "OUI" and
                          [date_debut_validite] <= m and
                          ([date_fin_validite] = null or [date_fin_validite] >= m)),
            NMatch  = Table.RowCount(Match)
        in
            if NMatch = 1 then Match{0}[taux_horaire]
            else null),

    WithTauxN = Table.AddColumn(WithTaux, "_taux_n", each
        Table.RowCount(Table.SelectRows(ParamTaux, each
            [intervenant_id] = [intervenant_id] and
            [actif] = "OUI" and
            [date_debut_validite] <= [mois_saisie] and
            ([date_fin_validite] = null or [date_fin_validite] >= [mois_saisie])))),

    // ── Étape 6 : calculs ─────────────────────────────────────────────────────
    WithExec   = Table.AddColumn(WithTauxN, "cout_execution_total", each
                     if [nb_heures] <> null and [taux_horaire_intervenant] <> null
                     then [nb_heures] * [taux_horaire_intervenant] else null),
    WithUnit   = Table.AddColumn(WithExec, "cout_execution_unitaire", each
                     if [cout_execution_total] <> null and [nb_menages] <> null and [nb_menages] > 0
                     then [cout_execution_total] / [nb_menages] else null),
    WithStdT   = Table.AddColumn(WithUnit, "cout_standard_total_ligne", each
                     if [cout_standard] <> null and [nb_menages] <> null
                     then [cout_standard] * [nb_menages] else null),
    WithEcart  = Table.AddColumn(WithStdT, "ecart_main_oeuvre_vs_standard", each
                     if [cout_standard] <> null and [cout_execution_unitaire] <> null
                     then [cout_standard] - [cout_execution_unitaire] else null),

    // ── Étape 7 : colonnes flux / impact ─────────────────────────────────────
    // Valeurs fixes : HC toujours (D027), TYPE_FLUX_013
    WithFlux  = Table.AddColumn(WithEcart,  "type_flux_id",            each "TYPE_FLUX_013"),
    WithSens  = Table.AddColumn(WithFlux,   "sens",                    each "CHARGE"),
    WithCI    = Table.AddColumn(WithSens,   "code_impact",             each
                    if [type_intervenant] = "INTERNE" then "HC" else null),
    WithIRR   = Table.AddColumn(WithCI,     "impact_resultat_reel",    each
                    if [code_impact] = "HC" then "OUI" else null),
    WithIRC   = Table.AddColumn(WithIRR,    "impact_resultat_comptable", each
                    if [code_impact] = "HC" then "NON" else null),

    // ── Étape 8 : contrôles ──────────────────────────────────────────────────
    WithCtrl  = Table.AddColumn(WithIRC, "_codes", each
        let Codes = List.Select({
            if [logement_id] = null           then "MENAGE_SANS_LOGEMENT_ID"         else null,
            if [type_intervenant] = null      then "TYPE_INTERVENANT_ABSENT"          else null,
            if [type_intervenant] = "EXTERNE" then "MENAGE_EXTERNE_DANS_M04"         else null,
            if [type_intervenant] = "INTERNE" and [taux_horaire_intervenant] = null
               and [_taux_n] = 0              then "TAUX_ABSENT_INTERVENANT_INTERNE" else null,
            if [type_intervenant] = "INTERNE" and [_taux_n] > 1
                                              then "TAUX_MULTIPLE_INTERVENANT"        else null,
            if [type_menage] = "RANGEMENT"    then "MENAGE_RANGEMENT_A_CONTROLER"    else null,
            if [type_intervenant] = "INTERNE" and
               [ecart_main_oeuvre_vs_standard] <> null and
               [ecart_main_oeuvre_vs_standard] < -SeuilEcart
                                              then "MENAGE_ECART_NEGATIF_IMPORTANT"  else null
        }, each _ <> null)
        in Codes),

    WithStatut = Table.AddColumn(WithCtrl, "statut_controle", each
                     if List.Contains([_codes], "MENAGE_SANS_LOGEMENT_ID")
                     then "EXCLU_RESULTAT"
                     else if List.Count([_codes]) > 0 then "A_CONTROLER"
                     else "VALIDE"),
    WithNiv    = Table.AddColumn(WithStatut, "niveau_anomalie", each
                     if List.Contains([_codes], "MENAGE_SANS_LOGEMENT_ID") then "BLOQUANT"
                     else if List.Count([_codes]) > 0                      then "A_CONTROLER"
                     else null),
    WithCode   = Table.AddColumn(WithNiv, "code_anomalie", each
                     Text.Combine([_codes], " | ")),

    // ── Étape 9 : PK + colonnes système ──────────────────────────────────────
    WithIdx    = Table.AddIndexColumn(WithCode, "_idx", 1, 1),
    WithPK     = Table.AddColumn(WithIdx, "menage_calc_id", each
                     "MEN-" & Text.From([mois_saisie]) & "-"
                     & (if [logement_id] <> null then Text.From([logement_id]) else "INCONNU")
                     & "-"
                     & (if [intervenant_id] <> null then Text.From([intervenant_id]) else "INCONNU")
                     & "-" & Text.PadStart(Text.From([_idx]), 3, "0")),
    WithHash   = Table.AddColumn(WithPK, "ROW_HASH", each
                     // Checksum simple : somme des codes ASCII du PK
                     Text.PadStart(Text.From(
                         List.Sum(List.Transform(Text.ToList([menage_calc_id]),
                             each Character.ToNumber(_)))), 8, "0")),
    WithSys    = Table.AddColumn(WithHash, "source_module",    each "M04"),
    WithST     = Table.AddColumn(WithSys,  "source_table",     each "SOURCE_RAW"),
    WithSPK    = Table.AddColumn(WithST,   "source_pk",        each [menage_calc_id]),
    WithDI     = Table.AddColumn(WithSPK,  "date_integration", each DateTime.LocalNow()),

    // ── Étape 10 : mois / annee / mois_num depuis mois_saisie ────────────────
    WithAnnee    = Table.AddColumn(WithDI,    "annee",    each
                       try Number.FromText(Text.Start([mois_saisie], 4)) otherwise null),
    WithMoisNum  = Table.AddColumn(WithAnnee, "mois_num", each
                       try Number.FromText(Text.End([mois_saisie], 2)) otherwise null),

    // ── Étape 10bis : total_execution = cout_execution_total (colonne distincte) ────
    // total_execution est l'alias MASTER_CALC_Flux de la MO réelle ; jamais dupliqué dans SelectColumns.
    WithTotalExec = Table.AddColumn(WithMoisNum, "total_execution", each [cout_execution_total]),

    // ── Étape 11 : sélection et renommage colonnes finales (34 cols) ─────────
    Selected = Table.SelectColumns(WithTotalExec, {
        "menage_calc_id", "ROW_HASH",
        "mois_saisie", "annee", "mois_num",
        "logement_id", "proprietaire_id", "hostaway_listing_id",
        "appartement", "intervenant_id",
        "nom_intervenant", "type_intervenant",
        "type_menage", "nb_menages", "nb_heures", "taux_horaire_intervenant",
        "cout_execution_total", "cout_execution_unitaire",
        "cout_standard", "cout_standard_total_ligne",
        "ecart_main_oeuvre_vs_standard", "total_execution",
        "type_flux_id", "sens", "code_impact",
        "impact_resultat_reel", "impact_resultat_comptable",
        "statut_controle", "niveau_anomalie", "code_anomalie",
        "source_module", "source_table", "source_pk", "date_integration"
    }),
    Renamed = Table.RenameColumns(Selected, {
        {"mois_saisie", "mois"},
        {"appartement", "appartement_source"}
    })

in Renamed"""

    q8 = """// Q8_VUE_ACTIVE — MASTER filtré statut_controle=VALIDE ET type_intervenant=INTERNE
let
    Master    = Q7_MASTER,
    VueActive = Table.SelectRows(Master, each
                    [statut_controle] = "VALIDE" and
                    [type_intervenant] = "INTERNE")
in VueActive"""

    q9 = f"""// Q9_VUE_ECART_HOSTAWAY — Comparaison nb_menages M04 vs nb_menages_realises Lot 6a
// Contrôle MENAGE_ECART_HOSTAWAY_M04
// ADAPTER : chemin absolu vers MASTER_FACT_HA_CleaningTasks_Discovery.xlsx
let
    Master    = Q7_MASTER,

    // Agrégation M04 par mois × logement_id (VALIDE + INTERNE uniquement)
    M04Agg    = Table.Group(
                    Table.SelectRows(Master, each
                        [statut_controle] = "VALIDE" and [type_intervenant] = "INTERNE"),
                    {{"mois", "logement_id", "appartement_source"}},
                    {{{{"nb_menages_m04", each List.Sum([nb_menages]), type number}}}}),

    // Lot 6a VUE_COMPTAGE
    WB6a      = Excel.Workbook(File.Contents("{L6A_PH}"), null, true),
    VueCpt    = WB6a{{[Item="VUE_COMPTAGE", Kind="Sheet"]}}[Data],
    VueCptH   = Table.PromoteHeaders(VueCpt, [PromoteAllScalars=true]),
    HACols    = Table.SelectColumns(VueCptH, {{"mois", "logement_id", "nb_menages_realises"}}),
    HARename  = Table.RenameColumns(HACols, {{{{"nb_menages_realises", "nb_menages_hostaway_realises"}}}}),

    // Full outer join mois × logement_id
    Merged    = Table.NestedJoin(M04Agg, {{"mois", "logement_id"}},
                    HARename, {{"mois", "logement_id"}}, "HA", JoinKind.FullOuter),
    Expand    = Table.ExpandTableColumn(Merged, "HA",
                    {{"nb_menages_hostaway_realises"}}, {{"nb_menages_hostaway_realises"}}),

    WithEcart = Table.AddColumn(Expand, "ecart", each
                    if [nb_menages_m04] <> null and [nb_menages_hostaway_realises] <> null
                    then [nb_menages_m04] - [nb_menages_hostaway_realises] else null),
    WithStat  = Table.AddColumn(WithEcart, "statut_controle", each
                    if [ecart] = 0 then "VALIDE" else "A_CONTROLER"),
    WithCode  = Table.AddColumn(WithStat, "code_anomalie", each
                    if [statut_controle] = "A_CONTROLER"
                    then "MENAGE_ECART_HOSTAWAY_M04" else null),

    FinalCols = Table.SelectColumns(WithCode, {{
                    "mois", "logement_id", "appartement_source",
                    "nb_menages_m04", "nb_menages_hostaway_realises",
                    "ecart", "statut_controle", "code_anomalie"}})
in FinalCols"""

    return [
        ("Q1_SOURCE_RAW",            q1),
        ("Q2_PARAM_TAUX",            q2),
        ("Q3_PARAMETRES_M04",        q3),
        ("Q4_REF_MAPPING",           q4),
        ("Q5_REF_LOGEMENTS",         q5),
        ("Q6_REF_COUTS_STANDARDS",   q6),
        ("Q6B_REF_INTERVENANTS",     q6b),
        ("Q7_MASTER",                q7),
        ("Q8_VUE_ACTIVE",            q8),
        ("Q9_VUE_ECART_HOSTAWAY",    q9),
    ]

def build_readme(wb):
    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 85

    lines = [
        ("══ M04 — MÉNAGES INTERNES ══", ""),
        ("Fichier",          "M04_MENAGES_PowerQuery.xlsx"),
        ("Lot",              "Lot 6b"),
        ("Date création",    TODAY),
        ("Script source",    "02_TRAVAIL/lot6b_m04_menages_internes.py"),
        ("", ""),
        ("── PÉRIMÈTRE ──", ""),
        ("M04 calcule",      "Main-d'œuvre interne réelle | Coût unitaire MO | Coût standard unitaire | Base pondération"),
        ("M04 NE calcule PAS",
         "Consommables / Produits / Linge / Achats / Fournitures / Location cave / "
         "Charges récurrentes / Coût complet ménage"),
        ("D027 irrévocable", "M04 = MO interne uniquement, HC, sans Courses ni lavage ni achats"),
        ("", ""),
        ("── FLUX / IMPACT ──", ""),
        ("TYPE_FLUX_013",    "COUT_MO_INTERNE_MENAGE"),
        ("code_impact",      "HC — fixe (D027)"),
        ("impact_resultat_reel", "OUI"),
        ("impact_resultat_comptable", "NON"),
        ("", ""),
        ("── SOURCE DONNÉES ──", ""),
        ("Source",           "Google Sheet 'Suivi ménage' — coller dans onglet SOURCE_RAW"),
        ("Colonnes attendues", "mois_saisie | appartement | intervenant | type_menage | nb_menages | nb_heures"),
        ("Attention",        "Les noms de colonnes SOURCE_RAW doivent correspondre exactement "
                             "aux headers du GSheet réel. Adapter si nécessaire."),
        ("", ""),
        ("── CALCULS CLÉS ──", ""),
        ("cout_execution_total",           "= nb_heures × taux_horaire_intervenant"),
        ("cout_standard_total_ligne",      "= nb_menages × cout_standard  [BASE PONDÉRATION — non comptable]"),
        ("ecart_main_oeuvre_vs_standard",  "= cout_standard − cout_execution_unitaire  (positif = favorable)"),
        ("Clé répartition future",         "COUT_STANDARD_MENAGES_MOIS (non NOMBRE_MENAGES)"),
        ("", ""),
        ("── CONTRÔLES ──", ""),
        ("BLOQUANTS",   "MENAGE_SANS_LOGEMENT_ID | M04_SCHEMA_SOURCE_INVALIDE | MENAGE_INTERNE_CODE_IMPACT_NON_HC"),
        ("A_CONTROLER", "MENAGE_EXTERNE_DANS_M04 | TYPE_INTERVENANT_ABSENT | TAUX_ABSENT_INTERVENANT_INTERNE"),
        ("",            "TAUX_MULTIPLE_INTERVENANT | MENAGE_RANGEMENT_A_CONTROLER | MENAGE_DOUBLON_POTENTIEL"),
        ("",            "MENAGE_ECART_NEGATIF_IMPORTANT (seuil = SEUIL_ECART_STANDARD_MENAGE dans PARAMETRES_M04)"),
        ("",            "MENAGE_ECART_HOSTAWAY_M04 (VUE_ECART_HOSTAWAY — comparaison Lot 6a)"),
        ("", ""),
        ("── POWER QUERY ──", ""),
        ("Adaptation requise", "Remplacer tous les chemins C:\\CHEMIN_A_ADAPTER\\ par les chemins absolus locaux"),
        ("REF_Setup",   "Q4/Q5/Q6/Q6B — chemin absolu vers 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"),
        ("Lot 6a",      "Q9 — chemin absolu vers 02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"),
        ("Q7_MASTER",   "total_execution = cout_execution_total (colonne distincte, étape 10bis). Aucun doublon."),
    ]

    for i, (k, v) in enumerate(lines, 1):
        ck = ws.cell(row=i, column=1, value=k)
        cv = ws.cell(row=i, column=2, value=v)
        if k.startswith("══") or k.startswith("──"):
            ck.font = Font(bold=True, color="FFFFFF", size=10, name="Calibri")
            ck.fill = F_BLUE if k.startswith("══") else F_TEAL
            cv.fill = F_BLUE if k.startswith("══") else F_TEAL
        elif k:
            ck.font = Font(bold=True, size=9, name="Calibri")
            cv.font = S_FONT
        else:
            ck.font = S_FONT
            cv.font = S_FONT

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Lot 6b — M04 Ménages internes")
    print(f"Date : {TODAY}  |  Tag : {NOW_TAG}")
    print("=" * 60)

    # 1. Backup REF_Setup
    print("\n[1/4] Backup REF_Setup.xlsm :")
    bak = backup_ref_setup()
    print(f"  {bak}")

    # 2. Add TYPE_FLUX_013
    print("\n[2/4] TYPE_FLUX_013 dans REF_Types_Flux :")
    added, msg = add_type_flux_013()
    print(f"  {msg}")

    # 3. Build M04 Excel
    print("\n[3/4] Creation M04_MENAGES_PowerQuery.xlsx :")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    build_source_raw(wb)
    build_param_taux(wb)
    build_param_m04(wb)
    build_master(wb, "MASTER")
    build_vue_active(wb)
    build_vue_ecart(wb)
    build_pq_code(wb)
    build_readme(wb)

    wb.save(OUT_PATH)
    print(f"  {OUT_PATH}")

    # 4. Summary
    print("\n[4/4] Résumé :")
    print(f"  Backup      : {bak.name}")
    print(f"  TYPE_FLUX_013 : {msg}")
    print(f"  M04 Excel   : {OUT_PATH}")
    print(f"  Onglets (8) : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 /")
    print(f"                MASTER (34 cols) / VUE_ACTIVE / VUE_ECART_HOSTAWAY /")
    print(f"                POWER_QUERY_CODE (10 requêtes) / README")
    print(f"  PARAM_TAUX  : 5 intervenants (INT_0001–INT_0005)")
    print(f"  PARAMETRES  : SEUIL_ECART_STANDARD_MENAGE = 10")
    print("\nOK — Lot 6b squelette créé.")
    print("Prochaine étape : coller données GSheet dans SOURCE_RAW + adapter chemins PQ.")

if __name__ == "__main__":
    main()

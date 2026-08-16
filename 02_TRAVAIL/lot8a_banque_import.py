#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lot8a_banque_import.py
Lot 8a — Import & normalisation bancaire Crédit Mutuel
Ingère un export de la banque actuelle depuis 01_SOURCES_BRUTES/Banque/
Produit 02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx

Deux formats d'entrée acceptés, convergeant vers le même raw_rows canonique (D-8a-FORMAT) :
  FORMAT_CREDIT_MUTUEL_NATIF — export brut historique, feuille `Cpt ...` (comportement inchangé).
  FORMAT_RELEVE_CONSOLIDE    — fusion outillée de plusieurs relevés successifs, feuille
                               `Mouvements` uniquement comme source économique (Synthese/Mensuel/
                               Controles/Sources ne génèrent jamais de mouvement, elles ne
                               vérifient que des totaux déjà calculés par le fichier lui-même).
Détection par présence de feuilles, jamais par nom de fichier. Ni l'un ni l'autre n'est remplacé.

Onglets :
  BRUT_Banque           — copie brute intégrale
  NORM_Banque           — mouvements normalisés + contrôles structurels
  CTRL_A_CONTROLER      — anomalies bloquantes et à contrôler
  LOG_Traitement        — trace du run import (colonne `format_source` : format détecté)
  REF_Cloture_Mensuelle — structure vide des états de mois (alimentée Lot 8c)
  POWER_QUERY_CODE      — documentation technique

ARCHI §13.2–§13.4 / REGLES §6 B1–B10 / Plan Lot 8a.
"""

import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
import hashlib
import os
import sys
import shutil
import re
from datetime import datetime, date

# ── Chemins ────────────────────────────────────────────────────────────────
BASE        = os.path.dirname(os.path.abspath(__file__))
ROOT        = os.path.dirname(BASE)
BANQUE_DIR  = os.path.join(ROOT, '01_SOURCES_BRUTES', 'Banque')
# Overrides d'environnement (tests uniquement) : mêmes conventions que
# PROJECT_ROOT/LOT4A_ENGINE_PYTHON ailleurs dans 02_TRAVAIL — absent => comportement inchangé.
BRUT_FILE   = os.environ.get(
    'LOT8A_BRUT_FILE_OVERRIDE',
    os.path.join(BANQUE_DIR, 'BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx'))
OUT_DIR     = os.path.join(BASE, 'Lot8_Banque')
OUT_FILE    = os.environ.get(
    'LOT8A_OUT_FILE_OVERRIDE',
    os.path.join(OUT_DIR, 'BANQUE_LOT8_IMPORT.xlsx'))
ARCHIVE_DIR = os.path.join(ROOT, '99_ARCHIVES', 'LOT8_Banque')

TS = datetime.now().strftime('%Y%m%d_%H%M%S')

os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ── Constantes ────────────────────────────────────────────────────────────
COMPTE_ID    = 'CM_02211_00021321603'
IMPORT_ID    = 'IMP-BQ-CM-2026-03-001'
DEVISE_REF   = 'EUR'
SHEET_METIER = 'Cpt 02211 00021321603'
HDR_ROW      = 5
DATA_ROW     = 6

# ── Declaration de la source (D-8a-DECLARATION) ─────────────────────────────
# Le lot supposait qu un export bancaire couvrait UN mois, deduit d un NOM_ANNEE/NOM_MOIS codes en
# dur. C etait faux pour le fichier reel : nomme "2026_03", il couvrait en realite dix mois, et le
# controle de periode se declenchait a chaque import sans qu il y ait la moindre anomalie.
#
# Le NOM DECLARE desormais la nature de la source, et le controle verifie ce que cette declaration
# promet - rien de plus :
#
#   BANQUE_ACTUELLE_HISTORIQUE_<debut>_<fin>.xlsx  -> HISTORIQUE : plusieurs mois sont NORMAUX ;
#                                                     on verifie que les bornes annoncees sont
#                                                     tenues.
#   <AAAA>_<MM>_BRUT_...xlsx                       -> MENSUEL : tout mouvement hors du mois
#                                                     annonce est une anomalie.
#   tout autre nom                                 -> INDETERMINE : aucune promesse, donc aucun
#                                                     controle de periode. Mieux vaut ne rien
#                                                     affirmer que d inventer une attente.
SRC_HISTORIQUE = 'HISTORIQUE'
SRC_MENSUEL = 'MENSUEL'
SRC_INDETERMINE = 'INDETERMINE'


def declarer_source(chemin):
    """(source_type, borne_min, borne_max) d apres le NOM du fichier. Bornes None si non annoncees."""
    nom = os.path.basename(chemin)
    m = re.match(r'BANQUE_ACTUELLE_HISTORIQUE_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})',
                 nom, re.IGNORECASE)
    if m:
        try:
            return (SRC_HISTORIQUE,
                    datetime.strptime(m.group(1), '%Y-%m-%d').date(),
                    datetime.strptime(m.group(2), '%Y-%m-%d').date())
        except ValueError:
            return SRC_HISTORIQUE, None, None
    m = re.match(r'(\d{4})[_-](\d{2})[_-]', nom)
    if m:
        annee, mois = int(m.group(1)), int(m.group(2))
        if 1 <= mois <= 12:
            return SRC_MENSUEL, date(annee, mois, 1), None
    return SRC_INDETERMINE, None, None


SOURCE_TYPE, BORNE_DECLAREE_MIN, BORNE_DECLAREE_MAX = declarer_source(BRUT_FILE)

# ── Formats d'entrée acceptés (D-8a-FORMAT) ─────────────────────────────────
# Deux adaptateurs convergent vers le même raw_rows canonique (7 colonnes) : le format natif
# Crédit Mutuel (export brut, feuille `Cpt ...`) et le format « relevé consolidé » (fusion
# outillée de plusieurs relevés successifs, feuilles Synthese/Mouvements/Mensuel/Controles/
# Sources). Ni l'un ni l'autre n'est remplacé — c'est un ajout, pas une migration de contrat.
FORMAT_CREDIT_MUTUEL_NATIF = 'CREDIT_MUTUEL_NATIF'
FORMAT_RELEVE_CONSOLIDE    = 'RELEVE_CONSOLIDE'
FORMAT_INCONNU             = 'INCONNU'

SHEET_CONSOLIDE_MOUVEMENTS  = 'Mouvements'
SHEETS_CONSOLIDE_REQUISES   = {'Mouvements', 'Controles', 'Sources'}
CONSOLIDE_HDR_ROW  = 5
CONSOLIDE_DATA_ROW = 6

BLOQUANT_CODES = {
    'BANQUE_DATE_INEXPLOITABLE',
    'BANQUE_LIGNE_SANS_LIBELLE',
    'BANQUE_DEBIT_CREDIT_VIDES',
    'BANQUE_DEBIT_CREDIT_DOUBLES',
    'BANQUE_MONTANT_NON_NUMERIQUE',
}

# ── Styles ─────────────────────────────────────────────────────────────────
FILL_HDR      = PatternFill('solid', fgColor='1F4E79')
FILL_SEC      = PatternFill('solid', fgColor='2E75B6')
FILL_BLOQUANT = PatternFill('solid', fgColor='C00000')
FILL_ACTRL    = PatternFill('solid', fgColor='ED7D31')
FILL_OK       = PatternFill('solid', fgColor='70AD47')
FILL_INFO     = PatternFill('solid', fgColor='DEEAF1')

FONT_HDR  = Font(bold=True, color='FFFFFF', size=10)
FONT_BOLD = Font(bold=True, size=10)
FONT_NRM  = Font(size=10)
FONT_INFO = Font(italic=True, size=9, color='555555')

ALIGN_CTR  = Alignment(horizontal='center', vertical='center', wrap_text=True)
ALIGN_LEFT = Alignment(vertical='center')


def style_header(ws, row, ncols, fill=None):
    f = fill or FILL_HDR
    for c in range(1, ncols + 1):
        cell = ws.cell(row, c)
        cell.fill = f
        cell.font = FONT_HDR
        cell.alignment = ALIGN_CTR


def set_widths(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


# ── Helpers ────────────────────────────────────────────────────────────────

def normalize_libelle(s):
    if s is None:
        return ''
    s = str(s).strip().upper()
    s = re.sub(r'\s+', ' ', s)
    return s


def parse_date(val):
    """Return (date_obj, True) or (None, False)."""
    if val is None:
        return None, False
    if isinstance(val, datetime):
        return val.date(), True
    if isinstance(val, date):
        return val, True
    s = str(val).strip()
    if not s:
        return None, False
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(s, fmt).date(), True
        except ValueError:
            pass
    return None, False


def to_float(val):
    """Return (float, True) or (None, False)."""
    if val is None or str(val).strip() == '':
        return None, False
    cleaned = str(val).replace(',', '.').replace('\xa0', '').replace(' ', '')
    try:
        return float(cleaned), True
    except ValueError:
        return None, False


def make_hash(parts):
    raw = '|'.join(str(p) for p in parts)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def is_cm_footer_row(cols):
    """Return True for known CM export footer rows to exclude (ARCHI §13.6).

    Handles three CM footer patterns:
    - 'LISTE DE VOS COMPTES' in any cell
    - 'SOLDE AU' prefix in any cell (ligne de solde final, cols Débit/Crédit = texte)
    - Row with no date, no libellé, no *numeric* amount (blank structural row)
    """
    def nonempty(v):
        return v is not None and str(v).strip() != ''

    def is_numeric(v):
        if v is None:
            return False
        if isinstance(v, (int, float)):
            return True
        try:
            float(str(v).replace(',', '.').replace('\xa0', '').replace(' ', ''))
            return True
        except (ValueError, TypeError):
            return False

    # Pattern check on all cells
    for c in cols:
        val = str(c or '').strip().upper()
        if 'LISTE DE VOS COMPTES' in val:
            return True
        if val.startswith('SOLDE AU'):
            return True

    # Structural blank: no date, no libellé, no numeric amount
    lib = str(cols[2] or '').strip().upper() if len(cols) > 2 else ''
    has_date          = nonempty(cols[0]) or (len(cols) > 1 and nonempty(cols[1]))
    has_lib           = bool(lib)
    has_numeric_amount = (
        (len(cols) > 3 and is_numeric(cols[3])) or
        (len(cols) > 4 and is_numeric(cols[4]))
    )
    return not has_date and not has_lib and not has_numeric_amount


def detecter_format(sheetnames):
    """Détecte le format d'entrée sans jamais se fier au nom du fichier (D-8a-FORMAT).

    FORMAT_CREDIT_MUTUEL_NATIF : la feuille métier `Cpt ...` est présente (export brut historique).
    FORMAT_RELEVE_CONSOLIDE    : les 3 feuilles `Mouvements`/`Controles`/`Sources` sont présentes
                                 (fusion outillée de plusieurs relevés successifs).
    FORMAT_INCONNU             : ni l'un ni l'autre — erreur bloquante explicite, jamais un
                                 contournement silencieux.
    """
    noms = set(sheetnames)
    if SHEET_METIER in noms:
        return FORMAT_CREDIT_MUTUEL_NATIF
    if SHEETS_CONSOLIDE_REQUISES <= noms:
        return FORMAT_RELEVE_CONSOLIDE
    return FORMAT_INCONNU


def lire_natif(wb_src):
    """Adaptateur FORMAT_CREDIT_MUTUEL_NATIF — comportement historique, inchangé.

    Retourne (raw_rows, raw_notes, excluded_footer) : raw_rows en 7 colonnes canoniques
    (Date|Valeur|Libellé|Débit|Crédit|Solde|Devise), raw_notes toujours None (pas de provenance
    multi-source à tracer pour ce format).
    """
    ws_src = wb_src[SHEET_METIER]
    raw_rows = []
    raw_notes = []
    excluded_footer = []
    for row in ws_src.iter_rows(min_row=DATA_ROW, values_only=True):
        if all(c is None or str(c).strip() == '' for c in row):
            continue
        padded = list(row) + [None] * max(0, 7 - len(row))
        cols7 = padded[:7]
        if is_cm_footer_row(cols7):
            excluded_footer.append(cols7)
            continue
        raw_rows.append(cols7)
        raw_notes.append(None)
    return raw_rows, raw_notes, excluded_footer


def lire_consolide(wb_src):
    """Adaptateur FORMAT_RELEVE_CONSOLIDE — feuille `Mouvements` UNIQUEMENT (règle §5 de la
    mission) : Synthese/Mensuel/Controles/Sources ne génèrent jamais de mouvement, elles ne
    servent qu'à vérifier des totaux déjà calculés par le fichier lui-même.

    Colonnes réelles (12) : N°|Date opération|Date de valeur|Libellé|Débit|Crédit|Montant net|
    Solde consolidé|Devise|Source du relevé|Mois|Ligne source. Converties vers les 7 colonnes
    canoniques natives. Débit/Crédit y sont TOUJOURS renseignés (0 si inactif, jamais vide) —
    contrairement au format natif où le côté inactif est une cellule vide : sans cette conversion,
    chaque ligne déclencherait à tort BANQUE_DEBIT_CREDIT_DOUBLES (les deux côtés « renseignés »).
    """
    ws_mouv = wb_src[SHEET_CONSOLIDE_MOUVEMENTS]
    raw_rows = []
    raw_notes = []
    excluded_footer = []
    for row in ws_mouv.iter_rows(min_row=CONSOLIDE_DATA_ROW, values_only=True):
        if all(c is None or str(c).strip() == '' for c in row):
            continue
        padded = list(row) + [None] * max(0, 12 - len(row))
        (_num, date_op, date_val, libelle, debit, credit, _montant_net,
         solde, devise, source_releve, _mois, _ligne_source) = padded[:12]

        deb_val, deb_ok = to_float(debit)
        cre_val, cre_ok = to_float(credit)
        deb_norm = deb_val if (deb_ok and deb_val) else None
        cre_norm = cre_val if (cre_ok and cre_val) else None

        cols7 = [date_op, date_val, libelle, deb_norm, cre_norm, solde, devise]
        if is_cm_footer_row(cols7):
            excluded_footer.append(cols7)
            continue
        raw_rows.append(cols7)
        raw_notes.append(str(source_releve) if source_releve else None)
    return raw_rows, raw_notes, excluded_footer


# ==========================================================================
# 1. LECTURE DU FICHIER BRUT (B2 : jamais modifié)
# ==========================================================================

if not os.path.exists(BRUT_FILE):
    print('[ERREUR BLOQUANT] Fichier bancaire brut introuvable :')
    print(f'  {BRUT_FILE}')
    print('Déposez le fichier dans 01_SOURCES_BRUTES/Banque/ puis relancez.')
    sys.exit(1)

print(f'[OK] Source brute : {BRUT_FILE}')
wb_src = openpyxl.load_workbook(BRUT_FILE, data_only=True, read_only=True)

format_detecte = detecter_format(wb_src.sheetnames)
print(f'[OK] Format détecté : {format_detecte}')

if format_detecte == FORMAT_CREDIT_MUTUEL_NATIF:
    raw_rows, raw_notes, excluded_footer = lire_natif(wb_src)
elif format_detecte == FORMAT_RELEVE_CONSOLIDE:
    raw_rows, raw_notes, excluded_footer = lire_consolide(wb_src)
else:
    print(f'[ERREUR BLOQUANT] Feuille "{SHEET_METIER}" absente.')
    print(f'  Feuilles disponibles : {wb_src.sheetnames}')
    print('  Format non reconnu : ni Crédit Mutuel natif, ni relevé consolidé '
          f'({sorted(SHEETS_CONSOLIDE_REQUISES)} requises).')
    sys.exit(1)
wb_src.close()
print(f'[OK] {len(raw_rows)} mouvements retenus '
      f'({len(excluded_footer)} pied(s) export exclus)')

# ==========================================================================
# 2. NORMALISATION + CONTRÔLES STRUCTURELS
# ==========================================================================

date_integration = datetime.now().strftime('%Y-%m-%d')
empreintes_vues  = {}
norm_data        = []
ctrl_data        = []
stats = dict(bloquants=0, a_controler=0, doublons=0, ok=0)

_data_row_effectif = DATA_ROW if format_detecte == FORMAT_CREDIT_MUTUEL_NATIF else CONSOLIDE_DATA_ROW

for idx, r in enumerate(raw_rows, 1):
    ligne_src = _data_row_effectif + idx - 1
    note_provenance = raw_notes[idx - 1] if idx - 1 < len(raw_notes) else None
    date_brute, val_brute, lib_brut, deb_brut, cre_brut, sol_brut, dev_brut = r

    anomalies = []

    # ── Libellé ──────────────────────────────────────────────────────────
    lib_norm = normalize_libelle(lib_brut)
    if not lib_norm:
        anomalies.append('BANQUE_LIGNE_SANS_LIBELLE')

    # ── Dates (B1 : rattachement par colonnes, jamais par nom du fichier) ─
    date_op,  op_ok  = parse_date(date_brute)
    date_val, val_ok = parse_date(val_brute)

    if not op_ok and not val_ok:
        anomalies.append('BANQUE_DATE_INEXPLOITABLE')
    elif not op_ok and val_ok:
        anomalies.append('BANQUE_LIGNE_SANS_DATE')
        date_op = date_val  # fallback : utiliser date_valeur

    # ── Devise ────────────────────────────────────────────────────────────
    devise = str(dev_brut).strip().upper() if dev_brut else DEVISE_REF
    if not devise:
        devise = DEVISE_REF
    if devise != DEVISE_REF:
        anomalies.append('BANQUE_DEVISE_NON_EUR')

    # ── Montant / Sens ────────────────────────────────────────────────────
    deb_f, deb_ok = to_float(deb_brut)
    cre_f, cre_ok = to_float(cre_brut)
    sens             = None
    montant          = None
    montant_centimes = 0

    deb_renseigne = deb_ok and deb_f is not None
    cre_renseigne = cre_ok and cre_f is not None

    if not deb_renseigne and not cre_renseigne:
        if not deb_ok and not cre_ok:
            anomalies.append('BANQUE_DEBIT_CREDIT_VIDES')
        else:
            anomalies.append('BANQUE_MONTANT_NON_NUMERIQUE')
    elif deb_renseigne and cre_renseigne:
        anomalies.append('BANQUE_DEBIT_CREDIT_DOUBLES')
    elif deb_renseigne:
        sens             = 'DEBIT'
        montant          = round(abs(deb_f), 2)
        montant_centimes = int(round(montant * 100))
    else:
        sens             = 'CREDIT'
        montant          = round(abs(cre_f), 2)
        montant_centimes = int(round(montant * 100))

    # ── Empreinte anti-doublon (B9) ───────────────────────────────────────
    date_op_str  = date_op.strftime('%Y-%m-%d')  if date_op  else 'NODATE'
    date_val_str = date_val.strftime('%Y-%m-%d') if date_val else 'NODATE'

    row_hash   = make_hash([COMPTE_ID, date_op_str, date_val_str,
                             str(sens), str(montant_centimes), lib_norm, devise])
    hash_court = row_hash[:6].upper()

    if row_hash in empreintes_vues:
        anomalies.append('DOUBLON_BANCAIRE_POTENTIEL')
        stats['doublons'] += 1
    else:
        empreintes_vues[row_hash] = ligne_src

    # ── mouvement_id ──────────────────────────────────────────────────────
    date_str     = date_op_str.replace('-', '')
    mouvement_id = (f'MVT-{COMPTE_ID}-{date_str}'
                    f'-{sens or "NOFLOW"}-{montant_centimes}-{hash_court}')

    # ── statut_controle ───────────────────────────────────────────────────
    bloquant = any(c in BLOQUANT_CODES for c in anomalies)
    if bloquant:
        statut = 'BLOQUANT'
        stats['bloquants'] += 1
    elif anomalies:
        statut = 'A_CONTROLER'
        stats['a_controler'] += 1
    else:
        statut = 'EN_ATTENTE_CLASSIFICATION'
        stats['ok'] += 1

    codes_str = ' | '.join(anomalies) if anomalies else ''

    norm_data.append([
        mouvement_id,                # 1  mouvement_id
        row_hash[:16],               # 2  ROW_HASH (16 premiers chars)
        IMPORT_ID,                   # 3  import_id
        ligne_src,                   # 4  ligne_source
        date_op_str,                 # 5  date_operation
        date_val_str,                # 6  date_valeur
        lib_norm,                    # 7  libelle
        str(lib_brut or ''),         # 8  libelle_brut
        montant,                     # 9  montant
        sens,                        # 10 sens
        devise,                      # 11 devise
        COMPTE_ID,                   # 12 compte_id
        None,                        # 13 tiers_detecte
        None,                        # 14 categorie
        None,                        # 15 type_flux_id
        None,                        # 16 code_impact
        'NON_CLASSE',                # 17 source_classification
        None,                        # 18 source_economique
        statut,                      # 19 statut_controle
        None,                        # 20 niveau_risque
        codes_str,                   # 21 codes_anomalie
        date_integration,            # 22 date_integration
        note_provenance,             # 23 commentaire (provenance consolidée, sinon None)
    ])

    for code in anomalies:
        sev = 'BLOQUANT' if code in BLOQUANT_CODES else 'A_CONTROLER'
        ctrl_data.append([
            mouvement_id,
            ligne_src,
            date_op_str,
            date_val_str,
            lib_norm or str(lib_brut or ''),
            montant,
            sens,
            code,
            sev,
            'Contrôle structurel Lot 8a',
            statut,
        ])

# ── Contrôle global BANQUE_FICHIER_PERIODE_INCOHERENTE ────────────────────
real_dates = []
for row in norm_data:
    d = row[4]  # date_operation
    if d and d != 'NODATE':
        try:
            real_dates.append(datetime.strptime(d, '%Y-%m-%d').date())
        except ValueError:
            pass

periode_incoherente = False
dmin_global = dmax_global = None
if real_dates:
    dmin_global = min(real_dates)
    dmax_global = max(real_dates)
    ecart = None

    if SOURCE_TYPE == SRC_MENSUEL:
        # Un export declare mensuel ne doit contenir QUE ce mois.
        attendu = f'{BORNE_DECLAREE_MIN.year}-{BORNE_DECLAREE_MIN.month:02d}'
        hors = [d for d in (dmin_global, dmax_global)
                if (d.year, d.month) != (BORNE_DECLAREE_MIN.year, BORNE_DECLAREE_MIN.month)]
        if hors:
            ecart = (f'Le fichier est declare MENSUEL {attendu} '
                     f'mais couvre {dmin_global} -> {dmax_global}.')

    elif SOURCE_TYPE == SRC_HISTORIQUE and BORNE_DECLAREE_MIN and BORNE_DECLAREE_MAX:
        # Plusieurs mois sont NORMAUX. On verifie seulement que le nom dit vrai.
        if dmin_global != BORNE_DECLAREE_MIN or dmax_global != BORNE_DECLAREE_MAX:
            ecart = (f'Le nom annonce {BORNE_DECLAREE_MIN} -> {BORNE_DECLAREE_MAX} '
                     f'mais le contenu couvre {dmin_global} -> {dmax_global}.')

    # SRC_INDETERMINE : aucune promesse dans le nom, donc aucun controle de periode.

    if ecart:
        periode_incoherente = True
        ctrl_data.insert(0, [
            f'IMPORT-{IMPORT_ID}',
            'GLOBAL',
            str(dmin_global),
            str(dmax_global),
            f'Periode reelle {dmin_global}->{dmax_global}, source declaree {SOURCE_TYPE}',
            None,
            None,
            'BANQUE_FICHIER_PERIODE_INCOHERENTE',
            'A_CONTROLER',
            ecart + ' Non bloquant si au moins une date exploitable (B10).',
            'A_CONTROLER',
        ])
        print(f'[WARN] BANQUE_FICHIER_PERIODE_INCOHERENTE : {ecart}')
    else:
        print(f'[OK] Periode conforme a la declaration {SOURCE_TYPE} : '
              f'{dmin_global} -> {dmax_global}')

print(f'[OK] Normalisation terminée : {len(norm_data)} lignes NORM | '
      f'{stats["bloquants"]} BLOQUANT | {stats["a_controler"]} A_CONTROLER | '
      f'{stats["doublons"]} doublons | {stats["ok"]} EN_ATTENTE_CLASSIFICATION')

# ==========================================================================
# 3. ÉCRITURE EXCEL
# ==========================================================================

if os.path.exists(OUT_FILE):
    bak = os.path.join(ARCHIVE_DIR, f'BANQUE_LOT8_IMPORT_BACKUP_{TS}.xlsx')
    shutil.copy2(OUT_FILE, bak)
    print(f'[OK] Backup : {bak}')

wb = openpyxl.Workbook()
wb.remove(wb.active)

# ==========================================================================
# Onglet 1 — BRUT_Banque
# ==========================================================================
ws_b = wb.create_sheet('BRUT_Banque')
BRUT_HDR = ['import_id', 'ligne_source', 'date_brute', 'valeur_brute',
            'libelle_brut', 'debit_brut', 'credit_brut', 'solde_brut', 'devise_brute']
BRUT_WID = [26, 13, 14, 14, 65, 14, 14, 14, 12]

ws_b.append(BRUT_HDR)
style_header(ws_b, 1, len(BRUT_HDR))
for idx, r in enumerate(raw_rows, 1):
    ws_b.append([IMPORT_ID, DATA_ROW + idx - 1] + r)
for row in ws_b.iter_rows(min_row=2):
    for cell in row:
        cell.font = FONT_NRM
        cell.alignment = ALIGN_LEFT
set_widths(ws_b, BRUT_WID)
ws_b.freeze_panes = 'A2'

# ==========================================================================
# Onglet 2 — NORM_Banque
# ==========================================================================
ws_n = wb.create_sheet('NORM_Banque')
NORM_HDR = [
    'mouvement_id', 'ROW_HASH', 'import_id', 'ligne_source',
    'date_operation', 'date_valeur', 'libelle', 'libelle_brut',
    'montant', 'sens', 'devise', 'compte_id',
    'tiers_detecte', 'categorie', 'type_flux_id', 'code_impact',
    'source_classification', 'source_economique',
    'statut_controle', 'niveau_risque',
    'codes_anomalie', 'date_integration', 'commentaire',
]
NORM_WID = [55, 20, 26, 13, 14, 14, 65, 65, 12, 10, 8, 30,
            25, 25, 20, 14, 28, 30, 28, 14, 55, 14, 35]

ws_n.append(NORM_HDR)
style_header(ws_n, 1, len(NORM_HDR))
for row in norm_data:
    ws_n.append(row)
    r_idx = ws_n.max_row
    statut = row[18]
    if statut == 'BLOQUANT':
        fill = FILL_BLOQUANT
        fnt  = Font(bold=True, color='FFFFFF', size=10)
    elif statut == 'A_CONTROLER':
        fill = FILL_ACTRL
        fnt  = Font(bold=True, color='FFFFFF', size=10)
    else:
        fill = FILL_INFO
        fnt  = FONT_NRM
    ws_n.cell(r_idx, 19).fill = fill
    ws_n.cell(r_idx, 19).font = fnt
    ws_n.cell(r_idx, 19).alignment = ALIGN_CTR
    for c in range(1, len(NORM_HDR) + 1):
        if c != 19:
            ws_n.cell(r_idx, c).font = FONT_NRM
            ws_n.cell(r_idx, c).alignment = ALIGN_LEFT
set_widths(ws_n, NORM_WID)
ws_n.freeze_panes = 'A2'
ws_n.auto_filter.ref = ws_n.dimensions

# ==========================================================================
# Onglet 3 — CTRL_A_CONTROLER
# ==========================================================================
ws_c = wb.create_sheet('CTRL_A_CONTROLER')
CTRL_HDR = [
    'mouvement_id', 'ligne_source', 'date_operation', 'date_valeur',
    'libelle', 'montant', 'sens',
    'code_controle', 'severite', 'description', 'statut_controle',
]
CTRL_WID = [55, 13, 14, 14, 55, 12, 10, 42, 14, 65, 22]

ws_c.append(CTRL_HDR)
style_header(ws_c, 1, len(CTRL_HDR))
for row in ctrl_data:
    ws_c.append(row)
    r_idx = ws_c.max_row
    sev = row[8]
    fill = FILL_BLOQUANT if sev == 'BLOQUANT' else FILL_ACTRL
    for col_idx in [9, 11]:
        cell = ws_c.cell(r_idx, col_idx)
        cell.fill = fill
        cell.font = Font(bold=True, color='FFFFFF', size=10)
        cell.alignment = ALIGN_CTR
    for col_idx in range(1, len(CTRL_HDR) + 1):
        if col_idx not in [9, 11]:
            ws_c.cell(r_idx, col_idx).font = FONT_NRM
            ws_c.cell(r_idx, col_idx).alignment = ALIGN_LEFT
set_widths(ws_c, CTRL_WID)
ws_c.freeze_panes = 'A2'
ws_c.auto_filter.ref = ws_c.dimensions

# ==========================================================================
# Onglet 4 — LOG_Traitement
# ==========================================================================
ws_l = wb.create_sheet('LOG_Traitement')
LOG_HDR = [
    'run_id', 'import_id', 'etape', 'timestamp',
    'nb_lignes_lues', 'nb_lignes_exclues_pied', 'nb_lignes_brut', 'nb_lignes_norm',
    'nb_bloquants', 'nb_a_controler', 'nb_doublons',
    'periode_incoherente', 'commentaire', 'format_source',
]
LOG_WID = [30, 26, 18, 22, 15, 22, 15, 15, 14, 16, 13, 22, 55, 22]

ws_l.append(LOG_HDR)
style_header(ws_l, 1, len(LOG_HDR))
run_id = f'RUN-LOT8A-{TS}'
nb_lues_total = len(raw_rows) + len(excluded_footer)
ws_l.append([
    run_id,
    IMPORT_ID,
    'LOT8A_IMPORT',
    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    nb_lues_total,
    len(excluded_footer),
    len(raw_rows),
    len(norm_data),
    stats['bloquants'],
    stats['a_controler'],
    stats['doublons'],
    'OUI' if periode_incoherente else 'NON',
    f'lot8a_banque_import.py — {TS}',
    format_detecte,
])
for row in ws_l.iter_rows(min_row=2):
    for cell in row:
        cell.font = FONT_NRM
        cell.alignment = ALIGN_LEFT
set_widths(ws_l, LOG_WID)
ws_l.freeze_panes = 'A2'

# ==========================================================================
# Onglet 5 — REF_Cloture_Mensuelle (structure, alimentée Lot 8c)
# ==========================================================================
ws_cl = wb.create_sheet('REF_Cloture_Mensuelle')
CLOTURE_HDR = [
    'mois', 'statut_mois', 'date_passage_controle',
    'date_cloture', 'nb_lignes_bancaires_non_classees',
    'nb_controles_bloquants_ouverts', 'commentaire',
]
CLOTURE_WID = [12, 16, 24, 16, 34, 32, 55]

ws_cl.append(CLOTURE_HDR)
style_header(ws_cl, 1, len(CLOTURE_HDR))

# Initialiser les mois observés dans le fichier
months_obs = sorted(set((d.year, d.month) for d in real_dates)) if real_dates else []
for y, m in months_obs:
    ws_cl.append([
        f'{y}-{m:02d}', 'OUVERT', None, None, None, None,
        f'Auto-créé Lot 8a — à compléter Lot 8c',
    ])
for row in ws_cl.iter_rows(min_row=2):
    for cell in row:
        cell.font = FONT_NRM
        cell.alignment = ALIGN_LEFT
set_widths(ws_cl, CLOTURE_WID)
ws_cl.freeze_panes = 'A2'

# ==========================================================================
# Onglet 6 — POWER_QUERY_CODE (documentation)
# ==========================================================================
ws_pq = wb.create_sheet('POWER_QUERY_CODE')
ws_pq.column_dimensions['A'].width = 120
title_cell = ws_pq.cell(1, 1, 'POWER_QUERY_CODE — Lot 8a — Import Bancaire (Python)')
title_cell.fill = FILL_HDR
title_cell.font = FONT_HDR
title_cell.alignment = ALIGN_LEFT

doc_lines = [
    '',
    'Ce fichier est généré par lot8a_banque_import.py.',
    'Pas de transformation Power Query dans cet onglet — pipeline Python uniquement.',
    '',
    'Onglets produits :',
    '  BRUT_Banque           — Copie brute intégrale (import_id, ligne_source, 7 cols CM)',
    '  NORM_Banque           — Mouvements normalisés, empreinte anti-doublon, 23 colonnes',
    '  CTRL_A_CONTROLER      — Anomalies structurelles (BLOQUANT + A_CONTROLER)',
    '  LOG_Traitement        — Trace du run import',
    '  REF_Cloture_Mensuelle — États de mois (OUVERT/EN_CONTROLE/CLOTURE) — alimentée Lot 8c',
    '',
    'Règles appliquées :',
    '  B1  — Rattachement par colonnes Date/Valeur, jamais par nom de fichier',
    '  B2  — Fichier brut jamais modifié',
    '  B9  — Empreinte : compte_id|date_op|date_val|sens|montant_centimes|libellé_norm|devise',
    '  B10 — BANQUE_FICHIER_PERIODE_INCOHERENTE : A_CONTROLER (non bloquant)',
    '',
    'Source brute (jamais committée, .gitignore) :',
    '  01_SOURCES_BRUTES/Banque/<export declare>',
    '',
    'Script : 02_TRAVAIL/lot8a_banque_import.py',
    'ARCHI §13.2–§13.4 / REGLES §6 / Plan Lot 8a',
]
for i, line in enumerate(doc_lines, 2):
    ws_pq.cell(i, 1, line).font = FONT_NRM

# ==========================================================================
# 4. SAUVEGARDE
# ==========================================================================

wb.save(OUT_FILE)
print(f'[OK] Fichier produit : {OUT_FILE}')

print()
print('=' * 65)
print('BILAN LOT 8a — Import & normalisation bancaire')
print('=' * 65)
nb_lues_total = len(raw_rows) + len(excluded_footer)
print(f'  Format detecte     : {format_detecte}')
print(f'  Lignes lues total  : {nb_lues_total}')
print(f'  Pieds export exclus: {len(excluded_footer)}')
print(f'  Lignes BRUT/NORM   : {len(raw_rows)}')
print(f'  BLOQUANT           : {stats["bloquants"]}')
print(f'  A_CONTROLER        : {stats["a_controler"]}')
print(f'  Doublons potentiels: {stats["doublons"]}')
print(f'  EN_ATTENTE_CLASS.  : {stats["ok"]}')
if periode_incoherente and dmin_global and dmax_global:
    print(f'  Periode reelle     : {dmin_global} -> {dmax_global}')
    print(f'  [WARN A_CONTROLER] BANQUE_FICHIER_PERIODE_INCOHERENTE')
print(f'  Mois REF_Cloture   : {[f"{y}-{m:02d}" for y, m in months_obs]}')
print(f'  Sortie             : {OUT_FILE}')
print('=' * 65)
print()
print('Prochain lot : 8b — REF_Banque_Regles + classification déterministe')
print('Ne pas lancer avant validation humaine de ce bilan.')

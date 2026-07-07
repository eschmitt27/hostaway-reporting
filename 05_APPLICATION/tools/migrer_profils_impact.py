"""Migration contrôlée — profils d'impact des charges + catégorie personnalisée CHG_024.

Schéma + référentiel uniquement. N'écrit AUCUNE charge réelle, ne lance aucun pipeline.
Idempotent : réappliquer ne duplique rien.

Ajoute :
  REF_Setup.xlsm / REF_Categories_Charges
    - colonne `famille_impact_categorie`   (GLOBAL / LOGEMENT_DIRECT / MENAGE / PARCOURS_DEDIE)
    - colonne `profils_impact_autorises`   (liste séparée par des virgules parmi les 5 profils)
    - ligne CHG_024 (AUTRE_PERSONNALISEE), profil GLOBAL forcé
    - extension de la table tbl_REF_Categories_Charges
  SAISIE_Charges_Flux.xlsx / SAISIE
    - colonne `profil_impact_charge`         (valeur finale portée par la charge)
    - colonne `libelle_categorie_personnalise` (libellé libre, séparé de categorie_charge_id)
  SAISIE_Charges_Flux.xlsx / REF_LOCALE
    - CHG_024 ajouté à lst_Categories (nom défini étendu)
    - nouvelle liste lst_Profils_Impact + validation sur profil_impact_charge

Familles : preuve — PARCOURS_DEDIE = catégories hors formulaire standard (Commit 1) ;
MENAGE = filtre_vue_menage=OUI (REF) hors CHG_001/002 ; GLOBAL = défaut conservateur
(aucune répartition, aucun logement) pour le reste. LOGEMENT_DIRECT reste à décider par
catégorie (aucune preuve dans REF_Categories_Charges) — non affecté ici.
"""
from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.datavalidation import DataValidation

# ── Modèle profils ────────────────────────────────────────────────────────────
# CHG_012/013/015/019 reclassés PARCOURS_DEDIE (décision types flux et parcours dédiés).
FAMILLE_PARCOURS_DEDIE = {
    "CHG_001", "CHG_002", "CHG_012", "CHG_013", "CHG_014",
    "CHG_015", "CHG_019", "CHG_020", "CHG_021", "CHG_022",
}
FAMILLE_MENAGE = {"CHG_003", "CHG_004", "CHG_018", "CHG_023"}

PROFILS_PAR_FAMILLE = {
    "PARCOURS_DEDIE": "PARCOURS_DEDIE",
    "MENAGE": "MENAGE_INTERVENANT,MENAGE_LOGEMENTS",
    "GLOBAL": "GLOBAL",
    "LOGEMENT_DIRECT": "LOGEMENT_DIRECT",
}
PROFILS_IMPACT = ["GLOBAL", "LOGEMENT_DIRECT", "MENAGE_INTERVENANT", "MENAGE_LOGEMENTS", "PARCOURS_DEDIE"]

COL_FAMILLE = "famille_impact_categorie"
COL_PROFILS = "profils_impact_autorises"
SAISIE_NEW_COLS = ["profil_impact_charge", "libelle_categorie_personnalise"]

# ── Nouveau type de flux TYPE_FLUX_020 (charge société payée compte pro) ──
TYPE_FLUX_020_ID = "TYPE_FLUX_020"
TYPE_FLUX_020_ROW = {
    "type_flux_id": "TYPE_FLUX_020",
    "type_flux": "CHARGE_SOCIETE_COMPTE_PRO",
    "description": "Charge société payée avec compte professionnel",
    "code_impact_defaut": "IC",
    "avantage_brut_defaut": "NON",
    "deduit_avantage_defaut": "NON",
    "comptabilisable_defaut": "OUI",
    "actif": "OUI",
    "commentaire": (
        "Charge société réellement payée depuis le compte professionnel. "
        "Remplace TYPE_FLUX_002 (dépense personnelle) pour les charges normales banque pro."
    ),
}
# Types à garantir dans lst_TypesFlux_Lot3 de SAISIE (writer réel).
LST_TYPESFLUX_ADD = ["TYPE_FLUX_016", "TYPE_FLUX_020"]

CHG_024_ID = "CHG_024"
CHG_024_ROW = {
    "categorie_charge_id": "CHG_024",
    "categorie_niveau_1": "Autre",
    "categorie_niveau_2": "Catégorie personnalisée",
    "description": (
        "Catégorie personnalisée saisie librement (libelle_categorie_personnalise). "
        "Profil GLOBAL forcé, jamais ménage/logement/incident/AirCover/avance (D-CHG-MODELE-07)."
    ),
    "impact_resultat": "OUI",
    "refacturable_defaut": "NON",
    "hors_compta_defaut": "NON",
    "actif": "OUI",
    "filtre_vue_menage": "NON",
    COL_FAMILLE: "GLOBAL",
    COL_PROFILS: "GLOBAL",
}


def _famille(cat_id: str) -> str:
    if cat_id in FAMILLE_PARCOURS_DEDIE:
        return "PARCOURS_DEDIE"
    if cat_id in FAMILLE_MENAGE:
        return "MENAGE"
    return "GLOBAL"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _headers(ws) -> list[str]:
    return [str(c.value).strip() if c.value is not None else "" for c in ws[1]]


def migrate_ref_types_flux(ref_path: Path) -> dict:
    """Ajoute TYPE_FLUX_020 dans REF_Types_Flux (idempotent). Pas de table ListObject ici."""
    wb = openpyxl.load_workbook(str(ref_path), keep_vba=True, data_only=False)
    report = {"tf020_ajoute": False}
    try:
        ws = wb["REF_Types_Flux"]
        headers = _headers(ws)
        col_idx = {h: i + 1 for i, h in enumerate(headers)}
        id_col = col_idx["type_flux_id"]
        existing = {
            str(ws.cell(row=r, column=id_col).value or "").strip()
            for r in range(2, ws.max_row + 1)
        }
        if TYPE_FLUX_020_ID not in existing:
            new_row = ws.max_row + 1
            for name, value in TYPE_FLUX_020_ROW.items():
                if name in col_idx:
                    ws.cell(row=new_row, column=col_idx[name], value=value)
            report["tf020_ajoute"] = True
            wb.calculation.fullCalcOnLoad = True
            wb.save(str(ref_path))
    finally:
        wb.close()
    return report


def migrate_ref_setup(ref_path: Path) -> dict:
    """Migre REF_Categories_Charges : 2 colonnes profils + ligne CHG_024. Idempotent."""
    wb = openpyxl.load_workbook(str(ref_path), keep_vba=True, data_only=False)
    report = {"colonnes_ajoutees": [], "chg024_ajoute": False}
    try:
        ws = wb["REF_Categories_Charges"]
        headers = _headers(ws)
        col_idx = {h: i + 1 for i, h in enumerate(headers)}

        # 1. Colonnes profils
        for col in (COL_FAMILLE, COL_PROFILS):
            if col not in col_idx:
                new_col = len(headers) + 1
                ws.cell(row=1, column=new_col, value=col)
                headers.append(col)
                col_idx[col] = new_col
                report["colonnes_ajoutees"].append(col)

        # 2. Remplir famille/profils pour chaque catégorie existante
        id_col = col_idx["categorie_charge_id"]
        existing_ids = set()
        last_data_row = 1
        for r in range(2, ws.max_row + 1):
            cid = str(ws.cell(row=r, column=id_col).value or "").strip()
            if not cid:
                continue
            existing_ids.add(cid)
            last_data_row = r
            famille = _famille(cid)
            ws.cell(row=r, column=col_idx[COL_FAMILLE], value=famille)
            ws.cell(row=r, column=col_idx[COL_PROFILS], value=PROFILS_PAR_FAMILLE[famille])

        # 3. Ligne CHG_024
        if CHG_024_ID not in existing_ids:
            new_row = last_data_row + 1
            for name, value in CHG_024_ROW.items():
                if name in col_idx:
                    ws.cell(row=new_row, column=col_idx[name], value=value)
            report["chg024_ajoute"] = True
            last_data_row = new_row

        # 4. Étendre la table pour couvrir toutes les colonnes/lignes réelles
        tbl = ws.tables.get("tbl_REF_Categories_Charges")
        if tbl is not None:
            from openpyxl.utils import get_column_letter
            last_col_letter = get_column_letter(len(headers))
            tbl.ref = f"A1:{last_col_letter}{last_data_row}"

        wb.calculation.fullCalcOnLoad = True
        wb.save(str(ref_path))
    finally:
        wb.close()
    return report


def migrate_saisie(saisie_path: Path) -> dict:
    """Migre SAISIE (2 colonnes) + REF_LOCALE (CHG_024, lst_Profils_Impact + DV). Idempotent."""
    wb = openpyxl.load_workbook(str(saisie_path), data_only=False)
    report = {
        "colonnes_saisie": [], "chg024_reflocale": False, "lst_profils": False,
        "dv_profil": False, "typesflux_ajoutes": [],
    }
    try:
        ws = wb["SAISIE"]
        headers = _headers(ws)
        col_map = {h: i + 1 for i, h in enumerate(headers)}
        for col in SAISIE_NEW_COLS:
            if col not in col_map:
                new_col = len(headers) + 1
                ws.cell(row=1, column=new_col, value=col)
                headers.append(col)
                col_map[col] = new_col
                report["colonnes_saisie"].append(col)
        profil_col = col_map["profil_impact_charge"]

        rl = wb["REF_LOCALE"]
        rl_headers = _headers(rl)
        rl_idx = {h: i + 1 for i, h in enumerate(rl_headers)}

        # lst_TypesFlux_Lot3 : ajouter TYPE_FLUX_016 et TYPE_FLUX_020 (idempotent)
        from openpyxl.utils import get_column_letter as _gcl
        tf_col = rl_idx["lst_TypesFlux_Lot3"]
        tf_vals = [str(rl.cell(r, tf_col).value or "").strip() for r in range(2, rl.max_row + 1)]
        tf_vals = [v for v in tf_vals if v]
        for tf in LST_TYPESFLUX_ADD:
            if tf not in tf_vals:
                rl.cell(row=2 + len(tf_vals), column=tf_col, value=tf)
                tf_vals.append(tf)
                report["typesflux_ajoutes"].append(tf)
        if report["typesflux_ajoutes"] and "lst_TypesFlux_Lot3" in wb.defined_names:
            tf_letter = _gcl(tf_col)
            last_tf = 1 + len(tf_vals)
            wb.defined_names["lst_TypesFlux_Lot3"].value = (
                f"'REF_LOCALE'!${tf_letter}$2:${tf_letter}${last_tf}"
            )

        # CHG_024 dans lst_Categories (colonne A)
        cat_col = rl_idx["lst_Categories"]
        cat_vals = [str(rl.cell(r, cat_col).value or "").strip() for r in range(2, rl.max_row + 1)]
        cat_vals = [v for v in cat_vals if v]
        if CHG_024_ID not in cat_vals:
            row_new = 2 + len(cat_vals)
            rl.cell(row=row_new, column=cat_col, value=CHG_024_ID)
            report["chg024_reflocale"] = True
            # Étendre le nom défini lst_Categories
            last_cat_row = 1 + len(cat_vals) + 1
            if "lst_Categories" in wb.defined_names:
                wb.defined_names["lst_Categories"].value = f"'REF_LOCALE'!$A$2:$A${last_cat_row}"

        # lst_Profils_Impact : nouvelle colonne à droite
        from openpyxl.utils import get_column_letter
        if "lst_Profils_Impact" not in rl_idx:
            prof_col = rl.max_column + 1
            rl.cell(row=1, column=prof_col, value="lst_Profils_Impact")
            for i, p in enumerate(PROFILS_IMPACT, start=2):
                rl.cell(row=i, column=prof_col, value=p)
            letter = get_column_letter(prof_col)
            last = 1 + len(PROFILS_IMPACT)
            wb.defined_names["lst_Profils_Impact"] = DefinedName(
                "lst_Profils_Impact", attr_text=f"'REF_LOCALE'!${letter}$2:${letter}${last}"
            )
            report["lst_profils"] = True

        # DV sur la colonne profil_impact_charge
        profil_letter = get_column_letter(profil_col)
        sqref = f"{profil_letter}2:{profil_letter}1001"
        already = any(
            str(getattr(dv, "formula1", "")) == "lst_Profils_Impact"
            for dv in ws.data_validations.dataValidation
        )
        if not already:
            dv = DataValidation(type="list", formula1="lst_Profils_Impact", allow_blank=True)
            dv.add(sqref)
            ws.add_data_validation(dv)
            report["dv_profil"] = True

        wb.calculation.fullCalcOnLoad = True
        wb.save(str(saisie_path))
    finally:
        wb.close()
    return report


def backup(path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    dest = backup_dir / path.name
    shutil.copy2(str(path), str(dest))
    return dest


def run(ref_path: Path, saisie_path: Path, backup_dir: Path) -> dict:
    result = {
        "ref_setup": {"path": str(ref_path)},
        "saisie": {"path": str(saisie_path)},
        "backups": [],
    }
    result["ref_setup"]["hash_avant"] = sha256(ref_path)
    result["saisie"]["hash_avant"] = sha256(saisie_path)
    result["backups"].append(str(backup(ref_path, backup_dir)))
    result["backups"].append(str(backup(saisie_path, backup_dir)))
    result["ref_setup"].update(migrate_ref_types_flux(ref_path))
    result["ref_setup"].update(migrate_ref_setup(ref_path))
    result["saisie"].update(migrate_saisie(saisie_path))
    result["ref_setup"]["hash_apres"] = sha256(ref_path)
    result["saisie"]["hash_apres"] = sha256(saisie_path)
    return result


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Migration profils impact + CHG_024")
    parser.add_argument("--ref", required=True)
    parser.add_argument("--saisie", required=True)
    parser.add_argument("--backup-dir", required=True)
    args = parser.parse_args()
    out = run(Path(args.ref), Path(args.saisie), Path(args.backup_dir))
    print(json.dumps(out, ensure_ascii=False, indent=2))

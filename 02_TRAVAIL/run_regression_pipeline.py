#!/usr/bin/env python3
"""
Runner de regression deterministe, sans API vivante ni banque en ligne.

Il relance deux fois la chaine aval locale puis compare un manifeste metier.
Les timestamps de fichiers et colonnes techniques de date d'execution sont exclus.
"""

import csv
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
HERE = ROOT / "02_TRAVAIL"
OUT_DIR = ROOT / "04_LOGS" / "REGRESSION_PIPELINE"

STEPS = [
    "lot4quater_resoudre_source_reservations.py",
    "lot9_construire_flux.py",
    "lot10_calculer_resultats.py",
    "lot11_controles_coherence.py",
    "lot12_generer_factures.py",
    "lot13_export_powerbi.py",
]

TABLES = [
    ("reservations_resolues", HERE / "Lot4quater_SourceResolue" / "MASTER_CALC_Reservations_Resolues.xlsx", "MASTER"),
    ("flux", HERE / "Lot9_FluxUnifie" / "MASTER_CALC_Flux.xlsx", "MASTER"),
    ("commissions", HERE / "Lot10_Resultats" / "MASTER_CALC_Commissions.xlsx", "COMMISSIONS"),
    ("net_reglement", HERE / "Lot10_Resultats" / "MASTER_CALC_NetProprietaire.xlsx", "REGLEMENT"),
    ("resultats_global", HERE / "Lot10_Resultats" / "MASTER_CALC_Resultats.xlsx", "GLOBAL"),
    ("resultats_mensuels", HERE / "Lot10_Resultats" / "MASTER_CALC_Resultats.xlsx", "PAR_MOIS_LOGEMENT"),
    ("controles_master", HERE / "Lot11_Controles" / "MASTER_CTRL_Coherence.xlsx", "MASTER"),
    ("prefactures_entete", HERE / "Lot12_Factures" / "MASTER_FACT_Proprietaires.xlsx", "FACT_FACTURE_ENTETE"),
    ("prefactures_lignes", HERE / "Lot12_Factures" / "MASTER_FACT_Proprietaires.xlsx", "FACT_FACTURE_LIGNES"),
]

TECHNICAL_DATE_COLUMNS = {
    "date_detection", "date_generation", "date_integration", "date_export",
    "fige_le", "calcule_le", "generated_at",
}

ID_COLUMNS = {
    "flux_id", "reservation_calc_id", "reservation_id_hostaway",
    "flux_source_pk", "ctrl_pk", "facture_id", "ajustement_id",
}


def _value(v):
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _rows(path, sheet):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[sheet]
        data = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
    finally:
        wb.close()
    if not data:
        return [], []
    headers = [str(c) for c in data[0]]
    ignored = {i for i, h in enumerate(headers) if h in TECHNICAL_DATE_COLUMNS}
    rows = []
    for r in data[1:]:
        rows.append({h: r[i] for i, h in enumerate(headers) if i not in ignored})
    return [h for i, h in enumerate(headers) if i not in ignored], rows


def _amount_columns(headers):
    markers = ("montant", "payout", "commission", "resultat", "reste", "credit", "menage", "net")
    return [h for h in headers if any(m in h.lower() for m in markers)]


def _canonical_hash(rows):
    payload = []
    for row in rows:
        payload.append(json.dumps({k: _value(row.get(k)) for k in sorted(row)}, sort_keys=True, ensure_ascii=False))
    return hashlib.sha256("\n".join(sorted(payload)).encode("utf-8")).hexdigest()


def table_manifest(name, path, sheet):
    if not path.exists():
        return {"status": "ABSENT", "rows": 0}
    headers, rows = _rows(path, sheet)
    id_cols = [c for c in headers if c in ID_COLUMNS]
    amount_cols = _amount_columns(headers)
    amounts = {}
    for col in amount_cols:
        total = 0.0
        for row in rows:
            try:
                total += float(row.get(col) or 0)
            except (TypeError, ValueError):
                pass
        amounts[col] = round(total, 2)

    open_controls = 0
    if "statut_resolution" in headers:
        open_controls = sum(1 for r in rows if str(r.get("statut_resolution")).upper() == "OUVERT")

    resultats = {}
    if name == "resultats_global":
        for row in rows:
            resultats[str(row.get("vision"))] = _value(row.get("resultat"))

    return {
        "status": "OK",
        "rows": len(rows),
        "id_columns": id_cols,
        "ids": sorted(
            "|".join(_value(row.get(c)) for c in id_cols)
            for row in rows
        ) if id_cols else [],
        "amounts": amounts,
        "open_controls": open_controls,
        "resultats": resultats,
        "hash": _canonical_hash(rows),
    }


def build_manifest():
    return {
        name: table_manifest(name, path, sheet)
        for name, path, sheet in TABLES
    }


def run_chain(label):
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for i, script in enumerate(STEPS, 1):
        path = HERE / script
        print(f"[{label}] {i}/{len(STEPS)} {script}")
        result = subprocess.run([sys.executable, str(path)], cwd=str(ROOT), env=env)
        if result.returncode != 0:
            raise SystemExit(result.returncode)


def write_manifest(manifest, label):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"manifest_{label}.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    return path


def main():
    print("Regression pipeline locale: aucune API vivante, aucun timestamp de fichier compare.")
    run_chain("RUN1")
    m1 = build_manifest()
    p1 = write_manifest(m1, "run1")

    run_chain("RUN2")
    m2 = build_manifest()
    p2 = write_manifest(m2, "run2")

    if m1 != m2:
        diff_path = OUT_DIR / "diff_regression.csv"
        with diff_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["table", "run1", "run2"])
            for key in sorted(set(m1) | set(m2)):
                if m1.get(key) != m2.get(key):
                    writer.writerow([
                        key,
                        json.dumps(m1.get(key), sort_keys=True, ensure_ascii=False),
                        json.dumps(m2.get(key), sort_keys=True, ensure_ascii=False),
                    ])
        print(f"[ECHEC] Difference non expliquee. Voir {diff_path}")
        raise SystemExit(2)

    print(f"[OK] Rejeu deterministe. Manifestes: {p1} / {p2}")


if __name__ == "__main__":
    main()

"""Crée / vérifie SAISIE_Charges_Impacts.xlsx — source de vérité durable des impacts d'une charge.

Source de SAISIE (pas un master de calcul régénéré). Onglets normalisés liés par `charge_id` :
- AFFECTATIONS          : ventilation analytique multi-logements / propriétaires (quotes-parts).
- MENAGE                : sélections / impacts ménage (intervenant OU logement) pour Lot6f.
- RESERVE_REFACTURATION : réserve de charges refacturables en attente (préparation préfactures Lot12).

Aucune donnée métier écrite : seul le schéma (en-têtes + tables) est établi. Idempotent :
réexécuter ne duplique rien et préserve les données existantes.

Colonnes communes à chaque ligne persistée : identifiant unique, charge_id (lien parent),
mois, périmètre, montant/quote-part si utile, statut, origine, commentaire, ROW_HASH (contrôle).
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
from openpyxl.worksheet.table import Table, TableStyleInfo

# ── Schéma normalisé ─────────────────────────────────────────────────────────
SCHEMA: dict[str, list[str]] = {
    "AFFECTATIONS": [
        "affectation_id", "charge_id", "mois", "logement_id", "proprietaire_id",
        "quote_part", "statut", "origine", "commentaire", "ROW_HASH",
    ],
    "MENAGE": [
        "menage_impact_id", "charge_id", "mois", "mode", "intervenant_id", "logement_id",
        "statut", "origine", "commentaire", "ROW_HASH",
    ],
    "RESERVE_REFACTURATION": [
        "reserve_id", "charge_id", "mois", "logement_id", "proprietaire_id",
        "montant_refacturable", "libelle", "justificatif", "statut_traitement",
        "trace_decision", "origine", "commentaire", "ROW_HASH",
    ],
}
TABLE_NAMES = {
    "AFFECTATIONS": "tbl_Affectations",
    "MENAGE": "tbl_MenageImpacts",
    "RESERVE_REFACTURATION": "tbl_ReserveRefacturation",
}
README = [
    ("SAISIE_Charges_Impacts.xlsx", ""),
    ("Source de vérité durable des impacts analytiques d'une charge (liés par charge_id).", ""),
    ("", ""),
    ("Règles :", ""),
    ("- Une charge économique reste unique (circuit Charges Lot3).", ""),
    ("- Les affectations/impacts ne sont jamais des doubles charges.", ""),
    ("- Somme des quote_part d'une charge = montant réparti (jamais répliqué).", ""),
    ("- Une charge ménage n'est jamais refacturable (aucune ligne RESERVE).", ""),
    ("- MENAGE alimente uniquement l'analytique ménage (Lot6f, COUT_STANDARD_MENAGES_MOIS).", ""),
    ("- RESERVE_REFACTURATION lue plus tard par Lot12 ; n'augmente jamais automatiquement une préfacture.", ""),
    ("- Avantages associés : source Lot7 (MASTER_FACT_MAN_IK_Avantages / SOURCE_SAISIE), lien_origine=charge_id.", ""),
    ("- Jamais de liste d'identifiants concaténée dans une cellule.", ""),
    ("- Aucune écriture réelle tant que CHARGES_REAL_WRITE_ENABLED = False.", ""),
]


def _write_sheet(ws, headers: list[str], table_name: str | None) -> None:
    for ci, h in enumerate(headers, 1):
        ws.cell(row=1, column=ci, value=h)
    ws.freeze_panes = "A2"
    if table_name:
        from openpyxl.utils import get_column_letter
        ref = f"A1:{get_column_letter(len(headers))}2"  # header + 1 ligne vide (table valide)
        tbl = Table(displayName=table_name, ref=ref)
        tbl.tableStyleInfo = TableStyleInfo(
            name="TableStyleLight9", showRowStripes=True, showColumnStripes=False
        )
        ws.add_table(tbl)


def creer(path: Path) -> dict:
    """Crée le fichier s'il n'existe pas. Idempotent (ne réécrit pas un fichier conforme)."""
    p = Path(path)
    if p.exists() and verifier(p)["conforme"]:
        return {"cree": False, "conforme": True, "path": str(p)}
    p.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for sheet, headers in SCHEMA.items():
        ws = wb.create_sheet(sheet)
        _write_sheet(ws, headers, TABLE_NAMES.get(sheet))
    wsr = wb.create_sheet("README")
    for ri, (a, b) in enumerate(README, 1):
        wsr.cell(row=ri, column=1, value=a)
        wsr.cell(row=ri, column=2, value=b)
    wb.save(str(p))
    wb.close()
    return {"cree": True, "conforme": True, "path": str(p)}


def verifier(path: Path) -> dict:
    """Vérifie que le fichier porte le schéma attendu (onglets + en-têtes)."""
    p = Path(path)
    if not p.exists():
        return {"conforme": False, "raison": "absent"}
    wb = openpyxl.load_workbook(str(p), read_only=True)
    try:
        for sheet, headers in SCHEMA.items():
            if sheet not in wb.sheetnames:
                return {"conforme": False, "raison": f"onglet manquant : {sheet}"}
            ws = wb[sheet]
            first = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), ())
            got = [str(x).strip() if x is not None else "" for x in first][:len(headers)]
            if got != headers:
                return {"conforme": False, "raison": f"en-têtes {sheet} : {got}"}
    finally:
        wb.close()
    return {"conforme": True}


if __name__ == "__main__":
    import argparse
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    args = ap.parse_args()
    print(json.dumps(creer(Path(args.path)), ensure_ascii=False, indent=2))

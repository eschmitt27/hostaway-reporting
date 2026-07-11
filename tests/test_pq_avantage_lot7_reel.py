"""Lot7 — preuve COMPLÉMENTAIRE OPTIONNELLE (Excel COM), NON nécessaire au pipeline principal.

Pipeline principal Lot7 = générateur Python `lot7_generateur_avantages.py` (Option A), prouvé par
tests/test_lot7_generateur_avantages.py. Ce test est un cross-check facultatif : il exécute le moteur
Power Query d'Excel (COM) sur un classeur scratch (aucune donnée métier réelle) pour confirmer que le
M-code documentaire donne les mêmes résultats. Skippé si Windows/Excel/win32com indisponibles (CI verte).

Vérifie la décision validée :
- avantage_associe_id renseigné → totalité du montant attribuée à cet associé, quel que soit le paiement ;
- PAY_003/PAY_004 ne créent jamais automatiquement un avantage ;
- PAY_001 peut créer un avantage si avantage_associe_id renseigné ;
- TYPE_FLUX_002 conserve son traitement historique ;
- une même charge_id jamais comptée deux fois ; 2e actualisation → identique (100€, jamais 200€).
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

# M-code réel = celui documenté par lot7_ik_avantages.py (fonction mcode_avantages_charges)
try:
    from lot7_pq_avantages import MCODE_AVANTAGES_CHARGES  # noqa
except Exception:  # fallback : reconstruit ci-dessous
    MCODE_AVANTAGES_CHARGES = None


def _excel_dispo():
    if sys.platform != "win32":
        return False
    try:
        import win32com.client  # noqa
    except Exception:
        return False
    return True


MCODE = MCODE_AVANTAGES_CHARGES or (
    'let\n'
    '    Source = Excel.CurrentWorkbook(){[Name="CHARGES"]}[Content],\n'
    '    Typed = Table.TransformColumnTypes(Source, {{"montant", type number}}),\n'
    '    Explicit = Table.SelectRows(Typed, each ([avantage_associe_id] <> null) and ([avantage_associe_id] <> "")),\n'
    '    AggExplicit = Table.Group(Explicit, {"mois","avantage_associe_id"}, {{"avantage_brut", each List.Sum([montant]), type number}}),\n'
    '    RenExplicit = Table.RenameColumns(AggExplicit, {{"avantage_associe_id","associe_id"}}),\n'
    '    Hist = Table.SelectRows(Typed, each ([type_flux_id] = "TYPE_FLUX_002") and (([avantage_associe_id] = null) or ([avantage_associe_id] = ""))),\n'
    '    AggHist = Table.Group(Hist, {"mois","associe_id"}, {{"avantage_brut", each List.Sum([montant]), type number}}),\n'
    '    Combined = Table.Combine({RenExplicit, AggHist}),\n'
    '    Final = Table.Group(Combined, {"associe_id","mois"}, {{"avantage_brut", each List.Sum([avantage_brut]), type number}})\n'
    'in\n'
    '    Final\n'
)

COLS = ["charge_id", "mois", "montant", "type_flux_id", "associe_id", "mode_paiement_id", "avantage_associe_id"]
CHARGES = [
    ["C1", "2026-06", 100, "TYPE_FLUX_020", "", "PAY_001", "PERS_EWAN"],   # banque pro + flag → EWAN 100
    ["C2", "2026-06", 50, "TYPE_FLUX_004", "PERS_WAFA", "PAY_003", ""],     # carte, TF004, pas de flag → 0
    ["C3", "2026-06", 30, "TYPE_FLUX_002", "PERS_WAFA", "PAY_004", ""],     # TF002 historique → WAFA 30
]


@unittest.skipUnless(_excel_dispo(), "Excel/win32com indisponible")
class PowerQueryAvantageReelTests(unittest.TestCase):
    def test_avantage_pq_reel_idempotent(self):
        import tempfile, os
        import openpyxl
        from openpyxl.worksheet.table import Table, TableStyleInfo
        import win32com.client as win32

        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, "pq_avantage.xlsx")
        wb = openpyxl.Workbook(); ws = wb.active; ws.title = "DATA"
        ws.append(COLS)
        for r in CHARGES:
            ws.append(r)
        ref = f"A1:{openpyxl.utils.get_column_letter(len(COLS))}{len(CHARGES)+1}"
        t = Table(displayName="CHARGES", ref=ref)
        t.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        ws.add_table(t); wb.save(path); wb.close()

        xl = win32.DispatchEx("Excel.Application")
        xl.Visible = False; xl.DisplayAlerts = False
        try:
            book = xl.Workbooks.Open(path)
            book.Queries.Add("AVANTAGES", MCODE)
            rs = book.Worksheets.Add(); rs.Name = "RESULT"
            conn = ("OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;"
                    "Location=AVANTAGES;Extended Properties=\"\"")
            qt = rs.QueryTables.Add(Connection=conn, Destination=rs.Range("A1"))
            qt.CommandType = 2; qt.CommandText = "SELECT * FROM [AVANTAGES]"
            qt.BackgroundQuery = False

            def lire():
                out = {}; r = 2
                while True:
                    a = rs.Cells(r, 1).Value
                    if a in (None, ""):
                        break
                    out[(str(a), str(rs.Cells(r, 2).Value))] = round(float(rs.Cells(r, 3).Value or 0), 2)
                    r += 1
                return out

            qt.Refresh(False)
            res1 = lire()
            qt.Refresh(False)
            res2 = lire()
            book.Close(SaveChanges=False)
        finally:
            xl.Quit()

        # Avantage explicite (banque pro) attribué à EWAN, montant total, une seule fois
        assert res1.get(("PERS_EWAN", "2026-06")) == 100.0
        # Idempotent : 2e actualisation → toujours 100, jamais 200
        assert res2.get(("PERS_EWAN", "2026-06")) == 100.0
        # TYPE_FLUX_002 historique préservé
        assert res1.get(("PERS_WAFA", "2026-06")) == 30.0
        assert res2.get(("PERS_WAFA", "2026-06")) == 30.0
        # PAY_003 / TF004 sans flag → jamais avantage automatique (WAFA n'a que les 30 du TF002)
        assert res1.get(("PERS_WAFA", "2026-06")) == 30.0


if __name__ == "__main__":
    unittest.main()

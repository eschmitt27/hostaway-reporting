"""APP-0 — Garantie lecture seule : aucun chemin non-SAISIE n'est writable."""
import pytest
from pathlib import Path
from app.services.file_registry import is_writable, assert_writable


@pytest.mark.parametrize("path", [
    "01_SOURCES_BRUTES/REF_Setup.xlsm",
    "02_TRAVAIL/Lot1_Hostaway/MASTER_RUN_Log.xlsx",
    "03_EXPORTS/PowerBI/PBI_Flux.csv",
    "02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx",
    "01_SOURCES_BRUTES/REF_Cloture_Mensuelle.xlsx",
    "01_SOURCES_BRUTES/REF_Banque_Regles.xlsx",
    "MASTER_FACT_Proprietaires.xlsx",
    "REF_Taux_Commission.xlsx",
])
def test_readonly_paths_not_writable(path):
    assert not is_writable(Path(path)), f"ERREUR : {path} ne devrait pas être writable"


@pytest.mark.parametrize("path", [
    "01_SOURCES_BRUTES/SAISIE_Charges_Flux.xlsx",
    "01_SOURCES_BRUTES/SAISIE_Acomptes.xlsx",
    "01_SOURCES_BRUTES/SAISIE_ReservationsHorsHostaway.xlsx",
    "01_SOURCES_BRUTES/SAISIE_AirCover.xlsx",
])
def test_saisie_paths_are_writable(path):
    assert is_writable(Path(path)), f"ERREUR : {path} devrait être writable"


def test_assert_writable_raises_on_ref():
    with pytest.raises(PermissionError):
        assert_writable(Path("REF_Setup.xlsm"))


def test_assert_writable_raises_on_master():
    with pytest.raises(PermissionError):
        assert_writable(Path("MASTER_CALC_Flux.xlsx"))


def test_excel_reader_uses_read_only(tmp_path):
    """Vérifie que excel_reader ouvre uniquement en mode read_only."""
    import openpyxl
    # Créer un xlsx de test
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["col_a", "col_b"])
    ws.append(["v1", "v2"])
    p = tmp_path / "test.xlsx"
    wb.save(str(p))

    from app.readers.excel_reader import read_sheet
    rows = read_sheet(p, max_rows=10)
    assert len(rows) == 1
    assert rows[0]["col_a"] == "v1"
    # Vérifier que le fichier n'a pas été modifié
    mtime_before = p.stat().st_mtime
    read_sheet(p, max_rows=10)
    mtime_after = p.stat().st_mtime
    assert mtime_before == mtime_after, "excel_reader a modifié le fichier !"


def test_saisie_writer_raises():
    from app.writers.saisie_writer import write_row, append_rows
    with pytest.raises(NotImplementedError):
        write_row(Path("SAISIE_Test.xlsx"), "Sheet1", {"col": "val"})
    with pytest.raises(NotImplementedError):
        append_rows(Path("SAISIE_Test.xlsx"), "Sheet1", [])

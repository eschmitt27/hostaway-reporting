import ast
import importlib.util
import shutil
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
LOT_NAMES = [
    "lot4bis_charger_reservations",
    "lot6c_menages_externes",
    "lot10_calculer_resultats",
    "lot11_controles_coherence",
    "lot12_generer_factures",
    "lot13_export_powerbi",
]
LOT_SCRIPTS = [ROOT / "02_TRAVAIL" / f"{name}.py" for name in LOT_NAMES]
SUPPORT_MODULES = [
    "lib_canape.py",
    "lib_cloture.py",
    "lib_controls.py",
    "lib_guestcount.py",
    "lib_menage_costs.py",
    "lib_parc.py",
    "lib_ref_history.py",
    "lib_settlements.py",
    "lib_sheet_source.py",
]


def _is_main_guard(node):
    if not isinstance(node, ast.If):
        return False
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.ops) != 1 or len(test.comparators) != 1:
        return False
    left = test.left
    right = test.comparators[0]
    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and isinstance(test.ops[0], ast.Eq)
        and isinstance(right, ast.Constant)
        and right.value == "__main__"
    )


def _call_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        parts = [func.attr]
        value = func.value
        while isinstance(value, ast.Attribute):
            parts.append(value.attr)
            value = value.value
        if isinstance(value, ast.Name):
            parts.append(value.id)
        return ".".join(reversed(parts))
    return ast.unparse(func)


def _snapshot(path):
    return sorted(str(p.relative_to(path)) for p in path.rglob("*"))


class TestLotImportsAreSideEffectFree(unittest.TestCase):
    def test_lot_scripts_have_main_guard(self):
        for path in LOT_SCRIPTS:
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                self.assertTrue(
                    any(isinstance(node, ast.FunctionDef) and node.name == "main" for node in tree.body),
                    f"{path} must expose main()",
                )
                self.assertTrue(
                    any(_is_main_guard(node) for node in tree.body),
                    f"{path} must guard execution with if __name__ == '__main__'",
                )

    def test_no_top_level_main_call_outside_guard(self):
        for path in LOT_SCRIPTS:
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in tree.body:
                    if _is_main_guard(node) or isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                        continue
                    for child in ast.walk(node):
                        if isinstance(child, ast.Call) and _call_name(child) == "main":
                            self.fail(f"{path} calls main() outside __main__ guard at line {child.lineno}")

    def test_no_top_level_excel_writes(self):
        dangerous = {"save", "to_excel", "ExcelWriter"}
        for path in LOT_SCRIPTS:
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in tree.body:
                    if _is_main_guard(node) or isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                        continue
                    for child in ast.walk(node):
                        if isinstance(child, ast.Call):
                            name = _call_name(child).split(".")[-1]
                            self.assertNotIn(
                                name,
                                dangerous,
                                f"{path} has top-level Excel write call {name} at line {child.lineno}",
                            )

    def test_imports_in_temp_copy_do_not_read_write_or_create_business_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = Path(tmp) / "project"
            work = tmp_root / "02_TRAVAIL"
            work.mkdir(parents=True)
            for script in LOT_SCRIPTS:
                shutil.copy2(script, work / script.name)
            for module in SUPPORT_MODULES:
                shutil.copy2(ROOT / "02_TRAVAIL" / module, work / module)

            before = _snapshot(tmp_root)
            opened = []

            def blocked_open(*args, **kwargs):
                opened.append(args[0] if args else None)
                raise AssertionError("business file access during import")

            old_path = list(sys.path)
            old_dont_write_bytecode = sys.dont_write_bytecode
            sys.path.insert(0, str(work))
            sys.dont_write_bytecode = True
            try:
                with patch("builtins.open", side_effect=blocked_open),                      patch("pathlib.Path.open", side_effect=blocked_open),                      patch("openpyxl.load_workbook", side_effect=AssertionError("workbook read during import")):
                    for name in LOT_NAMES:
                        with self.subTest(module=name):
                            spec = importlib.util.spec_from_file_location(f"tmp_{name}", work / f"{name}.py")
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)
            finally:
                sys.path[:] = old_path
                sys.dont_write_bytecode = old_dont_write_bytecode

            self.assertEqual(before, _snapshot(tmp_root))
            self.assertEqual(opened, [])


if __name__ == "__main__":
    unittest.main()

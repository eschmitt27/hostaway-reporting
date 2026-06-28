import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
LOT_SCRIPTS = [
    ROOT / "02_TRAVAIL" / "lot4bis_charger_reservations.py",
    ROOT / "02_TRAVAIL" / "lot10_calculer_resultats.py",
    ROOT / "02_TRAVAIL" / "lot11_controles_coherence.py",
    ROOT / "02_TRAVAIL" / "lot12_generer_factures.py",
    ROOT / "02_TRAVAIL" / "lot13_export_powerbi.py",
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


if __name__ == "__main__":
    unittest.main()

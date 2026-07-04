from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.services import saisie_hh_schema_real_prepare_service as schema_svc  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnostic ou migration controlee du schema HH reel.")
    parser.add_argument("--execute", action="store_true", help="Execute la migration reelle apres confirmation exacte.")
    args = parser.parse_args()

    if not args.execute:
        diagnostic = schema_svc.diagnostiquer_schema_hh()
        print(json.dumps(diagnostic, ensure_ascii=False, indent=2, default=str))
        return 0 if diagnostic["status"] == "OK" else 2

    confirmation = input("Saisir MIGRER_SCHEMA_HH_REELLE pour continuer : ").strip()
    result = schema_svc.executer_migration_hh_reelle(confirmation=confirmation)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("real_status") == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())

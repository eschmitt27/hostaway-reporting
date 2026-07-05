"""Outil de préparation REF_Assoc_Mode dans REF_Setup.xlsm — APP-3b-0.

Utilisation :

    # Diagnostic + dry-run (jamais d'écriture sur le fichier réel)
    python preparer_ref_assoc_mode.py

    # Migration réelle (flag cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED doit être True)
    python preparer_ref_assoc_mode.py --execute

La migration réelle nécessite une saisie interactive de la confirmation exacte :
    MIGRER_REF_ASSOC_MODE
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.services import ref_assoc_mode_prepare_service as svc  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnostic et préparation contrôlée de REF_Assoc_Mode dans REF_Setup.xlsm."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Exécute la migration réelle après confirmation interactive. "
            "Requiert cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED = True."
        ),
    )
    args = parser.parse_args()

    if not args.execute:
        # Mode dry-run : diagnostic + préparation sur copie
        print("[DIAGNOSTIC]")
        diag = svc.diagnostiquer()
        print(json.dumps(diag, ensure_ascii=False, indent=2, default=str))

        if not diag["migration_necessaire"]:
            print("\n[INFO] REF_Assoc_Mode déjà présente et cohérente. Aucune action requise.")
            return 0

        print("\n[PREPARATION SUR COPIE]")
        manifest = svc.preparer_sur_copie()
        print(json.dumps(manifest, ensure_ascii=False, indent=2, default=str))

        if manifest["status"] == "OK":
            print(f"\n[OK] Copie préparée dans : {manifest['paths']['output_dir']}")
            print(f"     Commande migration réelle (après activation du flag) :")
            print(f"     python preparer_ref_assoc_mode.py --execute")
        else:
            print(f"\n[ERREUR] Préparation échouée : {manifest.get('errors', [])}")

        return 0 if manifest["status"] == "OK" else 2

    # Mode execute : migration réelle
    print("[ATTENTION] Mode --execute : migration réelle dans REF_Setup.xlsm.")
    print(f"Saisir {svc.CONFIRMATION_EXECUTION!r} pour continuer : ", end="", flush=True)
    confirmation = input().strip()

    result = svc.executer_migration_reelle(confirmation=confirmation)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    real_status = result.get("real_status") or result.get("status") or ""
    if real_status in ("OK", "DEJA_OK"):
        return 0
    elif real_status == "MIGRATION_REELLE_NON_ACTIVE":
        print("\n[BLOQUE] cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED = False.")
        print("         Activer le flag avant de lancer --execute.")
        return 3
    elif result.get("status") == "REFUSE":
        print("\n[REFUSE] Confirmation incorrecte ou cible non autorisée.")
        return 3
    else:
        print(f"\n[ERREUR] real_status={real_status}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

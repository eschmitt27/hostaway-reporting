"""STUB d'extraction Hostaway (Lot6a) — recette ménages sur COPIES.

AUCUNE requête API Hostaway. Le vrai `lot6a_cleaning_tasks_comptage.py` interroge
l'API (`requests`, credentials .env) : il est volontairement REMPLACÉ par ce stub dans
l'arbre miroir, et jamais copié ni exécuté en mode copies.

Principe : le MASTER Hostaway réel a déjà été COPIÉ dans le workspace. Ce stub simule une
« extraction fraîche » en repartant de cette copie et en appliquant une injection contrôlée
optionnelle (`_stub_hostaway_injection.json` à la racine du workspace), puis en ré-écrivant
le classeur. Les onglets `data`, `MASTER_ENRICHI`, `VUE_COMPTAGE` sont préservés ; seule
la colonne `nb_menages_realises` de VUE_COMPTAGE (et, si présent, un compteur de MASTER_ENRICHI)
peut être ajustée pour reproduire « j'ai modifié des données Hostaway ». En l'absence
d'injection, le classeur est ré-écrit à l'identique (régénération neutre, traçable par mtime).

Format injection : {"vue_comptage": [{"mois": "2026-05", "logement_id": "LOG_0002",
                    "nb_menages_realises": 12}], "note": "..."}.
"""
import json
import os
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent  # <workspace>
MASTER = ROOT / "02_TRAVAIL" / "Lot1_Hostaway" / "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"
INJECTION = ROOT / "_stub_hostaway_injection.json"


def _charger_injection():
    if not INJECTION.exists():
        return {}
    try:
        return json.loads(INJECTION.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[stub_hostaway] injection illisible, ignorée : {exc}")
        return {}


def main():
    if not MASTER.exists():
        print(f"[stub_hostaway] BLOQUANT : MASTER Hostaway copié absent : {MASTER}")
        return 1
    inj = _charger_injection()
    wb = openpyxl.load_workbook(MASTER)
    n_applied = 0

    if "VUE_COMPTAGE" in wb.sheetnames and inj.get("vue_comptage"):
        ws = wb["VUE_COMPTAGE"]
        hdr = [c.value for c in ws[1]]
        try:
            i_mois = hdr.index("mois")
            i_log = hdr.index("logement_id")
            i_real = hdr.index("nb_menages_realises")
        except ValueError:
            i_mois = i_log = i_real = None
        if i_real is not None:
            cibles = {(str(d["mois"]), str(d["logement_id"])): d["nb_menages_realises"]
                      for d in inj["vue_comptage"]}
            for row in ws.iter_rows(min_row=2):
                cle = (str(row[i_mois].value), str(row[i_log].value))
                if cle in cibles:
                    row[i_real].value = cibles[cle]
                    n_applied += 1

    wb.save(MASTER)
    wb.close()
    print(f"[stub_hostaway] MASTER régénéré (copie) — injections VUE_COMPTAGE appliquées : {n_applied}")
    if inj.get("note"):
        print(f"[stub_hostaway] note : {inj['note']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

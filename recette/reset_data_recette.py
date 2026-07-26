#!/usr/bin/env python3
"""Réinitialise data_recette/ à l'état initial reproductible.

Sécurités :
- ne touche QUE data_recette/ sous le worktree courant ;
- refuse toute racine extérieure au worktree ;
- calcule les empreintes SHA-256 des fichiers sources RÉELS sensibles AVANT et APRÈS, et vérifie
  qu'elles sont inchangées (aucune écriture n'a fui vers le réel) ;
- idempotent (exécutable plusieurs fois de suite).
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

WT = Path(__file__).resolve().parent.parent
REC = WT / "data_recette"

# Fichiers RÉELS sensibles à surveiller (doivent rester inchangés).
FICHIERS_REELS_SURVEILLES = [
    WT / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm",
    WT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Flux.xlsx",
    WT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx",
    WT / "02_TRAVAIL" / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx",
    WT / "02_TRAVAIL" / "Lot7_IK_Avantages" / "MASTER_FACT_MAN_IK_Avantages.xlsx",
]


def _sha(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _empreintes() -> dict[str, str | None]:
    return {str(p): _sha(p) for p in FICHIERS_REELS_SURVEILLES}


def reset() -> dict:
    # Garde : la racine recette doit être sous le worktree, jamais ailleurs.
    rec = REC.resolve()
    if not str(rec).startswith(str(WT.resolve())) or rec.name != "data_recette":
        raise SystemExit(f"REFUS : racine recette inattendue : {rec}")

    avant = _empreintes()

    # Reconstruit intégralement via le générateur (qui ne touche que data_recette/).
    import importlib.util
    spec = importlib.util.spec_from_file_location("build_data_recette", WT / "recette" / "build_data_recette.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()

    apres = _empreintes()

    inchanges = all(avant[k] == apres[k] for k in avant)
    print("Racine recette réinitialisée :", rec)
    print("Fichiers réels sensibles inchangés :", "OUI" if inchanges else "NON")
    for k in avant:
        etat = "=" if avant[k] == apres[k] else "!! MODIFIÉ !!"
        print(f"  [{etat}] {Path(k).name}")
    if not inchanges:
        raise SystemExit("ALERTE : un fichier réel a changé pendant le reset.")
    return {"racine": str(rec), "reels_inchanges": inchanges}


if __name__ == "__main__":
    reset()

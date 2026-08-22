"""Scénarios de charges en RECETTE — confirmation réelle + impacts chiffrés attendus.

Lance `recette/run_scenarios.py` en sous-processus avec l'environnement recette isolé (writers vers
data_recette, moteur pandas). Chaque scénario est confirmé pour de vrai et ses chiffres écrits
(prise_en_compta, quotes-parts, réserve) sont comparés à des valeurs ATTENDUES explicites.

Skip automatique si l'interpréteur moteur (pandas) est absent — ce test exige la chaîne complète.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parents[1]
WT = APP_DIR.parent
ENGINE = Path(os.environ.get("LOT4A_ENGINE_PYTHON", r"C:\Program Files\Python312\python.exe"))
RUNNER = WT / "recette" / "run_scenarios.py"

pytestmark = pytest.mark.skipif(
    not ENGINE.exists() or not RUNNER.exists(),
    reason="moteur pandas ou runner scénarios absent (recette complète requise)",
)


def test_scenarios_charges_recette():
    rec = WT / "data_recette"
    env = dict(os.environ)
    env.update({
        "WT": str(WT),
        "PYTHONPATH": str(APP_DIR),
        "PROJECT_ROOT": str(rec),
        "APP_DATA_DIR": str(rec / "data"),
        "RECETTE_MODE": "1",
        "RECETTE_ROOT": str(rec),
        "CHARGES_REAL_WRITE_ENABLED": "1",
        "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED": "1",
        "LOT4A_ENGINE_PYTHON": str(ENGINE),
        "PYTHONUTF8": "1",
    })
    # `reset()` reconstruit intégralement data_recette/ (classeurs fictifs + import référentiel)
    # à chaque scénario, six fois. Mesuré à ~180 s par reconstruction sur ce poste (E/S synchronisées
    # OneDrive) : 600 s suffisaient quand la reconstruction ne prenait que quelques secondes, plus
    # aujourd'hui. Marge large plutôt qu'un chiffre ajusté au plus juste.
    proc = subprocess.run([sys.executable, str(RUNNER)], env=env, cwd=str(APP_DIR),
                          capture_output=True, text=True, timeout=1800)
    out = proc.stdout + proc.stderr
    assert "ALL SCENARIOS OK" in out, out[-2000:]
    assert proc.returncode == 0, out[-2000:]
    # Différenciation métier explicitement présente dans la sortie :
    assert "B: impact=IC compta=OUI" in out and "logement=LOG_A1" in out  # refacturable, 1 logement
    assert "D: impact=HC compta=NON" in out                               # HC hors comptabilité
    assert "H: impact=IC compta=OUI affectation=GLOBAL" in out            # 2 logements → GLOBAL

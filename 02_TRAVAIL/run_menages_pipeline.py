"""
run_menages_pipeline.py — Pipeline ménages (ordre garanti)
================================================================================
Lance, DANS L'ORDRE, le module ménages. lot6b est TOUJOURS exécuté en premier
pour rafraîchir M04 / MASTER_NORM depuis la Google Sheet AVANT le rapprochement
et les calculs (évite tout MASTER_NORM / M04 obsolète).

Ordre obligatoire :
  1. lot6b_m04_menages_internes.py      (alimente M04 + MASTER_NORM depuis REF/Google Sheet)
  2. lot6d_rapprochement_menages.py     (rapprochement volumes)
  3. lot6e_gainperte_menages.py         (gain/perte vs coût standard)
  4. lot6f_cout_complet_menages.py      (coût complet analytique)

Ne lance PAS lot9/10/11/12. Ne touche pas banque/Hostaway/factures/résultats aval.
Stoppe immédiatement (bloquant) si une étape échoue.
"""

import sys, os, subprocess

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
HERE = os.path.dirname(os.path.abspath(__file__))
STEPS = [
    "lot6b_m04_menages_internes.py",
    "lot6d_rapprochement_menages.py",
    "lot6e_gainperte_menages.py",
    "lot6f_cout_complet_menages.py",
]

def main():
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for i, script in enumerate(STEPS, 1):
        path = os.path.join(HERE, script)
        print(f"\n{'='*70}\n[{i}/{len(STEPS)}] {script}\n{'='*70}")
        r = subprocess.run([sys.executable, path], env=env)
        if r.returncode != 0:
            print(f"\n[BLOQUANT] Échec étape {i} ({script}, code {r.returncode}). Pipeline arrêté.")
            sys.exit(r.returncode)
    print(f"\n{'='*70}\n[OK] Pipeline ménages terminé ({len(STEPS)} étapes). lot9-12 NON relancés.\n{'='*70}")

if __name__ == "__main__":
    main()

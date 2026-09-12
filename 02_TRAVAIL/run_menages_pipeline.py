"""
run_menages_pipeline.py — Pipeline ménages (ordre garanti)
================================================================================
Lance, DANS L'ORDRE, le module ménages. lot6b est TOUJOURS exécuté en premier
pour rafraîchir menages_declarations_internes depuis la Google Sheet AVANT le
rapprochement et les calculs (évite tout dataset obsolète).

Ordre obligatoire :
  1. lot6b_m04_menages_internes.py      (alimente menages_declarations_internes, SQLite)
  2. lot6d_rapprochement_menages.py     (rapprochement volumes)
  3. lot6e_gainperte_menages.py         (gain/perte vs coût standard)
  4. lot6f_cout_complet_menages.py      (coût complet analytique)

`--source SQLITE --sans-excel` sur lot6d/6e/6f (mission « lot6c vers SQLite » §11) : sans ces
arguments, lot6d/6e retombaient sur leur défaut `--source EXCEL` — un chemin legacy réel, jamais
exercé par la chaîne de recette (`menages_chaine_service`) ni par le recalcul de production
(`menages_recalcul_service`, `orchestrateur_moteur`), mais que CE script, seul, pouvait encore
déclencher : il est cité tel quel (« commande ») dans `menages_service.load_dernier_calcul()`,
affiché sur `menages_diagnostic.html`. `--db` n'est volontairement PAS ajouté ici : ce script
attend son adressage par variables d'environnement héritées (`PILOTAGE_DB_PATH`/`APP_DATA_DIR`),
comme documenté à l'écran — un `--db` explicite en ferait un second mode d'adressage à maintenir.

Ne lance PAS lot9/10/11/12. Ne touche pas banque/Hostaway/factures/résultats aval.
Stoppe immédiatement (bloquant) si une étape échoue.
"""

import sys, os, subprocess

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
HERE = os.path.dirname(os.path.abspath(__file__))
STEPS = [
    ("lot6b_m04_menages_internes.py", ()),
    ("lot6d_rapprochement_menages.py", ("--source", "SQLITE", "--sans-excel")),
    ("lot6e_gainperte_menages.py", ("--source", "SQLITE", "--sans-excel")),
    ("lot6f_cout_complet_menages.py", ("--sans-excel",)),
]

def main():
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for i, (script, args) in enumerate(STEPS, 1):
        path = os.path.join(HERE, script)
        print(f"\n{'='*70}\n[{i}/{len(STEPS)}] {script} {' '.join(args)}\n{'='*70}")
        r = subprocess.run([sys.executable, path, *args], env=env)
        if r.returncode != 0:
            print(f"\n[BLOQUANT] Échec étape {i} ({script}, code {r.returncode}). Pipeline arrêté.")
            sys.exit(r.returncode)
    print(f"\n{'='*70}\n[OK] Pipeline ménages terminé ({len(STEPS)} étapes). lot9-12 NON relancés.\n{'='*70}")

if __name__ == "__main__":
    main()

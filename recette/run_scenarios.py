#!/usr/bin/env python3
"""Exécute les scénarios de charges en RECETTE : reset → prévisualisation → confirmation →
lecture de la charge écrite en SQLite. Valeurs attendues codées explicitement.

Doit être lancé avec l'environnement recette :
  PYTHONPATH=<...>/05_APPLICATION PROJECT_ROOT=<data_recette> APP_DATA_DIR=<data_recette>/data
  RECETTE_MODE=1 RECETTE_ROOT=<data_recette> WT=<worktree>

Sortie : une ligne par scénario + « ALL SCENARIOS OK » et sortie 0 si tout est conforme.

MIGRATION SQLITE (charges) — ce runner écrivait et relisait `SAISIE_Charges_Flux.xlsx` /
`SAISIE_Charges_Impacts.xlsx`, et déclenchait un recalcul Lot3/Lot7/Lot11 en sous-processus
(`python_moteur`) après confirmation. La charge vit maintenant dans la table SQLite `charges`
(migration 0052) : `charges_confirmation_service.confirmer` n'accepte plus `python_moteur` et
n'écrit plus aucun classeur. Les affectations multi-logements et la réserve de refacturation
n'étaient déjà consommées par aucun calcul aval (Lot10/Lot12/comptabilité) — seulement affichées en
prévisualisation puis notées dans le classeur Impacts à titre de bookkeeping — leur disparition ne
change donc aucun résultat économique réel ; les scénarios ci-dessous vérifient désormais
`affectation_type`/`logement_id` sur la charge elle-même plutôt qu'un classeur de ventilation.
"""
import contextlib
import importlib.util
import io
import os
import sys
from pathlib import Path

WT = Path(os.environ["WT"]).resolve()
REC = WT / "data_recette"


def reset():
    """Reconstruit data_recette/ (classeurs fictifs) PUIS initialise le référentiel SQLite.

    `build_data_recette.main()` ne produit encore que des classeurs (REF_Setup.xlsm fictif compris) :
    aucune table `ref_*` n'était peuplée avant ce runner, ce qui faisait échouer toute
    prévisualisation dès la première règle métier lue en base (catégorie, mode de paiement,
    référentiel de clôture...). Le classeur REF_Setup fictif reste un IMPORT_PONCTUEL légitime — on
    réutilise le service d'import existant (`ref_setup_import_service`, migration 0051) plutôt que de
    réécrire un second chemin de seeding.
    """
    spec = importlib.util.spec_from_file_location("bdr", WT / "recette" / "build_data_recette.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    with contextlib.redirect_stdout(io.StringIO()):
        mod.main()
    from app.db.connection import apply_migrations
    apply_migrations()

    from app.services import ref_setup_import_service as ref_import
    ref_setup_path = REC / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm"
    res = ref_import.importer(chemin=ref_setup_path)
    if not res.get("ok"):
        raise RuntimeError(f"Import référentiel recette refusé : {res.get('code')} {res.get('message')}")


def read_charge(charge_id: str) -> dict:
    """Relit la charge confirmée directement dans la table SQLite `charges`."""
    from app.db.connection import get_db
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM charges WHERE charge_id = ?", (charge_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else {}


SC = {
 "A": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
 "B": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "OUI"},
 "C": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_004", "associe_id": "PERS_X", "refacturable": "NON", "commentaire": "FICTIF compte perso"},
 "D": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "HC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "NON", "commentaire": "FICTIF hors compta"},
 "H": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1", "LOG_A2"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
 "I": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1", "LOG_B1"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
}

# affectation_type/logement_id : LOGEMENT+id pour une charge à un seul logement final, GLOBAL+None
# dès que le périmètre en compte 0 ou plusieurs (cf. `charges_preview_service._build_row_data`).
# proprietaire_id : matérialisé seulement si un propriétaire unique se dégage du périmètre.
EXPECT = {
 "A": {"prise_en_compta": "OUI", "affectation_type": "LOGEMENT", "logement_id": "LOG_A1",
       "proprietaire_id": "PROP_A"},
 "B": {"prise_en_compta": "OUI", "affectation_type": "LOGEMENT", "logement_id": "LOG_A1",
       "proprietaire_id": "PROP_A"},
 "C": {"prise_en_compta": "OUI", "affectation_type": "LOGEMENT", "logement_id": "LOG_A1",
       "proprietaire_id": "PROP_A"},
 "D": {"prise_en_compta": "NON", "affectation_type": "LOGEMENT", "logement_id": "LOG_A1",
       "proprietaire_id": "PROP_A"},
 # LOG_A1 + LOG_A2 partagent le même propriétaire (PROP_A) dans le jeu fictif : matérialisé même
 # si le périmètre final compte deux logements.
 "H": {"prise_en_compta": "OUI", "affectation_type": "GLOBAL", "logement_id": None,
       "proprietaire_id": "PROP_A"},
 # LOG_A1 (PROP_A) + LOG_B1 (PROP_B) : propriétaires distincts → aucune attribution arbitraire.
 "I": {"prise_en_compta": "OUI", "affectation_type": "GLOBAL", "logement_id": None,
       "proprietaire_id": None},
}


def main():
    from app.services.charges_preview_service import previsualiser
    from app.services import charges_confirmation_service as conf

    all_ok = True
    for name, form in SC.items():
        reset()
        r = previsualiser(dict(form))
        assert r["ok"], f"{name}: preview refusé {[e['code'] for e in r['manifest']['errors']]}"
        res = conf.confirmer(r["token"], acteur="recette")
        d = res.as_dict()
        assert d.get("statut") == "SUCCES", f"{name}: confirm {d.get('statut')} {d.get('code')}"

        charge = read_charge(d["charge_id"])
        assert charge, f"{name}: charge {d['charge_id']} introuvable en SQLite"

        exp = EXPECT[name]
        checks = {
            "montant": _round(charge.get("montant")) == 100.0,
            "prise_en_compta": charge.get("prise_en_compta") == exp["prise_en_compta"],
            "affectation_type": charge.get("affectation_type") == exp["affectation_type"],
            "logement_id": charge.get("logement_id") == exp["logement_id"],
            "proprietaire_id": charge.get("proprietaire_id") == exp["proprietaire_id"],
        }
        ok = all(checks.values())
        all_ok = all_ok and ok
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: impact={charge.get('code_impact')} "
              f"compta={charge.get('prise_en_compta')} affectation={charge.get('affectation_type')} "
              f"logement={charge.get('logement_id')} proprietaire={charge.get('proprietaire_id')}"
              f"{'' if ok else ' ECHECS=' + str([k for k, v in checks.items() if not v])}")

    print("ALL SCENARIOS OK" if all_ok else "SOME SCENARIOS FAILED")
    return 0 if all_ok else 1


def _round(v):
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""AVANT/APRÈS financier réel : baseline (lot9+lot10) → confirmation charge → recalcul lot9+lot10 →
lecture des impacts finaux (net propriétaire, résultat logement REEL/COMPTABLE, reste à payer,
préfacture). Pour scénarios A, B, D, H, I. Reset complet entre chaque.
"""
import contextlib
import importlib.util
import io
import os
import subprocess
import sys
from pathlib import Path

import openpyxl

WT = Path(os.environ["WT"]).resolve()
REC = WT / "data_recette"
ENGINE = Path(os.environ.get("LOT4A_ENGINE_PYTHON", r"C:\Program Files\Python312\python.exe"))
NETF = REC / "02_TRAVAIL" / "Lot10_Resultats" / "MASTER_CALC_NetProprietaire.xlsx"
RESF = REC / "02_TRAVAIL" / "Lot10_Resultats" / "MASTER_CALC_Resultats.xlsx"


def _load(mod_path):
    spec = importlib.util.spec_from_file_location(Path(mod_path).stem, mod_path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m


def full_reset():
    with contextlib.redirect_stdout(io.StringIO()):
        _load(WT / "recette" / "build_data_recette.py").main()
        _load(WT / "recette" / "build_reservations_recette.py").build()
    from app.db.connection import apply_migrations
    apply_migrations()


def run_pipeline():
    for lot in ("lot9_construire_flux.py", "lot10_calculer_resultats.py"):
        p = subprocess.run([str(ENGINE), str(REC / "02_TRAVAIL" / lot)], cwd=str(REC),
                           capture_output=True, text=True, timeout=300)
        if p.returncode != 0:
            raise SystemExit(f"{lot} rc={p.returncode}:\n" + (p.stdout + p.stderr)[-1500:])


def valider_charges_master():
    """Simule l'étape de validation humaine : passe les charges du MASTER de A_CONTROLER à VALIDE
    (sans quoi Lot9 ne les ingère pas — règle métier : seules les charges VALIDE impactent le net)."""
    f = REC / "02_TRAVAIL" / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx"
    wb = openpyxl.load_workbook(f)
    ws = wb["MASTER"]
    hdr = [c.value for c in ws[1]]
    if "statut_controle" not in hdr or "charge_id" not in hdr:
        wb.close(); return 0
    ci = hdr.index("charge_id"); si = hdr.index("statut_controle")
    n = 0
    for row in ws.iter_rows(min_row=2):
        cid = row[ci].value
        if cid and not str(cid).startswith(("[", "#")):
            row[si].value = "VALIDE"; n += 1
    wb.save(f); wb.close()
    return n


def _rows(path, sheet):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]; data = list(ws.iter_rows(values_only=True))
    wb.close()
    hdr = data[0]
    return [dict(zip(hdr, r)) for r in data[1:]]


def snapshot(mois="2026-06"):
    net = {r["proprietaire_id"]: r for r in _rows(NETF, "VUE_MOIS") if r.get("mois") == mois}
    # résultat logement : SOMME de toutes les lignes (réservation + charge) par (logement, vision)
    res_log = {}
    for r in _rows(RESF, "PAR_MOIS_LOGEMENT"):
        if r.get("mois") != mois:
            continue
        k = (r["logement_id"], r["vision"])
        res_log[k] = res_log.get(k, 0) + (r.get("resultat") or 0)
    return {"net": net, "res_log": res_log}


def fmt(s, prop="PROP_A", log="LOG_A1"):
    n = s["net"].get(prop, {})
    return {
        "net_prop": n.get("net_proprietaire_apres_charge_mois"),
        "reste_a_payer": n.get("reste_a_payer_conciergerie"),
        "prefacture": n.get("charges_exceptionnelles_refacturees"),
        "res_log_reel": round(s["res_log"].get((log, "REEL"), 0), 2),
        "res_log_compta": round(s["res_log"].get((log, "COMPTABLE"), 0), 2),
    }


SC = {
 "A": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
 "B": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "OUI"},
 "B2": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "proprietaires": ["PROP_A"], "mode_paiement_id": "PAY_001", "refacturable": "OUI"},
 "C": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_004", "associe_id": "PERS_X", "refacturable": "NON", "commentaire": "FICTIF payee hors banque conciergerie"},
 "D": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "HC", "logements": ["LOG_A1"], "mode_paiement_id": "PAY_001", "refacturable": "NON", "commentaire": "FICTIF hors compta"},
 "H": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1", "LOG_A2"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
 "I": {"date_charge": "2026-06-15", "montant": "100", "categorie_charge_id": "CHG_008", "code_impact": "IC", "logements": ["LOG_A1", "LOG_B1"], "mode_paiement_id": "PAY_001", "refacturable": "NON"},
}


def main():
    from app.services.charges_preview_service import previsualiser
    from app.services import charges_confirmation_service as conf
    only = sys.argv[1:] or list(SC)
    for name in only:
        form = SC[name]
        full_reset()
        run_pipeline()
        before = fmt(snapshot())
        r = previsualiser(dict(form))
        assert r["ok"], f"{name} preview {r['manifest']['errors']}"
        d = conf.confirmer(r["token"], python_moteur=ENGINE).as_dict()
        assert d.get("statut") == "SUCCES", f"{name} confirm {d.get('code')}"
        valider_charges_master()   # étape de validation humaine (A_CONTROLER → VALIDE)
        run_pipeline()
        after = fmt(snapshot())
        cible = form.get('logements') or form.get('proprietaires')
        print(f"\n===== SCENARIO {name} (charge 100 € {form['code_impact']} "
              f"refac={form.get('refacturable')} cible={cible}) =====")
        print(f"{'indicateur':<22}{'AVANT':>10}{'APRÈS':>10}{'Δ':>10}")
        for k in ("res_log_reel", "res_log_compta", "net_prop", "reste_a_payer", "prefacture"):
            b, a = before[k], after[k]
            delta = (a - b) if (isinstance(a, (int, float)) and isinstance(b, (int, float))) else "—"
            print(f"{k:<22}{str(b):>10}{str(a):>10}{str(delta):>10}")
        if name == "I":
            bf = fmt(snapshot(), prop="PROP_B", log="LOG_B1")  # note: after-state
            print(f"  PROP_B net après: {bf['net_prop']} | res LOG_B1 réel: {bf['res_log_reel']}")


if __name__ == "__main__":
    main()

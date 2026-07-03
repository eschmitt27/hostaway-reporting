"""LOT4A — Comparateur LECTURE SEULE : SAISIE ReservationsHH (recalcul Python) vs MASTER historique.

Mode unique : --compare-existing (lecture seule). AUCUN transformateur, AUCUN --write-master.

Principe :
  - lit SAISIE_ReservationsHorsHostaway.xlsx (structurel data_only=False pour le schema/formules,
    valeurs data_only=True pour les champs manuels) ;
  - recalcule les 9 champs derives en Python avec pandas/numpy (meme primitive que lot10) ;
  - resout le taux historique via lib_ref_history.resolve_commission_rate (date de reference = check-in) ;
  - compare au MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx (data_only=True) ;
  - categorise chaque champ, sans jamais dependre d'une valeur mise en cache par Excel pour les derives ;
  - ecrit un rapport UNIQUEMENT sous 04_LOGS/LOT4A_COMPARE/<horodatage-UTC>/.

Interdits garantis : aucune ecriture des classeurs metier, aucun wb.save/to_excel, aucun COM,
aucun subprocess, aucun saisie_writer. Hashes SHA-256 + taille + mtime_ns des 3 sources verifies avant/apres.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import sys
from pathlib import Path

import openpyxl

# Regles UNIQUES centralisees dans la bibliotheque partagee (aucune duplication).
from lib_lot4a_reservations_hh import (
    PROJECT_ROOT as ROOT,
    SAISIE_PATH, MASTER_PATH, REF_SETUP_PATH,
    MASTER_COLUMNS, DERIVED_FIELDS, SYSTEM_FIELDS, METADATA_FIELDS, TAUX_DEPENDENT,
    round2, _num, _norm, to_date as _to_date,
    read_saisie_values, check_saisie_formulas, read_master_values, read_taux_rows,
    recompute, file_fingerprint, assert_output_under,
)

LOGS_ROOT = ROOT / "04_LOGS" / "LOT4A_COMPARE"


# ── Garde de chemin propre au comparateur (delegue a la garde generique) ─────
def assert_output_allowed(path: Path) -> Path:
    return assert_output_under(path, LOGS_ROOT)


# ── Comparaison / categorisation ────────────────────────────────────────────
def _norm_compare(field: str, value):
    """Normalisation pour comparaison manuel SAISIE vs MASTER."""
    if value is None or value == "":
        return ""
    if field in ("date_arrivee", "date_depart"):
        d = _to_date(value)
        return d.strftime("%Y-%m-%d") if d else _norm(value)
    if field in ("total_percu", "menage", "montant_recupere", "montant_reverse_proprietaire"):
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return _norm(value)
    if field == "reservation_id_hostaway":
        try:
            return str(int(float(value)))
        except (TypeError, ValueError):
            return _norm(value)
    return _norm(value)


def _derived_str(field: str, value):
    if value is None:
        return ""
    if field == "nuits" and value != "":
        return str(int(value))
    if field in ("commission", "acompte_facture", "taux_commission") and value != "":
        return f"{float(value):g}"
    return _norm(value)


def build_rows(saisie_list, master_map, taux_rows, formula_check):
    """Retourne (lignes_csv, compteurs, statut_global)."""
    lignes = []
    counters = {k: 0 for k in (
        "MANUEL_IDENTIQUE", "MANUEL_DIFFERENT", "DERIVE_COHERENT",
        "ECART_HISTORIQUE_ATTENDU", "METADONNEE_NON_COMPARABLE", "TAUX_BLOQUANT",
    )}
    any_taux_block = False
    any_hist_ecart = False

    saisie_pks = {_norm(s.get("reservation_hh_id")) for s in saisie_list}
    master_pks = set(master_map.keys())

    for saisie in saisie_list:
        pk = _norm(saisie.get("reservation_hh_id"))
        master = master_map.get(pk, {})
        rc = recompute(saisie, taux_rows)
        derived = rc["derived"]
        taux_status = rc["taux_status"]

        # ligne de controle dediee si taux bloquant
        if taux_status in ("MISSING", "AMBIGUOUS"):
            any_taux_block = True
            counters["TAUX_BLOQUANT"] += 1
            lignes.append({
                "reservation_hh_id": pk, "champ": "__TAUX_HISTORIQUE__",
                "valeur_recalcul_python": "", "valeur_master_actuel": _norm(master.get("taux_commission")),
                "categorie_principale": "TAUX_BLOQUANT", "indicateur_impact_aval": "",
                "commentaire": f"{taux_status}: {rc['taux_message']} — champs derives NON calcules",
            })

        for field in MASTER_COLUMNS:
            master_val = master.get(field)
            impact = ""

            if field in METADATA_FIELDS:
                cat = "METADONNEE_NON_COMPARABLE"
                recalc_str = "(execution)"
                counters[cat] += 1
                commentaire = "Metadonnee d'execution — non comparable"
            elif field in DERIVED_FIELDS or field in SYSTEM_FIELDS:
                if field in TAUX_DEPENDENT and taux_status != "OK":
                    cat = "TAUX_BLOQUANT"
                    recalc_str = ""
                    commentaire = "Non calcule — taux historique bloquant"
                    # deja compte via ligne de controle ; ne pas recompter
                else:
                    recalc_val = derived.get(field)
                    recalc_str = _derived_str(field, recalc_val)
                    mst_str = _derived_str(field, master_val)
                    if _norm(master_val) == "":
                        cat = "ECART_HISTORIQUE_ATTENDU"
                        commentaire = "MASTER LEGACY INCOMPLET — valeur absente dans le MASTER historique"
                        any_hist_ecart = True
                        counters[cat] += 1
                    elif mst_str == recalc_str:
                        cat = "DERIVE_COHERENT"
                        commentaire = ""
                        counters[cat] += 1
                    else:
                        cat = "ECART_HISTORIQUE_ATTENDU"
                        commentaire = "MASTER LEGACY DIVERGENT — valeur presente differe du recalcul"
                        any_hist_ecart = True
                        counters[cat] += 1
                    if field == "acompte_facture" and recalc_val not in (None, "", 0, 0.0):
                        try:
                            if float(recalc_val) > 0:
                                impact = "ACOMPTE_POSITIF_A_VERIFIER"
                        except (TypeError, ValueError):
                            pass
            else:
                # champ manuel
                recalc_str = _norm_compare(field, saisie.get(field))
                mst_str = _norm_compare(field, master_val)
                if mst_str == recalc_str:
                    cat = "MANUEL_IDENTIQUE"
                    commentaire = ""
                else:
                    cat = "MANUEL_DIFFERENT"
                    commentaire = "Champ manuel different entre SAISIE et MASTER"
                counters[cat] += 1

            lignes.append({
                "reservation_hh_id": pk, "champ": field,
                "valeur_recalcul_python": recalc_str,
                "valeur_master_actuel": _norm_compare(field, master_val) if field not in DERIVED_FIELDS | SYSTEM_FIELDS | METADATA_FIELDS else _derived_str(field, master_val),
                "categorie_principale": cat, "indicateur_impact_aval": impact,
                "commentaire": commentaire,
            })

    # reservations orphelines
    for pk in sorted(saisie_pks - master_pks):
        lignes.append({"reservation_hh_id": pk, "champ": "__PRESENCE__",
                       "valeur_recalcul_python": "SAISIE", "valeur_master_actuel": "ABSENT",
                       "categorie_principale": "ECART_HISTORIQUE_ATTENDU", "indicateur_impact_aval": "",
                       "commentaire": "Presente dans SAISIE, absente du MASTER"})
        any_hist_ecart = True
    for pk in sorted(master_pks - saisie_pks):
        lignes.append({"reservation_hh_id": pk, "champ": "__PRESENCE__",
                       "valeur_recalcul_python": "ABSENT", "valeur_master_actuel": "MASTER",
                       "categorie_principale": "ECART_HISTORIQUE_ATTENDU", "indicateur_impact_aval": "",
                       "commentaire": "Presente dans MASTER, absente de SAISIE"})
        any_hist_ecart = True

    if any_taux_block:
        statut = "ANALYSE_BLOQUEE_TAUX"
    elif any_hist_ecart:
        statut = "ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES"
    else:
        statut = "ANALYSE_TERMINEE"
    return lignes, counters, statut


# ── Ecriture des rapports ────────────────────────────────────────────────────
def write_reports(out_dir: Path, lignes, counters, statut, manifest) -> dict:
    out_dir = assert_output_allowed(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = assert_output_allowed(out_dir / "rapport.csv")
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["reservation_hh_id", "champ", "valeur_recalcul_python",
                    "valeur_master_actuel", "categorie_principale", "indicateur_impact_aval", "commentaire"])
        for l in lignes:
            w.writerow([l["reservation_hh_id"], l["champ"], l["valeur_recalcul_python"],
                        l["valeur_master_actuel"], l["categorie_principale"],
                        l["indicateur_impact_aval"], l["commentaire"]])

    md_path = assert_output_allowed(out_dir / "rapport.md")
    md = _render_md(lignes, counters, statut, manifest)
    md_path.write_text(md, encoding="utf-8")

    manifest_path = assert_output_allowed(out_dir / "manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    return {"csv": str(csv_path), "md": str(md_path), "manifest": str(manifest_path)}


def _render_md(lignes, counters, statut, manifest) -> str:
    L = []
    L.append(f"# LOT4A — Comparateur SAISIE ReservationsHH vs MASTER (lecture seule)\n")
    L.append(f"**Statut** : `{statut}`  ")
    L.append(f"**Horodatage UTC** : {manifest['horodatage_utc']}  ")
    L.append(f"**as-of** : {manifest['as_of']}  ")
    L.append(f"**Interpreteur** : `{manifest['python_executable']}` (Python {manifest['python_version']})  ")
    L.append(f"**openpyxl** {manifest['openpyxl']}\n")
    L.append("## Compteurs par categorie\n")
    for k, v in counters.items():
        L.append(f"- `{k}` : {v}")
    L.append("\n## Ecarts manuels (SAISIE ≠ MASTER)\n")
    man = [l for l in lignes if l["categorie_principale"] == "MANUEL_DIFFERENT"]
    L.append("_Aucun._" if not man else "\n".join(f"- {l['reservation_hh_id']} · {l['champ']} : recalc=`{l['valeur_recalcul_python']}` / master=`{l['valeur_master_actuel']}`" for l in man))
    L.append("\n## Ecarts historiques legacy (MASTER incomplet/divergent)\n")
    hist = [l for l in lignes if l["categorie_principale"] == "ECART_HISTORIQUE_ATTENDU"]
    L.append("_Aucun._" if not hist else "\n".join(f"- {l['reservation_hh_id']} · {l['champ']} : {l['commentaire']}" for l in hist))
    L.append("\n## Reservations a taux bloquant\n")
    tb = [l for l in lignes if l["champ"] == "__TAUX_HISTORIQUE__"]
    L.append("_Aucune._" if not tb else "\n".join(f"- {l['reservation_hh_id']} : {l['commentaire']}" for l in tb))
    L.append("\n## Acomptes positifs a analyser (impact lot5/lot12)\n")
    ac = [l for l in lignes if l["indicateur_impact_aval"] == "ACOMPTE_POSITIF_A_VERIFIER"]
    L.append("_Aucun._" if not ac else "\n".join(f"- {l['reservation_hh_id']} · acompte_facture recalc=`{l['valeur_recalcul_python']}`" for l in ac))
    L.append("\n## Oracle RESHH-2026-05-001\n")
    oracle = [l for l in lignes if l["reservation_hh_id"] == "RESHH-2026-05-001"
              and l["champ"] in ("taux_commission", "commission", "acompte_facture")]
    for l in oracle:
        L.append(f"- {l['champ']} = `{l['valeur_recalcul_python']}` (master=`{l['valeur_master_actuel']}`, {l['categorie_principale']})")
    L.append("\n## Invariance des sources (lecture seule)\n")
    for name, fp in manifest["fingerprints_avant"].items():
        apres = manifest["fingerprints_apres"].get(name, {})
        inchange = fp == apres
        L.append(f"- `{name}` : {'INCHANGE' if inchange else 'MODIFIE !!!'} (sha256 {fp.get('sha256','?')[:12]}…)")
    return "\n".join(L) + "\n"


# ── CLI ──────────────────────────────────────────────────────────────────────
def run_compare_existing(as_of: str | None) -> int:
    horodatage = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for p in (SAISIE_PATH, MASTER_PATH, REF_SETUP_PATH):
        if not p.exists():
            print(f"ERREUR_TECHNIQUE : fichier introuvable {p}", file=sys.stderr)
            return 2

    sources = {
        "SAISIE_ReservationsHorsHostaway.xlsx": SAISIE_PATH,
        "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx": MASTER_PATH,
        "REF_Setup.xlsm": REF_SETUP_PATH,
    }
    fp_avant = {name: file_fingerprint(p) for name, p in sources.items()}

    try:
        formula_check = check_saisie_formulas()
        saisie_list = read_saisie_values()
        master_map = read_master_values()
        taux_rows = read_taux_rows()
    except Exception as exc:  # noqa: BLE001
        print(f"ERREUR_TECHNIQUE : {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    lignes, counters, statut = build_rows(saisie_list, master_map, taux_rows, formula_check)

    fp_apres = {name: file_fingerprint(p) for name, p in sources.items()}
    sources_inchangees = fp_avant == fp_apres

    manifest = {
        "horodatage_utc": horodatage,
        "as_of": as_of or "(non fourni)",
        "mode": "--compare-existing",
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "openpyxl": openpyxl.__version__,
        "statut": statut,
        "compteurs": counters,
        "formules_saisie_presentes": formula_check,
        "nb_saisie": len(saisie_list),
        "nb_master": len(master_map),
        "fingerprints_avant": fp_avant,
        "fingerprints_apres": fp_apres,
        "sources_inchangees": sources_inchangees,
        "aucune_ecriture_metier": True,
    }

    out_dir = LOGS_ROOT / horodatage
    paths = write_reports(out_dir, lignes, counters, statut, manifest)

    if not sources_inchangees:
        print("ERREUR_TECHNIQUE : une source metier a change pendant l'execution", file=sys.stderr)
        return 2

    print(f"Statut     : {statut}")
    print(f"Rapport    : {paths['md']}")
    print(f"CSV        : {paths['csv']}")
    print(f"Manifest   : {paths['manifest']}")
    print(f"Compteurs  : {counters}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="LOT4A comparateur lecture seule SAISIE vs MASTER (ReservationsHH).")
    parser.add_argument("--compare-existing", action="store_true", help="Mode comparaison lecture seule (unique mode disponible).")
    parser.add_argument("--as-of", default=None, help="Metadonnee de rapport uniquement — n'affecte aucun calcul metier.")
    args = parser.parse_args(argv)
    if not args.compare_existing:
        parser.error("Seul --compare-existing est disponible a ce stade.")
    return run_compare_existing(args.as_of)


if __name__ == "__main__":
    raise SystemExit(main())

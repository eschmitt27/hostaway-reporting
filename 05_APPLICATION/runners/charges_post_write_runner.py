"""APP-3b — Runner moteur POST-ÉCRITURE des charges. Exécuté HORS du processus FastAPI.

Ce fichier est un **script**, jamais un module importé par l'application : il est lancé en
sous-processus avec l'interpréteur moteur (celui qui porte pandas), sur le modèle éprouvé du
`lot4a_dryrun_runner` (APP-2c). C'est ce qui permet à l'app de déclencher les lots **sans jamais
importer le moteur** ni recoder une règle métier.

Il vit délibérément **hors du paquet `app/`** : le garde-fou `test_no_import_of_travail_modules`
interdit tout import du moteur sous `app/`, et cette interdiction est saine. Plutôt que de lui
ajouter une exception, on matérialise la frontière dans l'arborescence — ce script n'est pas du
code applicatif, il s'exécute dans un autre processus et un autre interpréteur.

Entrée : un JSON de chemins DÉJÀ résolus par l'appelant. Sortie : un JSON de résultats.

Enchaînement (Lot3 et Lot7 sont HORS de la transaction Excel) :
  1. Lot3  — régénère MASTER_FACT_MAN_Charges depuis SAISIE_Charges_Flux (sans quoi la charge
             resterait invisible pour Lot9/Lot10/Lot11/Lot12) ;
  2. Lot7  — régénère MASTER_CALC_AVANTAGES, uniquement si la charge porte un avantage associé ;
  3. Lot11 — contrôles du suivi associé (lecture seule).

Chaque étape est indépendante : l'échec de l'une n'annule JAMAIS la charge déjà écrite. Le statut
de chaque lot est rendu séparément, à charge de l'appelant de le restituer sans mentir.
"""
from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any

OK = "OK"
ECHEC = "ECHEC"
NON_APPLICABLE = "NON_APPLICABLE"
ANOMALIES = "ANOMALIES"


def _etape_echec(exc: BaseException) -> dict[str, Any]:
    return {
        "statut": ECHEC,
        "details": f"{type(exc).__name__}: {exc}",
        "trace": traceback.format_exc(limit=3),
    }


def _lot3(saisie: str, ref: str, master: str) -> dict[str, Any]:
    import lot3_generateur_charges as gen  # moteur — importé ICI, jamais dans FastAPI

    res = gen.generer(saisie, ref, master)
    return {
        "statut": OK,
        "details": f"MASTER régénéré : {res['nb_lignes']} ligne(s), "
                   f"{res['nb_vue_menage']} en VUE_MENAGE.",
        "nb_lignes": res["nb_lignes"],
        "nb_vue_menage": res["nb_vue_menage"],
        "anomalies": res["anomalies"],
    }


def _lot7(saisie: str, lot7: str) -> dict[str, Any]:
    import lot7_generateur_avantages as gen

    res = gen.generer(saisie, lot7, lot7)
    return {
        "statut": OK,
        "details": f"MASTER_CALC_AVANTAGES régénéré : {res['nb_lignes']} ligne(s) (HR).",
        "nb_lignes": res["nb_lignes"],
        "anomalies": res["anomalies"],
    }


def _lot11(saisie: str, lot7: str) -> dict[str, Any]:
    import lot11_controles_coherence as ctl   # nécessite pandas → interpréteur moteur

    df_ik = ctl._read_sheet(lot7, sheet="MASTER_CALC_AVANTAGES")
    anomalies = ctl.controles_suivi_associe(df_ik, saisie, lot7)
    bloquantes = [a for a in anomalies if a.get("severity") == "BLOQUANT"]
    a_controler = [a for a in anomalies if a.get("severity") == "A_CONTROLER"]
    statut = ANOMALIES if (bloquantes or a_controler) else OK
    return {
        "statut": statut,
        "details": (
            f"{len(bloquantes)} bloquante(s), {len(a_controler)} à contrôler."
            if statut == ANOMALIES else "Aucune anomalie."
        ),
        "anomalies": [
            {"code": a.get("code"), "severity": a.get("severity"), "message": a.get("message")}
            for a in anomalies
        ],
    }


def _main(input_path: str, output_path: str) -> int:
    requete = json.loads(Path(input_path).read_text(encoding="utf-8"))

    moteur = Path(requete["project_root"]) / "02_TRAVAIL"
    if str(moteur) not in sys.path:
        sys.path.insert(0, str(moteur))

    reponse: dict[str, Any] = {"ok": True, "charge_id": requete.get("charge_id")}

    # ── Lot3 (toujours) ──────────────────────────────────────────────────────
    try:
        reponse["lot3"] = _lot3(requete["saisie"], requete["ref"], requete["master_charges"])
    except Exception as exc:
        reponse["lot3"] = _etape_echec(exc)
        reponse["ok"] = False

    # ── Lot7 (seulement si avantage associé) ─────────────────────────────────
    if requete.get("avantage"):
        try:
            reponse["lot7"] = _lot7(requete["saisie"], requete["lot7"])
        except Exception as exc:
            reponse["lot7"] = _etape_echec(exc)
            reponse["ok"] = False
    else:
        reponse["lot7"] = {"statut": NON_APPLICABLE,
                           "details": "La charge ne porte pas d'avantage associé."}

    # ── Lot11 (contrôles, lecture seule) ─────────────────────────────────────
    try:
        reponse["lot11"] = _lot11(requete["saisie"], requete["lot7"])
    except Exception as exc:
        reponse["lot11"] = _etape_echec(exc)
        reponse["ok"] = False

    Path(output_path).write_text(
        json.dumps(reponse, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return 0        # le code retour ne porte JAMAIS l'échec métier : il est dans la réponse JSON


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: charges_post_write_runner.py requete.json reponse.json")
    raise SystemExit(_main(sys.argv[1], sys.argv[2]))

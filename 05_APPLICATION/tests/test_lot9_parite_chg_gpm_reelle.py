"""Lot9 — parité RÉELLE modules CHG et GPM (mission "FERMER LES DEUX GATES" §19-24).

CHG : `_module_chg` lit `charges_reader.read_charges()` -> `app.config.MASTER_CHARGES`
(`02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx`), EXACTEMENT le même fichier que
`SRC_CHG` du script legacy `02_TRAVAIL/lot9_construire_flux.py`. Le classeur réel ne contient
qu'1 ligne placeholder Power Query, `statut_controle` vide (jamais VALIDE) : OLD=0 et NEW=0 sur le
MÊME état de donnée, pas par coïncidence de deux sources vides indépendantes — vérifié ici en
relisant le fichier réel, pas en le mockant.

GPM : `_module_gpm` lit la table SQLite `menages_cout_complet` (colonnes miroir de l'onglet
DETAIL_COUT_COMPLET du classeur `MASTER_CALC_CoutComplet_Menages.xlsx` legacy). Reprise de
PARITÉ (§12) des 16 lignes réelles dans `menages_cout_complet`, puis comparaison OLD (lu depuis le
classeur réel) vs NEW (flux_unifie_service.construire()).
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from app.db.connection import get_db

MASTER_CHG = (Path(__file__).resolve().parents[2]
             / "02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx")
MASTER_GPM = (Path(__file__).resolve().parents[2]
             / "02_TRAVAIL/Lot6f_CoutComplet_Menages/MASTER_CALC_CoutComplet_Menages.xlsx")


def _lire_onglet(chemin: Path, onglet: str) -> list[dict]:
    wb = openpyxl.load_workbook(str(chemin), read_only=True, data_only=True)
    try:
        ws = wb[onglet]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    hdr = [str(c) for c in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:]]


@pytest.mark.skipif(not MASTER_CHG.exists(), reason="Master CHG legacy réel absent")
def test_parite_chg_reelle(tmp_db, monkeypatch):
    from app import config as cfg
    from app.services import flux_unifie_service as svc

    # Même fichier lu des deux côtés (chemin identique à SRC_CHG du script legacy) : preuve que
    # NEW=0 reflète le même état de donnée que OLD=0, pas une coïncidence de sources différentes.
    lignes = _lire_onglet(MASTER_CHG, "MASTER")
    old_valides = [l for l in lignes if l.get("statut_controle") == "VALIDE"]
    assert old_valides == [], (
        f"Baseline CHG legacy a changé sur le classeur réel : {len(old_valides)} ligne(s) "
        f"VALIDE trouvée(s) — la comparaison 0=0 n'est plus valide, revérifier.")

    monkeypatch.setattr(cfg, "MASTER_CHARGES", MASTER_CHG)
    resultat = svc.construire(db_path=tmp_db)
    assert resultat["ok"], resultat
    flux = svc.lire(db_path=tmp_db)
    chg = [f for f in flux if f["source_table"] == "MASTER_FACT_MAN_Charges"]
    assert chg == [], f"CHG : NEW attendu 0, obtenu {len(chg)}"
    print(f"CHG | OLD 0/0.0€ | NEW {len(chg)}/0.0€ | DELTA_COUNT 0 | DELTA_SUM 0.0€")


def _reprendre_gpm(db_path, lignes: list[dict]) -> None:
    conn = get_db(db_path)
    try:
        for l in lignes:
            conn.execute(
                "INSERT INTO menages_cout_complet (mois, logement_id, nom_appartement, "
                "proprietaire_id, intervenant_id, cout_standard_total, ecart_vs_standard_total, "
                "statut_ecart, statut_controle) VALUES (?,?,?,?,?,?,?,?,?)",
                (l["mois"], l["logement_id"], l.get("nom_appartement"), l.get("proprietaire_id"),
                 l.get("intervenant_id"), l.get("cout_standard_total"),
                 l.get("ecart_vs_standard_total"), l.get("statut_ecart"),
                 l.get("statut_controle")))
        conn.commit()
    finally:
        conn.close()


@pytest.mark.skipif(not MASTER_GPM.exists(), reason="Master GPM legacy réel absent")
def test_parite_gpm_reelle(tmp_db):
    from app.services import flux_unifie_service as svc

    lignes = _lire_onglet(MASTER_GPM, "DETAIL_COUT_COMPLET")

    old_std = [l for l in lignes if l.get("cout_standard_total") not in (None, 0)]
    old_ecart = [l for l in lignes if l.get("ecart_vs_standard_total") not in (None, 0)]
    old_count = len(old_std) + len(old_ecart)
    old_sum = round(sum(float(l["cout_standard_total"]) for l in old_std)
                    + sum(abs(float(l["ecart_vs_standard_total"])) for l in old_ecart), 2)

    # Baseline observée sur le classeur réel — non supposée (mission §26).
    assert (len(old_std), len(old_ecart), old_sum) == (16, 8, 3894.99), (
        f"Baseline GPM legacy a changé sur le classeur réel : std={len(old_std)} "
        f"ecart={len(old_ecart)} sum={old_sum}€ — revérifier avant de comparer.")

    _reprendre_gpm(tmp_db, lignes)
    resultat = svc.construire(db_path=tmp_db)
    assert resultat["ok"], resultat

    flux = svc.lire(db_path=tmp_db)
    gpm = [f for f in flux if f["source_table"] == "menages_cout_complet"]
    new_count = len(gpm)
    new_sum = round(sum(f["montant"] for f in gpm), 2)

    print(f"GPM | OLD {old_count}/{old_sum}€ | NEW {new_count}/{new_sum}€ | "
         f"DELTA_COUNT {new_count - old_count} | DELTA_SUM {round(new_sum - old_sum, 2)}€")

    assert new_count == old_count, f"COUNT GPM : OLD={old_count} NEW={new_count}"
    assert new_sum == old_sum, f"SUM GPM : OLD={old_sum}€ NEW={new_sum}€"

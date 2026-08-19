"""Lot9 — parité RÉELLE module BNQ (mission "FERMER LES DEUX GATES" §16-18).

Reprise de PARITÉ (outil test/migration, jamais runtime — §12) : injecte dans les VRAIES tables
`banque_mouvements`/`banque_classifications` les 24 lignes VALIDE réellement présentes dans
`02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` (onglet NORM_Banque, lu en lecture seule ici,
jamais modifié), puis fait tourner le VRAI chemin `flux_unifie_service.construire()` dessus.

Baseline OLD (lue depuis le classeur réel, pas supposée) : 24 lignes type_flux_id=TYPE_FLUX_016
(Loyers/paiements) et statut_controle=VALIDE / 130,01 €.
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from app.db.connection import get_db

MASTER_BNQ = (Path(__file__).resolve().parents[2]
             / "02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx")

TYPE_FLUX_016 = "TYPE_FLUX_016"

pytestmark = pytest.mark.skipif(not MASTER_BNQ.exists(), reason="Master BNQ legacy réel absent")


def _lire_norm_banque() -> list[dict]:
    wb = openpyxl.load_workbook(str(MASTER_BNQ), read_only=True, data_only=True)
    try:
        ws = wb["NORM_Banque"]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    hdr = [str(c) for c in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:]]


def _reprendre_bnq(db_path, lignes: list[dict], run_id: str = "REPRISE_PARITE_001") -> None:
    """Reprise de parité : un mouvement + une classification par ligne legacy.

    Une seule duplication réelle existe dans le classeur (même `mouvement_id`, marquée
    DOUBLON_BANCAIRE_POTENTIEL côté legacy, hors périmètre VALIDE) : dédoublonnée ici par
    `mouvement_id` pour respecter la contrainte UNIQUE de la table cible, sans affecter les 24
    lignes VALIDE comparées (aucune n'est concernée par ce doublon)."""
    conn = get_db(db_path)
    try:
        vus = set()
        for l in lignes:
            mid_dedup = str(l["mouvement_id"])
            if mid_dedup in vus:
                continue
            vus.add(mid_dedup)
            mid = str(l["mouvement_id"])
            fp = str(l.get("ROW_HASH") or mid)
            conn.execute(
                "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
                "date_operation, date_valeur, sens, montant, devise, libelle_brut, fingerprint, "
                "ligne_source) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (mid, str(l["import_id"]), str(l["compte_id"]), str(l["date_operation"])[:10],
                 str(l["date_valeur"])[:10] if l["date_valeur"] else None, l["sens"],
                 l["montant"], l["devise"], l["libelle_brut"], fp, l.get("ligne_source")))
            conn.execute(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id, "
                "type_flux_id, code_impact, categorie, tiers_detecte, statut_controle, "
                "statut_classification) VALUES (?,?,?,?,?,?,?,?)",
                (mid, run_id, l.get("type_flux_id"), l.get("code_impact"), l.get("categorie"),
                 l.get("tiers_detecte"), l.get("statut_controle"), l.get("statut_classification")))
        conn.commit()
    finally:
        conn.close()


def test_parite_bnq_reelle(tmp_db):
    from app.services import flux_unifie_service as svc

    lignes = _lire_norm_banque()
    old_valides = [l for l in lignes
                   if l.get("type_flux_id") == TYPE_FLUX_016
                   and l.get("statut_controle") == "VALIDE"]
    old_count = len(old_valides)
    old_sum = round(sum(l["montant"] for l in old_valides), 2)

    # Baseline observée sur le classeur réel — assertion de non-régression sur la LECTURE du
    # témoin, pas une règle métier codée en dur (mission §26).
    assert (old_count, old_sum) == (24, 130.01), (
        f"Baseline BNQ legacy a changé sur le classeur réel : {old_count} lignes / {old_sum}€ "
        f"— revérifier avant de comparer.")

    _reprendre_bnq(tmp_db, lignes)
    resultat = svc.construire(db_path=tmp_db)
    assert resultat["ok"], resultat

    flux = svc.lire(db_path=tmp_db)
    bnq = [f for f in flux if f["type_flux_id"] == svc.TYPE_FLUX_BNQ]
    new_count, new_sum = len(bnq), round(sum(f["montant"] for f in bnq), 2)

    print(f"BNQ | OLD {old_count}/{old_sum}€ | NEW {new_count}/{new_sum}€ | "
         f"DELTA_COUNT {new_count - old_count} | DELTA_SUM {round(new_sum - old_sum, 2)}€")

    assert new_count == old_count, f"COUNT BNQ : OLD={old_count} NEW={new_count}"
    assert new_sum == old_sum, f"SUM BNQ : OLD={old_sum}€ NEW={new_sum}€"

    par_pk = {f["source_pk"]: f["montant"] for f in bnq}
    for l in old_valides:
        pk = str(l["mouvement_id"])
        assert pk in par_pk, f"Ligne legacy absente côté NEW : {pk}"
        assert round(par_pk[pk], 2) == round(l["montant"], 2), (
            f"Montant différent pour {pk} : legacy={l['montant']} NEW={par_pk[pk]}")

"""Lot9 — parité RÉELLE module MEN (mission "FERMER LES DEUX GATES" §11-15).

Reprise de PARITÉ (outil test/migration, jamais runtime — §12) : injecte dans les VRAIES tables
`factures`/`facture_lignes_menage`/`facture_lignes_menage_pdf`/`facture_lignes_menage_detail` les
13 lignes réellement présentes dans `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`
(onglet MASTER, lu en lecture seule ici, jamais modifié), puis fait tourner le VRAI chemin
`flux_unifie_service.construire()` dessus — pas un test qui insère directement dans `flux_unifies`.

Baseline OLD (lue depuis le classeur réel, pas supposée) : 12 lignes VALIDE / 2 381,00 € — une 13e
ligne (MENEXT-2026-05-MOUNIR-003, montant 0€) est A_CONTROLER côté legacy et exclue.

Bug de port trouvé ici et corrigé dans `flux_unifie_service._module_men` (commit associé) :
`facture_lignes_menage` n'a pas de statut par ligne (seulement au niveau facture) — sans exclusion
explicite des lignes à 0€, le NEW comptait 13 (COUNT faux, SUM correcte car 0€ n'ajoute rien).
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from app.db.connection import get_db

MASTER_MEN = (Path(__file__).resolve().parents[2]
             / "02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx")

pytestmark = pytest.mark.skipif(not MASTER_MEN.exists(), reason="Master MEN legacy réel absent")


def _lire_master_men() -> list[dict]:
    wb = openpyxl.load_workbook(str(MASTER_MEN), read_only=True, data_only=True)
    try:
        ws = wb["MASTER"]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    hdr = [str(c) for c in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:]]


def _reprendre_men(db_path, lignes: list[dict]) -> None:
    """Reprise de parité : une facture par `facture_id` legacy, une ligne par ligne legacy. Statut
    facture VALIDEE (le legacy n'avait pas de notion d'approbation humaine séparée — la reprise
    simule l'état économique final que le workflow legacy considérait déjà acquis)."""
    conn = get_db(db_path)
    try:
        factures_vues = set()
        for l in lignes:
            fid = str(l["facture_id"])
            if fid not in factures_vues:
                factures_vues.add(fid)
                conn.execute(
                    "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                    "date_facture, montant_ttc, devise, statut, source) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (fid, str(l["prestataire_id"]), fid, str(l["date_facture"])[:10],
                     l["montant_facture_total_ttc"], "EUR", "VALIDEE", "REPRISE_PARITE"))
            ligne_id = str(l["menage_externe_id"])
            conn.execute(
                "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, "
                "type_ligne, logement_id, montant_ttc, source) VALUES (?,?,?,?,?,?)",
                (ligne_id, fid, "MENAGE_EXTERNE", l["logement_id"], l["montant_ligne_ttc"],
                 "REPRISE_PARITE"))
            conn.execute(
                "INSERT INTO facture_lignes_menage_pdf (ligne_id_opaque, date_menage, "
                "precision_date_menage, nom_prestataire) VALUES (?,?,?,?)",
                (ligne_id, str(l["date_menage"])[:10] if l["date_menage"] else None,
                 l["precision_date_menage"], l["nom_prestataire"]))
            conn.execute(
                "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite) "
                "VALUES (?,?)", (ligne_id, l["nombre_menages"]))
        conn.commit()
    finally:
        conn.close()


def test_parite_men_reelle(tmp_db):
    from app.services import flux_unifie_service as svc

    lignes = _lire_master_men()
    old_valides = [l for l in lignes if l.get("statut_controle") == "VALIDE"]
    old_count, old_sum = len(old_valides), round(sum(l["montant_ligne_ttc"] for l in old_valides), 2)

    # Baseline observée sur le classeur réel — assertion de non-régression sur la LECTURE du
    # témoin, pas une règle métier codée en dur (mission §26).
    assert (old_count, old_sum) == (12, 2381.0), (
        f"Baseline MEN legacy a changé sur le classeur réel : {old_count} lignes / {old_sum}€ "
        f"— revérifier avant de comparer.")

    _reprendre_men(tmp_db, lignes)
    resultat = svc.construire(db_path=tmp_db)
    assert resultat["ok"], resultat

    flux = svc.lire(db_path=tmp_db)
    men = [f for f in flux if f["type_flux_id"] == svc.TYPE_FLUX_MEN]
    new_count, new_sum = len(men), round(sum(f["montant"] for f in men), 2)

    print(f"MEN | OLD {old_count}/{old_sum}€ | NEW {new_count}/{new_sum}€ | "
         f"DELTA_COUNT {new_count - old_count} | DELTA_SUM {round(new_sum - old_sum, 2)}€")

    assert new_count == old_count, f"COUNT MEN : OLD={old_count} NEW={new_count}"
    assert new_sum == old_sum, f"SUM MEN : OLD={old_sum}€ NEW={new_sum}€"

    # Ligne à ligne : chaque menage_externe_id legacy retrouvé comme source_pk NEW, même montant.
    par_pk = {f["source_pk"]: f["montant"] for f in men}
    for l in old_valides:
        pk = str(l["menage_externe_id"])
        assert pk in par_pk, f"Ligne legacy absente côté NEW : {pk}"
        assert round(par_pk[pk], 2) == round(l["montant_ligne_ttc"], 2), (
            f"Montant différent pour {pk} : legacy={l['montant_ligne_ttc']} NEW={par_pk[pk]}")

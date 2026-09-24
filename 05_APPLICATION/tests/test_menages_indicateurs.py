"""ATTENDUS · RÉALISÉS / JUSTIFIÉS · À CONTRÔLER — même unité, même périmètre.

Reproduit la situation constatée en septembre 2026 (53 attendus, 9 lignes à contrôler) sur une
base temporaire, et vérifie que chaque nombre dit ce qu'il compte.
"""
from __future__ import annotations

import sqlite3

from app.services import menages_service as svc


def _exec(db, sql, lignes):
    conn = sqlite3.connect(db)
    conn.executemany(sql, lignes)
    conn.commit()
    conn.close()


def test_indicateurs_unites_explicites(tmp_db, monkeypatch):
    taches = ([("T%d" % i, "2026-09", "LOG_1", "completed") for i in range(34)]
              + [("P%d" % i, "2026-09", "LOG_1", "confirmed") for i in range(18)]
              + [("A%d" % i, "2026-09", "LOG_1", "cancelled") for i in range(25)])
    _exec(tmp_db, "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status) "
                  "VALUES (?,?,?,?)", taches)
    _exec(tmp_db, "INSERT INTO reservations_hors_hostaway (reservation_hh_id, mois, logement_id, "
                  "statut) VALUES (?,?,?,?)", [("HH1", "2026-09", "LOG_2", "ACTIVE")])

    # Neuf lignes de rapprochement (logement × intervenant), toutes sans déclaration.
    ecarts = [7, 6, 5, 5, 5, 2, 2, 1, 1]
    vues = [{"statut_effectif": "A_CONTROLER", "identification_incomplete": False,
             "ecart": -e, "interne_declare": 0, "externe_facture": 0} for e in ecarts]
    monkeypatch.setattr(svc, "_toutes_les_vues", lambda mois="": vues)
    monkeypatch.setattr(svc, "load_summary",
                        lambda mois="": {"interne_declare": 0, "externe_facture": 0})

    k = svc.indicateurs_perimetre("2026-09", db_path=tmp_db)
    assert k["attendus"] == 53 and k["attendus_hostaway"] == 52
    assert k["attendus_hors_hostaway"] == 1
    assert (k["realises"], k["a_venir"], k["annules"]) == (34, 18, 25)
    assert k["justifies"] == 0
    # Les 9 ne sont pas des ménages : ce sont 9 lignes, qui portent 34 ménages d'écart.
    assert k["a_controler_lignes"] == 9 and k["a_controler_menages"] == 34
    assert k["a_controler_sans_declaration"] == 9

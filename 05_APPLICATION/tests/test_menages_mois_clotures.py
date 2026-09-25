"""Mois clôturés : signalement juste, réouverture tracée, jamais de reclôture automatique.

Tout se passe sur une base temporaire (`tmp_db`) : aucun vrai mois n'est rouvert.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import menages_actualisation_service as chaine
from app.services import menages_mois_clotures_service as svc


def _sql(db, sql, args=()):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(sql, args).fetchall()
        conn.commit()
        return [dict(x) for x in r]
    finally:
        conn.close()


def _mois(db, mois, statut):
    _sql(db, "INSERT OR REPLACE INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
             "VALUES (?,?,?)", (mois, statut, "TEST"))


def _extraction(db, taches):
    eid = raw.ouvrir(mode=raw.MODE_FIXTURE, db_path=db)
    raw.enregistrer(eid, taches=taches, db_path=db)
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=db)
    return eid


def _t(i, mois="2026-03", statut="completed"):
    return {"task_id": str(i), "reservation_id": str(900 + i), "listing_map_id": "480136",
            "title": "t", "status": statut, "can_start_from": f"{mois}-10 10:00:00",
            "assignee_user_id": None, "extrait_le": "x", "row_hash": f"h{i}"}


# ── empreinte : une nouvelle extraction identique ne « modifie » aucun mois ─────────────────────

def test_extraction_identique_ne_modifie_aucun_mois(tmp_db):
    _extraction(tmp_db, [_t(1), _t(2), _t(3, "2026-09")])
    avant = chaine.empreintes(chaine.ORIGINE_HOSTAWAY, db_path=tmp_db)
    _extraction(tmp_db, [_t(1), _t(2), _t(3, "2026-09")])
    apres = chaine.empreintes(chaine.ORIGINE_HOSTAWAY, db_path=tmp_db)
    assert chaine._mois_modifies(avant, apres) == []


def test_changement_detat_et_quantite_mesures(tmp_db):
    _extraction(tmp_db, [_t(1), _t(2, statut="confirmed"), _t(3, "2026-09")])
    avant_e = chaine.empreintes(chaine.ORIGINE_HOSTAWAY, db_path=tmp_db)
    avant_t = chaine.taches_actives_par_mois(db_path=tmp_db)
    _extraction(tmp_db, [_t(1), _t(2), _t(4), _t(3, "2026-09")])
    apres_e = chaine.empreintes(chaine.ORIGINE_HOSTAWAY, db_path=tmp_db)
    apres_t = chaine.taches_actives_par_mois(db_path=tmp_db)
    assert chaine._mois_modifies(avant_e, apres_e) == ["2026-03"]
    assert chaine.nb_taches_modifiees(avant_t, apres_t, "2026-03") == 2   # 2 changé, 4 nouveau


# ── détail ──────────────────────────────────────────────────────────────────────────────────────

def test_detail_par_mois_type_date_quantite(tmp_db):
    _mois(tmp_db, "2026-03", "CLOTURE")
    chaine.signaler_mois_cloture("2026-03", "HOSTAWAY", "x", quantite=4, db_path=tmp_db)
    chaine.signaler_mois_cloture("2026-03", "PDF", "x", db_path=tmp_db)
    d = svc.detail(db_path=tmp_db)
    assert [m["mois"] for m in d] == ["2026-03"]
    origines = {o["origine"]: o for o in d[0]["origines"]}
    assert origines["HOSTAWAY"]["libelle"] == "Tâches de ménage Hostaway"
    assert origines["HOSTAWAY"]["quantite"] == 4
    assert origines["PDF"]["quantite"] is None and origines["PDF"]["derniere_detection"]
    assert d[0]["reouvrable"]


# ── réouverture ─────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("kwargs, message", [
    ({"motif": "x", "acteur": "Ewan", "confirmation": False}, "Confirmez"),
    ({"motif": " ", "acteur": "Ewan", "confirmation": True}, "motif"),
    ({"motif": "x", "acteur": "", "confirmation": True}, "auteur"),
])
def test_reouverture_exige_confirmation_motif_acteur(tmp_db, kwargs, message):
    _mois(tmp_db, "2026-03", "CLOTURE")
    with pytest.raises(svc.DecisionRefusee, match=message):
        svc.rouvrir("2026-03", db_path=tmp_db, recalculer=lambda *a, **k: {"ok": True}, **kwargs)
    assert svc.statut_mois("2026-03", db_path=tmp_db) == "CLOTURE"


def test_mois_ouvert_non_reouvrable(tmp_db):
    _mois(tmp_db, "2026-07", "OUVERT")
    with pytest.raises(svc.DecisionRefusee):
        svc.rouvrir("2026-07", motif="x", acteur="Ewan", confirmation=True, db_path=tmp_db,
                    recalculer=lambda *a, **k: {"ok": True})


def test_reouverture_trace_recalcule_et_ne_reclot_jamais(tmp_db):
    _mois(tmp_db, "2026-03", "CLOTURE")
    chaine.signaler_mois_cloture("2026-03", "HOSTAWAY", "x", quantite=2, db_path=tmp_db)
    recalculs = []
    r = svc.rouvrir("2026-03", motif="Factures de mars reçues en retard", acteur="Ewan",
                    confirmation=True, db_path=tmp_db,
                    recalculer=lambda mois, **k: recalculs.append(mois) or {"ok": True})
    assert r["ok"] and r["recalcul_ok"] and recalculs == ["2026-03"]
    # À contrôler / à reclore — jamais reclôturé.
    assert svc.statut_mois("2026-03", db_path=tmp_db) == "EN_CONTROLE"
    assert svc.mois_a_reclore(db_path=tmp_db) == ["2026-03"]
    journal = svc.reouvertures(db_path=tmp_db)[0]
    assert journal["motif"] == "Factures de mars reçues en retard"
    assert journal["acteur"] == "Ewan" and journal["reouvert_le"]
    assert journal["statut_avant"] == "CLOTURE" and journal["recalcul_statut"] == "SUCCES"
    sig = _sql(tmp_db, "SELECT * FROM menages_changements_mois_clotures")[0]
    assert sig["statut"] == "TRAITE" and sig["decision"] == "REOUVERT"
    assert sig["decision_par"] == "Ewan"
    audit = _sql(tmp_db, "SELECT * FROM audit_events WHERE action='MOIS_REOUVERT'")
    assert len(audit) == 1 and "2026-03" in audit[0]["details"]
    # Une seconde tentative est refusée : le mois n'est plus clôturé.
    with pytest.raises(svc.DecisionRefusee):
        svc.rouvrir("2026-03", motif="x", acteur="Ewan", confirmation=True, db_path=tmp_db,
                    recalculer=lambda *a, **k: {"ok": True})


def test_recalcul_en_echec_trace_mois_reste_rouvert(tmp_db):
    _mois(tmp_db, "2026-04", "CLOTURE")
    r = svc.rouvrir("2026-04", motif="m", acteur="Ewan", confirmation=True, db_path=tmp_db,
                    recalculer=lambda *a, **k: {"ok": False, "message": "lot6d KO"})
    assert not r["recalcul_ok"]
    assert svc.statut_mois("2026-04", db_path=tmp_db) == "EN_CONTROLE"
    assert svc.reouvertures(db_path=tmp_db)[0]["recalcul_statut"] == "ECHEC"


def test_mois_clos_protege_du_recalcul_automatique(tmp_db):
    """Le moteur refuse de recalculer un mois CLOTURE : seul `rouvrir` le rend recalculable."""
    from app.services import orchestrateur_moteur as moteur

    _mois(tmp_db, "2026-02", "CLOTURE")
    r = moteur.executer_menages_cible(mois="2026-02", db_path=tmp_db)
    assert not r["ok"] and r["code"] == "MENAGES_MOIS_CLOTURE"


def test_classer_sans_rouvrir(tmp_db):
    _mois(tmp_db, "2026-05", "CLOTURE")
    chaine.signaler_mois_cloture("2026-05", "HOSTAWAY", "x", db_path=tmp_db)
    with pytest.raises(svc.DecisionRefusee):
        svc.classer("2026-05", motif="", acteur="Ewan", db_path=tmp_db)
    svc.classer("2026-05", motif="Aucune différence réelle", acteur="Ewan", db_path=tmp_db)
    assert svc.detail(db_path=tmp_db) == []
    assert svc.statut_mois("2026-05", db_path=tmp_db) == "CLOTURE"


def test_routes_mois_clotures(client, tmp_db):
    _mois(tmp_db, "2026-03", "CLOTURE")
    chaine.signaler_mois_cloture("2026-03", "HOSTAWAY", "x", quantite=3, db_path=tmp_db)
    page = client.get("/menages/mois-clotures").text
    assert "2026-03" in page and "Rouvrir le mois" in page and "3 tâches" in page
    r = client.post("/menages/mois-clotures/rouvrir", data={"mois": "2026-03", "motif": "m",
                                                           "acteur": "Ewan"},
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur=" in r.headers["location"]
    assert svc.statut_mois("2026-03", db_path=tmp_db) == "CLOTURE"

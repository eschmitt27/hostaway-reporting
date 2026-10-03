"""Brouillons de factures propriétaires : séjours éditables, rechargement, suppression des
annulées, émission hors compta (demande utilisateur du 2026-10-03). Base temporaire, fixtures."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import creances_dettes_service as creances
from app.services import factures_proprietaires_brouillon_service as br
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie Fixture", "adresse": "1 rue de Test", "siret": "00000000000000"}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test"}
MOIS, PROP, LOG = "2026-09", "PROP_T", "LOG_T"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO lot10_runs (run_id, actif) VALUES ('L10-T', 1)")
    for rid, arrivee, payout, menage, commission in (("101", "2026-09-03", 100.12, 29.0, 12.80),
                                                      ("102", "2026-09-25", 117.00, 29.0, 15.84)):
        conn.execute(
            "INSERT INTO lot10_commissions (run_id, reservation_calc_id, reservation_id_hostaway, "
            "logement_id, proprietaire_id, mois, date_arrivee, date_depart, nuits, channel_type, "
            "payout_calcule, menage_retenu, assiette_commission, taux_commission, "
            "commission_conciergerie) VALUES ('L10-T',?,?,?,?,?,?,?,2,'DIRECT',?,?,?,0.18,?)",
            ("RES-HA-" + rid, rid, LOG, PROP, MOIS, arrivee, arrivee, payout, menage,
             round(payout - menage, 2), commission))
    conn.commit()
    conn.close()
    return chemin


def _source():
    return {"mois": MOIS, "proprietaire_id": PROP, "logement_id": LOG, "source_calcul": "PREF-T",
            "COMMISSION_CONCIERGERIE": 28.64, "MENAGE_FACTURE": 58.0, "CHARGE_FIXE": 35.0,
            "montant_du_conciergerie": 121.64}


def _lignes(db, fid):
    return {l["type_ligne"]: l["montant"] for l in svc.lire(fid, db_path=db)["lignes"]}


# ── Séjours éditables ──────────────────────────────────────────────────────────────────────────

def test_les_sejours_sont_figes_avec_menage_et_commission(db):
    f = svc.creer(_source(), db_path=db)
    sejours = svc.reservations(f["facture_id_opaque"], db_path=db)
    assert [(s["reservation_id"], s["payout"], s["assiette_commission"], s["menage_retenu"],
             s["commission"]) for s in sejours] == [("101", 100.12, 71.12, 29.0, 12.8),
                                                    ("102", 117.0, 88.0, 29.0, 15.84)]
    assert not any(s["menage_modifie"] or s["commission_modifiee"] for s in sejours)


def test_menage_offert_retire_le_menage_de_la_facture(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    br.modifier_sejour(fid, "102", menage="0", acteur="t", db_path=db)
    assert _lignes(db, fid)["MENAGE_FACTURE"] == 29.0
    assert svc.lire(fid, db_path=db)["montant_total"] == pytest.approx(92.64)
    s = {x["reservation_id"]: x for x in svc.reservations(fid, db_path=db)}["102"]
    assert s["menage_retenu"] == 0 and s["menage_modifie"] and s["menage_initial"] == 29.0
    ligne = next(l for l in svc.lire(fid, db_path=db)["lignes"] if l["type_ligne"] == "MENAGE_FACTURE")
    assert ligne["montant_modifie"] and ligne["montant_source_initial"] == 58.0


def test_tout_le_menage_offert_supprime_la_ligne_puis_la_recree(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    br.modifier_sejour(fid, "101", menage="0", db_path=db)
    br.modifier_sejour(fid, "102", menage="0", db_path=db)
    assert "MENAGE_FACTURE" not in _lignes(db, fid)
    br.modifier_sejour(fid, "101", menage="20", db_path=db)
    assert _lignes(db, fid)["MENAGE_FACTURE"] == 20.0


def test_commission_modifiee_ajuste_la_ligne_commission(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    br.modifier_sejour(fid, "101", commission="10,00", db_path=db)
    assert _lignes(db, fid)["COMMISSION_CONCIERGERIE"] == pytest.approx(25.84)
    assert svc.lire(fid, db_path=db)["montant_total"] == pytest.approx(118.84)


@pytest.mark.parametrize("valeur", ["-5", "abc"])
def test_montant_de_sejour_invalide_refuse(db, valeur):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    with pytest.raises(svc.FactureProprietaireError):
        br.modifier_sejour(fid, "101", menage=valeur, db_path=db)


def test_sejour_non_editable_hors_brouillon(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    with pytest.raises(svc.FactureProprietaireError):
        br.modifier_sejour(fid, "101", menage="0", db_path=db)


def test_suppression_de_ligne_toujours_possible(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    ligne = next(l for l in svc.lire(fid, db_path=db)["lignes"] if l["type_ligne"] == "CHARGE_FIXE")
    svc.supprimer_ligne(fid, ligne["ligne_id_opaque"], db_path=db)
    assert "CHARGE_FIXE" not in _lignes(db, fid)


# ── Recharger ──────────────────────────────────────────────────────────────────────────────────

def test_recharger_reprend_le_calcul_et_garde_les_lignes_manuelles(db, monkeypatch):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    br.modifier_sejour(fid, "102", menage="0", db_path=db)
    svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="Ajout manuel", montant=12,
                      db_path=db)
    # Le calcul a changé depuis la création (une troisième réservation est apparue).
    conn = get_db(db)
    conn.execute(
        "INSERT INTO lot10_commissions (run_id, reservation_calc_id, reservation_id_hostaway, "
        "logement_id, proprietaire_id, mois, date_arrivee, payout_calcule, menage_retenu, "
        "assiette_commission, taux_commission, commission_conciergerie) "
        "VALUES ('L10-T','RES-HA-103','103',?,?,?, '2026-09-28', 301.95, 29, 272.95, 0.18, 49.13)",
        (LOG, PROP, MOIS))
    conn.commit()
    conn.close()
    nouvelles = [{"type_ligne": "COMMISSION_CONCIERGERIE", "libelle": "Commission",
                  "montant": 77.77, "objet_source_type": "LOT10_NET_PROPRIETAIRE"},
                 {"type_ligne": "MENAGE_FACTURE", "libelle": "Ménage", "montant": 87.0,
                  "objet_source_type": "LOT10_NET_PROPRIETAIRE"}]
    monkeypatch.setattr(br, "_lignes_calculees_actuelles", lambda f, db_path=None: nouvelles)

    res = br.recharger(fid, acteur="t", db_path=db)

    f = svc.lire(fid, db_path=db)
    assert [(l["type_ligne"], l["montant"]) for l in f["lignes"]] == [
        ("COMMISSION_CONCIERGERIE", 77.77), ("MENAGE_FACTURE", 87.0), ("CHARGE_FIXE", 12.0)]
    assert [l["numero_ligne"] for l in f["lignes"]] == [1, 2, 3]
    assert f["total_source_calcule"] == pytest.approx(164.77)
    assert f["montant_total"] == pytest.approx(176.77)
    sejours = svc.reservations(fid, db_path=db)
    assert [s["reservation_id"] for s in sejours] == ["101", "102", "103"]
    assert all(not s["menage_modifie"] for s in sejours), "le rechargement remet le calcul"
    assert res["sejours"] == 3 and res["lignes_conservees"] == 1
    assert any(e["type_evenement"] == br.EVT_RECHARGEMENT for e in f["evenements"])


def test_recharger_refuse_hors_brouillon(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    with pytest.raises(svc.FactureProprietaireError):
        br.recharger(fid, db_path=db)


# ── Supprimer les annulées ─────────────────────────────────────────────────────────────────────

def test_supprimer_une_annulee_jamais_emise(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    with pytest.raises(svc.FactureProprietaireError):
        br.supprimer_annulee(fid, db_path=db)        # brouillon : refus
    svc.annuler(fid, motif="erreur", db_path=db)
    assert br.supprimer_annulees(db_path=db) == [fid]
    assert svc.lister(db_path=db) == []
    conn = get_db(db)
    try:
        for table in br.TABLES_PROPRES:
            assert conn.execute(f"SELECT COUNT(*) FROM {table} WHERE facture_id_opaque=?",
                                (fid,)).fetchone()[0] == 0, table
    finally:
        conn.close()


def test_une_facture_annulee_numerotee_ne_se_supprime_jamais(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    svc.annuler(fid, motif="erreur", db_path=db)
    conn = get_db(db)
    conn.execute("UPDATE factures_proprietaires SET numero_facture='2026-09-001' "
                 "WHERE facture_id_opaque=?", (fid,))
    conn.commit()
    conn.close()
    assert br.supprimer_annulees(db_path=db) == []
    with pytest.raises(svc.FactureProprietaireError):
        br.supprimer_annulee(fid, db_path=db)


# ── Hors compta ────────────────────────────────────────────────────────────────────────────────

def _emise(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    svc.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, serie="T-2026",
                date_facture="2026-10-03", db_path=db)
    return fid


def test_facture_hors_compta_conservee_sans_creance(db):
    fid = _emise(db)
    assert [c["facture_id_opaque"] for c in creances.creances(db_path=db)] == [fid]
    f = svc.marquer_hors_compta(fid, motif="geste commercial", acteur="t", db_path=db)
    assert f["statut"] == svc.ST_EMIS and f["hors_compta"] == 1
    assert f["motif_hors_compta"] == "geste commercial" and f["numero_facture"]
    assert creances.creances(db_path=db) == []
    s = svc.solde(fid, db_path=db)
    assert s["solde"] == 0 and s["statut_reglement"] == svc.ST_REGLEMENT_HORS_COMPTA


def test_hors_compta_refuse_sur_une_facture_non_emise(db):
    fid = svc.creer(_source(), db_path=db)["facture_id_opaque"]
    with pytest.raises(svc.FactureProprietaireError):
        svc.marquer_hors_compta(fid, db_path=db)


def test_hors_compta_motif_par_defaut(db):
    f = svc.marquer_hors_compta(_emise(db), db_path=db)
    assert f["motif_hors_compta"] == svc.MOTIF_HORS_COMPTA_DEFAUT

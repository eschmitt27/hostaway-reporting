"""Cycle de vie opérationnel du ménage unitaire — création, affectation, réalisation, transitions,
intégrations facture/charge, contrôles de base.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import fournisseurs_referentiel_service as frs_svc
from app.services import menages_cycle_service as svc

LOGEMENT = "LOG_A1"
PROPRIETAIRE = "PROP_A"


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED", True)

    def _fake_load_detail(logement_id):
        if logement_id == LOGEMENT:
            return {"logement_id": LOGEMENT, "proprietaire_id": PROPRIETAIRE, "status": "OK"}
        if logement_id == "LOG_SANS_PROP":
            return {"logement_id": "LOG_SANS_PROP", "proprietaire_id": "", "status": "OK"}
        return None

    from app.services import logements_service
    monkeypatch.setattr(logements_service, "load_detail", _fake_load_detail)
    return p


@pytest.fixture
def fournisseur_externe(db):
    r = frs_svc.creer("Prestataire Externe Test", "MENAGE", acteur="recette", db_path=db)
    opaque = r["fournisseur_id_opaque"]
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, type_menage, "
            "date_debut_validite, tarif_horaire, logements_autorises) VALUES (?,?,?,?,?)",
            (opaque, "EXTERNE", "2026-01-01", None, None))
        conn.commit()
    finally:
        conn.close()
    return opaque


@pytest.fixture
def fournisseur_interne(db):
    r = frs_svc.creer("Intervenante Interne Test", "MENAGE", acteur="recette", db_path=db)
    opaque = r["fournisseur_id_opaque"]
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, type_menage, "
            "date_debut_validite, tarif_horaire) VALUES (?,?,?,?)",
            (opaque, "INTERNE", "2026-01-01", 10.0))
        conn.commit()
    finally:
        conn.close()
    return opaque


FORM = {"logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
        "date_prevue": "2026-06-10", "duree_prevue_h": 2.0, "cout_prevu": 45.0}


# ── Double verrou d'écriture ──────────────────────────────────────────────────

def test_creer_refuse_sans_les_flags(db, monkeypatch):
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_ENABLED", False)
    res = svc.creer(dict(FORM), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_FLAGS


def test_creer_refuse_avec_le_flag_seul_sans_confirmation(db, monkeypatch):
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED", False)
    res = svc.creer(dict(FORM), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_FLAGS


# ── Création hors Hostaway ───────────────────────────────────────────────────

def test_creer_menage_valide(db):
    res = svc.creer(dict(FORM), acteur="recette", db_path=db)
    assert res["ok"], res
    m = svc.charger(res["menage_id_opaque"], db)
    assert m["statut"] == svc.ST_PREVU
    assert m["proprietaire_id"] == PROPRIETAIRE
    assert m["logement_id"] == LOGEMENT


def test_logement_inconnu_refuse(db):
    res = svc.creer(dict(FORM, logement_id="LOG_INEXISTANT"), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_LOGEMENT_INCONNU


def test_proprietaire_non_resolu_refuse(db):
    res = svc.creer(dict(FORM, logement_id="LOG_SANS_PROP"), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_PROPRIETAIRE_NON_RESOLU


def test_type_invalide_refuse(db):
    res = svc.creer(dict(FORM, type_menage="AUTRE"), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_TYPE_INVALIDE


def test_date_invalide_refuse(db):
    res = svc.creer(dict(FORM, date_prevue="pas-une-date"), db_path=db)
    assert not res["ok"] and res["code"] == svc.E_DATE_INVALIDE


def test_doublon_meme_empreinte_refuse(db):
    svc.creer(dict(FORM), db_path=db)
    res2 = svc.creer(dict(FORM), db_path=db)
    assert not res2["ok"] and res2["code"] == svc.E_DOUBLON


def test_doublon_impossible_meme_en_sql_direct(db):
    """Index unique en défense en profondeur : même comportement que factures/charges (0017)."""
    import sqlite3
    svc.creer(dict(FORM), db_path=db)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO menages (menage_id_opaque, mois, logement_id, proprietaire_id, "
                "type_menage, empreinte) VALUES ('MEN-DOUBLON','2026-06','LOG_A1','PROP_A',"
                "'EXTERNE', ?)", (svc.empreinte(LOGEMENT, "2026-06", "EXTERNE", "2026-06-10"),))
            conn.commit()
    finally:
        conn.close()


# ── Affectation, changement, remplacement ────────────────────────────────────

def test_affecter_prestataire_qualifie(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    res = svc.affecter(m, fournisseur_externe, db_path=db)
    assert res["ok"] and res["statut"] == svc.ST_A_REALISER
    assert svc.charger(m, db)["fournisseur_id_opaque"] == fournisseur_externe


def test_affecter_prestataire_type_incompatible_refuse(db, fournisseur_interne):
    """Un prestataire qualifié INTERNE ne peut pas être affecté à un ménage EXTERNE."""
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    res = svc.affecter(m, fournisseur_interne, db_path=db)
    assert not res["ok"] and res["code"] == svc.E_PRESTATAIRE_INCOMPATIBLE


def test_affecter_prestataire_archive_refuse(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    frs_svc.desactiver(frs_svc.charger_par_opaque(fournisseur_externe, db), db_path=db)
    res = svc.affecter(m, fournisseur_externe, db_path=db)
    assert not res["ok"] and res["code"] == svc.E_PRESTATAIRE_INCOMPATIBLE


def test_changement_prestataire_conserve_l_historique(db, fournisseur_externe):
    """L'ancien prestataire n'est jamais réécrit silencieusement : il reste dans l'événement."""
    r = frs_svc.creer("Autre Prestataire", "MENAGE", db_path=db)
    autre = r["fournisseur_id_opaque"]
    conn = get_db(db)
    conn.execute("INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, "
                "type_menage, date_debut_validite) VALUES (?,?,?)", (autre, "EXTERNE", "2026-01-01"))
    conn.commit(); conn.close()

    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    svc.changer_statut(m, svc.ST_A_AFFECTER, db_path=db)   # retour pour ré-affecter (test isolé)
    conn = get_db(db)
    conn.execute("UPDATE menages SET statut=? WHERE menage_id_opaque=?", (svc.ST_A_AFFECTER, m))
    conn.commit(); conn.close()
    svc.affecter(m, autre, db_path=db)

    hist = svc.historique(m, db_path=db)
    changement = next(h for h in hist if h["type_evenement"] == "CHANGEMENT_PRESTATAIRE")
    assert changement["ancien_prestataire"] == fournisseur_externe
    assert changement["nouveau_prestataire"] == autre


def test_remplacer_cree_un_nouveau_menage_et_termine_l_ancien(db, fournisseur_externe):
    r = frs_svc.creer("Prestataire Remplacant", "MENAGE", db_path=db)
    remplacant = r["fournisseur_id_opaque"]
    conn = get_db(db)
    conn.execute("INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, "
                "type_menage, date_debut_validite) VALUES (?,?,?)",
                (remplacant, "EXTERNE", "2026-01-01"))
    conn.commit(); conn.close()

    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    res = svc.remplacer(m, remplacant, db_path=db)
    assert res["ok"]
    assert svc.charger(m, db)["statut"] == svc.ST_REMPLACE
    nouveau = svc.charger(res["nouveau_menage_id_opaque"], db)
    assert nouveau["fournisseur_id_opaque"] == remplacant
    assert nouveau["statut"] == svc.ST_A_REALISER


# ── Réalisation et transitions ───────────────────────────────────────────────

def test_realiser_avec_ecart_faible_ok(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    res = svc.realiser(m, cout_reel=46.0, methode_cout="EXTERNE_FACTURE", db_path=db)
    assert res["ok"] and res["statut"] == svc.ST_REALISE


def test_realiser_avec_ecart_important_sans_justification_refuse(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    res = svc.realiser(m, cout_reel=200.0, db_path=db)
    assert not res["ok"] and res["code"] == "V09_ECART_NON_JUSTIFIE"


def test_realiser_avec_ecart_important_justifie_ok(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    res = svc.realiser(m, cout_reel=200.0, ecart_justification="Ménage exceptionnel après dégât.",
                       db_path=db)
    assert res["ok"]


def test_transition_interdite_refusee(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    res = svc.changer_statut(m, svc.ST_REGLE, db_path=db)
    assert not res["ok"] and res["code"] == svc.E_STATUT


def test_valider_sans_cout_refuse(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    svc.realiser(m, cout_reel=None, methode_cout="EXTERNE_FACTURE", db_path=db)
    conn = get_db(db)
    conn.execute("UPDATE menages SET statut=? WHERE menage_id_opaque=?", (svc.ST_A_CONTROLER, m))
    conn.commit(); conn.close()
    res = svc.changer_statut(m, svc.ST_VALIDE, db_path=db)
    assert not res["ok"] and res["code"] == "V10_VALIDATION_SANS_COUT"


def test_annulation_jamais_une_suppression(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.changer_statut(m, svc.ST_ANNULE, db_path=db)
    assert svc.charger(m, db) is not None            # jamais supprimé
    assert svc.charger(m, db)["statut"] == svc.ST_ANNULE


def test_litige_puis_reouverture_controlee(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    svc.changer_statut(m, svc.ST_LITIGE, db_path=db)
    res = svc.changer_statut(m, svc.ST_A_CONTROLER, db_path=db)
    assert res["ok"]
    # jamais un saut direct de LITIGE vers FACTURE/REGLE
    assert svc.ST_FACTURE not in svc.TRANSITIONS[svc.ST_LITIGE]
    assert svc.ST_REGLE not in svc.TRANSITIONS[svc.ST_LITIGE]


def test_toutes_les_transitions_du_brief_sont_couvertes():
    assert svc.ST_A_AFFECTER in svc.TRANSITIONS[svc.ST_PREVU]
    assert svc.ST_A_REALISER in svc.TRANSITIONS[svc.ST_A_AFFECTER]
    assert svc.ST_A_CONTROLER in svc.TRANSITIONS[svc.ST_REALISE]
    assert svc.ST_VALIDE in svc.TRANSITIONS[svc.ST_A_CONTROLER]
    assert svc.ST_FACTURE in svc.TRANSITIONS[svc.ST_VALIDE]
    assert svc.ST_REGLE in svc.TRANSITIONS[svc.ST_FACTURE]
    assert svc.ST_ANNULE in svc.TRANSITIONS[svc.ST_PREVU]
    assert svc.ST_REMPLACE in svc.TRANSITIONS[svc.ST_A_AFFECTER]
    # "Tout état compatible → litige" (brief §3) : compatible = un prestataire ou un coût existe
    # déjà à ce stade. PREVU (rien affecté) et les états terminaux en sont exclus par construction.
    etats_compatibles_litige = {svc.ST_A_AFFECTER, svc.ST_A_REALISER, svc.ST_REALISE,
                                svc.ST_A_CONTROLER, svc.ST_VALIDE, svc.ST_FACTURE, svc.ST_REGLE}
    for etat in etats_compatibles_litige:
        assert svc.ST_LITIGE in svc.TRANSITIONS[etat], f"{etat} devrait pouvoir passer en litige"


# ── Fournisseur archivé ───────────────────────────────────────────────────────

def test_fournisseur_archive_reste_visible_dans_les_menages_passes(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    frs_svc.desactiver(frs_svc.charger_par_opaque(fournisseur_externe, db), db_path=db)
    menage = svc.charger(m, db)
    assert menage["fournisseur_id_opaque"] == fournisseur_externe   # toujours visible, non effacé


# ── Intégrations facture / charge ────────────────────────────────────────────

def test_lier_facture_puis_double_liaison_refusee(db):
    m1 = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    m2 = svc.creer(dict(FORM, date_prevue="2026-06-11"), db_path=db)["menage_id_opaque"]
    r1 = svc.lier_facture(m1, "FAC-TEST01", db_path=db)
    assert r1["ok"]
    r2 = svc.lier_facture(m2, "FAC-TEST01", db_path=db)
    assert not r2["ok"] and r2["code"] == "E_FACTURE_DEJA_LIEE"


def test_lier_charge_puis_double_liaison_refusee_par_le_schema(db):
    import sqlite3
    m1 = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    m2 = svc.creer(dict(FORM, date_prevue="2026-06-12"), db_path=db)["menage_id_opaque"]
    svc.lier_charge(m1, "CHG_TEST01", db_path=db)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE menages SET charge_id=? WHERE menage_id_opaque=?",
                        ("CHG_TEST01", m2))
            conn.commit()
    finally:
        conn.close()


def test_contexte_sans_facture_renvoie_liste_vide(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    ctx = svc.contexte_facture_charge_reglement_banque(m, db_path=db)
    assert ctx["facture"] is None
    assert ctx["reglements"] == []


# ── Persistance / lister / historique ────────────────────────────────────────

def test_lister_filtre_par_mois_logement_type_statut(db, fournisseur_externe):
    svc.creer(dict(FORM), db_path=db)
    svc.creer(dict(FORM, logement_id="LOG_A1", date_prevue="2026-06-15", type_menage="INTERNE"),
             db_path=db)
    externes = svc.lister(mois="2026-06", type_menage="EXTERNE", db_path=db)
    assert all(m["type_menage"] == "EXTERNE" for m in externes)
    assert len(externes) == 1


def test_historique_ordonne_du_plus_recent(db, fournisseur_externe):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, fournisseur_externe, db_path=db)
    hist = svc.historique(m, db_path=db)
    assert hist[0]["type_evenement"] in ("AFFECTATION", "CHANGEMENT_PRESTATAIRE")
    assert hist[-1]["type_evenement"] == "CREATION"

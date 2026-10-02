"""Finition du cutover V1 — plus aucune comptabilité antérieure au 01/09/2026 (D-V1-FIN-1/2).

Base ISOLÉE : l'échantillon du cutover (`tests.test_cutover_v1.base`, cutover appliqué par
`v1_base`), enrichi de calculs anciens dans PLUSIEURS runs (un actif, un inactif), de préfactures,
d'un contrôle Lot11 et de sa décision humaine, d'une facture fournisseur antérieure avec toutes ses
tables propres, et d'une charge rapprochée par deux paiements. Jamais la vraie base. Les PDF réels
du dossier source ne servent qu'en LECTURE, copiés dans un dossier temporaire.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import cutover_v1_finition_service as fin
from app.services import factures_service as fact
from app.services import perimetre_v1_service as v1
from tests.test_cutover_v1 import _ids, _ins, _logique, base, v1_base  # noqa: F401

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
import lib_db_moteur as dbm  # noqa: E402

MESSAGE = "Aucune comptabilité disponible pour cette période."
DEBUT_V1 = "La comptabilité V1 débute en septembre 2026."
RESA_AOUT, RESA_SEPT = "65060946", "67000001"
PDF_AOUT = cfg.MENAGES_PDF_DIR / "08-26-Aissata.pdf"
PDF_MARS = cfg.MENAGES_PDF_DIR / "03-26-Mounir.pdf"
pdf_reels = pytest.mark.skipif(not (PDF_AOUT.exists() and PDF_MARS.exists()),
                               reason="PDF réels absents d'un checkout propre")


@pytest.fixture()
def ecriture_active(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


def _anciens_calculs(c) -> None:
    """Deux runs Lot10 et deux runs Lot12 (un inactif, un actif), chacun avec août ET septembre."""
    for run10, run12, actif, jour in (("L10-ANCIEN", "L12-ANCIEN", 0, "01"),
                                      ("L10-ACTIF", "L12-ACTIF", 1, "02")):
        _ins(c, "lot10_runs", run_id=run10, actif=actif, statut="SUCCES",
             date_calcul=f"2026-10-{jour}T10:00:00Z")
        for mois, resa, canape in (("2026-08", RESA_AOUT, "A_CONTROLER"),
                                   ("2026-09", RESA_SEPT, "OK")):
            calc = f"RC-{run10}-{mois}"
            _ins(c, "lot10_commissions", run_id=run10, reservation_calc_id=calc, mois=mois,
                 reservation_id_hostaway=resa, logement_id="LOG_T1", proprietaire_id="PROP_T1",
                 commission_conciergerie=30, controle_preparation_canape=canape)
            _ins(c, "lot10_net_exploitation", run_id=run10, reservation_calc_id=calc, mois=mois,
                 logement_id="LOG_T1", proprietaire_id="PROP_T1")
            _ins(c, "lot10_net_reglement", run_id=run10, mois=mois, logement_id="LOG_T1",
                 proprietaire_id="PROP_T1", total_payout_mois=900, total_commission_mois=30,
                 montant_du_conciergerie=465.88, reste_a_payer_conciergerie=465.88,
                 net_proprietaire_apres_charge_mois=738.8, statut_reglement="A_CONTROLER")
            _ins(c, "lot10_net_vue_mois", run_id=run10, mois=mois, proprietaire_id="PROP_T1")
            _ins(c, "lot10_resultats", run_id=run10, mois=mois, logement_id="LOG_T1",
                 proprietaire_id="PROP_T1", vision="REEL", resultat=30)
            _ins(c, "lot10_run_mois_provenance", run_id=run10, mois=mois,
                 classification="MOIS_TERMINE_OUVERT", mode_traitement="RECALCULE")
        for resa in (RESA_AOUT, RESA_SEPT, "999999999"):
            _ins(c, "lot10_commissions_a_controler", run_id=run10, reservation_id=resa,
                 code_anomalie_lot10="RESERVATION_EXCLUE_A_CONTROLER")
        _ins(c, "lot10_commissions_a_controler", run_id=run10,
             code_anomalie_lot10=fin.CODE_ANOMALIE_CANAPE)
        # Lot12, calculé juste après le Lot10 du même jour.
        _ins(c, "lot12_runs", run_id=run12, actif=actif, statut="SUCCES",
             date_calcul=f"2026-10-{jour}T10:00:01Z")
        for mois in ("2026-08", "2026-09"):
            pf = f"PF-{run12}-{mois}"
            _ins(c, "lot12_prefactures_entete", run_id=run12, facture_id=pf, mois=mois,
                 proprietaire_id="PROP_T1", logement_id="LOG_T1", reste_a_payer=465.88)
            for n in (1, 2):
                _ins(c, "lot12_prefactures_lignes", run_id=run12, facture_id=pf, ligne_num=n,
                     montant=10)
            _ins(c, "lot12_prefactures_id_legacy", run_id=run12, facture_id=pf,
                 facture_id_legacy=f"LEG-{pf}")
            _ins(c, "lot12_controle_mensuel", run_id=run12, mois=mois, proprietaire_id="PROP_T1",
                 reste_a_payer=465.88)
            _ins(c, "lot12_dashboard_facturation", run_id=run12, mois=mois,
                 proprietaire_id="PROP_T1")
        _ins(c, "lot12_a_controler", run_id=run12, mois="2026-08", proprietaire_id="PROP_T1",
             code_anomalie="CREDIT_A_TRAITER")
        for resa in (RESA_AOUT, RESA_SEPT):
            _ins(c, "lot12_a_controler", run_id=run12, reservation=resa,
                 code_anomalie="RESERVATION_EXCLUE_A_CONTROLER")
        _ins(c, "lot12_a_controler", run_id=run12, code_anomalie=fin.CODE_ANOMALIE_CANAPE)


def _referentiel_importe(c) -> None:
    """Le référentiel ne se dit disponible qu'après un import abouti."""
    _ins(c, "ref_setup_imports", import_id="IMP", horodatage="2026-09-01T00:00:00Z",
         chemin_source="REF_Setup.xlsm", empreinte_source="x", statut="IMPORTE")


@pytest.fixture()
def finition_base(v1_base):
    """Après le cutover : ce que la vraie base portait encore le 2026-10-03."""
    db = v1_base
    c = get_db(db)
    t = "2026-10-02T10:00:00Z"
    _referentiel_importe(c)
    # Réservations Hostaway réelles d'août et de septembre (le mois d'une anomalie en dépend).
    for rid, ci in ((RESA_AOUT, "2026-08-10"), (RESA_SEPT, "2026-09-12")):
        _ins(c, "hostaway_reservations", extraction_id="HAX-1", reservation_id=rid,
             check_in_date=ci)
    _anciens_calculs(c)
    # Lot11 : un contrôle de comptabilité propriétaire (août) et sa décision ; un contrôle de SOURCE.
    pk = f"RES-HA-{RESA_AOUT}||ASSIETTE_NEGATIVE_RAMENEE_ZERO"
    _ins(c, "controles_lot11_constats", ctrl_pk=pk, source_module="COMMISSIONS",
         source_table="lot10_commissions", code_controle="ASSIETTE_NEGATIVE_RAMENEE_ZERO",
         severity="A_CONTROLER")
    _ins(c, "controles_lot11_constats_champs", ctrl_pk=pk, mois="2026-08", logement_id="LOG_T1")
    _ins(c, "controles_lot11_constats", ctrl_pk="RES-X||GESTION_LOGEMENT", source_module="RESERVATIONS",
         source_table="reservations_resolues", code_controle="GESTION_LOGEMENT_HORS_PERIODE",
         severity="BLOQUANT")
    _ins(c, "controles_lot11_constats_champs", ctrl_pk="RES-X||GESTION_LOGEMENT", mois="2026-08")
    for mois in ("2025-01", "2026-08", "2026-09", "TRANSVERSE"):
        _ins(c, "controles_lot11_dashboard_mois", run_id="L11-T", mois=mois,
             facturation_lot12_ok="OUI")
    _ins(c, "controles_suivi", controle_id_opaque="CTRL-T1", ctrl_pk_moteur=pk,
         code_controle="ASSIETTE_NEGATIVE_RAMENEE_ZERO", module="COMMISSIONS", mois="2026-08",
         statut_suivi="ACCEPTE_AVEC_JUSTIFICATION", resultat_humain="EXCEPTION_ACCEPTEE")
    _ins(c, "controles_suivi_historique", controle_id_opaque="CTRL-T1", action="EXCEPTION",
         version=1)
    # La facture fournisseur de mars (créée AVANT le cutover par `base`) et ses tables propres.
    _ins(c, "facture_lignes_menage", ligne_id_opaque="FLM-1", facture_id_opaque="FAC-1",
         type_ligne="MENAGE_EXTERNE", montant_ttc=218, logement_id="LOG_T1")
    _ins(c, "facture_lignes_menage_detail", ligne_id_opaque="FLM-1", quantite=4)
    _ins(c, "facture_lignes_menage_pdf", ligne_id_opaque="FLM-1", nom_prestataire="Mounir")
    _ins(c, "facture_ventilations", ventilation_id_opaque="VEN-1", facture_id_opaque="FAC-1",
         montant_non_affecte=0, base_ponderation="MENAGES", statut="APPLIQUEE")
    _ins(c, "facture_ventilation_parts", ventilation_id_opaque="VEN-1", logement_id="LOG_T1",
         cout_menages_logement=218, poids=1, part_montant=218)
    _ins(c, "facture_evenements", facture_id_opaque="FAC-1", type_evenement="CREATION")
    _ins(c, "facture_interpretations", facture_id_opaque="FAC-1", version=1, source="PDF")
    _ins(c, "facture_pdf_diagnostics", nom_fichier="03-26-Mounir.pdf", statut_extraction="OK",
         numero_facture="0002", montant_total=218, facture_id_opaque="FAC-1", sha256_pdf="abc")
    _ins(c, "facture_pdf_diagnostics", nom_fichier="03-26-Mounir.pdf",
         statut_extraction="NON_SUPPORTE")
    _ins(c, "menages_pdf_fichiers_hash", nom_fichier="03-26-Mounir.pdf", sha256="abc", date_maj=t)
    # Une facture fournisseur de SEPTEMBRE : la V1, conservée telle quelle.
    _ins(c, "factures", facture_id_opaque="FAC-SEP", fournisseur_id_opaque="INT_T",
         facture_ref="2026-09-01", montant_ttc=120, statut="A_CONTROLER", date_facture="2026-09-15")
    # Une charge réglée en DEUX paiements : 3 rapprochements pour 2 charges + 1.
    for i in (5, 6):
        _ins(c, "qonto_transactions_raw", qonto_transaction_uuid=f"U{i}", transaction_id=f"TX{i}",
             qonto_account_id="QACC", charge_utile_json="{}", empreinte=f"E{i}",
             premiere_recuperation=t, derniere_maj=t)
        _ins(c, "qonto_transactions_statut_local", qonto_transaction_uuid=f"U{i}",
             mouvement_id_opaque=f"QMV-{i}", pose_le=t, maj_le=t)
    _ins(c, "charges", charge_id="CHG-K3", date_charge="2026-09-26", mois="2026-09", montant=20.25,
         statut="ACTIVE", statut_controle="VALIDE", code_impact="IC")
    for brp, mvt, m in (("BRP-K3A", "QMV-5", 5.0), ("BRP-K3B", "QMV-6", 15.25)):
        _ins(c, "banque_rapprochements", rapprochement_id_opaque=brp, mouvement_id_opaque=mvt,
             type_objet="CHARGE_FOURNISSEUR", objet_id="CHG-K3", montant_rapproche=m,
             statut="CONFIRME")
    c.commit()
    c.close()
    return db


@pytest.fixture()
def finition_executee(finition_base):
    r = fin.executer(db_path=finition_base, confirmer=True, acteur="test")
    assert r["ok"], r.get("echecs") or r.get("anomalies") or r.get("message")
    return finition_base


def _n(db, sql: str, args: tuple = ()) -> int:
    c = sqlite3.connect(str(db))
    try:
        return c.execute(sql, args).fetchone()[0]
    finally:
        c.close()


def _empreintes(db) -> dict[str, str]:
    from app.services import cutover_v1_service as cut
    c = cut._connexion(db, lecture_seule=True)
    try:
        return {t: cut.empreinte(c, t) for t in fin.TABLES_PROTEGEES}
    finally:
        c.close()


# ── Paramètre unique ──────────────────────────────────────────────────────────────────────────────

def test_cle_du_parametre_v1_identique_cote_moteur_et_application():
    assert dbm.CLE_DEBUT_V1 == v1.CLE_PARAMETRE


# ── Lot10 : août exclu, septembre calculé, anomalies anciennes retirées ───────────────────────────

def test_lot10_aout_exclu_septembre_calcule_et_trace(v1_base):
    pd = pytest.importorskip("pandas")
    import lot10_calculer_resultats as l10

    conn = get_db(v1_base)
    try:
        cls = l10.classifier_mois_lot10(conn, ["2026-07", "2026-08", "2026-09"],
                                        date_reference=datetime(2026, 10, 3))
    finally:
        conn.close()
    for mois in ("2026-07", "2026-08"):
        assert cls[mois] == {"classification": l10.CLASS_ANTERIEUR_V1,
                             "mode": l10.MODE_EXCLU_ANTERIEUR_V1}
    assert cls["2026-09"] == {"classification": l10.CLASS_MOIS_TERMINE_OUVERT,
                              "mode": l10.MODE_RECALCULE}

    def df():
        return pd.DataFrame([{"mois": "2026-08", "proprietaire_id": "P", "montant": 1},
                             {"mois": "2026-09", "proprietaire_id": "P", "montant": 2}])

    sortie = l10.exclure_mois_clotures(cls, df(), df(), df(), df(), df(), df(), df())
    for d in sortie[:7]:                   # règlement, net, commissions, exploitation, résultats…
        assert list(d["mois"]) == ["2026-09"]
    provenance = {p["mois"]: p["mode_traitement"] for p in sortie[7]}
    assert provenance["2026-08"] == l10.MODE_EXCLU_ANTERIEUR_V1


def test_lot10_sans_cutover_rien_n_est_exclu(tmp_db):
    pytest.importorskip("pandas")
    import lot10_calculer_resultats as l10

    conn = get_db(tmp_db)
    try:
        cls = l10.classifier_mois_lot10(conn, ["2026-08"], date_reference=datetime(2026, 10, 3))
    finally:
        conn.close()
    assert cls["2026-08"]["mode"] == l10.MODE_RECALCULE


def test_lot10_anomalies_commission_anterieures_retirees_les_autres_conservees():
    pd = pytest.importorskip("pandas")
    import lot10_calculer_resultats as l10

    df_ac = pd.DataFrame([
        {"reservation_id": RESA_AOUT, "code_anomalie_lot10": "RESERVATION_EXCLUE_A_CONTROLER"},
        {"reservation_id": RESA_SEPT, "code_anomalie_lot10": "RESERVATION_EXCLUE_A_CONTROLER"},
        {"reservation_id": "424242", "code_anomalie_lot10": "RESERVATION_EXCLUE_A_CONTROLER"},
        {"mois": "2026-08", "code_anomalie_lot10": fin.CODE_ANOMALIE_CANAPE},
        {"mois": "2026-10", "code_anomalie_lot10": fin.CODE_ANOMALIE_CANAPE},
    ])
    df_res = pd.DataFrame([{"reservation_id_hostaway": float(RESA_AOUT), "date_arrivee": "2026-08-10"},
                           {"reservation_id_hostaway": RESA_SEPT, "date_arrivee": "2026-09-12"}])
    garde = l10.exclure_anomalies_anterieures_v1(df_ac, df_res, "2026-09")
    # Août retiré (par la réservation comme par le mois) ; septembre, octobre et l'anomalie dont le
    # mois est inconnu restent : on ne retire que ce qui est prouvé antérieur.
    assert {r for r in garde["reservation_id"] if isinstance(r, str)} == {RESA_SEPT, "424242"}
    assert [m for m in garde["mois"] if isinstance(m, str)] == ["2026-10"]
    assert l10.exclure_anomalies_anterieures_v1(df_ac, df_res, None) is df_ac   # sans cutover


# ── Lot12 et Lot11 ────────────────────────────────────────────────────────────────────────────────

def test_lot12_aout_impossible_septembre_possible(v1_base):
    from app.services import lot12_prefactures_service as lot12
    c = get_db(v1_base)
    c.execute("INSERT INTO lot10_runs (run_id, actif, statut) VALUES ('L10', 1, 'SUCCES')")
    for mois in ("2026-08", "2026-09"):
        c.execute("INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
                  "total_commission_mois, montant_du_conciergerie) VALUES "
                  "('L10', ?, 'LOG_T1', 'PROP_T1', 10, 10)", (mois,))
    c.commit()
    c.close()
    lot12.construire(db_path=v1_base)
    # Août n'entre même pas dans le contrôle mensuel du run ; septembre y est.
    assert _ids(v1_base, "SELECT c.mois FROM lot12_controle_mensuel c JOIN lot12_runs r "
                         "ON r.run_id = c.run_id AND r.actif = 1") == {"2026-09"}
    assert _ids(v1_base, "SELECT e.mois FROM lot12_prefactures_entete e JOIN lot12_runs r "
                         "ON r.run_id = e.run_id AND r.actif = 1 WHERE e.mois < '2026-09'") == set()


def test_lot11_ne_fabrique_ni_mois_ni_ecart_avant_la_v1(v1_base, monkeypatch):
    from app.services import controles_lot11_service as l11
    from app.services import menages_ecarts_service as ecarts

    tableau = l11._dashboard_mois(
        [{"mois": "2026-08", "severity": "A_CONTROLER"}],
        [{"mois": "2025-01", "statut_mois": "CLOTURE"}, {"mois": "2026-09", "statut_mois": "OUVERT"}],
        True, mois_v1="2026-09")
    assert [d["mois"] for d in tableau] == ["2026-09", "TRANSVERSE"]

    # Écarts ménages externes ↔ Hostaway : un mois d'avant la V1 n'est jamais comparé.
    from app.readers import menages_reader
    from app.services import facture_lignes_menage_service as flm

    class _Src:
        class etat:
            etat = "OK"
        lignes = [{"mois": "2026-08", "logement_id": "LOG_T1", "nb_menages_realises": 4},
                  {"mois": "2026-09", "logement_id": "LOG_T1", "nb_menages_realises": 2}]

    monkeypatch.setattr(menages_reader, "hostaway_comptage", lambda db_path=None: _Src())
    monkeypatch.setattr(flm, "lignes_externes_pour_reader", lambda db_path=None: [])
    assert {l["mois"] for l in ecarts.calculer(db_path=v1_base)} == {"2026-09"}

    # Même règle côté moteur (lot6c, Lot11 legacy).
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE parametres_societe_facturation (cle TEXT, valeur TEXT)")
    conn.execute("INSERT INTO parametres_societe_facturation VALUES (?, '2026-09-01')",
                 (dbm.CLE_DEBUT_V1,))
    conn.execute("CREATE TABLE menages_taches_enrichies (mois TEXT, logement_id TEXT, "
                 "proprietaire_id TEXT, statut_menage TEXT, compte_comme_menage TEXT)")
    for mois in ("2026-08", "2026-09"):
        conn.execute("INSERT INTO menages_taches_enrichies VALUES (?, 'LOG_T1', 'P', 'réalisé', 'OUI')",
                     (mois,))
    assert {l["mois"] for l in dbm.calculer_ecarts_menages_externes(conn)} == {"2026-09"}


# ── Services propriétaire : rien avant la V1, ni en lecture ni en création ─────────────────────────

def test_releve_aout_aucune_comptabilite_meme_si_des_calculs_trainent(finition_base):
    from app.services import proprietaire_performance_service as perf
    from app.services import proprietaires_reglements_service as regl
    from app.services import proprietaires_service as ps

    # AVANT la purge, des lignes d'août existent encore : le service ne les lit pas.
    assert ps.load_releve("PROP_T1", "2026-08")["status"] == v1.ST_HORS_V1
    assert ps.load_prefacture("PROP_T1", "2026-08")["status"] == v1.ST_HORS_V1
    assert perf.releve("PROP_T1", "2026-08", db_path=finition_base)["status"] == v1.ST_HORS_V1
    assert perf.proprietaires_du_mois("2026-08", db_path=finition_base) == []
    assert "2026-08" not in perf.mois_disponibles(db_path=finition_base)
    assert regl.load_owner_detail("PROP_T1", "2026-08")["status"] == v1.ST_HORS_V1
    assert regl.load_dashboard(mois="2026-08")["hors_v1"] is True
    assert "2026-08" not in regl.load_periods()
    assert "2026-08" not in ps.load_detail("PROP_T1")["mois_disponibles"]
    # Septembre reste servi normalement.
    assert ps.load_releve("PROP_T1", "2026-09")["status"] == "OK"


def test_creation_comptabilite_proprietaire_anterieure_refusee(v1_base):
    from app.services import proprietaires_suivi_service as suivi
    from app.services import proprietaires_tresorerie_service as tres

    c = get_db(v1_base)
    _referentiel_importe(c)
    c.commit()
    c.close()

    with pytest.raises(suivi.ReleveRefuse) as exc:
        suivi.creer_ou_charger("PROP_T1", "2026-08", acteur="t", db_path=v1_base)
    assert str(exc.value) == DEBUT_V1
    assert suivi.creer_ou_charger("PROP_T1", "2026-09", acteur="t", db_path=v1_base)
    refus = tres.previsualiser("PROP_T1", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", 12,
                               "2026-08-31", db_path=v1_base)
    assert (refus["ok"], refus["code"], refus["message"]) == (
        False, v1.E_COMPTABILITE_PROPRIETAIRE_AVANT_V1, DEBUT_V1)
    assert tres.previsualiser("PROP_T1", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", 12,
                              "2026-09-15", db_path=v1_base)["ok"]


def test_ecrans_aout_message_propre_septembre_normal(finition_executee, client):
    for url in ("/proprietaires/PROP_T1/2026-08", "/proprietaires/PROP_T1/2026-08/prefacture",
                "/releves-proprietaires?mois=2026-08", "/proprietaires-reglements?mois=2026-08",
                "/proprietaires-reglements/PROP_T1?mois=2026-08",
                "/proprietaires-reglements/a-controler?mois=2026-08"):
        r = client.get(url)
        assert r.status_code == 200, url
        assert MESSAGE in r.text, url
        assert "Historique" not in r.text and "hors comptabilité" not in r.text.lower(), url
        assert "465,88" not in r.text and "465.88" not in r.text, url
    r = client.get("/proprietaires/PROP_T1/2026-08")
    assert "Reste à payer" not in r.text
    r = client.get("/proprietaires/PROP_T1/2026-09")
    assert r.status_code == 200 and MESSAGE not in r.text and "Reste à payer" in r.text


# ── Factures fournisseurs : refus serveur, verrou base ────────────────────────────────────────────

def test_facture_fournisseur_aout_refusee_septembre_autorisee(v1_base, ecriture_active):
    form = {"fournisseur_id_opaque": "INT_T", "facture_ref": "AOUT-1", "montant_ttc": 50}
    refus = fact.creer({**form, "date_facture": "2026-08-15"}, acteur="t", db_path=v1_base)
    assert (refus["ok"], refus["code"], refus["message"]) == (
        False, v1.E_FACTURE_FOURNISSEUR_AVANT_V1, DEBUT_V1)
    assert any(e["code"] == fact.E_DATE_ANTERIEURE_V1 and e["message"] == DEBUT_V1
               for e in fact.valider({**form, "date_facture": "2026-08-15"}, v1_base))
    assert _n(v1_base, "SELECT COUNT(*) FROM factures WHERE facture_ref='AOUT-1'") == 0
    ok = fact.creer({**form, "facture_ref": "SEPT-1", "date_facture": "2026-09-15"}, acteur="t",
                    db_path=v1_base)
    assert ok["ok"], ok


def test_facture_fournisseur_anterieure_refusee_par_la_base(v1_base):
    c = sqlite3.connect(str(v1_base))
    try:
        with pytest.raises(sqlite3.DatabaseError, match="FACTURE_FOURNISSEUR_AVANT_V1"):
            c.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                      "montant_ttc, statut, date_facture) VALUES ('FAC-X','INT_T','X',1,"
                      "'A_CONTROLER','2026-08-31')")
        c.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                  "montant_ttc, statut, date_facture) VALUES ('FAC-Y','INT_T','Y',1,"
                  "'A_CONTROLER','2026-09-01')")
        with pytest.raises(sqlite3.DatabaseError, match="FACTURE_FOURNISSEUR_AVANT_V1"):
            c.execute("UPDATE factures SET date_facture='2026-08-01' WHERE facture_id_opaque='FAC-Y'")
    finally:
        c.close()


def test_sans_cutover_une_facture_fournisseur_ancienne_reste_possible(tmp_db):
    c = sqlite3.connect(str(tmp_db))
    try:
        c.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                  "montant_ttc, statut, date_facture) VALUES ('FAC-X','INT_T','X',1,"
                  "'A_CONTROLER','2026-08-31')")
    finally:
        c.close()


# ── Import PDF : un document antérieur est reconnu, tracé, jamais importé ─────────────────────────

def _types_lignes(db) -> None:
    c = get_db(db)
    for tid, libelle, compte in (("TLM_001", "MENAGE_STANDARD", "OUI"),
                                 ("TLM_002", "REMISE_EN_ETAT", "OUI"),
                                 ("TLM_005", "ACHAT_PRODUIT", "NON"),
                                 ("TLM_006", "AUTRE", "NON")):
        c.execute("INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                  "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                  "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                  (tid, libelle, compte))
    c.commit()
    c.close()


@pdf_reels
def test_import_pdf_anterieur_ignore_trace_et_idempotent(v1_base, tmp_path, ecriture_active,
                                                         monkeypatch):
    pytest.importorskip("fitz")
    from app.services import facture_menage_pdf_service as imp
    from app.services import menages_pdf_import_service as pdf_import

    _types_lignes(v1_base)
    d = tmp_path / "depot"
    d.mkdir()
    shutil.copy(PDF_AOUT, d / PDF_AOUT.name)
    factures_avant = _n(v1_base, "SELECT COUNT(*) FROM factures")
    for _ in range(2):                      # même résultat, deux fois
        r = pdf_import.importer_nouveaux(acteur="t", dossier=d, db_path=v1_base)
        assert (r["ok"], r["nb_importees"], r["nb_anterieures_v1"]) == (True, 0, 1), r
        assert r["details"][0]["statut"] == pdf_import.STATUT_ANTERIEURE_V1
        # Le second passage ne relit même plus le document.
        monkeypatch.setattr(imp, "importer", lambda *a, **k: pytest.fail("document relu"))
    assert _n(v1_base, "SELECT COUNT(*) FROM factures") == factures_avant
    assert _n(v1_base, "SELECT COUNT(*) FROM facture_lignes_menage") == 0
    assert _n(v1_base, "SELECT COUNT(*) FROM facture_ventilations") == 0
    assert _n(v1_base, "SELECT COUNT(*) FROM facture_pdf_diagnostics WHERE nom_fichier=? AND "
                       "statut_extraction='ANTERIEURE_V1' AND facture_id_opaque IS NULL",
              (PDF_AOUT.name,)) == 1
    assert pdf_import.apercu(dossier=d, db_path=v1_base)["details"][0]["statut"] == \
        pdf_import.STATUT_ANTERIEURE_V1
    assert pdf_import.recharger(acteur="t", dossier=d, db_path=v1_base)["nb_importees"] == 0


@pdf_reels
def test_import_pdf_de_la_periode_v1_autorise(v1_base, tmp_path, ecriture_active, monkeypatch):
    pytest.importorskip("fitz")
    import lib_menages_externes_pdf as pdfex
    from app.services import menages_pdf_import_service as pdf_import

    _types_lignes(v1_base)
    d = tmp_path / "depot"
    d.mkdir()
    shutil.copy(PDF_AOUT, d / PDF_AOUT.name)
    reel = pdfex.extraire_pdf

    def en_septembre(path):                 # même pièce, datée de septembre
        fac = reel(path)
        fac.date_facture = "2026-09-30"
        return fac

    monkeypatch.setattr(pdfex, "extraire_pdf", en_septembre)
    r = pdf_import.importer_nouveaux(acteur="t", dossier=d, db_path=v1_base)
    assert (r["nb_importees"], r["nb_anterieures_v1"]) == (1, 0), r
    assert _n(v1_base, "SELECT COUNT(*) FROM factures WHERE date_facture='2026-09-30'") == 1


# ── La purge ──────────────────────────────────────────────────────────────────────────────────────

def test_simulation_ne_modifie_rien_deux_passages_identiques(finition_base):
    avant = _logique(finition_base)
    r1, r2 = fin.simuler(db_path=finition_base), fin.simuler(db_path=finition_base)
    assert _logique(finition_base) == avant
    assert r1["empreinte_rapport"] == r2["empreinte_rapport"] and not r1["anomalies"]
    cp = r1["comptabilite_proprietaire"]
    for t in ("lot10_commissions", "lot10_net_exploitation", "lot10_net_reglement",
              "lot10_net_vue_mois", "lot10_resultats", "lot10_run_mois_provenance",
              "lot12_prefactures_entete", "lot12_controle_mensuel", "lot12_dashboard_facturation"):
        assert cp[t]["a_supprimer"] == 2, t          # août, dans le run actif ET l'inactif
    assert cp["lot12_prefactures_lignes"]["a_supprimer"] == 4
    assert cp["lot10_commissions_a_controler"]["a_supprimer"] == 4   # août + canapé, × 2 runs
    assert cp["lot12_a_controler"]["a_supprimer"] == 6
    assert cp["controles_lot11_constats"]["a_supprimer"] == 1
    assert cp["controles_suivi"]["a_supprimer"] == 1
    assert cp["controles_lot11_dashboard_mois"]["a_supprimer"] == 2
    assert r1["factures_fournisseurs_resume"]["purge"] == 1
    assert r1["factures_fournisseurs_resume"]["keep_by_dependency"] == 0
    assert r1["dettes_actives_avant"]["nb"] == 1
    assert r1["verdicts_anterieure_v1_a_poser"] == 1


def test_purge_de_tous_les_runs_et_conservation_prouvee(finition_base):
    proteges = _empreintes(finition_base)
    r = fin.executer(db_path=finition_base, confirmer=True, acteur="test")
    assert r["ok"], r.get("echecs")
    assert all(v["ok"] for v in r["verifications"])
    # Plus rien d'avant la V1, dans aucun run.
    for t in fin.LOT10_PAR_MOIS + fin.LOT12_PAR_MOIS + (fin.LOT12_ENTETES,):
        assert _n(finition_base, f"SELECT COUNT(*) FROM {t} WHERE mois < '2026-09'") == 0, t
    # Septembre reste, dans les deux runs.
    assert _n(finition_base, "SELECT COUNT(*) FROM lot10_net_reglement WHERE mois='2026-09'") == 2
    assert _n(finition_base, "SELECT COUNT(*) FROM lot12_prefactures_lignes") == 4
    assert _n(finition_base, "SELECT COUNT(*) FROM lot10_commissions_a_controler") == 4
    # Lot11 : le contrôle de comptabilité et sa décision partent, celui de la source reste.
    assert _ids(finition_base, "SELECT ctrl_pk FROM controles_lot11_constats") == {
        "RES-X||GESTION_LOGEMENT"}
    assert _n(finition_base, "SELECT COUNT(*) FROM controles_suivi") == 0
    assert _n(finition_base, "SELECT COUNT(*) FROM controles_suivi_historique") == 0
    assert _ids(finition_base, "SELECT mois FROM controles_lot11_dashboard_mois") == {
        "2026-09", "TRANSVERSE"}
    # Facture fournisseur antérieure : partie avec toutes ses tables, verdict posé, pièce tracée.
    assert _ids(finition_base, "SELECT facture_id_opaque FROM factures") == {"FAC-SEP"}
    for t in ("facture_lignes_menage", "facture_lignes_menage_detail", "facture_lignes_menage_pdf",
              "facture_ventilations", "facture_ventilation_parts", "facture_evenements",
              "facture_interpretations"):
        assert _n(finition_base, f"SELECT COUNT(*) FROM {t}") == 0, t
    assert _n(finition_base, "SELECT COUNT(*) FROM facture_classification "
                             "WHERE facture_id_opaque='FAC-1'") == 0
    assert _ids(finition_base, "SELECT statut_extraction FROM facture_pdf_diagnostics") == {
        "NON_SUPPORTE", "ANTERIEURE_V1"}
    assert _n(finition_base, "SELECT COUNT(*) FROM menages_pdf_fichiers_hash") == 1
    # Banque, charges, rapprochements, Hostaway, référentiels, société : à l'identique.
    assert _empreintes(finition_base) == proteges
    c = sqlite3.connect(str(finition_base))
    try:
        assert c.execute("PRAGMA foreign_key_check").fetchall() == []
        assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        c.close()


def test_apres_purge_tout_est_a_zero_et_verifie(finition_executee):
    from app.services import creances_dettes_service as cd

    v = fin.verifier(db_path=finition_executee)
    assert v["checks"]["ok"], {k: x for k, x in v["checks"].items() if x is False}
    assert not any(v["etat_comptable_anterieur"].values())
    assert cd.creances(db_path=finition_executee) == []
    assert [d["numero"] for d in cd.dettes(db_path=finition_executee)] == ["2026-09-01"]


def test_rapprochements_plus_nombreux_que_les_charges_coherents(finition_base):
    rap = fin.simuler(db_path=finition_base)["rapprochements_charges"]
    k3 = next(l for l in rap["lignes"] if l["charge"] == "CHG-K3")
    assert (k3["nb_rapprochements"], k3["montant_rapproche"], k3["montant_charge"]) == (2, 20.25, 20.25)
    assert rap["nb_rapprochements"] == rap["nb_charges"] + 1
    assert rap["total_charges"] == rap["total_rapproche"]
    assert not rap["charges_sans_rapprochement"] and not rap["rapprochements_orphelins"]
    assert not rap["ecarts"]                 # aucun double comptage : chaque euro une seule fois


def test_rollback_si_un_invariant_echoue(finition_base):
    avant = _logique(finition_base)

    def casser(conn):                       # une source modifiée pendant la purge
        conn.execute("UPDATE hostaway_reservations SET check_in_date = '2026-07-04' "
                     "WHERE reservation_id = 'R-JUL'")

    r = fin.executer(db_path=finition_base, confirmer=True, acteur="test", _apres_purge=casser)
    assert (r["ok"], r["code"]) == (False, fin.E_VERIFICATION)
    assert {e["code"] for e in r["echecs"]} >= {"M", "PROTEGEES"}
    assert _logique(finition_base) == avant


def test_rollback_sur_erreur_inattendue(finition_base):
    avant = _logique(finition_base)

    def panne(conn):
        raise RuntimeError("coupure")

    r = fin.executer(db_path=finition_base, confirmer=True, acteur="test", _apres_purge=panne)
    assert (r["ok"], r["code"]) == (False, fin.E_VERIFICATION)
    assert _logique(finition_base) == avant


def test_refus_sans_confirmation_sans_cutover_et_second_passage_sans_effet(finition_base, base):
    assert fin.executer(db_path=finition_base, confirmer=False, acteur="t")["code"] == \
        fin.E_CONFIRMATION
    assert fin.executer(db_path=finition_base, confirmer=True, acteur="t")["ok"]
    apres = _logique(finition_base)
    second = fin.executer(db_path=finition_base, confirmer=True, acteur="t")
    assert second["ok"] and second["lignes_a_supprimer"] == 0
    assert _logique(finition_base) == apres


def test_refus_sans_cutover(tmp_db):
    r = fin.executer(db_path=tmp_db, confirmer=True, acteur="t")
    assert r["code"] == fin.E_CUTOVER_ABSENT


def test_facture_fournisseur_dependante_bloque_toute_la_finition(finition_base):
    c = get_db(finition_base)
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-FAC", mouvement_id_opaque="QMV-1",
         type_objet="FACTURE_FOURNISSEUR", objet_id="FAC-1", montant_rapproche=218,
         statut="PROPOSE")
    c.commit()
    c.close()
    avant = _logique(finition_base)
    r = fin.executer(db_path=finition_base, confirmer=True, acteur="t")
    assert r["code"] == fin.E_ANOMALIES and "FAC-1" in " ".join(r["anomalies"])
    assert _logique(finition_base) == avant


def test_la_trace_d_exclusion_lot10_n_est_ni_un_reliquat_ni_purgee(finition_executee):
    """Un run Lot10 écrit, pour chaque mois antérieur, une provenance `EXCLU_PERIMETRE_ANTERIEUR_V1` :
    c'est la preuve qu'il n'a rien produit. Elle ne compte pas comme reliquat, et une seconde finition
    ne la supprime pas."""
    c = get_db(finition_executee)
    _ins(c, "lot10_runs", run_id="L10-NOUVEAU", actif=0, statut="SUCCES")
    _ins(c, "lot10_run_mois_provenance", run_id="L10-NOUVEAU", mois="2026-08",
         classification="ANTERIEUR_V1", mode_traitement=fin.MODE_TRACE_EXCLUSION_V1)
    c.commit()
    c.close()
    etat = fin.etat_comptable(sqlite3.connect(str(finition_executee)), "2026-09-01")
    assert not any(etat.values()), {k: n for k, n in etat.items() if n}
    second = fin.executer(db_path=finition_executee, confirmer=True, acteur="t")
    assert second["ok"] and second["lignes_a_supprimer"] == 0
    assert _n(finition_executee, "SELECT COUNT(*) FROM lot10_run_mois_provenance WHERE "
                                 "run_id='L10-NOUVEAU'") == 1
    assert fin.verifier(db_path=finition_executee)["checks"]["comptabilite_anterieure_0"]


def test_rien_ne_revient_apres_redemarrage_ni_recalcul(finition_executee):
    from fastapi.testclient import TestClient

    from app.main import app
    from app.services import lot12_prefactures_service as lot12

    for _ in range(2):                       # deux démarrages complets (migrations comprises)
        with TestClient(app) as c:
            assert c.get("/health").status_code == 200
        lot12.construire(db_path=finition_executee)
        etat = fin.etat_comptable(sqlite3.connect(str(finition_executee)), "2026-09-01")
        assert not any(etat.values()), {k: n for k, n in etat.items() if n}
    assert _n(finition_executee, "SELECT COUNT(*) FROM factures WHERE date_facture < '2026-09-01'") == 0


@pdf_reels
def test_apres_purge_l_import_pdf_ne_recree_rien(finition_executee, tmp_path, ecriture_active):
    pytest.importorskip("fitz")
    from app.services import menages_pdf_import_service as pdf_import

    _types_lignes(finition_executee)
    d = tmp_path / "depot"
    d.mkdir()
    for p in (PDF_MARS, PDF_AOUT):
        shutil.copy(p, d / p.name)
    for _ in range(2):
        r = pdf_import.importer_nouveaux(acteur="t", dossier=d, db_path=finition_executee)
        assert (r["nb_importees"], r["nb_remplacees"], r["nb_anterieures_v1"]) == (0, 0, 2), r
    assert _ids(finition_executee, "SELECT facture_id_opaque FROM factures") == {"FAC-SEP"}
    assert _n(finition_executee, "SELECT COUNT(*) FROM charges") == 3

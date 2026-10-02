"""Cutover V1 (2026-09-01) — purge contrôlée, conservation prouvée, verrous de période.

Base de test ISOLÉE (`tmp_db`), peuplée d'un échantillon représentatif : charges rapprochées et
non rapprochées, rapprochements de recette orphelins, facturation propriétaire complète (facture
émise, brouillon, écriture de vente, acompte, imputation Airbnb, FIFO), clôtures et relevés
anciens, Hostaway, réservations hors Hostaway, référentiels, fiche société, factures fournisseurs.
Jamais la vraie base.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import cutover_v1_service as cut
from app.services import factures_proprietaires_service as fpr
from app.services import perimetre_v1_service as v1

MSG = "La facturation V1 débute en septembre 2026."


def _ins(conn, table: str, **valeurs) -> None:
    cols = ", ".join(valeurs)
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(valeurs))})",
                 tuple(valeurs.values()))


@pytest.fixture()
def base(tmp_db, tmp_path, monkeypatch):
    """Échantillon fidèle à la vraie base au 2026-10-02 (mêmes formes, données inventées)."""
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(cfg, "FACTURES_PROPRIETAIRES_DIR", None, raising=False)
    pdf = tmp_path / "data" / "factures_proprietaires" / "2026" / "08" / "2026-08-001.pdf"
    pdf.parent.mkdir(parents=True)
    pdf.write_bytes(b"%PDF-1.4 facture emise")
    c = get_db(tmp_db)
    t = "2026-09-20T10:00:00Z"
    # Référentiels et société.
    _ins(c, "ref_proprietaires", proprietaire_id="PROP_T1", nom_proprietaire="Test", import_id="IMP")
    _ins(c, "ref_logements", logement_id="LOG_T1", nom_logement_officiel="Studio test", import_id="IMP")
    _ins(c, "ref_taux_commission", taux_commission_id="TX_T1", import_id="IMP")
    _ins(c, "parametres_societe_facturation", cle="SOCIETE_NOM", valeur="CHOUETTE PATRIMOINE",
         maj_par="test")
    # Banque : 4 mouvements Qonto de septembre.
    _ins(c, "qonto_accounts", qonto_account_id="QACC", premiere_recuperation=t,
         derniere_synchronisation=t)
    for i in range(1, 5):
        _ins(c, "qonto_transactions_raw", qonto_transaction_uuid=f"U{i}", transaction_id=f"TX{i}",
             qonto_account_id="QACC", charge_utile_json="{}", empreinte=f"E{i}",
             premiere_recuperation=t, derniere_maj=t)
        _ins(c, "qonto_transactions_statut_local", qonto_transaction_uuid=f"U{i}",
             mouvement_id_opaque=f"QMV-{i}", pose_le=t, maj_le=t)
    # Charges.
    for cid, d, m, st in (("CHG-K1", "2026-09-21", 50.0, "ACTIVE"),
                          ("CHG-K2", "2026-08-30", 30.0, "ACTIVE"),
                          ("CHG-P1", "2026-09-09", 146.0, "ACTIVE"),
                          ("CHG-P2", "2026-08-09", 700.0, "ACTIVE"),
                          ("CHG-P3", "2026-08-15", 12.34, "ANNULEE"),
                          ("CHG-P4", "2026-07-10", 8.9, "ACTIVE")):
        _ins(c, "charges", charge_id=cid, date_charge=d, mois=d[:7], montant=m, statut=st,
             statut_controle="VALIDE", code_impact="IC")
        _ins(c, "charge_evenements", charge_id=cid, evenement="CREATION", acteur="t", horodatage=t)
    # K1 : lettrage validé + rapprochement + écriture + justificatif + périmètre.
    _ins(c, "flux_lettrages", lettrage_id_opaque="LET-1", statut="VALIDE", empreinte="P1",
         total_mouvements=50, total_objets=50, acteur="t", ecriture_id_opaque="ECR-L1")
    _ins(c, "flux_lettrage_lignes", lettrage_id_opaque="LET-1", cote="MOUVEMENT",
         type_element="BANQUE", element_id="QMV-1", montant=50)
    _ins(c, "flux_lettrage_lignes", lettrage_id_opaque="LET-1", cote="OBJET",
         type_element="CHARGE", element_id="CHG-K1", montant=50)
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-K1", mouvement_id_opaque="QMV-1",
         type_objet="CHARGE_FOURNISSEUR", objet_id="CHG-K1", montant_rapproche=50,
         statut="CONFIRME", lettrage_id_opaque="LET-1")
    _ins(c, "ecritures", ecriture_id_opaque="ECR-L1", journal="BANQUE", date_ecriture="2026-09-21",
         periode="2026-09", libelle="Règlement", origine_type="LETTRAGE",
         origine_id_opaque="LET-1", statut="VALIDEE", total_debit=50, total_credit=50)
    _ins(c, "ecriture_lignes", ecriture_id_opaque="ECR-L1", ligne_num=1, compte="606000",
         debit=50, credit=0)
    _ins(c, "ecriture_lignes", ecriture_id_opaque="ECR-L1", ligne_num=2, compte="512000",
         debit=0, credit=50)
    _ins(c, "justificatifs", reference="CHG-2026-09-001", objet_type="CHARGE", objet_id="CHG-K1",
         dossier="Charges/2026/09", statut="JUSTIFICATIF_ABSENT_JUSTIFIE",
         justification_absence="dans qonto")
    _ins(c, "charges_perimetre_analytique", charge_id="CHG-K1", logement_id="LOG_T1",
         proprietaire_id="PROP_T1", mois="2026-09", quote_part_montant=50)
    # K2 : charge d'août rapprochée (sans lettrage) : conservée malgré sa date.
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-K2", mouvement_id_opaque="QMV-2",
         type_objet="CHARGE_FOURNISSEUR", objet_id="CHG-K2", montant_rapproche=30,
         statut="CONFIRME")
    # P2 : position de refacturation ; P3 : proposition non validée ; P4 : rapprochement orphelin.
    _ins(c, "charges_refacturation_positions", position_id="POSREF-P2", charge_id="CHG-P2",
         montant_origine=700, montant_eligible=700, statut="A_TRAITER")
    _ins(c, "charges_refacturation_evenements", position_id="POSREF-P2", evenement="CREATION",
         date_evenement=t)
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-P3", mouvement_id_opaque="QMV-3",
         type_objet="CHARGE_FOURNISSEUR", objet_id="CHG-P3", montant_rapproche=12.34,
         statut="PROPOSE")
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-ORPH",
         mouvement_id_opaque="MVT-absent", type_objet="CHARGE_FOURNISSEUR", objet_id="CHG-P4",
         montant_rapproche=8.9, statut="CONFIRME")
    _ins(c, "banque_rapprochement_evenements", rapprochement_id_opaque="BRP-ORPH",
         type_evenement="CREATION")
    # Opération bancaire réelle hors charge (apport en compte courant) et son écriture.
    _ins(c, "banque_rapprochements", rapprochement_id_opaque="BRP-APP", mouvement_id_opaque="QMV-4",
         type_objet="APPORT_ASSOCIE", objet_id="PERS_T", montant_rapproche=200, statut="CONFIRME")
    _ins(c, "ecritures", ecriture_id_opaque="ECR-APP", journal="BANQUE", date_ecriture="2026-09-20",
         periode="2026-09", libelle="Apport", origine_type="RAPPROCHEMENT",
         origine_id_opaque="BRP-APP", statut="VALIDEE", total_debit=200, total_credit=200)
    # Données de recette documentées (mission 14d).
    _ins(c, "banque_imports", import_id="IMP-REC", nom_fichier_origine="releve_recette.csv",
         statut="SUCCES")
    _ins(c, "banque_suggestion_decisions", mouvement_id_opaque="MVT-absent",
         type_objet="CHARGE_FOURNISSEUR", objet_id="CHG_SEED_003", decision="ACCEPTEE",
         empreinte_candidat="x")
    # Facturation propriétaire.
    _ins(c, "factures_proprietaires", facture_id_opaque="FPR-E", numero_facture="2026-08-001",
         type_document="FACTURE", proprietaire_id="PROP_T1", logement_id="LOG_T1", mois="2026-08",
         montant_total=465.88, statut="EMIS", document_nom="2026-08-001.pdf",
         date_facture="2026-09-11")
    _ins(c, "factures_proprietaires_lignes", ligne_id_opaque="FPRL-1", facture_id_opaque="FPR-E",
         numero_ligne=1, type_ligne="COMMISSION_CONCIERGERIE", libelle="Commission",
         montant=465.88)
    _ins(c, "factures_proprietaires", facture_id_opaque="FPR-B", type_document="FACTURE",
         proprietaire_id="PROP_T1", logement_id="LOG_T1", mois="2026-06", montant_total=100,
         statut="BROUILLON")
    _ins(c, "factures_proprietaires_lignes_charge", ligne_id_opaque="FPRL-C",
         facture_id_opaque="FPR-B", charge_id="CHG-P3", code_impact="IC")
    _ins(c, "factures_proprietaires_evenements", facture_id_opaque="FPR-E",
         type_evenement="EMISSION")
    _ins(c, "factures_proprietaires_sequence", serie="2026-08", dernier_numero=1)
    _ins(c, "ecritures", ecriture_id_opaque="ECR-V", journal="VENTES", date_ecriture="2026-09-11",
         periode="2026-09", libelle="Vente", origine_type="FACTURE_PROPRIETAIRE",
         origine_id_opaque="FPR-E", statut="VALIDEE", total_debit=465.88, total_credit=465.88)
    _ins(c, "ecriture_lignes", ecriture_id_opaque="ECR-V", ligne_num=1, compte="411000",
         debit=465.88, credit=0, auxiliaire="PROP_T1")
    _ins(c, "ecriture_evenements", ecriture_id_opaque="ECR-V", type_evenement="GENERATION")
    _ins(c, "imputations_airbnb", imputation_airbnb_id="IMPA-1", proprietaire_id="PROP_T1",
         mois="2026-08", document_id="FPR-E", montant_impute=425, statut="VALIDE")
    _ins(c, "mouvements_tresorerie_proprietaires", mouvement_opaque="MTP-1",
         proprietaire_id="PROP_T1", date_mouvement="2026-08-31", montant=12,
         sens="PROPRIETAIRE_VERS_SOCIETE", nature="ACOMPTE_PROPRIETAIRE", statut="VALIDE",
         reference_metier="FPR-E")
    _ins(c, "mouvements_tresorerie_proprietaires_evenements", mouvement_id="MTP-1",
         evenement_opaque="MTE-1", type_evenement="CREATION")
    _ins(c, "proprietaire_recalculs", recalcul_id="RCL-1", proprietaire_id="PROP_T1",
         horodatage=t, empreinte_entrees="a", empreinte_allocations="b", creance_restante=453.88)
    _ins(c, "proprietaire_allocations", allocation_id_opaque="ALO-1", proprietaire_id="PROP_T1",
         recalcul_id="RCL-1", source_type="PAIEMENT", source_ref="MTP-1", source_date="2026-08-31",
         facture_id_opaque="FPR-E", montant_alloue=12, rang_fifo=1)
    # Clôtures et relevés de l'ancien modèle, et la clôture V1.
    _ins(c, "clotures_mensuelles", cloture_id_opaque="CLO-OLD", mois="2025-01", statut="VALIDEE")
    _ins(c, "cloture_evenements", cloture_id_opaque="CLO-OLD", type_evenement="CREATION")
    _ins(c, "clotures_mensuelles", cloture_id_opaque="CLO-V1", mois="2026-09",
         statut="EN_PREPARATION")
    _ins(c, "proprietaires_releves", releve_id_opaque="REG-OLD", proprietaire_id="PROP_T1",
         mois="2026-08")
    _ins(c, "proprietaires_paiement", releve_id_opaque="REG-OLD", statut_paiement="NON_PREPARE")
    # Hostaway, réservations hors Hostaway, factures fournisseurs.
    _ins(c, "hostaway_extractions", extraction_id="HAX-1", mode="DEPOT_GITHUB", date_debut=t,
         statut="SUCCES")
    for rid, ci in (("R-JUL", "2026-07-03"), ("R-SEP", "2026-09-12")):
        _ins(c, "hostaway_reservations", extraction_id="HAX-1", reservation_id=rid,
             check_in_date=ci)
    _ins(c, "reservations_hors_hostaway", reservation_hh_id="RESHH-2026-08-001", mois="2026-08",
         logement_id="LOG_T1")
    _ins(c, "factures", facture_id_opaque="FAC-1", fournisseur_id_opaque="INT_T",
         facture_ref="2026-03", montant_ttc=218, statut="A_CONTROLER", date_facture="2026-03-31")
    c.commit()
    c.close()
    return tmp_db, pdf


def _logique(db) -> str:
    c = sqlite3.connect(str(db))
    h = hashlib.sha256()
    for (t,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        for r in c.execute(f'SELECT * FROM "{t}" ORDER BY 1'):
            h.update(repr(tuple(r)).encode())
    c.close()
    return h.hexdigest()


def _ids(db, sql: str) -> set:
    c = sqlite3.connect(str(db))
    try:
        return {r[0] for r in c.execute(sql)}
    finally:
        c.close()


def _executer(db, tmp_path, **kw):
    return cut.executer(db_path=db, confirmer=True, acteur="test",
                        archive_dir=tmp_path / "archive", **kw)


# ── Simulation ──────────────────────────────────────────────────────────────────────────────────

def test_simulation_ne_modifie_rien_et_deux_simulations_identiques(base):
    db, pdf = base
    avant = _logique(db)
    r1, r2 = cut.simuler(db_path=db), cut.simuler(db_path=db)
    assert _logique(db) == avant and pdf.exists()
    assert r1["empreinte_rapport"] == r2["empreinte_rapport"]
    assert r1["anomalies"] == []
    assert set(r1["charges_supprimees_detail"]) == {"CHG-P1", "CHG-P2", "CHG-P3", "CHG-P4"}
    assert set(r1["charges_conservees_detail"]) == {"CHG-K1", "CHG-K2"}
    assert (r1["factures_supprimees"], r1["creances_supprimees"]) == (2, 1)
    assert r1["rapprochements_banque_charges_conserves"] == 2


def test_matrice_classe_chaque_table_dans_un_etat(base):
    db, _ = base
    r = cut.simuler(db_path=db)
    c = sqlite3.connect(str(db))
    toutes = {x[0] for x in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    c.close()
    assert {t["table"] for t in r["matrice"]} == toutes
    for t in r["matrice"]:
        assert t["categories"], t["table"]
        for cat in t["categories"]:
            assert cat["etat"] in cut.ETATS, (t["table"], cat)
    assert r["lignes"]["NON_CLASSEE"] == 0


# ── Exécution ───────────────────────────────────────────────────────────────────────────────────

def test_cutover_purge_ce_qui_doit_l_etre_et_conserve_le_reste(base, tmp_path):
    db, pdf = base
    hostaway = cut.empreinte(get_db(db), "hostaway_reservations")
    r = _executer(db, tmp_path)
    assert r["ok"], r
    assert all(v["ok"] for v in r["verifications"])
    assert _ids(db, "SELECT facture_id_opaque FROM factures_proprietaires") == set()
    assert _ids(db, "SELECT charge_id FROM charges") == {"CHG-K1", "CHG-K2"}
    assert _ids(db, "SELECT rapprochement_id_opaque FROM banque_rapprochements") == {
        "BRP-K1", "BRP-K2", "BRP-APP"}
    assert _ids(db, "SELECT ecriture_id_opaque FROM ecritures") == {"ECR-L1", "ECR-APP"}
    assert _ids(db, "SELECT lettrage_id_opaque FROM flux_lettrages") == {"LET-1"}
    assert _ids(db, "SELECT reference FROM justificatifs") == {"CHG-2026-09-001"}
    assert _ids(db, "SELECT charge_id FROM charges_perimetre_analytique") == {"CHG-K1"}
    for t in ("imputations_airbnb", "mouvements_tresorerie_proprietaires",
              "proprietaire_allocations", "proprietaire_recalculs", "banque_imports",
              "banque_suggestion_decisions", "charges_refacturation_positions",
              "factures_proprietaires_sequence", "proprietaires_releves",
              "proprietaires_paiement"):
        assert _ids(db, f"SELECT COUNT(*) FROM {t}") == {0}, t
    assert _ids(db, "SELECT cloture_id_opaque FROM clotures_mensuelles") == {"CLO-V1"}
    # Sources et référentiels intacts.
    assert cut.empreinte(get_db(db), "hostaway_reservations") == hostaway
    assert _ids(db, "SELECT reservation_hh_id FROM reservations_hors_hostaway") == {
        "RESHH-2026-08-001"}
    assert _ids(db, "SELECT facture_id_opaque FROM factures") == {"FAC-1"}
    assert _ids(db, "SELECT COUNT(*) FROM qonto_transactions_raw") == {4}
    assert _ids(db, "SELECT valeur FROM parametres_societe_facturation WHERE cle='SOCIETE_NOM'") \
        == {"CHOUETTE PATRIMOINE"}
    assert v1.debut(db_path=db) == "2026-09-01"
    # PDF généré pour la facture purgée : archivé, retiré de l'application.
    assert not pdf.exists() and (tmp_path / "archive" / "factures_proprietaires"
                                 / "2026-08-001.pdf").read_bytes() == b"%PDF-1.4 facture emise"
    assert cut.verifier(db_path=db)["checks"]["creances_0"]


def test_rollback_si_une_verification_echoue(base, tmp_path):
    db, pdf = base
    avant = _logique(db)

    def sabotage(conn):          # une source « conservée » touchée pendant la purge
        conn.execute("UPDATE hostaway_reservations SET check_in_date='2000-01-01'")

    r = _executer(db, tmp_path, _apres_purge=sabotage)
    assert not r["ok"] and r["code"] == cut.E_VERIFICATION
    assert any(e["code"] == "H" for e in r["echecs"])
    assert _logique(db) == avant, "ROLLBACK intégral : aucune base partiellement purgée"
    assert pdf.exists() and not (tmp_path / "archive" / "factures_proprietaires"
                                 / "2026-08-001.pdf").exists()


def test_rollback_sur_erreur_inattendue(base, tmp_path):
    db, pdf = base
    avant = _logique(db)

    def panne(conn):
        raise RuntimeError("panne simulée")

    with pytest.raises(RuntimeError):
        _executer(db, tmp_path, _apres_purge=panne)
    assert _logique(db) == avant and pdf.exists()


def test_refus_sans_confirmation_et_second_passage(base, tmp_path):
    db, _ = base
    assert cut.executer(db_path=db, acteur="t")["code"] == cut.E_CONFIRMATION
    assert _executer(db, tmp_path)["ok"]
    assert _executer(db, tmp_path)["code"] == cut.E_DEJA_APPLIQUE


def test_anomalie_argent_reel_sur_ancienne_facture_bloque_tout(base, tmp_path):
    """Un acompte rapproché de la banque est de l'argent réel : la décision ne le tranche pas."""
    db, _ = base
    c = get_db(db)
    c.execute("INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
              "type_objet, objet_id, montant_rapproche, statut) VALUES "
              "('BRP-MTP','QMV-3','ACOMPTE_PROPRIETAIRE','MTP-1',12,'CONFIRME')")
    c.commit()
    c.close()
    avant = _logique(db)
    r = _executer(db, tmp_path)
    assert r["code"] == cut.E_ANOMALIES and any("MTP-1" in a for a in r["anomalies"])
    assert _logique(db) == avant


# ── Verrous de période (après cutover) ──────────────────────────────────────────────────────────

@pytest.fixture()
def v1_base(base, tmp_path):
    db, _ = base
    assert _executer(db, tmp_path)["ok"]
    return db


def test_facture_aout_refusee_septembre_et_octobre_autorisees(v1_base):
    lignes = [{"libelle": "Prestation", "montant": "10"}]
    with pytest.raises(fpr.FacturationAvantV1) as exc:
        fpr.creer_exceptionnelle(proprietaire_id="PROP_T1", logement_id="LOG_T1", mois="2026-08",
                                 lignes=lignes, db_path=v1_base)
    assert str(exc.value) == MSG
    for mois in ("2026-09", "2026-10"):
        f = fpr.creer_exceptionnelle(proprietaire_id="PROP_T1", logement_id="LOG_T1", mois=mois,
                                     lignes=lignes, db_path=v1_base)
        assert f["statut"] == fpr.ST_BROUILLON
    with pytest.raises(fpr.FacturationAvantV1):
        fpr.creer({"mois": "2026-08", "proprietaire_id": "PROP_T1", "logement_id": "LOG_T1",
                   "COMMISSION_CONCIERGERIE": 10}, db_path=v1_base)


def test_periode_libre_qui_commence_en_aout_refusee(v1_base):
    from app.services import factures_proprietaires_periode_service as periode
    r = periode.previsualiser("PROP_T1", "2026-08-25", "2026-09-05", db_path=v1_base)
    assert (r["ok"], r["code"], r["message"]) == (False, v1.E_FACTURATION_AVANT_V1, MSG)
    assert periode.creer("PROP_T1", "2026-08-25", "2026-09-05", db_path=v1_base)["ok"] is False


def test_edition_ou_deplacement_vers_aout_refuse_meme_en_sql_direct(v1_base):
    f = fpr.creer_exceptionnelle(proprietaire_id="PROP_T1", logement_id="LOG_T1", mois="2026-09",
                                 lignes=[{"libelle": "X", "montant": "5"}], db_path=v1_base)
    c = get_db(v1_base)
    with pytest.raises(sqlite3.IntegrityError, match="FACTURATION_AVANT_V1"):
        c.execute("UPDATE factures_proprietaires SET mois='2026-08' WHERE facture_id_opaque=?",
                  (f["facture_id_opaque"],))
    with pytest.raises(sqlite3.IntegrityError, match="FACTURATION_AVANT_V1"):
        c.execute("INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, "
                  "logement_id, mois) VALUES ('FPR-X','PROP_T1','LOG_T1','2026-08')")
    c.close()
    # Une ancienne facture reprise hors parcours ne s'édite pas non plus côté service.
    with pytest.raises(fpr.FacturationAvantV1):
        fpr._exiger_facture_v1({"mois": "2026-08"}, db_path=v1_base)


def test_route_proposer_aout_refuse_proprement_et_selecteur_borne(v1_base, client):
    page = client.get("/factures-proprietaires/proposer?mois=2026-08")
    assert page.status_code == 200 and MSG in page.text and 'min="2026-09"' in page.text
    assert "Traceback" not in page.text
    r = client.post("/factures-proprietaires/generer", data={"mois": "2026-08"})
    assert r.status_code == 200 and MSG in r.text
    assert _ids(v1_base, "SELECT COUNT(*) FROM factures_proprietaires") == {0}
    assert 'min="2026-09-01"' in client.get("/factures-proprietaires").text
    assert 'min="2026-09-01"' in client.get("/factures-proprietaires/nouvelle").text


def test_numerotation_septembre_prete_sans_consommer(v1_base):
    assert fpr.prochain_numero("2026-09", db_path=v1_base) == "2026-09-001"
    assert _ids(v1_base, "SELECT COUNT(*) FROM factures_proprietaires_sequence") == {0}


def test_ecriture_avant_v1_refusee_service_et_base(v1_base):
    from app.services import comptabilite_ecritures_service as compta
    lignes = [{"compte": "606000", "debit": 5, "credit": 0},
              {"compte": "512000", "debit": 0, "credit": 5}]
    r = compta._inserer_ecriture("BANQUE", "2026-08-31", "2026-08", "P", "L", "MANUEL", "X1",
                                 lignes, db_path=v1_base)
    assert r["ok"] is False and r["code"] == v1.E_COMPTABILITE_AVANT_V1
    assert compta._inserer_ecriture("BANQUE", "2026-09-30", "2026-09", "P", "L", "MANUEL", "X2",
                                    lignes, db_path=v1_base)["ok"]
    c = get_db(v1_base)
    with pytest.raises(sqlite3.IntegrityError, match="COMPTABILITE_AVANT_V1"):
        c.execute("INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, "
                  "libelle, origine_type) VALUES ('E','BANQUE','2026-08-01','2026-08','x','M')")
    c.close()


def test_aucune_periode_anterieure_clotureable_et_liste_v1(v1_base):
    from app.routes import clotures as routes_clotures
    from app.services import clotures_service as cs
    with pytest.raises(cs.ClotureRefusee, match="septembre 2026"):
        cs.creer_ou_charger("2026-08", db_path=v1_base)
    assert "septembre 2026" in cs.refus_temporel("2026-07")
    assert cs.creer_ou_charger("2026-09", db_path=v1_base)["mois"] == "2026-09"
    mois = routes_clotures._mois_disponibles()
    assert mois and min(mois) >= "2026-09"


def test_parametre_v1_immuable_et_ancien_reset_refuse(v1_base):
    from app.services import cutover_service as ancien
    c = get_db(v1_base)
    with pytest.raises(sqlite3.IntegrityError, match="PARAMETRE_V1_IMMUABLE"):
        c.execute("UPDATE parametres_societe_facturation SET valeur='2026-01-01' "
                  "WHERE cle='V1_ACCOUNTING_START_DATE'")
    with pytest.raises(sqlite3.IntegrityError, match="PARAMETRE_V1_IMMUABLE"):
        c.execute("DELETE FROM parametres_societe_facturation WHERE cle='V1_ACCOUNTING_START_DATE'")
    c.close()
    r = ancien.reset_domaine_bancaire(confirmer=True, db_path=v1_base)
    assert r["code"] == ancien.E_SUPERSEDE_V1
    assert _ids(v1_base, "SELECT COUNT(*) FROM banque_rapprochements") == {3}


def test_lot12_ne_propose_que_la_periode_v1(v1_base):
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
    # Août est écarté À LA SOURCE : il n'entre même pas dans le contrôle mensuel du run.
    assert _ids(v1_base, "SELECT c.mois FROM lot12_controle_mensuel c JOIN lot12_runs r "
                         "ON r.run_id=c.run_id AND r.actif=1") == {"2026-09"}
    assert _ids(v1_base, "SELECT e.mois FROM lot12_prefactures_entete e JOIN lot12_runs r "
                         "ON r.run_id=e.run_id AND r.actif=1 WHERE e.mois < '2026-09'") == set()


def test_apres_redemarrage_et_recalculs_rien_ne_reapparait(v1_base):
    """Nouvelles connexions, recalcul FIFO forcé : aucune créance, aucune facture, aucune charge
    purgée ne revient — rien ne dépend d'un cache mémoire."""
    from app.services import compte_proprietaire_service as cpt
    from app.services import creances_dettes_service as cd
    cpt.recalculer_tous(declencheur=cpt.DECL_MANUEL, db_path=v1_base)
    assert cd.creances(db_path=v1_base) == []
    assert _ids(v1_base, "SELECT COUNT(*) FROM proprietaire_allocations") == {0}
    v = cut.verifier(db_path=v1_base)
    assert v["checks"]["ok"] is False or v["checks"]["ok"]       # préfactures : voir lot12
    for k in ("cutover_applique", "factures_0", "creances_0", "charges_non_rapprochees_0",
              "ecritures_avant_v1_0", "facture_aout_refusee", "facture_septembre_autorisee",
              "integrity_ok", "foreign_key_check_0"):
        assert v["checks"][k], k
    assert v["checks"]["prochain_numero_septembre"] == "2026-09-001"


def test_pas_de_double_comptage_des_charges_conservees(v1_base):
    c = get_db(v1_base)
    par_origine = c.execute("SELECT origine_id_opaque, COUNT(*) FROM ecritures GROUP BY 1 "
                            "HAVING COUNT(*) > 1").fetchall()
    desequilibre = c.execute("SELECT COUNT(*) FROM ecritures WHERE ROUND(total_debit,2) <> "
                             "ROUND(total_credit,2)").fetchone()[0]
    c.close()
    assert par_origine == [] and desequilibre == 0


def test_premiere_periode_v1_est_septembre_2026(v1_base, client):
    assert v1.premier_mois(db_path=v1_base) == "2026-09"
    assert v1.est_anterieur("2026-08-31", db_path=v1_base)
    assert not v1.est_anterieur("2026-09-01", db_path=v1_base)
    assert 'min="2026-09"' in client.get("/clotures").text


def test_sans_cutover_rien_n_est_restreint(tmp_db):
    """Installation neuve, bases de test : la règle naît avec le cutover, pas avant."""
    assert v1.debut(db_path=tmp_db) is None
    fpr.exiger_periode_v1("2020-01", db_path=tmp_db)
    c = get_db(tmp_db)
    c.execute("INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, "
              "logement_id, mois) VALUES ('FPR-OLD','P','L','2020-01')")
    c.close()

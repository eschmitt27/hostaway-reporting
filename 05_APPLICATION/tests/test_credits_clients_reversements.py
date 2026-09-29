"""Mission 37 — crédits clients : origine comptable des reversements Airbnb, acomptes, pièces.

Un reversement Airbnb (D032) est un versement d'Airbnb reçu pour le compte d'un propriétaire.
Il devient un CRÉDIT (419100, tiers = propriétaire) seulement quand son origine est constatée :
virement réel rapproché dans Flux (512 / 419100) ou origine justifiée sur un compte source nommé.
Il s'impute ensuite facture par facture (419100 → 411000). Sans origine : refus propre.

Données FICTIVES uniquement (base temporaire).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_mappings_service as maps
from app.services import credits_clients_service as credits
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from app.services import justificatifs_service as justif
from tests.test_circuit_banque_charges_compta import (_acompte_encaisse, _charge_depuis, _facture,
                                                      _lignes, _referentiel_saisie,  # noqa: F401
                                                      factures_ok)  # noqa: F401 — fixtures
from tests.test_flux_financiers import (ACTEUR, PROPRIO, _compter, _importer, _mvt, _par_montant,
                                        base, verrous)  # noqa: F401 — fixtures
from tests.test_lecture_seule_flux import _ecarts, _empreintes


def _credit_banque(db, montant, date="2026-09-10", reference="Payout Airbnb", pid=PROPRIO):
    res = credits.creer_reversement_airbnb(pid, montant, date, reference=reference,
                                           acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["credit_id_opaque"]


def _virement_airbnb(db, montant, date="2026-09-12"):
    _importer(db, [_mvt(montant, sens="credit", contrepartie="AIRBNB PAYMENTS", date=date,
                        libelle="AIRBNB PAYOUT")])
    return _par_montant(db, montant)


def _solde_411(db, pid=PROPRIO):
    conn = get_db(db)
    try:
        return round(conn.execute("SELECT COALESCE(SUM(debit-credit),0) FROM ecriture_lignes "
                                  "WHERE compte='411000' AND auxiliaire=?", (pid,)).fetchone()[0], 2)
    finally:
        conn.close()


# ══ Origine d'un reversement Airbnb ══════════════════════════════════════════════════════════════

def test_01_origine_bancaire_rapprochee_dans_flux(base, verrous):
    cid = _credit_banque(base, 150.0)
    c = credits.charger(cid, db_path=base)
    assert c["statut"] == credits.ST_EN_ATTENTE and c["reste"] == 150.0
    # Sans origine : pas d'imputation, et aucune écriture n'est passée.
    avant = _compter(base, "ecritures")
    assert _compter(base, "ecritures") == avant
    # Proposé au rapprochement comme objet d'encaissement, jamais comme réservation.
    obj = next(o for o in flux.objets(db_path=base) if o["id"] == cid)
    assert obj["type"] == flux.CREDIT_CLIENT and obj["sens"] == flux.ENTREE and obj["reste"] == 150.0
    m = _virement_airbnb(base, 150.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"{flux.CREDIT_CLIENT}:{cid}"], acteur=ACTEUR,
                           db_path=base)
    assert res["ok"], res
    assert _lignes(base, res["ecritures"][0]) == [("419100", 0.0, 150.0, PROPRIO),
                                                  ("512000", 150.0, 0.0, None)]
    c = credits.charger(cid, db_path=base)
    assert c["statut"] == credits.ST_DISPONIBLE and c["mouvement_origine"] == m["id"]
    assert c["ecriture_origine"] == res["ecritures"][0]
    assert all(o["id"] != cid for o in flux.objets(db_path=base)), "plus rien à rapprocher"
    conn = get_db(base)
    try:
        assert conn.execute("SELECT type_objet FROM banque_rapprochements WHERE objet_id=?",
                            (cid,)).fetchone()[0] == "PAYOUT_PLATEFORME"
        assert conn.execute("SELECT COUNT(*) FROM banque_rapprochements WHERE type_objet='RESERVATION'"
                            ).fetchone()[0] == 0
    finally:
        conn.close()


def test_02_un_virement_airbnb_pour_deux_proprietaires(base, verrous):
    from tests.fixtures_referentiel import semer
    semer(base, proprietaires=[{"proprietaire_id": "PROP_T2", "nom_proprietaire": "Second",
                                "prenom_proprietaire": "Paul", "actif": "OUI"}])
    a = _credit_banque(base, 70.0)
    b = _credit_banque(base, 30.0, pid="PROP_T2")
    m = _virement_airbnb(base, 100.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{a}", f"CREDIT_CLIENT:{b}"],
                           acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    assert _lignes(base, res["ecritures"][0]) == [("419100", 0.0, 30.0, "PROP_T2"),
                                                  ("419100", 0.0, 70.0, PROPRIO),
                                                  ("512000", 100.0, 0.0, None)]
    assert {credits.charger(x, db_path=base)["statut"] for x in (a, b)} == {credits.ST_DISPONIBLE}


def test_03_origine_partielle_refusee(base, verrous):
    cid = _credit_banque(base, 150.0)
    m = _virement_airbnb(base, 100.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR,
                           traitement_ecart=lettrage.SOLDE_OUVERT, db_path=base)
    assert not res["ok"] and "montant entier" in res["message"]
    assert credits.charger(cid, db_path=base)["statut"] == credits.ST_EN_ATTENTE


def test_04_origine_justifiee_sur_compte_source(base, verrous):
    refus = [
        credits.creer_reversement_airbnb(PROPRIO, 50, "2026-09-01", mode="JUSTIFIE",
                                         compte_source="455100", acteur=ACTEUR, db_path=base),
        credits.creer_reversement_airbnb(PROPRIO, 50, "2026-09-01", mode="JUSTIFIE",
                                         compte_source="512000", justification="x", acteur=ACTEUR,
                                         db_path=base),
        credits.creer_reversement_airbnb(PROPRIO, 50, "2026-09-01", mode="JUSTIFIE",
                                         compte_source="419100", justification="x", acteur=ACTEUR,
                                         db_path=base),
        credits.creer_reversement_airbnb("PROP_INCONNU", 50, "2026-09-01", acteur=ACTEUR,
                                         db_path=base),
        credits.creer_reversement_airbnb(PROPRIO, -5, "2026-09-01", acteur=ACTEUR, db_path=base)]
    assert [r["code"] for r in refus] == [credits.E_JUSTIFICATION, credits.E_COMPTE_SOURCE,
                                          credits.E_COMPTE_SOURCE, credits.E_PROPRIETAIRE,
                                          credits.E_MONTANT]
    ok = credits.creer_reversement_airbnb(
        PROPRIO, 50, "2026-07-31", mode="JUSTIFIE", compte_source="455100",
        auxiliaire_source="ASSOC_TEST", reference="Payout juillet",
        justification="Versement encaissé avant l'historique bancaire importé", acteur=ACTEUR,
        db_path=base)
    c = credits.charger(ok["credit_id_opaque"], db_path=base)
    assert c["statut"] == credits.ST_DISPONIBLE
    e = compta.charger(c["ecriture_origine"], db_path=base)
    assert e["statut"] == compta.ST_VALIDEE and e["journal"] == "ODIVERSES"
    assert _lignes(base, c["ecriture_origine"]) == [("419100", 0.0, 50.0, PROPRIO),
                                                    ("455100", 50.0, 0.0, "ASSOC_TEST")]


# ══ Application à des factures ═══════════════════════════════════════════════════════════════════

def test_05_credit_impute_sur_deux_factures_jusqu_a_extinction(base, verrous, factures_ok):
    cid = _credit_banque(base, 150.0)
    assert credits.imputer(cid, "X", 10, acteur=ACTEUR, db_path=base)["code"] == credits.E_SANS_ORIGINE
    m = _virement_airbnb(base, 150.0)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR, db_path=base)
    f1, e1 = _facture(base, {"gestion": 100.0}, mois="2026-07")
    f2, e2 = _facture(base, {"gestion": 80.0}, mois="2026-08")
    for e in (e1, e2):
        compta.comptabiliser_facture_emise(e, acteur=ACTEUR, db_path=base)
    assert _solde_411(base) == 180.0

    r1 = credits.imputer(cid, f1, 100.0, acteur=ACTEUR, db_path=base)
    assert r1["ok"] and r1["reste"] == 50.0
    assert _lignes(base, r1["ecriture"]["ecriture_id_opaque"]) == [
        ("411000", 0.0, 100.0, PROPRIO), ("419100", 100.0, 0.0, PROPRIO)]
    assert credits.charger(cid, db_path=base)["reste"] == 50.0
    # Plafonds : le reste du crédit, le solde de la facture.
    assert credits.imputer(cid, f2, 60.0, acteur=ACTEUR, db_path=base)["code"] == credits.E_RESTE
    assert credits.imputer(cid, f1, 1.0, acteur=ACTEUR, db_path=base)["code"] == \
        credits.E_SOLDE_FACTURE
    r2 = credits.imputer(cid, f2, 50.0, acteur=ACTEUR, db_path=base)
    assert r2["ok"] and r2["reste"] == 0.0
    c = credits.charger(cid, db_path=base)
    assert c["utilise"] == 150.0 and c["reste"] == 0.0
    assert sorted(i["document_id"] for i in c["imputations"]) == sorted([f1, f2])
    assert _solde_411(base) == 30.0, "180 facturés − 150 de reversement"
    from app.services import factures_proprietaires_service as fpr
    assert fpr.solde(f1, db_path=base)["solde"] == 0.0 and fpr.solde(f2, db_path=base)["solde"] == 30.0


def test_06_imputation_sur_brouillon_constatee_a_l_emission(base, verrous, factures_ok):
    from app.services import factures_proprietaires_service as fpr
    from tests.test_creances_regle_compense import DESTINATAIRE, EMETTEUR
    cid = credits.creer_reversement_airbnb(
        PROPRIO, 40, "2026-08-31", mode="JUSTIFIE", compte_source="455100",
        auxiliaire_source="ASSOC_TEST", justification="test", acteur=ACTEUR,
        db_path=base)["credit_id_opaque"]
    fid = fpr.creer({"mois": "2026-08", "proprietaire_id": PROPRIO, "logement_id": "LOG_T",
                     "source_calcul": "PREF-B", "COMMISSION_CONCIERGERIE": 90.0,
                     "montant_du_conciergerie": 90.0}, acteur="t", db_path=base)["facture_id_opaque"]
    r = credits.imputer(cid, fid, 40, acteur=ACTEUR, db_path=base)
    assert r["ok"] and r["ecriture"] is None, "facture non émise : pas encore de créance 411"
    fpr.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=base)
    emise = fpr.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, date_facture="2026-08-31",
                        acteur="t", exiger_conformite=False, db_path=base)
    res = compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=base)
    assert res["imputation"]["ok"] and len(res["imputation"]["ecritures"]) == 1
    assert _solde_411(base) == 50.0
    # Rejouer ne double rien.
    again = compta.generer_ecriture_imputation_acomptes(emise, acteur=ACTEUR, db_path=base)
    assert again["ok"] and _solde_411(base) == 50.0


# ══ Données historiques sans origine : refus propre, puis régularisation ═════════════════════════

def test_07_reversement_historique_sans_origine_refuse_puis_regularise(base, verrous, factures_ok):
    fid, emise = _facture(base, {"gestion": 200.0, "reversements": [60.0]})
    avant = _compter(base, "ecritures")
    res = compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=base)
    assert not res["imputation"]["ok"] and res["imputation"]["code"] == compta.E_ORIGINE_ACOMPTE
    assert res["imputation"]["sans_origine"][0]["type"] == "REVERSEMENT_AIRBNB"
    assert _compter(base, "ecritures") == avant + 1, "seule la vente est écrite, rien d'inventé"
    a_reg = credits.a_regulariser(proprietaire_id=PROPRIO, db_path=base)
    assert len(a_reg) == 1 and a_reg[0]["montant_impute"] == 60.0
    # Régularisation : rattachement au reversement réellement reçu (origine constatée).
    cid = _credit_banque(base, 60.0)
    assert credits.regulariser(a_reg[0]["imputation_airbnb_id"], cid, acteur=ACTEUR,
                               db_path=base)["code"] == credits.E_SANS_ORIGINE
    m = _virement_airbnb(base, 60.0)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR, db_path=base)
    reg = credits.regulariser(a_reg[0]["imputation_airbnb_id"], cid, acteur=ACTEUR, db_path=base)
    assert reg["ok"] and reg["ecriture"]["ok"], reg
    assert credits.a_regulariser(proprietaire_id=PROPRIO, db_path=base) == []
    assert credits.charger(cid, db_path=base)["reste"] == 0.0
    assert _solde_411(base) == 140.0


def test_08_annuler_l_origine_d_un_credit_deja_utilise_refuse(base, verrous, factures_ok):
    cid = _credit_banque(base, 80.0)
    m = _virement_airbnb(base, 80.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR,
                           db_path=base)
    fid, emise = _facture(base, {"gestion": 100.0})
    credits.imputer(cid, fid, 30, acteur=ACTEUR, db_path=base)
    refus = lettrage.annuler(res["lettrage_id_opaque"], motif="erreur", acteur=ACTEUR, db_path=base)
    assert not refus["ok"] and "déjà été imputé" in refus["message"]
    assert credits.charger(cid, db_path=base)["statut"] == credits.ST_DISPONIBLE


def test_09_annuler_l_origine_d_un_credit_inutilise(base, verrous):
    cid = _credit_banque(base, 80.0)
    m = _virement_airbnb(base, 80.0)
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR,
                           db_path=base)
    assert lettrage.annuler(res["lettrage_id_opaque"], motif="erreur", acteur=ACTEUR,
                            db_path=base)["ok"]
    c = credits.charger(cid, db_path=base)
    assert c["statut"] == credits.ST_EN_ATTENTE and c["ecriture_origine"] is None
    assert [e["type_evenement"] for e in c["historique"]] == [
        "CREATION", "ORIGINE_CONSTATEE", "ORIGINE_RETIREE"]


# ══ Acomptes : FIFO sur deux factures, origine 419100 ════════════════════════════════════════════

def test_10_acompte_impute_par_fifo_sur_deux_factures(base, verrous, factures_ok):
    mid, enc = _acompte_encaisse(base, 100.0)
    assert credits.ecriture_origine_acompte(mid, db_path=base) == enc["ecritures"][0]
    f1, e1 = _facture(base, {"gestion": 60.0}, mois="2026-07")
    r1 = compta.comptabiliser_facture_emise(e1, acteur=ACTEUR, db_path=base)
    assert r1["imputation"]["ok"] and len(r1["imputation"]["ecritures"]) == 1
    f2, e2 = _facture(base, {"gestion": 80.0}, mois="2026-08")
    r2 = compta.comptabiliser_facture_emise(e2, acteur=ACTEUR, db_path=base)
    assert r2["imputation"]["ok"]
    assert _lignes(base, r2["imputation"]["ecritures"][0]) == [
        ("411000", 0.0, 40.0, PROPRIO), ("419100", 40.0, 0.0, PROPRIO)]
    vue = credits.vue(PROPRIO, db_path=base)
    acompte = next(a for a in vue["acomptes"] if a["mouvement_opaque"] == mid)
    assert acompte["utilise"] == 100.0 and acompte["reste"] == 0.0
    assert [x["montant"] for x in acompte["factures"]] == [60.0, 40.0]
    assert _solde_411(base) == 40.0


def test_11_acompte_de_l_ancien_ecran_qonto_regularise_sur_son_rapprochement(base, verrous):
    from app.services import proprietaires_tresorerie_service as tres
    m = _virement_airbnb(base, 25.0)
    cree = tres.creer(PROPRIO, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", 25.0,
                      "2026-09-12", source_type="QONTO", acteur=ACTEUR, db_path=base)
    tres.valider(cree["mouvement_opaque"], acteur=ACTEUR, db_path=base)
    sans = credits.comptabiliser_encaissement_acompte(cree["mouvement_opaque"], acteur=ACTEUR,
                                                      db_path=base)
    assert not sans["ok"] and sans["code"] == credits.E_ORIGINE_ACOMPTE
    conn = get_db(base)
    try:
        conn.execute("INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
                     "type_objet, objet_id, montant_rapproche, statut) VALUES "
                     "('BRP-QONTO-T', ?, 'REVERSEMENT_PROPRIETAIRE', ?, 25.0, 'CONFIRME')",
                     (m["id"], cree["mouvement_opaque"]))
        conn.commit()
    finally:
        conn.close()
    res = credits.comptabiliser_encaissement_acompte(cree["mouvement_opaque"], acteur=ACTEUR,
                                                     db_path=base)
    assert res["ok"] and _lignes(base, res["ecriture_id_opaque"]) == [
        ("419100", 0.0, 25.0, PROPRIO), ("512000", 25.0, 0.0, None)]
    assert credits.ecriture_origine_acompte(cree["mouvement_opaque"], db_path=base)
    assert credits.comptabiliser_encaissement_acompte(cree["mouvement_opaque"], acteur=ACTEUR,
                                                      db_path=base)["code"] == credits.E_DEJA


# ══ Écrans : lisibles, sans écriture ═════════════════════════════════════════════════════════════

def test_12_ecran_credits_lisible_et_sans_ecriture(client, base, verrous, factures_ok):
    cid = _credit_banque(base, 150.0, reference="Payout septembre")
    m = _virement_airbnb(base, 150.0)
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CREDIT_CLIENT:{cid}"], acteur=ACTEUR, db_path=base)
    fid, emise = _facture(base, {"gestion": 100.0})
    credits.imputer(cid, fid, 100.0, acteur=ACTEUR, db_path=base)
    avant = _empreintes(base)
    page = client.get(f"/comptes-proprietaires/{PROPRIO}/credits")
    fiche = client.get(f"/factures-proprietaires/{fid}")
    assert page.status_code == 200 and fiche.status_code == 200
    import html
    texte = html.unescape(page.text)
    assert "Crédit disponible : 50.00 €" in texte and "Reversements Airbnb" in texte
    assert "Payout septembre" in texte and "Encaissement bancaire rapproché" in texte
    assert "100.00 €" in texte and "50.00 €" in texte and emise["numero_facture"] in texte
    assert "Crédit d'origine constatée" in html.unescape(fiche.text)
    assert "CRD-" not in texte.split("<table")[0], "aucun identifiant technique en titre"
    assert _ecarts(avant, _empreintes(base)) == []


def test_13_parcours_http_declarer_et_imputer(client, base, verrous, factures_ok):
    r = client.post(f"/comptes-proprietaires/{PROPRIO}/credits",
                    data={"montant": "45", "date_origine": "2026-08-01", "mode": "JUSTIFIE",
                          "compte_source": "455100", "auxiliaire_source": "",
                          "justification": "Versement antérieur", "acteur": ACTEUR},
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"], "455 exige son associé"
    r = client.post(f"/comptes-proprietaires/{PROPRIO}/credits",
                    data={"montant": "45", "date_origine": "2026-08-01", "reference": "Payout",
                          "acteur": ACTEUR}, follow_redirects=False)
    assert r.status_code == 303 and "message" in r.headers["location"]
    cid = credits.lister(proprietaire_id=PROPRIO, db_path=base)[0]["credit_id_opaque"]
    fid, _ = _facture(base, {"gestion": 100.0})
    refus = client.post(f"/factures-proprietaires/{fid}/imputer-credit",
                        data={"credit_id": cid, "montant": "10", "acteur": ACTEUR})
    assert refus.status_code == 422 and "origine" in refus.text


# ══ Pièces : lien depuis la charge et l'écriture, stockage persistant ════════════════════════════

def test_14_piece_ouverte_depuis_la_charge_et_l_ecriture(client, base, verrous):
    _importer(base, [_mvt(12.0, contrepartie="FREE MOBILE", date="2026-09-21")])
    m = _par_montant(base, 12.0)
    ref = justif.prochaine_reference(justif.OBJET_CHARGE, "2026-09-21", db_path=base)
    d = justif.preparer_dossier(justif.OBJET_CHARGE, "2026-09-21")
    (d / f"{ref}__FREE.pdf").write_bytes(b"%PDF-1.4 piece test")
    cid = _charge_depuis(base, m, 12.0, categorie="CHG_039", reponse={"present": "OUI"})
    import html
    fiche = html.unescape(client.get(f"/fournisseurs/{cid}").text)
    assert f'href="/justificatifs/{ref}"' in fiche
    piece = client.get(f"/justificatifs/{ref}")
    assert piece.status_code == 200 and piece.content == b"%PDF-1.4 piece test"
    assert client.get("/justificatifs/CHG-2026-09-999").status_code == 404
    assert client.get("/justificatifs/..%2F..%2Fapp.db").status_code == 404
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    ecriture = html.unescape(client.get(f"/comptabilite/ecritures/{res['ecritures'][0]}").text)
    assert 'data-testid="pieces-source"' in ecriture and ref in ecriture
    assert f'href="/justificatifs/{ref}"' in ecriture and "aucune seconde pièce" in ecriture


def test_15_stockage_suit_les_donnees_et_survit_a_un_deplacement(base, verrous, tmp_path,
                                                                  monkeypatch):
    # Par défaut, les pièces vivent avec la base (DATA_DIR, donc APP_DATA_DIR), hors du code.
    env = {k: v for k, v in os.environ.items() if k != "JUSTIFICATIFS_ROOT"}
    env.update(APP_DATA_DIR=str(tmp_path / "donnees"), PILOTAGE_IGNORE_ENV_FILE="1")
    sortie = subprocess.run([sys.executable, "-c", "import app.config as c; "
                             "print(c.JUSTIFICATIFS_ROOT); print(c.DB_PATH.parent)"],
                            capture_output=True, text=True, env=env,
                            cwd=str(Path(cfg.APP_ROOT))).stdout.split("\n")
    assert Path(sortie[0]) == Path(sortie[1]) / "justificatifs" == tmp_path / "donnees" / "justificatifs"
    # La base n'enregistre que le dossier relatif : déplacer les données garde le lien.
    _importer(base, [_mvt(9.0, date="2026-09-05")])
    m = _par_montant(base, 9.0)
    ref = justif.prochaine_reference(justif.OBJET_CHARGE, "2026-09-05", db_path=base)
    d = justif.preparer_dossier(justif.OBJET_CHARGE, "2026-09-05")
    (d / f"{ref}__X.pdf").write_bytes(b"%PDF")
    cid = _charge_depuis(base, m, 9.0, categorie="CHG_010", reponse={"present": "OUI"})
    j = justif.charger(justif.OBJET_CHARGE, cid, db_path=base)
    assert j["dossier"] == "Charges/2026/09"
    import shutil
    nouvelle = tmp_path / "autre_emplacement"
    shutil.copytree(cfg.JUSTIFICATIFS_ROOT, nouvelle)
    monkeypatch.setattr(cfg, "JUSTIFICATIFS_ROOT", nouvelle)
    assert justif.fichier(ref, db_path=base) == (nouvelle / "Charges" / "2026" / "09" / f"{ref}__X.pdf").resolve()


# ══ Catégories ambiguës ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("categorie,comptes", [("CHG_019", {"615200", "615500"}),
                                               ("CHG_025", {"625700", "625100"})])
def test_16_categories_a_plusieurs_natures_au_choix(tmp_db, categorie, comptes):
    p = maps.comptes_proposes(categorie, db_path=tmp_db)
    assert p["statut"] == maps.PROPOSITION_CHOIX and p["compte_defaut"] == ""
    assert {c["compte"] for c in p["comptes"]} == comptes


@pytest.mark.parametrize("categorie", ["CHG_017", "CHG_026", "CHG_024", "CHG_020", "CHG_021",
                                       "CHG_012", "CHG_027"])
def test_17_categories_sans_mapping_automatique(tmp_db, categorie):
    assert maps.comptes_proposes(categorie, db_path=tmp_db)["statut"] == maps.PROPOSITION_A_DEFINIR

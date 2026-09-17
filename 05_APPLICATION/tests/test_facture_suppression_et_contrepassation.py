"""Cycle de vie d'une facture fournisseur : suppression et contrepassation — recette n°4, §5.

E2E D — une facture À CONTRÔLER dont le PDF a disparu du dossier se supprime tant que le mois est
ouvert, sans laisser de résidu.
E2E E — une facture VALIDÉE ne se supprime jamais : elle se contrepasse. Sa dette, son écriture
d'achat et ses impacts ménages sont annulés, la trace de l'origine est conservée, et l'opération
est idempotente.

Avant cette mission, aucun des deux gestes n'existait : ni service, ni route, ni SQL. Le seul geste
disponible était le passage au statut ANNULEE par le sélecteur générique de l'écran — sans motif,
sans contrepassation, et en laissant les lignes de ménage ACTIVE, donc toujours comptées par le
rapprochement.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "ECRITURE_OPERATIONNELLE_ENABLED", True, raising=False)
    # La comptabilité a ses propres verrous : sans eux, la validation ne génère aucune écriture
    # d'achat, et il n'y aurait rien à contrepasser.
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)


@pytest.fixture(autouse=True)
def _pas_de_recalcul(monkeypatch):
    """Le recalcul du mois est un moteur lourd, couvert par ses propres tests : on vérifie ICI
    qu'il est bien DEMANDÉ, pas ce qu'il calcule."""
    from app.services import orchestrateur_moteur

    appels = []
    monkeypatch.setattr(orchestrateur_moteur, "executer_menages_cible",
                        lambda **kw: appels.append(kw) or {"ok": True})
    return appels


def _facture(tmp_db, *, ref="SUP-001", statut=fact.ST_A_CONTROLER, montant=155.0):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-TEST", "facture_ref": ref, "date_facture": "2026-08-31",
         "montant_ttc": montant},
        db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A",
                      montant_ttc=montant, description="t3 310 muret",
                      libelle_source="2. service de nettoyage T3 310 muret x 1",
                      source=flm.SOURCE_PDF, db_path=tmp_db)
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO facture_pdf_diagnostics (nom_fichier, format_detecte, "
                     "statut_extraction, facture_id_opaque) VALUES (?,?,?,?)",
                     ("08-26-Aissata.pdf", "AISSATA", "OK", opaque))
        conn.execute("INSERT INTO menages_pdf_fichiers_hash (nom_fichier, sha256) VALUES (?,?)",
                     ("08-26-Aissata.pdf", "abc123"))
        conn.commit()
    finally:
        conn.close()
    if statut != fact.ST_A_CONTROLER:
        fact.changer_statut(opaque, statut, acteur="test", db_path=tmp_db)
    return opaque


def _compte(tmp_db, table, opaque):
    conn = get_db(tmp_db)
    try:
        return conn.execute(f"SELECT COUNT(*) n FROM {table} WHERE facture_id_opaque=?",
                            (opaque,)).fetchone()["n"]
    finally:
        conn.close()


# ── E2E D : suppression d'une facture À CONTRÔLER ───────────────────────────────────────────────

def test_supprimer_une_facture_a_controler_ne_laisse_aucun_residu(tmp_db, _pas_de_recalcul):
    opaque = _facture(tmp_db)
    assert _compte(tmp_db, "facture_lignes_menage", opaque) == 1

    r = fact.supprimer(opaque, motif="PDF retiré du dossier par l'utilisateur", acteur="ewan",
                       db_path=tmp_db)

    assert r["ok"] is True
    conn = get_db(tmp_db)
    try:
        assert conn.execute("SELECT COUNT(*) n FROM factures WHERE facture_id_opaque=?",
                            (opaque,)).fetchone()["n"] == 0
        for table in ("facture_lignes_menage", "facture_ventilations", "facture_classification",
                      "facture_pdf_diagnostics", "facture_evenements"):
            assert _compte(tmp_db, table, opaque) == 0, table
        # Le PDF redevient importable : son hash ne doit plus le classer « déjà traité ».
        assert conn.execute("SELECT COUNT(*) n FROM menages_pdf_fichiers_hash "
                            "WHERE nom_fichier='08-26-Aissata.pdf'").fetchone()["n"] == 0
    finally:
        conn.close()
    assert r["fichiers_liberes"] == ["08-26-Aissata.pdf"]
    # Le mois est recalculé : les impacts provisoires ne se défont pas par des DELETE à la main.
    assert _pas_de_recalcul and _pas_de_recalcul[0]["mois"] == "2026-08"


def test_supprimer_exige_un_motif(tmp_db):
    opaque = _facture(tmp_db)
    r = fact.supprimer(opaque, motif="   ", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == fact.E_MOTIF_OBLIGATOIRE
    assert _compte(tmp_db, "facture_lignes_menage", opaque) == 1


def test_supprimer_est_refuse_si_le_mois_est_cloture(tmp_db, monkeypatch):
    opaque = _facture(tmp_db)
    monkeypatch.setattr(fact, "_mois_est_cloture", lambda mois, db_path=None: True)
    r = fact.supprimer(opaque, motif="ménage de printemps", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == fact.E_MOIS_CLOTURE
    assert _compte(tmp_db, "facture_lignes_menage", opaque) == 1


def test_supprimer_une_facture_validee_est_interdit(tmp_db):
    opaque = _facture(tmp_db, ref="SUP-002", statut=fact.ST_VALIDEE)
    r = fact.supprimer(opaque, motif="erreur", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == fact.E_SUPPRESSION_INTERDITE
    assert "contrepasse" in r["message"]
    conn = get_db(tmp_db)
    try:
        assert conn.execute("SELECT COUNT(*) n FROM factures WHERE facture_id_opaque=?",
                            (opaque,)).fetchone()["n"] == 1
    finally:
        conn.close()


# ── E2E E : contrepassation d'une facture VALIDÉE ───────────────────────────────────────────────

def test_contrepasser_annule_ecriture_dette_et_impacts_en_conservant_la_trace(tmp_db,
                                                                             _pas_de_recalcul):
    opaque = _facture(tmp_db, ref="CTP-001", statut=fact.ST_VALIDEE)
    # La validation a généré l'écriture d'achat au statut PROPOSEE : on la valide pour qu'elle
    # pèse réellement sur le solde fournisseur, sinon il n'y a rien à contrepasser.
    ecritures = fact.consequences_constatees(opaque, db_path=tmp_db)["ecritures"]
    assert ecritures, "la validation doit avoir généré une écriture d'achat"
    compta.valider(ecritures[0]["ecriture_id_opaque"], acteur="test", db_path=tmp_db)
    solde_avant = compta.solde_compte(compta.COMPTE_FOURNISSEURS, db_path=tmp_db)
    assert solde_avant["credit"] > 0, "la dette fournisseur existe avant contrepassation"

    r = fact.contrepasser(opaque, motif="facture annulée par le prestataire", acteur="ewan",
                          db_path=tmp_db)

    assert r["ok"] is True and r["miroirs"], r
    conn = get_db(tmp_db)
    try:
        facture = dict(conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                                    (opaque,)).fetchone())
        lignes = [dict(x) for x in conn.execute(
            "SELECT statut_ligne, motif_correction FROM facture_lignes_menage "
            "WHERE facture_id_opaque=?", (opaque,))]
        evenements = [dict(x) for x in conn.execute(
            "SELECT type_evenement, commentaire FROM facture_evenements "
            "WHERE facture_id_opaque=? ORDER BY id", (opaque,))]
        origine = dict(conn.execute(
            "SELECT statut FROM ecritures WHERE ecriture_id_opaque=?",
            (ecritures[0]["ecriture_id_opaque"],)).fetchone())
        miroir = dict(conn.execute(
            "SELECT contrepasse_de, statut FROM ecritures WHERE ecriture_id_opaque=?",
            (r["miroirs"][0],)).fetchone())
    finally:
        conn.close()

    # La facture reste, son annulation se lit.
    assert facture["statut"] == fact.ST_ANNULEE
    assert origine["statut"] == compta.ST_CONTREPASSEE
    assert miroir["contrepasse_de"] == ecritures[0]["ecriture_id_opaque"]
    # La dette fournisseur est annulée : débit et crédit s'équilibrent.
    solde_apres = compta.solde_compte(compta.COMPTE_FOURNISSEURS, db_path=tmp_db)
    assert solde_apres["solde"] == pytest.approx(0.0)
    # Les lignes ne pèsent plus sur le rapprochement, sans avoir été supprimées.
    assert lignes and all(l["statut_ligne"] == flm.STATUT_LIGNE_EXTRACTION_INCORRECTE
                          for l in lignes)
    assert all("contrepassée" in (l["motif_correction"] or "") for l in lignes)
    # La relation origine <-> contrepassation est tracée.
    assert [e["type_evenement"] for e in evenements][-1] == "CONTREPASSATION"
    assert "facture annulée par le prestataire" in evenements[-1]["commentaire"]
    assert _pas_de_recalcul[-1]["mois"] == "2026-08"


def test_contrepasser_est_idempotent(tmp_db, _pas_de_recalcul):
    opaque = _facture(tmp_db, ref="CTP-002", statut=fact.ST_VALIDEE)
    premier = fact.contrepasser(opaque, motif="doublon prestataire", db_path=tmp_db)
    assert premier["ok"] is True

    second = fact.contrepasser(opaque, motif="doublon prestataire", db_path=tmp_db)
    assert second["ok"] is True and second["deja_contrepassee"] is True

    conn = get_db(tmp_db)
    try:
        n = conn.execute("SELECT COUNT(*) n FROM facture_evenements WHERE facture_id_opaque=? "
                         "AND type_evenement='CONTREPASSATION'", (opaque,)).fetchone()["n"]
    finally:
        conn.close()
    assert n == 1, "une reprise après incident ne doit pas empiler les contrepassations"


def test_contrepasser_exige_un_motif_et_refuse_un_mois_cloture(tmp_db, monkeypatch):
    opaque = _facture(tmp_db, ref="CTP-003", statut=fact.ST_VALIDEE)
    assert fact.contrepasser(opaque, motif="", db_path=tmp_db)["code"] == fact.E_MOTIF_OBLIGATOIRE

    monkeypatch.setattr(fact, "_mois_est_cloture", lambda mois, db_path=None: True)
    r = fact.contrepasser(opaque, motif="erreur de facturation", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == fact.E_MOIS_CLOTURE


def test_contrepasser_une_facture_a_controler_renvoie_vers_la_suppression(tmp_db):
    opaque = _facture(tmp_db, ref="CTP-004")
    r = fact.contrepasser(opaque, motif="rien à annuler", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == fact.E_RIEN_A_CONTREPASSER
    assert "supprime" in r["message"]


# ── Le parcours est réellement accessible depuis l'écran ────────────────────────────────────────

@pytest.fixture()
def client(tmp_db, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from app.services import orchestrateur_service as orch
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(orch, "marquer_runs_interrompus", lambda **k: [])
    from app.main import app as application
    with TestClient(application) as c:
        yield c


def test_l_ecran_offre_la_suppression_et_refuse_l_annulation_silencieuse(client, tmp_db):
    opaque = _facture(tmp_db, ref="UI-001")
    page = client.get(f"/factures/{opaque}")
    assert page.status_code == 200
    assert "Supprimer cette facture" in page.text
    # Le sélecteur générique ne doit plus permettre d'annuler une facture en un clic, sans motif.
    assert '<option value="ANNULEE"' not in page.text

    reponse = client.post(f"/factures/{opaque}/supprimer",
                          data={"motif": "PDF retiré du dossier"}, follow_redirects=False)
    assert reponse.status_code == 303
    assert reponse.headers["location"].startswith("/factures?message=")
    assert client.get(f"/factures/{opaque}").status_code == 404


def test_l_ecran_propose_la_contrepassation_sur_une_facture_engagee(client, tmp_db):
    opaque = _facture(tmp_db, ref="UI-002", statut=fact.ST_VALIDEE)
    page = client.get(f"/factures/{opaque}")
    assert "Annuler par contrepassation" in page.text
    assert "Supprimer cette facture" not in page.text, "une facture engagée ne se supprime pas"

    reponse = client.post(f"/factures/{opaque}/contrepasser",
                          data={"motif": "annulée par le prestataire"}, follow_redirects=False)
    assert reponse.status_code == 303
    conn = get_db(tmp_db)
    try:
        assert dict(conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                                 (opaque,)).fetchone())["statut"] == fact.ST_ANNULEE
    finally:
        conn.close()


def test_une_facture_sans_ecriture_postee_est_annulee_sans_miroir(tmp_db, _pas_de_recalcul):
    """L'écriture d'achat naît PROPOSEE : tant qu'elle n'est pas validée, rien n'a été posté."""
    opaque = _facture(tmp_db, ref="CTP-005", statut=fact.ST_VALIDEE)
    r = fact.contrepasser(opaque, motif="annulation avant comptabilisation", db_path=tmp_db)
    assert r["ok"] is True and r["miroirs"] == []
    conn = get_db(tmp_db)
    try:
        assert dict(conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                                 (opaque,)).fetchone())["statut"] == fact.ST_ANNULEE
    finally:
        conn.close()

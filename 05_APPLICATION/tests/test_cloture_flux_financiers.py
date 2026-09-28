"""Mission 32 — clôture mensuelle branchée sur Flux financiers.

Données FICTIVES (base temporaire), aucun appel réseau : les mouvements Qonto passent par le double
de client GET des tests Qonto. La date du jour est FIXÉE au 28/09/2026 : septembre 2026 est le mois
courant, juillet 2026 un mois terminé, octobre 2026 un mois futur.

Ce qui est vérifié : le calendrier (seul un mois terminé se clôture, par le serveur), les bloqueurs
lus en direct dans Flux (mouvement à qualifier, à comptabiliser, en erreur, caisse, écriture
proposée, compte à définir), ce qui ne bloque pas (factures à contrôler, hors comptabilité, sans
effet, autre mois), la levée de chaque bloqueur quand l'utilisateur le traite, l'autorité du serveur
(POST forcés), l'atomicité, l'historique, et qu'aucune qualification, aucun mapping, aucune
validation d'écriture ou de facture n'est jamais décidé par la clôture.
"""
from __future__ import annotations

import hashlib
import html
from datetime import date
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import cloture_flux_service as cf
from app.services import clotures_service as cs
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_mappings_service as maps
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from tests.test_flux_financiers import (ACTEUR, _charge, _fournisseur, _importer, _mvt,  # noqa: F401
                                        _par_montant, base, verrous)

PASSE = "2026-07"
COURANT = "2026-09"
FUTUR = "2026-10"


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 9, 28))


def _analyse(db, mois=PASSE):
    return cf.analyser(mois, db_path=db)


def _types(db, mois=PASSE):
    return [b["type"] for b in _analyse(db, mois)["bloquants"]]


def _a_valider(db, mois=PASSE):
    c = cs.creer_ou_charger(mois, acteur=ACTEUR, db_path=db)
    c = cs.demarrer_preparation(c, acteur=ACTEUR, db_path=db)
    return cs.passer_a_valider(c, acteur=ACTEUR, db_path=db)


def _empreinte(db, *tables):
    conn = get_db(db)
    try:
        h = hashlib.sha256()
        for t in tables:
            for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1"):
                h.update(repr(tuple(r)).encode())
        return h.hexdigest()
    finally:
        conn.close()


def _ecriture_proposee(db, mois=PASSE, montant=120.0, piece="VT-TEST-1"):
    res = compta._inserer_ecriture(
        "VENTES", f"{mois}-15", mois, piece, "Vente test", "FACTURE_PROPRIETAIRE", f"FPR-{piece}",
        [{"compte": "411000", "debit": montant, "credit": 0, "auxiliaire": "PROP_TFLUX"},
         {"compte": "706000", "debit": 0, "credit": montant}], db_path=db)
    assert res["ok"], res
    return res["ecriture_id_opaque"]


def _facture(db, statut="A_CONTROLER", mois=PASSE, montant=300.0):
    fournisseur = _fournisseur(db, f"Fournisseur Fictif {statut.title()}")
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                     "date_facture, montant_ttc, statut, source) VALUES (?,?,?,?,?,?,?)",
                     (f"FAC-TEST-{statut}", fournisseur, "FA-TEST", f"{mois}-12", montant, statut,
                      "SAISIE"))
        conn.commit()
    finally:
        conn.close()


TABLES_METIER = ("qonto_transactions_raw", "qonto_transactions_statut_local", "banque_rapprochements",
                 "flux_lettrages", "ecritures", "ecriture_lignes", "charges", "factures",
                 "mapping_comptable_regles", "plan_comptable", "operations_caisse")


# ══ 1-3. Le calendrier ════════════════════════════════════════════════════════════════════════

def test_01_mois_courant_non_cloturable_meme_sans_bloqueur(base):
    c = _a_valider(base, COURANT)
    assert cs.calcul_progression(COURANT, base)["nb_bloqueurs"] == 0
    with pytest.raises(cs.ClotureRefusee) as exc:
        cs.valider(c, acteur=ACTEUR, commentaire="tentative", db_path=base)
    assert str(exc.value) == "Le mois de septembre 2026 est encore en cours et ne peut pas être clôturé."
    assert cs.charger_par_mois(COURANT, base)["statut"] == cs.ST_A_VALIDER
    prog = cs.calcul_progression(COURANT, base)
    assert prog["etat_libelle"] == "Prêt techniquement — mois en cours"
    assert prog["pret_a_cloturer"] and not prog["cloture_autorisee"]


def test_02_mois_futur_non_cloturable(base):
    c = _a_valider(base, FUTUR)
    with pytest.raises(cs.ClotureRefusee, match="mois futur ne peut jamais être clôturé"):
        cs.valider(c, acteur=ACTEUR, commentaire="tentative", db_path=base)
    assert cs.calcul_progression(FUTUR, base)["etat_libelle"] == "Mois futur — non clôturable"


def test_03_mois_passe_sans_bloqueur_cloturable(base):
    c = _a_valider(base, PASSE)
    prog = cs.calcul_progression(PASSE, base)
    assert prog["etat_libelle"] == "Prêt à clôturer" and prog["cloture_autorisee"]
    c = cs.valider(c, acteur=ACTEUR, commentaire="mois contrôlé", db_path=base)
    assert c["statut"] == cs.ST_VALIDEE


# ══ 4-12. Bloqueurs lus dans Flux financiers ══════════════════════════════════════════════════

def test_04_05_mouvement_qonto_a_qualifier_bloque_puis_resolu_ne_bloque_plus(base, verrous):
    _importer(base, [_mvt(33.0, date="2026-07-10", contrepartie="QUINCAILLERIE TEST")])
    a = _analyse(base)
    assert _types(base) == ["Mouvement bancaire à qualifier"]
    b = a["bloquants"][0]
    assert b["montant"] == 33.0 and b["date_fr"] == "10/07/2026" and "Qonto" in b["origine"]
    assert b["lien"].startswith("/flux-financiers/rapprochement?m=BANQUE%3A") and b["action"]
    m = _par_montant(base, 33.0)
    cid = _charge(base, 33.0, date="2026-07-10")
    assert lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)["ok"]
    assert _types(base) == [], "mouvement rapproché et comptabilisé : plus de bloqueur"


def test_06_mouvement_a_comptabiliser_bloque(base, verrous):
    from tests.test_qonto_rapprochement_caisse import retrait
    from app.services import qonto_ecran_service as ecran
    from tests.test_qonto_raw_import import ClientDouble
    ecran.actualiser(client=ClientDouble(pages=[[retrait(settled_at="2026-07-19T10:00:00.000Z",
                                                         emitted_at="2026-07-19T09:00:00.000Z")]]),
                     db_path=base)
    types = _types(base)
    assert types == ["Retrait d'espèces à comptabiliser"], "compté une fois (Banque), pas deux (Caisse)"


def test_07_erreur_flux_bloque(base, verrous):
    _importer(base, [_mvt(20.0, date="2026-07-05")])
    m = _par_montant(base, 20.0)
    conn = get_db(base)
    try:      # rapprochement confirmé SUPÉRIEUR au mouvement : l'anomalie que Flux dit « Erreur »
        conn.execute("INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
                     "type_objet, objet_id, montant_rapproche, statut) VALUES (?,?,?,?,?,?)",
                     ("BRP-TEST-ERR", m["id"], "CHARGE_FOURNISSEUR", "CHG-X", 25.0, "CONFIRME"))
        conn.commit()
    finally:
        conn.close()
    assert _types(base) == ["Mouvement bancaire en anomalie"]


def test_08_09_10_compte_a_definir_provisoire_insuffisante_validee_resout(base, verrous):
    _charge(base, 18.0, date="2026-07-08", categorie="CHG_010", commentaire="Frais test")
    a = _analyse(base)
    assert _types(base) == ["Compte comptable à définir"]
    assert a["message_comptes"] == "1 opération(s) nécessitent encore un compte comptable."
    assert a["bloquants"][0]["lien"].startswith("/comptabilite/mappings?categorie=CHG_010")
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "627000", cle="CHG_010", statut=maps.ST_PROVISOIRE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    assert _types(base) == ["Compte comptable à définir"], "une règle provisoire ne suffit pas"
    maps.valider_regle(rid, acteur=ACTEUR, db_path=base)
    a = _analyse(base)
    assert a["nb_bloquants"] == 0 and a["message_comptes"] == ""
    assert [i["type"] for i in a["informatifs"]] == ["Charge comptable pas encore comptabilisée"]


def test_11_un_autre_mois_ne_bloque_pas(base, verrous):
    _importer(base, [_mvt(44.0, date="2026-08-03")])
    _charge(base, 9.0, date="2026-08-04", categorie="CHG_010")
    _ecriture_proposee(base, "2026-08")
    assert _analyse(base, PASSE)["nb_bloquants"] == 0
    assert _analyse(base, "2026-08")["nb_bloquants"] == 3


def test_12_mouvement_de_caisse_selon_la_regle(base, verrous):
    from app.services import operations_caisse_service as caisse
    res = caisse.creer("AUTRE", 12.5, date_operation="2026-07-20", piece="Ticket 7", acteur=ACTEUR,
                       db_path=base)
    assert res["ok"], res
    a = _analyse(base)
    assert [b["type"] for b in a["bloquants"]] == ["Opération de caisse à qualifier"]
    assert a["bloquants"][0]["lien"].startswith("/flux-financiers/rapprochement?m=CAISSE%3A")


# ══ 13-15. Écritures et contrôles informatifs ═════════════════════════════════════════════════

def test_13_14_ecriture_proposee_bloque_validee_ne_bloque_plus(base, verrous):
    opaque = _ecriture_proposee(base)
    a = _analyse(base)
    assert _types(base) == ["Écriture proposée à valider"]
    assert a["bloquants"][0]["lien"] == f"/comptabilite/ecritures/{opaque}"
    assert "balance" in a["bloquants"][0]["raison"]
    assert compta.valider(opaque, acteur=ACTEUR, db_path=base)["ok"]
    assert _types(base) == []


def test_15_controles_informatifs_ne_bloquent_pas(base, verrous):
    _facture(base, "A_CONTROLER")
    _facture(base, "VALIDEE", montant=80.0)
    _charge(base, 70.0, date="2026-07-02", impact="HC", mode="PAY_003", commentaire="Perso")
    _importer(base, [_mvt(0.0, date="2026-07-06", statut="declined", contrepartie="CARTE TEST")])
    a = _analyse(base)
    assert a["nb_bloquants"] == 0
    types = sorted(i["type"] for i in a["informatifs"])
    assert "Facture fournisseur à contrôler" in types and "Facture fournisseur validée" in types
    assert "Charge hors comptabilité" in types and "Opération bancaire sans effet" in types
    assert all(i["raison"] for i in a["informatifs"]), "chaque informatif dit pourquoi il ne bloque pas"
    c = _a_valider(base)
    assert cs.valider(c, acteur=ACTEUR, commentaire="ok", db_path=base)["statut"] == cs.ST_VALIDEE


# ══ 16-19. Autorité du serveur, double clôture, atomicité ═════════════════════════════════════

def test_16_post_force_avec_bloqueur_refuse(client, base, verrous):
    c = _a_valider(base)
    _ecriture_proposee(base)                   # un bloqueur apparaît APRÈS la préparation
    r = client.post(f"/clotures/{c['cloture_id_opaque']}/valider",
                    data={"commentaire": "forçage"}, follow_redirects=True)
    assert "Ce mois ne peut pas être clôturé : 1 contrôle bloquant reste à traiter." in html.unescape(r.text)
    assert cs.charger_par_mois(PASSE, base)["statut"] == cs.ST_A_VALIDER
    page = html.unescape(client.get(f"/clotures/{c['cloture_id_opaque']}/validation").text)
    assert 'data-testid="validation-indisponible"' in page and "disabled" in page


def test_17_post_force_sur_mois_courant_et_futur_refuse(client, base):
    for mois, attendu in ((COURANT, "Le mois de septembre 2026 est encore en cours"),
                          (FUTUR, "Le mois d'octobre 2026 n'est pas commencé")):
        c = _a_valider(base, mois)
        r = client.post(f"/clotures/{c['cloture_id_opaque']}/valider",
                        data={"commentaire": "forçage"}, follow_redirects=True)
        assert attendu in html.unescape(r.text)
        assert cs.charger_par_mois(mois, base)["statut"] == cs.ST_A_VALIDER


def test_18_double_cloture_refusee(base):
    c = cs.valider(_a_valider(base), acteur=ACTEUR, commentaire="ok", db_path=base)
    with pytest.raises(cs.ClotureRefusee, match="interdite"):
        cs.valider(c, acteur=ACTEUR, commentaire="encore", db_path=base)
    evts = [e["nouveau_statut"] for e in cs.historique(c["cloture_id_opaque"], base)]
    assert evts.count(cs.ST_VALIDEE) == 1


def test_19_validation_atomique(base, monkeypatch):
    c = _a_valider(base)
    avant = len(cs.historique(c["cloture_id_opaque"], base))
    origine = cs._journaliser_evenement

    def panne(conn, opaque, type_evt, *a, **k):
        if type_evt == "CONTROLES" and cs.charger_par_mois(PASSE, base)["statut"] == cs.ST_A_VALIDER:
            raise RuntimeError("panne simulée après la transition")
        return origine(conn, opaque, type_evt, *a, **k)

    monkeypatch.setattr(cs, "_journaliser_evenement", panne)
    with pytest.raises(RuntimeError):
        cs.valider(c, acteur=ACTEUR, commentaire="ok", db_path=base)
    apres = cs.charger_par_mois(PASSE, base)
    assert apres["statut"] == cs.ST_A_VALIDER and apres["version"] == c["version"]
    assert len(cs.historique(c["cloture_id_opaque"], base)) == avant, "rien de partiel"


def test_archivage_refait_les_memes_gardes(base, verrous):
    conn = get_db(base)
    try:      # une clôture VALIDÉE sur le mois courant (état hérité, jamais atteignable aujourd'hui)
        conn.execute("INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                     "VALUES (?,?,?,?)", (cs.cloture_id_opaque(COURANT), COURANT, cs.ST_VALIDEE, "t"))
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(cs.ClotureRefusee, match="encore en cours"):
        cs.archiver(cs.charger_par_mois(COURANT, base), acteur=ACTEUR, db_path=base)
    c = cs.valider(_a_valider(base), acteur=ACTEUR, commentaire="ok", db_path=base)
    _ecriture_proposee(base)
    with pytest.raises(cs.ClotureRefusee, match="1 contrôle bloquant"):
        cs.archiver(c, acteur=ACTEUR, db_path=base)
    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM ref_cloture_mensuelle WHERE statut_mois='CLOTURE'"
                            ).fetchone()[0] == 0, "aucune clôture réelle posée"
    finally:
        conn.close()


# ══ 20-25. La clôture ne décide rien à la place de l'utilisateur ══════════════════════════════

def test_20_a_24_aucune_decision_automatique(client, base, verrous):
    _importer(base, [_mvt(33.0, date="2026-07-10")])
    _charge(base, 18.0, date="2026-07-08", categorie="CHG_010")
    opaque = _ecriture_proposee(base)
    _facture(base, "A_CONTROLER")
    avant = _empreinte(base, *TABLES_METIER)
    c = _a_valider(base)
    for url in ("/clotures", f"/clotures/mois/{PASSE}", f"/clotures/{c['cloture_id_opaque']}",
                f"/clotures/{c['cloture_id_opaque']}/validation"):
        assert client.get(url).status_code == 200
    client.post(f"/clotures/{c['cloture_id_opaque']}/valider", data={"commentaire": "x"})
    assert _empreinte(base, *TABLES_METIER) == avant, \
        "ni Qonto, ni qualification, ni mapping, ni écriture, ni facture modifiés"
    conn = get_db(base)
    try:
        assert conn.execute("SELECT statut FROM ecritures WHERE ecriture_id_opaque=?",
                            (opaque,)).fetchone()[0] == compta.ST_PROPOSEE
        assert conn.execute("SELECT statut FROM factures").fetchone()[0] == "A_CONTROLER"
    finally:
        conn.close()


def test_25_lecture_du_mois_courant_sans_ecriture(client, base, verrous):
    _importer(base, [_mvt(12.0, date="2026-09-21", contrepartie="FORFAIT TEST")])
    avant = _empreinte(base, "clotures_mensuelles", "cloture_evenements", *TABLES_METIER)
    r = client.get(f"/clotures/mois/{COURANT}")
    page = html.unescape(r.text)
    assert r.status_code == 200 and "Contrôle du mois — septembre 2026" in page
    assert "Clôture impossible — 1 élément bloquant" in page
    assert "encore en cours et ne peut pas être clôturé" in page
    assert "Mouvement bancaire à qualifier" in page and "Traiter dans Flux financiers" in page
    assert _empreinte(base, "clotures_mensuelles", "cloture_evenements", *TABLES_METIER) == avant
    liste = html.unescape(client.get("/clotures").text)
    assert "septembre 2026" in liste, "le mois courant figure dans la liste, même sans constat moteur"


# ══ 26-30. Cycle complet, historique, routes, isolement, non-régression ══════════════════════

def test_26_27_cycle_complet_et_historique(client, base, verrous):
    _importer(base, [_mvt(33.0, date="2026-07-10")])
    r = client.post("/clotures/demarrer", data={"mois": PASSE}, follow_redirects=False)
    opaque = r.headers["location"].rsplit("/", 1)[-1]
    client.post(f"/clotures/{opaque}/passer-a-valider")
    r = client.post(f"/clotures/{opaque}/valider", data={"commentaire": "c1"}, follow_redirects=True)
    assert "1 contrôle bloquant" in html.unescape(r.text)
    m = _par_montant(base, 33.0)
    cid = _charge(base, 33.0, date="2026-07-10")
    lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    r = client.post(f"/clotures/{opaque}/valider", data={"commentaire": "juillet contrôlé"},
                    follow_redirects=True)
    assert cs.charger_par_opaque(opaque, base)["statut"] == cs.ST_VALIDEE
    evenements = cs.historique(opaque, base)
    controles = [e["commentaire"] for e in evenements if e["type_evenement"] == "CONTROLES"]
    assert controles[0].startswith("Contrôles recalculés : 0 bloquant(s) moteur, 0 bloquant(s) Flux")
    assert "1 bloquant(s) Flux" in controles[-1], "la trace de la préparation reste dans l'historique"
    hist = html.unescape(client.get(f"/clotures/{opaque}/historique").text)
    assert "juillet contrôlé" in hist


def test_27_bis_alerte_si_un_bloqueur_apparait_apres_validation(client, base, verrous):
    c = cs.valider(_a_valider(base), acteur=ACTEUR, commentaire="ok", db_path=base)
    _ecriture_proposee(base)
    fiche = client.get(f"/clotures/{c['cloture_id_opaque']}").text
    assert 'data-testid="alerte-apres-validation"' in fiche


def test_28_securite_des_routes(client, base):
    for url in ("/clotures/mois/2026-13", "/clotures/mois/abc", "/clotures/mois?mois=%27%3B--"):
        r = client.get(url, follow_redirects=False)
        assert r.status_code == 303 and r.headers["location"] == "/clotures"
    assert client.post("/clotures/CLO-inconnu/valider", data={"commentaire": "x"},
                       follow_redirects=False).status_code == 303
    # Mission 33 (supersède l'assertion « aucune route ne pose la clôture réelle ») : la clôture
    # définitive a sa route, qui n'accepte qu'un POST confirmé et refait tous les contrôles.
    assert client.get("/clotures/CLO-inconnu/cloture-definitive").status_code == 404
    assert client.post("/clotures/CLO-inconnu/cloture-definitive", data={"confirmation": "oui"},
                       follow_redirects=False).status_code == 303
    assert client.get("/clotures/mois/2026-09").status_code == 200


def test_29_aucune_donnee_reelle(base):
    reelle = (Path(cfg.APP_ROOT) / "data" / "app.db").resolve()
    assert Path(cfg.DB_PATH).resolve() != reelle
    source = (Path(cfg.APP_ROOT) / "app" / "services" / "cloture_flux_service.py").read_text("utf-8")
    for interdit in ("INSERT ", "UPDATE ", "DELETE ", ".commit(", "requests.", "httpx."):
        assert interdit not in source, f"le service de bloqueurs ne fait que lire ({interdit})"


def test_30_non_regression_flux_mois_cloture_protege(base, verrous):
    """Après la clôture définitive (archivage, exposé par la route de la Mission 33), Flux refuse tout
    rapprochement sur le mois : la protection existante reste celle qui s'applique."""
    c = cs.valider(_a_valider(base), acteur=ACTEUR, commentaire="ok", db_path=base)
    cs.archiver(c, acteur=ACTEUR, db_path=base)
    assert flux.mois_cloture(PASSE, db_path=base)
    _importer(base, [_mvt(15.0, date="2026-07-25")])
    m = _par_montant(base, 15.0)
    cid = _charge(base, 15.0, date="2026-08-02")
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"] is False
    assert _analyse(base)["bloquants"][0]["type"] == "Mouvement bancaire à qualifier"


def test_retrait_passe_mais_ecriture_proposee_le_bloqueur_le_dit(base, verrous):
    """Mission 34 — défaut trouvé sur copie réelle : après « Comptabiliser le transfert », l'écriture
    530 / 512 naît PROPOSÉE (contrat existant) et le bloqueur affirmait encore « le transfert n'est
    pas encore passé ». Il doit dire que l'écriture est à valider, et y mener."""
    from tests.test_qonto_rapprochement_caisse import retrait
    from app.services import qonto_ecran_service as ecran
    from app.services import qonto_validation_service as qv
    from tests.test_qonto_raw_import import ClientDouble
    ecran.actualiser(client=ClientDouble(pages=[[retrait(settled_at="2026-07-19T10:00:00.000Z",
                                                         emitted_at="2026-07-19T09:00:00.000Z")]]),
                     db_path=base)
    conn = get_db(base)
    try:
        uuid = conn.execute("SELECT qonto_transaction_uuid FROM qonto_transactions_statut_local "
                            "WHERE nature='RETRAIT_ESPECES'").fetchone()[0]
    finally:
        conn.close()
    res = qv.valider(uuid, nature=qv.TRANSFERT_CAISSE, acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    opaque = res["ecriture"]["ecriture_id_opaque"]
    a = _analyse(base)
    assert [b["type"] for b in a["bloquants"]] == ["Retrait d'espèces : écriture à valider"]
    assert a["bloquants"][0]["lien"] == f"/comptabilite/ecritures/{opaque}"
    assert "pas encore passé" not in a["bloquants"][0]["raison"]
    assert compta.valider(opaque, acteur=ACTEUR, db_path=base)["ok"]
    assert _types(base) == []

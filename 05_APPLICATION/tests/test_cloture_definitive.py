"""Mission 33 — clôture définitive depuis l'interface (VALIDEE → ARCHIVEE, mois à CLOTURE).

Données FICTIVES (base temporaire). Date du jour FIXÉE au 28/09/2026 : septembre 2026 est le mois
courant, juillet 2026 un mois terminé, octobre 2026 un mois futur.

Ce qui est vérifié : le bouton n'apparaît que sur une clôture validée et éligible ; la page de
confirmation n'écrit rien ; le POST appelle le service existant, qui refait tous les contrôles
(calendrier, statut relu, état périmé, bloqueurs moteur et Flux) ; l'archivage est atomique
(archive, CLOTURE, ARCHIVEE, historique) ; persistance ; protections après clôture ; réouverture
existante ; aucune donnée réelle touchée.
"""
from __future__ import annotations

import html
import sqlite3
from datetime import date
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import clotures_service as cs
from app.services import comptabilite_ecritures_service as compta
from app.services import flux_lettrage_service as lettrage
from tests.test_cloture_flux_financiers import _a_valider, _ecriture_proposee
from tests.test_cloture_archivage_economique import db_avec_reservation  # noqa: F401
from tests.test_flux_financiers import (ACTEUR, _charge, _importer, _mvt, _par_montant,  # noqa: F401
                                        base, verrous)

PASSE, COURANT, FUTUR = "2026-07", "2026-09", "2026-10"


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 9, 28))


def _validee(db, mois=PASSE):
    return cs.valider(_a_valider(db, mois), acteur=ACTEUR, commentaire="contrôlé", db_path=db)


def _statut_ref(db, mois=PASSE):
    conn = get_db(db)
    try:
        r = conn.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?", (mois,)).fetchone()
        return r[0] if r else None
    finally:
        conn.close()


def _post(client, c, **data):
    actuelle = cs.charger_par_opaque(c["cloture_id_opaque"])
    donnees = {"confirmation": "oui", "version": str(actuelle["version"])}
    donnees.update(data)
    r = client.post(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive", data=donnees,
                    follow_redirects=True)
    return html.unescape(r.text)


def _statut(c):
    return cs.charger_par_opaque(c["cloture_id_opaque"])["statut"]


# ══ 1-4. Bouton, confirmation, POST ═══════════════════════════════════════════════════════════

def test_01_02_bouton_absent_si_non_validee_present_si_validee_et_eligible(client, base):
    c = _a_valider(base)
    assert 'data-testid="bouton-cloture-definitive"' not in client.get(f"/clotures/{c['cloture_id_opaque']}").text
    c = cs.valider(c, acteur=ACTEUR, commentaire="ok", db_path=base)
    fiche = client.get(f"/clotures/{c['cloture_id_opaque']}").text
    assert 'data-testid="bouton-cloture-definitive"' in fiche
    assert "Clôturer définitivement le mois" in html.unescape(fiche)


def test_02_bis_bouton_indisponible_si_bloqueur_apparu(client, base, verrous):
    c = _validee(base)
    _ecriture_proposee(base)
    fiche = html.unescape(client.get(f"/clotures/{c['cloture_id_opaque']}").text)
    assert 'data-testid="bouton-cloture-definitive"' not in fiche
    assert "Clôture définitive indisponible : 1 élément(s) bloquant(s)" in fiche


def test_03_page_de_confirmation_n_ecrit_rien(client, base):
    c = _validee(base)
    avant = (dict(cs.charger_par_opaque(c["cloture_id_opaque"])), len(cs.historique(c["cloture_id_opaque"])))
    page = html.unescape(client.get(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive").text)
    assert ("Cette action clôturera définitivement le mois de juillet 2026. Les opérations de cette "
            "période ne pourront plus être modifiées normalement.") in " ".join(page.split())
    assert "Confirmer la clôture définitive" in page and "Annuler" in page
    assert "Validée" in page and 'data-testid="nb-bloqueurs">0<' in page
    apres = (dict(cs.charger_par_opaque(c["cloture_id_opaque"])), len(cs.historique(c["cloture_id_opaque"])))
    assert apres == avant and _statut_ref(base) is None


def test_04_12_13_14_post_definitif_reussi(client, base):
    c = _validee(base)
    _post(client, c, commentaire="juillet arrêté")
    assert _statut(c) == cs.ST_ARCHIVEE
    assert _statut_ref(base) == "CLOTURE"
    fiche = html.unescape(client.get(f"/clotures/{c['cloture_id_opaque']}").text)
    assert "Clôturée définitivement" in fiche
    evts = cs.historique(c["cloture_id_opaque"])
    assert any(e["nouveau_statut"] == cs.ST_ARCHIVEE and e["commentaire"] == "juillet arrêté" for e in evts)
    assert evts[0]["type_evenement"] == "CONTROLES" and "0 bloquant(s) Flux" in evts[0]["commentaire"]


def test_04_bis_confirmation_obligatoire(client, base):
    c = _validee(base)
    page = _post(client, c, confirmation="")
    assert "Cochez la confirmation" in page and _statut(c) == cs.ST_VALIDEE


# ══ 5-9. Le serveur refait tout ═══════════════════════════════════════════════════════════════

def test_05_statut_non_validee_refuse(client, base):
    c = _a_valider(base)
    page = _post(client, c)
    assert cs.MSG_NON_VALIDEE in page
    assert _statut(c) == cs.ST_A_VALIDER and _statut_ref(base) is None, "jamais deux décisions en un clic"


def test_06_07_mois_courant_et_futur_refuses_meme_force(client, base):
    for mois, attendu in ((COURANT, "Le mois de septembre 2026 est encore en cours"),
                          (FUTUR, "Le mois d'octobre 2026 n'est pas commencé")):
        conn = get_db(base)
        try:      # état VALIDEE forgé : inatteignable par l'interface, le serveur refuse quand même
            conn.execute("INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                         "VALUES (?,?,?,?)", (cs.cloture_id_opaque(mois), mois, cs.ST_VALIDEE, "t"))
            conn.commit()
        finally:
            conn.close()
        c = cs.charger_par_mois(mois)
        assert attendu in _post(client, c)
        assert _statut(c) == cs.ST_VALIDEE and _statut_ref(base, mois) is None


def test_08_mois_avec_bloqueur_refuse(client, base, verrous):
    c = _validee(base)
    _importer(base, [_mvt(33.0, date="2026-07-10")])
    assert "Ce mois ne peut pas être clôturé : 1 contrôle bloquant reste à traiter." in _post(client, c)
    assert _statut(c) == cs.ST_VALIDEE and _statut_ref(base) is None


def test_09_nouveau_bloqueur_entre_get_et_post_refuse(client, base, verrous):
    c = _validee(base)
    page = client.get(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive").text
    assert 'data-testid="confirmer-cloture-definitive"' in page
    _ecriture_proposee(base)                                # apparaît après l'affichage
    assert "1 contrôle bloquant" in _post(client, c)
    assert _statut(c) == cs.ST_VALIDEE and _statut_ref(base) is None


def test_09_bis_etat_perime_refuse(client, base):
    c = _validee(base)
    version_affichee = cs.charger_par_opaque(c["cloture_id_opaque"])["version"]
    cs.rouvrir(cs.charger_par_opaque(c["cloture_id_opaque"]), acteur=ACTEUR, justification="revoir")
    cs.passer_a_valider(cs.demarrer_preparation(cs.charger_par_opaque(c["cloture_id_opaque"])))
    cs.valider(cs.charger_par_opaque(c["cloture_id_opaque"]), acteur=ACTEUR, commentaire="revalidé")
    page = _post(client, c, version=str(version_affichee))
    assert cs.MSG_ETAT_PERIME in page and _statut(c) == cs.ST_VALIDEE


# ══ 10-11. Écritures proposées ════════════════════════════════════════════════════════════════

def test_10_11_ecriture_proposee_bloque_contrepassee_ne_bloque_plus(client, base, verrous):
    c = _validee(base)
    opaque = _ecriture_proposee(base)
    assert "1 contrôle bloquant" in _post(client, c)
    # Le modèle distingue l'écriture devenue sans objet : elle se CONTREPASSE (statut CONTREPASSEE,
    # miroir VALIDEE) — mécanisme unique du projet. Elle cesse alors de bloquer.
    assert compta.contrepasser(opaque, commentaire="vente annulée (test)", acteur=ACTEUR)["ok"]
    _post(client, c)
    assert _statut(c) == cs.ST_ARCHIVEE and _statut_ref(base) == "CLOTURE"


# ══ 15-18. Archive, atomicité, double tentative, persistance ═════════════════════════════════

def test_16_rollback_complet_si_echec(base, monkeypatch):
    c = _validee(base)
    origine = cs._journaliser_evenement

    def panne(conn, opaque, type_evt, *a, **k):
        if type_evt == "CONTROLES":
            raise RuntimeError("panne simulée après l'archive, CLOTURE et la transition")
        return origine(conn, opaque, type_evt, *a, **k)

    monkeypatch.setattr(cs, "_journaliser_evenement", panne)
    with pytest.raises(RuntimeError):
        cs.archiver(c, acteur=ACTEUR, db_path=base)
    assert _statut(c) == cs.ST_VALIDEE and _statut_ref(base) is None
    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM reservations_historique_cloture WHERE mois_cloture=?",
                            (PASSE,)).fetchone()[0] == 0
    finally:
        conn.close()


def test_17_18_seconde_tentative_refusee_et_persistance(client, base):
    c = _validee(base)
    _post(client, c)
    assert cs.MSG_DEJA_ARCHIVEE in _post(client, c)
    evts = [e for e in cs.historique(c["cloture_id_opaque"]) if e["nouveau_statut"] == cs.ST_ARCHIVEE]
    assert len(evts) == 1
    brut = sqlite3.connect(str(base))          # « redémarrage » : connexion neuve, hors application
    try:
        assert brut.execute("SELECT statut FROM clotures_mensuelles WHERE mois=?", (PASSE,)).fetchone()[0] \
            == cs.ST_ARCHIVEE
        assert brut.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?",
                            (PASSE,)).fetchone()[0] == "CLOTURE"
    finally:
        brut.close()


# ══ 19-20. Après clôture : protections et réouverture existante ══════════════════════════════

def test_19_protections_apres_cloture(client, base, verrous):
    _importer(base, [_mvt(15.0, date="2026-07-25")])
    m = _par_montant(base, 15.0)
    cid = _charge(base, 15.0, date="2026-07-25")
    # le mouvement du mois bloque : on le traite AVANT la clôture, comme l'utilisateur
    assert lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)["ok"]
    c = _validee(base)
    _post(client, c)
    assert _statut(c) == cs.ST_ARCHIVEE
    # Saisie d'une charge : le parcours utilisateur (prévisualisation puis confirmation) rejoue
    # `validate_charge`, qui refuse un mois CLOTURE (V02).
    from app.services import charges_preview_service as prev
    erreurs = prev.validate_charge({"date_charge": "2026-07-26", "montant": "9", "categorie_charge_id":
                                    "CHG_018"}, prev.load_form_refs(db_path=base))
    assert "V02_MOIS_CLOTURE" in {e["code"] for e in erreurs}
    _importer(base, [_mvt(21.0, date="2026-07-27")])
    m2 = _par_montant(base, 21.0)
    cid2 = _charge(base, 21.0, date="2026-08-02")
    refus = lettrage.valider([f"BANQUE:{m2['id']}"], [f"CHARGE:{cid2}"], acteur=ACTEUR, db_path=base)
    assert refus["ok"] is False, "aucun rapprochement sur un mois clôturé"


def test_20_reouverture_existante(base):
    from app.services import menages_mois_clotures_service as mmc
    c = _validee(base)
    cs.archiver(c, acteur=ACTEUR, db_path=base)
    with pytest.raises(mmc.DecisionRefusee, match="archivée"):
        mmc.rouvrir(PASSE, motif="test", acteur=ACTEUR, confirmation=True, db_path=base)
    assert _statut_ref(base) == "CLOTURE", "une clôture définitive ne se rouvre pas par cet écran"
    # Une clôture seulement VALIDÉE se rouvre toujours par son automate (VALIDEE → ROUVERTE).
    v = _validee(base, "2026-06")
    assert cs.rouvrir(v, acteur=ACTEUR, justification="revoir juin", db_path=base)["statut"] == cs.ST_ROUVERTE


# ══ 21-22. Isolement ══════════════════════════════════════════════════════════════════════════

def test_21_22_aucune_donnee_reelle(base):
    reelle = (Path(cfg.APP_ROOT) / "data" / "app.db").resolve()
    assert Path(cfg.DB_PATH).resolve() != reelle


def test_15_archive_economique_creee_par_la_route(db_avec_reservation):
    """Archive économique réelle (réservation VALIDE du mois) créée par la ROUTE, puis vérifiée."""
    from fastapi.testclient import TestClient
    from app.main import app
    db = db_avec_reservation
    c = cs.valider(_a_valider(db, "2026-06"), acteur=ACTEUR, commentaire="ok", db_path=db)
    _post(TestClient(app), c)
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM reservations_historique_cloture "
                            "WHERE mois_cloture='2026-06'").fetchone()[0] == 1
        assert conn.execute("SELECT statut FROM clotures_mensuelles WHERE mois='2026-06'"
                            ).fetchone()[0] == cs.ST_ARCHIVEE
    finally:
        conn.close()


def test_archive_ne_lit_que_le_jeu_de_calcul_actif(db_avec_reservation):
    """Défaut trouvé en recette sur copie réelle : plusieurs jeux `reservations_resolues`
    conservés → chaque réservation archivée autant de fois, vérification refusée, clôture
    définitive impossible. Seul le jeu ACTIF fait foi."""
    from app.services import orchestrateur_moteur as om
    db = db_avec_reservation
    om.executer_reservations(db_path=db)          # un second jeu, l'ancien est conservé
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(DISTINCT dataset_id) FROM reservations_resolues "
                            "WHERE mois='2026-06'").fetchone()[0] >= 2
    finally:
        conn.close()
    c = cs.valider(_a_valider(db, "2026-06"), acteur=ACTEUR, commentaire="ok", db_path=db)
    cs.archiver(c, acteur=ACTEUR, db_path=db)
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM reservations_historique_cloture "
                            "WHERE mois_cloture='2026-06'").fetchone()[0] == 1
    finally:
        conn.close()

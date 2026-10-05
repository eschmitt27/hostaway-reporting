"""Clôture par modules — le parcours : démarrer, traiter, clôturer un module, clôturer le mois, rouvrir.

Données FICTIVES (base temporaire). Date du jour FIXÉE au 05/10/2026 : septembre 2026 est un mois
terminé, octobre 2026 le mois courant, novembre 2026 un mois futur.

Ce qui est vérifié, dans l'ordre du parcours :
  · ouvrir un mois, démarrer sa clôture (rien n'est clôturé ni verrouillé) ;
  · le tableau de bord : modules, compteurs, bloqueurs, lien « Traiter » vers la vraie page ;
  · un module bloqué ne se clôture pas (service, écran, route) ; un bloqueur disparaît tout seul quand le
    vrai problème est corrigé dans son module ;
  · clôturer un module : trace (date, heure, acteur), journal en ajout seul, VRAI verrou dans chaque
    domaine, les autres domaines et la consultation restent libres ;
  · clôturer le mois : refusé si un module est ouvert ou si un bloqueur réapparaît, accepté sinon, en une
    transaction ; le mois courant et les mois futurs ne se clôturent jamais ;
  · rouvrir un module : justification obligatoire, verrou levé, trace d'origine conservée, cohérence avec
    l'état de la clôture et la période comptable ;
  · aucune anomalie recopiée ; écrans sans vocabulaire technique, accessibles, prévus pour le téléphone.
"""
from __future__ import annotations

import html
import re
import sqlite3
from datetime import date

import pytest

from app.db.connection import get_db
from app.services import charges_saisie_service as saisie
from app.services import charges_validation_service as charges_val
from app.services import cloture_modules_service as cm
from app.services import cloture_verrous_service as verrous_cloture
from app.services import clotures_service as cs
from app.services import comptabilite_periodes_service as per
from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from tests.aides_cloture_modules import clore_modules
from tests.test_cloture_flux_financiers import _ecriture_proposee
from tests.test_cloture_modules_bloqueurs import _charge_a_controler, _facture_client
from tests.test_flux_financiers import (ACTEUR, _charge, _importer, _mvt, _par_montant,  # noqa: F401
                                        base, verrous)

MOIS, COURANT, FUTUR = "2026-09", "2026-10", "2026-11"
TOUS = [m.cle for m in cm.MODULES]


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 10, 5))


# ── Aides ───────────────────────────────────────────────────────────────────────────────────────────

def _demarrer(db, mois=MOIS):
    c = cs.creer_ou_charger(mois, acteur=ACTEUR, db_path=db)
    return cs.demarrer_preparation(c, acteur=ACTEUR, db_path=db)


def _recharger(db, c):
    return cs.charger_par_opaque(c["cloture_id_opaque"], db)


def _cloturer(db, c, *cles, commentaire=""):
    for cle in cles:
        cm.cloturer_module(_recharger(db, c), cle, acteur=ACTEUR, commentaire=commentaire, db_path=db)


def _tout_cloturer(db, c):
    _cloturer(db, c, *TOUS)


def _etat(db, c, cle):
    return next(m for m in cm.tableau_de_bord(_recharger(db, c), db_path=db)["modules"]
                if m["cle"] == cle)


def _lignes(db, sql, *params):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def _empreinte(db, *tables):
    import hashlib
    h = hashlib.sha256()
    conn = get_db(db)
    try:
        for t in tables:
            for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1"):
                h.update(repr(tuple(r)).encode())
    finally:
        conn.close()
    return h.hexdigest()


def _texte(page: str) -> str:
    """Le texte VISIBLE d'une page : sans balises, scripts ni styles."""
    sans = re.sub(r"<(script|style)\b.*?</\1>", " ", page, flags=re.S)
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", sans)).split())


def _fiche(client, c):
    return client.get(f"/clotures/{c['cloture_id_opaque']}").text


def _post_module(client, c, cle, action, **donnees):
    return client.post(f"/clotures/{c['cloture_id_opaque']}/modules/{cle}/{action}", data=donnees,
                       follow_redirects=False)


# ══ 1. Ouvrir un mois, démarrer sa clôture ═══════════════════════════════════════════════════════

def test_01_la_page_du_mois_montre_les_sept_modules_avant_tout_demarrage(client, base):
    avant = _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules",
                       "ref_cloture_mensuelle")
    r = client.get(f"/clotures/mois/{MOIS}")
    page = _texte(r.text)
    assert r.status_code == 200 and "Clôture de septembre 2026" in page
    assert "0 / 7 modules clôturés" in page and "Clôture non démarrée" in page
    for m in cm.MODULES:
        assert m.libelle in page
    assert 'data-testid="demarrer-cloture"' in r.text and "Démarrer la clôture" in page
    assert _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules",
                      "ref_cloture_mensuelle") == avant, "ouvrir un mois n'écrit rien"


def test_02_demarrer_ouvre_la_cloture_sans_rien_cloturer(client, base):
    r = client.post("/clotures/demarrer", data={"mois": MOIS}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/clotures/CLO-")
    c = cs.charger_par_mois(MOIS, base)
    assert c["statut"] == cs.ST_EN_PREPARATION
    assert _lignes(base, "SELECT * FROM cloture_modules") == []
    assert _lignes(base, "SELECT * FROM ref_cloture_mensuelle WHERE mois=?", MOIS) == []
    page = _texte(client.get(r.headers["location"]).text)
    assert "Clôture en cours" in page and "0 / 7 modules clôturés" in page
    assert "Ouverte le " in page, "la date d'ouverture de la clôture est visible"


def test_03_un_mois_futur_ne_se_demarre_pas(client, base):
    page = _texte(client.get(f"/clotures/mois/{FUTUR}").text)
    assert "n'est pas commencé" in page and "un mois futur ne peut jamais être clôturé" in page
    assert "Démarrer la clôture" not in page


def test_04_le_mois_courant_se_consulte_mais_ne_se_demarre_pas(client, base):
    """Démarrer ne sert à rien sur un mois qui court : ses points à traiter se consultent déjà, recalculés à chaque
    affichage. La clôture ne se démarre donc qu'une fois le mois terminé — jamais d'état « en préparation » sur
    un mois inachevé."""
    page = _texte(client.get(f"/clotures/mois/{COURANT}").text)
    assert "Clôture d'octobre 2026" in page, "devant une voyelle, « de » s'élide (jamais « de octobre »)"
    assert "Démarrer la clôture" not in page
    assert "est encore en cours" in page and "se consultent dès maintenant" in page
    assert "ne peut être démarrée qu'une fois le mois terminé" in page
    for m in cm.MODULES:
        assert m.libelle in page, "les modules et leurs bloqueurs restent consultables"


# ══ 2. Tableau de bord : compteurs, état, bloqueur, lien Traiter ═════════════════════════════════

def test_05_le_tableau_de_bord_compte_les_modules_clotures(client, base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS", "MENAGES", "BANQUE")
    t = cm.tableau_de_bord(_recharger(base, c), db_path=base)
    assert (t["nb_clos"], t["nb_total"], t["progression_libelle"]) == (3, 7, "3 / 7 modules clôturés")
    fiche = client.get(f"/clotures/{c['cloture_id_opaque']}").text
    assert "3 / 7 modules clôturés" in _texte(fiche)
    assert 'role="progressbar"' in fiche and 'aria-valuenow="3"' in fiche and 'aria-valuemax="7"' in fiche
    assert 'aria-valuetext="3 modules clôturés sur 7"' in fiche
    etats = {m["cle"]: m["etat"] for m in t["modules"]}
    assert [etats[k] for k in ("RESERVATIONS", "MENAGES", "BANQUE")] == [cm.ETAT_CLOS] * 3
    assert etats["CHARGES"] == cm.ETAT_PRET


def test_06_l_etat_d_un_module_suit_ses_bloqueurs_en_langage_metier(client, base):
    c = _demarrer(base)
    _charge_a_controler(base)
    t = cm.tableau_de_bord(c, db_path=base)
    charges = next(m for m in t["modules"] if m["cle"] == "CHARGES")
    assert (charges["etat"], charges["nb_bloqueurs"]) == (cm.ETAT_A_TRAITER, 1)
    assert charges["motif_indisponible"] == "1 bloqueur à traiter avant de clôturer ce module."
    page = _texte(_fiche(client, c))
    assert "1 bloqueur" in page and "1 charge à valider ou à rejeter" in page
    assert "Prêt à clôturer" in page, "les autres modules sont prêts"
    assert t["nb_bloqueurs"] == 1 and t["peut_cloturer_mois"] is False


def test_07_le_lien_traiter_mene_a_la_vraie_page_filtree(client, base):
    c = _demarrer(base)
    _charge_a_controler(base)
    fiche = _fiche(client, c)
    href = html.unescape(re.search(r'<a class="btn btn-secondary btn-sm clo-traiter" href="([^"]+)"',
                                   fiche).group(1))
    assert href == "/flux-financiers/charges?mois=2026-09&statut_controle=A_CONTROLER"
    assert 'data-testid="traiter"' in fiche
    _charge(base, 18.0, date="2026-09-08")                    # une charge validée : hors du filtre
    page = client.get(href)
    assert page.status_code == 200, "le lien ouvre la vraie page de Flux financiers, pas une copie"
    texte = _texte(page.text)
    assert "1 ligne affichée sur 2 au total" in texte, "la vraie page est filtrée sur ce qui reste à contrôler"
    assert "25.0 €" in texte and "18.0 €" not in texte


def test_08_corriger_dans_le_vrai_module_fait_disparaitre_le_bloqueur(client, base):
    from app.services import justificatifs_service as justif
    c = _demarrer(base)
    cid = _charge_a_controler(base)
    assert "1 charge à valider ou à rejeter" in _texte(_fiche(client, c))
    # Pas de case « traité » : on valide la charge DANS Charges, et c'est tout.
    justif.confirmer(justif.OBJET_CHARGE, cid, present="NON", justification="Test : sans pièce",
                     acteur=ACTEUR, db_path=base)
    assert saisie.valider_controle(cid, acteur=ACTEUR, db_path=base)["ok"]
    page = _texte(_fiche(client, c))
    assert "1 charge à valider ou à rejeter" not in page
    assert _etat(base, c, "CHARGES")["etat"] == cm.ETAT_PRET
    _cloturer(base, c, "CHARGES")
    assert _etat(base, c, "CHARGES")["etat"] == cm.ETAT_CLOS


# ══ 3. Un module bloqué ne se clôture pas ════════════════════════════════════════════════════════

def test_09_service_un_module_bloque_ne_se_cloture_pas(base):
    c = _demarrer(base)
    _charge_a_controler(base)
    with pytest.raises(cs.ClotureRefusee, match="1 bloqueur reste à traiter"):
        cm.cloturer_module(c, "CHARGES", acteur=ACTEUR, db_path=base)
    assert _lignes(base, "SELECT * FROM cloture_modules") == []
    assert _lignes(base, "SELECT * FROM cloture_modules_evenements") == [], "aucune trace d'un refus"


def test_10_ecran_et_route_un_module_bloque_n_offre_aucune_cloture(client, base):
    c = _demarrer(base)
    _charge_a_controler(base)
    fiche = _fiche(client, c)
    assert '/modules/CHARGES/cloturer"' not in fiche, "aucun lien pour clôturer le module bloqué"
    assert re.search(r'<button type="button"[^>]*aria-disabled="true"[^>]*data-testid="cloturer-module-indisponible"',
                     fiche), "un bouton désactivé explique pourquoi"
    assert '/modules/RESERVATIONS/cloturer"' in fiche, "un module prêt, lui, se clôture"
    confirmation = _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/modules/CHARGES/cloturer").text)
    assert "ne peut pas être clôturé pour le moment" in confirmation
    r = _post_module(client, c, "CHARGES", "cloturer", confirmation="oui")
    assert r.status_code == 303 and "erreur=" in r.headers["location"]
    assert _lignes(base, "SELECT * FROM cloture_modules") == []


def test_11_la_route_exige_la_confirmation_cochee(client, base):
    c = _demarrer(base)
    r = _post_module(client, c, "RESERVATIONS", "cloturer")
    assert "erreur=" in r.headers["location"] and _lignes(base, "SELECT * FROM cloture_modules") == []
    page = _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/modules/RESERVATIONS/cloturer").text)
    assert "Je confirme la clôture du module « Réservations » pour septembre 2026." in page


def test_12_une_cloture_non_demarree_ne_cloture_aucun_module(client, base):
    c = cs.creer_ou_charger(MOIS, acteur=ACTEUR, db_path=base)
    with pytest.raises(cs.ClotureRefusee, match="Démarrez d'abord la clôture"):
        cm.cloturer_module(c, "RESERVATIONS", acteur=ACTEUR, db_path=base)
    assert _lignes(base, "SELECT * FROM cloture_modules") == []


def test_13_le_mois_courant_et_le_mois_futur_ne_se_cloturent_jamais(client, base):
    for mois, attendu in ((COURANT, "encore en cours"), (FUTUR, "n'est pas commencé")):
        conn = get_db(base)
        try:      # clôture forgée : inatteignable par l'écran, le serveur refuse quand même
            conn.execute("INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                         "VALUES (?,?,?,?)", (cs.cloture_id_opaque(mois), mois, cs.ST_EN_PREPARATION, "t"))
            conn.commit()
        finally:
            conn.close()
        c = cs.charger_par_mois(mois, base)
        with pytest.raises(cs.ClotureRefusee, match=attendu):
            cm.cloturer_module(c, "RESERVATIONS", acteur=ACTEUR, db_path=base)
        with pytest.raises(cs.ClotureRefusee, match=attendu):
            cs.cloturer_mois(c, acteur=ACTEUR, db_path=base)
        r = _post_module(client, c, "RESERVATIONS", "cloturer", confirmation="oui")
        assert "erreur=" in r.headers["location"]
        modules = cm.tableau_de_bord(c, db_path=base)["modules"]
        assert not any(m["peut_cloturer"] for m in modules), "l'écran ne propose aucune clôture"
        assert "Clôturer le mois" in _texte(_fiche(client, c)) and \
            'data-testid="bouton-cloture-definitive"' not in _fiche(client, c)
    assert _lignes(base, "SELECT * FROM cloture_modules") == []
    assert _lignes(base, "SELECT * FROM ref_cloture_mensuelle WHERE statut_mois='CLOTURE'") == []


# ══ 4. Clôturer un module : trace et journal ═════════════════════════════════════════════════════

def test_14_cloturer_un_module_trace_date_heure_et_acteur(client, base):
    c = _demarrer(base)
    r = _post_module(client, c, "RESERVATIONS", "cloturer", confirmation="oui", commentaire="Vérifié")
    assert r.status_code == 303 and "message=" in r.headers["location"]
    ligne = _lignes(base, "SELECT * FROM cloture_modules")[0]
    assert (ligne["mois"], ligne["module"], ligne["statut"]) == (MOIS, "RESERVATIONS", "CLOS")
    assert ligne["acteur_cloture"] == "local" and ligne["commentaire_cloture"] == "Vérifié"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", ligne["date_cloture"]), ligne["date_cloture"]
    assert ligne["cloture_id_opaque"] == c["cloture_id_opaque"] and ligne["nb_clotures"] == 1
    evt = _lignes(base, "SELECT * FROM cloture_modules_evenements")
    assert [(e["type_evenement"], e["nouveau_statut"], e["acteur"]) for e in evt] == [
        ("CLOTURE_MODULE", "CLOS", "local")]
    assert evt[0]["resume"].startswith("Aucun bloqueur") and evt[0]["date_evenement"] == ligne["date_cloture"]
    journal = [e for e in cs.historique(c["cloture_id_opaque"], base) if e["type_evenement"] == "MODULE_CLOTURE"]
    assert journal and journal[0]["acteur"] == "local" and "Réservations : clôturé" in journal[0]["commentaire"]
    page = _texte(client.get(r.headers["location"]).text)
    assert "Module « Réservations » clôturé." in page
    assert re.search(r"Clôturé le \d{2}/\d{2}/\d{4} à \d{2}h\d{2}, par local\. « Vérifié »", page)


def test_15_le_journal_des_modules_est_en_ajout_seul(base):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS")
    conn = get_db(base)
    try:
        for sql in ("UPDATE cloture_modules_evenements SET commentaire='x'",
                    "DELETE FROM cloture_modules_evenements"):
            with pytest.raises(sqlite3.DatabaseError, match="CLOTURE_MODULE_TRACE"):
                conn.execute(sql)
    finally:
        conn.close()
    assert len(_lignes(base, "SELECT * FROM cloture_modules_evenements")) == 1


def test_16_un_module_deja_clos_ne_se_reclot_pas(base):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS")
    with pytest.raises(cs.ClotureRefusee, match="déjà clôturé"):
        cm.cloturer_module(_recharger(base, c), "RESERVATIONS", acteur=ACTEUR, db_path=base)
    assert len(_lignes(base, "SELECT * FROM cloture_modules_evenements")) == 1


def test_17_un_module_inconnu_est_refuse_proprement(client, base):
    c = _demarrer(base)
    with pytest.raises(cs.ClotureRefusee, match="Module de clôture inconnu"):
        cm.cloturer_module(c, "N_IMPORTE_QUOI", acteur=ACTEUR, db_path=base)
    assert client.get(f"/clotures/{c['cloture_id_opaque']}/modules/NIMPORTE/cloturer").status_code == 404


# ══ 5. Le verrou est réel, domaine par domaine ═══════════════════════════════════════════════════

def _refus_module(texte: str, module: str) -> None:
    libelle = cm.PAR_CLE[module].libelle
    assert f"Le module « {libelle} » est clôturé pour septembre 2026" in texte, texte
    assert "Rouvrez le module" in texte, "le refus dit comment rouvrir"


def test_18_charges_closes_refusent_saisie_modification_annulation_et_controle(base):
    cid = _charge(base, 18.0, date="2026-09-08")            # validée avant la clôture du module
    ouverte = _charge(base, 19.0, date="2026-09-09")
    a_valider = _charge_a_controler(base, date_="2026-09-21")
    c = _demarrer(base)
    # Verrou posé directement (cette charge à valider empêcherait la clôture réelle du module) : ce test
    # vérifie ce que le verrou REFUSE, la clôture du module a ses propres tests.
    clore_modules(base, MOIS, sauf=tuple(k for k in TOUS if k != "CHARGES"))
    formulaire = {"date_charge": "2026-09-20", "montant": 30, "categorie_charge_id": "CHG_018",
                  "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
                  "affectation_type": "GLOBAL", "refacturable": "NON", "statut_controle": "VALIDE",
                  "commentaire": "Après clôture"}
    refus = saisie.creer(formulaire, acteur=ACTEUR, db_path=base)
    assert refus["ok"] is False and refus["code"] == saisie.E_MOIS_CLOTURE
    _refus_module(refus["message"], "CHARGES")
    for res in (saisie.annuler(cid, acteur=ACTEUR, db_path=base),
                saisie.modifier(cid, {**formulaire, "date_charge": "2026-09-08"}, acteur=ACTEUR,
                                motif="essai", db_path=base),
                saisie.rouvrir_controle(ouverte, acteur=ACTEUR, motif="essai", db_path=base),
                saisie.signaler_anomalie(ouverte, acteur=ACTEUR, motif="essai", db_path=base)):
        assert res["ok"] is False and res["code"] == saisie.E_MOIS_CLOTURE, res
    # La validation de contrôle (service dédié) et la saisie à l'écran (validation de formulaire).
    res = charges_val.valider(a_valider, acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == "E_CHARGE_MOIS_CLOTURE"
    _refus_module(res["message"], "CHARGES")
    from app.services import charges_preview_service as prev
    erreurs = prev.validate_charge({"date_charge": "2026-09-26", "montant": "9",
                                    "categorie_charge_id": "CHG_018"}, prev.load_form_refs(db_path=base))
    assert "V02_MOIS_CLOTURE" in {e["code"] for e in erreurs}
    # Les autres mois de Charges restent saisissables.
    assert saisie.creer({**formulaire, "date_charge": "2026-10-02"}, acteur=ACTEUR, db_path=base)["ok"]


def test_19_charges_closes_refusent_les_factures_fournisseurs(base):
    from app.services import factures_service as fac
    c = _demarrer(base)
    _cloturer(base, c, "CHARGES")
    res = fac.creer({"date_facture": "2026-09-12", "fournisseur_id_opaque": "FRS-X", "montant_ttc": 50,
                     "facture_ref": "F-1"}, acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == fac.E_MOIS_CLOTURE
    _refus_module(res["message"], "CHARGES")
    assert fac._mois_est_cloture(MOIS, base) is True
    assert fac._mois_est_cloture("2026-08", base) is False


def test_20_banque_close_refuse_rapprochement_et_lettrage_mais_pas_la_lecture(client, base, verrous):
    _importer(base, [_mvt(15.0, date="2026-09-25")])
    m = _par_montant(base, 15.0)
    cid = _charge(base, 15.0, date="2026-09-25")
    assert lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)["ok"]
    c = _demarrer(base)
    _cloturer(base, c, "BANQUE")
    assert flux.mois_cloture(MOIS, db_path=base), "Flux voit le mois fermé pour la banque"
    _refus_module(flux.mois_cloture(MOIS, db_path=base), "BANQUE")
    # Qonto continue d'être lu ; mais rien ne se rapproche plus sur le mois clôturé.
    _importer(base, [_mvt(21.0, date="2026-09-27")])
    m2 = _par_montant(base, 21.0)
    cid2 = _charge(base, 21.0, date="2026-10-03")
    refus = lettrage.valider([f"BANQUE:{m2['id']}"], [f"CHARGE:{cid2}"], acteur=ACTEUR, db_path=base)
    assert refus["ok"] is False
    # La qualification humaine d'un mouvement Qonto (rapprochement) est refusée de la même façon, avant toute
    # autre vérification ; et rouvrir le module la rend de nouveau possible.
    from app.services import qonto_validation_service as qv
    uuid = qv.transaction_par_mouvement(m2["id"], db_path=base)["qonto_transaction_uuid"]
    qualification = qv.valider(uuid, nature=qv.REGLEMENT_CHARGE, objet_id="FAC-X", acteur=ACTEUR, db_path=base)
    assert qualification["ok"] is False and qualification["code"] == qv.E_MOIS_CLOTURE
    _refus_module(qualification["message"], "BANQUE")
    cm.rouvrir_module(_recharger(base, c), "BANQUE", acteur=ACTEUR, justification="mouvement tardif", db_path=base)
    autre = qv.valider(uuid, nature=qv.REGLEMENT_CHARGE, objet_id="FAC-X", acteur=ACTEUR, db_path=base)
    assert autre.get("code") != qv.E_MOIS_CLOTURE, "module rouvert : la garde ne refuse plus"
    assert client.get(f"/flux-financiers/banque?mois={MOIS}").status_code == 200, "la banque reste consultable"


def test_21_factures_clients_closes_refusent_toute_ecriture_mais_restent_lisibles(base):
    from app.services import factures_proprietaires_service as fpr
    _facture_client(base, statut="EMIS", fid="FPR-EMISE", numero="2026-09-001", logement="LOG_E")
    c = _demarrer(base)
    _cloturer(base, c, "FACTURES_CLIENTS")
    _facture_client(base, statut="BROUILLON", fid="FPR-TARDIVE", logement="LOG_T")   # apparue après
    with pytest.raises(fpr.FactureProprietaireError, match="Le module « Factures clients » est clôturé"):
        fpr.annuler("FPR-TARDIVE", motif="essai", acteur=ACTEUR, db_path=base)
    with pytest.raises(fpr.FactureProprietaireError, match="clôturé pour septembre 2026"):
        fpr.exiger_facturation_ouverte(MOIS, db_path=base)
    fpr.exiger_facturation_ouverte("2026-08", db_path=base)         # un autre mois n'est pas concerné
    assert fpr.lire("FPR-EMISE", db_path=base)["statut"] == "EMIS", "la facture reste consultable"


def test_22_menages_clos_refusent_les_declarations_et_la_resolution_des_conflits(base):
    from app.services import menages_declarations_service as mdecl
    conn = get_db(base)
    try:      # un conflit de déclaration ouvert : il empêcherait la clôture réelle du module, le verrou est posé
        conn.execute("INSERT INTO menages_declarations_conflits (mois, logement_id, intervenant_id, statut) "
                     "VALUES (?, 'LOG_X', 'INT_X', 'OUVERT')", (MOIS,))
        conn.commit()
    finally:
        conn.close()
    _demarrer(base)
    clore_modules(base, MOIS, sauf=tuple(k for k in TOUS if k != "MENAGES"))
    assert mdecl.mois_cloture(MOIS, db_path=base) is True and mdecl.mois_cloture("2026-08", db_path=base) is False
    refus = [
        mdecl.modifier(mois=MOIS, logement_id="LOG_X", intervenant_id="INT_X", nb_menages=2, acteur=ACTEUR,
                       db_path=base),
        mdecl.creer(mois=MOIS, logement_id="LOG_X", intervenant_id="INT_X", nb_menages=2, nb_heures=None,
                    acteur=ACTEUR, db_path=base),
        mdecl.resoudre_conflit(mdecl.lister_conflits(db_path=base)[0]["id"], choix="GARDER_APPLICATION",
                               acteur=ACTEUR, db_path=base),
    ]
    for res in refus:
        assert res["ok"] is False and res["code"] == mdecl.E_MOIS_CLOTURE, res
        _refus_module(res["message"], "MENAGES")
    assert mdecl.lister_conflits(db_path=base)[0]["statut"] == "OUVERT", "rien n'a été résolu"


def test_23_reservations_closes_refusent_saisie_manuelle_et_correction_d_assiette(base, monkeypatch):
    from app.services import assiette_correction_service as assiette
    from app.services import saisie_hh_service as hh
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS")
    formulaire = {"canal_id": "CANAL_001", "source_financiere": "SAISIE_MANUELLE", "proprietaire_id": "PROP_0001",
                  "logement_id": "LOG_0001", "date_arrivee": "2026-09-15", "date_depart": "2026-09-18",
                  "total_percu": "450.00", "code_impact": "HC", "comptabilisation": "OUI"}
    erreurs = hh.valider(formulaire, db_path=base)["erreurs"]
    refus = [e for e in erreurs if e["code"] == "MOIS_CLOTURE"]
    assert refus, erreurs
    _refus_module(refus[0]["message"], "RESERVATIONS")
    assert not [e for e in hh.valider({**formulaire, "date_arrivee": "2026-10-04", "date_depart": "2026-10-06"},
                                      db_path=base)["erreurs"] if e["code"] == "MOIS_CLOTURE"]
    monkeypatch.setattr(assiette, "preparer_formulaire", lambda *a, **k: {
        "mois": MOIS, "reservation_calc_id": "RES-X", "assiette_brute": -10.0,
        "assiette_automatique": 0.0, "taux_commission": 0.2, "commission_actuelle": 0.0})
    res = assiette.corriger("CTRL-X", nouvelle_assiette="50", justification="essai", acteur=ACTEUR,
                            db_path=base)
    assert res["ok"] is False and res["code"] == "MOIS_CLOTURE"
    _refus_module(res["message"], "RESERVATIONS")


def test_24_creances_closes_refusent_reversement_airbnb_et_reprise_de_solde(base, verrous):
    from app.services import credits_clients_service as credits
    c = _demarrer(base)
    _cloturer(base, c, "CREANCES")
    reversement = credits.creer_reversement_airbnb("PROP_TFLUX", 100, "2026-09-10", acteur=ACTEUR,
                                                   reference="AIR-1", db_path=base)
    reprise = credits.creer_reprise_solde("PROP_TFLUX", 100, "2026-09-10", acteur=ACTEUR, db_path=base)
    for res in (reversement, reprise):
        assert res["ok"] is False and res["code"] == credits.E_MOIS_CLOTURE, res
        _refus_module(res["message"], "CREANCES")
    ouvert = credits.creer_reversement_airbnb("PROP_TFLUX", 100, "2026-10-10", acteur=ACTEUR,
                                              reference="AIR-2", db_path=base)
    assert ouvert["ok"] is True, "un autre mois de Créances reste ouvert"


def test_25_un_module_clos_ne_ferme_que_son_domaine(base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, "CHARGES")
    assert verrous_cloture.module_clos(MOIS, "CHARGES", db_path=base) is True
    for autre in TOUS:
        if autre != "CHARGES":
            assert verrous_cloture.verrouille(MOIS, autre, db_path=base) is False, autre
    assert verrous_cloture.module_clos("2026-08", "CHARGES", db_path=base) is False
    _importer(base, [_mvt(12.0, date="2026-09-22")])         # Banque continue de lire Qonto
    assert _par_montant(base, 12.0)


def test_26_les_pages_des_modules_clos_restent_consultables(client, base, verrous):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    t = cm.tableau_de_bord(_recharger(base, c), db_path=base)
    assert t["nb_clos"] == 7
    for m in t["modules"]:
        reponse = client.get(m["consulter"])
        assert reponse.status_code == 200, (m["cle"], m["consulter"], reponse.status_code)
    assert client.get(f"/clotures/{c['cloture_id_opaque']}").status_code == 200
    assert client.get(f"/clotures/{c['cloture_id_opaque']}/historique").status_code == 200


# ══ 6. Clôturer le mois entier ═══════════════════════════════════════════════════════════════════

def test_27_le_mois_ne_se_cloture_pas_si_un_module_reste_ouvert(client, base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, *TOUS[:-1])
    with pytest.raises(cs.ClotureRefusee, match="tous les modules doivent d'abord l'être.*« Comptabilité »"):
        cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    page = client.post(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive",
                       data={"confirmation": "oui", "version": str(_recharger(base, c)["version"])},
                       follow_redirects=True)
    assert "tous les modules doivent d&#39;abord l&#39;être" in page.text or \
        "tous les modules doivent d'abord l'être" in page.text
    assert _recharger(base, c)["statut"] == cs.ST_EN_PREPARATION
    assert _lignes(base, "SELECT * FROM ref_cloture_mensuelle WHERE statut_mois='CLOTURE'") == []
    fiche = _fiche(client, c)
    assert 'data-testid="bouton-cloture-definitive"' not in fiche
    assert "1 module reste à clôturer : Comptabilité." in _texte(fiche)


def test_28_le_mois_ne_se_cloture_pas_si_un_bloqueur_reapparait(client, base, verrous):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    _importer(base, [_mvt(33.0, date="2026-09-10")])          # Qonto reçoit un mouvement après coup
    banque = _etat(base, c, "BANQUE")
    assert banque["etat"] == cm.ETAT_A_REVOIR and banque["peut_rouvrir"] and not banque["peut_cloturer"]
    page = _texte(_fiche(client, c))
    assert "À rouvrir" in page and "1 module reste à clôturer : Banque et caisse." in page
    assert "sont apparus depuis la clôture" in page or "est apparu depuis la clôture" in page
    with pytest.raises(cs.ClotureRefusee, match="1 contrôle bloquant"):
        cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    assert _recharger(base, c)["statut"] != cs.ST_ARCHIVEE


def test_29_le_mois_se_cloture_quand_tout_est_conforme_en_une_transaction_tracee(client, base, verrous):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    t = cm.tableau_de_bord(_recharger(base, c), db_path=base)
    assert t["peut_cloturer_mois"] is True and t["motif_mois"] == ""
    fiche = _fiche(client, c)
    assert 'data-testid="bouton-cloture-definitive"' in fiche and "Clôturer le mois" in _texte(fiche)
    confirmation = _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive").text)
    assert "Cette action clôturera définitivement le mois de septembre 2026." in confirmation
    assert "7 / 7" in confirmation
    r = client.post(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive",
                    data={"confirmation": "oui", "version": str(_recharger(base, c)["version"]),
                          "commentaire": "Septembre arrêté"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == f"/clotures/{c['cloture_id_opaque']}"
    assert _recharger(base, c)["statut"] == cs.ST_ARCHIVEE
    assert _lignes(base, "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?", MOIS) == [
        {"statut_mois": "CLOTURE"}]
    transitions = [(e["nouveau_statut"], e["acteur"]) for e in reversed(cs.historique(c["cloture_id_opaque"], base))
                   if e["type_evenement"] == "TRANSITION"]
    assert transitions[-3:] == [(cs.ST_A_VALIDER, "local"), (cs.ST_VALIDEE, "local"), (cs.ST_ARCHIVEE, "local")]
    final = next(e for e in cs.historique(c["cloture_id_opaque"], base) if e["nouveau_statut"] == cs.ST_ARCHIVEE)
    assert final["commentaire"] == "Septembre arrêté" and final["date_evenement"]
    # Une seule horloge (l'heure locale) : le mois n'apparaît jamais clôturé AVANT les modules qu'il referme.
    modules = [e["date_evenement"] for e in cs.historique(c["cloture_id_opaque"], base)
               if e["type_evenement"] == "MODULE_CLOTURE"]
    assert modules and all(re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", d) for d in modules + [final["date_evenement"]])
    assert final["date_evenement"] >= max(modules)
    page = _texte(_fiche(client, c))
    assert "Mois clôturé" in page and "Clôturé définitivement le " in page and "par local" in page
    assert "7 / 7 modules clôturés" in page
    assert not any(m["peut_rouvrir"] for m in cm.tableau_de_bord(_recharger(base, c), db_path=base)["modules"])


def test_30_un_echec_en_cours_de_cloture_n_applique_rien(base, verrous, monkeypatch):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    origine = cs._journaliser_evenement

    def panne(conn, opaque, type_evt, *a, **k):
        if type_evt == "CONTROLES":
            raise RuntimeError("panne simulée après l'archive, CLOTURE et les transitions")
        return origine(conn, opaque, type_evt, *a, **k)

    monkeypatch.setattr(cs, "_journaliser_evenement", panne)
    with pytest.raises(RuntimeError):
        cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    assert _recharger(base, c)["statut"] == cs.ST_EN_PREPARATION
    assert _lignes(base, "SELECT * FROM ref_cloture_mensuelle WHERE mois=?", MOIS) == []
    assert _lignes(base, "SELECT * FROM reservations_historique_cloture WHERE mois_cloture=?", MOIS) == []
    assert len(_lignes(base, "SELECT * FROM cloture_modules WHERE statut='CLOS'")) == 7, "les modules restent clos"


def test_31_apres_la_cloture_du_mois_ses_modules_ne_se_rouvrent_plus(client, base, verrous):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    with pytest.raises(cs.ClotureRefusee, match="ses modules ne se rouvrent plus"):
        cm.rouvrir_module(_recharger(base, c), "CHARGES", acteur=ACTEUR, justification="essai", db_path=base)
    r = _post_module(client, c, "CHARGES", "rouvrir", justification="essai")
    assert "erreur=" in r.headers["location"]
    assert "ne se rouvrent plus" in _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/modules/CHARGES/rouvrir").text)
    with pytest.raises(cs.ClotureRefusee, match="déjà clôturé définitivement"):
        cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)


# ══ 7. Rouvrir un module ═════════════════════════════════════════════════════════════════════════

def test_32_rouvrir_exige_une_justification(client, base):
    c = _demarrer(base)
    _cloturer(base, c, "CHARGES")
    for vide in ("", "   "):
        with pytest.raises(cs.ClotureRefusee, match="justification est obligatoire"):
            cm.rouvrir_module(_recharger(base, c), "CHARGES", acteur=ACTEUR, justification=vide, db_path=base)
    r = _post_module(client, c, "CHARGES", "rouvrir", justification="")
    assert "erreur=" in r.headers["location"]
    assert verrous_cloture.module_clos(MOIS, "CHARGES", db_path=base) is True


def test_33_rouvrir_leve_le_verrou_et_garde_la_trace_d_origine(client, base):
    c = _demarrer(base)
    _cloturer(base, c, "CHARGES", commentaire="Première clôture")
    avant = _lignes(base, "SELECT * FROM cloture_modules_evenements")[0]
    r = _post_module(client, c, "CHARGES", "rouvrir", justification="Facture oubliée")
    assert r.status_code == 303 and "message=" in r.headers["location"]
    assert verrous_cloture.module_clos(MOIS, "CHARGES", db_path=base) is False
    assert saisie.creer({"date_charge": "2026-09-20", "montant": 30, "categorie_charge_id": "CHG_018",
                         "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
                         "affectation_type": "GLOBAL", "refacturable": "NON", "statut_controle": "VALIDE",
                         "commentaire": "Facture oubliée"}, acteur=ACTEUR, db_path=base)["ok"], \
        "le verrou est levé : la saisie est de nouveau possible"
    ligne = _lignes(base, "SELECT * FROM cloture_modules")[0]
    assert ligne["statut"] == "ROUVERT" and ligne["justification_reouverture"] == "Facture oubliée"
    page = _texte(_fiche(client, c))
    assert re.search(r"Rouvert le \d{2}/\d{2}/\d{4} à \d{2}h\d{2}, par local\. « Facture oubliée »", page),         "la réouverture reste visible sur la carte du module"
    assert ligne["acteur_reouverture"] == "local" and ligne["date_reouverture"]
    assert (ligne["nb_clotures"], ligne["nb_reouvertures"]) == (1, 1)
    # La clôture d'origine reste intacte dans le journal ; la réouverture s'y ajoute.
    evts = _lignes(base, "SELECT * FROM cloture_modules_evenements ORDER BY id")
    assert [e["type_evenement"] for e in evts] == ["CLOTURE_MODULE", "REOUVERTURE_MODULE"]
    assert evts[0] == avant and evts[1]["commentaire"] == "Facture oubliée"
    # Re-clôture : un nouveau cycle, jamais une réécriture.
    _cloturer(base, c, "CHARGES", commentaire="Deuxième clôture")
    ligne = _lignes(base, "SELECT * FROM cloture_modules")[0]
    assert (ligne["statut"], ligne["nb_clotures"], ligne["nb_reouvertures"]) == ("CLOS", 2, 1)
    assert [e["type_evenement"] for e in _lignes(base, "SELECT * FROM cloture_modules_evenements ORDER BY id")] == [
        "CLOTURE_MODULE", "REOUVERTURE_MODULE", "CLOTURE_MODULE"]


def test_34_un_module_non_clos_ne_se_rouvre_pas(base):
    c = _demarrer(base)
    with pytest.raises(cs.ClotureRefusee, match="n'est pas clôturé : rien à rouvrir"):
        cm.rouvrir_module(c, "CHARGES", acteur=ACTEUR, justification="essai", db_path=base)


def test_35_rouvrir_un_module_d_une_cloture_validee_la_remet_en_preparation(base, verrous):
    c = _demarrer(base)
    c = cs.passer_a_valider(c, acteur=ACTEUR, db_path=base)
    c = cs.valider(c, acteur=ACTEUR, commentaire="validée avant", db_path=base)
    assert c["statut"] == cs.ST_VALIDEE
    clore_modules(base, MOIS)
    cm.rouvrir_module(_recharger(base, c), "CHARGES", acteur=ACTEUR, justification="oubli", db_path=base)
    assert _recharger(base, c)["statut"] == cs.ST_EN_PREPARATION, "un mois dont un module est rouvert n'est plus prêt"
    etapes = [e["nouveau_statut"] for e in reversed(cs.historique(c["cloture_id_opaque"], base))
              if e["type_evenement"] == "TRANSITION"][-2:]
    assert etapes == [cs.ST_ROUVERTE, cs.ST_EN_PREPARATION]


def test_36_rouvrir_la_comptabilite_rouvre_la_periode_comptable(base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, "COMPTABILITE")
    assert per.est_fermee(MOIS, base) is True, "clôturer le module, c'est clôturer la période comptable"
    cm.rouvrir_module(_recharger(base, c), "COMPTABILITE", acteur=ACTEUR, justification="écriture oubliée",
                      db_path=base)
    assert per.est_fermee(MOIS, base) is False
    assert per.charger(MOIS, base)["statut"] == per.ST_ROUVERTE
    _cloturer(base, c, "COMPTABILITE")                       # et se reclôture, par les étapes de l'automate
    assert per.est_fermee(MOIS, base) is True


def test_37_une_periode_comptable_rouverte_hors_cloture_rend_le_module_a_rouvrir(client, base, verrous):
    c = _demarrer(base)
    _tout_cloturer(base, c)
    assert per.rouvrir(MOIS, justification="correction directe", acteur=ACTEUR, db_path=base)["ok"]
    compta = _etat(base, c, "COMPTABILITE")
    assert compta["etat"] == cm.ETAT_A_REVOIR and "période comptable a été rouverte" in compta["motif_indisponible"]
    assert cm.modules_non_clos(MOIS, db_path=base) == ["COMPTABILITE"]
    with pytest.raises(cs.ClotureRefusee, match="« Comptabilité »"):
        cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    assert "À rouvrir" in _texte(_fiche(client, c))


# ══ 8. Aucune anomalie recopiée ══════════════════════════════════════════════════════════════════

def test_38_clôturer_ne_copie_aucune_anomalie(base):
    c = _demarrer(base)
    avant = _empreinte(base, "cloture_elements", "cloture_documents")
    _cloturer(base, c, "RESERVATIONS", "MENAGES")
    assert _empreinte(base, "cloture_elements", "cloture_documents") == avant
    colonnes = {r["name"] for r in _lignes(base, "PRAGMA table_info(cloture_modules)")}
    assert not {"code", "anomalie", "bloqueur", "entite", "entite_id", "severite"} & colonnes
    colonnes = {r["name"] for r in _lignes(base, "PRAGMA table_info(cloture_modules_evenements)")}
    assert not {"code", "anomalie", "bloqueur", "entite", "entite_id", "severite"} & colonnes


# ══ 9. Écrans : historique, liste, vocabulaire, accessibilité, téléphone ═════════════════════════

def test_39_l_historique_parle_metier(client, base):
    c = _demarrer(base)
    _cloturer(base, c, "CHARGES", commentaire="Charges arrêtées")
    cm.rouvrir_module(_recharger(base, c), "CHARGES", acteur=ACTEUR, justification="Facture oubliée", db_path=base)
    page = _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/historique").text)
    for attendu in ("Historique de la clôture — septembre 2026", "Clôture ouverte", "Préparation démarrée",
                    "Module clôturé", "Module rouvert", "Facture oubliée", "Charges et factures fournisseurs"):
        assert attendu in page, attendu
    for code in ("MODULE_CLOTURE", "MODULE_ROUVERT", "TRANSITION", "EN_PREPARATION", "CREATION"):
        assert code not in page, f"le code technique {code} ne s'affiche pas"


def test_40_la_liste_donne_l_avancement_de_chaque_mois(client, base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS", "MENAGES")
    page = _texte(client.get("/clotures").text)
    assert "septembre 2026" in page and "2 / 7" in page and "Clôture en cours" in page
    assert "octobre 2026" in page and "Clôture non démarrée" in page, "le mois courant est proposé aussi"
    assert "Modules clôturés" in page


def test_41_aucun_vocabulaire_technique_n_est_affiche(client, base, verrous):
    c = _demarrer(base)
    _charge_a_controler(base)
    _importer(base, [_mvt(33.0, date="2026-09-10")])
    _ecriture_proposee(base, MOIS)
    _facture_client(base, statut="BROUILLON", fid="FPR-TECH-1")
    _cloturer(base, c, "RESERVATIONS")
    pages = {
        "fiche": _fiche(client, c),
        "mois": client.get(f"/clotures/mois/{MOIS}").text,
        "confirmer": client.get(f"/clotures/{c['cloture_id_opaque']}/modules/MENAGES/cloturer").text,
        "rouvrir": client.get(f"/clotures/{c['cloture_id_opaque']}/modules/RESERVATIONS/rouvrir").text,
        "mois_confirmation": client.get(f"/clotures/{c['cloture_id_opaque']}/cloture-definitive").text,
        "historique": client.get(f"/clotures/{c['cloture_id_opaque']}/historique").text,
        "liste": client.get("/clotures").text,
    }
    interdits = ("cloture_modules", "cloture_elements", "reservations_resolues", "lot10", "lot11", "lot12",
                 "Lot10", "Lot11", "Lot12", "CHARGE_NON_VALIDEE", "BLOQUANT", "A_CONTROLER", "NON_DEMARREE",
                 "EN_PREPARATION", "ARCHIVEE", "Traceback", "sqlite", "SELECT ", "None", "undefined", "{{", "{%",
                 "FPR-TECH-1", "CLO-", "RESERVATIONS", "FACTURES_CLIENTS", "CREANCES", "COMPTABILITE")
    for nom, page in pages.items():
        texte = _texte(page)
        for mot in interdits:
            assert mot not in texte, f"« {mot} » visible dans la page {nom}"


def test_42_la_fiche_est_accessible(client, base, verrous):
    c = _demarrer(base)
    _charge_a_controler(base)
    _cloturer(base, c, "RESERVATIONS")
    fiche = _fiche(client, c)
    assert fiche.count("<h1") == 1 and re.findall(r"<h3[^>]*>", fiche).__len__() == 7, "un titre par module"
    assert 'aria-labelledby="titre-module-reservations"' in fiche
    assert re.search(r'<ol class="clo-modules" aria-label="Modules de la clôture de septembre 2026"', fiche)
    # bouton désactivé : focalisable, dit pourquoi (aria-describedby → le motif existe)
    for decrit in re.findall(r'aria-disabled="true" aria-describedby="([^"]+)"', fiche):
        assert f'id="{decrit}"' in fiche, decrit
    # lien « Traiter » : nom accessible complet (action + objet)
    assert re.search(r'class="btn btn-secondary btn-sm clo-traiter"[^>]*aria-label="[^"]+ — 1 charge à valider', fiche)
    # l'état n'est pas porté par la seule couleur : texte + icône masquée aux lecteurs d'écran
    for pastille in re.findall(r'<span class="clo-etat clo-etat--\w+"[^>]*>(.*?)</span>', fiche, flags=re.S):
        assert _texte(pastille), "chaque pastille d'état porte un texte"
        assert 'aria-hidden="true"' in pastille, "et une icône décorative"
    # les icônes sont décoratives ; aucun SVG ne porte de nom
    assert all('aria-hidden="true"' in s for s in re.findall(r"<svg[^>]*>", fiche) if "clo-icone" in s)
    # le résumé de progression est annoncé
    assert 'aria-label="Modules clôturés"' in fiche and 'aria-valuetext="1 module clôturé sur 7"' in fiche


def test_43_les_pages_de_confirmation_ont_des_champs_etiquetes(client, base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS")
    base_url = f"/clotures/{c['cloture_id_opaque']}"
    for url in (f"{base_url}/modules/MENAGES/cloturer", f"{base_url}/modules/RESERVATIONS/rouvrir"):
        page = client.get(url).text
        assert page.count("<h1") == 1
        for identifiant in re.findall(r'<(?:textarea|input)[^>]*\bid="([^"]+)"', page):
            assert f'for="{identifiant}"' in page, f"champ {identifiant} sans étiquette"
        assert "required" in page, "la confirmation / la justification sont obligatoires côté formulaire aussi"
        assert re.search(r'<button type="submit" class="btn btn-primary"', page)
        assert "Annuler" in _texte(page)
    assert "aria-describedby=\"aide-justification\"" in client.get(f"{base_url}/modules/RESERVATIONS/rouvrir").text


def test_44_la_feuille_de_style_prevoit_le_telephone_le_clavier_et_la_reduction_des_animations(client):
    r = client.get("/static/css/cloture.css")
    assert r.status_code == 200
    css = r.text
    assert "@media (max-width: 600px)" in css and "min-height: 44px" in css
    assert "prefers-reduced-motion" in css
    assert "overflow-wrap: anywhere" in css, "les noms longs ne débordent pas à 375 px"
    assert not re.search(r"\bwidth:\s*\d{4,}px", css), "aucune largeur fixe"
    assert "outline: none" not in css, "le focus visible n'est jamais retiré par cette feuille"
    page = client.get("/clotures").text
    assert '/static/css/cloture.css' in page


def test_46_le_dossier_exporte_l_etat_et_la_trace_de_chaque_module(client, base):
    c = _demarrer(base)
    _cloturer(base, c, "RESERVATIONS")
    r = client.get(f"/clotures/{c['cloture_id_opaque']}/export.csv")
    lignes = r.content.decode("utf-8").splitlines()
    assert "modules;module;etat;bloqueurs;cloture_le;cloture_par;reouvertures" in lignes
    reservations = next(l for l in lignes if l.startswith("module;Réservations;")).split(";")
    assert reservations[2:4] == ["Clôturé", "0"] and reservations[5] == ACTEUR and reservations[6] == "0"
    assert re.fullmatch(r"\d{2}/\d{2}/\d{4} à \d{2}h\d{2}", reservations[4])
    assert next(l for l in lignes if l.startswith("module;Ménages;")).split(";")[2] == "Prêt à clôturer"


# ══ 10. Migration ════════════════════════════════════════════════════════════════════════════════

def test_45_la_migration_est_additive_idempotente_et_refuse_un_mois_anterieur_a_la_v1(tmp_path):
    from app.db.connection import apply_migrations
    db = tmp_path / "m.db"
    apply_migrations(db)
    apply_migrations(db)                                  # rejouée : rien ne casse
    tables = {r["name"] for r in _lignes(db, "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"cloture_modules", "cloture_modules_evenements"} <= tables
    assert _lignes(db, "SELECT version FROM schema_migrations WHERE version='0126'") == [{"version": "0126"}]
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO parametres_societe_facturation (cle, valeur, maj_par) VALUES "
                     "('V1_ACCOUNTING_START_DATE', '2026-09-01', 'test')")
        conn.execute("INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut) "
                     "VALUES ('2026-09', 'CHARGES', 'CLO-x', 'CLOS')")
        with pytest.raises(sqlite3.DatabaseError, match="CLOTURE_AVANT_V1"):
            conn.execute("INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut) "
                         "VALUES ('2026-08', 'CHARGES', 'CLO-y', 'CLOS')")
    finally:
        conn.close()

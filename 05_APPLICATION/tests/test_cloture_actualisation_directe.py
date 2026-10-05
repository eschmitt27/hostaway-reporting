"""Le POST Clôture emprunte le DAG réel, jamais une résolution métier automatique."""
from datetime import date
from html import unescape
import re

import pytest

from app.db.connection import get_db
from app.services import cloture_actualisation_service as refresh
from app.services import cloture_modules_service as cm
from app.services import clotures_service as cs
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_moteur as moteur
from app.services import orchestrateur_service as orch
from tests.test_cloture_modules_bloqueurs import _charge_a_controler
from tests.test_flux_financiers import base, verrous  # noqa: F401

MOIS = "2026-09"


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 10, 5))


@pytest.fixture
def calculs(base):
    # Mois connu, clôture encore NON_DEMARREE : aucun module n'est fermé.
    cs.creer_ou_charger(MOIS, acteur="TEST", db_path=base)
    for n in dag.NOEUDS:
        orch.marquer_dataset(n, orch.ST_A_JOUR, db_path=base)
    return base


def perimer(db, *datasets):
    for n in datasets:
        orch.marquer_dataset(n, orch.ST_A_RECALCULER, db_path=db)


def url(module, mois=MOIS):
    return f"/clotures/mois/{mois}/modules/{module}/actualiser"


def carte(page, module):
    return unescape(re.search(
        rf'<li class="clo-module" id="module-{module.lower()}".*?(?=<li class="clo-module"|</ol>)',
        page, re.S).group())


def remplacer_services(monkeypatch, appels, *, erreur=False):
    def service(chemin, db_path):
        appels.append((chemin, orch.option_run("mois"), db_path))
        if erreur:
            raise RuntimeError('SELECT secret FROM table; C:\\secret\\app.db Traceback {"python": 1}')
        return {"ok": True}
    monkeypatch.setattr(orch, "_appeler_service", service)


def test_bouton_post_et_aucun_lien_de_navigation_actualiser(client, calculs):
    perimer(calculs, dag.MENAGES)
    page = client.get(f"/clotures/mois/{MOIS}").text
    m = carte(page, cm.MENAGES)
    opaque = cs.charger_par_mois(MOIS, calculs)["cloture_id_opaque"]
    assert f'action="/clotures/{opaque}/modules/MENAGES/actualiser"' in m and 'method="post"' in m
    assert 'data-testid="actualiser-calcul"' in m
    assert 'href="/actualisation"' not in m
    assert 'Voir l’élément' in m or "Voir l'élément" in m
    assert 'cloture_actualisation.js' in page


@pytest.mark.parametrize(("module", "perimes", "attendus"), [
    (cm.MENAGES, (dag.MENAGES,), (dag.MENAGES, dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12)),
    (cm.RESERVATIONS, (dag.LOT10,), (dag.LOT10, dag.LOT11, dag.LOT12)),
    (cm.FACTURES_CLIENTS, (dag.LOT12,), (dag.LOT12,)),
])
def test_vrai_orchestrateur_bon_module_mois_cascade_et_relecture(
        client, calculs, monkeypatch, module, perimes, attendus):
    perimer(calculs, *perimes)
    appels = []
    remplacer_services(monkeypatch, appels)
    # Aucune facture ni charge validée, aucune réouverture ni clôture automatique.
    from app.services import factures_proprietaires_service as fact
    from app.services import charges_validation_service as charges
    def interdit(*a, **k):
        pytest.fail("Une action métier a été exécutée")
    for objet, noms in ((cm, ("cloturer_module", "rouvrir_module")),
                        (fact, ("valider", "emettre")), (charges, ("valider",))):
        for nom in noms:
            monkeypatch.setattr(objet, nom, interdit)
    response = client.post(url(module), follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == (
        f"/clotures/mois/{MOIS}?actualisation=succes&module_actualise={module}#module-{module.lower()}")
    assert [a[0] for a in appels] == [dag.NOEUDS[n].service for n in attendus]
    assert all(a[1:] == (MOIS, None) for a in appels)
    page = client.get(response.headers["location"]).text
    assert "Actualisation terminée." in carte(page, module)
    assert "calcul à actualiser" not in carte(page, module)
    nb = len(appels)
    client.post(url(module))
    assert len(appels) == nb, "double POST : rien à recalculer après le succès"
    assert orch.option_run("mois") is None, "l'option ne fuit pas au run suivant"


def test_amonts_calcul_perimes_necessaires_rejoues_sans_import(client, calculs, monkeypatch):
    perimer(calculs, dag.LOT12, dag.LOT10, dag.MENAGES)
    appels = []
    remplacer_services(monkeypatch, appels)
    client.post(url(cm.FACTURES_CLIENTS))
    assert [a[0] for a in appels] == [dag.NOEUDS[n].service for n in
                                     (dag.MENAGES, dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12)]


@pytest.mark.parametrize("avec_bloqueur", [False, True])
def test_clos_technique_sans_reouverture_et_vrai_bloqueur_conserve(
        client, calculs, verrous, monkeypatch, avec_bloqueur):
    c = cs.creer_ou_charger(MOIS, acteur="TEST", db_path=calculs)
    c = cs.demarrer_preparation(c, acteur="TEST", db_path=calculs)
    cm.cloturer_module(c, cm.RESERVATIONS, acteur="TEST", db_path=calculs)
    if avec_bloqueur:
        # Une anomalie métier stable du moteur, indépendante du stale, reste active.
        from tests.test_cloture_modules_bloqueurs import _el
        monkeypatch.setattr(cs, "elements_du_mois", lambda *a, **k: [_el("VRBO_MONTANT_NON_RENSEIGNE", "RESERVATIONS")])
    perimer(calculs, dag.LOT10)
    def etat():
        return next(m for m in cm.tableau_de_bord(c, db_path=calculs)["modules"] if m["cle"] == cm.RESERVATIONS)
    assert etat()["etat"] == cm.ETAT_A_REVOIR
    conn = get_db(calculs)
    avant = [tuple(r) for r in conn.execute("SELECT * FROM cloture_modules")]
    journal = [tuple(r) for r in conn.execute("SELECT * FROM cloture_modules_evenements")]
    conn.close()
    appels = []
    remplacer_services(monkeypatch, appels)
    r = client.post(f"/clotures/{c['cloture_id_opaque']}/modules/RESERVATIONS/actualiser", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith(f"/clotures/{c['cloture_id_opaque']}?")
    assert etat()["etat"] == (cm.ETAT_A_REVOIR if avec_bloqueur else cm.ETAT_CLOS)
    assert etat()["nb_bloqueurs"] == int(avec_bloqueur)
    conn = get_db(calculs)
    assert [tuple(r) for r in conn.execute("SELECT * FROM cloture_modules")] == avant
    assert [tuple(r) for r in conn.execute("SELECT * FROM cloture_modules_evenements")] == journal
    conn.close()


def test_vrai_bloqueur_autre_module_ne_disparait_pas(client, calculs, verrous, monkeypatch):
    _charge_a_controler(calculs)
    perimer(calculs, dag.MENAGES)
    remplacer_services(monkeypatch, [])
    page = client.post(url(cm.MENAGES)).text
    assert "calcul à actualiser" not in carte(page, cm.MENAGES)
    assert "charge à valider" in carte(page, cm.CHARGES)
    assert 'data-testid="traiter"' in carte(page, cm.CHARGES)


def test_erreur_sans_fuite_et_stale_conserve(client, calculs, monkeypatch):
    perimer(calculs, dag.MENAGES)
    remplacer_services(monkeypatch, [], erreur=True)
    page = client.post(url(cm.MENAGES)).text
    m = carte(page, cm.MENAGES)
    assert "L'actualisation n'a pas pu aboutir." in m
    assert "calcul à actualiser" in m
    for mot in ("SELECT secret", "Traceback", "C:\\secret", '"python"'):
        assert mot not in page
    assert orch.verrou_actif(refresh.PORTEE, db_path=calculs) is None


@pytest.mark.parametrize("portee", [refresh.PORTEE, orch.PORTEE_GLOBALE])
def test_concurrence_refuse_second_traitement(client, calculs, monkeypatch, portee):
    perimer(calculs, dag.MENAGES)
    appels = []
    remplacer_services(monkeypatch, appels)
    orch.prendre_verrou(portee, "RUN-TEST", db_path=calculs)
    page = client.post(url(cm.MENAGES)).text
    assert "Une actualisation est déjà en cours." in page
    assert not appels
    assert "calcul à actualiser" in carte(page, cm.MENAGES)


def test_partiel_et_amont_source_en_echec_ne_sont_pas_un_faux_succes(client, calculs, monkeypatch):
    perimer(calculs, dag.MENAGES)
    orch.marquer_dataset(dag.HOSTAWAY_CLEANING_TASKS, orch.ST_ECHEC, db_path=calculs)
    appels = []
    remplacer_services(monkeypatch, appels)
    page = client.post(url(cm.MENAGES)).text
    assert "L'actualisation n'a pas pu aboutir." in carte(page, cm.MENAGES)
    assert not appels, "aucun calcul sur une entrée en échec, aucun import automatique"


@pytest.mark.parametrize("module", [cm.CHARGES, cm.BANQUE, cm.CREANCES, cm.COMPTABILITE])
def test_module_sans_refresh_pas_de_faux_bouton(client, calculs, monkeypatch, module):
    appels = []
    remplacer_services(monkeypatch, appels)
    page = client.get(f"/clotures/mois/{MOIS}").text
    assert 'data-testid="actualiser-calcul"' not in carte(page, module)
    assert client.post(url(module), follow_redirects=False).headers["location"].find("actualisation=indisponible") > 0
    assert not appels


@pytest.mark.parametrize("chemin", [url("INCONNU"), url(cm.MENAGES, "2026-99"), url(cm.MENAGES, "2099-01"),
                                    "/clotures/INCONNUE/modules/MENAGES/actualiser"])
def test_controle_serveur_module_et_mois_connus(client, calculs, chemin):
    assert client.post(chemin, follow_redirects=False).status_code == 404


def test_get_impossible_et_reponse_fetch_retour_meme_carte(client, calculs, monkeypatch):
    perimer(calculs, dag.MENAGES)
    assert client.get(url(cm.MENAGES)).status_code == 405
    remplacer_services(monkeypatch, [])
    r = client.post(url(cm.MENAGES), headers={"Accept": "application/json"})
    assert r.status_code == 200 and r.json()["retour"].endswith("#module-menages")


def test_mois_courant_sans_cloture_refresh_ne_demarre_rien(client, calculs, monkeypatch):
    mois = "2026-10"
    perimer(calculs, dag.MENAGES)
    remplacer_services(monkeypatch, [])
    page = client.post(url(cm.MENAGES, mois)).text
    assert "Actualisation terminée." in page
    assert cs.charger_par_mois(mois, calculs) is None


def test_option_mois_rejoint_vrai_moteur_menages_et_ne_change_pas_les_autres_signatures(calculs, monkeypatch):
    perimer(calculs, dag.MENAGES)
    appels = []
    monkeypatch.setattr(moteur, "_referentiel_intervenants_importe", lambda p: True)
    def executer(script, *, db_path, arguments):
        appels.append((script, arguments))
        return {"ok": True}
    monkeypatch.setattr(moteur, "executer", executer)
    from app.services import flux_unifie_service as flux
    from app.services import controles_lot11_service as controles
    from app.services import lot12_prefactures_service as prefactures
    for obj, nom in ((flux, "construire"), (controles, "construire"), (prefactures, "construire")):
        monkeypatch.setattr(obj, nom, lambda *, db_path: {"ok": True})
    monkeypatch.setattr(moteur, "executer_lot10", lambda *, db_path: {"ok": True})
    assert refresh.actualiser(MOIS, cm.MENAGES, db_path=calculs) == "succes"
    assert len(appels) == 3
    assert all(args[-2:] == ("--mois", MOIS) for _, args in appels)


def test_dataset_sans_service_et_en_cours_ne_proposent_pas_actualiser(client, calculs, monkeypatch):
    from dataclasses import replace
    perimer(calculs, dag.MENAGES)
    monkeypatch.setitem(dag.NOEUDS, dag.MENAGES, replace(dag.NOEUDS[dag.MENAGES], service=None))
    assert 'data-testid="actualiser-calcul"' not in carte(client.get(f"/clotures/mois/{MOIS}").text, cm.MENAGES)
    orch.marquer_dataset(dag.MENAGES, orch.ST_EN_COURS, db_path=calculs)
    assert 'data-testid="actualiser-calcul"' not in carte(client.get(f"/clotures/mois/{MOIS}").text, cm.MENAGES)


def test_message_metier_sur_code_connu_sans_message_brut(client, calculs, monkeypatch):
    perimer(calculs, dag.MENAGES)
    def service(chemin, db_path):
        return {"ok": False, "code": "MENAGES_REFERENTIEL_ABSENT", "message": "ref_intervenants SELECT secret"}
    monkeypatch.setattr(orch, "_appeler_service", service)
    page = client.post(url(cm.MENAGES)).text
    assert "Le référentiel des intervenants de ménage doit d'abord être renseigné." in carte(page, cm.MENAGES)
    assert "SELECT secret" not in page and "ref_intervenants" not in page

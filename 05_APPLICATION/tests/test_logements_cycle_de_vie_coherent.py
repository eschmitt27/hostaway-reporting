"""Cycle de vie d'un logement : ACTIF → INACTIF → ACTIF doit retrouver ses séjours facturables.

CAUSE RACINE (constatée le 2026-10-04 sur un logement réactivé) : « Aucun élément facturable par le
moteur sur cette période » alors que les séjours existaient. Le moteur ne lit pas `actif` : il range
un séjour chez le propriétaire d'une PÉRIODE DE GESTION qui couvre ses dates. L'archivage ferme cette
période ; l'activation GÉNÉRIQUE de l'administration du référentiel ne rebasculait que `actif`. Le
logement redevenait « actif » sans propriétaire : ses séjours étaient exclus (HORS_PERIODE_GESTION),
sans un message. Second défaut, indépendant : depuis le 2026-09-18, les formulaires d'archivage et de
réactivation de la fiche postaient leur date sous un mauvais nom de champ — le parcours dédié était
inutilisable, ce qui poussait vers l'activation générique.

Ces tests verrouillent le comportement GÉNÉRIQUE — aucun identifiant de logement réel.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import logements_gestion_service as svc
from app.services import referentiel_admin_service as adm
from test_logements import construire_referentiel

_TRAVAIL = str(Path(__file__).resolve().parents[2] / "02_TRAVAIL")
if _TRAVAIL not in sys.path:
    sys.path.insert(0, _TRAVAIL)
from lib_ref_history import resolve_management_period  # noqa: E402


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _gestion(db, logement):
    return [g for g in fx.lignes(db, "ref_gestion_logements_hist") if g["logement_id"] == logement]


def _fiche(db, logement):
    return next(l for l in fx.lignes(db, "ref_logements") if l["logement_id"] == logement)


def _casser_comme_l_activation_generique(db, logement):
    """Reproduit l'ancien comportement : `actif` repasse à OUI, rien d'autre ne bouge."""
    from app.db.connection import get_db
    conn = get_db(db)
    try:
        conn.execute("UPDATE ref_logements SET actif='OUI' WHERE logement_id=?", (logement,))
        conn.commit()
    finally:
        conn.close()


# ── Cohérence ─────────────────────────────────────────────────────────────────────────────────────

def test_un_logement_actif_et_gere_est_coherent(ref):
    etat = svc.coherence("LOG_A1", db_path=ref)
    assert etat["status"] == "OK" and etat["coherent"] is True
    assert etat["reactivable"] is False


def test_un_logement_archive_normalement_est_coherent(ref):
    etat = svc.coherence("LOG_INACTIF", db_path=ref)
    assert etat["coherent"] is True
    assert etat["reactivable"] is True


def test_actif_sans_periode_ouverte_est_une_incoherence(ref):
    _casser_comme_l_activation_generique(ref, "LOG_INACTIF")
    etat = svc.coherence("LOG_INACTIF", db_path=ref)
    codes = {i["code"] for i in etat["incoherences"]}
    assert etat["coherent"] is False
    assert svc.INC_ACTIF_SANS_GESTION in codes          # plus de propriétaire pour le moteur
    assert svc.INC_ACTIF_NON_GERE in codes              # statut de parc resté RETIRE
    assert etat["reactivable"] is True                  # et il se répare par une réactivation
    assert [i["logement_id"] for i in svc.incoherences(db_path=ref)] == ["LOG_INACTIF"]


def test_inactif_avec_periode_ouverte_est_une_incoherence(ref):
    from app.db.connection import get_db
    conn = get_db(ref)
    try:
        conn.execute("UPDATE ref_logements SET actif='NON' WHERE logement_id='LOG_A1'")
        conn.commit()
    finally:
        conn.close()
    codes = {i["code"] for i in svc.coherence("LOG_A1", db_path=ref)["incoherences"]}
    assert codes == {svc.INC_INACTIF_AVEC_GESTION}


def test_un_logement_technique_est_hors_regle(ref):
    from app.db.connection import get_db
    conn = get_db(ref)
    try:
        conn.execute("INSERT INTO ref_logements (logement_id, nom_court, actif, statut_parc, import_id) "
                     "VALUES ('LOG_TECH', 'Technique', 'OUI', 'HORS_PARC_TECHNIQUE', 'IMP-TEST')")
        conn.commit()
    finally:
        conn.close()
    etat = svc.coherence("LOG_TECH", db_path=ref)
    assert etat["coherent"] is True and etat["reactivable"] is False
    # Réactiver un logement technique n'a aucun sens : jamais de période de gestion fabriquée.
    res = svc.reactiver("LOG_TECH", "2026-07-01", "PROP_A", db_path=ref)
    assert res["ok"] is False and res["code"] == svc.E_DEJA_ACTIF
    assert _gestion(ref, "LOG_TECH") == []


# ── Réactivation : sans interruption par défaut, et réparation d'un logement incohérent ──────────

def test_reprise_par_defaut_prolonge_la_derniere_periode(ref):
    reprise = svc.reprise_par_defaut("LOG_INACTIF", db_path=ref)
    assert reprise == {"proprietaire_id": "PROP_C", "date_debut": "2026-06-01",
                       "fin_precedente": "2026-05-31"}


def test_pas_de_reprise_par_defaut_sans_periode_close(ref):
    assert svc.reprise_par_defaut("LOG_A1", db_path=ref) is None


def test_reactiver_sans_date_ni_proprietaire_reprend_sans_interruption(ref):
    res = svc.reactiver("LOG_INACTIF", db_path=ref)
    assert res["ok"], res
    assert res["proprietaire_id"] == "PROP_C" and res["date_debut"] == "2026-06-01"
    assert res["sans_interruption"] is True and res["periode_ouverte"] is True
    fiche = _fiche(ref, "LOG_INACTIF")
    assert fiche["actif"] == "OUI" and fiche["statut_parc"] == "GERE"
    ouvertes = [g for g in _gestion(ref, "LOG_INACTIF") if not g["date_fin"]]
    assert len(ouvertes) == 1 and ouvertes[0]["date_debut"] == "2026-06-01"
    assert svc.coherence("LOG_INACTIF", db_path=ref)["coherent"] is True


def test_reactiver_sans_date_et_sans_historique_exige_une_date(ref):
    res = svc.reactiver("LOG_A1", db_path=ref)
    assert res["ok"] is False        # ni date par défaut ni logement à réactiver


def test_reactiver_repare_un_logement_actif_sans_gestion(ref):
    """Le cas constaté : `actif` repassé à OUI sans reprise de gestion."""
    _casser_comme_l_activation_generique(ref, "LOG_INACTIF")
    assert svc.coherence("LOG_INACTIF", db_path=ref)["coherent"] is False
    res = svc.reactiver("LOG_INACTIF", db_path=ref)
    assert res["ok"], res
    assert svc.coherence("LOG_INACTIF", db_path=ref)["coherent"] is True
    assert _fiche(ref, "LOG_INACTIF")["statut_parc"] == "GERE"


def test_reactiver_ne_refuse_que_un_logement_reellement_actif_et_gere(ref):
    res = svc.reactiver("LOG_A1", "2026-07-01", "PROP_A", db_path=ref)
    assert res["ok"] is False and res["code"] == svc.E_DEJA_ACTIF


def test_reactiver_une_pause_reelle_avec_date_explicite_laisse_un_trou(ref):
    res = svc.reactiver("LOG_INACTIF", "2026-08-01", "PROP_B", db_path=ref)
    assert res["ok"] and res["sans_interruption"] is False
    rows = _gestion(ref, "LOG_INACTIF")
    # Le trou 01/06 → 31/07 reste non géré : aucun séjour n'y est rattaché.
    assert resolve_management_period(rows, logement_id="LOG_INACTIF",
                                     date_arrivee="2026-06-15", date_depart="2026-06-18").status == "MISSING"
    assert resolve_management_period(rows, logement_id="LOG_INACTIF",
                                     date_arrivee="2026-08-15", date_depart="2026-08-18").value == "PROP_B"


def test_reactiver_repare_aussi_un_statut_de_parc_resté_retire_avec_gestion_ouverte(ref):
    from app.db.connection import get_db
    conn = get_db(ref)
    try:
        conn.execute("UPDATE ref_logements SET statut_parc='RETIRE' WHERE logement_id='LOG_A1'")
        conn.commit()
    finally:
        conn.close()
    avant = len(_gestion(ref, "LOG_A1"))
    res = svc.reactiver("LOG_A1", "2026-07-01", "PROP_A", db_path=ref)
    assert res["ok"], res
    assert res["periode_ouverte"] is False               # une période était déjà ouverte : pas de doublon
    assert len(_gestion(ref, "LOG_A1")) == avant
    assert _fiche(ref, "LOG_A1")["statut_parc"] == "GERE"


# ── ACTIF → INACTIF → ACTIF : les séjours redeviennent attribuables ────────────────────────────

def test_cycle_complet_archivage_puis_reprise_sans_interruption(ref):
    """Archivage au 31/07, réactivation par défaut : tous les séjours, y compris celui à cheval sur la
    frontière, se rattachent au propriétaire — rien n'est orphelin."""
    assert svc.archiver("LOG_A1", "2026-07-31", justification="test", db_path=ref)["ok"]
    assert svc.coherence("LOG_A1", db_path=ref)["coherent"] is True
    rows = _gestion(ref, "LOG_A1")
    # Pendant l'archivage, un séjour d'août n'a pas de propriétaire…
    assert resolve_management_period(rows, logement_id="LOG_A1", date_arrivee="2026-08-10",
                                     date_depart="2026-08-12").status == "MISSING"

    assert svc.reactiver("LOG_A1", justification="archivage par erreur", db_path=ref)["ok"]
    rows = _gestion(ref, "LOG_A1")
    assert len(rows) == 2                                # ancienne période conservée + nouvelle
    avant = resolve_management_period(rows, logement_id="LOG_A1", date_arrivee="2026-07-20",
                                      date_depart="2026-07-25")
    chevauche = resolve_management_period(rows, logement_id="LOG_A1", date_arrivee="2026-07-31",
                                          date_depart="2026-08-03")
    apres = resolve_management_period(rows, logement_id="LOG_A1", date_arrivee="2026-08-10",
                                      date_depart="2026-08-12")
    assert (avant.status, avant.value) == ("OK", "PROP_A")
    assert (chevauche.status, chevauche.value) == ("OK", "PROP_A")
    assert (apres.status, apres.value) == ("OK", "PROP_A")
    fiche = _fiche(ref, "LOG_A1")
    assert fiche["actif"] == "OUI" and fiche["statut_parc"] == "GERE"


def test_l_ancienne_periode_n_est_jamais_reecrite(ref):
    svc.archiver("LOG_A1", "2026-07-31", db_path=ref)
    svc.reactiver("LOG_A1", db_path=ref)
    ancienne = [g for g in _gestion(ref, "LOG_A1") if g["date_fin"]][0]
    assert ancienne["date_fin"] == "2026-07-31" and ancienne["statut_gestion"] == "RETIRE"


# ── Administration générique : un logement ne s'active pas d'un clic ──────────────────────────────

def test_l_activation_generique_d_un_logement_est_refusee_et_ne_change_rien(ref):
    for demande in (True, False):
        res = adm.basculer_activation("ref_logements", "LOG_INACTIF", demande, db_path=ref)
        assert res["ok"] is False and res["code"] == adm.E_LOGEMENT_CYCLE_DEDIE
    fiche = _fiche(ref, "LOG_INACTIF")
    assert fiche["actif"] == "NON" and fiche["statut_parc"] == "RETIRE"
    assert svc.coherence("LOG_INACTIF", db_path=ref)["coherent"] is True


def test_l_activation_generique_des_autres_referentiels_fonctionne_toujours(ref):
    res = adm.basculer_activation("ref_proprietaires", "PROP_B", False, db_path=ref)
    assert res["ok"] is True


def test_l_edition_libre_ne_peut_pas_modifier_actif_ni_statut_parc(ref):
    for champ, valeur in (("actif", "OUI"), ("statut_parc", "GERE")):
        res = adm.modifier_ligne("ref_logements", "LOG_INACTIF", {champ: valeur}, db_path=ref)
        assert res["ok"] is False and res["code"] == adm.E_LOGEMENT_CYCLE_DEDIE
    assert _fiche(ref, "LOG_INACTIF")["actif"] == "NON"


def test_l_edition_libre_accepte_les_valeurs_inchangees_et_les_autres_champs(ref):
    """Le formulaire renvoie TOUTES les colonnes : une valeur inchangée ne doit rien refuser."""
    res = adm.modifier_ligne("ref_logements", "LOG_INACTIF",
                             {"actif": "NON", "statut_parc": "RETIRE", "nom_court": "Renommé"},
                             db_path=ref)
    assert res["ok"] is True
    assert _fiche(ref, "LOG_INACTIF")["nom_court"] == "Renommé"


def test_la_creation_generique_renseigne_toujours_actif_et_statut_parc(ref):
    res = adm.creer_ligne("ref_logements", {"logement_id": "LOG_NEUF", "nom_court": "Neuf",
                                            "actif": "OUI", "statut_parc": "GERE"}, db_path=ref)
    assert res["ok"] is True
    assert _fiche(ref, "LOG_NEUF")["statut_parc"] == "GERE"


# ── Routes et gabarits ────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def ref_http(tmp_path, monkeypatch):
    db = construire_referentiel(
        tmp_path,
        logements=[
            {"logement_id": "LOG_R1", "nom_logement_officiel": "Fictif R1", "nom_court": "R1",
             "adresse": "1 rue", "ville": "RECETTE", "type_logement_id": "TYPE_001",
             "sur_hostaway": "OUI", "actif": "OUI", "statut_parc": "RETIRE",
             "forfait_logiciel_consommables_mensuel": "0"},
            {"logement_id": "LOG_R2", "nom_logement_officiel": "Fictif R2", "nom_court": "R2",
             "adresse": "2 rue", "ville": "RECETTE", "type_logement_id": "TYPE_001",
             "sur_hostaway": "OUI", "actif": "OUI", "statut_parc": "GERE",
             "forfait_logiciel_consommables_mensuel": "0"}],
        gestion=[
            {"gestion_id": "GST_R1", "logement_id": "LOG_R1", "proprietaire_id": "PROP_A",
             "date_debut": "2025-01-01", "date_fin": "2026-09-01", "statut_gestion": "RETIRE",
             "source": "FICTIF"},
            {"gestion_id": "GST_R2", "logement_id": "LOG_R2", "proprietaire_id": "PROP_A",
             "date_debut": "2025-01-01", "date_fin": "", "statut_gestion": "ACTIF", "source": "FICTIF"}],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
        taux=[],
        proprietaires=[{"proprietaire_id": "PROP_A", "nom_proprietaire": "Nom A", "actif": "OUI"}])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return db


def test_la_fiche_signale_le_logement_actif_sans_gestion_et_propose_de_la_retablir(client, ref_http):
    html = client.get("/logements/LOG_R1").text
    assert 'data-testid="alerte-incoherence-logement"' in html
    assert "Gestion à rétablir" in html
    assert "Rétablir la gestion" in html
    # Proposition : reprise sans interruption — même propriétaire, lendemain de la fin précédente.
    assert 'data-testid="reprise-sans-interruption"' in html
    assert re.search(r'name="date_debut"[^>]*value="2026-09-02"', html) or \
        re.search(r'value="2026-09-02"[^>]*name="date_debut"', html)
    assert re.search(r'<option value="PROP_A"\s+selected', html)


def test_la_fiche_d_un_logement_coherent_ne_montre_aucune_alerte(client, ref_http):
    html = client.get("/logements/LOG_R2").text
    assert 'data-testid="alerte-incoherence-logement"' not in html
    assert "Archiver ce logement" in html and "Rétablir la gestion" not in html


def test_les_formulaires_du_cycle_de_vie_postent_leur_date_sous_le_bon_nom(client, ref_http):
    """Régression du 2026-09-18 : le champ s'appelait « Date de reprise de gestion », la route ne
    recevait aucune date. Chaque formulaire doit poster `date_debut` / `date_fin`."""
    for logement in ("LOG_R1", "LOG_R2"):
        html = client.get(f"/logements/{logement}").text
        formulaires = dict(re.findall(r'<form[^>]*action="/logements/\w+/([\w-]+)"[^>]*>(.*?)</form>',
                                      html, re.S))
        attendus = {"changer-proprietaire": "date_debut", "changer-taux-commission": "date_debut",
                    "archiver": "date_fin"}
        if logement == "LOG_R1":
            attendus["reactiver"] = "date_debut"
        for action, champ in attendus.items():
            noms = re.findall(r'<(?:input|select|textarea)[^>]*\bname="([^"]+)"', formulaires[action])
            assert champ in noms, (logement, action, noms)
            assert not [n for n in noms if " " in n], (logement, action, noms)


def test_reactivation_par_la_route_sans_date_reprend_sans_interruption(client, ref_http):
    # La date proposée (02/09/2026) est rétroactive : la justification reste exigée côté serveur.
    r = client.post("/logements/LOG_R1/reactiver", data={"proprietaire_id": "", "date_debut": ""},
                    follow_redirects=False)
    assert r.status_code == 303
    assert "justification" in client.get(r.headers["location"]).text.lower()

    r = client.post("/logements/LOG_R1/reactiver",
                    data={"justification": "archivage par erreur"}, follow_redirects=False)
    assert r.status_code == 303
    page = client.get(r.headers["location"]).text
    assert "réactivé" in page and "actualiser" in page
    html = client.get("/logements/LOG_R1").text
    assert 'data-testid="alerte-incoherence-logement"' not in html
    rows = [g for g in fx.lignes(ref_http, "ref_gestion_logements_hist") if g["logement_id"] == "LOG_R1"]
    assert sorted((g["date_debut"], g["date_fin"]) for g in rows) == [
        ("2025-01-01", "2026-09-01"), ("2026-09-02", "")]


def test_l_administration_generique_remplace_le_bouton_d_activation_par_un_lien_vers_la_fiche(
        client, ref_http):
    html = client.get("/administration/referentiels/ref_logements").text
    assert 'action="/administration/referentiels/ref_logements/activation"' not in html
    assert 'href="/logements/LOG_R1"' in html
    # La même page garde le bouton pour les référentiels qui s'activent réellement.
    props = client.get("/administration/referentiels/ref_proprietaires").text
    assert 'action="/administration/referentiels/ref_proprietaires/activation"' in props


def test_la_route_d_activation_generique_refuse_un_logement(client, ref_http):
    r = client.post("/administration/referentiels/ref_logements/activation",
                    data={"cle": "LOG_R1", "actif": "NON"}, follow_redirects=False)
    assert r.status_code == 303
    assert "fiche" in client.get(r.headers["location"]).text.lower()
    assert _fiche(ref_http, "LOG_R1")["actif"] == "OUI"

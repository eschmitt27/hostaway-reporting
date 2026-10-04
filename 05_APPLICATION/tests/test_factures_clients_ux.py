"""Factures clients — garde-fous de l'interface (mission UX / accessibilité, 2026-10-03).

Ce que ces tests figent :
  · le parcours reste lisible : fil d'étapes, sommaire, ancres utilisées par les redirections ;
  · chaque champ de saisie porte un nom accessible (libellé ou aria-label) ;
  · aucun code technique n'est présenté comme texte à l'utilisateur ;
  · le crédit client et les avoirs sont présentés tels que le service les calcule — l'affichage
    n'impute rien, et le compte du crédit (419100 / 419700) est celui de la règle canonique ;
  · les formulaires gardent leurs routes et leurs noms de champs (l'UX ne casse pas le métier).

Base temporaire, données fictives.
"""
from __future__ import annotations

import html
import re

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import credits_clients_service as credits
from app.services import factures_proprietaires_service as svc

PID = "PROP_UX"
EMETTEUR = {"nom": "Conciergerie UX", "adresse": "1 rue UX", "siret": "00000000000000"}
DEST = {"nom": "Client UX", "adresse": "2 rue UX"}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups", raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_NOM", "Conciergerie UX", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_ADRESSE", "1 rue UX", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_SIRET", "00000000000000", raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-UX','2026-09-01T00:00:00','x','x','IMPORTE',1,1)")
    conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                 "prenom_proprietaire, actif, import_id) VALUES (?, 'Martin', 'Didier', 'OUI', "
                 "'IMP-UX')", (PID,))
    conn.commit()
    conn.close()
    from app.main import app as application
    return TestClient(application), chemin


def _brouillon(db, montant=200.0, logement="LOG_UX", mois="2026-09"):
    return svc.creer({"mois": mois, "proprietaire_id": PID, "logement_id": logement,
                      "source_calcul": f"PREF-{logement}-{mois}",
                      "COMMISSION_CONCIERGERIE": montant, "montant_du_conciergerie": montant},
                     db_path=db)["facture_id_opaque"]


def _emettre(db, fid):
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    f = svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture="2026-10-03",
                    db_path=db)
    compta.comptabiliser_facture_emise(f, acteur="test", db_path=db)
    return f


def _texte_visible(source: str) -> str:
    sans_script = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", source)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"(?s)<[^>]+>", " ", sans_script)))


def _nb_imputations(db) -> int:
    conn = get_db(db)
    try:
        return conn.execute("SELECT COUNT(*) FROM imputations_airbnb").fetchone()[0]
    finally:
        conn.close()


# ── Structure et navigation ─────────────────────────────────────────────────────────────────────

def test_fiche_brouillon_etapes_sommaire_et_ancres(env):
    client, db = env
    fid = _brouillon(db)
    page = client.get(f"/factures-proprietaires/{fid}")
    assert page.status_code == 200
    soup = BeautifulSoup(page.text, "html.parser")

    etapes = soup.select('[data-testid="etapes-facture"] li')
    assert [e.get("aria-current") for e in etapes] == ["step", None, None]

    # Les ancres des redirections serveur (#lignes-facturees, #reservations, #reglement) et
    # celles du sommaire existent toutes dans la page.
    ancres = {a["href"][1:] for a in soup.select(".fc-sommaire a[href^='#']")}
    for ancre in ancres | {"lignes-facturees", "reservations", "reglement", "montant-du"}:
        assert soup.find(id=ancre) is not None, ancre
    assert soup.select_one("nav.fc-ariane a[href='/factures-proprietaires']")


def test_statut_lisible_et_code_conserve_pour_les_styles(env):
    client, db = env
    page = client.get(f"/factures-proprietaires/{_brouillon(db)}").text
    soup = BeautifulSoup(page, "html.parser")
    assert soup.select_one(".fc-fiche")["data-statut"] == "BROUILLON"
    assert "Brouillon" in soup.select_one("h1").get_text()


# ── Accessibilité ───────────────────────────────────────────────────────────────────────────────

def _champs_sans_nom(source: str) -> list[str]:
    soup = BeautifulSoup(source, "html.parser")
    ecran = soup.select_one(".ecran") or soup
    fautifs = []
    for ctl in ecran.select("input, select, textarea"):
        if ctl.name == "input" and ctl.get("type") in ("hidden", "submit", "button"):
            continue
        if ctl.get("aria-label") or ctl.get("aria-labelledby"):
            continue
        if ctl.find_parent("label"):
            continue
        if ctl.get("id") and soup.select_one(f'label[for="{ctl["id"]}"]'):
            continue
        fautifs.append(f'{ctl.name}[name={ctl.get("name")}]')
    return fautifs


@pytest.mark.parametrize("etat", ["brouillon", "valide", "emise"])
def test_chaque_champ_de_la_fiche_a_un_nom_accessible(env, etat):
    client, db = env
    fid = _brouillon(db)
    if etat in ("valide", "emise"):
        svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    if etat == "emise":
        svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture="2026-10-03",
                    db_path=db)
    page = client.get(f"/factures-proprietaires/{fid}")
    assert page.status_code == 200
    assert _champs_sans_nom(page.text) == []


@pytest.mark.parametrize("route", ["/factures-proprietaires", "/factures-proprietaires/nouvelle",
                                   "/factures-proprietaires/proposer?mois=2026-09"])
def test_chaque_champ_des_ecrans_de_creation_a_un_nom_accessible(env, route):
    client, _ = env
    page = client.get(route)
    assert page.status_code == 200
    assert _champs_sans_nom(page.text) == []


def test_refus_annonce_et_focalisable(env):
    client, db = env
    fid = _brouillon(db)
    r = client.post(f"/factures-proprietaires/{fid}/lignes/ajouter",
                    data={"type_ligne": "COMMISSION_CONCIERGERIE", "libelle": "x", "montant": "0"})
    assert r.status_code == 422
    erreur = BeautifulSoup(r.text, "html.parser").select_one("[data-fc-erreur]")
    assert erreur is not None
    assert erreur["role"] == "alert" and erreur["tabindex"] == "-1"
    assert "Ligne non ajout" in erreur.get_text()


def test_aucun_code_technique_dans_le_texte_lu(env):
    client, db = env
    fid = _brouillon(db)
    texte = _texte_visible(client.get(f"/factures-proprietaires/{fid}").text)
    for code in ("FACTURE_PROPRIETAIRE_IDENTITE_INCOMPLETE", "SANS_OBJET", "PRETE_A_EMETTRE",
                 "COMMISSION_CONCIERGERIE", "CALCULEE", "PRESTATION_DE_SERVICES",
                 "SOCIETE_ADRESSE", "BROUILLON"):
        assert code not in texte, code


def test_ressources_de_presentation_servies(env):
    client, _ = env
    for chemin in ("/static/css/factures_clients.css", "/static/js/factures_clients.js"):
        assert client.get(chemin).status_code == 200, chemin


# ── Crédits clients et avoirs : affichés, jamais imputés par l'affichage ──────────────────────

def test_brouillon_annonce_le_credit_disponible_sans_l_imputer(env):
    client, db = env
    assert credits.creer_reprise_solde(PID, 300, "2026-09-01", acteur="t", db_path=db)["ok"]
    fid = _brouillon(db, 140.0)
    avant = _nb_imputations(db)
    page = client.get(f"/factures-proprietaires/{fid}").text
    bloc = BeautifulSoup(page, "html.parser").select_one('[data-testid="credit-disponible"]')
    assert bloc is not None and "300.00 €" in bloc.get_text()
    assert 'data-testid="sans-impact-compte-client"' in page
    assert _nb_imputations(db) == avant, "afficher une fiche n'impute aucun crédit"


def test_emise_affiche_le_credit_utilise_et_le_compte_419700(env):
    client, db = env
    assert credits.creer_reprise_solde(PID, 300, "2026-09-01", acteur="t", db_path=db)["ok"]
    fid = _brouillon(db, 140.0)
    _emettre(db, fid)
    texte = _texte_visible(client.get(f"/factures-proprietaires/{fid}").text)
    assert "Crédit client utilisé" in texte
    assert "Crédit d'origine constatée" in texte
    # Solde repris = « autres avoirs » 4197 : la fiche ne dit plus 419100 pour ce crédit.
    assert "419700 → 411000" in texte and "419100 → 411000" not in texte
    assert "Net à payer" in texte


def test_avoir_designe_sa_facture_d_origine_par_son_numero(env):
    client, db = env
    fid = _brouillon(db)
    emise = _emettre(db, fid)
    avoir = svc.creer_avoir(fid, motif="Erreur de montant", acteur="t", db_path=db)
    page = client.get(f"/factures-proprietaires/{avoir['facture_id_opaque']}").text
    soup = BeautifulSoup(page, "html.parser")
    meta = soup.select_one(".fc-meta").get_text(" ")
    assert emise["numero_facture"] in meta and fid not in meta
    assert "Montant de l'avoir" in _texte_visible(page)


# ── Contrat des formulaires : l'interface n'a changé ni les routes ni les champs ─────────────────

def test_formulaires_du_brouillon_inchanges(env):
    client, db = env
    fid = _brouillon(db)
    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}").text, "html.parser")
    actions = {f["action"].replace(fid, "{id}") for f in soup.select("form[method=post]")}
    for attendu in ("/factures-proprietaires/{id}/valider", "/factures-proprietaires/{id}/extra",
                    "/factures-proprietaires/{id}/reduction",
                    "/factures-proprietaires/{id}/lignes/ajouter",
                    "/factures-proprietaires/{id}/recharger",
                    "/factures-proprietaires/{id}/type-client"):
        assert attendu in actions, attendu
    noms = {c.get("name") for c in soup.select("input, select")}
    assert {"libelle", "montant", "nature", "motif", "type_ligne", "code_impact",
            "categorie_charge_id", "date_charge", "refacturable", "type_client"} <= noms


def test_formulaire_d_emission_inchange(env):
    client, db = env
    fid = _brouillon(db)
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}").text, "html.parser")
    form = soup.select_one(f'form[action="/factures-proprietaires/{fid}/emettre"]')
    assert form is not None
    assert form.select_one('input[name="date_facture"][type="date"]')
    assert {r["value"] for r in form.select('input[name="comptabilite"]')} == {"EN_COMPTA",
                                                                               "HORS_COMPTA"}
    assert form.select_one('input[name="comptabilite"][value="EN_COMPTA"]').has_attr("checked")
    assert form.select_one('input[name="motif_hors_compta"]')
    # Le parcours complet passe toujours par cette route, avec ces champs.
    r = client.post(f"/factures-proprietaires/{fid}/emettre",
                    data={"date_facture": "2026-10-03", "comptabilite": "EN_COMPTA"},
                    follow_redirects=False)
    assert r.status_code == 303
    assert svc.lire(fid, db_path=db)["statut"] == svc.ST_EMIS


def test_liste_filtre_proprietaire_visible_et_conserve(env):
    client, db = env
    _brouillon(db)
    page = client.get(f"/factures-proprietaires?proprietaire_id={PID}").text
    soup = BeautifulSoup(page, "html.parser")
    assert "Didier" in soup.select_one('[data-testid="filtre-proprietaire"]').get_text()
    assert soup.select_one(f'form.ec-filtres input[type=hidden][name=proprietaire_id][value="{PID}"]')
    # Un brouillon sans numéro reste un lien lisible, jamais « — ».
    lien = soup.select_one("tbody td.fi-cle a")
    assert lien.get_text(strip=True) == "sans numéro"

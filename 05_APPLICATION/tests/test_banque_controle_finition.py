"""APP-4B — Mission de finition (20 points) : origine proposition, bouton, dates, formulaire,
libellés métier, statut « En cours », intégrité.

Aucune écriture bancaire réelle. Réel intact. Flags False.
"""
import hashlib
import re
from pathlib import Path

import pytest

import app.config as cfg
from app.readers.banques_reader import date_affichage, datetime_affichage
from app.services import banques_controle_service as ctrl
from app.services import banques_controle_writer as writer

REAL_BANQUE = Path(cfg.MASTER_BANQUE)
banque_requise = pytest.mark.skipif(not REAL_BANQUE.exists(), reason="BANQUE_LOT8_IMPORT.xlsx absent")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def _isoler_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "BANQUE_CONTROLE_WORKSPACE", tmp_path / "banque_ws")
    from app.services import snapshot_service
    monkeypatch.setattr(snapshot_service, "SNAPSHOTS_DIR", tmp_path / "snapshots")


@pytest.fixture
def opaque():
    ctrl.vider_cache()
    idx = ctrl.index_opaque()
    assert idx, "Aucun mouvement bancaire"
    return list(idx)[0]


# ── 1-3 : bouton contrôle sur /banques-caisse ────────────────────────────────

@banque_requise
def test_01_02_03_bouton_controle_present_compteur_lien(client):
    r = client.get("/banques-caisse")
    assert r.status_code == 200
    assert "Contrôler les mouvements" in r.text
    m = re.search(r"Contrôler les mouvements \((\d+)\)", r.text)
    assert m is not None
    n = int(m.group(1))
    assert n == ctrl.compter_a_controler()
    assert '/banques-caisse/controle' in r.text


def test_02b_nav_test_menages_pattern_ok():
    """Test de navigation minimal (pattern conforme à test_navigation_no_404) : la route existe."""
    from app.routes import banques
    routes = {r.path for r in banques.router.routes}
    assert "/banques-caisse/controle" in routes


# ── 4-5 : dates ───────────────────────────────────────────────────────────────

def test_04_dates_format_jjmmaaaa():
    assert date_affichage("2026-02-25") == "25/02/2026"
    assert date_affichage("2026-03-02") == "02/03/2026"
    assert date_affichage("2026-04-01") == "01/04/2026"


def test_04b_date_absente_tiret():
    assert date_affichage(None) == "—"
    assert date_affichage("") == "—"


def test_04c_date_invalide_explicite():
    v = date_affichage("PAS_UNE_DATE")
    assert v.startswith("Date invalide —") and "PAS_UNE_DATE" in v


def test_05_iso_conserve_en_interne():
    """to_date/to_mois (internes, filtres) restent ISO — seul l'affichage change."""
    from app.readers.banques_reader import to_date, to_mois
    assert to_date("2026-02-25T00:00:00") == "2026-02-25"
    assert to_mois("2026-02-25") == "2026-02"


def test_05b_datetime_affichage():
    v = datetime_affichage("2026-07-17T20:29:47Z")
    assert v == "17/07/2026 20:29"


# ── 6-9 : mise en page formulaire ────────────────────────────────────────────

@banque_requise
def test_06_libelle_pleine_largeur_classe_css(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}/modifier")
    assert r.status_code == 200
    assert 'mouvement-libelle' in r.text
    assert 'form-field-full' in r.text


@banque_requise
def test_07_champs_alignes_grille(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}/modifier")
    assert "form-grid-3" in r.text
    assert 'class="form-field"' in r.text


@banque_requise
def test_08_09_commentaire_justification_textarea(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}/modifier")
    assert r.text.count("<textarea") == 2
    assert 'name="commentaire"' in r.text
    assert 'name="justification"' in r.text


# ── 10-11 : type de flux — libellé métier + valeur technique ─────────────────

def test_10_type_flux_libelle_metier():
    assert ctrl.libelle_type_flux("TYPE_FLUX_001") == "Virement d'associé"
    assert ctrl.libelle_type_flux("TYPE_FLUX_016") == "Frais bancaires"


def test_10b_type_flux_inconnu_repli_explicite():
    v = ctrl.libelle_type_flux("TYPE_FLUX_999")
    assert v == "Type de flux non documenté — TYPE_FLUX_999"


@banque_requise
def test_11_type_flux_valeur_technique_conservee(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}/modifier")
    assert 'value="TYPE_FLUX_001"' in r.text or "TYPE_FLUX_0" in r.text  # au moins un <option value=CODE>


# ── 12 : catégories — libellé humain ─────────────────────────────────────────

def test_12_categorie_libelle_humain():
    assert ctrl.libelle_categorie("PAYOUT_PLATEFORME") == "Versement plateforme de réservation"
    assert ctrl.libelle_categorie("ACHAT_PERSO_CB") == "Achat personnel par carte bancaire"
    assert ctrl.libelle_categorie("VIR_ASSOCIE") == "Virement associé"
    assert ctrl.libelle_categorie("COTISATION_PREVOYANCE") == "Cotisation prévoyance"


def test_12b_categorie_inconnue_repli_normalise():
    assert ctrl.libelle_categorie("CODE_JAMAIS_VU") == "Code jamais vu"


# ── 13-14 : source réelle de la proposition, aucune mention IA non prouvée ───

@banque_requise
def test_13_source_proposition_affichee(opaque, tmp_db):
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f["proposition_moteur"]["source_classification"] == "REGLE_DETERMINISTE"
    assert f["proposition_moteur"]["libelle_source"] == "Proposition automatique"


@banque_requise
def test_14_aucune_mention_ia_sans_preuve(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}")
    # la banque réelle est classée à 100% par règles -> jamais "Proposition IA" affiché
    assert "Proposition IA" not in r.text
    assert "Proposition automatique" in r.text


def test_14b_libelle_source_fonction_pure():
    assert ctrl.libelle_source_proposition("REGLE_DETERMINISTE", False) == "Proposition automatique"
    assert ctrl.libelle_source_proposition("", False) == "Proposition du moteur"
    assert ctrl.libelle_source_proposition("AUTRE", False) == "Proposition du moteur"


# ── 15-16 : statut En cours conservé + aide ──────────────────────────────────

def test_15_statut_en_cours_conserve():
    assert ctrl.ST_EN_COURS in ctrl.STATUTS
    assert ctrl.STATUTS_LIBELLES[ctrl.ST_EN_COURS] == "En cours"


@banque_requise
def test_16_aide_statut_visible_dans_js(opaque, client):
    r = client.get(f"/banques-caisse/mouvements/{opaque}/modifier")
    assert "En cours : mouvement pris en charge, mais décision non finalisée." in r.text


def test_16b_legende_statuts_liste(client, monkeypatch):
    """Présence du bloc légende — n'exige aucune donnée bancaire réelle ni son contenu (0 lignes
    suffit) : fixture synthétique plutôt qu'un garde optionnel, car ce test ne vérifie rien qui
    dépende des valeurs réelles (contrairement aux tests 05/19/20/21-24 de test_controles_actionnable.py)."""
    monkeypatch.setattr(ctrl, "load_liste", lambda **k: {
        "status": "OK", "etat": None, "rows": [], "count": 0,
        "options": {"proprietaires": [], "logements": [], "categories": [], "statuts": [],
                   "types_flux": []},
        "read_at": "2026-01-01 00:00:00"})
    r = client.get("/banques-caisse/controle")
    assert r.status_code == 200
    assert "Légende des statuts" in r.text


# ── 17-18 : aucun chemin absolu, aucune donnée bancaire complète ─────────────

@banque_requise
def test_17_18_aucun_chemin_absolu_aucune_donnee_complete(opaque, client):
    # Écrans APP-4B (identifiant opaque) : ni chemin absolu, ni numéro de compte complet.
    for url in ("/banques-caisse/controle",
               f"/banques-caisse/mouvements/{opaque}", f"/banques-caisse/mouvements/{opaque}/modifier"):
        t = client.get(url).text
        assert "C:\\" not in t and "OneDrive" not in t
        assert "00021321603" not in t
    # /banques-caisse (dashboard APP-4A, préexistant) : aucun chemin absolu — vérifié séparément,
    # car son filtre "compte_id" (hors périmètre de cette finition) contient le compte complet en
    # valeur d'option (trouvaille auditée, consignée au rapport, non corrigée dans cette mission).
    t = client.get("/banques-caisse").text
    assert "C:\\" not in t and "OneDrive" not in t


# ── 19 : aucun 500 ────────────────────────────────────────────────────────────

@banque_requise
def test_19_aucun_500(opaque, client):
    for url in ("/banques-caisse", "/banques-caisse/controle",
               f"/banques-caisse/mouvements/{opaque}", f"/banques-caisse/mouvements/{opaque}/modifier"):
        assert client.get(url).status_code != 500
    assert client.get("/banques-caisse/mouvements/MVT-000000000000").status_code == 404


# ── 20 : réel intact ──────────────────────────────────────────────────────────

@banque_requise
def test_20_reel_intact_apres_finition(opaque, tmp_db):
    sha_avant = _sha(REAL_BANQUE)
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", categorie="FRAIS_BANCAIRES", db_path=tmp_db)
    writer.enregistrer_sur_copie(db_path=tmp_db)
    assert _sha(REAL_BANQUE) == sha_avant

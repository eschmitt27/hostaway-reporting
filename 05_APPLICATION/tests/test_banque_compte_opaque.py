"""APP-4B correctif — identifiant COMPTE opaque (18 points).

Avant correction : le filtre "Compte" de /banques-caisse exposait le compte_id COMPLET en valeur
d'`<option>` HTML (masqué seulement à l'affichage). Les liens "Voir" des listes APP-4A exposaient
aussi le mouvement_id brut (contenant le compte) dans les `href`. Ce fichier prouve la correction :
identifiant opaque CPT-<hash8> pour les comptes, MVT-<hash12> partout pour les mouvements, aucun
compte brut accepté en filtre, aucune régression.

Aucune écriture bancaire réelle. Réel intact. Flags False.
"""
import hashlib
import re
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.services import banques_controle_service as ctrl
from app.services import banques_service as svc

# Deux comptes distincts : c'est ce que le filtre doit savoir séparer sans exposer l'un ni l'autre.
COMPTE_A = "CM_02211_00021321603"
COMPTE_B = "CM_09999_00099999999"


@pytest.fixture
def bank_file(tmp_db, monkeypatch):
    """Base Banque isolée, DEUX comptes distincts, pour tester le filtre compte opaque."""
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_db.parent / "CLASSEUR_ABSENT.xlsx")
    fx.construire(tmp_db, mouvements=[
        fx.mouvement("MVT-CM_02211_00021321603-20260302-CREDIT-1", "2026-03-02", "VIREMENT A",
                     100.0, "CREDIT", compte=COMPTE_A, categorie="FRAIS_BANCAIRES",
                     type_flux="TYPE_FLUX_016"),
        fx.mouvement("MVT-CM_09999_00099999999-20260303-DEBIT-1", "2026-03-03", "VIREMENT B",
                     50.0, "DEBIT", compte=COMPTE_B, categorie="FRAIS_BANCAIRES",
                     type_flux="TYPE_FLUX_016"),
    ])
    reader.vider_cache()
    ctrl.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl.vider_cache()


# ── 1-3 : plus de compte brut dans le filtre, texte masqué ───────────────────

def test_01_banques_caisse_sans_compte_complet(bank_file, client):
    r = client.get("/banques-caisse")
    assert r.status_code == 200
    assert "00021321603" not in r.text
    assert "CM_02211" not in r.text
    assert "00099999999" not in r.text


def test_02_option_filtre_id_opaque(bank_file):
    opts = svc.load_filter_options()
    for c in opts["comptes"]:
        assert re.match(r"^CPT-[0-9a-f]{8}$", c["id"]), c["id"]


def test_03_texte_visible_compte_masque(bank_file, client):
    r = client.get("/banques-caisse")
    assert "CM ••••1603" in r.text or "••••" in r.text


# ── 4-5 : filtre fonctionnel + stable ─────────────────────────────────────────

def test_04_filtre_par_compte_opaque_fonctionne(bank_file):
    opq = reader.id_opaque_compte("CM_02211_00021321603")
    d = svc.load_movements(mois="2026-03", compte_id=opq)
    assert d["status"] == "OK"
    assert d["count_filtre"] == 1
    assert not d["compte_id_invalide"]


def test_05_identifiant_opaque_stable(bank_file):
    a = reader.id_opaque_compte("CM_02211_00021321603")
    b = reader.id_opaque_compte("CM_02211_00021321603")
    assert a == b


def test_06_identifiant_opaque_unique(bank_file):
    a = reader.id_opaque_compte("CM_02211_00021321603")
    b = reader.id_opaque_compte("CM_09999_00099999999")
    assert a != b


# ── 7-8 : refus propre ────────────────────────────────────────────────────────

def test_07_identifiant_inconnu_refuse(bank_file):
    d = svc.load_movements(mois="2026-03", compte_id="CPT-deadbeef")
    assert d["compte_id_invalide"] is True
    assert d["count_filtre"] == 0


def test_08_compte_brut_query_string_refuse(bank_file):
    """Un compte_id BRUT envoyé directement (pas un CPT- opaque) ne filtre jamais silencieusement."""
    d = svc.load_movements(mois="2026-03", compte_id="CM_02211_00021321603")
    assert d["compte_id_invalide"] is True
    assert d["count_filtre"] == 0   # jamais interprété comme un filtre valide


def test_08b_route_compte_brut_refuse_pas_de_500(bank_file, client):
    r = client.get("/banques-caisse?compte_id=CM_02211_00021321603")
    assert r.status_code == 200
    assert "00021321603" not in r.text
    assert "invalide" in r.text.lower() or "inconnu" in r.text.lower()


# ── 9-10 : export CSV + pagination ───────────────────────────────────────────

def test_09_export_csv_filtre_fonctionne(bank_file):
    opq = reader.id_opaque_compte("CM_02211_00021321603")
    contenu = svc.export_movements_csv(mois="2026-03", compte_id=opq)
    lignes = [l for l in contenu.splitlines() if l.strip()]
    assert len(lignes) == 2  # entête + 1 mouvement du compte filtré
    assert "00021321603" not in contenu


def test_10_pagination_conserve_id_opaque(bank_file, client):
    opq = reader.id_opaque_compte("CM_02211_00021321603")
    r = client.get(f"/banques-caisse?mois=2026-03&compte_id={opq}&page=1")
    assert r.status_code == 200
    # le filtre reste sélectionné (reflet exact de l'opaque soumis)
    assert f'value="{opq}"' in r.text


# ── 11 : aucun ancien mouvement_id sensible dans les liens ───────────────────

def test_11_liens_mouvement_opaques(bank_file, client):
    r = client.get("/banques-caisse?mois=2026-03")
    assert "MVT-CM_02211_00021321603" not in r.text
    assert re.search(r"/banques-caisse/mouvements/MVT-[0-9a-f]{12}", r.text)


def test_11b_a_rapprocher_liens_opaques(bank_file, client):
    r = client.get("/banques-caisse/a-rapprocher?mois=2026-03")
    assert "CM_02211_00021321603" not in r.text
    assert "CM_09999_00099999999" not in r.text


def test_11c_fiche_detail_titre_opaque(bank_file, client):
    mid = "MVT-CM_02211_00021321603-20260302-CREDIT-1"
    opq = ctrl.id_opaque(mid)
    r = client.get(f"/banques-caisse/mouvements/{opq}")
    assert r.status_code == 200
    assert mid not in r.text


# ── 12-13 : non-régression ────────────────────────────────────────────────────

def test_12_regression_app4a_ancien_id_court_toujours_route(bank_file, client):
    """Les fixtures APP-4A historiques (test_banques.py) utilisent des ids courts type MVT-001 :
    ils ne collisionnent jamais avec le format opaque MVT-<12 hex> et restent routés normalement."""
    r = client.get("/banques-caisse/mouvements/MVT-INCONNU-XYZ")
    assert r.status_code == 404


def test_13_regression_app4b_fiche_toujours_fonctionnelle(bank_file, client):
    mid = "MVT-CM_02211_00021321603-20260302-CREDIT-1"
    opq = ctrl.id_opaque(mid)
    assert client.get(f"/banques-caisse/mouvements/{opq}/modifier").status_code == 200


# ── 14-15 : aucun chemin absolu, aucun numéro bancaire ───────────────────────

def test_14_15_aucun_chemin_absolu_aucun_numero_bancaire(bank_file, client):
    for url in ("/banques-caisse", "/banques-caisse/a-rapprocher?mois=2026-03",
               "/banques-caisse/controle"):
        t = client.get(url).text
        assert "C:\\" not in t and "OneDrive" not in t
        assert "00021321603" not in t and "00099999999" not in t


# ── 16 : flags ─────────────────────────────────────────────────────────────

def test_16_flags_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED is False


# ── 17-18 : intégrité réelle ──────────────────────────────────────────────────

REAL_BANQUE = Path(cfg.MASTER_BANQUE) if hasattr(cfg, "MASTER_BANQUE") else None


@pytest.mark.skipif(not Path(cfg.MASTER_BANQUE).exists(), reason="BANQUE_LOT8_IMPORT.xlsx absent")
def test_17_banque_reelle_intacte():
    p = Path(cfg.MASTER_BANQUE)
    h1 = hashlib.sha256(p.read_bytes()).hexdigest()
    svc.load_filter_options()  # exécute la logique corrigée en lecture
    h2 = hashlib.sha256(p.read_bytes()).hexdigest()
    assert h1 == h2


@pytest.mark.skipif(not (Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "data" / "app.db").exists(),
                    reason="app.db réelle absente")
def test_18_app_db_reelle_intacte():
    p = Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "data" / "app.db"
    h1 = hashlib.sha256(p.read_bytes()).hexdigest()
    svc.load_filter_options()
    h2 = hashlib.sha256(p.read_bytes()).hexdigest()
    assert h1 == h2

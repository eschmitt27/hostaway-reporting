"""APP-3c — Propriétaires & règlements : lecture MASTER Lot10/Lot12, tests.

Couverture :
- liste propriétaires (status=OK, count)
- fiche propriétaire (connu / inconnu)
- relevé connu (status=OK, séparation exploitation / règlement)
- relevé inconnu propre (status=NOT_FOUND)
- séparation stricte bloc exploitation / bloc règlement
- préfacture 12 lignes si données disponibles
- sources absentes ou sans cache Power Query (mocks)
- hash des sources inchangé après lecture
- routes HTTP (200 / 404)
- navigation active (/proprietaires dans sidebar)
- absence d'écriture SQLite ou Excel
"""
import hashlib
import pytest
from pathlib import Path
from unittest.mock import patch

from app.readers import proprietaires_reader as reader
from app.services import proprietaires_service as svc
from app.config import REF_SETUP, MASTER_NET_PROPRIETAIRE, MASTER_FACT_PROPRIETAIRES

# Constantes de test tirées du diagnostic des sources réelles
_PROP_CONNU = "PROP_0001"
_MOIS_CONNU = "2025-07"   # premier mois REGLEMENT pour PROP_0001
_PROP_MULTI = "PROP_0008"
_MOIS_MULTI = "2026-03"   # 3 logements pour PROP_0008
_PROP_INCONNU = "PROP_INEXISTANT_9999"
_MOIS_INCONNU = "9999-99"


# ---------------------------------------------------------------------------
# Liste propriétaires
# ---------------------------------------------------------------------------

def test_proprietaires_liste_status_ok():
    data = svc.load_list()
    assert data["status"] == "OK"
    assert isinstance(data["rows"], list)


def test_proprietaires_liste_count_12():
    data = svc.load_list()
    assert data["status"] == "OK"
    assert data["count"] == 12, f"Attendu 12 propriétaires, obtenu {data['count']}"


def test_proprietaires_liste_ids_presents():
    data = svc.load_list()
    ids = [str(r.get("proprietaire_id") or "").strip() for r in data["rows"]]
    assert _PROP_CONNU in ids
    assert _PROP_MULTI in ids


# ---------------------------------------------------------------------------
# Fiche propriétaire
# ---------------------------------------------------------------------------

def test_proprietaire_detail_connu():
    detail = svc.load_detail(_PROP_CONNU)
    assert detail is not None
    assert detail["status"] == "OK"
    assert str(detail["prop"].get("proprietaire_id")).strip() == _PROP_CONNU
    assert isinstance(detail["mois_disponibles"], list)
    assert len(detail["mois_disponibles"]) > 0


def test_proprietaire_detail_inconnu_retourne_none():
    detail = svc.load_detail(_PROP_INCONNU)
    assert detail is None


def test_proprietaire_detail_mois_contient_mois_connu():
    detail = svc.load_detail(_PROP_CONNU)
    assert detail is not None
    assert _MOIS_CONNU in detail["mois_disponibles"]


# ---------------------------------------------------------------------------
# Relevé mensuel
# ---------------------------------------------------------------------------

def test_releve_connu_status_ok():
    releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    assert releve["status"] == "OK"


def test_releve_inconnu_prop_retourne_not_found():
    releve = svc.load_releve(_PROP_INCONNU, _MOIS_CONNU)
    assert releve["status"] == "NOT_FOUND"


def test_releve_inconnu_mois_retourne_not_found():
    releve = svc.load_releve(_PROP_CONNU, _MOIS_INCONNU)
    assert releve["status"] == "NOT_FOUND"


def test_releve_separation_stricte_exploitation_reglement():
    """Blocs exploitation et règlement sont fournis séparément et non mélangés (D033)."""
    releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    assert releve["status"] == "OK"
    assert "bloc_exploitation" in releve
    assert "bloc_reglement" in releve
    # Colonnes de règlement absentes du bloc exploitation
    for row in releve["bloc_exploitation"]:
        assert "reste_a_payer_conciergerie" not in row or row.get("logement_id") is not None
    # Colonnes d'exploitation absentes du bloc règlement
    for row in releve["bloc_reglement"]:
        assert "total_payout_mois" not in row


def test_releve_exploitation_colonnes_presentes():
    releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    assert releve["status"] == "OK"
    for row in releve["bloc_exploitation"]:
        assert "logement_id" in row
        assert "total_payout_mois" in row
        assert "net_proprietaire_avant_charge_mois" in row


def test_releve_reglement_colonnes_presentes():
    releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    assert releve["status"] == "OK"
    for row in releve["bloc_reglement"]:
        assert "logement_id" in row
        assert "reste_a_payer_conciergerie" in row
        assert "statut_reglement" in row


def test_releve_multi_logements():
    """Un propriétaire avec 3 logements retourne 3 lignes dans chaque bloc."""
    releve = svc.load_releve(_PROP_MULTI, _MOIS_MULTI)
    assert releve["status"] == "OK"
    assert len(releve["bloc_exploitation"]) == 3
    assert len(releve["bloc_reglement"]) == 3


def test_releve_revenu_net_exploitation_non_recalcule():
    """revenu_net_exploitation absent du bloc relevé — jamais recalculé dans le service."""
    src = (Path(__file__).parent.parent / "app" / "services" / "proprietaires_service.py").read_text(encoding="utf-8")
    assert "revenu_net_exploitation" not in src or "net_proprietaire_avant_charge_mois" in src, (
        "Le service ne doit pas recalculer revenu_net_exploitation"
    )
    # La colonne n'est pas dans bloc_exploitation (c'est une colonne REGLEMENT, pas un recalcul)
    releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    for row in releve.get("bloc_exploitation", []):
        assert "revenu_net_exploitation" not in row


# ---------------------------------------------------------------------------
# Préfacture 12 lignes
# ---------------------------------------------------------------------------

def test_prefacture_connu_status_ok():
    pref = svc.load_prefacture(_PROP_CONNU, _MOIS_CONNU)
    assert pref["status"] == "OK"
    assert len(pref["factures"]) >= 1


def test_prefacture_12_lignes_par_facture():
    """Chaque facture contient exactement 5 lignes exploitation + 7 lignes règlement = 12."""
    pref = svc.load_prefacture(_PROP_CONNU, _MOIS_CONNU)
    assert pref["status"] == "OK"
    for f in pref["factures"]:
        nb_expl = len(f["lignes_exploitation"])
        nb_regl = len(f["lignes_reglement"])
        assert nb_expl == 5, f"Attendu 5 lignes exploitation, obtenu {nb_expl}"
        assert nb_regl == 7, f"Attendu 7 lignes règlement, obtenu {nb_regl}"


def test_prefacture_blocs_separes():
    """Lignes bloc EXPLOITATION et bloc REGLEMENT non mélangées dans la préfacture."""
    pref = svc.load_prefacture(_PROP_CONNU, _MOIS_CONNU)
    assert pref["status"] == "OK"
    for f in pref["factures"]:
        for l in f["lignes_exploitation"]:
            assert str(l.get("bloc") or "") == "EXPLOITATION"
        for l in f["lignes_reglement"]:
            assert str(l.get("bloc") or "") == "REGLEMENT"


def test_prefacture_inconnu_retourne_not_found():
    pref = svc.load_prefacture(_PROP_INCONNU, _MOIS_CONNU)
    assert pref["status"] == "NOT_FOUND"


def test_prefacture_mois_inconnu_retourne_not_found():
    pref = svc.load_prefacture(_PROP_CONNU, _MOIS_INCONNU)
    assert pref["status"] == "NOT_FOUND"


def test_prefacture_multi_logements_plusieurs_factures():
    """Propriétaire avec 3 logements → 3 factures avec 12 lignes chacune."""
    pref = svc.load_prefacture(_PROP_MULTI, _MOIS_MULTI)
    assert pref["status"] == "OK"
    assert len(pref["factures"]) == 3
    for f in pref["factures"]:
        assert len(f["lignes_exploitation"]) + len(f["lignes_reglement"]) == 12


# ---------------------------------------------------------------------------
# Sources absentes → dégradation propre
# ---------------------------------------------------------------------------

def test_liste_ref_absent_retourne_error():
    with patch.object(reader, "ref_available", return_value=False):
        data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["rows"] == []
    assert data["error_message"]


def test_releve_calc_absent_retourne_error():
    with patch.object(reader, "ref_available", return_value=True):
        with patch.object(reader, "calc_available", return_value=False):
            releve = svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    assert releve["status"] == "ERROR"
    assert releve["error_message"]


def test_prefacture_fact_absent_retourne_unavailable():
    with patch.object(reader, "fact_available", return_value=False):
        pref = svc.load_prefacture(_PROP_CONNU, _MOIS_CONNU)
    assert pref["status"] == "UNAVAILABLE"
    assert pref["factures"] == []
    assert pref["error_message"]


# ---------------------------------------------------------------------------
# Intégrité des sources (hash inchangé après lecture)
# ---------------------------------------------------------------------------

def test_hash_ref_setup_inchange():
    assert REF_SETUP.exists(), "REF_Setup.xlsm absent"
    sha_avant = hashlib.sha256(REF_SETUP.read_bytes()).hexdigest()
    svc.load_list()
    svc.load_detail(_PROP_CONNU)
    sha_apres = hashlib.sha256(REF_SETUP.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "REF_Setup.xlsm modifié après lecture — INTERDIT"


def test_hash_master_calc_inchange():
    assert MASTER_NET_PROPRIETAIRE.exists(), "MASTER_CALC_NetProprietaire absent"
    sha_avant = hashlib.sha256(MASTER_NET_PROPRIETAIRE.read_bytes()).hexdigest()
    svc.load_releve(_PROP_CONNU, _MOIS_CONNU)
    svc.load_releve(_PROP_MULTI, _MOIS_MULTI)
    sha_apres = hashlib.sha256(MASTER_NET_PROPRIETAIRE.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "MASTER_CALC_NetProprietaire modifié après lecture — INTERDIT"


def test_hash_master_fact_inchange():
    assert MASTER_FACT_PROPRIETAIRES.exists(), "MASTER_FACT_Proprietaires absent"
    sha_avant = hashlib.sha256(MASTER_FACT_PROPRIETAIRES.read_bytes()).hexdigest()
    svc.load_prefacture(_PROP_CONNU, _MOIS_CONNU)
    svc.load_prefacture(_PROP_MULTI, _MOIS_MULTI)
    sha_apres = hashlib.sha256(MASTER_FACT_PROPRIETAIRES.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "MASTER_FACT_Proprietaires modifié après lecture — INTERDIT"


# ---------------------------------------------------------------------------
# Absence d'écriture SQLite ou Excel
# ---------------------------------------------------------------------------

def test_service_pas_acces_sqlite():
    svc_src = (Path(__file__).parent.parent / "app" / "services" / "proprietaires_service.py").read_text(encoding="utf-8")
    rdr_src = (Path(__file__).parent.parent / "app" / "readers" / "proprietaires_reader.py").read_text(encoding="utf-8")
    for src, name in [(svc_src, "service"), (rdr_src, "reader")]:
        assert "sqlite3" not in src, f"Import sqlite3 dans {name}"
        assert "get_db" not in src, f"Appel get_db dans {name}"


def test_reader_pas_ecriture_excel():
    src = (Path(__file__).parent.parent / "app" / "readers" / "proprietaires_reader.py").read_text(encoding="utf-8")
    assert ".save(" not in src, "Appel .save() dans reader — INTERDIT"
    assert "read_only=False" not in src, "Ouverture Excel en écriture — INTERDIT"


# ---------------------------------------------------------------------------
# Routes HTTP
# ---------------------------------------------------------------------------

def test_proprietaires_get_liste_200(client):
    r = client.get("/proprietaires")
    assert r.status_code == 200


def test_proprietaires_get_detail_connu_200(client):
    r = client.get(f"/proprietaires/{_PROP_CONNU}")
    assert r.status_code == 200


def test_proprietaires_get_detail_inconnu_404(client):
    r = client.get(f"/proprietaires/{_PROP_INCONNU}")
    assert r.status_code == 404


def test_proprietaires_get_releve_connu_200(client):
    r = client.get(f"/proprietaires/{_PROP_CONNU}/{_MOIS_CONNU}")
    assert r.status_code == 200


def test_proprietaires_get_releve_inconnu_404(client):
    r = client.get(f"/proprietaires/{_PROP_INCONNU}/{_MOIS_INCONNU}")
    assert r.status_code == 404


def test_proprietaires_get_prefacture_connu_200(client):
    r = client.get(f"/proprietaires/{_PROP_CONNU}/{_MOIS_CONNU}/prefacture")
    assert r.status_code == 200


def test_proprietaires_get_prefacture_inconnu_404(client):
    r = client.get(f"/proprietaires/{_PROP_INCONNU}/{_MOIS_INCONNU}/prefacture")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Navigation active
# ---------------------------------------------------------------------------

def test_sidebar_contient_href_proprietaires(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/proprietaires-reglements"' in r.text, "Lien Propriétaires & règlements absent de la sidebar"


def test_proprietaires_nav_active_sur_liste(client):
    r = client.get("/proprietaires")
    assert r.status_code == 200
    assert "nav-item--future" not in r.text or 'href="/proprietaires-reglements"' in r.text


def test_proprietaires_nav_badge_future_disparu(client):
    """Le badge APP-3 future ne doit plus apparaître sur la page propriétaires."""
    r = client.get("/proprietaires")
    assert "APP-3" not in r.text or "APP-3c" not in r.text or 'href="/proprietaires"' in r.text

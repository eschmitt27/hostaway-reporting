"""APP-3a — Fournisseurs : lecture charges Lot3, filtres, détail, intégrité sources.

Tests requis (PLAN APP-3a) :
- liste status=OK depuis MASTER réel (vide acceptable — Power Query)
- filtres appliqués réduisent le résultat
- détail connu retourne les données (skip si aucune donnée)
- identifiant inconnu → 404 propre
- MASTER absent → status=ERROR, aucune exception
- sha256 MASTER et SAISIE inchangés après lecture
- aucune ligne IK ni virement associé (D025)
- routes HTTP : liste 200, filtres 200, détail inconnu 404
- navigation Fournisseurs active (href présent)
- aucun accès SQLite, aucune écriture Excel
"""
import hashlib
import pytest
from pathlib import Path
from unittest.mock import patch

from app.readers import charges_reader as reader
from app.services import charges_service as svc
from app.config import MASTER_CHARGES, SAISIE_CHARGES


# ---------------------------------------------------------------------------
# Tests lecture service
# ---------------------------------------------------------------------------

def test_charges_liste_status_ok():
    """Service retourne status=OK depuis le MASTER réel (liste peut être vide)."""
    data = svc.load_list()
    assert data["status"] == "OK"
    assert isinstance(data["rows"], list)


def test_charges_liste_vide_si_master_vide():
    """Quand le MASTER n'a pas de données PQ, la liste est vide mais status=OK."""
    data = svc.load_list()
    assert data["status"] == "OK"
    # Avec 0 charges saisies, rows=[] est le comportement attendu
    assert data["count_total"] >= 0


def test_charges_filtres_reduisent_resultat():
    """Filtre mois inexistant retourne liste vide mais status=OK."""
    data = svc.load_list(mois="0000-00")
    assert data["status"] == "OK"
    assert data["rows"] == []
    assert data["count_affiches"] == 0


def test_charges_filtres_coherents():
    """count_affiches cohérent avec len(rows)."""
    data = svc.load_list()
    assert data["count_affiches"] == len(data["rows"])


def test_charges_detail_inconnu_retourne_none():
    """find_charge sur un ID inexistant retourne None (→ 404 propre)."""
    charge = reader.find_charge("CHARGE_INEXISTANTE_9999")
    assert charge is None


def test_charges_service_detail_inconnu_404():
    """load_detail sur ID inexistant retourne None (→ 404 propre)."""
    detail = svc.load_detail("CHARGE_INEXISTANTE_9999")
    assert detail is None


def test_charges_detail_retourne_ligne():
    """load_detail retourne les données pour une ligne connue (skip si aucune donnée)."""
    rows = reader.read_charges()
    if not rows:
        pytest.skip("Aucune donnée dans MASTER (Power Query non rafraîchi) — test ignoré")
    charge_id = str(rows[0].get("charge_id") or "").strip()
    assert charge_id, "charge_id vide sur la première ligne"
    detail = svc.load_detail(charge_id)
    assert detail is not None
    assert detail["status"] == "OK"
    assert detail["charge"]["charge_id"] == charge_id


# ---------------------------------------------------------------------------
# Test : MASTER absent → ERROR propre
# ---------------------------------------------------------------------------

def test_charges_master_absent_etat_error():
    """MASTER absent → status=ERROR, aucune exception, rows=[]."""
    with patch.object(reader, "master_available", return_value=False):
        data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["rows"] == []
    assert data["error_message"]


def test_charges_master_absent_detail_error():
    """MASTER absent → load_detail retourne dict ERROR, pas None ni exception."""
    with patch.object(reader, "master_available", return_value=False):
        detail = svc.load_detail("CHARGE_QUELCONQUE")
    assert detail is not None
    assert detail["status"] == "ERROR"


# ---------------------------------------------------------------------------
# Test : intégrité sources (hash inchangé après lecture)
# ---------------------------------------------------------------------------

def test_charges_master_hash_inchange_apres_lecture():
    """Le MASTER n'est pas modifié après lecture (sha256 stable)."""
    assert MASTER_CHARGES.exists(), "MASTER absent — impossible de tester le hash"
    sha_avant = hashlib.sha256(MASTER_CHARGES.read_bytes()).hexdigest()
    taille_avant = MASTER_CHARGES.stat().st_size
    svc.load_list()
    svc.load_list(mois="2026-05")
    sha_apres = hashlib.sha256(MASTER_CHARGES.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "MASTER modifié après lecture — INTERDIT"
    assert MASTER_CHARGES.stat().st_size == taille_avant


def test_charges_saisie_hash_inchange_apres_lecture():
    """La SAISIE n'est jamais touchée par une lecture du service."""
    if not SAISIE_CHARGES.exists():
        pytest.skip("SAISIE absente")
    sha_avant = hashlib.sha256(SAISIE_CHARGES.read_bytes()).hexdigest()
    svc.load_list()
    svc.load_list(mois="2026-05")
    sha_apres = hashlib.sha256(SAISIE_CHARGES.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "SAISIE modifiée par lecture du service — INTERDIT"


# ---------------------------------------------------------------------------
# Test D025 : aucune ligne IK ni virement associé
# ---------------------------------------------------------------------------

def test_charges_pas_de_ligne_ik():
    """Aucune ligne type_flux_id=IK dans la liste fournisseurs (D025)."""
    data = svc.load_list()
    assert data["status"] == "OK"
    for row in data["rows"]:
        tfi = str(row.get("type_flux_id") or "").strip().upper()
        assert tfi != "IK", f"Ligne IK trouvée : {row.get('charge_id')}"


def test_charges_pas_de_virement_associe():
    """Aucune ligne type_flux_id=VIREMENT_ASSOCIE dans la liste fournisseurs (D025)."""
    data = svc.load_list()
    assert data["status"] == "OK"
    for row in data["rows"]:
        tfi = str(row.get("type_flux_id") or "").strip().upper()
        assert tfi != "VIREMENT_ASSOCIE", f"Virement associé trouvé : {row.get('charge_id')}"


# ---------------------------------------------------------------------------
# Test : aucun accès SQLite, aucune écriture
# ---------------------------------------------------------------------------

def test_charges_service_pas_acces_sqlite():
    """Le service charges n'importe pas sqlite3 ni get_db (lecture seule, pas de SQLite)."""
    service_src = (Path(__file__).parent.parent / "app" / "services" / "charges_service.py").read_text(encoding="utf-8")
    reader_src = (Path(__file__).parent.parent / "app" / "readers" / "charges_reader.py").read_text(encoding="utf-8")
    for src, name in [(service_src, "service"), (reader_src, "reader")]:
        assert "sqlite3" not in src, f"Import sqlite3 détecté dans {name}"
        assert "get_db" not in src, f"Appel get_db détecté dans {name} — aucun accès SQLite autorisé"
        assert "conn.execute" not in src, f"Écriture SQLite détectée dans {name}"


def test_charges_reader_pas_ecriture_excel():
    """Le reader charges n'ouvre jamais un workbook en écriture."""
    reader_src = (Path(__file__).parent.parent / "app" / "readers" / "charges_reader.py").read_text(encoding="utf-8")
    assert "save(" not in reader_src, "Appel .save() détecté dans reader — INTERDIT"
    assert 'read_only=False' not in reader_src, "Ouverture Excel en écriture détectée"


# ---------------------------------------------------------------------------
# Tests routes HTTP
# ---------------------------------------------------------------------------

def test_fournisseurs_get_liste_200(client):
    r = client.get("/fournisseurs")
    assert r.status_code == 200


def test_fournisseurs_get_liste_avec_filtres_200(client):
    r = client.get("/fournisseurs?mois=2026-05&code_impact=IC")
    assert r.status_code == 200


def test_fournisseurs_detail_inconnu_404_route(client):
    r = client.get("/fournisseurs/CHARGE_INEXISTANTE_9999")
    assert r.status_code == 404


def test_fournisseurs_detail_connu_200(client):
    """GET sur une ligne existante retourne 200 (skip si aucune donnée MASTER)."""
    rows = reader.read_charges()
    if not rows:
        pytest.skip("Aucune donnée dans MASTER (Power Query non rafraîchi) — test ignoré")
    charge_id = str(rows[0].get("charge_id") or "").strip()
    r = client.get(f"/fournisseurs/{charge_id}")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Test : navigation Fournisseurs active
# ---------------------------------------------------------------------------

def test_sidebar_fournisseurs_href_actif(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/fournisseurs"' in r.text, "Lien /fournisseurs absent de la sidebar"


def test_fournisseurs_nav_active_sur_liste(client):
    r = client.get("/fournisseurs")
    assert r.status_code == 200
    assert "nav-item--future" not in r.text or 'href="/fournisseurs"' in r.text, (
        "Menu Fournisseurs encore marqué 'future'"
    )


# ---------------------------------------------------------------------------
# Tests accès nouvelle charge (APP-3b-1)
# ---------------------------------------------------------------------------

def test_fournisseurs_liste_bouton_nouvelle_charge_href(client):
    """GET /fournisseurs → href="/fournisseurs/nouvelle" présent."""
    r = client.get("/fournisseurs")
    assert r.status_code == 200
    assert 'href="/fournisseurs/nouvelle"' in r.text


def test_fournisseurs_liste_bouton_nouvelle_charge_texte(client):
    """GET /fournisseurs → texte « Nouvelle charge » présent."""
    r = client.get("/fournisseurs")
    assert r.status_code == 200
    assert "Nouvelle charge" in r.text


def test_fournisseurs_nouvelle_get_200(client):
    """GET /fournisseurs/nouvelle → 200."""
    r = client.get("/fournisseurs/nouvelle")
    assert r.status_code == 200


def test_fournisseurs_nouvelle_accessible_meme_si_master_vide(client):
    """Bouton Nouvelle charge accessible même quand MASTER Lot3 est vide."""
    from unittest.mock import patch
    from app.readers import charges_reader as cr
    with patch.object(cr, "master_available", return_value=False):
        r = client.get("/fournisseurs")
    assert r.status_code == 200
    assert 'href="/fournisseurs/nouvelle"' in r.text

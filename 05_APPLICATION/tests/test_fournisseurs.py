"""APP-3a — Fournisseurs : lecture charges (table `charges`, migration 0052), filtres, détail.

Table `charges` en SQLite depuis la migration 0052 : plus de MASTER_FACT_MAN_Charges.xlsx ni de
SAISIE_Charges_Flux.xlsx à lire. Chaque test travaille sur une base temporaire isolée (`tmp_db`).

Tests couverts :
- liste status=OK / vide=OK
- filtres appliqués réduisent le résultat
- détail connu retourne les données, inconnu → None/404
- table absente → status=ERROR, aucune exception
- aucune ligne IK ni virement associé (D025)
- routes HTTP : liste 200, filtres 200, détail 200/404
"""
from unittest.mock import patch

from app.readers import charges_reader as reader
from app.services import charges_saisie_service as saisie
from app.services import charges_service as svc

CHARGE = {"date_charge": "2026-06-15", "montant": 100.0, "categorie_charge_id": "CHG_017",
          "logement_id": "LOG_A1", "type_flux_id": "TYPE_FLUX_020", "code_impact": "IC",
          "statut_controle": "VALIDE"}


def _creer(db_path, **overrides) -> str:
    res = saisie.creer(dict(CHARGE, **overrides), acteur="fixture", db_path=db_path)
    assert res["ok"], res
    return res["charge_id"]


# ---------------------------------------------------------------------------
# Tests lecture service
# ---------------------------------------------------------------------------

def test_charges_liste_status_ok(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "OK"
    assert isinstance(data["rows"], list)
    assert len(data["rows"]) == 1


def test_charges_liste_vide_si_aucune_charge(tmp_db):
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "OK"
    assert data["count_total"] == 0


def test_charges_filtres_reduisent_resultat(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(mois="0000-00", db_path=tmp_db)
    assert data["status"] == "OK"
    assert data["rows"] == []
    assert data["count_affiches"] == 0


def test_charges_filtres_coherents(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(db_path=tmp_db)
    assert data["count_affiches"] == len(data["rows"])


def test_charges_detail_inconnu_retourne_none(tmp_db):
    charge = reader.find_charge("CHARGE_INEXISTANTE_9999", db_path=tmp_db)
    assert charge is None


def test_charges_service_detail_inconnu_404(tmp_db):
    detail = svc.load_detail("CHARGE_INEXISTANTE_9999", db_path=tmp_db)
    assert detail is None


def test_charges_detail_retourne_ligne(tmp_db):
    charge_id = _creer(tmp_db)
    detail = svc.load_detail(charge_id, db_path=tmp_db)
    assert detail is not None
    assert detail["status"] == "OK"
    assert detail["charge"]["charge_id"] == charge_id


# ---------------------------------------------------------------------------
# Test : table absente → ERROR propre
# ---------------------------------------------------------------------------

def test_charges_table_absente_etat_error(tmp_db):
    """Table absente/inaccessible → status=ERROR, aucune exception, rows=[]."""
    with patch.object(reader, "master_available", return_value=False):
        data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "ERROR"
    assert data["rows"] == []
    assert data["error_message"]


def test_charges_table_absente_detail_error(tmp_db):
    with patch.object(reader, "master_available", return_value=False):
        detail = svc.load_detail("CHARGE_QUELCONQUE", db_path=tmp_db)
    assert detail is not None
    assert detail["status"] == "ERROR"


# ---------------------------------------------------------------------------
# Test D025 : aucune ligne IK ni virement associé
# ---------------------------------------------------------------------------

def test_charges_pas_de_ligne_ik(tmp_db):
    _creer(tmp_db, type_flux_id="IK")
    _creer(tmp_db, charge_id="CHG-AUTRE", type_flux_id="TYPE_FLUX_020")
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "OK"
    for row in data["rows"]:
        tfi = str(row.get("type_flux_id") or "").strip().upper()
        assert tfi != "IK", f"Ligne IK trouvée : {row.get('charge_id')}"


def test_charges_pas_de_virement_associe(tmp_db):
    _creer(tmp_db, type_flux_id="VIREMENT_ASSOCIE")
    _creer(tmp_db, charge_id="CHG-AUTRE", type_flux_id="TYPE_FLUX_020")
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "OK"
    for row in data["rows"]:
        tfi = str(row.get("type_flux_id") or "").strip().upper()
        assert tfi != "VIREMENT_ASSOCIE", f"Virement associé trouvé : {row.get('charge_id')}"


def test_charges_reader_pas_ecriture_excel():
    """Le reader charges n'ouvre jamais un workbook en écriture (SQLite désormais)."""
    from pathlib import Path
    reader_src = (Path(__file__).parent.parent / "app" / "readers" / "charges_reader.py").read_text(encoding="utf-8")
    assert "openpyxl" not in reader_src, "Lecture Excel détectée dans le reader charges — INTERDIT"
    assert "save(" not in reader_src, "Appel .save() détecté dans reader — INTERDIT"


# ---------------------------------------------------------------------------
# Tests routes HTTP
# ---------------------------------------------------------------------------

def test_fournisseurs_get_liste_200(client, tmp_db):
    r = client.get("/fournisseurs")
    assert r.status_code == 200


def test_fournisseurs_get_liste_avec_filtres_200(client, tmp_db):
    r = client.get("/fournisseurs?mois=2026-05&code_impact=IC")
    assert r.status_code == 200


def test_fournisseurs_detail_inconnu_404_route(client, tmp_db):
    r = client.get("/fournisseurs/CHARGE_INEXISTANTE_9999")
    assert r.status_code == 404


def test_fournisseurs_detail_connu_200(client, tmp_db):
    charge_id = _creer(tmp_db)
    r = client.get(f"/fournisseurs/{charge_id}")
    assert r.status_code == 200

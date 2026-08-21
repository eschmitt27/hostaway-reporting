"""APP-2a — Réservations hors Hostaway (lecture seule).

Table `reservations_hors_hostaway` en SQLite depuis la migration 0052 : plus de
MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx à lire. Couvre : liste, table absente → erreur,
recherche, filtres, fiche détail, réservation inconnue → 404, lecture read-only, aucune écriture
provoquée par une simple consultation.
"""
from pathlib import Path
from unittest.mock import patch

import pytest

from app.db.connection import get_db
from app.services import reservations_hh_saisie_service as saisie
from app.services import reservations_hh_service as svc
from app.readers import reservations_hh_reader as reader

APP_DIR = Path(__file__).parent.parent / "app"

RESERVATION = {
    "mois": "2026-06", "canal_id": "CANAL_004", "source_financiere": "SAISIE_MANUELLE",
    "proprietaire_id": "PROP_0003", "logement_id": "LOG_0009", "date_arrivee": "2026-06-10",
    "date_depart": "2026-06-15", "montant_percu": 2343.48, "code_impact": "HC",
}


def _creer(db_path, **overrides) -> str:
    res = saisie.creer(dict(RESERVATION, **overrides), acteur="fixture", db_path=db_path)
    assert res["ok"], res
    return res["reservation_hh_id"]


# ---------------------------------------------------------------- liste / nav

def test_liste_repond_200(client):
    r = client.get("/reservations")
    assert r.status_code == 200
    assert "Réservations hors Hostaway" in r.text


def test_liste_lit_la_table_sqlite(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(db_path=tmp_db)
    assert data["sheet"] == "MASTER"
    assert data["status"] == "OK"


def test_liste_vide_si_aucune_reservation(tmp_db):
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "EMPTY"


def test_liste_contient_les_reservations_creees(tmp_db):
    rid = _creer(tmp_db)
    data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "OK"
    ids = [r["reservation_hh_id"] for r in data["rows"]]
    assert rid in ids


def test_placeholder_absent(tmp_db):
    _creer(tmp_db)
    for r in reader.read_reservations(db_path=tmp_db):
        assert str(r["reservation_hh_id"]).startswith("RESHH-")


# ---------------------------------------------------------------- source absente

def test_table_absente_erreur_lisible(tmp_db):
    with patch.object(reader, "master_available", return_value=False):
        data = svc.load_list(db_path=tmp_db)
    assert data["status"] == "ERROR"
    assert data["error_message"]
    assert data["rows"] == []


def test_table_absente_rendu(client, tmp_db):
    with patch.object(reader, "master_available", return_value=False):
        r = client.get("/reservations")
    assert r.status_code == 200
    assert "indisponible" in r.text.lower()


# ---------------------------------------------------------------- recherche / filtres

def test_recherche(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(q="LOG_0009", db_path=tmp_db)
    assert any(str(r.get("logement_id") or "") == "LOG_0009" for r in data["rows"])


def test_filtre_source_financiere(tmp_db):
    _creer(tmp_db)
    data = svc.load_list(source_financiere="SAISIE_MANUELLE", db_path=tmp_db)
    assert data["rows"]
    for r in data["rows"]:
        assert str(r.get("source_financiere") or "") == "SAISIE_MANUELLE"


def test_filtre_sans_resultat_etat_vide(client, tmp_db):
    _creer(tmp_db)
    r = client.get("/reservations?source_financiere=SOURCE_INEXISTANTE_XYZ")
    assert r.status_code == 200
    assert "Aucune réservation" in r.text


# ---------------------------------------------------------------- fiche détail

def test_fiche_detail_existante_200(client, tmp_db):
    rid = _creer(tmp_db)
    r = client.get(f"/reservations/{rid}")
    assert r.status_code == 200
    assert rid in r.text
    assert "Origine des données" in r.text


def test_fiche_detail_inconnue_404(client, tmp_db):
    r = client.get("/reservations/RESHH-9999-99-999")
    assert r.status_code == 404
    assert "introuvable" in r.text.lower()


# ---------------------------------------------------------------- read-only / no write

def test_lecture_excel_read_only():
    src = (APP_DIR / "readers" / "excel_reader.py").read_text(encoding="utf-8")
    assert "read_only=True" in src


def test_consulter_les_ecrans_ne_modifie_pas_la_table(client, tmp_db):
    rid = _creer(tmp_db)
    conn = get_db(tmp_db)
    try:
        avant = conn.execute(
            "SELECT COUNT(*) FROM reservations_hors_hostaway").fetchone()[0]
    finally:
        conn.close()
    client.get("/reservations")
    client.get(f"/reservations/{rid}")
    conn = get_db(tmp_db)
    try:
        apres = conn.execute(
            "SELECT COUNT(*) FROM reservations_hors_hostaway").fetchone()[0]
    finally:
        conn.close()
    assert apres == avant, "consulter un écran ne doit rien écrire"


def test_module_hh_n_appelle_pas_saisie_writer():
    for name in ("services/reservations_hh_service.py",
                 "readers/reservations_hh_reader.py",
                 "routes/reservations.py"):
        src = (APP_DIR / name).read_text(encoding="utf-8")
        assert "saisie_writer" not in src, f"{name} ne doit pas référencer saisie_writer"


def test_service_hh_lecture_seule():
    """Le service de lecture (liste/détail) ne fait aucune écriture SQLite lui-même."""
    src = (APP_DIR / "services" / "reservations_hh_service.py").read_text(encoding="utf-8")
    assert "conn.execute" not in src
    assert "INSERT" not in src
    assert "UPDATE" not in src


def test_champs_moteur_incluent_les_derives():
    for champ in ("commission", "acompte_facture", "taux_commission",
                  "impact_resultat_reel", "impact_resultat_comptable"):
        assert champ in svc.CHAMPS_MOTEUR


# ---------------------------------------------------------------- corrections visuelles

def test_header_sans_periode_ni_recherche_desactivee(client):
    r = client.get("/reservations")
    txt = r.text
    assert "period-badge" not in txt, "Le badge Période doit être retiré du header"
    assert "Période :" not in txt
    assert 'aria-label="Recherche globale"' not in txt, "Recherche globale désactivée doit être retirée"


def test_reinitialiser_est_bouton_secondaire(client):
    r = client.get("/reservations")
    assert 'class="btn btn-secondary"' in r.text and "Réinitialiser" in r.text


def test_montants_format_francais(client, tmp_db):
    rid = _creer(tmp_db, montant_percu=2343.48)
    r = client.get(f"/reservations/{rid}")
    assert "2 343,48 €" in r.text


def test_identifiants_techniques_conserves_bruts(client, tmp_db):
    rid = _creer(tmp_db)
    r = client.get(f"/reservations/{rid}")
    assert "LOG_0009" in r.text
    assert "PROP_0003" in r.text
    assert "CANAL_004" in r.text


def test_format_eur_ne_recalcule_pas():
    from app.routes.reservations import format_eur
    assert format_eur(2343.48) == "2 343,48 €"
    assert format_eur(55) == "55,00 €"
    assert format_eur(None) == "Non renseigné"
    assert format_eur("") == "Non renseigné"
    assert format_eur("LOG_0009") == "LOG_0009"

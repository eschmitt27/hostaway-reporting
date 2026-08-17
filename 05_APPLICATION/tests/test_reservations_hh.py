"""APP-2a — Réservations hors Hostaway (lecture seule).

Couvre : liste depuis MASTER réel, source absente → erreur, recherche, filtres,
fiche détail, réservation inconnue → 404, lecture read-only, aucune écriture
SAISIE/MASTER, aucun appel saisie_writer, aucune réservation métier en SQLite.
"""
from pathlib import Path
import sqlite3
import pytest

from app.config import MASTER_RESERVATIONS_HH
from app.services import reservations_hh_service as svc
from app.readers import reservations_hh_reader as reader

APP_DIR = Path(__file__).parent.parent / "app"
_real_rows = reader.read_reservations() if MASTER_RESERVATIONS_HH.exists() else []
HAS_DATA = len(_real_rows) > 0
PK = str(_real_rows[0]["reservation_hh_id"]) if HAS_DATA else "RESHH-0000-00-000"
data_required = pytest.mark.skipif(not HAS_DATA, reason="MASTER HH sans réservation réelle dans cet environnement")


# ---------------------------------------------------------------- liste / nav

def test_liste_repond_200(client):
    r = client.get("/reservations")
    assert r.status_code == 200
    assert "Réservations hors Hostaway" in r.text


def test_liste_lit_le_bon_master():
    data = svc.load_list()
    assert data["source"] == "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
    assert data["sheet"] == "MASTER"
    assert data["status"] in ("OK", "EMPTY")


@data_required
def test_liste_contient_les_reservations_reelles():
    data = svc.load_list()
    assert data["status"] == "OK"
    ids = [r["reservation_hh_id"] for r in data["rows"]]
    assert PK in ids


@data_required
def test_placeholder_power_query_exclu():
    # La ligne '[Charge par Power Query ...]' ne doit jamais apparaître
    for r in reader.read_reservations():
        assert str(r["reservation_hh_id"]).startswith("RESHH-")


# ---------------------------------------------------------------- source absente

def test_master_absent_erreur_lisible(monkeypatch, tmp_path):
    faux = tmp_path / "inexistant_MASTER.xlsx"
    monkeypatch.setattr(svc, "MASTER_RESERVATIONS_HH", faux)
    data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["error_message"]
    assert data["rows"] == []


def test_master_absent_rendu(client, monkeypatch, tmp_path):
    faux = tmp_path / "inexistant_MASTER.xlsx"
    monkeypatch.setattr(svc, "MASTER_RESERVATIONS_HH", faux)
    r = client.get("/reservations")
    assert r.status_code == 200
    assert "indisponible" in r.text.lower()


# ---------------------------------------------------------------- recherche / filtres

@data_required
def test_recherche():
    logement = str(_real_rows[0].get("logement_id") or "")
    data = svc.load_list(q=logement)
    assert any(str(r.get("logement_id") or "") == logement for r in data["rows"])


@data_required
def test_filtre_source_financiere():
    src = str(_real_rows[0].get("source_financiere") or "")
    data = svc.load_list(source_financiere=src)
    assert data["rows"]
    for r in data["rows"]:
        assert str(r.get("source_financiere") or "") == src


@data_required
def test_filtre_sans_resultat_etat_vide(client):
    r = client.get("/reservations?source_financiere=SOURCE_INEXISTANTE_XYZ")
    assert r.status_code == 200
    assert "Aucune réservation" in r.text


# ---------------------------------------------------------------- fiche détail

@data_required
def test_fiche_detail_existante_200(client):
    r = client.get(f"/reservations/{PK}")
    assert r.status_code == 200
    assert PK in r.text
    assert "Origine des données" in r.text
    # commission signalée comme issue du moteur
    assert "moteur" in r.text.lower()


def test_fiche_detail_inconnue_404(client):
    r = client.get("/reservations/RESHH-9999-99-999")
    assert r.status_code == 404
    assert "introuvable" in r.text.lower()


# ---------------------------------------------------------------- read-only / no write

def test_lecture_excel_read_only():
    src = (APP_DIR / "readers" / "excel_reader.py").read_text(encoding="utf-8")
    assert "read_only=True" in src


@data_required
def test_aucune_ecriture_saisie_ni_master(client):
    def sig(p: Path):
        st = p.stat()
        return (st.st_size, st.st_mtime_ns)
    before_master = sig(MASTER_RESERVATIONS_HH)
    client.get("/reservations")
    client.get(f"/reservations/{PK}")
    assert sig(MASTER_RESERVATIONS_HH) == before_master, "MASTER HH modifié par une consultation"


def test_module_hh_n_appelle_pas_saisie_writer():
    for name in ("services/reservations_hh_service.py",
                 "readers/reservations_hh_reader.py",
                 "routes/reservations.py"):
        src = (APP_DIR / name).read_text(encoding="utf-8")
        assert "saisie_writer" not in src, f"{name} ne doit pas référencer saisie_writer au Lot APP-2a"


def test_module_hh_ne_lit_pas_saisie_pour_donnees():
    """Le service ne lit jamais SAISIE_* pour construire la liste (nommée pour origine seulement)."""
    import ast
    src = (APP_DIR / "readers" / "reservations_hh_reader.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            ds = ast.get_docstring(node, clean=False)
            if ds:
                docstrings.add(ds)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value not in docstrings:
            assert "SAISIE" not in node.value, "Le reader ne doit pas cibler un fichier SAISIE"


# ---------------------------------------------------------------- SQLite = pas de métier

def test_consulter_les_ecrans_hh_n_ecrit_aucune_reservation(client, tmp_db):
    """Consulter un écran ne crée jamais de réservation en base.

    Ce test affirmait qu'AUCUNE table de réservation ne devait exister en SQLite. Cet invariant
    datait de l'époque où les réservations vivaient dans des classeurs ; la migration l'a inversé —
    les réservations calculées, résolues et historisées vivent maintenant en base, et c'est le but.

    Ce qui reste vrai, et qui compte davantage : les tables de réservations sont alimentées par le
    MOTEUR, jamais par la consultation d'un écran. Une lecture qui écrirait créerait des lignes que
    personne n'a calculées.
    """
    conn = sqlite3.connect(str(tmp_db))
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'reservations_%'").fetchall()]
        avant = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
    finally:
        conn.close()

    client.get("/reservations")
    client.get(f"/reservations/{PK}")

    conn = sqlite3.connect(str(tmp_db))
    try:
        apres = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
    finally:
        conn.close()
    assert apres == avant, "consulter un écran ne doit rien écrire"
    assert all(n == 0 for n in apres.values()), "aucune réservation n'a été calculée dans ce test"


def test_service_hh_n_importe_pas_sqlite():
    src = (APP_DIR / "services" / "reservations_hh_service.py").read_text(encoding="utf-8")
    assert "get_db" not in src
    assert "sqlite" not in src.lower()


def test_champs_moteur_incluent_les_derives():
    # commission / acompte / taux traités comme produits par le moteur, jamais recalculés
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


@data_required
def test_montants_format_francais(client):
    r = client.get(f"/reservations/{PK}")
    # RESHH-2026-05-001 : total_percu=2343.48, menage=55
    assert "2 343,48 €" in r.text
    assert "55,00 €" in r.text


@data_required
def test_identifiants_techniques_conserves_bruts(client):
    r = client.get(f"/reservations/{PK}")
    assert "LOG_0009" in r.text
    assert "PROP_0003" in r.text
    assert "CANAL_004" in r.text


def test_mention_power_query_toujours_presente(client):
    r = client.get("/reservations")
    assert "Actualisation Power Query manuelle requise" in r.text


def test_format_eur_ne_recalcule_pas():
    from app.routes.reservations import format_eur
    assert format_eur(2343.48) == "2 343,48 €"
    assert format_eur(55) == "55,00 €"
    assert format_eur(None) == "Non renseigné"
    assert format_eur("") == "Non renseigné"
    # texte non convertible renvoyé tel quel (aucune invention)
    assert format_eur("LOG_0009") == "LOG_0009"

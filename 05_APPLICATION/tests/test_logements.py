"""APP-1 — Module Logements (lecture seule).

Couvre : chargement liste, lecture read-only, absence d'écriture source,
recherche, filtres, fiche détail, logement introuvable, PBI manquant sans fallback,
lignes techniques, commission sans notion d'« actuel », données perso absentes,
absence de jointure gestion/propriétaire, aucune donnée métier en SQLite,
et /health confirmant le vrai chemin REF_Setup.
"""
from pathlib import Path
import sqlite3
import pytest

from app.config import PBI_LOGEMENTS, REF_SETUP
from app.services import logements_service as svc
from app.readers import ref_setup_reader as ref

APP_DIR = Path(__file__).parent.parent / "app"
REAL_DATA = PBI_LOGEMENTS.exists()
pbi_required = pytest.mark.skipif(not REAL_DATA, reason="PBI_Referentiel_Logements.csv absent de cet environnement")


# ---------------------------------------------------------------- liste / nav

def test_liste_repond_200(client):
    r = client.get("/logements")
    assert r.status_code == 200
    assert "Logements" in r.text


@pbi_required
def test_liste_lit_bien_la_source_pbi():
    data = svc.load_list()
    assert data["status"] == "OK"
    assert data["source"] == "PBI_Referentiel_Logements.csv"
    assert data["read_at"]
    assert data["rows"], "La liste réelle doit contenir des logements"


# ---------------------------------------------------------------- lignes techniques

@pbi_required
def test_lignes_techniques_absentes_par_defaut():
    data = svc.load_list()
    ids = [r["logement_id"] for r in data["rows"]]
    assert "APPARTEMENT_DIVERS" not in ids
    assert "LOGEMENT_DIVERS" not in ids
    assert data["count_technique"] == 2


@pbi_required
def test_lignes_techniques_presentes_si_filtre():
    data = svc.load_list(include_technique=True)
    ids = [r["logement_id"] for r in data["rows"]]
    assert "APPARTEMENT_DIVERS" in ids or "LOGEMENT_DIVERS" in ids


@pbi_required
def test_compteur_parc_exclut_les_techniques():
    data = svc.load_list()
    # Le compteur du parc ne compte jamais les lignes techniques
    for r in data["rows"]:
        assert r["logement_id"].startswith("LOG_")
    assert data["count_parc"] == len([r for r in data["rows"]])


@pbi_required
def test_liste_rendu_html_technique_toggle(client):
    r_def = client.get("/logements")
    assert "APPARTEMENT_DIVERS" not in r_def.text
    r_tech = client.get("/logements?include_technique=1")
    assert "HORS_PARC_TECHNIQUE" in r_tech.text


# ---------------------------------------------------------------- recherche / filtres

@pbi_required
def test_recherche_texte_filtre():
    data = svc.load_list(q="Blagnac")
    assert data["rows"], "Recherche 'Blagnac' doit retourner au moins un logement"
    for r in data["rows"]:
        blob = f"{r.get('nom_logement_officiel','')} {r.get('ville','')} {r.get('nom_court','')}".lower()
        assert "blagnac" in blob


@pbi_required
def test_filtre_ville():
    data = svc.load_list(ville="BLAGNAC")
    assert data["rows"]
    for r in data["rows"]:
        assert (r.get("ville") or "").strip() == "BLAGNAC"


@pbi_required
def test_filtre_ville_sans_resultat_etat_vide(client):
    r = client.get("/logements?ville=VILLE_INEXISTANTE_XYZ")
    assert r.status_code == 200
    assert "Aucun logement" in r.text


# ---------------------------------------------------------------- fiche détail

@pbi_required
def test_fiche_detail_existante_200(client):
    r = client.get("/logements/LOG_0001")
    assert r.status_code == 200
    assert "LOG_0001" in r.text
    assert "Historique des taux de commission" in r.text
    assert "Origine des données" in r.text


_PBI_HEADER = ("logement_id;nom_logement_officiel;nom_court;ville;type_logement_id;proprietaire_id;"
               "date_entree_gestion;date_sortie_gestion;sur_hostaway;actif;"
               "forfait_logiciel_consommables_mensuel\n")


def _pbi_fixture(tmp_path):
    """Petit CSV PBI isolé (source PRÉSENTE) pour tester le vrai chemin « logement inconnu »
    indépendamment de la présence du CSV réel (untracked)."""
    p = tmp_path / "PBI_Referentiel_Logements.csv"
    p.write_text(_PBI_HEADER + "LOG_0001;Studio - 46;Studio 46;TOULOUSE;TYPE_001;PROP_0001;2026-01-01;;OUI;OUI;0\n",
                 encoding="utf-8")
    return p


def test_fiche_detail_inconnue_404(client, monkeypatch, tmp_path):
    """Source PRÉSENTE + identifiant inconnu → 404 (jamais la page source-indisponible en 200)."""
    monkeypatch.setattr(svc, "PBI_LOGEMENTS", _pbi_fixture(tmp_path))
    r = client.get("/logements/LOG_INEXISTANT_9999")
    assert r.status_code == 404
    assert "introuvable" in r.text.lower()


def test_fiche_detail_existante_200_fixture(client, monkeypatch, tmp_path):
    """Source PRÉSENTE + logement existant → 200 (chemin « logement trouvé »)."""
    monkeypatch.setattr(svc, "PBI_LOGEMENTS", _pbi_fixture(tmp_path))
    r = client.get("/logements/LOG_0001")
    assert r.status_code == 200
    assert "LOG_0001" in r.text


def test_fiche_detail_source_absente_statut_distinct(client, monkeypatch, tmp_path):
    """Source INDISPONIBLE → statut distinct (page d'erreur, HTTP 200 déjà prévu), jamais confondu
    avec « logement inconnu » (404). Distingue clairement : source absente ≠ logement absent."""
    monkeypatch.setattr(svc, "PBI_LOGEMENTS", tmp_path / "inexistant_PBI.csv")
    r = client.get("/logements/LOG_0001")
    assert r.status_code == 200
    assert "indisponible" in r.text.lower() or "introuvable" in r.text.lower()


@pbi_required
def test_commission_aucune_notion_de_taux_actuel(client):
    r = client.get("/logements/LOG_0001")
    txt = r.text.lower()
    assert "historique des taux de commission" in txt
    assert "commission actuelle" not in txt
    assert "taux actuel" not in txt
    assert "en vigueur" not in txt


# ---------------------------------------------------------------- PBI manquant

def test_pbi_manquant_erreur_sans_fallback(monkeypatch, tmp_path):
    faux = tmp_path / "inexistant_PBI.csv"
    monkeypatch.setattr(svc, "PBI_LOGEMENTS", faux)
    data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["error_message"]
    assert data["rows"] == []          # aucun fallback Excel
    assert data["count_parc"] == 0


def test_pbi_manquant_rendu_erreur(client, monkeypatch, tmp_path):
    faux = tmp_path / "inexistant_PBI.csv"
    monkeypatch.setattr(svc, "PBI_LOGEMENTS", faux)
    r = client.get("/logements")
    assert r.status_code == 200
    assert "indisponible" in r.text.lower() or "introuvable" in r.text.lower()


# ---------------------------------------------------------------- read-only / no write

@pbi_required
def test_lecture_ref_setup_read_only():
    # excel_reader doit toujours ouvrir en lecture seule
    src = (APP_DIR / "readers" / "excel_reader.py").read_text(encoding="utf-8")
    assert "read_only=True" in src


@pbi_required
def test_aucune_ecriture_sources_apres_consultation(client):
    def sig(p: Path):
        st = p.stat()
        return (st.st_size, st.st_mtime_ns)
    before_pbi = sig(PBI_LOGEMENTS)
    before_ref = sig(REF_SETUP)
    client.get("/logements")
    client.get("/logements/LOG_0001")
    assert sig(PBI_LOGEMENTS) == before_pbi, "PBI CSV modifié par une consultation"
    assert sig(REF_SETUP) == before_ref, "REF_Setup modifié par une consultation"


# ---------------------------------------------------------------- données perso

@pbi_required
def test_pas_de_donnees_perso_dans_la_fiche(client):
    r = client.get("/logements/LOG_0001")
    # Aucun email propriétaire ne doit fuiter
    assert "@" not in r.text
    low = r.text.lower()
    assert "adresse_facturation" not in low
    assert "telephone" not in low


def test_code_ne_lit_jamais_proprietaires_ni_gestion():
    """Aucune reconstruction de jointure gestion/propriétaire dans le code applicatif.

    On inspecte les littéraux de chaîne RÉELS (via AST) : les mentions en docstring
    d'interdiction sont tolérées, une feuille effectivement lue ne l'est pas.
    """
    import ast
    FEUILLES_INTERDITES = {"REF_Gestion_Logements_Hist", "REF_Proprietaires"}
    for name in ("services/logements_service.py", "readers/ref_setup_reader.py"):
        tree = ast.parse((APP_DIR / name).read_text(encoding="utf-8"))
        # Docstrings module/fonction/classe = tolérés → on les retire de l'ensemble scanné
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                ds = ast.get_docstring(node, clean=False)
                if ds is not None:
                    docstrings.add(ds)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in docstrings:
                    continue
                assert node.value not in FEUILLES_INTERDITES, (
                    f"{name} lit une feuille interdite : {node.value!r}"
                )


# ---------------------------------------------------------------- SQLite = pas de métier

def test_aucune_donnee_metier_logement_en_sqlite(client, tmp_db):
    client.get("/logements")
    client.get("/logements/LOG_0001")
    conn = sqlite3.connect(str(tmp_db))
    try:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
    finally:
        conn.close()
    # Aucune table métier logement ne doit exister (SQLite = journal uniquement)
    for t in tables:
        assert "logement" not in t.lower(), f"Table métier logement interdite en SQLite : {t}"


def test_service_logements_n_importe_pas_sqlite():
    src = (APP_DIR / "services" / "logements_service.py").read_text(encoding="utf-8")
    assert "get_db" not in src
    assert "sqlite" not in src.lower()


# ---------------------------------------------------------------- health

def test_health_confirme_chemin_ref_setup(client, monkeypatch):
    # Contrat public /health minimal (APP-SEC-1) : le détail par source (dont REF_Setup) vit
    # désormais dans /health/diagnostic, réservé au local et désactivé par défaut.
    import app.config as cfg
    monkeypatch.setattr(cfg, "DIAGNOSTIC_DETAILS_ENABLED", True)
    r = client.get("/health/diagnostic")
    data = r.json()
    assert data["checks"]["ref_setup"]["present"] is True, (
        "REF_Setup doit être présent — chemin corrigé au Lot APP-1"
    )
    assert data["checks"]["ref_setup"]["source"] == "REF_Setup"

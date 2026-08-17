"""APP-1 — Module Logements, alimenté par le référentiel SQLite.

Le service lisait deux exports CSV Power BI et enrichissait la fiche depuis `REF_Setup.xlsm`. Les
trois lectures ont été supprimées : le référentiel vit en base depuis la migration 0029.

Toutes les fixtures construisent un référentiel SQLite synthétique — aucun CSV, aucun classeur.
C'est exactement ce que la migration devait rendre possible.
"""
import ast
import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import logements_service as svc
from app.services import ref_setup_catalogue as cat
from app.services import referentiel_service as referentiel

APP_DIR = Path(__file__).parent.parent / "app"


# ── Fixtures ────────────────────────────────────────────────────────────────────────────────────

_LOGEMENTS = [
    {"logement_id": "LOG_9001", "nom_logement_officiel": "Studio Fixture (A)",
     "nom_court": "Studio Fixture", "ville": "VILLEA", "type_logement_id": "TYPE_901",
     "sur_hostaway": "OUI", "actif": "OUI", "statut_parc": "GERE",
     "forfait_logiciel_consommables_mensuel": "35"},
    {"logement_id": "LOG_9002", "nom_logement_officiel": "T2 Fixture (B)",
     "nom_court": "T2 Fixture", "ville": "VILLEB", "type_logement_id": "TYPE_902",
     "sur_hostaway": "NON", "actif": "NON", "statut_parc": "GERE",
     "forfait_logiciel_consommables_mensuel": "0"},
    {"logement_id": "APPARTEMENT_DIVERS", "nom_court": "Appartement divers",
     "statut_parc": "HORS_PARC_TECHNIQUE", "actif": "OUI"},
    {"logement_id": "LOGEMENT_DIVERS", "nom_court": "Logement divers",
     "statut_parc": "HORS_PARC_TECHNIQUE", "actif": "OUI"},
]

_GESTION = [
    {"gestion_id": "GST_9001", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9001",
     "date_debut": "2025-01-01", "date_fin": "", "statut_gestion": "ACTIF", "source": "fixture"},
]

_TYPES = [{"type_logement_id": "TYPE_901", "type_logement": "Studio fixture"},
          {"type_logement_id": "TYPE_902", "type_logement": "T2 fixture"}]

_TAUX = [
    {"taux_commission_id": "TX_9001", "proprietaire_id": "PROP_9001", "logement_id": "",
     "taux_commission": "0.15", "date_debut": "2025-01-01", "date_fin": "2025-12-31",
     "actif": "OUI"},
    {"taux_commission_id": "TX_9002", "proprietaire_id": "PROP_9001", "logement_id": "",
     "taux_commission": "0.18", "date_debut": "2026-01-01", "date_fin": "", "actif": "OUI"},
]

_PROPRIETAIRES = [
    {"proprietaire_id": "PROP_9001", "nom_proprietaire": "DEMO UN", "actif": "OUI"},
    {"proprietaire_id": "PROP_9002", "nom_proprietaire": "DEMO DEUX", "actif": "OUI"},
]


def construire_referentiel(tmp_path, **contenu):
    """Référentiel SQLite synthétique, marqué comme importé.

    L'entrée dans `ref_setup_imports` compte : le service distingue « référentiel absent » de
    « référentiel vide », et cette distinction se teste.
    """
    db = tmp_path / "referentiel.db"
    apply_migrations(db)
    tables = {
        "ref_logements": contenu.get("logements", _LOGEMENTS),
        "ref_gestion_logements_hist": contenu.get("gestion", _GESTION),
        "ref_types_logements": contenu.get("types", _TYPES),
        "ref_taux_commission": contenu.get("taux", _TAUX),
        "ref_proprietaires": contenu.get("proprietaires", _PROPRIETAIRES),
        "ref_couts_menage_interne": contenu.get("couts_interne", []),
        "ref_couts_standards_menage": contenu.get("couts_std", []),
        "ref_cloture_mensuelle": contenu.get("cloture", []),
    }
    conn = get_db(db)
    try:
        for table, lignes in tables.items():
            if not lignes:
                continue
            cols = cat.PAR_TABLE[table].colonnes
            trous = ", ".join(["?"] * (len(cols) + 1))
            conn.executemany(
                f"INSERT INTO {table} ({', '.join(cols)}, import_id) VALUES ({trous})",
                [tuple(l.get(c, "") for c in cols) + ("IMP-TEST",) for l in lignes])
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-TEST','2026-01-01T00:00:00','fixture','x','IMPORTE',28,0)")
        conn.commit()
    finally:
        conn.close()
    return db


@pytest.fixture
def referentiel_db(tmp_path, monkeypatch):
    db = construire_referentiel(tmp_path)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    return db


@pytest.fixture
def sans_referentiel(tmp_path, monkeypatch):
    """Base migrée mais jamais importée — le cas « référentiel non initialisé »."""
    db = tmp_path / "vide.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    return db


# ── Liste ───────────────────────────────────────────────────────────────────────────────────────

def test_liste_repond_200(client, referentiel_db):
    r = client.get("/logements")
    assert r.status_code == 200
    assert "Logements" in r.text


def test_liste_lit_le_referentiel_sqlite(referentiel_db):
    data = svc.load_list()
    assert data["status"] == "OK"
    assert "SQLite" in data["source"]
    assert data["rows"]


def test_lignes_techniques_absentes_par_defaut(referentiel_db):
    data = svc.load_list()
    ids = [r["logement_id"] for r in data["rows"]]
    assert "APPARTEMENT_DIVERS" not in ids and "LOGEMENT_DIVERS" not in ids
    assert data["count_technique"] == 2


def test_lignes_techniques_presentes_si_filtre(referentiel_db):
    ids = [r["logement_id"] for r in svc.load_list(include_technique=True)["rows"]]
    assert "APPARTEMENT_DIVERS" in ids


def test_compteur_parc_exclut_les_techniques(referentiel_db):
    data = svc.load_list()
    for r in data["rows"]:
        assert r["logement_id"].startswith("LOG_")
    assert data["count_parc"] == len(data["rows"])


def test_rendu_html_technique_toggle(client, referentiel_db):
    assert "APPARTEMENT_DIVERS" not in client.get("/logements").text
    assert "HORS_PARC_TECHNIQUE" in client.get("/logements?include_technique=1").text


# ── Recherche et filtres ────────────────────────────────────────────────────────────────────────

def test_recherche_texte(referentiel_db):
    data = svc.load_list(q="Studio")
    assert [r["logement_id"] for r in data["rows"]] == ["LOG_9001"]


def test_filtre_ville(referentiel_db):
    data = svc.load_list(ville="VILLEB")
    assert [r["logement_id"] for r in data["rows"]] == ["LOG_9002"]


def test_filtre_sans_resultat_etat_vide(client, referentiel_db):
    r = client.get("/logements?ville=VILLE_INEXISTANTE_XYZ")
    assert r.status_code == 200
    assert "Aucun logement" in r.text


# ── Fiche détail ────────────────────────────────────────────────────────────────────────────────

def test_fiche_detail_existante(client, referentiel_db):
    r = client.get("/logements/LOG_9001")
    assert r.status_code == 200
    assert "LOG_9001" in r.text
    assert "Historique des taux de commission" in r.text
    assert "Origine des données" in r.text


def test_fiche_detail_inconnue_404(client, referentiel_db):
    r = client.get("/logements/LOG_INEXISTANT_9999")
    assert r.status_code == 404
    assert "introuvable" in r.text.lower()


def test_detail_expose_les_colonnes_du_referentiel(referentiel_db):
    """`ref_logements` porte toutes les colonnes de l'ancien export, et davantage."""
    detail = svc.load_detail("LOG_9001")
    assert detail["base"]["nom_court"] == "Studio Fixture"
    assert detail["base"]["forfait_logiciel_consommables_mensuel"] == "35"
    assert detail["type_label"] == "Studio fixture"


def test_detail_resout_le_proprietaire(referentiel_db):
    detail = svc.load_detail("LOG_9001")
    assert detail["proprietaire_id"] == "PROP_9001"
    assert detail["gestion_statut"] == svc.GESTION_RESOLU
    assert len(detail["gestion_rattachements"]) == 1


def test_commission_aucune_notion_de_taux_actuel(referentiel_db):
    """Lignes brutes : les deux périodes sont rendues, aucune n'est désignée « actuelle »."""
    detail = svc.load_detail("LOG_9001")
    assert len(detail["commission_rows"]) == 2
    assert {r["taux_commission"] for r in detail["commission_rows"]} == {"0.15", "0.18"}
    assert all("actuel" not in k for r in detail["commission_rows"] for k in r)


# ── Rattachement propriétaire ───────────────────────────────────────────────────────────────────

def test_liste_resout_le_proprietaire(referentiel_db):
    data = svc.load_list()
    ligne = next(r for r in data["rows"] if r["logement_id"] == "LOG_9001")
    assert ligne["proprietaire_id"] == "PROP_9001"
    assert ligne["gestion_statut"] == svc.GESTION_RESOLU
    assert data["filters"]["proprietaires"] == ["PROP_9001"]


def test_filtre_par_proprietaire(referentiel_db):
    assert len(svc.load_list(proprietaire_id="PROP_9001")["rows"]) == 1
    assert svc.load_list(proprietaire_id="PROP_9999")["rows"] == []


def test_gestion_close_reste_attribuee(tmp_path, monkeypatch):
    db = construire_referentiel(tmp_path, gestion=[
        {"gestion_id": "GST_9001", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9001",
         "date_debut": "2025-01-01", "date_fin": "2026-04-26", "statut_gestion": "INACTIF"}])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    ligne = next(r for r in svc.load_list()["rows"] if r["logement_id"] == "LOG_9001")
    assert ligne["proprietaire_id"] == "PROP_9001"
    assert ligne["gestion_statut"] == svc.GESTION_CLOS


def test_rattachement_ambigu_jamais_devine(tmp_path, monkeypatch):
    """Deux rattachements actifs : aucun propriétaire n'est choisi arbitrairement."""
    db = construire_referentiel(tmp_path, gestion=[
        {"gestion_id": "GST_A", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9001",
         "date_debut": "2025-01-01", "statut_gestion": "ACTIF"},
        {"gestion_id": "GST_B", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9002",
         "date_debut": "2026-01-01", "statut_gestion": "ACTIF"}])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    ligne = next(r for r in svc.load_list()["rows"] if r["logement_id"] == "LOG_9001")
    assert ligne["proprietaire_id"] == ""
    assert ligne["gestion_statut"] == svc.GESTION_A_CONTROLER


def test_homonymes_jamais_fusionnes(tmp_path, monkeypatch):
    """Deux propriétaires partageant un nom de famille restent DEUX comptes distincts.

    Garde-fou explicite : le référentiel réel contient une règle bancaire historique qui regroupe
    deux propriétaires sous un même libellé de famille. Rien dans l'application ne doit reproduire
    ce raccourci.
    """
    db = construire_referentiel(
        tmp_path,
        proprietaires=[
            {"proprietaire_id": "PROP_9001", "nom_proprietaire": "MEMENOM", "actif": "OUI"},
            {"proprietaire_id": "PROP_9002", "nom_proprietaire": "MEMENOM", "actif": "OUI"}],
        gestion=[
            {"gestion_id": "GST_A", "logement_id": "LOG_9001", "proprietaire_id": "PROP_9001",
             "date_debut": "2025-01-01", "statut_gestion": "ACTIF"},
            {"gestion_id": "GST_B", "logement_id": "LOG_9002", "proprietaire_id": "PROP_9002",
             "date_debut": "2025-01-01", "statut_gestion": "ACTIF"}])
    monkeypatch.setattr(cfg, "DB_PATH", db)

    par_logement = {r["logement_id"]: r["proprietaire_id"] for r in svc.load_list()["rows"]}
    assert par_logement["LOG_9001"] == "PROP_9001"
    assert par_logement["LOG_9002"] == "PROP_9002"
    assert svc.load_list()["filters"]["proprietaires"] == ["PROP_9001", "PROP_9002"]
    assert referentiel.nom_proprietaire("PROP_9001", db_path=db) == \
           referentiel.nom_proprietaire("PROP_9002", db_path=db) == "MEMENOM", (
        "les noms peuvent coïncider — ce sont les identifiants qui font foi")


def test_logement_sans_rattachement(tmp_path, monkeypatch):
    db = construire_referentiel(tmp_path, gestion=[])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    ligne = next(r for r in svc.load_list()["rows"] if r["logement_id"] == "LOG_9001")
    assert ligne["proprietaire_id"] == ""
    assert ligne["gestion_statut"] == svc.GESTION_ABSENT


# ── Fail-closed : référentiel non initialisé ────────────────────────────────────────────────────

def test_referentiel_absent_est_signale_sans_repli(sans_referentiel):
    """Aucun repli sur Excel : le service dit que le référentiel manque, et pourquoi."""
    data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["code"] == referentiel.REFERENTIEL_ABSENT
    assert "Référentiel Setup" in data["error_message"]
    assert data["rows"] == []


def test_referentiel_absent_rendu_lisible(client, sans_referentiel):
    r = client.get("/logements")
    assert r.status_code == 200
    assert "Référentiel" in r.text
    assert "Traceback" not in r.text


def test_referentiel_absent_sur_la_fiche(client, sans_referentiel):
    r = client.get("/logements/LOG_9001")
    assert r.status_code == 200
    assert "Traceback" not in r.text


def test_referentiel_vide_distinct_de_referentiel_absent(tmp_path, monkeypatch):
    """Importé mais sans logement ≠ jamais importé : deux situations, deux messages."""
    db = construire_referentiel(tmp_path, logements=[])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data.get("code") != referentiel.REFERENTIEL_ABSENT
    assert "aucun logement" in data["error_message"].lower()


# ── Non-dépendance aux fichiers ─────────────────────────────────────────────────────────────────

def test_le_service_ne_lit_plus_aucun_fichier():
    """Garde-fou : ni CSV Power BI, ni REF_Setup, ni lecteur Excel dans ce module."""
    src = (APP_DIR / "services" / "logements_service.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
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
            assert ".csv" not in node.value.lower(), f"lecture CSV résiduelle : {node.value!r}"
            assert ".xlsm" not in node.value.lower(), f"lecture Excel résiduelle : {node.value!r}"
    for interdit in ("PBI_LOGEMENTS", "PBI_GESTION_LOGEMENTS", "read_csv",
                     "ref_setup_reader", "excel_reader"):
        assert interdit not in src, f"dépendance fichier résiduelle : {interdit}"


def test_le_service_passe_par_la_couche_metier():
    """ROUTE → SERVICE MÉTIER → REPOSITORY : jamais de SQL brut dans le service d'écran."""
    src = (APP_DIR / "services" / "logements_service.py").read_text(encoding="utf-8")
    assert "get_db" not in src
    assert "SELECT" not in src.upper().replace("SELECTION", "")
    assert "referentiel_service" in src


def test_consultation_n_ecrit_rien(client, referentiel_db):
    """Une consultation ne modifie pas la base du référentiel."""
    def empreinte():
        st = Path(referentiel_db).stat()
        return (st.st_size, st.st_mtime_ns)
    avant = empreinte()
    client.get("/logements")
    client.get("/logements/LOG_9001")
    assert empreinte() == avant


def test_pas_de_donnees_perso_dans_la_fiche(client, referentiel_db):
    r = client.get("/logements/LOG_9001")
    low = r.text.lower()
    assert "@" not in r.text
    assert "adresse_facturation" not in low
    assert "telephone" not in low


# ── Le référentiel EST désormais en SQLite ──────────────────────────────────────────────────────

def test_le_referentiel_logements_vit_bien_en_sqlite(referentiel_db):
    """Inversion assumée d'un invariant précédent.

    L'ancienne règle « aucune table logement en SQLite » traduisait une architecture où SQLite
    n'était qu'un journal. La migration 0029 en fait la source canonique du référentiel : ces
    tables doivent exister, et c'est leur ABSENCE qui serait le défaut.
    """
    conn = sqlite3.connect(str(referentiel_db))
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert "ref_logements" in tables
    assert "ref_gestion_logements_hist" in tables


def test_health_confirme_chemin_ref_setup(client, monkeypatch):
    monkeypatch.setattr(cfg, "DIAGNOSTIC_DETAILS_ENABLED", True)
    data = client.get("/health/diagnostic").json()
    assert data["checks"]["ref_setup"]["source"] == "REF_Setup"

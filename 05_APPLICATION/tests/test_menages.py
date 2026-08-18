"""APP-2 — Ménages : rapprochement, séparation 3 flux, outrepassage tracé.

Tests requis (PLAN APP-2) :
- pas de doublon au 2e outrepassage (UNIQUE constraint SQLite)
- matching relançable (pipeline runner disponible)
- 3 flux ménage jamais fusionnés (HA tasks / M04 internes / externes = champs séparés)
- Hostaway jamais valorisation ménage (aucun coût Hostaway dans le service)
- outrepassage = motif+date+trace (SQLite menage_overrides + audit_events)
"""
import pytest
from pathlib import Path
from unittest.mock import patch

from app.services import menages_service as svc
from app.readers import menages_reader as reader
from app.db.connection import apply_migrations, get_db


# ---------------------------------------------------------------------------
# Fixture DB isolée
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Base SQLite isolée — patch get_db dans le service pour utiliser la DB de test.

    Une ligne minimale dans `menages_rapprochement` (0038) : `rapprochement()` lit SQLite sans
    repli Excel, donc plus les MASTER réels du projet comme avant cette migration (fuite
    d'isolation désormais impossible plutôt que corrigée par accident).
    """
    import app.config as cfg
    db_path = tmp_path / "test.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, nb_menages_declares_interne_m04, "
            "nb_menages_declares_externe, statut_controle) VALUES (?,?,?,?,?,?,?)",
            ("2026-05", "LOG_0001", "INT_0002", 9, 9, 0, "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    # Le service passe désormais cfg.DB_PATH explicitement : le double accepte l'argument.
    with patch("app.services.menages_service.get_db", lambda *_a, **_k: get_db(db_path)), \
         patch.object(cfg, "DB_PATH", db_path):
        yield db_path


# ---------------------------------------------------------------------------
# Tests lecture
# ---------------------------------------------------------------------------

def test_menages_liste_retourne_lignes(db):
    """Service retourne une liste non vide depuis les MASTER réels."""
    data = svc.load_list()
    assert data["status"] == "OK"
    assert len(data["rows"]) > 0


def test_menages_detail_retourne_ligne(db):
    """load_detail retourne les données pour une ligne connue."""
    rows = reader.read_tableau_comparaison()
    assert rows, "MASTER vide — impossible de tester le détail"
    r = rows[0]
    detail = svc.load_detail(
        str(r.get("mois") or ""),
        str(r.get("logement_id") or ""),
        str(r.get("intervenant_id") or ""),
    )
    assert detail is not None
    assert detail["status"] == "OK"
    assert detail["ligne"]["logement_id"] == str(r.get("logement_id") or "").strip()


def test_menages_detail_inconnu_404(db):
    """load_detail retourne None pour une clé inexistante (→ 404 propre)."""
    detail = svc.load_detail("9999-99", "LOG_INCONNU", "INT_INCONNU")
    assert detail is None


def test_menages_source_inchangee_apres_lecture(db):
    """La source MASTER n'est pas modifiée après lecture (sha256 stable)."""
    import hashlib
    path = reader.MASTER_RAPPROCHEMENT_MENAGES
    assert path.exists(), "MASTER rapprochement absent"
    sha_avant = hashlib.sha256(path.read_bytes()).hexdigest()
    taille_avant = path.stat().st_size
    svc.load_list()
    svc.load_list(mois="2026-05")
    sha_apres = hashlib.sha256(path.read_bytes()).hexdigest()
    assert sha_apres == sha_avant, "Source modifiée après lecture — INTERDIT"
    assert path.stat().st_size == taille_avant


def test_menages_source_absente_etat_erreur(db):
    """Quand la source MASTER est absente, le service retourne status=ERROR (pas d'exception)."""
    with patch.object(reader, "rapprochement_available", return_value=False):
        data = svc.load_list()
    assert data["status"] == "ERROR"
    assert data["rows"] == []


# ---------------------------------------------------------------------------
# Test : 3 flux jamais fusionnés
# ---------------------------------------------------------------------------

def test_menages_trois_flux_non_fusionnes(db):
    """Les 3 flux (HA tasks / M04 internes / externes) restent des champs séparés.

    Règle PLAN APP-2 : ne jamais fusionner ménage interne / ménage externe / tâches Hostaway.
    """
    data = svc.load_list()
    assert data["status"] == "OK"
    for row in data["rows"]:
        # Les 3 champs existent distinctement (jamais fusionnés en un seul)
        assert "nb_menages_tasks_hostaway_completed" in row, "Champ HA tasks absent"
        assert "nb_menages_declares_interne_m04" in row, "Champ M04 internes absent"
        assert "nb_menages_declares_externe" in row, "Champ externes absent"
        # Jamais remplacés par un agrégat unique
        assert "nb_menages_total_fusionne" not in row, "Fusion interdite détectée"


# ---------------------------------------------------------------------------
# Test : Hostaway jamais valorisation coût
# ---------------------------------------------------------------------------

def test_menages_hostaway_jamais_valorisation():
    """Le service n'expose aucune valeur de coût issue des données Hostaway.

    Règle REGLES_METIER §3 : ne jamais utiliser Hostaway pour valoriser le coût réel ménage.
    """
    service_src = (Path(__file__).parent.parent / "app" / "services" / "menages_service.py").read_text(encoding="utf-8")
    reader_src = (Path(__file__).parent.parent / "app" / "readers" / "menages_reader.py").read_text(encoding="utf-8")
    forbidden = ["hostaway_cost", "h6_cost", "cleaning_cost_hostaway", "cost_hostaway"]
    for pattern in forbidden:
        assert pattern not in service_src.lower(), f"Valorisation Hostaway détectée dans service : {pattern}"
        assert pattern not in reader_src.lower(), f"Valorisation Hostaway détectée dans reader : {pattern}"


# ---------------------------------------------------------------------------
# Tests outrepassage
# ---------------------------------------------------------------------------

def test_menages_outrepassage_ecrit_sqlite(db):
    """Outrepassage écrit dans menage_overrides + audit_events."""
    result = svc.enregistrer_outrepassage("2026-05", "LOG_0001", "INT_0002", "Écart justifié — forfait spécial")
    assert result["ok"] is True

    conn = get_db(db)
    try:
        row = conn.execute(
            "SELECT motif, statut_override FROM menage_overrides WHERE mois=? AND logement_id=? AND intervenant_id=?",
            ("2026-05", "LOG_0001", "INT_0002"),
        ).fetchone()
        assert row is not None
        assert row["motif"] == "Écart justifié — forfait spécial"
        assert row["statut_override"] == "JUSTIFIE"

        audit = conn.execute(
            "SELECT action, details FROM audit_events WHERE action='MENAGE_OVERRIDE' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert audit is not None
        assert "LOG_0001" in audit["details"]
    finally:
        conn.close()


def test_menages_outrepassage_motif_obligatoire(db):
    """Outrepassage avec motif vide est refusé (ok=False) — aucune écriture SQLite."""
    result = svc.enregistrer_outrepassage("2026-05", "LOG_0001", "INT_0002", "")
    assert result["ok"] is False
    assert "motif" in result["error"].lower()

    conn = get_db(db)
    try:
        row = conn.execute("SELECT id FROM menage_overrides").fetchone()
        assert row is None, "Aucun enregistrement ne doit exister après un motif vide"
    finally:
        conn.close()


def test_menages_outrepassage_pas_de_doublon(db):
    """2 outrepassages sur la même clé → 1 seule ligne (UNIQUE constraint — pas de doublon)."""
    svc.enregistrer_outrepassage("2026-05", "LOG_0001", "INT_0002", "Premier motif")
    svc.enregistrer_outrepassage("2026-05", "LOG_0001", "INT_0002", "Motif mis à jour")

    conn = get_db(db)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM menage_overrides WHERE mois='2026-05' AND logement_id='LOG_0001' AND intervenant_id='INT_0002'"
        ).fetchone()[0]
        assert count == 1, "UNIQUE doit garantir 1 seule ligne par clé"
        row = conn.execute(
            "SELECT motif FROM menage_overrides WHERE mois='2026-05' AND logement_id='LOG_0001' AND intervenant_id='INT_0002'"
        ).fetchone()
        assert row["motif"] == "Motif mis à jour"
    finally:
        conn.close()


def test_menages_outrepassage_trace_complete(db):
    """Outrepassage : motif + timestamp + audit_events présents (trace complète)."""
    svc.enregistrer_outrepassage("2026-05", "LOG_0002", "INT_0003", "Prestataire externe confirmé")

    conn = get_db(db)
    try:
        row = conn.execute(
            "SELECT ts, motif, statut_override FROM menage_overrides WHERE logement_id='LOG_0002'"
        ).fetchone()
        assert row is not None
        assert row["ts"] is not None and len(str(row["ts"])) >= 10, "Timestamp manquant"
        assert row["motif"] == "Prestataire externe confirmé"
        assert row["statut_override"] == "JUSTIFIE"

        audit_count = conn.execute(
            "SELECT COUNT(*) FROM audit_events WHERE action='MENAGE_OVERRIDE'"
        ).fetchone()[0]
        assert audit_count >= 1, "Trace audit_events manquante"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tests route HTTP
# ---------------------------------------------------------------------------
#
# `client` isole l'application (tmp_db) mais ne seed aucune donnée Ménages : avant la migration
# SQLite, ces routes lisaient (sans le vouloir) les classeurs réels du projet via les chemins par
# défaut de `cfg.MASTER_*`. `rapprochement()` étant désormais SQLite uniquement, cette fuite
# d'isolation ne peut plus se produire — d'où ce seed minimal explicite, demandé seulement par les
# tests qui en ont besoin (ne pas passer `tmp_db` par-dessus la base isolée des tests `db` ci-dessus).

def _seed_rapprochement_minimal(tmp_db) -> None:
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, statut_controle) VALUES (?,?,?,?,?)",
            ("2026-05", "LOG_0001", "INT_0002", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()


def test_menages_get_liste_200(client):
    r = client.get("/menages")
    assert r.status_code == 200


def test_menages_get_liste_avec_filtres_200(client):
    r = client.get("/menages?mois=2026-05&type_intervenant=INTERNE")
    assert r.status_code == 200


def test_menages_detail_connu_200(client):
    """GET sur une ligne existante retourne 200."""
    rows = reader.read_tableau_comparaison()
    if not rows:
        pytest.skip("Aucune donnée dans MASTER")
    r = rows[0]
    url = f"/menages/{r['mois']}/{r['logement_id']}/{r['intervenant_id']}"
    resp = client.get(url)
    assert resp.status_code == 200


def test_menages_detail_inconnu_404_route(client, tmp_db):
    _seed_rapprochement_minimal(tmp_db)
    r = client.get("/menages/9999-99/LOG_INCONNU/INT_INCONNU")
    assert r.status_code == 404


def test_menages_post_outrepasser_motif_vide_422(client):
    """POST outrepassage avec motif vide → 422."""
    rows = reader.read_tableau_comparaison()
    if not rows:
        pytest.skip("Aucune donnée dans MASTER")
    r = rows[0]
    url = f"/menages/{r['mois']}/{r['logement_id']}/{r['intervenant_id']}/outrepasser"
    resp = client.post(url, data={"motif": ""})
    assert resp.status_code == 422

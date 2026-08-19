"""APP-2b+ — Chaîne ménages COMPLÈTE sur copies, infos PDF, filtre propriétaire, cache.

Couvre :
  * la chaîne complète (préflight, ordre imposé, mode réel gardé, source déclarations obligatoire) ;
  * les informations « factures de ménage externes » (extraction MANUELLE — aucun faux compteur PDF) ;
  * le filtre propriétaire « Prénom NOM » (référentiel) avec repli explicite ;
  * l'invalidation publique du cache ;
  * les 3 actions distinctes de l'écran Ménages.

Aucun test ne touche un fichier métier réel ni la vraie base app.db. Un test E2E réel (marqué
skipif) exécute la chaîne entière sur copies quand l'interpréteur moteur et les données sont présents.
"""
import hashlib
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.db.connection import get_db
from app.readers import menages_reader as reader
from app.services import menages_service as svc
from app.services import menages_chaine_service as chaine


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mini_projet(tmp_path, monkeypatch):
    """Projet minimal : sources cœur factices + dossier PDF avec 2 fichiers."""
    root = tmp_path / "projet"
    for rel in chaine.SOURCES_COEUR:
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"FIXTURE " + rel.encode())
    pdfdir = root / chaine.PDF_DIR_REL
    pdfdir.mkdir(parents=True, exist_ok=True)
    (pdfdir / "Facture mai Aissata.pdf").write_bytes(b"%PDF-1.4 fixture")
    (pdfdir / "Facture mai Mounir.pdf").write_bytes(b"%PDF-1.4 fixture")
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "MENAGES_PDF_DIR", pdfdir)
    monkeypatch.setattr(cfg, "MENAGES_CHAINE_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    return root


# ── Chaîne : préflight & ordre ───────────────────────────────────────────────

def test_chaine_ordre_impose_hostaway_avant_lot6c(mini_projet):
    """L'extraction Hostaway précède lot6c (qui lit VUE_COMPTAGE) ; 6d après les 3 sources."""
    noms = [s["name"] for s in chaine.STEPS_CHAINE]
    assert noms.index("hostaway_stub") < noms.index("lot6c_menages_externes")
    assert noms.index("lot6b_declarations_internes") < noms.index("lot6d_rapprochement")
    assert noms.index("lot6c_menages_externes") < noms.index("lot6d_rapprochement")
    assert noms[-1] == "lot11_controles"


def test_chaine_preparer_liste_sources_et_pdf(mini_projet):
    plan = chaine.preparer_chaine()
    assert plan["nb_pdf"] == 2
    assert plan["pdf_dossier_relatif"] == cfg.MENAGES_PDF_DIR_REL
    assert plan["reel_active"] is False
    # etapes = liste de dicts {name, libelle} (libellés utilisateur).
    assert [s["name"] for s in chaine.STEPS_CHAINE] == [e["name"] for e in plan["etapes"]]
    assert all(e["libelle"] for e in plan["etapes"])


def test_chaine_mode_reel_bloque_sans_executer(mini_projet, tmp_db):
    """MODE_REEL avec flag False : statut BLOQUE, aucune exécution, run tracé."""
    assert cfg.MENAGES_REAL_RECALC_ENABLED is False
    res = chaine.executer_chaine(mode=chaine.MODE_REEL, declarations_csv="x", db_path=tmp_db)
    assert res["statut"] == chaine.STATUT_BLOQUE
    assert res["ok"] is False
    assert res["erreur_code"] == "E_MODE_REEL_DESACTIVE"


def test_chaine_source_declarations_obligatoire(mini_projet, tmp_db):
    """Mode copies sans source déclarations : refus explicite (jamais de reprise silencieuse)."""
    res = chaine.executer_chaine(mode=chaine.MODE_COPIES, declarations_csv="", db_path=tmp_db)
    assert res["statut"] == chaine.STATUT_ECHEC
    assert res["erreur_code"] == "E_SOURCE_DECLARATIONS_ABSENTE"


def test_chaine_source_coeur_absente_echec(tmp_path, monkeypatch, tmp_db):
    """Source cœur manquante : la chaîne échoue proprement avant toute exécution."""
    root = tmp_path / "vide"
    root.mkdir()
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "MENAGES_PDF_DIR", root / "pdf")
    monkeypatch.setattr(cfg, "MENAGES_CHAINE_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    res = chaine.executer_chaine(mode=chaine.MODE_COPIES, declarations_csv="a;b;c\n1;2;3\n", db_path=tmp_db)
    assert res["statut"] == chaine.STATUT_ECHEC
    assert res["erreur_code"] == "E_SOURCE_ABSENTE"


# ── Infos PDF : extraction manuelle, aucun faux compteur ─────────────────────

def test_pdf_info_source_absente_mode_inconnu(mini_projet, monkeypatch):
    """Source absente : mode inconnu, aucun faux compteur, dossier relatif exposé."""
    monkeypatch.setattr(reader, "externes", lambda: reader.SourceMenages(
        reader.EtatSource("externes", "Externes", "MASTER_FACT_MEN_MenagesExternes.xlsx",
                          "MASTER", reader.ETAT_FICHIER_ABSENT)))
    monkeypatch.setattr(reader, "diagnostic_pdf", lambda: reader.SourceMenages(
        reader.EtatSource("diag", "Diag", "MASTER_FACT_MEN_MenagesExternes.xlsx",
                          "DIAGNOSTIC_PDF", reader.ETAT_FICHIER_ABSENT)))
    info = svc.load_pdf_externes_info()
    assert info["extraction_automatique"] is False
    assert info["mode_extraction"] is None
    assert info["dossier_relatif"] == cfg.MENAGES_PDF_DIR_REL
    assert info["nb_pdf_presents"] == 2
    assert info["nb_pdf_reconnus"] == 0
    assert "C:\\" not in info["dossier_relatif"]


def test_pdf_info_prestataires_depuis_master(mini_projet, monkeypatch):
    """Prestataires/période viennent du MASTER Lot6c, pas d'une lecture PDF."""
    lignes = [
        {"nom_prestataire": "Kandia DIABATE", "mois": "2026-05", "nom_fichier_source": "Facture mai Aissata.pdf"},
        {"nom_prestataire": "MH Entreprise", "mois": "2026-05", "nom_fichier_source": "Facture mai Mounir.pdf"},
        {"nom_prestataire": "INCONNU", "mois": "2026-05", "nom_fichier_source": "Facture mai Mounir.pdf"},
    ]
    lignes_pdf = [dict(r, source_document="lot6c — TS — PDF_AUTOMATIQUE") for r in lignes]
    monkeypatch.setattr(reader, "externes", lambda: reader.SourceMenages(
        reader.EtatSource("externes", "Externes", "F", "MASTER", reader.ETAT_OK, len(lignes_pdf), "2026-06-16 10:32"),
        lignes_pdf))
    monkeypatch.setattr(reader, "diagnostic_pdf", lambda: reader.SourceMenages(
        reader.EtatSource("diag", "Diag", "F", "DIAGNOSTIC_PDF", reader.ETAT_FICHIER_ABSENT)))
    # `mode_extraction_externes()` lit désormais `facture_pdf_diagnostics` (0040) directement, plus
    # `source_document` dans les lignes `externes()` — stub indépendant, même mécanisme réel.
    monkeypatch.setattr(reader, "mode_extraction_externes", lambda: "PDF_AUTOMATIQUE")
    info = svc.load_pdf_externes_info()
    assert "Kandia DIABATE" in info["prestataires_detectes"]
    assert "INCONNU" not in info["prestataires_detectes"]
    assert info["periode_couverte"] == ["2026-05"]
    assert info["nb_fichiers_transcrits"] == 2
    assert info["derniere_extraction"] == "2026-06-16 10:32"
    assert info["mode_extraction"] == "PDF_AUTOMATIQUE"


# ── Filtre propriétaire : « Prénom NOM » + repli ─────────────────────────────

def test_libelle_proprietaire_connu(monkeypatch):
    monkeypatch.setattr(reader, "noms_proprietaires", lambda: {"PROP_0006": "Caroline PONS"})
    assert reader.libelle_proprietaire("PROP_0006") == "Caroline PONS"


def test_libelle_proprietaire_inconnu_fallback(monkeypatch):
    monkeypatch.setattr(reader, "noms_proprietaires", lambda: {})
    assert reader.libelle_proprietaire("PROP_9999") == "Propriétaire non identifié — PROP_9999"


def test_libelle_proprietaire_vide():
    assert reader.libelle_proprietaire("") == ""


# ── Cache : invalidation publique ────────────────────────────────────────────

def test_invalidate_menages_cache_vide_tout(monkeypatch):
    reader._CACHE[("x", "y", 1, 2)] = "sentinelle"
    reader._CACHE_PROPRIETAIRES = {"PROP_0001": "Test"}
    svc.invalidate_menages_cache()
    assert reader._CACHE == {}
    assert reader._CACHE_PROPRIETAIRES is None


# ── UI : 3 actions distinctes ────────────────────────────────────────────────

def test_ui_actualiser_affichage_redirige(client):
    r = client.post("/menages/actualiser-affichage", data={"mois": "2026-05"}, follow_redirects=False)
    assert r.status_code == 303
    assert "affichage=actualise" in r.headers["location"]


def test_ui_chaine_page_200(client):
    r = client.get("/menages/chaine")
    assert r.status_code == 200
    assert "Actualiser les sources et recalculer" in r.text
    assert "MENAGES_REAL_RECALC_ENABLED" in r.text


def test_ui_trois_actions_presentes(client):
    r = client.get("/menages")
    assert "Actualiser l'affichage" in r.text
    assert "Simuler avec les derniers exports" in r.text
    assert "Actualiser les sources et recalculer" in r.text


def test_ui_libelle_pdf_renomme(client, tmp_db):
    """« Factures prestataires » remplacé par « Factures de ménage externes ».

    `rapprochement()` lit SQLite (0038) sans repli Excel : une ligne minimale est nécessaire pour
    que l'écran dépasse l'alerte « Source indisponible » et affiche les libellés testés ici.
    """
    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, statut_controle) VALUES (?,?,?,?,?)",
            ("2026-05", "LOG_0001", "INT_0002", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    r = client.get("/menages")
    assert "Factures de ménage externes" in r.text
    assert "Factures prestataires" not in r.text


def test_ui_aucun_chemin_absolu(client):
    for url in ("/menages", "/menages/diagnostic", "/menages/chaine"):
        t = client.get(url).text
        assert "C:\\" not in t and "OneDrive" not in t, f"Chemin absolu exposé sur {url}"


# ── E2E réel (skip si moteur/données absents) ────────────────────────────────

def _sources_reelles_disponibles() -> bool:
    root = Path(cfg.PROJECT_ROOT)
    coeur_ok = all((root / rel).exists() for rel in chaine.SOURCES_COEUR)
    return coeur_ok and Path(cfg.MENAGES_ENGINE_PYTHON).exists()


@pytest.mark.skipif(not _sources_reelles_disponibles(),
                    reason="Interpréteur moteur ou sources réelles absents")
def test_chaine_e2e_reelle_sur_copies(tmp_db, tmp_path, monkeypatch):
    """Exécute la chaîne complète sur copies et prouve que les fichiers réels ne bougent pas."""
    monkeypatch.setattr(cfg, "MENAGES_CHAINE_WORKSPACE", tmp_path / "ws")
    # Précondition dataset SQLite (§4 mission) : CleaningTasks/M04 ne sont plus des masters copiés,
    # `preparer_chaine` exige `menages_taches_enrichies`/`menages_declarations_internes` non vides.
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-E2E-001", "2026-05", "LOG_0001", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle) VALUES (?,?,?,?,?)",
            ("2026-05", "LOG_0001", "INT_0002", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    temoins = [Path(cfg.PROJECT_ROOT) / rel for rel in chaine.SOURCES_COEUR]
    avant = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in temoins}
    res = chaine.executer_chaine(mode=chaine.MODE_COPIES, db_path=tmp_db)
    assert res["reel_intact"] is True
    for p in temoins:
        assert hashlib.sha256(p.read_bytes()).hexdigest() == avant[p]
    if res["statut"] == chaine.STATUT_SUCCES:
        assert all(v["ok"] for v in res["verif_sorties"].values())

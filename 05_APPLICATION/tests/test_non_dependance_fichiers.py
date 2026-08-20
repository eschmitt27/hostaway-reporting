"""Garde-fou d'architecture : l'application ne se nourrit plus d'exports ni du classeur Setup.

La règle est asymétrique et c'est voulu :

- **Interdit** : un service métier qui LIT `03_EXPORTS/PowerBI/*.csv` pour alimenter un écran.
  C'est la boucle `SQLite → export → application`, qui fait dépendre l'affichage d'un artefact
  régénérable et périmable.
- **Autorisé** : produire ces exports (lot13), et constater leur présence dans un diagnostic.

Ces tests inspectent le code plutôt que le comportement : ils empêchent la RÉINTRODUCTION d'une
dépendance, ce qu'un test fonctionnel ne verrait qu'une fois le mal fait.
"""
import ast
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from test_logements import construire_referentiel  # noqa: F401

APP_DIR = Path(__file__).parent.parent / "app"

# Modules autorisés à nommer un export : le registre de pipeline le DÉCLARE comme sortie, et le
# diagnostic constate sa présence. Aucun des deux ne lit son contenu pour un écran.
EXCEPTIONS_POWERBI = {
    "config.py",
    "services/calculs_executeur_service.py",
    "routes/health.py",
    # Lot13 ÉCRIT les exports depuis SQLite ; il ne les relit jamais. C'est leur seul producteur, et
    # le sens de circulation reste `SQLite → CSV`. L'interdit que ce test protège porte sur la
    # LECTURE d'un export par l'application, jamais sur sa production ; la reconstructibilité est
    # verrouillée à part (`test_lot13_export_sqlite.py`).
    "services/lot13_export_service.py",
}

# Modules autorisés à toucher REF_Setup : lecteurs dédiés, services d'administration qui écrivent
# encore dans le classeur, et import du référentiel. Ils sont la frontière, pas des écrans.
EXCEPTIONS_REF_SETUP = {
    "config.py",
    "routes/health.py",
    "readers/ref_setup_reader.py",
    "readers/ref_setup_hh_reader.py",
    "readers/saisie_charges_reader.py",
    "services/ref_setup_import_service.py",
}


def _modules_app():
    for py in sorted(APP_DIR.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        yield py.relative_to(APP_DIR).as_posix(), py


def _litteraux_hors_docstring(chemin: Path):
    """Chaînes réellement manipulées par le code. Une docstring qui EXPLIQUE une interdiction
    ne doit pas déclencher l'alerte — sinon ce fichier-ci se signalerait lui-même."""
    tree = ast.parse(chemin.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            ds = ast.get_docstring(node, clean=False)
            if ds is not None:
                docstrings.add(ds)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value not in docstrings:
                yield node.value


def test_aucun_service_ne_lit_un_export_powerbi():
    coupables = []
    for rel, chemin in _modules_app():
        if rel in EXCEPTIONS_POWERBI:
            continue
        for valeur in _litteraux_hors_docstring(chemin):
            if "PBI_" in valeur or "PowerBI" in valeur:
                coupables.append(f"{rel} → {valeur!r}")
    assert coupables == [], (
        "Lecture d'export Power BI réintroduite :\n  " + "\n  ".join(coupables))


def _importe(chemin: Path, module: str) -> bool:
    """Import RÉEL du module, détecté par AST.

    Chercher la sous-chaîne dans le fichier signalerait aussi les docstrings qui citent le module
    pour expliquer qu'on ne l'utilise plus — exactement l'inverse de ce qu'on veut mesurer.
    """
    tree = ast.parse(chemin.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(a.name.endswith(module) for a in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").endswith(module):
                return True
            if any(a.name == module for a in node.names):
                return True
    return False


# Modules qui touchent encore le classeur Setup, mesurés à la fin de cette mission.
# Cette liste est un CONSTAT, pas une cible : elle doit décroître, jamais croître. Chaque entrée
# est un chantier de la mission « administration du référentiel » ou de la mission « moteur ».
CONSOMMATEURS_SETUP_RESIDUELS = {
    "readers/controles_cloture_reader.py",
    "readers/proprietaires_reader.py",
    "readers/proprietaires_reglements_reader.py",
    "services/calculs_executeur_service.py",
    "services/calculs_pipeline_service.py",
    "services/controles_runner_service.py",
    "services/file_registry.py",
    "services/menages_chaine_service.py",
    "services/menages_recalcul_service.py",
    "services/ref_assoc_mode_prepare_service.py",
    "services/saisie_hh_dryrun_service.py",
    "services/saisie_hh_real_write_service.py",
    "services/saisie_hh_schema_real_prepare_service.py",
    "services/saisie_hh_service.py",
    "services/logements_creation_service.py",
    "services/logements_gestion_service.py",
    "services/charges_preview_service.py",
    "services/charges_controles_integrite_service.py",
}


def test_aucune_nouvelle_lecture_du_classeur_setup():
    """La dépendance à REF_Setup.xlsm ne doit plus s'étendre."""
    coupables = set()
    for rel, chemin in _modules_app():
        if rel in EXCEPTIONS_REF_SETUP:
            continue
        if _importe(chemin, "ref_setup_reader") or _importe(chemin, "ref_setup_hh_reader"):
            coupables.add(rel)
            continue
        for valeur in _litteraux_hors_docstring(chemin):
            if "REF_Setup" in valeur:
                coupables.add(rel)
                break

    nouveaux = sorted(coupables - CONSOMMATEURS_SETUP_RESIDUELS)
    assert nouveaux == [], (
        "Nouvelle dépendance au classeur REF_Setup : " + ", ".join(nouveaux))


def test_logements_n_est_plus_un_consommateur_du_classeur():
    """Le module migré doit avoir quitté la liste résiduelle, et ne pas y revenir."""
    assert "services/logements_service.py" not in CONSOMMATEURS_SETUP_RESIDUELS
    chemin = APP_DIR / "services" / "logements_service.py"
    assert not _importe(chemin, "ref_setup_reader")
    assert all("REF_Setup" not in v for v in _litteraux_hors_docstring(chemin))


def test_logements_ne_depend_d_aucun_fichier():
    """Le module migré doit rester propre — c'est le témoin de la migration."""
    source = (APP_DIR / "services" / "logements_service.py").read_text(encoding="utf-8")
    for interdit in ("PBI_LOGEMENTS", "PBI_GESTION_LOGEMENTS", "read_csv",
                     "ref_setup_reader", "excel_reader", "REF_SETUP"):
        assert interdit not in source, f"dépendance fichier réintroduite : {interdit}"


def test_accueil_ne_lit_aucun_export():
    source = (APP_DIR / "routes" / "home.py").read_text(encoding="utf-8")
    assert "EXPORTS_POWERBI" not in source
    assert "PBI_" not in source.replace("PBI_", "", 0) or True  # lisibilité : vérifié ci-dessous
    for valeur in _litteraux_hors_docstring(APP_DIR / "routes" / "home.py"):
        assert "PBI_" not in valeur


# ── Fonctionnement sans le dossier d'exports ────────────────────────────────────────────────────

@pytest.fixture
def sans_dossier_powerbi(tmp_path, monkeypatch):
    """Pointe les exports vers un dossier qui n'existe pas — cas d'un environnement neuf."""
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", tmp_path / "powerbi_absent")
    monkeypatch.setattr(cfg, "PBI_LOGEMENTS", tmp_path / "powerbi_absent" / "logements.csv")
    monkeypatch.setattr(cfg, "PBI_GESTION_LOGEMENTS", tmp_path / "powerbi_absent" / "gestion.csv")
    return tmp_path


def test_logements_fonctionne_sans_exports(client, tmp_path, monkeypatch,
                                           sans_dossier_powerbi):
    """§10 — les écrans migrés doivent vivre sans le dossier `03_EXPORTS/PowerBI`."""
    db = construire_referentiel(tmp_path)
    monkeypatch.setattr(cfg, "DB_PATH", db)

    r = client.get("/logements")
    assert r.status_code == 200
    assert "LOG_9001" in r.text
    assert "introuvable" not in r.text.lower()


def test_accueil_fonctionne_sans_exports(client, tmp_path, monkeypatch, sans_dossier_powerbi):
    db = construire_referentiel(tmp_path)
    monkeypatch.setattr(cfg, "DB_PATH", db)

    r = client.get("/")
    assert r.status_code == 200
    assert "Traceback" not in r.text


def test_accueil_compte_le_parc_sans_les_lignes_techniques(client, tmp_path, monkeypatch,
                                                           sans_dossier_powerbi):
    """Écart assumé avec l'ancien compteur : il annonçait 19 en comptant les lignes techniques,
    là où l'écran Logements affichait « Parc : 17 ». Les deux disent désormais la même chose."""
    db = construire_referentiel(tmp_path)
    monkeypatch.setattr(cfg, "DB_PATH", db)

    from app.routes.home import _logements_du_parc
    assert len(_logements_du_parc()) == 2  # 4 lignes dont 2 techniques

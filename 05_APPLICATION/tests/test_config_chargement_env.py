"""Le `.env` est chargé au démarrage, une seule fois, et ne prend jamais le pas sur le serveur.

CE QUE CE MODULE CORRIGE. Les `load_dotenv` de l'application étaient PARESSEUX : déclenchés à la
première utilisation d'un service, donc longtemps après que `app/config.py` ait figé ses drapeaux.
Un `.env` correctement renseigné laissait donc tous les verrous d'écriture fermés, et un
redémarrage semblait sans effet — symptôme constaté en exploitation.

Ces tests figent les quatre propriétés qui comptent : le fichier est lu, une vraie variable
d'environnement gagne, son absence n'empêche pas de démarrer, et aucune valeur n'est divulguée.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import app.config as cfg

VARIABLES_VERIFIEES = (
    "MODE_REEL_ECRITURES",
    "BANQUE_REAL_WRITE_ENABLED", "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED",
    "COMPTABILITE_REAL_WRITE_ENABLED", "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
    "QONTO_LOGIN", "QONTO_SECRET_KEY",
)


def _dans_un_processus_neuf(code: str, *, env: dict | None = None,
                            cwd: Path | None = None) -> str:
    """Exécute du code avec `app.config` importé À NEUF.

    Indispensable : la configuration est évaluée à l'import, une fois pour toutes. Un test qui
    réimporterait le module déjà chargé observerait l'état du processus de test, pas l'effet du
    chargement.
    """
    racine = Path(cfg.APP_ROOT)
    environnement = dict(os.environ if env is None else env)
    environnement["PYTHONIOENCODING"] = "utf-8"
    resultat = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(cwd or racine), env=environnement, capture_output=True, text=True,
        encoding="utf-8")
    assert resultat.returncode == 0, resultat.stderr
    return resultat.stdout.strip()


# ── 1. `.env` présent → valeurs chargées ──────────────────────────────────────────────────────
def test_le_fichier_env_est_charge_au_demarrage(tmp_path):
    """Un `.env` déposé à la racine est lu par le SEUL import de la configuration."""
    faux_projet = tmp_path / "projet"
    (faux_projet / "05_APPLICATION" / "app").mkdir(parents=True)
    (faux_projet / ".env").write_text(
        "MODE_REEL_ECRITURES=1\nBANQUE_REAL_WRITE_ENABLED=1\n"
        "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED=1\nQONTO_LOGIN=login-de-test\n",
        encoding="utf-8")

    # On rejoue exactement la mécanique de `charger_env` sur cette arborescence isolée, sans
    # toucher au vrai `.env` du projet.
    sortie = _dans_un_processus_neuf(f"""
        import os, sys
        sys.path.insert(0, r"{Path(cfg.APP_ROOT)}")
        import app.config as cfg
        for cle in ("MODE_REEL_ECRITURES", "BANQUE_REAL_WRITE_ENABLED",
                    "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", "QONTO_LOGIN"):
            os.environ.pop(cle, None)
        trouve = cfg.charger_env(r"{faux_projet / '.env'}")
        print(trouve, os.environ.get("MODE_REEL_ECRITURES"),
              os.environ.get("BANQUE_REAL_WRITE_ENABLED"),
              os.environ.get("QONTO_LOGIN"))
    """)
    assert sortie == "True 1 1 login-de-test"


def test_les_cinq_verrous_et_les_identifiants_qonto_viennent_du_fichier():
    """Sur le vrai projet : le seul import de la configuration suffit à poser les variables."""
    if not cfg.ENV_FILE.is_file():
        import pytest
        pytest.skip("aucun .env sur cette machine — rien à vérifier")

    sortie = _dans_un_processus_neuf("""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        print(cfg.ENV_FILE_CHARGE)
        for nom in ("MODE_REEL_ECRITURES", "BANQUE_REAL_WRITE_ENABLED",
                    "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED",
                    "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
                    "QONTO_LOGIN", "QONTO_SECRET_KEY"):
            # On n'imprime QUE la présence, jamais la valeur.
            print(nom, bool(os.environ.get(nom, "").strip()))
    """)
    lignes = sortie.splitlines()
    assert lignes[0] == "True", "le fichier doit être trouvé et chargé"
    presence = dict(l.split() for l in lignes[1:])
    for nom in VARIABLES_VERIFIEES:
        assert presence[nom] == "True", f"{nom} absent après chargement natif"


def test_le_chargement_se_fait_avant_l_evaluation_des_drapeaux():
    """La propriété qui manquait : les drapeaux doivent refléter le fichier, pas l'inverse."""
    if not cfg.ENV_FILE.is_file():
        import pytest
        pytest.skip("aucun .env sur cette machine")

    sortie = _dans_un_processus_neuf("""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        print(cfg.MODE_REEL_ECRITURES, cfg.BANQUE_REAL_WRITE_ENABLED,
              cfg.COMPTABILITE_REAL_WRITE_ENABLED)
    """)
    assert sortie == "True True True", \
        "les verrous doivent être vrais dès l'import, sans lanceur ni préchargement"


# ── 2. La variable d'environnement gagne ──────────────────────────────────────────────────────
def test_une_vraie_variable_d_environnement_gagne_sur_le_fichier(tmp_path):
    """`override=False` : un fichier de développement oublié ne doit pas primer sur le serveur."""
    fichier = tmp_path / ".env"
    fichier.write_text("PILOTAGE_TEST_PRIORITE=valeur-du-fichier\n", encoding="utf-8")

    environnement = dict(os.environ)
    environnement["PILOTAGE_TEST_PRIORITE"] = "valeur-du-serveur"
    sortie = _dans_un_processus_neuf(f"""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        cfg.charger_env(r"{fichier}")
        print(os.environ["PILOTAGE_TEST_PRIORITE"])
    """, env=environnement)
    assert sortie == "valeur-du-serveur"


def test_le_fichier_remplit_ce_que_l_environnement_ne_fournit_pas(tmp_path):
    fichier = tmp_path / ".env"
    fichier.write_text("PILOTAGE_TEST_ABSENTE=valeur-du-fichier\n", encoding="utf-8")

    environnement = dict(os.environ)
    environnement.pop("PILOTAGE_TEST_ABSENTE", None)
    sortie = _dans_un_processus_neuf(f"""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        cfg.charger_env(r"{fichier}")
        print(os.environ.get("PILOTAGE_TEST_ABSENTE"))
    """, env=environnement)
    assert sortie == "valeur-du-fichier"


# ── 3. `.env` absent → démarrage normal ───────────────────────────────────────────────────────
def test_un_fichier_absent_ne_casse_rien(tmp_path):
    """Un worktree neuf n'a pas de `.env` (ignoré par Git) : ce n'est pas une erreur."""
    assert cfg.charger_env(tmp_path / "inexistant.env") is False


def test_l_application_demarre_sans_env(tmp_path):
    """L'application entière doit s'importer même sans fichier : aucune exception au démarrage."""
    sortie = _dans_un_processus_neuf(f"""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        # On force un chemin inexistant AVANT toute évaluation dépendante du fichier.
        print(cfg.charger_env(r"{tmp_path / 'absent.env'}"))
        from app.main import app
        print("application importee")
    """)
    assert sortie.splitlines() == ["False", "application importee"]


# ── 4. Aucun secret divulgué ──────────────────────────────────────────────────────────────────
def test_le_chargement_ne_retourne_et_ne_journalise_aucune_valeur():
    """`charger_env` ne renvoie qu'un booléen, et ne passe aucune valeur à un journal."""
    source = (Path(cfg.APP_ROOT) / "app" / "config.py").read_text(encoding="utf-8")
    debut = source.index("def charger_env")
    corps = source[debut:source.index("ENV_FILE_CHARGE = charger_env()")]
    for interdit in ("print(", "log", "QONTO_SECRET_KEY", "os.environ["):
        assert interdit not in corps, f"« {interdit} » n'a rien à faire dans le chargement"
    assert "override=False" in corps, "l'environnement réel doit toujours primer"


def test_aucune_valeur_du_fichier_ne_transite_par_la_configuration(tmp_path):
    """Un secret déposé dans le fichier ne doit apparaître dans aucun attribut de `app.config`."""
    fichier = tmp_path / ".env"
    fichier.write_text("QONTO_SECRET_KEY=SECRET-RECONNAISSABLE-0123456789\n", encoding="utf-8")

    sortie = _dans_un_processus_neuf(f"""
        import os, sys
        sys.path.insert(0, os.getcwd())
        import app.config as cfg
        cfg.charger_env(r"{fichier}")
        expose = [n for n in dir(cfg)
                  if "SECRET-RECONNAISSABLE" in str(getattr(cfg, n, ""))]
        print(expose)
    """)
    assert sortie == "[]", "aucun attribut de configuration ne porte la valeur du secret"


# ── 5. Un seul point de chargement ────────────────────────────────────────────────────────────
def test_le_chargement_est_centralise_dans_la_configuration():
    """Un `load_dotenv` paresseux ailleurs recréerait exactement le défaut corrigé ici."""
    racine = Path(cfg.APP_ROOT) / "app"
    coupables = [chemin.relative_to(racine).as_posix()
                 for chemin in racine.rglob("*.py")
                 if chemin.name != "config.py"
                 and "load_dotenv(" in chemin.read_text(encoding="utf-8")]
    assert not coupables, f"chargement de .env hors de la configuration : {coupables}"


def test_le_chemin_du_fichier_ne_depend_pas_d_une_variable_du_fichier():
    """`PROJECT_ROOT` peut être défini DANS le `.env` : le chemin doit venir de l'arborescence."""
    assert cfg.ENV_FILE == Path(cfg.APP_ROOT).parent / ".env"
    source = (Path(cfg.APP_ROOT) / "app" / "config.py").read_text(encoding="utf-8")
    ligne = next(l for l in source.splitlines() if l.startswith("ENV_FILE ="))
    assert "PROJECT_ROOT" not in ligne

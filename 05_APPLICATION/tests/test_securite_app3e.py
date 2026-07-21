"""APP-3E — Audit de sécurité consolidé (structurel + comportemental).

Aucun champ bancaire, aucun appel réseau, aucun writer réel, autoescape Jinja, protection CSV,
disclaimer « déclaration humaine, pas une confirmation bancaire » présent partout, version optimiste.
"""
import inspect
from pathlib import Path

import app.config as cfg
from app.services import charges_affectations_service as aff
from app.services import charges_controles_service as ctrl
from app.services import fournisseur_rattachements_service as fl
from app.services import proprietaires_paiement_service as pay
from app.services import proprietaires_releve_cycle_service as cycle_svc

_SERVICES_APP3E = [aff, ctrl, fl, pay, cycle_svc]
_MIGRATIONS_APP3E = ["0011_charges_affectations.sql", "0012_releve_cycle_paiement.sql",
                     "0013_fournisseur_rattachements.sql"]


def test_aucun_champ_bancaire_dans_les_modeles():
    """Aucune colonne IBAN/RIB/compte/BIC dans les migrations APP-3E (hors commentaires d'interdiction)."""
    mig_dir = Path(cfg.APP_ROOT) / "app" / "db" / "migrations"
    for nom in _MIGRATIONS_APP3E:
        for ligne in (mig_dir / nom).read_text(encoding="utf-8").splitlines():
            code = ligne.split("--")[0].lower()   # ignore les commentaires SQL
            for interdit in ("iban", "rib", "bic", "swift", "numero_compte", "compte_bancaire"):
                assert interdit not in code, f"{interdit} trouvé dans {nom} : {ligne}"


def test_aucun_appel_reseau_dans_les_services():
    for mod in _SERVICES_APP3E:
        src = inspect.getsource(mod).lower()
        for interdit in ("import requests", "import httpx", "urllib.request", "socket.socket",
                         "qonto", "credit_mutuel", "sepa", "webhook"):
            assert interdit not in src, f"{interdit} trouvé dans {mod.__name__}"


def test_aucune_ecriture_fichier_reel_dans_les_services():
    """Les services APP-3E n'ouvrent aucun fichier en écriture (open(...,'w'), .write() fichier)."""
    for mod in (aff, fl, pay, cycle_svc):
        src = inspect.getsource(mod)
        assert "open(" not in src, f"open( trouvé dans {mod.__name__}"


def test_flags_write_tous_false():
    for attr in dir(cfg):
        if attr.isupper() and "WRITE" in attr:
            val = getattr(cfg, attr)
            if isinstance(val, bool):
                assert val is False, f"{attr} doit rester False"


def test_jinja_autoescape_actif():
    from fastapi.templating import Jinja2Templates
    t = Jinja2Templates(directory=str(cfg.TEMPLATES_DIR))
    # select_autoescape -> True pour .html
    assert t.env.autoescape is not False


def test_export_csv_neutralise_toutes_les_amorces():
    from app.services.proprietaires_releve_export_service import _cellule_sure, _AMORCES_FORMULE
    for amorce in _AMORCES_FORMULE:
        cell = _cellule_sure(f"{amorce}danger")
        assert cell.startswith("'"), f"amorce {amorce!r} non neutralisée"


def test_version_optimiste_presente_partout():
    for mod in (aff, fl, pay, cycle_svc):
        src = inspect.getsource(mod)
        assert "version_attendue" in src, f"pas de garde de version dans {mod.__name__}"
        assert "rollback" in src, f"pas de rollback transactionnel dans {mod.__name__}"


def test_disclaimer_declaration_humaine_dans_liste_et_export(client):
    r = client.get("/proprietaires-reglements/a-payer")
    assert "preuve bancaire" in r.text.lower() or "ordre bancaire" in r.text.lower()
    r2 = client.get("/proprietaires-reglements/a-payer/export.csv")
    assert "ne constitue pas un ordre bancaire" in r2.text


def test_disclaimer_declaration_humaine_dans_fiche_et_historique(client):
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": "PROP_SEC", "mois": "2026-01"}, follow_redirects=False)
    opaque = r.headers["location"].rsplit("/", 1)[-1]
    fiche = client.get(f"/proprietaires-reglements/{opaque}/releve")
    assert "preuve bancaire" in fiche.text.lower()
    histo = client.get(f"/proprietaires-reglements/{opaque}/historique")
    assert "confirmation bancaire" in histo.text.lower()


def test_aucun_id_sqlite_dans_les_urls_de_reponse(client):
    """Les redirections/liens exposent des identifiants opaques, jamais un id SQLite entier."""
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": "PROP_SEC2", "mois": "2026-01"}, follow_redirects=False)
    loc = r.headers["location"]
    tail = loc.rsplit("/", 1)[-1]
    assert tail.startswith("REG-") and not tail.isdigit()

"""APP-3F — Audit de sécurité consolidé (structurel + comportemental).

Aucune donnée bancaire sensible, aucun appel réseau/API bancaire, aucun writer, aucun virement,
export protégé anti-injection CSV, mention de non-preuve bancaire, identifiants opaques.
"""
import inspect
from pathlib import Path

import app.config as cfg
from app.readers import rapprochement_bancaire_reader as contrat
from app.services import rapprochement_candidats_service as cand
from app.services import rapprochement_reglements_service as rap

_MODULES_APP3F = [contrat, cand, rap]
_MIGRATION = "0014_rapprochement_reglements.sql"


class _FakeSource:
    def __init__(self, etat, lignes):
        self.etat = type("E", (), {"etat": etat})()
        self.lignes = lignes


def test_01_aucun_champ_bancaire_dans_la_migration():
    mig = (Path(cfg.APP_ROOT) / "app" / "db" / "migrations" / _MIGRATION).read_text(encoding="utf-8")
    for ligne in mig.splitlines():
        code = ligne.split("--")[0].lower()
        for interdit in ("iban", "rib", "bic", "swift", "numero_compte", "compte_bancaire", "libelle_brut"):
            assert interdit not in code, f"{interdit} dans {_MIGRATION} : {ligne}"


def test_02_aucun_appel_reseau_ou_api_bancaire():
    for mod in _MODULES_APP3F:
        src = inspect.getsource(mod).lower()
        for interdit in ("import requests", "import httpx", "urllib.request", "socket.socket",
                         "qonto", "credit_mutuel", "sepa", "webhook", "beneficiaire", "virement("):
            assert interdit not in src, f"{interdit} dans {mod.__name__}"


def test_03_aucune_ecriture_fichier():
    for mod in _MODULES_APP3F:
        src = inspect.getsource(mod)
        assert "open(" not in src, f"open( dans {mod.__name__}"


def test_04_contrat_ne_lit_jamais_le_fichier_directement():
    """Le contrat délègue à banques_reader, ne construit aucun chemin de fichier bancaire."""
    src = inspect.getsource(contrat)
    assert "openpyxl" not in src and "load_workbook" not in src
    assert "MASTER_BANQUE" not in src   # jamais le chemin réel manipulé ici


def test_05_flags_write_tous_false():
    for attr in dir(cfg):
        if attr.isupper() and "WRITE" in attr:
            v = getattr(cfg, attr)
            if isinstance(v, bool):
                assert v is False, f"{attr} doit rester False"


def test_06_banque_real_write_reste_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED is False


def test_07_contrat_ne_fuit_aucune_donnee_bancaire(monkeypatch):
    from app.readers import banques_reader as b
    monkeypatch.setattr(b, "mouvements", lambda: _FakeSource(contrat.ETAT_OK, [
        {"mouvement_id": "M1", "date_operation": "2026-01-05", "montant": -10.0, "sens": "DEBIT",
         "libelle": "x", "compte_id": "CM_02211_00021321603", "libelle_brut": "IBAN FR76 1234 5678"}]))
    m = contrat.source().mouvements[0]
    blob = str(vars(m))
    for fuite in ("FR76", "02211", "00021321603"):
        assert fuite not in blob


def test_08_mouvement_opaque_jamais_le_brut(monkeypatch):
    from app.readers import banques_reader as b
    monkeypatch.setattr(b, "mouvements", lambda: _FakeSource(contrat.ETAT_OK, [
        {"mouvement_id": "M-SECRET-123", "date_operation": "2026-01-05", "montant": -10.0,
         "sens": "DEBIT", "libelle": "x"}]))
    m = contrat.source().mouvements[0]
    assert m.mouvement_opaque.startswith("MVT-") and "M-SECRET-123" not in m.mouvement_opaque


def test_09_export_neutralise_injection_csv(client, monkeypatch):
    # créer un rapprochement puis injecter une amorce de formule dans le commentaire
    r = client.post("/proprietaires-reglements/demarrer",
                    data={"proprietaire_id": "PROP_INJ", "mois": "2026-01"}, follow_redirects=False)
    opq = r.headers["location"].rsplit("/", 1)[-1]
    client.post(f"/proprietaires-reglements/{opq}/rapprochement/anomalie", data={"motif": "=CMD('x')"})
    txt = client.get("/proprietaires-reglements/a-payer/rapprochement-export.csv").text
    assert "'=CMD" in txt   # amorce neutralisée par une quote de tête


def test_10_export_sans_donnee_bancaire(client):
    txt = client.get("/proprietaires-reglements/a-payer/rapprochement-export.csv").text
    low = txt.lower()
    assert "iban" not in low and "rib" not in low and "bic" not in low and "compte" not in low


def test_11_export_mention_non_preuve_bancaire(client):
    txt = client.get("/proprietaires-reglements/a-payer/rapprochement-export.csv").text
    assert "ne constitue ni un ordre de paiement ni une preuve bancaire certifiée" in txt


def test_12_toutes_amorces_neutralisees():
    from app.services.proprietaires_releve_export_service import _cellule_sure, _AMORCES_FORMULE
    for a in _AMORCES_FORMULE:
        assert _cellule_sure(f"{a}x").startswith("'")


def test_13_version_optimiste_et_rollback():
    src = inspect.getsource(rap)
    assert "version_attendue" in src and "rollback" in src


def test_14_confirmation_jamais_automatique():
    """Structurel : aucune fonction du service ne confirme sans passer par confirmer() humain."""
    src = inspect.getsource(rap)
    # la seule voie vers ST_RAPPROCHE est confirmer(), qui exige reglement_paye + mouvement_present + sens_sortant
    assert "def confirmer(" in src
    assert "reglement_paye" in src and "mouvement_present" in src and "sens_sortant" in src


def test_15_aucun_id_sqlite_expose_dans_les_routes():
    from app.routes import proprietaires_reglements as routes
    src = inspect.getsource(routes)
    # les routes rapprochement utilisent releve_opaque / mouvement_opaque, jamais un id entier
    assert "/{rapprochement_id}" not in src and "/{mouvement_id}" not in src

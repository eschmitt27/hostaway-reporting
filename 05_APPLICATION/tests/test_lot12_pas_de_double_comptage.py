"""Lot12 — garde formelle : une seule source de vérité comptable après une facture ÉMISE.

`lot12_prefactures_service` ne doit JAMAIS générer d'écriture comptable ni de facture — il ne
prépare/présente/contrôle QUE des PRÉFACTURES (`statut_generation` toujours `PREFACTURE_CONTROLE`).
Le chemin comptable réel reste `ventes_lot12_adapter_service` (qui lit `lot10_net_reglement`
directement, jamais les tables Lot12 0047) → `comptabilite_ecritures_service`/`factures_service`.
Ces deux chemins ne doivent jamais se rejoindre : si `lot12_prefactures_service` importait un
service d'écriture, une même donnée économique pourrait être comptabilisée deux fois (une fois via
Lot10 direct, une fois via une hypothétique « validation » de préfacture).
"""
from __future__ import annotations

from pathlib import Path

FORBIDDEN_IMPORTS = (
    "comptabilite_ecritures_service", "factures_service", "comptabilite_reconciliations_service",
)


def test_lot12_prefactures_service_importe_aucun_service_ecriture():
    """Recherche de VRAIES instructions d'import (pas une mention en docstring/commentaire)."""
    import re

    src = (Path(__file__).parent.parent / "app" / "services" / "lot12_prefactures_service.py"
          ).read_text(encoding="utf-8")
    violations = [f for f in FORBIDDEN_IMPORTS
                 if re.search(rf"^[ \t]*(?:import|from)[ \t]+.*\b{f}\b", src, re.MULTILINE)]
    assert not violations, (
        f"lot12_prefactures_service ne doit importer aucun service d'écriture comptable : "
        f"{violations}")


def test_construire_ne_touche_aucune_table_comptable(tmp_db):
    """Un `construire()` réel ne doit créer aucune écriture ni facture — vérifié en base, pas
    seulement par lecture du code source."""
    from app.db.connection import get_db
    from app.services import lot12_prefactures_service as svc

    conn = get_db(tmp_db)
    try:
        avant_ecritures = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='comptabilite_ecritures'"
        ).fetchone()
        table_existe = avant_ecritures is not None and conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='comptabilite_ecritures'"
        ).fetchone() is not None
        n_avant = (conn.execute("SELECT COUNT(*) FROM comptabilite_ecritures").fetchone()[0]
                  if table_existe else 0)
        n_factures_avant = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res

    conn = get_db(tmp_db)
    try:
        n_apres = (conn.execute("SELECT COUNT(*) FROM comptabilite_ecritures").fetchone()[0]
                  if table_existe else 0)
        n_factures_apres = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()

    assert n_apres == n_avant
    assert n_factures_apres == n_factures_avant


def test_statut_generation_toujours_prefacture_controle(tmp_db):
    from app.db.connection import get_db
    from app.services import lot12_prefactures_service as svc

    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('L10-T','SUCCES',1)")
        conn.execute(
            "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
            "montant_du_conciergerie, reste_a_payer_conciergerie, nb_reservations) "
            "VALUES ('L10-T','2026-06','LOG_A','PROP_A', 100, 100, 2)")
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES ('IMP','2026-01-01T00:00:00Z','t','t','IMPORTE')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, mode_facturation, actif, import_id) "
            "VALUES ('PROP_A', 'MENSUEL', 'OUI', 'IMP')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, actif, statut_parc, import_id) "
            "VALUES ('LOG_A', 'OUI', 'GERE', 'IMP')")
        conn.commit()
        svc.construire(db_path=tmp_db)
        rows = [dict(r) for r in conn.execute(
            "SELECT DISTINCT statut_generation FROM lot12_prefactures_entete")]
    finally:
        conn.close()

    assert rows == [{"statut_generation": "PREFACTURE_CONTROLE"}]

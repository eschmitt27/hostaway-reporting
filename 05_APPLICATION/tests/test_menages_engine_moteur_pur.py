"""Mission 9 — preuve A/B réelle : `intervenant_menage_compte_service.tarif_menage` (chaîne de
production, SQLite réelle via `tmp_db`) contre un appel direct au moteur pur
(`lib_menage_costs.resolve_fixed_internal_cost`, importé exactement comme le fait le service —
même `sys.path.insert` déjà établi dans `intervenant_menage_compte_service.py`).

Complète aussi la temporalité (§22/§23 de la mission) au niveau SQLite réel : un tarif futur
(2027) inséré dans `ref_couts_menage_interne` ne modifie jamais un tarif déjà résolu pour 2026,
même rejoué après coup.
"""
import sys

import app.config as cfg
from app.db.connection import get_db
from app.services import intervenant_menage_compte_service as compte

_TRAVAIL_DIR = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)
from lib_menage_costs import resolve_fixed_internal_cost  # noqa: E402

INTERVENANT = "FRS-INT-AB-0001"


def _importer_referentiel(db_path, lignes):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES (?,?,?,?,?)",
            ("IMP-AB-0001", "2026-08-26T00:00:00Z", "REF_Setup.xlsm", "TEST", "IMPORTE"))
        for i, ligne in enumerate(lignes):
            conn.execute(
                "INSERT INTO ref_couts_menage_interne (cout_menage_interne_id, actif, "
                "date_debut_validite, date_fin_validite, montant_interne_standard, import_id) "
                "VALUES (?,?,?,?,?,?)",
                (f"CIM_AB_{i:04d}", "OUI", ligne.get("date_debut_validite"),
                 ligne.get("date_fin_validite"), str(ligne["montant"]), "IMP-AB-0001"))
        conn.commit()
    finally:
        conn.close()


def test_production_identique_au_moteur_direct(tmp_db):
    """A/B : `compte.tarif_menage` (production, lit `ref_couts_menage_interne` via SQLite) doit
    produire exactement ce que donnerait un appel direct à `resolve_fixed_internal_cost` avec les
    mêmes lignes."""
    _importer_referentiel(tmp_db, [{"date_debut_validite": "2026-01-01",
                                    "date_fin_validite": None, "montant": 30.0}])

    resultat_production = compte.tarif_menage(INTERVENANT, "LOG_AB", "2026-07-01", db_path=tmp_db)

    rows_directes = [{
        "cout_interne_id": "CIM_AB_0000", "intervenant_id": "", "logement_id": "",
        "type_logement_id": "", "cout_fixe_par_menage": "30.0", "date_debut": "2026-01-01",
        "date_fin": "", "actif": "OUI",
    }]
    resultat_direct = resolve_fixed_internal_cost(
        rows_directes, intervenant_id=INTERVENANT, logement_id="LOG_AB", type_logement_id="",
        ref_date="2026-07-01", nb_menages=1)

    assert resultat_production["statut"] == resultat_direct.status == "OK"
    assert resultat_production["montant"] == resultat_direct.total == 30.0


def test_tarif_futur_2027_ne_change_jamais_2026_meme_rejoue(tmp_db):
    _importer_referentiel(tmp_db, [
        {"date_debut_validite": "2026-01-01", "date_fin_validite": "2026-12-31", "montant": 30.0},
    ])
    avant = compte.tarif_menage(INTERVENANT, "LOG_AB", "2026-06-15", db_path=tmp_db)
    assert avant["statut"] == "OK"
    assert avant["montant"] == 30.0

    # Le tarif 2027 est désormais introduit dans le référentiel réel (nouvelle ligne SQLite).
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_couts_menage_interne (cout_menage_interne_id, actif, "
            "date_debut_validite, montant_interne_standard, import_id) VALUES (?,?,?,?,?)",
            ("CIM_AB_2027", "OUI", "2027-01-01", "35.0", "IMP-AB-0001"))
        conn.commit()
    finally:
        conn.close()

    apres = compte.tarif_menage(INTERVENANT, "LOG_AB", "2026-06-15", db_path=tmp_db)
    assert apres["montant"] == avant["montant"] == 30.0

    futur = compte.tarif_menage(INTERVENANT, "LOG_AB", "2027-03-01", db_path=tmp_db)
    assert futur["statut"] == "OK"
    assert futur["montant"] == 35.0

"""APP-3E — Affectation logique des charges et contrôles associés.

Le montant/coût reste exclusivement produit par le moteur — jamais recalculé ici. Ces tests portent
uniquement sur l'affectation humaine (fournisseur, logement, propriétaire, statut) et les règles de
contrôle pures qui en découlent.
"""
import pytest

from app.services import charges_affectations_service as aff
from app.services import charges_controles_service as ctrl
from app.services import fournisseurs_referentiel_service as frs


def test_01_affecter_charge(tmp_db):
    a = aff.affecter("CHG-001", fournisseur_id_opaque=None, logement_id="LOG1",
                     proprietaire_id="PROP_X", mois="2026-01", nature="menage", db_path=tmp_db)
    assert a["affectation_id_opaque"].startswith("CHA-") and a["charge_id"] == "CHG-001"


def test_02_double_affectation_meme_charge_refusee(tmp_db):
    aff.affecter("CHG-002", proprietaire_id="P", mois="2026-01", db_path=tmp_db)
    with pytest.raises(aff.AffectationRefusee):
        aff.affecter("CHG-002", proprietaire_id="P", mois="2026-01", db_path=tmp_db)


def test_03_charge_id_vide_refuse(tmp_db):
    with pytest.raises(aff.AffectationRefusee):
        aff.affecter("", db_path=tmp_db)


def test_04_modification_version_optimiste(tmp_db):
    a = aff.affecter("CHG-004", db_path=tmp_db)
    a2 = aff.modifier(a, nature="entretien", version_attendue=a["version"], db_path=tmp_db)
    assert a2["nature"] == "entretien" and a2["version"] == 2


def test_05_version_obsolete_refusee(tmp_db):
    a = aff.affecter("CHG-005", db_path=tmp_db)
    aff.modifier(a, nature="x", version_attendue=a["version"], db_path=tmp_db)
    with pytest.raises(aff.AffectationRefusee):
        aff.modifier(a, nature="y", version_attendue=a["version"], db_path=tmp_db)


def test_06_historique_conserve(tmp_db):
    a = aff.affecter("CHG-006", db_path=tmp_db)
    aff.modifier(a, commentaire="x", version_attendue=a["version"], db_path=tmp_db)
    hist = aff.historique(a["affectation_id_opaque"], db_path=tmp_db)
    assert len(hist) >= 2


def test_07_lister_par_proprietaire_mois(tmp_db):
    aff.affecter("CHG-007a", proprietaire_id="P", mois="2026-01", db_path=tmp_db)
    aff.affecter("CHG-007b", proprietaire_id="P", mois="2026-01", db_path=tmp_db)
    rows = aff.lister_par_proprietaire_mois("P", "2026-01", db_path=tmp_db)
    assert len(rows) == 2


def test_08_source_absente_bloque():
    r = ctrl.evaluer({}, None, source_etat="ABSENTE")
    assert r["bloquants"] == ["CHARGE_SOURCE_ABSENTE"]


def test_09_source_vide_bloque():
    r = ctrl.evaluer({}, None, source_etat="VIDE")
    assert r["bloquants"] == ["CHARGE_SOURCE_VIDE"]


def test_10_schema_invalide_bloque():
    r = ctrl.evaluer({}, None, source_etat="ILLISIBLE")
    assert r["bloquants"] == ["CHARGE_SCHEMA_INVALIDE"]


def test_11_montant_invalide_bloque():
    r = ctrl.evaluer({"montant": None, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"}, None)
    assert "CHARGE_MONTANT_INVALIDE" in r["bloquants"]


def test_12_mois_absent_bloque():
    r = ctrl.evaluer({"montant": 10, "logement_id": "L", "proprietaire_id": "P"}, None)
    assert "CHARGE_MOIS_ABSENT" in r["bloquants"]


def test_13_logement_absent_bloque():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "proprietaire_id": "P"}, None)
    assert "CHARGE_LOGEMENT_ABSENT" in r["bloquants"]


def test_14_proprietaire_absent_bloque():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L"}, None)
    assert "CHARGE_PROPRIETAIRE_ABSENT" in r["bloquants"]


def test_15_fournisseur_absent_bloque(tmp_db):
    a = aff.affecter("CHG-015", logement_id="L", nature="menage", db_path=tmp_db)
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     a, db_path=tmp_db)
    assert "CHARGE_FOURNISSEUR_ABSENT" in r["bloquants"]


def test_16_fournisseur_inactif_bloque(tmp_db):
    f = frs.creer("F Inactif", "MENAGE", db_path=tmp_db)
    frs.desactiver(f, version_attendue=f["version"], db_path=tmp_db)
    a = aff.affecter("CHG-016", fournisseur_id_opaque=f["fournisseur_id_opaque"],
                     logement_id="L", nature="menage", db_path=tmp_db)
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     a, db_path=tmp_db)
    assert "CHARGE_FOURNISSEUR_INACTIF" in r["bloquants"]


def test_17_doublon_bloque():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     None, doublon=True)
    assert "CHARGE_DOUBLON" in r["bloquants"]


def test_18_refacturable_sans_justif_est_informatif(tmp_db):
    f = frs.creer("F Actif", "MENAGE", db_path=tmp_db)
    a = aff.affecter("CHG-018", fournisseur_id_opaque=f["fournisseur_id_opaque"], logement_id="L",
                     nature="menage", refacturable=True, db_path=tmp_db)
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     a, db_path=tmp_db)
    assert "CHARGE_REFACTURABLE_NON_JUSTIFIEE" in r["informatifs"]
    assert "CHARGE_REFACTURABLE_NON_JUSTIFIEE" not in r["bloquants"]   # jamais bloquant


def test_19_affectation_incomplete_bloque(tmp_db):
    a = aff.affecter("CHG-019", db_path=tmp_db)   # pas de logement ni nature
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     a, db_path=tmp_db)
    assert "CHARGE_AFFECTATION_INCOMPLETE" in r["bloquants"]


def test_20_source_modifiee_bloque(tmp_db):
    a = aff.affecter("CHG-020", source_empreinte="hash1", logement_id="L", nature="menage", db_path=tmp_db)
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P",
                      "source_empreinte": "hash2"}, a, db_path=tmp_db)
    assert "CHARGE_SOURCE_MODIFIEE" in r["bloquants"]


def test_21_apres_cloture_bloque():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P",
                      "ajoutee_apres_cloture": True}, None, mois_cloture=True)
    assert "CHARGE_APRES_CLOTURE" in r["bloquants"]


def test_22_ajustement_sans_motif_bloque():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P",
                      "est_ajustement": True, "motif": ""}, None)
    assert "AJUSTEMENT_SANS_MOTIF" in r["bloquants"]


def test_23_divergence_moteur_est_informative():
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P",
                      "divergence_moteur": True}, None)
    assert "CHARGE_DIVERGENCE_MOTEUR" in r["informatifs"]
    assert "CHARGE_DIVERGENCE_MOTEUR" not in r["bloquants"]


def test_24_charge_conforme_sans_anomalie(tmp_db):
    f = frs.creer("F OK", "MENAGE", db_path=tmp_db)
    a = aff.affecter("CHG-024", fournisseur_id_opaque=f["fournisseur_id_opaque"], logement_id="L",
                     nature="menage", db_path=tmp_db)
    r = ctrl.evaluer({"montant": 10, "mois": "2026-01", "logement_id": "L", "proprietaire_id": "P"},
                     a, db_path=tmp_db)
    assert r["conforme"] is True


def test_25_jamais_de_calcul_financier():
    """Contrôle structurel : aucune opération arithmétique sur un montant dans le service de contrôle."""
    import inspect
    src = inspect.getsource(ctrl)
    for motif in (" + montant", " - montant", "* montant", "somme("):
        assert motif not in src

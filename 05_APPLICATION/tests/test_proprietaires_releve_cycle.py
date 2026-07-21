"""APP-3E — Cycle de préparation du relevé : machine à états, snapshot, dérive."""
import pytest

from app.services import proprietaires_releve_cycle_service as cycle_svc

OPAQUE = "REG-test0001"


def _c(db):
    return cycle_svc.creer_ou_charger(OPAQUE, db_path=db)


def test_01_creation_etat_non_demarre(tmp_db):
    c = _c(tmp_db)
    assert c["etat_cycle"] == cycle_svc.ETAT_NON_DEMARRE


def test_02_creer_ou_charger_idempotent(tmp_db):
    c1 = _c(tmp_db)
    c2 = _c(tmp_db)
    assert c1["id"] == c2["id"]


def test_03_demarrer(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    assert c["etat_cycle"] == cycle_svc.ETAT_EN_PREPARATION


def test_04_validation_directe_depuis_non_demarre_refusee(tmp_db):
    c = _c(tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.valider(c, {}, version_attendue=c["version"], db_path=tmp_db)


def test_05_validation_avec_bloquant_refusee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.valider(c, {}, bloquants=["CHARGE_MONTANT_INVALIDE"], version_attendue=c["version"],
                          db_path=tmp_db)


def test_06_validation_sans_bloquant(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"proprietaire_id": "P", "mois": "2026-01", "net_exploitation": 100},
                          version_attendue=c["version"], db_path=tmp_db)
    assert c["etat_cycle"] == cycle_svc.ETAT_VALIDE
    assert c["snapshot_empreinte"]


def test_07_snapshot_immuable_apres_validation(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"net_exploitation": 100}, version_attendue=c["version"], db_path=tmp_db)
    empreinte_avant = c["snapshot_empreinte"]
    # aucune fonction publique ne permet de modifier le snapshot d'un relevé VALIDE sans réouverture
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.valider(c, {"net_exploitation": 999}, version_attendue=c["version"], db_path=tmp_db)
    c2 = cycle_svc.charger(OPAQUE, db_path=tmp_db)
    assert c2["snapshot_empreinte"] == empreinte_avant


def test_08_derive_reservation_ajoutee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"logements": [{"id": "L1", "nb_reservations": 2}]},
                          version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"logements": [{"id": "L1", "nb_reservations": 3}]})
    assert d["derive"] is True and "logements" in d["champs_modifies"]


def test_09_derive_charge_ajoutee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"charges": [1]}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"charges": [1, 2]})
    assert d["derive"] is True and "charges" in d["champs_modifies"]


def test_10_derive_fournisseur_modifie(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"fournisseurs": ["FRS-a"]}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"fournisseurs": ["FRS-b"]})
    assert d["derive"] is True


def test_11_derive_acompte_ajoute(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"acomptes": []}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"acomptes": [{"montant": 50}]})
    assert d["derive"] is True and "acomptes" in d["champs_modifies"]


def test_12_derive_reversement_ajoute(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"reversements": []}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"reversements": [{"montant": 10}]})
    assert d["derive"] is True


def test_13_derive_statut_moteur_modifie(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"statut_moteur": "CLOTURE"}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"statut_moteur": "OUVERT"})
    assert d["derive"] is True


def test_14_derive_statut_app5c_modifie(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"statut_app5c": "VALIDE"}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"statut_app5c": "EN_COURS"})
    assert d["derive"] is True


def test_15_pas_de_derive_sans_changement(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"net_exploitation": 100}, version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"net_exploitation": 100})
    assert d["derive"] is False


def test_16_reouverture_sans_motif_refusee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {}, version_attendue=c["version"], db_path=tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.rouvrir(c, "", version_attendue=c["version"], db_path=tmp_db)


def test_17_reouverture_avec_motif(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {}, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.rouvrir(c, "données évoluées", version_attendue=c["version"], db_path=tmp_db)
    assert c["etat_cycle"] == cycle_svc.ETAT_ROUVERT


def test_18_action_sur_version_obsolete_refusee(tmp_db):
    c = _c(tmp_db)
    cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)   # rejoué, version périmée


def test_19_transition_rejouee_refusee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)   # déjà EN_PREPARATION


def test_20_annulation_sans_motif_refusee(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    with pytest.raises(cycle_svc.CycleRefuse):
        cycle_svc.annuler(c, "", version_attendue=c["version"], db_path=tmp_db)


def test_21_annulation_avec_motif(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.annuler(c, "erreur de saisie", version_attendue=c["version"], db_path=tmp_db)
    assert c["etat_cycle"] == cycle_svc.ETAT_ANNULE and c["motif_annulation"] == "erreur de saisie"


def test_22_historique_append_only(tmp_db):
    from app.db.connection import get_db
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    conn = get_db(tmp_db)
    rows = conn.execute(
        "SELECT * FROM proprietaires_releve_evenements WHERE releve_id_opaque=? AND type_evenement='CYCLE_TRANSITION'",
        (OPAQUE,)).fetchall()
    conn.close()
    assert len(rows) == 1


def test_23_snapshot_sans_chemin_ni_iban(tmp_db):
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"net_exploitation": 100, "iban": "FR7612345"},
                          version_attendue=c["version"], db_path=tmp_db)
    assert "iban" not in c["snapshot_json"]   # champ non whitelisté -> jamais inclus dans le snapshot


def test_24_migration_0012_cree_tables(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "m12.db"
    apply_migrations(db); apply_migrations(db)   # idempotence
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"proprietaires_releve_cycle", "proprietaires_paiement"} <= tables


def test_25_derive_source_devenue_indisponible(tmp_db):
    """Une source dont l'empreinte disparaît (devenue indisponible) est détectée comme dérive."""
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"empreintes_sources": {"aircover": "h1"}},
                          version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"empreintes_sources": {"aircover": None}})
    assert d["derive"] is True and "empreintes_sources" in d["champs_modifies"]


def test_26_derive_source_redevenue_disponible(tmp_db):
    """Une source redevenue disponible (empreinte réapparue) est également une dérive vs snapshot."""
    c = _c(tmp_db)
    c = cycle_svc.demarrer(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.marquer_a_valider(c, version_attendue=c["version"], db_path=tmp_db)
    c = cycle_svc.valider(c, {"empreintes_sources": {"aircover": None}},
                          version_attendue=c["version"], db_path=tmp_db)
    d = cycle_svc.detecter_derive(c, {"empreintes_sources": {"aircover": "h2"}})
    assert d["derive"] is True and "empreintes_sources" in d["champs_modifies"]

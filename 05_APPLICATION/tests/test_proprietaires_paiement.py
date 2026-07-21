"""APP-3E — Préparation des règlements propriétaires SANS virement.

MARQUE_COMME_PAYE est une déclaration humaine, jamais une preuve bancaire. Aucun IBAN, aucun RIB,
aucune coordonnée bancaire, aucun ordre de virement, aucun appel API bancaire.
"""
import pytest

from app.services import proprietaires_paiement_service as pay

OPAQUE = "REG-test0002"


def _p(db):
    return pay.creer_ou_charger(OPAQUE, db_path=db)


def test_01_creation_non_prepare(tmp_db):
    p = _p(tmp_db)
    assert p["statut_paiement"] == pay.ST_NON_PREPARE


def test_02_demarrer_controle(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    assert p["statut_paiement"] == pay.ST_A_CONTROLER


def test_03_passage_pret_a_payer_direct_refuse(tmp_db):
    p = _p(tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)


def test_04_passage_pret_a_payer_avec_bloquant_refuse(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.marquer_pret_a_payer(p, bloquants=["DONNEE_MOTEUR_INDISPONIBLE"],
                                 version_attendue=p["version"], db_path=tmp_db)


def test_05_passage_pret_a_payer_sans_bloquant(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    assert p["statut_paiement"] == pay.ST_PRET_A_PAYER


def test_06_marquer_paye(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_paye(p, reference_interne="virement du 05/01 vu sur relevé bancaire",
                         version_attendue=p["version"], db_path=tmp_db)
    assert p["statut_paiement"] == pay.ST_MARQUE_COMME_PAYE


def test_07_marquer_paye_sans_ecriture_bancaire(tmp_db):
    """Déclaration seulement — aucun import réseau, aucun appel API bancaire dans ce module."""
    import inspect
    src = inspect.getsource(pay)
    for interdit in ("import requests", "import httpx", "qonto", "credit_mutuel", "webhook", "sepa"):
        assert interdit not in src.lower()


def test_08_reference_interne_avec_iban_refusee(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.marquer_paye(p, reference_interne="IBAN FR76...", version_attendue=p["version"], db_path=tmp_db)


def test_09_reouverture(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_paye(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.rouvrir(p, "erreur de montant déclarée", version_attendue=p["version"], db_path=tmp_db)
    assert p["statut_paiement"] == pay.ST_ROUVERT


def test_10_annulation(tmp_db):
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.annuler(p, "règlement annulé", version_attendue=p["version"], db_path=tmp_db)
    assert p["statut_paiement"] == pay.ST_ANNULE


def test_11_double_clic_meme_transition_refuse(tmp_db):
    p = _p(tmp_db)
    pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)   # version périmée


def test_12_version_obsolete_refusee(tmp_db):
    p = _p(tmp_db)
    p2 = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)   # p obsolète, p2 est courant


def test_13_historique_append_only(tmp_db):
    from app.db.connection import get_db
    p = _p(tmp_db)
    pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    conn = get_db(tmp_db)
    rows = conn.execute(
        "SELECT * FROM proprietaires_releve_evenements WHERE releve_id_opaque=? AND type_evenement='PAIEMENT_TRANSITION'",
        (OPAQUE,)).fetchall()
    conn.close()
    assert len(rows) == 1


def test_14_controle_passage_donnee_moteur_absente_bloque():
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat="VALIDE", derive=False, statut_app5c_compatible=True,
        statut_moteur_compatible=True, montant_moteur_disponible=False, proprietaire_connu=True,
        source_obligatoire_disponible=True)
    assert "DONNEE_MOTEUR_INDISPONIBLE" in codes


def test_15_controle_releve_non_valide_bloque():
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat="EN_PREPARATION", derive=False, statut_app5c_compatible=True,
        statut_moteur_compatible=True, montant_moteur_disponible=True, proprietaire_connu=True,
        source_obligatoire_disponible=True)
    assert "RELEVE_NON_VALIDE" in codes


def test_16_controle_derive_bloque():
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat="VALIDE", derive=True, statut_app5c_compatible=True,
        statut_moteur_compatible=True, montant_moteur_disponible=True, proprietaire_connu=True,
        source_obligatoire_disponible=True)
    assert "SNAPSHOT_OBSOLETE" in codes


def test_17_controle_writer_reel_actif_bloque():
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat="VALIDE", derive=False, statut_app5c_compatible=True,
        statut_moteur_compatible=True, montant_moteur_disponible=True, proprietaire_connu=True,
        source_obligatoire_disponible=True, writer_reel_actif=True)
    assert "WRITER_REEL_ACTIVE" in codes


def test_18_controle_aucun_bloquant_si_tout_ok():
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat="VALIDE", derive=False, statut_app5c_compatible=True,
        statut_moteur_compatible=True, montant_moteur_disponible=True, proprietaire_connu=True,
        source_obligatoire_disponible=True)
    assert codes == []


def test_19_champs_bancaires_interdits_liste():
    assert "iban" in pay.CHAMPS_BANCAIRES_INTERDITS
    assert "rib" in pay.CHAMPS_BANCAIRES_INTERDITS


def test_20_lister_a_payer(tmp_db):
    _p(tmp_db)
    rows = pay.lister_a_payer(db_path=tmp_db)
    assert len(rows) == 1


def test_21_retour_en_controle_depuis_pret_a_payer(tmp_db):
    """PRET_A_PAYER -> A_CONTROLER : retour en contrôle autorisé et tracé."""
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)   # retour en contrôle
    assert p["statut_paiement"] == pay.ST_A_CONTROLER


def test_22_declaration_repetee_marquer_paye_refusee(tmp_db):
    """Une 2e déclaration « payé » sur un paiement déjà MARQUE_COMME_PAYE est refusée (transition rejouée)."""
    p = _p(tmp_db)
    p = pay.demarrer_controle(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_pret_a_payer(p, version_attendue=p["version"], db_path=tmp_db)
    p = pay.marquer_paye(p, version_attendue=p["version"], db_path=tmp_db)
    with pytest.raises(pay.PaiementRefuse):
        pay.marquer_paye(p, version_attendue=p["version"], db_path=tmp_db)

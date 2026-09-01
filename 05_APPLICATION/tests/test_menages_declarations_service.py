"""Saisie directe d'une déclaration de ménage interne (mission « Activation réelle Ménages »).

Vérifie que la règle de coût (`lib_menage_costs.resolve_internal_cleaning_cost`) est reprise
verbatim, jamais réinventée, et qu'une déclaration sans référentiel de coût applicable reste
`A_CONTROLER` plutôt que d'inventer un montant.
"""
from __future__ import annotations

from app.db.connection import get_db
from app.services import menages_declarations_service as svc


def _ref_minimal(db_path):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
            "type_logement_id, actif, import_id) "
            "VALUES ('LOG_0001', 'T2 Test', 'T2 Test', 'TYPE_T2', 'OUI', 'TEST')")
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, import_id) VALUES ('INT_0001', 'Marie Dupont', 'INTERNE', 'OUI', 'TEST')")
        conn.commit()
    finally:
        conn.close()


def _taux_horaire(db_path, *, date_debut="2026-01-01", date_fin=None):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_taux_heures_menage (taux_horaire_id, intervenant_id, taux_horaire, "
            "date_debut, date_fin, actif, import_id) "
            "VALUES ('TAUX_0001', 'INT_0001', 15.0, ?, ?, 'OUI', 'TEST')",
            (date_debut, date_fin))
        conn.commit()
    finally:
        conn.close()


def test_logements_actifs_liste_uniquement_les_actifs(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, actif, "
            "import_id) VALUES ('LOG_A', 'A', 'A', 'OUI', 'TEST')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, actif, "
            "import_id) VALUES ('LOG_B', 'B', 'B', 'NON', 'TEST')")
        conn.commit()
    finally:
        conn.close()
    logements = svc.logements_actifs(db_path=tmp_db)
    ids = [l["logement_id"] for l in logements]
    assert "LOG_A" in ids
    assert "LOG_B" not in ids


def test_creer_logement_inconnu_refuse(tmp_db):
    res = svc.creer(mois="2026-03", logement_id="LOG_INEXISTANT", intervenant_id="INT_0001",
                     nb_menages=2, nb_heures=4, db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == svc.E_LOGEMENT_INCONNU


def test_creer_mois_invalide_refuse(tmp_db):
    _ref_minimal(tmp_db)
    res = svc.creer(mois="2026", logement_id="LOG_0001", intervenant_id="INT_0001",
                     nb_menages=2, nb_heures=4, db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == svc.E_MOIS_INVALIDE


def test_creer_sans_referentiel_cout_reste_a_controler(tmp_db):
    """Aucun taux horaire ni coût fixe applicable : la déclaration est créée, mais A_CONTROLER —
    jamais un coût inventé."""
    _ref_minimal(tmp_db)
    res = svc.creer(mois="2026-03", logement_id="LOG_0001", intervenant_id="INT_0001",
                     nb_menages=3, nb_heures=6, db_path=tmp_db)
    assert res["ok"] is True
    assert res["statut_controle"] == "A_CONTROLER"
    assert res["cout_lavage_attribue"] is None

    conn = get_db(tmp_db)
    try:
        row = conn.execute(
            "SELECT mois, logement_id, intervenant_id, nb_menages, nb_heures, statut_controle, "
            "source_url FROM menages_declarations_internes WHERE id = ?", (res["id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row["mois"] == "2026-03"
    assert row["logement_id"] == "LOG_0001"
    assert row["nb_menages"] == 3
    assert row["source_url"] == svc.SOURCE_UI


def test_creer_avec_taux_horaire_calcule_le_cout(tmp_db):
    """Avant le pivot du 2026-06-01 : coût = heures × taux horaire applicable — repris tel quel de
    lib_menage_costs, jamais recalculé ici."""
    _ref_minimal(tmp_db)
    _taux_horaire(tmp_db)
    res = svc.creer(mois="2026-03", logement_id="LOG_0001", intervenant_id="INT_0001",
                     nb_menages=2, nb_heures=4, db_path=tmp_db)
    assert res["ok"] is True
    assert res["statut_controle"] == "OK"
    assert res["cout_lavage_attribue"] == 60.0  # 4h * 15.0

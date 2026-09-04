"""Lot9 — flux économique unifié, SQLite (`flux_unifie_service`)."""
from __future__ import annotations

from app.db.connection import apply_migrations, get_db
from app.services import flux_unifie_service as svc

from fixtures_hostaway import ligne_reservation, peupler_reservations


def _facture_menage(db_path, *, facture_id="FAC-MEN-001", fournisseur="FRS-TEST-001",
                    facture_ref="REF-001", date_facture="2026-07-15",
                    statut="VALIDEE", logement_id="LOG_0001", montant_ttc=90.0):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, statut, source) VALUES (?,?,?,?,?,?,'SAISIE')",
            (facture_id, fournisseur, facture_ref, date_facture, montant_ttc, statut))
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, montant_ttc, source) VALUES (?,?,?,?,?,'SAISIE')",
            (f"FLM-{facture_id}", facture_id, "MENAGE_EXTERNE", logement_id, montant_ttc))
        conn.execute(
            "INSERT INTO facture_lignes_menage_pdf (ligne_id_opaque, date_menage) VALUES (?,?)",
            (f"FLM-{facture_id}", date_facture))
        conn.commit()
    finally:
        conn.close()


def _mouvement_banque(db_path, *, mid="MVT-9-001", montant=25.0, date="2026-07-10T00:00:00Z"):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO banque_import_source (import_id, bank_account_id, source_type, "
            "source_filename, source_sha256, nb_lignes, nb_inseres, nb_doublons, nb_a_controler, "
            "date_import, run_id, statut) VALUES ('IMP-9','CM_TEST','HISTORIQUE','R.xlsx', ?, 1, "
            "1, 0, 0, '2026-07-01T00:00:00Z', 'RUN-9', 'IMPORTE')", ("0" * 64,))
        conn.execute(
            "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
            "external_transaction_id, date_operation, date_valeur, sens, montant, devise, "
            "libelle_brut, contrepartie_brute, fingerprint, ligne_source) "
            "VALUES (?,'IMP-9','CM_TEST','',?,?,'DEBIT',?,'EUR','FRAIS BANCAIRES','','FP-9',1)",
            (mid, date, date, montant))
        conn.execute(
            "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id, "
            "regle_id, categorie, tiers_detecte, type_flux_id, code_impact, source_economique, "
            "statut_controle, statut_classification, niveau_risque, rapprochement_requis) "
            "VALUES (?, 'CLS-9', 'R_9', 'FRAIS_BANCAIRES', '', 'TYPE_FLUX_016', 'IC', 'REGLE', "
            "'VALIDE', 'CLASSE', '', '')", (mid,))
        conn.commit()
    finally:
        conn.close()


def _menage_cout_complet(db_path, *, mois="2026-07", logement_id="LOG_0001",
                         intervenant_id="INT_0001", cout_standard_total=60.0,
                         ecart_vs_standard_total=-5.0):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_cout_complet (mois, logement_id, proprietaire_id, "
            "intervenant_id, cout_standard_total, ecart_vs_standard_total, statut_ecart) "
            "VALUES (?,?,?,?,?,?,?)",
            (mois, logement_id, "PROP_0001", intervenant_id, cout_standard_total,
             ecart_vs_standard_total, "PERTE"))
        conn.commit()
    finally:
        conn.close()


def _sans_charges(monkeypatch, rows=()):
    monkeypatch.setattr(svc.charges_reader, "read_charges", lambda *a, **k: list(rows))


def test_construire_sans_donnees_ne_plante_pas(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["ok"] is True
    assert resultat["nb_total"] == 0
    assert svc.lire(db_path=db) == []


def test_construire_fusionne_tous_les_modules(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)

    peupler_reservations(db, [ligne_reservation("RES-2026-07-HA-001", montant=200.0)])
    _facture_menage(db)
    _mouvement_banque(db)
    _menage_cout_complet(db)
    _sans_charges(monkeypatch, [{
        "charge_id": "CHG-001", "type_flux_id": "TYPE_FLUX_020", "sens": "CHARGE",
        "montant": 42.0, "code_impact": "IC", "statut_controle": "VALIDE",
        "mois": "2026-07", "date_charge": "2026-07-12", "logement_id": "LOG_0001",
        "proprietaire_id": "PROP_0001", "associe_id": None, "commentaire": "",
    }])

    resultat = svc.construire(db_path=db)

    assert resultat["ok"] is True
    assert (resultat["nb_res"], resultat["nb_men"], resultat["nb_bnq"], resultat["nb_chg"]) == \
        (1, 1, 1, 1)
    assert resultat["nb_gpm"] == 2  # standard (TYPE_FLUX_019) + écart (TYPE_FLUX_018)
    assert resultat["nb_total"] == 6
    assert resultat["nb_doublons"] == 0

    lignes = svc.lire(db_path=db)
    assert len(lignes) == 6
    par_type = {r["type_flux_id"] for r in lignes}
    assert par_type == {"TYPE_FLUX_017", "TYPE_FLUX_014", "TYPE_FLUX_016", "TYPE_FLUX_020",
                        "TYPE_FLUX_019", "TYPE_FLUX_018"}
    assert all(r["montant"] >= 0 for r in lignes)
    assert len({r["flux_id"] for r in lignes}) == 6


def test_flux_id_stable_entre_deux_constructions(tmp_path, monkeypatch):
    """Le flux_id d'une même ligne source ne change pas d'un run à l'autre — contrairement au
    compteur positionnel du script legacy."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    peupler_reservations(db, [ligne_reservation("RES-2026-07-HA-001", montant=200.0)])
    _sans_charges(monkeypatch)

    r1 = svc.construire(db_path=db)
    lignes1 = {r["source_pk"]: r["flux_id"] for r in svc.lire(db_path=db)}

    # Une deuxième ligne apparaît AVANT la première dans l'ordre de tri — un compteur positionnel
    # décalerait le flux_id de la première ligne.
    peupler_reservations(db, [
        ligne_reservation("RES-2026-07-HA-000", montant=150.0),
        ligne_reservation("RES-2026-07-HA-001", montant=200.0),
    ])
    r2 = svc.construire(db_path=db)
    lignes2 = {r["source_pk"]: r["flux_id"] for r in svc.lire(db_path=db)}

    assert r1["run_id"] != r2["run_id"]
    assert lignes1["RES-2026-07-HA-001"] == lignes2["RES-2026-07-HA-001"]


def test_construire_est_idempotent(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    peupler_reservations(db, [ligne_reservation("RES-2026-07-HA-001", montant=200.0)])
    _sans_charges(monkeypatch)

    svc.construire(db_path=db)
    svc.construire(db_path=db)

    assert len(svc.lire(db_path=db)) == 1


def test_men_exclut_facture_non_validee(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    _facture_menage(db, facture_id="FAC-BROUILLON", statut="BROUILLON")
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 0


def test_lire_filtre_par_mois(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    peupler_reservations(db, [ligne_reservation("RES-2026-07-HA-001", mois="2026-07")])
    _sans_charges(monkeypatch)
    svc.construire(db_path=db)

    assert len(svc.lire(mois="2026-07", db_path=db)) == 1
    assert svc.lire(mois="2026-08", db_path=db) == []


# ── MEX historique figé : mission « Lot9 MEX historique + protection Lot10 » ────────────────────
# Avant cette mission, `_module_men` lisait uniquement `facture_lignes_menage_service.
# lignes_externes_pour_reader()` — le backfill figé (migration 0069, mois clôturés) restait
# invisible du vrai Lot9. Corrigé via l'interface unique `menages_backfill_historique_service.
# menages_externes_economiques()`, déjà conçue pour ce rôle mais jamais câblée jusqu'ici.

def _ligne_historique(db_path, *, source_pk="MENEXT-2026-05-TEST-001", mois="2026-05",
                      logement_id="LOG_0001", proprietaire_id="PROP_0001",
                      montant_ligne_ttc=100.0, statut_source="VALIDE"):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_externes_historique (source_pk, mois, logement_id, "
            "proprietaire_id, prestataire_id, date_facture, montant_ligne_ttc, statut_source, "
            "source_fichier, source_hash) VALUES (?,?,?,?, 'INT_TEST', ?, ?, ?, 'TEST.xlsx', "
            "'0'*64)",
            (source_pk, mois, logement_id, proprietaire_id, f"{mois}-28", montant_ligne_ttc,
             statut_source))
        conn.commit()
    finally:
        conn.close()


def test_lot9_lit_le_backfill_mex_historique(tmp_path, monkeypatch):
    """Cause du bug réel : `_module_men` ignorait `menages_externes_historique`. Une ligne figée
    VALIDE doit désormais produire un flux TYPE_FLUX_014."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _ligne_historique(db, montant_ligne_ttc=2381.0)
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 1
    lignes = svc.lire(mois="2026-05", db_path=db)
    men = [l for l in lignes if l["type_flux_id"] == "TYPE_FLUX_014"]
    assert len(men) == 1
    assert men[0]["montant"] == 2381.0


def test_lot9_mai_2026_douze_lignes_2381_euros(tmp_path, monkeypatch):
    """Preuve chiffrée exacte de la mission (§A4) : 12 lignes historiques VALIDE, total 2381,00€."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    montants = [29.0, 145.0, 290.0, 440.0, 58.0, 312.0, 55.0, 55.0, 55.0, 390.0, 520.0, 32.0]
    for i, m in enumerate(montants):
        _ligne_historique(db, source_pk=f"MENEXT-2026-05-TEST-{i:03d}", montant_ligne_ttc=m)
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 12
    men = [l for l in svc.lire(mois="2026-05", db_path=db) if l["type_flux_id"] == "TYPE_FLUX_014"]
    assert len(men) == 12
    assert round(sum(l["montant"] for l in men), 2) == 2381.00


def test_lot9_ligne_historique_non_validee_zero_impact(tmp_path, monkeypatch):
    """Une ligne historique figée non VALIDE reste hors calcul économique (§A2/A4)."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _ligne_historique(db, source_pk="MENEXT-2026-05-NONVALIDE",
                      montant_ligne_ttc=36.0, statut_source="A_CONTROLER")
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 0


def test_lot9_facture_a_controler_zero_type_flux_014(tmp_path, monkeypatch):
    """Une facture courante A_CONTROLER (ex. Aissata/Mounir) n'entre jamais dans TYPE_FLUX_014,
    historique ou pas (§A5)."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _facture_menage(db, facture_id="FAC-ACONTROLER", statut="A_CONTROLER")
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 0


def test_lot9_aucun_doublon_entre_historique_et_courant(tmp_path, monkeypatch):
    """Un mois figé et un mois courant distincts ne se chevauchent jamais (§A3/§C5)."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _ligne_historique(db, mois="2026-05", montant_ligne_ttc=100.0)
    _facture_menage(db, facture_id="FAC-JUILLET", date_facture="2026-07-20", montant_ttc=90.0)
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    assert resultat["nb_men"] == 2
    assert resultat["nb_doublons"] == 0


def test_lot9_mois_cloture_avec_backfill_historique_prioritaire(tmp_path, monkeypatch):
    """Autorité par période (§A3, §C6) : une facture courante déposée sur un mois DÉJÀ couvert
    par l'historique figé ne doit jamais s'additionner ni l'écraser — l'historique reste seul
    autoritaire pour ce mois tant qu'aucune correction rétroactive explicite n'a lieu."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    _ligne_historique(db, mois="2026-05", montant_ligne_ttc=2381.0)
    _facture_menage(db, facture_id="FAC-MAI-TARDIVE", date_facture="2026-05-15", montant_ttc=999.0)
    _sans_charges(monkeypatch)

    resultat = svc.construire(db_path=db)

    men = [l for l in svc.lire(mois="2026-05", db_path=db) if l["type_flux_id"] == "TYPE_FLUX_014"]
    assert resultat["nb_men"] == 1
    assert len(men) == 1
    assert men[0]["montant"] == 2381.0
    assert resultat["nb_doublons"] == 0


def test_lot9_db_path_none_resout_vers_cfg_db_path(tmp_path, monkeypatch):
    """Régression du bug réel : `menages_externes_economiques(db_path=None)` se connectait
    littéralement au fichier "None" (`sqlite3.connect(str(None))`), jamais à la vraie base — un
    fichier vide sans table, donc TYPE_FLUX_014 retombait silencieusement à 0. C'est exactement
    l'appel que fait l'orchestrateur en production (`construire()` sans `db_path` explicite).
    `cfg.DB_PATH` est monkeypatché pour que ce test n'ouvre jamais la vraie base."""
    import app.config as cfg

    db = tmp_path / "app.db"
    apply_migrations(db)
    _ligne_historique(db, montant_ligne_ttc=2381.0)
    _sans_charges(monkeypatch)
    monkeypatch.setattr(cfg, "DB_PATH", db)

    resultat = svc.construire()

    assert resultat["nb_men"] == 1

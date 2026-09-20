"""L'écran Banque montre Qonto — et ne montre ni secret, ni plomberie, ni UUID.

Ce que ces tests tiennent :
  · les soldes, la dernière synchronisation et le bouton sont bien là ;
  · chaque mouvement est lisible : date, montant, sens, statut, type, libellé, contrepartie,
    référence utile ;
  · une opération `pending` est visiblement NON DÉFINITIVE et n'est jamais présentée comme
    comptabilisée ;
  · le statut applicatif « À rapprocher » existe et n'a AUCUN effet comptable ;
  · rien de ce qui doit rester caché ne sort : clé secrète, en-tête Authorization, charge utile
    brute, empreintes, identifiants techniques quand une information humaine existe.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import qonto_ecran_service as ecran
from app.services import qonto_raw_service as raw
from app.services import qonto_statut_local_service as statut_local
from app.services import qonto_sync_service as sync
from tests.test_qonto_raw_import import COMPTE, ClientDouble, mouvement

TABLES_COMPTABLES = ("charges", "factures", "facture_lignes_menage", "factures_proprietaires",
                     "reglements_fournisseurs")


@pytest.fixture()
def base(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db, raising=False)
    return db


def _importer(db, mouvements):
    sync.synchroniser(client=ClientDouble(pages=[mouvements]), db_path=db)
    statut_local.synchroniser(db_path=db)


REGLE = dict(status="completed", side="credit", amount=200.0, amount_cents=20000,
             operation_type="income", label="Virement reçu", clean_counterparty_name="LOCATAIRE X",
             settled_at="2026-09-11T08:00:00.000Z")
EN_ATTENTE = dict(status="pending", side="debit", amount=20.0, amount_cents=2000,
                  operation_type="card", label="Paiement carte", clean_counterparty_name="FOURNISSEUR Y",
                  settled_at=None)


# ── Le tableau de bord ────────────────────────────────────────────────────────────────────────
def test_le_tableau_de_bord_porte_soldes_et_derniere_synchronisation(base):
    _importer(base, [mouvement(1, **REGLE)])
    vue = ecran.tableau_de_bord(db_path=base)

    assert vue["disponible"] is True
    assert vue["soldes"]["banque"] == 180.0, "le solde mis en avant est le disponible"
    assert vue["soldes"]["banque_comptable"] == 200.0
    assert vue["soldes"]["engage"] == 20.0, "ce que les opérations en attente immobilisent"
    assert vue["soldes"]["devise"] == "EUR"
    assert vue["derniere_synchronisation"]["horodatage"] != "jamais"
    assert vue["derniere_synchronisation"]["statut"] == raw.ST_SUCCES


def test_chaque_transaction_est_lisible(base):
    _importer(base, [mouvement(1, **REGLE)])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]

    assert ligne["date"] == "11/09/2026"
    assert ligne["montant"] == 200.0
    assert ligne["sens"] == "Crédit"
    assert ligne["statut_qonto"] == "Comptabilisé"
    assert ligne["type"] == "Encaissement"
    assert ligne["libelle"] == "Virement reçu"
    assert ligne["contrepartie"] == "LOCATAIRE X"
    assert ligne["traitement_libelle"] in ("À contrôler", "À rapprocher")


def test_les_codes_techniques_sont_traduits_en_francais(base):
    _importer(base, [mouvement(1, **REGLE), mouvement(2, **EN_ATTENTE)])
    lignes = ecran.tableau_de_bord(db_path=base)["transactions"]
    rendus = {ligne["statut_qonto"] for ligne in lignes} | {ligne["type"] for ligne in lignes}
    assert "completed" not in rendus and "pending" not in rendus
    assert {"Comptabilisé", "En attente", "Encaissement", "Carte"} <= rendus


def test_une_reference_qui_repete_le_libelle_n_est_pas_affichee(base):
    """Afficher deux fois la même chose n'informe personne."""
    _importer(base, [mouvement(1, label="Virement Dupont", reference="Virement Dupont")])
    assert ecran.tableau_de_bord(db_path=base)["transactions"][0]["reference"] == ""


def test_une_note_humaine_prend_le_pas_sur_la_reference(base):
    _importer(base, [mouvement(1, label="VIR SEPA", reference="VIR SEPA",
                               note="Loyer septembre — appartement Gare")])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]
    assert ligne["reference"] == "Loyer septembre — appartement Gare"


# ── L'opération en attente ────────────────────────────────────────────────────────────────────
def test_une_operation_en_attente_est_signalee_non_definitive(base):
    _importer(base, [mouvement(2, **EN_ATTENTE)])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]

    assert ligne["statut_qonto"] == "En attente"
    assert ligne["definitif"] is False
    assert "non définitif" in ligne["motif_non_definitif"]
    assert ligne["date_est_prevue"] is True, \
        "sans date de règlement, la date affichée est celle d'émission — et c'est dit"


def test_une_operation_en_attente_n_est_pas_comptabilisable(base):
    _importer(base, [mouvement(1, **REGLE), mouvement(2, **EN_ATTENTE)])
    conn = get_db(base)
    try:
        etats = dict(conn.execute(
            "SELECT t.statut, s.comptabilisable FROM qonto_transactions_raw t "
            "JOIN qonto_transactions_statut_local s "
            "  ON s.qonto_transaction_uuid = t.qonto_transaction_uuid"))
    finally:
        conn.close()
    assert etats["completed"] == 1
    assert etats["pending"] == 0, "une autorisation de carte n'est pas un mouvement acquis"
    assert ecran.tableau_de_bord(db_path=base)["en_attente"] == 1


def test_une_operation_en_attente_qui_se_regle_devient_definitive(base):
    _importer(base, [mouvement(2, **EN_ATTENTE)])
    assert ecran.tableau_de_bord(db_path=base)["en_attente"] == 1

    reglee = dict(EN_ATTENTE)
    reglee.update(status="completed", settled_at="2026-09-13T09:00:00.000Z")
    _importer(base, [mouvement(2, **reglee)])

    vue = ecran.tableau_de_bord(db_path=base)
    assert vue["en_attente"] == 0
    assert vue["nb_total"] == 1, "toujours pas de doublon"
    assert vue["transactions"][0]["definitif"] is True


# ── Le statut applicatif ──────────────────────────────────────────────────────────────────────
def test_a_rapprocher_est_pose_sur_chaque_mouvement_importe(base):
    _importer(base, [mouvement(1, **REGLE), mouvement(2, **EN_ATTENTE)])
    assert statut_local.resume(db_path=base)["a_rapprocher"] == 2
    assert all(ligne["traitement"] in ("A_CONTROLER", "A_RAPPROCHER", "EN_ATTENTE_QONTO")
               for ligne in ecran.tableau_de_bord(db_path=base)["transactions"])


def test_a_rapprocher_n_a_aucun_effet_comptable(base):
    conn = get_db(base)
    try:
        presentes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        avant = {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t}")]
                 for t in TABLES_COMPTABLES if t in presentes}
    finally:
        conn.close()
    assert avant, "aucune table comptable trouvée : le test ne prouverait rien"

    _importer(base, [mouvement(1, **REGLE), mouvement(2, **EN_ATTENTE)])

    conn = get_db(base)
    try:
        apres = {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t}")] for t in avant}
    finally:
        conn.close()
    assert apres == avant, "poser « À rapprocher » a modifié une table comptable"


def test_le_statut_applicatif_est_idempotent(base):
    _importer(base, [mouvement(1, **REGLE)])
    conn = get_db(base)
    try:
        avant = [tuple(r) for r in conn.execute("SELECT * FROM qonto_transactions_statut_local")]
    finally:
        conn.close()

    assert statut_local.synchroniser(db_path=base) == {"poses": 0, "rafraichis": 0}

    conn = get_db(base)
    try:
        apres = [tuple(r) for r in conn.execute("SELECT * FROM qonto_transactions_statut_local")]
    finally:
        conn.close()
    assert apres == avant


def test_le_statut_vit_hors_de_la_couche_raw(base):
    """Une réimportation qui réécrit la ligne RAW ne doit pas emporter notre statut."""
    _importer(base, [mouvement(1, **REGLE)])
    conn = get_db(base)
    try:
        conn.execute("UPDATE qonto_transactions_statut_local SET pose_le='2020-01-01T00:00:00Z'")
        conn.commit()
        colonnes = [r[1] for r in conn.execute("PRAGMA table_info(qonto_transactions_raw)")]
    finally:
        conn.close()
    assert "statut_local" not in colonnes, "le statut applicatif n'a rien à faire dans la table RAW"

    modifie = dict(REGLE)
    modifie["label"] = "Virement reçu (corrigé)"
    _importer(base, [mouvement(1, **modifie)])

    conn = get_db(base)
    try:
        pose = conn.execute(
            "SELECT pose_le FROM qonto_transactions_statut_local").fetchone()[0]
    finally:
        conn.close()
    assert pose == "2020-01-01T00:00:00Z", "la mise à jour RAW a écrasé le statut applicatif"


# ── Ce qui ne doit jamais s'afficher ──────────────────────────────────────────────────────────
def test_la_vue_ne_contient_aucune_plomberie(base):
    _importer(base, [mouvement(1, **REGLE)])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]
    interdits = ("charge_utile_json", "empreinte", "qonto_transaction_uuid", "transaction_id",
                 "dernier_sync_run_id", "qonto_account_id")
    for champ in interdits:
        assert champ not in ligne, f"« {champ} » ne doit pas atteindre l'écran"


def test_l_iban_complet_ne_sort_jamais_de_la_vue(base):
    _importer(base, [mouvement(1, **REGLE)])
    vue = ecran.tableau_de_bord(db_path=base)
    assert COMPTE["iban"] not in str(vue)
    assert vue["comptes"][0]["iban_masque"] == "FR76 **** **** 0185"


# ── L'écran ───────────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def client(base):
    from app.main import app
    return TestClient(app)


def test_l_ecran_banque_affiche_le_bloc_qonto(client, base):
    _importer(base, [mouvement(1, **REGLE), mouvement(2, **EN_ATTENTE)])
    page = client.get("/banques-caisse")
    assert page.status_code == 200
    html = page.text

    assert 'data-testid="kpi-tresorerie"' in html
    assert 'data-testid="qonto-actualiser"' in html and "Actualiser Qonto" in html
    assert "180.00 EUR" in html, "le KPI principal est le solde disponible"
    assert "Solde banque" in html and "Trésorerie disponible" in html
    assert "À rapprocher" in html
    # Les deux mouvements attendus. La contrepartie prime sur le libellé : elle dit QUI, ce
    # qui renseigne davantage que l'intitulé technique de l'opération.
    assert "LOCATAIRE X" in html and "FOURNISSEUR Y" in html
    assert "Comptabilisé" in html and "En attente" in html


def test_l_operation_en_attente_est_visuellement_distincte(client, base):
    _importer(base, [mouvement(2, **EN_ATTENTE)])
    html = client.get("/banques-caisse").text
    assert 'data-testid="qonto-non-definitif"' in html
    assert "ligne-non-definitive" in html
    assert "non définitif" in html


def test_l_ecran_montre_le_compte_avec_un_iban_masque(client, base):
    _importer(base, [mouvement(1, **REGLE)])
    html = client.get("/banques-caisse").text
    assert 'data-testid="qonto-comptes"' in html
    assert "Compte principal" in html
    assert "FR76 **** **** 0185" in html
    assert COMPTE["iban"] not in html, "l'IBAN complet ne doit jamais être rendu"


def test_l_ecran_ne_rend_aucun_identifiant_technique(client, base):
    _importer(base, [mouvement(1, **REGLE)])
    html = client.get("/banques-caisse").text
    assert "tx-uuid-0001" not in html, "un UUID ne dit rien à personne : le libellé suffit"
    assert "chouette-patrimoine-0001-transaction" not in html
    assert "charge_utile" not in html and "empreinte" not in html
    assert "Authorization" not in html
    assert COMPTE["iban"] not in html


def test_le_bouton_actualiser_ne_fait_qu_une_synchronisation(client, base, monkeypatch):
    """Le bouton appelle le service GET-only, puis renvoie sur l'écran. Rien de plus."""
    appels = {"n": 0}

    def faux_sync(**kwargs):
        appels["n"] += 1
        return {"ok": True, "creees": 0, "mises_a_jour": 0, "inchangees": 0}

    from app.routes import banques as routes_banques
    monkeypatch.setattr(routes_banques.qonto_ecran, "actualiser", faux_sync)

    reponse = client.post("/banques-caisse/qonto/actualiser", follow_redirects=False)
    assert reponse.status_code == 303
    assert "message=" in reponse.headers["location"]
    assert reponse.headers["location"].startswith("/banques-caisse?")
    assert appels["n"] == 1


def test_un_echec_d_actualisation_ne_casse_pas_l_ecran(client, base, monkeypatch):
    _importer(base, [mouvement(1, **REGLE)])
    from app.routes import banques as routes_banques
    monkeypatch.setattr(routes_banques.qonto_ecran, "actualiser",
                        lambda **k: {"ok": False, "code": "QONTO_API_ECHOUEE",
                                     "message": "L'API Qonto n'a pas répondu correctement."})

    reponse = client.post("/banques-caisse/qonto/actualiser", follow_redirects=True)
    assert reponse.status_code == 200
    assert "n&#39;a pas répondu correctement" in reponse.text or "n'a pas répondu" in reponse.text
    # Le mouvement déjà importé reste affiché : un échec ne détruit rien.
    assert "LOCATAIRE X" in reponse.text

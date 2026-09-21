"""La période libre produit une facture propriétaire COMPLÈTE, et n'invente aucune proratisation.

CE QUE CE MODULE VERROUILLE. L'écran de liste proposait « + Facturer une prestation ponctuelle » :
une désignation libre, un montant libre, une facture d'une seule ligne saisie à la main. Ce n'était
pas le besoin — il fallait une facture NORMALE sur des dates choisies. Le moteur de période libre
existait déjà, mais ne reprenait que la commission et le ménage : ni la préparation du canapé, ni
le forfait n'entraient dans la facture, alors qu'ils composent `montant_du_conciergerie`.

LA RÈGLE DES ÉLÉMENTS MENSUELS, ET POURQUOI ELLE N'EST PAS UNE INVENTION. `build_charge_fixe`
(Lot10) pose le forfait UNE FOIS PAR MOIS CIVIL de gestion, au montant plein de
`REF_Logements.forfait_logiciel_consommables_mensuel`. Il n'existe nulle part de règle de
proratisation. Un mois partiel ne peut donc porter ni un forfait entier (ce serait facturer un mois
qu'on ne facture pas), ni une fraction (ce serait un montant que rien d'autre ne sait reproduire) :
il est écarté, et RENDU VISIBLE. Ces tests échoueront si quelqu'un ajoute un prorata.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_proprietaires_conformite_service as conformite
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_periode_service as periode
from app.services import factures_proprietaires_service as svc

PROP = "PROP_0001"
LOG = "LOG_0001"
AUTRE_LOG = "LOG_0002"
RUN = "L10-TEST"
FORFAIT = 35.0


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


def _reservation(conn, *, reservation, arrivee, depart, commission, menage, canape=0.0,
                 logement=LOG):
    conn.execute(
        "INSERT INTO lot10_commissions (run_id, reservation_calc_id, logement_id, "
        "proprietaire_id, mois, date_arrivee, date_depart, nuits, payout_calcule, menage_retenu, "
        "assiette_commission, taux_commission, commission_conciergerie, net_proprietaire, "
        "preparation_canape_voyageurs) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (RUN, reservation, logement, PROP, arrivee[:7], arrivee, depart, 2.0,
         commission + menage, menage, commission / 0.2, 0.2, commission, 0.0, canape))


def _mois(conn, mois, *, logement=LOG, forfait=FORFAIT, refac=0.0, commission=0.0, menage=0.0,
          canape=0.0):
    """Une ligne de `lot10_net_reglement` : le grain MENSUEL du moteur."""
    conn.execute(
        "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
        "charge_fixe_mensuelle, charges_exceptionnelles_refacturees, total_commission_mois, "
        "total_menage_mois, total_preparation_canape_mois, montant_du_conciergerie) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (RUN, mois, logement, PROP, forfait, refac, commission, menage, canape,
         commission + menage + canape + forfait + refac))


@pytest.fixture()
def base(tmp_path):
    """Septembre 2026 : quatre séjours, un forfait de 35 €, un canapé sur deux séjours."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, actif) VALUES (?, 1)", (RUN,))
        _reservation(conn, reservation="RES-1", arrivee="2026-09-02", depart="2026-09-05",
                     commission=40.0, menage=50.0, canape=10.0)
        _reservation(conn, reservation="RES-2", arrivee="2026-09-08", depart="2026-09-10",
                     commission=30.0, menage=50.0, canape=10.0)
        _reservation(conn, reservation="RES-3", arrivee="2026-09-15", depart="2026-09-18",
                     commission=25.0, menage=50.0)
        _reservation(conn, reservation="RES-4", arrivee="2026-09-22", depart="2026-09-25",
                     commission=35.0, menage=50.0)
        _mois(conn, "2026-09", commission=130.0, menage=200.0, canape=20.0)
        conn.commit()
    finally:
        conn.close()
    return db


# ── La facture contient les MÊMES éléments qu'une facture mensuelle ─────────────────────────────

def test_un_mois_entier_reconstitue_exactement_le_montant_du_moteur(base):
    """La propriété qui compte : douze périodes d'un mois doivent refaire l'année du moteur."""
    conn = get_db(base)
    try:
        attendu = conn.execute(
            "SELECT montant_du_conciergerie FROM lot10_net_reglement WHERE mois='2026-09'"
        ).fetchone()[0]
    finally:
        conn.close()
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    assert apercu["propositions"][0]["montant_total"] == attendu


def test_les_cinq_composants_facturables_sont_repris(base):
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    montants = {l["type_ligne"]: l["montant"] for l in apercu["propositions"][0]["lignes"]}
    assert montants == {"COMMISSION_CONCIERGERIE": 130.0, "MENAGE_FACTURE": 200.0,
                        "PREPARATION_CANAPE": 20.0, "CHARGE_FIXE": FORFAIT}


def test_les_lignes_sont_dans_l_ordre_de_la_facture(base):
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    lignes = apercu["propositions"][0]["lignes"]
    assert [l["type_ligne"] for l in lignes] == [
        "COMMISSION_CONCIERGERIE", "MENAGE_FACTURE", "PREPARATION_CANAPE", "CHARGE_FIXE"]
    assert [l["numero_ligne"] for l in lignes] == [1, 2, 3, 4]


def test_le_canape_suit_la_date_d_arrivee_comme_la_commission(base):
    """Le canapé se calcule PAR SÉJOUR : il se découpe donc, contrairement au forfait."""
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-05", db_path=base)
    montants = {l["type_ligne"]: l["montant"] for l in apercu["propositions"][0]["lignes"]}
    assert montants["PREPARATION_CANAPE"] == 10.0, "seul le séjour du 02 est dans la période"


# ── Le forfait : entier, ou pas du tout ─────────────────────────────────────────────────────────

def test_mois_partiel_le_forfait_n_est_pas_facture(base):
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-13", db_path=base)
    types = {l["type_ligne"] for l in apercu["propositions"][0]["lignes"]}
    assert "CHARGE_FIXE" not in types


def test_mois_partiel_le_forfait_ecarte_est_rendu_visible(base):
    """Écarté n'est pas perdu : sans ce signalement, 35 € disparaîtraient en silence."""
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-13", db_path=base)
    ecartes = apercu["mensuels_ecartes"]
    assert len(ecartes) == 1
    assert ecartes[0]["type_ligne"] == "CHARGE_FIXE"
    assert ecartes[0]["montant"] == FORFAIT
    assert ecartes[0]["mois"] == "2026-09"
    assert "proratis" in ecartes[0]["motif"]


def test_aucun_prorata_n_est_calcule(base):
    """Treize jours sur trente ne doivent JAMAIS produire 35 × 13/30 = 15,17 €."""
    apercu = periode.previsualiser(PROP, "2026-09-01", "2026-09-13", db_path=base)
    montants = [l["montant"] for l in apercu["propositions"][0]["lignes"]]
    interdits = {round(FORFAIT * 13 / 30, 2), round(FORFAIT * 13 / 31, 2)}
    assert not interdits & set(montants)
    for m in apercu["mensuels_ecartes"]:
        assert m["montant"] == FORFAIT, "le montant signalé est le forfait PLEIN, pas une fraction"


def test_deux_periodes_qui_couvrent_le_mois_ne_facturent_le_forfait_qu_une_fois(base):
    """Le double comptage serait pire que l'oubli : il se répare par un avoir."""
    premiere = periode.previsualiser(PROP, "2026-09-01", "2026-09-15", db_path=base)
    seconde = periode.previsualiser(PROP, "2026-09-16", "2026-09-30", db_path=base)
    forfaits = [l["montant"] for a in (premiere, seconde)
                for p in a["propositions"] for l in p["lignes"] if l["type_ligne"] == "CHARGE_FIXE"]
    assert forfaits == [], "aucune des deux moitiés ne couvre le mois civil entier"


def test_une_periode_a_cheval_ne_retient_que_les_mois_entierement_couverts(base):
    conn = get_db(base)
    try:
        _mois(conn, "2026-10", commission=0.0, menage=0.0)
        conn.commit()
    finally:
        conn.close()
    apercu = periode.previsualiser(PROP, "2026-09-15", "2026-10-31", db_path=base)
    assert apercu["mois_traverses"] == ["2026-09", "2026-10"]
    retenus = [m["mois"] for p in apercu["propositions"] for m in p["mensuels_retenus"]]
    ecartes = [m["mois"] for m in apercu["mensuels_ecartes"]]
    assert retenus == ["2026-10"], "octobre est couvert en entier, pas septembre"
    assert ecartes == ["2026-09"]


def test_charges_exceptionnelles_refacturees_suivent_la_meme_regle(base):
    conn = get_db(base)
    try:
        conn.execute("UPDATE lot10_net_reglement SET charges_exceptionnelles_refacturees = 120.0 "
                     "WHERE mois = '2026-09'")
        conn.commit()
    finally:
        conn.close()
    entier = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    partiel = periode.previsualiser(PROP, "2026-09-01", "2026-09-13", db_path=base)
    assert {l["type_ligne"]: l["montant"] for l in entier["propositions"][0]["lignes"]
            }["CHARGES_EXCEPT_REFAC"] == 120.0
    assert {m["type_ligne"] for m in partiel["mensuels_ecartes"]} == {"CHARGE_FIXE",
                                                                     "CHARGES_EXCEPT_REFAC"}


# ── Le filtre par logement ──────────────────────────────────────────────────────────────────────

def test_le_logement_choisi_restreint_la_facture(base):
    conn = get_db(base)
    try:
        _reservation(conn, reservation="RES-5", arrivee="2026-09-04", depart="2026-09-07",
                     commission=99.0, menage=60.0, logement=AUTRE_LOG)
        _mois(conn, "2026-09", logement=AUTRE_LOG, commission=99.0, menage=60.0)
        conn.commit()
    finally:
        conn.close()
    tous = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", db_path=base)
    assert len(tous["propositions"]) == 2

    cible = periode.previsualiser(PROP, "2026-09-01", "2026-09-30", logement_id=LOG, db_path=base)
    assert [p["logement_id"] for p in cible["propositions"]] == [LOG]
    assert cible["mensuels_ecartes"] == []

    res = periode.creer(PROP, "2026-09-01", "2026-09-30", logement_id=LOG, acteur="test",
                        db_path=base)
    assert res["nb"] == 1, "UNE seule facture, pour le logement demandé"
    assert svc.lire(res["creees"][0]["facture_id_opaque"], db_path=base)["logement_id"] == LOG


# ── La mention réglementaire de période ─────────────────────────────────────────────────────────

def test_la_periode_imprimee_est_celle_de_la_facture_pas_le_mois(base):
    res = periode.creer(PROP, "2026-09-01", "2026-09-13", acteur="test", db_path=base)
    facture = svc.lire(res["creees"][0]["facture_id_opaque"], db_path=base)
    bloc = conformite.construire(facture, date_facture="2026-09-20", db_path=base)
    assert (bloc["periode_debut"], bloc["periode_fin"]) == ("2026-09-01", "2026-09-13")


def test_le_pdf_affiche_la_periode_au_format_francais(base):
    res = periode.creer(PROP, "2026-09-01", "2026-09-13", acteur="test", db_path=base)
    facture = svc.lire(res["creees"][0]["facture_id_opaque"], db_path=base)
    bloc = conformite.construire(facture, date_facture="2026-09-20", db_path=base)
    references = pdf._Facture({"mois": "2026-09", "conformite": bloc})._references()
    assert "Période des prestations : du 01/09/2026 au 13/09/2026" in references


def test_une_facture_mensuelle_garde_les_bornes_de_son_mois(base):
    """Le cycle mensuel ne porte pas de période : le repli sur le mois doit rester intact."""
    creee = svc.creer_exceptionnelle(
        proprietaire_id=PROP, logement_id=LOG, mois="2026-09",
        lignes=[{"libelle": "Commission", "montant": 100.0}], acteur="test", db_path=base)
    facture = svc.lire(creee["facture_id_opaque"], db_path=base)
    assert not facture["periode_debut"]
    bloc = conformite.construire(facture, date_facture="2026-09-20", db_path=base)
    assert (bloc["periode_debut"], bloc["periode_fin"]) == ("2026-09-01", "2026-09-30")


# ── L'écran de liste ────────────────────────────────────────────────────────────────────────────

def test_la_liste_propose_une_periode_libre_et_plus_une_saisie_manuelle(client, tmp_db):
    page = client.get("/factures-proprietaires").text
    assert 'data-testid="bloc-periode-libre"' in page
    assert "Facturer une période libre" in page
    for champ in ('name="proprietaire_id"', 'name="logement_id"', 'name="debut"', 'name="fin"'):
        assert champ in page
    assert "Créer le brouillon" in page
    # Ce qui a été RETIRÉ : une facture propriétaire ne se saisit pas à la main.
    assert "Désignation" not in page
    assert 'name="libelle"' not in page
    assert 'name="montant"' not in page
    assert "/factures-proprietaires/exceptionnelle" not in page


def test_la_route_de_saisie_manuelle_n_existe_plus(client, tmp_db):
    """405 et non 404 : le chemin reste capté par `GET /factures-proprietaires/{facture_id}`,
    qui le lit comme un identifiant. Ce qui compte est qu'aucun POST ne l'accepte plus, et donc
    qu'aucune facture ne puisse naître d'un libellé et d'un montant saisis à la main."""
    reponse = client.post("/factures-proprietaires/exceptionnelle",
                          data={"proprietaire_id": PROP, "logement_id": LOG, "mois": "2026-09",
                                "libelle": "Intervention", "montant": "150,00"},
                          follow_redirects=False)
    assert reponse.status_code in (404, 405)
    assert svc.lister(db_path=tmp_db) == []


def test_la_liste_cree_un_brouillon_de_periode_libre(client, tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, actif) VALUES (?, 1)", (RUN,))
        _reservation(conn, reservation="RES-1", arrivee="2026-09-02", depart="2026-09-05",
                     commission=40.0, menage=50.0, canape=10.0)
        _mois(conn, "2026-09", commission=40.0, menage=50.0, canape=10.0)
        conn.commit()
    finally:
        conn.close()
    reponse = client.post("/factures-proprietaires/periode-libre",
                          data={"proprietaire_id": PROP, "logement_id": LOG,
                                "debut": "2026-09-01", "fin": "2026-09-30"},
                          follow_redirects=False)
    assert reponse.status_code == 303
    factures = svc.lister(db_path=tmp_db)
    assert len(factures) == 1
    assert reponse.headers["location"].endswith(factures[0]["facture_id_opaque"])
    assert factures[0]["montant_total"] == 135.0, "40 + 50 + 10 + 35 de forfait"


def test_une_periode_sans_element_renvoie_le_formulaire_avec_son_message(client, tmp_db):
    reponse = client.post("/factures-proprietaires/periode-libre",
                          data={"proprietaire_id": PROP, "logement_id": LOG,
                                "debut": "2026-09-30", "fin": "2026-09-01"},
                          follow_redirects=False)
    assert reponse.status_code == 303
    cible = reponse.headers["location"]
    assert cible.startswith("/factures-proprietaires/nouvelle")
    assert "erreur=" in cible and "debut=2026-09-30" in cible, "la saisie n'est pas perdue"
    assert svc.lister(db_path=tmp_db) == []

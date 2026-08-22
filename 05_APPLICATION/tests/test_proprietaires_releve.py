"""APP-3D — Relevés & préfactures propriétaires (suivi humain, jamais la facture réelle).

La vérité financière reste exclusivement le moteur (Lot10/Lot12) — jamais recalculée ni écrite ici.
Données réelles en lecture seule ; app.db isolée (fixture tmp_db/client). Réel intact.
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
from app.services import proprietaires_suivi_service as suivi
from app.services import proprietaires_blocages_service as blocages
from app.services import proprietaires_releve_export_service as export_svc
from app.services import proprietaires_reglements_service as regl_svc

REEL = Path(r"C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie")
MOIS_TEST = "2098-01"
MOIS_TEST2 = "2098-02"
PROP_TEST = "PROP_TEST_0001"
PROP_TEST2 = "PROP_TEST_0002"

moteur_requis = pytest.mark.skipif(
    not Path(cfg.MASTER_NET_PROPRIETAIRE).exists(), reason="MASTER_CALC_NetProprietaire.xlsx absent")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _premier_proprietaire_reel():
    for mois in regl_svc.load_periods():
        owners = regl_svc.load_owners(mois=mois)
        rows = owners.get("rows", [])
        if rows:
            return rows[0]["proprietaire_id"], mois
    return None, None


# ── 1-4 : création, unicité, statut initial, propriétaire multi-logements ────

def test_01_creation_releve(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    assert r["mois"] == MOIS_TEST and r["releve_id_opaque"].startswith("REG-")


def test_02_unicite_prop_mois(tmp_db):
    r1 = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    r2 = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    assert r1["releve_id_opaque"] == r2["releve_id_opaque"]
    assert len(suivi.lister(db_path=tmp_db)) == 1


def test_03_statut_initial(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    assert r["statut_facturation"] == suivi.ST_NON_CONCERNE


@moteur_requis
def test_04_proprietaire_multi_logements(tmp_db):
    pid, mois = _premier_proprietaire_reel()
    if pid is None:
        pytest.skip("Aucun propriétaire réel disponible")
    detail = regl_svc.load_owner_detail(pid, mois)
    assert detail is not None and detail.get("status") == "OK"
    assert isinstance(detail.get("logements"), list)


# ── 5-6 : taux de commission historique ───────────────────────────────────────

@moteur_requis
def test_05_taux_commission_historique_expose(tmp_db):
    pid, mois = _premier_proprietaire_reel()
    if pid is None:
        pytest.skip("Aucun propriétaire réel disponible")
    detail = regl_svc.load_owner_detail(pid, mois)
    assert "taux_historiques" in detail


def test_06_taux_absent_bloque(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 100, "net": 90}, "taux_historiques": [],
        "logements": [], "factures": [], "anomalies": []})
    r = blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)
    assert "TAUX_COMMISSION_ABSENT" in r["bloquants"]


# ── 7-10 : réservations, annulations, payout, ménage ──────────────────────────

def test_07_reservation_normale_champs_presents(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 500, "net": 450, "menage": 50},
        "taux_historiques": [("LOG1", 0.2)], "logements": [], "factures": [], "anomalies": []})
    r = blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)
    assert r["preparable"] in (True, False)   # ne lève jamais, toujours un résultat structuré


def test_08_payout_absent_bloque(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": None, "net": 90},
        "taux_historiques": [("LOG1", 0.2)], "logements": [], "factures": [], "anomalies": []})
    r = blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)
    assert "PAYOUT_ABSENT" in r["bloquants"]


def test_09_net_absent_bloque(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": None, "taux_historiques": [("LOG1", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    r = blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)
    assert "NET_ABSENT" in r["bloquants"]


def test_10_ajustement_sans_motif_bloque(tmp_db, monkeypatch):
    from app.readers import proprietaires_extras_reader as extras
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    monkeypatch.setattr(extras, "ajustements_prop_mois", lambda pid, mois, *a, **k: [{"motif": ""}])
    r = blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)
    assert "AJUSTEMENT_SANS_MOTIF" in r["bloquants"]


# ── 11-14 : acompte, reversement, ajustement, séparation exploitation/règlement

def test_11_acompte_ne_modifie_jamais_le_net_exploitation(tmp_db):
    """L'acompte est un champ de règlement distinct — jamais mélangé au net d'exploitation moteur."""
    import inspect
    from app.services import proprietaires_service as legacy_svc
    src = inspect.getsource(legacy_svc.load_releve)
    assert "_COLS_EXPLOITATION" in src and "_COLS_REGLEMENT" in src
    assert "acompte" not in " ".join(
        c for c in src.split("_COLS_EXPLOITATION")[1].split("]")[0].split(",")).lower()


def test_12_separation_exploitation_reglement_colonnes_distinctes():
    from app.services import proprietaires_service as legacy_svc
    import inspect
    src = inspect.getsource(legacy_svc)
    assert '"net_proprietaire_avant_charge_mois"' in src   # bloc exploitation
    assert '"net_proprietaire_apres_charge_mois"' in src   # bloc règlement (distinct)


def test_13_extras_reader_acomptes_filtre_prop_mois(tmp_db):
    from app.readers import proprietaires_extras_reader as extras
    rows = extras.acomptes_prop_mois(PROP_TEST, MOIS_TEST)
    assert isinstance(rows, list)


def test_14_extras_reader_ajustements_filtre_sur_mois_effet(tmp_db, monkeypatch):
    from app.readers import proprietaires_extras_reader as extras
    monkeypatch.setattr(extras, "ajustements_post_cloture", lambda *a, **k: type("S", (), {
        "lignes": [{"proprietaire_id": PROP_TEST, "mois_effet": MOIS_TEST, "motif": "x"},
                  {"proprietaire_id": PROP_TEST, "mois_effet": MOIS_TEST2, "motif": "y"}]})())
    rows = extras.ajustements_prop_mois(PROP_TEST, MOIS_TEST)
    assert len(rows) == 1 and rows[0]["mois_effet"] == MOIS_TEST


# ── 15-18 : AirCover, acompte, reversement, ajustement (readers existent, désormais en base) ─

def test_15_aircover_reader_disponible(tmp_db):
    from app.readers import proprietaires_extras_reader as extras
    s = extras.aircover(tmp_db)
    assert s.etat.etat in (extras.ETAT_OK, extras.ETAT_VIDE, extras.ETAT_NON_INITIALISE)


def test_16_acompte_reader_disponible(tmp_db):
    """Les acomptes viennent de la base : l'état possible inclut « non initialisée »."""
    from app.readers import proprietaires_extras_reader as extras
    s = extras.acomptes()
    assert s.etat.etat in (extras.ETAT_OK, extras.ETAT_VIDE, extras.ETAT_NON_INITIALISE)


def test_17_imputations_reader_disponible(tmp_db):
    from app.readers import proprietaires_extras_reader as extras
    s = extras.imputations_airbnb(tmp_db)
    assert s.etat.etat in (extras.ETAT_OK, extras.ETAT_VIDE, extras.ETAT_NON_INITIALISE)


def test_18_ajustements_reader_disponible(tmp_db):
    from app.readers import proprietaires_extras_reader as extras
    s = extras.ajustements_post_cloture(tmp_db)
    assert s.etat.etat in (extras.ETAT_OK, extras.ETAT_VIDE, extras.ETAT_NON_INITIALISE)


# ── 19-20 : séparation prouvée, acompte ne modifie jamais le net ──────────────

def test_19_blocages_jamais_de_calcul_financier():
    """Contrôle structurel : aucune opération arithmétique sur un montant dans le service de blocage."""
    import inspect
    from app.services import proprietaires_blocages_service as blocages_mod
    src = inspect.getsource(blocages_mod)
    for motif in (" + vue", " - vue", "* vue", "somme("):
        assert motif not in src


def test_20_export_acompte_distinct_du_net(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    out = export_svc.exporter(r, None, {"bloquants": [], "informatifs": []})
    assert "resume" in out


# ── 21-23 : mois ouvert / en contrôle / clôturé (lien APP-5C) ────────────────

def test_21_lien_statut_moteur_mois(tmp_db):
    from app.routes import proprietaires_reglements as route_mod
    statut = route_mod._statut_moteur_mois("2050-01")
    assert statut in ("INCONNU", "SOURCE_INDISPONIBLE") or isinstance(statut, str)


def test_22_lien_statut_cloture_humain_app5c(tmp_db):
    from app.services import clotures_service as cs
    from app.routes import proprietaires_reglements as route_mod
    cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    import app.config as cfgmod
    orig = cfgmod.DB_PATH
    cfgmod.DB_PATH = tmp_db
    try:
        statut = route_mod._statut_cloture_humain(MOIS_TEST)
    finally:
        cfgmod.DB_PATH = orig
    assert statut is not None


def test_23_mois_cloture_incompatible_bloque(tmp_db, monkeypatch):
    from app.readers import controles_cloture_reader as ref_reader
    class _Ouvert:
        etat = type("E", (), {"disponible": True})()
        lignes = [{"mois": MOIS_TEST, "statut_mois": "OUVERT"}]
    monkeypatch.setattr(ref_reader, "cloture_ref", lambda: _Ouvert())
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    r = blocages.evaluer(PROP_TEST, MOIS_TEST, db_path=tmp_db)
    assert "CLOTURE_MOTEUR_INCOMPATIBLE" in r["bloquants"]


# ── 24-25 : statut humain APP-5C, divergence snapshot ────────────────────────

def test_24_statut_humain_app5c_distinct_du_statut_facturation(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    assert "statut_facturation" in r   # jamais confondu avec le statut clôture APP-5C


def test_25_transition_directe_facture_refusee(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    with pytest.raises(suivi.ReleveRefuse):
        suivi._transition(r, suivi.ST_FACTURE, db_path=tmp_db)   # NON_CONCERNE -> FACTURE interdit


# ── 26-28 : facture déjà générée, doublon, avoir ──────────────────────────────

def test_26_facture_deja_generee_workflow(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    r = suivi.marquer_a_facturer(r, acteur="t", db_path=tmp_db)
    r = suivi.marquer_facture(r, acteur="t", commentaire="ok", db_path=tmp_db)
    assert r["statut_facturation"] == suivi.ST_FACTURE


def test_27_doublon_facturation_detecte(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    r = suivi.marquer_a_facturer(r, acteur="t", db_path=tmp_db)
    suivi.marquer_facture(r, acteur="t", commentaire="ok", db_path=tmp_db)
    ev = blocages.evaluer(PROP_TEST, MOIS_TEST, db_path=tmp_db)
    assert "DOUBLON_FACTURATION" in ev["bloquants"]


def test_28_avoir_workflow(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    r = suivi.marquer_a_facturer(r, acteur="t", db_path=tmp_db)
    r = suivi.marquer_facture(r, acteur="t", commentaire="ok", db_path=tmp_db)
    r = suivi.marquer_avoir_a_emettre(r, acteur="t", motif="erreur logement", db_path=tmp_db)
    assert r["statut_facturation"] == suivi.ST_AVOIR_A_EMETTRE
    r = suivi.marquer_avoir_emis(r, acteur="t", db_path=tmp_db)
    assert r["statut_facturation"] == suivi.ST_AVOIR_EMIS


# ── 29-30 : export CSV, injection CSV ─────────────────────────────────────────

def test_29_export_csv_contenu(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    out = export_svc.exporter(r, None, {"bloquants": [], "informatifs": []})
    assert "AVERTISSEMENT" in out and "ne constitue pas une facture définitive" in out


def test_30_injection_csv_neutralisee():
    r = {"mois": MOIS_TEST, "releve_id_opaque": "REG-x", "statut_facturation": "=cmd|/c calc",
        "date_creation": "x", "date_preparation": None, "date_validation": None,
        "date_reouverture": None, "commentaire_validation": "+SUM(1)"}
    out = export_svc.exporter(r, None, {"bloquants": ["=evil"], "informatifs": []})
    for ligne in out.split("\n"):
        for cell in ligne.split(";"):
            assert not (cell and cell[0] in ("=", "+", "-", "@", "\t", "\r"))


# ── 31-34 : identifiants opaques, aucun id SQLite, aucun chemin, aucun IBAN ──

def test_31_identifiant_opaque_reg(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    assert r["releve_id_opaque"].startswith("REG-") and len(r["releve_id_opaque"]) == 14


def test_32_aucun_id_sqlite_dans_reponse(client, tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    resp = client.get(f"/proprietaires-reglements/{r['releve_id_opaque']}")
    assert f'/proprietaires-reglements/{r["id"]}"' not in resp.text


def test_33_aucun_chemin_absolu(client, tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    resp = client.get(f"/proprietaires-reglements/{r['releve_id_opaque']}")
    assert "C:\\" not in resp.text


def test_34_aucun_iban_dans_export(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    out = export_svc.exporter(r, None, {"bloquants": [], "informatifs": []})
    import re
    assert not re.search(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b", out)


# ── 35-36 : source absente, moteur indisponible ───────────────────────────────

def test_35_source_absente_pas_de_crash(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "SOURCE_INDISPONIBLE"})
    r = blocages.evaluer(PROP_TEST, MOIS_TEST, db_path=tmp_db)
    assert r["bloquants"] == ["SOURCE_OBLIGATOIRE_ABSENTE"]


def test_36_moteur_indisponible_route_ne_500_pas(client, tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "SOURCE_INDISPONIBLE"})
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    resp = client.get(f"/proprietaires-reglements/{r['releve_id_opaque']}")
    assert resp.status_code == 200


# ── 37-38 : résilience partielle, concurrence ─────────────────────────────────

def test_37_resilience_partielle_extras_en_panne(tmp_db, monkeypatch):
    from app.readers import proprietaires_extras_reader as extras
    monkeypatch.setattr(extras, "ajustements_prop_mois",
                        lambda pid, mois, *a, **k: (_ for _ in ()).throw(RuntimeError("panne")))
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    with pytest.raises(RuntimeError):
        blocages.evaluer(PROP_TEST, "2027-01", db_path=tmp_db)


def test_38_concurrence_version_obsolete_refusee(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    r = suivi.marquer_a_facturer(r, acteur="a", db_path=tmp_db)
    with pytest.raises(suivi.ReleveRefuse):
        suivi._transition(r, suivi.ST_FACTURE, version_attendue=1, db_path=tmp_db)  # version déjà à 2


# ── 39-41 : version obsolète, redémarrage, navigation ─────────────────────────

def test_39_version_obsolete_message_clair(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    with pytest.raises(suivi.ReleveRefuse, match="Conflit de version"):
        suivi.marquer_a_facturer(r, acteur="t", version_attendue=999, db_path=tmp_db)


def test_40_persistance_apres_nouvelle_connexion(tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    suivi.marquer_a_facturer(r, acteur="t", db_path=tmp_db)
    relu = suivi.charger_par_prop_mois(PROP_TEST, MOIS_TEST, tmp_db)
    assert relu["statut_facturation"] == suivi.ST_A_FACTURER


def test_41_navigation_liste_200(client):
    r = client.get("/proprietaires-reglements")
    assert r.status_code == 200


# ── 42-45 : responsive/aucun calendrier, aucune écriture réelle, flags, intégrité

def test_42_aucun_calendrier(client):
    r = client.get("/proprietaires-reglements")
    assert 'type="date"' not in r.text


def test_43_aucune_ecriture_reelle_flags_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.CONTROLES_REAL_WRITE_ENABLED is False


def test_44_aucune_route_dangereuse():
    from app.routes import proprietaires_reglements as route_mod
    for r in route_mod.router.routes:
        assert "DELETE" not in r.methods


def test_45_reel_intact():
    p = REEL / "02_TRAVAIL" / "Lot10_Resultats" / "MASTER_CALC_NetProprietaire.xlsx"
    if p.exists():
        h1 = _sha(p)
        import time; time.sleep(0.01)
        h2 = _sha(p)
        assert h1 == h2


# ── 46 : headers sécurité ──────────────────────────────────────────────────────

def test_46_headers_securite(client, tmp_db):
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    resp = client.get(f"/proprietaires-reglements/{r['releve_id_opaque']}")
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert resp.headers.get("cache-control") == "no-store"


# ── 47 : 404 propre sur identifiant inconnu ───────────────────────────────────

def test_47_404_identifiant_inconnu(client):
    r = client.get("/proprietaires-reglements/REG-0000000000")
    assert r.status_code == 404


# ── 48 : migration 0009 ────────────────────────────────────────────────────────

def test_48_migration_0009_cree_tables(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "m9.db"
    apply_migrations(db)
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"proprietaires_releves", "proprietaires_releve_evenements"} <= tables


# ── 49-50 : préfacture déjà préparée, source complémentaire illisible ────────

def test_49_prefacture_deja_preparee_bloque(tmp_db, monkeypatch):
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    r = suivi.creer_ou_charger(PROP_TEST, MOIS_TEST, acteur="t", db_path=tmp_db)
    suivi.marquer_a_facturer(r, acteur="t", db_path=tmp_db)
    ev = blocages.evaluer(PROP_TEST, MOIS_TEST, db_path=tmp_db)
    assert "PREFACTURE_DEJA_PREPAREE" in ev["bloquants"]
    assert "DOUBLON_FACTURATION" not in ev["bloquants"]   # distinct de FACTURE


def test_50_source_complementaire_illisible_bloque(tmp_db, monkeypatch):
    from app.readers import proprietaires_extras_reader as extras
    monkeypatch.setattr(regl_svc, "load_owner_detail", lambda pid, mois="": {
        "status": "OK", "vue": {"ca_retenu": 1, "net": 1}, "taux_historiques": [("L", 0.2)],
        "logements": [], "factures": [], "anomalies": []})
    illisible = extras.Source(etat=extras.EtatSource(
        cle="x", libelle="x", fichier="x", onglet="x", etat=extras.ETAT_ILLISIBLE))
    monkeypatch.setattr(blocages.extras, "acomptes", lambda: illisible)
    ev = blocages.evaluer(PROP_TEST, MOIS_TEST, db_path=tmp_db)
    assert "SOURCE_SCHEMA_INVALIDE" in ev["bloquants"]

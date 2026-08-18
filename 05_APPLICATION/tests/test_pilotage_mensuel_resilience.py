"""APP-5D — audit de résilience par source et vérification des liens de drill-down.

Chaque source (contrôles, clôtures, banques, réservations, ménages, charges, règlements) est mise en
panne isolément : la page doit rester 200, seul le bloc concerné doit passer à None/indisponible, les
autres blocs doivent rester intacts. Complète `test_pilotage_mensuel.py`.
"""
import pytest

from app.db.connection import get_db
from app.services import clotures_service as cs
from app.services import pilotage_mensuel_service as svc


def _mois_avec_donnee(tmp_db):
    return cs.creer_ou_charger("2097-08", acteur="t", db_path=tmp_db)["mois"]


@pytest.fixture(autouse=True)
def _seed_menages_minimal(tmp_db):
    """Une ligne dans chaque table SQLite lue par `menages_reader` (0038) : sans elle, `etat_global`
    de `menages_service.load_summary` vaut SOURCE_INCOMPLETE et `nb_menages_a_controler` reste None
    même quand ce bloc ne doit PAS être en panne — ce fichier teste l'isolement des AUTRES sources,
    pas celle-ci. `test_panne_menages_isolee`/`test_pannes_multiples_simultanees` stubbent
    `load_summary` directement : ce seed ne les affecte pas."""
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-SEED-001", "2097-08", "LOG_SEED", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle) VALUES (?,?,?,?,?)",
            ("2097-08", "LOG_SEED", "INT_SEED", 1, "VALIDE"))
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, statut_controle) VALUES (?,?,?,?,?)",
            ("2097-08", "LOG_SEED", "INT_SEED", 1, "VALIDE"))
        conn.execute(
            "INSERT INTO menages_gainperte (mois, logement_id, intervenant_id, statut_controle) "
            "VALUES (?,?,?,?)", ("2097-08", "LOG_SEED", "INT_SEED", "VALIDE"))
        conn.execute(
            "INSERT INTO menages_cout_complet (mois, logement_id, intervenant_id, statut_controle) "
            "VALUES (?,?,?,?)", ("2097-08", "LOG_SEED", "INT_SEED", "VALIDE"))
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, statut) VALUES (?,?,?,?,?,?)",
            ("FAC-SEED-0001", "INT_SEED", "SEED-0001", "2097-08-01", 30.0, "A_CONTROLER"))
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, montant_ttc) VALUES (?,?,?,?,?)",
            ("FLM-SEED-0001", "FAC-SEED-0001", "MENAGE_EXTERNE", "LOG_SEED", 30.0))
        conn.execute(
            "INSERT INTO controles_lot11_constats (ctrl_pk, source_module, code_controle, "
            "severity, statut_resolution) VALUES (?,?,?,?,?)",
            ("SEED||X", "MENAGES_EXT", "SEED_CONTROLE", "INFO", "OUVERT"))
        conn.commit()
    finally:
        conn.close()


# ── Panne isolée de chaque source (7 sources) ─────────────────────────────────

def test_panne_controles_isolee(tmp_db, monkeypatch):
    from app.services import clotures_service as cs_mod
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(cs_mod, "calcul_progression",
                        lambda m, db_path=None: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "controles" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_bloquants"] is None and l["nb_menages_a_controler"] is not None


def test_panne_moteur_ref_isolee(tmp_db, monkeypatch):
    from app.readers import controles_cloture_reader as ref_reader
    mois = _mois_avec_donnee(tmp_db)
    class _Indispo:
        etat = type("E", (), {"disponible": False})()
        lignes = []
    monkeypatch.setattr(ref_reader, "cloture_ref", lambda: _Indispo())
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "moteur" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["statut_moteur"] is None
    assert l["nb_bloquants"] is not None   # bloc contrôles intact


def test_panne_humain_isolee(tmp_db, monkeypatch):
    from app.services import clotures_service as cs_mod
    monkeypatch.setattr(cs_mod, "lister",
                        lambda db_path=None: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "humain" in t["indisponibles"]
    assert isinstance(t["lignes"], list)   # page rendue quand même


def test_panne_banque_isolee(tmp_db, monkeypatch):
    from app.services import banques_controle_service as banque_ctrl
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(banque_ctrl, "load_liste",
                        lambda **k: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "banque" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_mouvements_a_controler"] is None
    assert l["nb_menages_a_controler"] is not None


def test_panne_menages_isolee(tmp_db, monkeypatch):
    from app.services import menages_service
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(menages_service, "load_summary",
                        lambda mois="": (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "menages" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_menages_a_controler"] is None
    assert l["nb_reglements_a_controler"] is not None


def test_panne_charges_isolee(tmp_db, monkeypatch):
    from app.services import charges_service
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(charges_service, "load_list",
                        lambda **k: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "charges" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_charges_mois"] is None


def test_panne_reglements_isolee(tmp_db, monkeypatch):
    from app.services import proprietaires_reglements_service as regl_svc
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(regl_svc, "load_dashboard",
                        lambda mois="", **k: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "reglements" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_reglements_a_controler"] is None


def test_panne_reservations_isolee(tmp_db, monkeypatch):
    from app.services import reservations_hh_service as resa_svc
    mois = _mois_avec_donnee(tmp_db)
    monkeypatch.setattr(resa_svc, "load_list",
                        lambda **k: (_ for _ in ()).throw(RuntimeError("panne")))
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "reservations" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_reservations_mois"] is None


# ── Pannes multiples simultanées ──────────────────────────────────────────────

def test_pannes_multiples_simultanees(tmp_db, monkeypatch):
    from app.services import menages_service, charges_service
    from app.services import banques_controle_service as banque_ctrl
    monkeypatch.setattr(menages_service, "load_summary",
                        lambda mois="": (_ for _ in ()).throw(RuntimeError("p1")))
    monkeypatch.setattr(charges_service, "load_list",
                        lambda **k: (_ for _ in ()).throw(RuntimeError("p2")))
    monkeypatch.setattr(banque_ctrl, "load_liste",
                        lambda **k: (_ for _ in ()).throw(RuntimeError("p3")))
    mois = _mois_avec_donnee(tmp_db)
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert {"menages", "charges", "banque"} <= set(t["indisponibles"])
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_menages_a_controler"] is None
    assert l["nb_charges_mois"] is None
    assert l["nb_mouvements_a_controler"] is None
    assert l["nb_reglements_a_controler"] is not None   # source non impactée reste intacte


# ── Valeurs limites : None, liste vide, schéma incomplet, date invalide ──────

def test_valeur_none_ne_crashe_pas(tmp_db, monkeypatch):
    from app.services import menages_service
    monkeypatch.setattr(menages_service, "load_summary", lambda mois="": None)
    mois = _mois_avec_donnee(tmp_db)
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "menages" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_menages_a_controler"] is None


def test_schema_incomplet_dict_sans_cle_attendue(tmp_db, monkeypatch):
    from app.services import proprietaires_reglements_service as regl_svc
    monkeypatch.setattr(regl_svc, "load_dashboard", lambda mois="", **k: {"summary": {}})
    mois = _mois_avec_donnee(tmp_db)
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "reglements" in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_reglements_a_controler"] is None


def test_liste_vide_source_ok_sans_donnee(tmp_db, monkeypatch):
    from app.services import charges_service
    monkeypatch.setattr(charges_service, "load_list",
                        lambda **k: {"status": "OK", "count_affiches": 0, "rows": []})
    mois = _mois_avec_donnee(tmp_db)
    t = svc.tableau_mensuel(db_path=tmp_db)
    assert "charges" not in t["indisponibles"]
    l = next(x for x in t["lignes"] if x["mois"] == mois)
    assert l["nb_charges_mois"] == 0


def test_mois_date_invalide_dans_source_ignore_sans_crash(tmp_db):
    t = svc.tableau_mensuel(mois_filtre="not-a-month", db_path=tmp_db)
    assert t["lignes"] == []


# ── Drill-down : chaque lien existe, répond 200, applique le bon mois ────────

def test_drilldown_controles_cloture(client, tmp_db):
    cs.creer_ou_charger("2097-09", acteur="t", db_path=tmp_db)
    r = client.get("/controles-cloture?mois=2097-09&cloture_bloquee=true")
    assert r.status_code == 200
    assert 'value="2097-09"' in r.text or "2097-09" in r.text


def test_drilldown_banque_controle(client):
    r = client.get("/banques-caisse/controle?mois=2097-09&statut=A_CONTROLER")
    assert r.status_code == 200


def test_drilldown_menages_a_controler(client):
    r = client.get("/menages/a-controler?mois=2097-09")
    assert r.status_code == 200


def test_drilldown_reglements_a_controler(client):
    r = client.get("/proprietaires-reglements/a-controler?mois=2097-09")
    assert r.status_code == 200


def test_drilldown_fournisseurs(client):
    r = client.get("/fournisseurs?mois=2097-09")
    assert r.status_code == 200


def test_drilldown_reservations(client):
    r = client.get("/reservations?mois=2097-09")
    assert r.status_code == 200


def test_drilldown_fiche_cloture(client, tmp_db):
    c = cs.creer_ou_charger("2097-10", acteur="t", db_path=tmp_db)
    r = client.get(f"/clotures/{c['cloture_id_opaque']}")
    assert r.status_code == 200


def test_drilldown_liens_generes_dans_page_pointent_vers_routes_reelles(client, tmp_db, monkeypatch):
    """Chaque lien de compteur n'apparaît que si sa source a effectivement répondu (comportement
    voulu : jamais de lien vers une donnée indisponible) — sources stubbées ici pour vérifier le
    format exact des liens générés indépendamment de la disponibilité réelle des sources locales."""
    from app.services import banques_controle_service as banque_ctrl
    monkeypatch.setattr(banque_ctrl, "load_liste",
                        lambda **k: {"status": "OK", "count": 0, "rows": []})
    cs.creer_ou_charger("2097-11", acteur="t", db_path=tmp_db)
    r = client.get("/pilotage-mensuel?mois=2097-11")
    assert "/controles-cloture?mois=2097-11" in r.text
    assert "/banques-caisse/controle?mois=2097-11" in r.text
    assert "/menages/a-controler?mois=2097-11" in r.text
    assert "/proprietaires-reglements/a-controler?mois=2097-11" in r.text
    assert "/fournisseurs?mois=2097-11" in r.text
    assert "/reservations?mois=2097-11" in r.text
    assert "/controles-cloture?mois=2097-11&statut_suivi=ACCEPTE_AVEC_JUSTIFICATION" in r.text

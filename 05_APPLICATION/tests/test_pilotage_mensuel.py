"""APP-5D — Tableau de bord de clôture (`/pilotage-mensuel`). 17+ points.

Agrégation LECTURE SEULE composée depuis les services existants (APP-2/3/4/5B/5C) — aucune règle
métier recalculée ici, aucune écriture. Résilience : la panne d'une source dégrade uniquement son
bloc, jamais toute la page. app.db isolée (fixture tmp_db/client).
"""
import inspect

import app.config as cfg
from app.services import clotures_service as cs
from app.services import pilotage_mensuel_export_service as export_svc
from app.services import pilotage_mensuel_service as svc


# ── 1-2 : agrégation correcte / cohérente avec APP-5C ─────────────────────────

def test_01_agregation_reflete_progression_app5c(tmp_db):
    c = cs.creer_ou_charger("2098-01", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    ligne = next(l for l in tableau["lignes"] if l["mois"] == "2098-01")
    prog = cs.calcul_progression("2098-01", tmp_db)
    assert ligne["nb_bloquants"] == prog["nb_bloqueurs"]
    assert ligne["statut_humain"] == c["statut"]


def test_02_indicateur_coherent_avec_statut_humain(tmp_db):
    c = cs.creer_ou_charger("2098-02", acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    ligne = next(l for l in tableau["lignes"] if l["mois"] == "2098-02")
    assert ligne["indicateur"] == svc.INDIC_VALIDE


# ── 3 : lien vers la fiche source APP-5C ──────────────────────────────────────

def test_03_lien_vers_fiche_cloture(tmp_db):
    c = cs.creer_ou_charger("2098-03", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    ligne = next(l for l in tableau["lignes"] if l["mois"] == "2098-03")
    assert ligne["cloture_id_opaque"] == c["cloture_id_opaque"]


# ── 4 : filtres (année/mois/statut) ───────────────────────────────────────────

def test_04_filtre_mois_exact(tmp_db):
    cs.creer_ou_charger("2098-04", acteur="t", db_path=tmp_db)
    cs.creer_ou_charger("2098-05", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(mois_filtre="2098-04", db_path=tmp_db)
    assert all(l["mois"] == "2098-04" for l in tableau["lignes"])


def test_05_filtre_annee(tmp_db):
    cs.creer_ou_charger("2097-06", acteur="t", db_path=tmp_db)
    cs.creer_ou_charger("2098-06", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(annee="2097", db_path=tmp_db)
    assert all(l["mois"].startswith("2097") for l in tableau["lignes"])


def test_06_filtre_statut_indicateur(tmp_db):
    c = cs.creer_ou_charger("2098-07", acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(statut_filtre=svc.INDIC_EN_COURS, db_path=tmp_db)
    assert all(l["indicateur"] == svc.INDIC_EN_COURS for l in tableau["lignes"])


# ── 7 : multi-années présentes dans le filtre ─────────────────────────────────

def test_07_multi_annees_disponibles(tmp_db):
    cs.creer_ou_charger("2097-01", acteur="t", db_path=tmp_db)
    cs.creer_ou_charger("2098-01", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    assert {"2097", "2098"} <= set(tableau["annees_disponibles"])


# ── 8 : mois sans donnée ne crashe pas ────────────────────────────────────────

def test_08_mois_sans_donnee_rendu_normal(tmp_db):
    tableau = svc.tableau_mensuel(mois_filtre="2050-01", db_path=tmp_db)
    assert tableau["lignes"] == []   # mois inconnu de toute source -> liste vide, pas d'exception


# ── 9-10 : panne de source -> dégradation partielle, jamais un crash total ───

def test_09_panne_menages_degrade_uniquement_ce_bloc(tmp_db, monkeypatch):
    from app.services import menages_service
    def _panne(mois=""):
        raise RuntimeError("panne simulee")
    monkeypatch.setattr(menages_service, "load_summary", _panne)
    cs.creer_ou_charger("2098-08", acteur="t", db_path=tmp_db)
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    assert "menages" in tableau["indisponibles"]
    ligne = next(l for l in tableau["lignes"] if l["mois"] == "2098-08")
    assert ligne["nb_menages_a_controler"] is None
    assert ligne["nb_bloquants"] is not None   # autres blocs intacts


def test_10_panne_controles_ne_fait_pas_planter_la_page(client, monkeypatch):
    from app.services import clotures_service
    def _panne(mois, db_path=None):
        raise RuntimeError("panne moteur simulee")
    monkeypatch.setattr(clotures_service, "calcul_progression", _panne)
    r = client.get("/pilotage-mensuel")
    assert r.status_code == 200
    assert "indisponible" in r.text.lower()


# ── 11 : aucune écriture réelle ───────────────────────────────────────────────

def test_11_aucune_ecriture_flags_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.CONTROLES_REAL_WRITE_ENABLED is False


def test_12_aucune_route_post_sur_pilotage_mensuel():
    from app.routes import pilotage_mensuel as route_mod
    methodes = set()
    for r in route_mod.router.routes:
        methodes |= set(r.methods)
    assert "POST" not in methodes and "PUT" not in methodes and "DELETE" not in methodes


# ── 13 : aucune logique métier dupliquée (composition uniquement) ───────────

def test_13_service_compose_sans_reimplementer(tmp_db):
    """Structurel : le service n'importe aucun `readers` métier de calcul autre que la lecture du
    statut moteur déjà utilisée par l'export APP-5C — tous les compteurs viennent d'appels à des
    fonctions de service existantes (`cs.`, `banque_ctrl.`, `menages_svc.`, `regl_svc.`,
    `charges_svc.`, `resa_svc.`), jamais d'un recalcul local."""
    src = inspect.getsource(svc)
    for fonction_metier in ("_somme(", "sum(1 for", "* 100", "commission ="):
        assert fonction_metier not in src


# ── 14 : identifiants opaques uniquement ──────────────────────────────────────

def test_14_aucun_id_sqlite_brut_dans_reponse(client, tmp_db):
    cs.creer_ou_charger("2098-09", acteur="t", db_path=tmp_db)
    r = client.get("/pilotage-mensuel")
    assert "cloture_id=1" not in r.text and "/clotures/1\"" not in r.text


# ── 15 : aucun chemin absolu / aucune donnée bancaire brute ──────────────────

def test_15_aucun_chemin_absolu_ni_id_bancaire_brut(client, tmp_db):
    cs.creer_ou_charger("2098-10", acteur="t", db_path=tmp_db)
    r = client.get("/pilotage-mensuel")
    assert "C:\\" not in r.text
    import re
    assert not re.search(r"CM_\d+_\d+", r.text)


# ── 16 : export sécurisé (injection CSV neutralisée) ──────────────────────────

def test_16_export_neutralise_injection_formule(tmp_db):
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    tableau["lignes"].append({
        "mois": "2098-11", "statut_moteur": "=cmd|/c calc", "statut_humain_libelle": "+SUM(1)",
        "indicateur_libelle": "-1+1", "nb_bloquants": "@evil", "nb_exceptions": None,
        "nb_mouvements_a_controler": None, "nb_menages_a_controler": None,
        "nb_reglements_a_controler": None, "nb_charges_mois": None, "nb_reservations_mois": None,
        "derniere_action_humaine": None,
    })
    out = export_svc.exporter(tableau)
    for ligne in out.split("\n"):
        for cell in ligne.split(";"):
            assert not (cell and cell[0] in ("=", "+", "-", "@", "\t", "\r"))


def test_17_export_nom_fichier_sans_injection_entete():
    nom = export_svc.nom_fichier("2098-11\r\nX-Injected: 1")
    assert "\r" not in nom and "\n" not in nom


# ── 18 : en-têtes de sécurité + cache désactivé sur toutes les routes ────────

def test_18_headers_securite_toutes_routes(client):
    for route in ("/pilotage-mensuel", "/pilotage-mensuel/export.csv"):
        r = client.get(route)
        assert r.headers.get("x-content-type-options") == "nosniff"
        assert r.headers.get("x-frame-options") == "DENY"
        assert r.headers.get("cache-control") == "no-store"


# ── 19 : navigation sans 404, lien sidebar présent ────────────────────────────

def test_19_route_enregistree_200(client):
    assert client.get("/pilotage-mensuel").status_code == 200


def test_20_lien_sidebar_present(client):
    r = client.get("/")
    assert 'href="/pilotage-mensuel"' in r.text


def test_21_aucun_calendrier(client):
    r = client.get("/pilotage-mensuel")
    assert 'type="date"' not in r.text and "datepicker" not in r.text.lower() and "flatpickr" not in r.text.lower()


# ── 22 : disclaimer présent dans l'export ─────────────────────────────────────

def test_22_disclaimer_export(tmp_db):
    tableau = svc.tableau_mensuel(db_path=tmp_db)
    out = export_svc.exporter(tableau)
    assert "ne constitue pas la clôture comptable réelle" in out

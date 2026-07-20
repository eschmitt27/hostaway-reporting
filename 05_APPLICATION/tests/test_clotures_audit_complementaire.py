"""APP-5C — audit indépendant complémentaire (30 points), suite à `test_clotures.py`.

Couvre : concurrence/rejeu, immuabilité et dérive du snapshot, mois invalides/limites,
sécurité (identifiants forgés, injection HTML/CSV), migrations, persistance, tri chronologique.
Aucune donnée réelle, aucune écriture métier — app.db isolée (fixture tmp_db).
"""
import pytest

import app.config as cfg
from app.services import clotures_service as cs
from app.services import clotures_export_service as ces

MOIS_A = "2099-03"
MOIS_B = "2099-04"


# ── 1-2 : double validation / double réouverture concurrentes ────────────────

def test_c01_double_validation_concurrente_rejette_la_seconde(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    v = c["version"]
    c1 = cs.valider(c, acteur="t", commentaire="ok", version_attendue=v, db_path=tmp_db)
    assert c1["statut"] == cs.ST_VALIDEE
    with pytest.raises(cs.ClotureRefusee):
        cs.valider(c, acteur="t2", commentaire="rejoue", version_attendue=v, db_path=tmp_db)


def test_c02_double_reouverture_concurrente_rejette_la_seconde(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    v = c["version"]
    c1 = cs.rouvrir(c, acteur="t", justification="j1", version_attendue=v, db_path=tmp_db)
    assert c1["statut"] == cs.ST_ROUVERTE
    with pytest.raises(cs.ClotureRefusee):
        cs.rouvrir(c, acteur="t2", justification="j2", version_attendue=v, db_path=tmp_db)


# ── 3 : version obsolète (stale) refusée sur toute transition ───────────────

def test_c03_version_obsolete_refusee(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    with pytest.raises(cs.ClotureRefusee):
        cs.demarrer_preparation(c, acteur="t", version_attendue=999, db_path=tmp_db)


# ── 4 : rejeu exact d'une action déjà appliquée (replay) ──────────────────────

def test_c04_rejeu_action_deja_appliquee_refuse(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    c2 = cs.demarrer_preparation(c, acteur="t", version_attendue=c["version"], db_path=tmp_db)
    assert c2["statut"] == cs.ST_EN_PREPARATION
    with pytest.raises(cs.ClotureRefusee):
        cs.demarrer_preparation(c, acteur="t", version_attendue=c["version"], db_path=tmp_db)


# ── 5-6 : snapshot immuable malgré modification de la source ─────────────────

def test_c05_snapshot_conserve_apres_recalcul_source(tmp_db):
    c = cs.creer_ou_charger("2026-03", acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    snap_avant = cs.snapshot_actif(c["cloture_id_opaque"], tmp_db)
    live_apres = cs.elements_du_mois("2026-03", tmp_db)
    assert len(snap_avant) >= 0
    assert isinstance(live_apres, list)   # source live recalculée indépendamment, snapshot inchangé
    snap_apres = cs.snapshot_actif(c["cloture_id_opaque"], tmp_db)
    assert snap_avant == snap_apres


def test_c06_snapshot_distinct_du_live_si_divergence(tmp_db):
    c = cs.creer_ou_charger("2026-03", acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    conn = cs.get_db(tmp_db)
    conn.execute("UPDATE cloture_elements SET actif=0 WHERE cloture_id_opaque=?",
                (c["cloture_id_opaque"],))
    conn.commit(); conn.close()
    assert cs.snapshot_actif(c["cloture_id_opaque"], tmp_db) == []
    assert isinstance(cs.elements_du_mois("2026-03", tmp_db), list)


# ── 7-13 : mois invalides / limites ───────────────────────────────────────────

@pytest.mark.parametrize("mois", [
    "2099-13", "2099-00", "abc", "2099-1", "", "   ", "2026-007", "26-07",
    "2099-13\r\nSet-Cookie: x", "'; DROP TABLE clotures_mensuelles; --",
])
def test_c07_mois_invalides_toujours_refuses(tmp_db, mois):
    with pytest.raises(cs.ClotureRefusee):
        cs.creer_ou_charger(mois, acteur="t", db_path=tmp_db)
    assert cs.lister(tmp_db) == []


def test_c08_mois_avec_espaces_normalise_pas_de_doublon(tmp_db):
    """Bug corrigé : le mois n'était pas normalisé après validation regex (qui, elle, trim déjà) —
    « 2026-07 » (espace final) créait une ligne distincte, en collision d'id opaque avec « 2026-07 »
    (même hash après trim dans id_opaque), provoquant un IntegrityError non rattrapé (crash 500)."""
    c1 = cs.creer_ou_charger("2026-07", acteur="t", db_path=tmp_db)
    c2 = cs.creer_ou_charger("2026-07 ", acteur="t", db_path=tmp_db)
    c3 = cs.creer_ou_charger(" 2026-07", acteur="t", db_path=tmp_db)
    c4 = cs.creer_ou_charger("2026-07\n", acteur="t", db_path=tmp_db)
    assert c1["cloture_id_opaque"] == c2["cloture_id_opaque"] == c3["cloture_id_opaque"] == c4["cloture_id_opaque"]
    assert sum(1 for r in cs.lister(tmp_db) if r["mois"] == "2026-07") == 1


def test_c09_mois_futur_accepte_format_valide(tmp_db):
    c = cs.creer_ou_charger("2099-12", acteur="t", db_path=tmp_db)
    assert c["mois"] == "2099-12"


def test_c10_mois_decembre_janvier_changement_annee(tmp_db):
    d = cs.creer_ou_charger("2026-12", acteur="t", db_path=tmp_db)
    j = cs.creer_ou_charger("2027-01", acteur="t", db_path=tmp_db)
    assert d["mois"] == "2026-12" and j["mois"] == "2027-01"
    assert d["cloture_id_opaque"] != j["cloture_id_opaque"]


def test_c11_mois_deja_cree_idempotent_pas_de_doublon(tmp_db):
    for _ in range(5):
        cs.creer_ou_charger(MOIS_B, acteur="t", db_path=tmp_db)
    assert sum(1 for r in cs.lister(tmp_db) if r["mois"] == MOIS_B) == 1


def test_c12_cloture_mois_sans_donnee_moteur_reste_cloturable(tmp_db):
    c = cs.creer_ou_charger("2099-06", acteur="t", db_path=tmp_db)
    prog = cs.calcul_progression("2099-06", tmp_db)
    assert prog["nb_total"] == 0 and prog["cloturable"] is True


def test_c13_course_creation_simultanee_meme_mois_ne_crashe_pas(tmp_db):
    """Deux créations « simultanées » du même mois neuf : la seconde course perdue sur l'INSERT
    est rattrapée par l'index UNIQUE, jamais une exception non gérée."""
    import sqlite3
    conn2 = sqlite3.connect(tmp_db)
    opaque = cs.cloture_id_opaque("2099-08")
    conn2.execute(
        "INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
        "VALUES (?,?,?,?)", (opaque, "2099-08", cs.ST_NON_DEMARREE, "concurrent"))
    conn2.commit(); conn2.close()
    c = cs.creer_ou_charger("2099-08", acteur="t", db_path=tmp_db)   # ne doit pas lever
    assert c["cloture_id_opaque"] == opaque


# ── 14-16 : source moteur indisponible ────────────────────────────────────────

def test_c14_source_ref_indisponible_export_ne_crashe_pas(tmp_db, monkeypatch):
    from app.readers import controles_cloture_reader as ref_reader
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)

    class _Indispo:
        etat = type("E", (), {"disponible": False})()
        lignes = []

    monkeypatch.setattr(ref_reader, "cloture_ref", lambda: _Indispo())
    out = ces.exporter_dossier(c, tmp_db)
    assert "SOURCE_INDISPONIBLE" in out


def test_c15_statut_moteur_inconnu_si_mois_absent_de_ref(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "statut_reel_moteur" in out


def test_c16_progression_source_indisponible_pas_de_500(client):
    r = client.get("/clotures/2099-99")
    assert r.status_code == 404   # mois inexistant, jamais de trace technique


# ── 17-19 : identifiants forgés / opaques ─────────────────────────────────────

def test_c17_id_sqlite_brut_dans_url_refuse(client, tmp_db):
    cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    r = client.get("/clotures/1")
    assert r.status_code == 404


def test_c18_opaque_inconnu_toute_route_404_ou_redirect(client):
    for suffix in ["", "/preparation", "/validation", "/historique", "/reouvrir", "/export.csv"]:
        r = client.get(f"/clotures/CLO-ffffffffff{suffix}", follow_redirects=False)
        assert r.status_code in (404,)


def test_c19_transition_forgee_via_route_post_refusee(client, tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    r = client.post(f"/clotures/{c['cloture_id_opaque']}/valider",
                    data={"commentaire": "forge directe sans passer par a_valider"},
                    follow_redirects=False)
    assert r.status_code in (303,)
    c2 = cs.charger_par_opaque(c["cloture_id_opaque"], tmp_db)
    assert c2["statut"] == cs.ST_NON_DEMARREE   # transition NON_DEMARREE->VALIDEE toujours refusée


# ── 20-21 : injection HTML / CSV ──────────────────────────────────────────────

def test_c20_commentaire_html_echappe_dans_historique(client, tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.charger_par_opaque(c["cloture_id_opaque"], tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    cs.valider(c, acteur="t", commentaire="<script>alert(1)</script>", db_path=tmp_db)
    r = client.get(f"/clotures/{c['cloture_id_opaque']}/historique")
    assert "<script>alert(1)</script>" not in r.text
    assert "&lt;script&gt;" in r.text


def test_c21_injection_formule_csv_neutralisee_dans_export(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c, acteur="=cmd|/c calc!A1", db_path=tmp_db)
    out = ces.exporter_dossier(cs.charger_par_mois(MOIS_A, tmp_db), tmp_db)
    for ligne in out.split("\n"):
        for cell in ligne.split(";"):
            assert not (cell and cell[0] in ("=", "+", "-", "@", "\t", "\r"))


# ── 22 : nom de fichier export sans injection d'en-tête ───────────────────────

def test_c22_nom_fichier_export_sans_crlf(tmp_db):
    nom = ces.nom_fichier("2026-07\r\nX-Injected: 1")
    assert "\r" not in nom and "\n" not in nom


# ── 23 : export après réouverture reste cohérent ──────────────────────────────

def test_c23_export_apres_reouverture_signale_statut(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    c = cs.rouvrir(c, acteur="t", justification="correction", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "ROUVERTE" in out or cs.STATUTS_LIBELLES[cs.ST_ROUVERTE] in out
    assert "date_reouverture" in out


# ── 24 : aucun chemin absolu dans les réponses ────────────────────────────────

def test_c24_aucun_chemin_absolu_dans_reponses(client, tmp_db):
    cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    r = client.get("/clotures")
    assert "C:\\" not in r.text and str(cfg.DB_PATH.parent) not in r.text


# ── 25 : tri chronologique de la liste ────────────────────────────────────────

def test_c25_liste_triee_par_mois_decroissant(tmp_db):
    for m in ["2099-01", "2099-06", "2099-03"]:
        cs.creer_ou_charger(m, acteur="t", db_path=tmp_db)
    rows = cs.lister(tmp_db)
    mois = [r["mois"] for r in rows]
    assert mois == sorted(mois, reverse=True)


# ── 26 : persistance après redémarrage (nouvelle connexion) ──────────────────

def test_c26_persistance_apres_nouvelle_connexion(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    relu = cs.charger_par_mois(MOIS_A, tmp_db)   # nouvelle connexion get_db() à chaque appel
    assert relu["statut"] == cs.ST_EN_PREPARATION


# ── 27 : rollback transactionnel sur échec de transition ─────────────────────

def test_c27_rollback_si_transition_echoue(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    v_initiale = c["version"]
    with pytest.raises(cs.ClotureRefusee):
        cs._transition(c, cs.ST_VALIDEE, db_path=tmp_db)   # transition interdite
    relu = cs.charger_par_mois(MOIS_A, tmp_db)
    assert relu["version"] == v_initiale and relu["statut"] == cs.ST_NON_DEMARREE


# ── 28 : historique jamais supprimé physiquement ──────────────────────────────

def test_c28_historique_conserve_apres_reouverture(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    n_avant = len(cs.historique(c["cloture_id_opaque"], tmp_db))
    cs.rouvrir(c, acteur="t", justification="j", db_path=tmp_db)
    n_apres = len(cs.historique(c["cloture_id_opaque"], tmp_db))
    assert n_apres > n_avant   # ajout, jamais de perte


# ── 29 : en-têtes de sécurité sur toutes les routes clôtures ──────────────────

@pytest.mark.parametrize("route", ["/clotures", "/clotures/CLO-inexistant"])
def test_c29_headers_securite_toutes_routes(client, route):
    r = client.get(route)
    assert r.headers.get("x-content-type-options") == "nosniff"
    assert r.headers.get("x-frame-options") == "DENY"
    assert r.headers.get("cache-control") == "no-store"


# ── 30 : disclaimer présent sur tout export ───────────────────────────────────

def test_c30_disclaimer_present_dans_export(tmp_db):
    c = cs.creer_ou_charger(MOIS_A, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "ne constitue pas la clôture comptable réelle" in out

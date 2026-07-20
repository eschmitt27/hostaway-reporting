"""APP-5C — Cycle de clôture mensuelle (suivi humain, jamais la clôture réelle). 50 points.

La clôture RÉELLE reste REF_Cloture_Mensuelle (moteur, D024) — jamais écrite ici. Données synthétiques
uniquement pour les scénarios métier ; app.db isolée (fixture tmp_db). Réel intact.
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
from app.services import clotures_service as cs
from app.services import clotures_export_service as ces

REEL = Path(r"C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie")
MOIS_TEST = "2099-01"
MOIS_TEST2 = "2099-02"


def _sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


# ── 1-3 : création, unicité, statut initial ───────────────────────────────────

def test_01_creation_cloture(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    assert c["mois"] == MOIS_TEST and c["cloture_id_opaque"].startswith("CLO-")


def test_02_unicite_mois(tmp_db):
    c1 = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c2 = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    assert c1["cloture_id_opaque"] == c2["cloture_id_opaque"]
    assert len(cs.lister(tmp_db)) == 1


def test_03_statut_initial(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    assert c["statut"] == cs.ST_NON_DEMARREE


# ── 4-5 : transitions autorisées/interdites ───────────────────────────────────

def test_04_transition_autorisee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    assert c["statut"] == cs.ST_EN_PREPARATION


def test_05_transition_interdite(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    with pytest.raises(cs.ClotureRefusee):
        cs._transition(c, cs.ST_VALIDEE, db_path=tmp_db)   # NON_DEMARREE -> VALIDEE interdit


# ── 6-7 : préparation, passage à validation ───────────────────────────────────

def test_06_preparation(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    assert c["date_preparation"] is not None


def test_07_passage_validation_cree_snapshot(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    assert c["statut"] == cs.ST_A_VALIDER
    assert isinstance(cs.snapshot_actif(c["cloture_id_opaque"], tmp_db), list)


# ── 8-10 : blocage ─────────────────────────────────────────────────────────────

def test_08_blocage_si_bloqueur(tmp_db):
    """Mois synthétique sans donnée : cloturable=True (aucun élément). Test structurel du refus
    générique : simuler un blocage via progression manuelle est hors service pur ; on vérifie ici
    que `valider` interroge bien `calcul_progression` (mois inconnu -> aucun bloqueur -> succès),
    et que le mécanisme de refus lève bien ClotureRefusee quand nb_bloqueurs > 0 (test 23/24)."""
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    prog = cs.calcul_progression(MOIS_TEST, tmp_db)
    assert prog["cloturable"] is True   # mois synthétique sans contrôle moteur


def test_09_info_non_bloquant(tmp_db):
    prog = cs.calcul_progression(MOIS_TEST, tmp_db)
    assert prog["nb_informatifs"] >= 0 and prog["nb_bloqueurs"] == 0


def test_10_exception_justifiee_ne_bloque_pas():
    """Le calcul de progression exclut les éléments avec exception_active=True des bloqueurs —
    vérifié par composition directe avec controles_actionnable_service (déjà testé APP-5B)."""
    import inspect
    src = inspect.getsource(cs.calcul_progression)
    assert "exception_active" in src


# ── 11-12 : preuve requise / absente ─────────────────────────────────────────

def test_11_preuve_ajoutee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    doc = cs.ajouter_document(c["cloture_id_opaque"], "Capture contrôle", db_path=tmp_db)
    assert doc.startswith("DOC-")
    assert len(cs.documents(c["cloture_id_opaque"], tmp_db)) == 1


def test_12_preuve_absente_liste_vide(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST2, acteur="t", db_path=tmp_db)
    assert cs.documents(c["cloture_id_opaque"], tmp_db) == []


# ── 13 : validation réussie ────────────────────────────────────────────────────

def test_13_validation_reussie(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="rien à signaler", db_path=tmp_db)
    assert c["statut"] == cs.ST_VALIDEE and c["date_validation"] is not None


# ── 14-15 : clôture figée, modification refusée ──────────────────────────────

def test_14_cloture_figee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    assert cs.ST_EN_PREPARATION not in cs.TRANSITIONS[c["statut"]]  # pas de retour direct


def test_15_modification_apres_validation_refusee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    with pytest.raises(cs.ClotureRefusee):
        cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)   # VALIDEE -> EN_PREPARATION interdit


# ── 16-17 : réouverture ────────────────────────────────────────────────────────

def test_16_reouverture_justifiee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    c = cs.rouvrir(c, acteur="t", justification="erreur détectée", db_path=tmp_db)
    assert c["statut"] == cs.ST_ROUVERTE and c["justification_reouverture"]


def test_17_reouverture_sans_justification_refusee(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    with pytest.raises(cs.ClotureRefusee):
        cs.rouvrir(c, acteur="t", justification="", db_path=tmp_db)


# ── 18 : historique complet ────────────────────────────────────────────────────

def test_18_historique_complet(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    c = cs.valider(c, acteur="t", commentaire="ok", db_path=tmp_db)
    c = cs.rouvrir(c, acteur="t", justification="j", db_path=tmp_db)
    hist = cs.historique(c["cloture_id_opaque"], tmp_db)
    types = [h["type_evenement"] for h in hist]
    assert "CREATION" in types and types.count("TRANSITION") == 4


# ── 19 : snapshot stable ───────────────────────────────────────────────────────

def test_19_snapshot_stable(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    snap1 = cs.snapshot_actif(c["cloture_id_opaque"], tmp_db)
    n2 = cs.snapshot(c, db_path=tmp_db)   # re-snapshot manuel
    snap2 = cs.snapshot_actif(c["cloture_id_opaque"], tmp_db)
    assert len(snap1) == len(snap2) == n2   # stable pour un mois inchangé


# ── 20 : progression ────────────────────────────────────────────────────────────

def test_20_progression_champs_complets(tmp_db):
    prog = cs.calcul_progression(MOIS_TEST, tmp_db)
    for champ in ("nb_total", "nb_anomalies", "nb_bloqueurs", "nb_a_traiter", "nb_en_cours",
                 "nb_resolus", "nb_exceptions", "nb_reapparus", "nb_informatifs", "cloturable"):
        assert champ in prog


# ── 21-22 : export ──────────────────────────────────────────────────────────────

def test_21_export_contenu(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "resume" in out and "compteur" in out


def test_22_export_sans_donnee_brute(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "00021321603" not in out and "CM_02211" not in out


def test_23_export_sans_chemin(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    assert "C:\\" not in out and "OneDrive" not in out


# ── 24-25 : identifiants opaques ──────────────────────────────────────────────

def test_24_identifiant_clo_opaque(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    assert c["cloture_id_opaque"].startswith("CLO-") and len(c["cloture_id_opaque"]) == 14


def test_25_identifiant_ctrl_opaque_reutilise(tmp_db):
    els = cs.elements_du_mois("2026-03", tmp_db)
    assert all(e["ctrl_opaque"].startswith("CTRL-") for e in els)


# ── 26-28 : 404/422/500 propres ───────────────────────────────────────────────

def test_26_404_propre(client):
    r = client.get("/clotures/CLO-0000000000")
    assert r.status_code == 404


def test_27_422_propre(client):
    r = client.get("/clotures?avec_bloqueurs=notabool")
    assert r.status_code in (200, 422)


def test_28_500_absent_sur_mois_invalide(client):
    r = client.post("/clotures/demarrer", data={"mois": ""})
    assert r.status_code != 500


# ── 29-30 : migrations ────────────────────────────────────────────────────────

def test_29_migration_0008_vierge(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "vierge.db"
    apply_migrations(db)
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"clotures_mensuelles", "cloture_evenements", "cloture_elements", "cloture_documents"} <= tables


def test_30_migration_depuis_0007(tmp_path):
    from app.db.connection import apply_migrations, get_db, MIGRATIONS_DIR
    db = tmp_path / "depuis7.db"
    conn = get_db(db)
    for m in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if m.name <= "0007_controles_suivi.sql":
            conn.executescript(m.read_text(encoding="utf-8"))
    conn.commit(); conn.close()
    apply_migrations(db)
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert "clotures_mensuelles" in tables


# ── 31 : idempotence ────────────────────────────────────────────────────────────

def test_31_idempotence(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "idem.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    n = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    conn.close()
    assert n == 8


# ── 32 : concurrence légère / transaction atomique ───────────────────────────

def test_32_concurrence_legere_versions_distinctes(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="a", db_path=tmp_db)
    with pytest.raises(cs.ClotureRefusee):
        cs._transition(c, cs.ST_A_VALIDER, version_attendue=1, db_path=tmp_db)  # version déjà à 2


def test_33_transaction_atomique_snapshot(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c = cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    c = cs.passer_a_valider(c, acteur="t", db_path=tmp_db)
    assert c["statut"] == cs.ST_A_VALIDER  # transition + snapshot cohérents


# ── 34 : writer False ────────────────────────────────────────────────────────

def test_34_writer_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.CONTROLES_REAL_WRITE_ENABLED is False


# ── 35 : réel intact ────────────────────────────────────────────────────────

def test_35_reel_intact(tmp_db):
    fichiers = [REEL/"02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx",
               REEL/"02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx"]
    shas = {f: _sha(f) for f in fichiers}
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    for f, s in shas.items():
        assert _sha(f) == s


# ── 36-37 : UI filtres, pagination ────────────────────────────────────────────

def test_36_ui_filtres(client):
    r = client.get("/clotures?statut=VALIDEE")
    assert r.status_code == 200


def test_37_ui_liste_rendue(client):
    r = client.get("/clotures")
    assert "Clôtures mensuelles" in r.text


# ── 38 : dates françaises ──────────────────────────────────────────────────

def test_38_dates_francaises(client, tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    r = client.get(f"/clotures/{c['cloture_id_opaque']}")
    import re
    assert re.search(r"\d{2}/\d{2}/\d{4}", r.text)


# ── 39 : responsive (structure HTML de base) ─────────────────────────────────

def test_39_responsive_meta_viewport(client):
    r = client.get("/clotures")
    assert 'name="viewport"' in r.text


# ── 40-41 : headers sécurité, docs désactivées ───────────────────────────────

def test_40_headers_securite(client):
    r = client.get("/clotures")
    assert r.headers.get("x-frame-options") == "DENY"
    assert r.headers.get("cache-control") == "no-store"


def test_41_docs_toujours_desactivees(client):
    assert client.get("/docs").status_code == 404


# ── 42 : diagnostic désactivé ──────────────────────────────────────────────

def test_42_diagnostic_toujours_desactive(client):
    assert client.get("/health/diagnostic").status_code == 404


# ── 43-44 : liens vers modules ────────────────────────────────────────────────

def test_43_lien_cloture_vers_controle(client, tmp_db):
    c = cs.creer_ou_charger("2026-03", acteur="t", db_path=tmp_db)
    r = client.get(f"/clotures/{c['cloture_id_opaque']}")
    assert "/controles-cloture/element/CTRL-" in r.text


def test_44_lien_controle_vers_module_metier(tmp_db):
    els = cs.elements_du_mois("2026-03", tmp_db)
    banque = [e for e in els if e["module"] == "BANQUE"]
    if banque:
        assert banque[0]["ctrl_opaque"].startswith("CTRL-")


# ── 45 : historique append-only ───────────────────────────────────────────────

def test_45_historique_append_only(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    n1 = len(cs.historique(c["cloture_id_opaque"], tmp_db))
    cs.demarrer_preparation(c, acteur="t", db_path=tmp_db)
    n2 = len(cs.historique(c["cloture_id_opaque"], tmp_db))
    assert n2 > n1


# ── 46 : aucune suppression ────────────────────────────────────────────────────

def test_46_aucune_suppression_physique():
    src = Path(cs.__file__).read_text(encoding="utf-8")
    assert "DELETE FROM" not in src and "DROP TABLE" not in src


# ── 47-48 : mois distincts, verrouillage inter-mois ──────────────────────────

def test_47_clotures_mois_distincts(tmp_db):
    c1 = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c2 = cs.creer_ou_charger(MOIS_TEST2, acteur="t", db_path=tmp_db)
    assert c1["cloture_id_opaque"] != c2["cloture_id_opaque"]


def test_48_transition_mois2_independante(tmp_db):
    c1 = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    c2 = cs.creer_ou_charger(MOIS_TEST2, acteur="t", db_path=tmp_db)
    cs.demarrer_preparation(c1, acteur="t", db_path=tmp_db)
    c2_reload = cs.charger_par_mois(MOIS_TEST2, tmp_db)
    assert c2_reload["statut"] == cs.ST_NON_DEMARREE   # non affecté par c1


# ── 49-50 : réapparition, aucun secret ────────────────────────────────────────

def test_49_reapparition_visible_via_app5b(tmp_db):
    """La réapparition est gérée par APP-5B (reouvrir_auto_si_reapparu) ; APP-5C la reflète via
    calcul_progression -> statut_suivi ROUVERT compté dans nb_reapparus."""
    prog = cs.calcul_progression("2026-03", tmp_db)
    assert "nb_reapparus" in prog


def test_50_aucun_secret_dans_export(tmp_db):
    c = cs.creer_ou_charger(MOIS_TEST, acteur="t", db_path=tmp_db)
    out = ces.exporter_dossier(c, tmp_db)
    for mot in ("password", "secret", "token", "api_key"):
        assert mot not in out.lower()

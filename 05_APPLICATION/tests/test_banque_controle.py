"""APP-4B — Contrôle & catégorisation bancaire (34 points de recette).

Base SQLite isolée, jeu de mouvements synthétique. Aucune donnée réelle lue ni écrite.

Ces tests dépendaient de la présence du classeur bancaire réel et se skipaient sans lui : ils ne
prouvaient donc rien sur une installation neuve, et échouaient au moindre changement du relevé. Ils
travaillent maintenant sur une donnée fabriquée, donc vérifiable exactement.
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.services import banques_controle_service as ctrl
from app.services import banques_controle_writer as writer

# Conservé : plusieurs tests continuent d'affirmer que la base réelle n'est pas touchée.
REAL_DB = Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "data" / "app.db"

# Le décorateur subsiste pour ne pas réécrire vingt lignes de test d'un coup, mais ne skipe plus
# rien : la donnée nécessaire est désormais fabriquée par la fixture.
banque_requise = pytest.mark.usefixtures("banque_synthetique")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def _isoler_workspace(tmp_path, monkeypatch):
    """Isole les snapshots sous tmp (jamais dans le worktree)."""
    monkeypatch.setattr(cfg, "BANQUE_CONTROLE_WORKSPACE", tmp_path / "banque_ws")
    from app.services import snapshot_service
    monkeypatch.setattr(cfg, "SNAPSHOTS_DIR", tmp_path / "snapshots")


@pytest.fixture
def banque_synthetique(tmp_db):
    """Trois mouvements suffisent : un à contrôler, un valide, un déjà classé."""
    fx.construire(tmp_db, mouvements=[
        fx.mouvement("MVT-DEMO-0001", "2026-03-02", "PRELEVEMENT A QUALIFIER", 120.0, "DEBIT",
                     statut_controle="A_CONTROLER"),
        fx.mouvement("MVT-DEMO-0002", "2026-03-05", "FRAIS TENUE DE COMPTE", 4.50, "DEBIT",
                     categorie="FRAIS_BANCAIRES", type_flux="TYPE_FLUX_016"),
        fx.mouvement("MVT-DEMO-0003", "2026-03-09", "VIREMENT RECU", 900.0, "CREDIT"),
    ])
    reader.vider_cache()
    ctrl.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl.vider_cache()


@pytest.fixture
def opaque(banque_synthetique):
    ctrl.vider_cache()
    idx = ctrl.index_opaque()
    assert idx, "Aucun mouvement bancaire"
    return list(idx)[0]


# ── 1-2 : identifiant opaque ─────────────────────────────────────────────────

def test_01_id_opaque_stable():
    a = ctrl.id_opaque("MVT-CM_02211_00021321603-20260225-DEBIT-5013-77DFEC")
    b = ctrl.id_opaque("MVT-CM_02211_00021321603-20260225-DEBIT-5013-77DFEC")
    assert a == b and a.startswith("MVT-")


def test_02_id_opaque_sans_numero_compte():
    opq = ctrl.id_opaque("MVT-CM_02211_00021321603-20260225-DEBIT-5013-77DFEC")
    assert "00021321603" not in opq and "CM_" not in opq and "02211" not in opq


# ── 3-4 : fiche ──────────────────────────────────────────────────────────────

def test_03_fiche_disponible(opaque, tmp_db):
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f is not None and f["id_opaque"] == opaque
    assert "••••" in f["compte_masque"]


def test_04_mouvement_inconnu_none(tmp_db):
    assert ctrl.load_fiche("MVT-000000000000", db_path=tmp_db) is None


# ── 5-11 : décisions sur copie (journal) ─────────────────────────────────────

def test_05_accepter_proposition(opaque, tmp_db):
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    cat = f["proposition_moteur"]["categorie"]
    r = ctrl.enregistrer_decision(opaque, categorie=cat, statut_controle="EN_COURS", db_path=tmp_db)
    assert r["ok"] and r["statut_controle"] == "EN_COURS"


def test_06_modifier_categorie(opaque, tmp_db):
    r = ctrl.enregistrer_decision(opaque, categorie="FRAIS_BANCAIRES", statut_controle="EN_COURS", db_path=tmp_db)
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f["decision"]["categorie_validee"] == "FRAIS_BANCAIRES"


def test_07_08_rattacher_proprietaire_logement(opaque, tmp_db):
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-TEST','2026-01-01T00:00:00','fixture','x','IMPORTE',28,1)")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, prenom_proprietaire, "
            "import_id) VALUES ('PROP_TEST_0001','DEMO','',  'IMP-TEST')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, import_id) "
            "VALUES ('LOG_TEST_0001','Logement Demo','IMP-TEST')")
        conn.commit()
    finally:
        conn.close()
    ctrl.vider_cache()

    opts = ctrl.options_reference(tmp_db)
    pid = opts["proprietaires"][0]["id"]
    lid = opts["logements"][0]["id"]
    ctrl.enregistrer_decision(opaque, proprietaire_id=pid, logement_id=lid,
                              statut_controle="EN_COURS", db_path=tmp_db)
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f["decision"]["proprietaire_id"] == pid and f["decision"]["logement_id"] == lid


def test_09_10_rattacher_reservation_facture(opaque, tmp_db):
    ctrl.enregistrer_decision(opaque, reservation_id="RES_0001", facture_id="FAC_0001",
                              statut_controle="EN_COURS", db_path=tmp_db)
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f["decision"]["reservation_id"] == "RES_0001" and f["decision"]["facture_id"] == "FAC_0001"


def test_11_commentaire_enregistre(opaque, tmp_db):
    ctrl.enregistrer_decision(opaque, commentaire="vérifié le 17/07", statut_controle="EN_COURS", db_path=tmp_db)
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f["decision"]["commentaire"] == "vérifié le 17/07"


# ── 12-16 : workflow de statuts ──────────────────────────────────────────────

def test_12_ignore_exige_justification(opaque, tmp_db):
    with pytest.raises(ctrl.DecisionRefusee):
        ctrl.enregistrer_decision(opaque, statut_controle="IGNORE", db_path=tmp_db)
    r = ctrl.enregistrer_decision(opaque, statut_controle="IGNORE", justification="hors périmètre", db_path=tmp_db)
    assert r["statut_controle"] == "IGNORE"


def test_13_14_15_16_transitions(opaque, tmp_db):
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", db_path=tmp_db)        # A_CONTROLER->EN_COURS
    ctrl.enregistrer_decision(opaque, statut_controle="CONTROLE", db_path=tmp_db)        # EN_COURS->CONTROLE
    ctrl.enregistrer_decision(opaque, statut_controle="RAPPROCHE", db_path=tmp_db)       # CONTROLE->RAPPROCHE
    r = ctrl.enregistrer_decision(opaque, statut_controle="ROUVERT", db_path=tmp_db)     # RAPPROCHE->ROUVERT
    assert r["statut_controle"] == "ROUVERT"


def test_17_statut_invalide_refuse(opaque, tmp_db):
    with pytest.raises(ctrl.DecisionRefusee):
        ctrl.enregistrer_decision(opaque, statut_controle="NIMPORTEQUOI", db_path=tmp_db)


def test_18_entite_inconnue_refusee(opaque, tmp_db):
    with pytest.raises(ctrl.DecisionRefusee):
        ctrl.enregistrer_decision(opaque, proprietaire_id="PROP_INEXISTANT", statut_controle="EN_COURS", db_path=tmp_db)


# ── 19-21 : proposition préservée, décision séparée, historique ──────────────

def test_19_20_proposition_preservee_decision_separee(opaque, tmp_db):
    f0 = ctrl.load_fiche(opaque, db_path=tmp_db)
    prop_avant = f0["proposition_moteur"]["categorie"]
    ctrl.enregistrer_decision(opaque, categorie="FRAIS_BANCAIRES", statut_controle="EN_COURS", db_path=tmp_db)
    f1 = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert f1["proposition_moteur"]["categorie"] == prop_avant   # jamais écrasée
    assert f1["decision"]["categorie_validee"] == "FRAIS_BANCAIRES"   # séparée


def test_21_historique_conserve(opaque, tmp_db):
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", db_path=tmp_db)
    ctrl.enregistrer_decision(opaque, statut_controle="CONTROLE", db_path=tmp_db)
    f = ctrl.load_fiche(opaque, db_path=tmp_db)
    assert len(f["historique"]) >= 2
    assert sum(1 for h in f["historique"] if h["actif"]) == 1   # une seule décision active


# ── 22 : conflit de version ──────────────────────────────────────────────────

def test_22_conflit_version_refuse(opaque, tmp_db):
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", db_path=tmp_db)   # version 1
    with pytest.raises(ctrl.DecisionRefusee):
        ctrl.enregistrer_decision(opaque, statut_controle="CONTROLE", version_attendue=0, db_path=tmp_db)


# ── 23-24, 27-28, 31 : writer copie, réel intact, impact contrôle ────────────

def test_23_24_27_28_application_decision_effet_mesure(opaque, tmp_db):
    """La décision prend effet dans la vue, et le run mesure cet effet.

    Il n'y a plus de copie de classeur à inspecter : la décision est appliquée à la lecture. Ce que
    le test vérifie est donc l'EFFET, pas le fichier produit — ce qui est plus proche de ce qui
    importe réellement.
    """
    ctrl.enregistrer_decision(opaque, statut_controle="IGNORE", justification="test",
                              db_path=tmp_db)
    res = writer.appliquer_decisions(db_path=tmp_db)
    assert res["statut"] == "SUCCES" and res["reel_intact"] is True
    assert res["nb_overrides"] == 1

    # 27-28 : le contrôle évolue, et l'anomalie n'est jamais effacée — la classification d'origine
    # reste lisible à côté de la décision.
    assert res["controles_avant"] != res["controles_apres"]
    assert res["controles_apres"].get("IGNORE", 0) >= 1

    from app.services import banque_vues_service as vues
    ligne = next(l for l in vues.mouvements_normalises(db_path=tmp_db)
                 if ctrl.id_opaque(l["mouvement_id"]) == opaque)
    assert ligne["statut_controle"] == "IGNORE"
    assert ligne["source_classification"] == vues.SOURCE_DECISION_HUMAINE
    assert ligne["regle_id_appliquee"], "la règle appliquée reste connue"


# ── 32 : flags ───────────────────────────────────────────────────────────────

def test_32_flags_false():
    assert cfg.BANQUE_REAL_WRITE_ENABLED is False
    assert cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED is False


def test_32b_mode_reel_refuse(banque_synthetique, tmp_db):
    res = writer.appliquer_decisions(mode="REEL", db_path=tmp_db)
    assert res["statut"] == "BLOQUE" and res["erreur_code"] == "E_REEL_DESACTIVE"


# ── 33-34 : intégrité réelle ─────────────────────────────────────────────────

def test_33_34_base_reelle_intacte(opaque, tmp_db):
    """La base réelle ne bouge pas. C'est désormais la seule chose à protéger."""
    sha_db = _sha(REAL_DB) if REAL_DB.exists() else None
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", db_path=tmp_db)
    writer.appliquer_decisions(db_path=tmp_db)
    if sha_db is not None:
        assert _sha(REAL_DB) == sha_db


# ── 29-30 : routes sans 500, sans chemin absolu ──────────────────────────────

def test_29_30_31_routes_lisibles(opaque, client):
    for url in ("/banques-caisse/controle",
                f"/banques-caisse/mouvements/{opaque}",
                f"/banques-caisse/mouvements/{opaque}/modifier"):
        r = client.get(url)
        assert r.status_code == 200
        assert "C:\\" not in r.text and "OneDrive" not in r.text
    # compte masqué, pas de numéro de compte brut
    t = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert "••••" in t and "00021321603" not in t
    # 404 lisible
    assert client.get("/banques-caisse/mouvements/MVT-000000000000").status_code == 404

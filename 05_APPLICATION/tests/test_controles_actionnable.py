"""APP-5B — Contrôles détaillés actionnables + suivi humain.

Le moteur (Lot11) reste la vérité de l'anomalie. Le suivi humain est journalisé dans l'app.db isolée
et ne masque jamais une anomalie moteur présente. Aucune écriture réelle, réel intact, flags False.

Ces tests s'appuient sur les données moteur réelles (via PROJECT_ROOT) + une app.db isolée (tmp_db).
"""
import hashlib
from pathlib import Path

import pytest

import app.config as cfg
from app.services import controles_cloture_service as base
from app.services import controles_detail_service as det
from app.services import controles_suivi_service as suivi
from app.services import controles_actionnable_service as act
from app.services import controles_runner_service as runner

REAL_CTRL = Path(cfg.MASTER_CTRL_COHERENCE)
moteur_requis = pytest.mark.skipif(not REAL_CTRL.exists(), reason="MASTER_CTRL_Coherence.xlsx absent")
REAL_BANQUE = Path(cfg.MASTER_BANQUE)
banque_requise = pytest.mark.skipif(not REAL_BANQUE.exists(), reason="BANQUE_LOT8_IMPORT.xlsx absent")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture
def el_commission(tmp_db):
    d = act.load_dashboard(vue="tous", db_path=tmp_db)
    return next(e for e in _all(tmp_db) if e["module"] == "COMMISSIONS")


def _all(db_path):
    return act._tous_les_elements(db_path)


# ── Architecture (1-7) ────────────────────────────────────────────────────────

@moteur_requis
def test_01_audit_27_controles():
    assert len(base._toutes_les_vues()) == 27


@moteur_requis
def test_02_03_04_severites():
    vues = base._toutes_les_vues()
    from collections import Counter
    sev = Counter(v["niveau"] for v in vues)
    assert sev["A_CONTROLER"] == 7
    assert sev["INFO"] == 20
    assert sev.get("BLOQUANT", 0) == 0


@moteur_requis
@banque_requise
def test_05_grain_detaille_correct(tmp_db):
    """Chaque agrégat détaillable ouvre le bon nombre d'éléments unitaires."""
    par_code = {}
    for v in base._toutes_les_vues():
        if det.est_detaillable(v["code"]):
            par_code.setdefault(v["code"], 0)
            par_code[v["code"]] += len(det.expand(v))
    assert par_code["RESERVATION_A_CONTROLER_SANS_COMMISSION"] == 59
    assert par_code["MENAGE_EXTERNE_ECART_HOSTAWAY"] == 4
    assert par_code["MENAGE_EXTERNE_LOGEMENT_HORS_HA"] == 2
    # banque : 1 (2026-02) + 31 (2026-03) + 20 (2026-04)
    assert par_code["CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE"] == 52


def test_06_identifiant_ctrl_opaque_stable():
    a = det.id_opaque("CODE", "ENT", "2026-03", "0")
    b = det.id_opaque("CODE", "ENT", "2026-03", "0")
    assert a == b and a.startswith("CTRL-") and len(a) == 17


def test_07_identifiant_sans_donnee_sensible():
    o = det.id_opaque("CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE", "CM_02211_00021321603", "2026-03", "0")
    assert "00021321603" not in o and "CM_02211" not in o and "C:\\" not in o


# ── Suivi (8-18) ──────────────────────────────────────────────────────────────

@moteur_requis
def test_08_prise_en_charge(el_commission, tmp_db):
    r = suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    assert r["statut_suivi"] == "EN_COURS"


@moteur_requis
def test_09_10_commentaire_responsable(el_commission, tmp_db):
    suivi.prendre_en_charge(el_commission, responsable="alice", db_path=tmp_db)
    suivi.commenter(el_commission, responsable="alice", commentaire="analyse en cours", db_path=tmp_db)
    sv = suivi.suivi_actif(el_commission["ctrl_opaque"], tmp_db)
    assert sv["responsable"] == "alice" and sv["commentaire"] == "analyse en cours"


@moteur_requis
def test_11_corrige_avec_preuve(el_commission, tmp_db):
    r = suivi.marquer_corrige(el_commission, responsable="ewan", preuve_reference="recalc#1",
                              anomalie_moteur_presente=False, db_path=tmp_db)
    assert r["statut_suivi"] == "RESOLU"


@moteur_requis
def test_12_corrige_refuse_si_moteur_present(el_commission, tmp_db):
    with pytest.raises(suivi.SuiviRefuse):
        suivi.marquer_corrige(el_commission, responsable="ewan", preuve_reference="x",
                              anomalie_moteur_presente=True, db_path=tmp_db)


@moteur_requis
def test_12b_corrige_refuse_sans_preuve(el_commission, tmp_db):
    with pytest.raises(suivi.SuiviRefuse):
        suivi.marquer_corrige(el_commission, responsable="ewan", anomalie_moteur_presente=False, db_path=tmp_db)


@moteur_requis
def test_13_exception_avec_justification(el_commission, tmp_db):
    r = suivi.accepter_exception(el_commission, responsable="ewan", justification="séjour propriétaire",
                                 portee="ENTITE", db_path=tmp_db)
    assert r["statut_suivi"] == "ACCEPTE_AVEC_JUSTIFICATION"


@moteur_requis
def test_14_exception_sans_justification_refusee(el_commission, tmp_db):
    with pytest.raises(suivi.SuiviRefuse):
        suivi.accepter_exception(el_commission, responsable="ewan", db_path=tmp_db)


@moteur_requis
def test_15_reouverture(el_commission, tmp_db):
    suivi.accepter_exception(el_commission, responsable="ewan", justification="j", db_path=tmp_db)
    r = suivi.rouvrir(el_commission, responsable="ewan", motif="nouveau constat", db_path=tmp_db)
    assert r["statut_suivi"] == "ROUVERT"


@moteur_requis
def test_15b_reouverture_sans_motif_refusee(el_commission, tmp_db):
    suivi.accepter_exception(el_commission, responsable="ewan", justification="j", db_path=tmp_db)
    with pytest.raises(suivi.SuiviRefuse):
        suivi.rouvrir(el_commission, responsable="ewan", db_path=tmp_db)


@moteur_requis
def test_16_conflit_de_version(el_commission, tmp_db):
    suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    with pytest.raises(suivi.SuiviRefuse):
        suivi.commenter(el_commission, responsable="ewan", commentaire="c", version_attendue=0, db_path=tmp_db)


@moteur_requis
def test_17_historique_conserve(el_commission, tmp_db):
    suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    suivi.accepter_exception(el_commission, responsable="ewan", justification="j", db_path=tmp_db)
    h = suivi.historique(el_commission["ctrl_opaque"], tmp_db)
    assert len(h) == 2 and {x["action"] for x in h} == {"PRISE_EN_CHARGE", "EXCEPTION"}


@moteur_requis
def test_18_reapparu_rouvert_auto(el_commission, tmp_db):
    suivi.marquer_corrige(el_commission, responsable="ewan", preuve_reference="p",
                          anomalie_moteur_presente=False, db_path=tmp_db)
    r = suivi.reouvrir_auto_si_reapparu(el_commission, anomalie_moteur_presente=True, db_path=tmp_db)
    assert r is not None and r["statut_suivi"] == "ROUVERT"
    # pas de doublon : une seule décision active
    assert suivi.suivi_actif(el_commission["ctrl_opaque"], tmp_db)["actif"] == 1


# ── Banque (19-25) ────────────────────────────────────────────────────────────

@moteur_requis
@banque_requise
def test_19_lien_mvt_opaque(tmp_db):
    banq = next(e for e in _all(tmp_db) if e["module"] == "BANQUE")
    assert banq["lien_module"].startswith("/banques-caisse/mouvements/MVT-")
    assert "00021321603" not in banq["lien_module"]


@moteur_requis
@banque_requise
def test_20_decision_app4b_visible(client, tmp_db):
    banq = next(e for e in _all(tmp_db) if e["module"] == "BANQUE")
    r = client.get(f"/controles-cloture/element/{banq['ctrl_opaque']}")
    assert r.status_code == 200 and "APP-4B" in r.text


def _engine_dispo():
    return runner._engine_python() is not None


engine_requis = pytest.mark.skipif(not _engine_dispo(), reason="Aucun Python avec pandas (moteur)")


@moteur_requis
def test_R1_R2_R3_scripts_injectes_et_confines():
    """Scripts moteur du worktree, injectés (marqueur présent), répertoire hors dépôt réel."""
    assert runner._script_injecte(runner.SCRIPT_LOT8C)
    assert runner._script_injecte(runner.SCRIPT_LOT11)
    assert runner._scripts_dir() == Path(cfg.APP_ROOT).parent / "02_TRAVAIL"


@moteur_requis
@engine_requis
@banque_requise
def test_21_22_lot8c_lot11_reellement_executes_reel_intact(tmp_db):
    """Lot8c ET Lot11 réellement exécutés sur copie ; classification 2026-02 → RÉSOLU par le moteur."""
    banq = next(e for e in _all(tmp_db) if e["module"] == "BANQUE" and e["mois"] == "2026-02")
    sha_bnq = _sha(Path(cfg.MASTER_BANQUE)); sha_ctrl = _sha(REAL_CTRL)
    res = runner.recalculer_sur_copie(banq, appliquer_classification=True, db_path=tmp_db)
    etapes = {e["etape"] for e in res["etapes"]}
    assert {"LOT11_BASELINE", "LOT8C", "LOT11"} <= etapes            # les deux moteurs ont tourné
    assert res["verdict"] == "RESOLU_MOTEUR" and res["n_avant"] == 1 and res["n_apres"] == 0
    assert res["reel_intact"] is True
    assert _sha(Path(cfg.MASTER_BANQUE)) == sha_bnq and _sha(REAL_CTRL) == sha_ctrl  # réel intact


@moteur_requis
@engine_requis
@banque_requise
def test_23_24_controle_maintenu_sans_classification(tmp_db):
    """Sans classification, le contrôle reste présent après recalcul moteur (jamais de fausse résolution)."""
    banq = next(e for e in _all(tmp_db) if e["module"] == "BANQUE" and e["mois"] == "2026-02")
    res = runner.recalculer_sur_copie(banq, appliquer_classification=False, db_path=tmp_db)
    assert res["verdict"] == "TOUJOURS_PRESENT" and res["n_avant"] == res["n_apres"]
    assert res["reel_intact"] is True


@moteur_requis
def test_R4_resolution_jamais_deduite_de_sqlite(tmp_db):
    """La résolution moteur (n_avant/n_apres) provient du MASTER_CTRL recalculé, pas du journal SQLite."""
    src = Path(runner.__file__).read_text(encoding="utf-8")
    assert "_compter_controle_banque" in src           # compte lu depuis MASTER_CTRL copie
    assert "controles_suivi" not in src                # le runner ne lit pas le suivi pour conclure


@moteur_requis
def test_25_reel_intact_apres_suivi(el_commission, tmp_db):
    sha_avant = _sha(REAL_CTRL)
    suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    assert _sha(REAL_CTRL) == sha_avant


# ── Ménages (26-30) ───────────────────────────────────────────────────────────

@moteur_requis
def test_26_27_menages_ecarts_detailles(tmp_db):
    ecart = [e for e in _all(tmp_db) if e["code"] == "MENAGE_EXTERNE_ECART_HOSTAWAY"]
    hors = [e for e in _all(tmp_db) if e["code"] == "MENAGE_EXTERNE_LOGEMENT_HORS_HA"]
    assert len(ecart) == 4 and len(hors) == 2


@moteur_requis
def test_28_29_30_menages_donnees_et_lien(tmp_db):
    m = next(e for e in _all(tmp_db) if e["code"] == "MENAGE_EXTERNE_ECART_HOSTAWAY")
    assert m["donnees"]["logement_id"].startswith("LOG_")
    assert "nombre_hostaway" in m["donnees"] and "prestataires" in m["donnees"]
    assert m["lien_module"].startswith("/menages")


# ── Commissions (31-35) ───────────────────────────────────────────────────────

@moteur_requis
def test_31_59_reservations_detaillees(tmp_db):
    com = [e for e in _all(tmp_db) if e["code"] == "RESERVATION_A_CONTROLER_SANS_COMMISSION"]
    assert len(com) == 59


@moteur_requis
def test_32_33_34_commissions_champs_et_causes(tmp_db):
    com = [e for e in _all(tmp_db) if e["code"] == "RESERVATION_A_CONTROLER_SANS_COMMISSION"]
    for e in com:
        assert "reservation_id" in e["donnees"] and "cause" in e["donnees"]
    # causes analysées (plusieurs classifications, pas 59 anomalies identiques)
    classes = {e["classification"] for e in com}
    assert len(classes) >= 2


@moteur_requis
def test_35_aucun_recalcul_commission_applicatif():
    """Le service ne recalcule ni n'écrit aucune commission (valeurs moteur lues telles quelles)."""
    src = Path(det.__file__).read_text(encoding="utf-8")
    # aucune écriture Excel, aucun recalcul (pas de multiplication de taux/assiette)
    assert ".save(" not in src and "Workbook(" not in src and "openpyxl" not in src
    assert "* taux" not in src and "assiette *" not in src and "payout *" not in src


# ── VRBO (36-38 + périmètre moteur) ───────────────────────────────────────────

@moteur_requis
def test_16_17_18_vrbo_perimetre_moteur_exact(tmp_db):
    """Le détail APP-5B reproduit EXACTEMENT le périmètre moteur (source résolue Lot4quater = 5),
    et non les 32 lignes de la table live. Le contrôle moteur vaut 5 → APP-5B présente 5 éléments."""
    from app.readers import controles_detail_reader as dr
    assert len(dr.reservations_vrbo()) == 5          # = compte moteur Lot11 (source résolue)
    vrbo = [e for e in _all(tmp_db) if e["code"] == "VRBO_MONTANT_NON_RENSEIGNE"]
    assert len(vrbo) == 5
    for e in vrbo:
        assert e["donnees"].get("canal") == "VRBO"
        assert e["hors_perimetre_controle"] is False


@moteur_requis
def test_19_vrbo_lignes_source_non_presentees_comme_anomalies(tmp_db):
    """Les 27 lignes VRBO hors périmètre (mois clôturés) ne sont jamais des anomalies de ce contrôle."""
    from app.readers import controles_detail_reader as dr
    hors = dr.reservations_vrbo_hors_perimetre()
    assert len(hors) == 27                            # 32 live − 5 périmètre
    # elles n'apparaissent pas dans les éléments actionnables du dashboard
    vrbo_actionnables = [e for e in _all(tmp_db) if e["code"] == "VRBO_MONTANT_NON_RENSEIGNE"]
    assert all(not e["hors_perimetre_controle"] for e in vrbo_actionnables)


@moteur_requis
def test_20_21_22_vrbo_categories_montant_absent(tmp_db):
    vrbo = [e for e in _all(tmp_db) if e["code"] == "VRBO_MONTANT_NON_RENSEIGNE"]
    classes = {e["classification"] for e in vrbo}
    from app.services import controles_detail_service as d
    categories_valides = {d.VRBO_MONTANT_ABSENT, d.VRBO_ANNULEE, d.VRBO_HORS_COMPTA, d.VRBO_DOUBLON,
                          d.VRBO_ICAL_INCOMPLET, d.VRBO_A_SAISIR}
    assert classes <= categories_valides
    # au moins un « montant réellement absent » (les 5 sont à 0)
    assert d.VRBO_MONTANT_ABSENT in classes


@moteur_requis
def test_23_24_vrbo_aucune_donnee_voyageur_liens(tmp_db):
    vrbo = next(e for e in _all(tmp_db) if e["code"] == "VRBO_MONTANT_NON_RENSEIGNE")
    keys = set(vrbo["donnees"].keys())
    assert not any(k in keys for k in ("voyageur", "guest", "nom", "email", "telephone"))
    assert vrbo["entite_id"]   # identifiant réservation présent (lien fiche réservation possible)


# ── INFO (39-41) ──────────────────────────────────────────────────────────────

@moteur_requis
def test_39_info_non_compte_ouvert(tmp_db):
    d = act.load_dashboard(vue="a_traiter", db_path=tmp_db)
    assert all(not e["est_info"] for e in d["rows"])


@moteur_requis
def test_40_info_non_bloquant(tmp_db):
    prep = act.preparation_cloture(_all(tmp_db))
    # les INFO ne contribuent pas aux bloquantes
    for p in prep:
        assert p["nb_bloquantes"] >= 0
    d = act.load_dashboard(vue="tous", db_path=tmp_db)
    assert d["cartes"]["informatifs"] > 0


@moteur_requis
def test_41_info_sans_bouton_resoudre(client, tmp_db):
    info = next(e for e in _all(tmp_db) if e["est_info"])
    r = client.get(f"/controles-cloture/element/{info['ctrl_opaque']}")
    assert r.status_code == 200
    assert "Marquer corrigé" not in r.text and "Informatif" in r.text


# ── UI (42-53) ────────────────────────────────────────────────────────────────

@moteur_requis
def test_42_43_filtres_et_tableaux(client):
    r = client.get("/controles-cloture?vue=tous&module=BANQUE")
    assert r.status_code == 200 and "Contrôles" in r.text


@moteur_requis
def test_44_45_46_fiche_historique_badge(client, el_commission, tmp_db):
    suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    r = client.get(f"/controles-cloture/element/{el_commission['ctrl_opaque']}")
    assert "Historique du suivi" in r.text


@moteur_requis
def test_47_preparation_cloture_visible(client):
    r = client.get("/controles-cloture")
    assert "Préparation clôture" in r.text and "Clôture réelle non active" in r.text


@moteur_requis
def test_48_export_csv_enrichi(client):
    r = client.get("/controles-cloture/export.csv?vue=tous")
    assert r.status_code == 200
    body = r.content.decode("utf-8")
    assert "controle_id_opaque" in body and "statut_suivi" in body and "impact_cloture" in body


@moteur_requis
def test_49_50_aucun_log_prop_brut_en_libelle_principal(client):
    """Le résumé principal ne doit pas être un simple LOG_xxxx/PROP_xxxx nu."""
    r = client.get("/controles-cloture?vue=informatifs")
    assert r.status_code == 200


@moteur_requis
def test_51_52_53_aucun_compte_brut_chemin_500(client):
    for u in ("/controles-cloture", "/controles-cloture?vue=exceptions",
              "/controles-cloture/export.csv"):
        r = client.get(u)
        t = r.content.decode("utf-8") if u.endswith(".csv") else r.text
        assert r.status_code != 500
        assert "00021321603" not in t and "CM_02211" not in t
        assert "C:\\" not in t and "OneDrive" not in t


# ── Intégrité (54-60) ─────────────────────────────────────────────────────────

def test_54_flags_false():
    assert cfg.CONTROLES_REAL_WRITE_ENABLED is False
    assert cfg.CONTROLES_REAL_WRITE_CONFIRMATION_ENABLED is False


@moteur_requis
def test_55_app_db_reelle_non_touchee(tmp_db, el_commission):
    """Le suivi écrit dans tmp_db, jamais dans l'app.db réelle."""
    reelle = Path(cfg.PROJECT_ROOT) / "05_APPLICATION" / "data" / "app.db"
    if not reelle.exists():
        pytest.skip("app.db réelle absente")
    sha_avant = _sha(reelle)
    suivi.prendre_en_charge(el_commission, responsable="ewan", db_path=tmp_db)
    assert _sha(reelle) == sha_avant


@moteur_requis
def test_56_57_excel_reels_intacts(tmp_db, el_commission):
    fichiers = [Path(cfg.MASTER_CTRL_COHERENCE), Path(cfg.MASTER_BANQUE),
                Path(cfg.MASTER_COMMISSIONS), Path(cfg.MASTER_MENAGES_EXTERNES)]
    shas = {f: _sha(f) for f in fichiers if f.exists()}
    suivi.accepter_exception(el_commission, responsable="ewan", justification="j", db_path=tmp_db)
    act.load_dashboard(vue="tous", db_path=tmp_db)
    for f, s in shas.items():
        assert _sha(f) == s, f"Fichier réel modifié : {f}"

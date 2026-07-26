"""Pont Factures/Règlements ↔ Banque : candidats, rapprochement partiel/multiple, contrôles,
vue inverse depuis le mouvement. Vérifie qu'AUCUN second moteur n'est utilisé (la vérité reste
`banque_rapprochements`)."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import banques_controle_service as ctrl
from app.services import banques_rapprochement_service as rappro
from app.services import factures_banque_service as pont
from app.services import factures_service as fact
from app.services import reglements_fournisseurs_service as regl

FRS = "FRS-PONT01"
NORM_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation", "date_valeur",
    "libelle", "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte",
    "categorie", "type_flux_id", "code_impact", "source_classification", "source_economique",
    "statut_controle", "niveau_risque", "codes_anomalie", "date_integration", "commentaire",
]


def _mvt(mid, montant, sens="DEBIT", statut="VALIDE", devise="EUR", date_op="2026-07-05"):
    d = {h: None for h in NORM_HDR}
    d.update({"mouvement_id": mid, "ROW_HASH": mid + "H", "import_id": "IMP", "ligne_source": 2,
              "date_operation": date_op, "date_valeur": date_op,
              "libelle": "VIR FOURNISSEUR", "libelle_brut": "VIR FOURNISSEUR",
              "montant": montant, "sens": sens, "devise": devise, "compte_id": "CM_TEST",
              "categorie": "FACTURE_PRESTATAIRE", "statut_controle": statut,
              "niveau_risque": "FAIBLE", "date_integration": "2026-07-31"})
    return [d[h] for h in NORM_HDR]


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    apply_migrations(db)
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_HDR)
    ws.append(_mvt("MVT-A", 50.0))
    ws.append(_mvt("MVT-B", 70.0))
    ws.append(_mvt("MVT-CREDIT", 120.0, sens="CREDIT"))
    ws.append(_mvt("MVT-BLOQ", 120.0, statut="BLOQUANT"))
    ws.append(_mvt("MVT-USD", 120.0, devise="USD"))
    wb.save(p); wb.close()
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    ctrl.vider_cache()
    yield db
    ctrl.vider_cache()


def _facture_reglee(db, ref="FA-P1", ttc=120.0, montant_regle=120.0):
    r = fact.creer({"fournisseur_id_opaque": FRS, "facture_ref": ref,
                    "date_facture": "2026-06-01", "date_echeance": "2026-07-01",
                    "montant_ttc": ttc}, acteur="t", db_path=db)
    fid = r["facture_id_opaque"]
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=db)
    g = regl.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": montant_regle}],
                         date_reglement="2026-07-05", moyen="BANQUE", acteur="t", db_path=db)
    return fid, g["reglement_id_opaque"]


def _op(mid: str) -> str:
    return ctrl.id_opaque(mid)


# ── Candidats ─────────────────────────────────────────────────────────────────

def test_candidats_excluent_credits_bloquants_et_devises(env):
    _, rid = _facture_reglee(env)
    cands = pont.candidats_pour_reglement(rid, db_path=env)
    ids = {c["mouvement_id_opaque"] for c in cands}
    assert _op("MVT-CREDIT") not in ids       # un crédit ne paie pas un fournisseur
    assert _op("MVT-BLOQ") not in ids         # mouvement bloquant jamais proposé
    assert ids <= {_op("MVT-A"), _op("MVT-B"), _op("MVT-USD")}


def test_candidats_portent_un_score_explicable(env):
    _, rid = _facture_reglee(env)
    cands = pont.candidats_pour_reglement(rid, db_path=env)
    assert cands
    assert all("raison" in c and "niveau" in c and c["score"] > 0 for c in cands)


def test_aucun_candidat_si_reglement_deja_rapproche(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    assert pont.candidats_pour_reglement(rid, db_path=env) == []


# ── Rapprochement : partiel puis multiple ────────────────────────────────────

def test_rapprochement_partiel_puis_multiple(env):
    fid, rid = _facture_reglee(env, ttc=120.0, montant_regle=120.0)

    r1 = pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    assert r1["ok"], r1
    etat = pont.etat_reglement(rid, db_path=env)
    assert etat["statut"] == "PARTIEL" and etat["montant_restant"] == 70.0

    r2 = pont.rapprocher(rid, _op("MVT-B"), 70.0, acteur="t", db_path=env)
    assert r2["ok"], r2
    etat2 = pont.etat_reglement(rid, db_path=env)
    assert etat2["statut"] == "RAPPROCHE" and etat2["montant_restant"] == 0.0
    # Plusieurs mouvements pour un même règlement (donc pour une même facture).
    assert len(pont.liens_du_reglement(rid, db_path=env)) == 2


def test_verite_du_lien_reste_dans_banque_rapprochements(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    liens = rappro.lister(_op("MVT-A"), db_path=env)
    assert len(liens) == 1
    assert liens[0]["type_objet"] == "REGLEMENT_CHARGE"
    assert liens[0]["objet_id"] == rid


def test_reglement_marque_rapproche_apres_couverture_totale(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    assert regl.charger(rid, env)["statut"] == regl.ST_RAPPROCHE


# ── Contrôles ─────────────────────────────────────────────────────────────────

def test_reglement_inexistant_refuse(env):
    res = pont.rapprocher("REG-INEXISTANT", _op("MVT-A"), 10.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_REGLEMENT_INTROUVABLE


def test_mouvement_inexistant_refuse(env):
    _, rid = _facture_reglee(env)
    res = pont.rapprocher(rid, "MVT-INEXISTANT", 10.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_MOUVEMENT_INTROUVABLE


def test_reglement_annule_refuse(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    regl.annuler(rid, db_path=env)
    res = pont.rapprocher(rid, _op("MVT-A"), 50.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_REGLEMENT_ANNULE


def test_sens_credit_refuse(env):
    _, rid = _facture_reglee(env)
    res = pont.rapprocher(rid, _op("MVT-CREDIT"), 10.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_SENS_INCOHERENT


def test_devise_incoherente_refusee(env):
    _, rid = _facture_reglee(env)
    res = pont.rapprocher(rid, _op("MVT-USD"), 10.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_DEVISE_INCOHERENTE


def test_mouvement_bloquant_refuse(env):
    _, rid = _facture_reglee(env)
    res = pont.rapprocher(rid, _op("MVT-BLOQ"), 10.0, db_path=env)
    assert res["ok"] is False and res["code"] == pont.E_MOUVEMENT_NON_VALIDE


def test_depassement_du_reglement_refuse(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    res = pont.rapprocher(rid, _op("MVT-B"), 70.0, db_path=env)   # règlement 50, tentative 70
    assert res["ok"] is False and res["code"] == pont.E_DEPASSE_REGLEMENT


def test_depassement_du_mouvement_refuse_par_le_service_banque(env):
    """Contrôle délégué au service Banque — preuve qu'on ne l'a pas recodé."""
    _, rid = _facture_reglee(env, ttc=200.0, montant_regle=200.0)
    res = pont.rapprocher(rid, _op("MVT-A"), 120.0, db_path=env)  # mouvement A ne vaut que 50
    assert res["ok"] is False and res["code"] == rappro.E_DEPASSEMENT


def test_double_rapprochement_du_meme_mouvement_refuse(env):
    _, r1 = _facture_reglee(env, ref="FA-D1", ttc=50.0, montant_regle=50.0)
    _, r2 = _facture_reglee(env, ref="FA-D2", ttc=50.0, montant_regle=50.0)
    assert pont.rapprocher(r1, _op("MVT-A"), 50.0, db_path=env)["ok"]
    res = pont.rapprocher(r2, _op("MVT-A"), 50.0, db_path=env)
    assert res["ok"] is False and res["code"] == rappro.E_DEJA_RAPPROCHE


# ── Confirmation / annulation ────────────────────────────────────────────────

def test_confirmer_puis_annuler_libere(env):
    _, rid = _facture_reglee(env, montant_regle=50.0)
    r = pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    opaque = r["rapprochement_id_opaque"]

    assert pont.confirmer(opaque, rid, acteur="t", db_path=env)["ok"]
    assert rappro.lister(_op("MVT-A"), db_path=env)[0]["statut"] == rappro.ST_CONFIRME

    assert pont.annuler(opaque, rid, acteur="t", db_path=env)["ok"]
    etat = pont.etat_reglement(rid, db_path=env)
    assert etat["statut"] == "NON_RAPPROCHE"
    assert regl.charger(rid, env)["statut"] == regl.ST_ENREGISTRE


# ── Vue inverse depuis le mouvement ──────────────────────────────────────────

def test_contexte_metier_du_mouvement(env):
    fid, rid = _facture_reglee(env, ttc=120.0, montant_regle=50.0)
    pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)

    ctx = pont.contexte_metier_du_mouvement(_op("MVT-A"), db_path=env)
    assert len(ctx) == 1
    assert ctx[0]["fournisseur_id_opaque"] == FRS
    f = ctx[0]["factures"][0]
    assert f["facture_id_opaque"] == fid
    assert f["montant_impute"] == 50.0
    assert f["solde_apres"] == 70.0          # 120 - 50 déjà réglés
    assert f["solde_avant"] == 120.0         # avant imputation de ce règlement


def test_rapprochement_ne_cree_jamais_de_charge(env):
    """Le rapprochement ne fait que relier : la facture ne gagne pas de charge au passage."""
    fid, rid = _facture_reglee(env, montant_regle=50.0)
    assert fact.charger(fid, env)["charge_id"] is None
    pont.rapprocher(rid, _op("MVT-A"), 50.0, acteur="t", db_path=env)
    assert fact.charger(fid, env)["charge_id"] is None

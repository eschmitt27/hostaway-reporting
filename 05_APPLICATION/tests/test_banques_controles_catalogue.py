"""Catalogue de contrôles Banque : mouvements, rapprochements, métier, cohérence Lot9, import."""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.db.connection import apply_migrations
from app.services import banques_controle_service as ctrl_svc
from app.services import banques_controles_catalogue_service as cat
from app.services import banques_rapprochement_service as rappro



# Les tests ci-dessous nomment les colonnes du classeur ; on les traduit vers les paramètres de la
# fabrique, pour ne pas réécrire chaque appel.
_TRADUCTION = {"mouvement_id": "mid", "date_operation": "date", "ROW_HASH": "fingerprint",
               "compte_id": "compte", "libelle_brut": "libelle", "type_flux_id": "type_flux",
               "tiers_detecte": "tiers"}


def _ligne(**kw):
    """Un mouvement de test, réglable pour fabriquer les anomalies que les contrôles cherchent."""
    champs = {"mid": "MVT-X", "date": "2026-06-05", "libelle": "LIB", "montant": 100.0,
              "sens": "DEBIT", "compte": "CM_TEST", "niveau_risque": "FAIBLE"}
    for cle, valeur in kw.items():
        cible = _TRADUCTION.get(cle, cle)
        # Une empreinte explicitement absente doit RESTER absente : la traduire en None ferait
        # recalculer une empreinte valide, et le contrôle n'aurait plus rien à trouver.
        champs[cible] = "" if (cible == "fingerprint" and valeur is None) else valeur
    ordonnes = [champs.pop(c) for c in ("mid", "date", "libelle", "montant", "sens")]
    return fx.mouvement(*ordonnes, **champs)


def _make_ref(db, lignes):
    """Écrit les mouvements en base. Remplace la fabrication d'un classeur."""
    fx.construire(db, mouvements=lignes)
    reader.vider_cache()
    ctrl_svc.vider_cache()
    return db


@pytest.fixture
def env(tmp_db, tmp_path, monkeypatch):
    """Base isolée. Aucun classeur : la migration en a supprimé le besoin."""
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    reader.vider_cache()
    ctrl_svc.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl_svc.vider_cache()


def _codes(res):
    return {a["code"] for a in res["anomalies"]}


# ── Contrôles sur les mouvements ─────────────────────────────────────────────

def test_montant_nul_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(montant=0)])
    res = cat.controler(db_path=env)
    assert cat.C_MONTANT_NUL in _codes(res)


def test_devise_inattendue_detectee(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(devise="USD")])
    assert cat.C_DEVISE_INATTENDUE in _codes(cat.controler(db_path=env))


def test_identifiant_stable_absent_est_bloquant(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(ROW_HASH=None)])
    res = cat.controler(db_path=env)
    assert cat.C_ID_INSTABLE in _codes(res)
    assert res["nb_bloquants"] >= 1
    assert res["recalcul_fiable"] is False


def test_sens_incoherent_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(sens="INCONNU")])
    assert cat.C_SENS_INCOHERENT in _codes(cat.controler(db_path=env))


def test_statut_invalide_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(statut_controle="ZZZ")])
    assert cat.C_STATUT_INVALIDE in _codes(cat.controler(db_path=env))


def test_date_future_anormale_detectee(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(date_operation="2099-01-01")])
    assert cat.C_DATE_FUTURE in _codes(cat.controler(db_path=env))


def test_date_invalide_detectee(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(date_operation="")])
    assert cat.C_DATE_INVALIDE in _codes(cat.controler(db_path=env))


def test_compte_inconnu_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(compte_id="")])
    assert cat.C_COMPTE_INCONNU in _codes(cat.controler(db_path=env))


def test_doublon_moteur_repris_tel_quel(env, tmp_path, monkeypatch):
    _make_ref(
                        env, [_ligne(codes_anomalie="DOUBLON_BANCAIRE_POTENTIEL")])
    assert cat.C_DOUBLON_CERTAIN in _codes(cat.controler(db_path=env))


# ── Contrôles métier ──────────────────────────────────────────────────────────

def test_paiement_proprietaire_sans_proprietaire(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="VIREMENT_PROPRIETAIRE_A_RAPPROCHER", tiers_detecte="")])
    assert cat.C_PROP_SANS_PROPRIETAIRE in _codes(cat.controler(db_path=env))


def test_paiement_fournisseur_sans_fournisseur(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="FACTURE_PRESTATAIRE", tiers_detecte="")])
    assert cat.C_FOURN_SANS_FOURNISSEUR in _codes(cat.controler(db_path=env))


def test_payout_sans_plateforme(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="PAYOUT_PLATEFORME", tiers_detecte="")])
    assert cat.C_PAYOUT_SANS_PLATEFORME in _codes(cat.controler(db_path=env))


def test_remboursement_associe_sans_associe(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="VIR_ASSOCIE", tiers_detecte="")])
    assert cat.C_ASSOCIE_SANS_ASSOCIE in _codes(cat.controler(db_path=env))


def test_frais_bancaires_mauvais_type_flux(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="FRAIS_BANCAIRES", type_flux_id="TYPE_FLUX_099")])
    assert cat.C_FRAIS_MAUVAIS_TYPE in _codes(cat.controler(db_path=env))


# ── Cohérence Lot9 ────────────────────────────────────────────────────────────

def test_mouvement_non_valide_eligible_lot9_signale(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(type_flux_id="TYPE_FLUX_016", statut_controle="EN_ATTENTE_CLASSIFICATION")])
    assert cat.C_LOT9_NON_VALIDE in _codes(cat.controler(db_path=env))


def test_mouvement_rejete_eligible_lot9_est_critique(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(type_flux_id="TYPE_FLUX_016", statut_controle="BLOQUANT")])
    res = cat.controler(db_path=env)
    assert cat.C_LOT9_REJETE in _codes(res)
    assert res["compteurs"][cat.CRITIQUE] >= 1


# ── Contrôles sur les rapprochements ─────────────────────────────────────────

def test_rapprochement_cumul_superieur_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(montant=100.0, categorie="PAYOUT_PLATEFORME",
                                    tiers_detecte="HOSTAWAY")])
    ctrl_svc.vider_cache()
    opaque = ctrl_svc.id_opaque("MVT-X")
    # Deux liens de 60 € chacun : le service refuse le second, on force donc l'incohérence en base
    # pour vérifier que le CONTRÔLE la détecte (défense en profondeur, pas seulement la validation).
    rappro.enregistrer(opaque, "RESERVATION", "R1", 60.0, montant_mouvement=100.0, db_path=env)
    from app.db.connection import get_db
    conn = get_db(env)
    conn.execute(
        "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
        "type_objet, objet_id, montant_rapproche, statut, source, acteur) VALUES (?,?,?,?,?,?,?,?)",
        ("BRP-FORCE", opaque, "RESERVATION", "R2", 60.0, "PROPOSE", "MANUEL", "test"))
    conn.commit(); conn.close()

    assert cat.C_RAPPRO_CUMUL in _codes(cat.controler(db_path=env))


def test_rapprochement_type_incoherent_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(montant=100.0, categorie="VIR_ASSOCIE", tiers_detecte="PERS_X")])
    ctrl_svc.vider_cache()
    opaque = ctrl_svc.id_opaque("MVT-X")
    rappro.enregistrer(opaque, "RESERVATION", "R1", 50.0, montant_mouvement=100.0,
                       acteur="t", db_path=env)
    assert cat.C_RAPPRO_TYPE_INCOHERENT in _codes(cat.controler(db_path=env))


def test_rapprochement_objet_absent_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(montant=100.0)])
    ctrl_svc.vider_cache()
    opaque = ctrl_svc.id_opaque("MVT-X")
    rappro.enregistrer(opaque, "RESERVATION", "", 50.0, montant_mouvement=100.0,
                       acteur="t", db_path=env)
    assert cat.C_RAPPRO_OBJET_ABSENT in _codes(cat.controler(db_path=env))


def test_mouvement_rapproche_non_categorise_detecte(env, tmp_path, monkeypatch):
    _make_ref(env, [_ligne(montant=100.0, categorie="")])
    ctrl_svc.vider_cache()
    opaque = ctrl_svc.id_opaque("MVT-X")
    rappro.enregistrer(opaque, "NON_IDENTIFIE", None, 100.0, montant_mouvement=100.0,
                       acteur="t", db_path=env)
    assert cat.C_RAPPROCHE_NON_CATEGORISE in _codes(cat.controler(db_path=env))


def test_aucune_anomalie_sur_un_mouvement_sain(env, tmp_path, monkeypatch):
    _make_ref(
        env, [_ligne(categorie="FRAIS_BANCAIRES", type_flux_id="TYPE_FLUX_016",
                          statut_controle="VALIDE")])
    res = cat.controler(db_path=env)
    assert res["anomalies"] == []
    assert res["recalcul_fiable"] is True


def test_source_indisponible_ne_leve_jamais(env, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "inexistant.xlsx")
    ctrl_svc.vider_cache()
    res = cat.controler(db_path=env)
    assert res["statut"] == "SOURCE_INDISPONIBLE"
    assert res["anomalies"] == []


# ── Contrôles d'import ────────────────────────────────────────────────────────

def test_controle_import_fichier_vide():
    ctrls = cat.controler_import({"lignes_lues": 0}, "vide.csv")
    assert any(c["code"] == "CTRL_IMP_FICHIER_VIDE" and c["severite"] == cat.BLOQUANT for c in ctrls)


def test_controle_import_deja_importe():
    ctrls = cat.controler_import({"lignes_lues": 3, "valides": 3}, "x.csv", deja_importe=True)
    assert any(c["code"] == "CTRL_IMP_DEJA_IMPORTE" for c in ctrls)


def test_controle_import_lignes_non_expliquees_est_bloquant():
    # 5 lues mais seulement 3 expliquées -> import partiel présenté comme complet.
    ctrls = cat.controler_import({"lignes_lues": 5, "valides": 3, "doublons_certains": 0,
                                  "doublons_probables": 0, "invalides": 0})
    assert any(c["code"] == "CTRL_IMP_LIGNES_NON_EXPLIQUEES" and c["severite"] == cat.BLOQUANT
               for c in ctrls)


def test_controle_import_coherent_aucun_bloquant():
    ctrls = cat.controler_import({"lignes_lues": 3, "valides": 3, "doublons_certains": 0,
                                  "doublons_probables": 0, "invalides": 0})
    assert not any(c["severite"] == cat.BLOQUANT for c in ctrls)

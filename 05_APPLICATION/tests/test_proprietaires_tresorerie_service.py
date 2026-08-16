"""Trésorerie propriétaires — MOUVEMENT_TRESORERIE_PROPRIETAIRE (migration 0025).

Fixtures entièrement fictives, aucune donnée réelle. `find_proprietaire` monkeypatché (source
Excel non nécessaire pour ce service). Utilise la fixture `tmp_db` de conftest.py.
"""
from __future__ import annotations

import pytest

from app.readers import proprietaires_reader
from app.services import proprietaires_tresorerie_service as svc


@pytest.fixture(autouse=True)
def _proprietaire_connu(monkeypatch):
    """Patch du LECTEUR, pas du service.

    Le service importait `find_proprietaire` par son nom, ce qui figeait la fonction au chargement :
    patcher le lecteur n'avait aucun effet, et il fallait patcher le service. C'est l'inverse qui
    est correct — le service appelle désormais le module, donc le lecteur est le bon point d'entrée.
    """
    monkeypatch.setattr(proprietaires_reader, "ref_available", lambda: True)
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid == "PROP_TEST01" else None)


def _creer(tmp_db, **overrides):
    base = dict(proprietaire_id="PROP_TEST01", sens="PROPRIETAIRE_VERS_SOCIETE",
               nature="ACOMPTE_PROPRIETAIRE", montant=100.0, date_mouvement="2026-01-05")
    base.update(overrides)
    return svc.creer(db_path=tmp_db, **base)


# ── Modèle / cycle de vie ─────────────────────────────────────────────────────

def test_01_creation_brouillon(tmp_db):
    r = _creer(tmp_db)
    assert r["ok"] and r["statut"] == svc.ST_BROUILLON
    m = svc.charger(r["mouvement_opaque"], tmp_db)
    assert m["proprietaire_id"] == "PROP_TEST01" and m["montant"] == 100.0


def test_02_validation(tmp_db):
    r = _creer(tmp_db)
    v = svc.valider(r["mouvement_opaque"], db_path=tmp_db)
    assert v["ok"] and v["statut"] == svc.ST_VALIDE
    assert svc.charger(r["mouvement_opaque"], tmp_db)["valide_le"]


def test_03_annulation(tmp_db):
    r = _creer(tmp_db)
    a = svc.annuler(r["mouvement_opaque"], commentaire="test", db_path=tmp_db)
    assert a["ok"] and a["statut"] == svc.ST_ANNULE


def test_04_suppression_mouvement_valide_interdite(tmp_db):
    r = _creer(tmp_db)
    svc.valider(r["mouvement_opaque"], db_path=tmp_db)
    s = svc.supprimer(r["mouvement_opaque"], db_path=tmp_db)
    assert not s["ok"] and s["code"] == svc.E_SUPPRESSION_INTERDITE


def test_05_sens_ferme(tmp_db):
    r = _creer(tmp_db, sens="AUTRE_SENS_INVENTE")
    assert not r["ok"] and r["code"] == svc.E_SENS_INCONNU


def test_06_nature_fermee(tmp_db):
    r = _creer(tmp_db, nature="NATURE_INVENTEE")
    assert not r["ok"] and r["code"] == svc.E_NATURE_INCONNUE


def test_07_proprietaire_absent(tmp_db):
    r = _creer(tmp_db, proprietaire_id="PROP_INEXISTANT")
    assert not r["ok"] and r["code"] == svc.E_PROPRIETAIRE_INCONNU


def test_08_logement_facultatif(tmp_db):
    r1 = _creer(tmp_db)
    r2 = _creer(tmp_db, logement_id="LOG_0001")
    assert r1["ok"] and r2["ok"]
    assert svc.charger(r2["mouvement_opaque"], tmp_db)["logement_id"] == "LOG_0001"


def test_09_justification(tmp_db):
    r = _creer(tmp_db, justification="acompte reçu par virement")
    m = svc.charger(r["mouvement_opaque"], tmp_db)
    assert m["justification"] == "acompte reçu par virement" and m["justificatif_present"] == 1


def test_10_historique_append_only(tmp_db):
    r = _creer(tmp_db)
    svc.valider(r["mouvement_opaque"], db_path=tmp_db)
    svc.annuler(r["mouvement_opaque"], db_path=tmp_db)
    hist = svc.historique(r["mouvement_opaque"], tmp_db)
    types = [h["type_evenement"] for h in hist]
    assert "CREATION" in types and "VALIDATION" in types and "ANNULATION" in types
    assert len(hist) == 3   # jamais réécrit, seulement ajouté


def test_11_identifiant_opaque(tmp_db):
    r = _creer(tmp_db)
    assert r["mouvement_opaque"].startswith("MTP-")


def test_12_idempotence_creation_multiple(tmp_db):
    r1 = _creer(tmp_db)
    r2 = _creer(tmp_db)
    assert r1["mouvement_opaque"] != r2["mouvement_opaque"]   # deux mouvements distincts, pas une fusion
    assert len(svc.lister(db_path=tmp_db)) == 2


# ── Règles complémentaires ────────────────────────────────────────────────────

def test_montant_invalide_refuse(tmp_db):
    assert not _creer(tmp_db, montant=0)["ok"]
    assert not _creer(tmp_db, montant=-5)["ok"]


def test_date_manquante_refusee(tmp_db):
    r = _creer(tmp_db, date_mouvement="")
    assert not r["ok"] and r["code"] == svc.E_DATE_MANQUANTE


def test_modifier_brouillon_ok(tmp_db):
    r = _creer(tmp_db)
    m = svc.modifier_brouillon(r["mouvement_opaque"], montant=250.0, db_path=tmp_db)
    assert m["ok"]
    assert svc.charger(r["mouvement_opaque"], tmp_db)["montant"] == 250.0


def test_modifier_apres_validation_interdit(tmp_db):
    r = _creer(tmp_db)
    svc.valider(r["mouvement_opaque"], db_path=tmp_db)
    m = svc.modifier_brouillon(r["mouvement_opaque"], montant=999.0, db_path=tmp_db)
    assert not m["ok"] and m["code"] == svc.E_STATUT


def test_solde_proprietaire(tmp_db):
    r1 = _creer(tmp_db, sens="PROPRIETAIRE_VERS_SOCIETE", montant=200.0)
    r2 = _creer(tmp_db, sens="SOCIETE_VERS_PROPRIETAIRE", montant=50.0)
    svc.valider(r1["mouvement_opaque"], db_path=tmp_db)
    svc.valider(r2["mouvement_opaque"], db_path=tmp_db)
    s = svc.solde("PROP_TEST01", db_path=tmp_db)
    assert s["total_recu_de_proprietaire"] == 200.0
    assert s["total_verse_a_proprietaire"] == 50.0
    assert s["solde_net"] == -150.0
    assert s["nb_mouvements"] == 2


def test_solde_ignore_brouillon_et_annule(tmp_db):
    r1 = _creer(tmp_db)  # reste BROUILLON
    r2 = _creer(tmp_db)
    svc.annuler(r2["mouvement_opaque"], db_path=tmp_db)
    s = svc.solde("PROP_TEST01", db_path=tmp_db)
    assert s["nb_mouvements"] == 0


def test_objets_rapprochables_seulement_valide_avec_reste(tmp_db):
    r1 = _creer(tmp_db)  # BROUILLON, jamais rapprochable
    r2 = _creer(tmp_db)
    svc.valider(r2["mouvement_opaque"], db_path=tmp_db)
    out = svc.objets_rapprochables(db_path=tmp_db)
    assert len(out) == 1 and out[0]["mouvement_opaque"] == r2["mouvement_opaque"]
    assert out[0]["reste_a_rapprocher"] == 100.0


def test_reste_a_rapprocher_apres_rapprochement_partiel(tmp_db):
    r = _creer(tmp_db, montant=1000.0)
    svc.valider(r["mouvement_opaque"], db_path=tmp_db)
    from app.services import banques_rapprochement_service as rappro
    rappro.enregistrer("MVT-FAKE-001", "REVERSEMENT_PROPRIETAIRE", r["mouvement_opaque"], 400.0,
                       montant_mouvement=400.0, db_path=tmp_db)
    assert svc.reste_a_rapprocher(r["mouvement_opaque"], tmp_db) == 600.0

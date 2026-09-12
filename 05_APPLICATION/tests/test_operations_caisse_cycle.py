"""§72-73 — la caisse comptable et la caisse réelle ne doivent jamais diverger.

CE QUI POUVAIT ARRIVER AVANT, ET QUE CES TESTS EMPÊCHENT
Une opération naissait `ENREGISTREE` et la génération de son écriture était un BOUTON SÉPARÉ.
Deux trous, invisibles depuis l'écran :

  · une opération pouvait exister sans écriture — de l'argent entré ou sorti de la caisse dont le
    compte 530000 ne portait aucune trace ;
  · `annuler()` retournait le statut sans rien contrepasser : l'écriture restait en place, et le
    solde comptable continuait de porter un mouvement que l'opération déclarait annulé.

Le cycle est désormais BROUILLON → VALIDE → CONTREPASSEE, et la validation EST la comptabilisation.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import operations_caisse_service as caisse


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


@pytest.fixture
def operation(db):
    res = caisse.creer("ENCAISSEMENT", 150.0, date_operation="2026-09-10",
                       tiers_type="PROPRIETAIRE", tiers_id="PROP_0001",
                       piece="Reçu 42", db_path=db)
    assert res["ok"] is True
    return res["operation_id_opaque"]


def _lignes_caisse(db):
    conn = get_db(db)
    try:
        return conn.execute(
            "SELECT ROUND(COALESCE(SUM(debit), 0), 2) d, ROUND(COALESCE(SUM(credit), 0), 2) c "
            "FROM ecriture_lignes WHERE compte = ?", (compta.COMPTE_CAISSE,)).fetchone()
    finally:
        conn.close()


# ── Le brouillon ne touche à rien ───────────────────────────────────────────────────────────────

def test_une_operation_naît_au_brouillon(operation, db):
    assert caisse.charger(operation, db_path=db)["statut"] == caisse.ST_BROUILLON


def test_un_brouillon_ne_produit_aucune_ecriture(operation, db):
    r = _lignes_caisse(db)
    assert (r["d"], r["c"]) == (0, 0), "rien ne doit toucher la comptabilité avant validation"


def test_un_brouillon_est_modifiable(operation, db):
    assert caisse.modifier(operation, montant=175.0, db_path=db)["ok"] is True
    assert caisse.charger(operation, db_path=db)["montant"] == 175.0


def test_un_brouillon_s_abandonne_sans_contrepassation(operation, db):
    res = caisse.annuler(operation, db_path=db)
    assert res["ok"] is True and res["statut"] == caisse.ST_ANNULEE


# ── La validation EST la comptabilisation ───────────────────────────────────────────────────────

def test_valider_genere_une_ecriture_equilibree(operation, db):
    res = caisse.valider(operation, acteur="test", db_path=db)
    assert res["ok"] is True and res["statut"] == caisse.ST_VALIDE

    conn = get_db(db)
    try:
        e = conn.execute(
            "SELECT * FROM ecritures WHERE journal='CAISSE' AND origine_id_opaque=?",
            (operation,)).fetchone()
    finally:
        conn.close()
    assert e is not None, "une opération validée porte TOUJOURS son écriture"
    assert e["total_debit"] == e["total_credit"] == 150.0


def test_l_encaissement_debite_la_caisse(operation, db):
    caisse.valider(operation, db_path=db)
    conn = get_db(db)
    try:
        lignes = conn.execute(
            "SELECT compte, debit, credit FROM ecriture_lignes el "
            "JOIN ecritures e ON e.ecriture_id_opaque = el.ecriture_id_opaque "
            "WHERE e.origine_id_opaque=? ORDER BY el.ligne_num", (operation,)).fetchall()
    finally:
        conn.close()
    caisse_ligne = next(l for l in lignes if l["compte"] == compta.COMPTE_CAISSE)
    assert caisse_ligne["debit"] == 150.0, "de l'argent ENTRE en caisse : elle est débitée"
    assert round(sum(l["debit"] for l in lignes), 2) == \
        round(sum(l["credit"] for l in lignes), 2)


def test_valider_deux_fois_ne_double_pas_l_ecriture(operation, db):
    caisse.valider(operation, db_path=db)
    deux = caisse.valider(operation, db_path=db)
    assert deux["ok"] is True and deux.get("inchange") is True

    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                         (operation,)).fetchone()["c"]
    finally:
        conn.close()
    assert n == 1


def test_une_ecriture_refusee_laisse_l_operation_au_brouillon(db, monkeypatch):
    """Si l'écriture ne passe pas, la validation ne passe pas non plus — jamais l'un sans l'autre."""
    op = caisse.creer("ENCAISSEMENT", 90.0, date_operation="2026-09-10",
                      tiers_type="PROPRIETAIRE", tiers_id="PROP_0001", db_path=db)["operation_id_opaque"]
    monkeypatch.setattr(compta, "generer_ecriture_caisse_operation",
                        lambda *a, **k: {"ok": False, "code": "E_PERIODE_CLOTUREE",
                                         "message": "période fermée"})
    res = caisse.valider(op, db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_ECRITURE_REFUSEE
    assert caisse.charger(op, db_path=db)["statut"] == caisse.ST_BROUILLON


# ── Une opération validée est immuable ──────────────────────────────────────────────────────────

def test_une_operation_validee_n_est_plus_modifiable(operation, db):
    caisse.valider(operation, db_path=db)
    res = caisse.modifier(operation, montant=999.0, db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_PAS_UN_BROUILLON
    assert caisse.charger(operation, db_path=db)["montant"] == 150.0


def test_une_operation_validee_ne_s_annule_pas(operation, db):
    """C'EST LE CŒUR DE LA CORRECTION : ce geste retournait le statut sans toucher à l'écriture."""
    caisse.valider(operation, db_path=db)
    res = caisse.annuler(operation, db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_PAS_UN_BROUILLON
    assert "contrepassation" in res["detail"]
    assert caisse.charger(operation, db_path=db)["statut"] == caisse.ST_VALIDE


# ── La contrepassation ──────────────────────────────────────────────────────────────────────────

def test_contrepasser_exige_un_motif(operation, db):
    caisse.valider(operation, db_path=db)
    assert caisse.contrepasser(operation, motif="  ", db_path=db)["code"] == \
        caisse.E_MOTIF_OBLIGATOIRE


def test_contrepasser_neutralise_le_mouvement_sans_l_effacer(operation, db):
    caisse.valider(operation, db_path=db)
    avant = _lignes_caisse(db)
    assert avant["d"] == 150.0

    res = caisse.contrepasser(operation, motif="Erreur de saisie sur le reçu 42",
                              acteur="test", db_path=db)
    assert res["ok"] is True and res["statut"] == caisse.ST_CONTREPASSEE

    apres = _lignes_caisse(db)
    # Les deux mouvements restent LISIBLES ; c'est leur somme qui est nulle.
    assert apres["d"] == 150.0 and apres["c"] == 150.0
    assert round(apres["d"] - apres["c"], 2) == 0.0


def test_l_ecriture_d_origine_est_marquee_contrepassee(operation, db):
    caisse.valider(operation, db_path=db)
    caisse.contrepasser(operation, motif="erreur", db_path=db)
    conn = get_db(db)
    try:
        statuts = [r["statut"] for r in conn.execute(
            "SELECT statut FROM ecritures WHERE journal='CAISSE' ORDER BY id").fetchall()]
        lien = conn.execute(
            "SELECT contrepasse_de FROM ecritures WHERE contrepasse_de IS NOT NULL").fetchone()
    finally:
        conn.close()
    assert compta.ST_CONTREPASSEE in statuts, "l'écriture d'origine porte son annulation"
    assert lien is not None, "le miroir dit ce qu'il contrepasse"


def test_on_ne_contrepasse_pas_deux_fois(operation, db):
    caisse.valider(operation, db_path=db)
    caisse.contrepasser(operation, motif="erreur", db_path=db)
    res = caisse.contrepasser(operation, motif="encore", db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_DEJA_CONTREPASSEE


def test_un_brouillon_ne_se_contrepasse_pas(operation, db):
    res = caisse.contrepasser(operation, motif="rien à annuler", db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_PAS_VALIDE


# ── Le solde vient de la comptabilité, pas d'un second calcul ───────────────────────────────────

def test_le_solde_est_celui_du_compte_comptable(operation, db):
    """Un second calcul depuis `operations_caisse` finirait par diverger, sans qu'on sache lequel
    croire."""
    etat = caisse.solde(db_path=db)
    assert etat["compte"] == compta.COMPTE_CAISSE
    assert etat["nb_brouillons"] == 1


def test_le_solde_dit_ce_qui_attend_la_validation_comptable(operation, db):
    """« Solde 0,00 € » juste après avoir validé 150 € est exact, et incompréhensible.

    `solde_compte` ne compte que les écritures POSTÉES ; une écriture fraîchement générée est
    PROPOSEE. Le second chiffre lève l'ambiguïté au lieu de laisser l'utilisateur conclure que son
    encaissement s'est perdu.
    """
    caisse.valider(operation, db_path=db)
    etat = caisse.solde(db_path=db)
    assert etat["solde"] == 0.0, "rien n'est posté tant que le comptable n'a pas validé"
    assert etat["en_attente_de_validation_comptable"] == 150.0
    assert etat["nb_brouillons"] == 0

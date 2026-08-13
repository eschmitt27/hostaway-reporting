"""Factures propriétaires émises : cycle de vie complet, sur base et fixtures synthétiques.

Aucune identité réelle, aucun montant réel, aucun document réel. La base utilisée est créée par
les migrations dans un répertoire temporaire.
"""
import hashlib
import json
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie Fixture", "adresse": "1 rue de Test, 00000 Ville",
            "siret": "00000000000000"}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test, 00000 Ville"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


def source(**kw):
    base = {
        "mois": "2026-06",
        "proprietaire_id": "PROP_FIXT_1",
        "logement_id": "LOG_FIXT_1",
        "source_calcul": "PREF-2026-06-PROP_FIXT_1-LOG_FIXT_1-001",
        "COMMISSION_CONCIERGERIE": 300.0,
        "MENAGE_FACTURE": 150.0,
        "CHARGE_FIXE": 50.0,
        "montant_du_conciergerie": 500.0,
        # Éléments de relevé : ne doivent jamais devenir des lignes facturées.
        "TOTAL_PAYOUT": 2000.0,
        "REVENU_NET_EXPLOITATION": 1500.0,
        "RESTE_A_PAYER": 500.0,
    }
    base.update(kw)
    return base


# ── Prévisualisation : relevé vs facture ────────────────────────────────────────────────────────

def test_previsualisation_ne_facture_que_les_elements_facturables(db):
    ap = svc.previsualiser(source())
    types = [l["type_ligne"] for l in ap["lignes"]]
    assert types == ["COMMISSION_CONCIERGERIE", "MENAGE_FACTURE", "CHARGE_FIXE"]
    for interdit in svc.TYPES_NON_FACTURABLES:
        assert interdit not in types, f"{interdit} ne doit jamais etre une ligne facturee"
    assert ap["montant_total"] == 500.0
    assert ap["statut_proposition"] == "PRETE"
    # Le payout reste visible comme information de relevé, hors facture.
    assert ap["releve_informatif"]["TOTAL_PAYOUT"] == 2000.0


def test_total_facture_reconcilie_le_montant_du(db):
    ap = svc.previsualiser(source(montant_du_conciergerie=999.0))
    assert ap["statut_proposition"] == "A_CONTROLER"
    assert any(c["code"] == svc.C_TOTAL_INCOHERENT for c in ap["controles"])


def test_ligne_a_zero_absente(db):
    ap = svc.previsualiser(source(PREPARATION_CANAPE=0.0))
    assert "PREPARATION_CANAPE" not in [l["type_ligne"] for l in ap["lignes"]]


def test_canape_facture_quand_present(db):
    ap = svc.previsualiser(source(PREPARATION_CANAPE=40.0, montant_du_conciergerie=540.0))
    assert "PREPARATION_CANAPE" in [l["type_ligne"] for l in ap["lignes"]]
    assert ap["montant_total"] == 540.0


def test_refacturation_facturee(db):
    ap = svc.previsualiser(source(CHARGES_EXCEPT_REFAC=75.0, montant_du_conciergerie=575.0))
    assert ap["montant_total"] == 575.0


# ── Création et anti-doublon ────────────────────────────────────────────────────────────────────

def test_creation_brouillon(db):
    f = svc.creer(source(), db_path=db)
    assert f["statut"] == svc.ST_BROUILLON
    assert f["numero_facture"] is None      # pas de numéro avant émission
    assert len(f["lignes"]) == 3
    assert f["montant_total"] == 500.0
    assert f["evenements"][0]["type_evenement"] == "CREATION"


def test_anti_doublon_meme_grain(db):
    svc.creer(source(), db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match=svc.C_DOUBLON):
        svc.creer(source(), db_path=db)


def test_grains_differents_autorises(db):
    svc.creer(source(), db_path=db)
    svc.creer(source(logement_id="LOG_FIXT_2"), db_path=db)
    svc.creer(source(mois="2026-07"), db_path=db)
    assert len(svc.lister(db_path=db)) == 3


def test_annulation_libere_le_grain(db):
    f = svc.creer(source(), db_path=db)
    svc.annuler(f["facture_id_opaque"], motif="erreur de saisie", db_path=db)
    f2 = svc.creer(source(), db_path=db)
    assert f2["facture_id_opaque"] != f["facture_id_opaque"]
    assert len(svc.lister(db_path=db)) == 2      # l'annulée reste dans l'historique


# ── Validation ──────────────────────────────────────────────────────────────────────────────────

def test_validation_refuse_emetteur_incomplet(db):
    f = svc.creer(source(), db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match=svc.C_IDENTITE):
        svc.valider(f["facture_id_opaque"], emetteur={"nom": "X", "adresse": "", "siret": ""},
                    destinataire=DESTINATAIRE, db_path=db)
    assert svc.lire(f["facture_id_opaque"], db_path=db)["statut"] == svc.ST_BROUILLON


def test_validation_ok(db):
    f = svc.creer(source(), db_path=db)
    v = svc.valider(f["facture_id_opaque"], emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                    db_path=db)
    assert v["statut"] == svc.ST_VALIDE


def test_emission_refusee_depuis_brouillon(db):
    f = svc.creer(source(), db_path=db)
    with pytest.raises(svc.FactureProprietaireError):
        svc.emettre(f["facture_id_opaque"], emetteur=EMETTEUR, destinataire=DESTINATAIRE,
                    serie="RECETTE-2026", date_facture="2026-07-01", db_path=db)


# ── Numérotation ────────────────────────────────────────────────────────────────────────────────

def _emettre(fid, db, repertoire=None, serie="RECETTE-2026"):
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)
    gen = pdf.fabrique(repertoire) if repertoire else None
    return svc.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, serie=serie,
                       date_facture="2026-07-01", generer_pdf=gen, db_path=db)


def test_numeros_uniques_et_sequentiels(db):
    numeros = []
    for i in range(3):
        f = svc.creer(source(logement_id=f"LOG_FIXT_{i}"), db_path=db)
        numeros.append(_emettre(f["facture_id_opaque"], db)["numero_facture"])
    assert numeros == ["RECETTE-2026-00001", "RECETTE-2026-00002", "RECETTE-2026-00003"]
    assert len(set(numeros)) == 3


def test_numero_jamais_reutilise_apres_avoir(db):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db)
    avoir = svc.creer_avoir(emise["facture_id_opaque"], motif="erreur", db_path=db)
    num_avoir = _emettre(avoir["facture_id_opaque"], db)["numero_facture"]
    assert num_avoir != emise["numero_facture"]


# ── Snapshot : le test critique ─────────────────────────────────────────────────────────────────

def test_facture_emise_immutable_apres_changement_des_sources(db, tmp_path):
    rep = tmp_path / "factures"
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=rep)

    snap_avant = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    hash_avant = emise["document_hash"]

    # Les sources changent : nouveau calcul, nouveaux montants, autre propriétaire.
    _ = svc.previsualiser(source(COMMISSION_CONCIERGERIE=9999.0, proprietaire_id="AUTRE"))

    relue = svc.lire(emise["facture_id_opaque"], db_path=db)
    snap_apres = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)

    assert relue["statut"] == svc.ST_EMIS
    assert relue["montant_total"] == 500.0
    assert relue["document_hash"] == hash_avant
    assert snap_apres == snap_avant
    assert [l["montant"] for l in snap_apres["lignes"]] == [300.0, 150.0, 50.0]


def test_pdf_deterministe_meme_snapshot_meme_hash(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f1")
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    a = hashlib.sha256(pdf.rendre(snap)).hexdigest()
    b = hashlib.sha256(pdf.rendre(snap)).hexdigest()
    assert a == b == emise["document_hash"]


def test_snapshot_altere_detecte(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    import sqlite3
    conn = sqlite3.connect(db)
    snap = json.loads(emise["snapshot_json"])
    snap["montant_total"] = 1.0
    conn.execute("UPDATE factures_proprietaires SET snapshot_json=? WHERE facture_id_opaque=?",
                 (json.dumps(snap, sort_keys=True, ensure_ascii=False),
                  emise["facture_id_opaque"]))
    conn.commit()
    conn.close()
    with pytest.raises(svc.FactureProprietaireError, match=svc.C_EMISE_MODIFIEE):
        svc.contenu_emis(emise["facture_id_opaque"], db_path=db)


# ── PDF ─────────────────────────────────────────────────────────────────────────────────────────

def test_pdf_ecrit_et_retrouvable(db, tmp_path):
    rep = tmp_path / "factures"
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=rep)

    chemin = rep / "2026" / "06" / emise["document_nom"]
    assert chemin.exists()
    octets = chemin.read_bytes()
    assert octets.startswith(b"%PDF")
    assert hashlib.sha256(octets).hexdigest() == emise["document_hash"]
    assert "/" not in emise["document_nom"] and "\\" not in emise["document_nom"]


def test_pdf_sans_tva(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    snap = svc.contenu_emis(emise["facture_id_opaque"], db_path=db)
    assert snap["regime_tva"].startswith("NON_ASSUJETTI")
    assert "montant_tva" not in snap
    for l in snap["lignes"]:
        assert "tva" not in json.dumps(l).lower()


# ── Avoir ───────────────────────────────────────────────────────────────────────────────────────

def test_avoir_inverse_sans_toucher_l_originale(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    avoir = svc.creer_avoir(emise["facture_id_opaque"], motif="montant errone", db_path=db)

    assert avoir["type_document"] == svc.TYPE_AVOIR
    assert avoir["facture_origine"] == emise["facture_id_opaque"]
    assert avoir["montant_total"] == -500.0
    assert [l["montant"] for l in avoir["lignes"]] == [-300.0, -150.0, -50.0]

    originale = svc.lire(emise["facture_id_opaque"], db_path=db)
    assert originale["statut"] == svc.ST_EMIS
    assert originale["montant_total"] == 500.0
    assert any(e["type_evenement"] == "AVOIR_CREE" for e in originale["evenements"])


def test_facture_emise_non_annulable_en_place(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    with pytest.raises(svc.FactureProprietaireError, match="avoir"):
        svc.annuler(emise["facture_id_opaque"], motif="erreur", db_path=db)


def test_un_seul_avoir_par_facture(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    svc.creer_avoir(emise["facture_id_opaque"], motif="a", db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match=svc.C_DOUBLON):
        svc.creer_avoir(emise["facture_id_opaque"], motif="b", db_path=db)


# ── Règlement ───────────────────────────────────────────────────────────────────────────────────

def test_solde_derive(db):
    f = svc.creer(source(), db_path=db)
    fid = f["facture_id_opaque"]
    assert svc.solde(fid, paiements_imputes=0, db_path=db)["statut_reglement"] == "NON_REGLEE"
    partiel = svc.solde(fid, paiements_imputes=200, db_path=db)
    assert partiel["solde"] == 300.0
    assert partiel["statut_reglement"] == "PARTIELLEMENT_REGLEE"
    total = svc.solde(fid, paiements_imputes=500, db_path=db)
    assert total["solde"] == 0.0
    assert total["statut_reglement"] == "REGLEE"
    trop = svc.solde(fid, paiements_imputes=600, db_path=db)
    assert trop["statut_reglement"] == "TROP_PERCU_A_CONTROLER"


def test_reglement_ne_modifie_pas_la_facture(db, tmp_path):
    """Le règlement impute une créance existante : il ne recrée ni commission, ni charge,
    ni facture, et ne touche pas le montant facturé."""
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    avant = svc.lire(emise["facture_id_opaque"], db_path=db)
    svc.solde(emise["facture_id_opaque"], paiements_imputes=500, db_path=db)
    apres = svc.lire(emise["facture_id_opaque"], db_path=db)
    assert avant["montant_total"] == apres["montant_total"]
    assert avant["version"] == apres["version"]
    assert avant["document_hash"] == apres["document_hash"]


# ── Journal ─────────────────────────────────────────────────────────────────────────────────────

def test_journal_append_only(db, tmp_path):
    f = svc.creer(source(), db_path=db)
    emise = _emettre(f["facture_id_opaque"], db, repertoire=tmp_path / "f")
    types = [e["type_evenement"] for e in emise["evenements"]]
    assert types == ["CREATION", "VALIDATION", "EMISSION"]
    svc.creer_avoir(emise["facture_id_opaque"], motif="x", db_path=db)
    apres = svc.lire(emise["facture_id_opaque"], db_path=db)
    assert [e["type_evenement"] for e in apres["evenements"]] == types + ["AVOIR_CREE"]

"""§28/§29/§30/§35 — corriger une facture fournisseur sans jamais mentir sur la pièce reçue.

Le cycle complet, tel qu'il a été exercé sur les deux factures PDF réelles :

    lignes extraites  →  Σ lignes ≠ total document  →  validation REFUSÉE
                      →  correction TRACÉE (ligne manquante, ou extraction incorrecte)
                      →  Σ lignes = total document  →  validation acceptée
                      →  écriture ACHATS + dette fournisseur, une seule fois

Ce que ces tests interdisent explicitement : valider avec un écart, corriger sans motif,
supprimer une ligne du document, et compter deux fois la même dépense.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Base migrée avec les verrous d'écriture ouverts — comme `test_comptabilite_controles`."""
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


@pytest.fixture
def facture_avec_ecart(db):
    """Facture de 100 € dont les lignes extraites n'en totalisent que 80 : il en manque une."""
    frs = frs_svc.creer("Prestataire Test", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    f = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "F-ECART-1",
                    "date_facture": "2026-07-31", "montant_ttc": 100.0}, db_path=db)
    opaque = f["facture_id_opaque"]
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=50.0,
                      logement_id="LOG_0001", description="T2 (extrait du PDF)",
                      quantite=1, prix_unitaire=50.0, db_path=db)
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=30.0,
                      logement_id="LOG_0002", description="Studio (extrait du PDF)",
                      quantite=1, prix_unitaire=30.0, db_path=db)
    return opaque


def test_les_lignes_pdf_sont_les_lignes_de_la_facture(facture_avec_ecart, db):
    """§21 — l'extracteur et la facture ne sont plus deux mondes séparés."""
    charge = fact.charger(facture_avec_ecart, db)
    assert len(charge["lignes"]) == 2
    assert charge["montant_lignes_ttc"] == 80.0
    assert {l["description"] for l in charge["lignes"]} == {
        "T2 (extrait du PDF)", "Studio (extrait du PDF)"}
    assert all(l["origine_ligne"] == "PDF" for l in charge["lignes"])


def test_validation_refusee_tant_que_les_lignes_ne_bouclent_pas(facture_avec_ecart, db):
    """§28 — aucune tolérance, aucun « forcer valide »."""
    res = fact.changer_statut(facture_avec_ecart, fact.ST_VALIDEE, db_path=db)
    assert res["ok"] is False
    assert res["code"] == fact.E_ECART_LIGNES_TOTAL
    assert "80.00" in res["detail"] and "100.00" in res["detail"]

    conn = get_db(db)
    try:
        statut = conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                              (facture_avec_ecart,)).fetchone()["statut"]
    finally:
        conn.close()
    assert statut == fact.ST_A_CONTROLER


def test_ligne_manquante_exige_un_motif(facture_avec_ecart, db):
    """§29 — sans motif, rien n'est enregistré : ce n'est pas un « ajouter une ligne » banal."""
    res = flm.ajouter_ligne_manquante(facture_avec_ecart, motif="   ", montant_ttc=20.0,
                                      description="x", db_path=db)
    assert res["ok"] is False and res["code"] == "MOTIF_OBLIGATOIRE"
    assert fact.charger(facture_avec_ecart, db)["montant_lignes_ttc"] == 80.0


def test_ligne_manquante_debloque_la_validation_et_cree_la_dette(facture_avec_ecart, db):
    """§29 puis §36/§77 — la correction tracée rétablit l'égalité, la validation crée la dette."""
    ajout = flm.ajouter_ligne_manquante(
        facture_avec_ecart, motif="Ligne absente du texte extrait, relue sur le PDF",
        description="T3 (ligne omise par le parseur)", montant_ttc=20.0,
        logement_id="LOG_0003", quantite=1, prix_unitaire=20.0, acteur="test", db_path=db)
    assert ajout["ok"] is True

    charge = fact.charger(facture_avec_ecart, db)
    assert charge["montant_lignes_ttc"] == 100.0
    corrective = [l for l in charge["lignes"] if l["origine_ligne"] == "CORRECTIVE"]
    assert len(corrective) == 1
    assert corrective[0]["motif_correction"] == "Ligne absente du texte extrait, relue sur le PDF"

    res = fact.changer_statut(facture_avec_ecart, fact.ST_VALIDEE, db_path=db)
    assert res["ok"] is True
    assert res["ecriture_achat"]["ok"] is True

    conn = get_db(db)
    try:
        lignes = conn.execute(
            "SELECT compte, debit, credit FROM ecriture_lignes el "
            "JOIN ecritures e ON e.ecriture_id_opaque = el.ecriture_id_opaque "
            "WHERE e.origine_id_opaque=? ORDER BY el.id", (facture_avec_ecart,)).fetchall()
    finally:
        conn.close()
    assert round(sum(l["debit"] for l in lignes), 2) == 100.0
    assert round(sum(l["credit"] for l in lignes), 2) == 100.0
    assert any(l["compte"] == "401000" and l["credit"] == 100.0 for l in lignes)


def test_extraction_incorrecte_ecarte_sans_supprimer(facture_avec_ecart, db):
    """§30 — la donnée brute reste lisible ; seule sa prise en compte cesse."""
    charge = fact.charger(facture_avec_ecart, db)
    ligne = next(l for l in charge["lignes"] if l["montant_ttc"] == 30.0)

    sans_motif = flm.marquer_extraction_incorrecte(ligne["ligne_id_opaque"], motif="", db_path=db)
    assert sans_motif["ok"] is False and sans_motif["code"] == "MOTIF_OBLIGATOIRE"

    res = flm.marquer_extraction_incorrecte(
        ligne["ligne_id_opaque"], motif="Montant mal lu : 30 au lieu de 3,00",
        acteur="test", db_path=db)
    assert res["ok"] is True

    apres = fact.charger(facture_avec_ecart, db)
    # La ligne EXISTE toujours, avec son libellé et son montant d'origine…
    ecartee = next(l for l in apres["lignes"] if l["ligne_id_opaque"] == ligne["ligne_id_opaque"])
    assert ecartee["description"] == "Studio (extrait du PDF)"
    assert ecartee["montant_ttc"] == 30.0
    assert ecartee["neutralisee"] is True
    assert ecartee["motif_correction"] == "Montant mal lu : 30 au lieu de 3,00"
    # … mais elle ne compte plus dans le total.
    assert apres["montant_lignes_ttc"] == 50.0


def test_pas_de_double_comptage_au_rerun(facture_avec_ecart, db):
    """§35 — une ligne validée = une seule dépense économique, même après plusieurs passages."""
    from app.services import comptabilite_ecritures_service as compta

    flm.ajouter_ligne_manquante(facture_avec_ecart, motif="ligne omise",
                                description="T3", montant_ttc=20.0, logement_id="LOG_0003",
                                db_path=db)
    fact.changer_statut(facture_avec_ecart, fact.ST_VALIDEE, db_path=db)
    fact.changer_statut(facture_avec_ecart, fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(facture_avec_ecart, db_path=db)

    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                         (facture_avec_ecart,)).fetchone()["c"]
    finally:
        conn.close()
    assert n == 1


# ── Rouvrir le contrôle d'une facture validée (arbitrage utilisateur 2026-09-12) ────────────────

def test_rouvrir_controle_facture_sans_consequence(facture_avec_ecart, db):
    """Une facture validée à tort doit pouvoir revenir à contrôler — motif obligatoire."""
    flm.ajouter_ligne_manquante(facture_avec_ecart, motif="ligne omise", description="T3",
                                montant_ttc=20.0, logement_id="LOG_0003", db_path=db)
    fact.changer_statut(facture_avec_ecart, fact.ST_VALIDEE, db_path=db)

    # Une écriture d'achat a été générée : le retour n'est PLUS réversible.
    etat = fact.consequences_constatees(facture_avec_ecart, db)
    assert etat["reversible"] is False
    refus = fact.rouvrir_controle(facture_avec_ecart, motif="je me suis trompé", db_path=db)
    assert refus["ok"] is False
    assert refus["code"] == fact.E_CONSEQUENCES_IRREVERSIBLES
    assert "contrepassation" in refus["detail"]


def test_rouvrir_controle_exige_un_motif(db):
    frs = frs_svc.creer("Presta", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    f = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "F-RO-1",
                    "date_facture": "2026-07-31", "montant_ttc": 10.0}, db_path=db)
    fact.changer_statut(f["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    conn = get_db(db)
    try:      # on retire l'écriture pour se placer dans le cas réversible
        conn.execute("DELETE FROM ecriture_lignes WHERE ecriture_id_opaque IN "
                     "(SELECT ecriture_id_opaque FROM ecritures WHERE origine_id_opaque=?)",
                     (f["facture_id_opaque"],))
        conn.execute("DELETE FROM ecritures WHERE origine_id_opaque=?", (f["facture_id_opaque"],))
        conn.commit()
    finally:
        conn.close()

    assert fact.rouvrir_controle(f["facture_id_opaque"], motif=" ", db_path=db)["code"] == \
        "MOTIF_OBLIGATOIRE"

    res = fact.rouvrir_controle(f["facture_id_opaque"],
                                motif="Incohérence lignes / total détectée", db_path=db)
    assert res["ok"] is True and res["statut"] == fact.ST_A_CONTROLER

    conn = get_db(db)
    try:
        evt = conn.execute(
            "SELECT type_evenement, ancien_statut, nouveau_statut, commentaire "
            "FROM facture_evenements WHERE facture_id_opaque=? AND type_evenement=?",
            (f["facture_id_opaque"], "RETOUR_A_CONTROLER")).fetchone()
    finally:
        conn.close()
    assert evt is not None
    assert evt["ancien_statut"] == "VALIDEE" and evt["nouveau_statut"] == "A_CONTROLER"
    assert "Incohérence" in evt["commentaire"]

"""§22 — cinquième bout-en-bout : une facture fournisseur MIXTE, ménages et frais.

LE CAS. Un prestataire facture dans un même document des ménages — rattachés chacun à un logement,
donc refacturables à un propriétaire — et des frais qui ne le sont pas : un déplacement, des
consommables, des heures supplémentaires sans logement identifiable.

CE QUE CE MÉLANGE MET EN DANGER, ET QUE CE TEST SURVEILLE

  · Les frais ne doivent PAS gonfler le coût ménage d'un logement. Une ligne « déplacement »
    comptée comme ménage se retrouverait refacturée à un propriétaire qui n'a rien demandé.
  · Ils ne doivent PAS disparaître non plus : le total de la facture les comprend, et la dette
    envers le prestataire aussi. Les oublier ferait une facture qui ne boucle pas.
  · La ventilation d'un frais non affecté se répartit sur les logements RÉELLEMENT présents dans
    CETTE facture, au prorata de leur coût de ménages — jamais sur l'historique du prestataire.
  · Et tout cela une seule fois : rejouer la chaîne ne doit rien doubler.

Les quatre bouts-en-bout précédents portaient sur des factures homogènes. Celui-ci est le seul où
les deux natures cohabitent, et c'est précisément là que les erreurs de typage coûtent de l'argent.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import facture_lignes_menage_service as flm
from app.services import facture_ventilation_menage_service as vent
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                 "COMPTABILITE_REAL_WRITE_ENABLED",
                 "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True)
    conn = get_db(p)
    try:
        for lid, nom in (("LOG_0001", "Studio - 46"), ("LOG_0002", "T3 - Cyprien")):
            conn.execute(
                "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
                "adresse, ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
                (lid, nom, nom, "", "TOULOUSE", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return p


#: La facture : 3 ménages rattachés + 2 frais qui ne le sont pas. Total document = 340,00 €.
MENAGES = [
    ("LOG_0001", "Studio - 46 — ménage du 03", 2, 45.0, 90.0),
    ("LOG_0001", "Studio - 46 — ménage du 17", 1, 45.0, 45.0),
    ("LOG_0002", "T3 Cyprien — ménage du 11", 3, 55.0, 165.0),
]
FRAIS = [
    ("Déplacement (forfait)", 25.0),
    ("Produits d'entretien", 15.0),
]
TOTAL_MENAGES = 300.0
TOTAL_FRAIS = 40.0
TOTAL_DOCUMENT = 340.0


@pytest.fixture
def facture(db):
    fournisseur = frs.creer("Prestataire mixte", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    f = fact.creer({"fournisseur_id_opaque": fournisseur, "facture_ref": "MIX-001",
                    "date_facture": "2026-07-31", "montant_ttc": TOTAL_DOCUMENT}, db_path=db)
    opaque = f["facture_id_opaque"]
    for logement, libelle, quantite, pu, montant in MENAGES:
        flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=montant,
                          logement_id=logement, description=libelle, quantite=quantite,
                          prix_unitaire=pu, date_menage="2026-07-03", db_path=db)
    for libelle, montant in FRAIS:
        flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=montant,
                          description=libelle, db_path=db)
    return opaque


# ── 1. Les deux natures cohabitent sans se confondre ────────────────────────────────────────────

def test_la_facture_boucle_frais_compris(facture, db):
    """Les frais comptent dans le total du document : les oublier ferait un écart de 40,00 €."""
    controle = flm.controler_total(facture, db_path=db)
    assert controle["montant_lignes"] == TOTAL_DOCUMENT
    assert controle["coherent"] is True


def test_le_cout_menage_ignore_les_frais(facture, db):
    """C'est l'invariant central : un déplacement n'est pas un ménage, et ne se refacture pas."""
    cout = flm.cout_menages_par_logement(facture, db_path=db)
    assert cout == {"LOG_0001": 135.0, "LOG_0002": 165.0}
    assert round(sum(cout.values()), 2) == TOTAL_MENAGES, "les 40 € de frais n'y sont pas"


def test_les_frais_restent_visibles_et_typés(facture, db):
    lignes = flm.lignes(facture, db_path=db)
    frais = [l for l in lignes if l["type_ligne"] == flm.TYPE_FRAIS_NON_AFFECTE]
    assert len(frais) == 2
    assert round(sum(l["montant_ttc"] for l in frais), 2) == TOTAL_FRAIS
    assert all(l["logement_id"] is None for l in frais), "un frais n'a pas de logement"


def test_seuls_les_menages_alimentent_la_chaine_menage(facture, db):
    """`lignes_externes_pour_reader` alimente lot6c : les frais ne doivent pas y entrer."""
    externes = [l for l in flm.lignes_externes_pour_reader(db_path=db)
                if l["facture_id"] == "MIX-001"]
    assert len(externes) == 3, "3 ménages, et seulement eux"
    assert {l["logement_id"] for l in externes} == {"LOG_0001", "LOG_0002"}
    assert round(sum(l["montant_ligne_ttc"] for l in externes), 2) == TOTAL_MENAGES


# ── 2. La ventilation d'un frais suit les logements de CETTE facture ────────────────────────────

def test_le_frais_se_ventile_au_prorata_des_menages_de_la_facture(facture, db):
    """135/300 et 165/300 de 25,00 € — au centime, et sur ces deux logements seulement."""
    base = flm.cout_menages_par_logement(facture, db_path=db)
    resultat = vent.ventiler(25.0, base)
    parts = {p["logement_id"]: p["part_montant"] for p in resultat["parts"]}
    assert set(parts) == {"LOG_0001", "LOG_0002"}
    assert round(sum(parts.values()), 2) == 25.0, "la somme des parts égale le montant, au centime"
    assert parts["LOG_0002"] > parts["LOG_0001"], "le logement le plus nettoyé en porte davantage"


def test_la_ventilation_ne_regarde_pas_les_autres_factures(db, facture):
    """Un logement absent de CETTE facture ne reçoit rien, même s'il existe au référentiel."""
    base = flm.cout_menages_par_logement(facture, db_path=db)
    parts = {p["logement_id"] for p in vent.ventiler(40.0, base)["parts"]}
    assert "LOG_9999" not in parts
    assert parts == {"LOG_0001", "LOG_0002"}


def test_sans_menage_dans_la_facture_le_frais_ne_se_ventile_pas(db):
    """Aucune base exploitable : on ne répartit pas au hasard, on le dit."""
    fournisseur = frs.creer("Frais seuls", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    f = fact.creer({"fournisseur_id_opaque": fournisseur, "facture_ref": "FRAIS-ONLY",
                    "date_facture": "2026-07-31", "montant_ttc": 30.0}, db_path=db)["facture_id_opaque"]
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=30.0,
                      description="Déplacement", db_path=db)
    resultat = vent.ventiler(30.0, flm.cout_menages_par_logement(f, db_path=db))
    assert resultat["statut"] == vent.ST_NON_EFFECTUEE
    assert resultat["parts"] == []
    assert "Aucun logement" in resultat["message"]


# ── 3. La validation comptabilise le document ENTIER, une seule fois ────────────────────────────

def test_la_validation_comptabilise_menages_et_frais(facture, db):
    res = fact.changer_statut(facture, fact.ST_VALIDEE, db_path=db)
    assert res["ok"] is True, res
    assert res["ecriture_achat"]["ok"] is True

    conn = get_db(db)
    try:
        lignes = conn.execute(
            "SELECT compte, debit, credit FROM ecriture_lignes el "
            "JOIN ecritures e ON e.ecriture_id_opaque = el.ecriture_id_opaque "
            "WHERE e.origine_id_opaque=?", (facture,)).fetchall()
    finally:
        conn.close()
    assert round(sum(l["debit"] for l in lignes), 2) == TOTAL_DOCUMENT
    assert round(sum(l["credit"] for l in lignes), 2) == TOTAL_DOCUMENT
    assert any(l["compte"] == compta.COMPTE_FOURNISSEURS and l["credit"] == TOTAL_DOCUMENT
               for l in lignes), "le prestataire est dû du total, frais compris"


def test_rejouer_la_chaine_ne_double_rien(facture, db):
    """Le danger d'une facture mixte : compter deux fois ce qu'on a typé deux fois."""
    fact.changer_statut(facture, fact.ST_VALIDEE, db_path=db)
    fact.changer_statut(facture, fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(facture, db_path=db)

    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                         (facture,)).fetchone()["c"]
    finally:
        conn.close()
    assert n == 1
    assert flm.cout_menages_par_logement(facture, db_path=db) == \
        {"LOG_0001": 135.0, "LOG_0002": 165.0}, "le coût ménage n'a pas bougé non plus"


# ── 4. Requalifier une ligne déplace l'argent du bon côté ───────────────────────────────────────

def test_requalifier_un_frais_en_menage_l_ajoute_au_cout(facture, db):
    """§26 — le parseur se trompe : ce « déplacement » était un ménage du T3, avec son logement."""
    ligne = next(l for l in flm.lignes(facture, db_path=db)
                 if l["description"] == "Déplacement (forfait)")
    # Un ménage exige un logement : sans lui, la requalification est refusée.
    assert flm.marquer_menage(ligne["ligne_id_opaque"], menage=True, motif="test",
                              db_path=db)["code"] == "LOGEMENT_MANQUANT"

    flm.affecter_logement(ligne["ligne_id_opaque"], logement_id="LOG_0002", db_path=db)
    res = flm.marquer_menage(ligne["ligne_id_opaque"], menage=True,
                             motif="Relu sur le PDF : ménage du T3, pas un déplacement",
                             db_path=db)
    assert res["ok"] is True

    cout = flm.cout_menages_par_logement(facture, db_path=db)
    assert cout["LOG_0002"] == 190.0, "les 25 € rejoignent le coût du T3"
    assert round(sum(cout.values()), 2) == 325.0
    # Le TOTAL du document n'a pas bougé : on a déplacé, pas ajouté.
    assert flm.controler_total(facture, db_path=db)["montant_lignes"] == TOTAL_DOCUMENT


def test_requalifier_un_menage_en_frais_le_retire_du_cout(facture, db):
    ligne = next(l for l in flm.lignes(facture, db_path=db)
                 if l["description"] == "Studio - 46 — ménage du 17")
    res = flm.marquer_menage(ligne["ligne_id_opaque"], menage=False,
                             motif="Facturé par erreur : c'était une fourniture", db_path=db)
    assert res["ok"] is True
    cout = flm.cout_menages_par_logement(facture, db_path=db)
    assert cout["LOG_0001"] == 90.0, "les 45 € sortent du coût ménage"
    assert flm.controler_total(facture, db_path=db)["montant_lignes"] == TOTAL_DOCUMENT


# ── 5. Le diagnostic d'écart tient compte des deux natures ──────────────────────────────────────

def test_une_ligne_de_frais_incoherente_est_diagnostiquee_comme_telle(db):
    """§27 — un frais sans quantité n'est pas vérifiable ; un ménage mal lu, si.

    Le document porte 145,00 € : 90 (2 × 45) + 45 (1 × 45) + 10 de frais. Les lignes extraites en
    totalisent 160, parce que la deuxième porte 60 au lieu de 45. En retenant le calcul de chaque
    ligne, la facture retombe exactement sur son total — donc l'écart vient d'un montant mal lu, pas
    d'une ligne absente.
    """
    fournisseur = frs.creer("Mixte écart", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    f = fact.creer({"fournisseur_id_opaque": fournisseur, "facture_ref": "MIX-ECART",
                    "date_facture": "2026-07-31", "montant_ttc": 145.0},
                   db_path=db)["facture_id_opaque"]
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=90.0,
                      logement_id="LOG_0001", description="ménage", quantite=2,
                      prix_unitaire=45.0, db_path=db)
    # Ligne de ménage incohérente : 1 × 45 ≠ 60.
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_MENAGE_EXTERNE, montant_ttc=60.0,
                      logement_id="LOG_0002", description="ménage mal lu", quantite=1,
                      prix_unitaire=45.0, db_path=db)
    # Frais sans quantité : rien à recalculer, ce n'est pas une incohérence.
    flm.ajouter_ligne(f, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=10.0,
                      description="Déplacement", db_path=db)

    d = flm.diagnostic_ecart(f, db_path=db)
    assert d["diagnostic"] == flm.D_LIGNES_INCOHERENTES
    assert [l["description"] for l in d["lignes_incoherentes"]] == ["ménage mal lu"], \
        "le frais sans quantité n'est pas accusé"
    assert d["montant_lignes"] == 160.0, "les lignes extraites dépassent le document"
    assert d["somme_recalculee"] == 145.0, "en retenant 1 × 45, la facture tombe sur son total"

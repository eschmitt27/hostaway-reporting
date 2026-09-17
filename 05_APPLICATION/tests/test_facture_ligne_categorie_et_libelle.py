"""Libellé source immuable et nature de la prestation — recette utilisateur n°4, §8 à §13.

Ce que ces tests protègent :
  · le texte du document est conservé tel quel, et une correction humaine ne l'altère jamais ;
  · la nature de la prestation est rattachée au référentiel canonique `ref_types_lignes_menage`,
    pas à une seconde nomenclature ;
  · une remise en état compte pour un ménage, au coût RÉEL de la ligne — jamais au tarif standard ;
  · une prestation qui n'est pas un ménage ne se met pas à en compter un ;
  · une ligne au libellé incompris existe quand même, et attend un classement humain ;
  · une ligne facturée 0 € est enregistrée : c'est une information du document.
"""
from __future__ import annotations

import pytest

from app.db.connection import get_db
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact

import app.config as cfg


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)


#: La nomenclature vient du RÉFÉRENTIEL (REF_Setup), pas d'une migration : une base de test neuve
#: ne la porte donc pas. On la reproduit ici à l'identique de la base réelle, sans la dupliquer
#: dans le code de production — c'est bien le référentiel qui reste la source de vérité.
_TYPES_CANONIQUES = [
    ("TLM_001", "MENAGE_STANDARD", "OUI", "NON"),
    ("TLM_002", "REMISE_EN_ETAT", "OUI", "NON"),
    ("TLM_003", "FRAIS_DEPLACEMENT", "NON", "OUI"),
    ("TLM_004", "LINGE", "NON", "OUI"),
    ("TLM_005", "ACHAT_PRODUIT", "NON", "OUI"),
    ("TLM_006", "AUTRE", "NON", "A_DEFINIR"),
]


@pytest.fixture(autouse=True)
def referentiel_types_lignes(tmp_db):
    conn = get_db(tmp_db)
    try:
        for tid, libelle, compte, repartissable in _TYPES_CANONIQUES:
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,?,'OUI','OUI','IMP-TEST')",
                (tid, libelle, compte, repartissable))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def facture(tmp_db):
    return fact.creer(
        {"fournisseur_id_opaque": "FRS-TEST", "facture_ref": "CAT-001",
         "date_facture": "2026-08-31", "montant_ttc": 300.0},
        db_path=tmp_db)["facture_id_opaque"]


def _ligne(tmp_db, facture_id, **kw):
    params = {
        "type_ligne": flm.TYPE_MENAGE_EXTERNE, "logement_id": "LOG_A",
        "montant_ttc": 55.0, "description": "t3 310 muret",
        "libelle_source": "2. service de nettoyage T3 310 muret (David) x 1 passage le 12/08",
        "categorie": flm.CAT_MENAGE_STANDARD, "categorie_confiance": "CERTAIN",
        "source": flm.SOURCE_PDF, "db_path": tmp_db,
    }
    params.update(kw)
    return flm.ajouter_ligne(facture_id, **params)


def _lue(tmp_db, ligne_id):
    conn = get_db(tmp_db)
    try:
        return dict(conn.execute("SELECT * FROM facture_lignes_menage WHERE ligne_id_opaque=?",
                                 (ligne_id,)).fetchone())
    finally:
        conn.close()


# ── §8 : deux libellés, et un seul est modifiable ───────────────────────────────────────────────

def test_le_libelle_source_est_conserve_tel_quel(tmp_db, facture):
    r = _ligne(tmp_db, facture)
    assert r["ok"] is True
    ligne = _lue(tmp_db, r["ligne_id_opaque"])
    assert ligne["libelle_source"] == \
        "2. service de nettoyage T3 310 muret (David) x 1 passage le 12/08"
    assert ligne["description"] == "t3 310 muret", "le libellé métier reste la forme de travail"


def test_corriger_le_libelle_metier_n_altere_jamais_la_source(tmp_db, facture):
    ligne_id = _ligne(tmp_db, facture)["ligne_id_opaque"]
    source_avant = _lue(tmp_db, ligne_id)["libelle_source"]

    r = flm.corriger_classification(ligne_id, libelle_metier="T3 310 avenue de Muret",
                                    acteur="ewan", db_path=tmp_db)
    assert r["ok"] is True

    apres = _lue(tmp_db, ligne_id)
    assert apres["description"] == "T3 310 avenue de Muret"
    assert apres["libelle_source"] == source_avant
    assert "Contrôle ewan" in (apres["commentaire"] or ""), "la correction est tracée"


# ── §9 à §13 : la nature de la prestation ───────────────────────────────────────────────────────

def test_la_categorie_est_rattachee_au_referentiel_canonique(tmp_db, facture):
    ligne_id = _ligne(tmp_db, facture, categorie=flm.CAT_REMISE_EN_ETAT,
                      categorie_confiance="CERTAIN", montant_ttc=150.0)["ligne_id_opaque"]
    ligne = _lue(tmp_db, ligne_id)

    conn = get_db(tmp_db)
    try:
        ref = dict(conn.execute(
            "SELECT type_ligne_menage, compte_comme_menage FROM ref_types_lignes_menage "
            "WHERE type_ligne_menage_id=?", (ligne["type_ligne_menage_id"],)).fetchone())
    finally:
        conn.close()
    assert ref["type_ligne_menage"] == "REMISE_EN_ETAT"
    # §11 : une remise en état compte bien comme un ménage…
    assert ref["compte_comme_menage"] == "OUI"
    # …mais à son coût réel, jamais remplacé par un tarif standard.
    assert ligne["montant_ttc"] == pytest.approx(150.0)
    assert ligne["montant_ttc_source"] == pytest.approx(150.0)


def test_une_autre_prestation_ne_compte_pas_comme_un_menage(tmp_db, facture):
    ligne_id = _ligne(tmp_db, facture, categorie=flm.CAT_ACHAT_PRODUIT,
                      categorie_confiance="CERTAIN", montant_ttc=85.0,
                      description="frais de courses", logement_id="",
                      type_ligne=flm.TYPE_FRAIS_NON_AFFECTE)["ligne_id_opaque"]
    ligne = _lue(tmp_db, ligne_id)

    conn = get_db(tmp_db)
    try:
        compte = conn.execute(
            "SELECT compte_comme_menage FROM ref_types_lignes_menage WHERE type_ligne_menage_id=?",
            (ligne["type_ligne_menage_id"],)).fetchone()["compte_comme_menage"]
    finally:
        conn.close()
    assert compte == "NON"
    assert ligne["montant_ttc"] == pytest.approx(85.0), "le montant reste celui de la ligne"


def test_une_ligne_incomprise_existe_et_attend_un_classement_humain(tmp_db, facture):
    """« solde dû suite aux prestations du mois de juillet » : le logiciel ne tranche pas seul."""
    ligne_id = _ligne(tmp_db, facture, categorie=flm.CAT_AUTRE, categorie_confiance="AUCUN",
                      montant_ttc=50.0, description="solde dû", logement_id="",
                      type_ligne=flm.TYPE_FRAIS_NON_AFFECTE,
                      libelle_source="15. solde dû suite aux prestations du mois de juillet",
                      )["ligne_id_opaque"]
    ligne = _lue(tmp_db, ligne_id)
    assert ligne["type_ligne_menage_confiance"] == "AUCUN"
    assert ligne["libelle_source"].startswith("15.")

    # L'utilisateur tranche : la ligne devient une autre prestation, confirmée par un humain.
    flm.corriger_classification(ligne_id, categorie=flm.CAT_FRAIS_DEPLACEMENT, acteur="ewan",
                                db_path=tmp_db)
    apres = _lue(tmp_db, ligne_id)
    assert apres["type_ligne_menage_confiance"] == flm.CONFIANCE_CONFIRMEE
    assert apres["libelle_source"] == ligne["libelle_source"]


def test_une_categorie_hors_nomenclature_est_refusee(tmp_db, facture):
    ligne_id = _ligne(tmp_db, facture)["ligne_id_opaque"]
    r = flm.corriger_classification(ligne_id, categorie="MENAGE_PROFOND", db_path=tmp_db)
    assert r["ok"] is False and r["code"] == "CATEGORIE_INVALIDE"


def test_les_categories_proposees_viennent_du_referentiel(tmp_db):
    categories = flm.categories_disponibles(db_path=tmp_db)
    libelles = {c["type_ligne_menage"] for c in categories}
    assert {"MENAGE_STANDARD", "REMISE_EN_ETAT", "FRAIS_DEPLACEMENT", "LINGE", "ACHAT_PRODUIT",
            "AUTRE"} <= libelles
    comptent = {c["type_ligne_menage"] for c in categories if c["compte_comme_menage"] == "OUI"}
    assert comptent == {"MENAGE_STANDARD", "REMISE_EN_ETAT"}


# ── §7 : une ligne à 0 € est une information, pas une erreur ────────────────────────────────────

def test_une_ligne_facturee_zero_est_enregistree(tmp_db, facture):
    """« ce logement n'a eu aucun ménage ce mois-ci » : le prestataire prend la peine de l'écrire."""
    r = _ligne(tmp_db, facture, montant_ttc=0.0, description="t.2-65 (gabriel)",
               libelle_source="T.2-65 (Gabriel)")
    assert r["ok"] is True
    assert _lue(tmp_db, r["ligne_id_opaque"])["montant_ttc"] == pytest.approx(0.0)


def test_une_saisie_humaine_a_zero_reste_refusee(tmp_db, facture):
    """Un 0 lu sur une pièce est une donnée ; un 0 tapé à la main reste une erreur de saisie."""
    r = _ligne(tmp_db, facture, montant_ttc=0.0, source=flm.SOURCE_SAISIE)
    assert r["ok"] is False and r["code"] == "MONTANT_INVALIDE"


# ── §14 : le logement corrigé à la main est marqué comme tel ────────────────────────────────────

def test_le_logement_confirme_a_la_main_est_trace(tmp_db, facture):
    ligne_id = _ligne(tmp_db, facture, logement_id="", type_ligne=flm.TYPE_FRAIS_NON_AFFECTE,
                      )["ligne_id_opaque"]
    assert _lue(tmp_db, ligne_id)["type_ligne"] == flm.TYPE_FRAIS_NON_AFFECTE

    flm.corriger_classification(ligne_id, logement_id="LOG_0002", acteur="ewan", db_path=tmp_db)
    apres = _lue(tmp_db, ligne_id)
    assert apres["logement_id"] == "LOG_0002"
    assert apres["logement_confiance"] == flm.CONFIANCE_CONFIRMEE
    assert apres["type_ligne"] == flm.TYPE_MENAGE_EXTERNE
    # La proposition d'origine reste lisible : ici, aucune.
    assert apres["logement_id_source"] is None

"""E2E — Ménages consomme les lignes de la facture fournisseur, pas un second PDF re-lu.

Derniers correctifs recette 4 (lot 1), §7 à §13. Preuve, sur une base temporaire isolée et par de
VRAIS sous-processus lot6d/6e/6f (aucun mock du moteur) :

  · §7/§8 — une facture À CONTRÔLER alimente déjà le rapprochement (lot6d compte les ménages),
    sans créer d'écriture ACHATS ni de dette (réservé à VALIDÉE, lot6e/6f) ;
  · §6/§12 — un écart de 150 € disparaît réellement après l'ajout d'une ligne corrective de
    150 €, et la validation V11 s'ouvre ;
  · §4/§10 — une remise en état, comme un ménage standard, compte pour +1 dans le rapprochement ;
    une « autre prestation » compte pour 0 ;
  · §9/§11/§13 — ajouter, requalifier, déplacer ou neutraliser une ligne change IMMÉDIATEMENT
    (au recalcul suivant, sans double extraction PDF) ce que Ménages affiche, sans doublon ni
    ménage fantôme.
"""
from __future__ import annotations

import sqlite3

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact
from app.services import orchestrateur_moteur as om

MOIS = "2026-08"

#: La nomenclature vient du RÉFÉRENTIEL (REF_Setup), pas d'une migration : une base de test neuve
#: ne la porte donc pas. Reproduite ici à l'identique de la base réelle (TLM_001..TLM_006).
_TYPES_CANONIQUES = [
    ("TLM_001", "MENAGE_STANDARD", "OUI"), ("TLM_002", "REMISE_EN_ETAT", "OUI"),
    ("TLM_003", "FRAIS_DEPLACEMENT", "NON"), ("TLM_004", "LINGE", "NON"),
    ("TLM_005", "ACHAT_PRODUIT", "NON"), ("TLM_006", "AUTRE", "NON"),
]


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED", "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_path):
    """Référentiel minimal : un logement géré, son propriétaire, un tarif standard, et le
    référentiel intervenants sans lequel lot6d refuse de tourner."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, nom_normalise, import_id) "
            "VALUES ('INT1','Prestataire test','EXTERNE','OUI','PRESTATAIRETEST','IMP-T')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "nom_court, statut_parc, actif, import_id) "
            "VALUES ('LOG_A1','777001','STD','Logement test A','GERE','OUI','IMP-T')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "nom_court, statut_parc, actif, import_id) "
            "VALUES ('LOG_B1','777002','STD','Logement test B','GERE','OUI','IMP-T')")
        for lid in ("LOG_A1", "LOG_B1"):
            conn.execute(
                "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, "
                "proprietaire_id, date_debut, date_fin, statut_gestion, import_id) "
                f"VALUES ('GST-{lid}',?,?,'2025-01-01','','ACTIF','IMP-T')", (lid, f"PROP_{lid}"))
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, actif, import_id) "
            "VALUES ('CSM-1','STD',45.0,'2025-01-01','OUI','IMP-T')")
        for tid, libelle, compte in _TYPES_CANONIQUES:
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        conn.commit()
    finally:
        conn.close()
    return db_path


def _facture(db_path, *, ref, montant_ttc):
    return fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": ref, "date_facture": f"{MOIS}-31",
         "montant_ttc": montant_ttc}, db_path=db_path)["facture_id_opaque"]


def _rapprochement(db_path, logement_id):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT nb_menages_declares_externe FROM menages_rapprochement "
            "WHERE mois = ? AND logement_id = ?", (MOIS, logement_id)).fetchone()
        return row["nb_menages_declares_externe"] if row else 0
    finally:
        conn.close()


def _ecritures_achats(db_path, opaque):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT ecriture_id_opaque, statut FROM ecritures "
            "WHERE origine_id_opaque = ? AND journal = 'ACHATS'", (opaque,)).fetchall()
    finally:
        conn.close()


# ── §7/§8 : la facture À CONTRÔLER alimente déjà le rapprochement, sans ACHATS ni dette ─────────

def test_facture_a_controler_alimente_le_rapprochement_sans_achats(base):
    opaque = _facture(base, ref="E2E-8", montant_ttc=850.0)
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                      montant_ttc=850.0, description="menage aout", categorie=flm.CAT_MENAGE_STANDARD,
                      categorie_confiance="CERTAIN", source=flm.SOURCE_PDF, db_path=base)
    facture = fact.charger(opaque, db_path=base)
    assert facture["statut"] == fact.ST_A_CONTROLER

    resultat = om.executer_menages_cible(db_path=base, mois=MOIS)
    assert resultat["ok"] is True, resultat

    assert _rapprochement(base, "LOG_A1") == 1, \
        "une facture À CONTRÔLER doit déjà compter son ménage dans le rapprochement"
    assert _ecritures_achats(base, opaque) == [], \
        "aucune écriture ACHATS tant que la facture n'est pas validée"
    assert fact.consequences_constatees(opaque, db_path=base)["reversible"] is True


# ── §6/§12 : l'écart de 150 € disparaît réellement après l'ajout de la ligne corrective ─────────

def test_ajout_ligne_corrective_150_resorbe_l_ecart_et_alimente_menages(base):
    opaque = _facture(base, ref="E2E-150", montant_ttc=1000.0)
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                      montant_ttc=850.0, description="menage existant",
                      categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                      source=flm.SOURCE_PDF, db_path=base)
    assert flm.controler_total(opaque, db_path=base)["ecart"] == pytest.approx(150.0)
    refus = fact.changer_statut(opaque, fact.ST_VALIDEE, acteur="e2e", db_path=base)
    assert refus["ok"] is False and refus["code"] == fact.E_ECART_LIGNES_TOTAL

    ajout = flm.ajouter_ligne_manquante(
        opaque, motif="ligne omise par le parseur", montant_ttc=150.0,
        description="ligne manquante", categorie=flm.CAT_MENAGE_STANDARD, logement_id="LOG_A1",
        db_path=base)
    assert ajout["ok"] is True, ajout

    ligne = next(l for l in fact.lignes(opaque, db_path=base)
                if l["ligne_id_opaque"] == ajout["ligne_id_opaque"])
    assert ligne["est_menage"] is True, "colonne Ménage = Oui pour une ligne classée MÉNAGE"

    controle = flm.controler_total(opaque, db_path=base)
    assert controle["montant_lignes"] == pytest.approx(1000.0)
    assert controle["ecart"] == pytest.approx(0.0)
    facture = fact.charger(opaque, db_path=base)
    assert facture["montant_lignes_ttc"] == pytest.approx(1000.0), \
        "même somme à l'écran (charger) et au contrôle (controler_total) : UNE fonction canonique"

    # V11 disparaît : la facture reste À CONTRÔLER tant que rien d'autre ne bloque, mais peut être
    # validée.
    valide = fact.changer_statut(opaque, fact.ST_VALIDEE, acteur="e2e", db_path=base)
    assert valide["ok"] is True, valide

    resultat = om.executer_menages_cible(db_path=base, mois=MOIS)
    assert resultat["ok"] is True, resultat
    assert _rapprochement(base, "LOG_A1") == 2, "les deux lignes comptent, sans doublon"

    achats = _ecritures_achats(base, opaque)
    assert len(achats) == 1, "une seule dépense, jamais deux, même après une ligne ajoutée"


# ── §4/§10 : remise en état = +1 ménage ; autre prestation = +0 ─────────────────────────────────

def test_remise_en_etat_compte_un_menage_autre_prestation_compte_zero(base):
    opaque = _facture(base, ref="E2E-CAT", montant_ttc=239.0)
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                      montant_ttc=150.0, description="remise en etat",
                      categorie=flm.CAT_REMISE_EN_ETAT, categorie_confiance="CERTAIN",
                      source=flm.SOURCE_PDF, db_path=base)
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, logement_id="LOG_A1",
                      montant_ttc=89.0, description="frais de courses",
                      categorie=flm.CAT_ACHAT_PRODUIT, categorie_confiance="CERTAIN",
                      source=flm.SOURCE_PDF, db_path=base)

    lignes = fact.lignes(opaque, db_path=base)
    remise = next(l for l in lignes if l["categorie"] == flm.CAT_REMISE_EN_ETAT)
    achat = next(l for l in lignes if l["categorie"] == flm.CAT_ACHAT_PRODUIT)
    assert remise["est_menage"] is True and remise["montant_ttc"] == pytest.approx(150.0), \
        "coût réel, jamais le tarif standard (45 €)"
    assert achat["est_menage"] is False

    resultat = om.executer_menages_cible(db_path=base, mois=MOIS)
    assert resultat["ok"] is True, resultat
    assert _rapprochement(base, "LOG_A1") == 1, "la remise en état compte 1, l'achat compte 0"


# ── §13 : requalifications, en direct sur le rapprochement ──────────────────────────────────────

def test_requalifier_autre_en_menage_ajoute_un_menage(base):
    opaque = _facture(base, ref="E2E-REQ1", montant_ttc=55.0)
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, logement_id="LOG_A1",
                          montant_ttc=55.0, description="deplacement",
                          categorie=flm.CAT_FRAIS_DEPLACEMENT, categorie_confiance="CERTAIN",
                          source=flm.SOURCE_PDF, db_path=base)
    ligne_id = r["ligne_id_opaque"]

    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 0

    res = flm.corriger_classification(ligne_id, categorie=flm.CAT_MENAGE_STANDARD, acteur="ewan",
                                      db_path=base)
    assert res["ok"] is True
    ligne = next(l for l in fact.lignes(opaque, db_path=base) if l["ligne_id_opaque"] == ligne_id)
    assert ligne["est_menage"] is True

    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 1


def test_requalifier_menage_en_autre_retire_un_menage(base):
    opaque = _facture(base, ref="E2E-REQ2", montant_ttc=55.0)
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                          montant_ttc=55.0, description="menage",
                          categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                          source=flm.SOURCE_PDF, db_path=base)
    ligne_id = r["ligne_id_opaque"]
    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 1

    flm.corriger_classification(ligne_id, categorie=flm.CAT_AUTRE, acteur="ewan", db_path=base)
    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 0


def test_deplacer_le_logement_change_le_rapprochement_sans_doublon(base):
    opaque = _facture(base, ref="E2E-REQ3", montant_ttc=55.0)
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                          montant_ttc=55.0, description="menage",
                          categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                          source=flm.SOURCE_PDF, db_path=base)
    ligne_id = r["ligne_id_opaque"]
    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 1
    assert _rapprochement(base, "LOG_B1") == 0

    flm.affecter_logement(ligne_id, logement_id="LOG_B1", acteur="ewan", memoriser=False,
                          db_path=base)
    ligne = next(l for l in fact.lignes(opaque, db_path=base) if l["ligne_id_opaque"] == ligne_id)
    assert ligne["logement_id"] == "LOG_B1" and ligne["est_menage"] is True

    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 0, "disparaît de A"
    assert _rapprochement(base, "LOG_B1") == 1, "apparaît sur B"


# ── §11 : suppression/neutralisation avant validation se propage, sans fantôme ──────────────────

def test_neutraliser_une_ligne_la_retire_du_rapprochement(base):
    opaque = _facture(base, ref="E2E-NEUT", montant_ttc=55.0)
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A1",
                          montant_ttc=55.0, description="menage",
                          categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                          source=flm.SOURCE_PDF, db_path=base)
    ligne_id = r["ligne_id_opaque"]
    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 1

    res = flm.marquer_extraction_incorrecte(ligne_id, motif="doublon avec une autre ligne",
                                            acteur="ewan", db_path=base)
    assert res["ok"] is True

    om.executer_menages_cible(db_path=base, mois=MOIS)
    assert _rapprochement(base, "LOG_A1") == 0, "aucun ménage fantôme après neutralisation"

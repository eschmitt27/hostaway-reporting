"""Créances : RÉGLÉ / COMPENSÉ / SOLDE (recette n°2, §17, §18, §32, §56).

Le défaut corrigé ici s'est vu en recette : « Total 465,88 · Réglé 0 · Compensé 0 · Solde 40,88 ».
425 € agissaient sur le solde sans apparaître nulle part. L'invariant

    TOTAL − RÉGLÉ − COMPENSÉ = SOLDE

est donc testé comme une PROPRIÉTÉ, pas seulement sur un exemple : aucun montant ne peut plus
disparaître de l'explication.

Vocabulaire :
    RÉGLÉ    — argent reçu du propriétaire (acomptes, paiements) ;
    COMPENSÉ — somme déjà détenue pour son compte (reversements Airbnb), qui éteint la créance
               sans encaissement supplémentaire.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import compte_proprietaire_service as cpt
from app.services import creances_dettes_service as cd
from app.services import factures_proprietaires_service as fpr

EMETTEUR = {"nom": "CHOUETTE PATRIMOINE", "adresse": "48E Route de Larnavey, 33650 Saint-Selve",
            "siren": "109624767", "siret": ""}
DESTINATAIRE = {"nom": "Proprietaire Test", "adresse": "1 rue de Test"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)
    apply_migrations(chemin)
    return chemin


def _facture_emise(db, total=465.88, mois="2026-08"):
    """Facture ÉMISE d'un montant donné. Le montant est porté par une ligne de commission :
    c'est ainsi qu'une facture réelle est constituée."""
    source = {"mois": mois, "proprietaire_id": "PROP_T", "logement_id": "LOG_T",
              "source_calcul": f"PREF-{mois}", "COMMISSION_CONCIERGERIE": total,
              "montant_du_conciergerie": total}
    f = fpr.creer(source, acteur="t", db_path=db)
    fid = f["facture_id_opaque"]
    fpr.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, acteur="t", db_path=db)
    fpr.emettre(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, date_facture=f"{mois}-31",
                acteur="t", exiger_conformite=False, db_path=db)
    return fid


def _reversement(db, fid, montant, mois="2026-08"):
    """Reversement Airbnb imputé sur le document — la voie qu'a empruntée la donnée réelle."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO imputations_airbnb (imputation_airbnb_id, proprietaire_id, logement_id, "
            "mois, document_id, montant_impute, date_imputation, statut) "
            "VALUES (?,?,?,?,?,?,?,'VALIDE')",
            (f"IMPA-{abs(hash((fid, montant))) % 10**12:012d}", "PROP_T", "LOG_T", mois, fid,
             montant, f"{mois}-15"))
        conn.commit()
    finally:
        conn.close()


def _ligne(db, fid):
    return next(l for l in cd.creances(db_path=db) if l["facture_id_opaque"] == fid)


# ── L'invariant ─────────────────────────────────────────────────────────────────────────────────

def test_le_reversement_apparait_en_compense_et_non_dans_le_neant(db):
    """Le cas EXACT de la recette : 465,88 avec 425 de reversement."""
    fid = _facture_emise(db, 465.88)
    _reversement(db, fid, 425.0)
    l = _ligne(db, fid)
    assert l["total"] == 465.88
    assert l["regle"] == 0.0
    assert l["compense"] == 425.0, "le reversement doit être VISIBLE, pas seulement agir"
    assert l["solde"] == 40.88
    assert round(l["total"] - l["regle"] - l["compense"] - l["solde"], 2) == 0.0


@pytest.mark.parametrize("total,reversement", [
    (465.88, 425.0), (100.0, 0.0), (250.5, 250.5), (300.0, 500.0), (1234.56, 0.01),
])
def test_invariant_total_moins_regle_moins_compense_egale_solde(db, total, reversement):
    """Propriété, pas exemple : aucun montant ne peut disparaître de l'explication."""
    fid = _facture_emise(db, total)
    if reversement:
        _reversement(db, fid, reversement)
    l = _ligne(db, fid)
    assert round(l["total"] - l["regle"] - l["compense"] - l["solde"], 2) == 0.0


def test_acompte_compte_en_regle_et_reversement_en_compense(db):
    """§17 : les deux diminuent le reste à payer, mais ne sont pas la même chose économiquement.
    Les fondre dans une colonne unique effacerait cette différence."""
    fid = _facture_emise(db, 465.88)
    _reversement(db, fid, 325.0)
    from app.services import factures_proprietaires_edition_service as edition
    conn = get_db(db)
    try:
        conn.execute("INSERT OR IGNORE INTO ref_setup_imports (import_id, horodatage, "
                     "chemin_source, empreinte_source, statut, nb_feuilles, nb_lignes) "
                     "VALUES ('IMP-T','2026-08-01T00:00:00Z','f','0',"
                     "'IMPORTE',1,1)")
        conn.execute("INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                     "prenom_proprietaire, adresse_facturation, actif, import_id) "
                     "VALUES ('PROP_T','Test','P','1 rue','OUI','IMP-T')")
        conn.commit()
    finally:
        conn.close()
    edition.ajouter_acompte(fid, montant=100.0, date_mouvement="2026-08-31", acteur="t",
                            db_path=db)

    l = _ligne(db, fid)
    assert l["regle"] == 100.0, "un acompte est de l'argent REÇU"
    assert l["compense"] == 325.0, "un reversement est une COMPENSATION"
    assert l["solde"] == 40.88
    assert round(l["total"] - l["regle"] - l["compense"] - l["solde"], 2) == 0.0


# ── Net négatif (§18) ───────────────────────────────────────────────────────────────────────────

def test_trop_percu_se_nomme_a_reverser(db):
    """Un solde négatif n'est pas une facture négative : c'est une somme due AU propriétaire."""
    fid = _facture_emise(db, 465.88)
    _reversement(db, fid, 600.0)
    l = _ligne(db, fid)
    assert l["solde"] < 0
    assert l["sens"] == "A_REVERSER"
    assert l["montant_a_reverser"] == 134.12
    assert l["libelle_statut"] == "À reverser au propriétaire"


def test_facture_soldee(db):
    fid = _facture_emise(db, 250.0)
    _reversement(db, fid, 250.0)
    l = _ligne(db, fid)
    assert l["sens"] == "SOLDEE"
    assert l["libelle_statut"] == "Soldée"


def test_le_chiffre_d_affaires_reste_le_total_facture(db):
    """Reversements et acomptes ne diminuent NI le total facturé, NI le produit comptabilisé."""
    fid = _facture_emise(db, 465.88)
    _reversement(db, fid, 425.0)
    assert fpr.lire(fid, db_path=db)["montant_total"] == 465.88
    assert _ligne(db, fid)["total"] == 465.88


# ── Statuts lisibles (§33) ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("code,attendu", [
    (cd.ST_NON_REGLEE, "À régler"),
    (cd.ST_PARTIELLE, "Partiellement réglée"),
    (cd.ST_REGLEE, "Soldée"),
    (cd.ST_TROP_PERCU, "À reverser au propriétaire"),
])
def test_statuts_humanises(code, attendu):
    assert cd.libelle_statut(code) == attendu
    assert "_" not in cd.libelle_statut(code)


def test_le_retard_prime_sur_le_statut():
    """« En retard » appelle une action ; « À régler » ne la rend pas visible."""
    assert cd.libelle_statut(cd.ST_NON_REGLEE, jours_retard=12) == "En retard (12 j)"
    assert cd.badge_statut(cd.ST_NON_REGLEE, jours_retard=12) == "en-retard"
    # Une facture soldée en retard n'existe pas : elle est soldée.
    assert cd.libelle_statut(cd.ST_REGLEE, jours_retard=12) == "Soldée"


def test_imputations_detail_expose_le_detail_des_deux_sources(db):
    """`imputations_detail` doit lire les MÊMES sources que `solde()`, sinon l'écart réapparaît."""
    fid = _facture_emise(db, 465.88)
    _reversement(db, fid, 425.0)
    d = cpt.imputations_detail(fid, db_path=db)
    assert d["compense"] == 425.0
    assert d["reversements_airbnb"] == 425.0
    assert d["acomptes"] == 0.0
    assert d["total"] == 425.0

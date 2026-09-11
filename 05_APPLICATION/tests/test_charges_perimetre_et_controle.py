"""Charges : périmètre analytique, cycle de contrôle, refacturation (recette n°2, §54-§55).

Le cas de référence est celui de la recette : une charge de 700 € affectée à DEUX logements.
Elle doit peser 350 € sur le résultat de chacun (analytique) tout en restant refacturable pour
700 € au total (commercial). Les deux axes sont testés séparément, parce qu'ils sont séparés.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import charges_perimetre_service as perim
from app.services import charges_refacturation_service as refac
from app.services import charges_saisie_service as saisie


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


@pytest.fixture()
def ecritures_actives(monkeypatch):
    for flag in ("CHARGES_REAL_WRITE_ENABLED", "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)


def _charge(db, montant=700.0, refacturable="OUI", **kw):
    donnees = {"date_charge": "2026-08-09", "montant": montant, "categorie_charge_id": "CHG_008",
               "code_impact": "HC", "refacturable": refacturable,
               "commentaire": "Reparation serrurerie"}
    donnees.update(kw)
    return saisie.creer(donnees, acteur="test", db_path=db)["charge_id"]


PERIMETRE_2 = [
    {"logement_id": "LOG_A", "proprietaire_id": "PROP_A", "mois": "2026-08",
     "quote_part_montant": 350.0},
    {"logement_id": "LOG_B", "proprietaire_id": "PROP_B", "mois": "2026-08",
     "quote_part_montant": 350.0},
]


# ── Ventilation analytique (§54.13, §12) ────────────────────────────────────────────────────────

def test_700_sur_2_logements_donne_350_chacun(db, ecritures_actives):
    cid = _charge(db)
    saisie.creer({"date_charge": "2026-08-09", "montant": 0.01, "categorie_charge_id": "CHG_008",
                  "code_impact": "HC", "refacturable": "NON", "charge_id": "CHG-IGNOREE"},
                 acteur="t", db_path=db)
    perim.enregistrer(cid, PERIMETRE_2, mois="2026-08", db_path=db)

    r = perim.resume(cid, 700.0, db_path=db)
    assert r["nb_logements"] == 2
    assert r["quote_part_theorique"] == 350.0
    assert [l["quote_part_montant"] for l in r["lignes"]] == [350.0, 350.0]
    assert r["total_reparti"] == 700.0, "la somme des quotes-parts doit égaler le montant"


@pytest.mark.parametrize("montant,nb,attendu", [
    (700.0, 2, 350.0), (700.0, 4, 175.0), (100.0, 3, 33.34), (0.03, 2, 0.02),
])
def test_repartition_egale_deterministe(montant, nb, attendu):
    """`repartir_egal` distribue les centimes sans jamais perdre ni créer d'argent.

    Le centime résiduel va aux PREMIERS logements (ordre trié) : 100 / 3 donne 33,34 + 33,33 +
    33,33. Une répartition qui arrondirait chaque part isolément rendrait 99,99.
    """
    from app.services import charges_impact_service as impact
    logements = [f"LOG_{i}" for i in range(nb)]
    parts = impact.repartir_egal(montant, logements)
    assert parts[0]["quote_part"] == attendu
    assert round(sum(p["quote_part"] for p in parts), 2) == round(montant, 2)


def test_perimetre_absent_pour_une_charge_globale(db, ecritures_actives):
    cid = _charge(db, refacturable="NON")
    assert perim.lire(cid, db_path=db) == []
    assert perim.resume(cid, 700.0, db_path=db)["nb_logements"] == 0


def test_reenregistrer_remplace_le_perimetre(db, ecritures_actives):
    """Corriger une charge ne doit pas empiler deux périmètres contradictoires."""
    cid = _charge(db)
    perim.enregistrer(cid, PERIMETRE_2, mois="2026-08", db_path=db)
    perim.enregistrer(cid, [PERIMETRE_2[0]], mois="2026-08", db_path=db)
    assert perim.logements(cid, db_path=db) == ["LOG_A"]


# ── Éligibilité à la refacturation (§8, §9) ─────────────────────────────────────────────────────

def test_charge_multi_logements_est_proposable_a_chaque_proprietaire(db, ecritures_actives):
    """LE défaut de la recette : sans périmètre persisté, la position naissait `A_TRAITER` et
    n'était proposable à personne — la dépense était refacturable en théorie, irrécupérable en
    pratique."""
    cid = _charge(db)
    perim.enregistrer(cid, PERIMETRE_2, mois="2026-08", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)

    pos = refac.position_de_charge(cid, db_path=db)
    assert pos is not None
    assert pos["statut"] == refac.STATUT_DISPONIBLE
    assert sorted(pos["proprietaires_eligibles"]) == ["PROP_A", "PROP_B"]
    assert pos["montant_origine"] == 700.0, "le refacturable est le TOTAL, pas la quote-part"

    for pid, lid in (("PROP_A", "LOG_A"), ("PROP_B", "LOG_B")):
        proposees = refac.proposer_pour_facture(pid, logement_id=lid, db_path=db)
        assert cid in [p["charge_id"] for p in proposees], f"non proposable à {pid}"


def test_charge_non_refacturable_ne_cree_aucune_position(db, ecritures_actives):
    cid = _charge(db, refacturable="NON")
    assert refac.position_de_charge(cid, db_path=db) is None


def test_un_proprietaire_etranger_ne_voit_pas_la_charge(db, ecritures_actives):
    cid = _charge(db)
    perim.enregistrer(cid, PERIMETRE_2, mois="2026-08", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    assert refac.proposer_pour_facture("PROP_ETRANGER", db_path=db) == []


# ── Cycle de contrôle (§7, §54.6-9) ─────────────────────────────────────────────────────────────

def test_une_charge_naît_a_controler(db, ecritures_actives):
    cid = _charge(db)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_A_CONTROLER


def test_validation_rend_la_charge_conforme_et_persiste(db, ecritures_actives):
    cid = _charge(db)
    r = saisie.valider_controle(cid, acteur="t", db_path=db)
    assert r["ok"] and r["statut_controle"] == saisie.CONTROLE_VALIDE
    # Relecture par une NOUVELLE connexion : c'est la persistance qui est testée, pas un cache.
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_VALIDE


def test_double_validation_est_idempotente(db, ecritures_actives):
    """Double-clic, rafraîchissement, double soumission : un seul événement, un seul état."""
    cid = _charge(db)
    saisie.valider_controle(cid, acteur="t", db_path=db)
    second = saisie.valider_controle(cid, acteur="t", db_path=db)
    assert second["ok"] and second["inchange"] is True
    evenements = [e["evenement"] for e in saisie.historique(cid, db_path=db)]
    assert evenements.count(saisie.EVT_VALIDATION_CONTROLE) == 1


def test_anomalie_est_la_contrepartie_de_la_validation(db, ecritures_actives):
    """Un contrôle qui ne peut que dire « oui » n'est pas un contrôle."""
    cid = _charge(db)
    saisie.signaler_anomalie(cid, acteur="t", motif="montant douteux", db_path=db)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_ANOMALIE
    # Réversible : une anomalie levée se valide.
    saisie.valider_controle(cid, acteur="t", db_path=db)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_VALIDE


def test_charge_annulee_nest_plus_controlable(db, ecritures_actives):
    """Le cycle de VIE prime sur le cycle de CONTRÔLE : une charge annulée est close."""
    cid = _charge(db)
    saisie.annuler(cid, acteur="t", motif="doublon", db_path=db)
    r = saisie.valider_controle(cid, acteur="t", db_path=db)
    assert r["ok"] is False and r["code"] == saisie.E_DEJA_ANNULEE


def test_charge_inconnue_refusee(db, ecritures_actives):
    r = saisie.valider_controle("CHG-INEXISTANTE", acteur="t", db_path=db)
    assert r["ok"] is False and r["code"] == saisie.E_INTROUVABLE


def test_statut_controle_et_statut_de_vie_sont_deux_colonnes(db, ecritures_actives):
    """Une charge peut être ACTIVE et non contrôlée, ou VALIDE puis annulée. Les confondre
    ferait disparaître l'un des deux états."""
    cid = _charge(db)
    saisie.valider_controle(cid, acteur="t", db_path=db)
    saisie.annuler(cid, acteur="t", motif="erreur", db_path=db)
    ligne = saisie.lire(cid, db_path=db)
    assert ligne["statut"] == saisie.STATUT_ANNULEE
    assert saisie.statut_controle(ligne) == saisie.CONTROLE_VALIDE


# ── Migration 0074 ──────────────────────────────────────────────────────────────────────────────

def test_table_perimetre_contrainte_unique(db):
    """Un même logement ne peut pas figurer deux fois dans le périmètre d'une charge."""
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO charges_perimetre_analytique "
                     "(charge_id, logement_id, quote_part_montant) VALUES ('CHG-X','LOG_A',350)")
        conn.execute("INSERT OR REPLACE INTO charges_perimetre_analytique "
                     "(charge_id, logement_id, quote_part_montant) VALUES ('CHG-X','LOG_A',175)")
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM charges_perimetre_analytique "
                         "WHERE charge_id='CHG-X'").fetchone()[0]
    finally:
        conn.close()
    assert n == 1

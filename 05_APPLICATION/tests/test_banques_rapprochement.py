"""Rapprochement mouvement bancaire <-> objet métier : persistance, partiel/multiple,
dépassement refusé, double rapprochement refusé, propositions automatiques."""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations
from app.services import banques_rapprochement_service as svc


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


# ── Rapprochement simple par type d'objet ────────────────────────────────────

def test_rapprocher_reservation(db):
    res = svc.enregistrer("MVT-1", "RESERVATION", "RES_001", 850.0, montant_mouvement=850.0,
                          acteur="recette", db_path=db)
    assert res["ok"], res
    liens = svc.lister("MVT-1", db_path=db)
    assert len(liens) == 1 and liens[0]["type_objet"] == "RESERVATION"


def test_rapprocher_charge(db):
    res = svc.enregistrer("MVT-2", "CHARGE_FOURNISSEUR", "CHG_010", 120.0, montant_mouvement=120.0,
                          acteur="recette", db_path=db)
    assert res["ok"], res


def test_rapprocher_proprietaire(db):
    res = svc.enregistrer("MVT-3", "REVERSEMENT_PROPRIETAIRE", "PROP_A", 400.0,
                          montant_mouvement=400.0, acteur="recette", db_path=db)
    assert res["ok"], res


def test_rapprocher_associe(db):
    res = svc.enregistrer("MVT-4", "REMBOURSEMENT_ASSOCIE", "PERS_X", 75.0,
                          montant_mouvement=75.0, acteur="recette", db_path=db)
    assert res["ok"], res


def test_type_objet_inconnu_refuse(db):
    res = svc.enregistrer("MVT-5", "TYPE_INEXISTANT", "X", 10.0, montant_mouvement=10.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_TYPE_OBJET_INCONNU


def test_montant_invalide_refuse(db):
    res = svc.enregistrer("MVT-6", "RESERVATION", "RES_001", -10.0, montant_mouvement=10.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_MONTANT_INVALIDE
    res2 = svc.enregistrer("MVT-6", "RESERVATION", "RES_001", 0, montant_mouvement=10.0, db_path=db)
    assert res2["ok"] is False and res2["code"] == svc.E_MONTANT_INVALIDE


# ── Partiel / multiple ────────────────────────────────────────────────────────

def test_rapprochement_partiel_puis_complement(db):
    r1 = svc.enregistrer("MVT-7", "RESERVATION", "RES_A", 300.0, montant_mouvement=850.0, db_path=db)
    assert r1["ok"] and r1["montant_restant"] == 550.0
    etat = svc.etat_rapprochement("MVT-7", 850.0, db_path=db)
    assert etat["statut"] == "PARTIEL" and etat["montant_restant"] == 550.0

    r2 = svc.enregistrer("MVT-7", "RESERVATION", "RES_B", 550.0, montant_mouvement=850.0, db_path=db)
    assert r2["ok"] and r2["montant_restant"] == 0.0
    etat2 = svc.etat_rapprochement("MVT-7", 850.0, db_path=db)
    assert etat2["statut"] == "RAPPROCHE"
    liens = svc.lister("MVT-7", db_path=db)
    assert len(liens) == 2                       # un mouvement couvrant deux objets


def test_un_objet_regle_par_plusieurs_mouvements(db):
    """Le même objet (une charge de 500) réglé par deux mouvements distincts (300 + 200)."""
    r1 = svc.enregistrer("MVT-8", "CHARGE_FOURNISSEUR", "CHG_099", 300.0,
                         montant_mouvement=300.0, db_path=db)
    r2 = svc.enregistrer("MVT-9", "CHARGE_FOURNISSEUR", "CHG_099", 200.0,
                         montant_mouvement=200.0, db_path=db)
    assert r1["ok"] and r2["ok"]
    # Les deux mouvements référencent le même objet_id sans conflit.


def test_depassement_refuse(db):
    svc.enregistrer("MVT-10", "RESERVATION", "RES_C", 600.0, montant_mouvement=850.0, db_path=db)
    res = svc.enregistrer("MVT-10", "RESERVATION", "RES_D", 300.0, montant_mouvement=850.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEPASSEMENT


def test_double_rapprochement_refuse_si_deja_complet(db):
    svc.enregistrer("MVT-11", "RESERVATION", "RES_E", 850.0, montant_mouvement=850.0, db_path=db)
    res = svc.enregistrer("MVT-11", "RESERVATION", "RES_F", 10.0, montant_mouvement=850.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEJA_RAPPROCHE


def test_annulation_libere_le_montant(db):
    r1 = svc.enregistrer("MVT-12", "RESERVATION", "RES_G", 850.0, montant_mouvement=850.0, db_path=db)
    svc.annuler(r1["rapprochement_id_opaque"], acteur="recette", db_path=db)
    # Le lien annulé ne compte plus dans le montant déjà rapproché : un nouveau lien est possible.
    r2 = svc.enregistrer("MVT-12", "RESERVATION", "RES_H", 850.0, montant_mouvement=850.0, db_path=db)
    assert r2["ok"], r2


# ── Transitions et historique ─────────────────────────────────────────────────

def test_confirmer_puis_historique(db):
    r = svc.enregistrer("MVT-13", "RESERVATION", "RES_I", 850.0, montant_mouvement=850.0,
                        statut=svc.ST_PROPOSE, acteur="auto", db_path=db)
    opaque = r["rapprochement_id_opaque"]
    conf = svc.confirmer(opaque, acteur="recette", commentaire="ok", db_path=db)
    assert conf["ok"] and conf["statut"] == svc.ST_CONFIRME
    hist = svc.historique_evenements(opaque, db_path=db)
    assert len(hist) == 2                        # CREATION + CONFIRMATION
    assert hist[0]["type_evenement"] == "CONFIRMATION"


def test_refuser_transition(db):
    r = svc.enregistrer("MVT-14", "NON_IDENTIFIE", None, 45.0, montant_mouvement=45.0, db_path=db)
    res = svc.refuser(r["rapprochement_id_opaque"], commentaire="pas le bon objet", db_path=db)
    assert res["ok"] and res["statut"] == svc.ST_REFUSE
    # Un rapprochement REFUSE ne compte plus comme actif : le montant redevient disponible.
    etat = svc.etat_rapprochement("MVT-14", 45.0, db_path=db)
    assert etat["statut"] == "NON_RAPPROCHE"


def test_transition_sur_opaque_inconnu_refuse(db):
    res = svc.confirmer("BRP-INEXISTANT", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_INTROUVABLE


# ── Propositions automatiques ─────────────────────────────────────────────────

def test_proposer_correspondance_exacte():
    mouvement = {"montant": 850.0, "date_operation": "2026-06-05", "libelle": "VIR HOSTAWAY RES_A1"}
    candidats = [
        {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0,
         "date": "2026-06-04", "reference": "RES_A1"},
        {"type_objet": "RESERVATION", "objet_id": "RES_A2", "montant": 120.0, "date": "2026-01-01"},
    ]
    props = svc.proposer(mouvement, candidats)
    assert len(props) == 1
    assert props[0]["objet_id"] == "RES_A1"
    assert props[0]["raison"] in ("Correspondance exacte", "Correspondance probable")


def test_proposer_aucune_correspondance():
    mouvement = {"montant": 999.0, "date_operation": "2026-06-05", "libelle": "INCONNU"}
    candidats = [{"type_objet": "RESERVATION", "objet_id": "RES_X", "montant": 12.0,
                 "date": "2020-01-01"}]
    assert svc.proposer(mouvement, candidats) == []


# ── Rapprochement groupé (recherche bornée) ──────────────────────────────────

def _objet(oid, montant, date_):
    return {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": oid, "montant": montant,
            "date": date_}


def test_groupe_exact_deux_objets():
    mouvement = {"montant": 1100.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 600.0, "2026-06-12"),
                 _objet("C", 400.0, "2026-06-01")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert not r["ambigu"]
    assert len(r["groupes"]) == 1
    g = r["groupes"][0]
    assert g["nb_objets"] == 2 and g["somme"] == 1100.0
    ids = {o["objet_id"] for o in g["objets"]}
    assert ids == {"A", "B"}
    assert g["statut_propose"] == svc.ST_PROPOSE


def test_groupe_exact_trois_objets():
    mouvement = {"montant": 1500.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 600.0, "2026-06-12"),
                 _objet("C", 400.0, "2026-06-14")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert len(r["groupes"]) == 1 and r["groupes"][0]["nb_objets"] == 3


def test_groupe_aucune_combinaison():
    mouvement = {"montant": 999.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 600.0, "2026-06-12")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert r["groupes"] == [] and not r["ambigu"]


def test_groupe_plusieurs_combinaisons_ambigu():
    mouvement = {"montant": 500.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 250.0, "2026-06-11"),
                 _objet("C", 250.0, "2026-06-12")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert r["ambigu"] and len(r["groupes"]) == 2
    assert all(g["statut_propose"] == svc.ST_A_CONTROLER for g in r["groupes"])


def test_groupe_tolerance_1_centime():
    mouvement = {"montant": 1100.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 600.01, "2026-06-12")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert len(r["groupes"]) == 1


def test_groupe_ecart_superieur_a_la_tolerance_refuse():
    mouvement = {"montant": 1100.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 600.5, "2026-06-12")]
    r = svc.proposer_groupes(mouvement, candidats)
    assert r["groupes"] == []


def test_groupe_candidat_hors_fenetre_exclu():
    mouvement = {"montant": 900.0, "date_operation": "2026-06-15"}
    candidats = [_objet("A", 500.0, "2026-06-10"), _objet("B", 400.0, "2025-01-01")]
    r = svc.proposer_groupes(mouvement, candidats, fenetre_jours=30)
    assert r["nb_candidats_examines"] == 1
    assert r["groupes"] == []


def test_groupe_limite_candidats_appliquee():
    mouvement = {"montant": 100.0, "date_operation": "2026-06-15"}
    candidats = [_objet(f"O{i}", 10.0, "2026-06-15") for i in range(30)]
    r = svc.proposer_groupes(mouvement, candidats, max_candidats=20, max_objets_groupe=5)
    assert r["nb_candidats_examines"] == 20


def test_groupe_limite_taille_groupe_appliquee():
    mouvement = {"montant": 60.0, "date_operation": "2026-06-15"}
    candidats = [_objet(f"O{i}", 10.0, "2026-06-15") for i in range(10)]
    r = svc.proposer_groupes(mouvement, candidats, max_objets_groupe=5)
    # 6 objets à 10€ feraient 60€ mais le groupe est borné à 5 -> aucune combinaison à 6 trouvée
    assert all(g["nb_objets"] <= 5 for g in r["groupes"])
    assert r["groupes"] == []   # 5*10=50 != 60, 6*10=60 mais 6 > max_objets_groupe


def test_groupe_limite_iterations_signalee():
    mouvement = {"montant": 999999.0, "date_operation": "2026-06-15"}
    candidats = [_objet(f"O{i}", float(i + 1), "2026-06-15") for i in range(20)]
    r = svc.proposer_groupes(mouvement, candidats, max_iterations=10)
    assert r["limite_atteinte"] is True


def test_groupe_tri_deterministe_resultat_stable():
    mouvement = {"montant": 1100.0, "date_operation": "2026-06-15"}
    candidats = [_objet("B", 600.0, "2026-06-12"), _objet("A", 500.0, "2026-06-10"),
                 _objet("C", 400.0, "2026-06-01")]
    r1 = svc.proposer_groupes(mouvement, candidats)
    r2 = svc.proposer_groupes(mouvement, list(reversed(candidats)))
    assert r1["groupes"] == r2["groupes"]   # ordre d'entrée sans effet sur le résultat


def test_groupe_reste_utilise_pas_montant_brut():
    # objet A déjà partiellement rapproché à 400€ sur 1000€ -> reste 600€
    mouvement = {"montant": 1100.0, "date_operation": "2026-06-15"}
    candidats = [{"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant": 1000.0,
                 "reste": 600.0, "date": "2026-06-10"},
                {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "B", "montant": 500.0,
                 "date": "2026-06-12"}]
    r = svc.proposer_groupes(mouvement, candidats)
    assert len(r["groupes"]) == 1
    assert r["groupes"][0]["objets"][0]["montant_affecte"] in (600.0, 500.0)  # reste, pas 1000


def test_confirmer_groupe_ecrit_tous_les_liens(db):
    affectations = [
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant_affecte": 500.0},
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "B", "montant_affecte": 600.0},
    ]
    r = svc.confirmer_groupe("MVT-GROUPE-1", 1100.0, affectations, db_path=db)
    assert r["ok"] and r["nb_objets"] == 2
    liens = svc.lister("MVT-GROUPE-1", db_path=db)
    assert len(liens) == 2
    assert {l["objet_id"] for l in liens} == {"A", "B"}
    assert all(l["statut"] == svc.ST_PROPOSE for l in liens)   # jamais confirmé automatiquement


def test_confirmer_groupe_partiel_accepte_reste_calculable(db):
    # un groupe peut couvrir seulement une partie du mouvement — comportement normal du moteur
    # générique (comme enregistrer()), le reste demeure disponible pour un rapprochement ultérieur.
    affectations = [{"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A",
                    "montant_affecte": 500.0}]
    r = svc.confirmer_groupe("MVT-GROUPE-2", 1100.0, affectations, db_path=db)
    assert r["ok"]
    assert svc.montant_deja_rapproche("MVT-GROUPE-2", db_path=db) == 500.0


def test_confirmer_groupe_objet_duplique_refuse(db):
    affectations = [
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant_affecte": 500.0},
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant_affecte": 600.0},
    ]
    r = svc.confirmer_groupe("MVT-GROUPE-3", 1100.0, affectations, db_path=db)
    assert not r["ok"] and r["code"] == svc.E_GROUPE_OBJET_DEJA_UTILISE


def test_confirmer_groupe_vide_refuse(db):
    r = svc.confirmer_groupe("MVT-GROUPE-4", 100.0, [], db_path=db)
    assert not r["ok"] and r["code"] == svc.E_GROUPE_VIDE


def test_confirmer_groupe_depassement_refuse(db):
    svc.enregistrer("MVT-GROUPE-5", "REVERSEMENT_PROPRIETAIRE", "X", 1100.0,
                    montant_mouvement=1100.0, statut=svc.ST_CONFIRME, db_path=db)
    affectations = [{"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A",
                    "montant_affecte": 1100.0}]
    r = svc.confirmer_groupe("MVT-GROUPE-5", 1100.0, affectations, db_path=db)
    assert not r["ok"] and r["code"] == svc.E_DEPASSEMENT


def test_confirmer_groupe_mouvement_deja_partiellement_consomme_reste_ok(db):
    # mouvement de 1100€ déjà rapproché à 100€ -> il reste 1000€ de marge pour un groupe
    svc.enregistrer("MVT-GROUPE-6", "CHARGE_FOURNISSEUR", "Y", 100.0,
                    montant_mouvement=1100.0, db_path=db)
    affectations = [
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant_affecte": 500.0},
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "B", "montant_affecte": 500.0},
    ]
    r = svc.confirmer_groupe("MVT-GROUPE-6", 1100.0, affectations, db_path=db)
    assert r["ok"]
    assert svc.montant_deja_rapproche("MVT-GROUPE-6", db_path=db) == 1100.0


def test_confirmer_groupe_type_objet_inconnu_annule_toute_la_transaction(db):
    affectations = [
        {"type_objet": "REVERSEMENT_PROPRIETAIRE", "objet_id": "A", "montant_affecte": 500.0},
        {"type_objet": "TYPE_INEXISTANT", "objet_id": "B", "montant_affecte": 600.0},
    ]
    r = svc.confirmer_groupe("MVT-GROUPE-7", 1100.0, affectations, db_path=db)
    assert not r["ok"]
    assert svc.lister("MVT-GROUPE-7", db_path=db) == []   # rollback total, aucun lien partiel

"""Invariants de l'arborescence des charges — issus du banc d'essai exhaustif.

Le banc d'essai a parcouru chaque feuille métier du modèle de charge et relevé ses conséquences.
Ce fichier fige les INVARIANTS qui en ressortent, sous forme paramétrée : un invariant vrai pour
toutes les branches se teste une fois, sur toutes les branches — pas cinquante fois en copié-collé.

Les axes réels (découverts dans le code, pas supposés) :
    catégorie (menage FORCE/CHOIX/INTERDIT, avantage autorisé/interdit)
    périmètre (0, 1 ou N logements ; un propriétaire élargit à ses logements actifs)
    refacturable (exige ≥ 1 logement final)
    code_impact (IC intra-comptable / HC extra-comptable ; HR exclu du formulaire standard)
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import charges_impact_service as impact
from app.services import charges_perimetre_service as perim
from app.services import charges_refacturation_service as refac
from app.services import charges_saisie_service as saisie


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    for flag in ("CHARGES_REAL_WRITE_ENABLED", "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True, raising=False)
    apply_migrations(chemin)
    return chemin


def _charge(db, **kw):
    donnees = {"date_charge": "2026-09-05", "montant": 700.0, "categorie_charge_id": "CHG_008",
               "code_impact": "IC", "refacturable": "NON", "commentaire": "invariant"}
    donnees.update(kw)
    return saisie.creer(donnees, acteur="t", db_path=db)["charge_id"]


def _perimetre(n, montant=700.0):
    parts = impact.repartir_egal(montant, [f"LOG_{i}" for i in range(n)])
    return [{"logement_id": p["logement_id"], "proprietaire_id": f"PROP_{i}",
             "mois": "2026-09", "quote_part_montant": p["quote_part"]}
            for i, p in enumerate(parts)]


# ── INVARIANT 1 : la somme des quotes-parts vaut EXACTEMENT le montant ──────────────────────────

@pytest.mark.parametrize("montant,nb", [
    (700.0, 1), (700.0, 2), (700.0, 3), (700.0, 4), (700.0, 7),
    (100.0, 3), (100.0, 6), (0.03, 2), (0.01, 3), (1234.56, 5), (999.99, 7),
])
def test_somme_ventilation_egale_le_montant(db, montant, nb):
    """Aucun centime ne se perd ni ne s'invente, quel que soit le nombre de logements.

    Une répartition qui arrondirait chaque part isolément rendrait 99,99 pour 100 € sur 3.
    """
    cid = _charge(db, montant=montant)
    perim.enregistrer(cid, _perimetre(nb, montant), mois="2026-09", db_path=db)
    r = perim.resume(cid, montant, db_path=db)
    assert r["total_reparti"] == round(montant, 2)
    assert r["nb_logements"] == nb


@pytest.mark.parametrize("montant,nb,premiere", [
    (700.0, 2, 350.0), (700.0, 4, 175.0), (100.0, 3, 33.34), (10.0, 3, 3.34),
])
def test_le_centime_residuel_va_aux_premiers(montant, nb, premiere):
    """Règle déterministe : les premiers logements (ordre trié) reçoivent le centime en trop.
    Déterministe veut dire reproductible — deux exécutions donnent la même ventilation."""
    parts = impact.repartir_egal(montant, [f"LOG_{i}" for i in range(nb)])
    assert parts[0]["quote_part"] == premiere
    assert round(sum(p["quote_part"] for p in parts), 2) == round(montant, 2)
    assert parts == impact.repartir_egal(montant, [f"LOG_{i}" for i in range(nb)])


# ── INVARIANT 2 : le refacturable est le montant TOTAL, jamais la quote-part ────────────────────

@pytest.mark.parametrize("nb", [1, 2, 3, 5])
def test_le_refacturable_est_le_total_pas_la_quote_part(db, nb):
    """LA distinction structurante : l'analytique divise, la refacturation non.

    700 € sur 2 logements pèsent 350 € sur chaque résultat, mais restent 700 € à récupérer.
    """
    cid = _charge(db, refacturable="OUI")
    perim.enregistrer(cid, _perimetre(nb), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    pos = refac.position_de_charge(cid, db_path=db)
    assert pos["montant_origine"] == 700.0
    assert pos["montant_restant"] == 700.0
    quote = perim.resume(cid, 700.0, db_path=db)["quote_part_theorique"]
    if nb > 1:
        assert quote < pos["montant_origine"]


# ── INVARIANT 3 : une charge non refacturable n'existe pas pour la facturation ──────────────────

@pytest.mark.parametrize("nb", [0, 1, 2, 3])
def test_charge_non_refacturable_sans_position(db, nb):
    cid = _charge(db, refacturable="NON")
    if nb:
        perim.enregistrer(cid, _perimetre(nb), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    assert refac.position_de_charge(cid, db_path=db) is None
    for i in range(max(nb, 1)):
        assert refac.proposer_pour_facture(f"PROP_{i}", db_path=db) == []


# ── INVARIANT 4 : imputé + réservé ≤ éligible ───────────────────────────────────────────────────

@pytest.mark.parametrize("montants", [
    [700.0], [350.0, 350.0], [500.0, 200.0], [100.0, 100.0, 500.0], [699.99, 0.01],
])
def test_le_cumul_impute_ne_depasse_jamais_la_source(db, montants):
    """Quelle que soit la répartition commerciale, le total récupéré vaut au plus la dépense."""
    cid = _charge(db, refacturable="OUI")
    perim.enregistrer(cid, _perimetre(2), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    pos = refac.position_de_charge(cid, db_path=db)["position_id"]
    for i, m in enumerate(montants):
        r = refac.imputer(pos, m, facture_id=f"FPR-{i}", acteur="t",
                          justification="repartition de test", db_path=db)
        assert r["ok"], r
    conn = get_db(db)
    try:
        impute, eligible = conn.execute(
            "SELECT montant_impute_total, montant_eligible FROM charges_refacturation_positions "
            "WHERE position_id = ?", (pos,)).fetchone()
    finally:
        conn.close()
    assert round(impute, 2) == round(sum(montants), 2) == 700.0
    assert impute <= eligible + 0.005


def test_depassement_refuse_sur_position_ouverte(db):
    """Position encore ouverte (partiellement imputée) : le dépassement est refusé POUR CE MOTIF."""
    cid = _charge(db, refacturable="OUI")
    perim.enregistrer(cid, _perimetre(2), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    pos = refac.position_de_charge(cid, db_path=db)["position_id"]
    refac.imputer(pos, 500.0, facture_id="FPR-1", acteur="t", justification="partiel",
                  db_path=db)
    r = refac.imputer(pos, 300.0, facture_id="FPR-2", acteur="t", justification="x", db_path=db)
    assert r["ok"] is False and r["code"] == refac.E_MONTANT_DEPASSE


def test_position_entierement_imputee_est_close(db):
    """Une fois le montant entièrement récupéré, la position se ferme : toute imputation
    supplémentaire est refusée pour STATUT_CLOS, un motif plus précis que « dépassement »."""
    cid = _charge(db, refacturable="OUI")
    perim.enregistrer(cid, _perimetre(2), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    pos = refac.position_de_charge(cid, db_path=db)["position_id"]
    refac.imputer(pos, 700.0, facture_id="FPR-1", acteur="t", db_path=db)
    assert refac.position_de_charge(cid, db_path=db)["statut"] == refac.STATUT_IMPUTEE
    r = refac.imputer(pos, 1.0, facture_id="FPR-2", acteur="t", justification="x", db_path=db)
    assert r["ok"] is False and r["code"] == refac.E_STATUT_CLOS


def test_imputation_partielle_exige_une_justification(db):
    """Ne récupérer qu'une partie d'une dépense est une DÉCISION : elle se justifie.

    Le banc d'essai a montré que ce contrôle, appliqué seulement à la validation, rendait une
    facture partielle impossible à valider — le motif était réclamé à un moment où plus personne
    ne pouvait le fournir. Il est désormais recueilli à la composition (migration 0077).
    """
    cid = _charge(db, refacturable="OUI")
    perim.enregistrer(cid, _perimetre(2), mois="2026-09", db_path=db)
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    pos = refac.position_de_charge(cid, db_path=db)["position_id"]
    refuse = refac.imputer(pos, 500.0, facture_id="FPR-1", acteur="t", db_path=db)
    assert refuse["ok"] is False and refuse["code"] == refac.E_JUSTIFICATION_MANQUANTE
    accepte = refac.imputer(pos, 500.0, facture_id="FPR-1", acteur="t",
                            justification="solde reporté sur l'autre propriétaire", db_path=db)
    assert accepte["ok"] is True


# ── INVARIANT 5 : le code d'impact décide, et lui seul ──────────────────────────────────────────

@pytest.mark.parametrize("code,compta_attendue", [("IC", "OUI"), ("HC", "NON")])
def test_prise_en_compta_derive_du_code_impact(db, code, compta_attendue):
    """IC = intra-comptable, HC = extra-comptable. Le référentiel `ref_codes_impact`, le moteur
    `flux_unifie_service._IMPACT_FLAGS` et cette dérivation disent tous la même chose."""
    from app.services.charges_preview_service import PRISE_EN_COMPTA_BY_IMPACT
    assert PRISE_EN_COMPTA_BY_IMPACT[code] == compta_attendue


def test_hr_est_hors_du_formulaire_standard():
    """`HR` (hors résultat) existe au référentiel mais n'est pas proposé à la saisie : c'est un
    code de neutralisation, pas une charge ordinaire. Le vérifier empêche qu'il y entre par
    inadvertance."""
    from app.services.charges_preview_service import STANDARD_CODES_IMPACT
    assert STANDARD_CODES_IMPACT == frozenset({"IC", "HC"})
    assert "HR" not in STANDARD_CODES_IMPACT


# ── INVARIANT 6 : une charge ne produit AUCUNE écriture comptable ───────────────────────────────

@pytest.mark.parametrize("code_impact,refacturable", [
    ("IC", "OUI"), ("IC", "NON"), ("HC", "OUI"), ("HC", "NON"),
])
def test_une_charge_ne_cree_jamais_d_ecriture(db, code_impact, refacturable):
    """La comptabilité naît de la FACTURE, jamais de la charge — quelle que soit la branche.

    Une charge est une dépense constatée ; l'écriture de vente constate, elle, une créance sur le
    propriétaire. Les lier directement comptabiliserait deux fois la même opération.
    """
    cid = _charge(db, code_impact=code_impact, refacturable=refacturable)
    if refacturable == "OUI":
        perim.enregistrer(cid, _perimetre(1), mois="2026-09", db_path=db)
        refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque = ?",
                         (cid,)).fetchone()[0]
    finally:
        conn.close()
    assert n == 0


# ── INVARIANT 7 : le cycle de contrôle est stable ───────────────────────────────────────────────

@pytest.mark.parametrize("code_impact", ["IC", "HC"])
@pytest.mark.parametrize("refacturable", ["OUI", "NON"])
def test_toute_charge_nait_a_controler_et_se_valide(db, code_impact, refacturable):
    cid = _charge(db, code_impact=code_impact, refacturable=refacturable)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_A_CONTROLER
    saisie.valider_controle(cid, acteur="t", db_path=db)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_CONFORME
    # Une charge validée reste validée : aucune relecture ni resynchronisation ne la dégrade.
    refac.synchroniser_depuis_charge(cid, acteur="t", db_path=db)
    assert saisie.statut_controle(saisie.lire(cid, db_path=db)) == saisie.CONTROLE_CONFORME


# ── INVARIANT 8 : la branche MÉNAGE est persistée (migration 0076) ──────────────────────────────

@pytest.mark.parametrize("mode,cles", [
    ("INTERVENANT", ["INT_0001", "INT_0002"]),
    ("LOGEMENT", ["LOG_0001", "LOG_0002", "LOG_0003"]),
])
def test_perimetre_menage_persiste_et_boucle(db, mode, cles):
    """Le périmètre ménage était calculé puis jeté — même défaut que le périmètre analytique avant
    la migration 0074. `lot6f_cout_complet_menages` filtre sur `affectable_menage`."""
    cid = _charge(db, categorie_charge_id="CHG_004", affectable_menage="OUI")
    parts = impact.repartir_egal(700.0, cles)
    entrees = [{("intervenant_id" if mode == "INTERVENANT" else "logement_id"): p["logement_id"],
                "mois": "2026-09", "quote_part_montant": p["quote_part"]} for p in parts]
    n = perim.enregistrer_menage(cid, mode, entrees, mois="2026-09", db_path=db)
    assert n == len(cles)

    lignes = perim.lire_menage(cid, db_path=db)
    assert len(lignes) == len(cles)
    assert {l["mode"] for l in lignes} == {mode}
    assert round(sum(l["quote_part_montant"] for l in lignes), 2) == 700.0
    assert saisie.lire(cid, db_path=db)["affectable_menage"] == "OUI"


def test_perimetre_menage_exige_une_seule_dimension(db):
    """Un périmètre ménage désigne un intervenant OU un logement — jamais les deux, jamais aucun.
    La contrainte est SQL : même en contournant le service, la base refuse."""
    import sqlite3
    cid = _charge(db, categorie_charge_id="CHG_004")
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO charges_perimetre_menage (charge_id, mode, intervenant_id, "
                "logement_id, quote_part_montant) VALUES (?, 'LOGEMENT', 'INT_1', 'LOG_1', 10)",
                (cid,))
            conn.commit()
    finally:
        conn.close()


def test_charge_non_menage_ne_porte_pas_le_drapeau(db):
    cid = _charge(db, affectable_menage="NON")
    assert saisie.lire(cid, db_path=db)["affectable_menage"] == "NON"
    assert perim.lire_menage(cid, db_path=db) == []

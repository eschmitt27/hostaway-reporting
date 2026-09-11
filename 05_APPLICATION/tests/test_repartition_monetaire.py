"""§6/§7/§23 — aucun centime ne disparaît, et une seule règle le garantit.

Le dépôt portait QUATRE répartitions monétaires et TROIS comportements :

  · résidu aux PREMIERS de l'ordre trié      (`charges_engine.repartir_egal`) ;
  · le DERNIER absorbe tout le résidu        (`lib_charges_menage`, `facture_ventilation_menage`) ;
  · aucun rattrapage, le centime est perdu   (`lot6f`, `round(pool × poids / total, 2)`).

`lib_repartition` est désormais la seule implémentation pondérée. Ces tests figent son invariant —
`somme(parts) == montant`, au centime — et vérifient que les autres points d'entrée s'y accordent.

Ils portent sur la PROPRIÉTÉ, pas sur des exemples : un cas particulier qui passe ne prouve rien
d'une règle d'arrondi.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = str(Path(cfg.APP_ROOT).parent / "02_TRAVAIL")
if _TRAVAIL not in sys.path:
    sys.path.insert(0, _TRAVAIL)

import lib_repartition as rp  # noqa: E402


# ── L'invariant, sur les cas exigés par la mission ───────────────────────────

@pytest.mark.parametrize("montant, n", [
    (0.01, 3),      # 1 centime sur 3 : un seul peut le recevoir
    (0.10, 3),      # 10 centimes sur 3
    (1.00, 3),      # 1 € sur 3
    (100.00, 3),    # le cas de référence
    (100.01, 3),
    (700.00, 2),    # tombe juste : aucun résidu
    (100.00, 7),
    (10.01, 4),
    (0.03, 3),      # exactement divisible au centime
    (999999.99, 13),
])
def test_la_somme_vaut_toujours_le_montant(montant, n):
    cles = [f"L{i:02d}" for i in range(n)]
    parts = rp.repartir(montant, {c: 1.0 for c in cles})
    assert rp.somme(parts) == round(montant, 2), parts


@pytest.mark.parametrize("montant, poids", [
    (300.00, {"A": 8, "B": 1, "C": 5}),
    (50.00, {"X": 400, "Y": 245, "Z": 689, "W": 275}),
    (1234.56, {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}),
    (0.07, {"A": 1, "B": 1000000}),
])
def test_la_somme_vaut_le_montant_aussi_a_poids_inegaux(montant, poids):
    assert rp.somme(rp.repartir(montant, poids)) == round(montant, 2)


def test_le_cas_de_reference_de_la_mission():
    """100,00 € sur 3 logements : 33,34 / 33,33 / 33,33 — jamais 33,33 trois fois."""
    parts = rp.repartir(100.00, {"LOG_A": 1, "LOG_B": 1, "LOG_C": 1})
    assert parts == {"LOG_A": 33.34, "LOG_B": 33.33, "LOG_C": 33.33}
    assert rp.somme(parts) == 100.00


# ── Déterminisme ─────────────────────────────────────────────────────────────

def test_meme_entree_meme_beneficiaire_du_residu():
    """Deux appels identiques donnent le centime au MÊME élément."""
    for _ in range(5):
        assert rp.repartir(100.00, {"A": 1, "B": 1, "C": 1})["A"] == 33.34


def test_l_ordre_d_insertion_ne_decide_de_rien():
    """Le résidu ne doit pas dépendre de l'ordre d'une requête SQL sans ORDER BY.

    C'est le piège précis que la mission nomme : un dict construit dans l'ordre de lecture d'un
    curseur donnerait un bénéficiaire différent d'une exécution à l'autre."""
    attendu = rp.repartir(100.00, {"A": 1, "B": 1, "C": 1})
    assert rp.repartir(100.00, {"C": 1, "B": 1, "A": 1}) == attendu
    assert rp.repartir(100.00, {"B": 1, "A": 1, "C": 1}) == attendu


def test_le_residu_va_aux_parts_les_plus_lesees():
    """À poids inégaux, le centime revient à celle que l'arrondi a le plus rognée.

    Ce n'est pas « toujours la première » : c'est « la plus lésée, et la première en cas
    d'égalité ». À poids égaux les deux règles coïncident, ce qui rend le cas 100/3 lisible."""
    parts = rp.repartir(1.00, {"A": 1, "B": 1, "C": 97})
    # exact : A=1.03c B=1.03c C=97.94c -> entiers 1/1/97, reste 1c a la plus grande fraction (C).
    assert parts["C"] == 0.98
    assert rp.somme(parts) == 1.00


# ── Cas limites, sans exception ──────────────────────────────────────────────

def test_un_seul_beneficiaire_recoit_tout():
    assert rp.repartir(42.00, {"SEUL": 1}) == {"SEUL": 42.00}


def test_les_poids_nuls_ne_recoivent_rien_et_ne_captent_aucun_residu():
    """Une ligne sans poids ne doit pas récupérer le centime résiduel par effet de bord."""
    parts = rp.repartir(100.00, {"A": 1, "VIDE": 0, "B": 1})
    assert "VIDE" not in parts
    assert rp.somme(parts) == 100.00


def test_aucun_poids_exploitable_ne_repartit_rien():
    """Jamais de répartition arbitraire : l'appelant décide quoi faire d'un périmètre vide."""
    assert rp.repartir(100.00, {}) == {}
    assert rp.repartir(100.00, {"A": 0, "B": -3}) == {}


def test_un_montant_nul_ne_produit_aucune_part():
    assert rp.repartir(0, {"A": 1, "B": 1}) == {}


def test_un_avoir_se_repartit_avec_la_meme_regle():
    """Un montant négatif (avoir) doit rester exact, sans dérive d'arrondi de signe."""
    parts = rp.repartir(-100.00, {"A": 1, "B": 1, "C": 1})
    assert parts == {"A": -33.34, "B": -33.33, "C": -33.33}
    assert rp.somme(parts) == -100.00


# ── Une seule règle, partagée ────────────────────────────────────────────────

@pytest.mark.parametrize("montant, n", [(100.00, 3), (0.01, 3), (10.00, 7), (1.00, 6)])
def test_le_cas_egal_coincide_avec_repartir_egal_du_moteur_charges(montant, n):
    """`charges_engine.repartir_egal` et `lib_repartition` doivent rendre la MÊME chose.

    Les deux subsistent parce que le moteur Charges est un module PUR, sans accès au dossier des
    moteurs (test de pureté dédié). Ce test est ce qui empêche les deux règles de diverger — la
    duplication est tolérée, le désaccord ne l'est pas."""
    from app.moteurs.charges_engine import repartir_egal

    cles = [f"LOG_{i}" for i in range(n)]
    cote_moteur = {q["logement_id"]: q["quote_part"] for q in repartir_egal(montant, cles)}
    cote_canonique = rp.repartir_egal(montant, cles)
    assert cote_moteur == cote_canonique
    assert rp.somme(cote_moteur) == round(montant, 2)


def test_la_ventilation_de_facture_utilise_la_regle_canonique():
    """`facture_ventilation_menage_service` ne doit plus faire absorber le résidu au dernier."""
    from app.services import facture_ventilation_menage_service as fv

    res = fv.ventiler(100.00, {"LOG_A": 1.0, "LOG_B": 1.0, "LOG_C": 1.0})
    parts = {p["logement_id"]: p["part_montant"] for p in res["parts"]}
    assert parts == rp.repartir(100.00, {"LOG_A": 1.0, "LOG_B": 1.0, "LOG_C": 1.0})
    assert res["somme"] == 100.00


def test_la_ventilation_menage_moteur_utilise_la_regle_canonique():
    """`lib_charges_menage` non plus."""
    import lib_charges_menage as lcm

    impacts = [{"charge_id": "C1", "mode": "LOGEMENT", "logement_id": f"LOG_{i}"}
               for i in range(3)]
    res = lcm.ventiler_charge_menage(
        100.00, impacts,
        nb_menages={f"LOG_{i}": 1 for i in range(3)},
        cout_standard_unitaire={f"LOG_{i}": 50.0 for i in range(3)})
    parts = {q["cle"]: q["quote_part"] for q in res["quote_parts"]}
    assert parts == rp.repartir(100.00, {f"LOG_{i}": 50.0 for i in range(3)})
    assert res["somme"] == 100.00


def test_plus_aucune_division_arrondie_ligne_a_ligne_dans_lot6f():
    """STRUCTUREL — la formule qui perdait les centimes ne doit pas revenir."""
    source = (Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "lot6f_cout_complet_menages.py").read_text(
        encoding="utf-8", errors="replace")
    code = "\n".join(l for l in source.splitlines() if not l.lstrip().startswith("#"))
    assert "import lib_repartition as rp" in code
    for perdu in ("* w / sum_poids_all", "* w / sum_poids_interne", "* w / sp"):
        assert perdu not in code, f"répartition non rattrapée : {perdu}"

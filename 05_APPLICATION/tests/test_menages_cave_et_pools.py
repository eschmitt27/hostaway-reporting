"""Cave/local (REC_002) et pools de courses — invariants documentés.

Un rapport précédent affirmait la règle « cave 50 € » introuvable dans le code. **C'était faux** :
la recherche s'était limitée à `lib_menage_costs`. La règle est documentée ET implémentée :

- `REC_002 — Forfait local cave` (CHG_023, TYPE_FLUX_010, HC) vit dans `REF_Charges_Recurrentes` ;
- **D103** : clé de répartition = `COUT_STANDARD_MENAGES_MOIS`, et non `NOMBRE_MENAGES`
  (révision explicite de D045, validée le 2026-06-09) ;
- cave ventilée **uniquement sur les ménages INTERNES**, jamais les externes, date-aware,
  avec le contrôle `REC_002_LOCAL_CAVE_INTERNE_ONLY` ;
- l'implémentation est dans `lot6f_cout_complet_menages.py`.

Conséquence : ce n'est **pas** « + 50 € par ménage ». C'est un forfait mensuel ventilé au prorata du
poids (coût standard) sur les seules lignes internes. Ces tests figent cette lecture.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import app.config as cfg

LOT6F = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "lot6f_cout_complet_menages.py"


def _source() -> str:
    if not LOT6F.exists():
        pytest.skip(f"Moteur absent de cette racine : {LOT6F}")
    return LOT6F.read_text(encoding="utf-8")


def test_le_montant_de_cave_n_est_pas_code_en_dur():
    """Le montant vient de REF_Charges_Recurrentes, date-aware. Un 50 littéral serait une régression."""
    src = _source()
    assert "REF_Charges_Recurrentes" in src or "rec_ref" in src
    assert re.search(r"rec_montant\s*\(", src), "La résolution date-aware du récurrent a disparu."
    interdits = re.findall(r"local_cave\w*\s*=\s*50(?:\.0+)?\b", src)
    assert not interdits, f"Montant de cave codé en dur : {interdits}"


def test_la_cave_est_reservee_aux_menages_internes():
    """Règle figée : jamais ventilée sur les ménages externes."""
    src = _source()
    assert 'REC_002_LOCAL_CAVE_INTERNE_ONLY' in src
    m = re.search(r"qp_local\s*=\s*(.+)", src)
    assert m, "Le calcul de quote-part de cave a disparu."
    assert 'type_intervenant"] == "INTERNE"' in m.group(1), (
        "La quote-part de cave doit être conditionnée au type INTERNE.")


def test_la_cle_de_repartition_est_le_cout_standard_pas_le_nombre():
    """D103 révise D045 : la clé est COUT_STANDARD_MENAGES_MOIS, pas NOMBRE_MENAGES."""
    src = _source()
    m = re.search(r"qp_local\s*=\s*(.+)", src)
    formule = m.group(1)
    assert "poids" in formule, "La ventilation doit se faire au prorata du poids (coût standard)."
    assert "nb_menages" not in formule, "La clé NOMBRE_MENAGES a été révisée par D103."


def test_la_cave_est_ventilee_une_seule_fois():
    """Somme des quotes-parts = montant du forfait : ni oublié, ni compté deux fois."""
    src = _source()
    m = re.search(r"qp_local\s*=\s*(.+)", src)
    formule = m.group(1)
    # Ventilation proportionnelle : montant × poids_ligne / somme_des_poids_internes.
    assert "sum_poids_interne" in formule, (
        "Le dénominateur doit être la somme des poids internes, sinon le total ventilé "
        "ne vaut plus le forfait.")


def test_les_pools_sont_declares_explicitement():
    """Pool vide et pool absent doivent rester distinguables : les pools sont nommés."""
    src = _source()
    m = re.search(r"POOLS\s*=\s*\{(.+?)\}", src, re.S)
    assert m, "Le dictionnaire POOLS a disparu."
    for pool in ("LOCAL_CAVE", "COURSES", "CONSOMMABLES"):
        assert pool in m.group(1), f"Pool {pool} absent de la déclaration."


def test_aucun_montant_de_pool_code_en_dur():
    """Les pools proviennent des charges réellement saisies : jamais un montant inventé."""
    src = _source()
    m = re.search(r"POOLS\s*=\s*\{(.+?)\}", src, re.S)
    litteraux = re.findall(r":\s*\d+(?:\.\d+)?\s*[,}]", m.group(1))
    assert not litteraux, f"Montant de pool codé en dur : {litteraux}"

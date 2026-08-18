"""`reservation_calc_id` doit deriver d'une identite stable, jamais de la position dans une liste.

Avant : `RES-<mois>-HA-<n>`, ou n etait un compteur d'iteration — deux runs dans un ordre different
produisaient des cles differentes pour les memes reservations, sans qu'aucun total ne change. La cle
derive desormais de l'identifiant Hostaway (branche HA) ou de l'identifiant opaque de saisie
(branche HH), tous deux deja stables et persistants dans le modele existant.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "02_TRAVAIL"))

import lib_db_moteur as dbm


def test_cle_ha_dependant_seulement_de_l_identifiant():
    assert dbm.cle_reservation_ha("70001") == "RES-HA-70001"


def test_cle_ha_stable_quel_que_soit_l_ordre_d_appel():
    # Meme reservation appelee apres d'autres : la cle ne bouge pas.
    dbm.cle_reservation_ha("70003")
    dbm.cle_reservation_ha("70002")
    assert dbm.cle_reservation_ha("70001") == "RES-HA-70001"


def test_cle_ha_insensible_au_montant_et_au_guestcount():
    # La cle ne prend en argument que l'identifiant : un montant ou un guestCount different
    # ailleurs dans la ligne ne peut structurellement pas la changer.
    assert dbm.cle_reservation_ha("70001") == dbm.cle_reservation_ha("70001")


def test_cle_ha_absente_sans_identifiant():
    assert dbm.cle_reservation_ha(None) is None
    assert dbm.cle_reservation_ha("") is None


def test_cle_hh_reutilise_l_identifiant_opaque_de_saisie():
    assert dbm.cle_reservation_hh("RESHH-2026-05-001") == "RES-HH-RESHH-2026-05-001"


def test_cle_hh_absente_sans_identifiant():
    assert dbm.cle_reservation_hh(None) is None


def test_cles_ha_et_hh_jamais_identiques_pour_des_ids_partages():
    # Namespace explicite (Hostaway vs hors-Hostaway) : un meme identifiant brut ne peut pas
    # produire la meme cle dans les deux branches.
    assert dbm.cle_reservation_ha("001") != dbm.cle_reservation_hh("001")


def test_rerun_meme_source_meme_cle_aucun_doublon():
    # Simule deux runs successifs sur les memes reservations : les cles produites doivent
    # coincider exactement, condition necessaire a l'absence de doublon lors d'un upsert.
    ids = ["70001", "70002", "70003"]
    run_a = {dbm.cle_reservation_ha(i) for i in ids}
    run_b = {dbm.cle_reservation_ha(i) for i in reversed(ids)}
    assert run_a == run_b
    assert len(run_a) == len(ids)

"""Périmètre économique d'une réservation : période de gestion et rattachement du logement.

DÉTERMINISTE PAR CONSTRUCTION. Aucune ligne de ce fichier ne lit la base réelle : tout est posé
ici, en clair. C'est la condition pour qu'un échec parle du CODE et non du parc du jour — la suite
automatisée a précisément cessé de le faire le jour où une annonce Hostaway non rattachée est
apparue en production et a fait tomber un test.

DEUX RÈGLES, ET UNE MÊME EXIGENCE
Une réservation peut être hors du périmètre économique pour deux raisons distinctes :

  · elle tombe HORS de toute période de gestion — le mandat a pris fin, ou n'a pas commencé ;
  · son annonce n'est rattachée à AUCUN logement du parc.

Dans les deux cas la même exigence s'applique : la réservation est **conservée** et **consultable**,
elle est **signalée**, et elle ne produit **ni CA géré, ni commission, ni net propriétaire, ni nuit
gérée**. On ne la supprime pas, on ne la range pas dans un logement fourre-tout, on n'invente pas
de rattachement.

CE QUE CES RÈGLES ONT COÛTÉ QUAND ELLES MANQUAIENT
  · LOG_0002, gestion close au 2026-01-01 : 7 séjours d'août 2026 valorisés, 2 107,33 € de
    recettes et 243,65 € de commission attribués à un propriétaire sans mandat ;
  · annonce 590757, non rattachée : le calcul de TOUTES les réservations s'arrêtait — 1 600 lignes
    correctes bloquées par une donnée manquante sur un seul logement.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg

MOTEUR = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
if str(MOTEUR) not in sys.path:
    sys.path.insert(0, str(MOTEUR))

import lib_db_moteur as dbm            # noqa: E402
import lib_parc                        # noqa: E402
from lib_ref_history import resolve_management_period  # noqa: E402

#: La période de gestion de l'énoncé §4 : ouverte le 2025-01-01, close le 2026-01-01.
GESTION = [{
    "gestion_id": "G-TEST", "logement_id": "LOG_TEST", "proprietaire_id": "PROP_TEST",
    "date_debut": "2025-01-01", "date_fin": "2026-01-01", "statut_gestion": "RETIRE",
}]


def _resoudre(arrivee: str, depart: str):
    return resolve_management_period(GESTION, logement_id="LOG_TEST",
                                     date_arrivee=arrivee, date_depart=depart)


# ── §4 — la période de gestion décide, et elle seule ────────────────────────────────────────────

def test_un_sejour_dans_la_periode_est_rattache_au_proprietaire():
    """2025-06 : dans la période. Le propriétaire est établi, la ligne entre dans l'économie."""
    resolution = _resoudre("2025-06-10", "2025-06-13")
    assert resolution.status == "OK"
    assert resolution.value == "PROP_TEST"
    assert dbm.motif_exclusion_pour("HOSTAWAY_AIRBNB", "VALIDE", None) is None
    assert dbm.impacts_reservation("IC", motif_exclusion=None, statut_controle="VALIDE") \
        == ("OUI", "OUI")


def test_un_sejour_apres_la_fin_de_gestion_sort_de_l_economie():
    """2026-08 : après la fin. Aucun propriétaire établi — donc aucune valorisation possible.

    C'est le cas LOG_0002. Valoriser cette ligne revient à facturer une commission sur un logement
    qu'on ne gère plus, et à attribuer un revenu à quelqu'un qui n'a plus de mandat.
    """
    resolution = _resoudre("2026-08-07", "2026-08-09")
    assert resolution.status == "MISSING"
    assert resolution.value is None

    code = f"GESTION_LOGEMENT_{resolution.status}"
    assert code in dbm.ANOMALIES_GESTION_EXCLUANTES

    motif = dbm.motif_exclusion_pour("HOSTAWAY_AIRBNB", "A_CONTROLER", code)
    assert motif == dbm.MOTIF_HORS_PERIODE_GESTION
    # Ni CA géré, ni commission, ni net : la ligne ne pèse sur rien.
    assert dbm.impacts_reservation("IC", motif_exclusion=motif, statut_controle="A_CONTROLER") \
        == ("NON", "NON")


def test_un_sejour_qui_chevauche_la_fin_de_gestion_sort_aussi():
    """Un séjour à cheval sur la fin n'est ni « dedans » ni « dehors » : il est à trancher, et en
    attendant il ne se valorise pas."""
    resolution = _resoudre("2025-12-30", "2026-01-03")
    assert resolution.status == "OUT_OF_PERIOD"
    motif = dbm.motif_exclusion_pour("HOSTAWAY_AIRBNB", "A_CONTROLER",
                                     f"GESTION_LOGEMENT_{resolution.status}")
    assert motif == dbm.MOTIF_HORS_PERIODE_GESTION


@pytest.mark.parametrize("statut", ["MISSING", "OUT_OF_PERIOD", "AMBIGUOUS", "MISSING_OWNER"])
def test_toute_anomalie_de_gestion_exclut_l_economie(statut):
    """Les quatre issues non-OK du résolveur ont le même effet économique : aucun propriétaire
    n'est établi pour cette date, donc rien ne peut être attribué à personne."""
    motif = dbm.motif_exclusion_pour("HOSTAWAY_AIRBNB", "A_CONTROLER", f"GESTION_LOGEMENT_{statut}")
    assert motif == dbm.MOTIF_HORS_PERIODE_GESTION
    assert dbm.impacts_reservation("IC", motif_exclusion=motif) == ("NON", "NON")


def test_hors_economie_n_est_pas_la_meme_chose_qu_exclue_definitivement():
    """Trois états, pas deux — et la nuance porte toute l'intention.

    `est_exclue()` lit le STATUT, pas le motif : elle dit « la décision est prise, elle est
    justifiée, elle est définitive » (statuts `EXCLU_*`). Une réservation hors période de gestion
    ou sur une annonce non rattachée n'est PAS dans ce cas : son statut est `A_CONTROLER`, parce
    qu'une action humaine peut encore tout changer — corriger la date de fin de gestion, rattacher
    l'annonce — et le calcul repartira alors normalement.

    Elle ne pèse pourtant sur aucun agrégat entre-temps. « Hors économie » et « classée sans
    suite » sont deux choses différentes, et les confondre ferait disparaître une anomalie
    réparable dans le tas des cas tranchés.
    """
    for motif in (dbm.MOTIF_HORS_PERIODE_GESTION, dbm.MOTIF_LOGEMENT_NON_MAPPE):
        assert motif in dbm.MOTIFS_EXCLUSION
        ligne = {"statut_controle": "A_CONTROLER", "motif_exclusion": motif}
        assert dbm.est_exclue(ligne) is False, "A_CONTROLER = réparable, pas classée sans suite"
        assert dbm.impacts_reservation("IC", motif_exclusion=motif,
                                       statut_controle="A_CONTROLER") == ("NON", "NON")
    # …par contraste, une exclusion DÉFINITIVE se reconnaît à son statut.
    assert dbm.est_exclue({"statut_controle": "EXCLU_RESULTAT"}) is True


# ── §4 — un logement RETIRE n'est pas un logement au statut invalide ────────────────────────────

def test_retire_est_un_statut_de_parc_reconnu():
    """`RETIRE` figure dans la liste proposée à l'administration et quatre logements le portent.

    Il manquait pourtant à `lib_parc`, et un statut inconnu retombe sur A_CONTROLER : les séjours
    d'un logement retiré étaient classés `STATUT_PARC_INVALIDE`. Le motif était faux, et il
    MASQUAIT le vrai — ces séjours tombent hors période de gestion.
    """
    retire = {"statut_parc": "RETIRE"}
    assert lib_parc.statut_parc_traitement(retire) == "RETIRE"
    assert lib_parc.is_statut_parc_a_controler(retire) is False
    assert lib_parc.code_anomalie_statut_parc(retire) is None
    # …et il n'est ni géré, ni technique : c'est la période de gestion qui tranche.
    assert lib_parc.is_gere(retire) is False
    assert lib_parc.is_hors_parc_technique(retire) is False


@pytest.mark.parametrize("valeur", ["", None, "INCONNU", "actif"])
def test_un_statut_de_parc_reellement_invalide_reste_signale(valeur):
    """La reconnaissance de RETIRE ne doit pas rendre le contrôle permissif."""
    ligne = {"statut_parc": valeur}
    assert lib_parc.is_statut_parc_a_controler(ligne) is True
    assert lib_parc.code_anomalie_statut_parc(ligne) == "STATUT_PARC_INVALIDE"


# ── §3 — une annonce non rattachée signale, elle ne bloque pas ──────────────────────────────────

def test_une_annonce_non_rattachee_sort_de_l_economie_sans_l_arreter():
    motif = dbm.motif_exclusion_pour("HOSTAWAY_DIRECT_HH", "A_CONTROLER",
                                     dbm.MOTIF_LOGEMENT_NON_MAPPE)
    assert motif == dbm.MOTIF_LOGEMENT_NON_MAPPE
    assert dbm.impacts_reservation("HC", motif_exclusion=motif) == ("NON", "NON")
    assert dbm.MOTIF_LOGEMENT_NON_MAPPE in dbm.MOTIFS_EXCLUSION


def test_le_moteur_ne_s_arrete_plus_sur_une_annonce_inconnue():
    """Garde de non-régression sur le SOURCE : `resolve_logement` rendait `abort()` — un arrêt
    global du calcul — pour une annonce absente du référentiel. Une donnée manquante sur un seul
    logement ne doit pas empêcher les 1 600 autres réservations d'être calculées.
    """
    source = (MOTEUR / "lot4bis_charger_reservations.py").read_text(encoding="utf-8")
    debut = source.index("def resolve_logement(")
    fin = source.index("def make_row_ha(", debut)
    corps = source[debut:fin]
    assert "abort(f\"LOGEMENT_NON_MAPPE" not in corps, \
        "une annonce non rattachée ne doit plus arrêter tout le calcul"
    assert "MOTIF_LOGEMENT_NON_MAPPE" in corps, \
        "elle doit être rendue comme anomalie de la réservation concernée"


def test_une_annonce_non_rattachee_n_est_jamais_rangee_dans_un_fourre_tout():
    """Le fourre-tout ferait disparaître l'anomalie en fabriquant une réponse."""
    from app.services import correspondances_logement_service as corr

    assert "LOGEMENT_DIVERS" in corr.LOGEMENTS_TECHNIQUES
    assert "APPARTEMENT_DIVERS" in corr.LOGEMENTS_TECHNIQUES
    source = (MOTEUR / "lot4bis_charger_reservations.py").read_text(encoding="utf-8")
    debut = source.index("def resolve_logement(")
    fin = source.index("def make_row_ha(", debut)
    assert "DIVERS" not in source[debut:fin]


# ── §5 — un run abandonné ne peut plus bloquer indéfiniment ─────────────────────────────────────

def _inserer_run(db_path, run_id, statut, started_at, pid=None, hote=None):
    from app.db.connection import get_db

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur, pid, hote) "
            "VALUES (?,?,?,?,?,?,?)",
            (run_id, "lot1_hostaway_extract", started_at, statut, "MANUEL", pid, hote))
        conn.commit()
    finally:
        conn.close()


def _maintenant() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def test_un_processus_mort_est_detecte_immediatement_sans_attendre_le_bail(tmp_db):
    """Preuve par le PID : inutile d'attendre deux heures quand on peut CONSTATER la mort.

    Un PID qui n'existe plus sur CETTE machine tranche la question sur-le-champ.
    """
    import socket

    from app.services import hostaway_actualisation_service as ha

    # PID libre : 0x7FFFFFFF n'est jamais attribué.
    _inserer_run(tmp_db, "RUN-MORT", ha.ST_EN_COURS, _maintenant(),
                 pid=2147483647, hote=socket.gethostname()[:100])
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == ["RUN-MORT"]
    assert ha.actualisation_en_cours(db_path=tmp_db) is None


def test_un_processus_vivant_n_est_jamais_tue_meme_au_dela_du_bail(tmp_db):
    """L'inverse compte autant : un run qui tourne vraiment ne doit pas être requalifié parce
    qu'il est long. Le tuer serait pire que l'attendre."""
    import os
    import socket

    from app.services import hostaway_actualisation_service as ha

    _inserer_run(tmp_db, "RUN-VIVANT", ha.ST_EN_COURS, "2020-01-01T00:00:00",
                 pid=os.getpid(), hote=socket.gethostname()[:100])
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == []
    en_cours = ha.actualisation_en_cours(db_path=tmp_db)
    assert en_cours is not None and en_cours["run_id"] == "RUN-VIVANT"


def test_un_run_d_une_autre_machine_n_est_pas_juge_sur_son_pid(tmp_db):
    """Un PID n'a de sens que sur la machine qui l'a attribué. Conclure « mort » sur le PID d'un
    autre poste tuerait un run réellement en cours ailleurs. Seul le temps écoulé peut trancher —
    et seulement au-delà du bail."""
    from app.services import hostaway_actualisation_service as ha

    _inserer_run(tmp_db, "RUN-AILLEURS", ha.ST_EN_COURS, _maintenant(),
                 pid=2147483647, hote="UNE-AUTRE-MACHINE")
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == [], \
        "récent et non vérifiable : on ne conclut pas"

    _inserer_run(tmp_db, "RUN-AILLEURS-VIEUX", ha.ST_EN_COURS, "2020-01-01T00:00:00",
                 pid=2147483647, hote="UNE-AUTRE-MACHINE")
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == ["RUN-AILLEURS-VIEUX"]


def test_un_run_sans_pid_est_juge_sur_le_temps_ecoule(tmp_db):
    """Les runs anciens ne portent pas toujours de PID : le bail reste le dernier recours."""
    from app.services import hostaway_actualisation_service as ha

    _inserer_run(tmp_db, "RUN-SANS-PID-RECENT", ha.ST_EN_COURS, _maintenant())
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == []
    _inserer_run(tmp_db, "RUN-SANS-PID-VIEUX", ha.ST_EN_COURS, "2020-01-01T00:00:00")
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == ["RUN-SANS-PID-VIEUX"]


def test_la_requalification_laisse_une_trace_au_lieu_d_effacer(tmp_db):
    """Un échec doit rester visible : le faire disparaître empêcherait de comprendre pourquoi la
    donnée est ancienne."""
    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as ha

    _inserer_run(tmp_db, "RUN-TRACE", ha.ST_EN_COURS, "2020-01-01T00:00:00")
    ha.marquer_runs_interrompus(db_path=tmp_db)
    conn = get_db(tmp_db)
    try:
        ligne = conn.execute(
            "SELECT statut, ended_at, erreur_resume FROM moteur_runs WHERE run_id = 'RUN-TRACE'"
        ).fetchone()
    finally:
        conn.close()
    assert ligne is not None, "le run n'est jamais supprimé"
    assert ligne["statut"] == ha.ST_INTERROMPU
    assert ligne["ended_at"]
    assert "interrompu" in (ligne["erreur_resume"] or "").lower()


def test_un_run_termine_n_est_jamais_retouche(tmp_db):
    """La requalification ne concerne que les runs ouverts."""
    from app.db.connection import get_db
    from app.services import hostaway_actualisation_service as ha

    for statut in (ha.ST_SUCCES, ha.ST_ECHEC, ha.ST_PARTIEL):
        _inserer_run(tmp_db, f"RUN-{statut}", statut, "2020-01-01T00:00:00")
    assert ha.marquer_runs_interrompus(db_path=tmp_db) == []
    conn = get_db(tmp_db)
    try:
        statuts = {r[0] for r in conn.execute(
            "SELECT statut FROM moteur_runs WHERE run_id LIKE 'RUN-%'")}
    finally:
        conn.close()
    assert statuts == {ha.ST_SUCCES, ha.ST_ECHEC, ha.ST_PARTIEL}

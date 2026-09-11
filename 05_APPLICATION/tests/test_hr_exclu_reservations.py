"""§21 — `HR` a disparu de l'axe RÉSERVATIONS, l'exclusion est restée.

CE QUI A CHANGÉ, ET POURQUOI
`code_impact = 'HR'` disait « ni résultat réel, ni comptabilité ». Mais un code d'impact décrit
COMMENT une somme pèse sur l'économie ; il ne sait pas dire qu'une ligne en est absente. Les 125
réservations concernées sont des séjours du propriétaire dans son propre logement : une occupation
réelle, sans voyageur, sans encaissement, sans commission. Elles n'ont pas un impact neutre — elles
n'ont pas d'impact.

L'exclusion, elle, existait déjà : `statut_controle = EXCLU_RESULTAT`, vocabulaire partagé par
lot4bis, lot5, lot7 et la saisie HH depuis l'origine. `HR` n'en était qu'une seconde écriture,
redondante — lot4bis forçait d'ailleurs déjà l'impact à NON/NON dès que ce statut était posé, quel
que soit le code. Ce qui manquait était le MOTIF, jusque-là noyé dans un commentaire libre.

CE QUE CES TESTS PROTÈGENT
Deux choses opposées, et c'est le point :
  1. `HR` ne doit plus jamais réapparaître — ni en base, ni dans un moteur, ni par un réimport ;
  2. les 125 réservations doivent rester EXCLUES. Les « libérer » les transformerait en chiffre
     d'affaires, ce qui serait un faux résultat économique, pas un progrès.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))

import lib_db_moteur as dbm  # noqa: E402


@pytest.fixture()
def base(tmp_path):
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    return db


# ── Le vocabulaire ───────────────────────────────────────────────────────────

def test_l_exclusion_est_un_statut_pas_un_code_d_impact():
    assert dbm.STATUTS_EXCLUSION == ("EXCLU_RESULTAT", "EXCLU_LEGACY")
    assert "HR" not in dbm.MOTIFS_EXCLUSION
    assert dbm.MOTIF_OWNERSTAY == "OWNERSTAY"


@pytest.mark.parametrize("statut, attendu", [
    ("EXCLU_RESULTAT", True),
    ("EXCLU_LEGACY", True),
    ("VALIDE", False),
    ("A_CONTROLER", False),
    ("", False),
    (None, False),
])
def test_est_exclue_lit_le_statut(statut, attendu):
    assert dbm.est_exclue({"statut_controle": statut}) is attendu


def test_une_ligne_exclue_ne_pese_sur_rien():
    """Quel que soit le code d'impact, une ligne exclue rend NON/NON.

    C'est ce que `HR` produisait — d'où sa redondance. La différence est qu'il fallait penser à le
    poser ; le statut, lui, était déjà là."""
    assert dbm.impacts_reservation(None, motif_exclusion="OWNERSTAY") == ("NON", "NON")
    assert dbm.impacts_reservation(None, statut_controle="EXCLU_RESULTAT") == ("NON", "NON")
    # Même un code IC ne rattrape pas une ligne exclue : le statut prime.
    assert dbm.impacts_reservation("IC", statut_controle="EXCLU_LEGACY") == ("NON", "NON")


def test_une_ligne_normale_derive_de_son_code():
    assert dbm.impacts_reservation("IC") == ("OUI", "OUI")
    assert dbm.impacts_reservation("HC") == ("OUI", "NON")


@pytest.mark.parametrize("code", ["HR", "XX", "", None])
def test_un_code_inconnu_ressort_a_controler_jamais_neutralise(code):
    """`HR` n'est plus un code : il est traité comme n'importe quelle valeur inconnue.

    Le rendre « neutre » serait le réimplémenter sous le capot — exactement ce que la décision
    interdit. Il devient VISIBLE."""
    assert dbm.impacts_reservation(code) == ("A_CONTROLER", "A_CONTROLER")


def test_le_motif_se_deduit_de_ce_que_la_ligne_porte_deja():
    """Jamais inventé : le motif vient de `source` / `code_anomalie`, déjà présents."""
    assert dbm.motif_exclusion_pour("OWNERSTAY_EXCLU", "EXCLU_RESULTAT") == "OWNERSTAY"
    assert dbm.motif_exclusion_pour("HORS_PARC_TECHNIQUE", "EXCLU_RESULTAT") == "HORS_PARC_TECHNIQUE"
    assert dbm.motif_exclusion_pour(None, "EXCLU_LEGACY",
                                   "LEGACY_SANS_ARCHIVE_ORIGINE") == "LEGACY_SANS_ARCHIVE_ORIGINE"
    # Une ligne NON exclue n'a pas de motif : NULL se lit « participe au calcul ».
    assert dbm.motif_exclusion_pour("HOSTAWAY_AIRBNB", "VALIDE") is None


# ── La base ──────────────────────────────────────────────────────────────────

def test_une_base_neuve_ne_contient_aucun_HR(base):
    conn = sqlite3.connect(str(base))
    try:
        for table, colonne in (("reservations_resolues", "code_impact"),
                               ("reservations_calculees", "code_impact"),
                               ("ref_codes_impact", "code_impact"),
                               ("ref_types_flux", "code_impact_defaut")):
            n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {colonne}='HR'").fetchone()[0]
            assert n == 0, f"{table}.{colonne} porte encore HR"
    finally:
        conn.close()


def test_la_colonne_motif_exclusion_existe_des_la_base_vierge(base):
    conn = sqlite3.connect(str(base))
    try:
        for table in ("reservations_resolues", "reservations_calculees"):
            colonnes = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            assert "motif_exclusion" in colonnes, table
    finally:
        conn.close()


def test_la_migration_convertit_sans_rien_perdre(tmp_path):
    """0079 sur une base portant des lignes HR : aucune perte, motif posé, impact inchangé.

    Le risque exact que ce test écarte : « migrer » en perdant des lignes, ou en changeant leur
    effet économique. Les compteurs et les drapeaux d'impact doivent être identiques après."""
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        # On repose délibérément une ligne « à l'ancienne », telle qu'un import legacy la créerait.
        conn.execute(
            "INSERT INTO reservations_datasets (dataset_id, etape, statut)"
            " VALUES ('DS-TEST','RESOLUES','ACTIF')")
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, mois,"
            " logement_id, source, canal, montant_retenu, code_impact, impact_resultat_reel,"
            " impact_resultat_comptable, statut_controle, niveau_anomalie)"
            " VALUES ('DS-TEST','RES-LEGACY-1','2026-04','LOG_X','OWNERSTAY_EXCLU','OWNERSTAY',"
            "0,'HR','NON','NON','EXCLU_RESULTAT','INFO')")
        conn.execute("UPDATE reservations_resolues SET motif_exclusion = NULL")
        conn.commit()
        avant = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(montant_retenu),0) FROM reservations_resolues").fetchone()
    finally:
        conn.close()

    # Rejouer la migration ne suffit pas (elle est déjà enregistrée) : on applique son contenu.
    from app.db.connection import MIGRATIONS_DIR
    sql = (MIGRATIONS_DIR / "0079_reservations_exclusion_explicite.sql").read_text(encoding="utf-8")
    # Les lignes de commentaire sont retirées AVANT le découpage : elles contiennent des apostrophes
    # et de la ponctuation qui feraient éclater un `split(";")` naïf au mauvais endroit.
    sans_commentaires = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    instructions = [i for i in sans_commentaires.split(";")
                    if i.strip() and "ALTER TABLE" not in i and "CREATE INDEX" not in i]
    conn = sqlite3.connect(str(db))
    try:
        for i in instructions:
            conn.execute(i)
        conn.commit()
        apres = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(montant_retenu),0) FROM reservations_resolues").fetchone()
        ligne = conn.execute(
            "SELECT code_impact, motif_exclusion, impact_resultat_reel, statut_controle"
            " FROM reservations_resolues WHERE reservation_calc_id='RES-LEGACY-1'").fetchone()
    finally:
        conn.close()

    assert apres == avant, "aucune ligne, aucun euro ne doit bouger"
    assert ligne[0] is None, "HR doit avoir disparu"
    assert ligne[1] == "OWNERSTAY", "le motif doit être posé"
    assert ligne[2] == "NON", "l'effet d'exclusion doit être conservé"
    assert ligne[3] == "EXCLU_RESULTAT", "le statut ne change pas"


def test_un_reimport_du_referentiel_ne_ressuscite_pas_HR():
    """Le classeur REF_Setup contient toujours `HR` : le réimporter le remettrait en base.

    La ligne est ÉCARTÉE et SIGNALÉE, jamais corrigée en silence — corriger reviendrait à décider
    à la place de l'utilisateur ce que `HR` devient."""
    from app.services import ref_setup_import_service as imp

    assert "HR" in imp.LIGNES_RETIREES["ref_codes_impact"]
    assert "HR" in imp.VALEURS_COLONNE_RETIREES[("ref_types_flux", "code_impact_defaut")]

    class _Feuille:
        onglet, table, cle = "REF_Codes_Impact", "ref_codes_impact", "code_impact"
        colonnes = ("code_impact", "libelle")

    avert: list[str] = []
    gardees = imp._ecarter_valeurs_retirees(
        _Feuille(), [{"code_impact": "IC", "libelle": "Intra"},
                     {"code_impact": "HR", "libelle": "Hors resultat"}], avert)
    assert [l["code_impact"] for l in gardees] == ["IC"]
    assert avert and "HR" in avert[0]


# ── Les moteurs ──────────────────────────────────────────────────────────────

def _code_seul(source: str) -> str:
    """Source privé de ses commentaires ET de ses docstrings.

    Les docstrings ont le droit de nommer `HR` : elles expliquent la migration, ce que la mission
    autorise explicitement (« les documents historiques peuvent mentionner le terme »). Ce qui est
    interdit est de le PRODUIRE. Viser le texte brut forcerait à effacer l'explication pour faire
    passer un test — exactement le mauvais réflexe."""
    import ast

    lignes = source.splitlines()
    try:
        arbre = ast.parse(source)
    except SyntaxError:
        return source
    a_retirer: set[int] = set()
    for noeud in ast.walk(arbre):
        corps = getattr(noeud, "body", None)
        if not corps or not isinstance(corps, list):
            continue
        premier = corps[0]
        if (isinstance(premier, ast.Expr) and isinstance(premier.value, ast.Constant)
                and isinstance(premier.value.value, str)):
            a_retirer.update(range(premier.lineno - 1, premier.end_lineno))
    return "\n".join(l for i, l in enumerate(lignes)
                     if i not in a_retirer and not l.lstrip().startswith("#"))


def test_aucun_moteur_ne_produit_plus_HR():
    """STRUCTUREL — `HR` ne doit plus être écrit comme valeur par aucun moteur."""
    coupables = []
    for chemin in _TRAVAIL.glob("*.py"):
        code = _code_seul(chemin.read_text(encoding="utf-8", errors="replace"))
        if '"HR"' in code or "'HR'" in code:
            coupables.append(chemin.name)
    assert coupables == [], coupables


def test_le_service_de_flux_ne_connait_plus_HR():
    from app.services import flux_unifie_service as fx

    assert "HR" not in fx._IMPACT_FLAGS
    # Le défaut rendait déjà NON/NON/NON : c'est ce qui rendait le code redondant.
    assert fx._impact_flags("HR") == ("NON", "NON", "NON")
    assert fx._impact_flags("IC") == ("OUI", "OUI", "NON")


def test_la_saisie_hh_ne_propose_plus_HR(base):
    """L'UI lit `ref_codes_impact` : supprimer la ligne suffit à la retirer des formulaires."""
    from app.readers.ref_setup_hh_reader import get_codes_impact

    _, lignes = get_codes_impact(db_path=base)
    assert "HR" not in {str(r.get("code_impact", "")).strip() for r in lignes}

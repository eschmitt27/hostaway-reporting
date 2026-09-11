"""§5/§6/§19 — une charge ménage traverse la chaîne et se ventile, prouvé par exécution.

Tests MÉTIER et non structurels : lot6f est lancé pour de vrai sur une base construite pour le
cas, et ce sont les montants écrits dans `menages_cout_complet` qui sont vérifiés. Une assertion
sur le texte du source ne prouverait pas qu'un euro arrive au bon endroit.

Décor : trois logements du même type, un ménage interne chacun, donc trois poids égaux. La
ventilation est alors lisible à l'œil et un écart d'un centime se voit.

Ce que ces tests figent :
  - 300 € sur 3 logements = 100 / 100 / 100, et la somme ventilée vaut le pool ;
  - 100 € sur 3 est DÉTERMINISTE (33.33 ×3) — le centime résiduel est constaté, pas masqué ;
  - `affectable_menage != OUI` n'entre dans aucun pool ;
  - une charge ANNULÉE n'est pas un coût ;
  - une charge d'un autre mois ne fuit pas dans le mois calculé ;
  - aucun double comptage : une charge alimente UN pool, une fois.

Aucun classeur n'est lu ni écrit : le moteur tourne depuis une racine isolée, avec `--sans-excel`.
"""
from __future__ import annotations

import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
LOT6F = _TRAVAIL / "lot6f_cout_complet_menages.py"

pytestmark = pytest.mark.skipif(not LOT6F.exists(), reason="lot6f absent")

MOIS = "2026-07"
LOGEMENTS = ("LOG_T1", "LOG_T2", "LOG_T3")
#: Coût standard identique pour les trois : poids égaux, donc tiers exacts.
COUT_STANDARD = 50.0
#: Catégorie qui tombe dans le pool COURSES (ni CHG_004 consommables, ni CHG_018 autres).
CAT_COURSES = "CHG_002"


@pytest.fixture(scope="module")
def racine_isolee(tmp_path_factory) -> Path:
    """Racine ne contenant que les moteurs — lot6f y déduit un ROOT sans aucun classeur."""
    racine = tmp_path_factory.mktemp("lot6f_ventilation")
    (racine / "02_TRAVAIL").mkdir()
    for module in _TRAVAIL.glob("*.py"):
        shutil.copy2(module, racine / "02_TRAVAIL" / module.name)
    return racine


def _base(tmp_path: Path) -> Path:
    """Base migrée portant le décor : 3 logements du même type, 1 ménage interne chacun."""
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("INSERT INTO ref_types_logements (type_logement_id, type_logement, import_id)"
                     " VALUES ('TYP_A','Studio','TEST')")
        conn.execute("INSERT INTO ref_intervenants (intervenant_id, nom_intervenant,"
                     " type_intervenant, import_id) VALUES ('INT_A','Alex','INTERNE','TEST')")
        conn.execute("INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id,"
                     " cout_standard_menage, actif, import_id)"
                     " VALUES ('CS_A','TYP_A',?,'OUI','TEST')", (COUT_STANDARD,))
        # Forfait interne : le coût direct n'est pas l'objet du test, mais il doit être résoluble
        # sinon les lignes remontent en COUT_INTERNE_* et le calcul devient illisible.
        conn.execute("INSERT INTO ref_couts_menage_interne (cout_menage_interne_id,"
                     " type_logement_id, montant_interne_standard, actif, import_id)"
                     " VALUES ('CI_A','TYP_A',40.0,'OUI','TEST')")
        for i, lid in enumerate(LOGEMENTS, start=1):
            conn.execute("INSERT INTO ref_logements (logement_id, nom_logement_officiel,"
                         " type_logement_id, actif, import_id) VALUES (?,?,?,'OUI','TEST')",
                         (lid, f"Logement {i}", "TYP_A"))
            conn.execute(
                "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id,"
                " nom_intervenant, type_intervenant, nb_menages, statut_controle, date_calcul)"
                " VALUES (?,?,?,'Alex','INTERNE',1,'VALIDE','2026-09-01T00:00:00Z')",
                (MOIS, lid, "INT_A"))
        conn.commit()
    finally:
        conn.close()
    return db


def _charge(db: Path, charge_id: str, montant: float, *, affectable="OUI", statut="ACTIVE",
            date_charge=f"{MOIS}-15", categorie=CAT_COURSES) -> None:
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO charges (charge_id, date_charge, mois, montant, categorie_charge_id,"
            " affectable_menage, statut, date_creation)"
            " VALUES (?,?,?,?,?,?,?,'2026-09-01T00:00:00Z')",
            (charge_id, date_charge, str(date_charge)[:7], montant, categorie, affectable, statut))
        conn.commit()
    finally:
        conn.close()


def _lancer(racine: Path, db: Path, mois: str = MOIS):
    travail = racine / "02_TRAVAIL"
    proc = subprocess.run(
        [sys.executable, str(travail / LOT6F.name), "--db", str(db), "--mois", mois,
         "--sans-excel"],
        cwd=str(travail), capture_output=True, text=True, timeout=600,
        env={**_env_propre(), "PYTHONIOENCODING": "utf-8"})
    assert proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]
    return proc


def _env_propre() -> dict:
    import os

    env = dict(os.environ)
    for cle in ("PILOTAGE_DB_PATH", "APP_DATA_DIR"):
        env.pop(cle, None)
    return env


def _quotes_parts(db: Path, colonne: str = "quote_part_courses") -> dict[str, float]:
    conn = sqlite3.connect(str(db))
    try:
        return {lid: round(qp or 0.0, 2) for lid, qp in conn.execute(
            f"SELECT logement_id, {colonne} FROM menages_cout_complet WHERE mois=? ORDER BY"
            " logement_id", (MOIS,))}
    finally:
        conn.close()


# ── §5 — la ventilation nominale ─────────────────────────────────────────────

def test_300_euros_sur_3_logements_font_100_100_100(racine_isolee, tmp_path):
    """Le cas de référence de la mission, vérifié sur la donnée écrite, pas sur la formule."""
    db = _base(tmp_path)
    _charge(db, "CHG-300", 300.0)
    _lancer(racine_isolee, db)

    parts = _quotes_parts(db)
    assert parts == {"LOG_T1": 100.0, "LOG_T2": 100.0, "LOG_T3": 100.0}, parts
    assert round(sum(parts.values()), 2) == 300.0, "la somme ventilée doit valoir le pool"


def test_la_charge_n_est_comptee_qu_une_fois(racine_isolee, tmp_path):
    """Anti double comptage : une charge alimente UN pool, et sa ventilation totale vaut son montant.

    Les autres pools restent à zéro — une charge de courses ne doit pas apparaître aussi en
    consommables ou en autres charges, ce qui doublerait son poids économique."""
    db = _base(tmp_path)
    _charge(db, "CHG-300", 300.0)
    _lancer(racine_isolee, db)

    total_par_pool = {}
    for colonne in ("quote_part_courses", "quote_part_consommables",
                    "quote_part_autres_charges_menage", "quote_part_local", "quote_part_lavage"):
        total_par_pool[colonne] = round(sum(_quotes_parts(db, colonne).values()), 2)

    assert total_par_pool["quote_part_courses"] == 300.0
    autres = {k: v for k, v in total_par_pool.items() if k != "quote_part_courses"}
    assert set(autres.values()) == {0.0}, f"la charge a fui dans un autre pool : {autres}"


@pytest.mark.parametrize("categorie, colonne", [
    ("CHG_002", "quote_part_courses"),
    ("CHG_004", "quote_part_consommables"),
    ("CHG_018", "quote_part_autres_charges_menage"),
])
def test_chaque_categorie_alimente_son_pool(racine_isolee, tmp_path, categorie, colonne):
    """§19 — le routage catégorie → pool est vérifié pour chacune des trois destinations."""
    db = _base(tmp_path)
    _charge(db, f"CHG-{categorie}", 300.0, categorie=categorie)
    _lancer(racine_isolee, db)

    assert _quotes_parts(db, colonne) == {"LOG_T1": 100.0, "LOG_T2": 100.0, "LOG_T3": 100.0}


# ── §6 — les centimes ────────────────────────────────────────────────────────

def test_100_euros_sur_3_donnent_des_centimes_deterministes(racine_isolee, tmp_path):
    """100 / 3 ne tombe pas juste : le résultat doit être STABLE et le résidu VISIBLE.

    Le moteur arrondit chaque quote-part à 2 décimales (`round(pool × poids / Σ poids, 2)`), donc
    33.33 trois fois, soit 99.99 ventilés pour 100.00 de pool : **un centime n'est attribué à
    personne**.

    C'est constaté ici, pas corrigé : ajouter un rattrapage changerait la règle de répartition
    (quel logement reçoit le centime ?) — un arbitrage métier, signalé dans
    ARBRE_CHARGES_CONSEQUENCES.md. Ce que le test garantit, c'est qu'il n'y a ni instabilité, ni
    dérive silencieuse : le résidu est d'un centime, jamais davantage."""
    db = _base(tmp_path)
    _charge(db, "CHG-100", 100.0)
    _lancer(racine_isolee, db)

    parts = _quotes_parts(db)
    assert parts == {"LOG_T1": 33.33, "LOG_T2": 33.33, "LOG_T3": 33.33}, parts

    residu = round(100.0 - sum(parts.values()), 2)
    assert residu == 0.01, f"résidu attendu d'un centime, obtenu {residu}"


def test_la_ventilation_est_reproductible(racine_isolee, tmp_path):
    """Deux exécutions sur la même donnée rendent les mêmes centimes.

    lot6f remplace intégralement le mois à chaque run ; si la répartition dépendait de l'ordre de
    lecture, le second run ne rendrait pas la même chose."""
    db = _base(tmp_path)
    _charge(db, "CHG-100", 100.0)
    _lancer(racine_isolee, db)
    premier = _quotes_parts(db)
    _lancer(racine_isolee, db)
    second = _quotes_parts(db)

    assert premier == second, f"{premier} != {second}"


# ── Exclusions ───────────────────────────────────────────────────────────────

def test_une_charge_non_affectable_reste_hors_des_pools(racine_isolee, tmp_path):
    """`affectable_menage = NON` : la dépense existe, mais elle n'est pas un coût de ménage.

    C'est le drapeau qui décide, jamais la catégorie — deux charges CHG_002 identiques doivent
    pouvoir être, l'une affectable, l'autre non."""
    db = _base(tmp_path)
    _charge(db, "CHG-OUI", 300.0, affectable="OUI")
    _charge(db, "CHG-NON", 999.0, affectable="NON")
    _lancer(racine_isolee, db)

    parts = _quotes_parts(db)
    assert round(sum(parts.values()), 2) == 300.0, \
        f"seule la charge affectable doit être ventilée ; obtenu {parts}"


def test_une_charge_annulee_n_est_pas_un_cout(racine_isolee, tmp_path):
    """Filtre `statut = 'ACTIVE'` : une charge annulée ne pèse plus rien.

    Ce filtre n'avait pas d'équivalent dans le classeur — qui ne portait pas de charge annulée."""
    db = _base(tmp_path)
    _charge(db, "CHG-ANN", 300.0, statut="ANNULEE")
    _lancer(racine_isolee, db)

    parts = _quotes_parts(db)
    assert round(sum(parts.values()), 2) == 0.0, parts


def test_une_charge_d_un_autre_mois_ne_fuit_pas(racine_isolee, tmp_path):
    """Le mois vient de `date_charge`, jamais d'une colonne `mois` pré-calculée.

    Règle héritée du classeur, où `mois` était une formule dont le cache pouvait être vide : la
    charge disparaissait alors des pools sans un mot. On garde la dérivation, donc l'invariant."""
    db = _base(tmp_path)
    _charge(db, "CHG-JUIN", 300.0, date_charge="2026-06-15")
    _lancer(racine_isolee, db)

    assert round(sum(_quotes_parts(db).values()), 2) == 0.0


def test_le_mois_derive_de_la_date_pas_de_la_colonne(racine_isolee, tmp_path):
    """Une colonne `mois` incohérente avec `date_charge` ne doit pas décider du rattachement.

    Cas construit : `date_charge` en juillet, colonne `mois` en juin. La charge appartient à
    juillet."""
    db = _base(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO charges (charge_id, date_charge, mois, montant, categorie_charge_id,"
            " affectable_menage, statut, date_creation)"
            " VALUES ('CHG-INCOH','2026-07-15','2026-06',300.0,?,'OUI','ACTIVE',"
            "'2026-09-01T00:00:00Z')", (CAT_COURSES,))
        conn.commit()
    finally:
        conn.close()
    _lancer(racine_isolee, db)

    assert round(sum(_quotes_parts(db).values()), 2) == 300.0, \
        "la date de la charge doit primer sur la colonne mois"

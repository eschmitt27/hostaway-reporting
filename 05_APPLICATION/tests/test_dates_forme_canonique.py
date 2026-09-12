"""§76 — une colonne de date ne contient qu'une seule forme de date.

POURQUOI C'EST GRAVE, ET PAS COSMÉTIQUE
En SQLite, les dates sont du TEXTE et se comparent caractère par caractère. Donc :

    '11/09/2026' < '2026-09-12'   →   VRAI

Une date écrite à la française se trie AVANT toutes les dates ISO, quelle que soit l'année. Un tri
chronologique, un filtre « depuis le 1er septembre », un calcul d'ancienneté, un rapprochement par
date : tout se trompe sur ces lignes — silencieusement, puisque la valeur reste lisible à l'œil.

La base réelle en portait quatre, dans trois tables, venues de champs de saisie en texte libre qui
n'imposaient rien. Le balayage ci-dessous est un INVARIANT : il vaut pour toutes les colonnes de
date de toutes les tables, présentes et à venir, quel que soit le chemin d'écriture.
"""
from __future__ import annotations

import re

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import dates_service as dates


# ── Le normaliseur ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("entree,attendu", [
    ("2026-09-12", "2026-09-12"),
    ("2026-09-12T22:31:00Z", "2026-09-12"),
    ("11/09/2026", "2026-09-11"),
    ("1/9/2026", "2026-09-01"),
    ("11.09.2026", "2026-09-11"),
    ("11-09-2026", "2026-09-11"),
    ("20260911", "2026-09-11"),
    ("  2026-09-12  ", "2026-09-12"),
])
def test_normalisation(entree, attendu):
    assert dates.normaliser(entree) == attendu


@pytest.mark.parametrize("entree", [
    "", "   ", None, "hier", "septembre 2026", "2026-09", "09/2026",
    "31/02/2026",   # grammaticalement correcte, impossible dans le calendrier
    "2026-13-01",
    "2026-02-30",
])
def test_une_valeur_qui_n_est_pas_une_date_ressort_vide(entree):
    """Refuser plutôt qu'inventer : une date fausse en base ne se voit pas, et contamine les tris."""
    assert dates.normaliser(entree) == ""


def test_exiger_leve_plutot_que_stocker_une_valeur_douteuse():
    with pytest.raises(ValueError, match="date_facture"):
        dates.exiger("le 11 septembre", champ="date_facture")


def test_est_iso_distingue_la_forme_de_stockage():
    assert dates.est_iso("2026-09-12") is True
    assert dates.est_iso("11/09/2026") is False


def test_mois_extrait_la_periode():
    assert dates.mois("11/09/2026") == "2026-09"
    assert dates.mois("pas une date") == ""


# ── Le piège que ces dates tendaient ────────────────────────────────────────────────────────────

def test_une_date_francaise_se_trie_avant_toutes_les_dates_iso():
    """La démonstration du problème, en une ligne : c'est une comparaison de TEXTE."""
    assert "11/09/2026" < "2020-01-01", \
        "une date française passe avant 2020 : voilà pourquoi elle ne peut pas rester en base"


def test_le_tri_sql_redevient_chronologique_apres_normalisation(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute("CREATE TABLE essai (d TEXT)")
        for brut in ("11/09/2026", "2026-08-01", "2026-09-20"):
            conn.execute("INSERT INTO essai (d) VALUES (?)", (dates.normaliser(brut),))
        conn.commit()
        ordre = [r[0] for r in conn.execute("SELECT d FROM essai ORDER BY d").fetchall()]
    finally:
        conn.close()
    assert ordre == ["2026-08-01", "2026-09-11", "2026-09-20"]


# ── L'INVARIANT : aucune colonne de date ne porte de valeur non ISO ─────────────────────────────

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _colonnes_de_date(conn) -> list[tuple[str, str]]:
    """Toutes les colonnes qui portent une DATE, reconnues par leur nom.

    `date_creation`, `extrait_le` et consorts portent un horodatage ISO complet : ils commencent
    aussi par `AAAA-MM-JJ`, donc la même règle s'applique sans distinction à faire.
    """
    out = []
    for (table,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
        for col in conn.execute(f"PRAGMA table_info({table})").fetchall():
            nom = col[1]
            if nom.startswith("date_") or nom.endswith("_date") or nom == "date":
                out.append((table, nom))
    return out


def test_aucune_colonne_de_date_ne_contient_de_valeur_non_iso(tmp_path):
    """Invariant de schéma, vérifié sur une base fraîchement migrée.

    Il vaut aussi comme contrôle à passer sur la base réelle : le même balayage y a trouvé les
    quatre valeurs que la migration 0084 corrige.
    """
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        fautives = []
        for table, col in _colonnes_de_date(conn):
            for r in conn.execute(
                    f"SELECT DISTINCT {col} v FROM {table} "
                    f"WHERE {col} IS NOT NULL AND TRIM({col}) <> ''").fetchall():
                if not _ISO.match(str(r["v"])):
                    fautives.append(f"{table}.{col} = {r['v']!r}")
    finally:
        conn.close()
    assert not fautives, "dates hors forme ISO : " + ", ".join(fautives)


def test_la_migration_0084_convertit_sans_rien_inventer(tmp_path):
    """La conversion est une réécriture position par position, pas une interprétation."""
    import pathlib
    import sqlite3

    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        for ident, brut in (("IMP-TEST1", "15/08/2026"), ("IMP-TEST2", "2026-08-12")):
            conn.execute(
                "INSERT INTO imputations_airbnb (imputation_airbnb_id, proprietaire_id, mois, "
                "montant_impute, date_imputation) VALUES (?,?,?,?,?)",
                (ident, "PROP_0001", "2026-08", 50.0, brut))
        conn.commit()
    finally:
        conn.close()

    sql = next(pathlib.Path("app/db/migrations").glob("0084_*.sql")).read_text(encoding="utf-8")
    conn = sqlite3.connect(db)
    try:
        conn.executescript(sql)
        conn.commit()
        obtenu = dict(conn.execute(
            "SELECT imputation_airbnb_id, date_imputation FROM imputations_airbnb "
            "WHERE imputation_airbnb_id LIKE 'IMP-TEST%'").fetchall())
    finally:
        conn.close()
    assert obtenu["IMP-TEST1"] == "2026-08-15", "converti"
    assert obtenu["IMP-TEST2"] == "2026-08-12", "déjà ISO : laissé intact"

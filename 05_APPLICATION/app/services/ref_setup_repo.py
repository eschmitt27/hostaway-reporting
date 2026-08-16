"""Lecture du référentiel Setup depuis SQLite.

Point d'entrée unique des services métier vers le référentiel une fois l'import fait. Il rend des
dictionnaires de CHAÎNES, exactement comme `readers/csv_reader.py` : les appelants convertissent au
moment de l'usage. C'est délibéré — voir la migration 0029 pour le raisonnement complet.

Ce module ne décide rien. `est_disponible()` dit si le référentiel a été importé ; il ne se
substitue jamais silencieusement à Excel, et il ne complète jamais une donnée absente.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import ref_setup_catalogue as cat


def est_disponible(*, db_path=None) -> bool:
    """Vrai si les tables existent ET portent au moins un import abouti.

    Les deux conditions comptent : un schéma créé par la migration mais jamais alimenté n'est pas
    un référentiel disponible, c'est un référentiel vide. Confondre les deux ferait lire « aucun
    logement » là où il faut lire « référentiel non importé ».
    """
    conn = get_db(db_path)
    try:
        noms = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if cat.TABLE_IMPORTS not in noms:
            return False
        return conn.execute(
            f"SELECT COUNT(*) FROM {cat.TABLE_IMPORTS} WHERE statut='IMPORTE'").fetchone()[0] > 0
    finally:
        conn.close()


def lire_table(table: str, *, db_path=None) -> list[dict[str, str]]:
    """Toutes les lignes d'une table du catalogue, colonnes dans l'ordre du classeur."""
    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        raise KeyError(f"Table hors catalogue : {table}")
    conn = get_db(db_path)
    try:
        cols = ", ".join(feuille.colonnes)
        rows = conn.execute(f"SELECT {cols} FROM {feuille.table}").fetchall()
        return [dict(zip(feuille.colonnes, [("" if v is None else str(v)) for v in r]))
                for r in rows]
    finally:
        conn.close()


def lire_onglet(onglet: str, *, db_path=None) -> list[dict[str, str]]:
    """Même lecture, désignée par le nom d'onglet Excel — utile pendant la transition."""
    feuille = cat.PAR_ONGLET.get(onglet)
    if feuille is None:
        raise KeyError(f"Onglet hors catalogue : {onglet}")
    return lire_table(feuille.table, db_path=db_path)


def lire_par_cle(table: str, cle: str, *, db_path=None) -> dict[str, str] | None:
    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        raise KeyError(f"Table hors catalogue : {table}")
    conn = get_db(db_path)
    try:
        cols = ", ".join(feuille.colonnes)
        r = conn.execute(
            f"SELECT {cols} FROM {feuille.table} WHERE {feuille.cle} = ?", (cle,)).fetchone()
        if r is None:
            return None
        return dict(zip(feuille.colonnes, [("" if v is None else str(v)) for v in r]))
    finally:
        conn.close()


def compter(*, db_path=None) -> dict[str, int]:
    """{table: nombre de lignes} pour les 28 tables. Table absente → 0, jamais d'exception."""
    conn = get_db(db_path)
    try:
        noms = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        out: dict[str, int] = {}
        for f in cat.FEUILLES:
            out[f.table] = (conn.execute(f"SELECT COUNT(*) FROM {f.table}").fetchone()[0]
                            if f.table in noms else 0)
        return out
    finally:
        conn.close()


def etat(*, db_path=None) -> dict[str, Any]:
    """État complet du référentiel, tel qu'un écran doit pouvoir l'afficher sans rien recalculer."""
    from app.services import ref_setup_import_service as imp
    dernier = imp.dernier_import(db_path=db_path)
    compteurs = compter(db_path=db_path)
    return {
        "disponible": dernier is not None,
        "dernier_import": dernier,
        "compteurs": compteurs,
        "nb_lignes": sum(compteurs.values()),
        "feuilles": [{"onglet": f.onglet, "table": f.table, "cle": f.cle,
                      "nb_colonnes": len(f.colonnes), "nb_lignes": compteurs.get(f.table, 0)}
                     for f in cat.FEUILLES],
    }

"""§45 — un numéro se stocke sous une forme, se lit sous une autre, et ne s'invente jamais.

Le référentiel portait deux écritures du même genre de numéro selon la table d'origine
(`0610190367` côté propriétaires, `33775793253` côté intervenants). Aucune ne se lit, et deux
numéros identiques écrits différemment ne se rapprochaient pas.

La règle : STOCKER en E.164 (`+33610190367`), AFFICHER en groupes de deux (`06 10 19 03 67`).
Ce que ces tests protègent surtout, c'est le refus : un numéro incomplet ou étranger n'est jamais
complété, parce qu'un numéro inventé finit par être appelé.
"""
from __future__ import annotations

import pytest

from app.services import telephone_service as tel


# ── Toutes les écritures d'un même numéro convergent ────────────────────────────────────────────

@pytest.mark.parametrize("ecriture", [
    "0610190367", "06 10 19 03 67", "06.10.19.03.67", "06-10-19-03-67",
    "+33610190367", "+33 6 10 19 03 67", "0033610190367", "33610190367",
    "+33 (0)6 10 19 03 67", "  0610190367  ",
])
def test_toutes_les_ecritures_donnent_le_meme_numero(ecriture):
    """C'est la propriété qui rend la comparaison et le dédoublonnage possibles."""
    assert tel.normaliser(ecriture)["e164"] == "+33610190367"


def test_la_forme_lisible_est_en_groupes_de_deux():
    assert tel.normaliser("+33610190367")["affichage"] == "06 10 19 03 67"
    assert tel.normaliser("33775793253")["affichage"] == "07 75 79 32 53"


def test_les_deux_formats_reels_du_referentiel_se_rejoignent():
    """`0610190367` (propriétaires) et `33610190367` (intervenants) : le même numéro."""
    assert tel.normaliser("0610190367")["e164"] == tel.normaliser("33610190367")["e164"]


# ── Ce qui ne doit JAMAIS être inventé ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("valeur", [
    "", "   ", None,
    "06 10 19",          # incomplet — le compléter produirait un numéro qu'on appellerait
    "0810",              # trop court
    "abcdef",
    "12345678901234",    # trop long, aucun indicatif reconnaissable
])
def test_un_numero_illisible_reste_brut_et_signale(valeur):
    r = tel.normaliser(valeur)
    assert r["valide"] is False
    assert r["e164"] == "", "aucun indicatif n'est ajouté à un numéro qu'on ne sait pas lire"
    assert r["affichage"] == (str(valeur).strip() if valeur else "")


def test_un_numero_etranger_n_est_pas_francise():
    """`+1 415 555 0132` n'est pas un numéro français : on ne lui applique pas le format français."""
    r = tel.normaliser("+1 415 555 0132")
    assert r["valide"] is False
    assert r["e164"] == "+14155550132", "la ponctuation part, l'indicatif reste celui du numéro"
    assert r["affichage"] == "+1 415 555 0132"


def test_un_numero_francais_incomplet_n_est_pas_complete():
    assert tel.normaliser("+336101903")["valide"] is False


# ── Les points d'entrée employés par les écrans et le PDF ───────────────────────────────────────

def test_afficher_rend_la_forme_lisible():
    assert tel.afficher("+33610190367") == "06 10 19 03 67"


def test_afficher_ne_masque_pas_une_valeur_non_interpretee():
    """Mieux vaut montrer ce qui est en base que rien du tout."""
    assert tel.afficher("poste 42") == "poste 42"


def test_e164_refuse_de_rendre_une_valeur_fausse():
    assert tel.e164("06 10 19") == ""


# ── La migration SQL et le service disent la même chose ─────────────────────────────────────────

def test_migration_et_service_concordent(tmp_path):
    """Deux implémentations de la même règle : si elles divergent, l'une des deux est fausse."""
    import sqlite3

    from app.db.connection import apply_migrations, get_db

    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        valeurs = {"PROP_T1": "0610190367", "PROP_T2": "33775793253",
                   "PROP_T3": "poste interne", "PROP_T4": ""}
        for pid, num in valeurs.items():
            conn.execute(
                "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, telephone, "
                "actif, import_id) VALUES (?,?,?,?,?)", (pid, pid, num, "OUI", "TEST"))
        conn.commit()
    finally:
        conn.close()

    # La migration 0083 est déjà passée ; on rejoue son SQL sur les lignes insérées après coup.
    chemin = next(p for p in
                  (__import__("pathlib").Path("app/db/migrations")).glob("0083_*.sql"))
    conn = sqlite3.connect(db)
    try:
        conn.executescript(chemin.read_text(encoding="utf-8"))
        conn.commit()
        obtenus = dict(conn.execute(
            "SELECT proprietaire_id, telephone FROM ref_proprietaires "
            "WHERE proprietaire_id LIKE 'PROP_T%'").fetchall())
    finally:
        conn.close()

    for pid, brut in valeurs.items():
        attendu = tel.normaliser(brut)["e164"] or brut
        assert obtenus[pid] == attendu, f"{pid} : migration={obtenus[pid]!r} service={attendu!r}"

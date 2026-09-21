"""Qui fait foi, du classeur ou de l'application — et ce que `import_id` dit vraiment.

LA RÈGLE, telle que `ref_setup_import_service` la documente et l'applique :

    REF_Setup.xlsm  = source d'INITIALISATION. Elle amorce le référentiel, et continue d'alimenter
                      toute ligne que l'application n'a pas encore prise en gestion.
    SQLite + écran  = source CANONIQUE dès qu'une ligne a été créée ou modifiée dans
                      l'application. Elle porte alors `import_id = SAISIE_APPLICATION`.

POURQUOI CE FICHIER EXISTE. Cette règle ne se voit nulle part quand on regarde une table : elle est
portée par une clause `WHERE import_id IS NOT ?` au milieu d'un import de 28 onglets. Un jour
quelqu'un « simplifiera » ce DELETE, l'import redeviendra une recopie brutale, et une correction
saisie dans l'écran disparaîtra au réimport suivant — sans message, sans trace, sans que personne
ne fasse le lien. Ces tests rendent cette disparition impossible en silence.

CE QUE CES TESTS NE FONT PAS : ils ne resynchronisent rien. Réaligner automatiquement le classeur
sur une ligne applicative reviendrait à annuler la saisie qu'on cherche justement à protéger.
"""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import ref_setup_catalogue as cat
from app.services import ref_setup_import_service as imp
from app.services import referentiel_admin_service as adm
from test_ref_setup_import import LIGNES_SYNTHETIQUES, _ecrire_classeur

TABLE = "ref_proprietaires"
ONGLET = "REF_Proprietaires"
CLE = LIGNES_SYNTHETIQUES[ONGLET][0]["proprietaire_id"]


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "ref.db"
    apply_migrations(p)
    return p


@pytest.fixture
def classeur(tmp_path):
    return _ecrire_classeur(tmp_path / "REF_Setup_fixture.xlsx")


def _importer(db, classeur):
    r = imp.importer(chemin=classeur, db_path=db)
    assert r.get("ok"), r
    return r


def _colonne(db, cle, colonne):
    conn = get_db(db)
    try:
        r = conn.execute(
            f"SELECT {colonne} FROM {TABLE} WHERE proprietaire_id = ?", (cle,)).fetchone()
        return r[0] if r else None
    finally:
        conn.close()


# ── 1. Le classeur amorce ───────────────────────────────────────────────────────────────────────

def test_un_premier_import_alimente_le_referentiel(db, classeur):
    _importer(db, classeur)
    assert _colonne(db, CLE, "nom_proprietaire") == LIGNES_SYNTHETIQUES[ONGLET][0]["nom_proprietaire"]


def test_une_ligne_importee_porte_l_identifiant_de_son_import(db, classeur):
    """`import_id` vaut ici l'identifiant du RUN qui a créé la ligne : daté, tracé, remontable
    jusqu'au chemin et à l'empreinte du classeur dans `ref_setup_imports`."""
    resultat = _importer(db, classeur)
    assert _colonne(db, CLE, "import_id") == resultat["import_id"]
    assert resultat["import_id"].startswith("IMP-")


def test_un_reimport_remplace_bien_une_ligne_non_applicative(db, classeur, tmp_path):
    """La règle protège la saisie, elle ne gèle pas le référentiel : ce qui vient du classeur
    continue de suivre le classeur."""
    _importer(db, classeur)
    modifie = {onglet: [dict(l) for l in lignes]
               for onglet, lignes in LIGNES_SYNTHETIQUES.items()}
    modifie[ONGLET][0]["nom_proprietaire"] = "NOUVEAU NOM DU CLASSEUR"
    _importer(db, _ecrire_classeur(tmp_path / "v2.xlsx", modifie))
    assert _colonne(db, CLE, "nom_proprietaire") == "NOUVEAU NOM DU CLASSEUR"


# ── 2. L'application prend la main, et la garde ─────────────────────────────────────────────────

def test_une_ligne_modifiee_dans_l_ecran_devient_applicative(db, classeur):
    _importer(db, classeur)
    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ DANS L'ÉCRAN"},
                      action="MODIFICATION", acteur="ui", db_path=db)
    assert _colonne(db, CLE, "import_id") == adm.SOURCE_APPLICATION


def test_un_reimport_n_ecrase_jamais_une_ligne_applicative(db, classeur, tmp_path):
    """LE CŒUR DE LA RÈGLE. Le classeur repasse avec sa propre valeur ; la saisie tient."""
    _importer(db, classeur)
    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ DANS L'ÉCRAN"},
                      action="MODIFICATION", acteur="ui", db_path=db)

    modifie = {onglet: [dict(l) for l in lignes]
               for onglet, lignes in LIGNES_SYNTHETIQUES.items()}
    modifie[ONGLET][0]["nom_proprietaire"] = "VALEUR DU CLASSEUR"
    _importer(db, _ecrire_classeur(tmp_path / "v2.xlsx", modifie))

    assert _colonne(db, CLE, "nom_proprietaire") == "CORRIGÉ DANS L'ÉCRAN"
    assert _colonne(db, CLE, "import_id") == adm.SOURCE_APPLICATION


def test_une_ligne_applicative_n_est_jamais_supprimee_par_un_import(db, classeur, tmp_path):
    """Même absente du classeur, elle reste : l'écran est sa source, pas le fichier."""
    _importer(db, classeur)
    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ"}, action="MODIFICATION",
                      acteur="ui", db_path=db)

    sans_la_ligne = {onglet: [dict(l) for l in lignes]
                     for onglet, lignes in LIGNES_SYNTHETIQUES.items()}
    sans_la_ligne[ONGLET] = [l for l in sans_la_ligne[ONGLET]
                             if l["proprietaire_id"] != CLE]
    _importer(db, _ecrire_classeur(tmp_path / "v3.xlsx", sans_la_ligne))

    assert _colonne(db, CLE, "nom_proprietaire") == "CORRIGÉ"


def test_le_conflit_est_signale_et_jamais_regle_en_silence(db, classeur, tmp_path):
    """Un écart entre le classeur et la saisie DOIT remonter : c'est le seul endroit où
    l'exploitant peut apprendre que son classeur ne reflète plus la réalité."""
    _importer(db, classeur)
    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ"}, action="MODIFICATION",
                      acteur="ui", db_path=db)

    modifie = {onglet: [dict(l) for l in lignes]
               for onglet, lignes in LIGNES_SYNTHETIQUES.items()}
    modifie[ONGLET][0]["nom_proprietaire"] = "VALEUR DU CLASSEUR"
    resultat = _importer(db, _ecrire_classeur(tmp_path / "v2.xlsx", modifie))

    avertissements = " ".join(resultat.get("avertissements") or [])
    assert ONGLET in avertissements
    assert CLE in avertissements
    assert "administrée" in avertissements or "administrees" in avertissements


def test_aucune_resynchronisation_automatique_du_classeur_vers_l_application(db, classeur,
                                                                            tmp_path):
    """La règle est à SENS UNIQUE. Rien, nulle part, ne doit réaligner une ligne applicative sur
    le classeur sans décision humaine — ce serait annuler la saisie qu'on protège."""
    _importer(db, classeur)
    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ"}, action="MODIFICATION",
                      acteur="ui", db_path=db)
    for i in range(3):
        _importer(db, _ecrire_classeur(tmp_path / f"v{i}.xlsx"))
    assert _colonne(db, CLE, "nom_proprietaire") == "CORRIGÉ"


# ── 3. Ce que `import_id` dit — et ce qu'il ne dit plus ─────────────────────────────────────────

def test_import_id_porte_deux_sens_et_le_second_efface_le_premier(db, classeur):
    """CONSTAT, pas souhait. La colonne sert à deux choses à la fois :

      · sur une ligne jamais touchée par l'écran, elle nomme l'IMPORT QUI L'A CRÉÉE ;
      · dès la première modification applicative, elle bascule sur `SAISIE_APPLICATION` et
        l'origine de création n'est plus lisible dans la ligne.

    L'information n'est pas perdue pour autant — `ref_admin_evenements` garde l'avant/après de
    chaque édition — mais elle n'est plus dans la table. Ce test fige le comportement réel pour
    qu'une éventuelle séparation `source_creation` / `source_derniere_modification` se décide, et
    non se découvre.
    """
    resultat = _importer(db, classeur)
    assert _colonne(db, CLE, "import_id") == resultat["import_id"], "origine de création"

    adm.mettre_a_jour(TABLE, CLE, {"nom_proprietaire": "CORRIGÉ"}, action="MODIFICATION",
                      acteur="ui", db_path=db)
    assert _colonne(db, CLE, "import_id") == adm.SOURCE_APPLICATION, "source de dernière écriture"

    conn = get_db(db)
    try:
        imports_connus = {r[0] for r in conn.execute(
            f"SELECT import_id FROM {cat.TABLE_IMPORTS}")}
        trace_creation = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE} WHERE proprietaire_id = ? AND import_id IN "
            f"({','.join('?' * len(imports_connus))})",
            (CLE, *imports_connus)).fetchone()[0]
        journal = conn.execute(
            "SELECT COUNT(*) FROM ref_admin_evenements WHERE table_cible = ? AND cle = ?",
            (TABLE, CLE)).fetchone()[0]
    finally:
        conn.close()
    assert trace_creation == 0, "l'import créateur n'est plus lisible dans la ligne"
    assert journal >= 1, "mais le journal, lui, garde la trace de la modification"

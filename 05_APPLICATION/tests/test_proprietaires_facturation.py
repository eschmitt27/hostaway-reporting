"""Type de client de facturation d'un propriétaire — référentiel de classement (migration 0073).

La règle défendue par ce module tient en une phrase : le type est SAISI, jamais deviné. Ces tests
attaquent donc surtout ce que le service doit REFUSER de faire — conclure d'un nom, d'une adresse
ou d'un SIREN — parce que c'est là qu'une facture devient juridiquement fausse sans prévenir.

Tous les noms sont fictifs.
"""
import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import facturation_config_service as fconf
from app.services import proprietaires_facturation_service as classement


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


# ── Absence de classement ───────────────────────────────────────────────────────────────────────

def test_proprietaire_jamais_classe_vaut_a_controler(db):
    assert classement.lire("PROP_INCONNU", db_path=db) is None
    assert classement.type_client("PROP_INCONNU", db_path=db) == fconf.CLIENT_A_CONTROLER


def test_a_controler_nest_pas_stockable(db):
    """`A_CONTROLER` est l'ABSENCE de décision, pas une décision. Le stocker permettrait de
    « classer » un propriétaire en « on ne sait pas », et de perdre la distinction entre une
    question jamais posée et une réponse jamais tranchée."""
    assert fconf.CLIENT_A_CONTROLER not in classement.TYPES_STOCKABLES
    with pytest.raises(classement.TypeClientError, match=classement.E_TYPE_INVALIDE):
        classement.definir("PROP_1", fconf.CLIENT_A_CONTROLER, db_path=db)


@pytest.mark.parametrize("valeur", ["", "  ", "PRO", "PARTICULIERE", "particulier ?", "SAS",
                                    "ENTREPRISE", "0", "None"])
def test_valeur_hors_referentiel_refusee(db, valeur):
    """Champ FERMÉ : deux valeurs, pas de texte libre. Une orthographe approchante passerait
    silencieusement les contrôles B2B en les rendant inopérants."""
    with pytest.raises(classement.TypeClientError):
        classement.definir("PROP_1", valeur, db_path=db)
    assert classement.type_client("PROP_1", db_path=db) == fconf.CLIENT_A_CONTROLER


def test_identifiant_proprietaire_obligatoire(db):
    with pytest.raises(classement.TypeClientError, match=classement.E_PROPRIETAIRE_MANQUANT):
        classement.definir("", fconf.CLIENT_PARTICULIER, db_path=db)


# ── Le type n'est JAMAIS déduit ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("pid", ["SARL_MARTIN", "SCI_LES_TILLEULS", "PROP_SAS_DEMO",
                                 "EURL_DUPONT", "PROP_0001"])
def test_un_nom_a_consonance_societe_ne_classe_personne(db, pid):
    """LE test central de la mission : « SCI », « SARL » ou « SAS » dans un identifiant ne prouve
    rien. Déduire le type d'un libellé imprimerait des pénalités de retard chez un particulier."""
    assert classement.type_client(pid, db_path=db) == fconf.CLIENT_A_CONTROLER


def test_un_siren_connu_ne_classe_pas_en_professionnel(db):
    """Un SIREN peut être renseigné pour un propriétaire qu'on n'a pas encore tranché. Sa présence
    est une donnée d'identité, pas une conclusion sur le régime de facturation."""
    classement.definir("PROP_2", fconf.CLIENT_PARTICULIER, siren_client="111111111",
                       acteur="test", db_path=db)
    assert classement.type_client("PROP_2", db_path=db) == fconf.CLIENT_PARTICULIER
    assert classement.lire("PROP_2", db_path=db)["siren_client"] == "111111111"


# ── Saisie, relecture, modification ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("type_attendu", [fconf.CLIENT_PARTICULIER, fconf.CLIENT_PROFESSIONNEL])
def test_classement_saisi_est_relu(db, type_attendu):
    classement.definir("PROP_3", type_attendu, acteur="test", db_path=db)
    assert classement.type_client("PROP_3", db_path=db) == type_attendu


def test_casse_normalisee_a_la_saisie(db):
    classement.definir("PROP_4", "professionnel", acteur="test", db_path=db)
    assert classement.type_client("PROP_4", db_path=db) == fconf.CLIENT_PROFESSIONNEL


def test_reclassement_possible_et_journalise(db):
    """Une société peut cesser, un particulier peut en créer une. Ce qui ne doit pas se perdre,
    c'est la trace du changement d'avis."""
    classement.definir("PROP_5", fconf.CLIENT_PARTICULIER, acteur="alice", db_path=db)
    classement.definir("PROP_5", fconf.CLIENT_PROFESSIONNEL, motif="immatriculation",
                       acteur="bob", db_path=db)
    assert classement.type_client("PROP_5", db_path=db) == fconf.CLIENT_PROFESSIONNEL

    hist = classement.historique("PROP_5", db_path=db)
    assert len(hist) == 2
    dernier = hist[0]
    assert (dernier["valeur_avant"], dernier["valeur_apres"]) == (fconf.CLIENT_PARTICULIER,
                                                                  fconf.CLIENT_PROFESSIONNEL)
    assert dernier["motif"] == "immatriculation" and dernier["acteur"] == "bob"
    premier = hist[1]
    assert premier["valeur_avant"] == fconf.CLIENT_A_CONTROLER, \
        "le premier classement part de l'absence de decision, pas d'un defaut"


def test_reclassement_ne_duplique_pas_la_ligne(db):
    classement.definir("PROP_6", fconf.CLIENT_PARTICULIER, acteur="test", db_path=db)
    classement.definir("PROP_6", fconf.CLIENT_PROFESSIONNEL, acteur="test", db_path=db)
    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM proprietaires_facturation "
                         "WHERE proprietaire_id='PROP_6'").fetchone()[0]
    finally:
        conn.close()
    assert n == 1, "la cle primaire porte le proprietaire : un seul classement courant"


def test_classements_independants_entre_proprietaires(db):
    classement.definir("PROP_A", fconf.CLIENT_PARTICULIER, acteur="test", db_path=db)
    classement.definir("PROP_B", fconf.CLIENT_PROFESSIONNEL, acteur="test", db_path=db)
    assert classement.type_client("PROP_A", db_path=db) == fconf.CLIENT_PARTICULIER
    assert classement.type_client("PROP_B", db_path=db) == fconf.CLIENT_PROFESSIONNEL


# ── Robustesse du stockage ──────────────────────────────────────────────────────────────────────

def test_contrainte_sql_refuse_une_valeur_hors_referentiel(db):
    """Ceinture ET bretelles : même en contournant le service, SQLite refuse (CHECK 0073)."""
    import sqlite3
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO proprietaires_facturation "
                         "(proprietaire_id, type_client_facturation) VALUES ('PROP_X','INCONNU')")
            conn.commit()
    finally:
        conn.close()


def test_valeur_corrompue_en_base_est_lue_comme_a_controler(db):
    """Défense en profondeur : si une valeur illégitime existait malgré tout (base migrée à la
    main, restauration ancienne), la lecture ne doit pas la propager jusqu'aux mentions légales."""
    classement.definir("PROP_7", fconf.CLIENT_PROFESSIONNEL, acteur="test", db_path=db)
    conn = get_db(db)
    try:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute("UPDATE proprietaires_facturation SET type_client_facturation='BIZARRE' "
                     "WHERE proprietaire_id='PROP_7'")
        conn.commit()
    finally:
        conn.close()
    assert classement.type_client("PROP_7", db_path=db) == fconf.CLIENT_A_CONTROLER


def test_base_sans_la_table_ne_leve_pas(db, tmp_path, monkeypatch):
    """Une base antérieure à 0073 doit répondre « à contrôler », pas planter la fiche facture."""
    vide = tmp_path / "sans_0073.db"
    conn = get_db(vide)
    conn.execute("CREATE TABLE marqueur (x INTEGER)")
    conn.commit()
    conn.close()
    assert classement.lire("PROP_8", db_path=vide) is None
    assert classement.type_client("PROP_8", db_path=vide) == fconf.CLIENT_A_CONTROLER
    assert classement.historique("PROP_8", db_path=vide) == []

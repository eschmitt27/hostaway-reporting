"""§23-25 — le logiciel propose le logement d'une ligne de facture, et n'en invente jamais un.

CE QUE CES TESTS PROTÈGENT
Le rapprochement lit le RÉFÉRENTIEL. La régression à empêcher n'est pas « il ne trouve pas » — un
libellé inconnu doit rester inconnu — mais « il trouve quelque chose » quand le référentiel ne dit
rien. Un faux rapprochement impute silencieusement le ménage d'un propriétaire à un autre ; une
absence de rapprochement se voit à l'écran et se corrige en un geste.

Le référentiel des tests reproduit la forme du réel (nom officiel, nom court, adresse, alias
déclarés, logement retiré du parc, bac technique hors parc) sans en copier les valeurs.
"""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import logement_matching_service as lms


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    conn = get_db(p)
    try:
        for lid, officiel, court, adresse, ville, actif, parc in [
            ("LOG_0001", "Studio - 46 (Didier)", "Studio - 46",
             "46 ALLEE CHARLES DE FITTE", "TOULOUSE", "OUI", "GERE"),
            ("LOG_0002", "T4 - 90 Blagnac (Cédrine)", "T4 - 90 Blagnac",
             "90 Avenue de Cornebarrieu", "BLAGNAC", "NON", "RETIRE"),
            ("LOG_0003", "T3 - Cyprien (Clarisse)", "T3 - Cyprien",
             "20 RUE DE L'AMIRAL GALACHE", "TOULOUSE", "OUI", "GERE"),
            ("LOG_0004", "Studio - Puits vert (Caroline)", "Studio - Puits vert",
             "4 RUE DU PUITS VERT", "TOULOUSE", "OUI", "GERE"),
            ("LOGEMENT_DIVERS", "Logement divers (hors parc)", "LOGEMENT_DIVERS",
             "", "", "OUI", "HORS_PARC_TECHNIQUE"),
        ]:
            conn.execute(
                "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, "
                "adresse, ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
                (lid, officiel, court, adresse, ville, actif, parc, "TEST"))
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, niveau_confiance, actif, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("MAP_9001", "Facture ménage externe", "libelle_logement_source",
             "T.4-90 Blagnac (Cédrine)", "LOG_0002", "Fort", "OUI", "TEST"))
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, niveau_confiance, actif, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("MAP_9002", "Setup.xlsx", "adresse_source", "46 ALLEE CHARLES DE FITTE",
             "LOG_0001", "Moyen", "OUI", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return p


@pytest.fixture
def ref(db):
    return lms.charger_referentiel(db)


# ── Normalisation ───────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("a,b", [
    ("Studio - 46 (Didier)", "studio  46   didier"),
    ("T.4-90 Blagnac (Cédrine)", "T4 90 blagnac cedrine"),
    ("T 3 Cyprien", "t3 cyprien"),
    ("20 RUE DE L’AMIRAL GALACHE", "20 rue de l'amiral galache"),
])
def test_normalisation_reunit_les_ecritures_d_un_meme_libelle(a, b):
    """Accents, casse, ponctuation et « T.4 / T 4 / T4 » désignent la même chose."""
    assert lms.normaliser(a) == lms.normaliser(b)


def test_normalisation_ne_confond_pas_deux_logements_voisins():
    assert lms.normaliser("T4 - 90 Blagnac") != lms.normaliser("T3 - 90 Blagnac")


# ── Les cinq voies ──────────────────────────────────────────────────────────────────────────────

def test_correspondance_declaree_prime_et_vaut_certitude(ref):
    r = lms.proposer("T.4-90 Blagnac (Cédrine)", ref)
    assert r["logement_id"] == "LOG_0002"
    assert r["confiance"] == lms.CERTAIN
    assert r["methode"] == lms.M_MAPPING
    assert r["preremplir"] is True and r["a_confirmer"] is False


def test_correspondance_declaree_de_confiance_moyenne_reste_a_confirmer(ref):
    """`niveau_confiance = Moyen` au référentiel ne devient pas une certitude en chemin."""
    r = lms.proposer("46 ALLEE CHARLES DE FITTE", ref)
    assert r["logement_id"] == "LOG_0001"
    assert r["confiance"] == lms.PROBABLE and r["a_confirmer"] is True


def test_nom_officiel_exact(ref):
    r = lms.proposer("studio   -   46   (DIDIER)", ref)
    assert (r["logement_id"], r["confiance"]) == ("LOG_0001", lms.CERTAIN)
    assert r["methode"] in (lms.M_MAPPING, lms.M_NOM_OFFICIEL)


def test_nom_court_exact(ref):
    r = lms.proposer("T3 - Cyprien", ref)
    assert (r["logement_id"], r["confiance"]) == ("LOG_0003", lms.CERTAIN)


def test_adresse_lue_dans_le_libelle(ref):
    """« T3 4 rue du puits vert » : le libellé n'est pas le nom du logement, mais son adresse y est."""
    r = lms.proposer("T3 4 rue du puits vert", ref)
    assert r["logement_id"] == "LOG_0004"
    assert r["methode"] == lms.M_ADRESSE
    assert r["confiance"] == lms.PROBABLE


def test_mots_distinctifs_quand_rien_n_est_exact(ref):
    """« studio Puits VERTS » (pluriel) n'est égal à rien, mais ne désigne qu'un logement."""
    r = lms.proposer("studio Puits verts (Caroline)", ref)
    assert r["logement_id"] == "LOG_0004"
    assert r["methode"] == lms.M_TOKENS
    assert r["confiance"] == lms.PROBABLE, "un quasi-match n'est jamais promu en certitude"
    assert "caroline" in r["raison"] or "puits" in r["raison"]


# ── Ce qui ne doit JAMAIS être proposé ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("libelle", [
    "", "   ",
    "T5 - 200 avenue de Paris (Inconnu)",   # logement absent du parc
    "Frais de déplacement",                 # ce n'est pas un logement
    "Heures supplémentaires juillet",
    "Nettoyage des vitres",
    "Studio",                               # trop générique
    "T3",
    "T3 Toulouse",                          # « toulouse » est partagé : ne distingue rien
])
def test_aucune_invention(libelle, ref):
    r = lms.proposer(libelle, ref)
    assert r["logement_id"] == "", f"« {libelle} » ne devrait rapprocher aucun logement"
    assert r["confiance"] == lms.AUCUN
    assert r["preremplir"] is False


def test_bac_technique_jamais_propose_tout_seul(ref):
    """Rattacher d'office à « Logement divers » serait exactement l'invention à éviter."""
    r = lms.proposer("logement divers", ref)
    assert r["logement_id"] == ""


def test_ambiguite_nomme_les_candidats_sans_choisir(db):
    """Deux logements également plausibles : le rapprochement s'arrête et les nomme."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("LOG_0099", "Studio - Puits vert (Caroline)", "Studio - Puits vert bis",
             "5 RUE DU PUITS VERT", "TOULOUSE", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    r = lms.proposer("Studio - Puits vert (Caroline)", lms.charger_referentiel(db))
    assert r["logement_id"] == ""
    assert r["confiance"] == lms.AUCUN
    assert {c["logement_id"] for c in r["candidats"]} == {"LOG_0004", "LOG_0099"}
    assert "choisir" in r["raison"]


def test_logement_retire_du_parc_reste_rapprochable(ref):
    """Une facture d'un mois passé désigne légitimement un logement qui n'est plus géré."""
    r = lms.proposer("T.4-90 Blagnac (Cédrine)", ref)
    assert r["logement_id"] == "LOG_0002"
    assert r["actif"] is False, "l'écran doit pouvoir signaler qu'il est sorti du parc"


# ── Le poids des mots vient du référentiel, pas d'une liste écrite à la main ─────────────────────

def test_un_mot_partage_par_tout_le_parc_ne_distingue_rien(ref):
    poids = lms._poids_tokens(ref)
    assert poids["toulouse"] < poids["galache"], \
        "« toulouse » est partagé, « galache » est unique : le référentiel le dit lui-même"


def test_le_poids_se_recalibre_quand_le_parc_change(db):
    """Aucune liste de mots vides à maintenir : un mot qui se banalise perd son poids seul."""
    avant = lms._poids_tokens(lms.charger_referentiel(db))["cyprien"]
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("LOG_0098", "T2 - Cyprien autre", "T2 Cyprien", "", "TOULOUSE", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    assert lms._poids_tokens(lms.charger_referentiel(db))["cyprien"] < avant


# ── Le logiciel apprend du geste de l'utilisateur ────────────────────────────────────────────────

def test_confirmer_un_libelle_le_rend_certain_la_fois_suivante(db):
    libelle = "T.3 Cyprien chez Clarisse — 2e étage"
    assert lms.proposer(libelle, lms.charger_referentiel(db))["confiance"] != lms.CERTAIN

    res = lms.enregistrer_correspondance(libelle, "LOG_0003", acteur="test", db_path=db)
    assert res["ok"] is True and res["deja_connue"] is False

    apres = lms.proposer(libelle, lms.charger_referentiel(db))
    assert (apres["logement_id"], apres["confiance"]) == ("LOG_0003", lms.CERTAIN)
    assert apres["methode"] == lms.M_MAPPING


def test_reconfirmer_le_meme_libelle_ne_cree_pas_de_doublon(db):
    lms.enregistrer_correspondance("Chez Didier", "LOG_0001", db_path=db)
    deux = lms.enregistrer_correspondance("Chez Didier", "LOG_0001", db_path=db)
    assert deux["ok"] is True and deux["deja_connue"] is True


def test_correspondance_contradictoire_refusee(db):
    lms.enregistrer_correspondance("Chez Didier", "LOG_0001", db_path=db)
    res = lms.enregistrer_correspondance("Chez Didier", "LOG_0003", db_path=db)
    assert res["ok"] is False and res["code"] == "CORRESPONDANCE_CONTRADICTOIRE"
    assert "LOG_0001" in res["detail"]


def test_correspondance_vers_un_logement_inconnu_refusee(db):
    res = lms.enregistrer_correspondance("Chez quelqu'un", "LOG_9999", db_path=db)
    assert res["ok"] is False and res["code"] == "LOGEMENT_INCONNU"

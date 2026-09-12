"""§81-91 — chaque référentiel porte exactement une classe, et cette classe est APPLIQUÉE.

L'écran n'en distinguait que deux : « Administrable » et « Parcours dédié ». Deux situations très
différentes se retrouvaient donc ensemble dans « administrable » : les données que l'exploitant
possède (ses logements, ses propriétaires) et les NOMENCLATURES dont le moteur lit les identifiants
en dur. Renommer une catégorie de charge est inoffensif ; en supprimer une, ou en ajouter une que
le moteur ne connaît pas, casse un calcul sans un mot.

La classification n'est pas une opinion : elle se déduit de ce que le code fait de chaque table.
Ce fichier vérifie qu'elle est complète, exclusive, et qu'elle gouverne réellement l'UI ET le
service — pas seulement l'affichage d'un badge.
"""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import referentiel_admin_service as adm


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    conn = get_db(p)
    try:
        for i, (nom, prenom) in enumerate([("UZON", "Didier"), ("Delrieu", "Cédrine")], start=1):
            conn.execute(
                "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                "prenom_proprietaire, actif, import_id) VALUES (?,?,?,?,?)",
                (f"PROP_{i:04d}", nom, prenom, "OUI", "TEST"))
        conn.execute(
            "INSERT INTO ref_types_logements (type_logement_id, type_logement, import_id) "
            "VALUES (?,?,?)", ("TYPE_001", "STUDIO", "TEST"))
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, type_logement_id, actif, statut_parc, dynamic_pricing, import_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("LOG_0001", "Studio - 46", "Studio - 46", "", "TOULOUSE", "TYPE_001", "OUI",
             "GERE", "hostdynamic", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return p


def _toutes_les_tables():
    return [t for c in adm.CATEGORIES for t in c["tables"]]


# ── La classification est complète et exclusive ─────────────────────────────────────────────────

def test_chaque_referentiel_a_exactement_une_classe():
    for table in _toutes_les_tables():
        assert adm.classe(table) in adm.CLASSES, f"{table} sans classe valide"


def test_les_quatre_classes_sont_toutes_employees():
    """Si l'une ne sert jamais, c'est qu'elle ne veut rien dire — ou qu'on a mal classé."""
    employees = {adm.classe(t) for t in _toutes_les_tables()}
    assert employees == set(adm.CLASSES)


def test_une_table_non_classee_reste_administrable():
    """Le défaut est permissif : verrouiller par OUBLI serait le pire des deux comportements."""
    assert adm.classe("ref_table_inventee") == adm.EDITABLE
    assert adm.est_editable("ref_table_inventee") is True


def test_toute_classe_non_editable_porte_un_motif():
    """Un refus sans raison n'apprend rien : le motif doit dire où aller, ou pourquoi non."""
    for table in _toutes_les_tables():
        if not adm.est_editable(table):
            assert adm.motif_classe(table).strip(), f"{table} : classe non éditable sans motif"


def test_lecture_seule_est_derivee_de_la_classification():
    """Une seule source de vérité : `LECTURE_SEULE` n'est plus une liste tenue en parallèle."""
    attendu = {t for t in adm.CLASSIFICATION if not adm.est_editable(t)}
    assert set(adm.LECTURE_SEULE) == attendu


# ── Les classes disent ce qu'on attend d'elles ──────────────────────────────────────────────────

@pytest.mark.parametrize("table", [
    "ref_gestion_logements_hist",   # historique logement : modifié par la fiche logement
    "ref_taux_commission",
    "ref_couts_standards_menage",
    "ref_canape_parametres",
    "ref_cloture_mensuelle",        # clôturer est un ACTE tracé, pas une édition de champ
])
def test_les_parcours_dedies_sont_consultables_et_non_editables(table):
    assert adm.classe(table) == adm.DEDICATED_WORKFLOW
    assert adm.est_editable(table) is False
    assert adm.est_visible(table) is True, "un parcours dédié reste consultable"


@pytest.mark.parametrize("table", [
    "ref_types_flux", "ref_codes_impact", "ref_categories_charges", "ref_modes_paiement",
])
def test_les_nomenclatures_du_moteur_sont_en_consultation_seule(table):
    """Le moteur cite ces identifiants en dur : les éditer casserait un calcul en silence."""
    assert adm.classe(table) == adm.READ_ONLY
    assert adm.est_editable(table) is False


def test_les_tables_techniques_sortent_de_la_liste_courante_sans_disparaitre():
    for table in ("ref_assoc_mode", "ref_sources_systeme"):
        assert adm.classe(table) == adm.HIDDEN_TECHNICAL
        assert adm.est_visible(table) is False, "hors de la liste courante"
        assert adm.est_editable(table) is False


def test_le_mapping_logement_se_corrige_par_un_parcours_pas_en_table():
    """Une correspondance FAUSSE impute l'argent au mauvais propriétaire : elle doit se corriger.

    Ce test affirmait auparavant que la table devait rester ÉDITABLE — c'était confondre le besoin
    (pouvoir corriger) avec un moyen (éditer la ligne brute). Décision §18 : une correspondance est
    une décision prise sur un libellé venu de l'extérieur, pas une donnée qu'on saisit. Le besoin
    est donc satisfait — et mieux, puisque le choix est tracé — par un parcours dédié, auquel
    l'écran d'administration conduit.
    """
    assert adm.classe("ref_mapping_logements") == adm.DEDICATED_WORKFLOW
    assert adm.est_visible("ref_mapping_logements") is True, "l'état reste consultable"
    assert adm.url_parcours("ref_mapping_logements") == "/correspondances-logement"


@pytest.mark.parametrize("table", ["ref_logements", "ref_proprietaires", "ref_intervenants"])
def test_les_donnees_de_l_exploitant_sont_administrables(table):
    assert adm.classe(table) == adm.EDITABLE


# ── La classification GOUVERNE le service, pas seulement l'affichage ────────────────────────────

def test_une_table_non_editable_refuse_l_ecriture(db):
    """Quelle que soit la route employée : c'est le service qui tient la règle."""
    res = adm.modifier_ligne("ref_types_flux", "TYPE_FLUX_001", {"type_flux": "PIRATE"},
                             db_path=db)
    assert res["ok"] is False and res["code"] == adm.E_ECRITURE
    assert adm.motif_classe("ref_types_flux") in res["detail"]


def test_une_table_technique_refuse_aussi_l_ecriture(db):
    res = adm.modifier_ligne("ref_assoc_mode", "AM_001", {"libelle": "x"}, db_path=db)
    assert res["ok"] is False and res["code"] == adm.E_ECRITURE


def test_les_metadonnees_portent_la_classe(db):
    meta = adm.decrire_table("ref_types_flux", db_path=db)
    assert meta["classe"] == adm.READ_ONLY
    assert meta["editable"] is False
    assert meta["libelle_classe"] == "Consultation seule"


def test_la_liste_separe_les_referentiels_techniques(db):
    categories = adm.categories(db_path=db)
    courants = {t["table"] for c in categories for t in c["tables"]}
    techniques = {t["table"] for c in categories for t in c["tables_techniques"]}
    assert "ref_assoc_mode" in techniques
    assert "ref_assoc_mode" not in courants, "hors de la liste courante"
    assert "ref_logements" in courants
    assert not (courants & techniques), "une table est dans une liste, jamais dans les deux"


# ── §82 — l'identifiant se dérive ───────────────────────────────────────────────────────────────

def test_l_identifiant_est_propose_d_apres_le_dernier_attribue(db):
    assert adm.prochaine_cle("ref_proprietaires", db_path=db) == "PROP_0003"
    assert adm.prochaine_cle("ref_logements", db_path=db) == "LOG_0002"
    assert adm.prochaine_cle("ref_types_logements", db_path=db) == "TYPE_002"


def test_un_identifiant_libere_n_est_jamais_reutilise(db):
    """Rattacher des données anciennes à un nouveau tiers serait pire qu'un trou dans la série."""
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM ref_proprietaires WHERE proprietaire_id='PROP_0001'")
        conn.commit()
    finally:
        conn.close()
    assert adm.prochaine_cle("ref_proprietaires", db_path=db) == "PROP_0003", \
        "on repart du maximum, jamais du premier trou"


def test_une_table_sans_sequence_ne_propose_rien(db):
    assert adm.prochaine_cle("ref_types_flux", db_path=db) == ""


# ── §83-91 — les champs se choisissent ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("colonne", ["actif", "statut_parc", "sur_hostaway", "type_logement_id"])
def test_les_champs_du_logement_sont_des_listes(db, colonne):
    champs = adm.champs_edition("ref_logements", db_path=db)
    assert champs[colonne]["type"] == "liste", f"{colonne} devrait se choisir, pas se taper"
    assert champs[colonne]["options"], f"{colonne} sans valeur proposée"


def test_le_type_de_logement_se_choisit_par_son_libelle(db):
    options = adm.options_champ("ref_logements", "type_logement_id", db_path=db)
    assert options == [{"valeur": "TYPE_001", "libelle": "STUDIO"}], \
        "on choisit « STUDIO », la valeur transmise reste TYPE_001"


@pytest.mark.parametrize("colonne", ["actif", "mode_facturation"])
def test_les_champs_du_proprietaire_sont_des_listes(db, colonne):
    assert adm.champs_edition("ref_proprietaires", db_path=db)[colonne]["type"] == "liste"


def test_une_valeur_existante_hors_liste_reste_choisissable(db):
    """`dynamic_pricing` porte « hostdynamic » : le réduire à OUI/NON effacerait quel outil sert.

    La liste s'ouvre donc sur ce qui EXISTE en base, au lieu de remplacer silencieusement la
    valeur à la première modification de la ligne.
    """
    valeurs = [o["valeur"] for o in
               adm.options_champ("ref_logements", "dynamic_pricing", db_path=db)]
    assert "hostdynamic" in valeurs, "la valeur réellement présente doit rester sélectionnable"
    assert "non" in valeurs


def test_les_colonnes_de_date_prennent_un_calendrier(db):
    champs = adm.champs_edition("ref_gestion_logements_hist", db_path=db)
    assert champs["date_debut"]["type"] == "date"
    assert champs["date_fin"]["type"] == "date"


def test_une_colonne_libre_reste_libre(db):
    champs = adm.champs_edition("ref_logements", db_path=db)
    assert champs["adresse"]["type"] == "texte"
    assert champs["nom_court"]["type"] == "texte"


def test_la_cle_est_reconnue_comme_telle(db):
    assert adm.champs_edition("ref_logements", db_path=db)["logement_id"]["type"] == "cle"

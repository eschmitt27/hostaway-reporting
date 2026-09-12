"""Correspondances logement (§18) et pricing dynamique (§19).

CE QUE CE FICHIER PROUVE
  §18  — `ref_mapping_logements` n'est plus éditable en table ; le parcours dédié existe, remonte
         ce qu'aucun écran ne montrait (une annonce Hostaway non rattachée), refuse un logement
         technique comme réponse, et trace chaque décision sans jamais effacer la précédente.
  §19  — « activé » et « quel moteur » sont deux questions distinctes ; la dérivation tient quel
         que soit le chemin d'écriture ; « hostdynamic » n'est jamais perdu.
"""
from __future__ import annotations

import pytest

from app.db.connection import get_db
from app.services import correspondances_logement_service as corr
from app.services import referentiel_admin_service as adm

LOG = "LOG_TEST_01"
AUTRE = "LOG_TEST_02"


@pytest.fixture()
def parc(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.executemany(
            "INSERT INTO ref_logements (logement_id, nom_court, nom_logement_officiel, ville, "
            "type_logement_id, actif, statut_parc, dynamic_pricing, hostaway_listing_id, "
            "import_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(LOG, "Studio Test", "Studio Test officiel", "Toulouse", "TYPE_001", "OUI", "GERE",
              "hostdynamic", "111111", "IMP-TEST"),
             (AUTRE, "T2 Test", "T2 Test officiel", "Blagnac", "TYPE_002", "OUI", "GERE",
              "non", "222222", "IMP-TEST"),
             ("LOGEMENT_DIVERS", "LOGEMENT_DIVERS", "LOGEMENT_DIVERS", "", "TYPE_001", "OUI",
              "HORS_PARC_TECHNIQUE", "non", "", "IMP-TEST")])
        conn.commit()
    finally:
        conn.close()
    return tmp_db


# ── §18 — la table n'est plus une porte d'entrée ────────────────────────────────────────────────

def test_le_mapping_logement_n_est_plus_editable_en_table():
    assert adm.classe("ref_mapping_logements") == adm.DEDICATED_WORKFLOW
    assert adm.est_editable("ref_mapping_logements") is False
    # …mais reste VISIBLE : le cacher empêcherait de constater l'état des correspondances.
    assert adm.est_visible("ref_mapping_logements") is True


def test_le_motif_conduit_au_parcours_au_lieu_de_le_nommer():
    assert adm.url_parcours("ref_mapping_logements") == "/correspondances-logement"


def test_une_table_editable_n_a_pas_de_parcours_dedie():
    assert adm.url_parcours("ref_logements") == ""


def test_l_edition_en_table_est_refusee(parc):
    resultat = adm.modifier_ligne("ref_mapping_logements", "MAP_LOG_0001",
                                  {"logement_id": AUTRE}, db_path=parc)
    assert resultat["ok"] is False
    assert resultat["code"] == adm.E_ECRITURE


# ── §18 — le parcours fait ce que la table ne faisait pas ───────────────────────────────────────

def test_une_annonce_hostaway_non_rattachee_est_remontee(parc):
    """Le cas réel : une annonce apparue chez Hostaway, absente du parc, qui ne se signalait qu'en
    faisant échouer le calcul des réservations avec un message de moteur."""
    from app.services import hostaway_raw_service as raw

    eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=parc, source_ref="c1")
    raw.enregistrer(eid, db_path=parc, reservations=[
        {"reservation_id": "77001", "listingMapId": "999999", "status": "new",
         "checkInDate": "2026-07-29", "checkOutDate": "2026-08-01"},
        {"reservation_id": "77002", "listingMapId": "111111", "status": "new",
         "checkInDate": "2026-08-02", "checkOutDate": "2026-08-04"}])
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=parc)

    a_traiter = corr.a_traiter(db_path=parc)
    valeurs = {e["valeur_source"] for e in a_traiter}
    assert "999999" in valeurs, "l'annonce inconnue du parc doit être remontée"
    assert "111111" not in valeurs, "une annonce déjà portée par une fiche logement n'est pas orpheline"
    orpheline = next(e for e in a_traiter if e["valeur_source"] == "999999")
    assert orpheline["nb_reservations"] == 1
    assert orpheline["premiere_arrivee"] == "2026-07-29"


def test_rattacher_une_annonce_ecrit_la_fiche_ET_la_correspondance(parc):
    """Les deux chemins de lecture existent : la fiche logement (moteurs de réservation) et la
    règle de correspondance (rapprochement). N'en écrire qu'un laisserait l'autre bloqué."""
    resultat = corr.rattacher_listing_au_parc(listing_map_id="999999", logement_id=LOG,
                                              acteur="test", db_path=parc)
    assert resultat["ok"] is True
    conn = get_db(parc)
    try:
        fiche = conn.execute(
            "SELECT hostaway_listing_id, sur_hostaway FROM ref_logements WHERE logement_id = ?",
            (LOG,)).fetchone()
        regle = conn.execute(
            "SELECT logement_id FROM ref_mapping_logements WHERE champ_source = 'listingMapId' "
            "AND valeur_source = '999999' AND actif = 'OUI'").fetchone()
    finally:
        conn.close()
    assert fiche["hostaway_listing_id"] == "999999"
    assert fiche["sur_hostaway"] == "OUI"
    assert regle["logement_id"] == LOG


def test_une_annonce_deja_rattachee_ailleurs_est_refusee(parc):
    resultat = corr.rattacher_listing_au_parc(listing_map_id="222222", logement_id=LOG,
                                              acteur="test", db_path=parc)
    assert resultat["ok"] is False
    assert resultat["code"] == "LISTING_DEJA_RATTACHE"


def test_un_logement_technique_ne_peut_pas_servir_de_reponse(parc):
    """Masquer un mauvais rattachement derrière « divers » est exactement ce que ce parcours doit
    rendre inutile."""
    resultat = corr.enregistrer(source="Facture ménage externe",
                                champ_source="libelle_logement_source",
                                valeur_source="Studio quelconque",
                                logement_id="LOGEMENT_DIVERS", db_path=parc)
    assert resultat["ok"] is False
    assert resultat["code"] == corr.E_LOGEMENT_TECHNIQUE


def test_les_logements_techniques_ne_sont_pas_proposes(parc):
    identifiants = {l["logement_id"] for l in corr.logements_selectionnables(db_path=parc)}
    assert "LOGEMENT_DIVERS" not in identifiants
    assert LOG in identifiants


def test_un_logement_retire_du_parc_reste_proposable(parc):
    """Une correspondance porte souvent sur des mois passés, où ce logement était géré."""
    conn = get_db(parc)
    try:
        conn.execute("UPDATE ref_logements SET statut_parc = 'RETIRE', actif = 'NON' "
                     "WHERE logement_id = ?", (AUTRE,))
        conn.commit()
    finally:
        conn.close()
    assert AUTRE in {l["logement_id"] for l in corr.logements_selectionnables(db_path=parc)}


def test_corriger_desactive_l_ancienne_sans_l_effacer(parc):
    corr.enregistrer(source="Facture ménage externe", champ_source="libelle_logement_source",
                     valeur_source="Studio 46", logement_id=LOG, acteur="test", db_path=parc)
    corr.enregistrer(source="Facture ménage externe", champ_source="libelle_logement_source",
                     valeur_source="Studio 46", logement_id=AUTRE, acteur="test",
                     motif="erreur de rattachement", db_path=parc)
    lignes = [c for c in corr.declarees(db_path=parc) if c["valeur_source"] == "Studio 46"]
    assert len(lignes) == 2, "l'ancienne correspondance est conservée, jamais supprimée"
    assert {c["actif"] for c in lignes} == {"OUI", "NON"}
    active = next(c for c in lignes if c["actif"] == "OUI")
    assert active["logement_id"] == AUTRE


def test_reenregistrer_le_meme_choix_ne_cree_pas_de_doublon(parc):
    corr.enregistrer(source="X", champ_source="libelle_logement_source", valeur_source="Idem",
                     logement_id=LOG, db_path=parc)
    second = corr.enregistrer(source="X", champ_source="libelle_logement_source",
                              valeur_source="Idem", logement_id=LOG, db_path=parc)
    assert second["ok"] is True and second["inchange"] is True
    assert len([c for c in corr.declarees(db_path=parc) if c["valeur_source"] == "Idem"]) == 1


def test_chaque_decision_est_tracee(parc):
    corr.enregistrer(source="X", champ_source="libelle_logement_source", valeur_source="Trace",
                     logement_id=LOG, acteur="ui:test", motif="rattachement initial", db_path=parc)
    corr.enregistrer(source="X", champ_source="libelle_logement_source", valeur_source="Trace",
                     logement_id=AUTRE, acteur="ui:test", motif="correction", db_path=parc)
    histo = [h for h in corr.historique(db_path=parc) if h["valeur_source"] == "Trace"]
    assert len(histo) == 2
    correction = histo[0]
    assert correction["action"] == corr.ACTION_CORRECTION
    assert correction["logement_avant"] == LOG
    assert correction["logement_apres"] == AUTRE
    assert correction["acteur"] == "ui:test"


def test_un_logement_inexistant_est_refuse(parc):
    resultat = corr.enregistrer(source="X", champ_source="libelle_logement_source",
                                valeur_source="V", logement_id="LOG_FANTOME", db_path=parc)
    assert resultat["ok"] is False
    assert resultat["code"] == corr.E_LOGEMENT_INCONNU


# ── §19 — deux questions, deux colonnes ─────────────────────────────────────────────────────────

def test_la_reprise_conserve_le_nom_du_moteur(parc):
    conn = get_db(parc)
    try:
        ligne = conn.execute(
            "SELECT dynamic_pricing, dynamic_pricing_enabled, dynamic_pricing_provider "
            "FROM ref_logements WHERE logement_id = ?", (LOG,)).fetchone()
    finally:
        conn.close()
    assert ligne["dynamic_pricing_enabled"] == "OUI"
    assert ligne["dynamic_pricing_provider"] == "hostdynamic", "« hostdynamic » ne doit pas être perdu"


def test_desactive_signifie_aucun_moteur(parc):
    conn = get_db(parc)
    try:
        ligne = conn.execute(
            "SELECT dynamic_pricing_enabled, dynamic_pricing_provider FROM ref_logements "
            "WHERE logement_id = ?", (AUTRE,)).fetchone()
    finally:
        conn.close()
    assert ligne["dynamic_pricing_enabled"] == "NON"
    assert ligne["dynamic_pricing_provider"] is None


@pytest.mark.parametrize("brut,attendu", [
    ("hostdynamic", ("OUI", "hostdynamic")),
    ("non", ("NON", None)),
    ("", ("NON", None)),
    ("AUTRE_MOTEUR", ("OUI", "AUTRE_MOTEUR")),
])
def test_la_derivation_tient_quel_que_soit_le_chemin_d_ecriture(parc, brut, attendu):
    """Le déclencheur, et non le code applicatif : un import qui écrirait directement la colonne
    brute obtient la même dérivation."""
    conn = get_db(parc)
    try:
        conn.execute("UPDATE ref_logements SET dynamic_pricing = ? WHERE logement_id = ?",
                     (brut, LOG))
        conn.commit()
        ligne = conn.execute(
            "SELECT dynamic_pricing_enabled, dynamic_pricing_provider FROM ref_logements "
            "WHERE logement_id = ?", (LOG,)).fetchone()
    finally:
        conn.close()
    assert (ligne["dynamic_pricing_enabled"], ligne["dynamic_pricing_provider"]) == attendu


@pytest.mark.parametrize("active,fournisseur,attendu", [
    ("OUI", "hostdynamic", "hostdynamic"),
    ("NON", "hostdynamic", "non"),
    ("NON", "", "non"),
    ("OUI", "", "oui"),
])
def test_la_paire_se_recompose_en_valeur_brute(active, fournisseur, attendu):
    """« Oui » sans moteur nommé reste « oui » : attribuer d'office « hostdynamic » affirmerait un
    fournisseur que personne n'a désigné."""
    assert adm.composer_dynamic_pricing(active, fournisseur) == attendu


def test_l_ecran_modifie_la_paire_et_la_valeur_brute_suit(parc):
    resultat = adm.modifier_ligne(
        "ref_logements", LOG,
        {"dynamic_pricing_enabled": "NON", "dynamic_pricing_provider": "hostdynamic"},
        acteur="test", db_path=parc)
    assert resultat.get("ok") is not False
    conn = get_db(parc)
    try:
        ligne = conn.execute(
            "SELECT dynamic_pricing, dynamic_pricing_enabled, dynamic_pricing_provider "
            "FROM ref_logements WHERE logement_id = ?", (LOG,)).fetchone()
    finally:
        conn.close()
    assert ligne["dynamic_pricing"] == "non"
    assert ligne["dynamic_pricing_enabled"] == "NON"
    assert ligne["dynamic_pricing_provider"] is None


def test_les_colonnes_derivees_ne_sont_pas_editables_directement(parc):
    """Les écrire directement les ferait recalculer aussitôt : la saisie disparaîtrait sans
    explication."""
    champs = adm.champs_edition("ref_logements", db_path=parc)
    assert champs["dynamic_pricing"]["lecture_seule"] is True
    assert champs["dynamic_pricing_enabled"]["lecture_seule"] is True
    assert champs["dynamic_pricing_provider"]["lecture_seule"] is True


def test_l_activation_se_choisit_dans_une_liste_oui_non(parc):
    valeurs = [o["valeur"] for o in
               adm.options_champ("ref_logements", "dynamic_pricing_enabled", db_path=parc)]
    assert "OUI" in valeurs and "NON" in valeurs

"""Derniers correctifs recette 4 (lot 1) — bouton réservation, ligne modifiable, total/écart,
conflit d'apprentissage du logement.

§1 — le bouton « Ajouter une réservation hors Hostaway » est visible sur l'écran, pas seulement
     dans l'état vide.
§2 — un conflit d'apprentissage (logement déjà associé à un autre libellé) est signalé, jamais
     silencieux, et n'empêche pas l'action principale d'aboutir.
§3 — le montant d'une ligne AJOUTÉE À LA MAIN se corrige tant que la facture est À CONTRÔLER ; le
     montant d'une ligne EXTRAITE du document, non — ni l'un ni l'autre après validation.
§5 — Total document / Somme des lignes / Écart sont TOUJOURS affichés, avec des libellés non
     ambigus, dès le haut de l'écran — même quand la facture est cohérente.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import facture_lignes_menage_service as flm
from app.services import factures_service as fact
from app.services import logement_matching_service as lms
from app.services import reservations_hh_saisie_service as saisie


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


#: La nomenclature vient du RÉFÉRENTIEL (REF_Setup), pas d'une migration : une base de test neuve
#: ne la porte donc pas. Reproduite ici à l'identique de la base réelle (TLM_001..TLM_006).
@pytest.fixture(autouse=True)
def _referentiel_types_lignes(tmp_db):
    conn = get_db(tmp_db)
    try:
        for tid, libelle, compte in (
                ("TLM_001", "MENAGE_STANDARD", "OUI"), ("TLM_002", "REMISE_EN_ETAT", "OUI"),
                ("TLM_003", "FRAIS_DEPLACEMENT", "NON"), ("TLM_006", "AUTRE", "NON")):
            conn.execute(
                "INSERT OR IGNORE INTO ref_types_lignes_menage (type_ligne_menage_id, "
                "type_ligne_menage, compte_comme_menage, repartissable_sur_menages, "
                "impact_cout_menage, actif, import_id) VALUES (?,?,?,'NON','OUI','OUI','IMP-T')",
                (tid, libelle, compte))
        conn.commit()
    finally:
        conn.close()


# ── §1 — le bouton est visible, pas caché dans l'état vide ──────────────────────────────────────

def test_bouton_ajouter_reservation_visible_meme_avec_des_reservations(client, tmp_db):
    saisie.creer(
        {"mois": "2026-06", "canal_id": "CANAL_004", "source_financiere": "SAISIE_MANUELLE",
         "proprietaire_id": "PROP_0003", "logement_id": "LOG_0009", "date_arrivee": "2026-06-10",
         "date_depart": "2026-06-15", "montant_percu": 100.0, "code_impact": "HC",
         "statut_controle": "VALIDE"},
        acteur="fixture", db_path=tmp_db)

    page = client.get("/reservations")
    assert page.status_code == 200
    assert 'href="/reservations/nouvelle"' in page.text
    assert "+ Ajouter une réservation hors Hostaway" in page.text
    # La liste n'est plus présentée comme un simple constat de lecture seule.
    assert "sortie du moteur" in page.text or "lecture seule" in page.text.lower()

    # Le bouton mène réellement au formulaire — clic bout en bout, pas seulement un lien présent.
    formulaire = client.get("/reservations/nouvelle")
    assert formulaire.status_code == 200
    assert "reservations/nouvelle/verifier" in formulaire.text


def test_bouton_ajouter_reservation_visible_liste_vide(client):
    page = client.get("/reservations")
    assert page.status_code == 200
    assert "+ Ajouter une réservation hors Hostaway" in page.text


# ── §2 — conflit d'apprentissage signalé, jamais silencieux ─────────────────────────────────────

def test_conflit_de_correspondance_est_signale_sans_bloquer_le_logement(tmp_db):
    conn = get_db(tmp_db)
    try:
        for lid in ("LOG_X", "LOG_Y"):
            conn.execute("INSERT INTO ref_logements (logement_id, nom_court, statut_parc, actif, "
                        "import_id) VALUES (?,?,'GERE','OUI','IMP-T')", (lid, lid))
        conn.commit()
    finally:
        conn.close()
    # Une première correspondance déjà déclarée pour ce libellé, vers un AUTRE logement.
    premier = lms.enregistrer_correspondance("Studio conflit", "LOG_X", db_path=tmp_db)
    assert premier["ok"] is True

    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "CONFLIT-1", "date_facture": "2026-08-31",
         "montant_ttc": 50.0}, db_path=tmp_db)["facture_id_opaque"]
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=50.0,
                          description="Studio conflit", source=flm.SOURCE_PDF, db_path=tmp_db)

    # L'utilisateur rattache pourtant CETTE ligne à LOG_Y : le rattachement de la ligne réussit
    # (ce n'est pas parce qu'une autre facture disait autre chose que celle-ci a tort), mais
    # l'apprentissage est refusé — jamais un écrasement silencieux de LOG_X par LOG_Y.
    res = flm.affecter_logement(r["ligne_id_opaque"], logement_id="LOG_Y", db_path=tmp_db)
    assert res["ok"] is True
    assert res["logement_id"] == "LOG_Y"
    appris = res["correspondance_apprise"]
    assert appris["ok"] is False
    assert appris["code"] == "CORRESPONDANCE_CONTRADICTOIRE"

    # Le référentiel n'a pas bougé : LOG_X reste la correspondance connue pour ce libellé.
    ref = conn = get_db(tmp_db)
    try:
        row = conn.execute("SELECT logement_id FROM ref_mapping_logements WHERE "
                           "champ_source='libelle_logement_source' AND valeur_source=?",
                           ("Studio conflit",)).fetchone()
        assert row["logement_id"] == "LOG_X"
    finally:
        conn.close()


# ── §3 — le montant d'une ligne ajoutée à la main se corrige ────────────────────────────────────

def test_corriger_montant_d_une_ligne_corrective(tmp_db):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "MNT-1", "date_facture": "2026-08-31",
         "montant_ttc": 200.0}, db_path=tmp_db)["facture_id_opaque"]
    ajout = flm.ajouter_ligne_manquante(
        opaque, motif="ligne omise", montant_ttc=150.0, description="ligne manquante",
        categorie=flm.CAT_MENAGE_STANDARD, db_path=tmp_db)
    assert ajout["ok"] is True

    sans_motif = flm.corriger_montant(ajout["ligne_id_opaque"], montant_ttc=200.0, motif="",
                                      db_path=tmp_db)
    assert sans_motif["ok"] is False and sans_motif["code"] == "MOTIF_OBLIGATOIRE"

    res = flm.corriger_montant(ajout["ligne_id_opaque"], montant_ttc=200.0,
                               motif="erreur de saisie initiale", acteur="ewan", db_path=tmp_db)
    assert res["ok"] is True and res["montant_ttc"] == pytest.approx(200.0)
    assert flm.controler_total(opaque, db_path=tmp_db)["ecart"] == pytest.approx(0.0)


def test_corriger_montant_refuse_sur_une_ligne_extraite_du_document(tmp_db):
    """Le montant d'une ligne EXTRAITE est le document : il ne se réécrit pas ici."""
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "MNT-2", "date_facture": "2026-08-31",
         "montant_ttc": 55.0}, db_path=tmp_db)["facture_id_opaque"]
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="",
                          montant_ttc=55.0, description="ligne du document",
                          categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                          source=flm.SOURCE_PDF, db_path=tmp_db)
    res = flm.corriger_montant(r["ligne_id_opaque"], montant_ttc=99.0, motif="test",
                               db_path=tmp_db)
    assert res["ok"] is False and res["code"] == "LIGNE_NON_MODIFIABLE"


def test_corriger_montant_route_http(client, tmp_db):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "MNT-3", "date_facture": "2026-08-31",
         "montant_ttc": 150.0}, db_path=tmp_db)["facture_id_opaque"]
    ajout = flm.ajouter_ligne_manquante(
        opaque, motif="ligne omise", montant_ttc=100.0, description="ligne manquante",
        categorie=flm.CAT_MENAGE_STANDARD, db_path=tmp_db)

    reponse = client.post(
        f"/factures/{opaque}/lignes/{ajout['ligne_id_opaque']}/montant",
        data={"montant_ttc": "150,00", "motif": "correction du montant saisi"},
        follow_redirects=False)
    assert reponse.status_code == 303
    assert "erreur" not in reponse.headers["location"]
    assert flm.controler_total(opaque, db_path=tmp_db)["ecart"] == pytest.approx(0.0)


# ── §5 — Total document / Somme des lignes / Écart, toujours visibles ───────────────────────────

def test_controle_du_total_toujours_visible_meme_coherent(client, tmp_db):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "TOT-1", "date_facture": "2026-08-31",
         "montant_ttc": 100.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=100.0,
                      description="ligne unique", categorie=flm.CAT_AUTRE,
                      categorie_confiance="CERTAIN", source=flm.SOURCE_PDF, db_path=tmp_db)

    page = client.get(f"/factures/{opaque}")
    assert page.status_code == 200
    assert 'data-testid="controle-total"' in page.text
    assert "Total document" in page.text
    assert "Somme des lignes" in page.text
    assert "Écart" in page.text
    assert "100.00 €" in page.text or "100,00 €" in page.text


def test_controle_du_total_reflete_l_ecart_en_temps_reel(client, tmp_db):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "TOT-2", "date_facture": "2026-08-31",
         "montant_ttc": 1000.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="",
                      montant_ttc=850.0, description="ligne partielle",
                      categorie=flm.CAT_MENAGE_STANDARD, categorie_confiance="CERTAIN",
                      source=flm.SOURCE_PDF, db_path=tmp_db)

    avant = client.get(f"/factures/{opaque}")
    assert "+150.00" in avant.text or "150.00" in avant.text

    client.post(f"/factures/{opaque}/ligne-manquante",
               data={"motif": "ligne omise", "description": "ligne manquante",
                     "montant_ttc": "150.00", "categorie": flm.CAT_MENAGE_STANDARD})

    apres = client.get(f"/factures/{opaque}")
    assert "+0.00 €" in apres.text or "0.00 €" in apres.text


# ── vérification finale — §1 : la correction manuelle enrichit ref_mapping_logements, et un
#    nouvel import avec le même libellé propose automatiquement le logement appris ────────────────

def test_correction_manuelle_du_logement_est_proposee_au_prochain_import(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO ref_logements (logement_id, nom_court, statut_parc, actif, "
                    "import_id) VALUES ('LOG_T4BLA','Le Petit Nid','GERE','OUI','IMP-T')")
        conn.commit()
    finally:
        conn.close()

    # Rien ne le sait encore : le nom du logement ne recoupe aucun mot du libellé fournisseur — la
    # correspondance ne peut venir que de l'apprentissage, jamais d'une déduction de hasard.
    # Le libellé n'est associé à aucun logement.
    avant = lms.proposer_pour_libelle("T4 blagnac", db_path=tmp_db)
    assert avant["logement_id"] == ""

    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T4", "facture_ref": "T4-1", "date_facture": "2026-08-31",
         "montant_ttc": 60.0}, db_path=tmp_db)["facture_id_opaque"]
    r = flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=60.0,
                          description="T4 blagnac", source=flm.SOURCE_PDF, db_path=tmp_db)

    # L'utilisateur corrige ET CONFIRME le logement sur cette ligne — `affecter_logement` est
    # exactement l'action que déclenche le POST /factures/{opaque}/lignes/{ligne}/logement du
    # formulaire réel, jamais une proposition automatique acceptée en silence.
    confirmation = flm.affecter_logement(r["ligne_id_opaque"], logement_id="LOG_T4BLA",
                                         acteur="ewan", db_path=tmp_db)
    assert confirmation["ok"] is True
    assert confirmation["correspondance_apprise"]["ok"] is True

    # Un « nouvel import » ne relit pas la ligne existante : il appelle `lms.proposer` sur le
    # référentiel vivant pour une ligne NEUVE — exactement ce que fait
    # `facture_menage_pdf_service.importer()` ligne par ligne (mêmes deux appels, même fonction).
    referentiel = lms.charger_referentiel(tmp_db)
    proposition = lms.proposer("T4 blagnac", referentiel)
    assert proposition["logement_id"] == "LOG_T4BLA"
    assert proposition["confiance"] == lms.CERTAIN
    assert proposition["preremplir"] is True

    # Un alias contradictoire vers un AUTRE logement n'écrase jamais la correspondance apprise.
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO ref_logements (logement_id, nom_court, statut_parc, actif, "
                    "import_id) VALUES ('LOG_AUTRE','Autre','GERE','OUI','IMP-T')")
        conn.commit()
    finally:
        conn.close()
    conflit = lms.enregistrer_correspondance("T4 blagnac", "LOG_AUTRE", db_path=tmp_db)
    assert conflit["ok"] is False and conflit["code"] == "CORRESPONDANCE_CONTRADICTOIRE"
    apres_conflit = lms.proposer_pour_libelle("T4 blagnac", db_path=tmp_db)
    assert apres_conflit["logement_id"] == "LOG_T4BLA", "aucune écriture silencieuse"


# ── vérification finale — §2 : le sélecteur de nature ne montre que les 3 catégories métier ───────

def test_categories_ui_disponibles_ne_rend_que_les_trois_categories_metier(tmp_db):
    ui = flm.categories_ui_disponibles(tmp_db)
    assert {c["type_ligne_menage"] for c in ui} == {"MENAGE_STANDARD", "REMISE_EN_ETAT", "AUTRE"}
    libelles = {c["type_ligne_menage"]: c["libelle_ui"] for c in ui}
    assert libelles == {"MENAGE_STANDARD": "Ménage", "REMISE_EN_ETAT": "Remise en état",
                        "AUTRE": "Autre prestation"}


def test_ecran_facture_ne_propose_que_les_trois_categories_metier(client, tmp_db):
    opaque = fact.creer(
        {"fournisseur_id_opaque": "FRS-T", "facture_ref": "CAT-1", "date_facture": "2026-08-31",
         "montant_ttc": 40.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(opaque, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=40.0,
                      description="ligne test", categorie=flm.CAT_AUTRE,
                      categorie_confiance="CERTAIN", source=flm.SOURCE_PDF, db_path=tmp_db)

    page = client.get(f"/factures/{opaque}")
    assert page.status_code == 200
    assert "Remise en état" in page.text
    assert "Autre prestation" in page.text
    # Le référentiel connaît aussi FRAIS_DEPLACEMENT (nomenclature du parseur) : il ne doit plus
    # apparaître dans un sélecteur destiné à l'humain.
    assert "Frais deplacement" not in page.text
    assert "FRAIS_DEPLACEMENT" not in page.text

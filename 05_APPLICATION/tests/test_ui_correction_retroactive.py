"""Mission 6 quater §30 — présence effective des éléments UI (pas seulement le backend) : bandeau
rétroactif, champ justification, bouton « Voir les impacts », timeline historique existante."""
import fixtures_referentiel as fx


def test_fiche_logement_affiche_bandeau_justification_et_lien_impacts(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/logements/LOG_A1")
    assert resp.status_code == 200
    html = resp.text
    assert "js-bandeau-retro" in html
    assert "js-justification-retro" in html
    assert "js-lien-impacts" in html
    assert "MODIFICATION RÉTROACTIVE" in html or "Modification rétroactive" in html


def test_ecran_impacts_taux_repond_sans_date(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/logements/LOG_A1/impacts-taux")
    assert resp.status_code == 200
    assert "structurel" in resp.text.lower()


def test_ecran_couts_menage_affiche_bandeau_et_lien_impacts(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_couts_standards_menage")
    assert resp.status_code == 200
    html = resp.text
    assert "js-bandeau-retro" in html
    assert "js-justification-retro" in html
    assert "Voir les impacts" in html


def test_ecran_canape_affiche_bandeau_et_lien_impacts(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_canape_parametres")
    assert resp.status_code == 200
    html = resp.text
    assert "js-bandeau-retro" in html
    assert "Voir les impacts" in html


def test_ecran_regles_versions_affiche_bandeau_et_lien_impacts(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_regles_versions")
    assert resp.status_code == 200
    html = resp.text
    assert "js-bandeau-retro" in html
    assert "Voir les impacts" in html


def test_ecran_impacts_couts_menage_repond(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_couts_standards_menage/impacts")
    assert resp.status_code == 200


def test_ecran_impacts_canape_repond(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_canape_parametres/impacts")
    assert resp.status_code == 200


def test_ecran_impacts_regles_versions_repond(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_regles_versions/impacts")
    assert resp.status_code == 200


def test_pas_derreur_technique_brute_sur_refus_justification(client, tmp_db):
    """§17 : jamais IntegrityError/UNIQUE constraint/Traceback visible pour l'utilisateur."""
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/logements/LOG_A1/changer-taux-commission",
                       data={"taux_commission": "0.20", "date_debut": "2020-01-01"},
                       follow_redirects=True)
    html = resp.text
    for terme in ("IntegrityError", "UNIQUE constraint", "CHECK constraint", "Traceback",
                  "sqlite3."):
        assert terme not in html

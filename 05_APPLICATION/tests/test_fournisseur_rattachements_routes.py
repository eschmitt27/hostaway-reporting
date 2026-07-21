"""APP-3E — Routes de l'association fournisseur ↔ logement (dans la fiche fournisseur existante)."""
from app.services import fournisseurs_referentiel_service as frs


def _creer_fournisseur(client):
    client.post("/referentiel-fournisseurs/creer", data={"nom": "Maint SARL", "type": "MAINTENANCE"})
    import app.config as cfg
    f = frs.lister(db_path=cfg.DB_PATH)[0]
    return f["fournisseur_id_opaque"]


def test_fiche_affiche_section_associations(client):
    opaque = _creer_fournisseur(client)
    r = client.get(f"/referentiel-fournisseurs/{opaque}")
    assert r.status_code == 200
    assert "Logements associés" in r.text


def test_associer_logement(client):
    opaque = _creer_fournisseur(client)
    r = client.post(f"/referentiel-fournisseurs/{opaque}/associer-logement",
                    data={"logement_id": "LOG1", "type_prestation": "MAINTENANCE", "date_debut": "2026-01-01"},
                    follow_redirects=False)
    assert r.status_code == 303
    r2 = client.get(f"/referentiel-fournisseurs/{opaque}")
    assert "LOG1" in r2.text


def test_associer_chevauchement_refuse_sans_500(client):
    opaque = _creer_fournisseur(client)
    client.post(f"/referentiel-fournisseurs/{opaque}/associer-logement",
                data={"logement_id": "LOG1", "type_prestation": "MAINTENANCE", "date_debut": "2026-01-01"})
    r = client.post(f"/referentiel-fournisseurs/{opaque}/associer-logement",
                    data={"logement_id": "LOG1", "type_prestation": "MAINTENANCE", "date_debut": "2026-02-01"},
                    follow_redirects=False)
    assert r.status_code == 303   # refus propre, pas de 500


def test_fermer_association(client):
    opaque = _creer_fournisseur(client)
    client.post(f"/referentiel-fournisseurs/{opaque}/associer-logement",
                data={"logement_id": "LOG1", "type_prestation": "MAINTENANCE", "date_debut": "2026-01-01"})
    from app.services import fournisseur_rattachements_service as fl
    import app.config as cfg
    a = fl.lister_par_logement("LOG1", db_path=cfg.DB_PATH)[0]
    r = client.post(f"/referentiel-fournisseurs/{opaque}/fermer-association",
                    data={"association_opaque": a["association_id_opaque"], "date_fin": "2026-06-30"},
                    follow_redirects=False)
    assert r.status_code == 303
    a2 = fl.charger_par_opaque(a["association_id_opaque"], db_path=cfg.DB_PATH)
    assert a2["date_fin"] == "2026-06-30"

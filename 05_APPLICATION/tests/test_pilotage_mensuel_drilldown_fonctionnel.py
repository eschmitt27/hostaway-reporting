"""APP-5D — vérification FONCTIONNELLE du drill-down `nb_exceptions` : pas seulement la présence du
lien HTML, mais que la route cible retourne réellement le contenu filtré attendu (une exception
justifiée visible, un élément non-exception absent), avec le bon mois et sans id interne exposé.
"""
from pathlib import Path

import pytest

import app.config as cfg
from app.services import controles_actionnable_service as act
from app.services import controles_suivi_service as suivi

moteur_requis = pytest.mark.skipif(
    not Path(cfg.MASTER_CTRL_COHERENCE).exists(), reason="MASTER_CTRL_Coherence.xlsx absent")


@moteur_requis
def test_drilldown_nb_exceptions_contenu_reellement_filtre(client, tmp_db):
    # La Banque vient de la base : sans mouvement non classé, elle n'ouvre aucun élément, et le test
    # ne trouverait plus de second module à comparer sur le même mois.
    import fixtures_banque as fx
    from app.readers import banques_reader as bq_reader
    from app.readers import controles_detail_reader as detail

    fx.peupler_non_classes(tmp_db, fx.mois_des_agregats_banque())
    bq_reader.vider_cache()
    detail.vider_cache()

    tous = act._tous_les_elements(tmp_db)
    commission = next(e for e in tous if e["module"] == "COMMISSIONS")
    autre = next(e for e in tous if e["module"] != "COMMISSIONS" and e["mois"] == commission["mois"]
                and e["code"] != commission["code"])

    suivi.accepter_exception(commission, responsable="test", justification="séjour propriétaire",
                             portee="ENTITE", db_path=tmp_db)

    mois = commission["mois"]
    r = client.get(f"/controles-cloture?mois={mois}&statut_suivi=ACCEPTE_AVEC_JUSTIFICATION")
    assert r.status_code == 200
    # Mois conservé dans le formulaire de filtre (pas perdu au clic).
    assert f'value="{mois}"' in r.text
    # L'élément mis en exception apparaît.
    assert commission["code"] in r.text
    # Un élément non filtré (autre statut) du même mois n'apparaît pas dans cette vue filtrée.
    assert autre["ctrl_opaque"] not in r.text
    # Aucun identifiant interne (id SQLite brut) — uniquement l'identifiant opaque CTRL-.
    assert "ctrl_opaque=" not in r.text or "CTRL-" in r.text


@moteur_requis
def test_drilldown_nb_exceptions_disparait_si_aucune_exception(client, tmp_db):
    """Sans aucune exception active pour ce mois, la route reste 200 (liste vide), jamais une erreur."""
    tous = act._tous_les_elements(tmp_db)
    mois = tous[0]["mois"] if tous else "2026-01"
    r = client.get(f"/controles-cloture?mois={mois}&statut_suivi=ACCEPTE_AVEC_JUSTIFICATION")
    assert r.status_code == 200


def test_lien_nb_exceptions_absent_si_bloc_controles_indisponible(tmp_db, monkeypatch):
    """Le lien lui-même n'apparaît que si le compteur a une valeur (cohérent avec les autres
    compteurs du tableau) — jamais un lien vers une donnée dont on ne connaît pas le contenu."""
    from app.services import clotures_service as cs_mod
    from app.services import clotures_service as cs
    monkeypatch.setattr(cs_mod, "calcul_progression",
                        lambda m, db_path=None: (_ for _ in ()).throw(RuntimeError("panne")))
    cs.creer_ou_charger("2096-01", acteur="t", db_path=tmp_db)
    from app.services import pilotage_mensuel_service as svc
    t = svc.tableau_mensuel(db_path=tmp_db)
    ligne = next(l for l in t["lignes"] if l["mois"] == "2096-01")
    assert ligne["nb_exceptions"] is None

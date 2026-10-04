"""Factures clients — suppression avant émission et comptabilisation guidée (2026-10-04).

  · une facture non émise (brouillon / validée, sans numéro) se supprime ; émise ou comptabilisée,
    jamais — refus côté SERVEUR, pas seulement un bouton masqué ;
  · émettre ne comptabilise plus : « Non comptabilisée » → Comptabiliser → proposition (lecture
    pure, mappings du module Comptabilité) → validation humaine → écriture VENTES VALIDÉE ;
  · une facture ne se comptabilise jamais deux fois.

Base temporaire, données fictives.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_proprietaires_brouillon_service as brouillon
from app.services import factures_proprietaires_service as svc

PID = "PROP_SC"
EMETTEUR = {"nom": "Conciergerie SC", "adresse": "1 rue SC", "siret": "00000000000000"}
DEST = {"nom": "Client SC", "adresse": "2 rue SC"}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups", raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True, raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_NOM", "Conciergerie SC", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_ADRESSE", "1 rue SC", raising=False)
    monkeypatch.setattr(cfg, "SOCIETE_SIRET", "00000000000000", raising=False)
    apply_migrations(chemin)
    conn = get_db(chemin)
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-SC','2026-09-01T00:00:00','x','x','IMPORTE',1,1)")
    conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
                 "prenom_proprietaire, actif, import_id) VALUES (?, 'Dupont', 'Jean', 'OUI', "
                 "'IMP-SC')", (PID,))
    conn.commit()
    conn.close()
    from app.main import app as application
    return TestClient(application), chemin


def _brouillon(db, montant=200.0, logement="LOG_SC"):
    return svc.creer({"mois": "2026-09", "proprietaire_id": PID, "logement_id": logement,
                      "source_calcul": f"PREF-{logement}", "COMMISSION_CONCIERGERIE": montant,
                      "montant_du_conciergerie": montant}, db_path=db)["facture_id_opaque"]


def _emise(client, db, logement="LOG_SC"):
    """Émission par la ROUTE réelle : vérifie au passage qu'elle ne comptabilise plus."""
    fid = _brouillon(db, logement=logement)
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    r = client.post(f"/factures-proprietaires/{fid}/emettre",
                    data={"date_facture": "2026-10-04", "comptabilite": "EN_COMPTA"},
                    follow_redirects=False)
    assert r.status_code == 303
    return fid


def _ecritures_facture(db, fid):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ecritures WHERE origine_type=? AND origine_id_opaque=?",
            (compta.ORIGINE_FACTURE, fid))]
    finally:
        conn.close()


def _existe(db, fid):
    conn = get_db(db)
    try:
        return conn.execute("SELECT COUNT(*) FROM factures_proprietaires WHERE facture_id_opaque=?",
                            (fid,)).fetchone()[0] == 1
    finally:
        conn.close()


def _menu(client, fid):
    soup = BeautifulSoup(client.get("/factures-proprietaires").text, "html.parser")
    for tr in soup.select("tbody tr"):
        if tr.select_one(f'a[href="/factures-proprietaires/{fid}"]'):
            return [x.get_text(strip=True) for x in tr.select(".fc-menu__item")], tr
    raise AssertionError("ligne absente")


# ── Suppression ─────────────────────────────────────────────────────────────────────────────────

def test_suppression_brouillon_autorisee_avec_confirmation(env):
    client, db = env
    fid = _brouillon(db)
    autre = _brouillon(db, logement="LOG_AUTRE")
    actions, tr = _menu(client, fid)
    assert "Supprimer la facture" in actions
    form = tr.select_one("form[data-fc-supprimer]")
    assert form["data-fc-confirmer"] == "Supprimer définitivement cette facture non émise ?"
    dlg = BeautifulSoup(client.get("/factures-proprietaires").text, "html.parser") \
        .select_one("#fc-dialogue-supprimer")
    assert [b.get_text(strip=True) for b in dlg.select("button")] == ["Annuler", "Supprimer"]

    r = client.post(form["action"], follow_redirects=False)
    assert r.status_code == 303
    assert not _existe(db, fid)
    assert _existe(db, autre)                         # aucun impact sur les autres factures


def test_suppression_facture_validee_non_emise(env):
    client, db = env
    fid = _brouillon(db)
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    assert brouillon.supprimable_non_emise(svc.lire(fid, db_path=db))
    client.post(f"/factures-proprietaires/{fid}/supprimer", follow_redirects=False)
    assert not _existe(db, fid)


def test_suppression_emise_refusee_serveur_et_numero_conserve(env):
    client, db = env
    fid = _emise(client, db)
    numero = svc.lire(fid, db_path=db)["numero_facture"]
    actions, _ = _menu(client, fid)
    assert "Supprimer la facture" not in actions
    r = client.post(f"/factures-proprietaires/{fid}/supprimer", follow_redirects=False)
    assert r.status_code == 422
    assert _existe(db, fid) and svc.lire(fid, db_path=db)["numero_facture"] == numero
    with pytest.raises(svc.FactureProprietaireError):
        brouillon.supprimer_non_emise(fid, db_path=db)


def test_suppression_comptabilisee_refusee(env):
    client, db = env
    fid = _emise(client, db)
    assert compta.valider_comptabilisation_facture(fid, db_path=db)["ok"]
    r = client.post(f"/factures-proprietaires/{fid}/supprimer", follow_redirects=False)
    assert r.status_code == 422
    assert _existe(db, fid)
    assert len(_ecritures_facture(db, fid)) == 1


# ── Comptabilisation guidée ────────────────────────────────────────────────────────────────────

def test_emission_laisse_non_comptabilisee_et_propose_comptabiliser(env):
    client, db = env
    fid = _emise(client, db)
    assert _ecritures_facture(db, fid) == []          # émettre n'écrit plus rien en compta
    actions, tr = _menu(client, fid)
    assert actions == ["Voir", "Comptabiliser"]
    assert "Non comptabilisée" in tr.select_one('[data-testid="etat-compta"]').get_text()
    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}").text, "html.parser")
    assert soup.select_one('[data-testid="bloc-compta-non-comptabilisee"]')
    assert soup.select_one('[data-testid="action-comptabiliser"]')["href"].endswith("/comptabiliser")


def test_menu_brouillon_et_validee(env):
    client, db = env
    fid = _brouillon(db)
    assert _menu(client, fid)[0] == ["Compléter / modifier", "Valider", "Supprimer la facture"]
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    assert _menu(client, fid)[0] == ["Voir", "Émettre", "Supprimer la facture"]


def test_previsualisation_lisible_equilibree_mappings_existants_sans_ecrire(env):
    client, db = env
    fid = _emise(client, db)
    p = compta.proposition_comptabilisation_facture(svc.lire(fid, db_path=db), db_path=db)
    assert p["ok"] and p["equilibree"] and p["journal"] == "VENTES"
    assert p["total_debit"] == p["total_credit"] == 200.0
    assert p["client"] == p["auxiliaire"] and "Dupont" in p["client"] and PID not in p["client"]
    # Comptes = ceux du moteur (mapping_produits_facture), pas une constante de la route.
    attendu = compta.mapping_produits(db_path=db)["GESTION"]["compte"]
    assert {l["compte"] for l in p["lignes"] if l["credit"]} == {attendu}
    assert p["tva"] == 0

    page = client.get(f"/factures-proprietaires/{fid}/comptabiliser")
    assert page.status_code == 200
    soup = BeautifulSoup(page.text, "html.parser")
    assert "Dupont" in soup.select_one('[data-testid="prop-auxiliaire"]').get_text()
    assert soup.select_one('[data-testid="valider-comptabilisation"]')
    assert _ecritures_facture(db, fid) == []          # l'aperçu n'écrit rien


def test_mapping_manquant_bloque_la_validation(env):
    client, db = env
    fid = _emise(client, db)
    conn = get_db(db)
    conn.execute("UPDATE mapping_produits_facture SET actif=0 WHERE type_economique='GESTION'")
    conn.commit()
    conn.close()
    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}/comptabiliser").text,
                         "html.parser")
    assert soup.select_one('[data-testid="compte-a-confirmer"]')
    assert soup.select_one('[data-testid="valider-comptabilisation"]') is None
    r = client.post(f"/factures-proprietaires/{fid}/comptabiliser", follow_redirects=False)
    assert r.status_code == 303 and "erreur=" in r.headers["location"]
    assert _ecritures_facture(db, fid) == []


def test_validation_cree_ecriture_lien_et_statut(env):
    client, db = env
    fid = _emise(client, db)
    autre = _emise(client, db, logement="LOG_AUTRE")
    r = client.post(f"/factures-proprietaires/{fid}/comptabiliser", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].endswith("#comptabilite")
    ecr = _ecritures_facture(db, fid)
    assert len(ecr) == 1 and ecr[0]["statut"] == compta.ST_VALIDEE and ecr[0]["journal"] == "VENTES"
    assert ecr[0]["total_debit"] == ecr[0]["total_credit"] == 200.0
    etat = compta.etat_comptabilisation_facture(svc.lire(fid, db_path=db), db_path=db)
    assert etat["etat"] == compta.COMPTA_COMPTABILISEE and etat["date_comptabilisation"]

    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}").text, "html.parser")
    assert soup.select_one('[data-testid="bloc-compta-comptabilisee"]')
    lien = soup.select_one('[data-testid="voir-ecriture"]')["href"]
    assert lien == f"/comptabilite/ecritures/{ecr[0]['ecriture_id_opaque']}"
    assert client.get(lien).status_code == 200
    assert _menu(client, fid)[0] == ["Voir", "Voir l'écriture comptable"]
    assert _ecritures_facture(db, autre) == []        # l'autre facture n'est pas touchée


def test_seconde_comptabilisation_refusee_sans_doublon(env):
    client, db = env
    fid = _emise(client, db)
    assert compta.valider_comptabilisation_facture(fid, db_path=db)["ok"]
    res = compta.valider_comptabilisation_facture(fid, db_path=db)
    assert not res["ok"] and res["code"] == compta.E_DEJA_COMPTABILISEE
    r = client.post(f"/factures-proprietaires/{fid}/comptabiliser", follow_redirects=False)
    assert "erreur=" in r.headers["location"]
    assert len(_ecritures_facture(db, fid)) == 1
    soup = BeautifulSoup(client.get(f"/factures-proprietaires/{fid}").text, "html.parser")
    assert soup.select_one('[data-testid="action-comptabiliser"]') is None


def test_ecriture_proposee_heritee_est_validee_sans_doublon(env):
    """Facture émise AVANT ce changement : l'écriture PROPOSÉE existante est reprise et validée."""
    client, db = env
    fid = _emise(client, db)
    compta.comptabiliser_facture_emise(svc.lire(fid, db_path=db), db_path=db)
    assert _ecritures_facture(db, fid)[0]["statut"] == compta.ST_PROPOSEE
    assert compta.valider_comptabilisation_facture(fid, db_path=db)["ok"]
    ecr = _ecritures_facture(db, fid)
    assert len(ecr) == 1 and ecr[0]["statut"] == compta.ST_VALIDEE


def test_brouillon_non_comptabilisable(env):
    client, db = env
    fid = _brouillon(db)
    assert not compta.valider_comptabilisation_facture(fid, db_path=db)["ok"]
    assert _ecritures_facture(db, fid) == []


def test_base_temporaire_uniquement(env):
    _, db = env
    assert Path(cfg.DB_PATH) == db and db.parent != Path(cfg.__file__).resolve().parent.parent / "data"


def test_emise_puis_annulee_non_supprimable(env):
    """Une facture déjà émise ne se supprime jamais, même passée ensuite au statut ANNULE
    (état forcé en base : le service refuse d'annuler une émise, la règle ne doit pas en dépendre)."""
    client, db = env
    fid = _emise(client, db)
    numero = svc.lire(fid, db_path=db)["numero_facture"]
    conn = get_db(db)
    conn.execute("UPDATE factures_proprietaires SET statut='ANNULE' WHERE facture_id_opaque=?", (fid,))
    conn.commit()
    conn.close()
    f = svc.lire(fid, db_path=db)
    assert not brouillon.supprimable(f) and not brouillon.supprimable_non_emise(f)
    assert "Supprimer la facture" not in _menu(client, fid)[0]
    r = client.post(f"/factures-proprietaires/{fid}/supprimer", follow_redirects=False)
    assert r.status_code == 422
    with pytest.raises(svc.FactureProprietaireError):
        brouillon.supprimer_annulee(fid, db_path=db)
    assert brouillon.supprimer_annulees(db_path=db) == []
    assert _existe(db, fid) and svc.lire(fid, db_path=db)["numero_facture"] == numero


def test_trace_emission_seule_interdit_la_suppression(env):
    """Même sans numéro ni date, un historique montrant un passage à EMIS interdit la suppression."""
    _, db = env
    fid = _brouillon(db)
    conn = get_db(db)
    conn.execute("INSERT INTO factures_proprietaires_evenements (facture_id_opaque, type_evenement, "
                 "ancien_statut, nouveau_statut) VALUES (?, 'EMISSION', 'VALIDE', 'EMIS')", (fid,))
    conn.commit()
    conn.close()
    assert not brouillon.supprimable_non_emise(svc.lire(fid, db_path=db))
    with pytest.raises(svc.FactureProprietaireError):
        brouillon.supprimer_non_emise(fid, db_path=db)
    assert _existe(db, fid)

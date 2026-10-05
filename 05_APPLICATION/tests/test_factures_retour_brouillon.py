"""Retrait d'émission avant comptabilisation : transaction, traçabilité et serveur."""
import json

import pytest
from bs4 import BeautifulSoup

from app.db.connection import get_db
from app.services import compte_proprietaire_service as compte
from app.services import factures_proprietaires_service as svc
from app.services import factures_proprietaires_edition_service as edition
from app.services import credits_clients_service as credits
from app.services import comptabilite_ecritures_service as compta
from test_factures_clients_suppression_comptabilisation import (
    env, _brouillon, _emise, _menu, _ecritures_facture, EMETTEUR, DEST, PID,
)


def _photo(db):
    c = get_db(db)
    try:
        return {t: [tuple(r) for r in c.execute('SELECT * FROM "'+t+'"')]
                for (t,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    finally:
        c.close()


@pytest.mark.parametrize('emis', [False, True])
def test_cycle_menu_confirmation_modification_revalidation_reemission(env, emis):
    client, db = env
    autre = _brouillon(db, logement='AUTRE')
    original_autre = svc.lire(autre, db_path=db)
    fid = _emise(client, db) if emis else _brouillon(db)
    if not emis:
        svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    avant = svc.lire(fid, db_path=db)
    assert 'Remettre en brouillon' in _menu(client, fid)[0]
    for url in ['/factures-proprietaires', f'/factures-proprietaires/{fid}']:
        page = BeautifulSoup(client.get(url).text, 'html.parser')
        assert page.select_one('#fc-dialogue-brouillon[aria-labelledby][aria-describedby]')
        form = page.select_one(f'form[action="/factures-proprietaires/{fid}/repasser-en-brouillon"]')
        assert form['data-fc-brouillon'] == ('emise' if emis else 'validee')
        assert ('ancien numéro' if emis else 'recalculée') in form['data-fc-confirmer']
        assert page.select_one('[data-fc-brouillon-annuler][autofocus]')
    seq = svc.prochain_numero('2026-09', db_path=db)
    assert client.post(f'/factures-proprietaires/{fid}/repasser-en-brouillon', follow_redirects=False).status_code == 303
    f = svc.lire(fid, db_path=db)
    assert f['statut'] == 'BROUILLON' and f['lignes'] == avant['lignes']
    for k in ['numero_facture','date_emission','date_validation','date_facture','snapshot_json','snapshot_hash','document_nom','document_hash']:
        assert f[k] is None
    assert client.get(f'/factures-proprietaires/{fid}/document').status_code == 404
    assert svc.prochain_numero('2026-09', db_path=db) == seq
    trace = json.loads(f['evenements'][-1]['commentaire'])
    assert trace['ancien_numero'] == avant['numero_facture']
    assert trace['ancienne_date_emission'] == avant['date_emission']
    assert f['evenements'][-1]['acteur'] == 'interface'
    assert f['evenements'][-1]['ancien_statut'] == avant['statut']
    assert svc.lire(autre, db_path=db) == original_autre
    photo = _photo(db)
    client.post(f'/factures-proprietaires/{fid}/repasser-en-brouillon', follow_redirects=False)
    assert _photo(db) == photo  # double POST strictement idempotent
    svc.modifier_ligne(fid, f['lignes'][0]['ligne_id_opaque'], montant=210, db_path=db)
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    nouveau = svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture='2026-10-05', db_path=db)
    assert nouveau['numero_facture'] == seq
    if emis:
        assert nouveau['numero_facture'] != avant['numero_facture']
        assert compte.etat_persistance(PID, db_path=db)['a_jour']


def test_proposition_heritee_supprimee_avec_toutes_dependances(env):
    client, db = env
    fid = _emise(client, db)
    autre = _emise(client, db, logement='AUTRE')
    compta.comptabiliser_facture_emise(svc.lire(fid, db_path=db), db_path=db)
    compta.comptabiliser_facture_emise(svc.lire(autre, db_path=db), db_path=db)
    autres = _ecritures_facture(db, autre)
    eid = _ecritures_facture(db, fid)[0]['ecriture_id_opaque']
    c = get_db(db)
    c.execute("INSERT INTO ecriture_ligne_ventilation (ecriture_id_opaque,ligne_num,methode) VALUES (?,1,'SANS_DIMENSION')", (eid,))
    c.commit(); c.close()
    svc.repasser_en_brouillon(fid, db_path=db)
    c = get_db(db)
    for t in ['ecritures','ecriture_lignes','ecriture_evenements','ecriture_ligne_ventilation']:
        assert not c.execute(f'SELECT 1 FROM {t} WHERE ecriture_id_opaque=?', (eid,)).fetchone()
    assert not c.execute('PRAGMA foreign_key_check').fetchall()
    c.close()
    assert _ecritures_facture(db, autre) == autres


@pytest.mark.parametrize('statut', ['VALIDEE', 'CONTREPASSEE'])
def test_comptabilisee_refusee_service_post_menu_sans_aucune_mutation(env, statut):
    client, db = env
    fid = _emise(client, db)
    assert compta.valider_comptabilisation_facture(fid, db_path=db)['ok']
    if statut == 'CONTREPASSEE':
        eid = _ecritures_facture(db, fid)[0]['ecriture_id_opaque']
        assert compta.contrepasser(eid, db_path=db)['ok']
    photo = _photo(db)
    assert 'Remettre en brouillon' not in _menu(client, fid)[0]
    assert client.post(f'/factures-proprietaires/{fid}/repasser-en-brouillon').status_code == 422
    with pytest.raises(svc.FactureProprietaireError):
        svc.repasser_en_brouillon(fid, db_path=db)
    assert _photo(db) == photo


def _credit(db, montant=75):
    c = get_db(db)
    c.execute("INSERT INTO credits_clients (credit_id_opaque,proprietaire_id,origine,mode_origine,date_origine,montant_initial,compte_source,justification,statut,cree_par) VALUES ('CRD-RETOUR',?,'REVERSEMENT_AIRBNB','JUSTIFIE','2026-09-01',?,'467000','test','DISPONIBLE','test')", (PID,montant))
    c.commit(); c.close()


def test_credit_restitue_source_conservee_et_reutilisable(env):
    client, db = env
    _credit(db)
    fid = _emise(client, db)
    c = get_db(db)
    assert credits.credits_disponibles(c, PID) == []
    source = dict(c.execute('SELECT * FROM credits_clients').fetchone())
    c.close()
    svc.repasser_en_brouillon(fid, db_path=db)
    c = get_db(db)
    assert credits.credits_disponibles(c, PID)[0]['reste'] == 75
    assert dict(c.execute('SELECT * FROM credits_clients').fetchone()) == source
    c.close()
    assert svc.reversements_airbnb(fid, db_path=db) == []
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture='2026-10-05', db_path=db)
    assert len(svc.reversements_airbnb(fid, db_path=db)) == 1


def test_acompte_reversement_detaches_sans_perdre_les_mouvements(env):
    client, db = env
    fid = _emise(client, db)
    edition.ajouter_acompte(fid, montant=20, date_mouvement='2026-09-01', db_path=db)
    edition.ajouter_reversement_airbnb(fid, montant=10, date_imputation='2026-09-01', db_path=db)
    c = get_db(db)
    mtp = dict(c.execute('SELECT * FROM mouvements_tresorerie_proprietaires').fetchone())
    imp = dict(c.execute('SELECT * FROM imputations_airbnb').fetchone())
    c.close()
    svc.repasser_en_brouillon(fid, db_path=db)
    assert svc.acomptes_proprietaire(fid, db_path=db) == []
    assert svc.reversements_airbnb(fid, db_path=db) == []
    c = get_db(db)
    new_mtp = dict(c.execute('SELECT * FROM mouvements_tresorerie_proprietaires').fetchone())
    new_imp = dict(c.execute('SELECT * FROM imputations_airbnb').fetchone())
    assert new_mtp == {**mtp, 'reference_metier':None, 'version':mtp['version']+1}
    assert new_imp == {**imp, 'document_id':None}
    assert compte.etat_persistance(PID, db_path=db)['a_jour']
    c.close()


def test_panne_fifo_rollback_integral_credit_pdf_numero_proposition(env, monkeypatch):
    client, db = env
    _credit(db)
    fid = _emise(client, db)
    compta.comptabiliser_facture_emise(svc.lire(fid, db_path=db), db_path=db)
    photo = _photo(db)
    def panne(*args, **kwargs):
        raise RuntimeError('panne injectée en fin de transaction')
    monkeypatch.setattr(compte, 'recalculer_dans_transaction', panne)
    with pytest.raises(RuntimeError):
        svc.repasser_en_brouillon(fid, db_path=db)
    assert _photo(db) == photo
    assert client.get(f'/factures-proprietaires/{fid}/document').status_code == 200


@pytest.mark.parametrize('emis', [False, True])
def test_charge_refacturee_liberee_sans_perdre_source_ou_ligne(env, emis):
    from test_charges_refacturation_positions import _charge, _position_de
    from app.services import factures_proprietaires_composition_service as compo
    client, db = env
    fid = _brouillon(db)
    cid = _charge(db, 30, 'OUI', logement_id='LOG_SC', proprietaire_id=PID, mois='2026-09', date_charge='2026-09-01')
    pos = _position_de(db,cid)
    compo.rattacher_charge(fid, pos['position_id'], db_path=db)
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    assert _position_de(db,cid)['montant_impute_total'] == 30
    if emis:
        svc.emettre(fid, emetteur=EMETTEUR, destinataire=DEST, date_facture='2026-10-05', db_path=db)
    avant = svc.lire(fid, db_path=db)['lignes']
    svc.repasser_en_brouillon(fid, db_path=db)
    assert _position_de(db,cid)['montant_impute_total'] == 0
    assert svc.lire(fid, db_path=db)['lignes'] == avant
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DEST, db_path=db)
    assert _position_de(db,cid)['montant_impute_total'] == 30


@pytest.mark.parametrize('statut', ['PROPOSEE', 'VALIDEE'])
def test_ecriture_imputation_credit_controlee_egalement(env, statut):
    client, db = env
    _credit(db)
    fid = _emise(client, db)
    c = get_db(db)
    imp = c.execute('SELECT imputation_airbnb_id FROM imputations_airbnb WHERE document_id=?', (fid,)).fetchone()[0]
    c.execute("INSERT INTO ecritures (ecriture_id_opaque,journal,date_ecriture,periode,origine_type,origine_id_opaque,statut,libelle) VALUES ('ECR-IMP','ODIVERSES','2026-10-05','2026-09','IMPUTATION_CREDIT',?,?, 'credit')", (imp,statut))
    c.commit(); c.close()
    photo = _photo(db)
    if statut == 'VALIDEE':
        assert 'Remettre en brouillon' not in _menu(client,fid)[0]
        assert client.post(f'/factures-proprietaires/{fid}/repasser-en-brouillon').status_code == 422
        assert _photo(db) == photo
    else:
        svc.repasser_en_brouillon(fid,db_path=db)
        c = get_db(db)
        assert not c.execute("SELECT 1 FROM ecritures WHERE ecriture_id_opaque='ECR-IMP'").fetchone()
        assert credits.credits_disponibles(c,PID)[0]['reste'] == 75
        c.close()


def test_vente_agregee_historique_refuse_retour(env):
    client, db = env
    fid = _emise(client,db)
    c = get_db(db)
    c.execute("INSERT INTO ecritures (ecriture_id_opaque,journal,date_ecriture,periode,origine_type,origine_id_opaque,statut,libelle) VALUES ('ECR-LEGACY','VENTES','2026-10-05','2026-09',? ,?,'VALIDEE','legacy')", (compta.ORIGINE_LOT12,PID+':2026-09'))
    c.commit();c.close()
    photo = _photo(db)
    with pytest.raises(svc.FactureProprietaireError):
        svc.repasser_en_brouillon(fid,db_path=db)
    assert _photo(db) == photo

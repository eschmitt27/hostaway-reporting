"""Noms explicites sans changement du stockage, de la référence ou des octets PDF."""
import hashlib
from pathlib import Path
from urllib.parse import unquote

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import factures_proprietaires_nom_pdf as noms
from app.services import factures_proprietaires_service as svc
from test_factures_clients_suppression_comptabilisation import env, _emise, PID


@pytest.mark.parametrize('prenom,type_bien,adresse,numero,attendu', [
    ('David','T3','18 RUE DE CUGNAUX','2026-09-002','Facture - David - T3 18 - 2026-09-002.pdf'),
    ('François','T3','4 RUE BARDOU','2026-09-004','Facture - François - T3 4 - 2026-09-004.pdf'),
    ('Caroline','STUDIO','4 rue du Puits Vert','2026-09-005','Facture - Caroline - Studio 4 - 2026-09-005.pdf'),
    ('Caroline','Studio','','2026-09-005','Facture - Caroline - Studio - 2026-09-005.pdf'),
    ('Jean-Luc','T2','12 BIS RUE EXEMPLE','2026-09-006','Facture - Jean-Luc - T2 12 bis - 2026-09-006.pdf'),
])
def test_convention_et_accents(prenom,type_bien,adresse,numero,attendu):
    assert noms.nom_telechargement({'numero_facture':numero},
        proprietaire={'prenom_proprietaire':prenom},
        logement={'adresse':adresse,'nom_court':'Marketing trompeur'},type_logement=type_bien) == attendu


def test_fallback_prenom_et_nom_court_sans_type():
    assert noms.nom_telechargement({'numero_facture':'2026-09-001'},
        proprietaire={'nom_affichage':'David Touré'},logement={'nom_court':'Petit loft'},type_logement='') == 'Facture - David - Petit loft - 2026-09-001.pdf'


def test_plusieurs_logements_distingues():
    base={'numero_facture':'2026-09-001'}
    p={'prenom_proprietaire':'François'}
    a=noms.nom_telechargement(base,proprietaire=p,logement={'adresse':'4 rue A'},type_logement='T3')
    b=noms.nom_telechargement(base,proprietaire=p,logement={'adresse':'18 rue B'},type_logement='T3')
    assert a != b and 'T3 4' in a and 'T3 18' in b


@pytest.mark.parametrize('absent', [None, '', 'None', 'null', 'undefined'])
def test_donnees_manquantes_ne_produisent_pas_de_sentinelles(absent):
    nom=noms.nom_telechargement({'numero_facture':absent},
        proprietaire={'prenom_proprietaire':absent},logement={'nom_court':absent,'adresse':absent},type_logement='')
    assert nom == 'Facture - Client - Logement - Brouillon.pdf'


def test_caracteres_windows_et_controles_nettoyes():
    nom=noms.nom_telechargement({'numero_facture':'2026/09:001'},
        proprietaire={'prenom_proprietaire':'  Jo/<>:"?*\\|   --  Émile. '},
        logement={'nom_court':' Loft --\n  test. '},type_logement='')
    assert not set('\\/:*?"<>|').intersection(nom)
    assert '  ' not in nom and '--' not in nom and '\n' not in nom
    assert 'Émile' in nom and nom.endswith('.pdf')


def test_content_disposition_ascii_et_utf8_sans_injection():
    nom='Facture - François - T3 4 - 2026-09-004.pdf'
    entete=noms.content_disposition(nom)
    assert 'filename="Facture - Francois - T3 4 - 2026-09-004.pdf"' in entete
    assert unquote(entete.split("filename*=UTF-8''")[1]) == nom
    assert entete.startswith('attachment;')
    assert noms.content_disposition(nom,inline=True).startswith('inline;')
    assert '\r' not in noms.content_disposition('x\r\ny.pdf')
    assert '\n' not in noms.content_disposition('x\r\ny.pdf')


def test_routes_centralisees_referentiel_prioritaire_et_octets_inchanges(env,monkeypatch):
    client,db=env
    c=get_db(db)
    c.execute("UPDATE ref_proprietaires SET prenom_proprietaire='François',nom_proprietaire='Maurer' WHERE proprietaire_id=?",(PID,))
    c.execute("INSERT INTO ref_types_logements (type_logement_id,type_logement,import_id) VALUES ('TYPE_PDF','T3','IMP-SC')")
    c.execute("INSERT INTO ref_logements (logement_id,type_logement_id,adresse,nom_court,import_id) VALUES ('LOG_SC','TYPE_PDF','4 RUE BARDOU','Studio marketing','IMP-SC')")
    c.commit();c.close()
    fid=_emise(client,db)
    f=svc.lire(fid,db_path=db)
    avant=f.copy()
    fichier=Path(cfg.DATA_DIR)/'factures_proprietaires'/'2026'/'09'/f['document_nom']
    contenu=fichier.read_bytes()
    appels=[]
    original=noms.nom_telechargement
    def trace(facture,**kwargs):
        appels.append(facture['facture_id_opaque'])
        return original(facture,**kwargs)
    monkeypatch.setattr(noms,'nom_telechargement',trace)
    response=client.get(f'/factures-proprietaires/{fid}/document')
    assert response.status_code == 200 and response.content == contenu
    assert hashlib.sha256(response.content).hexdigest() == f['document_hash']
    nom='Facture - François - T3 4 - '+f['numero_facture']+'.pdf'
    assert unquote(response.headers['content-disposition'].split("filename*=UTF-8''")[1]) == nom
    apercu=client.get(f'/factures-proprietaires/{fid}/previsualiser')
    assert apercu.status_code == 200 and apercu.content == contenu
    assert unquote(apercu.headers['content-disposition'].split("filename*=UTF-8''")[1]) == nom
    assert appels == [fid,fid]
    assert svc.lire(fid,db_path=db) == avant
    assert fichier.read_bytes() == contenu
    assert f['document_nom'] == f['numero_facture']+'.pdf'
    assert f['numero_facture'] in client.get(f'/factures-proprietaires/{fid}').text

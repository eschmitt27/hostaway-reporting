"""Nom de téléchargement uniquement : ni stockage, ni numéro, ni contenu PDF modifié."""
import re
import unicodedata
from urllib.parse import quote

from app.services import referentiel_service as ref


def _texte(valeur) -> str:
    texte = str(valeur or '').strip()
    return '' if texte.lower() in ('none', 'null', 'undefined', 'nan') else texte


def nettoyer(valeur) -> str:
    texte = unicodedata.normalize('NFC', _texte(valeur))
    texte = re.sub(r'[\\/:*?"<>|\x00-\x1f\x7f]', ' ', texte)
    texte = re.sub(r'(?:\s*-\s*){2,}', ' - ', texte)
    return re.sub(r'\s+', ' ', texte).strip(' .-')


def nom_telechargement(facture: dict, *, proprietaire=None, logement=None,
                       type_logement=None, db_path=None) -> str:
    """Prénom et type référentiels prioritaires, adresse structurée, replis lisibles."""
    if proprietaire is None:
        proprietaire = ref.proprietaire(facture.get('proprietaire_id'), db_path=db_path) or {}
    if logement is None:
        logement = ref.logement(facture.get('logement_id'), db_path=db_path) or {}
    if type_logement is None:
        type_logement = ref.type_label(logement.get('type_logement_id'), db_path=db_path)
    prenom = nettoyer(proprietaire.get('prenom_proprietaire'))
    if not prenom:
        affichage = _texte(proprietaire.get('nom_affichage') or proprietaire.get('nom_proprietaire'))
        if not affichage and facture.get('snapshot_json'):
            import json
            affichage = _texte(json.loads(facture['snapshot_json']).get('destinataire', {}).get('nom'))
        prenom = nettoyer(affichage.split()[0]) if affichage else 'Client'
    type_court = nettoyer(type_logement or logement.get('type_logement'))
    if type_court.casefold() == 'studio':
        type_court = 'Studio'
    elif re.fullmatch(r't\d+', type_court, flags=re.IGNORECASE):
        type_court = type_court.upper()
    if not type_court:
        type_court = nettoyer(logement.get('nom_court') or logement.get('nom_logement_officiel')) or 'Logement'
    numero_rue = re.match(r'^\s*(\d+)(?:\s*(bis|ter|quater|[a-z])\b)?',
                          _texte(logement.get('adresse')), flags=re.IGNORECASE)
    adresse_courte = ''
    if numero_rue:
        adresse_courte = numero_rue.group(1)
        if numero_rue.group(2):
            adresse_courte += ' ' + numero_rue.group(2).lower()
    bien = nettoyer(type_court + ' ' + adresse_courte)
    numero = nettoyer(facture.get('numero_facture')) or 'Brouillon'
    nature = 'Avoir' if facture.get('type_document') == 'AVOIR' else 'Facture'
    return f'{nature} - {prenom} - {bien} - {numero}.pdf'


def content_disposition(nom: str, *, inline: bool = False) -> str:
    """RFC 6266 : repli ASCII et filename* UTF-8 pour conserver les accents."""
    nom = nettoyer(nom)
    ascii_nom = unicodedata.normalize('NFKD', nom).encode('ascii', 'ignore').decode('ascii')
    disposition = 'inline' if inline else 'attachment'
    return f'{disposition}; filename="{ascii_nom}"; filename*=UTF-8\'\'{quote(nom, safe="")}'

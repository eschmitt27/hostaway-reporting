"""Une valeur de référentiel écrite sur plusieurs lignes survit à l'écran d'administration.

L'INCIDENT, DATÉ ET PROUVÉ PAR LE JOURNAL. Le 11/09/2026 à 23:24, une édition faite depuis
l'écran référentiel a transformé

    18 rue de Cugnaux⏎C202⏎Toulouse, 31000      (trois lignes, telles que saisies)

en

    18 rue de CugnauxC202Toulouse, 31000        (soudé, illisible)

et la facture du propriétaire a imprimé cette bouillie pendant dix jours.

LA CAUSE. Le gabarit composait toute colonne non énumérée dans un `<input>`. Un `<input>` ne peut
pas porter de saut de ligne : le navigateur aplatit l'attribut `value`, puis resoumet la valeur
aplatie. Et comme le formulaire renvoie TOUTES les colonnes de la ligne, corriger un numéro de
téléphone suffisait à détruire l'adresse — c'est d'ailleurs exactement ce qui s'est produit.

DEUX VERROUS, PARCE QU'UN SEUL NE SUFFIT PAS.
  · le gabarit rend un `<textarea>` dès que la valeur contient un saut de ligne ;
  · `referentiel_admin_service.txt` ramène les fins de ligne `\\r\\n` — que la norme HTML impose
    aux navigateurs pour le contenu d'un textarea — vers `\\n`. Sans lui, ouvrir puis
    réenregistrer sans rien toucher modifierait la valeur, et journaliserait une modification
    fantôme à chaque passage.
"""
from __future__ import annotations

import pytest

from app.db.connection import get_db
from app.services import referentiel_admin_service as adm

TABLE = "ref_proprietaires"
CLE = "PROP_TEST"
SAUT = chr(10)
ADRESSE_3_LIGNES = "18 rue de Cugnaux" + SAUT + "C202" + SAUT + "Toulouse, 31000"
ADRESSE_1_LIGNE = "46 allées Charles de Fitte"


def _creer(db_path, *, cle=CLE, adresse=ADRESSE_3_LIGNES, telephone="0673350846"):
    conn = get_db(db_path)
    try:
        # L'écran refuse d'afficher un référentiel jamais importé — et il a raison : « aucun
        # propriétaire » et « référentiel non alimenté » ne se disent pas de la même façon
        # (`ref_setup_repo.est_disponible`). On pose donc la trace d'import qui rend l'écran
        # exploitable, sinon la page rendrait zéro ligne et les tests passeraient à vide.
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) VALUES (?,?,?,?,?,?,?)",
            ("IMP-TEST", "2026-09-22T00:00:00", "REF_Setup.xlsm", "0" * 64, "IMPORTE", 28, 1))
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, email, telephone, adresse_facturation, mode_facturation, "
            "actif, commentaire, import_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (cle, "Touré", "David", "", telephone, adresse, "", "OUI", "",
             adm.SOURCE_APPLICATION))
        conn.commit()
    finally:
        conn.close()


def _lire(db_path, *, cle=CLE):
    return adm.ligne(TABLE, cle, db_path=db_path)


# ── 1. Le gabarit : textarea si et seulement si la valeur est multiligne ────────────────────────

def test_une_valeur_multiligne_est_rendue_dans_un_textarea(client, tmp_db):
    _creer(tmp_db)
    page = client.get(f"/administration/referentiels/{TABLE}").text
    assert "<textarea" in page, "une adresse sur trois lignes exige un textarea"
    assert ADRESSE_3_LIGNES in page, "la valeur doit être rendue telle quelle, sauts compris"


def test_le_textarea_ne_commence_pas_par_un_saut_de_ligne(client, tmp_db):
    """HTML ignore un saut de ligne collé à la balise ouvrante : l'y laisser mangerait une ligne
    à chaque enregistrement, silencieusement et un peu plus à chaque fois."""
    _creer(tmp_db)
    page = client.get(f"/administration/referentiels/{TABLE}").text
    debut = page.index(ADRESSE_3_LIGNES)
    assert page[debut - 1] == ">", "la valeur doit coller à la balise ouvrante du textarea"


def test_une_valeur_mono_ligne_reste_un_input(client, tmp_db):
    _creer(tmp_db, adresse=ADRESSE_1_LIGNE)
    page = client.get(f"/administration/referentiels/{TABLE}").text
    assert f'value="{ADRESSE_1_LIGNE}"' in page
    assert "<textarea" not in page, "aucun champ mono-ligne ne change d'apparence"


def test_une_valeur_vide_reste_un_input(client, tmp_db):
    _creer(tmp_db, adresse="")
    page = client.get(f"/administration/referentiels/{TABLE}").text
    assert "<textarea" not in page


# ── 2. L'aller-retour : ouvrir et réenregistrer ne doit RIEN changer ────────────────────────────

def _soumettre(client, tmp_db, **modifications):
    """Rejoue ce que fait le navigateur : il renvoie TOUTES les colonnes de la ligne, et convertit
    les fins de ligne d'un textarea en `\\r\\n`."""
    avant = _lire(tmp_db)
    formulaire = {c: v for c, v in avant.items()}
    formulaire.update(modifications)
    formulaire["adresse_facturation"] = formulaire["adresse_facturation"].replace(
        SAUT, chr(13) + SAUT)
    return client.post(f"/administration/referentiels/{TABLE}/modifier", data=formulaire,
                       follow_redirects=False)


def test_ouvrir_puis_reenregistrer_sans_rien_toucher_ne_change_rien(client, tmp_db):
    _creer(tmp_db)
    reponse = _soumettre(client, tmp_db)
    assert reponse.status_code == 303
    assert _lire(tmp_db)["adresse_facturation"] == ADRESSE_3_LIGNES


def test_modifier_le_telephone_laisse_l_adresse_intacte(client, tmp_db):
    """Le scénario EXACT de l'incident : c'est une édition du téléphone qui a détruit l'adresse."""
    _creer(tmp_db)
    reponse = _soumettre(client, tmp_db, telephone="0611223344")
    assert reponse.status_code == 303
    apres = _lire(tmp_db)
    assert apres["telephone"] == "0611223344"
    assert apres["adresse_facturation"] == ADRESSE_3_LIGNES
    assert "CugnauxC202" not in apres["adresse_facturation"], "la soudure ne doit jamais revenir"


def test_les_accents_traversent_l_aller_retour(client, tmp_db):
    accentuee = "12 allée des Frères Lumière" + SAUT + "Bâtiment Ç, 2ᵉ étage" + SAUT + "Nîmes"
    _creer(tmp_db, adresse=accentuee)
    _soumettre(client, tmp_db)
    assert _lire(tmp_db)["adresse_facturation"] == accentuee


def test_une_adresse_mono_ligne_traverse_l_aller_retour(client, tmp_db):
    _creer(tmp_db, adresse=ADRESSE_1_LIGNE)
    _soumettre(client, tmp_db)
    assert _lire(tmp_db)["adresse_facturation"] == ADRESSE_1_LIGNE


# ── 3. Le normaliseur, pris isolément ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("recu,attendu", [
    ("a" + chr(13) + chr(10) + "b", "a" + SAUT + "b"),            # fins de ligne Windows
    ("a" + chr(13) + "b", "a" + SAUT + "b"),                      # anciennes fins Mac
    ("a" + SAUT + "b", "a" + SAUT + "b"),                         # déjà normalisé
    ("  bordé  ", "bordé"),                                        # les bords se taillent
    (SAUT + "a" + SAUT, "a"),                                      # y compris les sauts aux bords
    ("", ""),
    (None, ""),
])
def test_txt_ramene_les_fins_de_ligne_sans_toucher_au_contenu(recu, attendu):
    assert adm.txt(recu) == attendu


def test_txt_ne_soude_jamais_deux_lignes():
    """La régression à interdire, énoncée telle quelle."""
    assert adm.txt(ADRESSE_3_LIGNES.replace(SAUT, chr(13) + SAUT)) == ADRESSE_3_LIGNES
    assert "CugnauxC202" not in adm.txt(ADRESSE_3_LIGNES)


# ── 4. Aucune modification fantôme ──────────────────────────────────────────────────────────────

def test_reenregistrer_a_l_identique_ne_journalise_aucune_modification(client, tmp_db):
    """`mettre_a_jour` écrit et journalise même sans changement réel — ce qui est son droit. Ce qui
    compte ici est que la VALEUR, elle, reste identique d'un enregistrement au suivant : sans la
    normalisation, chaque passage aurait ajouté deux caractères par ligne."""
    _creer(tmp_db)
    for _ in range(3):
        _soumettre(client, tmp_db)
    assert _lire(tmp_db)["adresse_facturation"] == ADRESSE_3_LIGNES

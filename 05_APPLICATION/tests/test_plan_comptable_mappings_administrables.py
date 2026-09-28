"""Mission 31 — plan comptable et mappings administrables.

Données FICTIVES (base temporaire) : comptes et catégories de test, aucune identité réelle. Ce qui est
vérifié : le plan comptable se gère depuis l'application sans jamais qu'un compte soit inventé,
généré ou supprimé ; une règle de mapping ne désigne qu'un compte réel, actif et de charge, pour une
catégorie réelle ; le serveur refait toutes les validations ; la prévisualisation n'écrit rien ; Flux
financiers ne propose qu'une règle VALIDÉE et « Compte comptable à définir » reste la règle sinon.
"""
from __future__ import annotations

import html
import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_mappings_service as maps
from app.services import comptabilite_plan_service as plan

ACTEUR = "Testeur"
CAT = "CHG_T31"          # catégorie de test
CAT_2 = "CHG_T31B"


@pytest.fixture
def base(tmp_db):
    from tests.fixtures_referentiel import semer_comptabilite
    semer_comptabilite(tmp_db, categories=[CAT, CAT_2])
    return tmp_db


def _compter(db, table, where="1=1"):
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]
    finally:
        conn.close()


def _instantane(db) -> dict:
    """Tout ce qu'une prévisualisation ou une opération de paramétrage NE doit PAS toucher."""
    conn = get_db(db)
    try:
        return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall()
                for t in ("charges", "ecritures", "ecriture_lignes", "banque_rapprochements",
                          "flux_lettrages", "qonto_transactions_raw", "plan_comptable",
                          "mapping_comptable_regles")}
    finally:
        conn.close()


def _compte_test(db, numero="615100", libelle="Entretien (test)", type_="CHARGE"):
    return plan.creer(numero, libelle, type_, acteur=ACTEUR, db_path=db)


def _charge(db, montant, *, categorie=CAT, date="2026-09-10", impact="IC"):
    from app.services import charges_saisie_service as saisie
    res = saisie.creer({"date_charge": date, "montant": montant, "categorie_charge_id": categorie,
                        "code_impact": impact, "prise_en_compta": "OUI" if impact == "IC" else "NON",
                        "mode_paiement_id": "PAY_001", "affectation_type": "GLOBAL",
                        "refacturable": "NON", "statut_controle": "VALIDE",
                        "commentaire": f"charge test {montant}"}, acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["charge_id"]


# ══ Plan comptable ════════════════════════════════════════════════════════════════════════════

def test_01_affichage_plan_comptable(client, base):
    page = client.get("/comptabilite/plan-comptable")
    assert page.status_code == 200
    assert 'data-testid="plan-table"' in page.text and "606000" in page.text
    assert "Ajouter un compte" in page.text and "Charge" in page.text
    assert "ACTIF</td>" not in page.text, "types affichés en libellés, pas en codes"
    filtre = client.get("/comptabilite/plan-comptable?type_compte=CHARGE")
    assert filtre.text.count('data-testid="plan-ligne"') == 1
    recherche = client.get("/comptabilite/plan-comptable?q=Banque")
    assert "512000" in recherche.text and "401000" not in recherche.text


def test_02_ajout_manuel_d_un_compte(client, base):
    r = client.post("/comptabilite/plan-comptable", data={
        "compte": "615100", "libelle": "Entretien et réparations", "type_compte": "CHARGE",
        "acteur": ACTEUR}, follow_redirects=False)
    assert r.status_code == 303 and "/comptabilite/plan-comptable/615100" in r.headers["location"]
    c = plan.charger("615100", db_path=base)
    assert c["actif"] == 1 and c["type_compte"] == "CHARGE" and c["date_creation"]
    h = plan.historique("615100", db_path=base)
    assert h[0]["type_evenement"] == "CREATION" and h[0]["acteur"] == ACTEUR


@pytest.mark.parametrize("donnees, attendu", [
    ({"compte": "606000", "libelle": "Doublon", "type_compte": "CHARGE"}, "existe déjà"),
    ({"compte": "615200", "libelle": "", "type_compte": "CHARGE"}, "libellé"),
    ({"compte": "", "libelle": "Sans numéro", "type_compte": "CHARGE"}, "numéro"),
    ({"compte": "61A000", "libelle": "Lettres", "type_compte": "CHARGE"}, "chiffres"),
    ({"compte": "401500", "libelle": "Pas une charge", "type_compte": "CHARGE"}, "commence par 6"),
    ({"compte": "615300", "libelle": "Classe 6 en actif", "type_compte": "ACTIF"}, "compte de charge"),
    ({"compte": "615400", "libelle": "Type inconnu", "type_compte": "AUTRE"}, "type"),
])
def test_03_04_ajout_refuse_cote_serveur(client, base, donnees, attendu):
    avant = _compter(base, "plan_comptable")
    r = client.post("/comptabilite/plan-comptable", data=dict(donnees, acteur=ACTEUR))
    assert r.status_code == 200 and 'data-testid="plan-erreur"' in r.text
    assert attendu.lower() in r.text.lower()
    assert _compter(base, "plan_comptable") == avant


def test_ajout_sans_nom_refuse(client, base):
    r = client.post("/comptabilite/plan-comptable", data={
        "compte": "615100", "libelle": "X", "type_compte": "CHARGE", "acteur": ""})
    assert "Indiquez votre nom" in r.text and plan.charger("615100", db_path=base) is None


def test_05_06_desactivation_reactivation_historique(client, base):
    _compte_test(base)
    r = client.post("/comptabilite/plan-comptable/615100/desactiver",
                    data={"motif": "plus utilisé", "acteur": ACTEUR})
    assert r.status_code == 200 and 'data-testid="compte-message"' in r.text
    assert plan.charger("615100", db_path=base)["actif"] == 0
    assert "615100" not in {c["compte"] for c in plan.comptes_de_charge_actifs(db_path=base)}
    plan.reactiver("615100", acteur=ACTEUR, motif="de nouveau utilisé", db_path=base)
    types = [h["type_evenement"] for h in plan.historique("615100", db_path=base)]
    assert types == ["REACTIVATION", "DESACTIVATION", "CREATION"]
    fiche = client.get("/comptabilite/plan-comptable/615100").text
    assert "plus utilisé" in fiche and "Désactivation" in fiche


def test_desactivation_sans_motif_ou_compte_structurel_refusee(base):
    _compte_test(base)
    with pytest.raises(plan.CompteRefuse) as exc:
        plan.desactiver("615100", acteur=ACTEUR, motif="", db_path=base)
    assert exc.value.code == plan.E_MOTIF
    for structurel in ("512000", "401000", "606000"):
        with pytest.raises(plan.CompteRefuse) as exc:
            plan.desactiver(structurel, acteur=ACTEUR, motif="test", db_path=base)
        assert exc.value.code == plan.E_STRUCTUREL
        assert plan.charger(structurel, db_path=base)["actif"] == 1


def test_modification_limitee_au_libelle_et_commentaire(base):
    _compte_test(base)
    c = plan.modifier("615100", libelle="Entretien courant", commentaire="note", acteur=ACTEUR,
                      db_path=base)
    assert c["libelle"] == "Entretien courant" and c["type_compte"] == "CHARGE"
    assert plan.historique("615100", db_path=base)[0]["type_evenement"] == "MODIFICATION"


def test_07_aucune_suppression_destructive(client, base):
    _compte_test(base)
    maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, acteur=ACTEUR, db_path=base)
    conn = get_db(base)
    try:
        for sql in ("DELETE FROM plan_comptable WHERE compte='615100'",
                    "DELETE FROM mapping_comptable_regles",
                    "UPDATE plan_comptable SET compte='615999' WHERE compte='615100'"):
            with pytest.raises(sqlite3.DatabaseError):
                conn.execute(sql)
            conn.rollback()
    finally:
        conn.close()
    assert plan.charger("615100", db_path=base) is not None
    from app.main import app
    chemins = {getattr(r, "path", "") for r in app.routes}
    assert not any("supprimer" in p and ("plan-comptable" in p or "mappings" in p) for p in chemins)
    assert client.post("/comptabilite/plan-comptable/615100/supprimer",
                       data={"acteur": ACTEUR}).status_code in (404, 405)


# ══ Règles de mapping : validations côté serveur ══════════════════════════════════════════════

def test_08_10_regle_categorie_valide_compte_actif_accepte(base):
    _compte_test(base)
    res = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                           acteur=ACTEUR, db_path=base)
    assert res["ok"]
    h = maps.historique_regle(res["regle_id_opaque"], db_path=base)
    assert h[0]["type_evenement"] == "CREATION"


@pytest.mark.parametrize("cle, compte, statut, debut, fin, code", [
    ("CHG_INCONNUE", "615100", "VALIDE", "", "", maps.E_CLE_INCONNUE),
    (CAT, "699999", "VALIDE", "", "", maps.E_COMPTE_INEXISTANT),
    (CAT, "467000", "VALIDE", "", "", maps.E_COMPTE_INACTIF),
    (CAT, "401000", "VALIDE", "", "", maps.E_COMPTE_INCOMPATIBLE),
    (CAT, "706000", "VALIDE", "", "", maps.E_COMPTE_INCOMPATIBLE),
    (CAT, "615100", "VALIDEE", "", "", maps.E_STATUT),
    (CAT, "615100", "VALIDE", "2026-12-01", "2026-01-01", maps.E_PERIODE),
    (CAT, "615100", "VALIDE", "01/09/2026", "", maps.E_PERIODE),
    ("", "615100", "VALIDE", "", "", maps.E_CLE_MANQUANTE),
    (CAT, "", "VALIDE", "", "", maps.E_COMPTE_MANQUANT),
])
def test_09_11_12_13_regle_refusee_par_le_service(base, cle, compte, statut, debut, fin, code):
    _compte_test(base)
    avant = _compter(base, "mapping_comptable_regles")
    res = maps.creer_regle(maps.PORTEE_CATEGORIE, compte, cle=cle, statut=statut,
                           date_debut_validite=debut, date_fin_validite=fin, acteur=ACTEUR,
                           db_path=base)
    assert res["ok"] is False and res["code"] == code
    assert _compter(base, "mapping_comptable_regles") == avant


@pytest.mark.parametrize("donnees, message", [
    ({"cle": CAT, "compte": "699999", "statut": "VALIDE"}, "Ce compte comptable n'existe pas"),
    ({"cle": CAT, "compte": "467000", "statut": "VALIDE"}, "Ce compte est désactivé"),
    ({"cle": CAT, "compte": "401000", "statut": "VALIDE"}, "compte de charge"),
    ({"cle": "CHG_INCONNUE", "compte": "615100", "statut": "VALIDE"}, "n'existe pas dans le référentiel"),
    ({"cle": CAT, "compte": "615100", "statut": "VALIDE", "portee": "PROVISOIRE_GENERIQUE"}, None),
])
def test_autorite_serveur_post_http_invalide(client, base, donnees, message):
    """Requêtes forgées, indépendamment du formulaire : le serveur refuse et n'écrit rien."""
    _compte_test(base)
    avant = _compter(base, "mapping_comptable_regles")
    r = client.post("/comptabilite/mappings", data=dict(donnees, acteur=ACTEUR))
    if message:
        assert 'data-testid="mapping-erreur"' in r.text and message in html.unescape(r.text)
        assert _compter(base, "mapping_comptable_regles") == avant
    else:
        # La portée est fixée par le serveur (catégorie) : un champ forgé ne crée pas de filet.
        assert _compter(base, "mapping_comptable_regles", "portee='PROVISOIRE_GENERIQUE'") == 1


def test_autorite_serveur_actions_forgees(client, base):
    _compte_test(base)
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    # Changer le compte d'une règle validée : refusé (la temporalité passe par une nouvelle règle).
    _compte_test(base, "615200", "Autre (test)")
    r = client.post(f"/comptabilite/mappings/{rid}/modifier",
                    data={"compte": "615200", "acteur": ACTEUR})
    assert "ne change ni de compte" in r.text
    assert maps.charger_regle(rid, db_path=base)["compte"] == "615100"
    # Le filet générique n'est pas administrable, même par une requête forgée.
    r = client.post("/comptabilite/mappings/MAP-GENERIQUE-606000/desactiver",
                    data={"motif": "x", "acteur": ACTEUR})
    assert "filet générique" in r.text.lower()
    assert maps.charger_regle("MAP-GENERIQUE-606000", db_path=base)["actif"] == 1
    # Sans nom : refusé.
    r = client.post(f"/comptabilite/mappings/{rid}/desactiver", data={"motif": "x", "acteur": ""})
    assert "Indiquez votre nom" in r.text and maps.charger_regle(rid, db_path=base)["actif"] == 1


def test_14_15_listes_controlees_sans_saisie_libre(client, base):
    _compte_test(base)
    _compte_test(base, "615200", "Désactivé (test)")
    plan.desactiver("615200", acteur=ACTEUR, motif="test", db_path=base)
    assert [c["compte"] for c in plan.comptes_de_charge_actifs(db_path=base)] == ["606000", "615100"]
    page = client.get("/comptabilite/mappings").text
    formulaire = page.split('data-testid="mapping-formulaire"')[1].split("</form>")[0]
    assert '<select name="compte"' in formulaire and '<select name="cle"' in formulaire
    assert 'name="compte" ' not in formulaire.replace('<select name="compte"', "")
    assert "<input" not in formulaire.split('name="compte"')[0].split("<label")[-1]
    assert 'value="615100"' in formulaire and 'value="615200"' not in formulaire
    assert 'value="401000"' not in formulaire and 'value="512000"' not in formulaire
    assert f"Catégorie {CAT}" in formulaire, "catégories par libellé"


def test_16_17_regle_provisoire_puis_validee(base):
    _compte_test(base)
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_PROVISOIRE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    vue = next(v for v in maps.vue_par_categorie(db_path=base) if v["categorie"] == CAT)
    assert vue["compte"] is None and vue["regle_provisoire"]["compte"] == "615100"
    assert maps.valider_regle(rid, acteur=ACTEUR, db_path=base)["ok"]
    vue = next(v for v in maps.vue_par_categorie(db_path=base) if v["categorie"] == CAT)
    assert vue["compte"]["compte"] == "615100"
    types = [h["type_evenement"] for h in maps.historique_regle(rid, db_path=base)]
    assert types == ["VALIDATION", "CREATION"]
    assert maps.rendre_provisoire(rid, acteur=ACTEUR, motif="", db_path=base)["code"] == maps.E_MOTIF
    assert maps.rendre_provisoire(rid, acteur=ACTEUR, motif="revoir", db_path=base)["ok"]


def test_18_periode_de_validite_et_succession(base):
    _compte_test(base)
    _compte_test(base, "615200", "Nouveau compte (test)")
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                           date_debut_validite="2026-01-01", acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    # Changement de compte au 1er octobre : on CLÔT l'ancienne, on crée la suivante.
    assert maps.modifier_regle(rid, date_fin_validite="2026-09-30", acteur=ACTEUR,
                               db_path=base)["ok"]
    assert maps.creer_regle(maps.PORTEE_CATEGORIE, "615200", cle=CAT, statut=maps.ST_VALIDE,
                            date_debut_validite="2026-10-01", acteur=ACTEUR, db_path=base)["ok"]
    aout = maps.resoudre_compte(categorie_charge_id=CAT, date_reference="2026-08-15", db_path=base)
    octobre = maps.resoudre_compte(categorie_charge_id=CAT, date_reference="2026-10-15", db_path=base)
    assert aout["compte"] == "615100" and octobre["compte"] == "615200", \
        "une règle posée en octobre ne réécrit pas le traitement d'août"


def test_19_chevauchement_refuse(base):
    _compte_test(base)
    _compte_test(base, "615200", "Autre (test)")
    assert maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                            date_debut_validite="2026-01-01", date_fin_validite="2026-12-31",
                            acteur=ACTEUR, db_path=base)["ok"]
    res = maps.creer_regle(maps.PORTEE_CATEGORIE, "615200", cle=CAT, statut=maps.ST_VALIDE,
                           date_debut_validite="2026-06-01", acteur=ACTEUR, db_path=base)
    assert res["ok"] is False and res["code"] == maps.E_CHEVAUCHEMENT
    assert "chevauche" in res["message"]
    assert res["detail"] == "règle existante : compte 615100, du 01/01/2026 au 31/12/2026"
    # Une règle provisoire sous une validée n'est pas ambiguë (la validée prime) : acceptée.
    assert maps.creer_regle(maps.PORTEE_CATEGORIE, "615200", cle=CAT, statut=maps.ST_PROVISOIRE,
                            date_debut_validite="2026-06-01", acteur=ACTEUR, db_path=base)["ok"]
    # Une autre catégorie sur la même période : aucun conflit.
    assert maps.creer_regle(maps.PORTEE_CATEGORIE, "615200", cle=CAT_2, statut=maps.ST_VALIDE,
                            acteur=ACTEUR, db_path=base)["ok"]


def test_20_categorie_sans_regle_compte_a_definir(client, base):
    page = client.get("/comptabilite/mappings").text
    assert "Compte comptable à définir" in page
    vue = {v["categorie"]: v for v in maps.vue_par_categorie(db_path=base)}
    assert vue[CAT]["compte"] is None and vue[CAT_2]["compte"] is None
    assert 'data-testid="mapping-synthese"' in page and "2 catégories sur 2" in page


# ══ Intégration Flux financiers ═══════════════════════════════════════════════════════════════

@pytest.fixture
def flux(base, monkeypatch):
    for nom in ("MODE_REEL_ECRITURES", "BANQUE_REAL_WRITE_ENABLED",
                "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", "COMPTABILITE_REAL_WRITE_ENABLED",
                "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, nom, True, raising=False)
    monkeypatch.setattr(cfg, "RECETTE_MODE", False, raising=False)
    from tests.test_flux_financiers import _importer, _mvt
    _importer(base, [_mvt(55.0, date="2026-09-12")])
    from app.services import flux_financiers_service as fx
    m = next(x for x in fx.mouvements(source=fx.BANQUE, db_path=base) if x["montant"] == 55.0)
    return base, m


def _compte_propose(db, mouvement, charge_id):
    from app.services import flux_lettrage_service as lettrage
    prep = lettrage.preparer([f"BANQUE:{mouvement['id']}"], [f"CHARGE:{charge_id}"], db_path=db)
    return next(l for l in prep["ecritures"][0]["lignes"] if l["role"] == lettrage.ROLE_OBJET)["compte"]


def test_21_regle_validee_proposee_dans_flux_provisoire_non(flux):
    base, m = flux
    _compte_test(base)
    cid = _charge(base, 55.0)
    assert _compte_propose(base, m, cid) == "", "sans règle : à définir"
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_PROVISOIRE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    assert _compte_propose(base, m, cid) == "", "une règle provisoire ne propose rien"
    maps.valider_regle(rid, acteur=ACTEUR, db_path=base)
    assert _compte_propose(base, m, cid) == "615100"


def test_22_compte_desactive_plus_propose(flux):
    base, m = flux
    _compte_test(base)
    cid = _charge(base, 55.0)
    maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE, acteur=ACTEUR,
                     db_path=base)
    plan.desactiver("615100", acteur=ACTEUR, motif="test", db_path=base)
    assert _compte_propose(base, m, cid) == ""
    vue = next(v for v in maps.vue_par_categorie(db_path=base) if v["categorie"] == CAT)
    assert vue["compte"] is None and vue["regle_bloquee"]["compte"] == "615100"
    assert vue["blocage"] == "compte désactivé"
    from fastapi.testclient import TestClient
    from app.main import app
    page = TestClient(app).get("/comptabilite/mappings").text
    assert "Validée, bloquée" in page and "non proposée : compte désactivé" in page


def test_regle_desactivee_plus_proposee(flux):
    base, m = flux
    _compte_test(base)
    cid = _charge(base, 55.0)
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    maps.desactiver_regle(rid, acteur=ACTEUR, motif="test", db_path=base)
    assert _compte_propose(base, m, cid) == ""
    assert maps.charger_regle(rid, db_path=base) is not None, "désactivée, jamais supprimée"


def test_23_ecriture_historique_inchangee(flux):
    base, m = flux
    from app.services import comptabilite_ecritures_service as compta
    from app.services import flux_lettrage_service as lettrage
    _compte_test(base)
    cid = _charge(base, 55.0)
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_VALIDE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    res = lettrage.valider([f"BANQUE:{m['id']}"], [f"CHARGE:{cid}"], acteur=ACTEUR, db_path=base)
    assert res["ok"], res
    avant = compta.lignes(res["ecritures"][0], db_path=base)
    maps.rendre_provisoire(rid, acteur=ACTEUR, motif="revoir", db_path=base)
    maps.desactiver_regle(rid, acteur=ACTEUR, motif="test", db_path=base)
    plan.desactiver("615100", acteur=ACTEUR, motif="test", db_path=base)
    assert compta.lignes(res["ecritures"][0], db_path=base) == avant
    assert compta.charger(res["ecritures"][0], db_path=base)["statut"] == compta.ST_VALIDEE
    fiche = next(c for c in plan.lister(db_path=base) if c["compte"] == "615100")
    assert fiche["nb_ecritures"] == 1 and fiche["actif"] == 0, "l'historique reste consultable"


# ══ Prévisualisation d'impact ═════════════════════════════════════════════════════════════════

def test_24_25_previsualisation_ne_modifie_rien_et_compte_les_charges(client, base):
    _compte_test(base)
    _charge(base, 40.0, date="2026-09-01")
    _charge(base, 60.0, date="2026-09-20")
    _charge(base, 25.0, date="2026-10-05")
    _charge(base, 700.0, impact="HC")                        # hors comptabilité : hors du compte
    _charge(base, 13.0, categorie=CAT_2)                     # autre catégorie
    avant = _instantane(base)
    r = client.post("/comptabilite/mappings/previsualiser", data={
        "cle": CAT, "compte": "615100", "statut": "VALIDE", "date_debut_validite": "2026-09-01",
        "date_fin_validite": "2026-09-30", "acteur": ACTEUR})
    assert r.status_code == 200 and 'data-testid="apercu-impact"' in r.text
    assert "Rien n'a été enregistré" in r.text
    assert _instantane(base) == avant, "la prévisualisation n'écrit rien"
    apercu = maps.apercu_impact(maps.PORTEE_CATEGORIE, CAT, "615100", maps.ST_VALIDE,
                                "2026-09-01", "2026-09-30", db_path=base)
    assert apercu["nb_charges"] == 2 and apercu["montant"] == 100.0
    assert apercu["nb_a_traiter"] == 2 and apercu["proposera"] is True
    assert _instantane(base) == avant
    provisoire = maps.apercu_impact(maps.PORTEE_CATEGORIE, CAT, "615100", maps.ST_PROVISOIRE,
                                    db_path=base)
    assert provisoire["nb_charges"] == 3 and provisoire["proposera"] is False


def test_previsualisation_d_une_regle_invalide_ne_propose_pas_d_enregistrer(client, base):
    r = client.post("/comptabilite/mappings/previsualiser", data={
        "cle": CAT, "compte": "699999", "statut": "VALIDE", "acteur": ACTEUR})
    assert 'data-testid="apercu-refus"' in r.text and "n'existe pas" in html.unescape(r.text)
    assert 'data-testid="apercu-confirmer"' not in r.text


def test_parcours_http_creation_puis_validation(client, base):
    _compte_test(base)
    r = client.post("/comptabilite/mappings", data={
        "cle": CAT, "compte": "615100", "statut": "PROVISOIRE", "source": "arbitrage test",
        "acteur": ACTEUR}, follow_redirects=False)
    assert r.status_code == 303 and "message=" in r.headers["location"]
    rid = next(x for x in maps.lister_regles(db_path=base) if x["cle"] == CAT)["regle_id_opaque"]
    apercu = client.get(f"/comptabilite/mappings/{rid}/valider")
    assert 'data-testid="apercu-impact"' in apercu.text
    assert maps.charger_regle(rid, db_path=base)["statut"] == maps.ST_PROVISOIRE
    r = client.post(f"/comptabilite/mappings/{rid}/valider", data={"acteur": ACTEUR})
    assert maps.charger_regle(rid, db_path=base)["statut"] == maps.ST_VALIDE
    assert 'data-testid="regle-message"' in r.text
    fiche = client.get(f"/comptabilite/mappings/{rid}").text
    assert "Validation" in fiche and "Création" in fiche


# ══ Invariants : rien d'inventé, rien de réel touché ══════════════════════════════════════════

def test_26_aucun_compte_cree_automatiquement(client, base):
    avant = _compter(base, "plan_comptable")
    client.get("/comptabilite/mappings")
    client.get("/comptabilite/plan-comptable")
    maps.vue_par_categorie(db_path=base)
    maps.apercu_impact(maps.PORTEE_CATEGORIE, CAT, "606000", maps.ST_VALIDE, db_path=base)
    maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, acteur=ACTEUR, db_path=base)
    assert _compter(base, "plan_comptable") == avant


def test_27_aucun_repli_606000(base):
    # Toutes les catégories restent « à définir » malgré le filet générique existant.
    assert all(v["compte"] is None for v in maps.vue_par_categorie(db_path=base))
    # Sans AUCUNE règle, le résolveur ne rend plus de numéro codé en dur.
    conn = get_db(base)
    try:
        conn.execute("UPDATE mapping_comptable_regles SET actif=0")
        conn.commit()
    finally:
        conn.close()
    res = maps.resoudre_compte(categorie_charge_id=CAT, db_path=base)
    assert res["compte"] == "" and res["regle"] == "AUCUNE_REGLE"
    from app.services import flux_lettrage_service as lettrage
    assert lettrage.compte_de_charge({"categorie_charge_id": CAT}, db_path=base)[0] == ""


def test_journal_od_sans_compte_preselectionne_ni_compte_inactif(client, base):
    page = client.get("/comptabilite/journaux/od").text
    assert 'value="606000" selected' not in page and 'value="401000" selected' not in page
    assert 'value="467000"' not in page, "un compte désactivé ne se choisit plus"


def test_28_29_30_charge_hors_compta_qonto_et_base_reelle_intouchees(client, base):
    hc = _charge(base, 700.0, impact="HC", date="2026-08-09")
    from app.services import charges_saisie_service as saisie
    avant_charge = saisie.lire(hc, db_path=base)
    avant_qonto = _instantane(base)["qonto_transactions_raw"]
    _compte_test(base)
    rid = maps.creer_regle(maps.PORTEE_CATEGORIE, "615100", cle=CAT, statut=maps.ST_PROVISOIRE,
                           acteur=ACTEUR, db_path=base)["regle_id_opaque"]
    maps.valider_regle(rid, acteur=ACTEUR, db_path=base)
    client.post("/comptabilite/mappings/previsualiser",
                data={"cle": CAT, "compte": "615100", "statut": "VALIDE", "acteur": ACTEUR})
    assert saisie.lire(hc, db_path=base) == avant_charge
    assert _instantane(base)["qonto_transactions_raw"] == avant_qonto
    # Les tests n'ouvrent jamais la base réelle.
    reelle = Path(cfg.APP_ROOT) / "data" / "app.db"
    assert Path(cfg.DB_PATH).resolve() != reelle.resolve()
    # Aucun des modules de la mission ne parle à Qonto.
    racine = Path(cfg.APP_ROOT) / "app"
    for fichier in ("services/comptabilite_plan_service.py", "services/comptabilite_mappings_service.py"):
        assert "qonto" not in (racine / fichier).read_text(encoding="utf-8").lower()

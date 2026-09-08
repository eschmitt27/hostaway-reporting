"""Facture propriétaire BROUILLON éditable : lignes, charges, règlement, réservations.

Ce que ces tests protègent, dans l'ordre d'importance :
  1. éditer une facture ne modifie JAMAIS Lot10/Lot12 (comparaison avant/après, contenu inclus) ;
  2. le total source reste figé même quand le total facturé bouge ;
  3. créer une charge depuis un BROUILLON ne crée AUCUNE écriture comptable, quel que soit le mode ;
  4. une facture qui a quitté le BROUILLON refuse toute édition, explicitement.

Aucune donnée réelle : base temporaire créée par les migrations, identités et montants de fixture.
"""
import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import charges_saisie_service as charges
from app.services import factures_proprietaires_edition_service as edition
from app.services import factures_proprietaires_service as svc

EMETTEUR = {"nom": "Conciergerie Fixture", "adresse": "1 rue de Test", "siret": "00000000000000"}
DESTINATAIRE = {"nom": "Proprietaire Fixture", "adresse": "2 rue de Test"}


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


def source(**kw):
    base = {
        "mois": "2026-06",
        "proprietaire_id": "PROP_FIXT_1",
        "logement_id": "LOG_FIXT_1",
        "source_calcul": "PREF-2026-06-PROP_FIXT_1-LOG_FIXT_1",
        "COMMISSION_CONCIERGERIE": 300.0,
        "MENAGE_FACTURE": 150.0,
        "CHARGE_FIXE": 50.0,
        "montant_du_conciergerie": 500.0,
    }
    base.update(kw)
    return base


def _facture(db, **kw):
    return svc.creer(source(**kw), db_path=db)["facture_id_opaque"]


def _photo_lots(db):
    """Contenu intégral des tables de calcul — pas seulement leur nombre de lignes.

    Compter les lignes laisserait passer une mise à jour en place, qui est précisément le risque :
    une facture qui « corrigerait » un montant Lot10 au lieu de ne corriger qu'elle-même.
    """
    conn = get_db(db)
    try:
        photo = {}
        for table in ("lot10_commissions", "lot10_resultats", "lot10_net_exploitation",
                      "lot12_prefactures_entete", "lot12_prefactures_lignes"):
            photo[table] = conn.execute(
                f"SELECT * FROM {table} ORDER BY id").fetchall()
            photo[table] = [tuple(r) for r in photo[table]]
        return photo
    finally:
        conn.close()


def _categorie(db, categorie_id="CAT_FIXT_1"):
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO ref_categories_charges "
            "(categorie_charge_id, categorie_niveau_1, categorie_niveau_2, actif, import_id) "
            "VALUES (?,?,?,?,?)", (categorie_id, "Exploitation", "Divers", "OUI", "IMP_FIXT"))
        conn.commit()
    finally:
        conn.close()
    return categorie_id


# ── Origine des lignes ──────────────────────────────────────────────────────────────────────────

def test_origine_derivee_et_jamais_stockee_en_double(db):
    fid = _facture(db)
    svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="Ajout manuel", montant=25.0,
                      db_path=db)
    lignes = svc.lire(fid, db_path=db)["lignes"]
    calculees = [l for l in lignes if l["origine"] == svc.ORIGINE_CALCULEE]
    manuelles = [l for l in lignes if l["origine"] == svc.ORIGINE_MANUELLE]
    assert len(calculees) == 3 and len(manuelles) == 1
    # L'origine se déduit d'`objet_source_type`, elle n'est pas une colonne de plus à maintenir.
    assert all(l["objet_source_type"] for l in calculees)
    assert manuelles[0]["objet_source_type"] is None


# ── Les trois montants ──────────────────────────────────────────────────────────────────────────

def test_total_source_fige_et_total_facture_vivant(db):
    fid = _facture(db)
    avant = svc.montants(fid, db_path=db)
    assert avant == {"total_source_calcule": 500.0, "total_facture": 500.0,
                     "ajustement_manuel": 0.0, "ajustement_present": False}

    svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="Supplément", montant=40.0,
                      db_path=db)
    apres = svc.montants(fid, db_path=db)
    assert apres["total_source_calcule"] == 500.0, "le total source ne bouge jamais"
    assert apres["total_facture"] == 540.0
    assert apres["ajustement_manuel"] == 40.0
    assert apres["ajustement_present"] is True


def test_ajustement_nul_masque_la_decomposition(db):
    fid = _facture(db)
    ligne = svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="A", montant=40.0,
                              db_path=db)["ligne_id_opaque"]
    assert svc.montants(fid, db_path=db)["ajustement_present"] is True
    svc.supprimer_ligne(fid, ligne, db_path=db)
    assert svc.montants(fid, db_path=db)["ajustement_present"] is False


def test_montant_total_entete_suit_toujours_les_lignes(db):
    fid = _facture(db)
    svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="A", montant=10.0, db_path=db)
    f = svc.lire(fid, db_path=db)
    assert f["montant_total"] == f["total_facture"] == 510.0


# ── Ajout / modification / suppression ──────────────────────────────────────────────────────────

def test_modifier_ligne_calculee_conserve_le_montant_dorigine(db):
    fid = _facture(db)
    ligne = next(l for l in svc.lire(fid, db_path=db)["lignes"]
                 if l["type_ligne"] == "COMMISSION_CONCIERGERIE")
    svc.modifier_ligne(fid, ligne["ligne_id_opaque"], montant=280.0, db_path=db)
    apres = next(l for l in svc.lire(fid, db_path=db)["lignes"]
                 if l["ligne_id_opaque"] == ligne["ligne_id_opaque"])
    assert apres["montant"] == 280.0
    assert apres["montant_source_initial"] == 300.0, "l'écart au calcul reste reconstituable"
    assert apres["montant_modifie"] is True

    # Une seconde modification n'écrase pas la mémoire de la PREMIÈRE valeur calculée.
    svc.modifier_ligne(fid, ligne["ligne_id_opaque"], montant=250.0, db_path=db)
    apres2 = next(l for l in svc.lire(fid, db_path=db)["lignes"]
                  if l["ligne_id_opaque"] == ligne["ligne_id_opaque"])
    assert apres2["montant_source_initial"] == 300.0


def test_supprimer_ligne_calculee_ne_touche_pas_la_source(db):
    fid = _facture(db)
    photo = _photo_lots(db)
    ligne = next(l for l in svc.lire(fid, db_path=db)["lignes"]
                 if l["origine"] == svc.ORIGINE_CALCULEE)
    svc.supprimer_ligne(fid, ligne["ligne_id_opaque"], db_path=db)
    assert _photo_lots(db) == photo
    evt = svc.lire(fid, db_path=db)["evenements"][-1]
    assert evt["type_evenement"] == svc.EVT_SUPPRESSION_LIGNE
    assert "INCHANGEE" in evt["commentaire"]


@pytest.mark.parametrize("operation", ["ajouter", "modifier", "supprimer"])
def test_aucune_operation_de_ligne_ne_touche_lot10_lot12(db, operation):
    fid = _facture(db)
    photo = _photo_lots(db)
    ligne = svc.lire(fid, db_path=db)["lignes"][0]["ligne_id_opaque"]
    if operation == "ajouter":
        svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="X", montant=5.0, db_path=db)
    elif operation == "modifier":
        svc.modifier_ligne(fid, ligne, montant=1.0, db_path=db)
    else:
        svc.supprimer_ligne(fid, ligne, db_path=db)
    assert _photo_lots(db) == photo


def test_evenements_tracent_chaque_operation(db):
    fid = _facture(db)
    l = svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="X", montant=5.0,
                          db_path=db)["ligne_id_opaque"]
    svc.modifier_ligne(fid, l, montant=6.0, db_path=db)
    svc.supprimer_ligne(fid, l, db_path=db)
    types = [e["type_evenement"] for e in svc.lire(fid, db_path=db)["evenements"]]
    assert types == ["CREATION", svc.EVT_AJOUT_LIGNE, svc.EVT_MODIFICATION_LIGNE,
                     svc.EVT_SUPPRESSION_LIGNE]


# ── Édition interdite hors BROUILLON ────────────────────────────────────────────────────────────

def _valider(db, fid):
    svc.valider(fid, emetteur=EMETTEUR, destinataire=DESTINATAIRE, db_path=db)


def test_facture_validee_refuse_toute_edition(db):
    fid = _facture(db)
    ligne = svc.lire(fid, db_path=db)["lignes"][0]["ligne_id_opaque"]
    _valider(db, fid)
    for appel in (
        lambda: svc.ajouter_ligne(fid, type_ligne="CHARGE_FIXE", libelle="X", montant=5.0,
                                  db_path=db),
        lambda: svc.modifier_ligne(fid, ligne, montant=1.0, db_path=db),
        lambda: svc.supprimer_ligne(fid, ligne, db_path=db),
    ):
        with pytest.raises(svc.FactureProprietaireError, match="BROUILLON"):
            appel()


def test_route_refuse_en_422_html_et_reste_sur_la_facture(db, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    from app.main import app as application

    fid = _facture(db)
    _valider(db, fid)
    with TestClient(application) as client:
        r = client.post(f"/factures-proprietaires/{fid}/lignes/ajouter",
                        data={"type_ligne": "CHARGE_FIXE", "libelle": "X", "montant": "5"})
    assert r.status_code == 422
    assert "text/html" in r.headers["content-type"]
    assert "Ligne non ajout" in r.text


def test_route_ajout_redirige_vers_la_meme_facture(db, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    from app.main import app as application

    fid = _facture(db)
    with TestClient(application) as client:
        r = client.post(f"/factures-proprietaires/{fid}/lignes/ajouter",
                        data={"type_ligne": "CHARGE_FIXE", "libelle": "X", "montant": "5"},
                        follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == f"/factures-proprietaires/{fid}#lignes-facturees"


# ── B — Charges : trois modes, service canonique, zéro écriture ─────────────────────────────────

@pytest.mark.parametrize("code,reel,compta", [("IC", "OUI", "OUI"), ("HC", "OUI", "NON"),
                                              ("HR", "NON", "NON")])
def test_trois_modes_produisent_les_flags_documentes(db, code, reel, compta):
    fid = _facture(db)
    cat = _categorie(db)
    resultat = edition.ajouter_ligne_charge(fid, libelle=f"Charge {code}", montant=30.0,
                                            code_impact=code, categorie_charge_id=cat,
                                            db_path=db)
    conn = get_db(db)
    try:
        charge = dict(conn.execute("SELECT * FROM charges WHERE charge_id=?",
                                   (resultat["charge_id"],)).fetchone())
    finally:
        conn.close()
    assert charge["code_impact"] == code
    assert charge["impact_resultat_reel"] == reel
    assert charge["impact_resultat_comptable"] == compta
    # Marqueur posé par `charges_saisie_service.creer()` : preuve que la charge est passée par le
    # service canonique et non par un INSERT direct depuis la facture.
    assert charge["source_module"] == "SAISIE_APP"


@pytest.mark.parametrize("code", ["IC", "HC", "HR"])
def test_aucune_ecriture_comptable_sur_un_brouillon(db, code):
    fid = _facture(db)
    cat = _categorie(db)
    conn = get_db(db)
    try:
        avant = conn.execute("SELECT COUNT(*) FROM ecritures").fetchone()[0]
    finally:
        conn.close()
    edition.ajouter_ligne_charge(fid, libelle="Charge", montant=30.0, code_impact=code,
                                 categorie_charge_id=cat, db_path=db)
    conn = get_db(db)
    try:
        apres = conn.execute("SELECT COUNT(*) FROM ecritures").fetchone()[0]
    finally:
        conn.close()
    assert apres == avant == 0, "la comptabilisation est un workflow postérieur, jamais ici"


def test_charge_refusee_n_ecrit_rien_sur_la_facture(db):
    fid = _facture(db)
    avant = svc.lire(fid, db_path=db)
    with pytest.raises(svc.FactureProprietaireError, match="charge refusee"):
        # Catégorie absente du référentiel mais surtout montant nul : refus métier de `valider()`.
        edition.ajouter_ligne_charge(fid, libelle="Charge", montant=0, code_impact="IC",
                                     categorie_charge_id=_categorie(db), db_path=db)
    assert svc.lire(fid, db_path=db)["lignes"] == avant["lignes"]


def test_mode_inconnu_refuse_sans_rien_creer(db):
    fid = _facture(db)
    with pytest.raises(svc.FactureProprietaireError, match="mode d'impact inconnu"):
        edition.ajouter_ligne_charge(fid, libelle="X", montant=10.0, code_impact="XX",
                                     categorie_charge_id=_categorie(db), db_path=db)
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0] == 0
    finally:
        conn.close()


def test_anti_double_refacturation_une_seule_position_par_charge(db):
    """La file de refacturation est alimentée par le SEUL point d'entrée canonique.

    `charges_saisie_service.creer()` appelle `synchroniser_depuis_charge`, et la table impose
    `charge_id UNIQUE` : une seconde alimentation depuis la facture créerait un doublon impossible.
    """
    fid = _facture(db)
    cat = _categorie(db)
    resultat = edition.ajouter_ligne_charge(fid, libelle="Refacturable", montant=30.0,
                                            code_impact="IC", categorie_charge_id=cat,
                                            refacturable="OUI", db_path=db)
    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM charges_refacturation_positions WHERE charge_id=?",
                         (resultat["charge_id"],)).fetchone()[0]
    finally:
        conn.close()
    assert n == 1


def test_supprimer_ligne_charge_annule_la_charge_non_consommee(db):
    fid = _facture(db)
    cat = _categorie(db)
    ajout = edition.ajouter_ligne_charge(fid, libelle="X", montant=30.0, code_impact="HR",
                                         categorie_charge_id=cat, db_path=db)
    suppression = edition.supprimer_ligne_charge(fid, ajout["ligne_id_opaque"], db_path=db)
    assert suppression["charge_annulee"] is True
    conn = get_db(db)
    try:
        statut = conn.execute("SELECT statut FROM charges WHERE charge_id=?",
                              (ajout["charge_id"],)).fetchone()[0]
    finally:
        conn.close()
    # ANNULÉE, jamais supprimée physiquement : la charge peut déjà être référencée ailleurs.
    assert statut == charges.STATUT_ANNULEE


# ── C — Reversement Airbnb ──────────────────────────────────────────────────────────────────────

def _compte(db, table):
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def test_reversement_airbnb_ne_cree_aucun_autre_objet(db):
    fid = _facture(db)
    avant = {t: _compte(db, t) for t in ("reservations_resolues", "charges", "banque_mouvements",
                                         "ecritures")}
    resultat = edition.ajouter_reversement_airbnb(fid, montant=120.0,
                                                  date_imputation="2026-06-15",
                                                  reference_airbnb="AIRBNB-XYZ", db_path=db)
    assert resultat["ok"] and resultat["imputation_airbnb_id"].startswith("IMPA-")
    assert {t: _compte(db, t) for t in avant} == avant
    assert _compte(db, "imputations_airbnb") == 1
    reversements = svc.reversements_airbnb(fid, db_path=db)
    assert reversements[0]["montant_impute"] == 120.0
    assert reversements[0]["document_id"] == fid


# ── D — Acompte propriétaire ────────────────────────────────────────────────────────────────────

@pytest.fixture()
def proprietaire_connu(monkeypatch):
    """`proprietaires_tresorerie_service` refuse un propriétaire absent du référentiel."""
    from app.readers import proprietaires_reader
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid else None)


def test_acompte_valide_et_ne_touche_ni_lot9_ni_lot10_ni_lot12(db, proprietaire_connu):
    fid = _facture(db)
    photo = _photo_lots(db)
    resultat = edition.ajouter_acompte(fid, montant=200.0, date_mouvement="2026-06-10",
                                       mode_reglement="VIREMENT", db_path=db)
    assert resultat["statut"] == "VALIDE", "sans validation, l'acompte ne déduirait rien"
    assert _photo_lots(db) == photo
    acomptes = svc.acomptes_proprietaire(fid, db_path=db)
    assert len(acomptes) == 1
    assert acomptes[0]["nature"] == "ACOMPTE_PROPRIETAIRE"
    assert acomptes[0]["sens"] == "PROPRIETAIRE_VERS_SOCIETE"
    assert acomptes[0]["reference_metier"] == fid


# ── E — Solde de règlement ──────────────────────────────────────────────────────────────────────

def test_solde_deduit_reversements_et_acomptes(db, proprietaire_connu):
    fid = _facture(db)
    edition.ajouter_reversement_airbnb(fid, montant=120.0, date_imputation="2026-06-15",
                                       db_path=db)
    edition.ajouter_acompte(fid, montant=200.0, date_mouvement="2026-06-10", db_path=db)
    solde = svc.solde(fid, db_path=db)
    assert solde["montant_total"] == 500.0
    assert solde["total_reversements_airbnb"] == 120.0
    assert solde["total_acomptes_proprietaire"] == 200.0
    assert solde["solde"] == 180.0
    assert solde["statut_reglement"] == "PARTIELLEMENT_REGLEE"


def test_solde_regle_quand_tout_est_couvert(db, proprietaire_connu):
    fid = _facture(db)
    edition.ajouter_acompte(fid, montant=500.0, date_mouvement="2026-06-10", db_path=db)
    assert svc.solde(fid, db_path=db)["statut_reglement"] == "REGLEE"


# ── F — Réservations de la période ──────────────────────────────────────────────────────────────

def _seed_lot10(db, run_id="L10-FIXT", actif=1, payout=800.0):
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO lot10_runs (run_id, date_calcul, statut, actif) VALUES (?,?,?,?)",
            (run_id, "2026-07-01T00:00:00Z", "SUCCES", actif))
        conn.execute(
            "INSERT INTO lot10_commissions (run_id, reservation_calc_id, reservation_id_hostaway, "
            " logement_id, proprietaire_id, mois, date_arrivee, date_depart, nuits, guest_count, "
            " channel_type, payout_calcule) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, f"CALC-{run_id}-1", "RES-1", "LOG_FIXT_1", "PROP_FIXT_1", "2026-06",
             "2026-06-03", "2026-06-07", 4, 3, "airbnbOfficial", payout))
        conn.execute(
            "INSERT OR IGNORE INTO hostaway_reservations (extraction_id, reservation_id, "
            "payload_json) VALUES (?,?,?)",
            ("EXT-FIXT", "RES-1", '{"id": 1, "guestName": "Voyageur Fixture", "x": 2}'))
        conn.commit()
    finally:
        conn.close()


def test_reservations_figees_a_la_creation(db):
    _seed_lot10(db)
    fid = _facture(db)
    lignes = svc.reservations(fid, db_path=db)
    assert len(lignes) == 1
    r = lignes[0]
    assert r["guest_name"] == "Voyageur Fixture"
    assert (r["check_in"], r["check_out"], r["nights"], r["guest_count"]) == (
        "2026-06-03", "2026-06-07", 4, 3)
    assert r["payout"] == 800.0
    assert r["plateforme"] == "airbnbOfficial"
    assert r["source_run_id"] == "L10-FIXT"


def test_reservations_stables_apres_changement_hostaway(db):
    """Preuve du choix d'architecture : la jointure vivante n'aurait pas tenu.

    On simule exactement ce qui arrive en production — un nouveau run Lot10 remplace le run actif
    avec d'autres montants. L'instantané de la facture, lui, ne bouge pas.
    """
    _seed_lot10(db)
    fid = _facture(db)
    avant = svc.reservations(fid, db_path=db)

    conn = get_db(db)
    try:
        conn.execute("UPDATE lot10_runs SET actif=0")
        conn.execute("UPDATE lot10_commissions SET payout_calcule=9999")
        conn.commit()
    finally:
        conn.close()
    _seed_lot10(db, run_id="L10-FIXT-2", payout=1234.0)

    assert svc.reservations(fid, db_path=db) == avant


def test_reservations_sans_effet_sur_le_total(db):
    _seed_lot10(db)
    fid = _facture(db)
    f = svc.lire(fid, db_path=db)
    assert f["montant_total"] == 500.0
    assert f["total_facture"] == 500.0
    assert svc.reservations(fid, db_path=db), "les réservations existent bien, sans peser au total"

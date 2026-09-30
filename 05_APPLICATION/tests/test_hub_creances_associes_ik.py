"""Créances & Dettes comme point d'entrée financier, Associés, IK et compte courant.

Parcours propriétaire : créance → Compte propriétaire → Préparer le règlement → Régler, le
règlement passant par le rapprochement canonique de Flux (aucune donnée parallèle).
Associés : IK (période libre, trajets, prorata, part engagée pour l'activité), autres avantages,
apports et remboursements de compte courant, position nette.

Données FICTIVES uniquement (base temporaire).
"""
from __future__ import annotations

import sqlite3
from datetime import date
from urllib.parse import quote

import pytest

from app.db.connection import get_db
from app.services import associes_service as ass
from app.services import charges_saisie_service as saisie
from app.services import clotures_service as cs
from app.services import compte_proprietaire_service as cpt
from app.services import comptabilite_ecritures_service as compta
from app.services import creances_dettes_service as creances
from app.services import creances_reglement_service as reglement
from app.services import flux_financiers_service as flux
from app.services import qonto_validation_service as validation
from tests.fixtures_referentiel import IMPORT_TEST
from tests.test_circuit_banque_charges_compta import (_charge_depuis, _facture, _lignes,  # noqa: F401
                                                      _referentiel_saisie, factures_ok)  # noqa: F401
from tests.test_flux_financiers import (ACTEUR, PROPRIO, _importer, _mvt, _par_montant,  # noqa: F401
                                        base, verrous)
from tests.test_lecture_seule_flux import _ecarts, _empreintes

A1, A2 = "PERS_T1", "PERS_T2"


@pytest.fixture
def associes(base):
    conn = get_db(base)
    try:
        for pid, nom in ((A1, "Alice"), (A2, "Bruno")):
            conn.execute("INSERT OR IGNORE INTO ref_associes (personne_id, nom_personne, "
                         "type_personne, actif, import_id) VALUES (?,?,'ASSOCIE','OUI',?)",
                         (pid, nom, IMPORT_TEST))
        conn.commit()
    finally:
        conn.close()
    return base


def _charge(db, montant, *, categorie="CHG_009", date_charge="2026-09-05", **extra):
    donnees = {"date_charge": date_charge, "montant": montant, "categorie_charge_id": categorie,
               "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
               "affectation_type": "GLOBAL", "refacturable": "NON", "commentaire": "Test",
               "statut_controle": "A_CONTROLER", **extra}
    res = saisie.creer(donnees, acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["charge_id"]


def _ik(db, montant, debut="2026-09-01", fin="2026-09-30", associe=A1):
    cid = _charge(db, montant)
    res = ass.creer_ik(cid, associe, debut, fin, acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["ik_id_opaque"], cid


def _facture_vendue(db, lignes):
    """Facture émise ET vente comptabilisée : sans elle, un encaissement n'a pas de créance à
    éteindre (règle existante L04, Mission 36)."""
    fid, emise = _facture(db, lignes)
    assert compta.comptabiliser_facture_emise(emise, acteur=ACTEUR, db_path=db)["vente"]["ok"]
    return fid


def _compter(db, table):
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def _ligne(suivi, mois):
    return next(l for l in suivi["lignes"] if l["mois"] == mois)


# ══ Navigation (cas A et C) ═════════════════════════════════════════════════════════════════════

def test_01_menu_creances_est_le_hub(client):
    page = client.get("/creances").text
    assert 'href="/creances" class="nav-item' in page
    # Plus d'entrée principale « Compte propriétaire » ni « Règlements propriétaires »…
    assert 'href="/comptes-proprietaires" class="nav-item' not in page
    assert 'href="/proprietaires-reglements" class="nav-item' not in page
    # … mais les routes restent servies (liens internes, tests, favoris).
    for url in ("/comptes-proprietaires", "/proprietaires-reglements", "/dettes", "/echeancier",
                "/associes"):
        assert client.get(url).status_code == 200, url
    # Les quatre vues du hub.
    for onglet in ("Créances propriétaires", "Dettes fournisseurs", "Échéancier", "Associés"):
        assert onglet in page


def test_02_creance_ouvre_le_compte_proprietaire(base, verrous, factures_ok, client):
    _facture(base, {"gestion": 150.0, "menage": 50.0})
    p = next(x for x in reglement.positions(db_path=base) if x["proprietaire_id"] == PROPRIO)
    assert (p["facture"], p["regle"], p["restant_du"]) == (200.0, 0.0, 200.0)
    assert p["statut"] == "À régler"
    page = client.get("/creances").text
    assert f'href="/comptes-proprietaires/{PROPRIO}"' in page
    assert f'href="/creances/proprietaires/{PROPRIO}/reglement"' in page
    compte = client.get(f"/comptes-proprietaires/{PROPRIO}")
    assert compte.status_code == 200
    assert "Préparer le règlement" in compte.text and 'href="/creances"' in compte.text


# ══ Préparer le règlement → Régler (cas B) ══════════════════════════════════════════════════════

def test_03_preparer_puis_regler_met_tout_a_jour(base, verrous, factures_ok, client):
    fid = _facture_vendue(base, {"gestion": 150.0, "menage": 50.0})
    _importer(base, [_mvt(200.0, sens="credit", contrepartie="Claire Testeur", date="2026-09-15",
                          libelle="VIR LOYER")])
    m = _par_montant(base, 200.0)
    cle = f"BANQUE:{m['id']}"

    prep = reglement.preparer(PROPRIO, db_path=base)
    assert prep["montant_propose"] == 200.0 and [f["facture_id_opaque"] for f in
                                                prep["factures_ouvertes"]] == [fid]
    assert prep["mouvements"][0]["cle"] == cle and prep["mouvements"][0]["porte_nom"]

    avant = _empreintes(base)
    page = client.get(f"/creances/proprietaires/{PROPRIO}/reglement?mouvement={quote(cle)}")
    assert page.status_code == 200 and "Régler" in page.text and "411000" in page.text
    assert _ecarts(avant, _empreintes(base)) == [], "préparer un règlement n'écrit rien"

    r = client.post(f"/creances/proprietaires/{PROPRIO}/regler",
                    data={"mouvement": cle, "acteur": ACTEUR}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/creances?message="), \
        "après règlement : retour au hub"

    # Créances & Dettes, compte, facture, échéancier : tous à jour, depuis les mêmes données.
    ligne = next(l for l in creances.creances(db_path=base) if l["facture_id_opaque"] == fid)
    assert (ligne["regle"], ligne["solde"], ligne["statut_reglement"]) == (200.0, 0.0,
                                                                           creances.ST_REGLEE)
    pos = cpt.position(PROPRIO, db_path=base)
    assert pos["creance_restante"] == 0.0 and pos["paiements_recus"] == 200.0
    assert not [p for p in reglement.positions(db_path=base) if p["proprietaire_id"] == PROPRIO]
    assert all(l["facture_id_opaque"] != fid or l["solde"] == 0
               for t in creances.echeancier(db_path=base)["tranches"] for l in t["creances"])
    # FIFO persisté et cohérent avec le calcul en mémoire.
    assert cpt.etat_persistance(PROPRIO, db_path=base)["a_jour"]
    # Le règlement canonique : encaissement VALIDE, écriture 512 / 411, mouvement rapproché.
    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM mouvements_tresorerie_proprietaires WHERE "
                            "proprietaire_id=? AND statut='VALIDE'", (PROPRIO,)).fetchone()[0] == 1
        ecr = conn.execute("SELECT ecriture_id_opaque FROM ecritures WHERE origine_type='LETTRAGE'"
                           ).fetchone()[0]
    finally:
        conn.close()
    assert _lignes(base, ecr) == [("411000", 0.0, 200.0, PROPRIO), ("512000", 200.0, 0.0, None)]
    assert _par_montant(base, 200.0)["restant"] == 0.0
    # Rejouer le même envoi ne règle rien deux fois.
    r2 = client.post(f"/creances/proprietaires/{PROPRIO}/regler",
                     data={"mouvement": cle, "acteur": ACTEUR})
    assert cpt.position(PROPRIO, db_path=base)["paiements_recus"] == 200.0


def test_04_reglement_partiel_laisse_le_reste_du(base, verrous, factures_ok):
    fid = _facture_vendue(base, {"gestion": 150.0, "menage": 50.0})
    _importer(base, [_mvt(120.0, sens="credit", contrepartie="Claire Testeur", date="2026-09-15")])
    cle = f"BANQUE:{_par_montant(base, 120.0)['id']}"
    ap = reglement.apercu(PROPRIO, cle, db_path=base)
    assert ap["ecart_a_traiter"] and ap["ecart"] == -80.0
    assert not reglement.regler(PROPRIO, cle, acteur=ACTEUR, db_path=base)["ok"], \
        "un écart n'est jamais absorbé en silence"
    assert reglement.regler(PROPRIO, cle, acteur=ACTEUR, traitement_ecart="SOLDE_OUVERT",
                            db_path=base)["ok"]
    ligne = next(l for l in creances.creances(db_path=base) if l["facture_id_opaque"] == fid)
    assert (ligne["regle"], ligne["solde"], ligne["statut_reglement"]) == (
        120.0, 80.0, creances.ST_PARTIELLE)


def test_05_regler_sans_signature_refuse(base, verrous, factures_ok, client):
    _facture_vendue(base, {"gestion": 100.0})
    _importer(base, [_mvt(100.0, sens="credit", contrepartie="Claire Testeur", date="2026-09-15")])
    cle = f"BANQUE:{_par_montant(base, 100.0)['id']}"
    r = client.post(f"/creances/proprietaires/{PROPRIO}/regler", data={"mouvement": cle})
    assert r.status_code == 200 and 'data-testid="erreur-reglement"' in r.text
    assert cpt.position(PROPRIO, db_path=base)["paiements_recus"] == 0.0


# ══ IK ══════════════════════════════════════════════════════════════════════════════════════════

def test_06_ik_simple_sans_ventilation_avantage_100_pourcent(associes):
    ik_id, _ = _ik(associes, 300.0)
    ik = ass.charger_ik(ik_id, db_path=associes)
    assert (ik["montant"], ik["avantage_reel"], ik["statut"]) == (300.0, 300.0, ass.ST_BROUILLON)
    l = _ligne(ass.suivi(associe_id=A1, db_path=associes), "2026-09")
    assert (l["ik_brute"], l["depenses_activite"], l["avantage_ik"], l["position_nette"]) == (
        300.0, 0.0, 300.0, 300.0)


def test_07_ik_avec_depenses_activite_la_charge_reste_entiere(associes):
    ik_id, cid = _ik(associes, 400.0)
    ecritures_avant = _compter(associes, "ecritures")
    for montant, nature in ((100, "MENAGE_PRESTATAIRE_INTERNE"), (50, "MENAGE_PRESTATAIRE_INTERNE"),
                            (50, "FONCTIONNEMENT")):
        assert ass.ajouter_depense(ik_id, montant, "2026-09-12", nature, acteur=ACTEUR,
                                   db_path=associes)["ok"]
    ik = ass.charger_ik(ik_id, db_path=associes)
    assert (ik["montant"], ik["depenses_total"], ik["avantage_reel"]) == (400.0, 200.0, 200.0)
    assert saisie.lire(cid, db_path=associes)["montant"] == 400.0, "comptabilité : 400 €"
    assert _compter(associes, "ecritures") == ecritures_avant, "aucune écriture analytique"


def test_08_prorata_jours_inclus_et_total_exact():
    p = ass.prorata_mensuel(630.0, date(2026, 1, 17), date(2026, 3, 20))
    assert [(x["mois"], x["jours"]) for x in p] == [("2026-01", 15), ("2026-02", 28),
                                                    ("2026-03", 20)]
    assert [x["montant"] for x in p] == [150.0, 280.0, 200.0]
    for montant, debut, fin in ((100.0, date(2026, 1, 1), date(2026, 3, 31)),
                                (0.01, date(2026, 1, 30), date(2026, 2, 2)),
                                (333.33, date(2026, 2, 27), date(2026, 5, 3))):
        parts = ass.prorata_mensuel(montant, debut, fin)
        assert round(sum(x["montant"] for x in parts), 2) == montant, (montant, parts)
    assert ass.prorata_mensuel(50.0, date(2026, 4, 9), date(2026, 4, 9)) == [
        {"mois": "2026-04", "jours": 1, "total_jours": 1, "montant": 50.0}]


def test_09_ik_multi_mois_et_depense_a_la_date_reelle_du_debit(associes):
    ik_id, _ = _ik(associes, 630.0, "2026-01-17", "2026-03-20")
    assert ass.ajouter_depense(ik_id, 100, "2026-02-12", "MENAGE_PRESTATAIRE_INTERNE",
                               acteur=ACTEUR, db_path=associes)["ok"]
    s = ass.suivi(associe_id=A1, db_path=associes)
    jan, fev, mars = (_ligne(s, m) for m in ("2026-01", "2026-02", "2026-03"))
    assert (jan["ik_brute"], fev["ik_brute"], mars["ik_brute"]) == (150.0, 280.0, 200.0)
    assert (jan["depenses_activite"], fev["depenses_activite"], mars["depenses_activite"]) == (
        0.0, 100.0, 0.0), "la dépense appartient au mois de son débit, jamais proratisée"
    assert fev["avantage_ik"] == 180.0
    assert s["cumul_historique"]["avantage_ik"] == 530.0


def test_10_depassement_refuse_par_le_serveur_et_par_la_base(associes):
    ik_id, _ = _ik(associes, 300.0)
    r = ass.ajouter_depense(ik_id, 350, "2026-09-12", "FONCTIONNEMENT", db_path=associes)
    assert not r["ok"] and r["code"] == ass.E_DEPASSEMENT
    assert ass.ajouter_depense(ik_id, 200, "2026-09-12", "FONCTIONNEMENT", db_path=associes)["ok"]
    assert ass.ajouter_depense(ik_id, 150, "2026-09-13", "FONCTIONNEMENT",
                               db_path=associes)["code"] == ass.E_DEPASSEMENT
    conn = get_db(associes)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="IK_DEPENSES_SUPERIEURES_AU_MONTANT"):
            conn.execute("INSERT INTO ik_depenses_activite (depense_id_opaque, ik_id_opaque, "
                         "montant, date_debit, nature) VALUES ('X', ?, 150, '2026-09-13', "
                         "'FONCTIONNEMENT')", (ik_id,))
    finally:
        conn.close()
    assert ass.charger_ik(ik_id, db_path=associes)["depenses_total"] == 200.0


def test_11_trajets_periode_libre_et_verrou_apres_validation(associes):
    ik_id, _ = _ik(associes, 120.0, "2026-08-20", "2026-09-10")
    r = ass.ajouter_trajets(ik_id, [
        {"date_trajet": "2026-08-21", "motif": "ACHATS", "depart": "Bureau", "destination": "Magasin",
         "km": "12,5"},
        {"date_trajet": "", "km": ""},                       # ligne vide : ignorée
        {"date_trajet": "2026-09-02", "motif": "RDV_PROPRIETAIRE", "km": "30"}],
        acteur=ACTEUR, db_path=associes)
    assert r["ok"] and r["nb"] == 2
    ik = ass.charger_ik(ik_id, db_path=associes)
    assert (ik["nb_trajets"], ik["km_total"]) == (2, 42.5)
    assert ass.ajouter_trajets(ik_id, [{"date_trajet": "2026-09-03", "km": "-4"}],
                               db_path=associes)["code"] == ass.E_TRAJET
    assert ass.changer_statut(ik_id, ass.ST_VALIDEE, db_path=associes)["ok"]
    assert ass.ajouter_trajets(ik_id, [{"date_trajet": "2026-09-04", "km": "5"}],
                               db_path=associes)["code"] == ass.E_VERROUILLEE
    assert ass.ajouter_depense(ik_id, 10, "2026-09-04", "FONCTIONNEMENT",
                               db_path=associes)["code"] == ass.E_VERROUILLEE
    assert ass.changer_statut(ik_id, ass.ST_A_CONTROLER, db_path=associes)["ok"], "rouvrir"


def test_12_une_charge_ne_porte_qu_une_ik_de_deplacement(associes):
    ik_id, cid = _ik(associes, 50.0)
    assert ass.creer_ik(cid, A1, "2026-09-01", "2026-09-02", db_path=associes)["code"] == ass.E_DEJA_IK
    repas = _charge(associes, 20.0, categorie="CHG_025")
    assert ass.creer_ik(repas, A1, "2026-09-01", "2026-09-02",
                        db_path=associes)["code"] == ass.E_CHARGE
    assert ass.creer_ik(_charge(associes, 10.0), A1, "2026-09-05", "2026-09-01",
                        db_path=associes)["code"] == ass.E_PERIODE
    assert ass.creer_ik(_charge(associes, 10.0), "PERS_X", "2026-09-01", "2026-09-02",
                        db_path=associes)["code"] == ass.E_ASSOCIE


# ══ Autres avantages ════════════════════════════════════════════════════════════════════════════

def test_13_charge_avantage_associe_saisie_puis_suivie_sans_ressaisie(associes):
    _importer(associes, [_mvt(80.0, contrepartie="RESTAURANT", date="2026-09-18")])
    m = _par_montant(associes, 80.0)
    cid = _charge_depuis(associes, m, 80.0, categorie="CHG_018", avantage_associe="OUI",
                         avantage_associe_id=A1)
    ligne = saisie.lire(cid, db_path=associes)
    assert (ligne["avantage_associe"], ligne["avantage_associe_id"]) == ("OUI", A1)
    l = _ligne(ass.suivi(associe_id=A1, db_path=associes), "2026-09")
    assert (l["autres_avantages"], l["avantages_totaux"]) == (80.0, 80.0)
    assert not ass.suivi(associe_id=A2, db_path=associes)["lignes"], "le bon associé seulement"
    # Une correction de la charge qui ne transmet pas l'avantage ne l'efface pas.
    donnees = {k: ligne[k] for k in saisie.CHAMPS_SAISIE}
    donnees["commentaire"] = "Repas client (corrigé)"
    assert saisie.modifier(cid, donnees, acteur=ACTEUR, motif="libellé", db_path=associes)["ok"]
    assert saisie.lire(cid, db_path=associes)["avantage_associe_id"] == A1


# ══ Compte courant d'associé ════════════════════════════════════════════════════════════════════

def _qualifier(db, montant, sens, nature, **kw):
    _importer(db, [_mvt(montant, sens=sens, contrepartie="Alice", date="2026-09-10",
                        libelle=f"VIR {nature}")])
    m = _par_montant(db, montant)
    return validation.valider_par_mouvement(m["id"], nature=nature, objet_id=A1, acteur=ACTEUR,
                                            db_path=db, **kw)


def test_14_apport_avantages_puis_remboursement_position_nette(associes, verrous):
    assert _qualifier(associes, 1000.0, "credit", "APPORT_ASSOCIE")["ok"]
    _charge(associes, 600.0, categorie="CHG_018", date_charge="2026-09-11",
            avantage_associe="OUI", avantage_associe_id=A1)
    l = _ligne(ass.suivi(associe_id=A1, db_path=associes), "2026-09")
    assert (l["apports_cca"], l["avantages_totaux"], l["position_nette"]) == (1000.0, 600.0, -400.0)
    assert ass.solde_cca(A1, db_path=associes) == 1000.0

    res = _qualifier(associes, 400.0, "debit", "REMBOURSEMENT_ASSOCIE")
    assert res["ok"], res
    assert _lignes(associes, res["ecriture"]["ecriture_id_opaque"]) == [
        ("455100", 400.0, 0.0, A1), ("512000", 0.0, 400.0, None)]
    l = _ligne(ass.suivi(associe_id=A1, db_path=associes), "2026-09")
    assert (l["remboursements_cca"], l["apport_cca_net"], l["position_nette"]) == (400.0, 600.0, 0.0)
    assert ass.solde_cca(A1, db_path=associes) == 600.0


def test_15_remboursement_cca_jamais_au_dela_du_solde(associes, verrous, client):
    """Blocage strict : aucune confirmation ni justification ne permet de dépasser le solde."""
    assert _qualifier(associes, 600.0, "credit", "APPORT_ASSOCIE")["ok"]
    assert ass.solde_cca(A1, db_path=associes) == 600.0
    assert _qualifier(associes, 400.0, "debit", "REMBOURSEMENT_ASSOCIE")["ok"], "400 € : accepté"
    assert ass.solde_cca(A1, db_path=associes) == 200.0
    _importer(associes, [_mvt(700.0, contrepartie="Alice", date="2026-09-12")])
    m = _par_montant(associes, 700.0)
    avant = _compter(associes, "ecritures"), _compter(associes, "banque_rapprochements")
    refus = validation.valider_par_mouvement(m["id"], nature="REMBOURSEMENT_ASSOCIE", objet_id=A1,
                                             acteur=ACTEUR, commentaire="Avance voulue",
                                             db_path=associes)
    assert refus["code"] == validation.E_DEPASSEMENT_CCA
    assert refus["message"] == ("Le remboursement demandé dépasse le solde créditeur disponible "
                                "du compte courant d'associé.")
    assert (_compter(associes, "ecritures"), _compter(associes, "banque_rapprochements")) == avant
    with pytest.raises(TypeError):
        validation.valider_par_mouvement(m["id"], nature="REMBOURSEMENT_ASSOCIE", objet_id=A1,
                                         acteur=ACTEUR, confirmer_depassement=True,
                                         db_path=associes)
    # À l'écran : un refus, aucun bouton « confirmer quand même ».
    page = client.get(f"/banques-caisse/qonto/{m['id']}/traiter?nature=REMBOURSEMENT_ASSOCIE"
                      f"&objet_id={A1}").text
    assert 'data-testid="depassement-cca"' in page and "confirmer_depassement" not in page
    r = client.post(f"/banques-caisse/qonto/{m['id']}/valider",
                    data={"nature": "REMBOURSEMENT_ASSOCIE", "objet_id": A1, "montant": "700",
                          "confirmer_depassement": "1", "commentaire": "Forcer"},
                    follow_redirects=False)
    assert "erreur=" in r.headers["location"]
    assert ass.solde_cca(A1, db_path=associes) == 200.0


# ══ Clôture, écrans, lecture seule ══════════════════════════════════════════════════════════════

def test_16_absence_d_ik_ne_bloque_jamais_la_cloture(associes):
    assert not ass.lister_ik(db_path=associes)
    p = cs.calcul_progression("2026-08", db_path=associes)
    textes = " ".join(str(e) for e in cs.elements_du_mois("2026-08", db_path=associes)).lower()
    assert "kilom" not in textes and "ik_" not in textes
    ik_id, _ = _ik(associes, 90.0, "2026-08-01", "2026-08-31")
    assert cs.calcul_progression("2026-08", db_path=associes)["nb_bloqueurs"] == p["nb_bloqueurs"], \
        "une IK, présente ou non, n'est pas un contrôle de clôture"


def test_17_ecrans_associes_et_parcours_ik(associes, client):
    cid = _charge(associes, 400.0)
    page = client.get("/associes/ik/nouvelle")
    assert page.status_code == 200 and cid in page.text
    r = client.post("/associes/ik/nouvelle", data={"charge_id": cid, "associe_id": A1,
                                                    "date_debut": "2026-09-01",
                                                    "date_fin": "2026-09-30"},
                    follow_redirects=False)
    assert r.status_code == 303
    ik_id = r.headers["location"].split("/associes/ik/")[1].split("?")[0]
    assert client.post(f"/associes/ik/{ik_id}/trajets",
                       data={"date_trajet": ["2026-09-03", "2026-09-04", ""],
                             "motif": ["LOGEMENT", "INTERVENTION", "AUTRE"],
                             "km": ["10", "20", ""], "depart": ["", "", ""],
                             "destination": ["", "", ""], "vehicule": ["", "", ""],
                             "commentaire": ["", "", ""]}).status_code == 200
    r = client.post(f"/associes/ik/{ik_id}/depenses",
                    data={"montant": "450", "date_debit": "2026-09-12",
                          "nature": "FONCTIONNEMENT"}, follow_redirects=False)
    assert "erreur=" in r.headers["location"], "dépassement refusé à l'écran aussi"
    client.post(f"/associes/ik/{ik_id}/depenses",
                data={"montant": "100", "date_debit": "2026-09-12", "nature": "FONCTIONNEMENT"})
    fiche = client.get(f"/associes/ik/{ik_id}").text
    assert 'data-testid="nb-trajets">2<' in fiche and 'data-testid="km-total">30.0<' in fiche
    assert "300.00 €" in fiche
    charge = client.get(f"/fournisseurs/{cid}").text
    assert f'href="/associes/ik/{ik_id}"' in charge and 'data-testid="avantage-associe"' in charge
    for url in ("/associes", f"/associes?associe={A1}", "/associes/mois/2026-09",
                f"/associes/mois/2026-09?associe={A1}", f"/fournisseurs/{cid}"):
        assert client.get(url).status_code == 200, url
    assert "IKA" not in client.get("/associes").text


def test_18_consulter_n_ecrit_rien(associes, verrous, factures_ok, client):
    _facture_vendue(associes, {"gestion": 80.0})
    ik_id, cid = _ik(associes, 200.0, "2026-08-15", "2026-09-15")
    ass.ajouter_trajets(ik_id, [{"date_trajet": "2026-08-16", "km": "8"}], db_path=associes)
    _importer(associes, [_mvt(80.0, sens="credit", contrepartie="Claire Testeur")])
    cle = f"BANQUE:{_par_montant(associes, 80.0)['id']}"
    avant = _empreintes(associes)
    for url in ("/creances", "/creances?soldes=1", "/dettes", "/echeancier", "/associes",
                f"/associes?associe={A1}&debut=2026-08&fin=2026-09", "/associes/mois/2026-08",
                "/associes/ik/nouvelle", f"/associes/ik/{ik_id}",
                f"/creances/proprietaires/{PROPRIO}/reglement",
                f"/creances/proprietaires/{PROPRIO}/reglement?mouvement={quote(cle)}",
                f"/comptes-proprietaires/{PROPRIO}", "/comptes-proprietaires",
                f"/fournisseurs/{cid}"):
        assert client.get(url).status_code == 200, url
    assert _ecarts(avant, _empreintes(associes)) == []


# ══ Mission 38 bis — versement propriétaire depuis le hub ═══════════════════════════════════════

def test_19_versement_proprietaire_depuis_le_hub(base, verrous, client):
    from app.services import proprietaires_tresorerie_service as tres
    cree = tres.creer(PROPRIO, "SOCIETE_VERS_PROPRIETAIRE", "REMBOURSEMENT_PROPRIETAIRE", 300.0,
                      "2026-09-05", acteur=ACTEUR, db_path=base)
    assert tres.valider(cree["mouvement_opaque"], acteur=ACTEUR, db_path=base)["ok"]
    p = next(x for x in reglement.positions(db_path=base) if x["proprietaire_id"] == PROPRIO)
    assert p["reste_a_virer"] == 300.0 and p["statut"] == "À reverser au propriétaire"
    hub = client.get("/creances").text
    assert f'href="/creances/proprietaires/{PROPRIO}/versement"' in hub
    assert "Préparer le versement propriétaire" in client.get(f"/comptes-proprietaires/{PROPRIO}").text

    _importer(base, [_mvt(300.0, contrepartie="Claire Testeur", date="2026-09-20",
                          libelle="VIR PROPRIETAIRE")])
    cle = f"BANQUE:{_par_montant(base, 300.0)['id']}"
    prep = reglement.preparer_versement(PROPRIO, db_path=base)
    assert prep["montant_propose"] == 300.0 and prep["mouvements"][0]["cle"] == cle
    avant = _empreintes(base)
    page = client.get(f"/creances/proprietaires/{PROPRIO}/versement?mouvement={quote(cle)}")
    assert page.status_code == 200 and "Régler" in page.text and "411000" in page.text
    assert _ecarts(avant, _empreintes(base)) == [], "préparer un versement n'écrit rien"

    r = client.post(f"/creances/proprietaires/{PROPRIO}/verser",
                    data={"mouvement": cle, "acteur": ACTEUR}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/creances?message=")
    # Le mécanisme canonique de Flux : rapprochement du reversement + écriture 411 / 512.
    conn = get_db(base)
    try:
        rap = conn.execute("SELECT montant_rapproche, lettrage_id_opaque FROM banque_rapprochements "
                           "WHERE type_objet='REVERSEMENT_PROPRIETAIRE' AND objet_id=? "
                           "AND statut='CONFIRME'", (cree["mouvement_opaque"],)).fetchone()
        ecr = conn.execute("SELECT ecriture_id_opaque FROM ecritures WHERE origine_type='LETTRAGE' "
                           "AND origine_id_opaque=?", (rap["lettrage_id_opaque"],)).fetchone()[0]
    finally:
        conn.close()
    assert rap["montant_rapproche"] == 300.0
    assert _lignes(base, ecr) == [("411000", 300.0, 0.0, PROPRIO), ("512000", 0.0, 300.0, None)]
    pos = cpt.position(PROPRIO, db_path=base)
    assert (pos["vire"], pos["reste_a_virer"], pos["position_nette"]) == (300.0, 0.0, 0.0)
    assert not [x for x in reglement.positions(db_path=base) if x["proprietaire_id"] == PROPRIO]
    assert "Préparer le versement propriétaire" not in client.get(
        f"/comptes-proprietaires/{PROPRIO}").text


# ══ Mission 38 bis — IK : alerte barème, jamais bloquante ═══════════════════════════════════════

def test_20_bareme_calcul_tranches_electrique_et_annee(tmp_db):
    from app.services import bareme_ik_service as bareme
    db = tmp_db
    assert bareme.montant_indicatif(500, "AUTO", 5, "THERMIQUE", 2024, db_path=db)["montant"] == 318.0
    assert bareme.montant_indicatif(6000, "AUTO", 5, "THERMIQUE", 2024,
                                    db_path=db)["montant"] == 3537.0          # 6000 × 0,357 + 1395
    assert bareme.montant_indicatif(500, "AUTO", 5, "ELECTRIQUE", 2024,
                                    db_path=db)["montant"] == 381.6           # + 20 %
    assert bareme.montant_indicatif(100, "AUTO", 9, "THERMIQUE", 2024,
                                    db_path=db)["montant"] == 69.7            # 7 CV et plus
    futur = bareme.montant_indicatif(500, "AUTO", 5, "THERMIQUE", 2026, db_path=db)
    assert futur["annee"] == 2024 and futur["annee_demandee"] == 2026
    assert not bareme.montant_indicatif(500, "", None, "", 2024, db_path=db)["ok"]
    assert not bareme.montant_indicatif(0, "AUTO", 5, "THERMIQUE", 2024, db_path=db)["ok"]


def test_21_alerte_bareme_visible_mais_jamais_bloquante(associes, client):
    au_dessus, _ = _ik(associes, 400.0)
    en_dessous, _ = _ik(associes, 300.0)
    for ik_id in (au_dessus, en_dessous):
        assert ass.ajouter_trajets(ik_id, [{"date_trajet": "2026-09-03", "motif": "LOGEMENT",
                                            "km": "500"}], db_path=associes)["ok"]
        assert ass.definir_vehicule(ik_id, libelle="Clio", type_vehicule="AUTO",
                                    puissance_fiscale="5", motorisation="THERMIQUE",
                                    db_path=associes)["ok"]
    c1 = ass.charger_ik(au_dessus, db_path=associes)["controle_bareme"]
    c2 = ass.charger_ik(en_dessous, db_path=associes)["controle_bareme"]
    assert (c1["montant_saisi"], c1["montant"], c1["depassement"]) == (400.0, 318.0, True)
    assert (c2["montant_saisi"], c2["montant"], c2["depassement"]) == (300.0, 318.0, False)
    fiche = client.get(f"/associes/ik/{au_dessus}").text
    assert 'data-testid="alerte-bareme"' in fiche and "318.00 €" in fiche
    assert 'data-testid="alerte-bareme"' not in client.get(f"/associes/ik/{en_dessous}").text
    # Jamais bloquant : la validation passe, le montant de la charge reste celui saisi.
    r = client.post(f"/associes/ik/{au_dessus}/statut", data={"statut": "VALIDEE"},
                    follow_redirects=False)
    assert "message=" in r.headers["location"]
    ik = ass.charger_ik(au_dessus, db_path=associes)
    assert ik["statut"] == ass.ST_VALIDEE and ik["montant"] == 400.0
    assert ass.definir_vehicule(au_dessus, type_vehicule="MOTO", puissance_fiscale="0",
                                db_path=associes)["code"] in (ass.E_VEHICULE, ass.E_VERROUILLEE)

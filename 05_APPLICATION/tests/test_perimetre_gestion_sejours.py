"""Séjours hors périmètre de gestion — une VRAIE résolution, depuis la clôture jusqu'au module concerné.

LE CAS (générique, aucune donnée réelle) : un logement est retiré de la gestion le 01/09 ; un séjour Hostaway arrivé
ce jour-là repart le 03/09, et d'autres arrivent bien après. Le moteur ne sait pas à qui les attribuer : ils sortent
du calcul et des factures, et bloquent la clôture. Ce qui est vérifié ici, c'est que ce bloqueur a une issue
RÉELLE, comprise depuis la page où « Traiter » mène :

  · le séjour RELEVAIT de la gestion (il commence pendant la gestion, son ménage est fait et facturé) → on prolonge
    la gestion jusqu'à son départ, depuis la fiche du logement : une période ajoutée à la suite, la ligne d'origine
    intacte, le logement toujours archivé ;
  · le séjour ne relève PAS de la gestion → on l'EXCLUT, par une décision explicite du module Réservations :
    justification obligatoire, auteur, date et heure, journal en ajout seul, décision consultable et annulable ;
  · dans les deux cas le séjour reste visible avec son historique, le bloqueur de la clôture disparaît, et la clôture
    ne fait que LIRE ce que les modules ont décidé.

Le moteur (lot4bis) est éprouvé pour de bon, en sous-processus, sur une base isolée : la décision y devient un
statut « exclu » (`EXCLU_RESULTAT`, motif `EXCLUSION_DECIDEE`), et la prolongation un séjour VALIDE chez son
propriétaire.
"""
from __future__ import annotations

import html
import re
import sqlite3
from datetime import date

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.db.connection import get_db
from app.services import cloture_modules_service as cm
from app.services import clotures_service as cs
from app.services import logements_gestion_service as lg
from app.services import orchestrateur_moteur as om
from app.services import orchestrateur_service as orch
from app.services import perimetre_gestion_service as pg

MOIS, OCTOBRE, NOVEMBRE = "2026-09", "2026-10", "2026-11"
ACTEUR = "Testeur"


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 10, 5))
    monkeypatch.setattr(lg, "aujourdhui", lambda: date(2026, 10, 5))


@pytest.fixture
def parc(tmp_db, monkeypatch):
    """Un logement RETIRÉ (gestion du 01/01 au 01/09) et un logement actif ; les séjours se sèment à la demande."""
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    fx.semer(
        tmp_db,
        logements=[
            {"logement_id": "LOG_DUO", "hostaway_listing_id": "910001", "nom_logement_officiel": "Duo de test",
             "nom_court": "Duo de test", "adresse": "", "ville": "RECETTE", "type_logement_id": "TYPE_001",
             "sur_hostaway": "OUI", "actif": "NON", "statut_parc": "RETIRE", "commentaire": "",
             "forfait_logiciel_consommables_mensuel": "0"},
            {"logement_id": "LOG_VIVANT", "hostaway_listing_id": "910002", "nom_logement_officiel": "Studio vivant",
             "nom_court": "Studio vivant", "adresse": "", "ville": "RECETTE", "type_logement_id": "TYPE_001",
             "sur_hostaway": "OUI", "actif": "OUI", "statut_parc": "GERE", "commentaire": "",
             "forfait_logiciel_consommables_mensuel": "0"},
        ],
        gestion=[
            {"gestion_id": "G_DUO_1", "logement_id": "LOG_DUO", "proprietaire_id": "PROP_D", "date_debut": "2026-01-01",
             "date_fin": "2026-09-01", "statut_gestion": "RETIRE", "source": "FICTIF", "commentaire": ""},
            {"gestion_id": "G_VIV_1", "logement_id": "LOG_VIVANT", "proprietaire_id": "PROP_V", "date_debut": "2026-01-01",
             "date_fin": "", "statut_gestion": "ACTIF", "source": "FICTIF", "commentaire": ""},
        ],
        proprietaires=[{"proprietaire_id": "PROP_D", "nom_proprietaire": "Durand", "prenom_proprietaire": "Dana",
                        "actif": "OUI"},
                       {"proprietaire_id": "PROP_V", "nom_proprietaire": "Vidal", "prenom_proprietaire": "Vic",
                        "actif": "OUI"}],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
    )
    return tmp_db


# ── Aides ─────────────────────────────────────────────────────────────────────────────────────────

def _sejour(db, rid, logement, arrivee, depart, *, canal="BOOKING", code="GESTION_LOGEMENT_MISSING",
            statut="A_CONTROLER", montant=346.39, source=None, owner=None, motif=None, hh=None, dataset="RDS-T"):
    source = source or f"HOSTAWAY_{canal}"
    conn = get_db(db)
    try:
        conn.execute("INSERT OR IGNORE INTO reservations_datasets (dataset_id, etape, nb_lignes, statut, actif) "
                     "VALUES (?, 'RESOLUES', 1, 'SUCCES', 1)", (dataset,))
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, row_hash, source, "
            "reservation_id_hostaway, reservation_hh_id, mois, logement_id, proprietaire_id, date_arrivee, "
            "date_depart, nuits, guest_count, montant_retenu, statut_controle, code_anomalie, motif_exclusion, "
            "canal) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (dataset, f"RES-HA-{rid}", f"h{rid}", source, rid, hh, arrivee[:7], logement, owner, arrivee, depart,
             2, 4, montant, statut, code, motif, canal))
        conn.commit()
    finally:
        conn.close()
    return f"RES-HA-{rid}"


def _a_cheval(db):
    return _sejour(db, "91001", "LOG_DUO", "2026-09-01", "2026-09-03", code=pg.OUT_OF_PERIOD)


def _apres_la_fin(db, rid="91002", arrivee="2026-10-30", depart="2026-11-01"):
    return _sejour(db, rid, "LOG_DUO", arrivee, depart)


def _lignes(db, sql, *params):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def _texte(page: str) -> str:
    sans = re.sub(r"<(script|style)\b.*?</\1>", " ", page, flags=re.S)
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", sans)).split())


def _reservations(db, mois=MOIS):
    return cm.analyser(mois, db_path=db)["par_cle"][cm.RESERVATIONS]


def _gestion(db, logement="LOG_DUO"):
    return _lignes(db, "SELECT * FROM ref_gestion_logements_hist WHERE logement_id=? ORDER BY date_debut", logement)


# ══ 1. Le séjour s'explique, avec les vraies actions ═══════════════════════════════════════════════

def test_01_un_sejour_a_cheval_sur_la_fin_de_gestion_est_explique_et_propose_les_vraies_actions(client, parc):
    _a_cheval(parc)
    r = client.get(f"/reservations/hors-gestion?mois={MOIS}")
    page = _texte(r.text)
    assert r.status_code == 200 and "Séjours hors périmètre de gestion — septembre 2026" in page
    assert "Duo de test" in page and "du 1er au 3 septembre 2026" in page
    assert "Booking" in page and "346,39" in page
    assert "commence pendant la gestion de ce logement (qui s'arrête le 1er septembre 2026) et se termine après" in page
    # Les deux vraies actions : prolonger (si le séjour relevait de la gestion) ou exclure (s'il n'en relevait pas).
    assert "Prolonger la gestion jusqu'au 3 septembre 2026" in page
    assert "Exclure du périmètre de gestion" in page
    assert 'data-testid="action-prolonger"' in r.text and 'data-testid="action-exclure"' in r.text
    for technique in ("GESTION_LOGEMENT", "OUT_OF_PERIOD", "RES-HA", "reservations_resolues"):
        assert technique not in page, f"{technique} ne s'affiche pas"


def test_02_traiter_depuis_la_cloture_ouvre_cette_page_et_pas_une_fiche_a_deviner(client, parc):
    _a_cheval(parc)
    g = _reservations(parc)["bloqueurs"][0]
    assert g["libelle"] == "1 séjour à cheval sur la fin de gestion"
    assert g["lien"] == f"/reservations/hors-gestion?mois={MOIS}" and g["action"] == "Traiter les séjours"
    assert g["items"][0]["lien"] == f"/reservations/hors-gestion?mois={MOIS}#sejour-RES-HA-91001"
    c = cs.creer_ou_charger(MOIS, acteur=ACTEUR, db_path=parc)
    c = cs.demarrer_preparation(c, acteur=ACTEUR, db_path=parc)
    fiche = client.get(f"/clotures/{c['cloture_id_opaque']}").text
    assert f'href="/reservations/hors-gestion?mois={MOIS}"' in fiche
    page = _texte(client.get(g["lien"]).text)
    assert "Duo de test" in page and "Prolonger la gestion jusqu'au 3 septembre 2026" in page


def test_03_la_liste_des_reservations_renvoie_vers_cette_page(client, parc):
    _a_cheval(parc)
    page = client.get("/reservations")
    assert page.status_code == 200 and 'data-testid="lien-hors-gestion"' in page.text
    assert "1 à trancher" in _texte(page.text)


# ══ 2. Le séjour relevait de la gestion : on PROLONGE la gestion ═══════════════════════════════════

def test_04_prolonger_la_gestion_ajoute_une_periode_jointive_sans_toucher_a_la_ligne_d_origine(parc):
    r = lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="Ménage fait et facturé le 03/09",
                             db_path=parc)
    assert r["ok"] is True and (r["date_debut"], r["date_fin"], r["fin_precedente"]) == (
        "2026-09-02", "2026-09-03", "2026-09-01")
    periodes = _gestion(parc)
    assert len(periodes) == 2
    origine, ajout = periodes
    assert (origine["gestion_id"], origine["date_fin"], origine["statut_gestion"]) == ("G_DUO_1", "2026-09-01", "RETIRE")
    assert (ajout["proprietaire_id"], ajout["date_debut"], ajout["date_fin"], ajout["statut_gestion"]) == (
        "PROP_D", "2026-09-02", "2026-09-03", "RETIRE"), "même propriétaire, jointive, close"
    assert "Ménage fait et facturé le 03/09" in ajout["commentaire"]
    fiche = _lignes(parc, "SELECT actif, statut_parc FROM ref_logements WHERE logement_id='LOG_DUO'")[0]
    assert (fiche["actif"], fiche["statut_parc"]) == ("NON", "RETIRE"), "le logement reste archivé"
    audit = _lignes(parc, "SELECT * FROM ref_admin_evenements WHERE action='PROLONGATION_GESTION'")
    assert len(audit) == 1 and audit[0]["acteur"] == ACTEUR and "Ménage fait" in audit[0]["commentaire"]


def test_05_la_prolongation_rend_le_sejour_a_cheval_gere_par_le_moteur(parc):
    """Le résolveur du moteur lit la nouvelle période comme la CONTINUITÉ de la gestion : aucun propriétaire n'est
    substitué, aucune nuit n'échappe — le séjour a un propriétaire."""
    from lib_ref_history import resolve_management_period
    avant = resolve_management_period(_gestion(parc), logement_id="LOG_DUO", date_arrivee="2026-09-01",
                                      date_depart="2026-09-03")
    assert avant.status == "OUT_OF_PERIOD"
    assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="ménage facturé",
                                db_path=parc)["ok"]
    apres = resolve_management_period(_gestion(parc), logement_id="LOG_DUO", date_arrivee="2026-09-01",
                                      date_depart="2026-09-03")
    assert (apres.status, apres.value) == ("OK", "PROP_D")
    # …mais un séjour d'octobre, lui, reste hors gestion : la prolongation n'est pas un blanc-seing.
    hors = resolve_management_period(_gestion(parc), logement_id="LOG_DUO", date_arrivee="2026-10-30",
                                     date_depart="2026-11-01")
    assert hors.status == "MISSING"


def test_06_prolonger_exige_une_justification_une_date_valide_et_jamais_au_futur(parc):
    for vide in ("", "   "):
        assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", justification=vide, db_path=parc)["code"] == (
            lg.E_PROLONGATION_JUSTIFICATION)
    for date_fin in ("2026-09-01", "2026-08-15", "n'importe quoi", ""):
        assert lg.prolonger_gestion("LOG_DUO", date_fin, justification="x", db_path=parc)["code"] == (
            lg.E_PROLONGATION_DATE), date_fin
    r = lg.prolonger_gestion("LOG_DUO", "2026-10-06", justification="x", db_path=parc)
    assert r["code"] == lg.E_PROLONGATION_FUTURE and "Réactiver ce logement" in r["message"]
    assert lg.prolonger_gestion("LOG_VIVANT", "2026-09-03", justification="x", db_path=parc)["code"] == (
        lg.E_PROLONGATION_INDISPONIBLE), "une gestion en cours n'a rien à prolonger"
    assert lg.prolonger_gestion("LOG_INCONNU", "2026-09-03", justification="x", db_path=parc)["ok"] is False
    assert len(_gestion(parc)) == 1, "aucun refus n'a écrit quoi que ce soit"


def test_07_la_prolongation_respecte_la_cloture_du_module_reservations(parc):
    c = cs.creer_ou_charger(MOIS, acteur=ACTEUR, db_path=parc)
    conn = get_db(parc)
    try:
        conn.execute("INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut, date_cloture, "
                     "acteur_cloture, nb_clotures) VALUES (?,?,?,'CLOS','2026-10-01 10:00:00','T',1)",
                     (MOIS, cm.RESERVATIONS, c["cloture_id_opaque"]))
        conn.commit()
    finally:
        conn.close()
    r = lg.prolonger_gestion("LOG_DUO", "2026-09-03", justification="x", db_path=parc)
    assert r["ok"] is False and r["code"] == lg.E_PROLONGATION_MOIS_CLOTURE and "clôturé" in r["message"]
    assert len(_gestion(parc)) == 1


def test_08_apres_la_prolongation_le_bloqueur_s_en_va_et_le_calcul_est_a_actualiser(parc):
    _a_cheval(parc)
    assert _reservations(parc)["nb_bloqueurs"] == 1
    assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="ménage facturé",
                                db_path=parc)["ok"]
    res = _reservations(parc)
    assert not any(g["cle"].startswith("SEJOUR_") for g in res["bloqueurs"]), "le séjour est couvert : plus bloqueur"
    assert pg.sejours_hors_gestion(MOIS, db_path=parc) == []
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(parc)}
    assert etats.get("RESERVATIONS") == orch.ST_A_RECALCULER, "la gestion a changé : le calcul est à actualiser"
    # Tout l'aval du référentiel est périmé (réservations, flux, résultats, contrôles) : un seul groupe, dit une fois.
    assert [g["libelle"] for g in res["bloqueurs"] if g["cle"].startswith("CALCUL")] == ["4 calculs à actualiser"]


def test_09_la_page_de_prolongation_montre_l_effet_avant_la_decision_puis_l_applique(client, parc):
    _a_cheval(parc)
    chemin = "/logements/LOG_DUO/prolonger-gestion?jusqu_au=2026-09-03"
    r = client.get(chemin)
    page = _texte(r.text)
    assert r.status_code == 200 and "Prolonger la gestion de « Duo de test »" in page
    assert "Gestion enregistrée" in page and "Séjours qui deviendraient gérés" in page
    assert "Du 1er au 3 septembre 2026" in page and "Booking" in page
    assert "La période d'origine n'est pas modifiée" in page and "Le logement reste archivé" in page
    assert 'name="justification"' in r.text and 'name="confirmation"' in r.text
    # Sans confirmation ni justification : refus, rien d'écrit.
    refus = client.post("/logements/LOG_DUO/prolonger-gestion",
                        data={"date_fin": "2026-09-03", "justification": "ménage facturé"}, follow_redirects=False)
    assert refus.status_code == 422 and "Cochez la case" in _texte(refus.text)
    refus = client.post("/logements/LOG_DUO/prolonger-gestion",
                        data={"date_fin": "2026-09-03", "justification": "", "confirmation": "oui"},
                        follow_redirects=False)
    assert refus.status_code == 422 and "justification est obligatoire" in _texte(refus.text)
    assert len(_gestion(parc)) == 1
    ok = client.post("/logements/LOG_DUO/prolonger-gestion",
                     data={"date_fin": "2026-09-03", "justification": "ménage facturé", "confirmation": "oui",
                           "retour": f"/reservations/hors-gestion?mois={MOIS}"}, follow_redirects=False)
    assert ok.status_code == 303 and ok.headers["location"].startswith(f"/reservations/hors-gestion?mois={MOIS}")
    assert "Gestion%20prolong" in ok.headers["location"].replace("+", "%20"), "le message dit ce qui vient d'être fait"
    assert len(_gestion(parc)) == 2


def test_10_le_retour_n_envoie_jamais_hors_de_l_application(client, parc):
    ok = client.post("/logements/LOG_DUO/prolonger-gestion",
                     data={"date_fin": "2026-09-03", "justification": "x", "confirmation": "oui",
                           "retour": "https://exemple.invalide/piege"}, follow_redirects=False)
    assert ok.status_code == 303 and ok.headers["location"].startswith("/logements/LOG_DUO")


def test_11_la_fiche_du_logement_dit_les_sejours_hors_gestion_et_propose_de_prolonger(client, parc):
    _a_cheval(parc)
    r = client.get("/logements/LOG_DUO")
    page = _texte(r.text)
    assert r.status_code == 200 and 'data-testid="alerte-sejours-hors-gestion"' in r.text
    assert "1 séjour de ce logement n'a aucune période de gestion qui le couvre en entier" in page
    assert "Prolonger la gestion jusqu'au 3 septembre 2026" in page
    assert 'data-testid="lien-prolonger-gestion"' in r.text, "logement archivé : le lien général est proposé"
    vivant = client.get("/logements/LOG_VIVANT")
    assert 'data-testid="lien-prolonger-gestion"' not in vivant.text, "une gestion en cours n'a rien à prolonger"


# ══ 3. Le séjour ne relève PAS de la gestion : décision explicite d'exclusion ══════════════════════

def test_12_exclure_exige_une_justification_et_trace_acteur_date_heure(parc):
    cle = _apres_la_fin(parc)
    for vide in ("", "  "):
        r = pg.exclure(cle, justification=vide, acteur=ACTEUR, db_path=parc)
        assert r["ok"] is False and r["code"] == "JUSTIFICATION_OBLIGATOIRE"
    assert _lignes(parc, "SELECT * FROM reservation_perimetre_decisions") == []
    r = pg.exclure(cle, justification="Logement retiré : séjour géré par le propriétaire", acteur=ACTEUR, db_path=parc)
    assert r["ok"] is True and r["mois"] == OCTOBRE
    d = _lignes(parc, "SELECT * FROM reservation_perimetre_decisions")[0]
    assert (d["statut"], d["acteur"], d["decision"], d["mois"], d["reservation_id_hostaway"]) == (
        "ACTIVE", ACTEUR, "EXCLURE_PERIMETRE_GESTION", OCTOBRE, "91002")
    assert d["justification"] == "Logement retiré : séjour géré par le propriétaire"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", d["date_decision"]), "date ET heure, locales"
    assert d["code_constat"] == pg.MISSING and d["canal"] == "Booking" and d["montant_retenu"] == 346.39
    e = _lignes(parc, "SELECT * FROM reservation_perimetre_evenements")[0]
    assert (e["type_evenement"], e["ancien_etat"], e["nouvel_etat"], e["acteur"]) == (
        "EXCLUSION", pg.ETAT_A_TRANCHER, pg.ETAT_EXCLU, ACTEUR)
    assert pg.decision_active("91002", db_path=parc)["justification"] == d["justification"], "décision consultable"
    assert pg.reservations_exclues(db_path=parc) == {"91002"}


def test_13_une_exclusion_decidee_n_est_plus_un_bloqueur_et_le_sejour_reste_visible(client, parc):
    cle = _apres_la_fin(parc)
    assert _reservations(parc, OCTOBRE)["nb_bloqueurs"] == 1
    pg.exclure(cle, justification="Logement retiré", acteur=ACTEUR, db_path=parc)
    res = _reservations(parc, OCTOBRE)
    assert res["nb_bloqueurs"] == 0, "la clôture lit la décision du module Réservations"
    info = [g for g in res["informatifs"] if g["cle"] == "SEJOUR_EXCLU_DECIDE"]
    assert info and info[0]["libelle"] == "1 séjour exclu du périmètre de gestion"
    # Le séjour reste VISIBLE, avec son historique ; la ligne calculée n'est pas touchée.
    s = pg.sejours_hors_gestion(OCTOBRE, db_path=parc)[0]
    assert s["etat"] == pg.ETAT_EXCLU and s["decision"]["justification"] == "Logement retiré"
    assert len(_lignes(parc, "SELECT * FROM reservations_resolues WHERE reservation_id_hostaway='91002'")) == 1
    page = _texte(client.get(f"/reservations/hors-gestion?mois={OCTOBRE}").text)
    assert "Exclus du périmètre de gestion" in page and "Logement retiré" in page and f"Par {ACTEUR}" in page
    assert "ne produit ni commission ni facture et ne bloque pas la clôture" in page
    assert "Réintégrer au périmètre de gestion" in page


def test_14_reintegrer_annule_la_decision_sans_rien_effacer(parc):
    cle = _apres_la_fin(parc)
    pg.exclure(cle, justification="Logement retiré", acteur=ACTEUR, db_path=parc)
    for vide in ("", " "):
        assert pg.reintegrer(cle, justification=vide, acteur=ACTEUR, db_path=parc)["code"] == "JUSTIFICATION_OBLIGATOIRE"
    assert pg.reintegrer(cle, justification="Erreur de saisie", acteur="Autre", db_path=parc)["ok"] is True
    d = _lignes(parc, "SELECT * FROM reservation_perimetre_decisions")[0]
    assert (d["statut"], d["annulee_par"], d["justification_annulation"]) == ("ANNULEE", "Autre", "Erreur de saisie")
    assert d["justification"] == "Logement retiré" and d["acteur"] == ACTEUR, "la décision d'origine reste intacte"
    assert [e["type_evenement"] for e in _lignes(parc, "SELECT * FROM reservation_perimetre_evenements ORDER BY id")] == [
        "EXCLUSION", "REINTEGRATION"]
    assert pg.decision_active("91002", db_path=parc) is None
    assert _reservations(parc, OCTOBRE)["nb_bloqueurs"] == 1, "le séjour redevient à trancher"
    # Une nouvelle décision est possible : l'unicité ne porte que sur la décision ACTIVE.
    assert pg.exclure(cle, justification="Finalement exclu", acteur=ACTEUR, db_path=parc)["ok"] is True
    assert len(_lignes(parc, "SELECT * FROM reservation_perimetre_decisions")) == 2
    assert [h["libelle"] for h in pg.historique("91002", db_path=parc)] == [
        "Exclu du périmètre de gestion", "Réintégré au périmètre de gestion", "Exclu du périmètre de gestion"]


def test_15_les_traces_de_decision_ne_se_modifient_ni_ne_s_effacent(parc):
    cle = _apres_la_fin(parc)
    pg.exclure(cle, justification="Logement retiré", acteur=ACTEUR, db_path=parc)
    conn = get_db(parc)
    try:
        for sql in ("DELETE FROM reservation_perimetre_evenements",
                    "UPDATE reservation_perimetre_evenements SET justification='autre'",
                    "DELETE FROM reservation_perimetre_decisions",
                    "UPDATE reservation_perimetre_decisions SET justification='autre'",
                    "UPDATE reservation_perimetre_decisions SET acteur='autre'"):
            with pytest.raises(sqlite3.IntegrityError, match="DECISION_PERIMETRE_TRACE"):
                conn.execute(sql)
        conn.execute("UPDATE reservation_perimetre_decisions SET statut='ANNULEE', annulee_par='x'")  # annulation : permise
    finally:
        conn.close()


def test_16_seuls_les_sejours_hostaway_hors_de_nos_periodes_s_excluent_ici(parc):
    valide = _sejour(parc, "91010", "LOG_VIVANT", "2026-09-05", "2026-09-07", statut="VALIDE", code=None,
                     owner="PROP_V")
    ambigu = _sejour(parc, "91011", "LOG_DUO", "2026-09-10", "2026-09-12", code=pg.AMBIGUOUS)
    saisie = _sejour(parc, "91012", "LOG_DUO", "2026-09-14", "2026-09-16", source="MANUEL_HORS_HOSTAWAY", hh="RESHH-1")
    for cle, attendu in ((valide, "SEJOUR_INTROUVABLE"), ("RES-HA-0000", "SEJOUR_INTROUVABLE"),
                         (ambigu, "NON_DECIDABLE"), (saisie, "NON_DECIDABLE")):
        r = pg.exclure(cle, justification="x", acteur=ACTEUR, db_path=parc)
        assert (r["ok"], r["code"]) == (False, attendu), cle
    assert _lignes(parc, "SELECT * FROM reservation_perimetre_decisions") == []
    # La saisie manuelle se corrige depuis sa réservation, l'ambiguïté depuis la fiche du logement.
    page = {s["cle"]: s for s in pg.sejours_hors_gestion(MOIS, db_path=parc)}
    assert page[saisie]["decidable"] is False and page[ambigu]["decidable"] is False


def test_17_un_sejour_vrbo_sans_proprietaire_d_un_logement_retire_est_aussi_a_trancher(parc):
    """Le moteur range un séjour VRBO sans montant sous « montant à saisir » ; la vraie cause est qu'aucune
    période de gestion ne le couvre : c'est cela qui se tranche."""
    cle = _sejour(parc, "91020", "LOG_DUO", "2026-11-20", "2026-11-23", canal="VRBO", code="VRBO_MONTANT_NON_RENSEIGNE",
                  montant=0.0, source="HOSTAWAY_VRBO_A_CONTROLER")
    s = pg.sejour(cle, db_path=parc)
    assert s["code"] == pg.MISSING and s["decidable"] and s["canal"] == "VRBO"
    assert pg.exclure(cle, justification="Annonce encore en ligne, logement retiré", acteur=ACTEUR,
                      db_path=parc)["ok"] is True


def test_18_le_module_reservations_cloture_refuse_la_decision_comme_son_annulation(parc):
    cle = _apres_la_fin(parc, arrivee="2026-09-20", depart="2026-09-22", rid="91030")
    pg.exclure(cle, justification="Logement retiré", acteur=ACTEUR, db_path=parc)
    c = cs.creer_ou_charger(MOIS, acteur=ACTEUR, db_path=parc)
    conn = get_db(parc)
    try:
        conn.execute("INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut, date_cloture, "
                     "acteur_cloture, nb_clotures) VALUES (?,?,?,'CLOS','2026-10-01 10:00:00','T',1)",
                     (MOIS, cm.RESERVATIONS, c["cloture_id_opaque"]))
        conn.commit()
    finally:
        conn.close()
    r = pg.reintegrer(cle, justification="Erreur", acteur=ACTEUR, db_path=parc)
    assert r["ok"] is False and r["code"] == "MOIS_CLOTURE" and "Rouvrez le module" in r["message"]
    assert pg.decision_active("91030", db_path=parc) is not None, "la décision d'origine est restée en place"
    autre = _apres_la_fin(parc, arrivee="2026-09-25", depart="2026-09-27", rid="91031")
    r = pg.exclure(autre, justification="x", acteur=ACTEUR, db_path=parc)
    assert r["ok"] is False and r["code"] == "MOIS_CLOTURE"


def test_19_les_pages_de_decision_exigent_justification_et_confirmation(client, parc):
    cle = _apres_la_fin(parc)
    r = client.get(f"/reservations/hors-gestion/{cle}/exclure")
    page = _texte(r.text)
    assert r.status_code == 200 and "Exclure ce séjour du périmètre de gestion" in page
    assert "Il ne produit ni commission, ni facture, ni net propriétaire" in page
    assert "Il ne bloque plus la clôture du mois" in page and "Le séjour reste visible, avec son historique" in page
    refus = client.post(f"/reservations/hors-gestion/{cle}/exclure", data={"justification": "ok"},
                        follow_redirects=False)
    assert refus.status_code == 422 and "Cochez la case" in _texte(refus.text)
    refus = client.post(f"/reservations/hors-gestion/{cle}/exclure", data={"justification": " ", "confirmation": "oui"},
                        follow_redirects=False)
    assert refus.status_code == 422 and "justification est obligatoire" in _texte(refus.text)
    assert _lignes(parc, "SELECT * FROM reservation_perimetre_decisions") == []
    ok = client.post(f"/reservations/hors-gestion/{cle}/exclure",
                     data={"justification": "Logement retiré", "confirmation": "oui"}, follow_redirects=False)
    assert ok.status_code == 303 and ok.headers["location"].startswith(f"/reservations/hors-gestion?mois={OCTOBRE}")
    page = _texte(client.get(ok.headers["location"]).text)
    assert "Séjour exclu du périmètre de gestion" in page and "Logement retiré" in page
    # Réintégration : même exigence.
    r = client.get(f"/reservations/hors-gestion/{cle}/reintegrer")
    assert r.status_code == 200 and "Décision actuelle" in _texte(r.text)
    assert client.post(f"/reservations/hors-gestion/{cle}/reintegrer", data={"justification": "x"},
                       follow_redirects=False).status_code == 422
    ok = client.post(f"/reservations/hors-gestion/{cle}/reintegrer",
                     data={"justification": "Erreur", "confirmation": "oui"}, follow_redirects=False)
    assert ok.status_code == 303
    assert pg.decision_active("91002", db_path=parc) is None


def test_20_un_sejour_deja_traite_ou_inconnu_donne_une_page_claire(client, parc):
    r = client.get("/reservations/hors-gestion/RES-HA-INCONNU/exclure")
    assert r.status_code == 404 and "Ce séjour n'est plus hors de la gestion" in _texte(r.text)
    cle = _apres_la_fin(parc)
    pg.exclure(cle, justification="x", acteur=ACTEUR, db_path=parc)
    page = _texte(client.get(f"/reservations/hors-gestion/{cle}/exclure").text)
    assert "est déjà exclu du périmètre de gestion" in page
    page = _texte(client.get("/reservations/hors-gestion?mois=2026-12").text)
    assert "Aucun séjour à trancher pour décembre 2026" in page


def test_24_les_calculs_ne_sont_perimes_que_lorsque_la_decision_change_les_chiffres(parc):
    """Un séjour hors de toute période ne pèse sur aucun chiffre : l'exclure, ou annuler l'exclusion, ne périme rien.
    Un séjour que la gestion couvre aujourd'hui (la décision prime) est autre chose : annuler son exclusion le remet
    dans la facturation — les calculs sont périmés."""
    def remis_a_jour():
        for dataset in ("RESERVATIONS", "FLUX_LOT9", "LOT10", "LOT11"):
            orch.marquer_dataset(dataset, orch.ST_A_JOUR, db_path=parc)

    def etats():
        return {d["dataset"]: d["statut"] for d in orch.etat_datasets(parc)}

    hors = _apres_la_fin(parc)
    remis_a_jour()
    assert pg.exclure(hors, justification="Logement retiré", acteur=ACTEUR, db_path=parc)["ok"]
    assert {etats()[k] for k in ("RESERVATIONS", "LOT10")} == {orch.ST_A_JOUR}, "aucun chiffre ne change"
    assert pg.reintegrer(hors, justification="Erreur", acteur=ACTEUR, db_path=parc)["ok"]
    assert {etats()[k] for k in ("RESERVATIONS", "LOT10")} == {orch.ST_A_JOUR}
    cheval = _a_cheval(parc)
    assert pg.exclure(cheval, justification="Séjour du propriétaire", acteur=ACTEUR, db_path=parc)["ok"]
    assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="finalement géré",
                                db_path=parc)["ok"]
    remis_a_jour()
    assert pg.sejour(cheval, db_path=parc)["couvert"] is True
    assert pg.reintegrer(cheval, justification="Géré finalement", acteur=ACTEUR, db_path=parc)["ok"]
    assert etats()["RESERVATIONS"] == orch.ST_A_RECALCULER, "le séjour redevient facturable : à recalculer"


# ══ 4. Le moteur : une décision et une prolongation changent VRAIMENT le calcul ═════════════════════

@pytest.fixture
def moteur(parc):
    """Le même logement retiré, mais avec les données Hostaway qu'un vrai calcul lit : trois séjours."""
    conn = get_db(parc)
    try:
        conn.execute("INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, nb_listings, "
                     "nb_reservations, nb_payouts) VALUES ('HAX-M1','R1','API','2026-10-01T00:00:00Z','SUCCES',1,3,2)")
        for rid, a, d, canal, total in (("92001", "2026-09-01", "2026-09-03", "BOOKING", 440.51),
                                        ("92002", "2026-10-30", "2026-11-01", "BOOKING", 445.09),
                                        ("92003", "2026-11-20", "2026-11-23", "VRBO", 0.0)):
            conn.execute(
                "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, source, "
                "channel_type, source_financiere, status, total_price, is_owner_stay, inclure_resultat, "
                "check_in_date, check_out_date, nights) VALUES ('HAX-M1',?,?,'HOSTAWAY',?,?,'new',?,'false','OUI',?,?,2)",
                (rid, "910001", canal, canal, total, a, d))
            if canal == "BOOKING":
                conn.execute("INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
                             "statut_calcul_payout, payout_calcule) VALUES ('HAX-M1',?,?,'NORMAL',346.39)",
                             (rid, "910001"))
        conn.execute("INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, valeur_source, "
                     "logement_id, actif, import_id) VALUES ('MAP-1','Hostaway','listingMapId','910001','LOG_DUO',"
                     "'OUI','IMP-TEST')")
        conn.execute("INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES ('2026-05','CLOTURE',"
                     "'IMP-TEST')")
        conn.commit()
    finally:
        conn.close()
    return parc


def _calculer(db):
    r = om.executer_reservations(db_path=db)
    assert r["ok"], r
    return {x["reservation_id_hostaway"]: x for x in _lignes(
        db, "SELECT * FROM reservations_resolues WHERE dataset_id=(SELECT dataset_id FROM reservations_datasets "
            "WHERE etape='RESOLUES' AND actif=1 ORDER BY rowid DESC LIMIT 1)")}


def test_21_moteur_la_prolongation_rend_le_sejour_a_cheval_valide_chez_son_proprietaire(moteur):
    avant = _calculer(moteur)
    assert (avant["92001"]["statut_controle"], avant["92001"]["code_anomalie"], avant["92001"]["proprietaire_id"]) == (
        "A_CONTROLER", pg.OUT_OF_PERIOD, None)
    assert pg.sejour("RES-HA-92001", db_path=moteur)["prolongation_jusqu_au"] == "2026-09-03"
    assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="Ménage fait et facturé",
                                db_path=moteur)["ok"]
    apres = _calculer(moteur)
    assert (apres["92001"]["statut_controle"], apres["92001"]["proprietaire_id"], apres["92001"]["code_anomalie"]) == (
        "VALIDE", "PROP_D", None), "séjour géré : calculé, facturable, chez son propriétaire"
    assert apres["92002"]["statut_controle"] == "A_CONTROLER", "octobre reste hors gestion"
    assert [s["reservation_id"] for s in pg.a_trancher("", db_path=moteur)] == ["92002", "92003"]
    assert not any(g["cle"].startswith("SEJOUR_") for g in _reservations(moteur)["bloqueurs"])


def test_22_moteur_une_exclusion_decidee_sort_le_sejour_du_calcul_sans_le_cacher(moteur):
    avant = _calculer(moteur)
    assert avant["92002"]["statut_controle"] == "A_CONTROLER" and avant["92003"]["statut_controle"] == "A_CONTROLER"
    for cle in ("RES-HA-92002", "RES-HA-92003"):          # un Booking ET un VRBO : tous canaux
        assert pg.exclure(cle, justification="Logement retiré de la gestion", acteur=ACTEUR, db_path=moteur)["ok"]
    apres = _calculer(moteur)
    for rid, canal in (("92002", "BOOKING"), ("92003", "VRBO")):
        x = apres[rid]
        assert x["statut_controle"] == "EXCLU_RESULTAT" and x["motif_exclusion"] == "EXCLUSION_DECIDEE", rid
        assert (x["impact_resultat_reel"], x["impact_resultat_comptable"]) == ("NON", "NON"), "aucune facturation"
        assert x["montant_retenu"] == 0 and not x["proprietaire_id"] and not x["code_impact"]
        assert (x["logement_id"], x["canal"]) == ("LOG_DUO", canal), "le séjour reste visible, sous son logement"
        assert x["date_arrivee"] and x["date_depart"] and "Logement retiré de la gestion" in x["commentaire"]
    assert apres["92001"]["statut_controle"] == "A_CONTROLER", "les autres séjours ne sont pas touchés"
    assert {s["reservation_id"]: s["etat"] for s in pg.sejours_hors_gestion("", db_path=moteur)} == {
        "92001": pg.ETAT_A_TRANCHER, "92002": pg.ETAT_EXCLU, "92003": pg.ETAT_EXCLU}
    # Réintégration, puis recalcul : le séjour redevient « à contrôler », comme avant la décision.
    assert pg.reintegrer("RES-HA-92002", justification="Erreur", acteur=ACTEUR, db_path=moteur)["ok"]
    redevenu = _calculer(moteur)
    assert redevenu["92002"]["statut_controle"] == "A_CONTROLER" and redevenu["92002"]["code_anomalie"] == pg.MISSING
    assert redevenu["92003"]["statut_controle"] == "EXCLU_RESULTAT"


def test_23_moteur_la_decision_prime_sur_une_periode_de_gestion_tant_qu_elle_n_est_pas_annulee(moteur):
    """Une décision d'exclusion est PRIORITAIRE : si la gestion est prolongée ensuite, le séjour reste exclu jusqu'à
    ce qu'on annule la décision — jamais une facturation silencieuse contre une décision prise."""
    _calculer(moteur)
    assert pg.exclure("RES-HA-92001", justification="Séjour du propriétaire", acteur=ACTEUR, db_path=moteur)["ok"]
    assert lg.prolonger_gestion("LOG_DUO", "2026-09-03", acteur=ACTEUR, justification="finalement géré",
                                db_path=moteur)["ok"]
    x = _calculer(moteur)["92001"]
    assert (x["statut_controle"], x["motif_exclusion"]) == ("EXCLU_RESULTAT", "EXCLUSION_DECIDEE")
    s = pg.sejours_hors_gestion(MOIS, db_path=moteur)[0]
    assert s["etat"] == pg.ETAT_EXCLU and "décision d'exclusion reste prioritaire" in s["cause"]
    assert pg.reintegrer("RES-HA-92001", justification="Géré finalement", acteur=ACTEUR, db_path=moteur)["ok"]
    y = _calculer(moteur)["92001"]
    assert (y["statut_controle"], y["proprietaire_id"]) == ("VALIDE", "PROP_D")

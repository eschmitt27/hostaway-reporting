"""Réouverture EXCEPTIONNELLE d'un mois clôturé — et ce qui la borne : mois postérieurs, mois courant, ordre des modules.

Un mois clôturé définitivement ne doit pas devenir irréversible pour toujours, mais il ne se « décloture » pas non
plus d'un clic. La réouverture exceptionnelle est :

  · EXPLICITE et à confirmation forte (justification obligatoire, mois à ressaisir, case à cocher) ;
  · COMPLÈTE : elle remet le mois dans un état où l'on peut réellement corriger — les sept modules rouverts, la
    période comptable rouverte, l'archive économique retirée (copiée avant) pour que la reclôture fige les valeurs
    corrigées — et ne laisse jamais « mois rouvert, domaines toujours verrouillés » ;
  · TRACÉE, sans rien supprimer : le journal montre clôturé → rouvert → reclôturé, la clôture d'origine reste
    lisible, l'archive retirée reste en base ;
  · SÛRE : refusée tant qu'un mois postérieur est clôturé (on rouvre du plus récent au plus ancien), atomique,
    et sans effet sur les factures émises ni sur les écritures validées.

Données FICTIVES (base temporaire). Date du jour fixée au 05/10/2026 : septembre est terminé, octobre est le mois
courant — dont la clôture ne se démarre ni ne se ferme.
"""
from __future__ import annotations

import hashlib
from datetime import date

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import charges_saisie_service as saisie
from app.services import cloture_modules_service as cm
from app.services import cloture_verrous_service as verrous_cloture
from app.services import clotures_service as cs
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_periodes_service as per
from tests.test_cloture_modules_parcours import (ACTEUR, COURANT, FUTUR, MOIS, TOUS, _cloturer,  # noqa: F401
                                                 _demarrer, _fiche, _lignes, _post_module, _recharger, _texte,
                                                 _tout_cloturer, jour)
from tests.test_cloture_flux_financiers import _ecriture_proposee
from tests.test_cloture_modules_bloqueurs import _facture_client
from tests.test_flux_financiers import base, verrous  # noqa: F401


# ── Aides ─────────────────────────────────────────────────────────────────────────────────────────

def _semer_reservation(db, rid, mois, montant=310.0):
    """Une réservation VALIDE du mois : c'est ce que l'archive économique fige à la clôture."""
    conn = get_db(db)
    try:
        conn.execute("INSERT OR IGNORE INTO reservations_datasets (dataset_id, etape, nb_lignes, statut, actif) "
                     "VALUES ('RDS-R', 'RESOLUES', 1, 'SUCCES', 1)")
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, row_hash, source, "
            "reservation_id_hostaway, mois, logement_id, proprietaire_id, date_arrivee, date_depart, nuits, "
            "montant_retenu, statut_controle, canal) VALUES ('RDS-R',?,?,'HOSTAWAY_AIRBNB',?,?,'LOG_R','PROP_R',"
            "?,?,2,?,'VALIDE','AIRBNB')",
            (f"RES-HA-{rid}", f"h{rid}", rid, mois, f"{mois}-10", f"{mois}-12", montant))
        conn.commit()
    finally:
        conn.close()


def _fermer_le_mois(db, mois=MOIS, *, commentaire=""):
    c = _demarrer(db, mois)
    _tout_cloturer(db, c)
    cs.cloturer_mois(_recharger(db, c), acteur=ACTEUR, commentaire=commentaire, db_path=db)
    return _recharger(db, c)


def _rouvrir(db, c, justification="Facture oubliée à rattacher"):
    return cs.rouvrir_exceptionnellement(_recharger(db, c), acteur=ACTEUR, justification=justification, db_path=db)


def _empreinte(db, *tables):
    h = hashlib.sha256()
    conn = get_db(db)
    try:
        for t in tables:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone() is None:
                continue
            for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1"):
                h.update(repr(tuple(r)).encode())
    finally:
        conn.close()
    return h.hexdigest()


def _evenements(db, c):
    return list(reversed(cs.historique(c["cloture_id_opaque"], db)))           # du plus ancien au plus récent


@pytest.fixture
def clos(base, verrous):
    """Septembre 2026 clôturé définitivement : sept modules, période comptable, archive d'une réservation, une écriture
    comptable validée."""
    _semer_reservation(base, "93001", MOIS, 310.0)
    compta.valider(_ecriture_proposee(base, MOIS, montant=55.0, piece="VT-REO"), acteur=ACTEUR, db_path=base)
    return _fermer_le_mois(base, MOIS, commentaire="Clôture d'origine")


# ══ 1. La page : confirmation forte, effets dits avant d'agir ═══════════════════════════════════════

def test_01_un_mois_cloture_propose_la_reouverture_exceptionnelle_et_dit_ce_qu_elle_fait(client, base, clos):
    assert clos["statut"] == cs.ST_ARCHIVEE
    fiche = client.get(f"/clotures/{clos['cloture_id_opaque']}")
    assert 'data-testid="lien-reouverture-exceptionnelle"' in fiche.text
    assert "Rouvrir exceptionnellement ce mois" in _texte(fiche.text)
    r = client.get(f"/clotures/{clos['cloture_id_opaque']}/reouverture-exceptionnelle")
    page = _texte(r.text)
    assert r.status_code == 200 and "Rouvrir exceptionnellement septembre 2026" in page
    for attendu in ("sept modules", "période comptable", "retirée et conservée", "reclôturer chaque module, puis le mois",
                    "Rien n'est supprimé", "Les factures déjà émises et les écritures déjà validées ne sont pas modifiées"):
        assert attendu in page, attendu
    for champ in ('name="justification"', 'name="mois_saisi"', 'name="confirmation"'):
        assert champ in r.text, "justification, saisie du mois et case à cocher : une confirmation forte"
    assert "Pour confirmer, saisissez le mois à rouvrir : septembre 2026" in page
    for code in ("ARCHIVEE", "ROUVERTE", "EN_CONTROLE", "REOUVERTURE_EXCEPTIONNELLE", "cloture_modules"):
        assert code not in page, f"le code technique {code} ne s'affiche pas"


def test_02_un_mois_non_cloture_n_a_rien_a_rouvrir_exceptionnellement(client, base, verrous):
    c = _demarrer(base)
    r = client.get(f"/clotures/{c['cloture_id_opaque']}/reouverture-exceptionnelle")
    assert "n'est pas clôturé définitivement" in _texte(r.text) and 'name="justification"' not in r.text
    assert 'data-testid="lien-reouverture-exceptionnelle"' not in _fiche(client, c), "le lien n'apparaît que sur un mois clos"
    with pytest.raises(cs.ClotureRefusee, match="n'est pas clôturé définitivement"):
        cs.rouvrir_exceptionnellement(c, acteur=ACTEUR, justification="essai", db_path=base)
    assert client.get("/clotures/CLO-inconnue/reouverture-exceptionnelle").status_code == 404


# ══ 2. Justification obligatoire, confirmation forte ═══════════════════════════════════════════════

def test_03_la_justification_est_obligatoire_et_un_refus_ne_change_rien(client, base, clos):
    avant = _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules", "ref_cloture_mensuelle",
                       "reservations_historique_cloture", "periodes_comptables")
    for vide in ("", "   "):
        with pytest.raises(cs.ClotureRefusee, match="justification est obligatoire"):
            _rouvrir(base, clos, vide)
    r = client.post(f"/clotures/{clos['cloture_id_opaque']}/reouverture-exceptionnelle",
                    data={"justification": " ", "mois_saisi": "septembre 2026", "confirmation": "oui"},
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur=" in r.headers["location"]
    assert _recharger(base, clos)["statut"] == cs.ST_ARCHIVEE
    assert _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules", "ref_cloture_mensuelle",
                      "reservations_historique_cloture", "periodes_comptables") == avant


def test_04_la_confirmation_est_forte_case_a_cocher_et_mois_a_ressaisir(client, base, clos):
    url = f"/clotures/{clos['cloture_id_opaque']}/reouverture-exceptionnelle"
    r = client.post(url, data={"justification": "Facture oubliée", "mois_saisi": "septembre 2026"},
                    follow_redirects=False)
    assert "erreur=" in r.headers["location"] and _recharger(base, clos)["statut"] == cs.ST_ARCHIVEE
    r = client.post(url, data={"justification": "Facture oubliée", "mois_saisi": "octobre 2026",
                               "confirmation": "oui"}, follow_redirects=False)
    assert "erreur=" in r.headers["location"], "le mauvais mois saisi refuse"
    assert "saisissez le mois" in client.get(r.headers["location"]).text.replace("&nbsp;", " ")
    assert _recharger(base, clos)["statut"] == cs.ST_ARCHIVEE
    r = client.post(url, data={"justification": "Facture oubliée", "mois_saisi": "  Septembre   2026 ",
                               "confirmation": "oui"}, follow_redirects=False)
    assert r.status_code == 303 and "message=" in r.headers["location"], "casse et espaces n'importent pas"
    assert _recharger(base, clos)["statut"] == cs.ST_ROUVERTE


# ══ 3. La réouverture remet le mois dans un état où l'on peut RÉELLEMENT corriger ═══════════════════

def test_05_tous_les_domaines_sont_deverrouilles_et_les_ecritures_redeviennent_possibles(client, base, clos):
    assert all(verrous_cloture.verrouille(MOIS, m) for m in TOUS)
    _rouvrir(base, clos, "Facture oubliée à rattacher")
    for cle in TOUS:
        assert verrous_cloture.module_clos(MOIS, cle, db_path=base) is False, cle
        assert verrous_cloture.verrouille(MOIS, cle, db_path=base) is False, f"{cle} serait encore verrouillé"
    assert verrous_cloture.mois_clos(MOIS, db_path=base) is False
    lignes = {l["module"]: l for l in _lignes(base, "SELECT * FROM cloture_modules WHERE mois=?", MOIS)}
    assert len(lignes) == 7 and {l["statut"] for l in lignes.values()} == {"ROUVERT"}
    for l in lignes.values():
        assert l["justification_reouverture"] == "Réouverture exceptionnelle du mois : Facture oubliée à rattacher"
        assert (l["acteur_reouverture"], l["nb_clotures"], l["nb_reouvertures"]) == (ACTEUR, 1, 1)
    # Réellement modifiable : une charge se saisit de nouveau sur le mois.
    assert saisie.creer({"date_charge": "2026-09-20", "montant": 30, "categorie_charge_id": "CHG_018",
                         "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
                         "affectation_type": "GLOBAL", "refacturable": "NON", "statut_controle": "VALIDE",
                         "commentaire": "Facture oubliée"}, acteur=ACTEUR, db_path=base)["ok"]
    # La période comptable est rouverte avec la même justification.
    assert per.charger(MOIS, base)["statut"] == per.ST_ROUVERTE and per.est_fermee(MOIS, base) is False
    assert "Réouverture exceptionnelle du mois : Facture oubliée" in per.historique(MOIS, base)[0]["commentaire"]


def test_06_la_source_de_cloture_du_mois_n_est_plus_close_mais_a_reclore(base, clos):
    assert _lignes(base, "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?", MOIS)[0]["statut_mois"] == "CLOTURE"
    _rouvrir(base, clos)
    assert _lignes(base, "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?", MOIS)[0]["statut_mois"] == (
        "EN_CONTROLE"), "statut canonique « à contrôler / à reclore » : plus rien ne lit ce mois comme clos"
    j = _lignes(base, "SELECT * FROM mois_reouvertures WHERE mois=?", MOIS)
    assert len(j) == 1 and (j[0]["statut_avant"], j[0]["statut_apres"], j[0]["acteur"]) == ("CLOTURE", "EN_CONTROLE", ACTEUR)
    assert j[0]["motif"] == "Facture oubliée à rattacher" and j[0]["recalcul_statut"] == "NON_LANCE"


def test_07_l_archive_figee_est_retiree_mais_conservee_ligne_par_ligne(base, clos):
    assert len(_lignes(base, "SELECT * FROM reservations_historique_cloture WHERE mois_cloture=?", MOIS)) == 1
    fige_le = _lignes(base, "SELECT fige_le, montant_retenu FROM reservations_historique_cloture")[0]
    _rouvrir(base, clos)
    assert _lignes(base, "SELECT * FROM reservations_historique_cloture WHERE mois_cloture=?", MOIS) == []
    copie = _lignes(base, "SELECT * FROM cloture_archives_retirees")
    assert len(copie) == 1
    assert (copie[0]["mois"], copie[0]["nb_reservations"], copie[0]["acteur"]) == (MOIS, 1, ACTEUR)
    assert copie[0]["justification"] == "Facture oubliée à rattacher"
    assert "93001" in copie[0]["contenu_json"] and fige_le["fige_le"] in copie[0]["contenu_json"], "la ligne d'origine est copiée en entier"
    journal = [a for a in _lignes(base, "SELECT * FROM reservations_archives") if a["motif"] == "RETRAIT_REOUVERTURE_EXCEPTIONNELLE"]
    assert len(journal) == 1 and journal[0]["mois_traites"] == MOIS
    import sqlite3
    conn = get_db(base)
    try:
        for sql in ("DELETE FROM cloture_archives_retirees", "UPDATE cloture_archives_retirees SET justification='x'"):
            with pytest.raises(sqlite3.IntegrityError, match="CLOTURE_TRACE"):
                conn.execute(sql)
    finally:
        conn.close()


def test_08_le_tableau_de_bord_montre_un_mois_rouvert_a_reclore(client, base, clos):
    _rouvrir(base, clos)
    c = _recharger(base, clos)
    assert c["statut"] == cs.ST_ROUVERTE and c["justification_reouverture"] == "Facture oubliée à rattacher"
    t = cm.tableau_de_bord(c, db_path=base)
    assert (t["nb_clos"], t["etat_cloture_libelle"], t["mois_clos"]) == (0, "Clôture rouverte", False)
    assert {m["etat"] for m in t["modules"]} == {cm.ETAT_PRET}, "aucun bloqueur : chaque module est à reclôturer"
    assert all(m["peut_cloturer"] for m in t["modules"]), "on peut déjà reclôturer ce que la correction n'a pas touché"
    page = _texte(_fiche(client, c))
    assert "Clôture rouverte" in page and "0 / 7 modules clôturés" in page and "Prêt à clôturer" in page
    assert 'data-testid="lien-reouverture-exceptionnelle"' not in _fiche(client, c), "rouvert : plus de lien"


# ══ 4. Traçabilité : le journal montre clôturé → rouvert → reclôturé ═══════════════════════════════

def test_09_le_journal_montre_cloture_rouvert_recloture_et_rien_n_est_efface(client, base, clos):
    modules_avant = _lignes(base, "SELECT * FROM cloture_modules_evenements ORDER BY id")
    transitions_avant = [e["nouveau_statut"] for e in _evenements(base, clos) if e["type_evenement"] == "TRANSITION"]
    assert transitions_avant[-1] == cs.ST_ARCHIVEE
    _rouvrir(base, clos, "Facture oubliée à rattacher")
    c = _recharger(base, clos)
    _tout_cloturer(base, c)
    cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, commentaire="Reclôture", db_path=base)
    evts = _evenements(base, clos)
    transitions = [(e["ancien_statut"], e["nouveau_statut"]) for e in evts if e["type_evenement"] == "TRANSITION"]
    i = transitions.index((cs.ST_ARCHIVEE, cs.ST_ROUVERTE))
    assert transitions[i + 1:][-1][1] == cs.ST_ARCHIVEE, "clôturé → rouvert → reclôturé, dans le journal"
    assert [t[1] for t in transitions[:i]] == transitions_avant, "la clôture d'origine est restée telle quelle"
    ex = [e for e in evts if e["type_evenement"] == "REOUVERTURE_EXCEPTIONNELLE"]
    assert len(ex) == 1 and (ex[0]["acteur"], ex[0]["ancien_statut"], ex[0]["nouveau_statut"]) == (
        ACTEUR, cs.ST_ARCHIVEE, cs.ST_ROUVERTE)
    assert "7 modules rouverts" in ex[0]["commentaire"] and "rien n'est supprimé" in ex[0]["commentaire"]
    ouverture = next(e for e in evts if e["type_evenement"] == "TRANSITION" and e["ancien_statut"] == cs.ST_ARCHIVEE)
    assert ouverture["commentaire"] == "Facture oubliée à rattacher" and ouverture["acteur"] == ACTEUR
    assert ouverture["date_evenement"], "date et heure de la réouverture"
    # Les clôtures de modules d'origine ne bougent pas : la réouverture et la reclôture s'y ajoutent.
    modules = _lignes(base, "SELECT * FROM cloture_modules_evenements ORDER BY id")
    assert modules[:len(modules_avant)] == modules_avant
    charges = [e for e in modules if e["module"] == "CHARGES"]
    assert [e["type_evenement"] for e in charges] == ["CLOTURE_MODULE", "REOUVERTURE_MODULE", "CLOTURE_MODULE"]
    assert "Facture oubliée à rattacher" in charges[1]["commentaire"]
    page = _texte(client.get(f"/clotures/{c['cloture_id_opaque']}/historique").text)
    for attendu in ("Mois clôturé définitivement", "Mois clôturé rouvert exceptionnellement", "Facture oubliée à rattacher",
                    "Réouverture exceptionnelle : ce qui a été fait", "7 modules rouverts"):
        assert attendu in page, attendu
    assert page.count("Mois clôturé définitivement") == 2, "clôturé, puis reclôturé"
    for code in ("REOUVERTURE_EXCEPTIONNELLE", "ARCHIVEE", "ROUVERTE", "TRANSITION", "MODULE_ROUVERT"):
        assert code not in page, f"le code technique {code} ne s'affiche pas"


def test_10_la_reouverture_ne_touche_ni_factures_emises_ni_ecritures_validees_ni_charges(base, clos):
    _facture_client(base, statut="EMIS", fid="FPR-REO-E", logement="LOG_E", numero="2026-09-001")
    tables = ("factures_proprietaires", "factures_proprietaires_lignes", "ecritures", "ecriture_lignes", "charges",
              "banque_mouvements", "reservations_resolues")
    avant = _empreinte(base, *tables)
    assert _lignes(base, "SELECT statut FROM ecritures WHERE journal='VENTES'") == [{"statut": "VALIDEE"}]
    _rouvrir(base, clos)
    assert _empreinte(base, *tables) == avant, "factures, écritures, charges et réservations : rien n'a bougé"
    assert _lignes(base, "SELECT statut, numero_facture FROM factures_proprietaires") == [
        {"statut": "EMIS", "numero_facture": "2026-09-001"}], "la facture émise reste émise, sous son numéro"
    assert _lignes(base, "SELECT statut FROM ecritures WHERE journal='VENTES'") == [{"statut": "VALIDEE"}]


# ══ 5. Reclôture ═══════════════════════════════════════════════════════════════════════════════════

def test_11_le_mois_se_reclot_avec_une_archive_a_jour_et_se_reouvre_encore_si_besoin(base, clos):
    _rouvrir(base, clos, "Première réouverture")
    conn = get_db(base)
    try:
        conn.execute("UPDATE reservations_resolues SET montant_retenu = 275.0 WHERE reservation_id_hostaway='93001'")
        conn.commit()
    finally:
        conn.close()
    c = _recharger(base, clos)
    _tout_cloturer(base, c)
    cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, commentaire="Reclôture", db_path=base)
    assert _recharger(base, c)["statut"] == cs.ST_ARCHIVEE
    assert _lignes(base, "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?", MOIS)[0]["statut_mois"] == "CLOTURE"
    assert all(verrous_cloture.verrouille(MOIS, m, db_path=base) for m in TOUS), "reverrouillé"
    archive = _lignes(base, "SELECT montant_retenu FROM reservations_historique_cloture WHERE mois_cloture=?", MOIS)
    assert [a["montant_retenu"] for a in archive] == [275.0], "la nouvelle archive fige la valeur CORRIGÉE"
    assert '"montant_retenu": 310.0' in _lignes(base, "SELECT contenu_json FROM cloture_archives_retirees")[0]["contenu_json"], (
        "l'ancienne valeur reste dans la copie retirée")
    ligne = _lignes(base, "SELECT nb_clotures, nb_reouvertures FROM cloture_modules WHERE module='CHARGES'")[0]
    assert (ligne["nb_clotures"], ligne["nb_reouvertures"]) == (2, 1)
    _rouvrir(base, c, "Seconde réouverture")
    assert len(_lignes(base, "SELECT * FROM cloture_archives_retirees")) == 2
    assert [l["nb_reouvertures"] for l in _lignes(base, "SELECT nb_reouvertures FROM cloture_modules")] == [2] * 7
    assert len(_lignes(base, "SELECT * FROM mois_reouvertures")) == 2


def test_12_la_comptabilite_se_reclot_par_les_etapes_de_son_automate(base, clos):
    _rouvrir(base, clos)
    assert per.charger(MOIS, base)["statut"] == per.ST_ROUVERTE
    c = _recharger(base, clos)
    _cloturer(base, c, "COMPTABILITE")
    assert per.est_fermee(MOIS, base) is True


# ══ 6. Mois postérieurs : une règle simple et sûre ═════════════════════════════════════════════════

def test_13_un_mois_posterieur_cloture_protege_le_mois_precedent(client, base, verrous, monkeypatch):
    """Septembre clôturé, octobre clôturé : rouvrir septembre changerait en silence ce sur quoi octobre s'appuie. On
    rouvre du plus récent au plus ancien — la règle la plus simple, et la plus sûre."""
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 12, 5))
    sept = _fermer_le_mois(base, "2026-09")
    octo = _fermer_le_mois(base, "2026-10")
    with pytest.raises(cs.ClotureRefusee) as exc:
        _rouvrir(base, sept)
    assert "octobre 2026" in str(exc.value) and "rouvrez d'abord" in str(exc.value)
    assert "du plus récent au plus ancien" in str(exc.value)
    url = f"/clotures/{sept['cloture_id_opaque']}/reouverture-exceptionnelle"
    page = client.get(url)
    assert "Ce mois ne peut pas être rouvert pour le moment" in _texte(page.text)
    assert "Octobre 2026" in _texte(page.text) and 'name="justification"' not in page.text
    r = client.post(url, data={"justification": "x", "mois_saisi": "septembre 2026", "confirmation": "oui"},
                    follow_redirects=False)
    assert "erreur=" in r.headers["location"]
    assert _recharger(base, sept)["statut"] == cs.ST_ARCHIVEE, "septembre est resté clôturé"
    assert verrous_cloture.mois_clos("2026-09", db_path=base) is True
    # On rouvre octobre d'abord : septembre se rouvre ensuite.
    _rouvrir(base, octo, "Correction d'octobre")
    _rouvrir(base, sept, "Correction de septembre")
    assert _recharger(base, sept)["statut"] == cs.ST_ROUVERTE and _recharger(base, octo)["statut"] == cs.ST_ROUVERTE


def test_14_un_mois_posterieur_clos_en_partie_protege_aussi_le_mois_precedent(base, verrous, monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 12, 5))
    sept = _fermer_le_mois(base, "2026-09")
    octo = _demarrer(base, "2026-10")
    _cloturer(base, octo, "BANQUE")                 # un module d'octobre est clos, pas le mois
    with pytest.raises(cs.ClotureRefusee, match="octobre 2026"):
        _rouvrir(base, sept)
    cm.rouvrir_module(_recharger(base, octo), "BANQUE", acteur=ACTEUR, justification="reprise", db_path=base)
    _rouvrir(base, sept)                            # plus aucun module clos en aval : c'est permis
    assert _recharger(base, sept)["statut"] == cs.ST_ROUVERTE


# ══ 7. Atomicité ═══════════════════════════════════════════════════════════════════════════════════

def test_15_un_echec_en_cours_de_route_n_applique_rien(base, clos, monkeypatch):
    avant = _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules",
                       "cloture_modules_evenements", "ref_cloture_mensuelle", "reservations_historique_cloture",
                       "reservations_archives", "periodes_comptables", "periode_evenements", "mois_reouvertures",
                       "cloture_archives_retirees")

    def panne(*a, **k):
        raise RuntimeError("panne simulée après l'archive, les modules et la source de clôture")

    monkeypatch.setattr(per, "rouvrir_dans", panne)
    with pytest.raises(RuntimeError):
        _rouvrir(base, clos)
    assert _recharger(base, clos)["statut"] == cs.ST_ARCHIVEE
    assert _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules",
                      "cloture_modules_evenements", "ref_cloture_mensuelle", "reservations_historique_cloture",
                      "reservations_archives", "periodes_comptables", "periode_evenements", "mois_reouvertures",
                      "cloture_archives_retirees") == avant, "ni archive retirée, ni module rouvert, ni période touchée"
    assert all(verrous_cloture.verrouille(MOIS, m, db_path=base) for m in TOUS)


def test_16_l_ecriture_comptable_desactivee_refuse_la_reouverture_entiere(base, clos, monkeypatch):
    avant = _empreinte(base, "clotures_mensuelles", "cloture_modules", "reservations_historique_cloture",
                       "periodes_comptables")
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", False, raising=False)
    with pytest.raises(cs.ClotureRefusee, match="Écriture désactivée"):
        _rouvrir(base, clos)
    assert _empreinte(base, "clotures_mensuelles", "cloture_modules", "reservations_historique_cloture",
                      "periodes_comptables") == avant


# ══ 8. Le mois courant : consultable, jamais démarré ni clôturable ═════════════════════════════════

def test_17_le_mois_courant_et_les_mois_futurs_ne_se_demarrent_pas_et_rien_n_est_cree(client, base, monkeypatch):
    from app.services import orchestrateur_moteur as moteur
    from app.services import orchestrateur_service as orch

    def calcul_interdit(*a, **k):
        raise AssertionError("démarrer ne lance aucun calcul")

    monkeypatch.setattr(orch, "actualiser", calcul_interdit)
    monkeypatch.setattr(moteur, "executer_reservations", calcul_interdit)
    avant = _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules", "ref_cloture_mensuelle",
                       "periodes_comptables", "orchestrateur_datasets")
    for mois, attendu in ((COURANT, "est encore en cours"), (FUTUR, "n'est pas commencé")):
        with pytest.raises(cs.ClotureRefusee, match=attendu):
            cs.demarrer(mois, acteur=ACTEUR, db_path=base)
        r = client.post("/clotures/demarrer", data={"mois": mois}, follow_redirects=False)
        assert r.status_code == 303 and r.headers["location"].startswith("/clotures?erreur=")
        assert cs.charger_par_mois(mois, base) is None, "un refus ne laisse aucune clôture derrière lui"
    assert _empreinte(base, "clotures_mensuelles", "cloture_evenements", "cloture_modules", "ref_cloture_mensuelle",
                      "periodes_comptables", "orchestrateur_datasets") == avant
    liste = client.get("/clotures").text
    assert 'href="/clotures/mois/2026-10">Consulter' in liste, "le mois courant se CONSULTE depuis la liste"
    # Un mois terminé, lui, se démarre (et ne se redémarre pas).
    c = cs.demarrer(MOIS, acteur=ACTEUR, db_path=base)
    assert c["statut"] == cs.ST_EN_PREPARATION and cs.demarrer(MOIS, acteur=ACTEUR, db_path=base)["version"] == c["version"]
    assert _lignes(base, "SELECT * FROM cloture_modules") == [], "démarrer ne verrouille aucun module"


def test_18_une_cloture_heritee_sur_le_mois_courant_ne_peut_ni_clore_un_module_ni_se_cloturer(client, base, verrous):
    """Une clôture ouverte AVANT cette règle peut exister en base : rien de ce qu'elle permettrait ne doit aboutir."""
    conn = get_db(base)
    try:
        conn.execute("INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) VALUES (?,?,?,?)",
                     (cs.cloture_id_opaque(COURANT), COURANT, cs.ST_EN_PREPARATION, "heritage"))
        conn.commit()
    finally:
        conn.close()
    c = cs.charger_par_mois(COURANT, base)
    for cle in TOUS:
        with pytest.raises(cs.ClotureRefusee, match="encore en cours"):
            cm.cloturer_module(c, cle, acteur=ACTEUR, db_path=base)
    with pytest.raises(cs.ClotureRefusee, match="encore en cours"):
        cs.cloturer_mois(c, acteur=ACTEUR, db_path=base)
    with pytest.raises(cs.ClotureRefusee, match="encore en cours"):
        cs.passer_a_valider(c, acteur=ACTEUR, db_path=base)
    r = _post_module(client, c, "BANQUE", "cloturer", confirmation="oui")
    assert "erreur=" in r.headers["location"]
    assert _lignes(base, "SELECT * FROM cloture_modules") == []
    assert _lignes(base, "SELECT * FROM ref_cloture_mensuelle WHERE mois=?", COURANT) == []
    t = cm.tableau_de_bord(c, db_path=base)
    assert not any(m["peut_cloturer"] for m in t["modules"]) and t["peut_cloturer_mois"] is False
    assert "Possible une fois le mois terminé" in _texte(_fiche(client, c))


# ══ 9. Aucun ordre obligatoire entre les modules ═══════════════════════════════════════════════════

def test_19_les_modules_se_cloturent_dans_l_ordre_que_l_on_veut(base, verrous):
    c = _demarrer(base)
    _cloturer(base, c, *reversed(TOUS))              # la comptabilité d'abord, les réservations en dernier
    assert cm.modules_non_clos(MOIS, db_path=base) == []
    cs.cloturer_mois(_recharger(base, c), acteur=ACTEUR, db_path=base)
    assert _recharger(base, c)["statut"] == cs.ST_ARCHIVEE
    ordre = [e["module"] for e in _lignes(base, "SELECT module FROM cloture_modules_evenements ORDER BY id")]
    assert ordre == list(reversed(TOUS)), "chaque module s'est clôturé quand on l'a voulu, sans prérequis"

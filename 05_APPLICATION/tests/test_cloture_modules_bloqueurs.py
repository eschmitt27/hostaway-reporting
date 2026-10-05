"""Clôture par modules — les bloqueurs sont ceux des VRAIS modules, jamais une copie.

Données FICTIVES (base temporaire). Date du jour FIXÉE au 05/10/2026 : septembre 2026 est un mois
terminé, octobre 2026 le mois courant.

Ce qui est vérifié : la liste des modules ; le rangement d'un bloqueur dans le bon domaine, en langage
métier (jamais un code, un nom de table ni un identifiant technique) ; le lien « Traiter » mène à la
vraie page, déjà filtrée sur le mois ; un bloqueur DISPARAÎT quand le vrai problème est corrigé dans
son module (aucune case « traité ») ; aucun contrôle n'est écarté faute de domaine ; la lecture
n'écrit rien ; la progression globale est la somme des modules.
"""
from __future__ import annotations

import hashlib
import re
from datetime import date

import pytest

from app.db.connection import get_db
from app.services import charges_saisie_service as saisie
from app.services import cloture_modules_service as cm
from app.services import clotures_service as cs
from app.services import comptabilite_ecritures_service as compta
from app.services import orchestrateur_service as orch
from tests.test_cloture_flux_financiers import _ecriture_proposee
from tests.test_flux_financiers import (ACTEUR, _charge, _importer, _mvt, base,  # noqa: F401
                                        verrous)

MOIS = "2026-09"
FLUX_VIDE = {"bloquants": [], "informatifs": []}


@pytest.fixture(autouse=True)
def jour(monkeypatch):
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 10, 5))


def _analyse(db, mois=MOIS, elements=None):
    return cm.analyser(mois, elements=[] if elements is None else elements, flux=FLUX_VIDE,
                       db_path=db)["par_cle"]


def _complete(db, mois=MOIS):
    """Analyse COMPLÈTE comme la clôture la fait (contrôles moteur + Flux + lectures directes)."""
    return cm.analyser(mois, db_path=db)["par_cle"]


def _el(code, module, *, niveau="A_CONTROLER", mois=MOIS, donnees=None, exception=False, info=False,
        lien=None):
    return {"code": code, "module": module, "niveau": "INFO" if info else niveau, "mois": mois,
            "entite_id": "ENT-1", "resume": code, "donnees": donnees or {}, "lien_module": lien,
            "ctrl_opaque": "CTRL-" + code[:8], "est_info": info, "detaille": True,
            "etat": {"anomalie_moteur_presente": not info, "exception_active": exception,
                     "statut_suivi": "OUVERT", "statut_suivi_libelle": "À traiter"}}


def _empreinte(db):
    conn = get_db(db)
    try:
        h = hashlib.sha256()
        for (nom,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            for r in conn.execute(f'SELECT * FROM "{nom}"'):
                h.update(repr(tuple(r)).encode())
        return h.hexdigest()
    finally:
        conn.close()


def _texte_visible(analyse) -> str:
    """Tout ce qu'un écran afficherait d'une analyse : libellés, raisons, actions, détails."""
    morceaux = []
    for m in analyse.values():
        morceaux += [m["libelle"], m["resume"]]
        for g in m["bloqueurs"] + m["informatifs"]:
            morceaux += [g["libelle"], g["pourquoi"], g["action"]]
            for i in g["items"]:
                morceaux += [i["libelle"], i["detail"]]
    return " ".join(str(x) for x in morceaux)


# ══ 1-3. Les modules ══════════════════════════════════════════════════════════════════════════════

def test_01_les_modules_sont_ceux_des_vrais_domaines_dans_l_ordre_conseille():
    assert [m.cle for m in cm.MODULES] == [
        cm.RESERVATIONS, cm.MENAGES, cm.CHARGES, cm.BANQUE, cm.FACTURES_CLIENTS, cm.CREANCES,
        cm.COMPTABILITE]
    assert cm.MODULES[-1].cle == cm.COMPTABILITE, "la comptabilité se clôture en dernier"
    for m in cm.MODULES:
        assert m.libelle and m.resume and m.verrouille, m.cle
        assert "{mois}" in m.consulter or m.consulter.startswith("/"), m.cle


def test_02_un_mois_sans_rien_n_a_aucun_bloqueur(base):
    a = cm.analyser(MOIS, db_path=base)
    assert [m["cle"] for m in a["modules"]] == [m.cle for m in cm.MODULES]
    assert a["nb_bloqueurs"] == 0 and all(m["nb_bloqueurs"] == 0 for m in a["modules"])


def test_03_la_progression_globale_est_la_somme_des_modules(base, verrous):
    _importer(base, [_mvt(42.0)])
    _ecriture_proposee(base, MOIS)
    prog = cs.calcul_progression(MOIS, base)
    assert prog["modules"]["nb_bloqueurs"] == prog["nb_bloqueurs"] >= 2
    assert prog["nb_bloqueurs"] == sum(m["nb_bloqueurs"] for m in prog["modules"]["modules"])


# ══ 4-5. Le calcul réel des bloqueurs et leurs compteurs ═════════════════════════════════════════

def _ligne_menages(db, *, logement="LOG_X", intervenant="INT_X", mois=MOIS, statut="A_CONTROLER",
                   ecart=1, hostaway=5, declares=6, nom="Studio des Tilleuls", intervenant_nom="Kheira"):
    """Une ligne du rapprochement des ménages, telle que le moteur Ménages la produit."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, nom_appartement, logement_id, proprietaire_id, "
            "intervenant_id, nom_intervenant, type_intervenant, nb_menages_tasks_hostaway_completed, "
            "nb_menages_declares_interne_m04, nb_menages_declares_externe, total_menages_declares, ecart, "
            "statut_controle, code_controle) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (mois, nom, logement, "PROP_X", intervenant, intervenant_nom, "INTERNE", hostaway, declares, 0,
             declares, ecart, statut, "MENAGE_ECART_NOMBRE"))
        conn.commit()
    finally:
        conn.close()


def test_04_une_ligne_de_menages_a_controler_bloque_les_menages_en_langage_metier(base):
    _ligne_menages(base)
    a = _complete(base)
    men = a[cm.MENAGES]
    assert men["nb_bloqueurs"] == 1
    g = men["bloqueurs"][0]
    assert g["libelle"] == "1 ligne de ménages à contrôler"
    assert g["items"][0]["libelle"] == "Studio des Tilleuls — Kheira"
    assert g["items"][0]["detail"] == "5 ménages réalisés chez Hostaway, 6 déclarés ou facturés"
    assert g["items"][0]["lien"] == "/menages/2026-09/LOG_X/INT_X", "la vraie fiche de la ligne"
    assert g["lien"] == "/menages/a-controler?mois=2026-09", "l'écran « À contrôler » du module"
    assert all(a[c]["nb_bloqueurs"] == 0 for c in a if c != cm.MENAGES)


def test_04_bis_un_ecart_justifie_ou_valide_ne_bloque_plus_meme_si_le_moteur_le_signale(base):
    """Le module Ménages est seul juge de ses écarts : une ligne justifiée (outrepassée, avec son motif)
    ou validée n'est plus « à contrôler ». Le constat brut du moteur, lui, continue de signaler l'écart :
    il ne doit PAS bloquer à la place du module."""
    from app.services import menages_service as men
    _ligne_menages(base)                                              # à contrôler
    _ligne_menages(base, logement="LOG_V", statut="VALIDE", nom="Studio validé")
    _ligne_menages(base, logement="LOG_A", mois="2026-08", nom="Studio d'août")
    constat = _el("MENAGE_EXTERNE_ECART_HOSTAWAY", "MENAGES_EXT",
                  donnees={"logement": "LOG_X", "nombre_facture": 6, "nombre_hostaway": 5, "mois": MOIS})
    avant = cm.analyser(MOIS, elements=[constat], flux=FLUX_VIDE, db_path=base)["par_cle"][cm.MENAGES]
    assert avant["nb_bloqueurs"] == 1, "une seule ligne à contrôler : ni la validée, ni celle d'un autre mois"
    # Justification dans le VRAI module : aucun recalcul, aucune case « traité » dans la clôture.
    assert men.enregistrer_outrepassage(MOIS, "LOG_X", "INT_X", "Ménage pour mariage")["ok"]
    apres = cm.analyser(MOIS, elements=[constat], flux=FLUX_VIDE, db_path=base)["par_cle"][cm.MENAGES]
    assert apres["nb_bloqueurs"] == 0, "écart justifié dans Ménages : le bloqueur disparaît tout seul"
    assert apres["nb_informatifs"] == 0, "et le constat brut du moteur ne revient pas sous une autre forme"


def test_04_ter_une_identification_incomplete_bloque_les_menages(base):
    _ligne_menages(base, intervenant="NON_ATTRIBUE", intervenant_nom="", statut="VALIDE", ecart=0,
                   hostaway=3, declares=3)
    g = _complete(base)[cm.MENAGES]["bloqueurs"][0]
    assert g["libelle"] == "1 ligne de ménages à contrôler"
    assert g["items"][0]["detail"] == "intervenant ou logement à identifier"


def test_04_quater_une_lecture_impossible_des_menages_ne_se_tait_pas(base, monkeypatch):
    from app.services import menages_service as men
    monkeypatch.setattr(men, "lignes_a_controler", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    men_ = _complete(base)[cm.MENAGES]
    assert men_["nb_bloqueurs"] == 1 and men_["bloqueurs"][0]["libelle"] == "1 lecture des ménages impossible"
    assert men_["bloqueurs"][0]["lien"] == "/menages?mois=2026-09"


def test_05_les_compteurs_comptent_les_elements_pas_les_groupes(base):
    elements = [_el("RESERVATION_A_CONTROLER_SANS_COMMISSION", "COMMISSIONS") for _ in range(3)]
    elements += [_el("VRBO_MONTANT_NON_RENSEIGNE", "RESERVATIONS")]
    res = _analyse(base, elements=elements)[cm.RESERVATIONS]
    assert res["nb_bloqueurs"] == 4 and len(res["bloqueurs"]) == 2
    assert res["bloqueurs"][0]["libelle"] == "3 réservations exclues du calcul de commission"
    assert res["bloqueurs"][1]["libelle"] == "1 réservation VRBO sans montant"


def test_05_bis_aucun_code_ni_identifiant_technique_n_est_expose(base, verrous):
    elements = [_el("COMMISSION_SANS_TAUX", "TRANSVERSE", niveau="BLOQUANT"),
                _el("MENAGE_HA_SANS_FACTURE_EXTERNE", "MENAGES_EXT", info=True),
                _el("UN_CODE_QUE_PERSONNE_N_A_TRADUIT", "MODULE_INCONNU")]
    _importer(base, [_mvt(42.0)])
    a = cm.analyser(MOIS, elements=elements, db_path=base)["par_cle"]
    texte = _texte_visible(a)
    assert not re.search(r"[A-Z]{3,}_[A-Z_]{3,}", texte), texte
    for interdit in ("COMMISSION_SANS_TAUX", "TRANSVERSE", "MENAGES_EXT", "Lot10", "Lot11", "Lot12",
                     "sqlite", "SELECT", "{", "}"):
        assert interdit not in texte, interdit


def test_06_un_controle_inconnu_est_range_dans_un_domaine_jamais_ecarte(base):
    a = _analyse(base, elements=[_el("UN_CODE_QUE_PERSONNE_N_A_TRADUIT", "MODULE_INCONNU")])
    assert a[cm.RESERVATIONS]["nb_bloqueurs"] == 1
    assert a[cm.RESERVATIONS]["bloqueurs"][0]["libelle"] == "1 contrôle automatique à traiter"


def test_07_exception_acceptee_et_informatif_ne_bloquent_pas(base):
    a = _analyse(base, elements=[
        _el("COMMISSION_INCOHERENTE", "COMMISSIONS", exception=True),
        _el("HC_ZERO_SOURCES_VIDES", "TRANSVERSE", info=True)])
    assert a[cm.RESERVATIONS]["nb_bloqueurs"] == 0
    assert a[cm.RESERVATIONS]["nb_informatifs"] == 1


def test_07_bis_chaque_module_d_origine_connu_a_un_domaine():
    for module in ("RESERVATIONS", "COMMISSIONS", "HOSTAWAY", "HH", "FLUX", "EXPLOITATION", "RESULTATS",
                   "REF", "TRANSVERSE", "MENAGES", "MENAGES_EXT", "M04", "CHARGES", "BANQUE",
                   "REGLEMENT", "ACOMPTES", "AIRCOVER", "IK", "AVANTAGES_ASSOCIES", "CLOTURE"):
        assert cm.MODULE_DES_CONTROLES[module] in cm.PAR_CLE, module


# ══ Charges : relues en direct, elles disparaissent dès leur validation ══════════════════════════

def _charge_a_controler(db, montant=25.0, date_="2026-09-19"):
    res = saisie.creer({"date_charge": date_, "montant": montant, "categorie_charge_id": "CHG_018",
                        "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
                        "affectation_type": "GLOBAL", "refacturable": "NON",
                        "statut_controle": "A_CONTROLER", "commentaire": "Achat à contrôler"},
                       acteur=ACTEUR, db_path=db)
    assert res["ok"], res
    return res["charge_id"]


def test_08_une_charge_non_validee_bloque_les_charges_puis_disparait_a_sa_validation(base):
    cid = _charge_a_controler(base)
    avant = _complete(base)[cm.CHARGES]
    assert avant["nb_bloqueurs"] == 1
    g = avant["bloqueurs"][0]
    assert g["libelle"] == "1 charge à valider ou à rejeter"
    assert g["lien"] == "/flux-financiers/charges?mois=2026-09&statut_controle=A_CONTROLER"
    assert g["items"][0]["montant"] == 25.0

    from app.services import justificatifs_service as justif
    justif.confirmer(justif.OBJET_CHARGE, cid, present="NON", justification="Test : sans pièce",
                     acteur=ACTEUR, db_path=base)
    assert saisie.valider_controle(cid, acteur=ACTEUR, db_path=base)["ok"]
    # Aucun recalcul du moteur entre-temps : la lecture est directe.
    assert _complete(base)[cm.CHARGES]["nb_bloqueurs"] == 0


def test_08_bis_une_charge_rejetee_est_une_decision_prise_elle_ne_bloque_plus(base):
    cid = _charge_a_controler(base)
    conn = get_db(base)
    try:
        conn.execute("UPDATE charges SET statut_controle='REJETE' WHERE charge_id=?", (cid,))
        conn.commit()
    finally:
        conn.close()
    assert _complete(base)[cm.CHARGES]["nb_bloqueurs"] == 0


def test_08_ter_une_charge_d_un_autre_mois_ne_bloque_pas_ce_mois(base):
    _charge_a_controler(base, date_="2026-08-19")
    assert _complete(base, MOIS)[cm.CHARGES]["nb_bloqueurs"] == 0
    assert _complete(base, "2026-08")[cm.CHARGES]["nb_bloqueurs"] == 1


def test_09_le_constat_moteur_d_une_charge_n_est_pas_compte_en_double(base):
    """Le contrôle du moteur (relu au prochain calcul) et la lecture directe disent la même chose :
    seule la lecture directe compte, sinon le bloqueur resterait affiché après la validation."""
    cid = _charge_a_controler(base)
    constat = _el("CHARGE_NON_VALIDEE_HORS_CALCULS", "CHARGES", niveau="BLOQUANT")
    a = cm.analyser(MOIS, elements=[constat], flux=FLUX_VIDE, db_path=base)["par_cle"]
    assert a[cm.CHARGES]["nb_bloqueurs"] == 1, "une charge, un bloqueur"
    assert cid


def test_09_bis_le_bilan_du_moteur_ne_compte_pas_ce_que_le_module_juge(base, monkeypatch):
    """Un constat que la lecture directe d'un module remplace n'est pas un « bloquant moteur » : la trace de
    clôture ne doit pas affirmer « 5 bloquants moteur » pour un mois que ses modules disent sans bloqueur."""
    constats = [_el("MENAGE_EXTERNE_ECART_HOSTAWAY", "MENAGES_EXT"),
                _el("CHARGE_NON_VALIDEE_HORS_CALCULS", "CHARGES", niveau="BLOQUANT"),
                _el("COMMISSION_INCOHERENTE", "COMMISSIONS")]
    monkeypatch.setattr(cs, "elements_du_mois", lambda mois, db_path=None: constats)
    prog = cs.calcul_progression(MOIS, base)
    assert prog["nb_bloqueurs_moteur"] == 1 and [b["code"] for b in prog["bloqueurs"]] == ["COMMISSION_INCOHERENTE"]
    assert prog["nb_bloqueurs"] == 1, "seule la commission bloque : les deux autres sont jugés par leur module"
    assert cs._resume_controles(prog).startswith("Contrôles recalculés : 1 bloquant(s) moteur")


# ══ Banque, caisse, comptabilité : Flux ══════════════════════════════════════════════════════════

def test_10_un_mouvement_a_qualifier_bloque_la_banque_avec_son_lien_filtre(client, base, verrous):
    _importer(base, [_mvt(42.0, date="2026-09-20")])
    a = _complete(base)
    banque = a[cm.BANQUE]
    assert banque["nb_bloqueurs"] == 1
    g = banque["bloqueurs"][0]
    assert g["libelle"] == "1 mouvement bancaire à qualifier"
    assert g["lien"] == "/flux-financiers/banque?mois=2026-09"
    page = client.get(g["lien"])
    assert page.status_code == 200 and "COMMERCE TEST" in page.text, "la vraie page liste le mouvement"


def test_11_une_ecriture_proposee_bloque_la_comptabilite_et_le_lien_filtre_la_vraie_page(
        client, base, verrous):
    proposee = _ecriture_proposee(base, MOIS, montant=120.0, piece="VT-PROPOSEE")
    autre_mois = _ecriture_proposee(base, "2026-08", montant=77.0, piece="VT-AOUT")
    compta.valider(_ecriture_proposee(base, MOIS, montant=55.0, piece="VT-VALIDEE"), acteur=ACTEUR,
                   db_path=base)
    compt = _complete(base)[cm.COMPTABILITE]
    assert compt["nb_bloqueurs"] == 1
    g = compt["bloqueurs"][0]
    assert g["libelle"] == "1 écriture proposée à valider"
    assert g["lien"] == "/comptabilite/ecritures?periode=2026-09&statut=PROPOSEE"
    page = client.get(g["lien"])
    assert page.status_code == 200
    assert "VT-PROPOSEE" in page.text, "la vraie page, filtrée, retrouve l'écriture à traiter"
    assert "VT-VALIDEE" not in page.text and "VT-AOUT" not in page.text
    # L'écriture se valide DANS son module ; le bloqueur disparaît seul.
    compta.valider(proposee, acteur=ACTEUR, db_path=base)
    assert _complete(base)[cm.COMPTABILITE]["nb_bloqueurs"] == 0
    assert autre_mois


def test_12_un_compte_a_definir_bloque_la_comptabilite(base, verrous):
    _charge(base, 24.0, categorie="CHG_017")      # aucune règle de mapping validée
    compt = _complete(base)[cm.COMPTABILITE]
    assert [g["libelle"] for g in compt["bloqueurs"]] == ["1 compte comptable à définir"]
    assert compt["bloqueurs"][0]["lien"] == "/comptabilite/mappings"


# ══ Réservations : séjours hors période de gestion ═══════════════════════════════════════════════

def _semer_sejour(db, *, statut="A_CONTROLER", code="GESTION_LOGEMENT_MISSING", mois=MOIS,
                  dataset="RDS-TEST"):
    conn = get_db(db)
    try:
        conn.execute("INSERT OR IGNORE INTO ref_logements (logement_id, nom_court, actif, statut_parc, "
                     "import_id) VALUES ('LOG_CMOD', 'Studio des Tilleuls', 'OUI', 'GERE', 'IMP-TEST')")
        conn.execute("INSERT OR IGNORE INTO reservations_datasets (dataset_id, etape, nb_lignes, "
                     "statut, actif) VALUES (?, 'RESOLUES', 1, 'SUCCES', 1)", (dataset,))
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, row_hash, source, "
            "mois, logement_id, date_arrivee, date_depart, montant_retenu, statut_controle, "
            "code_anomalie) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (dataset, "RES-CMOD-1", "h1", "HOSTAWAY_AIRBNB", mois, "LOG_CMOD", "2026-09-10",
             "2026-09-13", 310.0, statut, code))
        conn.commit()
    finally:
        conn.close()


def test_13_un_sejour_hors_periode_de_gestion_bloque_les_reservations_puis_disparait(base):
    _semer_sejour(base)
    res = _complete(base)[cm.RESERVATIONS]
    assert res["nb_bloqueurs"] == 1
    g = res["bloqueurs"][0]
    assert g["libelle"] == "1 séjour hors période de gestion"
    assert g["items"][0]["libelle"] == "Studio des Tilleuls"
    assert g["items"][0]["detail"] == "du 10/09 au 13/09"
    assert g["lien"] == "/logements/LOG_CMOD"
    # La gestion est rétablie, le calcul relu : le séjour devient valide, le bloqueur s'en va.
    conn = get_db(base)
    try:
        conn.execute("UPDATE reservations_resolues SET statut_controle='VALIDE', code_anomalie=NULL")
        conn.commit()
    finally:
        conn.close()
    assert _complete(base)[cm.RESERVATIONS]["nb_bloqueurs"] == 0


def test_13_ter_chaque_cause_d_exclusion_est_dite_pour_ce_qu_elle_est(base):
    """Le séjour à cheval sur la fin de gestion (arrivé pendant la gestion, parti après) n'est pas un séjour
    sans gestion : ce qu'il faut corriger — la date de fin — n'est pas ce qu'on corrige pour l'autre."""
    _semer_sejour(base, code="GESTION_LOGEMENT_OUT_OF_PERIOD")
    g = _complete(base)[cm.RESERVATIONS]["bloqueurs"][0]
    assert g["libelle"] == "1 séjour à cheval sur la fin de gestion"
    assert "archiver à la date de départ du séjour" in g["pourquoi"] and g["lien"] == "/logements/LOG_CMOD"


def test_13_bis_un_sejour_valide_ou_d_un_autre_mois_ne_bloque_pas(base):
    _semer_sejour(base, statut="VALIDE", code="")
    assert _complete(base)[cm.RESERVATIONS]["nb_bloqueurs"] == 0


# ══ Calculs obsolètes ═════════════════════════════════════════════════════════════════════════════

def test_14_un_calcul_a_recalculer_bloque_son_domaine_pas_un_calcul_jamais_lance(base):
    assert _complete(base)[cm.RESERVATIONS]["nb_bloqueurs"] == 0, "jamais calculé n'est pas en retard"
    orch.marquer_dataset("LOT10", orch.ST_A_RECALCULER, db_path=base)
    orch.marquer_dataset("MENAGES", orch.ST_ECHEC, db_path=base)
    orch.marquer_dataset("LOT12", orch.ST_A_RECALCULER, db_path=base)
    a = _complete(base)
    assert a[cm.RESERVATIONS]["bloqueurs"][0]["libelle"] == "1 calcul à actualiser"
    assert a[cm.RESERVATIONS]["bloqueurs"][0]["lien"] == "/actualisation"
    assert a[cm.MENAGES]["nb_bloqueurs"] == 1
    assert a[cm.FACTURES_CLIENTS]["nb_bloqueurs"] == 1
    orch.marquer_dataset("LOT10", orch.ST_A_JOUR, db_path=base)
    assert _complete(base)[cm.RESERVATIONS]["nb_bloqueurs"] == 0


def test_14_bis_les_calculs_a_actualiser_d_un_domaine_forment_un_seul_groupe(base):
    """Un changement de référentiel périme plusieurs calculs d'un coup : un seul bloqueur par domaine et par état,
    avec la liste des calculs — pas quatre lignes identiques « 1 calcul à actualiser »."""
    for dataset in ("RESERVATIONS", "FLUX_LOT9", "LOT10", "LOT11"):
        orch.marquer_dataset(dataset, orch.ST_A_RECALCULER, db_path=base)
    res = _complete(base)[cm.RESERVATIONS]
    assert [g["libelle"] for g in res["bloqueurs"]] == ["4 calculs à actualiser"]
    assert res["nb_bloqueurs"] == 4 and len(res["bloqueurs"][0]["items"]) == 4
    assert res["bloqueurs"][0]["lien"] == "/actualisation"


# ══ Ménages : conflits ═══════════════════════════════════════════════════════════════════════════

def test_15_un_conflit_de_declaration_bloque_les_menages_puis_disparait_une_fois_resolu(base):
    conn = get_db(base)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(menages_declarations_conflits)")]
        valeurs = {"mois": MOIS, "logement_id": "LOG_CMOD", "intervenant_id": "INT_X", "statut": "OUVERT"}
        champs = [c for c in cols if c in valeurs]
        conn.execute(f"INSERT INTO menages_declarations_conflits ({', '.join(champs)}) VALUES "
                     f"({', '.join('?' * len(champs))})", [valeurs[c] for c in champs])
        conn.commit()
    finally:
        conn.close()
    men = _complete(base)[cm.MENAGES]
    assert men["nb_bloqueurs"] == 1 and men["bloqueurs"][0]["lien"] == "/menages/conflits"
    conn = get_db(base)
    try:
        conn.execute("UPDATE menages_declarations_conflits SET statut='RESOLU'")
        conn.commit()
    finally:
        conn.close()
    assert _complete(base)[cm.MENAGES]["nb_bloqueurs"] == 0


# ══ Factures clients ═════════════════════════════════════════════════════════════════════════════

def _facture_client(db, *, statut, fid, mois=MOIS, logement="LOG_CMOD", montant=300.0, numero=None):
    conn = get_db(db)
    try:
        conn.execute("INSERT OR IGNORE INTO ref_logements (logement_id, nom_court, actif, statut_parc, "
                     "import_id) VALUES ('LOG_CMOD', 'Studio des Tilleuls', 'OUI', 'GERE', 'IMP-TEST')")
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, proprietaire_id, "
            "logement_id, mois, montant_total, statut, numero_facture, date_facture) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (fid, "FACTURE", "PROP_TFLUX", logement, mois, montant, statut, numero,
             f"{mois}-30" if numero else None))
        conn.commit()
    finally:
        conn.close()


def test_16_un_brouillon_et_une_facture_validee_bloquent_les_factures_clients(base):
    _facture_client(base, statut="BROUILLON", fid="FPR-CMOD-B", logement="LOG_B")
    _facture_client(base, statut="VALIDE", fid="FPR-CMOD-V", logement="LOG_V")
    fc = _complete(base)[cm.FACTURES_CLIENTS]
    assert fc["nb_bloqueurs"] == 2
    assert [g["libelle"] for g in fc["bloqueurs"]] == ["1 facture en brouillon",
                                                       "1 facture validée à émettre"]
    assert fc["bloqueurs"][0]["lien"] == "/factures-proprietaires?mois=2026-09&statut=BROUILLON"
    assert fc["bloqueurs"][0]["items"][0]["lien"] == "/factures-proprietaires/FPR-CMOD-B"


def test_16_bis_une_facture_emise_ou_annulee_ne_bloque_pas_les_factures_clients(base):
    _facture_client(base, statut="EMIS", fid="FPR-CMOD-E", logement="LOG_E", numero="2026-09-001")
    _facture_client(base, statut="ANNULE", fid="FPR-CMOD-A", logement="LOG_A")
    assert _complete(base)[cm.FACTURES_CLIENTS]["nb_bloqueurs"] == 0


def test_17_une_facture_a_creer_bloque_les_factures_clients(base, monkeypatch):
    from app.services import factures_proprietaires_source as source
    monkeypatch.setattr(source, "propositions_du_mois", lambda mois, ids, **k: [
        {"proprietaire_id": "PROP_TFLUX", "logement_id": "LOG_CMOD", "montant_total": 498.03,
         "statut_proposition": "PRETE", "detail": ""},
        {"proprietaire_id": "PROP_TFLUX", "logement_id": "LOG_Z", "montant_total": 10.0,
         "statut_proposition": "NON_CONCERNE", "detail": "déjà facturée"}])
    fc = _complete(base)[cm.FACTURES_CLIENTS]
    assert fc["nb_bloqueurs"] == 1
    assert fc["bloqueurs"][0]["libelle"] == "1 facture propriétaire à créer"
    assert fc["bloqueurs"][0]["lien"] == "/factures-proprietaires/proposer?mois=2026-09"
    assert fc["bloqueurs"][0]["items"][0]["montant"] == 498.03


def test_18_une_facture_emise_non_comptabilisee_bloque_la_comptabilite(base, verrous):
    _facture_client(base, statut="EMIS", fid="FPR-CMOD-E", numero="2026-09-001")
    compt = _complete(base)[cm.COMPTABILITE]
    assert compt["nb_bloqueurs"] == 1
    g = compt["bloqueurs"][0]
    assert g["libelle"] == "1 facture émise à comptabiliser"
    assert g["lien"] == "/factures-proprietaires/FPR-CMOD-E/comptabiliser"
    # Une écriture seulement PROPOSÉE est déjà dite par Flux (« écriture proposée ») : pas de doublon.
    res = compta._inserer_ecriture(
        "VENTES", f"{MOIS}-30", MOIS, "2026-09-001", "Vente", compta.ORIGINE_FACTURE, "FPR-CMOD-E",
        [{"compte": "411000", "debit": 300.0, "credit": 0, "auxiliaire": "PROP_TFLUX"},
         {"compte": "706000", "debit": 0, "credit": 300.0}], db_path=base)
    assert res["ok"], res
    compt = _complete(base)[cm.COMPTABILITE]
    assert [g["libelle"] for g in compt["bloqueurs"]] == ["1 écriture proposée à valider"]
    compta.valider(res["ecriture_id_opaque"], acteur=ACTEUR, db_path=base)
    assert _complete(base)[cm.COMPTABILITE]["nb_bloqueurs"] == 0


def test_18_bis_une_facture_hors_compta_n_a_rien_a_comptabiliser(base):
    _facture_client(base, statut="EMIS", fid="FPR-CMOD-H", numero="2026-09-002")
    conn = get_db(base)
    try:
        conn.execute("UPDATE factures_proprietaires SET hors_compta=1 WHERE facture_id_opaque='FPR-CMOD-H'")
        conn.commit()
    finally:
        conn.close()
    assert _complete(base)[cm.COMPTABILITE]["nb_bloqueurs"] == 0


# ══ Créances et dettes : position du compte ══════════════════════════════════════════════════════

def test_19_une_position_de_compte_perimee_bloque_les_creances_puis_disparait_au_recalcul(base):
    from app.services import compte_proprietaire_service as cpt
    _facture_client(base, statut="EMIS", fid="FPR-CMOD-C", numero="2026-09-003", montant=200.0)
    conn = get_db(base)
    try:
        conn.execute(
            "INSERT INTO proprietaire_recalculs (recalcul_id, proprietaire_id, horodatage, "
            "empreinte_entrees, empreinte_allocations, nb_factures, nb_sources, nb_allocations, "
            "montant_alloue, creance_restante, credit_restant, declencheur) "
            "VALUES ('RCL-PERIME','PROP_TFLUX','2026-01-01T00:00:00Z','vieille','vieille',0,0,0,0,0,0,'MANUEL')")
        conn.commit()
    finally:
        conn.close()
    cre = _complete(base)[cm.CREANCES]
    assert cre["nb_bloqueurs"] == 1
    assert cre["bloqueurs"][0]["libelle"] == "1 position de compte à recalculer"
    assert cre["bloqueurs"][0]["lien"] == "/comptes-proprietaires/PROP_TFLUX"
    cpt.recalculer("PROP_TFLUX", declencheur=cpt.DECL_MANUEL, db_path=base)
    assert _complete(base)[cm.CREANCES]["nb_bloqueurs"] == 0


# ══ Contrôles comptables ═════════════════════════════════════════════════════════════════════════

def test_20_une_ecriture_desequilibree_est_un_bloqueur_comptable(base):
    conn = get_db(base)
    try:
        conn.execute("INSERT INTO ecritures (ecriture_id_opaque, journal, periode, date_ecriture, "
                     "piece, libelle, origine_type, origine_id_opaque, statut, total_debit, "
                     "total_credit) VALUES ('ECR-DESEQ','VENTES',?,?,?,?,?,?,?,?,?)",
                     (MOIS, f"{MOIS}-10", "P-DESEQ", "Écriture bancale", "MANUEL", None, "VALIDEE",
                      100.0, 90.0))
        conn.commit()
    finally:
        conn.close()
    compt = _complete(base)[cm.COMPTABILITE]
    assert any(g["libelle"] == "1 écriture déséquilibrée" for g in compt["bloqueurs"])


# ══ Lecture seule, liens, absence de duplication ═════════════════════════════════════════════════

def test_21_analyser_n_ecrit_rien(base, verrous):
    _importer(base, [_mvt(42.0)])
    _ecriture_proposee(base, MOIS)
    _charge_a_controler(base)
    _semer_sejour(base)
    _facture_client(base, statut="BROUILLON", fid="FPR-CMOD-B")
    avant = _empreinte(base)
    cs.calcul_progression(MOIS, base)
    cm.analyser(MOIS, db_path=base)
    assert _empreinte(base) == avant


def test_22_chaque_lien_traiter_mene_a_une_vraie_page(client, base, verrous):
    _importer(base, [_mvt(42.0)])
    _ecriture_proposee(base, MOIS)
    _charge_a_controler(base)
    _semer_sejour(base)
    _facture_client(base, statut="BROUILLON", fid="FPR-CMOD-B")
    _facture_client(base, statut="VALIDE", fid="FPR-CMOD-V", logement="LOG_V")
    liens = set()
    for m in cm.analyser(MOIS, db_path=base)["modules"]:
        liens.add(m["consulter"])
        for g in m["bloqueurs"]:
            liens.add(g["lien"])
            liens |= {i["lien"] for i in g["items"] if i["lien"]}
    assert len(liens) >= 8
    for lien in sorted(liens):
        reponse = client.get(lien)
        assert reponse.status_code == 200, (lien, reponse.status_code)


def test_23_aucune_table_ne_recopie_les_anomalies_de_cloture():
    """Les anomalies restent calculées depuis leur source : aucune table « problèmes de clôture »."""
    from pathlib import Path
    racine = Path(cm.__file__).resolve().parents[1] / "db" / "migrations"
    ddl = " ".join(p.read_text(encoding="utf-8", errors="ignore") for p in racine.glob("*.sql"))
    for table in re.findall(r"CREATE TABLE IF NOT EXISTS (cloture\w*|clotures\w*)", ddl):
        assert table in {"clotures_mensuelles", "cloture_evenements", "cloture_elements",
                         "cloture_documents", "cloture_statuts", "cloture_statut_evenements",
                         "cloture_modules", "cloture_modules_evenements"}, table

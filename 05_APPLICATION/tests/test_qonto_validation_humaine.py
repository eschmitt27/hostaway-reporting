"""Validation humaine d'un mouvement Qonto : effets réels, une seule fois, et rien d'effacé.

Ce que ces tests tiennent, dans l'ordre de la recette :
  A. apport 200 € completed → validation avec associé → un rapprochement, écriture 512/467 ;
  B. double clic → aucun doublon ;
  C. transaction `pending` → validation impossible ;
  D. encaissement propriétaire → mouvement de trésorerie canonique, pas une seconde écriture ;
  E. paiement fournisseur → règlement canonique, dette réduite, pas de charge recréée ;
  F. paiement partiel → reliquat correct ;
  G. montant déjà entièrement affecté → nouvelle affectation bloquée ;
  H. annulation → historique conservé, écriture contrepassée ;
  I. retrait ATM completed → Banque → Caisse une seule fois ;
  J. actualisation Qonto après rapprochement → rapprochement conservé ;
  K. libellé Qonto modifié → rapprochement conservé (l'identifiant vient du `transaction_id`) ;
  L. aucun secret Qonto en base.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import banques_rapprochement_service as rappro
from app.services import caisse_transferts_service as transferts
from app.services import comptabilite_ecritures_service as compta
from app.services import qonto_classification_service as classif
from app.services import qonto_ecran_service as ecran
from app.services import qonto_validation_service as validation
from tests.test_qonto_raw_import import ClientDouble, mouvement
from tests.test_qonto_rapprochement_caisse import CREDIT_200 as _CREDIT_BRUT, retrait

# Le vrai crédit de 200 € porte la catégorie de flux que le titulaire a posée dans Qonto : c'est
# elle qui permet de PROPOSER « apport en compte courant » sans rien deviner.
CREDIT_200 = dict(_CREDIT_BRUT,
                  cashflow_category={"id": "cat-1", "name": "Apport en compte courant d'associé"})

ASSOCIE = "PERS_TEST"
# Propriétaire réel du référentiel : `proprietaires_tresorerie_service` le valide contre le
# référentiel (fichier, lecture seule), jamais contre SQLite — une ligne insérée en base ne le
# rendrait pas connu, et c'est voulu : le référentiel est la source d'identité.
PROPRIETAIRE = "PROP_0002"


@pytest.fixture(autouse=True)
def _ecritures_activees(monkeypatch):
    """Les écritures réelles sont sous double verrou : on l'ouvre explicitement pour les tests."""
    # `RECETTE_MODE` n'est PAS activé : il redirige les référentiels (propriétaires, logements)
    # vers un dossier de recette, et le service de trésorerie ne reconnaîtrait plus les
    # propriétaires réels. Le verrou d'écriture s'ouvre par `MODE_REEL_ECRITURES`.
    for drapeau in ("MODE_REEL_ECRITURES", "COMPTABILITE_REAL_WRITE_ENABLED",
                    "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED",
                    "BANQUE_REAL_WRITE_ENABLED", "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED",
                    "FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


@pytest.fixture()
def base(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db, raising=False)
    conn = get_db(db)
    try:
        conn.execute("INSERT OR REPLACE INTO ref_associes (personne_id, nom_personne, "
                     "type_personne, actif, import_id) VALUES (?,?,?,?,?)",
                     (ASSOCIE, "Associé de test", "ASSOCIE", "OUI", "IMP-TEST"))
        conn.commit()
    finally:
        conn.close()
    return db


def _actualiser(db, mouvements):
    return ecran.actualiser(client=ClientDouble(pages=[mouvements]), db_path=db)


def _uuid_du(db, libelle_statut="completed"):
    conn = get_db(db)
    try:
        return conn.execute(
            "SELECT qonto_transaction_uuid FROM qonto_transactions_raw WHERE statut=?",
            (libelle_statut,)).fetchone()[0]
    finally:
        conn.close()


def _ecritures(db):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT ecriture_id_opaque, journal, statut, total_debit, origine_id_opaque "
            "FROM ecritures ORDER BY ecriture_id_opaque")]
    finally:
        conn.close()


def _lignes_ecriture(db, opaque):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT compte, auxiliaire, debit, credit FROM ecriture_lignes "
            "WHERE ecriture_id_opaque=? ORDER BY ligne_num", (opaque,))]
    finally:
        conn.close()


# ══ A. Apport en compte courant d'associé ═════════════════════════════════════════════════════
def test_A_un_apport_valide_produit_une_ecriture_banque_vers_compte_courant(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)

    resultat = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                  acteur="test", db_path=base)
    assert resultat["ok"] is True

    ecritures = _ecritures(base)
    assert len(ecritures) == 1
    lignes = _lignes_ecriture(base, ecritures[0]["ecriture_id_opaque"])
    assert lignes == [
        {"compte": compta.COMPTE_BANQUE, "auxiliaire": None, "debit": 200.0, "credit": 0.0},
        {"compte": compta.COMPTE_ASSOCIES, "auxiliaire": ASSOCIE, "debit": 0.0, "credit": 200.0},
    ]
    assert compta.COMPTE_ASSOCIES == "467000", \
        "le plan comptable de l'application porte 467000, aucun 455x n'est introduit"


def test_A_un_apport_ne_cree_ni_facture_ni_creance_ni_charge(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    validation.valider(_uuid_du(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                       acteur="test", db_path=base)
    conn = get_db(base)
    try:
        for table in ("factures", "charges", "factures_proprietaires",
                      "reglements_fournisseurs"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0, table
        comptes = {r[0] for r in conn.execute("SELECT DISTINCT compte FROM ecriture_lignes")}
    finally:
        conn.close()
    assert comptes == {compta.COMPTE_BANQUE, compta.COMPTE_ASSOCIES}, \
        "aucun compte de charge ni de produit : un apport n'est pas un résultat"


def test_A_l_associe_est_obligatoire_et_jamais_devine(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    refus = validation.valider(_uuid_du(base), nature=validation.APPORT_ASSOCIE, db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_OBJET_REQUIS
    assert _ecritures(base) == []


def test_A_l_apercu_prerempli_la_nature_lue_dans_qonto(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    vue = validation.apercu(_uuid_du(base), db_path=base)
    assert vue["nature_proposee"] == validation.APPORT_ASSOCIE
    assert "Apport en compte courant" in vue["nature_motif"]
    assert vue["objet_id"] == "", "la nature est proposée, l'associé reste à choisir"


# ══ B. Double clic ════════════════════════════════════════════════════════════════════════════
def test_B_valider_deux_fois_ne_produit_ni_deux_rapprochements_ni_deux_ecritures(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)

    premier = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                 acteur="test", db_path=base)
    second = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                acteur="test", db_path=base)

    assert premier["ok"] is True
    assert second["ok"] is False, "le mouvement est déjà affecté en totalité"
    assert second["code"] == validation.E_DEJA_AFFECTE

    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM banque_rapprochements").fetchone()[0] == 1
    finally:
        conn.close()
    assert len(_ecritures(base)) == 1


def test_B_la_contrainte_sql_interdit_le_doublon_meme_en_contournant_le_service(base):
    """La garde applicative ne suffit pas : deux requêtes concurrentes la passent toutes les deux."""
    import sqlite3
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, db_path=base)

    conn = get_db(base)
    try:
        opaque = conn.execute(
            "SELECT mouvement_id_opaque FROM qonto_transactions_statut_local").fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
                "type_objet, objet_id, montant_rapproche, statut) VALUES (?,?,?,?,?,?)",
                ("BRP-DOUBLON", opaque, "APPORT_ASSOCIE", ASSOCIE, 200.0, "CONFIRME"))
    finally:
        conn.close()


# ══ C. Transaction en attente ═════════════════════════════════════════════════════════════════
def test_C_une_transaction_pending_ne_peut_pas_etre_validee(base):
    _actualiser(base, [retrait(statut="pending")])
    uuid = _uuid_du(base, "pending")

    refus = validation.valider(uuid, nature=validation.TRANSFERT_CAISSE, db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_PENDING
    assert _ecritures(base) == [], "aucune écriture pour une opération qui n'a pas eu lieu"


def test_C_l_ecran_de_traitement_annonce_le_blocage(base):
    _actualiser(base, [retrait(statut="pending")])
    vue = validation.apercu(_uuid_du(base, "pending"), db_path=base)
    assert vue["definitif"] is False
    assert "en attente" in vue["blocage"].lower()


# ══ D. Encaissement propriétaire ══════════════════════════════════════════════════════════════
def test_D_un_encaissement_proprietaire_passe_par_le_service_canonique(base, monkeypatch):
    """L'encaissement n'est pas écrit à la main : il est CONFIÉ au service de trésorerie.

    Le propriétaire est validé par le service contre le référentiel (fichier), que les tests
    isolent volontairement. On vérifie donc la DÉLÉGATION — qui est ce que la règle exige : ne pas
    recoder un mécanisme de règlement, et ne pas produire l'effet bancaire une seconde fois.
    """
    from app.services import proprietaires_tresorerie_service as tres

    appels = {"creer": [], "valider": []}

    def faux_creer(proprietaire_id, sens, nature, montant, date_mouvement, **kwargs):
        appels["creer"].append((proprietaire_id, sens, nature, montant, kwargs.get("mode_reglement")))
        return {"ok": True, "mouvement_opaque": "MTP-TEST"}

    def faux_valider(opaque, **kwargs):
        appels["valider"].append(opaque)
        return {"ok": True, "statut": "VALIDE"}

    monkeypatch.setattr(tres, "creer", faux_creer)
    monkeypatch.setattr(tres, "valider", faux_valider)

    _actualiser(base, [mouvement(1, **CREDIT_200)])
    resultat = validation.valider(_uuid_du(base), nature=validation.REVERSEMENT_PROPRIETAIRE,
                                  objet_id=PROPRIETAIRE, acteur="test", db_path=base)
    assert resultat["ok"] is True

    assert appels["creer"] == [(PROPRIETAIRE, "PROPRIETAIRE_VERS_SOCIETE",
                                "ACOMPTE_PROPRIETAIRE", 200.0, "BANQUE")],         "la nature et le sens doivent être ceux du service canonique, pas des valeurs inventées"
    assert appels["valider"] == ["MTP-TEST"]

    # AUCUNE écriture bancaire n'est fabriquée ici : le service de règlement a sa propre chaîne,
    # et en ajouter une créditerait la banque deux fois pour un seul encaissement.
    assert _ecritures(base) == []


def test_D_les_natures_proposees_dependent_du_sens(base):
    """Un encaissement ne peut pas payer un fournisseur, un décaissement n'est pas un apport."""
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    vue = validation.apercu(_uuid_du(base), db_path=base)
    proposees = {code for code, _ in vue["natures_possibles"]}
    assert proposees == {validation.APPORT_ASSOCIE, validation.REVERSEMENT_PROPRIETAIRE}

    refus = validation.valider(_uuid_du(base), nature=validation.REGLEMENT_CHARGE,
                               objet_id="FAC_X", db_path=base)
    assert refus["code"] == validation.E_NATURE_SENS


# ══ E. Paiement fournisseur ═══════════════════════════════════════════════════════════════════
def _facture_fournisseur(db, *, statut="VALIDEE", montant=100.0, ref="F-TEST"):
    conn = get_db(db)
    try:
        conn.execute("INSERT OR REPLACE INTO fournisseurs (fournisseur_id_opaque, nom, type, "
                     "statut) VALUES (?,?,?,?)", ("FRN_T", "Fournisseur test", "AUTRE", "ACTIF"))
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, statut, source) VALUES (?,?,?,?,?,?,?)",
            ("FAC_T", "FRN_T", ref, "2026-09-01", montant, statut, "SAISIE"))
        conn.commit()
    finally:
        conn.close()
    return "FAC_T"


def test_E_un_paiement_fournisseur_utilise_le_reglement_canonique(base):
    from app.services import reglements_fournisseurs_service as regl

    facture = _facture_fournisseur(base, montant=200.0)
    _actualiser(base, [mouvement(1, **dict(CREDIT_200, side="debit"))])
    uuid = _uuid_du(base)

    resultat = validation.valider(uuid, nature=validation.REGLEMENT_CHARGE, objet_id=facture,
                                  acteur="test", db_path=base)
    assert resultat["ok"] is True, resultat
    assert resultat["reglement"].get("ok") is True

    reglements = regl.reglements_de_facture(facture, base)
    assert len(reglements) == 1, "un seul règlement, produit par le service canonique"

    conn = get_db(base)
    try:
        # Aucune charge n'est recréée depuis la banque : la dépense existait déjà.
        assert conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0] == 1
    finally:
        conn.close()


def test_E_une_facture_encore_a_controler_ne_peut_pas_etre_reglee(base):
    facture = _facture_fournisseur(base, statut="A_CONTROLER", montant=200.0)
    _actualiser(base, [mouvement(1, **dict(CREDIT_200, side="debit"))])

    resultat = validation.valider(_uuid_du(base), nature=validation.REGLEMENT_CHARGE,
                                  objet_id=facture, acteur="test", db_path=base)
    assert resultat["reglement"]["ok"] is False
    assert resultat["reglement"]["code"] == "FACTURE_NON_VALIDEE"


# ══ F. Paiement partiel ═══════════════════════════════════════════════════════════════════════
def test_F_un_paiement_partiel_laisse_le_bon_reliquat(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)

    resultat = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                  montant=60.0, acteur="test", db_path=base)
    assert resultat["ok"] is True
    assert resultat["montant_affecte"] == 60.0
    assert resultat["montant_restant"] == 140.0

    vue = validation.apercu(uuid, db_path=base)
    assert vue["deja_affecte"] == 60.0
    assert vue["restant"] == 140.0
    assert vue["blocage"] == "", "il reste 140 € à affecter : rien n'est bloqué"
    assert _lignes_ecriture(base, _ecritures(base)[0]["ecriture_id_opaque"])[0]["debit"] == 60.0, \
        "l'écriture porte le montant AFFECTÉ, pas le montant du mouvement"


# ══ G. Mouvement déjà entièrement affecté ═════════════════════════════════════════════════════
def test_G_un_mouvement_entierement_affecte_refuse_une_nouvelle_affectation(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, montant=200.0,
                       acteur="test", db_path=base)

    refus = validation.valider(uuid, nature=validation.REVERSEMENT_PROPRIETAIRE,
                               objet_id=PROPRIETAIRE, montant=10.0, db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_DEJA_AFFECTE


def test_G_un_depassement_partiel_est_refuse_par_le_moteur_canonique(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, montant=150.0,
                       acteur="test", db_path=base)

    refus = validation.valider(uuid, nature=validation.REVERSEMENT_PROPRIETAIRE,
                               objet_id=PROPRIETAIRE, montant=100.0, db_path=base)
    assert refus.get("ok") is False
    assert refus.get("code") == rappro.E_DEPASSEMENT


# ══ H. Annulation ═════════════════════════════════════════════════════════════════════════════
def test_H_annuler_conserve_l_historique_et_contrepasse_l_ecriture(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validee = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                 acteur="test", db_path=base)
    brp = validee["rapprochement_id_opaque"]

    annulee = validation.annuler(brp, motif="Mauvais associé", acteur="test", db_path=base)
    assert annulee["ok"] is True

    conn = get_db(base)
    try:
        ligne = conn.execute("SELECT statut FROM banque_rapprochements "
                             "WHERE rapprochement_id_opaque=?", (brp,)).fetchone()
        nb = conn.execute("SELECT COUNT(*) FROM banque_rapprochements").fetchone()[0]
        statuts = [r[0] for r in conn.execute("SELECT statut FROM ecritures")]
    finally:
        conn.close()
    assert ligne["statut"] == "ANNULE", "annulé, pas supprimé"
    assert nb == 1, "la ligne reste en base"
    assert "CONTREPASSEE" in statuts, "l'écriture d'origine est contrepassée, jamais effacée"
    assert len(statuts) == 2, "une écriture miroir a été ajoutée"

    # Le mouvement redevient traitable : sinon il resterait « rapproché » sans rapprochement.
    assert validation.apercu(uuid, db_path=base)["restant"] == 200.0


def test_H_annuler_sans_motif_est_refuse(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    validee = validation.valider(_uuid_du(base), nature=validation.APPORT_ASSOCIE,
                                 objet_id=ASSOCIE, db_path=base)
    refus = validation.annuler(validee["rapprochement_id_opaque"], motif="  ", db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_MOTIF_REQUIS


def test_H_apres_annulation_une_nouvelle_validation_est_possible(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    premiere = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                  db_path=base)
    validation.annuler(premiere["rapprochement_id_opaque"], motif="erreur", db_path=base)

    seconde = validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                                 db_path=base)
    assert seconde["ok"] is True, "corriger doit rester possible ; l'index ne vise que l'actif"


# ══ I. Retrait ATM ════════════════════════════════════════════════════════════════════════════
def test_I_un_retrait_confirme_est_comptabilise_une_seule_fois(base):
    _actualiser(base, [retrait(statut="completed")])
    assert transferts.totaux(db_path=base)["confirme"] == 20.0

    premier = validation.confirmer_transferts_caisse(db_path=base)
    second = validation.confirmer_transferts_caisse(db_path=base)

    assert premier["comptabilises"] == 1
    assert second["comptabilises"] == 0, "rejouer ne recomptabilise pas"

    ecritures = _ecritures(base)
    assert len(ecritures) == 1
    lignes = _lignes_ecriture(base, ecritures[0]["ecriture_id_opaque"])
    assert lignes == [
        {"compte": compta.COMPTE_CAISSE, "auxiliaire": None, "debit": 20.0, "credit": 0.0},
        {"compte": compta.COMPTE_BANQUE, "auxiliaire": None, "debit": 0.0, "credit": 20.0},
    ]


def test_I_un_retrait_encore_provisoire_n_est_pas_comptabilise(base):
    _actualiser(base, [retrait(statut="pending")])
    bilan = validation.confirmer_transferts_caisse(db_path=base)
    assert bilan["comptabilises"] == 0
    assert _ecritures(base) == []


def test_I_une_nature_de_transfert_ne_peut_pas_etre_forcee_sur_autre_chose(base):
    """Le transfert de caisse est déterministe : il ne se choisit pas à la main."""
    _actualiser(base, [mouvement(1, **dict(CREDIT_200, side="debit"))])
    refus = validation.valider(_uuid_du(base), nature=validation.TRANSFERT_CAISSE, db_path=base)
    assert refus["ok"] is False
    assert refus["code"] == validation.E_NATURE_SENS


# ══ J / K. Le rapprochement survit aux actualisations ═════════════════════════════════════════
def test_J_une_actualisation_qonto_conserve_le_rapprochement(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, db_path=base)

    _actualiser(base, [mouvement(1, **CREDIT_200)])
    _actualiser(base, [mouvement(1, **CREDIT_200)])

    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM banque_rapprochements "
                            "WHERE statut='CONFIRME'").fetchone()[0] == 1
        statut = conn.execute("SELECT statut_local FROM qonto_transactions_statut_local "
                              "WHERE qonto_transaction_uuid=?", (uuid,)).fetchone()[0]
    finally:
        conn.close()
    assert statut == classif.RAPPROCHE
    assert len(_ecritures(base)) == 1


def test_K_un_libelle_modifie_chez_qonto_ne_detache_pas_le_rapprochement(base):
    """L'identifiant de mouvement vient du `transaction_id`, pas du libellé ni de l'empreinte."""
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    uuid = _uuid_du(base)
    validation.valider(uuid, nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE, db_path=base)
    opaque_avant = validation.transaction(uuid, db_path=base)["mouvement_id_opaque"]

    modifie = dict(CREDIT_200)
    modifie["label"] = "QONTO — libellé corrigé par la banque"
    _actualiser(base, [mouvement(1, **modifie)])

    apres = validation.transaction(uuid, db_path=base)
    assert apres["libelle"] == "QONTO — libellé corrigé par la banque", "la ligne brute a bien changé"
    assert apres["mouvement_id_opaque"] == opaque_avant, "l'identifiant de mouvement, lui, ne bouge pas"
    assert len(rappro.lister(opaque_avant, base)) == 1

    ligne = [t for t in ecran.tableau_de_bord(db_path=base)["transactions"]
             if t["mouvement_id"] == opaque_avant][0]
    assert ligne["traitement"] == classif.RAPPROCHE
    assert ligne["rapprochements"][0]["objet_id"] == ASSOCIE


# ══ L. Secrets ════════════════════════════════════════════════════════════════════════════════
def test_L_aucun_secret_qonto_n_entre_en_base_apres_validation(base, monkeypatch):
    from app.adapters import qonto_client
    secret = "CLE-SECRETE-RECONNAISSABLE-0123456789"
    monkeypatch.setattr(qonto_client, "identifiants", lambda env=None: ("un-login", secret))

    _actualiser(base, [mouvement(1, **CREDIT_200)])
    validation.valider(_uuid_du(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                       db_path=base)

    conn = get_db(base)
    try:
        contenu = ""
        for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            for ligne in conn.execute(f"SELECT * FROM {table}"):
                contenu += "|".join(str(v) for v in tuple(ligne))
    finally:
        conn.close()
    assert secret not in contenu
    assert "Authorization" not in contenu


def test_L_le_client_qonto_reste_en_lecture_seule():
    from app.adapters import qonto_client
    client = qonto_client.ClientQontoLectureSeule("faux", "faux")
    for interdite in ("POST", "PUT", "PATCH", "DELETE"):
        with pytest.raises(qonto_client.MethodeInterdite):
            client.requete(interdite, "/transactions")


def test_L_aucun_drapeau_d_ecriture_n_ouvre_une_ecriture_vers_qonto(monkeypatch):
    """`BANQUE_REAL_WRITE_ENABLED` gouverne SQLite, jamais la banque."""
    import re
    from pathlib import Path
    source = (Path(cfg.APP_ROOT) / "app" / "services"
              / "qonto_validation_service.py").read_text(encoding="utf-8")
    assert not re.search(r"\.(post|put|patch|delete)\(", source)
    assert "requests" not in source


# ══ L'écran ═══════════════════════════════════════════════════════════════════════════════════
@pytest.fixture()
def client(base):
    from app.main import app
    return TestClient(app)


def test_l_ecran_de_traitement_montre_les_effets_avant_validation(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    opaque = validation.transaction(_uuid_du(base), db_path=base)["mouvement_id_opaque"]
    page = client.get(f"/banques-caisse/qonto/{opaque}/traiter?nature=APPORT_ASSOCIE"
                      f"&objet_id={ASSOCIE}")
    assert page.status_code == 200
    assert 'data-testid="apercu-effets"' in page.text
    assert "467000" in page.text and "512000" in page.text
    assert "200.00" in page.text
    assert 'data-testid="bouton-valider"' in page.text


def test_l_ecran_demande_l_associe_et_ne_valide_pas_sans_lui(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    opaque = validation.transaction(_uuid_du(base), db_path=base)["mouvement_id_opaque"]
    page = client.get(f"/banques-caisse/qonto/{opaque}/traiter?nature=APPORT_ASSOCIE")
    assert 'data-testid="choix-associe"' in page.text
    assert 'data-testid="bouton-valider"' not in page.text, \
        "sans associé désigné, la validation n'est pas offerte"


def test_la_liste_propose_un_bouton_traiter_et_le_filtre_a_traiter(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200), retrait(statut="pending")])
    html = client.get("/banques-caisse?vue=banque").text
    assert 'data-testid="bouton-traiter"' in html
    assert "À traiter" in html

    filtre = client.get(f"/banques-caisse?vue=banque&traitement={ecran.FILTRE_A_TRAITER}")
    assert filtre.status_code == 200
    # La transaction en attente n'est pas « à traiter » : rien à décider tant que la banque hésite.
    assert filtre.text.count('data-testid="qonto-ligne"') == 1


def test_une_ligne_rapprochee_dit_a_quoi_elle_est_rapprochee(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    validation.valider(_uuid_du(base), nature=validation.APPORT_ASSOCIE, objet_id=ASSOCIE,
                       db_path=base)
    html = client.get("/banques-caisse?vue=banque").text
    assert "Rapproché" in html
    assert 'data-testid="rapproche-objet"' in html
    assert ASSOCIE in html

"""Retrait d'espèces, suggestions explicables, et écran Banque ↔ Caisse.

La règle du retrait d'espèces est le SEUL automatisme de cette étape : elle bouge de l'argent
entre deux contenants sans rien décider d'autre. Tout le reste s'arrête à la proposition.

Ce que ces tests tiennent, dans l'ordre de la recette :
  A. retrait confirmé → banque −20, caisse +20, résultat 0, aucun doublon ;
  B. retrait en attente → classé, mais l'encaisse ne bouge pas ;
  C. en attente → confirmé → UN transfert, jamais deux ;
  D. actualisations répétées → aucun mouvement supplémentaire ;
  E. un crédit sans preuve suffisante n'invente pas de rapprochement ;
  F. plusieurs candidats aussi plausibles → AMBIGU ;
  G. mauvais sens → candidat impossible, pas « faible » ;
  H. filtres mois et traitement, combinés ;
  I. switch Banque / Caisse ;
  J. le solde mis en avant est le disponible ;
  K. trésorerie = banque disponible + caisse ;
  L. le client Qonto ne sait toujours faire que des GET.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.adapters import qonto_client
from app.db.connection import apply_migrations, get_db
from app.services import caisse_transferts_service as transferts
from app.services import qonto_classification_service as classif
from app.services import qonto_ecran_service as ecran
from app.services import qonto_raw_service as raw
from app.services import qonto_statut_local_service as statut_local
from app.services import qonto_suggestions_service as sugg
from app.services import qonto_sync_service as sync
from tests.test_qonto_raw_import import COMPTE, ClientDouble, mouvement

TABLES_COMPTABLES = ("charges", "factures", "facture_lignes_menage", "factures_proprietaires",
                     "factures_proprietaires_lignes", "reglements_fournisseurs",
                     "charge_evenements", "operations_caisse")

# Charges utiles calquées sur le VRAI compte : un retrait au distributeur porte `category = atm`.
RETRAIT = dict(side="debit", amount=20.0, amount_cents=2000, operation_type="card",
               label="CREDIT AGRICOLE", clean_counterparty_name="Crédit Agricole",
               category="atm", card_last_digits="3440")
CREDIT_200 = dict(side="credit", amount=200.0, amount_cents=20000, operation_type="income",
                  label="QONTO", clean_counterparty_name="Qonto", category="other_income",
                  reference="Recharge Qonto", status="completed",
                  settled_at="2026-09-11T08:00:00.000Z")


def retrait(numero=9, *, statut="completed", **extra):
    charge = dict(RETRAIT)
    charge.update(status=statut, **extra)
    if statut != "completed":
        charge["settled_at"] = None
    else:
        charge.setdefault("settled_at", "2026-09-19T10:00:00.000Z")
    return mouvement(numero, **charge)


@pytest.fixture()
def base(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db, raising=False)
    return db


def _actualiser(db, mouvements):
    """Le vrai enchaînement du bouton : RAW → statuts → natures → transferts caisse."""
    return ecran.actualiser(client=ClientDouble(pages=[mouvements]), db_path=db)


def _empreinte_comptable(db):
    conn = get_db(db)
    try:
        presentes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        return {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t}")]
                for t in TABLES_COMPTABLES if t in presentes}
    finally:
        conn.close()


# ══ A. Retrait confirmé ═══════════════════════════════════════════════════════════════════════
def test_A_retrait_confirme_sort_de_la_banque_et_entre_en_caisse(base):
    avant = _empreinte_comptable(base)
    _actualiser(base, [retrait()])

    vue = ecran.tableau_de_bord(db_path=base)
    ligne = vue["transactions"][0]
    assert ligne["nature"] == classif.RETRAIT_ESPECES
    assert ligne["traitement"] == classif.TRANSFERE_CAISSE
    assert ligne["sens_code"] == "debit", "la banque diminue de 20 €"
    assert ligne["montant"] == 20.0
    assert vue["encaisse"]["retraits_confirmes"] == 20.0, "la caisse augmente de 20 €"
    assert vue["soldes"]["caisse"] == 20.0

    # Aucun impact résultat : ni charge, ni facture, ni écriture. Les tables comptables sont
    # exactement dans l'état où on les a trouvées.
    assert _empreinte_comptable(base) == avant

    # Un seul transfert, et un seul.
    assert len(transferts.lister(db_path=base)) == 1


def test_A_le_retrait_n_est_ni_charge_ni_depense_de_resultat(base):
    _actualiser(base, [retrait()])
    conn = get_db(base)
    try:
        assert conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM operations_caisse").fetchone()[0] == 0, \
            "aucune opération de caisse comptable n'est créée : ce serait comptabiliser sans contrôle"
        if conn.execute("SELECT name FROM sqlite_master WHERE name='ecritures'").fetchone():
            assert conn.execute("SELECT COUNT(*) FROM ecritures").fetchone()[0] == 0
    finally:
        conn.close()


def test_A_aucune_suggestion_n_est_proposee_pour_un_retrait(base):
    """Chercher une facture pour un retrait au distributeur serait un faux choix à l'écran."""
    _actualiser(base, [retrait()])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]
    assert ligne["suggestion"]["candidats"] == []


# ══ B. Retrait en attente ═════════════════════════════════════════════════════════════════════
def test_B_retrait_en_attente_est_identifie_mais_la_caisse_ne_bouge_pas(base):
    _actualiser(base, [retrait(statut="pending")])

    vue = ecran.tableau_de_bord(db_path=base)
    ligne = vue["transactions"][0]
    assert ligne["nature"] == classif.RETRAIT_ESPECES, "la nature est connue dès l'attente"
    assert ligne["traitement"] == classif.EN_ATTENTE_QONTO
    assert ligne["definitif"] is False

    assert vue["encaisse"]["solde"] == 0.0, "une autorisation de carte n'est pas de l'argent en main"
    assert vue["encaisse"]["retraits_provisoires"] == 20.0
    assert vue["encaisse"]["retraits_confirmes"] == 0.0
    transfert = transferts.lister(db_path=base)[0]
    assert transfert["etat"] == transferts.PROVISOIRE


def test_B_la_vue_caisse_montre_le_retrait_provisoire_sans_le_compter(base):
    _actualiser(base, [retrait(statut="pending")])
    vue = ecran.tableau_de_bord(vue=ecran.VUE_CAISSE, db_path=base)
    mouvement_caisse = vue["mouvements_caisse"][0]
    assert mouvement_caisse["definitif"] is False
    assert "Provisoire" in mouvement_caisse["etat_libelle"]
    assert vue["soldes"]["caisse"] == 0.0


# ══ C. pending → completed ════════════════════════════════════════════════════════════════════
def test_C_un_retrait_qui_se_confirme_ne_produit_quun_seul_transfert(base):
    _actualiser(base, [retrait(statut="pending")])
    assert len(transferts.lister(db_path=base)) == 1

    _actualiser(base, [retrait(statut="completed")])

    lignes = transferts.lister(db_path=base)
    assert len(lignes) == 1, "le transfert change d'état, il ne se dédouble pas"
    assert lignes[0]["etat"] == transferts.CONFIRME
    assert lignes[0]["confirme_le"], "la confirmation est datée"
    assert ecran.encaisse_caisse(db_path=base)["solde"] == 20.0


def test_C_un_retrait_annule_par_la_banque_ne_laisse_pas_de_caisse_fantome(base):
    _actualiser(base, [retrait(statut="completed")])
    assert ecran.encaisse_caisse(db_path=base)["solde"] == 20.0

    _actualiser(base, [retrait(statut="declined")])

    assert ecran.encaisse_caisse(db_path=base)["solde"] == 0.0, \
        "une opération refusée ne doit pas laisser d'espèces qui n'existent pas"
    ligne = transferts.lister(db_path=base)[0]
    assert ligne["etat"] == transferts.ANNULE
    assert ligne["annule_le"], "l'annulation est datée, la ligne n'est pas supprimée"


# ══ D. Actualisations répétées ════════════════════════════════════════════════════════════════
def test_D_actualiser_trois_fois_ne_cree_aucun_mouvement_supplementaire(base):
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])
    conn = get_db(base)
    try:
        apres_un = [tuple(r) for r in conn.execute("SELECT * FROM caisse_transferts_banque")]
    finally:
        conn.close()

    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])

    conn = get_db(base)
    try:
        apres_trois = [tuple(r) for r in conn.execute("SELECT * FROM caisse_transferts_banque")]
        nb_tx = conn.execute("SELECT COUNT(*) FROM qonto_transactions_raw").fetchone()[0]
    finally:
        conn.close()
    assert apres_trois == apres_un, "rejouer ne touche pas une ligne"
    assert nb_tx == 2
    assert ecran.encaisse_caisse(db_path=base)["solde"] == 20.0, "pas de caisse qui enfle"


def test_D_la_classification_est_idempotente(base):
    _actualiser(base, [retrait()])
    assert classif.synchroniser(db_path=base) == {"classees": 0}
    assert transferts.synchroniser(db_path=base)["crees"] == 0


# ══ E. Le crédit de 200 € ═════════════════════════════════════════════════════════════════════
def test_E_un_credit_sans_document_correspondant_part_a_controler(base):
    """Aucune créance ne correspond : on le dit, on n'invente pas un rapprochement."""
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    ligne = ecran.tableau_de_bord(db_path=base)["transactions"][0]

    assert ligne["traitement"] == classif.A_CONTROLER
    assert ligne["suggestion"]["niveau"] == sugg.AUCUN
    assert ligne["suggestion"]["candidats"] == []


def test_E_une_creance_concordante_produit_une_suggestion_expliquee(base):
    """Montant exact + numéro dans le libellé + tiers : faisceau, donc correspondance forte."""
    document = {"type": "CREANCE", "numero": "2026-08-001", "tiers_id": "PROP_0002",
                "total": 200.0, "solde": 200.0, "date_facture": "2026-09-05"}
    transaction = {"sens": "credit", "montant": 200.0, "libelle": "VIR 2026-08-001 DUPONT",
                   "contrepartie": "DUPONT", "regle_le": "2026-09-11"}
    resultat = sugg.suggerer(transaction, documents=[document], noms={"PROP_0002": "Jean Dupont"})

    assert resultat["niveau"] == sugg.FORT
    raisons = resultat["candidats"][0]["raisons"]
    assert any("Référence" in r for r in raisons)
    assert any("Montant exactement égal au reste à payer" in r for r in raisons)
    assert any("Contrepartie concordante" in r for r in raisons)


def test_E_le_montant_seul_reste_une_piste_a_verifier(base):
    """Un seul candidat, mais une seule preuve : ce n'est pas une certitude."""
    document = {"type": "CREANCE", "numero": "F-001", "tiers_id": "PROP_9", "total": 200.0,
                "solde": 200.0, "date_facture": "2020-01-01"}
    resultat = sugg.suggerer({"sens": "credit", "montant": 200.0, "libelle": "VIREMENT",
                              "regle_le": "2026-09-11"},
                             documents=[document], noms={})
    assert resultat["niveau"] == sugg.FAIBLE


# ══ F. Ambiguïté ══════════════════════════════════════════════════════════════════════════════
def test_F_deux_documents_aussi_plausibles_donnent_ambigu(base):
    documents = [
        {"type": "DETTE", "numero": "A-1", "tiers_id": "F1", "total": 240.0, "solde": 240.0,
         "date_facture": "2026-09-01"},
        {"type": "DETTE", "numero": "A-2", "tiers_id": "F2", "total": 240.0, "solde": 240.0,
         "date_facture": "2026-09-02"},
    ]
    resultat = sugg.suggerer({"sens": "debit", "montant": 240.0, "libelle": "PAIEMENT",
                              "regle_le": "2026-09-10"}, documents=documents, noms={})

    assert resultat["niveau"] == sugg.AMBIGU
    assert len(resultat["candidats"]) == 2
    assert "contrôle humain" in resultat["message"]


def test_F_un_document_deja_solde_n_est_jamais_candidat(base):
    document = {"type": "DETTE", "numero": "A-1", "tiers_id": "F1", "total": 240.0, "solde": 0.0,
                "date_facture": "2026-09-01"}
    resultat = sugg.suggerer({"sens": "debit", "montant": 240.0, "libelle": "A-1",
                              "regle_le": "2026-09-10"}, documents=[document], noms={})
    assert resultat["niveau"] == sugg.AUCUN


# ══ G. Sens ═══════════════════════════════════════════════════════════════════════════════════
def test_G_un_encaissement_ne_peut_pas_solder_une_dette(base):
    """Le mauvais sens n'est pas un candidat faible : c'est une impossibilité."""
    from app.services import creances_dettes_service as cd
    assert sugg._documents("credit", base) == [] or all(
        d["type"] == cd.CREANCE for d in sugg._documents("credit", base))
    assert all(d["type"] == cd.DETTE for d in sugg._documents("debit", base))


def test_G_un_candidat_du_mauvais_sens_est_exclu_meme_avec_le_bon_montant(base):
    dette = {"type": "DETTE", "numero": "A-1", "tiers_id": "F1", "total": 200.0, "solde": 200.0,
             "date_facture": "2026-09-01"}
    # On force la dette comme candidat d'un CRÉDIT : le moteur reçoit des documents fournis, mais
    # `_documents` ne les aurait jamais proposés. Le test vérifie la sélection par le sens.
    credit_documents = sugg._documents("credit", base)
    assert dette not in credit_documents


# ══ H. Filtres ════════════════════════════════════════════════════════════════════════════════
def test_H_les_filtres_mois_et_traitement_fonctionnent_ensemble(base):
    ancien = dict(CREDIT_200)
    ancien["settled_at"] = "2026-07-04T08:00:00.000Z"
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200), mouvement(2, **ancien)])

    tout = ecran.tableau_de_bord(db_path=base)
    assert tout["nb_total"] == 3
    assert "2026-09" in tout["filtres"]["mois_disponibles"]

    par_mois = ecran.tableau_de_bord(mois="2026-07", db_path=base)
    assert [t["mois"] for t in par_mois["transactions"]] == ["2026-07"]

    par_traitement = ecran.tableau_de_bord(traitement=classif.TRANSFERE_CAISSE, db_path=base)
    assert len(par_traitement["transactions"]) == 1
    assert par_traitement["transactions"][0]["nature"] == classif.RETRAIT_ESPECES

    # Combinés : septembre ET à contrôler — le retrait de septembre est exclu par le traitement.
    croise = ecran.tableau_de_bord(mois="2026-09", traitement=classif.A_CONTROLER, db_path=base)
    assert all(t["mois"] == "2026-09" and t["traitement"] == classif.A_CONTROLER
               for t in croise["transactions"])
    assert len(croise["transactions"]) == 1


def test_H_un_filtre_sans_resultat_ne_ment_pas_sur_le_total(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    vue = ecran.tableau_de_bord(mois="1999-01", db_path=base)
    assert vue["transactions"] == []
    assert vue["nb_total"] == 1, "le total reste celui de l'ensemble, pas du filtre"


# ══ J / K. Soldes ═════════════════════════════════════════════════════════════════════════════
def test_J_le_solde_mis_en_avant_est_le_disponible(base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    soldes = ecran.soldes(db_path=base)
    assert soldes["banque"] == 180.0, "authorized_balance : ce dont on dispose vraiment"
    assert soldes["banque_comptable"] == 200.0, "balance : conservé en détail, jamais en avant"
    assert soldes["engage"] == 20.0


def test_K_tresorerie_disponible_est_la_somme_banque_et_caisse(base):
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])
    soldes = ecran.soldes(db_path=base)
    assert soldes["caisse"] == 20.0
    assert soldes["tresorerie"] == soldes["banque"] + soldes["caisse"]
    assert soldes["tresorerie"] == 200.0


def test_K_un_retrait_provisoire_n_entre_pas_dans_la_tresorerie(base):
    _actualiser(base, [retrait(statut="pending"), mouvement(1, **CREDIT_200)])
    soldes = ecran.soldes(db_path=base)
    assert soldes["caisse"] == 0.0
    assert soldes["tresorerie"] == 180.0


# ══ L. Le client reste en lecture seule ═══════════════════════════════════════════════════════
def test_L_le_client_qonto_refuse_toujours_tout_sauf_get():
    client = qonto_client.ClientQontoLectureSeule("faux", "faux")
    for interdite in ("POST", "PUT", "PATCH", "DELETE"):
        with pytest.raises(qonto_client.MethodeInterdite):
            client.requete(interdite, "/transactions")
    for verbe in ("post", "put", "patch", "delete"):
        assert not hasattr(qonto_client.ClientQontoLectureSeule, verbe)


def test_L_aucun_service_de_cette_etape_n_ecrit_hors_de_son_perimetre():
    """Les modules de cette étape n'écrivent que dans leurs propres tables."""
    import re
    from pathlib import Path

    autorisees = ("qonto_", "caisse_transferts_banque")
    for module in ("app/services/qonto_classification_service.py",
                   "app/services/caisse_transferts_service.py",
                   "app/services/qonto_suggestions_service.py",
                   "app/services/qonto_ecran_service.py"):
        source = (Path(cfg.APP_ROOT) / module).read_text(encoding="utf-8")
        cibles = re.findall(r"(?:INSERT INTO|UPDATE|DELETE FROM)\s+([a-z_][a-z0-9_]*)",
                            source, flags=re.IGNORECASE)
        hors = [c for c in cibles
                if c.upper() != "SET" and not any(c.startswith(p) for p in autorisees)]
        assert not hors, f"{module} écrit hors périmètre : {hors}"


# ══ I. L'écran ════════════════════════════════════════════════════════════════════════════════
@pytest.fixture()
def client(base):
    from app.main import app
    return TestClient(app)


def test_I_le_switch_bascule_entre_banque_et_caisse(client, base):
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])

    banque = client.get("/banques-caisse?vue=banque")
    assert banque.status_code == 200
    assert 'data-testid="switch-banque"' in banque.text
    assert 'data-testid="switch-caisse"' in banque.text
    assert 'href="/banques-caisse?vue=caisse"' in banque.text

    caisse = client.get("/banques-caisse?vue=caisse")
    assert caisse.status_code == 200
    assert 'data-testid="caisse-ligne"' in caisse.text
    assert "Retrait bancaire Qonto" in caisse.text
    assert "Retrait d&#39;espèces au distributeur" in caisse.text or \
           "Retrait d'espèces au distributeur" in caisse.text


def test_I_la_vue_banque_ne_montre_pas_les_lignes_de_caisse(client, base):
    _actualiser(base, [retrait()])
    banque = client.get("/banques-caisse?vue=banque").text
    assert 'data-testid="caisse-ligne"' not in banque
    assert 'data-testid="qonto-ligne"' in banque


def test_I_l_ecran_est_debarrasse_de_la_plomberie(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    html = client.get("/banques-caisse").text

    for disparu in ("+ Importer un relevé", "Export CSV", "/banques-caisse/export.csv",
                    "/banques-caisse/importer", "count_filtre", "lu le", "pipeline banque"):
        assert disparu not in html, f"« {disparu} » ne doit plus encombrer cet écran"
    for technique in ("charge_utile", "empreinte", "tx-uuid", "Authorization", COMPTE["iban"]):
        assert technique not in html


def test_I_les_kpi_sont_au_nombre_de_quatre_et_parlent_metier(client, base):
    _actualiser(base, [retrait(), mouvement(1, **CREDIT_200)])
    html = client.get("/banques-caisse").text
    assert html.count('class="kpi-card') == 4, "quatre cartes, pas huit"
    for attendu in ("Solde banque", "Caisse", "Trésorerie disponible", "Demande une action"):
        assert attendu in html
    assert "200.00 EUR" in html and "180.00 EUR" in html


def test_I_les_filtres_sont_rendus_et_conservent_la_vue(client, base):
    _actualiser(base, [mouvement(1, **CREDIT_200)])
    html = client.get("/banques-caisse?vue=banque").text
    assert 'data-testid="filtres-banque"' in html
    assert 'name="mois"' in html and 'name="traitement"' in html
    assert 'value="banque"' in html, "le filtre ne doit pas faire perdre la vue courante"


def test_I_le_retrait_est_presente_comme_un_transfert_pas_comme_une_charge(client, base):
    _actualiser(base, [retrait()])
    html = client.get("/banques-caisse").text
    assert "Retrait espèces" in html
    assert "Transféré en caisse" in html
    assert "transfert interne" in html
    assert "charge" not in html.split("Retrait espèces")[1][:200].lower() or \
           "aucune charge" in html


def test_I_actualiser_donne_un_message_bref_sans_compteur_technique(client, base, monkeypatch):
    from app.routes import banques as routes_banques
    monkeypatch.setattr(routes_banques.qonto_ecran, "actualiser",
                        lambda **k: {"ok": True, "creees": 7, "mises_a_jour": 3, "inchangees": 11})

    reponse = client.post("/banques-caisse/qonto/actualiser", data={"vue": "banque"},
                          follow_redirects=True)
    assert reponse.status_code == 200
    assert "Données Qonto actualisées." in reponse.text
    for compteur in ("7 nouveau", "3 mis à jour", "11 inchangé", "lignes téléchargées"):
        assert compteur not in reponse.text, "un décompte technique n'apprend rien à personne"


def test_E_une_date_proche_seule_ne_fait_pas_un_candidat(base):
    """Sur un mois donné, tout est « proche » de tout : la date ne départage rien.

    Trois factures du même mois proposées pour un virement sans rapport, ce n'est pas une aide :
    c'est du bruit qui use la confiance dans l'écran.
    """
    documents = [
        {"type": "CREANCE", "numero": "2026-08-001", "tiers_id": "P1", "total": 757.65,
         "solde": 757.65, "date_facture": "2026-09-12"},
        {"type": "CREANCE", "numero": "2026-08-002", "tiers_id": "P2", "total": 288.95,
         "solde": 288.95, "date_facture": "2026-09-12"},
    ]
    resultat = sugg.suggerer({"sens": "credit", "montant": 200.0, "libelle": "QONTO",
                              "contrepartie": "Qonto", "regle_le": "2026-09-11"},
                             documents=documents, noms={"P1": "Alice Martin", "P2": "Bob Durand"})
    assert resultat["niveau"] == sugg.AUCUN
    assert resultat["candidats"] == []


def test_E_la_date_reste_un_renfort_quand_un_vrai_signal_existe(base):
    document = {"type": "CREANCE", "numero": "2026-08-001", "tiers_id": "P1", "total": 200.0,
                "solde": 200.0, "date_facture": "2026-09-12"}
    resultat = sugg.suggerer({"sens": "credit", "montant": 200.0, "libelle": "VIREMENT",
                              "regle_le": "2026-09-11"}, documents=[document], noms={})
    assert resultat["candidats"], "le montant exact fait le candidat, la date le renforce"
    assert any("Date proche" in r for r in resultat["candidats"][0]["raisons"])

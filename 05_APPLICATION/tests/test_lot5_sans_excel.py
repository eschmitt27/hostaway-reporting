"""Lot 5 sans Excel ni Power Query : saisie → validation → compte propriétaire → consultation.

CE QUE CE FICHIER PROUVE
Le parcours complet d'un acompte propriétaire se fait dans l'application, sur SQLite, avec les deux
classeurs Lot 5 rendus introuvables :
  · `SAISIE_AcomptesProprietaires.xlsx`             (saisie + 5 requêtes Power Query)
  · `MASTER_FACT_MAN_AcomptesProprietaires.xlsx`    (sortie figée)

Les chemins de configuration sont pointés sur des fichiers inexistants — plus fiable que de supposer
qu'ils sont absents de la machine, et sans rien déplacer sur le disque.

LE SCÉNARIO FIFO
Zéro facture, un acompte de 100 € reçu, puis une facture de 80 € : la facture doit être soldée
automatiquement et 20 € rester en crédit. C'est le comportement que Power Query ne savait pas
produire — il figeait un montant, il ne suivait pas un compte.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.readers import proprietaires_extras_reader as extras
from app.services import compte_proprietaire_service as compte
from app.services import proprietaires_tresorerie_service as tresorerie
from app.services import tresorerie_controles_service as ctrl

PROP = "PROP_9001"
LOG = "LOG_9001"


@pytest.fixture(autouse=True)
def sans_excel_lot5(tmp_db, tmp_path, monkeypatch):
    """Aucun classeur Lot 5 atteignable."""
    monkeypatch.setattr(cfg, "SAISIE_ACOMPTES_PROPRIETAIRES",
                        tmp_path / "AUCUN" / "SAISIE_AcomptesProprietaires.xlsx")
    extras.vider_cache()
    yield
    extras.vider_cache()


@pytest.fixture
def proprietaire(tmp_db):
    """Propriétaire connu du référentiel, sinon la saisie le refuse — à juste titre."""
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, email, telephone, adresse_facturation, mode_facturation, actif, "
            "commentaire, import_id) VALUES (?,'DEMO','','','','','PAR_LOGEMENT','OUI','',"
            "'IMP-TEST')", (PROP,))
        conn.execute(
            "INSERT OR IGNORE INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-TEST','2026-01-01T00:00:00','fixture','x','IMPORTE',28,1)")
        conn.commit()
    finally:
        conn.close()
    return tmp_db


def _saisir_acompte(db, montant: float, *, valider: bool = True) -> str:
    """Saisie applicative d'un acompte, exactement comme l'écran la fait."""
    res = tresorerie.creer(PROP, "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE", montant,
                           "2026-04-10", logement_id=LOG,
                           reference_metier="FAC-2026-04-PROP-001",
                           acteur="recette", db_path=db)
    assert res["ok"], res
    mid = res["mouvement_opaque"]
    if valider:
        assert tresorerie.valider(mid, acteur="recette", db_path=db)["ok"]
    return mid


FACTURE_ID = "FPR-2026-04-001"


def _facture(db, montant: float, fid: str = FACTURE_ID) -> None:
    """Une créance propriétaire ÉMISE, telle que le compte propriétaire la lit.

    Seul `EMIS` constitue une créance : un brouillon n'engage personne. Le statut compte donc, et
    l'écrire en dur ici évite de faire dépendre le test du parcours de facturation.
    """
    from app.db.connection import get_db

    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, numero_facture, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, date_emission) "
            "VALUES (?,?,'FACTURE',?,?,'2026-04',?, 'EMIS','2026-04-15')",
            (fid, fid, PROP, LOG, montant))
        conn.commit()
    finally:
        conn.close()


# ── Les classeurs sont bien introuvables ────────────────────────────────────────────────────────

def test_les_classeurs_lot5_sont_introuvables():
    assert not Path(cfg.SAISIE_ACOMPTES_PROPRIETAIRES).exists()


def test_le_master_lot5_nest_lu_par_aucun_service():
    """Aucun service applicatif ne référence le MASTER Lot 5 comme source de lecture."""
    import subprocess

    racine = Path(cfg.APP_ROOT)
    resultat = subprocess.run(
        ["git", "grep", "-l", "MASTER_FACT_MAN_AcomptesProprietaires",
         "--", "app/services", "app/readers", "app/routes"],
        cwd=racine, capture_output=True, text=True)
    fichiers = [f for f in resultat.stdout.split() if f]
    # Les seules mentions tolérées sont documentaires : le service de contrôles explique ce qu'il
    # remplace, et les runners nomment le fichier comme ENTRÉE d'un moteur legacy, pas comme source.
    attendus = {"app/services/tresorerie_controles_service.py",
                "app/services/controles_runner_service.py",
                "app/services/menages_chaine_service.py"}
    assert set(fichiers) <= attendus, f"lecture inattendue du MASTER Lot 5 : {fichiers}"


# ── Saisie et validation depuis l'application ───────────────────────────────────────────────────

def test_saisie_puis_validation(proprietaire):
    mid = _saisir_acompte(proprietaire, 100.0)
    m = tresorerie.charger(mid, db_path=proprietaire)
    assert m["statut"] == tresorerie.ST_VALIDE
    assert m["montant"] == 100.0
    assert m["nature"] == "ACOMPTE_PROPRIETAIRE"


def test_un_brouillon_nest_pas_encore_un_acompte_recu(proprietaire):
    """Le compter fausserait le solde du propriétaire."""
    _saisir_acompte(proprietaire, 100.0, valider=False)
    assert extras.acomptes().lignes == []


def test_lecture_des_acomptes_depuis_la_base(proprietaire):
    _saisir_acompte(proprietaire, 100.0)
    extras.vider_cache()
    src = extras.acomptes()
    assert src.etat.disponible and src.etat.nb_lignes == 1
    ligne = src.lignes[0]
    assert ligne["proprietaire_id"] == PROP
    assert ligne["montant_acompte"] == 100.0
    assert ligne["mois"] == "2026-04"
    assert "xlsx" not in src.etat.fichier.lower()


def test_filtre_par_proprietaire_et_mois(proprietaire):
    _saisir_acompte(proprietaire, 100.0)
    extras.vider_cache()
    assert len(extras.acomptes_prop_mois(PROP, "2026-04")) == 1
    assert extras.acomptes_prop_mois(PROP, "2026-05") == []


# ── Historisation : rien n'est réécrit en silence ───────────────────────────────────────────────

def test_un_mouvement_valide_ne_se_supprime_pas(proprietaire):
    mid = _saisir_acompte(proprietaire, 100.0)
    res = tresorerie.supprimer(mid, db_path=proprietaire)
    assert res["ok"] is False and res["code"] == tresorerie.E_SUPPRESSION_INTERDITE


def test_la_correction_passe_par_une_annulation_tracee(proprietaire):
    mid = _saisir_acompte(proprietaire, 100.0)
    assert tresorerie.annuler(mid, commentaire="saisie erronée", acteur="recette",
                              db_path=proprietaire)["ok"]
    assert tresorerie.charger(mid, db_path=proprietaire)["statut"] == tresorerie.ST_ANNULE

    from app.db.connection import get_db
    conn = get_db(proprietaire)
    try:
        types = [r[0] for r in conn.execute(
            "SELECT type_evenement FROM mouvements_tresorerie_proprietaires_evenements "
            "WHERE mouvement_id = ? ORDER BY id", (mid,))]
    finally:
        conn.close()
    assert types == ["CREATION", "VALIDATION", "ANNULATION"], "l'historique conserve chaque étape"


def test_un_mouvement_annule_ne_compte_plus(proprietaire):
    mid = _saisir_acompte(proprietaire, 100.0)
    tresorerie.annuler(mid, commentaire="erreur", acteur="recette", db_path=proprietaire)
    extras.vider_cache()
    assert extras.acomptes().lignes == []


# ── §19 — compte propriétaire FIFO ──────────────────────────────────────────────────────────────

def test_acompte_sans_facture_devient_un_credit(proprietaire):
    _saisir_acompte(proprietaire, 100.0)
    compte.recalculer(PROP, declencheur="TEST", db_path=proprietaire)
    pos = compte.position(PROP, db_path=proprietaire)
    assert pos["credit_disponible"] == 100.0
    assert pos["factures_a_recevoir"] == 0.0
    assert pos["creance_restante"] == 0.0


def test_une_facture_ulterieure_est_soldee_par_le_credit(proprietaire):
    """0 facture, acompte 100 €, puis facture 80 € : facture soldée, crédit restant 20 €."""
    _saisir_acompte(proprietaire, 100.0)
    _facture(proprietaire, 80.0)
    compte.recalculer(PROP, declencheur="TEST", db_path=proprietaire)

    pos = compte.position(PROP, db_path=proprietaire)
    assert pos["credit_disponible"] == 20.0, "le solde du crédit doit être 100 − 80"
    assert compte.imputations_facture(FACTURE_ID, db_path=proprietaire) == 80.0
    assert pos["creance_restante"] == 0.0, "la facture est soldée"
    assert pos["factures"][0]["statut_reglement"] == "REGLEE"


def test_le_recalcul_est_idempotent(proprietaire):
    _saisir_acompte(proprietaire, 100.0)
    _facture(proprietaire, 80.0)
    compte.recalculer(PROP, declencheur="TEST", db_path=proprietaire)
    avant = compte.position(PROP, db_path=proprietaire)
    compte.recalculer(PROP, declencheur="TEST", db_path=proprietaire)
    assert compte.position(PROP, db_path=proprietaire)["credit_disponible"] == \
        avant["credit_disponible"]


# ── Contrôles : ils tournent sans Excel ─────────────────────────────────────────────────────────

def test_les_dix_controles_tournent_sans_power_query(proprietaire):
    _saisir_acompte(proprietaire, 100.0)
    resultat = ctrl.controler(db_path=proprietaire)
    assert resultat["nb_mouvements"] == 1
    assert resultat["nb_constats"] == 0, "un acompte complet ne déclenche aucun constat"
    assert len(ctrl.codes_connus()) == 10


# ── Les contrôles sont VISIBLES, sinon ils ne servent à rien ────────────────────────────────────

def test_ecran_tresorerie_affiche_les_constats(proprietaire, client):
    """Un mouvement sans référence ni logement doit produire des constats à l'écran."""
    from app.db.connection import get_db

    conn = get_db(proprietaire)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "date_mouvement, montant, sens, nature, statut) "
            "VALUES ('MTP-UI-0001', ?, '2026-04-10', 100.0, 'PROPRIETAIRE_VERS_SOCIETE', "
            "'ACOMPTE_PROPRIETAIRE', 'VALIDE')", (PROP,))
        conn.commit()
    finally:
        conn.close()

    r = client.get("/proprietaires/tresorerie")
    assert r.status_code == 200
    assert "controles-tresorerie" in r.text
    assert ctrl.C_REFERENCE_ABSENTE in r.text
    assert ctrl.C_LOGEMENT_ABSENT in r.text


def test_ecran_tresorerie_annonce_labsence_de_constat(proprietaire, client):
    _saisir_acompte(proprietaire, 100.0)
    r = client.get("/proprietaires/tresorerie")
    assert r.status_code == 200
    assert "Aucun constat" in r.text


def test_les_constats_portent_sur_tout_le_jeu_pas_sur_la_page(proprietaire, client):
    """Un constat en page 3 doit être visible dès la page 1 : on décide sur l'ensemble."""
    from app.db.connection import get_db

    conn = get_db(proprietaire)
    try:
        for i in range(60):
            conn.execute(
                "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, "
                "proprietaire_id, logement_id, date_mouvement, montant, sens, nature, statut, "
                "reference_metier) VALUES (?, ?, ?, '2026-04-10', ?, "
                "'PROPRIETAIRE_VERS_SOCIETE', 'ACOMPTE_PROPRIETAIRE', 'VALIDE', ?)",
                (f"MTP-PAGE-{i:03d}", PROP, LOG, 10.0 + i, f"FAC-2026-04-PROP-{i:03d}"))
        # Le dernier, volontairement sans référence : il ne peut pas être sur la première page.
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "logement_id, date_mouvement, montant, sens, nature, statut, reference_metier) "
            "VALUES ('MTP-CACHE-0001', ?, ?, '2026-04-10', 999.0, "
            "'PROPRIETAIRE_VERS_SOCIETE', 'ACOMPTE_PROPRIETAIRE', 'VALIDE', '')", (PROP, LOG))
        conn.commit()
    finally:
        conn.close()

    r = client.get("/proprietaires/tresorerie")
    assert "MTP-CACHE-0001" in r.text, "le constat doit apparaître même hors de la page affichée"

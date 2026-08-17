"""APP-5B — Validation POSITIVE du runner sur mini-projet 100% FACTICE.

Prouve que le chemin SÛR fonctionne réellement : Lot8c puis Lot11 (scripts du worktree, injectés) sont
exécutés sur un mini-projet synthétique, le contrôle bancaire est présent avant / absent après
classification, et AUCUN fichier réel n'est touché. Aucune donnée réelle en entrée ; le code moteur
appelé est bien celui du worktree (jamais une fonction factice qui le remplace).

La Banque du mini-projet est en BASE, plus dans un classeur : le runner fabrique lui-même le classeur
que les moteurs attendent. Ce test vérifie donc aussi que cette fabrication est correcte — un moteur
qui ne verrait pas le mouvement conclurait « aucune anomalie », ce qui passerait inaperçu.
"""
import hashlib
import shutil
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.services import controles_runner_service as runner
from app.services import banques_controle_service as bq

REEL = Path(r"C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie")
engine_requis = pytest.mark.skipif(runner._engine_python() is None, reason="aucun python avec pandas")
scripts_requis = pytest.mark.skipif(
    not (Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "lot11_controles_coherence.py").exists(),
    reason="scripts moteur worktree absents")

MOIS, MOIS2 = "2099-01", "2099-02"
_NORM = ["mouvement_id","ROW_HASH","import_id","ligne_source","date_operation","date_valeur","libelle",
 "libelle_brut","montant","sens","devise","compte_id","tiers_detecte","categorie","type_flux_id",
 "code_impact","source_classification","source_economique","statut_controle","niveau_risque",
 "codes_anomalie","date_integration","commentaire","statut_classification","niveau_anomalie","regle_id_appliquee"]


def _w(path, sheets):
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        ws = wb.create_sheet(name); ws.append(cols)
        for r in rows:
            ws.append([r.get(c) for c in cols])
    wb.save(str(path)); wb.close()


def _mvt(mid, montant, sens, sc, mois, cat=""):
    return {c: None for c in _NORM} | {
        "mouvement_id": mid, "date_operation": mois+"-15", "libelle": "TEST "+mid, "libelle_brut": "TEST",
        "montant": montant, "sens": sens, "devise": "EUR", "compte_id": "COMPTE_TEST_001", "categorie": cat,
        "statut_controle": "A_CONTROLER" if sc == "RAPPROCHEMENT_REQUIS" else "VALIDE",
        "statut_classification": sc, "niveau_anomalie": "INFO"}


def _mini_projet(root: Path):
    """Génère un mini-projet synthétique (aucune donnée réelle) suffisant pour Lot8c + Lot11."""
    E = [{}]  # ligne all-None : préserve les colonnes sans déclencher de logique
    # Aucun classeur bancaire : le runner le fabrique depuis SQLite (voir la fixture `banque_sqlite`).
    _w(root/"02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx", {"MASTER":(
        ["flux_id","mois","logement_id","proprietaire_id","source_pk","type_flux_id","code_impact",
         "sens","source_table"], E)})
    _w(root/"02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx", {"MASTER":(
        ["reservation_calc_id","reservation_id_hostaway","reservation_hh_id","source","mois","logement_id",
         "proprietaire_id","date_arrivee","date_depart","nuits","montant_retenu","source_montant",
         "statut_controle","niveau_anomalie","code_anomalie","source_pk"], E)})
    _w(root/"02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx", {"data":(["reservation_id","payout","mois"], E)})
    _w(root/"02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx", {
        "COMMISSIONS":(["reservation_id","logement_id","proprietaire_id","mois","payout_calcule","commission",
            "controle_taux_commission","assiette_commission","commission_conciergerie","taux_commission",
            "menage_retenu"], E),
        "A_CONTROLER":(["reservation_id","listingMapId","source","channel_type","statut_calcul_payout",
            "payout_calcule","source_payout","menage_retenu","assiette_commission","logement_id_snapshot",
            "code_anomalie_lot10"], E)})
    _w(root/"02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx", {
        "EXPLOITATION":(["proprietaire_id","mois","net","charge_fixe_mensuelle","commission_conciergerie",
            "menage_retenu","payout_calcule","preparation_canape_voyageurs","revenu_net_exploitation",
            "logement_id"], E),
        "REGLEMENT":(["proprietaire_id","mois","montant","charge_fixe_mensuelle",
            "acompte_conciergerie_recu_via_airbnb","autres_acomptes_recus","montant_du_conciergerie",
            "paiement_deja_recu","reste_a_payer_conciergerie","total_payout_mois","logement_id"], E)})
    _w(root/"02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", {
        "GLOBAL":(["indicateur","valeur"], E), "PAR_MOIS_LOGEMENT":(["mois","logement_id","resultat"], E)})
    _w(root/"02_TRAVAIL/Lot1_Hostaway/MASTER_CTRL_HA_Anomalies.xlsx", {"ANOMALIES":(
        ["reservation_id","code_anomalie","description"], E)})
    _w(root/"02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx", {"MASTER":(
        ["charge_id","mois","logement_id","proprietaire_id","mode_paiement_id","avantage_associe_id"], E)})
    _w(root/"02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx", {"MASTER":(
        ["reservation_hh_id","mois","logement_id","mode_paiement_id","reservation_id_hostaway"], E)})
    _w(root/"02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx", {"MASTER":(
        ["acompte_id","mois","proprietaire_id"], E)})
    _w(root/"02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx", {
        "MASTER":(["menage_ext_id","mois","logement_id","prestataire","montant"], E),
        "VUE_ECART_HOSTAWAY":(["mois","logement_id","proprietaire_id","prestataires_factures",
            "nombre_menages_facture","nombre_menages_hostaway","ecart","code_controle","niveau_controle",
            "commentaire_controle","date_actualisation"], E)})
    _w(root/"02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx", {"MASTER":(
        ["menage_calc_id","mois","logement_id"], E)})
    _w(root/"02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx", {"MASTER_CALC_AVANTAGES":(
        ["pk_id","mois","proprietaire_id","code_impact","cle_suivi","avantage_net"], E)})
    _w(root/"01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm", {
        "REF_Logements":(["logement_id","nom_officiel","hostaway_listing_id","statut_parc"],
            [{"logement_id":"LOG_TEST_001","nom_officiel":"Logement Test","statut_parc":"GERE"}]),
        "REF_Proprietaires":(["proprietaire_id","nom","mode_facturation","taux_commission","actif"],
            [{"proprietaire_id":"PROP_TEST_001","nom":"Proprio Test","mode_facturation":"STANDARD","actif":"OUI"}]),
        "REF_Mapping_Logements":(["mapping_logement_id","logement_id","actif"],
            [{"mapping_logement_id":"MAP_TEST_001","logement_id":"LOG_TEST_001","actif":"OUI"}]),
        "REF_Taux_Commission":(["taux_commission_id","taux","date_debut"], E),
        "REF_GESTION_LOGEMENTS_HIST":(["gestion_id","logement_id","proprietaire_id","date_debut_gestion",
            "date_fin_gestion"], [{"gestion_id":"GEST_TEST_001","logement_id":"LOG_TEST_001",
            "proprietaire_id":"PROP_TEST_001","date_debut_gestion":"2099-01-01"}]),
        "REF_Cloture_Mensuelle":(["mois","statut_mois"], [{"mois":MOIS,"statut_mois":"OUVERT"},
            {"mois":MOIS2,"statut_mois":"OUVERT"}])})
    (root/"02_TRAVAIL/Lot11_Controles").mkdir(parents=True, exist_ok=True)
    (root/"99_ARCHIVES/LOT8_Banque").mkdir(parents=True, exist_ok=True)


def _sha(p):
    p = Path(p)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else "ABSENT"


def _banque_sqlite(db_path):
    """Les trois mêmes mouvements qu'auparavant, en base au lieu d'un classeur.

    MVT-TEST-A porte le contrôle qu'on cherche à faire disparaître ; MVT-TEST-B est déjà classé ;
    MVT-TEST-C est sur un autre mois, et doit donc rester intact quoi qu'il arrive au premier.
    """
    import fixtures_banque as fx
    from app.services import banque_classification_service as cls
    from app.db.connection import get_db

    fx.construire(db_path, mouvements=[
        fx.mouvement("MVT-TEST-A", MOIS + "-15", "TEST MVT-TEST-A", 100.0, "CREDIT",
                     statut_controle="A_CONTROLER",
                     statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
        fx.mouvement("MVT-TEST-B", MOIS + "-15", "TEST MVT-TEST-B", 5.0, "DEBIT",
                     categorie="FRAIS_BANCAIRES"),
        fx.mouvement("MVT-TEST-C", MOIS2 + "-15", "TEST MVT-TEST-C", 33.0, "DEBIT",
                     statut_controle="A_CONTROLER",
                     statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
    ])
    # Statut de clôture : lot11 le lit dans le classeur bancaire, l'adaptateur le prend en base.
    conn = get_db(db_path)
    try:
        for mois in (MOIS, MOIS2):
            conn.execute(
                "INSERT OR REPLACE INTO ref_cloture_mensuelle (mois, statut_mois, "
                "date_passage_controle, date_cloture, nb_lignes_bancaires_non_classees, "
                "nb_controles_bloquants_ouverts, commentaire, import_id) "
                "VALUES (?, 'OUVERT', '', '', '', '', '', 'IMP-TEST')", (mois,))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def mini(tmp_path, tmp_db, monkeypatch):
    """Mini-projet factice + cfg monkeypatché dessus ; workspace runner HORS du mini-projet."""
    root = tmp_path / "mini_projet"
    _mini_projet(root)
    _banque_sqlite(tmp_db)
    ws = tmp_path / "runner_ws"    # hors mini-projet (respecte la garde d'isolation)
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE", root/"02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx")
    monkeypatch.setattr(cfg, "CONTROLES_RUNNER_WORKSPACE", ws)
    from app.readers import banques_reader as reader
    reader.vider_cache()
    bq.vider_cache()
    return root


def _element():
    opq = bq.id_opaque("MVT-TEST-A")
    return {"ctrl_opaque": "CTRL-TESTBANK", "module": "BANQUE",
            "code": "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE", "mois": MOIS, "entite_id": opq}


# ── 1,4-13 : chemin sûr fonctionnel ──────────────────────────────────────────

@scripts_requis
@engine_requis
def test_01_04_09_classification_resout_le_controle(mini, tmp_db):
    reels = {"b": _sha(REEL/"02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx"),
             "m": _sha(REEL/"02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx")}
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    etapes = {e["etape"]: e for e in res["etapes"]}
    # 1 workspace sûr accepté (pas de BLOQUE) ; 4 Lot8c exécuté ; 5 Lot11 exécuté ; 6 rc=0
    assert res["statut"] == "SUCCES"
    assert etapes["LOT8C"]["rc"] == 0 and etapes["LOT11"]["rc"] == 0 and etapes["LOT11_BASELINE"]["rc"] == 0
    # 7 contrôle présent avant ; 8 absent après ; 9 verdict correct
    assert res["n_avant"] == 1 and res["n_apres"] == 0 and res["verdict"] == "RESOLU_MOTEUR"
    # 11/12 aucun accès réel : hashes réels inchangés
    assert _sha(REEL/"02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx") == reels["b"]
    assert _sha(REEL/"02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx") == reels["m"]


@scripts_requis
@engine_requis
def test_07_08_sans_classification_controle_maintenu(mini, tmp_db):
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=False, db_path=tmp_db)
    assert res["verdict"] == "TOUJOURS_PRESENT" and res["n_avant"] == 1 and res["n_apres"] == 1


@scripts_requis
@engine_requis
def test_10_11_toutes_sorties_dans_workspace_aucun_chemin_reel(mini, tmp_db):
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    reel = str(REEL).lower()
    # aucun chemin résolu sous le réel / OneDrive
    hors = [c for c in res["chemins"] if reel in c.lower() or "onedrive" in c.lower()]
    assert hors == [] and len(res["chemins"]) > 0
    # toutes les copies (entrées + sorties) sont confinées au workspace du runner
    ws = str(cfg.CONTROLES_RUNNER_WORKSPACE).lower()
    assert all(ws in c.lower() for c in res["chemins"])


@scripts_requis
@engine_requis
def test_13_workspace_nettoye(mini, tmp_db):
    runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    ws_root = Path(cfg.CONTROLES_RUNNER_WORKSPACE)
    restes = [p for p in ws_root.glob("*")] if ws_root.exists() else []
    assert restes == []                                 # workspace nettoyé (aucun résidu)


# ── 14,16 : échec moteur lisible, aucun 500 ──────────────────────────────────

@scripts_requis
@engine_requis
def test_14_16_echec_moteur_lisible(mini, tmp_db):
    # corrompre une entrée requise -> Lot11 échoue -> verdict ERREUR, jamais d'exception
    (Path(cfg.PROJECT_ROOT)/"02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx").unlink()
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    assert res["statut"] in ("ECHEC", "BLOQUE") and res["verdict"] in ("ERREUR_MOTEUR", "NON_COMPARABLE")
    assert "motif" in res                                # message lisible, pas de 500


def test_15_timeout_gere_dans_le_code():
    src = Path(runner.__file__).read_text(encoding="utf-8")
    assert "TimeoutExpired" in src and "TIMEOUT_MOTEUR_S" in src


# ── 2,3 : garde d'isolation (workspace/scripts dans le réel) ─────────────────

def test_02_03_workspace_ou_scripts_dans_reel_refuses(mini, monkeypatch, tmp_db):
    # workspace SOUS le mini-projet (= PROJECT_ROOT) -> refusé par la garde
    monkeypatch.setattr(cfg, "CONTROLES_RUNNER_WORKSPACE", Path(cfg.PROJECT_ROOT)/"interne_ws")
    res = runner.recalculer_sur_copie(_element(), appliquer_classification=True, db_path=tmp_db)
    assert res["statut"] == "BLOQUE" and "WORKSPACE" in (res.get("motif","").upper() + "WORKSPACE")
    assert res["reel_intact"] is True

"""APP-3C — Propriétaires & règlements : lecture, séparation CA/commission/net, masquage, garde-fous.

Fixtures Excel ISOLÉES. Aucun recalcul de commission, aucune écriture, aucune source réelle modifiée,
aucune adresse exposée, vraie app.db jamais touchée.
"""
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.readers import proprietaires_reglements_reader as reader
from app.services import proprietaires_reglements_service as svc


def _sheet(ws, cols, rows):
    ws.append(cols)
    for r in rows:
        ws.append([r.get(c) for c in cols])


def _wb(path, sheets):
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        _sheet(wb.create_sheet(name), cols, rows)
    wb.save(str(path)); wb.close()


VUE_COLS = ["mois", "proprietaire_id", "total_payout_mois", "total_menage_mois", "total_commission_mois",
            "charge_fixe_mensuelle", "montant_du_conciergerie", "reste_a_payer_conciergerie",
            "net_proprietaire_avant_charge_mois", "net_proprietaire_apres_charge_mois", "nb_reservations"]
REG_COLS = ["mois", "logement_id", "proprietaire_id", "charge_fixe_mensuelle", "total_payout_mois",
            "total_menage_mois", "total_commission_mois", "net_proprietaire_avant_charge_mois",
            "nb_reservations", "montant_du_conciergerie", "acompte_conciergerie_recu_via_airbnb",
            "autres_acomptes_recus", "paiement_deja_recu", "reste_a_payer_conciergerie",
            "net_proprietaire_apres_charge_mois", "statut_reglement"]
COMM_COLS = ["proprietaire_id", "logement_id", "mois", "assiette_commission", "taux_commission",
             "commission_conciergerie", "net_proprietaire"]
FACT_COLS = ["facture_id", "mois", "proprietaire_id", "nom_proprietaire", "adresse_proprietaire",
             "logement_id", "statut_facture", "statut_generation", "total_exploitation_net",
             "total_reglement_du", "reste_a_payer", "mode_facturation"]
DASH_COLS = ["mois", "proprietaire_id", "nb_logements", "nb_bloquants_mois", "nb_a_controler_mois",
             "facturation_lot12_ok", "mode_facturation", "statut_facture", "balises_non_resolues"]
CTRL_COLS = ["mois", "proprietaire_id", "code_controle", "severite", "description"]


@pytest.fixture
def owners_files(tmp_path, monkeypatch):
    net = tmp_path / "NET.xlsx"; comm = tmp_path / "COMM.xlsx"; fact = tmp_path / "FACT.xlsx"
    res = tmp_path / "RES.xlsx"
    vue = [
        {"mois": "2026-01", "proprietaire_id": "PROP_A", "total_payout_mois": 2000, "total_menage_mois": 150,
         "total_commission_mois": 300, "charge_fixe_mensuelle": 0, "montant_du_conciergerie": 300,
         "reste_a_payer_conciergerie": 0, "net_proprietaire_avant_charge_mois": 1700,
         "net_proprietaire_apres_charge_mois": 1700, "nb_reservations": 4},
        {"mois": "2026-02", "proprietaire_id": "PROP_B", "total_payout_mois": 1000, "total_menage_mois": 80,
         "total_commission_mois": 120, "charge_fixe_mensuelle": 20, "montant_du_conciergerie": 140,
         "reste_a_payer_conciergerie": 140, "net_proprietaire_avant_charge_mois": 880,
         "net_proprietaire_apres_charge_mois": 860, "nb_reservations": 2},
    ]
    reg = [
        {"mois": "2026-01", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A", "total_payout_mois": 1200,
         "total_commission_mois": 180, "net_proprietaire_avant_charge_mois": 1020,
         "acompte_conciergerie_recu_via_airbnb": 180, "autres_acomptes_recus": 0, "paiement_deja_recu": 180,
         "reste_a_payer_conciergerie": 0, "statut_reglement": "REGLE"},
        {"mois": "2026-01", "logement_id": "LOG_A2", "proprietaire_id": "PROP_A", "total_payout_mois": 800,
         "total_commission_mois": 120, "net_proprietaire_avant_charge_mois": 680,
         "acompte_conciergerie_recu_via_airbnb": 120, "autres_acomptes_recus": 0, "paiement_deja_recu": 120,
         "reste_a_payer_conciergerie": 0, "statut_reglement": "REGLE"},
        {"mois": "2026-02", "logement_id": "LOG_B1", "proprietaire_id": "PROP_B", "total_payout_mois": 1000,
         "total_commission_mois": 120, "net_proprietaire_avant_charge_mois": 880,
         "acompte_conciergerie_recu_via_airbnb": 0, "autres_acomptes_recus": 0, "paiement_deja_recu": 0,
         "reste_a_payer_conciergerie": 140, "statut_reglement": "PARTIEL"},
    ]
    comm_rows = [
        {"proprietaire_id": "PROP_A", "logement_id": "LOG_A1", "mois": "2026-01", "assiette_commission": 1200,
         "taux_commission": 0.15, "commission_conciergerie": 180, "net_proprietaire": 1020},
        {"proprietaire_id": "PROP_B", "logement_id": "LOG_B1", "mois": "2026-02", "assiette_commission": 1000,
         "taux_commission": 0.12, "commission_conciergerie": 120, "net_proprietaire": 880},  # taux propre >= 02/2026
    ]
    fact_rows = [
        {"facture_id": "F-A-2601", "mois": "2026-01", "proprietaire_id": "PROP_A", "nom_proprietaire": "Didier D.",
         "adresse_proprietaire": "12 rue Secrete 31000 SECRETVILLE", "logement_id": "LOG_A1",
         "statut_facture": "EMISE", "statut_generation": "OK", "total_exploitation_net": 1700,
         "total_reglement_du": 300, "reste_a_payer": 0, "mode_facturation": "MENSUEL"},
    ]
    dash_rows = [
        {"mois": "2026-01", "proprietaire_id": "PROP_A", "nb_logements": 2, "nb_bloquants_mois": 0,
         "nb_a_controler_mois": 0, "facturation_lot12_ok": "OUI", "mode_facturation": "MENSUEL",
         "statut_facture": "EMISE", "balises_non_resolues": ""},
        {"mois": "2026-02", "proprietaire_id": "PROP_B", "nb_logements": 1, "nb_bloquants_mois": 0,
         "nb_a_controler_mois": 1, "facturation_lot12_ok": "NON", "mode_facturation": "MENSUEL",
         "statut_facture": "ABSENTE", "balises_non_resolues": "FACTURE_ABSENTE;RESTE_A_PAYER"},
    ]
    ctrl_rows = [
        {"mois": "2026-02", "proprietaire_id": "PROP_B", "code_controle": "FACTURE_ABSENTE",
         "severite": "A_CONTROLER", "description": "Net calcule sans facture generee"},
    ]
    _wb(net, {"VUE_MOIS": (VUE_COLS, vue), "REGLEMENT": (REG_COLS, reg)})
    _wb(comm, {"COMMISSIONS": (COMM_COLS, comm_rows), "A_CONTROLER": (["x"], [])})
    _wb(fact, {"FACT_FACTURE_ENTETE": (FACT_COLS, fact_rows), "DASHBOARD_FACTURATION": (DASH_COLS, dash_rows),
               "A_CONTROLER": (CTRL_COLS, ctrl_rows)})
    _wb(res, {"PAR_MOIS_PROPRIETAIRE": (["mois", "proprietaire_id", "resultat"], [])})
    monkeypatch.setattr(cfg, "MASTER_NET_PROPRIETAIRE", net)
    monkeypatch.setattr(cfg, "MASTER_COMMISSIONS", comm)
    monkeypatch.setattr(cfg, "MASTER_FACT_PROPRIETAIRES", fact)
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", res)
    reader.vider_cache()
    yield tmp_path
    reader.vider_cache()


# ── lecture & consolidation ──────────────────────────────────────────────────

def test_source_alimentee(owners_files):
    assert reader.net_vue_mois().etat.disponible
    assert len(svc.load_periods()) == 2


def test_prop_conforme(owners_files):
    v = next(v for v in svc._toutes_les_vues("2026-01") if v["proprietaire_id"] == "PROP_A")
    assert v["nb_logements"] == 2 and v["reste"] == 0 and not svc.a_controler(v)


def test_commission_reprise_sans_recalcul(owners_files):
    v = next(v for v in svc._toutes_les_vues("2026-01") if v["proprietaire_id"] == "PROP_A")
    assert v["commission"] == 300           # = total_commission_mois du moteur
    assert v["ca_retenu"] == 2000 and v["net"] == 1700
    assert v["ca_retenu"] != v["commission"] != v["net"]   # distincts


def test_base_commission_somme_assiette(owners_files):
    v = next(v for v in svc._toutes_les_vues("2026-01") if v["proprietaire_id"] == "PROP_A")
    assert v["base_commission"] == 1200     # somme assiette_commission (1 ligne)


def test_taux_historique_15_puis_propre(owners_files):
    da = svc.load_owner_detail("PROP_A", "2026-01")
    assert any(abs((t["taux"] or 0) - 0.15) < 1e-9 for t in da["taux_historiques"])
    db = svc.load_owner_detail("PROP_B", "2026-02")
    assert any(abs((t["taux"] or 0) - 0.12) < 1e-9 for t in db["taux_historiques"])  # taux propre >= 02/2026


def test_reste_et_a_controler(owners_files):
    v = next(v for v in svc._toutes_les_vues("2026-02") if v["proprietaire_id"] == "PROP_B")
    assert v["reste"] == 140 and svc.a_controler(v)
    assert "FACTURE_ABSENTE" in v["balises"]


def test_acompte_distinct_du_paiement(owners_files):
    d = svc.load_owner_detail("PROP_A", "2026-01")
    l = d["logements"][0]
    assert "acomptes" in l and "paiement" in l   # champs distincts


def test_facture_absente_prop_b(owners_files):
    v = next(v for v in svc._toutes_les_vues("2026-02") if v["proprietaire_id"] == "PROP_B")
    assert v["facture_nb"] == 0


def test_reglement_complet_vs_partiel(owners_files):
    va = next(v for v in svc._toutes_les_vues("2026-01") if v["proprietaire_id"] == "PROP_A")
    vb = next(v for v in svc._toutes_les_vues("2026-02") if v["proprietaire_id"] == "PROP_B")
    assert "REGLE" in va["reglement_statut"] and "PARTIEL" in vb["reglement_statut"]


def test_source_absente(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_NET_PROPRIETAIRE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    assert reader.net_vue_mois().etat.etat == reader.ETAT_FICHIER_ABSENT
    assert svc.load_owners()["status"] == "SOURCE_INDISPONIBLE"


def test_onglet_absent(monkeypatch, tmp_path):
    p = tmp_path / "n.xlsx"; _wb(p, {"AUTRE": (["x"], [{"x": 1}])})
    monkeypatch.setattr(cfg, "MASTER_NET_PROPRIETAIRE", p); reader.vider_cache()
    assert reader.net_vue_mois().etat.etat == reader.ETAT_ONGLET_ABSENT


def test_source_vide(monkeypatch, tmp_path):
    p = tmp_path / "n.xlsx"; _wb(p, {"VUE_MOIS": (VUE_COLS, [])})
    monkeypatch.setattr(cfg, "MASTER_NET_PROPRIETAIRE", p); reader.vider_cache()
    assert reader.net_vue_mois().etat.etat == reader.ETAT_VIDE


# ── filtres / pagination ─────────────────────────────────────────────────────

def test_filtre_periode(owners_files):
    assert svc.load_owners(mois="2026-02")["count_filtre"] == 1


def test_filtre_proprietaire(owners_files):
    r = svc.load_owners(mois="2026-01", proprietaire_id="PROP_A")
    assert r["count_filtre"] == 1 and r["rows"][0]["proprietaire_id"] == "PROP_A"


def test_filtre_avec_reste(owners_files):
    r = svc.load_owners(mois="2026-02", avec_reste=True)
    assert all(v["reste"] and v["reste"] > 0 for v in r["rows"])


def test_filtre_non_facture(owners_files):
    r = svc.load_owners(mois="2026-02", facture="non")
    assert all(v["facture_nb"] == 0 for v in r["rows"])


def test_pagination(owners_files, monkeypatch):
    monkeypatch.setattr(svc, "TAILLE_PAGE", 1)
    # mois vide -> défaut = période la plus récente (2026-02, 1 prop) ; on force 2026-01 pour 1 prop aussi
    r = svc.load_owners(mois="2026-01", page=1)
    assert len(r["rows"]) == 1


# ── détail / 404 ─────────────────────────────────────────────────────────────

def test_detail_connu(owners_files):
    d = svc.load_owner_detail("PROP_A", "2026-01")
    assert d["status"] == "OK" and d["proprietaire_id"] == "PROP_A"
    assert len(d["logements"]) == 2


def test_detail_inconnu_none(owners_files):
    assert svc.load_owner_detail("PROP_INCONNU", "2026-01") is None


# ── export / masquage / garde-fous ───────────────────────────────────────────

def test_export_csv(owners_files):
    csv = svc.export_csv(mois="2026-01")
    assert "commission" in csv and "net" in csv
    assert "SECRETVILLE" not in csv and "adresse" not in csv.lower()  # adresse jamais exportée


def test_adresse_jamais_exposee(owners_files):
    d = svc.load_owner_detail("PROP_A", "2026-01")
    blob = str(d)
    assert "SECRETVILLE" not in blob and "adresse" not in blob and "C:\\" not in blob


def test_aucune_source_reelle_modifiee(owners_files):
    import hashlib
    files = [cfg.MASTER_NET_PROPRIETAIRE, cfg.MASTER_COMMISSIONS, cfg.MASTER_FACT_PROPRIETAIRES]
    sha = {str(f): hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files}
    svc.load_dashboard("2026-01"); svc.load_owner_detail("PROP_A", "2026-01"); svc.export_csv(mois="2026-01")
    assert {str(f): hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files} == sha


def test_pas_import_moteur_ni_ecriture_ni_recalcul_commission():
    base = Path(__file__).parent.parent / "app"
    for f in [base / "services" / "proprietaires_reglements_service.py",
              base / "readers" / "proprietaires_reglements_reader.py",
              base / "routes" / "proprietaires_reglements.py"]:
        src = f.read_text(encoding="utf-8")
        assert "import lot" not in src and "02_TRAVAIL" not in src
        assert "Workbook(" not in src and ".save(" not in src
        # aucun calcul de commission : pas de multiplication assiette*taux
        assert "assiette" not in src or "* taux" not in src


# ── routes ───────────────────────────────────────────────────────────────────

def test_route_dashboard_200(client, owners_files):
    r = client.get("/proprietaires-reglements?mois=2026-01")
    assert r.status_code == 200
    assert "Propriétaires" in r.text and "SECRETVILLE" not in r.text and "C:\\" not in r.text


def test_route_detail_200_et_404(client, owners_files):
    assert client.get("/proprietaires-reglements/PROP_A?mois=2026-01").status_code == 200
    assert client.get("/proprietaires-reglements/PROP_INCONNU?mois=2026-01").status_code == 404


def test_route_a_controler_et_export(client, owners_files):
    assert client.get("/proprietaires-reglements/a-controler?mois=2026-02").status_code == 200
    exp = client.get("/proprietaires-reglements/export.csv?mois=2026-01")
    assert exp.status_code == 200 and exp.headers["content-type"].startswith("text/csv")


def test_aucune_route_paiement(client, owners_files):
    # aucune route POST de paiement/facturation
    assert client.post("/proprietaires-reglements/PROP_A/payer").status_code in (404, 405)


def test_sidebar_nav(client, owners_files):
    r = client.get("/proprietaires-reglements")
    assert 'href="/proprietaires-reglements"' in r.text

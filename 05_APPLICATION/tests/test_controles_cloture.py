"""APP-5A — Contrôles & clôture : lecture, niveaux, clôture (moteur), garde-fous.

Fixtures Excel ISOLÉES. Aucun contrôle inventé, aucun BLOQUANT déclassé, aucune clôture, aucune
écriture, vraie app.db jamais touchée.
"""
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.readers import controles_cloture_reader as reader
from app.services import controles_cloture_service as svc


def _wb(path, sheets):
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        ws = wb.create_sheet(name); ws.append(cols)
        for r in rows:
            ws.append([r.get(c) for c in cols])
    wb.save(str(path)); wb.close()


MASTER_COLS = ["ctrl_pk", "source_module", "source_table", "source_pk", "code_controle", "severity",
               "message", "mois", "logement_id", "proprietaire_id", "statut_resolution", "commentaire",
               "date_detection"]
DASH_COLS = ["mois", "nb_bloquants_ouverts", "nb_a_controler_ouverts", "nb_info", "statut_mois_banque",
             "cloture_possible", "facturation_lot12_ok", "commentaire"]
REF_COLS = ["mois", "statut_mois", "date_passage_controle", "date_cloture",
            "nb_lignes_bancaires_non_classees", "nb_controles_bloquants_ouverts", "commentaire"]


def _c(pk, module, code, sev, mois, msg="msg", statut="OUVERT", logement="", table="t"):
    return {"ctrl_pk": pk, "source_module": module, "source_table": table, "source_pk": pk + "-src",
            "code_controle": code, "severity": sev, "message": msg, "mois": mois, "logement_id": logement,
            "proprietaire_id": "", "statut_resolution": statut, "commentaire": "", "date_detection": "2026-05-01"}


@pytest.fixture
def ctrl_files(tmp_path, monkeypatch):
    coh = tmp_path / "COH.xlsx"; ref = tmp_path / "REF.xlsx"
    master = [
        _c("CTRL-001", "lot8", "BANQUE_NON_CLASSEE", "BLOQUANT", "2026-05", "Lignes non classees"),
        _c("CTRL-002", "lot6d", "MENAGE_TOTAL_ECART_HOSTAWAY", "A_CONTROLER", "2026-05", "Ecart menage"),
        _c("CTRL-003", "lot1", "INFO_HA", "INFO", "2026-05", "Info"),
        _c("CTRL-004", "lot10", "FACTURE_ABSENTE", "BLOQUANT", "2026-04", "Bloquant sur mois cloture"),
        _c("CTRL-005", "lot11", "SANS_PERIODE", "A_CONTROLER", "", "Controle sans periode"),
        _c("CTRL-006", "lot6d", "MENAGE_TOTAL_ECART_HOSTAWAY", "A_CONTROLER", "2026-05", "Doublon code"),
    ]
    dash = [
        {"mois": "2026-05", "nb_bloquants_ouverts": 1, "nb_a_controler_ouverts": 3, "nb_info": 1,
         "statut_mois_banque": "OUVERT", "cloture_possible": "NON", "facturation_lot12_ok": "NON", "commentaire": ""},
        {"mois": "2026-04", "nb_bloquants_ouverts": 0, "nb_a_controler_ouverts": 0, "nb_info": 0,
         "statut_mois_banque": "CLOTURE", "cloture_possible": "OUI", "facturation_lot12_ok": "OUI", "commentaire": ""},
    ]
    ref_rows = [
        {"mois": "2026-05", "statut_mois": "OUVERT", "date_passage_controle": "", "date_cloture": "",
         "nb_lignes_bancaires_non_classees": 5, "nb_controles_bloquants_ouverts": 1, "commentaire": ""},
        {"mois": "2026-04", "statut_mois": "CLOTURE", "date_passage_controle": "2026-05-08",
         "date_cloture": "2026-05-10", "nb_lignes_bancaires_non_classees": 0,
         "nb_controles_bloquants_ouverts": 0, "commentaire": "cloture"},
        {"mois": "2026-03", "statut_mois": "EN_CONTROLE", "date_passage_controle": "2026-04-05",
         "date_cloture": "", "nb_lignes_bancaires_non_classees": 0, "nb_controles_bloquants_ouverts": 0, "commentaire": ""},
    ]
    _wb(coh, {"MASTER": (MASTER_COLS, master),
              "DASHBOARD_MOIS": (DASH_COLS, dash),
              "BLOQUANTS_OUVERTS": (MASTER_COLS, [m for m in master if m["severity"] == "BLOQUANT"]),
              "A_CONTROLER_OUVERTS": (MASTER_COLS, [m for m in master if m["severity"] == "A_CONTROLER"])})
    _wb(ref, {"REF_Cloture_Mensuelle": (REF_COLS, ref_rows)})
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE_FILE", coh)
    monkeypatch.setattr(cfg, "REF_SETUP", ref)
    reader.vider_cache()
    yield tmp_path
    reader.vider_cache()


# ── lecture & niveaux ────────────────────────────────────────────────────────

def test_source_alimentee(ctrl_files):
    assert reader.controles().etat.disponible and reader.controles().etat.nb_lignes == 6


def test_niveaux(ctrl_files):
    vues = svc._toutes_les_vues()
    niv = {v["code"]: v["niveau"] for v in vues}
    assert niv["BANQUE_NON_CLASSEE"] == "BLOQUANT"
    assert niv["INFO_HA"] == "INFO"


def test_bloquant_jamais_declasse(ctrl_files):
    v = next(v for v in svc._toutes_les_vues() if v["code"] == "BANQUE_NON_CLASSEE")
    assert v["niveau"] == "BLOQUANT" and v["bloquant"] is True


def test_controle_sans_periode(ctrl_files):
    v = next(v for v in svc._toutes_les_vues() if v["code"] == "SANS_PERIODE")
    assert v["mois"] == ""  # sans période, lu quand même


def test_doublon_present(ctrl_files):
    vues = [v for v in svc._toutes_les_vues() if v["code"] == "MENAGE_TOTAL_ECART_HOSTAWAY"]
    assert len(vues) == 2 and vues[0]["stable_id"] != vues[1]["stable_id"]


def test_mois_ouvert_en_controle_cloture(ctrl_files):
    d = svc.load_dashboard()["summary"]
    assert d["nb_mois_ouverts"] == 1 and d["nb_mois_en_controle"] == 1 and d["nb_mois_clotures"] == 1


def test_mois_non_cloturable(ctrl_files):
    st = svc.load_month_status("2026-05")
    assert st["cloturable"] is False and len(st["bloquants"]) == 1


def test_anomalie_apres_cloture(ctrl_files):
    st = svc.load_month_status("2026-04")   # CLOTURE mais un BLOQUANT ouvert (CTRL-004)
    assert st["statut_mois"] == "CLOTURE" and st["anomalies_post_cloture"] is True


def test_source_absente(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE_FILE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_FICHIER_ABSENT
    assert svc.load_controls()["status"] == "SOURCE_INDISPONIBLE"


def test_onglet_absent(monkeypatch, tmp_path):
    p = tmp_path / "c.xlsx"; _wb(p, {"AUTRE": (["x"], [{"x": 1}])})
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE_FILE", p); reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_ONGLET_ABSENT


def test_source_vide(monkeypatch, tmp_path):
    p = tmp_path / "c.xlsx"; _wb(p, {"MASTER": (MASTER_COLS, [])})
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE_FILE", p); reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_VIDE


def test_code_inconnu_sans_explication(ctrl_files):
    v = next(v for v in svc._toutes_les_vues() if v["code"] == "SANS_PERIODE")
    assert v["explication"] == ""   # code inconnu -> pas d'explication inventée


# ── filtres / pagination ─────────────────────────────────────────────────────

def test_filtre_periode(ctrl_files):
    r = svc.load_controls(mois="2026-05")
    assert all(v["mois"] == "2026-05" for v in r["rows"]) and r["count_filtre"] == 4


def test_filtre_module(ctrl_files):
    r = svc.load_controls(module="lot6d")
    assert all(v["module"] == "lot6d" for v in r["rows"])


def test_filtre_niveau(ctrl_files):
    r = svc.load_controls(niveau="BLOQUANT")
    assert all(v["niveau"] == "BLOQUANT" for v in r["rows"]) and r["count_filtre"] == 2


def test_bloquants_seul(ctrl_files):
    r = svc.load_controls(bloquants_seul=True)
    assert all(v["bloquant"] for v in r["rows"])


def test_non_cloturable(ctrl_files):
    r = svc.load_controls(non_cloturable=True)
    assert all(v["mois"] == "2026-05" for v in r["rows"])  # seul 2026-05 non clôturable


def test_recherche(ctrl_files):
    r = svc.load_controls(recherche="non classees")
    assert r["count_filtre"] >= 1


def test_pagination(ctrl_files, monkeypatch):
    monkeypatch.setattr(svc, "TAILLE_PAGE", 2)
    r = svc.load_controls(page=1)
    assert len(r["rows"]) == 2 and r["pages"] == 3


def test_tri_bloquants_dabord(ctrl_files):
    r = svc.load_controls(tri="niveau")
    assert r["rows"][0]["niveau"] == "BLOQUANT"


# ── détail / bloquants / mois ────────────────────────────────────────────────

def test_detail_connu(ctrl_files):
    d = svc.load_control_detail("CTRL-001")
    assert d["status"] == "OK" and d["vue"]["code"] == "BANQUE_NON_CLASSEE"
    assert d["impact_cloture"]["empeche_cloture"] is True


def test_detail_inconnu_none(ctrl_files):
    assert svc.load_control_detail("CTRL-INCONNU") is None


def test_blockers(ctrl_files):
    b = svc.load_blockers()
    assert all(v["bloquant"] for v in b["lignes"]) and len(b["lignes"]) == 2
    assert set(b["par_module"].keys()) == {"lot8", "lot10"}


# ── export / garde-fous ──────────────────────────────────────────────────────

def test_export_csv(ctrl_files):
    csv = svc.export_csv()
    assert "niveau" in csv and "BANQUE_NON_CLASSEE" in csv


def test_aucun_chemin_absolu(ctrl_files):
    d = svc.load_control_detail("CTRL-001")
    assert "C:\\" not in str(d)


def test_pas_import_moteur_ni_ecriture():
    base = Path(__file__).parent.parent / "app"
    for f in [base / "services" / "controles_cloture_service.py",
              base / "readers" / "controles_cloture_reader.py",
              base / "routes" / "controles_cloture.py"]:
        src = f.read_text(encoding="utf-8")
        assert "import lot" not in src and "02_TRAVAIL" not in src
        assert "Workbook(" not in src and ".save(" not in src


def test_aucune_source_reelle_modifiee(ctrl_files):
    import hashlib
    files = [cfg.MASTER_CTRL_COHERENCE_FILE, cfg.REF_SETUP]
    sha = {str(f): hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files}
    svc.load_dashboard(); svc.load_month_status("2026-05"); svc.export_csv()
    assert {str(f): hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files} == sha


# ── routes ───────────────────────────────────────────────────────────────────

def test_route_dashboard_200(client, ctrl_files):
    r = client.get("/controles-cloture")
    assert r.status_code == 200 and "Contrôles" in r.text and "C:\\" not in r.text


def test_route_detail_200_et_404(client, ctrl_files):
    assert client.get("/controles-cloture/CTRL-001").status_code == 200
    assert client.get("/controles-cloture/CTRL-INCONNU").status_code == 404


def test_route_mois_bloquants_export(client, ctrl_files):
    assert client.get("/controles-cloture/mois/2026-05").status_code == 200
    assert client.get("/controles-cloture/bloquants").status_code == 200
    exp = client.get("/controles-cloture/export.csv")
    assert exp.status_code == 200 and exp.headers["content-type"].startswith("text/csv")


def test_aucune_route_post_cloture(client, ctrl_files):
    assert client.post("/controles-cloture/mois/2026-05/cloturer").status_code in (404, 405)


def test_sidebar_nav(client, ctrl_files):
    r = client.get("/controles-cloture")
    assert 'href="/controles-cloture"' in r.text

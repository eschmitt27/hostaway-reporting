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


def _seeder_constats_sqlite(db_path, master, dash):
    """Écrit les constats Lot11 et le DASHBOARD_MOIS en base, tels que `controles_lot11_service`
    les produirait — le reader ne lit plus que ces tables."""
    from app.db.connection import get_db

    conn = get_db(db_path)
    try:
        for m in master:
            conn.execute(
                "INSERT INTO controles_lot11_constats (ctrl_pk, source_module, source_table, "
                "source_pk, code_controle, severity, message, impact_facture, statut_resolution, "
                "commentaire) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (m["ctrl_pk"], m["source_module"], m["source_table"], m["source_pk"],
                 m["code_controle"], m["severity"], m["message"],
                 "BLOQUANT_FACTURE" if m["severity"] == "BLOQUANT" else "A_DECIDER",
                 m["statut_resolution"], m["commentaire"]))
            conn.execute(
                "INSERT OR REPLACE INTO controles_lot11_constats_champs (ctrl_pk, mois, "
                "logement_id, proprietaire_id, date_detection) VALUES (?,?,?,?,?)",
                (m["ctrl_pk"], m["mois"] or None, m["logement_id"] or None,
                 m["proprietaire_id"] or None, m["date_detection"]))
        for d in dash:
            conn.execute(
                "INSERT INTO controles_lot11_dashboard_mois (run_id, mois, nb_bloquants_ouverts, "
                "nb_a_controler_ouverts, nb_info, statut_mois_banque, cloture_possible, "
                "facturation_lot12_ok) VALUES ('L11-TEST',?,?,?,?,?,?,?)",
                (d["mois"], d["nb_bloquants_ouverts"], d["nb_a_controler_ouverts"], d["nb_info"],
                 d["statut_mois_banque"], d["cloture_possible"], d["facturation_lot12_ok"]))
        conn.commit()
    finally:
        conn.close()


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
    # Le statut de clôture vient désormais du référentiel SQLite, plus du classeur : la fixture
    # décrit donc les mêmes trois mois en base. Le classeur reste construit pour les tests qui
    # vérifient encore le chemin Excel (source absente, onglet absent).
    _wb(ref, {"REF_Cloture_Mensuelle": (REF_COLS, ref_rows)})
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE_FILE", coh)
    monkeypatch.setattr(cfg, "REF_SETUP", ref)

    from test_logements import construire_referentiel
    db = construire_referentiel(tmp_path, cloture=[
        {k: str(v) for k, v in r.items()} for r in ref_rows])
    # Lot11 est un moteur SQLite natif : les constats et le dashboard vivent en base
    # (0041/0042/0046), plus dans le classeur. La fixture sème donc les MÊMES lignes que
    # `master`/`dash` ci-dessus — une seule vérité, décrite une fois.
    _seeder_constats_sqlite(db, master, dash)
    monkeypatch.setattr(cfg, "DB_PATH", db)
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
    """Source indisponible côté SERVICE : l'écran l'annonce, il n'invente aucun contrôle.

    Le monkeypatch portait sur `MASTER_CTRL_COHERENCE_FILE`, qui ne commande plus rien depuis que
    les constats sont en base : le test dépendait alors de la base laissée par le test précédent
    (il passait seul, échouait en campagne). La source à rendre absente est désormais la BASE.
    """
    import sqlite3

    base = tmp_path / "sans_constats.db"
    sqlite3.connect(str(base)).close()
    monkeypatch.setattr(cfg, "DB_PATH", base)
    reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_FICHIER_ABSENT
    assert svc.load_controls()["status"] == "SOURCE_INDISPONIBLE"
    reader.vider_cache()


def test_base_non_migree_source_absente(monkeypatch, tmp_path):
    """Équivalent SQLite de l'ancien « onglet absent » : la table de constats n'existe pas.

    Le reader ne lit plus de classeur — il n'y a donc plus d'onglet à manquer. L'état à couvrir
    est celui d'une base qui n'a jamais reçu la migration Lot11.
    """
    import sqlite3

    base = tmp_path / "sans_tables.db"
    sqlite3.connect(str(base)).close()
    monkeypatch.setattr(cfg, "DB_PATH", base)
    reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_FICHIER_ABSENT
    reader.vider_cache()


def test_source_vide(monkeypatch, tmp_path):
    """Tables présentes mais aucun constat : VIDE, jamais « absente ».

    La distinction compte : « Lot11 n'a jamais tourné » et « Lot11 a tourné et n'a rien trouvé »
    ne se disent pas de la même façon à l'écran.
    """
    from app.db.connection import apply_migrations

    base = tmp_path / "migree_vide.db"
    apply_migrations(base)
    monkeypatch.setattr(cfg, "DB_PATH", base)
    reader.vider_cache()
    assert reader.controles().etat.etat == reader.ETAT_VIDE
    reader.vider_cache()


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

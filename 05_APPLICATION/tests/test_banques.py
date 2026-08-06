"""APP-4A — Banques & caisse : lecture, rapprochement (moteur), masquage, garde-fous.

Tous les tests s'appuient sur une fixture Excel banque ISOLÉE (jamais la source réelle).
Aucune écriture métier, aucune source réelle modifiée, aucun import du moteur, IBAN masqué.
"""
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.readers import banques_reader as reader
from app.services import banques_service as svc


# ── Fixture : classeur banque isolé ──────────────────────────────────────────

_NORM_COLS = ["mouvement_id", "ROW_HASH", "import_id", "date_operation", "date_valeur", "libelle",
              "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte", "categorie",
              "type_flux_id", "statut_controle", "niveau_risque", "codes_anomalie", "regle_id_appliquee"]


def _mvt(mid, date, lib, montant, sens, statut="VALIDE", type_flux="TYPE_FLUX_017",
         compte="CM_02211_00021321603", anomalie="", tiers=""):
    return {"mouvement_id": mid, "ROW_HASH": "H" + mid, "import_id": "IMP-BQ-CM-2026-03-001",
            "date_operation": date, "date_valeur": date, "libelle": lib,
            "libelle_brut": "BRUT " + lib + " SECRET-IBAN-FR7612345", "montant": montant, "sens": sens,
            "devise": "EUR", "compte_id": compte, "tiers_detecte": tiers, "categorie": "",
            "type_flux_id": type_flux, "statut_controle": statut, "niveau_risque": "",
            "codes_anomalie": anomalie, "regle_id_appliquee": "R_001"}


def _write(path, sheets: dict[str, tuple[list, list]]):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        ws = wb.create_sheet(name)
        ws.append(cols)
        for r in rows:
            ws.append([r.get(c) if isinstance(r, dict) else r[i] for i, c in enumerate(cols)])
    wb.save(str(path))
    wb.close()


@pytest.fixture
def bank_file(tmp_path, monkeypatch):
    """Construit un BANQUE_LOT8_IMPORT.xlsx isolé et pointe cfg.MASTER_BANQUE dessus."""
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    norm = [
        _mvt("MVT-001", "2026-03-02", "VIREMENT AIRBNB PAYOUT", 1250.00, "CREDIT", tiers="AIRBNB"),
        _mvt("MVT-002", "2026-03-05", "VIREMENT PROPRIETAIRE DIDIER", 800.00, "DEBIT"),
        _mvt("MVT-003", "2026-03-06", "FRAIS TENUE DE COMPTE", 4.50, "DEBIT", statut="VALIDE",
             type_flux="TYPE_FLUX_016"),
        _mvt("MVT-004", "2026-03-10", "PRELEVEMENT INCONNU", 33.00, "DEBIT", statut="A_CONTROLER",
             anomalie="CLASSIFICATION_INCERTAINE"),
        _mvt("MVT-005", "2026-05-04", "VIREMENT AIRBNB PAYOUT", 980.00, "CREDIT", tiers="AIRBNB"),
        _mvt("MVT-006", "2026-05-04", "VIREMENT AIRBNB PAYOUT", 980.00, "CREDIT", tiers="AIRBNB"),  # doublon
    ]
    ctrl = [{"mouvement_id": "MVT-004", "code_controle": "CLASSIFICATION_INCERTAINE",
             "severite": "A_CONTROLER", "description": "Libellé non classé", "statut_controle": "A_CONTROLER"}]
    rap_air = [{"mouvement_id": "MVT-001", "montant_banque": 1250.00, "reference_airbnb": "",
                "statut_rapprochement": "EN_ATTENTE_EXPORT_AIRBNB", "methode_rapprochement": "",
                "commentaire": "attente export"},
               {"mouvement_id": "MVT-005", "montant_banque": 980.00, "reference_airbnb": "",
                "statut_rapprochement": "EN_ATTENTE_EXPORT_AIRBNB", "methode_rapprochement": "", "commentaire": ""}]
    rap_prop = [{"mouvement_id": "MVT-002", "proprietaire_id": "PROP_0001", "nature_presumee": "ACOMPTE",
                 "statut_rapprochement": "EN_ATTENTE_SAISIE_ACOMPTE", "prerequis_rapprochement": "Lot5",
                 "commentaire": ""}]
    ctrl8c = [{"code_controle": "AIRBNB_ATTENTE", "severite": "INFO", "nb_lignes": 2,
               "total_montant_eur": 2230.0, "description": "En attente export Airbnb", "action_requise": "Importer export"}]
    _write(p, {
        "NORM_Banque": (_NORM_COLS, norm),
        "CTRL_A_CONTROLER": (["mouvement_id", "code_controle", "severite", "description", "statut_controle"], ctrl),
        "RAPPROCH_AIRBNB_ATTENTE": (["mouvement_id", "montant_banque", "reference_airbnb",
                                     "statut_rapprochement", "methode_rapprochement", "commentaire"], rap_air),
        "RAPPROCH_PROPRIETAIRES_ATTENTE": (["mouvement_id", "proprietaire_id", "nature_presumee",
                                            "statut_rapprochement", "prerequis_rapprochement", "commentaire"], rap_prop),
        "CTRL_RAPPROCHEMENT_8C": (["code_controle", "severite", "nb_lignes", "total_montant_eur",
                                   "description", "action_requise"], ctrl8c),
    })
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    reader.vider_cache()
    yield p
    reader.vider_cache()


# ── 1-13 : lecture & états ───────────────────────────────────────────────────

def test_source_alimentee(bank_file):
    assert reader.mouvements().etat.disponible
    assert reader.mouvements().etat.nb_lignes == 6


def test_mouvement_non_rapproche_et_rapproche_attente(bank_file):
    d = svc.load_movements(mois="2026-03")
    par_id = {r["mouvement_id"]: r for r in svc._toutes_les_vues("2026-03")[0]}
    assert par_id["MVT-001"]["rappro_statut"] == "EN_ATTENTE_EXPORT_AIRBNB"   # attente
    assert par_id["MVT-003"]["rappro_statut"] == ""                            # non concerné


def test_debit_credit(bank_file):
    vues, _ = svc._toutes_les_vues("2026-03")
    par = {v["mouvement_id"]: v for v in vues}
    assert par["MVT-001"]["sens"] == "CREDIT"
    assert par["MVT-002"]["sens"] == "DEBIT"


def test_caisse_non_alimentee(bank_file):
    assert reader.caisse().etat.etat == reader.ETAT_NON_ALIMENTE
    assert svc.load_summary("2026-03")["nb_mouvements_caisse"] is None


def test_doublon_present_non_fusionne(bank_file):
    vues, _ = svc._toutes_les_vues("2026-05")
    ids = [v["mouvement_id"] for v in vues]
    assert "MVT-005" in ids and "MVT-006" in ids  # les deux lignes existent, non fusionnées


def test_a_controler_flag(bank_file):
    vues, _ = svc._toutes_les_vues("2026-03")
    par = {v["mouvement_id"]: v for v in vues}
    assert svc.a_rapprocher(par["MVT-004"])   # statut A_CONTROLER + contrôle
    assert not svc.a_rapprocher(par["MVT-003"])  # FRAIS classé, aucune attente


def test_source_absente(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    assert reader.mouvements().etat.etat == reader.ETAT_FICHIER_ABSENT
    assert svc.load_movements()["status"] == "SOURCE_INDISPONIBLE"


def test_onglet_absent(monkeypatch, tmp_path):
    p = tmp_path / "b.xlsx"
    _write(p, {"AUTRE": (["x"], [{"x": 1}])})
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    reader.vider_cache()
    assert reader.mouvements().etat.etat == reader.ETAT_ONGLET_ABSENT


def test_source_vide(monkeypatch, tmp_path):
    p = tmp_path / "b.xlsx"
    _write(p, {"NORM_Banque": (_NORM_COLS, [])})
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    reader.vider_cache()
    assert reader.mouvements().etat.etat == reader.ETAT_VIDE


def test_compte_inconnu_toujours_lu(bank_file):
    # un compte inconnu du référentiel reste lu et masqué, jamais rejeté
    v = next(v for v in svc._toutes_les_vues("2026-03")[0])
    assert v["compte_masque"]


def test_summary_totaux(bank_file):
    s = svc.load_summary("2026-03")
    assert s["nb_mouvements_bancaires"] == 4
    assert s["nb_a_rapprocher"] >= 3
    assert s["nb_rapproches"] == 0  # aucun rapprochement validé produit


def test_freshness(bank_file):
    fr = svc.load_freshness()
    assert fr["etat"] == "A_JOUR" and fr["derniere_generation"]


def test_periodes_disponibles(bank_file):
    assert svc.load_available_periods() == ["2026-05", "2026-03"]


# ── 14-19 : filtres & pagination ─────────────────────────────────────────────

def test_filtre_periode(bank_file):
    assert svc.load_movements(mois="2026-05")["count_filtre"] == 2


def test_filtre_sens(bank_file):
    r = svc.load_movements(mois="2026-03", sens="CREDIT")
    assert all(v["sens"] == "CREDIT" for v in r["rows"])


def test_filtre_non_rapproche(bank_file):
    r = svc.load_movements(mois="2026-03", non_rapproche=True)
    assert all(svc.a_rapprocher(v) for v in r["rows"])
    assert r["count_filtre"] < 4


def test_filtre_montant(bank_file):
    r = svc.load_movements(mois="2026-03", montant_min="100")
    assert all(abs(v["montant"]) >= 100 for v in r["rows"])


def test_recherche_libelle(bank_file):
    r = svc.load_movements(mois="2026-03", recherche="airbnb")
    assert r["count_filtre"] >= 1 and all("airbnb" in v["libelle"].lower() for v in r["rows"])


def test_pagination(bank_file, monkeypatch):
    monkeypatch.setattr(svc, "TAILLE_PAGE", 2)
    r = svc.load_movements(mois="2026-03", page=1)
    assert len(r["rows"]) == 2 and r["pages"] == 2


# ── 20-21 : fiche détail ─────────────────────────────────────────────────────

def test_detail_connu(bank_file):
    d = svc.load_detail("MVT-001")
    assert d["status"] == "OK" and d["vue"]["mouvement_id"] == "MVT-001"


def test_detail_inconnu_none(bank_file):
    assert svc.load_detail("MVT-INCONNU") is None


# ── 22-23 : export & masquage ────────────────────────────────────────────────

def test_export_csv(bank_file):
    csv = svc.export_movements_csv(mois="2026-03")
    assert "statut_controle" in csv and "MVT" not in csv.split("\n")[0]  # en-tête métier
    assert "CM ••••" in csv                                             # compte masqué
    assert "IBAN" not in csv and "SECRET" not in csv                    # brut jamais exposé


def test_masquage_compte(bank_file):
    assert reader.masquer_compte("CM_02211_00021321603") == "CM ••••1603"
    v = svc.load_detail("MVT-001")["vue"]
    assert "02211" not in v["compte_masque"]  # partie médiane masquée


# ── 24-30 : garde-fous & routes ──────────────────────────────────────────────

def test_aucun_chemin_absolu_ni_brut_dans_vue(bank_file):
    v = svc.load_detail("MVT-001")["vue"]
    blob = str(v)
    assert "C:\\" not in blob and "libelle_brut" not in blob and "SECRET" not in blob


def test_service_ninvente_aucun_rapprochement(bank_file):
    # MVT-003 (frais, non listé en attente) ne reçoit aucun statut de rapprochement inventé
    v = next(v for v in svc._toutes_les_vues("2026-03")[0] if v["mouvement_id"] == "MVT-003")
    assert v["rappro_statut"] == "" and v["rappro_canal"] == ""


def test_aucune_source_reelle_modifiee(bank_file):
    import hashlib
    sha = hashlib.sha256(Path(bank_file).read_bytes()).hexdigest()
    svc.load_dashboard("2026-03")
    svc.load_detail("MVT-001")
    svc.export_movements_csv(mois="2026-03")
    assert hashlib.sha256(Path(bank_file).read_bytes()).hexdigest() == sha


def test_pas_import_moteur_ni_ecriture_openpyxl():
    base = Path(__file__).parent.parent / "app"
    for f in [base / "services" / "banques_service.py", base / "readers" / "banques_reader.py",
              base / "routes" / "banques.py"]:
        src = f.read_text(encoding="utf-8")
        assert "import lot8" not in src and "02_TRAVAIL" not in src
        assert "Workbook(" not in src and ".save(" not in src   # aucune écriture openpyxl


def test_route_dashboard_200(client, bank_file):
    r = client.get("/banques-caisse?mois=2026-03")
    assert r.status_code == 200
    assert "Banques" in r.text and "CM ••••" in r.text
    assert "C:\\" not in r.text and "SECRET" not in r.text


def test_route_detail_200_et_404(client, bank_file):
    assert client.get("/banques-caisse/mouvements/MVT-001").status_code == 200
    assert client.get("/banques-caisse/mouvements/MVT-INCONNU").status_code == 404


def test_route_a_rapprocher_et_export(client, bank_file):
    assert client.get("/banques-caisse/a-rapprocher?mois=2026-03").status_code == 200
    exp = client.get("/banques-caisse/export.csv?mois=2026-03")
    assert exp.status_code == 200 and exp.headers["content-type"].startswith("text/csv")


def test_sidebar_nav_present(client, bank_file):
    r = client.get("/banques-caisse")
    assert 'href="/banques-caisse"' in r.text  # lien nav actif


# ── Statut source Airbnb absente (SOURCE_AIRBNB_DETAILLEE_ABSENTE) ───────────

def test_statut_airbnb_compte_les_bloquees(bank_file):
    st = svc.statut_source_airbnb()
    assert st["disponible_export"] is False
    assert st["nb_bloquees"] == 2   # MVT-001 + MVT-005 dans la fixture


def test_statut_airbnb_visible_sur_dashboard(client, bank_file):
    r = client.get("/banques-caisse")
    assert "SOURCE_AIRBNB_DETAILLEE_ABSENTE" in r.text
    assert ">2<" in r.text or "2</strong>" in r.text or "<strong>2</strong>" in r.text


def test_statut_airbnb_donnees_minimales_affichees(client, bank_file):
    r = client.get("/banques-caisse")
    assert "référence Airbnb" in r.text and "montant net" in r.text


def test_statut_airbnb_aucun_bouton_import_actif(client, bank_file):
    r = client.get("/banques-caisse")
    assert "Import disponible après validation d'un format réel" in r.text
    assert 'disabled' in r.text


def test_statut_airbnb_source_absente_ne_casse_pas(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    st = svc.statut_source_airbnb()
    assert st["nb_bloquees"] == 0 and st["source_lisible"] is False
    reader.vider_cache()


def test_statut_airbnb_aucune_pii(client, bank_file):
    r = client.get("/banques-caisse")
    assert "iban" not in r.text.lower()


def test_statut_airbnb_aucun_chemin_absolu(client, bank_file):
    r = client.get("/banques-caisse")
    assert "C:\\" not in r.text

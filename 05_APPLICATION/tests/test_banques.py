"""APP-4A — Banques & caisse : lecture, rapprochement (moteur), masquage, garde-fous.

Tous les tests s'appuient sur une base SQLite ISOLÉE (jamais la base réelle, jamais un classeur).
Aucune écriture métier, aucune source réelle modifiée, aucun import du moteur, IBAN masqué.
"""
from pathlib import Path

import pytest

import app.config as cfg
from app.readers import banques_reader as reader
from app.services import banque_attentes_service as att
from app.services import banque_classification_service as cls
from app.services import banques_service as svc
import fixtures_banque as fx

COMPTE = fx.COMPTE


@pytest.fixture
def bank_file(tmp_db, monkeypatch):
    """Peuple la base isolée avec un jeu Banque connu.

    Le nom `bank_file` est conservé : ces tests le nomment partout, et le renommer en même temps
    qu'on change de source rendrait illisible ce qui casse quoi.
    """
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_db.parent / "CLASSEUR_ABSENT.xlsx")
    fx.construire(
        tmp_db,
        mouvements=[
            fx.mouvement("MVT-001", "2026-03-02", "VIREMENT AIRBNB PAYOUT", 1250.00, "CREDIT",
                         tiers="AIRBNB",
                         statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
            fx.mouvement("MVT-002", "2026-03-05", "VIREMENT PROPRIETAIRE DEMO", 800.00, "DEBIT",
                         tiers="PROP_0001",
                         statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
            fx.mouvement("MVT-003", "2026-03-06", "FRAIS TENUE DE COMPTE", 4.50, "DEBIT",
                         type_flux="TYPE_FLUX_016"),
            fx.mouvement("MVT-004", "2026-03-10", "PRELEVEMENT INCONNU", 33.00, "DEBIT",
                         statut_controle=cls.ST_A_CONTROLER,
                         statut_classification=cls.CLASS_A_ENVOYER_IA,
                         codes_anomalie="CLASSIFICATION_INCERTAINE",
                         niveau_anomalie=cls.ST_A_CONTROLER),
            fx.mouvement("MVT-005", "2026-05-04", "VIREMENT AIRBNB PAYOUT", 980.00, "CREDIT",
                         tiers="AIRBNB",
                         statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
            # Même montant, même jour, même libellé que MVT-005 : deux vrais mouvements possibles.
            # Ils doivent rester DEUX lignes.
            fx.mouvement("MVT-006", "2026-05-04", "VIREMENT AIRBNB PAYOUT", 980.00, "CREDIT",
                         tiers="AIRBNB",
                         statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
        ],
        controles=[
            fx.controle("MVT-004", "CLASSIFICATION_INCERTAINE", "Libelle non classe"),
        ],
        attentes=[
            fx.attente("MVT-001", 1250.00, att.ATTENTE_EXPORT_PLATEFORME, tiers="AIRBNB",
                       commentaire="attente export"),
            fx.attente("MVT-005", 980.00, att.ATTENTE_EXPORT_PLATEFORME, tiers="AIRBNB"),
            fx.attente("MVT-002", 800.00, att.ATTENTE_SAISIE_ACOMPTE, tiers="PROP_0001"),
        ],
    )
    reader.vider_cache()
    yield tmp_db
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


def test_banque_non_initialisee(tmp_db):
    """Base migrée mais sans aucun mouvement : un état nommé, pas un écran vide."""
    reader.vider_cache()
    etat = reader.mouvements().etat
    assert etat.etat == reader.ETAT_NON_INITIALISEE
    assert not etat.disponible
    assert etat.nb_lignes == 0


def test_mouvements_importes_mais_non_classes(tmp_db):
    """« Importé, pas classé » se distingue de « rien en base » : l'action à faire diffère."""
    fx.construire(tmp_db, mouvements=[
        fx.mouvement("MVT-900", "2026-03-02", "VIREMENT A CLASSER", 10.0, "CREDIT", classe=False)])
    reader.vider_cache()
    assert reader.mouvements().etat.etat == reader.ETAT_NON_CLASSEE


def test_vue_legitimement_vide(bank_file):
    """Aucun contrôle de rapprochement ouvert est une bonne nouvelle, pas une panne de source."""
    from app.db.connection import get_db
    conn = get_db(bank_file)
    try:
        conn.execute("DELETE FROM banque_rapprochements")
        conn.commit()
    finally:
        conn.close()
    reader.vider_cache()
    assert reader.controles_rappro_8c().etat.etat == reader.ETAT_VIDE


def test_aucune_lecture_du_classeur_banque(bank_file):
    """Le classeur pointé par la configuration n'existe pas, et rien ne s'en plaint."""
    assert not Path(cfg.MASTER_BANQUE).exists()
    assert reader.mouvements().etat.nb_lignes == 6
    assert "xlsx" not in reader.mouvements().etat.fichier.lower()


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


# ── Catégorisation des versements plateformes (Airbnb) ───────────────────────
# RÈGLE MÉTIER (2026-08-08) : la Banque catégorise l'origine d'un virement (PAYOUT_PLATEFORME),
# elle ne le rapproche jamais à une réservation individuelle. Aucun export Airbnb requis.

def test_categorisation_airbnb_compte_les_identifiables(bank_file):
    st = svc.categorisation_versements_airbnb()
    assert st["source_lisible"] is True
    assert st["nb_identifiables"] == 2   # MVT-001 + MVT-005 dans la fixture
    assert st["montant_identifiables"] == 2230.0


def test_categorisation_airbnb_visible_sur_dashboard(client, bank_file):
    r = client.get("/banques-caisse")
    assert "VERSEMENTS PLATEFORMES" in r.text
    assert ">2<" in r.text or "2</strong>" in r.text or "<strong>2</strong>" in r.text


def test_categorisation_airbnb_aucune_reference_a_une_reservation(client, bank_file):
    """Ne jamais suggérer qu'un export Airbnb ou une réservation candidate est nécessaire."""
    r = client.get("/banques-caisse")
    assert "SOURCE_AIRBNB_DETAILLEE_ABSENTE" not in r.text
    assert "export Airbnb détaillé" not in r.text
    assert "réservation candidate" not in r.text.lower()


def test_categorisation_airbnb_source_absente_ne_casse_pas(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    st = svc.categorisation_versements_airbnb()
    assert st["nb_identifiables"] == 0 and st["source_lisible"] is False
    reader.vider_cache()


def test_categorisation_airbnb_aucune_pii(client, bank_file):
    r = client.get("/banques-caisse")
    assert "iban" not in r.text.lower()


def test_categorisation_airbnb_aucun_chemin_absolu(client, bank_file):
    r = client.get("/banques-caisse")
    assert "C:\\" not in r.text

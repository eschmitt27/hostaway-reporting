"""APP-2a — Ménages : rapprochement en lecture, 4 flux séparés, écran de contrôle.

Toutes les données de ces tests viennent de fixtures Excel construites en tmp_path.
Aucun fichier métier réel n'est ouvert, et aucun n'est modifié : le dernier test le
vérifie par empreinte SHA256.

Règles vérifiées :
- les flux Hostaway / interne / externe ne sont jamais fusionnés ;
- le coût Hostaway n'est jamais utilisé comme coût réel ;
- l'application n'invente ni statut, ni ménage attendu ;
- une source absente, un onglet absent et une source vide sont trois états distincts.
"""
import hashlib
from pathlib import Path

import openpyxl
import pytest

from app.db.connection import apply_migrations, get_db
from app.readers import menages_reader as reader
from app.services import menages_service as svc


# ---------------------------------------------------------------------------
# Construction des fixtures Excel
# ---------------------------------------------------------------------------

COLONNES_RAPPROCHEMENT = [
    "mois", "nom_appartement", "logement_id", "proprietaire_id", "intervenant_id",
    "nom_intervenant", "type_intervenant", "source_mapping_hostaway",
    "nb_menages_tasks_hostaway_completed", "nb_menages_declares_externe",
    "nb_menages_declares_interne_m04", "total_menages_declares", "ecart",
    "statut_controle", "code_controle", "commentaire",
]

COLONNES_HA = [
    "task_id", "ROW_HASH", "mois", "logement_id", "proprietaire_id", "listingMapId",
    "reservation_id", "scheduled_date", "title", "status", "statut_menage",
    "type_ligne_menage_id", "type_ligne_menage_lib", "compte_comme_menage", "cost",
    "h6_note", "statut_controle", "niveau_anomalie", "code_anomalie", "extrait_le",
    "date_integration",
]

COLONNES_INTERNES = [
    "mois", "annee", "mois_saisie", "appartement_source", "nom_appartement",
    "logement_id", "intervenant_source", "intervenant_id", "nom_intervenant",
    "type_intervenant", "nb_menages", "nb_heures", "cout_lavage_attribue",
    "lavage_non_attribuable_mois", "statut_controle", "code_controle", "source_url",
    "date_extraction", "ROW_HASH",
]

COLONNES_EXTERNES = [
    "menage_externe_id", "facture_id", "date_facture", "date_menage",
    "precision_date_menage", "mois", "nom_fichier_source", "prestataire_id",
    "nom_prestataire", "logement_id", "type_ligne_menage_id", "nombre_menages",
    "montant_ligne_ttc", "statut_controle", "niveau_anomalie", "code_anomalie",
]

COLONNES_GAINPERTE = [
    "mois", "nom_appartement", "logement_id", "intervenant_id", "nom_intervenant",
    "type_intervenant", "nb_menages", "nb_heures", "cout_standard_unitaire",
    "cout_standard_total", "methode_cout_reel", "cout_reel_unitaire", "cout_reel_total",
    "ecart_total", "statut_ecart", "statut_controle", "code_controle", "commentaire",
]

COLONNES_COUTCOMPLET = [
    "mois", "logement_id", "nom_appartement", "intervenant_id", "nom_intervenant",
    "nb_menages", "cout_standard_total", "cout_direct_total", "quote_part_local",
    "quote_part_courses", "quote_part_lavage", "quote_part_consommables",
    "cout_complet_total", "cout_complet_unitaire", "ecart_vs_standard_total",
    "statut_controle", "code_controle", "commentaire",
]


def _ecrire(chemin: Path, onglets: dict[str, tuple[list[str], list[dict]]]) -> Path:
    """Écrit un classeur de test. Utilisé UNIQUEMENT pour fabriquer des fixtures."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for nom, (colonnes, lignes) in onglets.items():
        ws = wb.create_sheet(nom)
        ws.append(colonnes)
        for l in lignes:
            ws.append([l.get(c) for c in colonnes])
    wb.save(str(chemin))
    return chemin


def _inserer_lignes(conn, table: str, lignes: list[dict]) -> None:
    """Insère des dicts tels quels : seules les clés présentes sont écrites, les colonnes absentes
    gardent leur défaut SQLite (`run_id`/`date_calcul` notamment)."""
    for l in lignes:
        cols = list(l.keys())
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
            [l[c] for c in cols])


def _seeder_sqlite_menages(db_path: Path, *, rapp=(), gainperte=(), coutcomplet=()) -> None:
    """Alimente les sorties Lot6a/6b/6d/6e/6f (0038) — sources SQLite de `hostaway_taches`/
    `hostaway_comptage`/`internes`/`rapprochement`/`gainperte`/`cout_complet`, qui ne lisent plus les
    classeurs Excel construits par ailleurs dans cette fixture (conservés pour les sources non
    encore migrées : externes, controles, lot11). `rapp`/`gainperte`/`coutcomplet` sont les MÊMES
    listes de lignes que celles écrites dans les classeurs correspondants — une seule vérité, deux
    supports, le temps de la transition.

    Comptage Hostaway : LOG_0001 → 10 tâches, 9 réalisées + 1 annulée — cohérent, contrairement au
    classeur legacy où MASTER_ENRICHI (1 tâche) et VUE_COMPTAGE (10) divergeaient sans lien entre
    eux (`hostaway_comptage()` dérive désormais l'agrégat des tâches, il ne peut plus diverger).
    """
    conn = get_db(db_path)
    try:
        taches = [("réalisé", "OUI") for _ in range(9)] + [("annulé", "OUI")]
        for i, (statut_menage, ccm) in enumerate(taches, start=1):
            conn.execute(
                "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, "
                "status, statut_menage, compte_comme_menage, statut_controle) "
                "VALUES (?,?,?,?,?,?,?)",
                (f"HA-LOG1-{i:03d}", "2026-05", "LOG_0001",
                 "completed" if statut_menage == "réalisé" else "cancelled",
                 statut_menage, ccm, "OK"))
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage, statut_controle) VALUES (?,?,?,?,?,?,?)",
            ("HA-LOG5-001", "2026-05", "LOG_0005", "completed", "réalisé", "OUI", "OK"))

        for mois, logement_id, intervenant_id, nom, nb_menages, nb_heures, lavage in (
            ("2026-05", "LOG_0001", "INT_0002", "Kheira", 9, 18, 40),
            ("2026-05", "LOG_0020", "INT_0002", "Kheira", 2, 4, None),
        ):
            conn.execute(
                "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
                "nom_intervenant, type_intervenant, nb_menages, nb_heures, cout_lavage_attribue, "
                "statut_controle) VALUES (?,?,?,?,?,?,?,?,?)",
                (mois, logement_id, intervenant_id, nom, "INTERNE", nb_menages, nb_heures, lavage,
                 "VALIDE"))

        _inserer_lignes(conn, "menages_rapprochement", list(rapp))
        _inserer_lignes(conn, "menages_gainperte", list(gainperte))
        _inserer_lignes(conn, "menages_cout_complet", list(coutcomplet))
        conn.commit()
    finally:
        conn.close()


def _seeder_externes_sqlite(db_path: Path) -> None:
    """Trois lignes MENAGE_EXTERNE via les VRAIS services (`factures_service`/
    `facture_lignes_menage_service`), mêmes valeurs que l'ancien onglet MASTER Lot6c — exerce le
    chemin réel (`menages_reader.externes()` → `lignes_externes_pour_reader`), pas un raccourci SQL.
    Nécessite `FACTURES_REAL_WRITE_ENABLED`/`_CONFIRMATION_ENABLED` actifs (appelant)."""
    from app.services import facture_lignes_menage_service as flm
    from app.services import factures_service as fact

    for ref, logement_id, date_menage, precision, nb, montant in (
        ("FAC-2026-05-001", "LOG_0014", "2026-05-10", "DATE_PRECISE", 1, 29.0),
        ("FAC-2026-05-002", "LOG_0009", None, "MOIS_SEUL", 2, 58.0),
        ("FAC-2026-05-003", "LOG_0020", "2026-05-12", "DATE_PRECISE", 3, 87.0),
    ):
        r = fact.creer(
            {"fournisseur_id_opaque": "INT_0004", "facture_ref": ref,
             "date_facture": "2026-05-31", "montant_ttc": montant}, db_path=db_path)
        assert r["ok"], r
        flm.ajouter_ligne(
            r["facture_id_opaque"], type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id=logement_id,
            montant_ttc=montant, quantite=nb, date_menage=date_menage or "",
            precision_date_menage=precision, nom_prestataire="Aissata", db_path=db_path)


def _ligne_rapprochement(**kw) -> dict:
    base = {
        "mois": "2026-05", "nom_appartement": "Studio Test", "logement_id": "LOG_0001",
        "proprietaire_id": "PROP_0001", "intervenant_id": "INT_0002",
        "nom_intervenant": "Kheira", "type_intervenant": "INTERNE",
        "source_mapping_hostaway": "assigneeUserId",
        "nb_menages_tasks_hostaway_completed": 0, "nb_menages_declares_externe": 0,
        "nb_menages_declares_interne_m04": 0, "total_menages_declares": 0, "ecart": 0,
        "statut_controle": "VALIDE", "code_controle": None, "commentaire": None,
    }
    base.update(kw)
    return base


@pytest.fixture
def sources(tmp_path, monkeypatch):
    """Jeu de sources ménages complet et isolé. Couvre les scénarios 1 à 6."""
    import app.config as cfg

    LIGNES_RAPP = [
        # 1. conforme, interne rapproché
        _ligne_rapprochement(
            logement_id="LOG_0001", intervenant_id="INT_0002", type_intervenant="INTERNE",
            nb_menages_tasks_hostaway_completed=9, nb_menages_declares_interne_m04=9,
            total_menages_declares=9, ecart=0, statut_controle="VALIDE"),
        # 2. attendu Hostaway sans déclaration
        _ligne_rapprochement(
            logement_id="LOG_0005", nom_appartement="Studio 96", intervenant_id="INT_0002",
            nb_menages_tasks_hostaway_completed=6, nb_menages_declares_interne_m04=5,
            total_menages_declares=5, ecart=-1, statut_controle="A_CONTROLER",
            code_controle="MENAGE_TOTAL_ECART_HOSTAWAY",
            commentaire="1 ménage Hostaway non déclaré."),
        # 3. déclaration sans tâche Hostaway
        _ligne_rapprochement(
            logement_id="LOG_0009", nom_appartement="T2 hors HA", intervenant_id="INT_0004",
            nom_intervenant="Aissata", type_intervenant="EXTERNE",
            nb_menages_tasks_hostaway_completed=0, nb_menages_declares_externe=2,
            total_menages_declares=2, ecart=2, statut_controle="A_CONTROLER",
            code_controle="MENAGE_EXTERNE_LOGEMENT_HORS_HA",
            commentaire="Logement absent du comptage Hostaway."),
        # 4/5. externe facturé rapproché
        _ligne_rapprochement(
            logement_id="LOG_0014", nom_appartement="T3 Aissata", intervenant_id="INT_0004",
            nom_intervenant="Aissata", type_intervenant="EXTERNE",
            nb_menages_tasks_hostaway_completed=1, nb_menages_declares_externe=1,
            total_menages_declares=1, ecart=0, statut_controle="VALIDE"),
        # 6. interne ET externe sur le même logement : deux lignes distinctes
        _ligne_rapprochement(
            logement_id="LOG_0020", nom_appartement="Mixte", intervenant_id="INT_0002",
            nom_intervenant="Kheira", type_intervenant="INTERNE",
            nb_menages_tasks_hostaway_completed=2, nb_menages_declares_interne_m04=2,
            total_menages_declares=2, ecart=0, statut_controle="VALIDE"),
        _ligne_rapprochement(
            logement_id="LOG_0020", nom_appartement="Mixte", intervenant_id="INT_0004",
            nom_intervenant="Aissata", type_intervenant="EXTERNE",
            nb_menages_tasks_hostaway_completed=3, nb_menages_declares_externe=3,
            total_menages_declares=3, ecart=0, statut_controle="VALIDE"),
        # 13. logement inconnu / intervenant non attribué
        _ligne_rapprochement(
            logement_id="LOG_9999", nom_appartement="", intervenant_id="NON_ATTRIBUE",
            nom_intervenant="NON_ATTRIBUE", type_intervenant=None,
            nb_menages_tasks_hostaway_completed=1, total_menages_declares=0, ecart=-1,
            statut_controle="INFO", code_controle="TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE"),
        # 14. anomalie bloquante
        _ligne_rapprochement(
            logement_id="LOG_0030", nom_appartement="Bloquant", intervenant_id="INT_0009",
            nom_intervenant="Inconnu", type_intervenant="EXTERNE",
            nb_menages_tasks_hostaway_completed=4, nb_menages_declares_externe=0,
            total_menages_declares=0, ecart=4, statut_controle="BLOQUANT",
            code_controle="MENAGE_PRESTATAIRE_ECART_HOSTAWAY",
            commentaire="Prestataire facturant sans tâche Hostaway."),
        # 15. autre mois — sert au filtre période
        _ligne_rapprochement(
            mois="2026-04", logement_id="LOG_0001", intervenant_id="INT_0002",
            nb_menages_tasks_hostaway_completed=4, nb_menages_declares_interne_m04=4,
            total_menages_declares=4, ecart=0, statut_controle="VALIDE"),
    ]

    rapp = _ecrire(tmp_path / "Lot6d" / "MASTER_CTRL_Rapprochement_Menages.xlsx", {
        "TABLEAU_COMPARAISON": (COLONNES_RAPPROCHEMENT, LIGNES_RAPP),
        "CONTROLES": (["code_controle", "niveau", "nb", "exemple"], [
            {"code_controle": "MENAGE_TOTAL_ECART_HOSTAWAY", "niveau": "A_CONTROLER",
             "nb": 2, "exemple": "LOG_0005"},
            {"code_controle": "TASK_NON_ASSIGNEE_HISTORIQUE_IGNOREE", "niveau": "INFO",
             "nb": 1, "exemple": "task 1 logement LOG_9999"},
        ]),
    })

    ha = _ecrire(tmp_path / "Lot1" / "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx", {
        "MASTER_ENRICHI": (COLONNES_HA, [
            {"task_id": 111, "mois": "2026-05", "logement_id": "LOG_0001",
             "reservation_id": 54190063, "scheduled_date": "2026-05-17 09:00:00",
             "title": "Ménage Studio Kheira", "status": "completed", "statut_menage": "réalisé",
             "type_ligne_menage_lib": "MENAGE_STANDARD", "compte_comme_menage": "OUI",
             # Coût Hostaway volontairement renseigné : il ne doit JAMAIS ressortir.
             "cost": 999, "statut_controle": "OK"},
            {"task_id": 112, "mois": "2026-05", "logement_id": "LOG_0005",
             "scheduled_date": "2026-05-03 09:00:00", "title": "Ménage 96",
             "status": "completed", "statut_menage": "réalisé", "compte_comme_menage": "OUI",
             "cost": 555, "statut_controle": "OK"},
        ]),
        "VUE_COMPTAGE": (["mois", "logement_id", "proprietaire_id", "nb_menages_realises",
                          "nb_menages_confirmes", "nb_menages_pending", "nb_menages_annules",
                          "nb_taches_total", "statut_controle"], [
            {"mois": "2026-05", "logement_id": "LOG_0001", "nb_menages_realises": 9,
             "nb_menages_pending": 0, "nb_menages_annules": 1, "nb_taches_total": 10,
             "statut_controle": "OK"},
        ]),
    })

    internes = _ecrire(tmp_path / "Lot6b" / "MASTER_NORM_Declarations_Internes.xlsx", {
        "MASTER_NORMALISE": (COLONNES_INTERNES, [
            {"mois": "2026-05", "logement_id": "LOG_0001", "intervenant_id": "INT_0002",
             "nom_intervenant": "Kheira", "type_intervenant": "INTERNE", "nb_menages": 9,
             "nb_heures": 18, "cout_lavage_attribue": 40, "statut_controle": "VALIDE"},
            {"mois": "2026-05", "logement_id": "LOG_0020", "intervenant_id": "INT_0002",
             "nom_intervenant": "Kheira", "type_intervenant": "INTERNE", "nb_menages": 2,
             "nb_heures": 4, "statut_controle": "VALIDE"},
        ]),
    })

    externes = _ecrire(tmp_path / "Lot6c" / "MASTER_FACT_MEN_MenagesExternes.xlsx", {
        "MASTER": (COLONNES_EXTERNES, [
            {"menage_externe_id": "MENEXT-001", "facture_id": "FAC-2026-05-001",
             "date_facture": "2026-05-31", "date_menage": "2026-05-10",
             "precision_date_menage": "DATE_PRECISE", "mois": "2026-05",
             "nom_fichier_source": "Facture mai Aissata.pdf", "prestataire_id": "INT_0004",
             "nom_prestataire": "Aissata", "logement_id": "LOG_0014",
             "type_ligne_menage_id": "TLM_001", "nombre_menages": 1,
             "montant_ligne_ttc": 29, "statut_controle": "VALIDE"},
            # 12. date de ménage absente
            {"menage_externe_id": "MENEXT-002", "facture_id": "FAC-2026-05-002",
             "date_facture": "2026-05-31", "date_menage": None,
             "precision_date_menage": "MOIS_SEUL", "mois": "2026-05",
             "nom_fichier_source": "Facture mai Aissata.pdf", "prestataire_id": "INT_0004",
             "nom_prestataire": "Aissata", "logement_id": "LOG_0009",
             "type_ligne_menage_id": "TLM_001", "nombre_menages": 2,
             "montant_ligne_ttc": 58, "statut_controle": "A_CONTROLER",
             "code_anomalie": "DATE_MENAGE_ABSENTE"},
            {"menage_externe_id": "MENEXT-003", "facture_id": "FAC-2026-05-003",
             "date_menage": "2026-05-12", "precision_date_menage": "DATE_PRECISE",
             "mois": "2026-05", "prestataire_id": "INT_0004", "nom_prestataire": "Aissata",
             "logement_id": "LOG_0020", "type_ligne_menage_id": "TLM_001",
             "nombre_menages": 3, "montant_ligne_ttc": 87, "statut_controle": "VALIDE"},
        ]),
    })

    LIGNES_GAINPERTE = [
        {"mois": "2026-05", "logement_id": "LOG_0001", "intervenant_id": "INT_0002",
         "nb_menages": 9, "nb_heures": 18, "cout_standard_total": 261,
         "methode_cout_reel": "INTERNE_HEURES_M04", "cout_reel_total": 240,
         "ecart_total": -21, "statut_ecart": "GAIN", "statut_controle": "VALIDE"},
        {"mois": "2026-05", "logement_id": "LOG_0014", "intervenant_id": "INT_0004",
         "nb_menages": 1, "cout_standard_total": 29,
         "methode_cout_reel": "EXTERNE_FACTURE", "cout_reel_total": 29,
         "ecart_total": 0, "statut_ecart": "EQUILIBRE", "statut_controle": "VALIDE"},
    ]
    gainperte = _ecrire(tmp_path / "Lot6e" / "MASTER_CALC_GainPerte_Menages.xlsx", {
        "DETAIL_ECART_COUT": (COLONNES_GAINPERTE, LIGNES_GAINPERTE),
    })

    LIGNES_COUTCOMPLET = [
        {"mois": "2026-05", "logement_id": "LOG_0001", "intervenant_id": "INT_0002",
         "nb_menages": 9, "cout_standard_total": 261, "cout_direct_total": 240,
         "quote_part_lavage": 30, "quote_part_local": 10, "cout_complet_total": 280,
         "cout_complet_unitaire": 31.11, "statut_controle": "VALIDE"},
        {"mois": "2026-05", "logement_id": "LOG_0014", "intervenant_id": "INT_0004",
         "nb_menages": 1, "cout_standard_total": 29, "cout_direct_total": 29,
         "cout_complet_total": 35, "cout_complet_unitaire": 35, "statut_controle": "VALIDE"},
    ]
    coutcomplet = _ecrire(tmp_path / "Lot6f" / "MASTER_CALC_CoutComplet_Menages.xlsx", {
        "DETAIL_COUT_COMPLET": (COLONNES_COUTCOMPLET, LIGNES_COUTCOMPLET),
        "POOLS_CHARGES_MENAGE": (["mois", "pool", "montant_total", "source", "cle_repartition"], [
            {"mois": "2026-05", "pool": "LAVAGE", "montant_total": 307, "source": "Google Sheet"},
        ]),
    })

    lot11 = _ecrire(tmp_path / "Lot11" / "MASTER_CTRL_Coherence.xlsx", {
        "MASTER": (["ctrl_pk", "source_module", "source_table", "code_controle", "severity",
                    "message", "statut_resolution", "commentaire"], [
            {"ctrl_pk": "MENAGES_EXT||X", "source_module": "MENAGES_EXT",
             "code_controle": "MENAGE_EXTERNE_ECART_HOSTAWAY", "severity": "A_CONTROLER",
             "message": "1 logement avec écart volume.", "statut_resolution": "OUVERT"},
            {"ctrl_pk": "BANQUE||Y", "source_module": "BANQUE",
             "code_controle": "BANQUE_NON_RAPPROCHEE", "severity": "A_CONTROLER",
             "message": "Hors module ménages.", "statut_resolution": "OUVERT"},
        ]),
    })

    monkeypatch.setattr(cfg, "MASTER_RAPPROCHEMENT_MENAGES", rapp)
    monkeypatch.setattr(cfg, "MASTER_HA_CLEANINGTASKS", ha)
    monkeypatch.setattr(cfg, "MASTER_DECLARATIONS_INTERNES", internes)
    monkeypatch.setattr(cfg, "MASTER_MENAGES_EXTERNES", externes)
    monkeypatch.setattr(cfg, "MASTER_GAINPERTE_MENAGES", gainperte)
    monkeypatch.setattr(cfg, "MASTER_COUTCOMPLET_MENAGES", coutcomplet)
    monkeypatch.setattr(cfg, "MASTER_CTRL_COHERENCE", lot11)
    reader.vider_cache()

    db_path = tmp_path / "test.db"
    apply_migrations(db_path)
    monkeypatch.setattr(svc, "get_db", lambda *_a, **_k: get_db(db_path))
    # `menages_reader.hostaway_taches`/`hostaway_comptage`/`internes` lisent SQLite (0038), sans
    # repli Excel : cfg.DB_PATH doit pointer ici pour qu'ils voient les mêmes données que les
    # classeurs `ha`/`internes` ci-dessus (conservés pour les autres sources, encore Excel).
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    _seeder_sqlite_menages(db_path, rapp=LIGNES_RAPP, gainperte=LIGNES_GAINPERTE,
                          coutcomplet=LIGNES_COUTCOMPLET)
    _seeder_externes_sqlite(db_path)

    yield {
        "racine": tmp_path, "rapprochement": rapp, "hostaway": ha, "internes": internes,
        "externes": externes, "gainperte": gainperte, "coutcomplet": coutcomplet,
        "lot11": lot11, "db": db_path,
    }
    reader.vider_cache()


# ---------------------------------------------------------------------------
# 1 à 7 — scénarios de rapprochement
# ---------------------------------------------------------------------------

def test_01_rapprochement_conforme(sources):
    """Ligne parfaitement rapprochée : écart 0, statut moteur VALIDE."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0001", "INT_0002")
    assert r["hostaway_realise"] == 9
    assert r["interne_declare"] == 9
    assert r["ecart"] == 0
    assert r["statut_effectif"] == "VALIDE"


def test_02_menage_hostaway_sans_declaration(sources):
    """Tâche Hostaway réalisée non déclarée → écart négatif, statut moteur repris tel quel."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0005", "INT_0002")
    assert r["hostaway_realise"] == 6
    assert r["interne_declare"] == 5
    assert r["ecart"] == -1
    assert r["statut_effectif"] == "A_CONTROLER"
    assert r["code_controle"] == "MENAGE_TOTAL_ECART_HOSTAWAY"


def test_03_declaration_sans_tache_hostaway(sources):
    """Facture externe sans tâche Hostaway : la ligne existe et reste à contrôler."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0009", "INT_0004")
    assert r["hostaway_realise"] == 0
    assert r["externe_facture"] == 2
    assert r["ecart"] == 2
    assert r["statut_effectif"] == "A_CONTROLER"


def test_04_interne_rapproche_detail(sources):
    """Fiche : le bloc interne est alimenté par M04, avec heures et coût de lavage."""
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    assert detail["interne"]["lignes"], "Déclaration interne attendue"
    ligne = detail["interne"]["lignes"][0]
    assert ligne["nb_menages"] == 9
    assert ligne["nb_heures"] == 18
    assert detail["externe"]["lignes"] == []


def test_05_externe_rapproche_detail(sources):
    """Fiche : le bloc externe porte la facture, son fichier et son montant."""
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0014", "INT_0004")
    assert len(detail["externe"]["lignes"]) == 1
    ligne = detail["externe"]["lignes"][0]
    assert ligne["facture_id"] == "FAC-2026-05-001"
    assert ligne["montant_ligne_ttc"] == 29
    assert detail["interne"]["lignes"] == []


def test_06_interne_et_externe_restent_distincts(sources):
    """Un même logement servi en interne ET en externe : deux lignes, jamais fusionnées."""
    lignes = [r for r in svc.load_reconciliation_rows(mois="2026-05")["rows"]
              if r["logement_id"] == "LOG_0020"]
    assert len(lignes) == 2
    interne = next(r for r in lignes if r["type_intervenant"] == "INTERNE")
    externe = next(r for r in lignes if r["type_intervenant"] == "EXTERNE")
    assert interne["interne_declare"] == 2 and interne["externe_facture"] == 0
    assert externe["externe_facture"] == 3 and externe["interne_declare"] == 0
    # Aucun champ agrégeant les deux flux réels.
    assert "nb_menages_total_fusionne" not in interne


def test_07_cout_hostaway_jamais_utilise_comme_cout_reel(sources):
    """La colonne `cost` d'Hostaway (999 €) ne remonte jamais en coût réel."""
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    assert detail["cout"]["cout_reel_total"] == 240          # vient de Lot6e
    assert detail["cout"]["methode_cout_reel"] == "INTERNE_HEURES_M04"

    valeurs = _valeurs_profondes(detail)
    assert 999 not in valeurs, "Le coût Hostaway ne doit jamais apparaître dans la fiche"
    assert 555 not in valeurs

    for tache in detail["hostaway"]["taches"]:
        assert "cost" not in tache
        assert "cout" not in " ".join(tache.keys())


def test_07b_cout_reel_et_cout_complet_ne_sont_pas_confondus(sources):
    """Deux notions distinctes du moteur : coût réel (Lot6e) ≠ coût complet (Lot6f)."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0001", "INT_0002")
    assert r["cout_reel"] == 240        # Lot6e — heures M04
    assert r["cout_complet"] == 280     # Lot6f — + quotes-parts (30 lavage + 10 local)
    assert r["cout_standard"] == 261

    s = svc.load_summary("2026-05")
    assert s["cout_reel_total"] == 240 + 29
    assert s["cout_complet_total"] == 280 + 35


# ---------------------------------------------------------------------------
# 8 à 14 — sources dégradées
# ---------------------------------------------------------------------------

def test_08_source_interne_vide(sources, tmp_path, monkeypatch):
    """Source interne vide (table sans ligne) : état VIDE, jamais un plantage."""
    conn = get_db(sources["db"])
    try:
        conn.execute("DELETE FROM menages_declarations_internes")
        conn.commit()
    finally:
        conn.close()
    reader.vider_cache()

    assert reader.internes().etat.etat == reader.ETAT_VIDE
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    assert detail["status"] == "OK"
    assert detail["interne"]["lignes"] == []
    assert not detail["interne"]["etat"].disponible


def test_09_source_externe_vide(sources):
    """Aucune ligne MENAGE_EXTERNE en base : état VIDE, jamais un plantage."""
    conn = get_db(sources["db"])
    try:
        conn.execute("DELETE FROM facture_lignes_menage WHERE type_ligne = 'MENAGE_EXTERNE'")
        conn.commit()
    finally:
        conn.close()
    reader.vider_cache()

    assert reader.externes().etat.etat == reader.ETAT_VIDE
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0014", "INT_0004")
    assert detail["status"] == "OK"
    assert detail["externe"]["lignes"] == []


def test_10_source_sqlite_vide(sources, tmp_path, monkeypatch):
    """Table SQLite sans ligne (0038) : état VIDE, jamais un plantage — plus de notion de
    « fichier »/« onglet » absent depuis que `cout_complet()` lit SQLite (0038), sans repli Excel."""
    conn = get_db(sources["db"])
    try:
        conn.execute("DELETE FROM menages_cout_complet")
        conn.commit()
    finally:
        conn.close()
    reader.vider_cache()

    etat = reader.cout_complet().etat
    assert etat.etat == reader.ETAT_VIDE
    assert not etat.disponible

    resume = svc.load_summary("2026-05")
    assert resume["cout_complet_disponible"] is False
    assert resume["cout_complet_total"] is None
    assert resume["etat_global"] == "SOURCE_INCOMPLETE"


def test_11_gainperte_vide_detail_sans_cout_reel(sources):
    """Même règle pour `gainperte()` : source vide, la fiche reste utilisable, sans coût réel."""
    conn = get_db(sources["db"])
    try:
        conn.execute("DELETE FROM menages_gainperte")
        conn.commit()
    finally:
        conn.close()
    reader.vider_cache()

    etat = reader.gainperte().etat
    assert etat.etat == reader.ETAT_VIDE

    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    assert detail["status"] == "OK"
    assert detail["cout"]["cout_reel_total"] is None


def test_12_date_externe_absente(sources):
    """Une facture sans date de ménage est signalée, pas complétée d'office."""
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0009", "INT_0004")
    ligne = detail["externe"]["lignes"][0]
    assert ligne["date_absente"] is True
    assert ligne["date_menage"] == ""
    assert ligne["code_anomalie"] == "DATE_MENAGE_ABSENTE"


def test_13_logement_inconnu_identification_incomplete(sources):
    """Intervenant NON_ATTRIBUE : la ligne est marquée « identification incomplète »."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_9999", "NON_ATTRIBUE")
    assert r["identification_incomplete"] is True
    assert r["type_intervenant"] == ""

    detail = svc.load_reconciliation_detail("2026-05", "LOG_9999", "NON_ATTRIBUE")
    codes = [a["code"] for a in detail["anomalies"]]
    assert "IDENTIFICATION_INCOMPLETE" in codes


def test_14_anomalie_bloquante(sources):
    """Un BLOQUANT du moteur remonte tel quel et apparaît dans l'écran de contrôle."""
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0030", "INT_0009")
    assert r["statut_effectif"] == "BLOQUANT"

    anomalies = svc.load_anomalies("2026-05")
    cles = {(l["logement_id"], l["intervenant_id"]) for l in anomalies["lignes"]}
    assert ("LOG_0030", "INT_0009") in cles


# ---------------------------------------------------------------------------
# 15 à 19 — filtres, pagination, fiche
# ---------------------------------------------------------------------------

def test_15_filtre_periode(sources):
    mai = svc.load_reconciliation_rows(mois="2026-05")
    avril = svc.load_reconciliation_rows(mois="2026-04")
    assert {r["mois"] for r in mai["rows"]} == {"2026-05"}
    assert {r["mois"] for r in avril["rows"]} == {"2026-04"}
    assert avril["count_filtre"] == 1
    assert svc.load_available_periods() == ["2026-05", "2026-04"]


def test_16_filtre_logement(sources):
    res = svc.load_reconciliation_rows(mois="2026-05", logement_id="LOG_0020")
    assert res["count_filtre"] == 2
    assert {r["logement_id"] for r in res["rows"]} == {"LOG_0020"}


def test_17_filtre_avec_ecarts(sources):
    res = svc.load_reconciliation_rows(mois="2026-05", ecart_seul=True)
    assert res["count_filtre"] == 4
    assert all(r["ecart"] != 0 for r in res["rows"])


def test_17b_filtres_intervenant_type_et_statut(sources):
    par_intervenant = svc.load_reconciliation_rows(mois="2026-05", intervenant_id="INT_0004")
    assert {r["intervenant_id"] for r in par_intervenant["rows"]} == {"INT_0004"}

    par_type = svc.load_reconciliation_rows(mois="2026-05", type_intervenant="INTERNE")
    assert {r["type_intervenant"] for r in par_type["rows"]} == {"INTERNE"}

    par_statut = svc.load_reconciliation_rows(mois="2026-05", statut="BLOQUANT")
    assert par_statut["count_filtre"] == 1

    incomplets = svc.load_reconciliation_rows(mois="2026-05", identification_incomplete=True)
    assert all(r["identification_incomplete"] for r in incomplets["rows"])
    assert incomplets["count_filtre"] == 1


def test_18_pagination(sources, monkeypatch):
    monkeypatch.setattr(svc, "TAILLE_PAGE", 3)
    p1 = svc.load_reconciliation_rows(mois="2026-05", page=1)
    p2 = svc.load_reconciliation_rows(mois="2026-05", page=2)

    assert p1["pages"] == 3 and p1["count_filtre"] == 8
    assert len(p1["rows"]) == 3 and len(p2["rows"]) == 3
    assert {(r["logement_id"], r["intervenant_id"]) for r in p1["rows"]} \
        .isdisjoint({(r["logement_id"], r["intervenant_id"]) for r in p2["rows"]})

    # Une page hors bornes est ramenée dans les bornes, jamais une erreur.
    assert svc.load_reconciliation_rows(mois="2026-05", page=99)["page"] == 3


def test_18b_tri_anomalies_en_premier(sources):
    rows = svc.load_reconciliation_rows(mois="2026-05", tri="anomalie")["rows"]
    statuts = [r["statut_effectif"] for r in rows]
    premiers = statuts[:3]
    assert all(s in ("A_CONTROLER", "BLOQUANT") for s in premiers), statuts


def test_19_fiche_detail_complete(sources):
    """La fiche porte les 7 blocs, avec la source de chaque valeur."""
    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    for bloc in ("vue", "attendu", "hostaway", "interne", "externe", "cout",
                 "anomalies", "tracabilite"):
        assert bloc in detail, f"Bloc manquant : {bloc}"

    assert detail["attendu"]["etat"].etat == reader.ETAT_NON_ALIMENTE
    assert detail["attendu"]["comptage_hostaway"]["planifiees"] == 10
    assert len(detail["hostaway"]["taches"]) == 10
    assert detail["tracabilite"]["sources"]

    # Aucun chemin absolu : uniquement des noms de fichiers.
    for etat in detail["tracabilite"]["sources"]:
        assert "\\" not in etat.fichier and "/" not in etat.fichier


def test_19b_fiche_inconnue_retourne_none(sources):
    assert svc.load_reconciliation_detail("2026-05", "LOG_INCONNU", "INT_INCONNU") is None


def test_19c_synthese_sans_invention(sources):
    """Les cartes somment des colonnes du moteur ; l'attendu reste vide."""
    s = svc.load_summary("2026-05")
    assert s["attendu"] is None
    assert s["hostaway_realise"] == 9 + 6 + 0 + 1 + 2 + 3 + 1 + 4
    assert s["interne_declare"] == 9 + 5 + 2
    assert s["externe_facture"] == 2 + 1 + 3
    # Même critère que l'écran « À contrôler » : statuts à contrôler + identification incomplète.
    assert s["a_controler"] == 4   # LOG_0005, LOG_0009, LOG_0030 (BLOQUANT), LOG_9999 (non identifié)
    assert s["a_controler"] == len(svc.load_anomalies("2026-05")["lignes"])
    assert s["etat_global"] == "A_CONTROLER"


def test_19d_outrepassage_ne_modifie_pas_le_statut_moteur(sources):
    """Un outrepassage annote la ligne ; le statut du moteur reste lisible."""
    svc.enregistrer_outrepassage("2026-05", "LOG_0005", "INT_0002", "Ménage fait hors M04")
    r = _ligne(svc.load_reconciliation_rows(mois="2026-05"), "LOG_0005", "INT_0002")
    assert r["statut_moteur"] == "A_CONTROLER"       # le moteur n'est pas réécrit
    assert r["statut_effectif"] == "JUSTIFIE"
    assert r["override"]["motif"] == "Ménage fait hors M04"


def test_19e_anomalies_reprennent_les_controles_moteur(sources):
    a = svc.load_anomalies("2026-05")
    codes = {c["code"] for c in a["controles_moteur"]}
    assert "MENAGE_TOTAL_ECART_HOSTAWAY" in codes

    # Lot11 : uniquement le module ménages, jamais la banque.
    modules = {c["code"] for c in a["controles_lot11"]}
    assert "MENAGE_EXTERNE_ECART_HOSTAWAY" in modules
    assert "BANQUE_NON_RAPPROCHEE" not in modules


def test_19f_dernier_calcul(sources):
    dc = svc.load_dernier_calcul()
    assert dc["recalcul_active"] is False
    assert dc["commande"].endswith("run_menages_pipeline.py")
    assert dc["etapes"][0].startswith("lot6b")
    assert dc["derniere_generation"]


# ---------------------------------------------------------------------------
# 20 — aucune source modifiée
# ---------------------------------------------------------------------------

def test_20_aucune_source_modifiee(sources):
    """Après lectures, filtres, fiche et outrepassage : toutes les sources intactes."""
    fichiers = [sources[c] for c in ("rapprochement", "hostaway", "internes", "externes",
                                     "gainperte", "coutcomplet", "lot11")]
    avant = {f: (hashlib.sha256(f.read_bytes()).hexdigest(), f.stat().st_size) for f in fichiers}

    svc.load_dashboard(mois="2026-05")
    svc.load_reconciliation_rows(mois="2026-05", ecart_seul=True)
    svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    svc.load_anomalies("2026-05")
    svc.enregistrer_outrepassage("2026-05", "LOG_0005", "INT_0002", "Motif de test")

    for f in fichiers:
        apres = (hashlib.sha256(f.read_bytes()).hexdigest(), f.stat().st_size)
        assert apres == avant[f], f"Source modifiée : {f.name} — INTERDIT"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
#
# `client` isole déjà l'application (tmp_db) mais ne construit aucune fixture Ménages : ces tests
# vérifient la ROUTE (statut, libellés statiques, filtres), pas un scénario métier — un jeu minimal
# suffit. Avant la migration SQLite, ces tests lisaient (sans le vouloir) les classeurs RÉELS du
# projet via les chemins par défaut de `cfg.MASTER_*` : une fuite d'isolation, invisible tant que
# `menages_reader` retombait sur Excel. `rapprochement()`/`gainperte()`/`cout_complet()` étant
# désormais SQLite uniquement, cette fuite ne peut plus se produire — d'où ce seed explicite.

@pytest.fixture(autouse=True)
def _seed_route_minimal(request, tmp_db):
    """Une ligne minimale dans les 3 tables SQLite lues par l'écran `/menages`, pour que les tests
    de route (hors fixture `sources`) voient une source disponible plutôt que vide. `sources`
    fournit son propre jeu bien plus riche : ne pas semer par-dessus."""
    if "sources" in request.fixturenames:
        yield
        return
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, statut_controle) VALUES (?,?,?,?,?)",
            ("2026-05", "LOG_0001", "INT_0002", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    yield


def test_route_menages_200(client):
    r = client.get("/menages")
    assert r.status_code == 200
    assert "Ménages" in r.text


def test_route_menages_cartes_presentes(client):
    r = client.get("/menages")
    for libelle in ("Ménages attendus", "Hostaway réalisés", "Internes déclarés",
                    "Externes facturés", "Écarts à contrôler", "Coût complet total"):
        assert libelle in r.text, f"Carte manquante : {libelle}"


def test_route_menages_filtres(client):
    r = client.get("/menages?mois=2026-05&ecart_seul=true&tri=ecart")
    assert r.status_code == 200
    assert "Avec écart uniquement" in r.text


def test_route_menages_pagination_bornee(client):
    assert client.get("/menages?page=999").status_code == 200
    assert client.get("/menages?page=0").status_code == 200


def test_route_a_controler_200(client):
    r = client.get("/menages/a-controler")
    assert r.status_code == 200
    assert "À contrôler" in r.text


def test_route_diagnostic_200(client):
    r = client.get("/menages/diagnostic")
    assert r.status_code == 200
    assert "Diagnostic du pipeline" in r.text
    # Le recalcul est désormais câblé vers une recette sur copies ; le mode RÉEL reste gardé.
    assert "/menages/recalculer" in r.text
    assert "copies" in r.text.lower()
    assert "run_menages_pipeline.py" in r.text


def test_route_detail_inconnu_404(client):
    assert client.get("/menages/9999-99/LOG_X/INT_X").status_code == 404


def test_route_detail_connu_200(client):
    rows = reader.rapprochement().lignes
    if not rows:
        pytest.skip("Aucune donnée réelle de rapprochement")
    r0 = rows[0]
    url = f"/menages/{reader.to_mois(r0['mois'])}/{r0['logement_id']}/{r0['intervenant_id']}"
    assert client.get(url).status_code == 200


def test_route_aucun_chemin_absolu_expose(client):
    """Aucune page ménages n'expose un chemin Windows."""
    for url in ("/menages", "/menages/a-controler", "/menages/diagnostic"):
        texte = client.get(url).text
        assert "C:\\" not in texte and "C:/" not in texte, f"Chemin absolu exposé sur {url}"
        assert "OneDrive" not in texte


def test_route_navigation_sidebar(client):
    r = client.get("/menages")
    assert 'href="/menages"' in r.text
    assert 'class="nav-item active"' in r.text or "nav-item active" in r.text


def test_route_ne_touche_jamais_la_base_reelle(client, tmp_db):
    """Régression : le service lit cfg.DB_PATH à chaud, jamais le défaut figé de get_db().

    Sans cela, ouvrir /menages ouvrait la vraie app.db ; le PRAGMA journal_mode=WAL
    écrit dans l'en-tête, donc le fichier réel bougeait alors qu'aucun test ne l'avait
    demandé. `tmp_db` a déjà basculé cfg.DB_PATH : la base réelle doit rester intacte.
    """
    import app.config as cfg
    reelle = Path(__file__).parent.parent / "data" / "app.db"
    assert cfg.DB_PATH != reelle, "cfg.DB_PATH doit être isolé pendant les tests"
    if not reelle.exists():
        pytest.skip("Aucune base réelle sur ce poste")

    avant = (hashlib.sha256(reelle.read_bytes()).hexdigest(), reelle.stat().st_mtime_ns)

    for url in ("/menages", "/menages/a-controler", "/menages/diagnostic"):
        assert client.get(url).status_code == 200

    apres = (hashlib.sha256(reelle.read_bytes()).hexdigest(), reelle.stat().st_mtime_ns)
    assert apres == avant, "Les pages Ménages ont touché la base réelle — INTERDIT"


# ---------------------------------------------------------------------------
# Gardes structurelles
# ---------------------------------------------------------------------------

def _src(*parties: str) -> str:
    return (Path(__file__).parent.parent / "app" / Path(*parties)).read_text(encoding="utf-8")


def test_structure_aucune_ecriture_openpyxl():
    """Ni le reader ni le service n'ouvrent un classeur en écriture."""
    for module in (("readers", "menages_reader.py"), ("services", "menages_service.py")):
        src = _src(*module)
        assert "import openpyxl" not in src, f"{module} ne doit pas importer openpyxl"
        assert ".save(" not in src
        assert "write_only" not in src


def test_structure_aucun_import_moteur():
    for module in (("readers", "menages_reader.py"), ("services", "menages_service.py"),
                   ("routes", "menages.py")):
        src = _src(*module).lower()
        assert "import lot" not in src and "from lot" not in src


def test_structure_aucune_valorisation_hostaway():
    """Aucun champ de coût Hostaway n'est lu, exposé, ni même nommé comme tel."""
    interdits = ["hostaway_cost", "h6_cost", "cost_hostaway", "cleaning_cost_hostaway",
                 'get("cost")', "['cost']"]
    for module in (("readers", "menages_reader.py"), ("services", "menages_service.py")):
        src = _src(*module).lower()
        for motif in interdits:
            assert motif.lower() not in src, f"Valorisation Hostaway détectée : {motif}"


def test_structure_aucune_table_sqlite_metier():
    """Le service n'écrit que dans menage_overrides et audit_events."""
    src = _src("services", "menages_service.py")
    inserts = [l.strip() for l in src.splitlines() if "INSERT INTO" in l]
    for ligne in inserts:
        assert ("menage_overrides" in ligne or "audit_events" in ligne), \
            f"Écriture SQLite hors périmètre : {ligne}"
    assert "CREATE TABLE" not in src


def test_structure_recalcul_pipeline_non_active():
    """La route diagnostic ne lance aucun pipeline."""
    src = _src("routes", "menages.py")
    assert "run_pipeline" not in src
    assert "subprocess" not in src


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ligne(resultat: dict, logement_id: str, intervenant_id: str) -> dict:
    for r in resultat["rows"]:
        if r["logement_id"] == logement_id and r["intervenant_id"] == intervenant_id:
            return r
    raise AssertionError(f"Ligne absente : {logement_id} / {intervenant_id}")


def _valeurs_profondes(obj) -> list:
    """Aplatit récursivement les valeurs d'un dict/list pour y chercher une valeur interdite."""
    valeurs = []
    if isinstance(obj, dict):
        for v in obj.values():
            valeurs.extend(_valeurs_profondes(v))
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            valeurs.extend(_valeurs_profondes(v))
    else:
        valeurs.append(obj)
    return valeurs

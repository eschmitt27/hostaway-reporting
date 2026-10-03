"""« Exporter les données » — les fichiers reflètent TOUJOURS la dernière version valide.

L'incident : l'écran servait des CSV écrits sur disque par un bouton « Générer l'export » que
personne ne relançait. L'application avançait, les exports restaient au 12/09/2026. Désormais chaque
fichier est construit au clic depuis les datasets actifs ; ces tests le verrouillent par le
parcours HTTP réel, sans jamais toucher au module d'export entre deux lectures :

  1. une version A remplacée par une version B → l'export passe de A à B ;
  2. une ligne ajoutée à la source → l'export compte une ligne de plus ;
  3. une valeur modifiée → la nouvelle valeur sort ;
  4. suppression / archivage → l'export suit exactement la table canonique, sans règle inventée ;
  5. un recalcul qui échoue → l'ancienne version valide reste exportée, jamais un état partiel ;
  6. aucune dépendance à une copie disque, à une date figée, à un ancien fichier ;
  7. TOUTES les tables exportables, une par une.
"""
from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path

import pytest

from app.db.connection import get_db
from app.services import lot13_export_service as svc

APP = Path(__file__).resolve().parents[1] / "app"


# ── Jeu de données ──────────────────────────────────────────────────────────────────────────────

def _flux(conn, flux_id: str, montant: float, mois: str = "2026-09") -> None:
    conn.execute(
        "INSERT INTO flux_unifies (flux_id, row_hash, source_module, source_table, mois, "
        "logement_id, proprietaire_id, sens, montant, run_id, date_calcul) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (flux_id, f"h-{flux_id}", "RES", "reservations_resolues", mois, "LOG_A", "PROP_A",
         "PRODUIT", montant, "RUN9-A", "2026-09-12T17:51:47Z"))


def _activer_lot10(conn, run_id: str, resultat: float, date_calcul: str) -> None:
    """Ce que fait le moteur Lot10 : un NOUVEAU run, activé dans la même transaction que la
    désactivation du précédent (un seul run actif, garanti par le schéma)."""
    conn.execute("UPDATE lot10_runs SET actif = 0 WHERE actif = 1")
    conn.execute("INSERT INTO lot10_runs (run_id, statut, actif, date_calcul) "
                 "VALUES (?, 'SUCCES', 1, ?)", (run_id, date_calcul))
    conn.execute(
        "INSERT INTO lot10_resultats (run_id, mois, logement_id, proprietaire_id, vision, "
        "total_produits, total_charges, resultat, nb_flux) "
        "VALUES (?,'2026-09','LOG_A','PROP_A','REEL', ?, 0, ?, 1)", (run_id, resultat, resultat))


@pytest.fixture
def base(tmp_db):
    """Une version A de chaque source exportable, toutes datées du 12/09/2026 — la date figée de
    l'incident, pour qu'aucune assertion ne puisse passer en la relisant par hasard."""
    conn = get_db(tmp_db)
    try:
        for i in range(100):
            _flux(conn, f"FLX-{i:03d}", 10 + i)
        conn.execute("INSERT INTO flux_unifies_runs (run_id, nb_total, statut, date_calcul) "
                     "VALUES ('RUN9-A', 100, 'OK', '2026-09-12T17:51:47Z')")
        _activer_lot10(conn, "L10-A", 700, "2026-09-12T17:51:49Z")
        conn.execute(
            "INSERT INTO lot10_commissions (run_id, reservation_calc_id, logement_id, "
            "proprietaire_id, mois, payout_calcule, commission_conciergerie, nuits) "
            "VALUES ('L10-A','RES-1','LOG_A','PROP_A','2026-09', 500, 75, 3)")
        conn.execute(
            "INSERT INTO lot10_net_vue_mois (run_id, mois, proprietaire_id, total_payout_mois, "
            "nb_reservations) VALUES ('L10-A','2026-09','PROP_A', 500, 1)")
        conn.execute("INSERT INTO reservations_datasets (dataset_id, etape, actif, statut, "
                     "date_calcul) VALUES ('RDS-A','RESOLUES',1,'SUCCES','2026-09-12T17:51:46Z')")
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, mois, "
            "logement_id, proprietaire_id, montant_retenu, nuits, statut_controle) "
            "VALUES ('RDS-A','RES-1','2026-09','LOG_A','PROP_A', 500, 3, 'VALIDE')")
        conn.execute("INSERT INTO lot12_runs (run_id, statut, actif, date_calcul) "
                     "VALUES ('L12-A','SUCCES',1,'2026-09-12T17:51:50Z')")
        conn.execute(
            "INSERT INTO lot12_prefactures_lignes (run_id, facture_id, ligne_num, type_ligne, "
            "libelle, montant, bloc) VALUES ('L12-A','PREF-A',1,'TOTAL_PAYOUT','Total',500,"
            "'EXPLOITATION')")
        conn.execute(
            "INSERT INTO menages_cout_complet (mois, logement_id, nb_menages, cout_complet_total, "
            "date_calcul) VALUES ('2026-09','LOG_A', 4, 120, '2026-09-12T17:51:48Z')")
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, ecart, date_calcul) "
            "VALUES ('2026-09','LOG_A', 0, '2026-09-12T17:51:48Z')")
        conn.execute(
            "INSERT INTO controles_lot11_constats (ctrl_pk, code_controle, severity, message, "
            "statut_resolution) VALUES ('CTRL-1','CODE_A','A_CONTROLER','msg','OUVERT')")
        conn.execute("INSERT INTO controles_lot11_runs (run_id, source, statut, date_calcul) "
                     "VALUES ('L11-A','SQLITE','SUCCES','2026-09-12T17:51:50Z')")
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES ('IMP','2026-01-01T00:00:00Z','t','t','IMPORTE')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, actif, statut_parc, "
            "import_id) VALUES ('LOG_A','Logement A','OUI','GERE','IMP')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, statut_gestion, import_id) "
            "VALUES ('GES-1','LOG_A','PROP_A','2026-01-01','ACTIVE','IMP')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, mode_facturation, "
            "actif, import_id) VALUES ('PROP_A','Dupont','MENSUEL','OUI','IMP')")
        conn.execute(
            "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
            "taux_commission, date_debut, actif, import_id) "
            "VALUES ('TX-1','PROP_A','LOG_A', 0.2, '2026-01-01','OUI','IMP')")
        conn.commit()
    finally:
        conn.close()
    return tmp_db


@pytest.fixture
def web(base, tmp_path, monkeypatch):
    """Client HTTP sur la base de test. Le dossier Power BI pointe vers un dossier temporaire
    qui contient une COPIE OBSOLÈTE : si l'écran la relisait, les tests le verraient."""
    from fastapi.testclient import TestClient

    import app.config as cfg
    from app.main import app

    perime = tmp_path / "powerbi_perime"
    perime.mkdir()
    for nom, _, _ in svc.EXPORTS:
        (perime / f"{nom}.csv").write_text("colonne\nCOPIE_DU_12_09\n", encoding="utf-8-sig")
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", perime)
    with TestClient(app) as c:
        yield c


def _telecharger(client, nom: str) -> list[list[str]]:
    r = client.get(f"/exports/fichier/{nom}.csv")
    assert r.status_code == 200, r.text
    return list(csv.reader(io.StringIO(r.content.decode("utf-8-sig")), delimiter=";"))


def _colonne(lignes: list[list[str]], nom: str) -> list[str]:
    i = lignes[0].index(nom)
    return [l[i] for l in lignes[1:]]


# ── 1. Ancienne donnée remplacée ────────────────────────────────────────────────────────────────

def test_1_une_nouvelle_version_active_remplace_l_ancienne(web, base):
    assert _colonne(_telecharger(web, "PBI_Resultats_Mensuels"), "resultat") == ["700"]

    conn = get_db(base)
    try:
        _activer_lot10(conn, "L10-B", 900, "2026-10-03T08:00:00Z")
        conn.commit()
    finally:
        conn.close()

    # Ce que fait l'orchestrateur au succès du recalcul.
    from app.services import orchestrateur_service as orch
    orch.marquer_dataset("LOT10", orch.ST_A_JOUR, run_id="ORCH-B", db_path=base)

    # Aucune action sur le module d'export : le fichier suivant est déjà B.
    assert _colonne(_telecharger(web, "PBI_Resultats_Mensuels"), "resultat") == ["900"]
    page = web.get("/exports").text
    assert "08h00 · 03/10/2026" in page, "la date de génération affichée est celle de la version B"
    inv = {e["nom"]: e for e in svc.inventaire(db_path=base)["exports"]}
    assert inv["PBI_Resultats_Mensuels"]["etat"] == orch.ST_A_JOUR
    assert inv["PBI_Resultats_Mensuels"]["actualise_le"], "dernière actualisation = orchestrateur"
    assert inv["PBI_Referentiel_Logements"]["actualise_le"] is None, "rien d'inventé"


# ── 2. Nouvelle ligne ───────────────────────────────────────────────────────────────────────────

def test_2_une_ligne_ajoutee_a_la_source_apparait_dans_l_export(web, base):
    assert len(_telecharger(web, "PBI_Flux")) - 1 == 100

    conn = get_db(base)
    try:
        _flux(conn, "FLX-100", 999)
        conn.commit()
    finally:
        conn.close()

    lignes = _telecharger(web, "PBI_Flux")
    assert len(lignes) - 1 == 101
    assert "FLX-100" in _colonne(lignes, "flux_id")
    inv = {e["nom"]: e for e in svc.inventaire(db_path=base)["exports"]}
    assert inv["PBI_Flux"]["nb_lignes"] == 101, "la volumétrie affichée suit, elle aussi"


# ── 3. Modification ─────────────────────────────────────────────────────────────────────────────

def test_3_une_valeur_modifiee_sort_dans_l_export(web, base):
    assert _colonne(_telecharger(web, "PBI_Referentiel_Proprietaires"),
                    "nom_proprietaire") == ["Dupont"]

    conn = get_db(base)
    try:
        conn.execute("UPDATE ref_proprietaires SET nom_proprietaire = 'Dupont-Martin' "
                     "WHERE proprietaire_id = 'PROP_A'")
        conn.commit()
    finally:
        conn.close()

    assert _colonne(_telecharger(web, "PBI_Referentiel_Proprietaires"),
                    "nom_proprietaire") == ["Dupont-Martin"]


# ── 4. Suppression / archivage : le contrat de la table canonique, rien d'autre ─────────────────

def test_4_suppression_et_archivage_suivent_la_table_canonique(web, base):
    conn = get_db(base)
    try:
        # Un flux qui disparaît du recalcul disparaît de la table…
        conn.execute("DELETE FROM flux_unifies WHERE flux_id = 'FLX-000'")
        # … un logement sorti du parc reste en table, marqué inactif.
        conn.execute("UPDATE ref_logements SET actif = 'NON', statut_parc = 'SORTI' "
                     "WHERE logement_id = 'LOG_A'")
        conn.commit()
    finally:
        conn.close()

    assert "FLX-000" not in _colonne(_telecharger(web, "PBI_Flux"), "flux_id")
    logements = _telecharger(web, "PBI_Referentiel_Logements")
    assert _colonne(logements, "logement_id") == ["LOG_A"], "l'export ne filtre rien de plus"
    assert _colonne(logements, "actif") == ["NON"]
    assert _colonne(logements, "statut_parc") == ["SORTI"]


# ── 5. Échec de recalcul ────────────────────────────────────────────────────────────────────────

def test_5_un_recalcul_en_echec_laisse_la_derniere_version_valide(web, base):
    from app.services import orchestrateur_service as orch

    avant_flux = web.get("/exports/fichier/PBI_Flux.csv").content
    avant_res = web.get("/exports/fichier/PBI_Resultats_Mensuels.csv").content

    # Table remplacée en une transaction (Flux) : le recalcul plante après avoir vidé la table.
    conn = get_db(base)
    try:
        conn.execute("DELETE FROM flux_unifies")
        _flux(conn, "FLX-PARTIEL", 1)
        # Une lecture concurrente pendant le recalcul ne voit pas l'état partiel…
        assert b"FLX-PARTIEL" not in web.get("/exports/fichier/PBI_Flux.csv").content
        conn.rollback()                                  # … et l'échec n'en laisse aucune trace.
    finally:
        conn.close()
    # Dataset versionné (Lot10) : un run candidat en échec n'est jamais activé.
    conn = get_db(base)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('L10-KO','ECHEC',0)")
        conn.execute(
            "INSERT INTO lot10_resultats (run_id, mois, logement_id, proprietaire_id, vision, "
            "resultat) VALUES ('L10-KO','2026-09','LOG_A','PROP_A','REEL', -1)")
        conn.commit()
    finally:
        conn.close()
    orch.marquer_dataset("LOT10", orch.ST_ECHEC, erreur_code="TEST", db_path=base)

    assert web.get("/exports/fichier/PBI_Flux.csv").content == avant_flux
    assert web.get("/exports/fichier/PBI_Resultats_Mensuels.csv").content == avant_res
    inv = {e["nom"]: e for e in svc.inventaire(db_path=base)["exports"]}
    assert inv["PBI_Resultats_Mensuels"]["etat"] == orch.ST_ECHEC
    assert "Erreur" in web.get("/exports").text, "l'échec se voit, sans changer le fichier servi"


# ── 6. Aucune dépendance à une copie figée ──────────────────────────────────────────────────────

def test_6_la_copie_disque_obsolete_n_est_jamais_servie(web, tmp_path):
    for nom, _, _ in svc.EXPORTS:
        assert b"COPIE_DU_12_09" not in web.get(f"/exports/fichier/{nom}.csv").content, nom
    archive = zipfile.ZipFile(io.BytesIO(web.get("/exports/archive.zip").content))
    for nom in archive.namelist():
        assert b"COPIE_DU_12_09" not in archive.read(nom), nom
    # Un nom qui n'est pas un export connu n'atteint jamais le disque.
    assert web.get("/exports/fichier/..%2f..%2fapp.db").status_code == 404
    assert web.get("/exports/fichier/inconnu.csv").status_code == 404


def test_6_la_consultation_et_le_telechargement_n_ecrivent_rien(web, tmp_path):
    perime = tmp_path / "powerbi_perime"
    avant = {p.name: p.stat().st_mtime_ns for p in perime.iterdir()}
    web.get("/exports")
    web.get("/exports/archive.zip")
    web.get("/exports/fichier/PBI_Flux.csv")
    assert {p.name: p.stat().st_mtime_ns for p in perime.iterdir()} == avant


def test_6_aucune_date_figee_ni_lecture_de_fichier_dans_le_module():
    route = (APP / "routes" / "exports.py").read_text(encoding="utf-8")
    code_route = route.split('"""', 2)[2]          # hors docstring de module
    for interdit in ("repertoire_exports", "read_bytes", "open(", "glob(", "EXPORTS_POWERBI",
                     "/exports/generer"):
        assert interdit not in code_route, interdit
    for fichier in (APP / "routes" / "exports.py", APP / "services" / "lot13_export_service.py",
                    APP / "templates" / "exports.html"):
        code = fichier.read_text(encoding="utf-8")
        code = re.sub(r'""".*?"""', "", code, flags=re.S)        # docstrings : récit de l'incident
        code = "\n".join(l.split("#", 1)[0] for l in code.splitlines())
        assert not re.search(r"2026-?09-?12|12/09/2026", code), fichier.name
    page = (APP / "templates" / "exports.html").read_text(encoding="utf-8")
    assert "/exports/generer" not in page, "aucun second bouton « actualiser les exports »"


# ── 7. Toutes les tables ────────────────────────────────────────────────────────────────────────

def test_7_chaque_table_exportable_est_servie_depuis_sa_source(web, base, tmp_path):
    assert set(svc.SOURCES_EXPORT) == {nom for nom, _, _ in svc.EXPORTS}, \
        "chaque export doit déclarer sa source canonique et sa fraîcheur"

    # Référence : ce que le moteur écrit sur disque (format Power BI historique).
    disque = tmp_path / "reference"
    assert svc.exporter(db_path=base, destination=disque)["ok"]
    inv = {e["nom"]: e for e in svc.inventaire(db_path=base)["exports"]}
    archive = zipfile.ZipFile(io.BytesIO(web.get("/exports/archive.zip").content))

    noms = [f"{nom}.csv" for nom, _, _ in svc.EXPORTS] + [f"{svc.NOM_DICTIONNAIRE}.csv"]
    assert sorted(archive.namelist()) == sorted(noms)
    for fichier in noms:
        servi = web.get(f"/exports/fichier/{fichier}")
        assert servi.status_code == 200, fichier
        attendu = (disque / fichier).read_bytes()
        assert servi.content == attendu, f"{fichier} : téléchargement ≠ moteur"
        assert archive.read(fichier) == attendu, f"{fichier} : archive ≠ moteur"

    for nom, _, _ in svc.EXPORTS:
        e = inv[nom]
        assert e["disponible"], nom
        assert e["nb_lignes"] == len(_telecharger(web, nom)) - 1 >= 1, nom
        if e["lu_en_direct"]:
            assert e["genere_le"] is None, nom
        else:
            assert e["genere_le"], f"{nom} : la date de la version exportée doit être connue"

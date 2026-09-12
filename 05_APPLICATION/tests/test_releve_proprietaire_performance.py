"""Relevé propriétaire économique (§13-§15), export utilisateur (§16), suivi mensuel (§17).

CE QUE CE FICHIER PROUVE
  §13 — l'écran ne porte aucun élément de trésorerie : ni créance, ni règlement, ni compensation.
  §14 — chaque formule donne le résultat annoncé, y compris dans les cas qui font mentir une
        moyenne naïve (voyageurs manquants, logement entré en gestion en cours de mois).
  §15 — aucun code technique n'atteint l'écran ; le mois en cours est marqué PROVISOIRE.
  §16 — un parcours d'export existe, produit des fichiers et les rend téléchargeables.
  §17 — le suivi de facturation se lit depuis la clôture mensuelle, et son moteur est intact.

ET SURTOUT : que les montants viennent du RUN ACTIF du moteur. Six runs coexistent en base ; une
lecture non filtrée additionne six fois le même mois. C'est l'erreur qui a produit des ADR à
quatre chiffres avant d'être corrigée, et rien d'autre ne l'aurait signalée.
"""
from __future__ import annotations

import pytest

from app.db.connection import get_db
from app.services import proprietaire_performance_service as perf

PROP = "PROP_PERF"
LOG_A = "LOG_PERF_A"
LOG_B = "LOG_PERF_B"
MOIS = "2026-04"      # 30 jours


def _run_lot10(conn, run_id: str, actif: int) -> None:
    conn.execute("INSERT INTO lot10_runs (run_id, date_calcul, actif) VALUES (?,?,?)",
                 (run_id, "2026-05-01T10:00:00Z", actif))


@pytest.fixture()
def base(tmp_db):
    conn = get_db(tmp_db)
    try:
        conn.executemany(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, prenom_proprietaire,"
            " actif, import_id) VALUES (?,?,?,?,?)",
            [(PROP, "Martin", "Claire", "OUI", "IMP-T")])
        conn.executemany(
            "INSERT INTO ref_logements (logement_id, nom_court, nom_logement_officiel, ville, "
            "type_logement_id, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            [(LOG_A, "Studio Capitole", "Studio Capitole", "Toulouse", "TYPE_001", "OUI", "GERE",
              "IMP-T"),
             (LOG_B, "T2 Carmes", "T2 Carmes", "Toulouse", "TYPE_002", "OUI", "GERE", "IMP-T")])
        # LOG_A géré tout le mois ; LOG_B entré en gestion le 16 → 15 jours seulement.
        conn.executemany(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) VALUES (?,?,?,?,?,?,?)",
            [("G1", LOG_A, PROP, "2025-01-01", None, "ACTIF", "IMP-T"),
             ("G2", LOG_B, PROP, "2026-04-16", None, "ACTIF", "IMP-T")])

        _run_lot10(conn, "RUN-ANCIEN", 0)
        _run_lot10(conn, "RUN-ACTIF", 1)
        # Le MÊME mois dans les deux runs : une lecture non filtrée doublerait tout.
        for run in ("RUN-ANCIEN", "RUN-ACTIF"):
            conn.executemany(
                "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
                "total_payout_mois, total_menage_mois, total_commission_mois, "
                "net_proprietaire_avant_charge_mois, nb_reservations) VALUES (?,?,?,?,?,?,?,?,?)",
                [(run, MOIS, LOG_A, PROP, 1000.0, 100.0, 150.0, 750.0, 4),
                 (run, MOIS, LOG_B, PROP, 500.0, 60.0, 75.0, 365.0, 2)])

        conn.execute("INSERT INTO reservations_datasets (dataset_id, etape, nb_lignes, actif) "
                     "VALUES ('RDS-T','RESOLUES',6,1)")
        lignes = [
            # (logement, canal, nuits, voyageurs, montant, retenue)
            (LOG_A, "AIRBNB", 3, 2, 300.0, "OUI"),
            (LOG_A, "AIRBNB", 5, 4, 400.0, "OUI"),
            (LOG_A, "BOOKING", 2, None, 200.0, "OUI"),   # voyageurs INCONNUS
            (LOG_A, "BOOKING", 2, 2, 100.0, "OUI"),
            (LOG_B, "AIRBNB", 4, 3, 500.0, "OUI"),
            (LOG_B, "AIRBNB", 7, 2, 900.0, "NON"),       # écartée du résultat réel
        ]
        for i, (logement, canal, nuits, voyageurs, montant, retenue) in enumerate(lignes):
            conn.execute(
                "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, mois, "
                "logement_id, proprietaire_id, nuits, guest_count, montant_retenu, canal, "
                "impact_resultat_reel, date_arrivee, date_depart) "
                "VALUES ('RDS-T',?,?,?,?,?,?,?,?,?,?,?)",
                (f"RES-{i}", MOIS, logement, PROP, nuits, voyageurs, montant, canal, retenue,
                 f"2026-04-{10+i:02d}", f"2026-04-{11+i:02d}"))
        conn.commit()
    finally:
        conn.close()
    return tmp_db


# ── §14 — les montants viennent du run ACTIF ────────────────────────────────────────────────────

def test_les_montants_ne_comptent_que_le_run_actif(base):
    """Six runs coexistent sur la base réelle. Additionner les runs a produit 12 046 € là où le
    mois vaut 1 204 €, avec des ADR à quatre chiffres pour seul indice."""
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["total_percu"] == 1500.0, "1000 + 500, pas le double"
    assert r["commission"] == 225.0
    assert r["net_proprietaire"] == 1115.0


# ── §14 — les formules ──────────────────────────────────────────────────────────────────────────

def test_nuits_et_reservations_excluent_ce_qui_n_est_pas_retenu(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["nb_reservations"] == 5
    assert r["nuits_occupees"] == 16      # 3+5+2+2+4, la réservation écartée ne compte pas
    assert r["nb_exclues"] == 1


def test_duree_moyenne(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["duree_moyenne"] == 3.2      # 16 / 5


def test_voyageurs_moyens_ne_divisent_que_par_les_reservations_renseignees(base):
    """Diviser par le total ferait baisser la moyenne à chaque donnée manquante, et une donnée
    absente deviendrait un séjour à zéro voyageur."""
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["nb_reservations_avec_voyageurs"] == 4
    assert r["nb_reservations_sans_voyageurs"] == 1
    assert r["voyageurs_moyens"] == 2.8   # (2+4+2+3) / 4, PAS / 5


def test_nuits_commercialisables_comptent_les_jours_reellement_sous_gestion(base):
    """LOG_A tout le mois (30 j), LOG_B à partir du 16 (15 j) : 45, pas 60."""
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["nuits_commercialisables"] == 45
    assert r["taux_remplissage"] == pytest.approx(35.6, abs=0.1)   # 16 / 45


def test_adr_et_adr_net(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    assert r["adr"] == pytest.approx(93.75, abs=0.01)              # 1500 / 16
    assert r["adr_net_proprietaire"] == pytest.approx(69.69, abs=0.01)   # 1115 / 16


def test_repartition_par_canal(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    canaux = {c["canal"]: c for c in r["canaux"]}
    assert canaux["AIRBNB"]["reservations"] == 3
    assert canaux["AIRBNB"]["nuits"] == 12
    assert canaux["BOOKING"]["nuits"] == 4
    assert canaux["AIRBNB"]["part_nuits"] == 75.0
    assert sum(c["reservations"] for c in r["canaux"]) == r["nb_reservations"]


def test_un_mois_sans_nuit_ne_produit_pas_d_adr(tmp_db):
    """Diviser par zéro nuit donnerait soit une erreur, soit un zéro trompeur."""
    r = perf.releve("PROP_VIDE", "2026-04", db_path=tmp_db)
    assert r["adr"] is None
    assert r["taux_remplissage"] is None


# ── §14 — le détail par logement ────────────────────────────────────────────────────────────────

def test_le_detail_par_logement_porte_les_memes_regles(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    par_logement = {l["logement_id"]: l for l in r["logements"]}
    a, b = par_logement[LOG_A], par_logement[LOG_B]
    assert a["nuits"] == 12 and a["nuits_commercialisables"] == 30
    assert b["nuits"] == 4 and b["nuits_commercialisables"] == 15
    assert sum(l["total_percu"] for l in r["logements"]) == r["total_percu"]
    assert sum(l["net_proprietaire"] for l in r["logements"]) == r["net_proprietaire"]


# ── §15 — aucun code technique, mois en cours signalé ───────────────────────────────────────────

def test_aucun_code_technique_n_atteint_l_ecran(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    assert not r["proprietaire"].startswith("PROP_")
    for ligne in r["logements"]:
        assert not ligne["logement"].startswith("LOG_")


def test_le_mois_en_cours_est_provisoire(base):
    assert perf.releve(PROP, perf.mois_courant(), db_path=base)["provisoire"] is True
    assert perf.releve(PROP, MOIS, db_path=base)["provisoire"] is False


def test_les_quantites_sont_entieres(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    for cle in ("nb_reservations", "nuits_occupees", "nuits_commercialisables", "nb_logements"):
        assert isinstance(r[cle], int), cle


# ── §13 — aucun élément de trésorerie ───────────────────────────────────────────────────────────

INTERDITS = ("creance", "créance", "reglement", "règlement", "compensation", "acompte",
             "reste_a_payer", "solde", "impaye", "relance")


def test_l_ecran_ne_porte_aucun_element_de_tresorerie(base):
    """§13 : c'est un écran de PERFORMANCE. Y mêler la trésorerie produisait un document qu'on ne
    pouvait lire d'un bout à l'autre : on y cherchait une performance, on y trouvait un compte."""
    r = perf.releve(PROP, MOIS, db_path=base)
    for cle in r:
        assert not any(mot in cle.lower() for mot in INTERDITS), cle


def test_chaque_indicateur_dit_comment_il_est_obtenu(base):
    r = perf.releve(PROP, MOIS, db_path=base)
    for cle, description in r["formules"].items():
        assert description["libelle"] and description["formule"] and description["source"], cle
    for attendu in ("taux_remplissage", "adr", "adr_net_proprietaire", "voyageurs_moyens",
                    "duree_moyenne", "net_proprietaire"):
        assert attendu in r["formules"]


# ── §14 — une contradiction se dit, elle ne se remplit pas ──────────────────────────────────────

def test_des_recettes_sans_periode_de_gestion_sont_signalees(tmp_db):
    """Cas réel : un logement retiré du parc en janvier, mais des séjours et des recettes en août.
    Sans dénominateur, « — » laisserait croire à un détail d'affichage."""
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, actif, "
                     "import_id) VALUES ('PROP_X','Sans gestion','OUI','IMP-T')")
        conn.execute("INSERT INTO lot10_runs (run_id, date_calcul, actif) "
                     "VALUES ('R1','2026-05-01T00:00:00Z',1)")
        conn.execute(
            "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
            "total_payout_mois, net_proprietaire_avant_charge_mois, nb_reservations) "
            "VALUES ('R1',?, 'LOG_X','PROP_X', 900.0, 600.0, 3)", (MOIS,))
        conn.commit()
    finally:
        conn.close()
    r = perf.releve("PROP_X", MOIS, db_path=tmp_db)
    assert r["taux_remplissage"] is None
    assert "capacite_inconnue" in r
    assert "période de gestion" in r["capacite_inconnue"]


# ── §16 — l'export existe et se récupère ────────────────────────────────────────────────────────

def test_l_export_du_releve_porte_les_memes_chiffres(base, monkeypatch):
    from fastapi.testclient import TestClient

    import app.config as cfg
    from app.main import app

    monkeypatch.setattr(cfg, "DB_PATH", base)
    client = TestClient(app)
    reponse = client.get(f"/releves-proprietaires/export.csv?mois={MOIS}")
    assert reponse.status_code == 200
    texte = reponse.text
    assert "proprietaire;logement" in texte
    assert "Studio Capitole" in texte
    assert "LOG_PERF_A" not in texte, "un export destiné à l'utilisateur ne porte pas de code"
    assert "1000,0" in texte, "décimale française : un tableur FR lit « 1000.0 » comme du texte"
    assert "provisoire" in texte.splitlines()[0]


def test_le_parcours_d_export_existe_et_produit_des_fichiers(tmp_path, monkeypatch, tmp_db):
    from fastapi.testclient import TestClient

    import app.config as cfg
    from app.main import app

    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", tmp_path / "exports")
    client = TestClient(app)

    assert client.get("/exports").status_code == 200
    assert client.get("/exports/archive.zip").status_code == 404, "rien à télécharger avant génération"
    assert client.post("/exports/generer", follow_redirects=False).status_code == 303
    assert list((tmp_path / "exports").glob("*.csv")), "des fichiers doivent avoir été écrits"
    archive = client.get("/exports/archive.zip")
    assert archive.status_code == 200
    assert "PROVISOIRE" in archive.headers["content-disposition"]


def test_un_nom_de_fichier_ne_peut_pas_sortir_du_dossier_d_export(tmp_path, monkeypatch, tmp_db):
    from fastapi.testclient import TestClient

    import app.config as cfg
    from app.main import app

    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", tmp_path / "exports")
    client = TestClient(app)
    assert client.get("/exports/fichier/..%2f..%2fapp.db").status_code == 404


# ── §17 — le suivi de facturation se lit depuis la clôture ──────────────────────────────────────

def test_l_avancement_du_mois_compte_les_proprietaires_sans_suivi(base):
    """Un suivi non ouvert n'est pas « à jour » : il est invisible. Clôturer sur cette base
    fermerait le mois sur une facturation que plus aucun écran ne réclame."""
    from app.services import proprietaires_suivi_service as suivi

    avancement = suivi.avancement_mois(MOIS, db_path=base)
    assert avancement["valide"] is True
    assert avancement["nb_proprietaires_actifs"] == 1
    assert avancement["nb_non_demarres"] == 1
    assert avancement["non_demarres"][0]["libelle"].startswith("Martin") \
        or "Claire" in avancement["non_demarres"][0]["libelle"]

    suivi.creer_ou_charger(PROP, MOIS, acteur="test", db_path=base)
    apres = suivi.avancement_mois(MOIS, db_path=base)
    assert apres["nb_non_demarres"] == 0
    assert apres["total"] == 1


def test_un_mois_malforme_ne_produit_pas_d_avancement(base):
    from app.services import proprietaires_suivi_service as suivi

    assert suivi.avancement_mois("avril", db_path=base)["valide"] is False


def test_le_moteur_de_suivi_reste_intact(base):
    """§17 : déplacer le parcours ne doit pas toucher au service — il est transactionnel,
    idempotent et journalisé."""
    from app.services import proprietaires_suivi_service as suivi

    premier = suivi.creer_ou_charger(PROP, MOIS, acteur="test", db_path=base)
    second = suivi.creer_ou_charger(PROP, MOIS, acteur="test", db_path=base)
    assert premier["releve_id_opaque"] == second["releve_id_opaque"]
    with pytest.raises(suivi.ReleveRefuse):
        suivi.creer_ou_charger(PROP, "2026-13", acteur="test", db_path=base)
    with pytest.raises(suivi.ReleveRefuse):
        suivi.creer_ou_charger("", MOIS, acteur="test", db_path=base)

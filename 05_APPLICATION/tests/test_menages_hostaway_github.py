"""« Actualiser les ménages » par le workflow GitHub des tâches — cycle complet, sans réseau.

Le client GitHub est remplacé par un double qui simule le dépôt : runs, statuts, artifacts. La chaîne
Ménages est remplacée par un double qui appelle l'étape « tâches » injectée — c'est elle qui active
le jeu préparé ; le reste de la chaîne canonique est couvert par ses propres tests.
"""
from __future__ import annotations

import io
import zipfile
from datetime import datetime, timedelta, timezone

import pytest

from app.adapters import github_actions_client as gh
from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import menages_hostaway_github_service as svc
from app.services import orchestrateur_moteur as moteur

ENTETE = ("id\treservationId\tlistingMapId\ttitle\tstatus\ttaskType\ttype\tcanStartFrom\t"
          "shouldEndBy\tassigneeUserId")


def _tsv(n: int = 3, statut: str = "completed") -> bytes:
    lignes = [ENTETE] + [
        f"{1000 + i}\t{5000 + i}\t480136\tMénage {i}\t{statut}\t\t\t2026-09-{10 + i:02d} 10:00:00"
        f"\t2026-09-{10 + i:02d} 15:00:00\t" for i in range(n)]
    return ("﻿" + "\n".join(lignes) + "\n").encode("utf-8")


def _zip(nom: str, contenu: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(nom, contenu)
    return buf.getvalue()


class FauxGitHub:
    """Dépôt simulé. `sequence` : statuts successifs rendus pour le run de la demande."""

    def __init__(self, sequence=(("queued", None), ("in_progress", None), ("completed", "success")),
                 tsv: bytes | None = None, autres_runs=(), rend_run_id=False):
        self.sequence = list(sequence)
        self.tsv = _tsv() if tsv is None else tsv
        self.autres_runs = list(autres_runs)
        self.rend_run_id = rend_run_id
        self.dispatches: list[str] = []
        self.telechargements: list = []
        self.request_id = None

    def __call__(self):          # usage comme client_factory
        return self

    def dispatch(self, request_id):
        self.dispatches.append(request_id)
        self.request_id = request_id
        return {"workflow_run_id": 777 if self.rend_run_id else None}

    def _run(self):
        statut, conclusion = self.sequence[0]
        if len(self.sequence) > 1:
            self.sequence.pop(0)
        return {"id": 777, "display_title": gh.titre_run(self.request_id), "status": statut,
                "conclusion": conclusion, "created_at": "2026-09-24T20:00:00Z"}

    def run(self, run_id):
        return self._run()

    def trouver_run(self, request_id, depuis=None):
        # Le vrai client filtre par titre exact : les autres runs (manuels, autres demandes) ne
        # doivent jamais être retenus.
        for r in self.autres_runs:
            assert r["display_title"] != gh.titre_run(request_id)
        if self.request_id != request_id:
            return None
        return self._run()

    def telecharger_artifact(self, run_id, nom_artifact, nom_fichier):
        self.telechargements.append((run_id, nom_artifact, nom_fichier))
        assert str(run_id) == "777"
        return {"artifact_id": 42, "nom": nom_artifact,
                "octets": gh.extraire_fichier_zip(_zip(nom_fichier, self.tsv), nom_fichier)}


def _chaine_ok(**kwargs):
    etape = kwargs["etape_hostaway"]()
    return {"ok": True, "statut": "SUCCES", "etapes": [{"etape": "HOSTAWAY", **etape}],
            "echecs": [], "mois_recalcules": ["2026-09"], "statistiques": ["1 mois mis à jour"]}


def _chaine_echec(**kwargs):
    etape = kwargs["etape_hostaway"]()
    return {"ok": False, "statut": "PARTIEL", "etapes": [{"etape": "HOSTAWAY", **etape}],
            "echecs": [{"mois": "2026-09", "message": "lot6d KO"}], "mois_recalcules": [],
            "code": "RECALCUL", "statistiques": []}


@pytest.fixture
def sans_moteur(monkeypatch):
    """lot6a / lot6d ne sont pas lancés : on vérifie l'activation, pas le moteur."""
    appels = {"comptage": 0, "cible": []}

    def compter(**_):
        appels["comptage"] += 1
        return {"ok": True}

    monkeypatch.setattr(moteur, "compter_taches_menage", compter)
    monkeypatch.setattr(moteur, "executer_menages_cible",
                        lambda **k: appels["cible"].append(k["mois"]) or {"ok": True})
    return appels


def _jusqua_fin(refresh_id, faux, tmp_db, chaine=_chaine_ok, maintenant=None, pas_max=10):
    ligne = {}
    for _ in range(pas_max):
        kwargs = {"client_factory": faux, "chaine": chaine, "db_path": tmp_db}
        if maintenant:
            kwargs["maintenant"] = maintenant
        ligne = svc.avancer(refresh_id, **kwargs)
        if ligne["etat"] in svc.TERMINAUX:
            break
    return ligne


def _demarrer(tmp_db, mois="2026-09"):
    return svc.demarrer(acteur="test", mois_affiche=mois, lancer_suivi=False, db_path=tmp_db)


# ── request_id ──────────────────────────────────────────────────────────────────────────────────

def test_request_id_conforme_au_contrat_du_workflow():
    rid = svc.nouveau_request_id()
    assert rid.startswith("MEN-REFRESH-")
    assert gh.REQUEST_ID_VALIDE.match(rid) and len(rid) <= 64
    assert svc.nouveau_request_id() != rid
    assert gh.titre_run(rid) == f"Hostaway Cleaning Tasks — {rid}"


def test_dispatch_refuse_un_request_id_invalide():
    class Session:
        def __init__(self):
            self.headers = {}

        def request(self, *a, **k):
            raise AssertionError("aucun appel réseau attendu")

    client = gh.ClientGitHubActions(gh.Configuration("o/r", "w.yml", "main", "jeton"),
                                    session=Session())
    with pytest.raises(ValueError):
        client.dispatch("pas valide !")


# ── cycle nominal ───────────────────────────────────────────────────────────────────────────────

def test_cycle_queued_running_success_active_le_jeu(tmp_db, sans_moteur):
    faux = FauxGitHub()
    rid = _demarrer(tmp_db)["refresh_id"]
    etats = []
    for _ in range(6):
        ligne = svc.avancer(rid, client_factory=faux, chaine=_chaine_ok, db_path=tmp_db)
        etats.append(ligne["etat"])
        if ligne["etat"] in svc.TERMINAUX:
            break
    assert etats == ["EN_ATTENTE", "EN_ATTENTE", "EXTRACTION", "TERMINE"]
    assert len(faux.dispatches) == 1
    assert faux.telechargements == [("777", "cleaning-tasks-output", "cleaning_tasks_hostaway.tsv")]
    assert ligne["synchronisation_faite"] == 1 and ligne["artifact_traite"] == 1
    assert ligne["nb_taches"] == 3 and ligne["active"] is None
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ligne["extraction_id"]
    assert svc.vue(ligne)["message"] == "Données Hostaway actualisées."
    assert sans_moteur["comptage"] == 1


def test_run_exact_par_titre_et_autre_run_ignore(tmp_db, sans_moteur):
    autre = {"id": 1, "display_title": "Hostaway Cleaning Tasks — manual",
             "status": "completed", "conclusion": "success"}
    faux = FauxGitHub(sequence=[("completed", "success")], autres_runs=[autre])
    rid = _demarrer(tmp_db)["refresh_id"]
    ligne = _jusqua_fin(rid, faux, tmp_db)
    assert ligne["etat"] == "TERMINE" and ligne["github_run_id"] == "777"


def test_identifiant_rendu_par_le_dispatch_verifie_par_le_titre(tmp_db, sans_moteur):
    class Mauvais(FauxGitHub):
        def run(self, run_id):
            return {"id": run_id, "display_title": "Hostaway Cleaning Tasks — AUTRE",
                    "status": "completed", "conclusion": "failure"}

    faux = Mauvais(sequence=[("completed", "success")], rend_run_id=True)
    rid = _demarrer(tmp_db)["refresh_id"]
    # Le run 777 désigné par le dispatch porte un autre titre : il est écarté au profit de la
    # recherche par titre, qui trouve le bon (success) — jamais le failure d'un autre.
    assert _jusqua_fin(rid, faux, tmp_db)["etat"] == "TERMINE"


def test_trouver_run_client_reel_filtre_par_titre_exact():
    rid = "MEN-REFRESH-X"

    class Rep:
        status_code = 200
        headers: dict = {}

        def __init__(self, donnees):
            self._d = donnees

        def json(self):
            return self._d

    class Session:
        headers: dict = {}

        def request(self, methode, url, **kw):
            return Rep({"workflow_runs": [
                {"id": 3, "display_title": "Hostaway Cleaning Tasks — MEN-REFRESH-Y",
                 "created_at": "2026-09-24T20:00:03Z", "status": "completed"},
                {"id": 2, "display_title": gh.titre_run(rid),
                 "created_at": "2026-09-24T20:00:02Z", "status": "queued"},
                {"id": 1, "display_title": "Hostaway Cleaning Tasks — manual",
                 "created_at": "2026-09-24T20:00:01Z", "status": "completed"}]})

    client = gh.ClientGitHubActions(gh.Configuration("o/r", "w.yml", "main", "j"),
                                    session=Session())
    assert client.trouver_run(rid)["id"] == 2
    assert client.trouver_run("MEN-REFRESH-Z") is None


# ── échecs : aucun import ───────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("conclusion, etat", [("failure", "ECHEC"), ("cancelled", "ANNULE"),
                                              ("timed_out", "ECHEC")])
def test_conclusions_sans_import(tmp_db, sans_moteur, conclusion, etat):
    faux = FauxGitHub(sequence=[("in_progress", None), ("completed", conclusion)])
    rid = _demarrer(tmp_db)["refresh_id"]
    ligne = _jusqua_fin(rid, faux, tmp_db)
    assert ligne["etat"] == etat
    assert faux.telechargements == []
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ""
    vue = svc.vue(ligne)
    assert "dernières données valides ont été conservées" in vue["message"]
    if etat == "ANNULE":
        assert vue["message"].startswith("L'actualisation Hostaway a été annulée")


def test_delai_depasse_run_jamais_termine(tmp_db, sans_moteur):
    faux = FauxGitHub(sequence=[("queued", None)])
    rid = _demarrer(tmp_db)["refresh_id"]
    svc.avancer(rid, client_factory=faux, chaine=_chaine_ok, db_path=tmp_db)   # dispatch
    tard = datetime.now(timezone.utc) + timedelta(seconds=svc.DELAI_RUN_MAX_S + 60)
    ligne = svc.avancer(rid, client_factory=faux, chaine=_chaine_ok, db_path=tmp_db,
                        maintenant=lambda: tard)
    assert ligne["etat"] == "ECHEC" and ligne["code_erreur"] == gh.E_DELAI
    assert faux.telechargements == []


def test_run_introuvable_apres_delai(tmp_db, sans_moteur):
    class Muet(FauxGitHub):
        def trouver_run(self, request_id, depuis=None):
            return None

    rid = _demarrer(tmp_db)["refresh_id"]
    svc.avancer(rid, client_factory=Muet(), db_path=tmp_db)
    assert svc.avancer(rid, client_factory=Muet(), db_path=tmp_db)["etat"] == "EN_ATTENTE"
    tard = datetime.now(timezone.utc) + timedelta(seconds=svc.DELAI_RUN_INTROUVABLE_S + 5)
    ligne = svc.avancer(rid, client_factory=Muet(), db_path=tmp_db, maintenant=lambda: tard)
    assert ligne["etat"] == "ECHEC" and ligne["code_erreur"] == "RUN_INTROUVABLE"


@pytest.mark.parametrize("code, transitoire", [(gh.E_AUTHENTIFICATION, False),
                                               (gh.E_INTERDIT, False),
                                               (gh.E_INTROUVABLE, False),
                                               (gh.E_LIMITE, True), (gh.E_DELAI, True)])
def test_erreurs_http_pendant_le_suivi(tmp_db, sans_moteur, code, transitoire):
    class Panne(FauxGitHub):
        def trouver_run(self, request_id, depuis=None):
            raise gh.ErreurGitHub(code, "HTTP")

    faux = Panne()
    rid = _demarrer(tmp_db)["refresh_id"]
    svc.avancer(rid, client_factory=faux, db_path=tmp_db)                       # dispatch
    ligne = svc.avancer(rid, client_factory=faux, db_path=tmp_db)
    assert ligne["etat"] == ("EN_ATTENTE" if transitoire else "ECHEC")
    assert ligne["code_erreur"] == code


def test_jeton_absent_echec_propre(tmp_db, monkeypatch):
    monkeypatch.delenv(gh.ENV_JETON, raising=False)
    rid = _demarrer(tmp_db)["refresh_id"]
    ligne = svc.avancer(rid, db_path=tmp_db)       # vrai client : refuse avant tout réseau
    assert ligne["etat"] == "ECHEC" and ligne["code_erreur"] == gh.E_CONFIGURATION


@pytest.mark.parametrize("statut_http, code", [(401, gh.E_AUTHENTIFICATION), (403, gh.E_INTERDIT),
                                               (404, gh.E_INTROUVABLE), (429, gh.E_LIMITE),
                                               (500, gh.E_API)])
def test_client_classe_les_statuts_http(statut_http, code):
    class Rep:
        status_code = statut_http
        headers: dict = {}

    with pytest.raises(gh.ErreurGitHub) as exc:
        gh.ClientGitHubActions._verifier(Rep())
    assert exc.value.code == code


def test_client_timeout_reseau():
    import requests

    class Session:
        headers: dict = {}

        def request(self, *a, **k):
            raise requests.Timeout("lent")

    client = gh.ClientGitHubActions(gh.Configuration("o/r", "w.yml", "main", "j"),
                                    session=Session())
    with pytest.raises(gh.ErreurGitHub) as exc:
        client.run(1)
    assert exc.value.code == gh.E_DELAI


# ── artifact / TSV ──────────────────────────────────────────────────────────────────────────────

def test_artifact_mal_forme_aucune_activation(tmp_db, sans_moteur):
    faux = FauxGitHub(sequence=[("completed", "success")], tsv=b"colonne\tautre\n1\t2\n")
    rid = _demarrer(tmp_db)["refresh_id"]
    ligne = _jusqua_fin(rid, faux, tmp_db)
    assert ligne["etat"] == "ECHEC"
    assert "colonnes absentes" in (ligne["erreur"] or "")
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ""
    assert sans_moteur["comptage"] == 0


def test_artifact_sans_le_fichier_attendu():
    with pytest.raises(gh.ErreurGitHub) as exc:
        gh.extraire_fichier_zip(_zip("autre.tsv", b"x"), "cleaning_tasks_hostaway.tsv")
    assert exc.value.code == gh.E_ARTIFACT_INVALIDE
    with pytest.raises(gh.ErreurGitHub):
        gh.extraire_fichier_zip(b"pas un zip", "cleaning_tasks_hostaway.tsv")


def test_artifact_identique_au_jeu_actif_rien_de_nouveau(tmp_db, sans_moteur):
    rid = _demarrer(tmp_db)["refresh_id"]
    premiere = _jusqua_fin(rid, FauxGitHub(sequence=[("completed", "success")]), tmp_db)
    rid2 = _demarrer(tmp_db)["refresh_id"]
    seconde = _jusqua_fin(rid2, FauxGitHub(sequence=[("completed", "success")]), tmp_db)
    assert seconde["etat"] == "TERMINE"
    assert seconde["extraction_id"] == premiere["extraction_id"]      # aucune extraction ajoutée


# ── atomicité ───────────────────────────────────────────────────────────────────────────────────

def test_rollback_si_le_rapprochement_echoue(tmp_db, sans_moteur):
    rid = _demarrer(tmp_db)["refresh_id"]
    valide = _jusqua_fin(rid, FauxGitHub(sequence=[("completed", "success")]), tmp_db)
    jeu_valide = valide["extraction_id"]

    rid2 = _demarrer(tmp_db)["refresh_id"]
    ligne = _jusqua_fin(rid2, FauxGitHub(sequence=[("completed", "success")], tsv=_tsv(5)),
                        tmp_db, chaine=_chaine_echec)
    assert ligne["etat"] == "ECHEC" and ligne["synchronisation_faite"] == 0
    # Le jeu préparé a été activé puis retiré : le dernier jeu valide est de nouveau servi, et le
    # comptage a été relancé dessus.
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == jeu_valide
    assert sans_moteur["comptage"] == 3


def test_rapprochement_en_exception_rollback(tmp_db, sans_moteur):
    def chaine(**kwargs):
        kwargs["etape_hostaway"]()
        raise RuntimeError("panne")

    rid = _demarrer(tmp_db)["refresh_id"]
    ligne = _jusqua_fin(rid, FauxGitHub(sequence=[("completed", "success")]), tmp_db,
                        chaine=chaine)
    assert ligne["etat"] == "ECHEC"
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ""


# ── double clic / idempotence ──────────────────────────────────────────────────────────────────

def test_double_clic_une_seule_actualisation(tmp_db):
    a = _demarrer(tmp_db)
    b = _demarrer(tmp_db)
    assert b["deja_en_cours"] and b["refresh_id"] == a["refresh_id"]


def test_index_unique_interdit_deux_actives(tmp_db):
    import sqlite3

    _demarrer(tmp_db)
    conn = sqlite3.connect(tmp_db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO menages_actualisations_hostaway (refresh_id, request_id, acteur) "
                     "VALUES ('X', 'MEN-REFRESH-X', 't')")
    conn.close()


def test_suivi_idempotent_un_seul_dispatch(tmp_db, sans_moteur):
    faux = FauxGitHub(sequence=[("queued", None)])
    rid = _demarrer(tmp_db)["refresh_id"]
    for _ in range(5):
        svc.avancer(rid, client_factory=faux, db_path=tmp_db)
    assert len(faux.dispatches) == 1


def test_polling_apres_fin_ne_refait_rien(tmp_db, sans_moteur):
    faux = FauxGitHub(sequence=[("completed", "success")])
    rid = _demarrer(tmp_db)["refresh_id"]
    fin = _jusqua_fin(rid, faux, tmp_db)
    for _ in range(3):
        assert svc.avancer(rid, client_factory=faux, chaine=_chaine_ok, db_path=tmp_db) == fin
    assert len(faux.telechargements) == 1 and sans_moteur["comptage"] == 1


def test_recuperation_reclamee_une_seule_fois(tmp_db, sans_moteur):
    """Deux suiveurs voient le run terminé : un seul télécharge l'artifact."""
    faux = FauxGitHub(sequence=[("completed", "success")])
    rid = _demarrer(tmp_db)["refresh_id"]
    svc.avancer(rid, client_factory=faux, db_path=tmp_db)                      # dispatch
    ligne = svc.charger(rid, db_path=tmp_db)
    # Le premier suiveur a déjà réclamé la récupération.
    assert svc._maj(rid, db_path=tmp_db, depuis=(svc.EN_ATTENTE,), etat=svc.RECUPERATION)
    svc._pas_suivi_run(ligne, faux, datetime.now, None, tmp_db)
    assert faux.telechargements == []


# ── aucun secret ────────────────────────────────────────────────────────────────────────────────

def test_aucun_secret_dans_etat_ni_erreurs(tmp_db, sans_moteur):
    jeton = "ghp_SECRETSECRETSECRET"
    config = gh.Configuration("o/r", "w.yml", "main", jeton)
    assert jeton not in repr(config)
    assert jeton not in str(gh.ErreurGitHub(gh.E_AUTHENTIFICATION, "HTTP 401"))

    class Panne(FauxGitHub):
        def trouver_run(self, request_id, depuis=None):
            raise gh.ErreurGitHub(gh.E_AUTHENTIFICATION, "HTTP 401")

    rid = _demarrer(tmp_db)["refresh_id"]
    svc.avancer(rid, client_factory=Panne(), db_path=tmp_db)
    svc.avancer(rid, client_factory=Panne(), db_path=tmp_db)
    import sqlite3

    conn = sqlite3.connect(tmp_db)
    tout = " ".join(str(v) for r in conn.execute("SELECT * FROM menages_actualisations_hostaway")
                    for v in r)
    conn.close()
    assert jeton not in tout and "ghp_" not in tout


# ── écran ───────────────────────────────────────────────────────────────────────────────────────

def test_route_post_rend_la_main_et_etat_json(client, tmp_db, monkeypatch):
    monkeypatch.setattr(svc, "assurer_suivi", lambda *a, **k: False)
    r = client.post("/menages/actualiser", data={"mois": "2026-09"}, follow_redirects=False)
    assert r.status_code == 303 and "refresh=MRH-" in r.headers["location"]
    refresh_id = r.headers["location"].split("refresh=")[1]
    etat = client.get(f"/menages/actualisation/etat?refresh_id={refresh_id}").json()
    assert etat["etat"] == "LANCEMENT" and etat["libelle"] == "Lancement"
    assert "github" not in str(etat).lower()
    # Double clic : même actualisation.
    r2 = client.post("/menages/actualiser", data={"mois": "2026-09"}, follow_redirects=False)
    assert refresh_id in r2.headers["location"]
    page = client.get(f"/menages?mois=2026-09&refresh={refresh_id}").text
    assert "Actualisation en cours…" in page and 'disabled' in page


def test_ecran_sans_m04_ni_bloc_technique(client, tmp_db):
    page = client.get("/menages").text
    assert "Actualiser les ménages" in page
    assert ">M04<" not in page and "(M04)" not in page
    assert "rattaché" not in page or "à une facture" not in page
    assert "fichiers dans le dossier" not in page and "fichier dans le dossier" not in page

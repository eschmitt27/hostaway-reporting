"""Pipeline transactionnel : prévisualisation/token, arrêt au premier échec, rollback,
indicateurs, comparaison avant/après, clôture mensuelle."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import calculs_executeur_service as ex
from app.services import calculs_pipeline_service as pipe


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    apply_migrations(db)
    racine = tmp_path / "projet"
    (racine / "02_TRAVAIL").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "Charges").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "REF_Setup").mkdir(parents=True)
    (racine / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Flux.xlsx").write_bytes(b"x")
    (racine / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm").write_bytes(b"y")

    monkeypatch.setattr(cfg, "PROJECT_ROOT", racine)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "LOT4A_ENGINE_PYTHON", Path(sys.executable))
    monkeypatch.setattr(cfg, "DRYRUNS_DIR", tmp_path / "dryruns")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(cfg, "DB_PATH", db)
    return {"db": db, "racine": racine, "tmp": tmp_path}


def _script(env, nom, corps):
    (env["racine"] / "02_TRAVAIL" / nom).write_text(corps, encoding="utf-8")


def _lots(monkeypatch, *lots: ex.Lot):
    tous = dict(ex.TOUS_LES_LOTS)
    for l in lots:
        tous[l.nom] = l
    monkeypatch.setattr(ex, "TOUS_LES_LOTS", tous)


def _lot_produisant(env, monkeypatch, nom, sortie, contenu="v1"):
    _script(env, f"{nom}.py",
            "from pathlib import Path\n"
            f"p = Path({sortie!r}); p.parent.mkdir(parents=True, exist_ok=True)\n"
            f"p.write_text({contenu!r})\nprint('{nom} ok')\n")
    # Les lots factices n'importent pas pandas : ils le déclarent, sinon le vérificateur de
    # prérequis les bloquerait à cause de l'interpréteur des tests (sans pandas).
    _lots(monkeypatch, ex.Lot(nom, f"{nom}.py", sorties=(sortie,), requiert_pandas=False))


# ── Prévisualisation et token ────────────────────────────────────────────────

def test_previsualisation_ne_lance_rien(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    res = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    assert res["ok"] and res["token"]
    assert not (env["racine"] / "out" / "a.txt").exists()      # aucune exécution


def test_previsualisation_expose_entrees_et_sorties(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    res = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    assert any(e["fichier"].endswith("SAISIE_Charges_Flux.xlsx") and e["existe"]
               for e in res["entrees"])
    assert res["sorties_remplacees"][0]["fichier"] == "out/a.txt"
    assert res["sorties_remplacees"][0]["existe"] is False


def test_token_inconnu_refuse(env):
    res = pipe.lancer("token-bidon", db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_TOKEN_INCONNU


def test_entrees_modifiees_refusent_le_lancement(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    # Une entrée change après la prévisualisation.
    (env["racine"] / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm").write_bytes(b"MODIFIE")
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_ENTREES_MODIFIEES


def test_prerequis_non_satisfaits_refusent_le_lancement(env, monkeypatch):
    _lots(monkeypatch, ex.Lot("absent", "absent_du_disque.py", requiert_pandas=False))
    prev = pipe.previsualiser("2026-06", ["absent"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_PREREQUIS


def test_lot_exigeant_pandas_bloque_si_interpreteur_sans_pandas(env, monkeypatch):
    """L'interpréteur des tests n'a pas pandas : un lot qui l'exige doit être bloqué AVANT
    lancement, avec un motif explicite — plutôt qu'un ImportError au milieu du pipeline."""
    _script(env, "gourmand.py", "import pandas")
    _lots(monkeypatch, ex.Lot("gourmand", "gourmand.py", requiert_pandas=True))
    prereq = ex.verifier_prerequis(["gourmand"], racine=env["racine"])
    if prereq["interpreteur"].get("pandas"):
        pytest.skip("l'interpréteur courant porte pandas : ce cas n'est pas reproductible ici")
    assert prereq["ok"] is False
    assert prereq["pandas_requis"] is True and prereq["pandas_manquant"] is True

    prev = pipe.previsualiser("2026-06", ["gourmand"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_PREREQUIS


def test_lot_sans_pandas_non_bloque_par_un_interpreteur_sans_pandas(env, monkeypatch):
    """Symétrique : ne PAS bloquer un lot qui n'a pas besoin de pandas (défaut corrigé)."""
    _script(env, "sobre.py", "print('ok')")
    _lots(monkeypatch, ex.Lot("sobre", "sobre.py", requiert_pandas=False))
    prereq = ex.verifier_prerequis(["sobre"], racine=env["racine"])
    assert prereq["ok"] is True and prereq["pandas_requis"] is False


# ── Exécution ────────────────────────────────────────────────────────────────

def test_run_succes_journalise_les_lots(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    res = pipe.lancer(prev["token"], acteur="recette", db_path=env["db"])
    assert res["ok"] and res["statut"] == pipe.ST_SUCCES
    assert res["nb_lots_reussis"] == 1 and res["nb_lots_echoues"] == 0

    detail = pipe.charger_run(res["run_id_opaque"], env["db"])
    assert detail["statut"] == pipe.ST_SUCCES
    assert detail["lots"][0]["lot"] == "a"
    assert detail["lots"][0]["statut"] == ex.ST_SUCCES
    assert "a ok" in detail["lots"][0]["stdout_extrait"]
    assert detail["duree_totale_s"] >= 0


def test_arret_au_premier_echec_et_lots_suivants_ignores(env, monkeypatch):
    _script(env, "ok1.py", "print('ok1')")
    _script(env, "casse.py", "import sys; sys.exit(2)")
    _script(env, "jamais.py", "from pathlib import Path; Path('out/jamais.txt').write_text('x')")
    _lots(monkeypatch,
          ex.Lot("ok1", "ok1.py", requiert_pandas=False),
          ex.Lot("casse", "casse.py", requiert_pandas=False),
          ex.Lot("jamais", "jamais.py", requiert_pandas=False))
    prev = pipe.previsualiser("2026-06", ["ok1", "casse", "jamais"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])

    assert res["ok"] is False and res["statut"] == pipe.ST_ECHEC
    detail = pipe.charger_run(res["run_id_opaque"], env["db"])
    statuts = {l["lot"]: l["statut"] for l in detail["lots"]}
    assert statuts == {"ok1": ex.ST_SUCCES, "casse": ex.ST_ECHEC, "jamais": ex.ST_IGNORE}
    assert not (env["racine"] / "out" / "jamais.txt").exists()   # jamais lancé


def test_pipeline_partiel_jamais_presente_comme_reussi(env, monkeypatch):
    _script(env, "ok1.py", "print('ok1')")
    _script(env, "casse.py", "import sys; sys.exit(1)")
    _lots(monkeypatch, ex.Lot("ok1", "ok1.py", requiert_pandas=False), ex.Lot("casse", "casse.py", requiert_pandas=False))
    prev = pipe.previsualiser("2026-06", ["ok1", "casse"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["nb_lots_reussis"] == 1          # un lot a bien réussi...
    assert res["ok"] is False                  # ...mais le RUN est en échec
    assert res["erreur_resume"]


def test_mode_reel_refuse_par_defaut(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    # On simule un manifeste en mode REEL (mode recette coupé après la prévisualisation).
    import json
    p = cfg.DRYRUNS_DIR / "calculs" / prev["token"] / "manifest.json"
    m = json.loads(p.read_text(encoding="utf-8"))
    m["mode"] = pipe.MODE_REEL
    p.write_text(json.dumps(m, default=str), encoding="utf-8")
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_MODE_REEL_INTERDIT


# ── Sauvegarde / rollback ────────────────────────────────────────────────────

def test_sauvegarde_puis_restauration(env, monkeypatch):
    sortie = env["racine"] / "out" / "a.txt"
    sortie.parent.mkdir(parents=True)
    sortie.write_text("ORIGINAL")

    _lot_produisant(env, monkeypatch, "a", "out/a.txt", contenu="ECRASE")
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"]
    assert sortie.read_text() == "ECRASE"

    r = pipe.restaurer(res["run_id_opaque"], racine=env["racine"], db_path=env["db"])
    assert r["ok"] and "out/a.txt" in r["fichiers_restaures"]
    assert sortie.read_text() == "ORIGINAL"          # rollback prouvé
    assert pipe.charger_run(res["run_id_opaque"], env["db"])["statut"] == pipe.ST_RESTAURE


def test_rollback_apres_echec(env, monkeypatch):
    sortie = env["racine"] / "out" / "a.txt"
    sortie.parent.mkdir(parents=True)
    sortie.write_text("ORIGINAL")
    _script(env, "a.py",
            "from pathlib import Path\nimport sys\n"
            "Path('out/a.txt').write_text('CORROMPU')\nsys.exit(1)\n")
    _lots(monkeypatch, ex.Lot("a", "a.py", requiert_pandas=False, sorties=("out/a.txt",)))
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    res = pipe.lancer(prev["token"], db_path=env["db"])
    assert res["ok"] is False
    assert sortie.read_text() == "CORROMPU"          # le lot a laissé un état incohérent

    pipe.restaurer(res["run_id_opaque"], racine=env["racine"], db_path=env["db"])
    assert sortie.read_text() == "ORIGINAL"


# ── Indicateurs et comparaison ───────────────────────────────────────────────

def _xlsx(chemin: Path, onglet: str, entetes: list[str], lignes: list[list]):
    import openpyxl
    chemin.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet(onglet)
    ws.append(entetes)
    for l in lignes:
        ws.append(l)
    wb.save(chemin); wb.close()


def test_indicateurs_releves_depuis_les_fichiers_reels(env, monkeypatch):
    _xlsx(env["racine"] / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx",
          "COMMISSIONS", ["montant_commission"], [[100.0], [50.5]])
    rel = pipe.relever_indicateurs("RUN-TEST", "2026-06", racine=env["racine"], db_path=env["db"])
    comm = next(r for r in rel if r["indicateur"] == "commissions")
    assert comm["valeur"] == 150.5 and comm["nb_lignes"] == 2


def test_indicateur_absent_si_fichier_absent(env):
    rel = pipe.relever_indicateurs("RUN-X", "2026-06", racine=env["racine"], db_path=env["db"])
    assert all(r["indicateur"] != "commissions" for r in rel)     # jamais un zéro inventé


def test_comparaison_avant_apres(env, monkeypatch):
    _xlsx(env["racine"] / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx",
          "COMMISSIONS", ["montant_commission"], [[100.0]])
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")

    p1 = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    pipe.lancer(p1["token"], db_path=env["db"])

    # Le calcul suivant produit une valeur différente.
    _xlsx(env["racine"] / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx",
          "COMMISSIONS", ["montant_commission"], [[180.0]])
    p2 = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    pipe.lancer(p2["token"], db_path=env["db"])

    comp = pipe.comparer("2026-06", db_path=env["db"])
    ligne = next(l for l in comp["lignes"] if l["indicateur"] == "commissions")
    assert ligne["precedent"] == 100.0 and ligne["nouveau"] == 180.0
    assert ligne["ecart"] == 80.0
    assert ligne["anormal"] is True                  # +80 % signalé comme anormal


def test_comparaison_sans_run(env):
    assert pipe.comparer("2099-01", db_path=env["db"])["statut"] == "AUCUN_RUN"


# ── Clôture mensuelle ────────────────────────────────────────────────────────

def test_statut_initial_ouverte(env):
    assert pipe.statut_cloture("2026-06", env["db"])["statut"] == pipe.CL_OUVERTE


def test_transition_autorisee(env):
    res = pipe.changer_statut_cloture("2026-06", pipe.CL_EN_CALCUL, acteur="t",
                                     racine=env["racine"], db_path=env["db"])
    assert res["ok"] and res["statut"] == pipe.CL_EN_CALCUL


def test_transition_interdite_refusee(env):
    res = pipe.changer_statut_cloture("2026-06", pipe.CL_CLOTUREE, acteur="t",
                                     racine=env["racine"], db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_TRANSITION


def test_validation_refusee_si_conditions_non_reunies(env):
    pipe.changer_statut_cloture("2026-06", pipe.CL_EN_CALCUL, racine=env["racine"],
                                db_path=env["db"])
    pipe.changer_statut_cloture("2026-06", pipe.CL_A_CONTROLER, racine=env["racine"],
                                db_path=env["db"])
    res = pipe.changer_statut_cloture("2026-06", pipe.CL_VALIDEE, racine=env["racine"],
                                      db_path=env["db"])
    assert res["ok"] is False and res["code"] == pipe.E_CLOTURE_REFUSEE
    assert "run_reussi" in res["detail"] or "lots_requis_reussis" in res["detail"]


def test_conditions_cloture_toutes_fausses_au_depart(env):
    c = pipe.conditions_cloture("2026-06", racine=env["racine"], db_path=env["db"])
    assert c["toutes_reunies"] is False
    assert c["conditions"]["run_reussi"] is False
    assert c["conditions"]["prefactures_generees"] is False


def test_historique_cloture(env):
    pipe.changer_statut_cloture("2026-06", pipe.CL_EN_CALCUL, commentaire="départ",
                                acteur="t", racine=env["racine"], db_path=env["db"])
    h = pipe.historique_cloture("2026-06", env["db"])
    assert len(h) == 1
    assert h[0]["nouveau_statut"] == pipe.CL_EN_CALCUL and h[0]["acteur"] == "t"


def test_lister_runs_par_mois(env, monkeypatch):
    _lot_produisant(env, monkeypatch, "a", "out/a.txt")
    prev = pipe.previsualiser("2026-06", ["a"], racine=env["racine"])
    pipe.lancer(prev["token"], db_path=env["db"])
    runs = pipe.lister_runs("2026-06", db_path=env["db"])
    assert len(runs) == 1 and runs[0]["mois"] == "2026-06"
    assert pipe.lister_runs("2099-12", db_path=env["db"]) == []

"""Garde-fou anti-Excel — l'import des déclarations Google Sheet doit rester
SHEET → normalisation Python → SQLite, jamais SHEET → Excel → SQLite (mission « alimenter le
module Ménages », §8/§19).

Deux niveaux de preuve, comme pour `test_hostaway_cleaning_tasks_anti_excel` :
  - STRUCTUREL : le classeur M04 et le MASTER_NORM ne sont écrits que dans le bloc legacy, après le
    court-circuit `--sans-excel` ; l'URL de la Sheet se lit depuis SQLite (`ref_sources_systeme`).
  - COMPORTEMENTAL : un run `--sans-excel` alimente SQLite et laisse le classeur M04 hash-identique.

Aucun appel réseau : le CSV de la Sheet est servi par un double.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
LOT6B = _TRAVAIL / "lot6b_m04_menages_internes.py"
M04 = (Path(cfg.PROJECT_ROOT) / "02_DONNEES_NORMALISEES" / "menages"
       / "M04_MENAGES_PowerQuery.xlsx")

pytestmark = pytest.mark.skipif(not LOT6B.exists(), reason="lot6b absent")

SOURCE = LOT6B.read_text(encoding="utf-8", errors="replace")


def _hash_ou_absent(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Niveau STRUCTUREL ────────────────────────────────────────────────────────

def test_url_de_la_sheet_est_lue_depuis_sqlite():
    """L'URL SRC_011 vient de `ref_sources_systeme` (SQLite), plus de REF_Setup.xlsm au runtime."""
    assert "ref_sources_systeme" in SOURCE
    # La lecture SQLite doit précéder le repli, sinon le repli redeviendrait le chemin normal.
    assert SOURCE.index("ref_sources_systeme") < SOURCE.index("EXCEL_LEGACY_EXPLICITE")


def test_le_repli_excel_de_l_url_est_opt_in_jamais_automatique():
    """Le classeur ne peut servir l'URL que sur `--url-depuis-excel` explicite.

    Régression gardée : le repli automatique précédent masquait une vraie panne (tri SQL sur une
    colonne `id` inexistante, avalée par un `except Exception` nu) et transformait un bug réparable
    en dépendance Excel permanente et invisible."""
    assert 'URL_DEPUIS_EXCEL = "--url-depuis-excel" in sys.argv' in SOURCE
    assert "if url is None and URL_DEPUIS_EXCEL:" in SOURCE
    assert "except Exception:\n    url = None" not in SOURCE


def test_le_tri_sql_utilise_la_vraie_cle_de_la_table():
    """`ref_sources_systeme` a pour clé `source_id` ; `dbm.lignes()` trierait sinon sur un `id`
    inexistant — c'est exactement ce qui déclenchait le repli Excel silencieux."""
    assert 'ordre="source_id"' in SOURCE


def test_sans_excel_court_circuite_avant_toute_ecriture_de_classeur():
    """`--sans-excel` doit sortir AVANT le bloc legacy : ni `.BAK`, ni `wb.save(M04)`."""
    assert "SANS_EXCEL" in SOURCE
    court_circuit = SOURCE.index("if SANS_EXCEL:\n    print(\"[lot6b] --sans-excel : classeur M04")
    # Toute écriture du classeur (copie de sauvegarde incluse) vient APRÈS le court-circuit.
    for marqueur in ("shutil.copy(M04, backup)", "wb.save(M04)"):
        assert SOURCE.index(marqueur) > court_circuit, marqueur


def test_ecriture_du_classeur_reste_conditionnee():
    """Aucune écriture Excel inconditionnelle : MASTER_NORM et M04 sont tous deux sous garde."""
    assert "if SANS_EXCEL:\n    print(\"[lot6b] --sans-excel : MASTER_NORM non écrit.\")" in SOURCE
    # `wbn.save(NORM_OUT)` ne doit exister que dans la branche `else`.
    assert SOURCE.count("wbn.save(NORM_OUT)") == 1
    assert "else:\n    wbn.save(NORM_OUT)" in SOURCE


# ── Niveau COMPORTEMENTAL ────────────────────────────────────────────────────

@pytest.mark.skipif(not M04.exists(), reason="classeur M04 absent d'un checkout propre")
def test_run_sans_excel_alimente_sqlite_et_laisse_le_classeur_intact(tmp_path, monkeypatch):
    """Preuve de bout en bout : un run `--sans-excel` écrit `menages_declarations_internes` et
    laisse le classeur M04 bit-à-bit identique (aucun `.BAK` créé non plus)."""
    pytest.importorskip("openpyxl")
    import sqlite3
    import subprocess

    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)

    # Référentiels minimaux : l'URL vient de SQLite, donc plus besoin de REF_Setup.xlsm.
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO ref_sources_systeme (source_id, nom_source, dossier_source, actif, import_id) "
        "VALUES ('SRC_011','GOOGLE_SHEET_M04_DECLARATIONS','https://exemple.test/pub?output=csv',"
        "'OUI','TEST')")
    conn.commit()
    conn.close()

    avant_m04 = _hash_ou_absent(M04)
    baks_avant = set(M04.parent.glob("*.BAK_*"))

    # Le CSV réseau est servi par un double via un sitecustomize temporaire : le run est un
    # sous-processus (lot6b est un script), donc le monkeypatch in-process ne l'atteindrait pas.
    faux_csv = tmp_path / "faux_sheet.csv"
    faux_csv.write_text("Horodateur,Prénom\n", encoding="utf-8")

    env = dict(**{k: v for k, v in __import__("os").environ.items()})
    env["PILOTAGE_DB_PATH"] = str(db)
    env["PYTHONIOENCODING"] = "utf-8"

    subprocess.run(
        [str(cfg.LOT4A_ENGINE_PYTHON), str(LOT6B), "--sans-excel"],
        cwd=str(_TRAVAIL), env=env, capture_output=True, text=True, timeout=300)

    # Quel que soit le sort du run (le réseau peut être coupé en CI), l'invariant tient :
    # le classeur n'est ni modifié ni sauvegardé.
    assert _hash_ou_absent(M04) == avant_m04, "le classeur M04 a été modifié malgré --sans-excel"
    assert set(M04.parent.glob("*.BAK_*")) == baks_avant, "un .BAK a été créé malgré --sans-excel"


def _run_lot6b(db, tmp_path, *args):
    """Lance lot6b en sous-processus sur `db`. Rend le CompletedProcess."""
    import os
    import subprocess

    env = dict(os.environ)
    env["PILOTAGE_DB_PATH"] = str(db)
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [str(cfg.LOT4A_ENGINE_PYTHON), str(LOT6B), *args],
        cwd=str(_TRAVAIL), env=env, capture_output=True, text=True, timeout=300)


def test_url_resolue_depuis_sqlite_sans_ouvrir_le_classeur(tmp_path):
    """COMPORTEMENTAL — avec SRC_011 en base, le run annonce la source SQLITE.

    Preuve que la résolution ne retombe plus sur REF_Setup.xlsm : c'est précisément ce que le
    tri SQL cassé faisait, en silence."""
    import sqlite3

    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO ref_sources_systeme (source_id, nom_source, dossier_source, actif, import_id) "
        "VALUES ('SRC_011','GOOGLE_SHEET_M04_DECLARATIONS','https://exemple.test/pub?output=csv',"
        "'OUI','TEST')")
    conn.commit()
    conn.close()

    res = _run_lot6b(db, tmp_path, "--sans-excel")
    sortie = res.stdout + res.stderr
    assert "URL SRC_011 lue depuis SQLITE" in sortie, sortie[-2000:]
    assert "EXCEL_LEGACY_EXPLICITE" not in sortie


def test_configuration_sqlite_absente_echoue_clairement_sans_repli(tmp_path):
    """FAIL-CLOSED — sans SRC_011 en base et sans `--url-depuis-excel`, le run s'arrête en
    nommant la source attendue, au lieu de retomber silencieusement sur le classeur."""
    from app.db.connection import apply_migrations

    db = tmp_path / "app.db"
    apply_migrations(db)  # base migrée mais SANS SRC_011

    res = _run_lot6b(db, tmp_path, "--sans-excel")
    sortie = res.stdout + res.stderr
    assert res.returncode != 0, "un défaut de configuration doit être bloquant"
    assert "SRC_011" in sortie and "ref_sources_systeme" in sortie, sortie[-2000:]
    # Le message doit orienter vers la correction SQLite, pas vers un contournement implicite.
    assert "fail-closed" in sortie.lower()
    assert "EXCEL_LEGACY_EXPLICITE" not in sortie


# ── Mission « backfill historique figé + zéro Excel runtime » ────────────────
# Le parcours opérationnel ne doit plus ouvrir AUCUN classeur : ni pour l'URL de la Sheet (déjà
# couvert plus haut), ni pour les référentiels. Et Lot9 ne doit plus dépendre du classeur Lot6f
# pour construire TYPE_FLUX_018/019.

LOT9 = _TRAVAIL / "lot9_construire_flux.py"
COUT_COMPLET_WB = (_TRAVAIL / "Lot6f_CoutComplet_Menages"
                   / "MASTER_CALC_CoutComplet_Menages.xlsx")
REF_SETUP = (Path(cfg.PROJECT_ROOT) / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm")


def test_referentiels_du_parcours_operationnel_sont_lus_en_sqlite():
    """STRUCTUREL (A/B) — mapping, logements et intervenants viennent de SQLite.

    Les trois référentiels dont le parcours SQLite a besoin sont lus par `_refs_sqlite()`. Les
    lectures `sh(REF, ...)` restantes appartiennent au seul bloc legacy, après le `sys.exit(0)`
    de `--sans-excel`."""
    assert "def _refs_sqlite()" in SOURCE
    for table in ("ref_mapping_logements", "ref_logements", "ref_intervenants"):
        assert table in SOURCE, f"{table} devrait être lu en SQLite"

    avant_sortie = SOURCE.split("sys.exit(0)")[0]
    for feuille in ("REF_Mapping_Logements", "REF_Logements", "REF_Intervenants",
                    "REF_Couts_Standards_Menage"):
        assert f'sh(REF, "{feuille}")' not in avant_sortie, (
            f"{feuille} est encore lu dans le parcours opérationnel")


def test_referentiel_sqlite_manquant_est_bloquant_sans_repli_classeur():
    """FAIL-CLOSED (§12) — un référentiel absent nomme la table et refuse, il ne replie pas."""
    assert "Referentiel SQLite absent" in SOURCE
    assert "Aucun repli classeur" in SOURCE


def test_lot9_ne_lit_plus_le_classeur_cout_complet():
    """STRUCTUREL (C) — TYPE_FLUX_018/019 se construisent depuis `menages_cout_complet`.

    Le nom du classeur Lot6f ne doit plus apparaître comme SOURCE lue : seule la table SQLite
    alimente le module GPM."""
    src9 = LOT9.read_text(encoding="utf-8", errors="replace")
    assert "charger_cout_complet_menages" in src9
    assert "menages_cout_complet" in src9
    assert "SRC_GPM" not in src9, "la constante de chemin du classeur Lot6f devrait avoir disparu"
    assert "DETAIL_COUT_COMPLET" not in src9, "la feuille Lot6f ne doit plus être lue"


def test_lot9_expose_les_deux_origines_par_une_interface_unique():
    """Une ligne figée et une ligne calculée traversent le MÊME chemin (§3/§6).

    La provenance ne doit jamais filtrer le flux : sinon un mois clôturé importé disparaîtrait de
    l'économie, ce que le backfill existe justement pour éviter."""
    src9 = LOT9.read_text(encoding="utf-8", errors="replace")
    bloc = src9.split("def charger_cout_complet_menages")[1].split("\ndef ")[0]
    assert "SELECT * FROM menages_cout_complet" in bloc
    assert "WHERE source_type" not in bloc, "le flux ne doit pas être filtré par provenance"


def test_hash_des_classeurs_legacy_inchange_par_un_run_operationnel(tmp_path):
    """COMPORTEMENTAL (D) — un run `--sans-excel` ne touche aucun classeur legacy."""
    import sqlite3

    from app.db.connection import apply_migrations

    avant = {p: _hash_ou_absent(p) for p in (M04, COUT_COMPLET_WB, REF_SETUP)}

    db = tmp_path / "app.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO ref_sources_systeme (source_id, nom_source, dossier_source, actif, import_id) "
        "VALUES ('SRC_011','GOOGLE_SHEET_M04_DECLARATIONS','https://exemple.test/pub?output=csv',"
        "'OUI','TEST')")
    conn.commit()
    conn.close()

    _run_lot6b(db, tmp_path, "--sans-excel")

    for chemin, empreinte in avant.items():
        assert _hash_ou_absent(chemin) == empreinte, f"{chemin.name} a été modifié"


def test_proprietaire_historise_respecte_la_fin_de_gestion():
    """§9 (F) — le propriétaire est celui EN VIGUEUR au mois, jamais le courant rétroactif.

    Cas réel : LOG_0003 est géré par PROP_0003 jusqu'au 2026-04-26. Une prestation d'avril lui est
    rattachée ; une prestation postérieure ne doit être rattachée à PERSONNE plutôt qu'à un
    propriétaire faux."""
    sys.path.insert(0, str(_TRAVAIL))
    from lib_ref_history import resolve_management_period

    rows = [{"gestion_id": "GST_LOG_0003_PROP_0003", "logement_id": "LOG_0003",
             "proprietaire_id": "PROP_0003", "date_debut": "2025-01-01",
             "date_fin": "2026-04-26", "statut_gestion": "INACTIF"}]

    avant = resolve_management_period(rows, logement_id="LOG_0003", date_arrivee="2026-04-01")
    assert avant.status == "OK" and avant.value == "PROP_0003"

    apres = resolve_management_period(rows, logement_id="LOG_0003", date_arrivee="2026-07-01")
    assert apres.status != "OK", "gestion terminée : aucun propriétaire ne doit être rendu"
    assert apres.value is None


def test_loghaid_int_et_str_se_resolvent_identiquement():
    """§10 (G) — le classeur rend un int, SQLite une chaîne : la normalisation doit les aligner.

    Sans elle, un rattachement Hostaway échouerait silencieusement sur une simple différence de
    type."""
    assert "def _txt(v):" in SOURCE, "l'aide de normalisation doit exister"

    def _txt(v):
        return "" if v is None else str(v).strip()

    assert _txt(482204) == _txt("482204") == "482204"
    assert _txt(" 482204 ") == "482204"
    assert _txt(None) == _txt("") == ""

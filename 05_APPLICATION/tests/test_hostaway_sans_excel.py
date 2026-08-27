"""Non-régression mission 14 (activation réelle contrôlée) — Hostaway ne doit plus écrire de
MASTER_*.xlsx quand il est déclenché par l'orchestrateur/le scheduler/le bouton manuel de l'app.

Contexte du bug trouvé pendant l'audit §15 (écritures hors SQLite) : `orchestrateur_moteur.
executer_lot10` passe déjà `--sans-excel` à Lot10, mais `hostaway_actualisation_service.actualiser`
ne passait PAS l'équivalent à Lot1 — un run réel aurait donc réécrit `MASTER_REF_HA_Listings.xlsx`,
`MASTER_FACT_HA_Reservations.xlsx` et consorts dans `02_TRAVAIL/Lot1_Hostaway/`, alors que rien en
aval ne les relit plus (RESERVATIONS/MENAGES n'ont aucun `service` dans le DAG tant que
lot4quater/lot6b/lot6c ne savent pas lire SQLITE). Corrigé : `ARGUMENTS_DEFAUT` inclut désormais
`--sans-excel`. Un second bug existait dans `lot1_hostaway_extract.py` lui-même : la réécriture de
`MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` et `MASTER_CTRL_HA_Anomalies.xlsx` (section « CLEANING
TASKS ») n'était PAS protégée par `--sans-excel`, contrairement aux autres masters — corrigé pour
que le même flag couvre tout le bloc.

Test structurel (pas un run complet du script legacy, qui exigerait de simuler l'API Hostaway en
entier pour un bénéfice marginal — cf. principe mission "pas 100 tests E2E fragiles") : on vérifie
la CONFIGURATION réellement utilisée par l'app (`ARGUMENTS_DEFAUT`), et l'INVARIANT de structure
du script legacy (le bloc CleaningTasks/Anomalies reste bien sous le même garde `sans_excel` que
le bloc des masters principaux).
"""
from __future__ import annotations

from pathlib import Path

from app.db.connection import apply_migrations
from app.services import hostaway_actualisation_service as svc


def test_arguments_defaut_incluent_sans_excel():
    assert "--sans-excel" in svc.ARGUMENTS_DEFAUT, (
        "un run Hostaway réel (orchestrateur/scheduler/bouton manuel) ne doit plus écrire les "
        "MASTER_*.xlsx legacy — rien en aval ne les relit (cf. orchestrateur_dag.py)")


def test_commande_construite_porte_sans_excel(monkeypatch, tmp_path):
    """`actualiser()` doit transmettre --sans-excel au sous-processus, pas seulement le déclarer
    en constante — verrouille contre un futur appel qui passerait `arguments=` explicitement sans
    le garder."""
    captes = {}

    class FauxProcCompleted:
        returncode = 0
        stdout = ""
        stderr = ""
        pid = 4242

    def faux_run(commande, **kwargs):
        captes["commande"] = commande
        return FauxProcCompleted()

    monkeypatch.setattr(svc, "actualisation_en_cours", lambda **k: None)
    monkeypatch.setattr(svc, "_interpreteur", lambda: "python")
    monkeypatch.setattr(svc, "_racine_moteur", lambda: tmp_path)
    (tmp_path / svc.SCRIPT).write_text("", encoding="utf-8")
    monkeypatch.setattr(svc.subprocess, "run", faux_run)

    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    svc.actualiser(db_path=db_path, attendre=True)
    assert "--sans-excel" in captes["commande"], (
        f"commande sous-processus sans --sans-excel : {captes['commande']}")


def test_lot1_cleaning_tasks_et_anomalies_sous_le_meme_garde_sans_excel():
    """Invariant structurel du script legacy : le bloc qui écrit
    MASTER_FACT_HA_CleaningTasks_Discovery.xlsx et MASTER_CTRL_HA_Anomalies.xlsx (après l'extraction
    des tâches ménage) doit rester sous le même `if args.sans_excel` que le bloc des masters
    principaux — jamais un `write_excel(` inconditionnel entre les deux marqueurs."""
    racine = Path(__file__).resolve().parent.parent.parent
    lot1 = racine / "02_TRAVAIL" / "lot1_hostaway_extract.py"
    assert lot1.exists(), "lot1_hostaway_extract.py introuvable — chemin de test à ajuster"
    source = lot1.read_text(encoding="utf-8")

    debut = source.index("# ── CLEANING TASKS (non-bloquant) ")
    fin = source.index("# ── MASTER_RUN_LOG")
    bloc = source[debut:fin]

    assert "if args.sans_excel:" in bloc, (
        "le bloc CleaningTasks/Anomalies doit être protégé par --sans-excel")
    garde_idx = bloc.index("if args.sans_excel:")
    apres_garde = bloc[garde_idx:]
    assert "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx" in apres_garde
    assert "MASTER_CTRL_HA_Anomalies.xlsx" in apres_garde
    # Aucun write_excel AVANT le garde dans ce bloc (seul le garde lui-même doit précéder les écritures).
    avant_garde = bloc[:garde_idx]
    assert "write_excel(" not in avant_garde, (
        "un write_excel() apparaît avant le garde --sans-excel dans le bloc CleaningTasks/Anomalies")

"""APP-3b — Preuve de bout en bout : prévisualisation → confirmation → écriture → lots aval.

Environnement ENTIÈREMENT isolé : tous les chemins (SAISIE, impacts, MASTER, Lot7, REF, verrou,
journal SQLite, dry-runs) sont injectés sous tmp_path. **Aucun test n'ouvre les quatre Excel réels
ni la base app.db réelle** — un test le vérifie explicitement par empreinte.

Les flags ne sont forcés à True qu'en mémoire (monkeypatch), jamais dans le dépôt.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.services import charges_confirmation_service as confirmation
from app.services import charges_impacts_persist_service as persist
from app.services import charges_post_write_service as aval
from app.services import charges_preview_service as prev
from app.services import saisie_charges_journal_service as jrn
from app.services import saisie_charges_lock_service as lk
from app.services import saisie_charges_transaction_service as tx

REELS = [cfg.SAISIE_CHARGES, cfg.SAISIE_CHARGES_IMPACTS, cfg.MASTER_CHARGES, cfg.SAISIE_IK_AVANTAGES]
PYTHON_MOTEUR = cfg.LOT4A_ENGINE_PYTHON


def _sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


# ── Environnement isolé ──────────────────────────────────────────────────────

@pytest.fixture
def flags_on(monkeypatch):
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED", True)


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    """Copie des vrais classeurs (lecture seule) vers un environnement 100 % isolé.

    Les copies sont les cibles d'écriture ; les originaux ne sont JAMAIS ouverts en écriture.
    """
    e = tmp_path / "env"
    e.mkdir()
    saisie = e / "SAISIE_Charges_Flux.xlsx"
    impacts = e / "SAISIE_Charges_Impacts.xlsx"
    master = e / "MASTER_FACT_MAN_Charges.xlsx"
    lot7 = e / "MASTER_FACT_MAN_IK_Avantages.xlsx"
    shutil.copy2(cfg.SAISIE_CHARGES, saisie)
    shutil.copy2(cfg.SAISIE_CHARGES_IMPACTS, impacts)
    shutil.copy2(cfg.MASTER_CHARGES, master)
    shutil.copy2(cfg.SAISIE_IK_AVANTAGES, lot7)

    # La prévisualisation lit cfg.SAISIE_CHARGES / cfg.SAISIE_CHARGES_IMPACTS : on les fait pointer
    # sur les copies, en mémoire uniquement. Rien de réel n'est ouvert en écriture.
    monkeypatch.setattr(cfg, "SAISIE_CHARGES", saisie)
    monkeypatch.setattr(cfg, "SAISIE_CHARGES_IMPACTS", impacts)

    return {
        "root": e,
        "saisie": saisie, "impacts": impacts, "master": master, "lot7": lot7,
        "ref": cfg.REF_SETUP,                     # référentiel : LECTURE seule
        "dryruns": e / "dryruns",
        "lock": e / "verrous" / lk.LOCK_NAME,
        "db": e / "journal" / "journal.db",
    }


def _confirmer(env, **kw):
    """Confirmation avec TOUS les chemins injectés — jamais un chemin de production."""
    params = dict(
        dryruns_root=env["dryruns"], saisie_path=env["saisie"], impacts_path=env["impacts"],
        master_path=env["master"], lot7_path=env["lot7"], ref_path=env["ref"],
        lock_path=env["lock"], db_path=env["db"], python_moteur=PYTHON_MOTEUR,
    )
    params.update(kw)
    token = params.pop("token")
    return confirmation.confirmer(token, **params)


def _previsualiser(env, form: dict) -> dict:
    res = prev.previsualiser(form, saisie_source=env["saisie"], dryruns_root=env["dryruns"],
                             ref_path=env["ref"])
    assert res["ok"], res["manifest"].get("errors")
    return res


def _form(**kw) -> dict:
    base = {
        "date_charge": "2026-06-15", "montant": "100.00", "categorie_charge_id": "CHG_025",
        "code_impact": "IC", "mode_paiement_id": "PAY_001",
        "impact_menage": "NON", "refacturable": "NON",
        "commentaire": "Test E2E APP-3b (environnement isolé).",
    }
    base.update(kw)
    return base


def _logements(n: int = 2) -> list[str]:
    refs = prev.load_form_refs()
    return [str(l["logement_id"]).strip() for l in refs["logements"]][:n]


def _lignes(path: Path, sheet: str) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        rows = list(wb[sheet].iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    entetes = [str(h) for h in rows[0]]
    return [dict(zip(entetes, r)) for r in rows[1:] if any(c is not None for c in r)]


def _charges_ecrites(env, charge_id: str) -> list[dict]:
    return [r for r in _lignes(env["saisie"], "SAISIE") if r.get("charge_id") == charge_id]


# ═════════════════════════════════════════════════════════════════════════════
# A → E : les cas métier, de bout en bout
# ═════════════════════════════════════════════════════════════════════════════

def test_A_charge_globale_simple(env, flags_on):
    """Prévisualisation → confirmation → écriture → Lot3 → Lot11. La charge devient visible."""
    p = _previsualiser(env, _form())
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    assert res.charge_id and res.charge_id.startswith("CHG-2026-06-IC-")
    assert len(_charges_ecrites(env, res.charge_id)) == 1

    # Les DEUX fichiers de saisie ont été remplacés, transactionnellement.
    assert sorted(res.transaction["fichiers_remplaces"]) == [
        "SAISIE_Charges_Flux.xlsx", "SAISIE_Charges_Impacts.xlsx"]
    assert res.transaction["statut_journal"] == jrn.SUCCES

    # Lot3 a régénéré le MASTER : la charge est désormais vue par les lots aval.
    post = res.post_ecriture
    assert post["lot3"]["statut"] == aval.OK, post
    assert post["lot3"]["donnees"]["nb_lignes"] == 1
    master = _lignes(env["master"], "MASTER")
    assert [m["charge_id"] for m in master] == [res.charge_id]
    assert master[0]["mois"] == "2026-06"                      # dérivé, jamais le cache Excel
    assert master[0]["impact_resultat_reel"] == "OUI"          # IC
    assert master[0]["impact_resultat_comptable"] == "OUI"

    assert post["lot7"]["statut"] == aval.NON_APPLICABLE       # pas d'avantage
    assert post["lot11"]["statut"] in (aval.OK, aval.ANOMALIES)
    assert post["ok"] is True

    # Le journal porte la trace, avec le token.
    traces = jrn.traces_du_token(p["token"], db_path=env["db"])
    assert len(traces) == 1 and traces[0]["statut"] == jrn.SUCCES


def test_B_charge_ventilee_sur_deux_logements(env, flags_on):
    logs = _logements(2)
    p = _previsualiser(env, _form(logements=logs))
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    aff = [a for a in _lignes(env["impacts"], "AFFECTATIONS") if a["charge_id"] == res.charge_id]
    assert len(aff) == 2
    assert round(sum(float(a["quote_part"]) for a in aff), 2) == 100.0   # jamais 2 × 100
    assert sorted(a["logement_id"] for a in aff) == sorted(logs)


def test_C_charge_menage(env, flags_on):
    refs = prev.load_form_refs()
    intervenant = str(refs["intervenants"][0]["intervenant_id"]).strip()
    p = _previsualiser(env, _form(
        categorie_charge_id="CHG_004", impact_menage="OUI",       # ménage FORCÉ
        menage_mode="INTERVENANT", menage_intervenants=[intervenant], menage_mois="2026-06",
    ))
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    men = [m for m in _lignes(env["impacts"], "MENAGE") if m["charge_id"] == res.charge_id]
    assert len(men) == 1
    assert men[0]["intervenant_id"] == intervenant
    assert men[0]["logement_id"] is None                       # intervenant XOR logement
    # Jamais de réserve pour une charge ménage.
    resv = [r for r in _lignes(env["impacts"], "RESERVE_REFACTURATION")
            if r["charge_id"] == res.charge_id]
    assert resv == []


def test_D_charge_avec_reserve_de_refacturation(env, flags_on):
    logs = _logements(1)
    p = _previsualiser(env, _form(categorie_charge_id="CHG_008", logements=logs, refacturable="OUI"))
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    resv = [r for r in _lignes(env["impacts"], "RESERVE_REFACTURATION")
            if r["charge_id"] == res.charge_id]
    assert len(resv) == 1
    assert resv[0]["statut_traitement"] == "EN_ATTENTE"        # aucune préfacture automatique
    assert float(resv[0]["montant_refacturable"]) == 100.0


def test_E_charge_avec_avantage_associe(env, flags_on):
    """L'avantage déclenche Lot7 — et reste HR : aucun impact résultat ni propriétaire."""
    refs = prev.load_form_refs()
    associe = str(refs["associes"][0]["personne_id"]).strip()
    p = _previsualiser(env, _form(avantage_associe="OUI", avantage_associe_id=associe))
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    ligne = _charges_ecrites(env, res.charge_id)[0]
    assert ligne["avantage_associe_id"] == associe

    post = res.post_ecriture
    assert post["lot7"]["statut"] == aval.OK, post
    calc = _lignes(env["lot7"], "MASTER_CALC_AVANTAGES")
    mien = [c for c in calc if c["associe_id"] == associe and c["mois"] == "2026-06"]
    assert len(mien) == 1
    assert mien[0]["code_impact"] == "HR"                      # hors résultat réel ET comptable
    assert float(mien[0]["avantages_nets"]) == 100.0
    assert "proprietaire_id" not in calc[0]                    # aucune dimension propriétaire


# ═════════════════════════════════════════════════════════════════════════════
# F → K : les refus
# ═════════════════════════════════════════════════════════════════════════════

def test_F_double_clic_meme_token(env, flags_on):
    """La seconde confirmation du même token n'écrit RIEN. Idempotence par token."""
    p = _previsualiser(env, _form())
    r1 = _confirmer(env, token=p["token"])
    assert r1.statut == confirmation.SUCCES

    empreintes = {k: _sha(env[k]) for k in ("saisie", "impacts")}
    r2 = _confirmer(env, token=p["token"])

    assert r2.statut == confirmation.REFUSE
    assert r2.code == tx.E_TOKEN_DEJA_ECRIT
    assert "déjà été enregistrée" in r2.message
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == empreintes   # aucun second effet
    assert len(_charges_ecrites(env, r1.charge_id)) == 1


def test_G_token_inconnu(env, flags_on):
    empreintes = {k: _sha(env[k]) for k in ("saisie", "impacts")}
    res = _confirmer(env, token="20260101T000000Z_inexistant00")

    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_TOKEN_INCONNU
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == empreintes


def test_H_manifest_altere(env, flags_on):
    """Le sceau du manifest est recalculé : une altération est détectée AVANT toute écriture."""
    p = _previsualiser(env, _form())
    manifest_path = Path(p["run_dir"]) / prev.MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["form_data"]["montant"] = "999999.00"             # tentative de gonfler le montant
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")

    empreintes = {k: _sha(env[k]) for k in ("saisie", "impacts")}
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_ALTERE
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == empreintes


def test_I_source_reelle_modifiee_depuis_la_previsualisation(env, flags_on):
    """La base a bougé depuis la prévisualisation : la décision est périmée, on refuse."""
    p = _previsualiser(env, _form())

    wb = openpyxl.load_workbook(env["saisie"])                 # quelqu'un a touché le fichier
    wb["SAISIE"]["AC500"] = "modification externe"
    wb.save(env["saisie"])
    wb.close()

    res = _confirmer(env, token=p["token"])
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_SOURCE_MODIFIEE
    assert "périmée" in res.message


def test_J_flags_desactives(env):
    """Sans flags : refus AVANT toute écriture, et refus TRACÉ au journal."""
    p = _previsualiser(env, _form())
    empreintes = {k: _sha(env[k]) for k in ("saisie", "impacts")}

    res = _confirmer(env, token=p["token"])                    # aucun monkeypatch de flags

    assert res.statut == confirmation.REFUSE
    assert res.code == tx.E_FLAGS_DESACTIVES
    assert "n'est pas activée" in res.message
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == empreintes
    assert not env["lock"].exists()                            # pas même un verrou

    traces = jrn.traces_du_token(p["token"], db_path=env["db"])
    assert len(traces) == 1 and traces[0]["statut"] == jrn.REFUSE_FLAGS


def test_K_verrou_deja_pris(env, flags_on):
    p = _previsualiser(env, _form())
    empreintes = {k: _sha(env[k]) for k in ("saisie", "impacts")}

    tenu = lk.acquerir_verrou(charge_id="AUTRE", lock_path=env["lock"])
    try:
        res = _confirmer(env, token=p["token"])
    finally:
        lk.liberer_verrou(tenu)

    assert res.statut == confirmation.REFUSE
    assert res.code == tx.E_VERROU_DEJA_PRIS
    assert "en cours" in res.message
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == empreintes


# ═════════════════════════════════════════════════════════════════════════════
# L → N : incidents
# ═════════════════════════════════════════════════════════════════════════════

def test_L_echec_lot3_apres_ecriture_reussie(env, flags_on):
    """La charge EST écrite. Un recalcul aval raté ne doit JAMAIS être présenté comme un échec
    d'écriture — et surtout ne doit pas annuler la charge."""
    p = _previsualiser(env, _form())
    master_impossible = env["root"] / "dossier_inexistant" / "MASTER.xlsx"

    res = _confirmer(env, token=p["token"], master_path=master_impossible)

    # L'écriture, elle, a bien abouti.
    assert res.statut == confirmation.SUCCES, res
    assert len(_charges_ecrites(env, res.charge_id)) == 1
    assert res.transaction["statut_journal"] == jrn.SUCCES

    # Mais le recalcul est en échec, et c'est dit explicitement.
    post = res.post_ecriture
    assert post["lot3"]["statut"] == aval.ECHEC, post
    assert post["ok"] is False
    assert "écriture est bien effectuée" in res.message
    assert "recalcul aval a échoué" in res.message


def test_M_rollback_transactionnel(env, flags_on, monkeypatch):
    """Le second remplacement échoue : tout est restauré, aucune charge écrite."""
    p = _previsualiser(env, _form())
    avant = {k: _sha(env[k]) for k in ("saisie", "impacts")}

    vrai = os.replace
    etat = {"n": 0}

    def faux(src, dst, *a, **kw):
        etat["n"] += 1
        if etat["n"] == 2:
            raise OSError("échec simulé du second remplacement")
        return vrai(src, dst, *a, **kw)

    monkeypatch.setattr(os, "replace", faux)
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.ROLLBACK
    assert res.code == tx.E_REMPLACEMENT
    assert res.transaction["rollback_reussi"] is True
    assert {k: _sha(env[k]) for k in ("saisie", "impacts")} == avant   # état initial rétabli
    assert res.post_ecriture is None                                   # aucun recalcul lancé


def test_N_panne_du_journal_apres_succes(env, flags_on, monkeypatch):
    """Le journal tombe après l'écriture : la charge reste écrite, la panne est SIGNALÉE."""
    p = _previsualiser(env, _form())

    monkeypatch.setattr(jrn, "cloturer_trace",
                        lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("panne")))
    res = _confirmer(env, token=p["token"])

    assert res.statut == confirmation.SUCCES, res
    assert len(_charges_ecrites(env, res.charge_id)) == 1      # la charge est bien là
    assert res.journal_erreur is not None
    assert "JOURNALISATION ÉCHOUÉE" in res.journal_erreur


# ═════════════════════════════════════════════════════════════════════════════
# Garde-fous globaux
# ═════════════════════════════════════════════════════════════════════════════

def test_aucun_fichier_reel_ni_base_reelle_touches(env, flags_on):
    """Le test le plus important : rien de réel n'est écrit, ni Excel, ni app.db."""
    reels = [p for p in REELS if p.exists()]
    empreintes = {p: _sha(p) for p in reels}
    db_reelle = Path(cfg.DB_PATH)
    db_avant = _sha(db_reelle) if db_reelle.exists() else None

    p = _previsualiser(env, _form())
    assert _confirmer(env, token=p["token"]).statut == confirmation.SUCCES

    assert {p: _sha(p) for p in reels} == empreintes
    if db_avant is not None:
        assert _sha(db_reelle) == db_avant                     # la base réelle n'a pas bougé


def test_le_navigateur_ne_fournit_aucune_donnee_metier():
    """persister_reel ne prend qu'un token : aucun payload métier n'est acceptable côté client."""
    import inspect

    params = list(inspect.signature(persist.persister_reel).parameters)
    assert params[0] == "token"
    # Le reste n'est que de l'injection de chemins (tests) — aucun champ métier.
    assert set(params) <= {"token", "options"}


def test_persister_reel_delegue_a_la_chaine_securisee(env, flags_on):
    """Le point d'entrée historique est bien branché sur l'orchestrateur (plus un stub)."""
    p = _previsualiser(env, _form())
    res = persist.persister_reel(
        p["token"], dryruns_root=env["dryruns"], saisie_path=env["saisie"],
        impacts_path=env["impacts"], master_path=env["master"], lot7_path=env["lot7"],
        ref_path=env["ref"], lock_path=env["lock"], db_path=env["db"],
        python_moteur=PYTHON_MOTEUR,
    )
    assert res.statut == confirmation.SUCCES, res
    assert res.transaction["transaction_id"]                   # passé par la transaction verrouillée


def test_verrou_et_temporaires_ne_laissent_aucun_residu(env, flags_on):
    p = _previsualiser(env, _form())
    assert _confirmer(env, token=p["token"]).statut == confirmation.SUCCES

    assert not env["lock"].exists()
    residus = [f.name for f in env["root"].iterdir()
               if f.is_file() and (".bak" in f.name or f.name.startswith("~$"))]
    assert residus == []

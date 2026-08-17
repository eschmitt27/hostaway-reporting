"""Classification bancaire en SQLite (Lot 8b) — règles, durcissements, fail-closed, parité.

La règle de décision est celle de `lot8b_banque_regles.py`, portée sans modification : premier match
dans l'ordre de priorité, quatre types de correspondance, et deux durcissements. Seule l'interface
de données change — c'est ce que ces tests vérifient.

Règles synthétiques ; un seul test lit le référentiel réel, pour la parité.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import banque_classification_service as cls
from app.services import banque_mouvements_service as bq

COMPTE = "COMPTE_DEMO"

_COLS_REGLE = ("regle_id", "priorite", "actif", "compte_id", "type_match", "champ_cible", "motif",
               "tiers_detecte", "categorie", "type_flux_id", "code_impact", "source_economique",
               "rapprochement_requis", "validation_automatique", "niveau_risque",
               "statut_controle_defaut", "statut_classification_defaut", "date_debut_validite",
               "date_fin_validite", "commentaire")


def _regle(regle_id, priorite, type_match, motif, **kw):
    d = {c: "" for c in _COLS_REGLE}
    d.update({"regle_id": regle_id, "priorite": str(priorite), "actif": "OUI",
              "compte_id": "*", "type_match": type_match, "champ_cible": "libelle",
              "motif": motif, "niveau_risque": "FAIBLE",
              "statut_controle_defaut": cls.ST_VALIDE,
              "statut_classification_defaut": cls.CLASS_CLASSE})
    d.update({k: str(v) for k, v in kw.items()})
    return d


@pytest.fixture
def db(tmp_path) -> Path:
    p = tmp_path / "banque.db"
    apply_migrations(p)
    return p


def _poser_regles(db: Path, regles: list[dict]) -> None:
    conn = get_db(db)
    try:
        trous = ", ".join(["?"] * (len(_COLS_REGLE) + 1))
        conn.executemany(
            f"INSERT INTO ref_banque_regles ({', '.join(_COLS_REGLE)}, import_id) "
            f"VALUES ({trous})",
            [tuple(r[c] for c in _COLS_REGLE) + ("IMP-TEST",) for r in regles])
        conn.commit()
    finally:
        conn.close()


def _mvt(libelle, montant=10.0, date_op="2026-01-05"):
    return {"external_transaction_id": "", "date_operation": date_op, "date_valeur": "",
            "sens": bq.SENS_DEBIT, "montant": montant, "devise": "EUR",
            "libelle_brut": libelle, "contrepartie_brute": ""}


def _importer(db, libelles):
    bq.importer([_mvt(l) for l in libelles], bank_account_id=COMPTE,
                source_type=bq.SOURCE_HISTORIQUE, db_path=db)


# ── Fail-closed ─────────────────────────────────────────────────────────────────────────────────

def test_sans_regles_le_service_refuse(db):
    """Se rabattre sur un classeur ou un seed produirait une classification plausible mais fausse."""
    _importer(db, ["PAIEMENT CB DEMO"])
    r = cls.classer(bank_account_id=COMPTE, db_path=db)
    assert r["ok"] is False
    assert r["code"] == cls.E_REGLES_ABSENTES
    assert "importez le référentiel" in r["message"].lower()


def test_sans_mouvement_le_service_le_dit(db):
    _poser_regles(db, [_regle("R_1", 10, cls.MATCH_CATCH_ALL, "*")])
    r = cls.classer(bank_account_id=COMPTE, db_path=db)
    assert r["ok"] is False and r["code"] == cls.E_AUCUN_MOUVEMENT


# ── Types de correspondance ─────────────────────────────────────────────────────────────────────

def test_commence_par(db):
    _poser_regles(db, [_regle("R_CP", 10, cls.MATCH_COMMENCE_PAR, "VIR AIRBNB")])
    _importer(db, ["VIR AIRBNB PAYMENTS 123"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_CP"


def test_contient(db):
    _poser_regles(db, [_regle("R_CT", 10, cls.MATCH_CONTIENT, "URSSAF")])
    _importer(db, ["PRLV SEPA URSSAF OCCITANIE"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_CT"


def test_regex(db):
    _poser_regles(db, [_regle("R_RX", 10, cls.MATCH_REGEX, r"CARTE\s+\d{4}")])
    _importer(db, ["PAIEMENT CARTE 8259 DEMO"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_RX"


def test_regex_invalide_n_arrete_pas_la_classification(db):
    """Une expression cassée dans le référentiel ne doit pas bloquer les autres mouvements."""
    _poser_regles(db, [_regle("R_KO", 10, cls.MATCH_REGEX, "([nonferme"),
                       _regle("R_OK", 20, cls.MATCH_CATCH_ALL, "*")])
    _importer(db, ["N IMPORTE QUOI"])
    r = cls.classer(bank_account_id=COMPTE, db_path=db)
    assert r["ok"] and r["nb_classes"] == 1
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_OK"


def test_premier_match_par_priorite(db):
    """La priorité décide, et elle est comparée en ENTIER : « 9 » passe avant « 10 »."""
    _poser_regles(db, [_regle("R_DIX", 10, cls.MATCH_CONTIENT, "DEMO"),
                       _regle("R_NEUF", 9, cls.MATCH_CONTIENT, "DEMO")])
    _importer(db, ["PAIEMENT DEMO"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_NEUF"


def test_regle_inactive_ignoree(db):
    _poser_regles(db, [_regle("R_OFF", 10, cls.MATCH_CONTIENT, "DEMO", actif="NON"),
                       _regle("R_ON", 20, cls.MATCH_CONTIENT, "DEMO")])
    _importer(db, ["PAIEMENT DEMO"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["regle_id"] == "R_ON"


def test_mouvement_sans_regle_est_signale_pas_invente(db):
    """Sans catch-all, un mouvement peut rester non classé — on le dit."""
    _poser_regles(db, [_regle("R_1", 10, cls.MATCH_CONTIENT, "INTROUVABLE")])
    _importer(db, ["AUTRE CHOSE"])
    r = cls.classer(bank_account_id=COMPTE, db_path=db)
    assert r["nb_classes"] == 0 and r["nb_sans_regle"] == 1
    assert cls.classifications(db_path=db) == []


# ── Durcissements repris de lot8b ───────────────────────────────────────────────────────────────

def test_risque_eleve_force_a_controler(db):
    """Un risque élevé ne peut jamais rester VALIDE, quoi que dise la règle."""
    _poser_regles(db, [_regle("R_E", 10, cls.MATCH_CATCH_ALL, "*", niveau_risque="ELEVE",
                              statut_controle_defaut=cls.ST_VALIDE)])
    _importer(db, ["PAIEMENT DEMO"])
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert cls.classifications(db_path=db)[0]["statut_controle"] == cls.ST_A_CONTROLER


@pytest.mark.parametrize("libelle", ["VIR REMBOURSEMENT DEMO", "PRLV REMBT DEMO"])
def test_remboursement_declenche_le_signalement(db, libelle):
    """Un remboursement inverse un flux déjà comptabilisé : toujours signalé."""
    _poser_regles(db, [_regle("R_R", 10, cls.MATCH_CATCH_ALL, "*")])
    _importer(db, [libelle])
    r = cls.classer(bank_account_id=COMPTE, db_path=db)
    assert r["ok"] and r["nb_classes"] == 1


# ── Le brut n'est jamais touché ─────────────────────────────────────────────────────────────────

def test_classer_ne_modifie_pas_le_brut(db):
    _poser_regles(db, [_regle("R_1", 10, cls.MATCH_CATCH_ALL, "*")])
    _importer(db, ["PAIEMENT CB DEMO"])
    avant = bq.mouvements(bank_account_id=COMPTE, db_path=db)
    cls.classer(bank_account_id=COMPTE, db_path=db)
    assert bq.mouvements(bank_account_id=COMPTE, db_path=db) == avant


def test_rejouer_produit_une_nouvelle_execution(db):
    """Deux exécutions restent comparables sur la même donnée brute."""
    _poser_regles(db, [_regle("R_1", 10, cls.MATCH_CATCH_ALL, "*")])
    _importer(db, ["PAIEMENT CB DEMO"])
    a = cls.classer(bank_account_id=COMPTE, db_path=db)["classification_run_id"]
    b = cls.classer(bank_account_id=COMPTE, db_path=db)["classification_run_id"]
    assert a != b
    assert len(cls.classifications(classification_run_id=a, db_path=db)) == 1
    assert len(cls.classifications(classification_run_id=b, db_path=db)) == 1
    assert cls.derniere_execution(db_path=db) == b, "la dernière exécution fait foi"


# ── §6 Parité sur le référentiel et le relevé réels ─────────────────────────────────────────────

RELEVE = Path(cfg.SOURCES_BRUTES) / "Banque" / \
    "BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx"
MASTER = Path(cfg.MASTER_BANQUE)
REF = Path(cfg.REF_SETUP)

reel_requis = pytest.mark.skipif(
    not (RELEVE.exists() and MASTER.exists() and REF.exists()),
    reason="relevé, master Lot8b ou référentiel absent de cet environnement")


@reel_requis
def test_parite_avec_la_classification_du_master(db, monkeypatch):
    """OLD (NORM_Banque classé par lot8b) contre NEW (SQLite) : mêmes répartitions.

    Les nombres attendus ne sont pas écrits en dur : ils sont LUS dans le master. Un volume
    bancaire évolue à chaque export ; c'est l'égalité entre les deux chemins qui doit tenir.
    """
    import collections
    import openpyxl

    from app.services import banque_adaptateurs as ad
    from app.services import ref_setup_import_service as refimp

    monkeypatch.setattr(cfg, "DB_PATH", db)
    assert refimp.importer(db_path=db)["ok"]
    bq.importer(ad.depuis_xlsx_releve_consolide(RELEVE),
                bank_account_id="CM_02211_00021321603",
                source_type=bq.SOURCE_HISTORIQUE, db_path=db)

    r = cls.classer(db_path=db)
    assert r["ok"], r
    rep = r["repartition"]

    wb = openpyxl.load_workbook(MASTER, read_only=True, data_only=True)
    try:
        ws = wb["NORM_Banque"]
        it = ws.iter_rows(values_only=True)
        h = [str(c) for c in next(it)]
        old = [dict(zip(h, row)) for row in it]
    finally:
        wb.close()

    assert r["nb_mouvements"] == len(old)
    assert r["nb_sans_regle"] == 0, "toute ligne doit trouver une règle"

    classe_old = collections.Counter(str(x.get("statut_classification")) for x in old)
    for statut in (cls.CLASS_RAPPROCHEMENT_REQUIS, cls.CLASS_CLASSE, cls.CLASS_A_ENVOYER_IA):
        assert rep.get(statut, 0) == classe_old.get(statut, 0), (
            f"{statut} : NEW {rep.get(statut, 0)} contre OLD {classe_old.get(statut, 0)}")

    ctrl_old = collections.Counter(str(x.get("statut_controle")) for x in old)
    for statut in (cls.ST_VALIDE, cls.ST_A_CONTROLER):
        assert rep.get(statut, 0) == ctrl_old.get(statut, 0)


@reel_requis
def test_parite_regle_par_regle(db, monkeypatch):
    """Au-delà des totaux : la MÊME règle doit s'appliquer au même libellé."""
    import openpyxl

    from app.services import banque_adaptateurs as ad
    from app.services import ref_setup_import_service as refimp

    monkeypatch.setattr(cfg, "DB_PATH", db)
    refimp.importer(db_path=db)
    bq.importer(ad.depuis_xlsx_releve_consolide(RELEVE),
                bank_account_id="CM_02211_00021321603",
                source_type=bq.SOURCE_HISTORIQUE, db_path=db)
    cls.classer(db_path=db)

    par_mvt = {m["mouvement_id_opaque"]: m["libelle_brut"].upper().strip()
               for m in bq.mouvements(db_path=db)}
    regle_new: dict[str, set] = {}
    for c in cls.classifications(db_path=db):
        regle_new.setdefault(par_mvt[c["mouvement_id_opaque"]], set()).add(c["regle_id"])

    wb = openpyxl.load_workbook(MASTER, read_only=True, data_only=True)
    try:
        ws = wb["NORM_Banque"]
        it = ws.iter_rows(values_only=True)
        h = [str(c) for c in next(it)]
        regle_old: dict[str, set] = {}
        for row in it:
            d = dict(zip(h, row))
            regle_old.setdefault(str(d.get("libelle") or "").upper().strip(), set()).add(
                str(d.get("regle_id_appliquee") or ""))
    finally:
        wb.close()

    divergences = [lib for lib, regles in regle_new.items()
                   if lib in regle_old and regles != regle_old[lib]]
    assert divergences == [], f"{len(divergences)} libellé(s) classés différemment : {divergences[:5]}"

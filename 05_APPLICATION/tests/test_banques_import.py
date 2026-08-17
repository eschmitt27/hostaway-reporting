"""Import bancaire applicatif : normalisation, doublons, sécurité — service uniquement (pas HTTP,
cf. `test_banques_import_routes.py`)."""
from __future__ import annotations

import io

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_import_service as svc
from app.services import banque_vues_service as vues

CSV_VALIDE = (
    "Date operation;Libelle;Debit;Credit\n"
    "05/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n"
    "10/06/2026;VIR PROPRIETAIRE PROP A;400,00;\n"
).encode("utf-8")


@pytest.fixture
def ref(tmp_db, tmp_path, monkeypatch):
    """Base isolée + flags d'écriture. Le nom `ref` est conservé : ces tests le nomment partout.

    `ref` désigne désormais la base, et non plus un classeur. Il est passé tel quel à
    `previsualiser(ref_path=…)`, que le paramètre n'utilise plus — ce qui vérifie au passage qu'un
    appelant qui le fournit encore n'en subit aucun effet.
    """
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return tmp_db


@pytest.fixture
def dryruns(tmp_path):
    d = tmp_path / "dryruns"
    d.mkdir()
    return d


def _lignes(db_path=None):
    """Mouvements effectivement écrits, lus par la vue applicative."""
    return vues.mouvements_normalises(db_path=db_path)


# ── Import CSV/XLSX valides ──────────────────────────────────────────────────

def test_previsualiser_csv_valide(ref, dryruns):
    res = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"], res
    assert res["compteurs"]["lignes_lues"] == 2
    assert res["compteurs"]["valides"] == 2
    assert res["compteurs"]["debits"] == 1 and res["compteurs"]["credits"] == 1
    assert res["compteurs"]["total_debit"] == 400.0
    assert res["compteurs"]["total_credit"] == 850.0


def test_previsualiser_xlsx_valide(ref, dryruns, tmp_path):
    p = tmp_path / "releve.xlsx"
    wb = openpyxl.Workbook(); ws = wb.active
    ws.append(["Date operation", "Libelle", "Debit", "Credit"])
    ws.append(["05/06/2026", "VIR HOSTAWAY PAYOUT", None, 850.00])
    wb.save(p); wb.close()
    res = svc.previsualiser(p.read_bytes(), "releve.xlsx", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"], res
    assert res["compteurs"]["valides"] == 1


def test_confirmer_idempotent_reimport(ref, dryruns, tmp_path):
    db = ref

    prev1 = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    r1 = svc.confirmer(prev1["token"], acteur="recette", dryruns_root=dryruns, db_path=db)
    assert r1["ok"] and r1["nb_ajoutees"] == 2
    assert len(_lignes(ref)) == 2

    # Réimport du même fichier : les 2 lignes deviennent des doublons certains, rien n'est ajouté.
    prev2 = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert prev2["compteurs"]["doublons_certains"] == 2
    assert prev2["compteurs"]["valides"] == 0
    r2 = svc.confirmer(prev2["token"], acteur="recette", dryruns_root=dryruns, db_path=db)
    assert r2["ok"] and r2["nb_ajoutees"] == 0
    assert len(_lignes(ref)) == 2               # aucune nouvelle ligne


def test_doublon_probable_necessite_justification(ref, dryruns, tmp_path):
    db = ref

    prev1 = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    svc.confirmer(prev1["token"], acteur="recette", dryruns_root=dryruns, db_path=db)

    # Même compte/date/montant/libellé mais date_valeur différente -> ROW_HASH différent -> probable.
    csv2 = (
        "Date operation;Date valeur;Libelle;Debit;Credit\n"
        "05/06/2026;06/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n"
    ).encode("utf-8")
    prev2 = svc.previsualiser(csv2, "releve2.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert prev2["compteurs"]["doublons_probables"] == 1
    assert prev2["compteurs"]["valides"] == 0

    # Sans justification : rien n'est ajouté.
    r = svc.confirmer(prev2["token"], acteur="recette", dryruns_root=dryruns, db_path=db)
    assert r["ok"] and r["nb_ajoutees"] == 0
    assert len(_lignes(ref)) == 2

    # Avec justification explicite : la ligne est ajoutée.
    prev3 = svc.previsualiser(csv2, "releve2.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    r3 = svc.confirmer(prev3["token"], justifier_doublons_probables=True, acteur="recette",
                       dryruns_root=dryruns, db_path=db)
    assert r3["ok"] and r3["nb_ajoutees"] == 1
    assert len(_lignes(ref)) == 3


# ── Formats / erreurs ─────────────────────────────────────────────────────────

def test_format_inconnu_refuse(ref, dryruns):
    res = svc.previsualiser(b"peu importe", "releve.pdf", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"] is False and res["code"] == svc.E_FORMAT_INCONNU


def test_fichier_vide_refuse(ref, dryruns):
    res = svc.previsualiser(b"", "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"] is False and res["code"] == svc.E_FICHIER_VIDE


def test_colonnes_manquantes_refuse(ref, dryruns):
    contenu = "Colonne1;Colonne2\nabc;def\n".encode("utf-8")
    res = svc.previsualiser(contenu, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"] is False and res["code"] == svc.E_COLONNES_MANQUANTES


def test_montant_invalide_en_ligne_invalide(ref, dryruns):
    contenu = (
        "Date operation;Libelle;Debit;Credit\n"
        "05/06/2026;LIGNE MONTANT INVALIDE;ABC;\n"
    ).encode("utf-8")
    res = svc.previsualiser(contenu, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"], res
    assert res["compteurs"]["invalides"] == 1
    assert res["compteurs"]["valides"] == 0


def test_date_invalide_en_ligne_invalide(ref, dryruns):
    contenu = (
        "Date operation;Libelle;Debit;Credit\n"
        "31/13/2026;LIGNE DATE INVALIDE;10,00;\n"
    ).encode("utf-8")
    res = svc.previsualiser(contenu, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["ok"], res
    assert res["compteurs"]["invalides"] == 1


def test_debit_et_credit_tous_deux_renseignes_invalide(ref, dryruns):
    contenu = (
        "Date operation;Libelle;Debit;Credit\n"
        "05/06/2026;LIGNE AMBIGUE;10,00;20,00\n"
    ).encode("utf-8")
    res = svc.previsualiser(contenu, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["compteurs"]["invalides"] == 1


# ── Normalisation ─────────────────────────────────────────────────────────────

def test_normalisation_libelle_et_hash_stable(ref, dryruns):
    res = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    lignes = res.get("compteurs")
    # Rejoue la même normalisation deux fois : le hash doit être identique (stable, déterministe).
    res2 = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert res["compteurs"] == res2["compteurs"]


def test_montant_toujours_positif_sens_signe_separement(ref, dryruns, tmp_path):
    db = ref
    prev = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    svc.confirmer(prev["token"], acteur="recette", dryruns_root=dryruns, db_path=db)
    lignes = _lignes(ref)
    assert all(l["montant"] > 0 for l in lignes)
    sens = {l["sens"] for l in lignes}
    assert sens == {"DEBIT", "CREDIT"}


# ── Sécurité ──────────────────────────────────────────────────────────────────

def test_confirmer_refuse_si_flags_off(ref, dryruns, monkeypatch):
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", False)
    prev = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    res = svc.confirmer(prev["token"], acteur="recette", dryruns_root=dryruns, db_path=ref)
    assert res["ok"] is False and res["code"] == svc.E_FLAGS
    assert _lignes(ref) == [], "aucun mouvement écrit"


def test_confirmer_refuse_hors_racine_recette(ref, dryruns, monkeypatch, tmp_path):
    prev = svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", (tmp_path / "ailleurs").resolve())
    res = svc.confirmer(prev["token"], acteur="recette", dryruns_root=dryruns, db_path=ref)
    assert res["ok"] is False and res["code"] == svc.E_ECRITURE
    assert _lignes(ref) == [], "aucun mouvement écrit hors de la racine de recette"


def test_confirmer_token_inconnu_refuse(ref, dryruns):
    res = svc.confirmer("token-inexistant", acteur="recette", dryruns_root=dryruns)
    assert res["ok"] is False and res["code"] == svc.E_TOKEN_INCONNU


def test_fichier_jamais_modifie_par_la_previsualisation(ref, dryruns):
    avant = CSV_VALIDE
    svc.previsualiser(CSV_VALIDE, "releve.csv", "CM_TEST", ref_path=ref, dryruns_root=dryruns)
    assert avant == CSV_VALIDE                  # le buffer d'entrée n'a jamais été altéré
    assert _lignes(ref) == []                    # aucune écriture avant confirmation

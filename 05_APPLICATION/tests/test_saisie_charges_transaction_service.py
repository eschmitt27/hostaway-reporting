"""APP-3b / commit 3 — orchestrateur transactionnel : atomicité applicative sur deux classeurs.

Aucun fichier métier réel n'est touché : tout se joue sur des fixtures en tmp_path (nommées comme
les vrais fichiers, car file_registry n'autorise que les `SAISIE_*`). Les flags réels restent False
dans le dépôt — ils ne sont forcés à True que par monkeypatch, dans le seul processus de test.

`os.replace` est monkeypatché pour provoquer un échec à chaque position de la séquence de commit,
et vérifier que le rollback ramène les fichiers à leur empreinte initiale.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import openpyxl
import pytest

import app.config as cfg
from app.readers.saisie_charges_reader import MANUAL_COL_MAP, _col_index
from app.services import saisie_charges_transaction_service as svc
from app.writers import saisie_charges_writer as writer

MONTANT = 100.0
CHARGE_ID = "CHG-2026-06-IC-BANQUE-001"
LIGNE = 2

FORMULES = {
    "C": '=IF(B{r}="","",TEXT(B{r},"YYYY-MM"))',
    "I": '=IF(H{r}="IC","OUI",IF(H{r}="HC","OUI",IF(H{r}="HR","NON","A_CONTROLER")))',
    "J": '=IF(H{r}="IC","OUI",IF(H{r}="HC","NON",IF(H{r}="HR","NON","A_CONTROLER")))',
    "AD": '=IF(A{r}="","",TEXT(B{r},"YYYYMMDD")&"|"&TEXT(D{r},"0.00")&"|"&F{r}&"|"&M{r})',
}

IMPACTS_HEADERS = {
    "AFFECTATIONS": ["affectation_id", "charge_id", "mois", "logement_id", "proprietaire_id",
                     "quote_part", "statut", "origine", "commentaire", "ROW_HASH"],
    "MENAGE": ["menage_impact_id", "charge_id", "mois", "mode", "intervenant_id", "logement_id",
               "statut", "origine", "commentaire", "ROW_HASH"],
    "RESERVE_REFACTURATION": ["reserve_id", "charge_id", "mois", "logement_id", "proprietaire_id",
                              "montant_refacturable", "libelle", "justificatif",
                              "statut_traitement", "trace_decision", "origine", "commentaire",
                              "ROW_HASH"],
}

REELS = [cfg.SAISIE_CHARGES, cfg.SAISIE_CHARGES_IMPACTS, cfg.MASTER_CHARGES, cfg.SAISIE_IK_AVANTAGES]


def _sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def flags_on(monkeypatch):
    """Les deux flags à True — UNIQUEMENT en mémoire, dans ce processus de test."""
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED", True)


@pytest.fixture
def cibles(tmp_path: Path) -> tuple[Path, Path]:
    saisie = tmp_path / "SAISIE_Charges_Flux.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "SAISIE"
    entetes = {**MANUAL_COL_MAP, "C": "mois", "I": "impact_resultat_reel",
               "J": "impact_resultat_comptable", "AD": "ROW_HASH"}
    for col, champ in entetes.items():
        ws.cell(row=1, column=_col_index(col), value=champ)
    for r in range(2, 7):
        for col, f in FORMULES.items():
            ws.cell(row=r, column=_col_index(col), value=f.format(r=r))
    wb.save(saisie)
    wb.close()

    impacts = tmp_path / "SAISIE_Charges_Impacts.xlsx"
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for sheet, headers in IMPACTS_HEADERS.items():
        wb.create_sheet(sheet).append(headers)
    wb["AFFECTATIONS"].append(["AUTRE-AFF-001", "CHG-AUTRE", "2026-05", "LOG_0009", "PROP_01",
                              42.0, "A_CONTROLER", "NOUVELLE_CHARGE_GUIDEE", None, "hash"])
    wb.save(impacts)
    wb.close()
    return saisie, impacts


def _demande(cibles, **kw) -> svc.DemandeEcritureCharge:
    saisie, impacts = cibles
    row_data = {
        "charge_id": CHARGE_ID, "date_charge": "2026-06-15", "montant": MONTANT,
        "sens_flux": "DEPENSE", "categorie_charge_id": "CHG_025", "type_flux_id": "TYPE_FLUX_020",
        "code_impact": "IC", "prise_en_compta": "OUI", "mode_paiement_id": "PAY_001",
        "affectation_type": "GLOBAL", "refacturable": "NON", "source_flux": "SAISIE_MANUELLE",
        "statut_controle": "A_CONTROLER", "niveau_anomalie": "INFO",
        "avantage_associe_id": "PERS_EWAN",
    }
    persistable = {
        "charge_id": CHARGE_ID,
        "affectations": [{
            "affectation_id": f"{CHARGE_ID}-AFF-001", "charge_id": CHARGE_ID, "mois": "2026-06",
            "logement_id": "LOG_0001", "proprietaire_id": "PROP_01", "quote_part": MONTANT,
            "statut": "A_CONTROLER", "origine": "NOUVELLE_CHARGE_GUIDEE", "commentaire": None,
            "ROW_HASH": "h1",
        }],
        "menage": [], "reserve": [],
    }
    params = {
        "charge_id": CHARGE_ID, "montant": MONTANT, "target_row": LIGNE,
        "row_data": row_data, "persistable": persistable,
        "saisie_path": saisie, "impacts_path": impacts,
    }
    params.update(kw)
    return svc.DemandeEcritureCharge(**params)


def _fichiers_du_dossier(p: Path) -> list[str]:
    return sorted(f.name for f in p.iterdir() if f.is_file())


def _replace_qui_echoue_a(appel_ko: int):
    """Fabrique un os.replace qui échoue au N-ième appel et délègue au vrai sinon."""
    vrai = os.replace
    etat = {"n": 0}

    def faux(src, dst, *a, **kw):
        etat["n"] += 1
        if etat["n"] == appel_ko:
            raise OSError(f"échec simulé au remplacement n°{appel_ko}")
        return vrai(src, dst, *a, **kw)

    return faux, etat


# ── 1-2 : succès et garde des flags ──────────────────────────────────────────

def test_1_transaction_reussie_sur_tous_les_fichiers(cibles, flags_on):
    saisie, impacts = cibles
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.statut == svc.STATUT_OK, res
    assert res.ok
    assert sorted(res.fichiers_remplaces) == sorted([str(impacts), str(saisie)])
    assert res.nettoyage_incomplet == []

    wb = openpyxl.load_workbook(saisie, read_only=True, data_only=True)
    lignes = [r for r in wb["SAISIE"].iter_rows(values_only=True) if r[0] == CHARGE_ID]
    wb.close()
    assert len(lignes) == 1

    wb = openpyxl.load_workbook(impacts, read_only=True, data_only=True)
    aff = [r for r in wb["AFFECTATIONS"].iter_rows(values_only=True) if r[1] == CHARGE_ID]
    wb.close()
    assert len(aff) == 1 and aff[0][5] == MONTANT

    # Les empreintes finales annoncées correspondent aux fichiers sur disque.
    assert res.sha256_finaux[saisie.name] == _sha(saisie)
    assert res.sha256_finaux[impacts.name] == _sha(impacts)


def test_2_refus_si_les_flags_sont_desactives(cibles):
    """Aucun monkeypatch : les flags réels (False) doivent bloquer AVANT toute préparation."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}
    dossier = _fichiers_du_dossier(saisie.parent)

    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.statut == svc.STATUT_GARDE
    assert res.code == svc.E_FLAGS_DESACTIVES
    assert "CHARGES_REAL_WRITE_ENABLED" in res.details
    assert {p: _sha(p) for p in (saisie, impacts)} == avant       # rien touché
    assert _fichiers_du_dossier(saisie.parent) == dossier         # aucun temporaire créé


@pytest.mark.parametrize("flag_on", ["CHARGES_REAL_WRITE_ENABLED",
                                     "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED"])
def test_2b_un_seul_flag_ne_suffit_pas(cibles, monkeypatch, flag_on):
    monkeypatch.setattr(cfg, flag_on, True)          # l'autre reste False
    res = svc.confirmer_ecriture_charge(_demande(cibles))
    assert res.statut == svc.STATUT_GARDE
    assert res.code == svc.E_FLAGS_DESACTIVES


# ── 3-4 : échec de préparation → aucun fichier réel modifié ──────────────────

def test_3_echec_preparation_du_premier_temporaire(cibles, flags_on):
    """Impacts (préparé en premier) invalide : Σ quote_part ≠ montant."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}
    dossier = _fichiers_du_dossier(saisie.parent)

    d = _demande(cibles)
    d.persistable["affectations"][0]["quote_part"] = 60.0        # Σ = 60 ≠ 100
    res = svc.confirmer_ecriture_charge(d)

    assert res.statut == svc.STATUT_ERREUR
    assert res.code == svc.E_PREPARATION
    assert res.fichiers_remplaces == []
    assert {p: _sha(p) for p in (saisie, impacts)} == avant
    assert _fichiers_du_dossier(saisie.parent) == dossier         # aucun temporaire laissé


def test_4_echec_preparation_du_temporaire_intermediaire(cibles, flags_on):
    """Impacts OK, puis la charge est refusée (colonne formule visée) : rien ne doit rester."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}
    dossier = _fichiers_du_dossier(saisie.parent)

    d = _demande(cibles)
    d.row_data["mois"] = "TRICHE"                                 # colonne formule → refus writer
    res = svc.confirmer_ecriture_charge(d)

    assert res.statut == svc.STATUT_ERREUR
    assert res.code == svc.E_PREPARATION
    assert "E_COLONNE_FORMULE" in res.details
    assert {p: _sha(p) for p in (saisie, impacts)} == avant
    # Le temporaire des impacts, déjà produit, a bien été nettoyé (test 15).
    assert _fichiers_du_dossier(saisie.parent) == dossier


def test_18_erreur_de_validation_writer_ne_touche_aucun_fichier(cibles, flags_on):
    """Charge ménage portant une réserve : refus métier du writer, aucun remplacement."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}

    d = _demande(cibles)
    d.persistable["affectations"] = []
    d.persistable["menage"] = [{
        "menage_impact_id": f"{CHARGE_ID}-MEN-001", "charge_id": CHARGE_ID, "mois": "2026-06",
        "mode": "INTERVENANT", "intervenant_id": "INT_0001", "logement_id": None,
        "statut": "A_CONTROLER", "origine": "NOUVELLE_CHARGE_GUIDEE", "commentaire": None,
        "ROW_HASH": "m1",
    }]
    d.persistable["reserve"] = [{
        "reserve_id": f"{CHARGE_ID}-RES-001", "charge_id": CHARGE_ID, "mois": "2026-06",
        "logement_id": "LOG_0001", "proprietaire_id": "PROP_01", "montant_refacturable": MONTANT,
        "libelle": "x", "justificatif": None, "statut_traitement": "EN_ATTENTE",
        "trace_decision": None, "origine": "NOUVELLE_CHARGE_GUIDEE", "commentaire": None,
        "ROW_HASH": "r1",
    }]
    res = svc.confirmer_ecriture_charge(d)

    assert res.statut == svc.STATUT_ERREUR
    assert "E_MENAGE_AVEC_RESERVE" in res.details
    assert {p: _sha(p) for p in (saisie, impacts)} == avant


# ── 5-6 : temporaires manquants ou invalides → aucun remplacement ────────────

def test_5_aucun_remplacement_si_un_temporaire_est_manquant(cibles, flags_on, monkeypatch):
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}
    vrai = writer.prepare_charge_flux_write

    def prepare_puis_supprime(*a, **kw):
        res = vrai(*a, **kw)
        Path(res["temp_path"]).unlink()               # le temporaire disparaît après préparation
        return res

    monkeypatch.setattr(writer, "prepare_charge_flux_write", prepare_puis_supprime)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.code == svc.E_TEMPORAIRE_MANQUANT
    assert res.fichiers_remplaces == []
    assert {p: _sha(p) for p in (saisie, impacts)} == avant


def test_6_aucun_remplacement_si_un_temporaire_est_invalide(cibles, flags_on, monkeypatch):
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}
    vrai = writer.prepare_charge_impacts_write

    def prepare_puis_corrompt(*a, **kw):
        res = vrai(*a, **kw)
        Path(res["temp_path"]).write_bytes(b"ceci n'est pas un classeur")
        return res

    monkeypatch.setattr(writer, "prepare_charge_impacts_write", prepare_puis_corrompt)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.code == svc.E_TEMPORAIRE_INVALIDE
    assert res.fichiers_remplaces == []
    assert {p: _sha(p) for p in (saisie, impacts)} == avant
    assert _fichiers_du_dossier(saisie.parent) == ["SAISIE_Charges_Flux.xlsx",
                                                   "SAISIE_Charges_Impacts.xlsx"]


# ── 7-8 : sauvegardes avant remplacement, ordre déterministe ─────────────────

def test_7_sauvegardes_creees_avant_le_premier_remplacement(cibles, flags_on, monkeypatch):
    saisie, impacts = cibles
    vu: list[list[str]] = []
    vrai = svc._remplacer_fichier

    def espion(temp, cible):
        vu.append(sorted(f.name for f in cible.parent.iterdir() if ".bak" in f.name))
        return vrai(temp, cible)

    monkeypatch.setattr(svc, "_remplacer_fichier", espion)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.ok, res
    # Dès le PREMIER remplacement, les deux sauvegardes existent déjà.
    assert len(vu[0]) == 2
    assert any("SAISIE_Charges_Flux" in n for n in vu[0])
    assert any("SAISIE_Charges_Impacts" in n for n in vu[0])


def test_8_ordre_deterministe_des_remplacements(cibles, flags_on, monkeypatch):
    """Impacts d'abord, charge ensuite : des impacts orphelins sont préférables à une charge
    sans impacts (qui, elle, serait comptée à tort par le résultat)."""
    saisie, impacts = cibles
    ordre: list[str] = []
    vrai = svc._remplacer_fichier

    def espion(temp, cible):
        ordre.append(cible.name)
        return vrai(temp, cible)

    monkeypatch.setattr(svc, "_remplacer_fichier", espion)
    assert svc.confirmer_ecriture_charge(_demande(cibles)).ok
    assert ordre == ["SAISIE_Charges_Impacts.xlsx", "SAISIE_Charges_Flux.xlsx"]


# ── 9-12 : rollback à chaque position + vérification des empreintes ──────────

def test_9_rollback_si_le_premier_remplacement_echoue(cibles, flags_on, monkeypatch):
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}

    faux, _ = _replace_qui_echoue_a(1)
    monkeypatch.setattr(os, "replace", faux)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.statut == svc.STATUT_ROLLBACK
    assert res.code == svc.E_REMPLACEMENT
    assert {p: _sha(p) for p in (saisie, impacts)} == avant       # test 12 : empreintes initiales
    assert res.nettoyage_incomplet == []


def test_10_11_rollback_si_le_dernier_remplacement_echoue(cibles, flags_on, monkeypatch):
    """La transaction porte deux cibles : la position « intermédiaire » et la position « dernière »
    coïncident. Ici les impacts SONT déjà remplacés — le rollback doit les défaire."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}

    faux, etat = _replace_qui_echoue_a(2)
    monkeypatch.setattr(os, "replace", faux)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.statut == svc.STATUT_ROLLBACK
    assert res.code == svc.E_REMPLACEMENT
    assert etat["n"] >= 2                                    # le 1er remplacement avait bien eu lieu
    # test 12 : les DEUX fichiers sont revenus à leur empreinte initiale (impacts restaurés).
    assert {p: _sha(p) for p in (saisie, impacts)} == avant
    assert "restaurés" in res.details


def test_11b_rollback_si_la_verification_post_commit_echoue(cibles, flags_on, monkeypatch):
    """Troisième position d'échec : après les deux remplacements, un fichier final est illisible."""
    saisie, impacts = cibles
    avant = {p: _sha(p) for p in (saisie, impacts)}

    # Illisible UNIQUEMENT pour les cibles (les temporaires, eux, sont validés normalement) :
    # l'échec se produit donc bien après les deux remplacements.
    monkeypatch.setattr(svc, "_lisible", lambda p: not Path(p).name.startswith("SAISIE_"))
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.statut == svc.STATUT_ROLLBACK
    assert res.code == svc.E_POST_COMMIT
    assert {p: _sha(p) for p in (saisie, impacts)} == avant       # tout est revenu en arrière


# ── 13 / 17 : rollback impossible → erreur critique, sauvegardes conservées ──

def test_13_17_erreur_critique_si_une_restauration_echoue(cibles, flags_on, monkeypatch):
    saisie, impacts = cibles

    faux, _ = _replace_qui_echoue_a(2)                # le 2e remplacement échoue
    monkeypatch.setattr(os, "replace", faux)

    def restauration_ko(sauvegarde, cible):
        raise OSError("restauration impossible (disque en lecture seule)")

    monkeypatch.setattr(svc, "_restaurer_fichier", restauration_ko)

    with pytest.raises(svc.RollbackCritiqueError) as exc:
        svc.confirmer_ecriture_charge(_demande(cibles))

    err = exc.value
    assert len(err.fichiers_non_restaures) == 1
    assert "SAISIE_Charges_Impacts" in err.fichiers_non_restaures[0]["cible"]
    assert "ROLLBACK INCOMPLET" in str(err)

    # test 17 : la sauvegarde du fichier non restauré est CONSERVÉE — seule voie de retour.
    bak = Path(err.fichiers_non_restaures[0]["sauvegarde"])
    assert bak.exists()
    assert _sha(bak) != _sha(impacts)          # elle porte bien l'état d'AVANT (impacts remplacé)


# ── 14-16 : nettoyage ────────────────────────────────────────────────────────

def test_14_16_nettoyage_des_temporaires_et_sauvegardes_apres_succes(cibles, flags_on):
    saisie, _ = cibles
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.ok, res
    restants = _fichiers_du_dossier(saisie.parent)
    assert restants == ["SAISIE_Charges_Flux.xlsx", "SAISIE_Charges_Impacts.xlsx"]
    assert not any(".bak" in n for n in restants)        # test 16 : plus aucune sauvegarde
    assert res.sauvegardes_restantes == []
    assert res.nettoyage_incomplet == []


def test_15_nettoyage_des_temporaires_apres_echec_avant_commit(cibles, flags_on, monkeypatch):
    saisie, _ = cibles
    vrai = writer.prepare_charge_flux_write

    def prepare_puis_corrompt(*a, **kw):
        res = vrai(*a, **kw)
        Path(res["temp_path"]).write_bytes(b"corrompu")
        return res

    monkeypatch.setattr(writer, "prepare_charge_flux_write", prepare_puis_corrompt)
    res = svc.confirmer_ecriture_charge(_demande(cibles))

    assert res.code == svc.E_TEMPORAIRE_INVALIDE
    # Les deux temporaires (impacts + charge) ont été nettoyés, aucune sauvegarde n'a été créée.
    assert _fichiers_du_dossier(saisie.parent) == ["SAISIE_Charges_Flux.xlsx",
                                                   "SAISIE_Charges_Impacts.xlsx"]
    assert res.nettoyage_incomplet == []


# ── 19-20 : fichiers réels et flags du dépôt ─────────────────────────────────

def test_19_les_vrais_fichiers_du_projet_ne_sont_jamais_touches(cibles, flags_on):
    presents = [p for p in REELS if p.exists()]
    avant = {p: _sha(p) for p in presents}

    assert svc.confirmer_ecriture_charge(_demande(cibles)).ok

    assert {p: _sha(p) for p in presents} == avant


def test_20_les_flags_du_depot_restent_a_false():
    """Les flags ne sont forcés qu'en mémoire (monkeypatch). Le dépôt, lui, reste à False."""
    source = Path(cfg.__file__).read_text(encoding="utf-8")
    assert "CHARGES_REAL_WRITE_ENABLED = False" in source
    assert "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = False" in source
    assert cfg.CHARGES_REAL_WRITE_ENABLED is False        # hors monkeypatch
    assert cfg.CHARGES_REAL_WRITE_CONFIRMATION_ENABLED is False


def test_20b_os_replace_uniquement_dans_l_orchestrateur():
    """Le writer bas niveau ne remplace jamais : l'unique os.replace du flux est ici."""
    assert "os.replace(" not in Path(writer.__file__).read_text(encoding="utf-8")
    assert "os.replace(" in Path(svc.__file__).read_text(encoding="utf-8")

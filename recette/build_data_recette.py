#!/usr/bin/env python3
"""Génère data_recette/ : jeu de données FICTIF isolé pour la recette Charges.

- REF_Setup.xlsm fictif : feuilles config recopiées en VALEURS ; feuilles parc/personnes
  REMPLACÉES par des données fictives (PROP_A/B/C, LOG_A1..C1, LOG_INACTIF). Aucune PII réelle.
- SAISIE_Charges_Flux/Impacts vidés (en-têtes + REF_LOCALE conservés) : aucune charge de départ.
- MASTER_FACT_MAN_Charges.xlsx vide (sera régénéré par Lot3 à la confirmation).
- PBI_Referentiel_Logements.csv fictif (liste logements).

Idempotent : réécrit intégralement data_recette/ à chaque exécution.
Aucune écriture hors de data_recette/.
"""
from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path

import openpyxl

WT = Path(__file__).resolve().parent.parent          # racine du worktree
SRC_REF = WT / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm"
SRC_SAISIE_FLUX = WT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Flux.xlsx"
SRC_SAISIE_IMP = WT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx"

REC = WT / "data_recette"

# Feuilles à REMPLACER par du fictif (parc + personnes). Toutes les autres = copiées en valeurs.
SCRUB = {
    "REF_Proprietaires", "REF_Logements", "REF_Taux_Commission",
    "REF_Gestion_Logements_Hist", "REF_Mapping_Logements", "REF_Intervenants",
    "REF_Associes", "REF_Cartes_Paiement",
}

PERIODE = "2026-06"

# Feuilles copiées mais vidées (PII non nécessaire à la recette Charges — ex. règles banque).
EMPTY_HEADER_ONLY = {"REF_Banque_Regles"}

# Remplacements d'identifiants de personnes réelles → fictifs, appliqués partout.
PERSON_REMAP = {"PERS_EWAN": "PERS_X", "PERS_WAFA": "PERS_Y"}

# Jetons de PII réelle à neutraliser dans toute cellule texte des feuilles conservées.
PII_TOKENS = ["UZON", "Delrieu", "Gauthrot", "Rodrigues", "Vassal", "PONS", "Berrada", "Toure",
              "Maurer", "Dureuil", "Treiber", "Ewan", "Wafa", "Souci", "Imene", "Imène",
              "Cedrine", "Cédrine", "Gabriel", "Phillippe", "Florane", "Caroline", "Fatine",
              "Didier", "Maryline", "Clarisse", "Francois", "François", "David", "Noel", "Noël",
              "Dinneweth", "outlook.fr", "gmail.com", "hotmail", "Charles de Fitte", "Cornebarrieu"]


def _sanitize(value):
    """Neutralise toute PII réelle résiduelle d'une cellule copiée (remap ids, sinon FICTIF)."""
    if not isinstance(value, str):
        return value
    s = value
    for old, new in PERSON_REMAP.items():
        s = s.replace(old, new)
    for tok in PII_TOKENS:
        if tok.lower() in s.lower():
            return "FICTIF"
    return s

# ── Données fictives (par en-tête, colonnes inconnues laissées vides) ─────────
FICTIF: dict[str, list[dict]] = {
    "REF_Proprietaires": [
        {"proprietaire_id": "PROP_A", "nom_proprietaire": "Alpha", "prenom_proprietaire": "Proprio",
         "email": "prop.a@example.test", "telephone": "33600000001", "adresse_facturation": "1 rue Fictive",
         "mode_facturation": "PAR_LOGEMENT", "actif": "OUI", "commentaire": "FICTIF recette"},
        {"proprietaire_id": "PROP_B", "nom_proprietaire": "Beta", "prenom_proprietaire": "Proprio",
         "email": "prop.b@example.test", "telephone": "33600000002", "adresse_facturation": "2 rue Fictive",
         "mode_facturation": "PAR_LOGEMENT", "actif": "OUI", "commentaire": "FICTIF recette"},
        {"proprietaire_id": "PROP_C", "nom_proprietaire": "Gamma", "prenom_proprietaire": "Proprio",
         "email": "prop.c@example.test", "telephone": "33600000003", "adresse_facturation": "3 rue Fictive",
         "mode_facturation": "PAR_LOGEMENT", "actif": "OUI", "commentaire": "FICTIF recette"},
    ],
    "REF_Logements": [
        {"logement_id": "LOG_A1", "hostaway_listing_id": 900001, "nom_logement_officiel": "Fictif A1",
         "nom_court": "A1", "adresse": "1 rue Fictive", "ville": "RECETTE", "type_logement_id": "TYPE_001",
         "sur_hostaway": "OUI", "dynamic_pricing": "hostdynamic", "actif": "OUI", "statut_parc": "GERE",
         "commentaire": "FICTIF", "forfait_logiciel_consommables_mensuel": 0},
        {"logement_id": "LOG_A2", "hostaway_listing_id": 900002, "nom_logement_officiel": "Fictif A2",
         "nom_court": "A2", "adresse": "1 bis rue Fictive", "ville": "RECETTE", "type_logement_id": "TYPE_002",
         "sur_hostaway": "OUI", "dynamic_pricing": "hostdynamic", "actif": "OUI", "statut_parc": "GERE",
         "commentaire": "FICTIF", "forfait_logiciel_consommables_mensuel": 0},
        {"logement_id": "LOG_B1", "hostaway_listing_id": 900003, "nom_logement_officiel": "Fictif B1",
         "nom_court": "B1", "adresse": "2 rue Fictive", "ville": "RECETTE", "type_logement_id": "TYPE_001",
         "sur_hostaway": "OUI", "dynamic_pricing": "hostdynamic", "actif": "OUI", "statut_parc": "GERE",
         "commentaire": "FICTIF", "forfait_logiciel_consommables_mensuel": 0},
        {"logement_id": "LOG_C1", "hostaway_listing_id": 900004, "nom_logement_officiel": "Fictif C1",
         "nom_court": "C1", "adresse": "3 rue Fictive", "ville": "RECETTE", "type_logement_id": "TYPE_003",
         "sur_hostaway": "OUI", "dynamic_pricing": "hostdynamic", "actif": "OUI", "statut_parc": "GERE",
         "commentaire": "FICTIF", "forfait_logiciel_consommables_mensuel": 0},
        {"logement_id": "LOG_INACTIF", "hostaway_listing_id": 900005, "nom_logement_officiel": "Fictif Inactif",
         "nom_court": "INACTIF", "adresse": "9 rue Fictive", "ville": "RECETTE", "type_logement_id": "TYPE_001",
         "sur_hostaway": "NON", "dynamic_pricing": "NON", "actif": "NON", "statut_parc": "RETIRE",
         "commentaire": "FICTIF inactif", "forfait_logiciel_consommables_mensuel": 0},
    ],
    "REF_Taux_Commission": [
        # LOG_A1 : changement de taux (19% avant mars, 15% à partir de mars) → 2026-06 = 15%
        {"taux_commission_id": "TX_A1_OLD", "proprietaire_id": "PROP_A", "logement_id": "LOG_A1",
         "taux_commission": 0.19, "date_debut": "2026-01-01", "date_fin": "2026-02-28", "actif": "OUI",
         "justification": "FICTIF", "commentaire": "taux historique"},
        {"taux_commission_id": "TX_A1_NEW", "proprietaire_id": "PROP_A", "logement_id": "LOG_A1",
         "taux_commission": 0.15, "date_debut": "2026-03-01", "date_fin": None, "actif": "OUI",
         "justification": "FICTIF", "commentaire": "taux courant"},
        {"taux_commission_id": "TX_A2", "proprietaire_id": "PROP_A", "logement_id": "LOG_A2",
         "taux_commission": 0.19, "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
         "justification": "FICTIF", "commentaire": ""},
        {"taux_commission_id": "TX_B1", "proprietaire_id": "PROP_B", "logement_id": "LOG_B1",
         "taux_commission": 0.15, "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
         "justification": "FICTIF", "commentaire": ""},
        {"taux_commission_id": "TX_C1", "proprietaire_id": "PROP_C", "logement_id": "LOG_C1",
         "taux_commission": 0.19, "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
         "justification": "FICTIF", "commentaire": ""},
    ],
    "REF_Gestion_Logements_Hist": [
        {"gestion_id": f"GST_{lg}", "logement_id": lg, "proprietaire_id": pr, "date_debut": "2026-01-01",
         "date_fin": (None if act else "2026-05-31"), "statut_gestion": ("ACTIF" if act else "RETIRE"),
         "source": "FICTIF", "commentaire": "recette"}
        for lg, pr, act in [("LOG_A1", "PROP_A", True), ("LOG_A2", "PROP_A", True),
                             ("LOG_B1", "PROP_B", True), ("LOG_C1", "PROP_C", True),
                             ("LOG_INACTIF", "PROP_C", False)]
    ],
    "REF_Mapping_Logements": [
        {"mapping_logement_id": f"MAP_{lg}", "source": "Hostaway", "champ_source": "listingMapId",
         "valeur_source": vid, "logement_id": lg, "niveau_confiance": "Fort", "actif": "OUI",
         "commentaire": "FICTIF"}
        for lg, vid in [("LOG_A1", 900001), ("LOG_A2", 900002), ("LOG_B1", 900003),
                        ("LOG_C1", 900004), ("LOG_INACTIF", 900005)]
    ],
    "REF_Intervenants": [
        {"intervenant_id": "INT_A", "nom_intervenant": "Menage Interne A", "type_intervenant": "INTERNE",
         "societe": "Interne", "email": "int.a@example.test", "telephone": "33600000010", "actif": "OUI",
         "nom_normalise": "MENAGE INTERNE A", "date_debut_validite": "2026-01-01"},
        {"intervenant_id": "INT_B", "nom_intervenant": "Menage Externe B", "type_intervenant": "EXTERNE",
         "societe": "Externe SARL", "email": "int.b@example.test", "telephone": "33600000011", "actif": "OUI",
         "nom_normalise": "MENAGE EXTERNE B", "date_debut_validite": "2026-01-01"},
    ],
    "REF_Associes": [
        {"personne_id": "PERS_X", "nom_personne": "Associe X", "type_personne": "ASSOCIE", "actif": "OUI",
         "commentaire": "FICTIF"},
        {"personne_id": "PERS_Y", "nom_personne": "Associe Y", "type_personne": "ASSOCIEE", "actif": "OUI",
         "commentaire": "FICTIF"},
    ],
    "REF_Cartes_Paiement": [
        {"carte_id": "CARTE_001", "suffixe_carte": "0000", "personne_id": "PERS_X", "nom_personne": "Associe X",
         "type_personne": "ASSOCIE", "mode_paiement_id": "PAY_003", "date_debut_validite": "2026-01-01",
         "date_fin_validite": None, "actif": "OUI", "commentaire": "FICTIF"},
    ],
}


def _write_sheet(ws_new, header, rows):
    ws_new.append(list(header))
    for r in rows:
        ws_new.append([r.get(h) for h in header])


def build_ref_setup(dst: Path):
    src = openpyxl.load_workbook(SRC_REF, read_only=True, data_only=True)
    out = openpyxl.Workbook()
    out.remove(out.active)
    for ws in src.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            out.create_sheet(ws.title)
            continue
        header = list(rows[0])
        new = out.create_sheet(ws.title)
        if ws.title in SCRUB:
            _write_sheet(new, header, FICTIF.get(ws.title, []))
        elif ws.title == "REF_Cloture_Mensuelle":
            new.append(header)
            seen = set()
            for r in rows[1:]:
                d = dict(zip(header, r))
                # forcer la période de recette OUVERTE
                if str(d.get("mois")) == PERIODE:
                    d["statut_mois"] = "OUVERT"
                seen.add(str(d.get("mois")))
                new.append([d.get(h) for h in header])
            if PERIODE not in seen:
                d = {h: None for h in header}
                d["mois"] = PERIODE; d["statut_mois"] = "OUVERT"; d["commentaire"] = "FICTIF recette"
                new.append([d.get(h) for h in header])
        elif ws.title in EMPTY_HEADER_ONLY:
            new.append(header)                      # PII réelle retirée : en-tête seul
        else:
            for r in rows:
                new.append([_sanitize(v) for v in r])
    src.close()
    dst.parent.mkdir(parents=True, exist_ok=True)
    out.save(dst)


def build_saisie_empty(src_path: Path, dst_path: Path):
    """Recopie la structure (en-têtes + feuilles annexes) mais VIDE l'onglet SAISIE de ses lignes."""
    src = openpyxl.load_workbook(src_path, read_only=True, data_only=True)
    out = openpyxl.Workbook()
    out.remove(out.active)
    for ws in src.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        new = out.create_sheet(ws.title)
        if not rows:
            continue
        if ws.title in ("SAISIE", "AFFECTATIONS", "MENAGE", "RESERVE_REFACTURATION"):
            new.append(list(rows[0]))            # header seul, aucune ligne de données
        else:
            for r in rows:
                new.append(list(r))
    src.close()
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    out.save(dst_path)


SRC_LOT7 = WT / "02_TRAVAIL" / "Lot7_IK_Avantages" / "MASTER_FACT_MAN_IK_Avantages.xlsx"


def build_lot7(dst: Path):
    """Lot7 IK/Avantages fictif : structure conservée, feuilles de données VIDÉES (aucune PII).
    Requis par le contrôle Lot11 (lecture MASTER_CALC_AVANTAGES)."""
    src = openpyxl.load_workbook(SRC_LOT7, read_only=True, data_only=True)
    out = openpyxl.Workbook()
    out.remove(out.active)
    DATA_SHEETS = {"SOURCE_SAISIE", "MASTER_SAISIE", "MASTER_CALC_AVANTAGES", "PARAMETRES"}
    for ws in src.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        new = out.create_sheet(ws.title)
        if not rows:
            continue
        if ws.title in DATA_SHEETS:
            new.append([_sanitize(v) for v in rows[0]])   # en-tête seul, données retirées
        else:
            for r in rows:
                new.append([_sanitize(v) for v in r])
    src.close()
    dst.parent.mkdir(parents=True, exist_ok=True)
    out.save(dst)


def build_master_charges_empty(dst: Path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    # En-têtes minimales lues par charges_reader (charge_id + colonnes usuelles). Le Lot3 réécrira.
    ws.append(["charge_id", "mois", "date_charge", "montant", "categorie_charge_id",
               "type_flux_id", "code_impact", "logement_id", "proprietaire_id", "statut_controle"])
    dst.parent.mkdir(parents=True, exist_ok=True)
    wb.save(dst)


def build_pbi_logements(dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["logement_id", "nom_court", "nom_logement_officiel", "adresse", "ville",
                    "proprietaire_id", "nom_proprietaire", "type_logement_id", "actif", "statut_parc",
                    "taux_commission"])
        parc = [("LOG_A1", "PROP_A", "Alpha", "TYPE_001", "OUI", "GERE", 0.15),
                ("LOG_A2", "PROP_A", "Alpha", "TYPE_002", "OUI", "GERE", 0.19),
                ("LOG_B1", "PROP_B", "Beta", "TYPE_001", "OUI", "GERE", 0.15),
                ("LOG_C1", "PROP_C", "Gamma", "TYPE_003", "OUI", "GERE", 0.19),
                ("LOG_INACTIF", "PROP_C", "Gamma", "TYPE_001", "NON", "RETIRE", 0.0)]
        for lg, pr, nom, typ, act, statut, taux in parc:
            w.writerow([lg, lg.split("_")[1], f"Fictif {lg}", f"rue Fictive {lg}", "RECETTE",
                        pr, nom, typ, act, statut, taux])


def _robust_rmtree(path: Path, tries: int = 5):
    import stat
    import time

    def _onexc(func, p, exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass

    for i in range(tries):
        if not path.exists():
            return
        try:
            shutil.rmtree(path, onexc=_onexc)
        except TypeError:
            shutil.rmtree(path, onerror=lambda f, p, e: _onexc(f, p, e))
        except Exception:
            pass
        if not path.exists():
            return
        time.sleep(0.5)
    # dernier recours : vider fichier par fichier
    for p in sorted(path.rglob("*"), reverse=True):
        try:
            p.unlink() if p.is_file() else p.rmdir()
        except Exception:
            pass


def main():
    _robust_rmtree(REC)
    REC.mkdir(parents=True, exist_ok=True)
    build_ref_setup(REC / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm")
    # SAISIE = templates VIDES (formules + lignes modèle, aucune charge, aucune PII) → copie verbatim
    # pour préserver les formules et la ligne modèle exigée par le writer (E_FORMULE_MODELE_ABSENTE).
    dst_flux = REC / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Flux.xlsx"
    dst_imp = REC / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx"
    dst_flux.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SRC_SAISIE_FLUX, dst_flux)
    shutil.copy2(SRC_SAISIE_IMP, dst_imp)
    build_master_charges_empty(REC / "02_TRAVAIL" / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx")
    build_lot7(REC / "02_TRAVAIL" / "Lot7_IK_Avantages" / "MASTER_FACT_MAN_IK_Avantages.xlsx")
    # Copier les scripts moteur Python (CODE, aucune PII) — requis par le runner aval qui ajoute
    # <project_root>/02_TRAVAIL au sys.path pour importer lot3_generateur_charges & libs.
    dst_travail = REC / "02_TRAVAIL"
    dst_travail.mkdir(parents=True, exist_ok=True)
    for py in (WT / "02_TRAVAIL").glob("*.py"):
        shutil.copy2(py, dst_travail / py.name)
    build_pbi_logements(REC / "03_EXPORTS" / "PowerBI" / "PBI_Referentiel_Logements.csv")
    # dossiers data applicatifs isolés
    (REC / "data" / "snapshots").mkdir(parents=True, exist_ok=True)
    print("data_recette généré :", REC.resolve())
    for p in sorted(REC.rglob("*")):
        if p.is_file():
            print("  ", p.relative_to(REC))


if __name__ == "__main__":
    main()

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
import sys
from pathlib import Path

import openpyxl

# Import robuste quel que soit le répertoire courant (le dossier recette/ n'est pas un package).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_reservations_recette  # noqa: E402

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
# Mois forcés OUVERT dans le REF de recette. Le mois de recette lui-même, plus les mois qui
# portent le remplissage de volume de `build_reservations_recette` : sur un mois CLOTURE sans
# historique, lot4quater bascule en repli CLOTURE_SANS_HIST et n'alimente pas VUE_FLUX, ce qui
# ferait échouer le contrôle de volume CTR-9-003 de lot9 (VUE_FLUX >= 1000).
MOIS_OUVERTS = {PERIODE} | {f"2026-{m:02d}" for m in range(1, 6)}

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
                # forcer les mois de recette OUVERTS (cf. MOIS_OUVERTS)
                if str(d.get("mois")) in MOIS_OUVERTS:
                    d["statut_mois"] = "OUVERT"
                seen.add(str(d.get("mois")))
                new.append([d.get(h) for h in header])
            for mois in sorted(MOIS_OUVERTS - seen):
                d = {h: None for h in header}
                d["mois"] = mois; d["statut_mois"] = "OUVERT"; d["commentaire"] = "FICTIF recette"
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


# Sources que lot11 charge inconditionnellement : leur ABSENCE fait planter le lot, alors qu'une
# source vide est un cas métier normal. On en recopie l'EN-TÊTE SEUL depuis le fichier réel —
# lecture seule, aucune ligne de données, donc aucune PII.
ENTETES_SEULES = (
    "02_TRAVAIL/Lot1_Hostaway/MASTER_CTRL_HA_Anomalies.xlsx",
    "02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx",
    "02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx",
    "02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx",
    "01_SOURCES_BRUTES/AirCover/SAISIE_AirCover.xlsx",
    "01_SOURCES_BRUTES/ImputationsAirbnb/SAISIE_ImputationsAirbnb.xlsx",
    "01_SOURCES_BRUTES/AjustementsPostCloture/SAISIE_Ajustements_PostCloture.xlsx",
)


def build_entetes_seules():
    """Recopie l'en-tête (1re ligne) de chaque onglet des sources ci-dessus, sans aucune donnée."""
    for rel in ENTETES_SEULES:
        src_path = WT / rel
        if not src_path.exists():
            print(f"   [entête seule] source absente, ignorée : {rel}")
            continue
        src = openpyxl.load_workbook(src_path, read_only=True, data_only=True)
        out = openpyxl.Workbook()
        out.remove(out.active)
        for ws in src.worksheets:
            new = out.create_sheet(ws.title)
            premiere = next(ws.iter_rows(values_only=True), None)
            if premiere is not None:
                new.append([_sanitize(v) for v in premiere])
        src.close()
        dst = REC / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        out.save(dst)
    print(f"   {len(ENTETES_SEULES)} sources recopiées en en-tête seul (aucune donnée)")


def build_master_charges_empty(dst: Path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    # En-têtes minimales lues par charges_reader (charge_id + colonnes usuelles). Le Lot3 réécrira.
    hdr = ["charge_id", "mois", "date_charge", "montant", "categorie_charge_id",
           "type_flux_id", "code_impact", "logement_id", "proprietaire_id", "statut_controle",
           "facture_ref", "fournisseur"]
    ws.append(hdr)
    # AUCUNE charge n'est seedée ici : le MASTER est une SORTIE de Lot3. Le seeder créerait une
    # seconde vérité, que le premier passage de Lot3 écraserait. Les charges vivent dans la SAISIE
    # (cf. SCENARIOS_CHARGES), et `build_charges()` fait produire ce classeur par Lot3.
    # Le classeur doit néanmoins pré-exister : `ecrire_master` ouvre la cible pour en préserver
    # les autres onglets, il ne la crée pas.
    dst.parent.mkdir(parents=True, exist_ok=True)
    wb.save(dst)


# Scénarios Charges de la recette — écrits dans la SAISIE, qui est la VÉRITÉ MÉTIER.
# Le MASTER n'est pas seedé : il est produit par Lot3 à partir de ces lignes (cf. build_charges()).
# Colonnes renseignées : celles que Lot3 lit. Les colonnes formule de la SAISIE (mois, impacts,
# ROW_HASH) sont laissées telles quelles — Lot3 les recalcule en Python, jamais depuis le cache.
#
# (charge_id, date, montant, sens_flux, categorie, type_flux, code_impact, prise_en_compta,
#  associe_id, affectation_type, logement_id, proprietaire_id, refacturable, source_flux,
#  methode_traitement, statut_controle, justificatif, commentaire)
SCENARIOS_CHARGES = [
    # A — charge conciergerie pure : impact réel ET comptable, non refacturable.
    #     Diminue le résultat conciergerie, laisse le net propriétaire intact.
    ("CHG_A_LOGICIEL", "2026-06-03", 60.00, "DEPENSE", "CHG_005", "TYPE_FLUX_002", "IC", "OUI",
     None, "GLOBAL", None, None, "NON", "SAISIE_MANUELLE", "DIRECT", "VALIDE", "OUI",
     "A - abonnement Hostaway, charge conciergerie"),
    # B — charge refacturable propriétaire : rattachée à un logement, refacturable=OUI.
    ("CHG_B_REFACT", "2026-06-08", 150.00, "DEPENSE", "CHG_008", "TYPE_FLUX_009", "IC", "OUI",
     None, "LOGEMENT", "LOG_A1", "PROP_A", "OUI", "FACTURE_PDF", "REFACTURATION_PROPRIETAIRE",
     "VALIDE", "OUI", "B - reparation refacturable au proprietaire"),
    # C — paiement personnel associé : porté par une associée, hors comptabilité.
    ("CHG_C_PERSO", "2026-06-11", 40.00, "DEPENSE", "CHG_004", "TYPE_FLUX_004", "HC", "NON",
     "PERS_EWAN", "GLOBAL", None, None, "NON", "SAISIE_MANUELLE", "DIRECT", "VALIDE", "OUI",
     "C - paiement personnel associe, hors compta"),
    # D — charge hors comptabilité : impact réel, aucun impact comptable, justification conservée.
    ("CHG_D_HORSCOMPTA", "2026-06-13", 25.00, "DEPENSE", "CHG_009", "TYPE_FLUX_010", "HC", "NON",
     None, "GLOBAL", None, None, "NON", "SAISIE_MANUELLE", "DIRECT", "VALIDE", "NON",
     "D - frais de deplacement hors comptabilite, justification au dossier"),
    # E — répartition multi-logements : DEUX charges distinctes, une par logement. Le modèle
    #     interdit de dupliquer une charge par affectation ; une ventilation = plusieurs charges.
    ("CHG_E_MULTI_1", "2026-06-17", 30.00, "DEPENSE", "CHG_004", "TYPE_FLUX_004", "IC", "OUI",
     None, "LOGEMENT", "LOG_A2", "PROP_A", "NON", "SAISIE_MANUELLE", "DIRECT", "VALIDE", "OUI",
     "E - consommables ventiles 1/2 (total 60)"),
    ("CHG_E_MULTI_2", "2026-06-17", 30.00, "DEPENSE", "CHG_004", "TYPE_FLUX_004", "IC", "OUI",
     None, "LOGEMENT", "LOG_B1", "PROP_B", "NON", "SAISIE_MANUELLE", "DIRECT", "VALIDE", "OUI",
     "E - consommables ventiles 2/2 (total 60)"),
    # F — charge fournisseur adossée à une facture : la charge reste UNIQUE. Facture, règlement et
    #     rapprochement bancaire vivent dans SQLite et ne recréent jamais d'impact économique.
    ("CHG_SEED_001", "2026-06-12", 120.00, "DEPENSE", "CHG_003", "TYPE_FLUX_014", "IC", "OUI",
     None, "LOGEMENT", "LOG_B1", "PROP_B", "NON", "FACTURE_PDF", "DIRECT", "VALIDE", "OUI",
     "F - blanchisserie, facture FA-2026-0012"),
    ("CHG_SEED_002", "2026-06-25", 95.00, "DEPENSE", "CHG_003", "TYPE_FLUX_014", "IC", "OUI",
     None, "LOGEMENT", "LOG_C1", "PROP_C", "NON", "FACTURE_PDF", "DIRECT", "VALIDE", "OUI",
     "F - blanchisserie, facture FA-2026-0025"),
    # PAS de charge manuelle pour le frais bancaire de 8,90 € : Lot9 injecte déjà les mouvements
    # TYPE_FLUX_016 VALIDE depuis NORM_Banque. La saisir en plus la compterait DEUX FOIS.
    # Ce double comptage existait dans le jeu de recette précédent et n'était détecté par rien —
    # cf. `test_charges_pipeline.py::test_pas_de_double_comptage_frais_bancaire`.
    # Cas de contrôle : charge NON validée — ne doit jamais entrer dans les résultats.
    ("CHG_CTRL_NONVALID", "2026-06-19", 500.00, "DEPENSE", "CHG_008", "TYPE_FLUX_009", "IC", "OUI",
     None, "LOGEMENT", "LOG_A1", "PROP_A", "NON", "SAISIE_MANUELLE", "DIRECT", "A_CONTROLER",
     "NON", "CONTROLE - non validee, ne doit pas etre injectee"),
    # Cas de contrôle : charge exclue du résultat — idem.
    ("CHG_CTRL_EXCLUE", "2026-06-20", 400.00, "DEPENSE", "CHG_008", "TYPE_FLUX_009", "IC", "OUI",
     None, "LOGEMENT", "LOG_A1", "PROP_A", "NON", "SAISIE_MANUELLE", "DIRECT", "EXCLU_RESULTAT",
     "NON", "CONTROLE - exclue du resultat, ne doit pas etre injectee"),
]

# Colonnes de la SAISIE renseignées par le seeding, dans l'ordre des tuples ci-dessus.
_COLS_SCENARIO = [
    "charge_id", "date_charge", "montant", "sens_flux", "categorie_charge_id", "type_flux_id",
    "code_impact", "prise_en_compta", "associe_id", "affectation_type", "logement_id",
    "proprietaire_id", "refacturable", "source_flux", "methode_traitement", "statut_controle",
    "justificatif", "commentaire",
]


def build_saisie_charges(saisie_path: Path):
    """Écrit les scénarios dans l'onglet SAISIE, sans toucher aux colonnes formule ni au gabarit."""
    wb = openpyxl.load_workbook(saisie_path)
    ws = wb["SAISIE"]
    entetes = [c.value for c in ws[1]]
    index = {nom: i + 1 for i, nom in enumerate(entetes) if nom}
    manquantes = [c for c in _COLS_SCENARIO if c not in index]
    if manquantes:
        raise RuntimeError(f"Colonnes absentes de la SAISIE : {manquantes}")

    for decalage, valeurs in enumerate(SCENARIOS_CHARGES):
        ligne = 2 + decalage                      # les lignes 2..N portent déjà les formules
        for nom, valeur in zip(_COLS_SCENARIO, valeurs):
            ws.cell(row=ligne, column=index[nom]).value = valeur
    wb.save(saisie_path)
    wb.close()
    print(f"   {len(SCENARIOS_CHARGES)} charges de scenario ecrites dans la SAISIE")


def build_charges(saisie_path: Path, ref_path: Path, master_path: Path):
    """Fait produire le MASTER par LOT3 LUI-MÊME, à partir de la SAISIE.

    Le MASTER n'est pas seedé à la main : ce serait une seconde vérité, et le premier passage de
    Lot3 l'écraserait. Lot3 n'utilise qu'openpyxl — il tourne donc avec l'interpréteur courant.
    """
    import sys as _sys
    _sys.path.insert(0, str(WT / "02_TRAVAIL"))
    import lot3_generateur_charges as lot3

    res = lot3.generer(str(saisie_path), str(ref_path), str(master_path),
                       date_integration="2026-06-30T00:00:00")
    bloquants = [a for a in res["anomalies"] if a.get("niveau") == "BLOQUANT"]
    print(f"   Lot3 : {res['nb_lignes']} lignes MASTER, {res['nb_vue_menage']} en VUE_MENAGE, "
          f"{len(res['anomalies'])} anomalie(s) dont {len(bloquants)} bloquante(s)")
    if bloquants:
        raise RuntimeError(f"Lot3 a produit des anomalies bloquantes : {bloquants}")


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


NORM_BANQUE_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source",
    "date_operation", "date_valeur", "libelle", "libelle_brut",
    "montant", "sens", "devise", "compte_id",
    "tiers_detecte", "categorie", "type_flux_id", "code_impact",
    "source_classification", "source_economique",
    "statut_controle", "niveau_risque",
    "codes_anomalie", "date_integration", "commentaire",
    "statut_classification", "niveau_anomalie", "regle_id_appliquee",
]


def build_banque(dst: Path):
    """Jeu fictif NORM_Banque (schéma lot8a/8b exact — 26 colonnes) : un cas par catégorie du
    petit jeu demandé pour la recette Banque (encaissement, payout, paiement propriétaire,
    paiement fournisseur, remboursement associé, frais bancaire, doublon certain, inconnu)."""
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_BANQUE_HDR)
    compte = "CM_02211_00021321603"
    lignes = [
        # (id, date_op, libelle, montant, sens, tiers, categorie, type_flux, statut, commentaire)
        ("MVT-SEED-001", "2026-06-05", "VIR HOSTAWAY PAYOUT", 850.00, "CREDIT", "HOSTAWAY",
         "PAYOUT_PLATEFORME", "TYPE_FLUX_017", "VALIDE", "Encaissement reservation Hostaway"),
        ("MVT-SEED-002", "2026-06-06", "VIR HOSTAWAY PAYOUT LOT", 1200.00, "CREDIT", "HOSTAWAY",
         "PAYOUT_PLATEFORME", "TYPE_FLUX_017", "VALIDE", "Payout plateforme groupe"),
        ("MVT-SEED-003", "2026-06-10", "VIR PROPRIETAIRE PROP A", 400.00, "DEBIT", "PROP_A",
         "VIREMENT_PROPRIETAIRE_A_RAPPROCHER", "TYPE_FLUX_007", "VALIDE", "Reversement proprietaire"),
        ("MVT-SEED-004", "2026-06-12", "VIR FOURNISSEUR MENAGE B", 120.00, "DEBIT", "FOURNISSEUR_B",
         "FACTURE_PRESTATAIRE", "TYPE_FLUX_014", "VALIDE", "Paiement fournisseur menage"),
        ("MVT-SEED-005", "2026-06-14", "VIR ASSOCIE REMBOURSEMENT", 75.00, "DEBIT", "PERS_X",
         "VIR_ASSOCIE", "TYPE_FLUX_005", "VALIDE", "Remboursement associe"),
        ("MVT-SEED-006", "2026-06-15", "FRAIS TENUE DE COMPTE", 8.90, "DEBIT", "",
         "FRAIS_BANCAIRES", "TYPE_FLUX_016", "VALIDE", "Frais bancaires"),
        ("MVT-SEED-008", "2026-06-20", "PRLV INCONNU DIVERS", 45.00, "DEBIT", "",
         "DEPENSE_CB_A_CLASSIFIER", "", "EN_ATTENTE_CLASSIFICATION", "Mouvement non identifie"),
    ]
    for mid, date_op, libelle, montant, sens, tiers, categorie, type_flux, statut, commentaire in lignes:
        row_hash = f"{mid}-HASH"
        # code_impact : seuls les flux repris par lot9 doivent en porter un. lot9 exige un
        # code_impact valide (CTR-9-006) sur les TYPE_FLUX_016 (frais bancaires) qu'il injecte ;
        # un code_impact vide y produirait un BLOQUANT. Les autres mouvements ne sont pas repris.
        code_impact = "IC" if type_flux == "TYPE_FLUX_016" else ""
        ws.append([
            mid, row_hash, "IMPORT_SEED_2026_06", 1,
            date_op, date_op, libelle, libelle,
            montant, sens, "EUR", compte,
            tiers, categorie, type_flux, code_impact,
            "REGLE_DETERMINISTE", "",
            statut, "FAIBLE",
            "", "2026-06-30", commentaire,
            "CLASSIFIEE", "FAIBLE", "",
        ])
    # Doublon certain : même empreinte que MVT-SEED-006 (même date/montant/sens/libellé/compte) —
    # démontre la détection à la réimportation du même relevé.
    ws.append([
        "MVT-SEED-007", f"MVT-SEED-006-HASH", "IMPORT_SEED_2026_06", 2,
        "2026-06-15", "2026-06-15", "FRAIS TENUE DE COMPTE", "FRAIS TENUE DE COMPTE",
        8.90, "DEBIT", "EUR", compte,
        "", "FRAIS_BANCAIRES", "TYPE_FLUX_016", "",
        "REGLE_DETERMINISTE", "",
        "A_CONTROLER", "MOYEN",
        "DOUBLON_BANCAIRE_POTENTIEL", "2026-06-30", "Doublon certain (meme empreinte que SEED-006)",
        "CLASSIFIEE", "MOYEN", "",
    ])
    dst.parent.mkdir(parents=True, exist_ok=True)
    wb.save(dst)


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


def build_factures_sqlite(db_path: Path):
    """Base applicative de recette : fournisseurs, factures et règlements 100 % fictifs.

    Écrit dans une base SQLite ISOLÉE (sous data_recette), jamais la base applicative réelle.
    Idempotent : la base est recréée à chaque exécution puisque data_recette est effacé d'abord.
    Le module Factures est piloté par ses propres services pour que les invariants (soldes dérivés,
    statuts, historique) soient produits exactement comme en production — jamais des INSERT bruts
    qui pourraient fabriquer un état impossible.
    """
    import os
    import sys

    app_root = WT / "05_APPLICATION"
    if str(app_root) not in sys.path:
        sys.path.insert(0, str(app_root))
    # Les services exigent le double verrou : mode recette + flags dédiés.
    os.environ["RECETTE_MODE"] = "1"
    os.environ["FACTURES_REAL_WRITE_ENABLED"] = "1"
    os.environ["FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"] = "1"

    import app.config as cfg
    cfg.RECETTE_MODE = True
    cfg.FACTURES_REAL_WRITE_ENABLED = True
    cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED = True
    cfg.DB_PATH = db_path

    from app.db.connection import apply_migrations
    from app.services import factures_service as fact
    from app.services import fournisseurs_referentiel_service as frs
    from app.services import reglements_fournisseurs_service as regl

    db_path.parent.mkdir(parents=True, exist_ok=True)
    apply_migrations(db_path)

    fournisseurs = {}
    for nom, type_f in (("Menage Externe Fictif", "MENAGE"),
                        ("Logiciel Gestion Fictif", "FOURNITURE"),
                        ("Maintenance Fictive", "MAINTENANCE")):
        r = frs.creer(nom, type_f, acteur="seed", commentaire="FICTIF recette", db_path=db_path)
        fournisseurs[type_f] = r["fournisseur_id_opaque"]

    men, log, maint = fournisseurs["MENAGE"], fournisseurs["FOURNITURE"], fournisseurs["MAINTENANCE"]

    def facture(frs_id, ref, ttc, *, date_f="2026-06-05", ech="2026-07-05", statut=None,
                justificatif="justificatif_fictif.pdf", ht=None, tva=None):
        r = fact.creer({"fournisseur_id_opaque": frs_id, "facture_ref": ref,
                        "date_facture": date_f, "date_echeance": ech, "montant_ttc": ttc,
                        "montant_ht": ht, "montant_tva": tva, "justificatif": justificatif},
                       acteur="seed", db_path=db_path)
        fid = r["facture_id_opaque"]
        if statut:
            fact.changer_statut(fid, statut, acteur="seed", db_path=db_path)
        return fid

    # 1. Facture simple, validée, réglée en totalité (banque).
    f_simple = facture(log, "FA-LOG-2026-06", 35.00, statut=fact.ST_VALIDEE)
    fact.lier_charge(f_simple, "CHG_SEED_003", acteur="seed", db_path=db_path)
    regl.enregistrer(log, [{"facture_id_opaque": f_simple, "montant": 35.00}],
                     date_reglement="2026-06-15", moyen="BANQUE", acteur="seed", db_path=db_path)

    # 2. Facture ménage externe refacturable propriétaire, partiellement réglée.
    f_menage = facture(men, "FA-MEN-2026-06", 120.00, ht=100.00, tva=20.00,
                       statut=fact.ST_VALIDEE)
    fact.lier_charge(f_menage, "CHG_SEED_001", acteur="seed", db_path=db_path)
    regl.enregistrer(men, [{"facture_id_opaque": f_menage, "montant": 50.00}],
                     date_reglement="2026-06-20", moyen="BANQUE", acteur="seed", db_path=db_path)

    # 3. Facture réglée en plusieurs fois (banque puis caisse).
    f_multi = facture(maint, "FA-MAINT-MULTI", 200.00, statut=fact.ST_VALIDEE)
    regl.enregistrer(maint, [{"facture_id_opaque": f_multi, "montant": 120.00}],
                     date_reglement="2026-06-18", moyen="BANQUE", acteur="seed", db_path=db_path)
    regl.enregistrer(maint, [{"facture_id_opaque": f_multi, "montant": 80.00}],
                     date_reglement="2026-06-25", moyen="CAISSE", acteur="seed", db_path=db_path)

    # 4. Paiement GROUPÉ : un règlement couvrant deux factures du même fournisseur.
    f_g1 = facture(maint, "FA-MAINT-G1", 60.00, statut=fact.ST_VALIDEE)
    f_g2 = facture(maint, "FA-MAINT-G2", 40.00, statut=fact.ST_VALIDEE)
    regl.enregistrer(maint, [{"facture_id_opaque": f_g1, "montant": 60.00},
                             {"facture_id_opaque": f_g2, "montant": 40.00}],
                     date_reglement="2026-06-28", moyen="BANQUE", acteur="seed", db_path=db_path)

    # 5. Facture EN RETARD (échéance dépassée, non réglée).
    facture(men, "FA-MEN-RETARD", 95.00, date_f="2026-01-10", ech="2026-02-10",
            statut=fact.ST_VALIDEE)

    # 6. Facture EN LITIGE.
    f_litige = facture(maint, "FA-MAINT-LITIGE", 500.00, statut=fact.ST_VALIDEE)
    fact.changer_statut(f_litige, fact.ST_LITIGE, commentaire="montant contesté",
                        acteur="seed", db_path=db_path)

    # 7. Doublon PROBABLE : même fournisseur, même montant, date proche, référence différente.
    facture(men, "FA-MEN-DOUBLON-A", 77.00, date_f="2026-06-02")
    facture(men, "FA-MEN-DOUBLON-B", 77.00, date_f="2026-06-04")
    # (Le doublon CERTAIN — même référence — est impossible : refusé par l'index unique du schéma.)

    # 8. Facture ANNULÉE (jamais supprimée).
    f_annulee = facture(log, "FA-LOG-ANNULEE", 12.00)
    fact.changer_statut(f_annulee, fact.ST_ANNULEE, commentaire="saisie erronée",
                        acteur="seed", db_path=db_path)

    # 9. AVOIR : traité comme moyen de règlement sur une facture ouverte.
    f_avoir = facture(log, "FA-LOG-AVOIR", 30.00, statut=fact.ST_VALIDEE)
    regl.enregistrer(log, [{"facture_id_opaque": f_avoir, "montant": 30.00}],
                     date_reglement="2026-06-30", moyen="AVOIR", acteur="seed", db_path=db_path)

    # 10. Règlement ANNULÉ (le solde de la facture doit repartir à son montant plein).
    f_regl_annule = facture(maint, "FA-MAINT-REGANN", 45.00, statut=fact.ST_VALIDEE)
    r_ann = regl.enregistrer(maint, [{"facture_id_opaque": f_regl_annule, "montant": 45.00}],
                             date_reglement="2026-06-29", moyen="BANQUE", acteur="seed",
                             db_path=db_path)
    regl.annuler(r_ann["reglement_id_opaque"], commentaire="erreur de saisie", acteur="seed",
                 db_path=db_path)

    # 11. Règlement par paiement PERSONNEL ASSOCIÉ.
    f_perso = facture(men, "FA-MEN-PERSO", 25.00, statut=fact.ST_VALIDEE)
    regl.enregistrer(men, [{"facture_id_opaque": f_perso, "montant": 25.00}],
                     date_reglement="2026-06-27", moyen="PERSONNEL_ASSOCIE", acteur="seed",
                     db_path=db_path)

    # Qualification ménage du fournisseur externe fictif (module Ménages, migration 0019) : sans
    # cela la recette navigateur du cycle de vie ne peut affecter aucun prestataire.
    from app.db.connection import get_db as _get_db
    conn = _get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, type_menage, "
            "date_debut_validite) VALUES (?,?,?)", (men, "EXTERNE", "2026-01-01"))
        conn.commit()
    finally:
        conn.close()

    print(f"   base factures de recette : {db_path.name} "
          f"({len(fact.lister(db_path=db_path))} factures, "
          f"{len(regl.lister(db_path=db_path))} règlements, 3 fournisseurs)")


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
    # Charges de recette : écrites dans la SAISIE (vérité), puis MASTER produit par Lot3 lui-même.
    build_saisie_charges(dst_flux)
    build_charges(dst_flux, REC / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm",
                  REC / "02_TRAVAIL" / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx")
    build_lot7(REC / "02_TRAVAIL" / "Lot7_IK_Avantages" / "MASTER_FACT_MAN_IK_Avantages.xlsx")
    # Copier les scripts moteur Python (CODE, aucune PII) — requis par le runner aval qui ajoute
    # <project_root>/02_TRAVAIL au sys.path pour importer lot3_generateur_charges & libs.
    dst_travail = REC / "02_TRAVAIL"
    dst_travail.mkdir(parents=True, exist_ok=True)
    for py in (WT / "02_TRAVAIL").glob("*.py"):
        shutil.copy2(py, dst_travail / py.name)
    build_pbi_logements(REC / "03_EXPORTS" / "PowerBI" / "PBI_Referentiel_Logements.csv")
    # Sources amont de la chaîne aval (réservations live lot4bis + payout + ménages externes).
    # Appelé AVANT build_banque : ce builder ne touche pas au fichier bancaire, mais l'ordre rend
    # explicite que build_banque est la seule autorité sur BANQUE_LOT8_IMPORT.xlsx.
    build_reservations_recette.build()
    build_entetes_seules()
    build_banque(REC / "02_TRAVAIL" / "Lot8_Banque" / "BANQUE_LOT8_IMPORT.xlsx")
    # dossiers data applicatifs isolés
    (REC / "data" / "snapshots").mkdir(parents=True, exist_ok=True)
    # Dossiers de sortie des lots aval : certains moteurs écrivent sans créer leur répertoire
    # (pd.ExcelWriter échoue alors APRÈS avoir fait tout le travail). En réel ils existent déjà.
    for rel in ("02_TRAVAIL/Lot9_FluxUnifie", "02_TRAVAIL/Lot10_Resultats",
                "02_TRAVAIL/Lot11_Controles", "02_TRAVAIL/Lot12_Factures",
                "03_EXPORTS/PowerBI"):
        (REC / rel).mkdir(parents=True, exist_ok=True)
    # Base applicative de recette (fournisseurs/factures/règlements) — jamais la base réelle.
    # Le serveur de recette doit être lancé avec APP_DATA_DIR=<data_recette>/app_data pour la lire.
    build_factures_sqlite(REC / "app_data" / "app.db")
    print("data_recette généré :", REC.resolve())
    for p in sorted(REC.rglob("*")):
        if p.is_file():
            print("  ", p.relative_to(REC))


if __name__ == "__main__":
    main()

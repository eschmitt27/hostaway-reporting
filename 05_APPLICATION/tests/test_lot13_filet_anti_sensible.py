"""Contrat d'export Power BI de lot13 : filet de confidentialité et renommage de frontière.

Historique — le pilotage des calculs a permis d'exécuter la chaîne aval de bout en bout pour la
première fois, et lot13 s'est révélé **systématiquement bloquant** : sa whitelist `PBI_Commissions`
exportait `preparation_canape_voyageurs`, que son propre filet anti-sensible refuse (motif
`voyageur`). Le défaut était statique, donc valable aussi en mode réel.

Décision retenue : la colonne porte un **montant**, jamais une identité. Elle n'est ni supprimée ni
exemptée du filet ; elle est **renommée à la frontière d'export** en `montant_preparation_canape`.
Le nom historique reste celui des `MASTER_*` (lot10 le produit, lot11 le contrôle) — aucun calcul
métier n'est touché.

Ces tests remplacent le `xfail(strict=True)` qui tenait le défaut : ils prouvent la correction au
lieu de la constater.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import app.config as cfg

MOTEUR = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "lot13_export_powerbi.py"

ANCIEN_NOM = "preparation_canape_voyageurs"
NOUVEAU_NOM = "montant_preparation_canape"


def _source() -> str:
    if not MOTEUR.exists():
        pytest.skip(f"Moteur absent de cette racine : {MOTEUR}")
    return MOTEUR.read_text(encoding="utf-8")


def _regex_sensible(src: str) -> re.Pattern:
    """Reconstruit le motif SENSIBLE depuis le source, sans importer le moteur (il s'exécute au
    chargement du module et exigerait pandas)."""
    m = re.search(r"SENSIBLE\s*=\s*re\.compile\((.*?),\s*re\.I\)", src, re.S)
    assert m, "Motif SENSIBLE introuvable dans lot13 — la structure du moteur a changé."
    return re.compile(ast.literal_eval(f"({m.group(1)})"), re.I)


def _renommages(src: str) -> dict[str, dict[str, str]]:
    m = re.search(r"RENOMMAGES_EXPORT\s*=\s*(\{.*?\n\})", src, re.S)
    assert m, "RENOMMAGES_EXPORT introuvable dans lot13."
    return ast.literal_eval(m.group(1))


def _colonnes_whitelistees(src: str) -> dict[str, list[str]]:
    """Extrait {nom_export: [colonnes source]} des tuples EXPORTS, par analyse syntaxique."""
    arbre = ast.parse(src)
    out: dict[str, list[str]] = {}
    for noeud in ast.walk(arbre):
        if not isinstance(noeud, ast.Tuple) or len(noeud.elts) != 4:
            continue
        nom, _, _, cols = noeud.elts
        if not (isinstance(nom, ast.Constant) and isinstance(nom.value, str)
                and nom.value.startswith("PBI_")):
            continue
        if not isinstance(cols, ast.List):
            continue
        out[nom.value] = [c.value for c in cols.elts
                          if isinstance(c, ast.Constant) and isinstance(c.value, str)]
    return out


def _entetes_exportees(src: str) -> dict[str, list[str]]:
    """Noms tels qu'ils SORTENT réellement du système : whitelist après renommage de frontière."""
    ren = _renommages(src)
    return {export: [ren.get(export, {}).get(c, c) for c in cols]
            for export, cols in _colonnes_whitelistees(src).items()}


# ── Filet de confidentialité : toujours actif ────────────────────────────────

def test_le_filet_anti_sensible_est_reconstructible():
    rx = _regex_sensible(_source())
    assert rx.search("email_proprietaire")
    assert rx.search("iban")
    assert not rx.search("logement_id")


def test_le_filet_refuse_toujours_une_vraie_colonne_nominative():
    """Le renommage ne doit pas avoir affaibli le filet : une colonne réellement nominative
    contenant « voyageur » reste refusée."""
    rx = _regex_sensible(_source())
    for nominative in ("nom_voyageur", "voyageur_email", "guest_name", "telephone_voyageur",
                       "adresse_voyageur"):
        assert rx.search(nominative), f"{nominative} devrait être refusée par le filet."


def test_aucune_donnee_personnelle_dans_les_entetes_exportees():
    """Aucun nom, email, téléphone ni identifiant voyageur ne sort, quel que soit l'export."""
    src = _source()
    rx = _regex_sensible(src)
    fautives = {export: [n for n in entetes if rx.search(n)]
                for export, entetes in _entetes_exportees(src).items()}
    fautives = {k: v for k, v in fautives.items() if v}
    assert not fautives, f"Colonnes sensibles exportées : {fautives}"


# ── Migration du contrat PBI_Commissions ─────────────────────────────────────

def test_l_ancienne_colonne_n_est_plus_exportee():
    assert ANCIEN_NOM not in _entetes_exportees(_source())["PBI_Commissions"]


def test_la_nouvelle_colonne_est_exportee():
    assert NOUVEAU_NOM in _entetes_exportees(_source())["PBI_Commissions"]


def test_jamais_les_deux_colonnes_a_la_fois():
    entetes = _entetes_exportees(_source())["PBI_Commissions"]
    assert not (ANCIEN_NOM in entetes and NOUVEAU_NOM in entetes)


def test_le_nom_historique_reste_la_source_lue():
    """Le renommage est une projection : la donnée lue reste la colonne historique des MASTER_*.
    Sans cela, l'export sortirait vide au lieu de sortir sous un autre nom."""
    src = _source()
    assert ANCIEN_NOM in _colonnes_whitelistees(src)["PBI_Commissions"]
    assert _renommages(src)["PBI_Commissions"][ANCIEN_NOM] == NOUVEAU_NOM


def test_la_table_de_renommage_reste_minimale_et_verrouillee():
    """Un renommage laisse par construction sortir une colonne autrement refusée. La table doit
    donc rester exactement celle qui a été décidée : toute addition casse ce test et exige une
    décision explicite."""
    assert _renommages(_source()) == {"PBI_Commissions": {ANCIEN_NOM: NOUVEAU_NOM}}


def test_le_contrat_pbi_commissions_est_celui_documente():
    """Schéma attendu de PBI_Commissions — le contrat consommé par Power BI."""
    attendu = [
        "reservation_calc_id", "reservation_id_hostaway", "logement_id", "proprietaire_id",
        "mois", "date_arrivee", "date_depart", "nuits", "channel_type", "source_type",
        "statut_calcul_payout", "payout_calcule", "menage_retenu", "assiette_commission",
        "taux_commission", "commission_conciergerie", NOUVEAU_NOM,
        "controle_preparation_canape", "net_proprietaire",
    ]
    assert _entetes_exportees(_source())["PBI_Commissions"] == attendu


# ── Sortie réelle, quand un run a eu lieu sur cette racine ───────────────────

def _csv_commissions() -> Path:
    return Path(cfg.PROJECT_ROOT) / "03_EXPORTS" / "PowerBI" / "PBI_Commissions.csv"


def _lire_csv(p: Path) -> tuple[list[str], list[list[str]]]:
    import csv
    with p.open(encoding="utf-8-sig", newline="") as f:
        lignes = list(csv.reader(f, delimiter=";"))
    return (lignes[0], lignes[1:]) if lignes else ([], [])


def test_csv_exporte_porte_le_nouveau_nom():
    p = _csv_commissions()
    if not p.exists():
        pytest.skip("Aucun export lot13 sur cette racine.")
    entetes, _ = _lire_csv(p)
    assert NOUVEAU_NOM in entetes
    assert ANCIEN_NOM not in entetes


def test_csv_exporte_valeur_numerique_et_identique_a_la_source():
    """La valeur et le type sont conservés : le renommage ne touche qu'un libellé de colonne."""
    p = _csv_commissions()
    source = (Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "Lot10_Resultats"
              / "MASTER_CALC_Commissions.xlsx")
    if not (p.exists() and source.exists()):
        pytest.skip("Aucun export ou aucune source lot10 sur cette racine.")

    import openpyxl
    wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    try:
        it = wb["COMMISSIONS"].iter_rows(values_only=True)
        hdr = [str(c) for c in next(it)]
        i_cle, i_val = hdr.index("reservation_calc_id"), hdr.index(ANCIEN_NOM)
        attendu = {str(r[i_cle]): r[i_val] for r in it if r[i_cle] is not None}
    finally:
        wb.close()

    entetes, lignes = _lire_csv(p)
    j_cle, j_val = entetes.index("reservation_calc_id"), entetes.index(NOUVEAU_NOM)
    assert lignes, "Export vide : le renommage aurait fait perdre la donnée."
    compares = 0
    for ligne in lignes:
        brut = ligne[j_val]
        if brut == "":
            continue
        valeur = float(brut)                       # type numérique conservé
        assert valeur == pytest.approx(float(attendu[ligne[j_cle]]))
        compares += 1
    assert compares > 0, "Aucune valeur non vide comparée — preuve insuffisante."

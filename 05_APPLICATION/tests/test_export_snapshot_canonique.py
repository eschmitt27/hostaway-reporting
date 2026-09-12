"""Export Power BI — vérification par SNAPSHOT CANONIQUE, indépendant de la source.

POURQUOI CE FICHIER EXISTE
Le contrôle précédent comparait l'export au classeur `MASTER_CALC_Commissions.xlsx`. Ce classeur est
figé au 2026-09-09 et porte l'ANCIEN schéma de clés (`RES-2025-01-HA-001`) quand
`reservation_calc_id` vaut désormais `RES-HA-53441757` : les deux ensembles sont entièrement
disjoints, la comparaison est devenue impossible. Le remplacer par « export SQLite contre table
SQLite » aurait produit une preuve TAUTOLOGIQUE — elle aurait vérifié que SQLite ressemble à
SQLite, et serait restée verte devant n'importe quelle erreur de sémantique métier.

CE QUE FAIT CE FICHIER À LA PLACE
Il pose un jeu de données MINIMAL, ÉCRIT À LA MAIN, dont chaque valeur est connue :

    1 propriétaire · 2 logements · 3 réservations · nuits · montant perçu
    commission · net propriétaire · ménage

puis il énonce, en toutes lettres, ce que l'export DOIT contenir — valeurs comprises. La référence
n'est ni la base ni un artefact régénéré : elle est dans ce fichier, lisible, et un écart se voit.

CE QUE ÇA ATTRAPE, ET QU'UNE COMPARAISON À LA SOURCE NE VERRAIT PAS
Une colonne qui glisse d'une position, un renommage qui emporte la mauvaise colonne, un montant
divisé par cent, un identifiant technique qui fuit dans un fichier destiné à un tiers, une ligne
perdue au filtrage du run actif, un séparateur ou un encodage qui change. Dans tous ces cas la
table source reste identique à elle-même — et c'est bien pourquoi la comparer à elle-même ne prouve
rien.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import lot13_export_service as lot13

# ── Le jeu canonique. Chaque nombre est choisi pour être reconnaissable dans un fichier. ─────────

PROP = "PROP_SNAP"
LOG_A = "LOG_SNAP_A"
LOG_B = "LOG_SNAP_B"
MOIS = "2026-04"
RUN = "RUN-SNAP"

#: (reservation_calc_id, logement, nuits, payout, ménage, assiette, taux, commission, net)
RESERVATIONS = [
    ("RES-SNAP-0001", LOG_A, 3, 300.00, 30.00, 270.00, 0.20, 54.00, 216.00),
    ("RES-SNAP-0002", LOG_A, 5, 500.00, 30.00, 470.00, 0.20, 94.00, 376.00),
    ("RES-SNAP-0003", LOG_B, 2, 200.00, 40.00, 160.00, 0.15, 24.00, 136.00),
]

#: Ce que le fichier `PBI_Commissions.csv` doit contenir, colonne par colonne. Écrit ici, pas lu
#: de la base : c'est ce qui en fait une référence et non un miroir.
#:
#: LA FORME DES NOMBRES FAIT PARTIE DU CONTRAT. Un nombre entier s'écrit SANS décimale (« 300 »,
#: pas « 300.0 ») — règle délibérée de `lot13_export_service._val`, alignée sur ce que le legacy
#: produisait et sur ce que Power BI lit. Une valeur non entière garde toutes ses décimales
#: (« 12.5 », « 0.2 »). Ces littéraux pinnent donc aussi ce rendu : le changer casserait la lecture
#: de toutes les colonnes de comptage et de montant rond, sans qu'un seul chiffre bouge en base.
ATTENDU_COMMISSIONS = {
    "RES-SNAP-0001": {"logement_id": LOG_A, "mois": MOIS, "nuits": "3", "payout_calcule": "300",
                      "menage_retenu": "30", "assiette_commission": "270",
                      "taux_commission": "0.2", "commission_conciergerie": "54",
                      "montant_preparation_canape": "12.5", "net_proprietaire": "216"},
    "RES-SNAP-0002": {"logement_id": LOG_A, "mois": MOIS, "nuits": "5", "payout_calcule": "500",
                      "menage_retenu": "30", "assiette_commission": "470",
                      "taux_commission": "0.2", "commission_conciergerie": "94",
                      "montant_preparation_canape": "0", "net_proprietaire": "376"},
    "RES-SNAP-0003": {"logement_id": LOG_B, "mois": MOIS, "nuits": "2", "payout_calcule": "200",
                      "menage_retenu": "40", "assiette_commission": "160",
                      "taux_commission": "0.15", "commission_conciergerie": "24",
                      "montant_preparation_canape": "0", "net_proprietaire": "136"},
}

ATTENDU_NET_PROPRIETAIRE = {
    "total_payout_mois": "1000",
    "total_menage_mois": "100",
    "total_commission_mois": "172",
    "net_proprietaire_avant_charge_mois": "728",
    "nb_reservations": "3",
}

ATTENDU_COUT_COMPLET = {"nb_menages": "4", "cout_standard_total": "120",
                        "cout_complet_total": "143.5"}


@pytest.fixture()
def base_canonique(tmp_db):
    """Le jeu minimal, écrit tel quel. Aucune règle métier n'est rejouée ici : ce fichier vérifie
    l'EXPORT, pas le calcul — les moteurs ont leurs propres tests."""
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, mode_facturation, "
            "actif, import_id) VALUES (?,?,?,?,?)",
            (PROP, "Duval", "PAR_LOGEMENT", "OUI", "IMP-SNAP"))
        conn.executemany(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, ville, "
            "type_logement_id, sur_hostaway, actif, statut_parc, "
            "forfait_logiciel_consommables_mensuel, import_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(LOG_A, "Studio Snapshot A", "Snap A", "Toulouse", "TYPE_001", "OUI", "OUI", "GERE",
              12.0, "IMP-SNAP"),
             (LOG_B, "T2 Snapshot B", "Snap B", "Blagnac", "TYPE_002", "OUI", "OUI", "GERE",
              15.0, "IMP-SNAP")])
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, source, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("GST-SNAP", LOG_A, PROP, "2025-01-01", None, "ACTIF", "test", "IMP-SNAP"))
        conn.execute("INSERT INTO lot10_runs (run_id, date_calcul, actif) VALUES (?,?,1)",
                     (RUN, "2026-05-01T08:00:00Z"))

        canape = {"RES-SNAP-0001": 12.5}
        for (rid, logement, nuits, payout, menage, assiette, taux, commission, net) in RESERVATIONS:
            conn.execute(
                "INSERT INTO lot10_commissions (run_id, reservation_calc_id, "
                "reservation_id_hostaway, logement_id, proprietaire_id, mois, date_arrivee, "
                "date_depart, nuits, channel_type, source_type, statut_calcul_payout, "
                "payout_calcule, menage_retenu, assiette_commission, taux_commission, "
                "commission_conciergerie, preparation_canape_voyageurs, "
                "controle_preparation_canape, net_proprietaire) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (RUN, rid, rid.replace("RES-SNAP-", "9000"), logement, PROP, MOIS,
                 f"{MOIS}-10", f"{MOIS}-1{nuits}", nuits, "AIRBNB", "HOSTAWAY", "NORMAL",
                 payout, menage, assiette, taux, commission, canape.get(rid, 0.0), "OK", net))

        conn.execute(
            "INSERT INTO lot10_net_vue_mois (run_id, mois, proprietaire_id, total_payout_mois, "
            "total_menage_mois, total_commission_mois, total_preparation_canape_mois, "
            "charge_fixe_mensuelle, montant_du_conciergerie, reste_a_payer_conciergerie, "
            "net_proprietaire_avant_charge_mois, net_proprietaire_apres_charge_mois, "
            "nb_reservations) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (RUN, MOIS, PROP, 1000.0, 100.0, 172.0, 12.5, 27.0, 199.0, 199.0, 728.0, 701.0, 3))

        conn.execute(
            "INSERT INTO menages_cout_complet (mois, logement_id, proprietaire_id, "
            "type_logement_id, intervenant_id, type_intervenant, nb_menages, cout_standard_total, "
            "cout_direct_total, cout_complet_total, ecart_vs_standard_total, ecart_unitaire, "
            "statut_ecart, statut_controle) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (MOIS, LOG_A, PROP, "TYPE_001", "INT_0001", "EXTERNE", 4, 120.0, 120.0, 143.5,
             23.5, 5.875, "ECART", "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    return tmp_db


@pytest.fixture()
def exporte(base_canonique, tmp_path, monkeypatch):
    """Produit l'export dans un dossier jetable et rend le chemin."""
    sortie = tmp_path / "exports"
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", sortie)
    resultat = lot13.exporter(db_path=base_canonique, destination=sortie)
    assert resultat["ok"] is True, resultat.get("message")
    return sortie


def _lire(sortie: Path, nom: str) -> tuple[list[str], list[dict[str, str]]]:
    chemin = sortie / f"{nom}.csv"
    assert chemin.exists(), f"{nom}.csv non produit"
    with open(chemin, encoding="utf-8-sig", newline="") as f:
        lecteur = csv.reader(f, delimiter=";")
        entetes = next(lecteur)
        lignes = [dict(zip(entetes, r)) for r in lecteur]
    return entetes, lignes


# ── Les valeurs exportées sont EXACTEMENT celles attendues ──────────────────────────────────────

def test_commissions_exporte_les_valeurs_attendues(exporte):
    """Chaque montant est comparé à une valeur ÉCRITE DANS CE FICHIER, pas relue de la base."""
    _, lignes = _lire(exporte, "PBI_Commissions")
    par_id = {l["reservation_calc_id"]: l for l in lignes}
    assert set(par_id) == set(ATTENDU_COMMISSIONS), "l'export a perdu ou ajouté des réservations"
    for rid, attendu in ATTENDU_COMMISSIONS.items():
        ligne = par_id[rid]
        for colonne, valeur in attendu.items():
            assert ligne[colonne] == valeur, f"{rid}.{colonne} = {ligne[colonne]!r}, attendu {valeur!r}"


def test_net_proprietaire_exporte_les_valeurs_attendues(exporte):
    _, lignes = _lire(exporte, "PBI_Net_Proprietaire")
    assert len(lignes) == 1
    ligne = lignes[0]
    assert ligne["proprietaire_id"] == PROP and ligne["mois"] == MOIS
    for colonne, valeur in ATTENDU_NET_PROPRIETAIRE.items():
        assert ligne[colonne] == valeur, f"{colonne} = {ligne[colonne]!r}, attendu {valeur!r}"


def test_cout_menage_exporte_les_valeurs_attendues(exporte):
    _, lignes = _lire(exporte, "PBI_Menages_Cout_Complet")
    assert len(lignes) == 1
    for colonne, valeur in ATTENDU_COUT_COMPLET.items():
        assert lignes[0][colonne] == valeur


def test_les_totaux_du_mois_concordent_avec_le_detail(exporte):
    """La somme des commissions du détail doit valoir le total du mois.

    Ce contrôle est le seul du fichier qui confronte deux exports entre eux — il ne remplace pas
    les valeurs attendues ci-dessus, il vérifie en plus qu'aucun des deux n'a dérivé seul.
    """
    _, detail = _lire(exporte, "PBI_Commissions")
    _, mois = _lire(exporte, "PBI_Net_Proprietaire")
    somme = round(sum(float(l["commission_conciergerie"]) for l in detail), 2)
    assert somme == float(mois[0]["total_commission_mois"]) == 172.0
    assert round(sum(float(l["payout_calcule"]) for l in detail), 2) \
        == float(mois[0]["total_payout_mois"]) == 1000.0
    assert len(detail) == int(mois[0]["nb_reservations"]) == 3


# ── Le référentiel exporté reste lisible et sans donnée de trop ─────────────────────────────────

def test_le_referentiel_logements_exporte_les_bonnes_lignes(exporte):
    _, lignes = _lire(exporte, "PBI_Referentiel_Logements")
    par_id = {l["logement_id"]: l for l in lignes}
    assert set(par_id) == {LOG_A, LOG_B}
    assert par_id[LOG_A]["nom_court"] == "Snap A"
    assert par_id[LOG_A]["ville"] == "Toulouse"
    assert par_id[LOG_A]["statut_parc"] == "GERE"


def test_le_referentiel_proprietaires_n_exporte_pas_de_coordonnees(exporte):
    """Le filet anti-sensible existe ; ce test vérifie son EFFET sur des données réelles."""
    entetes, lignes = _lire(exporte, "PBI_Referentiel_Proprietaires")
    assert lignes[0]["nom_proprietaire"] == "Duval"
    for interdit in ("email", "telephone", "adresse_facturation", "iban"):
        assert interdit not in [e.lower() for e in entetes], interdit


# ── Forme du fichier : ce qui casse un tableur sans rien casser en base ─────────────────────────

def test_le_fichier_est_lisible_par_un_tableur_francais(exporte):
    """Séparateur `;` et BOM utf-8. Une base parfaitement juste produit un fichier illisible si
    l'un des deux change, et rien en base ne le signalerait."""
    brut = (exporte / "PBI_Commissions.csv").read_bytes()
    assert brut.startswith(b"\xef\xbb\xbf"), "BOM utf-8 absent : accents illisibles dans Excel FR"
    premiere = brut.decode("utf-8-sig").splitlines()[0]
    assert ";" in premiere and "\t" not in premiere


def test_le_dictionnaire_decrit_chaque_fichier_produit(exporte):
    _, dico = _lire(exporte, "PBI_Dictionnaire_Colonnes")
    tables = {l["table"] for l in dico}
    produits = {p.stem for p in exporte.glob("PBI_*.csv")} - {"PBI_Dictionnaire_Colonnes"}
    assert produits <= tables, f"non documenté(s) : {produits - tables}"
    entetes, _ = _lire(exporte, "PBI_Commissions")
    assert {l["colonne"] for l in dico if l["table"] == "PBI_Commissions"} == set(entetes)


def test_le_renommage_de_frontiere_porte_sur_la_bonne_colonne(exporte):
    """`preparation_canape_voyageurs` porte un MONTANT, jamais une identité : il est renommé à
    l'export. Le snapshot prouve que c'est bien sa valeur qui suit le nouveau nom — une inversion
    de colonnes passerait inaperçue si l'on ne comparait que des ensembles de noms."""
    entetes, lignes = _lire(exporte, "PBI_Commissions")
    assert "preparation_canape_voyageurs" not in entetes
    assert "montant_preparation_canape" in entetes
    par_id = {l["reservation_calc_id"]: l for l in lignes}
    assert par_id["RES-SNAP-0001"]["montant_preparation_canape"] == "12.5"
    assert par_id["RES-SNAP-0002"]["montant_preparation_canape"] == "0"


# ── Le run actif fait foi ───────────────────────────────────────────────────────────────────────

def test_un_run_inactif_n_est_pas_exporte(base_canonique, tmp_path, monkeypatch):
    """Plusieurs runs coexistent en base. Exporter les deux doublerait chaque montant — c'est
    exactement l'erreur qui a produit un total de 12 046 € pour un mois à 1 204 €."""
    conn = get_db(base_canonique)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, date_calcul, actif) VALUES (?,?,0)",
                     ("RUN-ANCIEN", "2026-04-01T08:00:00Z"))
        for (rid, logement, nuits, payout, menage, assiette, taux, commission, net) in RESERVATIONS:
            conn.execute(
                "INSERT INTO lot10_commissions (run_id, reservation_calc_id, logement_id, "
                "proprietaire_id, mois, nuits, payout_calcule, commission_conciergerie, "
                "net_proprietaire) VALUES (?,?,?,?,?,?,?,?,?)",
                ("RUN-ANCIEN", rid, logement, PROP, MOIS, nuits, payout * 99, commission * 99,
                 net * 99))
        conn.commit()
    finally:
        conn.close()

    sortie = tmp_path / "exports2"
    monkeypatch.setattr(cfg, "EXPORTS_POWERBI", sortie)
    assert lot13.exporter(db_path=base_canonique, destination=sortie)["ok"] is True
    _, lignes = _lire(sortie, "PBI_Commissions")
    assert len(lignes) == 3, "seul le run actif doit sortir"
    assert round(sum(float(l["commission_conciergerie"]) for l in lignes), 2) == 172.0

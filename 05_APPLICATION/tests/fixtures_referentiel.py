"""Semis du référentiel en SQLite, pour les tests.

Remplace les classeurs `REF_Setup.xlsx` que les tests fabriquaient : depuis la migration 0051, le
CRUD du parc écrit dans les tables `ref_*` (0029) et n'ouvre plus de classeur. Un test qui semait
un onglet sème désormais une table — mêmes données, même intention.

Insertion en SQL direct, comme `fixtures_lot10`/`fixtures_lot12` : ces tests vérifient le SERVICE
(validations, historisation), pas l'importateur, qui a ses propres tests.
"""
from __future__ import annotations

from typing import Any, Iterable

from app.db.connection import get_db

IMPORT_TEST = "IMP-TEST"


def _inserer(conn, table: str, lignes: Iterable[dict[str, Any]]) -> None:
    for l in lignes:
        cols = list(l.keys()) + ["import_id"]
        valeurs = [l[c] for c in l] + [IMPORT_TEST]
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
            valeurs)


def semer(db_path, *, logements=(), gestion=(), proprietaires=(), types=(), taux=(), couts=(),
          import_id: str = IMPORT_TEST) -> None:
    """Sème un référentiel minimal et marque l'import comme abouti.

    `ref_setup_repo.est_disponible()` exige un import au statut IMPORTE : sans cette ligne, le
    référentiel serait considéré « non initialisé » et les services refuseraient d'écrire — ce qui
    est le comportement voulu, mais pas ce que ces tests veulent éprouver.
    """
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES (?, '2026-01-01T00:00:00Z', 'fixture', 'fixture', "
            "'IMPORTE')", (import_id,))
        _inserer(conn, "ref_logements", logements)
        _inserer(conn, "ref_gestion_logements_hist", gestion)
        _inserer(conn, "ref_proprietaires", proprietaires)
        _inserer(conn, "ref_types_logements", types)
        _inserer(conn, "ref_taux_commission", taux)
        _inserer(conn, "ref_couts_standards_menage", couts)
        conn.commit()
    finally:
        conn.close()


def semer_parc_standard(db_path) -> None:
    """Le jeu que partageaient les tests de cycle de vie du parc.

    LOG_A1 actif avec un rattachement OUVERT et un taux ouvert ; LOG_INACTIF déjà archivé, son
    rattachement clos — c'est lui qui permet de tester la réactivation et le refus de double
    archivage.
    """
    semer(
        db_path,
        logements=[
            {"logement_id": "LOG_A1", "hostaway_listing_id": "900001",
             "nom_logement_officiel": "Fictif A1", "nom_court": "A1", "adresse": "1 rue",
             "ville": "RECETTE", "type_logement_id": "TYPE_001", "sur_hostaway": "OUI",
             "actif": "OUI", "statut_parc": "GERE", "commentaire": "",
             "forfait_logiciel_consommables_mensuel": "0"},
            {"logement_id": "LOG_INACTIF", "hostaway_listing_id": "900005",
             "nom_logement_officiel": "Fictif Inactif", "nom_court": "INACTIF", "adresse": "9 rue",
             "ville": "RECETTE", "type_logement_id": "TYPE_001", "sur_hostaway": "NON",
             "actif": "NON", "statut_parc": "RETIRE", "commentaire": "",
             "forfait_logiciel_consommables_mensuel": "0"},
        ],
        gestion=[
            {"gestion_id": "GST_1", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
             "date_debut": "2026-01-01", "date_fin": "", "statut_gestion": "ACTIF",
             "source": "FICTIF", "commentaire": ""},
            {"gestion_id": "GST_2", "logement_id": "LOG_INACTIF", "proprietaire_id": "PROP_C",
             "date_debut": "2026-01-01", "date_fin": "2026-05-31", "statut_gestion": "RETIRE",
             "source": "FICTIF", "commentaire": ""},
        ],
        proprietaires=[{"proprietaire_id": pid, "nom_proprietaire": f"Nom {pid}", "actif": "OUI"}
                       for pid in ("PROP_A", "PROP_B", "PROP_C")],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
        taux=[{"taux_commission_id": "TX_A1", "proprietaire_id": "PROP_A",
               "logement_id": "LOG_A1", "taux_commission": "0.19", "date_debut": "2026-01-01",
               "date_fin": "", "actif": "OUI", "justification": "FICTIF", "commentaire": ""}],
    )


def semer_referentiel_charges(db_path) -> None:
    """Référentiels nécessaires à la saisie Charges (formulaire Nouvelle charge / APP-3b).

    Complète `semer_parc_standard` (logements/gestion/proprietaires) avec les tables propres au
    formulaire charges : catégories, modes de paiement, codes d'impact, types de flux, clôture,
    assoc_mode, statuts.
    """
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES (?, '2026-01-01T00:00:00Z', 'fixture', 'fixture', "
            "'IMPORTE')", (IMPORT_TEST,))
        _inserer(conn, "ref_categories_charges", [
            {"categorie_charge_id": "CHG_017", "categorie_niveau_1": "Charges courantes",
             "categorie_niveau_2": "Generale", "description": "Charge generale", "actif": "OUI",
             "famille_impact_categorie": "GLOBAL"},
        ])
        _inserer(conn, "ref_modes_paiement", [
            {"mode_paiement_id": "PAY_001", "mode_paiement": "VIREMENT", "actif": "OUI"},
        ])
        _inserer(conn, "ref_codes_impact", [
            {"code_impact": "IC", "libelle": "Impact comptable", "actif": "OUI"},
        ])
        _inserer(conn, "ref_types_flux", [
            {"type_flux_id": "TYPE_FLUX_020", "type_flux": "Depense standard",
             "code_impact_defaut": "IC", "actif": "OUI"},
        ])
        _inserer(conn, "ref_cloture_mensuelle", [
            {"mois": "2026-06", "statut_mois": "OUVERT"},
            {"mois": "2026-01", "statut_mois": "CLOTURE"},
        ])
        _inserer(conn, "ref_assoc_mode", [
            {"assoc_mode_id": "AM_1", "mode_paiement_id": "PAY_001", "associe_id": "",
             "assoc_mode": "CONCIERGERIE", "actif": "OUI"},
        ])
        _inserer(conn, "ref_statuts", [
            {"statut_id": "STC_1", "famille_statut": "statut_controle", "statut": "A_CONTROLER",
             "actif": "OUI"},
        ])
        _inserer(conn, "ref_associes", [])
        _inserer(conn, "ref_cartes_paiement", [])
        _inserer(conn, "ref_types_affectation", [
            {"affectation_id": "AFF_1", "type_affectation": "GLOBAL", "actif": "OUI"},
        ])
        _inserer(conn, "ref_intervenants", [])
        _inserer(conn, "ref_couts_standards_menage", [])
        conn.commit()
    finally:
        conn.close()


def lignes(db_path, table: str) -> list[dict[str, str]]:
    """Contenu brut d'une table de référentiel — équivalent du `_lignes(p, sheet)` d'avant."""
    from app.services import ref_setup_repo as repo

    return repo.lire_table(table, db_path=db_path)

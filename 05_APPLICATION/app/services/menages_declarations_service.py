"""Saisie directe d'une déclaration de ménage interne — UI → SQLite, zéro Google Sheet.

`menages_declarations_internes` (migration 0038) était jusqu'ici alimentée uniquement par
`lot6b_m04_menages_internes.py` (Google Sheet M04). Ce module permet de créer une ligne
directement, avec EXACTEMENT les mêmes champs et la MÊME règle de coût que lot6b — reprise
verbatim de `lib_menage_costs.resolve_internal_cleaning_cost` (02_TRAVAIL), jamais réinventée.

La Google Sheet reste un chemin d'import possible (legacy), mais n'est plus nécessaire pour
qu'une nouvelle déclaration soit créée.
"""
from __future__ import annotations

import sys
from typing import Any

import app.config as cfg
from app.db.connection import get_db

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

SOURCE_UI = "UI_SAISIE"

E_LOGEMENT_INCONNU = "E_LOGEMENT_INCONNU"
E_INTERVENANT_INCONNU = "E_INTERVENANT_INCONNU"
E_MOIS_INVALIDE = "E_MOIS_INVALIDE"


def logements_actifs(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT logement_id, COALESCE(nom_court, nom_logement_officiel) AS libelle "
            "FROM ref_logements WHERE actif = 'OUI' ORDER BY libelle"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def intervenants_actifs(db_path=None) -> list[dict[str, Any]]:
    """Intervenants INTERNES uniquement — un ménage externe se déclare par facture PDF (§11 : une
    facture externe ne devient jamais une déclaration interne)."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT intervenant_id, nom_intervenant AS libelle, type_intervenant "
            "FROM ref_intervenants WHERE actif = 'OUI' AND type_intervenant = 'INTERNE' "
            "ORDER BY libelle"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _logement(conn, logement_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT logement_id, COALESCE(nom_court, nom_logement_officiel) AS libelle, "
        "type_logement_id FROM ref_logements WHERE logement_id = ? AND actif = 'OUI'",
        (logement_id,),
    ).fetchone()
    return dict(row) if row else None


def _intervenant(conn, intervenant_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT intervenant_id, nom_intervenant AS libelle, type_intervenant "
        "FROM ref_intervenants WHERE intervenant_id = ? AND actif = 'OUI'",
        (intervenant_id,),
    ).fetchone()
    return dict(row) if row else None


def creer(*, mois: str, logement_id: str, intervenant_id: str, nb_menages: int,
          nb_heures: float | None, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée une déclaration interne réelle. Statut `A_CONTROLER` si le coût ne se résout pas
    (référentiel absent/ambigu) — jamais un coût inventé pour faire disparaître l'anomalie."""
    from lib_menage_costs import resolve_internal_cleaning_cost

    if not mois or len(mois) != 7 or mois[4] != "-":
        return {"ok": False, "code": E_MOIS_INVALIDE, "message": "Mois attendu au format AAAA-MM."}

    conn = get_db(db_path)
    try:
        logement = _logement(conn, logement_id)
        if logement is None:
            return {"ok": False, "code": E_LOGEMENT_INCONNU,
                    "message": "Logement inconnu ou inactif."}
        intervenant = _intervenant(conn, intervenant_id)
        if intervenant is None:
            return {"ok": False, "code": E_INTERVENANT_INCONNU,
                    "message": "Intervenant inconnu ou inactif."}
        if intervenant.get("type_intervenant") != "INTERNE":
            return {"ok": False, "code": E_INTERVENANT_INCONNU,
                    "message": "Une déclaration interne ne peut porter que sur un intervenant "
                               "INTERNE — un ménage externe se déclare par facture fournisseur."}

        hourly_ref = [dict(r) for r in conn.execute(
            "SELECT * FROM ref_taux_heures_menage").fetchall()]
        fixed_ref = [dict(r) for r in conn.execute(
            "SELECT * FROM ref_couts_menage_interne").fetchall()]

        cost = resolve_internal_cleaning_cost(
            ref_date=f"{mois}-01",
            intervenant_id=intervenant_id,
            logement_id=logement_id,
            type_logement_id=logement.get("type_logement_id"),
            nb_menages=nb_menages,
            nb_heures=nb_heures,
            hourly_rows=hourly_ref,
            fixed_rows=fixed_ref,
        )
        cout = cost.total if cost.status == "OK" else None
        statut_controle = "OK" if cost.status == "OK" else "A_CONTROLER"
        code_controle = None if cost.status == "OK" else f"COUT_INTERNE_{cost.status}"

        cur = conn.execute(
            "INSERT INTO menages_declarations_internes "
            "(mois, annee, mois_saisie, nom_appartement, logement_id, nom_intervenant, "
            "intervenant_id, type_intervenant, nb_menages, nb_heures, cout_lavage_attribue, "
            "statut_controle, code_controle, source_url, date_extraction, run_id) "
            "VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%SZ','now'),?,?,?,?,?,?,?,?,?,?,?,"
            "strftime('%Y-%m-%dT%H:%M:%SZ','now'),?)",
            (mois, mois[:4], logement["libelle"], logement_id, intervenant["libelle"],
             intervenant_id, intervenant.get("type_intervenant"), nb_menages, nb_heures, cout,
             statut_controle, code_controle, SOURCE_UI, acteur),
        )
        conn.commit()
        return {"ok": True, "id": cur.lastrowid, "statut_controle": statut_controle,
                "cout_lavage_attribue": cout, "code_controle": code_controle}
    finally:
        conn.close()

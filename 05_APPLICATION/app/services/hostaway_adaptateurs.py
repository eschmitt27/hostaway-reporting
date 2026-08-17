"""Adaptateurs d'entrée Hostaway : masters legacy → couche RAW SQLite.

À QUOI CELA SERT, ET POUR COMBIEN DE TEMPS
La couche RAW doit être peuplée sans réinterroger l'API : pour établir la parité avec la baseline
legacy, pour rejouer un calcul sur une donnée connue, et pour qu'une installation neuve dispose de
l'historique déjà extrait. Ce module lit donc les masters `MASTER_*_HA_*.xlsx` produits par Lot 1 et
les écrit en base, à l'identique.

C'est une REPRISE, pas une source. Une fois l'extraction SQLite en service, le chemin normal est
API → base ; ces masters ne servent plus qu'à comparer. Le sens de lecture est important : ce module
lit Excel pour alimenter SQLite, il n'écrit jamais dans Excel.

AUCUNE VALEUR N'EST RÉINTERPRÉTÉE
Les colonnes sont reprises telles quelles, y compris les champs de contrôle que Lot 1 a calculés
(`controle_guestCount`, `code_controle_guestCount`, `statut_calcul_payout`). Recalculer ici ce que le
moteur a décidé produirait deux verdicts pour la même donnée.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import app.config as cfg

# Sous-répertoire des masters Hostaway, relatif à la racine du projet.
DOSSIER_LOT1 = ("02_TRAVAIL", "Lot1_Hostaway")

# Nom de fichier → clé attendue par `hostaway_raw_service.enregistrer()`.
MASTERS = {
    "listings": "MASTER_REF_HA_Listings.xlsx",
    "reservations": "MASTER_FACT_HA_Reservations.xlsx",
    "payouts": "MASTER_CALC_HA_Payout.xlsx",
    "fees": "MASTER_FACT_HA_ReservationFees.xlsx",
    "finance_fields": "MASTER_FACT_HA_ReservationFinanceFields.xlsx",
    "anomalies": "MASTER_CTRL_HA_Anomalies.xlsx",
}

# Les détails vivent dans un master à part et n'alimentent pas une table dédiée : leur seule donnée
# utile est le payload, qui rejoint la réservation correspondante.
MASTER_DETAILS = "MASTER_FACT_HA_ReservationDetails.xlsx"

ONGLET = "data"

E_MASTERS_ABSENTS = "HOSTAWAY_MASTERS_ABSENTS"


class SourceHostawayInvalide(Exception):
    """Le master existe mais ne présente pas la structure attendue."""


def _racine(racine: Path | None = None) -> Path:
    return Path(racine) if racine is not None else Path(cfg.PROJECT_ROOT)


def dossier_masters(racine: Path | None = None) -> Path:
    return _racine(racine).joinpath(*DOSSIER_LOT1)


def _lire_onglet(chemin: Path, onglet: str = ONGLET) -> list[dict[str, Any]]:
    """Lignes d'un onglet, en dictionnaires. Liste vide si le fichier ou l'onglet manque.

    Un master absent est un état possible — tous ne sont pas produits à chaque exécution — et se
    distingue d'un master illisible, qui lève.
    """
    import openpyxl

    if not chemin.exists():
        return []
    wb = openpyxl.load_workbook(str(chemin), read_only=True, data_only=True)
    try:
        if onglet not in wb.sheetnames:
            raise SourceHostawayInvalide(f"{chemin.name} : onglet « {onglet} » absent.")
        ws = wb[onglet]
        lignes = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if len(lignes) <= 1:
        return []
    entetes = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(lignes[0])]
    return [dict(zip(entetes, r)) for r in lignes[1:]
            if any(v is not None and str(v).strip() for v in r)]


def depuis_masters_excel(racine: Path | None = None) -> dict[str, list[dict[str, Any]]]:
    """Lit tous les masters Hostaway disponibles et rend les jeux prêts pour la couche RAW.

    Le payload des détails est rattaché à sa réservation : le repli historique de `guestCount` en
    dépend, et le laisser dans un jeu séparé obligerait chaque appelant à refaire la jointure.
    """
    dossier = dossier_masters(racine)
    jeux = {cle: _lire_onglet(dossier / fichier) for cle, fichier in MASTERS.items()}

    details = _lire_onglet(dossier / MASTER_DETAILS)
    payload_par_reservation = {
        str(d.get("reservation_id")): d.get("json_snapshot")
        for d in details if d.get("reservation_id") is not None and d.get("json_snapshot")
    }
    for reservation in jeux["reservations"]:
        charge = payload_par_reservation.get(str(reservation.get("reservation_id")))
        if charge:
            reservation["json_snapshot"] = charge

    return jeux


def masters_disponibles(racine: Path | None = None) -> dict[str, bool]:
    """Quel master est présent. Sert à expliquer une reprise incomplète plutôt qu'à la deviner."""
    dossier = dossier_masters(racine)
    presents = {cle: (dossier / fichier).exists() for cle, fichier in MASTERS.items()}
    presents["details"] = (dossier / MASTER_DETAILS).exists()
    return presents


def reprendre(*, racine: Path | None = None, db_path=None) -> dict[str, Any]:
    """Charge les masters Hostaway en base, sous une extraction de mode REPRISE_EXCEL.

    Le mode est explicite : une extraction reprise d'un classeur ne doit pas se confondre avec une
    extraction API. La distinction compte pour juger la fraîcheur — un classeur peut dater.
    """
    from app.services import hostaway_raw_service as raw

    presents = masters_disponibles(racine)
    if not presents.get("reservations"):
        return {"ok": False, "code": E_MASTERS_ABSENTS,
                "message": ("Master des réservations Hostaway introuvable : la reprise n'a rien à "
                            "lire."),
                "masters": presents}

    jeux = depuis_masters_excel(racine)
    extraction_id = raw.ouvrir(mode=raw.MODE_REPRISE_EXCEL, db_path=db_path)
    try:
        compte = raw.enregistrer(extraction_id, db_path=db_path, **jeux)
    except Exception as exc:
        raw.cloturer(extraction_id, statut=raw.ST_ECHEC,
                     message=f"{type(exc).__name__}: {exc}", db_path=db_path)
        raise

    # Un master manquant rend la reprise PARTIELLE, pas réussie : les payouts ou les frais absents
    # changent ce que les calculs aval pourront produire, et le taire les ferait passer pour nuls.
    complet = all(presents.get(cle) for cle in MASTERS)
    resultat = raw.cloturer(
        extraction_id,
        statut=raw.ST_SUCCES if complet else raw.ST_PARTIEL,
        message="" if complet else "Masters absents : "
                + ", ".join(sorted(c for c in MASTERS if not presents.get(c))),
        db_path=db_path)
    return {"ok": True, **resultat, "detail": compte, "masters": presents}

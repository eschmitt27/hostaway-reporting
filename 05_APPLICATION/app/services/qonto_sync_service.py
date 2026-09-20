"""Synchronisation Qonto → couche RAW. Lecture seule côté banque, aucune écriture comptable.

    ClientQontoLectureSeule (GET only) → collecte complète en mémoire → qonto_raw_service

DEUX TEMPS, JAMAIS MÉLANGÉS
  1. `collecter()` ne fait que du réseau : organisation, comptes, puis TOUTES les pages de
     mouvements de chaque compte. Elle n'ouvre aucune base.
  2. `qonto_raw_service.enregistrer()` ne fait que de la base, en une transaction.

C'est ce découpage qui tient la promesse « une erreur d'API conserve le dernier jeu valide » : si
la pagination casse à la troisième page, la phase 1 lève, la phase 2 n'est jamais atteinte, et
rien n'a été écrit — pas même partiellement. L'échec est seulement tracé dans `qonto_sync_runs`.

CE QUE CE MODULE NE FAIT PAS, ET NE DOIT JAMAIS FAIRE : créer un règlement, rapprocher un
mouvement d'une facture, modifier une créance ou une dette, toucher `charges`, `mouvements_*`,
`factures*` ou `reglements*`, émettre un virement, s'inscrire à un ordonnanceur. La synchronisation
est déclenchée par un appel explicite, jamais par une minuterie.
"""
from __future__ import annotations

from app.adapters import qonto_client
from app.adapters.qonto_client import ErreurQonto
from app.services import qonto_raw_service as raw

E_TABLES_ABSENTES = "QONTO_TABLES_ABSENTES"
MESSAGE_TABLES_ABSENTES = ("La base n'a pas encore les tables Qonto (migration 0095) : "
                           "redémarrez l'application pour appliquer les migrations.")


def collecter(client=None) -> dict:
    """Phase RÉSEAU. Retourne l'organisation, ses comptes et tous leurs mouvements.

    Aucune écriture, aucune connexion SQLite. Lève `ErreurQonto` au premier vrai problème : mieux
    vaut ne rien enregistrer qu'enregistrer une moitié d'historique qu'on croira complète.
    """
    client = client or qonto_client.ClientQontoLectureSeule()
    organisation = client.organisation()
    comptes = organisation.get("bank_accounts") or []

    mouvements: list[dict] = []
    pages = 0
    for compte in comptes:
        identifiant = compte.get("id")
        if not identifiant:
            continue
        lot, pages_compte = client.transactions(identifiant)
        mouvements.extend(lot)
        pages += pages_compte

    return {
        "organisation": organisation.get("legal_name") or organisation.get("name") or "",
        "comptes": comptes,
        "mouvements": mouvements,
        "pages_lues": pages,
    }


def synchroniser(*, client=None, db_path=None) -> dict:
    """Synchronisation complète : collecte réseau puis écriture RAW atomique.

    Retourne un bilan chiffré. En cas d'échec, `ok` est faux, l'échec est journalisé, et les
    données déjà importées sont intactes.
    """
    if not raw.tables_presentes(db_path=db_path):
        return {"ok": False, "code": E_TABLES_ABSENTES, "message": MESSAGE_TABLES_ABSENTES}

    try:
        collecte = collecter(client)
    except ErreurQonto as err:
        run_id = raw.enregistrer_echec(err.code, err.detail, db_path=db_path)
        return {"ok": False, "code": err.code, "message": str(err), "sync_run_id": run_id}

    bilan = raw.enregistrer(collecte["comptes"], collecte["mouvements"],
                            pages_lues=collecte["pages_lues"], db_path=db_path)
    bilan["ok"] = True
    bilan["organisation"] = collecte["organisation"]
    return bilan


def etat(*, db_path=None) -> dict:
    """Ce que l'écran peut montrer : comptes (IBAN masqué), compteurs, dernière synchronisation."""
    if not raw.tables_presentes(db_path=db_path):
        return {"disponible": False, "message": MESSAGE_TABLES_ABSENTES}
    return {
        "disponible": True,
        "identifiants_presents": qonto_client.identifiants_presents(),
        "comptes": raw.comptes(db_path=db_path),
        "compteurs": raw.compter(db_path=db_path),
        "derniere_synchronisation": raw.derniere_synchronisation(db_path=db_path),
    }

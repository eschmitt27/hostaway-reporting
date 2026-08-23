"""Compte global propriétaire — allocations FIFO, crédits, compensations.

PRINCIPE
Un propriétaire a UN compte, tous logements confondus. L'argent disponible solde ses factures de la
plus ancienne à la plus récente. L'utilisateur ne choisit jamais quelle facture un paiement solde :
c'est la règle métier, pas une commodité d'implémentation.

CE QUI RESTE DISTINCT
Le compte calcule une position ; il ne fusionne aucun objet. Restent séparés et lisibles :
la FACTURE, le PAIEMENT REÇU, la SOMME À REVERSER, la COMPENSATION, et le CRÉDIT.

LE CRÉDIT N'EST PAS UN OBJET
Un excédent de paiement ne crée pas de facture fictive ni de ligne de crédit : c'est la part d'une
source qui n'a trouvé aucune facture à solder. Il se déduit. Conséquence directe et voulue : quand
une facture est émise plus tard, le crédit existant la solde au recalcul suivant, sans qu'aucune
écriture de « report » n'ait à être faite.

DEUX SOURCES, UN SEUL ORDRE
    PAIEMENT     mouvement PROPRIETAIRE_VERS_SOCIETE validé — argent reçu.
    REVERSEMENT  mouvement SOCIETE_VERS_PROPRIETAIRE validé — utilisé en COMPENSATION.
Elles sont consommées dans l'ordre CHRONOLOGIQUE, sans priorité de type. Un ordre par type aurait
demandé de décider laquelle prime, décision que rien dans le métier ne tranche ; l'ordre
chronologique, lui, se lit directement dans les données.

POURQUOI L'AVOIR N'EST PAS UNE SOURCE
Un avoir réduit déjà ce que doit le propriétaire : `creances_dettes_service` le présente comme une
créance de montant NÉGATIF, et c'est la représentation en place. En faire aussi une source FIFO le
compterait DEUX FOIS — une fois en diminuant le solde de la facture, une fois en diminuant le total
des créances. Ce piège a été constaté en test, pas supposé.

Une autre lecture existe — « un avoir solde en priorité la facture qu'il corrige », dont il porte
d'ailleurs la référence dans `facture_origine`. Elle est défendable, elle n'est pas appliquée :
elle changerait une règle métier existante, et rien dans le cadrage ne la demande.

DÉTERMINISME
Mêmes entrées ⇒ mêmes allocations. Tous les tris ont un départage explicite jusqu'à l'identifiant
opaque : aucun ordre ne dépend de l'ordre d'insertion en base.
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import datetime
from typing import Any

from app.db.connection import get_db
from app.moteurs.fifo_engine import TOLERANCE, calculer_fifo  # noqa: F401 — ré-export, voir §Moteur FIFO

SRC_PAIEMENT = "PAIEMENT"
SRC_REVERSEMENT = "REVERSEMENT"

ST_NON_REGLEE = "NON_REGLEE"
ST_PARTIELLE = "PARTIELLEMENT_REGLEE"
ST_REGLEE = "REGLEE"


def _round(x: float) -> float:
    return round(float(x or 0), 2)


def _maintenant() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Lecture des entrées ─────────────────────────────────────────────────────────────────────────

def _factures(conn: sqlite3.Connection, proprietaire_id: str) -> list[dict[str, Any]]:
    """Factures ÉMISES du propriétaire, de la plus ancienne à la plus récente.

    Seul `EMIS` constitue une créance : un brouillon n'engage personne, une facture annulée n'engage
    plus personne. Le tri retenu suit la règle métier — date d'émission d'abord, puis numéro, puis
    identifiant opaque en dernier recours, pour que l'ordre ne dépende jamais de la base.
    """
    rows = conn.execute(
        "SELECT facture_id_opaque, numero_facture, logement_id, mois, montant_total, "
        "       date_emission, date_facture "
        "FROM factures_proprietaires "
        "WHERE proprietaire_id = ? AND statut = 'EMIS' AND type_document = 'FACTURE'",
        (proprietaire_id,)).fetchall()
    factures = [{
        "facture_id_opaque": r[0], "numero_facture": r[1] or "", "logement_id": r[2] or "",
        "mois": r[3] or "", "montant_total": _round(r[4]),
        "date_emission": r[5] or r[6] or "", "date_facture": r[6] or "",
    } for r in rows]
    factures.sort(key=lambda f: (f["date_emission"], f["numero_facture"], f["facture_id_opaque"]))
    return factures


def _sources(conn: sqlite3.Connection, proprietaire_id: str) -> list[dict[str, Any]]:
    """Sources financières disponibles, dans l'ordre chronologique de consommation."""
    sources: list[dict[str, Any]] = []

    for r in conn.execute(
            "SELECT mouvement_opaque, date_mouvement, montant, sens, nature "
            "FROM mouvements_tresorerie_proprietaires "
            "WHERE proprietaire_id = ? AND statut = 'VALIDE' AND actif = 1",
            (proprietaire_id,)).fetchall():
        montant = _round(r[2])
        if montant <= 0:
            continue
        sources.append({
            "source_type": SRC_PAIEMENT if r[3] == "PROPRIETAIRE_VERS_SOCIETE" else SRC_REVERSEMENT,
            "source_ref": r[0], "source_date": r[1] or "", "montant": montant, "nature": r[4],
        })

    # Les avoirs ne figurent PAS ici : ils réduisent déjà la créance côté `creances_dettes_service`
    # (créance négative). Les ajouter les compterait deux fois. Voir la docstring du module.
    sources.sort(key=lambda s: (s["source_date"], s["source_type"], s["source_ref"]))
    return sources


def _empreinte(factures: list[dict], sources: list[dict]) -> str:
    h = hashlib.sha256()
    for f in factures:
        h.update(f"F|{f['facture_id_opaque']}|{f['montant_total']:.2f}|"
                 f"{f['date_emission']}|{f['numero_facture']}\n".encode())
    for s in sources:
        h.update(f"S|{s['source_type']}|{s['source_ref']}|{s['montant']:.2f}|"
                 f"{s['source_date']}\n".encode())
    return h.hexdigest()


# ── Moteur FIFO ─────────────────────────────────────────────────────────────────────────────────
#
# `calculer_fifo` et `TOLERANCE` vivent désormais dans `app.moteurs.fifo_engine` (Mission 5,
# extraction du moteur pilote) — importés ci-dessus, ré-exportés ici sans changement de
# comportement pour que ce module et ses appelants existants (`intervenant_menage_compte_service`
# pointe maintenant directement vers `app.moteurs.fifo_engine`, plus vers ce service) continuent de
# fonctionner à l'identique.


# ── Recalcul ────────────────────────────────────────────────────────────────────────────────────

def recalculer(proprietaire_id: str, *, declencheur: str = "MANUEL", db_path=None) -> dict[str, Any]:
    """Recalcule intégralement les allocations du propriétaire. Idempotent.

    Remplacement intégral dans une transaction : les allocations sont une dérivation, pas un
    historique. L'historique, lui, est dans `proprietaire_recalculs`, qui n'est jamais effacé.
    """
    conn = get_db(db_path)
    try:
        factures = _factures(conn, proprietaire_id)
        sources = _sources(conn, proprietaire_id)
        allocations = calculer_fifo(factures, sources)

        recalcul_id = "RCL-" + uuid.uuid4().hex[:12].upper()
        empreinte_entrees = _empreinte(factures, sources)
        empreinte_alloc = hashlib.sha256("\n".join(
            f"{a['source_type']}|{a['source_ref']}|{a['facture_id_opaque']}|"
            f"{a['montant_alloue']:.2f}|{a['rang_fifo']}" for a in allocations).encode()).hexdigest()

        total_factures = _round(sum(f["montant_total"] for f in factures))
        total_sources = _round(sum(s["montant"] for s in sources))
        total_alloue = _round(sum(a["montant_alloue"] for a in allocations))

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM proprietaire_allocations WHERE proprietaire_id = ?",
                         (proprietaire_id,))
            for a in allocations:
                conn.execute(
                    "INSERT INTO proprietaire_allocations (allocation_id_opaque, proprietaire_id, "
                    "recalcul_id, source_type, source_ref, source_date, facture_id_opaque, "
                    "montant_alloue, rang_fifo) VALUES (?,?,?,?,?,?,?,?,?)",
                    ("ALO-" + uuid.uuid4().hex[:12].upper(), proprietaire_id, recalcul_id,
                     a["source_type"], a["source_ref"], a["source_date"],
                     a["facture_id_opaque"], a["montant_alloue"], a["rang_fifo"]))
            conn.execute(
                "INSERT INTO proprietaire_recalculs (recalcul_id, proprietaire_id, horodatage, "
                "empreinte_entrees, empreinte_allocations, nb_factures, nb_sources, "
                "nb_allocations, montant_alloue, creance_restante, credit_restant, declencheur) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (recalcul_id, proprietaire_id, _maintenant(), empreinte_entrees, empreinte_alloc,
                 len(factures), len(sources), len(allocations), total_alloue,
                 _round(total_factures - total_alloue), _round(total_sources - total_alloue),
                 declencheur))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "recalcul_id": recalcul_id, "proprietaire_id": proprietaire_id,
            "nb_factures": len(factures), "nb_sources": len(sources),
            "nb_allocations": len(allocations), "montant_alloue": total_alloue,
            "empreinte_entrees": empreinte_entrees, "empreinte_allocations": empreinte_alloc}


# ── Position ────────────────────────────────────────────────────────────────────────────────────

def position(proprietaire_id: str, *, db_path=None) -> dict[str, Any]:
    """Position financière complète. Aucun chiffre unique ambigu : chaque composant est lisible.

    Recalcule d'abord, pour ne jamais afficher une position construite sur des allocations
    devenues fausses. C'est un calcul déterministe sur des volumes petits, pas un traitement lourd.
    """
    recalculer(proprietaire_id, declencheur="AUTO", db_path=db_path)

    conn = get_db(db_path)
    try:
        factures = _factures(conn, proprietaire_id)
        sources = _sources(conn, proprietaire_id)
        rows = conn.execute(
            "SELECT source_type, source_ref, source_date, facture_id_opaque, montant_alloue, "
            "rang_fifo FROM proprietaire_allocations WHERE proprietaire_id = ? ORDER BY rang_fifo",
            (proprietaire_id,)).fetchall()
    finally:
        conn.close()

    allocations = [{"source_type": r[0], "source_ref": r[1], "source_date": r[2],
                    "facture_id_opaque": r[3], "montant_alloue": _round(r[4]),
                    "rang_fifo": r[5]} for r in rows]

    par_facture: dict[str, float] = {}
    par_source: dict[str, float] = {}
    for a in allocations:
        par_facture[a["facture_id_opaque"]] = _round(
            par_facture.get(a["facture_id_opaque"], 0) + a["montant_alloue"])
        par_source[a["source_ref"]] = _round(par_source.get(a["source_ref"], 0) + a["montant_alloue"])

    lignes_factures = []
    for f in factures:
        regle = par_facture.get(f["facture_id_opaque"], 0.0)
        solde = _round(f["montant_total"] - regle)
        lignes_factures.append({**f, "regle": regle, "solde": solde,
                                "statut_reglement": _statut(f["montant_total"], regle)})

    lignes_sources = []
    for s in sources:
        utilise = par_source.get(s["source_ref"], 0.0)
        lignes_sources.append({**s, "utilise": utilise,
                               "disponible": _round(s["montant"] - utilise)})

    def _somme(items, cle, filtre=None):
        return _round(sum(i[cle] for i in items if filtre is None or filtre(i)))

    factures_a_recevoir = _somme(lignes_factures, "montant_total")
    paiements_recus = _somme(lignes_sources, "montant", lambda s: s["source_type"] == SRC_PAIEMENT)
    reversements_dus = _somme(lignes_sources, "montant",
                              lambda s: s["source_type"] == SRC_REVERSEMENT)
    compensations = _round(sum(a["montant_alloue"] for a in allocations
                               if a["source_type"] == SRC_REVERSEMENT))
    creance_restante = _somme(lignes_factures, "solde")
    credit_disponible = _somme(lignes_sources, "disponible",
                               lambda s: s["source_type"] != SRC_REVERSEMENT)
    # Ce qui reste réellement à virer au propriétaire après compensation (§26).
    virement_net = _round(reversements_dus - compensations)

    return {
        "proprietaire_id": proprietaire_id,
        "factures": lignes_factures,
        "sources": lignes_sources,
        "allocations": allocations,
        "factures_a_recevoir": factures_a_recevoir,
        "paiements_recus": paiements_recus,
        "reversements_dus": reversements_dus,
        "compensations": compensations,
        "creance_restante": creance_restante,
        "credit_disponible": credit_disponible,
        "virement_net": virement_net,
        # Positif : le propriétaire nous doit. Négatif : nous lui devons.
        "position_nette": _round(creance_restante - credit_disponible - virement_net),
    }


def _statut(total: float, regle: float) -> str:
    if regle <= TOLERANCE:
        return ST_NON_REGLEE
    if abs(_round(total - regle)) <= TOLERANCE:
        return ST_REGLEE
    return ST_PARTIELLE


def imputations_facture(facture_id: str, *, db_path=None) -> float:
    """Montant total imputé sur UNE facture. Point d'entrée de `creances_dettes_service`."""
    d = imputations_detail(facture_id, db_path=db_path)
    return d["total"]


def imputations_detail(facture_id: str, *, db_path=None) -> dict[str, float]:
    """{regle, compense, total} — le règlement encaissé et la compensation restent distincts.

    Les additionner dans un seul chiffre ferait disparaître une différence économique réelle :
    un règlement est de l'argent reçu, une compensation ne l'est pas.
    """
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT source_type, COALESCE(SUM(montant_alloue), 0) FROM proprietaire_allocations "
            "WHERE facture_id_opaque = ? GROUP BY source_type", (facture_id,)).fetchall()
    finally:
        conn.close()
    par_type = {r[0]: _round(r[1]) for r in rows}
    compense = par_type.get(SRC_REVERSEMENT, 0.0)
    regle = par_type.get(SRC_PAIEMENT, 0.0)
    return {"regle": regle, "compense": compense, "total": _round(regle + compense)}


def recalculer_tous(*, declencheur: str = "AUTO", db_path=None) -> int:
    """Rafraîchit les allocations de tous les propriétaires concernés. Retourne leur nombre.

    Les allocations sont dérivées : elles vieillissent dès qu'une facture est émise ou qu'un
    mouvement est validé. Un écran qui les lit doit donc les rafraîchir, sinon il afficherait un
    solde exact au moment d'un calcul passé — c'est-à-dire faux.
    """
    proprietaires = proprietaires_concernes(db_path=db_path)
    for pid in proprietaires:
        recalculer(pid, declencheur=declencheur, db_path=db_path)
    return len(proprietaires)


def proprietaires_concernes(*, db_path=None) -> list[str]:
    """Propriétaires ayant au moins une facture émise ou un mouvement validé."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT proprietaire_id FROM factures_proprietaires WHERE statut='EMIS' "
            "UNION "
            "SELECT DISTINCT proprietaire_id FROM mouvements_tresorerie_proprietaires "
            "WHERE statut='VALIDE' AND actif=1").fetchall()
        return sorted(r[0] for r in rows if r[0])
    finally:
        conn.close()


def historique_recalculs(proprietaire_id: str, limite: int = 20, *,
                         db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT recalcul_id, horodatage, empreinte_entrees, nb_factures, nb_sources, "
            "nb_allocations, montant_alloue, creance_restante, credit_restant, declencheur "
            "FROM proprietaire_recalculs WHERE proprietaire_id = ? "
            "ORDER BY horodatage DESC, id DESC LIMIT ?", (proprietaire_id, limite)).fetchall()
        return [{"recalcul_id": r[0], "horodatage": r[1], "empreinte_entrees": r[2],
                 "nb_factures": r[3], "nb_sources": r[4], "nb_allocations": r[5],
                 "montant_alloue": _round(r[6]), "creance_restante": _round(r[7]),
                 "credit_restant": _round(r[8]), "declencheur": r[9]} for r in rows]
    finally:
        conn.close()

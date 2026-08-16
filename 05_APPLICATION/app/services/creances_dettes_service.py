"""Suivi des créances propriétaires et des dettes fournisseurs, et échéancier consolidé.

Trois vues d'un même besoin : *qui me doit quoi, à qui dois-je quoi, et quand*.

Ce service **ne calcule aucun montant métier** : il agrège ce que les services existants
produisent déjà. Les créances viennent des factures propriétaires émises (source unique de la
vente), les dettes des factures fournisseurs. Les soldes sont dérivés, jamais stockés.

Vocabulaire, volontairement distinct — trois « soldes » coexistent dans le projet et les
confondre serait une faute :

- **solde de facture** : ce qu'il reste à encaisser (ou à payer) sur un document précis ;
- **solde de trésorerie propriétaire** : mouvements société ↔ propriétaire, objet séparé ;
- **net d'exploitation** : résultat économique calculé par Lot 10, qui n'est pas une créance.

Ce module ne traite que le premier.
"""
from __future__ import annotations

from datetime import date
from typing import Any

CREANCE = "CREANCE"
DETTE = "DETTE"

ST_NON_REGLEE = "NON_REGLEE"
ST_PARTIELLE = "PARTIELLEMENT_REGLEE"
ST_REGLEE = "REGLEE"
ST_TROP_PERCU = "TROP_PERCU_A_CONTROLER"


def _aujourdhui() -> str:
    return date.today().isoformat()


def _anciennete(echeance: str | None, reference: str | None = None) -> int | None:
    """Nombre de jours depuis l'échéance. Négatif si l'échéance est à venir."""
    if not echeance:
        return None
    try:
        return (date.fromisoformat(str(reference or _aujourdhui())[:10])
                - date.fromisoformat(str(echeance)[:10])).days
    except ValueError:
        return None


# ── Créances propriétaires ──────────────────────────────────────────────────────────────────────

def creances(*, proprietaire_id: str = "", logement_id: str = "", mois: str = "",
             statut: str = "", echues_seulement: bool = False,
             db_path=None) -> list[dict[str, Any]]:
    """Créances issues des factures propriétaires **émises**.

    Une facture BROUILLON ou VALIDE ne constitue pas une créance : rien n'a encore été facturé.
    Une facture ANNULE non plus. Les avoirs sont inclus, avec un montant négatif : ils réduisent
    ce que le propriétaire doit.
    """
    from app.services import factures_proprietaires_service as fpr
    from app.services import factures_proprietaires_conformite_service as conformite

    # Les allocations FIFO sont dérivées : elles doivent être à jour avant d'être lues, sinon
    # l'écran afficherait des soldes exacts au moment d'un calcul passé.
    _cpt().recalculer_tous(db_path=db_path)

    out: list[dict[str, Any]] = []
    for f in fpr.lister(mois=mois or None, proprietaire_id=proprietaire_id or None,
                        statut=fpr.ST_EMIS, db_path=db_path):
        if logement_id and f["logement_id"] != logement_id:
            continue

        imput = _imputations_detail(f["facture_id_opaque"], db_path=db_path)
        s = fpr.solde(f["facture_id_opaque"], paiements_imputes=imput["total"], db_path=db_path)
        conf = conformite.charger(f["facture_id_opaque"], db_path=db_path) or {}
        echeance = conf.get("date_echeance")
        jours = _anciennete(echeance)

        ligne = {
            "type": CREANCE,
            "facture_id_opaque": f["facture_id_opaque"],
            "numero": f["numero_facture"],
            "type_document": f["type_document"],
            "tiers_id": f["proprietaire_id"],
            "logement_id": f["logement_id"],
            "mois": f["mois"],
            "date_facture": f["date_facture"],
            "date_echeance": echeance,
            "total": s["montant_total"],
            "regle": imput["regle"],
            "compense": imput["compense"],
            "solde": s["solde"],
            "statut_reglement": s["statut_reglement"],
            "jours_retard": jours,
            "echue": bool(jours is not None and jours > 0 and abs(s["solde"]) > 0.005),
        }
        if statut and ligne["statut_reglement"] != statut:
            continue
        if echues_seulement and not ligne["echue"]:
            continue
        out.append(ligne)

    out.sort(key=lambda x: (x["date_echeance"] or "9999", x["tiers_id"]))
    return out


def _cpt():
    """Import différé : le compte propriétaire lit les factures, qui lisent ce module."""
    from app.services import compte_proprietaire_service
    return compte_proprietaire_service


def _imputations_detail(facture_id: str, *, db_path=None) -> dict[str, float]:
    """Détail de l'imputation : règlement encaissé et compensation, gardés distincts."""
    return _cpt().imputations_detail(facture_id, db_path=db_path)


def _imputations(facture_id: str, *, db_path=None) -> float:
    """Montant déjà imputé sur une facture propriétaire (règlements et compensations).

    Câblé sur le compte global propriétaire (migration 0030) : l'imputation n'est jamais choisie
    facture par facture, elle résulte de l'allocation FIFO des sources financières du propriétaire.
    Cette fonction ne fait que lire le résultat de cette allocation.
    """
    from app.services import compte_proprietaire_service as cpt
    return cpt.imputations_facture(facture_id, db_path=db_path)


# ── Dettes fournisseurs ─────────────────────────────────────────────────────────────────────────

def dettes(*, fournisseur: str = "", statut: str = "", echues_seulement: bool = False,
           db_path=None) -> list[dict[str, Any]]:
    """Dettes issues des factures fournisseurs ouvertes. Délègue le solde au service factures."""
    from app.services import factures_service as fs

    out: list[dict[str, Any]] = []
    for f in fs.lister(statut=statut, fournisseur=fournisseur,
                       echues_seulement=echues_seulement, db_path=db_path):
        if f["statut"] not in fs.STATUTS_OUVERTS:
            continue
        solde = round(float(f.get("solde_restant") or 0), 2)
        if abs(solde) <= 0.005:
            continue
        jours = _anciennete(f.get("date_echeance"))
        out.append({
            "type": DETTE,
            "facture_id_opaque": f["facture_id_opaque"],
            "numero": f.get("facture_ref"),
            "tiers_id": f["fournisseur_id_opaque"],
            "date_facture": f.get("date_facture"),
            "date_echeance": f.get("date_echeance"),
            "total": round(float(f.get("montant_ttc") or 0), 2),
            "regle": round(float(f.get("montant_regle") or 0), 2),
            "solde": solde,
            "statut": f["statut"],
            "jours_retard": jours,
            "echue": bool(f.get("echue")),
        })
    out.sort(key=lambda x: (x["date_echeance"] or "9999", x["tiers_id"]))
    return out


# ── Synthèse ────────────────────────────────────────────────────────────────────────────────────

def synthese(*, db_path=None) -> dict[str, Any]:
    """Vue d'ensemble : ce qu'on attend, ce qu'on doit, et la part déjà échue."""
    c = creances(db_path=db_path)
    d = dettes(db_path=db_path)

    def _tot(lignes, filtre=None):
        return round(sum(l["solde"] for l in lignes if filtre is None or filtre(l)), 2)

    total_c, total_d = _tot(c), _tot(d)
    return {
        "creances": {
            "nombre": len(c), "total": total_c,
            "echu": _tot(c, lambda l: l["echue"]),
            "a_venir": _tot(c, lambda l: not l["echue"]),
        },
        "dettes": {
            "nombre": len(d), "total": total_d,
            "echu": _tot(d, lambda l: l["echue"]),
            "a_venir": _tot(d, lambda l: not l["echue"]),
        },
        # Position nette : ce qui reste à encaisser moins ce qui reste à payer. Ce n'est pas une
        # trésorerie disponible — seulement un solde de créances et dettes ouvertes.
        "position_nette": round(total_c - total_d, 2),
    }


# ── Échéancier ──────────────────────────────────────────────────────────────────────────────────

TRANCHES = (
    ("ECHU", "Échu", None, 0),
    ("J7", "Sous 7 jours", 0, 7),
    ("J30", "8 à 30 jours", 7, 30),
    ("PLUS_TARD", "Au-delà de 30 jours", 30, None),
    ("SANS_ECHEANCE", "Sans échéance", None, None),
)


def _tranche(ligne: dict[str, Any]) -> str:
    """Une ligne échue reste échue quelle que soit son ancienneté ; une ligne sans échéance est
    isolée plutôt que rangée arbitrairement dans une tranche à venir."""
    jours = ligne.get("jours_retard")
    if jours is None:
        return "SANS_ECHEANCE"
    if jours > 0:
        return "ECHU"
    restant = -jours
    if restant <= 7:
        return "J7"
    if restant <= 30:
        return "J30"
    return "PLUS_TARD"


def echeancier(*, db_path=None) -> dict[str, Any]:
    """Créances à encaisser et dettes à payer, ventilées par échéance."""
    resultat: dict[str, Any] = {"tranches": [], "creances": [], "dettes": []}
    resultat["creances"] = creances(db_path=db_path)
    resultat["dettes"] = dettes(db_path=db_path)

    for cle, libelle, _, _ in TRANCHES:
        lc = [l for l in resultat["creances"] if _tranche(l) == cle]
        ld = [l for l in resultat["dettes"] if _tranche(l) == cle]
        resultat["tranches"].append({
            "cle": cle, "libelle": libelle,
            "creances_nb": len(lc), "creances_total": round(sum(l["solde"] for l in lc), 2),
            "dettes_nb": len(ld), "dettes_total": round(sum(l["solde"] for l in ld), 2),
            "creances": lc, "dettes": ld,
        })
    resultat["synthese"] = synthese(db_path=db_path)
    return resultat


# ── Agrégation par tiers ────────────────────────────────────────────────────────────────────────

def par_tiers(*, db_path=None) -> dict[str, list[dict[str, Any]]]:
    """Créances par propriétaire et dettes par fournisseur — la question « combien me doit X ? »."""
    def _agreger(lignes):
        par: dict[str, dict[str, Any]] = {}
        for l in lignes:
            e = par.setdefault(l["tiers_id"], {"tiers_id": l["tiers_id"], "nombre": 0,
                                               "total": 0.0, "echu": 0.0})
            e["nombre"] += 1
            e["total"] = round(e["total"] + l["solde"], 2)
            if l["echue"]:
                e["echu"] = round(e["echu"] + l["solde"], 2)
        return sorted(par.values(), key=lambda x: -abs(x["total"]))

    return {"proprietaires": _agreger(creances(db_path=db_path)),
            "fournisseurs": _agreger(dettes(db_path=db_path))}

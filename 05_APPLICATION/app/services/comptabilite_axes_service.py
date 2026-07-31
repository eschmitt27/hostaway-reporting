"""Axes analytiques (Bloc 3, mission Analytique/Résultats — fermeture) : fournisseur, catégorie,
prestataire. Chaque axe est construit UNIQUEMENT sur une source déjà réelle et complète — jamais un
troisième calcul, jamais une dimension inventée.

Axes NON_DISPONIBLE, assumés (cf. `53_AXES_ANALYTIQUES_ETAT_FINAL.md`) :
- **plateforme** : un champ canal/plateforme n'existe que côté réservations « hors Hostaway »
  (saisie manuelle) ; les réservations Hostaway (l'écrasante majorité) n'exposent aucun canal
  résolu au niveau applicatif. Une agrégation ne porterait donc que sur une fraction non
  représentative du portefeuille — pas une source fiable au grain demandé (tout le portefeuille).
- **activité** : `type_flux_id` existe (Lot9/Lot10) mais c'est une classification technique fine
  (une vingtaine de codes, ex. `TYPE_FLUX_013`, `TYPE_FLUX_017`…), jamais organisée en une
  taxonomie « hébergement/ménage/frais/services/charges générales » dans les décisions métier.
  En construire une ici serait inventer un regroupement arbitraire — explicitement interdit.

Fournisseur ≠ Prestataire, même tiers possible : le même `fournisseur_id_opaque` porte une vue
comptable (dette, factures, règlements — `comptabilite_auxiliaires_service`) et une vue
opérationnelle ménage (ménages affectés, coût prévu/réel — ici), jamais confondues.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

NON_DISPONIBLE = "NON_DISPONIBLE"


# ── Catégorie (source : MASTER Lot3, lecture seule) ──────────────────────────

def categories() -> dict[str, Any]:
    from app.readers import charges_reader
    try:
        charges = charges_reader.read_charges()
    except Exception:
        charges = []
    if not charges:
        return {"statut": NON_DISPONIBLE, "lignes": []}
    par_categorie: dict[str, dict[str, Any]] = {}
    for c in charges:
        cat = str(c.get("categorie_charge_id") or "A_CONTROLER")
        d = par_categorie.setdefault(cat, {"categorie": cat, "montant": 0.0, "nb": 0})
        try:
            d["montant"] += float(c.get("montant") or 0)
        except (TypeError, ValueError):
            pass
        d["nb"] += 1
    return {"statut": "OK", "lignes": sorted(par_categorie.values(), key=lambda d: -d["montant"])}


def categorie_detail(categorie: str) -> dict[str, Any]:
    from app.readers import charges_reader
    try:
        charges = charges_reader.read_charges()
    except Exception:
        charges = []
    lignes = [c for c in charges if str(c.get("categorie_charge_id") or "A_CONTROLER") == categorie]
    if not lignes:
        return {"statut": NON_DISPONIBLE, "lignes": [], "montant_total": None}
    montant_total = 0.0
    for c in lignes:
        try:
            montant_total += float(c.get("montant") or 0)
        except (TypeError, ValueError):
            pass
    return {"statut": "OK", "lignes": lignes, "montant_total": round(montant_total, 2)}


# ── Fournisseur (source : Comptabilité, déjà livrée — `49`) ──────────────────

def fournisseurs(*, db_path=None) -> dict[str, Any]:
    from app.services import comptabilite_auxiliaires_service as aux
    lignes = aux.synthese(db_path=db_path).get(aux.FAMILLE_FOURNISSEUR, [])
    return {"statut": "OK" if lignes else NON_DISPONIBLE, "lignes": lignes}


def fournisseur_detail(fournisseur_id_opaque: str, *, db_path=None) -> dict[str, Any]:
    from app.services import comptabilite_auxiliaires_service as aux
    fiche = aux.fiche_auxiliaire(aux.FAMILLE_FOURNISSEUR, fournisseur_id_opaque, db_path=db_path)
    return {"statut": "OK" if fiche["mouvements"] else NON_DISPONIBLE, "fiche": fiche}


# ── Prestataire (source : module Ménages, `menages.fournisseur_id_opaque` — le même
#    référentiel Fournisseurs, une vue opérationnelle différente, jamais confondue) ──

def prestataires(*, mois: str = "", db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        clause = "AND mois=?" if mois else ""
        params: tuple = (mois,) if mois else ()
        rows = conn.execute(
            f"SELECT fournisseur_id_opaque, cout_prevu, cout_reel, logement_id, statut "
            f"FROM menages WHERE fournisseur_id_opaque IS NOT NULL AND statut <> 'ANNULE' {clause}",
            params).fetchall()
    finally:
        conn.close()
    if not rows:
        return {"statut": NON_DISPONIBLE, "lignes": []}
    par_prestataire: dict[str, dict[str, Any]] = {}
    for r in rows:
        pid = r["fournisseur_id_opaque"]
        d = par_prestataire.setdefault(pid, {
            "prestataire_id": pid, "nb_menages": 0, "cout_prevu": 0.0, "cout_reel": 0.0,
            "logements": set(),
        })
        d["nb_menages"] += 1
        d["cout_prevu"] += r["cout_prevu"] or 0
        d["cout_reel"] += r["cout_reel"] or 0
        if r["logement_id"]:
            d["logements"].add(r["logement_id"])
    lignes = []
    for d in par_prestataire.values():
        lignes.append({
            "prestataire_id": d["prestataire_id"], "nb_menages": d["nb_menages"],
            "cout_prevu": round(d["cout_prevu"], 2), "cout_reel": round(d["cout_reel"], 2),
            "ecart": round(d["cout_reel"] - d["cout_prevu"], 2),
            "nb_logements": len(d["logements"]),
        })
    lignes.sort(key=lambda d: -d["cout_reel"])
    return {"statut": "OK", "lignes": lignes}


def prestataire_detail(fournisseur_id_opaque: str, *, mois: str = "", db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        clause = "AND mois=?" if mois else ""
        params: tuple = (fournisseur_id_opaque, mois) if mois else (fournisseur_id_opaque,)
        rows = conn.execute(
            f"SELECT menage_id_opaque, mois, logement_id, type_menage, cout_prevu, cout_reel, "
            f"statut, charge_id, facture_id_opaque FROM menages WHERE fournisseur_id_opaque=? "
            f"AND statut <> 'ANNULE' {clause} ORDER BY mois DESC", params).fetchall()
    finally:
        conn.close()
    lignes = [dict(r) for r in rows]
    if not lignes:
        return {"statut": NON_DISPONIBLE, "lignes": []}
    return {"statut": "OK", "lignes": lignes,
            "cout_prevu_total": round(sum(l["cout_prevu"] or 0 for l in lignes), 2),
            "cout_reel_total": round(sum(l["cout_reel"] or 0 for l in lignes), 2)}


# ── Plateforme / Activité — NON_DISPONIBLE assumé ─────────────────────────────

def plateformes() -> dict[str, Any]:
    return {"statut": NON_DISPONIBLE,
            "raison": "Aucun canal/plateforme résolu au niveau applicatif pour les réservations "
                      "Hostaway (l'écrasante majorité du portefeuille) — seules les réservations "
                      "hors Hostaway (saisie manuelle) portent un canal_id. Une agrégation ne "
                      "porterait que sur une fraction non représentative : pas une source fiable."}


def activites() -> dict[str, Any]:
    return {"statut": NON_DISPONIBLE,
            "raison": "`type_flux_id` est une classification technique fine (~20 codes), jamais "
                      "organisée en activités (hébergement/ménage/frais/services/charges "
                      "générales) dans les décisions métier. Construire ce regroupement ici "
                      "serait inventer une taxonomie arbitraire — non fait."}

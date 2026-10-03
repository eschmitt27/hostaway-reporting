"""Créances & Dettes — position par propriétaire, puis « Préparer le règlement » → « Régler ».

AUCUN CIRCUIT PARALLÈLE
« Régler » n'enregistre rien par lui-même : il appelle le rapprochement canonique de Flux
financiers (`flux_lettrage_service.valider`) sur le mouvement bancaire (ou de caisse) reçu et les
factures ouvertes du propriétaire. C'est ce mécanisme, et lui seul, qui :
    · crée l'encaissement propriétaire (mouvement de trésorerie VALIDE) ;
    · passe l'écriture 512 / 411 (tiers = propriétaire) ;
    · recalcule et persiste les allocations FIFO du compte ;
    · marque le mouvement bancaire rapproché.
Créances & Dettes, le Compte propriétaire, les factures et l'échéancier lisent ensuite ces mêmes
données : aucun d'eux n'a de copie à mettre à jour, il n'y a donc aucune double saisie possible.

L'UTILISATEUR NE CHOISIT PAS LA FACTURE SOLDÉE
L'argent reçu solde les factures de la plus ancienne à la plus récente (FIFO du compte). Les
factures présentées au rapprochement sont donc les factures ouvertes DANS CET ORDRE, jusqu'à
couvrir le montant reçu.
"""
from __future__ import annotations

import unicodedata
from typing import Any

EPS = 0.005


def _r(v: Any) -> float:
    return round(float(v or 0), 2)


def _cpt():
    from app.services import compte_proprietaire_service
    return compte_proprietaire_service


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return " ".join(s.upper().split())


# ── Position par propriétaire (vue « Créances propriétaires ») ─────────────────────────────────

def _credits_airbnb_disponibles(pid: str, *, db_path=None) -> float:
    """Reversements Airbnb dont l'origine est constatée, pas encore imputés (Mission 37)."""
    try:
        from app.services import credits_clients_service as credits
        return _r(sum(c.get("reste", 0) for c in credits.lister(proprietaire_id=pid, db_path=db_path)
                      if c.get("statut") == credits.ST_DISPONIBLE))
    except Exception:  # noqa: BLE001 — table absente d'une base ancienne : rien de disponible
        return 0.0


def positions(*, inclure_soldes: bool = False, db_path=None) -> list[dict[str, Any]]:
    """Une ligne par propriétaire : facturé, réglé, crédits appliqués, restant dû, statut.

    Les montants sont ceux des créances (factures émises, avoirs compris) : la même source que la
    liste des factures juste en dessous, et que le Compte propriétaire (test de concordance)."""
    from app.services import creances_dettes_service as svc

    lignes = svc.creances(db_path=db_path)
    par: dict[str, list[dict]] = {}
    for l in lignes:
        par.setdefault(l["tiers_id"], []).append(l)
    ids = set(par) | set(_cpt().proprietaires_concernes(db_path=db_path))

    out = []
    for pid in ids:
        mine = par.get(pid, [])
        pos = _cpt().position(pid, db_path=db_path)
        restant = _r(sum(l["solde"] for l in mine))
        # Crédit disponible : UNE source, la position du compte propriétaire (paiements et avoirs
        # non consommés + crédits clients, dont reprise de solde et reversements Airbnb).
        credit = _r(pos["credit_disponible"])
        # À reverser = trop-perçu sur facture + ce qui reste à virer (déjà viré déduit).
        a_virer = _r(pos.get("reste_a_virer", max(pos["virement_net"], 0)))
        a_reverser = _r(sum(l["montant_a_reverser"] for l in mine) + a_virer)
        ouvertes = [l for l in mine if l["solde"] > EPS]
        retard = max([l["jours_retard"] for l in ouvertes if l["jours_retard"] is not None
                      and l["jours_retard"] > 0] or [0])
        echeances = sorted(l["date_echeance"] for l in ouvertes if l["date_echeance"])
        plus_ancienne = min((l["date_facture"] for l in ouvertes if l["date_facture"]), default="")
        if restant > EPS:
            statut, badge = (("En retard", "en-retard") if retard
                             else ("À régler", "a-regler"))
        elif a_reverser > EPS or restant < -EPS:
            statut, badge = "À reverser au propriétaire", "a-reverser"
        elif credit > EPS:
            statut, badge = "Crédit disponible", "partielle"
        else:
            statut, badge = "Soldé", "soldee"
        ligne = {
            "proprietaire_id": pid,
            "facture": _r(sum(l["total"] for l in mine)),
            "regle": _r(sum(l["regle"] for l in mine)),
            "credits_appliques": _r(sum(l["compense"] for l in mine)),
            "acomptes": _r(sum(l.get("acomptes", 0) for l in mine)),
            "restant_du": restant,
            "credit_disponible": credit,
            "a_reverser": a_reverser,
            "reste_a_virer": a_virer,
            "nb_factures_ouvertes": len(ouvertes),
            "jours_retard": retard,
            "prochaine_echeance": echeances[0] if echeances else "",
            "plus_ancienne": plus_ancienne,
            "statut": statut,
            "badge": badge,
            "a_traiter": restant > EPS or a_reverser > EPS or credit > EPS,
        }
        if inclure_soldes or ligne["a_traiter"]:
            out.append(ligne)
    out.sort(key=lambda x: (-x["restant_du"], x["proprietaire_id"]))
    return out


# ── Préparer le règlement ──────────────────────────────────────────────────────────────────────

def _factures_ouvertes(pid: str, *, db_path=None) -> list[dict[str, Any]]:
    """Factures émises non soldées, dans l'ordre FIFO du compte (émission, numéro, identifiant)."""
    pos = _cpt().position(pid, db_path=db_path)
    return [f for f in pos["factures"] if f["solde"] > EPS]


def mouvements_candidats(pid: str, *, montant_cible: float = 0.0, limite: int = 25,
                         sens: str = "", db_path=None) -> list[dict[str, Any]]:
    """Mouvements (banque ou caisse) encore à rapprocher, dans le sens voulu : ENTREE pour un
    encaissement (défaut), SORTIE pour un versement au propriétaire. Ceux qui portent le nom du
    propriétaire d'abord, puis les montants les plus proches du montant attendu."""
    from app.db.connection import get_db
    from app.services import flux_financiers_service as flux

    nom = ""
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT nom_proprietaire FROM ref_proprietaires WHERE proprietaire_id = ?",
                         (pid,)).fetchone()
        nom = _norm(r[0]) if r and r[0] else ""
    except Exception:  # noqa: BLE001 — sans référentiel, l'ordre par montant suffit
        nom = ""
    finally:
        conn.close()
    out = []
    for m in flux.mouvements(avec_propositions=False, db_path=db_path):
        if m["sens"] != (sens or flux.ENTREE) or not m["lettrable"] or m["restant"] <= EPS:
            continue
        porte_nom = bool(nom) and nom in _norm(m.get("texte_recherche") or m.get("libelle"))
        out.append({**m, "cle": f"{m['source']}:{m['id']}", "porte_nom": porte_nom,
                    "ecart_cible": abs(_r(m["restant"]) - _r(montant_cible))})
    out.sort(key=lambda m: (not m["porte_nom"], m["ecart_cible"], m["date"] or ""),)
    return out[:limite]


def preparer(pid: str, *, db_path=None) -> dict[str, Any]:
    """Ce qui compose le montant à régler — lecture seule."""
    pos = _cpt().position(pid, db_path=db_path)
    ouvertes = [f for f in pos["factures"] if f["solde"] > EPS]
    montant = _r(sum(f["solde"] for f in ouvertes))
    sources = [s for s in pos["sources"] if s["disponible"] > EPS]
    try:
        from app.services import credits_clients_service as credits
        airbnb = [c for c in credits.lister(proprietaire_id=pid, db_path=db_path)
                  if c.get("statut") == credits.ST_DISPONIBLE and c.get("reste", 0) > EPS]
    except Exception:  # noqa: BLE001
        airbnb = []
    return {
        "proprietaire_id": pid,
        "position": pos,
        "factures_ouvertes": ouvertes,
        "montant_propose": montant,
        "sources_disponibles": sources,
        "credits_airbnb": airbnb,
        "credits_airbnb_total": _r(sum(c.get("reste", 0) for c in airbnb)),
        "a_reverser": _r(max(pos["virement_net"], 0) + sum(-f["solde"] for f in pos["factures"]
                                                          if f["solde"] < -EPS)),
        "mouvements": mouvements_candidats(pid, montant_cible=montant, db_path=db_path)
                      if montant > EPS else [],
    }


# ── Régler (via le rapprochement canonique) ────────────────────────────────────────────────────

def objets_pour(pid: str, montant_recu: float, *, db_path=None) -> list[str]:
    """Factures ouvertes en ordre FIFO, jusqu'à couvrir le montant reçu (au moins une)."""
    from app.services import flux_financiers_service as flux
    out, cumul = [], 0.0
    for f in _factures_ouvertes(pid, db_path=db_path):
        out.append(f"{flux.FACTURE_PROPRIETAIRE}:{f['facture_id_opaque']}")
        cumul = _r(cumul + f["solde"])
        if cumul >= montant_recu - EPS:
            break
    return out


def _mouvement(cle: str, *, db_path=None) -> dict[str, Any] | None:
    from app.services import flux_financiers_service as flux
    source, _, ident = cle.partition(":")
    return next((m for m in flux.mouvements(avec_propositions=False, db_path=db_path)
                 if m["source"] == source and m["id"] == ident), None)


def apercu(pid: str, mouvement_cle: str, *, traitement_ecart: str = "", db_path=None) -> dict:
    """Aperçu du rapprochement — `flux_lettrage_service.preparer`, qui n'écrit rien."""
    from app.services import flux_lettrage_service as lettrage
    m = _mouvement(mouvement_cle, db_path=db_path)
    if m is None:
        return {"ok": False, "erreurs": [{"code": "R01", "message": "Mouvement introuvable."}]}
    objets = objets_pour(pid, m["restant"], db_path=db_path)
    if not objets:
        return {"ok": False, "erreurs": [{"code": "R02",
                                           "message": "Aucune facture ouverte à régler."}]}
    prep = lettrage.preparer([mouvement_cle], objets, traitement_ecart=traitement_ecart,
                             db_path=db_path)
    prep["objets_cles"] = objets
    prep["mouvement"] = m
    return prep


def regler(pid: str, mouvement_cle: str, *, acteur: str, traitement_ecart: str = "",
           compte_ecart: str = "", db_path=None) -> dict[str, Any]:
    """Enregistre le règlement : rapprochement + encaissement + écriture + FIFO, tout ou rien."""
    from app.services import flux_lettrage_service as lettrage
    m = _mouvement(mouvement_cle, db_path=db_path)
    if m is None:
        return {"ok": False, "message": "Mouvement introuvable."}
    objets = objets_pour(pid, m["restant"], db_path=db_path)
    if not objets:
        return {"ok": False, "message": "Aucune facture ouverte à régler."}
    return lettrage.valider([mouvement_cle], objets, acteur=acteur,
                            traitement_ecart=traitement_ecart, compte_ecart=compte_ecart,
                            justification=f"Règlement préparé depuis Créances & Dettes ({pid})",
                            db_path=db_path)


# ── Préparer le versement propriétaire → Régler (sens société → propriétaire) ────────────────────
#
# Même principe que l'encaissement, en sens inverse : le versement est le rapprochement canonique
# de Flux entre le débit bancaire et le(s) reversement(s) validé(s) du propriétaire (mouvement de
# trésorerie « société → propriétaire »). Écriture 411 / 512, rapprochement, compte mis à jour
# (« déjà viré » / « reste à virer ») : aucun mécanisme nouveau.

def reversements_ouverts(pid: str, *, db_path=None) -> list[dict[str, Any]]:
    """Reversements validés du propriétaire, pas encore entièrement virés — du plus ancien au
    plus récent."""
    from app.services import flux_financiers_service as flux
    return sorted((o for o in flux.objets(db_path=db_path)
                   if o["type"] == flux.MOUVEMENT_PROPRIETAIRE and o["sens"] == flux.SORTIE
                   and o["tiers_id"] == pid and o["reste"] > EPS),
                  key=lambda o: (o["date"] or "", o["id"]))


def preparer_versement(pid: str, *, db_path=None) -> dict[str, Any]:
    """Ce qui compose le versement — lecture seule."""
    from app.services import flux_financiers_service as flux
    pos = _cpt().position(pid, db_path=db_path)
    ouverts = reversements_ouverts(pid, db_path=db_path)
    montant = _r(min(pos["reste_a_virer"], sum(o["reste"] for o in ouverts)))
    trop_percu = [f for f in pos["factures"] if f["solde"] < -EPS]
    return {
        "proprietaire_id": pid,
        "position": pos,
        "reversements": ouverts,
        "montant_propose": montant,
        "trop_percu": trop_percu,
        "trop_percu_total": _r(sum(-f["solde"] for f in trop_percu)),
        "mouvements": mouvements_candidats(pid, montant_cible=montant, sens=flux.SORTIE,
                                           db_path=db_path) if montant > EPS else [],
    }


def objets_versement(pid: str, montant_verse: float, *, db_path=None) -> list[str]:
    """Reversements ouverts, du plus ancien, jusqu'à couvrir le montant versé (au moins un)."""
    from app.services import flux_financiers_service as flux
    out, cumul = [], 0.0
    for o in reversements_ouverts(pid, db_path=db_path):
        out.append(f"{flux.MOUVEMENT_PROPRIETAIRE}:{o['id']}")
        cumul = _r(cumul + o["reste"])
        if cumul >= montant_verse - EPS:
            break
    return out


def apercu_versement(pid: str, mouvement_cle: str, *, traitement_ecart: str = "",
                     db_path=None) -> dict:
    """Aperçu — `flux_lettrage_service.preparer`, qui n'écrit rien."""
    from app.services import flux_lettrage_service as lettrage
    m = _mouvement(mouvement_cle, db_path=db_path)
    if m is None:
        return {"ok": False, "erreurs": [{"code": "V01", "message": "Mouvement introuvable."}]}
    objets = objets_versement(pid, m["restant"], db_path=db_path)
    if not objets:
        return {"ok": False, "erreurs": [{"code": "V02",
                                           "message": "Aucun reversement à virer."}]}
    prep = lettrage.preparer([mouvement_cle], objets, traitement_ecart=traitement_ecart,
                             db_path=db_path)
    prep["objets_cles"] = objets
    prep["mouvement"] = m
    return prep


def verser(pid: str, mouvement_cle: str, *, acteur: str, traitement_ecart: str = "",
           db_path=None) -> dict[str, Any]:
    """Enregistre le versement : rapprochement + écriture 411 / 512, tout ou rien."""
    from app.services import flux_lettrage_service as lettrage
    m = _mouvement(mouvement_cle, db_path=db_path)
    if m is None:
        return {"ok": False, "message": "Mouvement introuvable."}
    objets = objets_versement(pid, m["restant"], db_path=db_path)
    if not objets:
        return {"ok": False, "message": "Aucun reversement à virer."}
    return lettrage.valider([mouvement_cle], objets, acteur=acteur,
                            traitement_ecart=traitement_ecart,
                            justification=f"Versement préparé depuis Créances & Dettes ({pid})",
                            db_path=db_path)

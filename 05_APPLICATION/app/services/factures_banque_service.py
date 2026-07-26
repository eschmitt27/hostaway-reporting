"""Pont Factures/Règlements ↔ Banque.

**Aucun second moteur de rapprochement.** Ce service ne fait qu'orchestrer :
- `banques_rapprochement_service` reste LA source de vérité du lien (table `banque_rapprochements`,
  type d'objet `REGLEMENT_CHARGE`, déjà prévu dans son énumération) ;
- `reglements_fournisseurs_service.marquer_rapproche()` ne fait que refléter l'état pour l'affichage ;
- `banques_suggestions_service` fournit le score explicable, jamais recodé ici.

Le rapprochement ne crée JAMAIS de charge : il relie seulement
mouvement bancaire ↔ règlement ↔ facture.
"""
from __future__ import annotations

from typing import Any

import app.config as cfg
from app.readers import banques_reader as reader
from app.readers.banques_reader import to_texte, to_nombre, to_date
from app.services import banques_controle_service as ctrl
from app.services import banques_rapprochement_service as rappro
from app.services import banques_suggestions_service as sugg
from app.services import factures_service as fact
from app.services import reglements_fournisseurs_service as regl

TYPE_OBJET = "REGLEMENT_CHARGE"

# Un règlement fournisseur sort de la trésorerie : seul un DÉBIT peut le porter.
SENS_ATTENDU = "DEBIT"

E_REGLEMENT_INTROUVABLE = "V01_REGLEMENT_INTROUVABLE"
E_MOUVEMENT_INTROUVABLE = "V02_MOUVEMENT_INTROUVABLE"
E_REGLEMENT_ANNULE = "V03_REGLEMENT_ANNULE"
E_SENS_INCOHERENT = "V04_SENS_INCOHERENT"
E_DEVISE_INCOHERENTE = "V05_DEVISE_INCOHERENTE"
E_MOUVEMENT_NON_VALIDE = "V06_MOUVEMENT_NON_VALIDE"
E_DEPASSE_REGLEMENT = "V07_MONTANT_SUPERIEUR_AU_REGLEMENT"

MESSAGES = {
    E_REGLEMENT_INTROUVABLE: "Règlement introuvable.",
    E_MOUVEMENT_INTROUVABLE: "Mouvement bancaire introuvable.",
    E_REGLEMENT_ANNULE: "Ce règlement est annulé : il ne peut plus être rapproché.",
    E_SENS_INCOHERENT: "Un règlement fournisseur ne peut être porté que par un débit bancaire.",
    E_DEVISE_INCOHERENTE: "La devise du mouvement diffère de l'euro attendu.",
    E_MOUVEMENT_NON_VALIDE: "Ce mouvement est rejeté ou bloquant : il ne peut pas être rapproché.",
    E_DEPASSE_REGLEMENT: "Le montant rapproché dépasserait le montant du règlement.",
}

STATUTS_MOUVEMENT_REFUSES = ("BLOQUANT", "REJETE")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _mouvement(opaque: str) -> dict[str, Any] | None:
    """Vue minimale d'un mouvement par identifiant opaque (jamais le mouvement_id brut)."""
    mid = ctrl.resoudre_opaque(opaque)
    if mid is None:
        return None
    for r in reader.mouvements().lignes:
        if to_texte(r.get("mouvement_id")) == mid:
            return {
                "id_opaque": opaque,
                "montant": to_nombre(r.get("montant")) or 0,
                "date_operation": to_date(r.get("date_operation")),
                "libelle": to_texte(r.get("libelle")),
                "sens": to_texte(r.get("sens")).upper(),
                "devise": to_texte(r.get("devise")).upper() or "EUR",
                "statut_controle": to_texte(r.get("statut_controle")),
                "compte_masque": reader.masquer_compte(r.get("compte_id")),
            }
    return None


def montant_rapproche_du_reglement(reglement_opaque: str, db_path=None) -> float:
    """Somme des liens ACTIFS portant ce règlement, tous mouvements confondus."""
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT montant_rapproche FROM banque_rapprochements "
            "WHERE type_objet=? AND objet_id=? AND statut IN (?,?)",
            (TYPE_OBJET, reglement_opaque, rappro.ST_PROPOSE, rappro.ST_CONFIRME)).fetchall()
    finally:
        conn.close()
    return round(sum(r["montant_rapproche"] for r in rows), 2)


def etat_reglement(reglement_opaque: str, db_path=None) -> dict[str, Any]:
    """NON_RAPPROCHE | PARTIEL | RAPPROCHE, du point de vue du règlement."""
    r = regl.charger(reglement_opaque, db_path)
    if r is None:
        return {"statut": "INTROUVABLE", "montant_rapproche": 0.0, "montant_restant": 0.0}
    deja = montant_rapproche_du_reglement(reglement_opaque, db_path)
    total = abs(r["montant"] or 0)
    if deja <= 1e-9:
        statut = "NON_RAPPROCHE"
    elif deja >= total - 1e-9:
        statut = "RAPPROCHE"
    else:
        statut = "PARTIEL"
    return {"statut": statut, "montant_rapproche": deja,
            "montant_restant": round(total - deja, 2), "montant_reglement": total}


def liens_du_reglement(reglement_opaque: str, db_path=None) -> list[dict[str, Any]]:
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE type_objet=? AND objet_id=? ORDER BY id DESC",
            (TYPE_OBJET, reglement_opaque)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def candidats_pour_reglement(reglement_opaque: str, db_path=None) -> list[dict[str, Any]]:
    """Mouvements bancaires réellement rapprochables avec ce règlement, avec score explicable
    calculé par le moteur Banque (jamais un scoring recodé ici)."""
    r = regl.charger(reglement_opaque, db_path)
    if r is None:
        return []
    etat = etat_reglement(reglement_opaque, db_path)
    restant = etat["montant_restant"]
    if restant <= 1e-9:
        return []

    # L'objet « candidat » vu par le moteur, c'est le RÈGLEMENT ; on évalue chaque mouvement
    # contre lui en réutilisant `evaluer()`, sans dupliquer sa logique de score.
    objet = {"type_objet": TYPE_OBJET, "objet_id": reglement_opaque,
             "montant": restant, "date": r["date_reglement"],
             "reference": r.get("compte") or "", "libelle": f"Règlement {r['date_reglement']}"}

    out: list[dict[str, Any]] = []
    for row in reader.mouvements().lignes:
        mid = to_texte(row.get("mouvement_id"))
        if not mid:
            continue
        sens = to_texte(row.get("sens")).upper()
        if sens != SENS_ATTENDU:
            continue                     # jamais proposer un crédit pour un paiement fournisseur
        if to_texte(row.get("statut_controle")) in STATUTS_MOUVEMENT_REFUSES:
            continue                     # jamais proposer un mouvement rejeté/bloquant
        opaque = ctrl.id_opaque(mid)
        dispo = rappro.etat_rapprochement(opaque, to_nombre(row.get("montant")) or 0,
                                          db_path=db_path)["montant_restant"]
        if dispo <= 1e-9:
            continue                     # mouvement déjà entièrement rapproché
        mvt = {"id_opaque": opaque, "montant": to_nombre(row.get("montant")) or 0,
               "date_operation": to_date(row.get("date_operation")),
               "libelle": to_texte(row.get("libelle")), "sens": sens}
        s = sugg.evaluer(mvt, objet, montant_disponible=dispo)
        if s["score"] <= 0:
            continue
        out.append({
            "mouvement_id_opaque": opaque,
            "date_operation": mvt["date_operation"],
            "libelle": mvt["libelle"],
            "montant_mouvement": mvt["montant"],
            "compte_masque": reader.masquer_compte(row.get("compte_id")),
            "montant_disponible": dispo,
            "montant_propose": round(min(restant, dispo), 2),
            "score": s["score"], "niveau": s["niveau"], "raison": s["raison"],
            "criteres_divergents": s["criteres_divergents"],
        })
    out.sort(key=lambda c: (-c["score"], c["date_operation"]))
    return out


def rapprocher(reglement_opaque: str, mouvement_opaque: str, montant: float, *,
               acteur: str = "", commentaire: str = "", db_path=None) -> dict[str, Any]:
    """Crée le lien mouvement ↔ règlement via le service Banque (source de vérité unique).

    Contrôles propres à ce pont (le service Banque gère déjà dépassement du MOUVEMENT, double
    rapprochement, montant invalide) : existence, règlement annulé, sens, devise, statut moteur,
    et dépassement du montant du RÈGLEMENT.
    """
    r = regl.charger(reglement_opaque, db_path)
    if r is None:
        return _refus(E_REGLEMENT_INTROUVABLE, reglement_opaque)
    if r["statut"] == regl.ST_ANNULE:
        return _refus(E_REGLEMENT_ANNULE, reglement_opaque)

    mvt = _mouvement(mouvement_opaque)
    if mvt is None:
        return _refus(E_MOUVEMENT_INTROUVABLE, mouvement_opaque)
    if mvt["sens"] != SENS_ATTENDU:
        return _refus(E_SENS_INCOHERENT, mvt["sens"])
    if mvt["devise"] != "EUR":
        return _refus(E_DEVISE_INCOHERENTE, mvt["devise"])
    if mvt["statut_controle"] in STATUTS_MOUVEMENT_REFUSES:
        return _refus(E_MOUVEMENT_NON_VALIDE, mvt["statut_controle"])

    try:
        montant_f = float(montant)
    except (TypeError, ValueError):
        return _refus(E_DEPASSE_REGLEMENT, str(montant))
    etat = etat_reglement(reglement_opaque, db_path)
    if montant_f > etat["montant_restant"] + 1e-9:
        return _refus(E_DEPASSE_REGLEMENT,
                      f"{montant_f:.2f} € > restant {etat['montant_restant']:.2f} €")

    # Source de vérité : le service Banque. Il refuse lui-même tout dépassement du MOUVEMENT.
    res = rappro.enregistrer(
        mouvement_opaque, TYPE_OBJET, reglement_opaque, montant_f,
        montant_mouvement=mvt["montant"], statut=rappro.ST_PROPOSE, source="MANUEL",
        commentaire=commentaire, acteur=acteur, db_path=db_path)
    if not res.get("ok"):
        return res

    _refleter_statut_reglement(reglement_opaque, mouvement_opaque, db_path)
    return {**res, "etat_reglement": etat_reglement(reglement_opaque, db_path)}


def _refleter_statut_reglement(reglement_opaque: str, mouvement_opaque: str, db_path=None) -> None:
    """Reflet d'affichage uniquement : la vérité du lien reste `banque_rapprochements`."""
    etat = etat_reglement(reglement_opaque, db_path)
    if etat["statut"] == "RAPPROCHE":
        regl.marquer_rapproche(reglement_opaque, mouvement_opaque, db_path=db_path)


def confirmer(rapprochement_opaque: str, reglement_opaque: str, *, acteur: str = "",
              commentaire: str = "", db_path=None) -> dict[str, Any]:
    res = rappro.confirmer(rapprochement_opaque, commentaire=commentaire, acteur=acteur,
                           db_path=db_path)
    if res.get("ok"):
        _refleter_statut_reglement(reglement_opaque, "", db_path)
    return res


def annuler(rapprochement_opaque: str, reglement_opaque: str, *, acteur: str = "",
            commentaire: str = "", db_path=None) -> dict[str, Any]:
    res = rappro.annuler(rapprochement_opaque, commentaire=commentaire, acteur=acteur,
                         db_path=db_path)
    if res.get("ok"):
        from app.db.connection import get_db
        conn = get_db(db_path)
        try:
            # Le lien annulé ne compte plus : le règlement redevient non/partiellement rapproché.
            conn.execute("UPDATE reglements_fournisseurs SET statut=?, mouvement_id_opaque=NULL "
                         "WHERE reglement_id_opaque=? AND statut<>?",
                         (regl.ST_ENREGISTRE, reglement_opaque, regl.ST_ANNULE))
            conn.commit()
        finally:
            conn.close()
    return res


# ── Vue inverse : depuis un mouvement bancaire ──────────────────────────────

def contexte_metier_du_mouvement(mouvement_opaque: str, db_path=None) -> list[dict[str, Any]]:
    """Pour la fiche mouvement : quels règlements/factures/fournisseurs ce mouvement porte-t-il,
    et quel est le solde de la facture avant/après ce rapprochement."""
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        liens = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE mouvement_id_opaque=? AND type_objet=? "
            "ORDER BY id DESC", (mouvement_opaque, TYPE_OBJET)).fetchall()
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for l in liens:
        reglement = regl.charger(l["objet_id"], db_path)
        if reglement is None:
            out.append({"rapprochement": dict(l), "reglement": None, "factures": []})
            continue
        factures = []
        for rep in reglement["repartitions"]:
            f = fact.charger(rep["facture_id_opaque"], db_path)
            if f is None:
                continue
            # Solde « avant » = solde actuel + la part imputée par ce règlement sur cette facture.
            factures.append({
                "facture_id_opaque": f["facture_id_opaque"], "facture_ref": f["facture_ref"],
                "statut": f["statut"], "montant_ttc": f["montant_ttc"],
                "montant_impute": rep["montant"],
                "solde_avant": round(f["solde_restant"] + rep["montant"], 2),
                "solde_apres": f["solde_restant"],
            })
        out.append({"rapprochement": dict(l), "reglement": reglement, "factures": factures,
                    "fournisseur_id_opaque": reglement["fournisseur_id_opaque"]})
    return out

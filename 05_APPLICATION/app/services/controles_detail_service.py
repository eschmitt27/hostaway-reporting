"""Service DÉTAIL & GRAIN ACTIONNABLE des contrôles (APP-5B) — LECTURE SEULE.

Ouvre chaque contrôle agrégé du moteur (Lot11) en éléments détaillés actionnables, un par entité
réelle (réservation, logement × mois, mouvement bancaire). Le grain vient de la SOURCE détaillée
(cf. controles_detail_reader), jamais d'une reconstruction : l'application n'invente ni ne reclasse
aucune anomalie. Chaque élément reçoit un identifiant PUBLIC OPAQUE `CTRL-<hash>` (aucune donnée
sensible) et, quand une fiche métier existe, un lien de correction.

Ne recalcule aucune commission, aucun montant : uniquement les valeurs du moteur et du référentiel.
Aucun compte bancaire complet, aucun chemin absolu, aucune donnée voyageur superflue exposés.
"""
from __future__ import annotations

import hashlib
from typing import Any

import app.config as cfg
from app.readers import controles_detail_reader as dreader
from app.services import banques_controle_service as banque_ctrl

# Codes agrégés que l'on sait détailler, et leur module d'origine.
CODES_DETAILLABLES = {
    "VRBO_MONTANT_NON_RENSEIGNE",
    "RESERVATION_A_CONTROLER_SANS_COMMISSION",
    "MENAGE_EXTERNE_ECART_HOSTAWAY",
    "MENAGE_EXTERNE_LOGEMENT_HORS_HA",
    "MENAGE_HA_SANS_FACTURE_EXTERNE",
    "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY",
    "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE",
}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _nombre(v: Any) -> float | int | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


# ── Identifiant public opaque des éléments détaillés ─────────────────────────

def id_opaque(code: str, entite: str, mois: str = "", index: str = "") -> str:
    """« CTRL-<hash12> » stable, unique, sans donnée sensible ni concaténation brute visible."""
    base = "|".join([cfg.CONTROLES_OPAQUE_SALT, _txt(code), _txt(entite), _txt(mois), _txt(index)])
    return "CTRL-" + hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]


# ── Expansion par code ───────────────────────────────────────────────────────

def _classer_commission(row: dict[str, Any]) -> tuple[str, str]:
    """(classification, libellé). Distingue les cas légitimement sans commission."""
    src = _txt(row.get("source")).lower()
    chan = _txt(row.get("channel_type")).upper()
    code_ano = _txt(row.get("code_anomalie_lot10")).upper()
    payout = _nombre(row.get("payout_calcule"))
    if "ownerstay" in src or "owner" in chan.lower() or "PROPRIETAIRE" in code_ano:
        return "SEJOUR_PROPRIETAIRE", "Séjour propriétaire — sans commission attendue"
    if "ANNUL" in code_ano or "CANCEL" in chan:
        return "ANNULEE", "Réservation annulée — hors commission"
    if payout is not None and payout == 0:
        return "MONTANT_NUL", "Montant nul — commission non applicable"
    if "vrbo" in src or chan == "VRBO":
        return "VRBO_SANS_MONTANT", "VRBO sans montant — saisie manuelle requise (Lot 4)"
    if "direct" in src or chan == "DIRECT":
        return "DIRECT_HORS_HOSTAWAY", "Réservation directe hors Hostaway — décision manuelle"
    return "A_ANALYSER", "À analyser — cause d'exclusion non catégorisée"


def _expand_commissions(vue: dict[str, Any]) -> list[dict[str, Any]]:
    idx_res = dreader.reservations_index()
    out = []
    for row in dreader.commissions_a_controler():
        rid = _txt(row.get("reservation_id"))
        res = idx_res.get(rid, {})
        mois = _txt(res.get("mois"))
        logement = _txt(res.get("logement_id")) or _txt(row.get("logement_id_snapshot"))
        prop = _txt(res.get("proprietaire_id"))
        classe, classe_lib = _classer_commission(row)
        out.append({
            "code": vue["code"], "module": "COMMISSIONS", "niveau": vue["niveau"], "mois": mois,
            "entite_id": rid,
            "resume": f"Réservation {rid} exclue du calcul de commission",
            "classification": classe, "classification_libelle": classe_lib,
            "donnees": {
                "reservation_id": rid, "logement": logement, "proprietaire": prop,
                "mois": mois, "canal": _txt(row.get("channel_type")) or _txt(row.get("source")),
                "montant_retenu": _nombre(res.get("montant_retenu")),
                "commission_attendue": _nombre(row.get("payout_calcule")),
                "assiette": _nombre(row.get("assiette_commission")),
                "cause": _txt(row.get("code_anomalie_lot10")) or _txt(row.get("source_payout")),
            },
            "lien_module": None, "lien_libelle": "Aucun écran de correction disponible pour ce contrôle",
        })
    return out


# Catégories explicatives VRBO (le périmètre du contrôle = source résolue Lot4quater).
VRBO_MONTANT_ABSENT = "MONTANT_RÉELLEMENT_ABSENT"
VRBO_ANNULEE = "ANNULÉE"
VRBO_HORS_COMPTA = "HORS_COMPTABILITÉ"
VRBO_DOUBLON = "DOUBLON"
VRBO_ICAL_INCOMPLET = "ICAL_INCOMPLET"
VRBO_A_SAISIR = "À_SAISIR_MANUELLEMENT"
VRBO_HORS_PERIMETRE = "NON_CONCERNÉE_PAR_LE_CONTRÔLE"

_VRBO_LIB = {
    VRBO_MONTANT_ABSENT: "Montant réellement absent",
    VRBO_ANNULEE: "Réservation annulée",
    VRBO_HORS_COMPTA: "Hors comptabilité",
    VRBO_DOUBLON: "Doublon",
    VRBO_ICAL_INCOMPLET: "iCal incomplet",
    VRBO_A_SAISIR: "À saisir manuellement (Lot 4)",
    VRBO_HORS_PERIMETRE: "Non concernée par ce contrôle (mois clôturé, historisée)",
}


def _classer_vrbo(row: dict[str, Any]) -> str:
    """Classe une réservation VRBO du périmètre moteur (jamais de donnée voyageur)."""
    montant = _nombre(row.get("montant_retenu"))
    statut = _txt(row.get("statut_controle")).upper()
    code_ano = _txt(row.get("code_anomalie")).upper()
    src_montant = _txt(row.get("source_montant")).upper()
    if "ANNUL" in statut or "CANCEL" in statut or "ANNUL" in code_ano:
        return VRBO_ANNULEE
    if "HORS_COMPTA" in code_ano or "EXCLU" in code_ano:
        return VRBO_HORS_COMPTA
    if "DOUBLON" in code_ano or "DUPLICATE" in code_ano:
        return VRBO_DOUBLON
    if "ICAL" in code_ano or "ICAL" in src_montant:
        return VRBO_ICAL_INCOMPLET
    if montant in (None, 0):
        return VRBO_MONTANT_ABSENT
    return VRBO_A_SAISIR


def _vrbo_element(vue: dict[str, Any], row: dict[str, Any], classe: str, hors_perimetre: bool) -> dict[str, Any]:
    rid = _txt(row.get("reservation_id_hostaway")) or _txt(row.get("reservation_calc_id"))
    montant = _nombre(row.get("montant_retenu"))
    return {
        "code": vue["code"], "module": "RESERVATIONS", "niveau": vue["niveau"],
        "mois": _txt(row.get("mois")), "entite_id": rid,
        "resume": f"Réservation VRBO {rid} — {_VRBO_LIB.get(classe, classe)}",
        "classification": classe, "classification_libelle": _VRBO_LIB.get(classe, classe),
        "hors_perimetre_controle": hors_perimetre,
        "donnees": {
            "reservation_id": rid, "logement": _txt(row.get("logement_id")),
            "periode": _txt(row.get("mois")),
            "date_arrivee": _txt(row.get("date_arrivee")), "date_depart": _txt(row.get("date_depart")),
            "canal": "VRBO", "montant_brut": montant,
            "source": _txt(row.get("source")), "statut": _txt(row.get("statut_controle")),
        },
        "lien_module": None, "lien_libelle": "Saisie manuelle des montants VRBO (Lot 4 — hors application)",
    }


def _expand_vrbo(vue: dict[str, Any]) -> list[dict[str, Any]]:
    """Éléments actionnables = périmètre moteur EXACT (source résolue). Aucune donnée voyageur."""
    return [_vrbo_element(vue, row, _classer_vrbo(row), hors_perimetre=False)
            for row in dreader.reservations_vrbo()]


def vrbo_hors_perimetre(vue: dict[str, Any]) -> list[dict[str, Any]]:
    """Vue TECHNIQUE : VRBO live hors périmètre moteur (mois clôturés), jamais des anomalies actives."""
    return [_vrbo_element(vue, row, VRBO_HORS_PERIMETRE, hors_perimetre=True)
            for row in dreader.reservations_vrbo_hors_perimetre()]


def _expand_menages(vue: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for row in dreader.ecarts_menages(vue["code"]):
        logement = _txt(row.get("logement_id"))
        mois = _txt(row.get("mois"))
        nb_fact = _nombre(row.get("nombre_menages_facture"))
        nb_ha = _nombre(row.get("nombre_menages_hostaway"))
        ecart = _nombre(row.get("ecart"))
        if vue["code"] == "MENAGE_EXTERNE_LOGEMENT_HORS_HA":
            classe, classe_lib = "LOGEMENT_HORS_HA", "Logement facturé absent du comptage Hostaway"
        elif nb_fact in (None, 0):
            classe, classe_lib = "FACTURE_ABSENTE_HOSTAWAY", "Ménages Hostaway sans facture externe"
        elif ecart and ecart != 0:
            classe, classe_lib = "ECART_QUANTITE", "Écart de quantité facturé vs Hostaway"
        else:
            classe, classe_lib = "RAPPROCHE", "Rapproché — volume cohérent"
        out.append({
            "code": vue["code"], "module": "MENAGES_EXT", "niveau": vue["niveau"], "mois": mois,
            "entite_id": logement,
            "resume": f"Logement {logement} — {mois} : écart ménages ({ecart if ecart is not None else '—'})",
            "classification": classe, "classification_libelle": classe_lib,
            "donnees": {
                "logement_id": logement, "mois": mois,
                "prestataires": _txt(row.get("prestataires_factures")),
                "nombre_hostaway": nb_ha, "nombre_facture": nb_fact, "ecart": ecart,
                "commentaire_moteur": _txt(row.get("commentaire_controle")),
            },
            "lien_module": f"/menages?logement_id={logement}&mois={mois}",
            "lien_libelle": "Diagnostic / rapprochement Ménages",
        })
    return out


def _expand_banque(vue: dict[str, Any]) -> list[dict[str, Any]]:
    from app.readers import banques_reader
    out = []
    mois = vue["mois"]
    for row in dreader.banque_non_classees(mois):
        mid = _txt(row.get("mouvement_id"))
        opq = banque_ctrl.id_opaque(mid)   # identifiant public MVT opaque (jamais le mouvement_id brut)
        out.append({
            "code": vue["code"], "module": "BANQUE", "niveau": vue["niveau"], "mois": mois,
            "entite_id": opq,
            "resume": f"Mouvement bancaire à classer — {mois}",
            "classification": "NON_CLASSEE", "classification_libelle": "Ligne bancaire non classée (rapprochement requis)",
            "donnees": {
                "mvt_opaque": opq,
                "date": banques_reader.date_affichage(row.get("date_operation")),
                "montant": _nombre(row.get("montant")), "sens": _txt(row.get("sens")),
                "compte_masque": banques_reader.masquer_compte(row.get("compte_id")),
                "proposition": _txt(row.get("categorie")) or "—",
                "statut": _txt(row.get("statut_classification")),
            },
            "lien_module": f"/banques-caisse/mouvements/{opq}/modifier",
            "lien_libelle": "Contrôler / catégoriser le mouvement (APP-4B)",
        })
    return out


_EXPANDERS = {
    "VRBO_MONTANT_NON_RENSEIGNE": _expand_vrbo,
    "RESERVATION_A_CONTROLER_SANS_COMMISSION": _expand_commissions,
    "MENAGE_EXTERNE_ECART_HOSTAWAY": _expand_menages,
    "MENAGE_EXTERNE_LOGEMENT_HORS_HA": _expand_menages,
    "MENAGE_HA_SANS_FACTURE_EXTERNE": _expand_menages,
    "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY": _expand_menages,
    "CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE": _expand_banque,
}


def est_detaillable(code: str) -> bool:
    return code in _EXPANDERS


def expand(vue: dict[str, Any]) -> list[dict[str, Any]]:
    """Éléments détaillés d'un contrôle agrégé. Chaque élément reçoit son CTRL- opaque + ctrl_pk parent."""
    fn = _EXPANDERS.get(vue["code"])
    if fn is None:
        return []
    elements = fn(vue)
    for i, el in enumerate(elements):
        el["ctrl_opaque"] = id_opaque(el["code"], el["entite_id"], el["mois"], str(i))
        el["ctrl_pk_moteur"] = vue.get("stable_id", "")
        el["parent_stable_id"] = vue.get("stable_id", "")
    return elements


# ── Index inverse (résolution opaque → détail) ───────────────────────────────

def index_detail(vues_agregees: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """{ctrl_opaque: élément détaillé} sur tous les contrôles détaillables fournis."""
    idx: dict[str, dict[str, Any]] = {}
    for v in vues_agregees:
        if est_detaillable(v["code"]):
            for el in expand(v):
                idx[el["ctrl_opaque"]] = el
    return idx


def resoudre(ctrl_opaque: str, vues_agregees: list[dict[str, Any]]) -> dict[str, Any] | None:
    return index_detail(vues_agregees).get(_txt(ctrl_opaque))

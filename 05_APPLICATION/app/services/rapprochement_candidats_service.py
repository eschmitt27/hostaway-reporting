"""Recherche de candidats + contrôles du rapprochement déclaratif (APP-3F).

Propose des mouvements bancaires candidats pour un règlement déclaré payé (APP-3E), avec critères
EXPLICABLES (jamais un score probabiliste opaque). Ne confirme JAMAIS automatiquement. Détection
pure — aucun recalcul d'un montant dont la vérité appartient au moteur.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.readers import rapprochement_bancaire_reader as banque_contrat

# ── Règle métier : fenêtre de date (constante explicite, configurable, jamais cachée en template) ──
# Le projet n'expose pas de fenêtre de rapprochement propriétaire réutilisable ; on fixe ici une
# fenêtre par défaut documentée. À ajuster si une règle métier officielle est validée.
FENETRE_JOURS_DEFAUT = 7

# Tolérance de montant : par défaut montant EXACT uniquement. Toute divergence => contrôle manuel /
# anomalie, jamais un rapprochement silencieux. Ne pas inventer de tolérance.
TOLERANCE_MONTANT_EXACT = 0.0

BLOQUANT = "BLOQUANT"
INFO = "INFO"

_LIBELLES = {
    "RAPPROCHEMENT_SOURCE_ABSENTE": "La source bancaire est absente.",
    "RAPPROCHEMENT_SOURCE_VIDE": "La source bancaire est vide.",
    "RAPPROCHEMENT_SCHEMA_INVALIDE": "Le schéma de la source bancaire est invalide.",
    "RAPPROCHEMENT_REGLEMENT_NON_PAYE": "Le règlement n'est pas marqué comme payé (APP-3E).",
    "RAPPROCHEMENT_REGLEMENT_ANNULE": "Le règlement est annulé.",
    "RAPPROCHEMENT_AUCUN_CANDIDAT": "Aucun mouvement candidat trouvé.",
    "RAPPROCHEMENT_PLUSIEURS_CANDIDATS": "Plusieurs mouvements candidats — contrôle manuel requis.",
    "RAPPROCHEMENT_MONTANT_DIFFERENT": "Le montant du mouvement diffère du montant déclaré.",
    "RAPPROCHEMENT_DATE_INCOHERENTE": "La date du mouvement est hors de la fenêtre autorisée.",
    "RAPPROCHEMENT_SENS_INVALIDE": "Le mouvement n'est pas un débit (sortant).",
    "RAPPROCHEMENT_MOUVEMENT_DEJA_UTILISE": "Ce mouvement est déjà rapproché à un autre règlement.",
    "RAPPROCHEMENT_REGLEMENT_DEJA_RAPPROCHE": "Ce règlement est déjà rapproché.",
    "RAPPROCHEMENT_MOUVEMENT_DISPARU": "Le mouvement a disparu de la source depuis la proposition.",
    "RAPPROCHEMENT_SOURCE_MODIFIEE": "La source a changé depuis la proposition (empreinte différente).",
    "RAPPROCHEMENT_VERSION_OBSOLETE": "Version obsolète — l'objet a été modifié entretemps.",
    "RAPPROCHEMENT_CONFLIT_CONCURRENT": "Conflit d'accès concurrent.",
    "RAPPROCHEMENT_DOUBLON": "Rapprochement en doublon détecté.",
    "RAPPROCHEMENT_REFERENCE_INCOMPATIBLE": "La référence interne ne correspond pas.",
}

# INFO = jamais bloquant.
_INFO_CODES = {
    "RAPPROCHEMENT_AUCUN_CANDIDAT", "RAPPROCHEMENT_PLUSIEURS_CANDIDATS",
    "RAPPROCHEMENT_MONTANT_DIFFERENT", "RAPPROCHEMENT_DATE_INCOHERENTE",
    "RAPPROCHEMENT_REFERENCE_INCOMPATIBLE",
}


def libelle(code: str) -> str:
    return _LIBELLES.get(code, code)


def _jours_ecart(date_a: str, date_b: str) -> int | None:
    try:
        da = date.fromisoformat((date_a or "")[:10])
        db = date.fromisoformat((date_b or "")[:10])
        return abs((da - db).days)
    except (ValueError, TypeError):
        return None


def chercher_candidats(*, montant_declare: float | None, date_declaree: str, mois: str,
                       reference_interne: str = "", fenetre_jours: int = FENETRE_JOURS_DEFAUT,
                       mouvements_deja_rapproches: set[str] | None = None) -> dict[str, Any]:
    """Retourne {'etat': ..., 'candidats': [ {mouvement, criteres, score, ecart_montant, ecart_jours} ]}.
    Critères satisfaits explicitement listés. Aucune confirmation — proposition uniquement."""
    deja = mouvements_deja_rapproches or set()
    src = banque_contrat.source()
    if not src.disponible:
        etat_code = {
            banque_contrat.ETAT_FICHIER_ABSENT: "RAPPROCHEMENT_SOURCE_ABSENTE",
            banque_contrat.ETAT_ONGLET_ABSENT: "RAPPROCHEMENT_SOURCE_ABSENTE",
            banque_contrat.ETAT_VIDE: "RAPPROCHEMENT_SOURCE_VIDE",
            banque_contrat.ETAT_NON_ALIMENTE: "RAPPROCHEMENT_SOURCE_VIDE",
            banque_contrat.ETAT_ILLISIBLE: "RAPPROCHEMENT_SCHEMA_INVALIDE",
        }.get(src.etat, "RAPPROCHEMENT_SCHEMA_INVALIDE")
        return {"etat": src.etat, "code_source": etat_code, "candidats": []}

    ref_norm = "".join(ch for ch in (reference_interne or "").upper() if ch.isalnum())
    candidats = []
    for m in src.mouvements:
        if m.mouvement_opaque in deja:
            continue
        criteres = []
        # Critère : sens sortant (débit)
        sens_ok = m.sens == "DEBIT"
        if sens_ok:
            criteres.append("sens_sortant")
        # Critère : montant exact (valeur absolue, tolérance 0 par défaut)
        ecart_montant = None
        montant_ok = False
        if montant_declare is not None and m.montant is not None:
            ecart_montant = round(abs(abs(m.montant) - abs(montant_declare)), 2)
            montant_ok = ecart_montant <= TOLERANCE_MONTANT_EXACT
            if montant_ok:
                criteres.append("montant_exact")
        # Critère : mois compatible
        if mois and m.mois == mois:
            criteres.append("mois_compatible")
        # Critère : date dans la fenêtre
        ecart_jours = _jours_ecart(date_declaree, m.date)
        date_ok = ecart_jours is not None and ecart_jours <= fenetre_jours
        if date_ok:
            criteres.append("date_dans_fenetre")
        # Critère : référence compatible (si fournie)
        if ref_norm and ref_norm in m.reference_normalisee:
            criteres.append("reference_compatible")
        # Un candidat DOIT être sortant et de montant exact pour être proposé (jamais silencieux sinon).
        if sens_ok and montant_ok:
            candidats.append({
                "mouvement": m, "criteres": criteres, "score": len(criteres),
                "ecart_montant": ecart_montant, "ecart_jours": ecart_jours,
            })
    # Tri : plus de critères d'abord, puis plus proche en date.
    candidats.sort(key=lambda c: (-c["score"], c["ecart_jours"] if c["ecart_jours"] is not None else 9999))
    return {"etat": src.etat, "code_source": None, "candidats": candidats}


def evaluer_controles(*, source_etat_code: str | None, reglement_paye: bool, reglement_annule: bool,
                      nb_candidats: int, mouvement_present: bool | None, empreinte_snapshot: str = "",
                      empreinte_courante: str = "", ecart_montant: float | None = None,
                      sens_sortant: bool | None = None, mouvement_deja_utilise: bool = False,
                      reglement_deja_rapproche: bool = False) -> dict[str, Any]:
    """Codes de contrôle purs. INFO ne bloque jamais."""
    codes: list[str] = []
    if source_etat_code:
        codes.append(source_etat_code)
    if reglement_annule:
        codes.append("RAPPROCHEMENT_REGLEMENT_ANNULE")
    if not reglement_paye:
        codes.append("RAPPROCHEMENT_REGLEMENT_NON_PAYE")
    if reglement_deja_rapproche:
        codes.append("RAPPROCHEMENT_REGLEMENT_DEJA_RAPPROCHE")
    if mouvement_deja_utilise:
        codes.append("RAPPROCHEMENT_MOUVEMENT_DEJA_UTILISE")
    if nb_candidats == 0 and not source_etat_code:
        codes.append("RAPPROCHEMENT_AUCUN_CANDIDAT")
    if nb_candidats > 1:
        codes.append("RAPPROCHEMENT_PLUSIEURS_CANDIDATS")
    if mouvement_present is False:
        codes.append("RAPPROCHEMENT_MOUVEMENT_DISPARU")
    if empreinte_snapshot and empreinte_courante and empreinte_snapshot != empreinte_courante:
        codes.append("RAPPROCHEMENT_SOURCE_MODIFIEE")
    if ecart_montant is not None and ecart_montant > TOLERANCE_MONTANT_EXACT:
        codes.append("RAPPROCHEMENT_MONTANT_DIFFERENT")
    if sens_sortant is False:
        codes.append("RAPPROCHEMENT_SENS_INVALIDE")

    codes = list(dict.fromkeys(codes))
    bloquants = [c for c in codes if c not in _INFO_CODES]
    informatifs = [c for c in codes if c in _INFO_CODES]
    return {"bloquants": bloquants, "informatifs": informatifs,
            "confirmable": len(bloquants) == 0}

"""Lot6f — ventilation d'une charge ménage (impact analytique) via COUT_STANDARD_MENAGES_MOIS.

Réplique testable de la clé Lot6f (D103) : poids = nb_menages × cout_standard_unitaire(type_logement),
quote_part = montant × poids_ligne / Σ poids. Somme des quotes-parts = montant réparti.

Une charge ménage est UNE charge économique unique : cette ventilation est purement analytique
(coût complet ménage / gain-perte), ne crée jamais de seconde charge réelle ni de réserve refacturable.
Lit la table MENAGE de SAISIE_Charges_Impacts (une charge → sélections intervenant OU logement).
Aucune écriture réelle.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

MODE_INTERVENANT = "INTERVENANT"
MODE_LOGEMENT = "LOGEMENT"


def charger_impacts_menage(path: Path) -> list[dict[str, Any]]:
    """Lit l'onglet MENAGE (lecture seule) de SAISIE_Charges_Impacts.xlsx."""
    import openpyxl
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        ws = wb["MENAGE"]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    out = []
    for r in rows[1:]:
        if not any(c not in (None, "") for c in r):
            continue
        out.append({headers[i]: r[i] for i in range(len(headers))})
    return out


def impacts_d_une_charge(impacts: Iterable[dict[str, Any]], charge_id: str) -> list[dict[str, Any]]:
    """Filtre les lignes MENAGE d'un charge_id valide (jamais de lecture hors charge_id)."""
    cid = str(charge_id or "").strip()
    return [r for r in impacts if str(r.get("charge_id", "") or "").strip() == cid]


def _modes(rows: list[dict[str, Any]]) -> set[str]:
    return {str(r.get("mode", "") or "").strip().upper() for r in rows}


def ventiler_charge_menage(
    montant: float,
    impact_rows: list[dict[str, Any]],
    nb_menages: dict[str, int],
    cout_standard_unitaire: dict[str, float],
) -> dict[str, Any]:
    """Ventile `montant` sur le périmètre ménage d'une charge (clé COUT_STANDARD).

    - `impact_rows` : lignes MENAGE de la charge (mode INTERVENANT ou LOGEMENT, jamais les deux).
    - `nb_menages`  : {clé_périmètre -> nb_menages du mois}  (clé = intervenant_id ou logement_id).
    - `cout_standard_unitaire` : {clé_périmètre -> coût standard unitaire}.
    Retourne {statut, quote_parts:[{cle, quote_part}], somme, message}.
    Somme des quotes-parts = montant. Aucun ménage éligible → A_CONTROLER (jamais de répartition arbitraire).
    """
    modes = _modes(impact_rows)
    if MODE_INTERVENANT in modes and MODE_LOGEMENT in modes:
        return {"statut": "REFUSE", "quote_parts": [], "somme": 0.0,
                "message": "Intervenant ET logement simultanés interdits (un seul mode)."}
    if not impact_rows:
        return {"statut": "A_CONTROLER", "quote_parts": [], "somme": 0.0,
                "message": "Aucune sélection ménage."}

    mode = (MODE_INTERVENANT if MODE_INTERVENANT in modes else MODE_LOGEMENT)
    key_col = "intervenant_id" if mode == MODE_INTERVENANT else "logement_id"
    cles = sorted({str(r.get(key_col, "") or "").strip() for r in impact_rows if str(r.get(key_col, "") or "").strip()})

    poids: dict[str, float] = {}
    for c in cles:
        nb = nb_menages.get(c, 0)
        su = cout_standard_unitaire.get(c)
        poids[c] = (nb * su) if (nb and su) else 0.0
    total_poids = sum(poids.values())
    if total_poids <= 0:
        # Aucun ménage éligible sur le périmètre/mois → jamais de répartition arbitraire.
        return {"statut": "A_CONTROLER", "quote_parts": [], "somme": 0.0,
                "message": "Aucun ménage éligible sur le périmètre et le mois."}

    # Répartition proportionnelle au poids, centimes déterministes (résidu au premier trié).
    total_cents = int(round(float(montant) * 100))
    quotes: list[dict[str, Any]] = []
    cumul = 0
    ordered = [c for c in cles if poids[c] > 0]
    for i, c in enumerate(ordered):
        if i < len(ordered) - 1:
            cents = int(round(total_cents * poids[c] / total_poids))
            cumul += cents
        else:
            cents = total_cents - cumul  # dernier absorbe le résidu → somme exacte
        quotes.append({"cle": c, "mode": mode, "quote_part": round(cents / 100.0, 2)})
    somme = round(sum(q["quote_part"] for q in quotes), 2)
    return {"statut": "VALIDE", "mode": mode, "quote_parts": quotes, "somme": somme,
            "message": f"Ventilé sur {len(quotes)} {mode.lower()}(s) par COUT_STANDARD_MENAGES_MOIS."}

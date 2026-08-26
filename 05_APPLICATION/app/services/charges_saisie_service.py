"""Saisie des charges — UI → SQLite (migration 0052).

CE QUE CE MODULE REMPLACE
La saisie des charges passait par `SAISIE_Charges_Flux.xlsx` : l'utilisateur remplissait le
classeur, un rafraîchissement Power Query produisait `MASTER_FACT_MAN_Charges.xlsx`, et
l'application lisait ce master. Trois artefacts, deux rafraîchissements manuels, et un moteur qui
refusait de démarrer sans le classeur.

La cible est directe : **UI → service → SQLite**. Pas d'aller-retour par Excel, et surtout pas de
circuit `UI → SQLite → Excel → moteur`, qui recréerait le problème sous un autre nom.

CE QUI N'EST PAS RECALCULÉ ICI
Rien. Ce service ENREGISTRE une saisie ; il ne dérive ni le flux, ni le résultat, ni un statut de
contrôle. `code_impact`, `impact_resultat_*` et `statut_controle` sont saisis ou laissés vides, puis
lus tels quels par le reste de la chaîne (D044). Le calcul économique reste à Lot9/Lot10, les
contrôles à Lot11.

CORRECTION, PAS SUPPRESSION
Une charge peut être référencée par une écriture comptable, un rapprochement bancaire ou un flux
déjà calculé. Elle n'est donc jamais supprimée physiquement : elle est ANNULÉE, et
`charge_evenements` conserve l'avant/après.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.contrats_donnees import Charge, ContratInvalideError
from app.db.connection import get_db

STATUT_ACTIVE = "ACTIVE"
STATUT_ANNULEE = "ANNULEE"

EVT_CREATION = "CREATION"
EVT_MODIFICATION = "MODIFICATION"
EVT_ANNULATION = "ANNULATION"

E_CHAMP_MANQUANT = "CHARGE_CHAMP_MANQUANT"
E_MONTANT_INVALIDE = "CHARGE_MONTANT_INVALIDE"
E_MOIS_INVALIDE = "CHARGE_MOIS_INVALIDE"
E_INTROUVABLE = "CHARGE_INTROUVABLE"
E_DEJA_ANNULEE = "CHARGE_DEJA_ANNULEE"
E_DOUBLON = "CHARGE_ID_DEJA_UTILISE"
E_CONTRAT_INVALIDE = "CHARGE_CONTRAT_INVALIDE"

# Champs modifiables par la saisie. `charge_id`, `statut` et les horodatages n'en font pas partie :
# l'identité et le cycle de vie ne se corrigent pas comme une valeur métier.
CHAMPS_SAISIE = (
    "date_charge", "mois", "montant", "sens_flux", "sens", "categorie_charge_id",
    "filtre_vue_menage", "type_flux_id", "code_impact", "impact_resultat_reel",
    "impact_resultat_comptable", "prise_en_compta", "associe_id", "mode_paiement_id", "carte_id",
    "affectation_type", "logement_id", "proprietaire_id", "reservation_id", "refacturable",
    "source_flux", "methode_traitement", "paye_avec_montant_recupere", "lien_virement_banque",
    "statut_controle", "niveau_anomalie", "code_anomalie", "statut_rapprochement", "justificatif",
    "commentaire",
)

OBLIGATOIRES = ("date_charge", "montant", "categorie_charge_id")


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _nombre(valeur: Any) -> float | None:
    if valeur is None or valeur == "":
        return None
    try:
        return float(str(valeur).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def _mois_depuis_date(date_charge: str) -> str:
    return str(date_charge)[:7]


def valider(donnees: dict[str, Any]) -> dict[str, Any]:
    """Validation métier (§17). Rend un refus lisible, jamais une exception."""
    for champ in OBLIGATOIRES:
        valeur = donnees.get(champ)
        # `0` est une valeur PRÉSENTE : la tester avec `or ""` la ferait passer pour manquante et
        # afficherait « champ obligatoire manquant » là où le vrai motif est « montant nul ».
        if valeur is None or str(valeur).strip() == "":
            return _refus(E_CHAMP_MANQUANT, f"Champ obligatoire manquant : {champ}.")

    montant = _nombre(donnees.get("montant"))
    if montant is None:
        return _refus(E_MONTANT_INVALIDE, "Le montant doit être un nombre.")
    if montant == 0:
        return _refus(E_MONTANT_INVALIDE, "Une charge à 0 n'a pas d'effet : montant attendu ≠ 0.")

    mois = str(donnees.get("mois") or "").strip() or _mois_depuis_date(donnees["date_charge"])
    if len(mois) != 7 or mois[4] != "-":
        return _refus(E_MOIS_INVALIDE, f"Mois attendu au format AAAA-MM, reçu : {mois!r}.")

    return {"ok": True, "montant": montant, "mois": mois}


def _verifier_contrat(donnees: dict[str, Any]) -> dict[str, Any] | None:
    """Mission 11 : `valider()` reste la seule source de vérité métier ; ce contrat structurel
    (`app.contrats_donnees.Charge`) rejette en plus une date_charge non calendaire ou un montant
    non numérique qui, avant cette mission, auraient été acceptés tels quels (ex. `date_charge=
    "12/06/2026"`, silencieusement tronqué en mois via `str(...)[:7]` — un mois `"12/06"` invalide
    aurait alors circulé jusqu'à Lot9/Lot10 sans jamais être détecté ici). Appelé APRÈS `valider()`,
    jamais à sa place (§docstring `contrats_donnees.py`)."""
    try:
        Charge.from_dict(donnees)
    except ContratInvalideError as exc:
        return _refus(E_CONTRAT_INVALIDE, str(exc))
    return None


def _journaliser(conn, charge_id: str, evenement: str, acteur: str, motif: str,
                 avant: Any = None, apres: Any = None) -> None:
    conn.execute(
        "INSERT INTO charge_evenements (charge_id, evenement, acteur, motif, avant_json, "
        "apres_json) VALUES (?,?,?,?,?,?)",
        (charge_id, evenement, acteur or None, motif or None,
         json.dumps(avant, default=str) if avant else None,
         json.dumps(apres, default=str) if apres else None))


def _charge(conn, charge_id: str) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM charges WHERE charge_id = ?", (charge_id,)).fetchone()
    return dict(r) if r else None


def creer(donnees: dict[str, Any], *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée une charge. `charge_id` fourni, ou dérivé d'un identifiant opaque — jamais un rang."""
    validation = valider(donnees)
    if not validation["ok"]:
        return validation
    contrat_refus = _verifier_contrat(donnees)
    if contrat_refus is not None:
        return contrat_refus

    charge_id = str(donnees.get("charge_id") or "").strip() or f"CHG-{uuid.uuid4().hex[:12]}"
    valeurs = {c: donnees.get(c) for c in CHAMPS_SAISIE}
    valeurs["montant"] = validation["montant"]
    valeurs["mois"] = validation["mois"]

    conn = get_db(db_path)
    try:
        if _charge(conn, charge_id) is not None:
            return _refus(E_DOUBLON, f"Une charge porte déjà l'identifiant {charge_id}.")
        colonnes = ["charge_id", *CHAMPS_SAISIE, "date_saisie", "source_module", "acteur"]
        params = [charge_id, *(valeurs[c] for c in CHAMPS_SAISIE), _maintenant(), "SAISIE_APP",
                  acteur or None]
        conn.execute(
            f"INSERT INTO charges ({', '.join(colonnes)}) "
            f"VALUES ({', '.join(['?'] * len(colonnes))})", params)
        _journaliser(conn, charge_id, EVT_CREATION, acteur, "", apres=valeurs)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id}


def modifier(charge_id: str, donnees: dict[str, Any], *, acteur: str = "", motif: str = "",
             db_path=None) -> dict[str, Any]:
    """Corrige une charge existante. L'avant/après est journalisé."""
    validation = valider(donnees)
    if not validation["ok"]:
        return validation
    contrat_refus = _verifier_contrat(donnees)
    if contrat_refus is not None:
        return contrat_refus

    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE, "Une charge annulée ne se corrige pas ; en créer une "
                                          "nouvelle.")
        valeurs = {c: donnees.get(c) for c in CHAMPS_SAISIE}
        valeurs["montant"] = validation["montant"]
        valeurs["mois"] = validation["mois"]
        conn.execute(
            f"UPDATE charges SET {', '.join(f'{c} = ?' for c in CHAMPS_SAISIE)}, "
            "date_modification = ? WHERE charge_id = ?",
            [*(valeurs[c] for c in CHAMPS_SAISIE), _maintenant(), charge_id])
        _journaliser(conn, charge_id, EVT_MODIFICATION, acteur, motif,
                     avant={c: avant.get(c) for c in CHAMPS_SAISIE}, apres=valeurs)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id}


def annuler(charge_id: str, *, acteur: str = "", motif: str = "", db_path=None) -> dict[str, Any]:
    """Annule une charge — jamais de suppression physique (elle peut être déjà référencée)."""
    conn = get_db(db_path)
    try:
        avant = _charge(conn, charge_id)
        if avant is None:
            return _refus(E_INTROUVABLE, f"Charge inconnue : {charge_id}.")
        if avant["statut"] == STATUT_ANNULEE:
            return _refus(E_DEJA_ANNULEE, f"La charge {charge_id} est déjà annulée.")
        conn.execute("UPDATE charges SET statut = ?, date_modification = ? WHERE charge_id = ?",
                     (STATUT_ANNULEE, _maintenant(), charge_id))
        _journaliser(conn, charge_id, EVT_ANNULATION, acteur, motif,
                     avant={"statut": STATUT_ACTIVE}, apres={"statut": STATUT_ANNULEE})
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "charge_id": charge_id, "statut": STATUT_ANNULEE}


def historique(charge_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM charge_evenements WHERE charge_id = ? ORDER BY id", (charge_id,))]
    finally:
        conn.close()

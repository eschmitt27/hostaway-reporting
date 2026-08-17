"""Adaptateur TEMPORAIRE : datasets de réservations SQLite → classeurs attendus par Lot 11.

Même statut que `banque_adaptateur_moteur`, et mêmes règles : le fichier produit vit dans le
workspace du run, n'est source de vérité pour rien, n'est lu que par le sous-processus moteur, et
disparaît avec le workspace. Voir `adaptateur_workspace` pour la mécanique commune.

CE QUE LOT 11 LIT RÉELLEMENT
Relevé dans sa source, pas supposé :
  · `MASTER_CALC_Reservations_Resolues.xlsx`, onglet `MASTER`  → réservations résolues
  · `MASTER_CALC_HA_Payout.xlsx`, onglet `data`                → payouts Hostaway

La liste doit être complète et les colonnes exactes : ces lectures échouent SANS erreur visible — le
lot conclut « source non disponible » et rend un code retour 0, donc zéro anomalie signalée.

CE QUI N'EST PAS FOURNI
Le classeur live (Lot 4bis) n'est pas produit : Lot 11 ne le lit pas. En écrire un donnerait
l'illusion d'une dépendance qui n'existe pas, et le jour où quelqu'un s'en servirait, la frontière
aurait bougé sans décision.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from app.services import adaptateur_workspace as ws
from app.services import reservations_dataset_service as ds

# Emplacements attendus par Lot 11, relatifs à la racine du workspace.
RESOLUES_REL = "02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx"
PAYOUT_REL = "02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx"

ONGLET_RESOLUES = "MASTER"
ONGLET_VUE_FLUX = "VUE_FLUX"
ONGLET_PAYOUT = "data"

E_AUCUN_DATASET = "RESERVATIONS_AUCUN_DATASET"
E_AUCUNE_EXTRACTION = "HOSTAWAY_AUCUNE_EXTRACTION"

# Colonnes du classeur résolu, dans l'ordre du moteur. `guestCount` et `ROW_HASH` gardent leur nom
# d'origine : Lot 11 les lit sous ce nom-là.
COLONNES_RESOLUES = (
    "reservation_calc_id", "ROW_HASH", "source", "reservation_id_hostaway", "reservation_hh_id",
    "mois", "logement_id", "proprietaire_id", "date_arrivee", "date_depart", "nuits", "guestCount",
    "source_guestCount", "montant_retenu", "source_montant", "code_impact", "impact_resultat_reel",
    "impact_resultat_comptable", "statut_controle", "niveau_anomalie", "code_anomalie",
    "commentaire", "source_module", "source_table", "source_pk", "date_integration", "canal",
    "etat_mois", "origine_initiale", "source_ligne", "methode", "payout_calcule", "menage_retenu",
    "assiette_commission")

COLONNES_PAYOUT = (
    "reservation_id", "listingMapId", "source", "channel_type", "statut_calcul_payout",
    "payout_calcule", "source_payout", "menage_retenu", "assiette_commission",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "cout_standard_date_debut_validite", "cout_standard_date_fin_validite", "logement_id_snapshot",
    "type_logement_id_snapshot", "date_reference_cout_menage", "inclure_resultat_auto",
    "extrait_le", "ROW_HASH")

# Nom en base → nom attendu par le moteur, pour les payouts.
_PAYOUT_VERS_MOTEUR = {"listing_map_id": "listingMapId", "row_hash": "ROW_HASH"}


def _vue_flux(lignes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sous-ensemble que le classeur portait dans son second onglet.

    Filtre repris tel quel de Lot 4quater : validé, impact réel, montant non nul. Le recalculer ici
    plutôt que de le stocker évite d'entretenir deux définitions du même sous-ensemble.
    """
    return [r for r in lignes
            if r.get("statut_controle") == "VALIDE"
            and r.get("impact_resultat_reel") == "OUI"
            and (r.get("montant_retenu") or 0) != 0]


def ecrire_resolues(racine_workspace: Path, *, db_path=None) -> dict[str, Any]:
    """Fabrique le classeur des réservations résolues attendu par Lot 11."""
    lignes = ds.lignes(ds.ETAPE_RESOLUES, db_path=db_path)
    if not lignes:
        return ws.refus(E_AUCUN_DATASET, ds.MESSAGES[ds.ETAT_NON_INITIALISE])

    resultat = ws.ecrire_classeur(
        Path(racine_workspace) / RESOLUES_REL,
        [(ONGLET_RESOLUES, COLONNES_RESOLUES, lignes),
         (ONGLET_VUE_FLUX, COLONNES_RESOLUES, _vue_flux(lignes))])
    resultat["nb_lignes"] = len(lignes)
    return resultat


def ecrire_payouts(racine_workspace: Path, *, db_path=None) -> dict[str, Any]:
    """Fabrique le classeur des payouts attendu par Lot 11."""
    from app.services import hostaway_raw_service as raw

    lignes = [{_PAYOUT_VERS_MOTEUR.get(k, k): v for k, v in ligne.items()}
              for ligne in raw.payouts(db_path=db_path)]
    if not lignes:
        return ws.refus(E_AUCUNE_EXTRACTION, raw.MESSAGES[raw.E_AUCUNE_EXTRACTION])

    resultat = ws.ecrire_classeur(Path(racine_workspace) / PAYOUT_REL,
                                  [(ONGLET_PAYOUT, COLONNES_PAYOUT, lignes)])
    resultat["nb_lignes"] = len(lignes)
    return resultat


def ecrire_tout(racine_workspace: Path, *, db_path=None) -> dict[str, Any]:
    """Fabrique les deux classeurs. Refuse en bloc si l'un des deux manque de données.

    Un classeur sur deux ferait tourner Lot 11 sur une moitié de contexte, et il conclurait sans
    erreur — la moitié absente ressemblerait à une absence d'anomalie.
    """
    resolues = ecrire_resolues(racine_workspace, db_path=db_path)
    if not resolues.get("ok"):
        return resolues
    payouts = ecrire_payouts(racine_workspace, db_path=db_path)
    if not payouts.get("ok"):
        return payouts
    return {"ok": True, "resolues": resolues, "payouts": payouts,
            "chemins": [resolues["chemin"], payouts["chemin"]]}

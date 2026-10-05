"""Déclenchement technique depuis Clôture : mêmes moteurs, aucun acte métier.

Les calculs du DAG sont globaux, sauf Ménages qui accepte un mois explicite.
Les descendants nécessaires restent exécutés par l'orchestrateur canonique.
Les sources externes, les factures réelles et les décisions métier sont hors périmètre.
"""
from __future__ import annotations

import logging
import uuid

from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

logger = logging.getLogger(__name__)

# Registre partagé par la détection des calculs périmés et leur déclenchement.
DATASETS_PAR_MODULE = {
    "RESERVATIONS": (dag.RESERVATIONS, dag.FLUX_LOT9, dag.LOT10, dag.LOT11),
    "MENAGES": (dag.MENAGES,),
    "FACTURES_CLIENTS": (dag.LOT12,),
}
ETATS_PERIMES = {orch.ST_A_RECALCULER, orch.ST_ECHEC, orch.ST_EN_COURS}
PORTEE = "CLOTURE_ACTUALISATION"


def calculable(dataset: str) -> bool:
    n = dag.NOEUDS.get(dataset)
    return bool(n and n.type_noeud == dag.TYPE_CALCUL and n.service and not n.externe)


def actualiser(mois: str, module: str, *, db_path=None) -> str:
    """Retourne un code de feedback sûr ; la vérité reste l'état réel des moteurs.

    Le bail sérialise les clics Clôture AVANT de relire les besoins : un second POST
    arrivé après le premier ne rejoue pas les calculs déjà frais. Le verrou global
    de l'orchestrateur protège également contre les autres écrans et le scheduler.
    Une clôture de module n'est jamais modifiée par cette opération technique.
    """
    from app.services import cloture_modules_service as cm
    from app.services import clotures_service as cs

    if not cs.mois_valide(mois) or module not in cm.PAR_CLE:
        raise ValueError("Mois ou module inconnu.")
    datasets = tuple(n for n in DATASETS_PAR_MODULE.get(module, ()) if calculable(n))
    if not datasets:
        return "indisponible"
    jeton = "CLO-ACT-" + uuid.uuid4().hex
    verrou = orch.prendre_verrou(PORTEE, jeton, db_path=db_path)
    if not verrou["ok"]:
        return "occupe"
    try:
        if orch.verrou_actif(orch.PORTEE_GLOBALE, db_path=db_path):
            return "occupe"
        orch.marquer_runs_interrompus(db_path=db_path)
        etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(db_path)}
        cibles = {n for n in datasets if etats.get(n) in ETATS_PERIMES}
        if any(etats.get(n) == orch.ST_EN_COURS for n in cibles):
            return "occupe"
        if not cibles:
            return "succes"
        # Une cible aval périmée ne peut pas être calculée sur un amont périmé
        # ou encore absent. Aucun import automatique pour lever un problème source.
        for n in tuple(cibles):
            cibles.update(a for a in dag.ascendants(n) if calculable(a)
                          and etats.get(a) != orch.ST_A_JOUR)
        resultat = orch.actualiser(
            cibles=[n for n in dag.ordre_topologique() if n in cibles],
            mois=mois, declencheur=orch.DECLENCHEUR_MANUEL, db_path=db_path)
        if resultat.get("code") == orch.E_VERROU:
            return "occupe"
        # PARTIEL est un succès partiel pour le moteur, pas un succès de ce bouton.
        if not resultat.get("ok") or resultat.get("nb_echecs", 0):
            codes = {e.get("code") for e in resultat.get("etapes", [])}
            if "MENAGES_REFERENTIEL_ABSENT" in codes:
                return "erreur_referentiel"
            return "erreur"
        apres = {d["dataset"]: d["statut"] for d in orch.etat_datasets(db_path)}
        return "succes" if all(apres.get(n) == orch.ST_A_JOUR for n in cibles) else "erreur"
    except Exception:
        logger.exception("Échec de l'actualisation Clôture (%s, %s)", mois, module)
        return "erreur"
    finally:
        orch.liberer_verrou(PORTEE, jeton, db_path=db_path)

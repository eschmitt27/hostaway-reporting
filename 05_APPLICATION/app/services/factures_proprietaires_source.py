"""Passerelle entre la préfacture Lot 12 et l'objet facture propriétaire.

Ce module ne calcule rien et ne corrige rien : il traduit une préfacture déjà produite par le
moteur en la structure attendue par `factures_proprietaires_service`. Toute la logique de calcul
reste dans Lot 10 / Lot 12.

C'est ici que s'opère concrètement la distinction relevé / facture : la préfacture porte 12 ou 13
lignes destinées au propriétaire, dont seules 5 correspondent à des prestations facturées.
"""
from __future__ import annotations

from typing import Any

from app.readers import proprietaires_reader as reader
from app.services import factures_proprietaires_service as svc

# Lot 12 nomme ses lignes par `type_ligne` ; le service facture utilise les mêmes identifiants
# pour les types facturables. La correspondance est donc directe, mais explicite pour éviter
# qu'un renommage d'un côté ne casse silencieusement l'autre.
CORRESPONDANCE = {t: t for t in svc.TYPES_FACTURABLES}


def _n(v: Any) -> float:
    try:
        return round(float(v or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def depuis_prefacture(entete: dict[str, Any], lignes: list[dict[str, Any]]) -> dict[str, Any]:
    """Construit la structure `source` attendue par le service depuis une préfacture Lot 12."""
    source: dict[str, Any] = {
        "mois": entete.get("mois"),
        "proprietaire_id": entete.get("proprietaire_id"),
        "logement_id": entete.get("logement_id"),
        "source_calcul": entete.get("facture_id"),
        "montant_du_conciergerie": _n(entete.get("total_reglement_du")),
    }
    for ligne in lignes:
        type_ligne = str(ligne.get("type_ligne") or "")
        if type_ligne in CORRESPONDANCE:
            source[CORRESPONDANCE[type_ligne]] = _n(ligne.get("montant"))
        elif type_ligne in svc.TYPES_NON_FACTURABLES:
            # Conservé pour l'affichage du relevé, jamais transformé en ligne facturée.
            source[type_ligne] = _n(ligne.get("montant"))
    return source


def propositions_du_mois(mois: str, proprietaires: list[str], *, db_path=None) -> list[dict]:
    """Propose une facture par préfacture du mois, sans rien écrire.

    Chaque proposition porte un statut :
      PRETE          — facturable en l'état ;
      A_CONTROLER    — données de calcul incomplètes ou incohérentes, facture non proposée ;
      NON_CONCERNE   — rien à facturer (aucun montant), ou facture déjà existante.
    """
    propositions: list[dict[str, Any]] = []
    for prop_id in proprietaires:
        entetes = reader.read_prefacture_entetes_prop_mois(prop_id, mois)
        if not entetes:
            continue
        lignes_par_facture = reader.read_prefacture_lignes(
            [e.get("facture_id") for e in entetes if e.get("facture_id")])

        for entete in entetes:
            source = depuis_prefacture(entete, lignes_par_facture.get(entete.get("facture_id"), []))
            apercu = svc.previsualiser(source)

            existantes = [f for f in svc.lister(mois=mois, proprietaire_id=prop_id, db_path=db_path)
                          if f["logement_id"] == source.get("logement_id")
                          and f["statut"] != svc.ST_ANNULE]
            if existantes:
                statut, detail = "NON_CONCERNE", (
                    f"facture {existantes[0]['facture_id_opaque']} deja existante "
                    f"({existantes[0]['statut']})")
            elif not apercu["lignes"]:
                statut, detail = "NON_CONCERNE", "aucun montant a facturer"
            elif apercu["controles"]:
                statut, detail = "A_CONTROLER", "; ".join(
                    c["message"] for c in apercu["controles"])
            else:
                statut, detail = "PRETE", ""

            propositions.append({
                "proprietaire_id": prop_id,
                "logement_id": source.get("logement_id"),
                "mois": mois,
                "source_calcul": source.get("source_calcul"),
                "lignes": apercu["lignes"],
                "montant_total": apercu["montant_total"],
                "releve_informatif": apercu["releve_informatif"],
                "statut_proposition": statut,
                "detail": detail,
                "source": source,
            })
    return propositions


def creer_lot(propositions: list[dict[str, Any]], *, acteur: str = "", db_path=None) -> dict:
    """Crée les BROUILLON des seules propositions PRETE. Rien d'autre n'est touché."""
    creees, ignorees, erreurs = [], [], []
    for p in propositions:
        if p["statut_proposition"] != "PRETE":
            ignorees.append({"proprietaire_id": p["proprietaire_id"],
                             "logement_id": p["logement_id"],
                             "statut": p["statut_proposition"], "detail": p["detail"]})
            continue
        try:
            f = svc.creer(p["source"], acteur=acteur, db_path=db_path)
            creees.append(f["facture_id_opaque"])
        except svc.FactureProprietaireError as exc:
            erreurs.append({"proprietaire_id": p["proprietaire_id"],
                            "logement_id": p["logement_id"], "erreur": str(exc)})
    return {"creees": creees, "ignorees": ignorees, "erreurs": erreurs}

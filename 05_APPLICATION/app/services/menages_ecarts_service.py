"""Écart ménages externes ↔ Hostaway — le calcul, en un seul endroit.

CE QUE C'EST
La comparaison, au grain mois × logement, entre le nombre de ménages FACTURÉS par un prestataire
externe et le nombre de ménages RÉALISÉS constaté côté Hostaway. C'est ce que produisait l'onglet
`VUE_ECART_HOSTAWAY` de `MASTER_FACT_MEN_MenagesExternes.xlsx` (Lot6c).

POURQUOI CE MODULE EXISTE
Deux consommateurs avaient besoin de cette comparaison :
  · `controles_lot11_service` (groupe 6f), qui en tire les CONSTATS agrégés — déjà en SQLite ;
  · `controles_detail_reader.ecarts_menages`, qui en tire le DÉTAIL affiché à l'écran — et qui
    lisait encore le classeur Lot6c.
Le calcul vivait donc en double : une version SQLite et un classeur produit par un moteur pandas.
Deux sources pour une même question, c'est la garantie qu'elles finiront par diverger — et pendant
ce temps l'écran affichait le dernier calcul legacy pendant que le contrôle, lui, était à jour.

La logique de classement (les quatre codes, les mêmes seuils, les mêmes libellés) est reprise TELLE
QUELLE de `lot6c_menages_externes.py` : elle n'est pas réinventée ici, seulement déplacée à l'unique
endroit d'où les deux consommateurs la lisent désormais.
"""
from __future__ import annotations

from typing import Any

CODE_ECART = "MENAGE_EXTERNE_ECART_HOSTAWAY"
CODE_HORS_HA = "MENAGE_EXTERNE_LOGEMENT_HORS_HA"
CODE_HA_SANS_FACTURE = "MENAGE_HA_SANS_FACTURE_EXTERNE"
CODE_RAPPROCHE = "MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY"

# Niveau et commentaire attachés à chaque code — vocabulaire du moteur, non réécrit.
_QUALIFICATION = {
    CODE_HORS_HA: ("A_CONTROLER",
                   "Logement facture absent du comptage Hostaway (archive/hors HA/mapping). "
                   "A valider, pas une erreur prestataire."),
    CODE_HA_SANS_FACTURE: ("INFO",
                           "Menages Hostaway sans facture externe : probable menage interne / "
                           "a croiser avec M04."),
    CODE_RAPPROCHE: ("INFO",
                     "Volume facture = volume Hostaway. Rapproche ; date jour non requise."),
    CODE_ECART: ("A_CONTROLER",
                 "Ecart volume facture vs Hostaway. A valider (interne, hors HA, decalage mois "
                 "ou saisie)."),
}


def _classer(nb_ext: float, nb_ha: float) -> str:
    """Règle de classement de `lot6c_menages_externes.py`, reprise à l'identique.

    Rend `""` quand il n'y a aucune activité ménage sur le mois : ce n'est pas un écart, il n'y a
    simplement rien à comparer.
    """
    if nb_ext == 0 and nb_ha == 0:
        return ""
    if nb_ext > 0 and nb_ha == 0:
        return CODE_HORS_HA
    if nb_ext == 0 and nb_ha > 0:
        return CODE_HA_SANS_FACTURE
    return CODE_RAPPROCHE if (nb_ext - nb_ha) == 0 else CODE_ECART


def calculer(db_path=None) -> list[dict[str, Any]]:
    """Une ligne par (mois × logement) portant une activité ménage, avec son code de contrôle.

    Colonnes rendues : celles de `VUE_ECART_HOSTAWAY`, pour que les consommateurs historiques
    n'aient rien à changer.
    """
    from app.readers import menages_reader
    from app.services import facture_lignes_menage_service as flm

    comptage = menages_reader.hostaway_comptage(db_path=db_path)
    vue_ha: dict[tuple, dict[str, Any]] = {}
    if comptage.etat.etat == "OK":
        for r in comptage.lignes:
            mois, logement = r.get("mois"), r.get("logement_id")
            if mois and logement:
                vue_ha[(mois, logement)] = {"nb_ha": r.get("nb_menages_realises") or 0,
                                            "prop": r.get("proprietaire_id")}

    ext_cpt: dict[tuple, float] = {}
    ext_prop: dict[tuple, Any] = {}
    ext_prest: dict[tuple, set] = {}
    for r in flm.lignes_externes_pour_reader(db_path=db_path):
        if r.get("statut_controle") == "BLOQUANT":
            continue
        logement, mois = r.get("logement_id"), r.get("mois")
        if not logement or logement == "A_CONTROLER" or not mois:
            continue
        cle = (mois, logement)
        ext_cpt[cle] = ext_cpt.get(cle, 0) + (r.get("nombre_menages") or 0)
        ext_prop[cle] = r.get("proprietaire_id")
        if r.get("nom_prestataire"):
            ext_prest.setdefault(cle, set()).add(str(r["nom_prestataire"]))

    lignes = []
    for cle in sorted(set(vue_ha) | set(ext_cpt)):
        mois, logement = cle
        nb_ha = vue_ha.get(cle, {}).get("nb_ha", 0) or 0
        nb_ext = ext_cpt.get(cle, 0)
        code = _classer(nb_ext, nb_ha)
        if not code:
            continue
        niveau, commentaire = _QUALIFICATION[code]
        lignes.append({
            "mois": mois,
            "logement_id": logement,
            "proprietaire_id": ext_prop.get(cle) or vue_ha.get(cle, {}).get("prop"),
            "prestataires_factures": ",".join(sorted(ext_prest.get(cle, set()))),
            "nombre_menages_facture": nb_ext,
            "nombre_menages_hostaway": nb_ha,
            "ecart": nb_ext - nb_ha,
            "code_controle": code,
            "niveau_controle": niveau,
            "commentaire_controle": commentaire,
        })
    return lignes


def par_code(code: str, db_path=None) -> list[dict[str, Any]]:
    """Les lignes d'un code donné — ce que `controles_detail_reader.ecarts_menages` expose."""
    return [l for l in calculer(db_path=db_path) if l["code_controle"] == code]


def logements_par_code(db_path=None) -> dict[str, list[str]]:
    """{code: [logement_id…]} — ce dont Lot11 a besoin pour ses constats agrégés."""
    groupes: dict[str, list[str]] = {}
    for l in calculer(db_path=db_path):
        groupes.setdefault(l["code_controle"], []).append(l["logement_id"])
    return groupes

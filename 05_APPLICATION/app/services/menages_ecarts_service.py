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

La logique de classement (les quatre codes, les mêmes seuils, les mêmes libellés) n'est plus
dupliquée ici : elle vit dans `lib_db_moteur.classer_ecart_menage_externe` (02_TRAVAIL), qui la
définit une seule fois pour les DEUX environnements — ce module applicatif ET le script moteur
`lot6c_menages_externes.py` (mode SQLite). Ce fichier importe cette règle plutôt que de la
recopier ; c'est la garantie qu'un classement ne peut plus diverger entre l'écran et le moteur.
"""
from __future__ import annotations

import sys
from typing import Any

import app.config as cfg

# `lib_db_moteur` vit dans 02_TRAVAIL, à côté du paquet `app` — même convention que
# `charges_preview_service.py` pour `lib_ref_history` (ancrée sur `APP_ROOT.parent`, jamais
# `cfg.PROJECT_ROOT`, qui peut être redirigé en test/recette).
_TRAVAIL_DIR = str(cfg.APP_ROOT.parent / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)
import lib_db_moteur as dbm  # noqa: E402

CODE_ECART = dbm.CODE_ECART_VOLUME
CODE_HORS_HA = dbm.CODE_ECART_HORS_HA
CODE_HA_SANS_FACTURE = dbm.CODE_ECART_HA_SANS_FACTURE
CODE_RAPPROCHE = dbm.CODE_ECART_RAPPROCHE

# Niveau et commentaire attachés à chaque code — vocabulaire du moteur, non réécrit. Ce n'est PAS
# de la logique de classement (elle est dans `lib_db_moteur`) : seulement l'habillage pour
# l'affichage, propre à ce lecteur applicatif.
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
    """Alias vers la règle canonique — conservé pour ne pas casser un import existant."""
    return dbm.classer_ecart_menage_externe(nb_ext, nb_ha)


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

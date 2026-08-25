"""Moteur commission pur — calcul de la commission conciergerie et du net proprietaire.

Extrait de `lot10_calculer_resultats.py` (Mission 7) : la formule etait dupliquee a l'identique
dans les 3 branches de routage (HOSTAWAY / HH / VRBO), chacune ayant deja resolu son assiette et
son taux de commission avant d'appliquer ce calcul (`resolve_commission_rate`,
`resolve_regle_version` + garde-fou `_verifier_version_regle`, Mission 6 ter — inchanges, non
touches ici). Ce module ne connait ni pandas, ni SQLite, ni FastAPI, ni les branches de routage :
il recoit assiette+taux et payout+menage+commission deja resolus, et retourne un resultat.

L'arithmetique est generique (aucun import pandas) : les fonctions marchent aussi bien sur des
scalaires Python que sur des `pandas.Series` (`round()` delegue a `Series.__round__`), c'est
pourquoi les 3 branches de Lot10 peuvent continuer a les appeler de facon vectorisee, sans boucle
Python explicite ni changement de performance.

CONTRAT
La resolution de la VERSION applicable (quelle regle s'applique a cette date economique) reste
hors du moteur — responsabilite de l'appelant, deja cablee (Mission 6 ter, `resolve_regle_version`
+ `_verifier_version_regle`, fail-closed inchange).

Mission 7 bis (audit assiette restante) : la derivation de l'assiette HOSTAWAY est un pur
pass-through de la valeur amont (Lot1/lot4quater, aucune decision ici, laissee dans Lot10 —
normalisation technique, pas une regle) ; celle des canaux de PAIEMENT DIRECT (HH, VRBO en repli)
EST une vraie regle economique (assiette = payout - menage) — extraite ici comme
`assiette_v1_paiement_direct`, partagee par les deux canaux qui l'appliquent identiquement.
"""
from __future__ import annotations


def calculer_commission_conciergerie(assiette, taux):
    """commission = assiette x taux, arrondie au centime.

    Formule V1 de la regle ASSIETTE_COMMISSION (deja versionnee et verifiee par
    `_verifier_version_regle` avant appel) — identique pour HOSTAWAY, HH et VRBO."""
    return round(assiette * taux, 2)


def calculer_net_proprietaire(payout, menage, commission):
    """net = payout - menage - commission, arrondi au centime.

    Les 3 branches ne passent pas les memes valeurs source : HOSTAWAY/VRBO passent
    `payout_calcule`/`menage_retenu` deja arrondis au centime ; HH passe `total_percu`/`menage`
    bruts (non arrondis avant ce calcul). C'est un comportement existant preserve tel quel par
    cette extraction, pas une regle nouvelle introduite ici — voir `MOTEUR_COMMISSION.md`."""
    return round(payout - menage - commission, 2)


def assiette_v1_paiement_direct(payout, menage):
    """assiette = payout - menage, arrondie au centime — formule V1 ASSIETTE_COMMISSION pour un
    canal de PAIEMENT DIRECT (HH, et VRBO en repli quand aucune assiette historique n'est deja
    resolue en amont) : le montant percu directement, diminue du menage. Contrairement a la
    branche HOSTAWAY (assiette fournie telle quelle par le payout amont Lot1/lot4quater, aucun
    calcul ici), c'est une VRAIE decision economique — extrait par Mission 7 bis apres audit
    (cf. `MOTEUR_COMMISSION.md` §assiette) car c'etait une regle metier reelle, pas une simple
    normalisation technique de colonnes heterogenes."""
    return round(payout - menage, 2)

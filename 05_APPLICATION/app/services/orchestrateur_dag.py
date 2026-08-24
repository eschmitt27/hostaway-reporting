"""DAG des datasets SQLite — la carte de dépendance réelle du pipeline.

CE FICHIER NE CALCULE RIEN. Il déclare QUOI dépend de QUOI, et par quel service se recalcule chaque
nœud. L'exécution est dans `orchestrateur_service`.

POURQUOI DES DATASETS ET PAS DES FICHIERS
Le pipeline raisonnait historiquement en classeurs (`MASTER_CALC_Flux.xlsx` → `MASTER_CALC_*.xlsx`).
Depuis la migration SQLite, la vérité est une TABLE, et la question « est-ce à jour ? » ne se répond
plus par une date de fichier mais par « quel run a produit ce dataset, et un amont a-t-il été
recalculé depuis ? ». Le DAG ci-dessous ne contient donc aucun nœud `MASTER_XLSX`.

TYPES DE NŒUDS
  SOURCE_EXTERNE       : donnée qui vient du dehors (API Hostaway, relevé bancaire, référentiel Setup).
                         L'orchestrateur ne la « calcule » pas : il l'IMPORTE.
  IMPORT_SQLITE        : l'import lui-même, qui matérialise la source externe en tables SQLite.
  CALCUL_SQLITE        : une transformation SQLite → SQLite (le cœur du pipeline).
  EXPORT_OPTIONNEL     : une sortie reconstructible qui ne sert à personne en amont (Lot13).

Un EXPORT_OPTIONNEL n'est jamais recalculé automatiquement par la propagation : le supprimer ne
casse rien, et le régénérer est une action explicite. C'est ce qui le distingue d'un dataset.
"""
from __future__ import annotations

from dataclasses import dataclass, field

TYPE_IMPORT = "IMPORT_SQLITE"
TYPE_CALCUL = "CALCUL_SQLITE"
TYPE_EXPORT = "EXPORT_OPTIONNEL"

# Datasets
HOSTAWAY_RAW = "HOSTAWAY_RAW"
HOSTAWAY_CLEANING_TASKS = "HOSTAWAY_CLEANING_TASKS"
REF_SETUP = "REF_SETUP"
BANQUE = "BANQUE"
RESERVATIONS = "RESERVATIONS"
MENAGES = "MENAGES"
FLUX_LOT9 = "FLUX_LOT9"
LOT10 = "LOT10"
LOT11 = "LOT11"
LOT12 = "LOT12"
LOT13_EXPORT = "LOT13_EXPORT"


@dataclass(frozen=True)
class Noeud:
    """Un dataset du pipeline.

    `service` désigne la fonction de recalcul, sous la forme "module:fonction" — résolue au moment
    de l'exécution (jamais importée ici, pour que ce fichier reste une carte, pas un point d'entrée
    qui tirerait la moitié de l'application).

    `service` vaut None quand le dataset ne se recalcule PAS tout seul : c'est le cas des imports
    dont la donnée vient d'une action extérieure (dépôt d'un relevé bancaire, import du référentiel Setup).
    L'orchestrateur les traite alors comme des points d'entrée : il constate leur état et propage
    aux descendants, mais ne fabrique jamais une donnée que personne ne lui a fournie.
    """
    nom: str
    type_noeud: str
    libelle: str
    depend_de: tuple[str, ...] = ()
    service: str | None = None
    tables: tuple[str, ...] = ()
    commentaire: str = ""
    # Ce nœud appelle-t-il un service EXTERNE (API tierce) ? Ces imports ne sont pas déclenchés par
    # un « Actualiser toute l'activité » ordinaire : ils consomment un quota, peuvent être limités
    # (429) et n'ont pas à partir à chaque recalcul interne. L'ordonnanceur et le bouton dédié les
    # demandent explicitement.
    externe: bool = False


NOEUDS: dict[str, Noeud] = {n.nom: n for n in (
    Noeud(HOSTAWAY_RAW, TYPE_IMPORT, "Hostaway — réservations, payouts, listings, anomalies",
          service="app.services.orchestrateur_moteur:importer_hostaway",
          externe=True,
          tables=("hostaway_extractions", "hostaway_reservations", "hostaway_payouts",
                  "hostaway_listings", "hostaway_anomalies"),
          commentaire="Source externe : API Hostaway. Passe par `hostaway_actualisation_service."
                      "actualiser` — le MÊME service que le bouton manuel et que l'ordonnanceur, "
                      "une seule implémentation. Attendu jusqu'au bout : un lancement n'est pas "
                      "un succès."),

    Noeud(HOSTAWAY_CLEANING_TASKS, TYPE_IMPORT, "Hostaway — tâches de ménage (H6)",
          depend_de=(HOSTAWAY_RAW,),
          tables=("hostaway_cleaning_tasks_extractions", "hostaway_cleaning_tasks"),
          commentaire="Cadence PROPRE, volontairement séparée de HOSTAWAY_RAW : H6 a rencontré des "
                      "limites 429 sévères, et le rafraîchir aussi souvent que les réservations "
                      "n'apporte rien. Jamais entraîné par la propagation automatique."),

    Noeud(REF_SETUP, TYPE_IMPORT, "Référentiel Setup (logements, propriétaires, taux, clôture)",
          tables=("ref_logements", "ref_proprietaires", "ref_taux_commission",
                  "ref_gestion_logements_hist", "ref_cloture_mensuelle",
                  "ref_couts_standards_menage", "ref_canape_parametres", "ref_regles_versions"),
          commentaire="Import du référentiel déclenché par l'utilisateur : l'orchestrateur "
                      "constate sa fraîcheur, il ne réimporte jamais de lui-même un fichier que "
                      "personne ne lui a désigné. `ref_couts_standards_menage`/`ref_canape_"
                      "parametres`/`ref_regles_versions` (Mission 6/6bis/6ter) partagent ce même "
                      "nœud : ce sont des référentiels historisés au même titre, jamais un import "
                      "Excel, mais leur modification doit invalider les mêmes descendants."),

    Noeud(BANQUE, TYPE_IMPORT, "Banque — mouvements normalisés et classification",
          tables=("banque_mouvements", "banque_classifications", "banque_controles"),
          commentaire="Import d'un relevé : action utilisateur. Même raison que REF_SETUP."),

    Noeud(RESERVATIONS, TYPE_CALCUL, "Réservations calculées puis résolues (Lot4bis/4quater)",
          depend_de=(HOSTAWAY_RAW, REF_SETUP),
          tables=("reservations_calculees", "reservations_resolues"),
          commentaire="CHAÎNE PARTIELLEMENT MIGRÉE : `lot4bis_charger_reservations.py` sait lire et "
                      "écrire SQLite (--source SQLITE), mais `lot4quater_source_resolue.py` n'a pas "
                      "encore de mode SQLite. La chaîne ne peut donc pas être rejouée de bout en "
                      "bout par l'orchestrateur : elle est déclarée sans service plutôt que "
                      "d'exécuter une moitié de calcul et de présenter le résultat comme complet."),

    Noeud(MENAGES, TYPE_CALCUL, "Ménages — comptage, déclarations, rapprochement, coût complet",
          depend_de=(HOSTAWAY_CLEANING_TASKS, REF_SETUP),
          tables=("menages_taches_enrichies", "menages_declarations_internes",
                  "menages_rapprochement", "menages_gainperte", "menages_cout_complet"),
          commentaire="CHAÎNE PARTIELLEMENT MIGRÉE : lot6a/6d/6e/6f acceptent --source SQLITE, mais "
                      "lot6b (M04) et lot6c (ménages externes) n'ont pas de mode SQLite. Même "
                      "raison que RESERVATIONS : pas de service, plutôt qu'un recalcul partiel "
                      "présenté comme complet. Le recalcul ménages reste `menages_recalcul_service`."),

    Noeud(FLUX_LOT9, TYPE_CALCUL, "Flux économique unifié (Lot9)",
          depend_de=(RESERVATIONS, MENAGES, BANQUE),
          service="app.services.flux_unifie_service:construire",
          tables=("flux_unifies",),
          commentaire="Fusionne RES/MEN/BNQ/CHG/GPM. CHARGES (Lot3) reste une frontière Excel "
                      "minimale, lue par `charges_reader` — pas un dataset SQLite à ce jour."),

    Noeud(LOT10, TYPE_CALCUL, "Résultats économiques (Lot10)",
          depend_de=(FLUX_LOT9,),
          service="app.services.orchestrateur_moteur:executer_lot10",
          tables=("lot10_runs", "lot10_commissions", "lot10_resultats", "lot10_net_exploitation",
                  "lot10_net_reglement", "lot10_net_vue_mois"),
          commentaire="Moteur pandas exécuté avec --source SQLITE : entrée `flux_unifies`, sorties "
                      "0044. Le calcul lui-même n'a pas été réécrit."),

    Noeud(LOT11, TYPE_CALCUL, "Contrôles de cohérence transverses (Lot11)",
          depend_de=(FLUX_LOT9, LOT10, RESERVATIONS, MENAGES, BANQUE, REF_SETUP, HOSTAWAY_RAW),
          service="app.services.controles_lot11_service:construire",
          tables=("controles_lot11_constats", "controles_lot11_constats_champs",
                  "controles_lot11_dashboard_mois"),
          commentaire="Dépend de presque tout : c'est sa raison d'être, contrôler la cohérence "
                      "ENTRE les chaînes."),

    Noeud(LOT12, TYPE_CALCUL, "Préfactures propriétaires (Lot12)",
          depend_de=(LOT10, LOT11, REF_SETUP),
          service="app.services.lot12_prefactures_service:construire",
          tables=("lot12_runs", "lot12_prefactures_entete", "lot12_prefactures_lignes",
                  "lot12_dashboard_facturation", "lot12_a_controler"),
          commentaire="PRÉFACTURES uniquement — jamais une facture émise. La facturation "
                      "propriétaire réelle est un autre chemin, hors DAG de calcul."),

    Noeud(LOT13_EXPORT, TYPE_EXPORT, "Exports Power BI (Lot13)",
          depend_de=(FLUX_LOT9, LOT10, LOT11, LOT12, RESERVATIONS, MENAGES, REF_SETUP),
          service="app.services.lot13_export_service:exporter",
          tables=(),
          commentaire="Terminal : personne ne lit ses sorties en amont. Reconstructible à tout "
                      "moment depuis SQLite, donc jamais entraîné par la propagation automatique."),
)}


def descendants(dataset: str) -> list[str]:
    """Tous les datasets qui dépendent (directement ou non) de `dataset`, en ordre topologique.

    C'est ce qui permet de répondre à « j'ai modifié les Ménages, que faut-il recalculer ? » sans
    que personne n'ait à connaître la chaîne par cœur.
    """
    atteints: set[str] = set()
    a_voir = [dataset]
    while a_voir:
        courant = a_voir.pop()
        for nom, noeud in NOEUDS.items():
            if courant in noeud.depend_de and nom not in atteints:
                atteints.add(nom)
                a_voir.append(nom)
    return [n for n in ordre_topologique() if n in atteints]


def ascendants(dataset: str) -> list[str]:
    """Tous les datasets dont `dataset` dépend, directement ou non."""
    atteints: set[str] = set()
    a_voir = list(NOEUDS[dataset].depend_de)
    while a_voir:
        courant = a_voir.pop()
        if courant in atteints:
            continue
        atteints.add(courant)
        a_voir.extend(NOEUDS[courant].depend_de)
    return [n for n in ordre_topologique() if n in atteints]


def ordre_topologique() -> list[str]:
    """Ordre d'exécution : un nœud n'apparaît jamais avant ceux dont il dépend.

    Tri déterministe (les nœuds prêts sont pris par ordre alphabétique) — deux exécutions produisent
    le même ordre, ce qui rend les journaux comparables d'un run à l'autre.
    """
    restants = {nom: set(n.depend_de) for nom, n in NOEUDS.items()}
    ordre: list[str] = []
    while restants:
        prets = sorted(nom for nom, deps in restants.items() if not deps - set(ordre))
        if not prets:
            # Impossible avec le DAG déclaré ; un cycle introduit par erreur doit être visible tout
            # de suite, jamais produire un ordre partiel silencieux.
            raise ValueError(f"Cycle dans le DAG : {sorted(restants)}")
        for nom in prets:
            ordre.append(nom)
            del restants[nom]
    return ordre


def noeuds_calculables() -> list[str]:
    """Datasets que l'orchestrateur sait recalculer lui-même (hors exports optionnels)."""
    return [n for n in ordre_topologique()
            if NOEUDS[n].type_noeud != TYPE_EXPORT]

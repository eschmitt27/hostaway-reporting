"""Lot13 — exports Power BI, SQLite natif (aucune migration).

Port fidèle de `02_TRAVAIL/lot13_export_powerbi.py` : produit les mêmes 13 CSV `PBI_*.csv` plus
`PBI_Dictionnaire_Colonnes.csv` dans `03_EXPORTS/PowerBI/`, mais en lisant les tables SQLite
migrées (Lot9/Lot10/Lot11/Lot12, Ménages, référentiel 0029) au lieu des classeurs `MASTER_*`.

LOT13 EST UN TERMINUS, PAS UN ÉTAGE
Le sens de circulation est strictement `SQLite canonique → Lot13 → CSV`. Aucun service applicatif
ne lit `03_EXPORTS/` : l'export est une sortie destinée à Power BI et à l'utilisateur, jamais une
source. C'est ce qui permet de supprimer un export sans rien casser, et de le régénérer à
l'identique depuis la base (vérifié par `tests/test_lot13_export_sqlite.py`).

POURQUOI AUCUNE TABLE `lot13_runs`
Lot10/Lot11/Lot12 versionnent leurs datasets parce que leurs sorties sont CONSOMMÉES en aval : un
demi-calcul actif corromprait le lot suivant. Un export Lot13 n'est consommé par rien dans le
système — c'est un artefact terminal, entièrement reconstructible depuis la base. Lui donner un
registre de runs `actif=1` créerait une notion de « version d'export courante » que rien ne lit, et
un quatrième registre de runs sans justification (cf. §26 de la mission : ne pas créer un énième
registre). Le résultat de `exporter()` porte le compte-rendu ; la fraîcheur réelle se lit sur les
datasets SOURCES, qui eux sont versionnés.

CE QUI EST REPRIS TEL QUEL DU LEGACY
Les 13 noms d'export, leurs listes blanches de colonnes ET LEUR ORDRE, le format CSV (UTF-8 BOM,
séparateur `;`), le dictionnaire de colonnes, le filet anti-sensible `SENSIBLE` avec son ABORT, et
la table `RENOMMAGES_EXPORT`. Aucune règle métier n'est recalculée ici : chaque export est une
projection de colonnes d'une table déjà calculée.
"""
from __future__ import annotations

import csv
import datetime
import io
import re
from pathlib import Path
from typing import Any, Callable

import app.config as cfg
from app.db.connection import get_db

# Filet anti-sensible : motifs interdits dans les NOMS de colonnes exportées. Repris à l'identique
# du legacy — l'affaiblir laisserait sortir une donnée personnelle sans que personne ne le voie.
SENSIBLE = re.compile(r"(e[-_ ]?mail|^mail|t[ée]l[ée]?phone|^tel$|iban|rib|adresse|secret|token|password|"
                      r"guest|voyageur|libelle_brut|libelle_banque|compte_bancaire|num_compte)", re.I)

# Renommages appliqués À LA FRONTIÈRE D'EXPORT uniquement : la colonne garde son nom historique en
# base (lot10 la produit, lot11 la contrôle), seul le CSV Power BI voit le nouveau nom.
#
# `preparation_canape_voyageurs` porte un MONTANT (supplément de préparation du canapé lorsque le
# nombre de voyageurs l'impose), jamais une identité. Son nom déclenchait pourtant le motif
# `voyageur` du filet ci-dessus, qui interrompait tout l'export. Plutôt que d'affaiblir le filet ou
# de supprimer la donnée, l'export expose `montant_preparation_canape`.
#
# Cette table est délibérément minuscule et fermée : un renommage permet, par construction, de
# faire sortir une colonne qui serait autrement refusée. Toute entrée supplémentaire doit être une
# décision explicite et documentée — un test la verrouille.
RENOMMAGES_EXPORT = {
    "PBI_Commissions": {"preparation_canape_voyageurs": "montant_preparation_canape"},
}

STATUT_OK = "OK"
STATUT_SOURCE_ABSENTE = "SOURCE_ABSENTE"

E_COLONNE_SENSIBLE = "COLONNE_SENSIBLE"

NOM_DICTIONNAIRE = "PBI_Dictionnaire_Colonnes"


def _run_actif(conn, table_runs: str) -> str | None:
    """`run_id` du dataset actif, ou None si la table/le run n'existe pas — jamais une exception."""
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_runs,)).fetchone() is None:
        return None
    row = conn.execute(f"SELECT run_id FROM {table_runs} WHERE actif = 1").fetchone()
    return row[0] if row else None


def _table_absente(conn, table: str) -> bool:
    return conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone() is None


def _lire_table(conn, table: str, colonnes: list[str], *, run_table: str | None = None,
                where: str = "", ordre: str = "id") -> list[dict[str, Any]] | None:
    """Projection des colonnes demandées, éventuellement restreinte au run actif.

    Retourne None si la table n'existe pas (équivalent SQLite du `SOURCE_ABSENTE` legacy, qui
    testait l'existence du classeur) — une liste vide signifie « table présente, aucune ligne ».
    """
    if _table_absente(conn, table):
        return None
    clauses = []
    params: list[Any] = []
    if run_table is not None:
        run_id = _run_actif(conn, run_table)
        if run_id is None:
            return None
        clauses.append("run_id = ?")
        params.append(run_id)
    if where:
        clauses.append(where)
    sql = f"SELECT {', '.join(colonnes)} FROM {table}"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += f" ORDER BY {ordre}"
    return [dict(zip(colonnes, r)) for r in conn.execute(sql, params).fetchall()]


# ── Définition des 13 exports ───────────────────────────────────────────────────────────────────
#
# (nom_export, fonction de lecture). Les listes blanches et leur ORDRE sont ceux du legacy : Power
# BI lit ces colonnes dans cet ordre, en changer un casserait un rapport existant.

WL_FLUX = ["flux_id", "mois", "date_flux", "logement_id", "proprietaire_id", "associe_id",
           "type_flux_id", "sens", "montant", "code_impact", "inclure_resultat_reel",
           "inclure_resultat_comptable", "inclure_resultat_hors_compta", "statut_controle",
           "niveau_anomalie", "code_anomalie"]

WL_RESULTATS = ["mois", "logement_id", "proprietaire_id", "vision", "total_produits",
                "total_charges", "resultat", "nb_flux"]

WL_RESERVATIONS = ["reservation_calc_id", "reservation_id_hostaway", "reservation_hh_id", "canal",
                   "source", "mois", "logement_id", "proprietaire_id", "date_arrivee",
                   "date_depart", "nuits", "montant_retenu", "code_impact", "etat_mois",
                   "origine_initiale", "statut_controle", "niveau_anomalie", "code_anomalie"]

WL_COMMISSIONS = ["reservation_calc_id", "reservation_id_hostaway", "logement_id",
                  "proprietaire_id", "mois", "date_arrivee", "date_depart", "nuits",
                  "channel_type", "source_type", "statut_calcul_payout", "payout_calcule",
                  "menage_retenu", "assiette_commission", "taux_commission",
                  "commission_conciergerie", "preparation_canape_voyageurs",
                  "controle_preparation_canape", "net_proprietaire"]

WL_NET_PROP = ["mois", "proprietaire_id", "total_payout_mois", "total_menage_mois",
               "total_commission_mois", "charge_fixe_mensuelle", "total_preparation_canape_mois",
               "montant_du_conciergerie", "reste_a_payer_conciergerie",
               "net_proprietaire_avant_charge_mois", "net_proprietaire_apres_charge_mois",
               "nb_reservations"]

WL_PREFACTURES = ["facture_id", "ligne_num", "type_ligne", "libelle", "montant", "bloc"]

WL_COUT_COMPLET = ["mois", "logement_id", "proprietaire_id", "type_logement_id", "intervenant_id",
                   "type_intervenant", "nb_menages", "cout_standard_total", "cout_direct_total",
                   "cout_complet_total", "ecart_vs_standard_total", "ecart_unitaire",
                   "statut_ecart", "statut_controle"]

WL_RAPPROCHEMENT = ["mois", "logement_id", "proprietaire_id", "intervenant_id", "type_intervenant",
                    "nb_menages_tasks_hostaway_completed", "nb_menages_declares_externe",
                    "nb_menages_declares_interne_m04", "total_menages_declares", "ecart",
                    "statut_controle", "code_controle"]

WL_CONTROLES = ["ctrl_pk", "code_controle", "severity", "mois", "logement_id", "proprietaire_id",
                "message", "statut_resolution"]

WL_REF_LOGEMENTS = ["logement_id", "nom_logement_officiel", "nom_court", "ville",
                    "type_logement_id", "sur_hostaway", "actif", "statut_parc",
                    "forfait_logiciel_consommables_mensuel"]

WL_REF_GESTION = ["gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
                  "statut_gestion", "source"]

WL_REF_PROPRIETAIRES = ["proprietaire_id", "nom_proprietaire", "mode_facturation", "actif"]

WL_REF_TAUX = ["taux_commission_id", "proprietaire_id", "logement_id", "taux_commission",
               "date_debut", "date_fin", "actif"]


def _controles_ouverts(conn, colonnes: list[str], db_path) -> list[dict[str, Any]] | None:
    """A_CONTROLER_OUVERTS — même filtre que `lot11_controles_coherence.py` :
    `severity == 'A_CONTROLER'` ET `statut_resolution == 'OUVERT'`.

    `mois`/`logement_id`/`proprietaire_id` vivent dans l'extension 1-1
    `controles_lot11_constats_champs` (0042) : la jointure est donc en LEFT JOIN — un constat sans
    ligne d'extension doit apparaître avec ses champs vides, jamais disparaître de l'export.
    """
    if _table_absente(conn, "controles_lot11_constats"):
        return None
    champs_dispo = not _table_absente(conn, "controles_lot11_constats_champs")
    if champs_dispo:
        sql = ("SELECT c.ctrl_pk, c.code_controle, c.severity, f.mois, f.logement_id, "
               "f.proprietaire_id, c.message, c.statut_resolution "
               "FROM controles_lot11_constats c "
               "LEFT JOIN controles_lot11_constats_champs f ON f.ctrl_pk = c.ctrl_pk "
               "WHERE c.severity = 'A_CONTROLER' AND c.statut_resolution = 'OUVERT' "
               "ORDER BY c.id")
    else:
        sql = ("SELECT ctrl_pk, code_controle, severity, NULL, NULL, NULL, message, "
               "statut_resolution FROM controles_lot11_constats "
               "WHERE severity = 'A_CONTROLER' AND statut_resolution = 'OUVERT' ORDER BY id")
    return [dict(zip(colonnes, r)) for r in conn.execute(sql).fetchall()]


def _ref(conn, table: str, colonnes: list[str], ordre: str) -> list[dict[str, Any]] | None:
    """Référentiel Setup (migration 0029). Ordre explicite : ces tables n'ont pas de colonne `id`."""
    return _lire_table(conn, table, colonnes, ordre=ordre)


# Chaque entrée : (nom, whitelist, lecteur). Le lecteur reçoit (conn, whitelist, db_path).
EXPORTS: list[tuple[str, list[str], Callable]] = [
    ("PBI_Flux", WL_FLUX,
     lambda c, wl, p: _lire_table(c, "flux_unifies", wl, ordre="rowid")),
    ("PBI_Resultats_Mensuels", WL_RESULTATS,
     lambda c, wl, p: _lire_table(c, "lot10_resultats", wl, run_table="lot10_runs")),
    ("PBI_Reservations_Resolues", WL_RESERVATIONS,
     lambda c, wl, p: _reservations_resolues(c, wl, p)),
    ("PBI_Commissions", WL_COMMISSIONS,
     lambda c, wl, p: _lire_table(c, "lot10_commissions", wl, run_table="lot10_runs")),
    ("PBI_Net_Proprietaire", WL_NET_PROP,
     lambda c, wl, p: _lire_table(c, "lot10_net_vue_mois", wl, run_table="lot10_runs")),
    ("PBI_Prefactures_Proprietaires", WL_PREFACTURES,
     lambda c, wl, p: _lire_table(c, "lot12_prefactures_lignes", wl, run_table="lot12_runs")),
    ("PBI_Menages_Cout_Complet", WL_COUT_COMPLET,
     lambda c, wl, p: _lire_table(c, "menages_cout_complet", wl)),
    ("PBI_Menages_Rapprochement", WL_RAPPROCHEMENT,
     lambda c, wl, p: _lire_table(c, "menages_rapprochement", wl)),
    ("PBI_Controles_Ouverts", WL_CONTROLES,
     lambda c, wl, p: _controles_ouverts(c, wl, p)),
    ("PBI_Referentiel_Logements", WL_REF_LOGEMENTS,
     lambda c, wl, p: _ref(c, "ref_logements", wl, "logement_id")),
    ("PBI_Referentiel_Gestion_Logements", WL_REF_GESTION,
     lambda c, wl, p: _ref(c, "ref_gestion_logements_hist", wl, "gestion_id")),
    ("PBI_Referentiel_Proprietaires", WL_REF_PROPRIETAIRES,
     lambda c, wl, p: _ref(c, "ref_proprietaires", wl, "proprietaire_id")),
    ("PBI_Referentiel_Taux_Commission", WL_REF_TAUX,
     lambda c, wl, p: _ref(c, "ref_taux_commission", wl, "taux_commission_id")),
]


def _reservations_resolues(conn, colonnes: list[str], db_path) -> list[dict[str, Any]] | None:
    """Réservations résolues (Lot4quater) — dataset ACTIF, via le registre `reservations_datasets`.

    Passe par `reservations_dataset_service` plutôt que par un SELECT direct : c'est lui qui sait
    quel dataset est actif pour l'étape RESOLUES, et le dupliquer ici ferait deux définitions du
    « jeu courant ».
    """
    from app.services import reservations_dataset_service as res_ds

    if _table_absente(conn, "reservations_resolues"):
        return None
    # Aucun dataset actif = source absente ; un dataset actif mais VIDE reste une source présente
    # (CSV réduit à son en-tête), exactement comme le legacy écrivait un classeur sans ligne.
    if res_ds.dataset_courant(res_ds.ETAPE_RESOLUES, db_path=db_path) is None:
        return None
    lignes = res_ds.lignes(res_ds.ETAPE_RESOLUES, db_path=db_path)
    return [{c: l.get(c) for c in colonnes} for l in lignes]


def _val(v) -> str:
    """Rendu CSV d'une valeur, identique au legacy (dates tronquées à 10 caractères).

    UN NOMBRE ENTIER S'ÉCRIT SANS DÉCIMALE. Le legacy lisait Excel, où openpyxl rend un entier
    quand la cellule en contient un : il n'écrit jamais `.0` (vérifié sur les 110 135 cellules des
    13 exports). En SQLite ces colonnes sont des REAL, donc `str(58.0)` donnerait `58.0` et
    changerait la valeur telle que Power BI la lit sur TOUTES les colonnes de comptage et de
    montant rond. On restitue donc la forme entière quand le nombre est entier — c'est un rendu,
    jamais un arrondi : une valeur non entière garde toutes ses décimales.
    """
    if v is None:
        return ""
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)[:10]
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def repertoire_exports() -> Path:
    """Résolu à l'appel, jamais figé à l'import : `cfg.EXPORTS_POWERBI` peut être monkeypatché
    (tests, instance APP_DATA_DIR distincte)."""
    return Path(cfg.EXPORTS_POWERBI)


def _csv(entetes: list[str], corps) -> bytes:
    """Un CSV au format du legacy (UTF-8 BOM, `;`, fins de ligne du module csv), en mémoire.

    C'est l'UNIQUE rendu : le fichier écrit sur disque par `exporter()` et celui téléchargé depuis
    l'écran sont les mêmes octets, jamais deux implémentations qui pourraient diverger.
    """
    tampon = io.StringIO(newline="")
    w = csv.writer(tampon, delimiter=";")
    w.writerow(entetes)
    for ligne in corps:
        w.writerow(ligne)
    return tampon.getvalue().encode("utf-8-sig")


def produire(*, db_path=None) -> dict[str, Any]:
    """Construit les 13 CSV + le dictionnaire EN MÉMOIRE, depuis les datasets actifs de l'instant.

    Aucun fichier n'est lu ni écrit : c'est ce que sert l'écran « Exporter les données » au moment
    du clic. Les datasets versionnés (Lot10/Lot12/réservations) sont lus sur leur run `actif=1` ;
    les autres tables sont remplacées par leur service en UNE transaction. Un recalcul en cours ou
    en échec n'est donc jamais visible ici : l'export porte la dernière version valide.

    ABORT (comme le legacy) si une colonne sensible apparaît : aucun contenu n'est rendu.
    """
    conn = get_db(db_path)
    try:
        prepares: list[tuple[str, list[str], list[str], list[dict]]] = []
        rapport: list[dict[str, Any]] = []
        abort_msgs: list[str] = []

        for nom, whitelist, lecteur in EXPORTS:
            lignes = lecteur(conn, whitelist, db_path)
            if lignes is None:
                rapport.append({"export": nom, "statut": STATUT_SOURCE_ABSENTE, "nb_lignes": 0})
                continue
            renommages = RENOMMAGES_EXPORT.get(nom, {})
            entetes = [renommages.get(c, c) for c in whitelist]
            sensibles = [n for n in entetes if SENSIBLE.search(n)]
            if sensibles:
                abort_msgs.append(f"{nom}: colonne sensible détectée {sensibles}")
                continue
            prepares.append((nom, whitelist, entetes, lignes))
            rapport.append({"export": nom, "statut": STATUT_OK, "nb_lignes": len(lignes)})
    finally:
        conn.close()

    if abort_msgs:
        # Rien n'est rendu : un export partiel laisserait croire que la campagne a réussi.
        return {"ok": False, "code": E_COLONNE_SENSIBLE, "message": " | ".join(abort_msgs),
                "rapport": rapport}

    fichiers: dict[str, bytes] = {}
    dico: list[tuple[str, str]] = []
    for nom, whitelist, entetes, lignes in prepares:
        fichiers[f"{nom}.csv"] = _csv(entetes, ([_val(r.get(c)) for c in whitelist]
                                                 for r in lignes))
        dico.extend((nom, c) for c in entetes)
    fichiers[f"{NOM_DICTIONNAIRE}.csv"] = _csv(["table", "colonne"], dico)

    return {"ok": True, "fichiers": fichiers, "nb_exports": len(prepares),
            "nb_colonnes_dictionnaire": len(dico), "rapport": rapport}


def exporter(*, db_path=None, destination: Path | None = None) -> dict[str, Any]:
    """Écrit les 13 CSV + le dictionnaire depuis SQLite dans le dossier Power BI. Ne lit aucun
    classeur. Mêmes octets que `produire()`, qui les construit.

    ABORT (comme le legacy) si une colonne sensible apparaît dans une sortie : dans ce cas AUCUN
    fichier n'est écrit — mieux vaut pas d'export du tout qu'un export qui fuit.
    """
    sortie = Path(destination) if destination is not None else repertoire_exports()
    resultat = produire(db_path=db_path)
    if not resultat["ok"]:
        return resultat

    sortie.mkdir(parents=True, exist_ok=True)
    for nom_fichier, contenu in resultat["fichiers"].items():
        (sortie / nom_fichier).write_bytes(contenu)

    return {"ok": True, "destination": str(sortie), "nb_exports": resultat["nb_exports"],
            "nb_colonnes_dictionnaire": resultat["nb_colonnes_dictionnaire"],
            "rapport": resultat["rapport"]}


# ── Fraîcheur : de quand date la version que l'export contient ? ────────────────────────────────
#
# Rien n'est stocké ici : la date se lit sur la donnée elle-même (run actif, ou `date_calcul` des
# lignes), l'état sur le registre de l'orchestrateur (`orchestrateur_datasets`). C'est le seul
# système de fraîcheur de l'application ; l'export n'en a pas de second.

def _date_run_actif(conn, table_runs: str) -> str | None:
    if _table_absente(conn, table_runs):
        return None
    r = conn.execute(f"SELECT date_calcul FROM {table_runs} WHERE actif = 1").fetchone()
    return r[0] if r else None


def _date_max(conn, table: str, where: str = "") -> str | None:
    if _table_absente(conn, table):
        return None
    sql = f"SELECT MAX(date_calcul) FROM {table}" + (f" WHERE {where}" if where else "")
    return conn.execute(sql).fetchone()[0]


def _date_reservations(conn) -> str | None:
    if _table_absente(conn, "reservations_datasets"):
        return None
    r = conn.execute("SELECT date_calcul FROM reservations_datasets "
                     "WHERE etape = 'RESOLUES' AND actif = 1").fetchone()
    return r[0] if r else None


# Pour chaque export : la table lue, le nœud du DAG qui la produit, et la lecture de la date de
# génération de la version exportée. Date `None` = table de référence lue en direct (saisie ou
# import), qui n'a pas de « génération » : on ne lui en invente pas une.
SOURCES_EXPORT: dict[str, tuple[str, str, Callable[[Any], str | None] | None]] = {
    "PBI_Flux": ("flux_unifies", "FLUX_LOT9", lambda c: _date_max(c, "flux_unifies")),
    "PBI_Resultats_Mensuels": ("lot10_resultats (run actif)", "LOT10",
                               lambda c: _date_run_actif(c, "lot10_runs")),
    "PBI_Reservations_Resolues": ("reservations_resolues (dataset actif)", "RESERVATIONS",
                                  _date_reservations),
    "PBI_Commissions": ("lot10_commissions (run actif)", "LOT10",
                        lambda c: _date_run_actif(c, "lot10_runs")),
    "PBI_Net_Proprietaire": ("lot10_net_vue_mois (run actif)", "LOT10",
                             lambda c: _date_run_actif(c, "lot10_runs")),
    "PBI_Prefactures_Proprietaires": ("lot12_prefactures_lignes (run actif)", "LOT12",
                                      lambda c: _date_run_actif(c, "lot12_runs")),
    "PBI_Menages_Cout_Complet": ("menages_cout_complet", "MENAGES",
                                 lambda c: _date_max(c, "menages_cout_complet")),
    "PBI_Menages_Rapprochement": ("menages_rapprochement", "MENAGES",
                                  lambda c: _date_max(c, "menages_rapprochement")),
    # Les constats ne sont remplacés que par un run RÉUSSI : un run en échec n'y touche pas.
    "PBI_Controles_Ouverts": ("controles_lot11_constats", "LOT11",
                              lambda c: _date_max(c, "controles_lot11_runs",
                                                  "statut = 'SUCCES'")),
    "PBI_Referentiel_Logements": ("ref_logements", "REF_SETUP", None),
    "PBI_Referentiel_Gestion_Logements": ("ref_gestion_logements_hist", "REF_SETUP", None),
    "PBI_Referentiel_Proprietaires": ("ref_proprietaires", "REF_SETUP", None),
    "PBI_Referentiel_Taux_Commission": ("ref_taux_commission", "REF_SETUP", None),
}


def inventaire(*, db_path=None) -> dict[str, Any]:
    """Ce que l'écran affiche : chaque export tel qu'il serait produit À CET INSTANT.

    Volumétrie et date lues sur les datasets actifs, état lu sur l'orchestrateur — seulement pour
    les nœuds CALCULÉS, seuls à porter un état fiable (un référentiel est une table de saisie :
    lui prêter un « jamais calculé » serait faux).
    """
    from app.services import orchestrateur_dag as dag
    from app.services import orchestrateur_service as orch

    resultat = produire(db_path=db_path)
    rapport = {r["export"]: r for r in resultat.get("rapport", [])}
    etats = {e["dataset"]: e for e in orch.etat_datasets(db_path)}
    conn = get_db(db_path)
    try:
        exports = []
        for nom, _, _ in EXPORTS:
            table, dataset, lire_date = SOURCES_EXPORT[nom]
            noeud = dag.NOEUDS[dataset]
            r = rapport.get(nom, {})
            calcule = noeud.type_noeud == dag.TYPE_CALCUL
            etat = etats.get(dataset, {}) if calcule else {}
            exports.append({
                "nom": nom,
                "fichier": f"{nom}.csv",
                "table": table,
                "source": noeud.nom_affiche,
                "disponible": r.get("statut") == STATUT_OK,
                "nb_lignes": r.get("nb_lignes", 0),
                # Dernier recalcul RÉUSSI de la source (conservé à travers un échec). Il peut être
                # plus récent que `genere_le` : un recalcul qui ne change rien ne réécrit pas ses
                # lignes — la donnée est alors à jour, sans être neuve.
                "actualise_le": etat.get("calcule_le"),
                "genere_le": lire_date(conn) if lire_date else None,
                "lu_en_direct": lire_date is None,
                "etat": etat.get("statut"),
            })
    finally:
        conn.close()
    return {"ok": resultat["ok"], "message": resultat.get("message", ""), "exports": exports}

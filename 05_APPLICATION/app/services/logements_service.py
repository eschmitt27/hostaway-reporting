"""Service métier logements — LECTURE SEULE, AUCUN CALCUL.

SOURCE : SQLITE UNIQUEMENT
Ce service lisait deux exports CSV du moteur (`PBI_Referentiel_Logements.csv` et
`PBI_Referentiel_Gestion_Logements.csv`) et enrichissait la fiche depuis `REF_Setup.xlsm`. Les
trois lectures sont supprimées : le référentiel vit en base depuis la migration 0029, et faire
transiter une donnée par `SQLite → CSV → application` était précisément la boucle que
l'architecture interdit.

Il ne reste donc AUCUNE dépendance à un fichier dans ce module.

Arbitrages conservés à l'identique :
- Propriétaire : résolu depuis `ref_gestion_logements_hist` en s'appuyant sur `statut_gestion`,
  champ produit PAR LE MOTEUR. Aucune comparaison de dates n'est refaite ici, et un rattachement
  ambigu n'est jamais tranché arbitrairement.
- Commission, coûts de ménage, libellé de type : lignes BRUTES, aucun tri, aucune notion
  d'« actuel ».
- Lignes techniques (logement_id ne commençant pas par LOG_) : exclues par défaut, jamais masquées.

FAIL-CLOSED
Si le référentiel n'a jamais été importé, ce service le DIT — il ne se rabat pas sur le classeur.
« Référentiel absent » et « référentiel vide » appellent des actions opposées ; les confondre
enverrait l'utilisateur chercher un problème de données là où il manque un import.
"""
from datetime import datetime
from typing import Any
from app.services import referentiel_service as referentiel

SOURCE_NAME = "Référentiel SQLite (ref_logements)"
SOURCE_GESTION_NAME = "Référentiel SQLite (ref_gestion_logements_hist)"
_LOG_PREFIX = "LOG_"
_STATUT_ACTIF = "ACTIF"

# Résolutions possibles du rattachement, exposées telles quelles à l'écran.
GESTION_RESOLU = "RESOLU"              # exactement une ligne ACTIF
GESTION_CLOS = "CLOS"                  # aucune ligne active, une seule ligne connue (gestion terminée)
GESTION_A_CONTROLER = "A_CONTROLER"    # plusieurs actifs, ou plusieurs lignes closes : arbitrage humain
GESTION_ABSENT = "ABSENT"              # aucun rattachement exporté pour ce logement
GESTION_SOURCE_ABSENTE = "SOURCE_ABSENTE"  # export de gestion non généré (Lot13 non lancé)


def _lire_rattachements(db_path=None) -> dict[str, list[dict[str, Any]]] | None:
    """{logement_id: [lignes de gestion]}. None si le référentiel n'est pas initialisé."""
    if not referentiel.disponible(db_path=db_path):
        return None
    return referentiel.gestion_par_logement(db_path=db_path)


def _resoudre_gestion(lignes: list[dict[str, Any]] | None) -> tuple[str, str]:
    """(proprietaire_id, statut de résolution). Chaîne vide dès que le rattachement n'est pas certain."""
    if lignes is None:
        return "", GESTION_SOURCE_ABSENTE
    if not lignes:
        return "", GESTION_ABSENT
    actifs = [l for l in lignes
              if str(l.get("statut_gestion") or "").strip().upper() == _STATUT_ACTIF]
    if len(actifs) == 1:
        return str(actifs[0].get("proprietaire_id") or "").strip(), GESTION_RESOLU
    if not actifs and len(lignes) == 1:
        return str(lignes[0].get("proprietaire_id") or "").strip(), GESTION_CLOS
    return "", GESTION_A_CONTROLER


def _enrichir_gestion(rows: list[dict[str, Any]],
                      par_logement: dict[str, list[dict[str, Any]]] | None) -> None:
    """Pose `proprietaire_id` / `gestion_statut` sur chaque ligne de la liste, en place."""
    for r in rows:
        lid = str(r.get("logement_id") or "").strip()
        lignes = None if par_logement is None else par_logement.get(lid, [])
        prop, statut = _resoudre_gestion(lignes)
        r["proprietaire_id"] = prop
        r["gestion_statut"] = statut


def _is_technique(logement_id: str) -> bool:
    """Ligne technique = identifiant hors nomenclature parc (APPARTEMENT_DIVERS / LOGEMENT_DIVERS)."""
    return not str(logement_id or "").strip().startswith(_LOG_PREFIX)


def _read_at() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_list(
    q: str = "",
    ville: str = "",
    type_logement_id: str = "",
    proprietaire_id: str = "",
    actif: str = "",
    include_technique: bool = False,
    db_path=None,
) -> dict[str, Any]:
    """Charge la liste depuis le référentiel SQLite. État structuré, jamais d'exception nue."""
    read_at = _read_at()

    # Fail-closed : référentiel jamais importé. Distinct d'un référentiel vide.
    if not referentiel.disponible(db_path=db_path):
        return {
            "status": "ERROR",
            "code": referentiel.REFERENTIEL_ABSENT,
            "error_message": referentiel.MESSAGE_ABSENT,
            "source": SOURCE_NAME,
            "read_at": read_at,
            "rows": [], "count_parc": 0, "count_technique": 0,
            "filters": _empty_filters(),
        }

    rows = referentiel.logements(db_path=db_path)

    if not rows:
        return {
            "status": "ERROR",
            "error_message": "Le référentiel a été importé mais ne contient aucun logement.",
            "source": SOURCE_NAME,
            "read_at": read_at,
            "rows": [], "count_parc": 0, "count_technique": 0,
            "filters": _empty_filters(),
        }

    # Propriétaire : rattachement historisé du référentiel, jamais recalculé ici.
    _enrichir_gestion(rows, _lire_rattachements(db_path))

    # Classement parc / technique (jamais de suppression)
    parc, technique = [], []
    for r in rows:
        r["_technique"] = _is_technique(r.get("logement_id"))
        (technique if r["_technique"] else parc).append(r)

    count_parc = len(parc)
    count_technique = len(technique)

    # Options de filtres bâties sur les données réelles du parc (pas les techniques)
    filters = {
        "villes": sorted({(r.get("ville") or "").strip() for r in parc if (r.get("ville") or "").strip()}),
        "types": sorted({(r.get("type_logement_id") or "").strip() for r in parc if (r.get("type_logement_id") or "").strip()}),
        "proprietaires": sorted({(r.get("proprietaire_id") or "").strip() for r in parc if (r.get("proprietaire_id") or "").strip()}),
        "actifs": sorted({(r.get("actif") or "").strip() for r in parc if (r.get("actif") or "").strip()}),
    }

    base = parc + technique if include_technique else parc

    # Filtres (égalité stricte sur valeurs réelles) + recherche texte
    ql = q.strip().lower()

    def _match(r: dict) -> bool:
        if ville and (r.get("ville") or "").strip() != ville:
            return False
        if type_logement_id and (r.get("type_logement_id") or "").strip() != type_logement_id:
            return False
        if proprietaire_id and (r.get("proprietaire_id") or "").strip() != proprietaire_id:
            return False
        if actif and (r.get("actif") or "").strip() != actif:
            return False
        if ql:
            hay = " ".join(str(r.get(k) or "") for k in
                           ("logement_id", "nom_logement_officiel", "nom_court", "ville", "proprietaire_id")).lower()
            if ql not in hay:
                return False
        return True

    filtered = [r for r in base if _match(r)]

    return {
        "status": "OK",
        "error_message": None,
        "source": SOURCE_NAME,
        "source_gestion": SOURCE_GESTION_NAME,
        # Le référentiel étant disponible à ce stade, les rattachements le sont aussi : ils vivent
        # dans la même base et sont importés par la même transaction.
        "gestion_disponible": True,
        "read_at": read_at,
        "rows": filtered,
        "count_parc": count_parc,
        "count_technique": count_technique,
        "count_affiches": len(filtered),
        "include_technique": include_technique,
        "filters": filters,
        "applied": {
            "q": q, "ville": ville, "type_logement_id": type_logement_id,
            "proprietaire_id": proprietaire_id, "actif": actif,
        },
    }


def _empty_filters() -> dict[str, list]:
    return {"villes": [], "types": [], "proprietaires": [], "actifs": []}


def load_detail(logement_id: str, *, db_path=None) -> dict[str, Any] | None:
    """Charge la fiche détail. None si le logement n'existe pas (→ 404 propre)."""
    read_at = _read_at()

    if not referentiel.disponible(db_path=db_path):
        return {
            "status": "ERROR",
            "code": referentiel.REFERENTIEL_ABSENT,
            "error_message": referentiel.MESSAGE_ABSENT,
            "logement_id": logement_id,
            "read_at": read_at,
        }

    base = referentiel.logement(logement_id, db_path=db_path)
    if base is None:
        return None  # 404 propre géré par la route

    is_tech = _is_technique(logement_id)
    type_id = (base.get("type_logement_id") or "").strip()

    # Propriétaire : export de gestion du moteur. `base` ne le porte plus (cf. docstring). Sans
    # cette résolution, `commission_rows` était systématiquement vide : l'historique des taux
    # disparaissait de toutes les fiches.
    par_logement = _lire_rattachements(db_path)
    rattachements = [] if par_logement is None else par_logement.get(str(logement_id).strip(), [])
    proprietaire_id, gestion_statut = _resoudre_gestion(
        None if par_logement is None else rattachements)
    base["proprietaire_id"] = proprietaire_id
    base["gestion_statut"] = gestion_statut

    # --- Enrichissement depuis le référentiel SQLite, par clé directe ---
    # `base` EST déjà la ligne de référentiel : ref_logements porte toutes les colonnes de
    # l'ancien export CSV et davantage. L'ancien « enrichissement » consistait à retrouver dans
    # REF_Setup.xlsm ce que le CSV avait tronqué ; ce détour n'a plus lieu d'être.
    ref_row = base
    enrichissement_absent = False

    type_label = referentiel.type_label(type_id, db_path=db_path)
    commission_rows = referentiel.taux_commission(proprietaire_id, db_path=db_path)
    menage_interne = referentiel.couts_menage_interne(logement_id, db_path=db_path)
    menage_standard = referentiel.couts_standards_menage(type_id, db_path=db_path)

    origine = {
        "source_liste": SOURCE_NAME,
        "source_gestion": SOURCE_GESTION_NAME,
        "source_referentiel": SOURCE_NAME,
        "referentiel_disponible": True,
        "read_at": read_at,
        "logement_id_enrichissement": logement_id,
        "enrichissement_message":
            "Fiche construite depuis le référentiel SQLite, par clé directe logement_id.",
    }

    return {
        "status": "OK",
        "error_message": None,
        "logement_id": logement_id,
        "is_technique": is_tech,
        "base": base,               # données PBI (propriétaire + dates déjà résolus)
        "ref_row": ref_row,         # REF_Logements brut (peut être None)
        "type_logement_id": type_id,
        "type_label": type_label,
        "proprietaire_id": proprietaire_id,
        "gestion_statut": gestion_statut,
        "gestion_rattachements": rattachements,
        "commission_rows": commission_rows,
        "menage_interne": menage_interne,
        "menage_standard": menage_standard,
        "origine": origine,
        "read_at": read_at,
    }

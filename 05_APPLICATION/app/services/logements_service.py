"""Service métier logements — LECTURE SEULE, AUCUN CALCUL.

Arbitrages APP-1 :
- Liste : source unique = PBI_Referentiel_Logements.csv (identité du bien).
  Si absent / vide / illisible → état d'erreur clair, JAMAIS de fallback Excel,
  JAMAIS de reconstruction par jointure REF_Logements × REF_Gestion × REF_Proprietaires.
- Propriétaire : second export du moteur, PBI_Referentiel_Gestion_Logements.csv. Depuis le commit
  moteur `c8dea3c`, `proprietaire_id` et les dates de gestion ont quitté l'export logements pour ne
  plus y être dupliqués. Ce service n'avait pas suivi : la colonne « Propriétaire » de la liste
  affichait « Non renseigné » sur TOUT le parc et le filtre propriétaire restait vide, sans aucun
  message d'erreur. On lit donc désormais les deux exports.
  La sélection du rattachement s'appuie sur `statut_gestion`, champ produit PAR LE MOTEUR : aucune
  comparaison de dates n'est refaite ici (cf. règle « le moteur calcule, l'application lit »).
  Aucun rattachement n'est deviné : un logement sans ligne active exploitable est signalé, pas
  complété.
- Détail : enrichissement REF_Setup UNIQUEMENT par clé directe logement_id.
- Commission : historique BRUT rattaché au propriétaire résolu par le CSV, sans notion d'« actuel ».
- Lignes techniques (logement_id ne commençant pas par LOG_) : exclues par défaut, jamais masquées.
"""
from datetime import datetime
from typing import Any
from app.config import PBI_LOGEMENTS, PBI_GESTION_LOGEMENTS, REF_SETUP
from app.readers.csv_reader import read_csv
from app.readers import ref_setup_reader as ref

PBI_SOURCE_NAME = "PBI_Referentiel_Logements.csv"
PBI_GESTION_SOURCE_NAME = "PBI_Referentiel_Gestion_Logements.csv"
REF_SOURCE_NAME = "REF_Setup.xlsm"
_LOG_PREFIX = "LOG_"
_STATUT_ACTIF = "ACTIF"

# Résolutions possibles du rattachement, exposées telles quelles à l'écran.
GESTION_RESOLU = "RESOLU"              # exactement une ligne ACTIF
GESTION_CLOS = "CLOS"                  # aucune ligne active, une seule ligne connue (gestion terminée)
GESTION_A_CONTROLER = "A_CONTROLER"    # plusieurs actifs, ou plusieurs lignes closes : arbitrage humain
GESTION_ABSENT = "ABSENT"              # aucun rattachement exporté pour ce logement
GESTION_SOURCE_ABSENTE = "SOURCE_ABSENTE"  # export de gestion non généré (Lot13 non lancé)


def _lire_rattachements() -> dict[str, list[dict[str, Any]]] | None:
    """{logement_id: [lignes de gestion]}. None si l'export du moteur est absent."""
    if not PBI_GESTION_LOGEMENTS.exists():
        return None
    par_logement: dict[str, list[dict[str, Any]]] = {}
    for r in read_csv(PBI_GESTION_LOGEMENTS, max_rows=None):
        par_logement.setdefault(str(r.get("logement_id") or "").strip(), []).append(r)
    return par_logement


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
) -> dict[str, Any]:
    """Charge la liste depuis le CSV PBI. Retourne un état structuré (jamais d'exception nue)."""
    read_at = _read_at()

    # État d'erreur : fichier absent
    if not PBI_LOGEMENTS.exists():
        return {
            "status": "ERROR",
            "error_message": f"Source liste introuvable : {PBI_SOURCE_NAME}. "
                             "La liste des logements ne peut pas être affichée.",
            "source": PBI_SOURCE_NAME,
            "read_at": read_at,
            "rows": [], "count_parc": 0, "count_technique": 0,
            "filters": _empty_filters(),
        }

    rows = read_csv(PBI_LOGEMENTS, max_rows=None)

    # État d'erreur : fichier vide ou illisible (aucune ligne exploitable)
    if not rows:
        return {
            "status": "ERROR",
            "error_message": f"Source liste vide ou illisible : {PBI_SOURCE_NAME}.",
            "source": PBI_SOURCE_NAME,
            "read_at": read_at,
            "rows": [], "count_parc": 0, "count_technique": 0,
            "filters": _empty_filters(),
        }

    # Propriétaire : second export du moteur, jamais recalculé ici.
    _enrichir_gestion(rows, _lire_rattachements())

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
        "source": PBI_SOURCE_NAME,
        "source_gestion": PBI_GESTION_SOURCE_NAME,
        # Signalé explicitement : sans cet export, la colonne Propriétaire est vide pour une raison
        # connue (Lot13 non lancé) et non par absence de rattachement.
        "gestion_disponible": PBI_GESTION_LOGEMENTS.exists(),
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


def load_detail(logement_id: str) -> dict[str, Any] | None:
    """Charge la fiche détail. None si le logement n'existe pas dans le CSV PBI (→ 404 propre)."""
    read_at = _read_at()

    if not PBI_LOGEMENTS.exists():
        return {
            "status": "ERROR",
            "error_message": f"Source liste introuvable : {PBI_SOURCE_NAME}.",
            "logement_id": logement_id,
            "read_at": read_at,
        }

    rows = read_csv(PBI_LOGEMENTS, max_rows=None)
    base = next((r for r in rows if str(r.get("logement_id", "")).strip() == str(logement_id).strip()), None)
    if base is None:
        return None  # 404 propre géré par la route

    is_tech = _is_technique(logement_id)
    type_id = (base.get("type_logement_id") or "").strip()

    # Propriétaire : export de gestion du moteur. `base` ne le porte plus (cf. docstring). Sans
    # cette résolution, `commission_rows` était systématiquement vide : l'historique des taux
    # disparaissait de toutes les fiches.
    par_logement = _lire_rattachements()
    rattachements = [] if par_logement is None else par_logement.get(str(logement_id).strip(), [])
    proprietaire_id, gestion_statut = _resoudre_gestion(
        None if par_logement is None else rattachements)
    base["proprietaire_id"] = proprietaire_id
    base["gestion_statut"] = gestion_statut

    # --- Enrichissement REF_Setup par clé directe logement_id ---
    ref_available = ref.ref_setup_available()
    ref_row = ref.get_logement_ref(logement_id) if ref_available else None
    enrichissement_absent = ref_available and ref_row is None

    type_label = ref.get_type_label(type_id) if ref_available else None
    commission_rows = ref.get_commission_rows(proprietaire_id) if ref_available else []
    menage_interne = ref.get_menage_interne_rows(logement_id) if ref_available else []
    menage_standard = ref.get_menage_standard_rows(type_id) if ref_available else []

    origine = {
        "source_liste": PBI_SOURCE_NAME,
        "source_gestion": PBI_GESTION_SOURCE_NAME,
        "source_referentiel": REF_SOURCE_NAME,
        "referentiel_disponible": ref_available,
        "read_at": read_at,
        "logement_id_enrichissement": logement_id,
        "enrichissement_message": (
            "Référentiel REF_Setup.xlsm introuvable — fiche limitée aux données de la liste."
            if not ref_available else
            (f"Aucune ligne REF_Logements pour {logement_id} — enrichissement partiel."
             if enrichissement_absent else
             "Enrichissement REF_Setup trouvé par clé directe logement_id.")
        ),
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

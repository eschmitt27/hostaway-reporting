"""Couche métier du référentiel — lecture SQLite, jamais Excel.

POSITION DANS L'ARCHITECTURE
    route → service métier → CE MODULE → ref_setup_repo → SQLite

Les écrans n'interrogent jamais une table directement. Ce module expose des accès nommés par le
métier (« les logements du parc », « les taux d'un propriétaire ») et laisse `ref_setup_repo` faire
la lecture brute.

PARITÉ AVEC L'ANCIEN CHEMIN
Chaque fonction reproduit exactement la sémantique du lecteur Excel qu'elle remplace
(`readers/ref_setup_reader.py`) : mêmes clés, mêmes champs retournés, même absence de tri et de
calcul. Aucune règle économique n'est introduite ici — c'est une migration d'interface de données,
pas une réécriture métier.

FAIL-CLOSED
Si le référentiel n'a jamais été importé, ces fonctions ne se rabattent PAS sur le classeur : elles
signalent l'absence. Un repli silencieux ferait croire à un référentiel vide alors qu'il n'est
qu'absent — deux situations qui appellent des actions opposées.
"""
from __future__ import annotations

from typing import Any

from app.services import ref_setup_repo as repo

# Message unique, pour que tous les écrans disent la même chose.
REFERENTIEL_ABSENT = "REFERENTIEL_NON_INITIALISE"
MESSAGE_ABSENT = (
    "Référentiel non initialisé : les données de paramétrage n'ont pas encore été importées. "
    "Ouvrez « Référentiel Setup » pour les prévisualiser puis les importer."
)


def disponible(*, db_path=None) -> bool:
    return repo.est_disponible(db_path=db_path)


def _txt(v: Any) -> str:
    return str(v or "").strip()


# ── Logements ───────────────────────────────────────────────────────────────────────────────────

def logements(*, db_path=None) -> list[dict[str, str]]:
    """Toutes les lignes de REF_Logements, techniques comprises — le tri du parc se fait plus haut."""
    return repo.lire_table("ref_logements", db_path=db_path)


def logement(logement_id: str, *, db_path=None) -> dict[str, str] | None:
    if not logement_id:
        return None
    return repo.lire_par_cle("ref_logements", _txt(logement_id), db_path=db_path)


# ── Rattachement de gestion ─────────────────────────────────────────────────────────────────────

def gestion_par_logement(*, db_path=None) -> dict[str, list[dict[str, str]]]:
    """{logement_id: [rattachements]}. L'historique est rendu tel quel, jamais arbitré ici."""
    out: dict[str, list[dict[str, str]]] = {}
    for r in repo.lire_table("ref_gestion_logements_hist", db_path=db_path):
        out.setdefault(_txt(r.get("logement_id")), []).append(r)
    return out


def gestion_du_logement(logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    return gestion_par_logement(db_path=db_path).get(_txt(logement_id), [])


# ── Propriétaires ───────────────────────────────────────────────────────────────────────────────

def proprietaires(*, db_path=None) -> list[dict[str, str]]:
    return repo.lire_table("ref_proprietaires", db_path=db_path)


def proprietaire(proprietaire_id: str, *, db_path=None) -> dict[str, str] | None:
    if not proprietaire_id:
        return None
    return repo.lire_par_cle("ref_proprietaires", _txt(proprietaire_id), db_path=db_path)


def nom_proprietaire(proprietaire_id: str, *, db_path=None) -> str:
    """Libellé d'affichage. Chaîne vide si inconnu — jamais un identifiant maquillé en nom."""
    p = proprietaire(proprietaire_id, db_path=db_path)
    return _txt(p.get("nom_proprietaire")) if p else ""


# ── Types de logement ───────────────────────────────────────────────────────────────────────────

def type_label(type_logement_id: str, *, db_path=None) -> str | None:
    """Décodage code → libellé. Même contrat que `ref_setup_reader.get_type_label`."""
    if not type_logement_id:
        return None
    row = repo.lire_par_cle("ref_types_logements", _txt(type_logement_id), db_path=db_path)
    return row.get("type_logement") if row else None


# ── Taux de commission ──────────────────────────────────────────────────────────────────────────

def taux_commission(proprietaire_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Lignes BRUTES rattachées au propriétaire.

    Aucun tri par date, aucune sélection, aucune notion d'« actuel » — reproduit à l'identique
    `ref_setup_reader.get_commission_rows`, y compris les quatre champs retournés.
    """
    if not proprietaire_id:
        return []
    cible = _txt(proprietaire_id)
    return [{
        "taux_commission": r.get("taux_commission"),
        "date_debut": r.get("date_debut"),
        "date_fin": r.get("date_fin"),
        "actif": r.get("actif"),
    } for r in repo.lire_table("ref_taux_commission", db_path=db_path)
        if _txt(r.get("proprietaire_id")) == cible]


# ── Coûts de ménage ─────────────────────────────────────────────────────────────────────────────

def couts_menage_interne(logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Lignes brutes rattachées par clé directe logement_id."""
    if not logement_id:
        return []
    cible = _txt(logement_id)
    return [r for r in repo.lire_table("ref_couts_menage_interne", db_path=db_path)
            if _txt(r.get("logement_id")) == cible]


def couts_standards_menage(type_logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Lignes brutes pour le type propre du logement. Décodage de référence, aucun calcul."""
    if not type_logement_id:
        return []
    cible = _txt(type_logement_id)
    return [r for r in repo.lire_table("ref_couts_standards_menage", db_path=db_path)
            if _txt(r.get("type_logement_id")) == cible]

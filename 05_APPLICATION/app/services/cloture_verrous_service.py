"""Verrous de clôture — ce que « module clôturé » veut dire pour les services de chaque domaine.

Une clôture de module n'est PAS un statut décoratif : les services qui écrivent dans un domaine
interrogent ce module AVANT d'écrire, et refusent quand le mois de l'opération est clôturé pour CE
domaine. Rien d'autre n'est verrouillé : les données restent consultables, et un domaine clôturé
n'empêche aucun autre domaine de travailler.

Trois choix de conception, pour que le verrou soit réel sans rendre l'application inutilisable :

  · UN SEUL point de lecture (ce module) — chaque domaine l'appelle avec sa clé, aucun ne relit
    `cloture_modules` à sa manière ;
  · LÉGER ET TOLÉRANT : une clé primaire (mois, module), aucune dépendance sur les services métier ;
    table absente (base non migrée) = rien n'est verrouillé, jamais une erreur ;
  · un MESSAGE MÉTIER, qui dit quel module est clôturé, pour quel mois, et comment le rouvrir.

Le verrou d'un module s'ajoute à la clôture GLOBALE du mois (`ref_cloture_mensuelle` = CLOTURE), que les
domaines respectaient déjà : `verrouille(mois, module)` répond aux deux.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

CLOS = "CLOS"
ROUVERT = "ROUVERT"


def _mois(valeur: Any) -> str:
    return ("" if valeur is None else str(valeur).strip())[:7]


def _table_presente(conn, nom: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (nom,)).fetchone() is not None


def module_clos(mois: Any, module: str, *, db_path=None) -> bool:
    """Le module est-il clôturé pour ce mois ? (Rouvert, jamais clôturé, table absente : non.)"""
    mois = _mois(mois)
    if len(mois) != 7:
        return False
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, "cloture_modules"):
            return False
        r = conn.execute("SELECT statut FROM cloture_modules WHERE mois = ? AND module = ?",
                         (mois, module)).fetchone()
        return bool(r and r[0] == CLOS)
    finally:
        conn.close()


def mois_clos(mois: Any, *, db_path=None) -> bool:
    """Le mois est-il clôturé définitivement (`ref_cloture_mensuelle` = CLOTURE) ?"""
    mois = _mois(mois)
    if len(mois) != 7:
        return False
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, "ref_cloture_mensuelle"):
            return False
        r = conn.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois = ?",
                         (mois,)).fetchone()
        return bool(r and str(r[0]).strip().upper() == "CLOTURE")
    finally:
        conn.close()


def verrouille(mois: Any, module: str, *, db_path=None) -> bool:
    """Le domaine `module` est fermé pour ce mois : par sa propre clôture OU par celle du mois."""
    return module_clos(mois, module, db_path=db_path) or mois_clos(mois, db_path=db_path)


def message(mois: Any, module: str) -> str:
    """Refus lisible : quel module, quel mois, comment le rouvrir. Jamais un code technique."""
    from app.services import cloture_modules_service as cm
    from app.services import clotures_service as cs

    mois = _mois(mois)
    m = cm.PAR_CLE.get(module)
    nom = m.libelle if m else "ce module"
    return (f"Le module « {nom} » est clôturé pour {cs.mois_fr(mois)} : cette opération n'est plus "
            "possible. Rouvrez le module depuis la clôture mensuelle (avec une justification) pour "
            "la faire.")


def refus(mois: Any, module: str, *, db_path=None) -> str:
    """Message de refus si le MODULE est clôturé pour ce mois, sinon chaîne vide.

    Ne regarde QUE le module : les domaines qui respectaient déjà la clôture globale du mois la
    conservent telle quelle, ce verrou s'y ajoute sans la dupliquer."""
    return message(mois, module) if module_clos(mois, module, db_path=db_path) else ""

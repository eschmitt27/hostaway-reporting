"""Acces SQLite pour les lots moteur — resolution de base et lecture/ecriture des datasets.

POURQUOI CETTE LIB
`lot8b` avait deja etabli la regle de resolution de la base : --db, puis PILOTAGE_DB_PATH, puis
APP_DATA_DIR/app.db, et JAMAIS de defaut vers la base reelle. Lot4bis, Lot4ter et Lot4quater ont le
meme besoin. La recopier trois fois garantirait qu'une des copies derive ; elle vit donc ici.

AUCUN DEFAUT VERS LA BASE REELLE
C'est la garantie principale. Un lot moteur lance sans precision ne doit pas tomber par accident sur
la base de production : il rend None et l'appelant refuse, plutot que d'ecrire quelque part au hasard.

PAS D'IMPORT DE L'APPLICATION
Les lots tournent sous l'interpreteur qui porte pandas ; l'application, sous un autre. Importer
`05_APPLICATION` depuis un lot lierait les deux environnements. On parle donc SQL directement, sur le
schema que les migrations ont cree.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

SOURCE_SQLITE = "SQLITE"
SOURCE_EXCEL = "EXCEL"
SOURCE_AUTO = "AUTO"
SOURCES = (SOURCE_SQLITE, SOURCE_EXCEL, SOURCE_AUTO)

ETAPE_CALCULEES = "CALCULEES"
ETAPE_RESOLUES = "RESOLUES"

ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"


def maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def chemin_db(argument: str | None = None) -> Path | None:
    """Base applicative a interroger. Resolue a l'appel, jamais figee a l'import.

    Priorite : argument explicite > PILOTAGE_DB_PATH > APP_DATA_DIR/app.db. None si rien n'est
    designe — l'appelant doit alors refuser, pas deviner.
    """
    if argument:
        return Path(argument)
    env = os.environ.get("PILOTAGE_DB_PATH")
    if env:
        return Path(env)
    data = os.environ.get("APP_DATA_DIR")
    if data:
        return Path(data) / "app.db"
    return None


def ouvrir(chemin: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(chemin))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# Identifiants Hostaway : TEXTE en base, ENTIERS cote moteur.
#
# Stocker un identifiant externe en texte est correct — le comparer numeriquement n'a pas de sens et
# un zero non significatif serait perdu. Mais tous les index du moteur ont ete construits sur les
# valeurs que le classeur fournissait, c'est-a-dire des entiers : mapping des logements, index des
# payouts, rapprochement live/historique. Une cle texte n'y est jamais trouvee, et l'echec est
# SILENCIEUX — le payout apparait simplement absent, et un revenu disparait sans erreur.
#
# On restitue donc le type attendu a la frontiere, sans changer la logique du moteur.
IDS_HOSTAWAY = ("reservation_id", "reservation_id_hostaway", "listingMapId", "listing_map_id")


def entier_si_possible(valeur):
    """Entier quand la valeur en est un, sinon la valeur telle quelle."""
    if valeur is None or isinstance(valeur, int):
        return valeur
    texte = str(valeur).strip()
    if not texte:
        return valeur
    try:
        return int(texte)
    except ValueError:
        return valeur


def traduire(ligne: dict[str, Any], correspondance: dict[str, str] | None = None) -> dict[str, Any]:
    """Renomme les cles d'une ligne SQLite vers le vocabulaire moteur, types d'identifiants compris."""
    correspondance = correspondance or {}
    traduite = {correspondance.get(k, k): v for k, v in ligne.items()}
    for cle in IDS_HOSTAWAY:
        if cle in traduite:
            traduite[cle] = entier_si_possible(traduite[cle])
    return traduite


def table_presente(conn: sqlite3.Connection, nom: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())


def verifier(chemin: Path | None, tables: Sequence[str]) -> tuple[sqlite3.Connection | None, str]:
    """Ouvre la base et verifie que les tables attendues existent.

    Rend (connexion, message). Connexion None = inutilisable, et le message dit pourquoi : base non
    designee, introuvable, ou migration non appliquee. Trois causes distinctes, trois corrections
    differentes — les confondre ferait chercher au mauvais endroit.
    """
    if chemin is None:
        return None, "aucune base applicative designee (--db / PILOTAGE_DB_PATH / APP_DATA_DIR)"
    if not chemin.exists():
        return None, "base applicative introuvable : %s" % chemin
    conn = ouvrir(chemin)
    manquantes = [t for t in tables if not table_presente(conn, t)]
    if manquantes:
        conn.close()
        return None, "tables absentes (migration non appliquee) : %s" % ", ".join(manquantes)
    return conn, str(chemin)


def lignes(conn: sqlite3.Connection, table: str, colonnes: Sequence[str],
           ou: str = "", args: Sequence[Any] = (), ordre: str = "id") -> list[dict[str, Any]]:
    sql = "SELECT %s FROM %s" % (", ".join(colonnes), table)
    if ou:
        sql += " WHERE " + ou
    sql += " ORDER BY " + ordre
    return [dict(zip(colonnes, r)) for r in conn.execute(sql, tuple(args)).fetchall()]


# ── Extraction Hostaway courante ────────────────────────────────────────────────────────────────

def extraction_utilisable(conn: sqlite3.Connection) -> str:
    """Derniere extraction Hostaway exploitable, ou chaine vide.

    Une extraction ECHEC est ecartee : ses lignes sont un fragment. Une extraction PARTIELLE est
    retenue — incomplete n'est pas fausse — et c'est a l'appelant de le signaler.
    """
    if not table_presente(conn, "hostaway_extractions"):
        return ""
    r = conn.execute(
        "SELECT extraction_id FROM hostaway_extractions WHERE statut IN ('SUCCES','PARTIEL') "
        "ORDER BY date_debut DESC, id DESC LIMIT 1").fetchone()
    return r[0] if r else ""


# ── Datasets de reservations ────────────────────────────────────────────────────────────────────

def ouvrir_dataset(conn: sqlite3.Connection, etape: str, *, extraction_id: str = "",
                   run_id: str = "") -> str:
    """Cree un dataset et le rend COURANT pour son etape.

    L'ancien jeu courant est desactive, pas supprime : un recalcul doit rester comparable au
    precedent, et effacer le passe interdirait de constater ce qui a change.
    """
    dataset_id = "RDS-" + uuid.uuid4().hex[:12].upper()
    conn.execute("UPDATE reservations_datasets SET actif = 0 WHERE etape = ? AND actif = 1",
                 (etape,))
    conn.execute(
        "INSERT INTO reservations_datasets (dataset_id, etape, extraction_id, run_id, date_calcul, "
        "actif) VALUES (?,?,?,?,?,1)",
        (dataset_id, etape, extraction_id or None, run_id or None, maintenant()))
    return dataset_id


def cloturer_dataset(conn: sqlite3.Connection, dataset_id: str, *, table: str,
                     statut: str = ST_SUCCES, message: str = "") -> int:
    """Fige le compteur du dataset, lu en base plutot que transmis par l'appelant."""
    nb = conn.execute("SELECT COUNT(*) FROM %s WHERE dataset_id = ?" % table,
                      (dataset_id,)).fetchone()[0]
    conn.execute(
        "UPDATE reservations_datasets SET nb_lignes = ?, statut = ?, message = ? "
        "WHERE dataset_id = ?", (nb, statut, message or None, dataset_id))
    return nb


def dataset_courant(conn: sqlite3.Connection, etape: str) -> str:
    if not table_presente(conn, "reservations_datasets"):
        return ""
    r = conn.execute(
        "SELECT dataset_id FROM reservations_datasets WHERE etape = ? AND actif = 1", (etape,)
    ).fetchone()
    return r[0] if r else ""


def ecrire_lignes(conn: sqlite3.Connection, table: str, colonnes: Sequence[str],
                  dataset_id: str, lignes_a_ecrire: Iterable[dict[str, Any]],
                  alias: dict[str, str] | None = None) -> int:
    """Insere les lignes d'un dataset. `alias` traduit un nom de colonne moteur vers la base.

    Le moteur nomme `guestCount`, la base `guest_count` : la traduction vit ici pour qu'aucune des
    deux couches n'impose son style a l'autre.
    """
    alias = alias or {}
    lignes_a_ecrire = list(lignes_a_ecrire)
    if not lignes_a_ecrire:
        return 0
    trous = ", ".join(["?"] * (len(colonnes) + 1))
    conn.executemany(
        "INSERT OR REPLACE INTO %s (dataset_id, %s) VALUES (%s)"
        % (table, ", ".join(colonnes), trous),
        [(dataset_id, *(l.get(alias.get(c, c)) for c in colonnes)) for l in lignes_a_ecrire])
    return len(lignes_a_ecrire)

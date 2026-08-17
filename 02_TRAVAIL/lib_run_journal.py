#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Journal des runs moteur — ecrit DES LE DEMARRAGE, jamais a la fin.

LE DEFAUT QUE CE MODULE CORRIGE
lot1 n ecrivait son entree de journal qu apres sa derniere etape. Le run du 2026-08-17 a telecharge
1542 reservations, ecrit sept masters, puis a ete interrompu pendant les taches menage : le journal
n en porte aucune trace. Un run qui reussit l essentiel puis echoue devient invisible, et rien ne
signale qu il faut verifier quoi que ce soit.

Regle : la ligne de run existe AVANT le premier appel reseau. Chaque etape est enregistree des
qu elle se termine. Si le processus est tue, ce qui a ete fait reste ecrit.

OU EST ECRIT LE JOURNAL
En SQLite (tables moteur_runs / moteur_run_etapes, migration 0031) des qu une base est designee :
--db, PILOTAGE_DB_PATH, ou APP_DATA_DIR/app.db. C est la cible d architecture.
A defaut, un fichier JSON a cote des sorties du lot — pour qu un run lance sans base laisse quand
meme une trace exploitable. Le JSON n est pas la cible : c est un filet.

CE MODULE N ECHOUE JAMAIS LE RUN
Journaliser est une observation, pas une etape metier. Si le journal est indisponible, le lot
continue et le signale. L inverse — un lot qui refuse de tourner parce qu il ne sait pas ecrire son
journal — serait une regression de disponibilite.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Statuts de run — repris de calculs_runs (0018) plutot que reinventes.
EN_COURS = "EN_COURS"
SUCCES = "SUCCES"
ECHEC = "ECHEC"
# Ajoutes ici : aucun statut existant ne les exprimait.
PARTIEL = "PARTIEL"        # au moins une etape reussie ET au moins une echouee
INTERROMPU = "INTERROMPU"  # deduit a posteriori : le run n a jamais ete clos

# Statuts d etape
ETAPE_SUCCES = "SUCCES"
ETAPE_ECHEC = "ECHEC"
ETAPE_IGNOREE = "IGNOREE"


def _maintenant() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _code_version(racine: Path) -> str:
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=str(racine),
                           capture_output=True, text=True, timeout=10)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:
        return ""


def resoudre_base(explicite: str | None = None) -> Path | None:
    """Base applicative ou journaliser. None si aucune n est designee.

    Ne devine JAMAIS un chemin de production : sans indication explicite, on retombe sur le JSON.
    Meme regle que _chemin_db() de lot8b.
    """
    if explicite:
        return Path(explicite)
    env = os.environ.get("PILOTAGE_DB_PATH")
    if env:
        return Path(env)
    data = os.environ.get("APP_DATA_DIR")
    if data:
        return Path(data) / "app.db"
    return None


class RunJournal:
    """Journal d un run. A ouvrir AVANT le premier travail, a fermer dans un finally."""

    def __init__(self, lot: str, *, racine: Path, db_path: str | None = None,
                 declencheur: str = "MANUEL", fallback_dir: Path | None = None,
                 log=None):
        self.lot = lot
        self.racine = Path(racine)
        self.declencheur = declencheur
        self.log = log
        self.run_id = f"RUN-{lot}-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.started_at = _maintenant()
        self.etapes: list[dict] = []
        self._ordre = 0
        self._db = resoudre_base(db_path)
        self._json = Path(fallback_dir or self.racine / "04_LOGS" / "runs") / f"{self.run_id}.json"
        self._sqlite_ok = False
        self._ouvrir()

    # ── Ecriture ────────────────────────────────────────────────────────────
    def _signaler(self, message: str) -> None:
        if self.log:
            self.log.warning(message)
        else:
            print(f"[RUN] {message}")

    def _connexion(self):
        if self._db is None or not self._db.exists():
            return None
        try:
            conn = sqlite3.connect(str(self._db), timeout=30)
            presente = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='moteur_runs'"
            ).fetchone()[0]
            if not presente:
                conn.close()
                return None
            return conn
        except sqlite3.Error:
            return None

    def _ouvrir(self) -> None:
        """Ligne de run ecrite AVANT tout travail. C est le coeur du correctif."""
        conn = self._connexion()
        if conn is not None:
            try:
                conn.execute(
                    "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur, "
                    "arguments, pid, hote, code_version) VALUES (?,?,?,?,?,?,?,?,?)",
                    (self.run_id, self.lot, self.started_at, EN_COURS, self.declencheur,
                     " ".join(sys.argv[1:])[:2000], os.getpid(), socket.gethostname()[:100],
                     _code_version(self.racine)))
                conn.commit()
                self._sqlite_ok = True
            except sqlite3.Error as exc:
                self._signaler(f"journal SQLite indisponible ({exc}) — bascule sur JSON")
            finally:
                conn.close()
        self._ecrire_json(EN_COURS)
        if self.log:
            self.log.info(f"Run {self.run_id} demarre "
                          f"({'SQLite' if self._sqlite_ok else 'JSON'})")

    def _ecrire_json(self, statut: str, erreur: str = "") -> None:
        try:
            self._json.parent.mkdir(parents=True, exist_ok=True)
            self._json.write_text(json.dumps({
                "run_id": self.run_id, "lot": self.lot, "started_at": self.started_at,
                "statut": statut, "declencheur": self.declencheur, "pid": os.getpid(),
                "arguments": sys.argv[1:], "erreur": erreur, "etapes": self.etapes,
            }, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            self._signaler(f"journal JSON indisponible : {exc}")

    # ── API ─────────────────────────────────────────────────────────────────
    def etape(self, nom: str, statut: str, *, nb_lus=None, nb_ecrits=None, position=None,
              tentatives: int = 0, http_status=None, erreur: str = "",
              sorties: list | None = None, started_at: str | None = None) -> None:
        """Enregistre une etape TERMINEE. Ecrit immediatement."""
        self._ordre += 1
        entree = {
            "etape": nom, "ordre": self._ordre, "started_at": started_at or _maintenant(),
            "ended_at": _maintenant(), "statut": statut, "nb_lus": nb_lus,
            "nb_ecrits": nb_ecrits, "position": position, "tentatives": tentatives,
            "http_status": http_status, "erreur": erreur[:2000] if erreur else "",
            "sorties": sorties or [],
        }
        self.etapes.append(entree)

        conn = self._connexion() if self._sqlite_ok else None
        if conn is not None:
            try:
                conn.execute(
                    "INSERT INTO moteur_run_etapes (run_id, etape, ordre, started_at, ended_at, "
                    "statut, nb_lus, nb_ecrits, position, tentatives, http_status, erreur, sorties) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.run_id, nom, entree["ordre"], entree["started_at"], entree["ended_at"],
                     statut, nb_lus, nb_ecrits, position, tentatives, http_status,
                     entree["erreur"], json.dumps(entree["sorties"], ensure_ascii=False)))
                conn.commit()
            except sqlite3.Error as exc:
                self._signaler(f"etape non journalisee en SQLite : {exc}")
            finally:
                conn.close()
        self._ecrire_json(EN_COURS)

    def statut_global(self) -> str:
        """SUCCES / PARTIEL / ECHEC, deduit des etapes — jamais affirme independamment."""
        reussies = [e for e in self.etapes if e["statut"] == ETAPE_SUCCES]
        echouees = [e for e in self.etapes if e["statut"] == ETAPE_ECHEC]
        if echouees and reussies:
            return PARTIEL
        if echouees:
            return ECHEC
        if reussies:
            return SUCCES
        return ECHEC  # rien n a abouti

    def fermer(self, erreur: str = "") -> str:
        """Cloture le run. A appeler dans un finally : un run non ferme reste INTERROMPU."""
        statut = self.statut_global()
        fin = _maintenant()
        duree = (datetime.fromisoformat(fin) - datetime.fromisoformat(self.started_at)).total_seconds()
        ok = sum(1 for e in self.etapes if e["statut"] == ETAPE_SUCCES)
        ko = sum(1 for e in self.etapes if e["statut"] == ETAPE_ECHEC)

        conn = self._connexion() if self._sqlite_ok else None
        if conn is not None:
            try:
                conn.execute(
                    "UPDATE moteur_runs SET ended_at=?, statut=?, nb_etapes=?, nb_etapes_ok=?, "
                    "nb_etapes_ko=?, duree_s=?, erreur_resume=? WHERE run_id=?",
                    (fin, statut, len(self.etapes), ok, ko, round(duree, 1),
                     erreur[:2000] if erreur else "", self.run_id))
                conn.commit()
            except sqlite3.Error as exc:
                self._signaler(f"cloture non journalisee en SQLite : {exc}")
            finally:
                conn.close()
        self._ecrire_json(statut, erreur)
        if self.log:
            self.log.info(f"Run {self.run_id} termine : {statut} "
                          f"({ok} etape(s) OK, {ko} en echec)")
        return statut


def marquer_runs_interrompus(db_path: str | Path, *, lot: str | None = None) -> int:
    """Passe en INTERROMPU les runs restes EN_COURS dont le processus n existe plus.

    Prepare le comportement attendu de l orchestrateur : au demarrage, un run EN_COURS sans
    processus vivant n est pas en cours — il a ete tue. Le laisser EN_COURS ferait croire a un
    traitement actif et bloquerait tout verrou pose sur ce critere.
    """
    chemin = Path(db_path)
    if not chemin.exists():
        return 0
    conn = sqlite3.connect(str(chemin), timeout=30)
    try:
        presente = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='moteur_runs'"
        ).fetchone()[0]
        if not presente:
            return 0
        sql = "SELECT run_id, pid FROM moteur_runs WHERE statut = ?"
        args: list = [EN_COURS]
        if lot:
            sql += " AND lot = ?"
            args.append(lot)
        orphelins = [r for r in conn.execute(sql, args).fetchall() if not _processus_vivant(r[1])]
        for run_id, _ in orphelins:
            conn.execute(
                "UPDATE moteur_runs SET statut=?, erreur_resume=? WHERE run_id=?",
                (INTERROMPU, "Processus absent : run jamais cloture.", run_id))
        conn.commit()
        return len(orphelins)
    finally:
        conn.close()


def _processus_vivant(pid) -> bool:
    """Le PID tourne-t-il encore ? En cas de doute, on repond OUI.

    Se tromper en disant « mort » marquerait INTERROMPU un run bien vivant et pourrait autoriser un
    second run concurrent. Se tromper en disant « vivant » laisse seulement une ligne EN_COURS de
    trop, corrigee au prochain passage.
    """
    if not pid:
        return False
    try:
        import psutil  # noqa: F401
        return psutil.pid_exists(int(pid))
    except ImportError:
        pass
    if os.name == "nt":
        try:
            r = subprocess.run(["tasklist", "/FI", f"PID eq {int(pid)}", "/NH"],
                               capture_output=True, text=True, timeout=15)
            return str(int(pid)) in (r.stdout or "")
        except Exception:
            return True
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except (PermissionError, ValueError, OSError):
        return True

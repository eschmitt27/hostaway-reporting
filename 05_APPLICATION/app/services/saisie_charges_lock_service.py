"""APP-3b / commit 4 — Verrou exclusif INTERPROCESSUS des écritures de charges.

Le contrôle d'unicité d'un `charge_id` est une lecture suivie d'une écriture. Entre les deux, un
autre processus peut conclure la même chose et écrire d'abord : les deux passent leur contrôle,
les deux écrivent, et la charge est dupliquée. Aucune relecture ne rattrape ça — seul un verrou
pris **avant la lecture qui sert à conclure** l'élimine.

Le verrou repose sur une création **atomique** de fichier :

    os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)

et jamais sur un `if not lock.exists(): créer` — qui rouvrirait exactement la course qu'on ferme.
L'atomicité est garantie par le système de fichiers, donc le verrou protège plusieurs **processus**
Python indépendants, pas seulement plusieurs threads.

Propriété du verrou : chaque détenteur écrit un **token aléatoire** dans le fichier. La libération
relit ce token et refuse de supprimer un verrou qui ne lui appartient pas — un processus ne peut
donc jamais déverrouiller le travail d'un autre.

Verrou périmé : `inspecter_verrou()` diagnostique, il **ne supprime rien**. Un PID qui semble mort
n'est pas une preuve suffisante (PID recyclé, autre machine, montage réseau) : la suppression forcée
reste une décision humaine, non branchée sur l'écriture normale.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import app.config as cfg

LOCK_NAME = ".saisie_charges_write.lock"
OPERATION_SAISIE_CHARGE = "saisie_charge"

# États retournés par inspecter_verrou (diagnostic uniquement — aucune action).
ETAT_ABSENT = "ABSENT"
ETAT_ACTIF_PROBABLE = "ACTIF_PROBABLE"
ETAT_POTENTIELLEMENT_PERIME = "POTENTIELLEMENT_PERIME"
ETAT_INDETERMINABLE = "INDETERMINABLE"


class VerrouSaisieChargesError(RuntimeError):
    """Base des erreurs de verrou."""


class VerrouSaisieChargesDejaPrisError(VerrouSaisieChargesError):
    """Un autre détenteur tient déjà le verrou. Aucune écriture ne doit avoir lieu."""

    def __init__(self, lock_path: Path, metadonnees: dict[str, Any] | None) -> None:
        self.lock_path = Path(lock_path)
        self.metadonnees = metadonnees or {}
        detenteur = (
            f"pid={self.metadonnees.get('pid')} sur {self.metadonnees.get('hostname')} "
            f"depuis {self.metadonnees.get('created_at_utc')}"
            if self.metadonnees else "détenteur inconnu (métadonnées illisibles)"
        )
        super().__init__(
            f"Écriture de charges déjà en cours — verrou détenu ({detenteur}). "
            f"Verrou : {self.lock_path}"
        )


class VerrouSaisieChargesInvalideError(VerrouSaisieChargesError):
    """Le fichier de verrou existe mais son contenu est illisible ou non conforme."""


class LiberationVerrouSaisieChargesError(VerrouSaisieChargesError):
    """La libération a échoué : verrou disparu, appartenant à un autre, ou non supprimable."""


@dataclass(frozen=True)
class VerrouSaisieCharges:
    """Preuve de détention. `token` est le seul élément qui autorise la libération."""

    lock_path: Path
    token: str
    metadonnees: dict[str, Any]

    def est_detenu(self) -> bool:
        """True si le fichier existe ET porte notre token (et pas celui d'un autre)."""
        try:
            return _lire_json(self.lock_path).get("token") == self.token
        except (VerrouSaisieChargesInvalideError, FileNotFoundError):
            return False


# ── Emplacement ──────────────────────────────────────────────────────────────

def chemin_verrou_par_defaut() -> Path:
    """Emplacement stable, propriété de l'application, sur le même environnement local.

    Jamais un dossier temporaire, jamais un dossier aléatoire, jamais dans Git.
    """
    return Path(cfg.DATA_DIR) / LOCK_NAME


# ── Lecture des métadonnées ──────────────────────────────────────────────────

def _lire_json(lock_path: Path) -> dict[str, Any]:
    contenu = Path(lock_path).read_text(encoding="utf-8")   # FileNotFoundError si absent
    try:
        donnees = json.loads(contenu)
    except json.JSONDecodeError as exc:
        raise VerrouSaisieChargesInvalideError(
            f"Verrou illisible ({lock_path}) : contenu JSON invalide ({exc}). "
            f"Ne pas le supprimer automatiquement — inspecter d'abord."
        ) from exc
    if not isinstance(donnees, dict):
        raise VerrouSaisieChargesInvalideError(
            f"Verrou illisible ({lock_path}) : objet JSON attendu, {type(donnees).__name__} lu."
        )
    return donnees


def lire_metadonnees(lock_path: Path | None = None) -> dict[str, Any] | None:
    """Métadonnées du verrou, ou None s'il n'existe pas. Lève si le contenu est corrompu."""
    p = Path(lock_path or chemin_verrou_par_defaut())
    try:
        return _lire_json(p)
    except FileNotFoundError:
        return None


# ── Acquisition / libération ─────────────────────────────────────────────────

def acquerir_verrou(
    operation: str = OPERATION_SAISIE_CHARGE,
    charge_id: str | None = None,
    lock_path: Path | None = None,
) -> VerrouSaisieCharges:
    """Acquiert le verrou de façon EXCLUSIVE et atomique. Lève si déjà pris.

    `charge_id` peut être None : le verrou est légitimement acquis AVANT que l'identifiant ne soit
    généré — c'est précisément ce qui rend la génération sûre.
    """
    p = Path(lock_path or chemin_verrou_par_defaut())
    p.parent.mkdir(parents=True, exist_ok=True)

    token = uuid.uuid4().hex
    metadonnees = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "operation": operation,
        "charge_id": charge_id,
        "token": token,
    }

    try:
        # Création ATOMIQUE : si le fichier existe déjà, le système lève, personne n'écrase rien.
        fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        metadonnees_existantes: dict[str, Any] | None
        try:
            metadonnees_existantes = _lire_json(p)
        except (VerrouSaisieChargesInvalideError, OSError):
            metadonnees_existantes = None       # verrou corrompu : on refuse quand même
        raise VerrouSaisieChargesDejaPrisError(p, metadonnees_existantes) from None

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(metadonnees, f, ensure_ascii=False, indent=2)
    except Exception:
        # Le verrou nous appartient : on le retire pour ne pas laisser un verrou fantôme.
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    return VerrouSaisieCharges(lock_path=p, token=token, metadonnees=metadonnees)


def liberer_verrou(verrou: VerrouSaisieCharges) -> None:
    """Libère le verrou — UNIQUEMENT si nous en sommes le détenteur (contrôle du token).

    Lève LiberationVerrouSaisieChargesError si le verrou a disparu, appartient à un autre détenteur,
    ou ne peut pas être supprimé. L'échec n'est jamais silencieux.
    """
    p = verrou.lock_path
    try:
        metadonnees = _lire_json(p)
    except FileNotFoundError:
        raise LiberationVerrouSaisieChargesError(
            f"Verrou déjà absent ({p}) — il a été supprimé par un tiers pendant l'opération."
        ) from None
    except VerrouSaisieChargesInvalideError as exc:
        raise LiberationVerrouSaisieChargesError(
            f"Verrou illisible ({p}) — suppression refusée : la propriété ne peut pas être "
            f"vérifiée. {exc}"
        ) from exc

    if metadonnees.get("token") != verrou.token:
        raise LiberationVerrouSaisieChargesError(
            f"Verrou détenu par un autre processus (pid={metadonnees.get('pid')}) — "
            f"suppression refusée. Aucun processus ne déverrouille le travail d'un autre."
        )

    try:
        p.unlink()
    except OSError as exc:
        raise LiberationVerrouSaisieChargesError(
            f"Suppression du verrou impossible ({p}) : {exc}. Le verrou reste en place."
        ) from exc


@contextmanager
def verrou_saisie_charges(
    operation: str = OPERATION_SAISIE_CHARGE,
    charge_id: str | None = None,
    lock_path: Path | None = None,
) -> Iterator[VerrouSaisieCharges]:
    """Détient le verrou pour toute la durée du bloc. Libéré dans un `finally`, quoi qu'il arrive.

    Si le corps lève ET que la libération échoue, l'erreur d'origine prime : l'échec de libération
    est rattaché en contexte (`__context__`), jamais masqué ni perdu.
    """
    verrou = acquerir_verrou(operation=operation, charge_id=charge_id, lock_path=lock_path)
    erreur_corps: BaseException | None = None
    try:
        yield verrou
    except BaseException as exc:
        erreur_corps = exc
        raise
    finally:
        try:
            liberer_verrou(verrou)
        except LiberationVerrouSaisieChargesError:
            if erreur_corps is None:
                raise            # rien d'autre n'a échoué : l'échec de libération doit remonter
            # Une erreur métier est déjà en cours : elle prime, mais on ne masque pas celle-ci
            # (elle reste chaînée dans __context__ et le verrou est resté en place, ce qui est
            # le comportement sûr).


# ── Diagnostic (ne supprime JAMAIS) ──────────────────────────────────────────

def _pid_actif(pid: int) -> bool | None:
    """True/False si on peut conclure, None si indéterminable.

    Windows : jamais `os.kill(pid, 0)` — sur cette plateforme, Python le traduit en
    TerminateProcess, ce qui **tuerait** le processus au lieu de le sonder.
    """
    if pid is None or pid <= 0:
        return None
    if sys.platform == "win32":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if not handle:
                return False        # le processus n'existe plus
            try:
                code = ctypes.c_ulong()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                    return None
                return code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return None
    try:
        os.kill(int(pid), 0)        # POSIX : signal 0 = sonde, n'envoie rien
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                 # existe, mais appartient à un autre utilisateur
    except OSError:
        return None


def inspecter_verrou(lock_path: Path | None = None) -> dict[str, Any]:
    """Diagnostic du verrou. **Ne supprime rien, jamais.**

    Retourne {"etat", "lock_path", "metadonnees", "raison"} avec etat parmi :
      ABSENT                  — aucun verrou.
      ACTIF_PROBABLE          — même machine, le processus détenteur tourne toujours.
      POTENTIELLEMENT_PERIME  — même machine, le PID détenteur n'existe plus. Ce n'est PAS une
                                preuve : un PID est recyclable. La suppression reste humaine.
      INDETERMINABLE          — verrou corrompu, autre machine, ou PID non sondable.
    """
    p = Path(lock_path or chemin_verrou_par_defaut())
    if not p.exists():
        return {"etat": ETAT_ABSENT, "lock_path": str(p), "metadonnees": None,
                "raison": "Aucun fichier de verrou."}

    try:
        metadonnees = _lire_json(p)
    except (VerrouSaisieChargesInvalideError, OSError) as exc:
        return {"etat": ETAT_INDETERMINABLE, "lock_path": str(p), "metadonnees": None,
                "raison": f"Verrou illisible : {exc}"}

    hote = metadonnees.get("hostname")
    if hote != socket.gethostname():
        return {"etat": ETAT_INDETERMINABLE, "lock_path": str(p), "metadonnees": metadonnees,
                "raison": f"Verrou posé par une autre machine ({hote}) — PID non sondable d'ici."}

    actif = _pid_actif(metadonnees.get("pid"))
    if actif is True:
        return {"etat": ETAT_ACTIF_PROBABLE, "lock_path": str(p), "metadonnees": metadonnees,
                "raison": f"Le processus {metadonnees.get('pid')} tourne toujours."}
    if actif is False:
        return {"etat": ETAT_POTENTIELLEMENT_PERIME, "lock_path": str(p),
                "metadonnees": metadonnees,
                "raison": (
                    f"Le processus {metadonnees.get('pid')} n'existe plus. Verrou PEUT-ÊTRE périmé "
                    f"— un PID est recyclable : aucune suppression automatique. Décision humaine."
                )}
    return {"etat": ETAT_INDETERMINABLE, "lock_path": str(p), "metadonnees": metadonnees,
            "raison": f"État du processus {metadonnees.get('pid')} non déterminable."}

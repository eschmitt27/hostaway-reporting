"""APP-2b/2c — Prévisualisation et confirmation d'une réservation HH, entièrement SQLite.

CE QUE CE MODULE REMPLACE
`saisie_hh_dryrun_service` (copie de travail du classeur, injection de ligne, comparaison Lot4A en
sous-processus) et `saisie_hh_real_write_service` (empreintes de fichiers, verrou, restauration de
snapshot) — toute cette machinerie protégeait des écritures Excel fragiles. Une transaction SQLite
(`reservations_hh_saisie_service.creer`) est atomique par construction : rien de tout cela n'a plus
d'objet.

CE QUI EST CONSERVÉ
La validation métier complète (`saisie_hh_service.valider`, règles D1-D11, dérogations menage et
commission) et le principe de prévisualisation : un token, un manifest JSON, une confirmation
séparée qui ne reçoit que ce token (le navigateur ne fournit plus aucune donnée métier à la
confirmation).
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.services import saisie_hh_service as saisie_svc
from app.services import reservations_hh_saisie_service as saisie

DRYRUNS_DIR = cfg.DRYRUNS_DIR
MANIFEST_NAME = "manifest_hh.json"
RESULTAT_NAME = "resultat_confirmation_hh.json"

DUREE_VIE_MANIFEST_HEURES = 24

SUCCES = "SUCCES"
REFUSE = "REFUSE"

E_TOKEN_INCONNU = "E_TOKEN_INCONNU"
E_MANIFEST_INVALIDE = "E_MANIFEST_INVALIDE"
E_MANIFEST_ILLISIBLE = "E_MANIFEST_ILLISIBLE"
E_MANIFEST_EXPIRE = "E_MANIFEST_EXPIRE"
E_VALIDATION_PERIMEE = "E_VALIDATION_PERIMEE"
E_ECRITURE_REFUSEE = "E_ECRITURE_REFUSEE"

MESSAGES: dict[str, str] = {
    E_TOKEN_INCONNU: "Cette prévisualisation est introuvable. Refaites la saisie.",
    E_MANIFEST_INVALIDE: "Cette prévisualisation n'est pas confirmable (validation non aboutie).",
    E_MANIFEST_ILLISIBLE: "La prévisualisation est illisible. Refaites la saisie.",
    E_MANIFEST_EXPIRE: f"Cette prévisualisation a plus de {DUREE_VIE_MANIFEST_HEURES} h. "
                       "Refaites la saisie pour repartir de l'état actuel des données.",
    E_VALIDATION_PERIMEE: "Les règles métier ne sont plus satisfaites (référentiel ou clôture "
                          "modifiés depuis la prévisualisation). Refaites la saisie.",
    E_ECRITURE_REFUSEE: "L'écriture a été refusée. Aucune réservation n'a été enregistrée.",
}


class DryRunError(RuntimeError):
    pass


def _token() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"HH_{stamp}_{uuid.uuid4().hex[:12]}"


def _safe_token(token: str) -> str:
    clean = str(token).strip()
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-TZ")
    if not clean or any(ch not in allowed for ch in clean):
        raise DryRunError("Identifiant de prévisualisation invalide")
    return clean


@dataclass
class ResultatConfirmation:
    token: str
    statut: str
    code: str | None = None
    message: str = ""
    reservation_hh_id: str | None = None
    horodatage_utc: str = ""

    @property
    def ok(self) -> bool:
        return self.statut == SUCCES

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def previsualiser(form_data: dict[str, str], *, db_path=None,
                  dryruns_root: Path | None = None) -> dict[str, Any]:
    """Valide la saisie et prépare le payload qui sera écrit dans `reservations_hors_hostaway` +
    `reservation_hh_overrides` (SQLite) à la confirmation. N'écrit rien avant la confirmation."""
    root = Path(dryruns_root or DRYRUNS_DIR)
    token = _token()
    run_dir = root / token
    run_dir.mkdir(parents=True, exist_ok=False)

    resultat_validation = saisie_svc.valider(form_data, db_path=db_path)

    manifest: dict[str, Any] = {
        "token": token,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "OK" if resultat_validation["ok"] else "VALIDATION_REFUSEE",
        "errors": resultat_validation["erreurs"],
        "form_data": form_data,
        "preview": resultat_validation.get("preview"),
        "pk": resultat_validation.get("pk"),
    }
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"ok": resultat_validation["ok"], "token": token, "run_dir": run_dir,
            "manifest": manifest}


def load_previsualisation(token: str, *, dryruns_root: Path | None = None) -> dict[str, Any]:
    simulation_id = _safe_token(token)
    root = Path(dryruns_root or DRYRUNS_DIR)
    run_dir = root / simulation_id
    manifest_path = run_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise DryRunError("Prévisualisation introuvable.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {"token": manifest["token"], "run_dir": run_dir, "manifest": manifest}


def _trop_vieux(manifest: dict[str, Any]) -> bool:
    cree = str(manifest.get("created_at_utc") or "")
    if not cree:
        return True
    try:
        instant = datetime.fromisoformat(cree)
    except ValueError:
        return True
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - instant > timedelta(hours=DUREE_VIE_MANIFEST_HEURES)


def _refus(token: str, code: str) -> ResultatConfirmation:
    return ResultatConfirmation(
        token=token, statut=REFUSE, code=code, message=MESSAGES.get(code, "Écriture refusée."),
        horodatage_utc=datetime.now(timezone.utc).isoformat())


def _chemin_resultat(token: str, root: Path) -> Path:
    return root / _safe_token(token) / RESULTAT_NAME


def resultat_existe(token: str, dryruns_root: Path | None = None) -> bool:
    try:
        return _chemin_resultat(token, Path(dryruns_root or DRYRUNS_DIR)).exists()
    except Exception:
        return False


def charger_resultat(token: str, dryruns_root: Path | None = None) -> dict[str, Any] | None:
    p = _chemin_resultat(token, Path(dryruns_root or DRYRUNS_DIR))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _enregistrer_resultat(token: str, resultat: ResultatConfirmation, root: Path) -> None:
    p = _chemin_resultat(token, root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(resultat.as_dict(), ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")


def confirmer(token: str, *, db_path=None, dryruns_root: Path | None = None,
             acteur: str = "") -> ResultatConfirmation:
    """Confirme une prévisualisation : écrit la réservation en SQLite (transaction atomique)."""
    root = Path(dryruns_root or DRYRUNS_DIR)

    try:
        data = load_previsualisation(token, dryruns_root=root)
    except DryRunError:
        return _refus(token, E_TOKEN_INCONNU)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return _refus(token, E_MANIFEST_ILLISIBLE)

    token = data["token"]
    manifest = data["manifest"]

    if manifest.get("status") != "OK":
        resultat = _refus(token, E_MANIFEST_INVALIDE)
        _enregistrer_resultat(token, resultat, root)
        return resultat

    if _trop_vieux(manifest):
        resultat = _refus(token, E_MANIFEST_EXPIRE)
        _enregistrer_resultat(token, resultat, root)
        return resultat

    if resultat_existe(token, root):
        existant = charger_resultat(token, root)
        if existant and existant.get("statut") == SUCCES:
            return ResultatConfirmation(**existant)

    # Revalidation métier sur l'état ACTUEL (mois clôturé, référentiel modifié entre-temps...).
    revalidation = saisie_svc.valider(manifest["form_data"], db_path=db_path)
    if not revalidation["ok"]:
        resultat = _refus(token, E_VALIDATION_PERIMEE)
        _enregistrer_resultat(token, resultat, root)
        return resultat

    row_data = saisie_svc.build_row_data(revalidation["preview"])
    row_data["mois"] = revalidation["preview"].get("mois")
    row_data["montant_percu"] = row_data.pop("total_percu", None)
    row_data["impact_resultat_comptable"] = row_data.pop("comptabilisation", None)
    row_data["menage_standard"] = revalidation["preview"].get("menage_standard")
    row_data["menage_standard_source"] = revalidation["preview"].get("menage_standard_source")
    row_data["taux_commission_standard"] = revalidation["preview"].get("taux_commission_standard")
    row_data["taux_commission_standard_source"] = revalidation["preview"].get(
        "taux_commission_standard_source")

    res = saisie.creer(row_data, acteur=acteur, db_path=db_path)
    if not res.get("ok"):
        resultat = ResultatConfirmation(
            token=token, statut=REFUSE, code=res.get("code", E_ECRITURE_REFUSEE),
            message=res.get("message") or MESSAGES[E_ECRITURE_REFUSEE],
            horodatage_utc=datetime.now(timezone.utc).isoformat())
        _enregistrer_resultat(token, resultat, root)
        return resultat

    resultat = ResultatConfirmation(
        token=token, statut=SUCCES, code=None,
        message=f"Réservation {res['reservation_hh_id']} enregistrée.",
        reservation_hh_id=res["reservation_hh_id"],
        horodatage_utc=datetime.now(timezone.utc).isoformat())
    _enregistrer_resultat(token, resultat, root)
    return resultat

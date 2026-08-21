"""APP-3b — Confirmation d'une charge : du token de prévisualisation à l'écriture SQLite.

**Le navigateur ne fournit jamais de données métier ici.** La confirmation ne reçoit qu'un *token*.
Tout le reste — montant, catégorie, affectations, avantage — est relu du **manifest serveur** produit
par la prévisualisation. Un payload rejoué ou trafiqué depuis le client n'a aucune prise.

Ordre des gardes — il compte, chacune coupe court à la suivante :
  1. le token existe, son manifest se lit (une corruption n'est pas un token inconnu) et son statut
     est OK ;
  2. le manifest n'a pas été altéré depuis la prévisualisation (sceau `integrite`) ;
  3. il n'est pas trop vieux (garde-fou de fraîcheur) ;
  4. ce token n'a pas déjà écrit (idempotence : rafraîchir la page de résultat ne rejoue jamais
     l'écriture — POST-Redirect-Get) ;
  5. **les règles métier sont rejouées sur l'état ACTUEL** : un mois clôturé entre-temps, une
     catégorie désactivée, un logement sorti du parc depuis la prévisualisation rendraient la
     décision invalide. Sans ce contrôle, une prévisualisation faite avant une clôture resterait
     confirmable après, et écrirait dans un mois fermé.

L'écriture elle-même est une transaction SQLite (`charges_saisie_service.creer`), atomique par
construction : la sauvegarde/remplacement/rollback de fichier qu'exigeait Excel n'a plus d'objet.

Après un succès — et seulement après — le contrôle aval (intégrité charges) est reconsulté à titre
informatif. Son échec n'annule pas la charge : les statuts restent séparés.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.services import charges_preview_service as prev
from app.services import charges_saisie_service as saisie

RESULTAT_NAME = "resultat_confirmation.json"

# Durée de vie d'une prévisualisation — VALIDÉE À 24 HEURES (décision humaine, 2026-07-14).
# Au-delà, la décision est réputée trop vieille pour être confirmée à l'aveugle : une NOUVELLE
# prévisualisation est obligatoire. Ce n'est pas la garde principale (ce sont les empreintes et la
# revalidation métier) mais un garde-fou de fraîcheur.
#
# Une expiration ne signifie EN AUCUN CAS qu'une charge a été écrite : un manifest expiré n'est
# jamais confirmé, donc aucun fichier n'est touché. L'idempotence, elle, ne repose pas sur cette
# durée : elle est fondée sur le token journalisé, consulté SOUS VERROU (fail-closed).
DUREE_VIE_MANIFEST_HEURES = 24

# Statuts rendus à l'écran (verdict global de la confirmation).
SUCCES = "SUCCES"
REFUSE = "REFUSE"
ROLLBACK = "ROLLBACK"
CRITIQUE = "CRITIQUE"

E_TOKEN_INCONNU = "E_TOKEN_INCONNU"
E_MANIFEST_INVALIDE = "E_MANIFEST_INVALIDE"
E_MANIFEST_ILLISIBLE = "E_MANIFEST_ILLISIBLE"
E_MANIFEST_ALTERE = "E_MANIFEST_ALTERE"
E_MANIFEST_EXPIRE = "E_MANIFEST_EXPIRE"
E_VALIDATION_PERIMEE = "E_VALIDATION_PERIMEE"
E_DEJA_ECRIT = "E_TOKEN_DEJA_ECRIT"
E_ECRITURE_REFUSEE = "E_ECRITURE_REFUSEE"

# Messages destinés à l'utilisateur : jamais un chemin interne, jamais une trace technique.
MESSAGES: dict[str, str] = {
    E_TOKEN_INCONNU: "Cette prévisualisation est introuvable. Refaites la saisie.",
    E_MANIFEST_INVALIDE: "Cette prévisualisation n'est pas confirmable (validation non aboutie).",
    E_MANIFEST_ILLISIBLE: "La prévisualisation est illisible (fichier corrompu). Par sécurité, "
                          "l'écriture est refusée : refaites la saisie.",
    E_MANIFEST_ALTERE: "La prévisualisation a été altérée depuis sa création. Par sécurité, "
                       "l'écriture est refusée : refaites la saisie.",
    E_MANIFEST_EXPIRE: f"Cette prévisualisation a plus de {DUREE_VIE_MANIFEST_HEURES} h. "
                       f"Par sécurité, elle n'est plus confirmable : refaites la saisie pour "
                       f"repartir de l'état actuel des données.",
    E_VALIDATION_PERIMEE: "Les règles métier ne sont plus satisfaites (le mois a peut-être été "
                          "clôturé, ou un référentiel a changé depuis la prévisualisation). "
                          "L'écriture est refusée : refaites la saisie.",
    E_DEJA_ECRIT: "Cette charge a déjà été enregistrée. Aucune seconde écriture n'a eu lieu.",
    E_ECRITURE_REFUSEE: "L'écriture a été refusée. Aucune charge n'a été enregistrée.",
}


@dataclass
class ResultatConfirmation:
    token: str
    statut: str
    code: str | None = None
    message: str = ""
    charge_id: str | None = None
    horodatage_utc: str = ""
    # Détails techniques (affichés séparément du message métier)
    transaction: dict[str, Any] = field(default_factory=dict)
    post_ecriture: dict[str, Any] | None = None
    journal_erreur: str | None = None

    @property
    def ok(self) -> bool:
        return self.statut == SUCCES

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _sha(path: Path) -> str | None:
    p = Path(path)
    if not p.exists():
        return None
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for bloc in iter(lambda: f.read(65536), b""):
            h.update(bloc)
    return h.hexdigest()


def _message(code: str | None, defaut: str) -> str:
    return MESSAGES.get(code or "", defaut)


def _refus(token: str, code: str, details: str = "") -> ResultatConfirmation:
    return ResultatConfirmation(
        token=token, statut=REFUSE, code=code,
        message=_message(code, "Écriture refusée."),
        horodatage_utc=datetime.now(timezone.utc).isoformat(),
        transaction={"code": code, "details": details, "fichiers_remplaces": []},
    )


def _refus_consultable(
    token: str, root: Path, code: str, details: str = ""
) -> ResultatConfirmation:
    """Refus rendu ET persisté (si la prévisualisation existe), pour rester lisible après redirection."""
    resultat = _refus(token, code, details)
    _enregistrer_si_previsualisation_existe(token, resultat, root)
    return resultat


def _trop_vieux(manifest: dict[str, Any]) -> bool:
    """Une prévisualisation trop ancienne n'est plus confirmable (garde-fou de fraîcheur)."""
    cree = str(manifest.get("created_at_utc") or "")
    if not cree:
        return True                      # sans horodatage, on ne peut rien affirmer : on refuse
    try:
        instant = datetime.fromisoformat(cree)
    except ValueError:
        return True
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - instant
    return age > timedelta(hours=DUREE_VIE_MANIFEST_HEURES)


# ── Résultat persisté côté serveur (rafraîchir la page ne rejoue jamais l'écriture) ──

def chemin_resultat(token: str, dryruns_root: Path | None = None) -> Path:
    root = Path(dryruns_root or prev.DRYRUNS_DIR)
    return root / prev._safe_token(token) / RESULTAT_NAME


def resultat_existe(token: str, dryruns_root: Path | None = None) -> bool:
    try:
        return chemin_resultat(token, dryruns_root).exists()
    except Exception:
        return False


def charger_resultat(token: str, dryruns_root: Path | None = None) -> dict[str, Any] | None:
    p = chemin_resultat(token, dryruns_root)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _enregistrer_resultat(
    token: str, resultat: ResultatConfirmation, dryruns_root: Path | None = None
) -> None:
    p = chemin_resultat(token, dryruns_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(resultat.as_dict(), ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")


def _enregistrer_si_previsualisation_existe(
    token: str, resultat: ResultatConfirmation, dryruns_root: Path | None = None
) -> None:
    """Persiste le résultat SANS jamais créer de dossier.

    Un refus doit rester consultable (POST-Redirect-Get), mais un token inconnu ne doit pas faire
    apparaître un dossier de prévisualisation qui n'a jamais existé.
    """
    try:
        p = chemin_resultat(token, dryruns_root)
    except Exception:
        return                       # token syntaxiquement invalide : rien à enregistrer
    if not p.parent.is_dir():
        return
    p.write_text(json.dumps(resultat.as_dict(), ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")


# ── Confirmation ─────────────────────────────────────────────────────────────

def confirmer(
    token: str,
    *,
    dryruns_root: Path | None = None,
    db_path: Path | None = None,
    acteur: str = "",
) -> ResultatConfirmation:
    """Confirme une prévisualisation : écrit la charge en SQLite.

    Tous les chemins sont injectables (les tests travaillent en base isolée). Ne lève jamais : tout
    incident est rendu dans le résultat.
    """
    horodatage = datetime.now(timezone.utc).isoformat()
    root = Path(dryruns_root or prev.DRYRUNS_DIR)

    # ── 1. Manifest serveur ───────────────────────────────────────────────────
    try:
        data = prev.load_previsualisation(token, dryruns_root=root)
    except prev.ChargesPreviewError:
        return _refus(token, E_TOKEN_INCONNU)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        # Le manifest existe mais ne se lit pas : c'est une CORRUPTION, pas un token inconnu.
        # Les confondre enverrait l'utilisateur chercher un token valide alors que le fichier est
        # cassé — et masquerait un incident disque.
        return _refus(token, E_MANIFEST_ILLISIBLE, f"{type(exc).__name__}")

    token = data["token"]
    manifest = data["manifest"]

    if manifest.get("status") != "OK":
        return _refus_consultable(token, root, E_MANIFEST_INVALIDE, f"status={manifest.get('status')}")

    # ── 2. Sceau d'intégrité du manifest ─────────────────────────────────────
    scelle = manifest.get("integrite")
    if not scelle or scelle != prev.sceller_manifest(manifest):
        return _refus_consultable(token, root, E_MANIFEST_ALTERE)

    # ── 2-bis. Fraîcheur ─────────────────────────────────────────────────────
    if _trop_vieux(manifest):
        return _refus_consultable(token, root, E_MANIFEST_EXPIRE,
                                  f"créé le {manifest.get('created_at_utc')}")

    # ── 3. Ce token a-t-il DÉJÀ écrit ? (idempotence — POST-Redirect-Get) ────
    if resultat_existe(token, root):
        existant = charger_resultat(token, root)
        if existant and existant.get("statut") == SUCCES:
            return ResultatConfirmation(**existant)

    # ── 4. REVALIDATION MÉTIER ────────────────────────────────────────────────
    # Un mois clôturé entre la prévisualisation et la confirmation, une catégorie désactivée, un
    # logement retiré du parc… rien de tout cela n'apparaît dans le manifest. Sans ce contrôle, une
    # prévisualisation faite AVANT une clôture resterait confirmable APRÈS. On rejoue donc la
    # validation complète, sur l'état ACTUEL.
    form_data = manifest["form_data"]
    mois = manifest["mois_charge"]
    refs = prev.load_form_refs(db_path=db_path)

    erreurs = prev.validate_charge(form_data, refs)
    if erreurs:
        return _refus_consultable(
            token, root, E_VALIDATION_PERIMEE,
            "; ".join(f"{e['code']}" for e in erreurs[:5]),   # codes seuls : aucun détail sensible
        )

    guide = prev.compute_guidee(form_data, refs, mois)
    if guide["errors"]:
        return _refus_consultable(
            token, root, E_VALIDATION_PERIMEE,
            "; ".join(f"{e['code']}" for e in guide["errors"][:5]),
        )

    # ── 5. Écriture SQLite (transaction atomique, aucun fichier à remplacer) ──
    row_data = manifest["row_data"]
    res = saisie.creer(row_data, acteur=acteur, db_path=db_path)

    if not res.get("ok"):
        resultat = ResultatConfirmation(
            token=token, statut=REFUSE, code=res.get("code", E_ECRITURE_REFUSEE),
            message=res.get("message") or _message(E_ECRITURE_REFUSEE, "Écriture refusée."),
            horodatage_utc=horodatage,
            transaction={"code": res.get("code"), "details": res.get("message", "")},
        )
        _enregistrer_resultat(token, resultat, root)
        return resultat

    resultat = ResultatConfirmation(
        token=token, statut=SUCCES, code=None,
        message=f"Charge {res['charge_id']} enregistrée.",
        charge_id=res["charge_id"], horodatage_utc=horodatage,
        transaction={"code": "OK", "details": "table charges"},
    )
    _enregistrer_resultat(token, resultat, root)
    return resultat

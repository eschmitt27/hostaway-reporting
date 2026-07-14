"""APP-3b — Confirmation d'une charge : du token de prévisualisation à l'écriture réelle.

C'est le maillon qui manquait : la chaîne (writer → transaction → verrou → journal) existait mais
n'avait aucun appelant. Ce service est cet appelant, et le SEUL.

**Le navigateur ne fournit jamais de données métier ici.** La confirmation ne reçoit qu'un *token*.
Tout le reste — montant, catégorie, affectations, avantage — est relu du **manifest serveur** produit
par la prévisualisation. Un payload rejoué ou trafiqué depuis le client n'a aucune prise.

Ordre des gardes — il compte, chacune coupe court à la suivante :
  0. **flags** : si l'écriture n'est pas activée, aucune autre cause de refus n'a de sens à être
     annoncée (dire « les fichiers ont changé » à une installation qui n'écrit rien serait un faux
     guidage). Refus immédiat, tracé au journal ;
  1. le token existe, son manifest se lit (une corruption n'est pas un token inconnu) et son statut
     est OK ;
  2. le manifest n'a pas été altéré depuis la prévisualisation (sceau `integrite`) ;
  3. il n'est pas trop vieux (garde-fou de fraîcheur) ;
  4. ce token n'a pas déjà écrit (pré-contrôle ; la garde autoritaire est sous verrou) ;
  5. la copie de travail correspond bien à ce manifest ;
  6. les fichiers RÉELS ont toujours l'empreinte qu'ils avaient à la prévisualisation — sinon la
     base a bougé et la décision est périmée ;
  7. **les règles métier sont rejouées sur l'état ACTUEL** : les empreintes ne couvrent que les deux
     fichiers de saisie, pas les référentiels. Un mois clôturé entre-temps, une catégorie
     désactivée, un logement sorti du parc ne changent aucune empreinte — mais rendent la décision
     invalide. Sans ce contrôle, une prévisualisation faite avant une clôture resterait confirmable
     après, et écrirait dans un mois fermé ;
  8. le verrou, l'unicité du charge_id, la transaction : gardés par l'orchestrateur, pas ici.

Le `charge_id` définitif est **résolu sous verrou par l'orchestrateur**, jamais figé ici : celui du
manifest est indicatif. On fournit un préfixe et une fabrique de payload — la fabrique rappelle les
constructeurs existants (`compute_guidee`, `_build_row_data`, `build_persistable`), donc aucune règle
métier n'est dupliquée.

Après un succès — et seulement après — les lots aval sont déclenchés (Lot3, Lot7 si avantage, Lot11).
Leur échec n'annule pas la charge : les statuts restent séparés.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.services import charges_impacts_persist_service as persist
from app.services import charges_post_write_service as aval
from app.services import charges_preview_service as prev
from app.services import saisie_charges_transaction_service as tx

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

# Codes propres à la confirmation (ceux de la transaction sont repris tels quels).
E_TOKEN_INCONNU = "E_TOKEN_INCONNU"
E_MANIFEST_INVALIDE = "E_MANIFEST_INVALIDE"
E_MANIFEST_ILLISIBLE = "E_MANIFEST_ILLISIBLE"
E_MANIFEST_ALTERE = "E_MANIFEST_ALTERE"
E_MANIFEST_EXPIRE = "E_MANIFEST_EXPIRE"
E_COPIE_ALTEREE = "E_COPIE_ALTEREE"
E_SOURCE_MODIFIEE = "E_SOURCE_MODIFIEE"
E_VALIDATION_PERIMEE = "E_VALIDATION_PERIMEE"

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
    E_COPIE_ALTEREE: "La copie de travail de cette prévisualisation n'est plus intacte. "
                     "Par sécurité, l'écriture est refusée : refaites la saisie.",
    E_SOURCE_MODIFIEE: "Les fichiers de saisie ont changé depuis la prévisualisation. "
                       "Cette décision est périmée : refaites la saisie pour repartir de l'état actuel.",
    tx.E_FLAGS_DESACTIVES: "L'écriture réelle n'est pas activée sur cette installation. "
                           "Aucune donnée n'a été modifiée.",
    tx.E_VERROU_DEJA_PRIS: "Une autre écriture de charge est en cours. Réessayez dans un instant.",
    tx.E_TOKEN_DEJA_ECRIT: "Cette charge a déjà été enregistrée. Aucune seconde écriture n'a eu lieu.",
    tx.E_IDEMPOTENCE_INDISPONIBLE: "Impossible de vérifier si cette charge a déjà été enregistrée "
                                   "(journal indisponible). Par sécurité, l'écriture est refusée.",
    tx.E_CHARGE_ID_EXISTANT: "L'identifiant de charge est déjà utilisé. Refaites la saisie.",
    tx.E_CHARGE_ID_INVALIDE: "L'identifiant de charge est invalide. Refaites la saisie.",
    tx.E_DEMANDE_INVALIDE: "La demande d'écriture est invalide.",
    tx.E_PREPARATION: "L'écriture a été refusée avant toute modification (contrôle de cohérence).",
    tx.E_TEMPORAIRE_MANQUANT: "L'écriture a été refusée avant toute modification (fichier de travail).",
    tx.E_TEMPORAIRE_INVALIDE: "L'écriture a été refusée avant toute modification (fichier de travail).",
    tx.E_SAUVEGARDE: "L'écriture a été refusée : la sauvegarde préalable a échoué.",
    tx.E_REMPLACEMENT: "L'écriture a échoué et tout a été restauré. Aucune charge n'a été enregistrée.",
    tx.E_POST_COMMIT: "L'écriture a échoué et tout a été restauré. Aucune charge n'a été enregistrée.",
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


def _tracer_refus_flags(
    token: str, saisie_path: Path | None, impacts_path: Path | None, db_path: Path | None
) -> str | None:
    """Journalise le refus de garde. Retourne le message d'erreur si le journal a lâché.

    La transaction journalise elle-même ses propres refus ; ici elle n'est pas atteinte (on refuse
    avant), donc la trace doit être posée par nous — sinon un refus flags disparaîtrait du journal.
    """
    saisie = Path(saisie_path or cfg.SAISIE_CHARGES)
    impacts = Path(impacts_path or cfg.SAISIE_CHARGES_IMPACTS)
    trace = tx.journal.ouvrir_trace(
        token_previsualisation=token,
        charge_id=None,
        charge_id_source=tx.journal.ID_DEMANDE,
        cible_saisie=saisie,
        cible_impacts=impacts,
        db_path=Path(db_path) if db_path else None,
    )
    empreinte_saisie = _sha(saisie)
    empreinte_impacts = _sha(impacts)
    trace.sha256_saisie_avant = empreinte_saisie
    trace.sha256_impacts_avant = empreinte_impacts
    return tx.journal.cloturer_trace_signalee(
        trace, tx.journal.REFUSE_FLAGS,
        code=tx.E_FLAGS_DESACTIVES,
        details="Écriture réelle non activée (flags). Aucun fichier touché.",
        sha256_saisie_apres=empreinte_saisie,       # rien n'a bougé : avant == après
        sha256_impacts_apres=empreinte_impacts,
    )


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
    saisie_path: Path | None = None,
    impacts_path: Path | None = None,
    master_path: Path | None = None,
    lot7_path: Path | None = None,
    ref_path: Path | None = None,
    lock_path: Path | None = None,
    db_path: Path | None = None,
    post_ecriture: bool = True,
    python_moteur: Path | None = None,
) -> ResultatConfirmation:
    """Confirme une prévisualisation : écrit la charge, puis déclenche les lots aval.

    Tous les chemins sont injectables (les tests travaillent sur copies isolées) et retombent sinon
    sur les fichiers réels. Ne lève jamais : tout incident est rendu dans le résultat.
    """
    horodatage = datetime.now(timezone.utc).isoformat()
    root = Path(dryruns_root or prev.DRYRUNS_DIR)

    # ── 0. Flags — EN PREMIER ────────────────────────────────────────────────
    # Si l'écriture n'est pas activée, aucune autre cause de refus n'a de sens à être annoncée :
    # dire « les fichiers ont changé, refaites la saisie » à quelqu'un dont l'installation n'écrit
    # de toute façon rien serait un faux guidage. On tranche donc ici, et on trace.
    if not tx.flags_actifs():
        resultat = _refus(token, tx.E_FLAGS_DESACTIVES)
        resultat.journal_erreur = _tracer_refus_flags(token, saisie_path, impacts_path, db_path)
        # Consultable via la page de résultat (POST-Redirect-Get), mais sans jamais créer de dossier
        # pour un token qui n'existe pas.
        _enregistrer_si_previsualisation_existe(token, resultat, root)
        return resultat

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
    run_dir = Path(data["run_dir"])
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

    # ── 3. Ce token a-t-il DÉJÀ écrit ? ──────────────────────────────────────
    # Consulté AVANT les empreintes : après une première écriture, les fichiers ont forcément
    # changé, et l'utilisateur qui double-clique doit lire « déjà enregistrée », pas « les fichiers
    # ont changé, refaites la saisie » — qui l'enverrait ressaisir une charge qui existe déjà.
    # Ce n'est qu'un pré-contrôle de confort : la garde AUTORITAIRE reste celle de l'orchestrateur,
    # prise sous verrou. Si le journal est illisible ici, on n'en tire aucune conclusion et on
    # laisse l'orchestrateur trancher (il est fail-closed).
    try:
        deja = tx.journal.token_deja_ecrit(token, db_path=Path(db_path) if db_path else None)
    except Exception:
        deja = None
    if deja is not None:
        resultat = ResultatConfirmation(
            token=token, statut=REFUSE, code=tx.E_TOKEN_DEJA_ECRIT,
            message=_message(tx.E_TOKEN_DEJA_ECRIT, "Charge déjà enregistrée."),
            charge_id=str(deja.get("charge_id") or "") or None,
            horodatage_utc=horodatage,
            transaction={"code": tx.E_TOKEN_DEJA_ECRIT,
                         "transaction_id": deja.get("transaction_id"),
                         "details": f"Écrite le {deja.get('fin_utc')}.",
                         "fichiers_remplaces": []},
        )
        return resultat        # on ne réécrit PAS le resultat_confirmation.json du succès initial

    # ── 4. Copie de travail conforme au manifest ─────────────────────────────
    copie = Path(manifest.get("paths", {}).get("saisie_copy", run_dir / prev.SAISIE_COPY_NAME))
    if not copie.exists() or _sha(copie) != manifest.get("copy_hash"):
        return _refus_consultable(token, root, E_COPIE_ALTEREE)

    # ── 5. Les fichiers RÉELS n'ont pas bougé depuis la prévisualisation ─────
    saisie = Path(saisie_path or cfg.SAISIE_CHARGES)
    impacts = Path(impacts_path or cfg.SAISIE_CHARGES_IMPACTS)
    if _sha(saisie) != manifest.get("source_hash_avant"):
        return _refus_consultable(token, root, E_SOURCE_MODIFIEE, "SAISIE_Charges_Flux")
    if _sha(impacts) != manifest.get("impacts_hash_avant"):
        return _refus_consultable(token, root, E_SOURCE_MODIFIEE, "SAISIE_Charges_Impacts")

    # ── 6. REVALIDATION MÉTIER ───────────────────────────────────────────────
    # Les empreintes ne couvrent que les deux fichiers de SAISIE. Or les règles dépendent aussi des
    # référentiels (REF_Setup) : un mois clôturé entre la prévisualisation et la confirmation, une
    # catégorie désactivée, un logement retiré du parc… Rien de tout cela ne change les empreintes.
    # Sans ce contrôle, une prévisualisation faite AVANT une clôture resterait confirmable APRÈS —
    # et écrirait dans un mois fermé. On rejoue donc la validation complète, sur l'état ACTUEL.
    form_data = manifest["form_data"]
    mois = manifest["mois_charge"]
    montant = float(str(form_data.get("montant", "0")).replace(",", "."))
    refs = prev.load_form_refs(ref_path)

    erreurs = prev.validate_charge(form_data, refs, saisie_path=saisie)
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

    def fabriquer(charge_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Construit le payload définitif une fois le charge_id résolu SOUS VERROU.

        Rappelle les constructeurs existants : aucune règle métier n'est réécrite ici.
        """
        row_data = prev._build_row_data(
            form_data, charge_id,
            profil_impact=manifest.get("profil_impact"),
            type_flux_id=manifest.get("type_flux_id"),
            guide=guide,
        )
        persistable = persist.build_persistable(charge_id, mois, montant, guide, form_data)
        return row_data, persistable

    prefixe = str(manifest["charge_id"]).rsplit("-", 1)[0]   # le numéro sera choisi sous verrou

    demande = tx.DemandeEcritureCharge(
        montant=montant,
        target_row=int(manifest["target_row"]),
        saisie_path=saisie,
        impacts_path=impacts,
        charge_id="",                       # résolu sous verrou — jamais figé ici
        charge_id_prefixe=prefixe,
        payload_factory=fabriquer,
        master_path=Path(master_path) if master_path else None,
        lock_path=Path(lock_path) if lock_path else None,
        db_path=Path(db_path) if db_path else None,
        token_previsualisation=token,       # clé d'idempotence, transmise au journal
        sha256_saisie_attendu=manifest.get("source_hash_avant"),
        sha256_impacts_attendu=manifest.get("impacts_hash_avant"),
    )

    # ── 7. Transaction (flags, verrou, charge_id, écriture, journal) ─────────
    try:
        res = tx.confirmer_ecriture_charge(demande)
    except tx.RollbackCritiqueError as exc:
        resultat = ResultatConfirmation(
            token=token, statut=CRITIQUE, code=tx.E_REMPLACEMENT,
            message=(
                "INCIDENT : l'écriture a échoué et la restauration n'a pas pu aboutir. "
                "Les fichiers de saisie peuvent être dans un état incohérent — ne pas relancer, "
                "prévenir immédiatement. Les sauvegardes ont été conservées."
            ),
            horodatage_utc=horodatage,
            transaction={
                "statut": "ROLLBACK_CRITIQUE",
                "fichiers_non_restaures": [
                    Path(f.get("cible", "")).name for f in exc.fichiers_non_restaures
                ],
            },
            journal_erreur=getattr(exc, "journal_erreur", None),
        )
        _enregistrer_resultat(token, resultat, root)
        return resultat

    detail_tx = {
        "statut": res.statut,
        "statut_journal": res.statut_journal,
        "code": res.code,
        "details": res.details,
        "transaction_id": res.transaction_id,
        "fichiers_remplaces": [Path(f).name for f in res.fichiers_remplaces],
        "rollback_tente": res.rollback_tente,
        "rollback_reussi": res.rollback_reussi,
    }

    if res.statut == tx.STATUT_ROLLBACK:
        resultat = ResultatConfirmation(
            token=token, statut=ROLLBACK, code=res.code,
            message=_message(res.code, "L'écriture a échoué. Tout a été restauré."),
            horodatage_utc=horodatage, transaction=detail_tx, journal_erreur=res.journal_erreur,
        )
        _enregistrer_resultat(token, resultat, root)
        return resultat

    if not res.ok:
        resultat = ResultatConfirmation(
            token=token, statut=REFUSE, code=res.code,
            message=_message(res.code, "Écriture refusée."),
            charge_id=res.charge_id or None,
            horodatage_utc=horodatage, transaction=detail_tx, journal_erreur=res.journal_erreur,
        )
        _enregistrer_resultat(token, resultat, root)
        return resultat

    # ── 8. Succès : lots aval, HORS transaction ──────────────────────────────
    post: dict[str, Any] | None = None
    if post_ecriture:
        chemins = aval.CheminsMoteur(
            saisie=saisie,
            ref=Path(ref_path or cfg.REF_SETUP),
            master_charges=Path(master_path or cfg.MASTER_CHARGES),
            lot7=Path(lot7_path or cfg.SAISIE_IK_AVANTAGES),
        )
        post = aval.executer_post_ecriture(
            charge_id=res.charge_id,
            avantage=bool(manifest.get("avantage_associe")),
            chemins=chemins,
            run_dir=run_dir,
            python_moteur=python_moteur,
        ).as_dict()

    message = f"Charge {res.charge_id} enregistrée."
    if post is not None and not post["ok"]:
        # La charge EST écrite. On ne prétend pas le contraire.
        message += (" L'écriture est bien effectuée, mais le recalcul aval a échoué : "
                    "les masters doivent être régénérés avant exploitation.")

    resultat = ResultatConfirmation(
        token=token, statut=SUCCES, code=None, message=message,
        charge_id=res.charge_id, horodatage_utc=horodatage,
        transaction=detail_tx, post_ecriture=post, journal_erreur=res.journal_erreur,
    )
    _enregistrer_resultat(token, resultat, root)
    return resultat

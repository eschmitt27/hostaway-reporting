"""Import de factures fournisseurs par PDF — prévisualisation puis confirmation.

**Aucun nouvel extracteur.** Réutilise `02_TRAVAIL/lib_menages_externes_pdf.py` (`extraire_pdf`),
qui lit le texte NATIF via PyMuPDF et n'utilise jamais d'OCR. Cet extracteur ne connaît que deux
formats fournisseur ; pour tout autre PDF, le parcours ne échoue PAS : il bascule en saisie
manuelle pré-remplie de ce qui a pu être lu, et le dit explicitement à l'utilisateur.

Principes identiques à l'import bancaire déjà livré :
- le fichier importé est copié sous `DRYRUNS_DIR` et n'est jamais modifié ;
- aucune écriture en base avant confirmation explicite (manifeste + token) ;
- la confirmation crée la FACTURE uniquement — **jamais la charge**, qui reste produite par le
  parcours Charges puis rattachée depuis la fiche facture.
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.services import factures_service as fact

MANIFEST_NAME = "manifest.json"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_FORMAT_FICHIER = "V01_FORMAT_FICHIER_INCONNU"
E_PDF_ILLISIBLE = "V02_PDF_ILLISIBLE"
E_EXTRACTEUR_INDISPONIBLE = "V03_EXTRACTEUR_INDISPONIBLE"
E_TOKEN_INCONNU = "E01_TOKEN_INCONNU"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : l'import est impossible.",
    E_FORMAT_FICHIER: "Format de fichier non reconnu (PDF attendu).",
    E_PDF_ILLISIBLE: "PDF illisible ou vide.",
    E_EXTRACTEUR_INDISPONIBLE: "Extracteur PDF indisponible (PyMuPDF absent de cet environnement).",
    E_TOKEN_INCONNU: "Prévisualisation introuvable ou expirée : reprenez l'import.",
}


def _flags_actifs() -> bool:
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _charger_extracteur():
    """Import paresseux du moteur Lot6c (hors paquet `app/`). None si indisponible."""
    racine = Path(cfg.PIPELINE_SCRIPTS_ROOT)
    if str(racine) not in sys.path:
        sys.path.insert(0, str(racine))
    try:
        import lib_menages_externes_pdf as moteur
        return moteur
    except Exception:
        return None


def previsualiser(contenu: bytes, nom_fichier: str, *, fournisseur_id_opaque: str = "",
                  dryruns_root: Path | None = None, db_path=None) -> dict[str, Any]:
    """Copie le PDF sous DRYRUNS_DIR, l'extrait, propose des valeurs — sans rien écrire en base."""
    if not nom_fichier.lower().endswith(".pdf"):
        return _refus(E_FORMAT_FICHIER, nom_fichier)

    moteur = _charger_extracteur()
    if moteur is None:
        return _refus(E_EXTRACTEUR_INDISPONIBLE)

    token = uuid.uuid4().hex
    root = Path(dryruns_root or cfg.DRYRUNS_DIR) / "factures_import" / token
    root.mkdir(parents=True, exist_ok=True)
    copie = root / Path(nom_fichier).name
    copie.write_bytes(contenu)              # copie de travail ; l'original n'est jamais touché

    try:
        extrait = moteur.extraire_pdf(copie)
    except Exception as exc:
        return _refus(E_PDF_ILLISIBLE, f"{type(exc).__name__}: {exc}")

    statut = getattr(extrait, "statut_extraction", moteur.EX_ERREUR)
    supporte = statut == moteur.EX_OK
    if statut in (moteur.EX_VIDE, moteur.EX_CORROMPU):
        return _refus(E_PDF_ILLISIBLE, statut)

    # Champs proposés — jamais inventés : ce qui n'a pas été lu reste vide et est signalé.
    propose = {
        "facture_ref": getattr(extrait, "numero_facture", None) or "",
        "date_facture": getattr(extrait, "date_facture", None) or "",
        "date_echeance": "",
        "montant_ttc": getattr(extrait, "montant_total_facture", None),
        "montant_ht": None,
        "montant_tva": None,
        "devise": getattr(extrait, "devise", "EUR"),
        "fournisseur_id_opaque": fournisseur_id_opaque,
        "justificatif": Path(nom_fichier).name,
        "source": "PDF",
    }

    incertitudes: list[str] = list(getattr(extrait, "anomalies", []) or [])
    if not supporte:
        incertitudes.insert(0, (
            "Format fournisseur non reconnu par l'extracteur : les champs n'ont pas pu être lus "
            "automatiquement. Complétez-les à la main avant de confirmer."))
    for champ, libelle in (("facture_ref", "référence"), ("date_facture", "date de facture"),
                           ("montant_ttc", "montant TTC")):
        if not propose.get(champ):
            incertitudes.append(f"{libelle} non extraite du PDF — saisie manuelle requise")
    if not fournisseur_id_opaque:
        incertitudes.append("fournisseur non déterminé — à sélectionner avant confirmation")

    # Doublons : seulement calculables si on connaît fournisseur + référence/montant.
    doublon_certain = False
    doublons_probables: list[dict[str, Any]] = []
    if fournisseur_id_opaque and propose["facture_ref"]:
        doublon_certain = bool(fact._doublon_certain(
            fournisseur_id_opaque, propose["facture_ref"], db_path))
    if fournisseur_id_opaque and propose["montant_ttc"]:
        doublons_probables = fact.doublons_probables(
            fournisseur_id_opaque, propose["montant_ttc"], propose["date_facture"], db_path=db_path)

    lignes = [{"libelle": getattr(l, "libelle", ""), "montant": getattr(l, "montant", None),
               "logement_id": getattr(l, "logement_id", None)}
              for l in (getattr(extrait, "lignes", []) or [])]

    manifest = {
        "token": token, "nom_fichier": Path(nom_fichier).name, "copie": str(copie),
        "format_detecte": getattr(extrait, "format_detecte", ""), "statut_extraction": statut,
        "supporte": supporte, "propose": propose, "lignes": lignes,
        "incertitudes": incertitudes, "doublon_certain": doublon_certain,
        "doublons_probables": [{"facture_ref": d["facture_ref"], "montant_ttc": d["montant_ttc"],
                                "date_facture": d["date_facture"]} for d in doublons_probables],
        "somme_lignes": getattr(extrait, "somme_lignes", None),
        "ecart_reconciliation": getattr(extrait, "ecart_reconciliation", None),
        "cree_le": datetime.now(timezone.utc).isoformat(),
    }
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    return {"ok": True, "token": token, **manifest}


def charger_manifest(token: str, dryruns_root: Path | None = None) -> dict[str, Any] | None:
    p = Path(dryruns_root or cfg.DRYRUNS_DIR) / "factures_import" / token / MANIFEST_NAME
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def confirmer(token: str, corrections: dict[str, Any] | None = None, *, acteur: str = "",
              dryruns_root: Path | None = None, db_path=None) -> dict[str, Any]:
    """Crée la FACTURE à partir du manifeste, complété par les corrections saisies à l'écran.

    Ne crée JAMAIS la charge : la facture sort en `A_CONTROLER`, la charge reste à produire par le
    parcours Charges puis à rattacher depuis la fiche facture.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    manifest = charger_manifest(token, dryruns_root)
    if manifest is None:
        return _refus(E_TOKEN_INCONNU, token)

    form = dict(manifest["propose"])
    for cle, val in (corrections or {}).items():
        if str(val).strip() != "":
            form[cle] = val

    res = fact.creer(form, acteur=acteur, db_path=db_path,
                     fournisseur_actif=None)
    if not res.get("ok"):
        return res
    return {**res, "source": "PDF", "nom_fichier": manifest["nom_fichier"]}

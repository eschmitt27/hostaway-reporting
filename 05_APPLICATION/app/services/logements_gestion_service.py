"""Cycle de vie d'un logement existant : modification, archivage, réactivation, changement de
propriétaire, changement de taux de commission — écriture gardée et validée.

Complète `logements_creation_service.py` (qui ne fait que la création). Périmètre fixé par
`AUDIT_CIBLE_FORFAITS_ET_TAUX_LOGEMENTS.md` :
- `changer_proprietaire()` clôt le rattachement `REF_Gestion_Logements_Hist` actif (date_fin =
  veille de la nouvelle date) et ouvre une nouvelle ligne — jamais de modification d'une ligne
  historique déjà close (règle « ne jamais modifier les mois passés »).
- `changer_taux_commission()` fonctionne au grain **logement** (`logement_id` obligatoire),
  cohérent avec `resolve_commission_rate()` (Lot10, `lib_ref_history.py`) qui priorise déjà une
  ligne à grain logement sur une ligne à grain propriétaire : aucun changement de moteur requis.
- `modifier()` ne touche jamais `logement_id`, `proprietaire_id` ni le taux (routes dédiées) —
  seuls les champs descriptifs (nom, adresse, type, forfait...).
- `archiver()` / `reactiver()` basculent `actif`/`statut_parc` et closent/ouvrent le rattachement
  de gestion en cours, sans jamais supprimer de ligne.
"""
from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

SH_LOG = "REF_Logements"
SH_GEST = "REF_Gestion_Logements_Hist"
SH_PROP = "REF_Proprietaires"
SH_TAUX = "REF_Taux_Commission"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_LOGEMENT_INCONNU = "V01_LOGEMENT_INCONNU"
E_PROP_MANQUANT = "V02_PROPRIETAIRE_MANQUANT"
E_PROP_INCONNU = "V03_PROPRIETAIRE_INCONNU"
E_DATE_INVALIDE = "V04_DATE_INVALIDE"
E_TAUX_INVALIDE = "V05_TAUX_INVALIDE"
E_DEJA_ACTIF = "V06_DEJA_ACTIF"
E_DEJA_ARCHIVE = "V07_DEJA_ARCHIVE"
E_ECRITURE = "E_ECRITURE_REFUSEE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : la modification est impossible.",
    E_LOGEMENT_INCONNU: "Ce logement n'existe pas dans le référentiel.",
    E_PROP_MANQUANT: "Le nouveau propriétaire est obligatoire.",
    E_PROP_INCONNU: "Ce propriétaire n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "La date est invalide (format AAAA-MM-JJ attendu).",
    E_TAUX_INVALIDE: "Le taux de commission doit être un nombre entre 0 et 1 (ex. 0.15 pour 15 %).",
    E_DEJA_ACTIF: "Ce logement est déjà actif.",
    E_DEJA_ARCHIVE: "Ce logement est déjà archivé.",
    E_ECRITURE: "Écriture refusée : la cible n'est pas autorisée.",
}


def _flags_actifs() -> bool:
    return bool(cfg.CHARGES_REAL_WRITE_ENABLED and cfg.CHARGES_REAL_WRITE_CONFIRMATION_ENABLED)


def _txt(v) -> str:
    return str(v or "").strip()


def _date_valide(d: str) -> bool:
    try:
        date.fromisoformat(d)
        return True
    except ValueError:
        return False


def _veille(d: str) -> str:
    return (date.fromisoformat(d) - timedelta(days=1)).isoformat()


def _lire(wb, sheet: str) -> tuple[list[str], list[dict[str, Any]]]:
    if sheet not in wb.sheetnames:
        return [], []
    ws = wb[sheet]
    data = list(ws.iter_rows(values_only=True))
    if not data:
        return [], []
    hdr = [c for c in data[0]]
    return hdr, [dict(zip(hdr, r)) for r in data[1:]]


def _proprietaires_actifs(wb) -> set[str]:
    _, props = _lire(wb, SH_PROP)
    return {_txt(r.get("proprietaire_id")) for r in props
            if _txt(r.get("proprietaire_id")) and _txt(r.get("actif")).upper() == "OUI"}


def _ecrire(p: Path, wb, cb_erreur) -> dict[str, Any] | None:
    """Sauvegarde `wb` dans un temporaire puis remplacement atomique gardé. Retourne un dict
    d'erreur si le remplacement échoue, sinon None."""
    from app.services.saisie_charges_transaction_service import _remplacer_fichier
    fd, tmp = tempfile.mkstemp(suffix=p.suffix, dir=str(p.parent))
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        wb.save(tmp_path)
        _remplacer_fichier(tmp_path, p)
        return None
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        return cb_erreur(f"{type(exc).__name__}: {exc}")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _logement_ligne(wb, logement_id: str):
    ws = wb[SH_LOG]
    hdr = [c.value for c in ws[1]]
    if "logement_id" not in hdr:
        return None, None
    ci = hdr.index("logement_id")
    for row in ws.iter_rows(min_row=2):
        if str(row[ci].value or "").strip() == logement_id:
            return hdr, row
    return hdr, None


def _gestion_active(wb, logement_id: str):
    """Dernière ligne de rattachement sans date_fin (ou la plus récente) pour ce logement."""
    ws = wb[SH_GEST]
    hdr = [c.value for c in ws[1]]
    ci = hdr.index("logement_id")
    fi = hdr.index("date_fin")
    candidates = [row for row in ws.iter_rows(min_row=2)
                  if str(row[ci].value or "").strip() == logement_id
                  and not str(row[fi].value or "").strip()]
    if not candidates:
        return hdr, None
    return hdr, candidates[-1]


def modifier(logement_id: str, form: dict[str, Any], ref_path: Path | None = None) -> dict[str, Any]:
    """Met à jour les champs descriptifs d'un logement. Ne modifie jamais `logement_id`,
    `proprietaire_id` ni le taux de commission (routes dédiées `changer_proprietaire`/
    `changer_taux_commission`)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)

    p = Path(ref_path or cfg.REF_SETUP)
    if not p.exists():
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    wb = openpyxl.load_workbook(p, keep_vba=p.suffix.lower() == ".xlsm")
    try:
        hdr, row = _logement_ligne(wb, logement_id)
        if row is None:
            return _refus(E_LOGEMENT_INCONNU, logement_id)

        t = _txt(form.get("type_logement_id"))
        if t:
            _, types = _lire(wb, "REF_Types_Logements")
            types_connus = {_txt(x.get("type_logement_id")) for x in types}
            if types_connus and t not in types_connus:
                return _refus("V08_TYPE_INCONNU", t)

        champs_modifiables = ("nom_logement_officiel", "nom_court", "adresse", "ville",
                              "type_logement_id", "hostaway_listing_id", "sur_hostaway",
                              "commentaire", "forfait_logiciel_consommables_mensuel")
        for champ in champs_modifiables:
            if champ in form and champ in hdr:
                row[hdr.index(champ)].value = form.get(champ) or None

        err = _ecrire(p, wb, lambda d: _refus(E_ECRITURE, d))
        if err:
            return err
    finally:
        wb.close()

    return {"ok": True, "logement_id": logement_id}


def archiver(logement_id: str, date_fin: str, ref_path: Path | None = None) -> dict[str, Any]:
    """Archive un logement : `actif=NON`, `statut_parc=RETIRE`, clôture le rattachement de
    gestion en cours à `date_fin`. Ne supprime aucune ligne."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if not _date_valide(date_fin):
        return _refus(E_DATE_INVALIDE, date_fin)

    p = Path(ref_path or cfg.REF_SETUP)
    if not p.exists():
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    wb = openpyxl.load_workbook(p, keep_vba=p.suffix.lower() == ".xlsm")
    try:
        hdr, row = _logement_ligne(wb, logement_id)
        if row is None:
            return _refus(E_LOGEMENT_INCONNU, logement_id)
        if _txt(row[hdr.index("actif")].value).upper() == "NON":
            return _refus(E_DEJA_ARCHIVE, logement_id)

        row[hdr.index("actif")].value = "NON"
        if "statut_parc" in hdr:
            row[hdr.index("statut_parc")].value = "RETIRE"

        hdrg, ligne_gest = _gestion_active(wb, logement_id)
        if ligne_gest is not None:
            ligne_gest[hdrg.index("date_fin")].value = date_fin
            ligne_gest[hdrg.index("statut_gestion")].value = "RETIRE"

        err = _ecrire(p, wb, lambda d: _refus(E_ECRITURE, d))
        if err:
            return err
    finally:
        wb.close()

    return {"ok": True, "logement_id": logement_id, "date_fin": date_fin}


def reactiver(logement_id: str, date_debut: str, proprietaire_id: str,
             ref_path: Path | None = None) -> dict[str, Any]:
    """Réactive un logement archivé : `actif=OUI`, `statut_parc=GERE`, ouvre un nouveau
    rattachement de gestion daté (jamais de réouverture d'une ligne close)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)

    prop = _txt(proprietaire_id)
    if not prop:
        return _refus(E_PROP_MANQUANT)

    p = Path(ref_path or cfg.REF_SETUP)
    if not p.exists():
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    wb = openpyxl.load_workbook(p, keep_vba=p.suffix.lower() == ".xlsm")
    try:
        if prop not in _proprietaires_actifs(wb):
            return _refus(E_PROP_INCONNU, prop)

        hdr, row = _logement_ligne(wb, logement_id)
        if row is None:
            return _refus(E_LOGEMENT_INCONNU, logement_id)
        if _txt(row[hdr.index("actif")].value).upper() == "OUI":
            return _refus(E_DEJA_ACTIF, logement_id)

        row[hdr.index("actif")].value = "OUI"
        if "statut_parc" in hdr:
            row[hdr.index("statut_parc")].value = "GERE"

        wsg = wb[SH_GEST]
        hdrg = [c.value for c in wsg[1]]
        wsg.append([{
            "gestion_id": f"GST_{logement_id}_{prop}_{date_debut}",
            "logement_id": logement_id,
            "proprietaire_id": prop,
            "date_debut": date_debut,
            "date_fin": None,
            "statut_gestion": "ACTIF",
            "source": "SAISIE_APPLICATION",
            "commentaire": "Réactivation",
        }.get(h) for h in hdrg])

        err = _ecrire(p, wb, lambda d: _refus(E_ECRITURE, d))
        if err:
            return err
    finally:
        wb.close()

    return {"ok": True, "logement_id": logement_id, "proprietaire_id": prop, "date_debut": date_debut}


def changer_proprietaire(logement_id: str, proprietaire_id: str, date_debut: str,
                         ref_path: Path | None = None) -> dict[str, Any]:
    """Change le propriétaire d'un logement à `date_debut` : clôture le rattachement en cours
    (date_fin = veille) et ouvre une nouvelle ligne. Jamais de modification d'une ligne close."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)

    prop = _txt(proprietaire_id)
    if not prop:
        return _refus(E_PROP_MANQUANT)

    p = Path(ref_path or cfg.REF_SETUP)
    if not p.exists():
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    wb = openpyxl.load_workbook(p, keep_vba=p.suffix.lower() == ".xlsm")
    try:
        if prop not in _proprietaires_actifs(wb):
            return _refus(E_PROP_INCONNU, prop)

        hdr, row = _logement_ligne(wb, logement_id)
        if row is None:
            return _refus(E_LOGEMENT_INCONNU, logement_id)

        hdrg, ligne_gest = _gestion_active(wb, logement_id)
        if ligne_gest is not None:
            ligne_gest[hdrg.index("date_fin")].value = _veille(date_debut)
            ligne_gest[hdrg.index("statut_gestion")].value = "RETIRE"

        wsg = wb[SH_GEST]
        wsg.append([{
            "gestion_id": f"GST_{logement_id}_{prop}_{date_debut}",
            "logement_id": logement_id,
            "proprietaire_id": prop,
            "date_debut": date_debut,
            "date_fin": None,
            "statut_gestion": "ACTIF",
            "source": "SAISIE_APPLICATION",
            "commentaire": "Changement de propriétaire",
        }.get(h) for h in hdrg])

        err = _ecrire(p, wb, lambda d: _refus(E_ECRITURE, d))
        if err:
            return err
    finally:
        wb.close()

    return {"ok": True, "logement_id": logement_id, "proprietaire_id": prop, "date_debut": date_debut}


def changer_taux_commission(logement_id: str, taux: float, date_debut: str,
                            proprietaire_id: str = "", ref_path: Path | None = None) -> dict[str, Any]:
    """Change le taux de commission d'un logement, au grain **logement** (`logement_id`
    obligatoire) — cohérent avec `resolve_commission_rate()` qui priorise déjà ce grain sur le
    grain propriétaire. Clôture la ligne courante active à `date_debut` (veille) et ouvre une
    nouvelle ligne : aucune ligne historique déjà close n'est jamais modifiée."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)
    try:
        taux_f = float(taux)
    except (TypeError, ValueError):
        return _refus(E_TAUX_INVALIDE, str(taux))
    if not (0 <= taux_f <= 1):
        return _refus(E_TAUX_INVALIDE, str(taux))

    p = Path(ref_path or cfg.REF_SETUP)
    if not p.exists():
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    wb = openpyxl.load_workbook(p, keep_vba=p.suffix.lower() == ".xlsm")
    try:
        hdr, row = _logement_ligne(wb, logement_id)
        if row is None:
            return _refus(E_LOGEMENT_INCONNU, logement_id)

        if SH_TAUX not in wb.sheetnames:
            return _refus(E_LOGEMENT_INCONNU, "REF_Taux_Commission absente")
        ws = wb[SH_TAUX]
        hdrt = [c.value for c in ws[1]]
        li, fi = hdrt.index("logement_id"), hdrt.index("date_fin")
        for line in ws.iter_rows(min_row=2):
            if (str(line[li].value or "").strip() == logement_id
                    and not str(line[fi].value or "").strip()):
                line[fi].value = _veille(date_debut)

        prop = _txt(proprietaire_id)
        if not prop:
            _, gest_rows = _lire(wb, SH_GEST)
            actives = [g for g in gest_rows if _txt(g.get("logement_id")) == logement_id
                      and not _txt(g.get("date_fin"))]
            prop = _txt(actives[-1].get("proprietaire_id")) if actives else ""

        ws.append([{
            "taux_commission_id": f"TX_{logement_id}_{date_debut}",
            "proprietaire_id": prop or None,
            "logement_id": logement_id,
            "taux_commission": taux_f,
            "date_debut": date_debut,
            "date_fin": None,
            "actif": "OUI",
            "justification": "SAISIE_APPLICATION",
            "commentaire": "",
        }.get(h) for h in hdrt])

        err = _ecrire(p, wb, lambda d: _refus(E_ECRITURE, d))
        if err:
            return err
    finally:
        wb.close()

    return {"ok": True, "logement_id": logement_id, "taux_commission": taux_f,
            "date_debut": date_debut}

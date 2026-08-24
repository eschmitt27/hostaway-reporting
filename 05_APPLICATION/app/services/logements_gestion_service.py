"""Cycle de vie d'un logement existant : modification, archivage, réactivation, changement de
propriétaire, changement de taux de commission — écriture SQLite, validée et journalisée.

Complète `logements_creation_service.py` (qui ne fait que la création). Périmètre fixé par
`AUDIT_CIBLE_FORFAITS_ET_TAUX_LOGEMENTS.md` :
- `changer_proprietaire()` clôt le rattachement `ref_gestion_logements_hist` actif (date_fin =
  veille de la nouvelle date) et ouvre une nouvelle ligne — jamais de modification d'une ligne
  historique déjà close (règle « ne jamais modifier les mois passés »).
- `changer_taux_commission()` fonctionne au grain **logement** (`logement_id` obligatoire),
  cohérent avec `resolve_commission_rate()` (Lot10, `lib_ref_history.py`) qui priorise déjà une
  ligne à grain logement sur une ligne à grain propriétaire : aucun changement de moteur requis.
- `modifier()` ne touche jamais `logement_id`, `proprietaire_id` ni le taux (routes dédiées) —
  seuls les champs descriptifs (nom, adresse, type, forfait...).
- `archiver()` / `reactiver()` basculent `actif`/`statut_parc` et closent/ouvrent le rattachement
  de gestion en cours, sans jamais supprimer de ligne.

MIGRATION EXCEL → SQLITE
Ce service écrivait dans `REF_Setup.xlsm`. Il écrit désormais dans les tables `ref_*` (0029) via
`referentiel_admin_service`. Les règles ci-dessus sont INCHANGÉES — c'est la destination qui
change. L'invariant « au plus une période ouverte par logement », qu'un classeur ne savait pas
défendre, est désormais porté par un index partiel (migration 0051) EN PLUS de la validation.
"""
from __future__ import annotations

from typing import Any

from app.services import referentiel_admin_service as adm

SH_LOG = adm.TABLE_LOGEMENTS
SH_GEST = adm.TABLE_GESTION
SH_PROP = adm.TABLE_PROPRIETAIRES
SH_TAUX = adm.TABLE_TAUX

E_REFERENTIEL_ABSENT = adm.E_REFERENTIEL_ABSENT
E_LOGEMENT_INCONNU = "V01_LOGEMENT_INCONNU"
E_PROP_MANQUANT = "V02_PROPRIETAIRE_MANQUANT"
E_PROP_INCONNU = "V03_PROPRIETAIRE_INCONNU"
E_DATE_INVALIDE = "V04_DATE_INVALIDE"
E_TAUX_INVALIDE = "V05_TAUX_INVALIDE"
E_DEJA_ACTIF = "V06_DEJA_ACTIF"
E_DEJA_ARCHIVE = "V07_DEJA_ARCHIVE"
E_TYPE_INCONNU = "V08_TYPE_INCONNU"
E_PERIODE_INCOHERENTE = adm.E_PERIODE_INCOHERENTE
E_ECRITURE = adm.E_ECRITURE

MESSAGES = {
    E_REFERENTIEL_ABSENT: adm.MESSAGES[adm.E_REFERENTIEL_ABSENT],
    E_LOGEMENT_INCONNU: "Ce logement n'existe pas dans le référentiel.",
    E_PROP_MANQUANT: "Le nouveau propriétaire est obligatoire.",
    E_PROP_INCONNU: "Ce propriétaire n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "La date est invalide (format AAAA-MM-JJ attendu).",
    E_TAUX_INVALIDE: "Le taux de commission doit être un nombre entre 0 et 1 (ex. 0.15 pour 15 %).",
    E_DEJA_ACTIF: "Ce logement est déjà actif.",
    E_DEJA_ARCHIVE: "Ce logement est déjà archivé.",
    E_TYPE_INCONNU: "Ce type de logement n'existe pas dans le référentiel.",
    E_PERIODE_INCOHERENTE: adm.MESSAGES[adm.E_PERIODE_INCOHERENTE],
    E_ECRITURE: "Écriture refusée.",
}

CHAMPS_MODIFIABLES = ("nom_logement_officiel", "nom_court", "adresse", "ville",
                      "type_logement_id", "hostaway_listing_id", "sur_hostaway",
                      "commentaire", "forfait_logiciel_consommables_mensuel")


def _txt(v) -> str:
    return adm.txt(v)


def _date_valide(d: str) -> bool:
    return adm.date_valide(d)


def _veille(d: str) -> str:
    return adm.veille(d)


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _proprietaires_actifs(*, db_path=None) -> set[str]:
    return {_txt(r.get("proprietaire_id"))
            for r in adm.lignes(SH_PROP, db_path=db_path)
            if _txt(r.get("proprietaire_id")) and _txt(r.get("actif")).upper() == "OUI"}


def _fiche(logement_id: str, *, db_path=None) -> dict[str, str] | None:
    return adm.ligne(SH_LOG, logement_id, db_path=db_path)


def _nombre(v: Any) -> float | None:
    """Convertit une valeur du référentiel en nombre, ou None si elle n'en est pas un.

    Le référentiel SQLite rend des CHAÎNES (choix de `ref_setup_repo` : les appelants convertissent
    au moment de l'usage). Les écrans, eux, calculent avec ces valeurs — `taux * 100`. Sans cette
    conversion, un taux stocké « 0.19 » arriverait au template en texte et le rendu échouerait.
    """
    if v is None or str(v).strip() == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _typer_taux(ligne_taux: dict[str, str] | None) -> dict[str, Any] | None:
    """Rend la ligne de taux avec un `taux_commission` NUMÉRIQUE, comme l'attendent les écrans."""
    if ligne_taux is None:
        return None
    return {**ligne_taux, "taux_commission": _nombre(ligne_taux.get("taux_commission"))}


def modifier(logement_id: str, form: dict[str, Any], *, acteur: str = "",
             db_path=None) -> dict[str, Any]:
    """Met à jour les champs descriptifs d'un logement. Ne modifie jamais `logement_id`,
    `proprietaire_id` ni le taux de commission (routes dédiées `changer_proprietaire`/
    `changer_taux_commission`)."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)

    logement_id = _txt(logement_id)
    if _fiche(logement_id, db_path=db_path) is None:
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    t = _txt(form.get("type_logement_id"))
    if t:
        types_connus = {_txt(x.get("type_logement_id"))
                        for x in adm.lignes(adm.TABLE_TYPES, db_path=db_path)}
        if types_connus and t not in types_connus:
            return _refus(E_TYPE_INCONNU, t)

    champs = {c: form.get(c) for c in CHAMPS_MODIFIABLES if c in form}
    if not champs:
        return {"ok": True, "logement_id": logement_id}

    res = adm.mettre_a_jour(SH_LOG, logement_id, champs, action="MODIFICATION",
                            acteur=acteur, db_path=db_path)
    if not res.get("ok"):
        return res
    return {"ok": True, "logement_id": logement_id}


def archiver(logement_id: str, date_fin: str, *, acteur: str = "", justification: str = "",
             db_path=None) -> dict[str, Any]:
    """Archive un logement : `actif=NON`, `statut_parc=RETIRE`, clôture le rattachement de
    gestion en cours à `date_fin`. Ne supprime aucune ligne."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not _date_valide(date_fin):
        return _refus(E_DATE_INVALIDE, date_fin)

    logement_id = _txt(logement_id)
    fiche = _fiche(logement_id, db_path=db_path)
    if fiche is None:
        return _refus(E_LOGEMENT_INCONNU, logement_id)
    if _txt(fiche.get("actif")).upper() == "NON":
        return _refus(E_DEJA_ARCHIVE, logement_id)

    # Clôture + désactivation en une seule transaction : si la désactivation échouait après une
    # clôture déjà committée, le logement resterait sans période de gestion ouverte alors qu'il
    # est toujours marqué actif — un état incohérent que rien ne permettrait de corriger seul.
    action_cloture = "CORRECTION_RETROACTIVE" if adm.est_retroactif(date_fin) else "CLOTURE_PERIODE"
    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(SH_GEST, logement_id, date_fin, statut="RETIRE",
                                        acteur=acteur, commentaire=justification,
                                        action=action_cloture, conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)
            res = adm.mettre_a_jour(SH_LOG, logement_id, {"actif": "NON", "statut_parc": "RETIRE"},
                                    action="ARCHIVAGE", acteur=acteur, conn=conn, db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001 — panne DB imprévue : rollback déjà fait, refus lisible
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "logement_id": logement_id, "date_fin": _txt(date_fin)}


def reactiver(logement_id: str, date_debut: str, proprietaire_id: str, *, acteur: str = "",
              justification: str = "", db_path=None) -> dict[str, Any]:
    """Réactive un logement archivé : `actif=OUI`, `statut_parc=GERE`, ouvre un nouveau
    rattachement de gestion daté (jamais de réouverture d'une ligne close)."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)

    prop = _txt(proprietaire_id)
    if not prop:
        return _refus(E_PROP_MANQUANT)
    if prop not in _proprietaires_actifs(db_path=db_path):
        return _refus(E_PROP_INCONNU, prop)

    logement_id = _txt(logement_id)
    fiche = _fiche(logement_id, db_path=db_path)
    if fiche is None:
        return _refus(E_LOGEMENT_INCONNU, logement_id)
    if _txt(fiche.get("actif")).upper() == "OUI":
        return _refus(E_DEJA_ACTIF, logement_id)

    action_ouverture = "CORRECTION_RETROACTIVE" if adm.est_retroactif(date_debut) else "REACTIVATION"
    try:
        with adm.transaction(db_path=db_path) as conn:
            res_gest = adm.inserer(SH_GEST, {
                "gestion_id": f"GST_{logement_id}_{prop}_{_txt(date_debut)}",
                "logement_id": logement_id,
                "proprietaire_id": prop,
                "date_debut": _txt(date_debut),
                "date_fin": "",
                "statut_gestion": "ACTIF",
                "source": adm.SOURCE_APPLICATION,
                "commentaire": justification or "Réactivation",
            }, action=action_ouverture, acteur=acteur, commentaire=justification, conn=conn,
               db_path=db_path)
            if not res_gest.get("ok"):
                raise adm.RefusTransaction(res_gest)

            res = adm.mettre_a_jour(SH_LOG, logement_id, {"actif": "OUI", "statut_parc": "GERE"},
                                    action="REACTIVATION", acteur=acteur, conn=conn, db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "logement_id": logement_id, "proprietaire_id": prop,
            "date_debut": _txt(date_debut)}


def changer_proprietaire(logement_id: str, proprietaire_id: str, date_debut: str, *,
                         acteur: str = "", justification: str = "", db_path=None) -> dict[str, Any]:
    """Change le propriétaire d'un logement à `date_debut` : clôture le rattachement en cours
    (date_fin = veille) et ouvre une nouvelle ligne. Jamais de modification d'une ligne close."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)

    prop = _txt(proprietaire_id)
    if not prop:
        return _refus(E_PROP_MANQUANT)
    if prop not in _proprietaires_actifs(db_path=db_path):
        return _refus(E_PROP_INCONNU, prop)

    logement_id = _txt(logement_id)
    if _fiche(logement_id, db_path=db_path) is None:
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    # Clôture puis ouverture dans UNE transaction : l'index partiel (0051) interdit deux périodes
    # ouvertes simultanées, mais seul un commit unique empêche un logement de rester sans
    # rattachement ouvert si l'insertion échouait après une clôture déjà committée.
    retroactif = adm.est_retroactif(date_debut)
    action_ouverture = "CORRECTION_RETROACTIVE" if retroactif else "CHANGEMENT_PROPRIETAIRE"
    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(SH_GEST, logement_id, _veille(date_debut), statut="RETIRE",
                                        acteur=acteur, commentaire=justification,
                                        action=action_ouverture, conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)

            res = adm.inserer(SH_GEST, {
                "gestion_id": f"GST_{logement_id}_{prop}_{_txt(date_debut)}",
                "logement_id": logement_id,
                "proprietaire_id": prop,
                "date_debut": _txt(date_debut),
                "date_fin": "",
                "statut_gestion": "ACTIF",
                "source": adm.SOURCE_APPLICATION,
                "commentaire": justification or "Changement de propriétaire",
            }, action=action_ouverture, acteur=acteur, commentaire=justification, conn=conn,
               db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "logement_id": logement_id, "proprietaire_id": prop,
            "date_debut": _txt(date_debut)}


def changer_taux_commission(logement_id: str, taux: float, date_debut: str,
                            proprietaire_id: str = "", *, acteur: str = "",
                            justification: str = "", db_path=None) -> dict[str, Any]:
    """Change le taux de commission d'un logement, au grain **logement** (`logement_id`
    obligatoire) — cohérent avec `resolve_commission_rate()` qui priorise déjà ce grain sur le
    grain propriétaire. Clôture la ligne courante active à `date_debut` (veille) et ouvre une
    nouvelle ligne : aucune ligne historique déjà close n'est jamais modifiée."""
    if not adm.disponible(db_path=db_path):
        return _refus(E_REFERENTIEL_ABSENT)
    if not _date_valide(date_debut):
        return _refus(E_DATE_INVALIDE, date_debut)
    try:
        taux_f = float(taux)
    except (TypeError, ValueError):
        return _refus(E_TAUX_INVALIDE, str(taux))
    if not (0 <= taux_f <= 1):
        return _refus(E_TAUX_INVALIDE, str(taux))

    logement_id = _txt(logement_id)
    if _fiche(logement_id, db_path=db_path) is None:
        return _refus(E_LOGEMENT_INCONNU, logement_id)

    prop = _txt(proprietaire_id)
    if not prop:
        active = adm.periode_ouverte(SH_GEST, logement_id, db_path=db_path)
        prop = _txt(active.get("proprietaire_id")) if active else ""

    retroactif = adm.est_retroactif(date_debut)
    action_ouverture = "CORRECTION_RETROACTIVE" if retroactif else "CHANGEMENT_TAUX"
    try:
        with adm.transaction(db_path=db_path) as conn:
            cloture = adm.clore_periode(SH_TAUX, logement_id, _veille(date_debut),
                                        acteur=acteur, commentaire=justification,
                                        action=action_ouverture, conn=conn, db_path=db_path)
            if not cloture.get("ok"):
                raise adm.RefusTransaction(cloture)

            res = adm.inserer(SH_TAUX, {
                "taux_commission_id": f"TX_{logement_id}_{_txt(date_debut)}",
                "proprietaire_id": prop,
                "logement_id": logement_id,
                "taux_commission": taux_f,
                "date_debut": _txt(date_debut),
                "date_fin": "",
                "actif": "OUI",
                "justification": adm.SOURCE_APPLICATION,
                "commentaire": justification,
            }, action=action_ouverture, acteur=acteur, commentaire=justification, conn=conn,
               db_path=db_path)
            if not res.get("ok"):
                raise adm.RefusTransaction(res)
    except adm.RefusTransaction as exc:
        return exc.refus
    except Exception as exc:   # noqa: BLE001
        return _refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    adm.invalider_dag_referentiel(db_path=db_path)
    return {"ok": True, "logement_id": logement_id, "taux_commission": taux_f,
            "date_debut": _txt(date_debut)}


# ── Lecture directe du référentiel (état actuel + historique complet) ───────────────────────────
#
# Distincte de `app/services/logements_service.py`. Ici, c'est le module qui ÉCRIT ces tables : il
# peut légitimement les relire pour donner un état immédiat après une action, sans attendre un
# cycle de pipeline. Résolution simple : la ligne sans `date_fin` est la ligne active (invariant
# maintenu par ce module ET par l'index partiel 0051).


def etat_actuel(logement_id: str, *, db_path=None) -> dict[str, Any]:
    """État courant d'un logement : fiche + rattachement de gestion actif + taux actif."""
    if not adm.disponible(db_path=db_path):
        return {"status": "SOURCE_ABSENTE"}

    logement_id = _txt(logement_id)
    fiche = _fiche(logement_id, db_path=db_path)
    if fiche is None:
        return {"status": "INTROUVABLE"}

    return {"status": "OK", "fiche": fiche,
            "gestion_active": adm.periode_ouverte(SH_GEST, logement_id, db_path=db_path),
            "taux_actif": _typer_taux(adm.periode_ouverte(SH_TAUX, logement_id, db_path=db_path))}


def historique(logement_id: str, *, db_path=None) -> dict[str, Any]:
    """Historique complet (toutes les lignes, closes ou non) des rattachements de gestion et des
    taux de commission d'un logement, triées par date de début décroissante."""
    if not adm.disponible(db_path=db_path):
        return {"status": "SOURCE_ABSENTE", "gestion": [], "taux": []}

    logement_id = _txt(logement_id)
    gestion = [g for g in adm.lignes(SH_GEST, db_path=db_path)
               if _txt(g.get("logement_id")) == logement_id]
    gestion.sort(key=lambda g: _txt(g.get("date_debut")), reverse=True)

    taux_rows = [_typer_taux(t) for t in adm.lignes(SH_TAUX, db_path=db_path)
                 if _txt(t.get("logement_id")) == logement_id]
    taux_rows.sort(key=lambda t: _txt(t.get("date_debut")), reverse=True)

    return {"status": "OK", "gestion": gestion, "taux": taux_rows}

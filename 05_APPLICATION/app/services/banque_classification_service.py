"""Classification bancaire déterministe — lit SQLite, écrit SQLite (Lot 8b).

    banque_mouvements + ref_banque_regles → banque_classifications

PORTAGE À L'IDENTIQUE
La règle de décision est celle de `lot8b_banque_regles.py`, reprise sans modification :
premier match dans l'ordre de priorité, quatre types de correspondance, et deux durcissements
(`niveau_risque == ELEVE` force `A_CONTROLER` ; un libellé de remboursement lève `niveau_anomalie`).
Aucune règle métier n'est inventée ici — seule l'interface de données change.

LE BRUT N'EST JAMAIS TOUCHÉ
La classification s'écrit dans sa propre table, indexée par exécution. Rejouer les règles n'altère
donc pas ce que la banque a envoyé, et deux exécutions restent comparables sur la même donnée.

FAIL-CLOSED
Sans règles en base, le service REFUSE. Se rabattre sur un classeur ou sur un jeu de règles
embarqué produirait une classification plausible mais fausse — silencieusement, ce qui est le pire
comportement possible pour un moteur bancaire.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

from app.db.connection import get_db

# Types de correspondance, dans l'ordre où lot8b les évalue.
MATCH_CATCH_ALL = "CATCH_ALL"
MATCH_COMMENCE_PAR = "COMMENCE_PAR"
MATCH_CONTIENT = "CONTIENT"
MATCH_REGEX = "REGEX"

ST_VALIDE = "VALIDE"
ST_A_CONTROLER = "A_CONTROLER"

CLASS_CLASSE = "CLASSE"
CLASS_RAPPROCHEMENT_REQUIS = "RAPPROCHEMENT_REQUIS"
CLASS_A_ENVOYER_IA = "A_ENVOYER_IA"

SOURCE_REGLE = "REGLE_DETERMINISTE"

E_REGLES_ABSENTES = "BANQUE_REGLES_ABSENTES"
E_AUCUN_MOUVEMENT = "BANQUE_AUCUN_MOUVEMENT"

MESSAGES = {
    E_REGLES_ABSENTES: ("Aucune règle de classification en base : importez le référentiel "
                        "avant de classer les mouvements."),
    E_AUCUN_MOUVEMENT: "Aucun mouvement bancaire à classer.",
}


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _priorite(v: Any) -> int:
    """Priorité comparable. Les colonnes du référentiel sont du TEXTE : trier « 10 » et « 9 »
    comme des chaînes les inverserait, donc changerait la règle appliquée à un mouvement."""
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 10 ** 9


def charger_regles(*, db_path=None) -> list[dict[str, Any]]:
    """Règles actives, triées comme lot8b : priorité croissante puis identifiant."""
    conn = get_db(db_path)
    try:
        noms = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "ref_banque_regles" not in noms:
            return []
        rows = conn.execute(
            "SELECT regle_id, priorite, actif, compte_id, type_match, champ_cible, motif, "
            "tiers_detecte, categorie, type_flux_id, code_impact, source_economique, "
            "rapprochement_requis, validation_automatique, niveau_risque, "
            "statut_controle_defaut, statut_classification_defaut "
            "FROM ref_banque_regles").fetchall()
    finally:
        conn.close()

    colonnes = ("regle_id", "priorite", "actif", "compte_id", "type_match", "champ_cible",
                "motif", "tiers_detecte", "categorie", "type_flux_id", "code_impact",
                "source_economique", "rapprochement_requis", "validation_automatique",
                "niveau_risque", "statut_controle_defaut", "statut_classification_defaut")
    regles = []
    for r in rows:
        d = {k: (_txt(v) or None) for k, v in zip(colonnes, r)}
        # Une règle explicitement inactive ne s'applique pas. Absence de valeur = active,
        # comme le classeur le pratiquait.
        if (d.get("actif") or "OUI").upper() == "NON":
            continue
        d["priorite"] = _priorite(r[1])
        regles.append(d)
    regles.sort(key=lambda d: (d["priorite"], _txt(d.get("regle_id"))))
    return regles


def appliquer_regle(libelle_norm: str, regles: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Premier match, dans l'ordre de priorité. Portage exact de `lot8b.apply_rule`."""
    for r in regles:
        tm = _txt(r.get("type_match")).upper()
        motif = _txt(r.get("motif"))
        if tm == MATCH_CATCH_ALL:
            return r
        if tm == MATCH_COMMENCE_PAR and libelle_norm.startswith(motif):
            return r
        if tm == MATCH_CONTIENT and motif and motif in libelle_norm:
            return r
        if tm == MATCH_REGEX and motif:
            try:
                if re.search(motif, libelle_norm):
                    return r
            except re.error:
                # Une expression invalide dans le référentiel ne doit pas arrêter la
                # classification des 540 autres mouvements : on passe à la règle suivante.
                continue
    return None


def _durcir(rule: dict[str, Any], libelle_norm: str) -> dict[str, str]:
    """Les deux durcissements de lot8b, repris tels quels."""
    statut_ctrl = _txt(rule.get("statut_controle_defaut")) or ST_A_CONTROLER
    niveau_risque = _txt(rule.get("niveau_risque")).upper()

    # Un risque élevé ne peut jamais rester VALIDE, quoi que dise la règle.
    if niveau_risque == "ELEVE":
        statut_ctrl = ST_A_CONTROLER

    # Un remboursement est toujours signalé : il inverse un flux déjà comptabilisé.
    remboursement = "REMBOURSEMENT" in libelle_norm or "REMBT" in libelle_norm
    niveau_anomalie = ST_A_CONTROLER if (niveau_risque == "ELEVE" or remboursement) else ""

    return {"statut_controle": statut_ctrl, "niveau_anomalie": niveau_anomalie}


def classer(*, bank_account_id: str = "", db_path=None) -> dict[str, Any]:
    """Classe les mouvements et écrit le résultat. Une transaction, une exécution identifiée."""
    from app.services import banque_mouvements_service as bq

    regles = charger_regles(db_path=db_path)
    if not regles:
        return {"ok": False, "code": E_REGLES_ABSENTES, "message": MESSAGES[E_REGLES_ABSENTES]}

    mouvements = bq.mouvements(bank_account_id=bank_account_id, db_path=db_path)
    if not mouvements:
        return {"ok": False, "code": E_AUCUN_MOUVEMENT, "message": MESSAGES[E_AUCUN_MOUVEMENT]}

    run_id = "CLS-" + uuid.uuid4().hex[:12].upper()
    lignes: list[tuple] = []
    sans_regle: list[str] = []
    stats = {"total": 0, ST_VALIDE: 0, ST_A_CONTROLER: 0,
             CLASS_CLASSE: 0, CLASS_RAPPROCHEMENT_REQUIS: 0, CLASS_A_ENVOYER_IA: 0,
             "ELEVE": 0, "MOYEN": 0, "FAIBLE": 0}

    for m in mouvements:
        libelle_norm = _txt(m.get("libelle_brut")).upper()
        rule = appliquer_regle(libelle_norm, regles)
        if rule is None:
            # Sans catch-all dans le référentiel, un mouvement peut rester non classé. On le
            # signale plutôt que de lui inventer une catégorie.
            sans_regle.append(m["mouvement_id_opaque"])
            continue

        durci = _durcir(rule, libelle_norm)
        statut_class = _txt(rule.get("statut_classification_defaut")) or CLASS_CLASSE
        niveau_risque = _txt(rule.get("niveau_risque")).upper()

        stats["total"] += 1
        stats[durci["statut_controle"]] = stats.get(durci["statut_controle"], 0) + 1
        stats[statut_class] = stats.get(statut_class, 0) + 1
        if niveau_risque in ("ELEVE", "MOYEN", "FAIBLE"):
            stats[niveau_risque] += 1

        lignes.append((
            m["mouvement_id_opaque"], run_id, _txt(rule.get("regle_id")),
            _txt(rule.get("categorie")), _txt(rule.get("tiers_detecte")),
            _txt(rule.get("type_flux_id")), _txt(rule.get("code_impact")),
            _txt(rule.get("source_economique")) or SOURCE_REGLE,
            durci["statut_controle"], statut_class, niveau_risque,
            _txt(rule.get("rapprochement_requis")),
        ))

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id, "
                "regle_id, categorie, tiers_detecte, type_flux_id, code_impact, "
                "source_economique, statut_controle, statut_classification, niveau_risque, "
                "rapprochement_requis) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", lignes)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "classification_run_id": run_id, "nb_regles": len(regles),
            "nb_mouvements": len(mouvements), "nb_classes": len(lignes),
            "nb_sans_regle": len(sans_regle), "sans_regle": sans_regle[:20],
            "repartition": stats}


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

_COLS = ("mouvement_id_opaque", "classification_run_id", "regle_id", "categorie",
         "tiers_detecte", "type_flux_id", "code_impact", "source_economique",
         "statut_controle", "statut_classification", "niveau_risque", "rapprochement_requis",
         "date_classification")


def derniere_execution(*, db_path=None) -> str:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT classification_run_id FROM banque_classifications "
            "ORDER BY date_classification DESC, id DESC LIMIT 1").fetchone()
        return r[0] if r else ""
    finally:
        conn.close()


def classifications(*, classification_run_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Classifications d'une exécution. Par défaut, la plus récente.

    Rendre toutes les exécutions confondues mélangerait deux lectures du même mouvement : la
    dernière fait foi, les précédentes restent auditables en la nommant.
    """
    run = classification_run_id or derniere_execution(db_path=db_path)
    if not run:
        return []
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            f"SELECT {', '.join(_COLS)} FROM banque_classifications "
            "WHERE classification_run_id = ?", (run,)).fetchall()
        return [dict(zip(_COLS, r)) for r in rows]
    finally:
        conn.close()


def repartition(*, classification_run_id: str = "", db_path=None) -> dict[str, int]:
    """Compteurs par statut de classification — pour comparer deux exécutions."""
    run = classification_run_id or derniere_execution(db_path=db_path)
    if not run:
        return {}
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT statut_classification, COUNT(*) FROM banque_classifications "
            "WHERE classification_run_id = ? GROUP BY statut_classification", (run,)).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()

"""Trésorerie propriétaires — MOUVEMENT_TRESORERIE_PROPRIETAIRE (migration 0025).

Objet métier distinct de Lot5 (acompte de réservation hors Hostaway, inchangé). Représente un
mouvement financier société <-> propriétaire (acompte, remboursement, régularisation, compensation,
avance, restitution) — jamais une réservation, une charge, une facture, une écriture bancaire ou
comptable. Un mouvement VALIDE devient un candidat au rapprochement bancaire générique existant
(`banques_rapprochement_service`, type_objet REVERSEMENT_PROPRIETAIRE) via
`banques_candidats_service`, jamais un second moteur.

Un mouvement bancaire ne détermine jamais automatiquement la nature — la nature est déclarée
manuellement à la création, ou héritée d'un objet métier déjà validé (`source_type=OBJET_VALIDE`).
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from app.db.connection import get_db
# Import du MODULE : lier le nom fige la fonction au chargement et rend toute
# redirection du référentiel sans effet.
from app.readers import proprietaires_reader

SENS = ("PROPRIETAIRE_VERS_SOCIETE", "SOCIETE_VERS_PROPRIETAIRE")
NATURES = (
    "ACOMPTE_PROPRIETAIRE", "REMBOURSEMENT_PROPRIETAIRE", "REGULARISATION_PROPRIETAIRE",
    "COMPENSATION_PROPRIETAIRE", "AVANCE_PROPRIETAIRE", "RESTITUTION_PROPRIETAIRE",
    "AUTRE_A_CONTROLER",
)

ST_BROUILLON = "BROUILLON"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDE = "VALIDE"
ST_ANNULE = "ANNULE"
STATUTS = (ST_BROUILLON, ST_A_CONTROLER, ST_VALIDE, ST_ANNULE)

E_PROPRIETAIRE_INCONNU = "V01_PROPRIETAIRE_INCONNU"
E_SENS_INCONNU = "V02_SENS_INCONNU"
E_NATURE_INCONNUE = "V03_NATURE_INCONNUE"
E_MONTANT_INVALIDE = "V04_MONTANT_INVALIDE"
E_DATE_MANQUANTE = "V05_DATE_MANQUANTE"
E_INTROUVABLE = "E01_MOUVEMENT_INTROUVABLE"
E_STATUT = "E02_TRANSITION_INTERDITE"
E_SUPPRESSION_INTERDITE = "E03_SUPPRESSION_MOUVEMENT_VALIDE_INTERDITE"

MESSAGES = {
    E_PROPRIETAIRE_INCONNU: "Propriétaire inconnu ou inactif.",
    E_SENS_INCONNU: "Sens inconnu.",
    E_NATURE_INCONNUE: "Nature inconnue.",
    E_MONTANT_INVALIDE: "Le montant doit être un nombre strictement positif.",
    E_DATE_MANQUANTE: "La date du mouvement est obligatoire.",
    E_INTROUVABLE: "Mouvement de trésorerie propriétaire introuvable.",
    E_STATUT: "Transition de statut interdite.",
    E_SUPPRESSION_INTERDITE: "Un mouvement validé ne peut jamais être supprimé, seulement annulé.",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def _evenement(conn, mouvement_opaque: str, type_evt: str, ancien: str | None, nouveau: str | None,
              commentaire: str, acteur: str) -> None:
    evt_opaque = "MTE-" + uuid.uuid4().hex[:12].upper()
    conn.execute(
        "INSERT INTO mouvements_tresorerie_proprietaires_evenements "
        "(mouvement_id, evenement_opaque, type_evenement, ancien_statut, nouveau_statut, "
        "commentaire, acteur) VALUES (?,?,?,?,?,?,?)",
        (mouvement_opaque, evt_opaque, type_evt, ancien, nouveau, commentaire, acteur or "local"))


def previsualiser(proprietaire_id: str, sens: str, nature: str, montant: Any,
                  date_mouvement: str, *, logement_id: str = "", mode_reglement: str = "",
                  reference_metier: str = "", justification: str = "") -> dict[str, Any]:
    """Valide les champs sans écrire — retourne les erreurs ou un aperçu."""
    pid = _txt(proprietaire_id)
    if not proprietaires_reader.find_proprietaire(pid):
        return _refus(E_PROPRIETAIRE_INCONNU, pid)
    sens = _txt(sens)
    if sens not in SENS:
        return _refus(E_SENS_INCONNU, sens)
    nature = _txt(nature)
    if nature not in NATURES:
        return _refus(E_NATURE_INCONNUE, nature)
    montant_f = _nombre(montant)
    if montant_f is None or montant_f <= 0:
        return _refus(E_MONTANT_INVALIDE)
    if not _txt(date_mouvement):
        return _refus(E_DATE_MANQUANTE)
    return {
        "ok": True,
        "apercu": {
            "proprietaire_id": pid, "sens": sens, "nature": nature, "montant": round(montant_f, 2),
            "date_mouvement": date_mouvement, "logement_id": _txt(logement_id) or None,
            "mode_reglement": _txt(mode_reglement) or None,
            "reference_metier": _txt(reference_metier) or None,
            "justification": _txt(justification) or None,
        },
    }


def creer(proprietaire_id: str, sens: str, nature: str, montant: Any, date_mouvement: str, *,
         logement_id: str = "", mode_reglement: str = "", reference_metier: str = "",
         justification: str = "", source_type: str = "MANUEL", source_id: str = "",
         acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée un mouvement en BROUILLON. Ne devient jamais un candidat au rapprochement tant qu'il
    n'est pas VALIDE."""
    verif = previsualiser(proprietaire_id, sens, nature, montant, date_mouvement,
                          logement_id=logement_id, mode_reglement=mode_reglement,
                          reference_metier=reference_metier, justification=justification)
    if not verif["ok"]:
        return verif

    opaque = "MTP-" + uuid.uuid4().hex[:12].upper()
    montant_f = _nombre(montant)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires "
            "(mouvement_opaque, proprietaire_id, logement_id, date_mouvement, montant, sens, "
            "nature, statut, mode_reglement, reference_metier, justification, "
            "justificatif_present, source_type, source_id, cree_par) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, _txt(proprietaire_id), _txt(logement_id) or None, date_mouvement,
             round(montant_f, 2), _txt(sens), _txt(nature), ST_BROUILLON,
             _txt(mode_reglement) or None, _txt(reference_metier) or None,
             _txt(justification) or None, 1 if _txt(justification) else 0, _txt(source_type),
             _txt(source_id) or None, acteur or "local"))
        _evenement(conn, opaque, "CREATION", None, ST_BROUILLON, justification, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mouvement_opaque": opaque, "statut": ST_BROUILLON}


def charger(mouvement_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM mouvements_tresorerie_proprietaires WHERE mouvement_opaque=?",
            (mouvement_opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def lister(*, proprietaire_id: str = "", statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM mouvements_tresorerie_proprietaires ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = [dict(r) for r in rows]
    if proprietaire_id:
        out = [m for m in out if m["proprietaire_id"] == proprietaire_id]
    if statut:
        out = [m for m in out if m["statut"] == statut]
    return out


def modifier_brouillon(mouvement_opaque: str, *, acteur: str = "", db_path=None,
                       **champs: Any) -> dict[str, Any]:
    """Modifie un mouvement encore en BROUILLON. Refuse toute modification hors BROUILLON."""
    m = charger(mouvement_opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, mouvement_opaque)
    if m["statut"] != ST_BROUILLON:
        return _refus(E_STATUT, f"modification impossible depuis {m['statut']}")

    autorises = {"logement_id", "date_mouvement", "montant", "sens", "nature", "mode_reglement",
                "reference_metier", "justification"}
    maj = {k: v for k, v in champs.items() if k in autorises}
    if "sens" in maj and _txt(maj["sens"]) not in SENS:
        return _refus(E_SENS_INCONNU, maj["sens"])
    if "nature" in maj and _txt(maj["nature"]) not in NATURES:
        return _refus(E_NATURE_INCONNUE, maj["nature"])
    if "montant" in maj:
        mf = _nombre(maj["montant"])
        if mf is None or mf <= 0:
            return _refus(E_MONTANT_INVALIDE)
        maj["montant"] = round(mf, 2)
    if not maj:
        return {"ok": True, "mouvement_opaque": mouvement_opaque, "statut": m["statut"]}

    set_clause = ", ".join(f"{k}=?" for k in maj)
    conn = get_db(db_path)
    try:
        conn.execute(
            f"UPDATE mouvements_tresorerie_proprietaires SET {set_clause}, version=version+1 "
            "WHERE mouvement_opaque=?", (*maj.values(), mouvement_opaque))
        _evenement(conn, mouvement_opaque, "MODIFICATION", ST_BROUILLON, ST_BROUILLON,
                  f"champs modifiés: {sorted(maj)}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mouvement_opaque": mouvement_opaque, "statut": ST_BROUILLON}


def valider(mouvement_opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """BROUILLON ou A_CONTROLER -> VALIDE. Un mouvement validé ne peut plus être supprimé."""
    m = charger(mouvement_opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, mouvement_opaque)
    if m["statut"] not in (ST_BROUILLON, ST_A_CONTROLER):
        return _refus(E_STATUT, f"{m['statut']} -> {ST_VALIDE}")
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE mouvements_tresorerie_proprietaires SET statut=?, valide_le=?, valide_par=?, "
            "version=version+1 WHERE mouvement_opaque=?",
            (ST_VALIDE, _now(), acteur or "local", mouvement_opaque))
        _evenement(conn, mouvement_opaque, "VALIDATION", m["statut"], ST_VALIDE, "", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mouvement_opaque": mouvement_opaque, "statut": ST_VALIDE}


def annuler(mouvement_opaque: str, *, commentaire: str = "", acteur: str = "",
           db_path=None) -> dict[str, Any]:
    """Annule un mouvement (BROUILLON, A_CONTROLER ou VALIDE) — jamais une suppression physique."""
    m = charger(mouvement_opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, mouvement_opaque)
    if m["statut"] == ST_ANNULE:
        return _refus(E_STATUT, "déjà annulé")
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE mouvements_tresorerie_proprietaires SET statut=?, annule_le=?, "
            "version=version+1 WHERE mouvement_opaque=?", (ST_ANNULE, _now(), mouvement_opaque))
        _evenement(conn, mouvement_opaque, "ANNULATION", m["statut"], ST_ANNULE, commentaire, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mouvement_opaque": mouvement_opaque, "statut": ST_ANNULE}


def supprimer(mouvement_opaque: str, db_path=None) -> dict[str, Any]:
    """Aucune suppression physique n'est jamais autorisée pour un mouvement validé — la fonction
    existe uniquement pour documenter et faire échouer explicitement cette tentative."""
    m = charger(mouvement_opaque, db_path)
    if m is None:
        return _refus(E_INTROUVABLE, mouvement_opaque)
    if m["statut"] == ST_VALIDE:
        return _refus(E_SUPPRESSION_INTERDITE, mouvement_opaque)
    return _refus(E_SUPPRESSION_INTERDITE, "suppression physique jamais autorisée par ce service")


def historique(mouvement_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM mouvements_tresorerie_proprietaires_evenements WHERE mouvement_id=? "
            "ORDER BY id DESC", (mouvement_opaque,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def montant_rapproche(mouvement_opaque: str, db_path=None) -> float:
    """Somme des montants rapprochés actifs (PROPOSE + CONFIRME) côté moteur bancaire générique —
    délègue à banques_rapprochement_service, ne duplique jamais le calcul."""
    from app.services import banques_rapprochement_service as rappro
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT rapprochement_id_opaque, montant_rapproche FROM banque_rapprochements "
            "WHERE type_objet='REVERSEMENT_PROPRIETAIRE' AND objet_id=? AND statut IN (?,?)",
            (mouvement_opaque, rappro.ST_PROPOSE, rappro.ST_CONFIRME)).fetchall()
    finally:
        conn.close()
    return round(sum(r["montant_rapproche"] for r in rows), 2)


def reste_a_rapprocher(mouvement_opaque: str, db_path=None) -> float:
    m = charger(mouvement_opaque, db_path)
    if m is None:
        return 0.0
    return round(abs(m["montant"]) - montant_rapproche(mouvement_opaque, db_path), 2)


def solde(proprietaire_id: str, db_path=None) -> dict[str, Any]:
    """Solde net des mouvements VALIDE pour un propriétaire — SOCIETE_VERS_PROPRIETAIRE positif
    pour le propriétaire, PROPRIETAIRE_VERS_SOCIETE négatif (dette du propriétaire envers la
    société diminue). Purement informatif, ne crée jamais d'écriture."""
    mouvements = [m for m in lister(proprietaire_id=proprietaire_id, statut=ST_VALIDE, db_path=db_path)]
    total_recu = sum(m["montant"] for m in mouvements if m["sens"] == "PROPRIETAIRE_VERS_SOCIETE")
    total_verse = sum(m["montant"] for m in mouvements if m["sens"] == "SOCIETE_VERS_PROPRIETAIRE")
    return {"proprietaire_id": proprietaire_id, "total_recu_de_proprietaire": round(total_recu, 2),
            "total_verse_a_proprietaire": round(total_verse, 2),
            "solde_net": round(total_verse - total_recu, 2), "nb_mouvements": len(mouvements)}


def objets_rapprochables(db_path=None) -> list[dict[str, Any]]:
    """Mouvements VALIDE avec un reste à rapprocher > 0 — matière pour
    `banques_candidats_service`, jamais un BROUILLON ni un ANNULE."""
    out = []
    for m in lister(statut=ST_VALIDE, db_path=db_path):
        reste = reste_a_rapprocher(m["mouvement_opaque"], db_path)
        if reste > 0.005:
            out.append({**m, "reste_a_rapprocher": reste})
    return out

"""Premier socle Comptabilité — écritures en partie double (migration `0021`).

Ne recalcule AUCUN résultat de gestion : Lot10 reste seul maître du résultat économique. Ce service
ne fait que TRADUIRE en débit/crédit des objets déjà validés (facture, rapprochement bancaire),
jamais l'inverse — aucune écriture ne crée ou ne modifie une facture, une charge, un règlement ou
un rapprochement.

Deux journaux câblés ce tour (cadrage `43_CADRAGE_COMPTABILITE_APPLICATION.md`) : ACHATS (facture
fournisseur validée) et BANQUE (rapprochement confirmé). VENTES/CAISSE/ODIVERSES sont déclarés,
non générés.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

JOURNAUX = ("ACHATS", "VENTES", "BANQUE", "CAISSE", "ODIVERSES")

ST_PROPOSEE = "PROPOSEE"
ST_VALIDEE = "VALIDEE"
ST_CONTREPASSEE = "CONTREPASSEE"
STATUTS = (ST_PROPOSEE, ST_VALIDEE, ST_CONTREPASSEE)

COMPTE_FOURNISSEURS = "401000"
COMPTE_BANQUE = "512000"
COMPTE_ACHAT_GENERIQUE = "606000"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_INTROUVABLE = "E_ECRITURE_INTROUVABLE"
E_DEJA_GENEREE = "E_DEJA_GENEREE"
E_DESEQUILIBRE = "E_ECRITURE_DESEQUILIBREE"
E_COMPTE_INCONNU = "E_COMPTE_INCONNU"
E_ORIGINE_INVALIDE = "E_ORIGINE_INVALIDE"
E_STATUT = "E_TRANSITION_INTERDITE"

MESSAGES = {
    E_FLAGS: "Écriture comptable désactivée sur cette installation.",
    E_INTROUVABLE: "Écriture introuvable.",
    E_DEJA_GENEREE: "Une écriture existe déjà pour cette origine sur ce journal.",
    E_DESEQUILIBRE: "Écriture déséquilibrée : le total débit doit égaler le total crédit.",
    E_COMPTE_INCONNU: "Compte inconnu ou inactif dans le plan comptable.",
    E_ORIGINE_INVALIDE: "Origine de l'écriture invalide pour ce journal.",
    E_STATUT: "Transition de statut interdite.",
}


def _flags_actifs() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _compte_valide(compte: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM plan_comptable WHERE compte=? AND actif=1", (compte,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _evenement(conn, opaque: str, type_evt: str, *, ancien: str | None = None,
              nouveau: str | None = None, commentaire: str = "", acteur: str = "") -> None:
    conn.execute(
        "INSERT INTO ecriture_evenements (ecriture_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, commentaire, acteur or "local"))


def _deja_generee(journal: str, origine_type: str, origine_id: str, db_path=None) -> str | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT ecriture_id_opaque FROM ecritures WHERE journal=? AND origine_type=? "
            "AND origine_id_opaque=? AND statut <> ?",
            (journal, origine_type, origine_id, ST_CONTREPASSEE)).fetchone()
    finally:
        conn.close()
    return row["ecriture_id_opaque"] if row else None


def _inserer_ecriture(journal: str, date_ecriture: str, periode: str, piece: str, libelle: str,
                      origine_type: str, origine_id: str, lignes: list[dict[str, Any]], *,
                      acteur: str = "", db_path=None) -> dict[str, Any]:
    """Insère une écriture équilibrée. Ne vérifie PAS les flags — appelé par les générateurs
    spécifiques, qui l'ont déjà fait."""
    total_debit = round(sum(l.get("debit", 0) or 0 for l in lignes), 2)
    total_credit = round(sum(l.get("credit", 0) or 0 for l in lignes), 2)
    if total_debit != total_credit or total_debit == 0:
        return _refus(E_DESEQUILIBRE, f"débit={total_debit} crédit={total_credit}")

    for l in lignes:
        if _compte_valide(l["compte"], db_path) is None:
            return _refus(E_COMPTE_INCONNU, l["compte"])

    existant = _deja_generee(journal, origine_type, origine_id, db_path)
    if existant:
        # Idempotence : regénérer pour la même origine est un no-op explicite, jamais un doublon.
        return {"ok": True, "ecriture_id_opaque": existant, "deja_generee": True}

    opaque = "ECR-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, piece, "
            "libelle, origine_type, origine_id_opaque, statut, total_debit, total_credit, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, journal, date_ecriture, periode, piece, libelle, origine_type, origine_id,
             ST_PROPOSEE, total_debit, total_credit, acteur or "local"))
        for i, l in enumerate(lignes, start=1):
            conn.execute(
                "INSERT INTO ecriture_lignes (ecriture_id_opaque, ligne_num, compte, auxiliaire, "
                "debit, credit, logement_id, proprietaire_id, reservation_id, libelle) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (opaque, i, l["compte"], l.get("auxiliaire"), l.get("debit", 0) or 0,
                 l.get("credit", 0) or 0, l.get("logement_id"), l.get("proprietaire_id"),
                 l.get("reservation_id"), l.get("libelle")))
        _evenement(conn, opaque, "GENERATION", nouveau=ST_PROPOSEE, commentaire=libelle, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ecriture_id_opaque": opaque, "deja_generee": False}


def generer_ecriture_achat(facture_id_opaque: str, *, acteur: str = "",
                           db_path=None) -> dict[str, Any]:
    """Génère l'écriture ACHATS d'une facture fournisseur VALIDÉE (ou plus avancée dans son cycle).

    606 (Achats, débit) / 401 (Fournisseurs, crédit) — compte générique par défaut (cf. cadrage 43,
    le mapping catégorie->compte fin n'est pas arbitré). Idempotent : une facture ne génère jamais
    deux écritures ACHATS.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    from app.services import factures_service as fact
    f = fact.charger(facture_id_opaque, db_path)
    if f is None:
        return _refus(E_ORIGINE_INVALIDE, facture_id_opaque)
    if f["statut"] not in (fact.ST_VALIDEE, fact.ST_PARTIELLEMENT_REGLEE, fact.ST_REGLEE):
        return _refus(E_ORIGINE_INVALIDE, f"facture au statut {f['statut']}")

    montant = round(f["montant_ttc"], 2)
    lignes = [
        {"compte": COMPTE_ACHAT_GENERIQUE, "debit": montant, "credit": 0,
         "libelle": f"Achat {f['facture_ref']}"},
        {"compte": COMPTE_FOURNISSEURS, "debit": 0, "credit": montant,
         "auxiliaire": f["fournisseur_id_opaque"], "libelle": f["facture_ref"]},
    ]
    return _inserer_ecriture(
        "ACHATS", f.get("date_facture") or _now()[:10], (f.get("date_facture") or _now())[:7],
        f["facture_ref"], f"Facture fournisseur {f['facture_ref']}", "FACTURE", facture_id_opaque,
        lignes, acteur=acteur, db_path=db_path)


def generer_ecriture_banque(rapprochement_id_opaque: str, *, acteur: str = "",
                            db_path=None) -> dict[str, Any]:
    """Génère l'écriture BANQUE d'un rapprochement CONFIRMÉ (règlement fournisseur ↔ mouvement).

    401 (Fournisseurs, débit — extinction de la dette) / 512 (Banque, crédit). Le montant est celui
    RAPPROCHÉ (peut être partiel), jamais le montant de la facture entière.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        rap = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE rapprochement_id_opaque=?",
            (rapprochement_id_opaque,)).fetchone()
    finally:
        conn.close()
    if rap is None or rap["type_objet"] != "REGLEMENT_CHARGE" or rap["statut"] != "CONFIRME":
        return _refus(E_ORIGINE_INVALIDE, rapprochement_id_opaque)

    from app.services import reglements_fournisseurs_service as regl
    reglement = regl.charger(rap["objet_id"], db_path) if hasattr(regl, "charger") else None
    fournisseur = (reglement or {}).get("fournisseur_id_opaque")
    montant = round(rap["montant_rapproche"], 2)

    lignes = [
        {"compte": COMPTE_FOURNISSEURS, "debit": montant, "credit": 0, "auxiliaire": fournisseur,
         "libelle": "Règlement rapproché"},
        {"compte": COMPTE_BANQUE, "debit": 0, "credit": montant, "libelle": "Sortie banque"},
    ]
    return _inserer_ecriture(
        "BANQUE", rap["date_creation"][:10],
        rap["date_creation"][:7], rapprochement_id_opaque,
        "Rapprochement bancaire", "RAPPROCHEMENT", rapprochement_id_opaque, lignes,
        acteur=acteur, db_path=db_path)


def generer_ecriture_avoir(facture_avoir_id_opaque: str, ecriture_origine_id_opaque: str, *,
                           acteur: str = "", db_path=None) -> dict[str, Any]:
    """Contrepasse l'écriture ACHATS d'origine pour un avoir : lignes inversées, jamais une
    suppression ni une réécriture de l'écriture d'origine."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    origine = charger(ecriture_origine_id_opaque, db_path)
    if origine is None:
        return _refus(E_INTROUVABLE, ecriture_origine_id_opaque)

    lignes_inversees = [
        {"compte": l["compte"], "debit": l["credit"], "credit": l["debit"],
         "auxiliaire": l["auxiliaire"], "libelle": f"Avoir — contrepasse {l['libelle'] or ''}".strip()}
        for l in lignes(ecriture_origine_id_opaque, db_path)
    ]
    return _inserer_ecriture(
        origine["journal"], _now()[:10], _now()[:7], facture_avoir_id_opaque,
        f"Avoir sur {origine['piece']}", "AVOIR", facture_avoir_id_opaque, lignes_inversees,
        acteur=acteur, db_path=db_path)


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM ecritures WHERE ecriture_id_opaque=?", (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def lignes(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM ecriture_lignes WHERE ecriture_id_opaque=? ORDER BY ligne_num",
            (opaque,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def lister(*, journal: str = "", periode: str = "", statut: str = "",
          db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM ecritures ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        e = dict(r)
        if journal and e["journal"] != journal:
            continue
        if periode and e["periode"] != periode:
            continue
        if statut and e["statut"] != statut:
            continue
        out.append(e)
    return out


def valider(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    e = charger(opaque, db_path)
    if e is None:
        return _refus(E_INTROUVABLE, opaque)
    if e["statut"] != ST_PROPOSEE:
        return _refus(E_STATUT, f"{e['statut']} -> {ST_VALIDEE}")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE ecritures SET statut=?, version=version+1 WHERE ecriture_id_opaque=?",
                    (ST_VALIDEE, opaque))
        _evenement(conn, opaque, "VALIDATION", ancien=ST_PROPOSEE, nouveau=ST_VALIDEE, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ecriture_id_opaque": opaque, "statut": ST_VALIDEE}


def contrepasser(opaque: str, *, commentaire: str = "", acteur: str = "",
                 db_path=None) -> dict[str, Any]:
    """Annule une écriture par une écriture MIROIR (débit/crédit inversés), jamais une suppression."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    e = charger(opaque, db_path)
    if e is None:
        return _refus(E_INTROUVABLE, opaque)
    if e["statut"] == ST_CONTREPASSEE:
        return _refus(E_STATUT, "déjà contrepassée")

    lignes_inversees = [
        {"compte": l["compte"], "debit": l["credit"], "credit": l["debit"],
         "auxiliaire": l["auxiliaire"], "logement_id": l["logement_id"],
         "proprietaire_id": l["proprietaire_id"], "reservation_id": l["reservation_id"],
         "libelle": f"Contrepassation de {opaque}"}
        for l in lignes(opaque, db_path)
    ]
    miroir_opaque = "ECR-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, piece, "
            "libelle, origine_type, origine_id_opaque, statut, total_debit, total_credit, "
            "contrepasse_de, acteur) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (miroir_opaque, e["journal"], _now()[:10], _now()[:7], e["piece"],
             f"Contrepassation — {e['libelle']}", "MANUEL", None, ST_VALIDEE,
             e["total_debit"], e["total_credit"], opaque, acteur or "local"))
        for i, l in enumerate(lignes_inversees, start=1):
            conn.execute(
                "INSERT INTO ecriture_lignes (ecriture_id_opaque, ligne_num, compte, auxiliaire, "
                "debit, credit, logement_id, proprietaire_id, reservation_id, libelle) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (miroir_opaque, i, l["compte"], l.get("auxiliaire"), l["debit"], l["credit"],
                 l.get("logement_id"), l.get("proprietaire_id"), l.get("reservation_id"),
                 l["libelle"]))
        conn.execute("UPDATE ecritures SET statut=?, version=version+1 WHERE ecriture_id_opaque=?",
                    (ST_CONTREPASSEE, opaque))
        _evenement(conn, opaque, "CONTREPASSATION", ancien=e["statut"], nouveau=ST_CONTREPASSEE,
                  commentaire=commentaire, acteur=acteur)
        _evenement(conn, miroir_opaque, "GENERATION", nouveau=ST_VALIDEE,
                  commentaire=f"Contrepasse {opaque}", acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ecriture_id_opaque": opaque, "miroir_id_opaque": miroir_opaque,
           "statut": ST_CONTREPASSEE}


def solde_compte(compte: str, *, db_path=None) -> dict[str, Any]:
    """Solde d'un compte, sur les écritures réellement POSTÉES : VALIDEE et CONTREPASSEE.

    Une écriture CONTREPASSEE reste une écriture historiquement validée — sa contrepartie est son
    miroir, lui-même VALIDEE. L'exclure du solde casserait l'équilibre : le miroir compenserait une
    ligne qui n'existe plus dans le calcul, et le solde net ne reviendrait jamais à zéro. Seule
    PROPOSEE (jamais validée) est exclue."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT l.debit, l.credit FROM ecriture_lignes l "
            "JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE l.compte=? AND e.statut IN (?,?)",
            (compte, ST_VALIDEE, ST_CONTREPASSEE)).fetchall()
    finally:
        conn.close()
    debit = round(sum(r["debit"] for r in rows), 2)
    credit = round(sum(r["credit"] for r in rows), 2)
    return {"compte": compte, "debit": debit, "credit": credit, "solde": round(debit - credit, 2)}


def solde_auxiliaire(auxiliaire: str, *, db_path=None) -> dict[str, Any]:
    """Même règle que `solde_compte` : VALIDEE et CONTREPASSEE comptent, PROPOSEE non."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT l.debit, l.credit FROM ecriture_lignes l "
            "JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE l.auxiliaire=? AND e.statut IN (?,?)",
            (auxiliaire, ST_VALIDEE, ST_CONTREPASSEE)).fetchall()
    finally:
        conn.close()
    debit = round(sum(r["debit"] for r in rows), 2)
    credit = round(sum(r["credit"] for r in rows), 2)
    return {"auxiliaire": auxiliaire, "debit": debit, "credit": credit,
           "solde": round(debit - credit, 2)}

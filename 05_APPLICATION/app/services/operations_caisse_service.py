"""Opérations de caisse (migration `0023`) — objet métier distinct de l'écriture CAISSE.

Sert les cas de caisse SANS objet existant déjà réel : encaissement, remboursement associé en
espèces. Un paiement fournisseur en espèces passe par `reglements_fournisseurs_service` (moyen
CAISSE), jamais dupliqué ici.

LE CYCLE DE VIE (§72-73), ET CE QU'IL CORRIGE

Une opération naissait `ENREGISTREE`, et la génération de son écriture comptable était un BOUTON
SÉPARÉ. Deux conséquences, aucune visible depuis l'écran :

  · une opération pouvait exister sans écriture — de l'argent entré ou sorti de la caisse dont le
    compte 530000 ne portait pas trace ;
  · `annuler()` retournait le statut sans rien contrepasser — l'écriture, elle, restait. La caisse
    comptable et la caisse réelle divergeaient en silence.

Désormais : `BROUILLON` → `VALIDE` → (éventuellement) `CONTREPASSEE`.

  BROUILLON     tant qu'on saisit. Modifiable, supprimable, sans aucune trace comptable.
  VALIDE        la validation EST la génération de l'écriture, dans le même geste. Une opération
                validée sans écriture équilibrée n'existe pas : si l'écriture est refusée
                (période close, compte inconnu, déséquilibre), la validation l'est aussi.
  CONTREPASSEE  une opération validée ne se supprime pas et ne se modifie pas. Elle s'annule par
                une écriture MIROIR, qui laisse les deux mouvements lisibles. C'est la seule
                façon honnête de corriger une caisse : ce qui est passé est passé.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TYPES = ("ENCAISSEMENT", "REMBOURSEMENT_ASSOCIE", "AUTRE")

ST_BROUILLON = "BROUILLON"
ST_VALIDE = "VALIDE"
ST_CONTREPASSEE = "CONTREPASSEE"
ST_ANNULEE = "ANNULEE"          # brouillon abandonné : rien n'a été comptabilisé
#: Ancien statut unique, conservé pour ne pas casser la lecture d'une base antérieure.
ST_ENREGISTREE = "ENREGISTREE"

STATUTS = (ST_BROUILLON, ST_VALIDE, ST_CONTREPASSEE, ST_ANNULEE)

E_FLAGS = "E_FLAGS_DESACTIVES"
E_TYPE_INCONNU = "V01_TYPE_OPERATION_INCONNU"
E_MONTANT_INVALIDE = "V02_MONTANT_INVALIDE"
E_INTROUVABLE = "E01_OPERATION_INTROUVABLE"
E_PAS_UN_BROUILLON = "E02_OPERATION_NON_MODIFIABLE"
E_PAS_VALIDE = "E03_OPERATION_NON_VALIDE"
E_DEJA_CONTREPASSEE = "E04_DEJA_CONTREPASSEE"
E_MOTIF_OBLIGATOIRE = "E05_MOTIF_OBLIGATOIRE"
E_ECRITURE_REFUSEE = "E06_ECRITURE_REFUSEE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation.",
    E_TYPE_INCONNU: "Type d'opération de caisse inconnu.",
    E_MONTANT_INVALIDE: "Le montant doit être un nombre strictement positif.",
    E_INTROUVABLE: "Opération de caisse introuvable.",
    E_PAS_UN_BROUILLON: "Seul un brouillon est modifiable. Une opération validée se corrige par "
                        "contrepassation.",
    E_PAS_VALIDE: "Seule une opération validée peut être contrepassée.",
    E_DEJA_CONTREPASSEE: "Cette opération a déjà été contrepassée.",
    E_MOTIF_OBLIGATOIRE: "Contrepasser une opération de caisse exige d'en donner la raison.",
    E_ECRITURE_REFUSEE: "L'écriture comptable a été refusée : l'opération reste au brouillon.",
}


def _flags_actifs() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


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


def creer(type_operation: str, montant: Any, *, date_operation: str = "", tiers_type: str = "",
         tiers_id: str = "", piece: str = "", commentaire: str = "", acteur: str = "",
         db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    type_operation = _txt(type_operation)
    if type_operation not in TYPES:
        return _refus(E_TYPE_INCONNU, type_operation)
    m = _nombre(montant)
    if m is None or m <= 0:
        return _refus(E_MONTANT_INVALIDE, _txt(montant))

    opaque = "CAI-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO operations_caisse (operation_id_opaque, type_operation, date_operation, "
            "montant, tiers_type, tiers_id, piece, commentaire, statut, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (opaque, type_operation, date_operation or date.today().isoformat(), round(m, 2),
             _txt(tiers_type) or None, _txt(tiers_id) or None, _txt(piece) or None,
             _txt(commentaire) or None, ST_BROUILLON, acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_BROUILLON}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM operations_caisse WHERE operation_id_opaque=?",
                           (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def lister(*, statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM operations_caisse ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = [dict(r) for r in rows]
    if statut:
        out = [o for o in out if o["statut"] == statut]
    return out


def valider(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """§72 — la validation EST la comptabilisation. Les deux ne se séparent pas.

    Elles l'étaient : un bouton « générer l'écriture » distinct du statut laissait exister des
    opérations de caisse sans écriture — de l'argent entré ou sorti dont le compte 530000 ne
    portait aucune trace.

    Si l'écriture est refusée (période close, compte inconnu, déséquilibre), la validation l'est
    aussi et l'opération RESTE au brouillon. Une opération validée porte donc toujours son
    écriture, sans exception à vérifier ailleurs.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    op = charger(opaque, db_path=db_path)
    if op is None:
        return _refus(E_INTROUVABLE, opaque)
    if op["statut"] == ST_VALIDE:
        return {"ok": True, "operation_id_opaque": opaque, "statut": ST_VALIDE, "inchange": True}
    if op["statut"] not in (ST_BROUILLON, ST_ENREGISTREE):
        return _refus(E_PAS_UN_BROUILLON, op["statut"])

    from app.services import comptabilite_ecritures_service as compta
    ecriture = compta.generer_ecriture_caisse_operation(opaque, acteur=acteur, db_path=db_path)
    if not ecriture.get("ok"):
        return _refus(E_ECRITURE_REFUSEE,
                      f"{ecriture.get('code')} : {ecriture.get('message') or ecriture.get('detail')}")

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE operations_caisse SET statut=?, version=version+1 "
                     "WHERE operation_id_opaque=?", (ST_VALIDE, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_VALIDE, "ecriture": ecriture}


def contrepasser(opaque: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """§73 — annuler une opération VALIDÉE, par une écriture miroir. Jamais par une suppression.

    Retourner le statut sans rien contrepasser — ce que faisait `annuler()` — laissait l'écriture
    en place : la caisse comptable et la caisse réelle divergeaient, en silence et pour toujours.

    L'écriture miroir reprend les mêmes comptes avec débit et crédit échangés. Les deux mouvements
    restent lisibles : c'est la seule façon honnête de corriger une caisse, parce que ce qui est
    passé est passé.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    motif = _txt(motif)
    if not motif:
        return _refus(E_MOTIF_OBLIGATOIRE)
    op = charger(opaque, db_path=db_path)
    if op is None:
        return _refus(E_INTROUVABLE, opaque)
    if op["statut"] == ST_CONTREPASSEE:
        return _refus(E_DEJA_CONTREPASSEE, opaque)
    if op["statut"] not in (ST_VALIDE, ST_ENREGISTREE):
        return _refus(E_PAS_VALIDE, op["statut"])

    # La contrepassation d'écriture EXISTE DÉJÀ, générique : écriture miroir, origine marquée
    # CONTREPASSEE, lien `contrepasse_de`, les deux événements journalisés. En écrire une seconde,
    # spécifique à la caisse, aurait créé deux mécanismes pour le même geste — et l'un des deux
    # aurait fini par diverger de l'autre.
    from app.services import comptabilite_ecritures_service as compta
    origine = _ecriture_de(opaque, db_path=db_path)
    if origine is None:
        return _refus(E_ECRITURE_REFUSEE,
                      "aucune écriture de caisse n'est rattachée à cette opération")
    miroir = compta.contrepasser(
        origine, commentaire=f"Opération de caisse {opaque} — {motif}", acteur=acteur,
        db_path=db_path)
    if not miroir.get("ok"):
        return _refus(E_ECRITURE_REFUSEE,
                      f"{miroir.get('code')} : {miroir.get('message') or miroir.get('detail')}")

    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE operations_caisse SET statut=?, version=version+1, "
            "commentaire=COALESCE(commentaire,'') || ? WHERE operation_id_opaque=?",
            (ST_CONTREPASSEE, f" · Contrepassée ({acteur or 'local'}) : {motif}", opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_CONTREPASSEE,
            "ecriture_contrepassation": miroir, "motif": motif}


def annuler(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Abandonne un BROUILLON — rien n'ayant été comptabilisé, il n'y a rien à contrepasser.

    REFUSE une opération validée, et dit par quoi la remplacer. C'est le cœur de la correction :
    ce geste retournait auparavant n'importe quel statut sans toucher à l'écriture.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    op = charger(opaque, db_path=db_path)
    if op is None:
        return _refus(E_INTROUVABLE, opaque)
    if op["statut"] in (ST_VALIDE, ST_ENREGISTREE):
        return _refus(E_PAS_UN_BROUILLON,
                      "Cette opération est comptabilisée : elle se corrige par contrepassation, "
                      "qui laisse les deux mouvements lisibles.")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE operations_caisse SET statut=?, version=version+1 "
                     "WHERE operation_id_opaque=?", (ST_ANNULEE, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_ANNULEE}


def modifier(opaque: str, *, montant: Any = None, date_operation: str = "", piece: str = "",
             commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Corrige un BROUILLON. Une opération validée est IMMUABLE (§73)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    op = charger(opaque, db_path=db_path)
    if op is None:
        return _refus(E_INTROUVABLE, opaque)
    if op["statut"] != ST_BROUILLON:
        return _refus(E_PAS_UN_BROUILLON, op["statut"])

    champs, valeurs = [], []
    if montant is not None and _txt(montant) != "":
        m = _nombre(montant)
        if m is None or m <= 0:
            return _refus(E_MONTANT_INVALIDE, _txt(montant))
        champs.append("montant=?"); valeurs.append(round(m, 2))
    for colonne, valeur in (("date_operation", date_operation), ("piece", piece),
                            ("commentaire", commentaire)):
        if _txt(valeur):
            champs.append(f"{colonne}=?"); valeurs.append(_txt(valeur))
    if not champs:
        return {"ok": True, "operation_id_opaque": opaque, "inchange": True}

    conn = get_db(db_path)
    try:
        conn.execute(f"UPDATE operations_caisse SET {', '.join(champs)}, version=version+1 "
                     "WHERE operation_id_opaque=?", (*valeurs, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_BROUILLON}


def _ecriture_de(opaque: str, db_path=None) -> str | None:
    """Écriture de caisse rattachée à cette opération, si elle existe et n'est pas déjà annulée."""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT ecriture_id_opaque FROM ecritures WHERE journal='CAISSE' "
            "AND origine_type='OPERATION_CAISSE' AND origine_id_opaque=? "
            "AND statut <> 'CONTREPASSEE' ORDER BY id LIMIT 1", (opaque,)).fetchone()
    finally:
        conn.close()
    return row["ecriture_id_opaque"] if row else None


def solde(db_path=None) -> dict[str, Any]:
    """Solde de caisse tel que la COMPTABILITÉ le porte, et non recalculé ici.

    Recalculer le solde depuis `operations_caisse` donnerait un SECOND chiffre, qui finirait par
    diverger du premier — et personne ne saurait lequel croire. `solde_compte` fait foi : il ne
    compte que les écritures réellement postées, donc ni un brouillon d'opération, ni une écriture
    encore proposée.
    """
    from app.services import comptabilite_ecritures_service as compta

    conn = get_db(db_path)
    try:
        brouillons = conn.execute(
            "SELECT COUNT(*) c FROM operations_caisse WHERE statut = ?", (ST_BROUILLON,)
        ).fetchone()["c"]
        # `solde_compte` ne compte que les écritures POSTÉES (VALIDEE, CONTREPASSEE). Une écriture
        # fraîchement générée est PROPOSEE : elle attend le contrôle du comptable. Sans ce second
        # chiffre, l'écran affichait « solde 0,00 € » juste après qu'on ait validé un encaissement
        # de 250 € — exact, et incompréhensible.
        attente = conn.execute(
            "SELECT ROUND(COALESCE(SUM(l.debit) - SUM(l.credit), 0), 2) AS m "
            "FROM ecriture_lignes l JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE l.compte = ? AND e.statut = ?",
            (compta.COMPTE_CAISSE, compta.ST_PROPOSEE)).fetchone()["m"]
    finally:
        conn.close()
    etat = compta.solde_compte(compta.COMPTE_CAISSE, db_path=db_path)
    return {**etat, "compte": compta.COMPTE_CAISSE, "nb_brouillons": brouillons,
            "en_attente_de_validation_comptable": attente}

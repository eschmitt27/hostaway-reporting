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


#: Sens d'un type d'opération sur l'encaisse. Identique à celui que l'écriture applique au compte
#: 530000 (`generer_ecriture_caisse_operation`) : un encaissement le débite, tout le reste le
#: crédite. Les deux lectures décrivent le même fait physique et ne peuvent donc pas diverger.
SENS_SUR_ENCAISSE = {"ENCAISSEMENT": +1, "REMBOURSEMENT_ASSOCIE": -1, "AUTRE": -1}

#: Opérations qui pèsent réellement sur l'encaisse. Un brouillon n'est pas encore un mouvement ;
#: un brouillon abandonné n'en a jamais été un ; une opération contrepassée a été annulée par un
#: mouvement inverse, et les deux se neutralisent.
STATUTS_DANS_L_ENCAISSE = (ST_VALIDE, ST_ENREGISTREE)


def solde(db_path=None) -> dict[str, Any]:
    """Solde OPÉRATIONNEL de la caisse : ce qu'il y a réellement dans le tiroir.

    LE SOLDE OPÉRATIONNEL NE SE DÉDUIT PAS DU STATUT COMPTABLE. Il se déduisait de `solde_compte`,
    qui ne compte que les écritures POSTÉES — l'écran affichait donc « 0,00 € » juste après qu'un
    encaissement de 250 € ait été validé, au motif que le comptable n'avait pas encore contrôlé
    l'écriture. C'était faux : l'argent était dans le tiroir. Expliquer ce zéro en note ne le
    rendait pas vrai.

    Les deux chiffres mesurent DEUX CHOSES DIFFÉRENTES, et c'est pourquoi ils coexistent :

      · `solde`                — fait économique : les mouvements de caisse validés. Il ne bouge
                                 PAS quand le comptable valide l'écriture ; l'argent avait déjà
                                 bougé. Compter le mouvement une seconde fois à ce moment-là
                                 doublerait l'encaisse.
      · `en_attente_...`       — état de contrôle : la part de ce solde dont l'écriture miroir
                                 n'est pas encore validée en comptabilité générale. Elle tombe à
                                 zéro quand le comptable a fait son travail, sans que le solde
                                 bouge d'un centime.
      · `solde_comptable`      — ce que porte le compte 530000 sur les écritures postées. Il doit
                                 rejoindre `solde` une fois tout contrôlé ; `ecart_comptable` le
                                 dit, et c'est ce qui rend les deux pistes sûres au lieu de
                                 concurrentes.
    """
    from app.services import comptabilite_ecritures_service as compta

    conn = get_db(db_path)
    try:
        lignes = conn.execute(
            "SELECT o.type_operation, o.montant, "
            "       (SELECT e.statut FROM ecritures e "
            "          WHERE e.journal = 'CAISSE' AND e.origine_type = 'OPERATION_CAISSE' "
            "            AND e.origine_id_opaque = o.operation_id_opaque "
            "          ORDER BY e.id LIMIT 1) AS statut_ecriture "
            "  FROM operations_caisse o "
            f" WHERE o.statut IN ({','.join('?' * len(STATUTS_DANS_L_ENCAISSE))})",
            STATUTS_DANS_L_ENCAISSE).fetchall()
        brouillons = conn.execute(
            "SELECT COUNT(*) c FROM operations_caisse WHERE statut = ?", (ST_BROUILLON,)
        ).fetchone()["c"]
    finally:
        conn.close()

    encaisse = 0.0
    attente = 0.0
    for l in lignes:
        signe = SENS_SUR_ENCAISSE.get(l["type_operation"], -1)
        montant = round(signe * float(l["montant"] or 0), 2)
        encaisse += montant
        # Une écriture absente compte AUSSI comme en attente : le mouvement est réel, et rien ne
        # le porte encore en comptabilité. C'est le cas qu'il faut le plus voir.
        if l["statut_ecriture"] != compta.ST_VALIDEE:
            attente += montant

    comptable = compta.solde_compte(compta.COMPTE_CAISSE, db_path=db_path)
    encaisse = round(encaisse, 2)
    return {
        "compte": compta.COMPTE_CAISSE,
        "solde": encaisse,
        "en_attente_de_validation_comptable": round(attente, 2),
        "solde_comptable": comptable["solde"],
        # Une fois toutes les écritures validées, les deux pistes doivent tomber d'accord. Tant
        # qu'un écart subsiste, il s'explique par ce qui attend le contrôle — et s'il subsiste
        # APRÈS, c'est une anomalie qu'il vaut mieux voir que masquer.
        "ecart_comptable": round(encaisse - comptable["solde"], 2),
        "nb_brouillons": brouillons,
    }

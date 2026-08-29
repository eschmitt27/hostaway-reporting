"""Factures propriétaires ÉMISES par la conciergerie.

Distinction fondamentale, à ne jamais confondre :

- le **relevé** propriétaire (Lot 12, 12/13 lignes) explique au propriétaire son revenu et son
  solde : il contient du payout, du revenu net, des acomptes, un reste à payer ;
- la **facture** ne contient QUE ce que la société facture réellement. Son total réconcilie
  exactement `montant_du_conciergerie` (Lot 10), qui vaut par construction
  commission + ménage + préparation canapé + charge fixe + charges exceptionnelles refacturées.

Ce module ne calcule rien : il transforme des éléments déjà calculés en objet facture. Il ne crée
jamais de commission, de charge, de réservation ni d'écriture comptable.

Une facture ÉMISE est immutable : son contenu est figé dans un snapshot et le PDF est identifié par
son hash. Une correction passe par un AVOIR lié, jamais par une modification en place.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any

from app.db.connection import get_db

# ── Contrat des lignes ──────────────────────────────────────────────────────────────────────────
# Seuls ces types entrent dans une facture. Ils correspondent un pour un aux composants de
# `montant_du_conciergerie` (lot10_calculer_resultats.build_net_proprietaire).
TYPES_FACTURABLES = (
    "COMMISSION_CONCIERGERIE",
    "MENAGE_FACTURE",
    "PREPARATION_CANAPE",
    "CHARGE_FIXE",
    "CHARGES_EXCEPT_REFAC",
)

# Types Lot 12 explicitement NON facturables — ils appartiennent au relevé ou au règlement.
# Listés pour que le refus soit une décision lisible, pas un oubli.
TYPES_NON_FACTURABLES = (
    "TOTAL_PAYOUT",              # information d'exploitation
    "REVENU_NET_EXPLOITATION",   # information d'exploitation
    "ACOMPTE_AIRBNB",            # paiement déjà reçu
    "PAIEMENT_DEJA_RECU",        # paiement déjà reçu
    "ACOMPTES_PROPRIETAIRES",    # paiement déjà reçu
    "RESTE_A_PAYER",             # solde, dérivé
    "STATUT_REGLEMENT",          # statut
)

LIBELLES = {
    "COMMISSION_CONCIERGERIE": "Commission de conciergerie",
    "MENAGE_FACTURE": "Prestations de ménage",
    "PREPARATION_CANAPE": "Préparation du canapé",
    "CHARGE_FIXE": "Charge fixe mensuelle",
    "CHARGES_EXCEPT_REFAC": "Charges et achats exceptionnels refacturés",
}

ST_BROUILLON, ST_VALIDE, ST_EMIS, ST_ANNULE = "BROUILLON", "VALIDE", "EMIS", "ANNULE"
STATUTS = (ST_BROUILLON, ST_VALIDE, ST_EMIS, ST_ANNULE)

TYPE_FACTURE, TYPE_AVOIR = "FACTURE", "AVOIR"

# Codes de contrôle stables (catalogue facturation).
C_SOURCE_INCOMPLETE = "FACTURE_PROPRIETAIRE_SOURCE_INCOMPLETE"
C_DOUBLON = "FACTURE_PROPRIETAIRE_DOUBLON"
C_TOTAL_INCOHERENT = "FACTURE_PROPRIETAIRE_TOTAL_INCOHERENT"
C_IDENTITE = "FACTURE_PROPRIETAIRE_IDENTITE_INCOMPLETE"
C_PDF_ABSENT = "FACTURE_PROPRIETAIRE_PDF_ABSENT"
C_SNAPSHOT = "FACTURE_PROPRIETAIRE_SNAPSHOT_INCOHERENT"
C_EMISE_MODIFIEE = "FACTURE_PROPRIETAIRE_EMISE_MODIFIEE"

TOLERANCE = 0.01


class FactureProprietaireError(RuntimeError):
    """Refus métier explicite. Jamais levée pour un cas nominal."""


def _opaque(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


def _round(v: Any) -> float:
    try:
        return round(float(v or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _journal(conn, facture_id, type_evt, ancien=None, nouveau=None, commentaire=None, acteur=None):
    conn.execute(
        "INSERT INTO factures_proprietaires_evenements "
        "(facture_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)",
        (facture_id, type_evt, ancien, nouveau, commentaire, acteur),
    )


# ── Prévisualisation ────────────────────────────────────────────────────────────────────────────

def previsualiser(source: dict[str, Any],
                  decisions_charges: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Construit la facture proposée SANS rien écrire.

    `source` est un enregistrement de calcul propriétaire (Lot 10 / Lot 12) pour un couple
    mois x propriétaire x logement. Les montants ne sont ni recalculés ni corrigés ici.

    `decisions_charges` (mission 15) : décisions humaines d'imputation de positions individuelles
    de `charges_refacturation_service` (chacune {position_id, montant, libelle, justification}).
    Quand fourni, remplace la ligne agrégée `CHARGES_EXCEPT_REFAC` par UNE LIGNE PAR POSITION — le
    propriétaire doit voir exactement ce qu'il paie, pas un total masqué. Aucune consommation ici :
    ce sont des lignes de BROUILLON, la position n'est réellement imputée qu'à `valider()`.
    """
    types = (TYPES_FACTURABLES if decisions_charges is None
            else tuple(t for t in TYPES_FACTURABLES if t != "CHARGES_EXCEPT_REFAC"))
    lignes = []
    for i, type_ligne in enumerate(types, start=1):
        montant = _round(source.get(type_ligne))
        if montant == 0:
            continue  # une ligne à zéro n'est pas facturée — elle n'apparaît pas
        lignes.append({
            "numero_ligne": len(lignes) + 1,
            "type_ligne": type_ligne,
            "libelle": LIBELLES[type_ligne],
            "montant": montant,
            "objet_source_type": "LOT10_NET_PROPRIETAIRE",
            "objet_source_ref": str(source.get("source_calcul") or ""),
        })

    for d in (decisions_charges or []):
        montant = _round(d.get("montant"))
        if montant == 0:
            continue
        lignes.append({
            "numero_ligne": len(lignes) + 1,
            "type_ligne": "CHARGE_REFACTUREE",
            "libelle": d.get("libelle") or "Charge refacturée",
            "montant": montant,
            "objet_source_type": "CHARGES_REFACTURATION_POSITION",
            "objet_source_ref": str(d["position_id"]),
        })

    total = _round(sum(l["montant"] for l in lignes))
    controles = []

    for champ in ("proprietaire_id", "logement_id", "mois"):
        if not str(source.get(champ) or "").strip():
            controles.append({"code": C_SOURCE_INCOMPLETE, "message": f"{champ} absent"})

    # Le total facturé doit réconcilier le montant dû calculé par le moteur. On ne corrige pas
    # l'écart : on le signale. Un écart signifie que la facture ne représente pas le calcul.
    # Mission 15 : `montant_du_conciergerie` (Lot10) ne porte plus que les charges refacturables
    # DÉJÀ réalisées (montant_realise) — le montant attendu doit donc être ajusté du delta apporté
    # par les décisions humaines de CETTE facture (jamais recalculé, juste réconcilié).
    montant_du = source.get("montant_du_conciergerie")
    if montant_du is not None:
        montant_du_attendu = _round(montant_du)
        if decisions_charges is not None:
            montant_du_attendu = _round(
                montant_du_attendu - _round(source.get("CHARGES_EXCEPT_REFAC"))
                + sum(_round(d.get("montant")) for d in decisions_charges))
        if abs(montant_du_attendu - total) > TOLERANCE:
            controles.append({
                "code": C_TOTAL_INCOHERENT,
                "message": (f"total facturé {total:.2f} != montant dû calculé "
                            f"{montant_du_attendu:.2f}"),
            })

    if not lignes:
        controles.append({"code": C_SOURCE_INCOMPLETE, "message": "aucune ligne facturable"})

    return {
        "proprietaire_id": source.get("proprietaire_id"),
        "logement_id": source.get("logement_id"),
        "mois": source.get("mois"),
        "lignes": lignes,
        "montant_total": total,
        "controles": controles,
        "statut_proposition": "PRETE" if not controles else "A_CONTROLER",
        # Éléments de relevé, affichés pour information, JAMAIS facturés.
        "releve_informatif": {t: _round(source.get(t)) for t in TYPES_NON_FACTURABLES
                              if source.get(t) is not None},
    }


# ── Création ────────────────────────────────────────────────────────────────────────────────────

def creer(source: dict[str, Any], *, acteur: str = "", db_path=None,
          type_document: str = TYPE_FACTURE, facture_origine: str | None = None,
          decisions_charges: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Crée une facture BROUILLON. Anti-doublon garanti par l'index unique du schéma.

    `decisions_charges` : voir `previsualiser()`. Purement représentatif à ce stade — aucune
    position de refacturation n'est consommée avant `valider()`.
    """
    apercu = previsualiser(source, decisions_charges=decisions_charges)
    if not apercu["lignes"]:
        raise FactureProprietaireError(f"{C_SOURCE_INCOMPLETE}: aucune ligne facturable")

    conn = get_db(db_path)
    try:
        existante = conn.execute(
            "SELECT facture_id_opaque, statut FROM factures_proprietaires "
            "WHERE mois=? AND proprietaire_id=? AND logement_id=? AND type_document=? "
            "AND statut <> ?",
            (source["mois"], source["proprietaire_id"], source["logement_id"],
             type_document, ST_ANNULE),
        ).fetchone()
        if existante:
            raise FactureProprietaireError(
                f"{C_DOUBLON}: {existante['facture_id_opaque']} existe deja "
                f"({existante['statut']}) pour ce mois/proprietaire/logement"
            )

        fid = _opaque("FPR")
        conn.execute(
            "INSERT INTO factures_proprietaires "
            "(facture_id_opaque, type_document, facture_origine, proprietaire_id, logement_id, "
            " mois, montant_total, statut, source_calcul, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (fid, type_document, facture_origine, source["proprietaire_id"],
             source["logement_id"], source["mois"], apercu["montant_total"], ST_BROUILLON,
             str(source.get("source_calcul") or ""), acteur),
        )
        for l in apercu["lignes"]:
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant, "
                " objet_source_type, objet_source_ref) VALUES (?,?,?,?,?,?,?,?)",
                (_opaque("FPRL"), fid, l["numero_ligne"], l["type_ligne"], l["libelle"],
                 l["montant"], l["objet_source_type"], l["objet_source_ref"]),
            )
        _journal(conn, fid, "CREATION", None, ST_BROUILLON,
                 f"{len(apercu['lignes'])} lignes, total {apercu['montant_total']:.2f}", acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(fid, db_path=db_path)


def lire(facture_id: str, *, db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM factures_proprietaires WHERE facture_id_opaque=?",
                           (facture_id,)).fetchone()
        if row is None:
            raise FactureProprietaireError(f"facture inconnue: {facture_id}")
        lignes = conn.execute(
            "SELECT * FROM factures_proprietaires_lignes WHERE facture_id_opaque=? "
            "ORDER BY numero_ligne", (facture_id,)).fetchall()
        evts = conn.execute(
            "SELECT * FROM factures_proprietaires_evenements WHERE facture_id_opaque=? "
            "ORDER BY id", (facture_id,)).fetchall()
    finally:
        conn.close()
    d = dict(row)
    d["lignes"] = [dict(l) for l in lignes]
    d["evenements"] = [dict(e) for e in evts]
    return d


def lister(*, mois=None, proprietaire_id=None, statut=None, db_path=None) -> list[dict[str, Any]]:
    sql = "SELECT * FROM factures_proprietaires WHERE 1=1"
    params: list[Any] = []
    for champ, val in (("mois", mois), ("proprietaire_id", proprietaire_id), ("statut", statut)):
        if val:
            sql += f" AND {champ}=?"
            params.append(val)
    sql += " ORDER BY mois DESC, proprietaire_id, logement_id"
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


# ── Validation ──────────────────────────────────────────────────────────────────────────────────

def valider(facture_id: str, *, emetteur: dict[str, Any], destinataire: dict[str, Any],
            acteur: str = "", decisions_charges: list[dict[str, Any]] | None = None,
            db_path=None) -> dict[str, Any]:
    """BROUILLON -> VALIDE. Aucune validation silencieuse : chaque refus porte un code.

    Mission 15 — SEUL point de consommation des positions de refacturation : chaque ligne
    `CHARGE_REFACTUREE` de la facture est imputée ICI, dans LA MÊME transaction que le passage
    VALIDE (`charges_refacturation_service.imputer(..., conn=conn)`). Si une imputation échoue
    (position déjà consommée par ailleurs, montant dépassé), tout est annulé : la facture reste
    BROUILLON et aucune position n'est touchée. `decisions_charges` reporte la justification
    éventuelle par position (non persistée sur la ligne elle-même) — fournie par l'appelant au
    moment de la validation, pas dérivée des lignes déjà écrites.
    """
    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_BROUILLON:
        raise FactureProprietaireError(f"statut {f['statut']}: seul un BROUILLON peut etre valide")

    manques = [c for c in ("nom", "adresse", "siret") if not str(emetteur.get(c) or "").strip()]
    if manques:
        raise FactureProprietaireError(f"{C_IDENTITE}: emetteur incomplet ({', '.join(manques)})")
    if not str(destinataire.get("nom") or "").strip():
        raise FactureProprietaireError(f"{C_IDENTITE}: destinataire sans nom")

    if not f["lignes"]:
        raise FactureProprietaireError(f"{C_SOURCE_INCOMPLETE}: facture sans ligne")
    total = _round(sum(_round(l["montant"]) for l in f["lignes"]))
    if abs(total - _round(f["montant_total"])) > TOLERANCE:
        raise FactureProprietaireError(
            f"{C_TOTAL_INCOHERENT}: entete {f['montant_total']} != lignes {total}")

    justifications = {d["position_id"]: d.get("justification")
                      for d in (decisions_charges or [])}
    lignes_charges = [l for l in f["lignes"] if l["type_ligne"] == "CHARGE_REFACTUREE"]

    from app.services import charges_refacturation_service as refac

    conn = get_db(db_path)
    try:
        for l in lignes_charges:
            resultat = refac.imputer(
                l["objet_source_ref"], l["montant"], facture_id=facture_id, acteur=acteur,
                justification=justifications.get(l["objet_source_ref"]), conn=conn)
            if not resultat.get("ok"):
                raise FactureProprietaireError(
                    f"IMPUTATION_REFUSEE {l['objet_source_ref']}: {resultat.get('message')}")
        conn.execute(
            "UPDATE factures_proprietaires SET statut=?, date_validation="
            "strftime('%Y-%m-%dT%H:%M:%SZ','now'), version=version+1 WHERE facture_id_opaque=?",
            (ST_VALIDE, facture_id))
        _journal(conn, facture_id, "VALIDATION", ST_BROUILLON, ST_VALIDE,
                 f"total {total:.2f}", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return lire(facture_id, db_path=db_path)


# ── Numérotation ────────────────────────────────────────────────────────────────────────────────

def _attribuer_numero(conn, serie: str) -> str:
    """Numéro unique, jamais réutilisé. `BEGIN IMMEDIATE` sérialise deux émissions concurrentes.

    Le numéro n'est consommé qu'à l'émission : un brouillon abandonné ne crée aucun trou dans la
    séquence. Le format est porté par `facturation_config_service`, pas décidé ici.
    """
    from app.services import facturation_config_service as fconf

    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT OR IGNORE INTO factures_proprietaires_sequence (serie) VALUES (?)",
                 (serie,))
    conn.execute(
        "UPDATE factures_proprietaires_sequence SET dernier_numero = dernier_numero + 1, "
        "date_modification = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE serie=?", (serie,))
    seq = conn.execute("SELECT dernier_numero FROM factures_proprietaires_sequence WHERE serie=?",
                       (serie,)).fetchone()[0]
    return fconf.formater_numero(serie, seq)


# ── Émission ────────────────────────────────────────────────────────────────────────────────────

def emettre(facture_id: str, *, emetteur: dict[str, Any], destinataire: dict[str, Any],
            serie: str | None = None, date_facture: str, generer_pdf=None, acteur: str = "",
            db_path=None, exiger_conformite: bool = False) -> dict[str, Any]:
    """VALIDE -> EMIS. Fige le snapshot, attribue le numéro, génère le document, enregistre le hash.

    `generer_pdf(snapshot) -> (nom_fichier, sha256)` est injecté : ce service ne connaît ni le
    format du document ni son emplacement de stockage.
    """
    from app.services import facturation_config_service as fconf
    from app.services import factures_proprietaires_conformite_service as conformite

    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_VALIDE:
        raise FactureProprietaireError(f"statut {f['statut']}: seul un VALIDE peut etre emis")

    # Contrôle de pré-émission : il décrit toujours l'état de conformité, mais ne bloque que si on
    # le lui demande. La recette peut ainsi émettre avec une configuration fictive incomplète,
    # tandis que l'émission réelle exige `exiger_conformite=True` et refuse au premier manque.
    controle = conformite.verifier(f, date_facture=date_facture, db_path=db_path)
    if exiger_conformite and controle["statut"] != conformite.PRETE:
        codes = ", ".join(m["code"] for m in controle["manques"])
        raise FactureProprietaireError(f"emission refusee — conformite incomplete: {codes}")
    bloc = controle["conformite"]

    # Série dérivée du type de document et de l'année de facturation : factures et avoirs ont
    # chacun leur compteur, et chaque année ouvre sa propre série.
    if serie is None:
        serie = fconf.serie(f["type_document"], str(date_facture)[:4])

    conn = get_db(db_path)
    try:
        numero = _attribuer_numero(conn, serie)

        snapshot = {
            "facture_id_opaque": facture_id,
            "numero_facture": numero,
            "type_document": f["type_document"],
            "facture_origine": f["facture_origine"],
            "emetteur": dict(emetteur),
            "destinataire": dict(destinataire),
            "proprietaire_id": f["proprietaire_id"],
            "logement_id": f["logement_id"],
            "mois": f["mois"],
            "date_facture": date_facture,
            "devise": f["devise"],
            "lignes": [{k: l[k] for k in ("numero_ligne", "type_ligne", "libelle", "montant",
                                          "objet_source_type", "objet_source_ref")}
                       for l in f["lignes"]],
            "montant_total": _round(f["montant_total"]),
            "source_calcul": f["source_calcul"],
            # Bloc réglementaire figé : identités complètes, type de client, régime de TVA et son
            # fondement, période de prestation, conditions de règlement. C'est lui qui rend le
            # document reproductible à l'identique, et non un recalcul depuis les référentiels.
            "conformite": bloc,
            "regime_tva": bloc["regime_tva"],
            "total_ht": bloc["total_ht"],
            "total_tva": bloc["total_tva"],
            "total_ttc": bloc["total_ttc"],
        }
        payload = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
        snapshot_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        document_nom = document_hash = None
        if generer_pdf is not None:
            document_nom, document_hash = generer_pdf(snapshot)

        conn.execute(
            "UPDATE factures_proprietaires SET statut=?, numero_facture=?, date_facture=?, "
            "snapshot_json=?, snapshot_hash=?, document_nom=?, document_hash=?, "
            "date_emission=strftime('%Y-%m-%dT%H:%M:%SZ','now'), version=version+1 "
            "WHERE facture_id_opaque=?",
            (ST_EMIS, numero, date_facture, payload, snapshot_hash, document_nom,
             document_hash, facture_id))
        _journal(conn, facture_id, "EMISSION", ST_VALIDE, ST_EMIS,
                 f"numero {numero}, snapshot {snapshot_hash[:16]}", acteur)
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Le bloc réglementaire est figé après l'émission, en écriture unique : une facture émise ne
    # voit jamais ses données de conformité réécrites.
    conformite.enregistrer(bloc, db_path=db_path)
    return lire(facture_id, db_path=db_path)


def contenu_emis(facture_id: str, *, db_path=None) -> dict[str, Any]:
    """Contenu d'une facture émise, relu DEPUIS LE SNAPSHOT et non depuis les sources.

    C'est ce qui garantit qu'un recalcul Lot 10, un changement de taux ou une correction de charge
    ne peut pas modifier après coup un document déjà émis.
    """
    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_EMIS or not f["snapshot_json"]:
        raise FactureProprietaireError(f"{C_SNAPSHOT}: facture non emise ou snapshot absent")
    snap = json.loads(f["snapshot_json"])
    recalcul = hashlib.sha256(
        json.dumps(snap, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    if recalcul != f["snapshot_hash"]:
        raise FactureProprietaireError(f"{C_EMISE_MODIFIEE}: snapshot altere")
    return snap


# ── Annulation et avoir ─────────────────────────────────────────────────────────────────────────

def annuler(facture_id: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Annule un BROUILLON ou un VALIDE. Une facture EMIS ne s'annule pas : elle se corrige par
    un avoir (`creer_avoir`), pour que le document déjà transmis reste dans l'historique."""
    f = lire(facture_id, db_path=db_path)
    if f["statut"] == ST_EMIS:
        raise FactureProprietaireError(
            "une facture EMIS ne peut pas etre annulee en place : emettre un avoir")
    if f["statut"] == ST_ANNULE:
        raise FactureProprietaireError("facture deja annulee")
    if not str(motif or "").strip():
        raise FactureProprietaireError("annulation sans motif refusee")

    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE factures_proprietaires SET statut=?, motif_annulation=?, "
            "date_annulation=strftime('%Y-%m-%dT%H:%M:%SZ','now'), version=version+1 "
            "WHERE facture_id_opaque=?", (ST_ANNULE, motif, facture_id))
        _journal(conn, facture_id, "ANNULATION", f["statut"], ST_ANNULE, motif, acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(facture_id, db_path=db_path)


def creer_avoir(facture_id: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Avoir lié à une facture émise : mêmes lignes, montants inversés. L'originale reste intacte."""
    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_EMIS:
        raise FactureProprietaireError("un avoir ne se cree que depuis une facture EMIS")
    if f["type_document"] != TYPE_FACTURE:
        raise FactureProprietaireError("un avoir ne se cree pas depuis un avoir")
    if not str(motif or "").strip():
        raise FactureProprietaireError("avoir sans motif refuse")

    conn = get_db(db_path)
    try:
        existant = conn.execute(
            "SELECT facture_id_opaque FROM factures_proprietaires "
            "WHERE facture_origine=? AND type_document=? AND statut <> ?",
            (facture_id, TYPE_AVOIR, ST_ANNULE)).fetchone()
        if existant:
            raise FactureProprietaireError(
                f"{C_DOUBLON}: avoir {existant['facture_id_opaque']} existe deja")

        aid = _opaque("FPR")
        total = _round(-_round(f["montant_total"]))
        conn.execute(
            "INSERT INTO factures_proprietaires "
            "(facture_id_opaque, type_document, facture_origine, proprietaire_id, logement_id, "
            " mois, montant_total, statut, source_calcul, acteur) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (aid, TYPE_AVOIR, facture_id, f["proprietaire_id"], f["logement_id"], f["mois"],
             total, ST_BROUILLON, f["source_calcul"], acteur))
        for l in f["lignes"]:
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant, "
                " objet_source_type, objet_source_ref) VALUES (?,?,?,?,?,?,?,?)",
                (_opaque("FPRL"), aid, l["numero_ligne"], l["type_ligne"],
                 f"Avoir — {l['libelle']}", -_round(l["montant"]),
                 l["objet_source_type"], l["objet_source_ref"]))
        _journal(conn, aid, "CREATION", None, ST_BROUILLON, f"avoir sur {facture_id}: {motif}",
                 acteur)
        _journal(conn, facture_id, "AVOIR_CREE", ST_EMIS, ST_EMIS, f"avoir {aid}: {motif}", acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(aid, db_path=db_path)


# ── Solde ───────────────────────────────────────────────────────────────────────────────────────

def solde(facture_id: str, *, paiements_imputes: float = 0.0, db_path=None) -> dict[str, Any]:
    """Solde dérivé, jamais stocké. Le règlement ne recrée aucun objet économique : il impute un
    montant sur une créance qui existe déjà."""
    f = lire(facture_id, db_path=db_path)
    total = _round(f["montant_total"])
    impute = _round(paiements_imputes)
    reste = _round(total - impute)
    if impute == 0:
        statut = "NON_REGLEE"
    elif abs(reste) <= TOLERANCE:
        statut = "REGLEE"
    elif (reste > 0) == (total > 0):
        statut = "PARTIELLEMENT_REGLEE"
    else:
        statut = "TROP_PERCU_A_CONTROLER"
    return {"facture_id_opaque": facture_id, "montant_total": total,
            "paiements_imputes": impute, "solde": reste, "statut_reglement": statut}

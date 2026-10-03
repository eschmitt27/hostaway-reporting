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
import re
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

_DATE_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_FR = re.compile(r"^(\d{1,2})[/.](\d{1,2})[/.](\d{4})$")


def _normaliser_date(valeur: Any) -> str:
    """Ramène une date à `AAAA-MM-JJ`. Accepte le format français, refuse le reste.

    Une date de facture est imprimée, sert d'échéance ET servait de source à la série de
    numérotation : la laisser passer telle quelle a produit le numéro « F-11/0-000001 » sur une
    facture réellement émise. On convertit donc ce que l'on sait lire, et on refuse explicitement
    ce que l'on ne sait pas — jamais de troncature silencieuse.
    """
    texte = str(valeur or "").strip()
    if not texte:
        return texte
    if _DATE_ISO.match(texte):
        return texte
    fr = _DATE_FR.match(texte)
    if fr:
        jour, mois, annee = fr.groups()
        return f"{annee}-{int(mois):02d}-{int(jour):02d}"
    raise FactureProprietaireError(
        f"date de facture illisible : {texte!r} — format attendu AAAA-MM-JJ (ou JJ/MM/AAAA)")


# `objet_source_ref` d'une ligne CHARGE_REFACTUREE désigne une POSITION de refacturation, jamais la
# charge elle-même : `valider()` la passe telle quelle à `charges_refacturation_service.imputer()`.
# Constante partagée pour que producteur (composition) et consommateur (validation) ne puissent pas
# diverger — une ligne qui référencerait une charge produirait une facture invalidable.
SOURCE_POSITION_REFAC = "CHARGES_REFACTURATION_POSITION"

# ── Édition d'un BROUILLON (migration 0071) ─────────────────────────────────────────────────────
# Un BROUILLON est un DOCUMENT éditable, pas le miroir figé d'un calcul. Trois montants coexistent
# et ne doivent jamais être confondus :
#   TOTAL_SOURCE_CALCULE  Lot10/Lot12 au moment de la création — gelé, jamais recalculé ;
#   TOTAL_FACTURE         somme VIVE des lignes ;
#   AJUSTEMENT_MANUEL     la différence, dérivée et jamais stockée.
# Éditer une facture ne touche JAMAIS Lot9/Lot10/Lot12 : le calcul reste la source de vérité du
# calcul, la facture est la source de vérité du document.
ORIGINE_CALCULEE = "CALCULEE"
ORIGINE_MANUELLE = "MANUELLE"

# Parcours « charge métier » (partie B) : l'utilisateur choisit CHOIX_LIGNE_CHARGE dans le
# formulaire, et la ligne est STOCKÉE en 'CHARGES_EXCEPT_REFAC'.
#
# Pourquoi ne pas créer une valeur `type_ligne` dédiée : `factures_proprietaires_lignes` porte un
# CHECK(type_ligne IN (...)) posé par 0055 et élargi par 0063. Élargir ce CHECK impose de
# RECONSTRUIRE la table (SQLite n'a pas d'ALTER CONSTRAINT) — une opération destructive rejouée à
# chaque démarrage, pour une distinction que le schéma sait déjà exprimer autrement : la table
# compagne `factures_proprietaires_lignes_charge` identifie sans ambiguïté les lignes issues de ce
# parcours, et l'origine MANUELLE les sépare des lignes CHARGES_EXCEPT_REFAC calculées par Lot10.
# Un discriminant redondant ne valait pas une reconstruction de table.
CHOIX_LIGNE_CHARGE = "CHARGE"
TYPE_LIGNE_CHARGE = "CHARGES_EXCEPT_REFAC"
TYPES_LIGNE_SAISISSABLES = TYPES_FACTURABLES

# Vocabulaire d'événements ÉTENDU (colonne libre, aucune contrainte CHECK — vérifié sur 0027).
EVT_AJOUT_LIGNE = "AJOUT_LIGNE"
EVT_MODIFICATION_LIGNE = "MODIFICATION_LIGNE"
EVT_SUPPRESSION_LIGNE = "SUPPRESSION_LIGNE"
EVT_AJOUT_CHARGE = "AJOUT_CHARGE"
EVT_AJOUT_REVERSEMENT_AIRBNB = "AJOUT_REVERSEMENT_AIRBNB"
EVT_AJOUT_ACOMPTE = "AJOUT_ACOMPTE"

AVERTISSEMENT_LIGNE_CALCULEE = (
    "Cette ligne provient du calcul automatique. La modification changera uniquement la facture, "
    "pas le calcul source."
)

TYPE_FACTURE, TYPE_AVOIR = "FACTURE", "AVOIR"

#: §79 — marque d'une facture saisie hors cycle mensuel.
#
# UNE FACTURE EXCEPTIONNELLE RESTE UNE FACTURE. Fiscalement, rien ne la distingue : même
# numérotation, même PDF, mêmes mentions, même créance, même écriture VENTES. J'avais commencé par
# lui donner un `type_document` à elle — le schéma l'a refusé, et il avait raison : `type_document`
# dit la NATURE du document (facture ou avoir), pas la façon dont on l'a fabriqué.
#
# Ce qui la distingue tient dans `source_calcul` : elle ne vient d'aucun calcul mensuel. C'est
# aussi ce qui la libère de l'index d'unicité `(mois, propriétaire, logement, type_document)` —
# index dont le rôle est d'empêcher DEUX factures mensuelles pour le même grain, pas d'interdire
# une prestation ponctuelle. Et plusieurs prestations ponctuelles dans un même mois sont
# parfaitement légitimes : deux interventions d'urgence, deux factures.
SOURCE_EXCEPTIONNELLE = "SAISIE_EXCEPTIONNELLE"

# Codes de contrôle stables (catalogue facturation).
C_SOURCE_INCOMPLETE = "FACTURE_PROPRIETAIRE_SOURCE_INCOMPLETE"
C_DOUBLON = "FACTURE_PROPRIETAIRE_DOUBLON"
C_TOTAL_INCOHERENT = "FACTURE_PROPRIETAIRE_TOTAL_INCOHERENT"
C_IDENTITE = "FACTURE_PROPRIETAIRE_IDENTITE_INCOMPLETE"
C_PDF_ABSENT = "FACTURE_PROPRIETAIRE_PDF_ABSENT"
C_SNAPSHOT = "FACTURE_PROPRIETAIRE_SNAPSHOT_INCOHERENT"
C_EMISE_MODIFIEE = "FACTURE_PROPRIETAIRE_EMISE_MODIFIEE"
# Une réservation du mois encore À CONTRÔLER n'a AUCUN montant dans le calcul : facturer quand même
# l'omettrait en silence (constaté le 2026-10-03 : deux réservations directes absentes des
# factures de septembre, sans aucune alerte). Création, validation et émission sont refusées tant
# qu'elle n'est pas résolue.
C_RESERVATIONS_A_CONTROLER = "FACTURE_PROPRIETAIRE_RESERVATIONS_A_CONTROLER"

TOLERANCE = 0.01


class FactureProprietaireError(RuntimeError):
    """Refus métier explicite. Jamais levée pour un cas nominal."""


class FacturationAvantV1(FactureProprietaireError):
    """Période antérieure au début de la comptabilité V1. Le message est destiné à l'utilisateur
    tel quel (« La facturation V1 débute en septembre 2026. ») : aucun code technique devant."""
    code = "FACTURATION_AVANT_V1"


def exiger_periode_v1(periode: Any, *, db_path=None) -> None:
    """Refuse toute facture dont la période de prestation précède la comptabilité V1.

    Contrôle SERVEUR, appelé par chaque chemin qui crée, édite, valide ou émet une facture : le
    masquage des sélecteurs à l'écran n'est qu'un confort. Le déclencheur de la migration 0120
    double ce contrôle au niveau de la base. Sans cutover appliqué, ne refuse rien.
    """
    from app.services import perimetre_v1_service as v1
    if v1.est_anterieur(periode, db_path=db_path):
        raise FacturationAvantV1(v1.message_facturation(db_path=db_path))


def _exiger_facture_v1(f: dict[str, Any], *, db_path=None) -> None:
    exiger_periode_v1(f.get("mois"), db_path=db_path)
    if f.get("periode_debut"):
        exiger_periode_v1(f["periode_debut"], db_path=db_path)


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
            "objet_source_type": SOURCE_POSITION_REFAC,
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

def creer_exceptionnelle(*, proprietaire_id: str, logement_id: str, mois: str,
                         lignes: list[dict[str, Any]], acteur: str = "",
                         db_path=None) -> dict[str, Any]:
    """§79 — facture ponctuelle, hors cycle mensuel. Les lignes sont DONNÉES, pas calculées.

    LE MANQUE. Toute facture naissait d'une source Lot12 : un calcul mensuel par propriétaire et
    par logement. Une prestation ponctuelle — une intervention d'urgence, un service rendu une
    fois — n'en a aucune. Elle n'avait donc pas de chemin du tout : il fallait attendre le cycle
    suivant et l'y glisser, ou renoncer à la facturer.

    CE QUI NE CHANGE PAS. Le document reste une facture entière : même numérotation, même PDF,
    même contrôle de conformité, même créance, même écriture VENTES. Rien n'est allégé sous
    prétexte qu'elle est exceptionnelle — c'est une facture remise à un tiers.

    CE QUI EST EXIGÉ. Au moins une ligne, chacune avec un libellé et un montant non nul. Un
    montant négatif est refusé : une facture qui rend de l'argent est un AVOIR, et l'avoir a son
    propre parcours (`creer_avoir`), avec son lien vers la facture d'origine.
    """
    proprietaire_id = str(proprietaire_id or "").strip()
    logement_id = str(logement_id or "").strip()
    mois = str(mois or "").strip()
    if not (proprietaire_id and logement_id and mois):
        raise FactureProprietaireError(
            f"{C_SOURCE_INCOMPLETE}: propriétaire, logement et mois sont obligatoires")
    exiger_periode_v1(mois, db_path=db_path)

    preparees = []
    for i, l in enumerate(lignes or [], start=1):
        libelle = str(l.get("libelle") or "").strip()
        try:
            montant = round(float(str(l.get("montant") or "0").replace(",", ".")), 2)
        except (TypeError, ValueError):
            raise FactureProprietaireError(
                f"{C_SOURCE_INCOMPLETE}: ligne {i}, montant illisible « {l.get('montant')} »")
        if not libelle:
            raise FactureProprietaireError(f"{C_SOURCE_INCOMPLETE}: ligne {i} sans libellé")
        if montant <= 0:
            raise FactureProprietaireError(
                f"{C_SOURCE_INCOMPLETE}: ligne {i}, montant {montant:.2f} € — une facture qui rend "
                f"de l'argent est un avoir, pas une facture négative")
        # `EXTRA` est le type canonique d'une prestation ponctuelle (cf.
        # `factures_proprietaires_composition_service.TYPE_EXTRA`) : c'est celui d'une ligne qui
        # n'est ni une commission, ni un ménage, ni une charge refacturée — exactement le cas ici.
        type_ligne = str(l.get("type_ligne") or "EXTRA").strip()
        if type_ligne not in TYPES_LIGNE_SAISISSABLES + ("EXTRA",):
            raise FactureProprietaireError(
                f"{C_SOURCE_INCOMPLETE}: ligne {i}, type « {type_ligne} » non facturable")
        preparees.append({"numero_ligne": i, "type_ligne": type_ligne, "libelle": libelle,
                          "montant": montant})
    if not preparees:
        raise FactureProprietaireError(f"{C_SOURCE_INCOMPLETE}: aucune ligne facturable")

    total = round(sum(l["montant"] for l in preparees), 2)
    conn = get_db(db_path)
    try:
        # PAS de contrôle d'unicité ici, volontairement : deux interventions ponctuelles dans un
        # même mois font deux factures. L'index d'unicité, lui, ne garde que le cycle mensuel.
        fid = _opaque("FPR")
        conn.execute(
            "INSERT INTO factures_proprietaires "
            "(facture_id_opaque, type_document, proprietaire_id, logement_id, mois, "
            " montant_total, statut, source_calcul, acteur) VALUES (?,?,?,?,?,?,?,?,?)",
            (fid, TYPE_FACTURE, proprietaire_id, logement_id, mois, total, ST_BROUILLON,
             SOURCE_EXCEPTIONNELLE, acteur or "local"))
        for l in preparees:
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant, "
                " objet_source_type) VALUES (?,?,?,?,?,?,?)",
                (_opaque("FPRL"), fid, l["numero_ligne"], l["type_ligne"], l["libelle"],
                 l["montant"], SOURCE_EXCEPTIONNELLE))
        _journal(conn, fid, "CREATION_EXCEPTIONNELLE", None, ST_BROUILLON,
                 f"{len(preparees)} ligne(s), {total:.2f} €", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"facture_id_opaque": fid, "montant_total": total, "statut": ST_BROUILLON,
            "type_document": TYPE_FACTURE, "source_calcul": SOURCE_EXCEPTIONNELLE,
            "nb_lignes": len(preparees)}


def reservations_a_controler(mois: str, logement_id: str, *, db_path=None,
                             conn=None) -> list[dict[str, Any]]:
    """Réservations du logement pour le mois, dans le jeu résolu ACTIF, encore À CONTRÔLER."""
    propre = conn is None
    conn = conn or get_db(db_path)
    try:
        try:
            ds = conn.execute("SELECT dataset_id FROM reservations_datasets WHERE etape='RESOLUES' "
                              "AND actif=1 ORDER BY rowid DESC LIMIT 1").fetchone()
        except sqlite3.Error:
            return []
        if ds is None:
            return []
        return [dict(r) for r in conn.execute(
            "SELECT reservation_calc_id, reservation_id_hostaway, reservation_hh_id, source, "
            "date_arrivee, date_depart, code_anomalie, commentaire FROM reservations_resolues "
            "WHERE dataset_id=? AND mois=? AND logement_id=? AND statut_controle='A_CONTROLER' "
            "ORDER BY date_arrivee", (ds[0], mois, logement_id))]
    finally:
        if propre:
            conn.close()


def exiger_reservations_resolues(mois: str, logement_id: str, *, db_path=None) -> None:
    restantes = reservations_a_controler(mois, logement_id, db_path=db_path)
    if restantes:
        detail = "; ".join(
            f"{r.get('reservation_id_hostaway') or r.get('reservation_hh_id') or r['reservation_calc_id']}"
            f" du {r.get('date_arrivee')} ({r.get('code_anomalie') or 'à contrôler'})"
            for r in restantes)
        raise FactureProprietaireError(
            f"{C_RESERVATIONS_A_CONTROLER}: {len(restantes)} réservation(s) à contrôler pour ce "
            f"logement et ce mois — elles seraient absentes de la facture : {detail}")


def creer(source: dict[str, Any], *, acteur: str = "", db_path=None,
          type_document: str = TYPE_FACTURE, facture_origine: str | None = None,
          decisions_charges: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Crée une facture BROUILLON. Anti-doublon garanti par l'index unique du schéma.

    `decisions_charges` : voir `previsualiser()`. Purement représentatif à ce stade — aucune
    position de refacturation n'est consommée avant `valider()`.
    """
    exiger_periode_v1(source.get("mois"), db_path=db_path)
    exiger_reservations_resolues(source.get("mois"), source.get("logement_id"), db_path=db_path)
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
        # Le total source est gelé ICI, une fois pour toutes. C'est le seul instant où il est
        # écrit : aucune régénération Lot10/Lot12 ne peut plus le déplacer.
        conn.execute(
            "INSERT OR IGNORE INTO factures_proprietaires_meta "
            "(facture_id_opaque, total_source_calcule) VALUES (?,?)",
            (fid, apercu["montant_total"]))
        _figer_reservations(conn, fid, source["mois"], source["proprietaire_id"],
                            source["logement_id"])
        _journal(conn, fid, "CREATION", None, ST_BROUILLON,
                 f"{len(apercu['lignes'])} lignes, total {apercu['montant_total']:.2f}", acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(fid, db_path=db_path)


# ── Réservations de la période : instantané, jamais une jointure ────────────────────────────────

def _figer_reservations(conn, facture_id: str, mois: str, proprietaire_id: str,
                        logement_id: str, *, debut: str = "", fin: str = "") -> int:
    """Fige, à la création du BROUILLON, les réservations de la période. Purement informatif.

    POURQUOI UN INSTANTANÉ ET PAS UNE JOINTURE — audit exécuté sur une copie de la base réelle :
    `factures_proprietaires.source_calcul` porte le `facture_id` Lot12 (ex.
    'PREF-2026-08-PROP_0001-LOG_0001'), qui n'est PAS scopé par run — la même valeur existe dans
    quatre runs Lot12, avec des `nb_reservations` différents (18/36/9/9). `lot12_runs` ne référence
    aucun run Lot10, et `reservations_resolues` est régénérée par dataset. Aucune jointure ne
    reproduit donc l'état vu par le calcul source ; seul un gel le garantit.

    Une panne de lecture ne fait JAMAIS échouer la création de la facture : la section réservations
    est informative, son absence est visible à l'écran et ne dégrade aucun montant.
    """
    try:
        actif = conn.execute(
            "SELECT run_id FROM lot10_runs WHERE actif=1 ORDER BY id DESC LIMIT 1").fetchone()
        run_id = actif["run_id"] if actif else None
        if run_id is None:
            return 0
        # Période libre : même critère que le moteur (date d'arrivée dans les bornes) ;
        # cycle mensuel : le mois de la réservation.
        filtre, borne = (("date_arrivee >= ? AND date_arrivee <= ?", (debut, fin))
                         if debut and fin else ("mois=?", (mois,)))
        lignes = conn.execute(
            "SELECT reservation_id_hostaway, date_arrivee, date_depart, nuits, guest_count, "
            "       payout_calcule, channel_type, assiette_commission, taux_commission, "
            "       commission_conciergerie, menage_retenu "
            f"FROM lot10_commissions WHERE run_id=? AND {filtre} AND proprietaire_id=? "
            "AND logement_id=? ORDER BY date_arrivee, reservation_id_hostaway",
            (run_id, *borne, proprietaire_id, logement_id)).fetchall()
    except sqlite3.Error:
        return 0

    noms = _noms_voyageurs(conn, [str(l["reservation_id_hostaway"] or "") for l in lignes])
    for l in lignes:
        rid = str(l["reservation_id_hostaway"] or "")
        if not rid:
            continue
        # Assiette, taux, ménage et commission sont FIGÉS avec le séjour (migration 0122) : ce
        # sont eux que l'utilisateur peut ajuster sur le brouillon (ménage offert, commission
        # négociée). Les valeurs d'origine restent à côté, pour que l'écart reste visible.
        menage, commission = _round(l["menage_retenu"]), _round(l["commission_conciergerie"])
        conn.execute(
            "INSERT OR IGNORE INTO factures_proprietaires_reservations "
            "(facture_id_opaque, reservation_id, guest_name, check_in, check_out, nights, "
            " guest_count, payout, plateforme, source_run_id, assiette, taux, menage, commission, "
            " menage_initial, commission_initiale) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (facture_id, rid, noms.get(rid), l["date_arrivee"], l["date_depart"], l["nuits"],
             l["guest_count"], _round(l["payout_calcule"]), l["channel_type"], run_id,
             _round(l["assiette_commission"]), l["taux_commission"], menage, commission,
             menage, commission))
    return len(lignes)


def _noms_voyageurs(conn, reservation_ids: list[str]) -> dict[str, str]:
    """Nom du voyageur, extrait du payload Hostaway conservé.

    Aucune colonne `guest_name` n'existe dans `reservations_resolues`, `lot10_commissions` ni
    `hostaway_reservations` (vérifié) : la seule source est `payload_json`, tronqué à 4000
    caractères. `guestName` s'y trouve tôt dans la charge utile, donc présent en pratique — mais le
    JSON tronqué n'est pas parsable, d'où l'extraction par motif plutôt que `json.loads`.
    Un nom absent reste absent : jamais de valeur inventée.
    """
    import re

    if not reservation_ids:
        return {}
    out: dict[str, str] = {}
    marques = ",".join("?" * len(reservation_ids))
    try:
        rows = conn.execute(
            f"SELECT reservation_id, payload_json FROM hostaway_reservations "
            f"WHERE reservation_id IN ({marques})", reservation_ids).fetchall()
    except sqlite3.Error:
        return {}
    for r in rows:
        charge = r["payload_json"] or ""
        m = re.search(r'"guestName"\s*:\s*"([^"]*)"', charge)
        if m and m.group(1).strip():
            out[str(r["reservation_id"])] = m.group(1).strip()
    return out


def _decomposition_figee(facture_id: str, *, db_path=None) -> dict[str, Any]:
    """Décomposition du montant dû, sans les objets `lignes` (déjà présents dans le snapshot).

    Import local : `factures_proprietaires_composition_service` importe ce module, un import de
    niveau fichier créerait un cycle.
    """
    from app.services import factures_proprietaires_composition_service as compo

    d = compo.decomposition(facture_id, db_path=db_path)
    return {**d, "postes": [{k: v for k, v in p.items() if k != "lignes"} for p in d["postes"]]}


def reservations(facture_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Réservations figées de la période, enrichies de leur commission. Lecture pure.

    ASSIETTE, TAUX et COMMISSION viennent de `lot10_commissions`, jamais d'un calcul refait ici :
    il n'existe qu'une seule formule de commission dans ce projet, et elle appartient à Lot10. La
    jointure est faite sur le `source_run_id` FIGÉ dans l'instantané — pas sur le run actif du
    moment — sinon la facture d'août afficherait les chiffres d'un run de novembre.

    L'enrichissement est facultatif par construction : si le run a été purgé, les colonnes de
    commission restent vides et la section reste lisible. Une facture ne doit pas devenir
    inaffichable parce qu'un run d'historique a disparu.
    """
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            "SELECT * FROM factures_proprietaires_reservations WHERE facture_id_opaque=? "
            "ORDER BY check_in, reservation_id", (facture_id,)).fetchall()]
        for l in lignes:
            if l.get("commission") is not None or l.get("menage") is not None:
                # Séjour figé avec ses montants (0122) : ce sont eux qui font foi sur le document.
                l.update({"assiette_commission": l.get("assiette"),
                          "taux_commission": l.get("taux"),
                          "menage_retenu": l.get("menage"),
                          "menage_modifie": abs(_round(l.get("menage"))
                                                - _round(l.get("menage_initial"))) > TOLERANCE,
                          "commission_modifiee": abs(_round(l.get("commission"))
                                                     - _round(l.get("commission_initiale")))
                                                 > TOLERANCE})
                continue
            run, rid = l.get("source_run_id"), l.get("reservation_id")
            if not (run and rid):
                continue
            try:
                c = conn.execute(
                    "SELECT assiette_commission, taux_commission, commission_conciergerie, "
                    "       menage_retenu FROM lot10_commissions "
                    "WHERE run_id=? AND reservation_id_hostaway=? LIMIT 1", (run, rid)).fetchone()
            except sqlite3.Error:
                c = None
            if c is not None:
                l.update({"assiette_commission": _round(c["assiette_commission"]),
                          "taux_commission": c["taux_commission"],
                          "commission": _round(c["commission_conciergerie"]),
                          "menage_retenu": _round(c["menage_retenu"])})
        return lignes
    finally:
        conn.close()


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
        provenance = {r["ligne_id_opaque"]: dict(r) for r in conn.execute(
            "SELECT * FROM factures_proprietaires_lignes_provenance WHERE facture_id_opaque=?",
            (facture_id,)).fetchall()}
        charges = {r["ligne_id_opaque"]: dict(r) for r in conn.execute(
            "SELECT * FROM factures_proprietaires_lignes_charge WHERE facture_id_opaque=?",
            (facture_id,)).fetchall()}
        meta = conn.execute(
            "SELECT total_source_calcule FROM factures_proprietaires_meta "
            "WHERE facture_id_opaque=?", (facture_id,)).fetchone()
    finally:
        conn.close()
    d = dict(row)
    d["lignes"] = []
    for l in lignes:
        ligne = dict(l)
        # ORIGINE DÉRIVÉE, jamais stockée en double : une ligne née du calcul porte un
        # `objet_source_type`, une ligne saisie à la main n'en a pas. Ajouter une colonne
        # « origine » créerait une seconde vérité qu'il faudrait maintenir cohérente.
        ligne["origine"] = (ORIGINE_CALCULEE if str(l["objet_source_type"] or "").strip()
                            else ORIGINE_MANUELLE)
        prov = provenance.get(l["ligne_id_opaque"])
        ligne["montant_source_initial"] = prov["montant_source_initial"] if prov else None
        ligne["montant_modifie"] = bool(
            prov and abs(_round(prov["montant_source_initial"]) - _round(l["montant"])) > TOLERANCE)
        ligne["charge"] = charges.get(l["ligne_id_opaque"])
        d["lignes"].append(ligne)
    d["evenements"] = [dict(e) for e in evts]
    # Une facture antérieure à 0071 n'a pas de méta : son total source est son total, puisque
    # aucun ajustement manuel ne pouvait exister avant l'édition des brouillons.
    d["total_source_calcule"] = _round(meta["total_source_calcule"] if meta
                                       else row["montant_total"])
    d["total_facture"] = _round(sum(_round(l["montant"]) for l in d["lignes"]))
    d["ajustement_manuel"] = _round(d["total_facture"] - d["total_source_calcule"])
    # L'écran ne montre la décomposition en trois montants QUE si un ajustement existe :
    # afficher « ajustements 0,00 € » sur toutes les factures apprendrait à ne plus le lire.
    d["ajustement_present"] = abs(d["ajustement_manuel"]) > TOLERANCE
    return d


def montants(facture_id: str, *, db_path=None) -> dict[str, Any]:
    """Les trois montants d'une facture, calculés côté serveur — zéro arithmétique en gabarit."""
    f = lire(facture_id, db_path=db_path)
    return {k: f[k] for k in ("total_source_calcule", "total_facture", "ajustement_manuel",
                              "ajustement_present")}


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


# ── Édition des lignes d'un BROUILLON ───────────────────────────────────────────────────────────
# INVARIANT ABSOLU DE TOUTE CETTE SECTION : aucune écriture, jamais, dans une table Lot9/Lot10/
# Lot12. Modifier une facture modifie le DOCUMENT ; le CALCUL reste intact et reste consultable via
# `total_source_calcule`. Supprimer une ligne CALCULEE retire la ligne du document — la ligne Lot12
# d'origine n'est ni touchée ni supprimée.

def _exiger_brouillon(f: dict[str, Any], action: str) -> None:
    """Une facture qui a quitté le BROUILLON n'est plus éditable — jamais en silence.

    Le refus est une exception métier, remontée par la route en 422 HTML lisible (pas un JSON brut,
    pas un 500) : l'utilisateur doit comprendre POURQUOI l'action est refusée.
    """
    if f["statut"] != ST_BROUILLON:
        raise FactureProprietaireError(
            f"statut {f['statut']} : {action} impossible. Seul un BROUILLON est modifiable ; "
            "une facture validée ou émise se corrige par un avoir.")
    # Une facture d'avant le cutover ne s'édite pas non plus (il n'en reste aucune après le
    # cutover ; ce refus garde la règle vraie même pour une donnée reprise hors parcours).
    _exiger_facture_v1(f)


def _resynchroniser_total(conn, facture_id: str) -> float:
    """`montant_total` redevient la somme EXACTE des lignes. Jamais une saisie, toujours un calcul."""
    total = _round(conn.execute(
        "SELECT COALESCE(SUM(montant), 0) FROM factures_proprietaires_lignes "
        "WHERE facture_id_opaque=?", (facture_id,)).fetchone()[0])
    conn.execute(
        "UPDATE factures_proprietaires SET montant_total=?, version=version+1 "
        "WHERE facture_id_opaque=?", (total, facture_id))
    return total


def _prochain_numero_ligne(conn, facture_id: str) -> int:
    return int(conn.execute(
        "SELECT COALESCE(MAX(numero_ligne), 0) + 1 FROM factures_proprietaires_lignes "
        "WHERE facture_id_opaque=?", (facture_id,)).fetchone()[0])


def ajouter_ligne(facture_id: str, *, type_ligne: str, libelle: str, montant: Any,
                  objet_source_type: str | None = None, objet_source_ref: str | None = None,
                  acteur: str = "", commentaire: str = "",
                  justification_imputation: str = "", type_economique: str = "", db_path=None,
                  _conn=None) -> dict[str, Any]:
    """Ajoute une ligne MANUELLE (ou reliée à une source, si `objet_source_type` est fourni).

    `_conn` permet à `ajouter_ligne_charge` d'insérer dans SA transaction — usage interne
    exclusivement, jamais exposé à une route.

    `justification_imputation` (migration 0077) : motif d'une refacturation PARTIELLE. `commentaire`
    alimente le JOURNAL d'événements, pas la ligne — ranger la justification là l'aurait rendue
    introuvable au moment de valider, et indistinguable d'une note quelconque.
    """
    f = lire(facture_id, db_path=db_path)
    _exiger_brouillon(f, "ajout de ligne")
    type_ligne = str(type_ligne or "").strip()
    if not type_ligne:
        raise FactureProprietaireError("type de ligne obligatoire")
    libelle = str(libelle or "").strip()
    if not libelle:
        raise FactureProprietaireError("libelle obligatoire")
    valeur = _round(montant)
    if valeur == 0:
        raise FactureProprietaireError("une ligne a 0 n'a pas d'effet : montant attendu different de 0")

    interne = _conn is not None
    conn = _conn if interne else get_db(db_path)
    try:
        lid = _opaque("FPRL")
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes "
            "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant, "
            " objet_source_type, objet_source_ref, justification_imputation, type_economique) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (lid, facture_id, _prochain_numero_ligne(conn, facture_id), type_ligne, libelle,
             valeur, objet_source_type, objet_source_ref,
             str(justification_imputation or "").strip() or None,
             # NULL : dérivé du type technique par la base (déclencheur 0114).
             str(type_economique or "").strip() or None))
        total = _resynchroniser_total(conn, facture_id)
        _journal(conn, facture_id, EVT_AJOUT_LIGNE, ST_BROUILLON, ST_BROUILLON,
                 _commentaire_ligne(type_ligne, libelle, valeur, total, commentaire), acteur)
        if not interne:
            conn.commit()
    finally:
        if not interne:
            conn.close()
    return {"ok": True, "ligne_id_opaque": lid}


def _commentaire_ligne(type_ligne: str, libelle: str, montant: float, total: float,
                       extra: str = "") -> str:
    """Format aligné sur l'existant (`CREATION` : « N lignes, total X.XX ») : montant, objet, total."""
    base = f"{type_ligne} « {libelle} » {montant:.2f}, total {total:.2f}"
    return f"{base} — {extra}" if str(extra or "").strip() else base


def modifier_ligne(facture_id: str, ligne_id: str, *, libelle: str | None = None,
                   montant: Any = None, acteur: str = "", commentaire: str = "",
                   db_path=None) -> dict[str, Any]:
    """Modifie une ligne d'un BROUILLON.

    Si la ligne est CALCULEE, son montant d'origine est conservé AVANT la première modification
    (`factures_proprietaires_lignes_provenance`, écrit une seule fois) : l'écart au calcul reste
    reconstituable pour toujours, même après plusieurs corrections successives. Le calcul source
    lui-même n'est jamais touché.
    """
    f = lire(facture_id, db_path=db_path)
    _exiger_brouillon(f, "modification de ligne")
    ligne = next((l for l in f["lignes"] if l["ligne_id_opaque"] == ligne_id), None)
    if ligne is None:
        raise FactureProprietaireError(f"ligne inconnue sur cette facture : {ligne_id}")

    nouveau_libelle = str(libelle).strip() if libelle is not None else ligne["libelle"]
    if not nouveau_libelle:
        raise FactureProprietaireError("libelle obligatoire")
    nouveau_montant = _round(montant) if montant is not None else _round(ligne["montant"])
    if nouveau_montant == 0:
        raise FactureProprietaireError("une ligne a 0 n'a pas d'effet : montant attendu different de 0")

    conn = get_db(db_path)
    try:
        if ligne["origine"] == ORIGINE_CALCULEE:
            conn.execute(
                "INSERT OR IGNORE INTO factures_proprietaires_lignes_provenance "
                "(ligne_id_opaque, facture_id_opaque, montant_source_initial, objet_source_type, "
                " objet_source_ref) VALUES (?,?,?,?,?)",
                (ligne_id, facture_id, _round(ligne["montant"]), ligne["objet_source_type"],
                 ligne["objet_source_ref"]))
        conn.execute(
            "UPDATE factures_proprietaires_lignes SET libelle=?, montant=? WHERE ligne_id_opaque=?",
            (nouveau_libelle, nouveau_montant, ligne_id))
        total = _resynchroniser_total(conn, facture_id)
        detail = (f"{ligne['origine']} {_round(ligne['montant']):.2f} -> {nouveau_montant:.2f}"
                  + (f" ; {AVERTISSEMENT_LIGNE_CALCULEE}"
                     if ligne["origine"] == ORIGINE_CALCULEE else ""))
        _journal(conn, facture_id, EVT_MODIFICATION_LIGNE, ST_BROUILLON, ST_BROUILLON,
                 _commentaire_ligne(ligne["type_ligne"], nouveau_libelle, nouveau_montant, total,
                                    f"{detail}{(' ; ' + commentaire) if commentaire else ''}"),
                 acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id}


def supprimer_ligne(facture_id: str, ligne_id: str, *, acteur: str = "", commentaire: str = "",
                    db_path=None) -> dict[str, Any]:
    """Retire une ligne du DOCUMENT. La source de calcul correspondante n'est jamais supprimée.

    Une ligne CALCULEE peut être retirée tant que la facture est un BROUILLON : c'est une décision
    de facturation (« je ne facture pas cet élément ce mois-ci »), pas une correction de calcul.
    L'événement conserve la trace qu'une ligne CALCULEE a été retirée, avec son montant.
    """
    f = lire(facture_id, db_path=db_path)
    _exiger_brouillon(f, "suppression de ligne")
    ligne = next((l for l in f["lignes"] if l["ligne_id_opaque"] == ligne_id), None)
    if ligne is None:
        raise FactureProprietaireError(f"ligne inconnue sur cette facture : {ligne_id}")

    conn = get_db(db_path)
    try:
        conn.execute("DELETE FROM factures_proprietaires_lignes WHERE ligne_id_opaque=?",
                     (ligne_id,))
        total = _resynchroniser_total(conn, facture_id)
        detail = f"origine {ligne['origine']}"
        if ligne["origine"] == ORIGINE_CALCULEE:
            detail += (f" ; ligne calculee retiree du document — source {ligne['objet_source_type']}"
                       f"/{ligne['objet_source_ref']} INCHANGEE")
        _journal(conn, facture_id, EVT_SUPPRESSION_LIGNE, ST_BROUILLON, ST_BROUILLON,
                 _commentaire_ligne(ligne["type_ligne"], ligne["libelle"],
                                    _round(ligne["montant"]), total,
                                    f"{detail}{(' ; ' + commentaire) if commentaire else ''}"),
                 acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id, "charge_liee": ligne.get("charge")}


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
    _exiger_facture_v1(f, db_path=db_path)
    if f["statut"] != ST_BROUILLON:
        raise FactureProprietaireError(f"statut {f['statut']}: seul un BROUILLON peut etre valide")
    if f.get("type_document", TYPE_FACTURE) == TYPE_FACTURE:
        exiger_reservations_resolues(f["mois"], f["logement_id"], db_path=db_path)

    # Identifiant d'entreprise : SIRET (établissement, 14 chiffres) OU SIREN (entreprise, 9).
    # Exiger le SIRET seul bloquait une société qui ne dispose que de son SIREN — et la seule issue
    # aurait été d'en fabriquer un en complétant 5 chiffres, c'est-à-dire d'inventer un
    # établissement sur un document légal. Les deux identifiants restent stockés séparément et
    # imprimés sous leur propre étiquette : un SIREN n'est jamais présenté comme un SIRET.
    manques = [c for c in ("nom", "adresse") if not str(emetteur.get(c) or "").strip()]
    if not (str(emetteur.get("siret") or "").strip() or str(emetteur.get("siren") or "").strip()):
        manques.append("siret ou siren")
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

    # Justification d'une refacturation PARTIELLE, portée par la ligne elle-même. `imputer()` en
    # exige une dès que le montant diffère du solde ; sans cette relecture, une facture composée
    # avec un montant partiel se validait... jamais : le motif était demandé à un moment où plus
    # personne ne pouvait le fournir. Il est désormais saisi au moment de la décision (voir
    # `composition_service.rattacher_charge`) et retrouvé ici.
    for l in lignes_charges:
        ref = l.get("objet_source_ref")
        if not justifications.get(ref) and l.get("justification_imputation"):
            justifications[ref] = str(l["justification_imputation"]).strip()

    from app.services import charges_refacturation_service as refac

    conn = get_db(db_path)
    try:
        for l in lignes_charges:
            # Contrôle du contrat AVANT l'appel : sans lui, une ligne qui référencerait la charge
            # au lieu de sa position ressortirait en « Position introuvable », un message qui
            # désigne la position comme coupable alors que le défaut est dans la ligne.
            if str(l.get("objet_source_type") or "") != SOURCE_POSITION_REFAC:
                raise FactureProprietaireError(
                    f"{C_SOURCE_INCOMPLETE}: ligne {l['numero_ligne']} CHARGE_REFACTUREE de source "
                    f"{l.get('objet_source_type')!r} — une charge refacturee doit referencer sa "
                    f"position de refacturation ({SOURCE_POSITION_REFAC})")
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


def repasser_en_brouillon(facture_id: str, *, acteur: str = "", motif: str = "",
                          db_path=None) -> dict[str, Any]:
    """VALIDE -> BROUILLON. Corrige une facture validée trop tôt, sans passer par un avoir.

    VALIDE ≠ ÉMIS, et c'est toute la raison d'être de cette fonction. Une facture VALIDE n'a ni
    numéro définitif, ni PDF figé, ni écriture comptable : rien d'irréversible n'a encore été
    produit, la rouvrir ne trompe personne. Une facture ÉMISE, elle, a consommé un numéro de la
    série légale et constaté une vente : elle se corrige par annulation ou avoir, jamais par un
    retour discret à l'état modifiable. Les deux cas sont donc traités différemment — ce refus est
    la garantie que la séquence de numérotation reste continue et opposable.

    Les imputations faites à la validation sont RENDUES aux positions : sans cela le montant
    resterait consommé alors que la ligne redevient modifiable, et serait compté deux fois.
    Tout se fait dans une seule transaction.
    """
    from app.services import charges_refacturation_service as refac

    f = lire(facture_id, db_path=db_path)
    if f["statut"] == ST_BROUILLON:
        return f                      # déjà brouillon : rien à faire, pas une erreur
    if f["statut"] != ST_VALIDE:
        raise FactureProprietaireError(
            f"statut {f['statut']}: seule une facture VALIDE peut repasser en brouillon. "
            f"Une facture emise est numerotee et comptabilisee : utilisez l'annulation ou l'avoir.")
    if f.get("numero_facture"):
        raise FactureProprietaireError(
            f"{C_SOURCE_INCOMPLETE}: la facture porte deja le numero {f['numero_facture']} — "
            f"un numero attribue ne se libere pas")

    conn = get_db(db_path)
    try:
        ecr = conn.execute(
            "SELECT ecriture_id_opaque FROM ecritures WHERE origine_type = ? "
            "AND origine_id_opaque = ? AND statut <> 'CONTREPASSEE'",
            ("FACTURE_PROPRIETAIRE", facture_id)).fetchone()
        if ecr is not None:
            raise FactureProprietaireError(
                f"une ecriture comptable existe deja pour cette facture ({ecr[0]}) : "
                f"elle ne peut pas repasser en brouillon")

        liberees = []
        for l in f["lignes"]:
            if l["type_ligne"] != "CHARGE_REFACTUREE":
                continue
            if str(l.get("objet_source_type") or "") != SOURCE_POSITION_REFAC:
                continue
            r = refac.desimputer(l["objet_source_ref"], l["montant"], facture_id=facture_id,
                                 acteur=acteur, motif=motif or "retour en brouillon", conn=conn)
            if not r.get("ok"):
                raise FactureProprietaireError(
                    f"LIBERATION_REFUSEE {l['objet_source_ref']}: {r.get('message')}")
            liberees.append(l["objet_source_ref"])

        conn.execute(
            "UPDATE factures_proprietaires SET statut=?, date_validation=NULL, "
            "version=version+1 WHERE facture_id_opaque=?", (ST_BROUILLON, facture_id))
        _journal(conn, facture_id, "RETOUR_BROUILLON", ST_VALIDE, ST_BROUILLON,
                 motif or f"{len(liberees)} position(s) liberee(s)", acteur)
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


def prochain_numero(mois: str, *, type_document: str = TYPE_FACTURE, db_path=None) -> str:
    """Le numéro que recevrait la PROCHAINE émission de ce mois — sans rien consommer.

    Même format (`facturation_config_service`) et même compteur que `_attribuer_numero` : sert à
    vérifier que la séquence d'un mois (ex. `2026-09-001` après le cutover V1) est prête, sans
    jamais créer de facture ni brûler de numéro définitif.
    """
    from app.services import facturation_config_service as fconf

    serie = fconf.serie_mois(type_document, mois)
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT dernier_numero FROM factures_proprietaires_sequence WHERE serie=?",
                         (serie,)).fetchone()
    finally:
        conn.close()
    return fconf.formater_numero(serie, (int(r[0]) if r else 0) + 1)


# ── Émission ────────────────────────────────────────────────────────────────────────────────────

def emettre(facture_id: str, *, emetteur: dict[str, Any], destinataire: dict[str, Any],
            serie: str | None = None, date_facture: str, generer_pdf=None, acteur: str = "",
            db_path=None, exiger_conformite: bool = False,
            hors_compta: bool = False) -> dict[str, Any]:
    """VALIDE -> EMIS. Fige le snapshot, attribue le numéro, génère le document, enregistre le hash.

    `generer_pdf(snapshot) -> (nom_fichier, sha256)` est injecté : ce service ne connaît ni le
    format du document ni son emplacement de stockage.
    """
    from app.services import facturation_config_service as fconf
    from app.services import factures_proprietaires_conformite_service as conformite

    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_VALIDE:
        raise FactureProprietaireError(f"statut {f['statut']}: seul un VALIDE peut etre emis")
    _exiger_facture_v1(f, db_path=db_path)
    if f.get("type_document", TYPE_FACTURE) == TYPE_FACTURE:
        exiger_reservations_resolues(f["mois"], f["logement_id"], db_path=db_path)

    # Contrôle de pré-émission : il décrit toujours l'état de conformité, mais ne bloque que si on
    # le lui demande. La recette peut ainsi émettre avec une configuration fictive incomplète,
    # tandis que l'émission réelle exige `exiger_conformite=True` et refuse au premier manque.
    controle = conformite.verifier(f, date_facture=date_facture, db_path=db_path)
    if exiger_conformite and controle["statut"] != conformite.PRETE:
        codes = ", ".join(m["code"] for m in controle["manques"])
        raise FactureProprietaireError(f"emission refusee — conformite incomplete: {codes}")
    bloc = controle["conformite"]

    # La date d'émission est NORMALISÉE avant tout usage : saisie « 11/09/2026 », elle produisait
    # auparavant la série « F-11/0 » par simple découpage `[:4]`. Un format inattendu doit être
    # converti ou refusé, jamais tronqué en silence.
    date_facture = _normaliser_date(date_facture)

    # Série MENSUELLE indexée sur le mois de PRESTATION (§22) : `2026-08-001`. Elle ne dépend plus
    # d'une date d'affichage, donc aucune saisie ne peut plus la corrompre. Factures et avoirs
    # gardent des compteurs distincts, et chaque mois rouvre sa propre séquence à 001.
    if serie is None:
        serie = fconf.serie_mois(f["type_document"], f["mois"])

    conn = get_db(db_path)
    try:
        numero = _attribuer_numero(conn, serie)

        # CRÉDIT CLIENT (2026-10-03) : à l'émission d'une FACTURE en comptabilité, et seulement
        # là, le crédit disponible du client s'impute sur la créance — jamais plus que ce qui
        # reste dû, le reliquat demeure sur le crédit. DANS cette transaction : si l'émission
        # échoue, rien n'est imputé. Le montant de la facture n'est jamais modifié.
        imputations_credit: list[dict[str, Any]] = []
        credit_restant = 0.0
        if f["type_document"] == TYPE_FACTURE and not hors_compta:
            from app.services import credits_clients_service as credits
            deja = _round(sum(_round(a["montant"]) for a in acomptes_proprietaire(
                facture_id, db_path=db_path) if a["statut"] == "VALIDE" and int(a["actif"] or 0))
                + sum(_round(r["montant_impute"]) for r in reversements_airbnb(
                    facture_id, db_path=db_path)))
            imputations_credit = credits.imputer_a_l_emission(
                conn, f, a_couvrir=_round(_round(f["montant_total"]) - deja), acteur=acteur)
            credit_restant = _round(sum(c["reste"] for c in credits.credits_disponibles(
                conn, f["proprietaire_id"])))
        decomposition = _decomposition_figee(facture_id, db_path=db_path)
        if imputations_credit:
            from app.services import factures_proprietaires_composition_service as compo
            compo.ajouter_credit_client(decomposition,
                                        sum(i["montant"] for i in imputations_credit),
                                        credit_restant)

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
            # Séjours et décomposition FIGÉS avec le reste : le PDF d'une facture émise doit rester
            # reproductible même si les datasets de réservations sont régénérés ensuite. Les figer
            # ici est la seule façon d'y parvenir — les relire plus tard donnerait un autre document.
            "reservations": reservations(facture_id, db_path=db_path),
            "decomposition": decomposition,
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
            "date_emission=strftime('%Y-%m-%dT%H:%M:%SZ','now'), version=version+1, "
            "hors_compta=?, motif_hors_compta=COALESCE(motif_hors_compta, ?) "
            "WHERE facture_id_opaque=?",
            (ST_EMIS, numero, date_facture, payload, snapshot_hash, document_nom,
             document_hash, 1 if hors_compta else 0,
             MOTIF_HORS_COMPTA_DEFAUT if hors_compta else None, facture_id))
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
    # Les écritures 419100 → 411000 des crédits imputés suivent la vente, dans
    # `comptabilite_ecritures_service.comptabiliser_facture_emise` : CA, créance, puis crédit.
    # Une FACTURE émise devient une créance du FIFO : les allocations du propriétaire sont
    # persistées maintenant (un avoir, lui, n'entre pas dans le FIFO — voir compte propriétaire).
    if f["type_document"] in (TYPE_FACTURE, TYPE_AVOIR):
        # Un AVOIR émis est une source du FIFO (il diminue la créance, son surplus devient crédit).
        from app.services import compte_proprietaire_service as cpt
        cpt.apres_ecriture([f["proprietaire_id"]], declencheur=cpt.DECL_EMISSION_FACTURE,
                           db_path=db_path)
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

ST_REGLEMENT_HORS_COMPTA = "HORS_COMPTA"
EVT_HORS_COMPTA = "HORS_COMPTA"
MOTIF_HORS_COMPTA_DEFAUT = "Émise hors comptabilité : aucun paiement attendu sur le compte"


def marquer_hors_compta(facture_id: str, *, motif: str = "", acteur: str = "",
                        db_path=None) -> dict[str, Any]:
    """Facture ÉMISE conservée HORS COMPTA (choix fait à l'émission ; par défaut, en compta).

    Aucune écriture VENTES, aucune créance, aucun règlement attendu ; la facture, son numéro et son
    PDF restent. Refusé si une écriture de vente existe déjà : on ne sort pas en silence une vente
    déjà constatée (il faudrait un avoir).
    """
    f = lire(facture_id, db_path=db_path)
    if f["statut"] != ST_EMIS or f["type_document"] != TYPE_FACTURE:
        raise FactureProprietaireError("seule une facture ÉMISE peut être conservée hors compta")
    conn = get_db(db_path)
    try:
        try:
            vente = conn.execute(
                "SELECT COUNT(*) FROM ecritures WHERE origine_id_opaque=? AND statut <> 'ANNULEE'",
                (facture_id,)).fetchone()[0]
        except sqlite3.Error:
            vente = 0
        if vente:
            raise FactureProprietaireError(
                "une écriture de vente existe déjà pour cette facture : elle est en comptabilité")
        motif = str(motif or "").strip() or MOTIF_HORS_COMPTA_DEFAUT
        conn.execute("UPDATE factures_proprietaires SET hors_compta=1, motif_hors_compta=?, "
                     "version=version+1 WHERE facture_id_opaque=?", (motif, facture_id))
        _journal(conn, facture_id, EVT_HORS_COMPTA, ST_EMIS, ST_EMIS, motif, acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(facture_id, db_path=db_path)


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
    _exiger_facture_v1(f, db_path=db_path)

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


def creer_avoir_libre(*, proprietaire_id: str, motif: str, montant: Any, logement_id: str = "",
                      facture_origine: str = "", mois: str = "", acteur: str = "",
                      db_path=None) -> dict[str, Any]:
    """AVOIR en BROUILLON, créé depuis Factures / Créances (2026-10-03) : client, facture d'origine
    facultative, motif, montant. Brouillon : aucun impact. Émis (même parcours qu'une facture :
    validation, numéro `A-…`, PDF, écriture de vente inversée), il diminue la créance du client ;
    s'il dépasse ce qui reste dû, le surplus devient un crédit disponible (compte propriétaire).

    L'avoir porte une seule ligne de RÉDUCTION négative : comptabilisée au débit de 709600
    (« rabais, remises et ristournes accordés »), jamais en produit négatif."""
    pid = str(proprietaire_id or "").strip()
    motif = str(motif or "").strip()
    if not pid:
        raise FactureProprietaireError(f"{C_SOURCE_INCOMPLETE}: client obligatoire")
    if not motif:
        raise FactureProprietaireError("avoir sans motif refusé")
    try:
        valeur = round(float(str(montant).replace(",", ".").replace("€", "").strip()), 2)
    except (TypeError, ValueError):
        raise FactureProprietaireError(f"montant illisible « {montant} »")
    if valeur <= 0:
        raise FactureProprietaireError("le montant d'un avoir est strictement positif")
    origine = str(facture_origine or "").strip() or None
    if origine:
        f = lire(origine, db_path=db_path)
        if f["proprietaire_id"] != pid or f["type_document"] != TYPE_FACTURE \
                or f["statut"] != ST_EMIS:
            raise FactureProprietaireError(
                "la facture d'origine doit être une facture ÉMISE de ce client")
        logement_id, mois = f["logement_id"], f["mois"]
    logement_id = str(logement_id or "").strip()
    mois = str(mois or "").strip()[:7]
    if not (logement_id and mois):
        raise FactureProprietaireError(
            f"{C_SOURCE_INCOMPLETE}: logement et mois obligatoires sans facture d'origine")
    exiger_periode_v1(mois, db_path=db_path)

    conn = get_db(db_path)
    try:
        aid = _opaque("FPR")
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, facture_origine, "
            "proprietaire_id, logement_id, mois, montant_total, statut, source_calcul, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (aid, TYPE_AVOIR, origine, pid, logement_id, mois, -valeur, ST_BROUILLON,
             SOURCE_EXCEPTIONNELLE, acteur or "local"))
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes (ligne_id_opaque, facture_id_opaque, "
            "numero_ligne, type_ligne, libelle, montant) VALUES (?,?,?,?,?,?)",
            (_opaque("FPRL"), aid, 1, "REDUCTION", f"Avoir — {motif}"[:240], -valeur))
        _journal(conn, aid, "CREATION", None, ST_BROUILLON,
                 f"avoir {valeur:.2f} €{(' sur ' + origine) if origine else ''} : {motif}", acteur)
        if origine:
            _journal(conn, origine, "AVOIR_CREE", ST_EMIS, ST_EMIS,
                     f"avoir {aid} ({valeur:.2f} €) : {motif}", acteur)
        conn.commit()
    finally:
        conn.close()
    return lire(aid, db_path=db_path)


# ── Solde ───────────────────────────────────────────────────────────────────────────────────────

def reversements_airbnb(facture_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Reversements Airbnb rattachés à cette facture (`imputations_airbnb.document_id`).

    Objet canonique inchangé : un reversement Airbnb reste un PAYOUT_PLATEFORME. Il ne crée ni
    réservation, ni charge, ni mouvement bancaire, ni écriture comptable.
    """
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM imputations_airbnb WHERE document_id=? ORDER BY date_imputation, "
            "imputation_airbnb_id", (facture_id,)).fetchall()]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def acomptes_proprietaire(facture_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Acomptes propriétaire rattachés à cette facture (`reference_metier`).

    Seuls les mouvements VALIDE et actifs déduisent réellement — même règle que Lot10
    (`charger_acomptes_proprietaires_sqlite` filtre `statut='VALIDE' AND actif=1`). Les autres sont
    rendus pour affichage, avec leur statut, jamais silencieusement ignorés.
    """
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM mouvements_tresorerie_proprietaires WHERE reference_metier=? "
            "AND nature='ACOMPTE_PROPRIETAIRE' AND statut <> 'ANNULE' "
            "ORDER BY date_mouvement, id", (facture_id,)).fetchall()]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def solde(facture_id: str, *, paiements_imputes: float = 0.0, db_path=None) -> dict[str, Any]:
    """Solde dérivé, jamais stocké. Le règlement ne recrée aucun objet économique : il impute un
    montant sur une créance qui existe déjà.

    Deux déductions supplémentaires sont pliées ici, jamais dans le gabarit (§ « zéro arithmétique
    en Jinja ») : les REVERSEMENTS AIRBNB (`imputations_airbnb`) et les ACOMPTES PROPRIÉTAIRE
    (`mouvements_tresorerie_proprietaires`). Ce sont des paiements DÉJÀ REÇUS que l'on impute sur
    une créance existante — pas des lignes de facture, et donc jamais un composant du total.
    """
    f = lire(facture_id, db_path=db_path)
    total = _round(f["montant_total"])
    if int(f.get("hors_compta") or 0):
        # Hors compta : la facture existe et a été envoyée, mais aucun paiement n'est attendu.
        return {"facture_id_opaque": facture_id, "montant_total": total, "paiements_imputes": 0.0,
                "solde": 0.0, "statut_reglement": ST_REGLEMENT_HORS_COMPTA,
                "reversements_airbnb": [], "acomptes_proprietaire": [],
                "total_reversements_airbnb": 0.0, "total_acomptes_proprietaire": 0.0,
                "autres_paiements": 0.0}
    reversements = reversements_airbnb(facture_id, db_path=db_path)
    acomptes = acomptes_proprietaire(facture_id, db_path=db_path)
    total_reversements = _round(sum(_round(r["montant_impute"]) for r in reversements))
    # Seul un acompte VALIDE et actif déduit : un brouillon d'acompte ne règle rien.
    total_acomptes = _round(sum(_round(a["montant"]) for a in acomptes
                                if a["statut"] == "VALIDE" and int(a["actif"] or 0) == 1))
    impute = _round(_round(paiements_imputes) + total_reversements + total_acomptes)
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
            "paiements_imputes": impute, "solde": reste, "statut_reglement": statut,
            "reversements_airbnb": reversements, "acomptes_proprietaire": acomptes,
            "total_reversements_airbnb": total_reversements,
            "total_acomptes_proprietaire": total_acomptes,
            "autres_paiements": _round(paiements_imputes)}

"""Couche RAW Hostaway — ce que l'API a retourné, tel quel, en SQLite.

    API Hostaway → hostaway_extractions + hostaway_reservations / _payouts / _listings / …

CE QUE « RAW » VEUT DIRE ICI
Les lignes portent les champs renvoyés par la plateforme, sans interprétation métier : aucun montant
recalculé, aucun rattachement à un logement interne, aucun statut de contrôle applicatif. Ces
jugements appartiennent aux étapes suivantes (Lot 4bis, Lot 4quater), qui écrivent ailleurs. Séparer
le fait de son interprétation permet de rejouer un calcul sans réinterroger l'API.

RÉSERVATION ET PAYOUT SONT DEUX FAITS DISTINCTS
Ils vivent dans deux tables. Un payout peut manquer, arriver plus tard, ou être incomplet : fondu
dans la réservation, « payout absent » deviendrait indistinguable de « payout à zéro », et un revenu
manquant ressemblerait à un revenu nul. Un payout n'est jamais reconstruit depuis la Banque — un
mouvement bancaire ne dit pas à quelle réservation il correspond.

IDEMPOTENCE
Une extraction est identifiée. Rejouer la même extraction n'ajoute rien : la contrainte porte sur
(extraction_id, reservation_id). Une nouvelle extraction crée un nouvel `extraction_id`, donc un
nouvel état complet, comparable au précédent ligne à ligne — on ne modifie jamais une extraction
passée pour y refléter une réservation qui a changé depuis.

LE PAYLOAD EST CONSERVÉ
`payload_json` garde la réponse brute. Ce n'est pas de la redondance : le repli historique de
`guestCount` en dépend, et c'est la seule preuve de ce que la plateforme a dit ce jour-là.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from app.db.connection import get_db

MODE_API = "API"
MODE_FIXTURE = "FIXTURE"
MODE_REPRISE_EXCEL = "REPRISE_EXCEL"

ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"

E_AUCUNE_EXTRACTION = "HOSTAWAY_AUCUNE_EXTRACTION"
E_EXTRACTION_PARTIELLE = "HOSTAWAY_EXTRACTION_PARTIELLE"

MESSAGES = {
    E_AUCUNE_EXTRACTION: ("Aucune extraction Hostaway en base : lancez une actualisation avant de "
                          "consulter les réservations."),
    E_EXTRACTION_PARTIELLE: ("La dernière extraction Hostaway est incomplète : les données "
                             "affichées peuvent manquer de réservations récentes."),
}


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _entier(v: Any) -> int | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return int(float(str(v).replace(",", ".")))
    except (TypeError, ValueError):
        return None


def _reel(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def table_presente(nom: str, *, db_path=None) -> bool:
    """Les tables Hostaway n'existent qu'à partir de la migration 0034.

    Une base antérieure est un état LÉGITIME — la base réelle en production n'est pas encore migrée.
    Laisser remonter « no such table » transformerait cet état normal en erreur 500.
    """
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())
    finally:
        conn.close()


# ── Ouverture et clôture d'une extraction ───────────────────────────────────────────────────────

def ouvrir(*, mode: str = MODE_API, run_id: str = "", db_path=None) -> str:
    """Ouvre une extraction et rend son identifiant.

    Ouverte AVANT le premier appel réseau : une extraction qui échoue à mi-parcours doit laisser une
    trace, sinon un échec ressemble à une absence d'exécution.
    """
    extraction_id = "HAX-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut) "
            "VALUES (?,?,?,?,?)",
            (extraction_id, run_id or None, mode, _maintenant(), ST_EN_COURS))
        conn.commit()
    finally:
        conn.close()
    return extraction_id


def cloturer(extraction_id: str, *, statut: str, message: str = "", db_path=None) -> dict[str, Any]:
    """Ferme l'extraction et fige ses compteurs, lus en base plutôt que reçus de l'appelant.

    Un compteur transmis par le code d'extraction dirait ce qu'il croit avoir écrit ; celui-ci dit ce
    qui est réellement en base. Quand les deux divergent, c'est le second qui compte.
    """
    conn = get_db(db_path)
    try:
        compte = {}
        for table, cle in (("hostaway_listings", "nb_listings"),
                           ("hostaway_reservations", "nb_reservations"),
                           ("hostaway_payouts", "nb_payouts")):
            compte[cle] = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE extraction_id = ?", (extraction_id,)
            ).fetchone()[0]
        conn.execute(
            "UPDATE hostaway_extractions SET date_fin = ?, statut = ?, message = ?, "
            "nb_listings = ?, nb_reservations = ?, nb_payouts = ? WHERE extraction_id = ?",
            (_maintenant(), statut, message or None, compte["nb_listings"],
             compte["nb_reservations"], compte["nb_payouts"], extraction_id))
        conn.commit()
    finally:
        conn.close()
    return {"extraction_id": extraction_id, "statut": statut, **compte}


# ── Écriture des lignes ─────────────────────────────────────────────────────────────────────────

_COLS_LISTING = ("listing_map_id", "listing_id_ha", "nom_listing", "internal_name", "ville",
                 "actif", "special_status", "airbnb_status", "bookingcom_status", "sur_hostaway",
                 "extrait_le", "row_hash")

# Correspondance depuis les noms de colonnes du moteur. Le moteur nomme en camelCase ce que la base
# nomme en snake_case ; traduire ici évite d'imposer l'un des deux styles à l'autre couche.
_ALIAS_LISTING = {"listing_map_id": "listingMapId", "internal_name": "internalName",
                  "row_hash": "ROW_HASH"}

_COLS_RESERVATION = (
    "reservation_id", "listing_map_id", "source", "channel_type", "source_financiere", "status",
    "payment_status", "check_in_date", "check_out_date", "nights", "number_of_guests",
    "guest_count", "source_guest_count", "controle_guest_count", "code_controle_guest_count",
    "total_price", "cleaning_fee_res", "channel_commission", "airbnb_expected_payout",
    "is_owner_stay", "inclure_resultat", "updated_on", "created_on", "extrait_le", "row_hash",
    "payload_json")
_ALIAS_RESERVATION = {
    "listing_map_id": "listingMapId", "payment_status": "paymentStatus",
    "check_in_date": "checkInDate", "check_out_date": "checkOutDate",
    "number_of_guests": "numberOfGuests", "guest_count": "guestCount",
    "source_guest_count": "source_guestCount", "controle_guest_count": "controle_guestCount",
    "code_controle_guest_count": "code_controle_guestCount", "total_price": "totalPrice",
    "channel_commission": "channelCommission", "airbnb_expected_payout": "airbnbExpectedPayout",
    "is_owner_stay": "is_ownerStay", "updated_on": "updatedOn", "created_on": "createdOn",
    "row_hash": "ROW_HASH", "payload_json": "json_snapshot",
}

_COLS_PAYOUT = (
    "reservation_id", "listing_map_id", "source", "channel_type", "statut_calcul_payout",
    "payout_calcule", "source_payout", "menage_retenu", "assiette_commission",
    "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
    "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
    "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
    "inclure_resultat_auto", "extrait_le", "row_hash")
_ALIAS_PAYOUT = {"listing_map_id": "listingMapId", "row_hash": "ROW_HASH"}

_COLS_FEE = ("reservation_id", "fee_id", "fee_name", "fee_type", "amount", "currency", "row_hash")
_ALIAS_FEE = {"row_hash": "ROW_HASH"}

_COLS_FF = ("reservation_id", "finance_field_name", "finance_field_value", "currency", "row_hash")
_ALIAS_FF = {"finance_field_name": "financeField_name",
             "finance_field_value": "financeField_value", "row_hash": "ROW_HASH"}

_COLS_ANOMALIE = ("reservation_id", "code_anomalie", "severite", "description", "statut",
                  "date_detection", "row_hash")
_ALIAS_ANOMALIE = {"row_hash": "ROW_HASH"}

# Colonnes numériques, par table. Le moteur produit parfois des chaînes ; les convertir à l'entrée
# évite qu'un tri ou une somme se fasse sur du texte plus loin.
_ENTIERS = {"nights", "number_of_guests", "guest_count"}
_REELS = {"total_price", "cleaning_fee_res", "channel_commission", "airbnb_expected_payout",
          "payout_calcule", "menage_retenu", "assiette_commission",
          "cout_standard_menage_snapshot", "amount"}


def _valeur(ligne: dict[str, Any], colonne: str, alias: dict[str, str]) -> Any:
    """Valeur d'une colonne, en acceptant le nom de base ou celui du moteur."""
    if colonne in ligne:
        brut = ligne[colonne]
    else:
        brut = ligne.get(alias.get(colonne, colonne))
    if colonne in _ENTIERS:
        return _entier(brut)
    if colonne in _REELS:
        return _reel(brut)
    if colonne == "payload_json" and brut is not None and not isinstance(brut, str):
        # Un dictionnaire de réponse est sérialisé ; une chaîne déjà sérialisée est laissée telle
        # quelle, pour ne pas la ré-encoder et rendre la preuve illisible.
        return json.dumps(brut, ensure_ascii=False)
    return brut


def _ecrire(conn, table: str, colonnes: tuple[str, ...], alias: dict[str, str],
            extraction_id: str, lignes: Iterable[dict[str, Any]], *, remplacer: bool) -> int:
    lignes = list(lignes)
    if not lignes:
        return 0
    verbe = "INSERT OR REPLACE" if remplacer else "INSERT"
    trous = ", ".join(["?"] * (len(colonnes) + 1))
    conn.executemany(
        f"{verbe} INTO {table} (extraction_id, {', '.join(colonnes)}) VALUES ({trous})",
        [(extraction_id, *(_valeur(l, c, alias) for c in colonnes)) for l in lignes])
    return len(lignes)


def enregistrer(extraction_id: str, *, listings: Iterable[dict] = (),
                reservations: Iterable[dict] = (), payouts: Iterable[dict] = (),
                fees: Iterable[dict] = (), finance_fields: Iterable[dict] = (),
                anomalies: Iterable[dict] = (), db_path=None) -> dict[str, int]:
    """Écrit les lignes d'une extraction. Une transaction : intégral ou inexistant.

    Un import à moitié écrit fausserait tous les agrégats calculés en aval, sans qu'aucune erreur ne
    subsiste pour l'expliquer.

    Les listings, réservations et payouts sont écrits en `INSERT OR REPLACE` : l'extraction peut
    procéder par pages et repasser sur une réservation. Les frais, champs financiers et anomalies
    sont multi-lignes par réservation et ne portent pas de clé unique : ils s'ajoutent.
    """
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            compte = {
                "listings": _ecrire(conn, "hostaway_listings", _COLS_LISTING, _ALIAS_LISTING,
                                    extraction_id, listings, remplacer=True),
                "reservations": _ecrire(conn, "hostaway_reservations", _COLS_RESERVATION,
                                        _ALIAS_RESERVATION, extraction_id, reservations,
                                        remplacer=True),
                "payouts": _ecrire(conn, "hostaway_payouts", _COLS_PAYOUT, _ALIAS_PAYOUT,
                                   extraction_id, payouts, remplacer=True),
                "fees": _ecrire(conn, "hostaway_reservation_fees", _COLS_FEE, _ALIAS_FEE,
                                extraction_id, fees, remplacer=False),
                "finance_fields": _ecrire(conn, "hostaway_reservation_finance_fields", _COLS_FF,
                                          _ALIAS_FF, extraction_id, finance_fields,
                                          remplacer=False),
                "anomalies": _ecrire(conn, "hostaway_anomalies", _COLS_ANOMALIE, _ALIAS_ANOMALIE,
                                     extraction_id, anomalies, remplacer=False),
            }
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
    return compte


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

_COLS_EXTRACTION = ("extraction_id", "run_id", "mode", "date_debut", "date_fin", "statut",
                    "nb_listings", "nb_reservations", "nb_payouts", "message")


def derniere_extraction(*, db_path=None) -> dict[str, Any] | None:
    """Extraction la plus récente, terminée ou non.

    Une extraction en cours ou partielle est rendue elle aussi : l'écran doit pouvoir dire que la
    donnée affichée est incomplète, ce qu'il ne pourrait pas faire si on ne montrait que les succès.
    """
    if not table_presente("hostaway_extractions", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_EXTRACTION)} FROM hostaway_extractions "
            "ORDER BY date_debut DESC, id DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_EXTRACTION, r)) if r else None


def derniere_extraction_utilisable(*, db_path=None) -> str:
    """Identifiant de la dernière extraction dont les données sont exploitables.

    Une extraction ÉCHOUÉE est écartée : ses lignes sont, par construction, un fragment. Une
    extraction PARTIELLE est retenue — ses données valent, elles sont seulement incomplètes, et
    l'appelant est censé le signaler.
    """
    if not table_presente("hostaway_extractions", db_path=db_path):
        return ""
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT extraction_id FROM hostaway_extractions WHERE statut IN (?, ?) "
            "ORDER BY date_debut DESC, id DESC LIMIT 1", (ST_SUCCES, ST_PARTIEL)).fetchone()
        return r[0] if r else ""
    finally:
        conn.close()


def extractions(*, limite: int = 20, db_path=None) -> list[dict[str, Any]]:
    if not table_presente("hostaway_extractions", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_EXTRACTION, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_EXTRACTION)} FROM hostaway_extractions "
            "ORDER BY date_debut DESC, id DESC LIMIT ?", (limite,))]
    finally:
        conn.close()


def _lire(table: str, colonnes: tuple[str, ...], extraction_id: str, *, db_path=None,
          ordre: str = "id") -> list[dict[str, Any]]:
    if not extraction_id or not table_presente(table, db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(colonnes, r)) for r in conn.execute(
            f"SELECT {', '.join(colonnes)} FROM {table} WHERE extraction_id = ? ORDER BY {ordre}",
            (extraction_id,))]
    finally:
        conn.close()


# ORDRE D'ARRIVÉE PARTOUT.
#
# Les lignes sont rendues dans l'ordre où l'extraction les a écrites (`id`), jamais triées par
# identifiant — pour que la comparaison ligne à ligne avec la baseline legacy reste valable. Ceci
# n'est plus une contrainte d'identité : `reservation_calc_id` dérive de `reservation_id` (ou
# `reservation_hh_id`), donc stable quel que soit l'ordre de lecture. Le moteur lit dans l'ordre
# d'insertion ; ce service fait de même, par cohérence de parité plutôt que par nécessité.


def reservations(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Réservations d'une extraction. Par défaut la dernière utilisable."""
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_reservations", _COLS_RESERVATION, eid, db_path=db_path)


def payouts(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_payouts", _COLS_PAYOUT, eid, db_path=db_path)


def listings(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_listings", _COLS_LISTING, eid, db_path=db_path)


def fees(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_reservation_fees", _COLS_FEE, eid, db_path=db_path)


def finance_fields(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_reservation_finance_fields", _COLS_FF, eid, db_path=db_path)


def anomalies(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    return _lire("hostaway_anomalies", _COLS_ANOMALIE, eid, db_path=db_path)


# Nombre de voyageurs dans un payload, y compris TRONQUÉ.
#
# Les payloads repris des masters sont coupés à 4 000 caractères — la limite d'une cellule Excel. Un
# `json.loads` échoue donc sur la plupart d'entre eux, et s'arrêter là ferait silencieusement perdre
# tout le repli historique. Le moteur (`lib_guestcount`) résout cela en cherchant la clé dans le
# texte ; on reprend sa stratégie plutôt qu'une autre, pour que les deux donnent le même nombre.
_RE_GUEST = re.compile(r'"(?:numberOfGuests|guestCount)"\s*:\s*(-?\d+(?:\.\d+)?)')


def guest_count_depuis_payload(brut: Any) -> int | None:
    """Nombre de voyageurs porté par un payload, ou None.

    Le JSON est essayé d'abord — il est plus sûr quand il est complet ; la recherche textuelle ne sert
    que de repli, jamais de méthode principale.
    """
    if not brut:
        return None
    texte = str(brut)
    try:
        charge = json.loads(texte)
    except (TypeError, ValueError):
        charge = None
    if isinstance(charge, dict):
        for cle in ("numberOfGuests", "guestCount"):
            valeur = _entier(charge.get(cle))
            if valeur is not None:
                return valeur
    trouve = _RE_GUEST.search(texte)
    return _entier(trouve.group(1)) if trouve else None


def guest_count_par_reservation(*, extraction_id: str = "", db_path=None) -> dict[str, int]:
    """{reservation_id: numberOfGuests} extrait des payloads bruts.

    C'est le REPLI HISTORIQUE de `guestCount` : quand la réservation courante ne porte pas de nombre
    de voyageurs exploitable, le payload conservé peut encore le dire. Ce repli a corrigé un défaut
    réel de comptage ; il ne doit pas disparaître avec le classeur qui le portait.
    """
    resultat: dict[str, int] = {}
    for r in reservations(extraction_id=extraction_id, db_path=db_path):
        valeur = guest_count_depuis_payload(r.get("payload_json"))
        if valeur is not None:
            resultat[_txt(r.get("reservation_id"))] = valeur
    return resultat


# ── Fraîcheur ───────────────────────────────────────────────────────────────────────────────────

def fraicheur(*, db_path=None) -> dict[str, Any]:
    """État de la donnée Hostaway, pour l'affichage.

    Ne se fie ni à un nom de fichier ni à une date de modification : les deux peuvent changer sans
    que la donnée ait bougé, et inversement. La source est le journal des extractions.
    """
    derniere = derniere_extraction(db_path=db_path)
    if derniere is None:
        return {"disponible": False, "code": E_AUCUNE_EXTRACTION,
                "message": MESSAGES[E_AUCUNE_EXTRACTION]}

    complet = derniere["statut"] == ST_SUCCES
    return {
        "disponible": derniere["statut"] in (ST_SUCCES, ST_PARTIEL),
        "complet": complet,
        "code": "" if complet else (E_EXTRACTION_PARTIELLE
                                    if derniere["statut"] == ST_PARTIEL else derniere["statut"]),
        "message": "" if complet else MESSAGES.get(E_EXTRACTION_PARTIELLE, ""),
        "extraction_id": derniere["extraction_id"],
        "run_id": derniere["run_id"],
        "mode": derniere["mode"],
        "statut": derniere["statut"],
        "date_debut": derniere["date_debut"],
        "date_fin": derniere["date_fin"],
        "nb_reservations": derniere["nb_reservations"],
        "nb_payouts": derniere["nb_payouts"],
        "nb_listings": derniere["nb_listings"],
    }

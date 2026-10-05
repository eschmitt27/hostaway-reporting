"""Flux financiers — ce que les quatre écrans Banque, Caisse, Charges et Rapprochement lisent.

UN SEUL ÉCRAN DE TRAVAIL, AUCUNE TABLE FUSIONNÉE. Ce module ne fait que PROJETER, pour l'écran,
des objets qui vivent chacun dans leur table canonique :

  · mouvements BANQUE  → `qonto_transactions_raw` (+ `qonto_transactions_statut_local`) — GET-only ;
  · mouvements CAISSE  → `operations_caisse` et `caisse_transferts_banque` ;
  · objets métier      → `charges`, `factures` (fournisseurs), `factures_proprietaires` (créances),
                         `reglements_fournisseurs`, `mouvements_tresorerie_proprietaires` ;
  · rapprochements     → `banque_rapprochements` (la vérité sur « ce mouvement est-il rapproché ») ;
  · comptabilisation   → `ecritures` (la vérité sur « ce mouvement est-il comptabilisé »).

Il n'écrit rien. Les décisions passent par `flux_lettrage_service`.

DEUX STATUTS, DEUX QUESTIONS — jamais confondus :
  · RAPPROCHEMENT : à quoi ce mouvement correspond-il ? (NON MATCHÉ, MATCHÉ, PARTIEL, RAPPROCHÉ)
  · COMPTABILITÉ  : est-il traduit en écriture ? (À QUALIFIER, À COMPTABILISER, COMPTABILISÉ, ERREUR)

Un mouvement réel de trésorerie n'est JAMAIS « hors comptabilité » : ce statut n'existe que pour
une charge saisie à part (`prise_en_compta = NON`).
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import date
from typing import Any

from app.db.connection import get_db

# ── Sources de mouvements ─────────────────────────────────────────────────────────────────────
BANQUE = "BANQUE"
CAISSE = "CAISSE"
SOURCES = (BANQUE, CAISSE)

# Sens normalisé : l'argent ENTRE (crédit banque, encaissement caisse) ou SORT.
ENTREE = "ENTREE"
SORTIE = "SORTIE"

# ── Objets rapprochables ──────────────────────────────────────────────────────────────────────
CHARGE = "CHARGE"
FACTURE_FOURNISSEUR = "FACTURE_FOURNISSEUR"
FACTURE_PROPRIETAIRE = "FACTURE_PROPRIETAIRE"
REGLEMENT_FOURNISSEUR = "REGLEMENT_FOURNISSEUR"
MOUVEMENT_PROPRIETAIRE = "MOUVEMENT_PROPRIETAIRE"
# Mission 37 — reversement Airbnb déclaré pour un propriétaire, qui attend son virement réel.
CREDIT_CLIENT = "CREDIT_CLIENT"
TYPES_OBJET = (CHARGE, FACTURE_FOURNISSEUR, FACTURE_PROPRIETAIRE, REGLEMENT_FOURNISSEUR,
               MOUVEMENT_PROPRIETAIRE, CREDIT_CLIENT)

LIBELLES_TYPE_OBJET = {
    CHARGE: "Charge",
    FACTURE_FOURNISSEUR: "Facture fournisseur",
    FACTURE_PROPRIETAIRE: "Facture propriétaire",
    REGLEMENT_FOURNISSEUR: "Règlement fournisseur",
    MOUVEMENT_PROPRIETAIRE: "Règlement propriétaire",
    CREDIT_CLIENT: "Reversement Airbnb à encaisser",
}

#: Type d'objet tel qu'il est écrit dans `banque_rapprochements` (vocabulaire existant réutilisé).
TYPE_RAPPROCHEMENT = {
    CHARGE: "CHARGE_FOURNISSEUR",
    FACTURE_FOURNISSEUR: "FACTURE_FOURNISSEUR",
    FACTURE_PROPRIETAIRE: "FACTURE_PROPRIETAIRE",
    REGLEMENT_FOURNISSEUR: "REGLEMENT_FOURNISSEUR",
    # Même nom que le moteur historique : `proprietaires_tresorerie_service.montant_rapproche` lit
    # ce type-là pour dire ce qui reste à rapprocher d'un règlement propriétaire.
    MOUVEMENT_PROPRIETAIRE: "REVERSEMENT_PROPRIETAIRE",
    # Vocabulaire existant : un versement de plateforme. Jamais une réservation.
    CREDIT_CLIENT: "PAYOUT_PLATEFORME",
}

# ── Statuts de rapprochement ──────────────────────────────────────────────────────────────────
NON_MATCHE = "NON_MATCHE"
MATCHE = "MATCHE"
PARTIEL = "PARTIEL"
RAPPROCHE = "RAPPROCHE"
ANOMALIE = "ANOMALIE"
NON_RAPPROCHABLE = "NON_RAPPROCHABLE"

# Couleur + symbole + mot : la couleur n'est jamais le seul porteur de l'information.
STATUTS_RAPPROCHEMENT = {
    NON_MATCHE: {"libelle": "Non matché", "badge": "neutral", "symbole": "○"},
    MATCHE: {"libelle": "Matché", "badge": "warning", "symbole": "◔"},
    PARTIEL: {"libelle": "Partiel", "badge": "info", "symbole": "◑"},
    RAPPROCHE: {"libelle": "Rapproché", "badge": "success", "symbole": "●"},
    ANOMALIE: {"libelle": "Anomalie", "badge": "error", "symbole": "!"},
    NON_RAPPROCHABLE: {"libelle": "Non rapprochable", "badge": "neutral", "symbole": "–"},
}

# Une charge se dit au féminin, et « matché » n'a pas de sens pour elle : elle est rapprochée ou non.
STATUTS_RAPPROCHEMENT_CHARGE = {
    NON_MATCHE: {"libelle": "Non rapprochée", "badge": "neutral", "symbole": "○"},
    PARTIEL: {"libelle": "Partiellement rapprochée", "badge": "info", "symbole": "◑"},
    RAPPROCHE: {"libelle": "Rapprochée", "badge": "success", "symbole": "●"},
    NON_RAPPROCHABLE: {"libelle": "Non rapprochable", "badge": "neutral", "symbole": "–"},
}

# ── Statuts comptables ────────────────────────────────────────────────────────────────────────
A_QUALIFIER = "A_QUALIFIER"
A_COMPTABILISER = "A_COMPTABILISER"
COMPTABILISE = "COMPTABILISE"
ERREUR = "ERREUR"
SANS_EFFET = "SANS_EFFET"          # opération refusée / contrepassée par la banque : rien n'a bougé

STATUTS_COMPTA_MOUVEMENT = {
    A_QUALIFIER: {"libelle": "À qualifier", "badge": "neutral", "symbole": "?"},
    A_COMPTABILISER: {"libelle": "À comptabiliser", "badge": "warning", "symbole": "◔"},
    COMPTABILISE: {"libelle": "Comptabilisé", "badge": "success", "symbole": "✓"},
    ERREUR: {"libelle": "Erreur", "badge": "error", "symbole": "!"},
    SANS_EFFET: {"libelle": "Sans effet", "badge": "neutral", "symbole": "–"},
}

# Charges : le vocabulaire du modèle existant (`statut_controle`, `prise_en_compta`).
CH_A_CONTROLER = "A_CONTROLER"
CH_COMPTABLE = "COMPTABLE"
CH_COMPTABILISEE = "COMPTABILISEE"
CH_HORS_COMPTA = "HORS_COMPTABILITE"

STATUTS_COMPTA_CHARGE = {
    CH_A_CONTROLER: {"libelle": "À contrôler", "badge": "warning", "symbole": "?"},
    CH_COMPTABLE: {"libelle": "Comptable", "badge": "info", "symbole": "◑"},
    CH_COMPTABILISEE: {"libelle": "Comptabilisée", "badge": "success", "symbole": "✓"},
    CH_HORS_COMPTA: {"libelle": "Hors comptabilité", "badge": "neutral", "symbole": "–"},
}

# Modes de paiement du référentiel (`ref_modes_paiement`) : banque pro / espèces de la caisse.
MODE_BANQUE = "PAY_001"
MODE_CAISSE = "PAY_002"
MODE_PAR_SOURCE = {BANQUE: MODE_BANQUE, CAISSE: MODE_CAISSE}

TRESORERIE_PAR_SOURCE = {BANQUE: "512000", CAISSE: "530000"}
JOURNAL_PAR_SOURCE = {BANQUE: "BANQUE", CAISSE: "CAISSE"}

EPS = 0.005


# ══ Utilitaires ═══════════════════════════════════════════════════════════════════════════════

def _r(v: Any) -> float:
    try:
        return round(float(v or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def date_fr(valeur: Any) -> str:
    """`2026-09-21…` → `21/09/2026`. Absente → tiret, jamais une date inventée."""
    texte = _txt(valeur)[:10]
    morceaux = texte.split("-")
    if len(morceaux) != 3:
        return texte or "—"
    return f"{morceaux[2]}/{morceaux[1]}/{morceaux[0]}"


def statut_info(table: dict, code: str) -> dict:
    info = table.get(code) or {"libelle": code, "badge": "neutral", "symbole": ""}
    return {"code": code, **info}


def jours_entre(a: Any, b: Any) -> int | None:
    try:
        return abs((date.fromisoformat(_txt(a)[:10]) - date.fromisoformat(_txt(b)[:10])).days)
    except ValueError:
        return None


# ══ Périodes clôturées ════════════════════════════════════════════════════════════════════════

def mois_cloture(mois: str, *, db_path=None) -> str:
    """Motif de clôture du mois, ou chaîne vide s'il est ouvert.

    Deux clôtures coexistent et les deux protègent : la clôture mensuelle métier
    (`ref_cloture_mensuelle`, celle que la saisie des charges respecte déjà) et la période
    comptable (`periodes_comptables`, celle que le moteur d'écritures respecte). Un mois fermé
    par l'une OU l'autre ne reçoit aucun nouveau rapprochement ni aucune écriture : on ne rouvre
    rien ici, le workflow de réouverture existant reste le seul chemin."""
    mois = _txt(mois)[:7]
    if len(mois) != 7:
        return ""
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        if "ref_cloture_mensuelle" in tables:
            r = conn.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois=?",
                             (mois,)).fetchone()
            if r and _txt(r["statut_mois"]).upper() == "CLOTURE":
                return f"Le mois {mois} est clôturé (clôture mensuelle)."
        if "periodes_comptables" in tables:
            r = conn.execute("SELECT statut FROM periodes_comptables WHERE periode=?",
                             (mois,)).fetchone()
            if r and _txt(r["statut"]).upper() == "CLOTUREE":
                return f"La période comptable {mois} est clôturée."
    finally:
        conn.close()
    # Troisième fermeture : le module « Banque et caisse » clôturé pour ce mois — plus de
    # rapprochement, de lettrage ni d'opération de caisse (cloture_verrous_service).
    from app.services import cloture_verrous_service as verrous
    return verrous.refus(mois, "BANQUE", db_path=db_path)


# ══ Noms humains ══════════════════════════════════════════════════════════════════════════════

def noms_tiers(*, db_path=None) -> dict[str, str]:
    """Nom lisible de tout tiers connu : propriétaire, fournisseur, prestataire, associé.

    Une table absente fait perdre un nom, jamais la page."""
    noms: dict[str, str] = {}
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        if "ref_proprietaires" in tables:
            for r in conn.execute("SELECT proprietaire_id, prenom_proprietaire, nom_proprietaire "
                                  "FROM ref_proprietaires"):
                noms[r["proprietaire_id"]] = " ".join(
                    x for x in (_txt(r["prenom_proprietaire"]), _txt(r["nom_proprietaire"])) if x)
        if "ref_intervenants" in tables:
            for r in conn.execute("SELECT intervenant_id, nom_intervenant, nom_legal, societe "
                                  "FROM ref_intervenants"):
                legal = _txt(r["nom_legal"])
                nom = _txt(r["nom_intervenant"])
                noms[r["intervenant_id"]] = f"{nom} ({legal})" if legal and legal != nom else nom
        if "fournisseurs" in tables:
            for r in conn.execute("SELECT fournisseur_id_opaque, nom FROM fournisseurs WHERE actif=1"):
                noms[r["fournisseur_id_opaque"]] = _txt(r["nom"])
        if "ref_associes" in tables:
            for r in conn.execute("SELECT personne_id, nom_personne FROM ref_associes"):
                noms[r["personne_id"]] = _txt(r["nom_personne"])
    finally:
        conn.close()
    return {k: v for k, v in noms.items() if v}


def fournisseurs_connus(*, db_path=None) -> list[dict[str, str]]:
    """Auxiliaires 401 possibles, par nom : référentiel fournisseurs + prestataires externes."""
    out: list[dict[str, str]] = []
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        if "fournisseurs" in tables:
            for r in conn.execute("SELECT fournisseur_id_opaque, nom FROM fournisseurs "
                                  "WHERE actif=1 AND statut='ACTIF' ORDER BY nom"):
                out.append({"id": r["fournisseur_id_opaque"], "nom": _txt(r["nom"])})
        if "ref_intervenants" in tables:
            for r in conn.execute("SELECT intervenant_id, nom_intervenant, nom_legal "
                                  "FROM ref_intervenants WHERE type_intervenant='EXTERNE' "
                                  "ORDER BY nom_intervenant"):
                legal = _txt(r["nom_legal"])
                nom = _txt(r["nom_intervenant"])
                out.append({"id": r["intervenant_id"],
                            "nom": f"{nom} ({legal})" if legal and legal != nom else nom})
    finally:
        conn.close()
    return out


def proprietaires_connus(*, db_path=None) -> list[dict[str, str]]:
    conn = get_db(db_path)
    try:
        if "ref_proprietaires" not in _tables(conn):
            return []
        rows = conn.execute("SELECT proprietaire_id, prenom_proprietaire, nom_proprietaire "
                            "FROM ref_proprietaires ORDER BY nom_proprietaire").fetchall()
    finally:
        conn.close()
    return [{"id": r["proprietaire_id"],
             "nom": " ".join(x for x in (_txt(r["prenom_proprietaire"]),
                                         _txt(r["nom_proprietaire"])) if x) or r["proprietaire_id"]}
            for r in rows]


def associes_connus(*, db_path=None) -> list[dict[str, str]]:
    conn = get_db(db_path)
    try:
        if "ref_associes" not in _tables(conn):
            return []
        rows = conn.execute("SELECT personne_id, nom_personne FROM ref_associes "
                            "WHERE actif='OUI' ORDER BY nom_personne").fetchall()
    finally:
        conn.close()
    return [{"id": r["personne_id"], "nom": _txt(r["nom_personne"])} for r in rows]


def comptes_actifs(*, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT compte, libelle, auxiliaire_autorise FROM plan_comptable "
                            "WHERE actif=1 ORDER BY compte").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ══ Liens de rapprochement et écritures (lecture) ═════════════════════════════════════════════

def _liens_actifs(conn) -> dict[str, list[dict]]:
    """Liens PROPOSE/CONFIRME par mouvement. La table a une colonne `lettrage_id_opaque` depuis
    la migration 0112 ; une base plus ancienne est lue sans elle."""
    colonnes = {r[1] for r in conn.execute("PRAGMA table_info(banque_rapprochements)")}
    champ_let = "lettrage_id_opaque" if "lettrage_id_opaque" in colonnes else "NULL"
    out: dict[str, list[dict]] = {}
    for r in conn.execute(
            f"SELECT rapprochement_id_opaque, mouvement_id_opaque, type_objet, objet_id, "
            f"montant_rapproche, statut, {champ_let} AS lettrage_id_opaque "
            f"FROM banque_rapprochements WHERE statut IN ('PROPOSE','CONFIRME')"):
        out.setdefault(r["mouvement_id_opaque"], []).append(dict(r))
    return out


def _ecritures_par_origine(conn) -> dict[tuple[str, str], str]:
    """(origine_type, origine_id) → statut de l'écriture vivante la plus pertinente."""
    out: dict[tuple[str, str], str] = {}
    if "ecritures" not in _tables(conn):
        return out
    for r in conn.execute("SELECT origine_type, origine_id_opaque, statut FROM ecritures "
                          "WHERE origine_id_opaque IS NOT NULL"):
        cle = (r["origine_type"], r["origine_id_opaque"])
        # Une écriture VALIDEE l'emporte ; une CONTREPASSEE ne compte que s'il n'y a rien d'autre.
        actuel = out.get(cle)
        if actuel == "VALIDEE":
            continue
        if r["statut"] == "VALIDEE" or actuel is None or actuel == "CONTREPASSEE":
            out[cle] = r["statut"]
    return out


def _objets_de_lettrage(conn) -> dict[str, list[str]]:
    """Libellés des objets réglés par chaque lettrage, figés au moment de la décision."""
    out: dict[str, list[str]] = {}
    if "flux_lettrage_lignes" not in _tables(conn):
        return out
    for r in conn.execute("SELECT lettrage_id_opaque, libelle FROM flux_lettrage_lignes "
                          "WHERE cote='OBJET' ORDER BY id"):
        out.setdefault(r["lettrage_id_opaque"], []).append(_txt(r["libelle"]))
    return out


LIBELLES_LIEN_HISTORIQUE = {
    "APPORT_ASSOCIE": "Apport compte courant",
    "TRANSFERT_CAISSE": "Transfert Banque → Caisse",
    "REGLEMENT_CHARGE": "Facture fournisseur",
    "REVERSEMENT_PROPRIETAIRE": "Règlement propriétaire",
    "CHARGE_FOURNISSEUR": "Charge",
    "RESERVATION": "Réservation",
}


def rattachements(liens: list[dict], objets_lettrage: dict, noms: dict) -> list[str]:
    """À quoi ce mouvement est rattaché, en mots : « Apport compte courant — Ewan »."""
    out: list[str] = []
    for l in liens:
        if l["statut"] != "CONFIRME":
            continue
        if l.get("lettrage_id_opaque"):
            out += objets_lettrage.get(l["lettrage_id_opaque"], [])
            continue
        nature = LIBELLES_LIEN_HISTORIQUE.get(l["type_objet"], l["type_objet"].replace("_", " ").capitalize())
        nom = noms.get(l.get("objet_id") or "", "")
        out.append(f"{nature} — {nom}" if nom else nature)
    return list(dict.fromkeys(x for x in out if x))


def _lettrages(conn) -> dict[str, dict]:
    if "flux_lettrages" not in _tables(conn):
        return {}
    return {r["lettrage_id_opaque"]: dict(r) for r in conn.execute("SELECT * FROM flux_lettrages")}


def _ecritures_de_lettrage(conn) -> dict[str, list[str]]:
    """Statuts des écritures produites par chaque lettrage (origine `LETTRAGE`)."""
    out: dict[str, list[str]] = {}
    if "ecritures" not in _tables(conn):
        return out
    for r in conn.execute("SELECT piece, statut FROM ecritures WHERE origine_type='LETTRAGE'"):
        out.setdefault(r["piece"], []).append(r["statut"])
    return out


def _lien_comptabilise(lien: dict, ecritures: dict, ecr_lettrage: dict, lettrages: dict) -> str:
    """VALIDEE | PROPOSEE | CONTREPASSEE | AUCUNE pour un lien CONFIRMÉ."""
    let = lien.get("lettrage_id_opaque")
    if let:
        statuts = ecr_lettrage.get(let, [])
        if lettrages.get(let, {}).get("statut") == "ANNULE":
            return "CONTREPASSEE"
        if statuts and all(s == "VALIDEE" for s in statuts):
            return "VALIDEE"
        if any(s == "PROPOSEE" for s in statuts):
            return "PROPOSEE"
        if any(s == "CONTREPASSEE" for s in statuts):
            return "CONTREPASSEE"
        return "AUCUNE"
    return ecritures.get(("RAPPROCHEMENT", lien["rapprochement_id_opaque"]), "AUCUNE")


def statuts_mouvement(montant: float, liens: list[dict], *, a_proposition: bool,
                      definitif: bool, sans_effet: bool, ecritures: dict, ecr_lettrage: dict,
                      lettrages: dict) -> tuple[str, str, dict]:
    """(statut rapprochement, statut comptable, détail) — la règle, en un seul endroit."""
    confirmes = [l for l in liens if l["statut"] == "CONFIRME"]
    proposes = [l for l in liens if l["statut"] == "PROPOSE"]
    total_confirme = _r(sum(l["montant_rapproche"] for l in confirmes))
    restant = _r(montant - total_confirme)
    detail = {"montant_rapproche": total_confirme, "restant": max(restant, 0.0),
              "nb_liens": len(confirmes)}

    if sans_effet:
        return NON_RAPPROCHABLE, SANS_EFFET, detail

    if total_confirme > montant + EPS:
        rappro = ANOMALIE
    elif confirmes and restant <= EPS:
        rappro = RAPPROCHE
    elif confirmes:
        rappro = PARTIEL
    elif a_proposition or proposes:
        rappro = MATCHE
    else:
        rappro = NON_MATCHE

    etats = [_lien_comptabilise(l, ecritures, ecr_lettrage, lettrages) for l in confirmes]
    if rappro == ANOMALIE or "CONTREPASSEE" in etats:
        compta = ERREUR
    elif any(e != "VALIDEE" for e in etats):
        compta = A_COMPTABILISER
    elif rappro == RAPPROCHE:
        compta = COMPTABILISE
    else:
        compta = A_QUALIFIER
    detail["definitif"] = definitif
    return rappro, compta, detail


# ══ Mouvements BANQUE (Qonto, lecture seule) ══════════════════════════════════════════════════

def id_mouvement_qonto(transaction_id: str) -> str:
    """Même dérivation que `qonto_validation_service.mouvement_opaque` : un seul identifiant."""
    empreinte = hashlib.sha256(f"QONTO|{transaction_id}".encode("utf-8")).hexdigest()
    return f"QMV-{empreinte[:12]}"


def _mouvements_banque_bruts(conn) -> list[dict]:
    if "qonto_transactions_raw" not in _tables(conn):
        return []
    lignes = []
    for r in conn.execute(
            "SELECT t.transaction_id, t.montant, t.devise, t.sens, t.statut, t.type_operation, "
            "       t.libelle, t.reference, t.note, t.contrepartie, t.emis_le, t.regle_le, "
            "       t.categorie, s.nature, s.statut_local, s.mouvement_id_opaque "
            "  FROM qonto_transactions_raw t "
            "  LEFT JOIN qonto_transactions_statut_local s "
            "    ON s.qonto_transaction_uuid = t.qonto_transaction_uuid "
            " ORDER BY COALESCE(t.regle_le, t.emis_le, t.cree_le) DESC"):
        d = dict(r)
        statut = _txt(d["statut"]).lower()
        mouvement_id = d["mouvement_id_opaque"] or id_mouvement_qonto(d["transaction_id"])
        libelle = _txt(d["contrepartie"]) or _txt(d["libelle"]) or "(sans libellé)"
        reference = ""
        for champ in ("note", "reference"):
            v = _txt(d.get(champ))
            if v and v.lower() != _txt(d["libelle"]).lower():
                reference = v
                break
        lignes.append({
            "source": BANQUE,
            "id": mouvement_id,
            "date": _txt(d["regle_le"] or d["emis_le"])[:10],
            "date_est_prevue": not d["regle_le"],
            "montant": _r(abs(float(d["montant"] or 0))),
            "devise": d["devise"] or "EUR",
            "sens": ENTREE if _txt(d["sens"]).lower() == "credit" else SORTIE,
            "libelle": libelle,
            "libelle_banque": _txt(d["libelle"]),
            "reference": reference,
            "texte_recherche": " ".join(_txt(d.get(c)) for c in
                                        ("libelle", "contrepartie", "reference", "note")),
            "definitif": statut == "completed",
            "sans_effet": statut in ("declined", "reversed") or _r(d["montant"]) == 0,
            "statut_banque": statut,
            "nature": d["nature"] or "",
            "type_operation": _txt(d["type_operation"]).lower(),
        })
    return lignes


# ══ Mouvements CAISSE ═════════════════════════════════════════════════════════════════════════

LIBELLES_OPERATION_CAISSE = {
    "ENCAISSEMENT": "Encaissement en espèces",
    "REMBOURSEMENT_ASSOCIE": "Remboursement d'associé en espèces",
    "AUTRE": "Dépense en espèces",
}


def _mouvements_caisse_bruts(conn) -> list[dict]:
    tables = _tables(conn)
    lignes: list[dict] = []
    noms = {}
    if "ref_associes" in tables:
        noms.update({r[0]: r[1] for r in conn.execute(
            "SELECT personne_id, nom_personne FROM ref_associes")})
    if "operations_caisse" in tables:
        for r in conn.execute("SELECT * FROM operations_caisse ORDER BY date_operation DESC, id DESC"):
            op = dict(r)
            type_op = op["type_operation"]
            tiers = noms.get(op.get("tiers_id") or "", "") or ""
            lignes.append({
                "source": CAISSE,
                "id": op["operation_id_opaque"],
                "nature_caisse": "OPERATION",
                "type_operation": type_op,
                "date": _txt(op["date_operation"])[:10],
                "date_est_prevue": False,
                "montant": _r(op["montant"]),
                "devise": "EUR",
                "sens": ENTREE if type_op == "ENCAISSEMENT" else SORTIE,
                "libelle": LIBELLES_OPERATION_CAISSE.get(type_op, type_op),
                "reference": " · ".join(x for x in (_txt(op.get("piece")), tiers,
                                                     _txt(op.get("commentaire"))) if x),
                "texte_recherche": " ".join(_txt(op.get(c)) for c in ("piece", "commentaire")),
                "definitif": True,
                "sans_effet": op["statut"] == "ANNULEE",
                "statut_operation": op["statut"],
                "tiers_type": op.get("tiers_type") or "",
                "tiers_id": op.get("tiers_id") or "",
            })
    if "caisse_transferts_banque" in tables and "qonto_transactions_raw" in tables:
        for r in conn.execute(
                "SELECT c.*, t.transaction_id FROM caisse_transferts_banque c "
                "JOIN qonto_transactions_raw t ON t.qonto_transaction_uuid = c.qonto_transaction_uuid "
                "ORDER BY c.date_operation DESC"):
            tr = dict(r)
            lignes.append({
                "source": CAISSE,
                "id": tr["transfert_id"],
                "nature_caisse": "TRANSFERT",
                "type_operation": "TRANSFERT_BANQUE",
                "date": _txt(tr["date_operation"])[:10],
                "date_est_prevue": tr["etat"] != "CONFIRME",
                "montant": _r(tr["montant"]),
                "devise": tr["devise"] or "EUR",
                "sens": ENTREE,
                "libelle": "Retrait d'espèces au distributeur",
                "reference": f"Retrait bancaire Qonto — {_txt(tr.get('libelle_source'))}".strip(" —"),
                "texte_recherche": _txt(tr.get("libelle_source")),
                "definitif": tr["etat"] == "CONFIRME",
                "sans_effet": tr["etat"] == "ANNULE",
                "etat_transfert": tr["etat"],
                "mouvement_banque": id_mouvement_qonto(tr["transaction_id"]),
            })
    lignes.sort(key=lambda m: m["date"], reverse=True)
    return lignes


# ══ Projection complète ═══════════════════════════════════════════════════════════════════════

def mouvements(*, source: str = "", avec_propositions: bool = True, db_path=None) -> list[dict]:
    """Tous les mouvements de la source (ou des deux), avec leurs deux statuts."""
    conn = get_db(db_path)
    try:
        liens = _liens_actifs(conn)
        ecritures = _ecritures_par_origine(conn)
        ecr_lettrage = _ecritures_de_lettrage(conn)
        lettrages = _lettrages(conn)
        objets_lettrage = _objets_de_lettrage(conn)
        bruts: list[dict] = []
        if source in ("", BANQUE):
            bruts += _mouvements_banque_bruts(conn)
        if source in ("", CAISSE):
            bruts += _mouvements_caisse_bruts(conn)
    finally:
        conn.close()

    avec_prop: set[str] = set()
    if avec_propositions:
        from app.services import flux_matching_service as matching
        for p in matching.propositions(db_path=db_path):
            for m in p["mouvements"]:
                avec_prop.add(m["id"])

    noms = noms_tiers(db_path=db_path)
    out = []
    for m in bruts:
        m["rattachements"] = rattachements(liens.get(m["id"], []), objets_lettrage, noms)
        if m["source"] == CAISSE and m.get("nature_caisse") == "TRANSFERT":
            m.update(_statuts_transfert(m, liens, ecritures, ecr_lettrage, lettrages))
        elif m["source"] == CAISSE:
            m.update(_statuts_operation_caisse(m, liens, ecritures, ecr_lettrage, lettrages,
                                               m["id"] in avec_prop))
        else:
            rappro, compta, detail = statuts_mouvement(
                m["montant"], liens.get(m["id"], []), a_proposition=m["id"] in avec_prop,
                definitif=m["definitif"], sans_effet=m["sans_effet"], ecritures=ecritures,
                ecr_lettrage=ecr_lettrage, lettrages=lettrages)
            m.update(detail)
            # Retrait d'espèces réglé : sa nature est certaine (Qonto le classe « atm »), il ne
            # reste qu'à passer le transfert 530 / 512 — même lecture que côté Caisse.
            if (m.get("nature") == "RETRAIT_ESPECES" and rappro == NON_MATCHE
                    and m["definitif"] and not m["sans_effet"]):
                rappro, compta = MATCHE, A_COMPTABILISER
            m["statut_rapprochement"] = rappro
            m["statut_compta"] = compta
        m["rapprochement"] = statut_info(STATUTS_RAPPROCHEMENT, m["statut_rapprochement"])
        m["compta"] = statut_info(STATUTS_COMPTA_MOUVEMENT, m["statut_compta"])
        m["date_fr"] = date_fr(m["date"])
        m["mois"] = m["date"][:7]
        # Un retrait d'espèces n'est ni une charge ni un produit : il a son propre circuit
        # (transfert Banque → Caisse, 530 / 512). Il ne se rapproche donc d'aucun objet.
        transfert = m.get("nature") == "RETRAIT_ESPECES" or m.get("nature_caisse") == "TRANSFERT"
        m["parcours_dedie"] = ("RETRAIT" if transfert else
                               "APPORT" if m.get("nature") == "APPORT_ASSOCIE" else "")
        m["lettrable"] = (m["definitif"] and not m["sans_effet"] and not transfert
                          and m.get("restant", 0) > EPS
                          and m["statut_rapprochement"] not in (RAPPROCHE, ANOMALIE))
        # « À traiter » : tout ce qui attend une décision humaine. Une opération encore en attente
        # chez la banque n'en fait pas partie — il n'y a rien à décider tant qu'elle hésite.
        m["a_traiter"] = (m["definitif"] and not m["sans_effet"]
                          and m["statut_compta"] in (A_QUALIFIER, A_COMPTABILISER, ERREUR))
        out.append(m)
    return out


def _statuts_transfert(m, liens, ecritures, ecr_lettrage, lettrages) -> dict:
    """Retrait d'espèces : son rapprochement est celui du mouvement bancaire d'origine."""
    lien = next((l for l in liens.get(m["mouvement_banque"], [])
                 if l["type_objet"] == "TRANSFERT_CAISSE" and l["statut"] == "CONFIRME"), None)
    if m["sans_effet"]:
        return {"statut_rapprochement": NON_RAPPROCHABLE, "statut_compta": SANS_EFFET,
                "restant": 0.0, "montant_rapproche": 0.0}
    if lien is None:
        return {"statut_rapprochement": MATCHE if m["definitif"] else NON_MATCHE,
                "statut_compta": A_COMPTABILISER if m["definitif"] else A_QUALIFIER,
                "restant": m["montant"], "montant_rapproche": 0.0}
    etat = _lien_comptabilise(lien, ecritures, ecr_lettrage, lettrages)
    return {"statut_rapprochement": RAPPROCHE,
            "statut_compta": (COMPTABILISE if etat == "VALIDEE" else
                              ERREUR if etat == "CONTREPASSEE" else A_COMPTABILISER),
            "restant": 0.0, "montant_rapproche": m["montant"]}


def _statuts_operation_caisse(m, liens, ecritures, ecr_lettrage, lettrages,
                              a_proposition: bool) -> dict:
    statut = m["statut_operation"]
    if m["sans_effet"]:
        return {"statut_rapprochement": NON_RAPPROCHABLE, "statut_compta": SANS_EFFET,
                "restant": 0.0, "montant_rapproche": 0.0}
    liens_op = liens.get(m["id"], [])
    if liens_op:
        rappro, compta, detail = statuts_mouvement(
            m["montant"], liens_op, a_proposition=a_proposition, definitif=True,
            sans_effet=False, ecritures=ecritures, ecr_lettrage=ecr_lettrage,
            lettrages=lettrages)
        return {"statut_rapprochement": rappro, "statut_compta": compta, **detail}
    if statut in ("VALIDE", "ENREGISTREE", "CONTREPASSEE"):
        # Qualifiée par son propre type et son tiers, comptabilisée par sa validation (§72).
        ecr = ecritures.get(("OPERATION_CAISSE", m["id"]))
        compta = (COMPTABILISE if ecr in ("VALIDEE", "PROPOSEE", "CONTREPASSEE")
                  else A_COMPTABILISER)
        return {"statut_rapprochement": RAPPROCHE, "statut_compta": compta,
                "restant": 0.0, "montant_rapproche": m["montant"]}
    # Brouillon : l'argent a bougé, sa nature n'est pas encore arrêtée.
    qualifiee = m["type_operation"] in ("ENCAISSEMENT", "REMBOURSEMENT_ASSOCIE") and m["tiers_id"]
    return {"statut_rapprochement": MATCHE if a_proposition else NON_MATCHE,
            "statut_compta": A_COMPTABILISER if qualifiee else A_QUALIFIER,
            "restant": m["montant"], "montant_rapproche": 0.0}


def mouvement(source: str, identifiant: str, *, db_path=None) -> dict | None:
    for m in mouvements(source=source, db_path=db_path):
        if m["id"] == identifiant:
            return m
    return None


# ══ Objets rapprochables (colonne de droite) ══════════════════════════════════════════════════

def _montants_lettres(conn) -> dict[tuple[str, str], float]:
    """Part déjà engagée de chaque objet dans un lettrage VALIDE."""
    out: dict[tuple[str, str], float] = {}
    if "flux_lettrage_lignes" not in _tables(conn):
        return out
    for r in conn.execute(
            "SELECT l.type_element, l.element_id, SUM(l.montant) AS total "
            "FROM flux_lettrage_lignes l JOIN flux_lettrages t "
            "  ON t.lettrage_id_opaque = l.lettrage_id_opaque "
            "WHERE t.statut='VALIDE' AND l.cote='OBJET' GROUP BY l.type_element, l.element_id"):
        out[(r["type_element"], r["element_id"])] = _r(r["total"])
    return out


def _charges_couvertes_par_facture(conn) -> set[str]:
    """Charges déjà portées par une facture fournisseur : c'est la facture qui se règle, jamais
    la charge une seconde fois (anti double comptabilisation)."""
    out: set[str] = set()
    tables = _tables(conn)
    if "factures" in tables:
        out |= {r[0] for r in conn.execute(
            "SELECT charge_id FROM factures WHERE charge_id IS NOT NULL AND statut <> 'ANNULEE'")}
    if "facture_lignes" in tables:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(facture_lignes)")}
        if "charge_id" in cols:
            out |= {r[0] for r in conn.execute(
                "SELECT l.charge_id FROM facture_lignes l JOIN factures f "
                "  ON f.facture_id_opaque = l.facture_id_opaque "
                "WHERE l.charge_id IS NOT NULL AND f.statut <> 'ANNULEE'")}
    return {c for c in out if c}


def _ecriture_existe(conn, origine_type: str, origine_id: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM ecritures WHERE origine_type=? AND origine_id_opaque=? "
        "AND statut <> 'CONTREPASSEE'", (origine_type, origine_id)).fetchone())


def objets(*, db_path=None, inclure_non_rapprochables: bool = False) -> list[dict]:
    """Objets métier encore ouverts, avec le montant restant à rapprocher.

    Chaque objet porte `sens` (ENTREE = argent attendu, SORTIE = argent à payer) et `sources`
    (Banque, Caisse ou les deux) : un débit bancaire ne peut viser qu'un objet SORTIE payable par
    la banque. Le mauvais sens n'est jamais « moins probable » : il est exclu."""
    noms = noms_tiers(db_path=db_path)
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        lettres = _montants_lettres(conn)
        couvertes = _charges_couvertes_par_facture(conn)
        out: list[dict] = []

        # ── Charges ──────────────────────────────────────────────────────────────────────────
        if "charges" in tables:
            for r in conn.execute("SELECT * FROM charges WHERE statut='ACTIVE'"):
                c = dict(r)
                montant = _r(abs(c["montant"] or 0))
                if montant <= EPS:
                    continue
                hors_compta = _txt(c["prise_en_compta"]).upper() == "NON"
                mode = _txt(c["mode_paiement_id"])
                reste = _r(montant - lettres.get((CHARGE, c["charge_id"]), 0))
                raison = ""
                if hors_compta:
                    raison = "Charge hors comptabilité : elle ne peut pas être liée à un mouvement réel de la société."
                elif c["charge_id"] in couvertes:
                    raison = "Charge portée par une facture fournisseur : c'est la facture qui se règle."
                elif mode not in (MODE_BANQUE, MODE_CAISSE):
                    raison = "Payée hors banque et hors caisse de la société (mode de paiement)."
                if reste <= EPS:
                    continue
                if raison and not inclure_non_rapprochables:
                    continue
                from app.services import referentiel_service as ref
                libelle_cat = (ref.libelle_categorie_charge(c["categorie_charge_id"],
                                                            db_path=db_path)
                               if c["categorie_charge_id"] else "Charge")
                out.append({
                    "type": CHARGE, "id": c["charge_id"],
                    "libelle": libelle_cat,
                    "detail": " · ".join(x for x in (
                        ref.nom_logement(c["logement_id"], db_path=db_path)
                        if c["logement_id"] else "",
                        _txt(c["commentaire"])) if x),
                    "tiers_id": "", "tiers": "",
                    "numero": _txt(c["justificatif"]),
                    "date": _txt(c["date_charge"])[:10],
                    "montant": montant, "reste": reste,
                    "sens": SORTIE,
                    "sources": ((BANQUE,) if mode == MODE_BANQUE else
                                (CAISSE,) if mode == MODE_CAISSE else ()),
                    "lien_mouvement": _txt(c["lien_virement_banque"]),
                    "non_rapprochable": raison,
                    "statut_controle": _txt(c["statut_controle"]) or "A_CONTROLER",
                })

        # ── Factures fournisseurs (dette 401 déjà constatée à la validation) ──────────────────
        if "factures" in tables:
            from app.services import factures_service as fs
            for r in conn.execute("SELECT * FROM factures WHERE statut IN "
                                  "('VALIDEE','PARTIELLEMENT_REGLEE','A_CONTROLER','LITIGE')"):
                f = dict(r)
                solde = fs.solde(f["facture_id_opaque"], conn=conn)["solde_restant"]
                if solde <= EPS:
                    continue
                raison = ""
                if f["statut"] in ("A_CONTROLER", "LITIGE"):
                    raison = ("Facture encore à contrôler : elle ne peut pas être réglée."
                              if f["statut"] == "A_CONTROLER" else "Facture en litige.")
                elif not _ecriture_existe(conn, "FACTURE", f["facture_id_opaque"]):
                    raison = ("L'écriture d'achat de cette facture n'existe pas : le règlement "
                              "n'aurait pas de dette à éteindre.")
                if raison and not inclure_non_rapprochables:
                    continue
                out.append({
                    "type": FACTURE_FOURNISSEUR, "id": f["facture_id_opaque"],
                    "libelle": f"Facture {f['facture_ref']}",
                    "detail": "", "tiers_id": f["fournisseur_id_opaque"],
                    "tiers": noms.get(f["fournisseur_id_opaque"], ""),
                    "numero": _txt(f["facture_ref"]),
                    "date": _txt(f["date_facture"])[:10],
                    "montant": _r(f["montant_ttc"]), "reste": _r(solde),
                    "sens": SORTIE, "sources": (BANQUE, CAISSE),
                    "lien_mouvement": "", "non_rapprochable": raison,
                })

        # ── Règlements fournisseurs déjà saisis, pas encore rapprochés ────────────────────────
        if "reglements_fournisseurs" in tables:
            for r in conn.execute("SELECT * FROM reglements_fournisseurs "
                                  "WHERE statut='ENREGISTRE' AND moyen='BANQUE'"):
                g = dict(r)
                if _ecriture_existe(conn, "REGLEMENT", g["reglement_id_opaque"]):
                    continue
                reste = _r(g["montant"] - lettres.get((REGLEMENT_FOURNISSEUR,
                                                       g["reglement_id_opaque"]), 0))
                if reste <= EPS:
                    continue
                out.append({
                    "type": REGLEMENT_FOURNISSEUR, "id": g["reglement_id_opaque"],
                    "libelle": "Règlement fournisseur enregistré",
                    "detail": _txt(g["commentaire"]),
                    "tiers_id": g["fournisseur_id_opaque"],
                    "tiers": noms.get(g["fournisseur_id_opaque"], ""),
                    "numero": "", "date": _txt(g["date_reglement"])[:10],
                    "montant": _r(g["montant"]), "reste": reste,
                    "sens": SORTIE, "sources": (BANQUE,),
                    "lien_mouvement": "", "non_rapprochable": "",
                })

        # ── Règlements propriétaires validés (encaissements attendus / reversements) ─────────
        if "mouvements_tresorerie_proprietaires" in tables:
            deja = {}
            for r in conn.execute(
                    "SELECT objet_id, SUM(montant_rapproche) AS t FROM banque_rapprochements "
                    "WHERE type_objet='REVERSEMENT_PROPRIETAIRE' AND statut IN ('PROPOSE','CONFIRME') "
                    "GROUP BY objet_id"):
                deja[r["objet_id"]] = _r(r["t"])
            for r in conn.execute("SELECT * FROM mouvements_tresorerie_proprietaires "
                                  "WHERE statut='VALIDE'"):
                mt = dict(r)
                # Un encaissement créé PAR un lettrage est déjà rapproché de son mouvement.
                if _txt(mt.get("source_type")) == "FLUX_LETTRAGE":
                    continue
                reste = _r(abs(mt["montant"]) - deja.get(mt["mouvement_opaque"], 0))
                if reste <= EPS:
                    continue
                entree = mt["sens"] == "PROPRIETAIRE_VERS_SOCIETE"
                acompte = entree and _txt(mt.get("nature")) == "ACOMPTE_PROPRIETAIRE"
                out.append({
                    "type": MOUVEMENT_PROPRIETAIRE, "id": mt["mouvement_opaque"],
                    "libelle": ("Acompte reçu d'un propriétaire" if acompte
                                else "Règlement reçu d'un propriétaire" if entree
                                else "Reversement à un propriétaire"),
                    "nature": _txt(mt.get("nature")),
                    "detail": _txt(mt.get("reference_metier")),
                    "tiers_id": mt["proprietaire_id"],
                    "tiers": noms.get(mt["proprietaire_id"], ""),
                    "numero": _txt(mt.get("reference_metier")),
                    "date": _txt(mt["date_mouvement"])[:10],
                    "montant": _r(abs(mt["montant"])), "reste": reste,
                    "sens": ENTREE if entree else SORTIE, "sources": (BANQUE, CAISSE),
                    "lien_mouvement": "", "non_rapprochable": "",
                })
        # ── Reversements Airbnb déclarés, en attente de leur virement (Mission 37) ───────────
        if "credits_clients" in tables:
            for r in conn.execute("SELECT * FROM credits_clients WHERE statut='EN_ATTENTE_ORIGINE' "
                                  "AND mode_origine='BANQUE'"):
                cr = dict(r)
                reste = _r(cr["montant_initial"] - lettres.get((CREDIT_CLIENT, cr["credit_id_opaque"]), 0))
                if reste <= EPS:
                    continue
                out.append({
                    "type": CREDIT_CLIENT, "id": cr["credit_id_opaque"],
                    "libelle": "Reversement Airbnb",
                    "detail": _txt(cr.get("reference")),
                    "tiers_id": cr["proprietaire_id"], "tiers": noms.get(cr["proprietaire_id"], ""),
                    "numero": _txt(cr.get("reference")),
                    "date": _txt(cr["date_origine"])[:10],
                    "montant": _r(cr["montant_initial"]), "reste": reste,
                    "sens": ENTREE, "sources": (BANQUE,),
                    "lien_mouvement": "", "non_rapprochable": "",
                })
    finally:
        conn.close()

    # ── Créances propriétaires (factures ÉMISES, vente 411/706 déjà constatée) ────────────────
    out += _creances_ouvertes(noms, lettres, inclure_non_rapprochables, db_path=db_path)
    for o in out:
        o["type_libelle"] = LIBELLES_TYPE_OBJET[o["type"]]
        o["date_fr"] = date_fr(o["date"])
        o["cle"] = f"{o['type']}:{o['id']}"
    out.sort(key=lambda o: (o["date"] or "9999", o["type"], o["id"]), reverse=True)
    return out


def _creances_ouvertes(noms: dict, lettres: dict, inclure: bool, *, db_path=None) -> list[dict]:
    try:
        from app.services import creances_dettes_service as cd
        creances = cd.creances(db_path=db_path)
    except Exception:      # noqa: BLE001 — un module amont indisponible ne casse pas l'écran
        return []
    conn = get_db(db_path)
    try:
        out = []
        for c in creances:
            if _r(c.get("solde")) <= EPS:
                continue
            fid = c["facture_id_opaque"]
            reste = _r(c["solde"])
            # Ce qu'un lettrage validé a déjà encaissé est imputé par l'allocation FIFO : le
            # solde ci-dessus en tient compte. On ne le retranche pas une seconde fois.
            raison = ""
            if not _ecriture_existe(conn, "FACTURE_PROPRIETAIRE", fid):
                raison = ("La vente de cette facture n'est pas comptabilisée : l'encaissement "
                          "n'aurait pas de créance à éteindre.")
            if raison and not inclure:
                continue
            out.append({
                "type": FACTURE_PROPRIETAIRE, "id": fid,
                "libelle": f"Facture {c.get('numero') or ''}".strip(),
                "detail": c.get("mois") or "",
                "tiers_id": c.get("tiers_id") or "",
                "tiers": noms.get(c.get("tiers_id") or "", ""),
                "numero": _txt(c.get("numero")),
                "date": _txt(c.get("date_facture"))[:10],
                "montant": _r(c.get("total")), "reste": reste,
                "sens": ENTREE, "sources": (BANQUE, CAISSE),
                "lien_mouvement": "", "non_rapprochable": raison,
            })
        return out
    finally:
        conn.close()


def objet(type_objet: str, identifiant: str, *, db_path=None) -> dict | None:
    for o in objets(db_path=db_path, inclure_non_rapprochables=True):
        if o["type"] == type_objet and o["id"] == identifiant:
            return o
    return None


# ══ Charges : statuts pour la page Charges ════════════════════════════════════════════════════

def statuts_charges(*, db_path=None) -> dict[str, dict]:
    """charge_id → {rapprochement, compta, origine, resultat} pour l'écran Charges."""
    conn = get_db(db_path)
    try:
        tables = _tables(conn)
        if "charges" not in tables:
            return {}
        lettres = _montants_lettres(conn)
        couvertes = _charges_couvertes_par_facture(conn)
        charges = [dict(r) for r in conn.execute("SELECT * FROM charges")]
        factures_ecrites = set()
        if "factures" in tables and "ecritures" in tables:
            for r in conn.execute(
                    "SELECT f.charge_id FROM factures f JOIN ecritures e "
                    "  ON e.origine_type='FACTURE' AND e.origine_id_opaque=f.facture_id_opaque "
                    "WHERE e.statut <> 'CONTREPASSEE' AND f.charge_id IS NOT NULL"):
                factures_ecrites.add(r[0])
    finally:
        conn.close()

    from app.moteurs.charges_engine import impact_charge
    out: dict[str, dict] = {}
    for c in charges:
        cid = c["charge_id"]
        montant = _r(abs(c["montant"] or 0))
        hors = _txt(c["prise_en_compta"]).upper() == "NON"
        lettre = lettres.get((CHARGE, cid), 0.0)
        if hors:
            rappro = NON_RAPPROCHABLE
            compta = CH_HORS_COMPTA
        else:
            if cid in couvertes:
                rappro = RAPPROCHE if cid in factures_ecrites else NON_MATCHE
            elif lettre >= montant - EPS and montant > 0:
                rappro = RAPPROCHE
            elif lettre > EPS:
                rappro = PARTIEL
            else:
                rappro = NON_MATCHE
            if lettre > EPS or cid in factures_ecrites:
                compta = CH_COMPTABILISEE
            elif _txt(c["statut_controle"]).upper() == "VALIDE":
                compta = CH_COMPTABLE
            else:
                compta = CH_A_CONTROLER
        lien = _txt(c["lien_virement_banque"])
        origine = ("Banque" if lien.startswith("QMV-") else
                   "Caisse" if lien.startswith(("CAI-", "TRF-")) else
                   "Facture fournisseur" if cid in couvertes else "Saisie")
        impact = impact_charge(c.get("code_impact")) or {}
        out[cid] = {
            "rapprochement": statut_info(STATUTS_RAPPROCHEMENT_CHARGE, rappro),
            "compta": statut_info(STATUTS_COMPTA_CHARGE, compta),
            "origine": origine,
            "lien_mouvement": lien,
            "resultat": ("Résultat réel et comptable" if impact.get("impact_resultat_comptable") == "OUI"
                         else "Résultat réel seulement" if impact.get("impact_resultat_reel") == "OUI"
                         else "—"),
            "montant_lettre": lettre,
        }
    return out


def resume_charge(charge_id: str, *, db_path=None) -> str:
    """« Rapprochée · Comptabilisée », « Non rapprochée · Comptabilisée », « Hors comptabilité »."""
    s = statuts_charges(db_path=db_path).get(charge_id)
    if not s:
        return ""
    if s["compta"]["code"] == CH_HORS_COMPTA:
        return "Hors comptabilité"
    return f"{s['rapprochement']['libelle']} · {s['compta']['libelle']}"


# ══ Soldes (en-tête des pages) ════════════════════════════════════════════════════════════════

def soldes(*, db_path=None) -> dict:
    try:
        from app.services import qonto_ecran_service as ecran
        return ecran.soldes(db_path=db_path)
    except (sqlite3.Error, Exception):      # noqa: BLE001
        return {"banque": 0.0, "banque_comptable": 0.0, "engage": 0.0, "caisse": 0.0,
                "tresorerie": 0.0, "devise": "EUR"}

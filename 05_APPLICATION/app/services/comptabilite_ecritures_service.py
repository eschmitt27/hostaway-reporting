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

import sqlite3
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
COMPTE_PROPRIETAIRES = "411000"
COMPTE_BANQUE = "512000"
COMPTE_CAISSE = "530000"
COMPTE_ASSOCIES = "467000"
COMPTE_ACHAT_GENERIQUE = "606000"
COMPTE_VENTE_GENERIQUE = "706000"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_INTROUVABLE = "E_ECRITURE_INTROUVABLE"
E_DEJA_GENEREE = "E_DEJA_GENEREE"
E_DESEQUILIBRE = "E_ECRITURE_DESEQUILIBREE"
E_COMPTE_INCONNU = "E_COMPTE_INCONNU"
E_ORIGINE_INVALIDE = "E_ORIGINE_INVALIDE"
E_STATUT = "E_TRANSITION_INTERDITE"
E_PERIODE_CLOTUREE = "E_PERIODE_CLOTUREE"

MESSAGES = {
    E_FLAGS: "Écriture comptable désactivée sur cette installation.",
    E_INTROUVABLE: "Écriture introuvable.",
    E_DEJA_GENEREE: "Une écriture existe déjà pour cette origine sur ce journal.",
    E_DESEQUILIBRE: "Écriture déséquilibrée : le total débit doit égaler le total crédit.",
    E_COMPTE_INCONNU: "Compte inconnu ou inactif dans le plan comptable.",
    E_ORIGINE_INVALIDE: "Origine de l'écriture invalide pour ce journal.",
    E_STATUT: "Transition de statut interdite.",
    E_PERIODE_CLOTUREE: "Cette période comptable est clôturée : aucune écriture directe n'est autorisée.",
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

    from app.services import comptabilite_periodes_service as per
    if per.est_fermee(periode, db_path):
        return _refus(E_PERIODE_CLOTUREE, periode)

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


def _menage_dimensions(charge_id: str, db_path=None) -> tuple[str | None, str | None]:
    """Case E — répartition Ménages : un ménage porte NOT NULL logement_id/proprietaire_id et,
    s'il est lié à cette charge, fournit la dimension quand la charge elle-même n'en porte aucune."""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT logement_id, proprietaire_id FROM menages WHERE charge_id=? AND statut <> 'ANNULE'",
            (charge_id,)).fetchone()
    finally:
        conn.close()
    return (row["logement_id"], row["proprietaire_id"]) if row else (None, None)


def _ligne_source_depuis_charge(charge_id: str, montant: float, *, origine_type: str,
                                origine_id: str, logement_id_hint: str | None = None,
                                db_path=None) -> dict[str, Any]:
    """Résout compte + dimensions pour UNE charge (cas mono-charge historique, ou une ligne d'une
    facture multi-lignes) — cases A/B (affectation directe), E (ménage) et F (A_CONTROLER).

    `logement_id_hint` : logement saisi explicitement sur la ligne de facture (`facture_lignes`,
    case D) — prioritaire sur celui de la charge elle-même, car c'est une précision humaine
    explicite au moment de la facturation, pas une donnée dérivée."""
    from app.readers import charges_reader

    charge = charges_reader.find_charge(charge_id)
    logement_id = logement_id_hint or (charge or {}).get("logement_id") or None
    proprietaire_id = (charge or {}).get("proprietaire_id") or None
    categorie_charge_id = (charge or {}).get("categorie_charge_id") or None
    type_flux_id = (charge or {}).get("type_flux_id") or None
    methode = "AFFECTATION_DIRECTE_LOGEMENT" if logement_id else None
    statut_ventilation = "VALIDE"

    if not logement_id:
        men_log, men_prop = _menage_dimensions(charge_id, db_path)
        if men_log:
            logement_id, proprietaire_id = men_log, men_prop
            methode = "REPARTITION_MENAGE"
        else:
            methode = "SANS_DIMENSION"
            statut_ventilation = "A_CONTROLER"

    from app.services import comptabilite_mappings_service as maps
    resolu = maps.resoudre_compte(categorie_charge_id=categorie_charge_id or "",
                                  type_flux_id=type_flux_id or "", db_path=db_path)

    return {
        "compte": resolu["compte"], "montant": round(montant, 2),
        "logement_id": logement_id, "proprietaire_id": proprietaire_id,
        "methode": methode, "statut_ventilation": statut_ventilation,
        "origine_type": origine_type, "origine_id": origine_id,
        "mapping_regle_id_opaque": resolu.get("regle_id_opaque"),
        "mapping_statut": resolu["statut"],
    }


def generer_ecriture_achat(facture_id_opaque: str, *, acteur: str = "",
                           db_path=None) -> dict[str, Any]:
    """Génère l'écriture ACHATS d'une facture fournisseur VALIDÉE (ou plus avancée dans son cycle).

    Une ligne de débit PAR charge source (mono-charge historique, ou une ligne par
    `facture_lignes` — case D « facture multi-lignes »), chacune sur le compte résolu par
    `comptabilite_mappings_service` et portant ses propres `logement_id`/`proprietaire_id` quand
    disponibles. Une seule ligne de crédit 401 (fournisseur). Idempotent : une facture ne génère
    jamais deux écritures ACHATS.
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
    facture_lignes = f.get("lignes") or []

    ventilation: list[dict[str, Any]] = []
    if facture_lignes:
        for fl in facture_lignes:
            src = _ligne_source_depuis_charge(
                fl["charge_id"], fl["montant_ttc"], origine_type="FACTURE_LIGNE",
                origine_id=fl["ligne_id_opaque"], logement_id_hint=fl.get("logement_id"),
                db_path=db_path)
            if src["methode"] != "SANS_DIMENSION":
                src["methode"] = "FACTURE_MULTI_LIGNES"
            ventilation.append(src)
    elif f.get("charge_id"):
        ventilation.append(_ligne_source_depuis_charge(
            f["charge_id"], montant, origine_type="CHARGE", origine_id=f["charge_id"],
            db_path=db_path))
    else:
        # Aucune charge liée du tout (facture pas encore rattachée) : un seul débit générique,
        # sans dimension, A_CONTROLER — le générateur ne bloque jamais sur ce cas historique.
        from app.services import comptabilite_mappings_service as maps
        resolu = maps.resoudre_compte(db_path=db_path)
        ventilation.append({
            "compte": resolu["compte"], "montant": montant, "logement_id": None,
            "proprietaire_id": None, "methode": "SANS_DIMENSION", "statut_ventilation": "A_CONTROLER",
            "origine_type": "FACTURE", "origine_id": facture_id_opaque,
            "mapping_regle_id_opaque": resolu.get("regle_id_opaque"),
            "mapping_statut": resolu["statut"],
        })

    lignes = [
        {"compte": v["compte"], "debit": v["montant"], "credit": 0,
         "logement_id": v["logement_id"], "proprietaire_id": v["proprietaire_id"],
         "libelle": f"Achat {f['facture_ref']}"}
        for v in ventilation
    ] + [
        {"compte": COMPTE_FOURNISSEURS, "debit": 0, "credit": montant,
         "auxiliaire": f["fournisseur_id_opaque"], "libelle": f["facture_ref"]},
    ]
    res = _inserer_ecriture(
        "ACHATS", f.get("date_facture") or _now()[:10], (f.get("date_facture") or _now())[:7],
        f["facture_ref"], f"Facture fournisseur {f['facture_ref']}", "FACTURE", facture_id_opaque,
        lignes, acteur=acteur, db_path=db_path)

    if res.get("ok") and not res.get("deja_generee"):
        _enregistrer_ventilation(res["ecriture_id_opaque"], ventilation, db_path=db_path)
    return res


def _enregistrer_ventilation(ecriture_id_opaque: str, ventilation: list[dict[str, Any]],
                             db_path=None) -> None:
    """Trace, pour chaque ligne de débit générée (dans l'ordre, `ligne_num` 1..N), comment sa
    dimension et son compte ont été déterminés — jamais un second calcul du montant lui-même."""
    if not ventilation:
        return
    conn = get_db(db_path)
    try:
        for i, v in enumerate(ventilation, start=1):
            conn.execute(
                "INSERT INTO ecriture_ligne_ventilation (ecriture_id_opaque, ligne_num, methode, "
                "pourcentage, montant_non_arrondi, montant_affiche, statut_ventilation, "
                "origine_type, origine_id, mapping_regle_id_opaque) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ecriture_id_opaque, i, v["methode"], None, v["montant"], v["montant"],
                 v["statut_ventilation"], v.get("origine_type"), v.get("origine_id"),
                 v.get("mapping_regle_id_opaque")))
        conn.commit()
    finally:
        conn.close()


def ventilation_ecriture(ecriture_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM ecriture_ligne_ventilation WHERE ecriture_id_opaque=? ORDER BY ligne_num",
            (ecriture_id_opaque,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


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


ORIGINE_LOT12 = "LOT12_PROPRIETAIRE_MOIS"
ORIGINE_FACTURE = "FACTURE_PROPRIETAIRE"
E_DOUBLE_SOURCE = "FACTURE_PROPRIETAIRE_DOUBLE_SOURCE_COMPTABLE"


def _ventes_lot12_du_mois(proprietaire_id: str, mois: str, db_path=None) -> list[str]:
    """Écritures VENTES déjà produites par l'ancien mécanisme agrégé, pour ce propriétaire/mois."""
    conn = get_db(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT ecriture_id_opaque FROM ecritures WHERE journal='VENTES' AND origine_type=? "
            "AND origine_id_opaque=? AND statut <> ?",
            (ORIGINE_LOT12, f"{proprietaire_id}:{mois}", ST_CONTREPASSEE)).fetchall()]
    finally:
        conn.close()


def ventes_factures_du_mois(proprietaire_id: str, mois: str, db_path=None) -> list[str]:
    """Factures propriétaires déjà comptabilisées pour ce propriétaire/mois."""
    conn = get_db(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT e.origine_id_opaque FROM ecritures e "
            "JOIN factures_proprietaires f ON f.facture_id_opaque = e.origine_id_opaque "
            "WHERE e.journal='VENTES' AND e.origine_type=? AND e.statut <> ? "
            "AND f.proprietaire_id=? AND f.mois=?",
            (ORIGINE_FACTURE, ST_CONTREPASSEE, proprietaire_id, mois)).fetchall()]
    except sqlite3.OperationalError:
        return []   # base antérieure à la migration 0027 : aucune facture ne peut exister
    finally:
        conn.close()


def charger_par_origine(origine_type: str, origine_id: str, db_path=None) -> dict[str, Any] | None:
    """Écriture vivante rattachée à un objet source — permet de remonter d'une facture à sa vente."""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM ecritures WHERE origine_type=? AND origine_id_opaque=? AND statut <> ?",
            (origine_type, origine_id, ST_CONTREPASSEE)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def generer_ecriture_vente_facture(facture: dict[str, Any], *, acteur: str = "",
                                   db_path=None) -> dict[str, Any]:
    """Écriture VENTES d'une facture propriétaire **ÉMISE** — source unique de la vente.

    Décision d'architecture : c'est la facture au statut EMIS qui matérialise la vente. Lot 10
    calcule, Lot 12 prépare le relevé, la facture constate, le règlement éteint la créance, la
    Banque prouve le mouvement. Une facture BROUILLON ou VALIDE ne produit aucune écriture.

    Le montant provient du **total figé de la facture**, jamais d'un recalcul : une écriture
    comptable ne doit pas pouvoir diverger du document remis au propriétaire.

    Anti-double comptage : si l'ancien mécanisme agrégé (`LOT12_PROPRIETAIRE_MOIS`) a déjà produit
    une vente pour ce propriétaire et ce mois, on **refuse**. Le conflit est signalé, jamais résolu
    en silence — les deux sources décrivent la même réalité économique à des grains différents
    (mois × propriétaire d'un côté, mois × propriétaire × logement de l'autre).
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if facture.get("statut") != "EMIS":
        return _refus(E_ORIGINE_INVALIDE,
                      f"statut {facture.get('statut')} — seule une facture EMIS constate la vente")

    facture_id = facture["facture_id_opaque"]
    proprietaire_id = facture["proprietaire_id"]
    mois = facture["mois"]

    conflits = _ventes_lot12_du_mois(proprietaire_id, mois, db_path)
    if conflits:
        return _refus(E_DOUBLE_SOURCE,
                      f"vente deja comptabilisee par {ORIGINE_LOT12} pour {proprietaire_id}:{mois} "
                      f"({', '.join(conflits)}) — arbitrer avant de facturer cette periode")

    montant = round(float(facture.get("montant_total") or 0), 2)
    if montant == 0:
        return _refus(E_ORIGINE_INVALIDE, "montant de facture nul — rien a constater")

    numero = facture.get("numero_facture") or facture_id
    libelle = f"Facture {numero} — {proprietaire_id} / {facture.get('logement_id')} — {mois}"

    # Un avoir porte un montant négatif : le sens s'inverse, sans traitement particulier ailleurs.
    if montant > 0:
        lignes_ecr = [
            {"compte": COMPTE_PROPRIETAIRES, "debit": montant, "credit": 0,
             "auxiliaire": proprietaire_id, "proprietaire_id": proprietaire_id, "libelle": libelle},
            {"compte": COMPTE_VENTE_GENERIQUE, "debit": 0, "credit": montant,
             "proprietaire_id": proprietaire_id, "libelle": libelle},
        ]
    else:
        m = abs(montant)
        lignes_ecr = [
            {"compte": COMPTE_VENTE_GENERIQUE, "debit": m, "credit": 0,
             "proprietaire_id": proprietaire_id, "libelle": libelle},
            {"compte": COMPTE_PROPRIETAIRES, "debit": 0, "credit": m,
             "auxiliaire": proprietaire_id, "proprietaire_id": proprietaire_id, "libelle": libelle},
        ]

    # Écriture agrégée au total de la facture : le détail par prestation reste porté par les lignes
    # de facture, qui constituent la piste d'audit. Ventiler par type exigerait un compte de produit
    # par prestation — mapping non arbitré, qu'on ne décide pas ici (706000 reste provisoire).
    return _inserer_ecriture(
        "VENTES", facture.get("date_facture") or f"{mois}-01", mois, numero, libelle,
        ORIGINE_FACTURE, facture_id, lignes_ecr, acteur=acteur, db_path=db_path)


def generer_ecriture_vente(proprietaire_id: str, mois: str, montant_du_conciergerie: float, *,
                           nom_proprietaire: str = "", acteur: str = "",
                           db_path=None) -> dict[str, Any]:
    """Génère l'écriture VENTES d'un propriétaire pour un mois, depuis le montant déjà calculé par
    Lot12 (`montant_du_conciergerie`, jamais recalculé ici — cf. cadrage §4). Origine
    `LOT12_PROPRIETAIRE_MOIS` : adaptateur explicite, Lot12 reste l'unique moteur de calcul.

    411 (Propriétaires, débit — créance conciergerie) / 706 (Ventes, crédit) si le montant est
    positif ; sens inversé si négatif (conciergerie redevable au propriétaire ce mois-ci). Montant
    nul : aucune écriture (rien à constater).
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    montant = round(montant_du_conciergerie or 0, 2)
    if montant == 0:
        return _refus(E_ORIGINE_INVALIDE, "montant_du_conciergerie nul — rien à générer")

    # Garde symétrique de `generer_ecriture_vente_facture` : dès qu'une facture propriétaire a
    # constaté la vente de ce mois, l'ancien mécanisme agrégé doit se taire. Sans cette garde, le
    # double comptage resterait possible dans ce sens-là (facture émise puis pipeline Lot12 rejoué).
    deja_facture = ventes_factures_du_mois(proprietaire_id, mois, db_path)
    if deja_facture:
        return _refus(E_DOUBLE_SOURCE,
                      f"vente deja constatee par facture(s) {', '.join(deja_facture)} pour "
                      f"{proprietaire_id}:{mois} — {ORIGINE_LOT12} ne doit plus generer")

    origine_id = f"{proprietaire_id}:{mois}"
    libelle = f"Prestations {nom_proprietaire or proprietaire_id} — {mois} (SOURCE_PROVISOIRE_LOT12)"
    if montant > 0:
        lignes = [
            {"compte": COMPTE_PROPRIETAIRES, "debit": montant, "credit": 0,
             "auxiliaire": proprietaire_id, "proprietaire_id": proprietaire_id, "libelle": libelle},
            {"compte": COMPTE_VENTE_GENERIQUE, "debit": 0, "credit": montant,
             "proprietaire_id": proprietaire_id, "libelle": libelle},
        ]
    else:
        m = abs(montant)
        lignes = [
            {"compte": COMPTE_VENTE_GENERIQUE, "debit": m, "credit": 0,
             "proprietaire_id": proprietaire_id, "libelle": libelle},
            {"compte": COMPTE_PROPRIETAIRES, "debit": 0, "credit": m,
             "auxiliaire": proprietaire_id, "proprietaire_id": proprietaire_id, "libelle": libelle},
        ]
    return _inserer_ecriture(
        "VENTES", f"{mois}-01", mois, origine_id, libelle, "LOT12_PROPRIETAIRE_MOIS", origine_id,
        lignes, acteur=acteur, db_path=db_path)


def generer_ecriture_caisse_reglement(reglement_id_opaque: str, *, acteur: str = "",
                                      db_path=None) -> dict[str, Any]:
    """Génère l'écriture CAISSE d'un règlement fournisseur payé en espèces (moyen=CAISSE) — même
    source déjà réelle que le journal ACHATS, jamais un second moteur de règlement."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    from app.services import reglements_fournisseurs_service as regl
    r = regl.charger(reglement_id_opaque, db_path)
    if r is None or r["moyen"] != "CAISSE" or r["statut"] == regl.ST_ANNULE:
        return _refus(E_ORIGINE_INVALIDE, reglement_id_opaque)
    montant = round(r["montant"], 2)
    lignes = [
        {"compte": COMPTE_FOURNISSEURS, "debit": montant, "credit": 0,
         "auxiliaire": r["fournisseur_id_opaque"], "libelle": "Règlement caisse"},
        {"compte": COMPTE_CAISSE, "debit": 0, "credit": montant, "libelle": "Sortie caisse"},
    ]
    return _inserer_ecriture(
        "CAISSE", r["date_reglement"], r["date_reglement"][:7], reglement_id_opaque,
        "Règlement fournisseur en espèces", "REGLEMENT", reglement_id_opaque, lignes,
        acteur=acteur, db_path=db_path)


def generer_ecriture_caisse_operation(operation_id_opaque: str, *, acteur: str = "",
                                      db_path=None) -> dict[str, Any]:
    """Génère l'écriture CAISSE d'une opération de caisse sans objet existant (encaissement,
    remboursement associé en espèces) — cf. `operations_caisse` (migration 0023)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        op = conn.execute(
            "SELECT * FROM operations_caisse WHERE operation_id_opaque=?",
            (operation_id_opaque,)).fetchone()
    finally:
        conn.close()
    if op is None or op["statut"] == "ANNULEE":
        return _refus(E_ORIGINE_INVALIDE, operation_id_opaque)
    montant = round(op["montant"], 2)
    if op["type_operation"] == "ENCAISSEMENT":
        # entrée d'argent en caisse : contrepartie sur l'auxiliaire du tiers (créance qui diminue)
        lignes = [
            {"compte": COMPTE_CAISSE, "debit": montant, "credit": 0, "libelle": "Encaissement"},
            {"compte": COMPTE_PROPRIETAIRES if op["tiers_type"] == "PROPRIETAIRE" else COMPTE_ASSOCIES,
             "debit": 0, "credit": montant, "auxiliaire": op["tiers_id"], "libelle": "Encaissement"},
        ]
    else:
        # REMBOURSEMENT_ASSOCIE ou AUTRE : sortie de caisse vers le tiers (compte associés par défaut)
        lignes = [
            {"compte": COMPTE_ASSOCIES, "debit": montant, "credit": 0,
             "auxiliaire": op["tiers_id"], "libelle": op["type_operation"]},
            {"compte": COMPTE_CAISSE, "debit": 0, "credit": montant, "libelle": op["type_operation"]},
        ]
    return _inserer_ecriture(
        "CAISSE", op["date_operation"], op["date_operation"][:7], operation_id_opaque,
        f"Opération caisse {op['type_operation']}", "OPERATION_CAISSE", operation_id_opaque, lignes,
        acteur=acteur, db_path=db_path)


def generer_ecriture_od(od_id_opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Génère l'écriture ODIVERSES d'une opération diverse VALIDÉE — les lignes viennent
    directement de `od_lignes` (déjà équilibrées à la validation, cf. `operations_diverses_service`)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        od = conn.execute(
            "SELECT * FROM operations_diverses WHERE od_id_opaque=?", (od_id_opaque,)).fetchone()
        if od is None:
            return _refus(E_ORIGINE_INVALIDE, od_id_opaque)
        rows = conn.execute(
            "SELECT * FROM od_lignes WHERE od_id_opaque=? ORDER BY ligne_num",
            (od_id_opaque,)).fetchall()
    finally:
        conn.close()
    if od["statut"] != "VALIDEE":
        return _refus(E_ORIGINE_INVALIDE, f"OD au statut {od['statut']}")
    lignes = [{"compte": r["compte"], "auxiliaire": r["auxiliaire"], "debit": r["debit"],
              "credit": r["credit"], "libelle": r["commentaire"] or od["libelle"]} for r in rows]
    return _inserer_ecriture(
        "ODIVERSES", od["date_operation"], od["date_operation"][:7], od_id_opaque, od["libelle"],
        "OPERATION_DIVERSE", od_id_opaque, lignes, acteur=acteur, db_path=db_path)


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

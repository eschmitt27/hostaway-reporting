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
from datetime import date, datetime, timezone
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
# `455100` depuis la migration 0099. `467000`, retenu par défaut au démarrage du
# module comptable, est désactivé : il n'a jamais porté d'écriture.
COMPTE_ASSOCIES = "455100"
COMPTE_ACHAT_GENERIQUE = "606000"
COMPTE_VENTE_GENERIQUE = "706000"
# Mission 36 — acomptes clients (et reversements Airbnb, famille acompte) : jamais un produit.
COMPTE_ACOMPTES_CLIENTS = "419100"
# TVA collectée : utilisée SEULEMENT si la facture porte de la TVA (hors franchise) et que le
# compte existe, actif, dans le plan. Rien n'est supposé : sinon la comptabilisation est refusée.
COMPTE_TVA_COLLECTEE = "445710"

# Dérivation CERTAINE type technique → type économique (même table que la migration 0114).
TYPE_ECONOMIQUE_PAR_TYPE_LIGNE = {
    "COMMISSION_CONCIERGERIE": "GESTION", "MENAGE_FACTURE": "MENAGE", "CHARGE_FIXE": "FORFAIT",
    "PREPARATION_CANAPE": "SERVICE_ADDITIONNEL", "EXTRA": "SERVICE_ADDITIONNEL",
    "CHARGES_EXCEPT_REFAC": "REFACTURATION", "CHARGE_REFACTUREE": "REFACTURATION",
    "REDUCTION": "REDUCTION",
}
ORIGINE_IMPUTATION_ACOMPTE = "IMPUTATION_ACOMPTE"        # une écriture par (facture, acompte)
ORIGINE_IMPUTATION_CREDIT = "IMPUTATION_CREDIT"          # une écriture par imputation Airbnb

E_FLAGS = "E_FLAGS_DESACTIVES"
E_INTROUVABLE = "E_ECRITURE_INTROUVABLE"
E_DEJA_GENEREE = "E_DEJA_GENEREE"
E_DESEQUILIBRE = "E_ECRITURE_DESEQUILIBREE"
E_COMPTE_INCONNU = "E_COMPTE_INCONNU"
E_ORIGINE_INVALIDE = "E_ORIGINE_INVALIDE"
E_STATUT = "E_TRANSITION_INTERDITE"
E_PERIODE_CLOTUREE = "E_PERIODE_CLOTUREE"
E_AUXILIAIRE = "E_AUXILIAIRE_REQUIS"
E_TYPE_LIGNE = "E_TYPE_LIGNE_SANS_COMPTE"
E_TVA = "E_TVA_NON_PARAMETREE"
E_ORIGINE_ACOMPTE = "E_ACOMPTE_SANS_ORIGINE_COMPTABLE"

MESSAGES = {
    E_FLAGS: "Écriture comptable désactivée sur cette installation.",
    E_INTROUVABLE: "Écriture introuvable.",
    E_DEJA_GENEREE: "Une écriture existe déjà pour cette origine sur ce journal.",
    E_DESEQUILIBRE: "Écriture déséquilibrée : le total débit doit égaler le total crédit.",
    E_COMPTE_INCONNU: "Compte inconnu ou inactif dans le plan comptable.",
    E_ORIGINE_INVALIDE: "Origine de l'écriture invalide pour ce journal.",
    E_STATUT: "Transition de statut interdite.",
    E_PERIODE_CLOTUREE: "Cette période comptable est clôturée : aucune écriture directe n'est autorisée.",
    E_AUXILIAIRE: "Ce compte exige un tiers (auxiliaire) : fournisseur, client ou associé selon le compte.",
    E_TYPE_LIGNE: "Une ligne de facture n'a pas de compte de produit actif pour son type.",
    E_TVA: "La facture porte de la TVA, mais aucun compte de TVA collectée actif n'est paramétré.",
    E_ORIGINE_ACOMPTE: "Acompte ou reversement imputé sans crédit correspondant au compte d'acomptes "
                       "du client (419100) : aucune écriture n'est inventée.",
}


def _flags_actifs() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _nom_tiers(proprietaire_id: Any, db_path=None) -> str:
    """Nom du propriétaire pour un LIBELLÉ d'écriture. Le code si le référentiel ne le connaît pas.

    Un libellé se lit dans un journal, un grand livre, un export remis au comptable : « PROP_0002 »
    n'y apprend rien. L'identifiant, lui, reste porté par la colonne `auxiliaire`, qui est faite
    pour ça — on ne perd donc aucune capacité de rapprochement en le retirant du texte.
    """
    ident = str(proprietaire_id or "").strip()
    if not ident:
        return ""
    try:
        from app.services import referentiel_service as ref
        nom = ref.nom_complet_proprietaire(ident, db_path=db_path)
    except Exception:      # noqa: BLE001 — référentiel absent : le code reste lisible
        nom = ""
    return nom or ident


def _nom_logement(logement_id: Any, db_path=None) -> str:
    ident = str(logement_id or "").strip()
    if not ident:
        return ""
    try:
        from app.services import referentiel_service as ref
        nom = ref.nom_logement(ident, db_path=db_path)
    except Exception:      # noqa: BLE001
        nom = ""
    return nom or ident


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
                      acteur: str = "", db_path=None, conn=None) -> dict[str, Any]:
    """Insère une écriture équilibrée. Ne vérifie PAS les flags — appelé par les générateurs
    spécifiques, qui l'ont déjà fait.

    `conn` : même idiome que `charges_saisie_service.creer`. Fourni, l'écriture s'insère DANS la
    transaction de l'appelant (ni commit ni fermeture ici) : c'est ce qui permet au lettrage des
    flux financiers de rapprocher ET de comptabiliser en une seule opération, annulée en bloc si
    l'une des deux échoue. Les contrôles (équilibre, période, comptes, doublon) restent ceux-ci,
    lus sur l'état validé de la base — aucun second jeu de règles."""
    total_debit = round(sum(l.get("debit", 0) or 0 for l in lignes), 2)
    total_credit = round(sum(l.get("credit", 0) or 0 for l in lignes), 2)
    if total_debit != total_credit or total_debit == 0:
        return _refus(E_DESEQUILIBRE, f"débit={total_debit} crédit={total_credit}")

    from app.services import comptabilite_periodes_service as per
    if per.est_fermee(periode, db_path):
        return _refus(E_PERIODE_CLOTUREE, periode)
    # Cutover V1 : la comptabilité applicative commence à sa date de début ; une écriture d'une
    # période antérieure recréerait l'ancienne comptabilité (doublé par la migration 0120).
    from app.services import perimetre_v1_service as v1
    if v1.est_anterieur(periode, db_path=db_path):
        return {"ok": False, "code": v1.E_COMPTABILITE_AVANT_V1,
                "message": v1.message_comptabilite(db_path=db_path), "detail": periode}

    # Tiers (auxiliaire) : c'est le PLAN qui dit si un compte en porte un (Mission 36). Sur un
    # compte sans tiers, un auxiliaire resté d'un ancien choix est effacé — jamais enregistré ;
    # sur un compte à tiers obligatoire, son absence est refusée.
    from app.services import comptabilite_plan_service as plan
    lignes = [dict(l) for l in lignes]
    for l in lignes:
        compte = _compte_valide(l["compte"], db_path)
        if compte is None:
            return _refus(E_COMPTE_INCONNU, l["compte"])
        mode, type_tiers = plan.mode_auxiliaire(compte)
        if mode == plan.AUX_NONE:
            l["auxiliaire"] = None
        elif mode == plan.AUX_REQUIRED and not str(l.get("auxiliaire") or "").strip():
            return _refus(E_AUXILIAIRE, f"{l['compte']} : "
                          f"{plan.LIBELLES_AUX_TYPE.get(type_tiers, 'tiers')} obligatoire")

    existant = _deja_generee(journal, origine_type, origine_id, db_path)
    if existant:
        # Idempotence : regénérer pour la même origine est un no-op explicite, jamais un doublon.
        return {"ok": True, "ecriture_id_opaque": existant, "deja_generee": True}

    opaque = "ECR-" + uuid.uuid4().hex[:12].upper()
    connexion_locale = conn is None
    if connexion_locale:
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
        if connexion_locale:
            conn.commit()
    finally:
        if connexion_locale:
            conn.close()
    return {"ok": True, "ecriture_id_opaque": opaque, "deja_generee": False}


def valider_dans_transaction(conn, opaque: str, *, commentaire: str = "",
                             acteur: str = "") -> None:
    """PROPOSEE → VALIDEE dans la transaction de l'appelant, avec son événement.

    Réservé à une écriture que l'appelant vient d'insérer dans cette même transaction et que
    l'utilisateur a relue AVANT de valider (écran « écriture comptable proposée ») : la validation
    humaine a eu lieu, il n'y a pas de seconde étape à attendre."""
    conn.execute("UPDATE ecritures SET statut=?, version=version+1 WHERE ecriture_id_opaque=? "
                 "AND statut=?", (ST_VALIDEE, opaque, ST_PROPOSEE))
    _evenement(conn, opaque, "VALIDATION", ancien=ST_PROPOSEE, nouveau=ST_VALIDEE,
               commentaire=commentaire, acteur=acteur)


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
    # Une ligne marquée EXTRACTION_INCORRECTE (§30) est écartée du total de la facture : elle doit
    # l'être AUSSI de l'écriture, sinon le débit (toutes lignes) et le crédit (total document) ne
    # s'équilibrent plus et l'écriture est refusée — la facture resterait validée SANS sa dette.
    facture_lignes = [l for l in (f.get("lignes") or []) if not l.get("neutralisee")]

    ventilation: list[dict[str, Any]] = []
    if facture_lignes:
        for fl in facture_lignes:
            # `charge_id` n'existe que sur les lignes issues de l'ancien rattachement de charge.
            # Une ligne extraite d'un PDF (`facture_lignes_menage`) n'en a pas — et n'en a pas
            # besoin : elle porte déjà son `logement_id` résolu. Sans ce `.get()`, la génération
            # de l'écriture d'achat levait un KeyError sur toute facture réellement extraite.
            src = _ligne_source_depuis_charge(
                fl.get("charge_id") or "", fl["montant_ttc"], origine_type="FACTURE_LIGNE",
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


def _rapprochement(rapprochement_id_opaque: str, type_attendu: str, db_path=None, conn=None):
    """`conn` fourni : lu dans la transaction de l'appelant, qui voit donc un rapprochement qu'il
    vient d'insérer sans l'avoir encore validé."""
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        rap = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE rapprochement_id_opaque=?",
            (rapprochement_id_opaque,)).fetchone()
    finally:
        if connexion_locale:
            conn.close()
    if rap is None or rap["type_objet"] != type_attendu or rap["statut"] != "CONFIRME":
        return None
    return rap


def generer_ecriture_apport_associe(rapprochement_id_opaque: str, *, acteur: str = "",
                                    db_path=None, conn=None) -> dict[str, Any]:
    """Apport d'un associé en compte courant : 512 (Banque, débit) / 455 (Associés, crédit).

    Le compte est `455100 — Associés - comptes courants - Principal` (migration 0099), le compte
    normatif d'un apport en compte courant. Un seul compte pour tous les associés : c'est
    l'AUXILIAIRE qui nomme la personne, et c'est lui qui permet de dire plus tard « combien la
    société doit-elle à Untel ».

    L'associé est porté en AUXILIAIRE — c'est ce qui permet de dire plus tard « combien la société
    doit-elle à Untel », et pourquoi l'écran exige de le choisir au lieu de le deviner.

    Ce mouvement n'est ni une vente, ni un produit : il n'y a pas de résultat, seulement une dette
    de la société envers son associé.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    rap = _rapprochement(rapprochement_id_opaque, "APPORT_ASSOCIE", db_path, conn=conn)
    if rap is None:
        return _refus(E_ORIGINE_INVALIDE, rapprochement_id_opaque)

    montant = round(rap["montant_rapproche"], 2)
    associe = rap["objet_id"]
    lignes = [
        {"compte": COMPTE_BANQUE, "debit": montant, "credit": 0, "libelle": "Apport en compte courant"},
        {"compte": COMPTE_ASSOCIES, "debit": 0, "credit": montant, "auxiliaire": associe,
         "libelle": "Apport en compte courant d'associé"},
    ]
    return _inserer_ecriture(
        "BANQUE", rap["date_creation"][:10], rap["date_creation"][:7], rapprochement_id_opaque,
        "Apport en compte courant d'associé", "RAPPROCHEMENT", rapprochement_id_opaque, lignes,
        acteur=acteur, db_path=db_path, conn=conn)


def generer_ecriture_remboursement_associe(rapprochement_id_opaque: str, *, acteur: str = "",
                                           db_path=None, conn=None) -> dict[str, Any]:
    """Remboursement d'un compte courant d'associé : 455 (Associés, débit) / 512 (Banque, crédit).

    L'écriture miroir de l'apport : même compte `455100`, même auxiliaire (l'associé), sens
    inverse. La dette de la société envers l'associé diminue ; aucun résultat n'est touché.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    rap = _rapprochement(rapprochement_id_opaque, "REMBOURSEMENT_ASSOCIE", db_path, conn=conn)
    if rap is None:
        return _refus(E_ORIGINE_INVALIDE, rapprochement_id_opaque)

    montant = round(rap["montant_rapproche"], 2)
    associe = rap["objet_id"]
    lignes = [
        {"compte": COMPTE_ASSOCIES, "debit": montant, "credit": 0, "auxiliaire": associe,
         "libelle": "Remboursement de compte courant d'associé"},
        {"compte": COMPTE_BANQUE, "debit": 0, "credit": montant,
         "libelle": "Remboursement de compte courant"},
    ]
    return _inserer_ecriture(
        "BANQUE", rap["date_creation"][:10], rap["date_creation"][:7], rapprochement_id_opaque,
        "Remboursement de compte courant d'associé", "RAPPROCHEMENT", rapprochement_id_opaque,
        lignes, acteur=acteur, db_path=db_path, conn=conn)


def generer_ecriture_transfert_caisse(rapprochement_id_opaque: str, *, acteur: str = "",
                                      db_path=None, conn=None) -> dict[str, Any]:
    """Retrait d'espèces : 530 (Caisse, débit) / 512 (Banque, crédit).

    L'argent change de contenant, il ne se dépense pas : aucun compte de charge n'apparaît, et le
    résultat est rigoureusement inchangé. C'est ce qui rend cette écriture automatisable sans
    décision humaine, là où un paiement par carte demanderait de savoir ce qui a été acheté.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    rap = _rapprochement(rapprochement_id_opaque, "TRANSFERT_CAISSE", db_path, conn=conn)
    if rap is None:
        return _refus(E_ORIGINE_INVALIDE, rapprochement_id_opaque)

    montant = round(rap["montant_rapproche"], 2)
    lignes = [
        {"compte": COMPTE_CAISSE, "debit": montant, "credit": 0, "libelle": "Retrait d'espèces"},
        {"compte": COMPTE_BANQUE, "debit": 0, "credit": montant, "libelle": "Retrait au distributeur"},
    ]
    return _inserer_ecriture(
        "BANQUE", rap["date_creation"][:10], rap["date_creation"][:7], rapprochement_id_opaque,
        "Transfert banque vers caisse", "RAPPROCHEMENT", rapprochement_id_opaque, lignes,
        acteur=acteur, db_path=db_path, conn=conn)


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

    prevue = ecriture_vente_prevue(facture, db_path=db_path)
    if not prevue["ok"]:
        return prevue
    return _inserer_ecriture(
        "VENTES", facture.get("date_facture") or f"{mois}-01", mois, prevue["piece"],
        prevue["libelle"], ORIGINE_FACTURE, facture_id, prevue["lignes"], acteur=acteur,
        db_path=db_path)


def mapping_produits(*, db_path=None) -> dict[str, dict[str, Any]]:
    """{type_economique: {compte, famille, libelle}} — table `mapping_produits_facture` (0114)."""
    conn = get_db(db_path)
    try:
        return {r["type_economique"]: dict(r) for r in conn.execute(
            "SELECT * FROM mapping_produits_facture WHERE actif=1")}
    finally:
        conn.close()


def type_economique(ligne: dict[str, Any]) -> str:
    return (str(ligne.get("type_economique") or "").strip()
            or TYPE_ECONOMIQUE_PAR_TYPE_LIGNE.get(str(ligne.get("type_ligne") or ""), ""))


def ecriture_vente_prevue(facture: dict[str, Any], *, db_path=None) -> dict[str, Any]:
    """L'écriture VENTES d'une facture, LIGNE PAR LIGNE — calcul pur, n'écrit rien (Mission 36).

    Chaque ligne va au compte de SON TYPE économique (jamais déduit du libellé) :
    gestion 706100, ménage 706200, forfait 706300, sinistre 706400, services additionnels 706500,
    autres prestations 706900, refacturations 708800 ; une RÉDUCTION va au débit de 709600
    (« rabais, remises et ristournes accordés »), jamais en produit négatif. Le client est débité
    du total (411000 + propriétaire). Un acompte n'est pas une ligne : il s'impute à part
    (`generer_ecriture_imputation_acomptes`), 419100 → 411000.

    TVA : sous franchise (état actuel), aucune ligne de TVA. Si la facture en porte, le compte de
    TVA collectée doit exister, actif — sinon refus explicite, aucune hypothèse fiscale."""
    from app.services import factures_proprietaires_conformite_service as conformite
    from app.services import factures_proprietaires_service as fpr

    facture_id = facture["facture_id_opaque"]
    proprietaire_id = facture["proprietaire_id"]
    mois = facture["mois"]
    lignes_facture = facture.get("lignes")
    if lignes_facture is None:
        lignes_facture = fpr.lire(facture_id, db_path=db_path)["lignes"]
    produits = mapping_produits(db_path=db_path)
    numero = facture.get("numero_facture") or facture_id
    # §78 — le libellé d'une écriture se lit dans un journal, un grand livre, un export comptable.
    # Il portait « PROP_0002 / LOG_0002 » : deux codes internes, illisibles pour qui tient les
    # comptes. Les identifiants restent dans `auxiliaire` et `logement_id`, qui sont faits pour ça.
    libelle = (f"Facture {numero} — {_nom_tiers(proprietaire_id, db_path)} / "
               f"{_nom_logement(facture.get('logement_id'), db_path)} — {mois}")

    # Montant net par compte de produit (signé : positif = crédit). Un avoir porte des lignes
    # négatives : les sens s'inversent d'eux-mêmes, ligne par ligne.
    par_compte: dict[str, float] = {}
    libelles: dict[str, str] = {}
    for l in lignes_facture:
        te = type_economique(l)
        regle = produits.get(te)
        if regle is None or regle["famille"] == "ACOMPTE" or _compte_valide(regle["compte"],
                                                                             db_path) is None:
            return _refus(E_TYPE_LIGNE, f"ligne {l.get('numero_ligne')} « {l.get('libelle')} » "
                                        f"(type {te or l.get('type_ligne')}) : aucun compte de "
                                        "produit actif — Comptabilité › Mappings")
        v = round(float(l.get("montant") or 0), 2)
        par_compte[regle["compte"]] = round(par_compte.get(regle["compte"], 0) + v, 2)
        libelles.setdefault(regle["compte"], regle["libelle"])

    tva = 0.0
    conf = conformite.charger(facture_id, db_path=db_path) or {}
    try:
        tva = round(float(conf.get("total_tva") or 0), 2)
    except (TypeError, ValueError):
        tva = 0.0
    if abs(tva) > 0.005 and _compte_valide(COMPTE_TVA_COLLECTEE, db_path) is None:
        return _refus(E_TVA, f"TVA {tva:.2f} € — compte {COMPTE_TVA_COLLECTEE} absent ou inactif")

    total_client = round(sum(par_compte.values()) + tva, 2)
    lignes_ecr: list[dict[str, Any]] = []

    def _ligne(compte, signe_credit, texte, **kw):
        v = round(signe_credit, 2)
        if abs(v) > 0.005:
            lignes_ecr.append({"compte": compte, "debit": abs(v) if v < 0 else 0,
                               "credit": v if v > 0 else 0, "libelle": texte,
                               "proprietaire_id": proprietaire_id,
                               "logement_id": facture.get("logement_id"), **kw})

    _ligne(COMPTE_PROPRIETAIRES, -total_client, libelle, auxiliaire=proprietaire_id)
    for compte in sorted(par_compte):
        _ligne(compte, par_compte[compte], f"{libelles[compte]} — Facture {numero}")
    if abs(tva) > 0.005:
        _ligne(COMPTE_TVA_COLLECTEE, tva, f"TVA collectée — Facture {numero}")
    return {"ok": True, "piece": numero, "libelle": libelle, "lignes": lignes_ecr,
            "total_client": total_client}


def generer_ecriture_imputation_credit(imputation_airbnb_id: str, *, acteur: str = "",
                                       db_path=None) -> dict[str, Any]:
    """Constate l'imputation d'un reversement Airbnb sur une facture ÉMISE : 419100 → 411000.

    L'imputation doit être reliée à un crédit dont l'origine est constatée (Mission 37) : sans lui,
    refus — aucune écriture d'origine n'est inventée. Idempotent (une écriture par imputation)."""
    from app.services import credits_clients_service as credits
    from app.services import factures_proprietaires_service as fpr
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        imp = conn.execute("SELECT * FROM imputations_airbnb WHERE imputation_airbnb_id=?",
                           (imputation_airbnb_id,)).fetchone()
    finally:
        conn.close()
    if imp is None:
        return _refus(E_ORIGINE_INVALIDE, imputation_airbnb_id)
    credit = credits.charger(imp["credit_id_opaque"], db_path=db_path) \
        if imp["credit_id_opaque"] else None
    if credit is None or credit["statut"] != credits.ST_DISPONIBLE:
        return _refus(E_ORIGINE_ACOMPTE, f"reversement Airbnb {round(imp['montant_impute'], 2):.2f} € "
                                         "sans crédit d'origine constatée — à régulariser")
    facture = fpr.lire(imp["document_id"], db_path=db_path)
    if facture["statut"] != fpr.ST_EMIS:
        return {"ok": True, "en_attente_emission": True}
    montant = round(float(imp["montant_impute"]), 2)
    numero = facture.get("numero_facture") or facture["facture_id_opaque"]
    pid = facture["proprietaire_id"]
    libelle = (f"Reversement Airbnb imputé — Facture {numero}"
               if credit["origine"] == credits.ORIGINE_REVERSEMENT_AIRBNB
               else f"Crédit client imputé ({credit['libelle_origine']}) — Facture {numero}")
    res = _inserer_ecriture(
        "ODIVERSES", facture.get("date_facture") or f"{facture['mois']}-01", facture["mois"],
        numero, libelle, ORIGINE_IMPUTATION_CREDIT, imputation_airbnb_id,
        [{"compte": credits.compte_du_credit(credit["origine"]), "debit": montant, "credit": 0,
          "auxiliaire": pid, "proprietaire_id": pid,
          "libelle": f"{libelle} ({credit['reference'] or credit['credit_id_opaque']})"},
         {"compte": COMPTE_PROPRIETAIRES, "debit": 0, "credit": montant, "auxiliaire": pid,
          "proprietaire_id": pid, "libelle": libelle}],
        acteur=acteur, db_path=db_path)
    if res.get("ok") and not res.get("deja_generee"):
        conn = get_db(db_path)
        try:
            conn.execute("UPDATE imputations_airbnb SET ecriture_imputation=? "
                         "WHERE imputation_airbnb_id=?", (res["ecriture_id_opaque"], imputation_airbnb_id))
            credits._evenement(conn, credit["credit_id_opaque"], "ECRITURE_IMPUTATION", acteur=acteur,
                               montant=montant, facture_id=facture["facture_id_opaque"],
                               ecriture_id=res["ecriture_id_opaque"])
            conn.commit()
        finally:
            conn.close()
    return res


def _acomptes_imputes_par_fifo(facture: dict[str, Any], *, db_path=None) -> list[dict[str, Any]]:
    """Part de chaque ACOMPTE que le FIFO du compte propriétaire impute sur cette facture.

    Seuls comptent les acomptes (mouvement propriétaire → société, nature ACOMPTE) qui ne sont pas
    un règlement direct de facture : un encaissement créé par un rapprochement de facture est déjà
    constaté 512 / 411 et n'a rien à transiter par 419100."""
    from app.services import compte_proprietaire_service as cpt
    from app.services import proprietaires_tresorerie_service as tres
    fid = facture["facture_id_opaque"]
    out = []
    for a in cpt.calculer(facture["proprietaire_id"], db_path=db_path)["allocations"]:
        if a["facture_id_opaque"] != fid or a["source_type"] != cpt.SRC_PAIEMENT:
            continue
        m = tres.charger(a["source_ref"], db_path)
        if (m is None or m["nature"] != "ACOMPTE_PROPRIETAIRE"
                or str(m.get("source_type") or "") == "FLUX_LETTRAGE"):
            continue
        out.append({"reference": a["source_ref"], "montant": round(a["montant_alloue"], 2),
                    "date": str(m.get("date_mouvement") or "")[:10]})
    return out


def generer_ecriture_imputation_acomptes(facture: dict[str, Any], *, acteur: str = "",
                                         db_path=None) -> dict[str, Any]:
    """Impute sur la créance de cette facture ÉMISE ce que le client a déjà en 419100.

    · ACOMPTES : la part que le FIFO leur attribue sur cette facture ; une écriture 419100 → 411000
      par acompte, si l'acompte a son origine comptable (512 / 419100) ;
    · REVERSEMENTS AIRBNB : chaque imputation reliée à un crédit dont l'origine est constatée
      (`generer_ecriture_imputation_credit`).
    Ce qui n'a pas d'origine comptable n'est PAS écrit : il est rendu dans `sans_origine` (refus
    propre, à régulariser), jamais compensé par une écriture inventée. Rejouable sans doublon."""
    from app.services import credits_clients_service as credits
    from app.services import factures_proprietaires_service as fpr
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if facture.get("statut") != "EMIS" or facture.get("type_document", "FACTURE") != "FACTURE":
        return {"ok": True, "rien_a_imputer": True, "ecritures": [], "sans_origine": []}
    fid = facture["facture_id_opaque"]
    pid = facture["proprietaire_id"]
    numero = facture.get("numero_facture") or fid
    ecritures, sans_origine = [], []

    for a in _acomptes_imputes_par_fifo(facture, db_path=db_path):
        if not credits.ecriture_origine_acompte(a["reference"], db_path=db_path):
            sans_origine.append({"type": "ACOMPTE_APPLIQUE", "montant": a["montant"],
                                 "detail": f"acompte du {a['date']} sans encaissement comptabilisé"})
            continue
        libelle = f"Acompte imputé — Facture {numero}"
        res = _inserer_ecriture(
            "ODIVERSES", facture.get("date_facture") or f"{facture['mois']}-01", facture["mois"],
            numero, libelle, ORIGINE_IMPUTATION_ACOMPTE, f"{fid}|{a['reference']}",
            [{"compte": COMPTE_ACOMPTES_CLIENTS, "debit": a["montant"], "credit": 0,
              "auxiliaire": pid, "proprietaire_id": pid, "libelle": f"Acompte du {a['date']} imputé"},
             {"compte": COMPTE_PROPRIETAIRES, "debit": 0, "credit": a["montant"],
              "auxiliaire": pid, "proprietaire_id": pid, "libelle": libelle}],
            acteur=acteur, db_path=db_path)
        if res.get("ok"):
            ecritures.append(res["ecriture_id_opaque"])

    for r in fpr.reversements_airbnb(fid, db_path=db_path):
        if str(r.get("statut") or "VALIDE").upper() not in ("VALIDE", "VALIDEE"):
            continue
        res = generer_ecriture_imputation_credit(r["imputation_airbnb_id"], acteur=acteur,
                                                 db_path=db_path)
        if res.get("ok") and res.get("ecriture_id_opaque"):
            ecritures.append(res["ecriture_id_opaque"])
        elif not res.get("ok"):
            sans_origine.append({"type": "REVERSEMENT_AIRBNB",
                                 "montant": round(float(r["montant_impute"]), 2),
                                 "detail": res.get("detail") or res.get("message")})
    if sans_origine:
        return {**_refus(E_ORIGINE_ACOMPTE, "; ".join(f"{x['montant']:.2f} € — {x['detail']}"
                                                      for x in sans_origine)),
                "ecritures": ecritures, "sans_origine": sans_origine}
    return {"ok": True, "ecritures": ecritures, "sans_origine": [],
            "rien_a_imputer": not ecritures,
            "ecriture_id_opaque": ecritures[0] if len(ecritures) == 1 else None}


def comptabiliser_facture_emise(facture: dict[str, Any], *, acteur: str = "",
                                db_path=None) -> dict[str, Any]:
    """À l'émission : l'écriture de vente ligne par ligne, puis l'imputation des acomptes."""
    vente = generer_ecriture_vente_facture(facture, acteur=acteur, db_path=db_path)
    imputation = generer_ecriture_imputation_acomptes(facture, acteur=acteur, db_path=db_path)
    # Crédits clients imputés à l'émission (2026-10-03) : 419100 → 411000, APRÈS la vente et la
    # créance. Idempotent (une écriture par imputation).
    conn = get_db(db_path)
    try:
        ids = [r[0] for r in conn.execute(
            "SELECT imputation_airbnb_id FROM imputations_airbnb WHERE document_id=? "
            "AND credit_id_opaque IS NOT NULL", (facture["facture_id_opaque"],))]
    finally:
        conn.close()
    credits = [generer_ecriture_imputation_credit(i, acteur=acteur, db_path=db_path) for i in ids]
    surplus = None
    if facture.get("type_document") == "AVOIR" and vente.get("ok"):
        # Surplus d'avoir au-delà de la créance : 411 → 419700, crédit client canonique.
        from app.services import credits_clients_service as credits_svc
        surplus = credits_svc.convertir_surplus_avoir(facture["facture_id_opaque"], acteur=acteur,
                                                      db_path=db_path)
    return {"vente": vente, "imputation": imputation, "credits": credits, "surplus": surplus}


# ── Comptabilisation GUIDÉE d'une facture client émise (2026-10-04) ──────────────────────────────
# Émettre ne comptabilise plus : la facture émise reste « Non comptabilisée » jusqu'à ce qu'un
# humain relise la proposition d'écriture et la valide. La proposition est calculée par
# `ecriture_vente_prevue` (mappings `mapping_produits_facture`, plan comptable, TVA de la
# conformité figée) — aucun second jeu de règles. « Comptabilisée » = écriture VENTES de la facture
# au statut VALIDEE ; une écriture PROPOSEE (générée par l'ancienne émission automatique) n'est
# qu'une proposition en attente de validation.

E_DEJA_COMPTABILISEE = "E_FACTURE_DEJA_COMPTABILISEE"
E_PROPOSITION_INCOMPLETE = "E_PROPOSITION_INCOMPLETE"
MESSAGES[E_DEJA_COMPTABILISEE] = "Cette facture est déjà comptabilisée."
MESSAGES[E_PROPOSITION_INCOMPLETE] = ("L'écriture proposée est incomplète (compte à confirmer) : "
                                      "complétez les mappings avant de valider.")
COMPTA_NON_COMPTABILISEE = "NON_COMPTABILISEE"
COMPTA_COMPTABILISEE = "COMPTABILISEE"


def _libelle_compte(compte: str, db_path=None) -> str:
    c = _compte_valide(compte, db_path)
    return (c or {}).get("libelle") or ""


def _date_validation(opaque: str, db_path=None) -> str:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT date_evenement FROM ecriture_evenements WHERE ecriture_id_opaque=? "
            "AND type_evenement='VALIDATION' ORDER BY id DESC LIMIT 1", (opaque,)).fetchone()
    finally:
        conn.close()
    return r[0] if r else ""


def etat_comptabilisation_facture(facture: dict[str, Any], *, db_path=None) -> dict[str, Any]:
    """État comptable d'une facture ÉMISE, lu dans les écritures (jamais un drapeau)."""
    ecr = charger_par_origine(ORIGINE_FACTURE, facture["facture_id_opaque"], db_path=db_path)
    if ecr and ecr["statut"] == ST_VALIDEE:
        return {"etat": COMPTA_COMPTABILISEE, "ecriture": ecr,
                "date_comptabilisation": _date_validation(ecr["ecriture_id_opaque"], db_path)}
    return {"etat": COMPTA_NON_COMPTABILISEE, "ecriture": ecr, "date_comptabilisation": ""}


def proposition_comptabilisation_facture(facture: dict[str, Any], *,
                                         db_path=None) -> dict[str, Any]:
    """Proposition d'écriture VENTES d'une facture émise, à relire AVANT validation. N'écrit rien.

    `bloquants` non vide ⇒ la validation est refusée. Une ligne de facture sans mapping de produit
    actif apparaît « Compte à confirmer » (compte vide) au lieu de faire échouer l'affichage.
    """
    bloquants: list[str] = []
    if not _flags_actifs():
        bloquants.append(MESSAGES[E_FLAGS])
    if facture.get("statut") != "EMIS":
        bloquants.append("seule une facture émise peut être comptabilisée")
    if int(facture.get("hors_compta") or 0):
        bloquants.append("facture émise hors comptabilité")
    etat = etat_comptabilisation_facture(facture, db_path=db_path)
    if etat["etat"] == COMPTA_COMPTABILISEE:
        bloquants.append(MESSAGES[E_DEJA_COMPTABILISEE])
    conflits = _ventes_lot12_du_mois(facture["proprietaire_id"], facture["mois"], db_path)
    if conflits:
        bloquants.append(f"vente déjà comptabilisée par l'ancien mécanisme ({', '.join(conflits)})")
    if round(float(facture.get("montant_total") or 0), 2) == 0:
        bloquants.append("montant de facture nul — rien à constater")

    numero = facture.get("numero_facture") or facture["facture_id_opaque"]
    client = _nom_tiers(facture["proprietaire_id"], db_path)
    lignes_ecr: list[dict[str, Any]] = []
    # Une écriture déjà PROPOSÉE (ancienne émission automatique) est celle qui sera validée :
    # c'est elle qu'on montre, pas un recalcul qui pourrait en différer.
    if etat["ecriture"] is not None:
        e = etat["ecriture"]
        libelle = e["libelle"]
        for l in lignes(e["ecriture_id_opaque"], db_path):
            lignes_ecr.append({"compte": l["compte"], "libelle": l["libelle"] or "",
                               "debit": l["debit"] or 0, "credit": l["credit"] or 0,
                               "auxiliaire": l["auxiliaire"]})
    else:
        prevue = ecriture_vente_prevue(facture, db_path=db_path)
        if prevue["ok"]:
            libelle = prevue["libelle"]
            lignes_ecr = [{k: l.get(k) for k in ("compte", "libelle", "debit", "credit",
                                                  "auxiliaire")} for l in prevue["lignes"]]
        else:
            # Le moteur refuse au premier manque : on reconstitue ligne à ligne, depuis les MÊMES
            # mappings, pour montrer précisément ce qui est à confirmer.
            from app.services import factures_proprietaires_service as fpr
            libelle = f"Facture {numero} — {client}"
            bloquants.append(prevue.get("message") or MESSAGES[E_PROPOSITION_INCOMPLETE])
            produits = mapping_produits(db_path=db_path)
            lignes_facture = facture.get("lignes")
            if lignes_facture is None:
                lignes_facture = fpr.lire(facture["facture_id_opaque"], db_path=db_path)["lignes"]
            total = 0.0
            for lf in lignes_facture:
                v = round(float(lf.get("montant") or 0), 2)
                total = round(total + v, 2)
                regle = produits.get(type_economique(lf))
                ok = (regle is not None and regle["famille"] != "ACOMPTE"
                      and _compte_valide(regle["compte"], db_path) is not None)
                lignes_ecr.append({"compte": regle["compte"] if ok else "",
                                   "libelle": f"{lf.get('libelle')} — Facture {numero}",
                                   "debit": -v if v < 0 else 0, "credit": v if v > 0 else 0,
                                   "auxiliaire": None})
            lignes_ecr.insert(0, {"compte": COMPTE_PROPRIETAIRES, "libelle": libelle,
                                  "debit": total if total > 0 else 0,
                                  "credit": -total if total < 0 else 0,
                                  "auxiliaire": facture["proprietaire_id"]})

    for l in lignes_ecr:
        l["a_confirmer"] = not l["compte"]
        l["libelle_compte"] = _libelle_compte(l["compte"], db_path) if l["compte"] else ""
        l["auxiliaire_nom"] = _nom_tiers(l["auxiliaire"], db_path) if l.get("auxiliaire") else ""
        l["est_tva"] = l["compte"] == COMPTE_TVA_COLLECTEE
    if any(l["a_confirmer"] for l in lignes_ecr) and \
            MESSAGES[E_PROPOSITION_INCOMPLETE] not in bloquants:
        bloquants.append(MESSAGES[E_PROPOSITION_INCOMPLETE])
    total_debit = round(sum(float(l["debit"] or 0) for l in lignes_ecr), 2)
    total_credit = round(sum(float(l["credit"] or 0) for l in lignes_ecr), 2)
    if total_debit != total_credit:
        bloquants.append(MESSAGES[E_DESEQUILIBRE])
    return {
        "ok": not bloquants, "bloquants": bloquants,
        "date_comptable": facture.get("date_facture") or f"{facture['mois']}-01",
        "periode": facture["mois"], "journal": "VENTES", "numero_facture": numero,
        "piece": numero, "client": client, "auxiliaire": client, "libelle": libelle,
        "lignes": lignes_ecr, "total_debit": total_debit, "total_credit": total_credit,
        "equilibree": total_debit == total_credit,
        "montant_total": round(float(facture.get("montant_total") or 0), 2),
        "tva": round(sum(float(l["credit"] or 0) - float(l["debit"] or 0)
                         for l in lignes_ecr if l["est_tva"]), 2),
        "ecriture_proposee": etat["ecriture"]["ecriture_id_opaque"] if etat["ecriture"] else None,
    }


def valider_comptabilisation_facture(facture_id: str, *, acteur: str = "",
                                     db_path=None) -> dict[str, Any]:
    """Validation HUMAINE explicite : crée (ou reprend) l'écriture VENTES et la passe VALIDEE.

    Idempotence : refus si l'écriture de la facture est déjà validée ; l'index unique
    `idx_ecritures_origine` interdit de toute façon deux écritures vivantes pour la même facture.
    Les imputations d'acomptes / crédits suivent la vente, exactement comme à l'ancienne émission
    automatique (`comptabiliser_facture_emise`)."""
    from app.services import factures_proprietaires_service as fpr
    facture = fpr.lire(facture_id, db_path=db_path)
    prop = proposition_comptabilisation_facture(facture, db_path=db_path)
    if etat_comptabilisation_facture(facture, db_path=db_path)["etat"] == COMPTA_COMPTABILISEE:
        return _refus(E_DEJA_COMPTABILISEE, facture_id)
    if not prop["ok"]:
        return {**_refus(E_PROPOSITION_INCOMPLETE, "; ".join(prop["bloquants"])),
                "message": "; ".join(prop["bloquants"])}
    res = comptabiliser_facture_emise(facture, acteur=acteur, db_path=db_path)
    vente = res["vente"]
    if not vente.get("ok"):
        return vente
    opaque = vente["ecriture_id_opaque"]
    e = charger(opaque, db_path)
    if e["statut"] == ST_PROPOSEE:
        v = valider(opaque, acteur=acteur, db_path=db_path)
        if not v.get("ok"):
            return v
    elif e["statut"] == ST_VALIDEE:
        return _refus(E_DEJA_COMPTABILISEE, facture_id)
    return {"ok": True, "ecriture_id_opaque": opaque, "details": res}


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
    # Une opération abandonnée ou déjà contrepassée n'a rien à comptabiliser. Le statut VALIDE
    # n'est PAS exigé ici : c'est `operations_caisse_service.valider()` qui appelle ce générateur
    # avant de poser le statut, précisément pour qu'une opération validée porte toujours son
    # écriture (si l'écriture est refusée, la validation l'est aussi).
    if op is None or op["statut"] in ("ANNULEE", "CONTREPASSEE"):
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
                 db_path=None, conn=None) -> dict[str, Any]:
    """Annule une écriture par une écriture MIROIR (débit/crédit inversés), jamais une suppression.

    `conn` fourni : le miroir s'écrit dans la transaction de l'appelant (même idiome que
    `_inserer_ecriture`)."""
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
    connexion_locale = conn is None
    if connexion_locale:
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
        if connexion_locale:
            conn.commit()
    finally:
        if connexion_locale:
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

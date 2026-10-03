"""Crédits clients — l'argent détenu pour un propriétaire, son origine et son usage (Mission 37).

DEUX FAMILLES, UN MÊME COMPTE (419100, tiers = propriétaire)
    REVERSEMENT AIRBNB  versement d'Airbnb reçu par la conciergerie pour le compte du propriétaire
                        (D032). Objet `credits_clients` : montant initial, origine, imputations
                        (`imputations_airbnb`, contrat Lot10 inchangé) choisies facture par facture.
    ACOMPTE             paiement du propriétaire avant facture : mouvement de trésorerie
                        propriétaire, imputé par le FIFO du compte propriétaire (règle métier : on
                        ne choisit pas la facture qu'un paiement solde). Présenté ici, pas recopié.

L'ORIGINE COMPTABLE N'EST JAMAIS INVENTÉE
    · Airbnb, mode BANQUE : le crédit attend son encaissement ; il est rapproché dans Flux du
      virement Airbnb réel → écriture 512 / 419100. Tant que ce n'est pas fait : il ne s'impute pas.
    · Airbnb, mode JUSTIFIÉ : pas de trace bancaire exploitable (encaissement antérieur à
      l'historique) → l'utilisateur nomme le compte source (jamais 512, 530, 411, 419) et justifie →
      écriture <compte source> / 419100, validée avec la création.
    · Acompte : son origine est l'écriture 512 / 419100 du rapprochement Flux, ou — s'il a été
      rapproché par l'ancien écran Qonto, qui n'écrivait rien — une régularisation explicite qui
      s'appuie sur ce rapprochement bancaire réel.
    · Imputation d'un reversement ancien sans crédit d'origine (données historiques) : À
      RÉGULARISER — on le rattache à un crédit dont l'origine est constatée ; sinon rien n'est écrit.
Une imputation sur une facture ÉMISE est constatée 419100 → 411000 (écriture proposée).

Aucune réservation n'est jamais reliée à un virement Airbnb : ce module n'en lit ni n'en crée.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from app.db.connection import get_db

ORIGINE_REVERSEMENT_AIRBNB = "REVERSEMENT_AIRBNB"
ORIGINE_ACOMPTE = "ACOMPTE"
# Solde créditeur dû par l'ancienne structure, repris par la nouvelle société (migration 0123).
ORIGINE_REPRISE_SOLDE = "REPRISE_SOLDE"
LIBELLE_REPRISE_SOLDE = "Solde créditeur repris de l'ancienne structure"
# Surplus d'un avoir émis au-delà de la créance restante (migration 0125).
ORIGINE_SURPLUS_AVOIR = "SURPLUS_AVOIR"
COMPTE_ANCIENNE_STRUCTURE = "467100"
COMPTE_PERTE_CREANCE = "654000"
MODE_BANQUE, MODE_JUSTIFIE = "BANQUE", "JUSTIFIE"
ST_EN_ATTENTE, ST_DISPONIBLE, ST_ANNULE = "EN_ATTENTE_ORIGINE", "DISPONIBLE", "ANNULE"
LIBELLES_ORIGINE = {ORIGINE_REVERSEMENT_AIRBNB: "Reversement Airbnb", ORIGINE_ACOMPTE: "Acompte",
                    ORIGINE_REPRISE_SOLDE: LIBELLE_REPRISE_SOLDE,
                    ORIGINE_SURPLUS_AVOIR: "Surplus d'avoir"}
LIBELLES_STATUT = {ST_EN_ATTENTE: "En attente de son encaissement bancaire",
                   ST_DISPONIBLE: "Disponible", ST_ANNULE: "Annulé"}

COMPTE_CREDITS = "419100"
# Crédit client qui n'est ni une avance ni un acompte (reprise de solde) : 4197 « Clients, autres
# avoirs » — le PCG réserve 4191 aux avances et acomptes reçus (migration 0124).
COMPTE_AUTRES_AVOIRS = "419700"


def compte_du_credit(origine: str) -> str:
    """Compte de tiers qui porte un crédit client, selon son origine."""
    return COMPTE_AUTRES_AVOIRS if origine in ("REPRISE_SOLDE", "SURPLUS_AVOIR") \
        else COMPTE_CREDITS
COMPTE_CLIENTS = "411000"
COMPTES_SOURCE_INTERDITS = ("512", "530", "411", "419")

E_PROPRIETAIRE = "CR01_PROPRIETAIRE_INCONNU"
E_MONTANT = "CR02_MONTANT_INVALIDE"
E_DATE = "CR03_DATE_INVALIDE"
E_MODE = "CR04_MODE_INCONNU"
E_COMPTE_SOURCE = "CR05_COMPTE_SOURCE_INVALIDE"
E_JUSTIFICATION = "CR06_JUSTIFICATION_OBLIGATOIRE"
E_INTROUVABLE = "CR07_CREDIT_INTROUVABLE"
E_SANS_ORIGINE = "CR08_CREDIT_SANS_ORIGINE"
E_RESTE = "CR09_DEPASSE_LE_RESTE_DU_CREDIT"
E_FACTURE = "CR10_FACTURE_INCOMPATIBLE"
E_SOLDE_FACTURE = "CR11_DEPASSE_LE_SOLDE_DE_LA_FACTURE"
E_ACTEUR = "CR12_AUTEUR_OBLIGATOIRE"
E_DEJA = "CR13_DEJA_REGULARISE"
E_ECRITURE = "CR14_ECRITURE_REFUSEE"
E_ORIGINE_ACOMPTE = "CR15_ORIGINE_ACOMPTE_IMPOSSIBLE"
E_NON_EMISE = "CR16_FACTURE_NON_EMISE"
E_REPRISE_EXISTANTE = "CR17_REPRISE_DEJA_ENREGISTREE"
EPS = 0.005


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _r(v: Any) -> float:
    return round(float(v or 0), 2)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _date_fr(d: Any) -> str:
    t = _txt(d)[:10]
    return f"{t[8:10]}/{t[5:7]}/{t[:4]}" if len(t) == 10 else t


def _evenement(conn, credit_id: str, type_evt: str, *, acteur: str, montant=None,
               facture_id=None, ecriture_id=None, detail: str = "") -> None:
    conn.execute("INSERT INTO credit_client_evenements (credit_id_opaque, type_evenement, montant, "
                 "facture_id, ecriture_id, detail, acteur) VALUES (?,?,?,?,?,?,?)",
                 (credit_id, type_evt, montant, facture_id, ecriture_id, detail or None,
                  acteur or "local"))


# ══ Lecture ══════════════════════════════════════════════════════════════════════════════════════

def charger(credit_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM credits_clients WHERE credit_id_opaque=?",
                         (_txt(credit_id),)).fetchone()
        if r is None:
            return None
        c = dict(r)
        imputations = [dict(x) for x in conn.execute(
            "SELECT * FROM imputations_airbnb WHERE credit_id_opaque=? AND "
            "UPPER(COALESCE(statut,'VALIDE')) IN ('VALIDE','VALIDEE') ORDER BY date_imputation, "
            "imputation_airbnb_id", (c["credit_id_opaque"],))]
        historique = [dict(x) for x in conn.execute(
            "SELECT * FROM credit_client_evenements WHERE credit_id_opaque=? ORDER BY id",
            (c["credit_id_opaque"],))]
    finally:
        conn.close()
    utilise = _r(sum(_r(i["montant_impute"]) for i in imputations))
    c.update(imputations=imputations, historique=historique, utilise=utilise,
             reste=_r(c["montant_initial"] - utilise),
             libelle_origine=LIBELLES_ORIGINE[c["origine"]],
             libelle_statut=LIBELLES_STATUT[c["statut"]])
    return c


def lister(*, proprietaire_id: str = "", statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        sql, params = "SELECT credit_id_opaque FROM credits_clients WHERE 1=1", []
        if proprietaire_id:
            sql += " AND proprietaire_id=?"
            params.append(proprietaire_id)
        if statut:
            sql += " AND statut=?"
            params.append(statut)
        ids = [r[0] for r in conn.execute(sql + " ORDER BY date_origine, id", params)]
    finally:
        conn.close()
    return [charger(i, db_path=db_path) for i in ids]


def a_regulariser(*, proprietaire_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Reversements Airbnb imputés SANS crédit d'origine (données antérieures) : à régulariser."""
    conn = get_db(db_path)
    try:
        sql = ("SELECT * FROM imputations_airbnb WHERE credit_id_opaque IS NULL AND "
               "UPPER(COALESCE(statut,'VALIDE')) IN ('VALIDE','VALIDEE')")
        params: list[Any] = []
        if proprietaire_id:
            sql += " AND proprietaire_id=?"
            params.append(proprietaire_id)
        return [dict(r) for r in conn.execute(sql + " ORDER BY date_imputation", params)]
    finally:
        conn.close()


# ══ Création ═════════════════════════════════════════════════════════════════════════════════════

def creer_reversement_airbnb(proprietaire_id: str, montant: Any, date_origine: str, *,
                             reference: str = "", mode: str = MODE_BANQUE, compte_source: str = "",
                             auxiliaire_source: str = "", justification: str = "", acteur: str,
                             db_path=None) -> dict[str, Any]:
    """Déclare un reversement Airbnb reçu pour un propriétaire.

    BANQUE : le crédit attend d'être rapproché, dans Flux, du virement Airbnb réel (aucune
    écriture maintenant). JUSTIFIÉ : écriture <compte source> / 419100, validée avec la création
    (c'est l'utilisateur qui constate et justifie l'événement ; la trésorerie en est exclue)."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import comptabilite_plan_service as plan
    from app.services import flux_financiers_service as flux

    acteur, pid, mode = _txt(acteur), _txt(proprietaire_id), _txt(mode).upper() or MODE_BANQUE
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom : le crédit est tracé.")
    if pid not in {p["id"] for p in flux.proprietaires_connus(db_path=db_path)}:
        return _refus(E_PROPRIETAIRE, "Propriétaire inconnu du référentiel.")
    try:
        valeur = _r(str(montant).replace(",", ".").replace(" ", ""))
    except ValueError:
        valeur = 0.0
    if valeur <= 0:
        return _refus(E_MONTANT, "Le montant doit être strictement positif.")
    try:
        date.fromisoformat(_txt(date_origine)[:10])
    except ValueError:
        return _refus(E_DATE, "Date d'origine invalide (AAAA-MM-JJ).")
    if mode not in (MODE_BANQUE, MODE_JUSTIFIE):
        return _refus(E_MODE, "Origine : encaissement bancaire ou justifiée.")
    compte_source = _txt(compte_source)
    if mode == MODE_JUSTIFIE:
        if not _txt(justification):
            return _refus(E_JUSTIFICATION, "Sans encaissement bancaire rapproché, une "
                                           "justification écrite est obligatoire.")
        ligne_plan = plan.charger(compte_source, db_path=db_path) if compte_source else None
        if (ligne_plan is None or not ligne_plan["actif"]
                or compte_source.startswith(COMPTES_SOURCE_INTERDITS)):
            return _refus(E_COMPTE_SOURCE, "Choisissez le compte d'où vient ce crédit (actif). La "
                                           "banque et la caisse se prouvent par un rapprochement "
                                           "dans Flux ; 411 et 419 ne peuvent pas être sources.")
        if not compta._flags_actifs():
            return _refus(E_ECRITURE, compta.MESSAGES[compta.E_FLAGS])

    credit_id = "CRD-" + uuid.uuid4().hex[:12].upper()
    jour = _txt(date_origine)[:10]
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO credits_clients (credit_id_opaque, proprietaire_id, origine, mode_origine, "
            "date_origine, mois, montant_initial, reference, justification, compte_source, statut, "
            "cree_par) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (credit_id, pid, ORIGINE_REVERSEMENT_AIRBNB, mode, jour, jour[:7], valeur,
             _txt(reference) or None, _txt(justification) or None,
             compte_source if mode == MODE_JUSTIFIE else None, ST_EN_ATTENTE, acteur))
        _evenement(conn, credit_id, "CREATION", acteur=acteur, montant=valeur,
                   detail=f"{LIBELLES_ORIGINE[ORIGINE_REVERSEMENT_AIRBNB]} — origine {mode}")
        if mode == MODE_JUSTIFIE:
            res = compta._inserer_ecriture(
                "ODIVERSES", jour, jour[:7], credit_id,
                f"Reversement Airbnb détenu pour le propriétaire — {_txt(reference) or credit_id}",
                "CREDIT_CLIENT", credit_id,
                [{"compte": compte_source, "debit": valeur, "credit": 0,
                  "auxiliaire": _txt(auxiliaire_source) or None,
                  "libelle": f"Origine justifiée : {_txt(justification)}"[:250]},
                 {"compte": COMPTE_CREDITS, "debit": 0, "credit": valeur, "auxiliaire": pid,
                  "proprietaire_id": pid, "libelle": "Crédit client — reversement Airbnb"}],
                acteur=acteur, db_path=db_path, conn=conn)
            if not res.get("ok"):
                conn.rollback()
                return _refus(E_ECRITURE, f"{res.get('message')} {res.get('detail', '')}".strip())
            compta.valider_dans_transaction(conn, res["ecriture_id_opaque"],
                                            commentaire="Validée avec la création du crédit",
                                            acteur=acteur)
            conn.execute("UPDATE credits_clients SET statut=?, ecriture_origine=?, "
                         "version=version+1 WHERE credit_id_opaque=?",
                         (ST_DISPONIBLE, res["ecriture_id_opaque"], credit_id))
            _evenement(conn, credit_id, "ORIGINE_CONSTATEE", acteur=acteur, montant=valeur,
                       ecriture_id=res["ecriture_id_opaque"],
                       detail=f"Origine justifiée (compte {compte_source}) : {_txt(justification)}")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "credit_id_opaque": credit_id,
            "statut": ST_DISPONIBLE if mode == MODE_JUSTIFIE else ST_EN_ATTENTE}


# ══ Origine bancaire (appelé par le lettrage Flux, dans SA transaction) ══════════════════════════

def en_attente_de_banque(*, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM credits_clients WHERE statut=? AND mode_origine=?",
            (ST_EN_ATTENTE, MODE_BANQUE))]
    finally:
        conn.close()


def constater_origine_bancaire(conn, credit_id: str, *, mouvement_id: str, lettrage: str,
                               ecriture_id: str, acteur: str) -> None:
    conn.execute("UPDATE credits_clients SET statut=?, mouvement_origine=?, lettrage_origine=?, "
                 "ecriture_origine=?, version=version+1 WHERE credit_id_opaque=? AND statut=?",
                 (ST_DISPONIBLE, mouvement_id, lettrage, ecriture_id, credit_id, ST_EN_ATTENTE))
    _evenement(conn, credit_id, "ORIGINE_CONSTATEE", acteur=acteur, ecriture_id=ecriture_id,
               detail=f"Encaissement bancaire rapproché ({lettrage})")


def retirer_origine_bancaire(conn, credit_id: str, *, lettrage: str, acteur: str) -> str:
    """Annulation du lettrage d'origine : refusée si le crédit a déjà servi. Retourne un motif de
    refus, ou « » si le crédit est revenu EN_ATTENTE_ORIGINE."""
    utilise = conn.execute("SELECT COUNT(*) FROM imputations_airbnb WHERE credit_id_opaque=?",
                           (credit_id,)).fetchone()[0]
    if utilise:
        return ("Le crédit issu de ce virement a déjà été imputé sur une facture : annulez d'abord "
                "ces imputations.")
    conn.execute("UPDATE credits_clients SET statut=?, mouvement_origine=NULL, lettrage_origine=NULL, "
                 "ecriture_origine=NULL, version=version+1 WHERE credit_id_opaque=?",
                 (ST_EN_ATTENTE, credit_id))
    _evenement(conn, credit_id, "ORIGINE_RETIREE", acteur=acteur,
               detail=f"Rapprochement {lettrage} annulé")
    return ""


# ══ Imputation sur une facture ═══════════════════════════════════════════════════════════════════

def _facture_et_solde(facture_id: str, db_path=None) -> tuple[dict | None, float]:
    from app.services import factures_proprietaires_service as fpr
    try:
        f = fpr.lire(facture_id, db_path=db_path)
    except Exception:      # noqa: BLE001 — facture introuvable : refus lisible plus bas
        return None, 0.0
    if f["statut"] == fpr.ST_EMIS:
        from app.services import compte_proprietaire_service as cpt
        imput = cpt.imputations_detail(facture_id, db_path=db_path)
        return f, _r(f["montant_total"] - imput["total"])
    return f, _r(fpr.solde(facture_id, db_path=db_path)["solde"])


def imputer(credit_id: str, facture_id: str, montant: Any, *, acteur: str,
            db_path=None) -> dict[str, Any]:
    """Impute une part d'un reversement Airbnb DISPONIBLE sur une facture du même propriétaire.

    Plafonds : le reste du crédit, et le solde de la facture. Effet : une ligne
    `imputations_airbnb` reliée au crédit (le solde de la facture et Lot10 la voient) ; si la
    facture est émise, l'écriture 419100 → 411000 est proposée aussitôt, sinon à l'émission."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import factures_proprietaires_service as fpr

    if not _txt(acteur):
        return _refus(E_ACTEUR, "Indiquez votre nom : l'imputation est tracée.")
    c = charger(credit_id, db_path=db_path)
    if c is None:
        return _refus(E_INTROUVABLE, "Crédit introuvable.")
    if c["statut"] != ST_DISPONIBLE:
        return _refus(E_SANS_ORIGINE, "Ce crédit n'a pas encore d'origine comptable : rapprochez "
                                      "d'abord le virement Airbnb dans Flux (ou justifiez son "
                                      "origine). Rien n'est imputé sans elle.")
    try:
        valeur = _r(str(montant).replace(",", ".").replace(" ", ""))
    except ValueError:
        valeur = 0.0
    if valeur <= 0:
        return _refus(E_MONTANT, "Le montant doit être strictement positif.")
    if valeur > c["reste"] + EPS:
        return _refus(E_RESTE, f"Il ne reste que {c['reste']:.2f} € sur ce crédit.")
    f, solde = _facture_et_solde(facture_id, db_path)
    if (f is None or f["proprietaire_id"] != c["proprietaire_id"]
            or f["type_document"] != fpr.TYPE_FACTURE or f["statut"] == fpr.ST_ANNULE):
        return _refus(E_FACTURE, "Facture introuvable, annulée, avoir, ou d'un autre propriétaire.")
    # BROUILLON = AUCUN IMPACT : un crédit ne se consomme que sur une facture ÉMISE (et en compta).
    if f["statut"] != fpr.ST_EMIS or int(f.get("hors_compta") or 0):
        return _refus(E_NON_EMISE, "Un crédit ne s'impute que sur une facture émise (en "
                                   "comptabilité). Un brouillon n'a aucun impact sur le compte "
                                   "client.")
    if valeur > solde + EPS:
        return _refus(E_SOLDE_FACTURE, f"La facture ne doit plus que {solde:.2f} €.")

    imputation_id = "IMPA-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO imputations_airbnb (imputation_airbnb_id, transaction_banque_id, "
            "reference_airbnb, proprietaire_id, logement_id, mois, document_id, montant_impute, "
            "date_imputation, justificatif, statut, commentaire, credit_id_opaque) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (imputation_id, c["mouvement_origine"] or c["ecriture_origine"], c["reference"],
             c["proprietaire_id"], f["logement_id"], f["mois"], facture_id, valeur,
             date.today().isoformat(), c["credit_id_opaque"], "VALIDE",
             f"Imputation du crédit {c['credit_id_opaque']}", c["credit_id_opaque"]))
        _evenement(conn, c["credit_id_opaque"], "IMPUTATION", acteur=acteur, montant=valeur,
                   facture_id=facture_id,
                   detail=f"Facture {f.get('numero_facture') or 'brouillon'}")
        fpr._journal(conn, facture_id, fpr.EVT_AJOUT_REVERSEMENT_AIRBNB, f["statut"], f["statut"],
                     f"reversement Airbnb {valeur:.2f} imputé depuis le crédit "
                     f"{c['credit_id_opaque']}", acteur)
        conn.commit()
    finally:
        conn.close()
    ecriture = None
    if f["statut"] == fpr.ST_EMIS:
        ecriture = compta.generer_ecriture_imputation_credit(imputation_id, acteur=acteur,
                                                             db_path=db_path)
    return {"ok": True, "imputation_airbnb_id": imputation_id, "ecriture": ecriture,
            "reste": _r(c["reste"] - valeur)}


def regulariser(imputation_id: str, credit_id: str, *, acteur: str,
                db_path=None) -> dict[str, Any]:
    """Rattache un reversement historique SANS origine à un crédit dont l'origine est constatée.
    Rien d'autre n'est modifié sur l'imputation ; l'écriture 419100 → 411000 suit si la facture
    est émise."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import factures_proprietaires_service as fpr

    if not _txt(acteur):
        return _refus(E_ACTEUR, "Indiquez votre nom : la régularisation est tracée.")
    conn = get_db(db_path)
    try:
        imp = conn.execute("SELECT * FROM imputations_airbnb WHERE imputation_airbnb_id=?",
                           (imputation_id,)).fetchone()
    finally:
        conn.close()
    if imp is None:
        return _refus(E_INTROUVABLE, "Reversement introuvable.")
    if imp["credit_id_opaque"]:
        return _refus(E_DEJA, "Ce reversement est déjà rattaché à un crédit.")
    c = charger(credit_id, db_path=db_path)
    if c is None or c["proprietaire_id"] != imp["proprietaire_id"]:
        return _refus(E_INTROUVABLE, "Crédit introuvable ou d'un autre propriétaire.")
    if c["statut"] != ST_DISPONIBLE:
        return _refus(E_SANS_ORIGINE, "Ce crédit n'a pas encore d'origine comptable.")
    if _r(imp["montant_impute"]) > c["reste"] + EPS:
        return _refus(E_RESTE, f"Il ne reste que {c['reste']:.2f} € sur ce crédit.")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE imputations_airbnb SET credit_id_opaque=? WHERE imputation_airbnb_id=? "
                     "AND credit_id_opaque IS NULL", (c["credit_id_opaque"], imputation_id))
        _evenement(conn, c["credit_id_opaque"], "REGULARISATION", acteur=acteur,
                   montant=_r(imp["montant_impute"]), facture_id=imp["document_id"],
                   detail=f"Reversement historique {imputation_id} rattaché")
        conn.commit()
    finally:
        conn.close()
    ecriture = None
    try:
        f = fpr.lire(imp["document_id"], db_path=db_path) if imp["document_id"] else None
    except Exception:      # noqa: BLE001
        f = None
    if f and f["statut"] == fpr.ST_EMIS:
        ecriture = compta.generer_ecriture_imputation_credit(imputation_id, acteur=acteur,
                                                             db_path=db_path)
    return {"ok": True, "ecriture": ecriture}


# ══ Reprise d'un solde créditeur de l'ancienne structure ══════════════════════════════════════════

def creer_reprise_solde(proprietaire_id: str, montant: Any, date_origine: str, *, acteur: str,
                        libelle: str = LIBELLE_REPRISE_SOLDE, db_path=None) -> dict[str, Any]:
    """Crédit client DISPONIBLE repris de l'ancienne structure, et ses deux écritures validées :

        1. reprise de la dette envers le client   467100 D  /  419700 C (auxiliaire = client)
        2. créance sur l'ancienne structure perdue 654000 D  /  467100 C

    467100 est soldé, la perte est constatée en charge, le client reste créditeur. Ce n'est ni un
    encaissement, ni une facture, ni une réduction, ni un acompte, ni un mouvement bancaire.
    Une seule reprise par client (idempotence)."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import flux_financiers_service as flux

    acteur, pid = _txt(acteur), _txt(proprietaire_id)
    if not acteur:
        return _refus(E_ACTEUR, "Indiquez votre nom : la reprise est tracée.")
    if pid not in {p["id"] for p in flux.proprietaires_connus(db_path=db_path)}:
        return _refus(E_PROPRIETAIRE, "Propriétaire inconnu du référentiel.")
    try:
        valeur = _r(str(montant).replace(",", ".").replace(" ", ""))
    except ValueError:
        valeur = 0.0
    if valeur <= 0:
        return _refus(E_MONTANT, "Le montant doit être strictement positif.")
    jour = _txt(date_origine)[:10]
    try:
        date.fromisoformat(jour)
    except ValueError:
        return _refus(E_DATE, "Date d'origine invalide (AAAA-MM-JJ).")
    if not compta._flags_actifs():
        return _refus(E_ECRITURE, compta.MESSAGES[compta.E_FLAGS])

    credit_id = "CRD-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM credits_clients WHERE proprietaire_id=? AND origine=? "
                        "AND statut<>?", (pid, ORIGINE_REPRISE_SOLDE, ST_ANNULE)).fetchone():
            conn.rollback()
            return _refus(E_REPRISE_EXISTANTE, "Une reprise de solde existe déjà pour ce client.")
        conn.execute(
            "INSERT INTO credits_clients (credit_id_opaque, proprietaire_id, origine, mode_origine, "
            "date_origine, mois, montant_initial, reference, justification, compte_source, statut, "
            "cree_par) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (credit_id, pid, ORIGINE_REPRISE_SOLDE, MODE_JUSTIFIE, jour, jour[:7], valeur,
             _txt(libelle) or LIBELLE_REPRISE_SOLDE, LIBELLE_REPRISE_SOLDE,
             COMPTE_ANCIENNE_STRUCTURE, ST_DISPONIBLE, acteur))
        _evenement(conn, credit_id, "CREATION", acteur=acteur, montant=valeur,
                   detail=_txt(libelle) or LIBELLE_REPRISE_SOLDE)
        ecritures = []
        for piece, texte, lignes in (
                (f"{credit_id}-REPRISE", f"{LIBELLE_REPRISE_SOLDE} — reprise de la dette client",
                 [{"compte": COMPTE_ANCIENNE_STRUCTURE, "debit": valeur, "credit": 0,
                   "libelle": "Ancienne structure — solde client repris"},
                  {"compte": COMPTE_AUTRES_AVOIRS, "debit": 0, "credit": valeur, "auxiliaire": pid,
                   "proprietaire_id": pid, "libelle": LIBELLE_REPRISE_SOLDE}]),
                (f"{credit_id}-PERTE", "Créance sur l'ancienne structure considérée perdue",
                 [{"compte": COMPTE_PERTE_CREANCE, "debit": valeur, "credit": 0,
                   "libelle": "Perte sur créance irrécouvrable — ancienne structure"},
                  {"compte": COMPTE_ANCIENNE_STRUCTURE, "debit": 0, "credit": valeur,
                   "libelle": "Ancienne structure — créance abandonnée"}])):
            res = compta._inserer_ecriture("ODIVERSES", jour, jour[:7], piece, texte,
                                           "CREDIT_CLIENT", piece, lignes, acteur=acteur,
                                           db_path=db_path, conn=conn)
            if not res.get("ok"):
                conn.rollback()
                return _refus(E_ECRITURE, f"{res.get('message')} {res.get('detail', '')}".strip())
            compta.valider_dans_transaction(conn, res["ecriture_id_opaque"],
                                            commentaire="Validée avec la reprise du solde",
                                            acteur=acteur)
            ecritures.append(res["ecriture_id_opaque"])
        conn.execute("UPDATE credits_clients SET ecriture_origine=? WHERE credit_id_opaque=?",
                     (ecritures[0], credit_id))
        _evenement(conn, credit_id, "ORIGINE_CONSTATEE", acteur=acteur, montant=valeur,
                   ecriture_id=ecritures[0],
                   detail=f"467100 / 419700 puis 654000 / 467100 ({', '.join(ecritures)})")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "credit_id_opaque": credit_id, "ecritures": ecritures,
            "statut": ST_DISPONIBLE}


def reclasser_reprise_vers_419700(credit_id: str, *, acteur: str, db_path=None) -> dict[str, Any]:
    """Reprise de solde comptabilisée en 419100 avant la migration 0124 : reclassement
    419100 D / 419700 C (auxiliaire = client), validé. Le crédit lui-même (montant, reste,
    imputations) n'est pas touché. Idempotent."""
    from app.services import comptabilite_ecritures_service as compta
    c = charger(credit_id, db_path=db_path)
    if c is None or c["origine"] != ORIGINE_REPRISE_SOLDE:
        return _refus(E_INTROUVABLE, "Reprise de solde introuvable.")
    conn = get_db(db_path)
    try:
        sur_419100 = _r(conn.execute(
            "SELECT COALESCE(SUM(l.credit),0) - COALESCE(SUM(l.debit),0) FROM ecriture_lignes l "
            "JOIN ecritures e ON e.ecriture_id_opaque = l.ecriture_id_opaque "
            "WHERE e.statut <> 'ANNULEE' AND l.compte = ? AND l.auxiliaire = ? "
            "AND (e.origine_id_opaque LIKE ? OR e.ecriture_id_opaque = ?)",
            (COMPTE_CREDITS, c["proprietaire_id"], f"{credit_id}%",
             c["ecriture_origine"] or "")).fetchone()[0])
        if sur_419100 <= EPS:
            return {"ok": True, "deja_reclasse": True}
        piece = f"{credit_id}-RECLASSEMENT"
        conn.execute("BEGIN IMMEDIATE")
        res = compta._inserer_ecriture(
            "ODIVERSES", c["date_origine"], c["mois"], piece,
            f"{LIBELLE_REPRISE_SOLDE} — reclassement 4191 → 4197 (autres avoirs)",
            "CREDIT_CLIENT", piece,
            [{"compte": COMPTE_CREDITS, "debit": sur_419100, "credit": 0,
              "auxiliaire": c["proprietaire_id"], "proprietaire_id": c["proprietaire_id"],
              "libelle": "Reclassement : ce crédit n'est ni une avance ni un acompte"},
             {"compte": COMPTE_AUTRES_AVOIRS, "debit": 0, "credit": sur_419100,
              "auxiliaire": c["proprietaire_id"], "proprietaire_id": c["proprietaire_id"],
              "libelle": LIBELLE_REPRISE_SOLDE}], acteur=acteur, db_path=db_path, conn=conn)
        if not res.get("ok"):
            conn.rollback()
            return _refus(E_ECRITURE, f"{res.get('message')} {res.get('detail', '')}".strip())
        compta.valider_dans_transaction(conn, res["ecriture_id_opaque"],
                                        commentaire="Reclassement 419100 → 419700", acteur=acteur)
        # Vocabulaire d'événements contraint (0115) : l'origine comptable est re-constatée sur 419700.
        _evenement(conn, credit_id, "ORIGINE_CONSTATEE", acteur=acteur, montant=sur_419100,
                   ecriture_id=res["ecriture_id_opaque"],
                   detail="Reclassement 419100 → 419700 (4197, autres avoirs)")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ecriture_id_opaque": res["ecriture_id_opaque"], "montant": sur_419100}


# ══ Surplus d'un avoir émis → crédit client ═════════════════════════════════════════════════════

def convertir_surplus_avoir(avoir_id: str, *, acteur: str, db_path=None) -> dict[str, Any]:
    """Avoir ÉMIS qui dépasse la créance restante : le surplus quitte le 411 et devient un crédit
    client canonique — écriture validée 411000 D / 419700 C (auxiliaire = client) et crédit
    DISPONIBLE au registre (origine SURPLUS_AVOIR, référence = l'avoir), imputable
    automatiquement à la prochaine émission. Le surplus = ce que le compte client n'a pas pu
    imputer de cet avoir sur les factures émises. Idempotent (un crédit par avoir)."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import compte_proprietaire_service as cpt
    from app.services import factures_proprietaires_service as fpr

    a = fpr.lire(avoir_id, db_path=db_path)
    if a["type_document"] != fpr.TYPE_AVOIR or a["statut"] != fpr.ST_EMIS \
            or int(a.get("hors_compta") or 0):
        return {"ok": True, "sans_objet": True}
    pid = a["proprietaire_id"]
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM credits_clients WHERE origine=? AND reference=?",
                        (ORIGINE_SURPLUS_AVOIR, avoir_id)).fetchone():
            return {"ok": True, "deja_converti": True}
    finally:
        conn.close()
    source = next((s for s in cpt.position(pid, db_path=db_path)["sources"]
                   if s["source_ref"] == avoir_id), None)
    surplus = _r(source["disponible"]) if source else 0.0
    if surplus <= EPS:
        return {"ok": True, "surplus": 0.0}
    if not compta._flags_actifs():
        return _refus(E_ECRITURE, compta.MESSAGES[compta.E_FLAGS])

    numero = a.get("numero_facture") or avoir_id
    jour = (a.get("date_facture") or f"{a['mois']}-01")[:10]
    credit_id = "CRD-" + uuid.uuid4().hex[:12].upper()
    piece = f"{numero}-SURPLUS"
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO credits_clients (credit_id_opaque, proprietaire_id, origine, mode_origine, "
            "date_origine, mois, montant_initial, reference, justification, compte_source, statut, "
            "cree_par) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (credit_id, pid, ORIGINE_SURPLUS_AVOIR, MODE_JUSTIFIE, jour, jour[:7], surplus,
             avoir_id, f"Surplus de l'avoir {numero} au-delà de la créance restante",
             COMPTE_CLIENTS, ST_DISPONIBLE, acteur or "local"))
        _evenement(conn, credit_id, "CREATION", acteur=acteur, montant=surplus,
                   facture_id=avoir_id, detail=f"Surplus de l'avoir {numero}")
        res = compta._inserer_ecriture(
            "ODIVERSES", jour, jour[:7], piece, f"Surplus de l'avoir {numero} — crédit client",
            "CREDIT_CLIENT", piece,
            [{"compte": COMPTE_CLIENTS, "debit": surplus, "credit": 0, "auxiliaire": pid,
              "proprietaire_id": pid, "libelle": f"Avoir {numero} : surplus reclassé en crédit"},
             {"compte": COMPTE_AUTRES_AVOIRS, "debit": 0, "credit": surplus, "auxiliaire": pid,
              "proprietaire_id": pid, "libelle": f"Crédit client — surplus de l'avoir {numero}"}],
            acteur=acteur, db_path=db_path, conn=conn)
        if not res.get("ok"):
            conn.rollback()
            return _refus(E_ECRITURE, f"{res.get('message')} {res.get('detail', '')}".strip())
        compta.valider_dans_transaction(conn, res["ecriture_id_opaque"],
                                        commentaire="Surplus d'avoir reclassé en crédit client",
                                        acteur=acteur)
        conn.execute("UPDATE credits_clients SET ecriture_origine=? WHERE credit_id_opaque=?",
                     (res["ecriture_id_opaque"], credit_id))
        _evenement(conn, credit_id, "ORIGINE_CONSTATEE", acteur=acteur, montant=surplus,
                   ecriture_id=res["ecriture_id_opaque"], detail="411000 → 419700")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "credit_id_opaque": credit_id, "surplus": surplus,
            "ecriture_id_opaque": res["ecriture_id_opaque"]}


# ══ Imputation automatique à l'émission ═════════════════════════════════════════════════════════

def credits_disponibles(conn, proprietaire_id: str) -> list[dict[str, Any]]:
    """Crédits DISPONIBLES du client et leur reste, du plus ancien au plus récent (même connexion
    que l'appelant : utilisé DANS la transaction d'émission)."""
    out = []
    for c in conn.execute("SELECT credit_id_opaque, montant_initial, origine, reference, "
                          "mouvement_origine, ecriture_origine, date_origine "
                          "FROM credits_clients WHERE proprietaire_id=? AND statut=? "
                          "ORDER BY date_origine, id", (proprietaire_id, ST_DISPONIBLE)):
        utilise = conn.execute(
            "SELECT COALESCE(SUM(montant_impute),0) FROM imputations_airbnb WHERE credit_id_opaque=? "
            "AND UPPER(COALESCE(statut,'VALIDE')) IN ('VALIDE','VALIDEE')",
            (c["credit_id_opaque"],)).fetchone()[0]
        reste = _r(_r(c["montant_initial"]) - _r(utilise))
        if reste > EPS:
            out.append({**dict(c), "reste": reste})
    return out


def imputer_a_l_emission(conn, facture: dict[str, Any], *, a_couvrir: float,
                         acteur: str) -> list[dict[str, Any]]:
    """DANS la transaction d'émission : impute les crédits disponibles du client sur la facture,
    du plus ancien au plus récent, jamais au-delà de `a_couvrir` (la créance). Le reliquat reste
    sur chaque crédit. Le montant de la facture n'est jamais modifié. Rend les imputations créées
    (l'écriture 419100 → 411000 est générée après la validation de l'émission)."""
    from app.services import factures_proprietaires_service as fpr
    restant = _r(a_couvrir)
    faites = []
    for c in credits_disponibles(conn, facture["proprietaire_id"]):
        if restant <= EPS:
            break
        montant = _r(min(c["reste"], restant))
        imputation_id = "IMPA-" + uuid.uuid4().hex[:12].upper()
        conn.execute(
            "INSERT INTO imputations_airbnb (imputation_airbnb_id, transaction_banque_id, "
            "reference_airbnb, proprietaire_id, logement_id, mois, document_id, montant_impute, "
            "date_imputation, justificatif, statut, commentaire, credit_id_opaque) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (imputation_id, c["mouvement_origine"] or c["ecriture_origine"], c["reference"],
             facture["proprietaire_id"], facture["logement_id"], facture["mois"],
             facture["facture_id_opaque"], montant, date.today().isoformat(),
             c["credit_id_opaque"], "VALIDE",
             f"Crédit client imputé à l'émission ({LIBELLES_ORIGINE.get(c['origine'], c['origine'])})",
             c["credit_id_opaque"]))
        _evenement(conn, c["credit_id_opaque"], "IMPUTATION", acteur=acteur, montant=montant,
                   facture_id=facture["facture_id_opaque"],
                   detail=f"Imputé automatiquement à l'émission — reste {c['reste'] - montant:.2f} €")
        fpr._journal(conn, facture["facture_id_opaque"], "IMPUTATION_CREDIT_CLIENT", None, None,
                     f"crédit client {c['credit_id_opaque']} imputé {montant:.2f} €", acteur)
        faites.append({"imputation_airbnb_id": imputation_id, "credit_id_opaque": c["credit_id_opaque"],
                       "montant": montant, "reste_credit": _r(c["reste"] - montant)})
        restant = _r(restant - montant)
    return faites


# ══ Acomptes : origine comptable et vue ══════════════════════════════════════════════════════════

def ecriture_origine_acompte(mouvement_opaque: str, *, db_path=None) -> str:
    """Écriture qui a porté l'acompte en 419100 : rapprochement Flux, ou régularisation d'un
    rapprochement bancaire de l'ancien écran Qonto. « » s'il n'en a aucune."""
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT t.ecriture_id_opaque FROM flux_lettrage_lignes l JOIN flux_lettrages t "
            "ON t.lettrage_id_opaque=l.lettrage_id_opaque WHERE t.statut='VALIDE' "
            "AND l.cote='OBJET' AND l.type_element='MOUVEMENT_PROPRIETAIRE' AND l.element_id=?",
            (mouvement_opaque,)).fetchone()
        candidats = [x for x in (r[0] or "").split(",") if x] if r else []
        r2 = conn.execute("SELECT ecriture_id_opaque FROM ecritures WHERE origine_type='ORIGINE_ACOMPTE' "
                          "AND origine_id_opaque=? AND statut <> 'CONTREPASSEE'",
                          (mouvement_opaque,)).fetchone()
        if r2:
            candidats.append(r2[0])
        for e in candidats:
            if conn.execute("SELECT 1 FROM ecriture_lignes WHERE ecriture_id_opaque=? AND compte=? "
                            "AND credit > 0", (e, COMPTE_CREDITS)).fetchone():
                return e
        return ""
    finally:
        conn.close()


def comptabiliser_encaissement_acompte(mouvement_opaque: str, *, acteur: str,
                                       db_path=None) -> dict[str, Any]:
    """Acompte rapproché d'un encaissement bancaire par l'ancien écran Qonto, qui n'écrivait rien :
    constate 512 / 419100 à partir de ce rapprochement réel (aucun autre cas n'est accepté)."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import proprietaires_tresorerie_service as tres

    if not _txt(acteur):
        return _refus(E_ACTEUR, "Indiquez votre nom : la régularisation est tracée.")
    m = tres.charger(mouvement_opaque, db_path)
    if (m is None or m["statut"] != "VALIDE" or m["nature"] != "ACOMPTE_PROPRIETAIRE"
            or m["sens"] != "PROPRIETAIRE_VERS_SOCIETE"):
        return _refus(E_ORIGINE_ACOMPTE, "Acompte validé introuvable.")
    if ecriture_origine_acompte(mouvement_opaque, db_path=db_path):
        return _refus(E_DEJA, "Cet acompte a déjà son écriture d'origine.")
    conn = get_db(db_path)
    try:
        rap = conn.execute("SELECT * FROM banque_rapprochements WHERE objet_id=? AND statut='CONFIRME' "
                           "AND type_objet='REVERSEMENT_PROPRIETAIRE' ORDER BY id LIMIT 1",
                           (mouvement_opaque,)).fetchone()
    finally:
        conn.close()
    if rap is None:
        return _refus(E_ORIGINE_ACOMPTE, "Aucun encaissement bancaire rapproché de cet acompte : "
                                         "rapprochez-le dans Flux, où l'écriture 512 / 419100 est "
                                         "proposée avec le rapprochement.")
    montant = _r(rap["montant_rapproche"])
    jour = _txt(m["date_mouvement"])[:10]
    return compta._inserer_ecriture(
        "BANQUE", jour, jour[:7], rap["rapprochement_id_opaque"],
        "Encaissement d'acompte propriétaire (régularisation)", "ORIGINE_ACOMPTE",
        mouvement_opaque,
        [{"compte": "512000", "debit": montant, "credit": 0, "libelle": "Encaissement acompte"},
         {"compte": COMPTE_CREDITS, "debit": 0, "credit": montant, "auxiliaire": m["proprietaire_id"],
          "proprietaire_id": m["proprietaire_id"], "libelle": "Acompte client"}],
        acteur=acteur, db_path=db_path)


def _factures_numeros(ids: set[str], db_path=None) -> dict[str, str]:
    if not ids:
        return {}
    conn = get_db(db_path)
    try:
        marques = ",".join("?" * len(ids))
        return {r[0]: r[1] or "brouillon" for r in conn.execute(
            f"SELECT facture_id_opaque, numero_facture FROM factures_proprietaires "
            f"WHERE facture_id_opaque IN ({marques})", list(ids))}
    finally:
        conn.close()


def vue(proprietaire_id: str, *, db_path=None) -> dict[str, Any]:
    """Tout ce que l'écran « Crédits » d'un propriétaire affiche — LECTURE SEULE."""
    from app.services import compte_proprietaire_service as cpt
    from app.services import proprietaires_tresorerie_service as tres

    credits = []
    for c in lister(proprietaire_id=proprietaire_id, db_path=db_path):
        numeros = _factures_numeros({i["document_id"] for i in c["imputations"] if i["document_id"]},
                                    db_path)
        origine = (LIBELLE_REPRISE_SOLDE if c["origine"] == ORIGINE_REPRISE_SOLDE
                   else "Justifiée — compte " + (c["compte_source"] or "") if c["mode_origine"] == MODE_JUSTIFIE
                   else "Encaissement bancaire rapproché" if c["statut"] == ST_DISPONIBLE
                   else "À rapprocher du virement Airbnb dans Flux")
        credits.append({**c, "origine_lisible": origine, "date_fr": _date_fr(c["date_origine"]),
                        "factures": [{"numero": numeros.get(i["document_id"], "—"),
                                      "facture_id": i["document_id"],
                                      "montant": _r(i["montant_impute"]),
                                      "date_fr": _date_fr(i["date_imputation"]),
                                      "ecriture": i.get("ecriture_imputation")}
                                     for i in c["imputations"]]})

    allocations = cpt.calculer(proprietaire_id, db_path=db_path)["allocations"] \
        if proprietaire_id else []
    acomptes = []
    for m in tres.lister(proprietaire_id=proprietaire_id, db_path=db_path):
        if (m["statut"] != "VALIDE" or m["nature"] != "ACOMPTE_PROPRIETAIRE"
                or m["sens"] != "PROPRIETAIRE_VERS_SOCIETE" or str(m.get("actif", 1)) == "0"
                or _txt(m.get("source_type")) == "FLUX_LETTRAGE"):
            continue
        parts = [a for a in allocations if a["source_ref"] == m["mouvement_opaque"]]
        numeros = _factures_numeros({a["facture_id_opaque"] for a in parts}, db_path)
        utilise = _r(sum(a["montant_alloue"] for a in parts))
        ecr = ecriture_origine_acompte(m["mouvement_opaque"], db_path=db_path)
        acomptes.append({
            "mouvement_opaque": m["mouvement_opaque"], "libelle_origine": "Acompte",
            "date_fr": _date_fr(m["date_mouvement"]), "montant_initial": _r(m["montant"]),
            "utilise": utilise, "reste": _r(_r(m["montant"]) - utilise),
            "ecriture_origine": ecr,
            "origine_lisible": ("Encaissement comptabilisé (512 / 419100)" if ecr
                                else "Sans origine comptable : à rapprocher de son encaissement"),
            "factures": [{"numero": numeros.get(a["facture_id_opaque"], "—"),
                          "facture_id": a["facture_id_opaque"], "montant": a["montant_alloue"]}
                         for a in parts]})
    return {"credits": credits, "acomptes": acomptes,
            "a_regulariser": a_regulariser(proprietaire_id=proprietaire_id, db_path=db_path),
            "total_disponible": _r(sum(c["reste"] for c in credits if c["statut"] == ST_DISPONIBLE)
                                   + sum(a["reste"] for a in acomptes if a["ecriture_origine"]))}

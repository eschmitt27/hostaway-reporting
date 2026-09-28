"""Plan comptable administrable (Mission 31) — les comptes, SAISIS par l'utilisateur.

CE QUE LE LOGICIEL NE FAIT JAMAIS ICI
  · inventer ou générer un numéro de compte ;
  · créer un compte parce qu'une catégorie existe ;
  · supprimer un compte : il se DÉSACTIVE (le schéma refuse la suppression, migration 0113) ;
  · renommer un numéro : les lignes d'écriture le portent par valeur (refusé par le schéma aussi).

RÈGLES DE FORMAT — celles que le projet utilise déjà, rien de plus :
  · un numéro est fait de chiffres (tous les comptes existants le sont, et le moteur lit la CLASSE
    au premier chiffre : `6` charge, `7` produit — cf. le contrôle anti double comptabilisation) ;
  · le type est l'un de ceux du schéma (`ACTIF`, `PASSIF`, `CHARGE`, `PRODUIT`, migration 0021) ;
  · un compte de charge est de classe 6, un compte de produit de classe 7, et réciproquement.

COMPTES STRUCTURELS. Les générateurs d'écritures citent certains comptes par leur numéro (banque,
caisse, fournisseurs, propriétaires, associés, ventes, filet provisoire des achats). Les désactiver
casserait la comptabilisation : c'est refusé, et l'écran le dit.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

TYPES = ("ACTIF", "PASSIF", "CHARGE", "PRODUIT")
LIBELLES_TYPE = {"ACTIF": "Actif (bilan)", "PASSIF": "Passif (bilan)", "CHARGE": "Charge",
                 "PRODUIT": "Produit"}
CLASSE_ATTENDUE = {"CHARGE": "6", "PRODUIT": "7"}

E_NUMERO_VIDE = "PC01_NUMERO_OBLIGATOIRE"
E_NUMERO_FORMAT = "PC02_NUMERO_FORMAT"
E_LIBELLE_VIDE = "PC03_LIBELLE_OBLIGATOIRE"
E_TYPE = "PC04_TYPE_INCONNU"
E_CLASSE = "PC05_TYPE_INCOHERENT_AVEC_LA_CLASSE"
E_DOUBLON = "PC06_COMPTE_DEJA_EXISTANT"
E_INTROUVABLE = "PC07_COMPTE_INTROUVABLE"
E_STRUCTUREL = "PC08_COMPTE_STRUCTUREL"
E_DEJA = "PC09_ETAT_INCHANGE"
E_ACTEUR = "PC10_ACTEUR_OBLIGATOIRE"
E_MOTIF = "PC11_MOTIF_OBLIGATOIRE"


class CompteRefuse(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def comptes_structurels() -> dict[str, str]:
    """Comptes cités par leur numéro dans les générateurs d'écritures — lus dans le moteur lui-même,
    jamais recopiés ici, pour qu'une évolution du moteur ne laisse pas cette liste en retard."""
    from app.services import comptabilite_ecritures_service as compta
    return {
        compta.COMPTE_FOURNISSEURS: "fournisseurs (règlements, dettes)",
        compta.COMPTE_PROPRIETAIRES: "propriétaires (créances, encaissements)",
        compta.COMPTE_BANQUE: "banque",
        compta.COMPTE_CAISSE: "caisse",
        compta.COMPTE_ASSOCIES: "comptes courants d'associés",
        compta.COMPTE_VENTE_GENERIQUE: "ventes (factures propriétaires)",
        compta.COMPTE_ACHAT_GENERIQUE: "filet provisoire du journal Achats (factures fournisseurs)",
    }


def _evenement(conn, compte: str, type_evt: str, *, avant=None, apres=None, motif: str = "",
               acteur: str) -> None:
    conn.execute(
        "INSERT INTO plan_comptable_evenements (compte, type_evenement, avant_json, apres_json, "
        "motif, acteur) VALUES (?,?,?,?,?,?)",
        (compte, type_evt, json.dumps(avant, ensure_ascii=False) if avant else None,
         json.dumps(apres, ensure_ascii=False) if apres else None, _txt(motif) or None, acteur))


def charger(compte: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM plan_comptable WHERE compte=?", (_txt(compte),)).fetchone()
    finally:
        conn.close()
    return dict(r) if r else None


def utilisation(*, db_path=None) -> dict[str, dict[str, int]]:
    """Par compte : lignes d'écriture, lignes d'OD, règles de mapping qui le citent."""
    conn = get_db(db_path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        out: dict[str, dict[str, int]] = {}

        def compter(sql: str, cle: str):
            for r in conn.execute(sql):
                out.setdefault(r[0], {"ecritures": 0, "od": 0, "regles": 0})[cle] = r[1]
        if "ecriture_lignes" in tables:
            compter("SELECT compte, COUNT(*) FROM ecriture_lignes GROUP BY compte", "ecritures")
        if "od_lignes" in tables:
            compter("SELECT compte, COUNT(*) FROM od_lignes GROUP BY compte", "od")
        if "mapping_comptable_regles" in tables:
            compter("SELECT compte, COUNT(*) FROM mapping_comptable_regles GROUP BY compte", "regles")
        return out
    finally:
        conn.close()


def lister(*, recherche: str = "", type_compte: str = "", statut: str = "",
           db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        comptes = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY compte")]
    finally:
        conn.close()
    usages = utilisation(db_path=db_path)
    structurels = comptes_structurels()
    q = _txt(recherche).lower()
    out = []
    for c in comptes:
        if q and q not in c["compte"].lower() and q not in _txt(c["libelle"]).lower():
            continue
        if type_compte and c["type_compte"] != type_compte:
            continue
        if statut == "ACTIF" and not c["actif"]:
            continue
        if statut == "INACTIF" and c["actif"]:
            continue
        u = usages.get(c["compte"], {"ecritures": 0, "od": 0, "regles": 0})
        c.update(type_libelle=LIBELLES_TYPE.get(c["type_compte"], c["type_compte"]),
                 nb_ecritures=u["ecritures"], nb_od=u["od"], nb_regles=u["regles"],
                 utilise=bool(u["ecritures"] or u["od"] or u["regles"]),
                 structurel=structurels.get(c["compte"], ""))
        out.append(c)
    return out


def comptes_de_charge_actifs(*, db_path=None) -> list[dict[str, Any]]:
    """Seuls comptes proposables pour une catégorie de charge : actifs, type CHARGE, classe 6."""
    return [c for c in lister(statut="ACTIF", db_path=db_path)
            if c["type_compte"] == "CHARGE" and c["compte"].startswith("6")]


def compatible_charge(compte: dict[str, Any] | None) -> bool:
    return bool(compte and compte.get("actif") and compte.get("type_compte") == "CHARGE"
                and _txt(compte.get("compte")).startswith("6"))


def historique(compte: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM plan_comptable_evenements WHERE compte=? ORDER BY id DESC",
            (_txt(compte),))]
    finally:
        conn.close()


def _valider_saisie(compte: str, libelle: str, type_compte: str) -> None:
    if not compte:
        raise CompteRefuse(E_NUMERO_VIDE, "Le numéro de compte est obligatoire.")
    if not re.fullmatch(r"\d+", compte):
        raise CompteRefuse(E_NUMERO_FORMAT, "Un numéro de compte ne contient que des chiffres.")
    if not libelle:
        raise CompteRefuse(E_LIBELLE_VIDE, "Le libellé du compte est obligatoire.")
    if type_compte not in TYPES:
        raise CompteRefuse(E_TYPE, "Choisissez le type du compte : actif, passif, charge ou produit.")
    classe = CLASSE_ATTENDUE.get(type_compte)
    if classe and not compte.startswith(classe):
        raise CompteRefuse(E_CLASSE, f"Un compte de {LIBELLES_TYPE[type_compte].lower()} commence "
                                     f"par {classe}.")
    for type_, classe_ in CLASSE_ATTENDUE.items():
        if compte.startswith(classe_) and type_compte != type_:
            raise CompteRefuse(E_CLASSE, f"Un compte commençant par {classe_} est un compte de "
                                         f"{LIBELLES_TYPE[type_].lower()}.")


def creer(compte: str, libelle: str, type_compte: str, *, auxiliaire_autorise: bool = False,
          commentaire: str = "", acteur: str, db_path=None) -> dict[str, Any]:
    """Ajoute un compte SAISI par l'utilisateur. Rien n'est complété ni deviné."""
    compte, libelle, type_compte = _txt(compte), _txt(libelle), _txt(type_compte).upper()
    acteur = _txt(acteur)
    if not acteur:
        raise CompteRefuse(E_ACTEUR, "Indiquez votre nom : chaque modification du plan est tracée.")
    _valider_saisie(compte, libelle, type_compte)
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM plan_comptable WHERE compte=?", (compte,)).fetchone():
            conn.rollback()
            raise CompteRefuse(E_DOUBLON, f"Le compte {compte} existe déjà.")
        maintenant = _now()
        conn.execute(
            "INSERT INTO plan_comptable (compte, libelle, type_compte, actif, auxiliaire_autorise, "
            "commentaire, date_creation, date_modification) VALUES (?,?,?,1,?,?,?,?)",
            (compte, libelle, type_compte, 1 if auxiliaire_autorise else 0,
             _txt(commentaire) or None, maintenant, maintenant))
        _evenement(conn, compte, "CREATION", acteur=acteur,
                   apres={"libelle": libelle, "type_compte": type_compte,
                          "auxiliaire_autorise": bool(auxiliaire_autorise)})
        conn.commit()
    finally:
        conn.close()
    return charger(compte, db_path=db_path)


def modifier(compte: str, *, libelle: str, commentaire: str = "", acteur: str, motif: str = "",
             db_path=None) -> dict[str, Any]:
    """Seuls le libellé et le commentaire se corrigent : le numéro et le type font l'identité du
    compte et le sens des écritures déjà passées."""
    acteur, libelle = _txt(acteur), _txt(libelle)
    if not acteur:
        raise CompteRefuse(E_ACTEUR, "Indiquez votre nom : chaque modification du plan est tracée.")
    if not libelle:
        raise CompteRefuse(E_LIBELLE_VIDE, "Le libellé du compte est obligatoire.")
    avant = charger(compte, db_path=db_path)
    if avant is None:
        raise CompteRefuse(E_INTROUVABLE, "Ce compte comptable n'existe pas.")
    apres = {"libelle": libelle, "commentaire": _txt(commentaire) or None}
    if avant["libelle"] == apres["libelle"] and (avant["commentaire"] or None) == apres["commentaire"]:
        return avant
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE plan_comptable SET libelle=?, commentaire=?, date_modification=? "
                     "WHERE compte=?", (apres["libelle"], apres["commentaire"], _now(), avant["compte"]))
        _evenement(conn, avant["compte"], "MODIFICATION", acteur=acteur, motif=motif,
                   avant={"libelle": avant["libelle"], "commentaire": avant["commentaire"]},
                   apres=apres)
        conn.commit()
    finally:
        conn.close()
    return charger(compte, db_path=db_path)


def _changer_etat(compte: str, actif: bool, *, acteur: str, motif: str, db_path=None) -> dict:
    acteur, motif = _txt(acteur), _txt(motif)
    if not acteur:
        raise CompteRefuse(E_ACTEUR, "Indiquez votre nom : chaque modification du plan est tracée.")
    if not motif:
        raise CompteRefuse(E_MOTIF, "Indiquez le motif : il reste dans l'historique du compte.")
    c = charger(compte, db_path=db_path)
    if c is None:
        raise CompteRefuse(E_INTROUVABLE, "Ce compte comptable n'existe pas.")
    if bool(c["actif"]) == actif:
        raise CompteRefuse(E_DEJA, "Ce compte est déjà " + ("actif." if actif else "désactivé."))
    if not actif and c["compte"] in comptes_structurels():
        raise CompteRefuse(E_STRUCTUREL, f"Le compte {c['compte']} est utilisé par le moteur "
                                         f"comptable ({comptes_structurels()[c['compte']]}) : le "
                                         "désactiver empêcherait de comptabiliser.")
    if actif:
        _valider_saisie(c["compte"], c["libelle"], c["type_compte"])
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE plan_comptable SET actif=?, date_modification=? WHERE compte=?",
                     (1 if actif else 0, _now(), c["compte"]))
        _evenement(conn, c["compte"], "REACTIVATION" if actif else "DESACTIVATION", acteur=acteur,
                   motif=motif, avant={"actif": bool(c["actif"])}, apres={"actif": actif})
        conn.commit()
    finally:
        conn.close()
    return charger(compte, db_path=db_path)


def desactiver(compte: str, *, acteur: str, motif: str, db_path=None) -> dict[str, Any]:
    """Le compte n'est plus proposé ni accepté pour une NOUVELLE écriture ; l'historique qui le
    cite reste intact et lisible."""
    return _changer_etat(compte, False, acteur=acteur, motif=motif, db_path=db_path)


def reactiver(compte: str, *, acteur: str, motif: str, db_path=None) -> dict[str, Any]:
    return _changer_etat(compte, True, acteur=acteur, motif=motif, db_path=db_path)

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

AUXILIAIRES (Mission 36). Chaque compte dit s'il porte un tiers : `auxiliaire_mode` NONE (jamais),
OPTIONAL (permis) ou REQUIRED (obligatoire), et `auxiliaire_type` FOURNISSEUR, CLIENT ou ASSOCIE.
Un compte antérieur non paramétré se lit par son numéro (401 fournisseur, 411/4191 client, 455/467
associé : obligatoire) puis par `auxiliaire_autorise` (permis). Le champ auxiliaire ne s'affiche
que si le compte en porte un, et un auxiliaire posé sur un compte NONE est effacé à l'écriture.

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
E_AUXILIAIRE = "PC12_PARAMETRE_AUXILIAIRE"

AUX_NONE, AUX_OPTIONAL, AUX_REQUIRED = "NONE", "OPTIONAL", "REQUIRED"
AUX_MODES = (AUX_NONE, AUX_OPTIONAL, AUX_REQUIRED)
LIBELLES_AUX_MODE = {AUX_NONE: "Sans tiers", AUX_OPTIONAL: "Tiers facultatif",
                     AUX_REQUIRED: "Tiers obligatoire"}
AUX_FOURNISSEUR, AUX_CLIENT, AUX_ASSOCIE = "FOURNISSEUR", "CLIENT", "ASSOCIE"
AUX_TYPES = (AUX_FOURNISSEUR, AUX_CLIENT, AUX_ASSOCIE)
LIBELLES_AUX_TYPE = {AUX_FOURNISSEUR: "fournisseur", AUX_CLIENT: "client / propriétaire",
                     AUX_ASSOCIE: "associé"}
# Lecture d'un compte ANTÉRIEUR non paramétré : numéro → tiers obligatoire. Rien d'autre n'est
# déduit du numéro ; un compte paramétré (auxiliaire_mode renseigné) fait toujours foi.
_TIERS_PAR_PREFIXE = (("401", AUX_FOURNISSEUR), ("4191", AUX_CLIENT), ("411", AUX_CLIENT),
                      ("455", AUX_ASSOCIE), ("467", AUX_ASSOCIE))


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


def _type_par_prefixe(numero: str) -> str:
    return next((t for prefixe, t in _TIERS_PAR_PREFIXE if numero.startswith(prefixe)), "")


def mode_auxiliaire(compte: dict[str, Any] | str | None, *, db_path=None) -> tuple[str, str]:
    """(mode, type de tiers) d'un compte — NONE / OPTIONAL / REQUIRED, et FOURNISSEUR / CLIENT /
    ASSOCIE (vide si NONE). Accepte une ligne du plan ou un numéro."""
    row = charger(compte, db_path=db_path) if isinstance(compte, str) else compte
    numero = _txt(row.get("compte")) if row else _txt(compte if isinstance(compte, str) else "")
    mode = _txt((row or {}).get("auxiliaire_mode")).upper()
    type_ = _txt((row or {}).get("auxiliaire_type")).upper()
    if mode in AUX_MODES:
        if mode == AUX_NONE:
            return AUX_NONE, ""
        return mode, type_ or _type_par_prefixe(numero)
    type_prefixe = _type_par_prefixe(numero)
    if type_prefixe:
        return AUX_REQUIRED, type_prefixe
    if row and row.get("auxiliaire_autorise"):
        return AUX_OPTIONAL, type_
    return AUX_NONE, ""


def modes_auxiliaires(*, db_path=None) -> dict[str, dict[str, str]]:
    """{compte: {mode, type, libelle_type}} pour tous les comptes — ce que l'écran d'écriture lit
    pour afficher (ou masquer) le champ tiers au changement de compte."""
    conn = get_db(db_path)
    try:
        comptes = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable")]
    finally:
        conn.close()
    out = {}
    for c in comptes:
        mode, type_ = mode_auxiliaire(c)
        out[c["compte"]] = {"mode": mode, "type": type_,
                            "libelle_type": LIBELLES_AUX_TYPE.get(type_, "tiers")}
    return out


def _valider_auxiliaire(mode: str, type_: str) -> tuple[str | None, str | None]:
    mode, type_ = _txt(mode).upper(), _txt(type_).upper()
    if not mode:
        return None, None
    if mode not in AUX_MODES:
        raise CompteRefuse(E_AUXILIAIRE, "Mode d'auxiliaire inconnu : sans tiers, tiers facultatif "
                                         "ou tiers obligatoire.")
    if mode == AUX_NONE:
        return AUX_NONE, None
    if type_ not in AUX_TYPES:
        raise CompteRefuse(E_AUXILIAIRE, "Précisez le type de tiers porté par ce compte : "
                                         "fournisseur, client ou associé.")
    return mode, type_


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
          auxiliaire_mode: str = "", auxiliaire_type: str = "",
          commentaire: str = "", acteur: str, db_path=None) -> dict[str, Any]:
    """Ajoute un compte SAISI par l'utilisateur. Rien n'est complété ni deviné."""
    compte, libelle, type_compte = _txt(compte), _txt(libelle), _txt(type_compte).upper()
    acteur = _txt(acteur)
    if not acteur:
        raise CompteRefuse(E_ACTEUR, "Indiquez votre nom : chaque modification du plan est tracée.")
    _valider_saisie(compte, libelle, type_compte)
    mode, type_aux = _valider_auxiliaire(auxiliaire_mode, auxiliaire_type)
    if mode is None:
        # Non précisé : même lecture que pour un compte antérieur (numéro, puis case « tiers »).
        mode, type_aux = mode_auxiliaire({"compte": compte, "auxiliaire_autorise":
                                          auxiliaire_autorise})
        type_aux = type_aux or None
    auxiliaire_autorise = mode != AUX_NONE
    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM plan_comptable WHERE compte=?", (compte,)).fetchone():
            conn.rollback()
            raise CompteRefuse(E_DOUBLON, f"Le compte {compte} existe déjà.")
        maintenant = _now()
        conn.execute(
            "INSERT INTO plan_comptable (compte, libelle, type_compte, actif, auxiliaire_autorise, "
            "auxiliaire_mode, auxiliaire_type, commentaire, date_creation, date_modification) "
            "VALUES (?,?,?,1,?,?,?,?,?,?)",
            (compte, libelle, type_compte, 1 if auxiliaire_autorise else 0, mode, type_aux,
             _txt(commentaire) or None, maintenant, maintenant))
        _evenement(conn, compte, "CREATION", acteur=acteur,
                   apres={"libelle": libelle, "type_compte": type_compte,
                          "auxiliaire_mode": mode, "auxiliaire_type": type_aux})
        conn.commit()
    finally:
        conn.close()
    return charger(compte, db_path=db_path)


def modifier(compte: str, *, libelle: str, commentaire: str = "", acteur: str, motif: str = "",
             auxiliaire_mode: str = "", auxiliaire_type: str = "",
             db_path=None) -> dict[str, Any]:
    """Se corrigent : le libellé, le commentaire et le paramètre de tiers (auxiliaire). Le numéro
    et le type font l'identité du compte et le sens des écritures déjà passées."""
    acteur, libelle = _txt(acteur), _txt(libelle)
    if not acteur:
        raise CompteRefuse(E_ACTEUR, "Indiquez votre nom : chaque modification du plan est tracée.")
    if not libelle:
        raise CompteRefuse(E_LIBELLE_VIDE, "Le libellé du compte est obligatoire.")
    avant = charger(compte, db_path=db_path)
    if avant is None:
        raise CompteRefuse(E_INTROUVABLE, "Ce compte comptable n'existe pas.")
    mode, type_aux = _valider_auxiliaire(auxiliaire_mode, auxiliaire_type)
    if mode is None:
        mode, type_aux = avant.get("auxiliaire_mode"), avant.get("auxiliaire_type")
    apres = {"libelle": libelle, "commentaire": _txt(commentaire) or None,
             "auxiliaire_mode": mode, "auxiliaire_type": type_aux}
    cles = ("libelle", "commentaire", "auxiliaire_mode", "auxiliaire_type")
    if all((avant.get(k) or None) == (apres[k] or None) for k in cles):
        return avant
    autorise = avant.get("auxiliaire_autorise") or 0
    if mode:
        autorise = 0 if mode == AUX_NONE else 1
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE plan_comptable SET libelle=?, commentaire=?, auxiliaire_mode=?, "
                     "auxiliaire_type=?, auxiliaire_autorise=?, date_modification=? WHERE compte=?",
                     (apres["libelle"], apres["commentaire"], mode, type_aux, autorise, _now(),
                      avant["compte"]))
        _evenement(conn, avant["compte"], "MODIFICATION", acteur=acteur, motif=motif,
                   avant={k: avant.get(k) for k in cles}, apres=apres)
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

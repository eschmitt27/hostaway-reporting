"""Mapping catégorie de charge → compte comptable (migrations `0024`, `0113`).

SOURCE CANONIQUE UNIQUE : `mapping_comptable_regles`. (`mapping_categorie_compte`, migration 0023,
est une table héritée, vide, qui ne sert qu'à un contrôle de signalement : elle ne décide rien.)

RÉSOLUTION — dans cet ordre (cf. mission Analytique §2) :

1. règle `CATEGORIE` active, VALIDÉE et applicable à la date de référence ;
2. règle `TYPE_FLUX` active, VALIDÉE et applicable à la date de référence ;
3. règle `CATEGORIE` active PROVISOIRE applicable (résultat marqué PROVISOIRE) ;
4. filet `PROVISOIRE_GENERIQUE` (résultat marqué PROVISOIRE) — il ne sert qu'au journal Achats des
   factures fournisseurs ; Flux financiers ne reprend JAMAIS un résultat qui n'est pas VALIDE ;
5. aucune règle : AUCUN compte (compte vide). Il n'y a plus de numéro codé en dur ici.

Une règle expirée, pas encore commencée ou DÉSACTIVÉE n'est jamais utilisée.

ADMINISTRATION (Mission 31). Toute règle passe par les validations de ce module — le formulaire
n'est jamais la seule protection :
  · la catégorie (ou le type de flux) existe dans le référentiel ;
  · le compte existe dans le plan comptable, est ACTIF, et est un compte de CHARGE (classe 6) ;
  · la période est cohérente (début ≤ fin, dates ISO) ;
  · deux règles actives de même statut ne se chevauchent pas pour la même catégorie : le résultat
    serait ambigu. (Une règle provisoire sous une règle validée n'est pas ambiguë : la validée prime.)

Une règle VALIDÉE ne change pas de compte : on la clôt (date de fin) et on en crée une autre à
partir de la date voulue. C'est ce qui garantit qu'une règle posée en octobre ne réécrit pas le
traitement d'une opération d'août. Aucune écriture déjà passée n'est jamais modifiée par ce module.

Ce service ne CRÉE jamais de compte dans `plan_comptable`.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import Any

from app.db.connection import get_db

PORTEE_CATEGORIE = "CATEGORIE"
PORTEE_TYPE_FLUX = "TYPE_FLUX"
PORTEE_PROVISOIRE = "PROVISOIRE_GENERIQUE"
PORTEES = (PORTEE_CATEGORIE, PORTEE_TYPE_FLUX, PORTEE_PROVISOIRE)

# Rôle d'une règle (Mission 36) : DEFAUT = le compte présélectionné (au plus un par catégorie et
# par période) ; AUTORISE = un autre compte que l'utilisateur peut choisir sans justification.
ROLE_DEFAUT = "DEFAUT"
ROLE_AUTORISE = "AUTORISE"
ROLES = (ROLE_DEFAUT, ROLE_AUTORISE)
LIBELLES_ROLE = {ROLE_DEFAUT: "Compte par défaut", ROLE_AUTORISE: "Compte autorisé"}

# Ce que la saisie d'une charge propose, une fois la catégorie choisie.
PROPOSITION_UNIQUE = "UNIQUE"          # un seul compte valide : présélectionné
PROPOSITION_CHOIX = "CHOIX"            # plusieurs comptes autorisés : l'utilisateur choisit
PROPOSITION_A_DEFINIR = "A_DEFINIR"    # aucun : « Compte comptable à définir » ou imputation libre

ST_PROVISOIRE = "PROVISOIRE"
ST_VALIDE = "VALIDE"
STATUTS = (ST_PROVISOIRE, ST_VALIDE)
LIBELLES_STATUT = {ST_PROVISOIRE: "Provisoire", ST_VALIDE: "Validée"}

E_PORTEE_INCONNUE = "V01_PORTEE_INCONNUE"
E_CLE_MANQUANTE = "V02_CLE_MANQUANTE"
E_COMPTE_MANQUANT = "V03_COMPTE_MANQUANT"
E_CLE_INCONNUE = "V04_CATEGORIE_INCONNUE"
E_COMPTE_INEXISTANT = "V05_COMPTE_INEXISTANT"
E_COMPTE_INACTIF = "V06_COMPTE_INACTIF"
E_COMPTE_INCOMPATIBLE = "V07_COMPTE_INCOMPATIBLE"
E_PERIODE = "V08_PERIODE_INCOHERENTE"
E_CHEVAUCHEMENT = "V09_CHEVAUCHEMENT"
E_STATUT = "V10_STATUT_INCONNU"
E_GENERIQUE = "V11_FILET_GENERIQUE_NON_ADMINISTRABLE"
E_INTROUVABLE = "V12_REGLE_INTROUVABLE"
E_TRANSITION = "V13_TRANSITION_INTERDITE"
E_MOTIF = "V14_MOTIF_OBLIGATOIRE"

MESSAGES = {
    E_PORTEE_INCONNUE: "Portée de mapping inconnue.",
    E_CLE_MANQUANTE: "La catégorie est obligatoire.",
    E_COMPTE_MANQUANT: "Le compte comptable est obligatoire.",
    E_CLE_INCONNUE: "Cette catégorie n'existe pas dans le référentiel.",
    E_COMPTE_INEXISTANT: "Ce compte comptable n'existe pas.",
    E_COMPTE_INACTIF: "Ce compte est désactivé.",
    E_COMPTE_INCOMPATIBLE: "Une catégorie de charge se rattache à un compte de charge (classe 6).",
    E_PERIODE: "Période incohérente.",
    E_CHEVAUCHEMENT: "Cette règle chevauche déjà une règle de même statut pour cette catégorie.",
    E_STATUT: "Statut inconnu : une règle est provisoire ou validée.",
    E_GENERIQUE: "Le filet générique n'est pas administrable ici : il ne sert qu'au journal Achats "
                 "des factures fournisseurs et sera traité avec ce bloc.",
    E_INTROUVABLE: "Règle introuvable.",
    E_TRANSITION: "Cette modification n'est pas permise sur cette règle.",
    E_MOTIF: "Indiquez le motif : il reste dans l'historique de la règle.",
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _role(r: dict[str, Any]) -> str:
    return _txt(r.get("role")) or ROLE_DEFAUT


def _actif(r: dict[str, Any]) -> bool:
    return int(r.get("actif", 1) if r.get("actif") is not None else 1) == 1


def _active_a_date(row: dict[str, Any], date_reference: str) -> bool:
    debut = row.get("date_debut_validite")
    fin = row.get("date_fin_validite")
    if debut and date_reference < debut:
        return False
    if fin and date_reference > fin:
        return False
    return True


# ══ Référentiels lus (jamais écrits) ══════════════════════════════════════════════════════════

def categories(*, db_path=None, actives_seulement: bool = True) -> list[dict[str, str]]:
    """Catégories de charge du référentiel, avec leur libellé métier."""
    conn = get_db(db_path)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name='ref_categories_charges'").fetchone():
            return []
        rows = [dict(r) for r in conn.execute(
            "SELECT categorie_charge_id, categorie_niveau_1, categorie_niveau_2, actif "
            "FROM ref_categories_charges ORDER BY categorie_niveau_1, categorie_niveau_2")]
    finally:
        conn.close()
    out = []
    for r in rows:
        if actives_seulement and _txt(r.get("actif")).upper() not in ("OUI", ""):
            continue
        libelle = " · ".join(x for x in (_txt(r["categorie_niveau_1"]),
                                         _txt(r["categorie_niveau_2"])) if x)
        out.append({"id": r["categorie_charge_id"], "libelle": libelle or r["categorie_charge_id"]})
    return out


def _types_flux(*, db_path=None) -> dict[str, str]:
    conn = get_db(db_path)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name='ref_types_flux'").fetchone():
            return {}
        return {r[0]: _txt(r[1]).replace("_", " ").capitalize()
                for r in conn.execute("SELECT type_flux_id, type_flux FROM ref_types_flux")}
    finally:
        conn.close()


def libelle_cle(portee: str, cle: str, *, db_path=None) -> str:
    if portee == PORTEE_CATEGORIE:
        return next((c["libelle"] for c in categories(db_path=db_path, actives_seulement=False)
                     if c["id"] == cle), "Catégorie inconnue")
    if portee == PORTEE_TYPE_FLUX:
        return _types_flux(db_path=db_path).get(cle, "Type de flux inconnu")
    return "Filet générique (journal Achats uniquement)"


# ══ Lecture des règles ════════════════════════════════════════════════════════════════════════

def lister_regles(*, portee: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM mapping_comptable_regles ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = [dict(r) for r in rows]
    if portee:
        out = [r for r in out if r["portee"] == portee]
    return out


def charger_regle(regle_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM mapping_comptable_regles WHERE regle_id_opaque=?",
                         (_txt(regle_id),)).fetchone()
    finally:
        conn.close()
    return dict(r) if r else None


def historique_regle(regle_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM mapping_regle_evenements WHERE regle_id_opaque=? ORDER BY id DESC",
            (_txt(regle_id),))]
    finally:
        conn.close()


def resoudre_compte(*, categorie_charge_id: str = "", type_flux_id: str = "",
                    date_reference: str = "", db_path=None) -> dict[str, Any]:
    """Résout le compte à utiliser pour une charge donnée. Ne lève jamais : renvoie toujours un
    résultat exploitable, avec `statut` VALIDE ou PROVISOIRE — et un compte VIDE quand aucune
    règle ne s'applique. Aucun numéro n'est choisi en silence."""
    date_reference = date_reference or date.today().isoformat()
    regles = [r for r in lister_regles(db_path=db_path) if _actif(r)]

    def _cherche(portee: str, cle: str) -> dict[str, Any] | None:
        candidats = [r for r in regles if r["portee"] == portee and r["cle"] == cle
                     and r["statut"] == ST_VALIDE and _role(r) == ROLE_DEFAUT
                     and _active_a_date(r, date_reference)]
        candidats.sort(key=lambda r: r.get("date_debut_validite") or "", reverse=True)
        return candidats[0] if candidats else None

    if categorie_charge_id:
        r = _cherche(PORTEE_CATEGORIE, categorie_charge_id)
        if r:
            return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                    "regle": PORTEE_CATEGORIE, "statut": ST_VALIDE, "source": r.get("source")}

    if type_flux_id:
        r = _cherche(PORTEE_TYPE_FLUX, type_flux_id)
        if r:
            return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                    "regle": PORTEE_TYPE_FLUX, "statut": ST_VALIDE, "source": r.get("source")}

    provisoires = [r for r in regles if r["portee"] == PORTEE_PROVISOIRE
                  and _active_a_date(r, date_reference)]
    if categorie_charge_id:
        prov_categorie = [r for r in regles if r["portee"] == PORTEE_CATEGORIE
                         and r["cle"] == categorie_charge_id and r["statut"] == ST_PROVISOIRE
                         and _role(r) == ROLE_DEFAUT and _active_a_date(r, date_reference)]
        if prov_categorie:
            r = prov_categorie[0]
            return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                    "regle": PORTEE_CATEGORIE, "statut": ST_PROVISOIRE, "source": r.get("source")}
    if provisoires:
        r = provisoires[0]
        return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                "regle": PORTEE_PROVISOIRE, "statut": ST_PROVISOIRE, "source": r.get("source")}

    # Aucune règle du tout : AUCUN compte. Le filet absolu codé en dur (606000) a disparu
    # (Mission 31) : un consommateur qui reçoit un compte vide refuse d'écrire, il ne devine pas.
    return {"compte": "", "regle_id_opaque": None, "regle": "AUCUNE_REGLE",
            "statut": ST_PROVISOIRE, "source": "Aucune règle applicable : compte à définir."}


def comptes_proposes(categorie_charge_id: str, *, date_reference: str = "",
                     db_path=None) -> dict[str, Any]:
    """Ce que la saisie d'une charge propose pour une catégorie — lecture seule.

    Seules comptent les règles VALIDÉES, actives à la date, pointant vers un compte de charge
    actif. Un seul compte → présélectionné ; plusieurs → à choisir ; aucun → « Compte comptable
    à définir » (l'imputation libre justifiée reste possible)."""
    from app.services import comptabilite_plan_service as plan

    date_reference = date_reference or date.today().isoformat()
    actifs = {c["compte"]: c for c in plan.comptes_de_charge_actifs(db_path=db_path)}
    regles = [r for r in lister_regles(portee=PORTEE_CATEGORIE, db_path=db_path)
              if _actif(r) and r["cle"] == _txt(categorie_charge_id) and r["statut"] == ST_VALIDE
              and _active_a_date(r, date_reference) and r["compte"] in actifs]
    defaut = next((r["compte"] for r in regles if _role(r) == ROLE_DEFAUT), "")
    comptes = list(dict.fromkeys(([defaut] if defaut else [])
                                 + [r["compte"] for r in regles if _role(r) == ROLE_AUTORISE]))
    statut = (PROPOSITION_A_DEFINIR if not comptes
              else PROPOSITION_UNIQUE if len(comptes) == 1 else PROPOSITION_CHOIX)
    return {"categorie_charge_id": _txt(categorie_charge_id), "statut": statut,
            # Un seul compte valide : présélectionné. Plusieurs : le défaut s'il existe, sinon
            # rien — le choix revient à l'utilisateur (impôts : compte selon la taxe).
            "compte_defaut": comptes[0] if statut == PROPOSITION_UNIQUE else defaut,
            "comptes": [{"compte": c, "libelle": actifs[c]["libelle"],
                         "defaut": c == defaut} for c in comptes]}


# ══ Validations ═══════════════════════════════════════════════════════════════════════════════

def _date_iso(v: str) -> bool:
    try:
        date.fromisoformat(v)
        return True
    except ValueError:
        return False


def _chevauche(a_debut, a_fin, b_debut, b_fin) -> bool:
    """Deux périodes [début, fin] (bornes vides = ouvertes) ont-elles un jour commun ?"""
    a_d, a_f = a_debut or "0000-01-01", a_fin or "9999-12-31"
    b_d, b_f = b_debut or "0000-01-01", b_fin or "9999-12-31"
    return a_d <= b_f and b_d <= a_f


def verifier(portee: str, cle: str, compte: str, statut: str, debut: str, fin: str, *,
             exclure: str = "", role: str = ROLE_DEFAUT, db_path=None) -> dict[str, Any] | None:
    """Toutes les règles d'une règle de mapping. `None` = conforme, sinon le refus."""
    from app.services import comptabilite_plan_service as plan

    if portee not in PORTEES:
        return _refus(E_PORTEE_INCONNUE, portee)
    if portee == PORTEE_PROVISOIRE:
        return _refus(E_GENERIQUE)
    if not cle:
        return _refus(E_CLE_MANQUANTE)
    if not compte:
        return _refus(E_COMPTE_MANQUANT)
    if statut not in STATUTS:
        return _refus(E_STATUT, statut)
    role = _txt(role) or ROLE_DEFAUT
    if role not in ROLES or (role == ROLE_AUTORISE and portee != PORTEE_CATEGORIE):
        return _refus(E_STATUT, f"rôle {role}")
    if portee == PORTEE_CATEGORIE and cle not in {c["id"] for c in categories(
            db_path=db_path, actives_seulement=False)}:
        return _refus(E_CLE_INCONNUE, cle)
    if portee == PORTEE_TYPE_FLUX and cle not in _types_flux(db_path=db_path):
        return _refus(E_CLE_INCONNUE, cle)
    c = plan.charger(compte, db_path=db_path)
    if c is None:
        return _refus(E_COMPTE_INEXISTANT, compte)
    if not c["actif"]:
        return _refus(E_COMPTE_INACTIF, compte)
    if not plan.compatible_charge(c):
        return _refus(E_COMPTE_INCOMPATIBLE, compte)
    for d in (debut, fin):
        if d and not _date_iso(d):
            return _refus(E_PERIODE, f"date non reconnue : {d}")
    if debut and fin and debut > fin:
        return _refus(E_PERIODE, f"fin au {_date_fr(fin)}, avant le début au {_date_fr(debut)}")
    for r in lister_regles(db_path=db_path):
        # Un seul compte PAR DÉFAUT à la fois ; un compte AUTORISÉ ne se déclare pas deux fois.
        if role == ROLE_AUTORISE and (_role(r) != ROLE_AUTORISE or r["compte"] != compte):
            continue
        if role == ROLE_DEFAUT and _role(r) != ROLE_DEFAUT:
            continue
        if (r["regle_id_opaque"] != exclure and _actif(r) and r["portee"] == portee
                and r["cle"] == cle and r["statut"] == statut
                and _chevauche(debut, fin, r["date_debut_validite"], r["date_fin_validite"])):
            return _refus(E_CHEVAUCHEMENT,
                          f"règle existante : compte {r['compte']}, "
                          f"{periode_fr(r['date_debut_validite'], r['date_fin_validite'])}")
    return None


def _date_fr(d: str | None) -> str:
    return f"{d[8:10]}/{d[5:7]}/{d[:4]}" if d and _date_iso(d) else (d or "")


def periode_fr(debut: str | None, fin: str | None) -> str:
    """Période de validité en clair (JJ/MM/AAAA), bornes absentes comprises."""
    fr = _date_fr
    if debut and fin:
        return f"du {fr(debut)} au {fr(fin)}"
    if debut:
        return f"à partir du {fr(debut)}, sans fin"
    if fin:
        return f"jusqu'au {fr(fin)}"
    return "sans limite de dates"


def _evenement(conn, regle_id: str, type_evt: str, *, avant=None, apres=None, motif: str = "",
               acteur: str = "") -> None:
    conn.execute(
        "INSERT INTO mapping_regle_evenements (regle_id_opaque, type_evenement, avant_json, "
        "apres_json, motif, acteur) VALUES (?,?,?,?,?,?)",
        (regle_id, type_evt, json.dumps(avant, ensure_ascii=False) if avant else None,
         json.dumps(apres, ensure_ascii=False) if apres else None, _txt(motif) or None,
         _txt(acteur) or "local"))


def _figer(r: dict[str, Any]) -> dict[str, Any]:
    return {k: r.get(k) for k in ("compte", "statut", "date_debut_validite", "date_fin_validite",
                                  "actif", "source", "role")}


# ══ Écritures des règles ══════════════════════════════════════════════════════════════════════

def creer_regle(portee: str, compte: str, *, cle: str = "", statut: str = ST_PROVISOIRE,
                date_debut_validite: str = "", date_fin_validite: str = "", source: str = "",
                role: str = ROLE_DEFAUT, acteur: str = "", db_path=None) -> dict[str, Any]:
    portee, cle, compte = _txt(portee), _txt(cle), _txt(compte)
    statut = _txt(statut) or ST_PROVISOIRE
    role = _txt(role) or ROLE_DEFAUT
    debut, fin = _txt(date_debut_validite), _txt(date_fin_validite)
    refus = verifier(portee, cle, compte, statut, debut, fin, role=role, db_path=db_path)
    if refus:
        return refus

    opaque = "MAP-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO mapping_comptable_regles (regle_id_opaque, portee, cle, compte, statut, "
            "date_debut_validite, date_fin_validite, source, acteur, actif, date_modification, "
            "role) VALUES (?,?,?,?,?,?,?,?,?,1,?,?)",
            (opaque, portee, cle, compte, statut, debut or None, fin or None,
             _txt(source) or None, _txt(acteur) or "local", _now(), role))
        _evenement(conn, opaque, "CREATION", acteur=acteur, motif=source,
                   apres={"portee": portee, "cle": cle, "compte": compte, "statut": statut,
                          "role": role,
                          "date_debut_validite": debut or None, "date_fin_validite": fin or None})
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "regle_id_opaque": opaque}


def _mettre_a_jour(r: dict[str, Any], nouveau: dict[str, Any], type_evt: str, *, acteur: str,
                   motif: str, db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE mapping_comptable_regles SET compte=?, statut=?, date_debut_validite=?, "
            "date_fin_validite=?, actif=?, source=?, date_modification=?, version=version+1 "
            "WHERE regle_id_opaque=?",
            (nouveau["compte"], nouveau["statut"], nouveau["date_debut_validite"] or None,
             nouveau["date_fin_validite"] or None, 1 if nouveau["actif"] else 0,
             nouveau["source"] or None, _now(), r["regle_id_opaque"]))
        _evenement(conn, r["regle_id_opaque"], type_evt, avant=_figer(r), apres=_figer(nouveau),
                   motif=motif, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "regle_id_opaque": r["regle_id_opaque"]}


def _regle_administrable(regle_id: str, *, db_path=None):
    r = charger_regle(regle_id, db_path=db_path)
    if r is None:
        return None, _refus(E_INTROUVABLE, regle_id)
    if r["portee"] == PORTEE_PROVISOIRE:
        return None, _refus(E_GENERIQUE)
    return r, None


def modifier_regle(regle_id: str, *, compte: str | None = None, date_debut_validite: str | None = None,
                   date_fin_validite: str | None = None, source: str | None = None,
                   acteur: str = "", motif: str = "", db_path=None) -> dict[str, Any]:
    """Règle PROVISOIRE : compte, période et justification se corrigent. Règle VALIDÉE : seule sa
    date de fin se pose (clôture) — pour changer de compte, on en crée une nouvelle à partir de la
    date voulue, et l'historique reste vrai pour les périodes passées."""
    r, refus = _regle_administrable(regle_id, db_path=db_path)
    if refus:
        return refus
    nouveau = dict(r)
    if r["statut"] == ST_VALIDE:
        if (compte is not None and _txt(compte) != r["compte"]) or (
                date_debut_validite is not None
                and _txt(date_debut_validite) != _txt(r["date_debut_validite"])):
            return _refus(E_TRANSITION, "Une règle validée ne change ni de compte ni de début : "
                                        "posez sa date de fin, puis créez la nouvelle règle.")
    if compte is not None:
        nouveau["compte"] = _txt(compte)
    if date_debut_validite is not None:
        nouveau["date_debut_validite"] = _txt(date_debut_validite)
    if date_fin_validite is not None:
        nouveau["date_fin_validite"] = _txt(date_fin_validite)
    if source is not None:
        nouveau["source"] = _txt(source)
    refus = verifier(r["portee"], r["cle"], nouveau["compte"], r["statut"],
                     _txt(nouveau["date_debut_validite"]), _txt(nouveau["date_fin_validite"]),
                     exclure=r["regle_id_opaque"], role=_role(r), db_path=db_path)
    if refus:
        return refus
    return _mettre_a_jour(r, nouveau, "MODIFICATION", acteur=acteur, motif=motif, db_path=db_path)


def valider_regle(regle_id: str, *, acteur: str = "", motif: str = "", db_path=None) -> dict[str, Any]:
    r, refus = _regle_administrable(regle_id, db_path=db_path)
    if refus:
        return refus
    if r["statut"] == ST_VALIDE or not _actif(r):
        return _refus(E_TRANSITION, "Seule une règle provisoire active se valide.")
    refus = verifier(r["portee"], r["cle"], r["compte"], ST_VALIDE, _txt(r["date_debut_validite"]),
                     _txt(r["date_fin_validite"]), exclure=r["regle_id_opaque"], role=_role(r),
                     db_path=db_path)
    if refus:
        return refus
    return _mettre_a_jour(r, dict(r, statut=ST_VALIDE), "VALIDATION", acteur=acteur, motif=motif,
                          db_path=db_path)


def rendre_provisoire(regle_id: str, *, acteur: str = "", motif: str = "",
                      db_path=None) -> dict[str, Any]:
    """Une règle validée redevient provisoire : elle cesse d'être PROPOSÉE pour les opérations à
    venir. Les écritures déjà passées avec elle ne changent pas."""
    if not _txt(motif):
        return _refus(E_MOTIF)
    r, refus = _regle_administrable(regle_id, db_path=db_path)
    if refus:
        return refus
    if r["statut"] != ST_VALIDE:
        return _refus(E_TRANSITION, "Seule une règle validée repasse en provisoire.")
    refus = verifier(r["portee"], r["cle"], r["compte"], ST_PROVISOIRE,
                     _txt(r["date_debut_validite"]), _txt(r["date_fin_validite"]),
                     exclure=r["regle_id_opaque"], role=_role(r), db_path=db_path)
    if refus and refus["code"] != E_COMPTE_INACTIF:
        return refus
    return _mettre_a_jour(r, dict(r, statut=ST_PROVISOIRE), "PASSAGE_PROVISOIRE", acteur=acteur,
                          motif=motif, db_path=db_path)


def desactiver_regle(regle_id: str, *, acteur: str = "", motif: str = "",
                     db_path=None) -> dict[str, Any]:
    if not _txt(motif):
        return _refus(E_MOTIF)
    r, refus = _regle_administrable(regle_id, db_path=db_path)
    if refus:
        return refus
    if not _actif(r):
        return _refus(E_TRANSITION, "Cette règle est déjà désactivée.")
    return _mettre_a_jour(r, dict(r, actif=0), "DESACTIVATION", acteur=acteur, motif=motif,
                          db_path=db_path)


def reactiver_regle(regle_id: str, *, acteur: str = "", motif: str = "",
                    db_path=None) -> dict[str, Any]:
    if not _txt(motif):
        return _refus(E_MOTIF)
    r, refus = _regle_administrable(regle_id, db_path=db_path)
    if refus:
        return refus
    if _actif(r):
        return _refus(E_TRANSITION, "Cette règle est déjà active.")
    refus = verifier(r["portee"], r["cle"], r["compte"], r["statut"], _txt(r["date_debut_validite"]),
                     _txt(r["date_fin_validite"]), exclure=r["regle_id_opaque"], role=_role(r),
                     db_path=db_path)
    if refus:
        return refus
    return _mettre_a_jour(r, dict(r, actif=1), "REACTIVATION", acteur=acteur, motif=motif,
                          db_path=db_path)


# ══ Vue par catégorie et prévisualisation d'impact (lecture seule) ════════════════════════════

def _charges_de_categorie(categorie: str, debut: str = "", fin: str = "", *,
                          db_path=None) -> list[dict[str, Any]]:
    """Charges ACTIVES et COMPTABLES de la catégorie dans la période, avec leur état comptable."""
    from app.services import flux_financiers_service as flux
    conn = get_db(db_path)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT c.charge_id, c.date_charge, c.montant, c.commentaire, c.prise_en_compta, c.statut, "
            "COALESCE(l.nom_court, l.nom_logement_officiel, '') AS logement "
            "FROM charges c LEFT JOIN ref_logements l ON l.logement_id = c.logement_id "
            "WHERE c.statut='ACTIVE' AND c.categorie_charge_id=? ORDER BY c.date_charge",
            (categorie,))]
    finally:
        conn.close()
    statuts = flux.statuts_charges(db_path=db_path)
    out = []
    for c in rows:
        if _txt(c["prise_en_compta"]).upper() == "NON":
            continue
        d = _txt(c["date_charge"])[:10]
        if (debut and d < debut) or (fin and d > fin):
            continue
        st = statuts.get(c["charge_id"], {})
        c["comptabilisee"] = st.get("compta", {}).get("code") == flux.CH_COMPTABILISEE
        c["date_fr"] = flux.date_fr(d)
        out.append(c)
    return out


def vue_par_categorie(*, date_reference: str = "", db_path=None) -> list[dict[str, Any]]:
    """Pour chaque catégorie active : le compte réellement proposé aujourd'hui (règle validée vers
    un compte de charge actif), ou « Compte comptable à définir »."""
    from app.services import comptabilite_plan_service as plan
    date_reference = date_reference or date.today().isoformat()
    regles = [r for r in lister_regles(db_path=db_path) if r["portee"] == PORTEE_CATEGORIE]
    comptes = {c["compte"]: c for c in plan.lister(db_path=db_path)}
    out = []
    for cat in categories(db_path=db_path):
        propres = [r for r in regles if r["cle"] == cat["id"]]
        valide = next((r for r in sorted(propres, key=lambda r: r.get("date_debut_validite") or "",
                                        reverse=True)
                       if _actif(r) and r["statut"] == ST_VALIDE
                       and _active_a_date(r, date_reference)), None)
        provisoire = next((r for r in propres if _actif(r) and r["statut"] == ST_PROVISOIRE
                           and _active_a_date(r, date_reference)), None)
        compte = comptes.get(valide["compte"]) if valide else None
        propose = bool(valide and plan.compatible_charge(compte))
        charges = _charges_de_categorie(cat["id"], db_path=db_path)
        a_traiter = [c for c in charges if not c["comptabilisee"]]
        blocage = ""
        if valide and not propose:
            blocage = ("compte absent du plan comptable" if compte is None
                       else "compte désactivé" if not compte["actif"]
                       else "ce n'est pas un compte de charge")
        out.append({
            "categorie": cat["id"], "libelle": cat["libelle"],
            "regle": valide if propose else None, "regle_provisoire": provisoire,
            "regle_bloquee": valide if (valide and not propose) else None, "blocage": blocage,
            "compte": compte if propose else None,
            "nb_charges": len(charges), "montant": round(sum(c["montant"] or 0 for c in charges), 2),
            "nb_a_traiter": len(a_traiter),
            "montant_a_traiter": round(sum(c["montant"] or 0 for c in a_traiter), 2),
            "nb_regles": len(propres),
        })
    return out


def apercu_impact(portee: str, cle: str, compte: str, statut: str, debut: str = "", fin: str = "",
                  *, exclure: str = "", role: str = ROLE_DEFAUT, db_path=None) -> dict[str, Any]:
    """Ce que la règle CHANGERAIT, sans rien changer : aucune charge, écriture ni rapprochement n'est
    écrit ni recalculé ici."""
    from app.services import comptabilite_plan_service as plan
    portee, cle, compte = _txt(portee), _txt(cle), _txt(compte)
    statut = _txt(statut) or ST_PROVISOIRE
    debut, fin = _txt(debut), _txt(fin)
    refus = verifier(portee, cle, compte, statut, debut, fin, exclure=exclure, role=role,
                     db_path=db_path)
    c = plan.charger(compte, db_path=db_path) if compte else None
    charges = _charges_de_categorie(cle, debut, fin, db_path=db_path) if portee == PORTEE_CATEGORIE \
        else []
    a_traiter = [x for x in charges if not x["comptabilisee"]]
    deja = [x for x in charges if x["comptabilisee"]]
    return {
        "ok": refus is None, "refus": refus,
        "portee": portee, "cle": cle, "cle_libelle": libelle_cle(portee, cle, db_path=db_path),
        "compte": compte, "compte_libelle": (c or {}).get("libelle", ""),
        "statut": statut, "statut_libelle": LIBELLES_STATUT.get(statut, statut),
        "debut": debut, "fin": fin,
        "nb_charges": len(charges), "montant": round(sum(x["montant"] or 0 for x in charges), 2),
        "nb_a_traiter": len(a_traiter),
        "montant_a_traiter": round(sum(x["montant"] or 0 for x in a_traiter), 2),
        "nb_deja_comptabilisees": len(deja),
        "exemples": a_traiter[:10],
        # Une règle provisoire ne propose rien à Flux financiers : l'écran doit le dire.
        "proposera": statut == ST_VALIDE and refus is None,
    }

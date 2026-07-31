"""Résolution du compte comptable d'une charge (migration `0024`).

Trois niveaux de règle, dans cet ordre (cf. mission Analytique §2) :

1. mapping exact (`portee='CATEGORIE'`) actif et valide à la date de référence ;
2. mapping de type de flux (`portee='TYPE_FLUX'`) actif et valide à la date de référence ;
3. mapping provisoire générique (`portee='PROVISOIRE_GENERIQUE'`), explicitement autorisé —
   jamais un fallback silencieux : la résolution renvoie `statut='PROVISOIRE'`, jamais masqué.

Un mapping expiré (`date_fin_validite` dépassée) ou pas encore actif (`date_debut_validite` future)
n'est jamais utilisé, même s'il porte la bonne catégorie — la même règle temporelle que
`REF_Taux_Commission` / `REF_Gestion_Logements_Hist` dans les sources réelles (ne jamais appliquer
un tarif ou un rattachement actuel à une période historique).

Ce service ne CRÉE jamais de compte dans `plan_comptable` : il ne fait que choisir, parmi les
comptes déjà déclarés, celui que la règle active désigne.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from app.db.connection import get_db

PORTEE_CATEGORIE = "CATEGORIE"
PORTEE_TYPE_FLUX = "TYPE_FLUX"
PORTEE_PROVISOIRE = "PROVISOIRE_GENERIQUE"
PORTEES = (PORTEE_CATEGORIE, PORTEE_TYPE_FLUX, PORTEE_PROVISOIRE)

ST_PROVISOIRE = "PROVISOIRE"
ST_VALIDE = "VALIDE"

E_PORTEE_INCONNUE = "V01_PORTEE_INCONNUE"
E_CLE_MANQUANTE = "V02_CLE_MANQUANTE"
E_COMPTE_MANQUANT = "V03_COMPTE_MANQUANT"

MESSAGES = {
    E_PORTEE_INCONNUE: "Portée de mapping inconnue.",
    E_CLE_MANQUANTE: "La clé (catégorie ou type de flux) est obligatoire sauf portée provisoire générique.",
    E_COMPTE_MANQUANT: "Le compte cible est obligatoire.",
}


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _active_a_date(row: dict[str, Any], date_reference: str) -> bool:
    debut = row.get("date_debut_validite")
    fin = row.get("date_fin_validite")
    if debut and date_reference < debut:
        return False
    if fin and date_reference > fin:
        return False
    return True


def creer_regle(portee: str, compte: str, *, cle: str = "", statut: str = ST_PROVISOIRE,
                date_debut_validite: str = "", date_fin_validite: str = "", source: str = "",
                acteur: str = "", db_path=None) -> dict[str, Any]:
    portee = _txt(portee)
    if portee not in PORTEES:
        return _refus(E_PORTEE_INCONNUE, portee)
    if portee != PORTEE_PROVISOIRE and not _txt(cle):
        return _refus(E_CLE_MANQUANTE)
    if not _txt(compte):
        return _refus(E_COMPTE_MANQUANT)

    opaque = "MAP-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO mapping_comptable_regles (regle_id_opaque, portee, cle, compte, statut, "
            "date_debut_validite, date_fin_validite, source, acteur) VALUES (?,?,?,?,?,?,?,?,?)",
            (opaque, portee, _txt(cle) or None, compte,
             statut if statut in (ST_PROVISOIRE, ST_VALIDE) else ST_PROVISOIRE,
             date_debut_validite or None, date_fin_validite or None, _txt(source) or None,
             acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "regle_id_opaque": opaque}


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


def resoudre_compte(*, categorie_charge_id: str = "", type_flux_id: str = "",
                    date_reference: str = "", db_path=None) -> dict[str, Any]:
    """Résout le compte à utiliser pour une charge donnée. Ne lève jamais : renvoie toujours un
    résultat exploitable, avec `statut` VALIDE ou PROVISOIRE — jamais un compte choisi en silence
    sans que l'appelant puisse savoir sur quelle base."""
    date_reference = date_reference or date.today().isoformat()
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM mapping_comptable_regles").fetchall()
    finally:
        conn.close()
    regles = [dict(r) for r in rows]

    def _cherche(portee: str, cle: str) -> dict[str, Any] | None:
        candidats = [r for r in regles if r["portee"] == portee and r["cle"] == cle
                    and r["statut"] == ST_VALIDE and _active_a_date(r, date_reference)]
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
                         and _active_a_date(r, date_reference)]
        if prov_categorie:
            r = prov_categorie[0]
            return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                    "regle": PORTEE_CATEGORIE, "statut": ST_PROVISOIRE, "source": r.get("source")}
    if provisoires:
        r = provisoires[0]
        return {"compte": r["compte"], "regle_id_opaque": r["regle_id_opaque"],
                "regle": PORTEE_PROVISOIRE, "statut": ST_PROVISOIRE, "source": r.get("source")}

    # Aucune règle du tout (base vide) : compte générique en dur, dernier filet — signalé
    # A_CONTROLER, jamais présenté comme une résolution normale.
    return {"compte": "606000", "regle_id_opaque": None, "regle": "AUCUNE_REGLE",
            "statut": ST_PROVISOIRE, "source": "Aucune règle en base — filet absolu."}

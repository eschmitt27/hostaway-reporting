"""Barème kilométrique — montant INDICATIF d'une IK (contrôle seulement) et administration.

ALERTE, JAMAIS BLOCAGE
Ce service ne refuse aucune IK et ne modifie aucun montant : il calcule une référence et dit si le
montant saisi la dépasse. Création, modification, validation, clôture et comptabilisation de l'IK
n'en dépendent pas. Le montant comptable reste celui de la charge.

LE BARÈME EST ANNUEL, ET PAR VÉHICULE
Dans le barème, `d` est le kilométrage professionnel ANNUEL du véhicule. Une IK n'est donc pas
calculée isolément : pour chaque année couverte par ses trajets,
    cumul avant  = km de l'année déjà déclarés pour ce véhicule par les IK antérieures ;
    cumul après  = cumul avant + km de l'IK ;
    indicatif    = barème(cumul après) − barème(cumul avant).
Deux véhicules ont deux cumuls indépendants (identité stable : `ik_vehicules`).

UN RÉFÉRENTIEL ADMINISTRABLE ET VERSIONNÉ (migration 0118)
`ik_baremes` (année, version, statut BROUILLON / ACTIF / ARCHIVE) et `ik_bareme_tranches`
(valeurs structurées : montant = km × coefficient + constante). Un seul barème actif par année.
Rien n'est jamais supprimé. Une IK validée garde la référence du barème utilisé : créer ou modifier
une autre année ne change pas son contrôle.

ANNÉE NON CONFIGURÉE
On retient le dernier barème actif ANTÉRIEUR, et l'écran le dit en toutes lettres — jamais un
ancien barème présenté comme celui de l'année.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

EPS = 0.005
TYPES_VEHICULE = {"AUTO": "Voiture", "MOTO": "Motocyclette (plus de 50 cm³)",
                  "CYCLO": "Cyclomoteur (50 cm³ et moins)"}
MOTORISATIONS = {"THERMIQUE": "Thermique / hybride / hydrogène",
                 "ELECTRIQUE": "100 % électrique"}
ST_BROUILLON, ST_ACTIF, ST_ARCHIVE = "BROUILLON", "ACTIF", "ARCHIVE"
LIBELLES_STATUT = {ST_BROUILLON: "Brouillon", ST_ACTIF: "Actif", ST_ARCHIVE: "Archivé"}

E_INTROUVABLE = "BK01_BAREME_INTROUVABLE"
E_NON_MODIFIABLE = "BK02_BAREME_NON_MODIFIABLE"
E_INCOHERENT = "BK03_BAREME_INCOHERENT"
E_STATUT = "BK04_TRANSITION_INTERDITE"
E_ANNEE = "BK05_ANNEE_INVALIDE"
E_AUCUN_MODELE = "BK06_AUCUN_BAREME_A_COPIER"


def _r(v) -> float:
    return round(float(v or 0), 2)


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, message: str, erreurs: list[str] | None = None) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message, "erreurs": erreurs or [message]}


def _tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


# ── Lecture du référentiel ───────────────────────────────────────────────────────────────────────

def _bareme(conn, bid: str) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM ik_baremes WHERE bareme_id_opaque = ?", (bid,)).fetchone()
    if r is None:
        return None
    b = dict(r)
    b["tranches"] = [dict(t) for t in conn.execute(
        "SELECT * FROM ik_bareme_tranches WHERE bareme_id_opaque = ? "
        "ORDER BY type_vehicule, cv_min, km_min", (bid,))]
    return b


def _bareme_applicable(conn, annee: int) -> dict[str, Any] | None:
    """Barème ACTIF de l'année, ou à défaut le dernier ACTIF antérieur."""
    r = conn.execute("SELECT bareme_id_opaque FROM ik_baremes WHERE statut = 'ACTIF' AND annee <= ? "
                     "ORDER BY annee DESC LIMIT 1", (annee,)).fetchone()
    return _bareme(conn, r[0]) if r else None


def bareme_applicable(annee: int, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        return _bareme_applicable(conn, annee) if "ik_baremes" in _tables(conn) else None
    finally:
        conn.close()


def _theorique(bareme: dict[str, Any], vehicule: dict[str, Any], km: float) -> float | None:
    """Montant théorique du barème pour un kilométrage ANNUEL `km` (None : aucune tranche)."""
    if km <= 0:
        return 0.0
    cv = int(vehicule.get("puissance_fiscale") or 0)
    for t in bareme["tranches"]:
        if (t["type_vehicule"] == vehicule["type_vehicule"] and t["cv_min"] <= cv
                and (t["cv_max"] is None or cv <= t["cv_max"])
                and t["km_min"] < km and (t["km_max"] is None or km <= t["km_max"])):
            base = km * float(t["coefficient"]) + float(t["constante"] or 0)
            if vehicule.get("motorisation") == "ELECTRIQUE":
                base *= 1 + float(bareme["majoration_electrique"] or 0)
            return base
    return None


def _message_repli(bareme: dict[str, Any], annee: int) -> str:
    return ("" if bareme["annee"] == annee else
            f"Estimation basée sur le dernier barème officiel disponible : barème {bareme['annee']}.")


def montant_indicatif(km: float, type_vehicule: str, puissance: int | None, motorisation: str,
                      annee: int, *, km_avant: float = 0.0, db_path=None) -> dict[str, Any]:
    """Indicatif d'un kilométrage `km` venant après `km_avant` km déjà déclarés dans l'année."""
    vehicule = {"type_vehicule": (type_vehicule or "").upper(), "puissance_fiscale": puissance,
                "motorisation": (motorisation or "THERMIQUE").upper()}
    if vehicule["type_vehicule"] not in TYPES_VEHICULE:
        return {"ok": False, "manque": "Renseignez le véhicule (type, puissance fiscale, motorisation)."}
    b = bareme_applicable(annee, db_path=db_path)
    if b is None:
        return {"ok": False, "manque": f"Aucun barème configuré pour {annee} ni pour une année antérieure."}
    avant, apres = _theorique(b, vehicule, km_avant), _theorique(b, vehicule, km_avant + km)
    if avant is None or apres is None:
        return {"ok": False, "manque": "Aucune tranche du barème ne correspond à ce véhicule."}
    return {"ok": True, "montant": _r(apres - avant), "annee": b["annee"], "annee_demandee": annee,
            "bareme_id_opaque": b["bareme_id_opaque"], "repli": _message_repli(b, annee)}


# ── Contrôle d'une IK : cumul annuel par véhicule ────────────────────────────────────────────────

def vehicule(vehicule_id: str, *, db_path=None) -> dict[str, Any] | None:
    if not vehicule_id:
        return None
    conn = get_db(db_path)
    try:
        if "ik_vehicules" not in _tables(conn):
            return None
        r = conn.execute("SELECT * FROM ik_vehicules WHERE vehicule_id_opaque = ?",
                         (vehicule_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def _km_par_annee(conn, ik_id: str) -> dict[int, float]:
    out: dict[int, float] = defaultdict(float)
    for r in conn.execute("SELECT date_trajet, km FROM ik_trajets WHERE ik_id_opaque = ? AND actif = 1",
                          (ik_id,)):
        out[int(str(r["date_trajet"])[:4])] += float(r["km"] or 0)
    return dict(out)


def _km_avant(conn, ik: dict[str, Any], annee: int) -> float:
    """Km de l'année déjà déclarés pour le même véhicule par les IK ANTÉRIEURES (charge active)."""
    r = conn.execute(
        "SELECT COALESCE(SUM(t.km), 0) FROM ik_trajets t JOIN ik i ON i.ik_id_opaque = t.ik_id_opaque "
        "JOIN charges c ON c.charge_id = i.charge_id "
        "WHERE t.actif = 1 AND c.statut = 'ACTIVE' AND i.vehicule_id = ? AND i.ik_id_opaque <> ? "
        "AND substr(t.date_trajet, 1, 4) = ? AND (i.date_debut < ? OR (i.date_debut = ? AND i.id < ?))",
        (ik["vehicule_id"], ik["ik_id_opaque"], f"{annee:04d}", ik["date_debut"], ik["date_debut"],
         ik["id"])).fetchone()
    return float(r[0] or 0)


def controle(ik: dict[str, Any], *, db_path=None) -> dict[str, Any]:
    """Compare le montant de l'IK (celui de la charge) au montant indicatif. Lecture seule."""
    saisi = _r(ik.get("montant"))
    base = {"montant_saisi": saisi, "ok": False, "depassement": False, "ecart": None,
            "details": [], "repli": ""}
    veh = vehicule(ik.get("vehicule_id") or "", db_path=db_path)
    if veh is None:
        return {**base, "manque": "Renseignez le véhicule (type, puissance fiscale, motorisation)."}
    conn = get_db(db_path)
    try:
        par_annee = _km_par_annee(conn, ik["ik_id_opaque"])
        if not par_annee:
            return {**base, "vehicule": veh,
                    "manque": "Ajoutez les trajets : le calcul part des kilomètres du relevé."}
        fige = (_bareme(conn, ik["bareme_id_opaque"])
                if ik.get("statut") == "VALIDEE" and ik.get("bareme_id_opaque") else None)
        details, total, replis = [], 0.0, []
        for annee in sorted(par_annee):
            b = fige or _bareme_applicable(conn, annee)
            if b is None:
                return {**base, "vehicule": veh,
                        "manque": f"Aucun barème configuré pour {annee} ni pour une année antérieure."}
            km = par_annee[annee]
            avant_km = _km_avant(conn, ik, annee)
            t_avant, t_apres = _theorique(b, veh, avant_km), _theorique(b, veh, avant_km + km)
            if t_avant is None or t_apres is None:
                return {**base, "vehicule": veh,
                        "manque": "Aucune tranche du barème ne correspond à ce véhicule."}
            part = t_apres - t_avant
            total += part
            if _message_repli(b, annee):
                replis.append(_message_repli(b, annee))
            details.append({"annee": annee, "bareme_annee": b["annee"],
                            "bareme_id_opaque": b["bareme_id_opaque"], "km_avant": avant_km,
                            "km_ik": km, "km_apres": avant_km + km, "theorique_avant": _r(t_avant),
                            "theorique_apres": _r(t_apres), "part": _r(part)})
    finally:
        conn.close()
    montant = _r(total)
    return {**base, "ok": True, "vehicule": veh, "montant": montant, "details": details,
            "repli": " ".join(dict.fromkeys(replis)), "fige": fige is not None,
            "depassement": saisi > montant + EPS, "ecart": _r(saisi - montant)}


def bareme_pour_validation(ik: dict[str, Any], *, db_path=None) -> str | None:
    """Barème à figer sur une IK qu'on valide : celui applicable à l'année de fin de période."""
    b = bareme_applicable(int(str(ik.get("date_fin") or "0")[:4] or 0), db_path=db_path)
    return b["bareme_id_opaque"] if b else None


# ── Administration ───────────────────────────────────────────────────────────────────────────────

def _nb_ik_validees(conn, bid: str) -> int:
    return conn.execute("SELECT COUNT(*) FROM ik WHERE bareme_id_opaque = ? AND statut = 'VALIDEE'",
                        (bid,)).fetchone()[0]


def lister(*, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if "ik_baremes" not in _tables(conn):
            return []
        out = []
        for r in conn.execute("SELECT * FROM ik_baremes ORDER BY annee DESC, version DESC"):
            b = dict(r)
            b["nb_tranches"] = conn.execute("SELECT COUNT(*) FROM ik_bareme_tranches WHERE "
                                            "bareme_id_opaque = ?", (b["bareme_id_opaque"],)).fetchone()[0]
            b["nb_ik_validees"] = _nb_ik_validees(conn, b["bareme_id_opaque"])
            b["statut_libelle"] = LIBELLES_STATUT.get(b["statut"], b["statut"])
            out.append(b)
        return out
    finally:
        conn.close()


def charger(bid: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        b = _bareme(conn, bid)
        if b is None:
            return None
        b["nb_ik_validees"] = _nb_ik_validees(conn, bid)
        b["statut_libelle"] = LIBELLES_STATUT.get(b["statut"], b["statut"])
        b["modifiable"] = b["statut"] == ST_BROUILLON or (b["statut"] == ST_ACTIF
                                                           and b["nb_ik_validees"] == 0)
        return b
    finally:
        conn.close()


def verifier(tranches: list[dict[str, Any]], majoration: Any) -> list[str]:
    """Cohérence d'un barème. Retourne la liste des erreurs (vide = cohérent)."""
    erreurs: list[str] = []
    try:
        m = float(str(majoration).replace(",", "."))
        if not 0 <= m <= 1:
            erreurs.append("La majoration électrique doit être comprise entre 0 et 1 (0,20 = +20 %).")
    except (TypeError, ValueError):
        erreurs.append("La majoration électrique doit être un nombre (0,20 = +20 %).")
    if not tranches:
        erreurs.append("Un barème doit comporter au moins une tranche.")
    groupes: dict[tuple, list[dict]] = defaultdict(list)
    for i, t in enumerate(tranches, 1):
        if t["type_vehicule"] not in TYPES_VEHICULE:
            erreurs.append(f"Tranche {i} : type de véhicule inconnu.")
            continue
        if t["cv_min"] < 0 or (t["cv_max"] is not None and t["cv_max"] < t["cv_min"]):
            erreurs.append(f"Tranche {i} : catégorie CV incohérente.")
        if t["km_min"] < 0 or (t["km_max"] is not None and t["km_max"] <= t["km_min"]):
            erreurs.append(f"Tranche {i} : bornes kilométriques incohérentes.")
        if t["coefficient"] < 0 or t["constante"] < 0:
            erreurs.append(f"Tranche {i} : coefficient et constante doivent être positifs.")
        groupes[(t["type_vehicule"], t["cv_min"], t["cv_max"])].append(t)
    # Catégories CV d'un même type : identiques ou disjointes, jamais à cheval.
    par_type: dict[str, list[tuple]] = defaultdict(list)
    for (typ, a, b) in groupes:
        par_type[typ].append((a, b))
    for typ, plages in par_type.items():
        for i, (a1, b1) in enumerate(plages):
            for a2, b2 in plages[i + 1:]:
                fin1 = b1 if b1 is not None else 10 ** 6
                fin2 = b2 if b2 is not None else 10 ** 6
                if a1 <= fin2 and a2 <= fin1:
                    erreurs.append(f"{TYPES_VEHICULE[typ]} : les catégories CV {a1}–{b1 or '+'} et "
                                   f"{a2}–{b2 or '+'} se chevauchent.")
    # Tranches kilométriques d'une catégorie : de 0 à l'infini, sans trou ni chevauchement.
    for (typ, a, b), ts in groupes.items():
        ts = sorted(ts, key=lambda t: t["km_min"])
        nom = f"{TYPES_VEHICULE[typ]} {a}–{b if b is not None else '+'} CV"
        if ts[0]["km_min"] != 0:
            erreurs.append(f"{nom} : la première tranche doit commencer à 0 km.")
        for p, s in zip(ts, ts[1:]):
            if p["km_max"] is None or abs(s["km_min"] - p["km_max"]) > 1e-9:
                erreurs.append(f"{nom} : tranches kilométriques qui se chevauchent ou laissent un trou "
                               f"(après {p['km_max'] if p['km_max'] is not None else 'l’infini'} km).")
        if ts[-1]["km_max"] is not None:
            erreurs.append(f"{nom} : la dernière tranche doit être ouverte (« au-delà »).")
    return list(dict.fromkeys(erreurs))


def lire_tranches_formulaire(colonnes: dict[str, list[str]]) -> tuple[list[dict], list[str]]:
    """Formulaire → tranches typées. Les lignes vides ou cochées « retirer » sont ignorées."""
    def nombre(v, entier=False):
        v = str(v or "").strip().replace(",", ".").replace(" ", "")
        if v == "":
            return None
        x = float(v)
        return int(x) if entier else x

    n = max((len(v) for v in colonnes.values()), default=0)
    tranches, erreurs = [], []
    retirer = set(colonnes.get("retirer", []))
    for i in range(n):
        ligne = {k: (colonnes[k][i] if i < len(colonnes.get(k, [])) else "") for k in
                 ("type_vehicule", "cv_min", "cv_max", "km_min", "km_max", "coefficient", "constante")}
        if str(i) in retirer or not any(str(ligne[k]).strip() for k in
                                        ("cv_min", "km_min", "coefficient")):
            continue
        try:
            tranches.append({"type_vehicule": ligne["type_vehicule"].upper() or "AUTO",
                             "cv_min": nombre(ligne["cv_min"], True) or 0,
                             "cv_max": nombre(ligne["cv_max"], True),
                             "km_min": nombre(ligne["km_min"]) or 0.0,
                             "km_max": nombre(ligne["km_max"]),
                             "coefficient": nombre(ligne["coefficient"]),
                             "constante": nombre(ligne["constante"]) or 0.0})
            if tranches[-1]["coefficient"] is None:
                raise ValueError
        except (TypeError, ValueError):
            erreurs.append(f"Ligne {i + 1} : valeurs numériques invalides.")
    return tranches, erreurs


def _copier(conn, source: dict[str, Any], annee: int, acteur: str) -> str:
    version = conn.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM ik_baremes WHERE annee = ?",
                           (annee,)).fetchone()[0]
    bid = f"IKB-{annee}-V{version}-{uuid.uuid4().hex[:6].upper()}"
    conn.execute("INSERT INTO ik_baremes (bareme_id_opaque, annee, version, statut, "
                 "majoration_electrique, source, cree_par) VALUES (?,?,?,?,?,?,?)",
                 (bid, annee, version, ST_BROUILLON, source["majoration_electrique"],
                  f"Copie du barème {source['annee']} (v{source['version']}) — à vérifier",
                  acteur or None))
    conn.executemany(
        "INSERT INTO ik_bareme_tranches (bareme_id_opaque, type_vehicule, cv_min, cv_max, km_min, "
        "km_max, coefficient, constante) VALUES (?,?,?,?,?,?,?,?)",
        [(bid, t["type_vehicule"], t["cv_min"], t["cv_max"], t["km_min"], t["km_max"],
          t["coefficient"], t["constante"]) for t in source["tranches"]])
    return bid


def creer_annee(annee: Any, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """« Ajouter un barème annuel » : copie du dernier barème existant, en BROUILLON."""
    try:
        annee = int(str(annee).strip())
    except ValueError:
        return _refus(E_ANNEE, "Année invalide.")
    if not 2000 <= annee <= 2100:
        return _refus(E_ANNEE, "Année invalide.")
    conn = get_db(db_path)
    try:
        r = (conn.execute("SELECT bareme_id_opaque FROM ik_baremes WHERE statut = 'ACTIF' "
                          "ORDER BY annee DESC LIMIT 1").fetchone()
             or conn.execute("SELECT bareme_id_opaque FROM ik_baremes "
                             "ORDER BY annee DESC, version DESC LIMIT 1").fetchone())
        if r is None:
            return _refus(E_AUCUN_MODELE, "Aucun barème existant à copier.")
        bid = _copier(conn, _bareme(conn, r[0]), annee, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "bareme_id_opaque": bid}


def nouvelle_version(bid: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Corriger un barème déjà utilisé : on en crée une nouvelle version, l'ancien reste intact."""
    conn = get_db(db_path)
    try:
        b = _bareme(conn, bid)
        if b is None:
            return _refus(E_INTROUVABLE, "Barème introuvable.")
        nouveau = _copier(conn, b, b["annee"], acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "bareme_id_opaque": nouveau}


def enregistrer(bid: str, tranches: list[dict[str, Any]], majoration: Any, source: str = "", *,
                db_path=None) -> dict[str, Any]:
    b = charger(bid, db_path=db_path)
    if b is None:
        return _refus(E_INTROUVABLE, "Barème introuvable.")
    if not b["modifiable"]:
        return _refus(E_NON_MODIFIABLE,
                      "Ce barème n'est plus modifiable (archivé, ou déjà utilisé par une IK validée) : "
                      "créez une nouvelle version.")
    erreurs = verifier(tranches, majoration)
    if erreurs:
        return _refus(E_INCOHERENT, "Barème incohérent : " + erreurs[0], erreurs)
    conn = get_db(db_path)
    try:
        conn.execute("DELETE FROM ik_bareme_tranches WHERE bareme_id_opaque = ?", (bid,))
        conn.executemany(
            "INSERT INTO ik_bareme_tranches (bareme_id_opaque, type_vehicule, cv_min, cv_max, km_min, "
            "km_max, coefficient, constante) VALUES (?,?,?,?,?,?,?,?)",
            [(bid, t["type_vehicule"], t["cv_min"], t["cv_max"], t["km_min"], t["km_max"],
              t["coefficient"], t["constante"]) for t in tranches])
        conn.execute("UPDATE ik_baremes SET majoration_electrique = ?, source = ?, modifie_le = ? "
                     "WHERE bareme_id_opaque = ?",
                     (float(str(majoration).replace(",", ".")), source or b["source"], _maintenant(), bid))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


def activer(bid: str, *, db_path=None) -> dict[str, Any]:
    """BROUILLON → ACTIF ; l'actif précédent de la même année est archivé (un seul actif)."""
    b = charger(bid, db_path=db_path)
    if b is None:
        return _refus(E_INTROUVABLE, "Barème introuvable.")
    if b["statut"] != ST_BROUILLON:
        return _refus(E_STATUT, "Seul un barème en brouillon peut être activé.")
    erreurs = verifier(b["tranches"], b["majoration_electrique"])
    if erreurs:
        return _refus(E_INCOHERENT, "Barème incohérent : " + erreurs[0], erreurs)
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE ik_baremes SET statut = 'ARCHIVE', modifie_le = ? WHERE annee = ? "
                     "AND statut = 'ACTIF'", (_maintenant(), b["annee"]))
        conn.execute("UPDATE ik_baremes SET statut = 'ACTIF', modifie_le = ? WHERE bareme_id_opaque = ?",
                     (_maintenant(), bid))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


def archiver(bid: str, *, db_path=None) -> dict[str, Any]:
    b = charger(bid, db_path=db_path)
    if b is None:
        return _refus(E_INTROUVABLE, "Barème introuvable.")
    if b["statut"] == ST_ARCHIVE:
        return _refus(E_STATUT, "Ce barème est déjà archivé.")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE ik_baremes SET statut = 'ARCHIVE', modifie_le = ? WHERE bareme_id_opaque = ?",
                     (_maintenant(), bid))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

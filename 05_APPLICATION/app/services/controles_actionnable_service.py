"""Orchestration APP-5B — contrôles détaillés actionnables (moteur + suivi humain).

Couche au-dessus de :
  - controles_cloture_service (APP-5A) : contrôles moteur agrégés + clôturabilité (inchangé) ;
  - controles_detail_service           : ouverture des agrégats en éléments actionnables + CTRL opaque ;
  - controles_suivi_service            : journal du suivi humain (SQLite isolé).

Compose, sans jamais masquer une anomalie moteur présente, l'état effectif de chaque élément détaillé
(vues, cartes, filtres, préparation clôture). Lecture seule côté sources métier ; seules les décisions
de suivi écrivent, et uniquement dans l'app.db isolée (flags réels False).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import controles_cloture_service as base
from app.services import controles_detail_service as det
from app.services import controles_suivi_service as suivi

# Vues de l'écran principal.
VUES = {
    "tous": "Tous",
    "a_traiter": "À traiter",
    "en_cours": "En cours",
    "resolus": "Résolus",
    "exceptions": "Exceptions acceptées",
    "informatifs": "Informatifs",
    "reapparus": "Réapparus",
    "incoherences": "Incohérences de suivi",
}

NIVEAU_INFO = "INFO"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _anomalie_moteur_presente(el: dict[str, Any]) -> bool:
    """Une anomalie est présente tant qu'elle figure dans la sortie moteur courante.

    Les INFO ne sont pas des anomalies « à corriger » : présence moteur = True pour A_CONTROLER/
    BLOQUANT, False pour INFO (informatif, aucune action requise).
    """
    return el.get("niveau", "").upper() not in ("", NIVEAU_INFO)


def _tous_les_elements(db_path=None) -> list[dict[str, Any]]:
    """Tous les éléments détaillés (agrégats ouverts) enrichis de leur suivi et état effectif."""
    vues = base._toutes_les_vues()
    suivis = suivi.index_suivis_actifs(db_path)
    elements: list[dict[str, Any]] = []
    for v in vues:
        if not det.est_detaillable(v["code"]):
            # contrôle déjà au grain unitaire (INFO par logement, etc.) : élément direct.
            el = {
                "ctrl_opaque": det.id_opaque(v["code"], v["entite"], v["mois"]),
                "ctrl_pk_moteur": v["stable_id"], "parent_stable_id": v["stable_id"],
                "code": v["code"], "module": v["module"], "niveau": v["niveau"], "mois": v["mois"],
                "entite_id": v["entite"], "resume": v["message"] or v["code"],
                "classification": "", "classification_libelle": "",
                "donnees": {"entite": v["entite"], "message": v["message"]},
                "lien_module": None, "lien_libelle": "",
                "detaille": False,
            }
            elements.append(el)
        else:
            for el in det.expand(v):
                el["detaille"] = True
                elements.append(el)
    # composition état effectif
    for el in elements:
        amp = _anomalie_moteur_presente(el)
        sv = suivis.get(el["ctrl_opaque"])
        el["suivi"] = sv
        el["etat"] = suivi.etat_effectif(el, sv, amp)
        el["est_info"] = el["niveau"].upper() == NIVEAU_INFO
    return elements


# ── Filtrage par vue ─────────────────────────────────────────────────────────

def _dans_vue(el: dict[str, Any], vue: str) -> bool:
    etat = el["etat"]
    st = etat["statut_suivi"]
    if vue == "tous":
        return True
    if vue == "informatifs":
        return el["est_info"]
    if el["est_info"]:
        return False  # INFO n'apparaît pas dans les vues d'anomalies
    if vue == "a_traiter":
        return st == suivi.ST_OUVERT
    if vue == "en_cours":
        return st in (suivi.ST_EN_COURS,)
    if vue == "resolus":
        return st == suivi.ST_RESOLU
    if vue == "exceptions":
        return st == suivi.ST_ACCEPTE
    if vue == "reapparus":
        return st == suivi.ST_ROUVERT
    if vue == "incoherences":
        return etat["incoherence_suivi"] or etat["suivi_a_cloturer"]
    return True


def _match_filtres(el, mois, module, niveau, code, statut_suivi, responsable, proprietaire,
                   logement, recherche, actionnables_seul, cloture_bloquee, classification="") -> bool:
    if mois and el["mois"] != mois:
        return False
    if module and el["module"] != module:
        return False
    if niveau and el["niveau"].upper() != niveau.upper():
        return False
    if code and el["code"] != code:
        return False
    if classification and el.get("classification", "") != classification:
        return False
    if statut_suivi and el["etat"]["statut_suivi"] != statut_suivi:
        return False
    if responsable and (el.get("suivi") or {}).get("responsable", "") != responsable:
        return False
    d = el.get("donnees", {})
    if proprietaire and d.get("proprietaire", "") != proprietaire:
        return False
    if logement and d.get("logement", d.get("logement_id", "")) != logement:
        return False
    if actionnables_seul and not el.get("lien_module"):
        return False
    if cloture_bloquee and (el["est_info"] or el["niveau"].upper() == "INFO"):
        return False
    if recherche:
        r = recherche.lower()
        blob = " ".join(str(x) for x in [el["resume"], el["code"], el["entite_id"],
                                         el.get("classification_libelle", "")]).lower()
        if r not in blob:
            return False
    return True


# ── Cartes de synthèse ───────────────────────────────────────────────────────

def _cartes(elements: list[dict[str, Any]]) -> dict[str, int]:
    anomalies = [e for e in elements if not e["est_info"]]
    return {
        "anomalies_moteur": sum(1 for e in anomalies if e["etat"]["anomalie_moteur_presente"]),
        "a_traiter": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == suivi.ST_OUVERT),
        "en_cours": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == suivi.ST_EN_COURS),
        "resolus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == suivi.ST_RESOLU),
        "exceptions": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == suivi.ST_ACCEPTE),
        "reapparus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == suivi.ST_ROUVERT),
        "incoherences": sum(1 for e in anomalies if e["etat"]["incoherence_suivi"] or e["etat"]["suivi_a_cloturer"]),
        "informatifs": sum(1 for e in elements if e["est_info"]),
        "bloquants_cloture": sum(1 for e in anomalies
                                 if e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]),
    }


# ── Dashboard actionnable ────────────────────────────────────────────────────

TAILLE_PAGE = 25


def load_dashboard(vue: str = "tous", mois: str = "", module: str = "", niveau: str = "", code: str = "",
                   statut_suivi: str = "", responsable: str = "", proprietaire: str = "", logement: str = "",
                   recherche: str = "", actionnables_seul: bool = False, cloture_bloquee: bool = False,
                   page: int = 1, classification: str = "", db_path=None) -> dict[str, Any]:
    if vue not in VUES:
        vue = "tous"
    elements = _tous_les_elements(db_path)
    cartes = _cartes(elements)
    filtres = [e for e in elements if _dans_vue(e, vue)
               and _match_filtres(e, mois, module, niveau, code, statut_suivi, responsable,
                                   proprietaire, logement, recherche, actionnables_seul, cloture_bloquee,
                                   classification)]
    filtres.sort(key=lambda e: (0 if not e["est_info"] else 1, e["module"], e["code"], e["entite_id"]))
    pages = max(1, (len(filtres) + TAILLE_PAGE - 1) // TAILLE_PAGE)
    page = min(max(1, page), pages)
    debut = (page - 1) * TAILLE_PAGE
    options = _options(elements)
    return {
        "vue": vue, "vues": VUES, "cartes": cartes,
        "rows": filtres[debut:debut + TAILLE_PAGE], "page": page, "pages": pages,
        "count_total": len(elements), "count_filtre": len(filtres),
        "options": options, "preparation_cloture": preparation_cloture(elements),
        "applied": {"vue": vue, "mois": mois, "module": module, "niveau": niveau, "code": code,
                    "statut_suivi": statut_suivi, "responsable": responsable, "proprietaire": proprietaire,
                    "logement": logement, "recherche": recherche, "actionnables_seul": actionnables_seul,
                    "cloture_bloquee": cloture_bloquee, "page": page, "classification": classification},
        "read_at": _now(),
    }


def _options(elements: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    def uniq(key):
        return sorted({v for v in (str(e.get(key, "")) for e in elements) if v})
    mois = uniq("mois")
    modules = uniq("module")
    codes = uniq("code")
    classifications = sorted({e.get("classification", "") for e in elements if e.get("classification")})
    props = sorted({e["donnees"].get("proprietaire", "") for e in elements if e["donnees"].get("proprietaire")})
    logs = sorted({e["donnees"].get("logement", e["donnees"].get("logement_id", "")) for e in elements
                   if e["donnees"].get("logement") or e["donnees"].get("logement_id")})
    resp = sorted({(e.get("suivi") or {}).get("responsable", "") for e in elements
                   if (e.get("suivi") or {}).get("responsable")})
    return {
        "mois": [{"id": m, "libelle": m} for m in mois],
        "modules": [{"id": m, "libelle": m} for m in modules],
        "niveaux": [{"id": n, "libelle": n} for n in ("BLOQUANT", "A_CONTROLER", "INFO")],
        "codes": [{"id": c, "libelle": c} for c in codes],
        "classifications": [{"id": c, "libelle": c} for c in classifications],
        "statuts_suivi": [{"id": s, "libelle": suivi.STATUTS_LIBELLES[s]} for s in
                          (suivi.ST_OUVERT, suivi.ST_EN_COURS, suivi.ST_RESOLU, suivi.ST_ACCEPTE, suivi.ST_ROUVERT)],
        "responsables": [{"id": r, "libelle": r} for r in resp],
        "proprietaires": [{"id": p, "libelle": p} for p in props],
        "logements": [{"id": l, "libelle": l} for l in logs],
    }


# ── Fiche détaillée ──────────────────────────────────────────────────────────

def load_fiche(ctrl_opaque: str, db_path=None) -> dict[str, Any] | None:
    elements = _tous_les_elements(db_path)
    el = next((e for e in elements if e["ctrl_opaque"] == str(ctrl_opaque).strip()), None)
    if el is None:
        return None
    # réouverture auto si réapparu (anomalie moteur présente + suivi résolu)
    if not el["est_info"]:
        suivi.reouvrir_auto_si_reapparu(el, anomalie_moteur_presente=el["etat"]["anomalie_moteur_presente"],
                                        db_path=db_path)
    sv = suivi.suivi_actif(ctrl_opaque, db_path)
    el["suivi"] = sv
    el["etat"] = suivi.etat_effectif(el, sv, _anomalie_moteur_presente(el))
    return {
        "status": "OK", "element": el, "suivi": sv,
        "historique": suivi.historique(ctrl_opaque, db_path),
        "actions_possibles": _actions_possibles(el),
        "read_at": _now(),
    }


def _actions_possibles(el: dict[str, Any]) -> list[str]:
    if el["est_info"]:
        return ["commenter", "masquer_info"]   # un INFO n'a pas de résolution métier par défaut
    st = el["etat"]["statut_suivi"]
    actions = ["commenter"]
    if st in (suivi.ST_OUVERT, suivi.ST_ROUVERT):
        actions += ["prendre_en_charge", "accepter_exception"]
    if st == suivi.ST_EN_COURS:
        actions += ["marquer_corrige", "accepter_exception", "annuler"]
    if st in (suivi.ST_RESOLU, suivi.ST_ACCEPTE):
        actions += ["rouvrir", "annuler"]
    if el.get("lien_module"):
        actions.append("recalcul_copie")
    return actions


# ── Préparation clôture (sans clôturer — APP-5C plus tard) ────────────────────

STATUT_NON_PRET = "NON_PRET"
STATUT_A_CONTROLER = "A_CONTROLER"
STATUT_PRET_SOUS_EXCEPTION = "PRET_SOUS_EXCEPTION"
STATUT_PRET = "PRET"


def preparation_cloture(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Statut de préparation par mois — affichage seul, aucune clôture réelle (APP-5C non actif)."""
    par_mois: dict[str, dict[str, int]] = {}
    for e in elements:
        if e["est_info"]:
            continue
        m = e["mois"] or "—"
        d = par_mois.setdefault(m, {"anomalies": 0, "exceptions": 0, "non_exception": 0})
        if e["etat"]["anomalie_moteur_presente"]:
            d["anomalies"] += 1
            if e["etat"]["exception_active"]:
                d["exceptions"] += 1
            else:
                d["non_exception"] += 1
    out = []
    for m in sorted(par_mois, reverse=True):
        d = par_mois[m]
        if d["non_exception"] > 0:
            statut = STATUT_A_CONTROLER if d["non_exception"] else STATUT_NON_PRET
        elif d["exceptions"] > 0:
            statut = STATUT_PRET_SOUS_EXCEPTION
        else:
            statut = STATUT_PRET
        out.append({"mois": m, "statut": statut, "nb_anomalies": d["anomalies"],
                    "nb_exceptions": d["exceptions"], "nb_bloquantes": d["non_exception"],
                    "cloture_reelle_active": False})
    return out


# ── Export CSV enrichi ───────────────────────────────────────────────────────

_CSV_COLS = [
    ("ctrl_opaque", "controle_id_opaque"), ("mois", "periode"), ("module", "module"),
    ("code", "code"), ("niveau", "niveau"), ("resume", "resume"),
    ("_proprietaire", "proprietaire"), ("_logement", "logement"), ("entite_id", "entite"),
    ("_anomalie", "anomalie_moteur_presente"), ("_statut_suivi", "statut_suivi"),
    ("_resultat", "resultat_humain"), ("_responsable", "responsable"),
    ("_commentaire", "commentaire"), ("_justification", "justification"),
    ("_impact", "impact_cloture"), ("lien_module", "lien_module"),
    ("classification_libelle", "classification"),
]


def export_csv(vue: str = "tous", db_path=None, **filtres) -> str:
    import csv
    import io
    filtres.pop("page", None)
    data = load_dashboard(vue=vue, page=1, db_path=db_path, **filtres)
    # récupérer TOUT le filtré (pas seulement la page)
    elements = _tous_les_elements(db_path)
    rows = [e for e in elements if _dans_vue(e, data["vue"])
            and _match_filtres(e, filtres.get("mois", ""), filtres.get("module", ""),
                               filtres.get("niveau", ""), filtres.get("code", ""),
                               filtres.get("statut_suivi", ""), filtres.get("responsable", ""),
                               filtres.get("proprietaire", ""), filtres.get("logement", ""),
                               filtres.get("recherche", ""), filtres.get("actionnables_seul", False),
                               filtres.get("cloture_bloquee", False), filtres.get("classification", ""))]
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([lab for _, lab in _CSV_COLS])
    for e in rows:
        d = e.get("donnees", {})
        sv = e.get("suivi") or {}
        etat = e["etat"]
        vals = {
            "_proprietaire": d.get("proprietaire", ""),
            "_logement": d.get("logement", d.get("logement_id", "")),
            "_anomalie": "OUI" if etat["anomalie_moteur_presente"] else "NON",
            "_statut_suivi": etat["statut_suivi_libelle"],
            "_resultat": etat["resultat_humain"],
            "_responsable": sv.get("responsable", ""),
            "_commentaire": sv.get("commentaire", ""),
            "_justification": sv.get("justification", ""),
            "_impact": "Bloque la clôture" if (etat["anomalie_moteur_presente"] and not etat["exception_active"])
                       else ("Exception" if etat["exception_active"] else "—"),
        }
        w.writerow([vals.get(k, e.get(k, "")) if k.startswith("_") else e.get(k, "") for k, _ in _CSV_COLS])
    return buf.getvalue()

"""Service Contrôles & clôture (APP-5A) — orchestration d'affichage, LECTURE SEULE.

Lit les contrôles consolidés du moteur (Lot11 MASTER_CTRL_Coherence) et l'état de clôture
(REF_Cloture_Mensuelle). Ne recrée aucun contrôle, ne déclasse aucun BLOQUANT, ne clôture rien.

Règles :
- code, niveau (severity) et statut viennent du moteur — jamais inventés ;
- un mois clôturé reste affiché clôturé ; une anomalie subsistant après clôture reste visible ;
- SQLite n'est pas une source de clôture ; aucune écriture ; aucun acquittement réel ;
- les explications sont un simple dictionnaire d'affichage ; aucun chemin absolu.
"""
from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime
from typing import Any

from app.readers import controles_cloture_reader as reader
from app.readers.controles_cloture_reader import to_texte, to_nombre, to_mois, to_date

TAILLE_PAGE = 25

NIVEAU_BLOQUANT = "BLOQUANT"
NIVEAUX_ORDRE = {"BLOQUANT": 0, "A_CONTROLER": 1, "AVERTISSEMENT": 1, "INFO": 2, "": 3}

TRIS = {"niveau": "Niveau (bloquants d'abord)", "periode": "Période", "module": "Module", "code": "Code"}

# Statuts de clôture des mois (REF_Cloture_Mensuelle) — repris tels quels.
STATUT_OUVERT = "OUVERT"
STATUT_EN_CONTROLE = "EN_CONTROLE"
STATUT_CLOTURE = "CLOTURE"

# Explications LISIBLES (affichage seul — le code moteur reste autoritaire).
EXPLICATIONS: dict[str, str] = {
    "MENAGE_TOTAL_ECART_HOSTAWAY": "Le total des ménages déclarés ne couvre pas les tâches Hostaway réalisées.",
    "CONFLIT_TITLE_ASSIGNEE": "Le titre de la tâche Hostaway et son intervenant assigné ne concordent pas.",
    "PAYOUT_BANQUE_ECART": "Écart entre le payout plateforme et le payout Hostaway du mois.",
    "FACTURE_ABSENTE": "Un net est calculé sans facture propriétaire générée.",
    "BANQUE_NON_CLASSEE": "Des lignes bancaires ne sont pas classées, bloquant la clôture du mois.",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def explication(code: str) -> str:
    return EXPLICATIONS.get(str(code or "").strip(), "")


# ── Identifiant stable ───────────────────────────────────────────────────────

def _stable_id(row: dict[str, Any]) -> str:
    """Clé moteur (ctrl_pk) si présente ; sinon clé d'affichage stable (jamais écrite en source)."""
    pk = to_texte(row.get("ctrl_pk"))
    if pk:
        return pk
    composite = "|".join([to_mois(row.get("mois")), to_texte(row.get("source_module")),
                          to_texte(row.get("code_controle")),
                          to_texte(row.get("logement_id")) or to_texte(row.get("proprietaire_id")),
                          to_texte(row.get("source_pk"))])
    return "GEN-" + hashlib.sha1(composite.encode()).hexdigest()[:12]


def _niveau(row: dict[str, Any]) -> str:
    return to_texte(row.get("severity")).upper()


def _entite(row: dict[str, Any]) -> str:
    for k in ("logement_id", "proprietaire_id", "source_pk"):
        v = to_texte(row.get(k))
        if v:
            return v
    return ""


def _vue(row: dict[str, Any]) -> dict[str, Any]:
    niveau = _niveau(row)
    code = to_texte(row.get("code_controle"))
    return {
        "stable_id": _stable_id(row),
        "code": code,
        "niveau": niveau,
        "mois": to_mois(row.get("mois")),
        "module": to_texte(row.get("source_module")),
        "table": to_texte(row.get("source_table")),
        "entite": _entite(row),
        "message": to_texte(row.get("message")),
        "statut": to_texte(row.get("statut_resolution")) or "OUVERT",
        "commentaire": to_texte(row.get("commentaire")),
        "date_detection": to_date(row.get("date_detection")),
        "explication": explication(code),
        "bloquant": niveau == NIVEAU_BLOQUANT,
    }


# ── Périodes & filtres ───────────────────────────────────────────────────────

def load_periods() -> list[str]:
    mois = {to_mois(r.get("mois")) for r in reader.controles().lignes}
    mois |= {to_mois(r.get("mois")) for r in reader.cloture_ref().lignes}
    return sorted((m for m in mois if m), reverse=True)


def periode_par_defaut() -> str:
    p = load_periods()
    return p[0] if p else ""


def load_filters() -> dict[str, list[dict[str, str]]]:
    lignes = reader.controles().lignes
    modules = sorted({to_texte(r.get("source_module")) for r in lignes if to_texte(r.get("source_module"))})
    niveaux = sorted({_niveau(r) for r in lignes if _niveau(r)}, key=lambda n: NIVEAUX_ORDRE.get(n, 9))
    codes = sorted({to_texte(r.get("code_controle")) for r in lignes if to_texte(r.get("code_controle"))})
    statuts = sorted({to_texte(r.get("statut_resolution")) for r in lignes if to_texte(r.get("statut_resolution"))})
    return {
        "periodes": [{"id": m, "libelle": m} for m in load_periods()],
        "modules": [{"id": m, "libelle": m} for m in modules],
        "niveaux": [{"id": n, "libelle": n} for n in niveaux],
        "codes": [{"id": c, "libelle": c} for c in codes],
        "statuts": [{"id": s, "libelle": s} for s in statuts],
    }


# ── Vues filtrées ────────────────────────────────────────────────────────────

def _toutes_les_vues() -> list[dict[str, Any]]:
    src = reader.controles()
    if not src.etat.disponible:
        return []
    return [_vue(r) for r in src.lignes]


def _match(v: dict, mois: str, module: str, niveau: str, code: str, statut: str,
           bloquants_seul: bool, recherche: str) -> bool:
    if mois and v["mois"] != mois:
        return False
    if module and v["module"] != module:
        return False
    if niveau and v["niveau"] != niveau:
        return False
    if code and v["code"] != code:
        return False
    if statut and v["statut"] != statut:
        return False
    if bloquants_seul and not v["bloquant"]:
        return False
    if recherche:
        r = recherche.lower()
        if r not in v["message"].lower() and r not in v["code"].lower() and r not in v["entite"].lower():
            return False
    return True


def _tri_cle(v: dict, tri: str):
    niv = NIVEAUX_ORDRE.get(v["niveau"], 9)
    if tri == "periode":
        return (v["mois"], niv)
    if tri == "module":
        return (v["module"], niv)
    if tri == "code":
        return (v["code"], niv)
    return (niv, v["mois"], v["module"])


def load_controls(mois: str = "", module: str = "", niveau: str = "", code: str = "", statut: str = "",
                  bloquants_seul: bool = False, non_cloturable: bool = False, recherche: str = "",
                  tri: str = "niveau", page: int = 1) -> dict[str, Any]:
    src = reader.controles()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat, "rows": [],
                "page": 1, "pages": 1, "count_total": 0, "count_filtre": 0, "read_at": _now()}
    vues = _toutes_les_vues()
    if non_cloturable:
        mois_ko = {m for m, s in _cloturabilite().items() if not s["cloturable"]}
        vues = [v for v in vues if v["mois"] in mois_ko]
    filt = [v for v in vues if _match(v, mois, module, niveau, code, statut, bloquants_seul, recherche)]
    filt.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "niveau"))
    pages = max(1, (len(filt) + TAILLE_PAGE - 1) // TAILLE_PAGE)
    page = min(max(1, page), pages)
    debut = (page - 1) * TAILLE_PAGE
    return {"status": "OK", "etat_source": src.etat, "rows": filt[debut:debut + TAILLE_PAGE],
            "page": page, "pages": pages, "count_total": len(vues), "count_filtre": len(filt),
            "read_at": _now()}


# ── Clôture ──────────────────────────────────────────────────────────────────

def _cloturabilite() -> dict[str, dict[str, Any]]:
    """Par mois : statut officiel (REF) + clôturabilité (DASHBOARD_MOIS)."""
    idx: dict[str, dict[str, Any]] = {}
    for r in reader.cloture_ref().lignes:
        m = to_mois(r.get("mois"))
        if not m:
            continue
        idx[m] = {"statut_mois": to_texte(r.get("statut_mois")).upper() or STATUT_OUVERT,
                  "date_cloture": to_date(r.get("date_cloture")),
                  "date_controle": to_date(r.get("date_passage_controle")),
                  "nb_bloquants_ref": to_nombre(r.get("nb_controles_bloquants_ouverts")),
                  "nb_bancaires_non_classees": to_nombre(r.get("nb_lignes_bancaires_non_classees")),
                  "commentaire": to_texte(r.get("commentaire")),
                  "cloturable": None, "cloture_possible_moteur": None}
    for r in reader.dashboard_mois().lignes:
        m = to_mois(r.get("mois"))
        d = idx.setdefault(m, {"statut_mois": STATUT_OUVERT, "date_cloture": "", "date_controle": "",
                               "nb_bloquants_ref": None, "nb_bancaires_non_classees": None,
                               "commentaire": "", "cloturable": None, "cloture_possible_moteur": None})
        cp = to_texte(r.get("cloture_possible")).upper()
        d["cloture_possible_moteur"] = cp
        d["nb_bloquants_ouverts"] = to_nombre(r.get("nb_bloquants_ouverts"))
        d["nb_a_controler_ouverts"] = to_nombre(r.get("nb_a_controler_ouverts"))
        d["cloturable"] = cp in ("OUI", "TRUE", "1", "VRAI")
    # défaut : clôturable inconnu -> False si bloquants, sinon None
    for m, d in idx.items():
        if d["cloturable"] is None:
            nb = d.get("nb_bloquants_ouverts") or d.get("nb_bloquants_ref")
            d["cloturable"] = (nb == 0) if nb is not None else (d["statut_mois"] == STATUT_CLOTURE)
    return idx


def load_month_status(mois: str) -> dict[str, Any]:
    clot = _cloturabilite().get(mois, {"statut_mois": STATUT_OUVERT, "cloturable": None})
    controles_mois = [v for v in _toutes_les_vues() if v["mois"] == mois]
    bloquants = [v for v in controles_mois if v["bloquant"] and v["statut"] != "RESOLU"]
    a_controler = [v for v in controles_mois if v["niveau"] in ("A_CONTROLER", "AVERTISSEMENT") and v["statut"] != "RESOLU"]
    # anomalie après clôture : mois clôturé mais contrôles ouverts subsistants
    anomalies_post_cloture = (clot.get("statut_mois") == STATUT_CLOTURE
                              and any(v["statut"] != "RESOLU" and v["bloquant"] for v in controles_mois))
    etats = reader.etats_sources()
    return {
        "mois": mois,
        "statut_mois": clot.get("statut_mois"),
        "date_cloture": clot.get("date_cloture"),
        "date_controle": clot.get("date_controle"),
        "cloturable": clot.get("cloturable"),
        "cloture_possible_moteur": clot.get("cloture_possible_moteur"),
        "commentaire": clot.get("commentaire"),
        "bloquants": bloquants,
        "a_controler": a_controler,
        "anomalies_post_cloture": anomalies_post_cloture,
        "sources_manquantes": [e for e in etats if e.etat != reader.ETAT_OK],
        "read_at": _now(),
    }


# ── Dashboard ────────────────────────────────────────────────────────────────

def load_freshness() -> dict[str, Any]:
    e = reader.controles().etat
    return {"etat": "A_JOUR" if e.disponible else "INDISPONIBLE", "derniere_generation": e.derniere_maj,
            "libelle": ("Généré le " + e.derniere_maj) if e.derniere_maj else "Source indisponible"}


def load_dashboard(mois: str = "", **filtres: Any) -> dict[str, Any]:
    page = int(filtres.pop("page", 1) or 1)
    liste = load_controls(mois=mois, page=page, **filtres)
    vues = _toutes_les_vues()
    clot = _cloturabilite()
    etats = reader.etats_sources()
    sources_ko = [e for e in etats if e.etat != reader.ETAT_OK]
    def _niv(n):
        return sum(1 for v in vues if v["niveau"] == n)
    summary = {
        "mois": mois,
        "etat_global": ("SOURCE_INCOMPLETE" if sources_ko else
                        ("A_CONTROLER" if any(v["bloquant"] for v in vues) else "CONFORME")),
        "nb_controles": len(vues),
        "nb_bloquants": _niv("BLOQUANT"),
        "nb_a_controler": _niv("A_CONTROLER") + _niv("AVERTISSEMENT"),
        "nb_info": _niv("INFO"),
        "nb_mois_ouverts": sum(1 for d in clot.values() if d["statut_mois"] == STATUT_OUVERT),
        "nb_mois_en_controle": sum(1 for d in clot.values() if d["statut_mois"] == STATUT_EN_CONTROLE),
        "nb_mois_clotures": sum(1 for d in clot.values() if d["statut_mois"] == STATUT_CLOTURE),
        "nb_mois_non_cloturables": sum(1 for d in clot.values() if d["cloturable"] is False),
        "nb_sources_indispo": len(sources_ko),
        "etats_sources": etats,
        "sources_manquantes": sources_ko,
        "read_at": _now(),
    }
    return {"mois": mois, "summary": summary, "liste": liste, "options": load_filters(),
            "cloture": sorted(clot.items(), reverse=True), "freshness": load_freshness(),
            "tris": TRIS, "applied": {"mois": mois, "page": page, **filtres}}


# ── Fiche contrôle ───────────────────────────────────────────────────────────

def load_control_detail(stable_id: str) -> dict[str, Any] | None:
    src = reader.controles()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat,
                "stable_id": stable_id, "read_at": _now()}
    cible = str(stable_id).strip()
    vue = next((v for v in _toutes_les_vues() if v["stable_id"] == cible), None)
    if vue is None:
        return None
    clot = _cloturabilite().get(vue["mois"], {})
    return {
        "status": "OK",
        "stable_id": cible,
        "vue": vue,
        "impact_cloture": {
            "mois": vue["mois"],
            "statut_mois": clot.get("statut_mois"),
            "empeche_cloture": vue["bloquant"] and vue["statut"] != "RESOLU",
        },
        "tracabilite": {"fichier": reader.SOURCE_COHERENCE, "onglet": reader.ONGLET_MASTER,
                        "module": vue["module"], "table": vue["table"]},
        "read_at": _now(),
    }


# ── Bloquants ────────────────────────────────────────────────────────────────

def load_blockers() -> dict[str, Any]:
    src = reader.controles()
    vues = [v for v in _toutes_les_vues() if v["bloquant"] and v["statut"] != "RESOLU"]
    par_module: dict[str, list] = {}
    for v in vues:
        par_module.setdefault(v["module"] or "—", []).append(v)
    return {"status": src.etat.etat if not src.etat.disponible else "OK", "etat_source": src.etat,
            "lignes": sorted(vues, key=lambda v: _tri_cle(v, "niveau")),
            "par_module": {k: par_module[k] for k in sorted(par_module)},
            "etats_sources": reader.etats_sources(), "read_at": _now()}


# ── Export CSV ───────────────────────────────────────────────────────────────

_CSV_COLS = [("mois", "periode"), ("module", "module"), ("code", "code"), ("niveau", "niveau"),
             ("message", "message"), ("entite", "entite"), ("statut", "statut")]


def export_csv(mois: str = "", module: str = "", niveau: str = "", code: str = "", statut: str = "",
               bloquants_seul: bool = False, non_cloturable: bool = False, recherche: str = "",
               tri: str = "niveau") -> str:
    vues = _toutes_les_vues()
    if non_cloturable:
        mois_ko = {m for m, s in _cloturabilite().items() if not s["cloturable"]}
        vues = [v for v in vues if v["mois"] in mois_ko]
    vues = [v for v in vues if _match(v, mois, module, niveau, code, statut, bloquants_seul, recherche)]
    vues.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "niveau"))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([lab for _, lab in _CSV_COLS])
    for v in vues:
        w.writerow(["" if v.get(k) is None else v.get(k) for k, _ in _CSV_COLS])
    return buf.getvalue()

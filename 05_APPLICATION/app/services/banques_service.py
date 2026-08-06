"""Service Banques & Caisse (APP-4A) — orchestration d'affichage, LECTURE SEULE.

Ce service lit les sorties du pipeline banque (lot8a/8b/8c) et les présente. Il ne rejoue aucune
règle, n'invente aucun rapprochement et n'écrit rien.

Règles :
- la banque est une SOURCE DE FLUX, pas une vérité comptable : un mouvement ne devient jamais
  automatiquement une charge ou un revenu ;
- les statuts (`statut_controle`, `statut_rapprochement`) viennent du moteur ;
- le rattachement mouvement ↔ attente de rapprochement se fait par `mouvement_id` (clé du moteur),
  jamais par un calcul de correspondance ajouté ici ;
- les numéros de compte sont masqués ; le libellé brut n'est jamais exposé (seul le libellé nettoyé) ;
- la caisse n'a pas de module moteur : elle est affichée « non alimentée », jamais fabriquée.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any

from app.readers import banques_reader as reader
from app.readers.banques_reader import to_texte, to_nombre, to_date, to_mois, masquer_compte

TAILLE_PAGE = 25
STATUTS_A_CONTROLER = ("A_CONTROLER", "BLOQUANT")
STATUTS_RAPPRO_ATTENTE = ("EN_ATTENTE_EXPORT_AIRBNB", "EN_ATTENTE_SAISIE_ACOMPTE", "EN_ATTENTE")

TRIS = {
    "anomalie": "Anomalies d'abord",
    "date": "Date",
    "montant": "Montant",
    "compte": "Compte",
}

# Explications lisibles (affichage seul — le code moteur reste autoritaire).
EXPLICATIONS: dict[str, str] = {
    "EN_ATTENTE_EXPORT_AIRBNB":
        "Encaissement Airbnb en attente de l'export détaillé Airbnb pour être rapproché.",
    "EN_ATTENTE_SAISIE_ACOMPTE":
        "Virement propriétaire en attente de la saisie de l'acompte correspondant (Lot 5).",
    "AJUSTEMENT":
        "Petit écart traité comme ajustement, sans produit économique.",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def explication(code: str) -> str:
    return EXPLICATIONS.get(str(code or "").strip(), "")


# ── Statut de la source Airbnb détaillée (SOURCE_AIRBNB_DETAILLEE_ABSENTE) ───
# Aucun import spéculatif : ce bloc affiche uniquement un état, jamais un bouton d'import
# fonctionnel tant qu'aucun format réel n'a été validé (cf. 74_CONTRAT_SOURCE_AIRBNB_
# RAPPROCHEMENT.md). Le contrat cible y est documenté (transaction_id, payout_id, reference_
# airbnb/G-code, dates, montant_brut/frais/montant_net, devise, listing_id, reservation_id).

DONNEES_MINIMALES_AIRBNB = (
    "identifiant transaction ou payout", "référence Airbnb (G-code)", "date du versement",
    "montant net", "frais", "devise", "réservation associée (lorsque disponible)",
)


def statut_source_airbnb() -> dict[str, Any]:
    """Compte les propositions Airbnb réellement bloquées par l'absence d'export détaillé —
    jamais un chiffre fabriqué : lu directement sur RAPPROCH_AIRBNB_ATTENTE."""
    src = reader.rappro_airbnb()
    if not src.etat.disponible:
        return {"disponible_export": False, "nb_bloquees": 0, "source_lisible": False,
                "donnees_minimales": DONNEES_MINIMALES_AIRBNB, "date_audit": _now()}
    nb = sum(1 for r in src.lignes
            if to_texte(r.get("statut_rapprochement")) == "EN_ATTENTE_EXPORT_AIRBNB")
    return {"disponible_export": False, "nb_bloquees": nb, "source_lisible": True,
            "donnees_minimales": DONNEES_MINIMALES_AIRBNB, "date_audit": _now()}


# ── Index des attentes de rapprochement (par mouvement_id) ───────────────────

def _index_rappro() -> dict[str, dict[str, Any]]:
    """mouvement_id -> infos d'attente de rapprochement (statut moteur), sans rien inventer."""
    idx: dict[str, dict[str, Any]] = {}
    for r in reader.rappro_airbnb().lignes:
        mid = to_texte(r.get("mouvement_id"))
        if mid:
            idx[mid] = {"canal": "AIRBNB", "statut": to_texte(r.get("statut_rapprochement")),
                        "reference": to_texte(r.get("reference_airbnb")),
                        "methode": to_texte(r.get("methode_rapprochement")),
                        "commentaire": to_texte(r.get("commentaire"))}
    for r in reader.rappro_proprietaires().lignes:
        mid = to_texte(r.get("mouvement_id"))
        if mid:
            idx[mid] = {"canal": "PROPRIETAIRE", "statut": to_texte(r.get("statut_rapprochement")),
                        "reference": to_texte(r.get("proprietaire_id")),
                        "nature": to_texte(r.get("nature_presumee")),
                        "prerequis": to_texte(r.get("prerequis_rapprochement")),
                        "commentaire": to_texte(r.get("commentaire"))}
    return idx


def _index_controles() -> dict[str, list[dict[str, Any]]]:
    idx: dict[str, list[dict[str, Any]]] = {}
    for r in reader.controles().lignes:
        mid = to_texte(r.get("mouvement_id"))
        if mid:
            idx.setdefault(mid, []).append({
                "code": to_texte(r.get("code_controle")),
                "severite": to_texte(r.get("severite")),
                "description": to_texte(r.get("description")),
            })
    return idx


# ── Vue d'un mouvement ───────────────────────────────────────────────────────

def _vue(row: dict[str, Any], rappro: dict[str, dict], ctrl: dict[str, list]) -> dict[str, Any]:
    mid = to_texte(row.get("mouvement_id"))
    att = rappro.get(mid)
    statut_moteur = to_texte(row.get("statut_controle"))
    return {
        "mouvement_id": mid,
        "date_operation": to_date(row.get("date_operation")),
        "date_valeur": to_date(row.get("date_valeur")),
        "mois": to_mois(row.get("date_operation")),
        "compte_masque": masquer_compte(row.get("compte_id")),
        "libelle": to_texte(row.get("libelle")),          # nettoyé — jamais libelle_brut
        "montant": to_nombre(row.get("montant")),
        "sens": to_texte(row.get("sens")).upper(),
        "devise": to_texte(row.get("devise")) or "EUR",
        "tiers": to_texte(row.get("tiers_detecte")),
        "categorie": to_texte(row.get("categorie")),
        "type_flux_id": to_texte(row.get("type_flux_id")),
        "statut_moteur": statut_moteur,
        "niveau_risque": to_texte(row.get("niveau_risque")),
        "codes_anomalie": to_texte(row.get("codes_anomalie")),
        "regle_id": to_texte(row.get("regle_id_appliquee")),
        "row_hash": to_texte(row.get("ROW_HASH")),
        "import_id": to_texte(row.get("import_id")),
        # Rapprochement — repris du moteur (jamais calculé ici).
        "rappro_canal": (att or {}).get("canal", ""),
        "rappro_statut": (att or {}).get("statut", ""),
        "rappro_reference": (att or {}).get("reference", ""),
        "controles": ctrl.get(mid, []),
    }


def a_rapprocher(vue: dict[str, Any]) -> bool:
    """À rapprocher : le moteur signale une attente de rapprochement, ou un contrôle à contrôler."""
    return (vue["statut_moteur"] in STATUTS_A_CONTROLER
            or bool(vue["rappro_statut"])
            or bool(vue["controles"]))


# ── Périodes & filtres ───────────────────────────────────────────────────────

def load_available_periods() -> list[str]:
    mois = {to_mois(r.get("date_operation")) for r in reader.mouvements().lignes}
    return sorted((m for m in mois if m), reverse=True)


def periode_par_defaut() -> str:
    p = load_available_periods()
    return p[0] if p else ""


def load_filter_options(mois: str = "") -> dict[str, list[dict[str, str]]]:
    lignes = [r for r in reader.mouvements().lignes
              if not mois or to_mois(r.get("date_operation")) == mois]
    comptes: dict[str, str] = {}
    for r in lignes:
        cid = to_texte(r.get("compte_id"))
        if cid:
            comptes.setdefault(cid, masquer_compte(cid))
    types = sorted({to_texte(r.get("type_flux_id")) for r in lignes if to_texte(r.get("type_flux_id"))})
    statuts = sorted({to_texte(r.get("statut_controle")) for r in lignes if to_texte(r.get("statut_controle"))})
    return {
        "periodes": [{"id": m, "libelle": m} for m in load_available_periods()],
        # Identifiant OPAQUE (jamais le compte_id brut dans le HTML) ; libellé = compte masqué.
        "comptes": [{"id": reader.id_opaque_compte(k), "libelle": v} for k, v in sorted(comptes.items())],
        "types": [{"id": t, "libelle": t} for t in types],
        "statuts": [{"id": s, "libelle": s} for s in statuts],
    }


_RE_CPT_OPAQUE = re.compile(r"^CPT-[0-9a-f]{8}$")


def _traduire_compte_opaque(compte_id_opaque: str) -> tuple[str, bool]:
    """(compte_id_reel, valide). Vide + valide si aucun filtre demandé.

    Jamais un compte brut accepté : seul un identifiant opaque bien formé (CPT-xxxxxxxx) ET connu
    est résolu. Une valeur non conforme ou inconnue -> refusée explicitement (valide=False),
    jamais silencieusement ignorée ni utilisée telle quelle comme clé de filtrage.
    """
    v = to_texte(compte_id_opaque)
    if not v:
        return "", True
    if not _RE_CPT_OPAQUE.match(v):
        return "", False
    reel = reader.resoudre_opaque_compte(v)
    if reel is None:
        return "", False
    return reel, True


# ── Filtre partagé (liste + export, jamais divergents) ───────────────────────

def _match(v: dict[str, Any], mois: str, compte_id: str, sens: str, statut: str,
           non_rapproche: bool, montant_min: float | None, montant_max: float | None,
           recherche: str, compte_id_brut_map: dict[str, str]) -> bool:
    if mois and v["mois"] != mois:
        return False
    if compte_id and compte_id_brut_map.get(v["mouvement_id"]) != compte_id:
        return False
    if sens and v["sens"] != sens.upper():
        return False
    if statut and v["statut_moteur"] != statut:
        return False
    if non_rapproche and not a_rapprocher(v):
        return False
    m = v["montant"]
    if montant_min is not None and (m is None or abs(m) < montant_min):
        return False
    if montant_max is not None and (m is None or abs(m) > montant_max):
        return False
    if recherche:
        r = recherche.lower()
        if r not in v["libelle"].lower() and r not in v["tiers"].lower():
            return False
    return True


def _tri_cle(v: dict[str, Any], tri: str):
    a_ctrl = 0 if a_rapprocher(v) else 1
    montant = abs(v["montant"] or 0)
    if tri == "date":
        return (v["date_operation"], v["mouvement_id"])
    if tri == "montant":
        return (-montant, v["date_operation"])
    if tri == "compte":
        return (v["compte_masque"], v["date_operation"])
    return (a_ctrl, v["date_operation"])


def _toutes_les_vues(mois: str = "") -> tuple[list[dict[str, Any]], dict[str, str]]:
    src = reader.mouvements()
    if not src.etat.disponible:
        return [], {}
    rappro = _index_rappro()
    ctrl = _index_controles()
    compte_map: dict[str, str] = {}
    vues = []
    for r in src.lignes:
        if mois and to_mois(r.get("date_operation")) != mois:
            continue
        v = _vue(r, rappro, ctrl)
        compte_map[v["mouvement_id"]] = to_texte(r.get("compte_id"))
        vues.append(v)
    return vues, compte_map


def load_movements(mois: str = "", compte_id: str = "", sens: str = "", statut: str = "",
                   non_rapproche: bool = False, montant_min: str = "", montant_max: str = "",
                   recherche: str = "", tri: str = "anomalie", page: int = 1) -> dict[str, Any]:
    src = reader.mouvements()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat, "rows": [],
                "page": 1, "pages": 1, "count_total": 0, "count_filtre": 0, "read_at": _now(),
                "compte_id_invalide": False}
    compte_reel, compte_valide = _traduire_compte_opaque(compte_id)
    vues, compte_map = _toutes_les_vues(mois)
    mn = to_nombre(montant_min)
    mx = to_nombre(montant_max)
    if not compte_valide:
        filtrees: list[dict[str, Any]] = []   # refus propre : jamais un compte brut/inconnu accepté
    else:
        filtrees = [v for v in vues if _match(v, mois, compte_reel, sens, statut, non_rapproche,
                                              mn, mx, recherche, compte_map)]
    filtrees.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "anomalie"))
    pages = max(1, (len(filtrees) + TAILLE_PAGE - 1) // TAILLE_PAGE)
    page = min(max(1, page), pages)
    debut = (page - 1) * TAILLE_PAGE
    return {"status": "OK", "etat_source": src.etat, "rows": filtrees[debut:debut + TAILLE_PAGE],
            "page": page, "pages": pages, "count_total": len(vues), "count_filtre": len(filtrees),
            "read_at": _now(), "compte_id_invalide": not compte_valide}


# ── Synthèse & tableau de bord ───────────────────────────────────────────────

def load_summary(mois: str = "") -> dict[str, Any]:
    src = reader.mouvements()
    vues, _ = _toutes_les_vues(mois)
    nb_a_rapprocher = sum(1 for v in vues if a_rapprocher(v))
    nb_ctrl = sum(1 for v in vues if v["statut_moteur"] in STATUTS_A_CONTROLER)
    debit = sum(v["montant"] for v in vues if v["sens"] == "DEBIT" and v["montant"] is not None)
    credit = sum(v["montant"] for v in vues if v["sens"] == "CREDIT" and v["montant"] is not None)
    etats = reader.etats_sources()
    sources_ko = [e for e in etats if e.etat not in (reader.ETAT_OK, reader.ETAT_NON_ALIMENTE)]
    if not src.etat.disponible:
        etat_global = "SOURCE_INCOMPLETE"
    elif nb_a_rapprocher:
        etat_global = "A_RAPPROCHER"
    else:
        etat_global = "CONFORME"
    return {
        "mois": mois,
        "etat_global": etat_global,
        "nb_mouvements_bancaires": len(vues),
        "nb_mouvements_caisse": None,                # caisse non alimentée
        "caisse_etat": reader.caisse().etat,
        "nb_a_rapprocher": nb_a_rapprocher,
        "nb_rapproches": 0,                          # aucun rapprochement validé produit par le moteur
        "nb_a_controler": nb_ctrl,
        "total_debit": round(debit, 2),
        "total_credit": round(credit, 2),
        "etats_sources": etats,
        "sources_manquantes": sources_ko,
        "read_at": _now(),
    }


def load_freshness() -> dict[str, Any]:
    """Fraîcheur : date de dernière génération du fichier banque (indicateur, pas une preuve)."""
    e = reader.mouvements().etat
    return {"etat": "A_JOUR" if e.disponible else "INDISPONIBLE",
            "derniere_generation": e.derniere_maj,
            "libelle": ("Généré le " + e.derniere_maj) if e.derniere_maj else "Source indisponible"}


def load_dashboard(mois: str = "", **filtres: Any) -> dict[str, Any]:
    if not mois:
        mois = periode_par_defaut()
    page = int(filtres.pop("page", 1) or 1)
    liste = load_movements(mois=mois, page=page, **filtres)
    applied = {"mois": mois, "page": page, **filtres}
    if liste.get("compte_id_invalide"):
        # Un compte_id brut/inconnu n'est JAMAIS réinjecté dans les liens générés (pagination,
        # export CSV, filtre re-sélectionné) : on ne reflète pas la valeur invalide soumise.
        applied["compte_id"] = ""
    return {
        "mois": mois,
        "summary": load_summary(mois),
        "liste": liste,
        "options": load_filter_options(mois),
        "freshness": load_freshness(),
        "tris": TRIS,
        "applied": applied,
    }


# ── Écran « À rapprocher » ───────────────────────────────────────────────────

def load_unmatched(mois: str = "") -> dict[str, Any]:
    src = reader.mouvements()
    vues, _ = _toutes_les_vues(mois)
    lignes = sorted((v for v in vues if a_rapprocher(v)), key=lambda v: _tri_cle(v, "anomalie"))
    ctrl8c = [
        {"code": to_texte(r.get("code_controle")), "severite": to_texte(r.get("severite")),
         "nb_lignes": to_nombre(r.get("nb_lignes")), "total": to_nombre(r.get("total_montant_eur")),
         "description": to_texte(r.get("description")), "action": to_texte(r.get("action_requise"))}
        for r in reader.controles_rappro_8c().lignes
    ]
    return {
        "mois": mois,
        "status": src.etat.etat if not src.etat.disponible else "OK",
        "etat_source": src.etat,
        "lignes": lignes,
        "controles_8c": ctrl8c,
        "etats_sources": reader.etats_sources(),
        "read_at": _now(),
    }


# ── Fiche détail ─────────────────────────────────────────────────────────────

def load_detail(stable_id: str) -> dict[str, Any] | None:
    src = reader.mouvements()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat,
                "stable_id": stable_id, "read_at": _now()}
    cible = str(stable_id).strip()
    brute = next((r for r in src.lignes if to_texte(r.get("mouvement_id")) == cible), None)
    if brute is None:
        return None
    vue = _vue(brute, _index_rappro(), _index_controles())
    anomalies = []
    if vue["codes_anomalie"]:
        anomalies.append({"code": vue["codes_anomalie"], "niveau": vue["statut_moteur"],
                          "explication": explication(vue["codes_anomalie"])})
    if vue["rappro_statut"]:
        anomalies.append({"code": vue["rappro_statut"], "niveau": "A_RAPPROCHER",
                          "explication": explication(vue["rappro_statut"])})
    return {
        "status": "OK",
        "stable_id": cible,
        "vue": vue,
        "anomalies": anomalies,
        "controles": vue["controles"],
        "tracabilite": {
            "source": reader.SOURCE_BANQUE,
            "import_id": vue["import_id"],
            "row_hash": vue["row_hash"],
            "regle_id": vue["regle_id"],
            "onglet": reader.ONGLET_MOUVEMENTS,
        },
        "read_at": _now(),
    }


# ── Export CSV (mémoire, masqué) ─────────────────────────────────────────────

_CSV_COLS = [
    ("date_operation", "date"), ("compte_masque", "compte"), ("libelle", "libelle"),
    ("montant", "montant"), ("sens", "sens"), ("type_flux_id", "type_flux"),
    ("rappro_statut", "statut_rapprochement"), ("statut_moteur", "statut_controle"),
    ("codes_anomalie", "code_anomalie"),
]


def export_movements_csv(mois: str = "", compte_id: str = "", sens: str = "", statut: str = "",
                         non_rapproche: bool = False, montant_min: str = "", montant_max: str = "",
                         recherche: str = "", tri: str = "anomalie") -> str:
    if not mois:
        mois = periode_par_defaut()
    compte_reel, compte_valide = _traduire_compte_opaque(compte_id)
    vues, compte_map = _toutes_les_vues(mois)
    mn, mx = to_nombre(montant_min), to_nombre(montant_max)
    if not compte_valide:
        vues = []   # refus propre : jamais un compte brut/inconnu accepté
    else:
        vues = [v for v in vues if _match(v, mois, compte_reel, sens, statut, non_rapproche,
                                          mn, mx, recherche, compte_map)]
    vues.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "anomalie"))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([lab for _, lab in _CSV_COLS])
    for v in vues:
        w.writerow(["" if v.get(k) is None else v.get(k) for k, _ in _CSV_COLS])
    return buf.getvalue()

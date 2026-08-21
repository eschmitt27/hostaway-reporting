"""Service Propriétaires & règlements (APP-3C) — orchestration d'affichage, LECTURE SEULE.

Consolide, au grain mois × propriétaire, les sorties moteur : net (VUE_MOIS), règlements (REGLEMENT),
commissions (COMMISSIONS), facturation (DASHBOARD_FACTURATION / FACT_FACTURE_ENTETE).

Règles :
- ne JAMAIS recalculer une commission ni un net : `commission = total_commission_mois`,
  `net = net_proprietaire_apres_charge_mois`, repris tels quels du moteur ;
- le taux appliqué (historique) vient de COMMISSIONS.taux_commission, jamais recomposé ici ;
- CA retenu, commission, net, préfacture, facture, règlement, reste : champs DISTINCTS, jamais confondus ;
- statut de règlement / facture repris du moteur (aucun statut de paiement inventé) ;
- adresse propriétaire et données bancaires jamais exposées ; aucun chemin absolu.
- les agrégats ne font que SOMMER des colonnes déjà calculées par le moteur.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any

from app.readers import proprietaires_reglements_reader as reader
from app.readers.proprietaires_reglements_reader import to_texte, to_nombre, to_mois

TAILLE_PAGE = 25

TRIS = {"anomalie": "Anomalies d'abord", "proprietaire": "Propriétaire", "reste": "Reste à régler",
        "net": "Net propriétaire"}

EXPLICATIONS: dict[str, str] = {
    "PROPRIETAIRE_SANS_TAUX": "Aucun taux de commission applicable trouvé pour ce propriétaire/logement.",
    "LOGEMENT_SANS_PROPRIETAIRE": "Un logement n'est rattaché à aucun propriétaire.",
    "FACTURE_ABSENTE": "Le net est calculé mais aucune facture propriétaire n'a été générée.",
    "RESTE_A_PAYER": "Un reste à payer subsiste après acomptes et paiements connus.",
    "REGLEMENT_ABSENT": "Aucun règlement connu pour un montant dû.",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def explication(code: str) -> str:
    return EXPLICATIONS.get(str(code or "").strip(), "")


# ── Index auxiliaires ────────────────────────────────────────────────────────

def _index_dashboard(mois: str, db_path=None) -> dict[str, dict[str, Any]]:
    idx = {}
    for r in reader.dashboard_facturation(db_path=db_path).lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        idx[to_texte(r.get("proprietaire_id"))] = r
    return idx


def _agg_reglement(mois: str, db_path=None) -> dict[str, dict[str, Any]]:
    """Agrège REGLEMENT (mois × logement) au grain propriétaire : acomptes, paiements, reste, logements."""
    agg: dict[str, dict[str, Any]] = {}
    for r in reader.net_reglement(db_path=db_path).lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        pid = to_texte(r.get("proprietaire_id"))
        a = agg.setdefault(pid, {"acomptes": 0.0, "paiement": 0.0, "reste": 0.0,
                                 "logements": set(), "statuts": set()})
        a["acomptes"] += (to_nombre(r.get("acompte_conciergerie_recu_via_airbnb")) or 0) \
            + (to_nombre(r.get("autres_acomptes_recus")) or 0)
        a["paiement"] += to_nombre(r.get("paiement_deja_recu")) or 0
        a["reste"] += to_nombre(r.get("reste_a_payer_conciergerie")) or 0
        if to_texte(r.get("logement_id")):
            a["logements"].add(to_texte(r.get("logement_id")))
        if to_texte(r.get("statut_reglement")):
            a["statuts"].add(to_texte(r.get("statut_reglement")))
    return agg


def _agg_commission_assiette(mois: str, db_path=None) -> dict[str, float]:
    agg: dict[str, float] = {}
    for r in reader.commissions(db_path=db_path).lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        pid = to_texte(r.get("proprietaire_id"))
        agg[pid] = agg.get(pid, 0.0) + (to_nombre(r.get("assiette_commission")) or 0)
    return agg


def _index_noms(db_path=None) -> dict[str, str]:
    idx = {}
    for r in reader.factures_entetes(db_path=db_path).lignes:
        pid = to_texte(r.get("proprietaire_id"))
        if pid and pid not in idx:
            idx[pid] = to_texte(r.get("nom_proprietaire"))  # adresse jamais reprise
    return idx


def _index_factures(mois: str, db_path=None) -> dict[str, dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    for r in reader.factures_entetes(db_path=db_path).lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        pid = to_texte(r.get("proprietaire_id"))
        a = idx.setdefault(pid, {"nb": 0, "statuts": set(), "reste": 0.0})
        a["nb"] += 1
        if to_texte(r.get("statut_facture")):
            a["statuts"].add(to_texte(r.get("statut_facture")))
        a["reste"] += to_nombre(r.get("reste_a_payer")) or 0
    return idx


# ── Vue consolidée ───────────────────────────────────────────────────────────

def _vue(row: dict[str, Any], dash: dict, regl: dict, fact: dict, assiette: dict,
         noms: dict) -> dict[str, Any]:
    pid = to_texte(row.get("proprietaire_id"))
    d = dash.get(pid, {})
    rg = regl.get(pid, {})
    fa = fact.get(pid, {})
    nb_bloq = to_nombre(d.get("nb_bloquants_mois")) or 0
    nb_ctrl = to_nombre(d.get("nb_a_controler_mois")) or 0
    reste = to_nombre(row.get("reste_a_payer_conciergerie"))
    return {
        "mois": to_mois(row.get("mois")),
        "proprietaire_id": pid,
        "nom": noms.get(pid, pid),
        "nb_logements": int(to_nombre(d.get("nb_logements")) or len(rg.get("logements", set()))),
        "ca_retenu": to_nombre(row.get("total_payout_mois")),
        "menage": to_nombre(row.get("total_menage_mois")),
        "base_commission": round(assiette.get(pid, 0.0), 2) if pid in assiette else None,
        "commission": to_nombre(row.get("total_commission_mois")),          # moteur, jamais recalculée
        "autres_impacts": to_nombre(row.get("charge_fixe_mensuelle")),
        "net": to_nombre(row.get("net_proprietaire_apres_charge_mois")),    # moteur
        "net_avant_charge": to_nombre(row.get("net_proprietaire_avant_charge_mois")),
        "montant_du_conciergerie": to_nombre(row.get("montant_du_conciergerie")),
        # facturation / règlement — champs distincts
        "facture_nb": fa.get("nb", 0),
        "facture_statut": ", ".join(sorted(fa.get("statuts", set()))) or (to_texte(d.get("statut_facture")) or "—"),
        "reglement_acomptes": round(rg.get("acomptes", 0.0), 2),
        "reglement_paiement": round(rg.get("paiement", 0.0), 2),
        "reglement_statut": ", ".join(sorted(rg.get("statuts", set()))) or "—",
        "reste": reste,
        "nb_reservations": to_nombre(row.get("nb_reservations")),
        "nb_bloquants": int(nb_bloq),
        "nb_a_controler": int(nb_ctrl),
        "balises": to_texte(d.get("balises_non_resolues")),
        "mode_facturation": to_texte(d.get("mode_facturation")),
    }


def a_controler(v: dict[str, Any]) -> bool:
    return (v["nb_bloquants"] > 0 or v["nb_a_controler"] > 0
            or (v["reste"] is not None and v["reste"] > 0.005)
            or bool(v["balises"]))


# ── Périodes & filtres ───────────────────────────────────────────────────────

def _mois_courant() -> str:
    return datetime.now().strftime("%Y-%m")


def load_periods() -> list[str]:
    """Tous les mois présents dans le MASTER, du plus récent au plus ancien (usage interne)."""
    mois = {to_mois(r.get("mois")) for r in reader.net_vue_mois().lignes}
    return sorted((m for m in mois if m), reverse=True)


def load_periods_split() -> dict[str, Any]:
    """Sépare les mois DISPONIBLES (≤ mois courant) des mois FUTURS (> courant).

    Les mois futurs existent dans le MASTER moteur (calendrier prévisionnel étendu) : on ne les
    masque pas silencieusement — ils sont isolés dans « Données futures à contrôler ».
    """
    courant = _mois_courant()
    tous = load_periods()
    return {
        "normales": [m for m in tous if m <= courant],
        "futures": [m for m in tous if m > courant],
        "mois_courant": courant,
    }


def periode_par_defaut() -> str:
    """Mois par défaut = le plus récent NON futur (jamais un mois futur par défaut)."""
    split = load_periods_split()
    if split["normales"]:
        return split["normales"][0]
    tous = load_periods()
    return tous[0] if tous else ""


def load_filters(mois: str = "") -> dict[str, Any]:
    noms = _index_noms()
    props: dict[str, str] = {}
    for r in reader.net_vue_mois().lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        pid = to_texte(r.get("proprietaire_id"))
        if pid:
            props.setdefault(pid, noms.get(pid, pid))
    logements = sorted({to_texte(r.get("logement_id")) for r in reader.net_reglement().lignes
                        if to_texte(r.get("logement_id")) and (not mois or to_mois(r.get("mois")) == mois)})
    split = load_periods_split()
    return {
        # Filtre normal : uniquement les mois ≤ courant (aucun mois futur).
        "periodes": [{"id": m, "libelle": m} for m in split["normales"]],
        # Mois futurs isolés, présentés à part (« Données futures à contrôler »).
        "periodes_futures": [{"id": m, "libelle": m} for m in split["futures"]],
        "mois_courant": split["mois_courant"],
        "proprietaires": [{"id": k, "libelle": v} for k, v in sorted(props.items())],
        # Libellé = nom officiel du logement ; repli explicite si inconnu.
        "logements": [{"id": lg, "libelle": reader.libelle_logement(lg)} for lg in logements],
    }


# ── Vues (partagé liste + export) ────────────────────────────────────────────

def _toutes_les_vues(mois: str, db_path=None) -> list[dict[str, Any]]:
    src = reader.net_vue_mois(db_path=db_path)
    if not src.etat.disponible:
        return []
    dash = _index_dashboard(mois, db_path=db_path)
    regl = _agg_reglement(mois, db_path=db_path)
    fact = _index_factures(mois, db_path=db_path)
    assiette = _agg_commission_assiette(mois, db_path=db_path)
    noms = _index_noms(db_path=db_path)
    return [_vue(r, dash, regl, fact, assiette, noms) for r in src.lignes
            if not mois or to_mois(r.get("mois")) == mois]


def _match(v: dict, proprietaire_id: str, logement_id: str, statut: str, avec_anomalie: bool,
           avec_reste: bool, facture: str, regle: str, logement_index: dict[str, set]) -> bool:
    if proprietaire_id and v["proprietaire_id"] != proprietaire_id:
        return False
    if logement_id and v["proprietaire_id"] not in logement_index.get(logement_id, set()):
        return False
    if statut and statut not in v["reglement_statut"] and statut not in v["facture_statut"]:
        return False
    if avec_anomalie and not a_controler(v):
        return False
    if avec_reste and not (v["reste"] is not None and v["reste"] > 0.005):
        return False
    if facture == "oui" and v["facture_nb"] == 0:
        return False
    if facture == "non" and v["facture_nb"] > 0:
        return False
    if regle == "oui" and (v["reste"] is None or v["reste"] > 0.005):
        return False
    if regle == "non" and (v["reste"] is not None and v["reste"] <= 0.005):
        return False
    return True


def _tri_cle(v: dict, tri: str):
    ctrl = 0 if a_controler(v) else 1
    if tri == "proprietaire":
        return (v["nom"] or v["proprietaire_id"],)
    if tri == "reste":
        return (-(v["reste"] or 0),)
    if tri == "net":
        return (-(v["net"] or 0),)
    return (ctrl, -(v["reste"] or 0), v["nom"] or v["proprietaire_id"])


def _logement_index(mois: str, db_path=None) -> dict[str, set]:
    idx: dict[str, set] = {}
    for r in reader.net_reglement(db_path=db_path).lignes:
        if mois and to_mois(r.get("mois")) != mois:
            continue
        lg = to_texte(r.get("logement_id"))
        if lg:
            idx.setdefault(lg, set()).add(to_texte(r.get("proprietaire_id")))
    return idx


def load_owners(mois: str = "", proprietaire_id: str = "", logement_id: str = "", statut: str = "",
                avec_anomalie: bool = False, avec_reste: bool = False, facture: str = "",
                regle: str = "", tri: str = "anomalie", page: int = 1, db_path=None) -> dict[str, Any]:
    src = reader.net_vue_mois(db_path=db_path)
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat, "rows": [],
                "page": 1, "pages": 1, "count_total": 0, "count_filtre": 0, "read_at": _now()}
    vues = _toutes_les_vues(mois, db_path=db_path)
    lg_idx = _logement_index(mois, db_path=db_path)
    filt = [v for v in vues if _match(v, proprietaire_id, logement_id, statut, avec_anomalie,
                                      avec_reste, facture, regle, lg_idx)]
    filt.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "anomalie"))
    pages = max(1, (len(filt) + TAILLE_PAGE - 1) // TAILLE_PAGE)
    page = min(max(1, page), pages)
    debut = (page - 1) * TAILLE_PAGE
    return {"status": "OK", "etat_source": src.etat, "rows": filt[debut:debut + TAILLE_PAGE],
            "page": page, "pages": pages, "count_total": len(vues), "count_filtre": len(filt),
            "read_at": _now()}


# ── Synthèse / dashboard ─────────────────────────────────────────────────────

def load_freshness() -> dict[str, Any]:
    e = reader.net_vue_mois().etat
    return {"etat": "A_JOUR" if e.disponible else "INDISPONIBLE",
            "derniere_generation": e.derniere_maj,
            "libelle": ("Généré le " + e.derniere_maj) if e.derniere_maj else "Source indisponible"}


def load_dashboard(mois: str = "", **filtres: Any) -> dict[str, Any]:
    if not mois:
        mois = periode_par_defaut()
    page = int(filtres.pop("page", 1) or 1)
    liste = load_owners(mois=mois, page=page, **filtres)
    vues = _toutes_les_vues(mois)
    def _somme(cle):
        vals = [v[cle] for v in vues if v[cle] is not None]
        return round(sum(vals), 2) if vals else None
    etats = reader.etats_sources()
    sources_ko = [e for e in etats if e.etat != reader.ETAT_OK]
    summary = {
        "mois": mois,
        "etat_global": ("SOURCE_INCOMPLETE" if sources_ko else
                        ("A_CONTROLER" if any(a_controler(v) for v in vues) else "CONFORME")),
        "nb_proprietaires": len({v["proprietaire_id"] for v in vues}),
        "nb_logements": sum(v["nb_logements"] for v in vues),
        "commission_totale": _somme("commission"),
        "net_total": _somme("net"),
        "reste_total": _somme("reste"),
        "nb_factures": sum(v["facture_nb"] for v in vues),
        "reglement_total": _somme("reglement_paiement"),
        "nb_a_controler": sum(1 for v in vues if a_controler(v)),
        "etats_sources": etats,
        "sources_manquantes": sources_ko,
        "read_at": _now(),
    }
    return {"mois": mois, "summary": summary, "liste": liste, "options": load_filters(mois),
            "freshness": load_freshness(), "tris": TRIS,
            "applied": {"mois": mois, "page": page, **filtres}}


# ── Fiche propriétaire ───────────────────────────────────────────────────────

def load_owner_detail(proprietaire_id: str, mois: str = "") -> dict[str, Any] | None:
    if not mois:
        mois = periode_par_defaut()
    src = reader.net_vue_mois()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat_source": src.etat,
                "proprietaire_id": proprietaire_id, "read_at": _now()}
    pid = str(proprietaire_id).strip()
    vues = [v for v in _toutes_les_vues(mois) if v["proprietaire_id"] == pid]
    if not vues:
        # peut exister sur un autre mois : on vérifie la présence globale
        present = any(to_texte(r.get("proprietaire_id")) == pid for r in src.lignes)
        if not present:
            return None
        vue = None
    else:
        vue = vues[0]
    # logements du propriétaire (REGLEMENT), taux historiques (COMMISSIONS), factures, contrôles
    logements = [{
        "logement_id": to_texte(r.get("logement_id")),
        "total_payout": to_nombre(r.get("total_payout_mois")),
        "total_commission": to_nombre(r.get("total_commission_mois")),
        "charge_fixe": to_nombre(r.get("charge_fixe_mensuelle")),
        "net_avant_charge": to_nombre(r.get("net_proprietaire_avant_charge_mois")),
        "acomptes": (to_nombre(r.get("acompte_conciergerie_recu_via_airbnb")) or 0) + (to_nombre(r.get("autres_acomptes_recus")) or 0),
        "paiement": to_nombre(r.get("paiement_deja_recu")),
        "reste": to_nombre(r.get("reste_a_payer_conciergerie")),
        "statut_reglement": to_texte(r.get("statut_reglement")),
    } for r in reader.net_reglement().lignes
        if to_texte(r.get("proprietaire_id")) == pid and (not mois or to_mois(r.get("mois")) == mois)]
    taux = sorted({(to_texte(r.get("logement_id")), to_nombre(r.get("taux_commission")))
                   for r in reader.commissions().lignes
                   if to_texte(r.get("proprietaire_id")) == pid and (not mois or to_mois(r.get("mois")) == mois)
                   and r.get("taux_commission") is not None})
    factures = [{
        "facture_id": to_texte(r.get("facture_id")),
        "logement_id": to_texte(r.get("logement_id")),
        "statut_facture": to_texte(r.get("statut_facture")),
        "statut_generation": to_texte(r.get("statut_generation")),
        "total_exploitation_net": to_nombre(r.get("total_exploitation_net")),
        "total_reglement_du": to_nombre(r.get("total_reglement_du")),
        "reste_a_payer": to_nombre(r.get("reste_a_payer")),
        "mode_facturation": to_texte(r.get("mode_facturation")),
    } for r in reader.factures_entetes().lignes
        if to_texte(r.get("proprietaire_id")) == pid and (not mois or to_mois(r.get("mois")) == mois)]
    anomalies = [{
        "code": to_texte(r.get("code_controle") or r.get("code")),
        "severite": to_texte(r.get("severite") or r.get("niveau")),
        "description": to_texte(r.get("description") or r.get("message")),
    } for r in reader.controles_factures().lignes
        if to_texte(r.get("proprietaire_id")) == pid and (not mois or to_mois(r.get("mois")) == mois)]
    return {
        "status": "OK",
        "proprietaire_id": pid,
        "mois": mois,
        "vue": vue,
        "logements": logements,
        "taux_historiques": [{"logement_id": t[0], "taux": t[1]} for t in taux],
        "factures": factures,
        "anomalies": anomalies,
        "tracabilite": {"sources": [reader.SOURCE_NET, reader.SOURCE_COMMISSIONS, reader.SOURCE_FACT],
                        "note": "Commissions et net repris du moteur (Lot10/Lot12), jamais recalculés."},
        "read_at": _now(),
    }


# ── Écran à contrôler ────────────────────────────────────────────────────────

def load_to_control(mois: str = "") -> dict[str, Any]:
    if not mois:
        mois = periode_par_defaut()
    src = reader.net_vue_mois()
    vues = [v for v in _toutes_les_vues(mois) if a_controler(v)]
    vues.sort(key=lambda v: _tri_cle(v, "anomalie"))
    controles = [{
        "proprietaire_id": to_texte(r.get("proprietaire_id")),
        "code": to_texte(r.get("code_controle") or r.get("code")),
        "severite": to_texte(r.get("severite") or r.get("niveau")),
        "description": to_texte(r.get("description") or r.get("message")),
    } for r in reader.controles_factures().lignes
        if not mois or to_mois(r.get("mois")) == mois]
    return {"mois": mois, "status": src.etat.etat if not src.etat.disponible else "OK",
            "etat_source": src.etat, "lignes": vues, "controles": controles,
            "etats_sources": reader.etats_sources(), "read_at": _now()}


# ── Export CSV ───────────────────────────────────────────────────────────────

_CSV_COLS = [
    ("mois", "periode"), ("proprietaire_id", "proprietaire"), ("nom", "nom"),
    ("nb_logements", "logements"), ("ca_retenu", "ca_retenu"), ("menage", "menage"),
    ("base_commission", "base_commission"), ("commission", "commission"), ("net", "net"),
    ("facture_statut", "facture"), ("reglement_paiement", "reglement"), ("reste", "reste"),
    ("reglement_statut", "statut_reglement"),
]


def export_csv(mois: str = "", proprietaire_id: str = "", logement_id: str = "", statut: str = "",
               avec_anomalie: bool = False, avec_reste: bool = False, facture: str = "",
               regle: str = "", tri: str = "anomalie") -> str:
    if not mois:
        mois = periode_par_defaut()
    vues = _toutes_les_vues(mois)
    lg_idx = _logement_index(mois)
    vues = [v for v in vues if _match(v, proprietaire_id, logement_id, statut, avec_anomalie,
                                      avec_reste, facture, regle, lg_idx)]
    vues.sort(key=lambda v: _tri_cle(v, tri if tri in TRIS else "anomalie"))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([lab for _, lab in _CSV_COLS])
    for v in vues:
        w.writerow(["" if v.get(k) is None else v.get(k) for k, _ in _CSV_COLS])
    return buf.getvalue()

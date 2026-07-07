"""Moteur déterministe des impacts d'une charge (Nouvelle charge guidée).

Une charge reste TOUJOURS une seule charge économique. Les impacts analytiques
(multi-logements, multi-propriétaires, ménage) sont des ventilations : la somme des
quotes-parts est toujours égale au montant réel de la charge. Aucune seconde charge
réelle ni comptable n'est jamais créée.

Ce module ne fait AUCUNE écriture réelle : il produit des structures (périmètre,
quotes-parts, réserve de facturation, effet-de-saisie) exploitables en prévisualisation.
Aucune liste d'identifiants concaténée n'est jamais stockée dans une cellule métier :
les ventilations sont des listes structurées portées par le manifest / la réserve.
"""
from __future__ import annotations

from typing import Any

# ── Catalogue métier des catégories visibles dans « Nouvelle charge » ─────────
# menage : FORCE  (impact ménage forcé), CHOIX (Oui/Non proposé), INTERDIT.
# avantage : True si le choix « avantage associé ? » est proposé.
# groupe : regroupement d'affichage.
MENAGE_FORCE = "FORCE"
MENAGE_CHOIX = "CHOIX"
MENAGE_INTERDIT = "INTERDIT"

CATEGORY_CATALOG: dict[str, dict[str, Any]] = {
    # Logiciels
    "CHG_005": {"label": "Logiciel Hostaway", "groupe": "Logiciels", "menage": MENAGE_INTERDIT, "avantage": False},
    "CHG_006": {"label": "Dynamic pricing", "groupe": "Logiciels", "menage": MENAGE_INTERDIT, "avantage": False},
    "CHG_007": {"label": "Autres logiciels", "groupe": "Logiciels", "menage": MENAGE_INTERDIT, "avantage": False},
    # Charges courantes et opérationnelles
    "CHG_010": {"label": "Frais bancaires", "groupe": "Charges courantes", "menage": MENAGE_INTERDIT, "avantage": False},
    "CHG_011": {"label": "Assurance", "groupe": "Charges courantes", "menage": MENAGE_INTERDIT, "avantage": False},
    "CHG_009": {"label": "Déplacement professionnel", "groupe": "Charges courantes", "menage": MENAGE_CHOIX, "avantage": True},
    "CHG_025": {"label": "Repas", "groupe": "Charges courantes", "menage": MENAGE_CHOIX, "avantage": True},
    "CHG_018": {"label": "Achat divers", "groupe": "Charges courantes", "menage": MENAGE_CHOIX, "avantage": True},
    "CHG_026": {"label": "Prestation diverse", "groupe": "Charges courantes", "menage": MENAGE_CHOIX, "avantage": True},
    "CHG_008": {"label": "Maintenance / réparation logement", "groupe": "Charges courantes", "menage": MENAGE_INTERDIT, "avantage": False},
    "CHG_017": {"label": "Charge générale / non affectée", "groupe": "Charges courantes", "menage": MENAGE_INTERDIT, "avantage": False},
    # Ménages (ouvrent le parcours ménage, impact ménage forcé)
    "CHG_004": {"label": "Achat ménage", "groupe": "Ménages", "menage": MENAGE_FORCE, "avantage": False},
    "CHG_003": {"label": "Blanchisserie / linge", "groupe": "Ménages", "menage": MENAGE_FORCE, "avantage": False},
    "CHG_027": {"label": "Supplément ménage", "groupe": "Ménages", "menage": MENAGE_FORCE, "avantage": False},
    # Catégorie personnalisée (GLOBAL forcé, jamais ménage)
    "CHG_024": {"label": "Autre (catégorie personnalisée)", "groupe": "Autre", "menage": MENAGE_INTERDIT, "avantage": False},
}

# Catégories exclues explicitement de Nouvelle charge (parcours dédiés / non-dépense).
# CHG_016 = Forfait client : ligne de facturation propriétaire, PAS une charge réelle.
# CHG_023 = Forfait cave : charge récurrente (REF_Charges_Recurrentes), pas de saisie manuelle.
CATEGORIES_HORS_FORMULAIRE_EXPLICITE = {"CHG_016", "CHG_023"}

MENAGE_MODE_INTERVENANT = "INTERVENANT"
MENAGE_MODE_LOGEMENT = "LOGEMENT"


def catalog_entry(categorie_id: str) -> dict[str, Any] | None:
    return CATEGORY_CATALOG.get(str(categorie_id or "").strip())


def menage_comportement(categorie_id: str) -> str:
    e = catalog_entry(categorie_id)
    return e["menage"] if e else MENAGE_INTERDIT


def avantage_possible(categorie_id: str) -> bool:
    e = catalog_entry(categorie_id)
    return bool(e and e["avantage"])


# ── Périmètre analytique (charge non ménage) ─────────────────────────────────

def _mois_prefixe(valeur: Any) -> str:
    return str(valeur or "").strip()[:7]


def gestion_active_pour_mois(row: dict[str, Any], mois: str) -> bool:
    """Une ligne de gestion couvre `mois` (YYYY-MM) si statut ACTIF et dates englobantes."""
    if str(row.get("statut_gestion", "")).strip().upper() != "ACTIF":
        return False
    debut = _mois_prefixe(row.get("date_debut"))
    fin = _mois_prefixe(row.get("date_fin"))
    if debut and debut > mois:
        return False
    if fin and fin < mois:
        return False
    return True


def logements_actifs_proprietaire(
    proprietaire_id: str, mois: str, gestion_rows: list[dict[str, Any]]
) -> list[str]:
    """Logements gérés ACTIFS d'un propriétaire au mois donné (triés, dédupliqués)."""
    prop = str(proprietaire_id or "").strip()
    out: set[str] = set()
    for r in gestion_rows:
        if str(r.get("proprietaire_id", "")).strip() != prop:
            continue
        if gestion_active_pour_mois(r, mois):
            lid = str(r.get("logement_id", "")).strip()
            if lid:
                out.add(lid)
    return sorted(out)


def compute_perimetre_logements(
    logements_directs: list[str],
    proprietaires: list[str],
    mois: str,
    gestion_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Périmètre final = logements directs + logements actifs des propriétaires − doublons.

    Retourne une trace lisible et déterministe.
    """
    directs = [str(l).strip() for l in (logements_directs or []) if str(l).strip()]
    props = [str(p).strip() for p in (proprietaires or []) if str(p).strip()]

    via_prop: dict[str, list[str]] = {}
    for p in props:
        via_prop[p] = logements_actifs_proprietaire(p, mois, gestion_rows)

    finaux: set[str] = set(directs)
    for logs in via_prop.values():
        finaux.update(logs)
    finaux_tries = sorted(finaux)

    # Propriétaire par logement final (gestion active du mois) — trace sans concaténation.
    prop_par_log: dict[str, str] = {}
    for r in gestion_rows:
        if gestion_active_pour_mois(r, mois):
            lid = str(r.get("logement_id", "")).strip()
            if lid in finaux and lid not in prop_par_log:
                prop_par_log[lid] = str(r.get("proprietaire_id", "")).strip()

    return {
        "logements_directs": sorted(set(directs)),
        "proprietaires": sorted(set(props)),
        "logements_via_proprietaires": via_prop,
        "logements_finaux": finaux_tries,
        "nb_logements_finaux": len(finaux_tries),
        "proprietaire_par_logement": prop_par_log,
        "global_conciergerie": len(finaux_tries) == 0 and len(props) == 0,
    }


# ── Répartition égale déterministe (centimes) ────────────────────────────────

def repartir_egal(montant: float, logements: list[str]) -> list[dict[str, Any]]:
    """Répartit `montant` (€) également entre logements, centimes déterministes.

    La somme des quotes-parts est TOUJOURS égale au montant. Les premiers logements
    (ordre trié) reçoivent le centime résiduel. Jamais le montant entier répliqué.
    """
    logs = sorted({str(l).strip() for l in (logements or []) if str(l).strip()})
    if not logs:
        return []
    total_cents = int(round(float(montant) * 100))
    n = len(logs)
    base = total_cents // n
    reste = total_cents - base * n  # 0..n-1
    out = []
    for i, lid in enumerate(logs):
        cents = base + (1 if i < reste else 0)
        out.append({"logement_id": lid, "quote_part": round(cents / 100.0, 2)})
    return out


def somme_quotes_parts(quotes: list[dict[str, Any]]) -> float:
    return round(sum(float(q.get("quote_part", 0.0)) for q in quotes), 2)


# ── Périmètre ménage (analytique) ────────────────────────────────────────────

def menage_perimetre(
    mode: str,
    intervenants: list[str],
    logements: list[str],
    proprietaires: list[str],
    mois: str,
    gestion_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Périmètre ménage : intervenants XOR logements. Jamais les deux.

    Mode LOGEMENT peut utiliser les propriétaires comme sélection indirecte de logements.
    """
    m = str(mode or "").strip().upper()
    if m == MENAGE_MODE_INTERVENANT:
        ints = sorted({str(i).strip() for i in (intervenants or []) if str(i).strip()})
        return {"mode": m, "intervenants": ints, "logements": [], "nb": len(ints)}
    if m == MENAGE_MODE_LOGEMENT:
        directs = [str(l).strip() for l in (logements or []) if str(l).strip()]
        finaux = set(directs)
        for p in (proprietaires or []):
            finaux.update(logements_actifs_proprietaire(p, mois, gestion_rows))
        logs = sorted(l for l in finaux if l)
        return {"mode": m, "intervenants": [], "logements": logs, "nb": len(logs)}
    return {"mode": m, "intervenants": [], "logements": [], "nb": 0}


# ── Réserve de facturation (charges refacturables) ───────────────────────────

def build_reserve_refacturation(
    charge_id: str,
    mois: str,
    libelle: str,
    justificatif: str | None,
    quotes_parts: list[dict[str, Any]],
    proprietaire_par_logement: dict[str, str],
) -> dict[str, Any]:
    """Réserve de facturation : une entrée par quote-part, jamais dupliquée.

    Somme des quotes-parts réservées = montant total refacturable de la charge.
    Statut initial EN_ATTENTE (report / ignore / application décidés au futur processus).
    """
    entrees = []
    for q in quotes_parts:
        lid = str(q.get("logement_id", "")).strip()
        entrees.append({
            "charge_id": charge_id,
            "logement_id": lid,
            "proprietaire_id": proprietaire_par_logement.get(lid),
            "mois": mois,
            "montant_refacturable": round(float(q.get("quote_part", 0.0)), 2),
            "libelle": libelle,
            "justificatif": justificatif or None,
            "statut_traitement": "EN_ATTENTE",
            "decisions_possibles": ["APPLIQUER", "REPORTER", "IGNORER"],
            "trace_decision": None,
        })
    return {
        "charge_id": charge_id,
        "mois": mois,
        "montant_total_refacturable": somme_quotes_parts(quotes_parts),
        "nb_entrees": len(entrees),
        "entrees": entrees,
    }


# ── Effet de la saisie (résumé métier, sans code technique) ──────────────────

def build_effet_saisie(
    *,
    code_impact: str,
    impact_menage: bool,
    perimetre: dict[str, Any] | None,
    menage: dict[str, Any] | None,
    refacturable: bool,
    reserve: dict[str, Any] | None,
    avantage_associe: bool,
    associe_id: str | None,
) -> dict[str, Any]:
    """Résumé « Effet de la saisie » en langage métier."""
    impact = str(code_impact or "").strip().upper()
    effet: dict[str, Any] = {
        "cree_charge_reelle": "Oui",
        "impacte_resultat_reel": "Oui",
        "impacte_resultat_comptable": "Oui" if impact == "IC" else "Non",
        "impact_menage": "Oui" if impact_menage else "Non",
    }
    if impact_menage:
        effet["menage_detail"] = [
            "alimente le coût complet ménage",
            "alimente l'analyse gain / perte ménage",
            "reste analytique",
            "ne crée pas une seconde charge réelle",
            "n'est pas refacturable",
        ]
        if menage:
            if menage.get("mode") == MENAGE_MODE_INTERVENANT:
                effet["perimetre_analytique"] = f"{menage.get('nb', 0)} intervenant(s) ménage"
            else:
                effet["perimetre_analytique"] = f"{menage.get('nb', 0)} logement(s) ménage"
    else:
        if perimetre and perimetre.get("global_conciergerie"):
            effet["perimetre_analytique"] = "global conciergerie"
        elif perimetre:
            effet["perimetre_analytique"] = (
                f"{len(perimetre.get('proprietaires', []))} propriétaire(s), "
                f"{perimetre.get('nb_logements_finaux', 0)} logement(s)"
            )
        else:
            effet["perimetre_analytique"] = "global conciergerie"

    if impact_menage:
        effet["refacturation"] = "non applicable (charge ménage jamais refacturable)"
    elif refacturable and reserve:
        effet["refacturation"] = (
            f"mise en réserve de facturation : {reserve.get('nb_entrees', 0)} quote(s)-part(s), "
            f"total {reserve.get('montant_total_refacturable', 0.0)} €"
        )
    else:
        effet["refacturation"] = "non refacturable"

    if avantage_associe and associe_id:
        effet["avantage_associe"] = f"associé identifié : {associe_id}"
    else:
        effet["avantage_associe"] = "non"
    return effet

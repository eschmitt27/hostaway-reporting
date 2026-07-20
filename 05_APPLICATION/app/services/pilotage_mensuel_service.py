"""APP-5D — Tableau de bord de clôture (`/pilotage-mensuel`) : agrégation LECTURE SEULE.

Aucune règle métier n'est recalculée ici — chaque compteur est lu depuis le service existant du
module concerné (APP-2/3/4/5B/5C). Aucune écriture, aucune donnée dupliquée. Résilient : la panne
d'une source affecte uniquement son bloc ; jamais une page entière en échec.
"""
from datetime import datetime
from typing import Any

import app.config as cfg
from app.readers import controles_cloture_reader as ref_reader
from app.services import banques_controle_service as banque_ctrl
from app.services import charges_service as charges_svc
from app.services import clotures_service as cs
from app.services import controles_actionnable_service as act
from app.services import controles_cloture_service as ctrl_cloture
from app.services import menages_service as menages_svc
from app.services import proprietaires_reglements_service as regl_svc
from app.services import reservations_hh_service as resa_svc

BLOCS = ("moteur", "humain", "controles", "banque", "menages", "reglements", "charges", "reservations")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe(label: str, fn, defaut, indisponibles: set[str]):
    try:
        return fn()
    except Exception:
        indisponibles.add(label)
        return defaut


def _statut_moteur_par_mois(indisponibles: set[str]) -> dict[str, str]:
    def _lire():
        src = ref_reader.cloture_ref()
        if not src.etat.disponible:
            raise RuntimeError("source cloture_ref indisponible")
        out = {}
        for r in src.lignes:
            m = ref_reader.to_mois(r.get("mois"))
            if m:
                out[m] = ref_reader.to_texte(r.get("statut_mois")).upper() or "INCONNU"
        return out
    return _safe("moteur", _lire, {}, indisponibles)


def _humain_par_mois(db_path, indisponibles: set[str]) -> dict[str, dict]:
    return _safe("humain", lambda: {c["mois"]: c for c in cs.lister(db_path)}, {}, indisponibles)


def _progression_par_mois(mois: str, db_path, indisponibles: set[str]) -> dict[str, Any]:
    defaut = {"nb_bloqueurs": None, "nb_exceptions": None, "nb_a_traiter": None}
    return _safe("controles", lambda: cs.calcul_progression(mois, db_path), defaut, indisponibles)


def _nb_mouvements_a_controler(mois: str, db_path, indisponibles: set[str]) -> int | None:
    def _lire():
        r = banque_ctrl.load_liste(db_path=db_path, statut=banque_ctrl.ST_A_CONTROLER, mois=mois)
        if r.get("status") != "OK":
            raise RuntimeError("source banque indisponible")
        return r["count"]
    return _safe("banque", _lire, None, indisponibles)


def _nb_menages_a_controler(mois: str, indisponibles: set[str]) -> int | None:
    def _lire():
        r = menages_svc.load_summary(mois)
        if r.get("etat_global") == "SOURCE_INCOMPLETE":
            raise RuntimeError("source menages incomplete")
        return r.get("a_controler")
    return _safe("menages", _lire, None, indisponibles)


def _nb_reglements_a_controler(mois: str, indisponibles: set[str]) -> int | None:
    def _lire():
        r = regl_svc.load_dashboard(mois)
        return r["summary"]["nb_a_controler"]
    return _safe("reglements", _lire, None, indisponibles)


def _nb_charges_mois(mois: str, indisponibles: set[str]) -> int | None:
    def _lire():
        r = charges_svc.load_list(mois=mois)
        if r.get("status") != "OK":
            raise RuntimeError("source charges indisponible")
        return r["count_affiches"]
    return _safe("charges", _lire, None, indisponibles)


def _nb_reservations_mois(mois: str, indisponibles: set[str]) -> int | None:
    def _lire():
        r = resa_svc.load_list(mois=mois)
        if r.get("status") not in ("OK", "EMPTY"):
            raise RuntimeError("source reservations indisponible")
        return r.get("count_affiches", 0)
    return _safe("reservations", _lire, None, indisponibles)


STATUTS_HUMAIN_LIBELLES = cs.STATUTS_LIBELLES

INDIC_A_TRAITER = "A_TRAITER"
INDIC_EN_COURS = "EN_COURS"
INDIC_PRET = "PRET_POUR_VALIDATION"
INDIC_VALIDE = "VALIDE"
INDIC_ROUVERT = "ROUVERT"
INDIC_INDISPONIBLE = "DONNEES_INDISPONIBLES"

INDIC_LIBELLES = {
    INDIC_A_TRAITER: "À traiter", INDIC_EN_COURS: "En cours",
    INDIC_PRET: "Prêt pour validation", INDIC_VALIDE: "Validé",
    INDIC_ROUVERT: "Rouvert", INDIC_INDISPONIBLE: "Données indisponibles",
}


def _indicateur_global(statut_humain: str | None, nb_bloqueurs, bloc_controles_indisponible: bool) -> str:
    """Dérive l'indicateur global à partir du SEUL statut de suivi humain APP-5C déjà calculé —
    aucune nouvelle règle de clôture, uniquement un libellé de synthèse pour la vue d'ensemble."""
    if bloc_controles_indisponible and statut_humain in (None, cs.ST_NON_DEMARREE):
        return INDIC_INDISPONIBLE
    if statut_humain == cs.ST_VALIDEE:
        return INDIC_VALIDE
    if statut_humain == cs.ST_ROUVERTE:
        return INDIC_ROUVERT
    if statut_humain == cs.ST_A_VALIDER:
        return INDIC_PRET if nb_bloqueurs == 0 else INDIC_EN_COURS
    if statut_humain == cs.ST_EN_PREPARATION:
        return INDIC_EN_COURS
    return INDIC_A_TRAITER


def tableau_mensuel(annee: str = "", mois_filtre: str = "", statut_filtre: str = "",
                    db_path=None) -> dict[str, Any]:
    """Une ligne par mois connu du moteur ou du suivi humain. Chaque compteur peut être `None`
    si sa source est indisponible (jamais une exception qui ferait tomber toute la page)."""
    db_path = db_path or cfg.DB_PATH
    indisponibles: set[str] = set()

    mois_moteur = set(_statut_moteur_par_mois(indisponibles).keys())
    humain = _humain_par_mois(db_path, indisponibles)
    mois_periodes = set(_safe("controles", lambda: ctrl_cloture.load_periods(), [], indisponibles))
    tous_les_mois = sorted(mois_moteur | set(humain.keys()) | mois_periodes, reverse=True)

    statuts_moteur = _statut_moteur_par_mois(indisponibles)
    lignes = []
    for m in tous_les_mois:
        if annee and not m.startswith(annee):
            continue
        if mois_filtre and m != mois_filtre:
            continue
        c = humain.get(m)
        statut_h = c["statut"] if c else cs.ST_NON_DEMARREE
        prog = _progression_par_mois(m, db_path, indisponibles)
        indicateur = _indicateur_global(statut_h, prog.get("nb_bloqueurs"), "controles" in indisponibles)
        if statut_filtre and indicateur != statut_filtre:
            continue
        ligne = {
            "mois": m,
            "statut_moteur": statuts_moteur.get(m, "INCONNU" if "moteur" not in indisponibles else None),
            "statut_humain": statut_h,
            "statut_humain_libelle": STATUTS_HUMAIN_LIBELLES.get(statut_h, statut_h),
            "cloture_id_opaque": c["cloture_id_opaque"] if c else None,
            "nb_bloquants": prog.get("nb_bloqueurs"),
            "nb_exceptions": prog.get("nb_exceptions"),
            "nb_mouvements_a_controler": _nb_mouvements_a_controler(m, db_path, indisponibles),
            "nb_menages_a_controler": _nb_menages_a_controler(m, indisponibles),
            "nb_reglements_a_controler": _nb_reglements_a_controler(m, indisponibles),
            "nb_charges_mois": _nb_charges_mois(m, indisponibles),
            "nb_reservations_mois": _nb_reservations_mois(m, indisponibles),
            "derniere_action_humaine": (c.get("date_validation") or c.get("date_preparation")
                                        or c.get("date_creation")) if c else None,
            "indicateur": indicateur,
            "indicateur_libelle": INDIC_LIBELLES.get(indicateur, indicateur),
        }
        lignes.append(ligne)

    return {
        "lignes": lignes, "read_at": _now(),
        "indisponibles": sorted(indisponibles),
        "annees_disponibles": sorted({m[:4] for m in tous_les_mois}, reverse=True),
        "indicateurs": INDIC_LIBELLES,
    }

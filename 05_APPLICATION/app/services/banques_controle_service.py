"""APP-4B — Contrôle & catégorisation des mouvements bancaires (LECTURE + journal SQLite).

Principes :
- La VÉRITÉ métier est en base : mouvements bruts, classification déterministe, décisions humaines.
  Aucun classeur n'est lu. Une décision humaine ne masque jamais la proposition du moteur — les deux
  restent lisibles côte à côte, et c'est ce qui permet de dire qu'un humain a tranché.
- Identifiant public OPAQUE (aucune donnée de compte) ; correspondance interne préservée.
- Proposition moteur (classification déterministe) et proposition IA (mouvements à envoyer à l'IA)
  sont TOUJOURS préservées et affichées SÉPARÉMENT de la décision humaine.
- Référentiels d'affichage lisibles (Prénom NOM, nom officiel, libellé métier) avec repli explicite ;
  jamais de rapprochement approximatif silencieux.

Ce service lit et journalise. L'application des décisions — c'est-à-dire leur prise en compte dans
la vue de lecture, et le run qui la trace — relève de `banques_controle_writer`. Les flags
`BANQUE_REAL_WRITE_ENABLED/_CONFIRMATION` restent False.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers import banques_reader as reader
from app.readers.banques_reader import to_texte, to_nombre, to_date, to_mois, masquer_compte
from app.services import ref_setup_repo as repo

# ── Workflow de statuts ──────────────────────────────────────────────────────
ST_A_CONTROLER = "A_CONTROLER"
ST_EN_COURS = "EN_COURS"
ST_CONTROLE = "CONTROLE"
ST_RAPPROCHE = "RAPPROCHE"
ST_IGNORE = "IGNORE"
ST_ROUVERT = "ROUVERT"

STATUTS = [ST_A_CONTROLER, ST_EN_COURS, ST_CONTROLE, ST_RAPPROCHE, ST_IGNORE, ST_ROUVERT]
STATUTS_LIBELLES = {
    ST_A_CONTROLER: "À contrôler", ST_EN_COURS: "En cours", ST_CONTROLE: "Contrôlé",
    ST_RAPPROCHE: "Rapproché", ST_IGNORE: "Ignoré avec justification", ST_ROUVERT: "Rouvert",
}
# Transitions autorisées (depuis -> ensemble des cibles permises).
TRANSITIONS: dict[str, set[str]] = {
    ST_A_CONTROLER: {ST_EN_COURS, ST_IGNORE},
    ST_EN_COURS: {ST_CONTROLE, ST_IGNORE, ST_A_CONTROLER},
    ST_CONTROLE: {ST_RAPPROCHE, ST_EN_COURS, ST_IGNORE},
    ST_RAPPROCHE: {ST_ROUVERT},
    ST_IGNORE: {ST_ROUVERT},
    ST_ROUVERT: {ST_EN_COURS, ST_A_CONTROLER},
}


def transition_autorisee(depuis: str, vers: str) -> bool:
    if depuis == vers:
        return True
    return vers in TRANSITIONS.get(depuis, set())


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Identifiant opaque ───────────────────────────────────────────────────────

def id_opaque(mouvement_id_interne: str) -> str:
    """« MVT-<hash12> » stable et sans donnée de compte, dérivé du mouvement_id interne + sel."""
    base = (cfg.BANQUE_OPAQUE_SALT + "|" + to_texte(mouvement_id_interne)).encode("utf-8")
    return "MVT-" + hashlib.sha256(base).hexdigest()[:12]


_INDEX_OPAQUE: dict[str, str] | None = None


def index_opaque() -> dict[str, str]:
    """{id_opaque: mouvement_id_interne} pour tous les mouvements. Mémorisé (vider_cache le réinit.)."""
    global _INDEX_OPAQUE
    if _INDEX_OPAQUE is not None:
        return _INDEX_OPAQUE
    idx: dict[str, str] = {}
    for r in reader.mouvements().lignes:
        mid = to_texte(r.get("mouvement_id"))
        if mid:
            idx[id_opaque(mid)] = mid
    _INDEX_OPAQUE = idx
    return idx


def resoudre_opaque(opaque: str) -> str | None:
    """id_opaque -> mouvement_id interne, ou None si inconnu."""
    return index_opaque().get(to_texte(opaque))


def vider_cache() -> None:
    global _INDEX_OPAQUE
    _INDEX_OPAQUE = None
    reader.vider_cache()


# ── Référentiels d'affichage (lisibles, repli explicite) ─────────────────────

_REF_CACHE: dict[str, dict[str, str]] = {}


def _ref(sheet: str, cle: str, val_fn) -> dict[str, str]:
    """Index d'affichage {clé → libellé} lu en SQLite, jamais dans le classeur.

    Le référentiel vit dans les tables `ref_*` (0029) ; `ref_setup_repo` les lit par nom d'onglet,
    ce qui permet de garder ici le vocabulaire métier (« REF_Proprietaires ») sans rouvrir
    `REF_Setup.xlsm`. Un référentiel non importé rend un index vide : ces libellés sont du confort
    d'affichage, leur absence ne doit jamais empêcher un écran de s'afficher.
    """
    if sheet in _REF_CACHE:
        return _REF_CACHE[sheet]
    out: dict[str, str] = {}
    try:
        for r in repo.lire_onglet(sheet):
            k = to_texte(r.get(cle))
            if k and k != cle:
                out[k] = val_fn(r)
    except Exception:
        out = {}
    _REF_CACHE[sheet] = out
    return out


def libelle_proprietaire(pid: str) -> str:
    pid = to_texte(pid)
    if not pid:
        return ""
    noms = _ref("REF_Proprietaires", "proprietaire_id",
                lambda r: " ".join(x for x in (to_texte(r.get("prenom_proprietaire")),
                                               to_texte(r.get("nom_proprietaire"))) if x).strip())
    v = noms.get(pid)
    return v if v else f"Propriétaire non identifié — {pid}"


def libelle_logement(lid: str) -> str:
    lid = to_texte(lid)
    if not lid:
        return ""
    noms = _ref("REF_Logements", "logement_id",
                lambda r: to_texte(r.get("nom_logement_officiel")) or to_texte(r.get("nom_court")))
    v = noms.get(lid)
    return v if v else f"Logement non identifié — {lid}"


def libelle_reservation(rid: str) -> str:
    rid = to_texte(rid)
    if not rid:
        return ""
    return f"Réservation non identifiée — {rid}"   # résolution fine différée (source réservations)


def libelle_facture(fid: str) -> str:
    fid = to_texte(fid)
    if not fid:
        return ""
    return f"Facture non identifiée — {fid}"


# Libellés métier des types de flux (REF_Types_Flux.type_flux = le CODE technique, pas un libellé
# humain ; REF_Types_Flux.description est une phrase longue). Mapping curé validé métier ; jamais
# inventé pour un code hors de cette liste (repli explicite).
LIBELLES_TYPE_FLUX: dict[str, str] = {
    "TYPE_FLUX_001": "Virement d'associé",
    "TYPE_FLUX_002": "Dépense personnelle réglée par le compte professionnel",
    "TYPE_FLUX_003": "Revenu d'une réservation hors Hostaway",
    "TYPE_FLUX_004": "Charge payée personnellement ou en espèces",
    "TYPE_FLUX_005": "Remboursement à un associé",
    "TYPE_FLUX_006": "Acompte sur facture propriétaire",
    "TYPE_FLUX_007": "Reversement au propriétaire",
    "TYPE_FLUX_008": "Paiement d'un prestataire en espèces",
    "TYPE_FLUX_009": "Achat lié au ménage",
    "TYPE_FLUX_010": "Frais liés au local",
    "TYPE_FLUX_011": "Charge exceptionnelle refacturable",
    "TYPE_FLUX_012": "Charge récurrente refacturable",
    "TYPE_FLUX_013": "Coût de main-d'œuvre interne du ménage",
    "TYPE_FLUX_014": "Coût réel d'un ménage externe",
    "TYPE_FLUX_015": "Indemnité kilométrique",
    "TYPE_FLUX_016": "Frais bancaires",
    "TYPE_FLUX_017": "Revenu de réservation Hostaway",
}


def libelle_type_flux(tid: str) -> str:
    """Libellé métier humain. Repli explicite « non documenté » si le code n'est pas dans le mapping
    curé (jamais un libellé inventé à partir de la description longue du référentiel)."""
    tid = to_texte(tid)
    if not tid:
        return ""
    v = LIBELLES_TYPE_FLUX.get(tid)
    return v if v else f"Type de flux non documenté — {tid}"


# Libellés métier des catégories bancaires (codes lot8b/REF_Banque_Regles). Les 17 codes réellement
# produits par le moteur sur la banque réelle sont couverts explicitement ; tout code inconnu retombe
# sur un repli lisible (accents/majuscules normalisés), jamais un libellé métier inventé.
LIBELLES_CATEGORIE: dict[str, str] = {
    "ABONNEMENT_MOBILITE": "Abonnement mobilité",
    "ACHAT_PERSO_CB": "Achat personnel par carte bancaire",
    "COTISATIONS_SOCIALES": "Cotisations sociales",
    "COTISATION_PREVOYANCE": "Cotisation prévoyance",
    "DEPENSE_CB_A_CLASSIFIER": "Dépense par carte bancaire à classifier",
    "EFFET_DOMICILIE": "Effet domicilié",
    "FACTURE_PRESTATAIRE": "Facture prestataire",
    "FRAIS_BANCAIRES": "Frais bancaires",
    "HONORAIRES_COMPTABLES": "Honoraires comptables",
    "IMPAYE": "Impayé",
    "LOGICIEL_GESTION": "Logiciel de gestion",
    "LOYER_LOCAL_A_CONTROLER": "Loyer du local à contrôler",
    "PAYOUT_PLATEFORME": "Versement plateforme de réservation",
    "SANTE_A_CONTROLER": "Santé à contrôler",
    "VIREMENT_PROPRIETAIRE_A_RAPPROCHER": "Virement propriétaire à rapprocher",
    "VIREMENT_SORTANT_A_CONTROLER": "Virement sortant à contrôler",
    "VIR_ASSOCIE": "Virement associé",
    "VIR_INST_GENERIQUE": "Virement instantané générique",
}


def libelle_categorie(code: str) -> str:
    """Libellé lisible d'une catégorie bancaire. Mapping curé en priorité (codes réels du moteur) ;
    repli normalisé (accents/majuscules) pour un code non couvert — jamais un libellé métier inventé."""
    c = to_texte(code)
    if not c:
        return ""
    v = LIBELLES_CATEGORIE.get(c)
    if v:
        return v
    return c.replace("_", " ").capitalize()


def options_reference() -> dict[str, list[dict[str, str]]]:
    """Listes pour les sélecteurs (valeur technique + libellé lisible)."""
    props = _ref("REF_Proprietaires", "proprietaire_id",
                 lambda r: " ".join(x for x in (to_texte(r.get("prenom_proprietaire")),
                                                to_texte(r.get("nom_proprietaire"))) if x).strip())
    logs = _ref("REF_Logements", "logement_id",
                lambda r: to_texte(r.get("nom_logement_officiel")) or to_texte(r.get("nom_court")))
    # REF_Types_Flux ne fournit qu'un CODE (type_flux) et une phrase longue (description) : les ids
    # valides viennent du référentiel, le libellé métier vient du mapping curé (jamais le code brut).
    flux_ids = set(_ref("REF_Types_Flux", "type_flux_id", lambda r: to_texte(r.get("type_flux"))))
    cats = sorted({to_texte(r.get("categorie")) for r in reader.mouvements().lignes
                   if to_texte(r.get("categorie"))})
    return {
        "proprietaires": [{"id": k, "libelle": v or k} for k, v in sorted(props.items())],
        "logements": [{"id": k, "libelle": v or k} for k, v in sorted(logs.items())],
        "types_flux": [{"id": tid, "libelle": libelle_type_flux(tid)} for tid in sorted(flux_ids)],
        "categories": [{"id": c, "libelle": libelle_categorie(c)} for c in cats],
        "statuts": [{"id": s, "libelle": STATUTS_LIBELLES[s]} for s in STATUTS],
    }


# ── Décisions (journal SQLite) ───────────────────────────────────────────────

def _override_actif(db_path, opaque: str) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM banque_overrides WHERE mouvement_id_opaque=? AND actif=1 "
            "ORDER BY version DESC LIMIT 1", (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def historique(db_path, opaque: str) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT statut_controle, categorie_validee, commentaire, justification, date_action, "
            "auteur, version, actif FROM banque_overrides WHERE mouvement_id_opaque=? "
            "ORDER BY version DESC", (opaque,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _mouvement_brut(mid_interne: str) -> dict[str, Any] | None:
    for r in reader.mouvements().lignes:
        if to_texte(r.get("mouvement_id")) == mid_interne:
            return r
    return None


def _proposition_ia(mid_interne: str) -> dict[str, Any] | None:
    """Ligne IA_Classification réelle pour ce mouvement, ou None.

    IMPORTANT — audité : IA_Classification ne contient, sur la banque réelle, qu'une ligne
    d'information (« Aucune ligne orientee IA - toutes les lignes classees par regles
    deterministes »), jamais un `mouvement_id` valide. Le filtre par égalité stricte exclut donc
    naturellement ce stub — cette fonction ne retourne quelque chose que si une VRAIE ligne IA
    existe un jour (mouvement_id + categorie_proposee renseignés).
    """
    for r in reader.ia_classification().lignes:
        if to_texte(r.get("mouvement_id")) != mid_interne:
            continue
        cat = to_texte(r.get("categorie_proposee"))
        if not cat:
            continue
        return {"categorie_proposee": cat, "score_confiance": to_nombre(r.get("score_confiance")),
                "commentaire_ia": to_texte(r.get("commentaire_ia"))}
    return None


# Origine de la classification — auditée dans lot8b (`apply_rule`, first-match déterministe sur
# libellé normalisé) : 100% des lignes réelles portent source_classification="REGLE_DETERMINISTE",
# aucun appel API/modèle nulle part dans lot8a/8b/8c. Le libellé UI reflète cette preuve, jamais le
# mot « IA » tant qu'aucune ligne IA_Classification réelle n'existe.
SOURCES_PROPOSITION_LIBELLES = {
    "REGLE_DETERMINISTE": "Proposition automatique",
    "IA": "Proposition IA",
}


def libelle_source_proposition(source_classification: str, a_proposition_ia: bool) -> str:
    if a_proposition_ia and to_texte(source_classification) not in ("REGLE_DETERMINISTE", ""):
        return "Proposition IA"
    if to_texte(source_classification) == "REGLE_DETERMINISTE":
        return "Proposition automatique"
    if not source_classification:
        return "Proposition du moteur"   # source incertaine — jamais « IA » sans preuve
    return "Proposition du moteur"


def load_fiche(opaque: str, db_path=None) -> dict[str, Any] | None:
    """Fiche mouvement actionnable. None si l'identifiant opaque est inconnu (→ 404)."""
    db_path = db_path or cfg.DB_PATH
    mid = resoudre_opaque(opaque)
    if mid is None:
        return None
    row = _mouvement_brut(mid)
    if row is None:
        return None
    ia = _proposition_ia(mid)
    override = _override_actif(db_path, opaque)

    statut_moteur = to_texte(row.get("statut_controle"))
    statut_effectif = (override or {}).get("statut_controle") or statut_moteur or ST_A_CONTROLER
    source_class = to_texte(row.get("source_classification"))
    regle_id = to_texte(row.get("regle_id_appliquee"))
    return {
        "id_opaque": opaque,
        "date_operation": to_date(row.get("date_operation")),
        "date_valeur": to_date(row.get("date_valeur")),
        "mois": to_mois(row.get("date_operation")),
        "montant": to_nombre(row.get("montant")),
        "sens": to_texte(row.get("sens")),
        "compte_masque": masquer_compte(row.get("compte_id")),
        "libelle": to_texte(row.get("libelle")),           # libellé sécurisé (jamais le compte)
        "niveau_risque": to_texte(row.get("niveau_risque")),
        # Proposition MOTEUR (jamais écrasée). Libellé + source auditée (voir SOURCES_PROPOSITION_LIBELLES) :
        # jamais « IA » sans preuve — la banque réelle est classée à 100% par règles déterministes.
        "proposition_moteur": {
            "categorie": to_texte(row.get("categorie")),
            "categorie_libelle": libelle_categorie(row.get("categorie")),
            "type_flux_id": to_texte(row.get("type_flux_id")),
            "type_flux_libelle": libelle_type_flux(row.get("type_flux_id")),
            "statut_controle": statut_moteur,
            "libelle_source": libelle_source_proposition(source_class, ia is not None),
            "source_classification": source_class or "INCONNUE",
            "regle_id_appliquee": regle_id,
        },
        # Proposition IA (jamais écrasée) — None tant qu'aucune ligne IA réelle n'existe (audité).
        "proposition_ia": ia,
        # Décision HUMAINE validée (journal SQLite) — séparée de la proposition
        "decision": _decision_affichage(override) if override else None,
        "statut_effectif": statut_effectif,
        "statut_effectif_libelle": STATUTS_LIBELLES.get(statut_effectif, statut_effectif),
        "transitions_possibles": sorted(TRANSITIONS.get(statut_effectif, set())),
        "historique": historique(db_path, opaque),
        "read_at": _now(),
    }


def _decision_affichage(ov: dict[str, Any]) -> dict[str, Any]:
    return {
        "categorie_validee": ov.get("categorie_validee"),
        "categorie_libelle": libelle_categorie(ov.get("categorie_validee")),
        "type_flux_id": ov.get("type_flux_id"),
        "type_flux_libelle": libelle_type_flux(ov.get("type_flux_id")),
        "proprietaire_id": ov.get("proprietaire_id"),
        "proprietaire_libelle": libelle_proprietaire(ov.get("proprietaire_id")) if ov.get("proprietaire_id") else "",
        "logement_id": ov.get("logement_id"),
        "logement_libelle": libelle_logement(ov.get("logement_id")) if ov.get("logement_id") else "",
        "reservation_id": ov.get("reservation_id"),
        "reservation_libelle": libelle_reservation(ov.get("reservation_id")) if ov.get("reservation_id") else "",
        "facture_id": ov.get("facture_id"),
        "facture_libelle": libelle_facture(ov.get("facture_id")) if ov.get("facture_id") else "",
        "statut_controle": ov.get("statut_controle"),
        "commentaire": ov.get("commentaire"),
        "justification": ov.get("justification"),
        "auteur": ov.get("auteur"),
        "date_action": ov.get("date_action"),
        "version": ov.get("version"),
    }


class DecisionRefusee(Exception):
    """Décision refusée (entité inconnue, statut invalide, justification manquante, conflit version)."""


def enregistrer_decision(opaque: str, *, categorie=None, type_flux_id=None, proprietaire_id=None,
                         logement_id=None, reservation_id=None, facture_id=None,
                         statut_controle=None, commentaire=None, justification=None,
                         auteur="local", version_attendue: int | None = None,
                         db_path=None) -> dict[str, Any]:
    """Journalise une décision (nouvelle version active). Valide entités, statut, justification, conflit.

    N'écrit aucun fichier : décision applicative en base uniquement. Sa prise en compte dans la vue
    de lecture est tracée séparément par le writer.
    """
    db_path = db_path or cfg.DB_PATH
    mid = resoudre_opaque(opaque)
    if mid is None:
        raise DecisionRefusee("Mouvement inconnu.")
    row = _mouvement_brut(mid)
    if row is None:
        raise DecisionRefusee("Mouvement absent de la source.")

    override = _override_actif(db_path, opaque)
    statut_courant = (override or {}).get("statut_controle") or to_texte(row.get("statut_controle")) or ST_A_CONTROLER
    version_courante = int((override or {}).get("version") or 0)

    # Conflit de version optimiste
    if version_attendue is not None and version_attendue != version_courante:
        raise DecisionRefusee(f"Conflit de version (attendu {version_attendue}, courant {version_courante}).")

    cible = to_texte(statut_controle) or statut_courant
    if cible not in STATUTS:
        raise DecisionRefusee(f"Statut inconnu : {cible}.")
    if not transition_autorisee(statut_courant, cible):
        raise DecisionRefusee(f"Transition interdite : {statut_courant} → {cible}.")
    if cible == ST_IGNORE and not to_texte(justification):
        raise DecisionRefusee("Une justification est obligatoire pour « Ignoré ».")

    opts = options_reference()
    _valider_entite(proprietaire_id, {o["id"] for o in opts["proprietaires"]}, "Propriétaire")
    _valider_entite(logement_id, {o["id"] for o in opts["logements"]}, "Logement")
    _valider_entite(type_flux_id, {o["id"] for o in opts["types_flux"]}, "Type de flux")

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE banque_overrides SET actif=0 WHERE mouvement_id_opaque=? AND actif=1", (opaque,))
        cur = conn.execute(
            """INSERT INTO banque_overrides
               (mouvement_id_opaque, mouvement_id_interne, categorie_validee, type_flux_id,
                proprietaire_id, logement_id, reservation_id, facture_id, statut_controle,
                commentaire, justification, proposition_categorie, proposition_ia, auteur,
                source_action, actif, version)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'APP',1,?)""",
            (opaque, mid, to_texte(categorie) or None, to_texte(type_flux_id) or None,
             to_texte(proprietaire_id) or None, to_texte(logement_id) or None,
             to_texte(reservation_id) or None, to_texte(facture_id) or None, cible,
             to_texte(commentaire) or None, to_texte(justification) or None,
             to_texte(row.get("categorie")) or None,
             (_proposition_ia(mid) or {}).get("categorie_proposee"),
             auteur, version_courante + 1),
        )
        conn.commit()
        new_id = cur.lastrowid
    finally:
        conn.close()
    return {"ok": True, "override_id": new_id, "version": version_courante + 1,
            "statut_controle": cible, "id_opaque": opaque}


def _valider_entite(valeur, valides: set[str], libelle: str) -> None:
    v = to_texte(valeur)
    if v and v not in valides:
        raise DecisionRefusee(f"{libelle} inconnu : {v}.")


# ── Liste (filtres) ──────────────────────────────────────────────────────────

def load_liste(db_path=None, statut: str = "", categorie: str = "", proprietaire_id: str = "",
               logement_id: str = "", anomalie: str = "", mois: str = "") -> dict[str, Any]:
    """Liste des mouvements avec statut effectif (décision > moteur) et proposition moteur."""
    db_path = db_path or cfg.DB_PATH
    src = reader.mouvements()
    if not src.etat.disponible:
        return {"status": "SOURCE_INDISPONIBLE", "etat": src.etat, "rows": [], "read_at": _now()}

    conn = get_db(db_path)
    try:
        overs = {r["mouvement_id_opaque"]: dict(r) for r in conn.execute(
            "SELECT * FROM banque_overrides WHERE actif=1").fetchall()}
    finally:
        conn.close()

    rows = []
    for r in src.lignes:
        mid = to_texte(r.get("mouvement_id"))
        if not mid:
            continue
        opq = id_opaque(mid)
        ov = overs.get(opq)
        st_moteur = to_texte(r.get("statut_controle"))
        st_eff = (ov or {}).get("statut_controle") or st_moteur or ST_A_CONTROLER
        v = {
            "id_opaque": opq,
            "date_operation": to_date(r.get("date_operation")),
            "mois": to_mois(r.get("date_operation")),
            "montant": to_nombre(r.get("montant")),
            "sens": to_texte(r.get("sens")),
            "compte_masque": masquer_compte(r.get("compte_id")),
            "libelle": to_texte(r.get("libelle")),
            "categorie_moteur": to_texte(r.get("categorie")),
            "categorie_libelle": libelle_categorie(r.get("categorie")),
            "categorie_validee": (ov or {}).get("categorie_validee"),
            "statut_effectif": st_eff,
            "statut_effectif_libelle": STATUTS_LIBELLES.get(st_eff, st_eff),
            "a_decision": ov is not None,
            "proprietaire_id": (ov or {}).get("proprietaire_id"),
        }
        if statut and st_eff != statut:
            continue
        if categorie and to_texte(r.get("categorie")) != categorie and (ov or {}).get("categorie_validee") != categorie:
            continue
        if proprietaire_id and (ov or {}).get("proprietaire_id") != proprietaire_id:
            continue
        if mois and v["mois"] != mois:
            continue
        if anomalie == "oui" and st_eff not in (ST_A_CONTROLER, ST_EN_COURS, ST_ROUVERT):
            continue
        if anomalie == "non" and st_eff in (ST_A_CONTROLER, ST_EN_COURS, ST_ROUVERT):
            continue
        rows.append(v)
    rows.sort(key=lambda x: (x["date_operation"], x["id_opaque"]))
    return {"status": "OK", "etat": src.etat, "rows": rows, "count": len(rows),
            "options": options_reference(), "read_at": _now()}


def compter_a_controler(db_path=None) -> int:
    """Nombre de mouvements réellement à contrôler (statut effectif hors Contrôlé/Rapproché/Ignoré).

    Utilisé pour le compteur du bouton « Contrôler les mouvements (N) » sur /banques-caisse.
    """
    db_path = db_path or cfg.DB_PATH
    src = reader.mouvements()
    if not src.etat.disponible:
        return 0
    conn = get_db(db_path)
    try:
        overs = {r["mouvement_id_opaque"]: r["statut_controle"] for r in conn.execute(
            "SELECT mouvement_id_opaque, statut_controle FROM banque_overrides WHERE actif=1").fetchall()}
    finally:
        conn.close()
    n = 0
    for r in src.lignes:
        mid = to_texte(r.get("mouvement_id"))
        if not mid:
            continue
        opq = id_opaque(mid)
        st_eff = overs.get(opq) or to_texte(r.get("statut_controle")) or ST_A_CONTROLER
        if st_eff in (ST_A_CONTROLER, ST_EN_COURS, ST_ROUVERT):
            n += 1
    return n

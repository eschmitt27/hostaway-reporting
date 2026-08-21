"""Administration du référentiel — écriture SQLite, jamais Excel (migration 0051).

POSITION DANS L'ARCHITECTURE
    route → service métier (logements_creation/gestion) → CE MODULE → tables `ref_*` (0029)

`referentiel_service` lit, ce module écrit. Les deux parlent aux mêmes tables : il n'existe pas
deux vérités du référentiel.

POURQUOI L'ÉCRITURE N'EST PLUS GARDÉE PAR LES FLAGS D'ÉCRITURE RÉELLE
Les flags `CHARGES_REAL_WRITE_*` protègent l'écriture dans un CLASSEUR SOURCE réel — un fichier que
l'application ne possède pas et qu'un tiers peut avoir ouvert. Écrire dans `app.db`, la base de
l'application, est au contraire son mode de fonctionnement normal : les factures, les clôtures, les
décisions bancaires s'y écrivent déjà sans flag. Conserver la garde ici rendrait l'administration du
référentiel inutilisable par défaut, sans rien protéger de plus.

LES LIGNES SAISIES ICI SURVIVENT À UN RÉIMPORT
Elles portent `import_id = SAISIE_APPLICATION`. `ref_setup_import_service` ne les supprime pas et
signale tout conflit de clé plutôt que d'écraser. Sans cela, réimporter le classeur effacerait la
saisie applicative — et SQLite ne serait pas canonique.

HISTORISATION : ON NE RÉÉCRIT JAMAIS LE PASSÉ
Un changement de propriétaire ou de taux CLÔT la période courante (`date_fin` = veille) et en OUVRE
une nouvelle. Aucune ligne close n'est jamais modifiée, aucune ligne n'est jamais supprimée. C'est
l'invariant dont dépend `lib_ref_history.resolve_management_period` / `resolve_commission_rate`.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from app.db.connection import get_db
from app.services import ref_setup_repo as repo

SOURCE_APPLICATION = "SAISIE_APPLICATION"

TABLE_LOGEMENTS = "ref_logements"
TABLE_GESTION = "ref_gestion_logements_hist"
TABLE_TAUX = "ref_taux_commission"
TABLE_PROPRIETAIRES = "ref_proprietaires"
TABLE_TYPES = "ref_types_logements"

# Codes d'erreur — repris tels quels des services qui délèguent ici, pour que les écrans et les
# tests existants continuent de parler le même vocabulaire.
E_REFERENTIEL_ABSENT = "E_REFERENTIEL_NON_INITIALISE"
E_CLE_MANQUANTE = "V01_ID_MANQUANT"
E_CLE_EXISTANTE = "V02_ID_DEJA_UTILISE"
E_INTROUVABLE = "V01_LOGEMENT_INCONNU"
E_DATE_INVALIDE = "V04_DATE_INVALIDE"
E_PERIODE_INCOHERENTE = "V09_PERIODE_INCOHERENTE"
E_ECRITURE = "E_ECRITURE_REFUSEE"

MESSAGES = {
    E_REFERENTIEL_ABSENT: (
        "Référentiel non initialisé : faites l'import initial depuis l'écran Référentiel Setup, "
        "ou créez les données depuis l'administration."),
    E_CLE_MANQUANTE: "L'identifiant est obligatoire.",
    E_CLE_EXISTANTE: "Cet identifiant existe déjà.",
    E_INTROUVABLE: "Cet enregistrement n'existe pas dans le référentiel.",
    E_DATE_INVALIDE: "Date invalide (format AAAA-MM-JJ attendu).",
    E_PERIODE_INCOHERENTE: (
        "La date demandée est antérieure au début de la période en cours : la clôturer ainsi "
        "produirait une période négative."),
    E_ECRITURE: "Écriture refusée.",
}


def txt(v: Any) -> str:
    return str(v or "").strip()


def date_valide(d: str) -> bool:
    try:
        date.fromisoformat(txt(d))
        return True
    except ValueError:
        return False


def veille(d: str) -> str:
    return (date.fromisoformat(txt(d)) - timedelta(days=1)).isoformat()


def refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def disponible(*, db_path=None) -> bool:
    """Le référentiel est-il exploitable ? Aucun repli sur le classeur (§18)."""
    return repo.est_disponible(db_path=db_path)


# ── Lecture (pour valider avant d'écrire) ───────────────────────────────────────────────────────

def lignes(table: str, *, db_path=None) -> list[dict[str, str]]:
    return repo.lire_table(table, db_path=db_path)


def ligne(table: str, cle_valeur: str, *, db_path=None) -> dict[str, str] | None:
    return repo.lire_par_cle(table, txt(cle_valeur), db_path=db_path)


#: Référentiels à périodes : une ligne sans `date_fin` est la ligne COURANTE. Tout le moteur
#: (`lib_ref_history.resolve_management_period`, `resolve_commission_rate`) repose là-dessus.
TABLES_HISTORISEES = ("ref_gestion_logements_hist", "ref_taux_commission")


def periodes_ouvertes(table: str, logement_id: str, *, db_path=None) -> list[dict[str, str]]:
    """Toutes les lignes historisées encore ouvertes (`date_fin` vide) pour ce logement.

    Renvoie une liste, et non une ligne, parce qu'un référentiel PEUT être ambigu : l'application
    s'interdit de créer ce cas (voir `inserer`), mais un classeur importé ou une correction faite
    directement en base peuvent l'introduire. Le rendre irreprésentable en base a été essayé puis
    écarté — voir la note de la migration 0051.
    """
    lid = txt(logement_id)
    return [r for r in lignes(table, db_path=db_path)
            if txt(r.get("logement_id")) == lid and not txt(r.get("date_fin"))]


def periode_ouverte(table: str, logement_id: str, *, db_path=None) -> dict[str, str] | None:
    """Période courante de ce logement, ou None s'il n'y en a pas.

    En cas d'ambiguïté, renvoie la dernière : les APPELANTS EN ÉCRITURE veulent alors clore ce qui
    traîne. Les appelants en LECTURE qui doivent refuser de deviner (`logements_service`, qui rend
    `A_CONTROLER`) passent par `periodes_ouvertes` et comptent eux-mêmes.
    """
    ouvertes = periodes_ouvertes(table, logement_id, db_path=db_path)
    return ouvertes[-1] if ouvertes else None


# ── Écriture ────────────────────────────────────────────────────────────────────────────────────

def _colonnes(table: str) -> tuple[str, ...]:
    from app.services import ref_setup_catalogue as cat
    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        raise KeyError(f"Table hors catalogue : {table}")
    return feuille.colonnes


def journaliser(conn, table: str, cle: str, action: str, avant: Any, apres: Any,
                acteur: str = "", commentaire: str = "") -> None:
    conn.execute(
        "INSERT INTO ref_admin_evenements (table_cible, cle, action, avant_json, apres_json, "
        "acteur, commentaire) VALUES (?,?,?,?,?,?,?)",
        (table, txt(cle), action,
         json.dumps(avant, ensure_ascii=False, default=str) if avant else None,
         json.dumps(apres, ensure_ascii=False, default=str) if apres else None,
         acteur or None, commentaire or None))


def inserer(table: str, valeurs: dict[str, Any], *, action: str, acteur: str = "",
            commentaire: str = "", db_path=None) -> dict[str, Any]:
    """Insère une ligne de référentiel saisie dans l'application, et la journalise."""
    colonnes = _colonnes(table)
    ligne_complete = {c: txt(valeurs.get(c)) for c in colonnes}

    # Deux périodes ouvertes pour un même logement rendent la période courante indécidable : le
    # moteur choisirait un propriétaire ou un taux au hasard. On ouvre donc UNIQUEMENT après avoir
    # clos ce qui précède (`clore_periode`), jamais en parallèle.
    if table in TABLES_HISTORISEES and not txt(ligne_complete.get("date_fin")):
        lid = txt(ligne_complete.get("logement_id"))
        if lid and periodes_ouvertes(table, lid, db_path=db_path):
            return refus(E_PERIODE_INCOHERENTE,
                         f"{lid} a déjà une période ouverte dans {table} ; il faut la clore avant "
                         "d'en ouvrir une nouvelle")

    conn = get_db(db_path)
    try:
        cols = ", ".join((*colonnes, "import_id"))
        trous = ", ".join(["?"] * (len(colonnes) + 1))
        conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({trous})",
                     tuple(ligne_complete[c] for c in colonnes) + (SOURCE_APPLICATION,))
        journaliser(conn, table, ligne_complete.get(_cle(table), ""), action, None,
                    ligne_complete, acteur, commentaire)
        conn.commit()
    except Exception as exc:   # noqa: BLE001 — toute panne devient un refus lisible
        conn.rollback()
        return refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    finally:
        conn.close()
    return {"ok": True, "ligne": ligne_complete}


def mettre_a_jour(table: str, cle_valeur: str, champs: dict[str, Any], *, action: str,
                  acteur: str = "", commentaire: str = "", db_path=None) -> dict[str, Any]:
    """Met à jour des champs d'une ligne identifiée par sa clé métier, et journalise l'avant/après.

    Une ligne modifiée dans l'application devient une ligne applicative (`import_id`) : un réimport
    du classeur ne doit pas la réécraser silencieusement.
    """
    cle = _cle(table)
    colonnes = set(_colonnes(table))
    inconnues = [c for c in champs if c not in colonnes]
    if inconnues:
        return refus(E_ECRITURE, f"colonnes inconnues : {inconnues}")

    avant = ligne(table, cle_valeur, db_path=db_path)
    if avant is None:
        return refus(E_INTROUVABLE, txt(cle_valeur))

    apres = {**avant, **{c: txt(v) for c, v in champs.items()}}
    conn = get_db(db_path)
    try:
        assignations = ", ".join(f"{c} = ?" for c in champs) + ", import_id = ?"
        conn.execute(f"UPDATE {table} SET {assignations} WHERE {cle} = ?",
                     tuple(txt(v) for v in champs.values()) + (SOURCE_APPLICATION, txt(cle_valeur)))
        journaliser(conn, table, cle_valeur, action, avant, apres, acteur, commentaire)
        conn.commit()
    except Exception as exc:   # noqa: BLE001
        conn.rollback()
        return refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
    finally:
        conn.close()
    return {"ok": True, "avant": avant, "apres": apres}


def clore_periode(table: str, logement_id: str, date_fin: str, *, statut: str = "",
                  acteur: str = "", db_path=None) -> dict[str, Any]:
    """Clôt la période ouverte d'un logement. Ne touche JAMAIS une période déjà close.

    Refuse une `date_fin` antérieure au `date_debut` de la période courante : une période négative
    rendrait la résolution datée incohérente au lieu de la corriger.
    """
    ouverte = periode_ouverte(table, logement_id, db_path=db_path)
    if ouverte is None:
        return {"ok": True, "cloturee": False}

    debut = txt(ouverte.get("date_debut"))
    if debut and date_valide(debut) and date_valide(date_fin) \
            and date.fromisoformat(txt(date_fin)) < date.fromisoformat(debut):
        return refus(E_PERIODE_INCOHERENTE, f"{debut} → {date_fin}")

    cle = _cle(table)
    champs: dict[str, Any] = {"date_fin": txt(date_fin)}
    if statut and "statut_gestion" in _colonnes(table):
        champs["statut_gestion"] = statut
    res = mettre_a_jour(table, ouverte.get(cle, ""), champs, action="CLOTURE_PERIODE",
                        acteur=acteur, db_path=db_path)
    if not res.get("ok"):
        return res
    return {"ok": True, "cloturee": True, "ligne": res["apres"]}


def _cle(table: str) -> str:
    from app.services import ref_setup_catalogue as cat
    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        raise KeyError(f"Table hors catalogue : {table}")
    return feuille.cle


def historique_evenements(table: str = "", cle: str = "", *, limite: int = 50,
                          db_path=None) -> list[dict[str, Any]]:
    """Journal des modifications, du plus récent au plus ancien."""
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name='ref_admin_evenements'").fetchone() is None:
            return []
        where, params = [], []
        if table:
            where.append("table_cible = ?"); params.append(table)
        if cle:
            where.append("cle = ?"); params.append(txt(cle))
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        return [dict(r) for r in conn.execute(
            f"SELECT * FROM ref_admin_evenements{clause} ORDER BY id DESC LIMIT ?",
            (*params, limite))]
    finally:
        conn.close()


# ── Organisation des écrans d'administration ────────────────────────────────────────────────────
#
# 28 tables, PAS 28 écrans : elles sont regroupées par usage métier. Un référentiel se cherche par
# « à quoi il sert », pas par son nom technique.
#
# `lecture_seule` marque les tables HISTORISÉES qui ont déjà un parcours dédié (fiche logement) :
# les éditer librement ici permettrait de réécrire une période passée en contournant la règle de
# clôture/ouverture. Elles restent consultables — c'est leur modification qui passe par le parcours
# métier.

CATEGORIES: tuple[dict[str, Any], ...] = (
    {
        "cle": "parc",
        "titre": "Parc & propriétaires",
        "description": "Les biens gérés, leurs propriétaires et leur rattachement daté.",
        "tables": ("ref_logements", "ref_proprietaires", "ref_types_logements",
                   "ref_mapping_logements", "ref_gestion_logements_hist"),
    },
    {
        "cle": "tarifs",
        "titre": "Tarifs & coûts historisés",
        "description": "Taux de commission et coûts de ménage, avec leurs périodes de validité.",
        "tables": ("ref_taux_commission", "ref_couts_standards_menage",
                   "ref_couts_menage_interne", "ref_taux_heures_menage",
                   "ref_charges_recurrentes", "ref_abonnements_logiciels"),
    },
    {
        "cle": "menages",
        "titre": "Ménages & intervenants",
        "description": "Qui réalise les ménages et comment leurs lignes sont typées.",
        "tables": ("ref_intervenants", "ref_types_lignes_menage"),
    },
    {
        "cle": "banque",
        "titre": "Banque & comptable",
        "description": "Règles de classement bancaire et nomenclatures comptables.",
        "tables": ("ref_banque_regles", "ref_categories_charges", "ref_types_flux",
                   "ref_types_affectation", "ref_modes_paiement", "ref_cartes_paiement",
                   "ref_codes_impact", "ref_statuts_payout"),
    },
    {
        "cle": "exploitation",
        "titre": "Exploitation & divers",
        "description": "Clôture mensuelle, canaux, statuts et paramètres généraux.",
        "tables": ("ref_cloture_mensuelle", "ref_canaux_reservation", "ref_statuts",
                   "ref_sources_systeme", "ref_parametres_generaux", "ref_associes",
                   "ref_assoc_mode"),
    },
)

# Tables dont la modification passe par un parcours métier dédié (voir ci-dessus).
LECTURE_SEULE = {
    "ref_gestion_logements_hist": "Fiche logement → changement de propriétaire / archivage",
    "ref_taux_commission": "Fiche logement → changement de taux de commission",
}

# Colonne portant l'activation, quand la table en a une.
COLONNE_ACTIF = "actif"


def categories(*, db_path=None) -> list[dict[str, Any]]:
    """Les catégories, enrichies du libellé et de la volumétrie de chaque table."""
    from app.services import ref_setup_catalogue as cat

    compte = repo.compter(db_path=db_path) if disponible(db_path=db_path) else {}
    out = []
    for c in CATEGORIES:
        tables = []
        for t in c["tables"]:
            feuille = cat.PAR_TABLE.get(t)
            if feuille is None:
                continue
            tables.append({
                "table": t,
                "onglet": feuille.onglet,
                "libelle": feuille.onglet.replace("REF_", "").replace("_", " "),
                "cle": feuille.cle,
                "nb_lignes": compte.get(t, 0),
                "lecture_seule": t in LECTURE_SEULE,
                "motif_lecture_seule": LECTURE_SEULE.get(t, ""),
            })
        out.append({**c, "tables": tables})
    return out


def decrire_table(table: str, *, db_path=None) -> dict[str, Any]:
    """Métadonnées d'une table pour l'écran de liste/édition."""
    from app.services import ref_setup_catalogue as cat

    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        return {"ok": False, "code": "TABLE_INCONNUE", "table": table}
    return {
        "ok": True,
        "table": table,
        "onglet": feuille.onglet,
        "cle": feuille.cle,
        "colonnes": list(feuille.colonnes),
        "lecture_seule": table in LECTURE_SEULE,
        "motif_lecture_seule": LECTURE_SEULE.get(table, ""),
        "a_colonne_actif": COLONNE_ACTIF in feuille.colonnes,
    }


def creer_ligne(table: str, valeurs: dict[str, Any], *, acteur: str = "",
                db_path=None) -> dict[str, Any]:
    """Création depuis l'administration : refuse une clé vide ou déjà prise."""
    meta = decrire_table(table, db_path=db_path)
    if not meta.get("ok"):
        return refus("TABLE_INCONNUE", table)
    if meta["lecture_seule"]:
        return refus(E_ECRITURE, meta["motif_lecture_seule"])
    if not disponible(db_path=db_path):
        return refus(E_REFERENTIEL_ABSENT)

    cle_valeur = txt(valeurs.get(meta["cle"]))
    if not cle_valeur:
        return refus(E_CLE_MANQUANTE, meta["cle"])
    if ligne(table, cle_valeur, db_path=db_path) is not None:
        return refus(E_CLE_EXISTANTE, cle_valeur)

    return inserer(table, valeurs, action="CREATION", acteur=acteur, db_path=db_path)


def modifier_ligne(table: str, cle_valeur: str, valeurs: dict[str, Any], *, acteur: str = "",
                   db_path=None) -> dict[str, Any]:
    """Édition depuis l'administration. La CLÉ n'est jamais modifiable : la changer casserait
    silencieusement toutes les références qui la pointent."""
    meta = decrire_table(table, db_path=db_path)
    if not meta.get("ok"):
        return refus("TABLE_INCONNUE", table)
    if meta["lecture_seule"]:
        return refus(E_ECRITURE, meta["motif_lecture_seule"])

    champs = {c: v for c, v in valeurs.items() if c in meta["colonnes"] and c != meta["cle"]}
    if not champs:
        return {"ok": True, "inchange": True}
    return mettre_a_jour(table, cle_valeur, champs, action="MODIFICATION", acteur=acteur,
                         db_path=db_path)


def basculer_activation(table: str, cle_valeur: str, actif: bool, *, acteur: str = "",
                        db_path=None) -> dict[str, Any]:
    """Active/désactive une ligne. JAMAIS de suppression physique : une donnée déjà référencée
    ailleurs doit rester lisible, sinon les objets qui la citent deviennent orphelins."""
    meta = decrire_table(table, db_path=db_path)
    if not meta.get("ok"):
        return refus("TABLE_INCONNUE", table)
    if not meta["a_colonne_actif"]:
        return refus(E_ECRITURE, f"{table} n'a pas de colonne d'activation")
    return mettre_a_jour(table, cle_valeur, {COLONNE_ACTIF: "OUI" if actif else "NON"},
                         action="ACTIVATION" if actif else "DESACTIVATION",
                         acteur=acteur, db_path=db_path)

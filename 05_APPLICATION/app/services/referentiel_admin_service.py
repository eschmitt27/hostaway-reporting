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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from app.db.connection import get_db
from app.services import ref_setup_repo as repo

SOURCE_APPLICATION = "SAISIE_APPLICATION"


@dataclass(frozen=True)
class TableNative:
    onglet: str
    cle: str
    colonnes: tuple[str, ...]


#: Tables SQLite natives, historisées, qui réutilisent le CRUD/historisation générique de ce
#: module mais n'existent PAS dans le classeur REF_Setup — jamais dans `ref_setup_catalogue.
#: FEUILLES`, sinon `ref_setup_import_service._lire_classeur` échouerait en cherchant un onglet
#: Excel qui n'existe pas (`E_ONGLET_MANQUANT`). Mission 6 : premier exemple, paramètres canapé.
TABLES_NATIVES: dict[str, TableNative] = {
    "ref_canape_parametres": TableNative(
        onglet="REF_Canape_Parametres",
        cle="canape_parametre_id",
        colonnes=("canape_parametre_id", "logement_id",
                  "seuil_voyageurs_preparation_canape", "montant_preparation_canape",
                  "date_debut", "date_fin", "actif", "commentaire"),
    ),
    "ref_regles_versions": TableNative(
        onglet="REF_Regles_Versions",
        cle="regle_version_id",
        colonnes=("regle_version_id", "rule_code", "version",
                  "date_debut", "date_fin", "actif", "parametres", "commentaire"),
    ),
}

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
E_JUSTIFICATION_REQUISE = "V11_JUSTIFICATION_REQUISE"

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
    E_JUSTIFICATION_REQUISE: (
        "Cette modification concerne une période passée (correction rétroactive) : une "
        "justification est obligatoire."),
}


def est_retroactif(date_reference: str, *, aujourdhui: date | None = None) -> bool:
    """Une date d'effet est rétroactive si elle n'est pas dans le futur (Mission 6 quater §4) —
    « aujourd'hui » compris comme rétroactif : la période concernée a déjà commencé ou commence
    aujourd'hui, jamais purement à venir. `aujourdhui` est injectable pour les tests, jamais figé
    par défaut (résolu à l'appel, comme partout ailleurs dans ce module)."""
    if not date_valide(date_reference):
        return False
    return date.fromisoformat(txt(date_reference)) <= (aujourdhui or date.today())


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


def verifier_justification_retroactive(date_reference: str, justification: str) -> dict[str, Any] | None:
    """Contrôle BACKEND (pas seulement HTML) de la justification obligatoire pour une correction
    rétroactive (Mission 6 quater §5) : `None` si le changement est futur, ou rétroactif avec une
    justification non vide ; un `refus(E_JUSTIFICATION_REQUISE)` sinon. À appeler dans la route,
    AVANT tout appel au service d'écriture — un champ vide ne doit jamais atteindre la base."""
    if est_retroactif(date_reference) and not txt(justification):
        return refus(E_JUSTIFICATION_REQUISE)
    return None


def invalider_dag_referentiel(*, db_path=None) -> list[str]:
    """Marque obsolètes (`A_RECALCULER`) les datasets aval du référentiel via le DAG EXISTANT
    (`orchestrateur_dag`/`orchestrateur_service`, aucune deuxième carte de dépendances) — Mission
    6 ter §24/§25. Ne recalcule jamais rien elle-même : seule une actualisation explicite (bouton
    « Actualiser maintenant » ou ciblée) via l'orchestrateur relance un vrai calcul. Appelée APRÈS
    le commit de la transaction référentielle (écrit dans une autre table, `orchestrateur_datasets`
    — un échec ici ne remet jamais en cause l'écriture référentielle déjà actée)."""
    from app.services import orchestrateur_dag as dag
    from app.services import orchestrateur_service as orch
    return orch.invalider_descendants(dag.REF_SETUP, db_path=db_path)


class RefusTransaction(Exception):
    """Lève l'échec d'une étape à l'intérieur d'une `transaction()` pour déclencher son rollback.

    Porte le dict `refus(...)` original : l'appelant le récupère via `exc.refus` après le `with`,
    au lieu de traduire une exception générique en un nouveau code d'erreur.
    """

    def __init__(self, refus: dict[str, Any]) -> None:
        super().__init__(refus.get("message", refus.get("code", "refus")))
        self.refus = refus


@contextmanager
def transaction(*, db_path=None):
    """Une connexion SQLite partagée par plusieurs écritures de ce module, committée en un seul bloc.

    Sert exactement le cas visé par la mission : « clôturer une période + en ouvrir une nouvelle +
    journaliser » doit être atomique — si l'ouverture échoue, la clôture déjà faite ne doit jamais
    rester seule committée. `inserer`/`mettre_a_jour`/`clore_periode` acceptent un `conn=` : passé,
    ils écrivent dessus sans committer ni fermer — c'est CE bloc qui décide du commit final.
    """
    conn = get_db(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def disponible(*, db_path=None) -> bool:
    """Le référentiel est-il exploitable ? Aucun repli sur le classeur (§18)."""
    return repo.est_disponible(db_path=db_path)


# ── Lecture (pour valider avant d'écrire) ───────────────────────────────────────────────────────

def _lire_table_native(table: str, native: TableNative, *, conn=None,
                       db_path=None) -> list[dict[str, str]]:
    """Équivalent de `ref_setup_repo.lire_table`, mais sans passer par le catalogue Excel — cette
    table n'a pas d'onglet, donc pas de `Feuille` dans `ref_setup_catalogue`."""
    c = conn if conn is not None else get_db(db_path)
    try:
        cols = ", ".join(native.colonnes)
        rows = c.execute(f"SELECT {cols} FROM {table}").fetchall()
        return [dict(zip(native.colonnes, [("" if v is None else str(v)) for v in r]))
                for r in rows]
    finally:
        if conn is None:
            c.close()


def lignes(table: str, *, conn=None, db_path=None) -> list[dict[str, str]]:
    native = TABLES_NATIVES.get(table)
    if native is not None:
        return _lire_table_native(table, native, conn=conn, db_path=db_path)
    return repo.lire_table(table, conn=conn, db_path=db_path)


def ligne(table: str, cle_valeur: str, *, conn=None, db_path=None) -> dict[str, str] | None:
    native = TABLES_NATIVES.get(table)
    if native is not None:
        cv = txt(cle_valeur)
        return next((r for r in _lire_table_native(table, native, conn=conn, db_path=db_path)
                    if txt(r.get(native.cle)) == cv), None)
    return repo.lire_par_cle(table, txt(cle_valeur), conn=conn, db_path=db_path)


#: Référentiels à périodes : une ligne sans colonne de fin est la ligne COURANTE. Le grain diffère
#: selon la table (logement pour la gestion/le taux, TYPE de logement pour le coût ménage — c'est
#: `lot6f_cout_complet_menages.py::date_aware` qui résout déjà par type + date, inchangé ici) ; les
#: noms de colonnes de période diffèrent aussi (`date_debut`/`date_fin` vs `*_validite`).
PERIODES: dict[str, dict[str, str]] = {
    "ref_gestion_logements_hist": {
        "grain": "logement_id", "debut": "date_debut", "fin": "date_fin"},
    "ref_taux_commission": {
        "grain": "logement_id", "debut": "date_debut", "fin": "date_fin"},
    "ref_couts_standards_menage": {
        "grain": "type_logement_id", "debut": "date_debut_validite", "fin": "date_fin_validite"},
    "ref_canape_parametres": {
        "grain": "logement_id", "debut": "date_debut", "fin": "date_fin"},
    "ref_regles_versions": {
        "grain": "rule_code", "debut": "date_debut", "fin": "date_fin"},
}

#: Tout le moteur (`lib_ref_history.resolve_management_period`, `resolve_commission_rate`,
#: `lot6f_cout_complet_menages.date_aware`) repose sur cet invariant : une ligne sans date de fin
#: est la ligne courante.
TABLES_HISTORISEES = tuple(PERIODES)


def periodes_ouvertes(table: str, grain_valeur: str, *, conn=None, db_path=None) -> list[dict[str, str]]:
    """Toutes les lignes historisées encore ouvertes (colonne de fin vide) pour ce grain.

    Renvoie une liste, et non une ligne, parce qu'un référentiel PEUT être ambigu : l'application
    s'interdit de créer ce cas (voir `inserer`), mais un classeur importé ou une correction faite
    directement en base peuvent l'introduire. Le rendre irreprésentable en base a été essayé puis
    écarté — voir la note de la migration 0051.
    """
    spec = PERIODES[table]
    gv = txt(grain_valeur)
    return [r for r in lignes(table, conn=conn, db_path=db_path)
            if txt(r.get(spec["grain"])) == gv and not txt(r.get(spec["fin"]))]


def periode_ouverte(table: str, grain_valeur: str, *, conn=None, db_path=None) -> dict[str, str] | None:
    """Période courante de ce grain, ou None s'il n'y en a pas.

    En cas d'ambiguïté, renvoie la dernière : les APPELANTS EN ÉCRITURE veulent alors clore ce qui
    traîne. Les appelants en LECTURE qui doivent refuser de deviner (`logements_service`, qui rend
    `A_CONTROLER`) passent par `periodes_ouvertes` et comptent eux-mêmes.
    """
    ouvertes = periodes_ouvertes(table, grain_valeur, conn=conn, db_path=db_path)
    return ouvertes[-1] if ouvertes else None


# ── Écriture ────────────────────────────────────────────────────────────────────────────────────

def _colonnes(table: str) -> tuple[str, ...]:
    native = TABLES_NATIVES.get(table)
    if native is not None:
        return native.colonnes
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
            commentaire: str = "", conn=None, db_path=None) -> dict[str, Any]:
    """Insère une ligne de référentiel saisie dans l'application, et la journalise.

    `conn`, si fourni, est réutilisé sans commit ni fermeture (voir `transaction()`) : l'appelant
    décide seul du commit final, pour que « clôturer puis ouvrir » ne puisse pas laisser la base à
    mi-chemin. Sans `conn`, le comportement autonome d'origine (ouvre/committe/ferme) est inchangé.
    """
    colonnes = _colonnes(table)
    ligne_complete = {c: txt(valeurs.get(c)) for c in colonnes}

    if table in TABLES_HISTORISEES and not txt(ligne_complete.get(PERIODES[table]["fin"])):
        spec = PERIODES[table]
        gv = txt(ligne_complete.get(spec["grain"]))
        # Deux périodes ouvertes pour un même grain rendent la période courante indécidable : le
        # moteur choisirait une ligne au hasard. On ouvre donc UNIQUEMENT après avoir clos ce qui
        # précède (`clore_periode`), jamais en parallèle.
        if gv and periodes_ouvertes(table, gv, conn=conn, db_path=db_path):
            return refus(E_PERIODE_INCOHERENTE,
                         f"{gv} a déjà une période ouverte dans {table} ; il faut la clore avant "
                         "d'en ouvrir une nouvelle")
        # Chevauchement avec une période déjà CLOSE : une saisie manuelle d'une date antérieure à
        # la fin d'une période passée rendrait deux lignes actives sur le même intervalle. Les
        # clôtures normales (`clore_periode` puis `inserer` à la date suivante) sont toujours
        # strictement croissantes et ne déclenchent jamais ce refus.
        debut = txt(ligne_complete.get(spec["debut"]))
        if gv and debut and date_valide(debut):
            closes = [r for r in lignes(table, conn=conn, db_path=db_path)
                      if txt(r.get(spec["grain"])) == gv and txt(r.get(spec["fin"]))]
            for c in closes:
                fin_close = txt(c.get(spec["fin"]))
                if date_valide(fin_close) \
                        and date.fromisoformat(debut) <= date.fromisoformat(fin_close):
                    return refus(E_PERIODE_INCOHERENTE,
                                 f"{gv} : la période à ouvrir ({debut}) chevauche une période "
                                 f"déjà close se terminant le {fin_close}")

    proprio = conn is None
    c = conn if conn is not None else get_db(db_path)
    try:
        cols = ", ".join((*colonnes, "import_id"))
        trous = ", ".join(["?"] * (len(colonnes) + 1))
        c.execute(f"INSERT INTO {table} ({cols}) VALUES ({trous})",
                  tuple(ligne_complete[c2] for c2 in colonnes) + (SOURCE_APPLICATION,))
        journaliser(c, table, ligne_complete.get(_cle(table), ""), action, None,
                    ligne_complete, acteur, commentaire)
        if proprio:
            c.commit()
    except Exception as exc:   # noqa: BLE001
        if proprio:
            c.rollback()
            return refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if proprio:
            c.close()
    return {"ok": True, "ligne": ligne_complete}


def mettre_a_jour(table: str, cle_valeur: str, champs: dict[str, Any], *, action: str,
                  acteur: str = "", commentaire: str = "", conn=None, db_path=None) -> dict[str, Any]:
    """Met à jour des champs d'une ligne identifiée par sa clé métier, et journalise l'avant/après.

    Une ligne modifiée dans l'application devient une ligne applicative (`import_id`) : un réimport
    du classeur ne doit pas la réécraser silencieusement. `conn` : voir `inserer`.
    """
    cle = _cle(table)
    colonnes = set(_colonnes(table))
    inconnues = [c for c in champs if c not in colonnes]
    if inconnues:
        return refus(E_ECRITURE, f"colonnes inconnues : {inconnues}")

    avant = ligne(table, cle_valeur, conn=conn, db_path=db_path)
    if avant is None:
        return refus(E_INTROUVABLE, txt(cle_valeur))

    apres = {**avant, **{c: txt(v) for c, v in champs.items()}}
    proprio = conn is None
    c = conn if conn is not None else get_db(db_path)
    try:
        assignations = ", ".join(f"{c2} = ?" for c2 in champs) + ", import_id = ?"
        c.execute(f"UPDATE {table} SET {assignations} WHERE {cle} = ?",
                  tuple(txt(v) for v in champs.values()) + (SOURCE_APPLICATION, txt(cle_valeur)))
        journaliser(c, table, cle_valeur, action, avant, apres, acteur, commentaire)
        if proprio:
            c.commit()
    except Exception as exc:   # noqa: BLE001
        if proprio:
            c.rollback()
            return refus(E_ECRITURE, f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if proprio:
            c.close()
    return {"ok": True, "avant": avant, "apres": apres}


def clore_periode(table: str, grain_valeur: str, date_fin: str, *, statut: str = "",
                  acteur: str = "", commentaire: str = "", action: str = "CLOTURE_PERIODE",
                  conn=None, db_path=None) -> dict[str, Any]:
    """Clôt la période ouverte de ce grain. Ne touche JAMAIS une période déjà close.

    Refuse une `date_fin` antérieure au début de la période courante : une période négative
    rendrait la résolution datée incohérente au lieu de la corriger. `conn` : voir `inserer`.
    `commentaire` porte la justification d'une correction rétroactive (Mission 6 quater) ; `action`
    permet à l'appelant de journaliser `CORRECTION_RETROACTIVE` au lieu du `CLOTURE_PERIODE`
    générique quand c'en est une — la décision reste celle de l'appelant, qui connaît la date
    réellement demandée par l'utilisateur.
    """
    spec = PERIODES[table]
    ouverte = periode_ouverte(table, grain_valeur, conn=conn, db_path=db_path)
    if ouverte is None:
        return {"ok": True, "cloturee": False}

    debut = txt(ouverte.get(spec["debut"]))
    if debut and date_valide(debut) and date_valide(date_fin) \
            and date.fromisoformat(txt(date_fin)) < date.fromisoformat(debut):
        return refus(E_PERIODE_INCOHERENTE, f"{debut} → {date_fin}")

    cle = _cle(table)
    champs: dict[str, Any] = {spec["fin"]: txt(date_fin)}
    if statut and "statut_gestion" in _colonnes(table):
        champs["statut_gestion"] = statut
    res = mettre_a_jour(table, ouverte.get(cle, ""), champs, action=action, acteur=acteur,
                        commentaire=commentaire, conn=conn, db_path=db_path)
    if not res.get("ok"):
        return res
    return {"ok": True, "cloturee": True, "ligne": res["apres"]}


def _cle(table: str) -> str:
    native = TABLES_NATIVES.get(table)
    if native is not None:
        return native.cle
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
                   "ref_charges_recurrentes", "ref_abonnements_logiciels",
                   "ref_canape_parametres"),
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
    {
        "cle": "regles_versionnees",
        "titre": "Règles versionnées",
        "description": (
            "Règles ALGORITHMIQUES (pas de simples variables) — assiette de commission, "
            "répartition des charges communes de facture — versionnées par date. "
            "L'implémentation reste dans le code ; ceci dit seulement quelle version s'applique "
            "à quelle date."),
        "tables": ("ref_regles_versions",),
    },
)

# Tables dont la modification passe par un parcours métier dédié (voir ci-dessus).
LECTURE_SEULE = {
    "ref_gestion_logements_hist": "Fiche logement → changement de propriétaire / archivage",
    "ref_taux_commission": "Fiche logement → changement de taux de commission",
    "ref_couts_standards_menage": "Écran coûts ménage → changement de coût standard",
    "ref_canape_parametres": "Écran paramètres canapé → changement de seuil/montant",
    "ref_regles_versions": "Écran règles versionnées → nouvelle version d'une règle",
}

# Colonnes exclues de l'édition libre bien que leur TABLE reste administrable — la donnée vit
# aussi (et fait foi pour le calcul) dans une table historisée dédiée. `ref_logements` reste
# éditable pour ses champs descriptifs ; seules ces deux colonnes, désormais vestigiales, passent
# par `canape_gestion_service.changer_parametres` (Mission 6).
COLONNES_LECTURE_SEULE: dict[str, set[str]] = {
    "ref_logements": {"seuil_voyageurs_preparation_canape", "montant_preparation_canape"},
}

# Colonne portant l'activation, quand la table en a une.
COLONNE_ACTIF = "actif"


def _compter_native(table: str, *, db_path=None) -> int:
    conn = get_db(db_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def categories(*, db_path=None) -> list[dict[str, Any]]:
    """Les catégories, enrichies du libellé et de la volumétrie de chaque table."""
    from app.services import ref_setup_catalogue as cat

    dispo = disponible(db_path=db_path)
    compte = repo.compter(db_path=db_path) if dispo else {}
    out = []
    for c in CATEGORIES:
        tables = []
        for t in c["tables"]:
            native = TABLES_NATIVES.get(t)
            if native is not None:
                onglet, cle_t = native.onglet, native.cle
                nb_lignes = _compter_native(t, db_path=db_path) if dispo else 0
            else:
                feuille = cat.PAR_TABLE.get(t)
                if feuille is None:
                    continue
                onglet, cle_t = feuille.onglet, feuille.cle
                nb_lignes = compte.get(t, 0)
            tables.append({
                "table": t,
                "onglet": onglet,
                "libelle": onglet.replace("REF_", "").replace("_", " "),
                "cle": cle_t,
                "nb_lignes": nb_lignes,
                "lecture_seule": t in LECTURE_SEULE,
                "motif_lecture_seule": LECTURE_SEULE.get(t, ""),
            })
        out.append({**c, "tables": tables})
    return out


def decrire_table(table: str, *, db_path=None) -> dict[str, Any]:
    """Métadonnées d'une table pour l'écran de liste/édition."""
    native = TABLES_NATIVES.get(table)
    if native is not None:
        return {
            "ok": True,
            "table": table,
            "onglet": native.onglet,
            "cle": native.cle,
            "colonnes": list(native.colonnes),
            "lecture_seule": table in LECTURE_SEULE,
            "motif_lecture_seule": LECTURE_SEULE.get(table, ""),
            "a_colonne_actif": COLONNE_ACTIF in native.colonnes,
            "colonnes_lecture_seule": sorted(COLONNES_LECTURE_SEULE.get(table, set())),
        }

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
        "colonnes_lecture_seule": sorted(COLONNES_LECTURE_SEULE.get(table, set())),
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

    verrouillees = COLONNES_LECTURE_SEULE.get(table, set())
    if verrouillees:
        valeurs = {c: v for c, v in valeurs.items() if c not in verrouillees}
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

    verrouillees = COLONNES_LECTURE_SEULE.get(table, set())
    champs = {c: v for c, v in valeurs.items()
             if c in meta["colonnes"] and c != meta["cle"] and c not in verrouillees}
    if not champs:
        return {"ok": True, "inchange": True}
    return mettre_a_jour(table, cle_valeur, champs, action="MODIFICATION", acteur=acteur,
                         db_path=db_path)


E_PROPRIETAIRE_REFERENCE = "V10_PROPRIETAIRE_LOGEMENT_ACTIF"
MESSAGES[E_PROPRIETAIRE_REFERENCE] = (
    "Ce propriétaire gère encore au moins un logement actif : changez son propriétaire ou "
    "archivez le logement avant de désactiver ce propriétaire.")


def basculer_activation(table: str, cle_valeur: str, actif: bool, *, acteur: str = "",
                        db_path=None) -> dict[str, Any]:
    """Active/désactive une ligne. JAMAIS de suppression physique : une donnée déjà référencée
    ailleurs doit rester lisible, sinon les objets qui la citent deviennent orphelins.

    Désactiver un propriétaire encore rattaché à un logement actif laisserait ce logement sans
    propriétaire exploitable par le moteur (`resolve_management_period` continuerait de le
    résoudre vers un propriétaire désactivé) : refusé tant que le rattachement n'a pas été fermé.
    """
    meta = decrire_table(table, db_path=db_path)
    if not meta.get("ok"):
        return refus("TABLE_INCONNUE", table)
    if not meta["a_colonne_actif"]:
        return refus(E_ECRITURE, f"{table} n'a pas de colonne d'activation")

    if table == TABLE_PROPRIETAIRES and not actif:
        pid = txt(cle_valeur)
        rattaches = [r for r in lignes(TABLE_GESTION, db_path=db_path)
                     if txt(r.get("proprietaire_id")) == pid and not txt(r.get("date_fin"))]
        if rattaches:
            logs = ", ".join(sorted({txt(r.get("logement_id")) for r in rattaches}))
            return refus(E_PROPRIETAIRE_REFERENCE, logs)

    return mettre_a_jour(table, cle_valeur, {COLONNE_ACTIF: "OUI" if actif else "NON"},
                         action="ACTIVATION" if actif else "DESACTIVATION",
                         acteur=acteur, db_path=db_path)

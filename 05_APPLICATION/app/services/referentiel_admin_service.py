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
from app.services import ref_setup_catalogue as _cat
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


#: Séparateurs de ligne, nommés plutôt qu'échappés au fil des expressions.
SAUT = chr(10)
RETOUR_CHARIOT = chr(13)


def txt(v: Any) -> str:
    """Valeur de référentiel → texte stable. Entonnoir UNIQUE de toutes les écritures.

    LES SAUTS DE LIGNE INTERNES SONT CONSERVÉS, et ramenés à `\\n`. Une adresse se saisit
    couramment sur trois lignes — voie, complément, code postal et ville — et cette forme est une
    donnée, pas une décoration : l'aplatir changerait ce que le propriétaire a écrit.

    La normalisation n'est pas cosmétique. Un navigateur renvoie le contenu d'un `<textarea>` avec
    des fins de ligne `\\r\\n` (la norme HTML l'impose), alors que la valeur lue en base porte des
    `\\n`. Sans cette conversion, ouvrir une fiche et la réenregistrer SANS RIEN TOUCHER modifierait
    la valeur — et le journal enregistrerait une modification fantôme à chaque passage.
    """
    return str(v or "").replace(RETOUR_CHARIOT + SAUT, SAUT).replace(RETOUR_CHARIOT, SAUT).strip()


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

# ── §81-91 — CLASSIFICATION DES RÉFÉRENTIELS : quatre classes, une seule par table ──────────────
#
# L'écran n'en distinguait que deux : « Administrable » et « Parcours dédié ». Deux situations
# pourtant très différentes se retrouvaient donc côte à côte dans « administrable » : les données
# que l'exploitant possède vraiment (ses logements, ses propriétaires) et les NOMENCLATURES dont le
# moteur lit les identifiants en dur. Renommer une catégorie de charge est inoffensif ; en
# supprimer une, ou en ajouter une que le moteur ne connaît pas, casse un calcul sans un mot.
#
# La classe n'est pas une opinion : elle se déduit de ce que le code fait réellement de la table.

EDITABLE = "EDITABLE"
READ_ONLY = "READ_ONLY"
DEDICATED_WORKFLOW = "DEDICATED_WORKFLOW"
HIDDEN_TECHNICAL = "HIDDEN_TECHNICAL"
CLASSES = (EDITABLE, READ_ONLY, DEDICATED_WORKFLOW, HIDDEN_TECHNICAL)

LIBELLES_CLASSE = {
    EDITABLE: "Administrable",
    READ_ONLY: "Consultation seule",
    DEDICATED_WORKFLOW: "Parcours dédié",
    HIDDEN_TECHNICAL: "Technique",
}

#: Ce que chaque classe autorise. `editable` gouverne l'UI ET le service : une table non éditable
#: refuse l'écriture, quelle que soit la route employée.
DROITS_CLASSE = {
    EDITABLE: {"editable": True, "visible": True},
    READ_ONLY: {"editable": False, "visible": True},
    DEDICATED_WORKFLOW: {"editable": False, "visible": True},
    # « Technique » n'est pas « secret » : la table reste atteignable en dépliant les référentiels
    # techniques. Elle sort seulement de la liste courante, où elle n'apprend rien à personne.
    HIDDEN_TECHNICAL: {"editable": False, "visible": False},
}

#: Table → (classe, motif). Le motif est affiché tel quel : il doit dire où aller, ou pourquoi non.
CLASSIFICATION: dict[str, tuple[str, str]] = {
    # ── Parcours dédié : consultable ici, modifié là où la règle est appliquée ──────────────────
    "ref_gestion_logements_hist": (
        DEDICATED_WORKFLOW, "Fiche logement → changement de propriétaire / archivage"),
    "ref_taux_commission": (
        DEDICATED_WORKFLOW, "Fiche logement → changement de taux de commission"),
    "ref_couts_standards_menage": (
        DEDICATED_WORKFLOW, "Écran coûts ménage → changement de coût standard"),
    "ref_canape_parametres": (
        DEDICATED_WORKFLOW, "Écran paramètres canapé → changement de seuil/montant"),
    "ref_regles_versions": (
        DEDICATED_WORKFLOW, "Écran règles versionnées → nouvelle version d'une règle"),
    # La clôture est un ACTE tracé, avec sa date et son auteur. Basculer `statut_mois` à la main
    # ici rouvrirait ou fermerait une période sans rien de tout cela.
    "ref_cloture_mensuelle": (
        DEDICATED_WORKFLOW, "Écran Clôture mensuelle → ouverture / clôture d'une période"),
    # CORRESPONDANCES LOGEMENT — décision utilisateur, §18.
    #
    # Classée EDITABLE au départ, et c'était une erreur d'appréciation : l'exploitant DOIT pouvoir
    # corriger une correspondance — c'est même une des opérations les plus fréquentes — mais une
    # ligne de cette table n'est pas une donnée qu'on saisit, c'est une DÉCISION qu'on prend sur un
    # libellé venu de l'extérieur. Éditer la ligne brute demande de connaître `source`,
    # `champ_source`, `valeur_source` et l'identifiant technique du logement, et n'enregistre ni
    # qui a tranché ni contre quelle proposition.
    #
    # Le parcours « Corriger une correspondance logement » pose la seule question qui compte —
    # « ce libellé, c'est quel logement ? » — montre la correspondance actuelle et celle que le
    # moteur propose, et trace le choix.
    "ref_mapping_logements": (
        DEDICATED_WORKFLOW, "Écran Correspondances logement → corriger / déclarer une "
                            "correspondance"),

    # ── Consultation seule : le MOTEUR lit ces identifiants en dur ──────────────────────────────
    # Les compter n'est pas une intuition : `TYPE_FLUX_0…` apparaît 49 fois dans `app/`,
    # `CHG_0…` 35 fois, `PAY_00…` 22 fois, les codes d'impact 10 fois. Supprimer ou renuméroter
    # une de ces lignes casserait un calcul en silence.
    "ref_types_flux": (
        READ_ONLY, "Nomenclature du moteur : les règles de flux citent ces identifiants"),
    "ref_codes_impact": (
        READ_ONLY, "Nomenclature du moteur : l'impact comptable est décidé sur ces codes"),
    "ref_categories_charges": (
        READ_ONLY, "Nomenclature du moteur : le catalogue des charges porte les règles par code"),
    "ref_modes_paiement": (
        READ_ONLY, "Nomenclature du moteur : les règles de saisie citent ces modes"),
    "ref_types_affectation": (
        READ_ONLY, "Nomenclature du moteur : l'axe d'affectation des charges"),
    "ref_types_lignes_menage": (
        READ_ONLY, "Nomenclature importée du classeur ; aucun écran ne la modifie"),
    "ref_statuts_payout": (
        READ_ONLY, "Nomenclature importée du classeur ; aucun écran ne la modifie"),
    "ref_parametres_generaux": (
        READ_ONLY, "Paramètres du classeur d'origine ; la configuration vit dans l'environnement"),

    # ── Technique : de la plomberie, sans signification pour l'exploitant ───────────────────────
    "ref_assoc_mode": (
        HIDDEN_TECHNICAL, "Table de correspondance préparée par une migration, lue par le moteur"),
    "ref_sources_systeme": (
        HIDDEN_TECHNICAL, "Registre des sources techniques (modules, fichiers), pas une donnée métier"),
    "ref_statuts": (
        HIDDEN_TECHNICAL, "Vocabulaire interne des statuts d'import, sans usage direct à l'écran"),
}


def classe(table: str) -> str:
    """Classe d'un référentiel. Par défaut EDITABLE : c'est une donnée que l'exploitant possède.

    Le défaut est délibérément permissif — une table oubliée dans la classification reste
    administrable, et non muette. Verrouiller par oubli serait le pire des deux comportements.
    """
    return CLASSIFICATION.get(table, (EDITABLE, ""))[0]


def motif_classe(table: str) -> str:
    return CLASSIFICATION.get(table, (EDITABLE, ""))[1]


#: Table → adresse du parcours qui la modifie, quand ce parcours est UN écran identifiable.
#
# Dire « allez dans la fiche logement » sans y conduire laisse l'utilisateur chercher. Les tables
# absentes de cette table le sont à dessein : leur parcours n'a pas d'adresse unique (le taux de
# commission se change depuis LA fiche du logement concerné, pas depuis un écran général).
URLS_PARCOURS: dict[str, str] = {
    "ref_mapping_logements": "/correspondances-logement",
    "ref_cloture_mensuelle": "/clotures",
    # Le changement de coût se fait sur la fiche du référentiel elle-même (formulaire daté).
    "ref_couts_standards_menage": "/administration/referentiels/ref_couts_standards_menage",
}


def url_parcours(table: str) -> str:
    """Adresse du parcours dédié, ou chaîne vide s'il n'en existe pas une seule."""
    return URLS_PARCOURS.get(table, "") if classe(table) == DEDICATED_WORKFLOW else ""


def est_editable(table: str) -> bool:
    return DROITS_CLASSE[classe(table)]["editable"]


def est_visible(table: str) -> bool:
    return DROITS_CLASSE[classe(table)]["visible"]


# Compatibilité : tout le service raisonnait sur `LECTURE_SEULE`. La table est désormais DÉRIVÉE
# de la classification, pour qu'il n'existe qu'une seule source de vérité.
LECTURE_SEULE = {t: motif for t, (c, motif) in CLASSIFICATION.items()
                 if not DROITS_CLASSE[c]["editable"]}

# Colonnes exclues de l'édition libre bien que leur TABLE reste administrable — la donnée vit
# aussi (et fait foi pour le calcul) dans une table historisée dédiée. `ref_logements` reste
# éditable pour ses champs descriptifs ; seules ces deux colonnes, désormais vestigiales, passent
# par `canape_gestion_service.changer_parametres` (Mission 6).
COLONNES_LECTURE_SEULE: dict[str, set[str]] = {
    # `dynamic_pricing` (valeur brute de la source) et les deux colonnes qui en DÉRIVENT par
    # déclencheur (migration 0087). L'écran modifie la paire lisible — « Pricing dynamique :
    # Oui/Non » et « Moteur » — et le service recompose la valeur brute. Laisser éditer les trois
    # séparément permettrait de les rendre contradictoires en trois clics.
    "ref_logements": {"seuil_voyageurs_preparation_canape", "montant_preparation_canape",
                      "dynamic_pricing"},
}

# Colonnes DÉRIVÉES : affichées, jamais écrites directement — une écriture les recalculerait
# aussitôt, donc l'utilisateur verrait sa saisie disparaître sans explication.
#
# La liste n'est pas réécrite ici : elle est LUE du catalogue, qui la déclare déjà pour le contrôle
# d'alignement catalogue ↔ schéma. Deux listes finiraient par diverger, et c'est exactement l'écart
# qu'aucun des deux contrôles ne verrait.
COLONNES_DERIVEES: dict[str, set[str]] = {
    table: set(colonnes) for table, colonnes in _cat.COLONNES_DERIVEES.items()
}

#: Libellés de la question posée à l'écran, là où le nom de colonne ne la pose pas.
PRICING_DESACTIVE = "non"


def composer_dynamic_pricing(active: Any, fournisseur: Any) -> str:
    """Recompose la valeur brute `dynamic_pricing` à partir de la paire lisible (§19).

    Bijection stricte avec la dérivation du déclencheur 0087 :
        (NON, quoi que ce soit) → « non »
        (OUI, « hostdynamic »)  → « hostdynamic »
    Un « oui » sans moteur nommé reste « oui » : c'est une information incomplète, pas une
    invention. Lui attribuer d'office « hostdynamic » affirmerait un fournisseur que personne n'a
    désigné — et ferait passer pour constaté ce qui n'est que le cas le plus fréquent.
    """
    actif = txt(active).upper() in ("OUI", "1", "TRUE", "ON")
    if not actif:
        return PRICING_DESACTIVE
    return txt(fournisseur) or "oui"

# Colonne portant l'activation, quand la table en a une.
COLONNE_ACTIF = "actif"


# ── §82-91 — LES CHAMPS SE CHOISISSENT, ILS NE SE RÉCITENT PAS ──────────────────────────────────
#
# L'éditeur générique rendait CHAQUE colonne en zone de texte libre. Conséquence : pour dire qu'un
# logement est actif, il fallait savoir qu'on écrit « OUI » et non « oui », « Oui » ou « true » ; et
# pour le rattacher à un type, connaître « TYPE_003 » de mémoire. Une faute de frappe ne produisait
# aucun refus — juste une valeur que plus aucun filtre ne retrouvait.

OUI_NON = ("OUI", "NON")

#: Table → colonne → valeurs canoniques. Les valeurs RÉELLEMENT présentes en base sont ajoutées à
#: l'exécution (`options_champ`) : une donnée existante hors liste reste sélectionnable au lieu
#: d'être silencieusement remplacée à la première modification de la ligne.
CHAMPS_ENUM: dict[str, dict[str, tuple[str, ...]]] = {
    "ref_logements": {
        "sur_hostaway": OUI_NON,
        "actif": OUI_NON,
        "statut_parc": ("GERE", "RETIRE", "HORS_PARC_TECHNIQUE"),
        # `dynamic_pricing` est la valeur BRUTE de la source : « hostdynamic » ou « non ». Elle
        # mélangeait deux questions — activé ou non, et par quel moteur. La migration 0087 les
        # sépare en `dynamic_pricing_enabled` / `dynamic_pricing_provider`, dérivées par
        # déclencheur. La liste reste ouverte sur ce qui existe, pour qu'une valeur en base ne
        # soit jamais remplacée en silence.
        "dynamic_pricing": ("non",),
        "dynamic_pricing_enabled": OUI_NON,
    },
    "ref_proprietaires": {"actif": OUI_NON, "mode_facturation": ("PAR_LOGEMENT",)},
    "ref_intervenants": {"actif": OUI_NON, "type_intervenant": ("INTERNE", "EXTERNE")},
    "ref_mapping_logements": {"actif": OUI_NON, "niveau_confiance": ("Fort", "Moyen", "Faible")},
    "ref_associes": {"actif": OUI_NON},
    "ref_canaux_reservation": {"dans_hostaway": OUI_NON},
    "ref_banque_regles": {"actif": OUI_NON},
    "ref_charges_recurrentes": {"actif": OUI_NON},
    "ref_abonnements_logiciels": {"actif": OUI_NON},
    "ref_couts_menage_interne": {"actif": OUI_NON},
    "ref_taux_heures_menage": {"actif": OUI_NON},
}

#: Table → colonne → table référencée. La liste vient du RÉFÉRENTIEL, avec les libellés humains :
#: on choisit « Studio » et non « TYPE_001 ».
CHAMPS_REFERENCE: dict[str, dict[str, str]] = {
    "ref_logements": {"type_logement_id": "ref_types_logements"},
    "ref_mapping_logements": {"logement_id": "ref_logements"},
    "ref_canape_parametres": {"logement_id": "ref_logements"},
    "ref_couts_menage_interne": {"type_logement_id": "ref_types_logements"},
    "ref_taux_heures_menage": {"intervenant_id": "ref_intervenants"},
    "ref_gestion_logements_hist": {"logement_id": "ref_logements",
                                   "proprietaire_id": "ref_proprietaires"},
    "ref_taux_commission": {"logement_id": "ref_logements",
                            "proprietaire_id": "ref_proprietaires"},
    "ref_couts_standards_menage": {"type_logement_id": "ref_types_logements"},
    "ref_cartes_paiement": {"personne_id": "ref_associes"},
}

#: Colonnes portant une DATE : sélecteur de calendrier, jamais du texte libre (§40, §76).
def _est_colonne_date(colonne: str) -> bool:
    return colonne.startswith("date_") or colonne.endswith("_date")


#: Tables dont l'identifiant suit une séquence `PREFIXE_NNNN` et se dérive donc tout seul.
CLES_AUTOMATIQUES: dict[str, tuple[str, int]] = {
    "ref_proprietaires": ("PROP_", 4),
    "ref_logements": ("LOG_", 4),
    "ref_intervenants": ("INT_", 4),
    "ref_types_logements": ("TYPE_", 3),
}


def prochaine_cle(table: str, *, db_path=None) -> str:
    """§82 — l'identifiant se DÉRIVE, il ne se saisit pas.

    Le formulaire de création demandait « PROP_0013 » à l'utilisateur. Rien ne l'empêchait de
    saisir « PROP_13 », « prop_0013 » ou un identifiant déjà pris : le refus arrivait après coup,
    et les formes divergentes cassent tous les rapprochements qui trient sur ce champ.

    La dérivation ne comble JAMAIS un trou : elle part du maximum existant et prend le suivant.
    Réutiliser un identifiant libéré rattacherait des données anciennes à un nouveau tiers.
    """
    prefixe_largeur = CLES_AUTOMATIQUES.get(table)
    if not prefixe_largeur:
        return ""
    prefixe, largeur = prefixe_largeur
    cle_col = _cle_de(table)
    if not cle_col:
        return ""
    conn = get_db(db_path)
    try:
        rows = conn.execute(f"SELECT {cle_col} FROM {table}").fetchall()
    finally:
        conn.close()
    maxi = 0
    for r in rows:
        valeur = txt(r[0])
        if valeur.startswith(prefixe) and valeur[len(prefixe):].isdigit():
            maxi = max(maxi, int(valeur[len(prefixe):]))
    return f"{prefixe}{maxi + 1:0{largeur}d}"


def _cle_de(table: str) -> str:
    native = TABLES_NATIVES.get(table)
    if native is not None:
        return native.cle
    from app.services import ref_setup_catalogue as cat
    feuille = cat.PAR_TABLE.get(table)
    return feuille.cle if feuille else ""


def options_champ(table: str, colonne: str, *, db_path=None) -> list[dict[str, str]]:
    """Valeurs proposées pour une colonne. Liste vide = champ libre.

    Deux sources, jamais mélangées : une énumération déclarée, ou une TABLE référencée dont on
    rend les libellés humains. Dans les deux cas les valeurs réellement présentes en base sont
    ajoutées — une donnée existante hors liste reste choisissable au lieu de disparaître.
    """
    from app.services import referentiel_service as ref

    reference = CHAMPS_REFERENCE.get(table, {}).get(colonne)
    if reference:
        cle_ref = _cle_de(reference)
        conn = get_db(db_path)
        try:
            ids = [txt(r[0]) for r in
                   conn.execute(f"SELECT {cle_ref} FROM {reference} ORDER BY {cle_ref}").fetchall()]
        finally:
            conn.close()
        libelle = {
            "ref_logements": lambda i: ref.libelle_logement(i, db_path=db_path),
            "ref_proprietaires": lambda i: ref.libelle_proprietaire(i, db_path=db_path),
            "ref_types_logements": lambda i: ref.libelle_type_logement(i, db_path=db_path),
            "ref_intervenants": lambda i: ref.libelle_intervenant(i, db_path=db_path),
        }.get(reference, lambda i: i)
        return [{"valeur": i, "libelle": libelle(i) or i} for i in ids if i]

    canoniques = CHAMPS_ENUM.get(table, {}).get(colonne)
    if not canoniques:
        return []
    conn = get_db(db_path)
    try:
        presentes = [txt(r[0]) for r in conn.execute(
            f"SELECT DISTINCT {colonne} FROM {table} "
            f"WHERE {colonne} IS NOT NULL AND TRIM({colonne}) <> ''").fetchall()]
    except Exception:      # noqa: BLE001 — colonne absente d'une base plus ancienne
        presentes = []
    finally:
        conn.close()
    valeurs = list(canoniques) + [v for v in sorted(presentes) if v not in canoniques]
    return [{"valeur": v, "libelle": v} for v in valeurs]


def champs_edition(table: str, *, db_path=None) -> dict[str, dict[str, Any]]:
    """Description de CHAQUE colonne pour l'écran : type de contrôle et valeurs proposées."""
    meta = decrire_table(table, db_path=db_path)
    if not meta.get("ok"):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for c in meta["colonnes"]:
        options = options_champ(table, c, db_path=db_path)
        if c == meta["cle"]:
            type_champ = "cle"
        elif options:
            type_champ = "liste"
        elif _est_colonne_date(c):
            type_champ = "date"
        else:
            type_champ = "texte"
        out[c] = {"type": type_champ, "options": options,
                  "lecture_seule": c in meta.get("colonnes_lecture_seule", [])}
    return out


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
                # §81-91 — la classe gouverne l'écran ET le service. `lecture_seule` en est
                # dérivé, pour les appelants qui raisonnaient déjà dessus.
                "classe": classe(t),
                "libelle_classe": LIBELLES_CLASSE[classe(t)],
                "motif_classe": motif_classe(t),
                "url_parcours": url_parcours(t),
                "editable": est_editable(t),
                "technique": classe(t) == HIDDEN_TECHNICAL,
                "lecture_seule": t in LECTURE_SEULE,
                "motif_lecture_seule": LECTURE_SEULE.get(t, ""),
            })
        # Les référentiels TECHNIQUES sortent de la liste courante — ils n'apprennent rien à
        # l'exploitant — mais restent servis à part, jamais supprimés de l'écran.
        out.append({**c,
                    "tables": [t for t in tables if not t["technique"]],
                    "tables_techniques": [t for t in tables if t["technique"]]})
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
            "classe": classe(table),
            "libelle_classe": LIBELLES_CLASSE[classe(table)],
            "motif_classe": motif_classe(table),
            "url_parcours": url_parcours(table),
            "editable": est_editable(table),
            "lecture_seule": table in LECTURE_SEULE,
            "motif_lecture_seule": LECTURE_SEULE.get(table, ""),
            "a_colonne_actif": COLONNE_ACTIF in native.colonnes,
            "colonnes_lecture_seule": sorted(COLONNES_LECTURE_SEULE.get(table, set())
                                             | COLONNES_DERIVEES.get(table, set())),
        }

    from app.services import ref_setup_catalogue as cat

    feuille = cat.PAR_TABLE.get(table)
    if feuille is None:
        return {"ok": False, "code": "TABLE_INCONNUE", "table": table}
    # Le catalogue décrit le CLASSEUR ; les colonnes dérivées n'existent qu'en base (migration
    # 0087). Les ajouter au catalogue ferait attendre à l'import des colonnes absentes de la
    # feuille ; les taire ici les rendrait invisibles à l'écran alors qu'elles portent la réponse
    # lisible. Elles sont donc jointes à l'affichage, et déclarées non modifiables.
    colonnes = list(feuille.colonnes) + [c for c in sorted(COLONNES_DERIVEES.get(table, set()))
                                         if c not in feuille.colonnes]
    return {
        "ok": True,
        "table": table,
        "onglet": feuille.onglet,
        "cle": feuille.cle,
        "colonnes": colonnes,
        "classe": classe(table),
        "libelle_classe": LIBELLES_CLASSE[classe(table)],
        "motif_classe": motif_classe(table),
        "url_parcours": url_parcours(table),
        "editable": est_editable(table),
        "lecture_seule": table in LECTURE_SEULE,
        "motif_lecture_seule": LECTURE_SEULE.get(table, ""),
        "a_colonne_actif": COLONNE_ACTIF in feuille.colonnes,
        "colonnes_lecture_seule": sorted(COLONNES_LECTURE_SEULE.get(table, set())
                                             | COLONNES_DERIVEES.get(table, set())),
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

    verrouillees = COLONNES_LECTURE_SEULE.get(table, set()) | COLONNES_DERIVEES.get(table, set())
    champs = {c: v for c, v in valeurs.items()
             if c in meta["colonnes"] and c != meta["cle"] and c not in verrouillees}

    # §19 — la paire lisible est ce que l'écran modifie ; la valeur brute en est recomposée, puis
    # le déclencheur 0087 redérive la paire. La boucle est fermée : les trois colonnes ne peuvent
    # pas se contredire, quel que soit le chemin d'écriture.
    if table == TABLE_LOGEMENTS and "dynamic_pricing_enabled" in valeurs:
        champs["dynamic_pricing"] = composer_dynamic_pricing(
            valeurs.get("dynamic_pricing_enabled"), valeurs.get("dynamic_pricing_provider"))

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

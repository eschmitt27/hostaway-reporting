"""Import déterministe REF_Setup.xlsm → SQLite. Prévisualiser, puis importer.

CE QUE FAIT CE SERVICE
Il lit le classeur en LECTURE SEULE et recopie ses 28 onglets dans les 28 tables de la migration
0029. Il ne calcule rien, ne complète rien, ne corrige rien. Une valeur absente reste absente.

DEUX TEMPS, JAMAIS UN SEUL
`previsualiser()` lit et contrôle sans écrire une seule ligne. `importer()` refait exactement les
mêmes contrôles avant d'écrire : la prévisualisation ne dispense pas de la vérification, sinon un
classeur modifié entre les deux passages entrerait sans contrôle.

FAIL-CLOSED
Les contrôles de STRUCTURE sont bloquants — onglet manquant, colonnes inattendues, clé vide ou
dupliquée. Rien n'est écrit du tout : l'écriture des 28 tables tient dans UNE transaction, donc un
import est intégral ou inexistant. Il n'y a pas d'état « à moitié importé ».

Les contrôles MÉTIER — relations orphelines, périodes d'historique qui se chevauchent — sont
signalés `A_CONTROLER` sans bloquer. C'est un arbitrage explicite : ces anomalies existent
peut-être déjà dans le référentiel réel, et refuser l'import les rendrait invisibles au lieu de les
exposer. Bloquer sur elles reviendrait à exiger que les données soient propres avant de pouvoir
les regarder.

IDEMPOTENCE
Un import remplace intégralement le contenu des tables (DELETE puis INSERT dans la transaction).
Deux imports du même classeur produisent donc les mêmes lignes et les mêmes empreintes de contenu.
Le journal, lui, s'ajoute : on garde la trace de chaque tentative, réussie ou refusée.

DÉTERMINISME DES VALEURS
Excel rend des types hétérogènes (datetime, float, int, None) pour des colonnes visuellement
identiques. `_texte()` les normalise d'une seule façon, sinon l'empreinte de contenu changerait
d'une lecture à l'autre sans qu'aucune donnée n'ait bougé.
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid

from app.services.referentiel_admin_service import SOURCE_APPLICATION
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app import config as cfg
from app.db.connection import get_db
from app.services import ref_setup_catalogue as cat

# ── Codes de refus (bloquants) ──────────────────────────────────────────────────────────────────
E_SOURCE_ABSENTE = "REF_SETUP_SOURCE_ABSENTE"
E_SOURCE_ILLISIBLE = "REF_SETUP_SOURCE_ILLISIBLE"
E_ONGLET_MANQUANT = "REF_SETUP_ONGLET_MANQUANT"
E_COLONNES_INATTENDUES = "REF_SETUP_COLONNES_INATTENDUES"
E_CLE_VIDE = "REF_SETUP_CLE_VIDE"
E_CLE_DUPLIQUEE = "REF_SETUP_CLE_DUPLIQUEE"
E_SCHEMA_ABSENT = "REF_SETUP_SCHEMA_ABSENT"

MESSAGES = {
    E_SOURCE_ABSENTE: "Le classeur REF_Setup est introuvable.",
    E_SOURCE_ILLISIBLE: "Le classeur REF_Setup n'a pas pu être ouvert.",
    E_ONGLET_MANQUANT: "Un onglet attendu est absent du classeur.",
    E_COLONNES_INATTENDUES: "Les colonnes d'un onglet ne correspondent pas au catalogue.",
    E_CLE_VIDE: "Une ligne porte une clé vide.",
    E_CLE_DUPLIQUEE: "Une clé apparaît plusieurs fois dans le même onglet.",
    E_SCHEMA_ABSENT: "Les tables du référentiel n'existent pas — migration 0029 non appliquée.",
}

# ── Avertissements (non bloquants) ──────────────────────────────────────────────────────────────
A_ONGLET_HORS_CATALOGUE = "REF_SETUP_ONGLET_HORS_CATALOGUE"
A_RELATION_ORPHELINE = "REF_SETUP_RELATION_ORPHELINE"
A_HISTORIQUE_CHEVAUCHANT = "REF_SETUP_HISTORIQUE_CHEVAUCHANT"

# Relations vérifiées : colonne → table cible. Volontairement limitées aux rattachements dont la
# cible est elle-même un onglet du catalogue ; rien n'est déduit d'un nom de colonne au hasard.
RELATIONS: tuple[tuple[str, str, str], ...] = (
    ("ref_gestion_logements_hist", "logement_id", "ref_logements"),
    ("ref_gestion_logements_hist", "proprietaire_id", "ref_proprietaires"),
    ("ref_taux_commission", "proprietaire_id", "ref_proprietaires"),
    ("ref_taux_commission", "logement_id", "ref_logements"),
    ("ref_logements", "type_logement_id", "ref_types_logements"),
    ("ref_couts_standards_menage", "type_logement_id", "ref_types_logements"),
    ("ref_couts_menage_interne", "type_logement_id", "ref_types_logements"),
    ("ref_couts_menage_interne", "intervenant_id", "ref_intervenants"),
    ("ref_taux_heures_menage", "intervenant_id", "ref_intervenants"),
    ("ref_mapping_logements", "logement_id", "ref_logements"),
    ("ref_charges_recurrentes", "categorie_charge_id", "ref_categories_charges"),
    ("ref_charges_recurrentes", "type_flux_id", "ref_types_flux"),
    ("ref_assoc_mode", "mode_paiement_id", "ref_modes_paiement"),
    ("ref_cartes_paiement", "mode_paiement_id", "ref_modes_paiement"),
)

# Onglets historisés : (table, colonnes de regroupement, début, fin).
#
# Le regroupement est un TUPLE de colonnes, pas une seule : `REF_Taux_Commission` s'historise au
# niveau propriétaire (`logement_id` vide) OU au niveau logement. Grouper sur `logement_id` seul
# rendait invisibles tous les taux propriétaire — c'est-à-dire la totalité des lignes réelles.
HISTORIQUES: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    ("ref_gestion_logements_hist", ("logement_id",), "date_debut", "date_fin"),
    ("ref_taux_commission", ("proprietaire_id", "logement_id"), "date_debut", "date_fin"),
)


# ── Normalisation ───────────────────────────────────────────────────────────────────────────────

def _texte(v: Any) -> str:
    """Valeur Excel → texte stable. Une seule règle, pour que l'empreinte soit reproductible."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        # Minuit = date pure saisie dans Excel ; conserver « 00:00:00 » ferait varier l'empreinte
        # selon la façon dont la cellule a été saisie, pas selon son contenu.
        if v.hour == 0 and v.minute == 0 and v.second == 0 and v.microsecond == 0:
            return v.date().isoformat()
        return v.isoformat(sep=" ")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, bool):
        return "OUI" if v else "NON"
    if isinstance(v, float):
        # 35.0 saisi comme entier ne doit pas devenir « 35.0 » un jour et « 35 » un autre.
        if v.is_integer():
            return str(int(v))
        return repr(v)
    return str(v).strip()


def _empreinte(lignes: list[dict[str, str]], colonnes: tuple[str, ...]) -> str:
    """Empreinte du CONTENU d'un onglet, indépendante de l'ordre des lignes.

    Trier avant de hacher : une réorganisation des lignes dans Excel ne change pas le référentiel,
    elle ne doit pas faire croire à une modification.
    """
    corps = sorted("\x1f".join(l.get(c, "") for c in colonnes) for l in lignes)
    h = hashlib.sha256()
    h.update("\x1e".join(colonnes).encode("utf-8"))
    for ligne in corps:
        h.update(b"\x1d")
        h.update(ligne.encode("utf-8"))
    return h.hexdigest()


def empreinte_fichier(chemin: Path) -> str:
    h = hashlib.sha256()
    with open(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

def _lire_classeur(chemin: Path) -> tuple[dict[str, list[dict[str, str]]], list[str], dict | None]:
    """(contenu par onglet, onglets du classeur, refus éventuel). Lecture seule, jamais d'écriture."""
    try:
        import openpyxl
    except ImportError:  # pragma: no cover - dépend de l'interpréteur
        return {}, [], _refus(E_SOURCE_ILLISIBLE, "openpyxl absent de cet interpréteur.")
    try:
        wb = openpyxl.load_workbook(chemin, read_only=True, data_only=True, keep_vba=True)
    except Exception as exc:
        return {}, [], _refus(E_SOURCE_ILLISIBLE, f"{type(exc).__name__}: {exc}")

    contenu: dict[str, list[dict[str, str]]] = {}
    try:
        for feuille in cat.FEUILLES:
            if feuille.onglet not in wb.sheetnames:
                return {}, list(wb.sheetnames), _refus(E_ONGLET_MANQUANT, feuille.onglet)
            ws = wb[feuille.onglet]
            iterateur = ws.iter_rows(values_only=True)
            try:
                entete = [_texte(v) for v in next(iterateur)]
            except StopIteration:
                entete = []
            entete = [c for c in entete if c]
            if tuple(entete) != feuille.colonnes:
                manquantes = [c for c in feuille.colonnes if c not in entete]
                nouvelles = [c for c in entete if c not in feuille.colonnes]
                return {}, list(wb.sheetnames), _refus(
                    E_COLONNES_INATTENDUES,
                    f"{feuille.onglet} — manquantes: {manquantes or 'aucune'} ; "
                    f"inattendues: {nouvelles or 'aucune'}")
            lignes: list[dict[str, str]] = []
            for brute in iterateur:
                valeurs = [_texte(v) for v in brute][:len(feuille.colonnes)]
                valeurs += [""] * (len(feuille.colonnes) - len(valeurs))
                # Ligne entièrement vide = fin de tableau Excel, pas une donnée.
                if not any(valeurs):
                    continue
                lignes.append(dict(zip(feuille.colonnes, valeurs)))
            contenu[feuille.onglet] = lignes
        onglets = list(wb.sheetnames)
    finally:
        wb.close()
    return contenu, onglets, None


# ── Contrôles ───────────────────────────────────────────────────────────────────────────────────

def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _controler(contenu: dict[str, list[dict[str, str]]],
               onglets_classeur: list[str]) -> tuple[dict | None, list[dict[str, str]]]:
    """(refus bloquant ou None, avertissements). Les deux sont retournés : un refus n'efface pas
    les avertissements déjà constatés, ils restent utiles au diagnostic."""
    avertissements: list[dict[str, str]] = []

    connus = {f.onglet for f in cat.FEUILLES}
    for nom in onglets_classeur:
        if nom not in connus:
            avertissements.append({
                "code": A_ONGLET_HORS_CATALOGUE, "cible": nom,
                "message": f"Onglet « {nom} » présent dans le classeur mais absent du catalogue — "
                           "il n'est pas importé."})

    # Clés : vides et doublons. Bloquant — sans clé fiable, aucune table n'a de sens.
    for feuille in cat.FEUILLES:
        vues: dict[str, int] = {}
        for i, ligne in enumerate(contenu.get(feuille.onglet, []), start=2):
            cle = ligne.get(feuille.cle, "")
            if not cle:
                return _refus(E_CLE_VIDE, f"{feuille.onglet} ligne {i}"), avertissements
            if cle in vues:
                return _refus(E_CLE_DUPLIQUEE,
                              f"{feuille.onglet} — « {cle} » lignes {vues[cle]} et {i}"), avertissements
            vues[cle] = i

    # Relations : signalées, non bloquantes (cf. docstring).
    index = {f.table: {l.get(f.cle, "") for l in contenu.get(f.onglet, [])} for f in cat.FEUILLES}
    for table, colonne, cible in RELATIONS:
        feuille = cat.PAR_TABLE[table]
        if colonne not in feuille.colonnes:
            continue
        connus_cible = index.get(cible, set())
        for ligne in contenu.get(feuille.onglet, []):
            valeur = ligne.get(colonne, "")
            if valeur and valeur not in connus_cible:
                avertissements.append({
                    "code": A_RELATION_ORPHELINE, "cible": f"{feuille.onglet}.{colonne}",
                    "message": f"{feuille.onglet} — {colonne} « {valeur} » absent de "
                               f"{cat.PAR_TABLE[cible].onglet}."})

    # Historique : chevauchement de périodes sur une même clé de regroupement.
    for table, groupes, debut, fin in HISTORIQUES:
        feuille = cat.PAR_TABLE[table]
        par_groupe: dict[tuple[str, ...], list[tuple[str, str, str]]] = {}
        for ligne in contenu.get(feuille.onglet, []):
            g = tuple(ligne.get(col, "") for col in groupes)
            # Toutes les colonnes de regroupement vides = rattachement non renseigné, rien à
            # comparer. Une seule renseignée suffit à constituer un groupe.
            if not any(g):
                continue
            par_groupe.setdefault(g, []).append(
                (ligne.get(debut, ""), ligne.get(fin, ""), ligne.get(feuille.cle, "")))
        for g, periodes in par_groupe.items():
            periodes.sort()
            libelle = " / ".join(v for v in g if v)
            for i in range(len(periodes) - 1):
                _, fin_i, id_i = periodes[i]
                debut_j, _, id_j = periodes[i + 1]
                # Une période sans fin couvre tout l'avenir : elle chevauche toute période suivante.
                if not fin_i or (debut_j and fin_i > debut_j):
                    avertissements.append({
                        "code": A_HISTORIQUE_CHEVAUCHANT, "cible": f"{feuille.onglet} / {libelle}",
                        "message": f"{feuille.onglet} — « {id_i} » et « {id_j} » se chevauchent "
                                   f"sur {libelle}."})
    return None, avertissements


# ── API publique ────────────────────────────────────────────────────────────────────────────────

def _chemin_source(chemin: Path | None = None) -> Path:
    """Résolu à l'appel, jamais figé : une instance qui redirige la configuration doit être suivie."""
    return Path(chemin) if chemin is not None else Path(cfg.REF_SETUP)


def _schema_present(conn: sqlite3.Connection) -> bool:
    noms = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    return cat.TABLE_IMPORTS in noms and all(t in noms for t in cat.toutes_les_tables())


def previsualiser(*, chemin: Path | None = None, db_path=None) -> dict[str, Any]:
    """Lit, contrôle, ne modifie RIEN. Le résultat dit exactement ce qu'un import écrirait."""
    source = _chemin_source(chemin)
    if not source.exists():
        return {**_refus(E_SOURCE_ABSENTE, str(source)), "feuilles": [], "avertissements": []}

    contenu, onglets, refus = _lire_classeur(source)
    if refus:
        return {**refus, "feuilles": [], "avertissements": []}

    refus, avertissements = _controler(contenu, onglets)
    feuilles = [{
        "onglet": f.onglet, "table": f.table, "cle": f.cle,
        "nb_colonnes": len(f.colonnes),
        "nb_lignes": len(contenu.get(f.onglet, [])),
        "empreinte": _empreinte(contenu.get(f.onglet, []), f.colonnes),
    } for f in cat.FEUILLES]

    conn = get_db(db_path)
    try:
        existant = {}
        if _schema_present(conn):
            for f in cat.FEUILLES:
                existant[f.table] = conn.execute(f"SELECT COUNT(*) FROM {f.table}").fetchone()[0]
    finally:
        conn.close()
    for ligne in feuilles:
        ligne["nb_lignes_actuelles"] = existant.get(ligne["table"])

    return {
        "ok": refus is None,
        **({} if refus is None else refus),
        "chemin_source": str(source),
        "empreinte_source": empreinte_fichier(source),
        "feuilles": feuilles,
        "avertissements": avertissements,
        "nb_feuilles": len(feuilles),
        "nb_lignes": sum(f["nb_lignes"] for f in feuilles),
    }


def importer(*, chemin: Path | None = None, db_path=None) -> dict[str, Any]:
    """Contrôle puis écrit les 28 tables dans UNE transaction. Intégral ou inexistant."""
    source = _chemin_source(chemin)
    import_id = "IMP-" + uuid.uuid4().hex[:12].upper()
    horodatage = datetime.now().isoformat(timespec="seconds")

    if not source.exists():
        return {**_refus(E_SOURCE_ABSENTE, str(source)), "import_id": None}

    contenu, onglets, refus = _lire_classeur(source)
    if refus is None:
        refus, avertissements = _controler(contenu, onglets)
    else:
        avertissements = []

    empreinte = empreinte_fichier(source)
    conn = get_db(db_path)
    try:
        if not _schema_present(conn):
            return {**_refus(E_SCHEMA_ABSENT), "import_id": None}

        if refus is not None:
            # Un refus est journalisé : c'est une tentative, et elle doit rester visible.
            conn.execute(
                f"INSERT INTO {cat.TABLE_IMPORTS} (import_id, horodatage, chemin_source, "
                "empreinte_source, statut, nb_feuilles, nb_lignes, code_refus, message) "
                "VALUES (?,?,?,?,'REFUSE',0,0,?,?)",
                (import_id, horodatage, str(source), empreinte,
                 refus["code"], refus.get("detail") or refus["message"]))
            conn.commit()
            return {**refus, "import_id": import_id, "avertissements": avertissements}

        total = 0
        conn.execute("BEGIN IMMEDIATE")
        try:
            for f in cat.FEUILLES:
                lignes = contenu.get(f.onglet, [])
                # Remplacement du contenu IMPORTÉ — mais jamais de ce qui a été saisi dans
                # l'application. Depuis que le référentiel est administrable (0051), une ligne
                # peut naître ou être modifiée dans l'interface : la supprimer au prochain import
                # du classeur ferait de SQLite une simple copie, et perdrait la saisie sans
                # prévenir.
                conn.execute(f"DELETE FROM {f.table} WHERE import_id IS NOT ? ", (SOURCE_APPLICATION,))
                deja = {r[0] for r in conn.execute(
                    f"SELECT {f.cle} FROM {f.table} WHERE import_id IS ?", (SOURCE_APPLICATION,))}
                if deja:
                    conflits = [l for l in lignes if str(l.get(f.cle, "")).strip() in deja]
                    if conflits:
                        # Signalé, jamais silencieux : c'est une divergence entre le classeur et
                        # une saisie applicative, et c'est la saisie qui fait foi.
                        avertissements.append(
                            f"{f.onglet} : {len(conflits)} ligne(s) du classeur ignorée(s), déjà "
                            f"administrée(s) dans l'application "
                            f"({', '.join(sorted(str(l.get(f.cle)) for l in conflits)[:5])})")
                    lignes = [l for l in lignes if str(l.get(f.cle, "")).strip() not in deja]
                if lignes:
                    cols = ", ".join((*f.colonnes, "import_id"))
                    trous = ", ".join(["?"] * (len(f.colonnes) + 1))
                    conn.executemany(
                        f"INSERT INTO {f.table} ({cols}) VALUES ({trous})",
                        [tuple(l.get(c, "") for c in f.colonnes) + (import_id,) for l in lignes])
                conn.execute(
                    f"INSERT INTO {cat.TABLE_IMPORT_FEUILLES} (import_id, onglet, table_cible, "
                    "nb_lignes_source, nb_lignes_ecrites, empreinte_contenu) VALUES (?,?,?,?,?,?)",
                    (import_id, f.onglet, f.table, len(lignes), len(lignes),
                     _empreinte(lignes, f.colonnes)))
                total += len(lignes)
            conn.execute(
                f"INSERT INTO {cat.TABLE_IMPORTS} (import_id, horodatage, chemin_source, "
                "empreinte_source, statut, nb_feuilles, nb_lignes) VALUES (?,?,?,?,'IMPORTE',?,?)",
                (import_id, horodatage, str(source), empreinte, len(cat.FEUILLES), total))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "import_id": import_id, "horodatage": horodatage,
            "chemin_source": str(source), "empreinte_source": empreinte,
            "nb_feuilles": len(cat.FEUILLES), "nb_lignes": total,
            "avertissements": avertissements}


def dernier_import(*, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        if not _schema_present(conn):
            return None
        r = conn.execute(
            f"SELECT import_id, horodatage, chemin_source, empreinte_source, statut, nb_feuilles, "
            f"nb_lignes, code_refus, message FROM {cat.TABLE_IMPORTS} "
            "WHERE statut = 'IMPORTE' ORDER BY horodatage DESC, rowid DESC LIMIT 1").fetchone()
        if r is None:
            return None
        return {"import_id": r[0], "horodatage": r[1], "chemin_source": r[2],
                "empreinte_source": r[3], "statut": r[4], "nb_feuilles": r[5],
                "nb_lignes": r[6], "code_refus": r[7], "message": r[8]}
    finally:
        conn.close()


def historique_imports(limite: int = 20, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if not _schema_present(conn):
            return []
        rows = conn.execute(
            f"SELECT import_id, horodatage, statut, nb_feuilles, nb_lignes, code_refus, message "
            f"FROM {cat.TABLE_IMPORTS} ORDER BY horodatage DESC, rowid DESC LIMIT ?",
            (limite,)).fetchall()
        return [{"import_id": r[0], "horodatage": r[1], "statut": r[2], "nb_feuilles": r[3],
                 "nb_lignes": r[4], "code_refus": r[5], "message": r[6]} for r in rows]
    finally:
        conn.close()

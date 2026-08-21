"""Reader Contrôles & clôture (APP-5A) — LECTURE SEULE, 100% SQLite.

Lit les sorties consolidées du Lot11 et l'état de clôture, toutes deux en base :
  - `controles_lot11_constats` / `_champs` (0041/0042) : contrôles unifiés. Les quatre « onglets »
    du classeur historique (MASTER, BLOQUANTS_OUVERTS, A_CONTROLER_OUVERTS, DASHBOARD_MOIS) sont
    devenus des filtres sur ces tables et sur `controles_lot11_dashboard_mois` (0046).
  - `ref_cloture_mensuelle` (0029) : statut de clôture officiel des mois
    (OUVERT/EN_CONTROLE/CLOTURE).

`MASTER_CTRL_Coherence.xlsx` N'EST PLUS LU. Lot11 étant un moteur SQLite natif
(`controles_lot11_service`), continuer à lire le classeur faisait servir aux écrans les contrôles
du DERNIER CALCUL LEGACY, pas ceux du calcul courant — un écart invisible, puisque le fichier
existe toujours et se lit sans erreur.

Ne recrée aucun contrôle, ne clôture rien, n'écrit rien. Le code, le niveau et le statut viennent
du moteur. Aucun chemin absolu exposé.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any

# ── Onglets ──────────────────────────────────────────────────────────────────
ONGLET_MASTER = "MASTER"
ONGLET_BLOQUANTS = "BLOQUANTS_OUVERTS"
ONGLET_A_CONTROLER = "A_CONTROLER_OUVERTS"
ONGLET_DASHBOARD = "DASHBOARD_MOIS"
ONGLET_CLOTURE_REF = "REF_Cloture_Mensuelle"

SOURCE_COHERENCE = "MASTER_CTRL_Coherence.xlsx"
SOURCE_REF = "Référentiel SQLite (ref_cloture_mensuelle)"
ETAT_REFERENTIEL_ABSENT = "REFERENTIEL_NON_INITIALISE"

# ── États ────────────────────────────────────────────────────────────────────
ETAT_OK = "OK"
ETAT_FICHIER_ABSENT = "FICHIER_ABSENT"
ETAT_ONGLET_ABSENT = "ONGLET_ABSENT"
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"

_ETAT_LIBELLE = {
    ETAT_OK: "Alimentée", ETAT_FICHIER_ABSENT: "Fichier absent", ETAT_ONGLET_ABSENT: "Onglet absent",
    ETAT_VIDE: "Source vide", ETAT_ILLISIBLE: "Source illisible",
    ETAT_REFERENTIEL_ABSENT: "Référentiel non initialisé",
}


@dataclass(frozen=True)
class EtatSource:
    cle: str
    libelle: str
    fichier: str
    onglet: str
    etat: str
    nb_lignes: int = 0
    derniere_maj: str | None = None

    @property
    def disponible(self) -> bool:
        return self.etat == ETAT_OK

    @property
    def etat_libelle(self) -> str:
        return _ETAT_LIBELLE.get(self.etat, self.etat)


@dataclass(frozen=True)
class Source:
    etat: EtatSource
    lignes: list[dict[str, Any]] = field(default_factory=list)


_CACHE: dict[tuple, tuple[str, list[dict[str, Any]], str | None]] = {}


def vider_cache() -> None:
    _CACHE.clear()


# La lecture Excel a été RETIRÉE, pas seulement contournée : ce module n'importe plus openpyxl et
# ne peut donc plus ouvrir un classeur, même par erreur. L'ancien `_lire` enveloppait la lecture
# dans un `except Exception` large qui transformait toute panne — y compris l'interception d'un
# test de non-régression — en simple état ILLISIBLE, ce qui masquait la dépendance au lieu de la
# signaler.


def _src_coherence(cle: str, libelle: str, sheet: str) -> Source:
    """Constats Lot11 depuis SQLite — le classeur n'est plus lu.

    Lot11 est un moteur SQLite natif (`controles_lot11_service`) : les constats vivent dans
    `controles_lot11_constats` (+ `_champs` pour mois/logement/propriétaire, 0041/0042) et la
    clôturabilité par mois dans `controles_lot11_dashboard_mois` (0046). Les quatre « onglets »
    historiques deviennent quatre FILTRES sur ces tables, avec exactement les mêmes noms de
    colonnes : les écrans consommateurs n'ont rien à changer.

    Aucun repli sur le classeur. Une base sans constats rend un état affiché (VIDE /
    FICHIER_ABSENT), jamais un retour silencieux sur un fichier qui daterait d'un calcul
    précédent — c'était le défaut de la lecture Excel : après un recalcul SQLite, l'écran
    continuait de servir les contrôles de l'ancien classeur sans que rien ne le signale.
    """
    from app.db.connection import get_db

    conn = get_db(None)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        ("controles_lot11_constats",)).fetchone() is None:
            etat, lignes, maj = ETAT_FICHIER_ABSENT, [], None
        elif sheet == ONGLET_DASHBOARD:
            lignes = _dashboard_sqlite(conn)
            etat, maj = (ETAT_OK if lignes else ETAT_VIDE), _dernier_calcul(conn)
        else:
            lignes = _constats_sqlite(conn, sheet)
            etat, maj = (ETAT_OK if lignes else ETAT_VIDE), _dernier_calcul(conn)
    finally:
        conn.close()
    return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_COHERENCE, onglet=sheet,
                                  etat=etat, nb_lignes=len(lignes), derniere_maj=maj), lignes=lignes)


def _dernier_calcul(conn) -> str | None:
    """Date du dernier run Lot11 réussi — la fraîcheur vient du run, jamais d'un `mtime`."""
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    ("controles_lot11_runs",)).fetchone() is None:
        return None
    r = conn.execute("SELECT date_calcul FROM controles_lot11_runs WHERE statut = 'SUCCES' "
                     "ORDER BY date_calcul DESC LIMIT 1").fetchone()
    return r[0] if r else None


def _constats_sqlite(conn, sheet: str) -> list[dict[str, Any]]:
    """MASTER / BLOQUANTS_OUVERTS / A_CONTROLER_OUVERTS — mêmes filtres que le moteur.

    Le legacy construisait ses deux onglets « ouverts » en filtrant le MASTER sur `severity` +
    `statut_resolution == OUVERT` : le filtre est repris tel quel, pas réinventé.
    """
    filtres = {
        ONGLET_BLOQUANTS: " WHERE c.severity = 'BLOQUANT' AND c.statut_resolution = 'OUVERT'",
        ONGLET_A_CONTROLER: " WHERE c.severity = 'A_CONTROLER' AND c.statut_resolution = 'OUVERT'",
    }
    champs_dispo = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        ("controles_lot11_constats_champs",)).fetchone() is not None
    colonnes = ("c.ctrl_pk, c.source_module, c.source_table, c.source_pk, c.code_controle, "
                "c.severity, c.message, c.impact_facture, c.statut_resolution, c.commentaire")
    jointure = ""
    if champs_dispo:
        colonnes += (", f.mois, f.logement_id, f.proprietaire_id, f.reservation_id, "
                     "f.document_id, f.date_detection")
        jointure = " LEFT JOIN controles_lot11_constats_champs f ON f.ctrl_pk = c.ctrl_pk"
    sql = f"SELECT {colonnes} FROM controles_lot11_constats c{jointure}{filtres.get(sheet, '')}"
    return [dict(r) for r in conn.execute(sql)]


def _dashboard_sqlite(conn) -> list[dict[str, Any]]:
    """DASHBOARD_MOIS — clôturabilité par mois, telle que Lot11 la calcule (0046)."""
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    ("controles_lot11_dashboard_mois",)).fetchone() is None:
        return []
    return [dict(r) for r in conn.execute(
        "SELECT mois, nb_bloquants_ouverts, nb_a_controler_ouverts, nb_info, "
        "statut_mois_banque, cloture_possible, facturation_lot12_ok "
        "FROM controles_lot11_dashboard_mois ORDER BY mois")]


# ── Convertisseurs ───────────────────────────────────────────────────────────

def to_texte(v: Any) -> str:
    return "" if v is None else str(v).strip()


def to_nombre(v: Any) -> float | int | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def to_mois(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m")
    return str(v)[:7]


def to_date(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m-%d")
    return str(v)[:10]


# ── Sources ──────────────────────────────────────────────────────────────────

def controles() -> Source:
    return _src_coherence("controles", "Contrôles consolidés (Lot11)", ONGLET_MASTER)


def bloquants_ouverts() -> Source:
    return _src_coherence("bloquants", "Contrôles bloquants ouverts", ONGLET_BLOQUANTS)


def a_controler_ouverts() -> Source:
    return _src_coherence("a_controler", "Contrôles à contrôler ouverts", ONGLET_A_CONTROLER)


def dashboard_mois() -> Source:
    return _src_coherence("dashboard_mois", "Clôturabilité par mois", ONGLET_DASHBOARD)


def cloture_ref() -> Source:
    """État de clôture officiel des mois — REF_Setup.xlsm > REF_Cloture_Mensuelle."""
    # Lecture du référentiel SQLite. Aucun repli sur le classeur : « référentiel non initialisé »
    # et « statuts de clôture vides » appellent deux actions différentes, et les confondre
    # enverrait l'utilisateur chercher un problème de données là où il manque un import.
    from app.services import referentiel_service as referentiel

    if not referentiel.disponible():
        etat, lignes, maj = ETAT_REFERENTIEL_ABSENT, [], None
    else:
        lignes = referentiel.cloture_mensuelle()
        etat, maj = (ETAT_OK if lignes else ETAT_VIDE), None
    return Source(etat=EtatSource(cle="cloture_ref", libelle="Statut de clôture des mois",
                                  fichier=SOURCE_REF, onglet=ONGLET_CLOTURE_REF, etat=etat,
                                  nb_lignes=len(lignes), derniere_maj=maj), lignes=lignes)


def etats_sources() -> list[EtatSource]:
    return [controles().etat, dashboard_mois().etat, cloture_ref().etat]

"""Reader Propriétaires & règlements (APP-3C) — LECTURE SEULE.

Lit les sorties moteur du pilotage propriétaire :
  - Lot10 (SQLite, migration 0044) : `lot10_net_vue_mois`, `lot10_net_reglement`,
    `lot10_commissions`, `lot10_resultats` — dataset du run ACTIF (`lot10_runs.actif = 1`).
  - Lot12 (SQLite, migration 0047) : `lot12_prefactures_entete`, `lot12_prefactures_lignes`,
    `lot12_dashboard_facturation`, `lot12_a_controler` — dataset du run ACTIF
    (`lot12_runs.actif = 1`). `MASTER_FACT_Proprietaires.xlsx` n'est plus lu.

Ne recalcule aucune commission, aucun net : ces valeurs viennent du moteur. Aucune écriture.
N'expose jamais l'adresse du propriétaire ni de chemin absolu.

SQLITE SANS REPLI EXCEL (mission Lot10 §32)
Les trois masters Lot10 ne sont plus lus. Une base sans run actif rend `DATASET_NON_INITIALISE` —
un état affiché, jamais une exception, et jamais un retour silencieux sur un classeur qui daterait.

PAR_MOIS_PROPRIETAIRE ET GLOBAL SONT DÉRIVÉS, PAS STOCKÉS
Le classeur legacy portait ces deux onglets ; la migration 0044 ne stocke que le grain fin
(`lot10_resultats`, mois × logement × propriétaire × vision). Les agrégats sont recalculés ici avec
EXACTEMENT la règle de `lot10_calculer_resultats.write_all` (somme par mois/propriétaire/vision,
puis total par vision) — reprise telle quelle, jamais réinventée. Stocker trois copies d'une même
vérité, c'est se garantir qu'elles finiront par diverger.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

# ── Onglets ──────────────────────────────────────────────────────────────────
ONGLET_VUE_MOIS = "VUE_MOIS"
ONGLET_REGLEMENT = "REGLEMENT"
ONGLET_COMMISSIONS = "COMMISSIONS"
ONGLET_RESULTATS = "PAR_MOIS_PROPRIETAIRE"
ONGLET_RESULTATS_LOGEMENT = "PAR_MOIS_LOGEMENT"
ONGLET_RESULTATS_GLOBAL = "GLOBAL"
ONGLET_FACT_ENTETE = "FACT_FACTURE_ENTETE"
ONGLET_DASHBOARD = "DASHBOARD_FACTURATION"
ONGLET_A_CONTROLER = "A_CONTROLER"

# ── Libellés de source (noms de fichier uniquement) ──────────────────────────
SOURCE_NET = "MASTER_CALC_NetProprietaire.xlsx"
SOURCE_COMMISSIONS = "MASTER_CALC_Commissions.xlsx"
SOURCE_RESULTATS = "MASTER_CALC_Resultats.xlsx"
SOURCE_FACT = "MASTER_FACT_Proprietaires.xlsx"

# ── États ────────────────────────────────────────────────────────────────────
ETAT_OK = "OK"
ETAT_FICHIER_ABSENT = "FICHIER_ABSENT"
ETAT_ONGLET_ABSENT = "ONGLET_ABSENT"
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"

_ETAT_LIBELLE = {
    ETAT_OK: "Alimentée", ETAT_FICHIER_ABSENT: "Fichier absent", ETAT_ONGLET_ABSENT: "Onglet absent",
    ETAT_VIDE: "Source vide", ETAT_ILLISIBLE: "Source illisible",
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
    global _CACHE_NOMS_LOGEMENTS
    _CACHE.clear()
    _CACHE_NOMS_LOGEMENTS = None


def _lire(path: Path, sheet: str) -> tuple[str, list[dict[str, Any]], str | None]:
    p = Path(path)
    if not p.exists():
        return ETAT_FICHIER_ABSENT, [], None
    try:
        st = p.stat()
        cle = (str(p), sheet, st.st_mtime_ns, st.st_size)
        if cle in _CACHE:
            return _CACHE[cle]
        maj = _dt.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M")
        wb = openpyxl.load_workbook(str(p), read_only=True, data_only=True)
        try:
            if sheet not in wb.sheetnames:
                res = (ETAT_ONGLET_ABSENT, [], maj); _CACHE[cle] = res; return res
            ws = wb[sheet]
            rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
        finally:
            wb.close()
        if len(rows) <= 1:
            res = (ETAT_VIDE, [], maj)
        else:
            hdr = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
            res = (ETAT_OK, [dict(zip(hdr, r)) for r in rows[1:]], maj)
        _CACHE[cle] = res
        return res
    except Exception:
        return ETAT_ILLISIBLE, [], None


def _src(path: Path, cle: str, libelle: str, fichier: str, sheet: str) -> Source:
    etat, lignes, maj = _lire(path, sheet)
    return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=fichier, onglet=sheet,
                                  etat=etat, nb_lignes=len(lignes), derniere_maj=maj), lignes=lignes)


# ── Lot10 en SQLite (migration 0044) ─────────────────────────────────────────
# Le dataset servi est celui du run ACTIF. Un run en cours ou échoué n'est jamais lu : c'est la
# contrepartie de l'écriture atomique côté moteur — un demi-calcul n'apparaît pas comme frais.

SOURCE_LOT10 = "SQLite (Lot10)"


def _run_actif(db_path=None) -> tuple[str, str | None]:
    """(run_id, date_calcul) du dataset Lot10 actif. ("", None) si aucun — état légitime."""
    from app.db.connection import get_db

    conn = get_db(db_path)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lot10_runs'"
                        ).fetchone() is None:
            return "", None
        r = conn.execute("SELECT run_id, date_calcul FROM lot10_runs WHERE actif = 1").fetchone()
        return (r[0], r[1]) if r else ("", None)
    finally:
        conn.close()


def _src_sqlite(cle: str, libelle: str, table: str, colonnes: str = "*",
                ordre: str = "id", db_path=None) -> Source:
    """Lecture d'une table Lot10 du run actif. Aucun repli Excel : dataset absent = état affiché."""
    from app.db.connection import get_db

    run_id, maj = _run_actif(db_path)
    if not run_id:
        return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_LOT10, onglet=table,
                                      etat=ETAT_FICHIER_ABSENT))
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            f"SELECT {colonnes} FROM {table} WHERE run_id = ? ORDER BY {ordre}", (run_id,))]
    finally:
        conn.close()
    etat = ETAT_OK if lignes else ETAT_VIDE
    return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_LOT10, onglet=table,
                                  etat=etat, nb_lignes=len(lignes), derniere_maj=maj),
                  lignes=lignes)


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


# ── Sources ──────────────────────────────────────────────────────────────────

def net_vue_mois(db_path=None) -> Source:
    return _src_sqlite("net_vue_mois", "Net propriétaire (mois)", "lot10_net_vue_mois",
                       db_path=db_path)


def net_reglement(db_path=None) -> Source:
    return _src_sqlite("net_reglement", "Règlements (mois × logement)", "lot10_net_reglement",
                       db_path=db_path)


def commissions(db_path=None) -> Source:
    """COMMISSIONS — grain réservation. `guest_count` est réexposé sous le nom moteur `guestCount`
    (la base nomme en snake_case, les consommateurs applicatifs connaissent le nom du classeur)."""
    src = _src_sqlite("commissions", "Commissions (par réservation)", "lot10_commissions",
                      db_path=db_path)
    for ligne in src.lignes:
        if "guest_count" in ligne:
            ligne["guestCount"] = ligne["guest_count"]
    return src


def resultats(db_path=None) -> Source:
    """PAR_MOIS_PROPRIETAIRE — agrégat DÉRIVÉ de `lot10_resultats`, même règle que
    `lot10_calculer_resultats.write_all._agg_prop` : somme par (mois, propriétaire, vision).

    Le legacy n'agrège que REEL et COMPTABLE dans cet onglet — HORS_COMPTA en est absent. Cette
    restriction est reprise telle quelle : l'ajouter changerait ce que les écrans affichent.
    """
    base = _src_sqlite("resultats", "Résultats par mois/propriétaire", "lot10_resultats",
                       db_path=db_path)
    if not base.etat.disponible:
        return base
    cumuls: dict[tuple, dict[str, Any]] = {}
    for r in base.lignes:
        vision = to_texte(r.get("vision"))
        if vision not in ("REEL", "COMPTABLE"):
            continue
        cle = (to_texte(r.get("mois")), to_texte(r.get("proprietaire_id")), vision)
        cumul = cumuls.setdefault(cle, {
            "mois": cle[0], "proprietaire_id": cle[1], "vision": vision,
            "total_produits": 0.0, "total_charges": 0.0, "resultat": 0.0, "nb_flux": 0.0})
        for champ in ("total_produits", "total_charges", "resultat", "nb_flux"):
            cumul[champ] += to_nombre(r.get(champ)) or 0.0
    lignes = [{**v, **{c: round(v[c], 2) for c in ("total_produits", "total_charges", "resultat")}}
              for v in cumuls.values()]
    return Source(etat=EtatSource(cle="resultats", libelle="Résultats par mois/propriétaire",
                                  fichier=SOURCE_LOT10, onglet="lot10_resultats (agrégé)",
                                  etat=base.etat.etat, nb_lignes=len(lignes),
                                  derniere_maj=base.etat.derniere_maj), lignes=lignes)


def resultats_par_logement(db_path=None) -> Source:
    """PAR_MOIS_LOGEMENT — mois × logement × vision (REEL/COMPTABLE/HORS_COMPTA empilées),
    déjà calculé par Lot10 (`build_resultats`). Jamais recalculé ici."""
    return _src_sqlite("resultats_logement", "Résultats par mois/logement/vision",
                       "lot10_resultats", db_path=db_path)


def resultats_global(db_path=None) -> Source:
    """GLOBAL — un total par vision, dérivé de `lot10_resultats` comme le fait `write_all`.

    `commentaire_hc` reprend la formulation et le seuil du moteur (identité REEL=COMPTABLE+HC
    vérifiée à 1,00 € près, D035) : c'est le même contrôle, au même endroit du calcul, pas un
    second verdict inventé par l'application.
    """
    base = _src_sqlite("resultats_global", "Résultats globaux par vision", "lot10_resultats",
                       db_path=db_path)
    if not base.etat.disponible:
        return base
    par_vision: dict[str, dict[str, float]] = {}
    for r in base.lignes:
        v = to_texte(r.get("vision"))
        cumul = par_vision.setdefault(v, {"total_produits": 0.0, "total_charges": 0.0,
                                          "resultat": 0.0})
        for champ in cumul:
            cumul[champ] += to_nombre(r.get(champ)) or 0.0

    def _tot(vision: str, champ: str) -> float:
        return round(par_vision.get(vision, {}).get(champ, 0.0), 2)

    reel, compt, hc = _tot("REEL", "resultat"), _tot("COMPTABLE", "resultat"), _tot("HORS_COMPTA", "resultat")
    ecart = abs(reel - (compt + hc))
    commentaires = {
        "REEL": (f"REEL=COMPTABLE+HC verifie (ecart={ecart:.2f} EUR)" if ecart <= 1.00
                 else f"!! RUPTURE REEL != COMPTABLE+HC (ecart={ecart:.2f} EUR)"),
        "COMPTABLE": "Vision comptable (IC)",
        "HORS_COMPTA": ("Aucun flux HC" if hc == 0.0 and _tot("HORS_COMPTA", "total_produits") == 0.0
                        and _tot("HORS_COMPTA", "total_charges") == 0.0 else "Flux HC presents"),
    }
    lignes = [{"vision": v, "total_produits": _tot(v, "total_produits"),
               "total_charges": _tot(v, "total_charges"), "resultat": _tot(v, "resultat"),
               "commentaire_hc": commentaires.get(v, "")}
              for v in ("REEL", "COMPTABLE", "HORS_COMPTA") if v in par_vision]
    return Source(etat=EtatSource(cle="resultats_global", libelle="Résultats globaux par vision",
                                  fichier=SOURCE_LOT10, onglet="lot10_resultats (agrégé)",
                                  etat=base.etat.etat, nb_lignes=len(lignes),
                                  derniere_maj=base.etat.derniere_maj), lignes=lignes)


SOURCE_LOT12 = "SQLite (Lot12)"


def _run_actif_lot12(db_path=None) -> tuple[str, str | None]:
    """(run_id, date_calcul) du dataset Lot12 actif. ("", None) si aucun — état légitime."""
    from app.db.connection import get_db

    conn = get_db(db_path)
    try:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lot12_runs'"
                        ).fetchone() is None:
            return "", None
        r = conn.execute("SELECT run_id, date_calcul FROM lot12_runs WHERE actif = 1").fetchone()
        return (r[0], r[1]) if r else ("", None)
    finally:
        conn.close()


def _src_sqlite_lot12(cle: str, libelle: str, table: str, db_path=None) -> Source:
    """Lecture d'une table Lot12 (0047) du run actif. Aucun repli Excel."""
    from app.db.connection import get_db

    run_id, maj = _run_actif_lot12(db_path)
    if not run_id:
        return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_LOT12, onglet=table,
                                      etat=ETAT_FICHIER_ABSENT))
    conn = get_db(db_path)
    try:
        lignes = [dict(r) for r in conn.execute(
            f"SELECT * FROM {table} WHERE run_id = ? ORDER BY id", (run_id,))]
    finally:
        conn.close()
    etat = ETAT_OK if lignes else ETAT_VIDE
    return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=SOURCE_LOT12, onglet=table,
                                  etat=etat, nb_lignes=len(lignes), derniere_maj=maj), lignes=lignes)


def factures_entetes(db_path=None) -> Source:
    return _src_sqlite_lot12("factures", "Factures propriétaires (entêtes)",
                             "lot12_prefactures_entete", db_path=db_path)


def dashboard_facturation(db_path=None) -> Source:
    return _src_sqlite_lot12("dashboard", "Tableau de bord facturation",
                             "lot12_dashboard_facturation", db_path=db_path)


def lignes_prefactures(db_path=None) -> Source:
    """FACT_FACTURE_LIGNES — les 12 (ou 13) lignes de chaque préfacture, run Lot12 actif."""
    return _src_sqlite_lot12("lignes_prefactures", "Lignes de préfacture",
                             "lot12_prefactures_lignes", db_path=db_path)


def controles_factures(db_path=None) -> Source:
    return _src_sqlite_lot12("controles", "Contrôles facturation", "lot12_a_controler",
                             db_path=db_path)


def ref_logements(db_path=None) -> Source:
    """Référentiel logements — SQLite (`ref_logements`, migration 0029), noms officiels
    (affichage). Ne lit plus `REF_Setup.xlsm`."""
    from app.services import ref_setup_repo

    lignes = ref_setup_repo.lire_onglet("REF_Logements", db_path=db_path)
    etat = ETAT_OK if lignes else ETAT_VIDE
    return Source(etat=EtatSource(cle="ref_logements", libelle="Référentiel logements",
                                  fichier="SQLite (REF_Setup)", onglet="ref_logements", etat=etat,
                                  nb_lignes=len(lignes)), lignes=lignes)


_CACHE_NOMS_LOGEMENTS: dict[str, str] | None = None


def noms_logements() -> dict[str, str]:
    """{logement_id: nom_logement_officiel} depuis REF_Setup. Mémorisé ; vidé par vider_cache."""
    global _CACHE_NOMS_LOGEMENTS
    if _CACHE_NOMS_LOGEMENTS is not None:
        return _CACHE_NOMS_LOGEMENTS
    idx: dict[str, str] = {}
    try:
        for r in ref_logements().lignes:
            lid = to_texte(r.get("logement_id"))
            if not lid or lid == "logement_id":
                continue
            nom = to_texte(r.get("nom_logement_officiel")) or to_texte(r.get("nom_court"))
            idx[lid] = nom or lid
    except Exception:
        idx = {}
    _CACHE_NOMS_LOGEMENTS = idx
    return idx


def libelle_logement(logement_id: str) -> str:
    """« Nom officiel » si connu, sinon repli « Logement non identifié — <id> »."""
    lid = to_texte(logement_id)
    if not lid:
        return ""
    nom = noms_logements().get(lid)
    return nom if nom else f"Logement non identifié — {lid}"


def etats_sources() -> list[EtatSource]:
    return [net_vue_mois().etat, net_reglement().etat, commissions().etat,
            resultats().etat, factures_entetes().etat, dashboard_facturation().etat]

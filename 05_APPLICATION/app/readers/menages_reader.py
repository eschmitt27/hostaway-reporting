"""Lecteur read-only ménages — Lot APP-2a.

Quatre blocs métier, quatre fichiers, jamais fusionnés :

  A. ATTENDU            — non alimenté par le moteur (voir plus bas).
  B. HOSTAWAY RÉALISÉ   — MASTER_FACT_HA_CleaningTasks_Discovery.xlsx (Lot6a).
                          Comptage opérationnel UNIQUEMENT. La colonne `cost`
                          d'Hostaway n'est jamais lue ni exposée : elle n'est pas
                          autoritaire et ne vaut aucun coût réel.
  C. INTERNE DÉCLARÉ    — MASTER_NORM_Declarations_Internes.xlsx (Lot6b).
                          Heures + taux internes.
  D. EXTERNE FACTURÉ    — MASTER_FACT_MEN_MenagesExternes.xlsx (Lot6c).
                          Le montant vient de la facture.

Sorties moteur consommées telles quelles :
  - MASTER_CTRL_Rapprochement_Menages.xlsx (Lot6d) — LA ligne de rapprochement,
    au grain (mois × logement × intervenant). C'est le moteur qui produit `ecart`,
    `statut_controle` et `code_controle` ; l'application ne les recalcule jamais.
  - MASTER_CALC_GainPerte_Menages.xlsx  (Lot6e) — coût standard vs coût réel.
  - MASTER_CALC_CoutComplet_Menages.xlsx (Lot6f) — coût complet analytique.
  - MASTER_CTRL_Coherence.xlsx (Lot11) — contrôles transverses du module ménages.

BLOC A — « ménages attendus » : le moteur ne produit AUCUN flux « attendu »
indépendant. D090 fixe la règle (« 1 réservation validée = 1 ménage attendu »)
mais aucun script ne matérialise cette colonne. Déduire un attendu ici
reviendrait à créer une règle de matching dans FastAPI : interdit. Le reader
expose donc le comptage Hostaway planifié produit par Lot6a (VUE_COMPTAGE :
nb_taches_total / réalisées / pending / annulées), explicitement nommé pour ce
qu'il est, et signale l'attendu métier comme NON_ALIMENTE.

Toutes les lectures passent par openpyxl read_only=True. Aucun handle d'écriture,
aucune normalisation métier : uniquement des conversions techniques d'affichage.
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers.excel_reader import list_sheets, read_sheet

# --- Onglets ---------------------------------------------------------------
SHEET_COMPARAISON = "TABLEAU_COMPARAISON"
SHEET_RESUME_APT = "RESUME_APPARTEMENT"
SHEET_RESUME_INT = "RESUME_INTERVENANT"
SHEET_CONTROLES = "CONTROLES"
SHEET_GAINPERTE = "DETAIL_ECART_COUT"
SHEET_COUTCOMPLET = "DETAIL_COUT_COMPLET"
SHEET_POOLS = "POOLS_CHARGES_MENAGE"
SHEET_HA_TASKS = "MASTER_ENRICHI"
SHEET_HA_COMPTAGE = "VUE_COMPTAGE"
SHEET_INTERNES = "MASTER_NORMALISE"
SHEET_EXTERNES = "MASTER"
SHEET_LOT11 = "MASTER"

# --- Noms de fichiers (affichés en interface — jamais de chemin absolu) -----
SOURCE_RAPPROCHEMENT = "MASTER_CTRL_Rapprochement_Menages.xlsx"
SOURCE_GAINPERTE = "MASTER_CALC_GainPerte_Menages.xlsx"
SOURCE_COUTCOMPLET = "MASTER_CALC_CoutComplet_Menages.xlsx"
SOURCE_HOSTAWAY = "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx"
SOURCE_INTERNES = "MASTER_NORM_Declarations_Internes.xlsx"
SOURCE_EXTERNES = "MASTER_FACT_MEN_MenagesExternes.xlsx"
SOURCE_LOT11 = "MASTER_CTRL_Coherence.xlsx"

# Alias historiques conservés (utilisés par les tests APP-2 d'origine).
MASTER_RAPPROCHEMENT_MENAGES = cfg.MASTER_RAPPROCHEMENT_MENAGES
MASTER_GAINPERTE_MENAGES = cfg.MASTER_GAINPERTE_MENAGES

# --- États de source -------------------------------------------------------
ETAT_OK = "OK"
ETAT_FICHIER_ABSENT = "FICHIER_ABSENT"
ETAT_ONGLET_ABSENT = "ONGLET_ABSENT"
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"
ETAT_NON_ALIMENTE = "NON_ALIMENTE"

_LIBELLES_ETAT = {
    ETAT_OK: "Alimentée",
    ETAT_FICHIER_ABSENT: "Fichier absent",
    ETAT_ONGLET_ABSENT: "Onglet absent",
    ETAT_VIDE: "Source vide",
    ETAT_ILLISIBLE: "Source illisible",
    ETAT_NON_ALIMENTE: "Non alimentée",
}


@dataclass(frozen=True)
class EtatSource:
    """État d'une source ménages. `fichier` est un NOM, jamais un chemin absolu."""
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
        return _LIBELLES_ETAT.get(self.etat, self.etat)


@dataclass
class SourceMenages:
    """Une source lue : ses lignes + son état. Les lignes sont vides si non disponible."""
    etat: EtatSource
    lignes: list[dict[str, Any]] = field(default_factory=list)


# --- Conversions techniques d'affichage ------------------------------------

def to_texte(v: Any) -> str:
    """Valeur affichable. Ne fabrique aucune donnée : None → chaîne vide."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, date):
        return v.isoformat()
    return str(v).strip()


def to_nombre(v: Any) -> float | int | None:
    """Nombre affichable, ou None. Ne remplace jamais une absence par 0."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return v
    try:
        texte = str(v).strip().replace(" ", "").replace(",", ".")
        return float(texte) if "." in texte else int(texte)
    except (TypeError, ValueError):
        return None


def to_mois(v: Any) -> str:
    """Mois AAAA-MM tel que produit par le moteur. Tronque un datetime éventuel."""
    if isinstance(v, (datetime, date)):
        return f"{v.year:04d}-{v.month:02d}"
    return to_texte(v)[:7]


def _mtime(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    except OSError:
        return None


# Cache de lecture. Clé = (fichier, onglet, mtime_ns, taille) : une source régénérée
# par le moteur produit une clé différente, donc une relecture.
_CACHE: dict[tuple[str, str, int, int], SourceMenages] = {}


def vider_cache() -> None:
    """Vide le cache de lecture (MASTER + référentiel propriétaires).

    Utilisé par « Actualiser l'affichage » et par les tests qui réécrivent une fixture :
    la prochaine lecture rouvre les fichiers et reflète l'état courant du disque.
    """
    global _CACHE_PROPRIETAIRES
    _CACHE.clear()
    _CACHE_PROPRIETAIRES = None


def _lire(cle: str, libelle: str, path: Path, fichier: str, onglet: str) -> SourceMenages:
    """Lecture d'un onglet avec diagnostic explicite : fichier / onglet / vide.

    Le résultat est mémorisé tant que le fichier n'a pas bougé (mtime + taille).
    Un MASTER régénéré par le moteur invalide donc l'entrée automatiquement.
    """
    if not path.exists():
        return SourceMenages(EtatSource(cle, libelle, fichier, onglet, ETAT_FICHIER_ABSENT))

    stat = path.stat()
    empreinte = (str(path), onglet, stat.st_mtime_ns, stat.st_size)
    memo = _CACHE.get(empreinte)
    if memo is not None:
        return SourceMenages(memo.etat, memo.lignes)

    maj = _mtime(path)
    feuilles = list_sheets(path)
    if not feuilles:
        resultat = SourceMenages(
            EtatSource(cle, libelle, fichier, onglet, ETAT_ILLISIBLE, derniere_maj=maj)
        )
    elif onglet not in feuilles:
        resultat = SourceMenages(
            EtatSource(cle, libelle, fichier, onglet, ETAT_ONGLET_ABSENT, derniere_maj=maj)
        )
    else:
        lignes = read_sheet(path, onglet, max_rows=None)
        if not lignes:
            resultat = SourceMenages(
                EtatSource(cle, libelle, fichier, onglet, ETAT_VIDE, derniere_maj=maj)
            )
        else:
            resultat = SourceMenages(
                EtatSource(cle, libelle, fichier, onglet, ETAT_OK, len(lignes), maj), lignes
            )

    _CACHE[empreinte] = resultat
    return SourceMenages(resultat.etat, resultat.lignes)


def _table_presente(conn, table: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _lire_sqlite(cle: str, libelle: str, table: str, db_path=None) -> SourceMenages:
    """Lecture SQLite d'une sortie Lot6a/6b/6d/6e/6f (migration 0038) : équivalent de `_lire`, même
    contrat `SourceMenages`/`EtatSource`, mais SANS repli Excel — une table absente ou vide est un
    état affiché, jamais une exception (`FileNotFoundError` interdite en amont, mission §21).

    `fichier`/`onglet` de l'`EtatSource` portent le nom de la table : ce ne sont plus des chemins
    Excel, mais l'écran qui affiche l'état de la source n'a pas besoin de le savoir.

    `db_path` optionnel : les écrans n'en passent pas (ils lisent `cfg.DB_PATH`), mais un appelant
    qui travaille sur une base précise — parité, recette, run isolé — doit pouvoir l'imposer.
    Sans lui, un service appelé avec `db_path=` lirait quand même la base globale.
    """
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, table):
            return SourceMenages(EtatSource(cle, libelle, table, table, ETAT_FICHIER_ABSENT))
        lignes = [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY id")]
    finally:
        conn.close()
    if not lignes:
        return SourceMenages(EtatSource(cle, libelle, table, table, ETAT_VIDE))
    maj = max((r.get("date_calcul") or "" for r in lignes), default=None) or None
    return SourceMenages(
        EtatSource(cle, libelle, table, table, ETAT_OK, len(lignes), maj), lignes)


# --- Sources ---------------------------------------------------------------

def rapprochement() -> SourceMenages:
    """Lot6d — la ligne de rapprochement (mois × logement × intervenant). SQLite (0038), sans
    repli Excel : `menages_rapprochement` porte exactement les mêmes colonnes que
    TABLEAU_COMPARAISON, moteur inchangé, seule l'entrée/sortie change de support."""
    return _lire_sqlite("rapprochement", "Rapprochement (Lot6d)", "menages_rapprochement")


def controles_rapprochement() -> SourceMenages:
    """Résumé par code de contrôle — dérivé de `menages_rapprochement` (0038), jamais recalculé :
    lot6d a DÉJÀ décidé `code_controle`/`statut_controle` par ligne (mission Bloc B §7), ce résumé
    ne fait que les grouper. C'est exactement pourquoi 0038 n'a pas dupliqué l'onglet CONTROLES :
    un agrégat dérivable de `menages_rapprochement`, pas une seconde source."""
    base = rapprochement()
    if base.etat.etat != ETAT_OK:
        return SourceMenages(EtatSource("controles_rapprochement", "Contrôles rapprochement (Lot6d)",
                                        base.etat.fichier, base.etat.onglet, base.etat.etat))
    groupes: dict[str, list[dict]] = {}
    for r in base.lignes:
        code = to_texte(r.get("code_controle"))
        if code:
            groupes.setdefault(code, []).append(r)
    lignes = [{
        "code_controle": code, "niveau": to_texte(grp[0].get("statut_controle")),
        "nb": len(grp), "exemple": to_texte(grp[0].get("logement_id")),
    } for code, grp in sorted(groupes.items())]
    return SourceMenages(
        EtatSource("controles_rapprochement", "Contrôles rapprochement (Lot6d)",
                  base.etat.fichier, base.etat.onglet, ETAT_OK, len(lignes)),
        lignes)


def gainperte() -> SourceMenages:
    """Lot6e — coût standard vs coût réel. SQLite (0038), sans repli Excel."""
    return _lire_sqlite("gainperte", "Gain / perte (Lot6e)", "menages_gainperte")


def cout_complet() -> SourceMenages:
    """Lot6f — coût complet analytique (direct + quotes-parts de charges). SQLite (0038)."""
    return _lire_sqlite("coutcomplet", "Coût complet (Lot6f)", "menages_cout_complet")


# LEGACY_DEAD_CODE : aucun appelant (vérifié par grep, mission Ménages §2). Non migré vers SQLite
# volontairement — construire une persistance sans consommateur n'a pas de sens. À supprimer avec
# la prochaine passe de nettoyage du reader.
def pools_charges() -> SourceMenages:
    return _lire("pools", "Pools de charges ménage (Lot6f)",
                 cfg.MASTER_COUTCOMPLET_MENAGES, SOURCE_COUTCOMPLET, SHEET_POOLS)


def hostaway_taches() -> SourceMenages:
    """Lot6a — tâches Hostaway. Comptage opérationnel, jamais valorisation. SQLite (`--source
    SQLITE`, migration 0038) : plus de repli Excel, la table est la seule source lue ici."""
    return _lire_sqlite("hostaway_taches", "Tâches Hostaway (Lot6a)", "menages_taches_enrichies")


def hostaway_comptage(db_path=None) -> SourceMenages:
    """Lot6a VUE_COMPTAGE — planifié / réalisé / pending / annulé par logement et mois.

    `menages_taches_enrichies` (0038) est au grain TÂCHE, pas au grain agrégé : cet agrégat n'est
    volontairement pas dupliqué en table SQLite (il se déduit, comme tout agrégat de cette
    migration — cf. 0038, « pas de registre de datasets »). Même règle de comptage que
    `lot6a_cleaning_tasks_comptage.build_comptage_rows` : compte_comme_menage=OUI pour réalisé/
    prévu/pending, les annulés comptent quel que soit compte_comme_menage.
    """
    source = _lire_sqlite("hostaway_comptage", "Comptage Hostaway (Lot6a)",
                          "menages_taches_enrichies", db_path=db_path)
    if source.etat.etat != ETAT_OK:
        return SourceMenages(EtatSource("hostaway_comptage", "Comptage Hostaway (Lot6a)",
                                        source.etat.fichier, source.etat.onglet,
                                        source.etat.etat, derniere_maj=source.etat.derniere_maj))

    def _cnt(groupe, statut_val, ccm_filter=True):
        return sum(1 for r in groupe if to_texte(r.get("statut_menage")) == statut_val
                  and (not ccm_filter or to_texte(r.get("compte_comme_menage")) == "OUI"))

    groupes: dict[tuple[str, str], list[dict]] = {}
    for r in source.lignes:
        groupes.setdefault((to_mois(r.get("mois")), to_texte(r.get("logement_id"))), []).append(r)

    lignes = []
    for (mois, logement_id), grp in sorted(groupes.items()):
        prop_id = next((r.get("proprietaire_id") for r in grp if r.get("proprietaire_id")), None)
        lignes.append({
            "mois": mois, "logement_id": logement_id, "proprietaire_id": prop_id,
            "nb_menages_realises": _cnt(grp, "réalisé"),
            "nb_menages_confirmes": _cnt(grp, "prévu"),
            "nb_menages_pending": _cnt(grp, "A_CONTROLER"),
            "nb_menages_annules": _cnt(grp, "annulé", ccm_filter=False),
            "nb_taches_total": len(grp),
            "statut_controle": "BLOQUANT" if not logement_id else "OK",
            "niveau_anomalie": "BLOQUANT" if not logement_id else "",
            "code_anomalie": "COMPTAGE_LOGEMENT_ABSENT" if not logement_id else "",
        })
    return SourceMenages(
        EtatSource("hostaway_comptage", "Comptage Hostaway (Lot6a)", source.etat.fichier,
                  source.etat.onglet, ETAT_OK, len(lignes), source.etat.derniere_maj),
        lignes)


def internes() -> SourceMenages:
    """Lot6b — déclarations internes M04. SQLite (migration 0038), plus de repli Excel."""
    return _lire_sqlite("internes", "Déclarations internes M04 (Lot6b)",
                        "menages_declarations_internes")


def externes() -> SourceMenages:
    """Lot6c — ménages externes facturés. SQLite (0037/0039/0040 : facture_lignes_menage +
    compagnons), sans repli Excel. Pont : `facture_menage_pdf_service` (PDF → SQLite direct,
    mission précédente) alimente ces tables à l'import, `facture_lignes_menage_service.
    lignes_externes_pour_reader` rend le même grain/mêmes noms de champs que l'ancien onglet MASTER."""
    from app.services import facture_lignes_menage_service as flm
    lignes = flm.lignes_externes_pour_reader()
    if not lignes:
        return SourceMenages(EtatSource("externes", "Ménages externes facturés (Lot6c)",
                                        "facture_lignes_menage", "facture_lignes_menage",
                                        ETAT_VIDE))
    return SourceMenages(
        EtatSource("externes", "Ménages externes facturés (Lot6c)", "facture_lignes_menage",
                  "facture_lignes_menage", ETAT_OK, len(lignes)),
        lignes)


SHEET_DIAGNOSTIC_PDF = "DIAGNOSTIC_PDF"


def diagnostic_pdf() -> SourceMenages:
    """Lot6c — diagnostic d'extraction PDF, un par tentative d'import. SQLite (0040)."""
    source = _lire_sqlite("diagnostic_pdf", "Diagnostic extraction PDF (Lot6c)",
                          "facture_pdf_diagnostics")
    lignes = [{
        "nom_fichier": r.get("nom_fichier"), "format_detecte": r.get("format_detecte"),
        "statut_extraction": r.get("statut_extraction"), "numero_facture": r.get("numero_facture"),
        "montant_total": r.get("montant_total"), "nb_lignes": r.get("nb_lignes"),
        "ecart_reconciliation": r.get("ecart_reconciliation"), "doublon_de": r.get("doublon_de"),
        "anomalies": r.get("anomalies"),
    } for r in source.lignes]
    return SourceMenages(source.etat, lignes)


def mode_extraction_externes() -> str | None:
    """Mode d'extraction de la dernière tentative d'import PDF connue (0040) — plus lu dans
    `source_document` d'un classeur, directement la colonne dédiée `mode_extraction`."""
    conn = get_db()
    try:
        if not _table_presente(conn, "facture_pdf_diagnostics"):
            return None
        row = conn.execute(
            "SELECT mode_extraction FROM facture_pdf_diagnostics ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["mode_extraction"] if row else None
    finally:
        conn.close()


def controles_lot11() -> SourceMenages:
    """Lot11 — contrôles transverses. SQLite (`controles_lot11_constats`, 0041), alimentée par
    `controles_lot11_service.construire` (moteur SQLite natif — seule source depuis la fermeture
    complète de Lot11). Jamais lu depuis Excel. Filtrés sur le module ménages par le service."""
    return _lire_sqlite("lot11", "Contrôles de cohérence (Lot11)", "controles_lot11_constats")


# --- Référentiel propriétaires (libellés d'affichage « Prénom NOM ») ----------
SHEET_REF_PROPRIETAIRES = "REF_Proprietaires"

_CACHE_PROPRIETAIRES: dict[str, str] | None = None


def noms_proprietaires() -> dict[str, str]:
    """{proprietaire_id: "Prénom NOM"} depuis REF_Setup (référentiel officiel).

    Sert à afficher un nom lisible partout où le moteur ne porte que l'identifiant
    technique. Lecture seule, mémorisée pour la durée du process ; `vider_cache`
    la réinitialise. En cas de source absente/illisible, rend un dict vide (l'appelant
    retombe alors sur un libellé de repli, jamais une invention).
    """
    global _CACHE_PROPRIETAIRES
    if _CACHE_PROPRIETAIRES is not None:
        return _CACHE_PROPRIETAIRES
    resultat: dict[str, str] = {}
    if cfg.REF_SETUP.exists():
        try:
            for r in read_sheet(cfg.REF_SETUP, SHEET_REF_PROPRIETAIRES, max_rows=None):
                pid = str(r.get("proprietaire_id") or "").strip()
                if not pid or pid.startswith("["):
                    continue
                prenom = str(r.get("prenom_proprietaire") or "").strip()
                nom = str(r.get("nom_proprietaire") or "").strip()
                libelle = " ".join(x for x in (prenom, nom) if x).strip()
                resultat[pid] = libelle or pid
        except Exception:
            resultat = {}
    _CACHE_PROPRIETAIRES = resultat
    return resultat


def libelle_proprietaire(prop_id: str) -> str:
    """« Prénom NOM » si connu, sinon repli explicite « Propriétaire non identifié — <id> »."""
    pid = str(prop_id or "").strip()
    if not pid:
        return ""
    nom = noms_proprietaires().get(pid)
    return nom if nom else f"Propriétaire non identifié — {pid}"


def attendu() -> EtatSource:
    """Bloc A — aucun flux « ménages attendus » n'est produit par le moteur.

    Ne rien déduire ici : construire un attendu depuis les réservations serait une
    nouvelle règle de matching côté application. Le module l'affiche comme non
    alimenté tant que le moteur ne produit pas la colonne.
    """
    return EtatSource(
        cle="attendu",
        libelle="Ménages attendus (règle réservation / check-out)",
        fichier="—",
        onglet="—",
        etat=ETAT_NON_ALIMENTE,
    )


def etats_sources() -> list[EtatSource]:
    """État des sources, dans l'ordre des 4 blocs métier puis des calculs."""
    return [
        attendu(),
        hostaway_taches().etat,
        internes().etat,
        externes().etat,
        rapprochement().etat,
        gainperte().etat,
        cout_complet().etat,
        controles_lot11().etat,
    ]


# --- Compatibilité APP-2 (tests et service d'origine) -----------------------

def rapprochement_available() -> bool:
    return rapprochement().etat.disponible


def gainperte_available() -> bool:
    return gainperte().etat.disponible


def read_tableau_comparaison() -> list[dict[str, Any]]:
    return rapprochement().lignes


def read_controles() -> list[dict[str, Any]]:
    return controles_rapprochement().lignes


# LEGACY_DEAD_CODE : aucun appelant (vérifié par grep, mission Ménages §2). Idem `pools_charges`.
def read_resume_appartement() -> list[dict[str, Any]]:
    return _lire("resume_apt", "Résumé par appartement (Lot6d)",
                 cfg.MASTER_RAPPROCHEMENT_MENAGES, SOURCE_RAPPROCHEMENT, SHEET_RESUME_APT).lignes


# LEGACY_DEAD_CODE : aucun appelant (vérifié par grep, mission Ménages §2). Idem `pools_charges`.
def read_resume_intervenant() -> list[dict[str, Any]]:
    return _lire("resume_int", "Résumé par intervenant (Lot6d)",
                 cfg.MASTER_RAPPROCHEMENT_MENAGES, SOURCE_RAPPROCHEMENT, SHEET_RESUME_INT).lignes


def read_gainperte_detail() -> list[dict[str, Any]]:
    return gainperte().lignes


def _cle(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        to_mois(row.get("mois")),
        to_texte(row.get("logement_id")),
        to_texte(row.get("intervenant_id")),
    )


def find_ligne(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    cible = (str(mois).strip(), str(logement_id).strip(), str(intervenant_id).strip())
    for row in read_tableau_comparaison():
        if _cle(row) == cible:
            return row
    return None


def find_gainperte(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    cible = (str(mois).strip(), str(logement_id).strip(), str(intervenant_id).strip())
    for row in read_gainperte_detail():
        if _cle(row) == cible:
            return row
    return None


def find_cout_complet(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    cible = (str(mois).strip(), str(logement_id).strip(), str(intervenant_id).strip())
    for row in cout_complet().lignes:
        if _cle(row) == cible:
            return row
    return None

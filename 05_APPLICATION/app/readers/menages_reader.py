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


# --- Sources ---------------------------------------------------------------

def rapprochement() -> SourceMenages:
    """Lot6d — la ligne de rapprochement (mois × logement × intervenant)."""
    return _lire("rapprochement", "Rapprochement (Lot6d)",
                 cfg.MASTER_RAPPROCHEMENT_MENAGES, SOURCE_RAPPROCHEMENT, SHEET_COMPARAISON)


def controles_rapprochement() -> SourceMenages:
    return _lire("controles_rapprochement", "Contrôles rapprochement (Lot6d)",
                 cfg.MASTER_RAPPROCHEMENT_MENAGES, SOURCE_RAPPROCHEMENT, SHEET_CONTROLES)


def gainperte() -> SourceMenages:
    """Lot6e — coût standard vs coût réel."""
    return _lire("gainperte", "Gain / perte (Lot6e)",
                 cfg.MASTER_GAINPERTE_MENAGES, SOURCE_GAINPERTE, SHEET_GAINPERTE)


def cout_complet() -> SourceMenages:
    """Lot6f — coût complet analytique (direct + quotes-parts de charges)."""
    return _lire("coutcomplet", "Coût complet (Lot6f)",
                 cfg.MASTER_COUTCOMPLET_MENAGES, SOURCE_COUTCOMPLET, SHEET_COUTCOMPLET)


def pools_charges() -> SourceMenages:
    return _lire("pools", "Pools de charges ménage (Lot6f)",
                 cfg.MASTER_COUTCOMPLET_MENAGES, SOURCE_COUTCOMPLET, SHEET_POOLS)


def hostaway_taches() -> SourceMenages:
    """Lot6a — tâches Hostaway. Comptage opérationnel, jamais valorisation."""
    return _lire("hostaway_taches", "Tâches Hostaway (Lot6a)",
                 cfg.MASTER_HA_CLEANINGTASKS, SOURCE_HOSTAWAY, SHEET_HA_TASKS)


def hostaway_comptage() -> SourceMenages:
    """Lot6a VUE_COMPTAGE — planifié / réalisé / pending / annulé par logement et mois."""
    return _lire("hostaway_comptage", "Comptage Hostaway (Lot6a)",
                 cfg.MASTER_HA_CLEANINGTASKS, SOURCE_HOSTAWAY, SHEET_HA_COMPTAGE)


def internes() -> SourceMenages:
    """Lot6b — déclarations internes M04."""
    return _lire("internes", "Déclarations internes M04 (Lot6b)",
                 cfg.MASTER_DECLARATIONS_INTERNES, SOURCE_INTERNES, SHEET_INTERNES)


def externes() -> SourceMenages:
    """Lot6c — ménages externes facturés."""
    return _lire("externes", "Ménages externes facturés (Lot6c)",
                 cfg.MASTER_MENAGES_EXTERNES, SOURCE_EXTERNES, SHEET_EXTERNES)


SHEET_DIAGNOSTIC_PDF = "DIAGNOSTIC_PDF"


def diagnostic_pdf() -> SourceMenages:
    """Lot6c — onglet DIAGNOSTIC_PDF (une ligne par PDF analysé). Vide si extraction non exécutée."""
    return _lire("diagnostic_pdf", "Diagnostic extraction PDF (Lot6c)",
                 cfg.MASTER_MENAGES_EXTERNES, SOURCE_EXTERNES, SHEET_DIAGNOSTIC_PDF)


def mode_extraction_externes() -> str | None:
    """Mode d'extraction du MASTER Lot6c, lu dans `source_document` (…— PDF_AUTOMATIQUE / …— SAISIE_MANUELLE_SECOURS)."""
    src = externes()
    for r in src.lignes:
        sd = to_texte(r.get("source_document"))
        if "PDF_AUTOMATIQUE" in sd:
            return "PDF_AUTOMATIQUE"
        if "SAISIE_MANUELLE_SECOURS" in sd:
            return "SAISIE_MANUELLE_SECOURS"
        if sd:
            return "INCONNU"
    return None


def controles_lot11() -> SourceMenages:
    """Lot11 — contrôles transverses. Filtrés sur le module ménages par le service."""
    return _lire("lot11", "Contrôles de cohérence (Lot11)",
                 cfg.MASTER_CTRL_COHERENCE, SOURCE_LOT11, SHEET_LOT11)


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
    return cfg.MASTER_RAPPROCHEMENT_MENAGES.exists()


def gainperte_available() -> bool:
    return cfg.MASTER_GAINPERTE_MENAGES.exists()


def read_tableau_comparaison() -> list[dict[str, Any]]:
    return rapprochement().lignes


def read_controles() -> list[dict[str, Any]]:
    return controles_rapprochement().lignes


def read_resume_appartement() -> list[dict[str, Any]]:
    return _lire("resume_apt", "Résumé par appartement (Lot6d)",
                 cfg.MASTER_RAPPROCHEMENT_MENAGES, SOURCE_RAPPROCHEMENT, SHEET_RESUME_APT).lignes


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

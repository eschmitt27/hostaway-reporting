"""Lecteur du référentiel pour APP-2b — validation de la saisie HH contrôlée.

Séparé de l'enrichissement fiche logement (APP-1) : ce module a le droit de lire
`REF_Gestion_Logements_Hist` et `REF_Proprietaires`, que l'autre s'interdisait.

MIGRATION EXCEL → SQLITE
Par défaut, ces fonctions lisent le référentiel SQLITE (`ref_*`, migration 0029) via
`ref_setup_repo`. `REF_Setup.xlsm` n'est plus ouvert au runtime.

POURQUOI `ref_setup_path` SUBSISTE — ET POURQUOI CE N'EST PAS UN REPLI EXCEL
Les flux « préparation / écriture réelle sur COPIE » (`saisie_hh_dryrun_service`,
`saisie_hh_real_write_service`, `saisie_hh_schema_real_prepare_service`) simulent une opération sur
une copie de travail du classeur, et doivent donc lire CETTE copie — pas la base. Le paramètre sert
uniquement à cela : il est fourni EXPLICITEMENT par un appelant qui manipule un fichier de travail.

Ce n'est pas le repli interdit par la règle « SQLite sinon Excel » : ce repli-là consisterait à
rouvrir le classeur QUAND LA BASE EST VIDE, ce qui masquerait un référentiel non initialisé. Ici,
sans chemin explicite, la lecture est SQLite et un référentiel absent se voit (liste vide), il n'est
jamais compensé en douce par un fichier.
"""
from pathlib import Path
from typing import Any

from app.readers.excel_reader import read_sheet
from app.services import ref_setup_repo as repo

_SHEET_CLOTURE = "REF_Cloture_Mensuelle"
_SHEET_GESTION = "REF_Gestion_Logements_Hist"
_SHEET_LOGEMENTS = "REF_Logements"
_SHEET_PROPRIETAIRES = "REF_Proprietaires"
_SHEET_ASSOCIES = "REF_Associes"
_SHEET_CANAUX_RESERVATION = "REF_Canaux_Reservation"
_SHEET_MODES_PAIEMENT = "REF_Modes_Paiement"
_SHEET_CODES_IMPACT = "REF_Codes_Impact"
_SHEET_TAUX_COMMISSION = "REF_Taux_Commission"
_SHEET_COUTS_STANDARDS_MENAGE = "REF_Couts_Standards_Menage"


def _lire(onglet: str, ref_setup_path: Path | None, *, db_path=None) -> list[dict[str, Any]]:
    """SQLite par défaut ; le classeur UNIQUEMENT si un chemin de travail est fourni."""
    if ref_setup_path is not None:
        return read_sheet(Path(ref_setup_path), onglet, max_rows=None)
    return repo.lire_onglet(onglet, db_path=db_path)


def get_all_logements_hh(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Logements (APP-2b : éligibilité logement D8)."""
    return _lire(_SHEET_LOGEMENTS, ref_setup_path, db_path=db_path)


def get_gestion_hist(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Gestion_Logements_Hist (APP-2b : D7/D8)."""
    return _lire(_SHEET_GESTION, ref_setup_path, db_path=db_path)


def get_all_proprietaires_hh(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Proprietaires (APP-2b : divergence D9)."""
    return _lire(_SHEET_PROPRIETAIRES, ref_setup_path, db_path=db_path)


def get_all_associes(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Associes (APP-2b : D11 associé récupérateur)."""
    return _lire(_SHEET_ASSOCIES, ref_setup_path, db_path=db_path)


def get_canaux(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_CANAUX_RESERVATION, _lire(_SHEET_CANAUX_RESERVATION, ref_setup_path,
                                            db_path=db_path)


def get_modes_paiement(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_MODES_PAIEMENT, _lire(_SHEET_MODES_PAIEMENT, ref_setup_path, db_path=db_path)


def get_codes_impact(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_CODES_IMPACT, _lire(_SHEET_CODES_IMPACT, ref_setup_path, db_path=db_path)


def get_taux_commission(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    return _lire(_SHEET_TAUX_COMMISSION, ref_setup_path, db_path=db_path)


def get_couts_standards_menage(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    return _lire(_SHEET_COUTS_STANDARDS_MENAGE, ref_setup_path, db_path=db_path)


def get_cloture_mois(mois_str: str, ref_setup_path: Path | None = None, *,
                     db_path=None) -> dict[str, Any] | None:
    """Ligne de REF_Cloture_Mensuelle pour le mois donné (AAAA-MM), ou None si absent."""
    target = str(mois_str).strip()
    for row in _lire(_SHEET_CLOTURE, ref_setup_path, db_path=db_path):
        if str(row.get("mois", "")).strip() == target:
            return row
    return None


# ── Ouverture d'un mois à la saisie — « non déclaré » n'est PAS « fermé » ────────────────────────
#
# BUG RÉEL CORRIGÉ (recette utilisateur n°3, §4/§56). `ref_cloture_mensuelle` s'arrêtait à 2026-06
# (« Mois courant — source live »), figé à la date où il avait été peuplé. Le contrôle D10 lisait
# `get_cloture_mois(mois) is None` comme « mois non ouvert » et REFUSAIT donc toute réservation de
# juillet, août et septembre 2026 : plus aucune saisie hors Hostaway n'était possible depuis trois
# mois, sans que rien ne soit cassé côté code — le référentiel devait simplement être avancé à la
# main chaque mois, et ne l'avait pas été.
#
# La règle métier juste est asymétrique : une clôture est un ACTE EXPLICITE et tracé ; son absence
# ne vaut pas clôture. Un mois jamais déclaré qui se situe APRÈS la frontière de clôture est donc
# ouvert (il n'a jamais été fermé) ; un mois jamais déclaré situé DERRIÈRE cette frontière reste
# refusé — sinon on pourrait antidater une saisie dans une période déjà arrêtée, ce que le contrôle
# D10 existe précisément pour empêcher.
#
# Conséquence voulue : le mois courant est saisissable sans intervention, et il ne devient clos que
# lorsqu'une clôture réelle est prononcée. Le moteur ne clôture toujours rien de lui-même (§56).
STATUT_MOIS_CLOTURE = "CLOTURE"


def get_cloture_rows(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Cloture_Mensuelle — nécessaire pour situer un mois par rapport à
    la frontière de clôture (`mois_ouvert_pour_saisie`), là où `get_cloture_mois` ne rend que le
    mois demandé et ne peut donc pas distinguer « jamais déclaré » de « antérieur à une clôture »."""
    return _lire(_SHEET_CLOTURE, ref_setup_path, db_path=db_path)


def mois_ouvert_pour_saisie(mois_str: str, cloture_rows: list[dict[str, Any]]) -> tuple[bool, str]:
    """Le mois `AAAA-MM` accepte-t-il une saisie ? Rend (ouvert, code_refus).

    Fonction PURE (aucune I/O) : testable sans base, et partagée par la validation et l'UI.
    `code_refus` vaut "" si ouvert, sinon `MOIS_CLOTURE` ou `MOIS_ANTERIEUR_A_CLOTURE`.
    """
    mois = str(mois_str).strip()
    if not mois:
        return False, "MOIS_ABSENT"

    statuts = {
        str(r.get("mois", "")).strip(): str(r.get("statut_mois", "")).strip().upper()
        for r in cloture_rows
        if str(r.get("mois", "")).strip()
    }
    statut = statuts.get(mois)
    if statut is not None:
        if statut == STATUT_MOIS_CLOTURE:
            return False, "MOIS_CLOTURE"
        return True, ""

    # Mois jamais déclaré : ouvert s'il est postérieur au dernier mois réellement CLÔTURÉ.
    clos = sorted(m for m, s in statuts.items() if s == STATUT_MOIS_CLOTURE)
    if clos and mois <= clos[-1]:
        return False, "MOIS_ANTERIEUR_A_CLOTURE"
    return True, ""


# Horizon de la fenêtre proposée à l'écran. Le backend, lui, n'a AUCUNE borne vers l'avenir : tout
# mois postérieur à la dernière clôture est ouvert (`mois_ouvert_pour_saisie`). Mais une liste
# affichable doit bien s'arrêter quelque part ; deux ans couvrent très largement les réservations
# prises à l'avance. Au-delà, c'est toujours D10 qui tranche — jamais cette borne d'affichage.
HORIZON_MOIS_OUVERTS = 24

# Garde-fou d'énumération : un référentiel contenant un mois très ancien ne doit pas faire boucler
# l'écran sur des milliers d'itérations pour un résultat que personne ne lira.
_MAX_MOIS_ENUMERES = 600


def mois_suivant(mois: str) -> str:
    """`2026-12` → `2027-01`. Le mois est une chaîne AAAA-MM, jamais une date."""
    annee, numero = int(str(mois)[:4]), int(str(mois)[5:7])
    return f"{annee + 1:04d}-01" if numero == 12 else f"{annee:04d}-{numero + 1:02d}"


def mois_ouverts_pour_saisie(mois_courant: str, cloture_rows: list[dict[str, Any]],
                             horizon: int = HORIZON_MOIS_OUVERTS) -> list[str]:
    """Liste FINIE des mois saisissables, pour que l'écran dise exactement ce que dit le backend.

    Fonction PURE, bâtie sur `mois_ouvert_pour_saisie` : elle n'a pas sa propre règle d'ouverture,
    elle ne fait qu'énumérer des candidats et lui demander, mois par mois, s'il accepte une saisie.
    Toute divergence UI/backend devient ainsi structurellement impossible.

    BUG RÉEL CORRIGÉ : l'écran de saisie n'énumérait les mois ouverts que JUSQU'AU mois courant, et
    désactivait son bouton pour tout mois absent de cette liste. Une réservation d'octobre saisie en
    septembre était donc refusée à l'écran (« n'est pas ouvert dans le référentiel de clôture »)
    alors que le backend l'acceptait sans réserve — une réservation se prend par définition AVANT le
    séjour.
    """
    courant = str(mois_courant).strip()
    if not courant:
        return []
    declares = sorted(str(r.get("mois", "")).strip() for r in cloture_rows
                      if str(r.get("mois", "")).strip())
    curseur = min(declares[0], courant) if declares else courant
    fin = courant
    for _ in range(horizon):
        fin = mois_suivant(fin)

    ouverts: list[str] = []
    for _ in range(_MAX_MOIS_ENUMERES):
        if curseur > fin:
            break
        if mois_ouvert_pour_saisie(curseur, cloture_rows)[0]:
            ouverts.append(curseur)
        curseur = mois_suivant(curseur)
    return ouverts

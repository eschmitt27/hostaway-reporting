"""APP-3b / commit 3 — Orchestrateur TRANSACTIONNEL de l'écriture d'une charge.

Deux classeurs sont concernés par une charge, et `os.replace` n'est atomique que **par fichier** :
il n'existe aucune atomicité entre deux fichiers. L'atomicité est donc reconstruite au niveau
applicatif — préparer tout, ne rien remplacer tant que tout n'est pas prêt, et restaurer ce qui a
déjà été remplacé si la fin échoue.

    SAISIE_Charges_Impacts.xlsx   (ordre 1)  — AFFECTATIONS / MENAGE / RESERVE
    SAISIE_Charges_Flux.xlsx      (ordre 2)  — la ligne de charge

**L'ordre n'est pas arbitraire.** Les impacts sont remplacés d'abord : si le second remplacement
échoue et que le rollback échoue lui aussi, on laisse des impacts *orphelins* (aucune charge ne les
référence, donc invisibles pour les lots) plutôt qu'une charge *sans impacts* — qui serait, elle,
comptée à tort par le résultat. Le pire cas est choisi, pas subi.

Lot7 (`MASTER_FACT_MAN_IK_Avantages`) est **hors transaction** : `file_registry` interdit à l'app
d'écrire dans `02_TRAVAIL` / `MASTER_*`. L'avantage associé est porté par la colonne
`avantage_associe_id` de la ligne de charge ; Lot7 se régénère ensuite par le moteur.

Séquence (chaque étape est une fonction distincte, monkeypatchable) :
    1. garde des flags          — refus AVANT toute préparation
    2. validation de la demande
    3. préparation des temporaires (writer bas niveau — aucun remplacement)
    4. validation des temporaires (existence, lisibilité, empreinte, même volume)
    5. sauvegardes techniques    — créées AVANT le premier remplacement
    6. commit                    — os.replace, dans l'ordre déterministe (SEUL endroit du projet)
    7. vérification post-commit  — fichiers finaux lisibles et conformes aux temporaires
    8. rollback si échec         — restauration vérifiée par SHA256
    9. nettoyage                 — temporaires puis sauvegardes ; jamais avant la validation complète

Garde-fous : tant que `CHARGES_REAL_WRITE_ENABLED` **et**
`CHARGES_REAL_WRITE_CONFIRMATION_ENABLED` ne sont pas tous deux à True, la transaction est refusée
avant qu'un seul octet d'un fichier réel ne soit touché. L'orchestrateur ne contourne jamais ces flags.
"""
from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg
from app.writers import saisie_charges_writer as writer
from app.writers.saisie_hh_writer import _same_volume, _sha256

# Ordre déterministe des remplacements (voir docstring : impacts avant charge).
ORDRE_IMPACTS = 1
ORDRE_FLUX = 2

STATUT_OK = "OK"
STATUT_GARDE = "GARDE_SECURITE"
STATUT_ERREUR = "ERREUR"
STATUT_ROLLBACK = "ROLLBACK"

E_FLAGS_DESACTIVES = "E_FLAGS_DESACTIVES"
E_DEMANDE_INVALIDE = "E_DEMANDE_INVALIDE"
E_PREPARATION = "E_PREPARATION"
E_TEMPORAIRE_MANQUANT = "E_TEMPORAIRE_MANQUANT"
E_TEMPORAIRE_INVALIDE = "E_TEMPORAIRE_INVALIDE"
E_SAUVEGARDE = "E_SAUVEGARDE"
E_REMPLACEMENT = "E_REMPLACEMENT"
E_POST_COMMIT = "E_POST_COMMIT"


class RollbackCritiqueError(RuntimeError):
    """Le rollback n'a pas pu restaurer tous les fichiers déjà remplacés.

    État NON récupérable automatiquement. Les sauvegardes des fichiers non restaurés sont
    CONSERVÉES et listées ici : elles sont la seule voie de retour. Ne jamais les supprimer.
    """

    def __init__(self, fichiers_non_restaures: list[dict[str, str]], cause: str) -> None:
        self.fichiers_non_restaures = fichiers_non_restaures
        self.cause = cause
        details = "; ".join(
            f"{f['cible']} (sauvegarde conservée : {f['sauvegarde']})"
            for f in fichiers_non_restaures
        )
        super().__init__(
            f"ROLLBACK INCOMPLET — {len(fichiers_non_restaures)} fichier(s) non restauré(s) : "
            f"{details}. Cause initiale : {cause}"
        )


# ── Objets de transaction ────────────────────────────────────────────────────

@dataclass(frozen=True)
class DemandeEcritureCharge:
    """Demande déjà structurée (validée en amont par la prévisualisation)."""

    charge_id: str
    montant: float
    target_row: int
    row_data: dict[str, Any]
    persistable: dict[str, Any]
    saisie_path: Path
    impacts_path: Path
    sha256_saisie_attendu: str | None = None
    sha256_impacts_attendu: str | None = None


@dataclass
class FichierPrepare:
    cible: Path
    temp: Path
    sha256_cible_avant: str
    sha256_temp: str
    ordre: int
    libelle: str


@dataclass
class FichierRemplace:
    cible: Path
    sauvegarde: Path
    sha256_avant: str
    sha256_apres: str


@dataclass
class ResultatTransaction:
    statut: str
    charge_id: str
    code: str | None = None
    details: str | None = None
    fichiers_remplaces: list[str] = field(default_factory=list)
    sauvegardes_restantes: list[str] = field(default_factory=list)
    nettoyage_incomplet: list[str] = field(default_factory=list)
    sha256_finaux: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.statut == STATUT_OK


# ── 1. Garde des flags ───────────────────────────────────────────────────────

def _garde_flags() -> ResultatTransaction | None:
    """Refus AVANT toute préparation. Les deux flags sont requis (double garde-fou)."""
    manquants = []
    if not cfg.CHARGES_REAL_WRITE_ENABLED:
        manquants.append("CHARGES_REAL_WRITE_ENABLED")
    if not cfg.CHARGES_REAL_WRITE_CONFIRMATION_ENABLED:
        manquants.append("CHARGES_REAL_WRITE_CONFIRMATION_ENABLED")
    if not manquants:
        return None
    return ResultatTransaction(
        statut=STATUT_GARDE,
        charge_id="",
        code=E_FLAGS_DESACTIVES,
        details=(
            f"Écriture réelle interdite — flag(s) désactivé(s) : {', '.join(manquants)}. "
            f"Aucun fichier n'a été touché."
        ),
    )


# ── 2. Validation de la demande ──────────────────────────────────────────────

def _valider_demande(d: DemandeEcritureCharge) -> str | None:
    if not str(d.charge_id or "").strip():
        return "charge_id obligatoire."
    if str(d.row_data.get("charge_id") or "").strip() != d.charge_id:
        return "row_data.charge_id incohérent avec la demande."
    if str(d.persistable.get("charge_id") or "").strip() != d.charge_id:
        return "persistable.charge_id incohérent avec la demande."
    if float(d.montant) <= 0:
        return f"montant invalide : {d.montant}."
    if int(d.target_row) < 2:
        return f"target_row invalide : {d.target_row} (la ligne 1 est l'en-tête)."
    return None


# ── 3. Préparation des temporaires (writer bas niveau) ───────────────────────

def _preparer(d: DemandeEcritureCharge) -> tuple[list[FichierPrepare], ResultatTransaction | None]:
    """Prépare TOUS les temporaires. Aucun fichier réel n'est modifié ici.

    Si une préparation échoue, les temporaires déjà produits sont nettoyés et rien n'est remplacé.
    """
    prepares: list[FichierPrepare] = []

    res_impacts = writer.prepare_charge_impacts_write(
        d.impacts_path, d.persistable, d.montant, sha256_attendu=d.sha256_impacts_attendu
    )
    if res_impacts["statut"] != STATUT_OK:
        return [], ResultatTransaction(
            statut=STATUT_ERREUR, charge_id=d.charge_id, code=E_PREPARATION,
            details=f"Impacts : [{res_impacts['code']}] {res_impacts['details']}",
        )
    prepares.append(FichierPrepare(
        cible=Path(d.impacts_path), temp=Path(res_impacts["temp_path"]),
        sha256_cible_avant=res_impacts["sha256_source"], sha256_temp=res_impacts["sha256_temp"],
        ordre=ORDRE_IMPACTS, libelle="SAISIE_Charges_Impacts",
    ))

    res_flux = writer.prepare_charge_flux_write(
        d.saisie_path, d.row_data, d.target_row, sha256_attendu=d.sha256_saisie_attendu
    )
    if res_flux["statut"] != STATUT_OK:
        _nettoyer([p.temp for p in prepares])       # aucun remplacement n'a eu lieu
        return [], ResultatTransaction(
            statut=STATUT_ERREUR, charge_id=d.charge_id, code=E_PREPARATION,
            details=f"Charge : [{res_flux['code']}] {res_flux['details']}",
        )
    prepares.append(FichierPrepare(
        cible=Path(d.saisie_path), temp=Path(res_flux["temp_path"]),
        sha256_cible_avant=res_flux["sha256_source"], sha256_temp=res_flux["sha256_temp"],
        ordre=ORDRE_FLUX, libelle="SAISIE_Charges_Flux",
    ))
    return prepares, None


# ── 4. Validation des temporaires ────────────────────────────────────────────

def _valider_temporaires(prepares: list[FichierPrepare]) -> tuple[str, str] | None:
    """(code, details) si un temporaire est inutilisable. Aucun remplacement ne doit suivre."""
    for p in prepares:
        if not p.temp.exists():
            return E_TEMPORAIRE_MANQUANT, f"{p.libelle} : temporaire absent ({p.temp})."
        if p.temp == p.cible:
            return E_TEMPORAIRE_INVALIDE, f"{p.libelle} : le temporaire est le fichier cible."
        if not _same_volume(p.temp, p.cible):
            return E_TEMPORAIRE_INVALIDE, (
                f"{p.libelle} : temporaire sur un autre volume que la cible — "
                f"remplacement atomique impossible."
            )
        if _sha256(p.temp) != p.sha256_temp:
            return E_TEMPORAIRE_INVALIDE, (
                f"{p.libelle} : le temporaire a changé depuis sa préparation "
                f"(empreinte différente)."
            )
        if not _lisible(p.temp):
            return E_TEMPORAIRE_INVALIDE, f"{p.libelle} : temporaire illisible (classeur corrompu)."
        if _sha256(p.cible) != p.sha256_cible_avant:
            return E_TEMPORAIRE_INVALIDE, (
                f"{p.libelle} : la cible a changé depuis la préparation — base périmée."
            )
    return None


def _lisible(path: Path) -> bool:
    """Le classeur s'ouvre sans réparation (aucun Excel/LibreOffice : openpyxl seul)."""
    try:
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        try:
            return bool(wb.sheetnames)
        finally:
            wb.close()
    except Exception:
        return False


# ── 5. Sauvegardes techniques ────────────────────────────────────────────────

def _sauvegarder(prepares: list[FichierPrepare], jeton: str) -> tuple[dict[Path, Path], str | None]:
    """Copie chaque cible AVANT le premier remplacement. Même dossier = même volume."""
    sauvegardes: dict[Path, Path] = {}
    for p in prepares:
        bak = p.cible.parent / f"{p.cible.stem}.{jeton}.bak{p.cible.suffix}"
        try:
            shutil.copy2(str(p.cible), str(bak))
        except Exception as exc:
            _nettoyer(list(sauvegardes.values()))
            return {}, f"{p.libelle} : sauvegarde impossible ({exc})."
        sauvegardes[p.cible] = bak
    return sauvegardes, None


# ── 6/7/8. Commit, vérification post-commit, rollback ────────────────────────

def _remplacer_fichier(temp: Path, cible: Path) -> None:
    """SEUL point de remplacement définitif du projet. Atomique (même volume garanti en amont)."""
    os.replace(str(temp), str(cible))


def _restaurer_fichier(sauvegarde: Path, cible: Path) -> None:
    """Restaure une cible depuis sa sauvegarde. Atomique si possible, sinon copie de secours.

    Le repli par copie est volontaire : si `os.replace` échoue (verrou, erreur disque), une copie
    non atomique vaut mieux qu'un fichier laissé dans un état incohérent.
    """
    try:
        os.replace(str(sauvegarde), str(cible))
    except Exception:
        shutil.copy2(str(sauvegarde), str(cible))


def _rollback(
    remplaces: list[FichierRemplace], sauvegardes: dict[Path, Path], cause: str
) -> None:
    """Restaure les fichiers DÉJÀ remplacés, en ordre inverse. Vérifie chaque restauration.

    Lève RollbackCritiqueError si un fichier ne revient pas à son empreinte initiale. Les
    sauvegardes des fichiers non restaurés sont CONSERVÉES (jamais nettoyées).
    """
    non_restaures: list[dict[str, str]] = []
    for r in reversed(remplaces):
        bak = sauvegardes.get(r.cible)
        if bak is None or not bak.exists():
            non_restaures.append({"cible": str(r.cible), "sauvegarde": str(bak or "(absente)")})
            continue
        try:
            _restaurer_fichier(bak, r.cible)
        except Exception as exc:  # jamais masquée : elle devient une erreur critique
            non_restaures.append({
                "cible": str(r.cible), "sauvegarde": str(bak), "erreur": str(exc),
            })
            continue
        if _sha256(r.cible) != r.sha256_avant:      # restauration non conforme = non restauré
            non_restaures.append({"cible": str(r.cible), "sauvegarde": str(bak)})

    if non_restaures:
        raise RollbackCritiqueError(non_restaures, cause)


def _nettoyer(chemins: list[Path]) -> list[str]:
    """Suppression best-effort. Retourne ce qui n'a PAS pu être supprimé (jamais silencieux)."""
    restants: list[str] = []
    for c in chemins:
        try:
            Path(c).unlink(missing_ok=True)
        except Exception:
            restants.append(str(c))
    return restants


# ── Point d'entrée ───────────────────────────────────────────────────────────

def confirmer_ecriture_charge(demande: DemandeEcritureCharge) -> ResultatTransaction:
    """Écrit une charge dans les deux classeurs SAISIE, de façon transactionnelle.

    Aucun fichier réel n'est modifié tant que TOUS les temporaires ne sont pas préparés et validés.
    Si un remplacement ou une vérification post-commit échoue, les fichiers déjà remplacés sont
    restaurés depuis leurs sauvegardes ; si cette restauration échoue, RollbackCritiqueError est
    levée (état explicite, sauvegardes conservées).
    """
    if (garde := _garde_flags()) is not None:
        return garde

    if (invalide := _valider_demande(demande)) is not None:
        return ResultatTransaction(
            statut=STATUT_ERREUR, charge_id=demande.charge_id,
            code=E_DEMANDE_INVALIDE, details=invalide,
        )

    # ── Préparation : rien de réel n'est touché ───────────────────────────────
    prepares, erreur = _preparer(demande)
    if erreur is not None:
        return erreur

    temps = [p.temp for p in prepares]

    if (probleme := _valider_temporaires(prepares)) is not None:
        code, details = probleme
        restants = _nettoyer(temps)
        return ResultatTransaction(
            statut=STATUT_ERREUR, charge_id=demande.charge_id, code=code, details=details,
            nettoyage_incomplet=restants,
        )

    # ── Sauvegardes : AVANT le premier remplacement ───────────────────────────
    jeton = uuid.uuid4().hex[:12]
    sauvegardes, erreur_bak = _sauvegarder(prepares, jeton)
    if erreur_bak is not None:
        restants = _nettoyer(temps)
        return ResultatTransaction(
            statut=STATUT_ERREUR, charge_id=demande.charge_id, code=E_SAUVEGARDE,
            details=erreur_bak, nettoyage_incomplet=restants,
        )

    # ── Commit : ordre déterministe ───────────────────────────────────────────
    remplaces: list[FichierRemplace] = []
    for p in sorted(prepares, key=lambda x: x.ordre):
        try:
            _remplacer_fichier(p.temp, p.cible)
        except Exception as exc:
            cause = f"{p.libelle} : remplacement échoué ({exc})."
            _rollback(remplaces, sauvegardes, cause)   # peut lever RollbackCritiqueError
            restants = _nettoyer(temps + list(sauvegardes.values()))
            return ResultatTransaction(
                statut=STATUT_ROLLBACK, charge_id=demande.charge_id, code=E_REMPLACEMENT,
                details=f"{cause} Tous les fichiers ont été restaurés.",
                nettoyage_incomplet=restants,
            )
        remplaces.append(FichierRemplace(
            cible=p.cible, sauvegarde=sauvegardes[p.cible],
            sha256_avant=p.sha256_cible_avant, sha256_apres=p.sha256_temp,
        ))

    # ── Vérification post-commit ──────────────────────────────────────────────
    ecarts = []
    for r in remplaces:
        if not _lisible(r.cible):
            ecarts.append(f"{r.cible.name} : illisible après remplacement.")
        elif _sha256(r.cible) != r.sha256_apres:
            ecarts.append(f"{r.cible.name} : empreinte différente du temporaire validé.")
    if ecarts:
        cause = "Vérification post-commit : " + " ".join(ecarts)
        _rollback(remplaces, sauvegardes, cause)       # peut lever RollbackCritiqueError
        restants = _nettoyer(temps + list(sauvegardes.values()))
        return ResultatTransaction(
            statut=STATUT_ROLLBACK, charge_id=demande.charge_id, code=E_POST_COMMIT,
            details=f"{cause} Tous les fichiers ont été restaurés.",
            nettoyage_incomplet=restants,
        )

    # ── Succès : nettoyage des temporaires PUIS des sauvegardes ───────────────
    restants = _nettoyer(temps)                        # les temps ont été consommés par os.replace
    restants += _nettoyer(list(sauvegardes.values()))  # jamais avant la validation complète
    return ResultatTransaction(
        statut=STATUT_OK, charge_id=demande.charge_id,
        fichiers_remplaces=[str(r.cible) for r in remplaces],
        sauvegardes_restantes=[],
        nettoyage_incomplet=restants,                  # signalé, jamais tu
        sha256_finaux={r.cible.name: r.sha256_apres for r in remplaces},
    )

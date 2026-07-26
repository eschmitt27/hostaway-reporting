"""Exécuteur contrôlé des lots de calcul.

Corrige une limite documentée à tort dans les tours précédents : les moteurs SONT exécutables ici,
mais avec **l'interpréteur des lots** (`C:\\Program Files\\Python312\\python.exe`, qui porte pandas),
pas avec celui de l'application (miniconda, sans pandas). Voir `35_AUDIT_PIPELINE_CALCULS.md`.

Garanties :
- interpréteur choisi explicitement, vérifié présent avant tout lancement ;
- environnement du processus enfant dérivé, jamais l'environnement global modifié ;
- stdout, stderr et code retour capturés ; timeout imposé ; durée mesurée ;
- **jamais de faux succès** : un lot n'est SUCCES que si `returncode == 0` ET que ses sorties
  attendues existent réellement sur le disque.
"""
from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import app.config as cfg

ST_ATTENDU = "ATTENDU"
ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_ECHEC = "ECHEC"
ST_IGNORE = "IGNORE"
ST_TIMEOUT = "TIMEOUT"

# Extrait de sortie conservé (les logs complets restent dans le fichier de log du lot lui-même).
MAX_EXTRAIT = 4000
TIMEOUT_DEFAUT_S = int(os.environ.get("CALCULS_TIMEOUT_S", "600"))


@dataclass
class Lot:
    """Définition d'un lot exécutable. `sorties` = fichiers relatifs à la racine du projet dont
    l'existence conditionne le succès (jamais un simple code retour).

    `requiert_pandas` : les moteurs métier importent pandas, donc l'interpréteur choisi doit le
    porter. Ce n'est PAS une propriété globale du pilotage : un lot qui n'en a pas besoin ne doit
    pas être bloqué parce que l'interpréteur courant en est dépourvu.
    """
    nom: str
    script: str
    sorties: tuple[str, ...] = ()
    depend_de: tuple[str, ...] = ()
    entrees: tuple[str, ...] = ()
    requiert_pandas: bool = True


@dataclass
class ResultatLot:
    lot: str
    statut: str
    code_retour: int | None = None
    duree_s: float = 0.0
    stdout: str = ""
    stderr: str = ""
    sorties: dict[str, bool] = field(default_factory=dict)
    message: str = ""


# ── Chaîne des lots — ordre repris des orchestrateurs existants ─────────────
# `run_regression_pipeline.py` définit la chaîne aval ; `run_menages_pipeline.py` la chaîne ménages.
# Rien n'est réinventé : on reprend leurs listes et leurs sorties.

CHAINE_AVAL: tuple[Lot, ...] = (
    Lot("lot4quater", "lot4quater_resoudre_source_reservations.py",
        sorties=("02_TRAVAIL/Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx",)),
    Lot("lot9", "lot9_construire_flux.py",
        sorties=("02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx",),
        depend_de=("lot4quater",)),
    Lot("lot10", "lot10_calculer_resultats.py",
        sorties=("02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx",
                 "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx",
                 "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx"),
        depend_de=("lot9",)),
    Lot("lot11", "lot11_controles_coherence.py",
        sorties=("02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx",),
        depend_de=("lot10",)),
    Lot("lot12", "lot12_generer_factures.py",
        sorties=("02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx",),
        depend_de=("lot10",)),
    Lot("lot13", "lot13_export_powerbi.py",
        sorties=("03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv",),
        depend_de=("lot10", "lot11", "lot12")),
)

CHAINE_CHARGES: tuple[Lot, ...] = (
    Lot("lot3", "lot3_generateur_charges.py",
        sorties=("02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx",)),
)

CHAINE_MENAGES: tuple[Lot, ...] = (
    Lot("lot6b", "lot6b_m04_menages_internes.py"),
    Lot("lot6d", "lot6d_rapprochement_menages.py", depend_de=("lot6b",)),
    Lot("lot6e", "lot6e_gainperte_menages.py", depend_de=("lot6d",)),
    Lot("lot6f", "lot6f_cout_complet_menages.py", depend_de=("lot6e",)),
)

TOUS_LES_LOTS: dict[str, Lot] = {
    l.nom: l for l in (*CHAINE_CHARGES, *CHAINE_MENAGES, *CHAINE_AVAL)
}


def interpreteur_lots() -> Path:
    """Interpréteur portant pandas. Déjà connu de la configuration (LOT4A_ENGINE_PYTHON)."""
    return Path(cfg.LOT4A_ENGINE_PYTHON)


def verifier_interpreteur() -> dict[str, Any]:
    """Vérifie que l'interpréteur des lots existe et dit s'il porte pandas — AVANT tout lancement.

    `ok` reflète « interpréteur utilisable pour un lot qui exige pandas ». `present` reflète la
    seule existence de l'exécutable : c'est cette distinction qui permet d'exécuter un lot sans
    pandas sans être bloqué à tort.
    """
    p = interpreteur_lots()
    if not p.exists():
        return {"ok": False, "present": False, "chemin": str(p),
                "message": "Interpréteur des lots introuvable."}
    try:
        r = subprocess.run(
            [str(p), "-c", "import sys, importlib.util as u; "
                           "print(sys.version.split()[0]); "
                           "print('pandas' if u.find_spec('pandas') else 'NO_PANDAS')"],
            capture_output=True, text=True, timeout=60)
    except Exception as exc:
        return {"ok": False, "present": True, "chemin": str(p),
                "message": f"{type(exc).__name__}: {exc}"}
    lignes = [l.strip() for l in (r.stdout or "").splitlines() if l.strip()]
    version = lignes[0] if lignes else "?"
    a_pandas = len(lignes) > 1 and lignes[1] == "pandas"
    return {"ok": bool(a_pandas), "present": True, "chemin": str(p), "version": version,
            "pandas": a_pandas,
            "message": "" if a_pandas else "pandas absent de l'interpréteur des lots."}


def _racine(racine: Path | None = None) -> Path:
    return Path(racine or cfg.PROJECT_ROOT)


def verifier_prerequis(lots: list[str], racine: Path | None = None) -> dict[str, Any]:
    """Prérequis AVANT lancement : interpréteur, scripts présents, entrées des premiers lots.

    Ne remplace pas les contrôles internes des lots (chaque lot émet ses propres `BLOQUANT`, ex.
    `CTR-9-001 Source manquante`) : c'est une première barrière, pas une réécriture de leurs règles.
    """
    r = _racine(racine)
    interp = verifier_interpreteur()
    scripts_manquants: list[str] = []
    for nom in lots:
        lot = TOUS_LES_LOTS.get(nom)
        if lot is None:
            scripts_manquants.append(f"{nom} (lot inconnu)")
            continue
        if not (r / "02_TRAVAIL" / lot.script).exists():
            scripts_manquants.append(lot.script)

    # Dépendances : un lot dont la dépendance n'est ni demandée ni déjà produite est signalé.
    dependances_manquantes: list[str] = []
    demandes = set(lots)
    for nom in lots:
        lot = TOUS_LES_LOTS.get(nom)
        if lot is None:
            continue
        for dep in lot.depend_de:
            if dep in demandes:
                continue
            dep_lot = TOUS_LES_LOTS.get(dep)
            if dep_lot and dep_lot.sorties and not all(
                    (r / s).exists() for s in dep_lot.sorties):
                dependances_manquantes.append(f"{nom} dépend de {dep} (sortie absente)")

    # pandas ne bloque que si au moins un lot demandé en a réellement besoin.
    pandas_requis = any(
        (TOUS_LES_LOTS.get(n).requiert_pandas if TOUS_LES_LOTS.get(n) else False) for n in lots)
    pandas_manquant = pandas_requis and not interp.get("pandas", False)

    bloquant = (not interp.get("present", False)) or bool(scripts_manquants) or pandas_manquant
    return {
        "ok": not bloquant,
        "interpreteur": interp,
        "pandas_requis": pandas_requis,
        "pandas_manquant": pandas_manquant,
        "scripts_manquants": scripts_manquants,
        "dependances_manquantes": dependances_manquantes,
        "racine": str(r),
        "lots": lots,
    }


def _extrait(texte: str | None) -> str:
    t = texte or ""
    if len(t) <= MAX_EXTRAIT:
        return t
    return t[:MAX_EXTRAIT] + f"\n… (tronqué, {len(t)} caractères au total)"


def executer_lot(nom: str, *, racine: Path | None = None, timeout_s: int = TIMEOUT_DEFAUT_S,
                 interpreteur: Path | None = None) -> ResultatLot:
    """Exécute UN lot. Ne conclut au succès que si code retour 0 ET sorties présentes."""
    lot = TOUS_LES_LOTS.get(nom)
    if lot is None:
        return ResultatLot(nom, ST_ECHEC, message="Lot inconnu.")
    r = _racine(racine)
    script = r / "02_TRAVAIL" / lot.script
    if not script.exists():
        return ResultatLot(nom, ST_ECHEC, message=f"Script absent : {lot.script}")

    interp = Path(interpreteur or interpreteur_lots())
    if not interp.exists():
        return ResultatLot(nom, ST_ECHEC, message="Interpréteur des lots introuvable.")

    # Environnement DÉRIVÉ : l'environnement global du serveur n'est jamais modifié.
    env = dict(os.environ)
    env["PROJECT_ROOT"] = str(r)
    env["PYTHONIOENCODING"] = "utf-8"

    debut = time.monotonic()
    try:
        proc = subprocess.run([str(interp), str(script)], cwd=str(r), env=env,
                              capture_output=True, text=True, timeout=timeout_s,
                              encoding="utf-8", errors="replace")
        duree = round(time.monotonic() - debut, 2)
        code = proc.returncode
        out, err = proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        return ResultatLot(nom, ST_TIMEOUT, duree_s=round(time.monotonic() - debut, 2),
                           stdout=_extrait(getattr(exc, "stdout", "") or ""),
                           stderr=_extrait(getattr(exc, "stderr", "") or ""),
                           message=f"Délai dépassé ({timeout_s} s).")
    except Exception as exc:
        return ResultatLot(nom, ST_ECHEC, duree_s=round(time.monotonic() - debut, 2),
                           message=f"{type(exc).__name__}: {exc}")

    sorties = {s: (r / s).exists() for s in lot.sorties}
    sorties_ok = all(sorties.values()) if sorties else True

    if code != 0:
        statut, message = ST_ECHEC, f"Code retour {code}."
    elif not sorties_ok:
        # Cas piège : le lot « réussit » mais n'a rien produit -> jamais annoncé comme un succès.
        absentes = [s for s, present in sorties.items() if not present]
        statut = ST_ECHEC
        message = "Code retour 0 mais sortie(s) absente(s) : " + ", ".join(absentes)
    else:
        statut, message = ST_SUCCES, ""

    return ResultatLot(nom, statut, code_retour=code, duree_s=duree, stdout=_extrait(out),
                       stderr=_extrait(err), sorties=sorties, message=message)


def nettoyer_chemins(texte: str, racine: Path | None = None) -> str:
    """Remplace la racine absolue par un marqueur : les logs restent lisibles sans exposer
    l'arborescence de la machine (ni le nom d'utilisateur) dans l'interface."""
    r = str(_racine(racine))
    sortie = (texte or "").replace(r, "<projet>")
    maison = str(Path.home())
    return sortie.replace(maison, "<utilisateur>")

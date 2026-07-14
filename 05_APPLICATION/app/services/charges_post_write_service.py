"""APP-3b — Orchestration POST-ÉCRITURE : Lot3, Lot7, Lot11. HORS transaction Excel.

Une charge écrite dans `SAISIE_Charges_Flux` n'est vue par personne tant que le MASTER Lot3 n'a pas
été régénéré : Lot9, Lot10 (net propriétaire), Lot11 et Lot12 lisent le MASTER, pas la SAISIE.
Ce service déclenche donc les lots aval — **après** la transaction, jamais dedans.

Deux principes qui ne se négocient pas :

1. **L'échec d'un lot aval n'annule JAMAIS la charge écrite.** L'écriture Excel est committée et
   valide ; un recalcul raté est un incident de recalcul, pas un échec d'écriture. On rend donc des
   statuts SÉPARÉS (écriture / Lot3 / Lot7 / Lot11) et on se garde bien de dire « l'opération a
   échoué » quand seul le recalcul a échoué.
2. **Le moteur n'est jamais importé dans FastAPI.** Le runner est lancé en sous-processus avec
   l'interpréteur moteur (celui qui porte pandas), sur le modèle d'APP-2c : requête JSON de chemins
   → réponse JSON de résultats. Aucune règle métier n'est recodée ici.

`app/adapters/pipeline_runner.run_pipeline` n'est pas utilisable pour ce besoin : son exécution
réelle est verrouillée (`_REAL_EXECUTION_ENABLED = False`, réservée à APP-5) et il lance les scripts
SANS argument, donc sur les chemins de production — alors qu'il nous faut des chemins injectés
(copies isolées en test, fichiers réels en production).
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import app.config as cfg

# Le runner vit HORS du paquet `app/` : ce n'est pas du code applicatif, mais un script moteur
# lancé par chemin dans l'interpréteur moteur. La frontière est matérialisée par l'arborescence —
# le garde-fou `test_no_import_of_travail_modules` interdit (à raison) tout import du moteur sous
# `app/`, et rien ici ne doit y déroger.
RUNNER = Path(__file__).resolve().parents[2] / "runners" / "charges_post_write_runner.py"
REQUETE_NAME = "post_write_requete.json"
REPONSE_NAME = "post_write_reponse.json"

OK = "OK"
ECHEC = "ECHEC"
NON_APPLICABLE = "NON_APPLICABLE"
NON_LANCE = "NON_LANCE"
ANOMALIES = "ANOMALIES"


@dataclass
class CheminsMoteur:
    """Chemins injectés. En test : des copies isolées. En production : les fichiers réels."""

    saisie: Path
    ref: Path
    master_charges: Path
    lot7: Path


@dataclass
class EtapeMoteur:
    statut: str = NON_LANCE
    details: str = ""
    donnees: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.statut in (OK, NON_APPLICABLE)


@dataclass
class ResultatPostEcriture:
    lot3: EtapeMoteur = field(default_factory=EtapeMoteur)
    lot7: EtapeMoteur = field(default_factory=EtapeMoteur)
    lot11: EtapeMoteur = field(default_factory=EtapeMoteur)
    erreur: str | None = None          # panne du sous-processus lui-même (≠ échec d'un lot)

    @property
    def ok(self) -> bool:
        """Tous les lots ont abouti. Des ANOMALIES Lot11 ne sont PAS un échec technique."""
        return (
            self.erreur is None
            and self.lot3.ok
            and self.lot7.ok
            and self.lot11.statut in (OK, ANOMALIES)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "lot3": asdict(self.lot3),
            "lot7": asdict(self.lot7),
            "lot11": asdict(self.lot11),
            "erreur": self.erreur,
            "ok": self.ok,
        }


def _etape(brut: dict[str, Any] | None) -> EtapeMoteur:
    if not brut:
        return EtapeMoteur()
    donnees = {k: v for k, v in brut.items() if k not in ("statut", "details")}
    return EtapeMoteur(
        statut=str(brut.get("statut") or NON_LANCE),
        details=str(brut.get("details") or ""),
        donnees=donnees,
    )


def executer_post_ecriture(
    charge_id: str,
    avantage: bool,
    chemins: CheminsMoteur,
    run_dir: Path,
    *,
    python_moteur: Path | None = None,
    timeout: int | None = None,
) -> ResultatPostEcriture:
    """Lance Lot3 (+ Lot7 si avantage) puis les contrôles Lot11, en sous-processus.

    N'est appelé QUE si la transaction Excel a réussi. Ne lève jamais : un incident est rendu dans
    le résultat, car à ce stade la charge est déjà écrite et doit être annoncée comme telle.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    requete_path = run_dir / REQUETE_NAME
    reponse_path = run_dir / REPONSE_NAME

    requete = {
        "project_root": str(Path(cfg.PROJECT_ROOT).resolve()),
        "charge_id": charge_id,
        "avantage": bool(avantage),
        "saisie": str(Path(chemins.saisie).resolve()),
        "ref": str(Path(chemins.ref).resolve()),
        "master_charges": str(Path(chemins.master_charges).resolve()),
        "lot7": str(Path(chemins.lot7).resolve()),
    }
    requete_path.write_text(json.dumps(requete, ensure_ascii=False, indent=2), encoding="utf-8")

    # Interpréteur MOTEUR (celui qui porte pandas) — jamais l'interpréteur de FastAPI.
    executable = Path(python_moteur or cfg.LOT4A_ENGINE_PYTHON)
    cmd = [str(executable), str(RUNNER), str(requete_path), str(reponse_path)]

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cfg.PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout or cfg.LOT4A_ENGINE_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return ResultatPostEcriture(erreur=f"Recalcul aval : délai dépassé ({exc}).")
    except OSError as exc:
        return ResultatPostEcriture(erreur=f"Recalcul aval : moteur introuvable ou illisible ({exc}).")

    if not reponse_path.exists():
        stderr = (proc.stderr or "").strip()[:500]
        return ResultatPostEcriture(
            erreur=f"Recalcul aval : le moteur n'a produit aucune réponse (rc={proc.returncode}). {stderr}"
        )

    try:
        reponse = json.loads(reponse_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return ResultatPostEcriture(erreur=f"Recalcul aval : réponse illisible ({exc}).")

    return ResultatPostEcriture(
        lot3=_etape(reponse.get("lot3")),
        lot7=_etape(reponse.get("lot7")),
        lot11=_etape(reponse.get("lot11")),
    )

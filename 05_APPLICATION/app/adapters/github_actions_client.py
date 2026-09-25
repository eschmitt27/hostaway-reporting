"""Client GitHub Actions — MINIMAL : lancer UN workflow, retrouver SON run, lire SON artifact.

Quatre opérations, pas une de plus :
  · `dispatch`            POST /repos/{repo}/actions/workflows/{workflow}/dispatches
  · `trouver_run`         GET  /repos/{repo}/actions/workflows/{workflow}/runs  (par display_title)
  · `run`                 GET  /repos/{repo}/actions/runs/{run_id}               (status/conclusion)
  · `telecharger_artifact` GET /repos/{repo}/actions/runs/{run_id}/artifacts  puis  …/zip

Aucune lecture ni écriture du contenu du dépôt : ni `contents`, ni `git pull`, ni checkout. Les
données voyagent par l'artifact du run exact, jamais par le « dernier run » ni par la branche.

PERMISSIONS MINIMALES (documentation GitHub, « Permissions required for fine-grained personal access
tokens », rubrique « Actions ») — jeton à grain fin, limité au seul dépôt de données :
  · Actions : Read and write   — `dispatches` exige write ; runs et artifacts n'exigent que read ;
  · Metadata : Read-only       — imposée d'office par GitHub à tout jeton à grain fin.
Rien d'autre. En particulier aucun `Contents`.

LE JETON (`HOSTAWAY_GITHUB_TOKEN`) vit dans le `.env` de la racine du projet. Il ne quitte jamais
l'en-tête de la session HTTP : jamais journalisé, jamais en base, jamais dans une page, jamais dans le
texte d'une exception — `ErreurGitHub` ne porte qu'un code et un statut HTTP. `requests` retire
l'en-tête `Authorization` quand GitHub redirige le téléchargement vers son stockage d'objets (autre
hôte) : le jeton n'est donc jamais présenté à un tiers.
"""
from __future__ import annotations

import io
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

API = "https://api.github.com"
USER_AGENT = "PilotageConciergerie/1.0 (+menages)"
VERSION_API = "2022-11-28"
TIMEOUT = 30
# Un artifact de tâches pèse quelques centaines de Ko ; 50 Mo est un garde-fou, pas une attente.
TAILLE_MAX_ARTIFACT = 50 * 1024 * 1024

ENV_DEPOT = "HOSTAWAY_GITHUB_REPOSITORY"
ENV_WORKFLOW = "HOSTAWAY_GITHUB_WORKFLOW"
ENV_REF = "HOSTAWAY_GITHUB_REF"
ENV_JETON = "HOSTAWAY_GITHUB_TOKEN"

DEFAUT_DEPOT = "eschmitt27/hostaway-reporting"
DEFAUT_WORKFLOW = "hostaway-cleaning-tasks.yml"
DEFAUT_REF = "main"

# Contrat du workflow (`run-name` du fichier YAML) : le tiret est un TIRET CADRATIN (U+2014).
PREFIXE_TITRE_RUN = "Hostaway Cleaning Tasks — "
REQUEST_ID_VALIDE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

E_CONFIGURATION = "GITHUB_CONFIGURATION_ABSENTE"
E_AUTHENTIFICATION = "GITHUB_AUTHENTIFICATION_REFUSEE"   # 401
E_INTERDIT = "GITHUB_ACCES_INTERDIT"                     # 403 hors limite de débit
E_INTROUVABLE = "GITHUB_RESSOURCE_INTROUVABLE"           # 404
E_LIMITE = "GITHUB_LIMITE_DEBIT"                         # 429, ou 403 de limite de débit
E_DELAI = "GITHUB_DELAI_DEPASSE"                         # timeout réseau
E_RESEAU = "GITHUB_RESEAU_INDISPONIBLE"
E_API = "GITHUB_REPONSE_INATTENDUE"
E_ARTIFACT_ABSENT = "GITHUB_ARTIFACT_ABSENT"
E_ARTIFACT_EXPIRE = "GITHUB_ARTIFACT_EXPIRE"
E_ARTIFACT_INVALIDE = "GITHUB_ARTIFACT_INVALIDE"

# Messages TECHNIQUES (journal des actualisations, Observabilité). L'écran Ménages n'emploie pas le
# vocabulaire GitHub : il traduit en « l'actualisation Hostaway a échoué ».
MESSAGES = {
    E_CONFIGURATION: "Jeton GitHub absent : renseigner HOSTAWAY_GITHUB_TOKEN dans le « .env ».",
    E_AUTHENTIFICATION: "GitHub a refusé le jeton (401) : jeton expiré ou révoqué.",
    E_INTERDIT: "GitHub refuse l'accès (403) : permission « Actions » manquante sur le dépôt.",
    E_INTROUVABLE: "Dépôt, workflow ou run introuvable (404) — ou jeton sans accès à ce dépôt.",
    E_LIMITE: "Limite de débit GitHub atteinte : réessayer plus tard.",
    E_DELAI: "GitHub n'a pas répondu à temps.",
    E_RESEAU: "GitHub est injoignable.",
    E_API: "Réponse GitHub inattendue.",
    E_ARTIFACT_ABSENT: "Le run ne porte pas l'artifact attendu.",
    E_ARTIFACT_EXPIRE: "L'artifact du run a expiré.",
    E_ARTIFACT_INVALIDE: "L'artifact du run est illisible ou ne contient pas le fichier attendu.",
}


class ErreurGitHub(RuntimeError):
    """Échec décrit par un code. Ne porte JAMAIS le jeton, un en-tête ni une URL signée."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(MESSAGES.get(code, code) + (f" ({detail})" if detail else ""))


@dataclass(frozen=True)
class Configuration:
    depot: str
    workflow: str
    ref: str
    jeton: str

    def __repr__(self) -> str:          # le jeton ne s'imprime jamais, même par accident
        return (f"Configuration(depot={self.depot!r}, workflow={self.workflow!r}, "
                f"ref={self.ref!r}, jeton={'<défini>' if self.jeton else '<absent>'})")


def configuration(env: dict | None = None) -> Configuration:
    source = env if env is not None else os.environ
    return Configuration(
        depot=(source.get(ENV_DEPOT) or DEFAUT_DEPOT).strip(),
        workflow=(source.get(ENV_WORKFLOW) or DEFAUT_WORKFLOW).strip(),
        ref=(source.get(ENV_REF) or DEFAUT_REF).strip(),
        jeton=(source.get(ENV_JETON) or "").strip(),
    )


def jeton_present(env: dict | None = None) -> bool:
    return bool(configuration(env).jeton)


def titre_run(request_id: str) -> str:
    return PREFIXE_TITRE_RUN + request_id


def _horodatage(texte: str | None) -> datetime | None:
    if not texte:
        return None
    try:
        return datetime.fromisoformat(str(texte).replace("Z", "+00:00"))
    except ValueError:
        return None


class ClientGitHubActions:
    def __init__(self, config: Configuration | None = None, *, session=None) -> None:
        self.config = config or configuration()
        if not self.config.jeton:
            raise ErreurGitHub(E_CONFIGURATION)
        self._session = session if session is not None else requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.config.jeton}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": VERSION_API,
            "User-Agent": USER_AGENT,
        })

    # ── transport ────────────────────────────────────────────────────────────────────────────
    def _appel(self, methode: str, chemin: str, *, params=None, json=None, stream=False):
        url = chemin if chemin.startswith("https://") else f"{API}{chemin}"
        try:
            reponse = self._session.request(methode, url, params=params, json=json,
                                            timeout=TIMEOUT, stream=stream)
        except requests.Timeout:
            raise ErreurGitHub(E_DELAI) from None
        except requests.RequestException as err:
            raise ErreurGitHub(E_RESEAU, type(err).__name__) from None
        self._verifier(reponse)
        return reponse

    @staticmethod
    def _verifier(reponse) -> None:
        code = reponse.status_code
        if code < 400:
            return
        if code == 401:
            raise ErreurGitHub(E_AUTHENTIFICATION, "HTTP 401")
        if code == 429 or (code == 403 and str(reponse.headers.get("x-ratelimit-remaining", ""))
                           == "0"):
            raise ErreurGitHub(E_LIMITE, f"HTTP {code}")
        if code == 403:
            raise ErreurGitHub(E_INTERDIT, "HTTP 403")
        if code == 404:
            raise ErreurGitHub(E_INTROUVABLE, "HTTP 404")
        if code == 410:
            raise ErreurGitHub(E_ARTIFACT_EXPIRE, "HTTP 410")
        raise ErreurGitHub(E_API, f"HTTP {code}")

    def _json(self, reponse) -> dict:
        try:
            return reponse.json() or {}
        except ValueError:
            raise ErreurGitHub(E_API, "réponse illisible") from None

    # ── 1. dispatch ──────────────────────────────────────────────────────────────────────────
    def dispatch(self, request_id: str) -> dict:
        """Déclenche le workflow avec `request_id`. Rend `workflow_run_id` si GitHub le fournit
        (réponse 200 récente) — simple indice : le run est toujours VÉRIFIÉ par son titre."""
        if not REQUEST_ID_VALIDE.match(request_id or ""):
            raise ValueError("request_id invalide : [A-Za-z0-9._-], 64 caractères max")
        c = self.config
        reponse = self._appel(
            "POST", f"/repos/{c.depot}/actions/workflows/{c.workflow}/dispatches",
            json={"ref": c.ref, "inputs": {"request_id": request_id}})
        run_id = None
        if reponse.status_code == 200 and reponse.content:
            try:
                run_id = (reponse.json() or {}).get("workflow_run_id")
            except ValueError:
                run_id = None
        return {"workflow_run_id": run_id}

    # ── 2. retrouver CE run ──────────────────────────────────────────────────────────────────
    def trouver_run(self, request_id: str, *, depuis: datetime | None = None) -> dict | None:
        """Le run dont le titre est EXACTEMENT « Hostaway Cleaning Tasks — <request_id> ».

        Jamais « le dernier run » : un run lancé à la main ou par une autre demande dans la même
        minute porterait un autre titre et serait ignoré. Deux runs portant le même titre (même
        request_id réutilisé) : le plus ancien postérieur à `depuis` — celui de notre dispatch.
        """
        c = self.config
        params = {"event": "workflow_dispatch", "per_page": 50}
        if depuis is not None:
            borne = (depuis - timedelta(minutes=2)).astimezone(timezone.utc)
            params["created"] = ">=" + borne.strftime("%Y-%m-%dT%H:%M:%SZ")
        donnees = self._json(self._appel(
            "GET", f"/repos/{c.depot}/actions/workflows/{c.workflow}/runs", params=params))
        attendu = titre_run(request_id)
        candidats = [r for r in donnees.get("workflow_runs") or []
                     if r.get("display_title") == attendu]
        if not candidats:
            return None
        candidats.sort(key=lambda r: _horodatage(r.get("created_at"))
                       or datetime.max.replace(tzinfo=timezone.utc))
        return _resume_run(candidats[0])

    def run(self, run_id: int | str) -> dict:
        c = self.config
        return _resume_run(self._json(self._appel("GET", f"/repos/{c.depot}/actions/runs/{run_id}")))

    # ── 3. artifact du run exact ─────────────────────────────────────────────────────────────
    def telecharger_artifact(self, run_id: int | str, nom_artifact: str,
                             nom_fichier: str) -> dict:
        """Octets de `nom_fichier` dans l'artifact `nom_artifact` DU run `run_id` — aucun autre."""
        c = self.config
        donnees = self._json(self._appel(
            "GET", f"/repos/{c.depot}/actions/runs/{run_id}/artifacts",
            params={"name": nom_artifact, "per_page": 100}))
        artefacts = [a for a in donnees.get("artifacts") or []
                     if a.get("name") == nom_artifact
                     and str((a.get("workflow_run") or {}).get("id", run_id)) == str(run_id)]
        if not artefacts:
            raise ErreurGitHub(E_ARTIFACT_ABSENT, nom_artifact)
        artefact = artefacts[0]
        if artefact.get("expired"):
            raise ErreurGitHub(E_ARTIFACT_EXPIRE, nom_artifact)
        if int(artefact.get("size_in_bytes") or 0) > TAILLE_MAX_ARTIFACT:
            raise ErreurGitHub(E_ARTIFACT_INVALIDE, "taille excessive")
        reponse = self._appel(
            "GET", f"/repos/{c.depot}/actions/artifacts/{artefact['id']}/zip")
        contenu = reponse.content
        if len(contenu) > TAILLE_MAX_ARTIFACT:
            raise ErreurGitHub(E_ARTIFACT_INVALIDE, "taille excessive")
        return {"artifact_id": artefact.get("id"), "nom": nom_artifact,
                "octets": extraire_fichier_zip(contenu, nom_fichier)}


def extraire_fichier_zip(contenu: bytes, nom_fichier: str) -> bytes:
    """Un seul fichier, désigné par son nom, lu EN MÉMOIRE. Rien n'est écrit sur le disque, et un
    chemin d'archive (`../x`) ne peut rien atteindre puisqu'aucune extraction n'a lieu."""
    try:
        with zipfile.ZipFile(io.BytesIO(contenu)) as archive:
            noms = [n for n in archive.namelist() if n.rsplit("/", 1)[-1] == nom_fichier]
            if len(noms) != 1:
                raise ErreurGitHub(E_ARTIFACT_INVALIDE, f"{nom_fichier} absent de l'artifact")
            info = archive.getinfo(noms[0])
            if info.file_size > TAILLE_MAX_ARTIFACT:
                raise ErreurGitHub(E_ARTIFACT_INVALIDE, "taille excessive")
            return archive.read(noms[0])
    except zipfile.BadZipFile:
        raise ErreurGitHub(E_ARTIFACT_INVALIDE, "archive illisible") from None


def _resume_run(r: dict) -> dict:
    return {"id": r.get("id"), "display_title": r.get("display_title"),
            "status": r.get("status"), "conclusion": r.get("conclusion"),
            "created_at": r.get("created_at"), "updated_at": r.get("updated_at"),
            "event": r.get("event")}

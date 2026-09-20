"""Client Qonto — LECTURE SEULE, structurellement incapable d'écrire.

Ce client ne connaît qu'un seul verbe. Il n'expose aucune méthode `post`, `put`, `patch` ou
`delete`, et `requete()` refuse toute méthode autre que GET **avant** d'ouvrir la moindre socket :
la garde n'est pas une convention de nommage, c'est un refus qui précède le réseau. Un virement,
une catégorisation, un ajout de pièce jointe sont donc impossibles par construction, et pas
seulement « non appelés ».

Documentation officielle suivie :
  · authentification par clé d'API — en-tête `Authorization: {login}:{secret}`, concaténation par
    deux-points, SANS encodage Base64 (ce n'est pas du Basic Auth malgré la ressemblance) ;
    https://docs.qonto.com/get-started/business-api/authentication/api-key
  · `GET /v2/organization`  → organisation + comptes bancaires ;
  · `GET /v2/transactions`  → mouvements d'un compte, paginés.

SECRETS. `QONTO_LOGIN` et `QONTO_SECRET_KEY` sont lus dans le `.env` de la racine du projet — le
même fichier que le reste de l'application — et ne quittent jamais ce processus : ils vivent dans
les en-têtes de la session HTTP, ne sont jamais journalisés, jamais écrits en base, jamais rendus
dans une page. Les exceptions levées ici ne portent volontairement ni URL complète, ni en-tête.

USER-AGENT. Un `User-Agent` explicite est obligatoire : sans lui, le pare-feu de Qonto
(Cloudflare) renvoie un 403 « erreur 1010 » avant même de regarder les identifiants — l'échec
ressemble alors à un problème de clé alors qu'il n'en est pas un.
"""
from __future__ import annotations

import os
from pathlib import Path

import requests

import app.config as cfg

BASE_URL = "https://thirdparty.qonto.com/v2"
USER_AGENT = "PilotageConciergerie/1.0 (+lecture-seule)"
TIMEOUT = 30
# Qonto plafonne `per_page` à 100.
PAR_PAGE = 100
# Filet anti-boucle : une pagination qui ne se termine pas est un défaut, pas une raison de tourner
# indéfiniment. 200 pages × 100 = 20 000 mouvements, très au-delà de l'historique réel.
PAGES_MAX = 200

E_IDENTIFIANTS_ABSENTS = "QONTO_IDENTIFIANTS_ABSENTS"
E_AUTHENTIFICATION = "QONTO_AUTHENTIFICATION_REFUSEE"
E_API = "QONTO_API_ECHOUEE"
E_RESEAU = "QONTO_RESEAU_INDISPONIBLE"

MESSAGES = {
    E_IDENTIFIANTS_ABSENTS: (
        "Identifiants Qonto absents : la synchronisation bancaire est impossible. "
        "Renseignez QONTO_LOGIN et QONTO_SECRET_KEY dans le fichier « .env » à la racine du "
        "projet, puis redémarrez l'application."),
    E_AUTHENTIFICATION: (
        "Qonto a refusé les identifiants fournis : la synchronisation bancaire est impossible. "
        "Vérifiez la clé d'API dans « .env » (Qonto > Intégrations et partenariats > Clé d'API)."),
    E_API: "L'API Qonto n'a pas répondu correctement : les données déjà importées sont conservées.",
    E_RESEAU: "Qonto est injoignable : les données déjà importées sont conservées.",
}


class MethodeInterdite(RuntimeError):
    """Une méthode HTTP autre que GET a été demandée. Refusée AVANT tout accès réseau."""


class ErreurQonto(RuntimeError):
    """Échec d'appel, décrit par un code métier. Ne porte jamais d'identifiant ni d'en-tête."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(MESSAGES.get(code, code))


def identifiants(env: dict | None = None) -> tuple[str, str]:
    """Lit `QONTO_LOGIN` / `QONTO_SECRET_KEY`. Les valeurs ne sont jamais retournées ailleurs.

    Même chemin que le reste de l'application : `.env` à la racine du projet (cf. `.env.example`).
    """
    # `app.config` a déjà chargé le `.env` au démarrage : le recharger ici ne ferait que masquer
    # une éventuelle défaillance de ce chargement central.
    source = env if env is not None else os.environ
    login = (source.get("QONTO_LOGIN") or "").strip()
    secret = (source.get("QONTO_SECRET_KEY") or "").strip()
    return login, secret


def identifiants_presents(env: dict | None = None) -> bool:
    """Dit s'il y a de quoi se connecter, SANS rien révéler de plus."""
    login, secret = identifiants(env)
    return bool(login and secret)


class ClientQontoLectureSeule:
    """Ne sait faire que des GET. Aucune méthode d'écriture n'existe sur cette classe."""

    METHODES_AUTORISEES = frozenset({"GET"})

    def __init__(self, login: str = "", secret: str = "", *, session=None) -> None:
        if not login or not secret:
            login, secret = identifiants()
        if not login or not secret:
            raise ErreurQonto(E_IDENTIFIANTS_ABSENTS)
        self._session = session if session is not None else requests.Session()
        self._session.headers.update({
            "Authorization": f"{login}:{secret}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        })

    # ── transport ────────────────────────────────────────────────────────────────────────────
    def requete(self, methode: str, chemin: str, parametres: dict | None = None):
        """Unique point de sortie réseau. Refuse tout ce qui n'est pas GET, avant la socket."""
        if str(methode).upper() not in self.METHODES_AUTORISEES:
            raise MethodeInterdite(
                f"methode {str(methode).upper()} refusée : ce client est en lecture seule")
        try:
            # "GET" est écrit en dur : même un appelant malveillant ne peut pas le détourner.
            return self._session.request("GET", f"{BASE_URL}{chemin}", params=parametres,
                                         timeout=TIMEOUT)
        except requests.RequestException as err:
            raise ErreurQonto(E_RESEAU, type(err).__name__) from None

    def get(self, chemin: str, parametres: dict | None = None) -> dict:
        reponse = self.requete("GET", chemin, parametres)
        if reponse.status_code in (401, 403):
            raise ErreurQonto(E_AUTHENTIFICATION, f"HTTP {reponse.status_code}")
        if reponse.status_code != 200:
            raise ErreurQonto(E_API, f"HTTP {reponse.status_code} sur {chemin}")
        try:
            return reponse.json()
        except ValueError:
            raise ErreurQonto(E_API, f"réponse illisible sur {chemin}") from None

    # ── lectures métier ──────────────────────────────────────────────────────────────────────
    def organisation(self) -> dict:
        """`GET /v2/organization` — l'organisation et ses comptes bancaires."""
        return self.get("/organization").get("organization", {}) or {}

    def transactions(self, qonto_account_id: str) -> tuple[list[dict], int]:
        """`GET /v2/transactions` — TOUTES les pages d'un compte, tous statuts.

        Par défaut Qonto ne renvoie que les mouvements `completed`. Une couche RAW qui ignorerait
        les opérations en attente, refusées ou contrepassées donnerait une image fausse de la
        banque : les quatre statuts sont donc demandés explicitement.

        Retourne (mouvements, nombre de pages lues). Une erreur en cours de pagination remonte :
        l'appelant décide alors de tout abandonner plutôt que d'écrire un jeu partiel.
        """
        collectes: list[dict] = []
        vus: set[str] = set()
        pages = 0
        page = 1
        while page and pages < PAGES_MAX:
            charge = self.get("/transactions", {
                "bank_account_id": qonto_account_id,
                "per_page": PAR_PAGE,
                "page": page,
                "status[]": ["pending", "declined", "completed", "reversed"],
                "sort_by": "settled_at:desc",
            })
            pages += 1
            lot = charge.get("transactions") or []
            for mouvement in lot:
                identifiant = mouvement.get("id")
                # Une opération peut basculer de page entre deux appels (tri par date de
                # règlement, nouvelle écriture pendant la pagination) : on la compte une fois.
                if identifiant and identifiant not in vus:
                    vus.add(identifiant)
                    collectes.append(mouvement)
            meta = charge.get("meta") or {}
            suivante = meta.get("next_page")
            page = suivante if suivante and lot else None
        return collectes, pages

#!/usr/bin/env python3
"""
lot1_hostaway_extract.py — Module Hostaway — Extraction Lot 1

Usage:
  python lot1_hostaway_extract.py --check-auth   # Test auth uniquement (aucun secret affiché)
  python lot1_hostaway_extract.py --dry-run      # Auth + comptage, aucune écriture
  python lot1_hostaway_extract.py                # Extraction complète depuis 2026-01-01

Sorties dans 02_TRAVAIL/Lot1_Hostaway/ :
  MASTER_REF_HA_Listings.xlsx
  MASTER_FACT_HA_Reservations.xlsx
  MASTER_FACT_HA_ReservationDetails.xlsx
  MASTER_FACT_HA_ReservationFinanceFields.xlsx
  MASTER_FACT_HA_ReservationFees.xlsx
  MASTER_CALC_HA_Payout.xlsx
  MASTER_CTRL_HA_Anomalies.xlsx
  MASTER_FACT_HA_CleaningTasks_Discovery.xlsx
  MASTER_RUN_Log.xlsx

Sécurité : CLIENT_SECRET, access_token, Authorization header jamais loggés ni affichés.
"""

import os
import sys
import re
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

# ── Vérification dépendances ─────────────────────────────────
_missing = []
try:
    import requests
except ImportError:
    _missing.append("requests")
try:
    import pandas as pd
except ImportError:
    _missing.append("pandas")
try:
    import openpyxl
except ImportError:
    _missing.append("openpyxl")
try:
    from dotenv import load_dotenv
except ImportError:
    _missing.append("python-dotenv")

if _missing:
    print(f"[ERREUR] Dépendances manquantes : {', '.join(_missing)}")
    print(f"         Installer : pip install {' '.join(_missing)}")
    sys.exit(1)

# ── Constantes ────────────────────────────────────────────────
DATE_FROM       = "2026-01-01"
PAGE_SIZE       = 100
MAX_RETRIES     = 3
RETRY_BASE_WAIT = 2   # secondes (exponentiel)

DETAILS_MINIMAL = "minimal"   # détail uniquement si payout impossible depuis liste
DETAILS_FULL    = "full"      # détail systématique (comportement original)

# Statuts Hostaway (§6.4)
STATUS_ACTIVE   = {"new", "modified"}
STATUS_OWNER    = {"ownerStay"}
STATUS_CANCEL   = {"cancelled"}
STATUS_SKIP     = {
    "inquiry", "declined", "expired",
    "inquiryPreapproved", "inquiryNotPossible",
    "closedByHost", "unavailable",
}

# Canaux (pour logique payout H1/H2/H3)
CH_AIRBNB  = {"airbnbOfficial", "airbnb2", "airbnb"}
CH_BOOKING = {"bookingcom", "booking.com"}
CH_VRBO    = {"vrboical", "vrbo", "homeaway"}
CH_DIRECT  = {"direct", "manuel", "owner"}

# ── Chemins ───────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"
OUT_DIR  = BASE_DIR / "02_TRAVAIL" / "Lot1_Hostaway"
LOG_DIR  = BASE_DIR / "04_LOGS"
REF_FILE = BASE_DIR / "01_SOURCES_BRUTES" / "REF_Setup" / "REF_Setup.xlsm"


# ═══════════════════════════════════════════════════════════════
# LOGGING SÉCURISÉ
# ═══════════════════════════════════════════════════════════════
class _SecretFilter(logging.Filter):
    """Bloque les lignes contenant des valeurs de secrets (pas les noms de variables)."""
    _PAT = re.compile(
        r"(access_token\s*[:=]\s*\S|bearer\s+\S{8,}|authorization\s*:\s*bearer)",
        re.IGNORECASE,
    )
    def filter(self, record):
        return not self._PAT.search(record.getMessage())


def setup_logging(run_id: str, silent_file: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if not silent_file:
        fh = logging.FileHandler(LOG_DIR / f"lot1_ha_{run_id}.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt))
        handlers.append(fh)
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers, force=True)
    log = logging.getLogger("lot1_ha")
    for h in log.handlers:
        h.addFilter(_SecretFilter())
    log.addFilter(_SecretFilter())
    return log


# ═══════════════════════════════════════════════════════════════
# CHECK-LISTINGS — test léger endpoint /v1/listings
# ═══════════════════════════════════════════════════════════════
def check_listings_mode(env_file: Path) -> None:
    """
    Mode --check-listings : auth + appel /v1/listings uniquement.
    Affiche : HTTP OK, nombre de listings, champs disponibles, 3 premiers IDs.
    Aucun token ni header Authorization affiché. sys.exit(0|1|2).
    """
    print("[LISTINGS] Démarrage vérification endpoint listings")

    if not env_file.exists():
        print(f"[LISTINGS] ÉCHEC — .env introuvable : {env_file}")
        sys.exit(1)

    load_dotenv(env_file)
    client_id     = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id    = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url      = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com").rstrip("/")

    missing = [k for k, v in [
        ("HOSTAWAY_CLIENT_ID", client_id),
        ("HOSTAWAY_CLIENT_SECRET", client_secret),
        ("HOSTAWAY_ACCOUNT_ID", account_id),
    ] if not v]
    if missing:
        print(f"[LISTINGS] ÉCHEC — variables manquantes : {', '.join(missing)}")
        sys.exit(1)

    # Auth
    print("[LISTINGS] Authentification OAuth2...")
    try:
        resp_auth = requests.post(
            f"{base_url}/v1/accessTokens",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "general",
            },
            timeout=15,
        )
        resp_auth.raise_for_status()
        token = resp_auth.json().get("access_token", "")
        if not token:
            print("[LISTINGS] ÉCHEC — token absent de la réponse auth")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"[LISTINGS] ÉCHEC — erreur réseau vers {base_url}")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("[LISTINGS] ÉCHEC — timeout auth (15s)")
        sys.exit(1)
    except Exception as e:
        print(f"[LISTINGS] ÉCHEC — auth : {type(e).__name__}: {e}")
        sys.exit(1)

    print("[LISTINGS] Auth OK (token non affiché)")

    # Appel /v1/listings
    listings_url = f"{base_url}/v1/listings"
    print(f"[LISTINGS] GET {listings_url} ...")
    try:
        resp = requests.get(
            listings_url,
            headers={
                "Authorization": f"Bearer {token}",
                "account-id": account_id,
                "Content-Type": "application/json",
            },
            params={"limit": 200, "includeResources": 0},
            timeout=30,
        )
    except requests.exceptions.ConnectionError:
        print(f"[LISTINGS] ÉCHEC — erreur réseau sur {listings_url}")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("[LISTINGS] ÉCHEC — timeout (30s) sur /v1/listings")
        sys.exit(1)
    except Exception as e:
        print(f"[LISTINGS] ÉCHEC — {type(e).__name__}: {e}")
        sys.exit(2)

    print(f"[LISTINGS] HTTP {resp.status_code}")
    if resp.status_code == 404:
        print(f"[LISTINGS] ÉCHEC — endpoint introuvable (HTTP 404) : {listings_url}")
        sys.exit(1)
    if resp.status_code == 401:
        print("[LISTINGS] ÉCHEC — token rejeté (HTTP 401)")
        sys.exit(1)
    if resp.status_code >= 400:
        print(f"[LISTINGS] ÉCHEC — erreur HTTP {resp.status_code}")
        sys.exit(1)

    try:
        body = resp.json()
    except Exception:
        print("[LISTINGS] ÉCHEC — réponse JSON invalide")
        sys.exit(1)

    results = body.get("result", [])
    count   = body.get("count", len(results))
    print(f"[LISTINGS] HTTP OK")
    print(f"[LISTINGS] Nombre de listings : {count} (reçus dans cette page : {len(results)})")

    if not results:
        print("[LISTINGS] Aucun listing retourné — compte vide ou paramètres incorrects")
        sys.exit(1)

    # Champs disponibles (1er objet)
    fields = list(results[0].keys())
    print(f"[LISTINGS] Champs disponibles ({len(fields)}) : {fields}")

    # IDs non sensibles — ce sont des entiers internes Hostaway
    ids_sample = [r.get("id") for r in results[:3]]
    names_sample = [r.get("name", r.get("internalListingName", "?"))[:40] for r in results[:3]]
    print(f"[LISTINGS] Premiers IDs (listing.id = listingMapId réservations) : {ids_sample}")
    print(f"[LISTINGS] Noms correspondants : {names_sample}")

    # Vérifier si listingMapId est un champ distinct
    has_map_field = any("listingMapId" in r for r in results[:5])
    print(f"[LISTINGS] Champ listingMapId présent dans listings : {'OUI' if has_map_field else 'NON — utiliser id'}")

    print("[LISTINGS] OK — endpoint /v1/listings opérationnel")
    sys.exit(0)


# ═══════════════════════════════════════════════════════════════
# CHECK-RESERVATIONS-SAMPLE — diagnostic léger sur 5 réservations
# ═══════════════════════════════════════════════════════════════
def check_reservations_sample_mode(env_file: Path) -> None:
    """
    Mode --check-reservations-sample : auth + 5 premières réservations.
    Affiche : clés disponibles, champs canal présents, nb avec canal vide.
    Aucune donnée personnelle ni secret affiché. sys.exit(0|1|2).
    """
    print("[SAMPLE] Diagnostic réservations — 5 premières depuis 2026-01-01")

    if not env_file.exists():
        print(f"[SAMPLE] ÉCHEC — .env introuvable : {env_file}")
        sys.exit(1)

    load_dotenv(env_file)
    client_id     = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id    = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url      = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com").rstrip("/")

    missing = [k for k, v in [
        ("HOSTAWAY_CLIENT_ID", client_id),
        ("HOSTAWAY_CLIENT_SECRET", client_secret),
        ("HOSTAWAY_ACCOUNT_ID", account_id),
    ] if not v]
    if missing:
        print(f"[SAMPLE] ÉCHEC — variables manquantes : {', '.join(missing)}")
        sys.exit(1)

    # Auth
    try:
        resp_auth = requests.post(
            f"{base_url}/v1/accessTokens",
            data={"grant_type": "client_credentials", "client_id": client_id,
                  "client_secret": client_secret, "scope": "general"},
            timeout=15,
        )
        resp_auth.raise_for_status()
        token = resp_auth.json().get("access_token", "")
        if not token:
            print("[SAMPLE] ÉCHEC — token absent")
            sys.exit(1)
    except Exception as e:
        print(f"[SAMPLE] ÉCHEC — auth : {type(e).__name__}")
        sys.exit(1)

    print("[SAMPLE] Auth OK")

    # Fetch 5 réservations
    headers = {
        "Authorization": f"Bearer {token}",
        "account-id": account_id,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(
            f"{base_url}/v1/reservations",
            headers=headers,
            params={"dateFrom": DATE_FROM, "limit": 5, "offset": 0},
            timeout=30,
        )
    except Exception as e:
        print(f"[SAMPLE] ÉCHEC — requête réservations : {type(e).__name__}")
        sys.exit(1)

    print(f"[SAMPLE] HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"[SAMPLE] ÉCHEC — HTTP {resp.status_code} sur /v1/reservations")
        sys.exit(1)

    try:
        body = resp.json()
    except Exception:
        print("[SAMPLE] ÉCHEC — JSON invalide")
        sys.exit(1)

    results = body.get("result", [])
    total   = body.get("count", "?")
    print(f"[SAMPLE] Total réservations (API count) : {total}")
    print(f"[SAMPLE] Réservations dans ce lot      : {len(results)}")

    if not results:
        print("[SAMPLE] Aucune réservation retournée")
        sys.exit(1)

    # Clés disponibles
    all_keys = set()
    for r in results:
        all_keys.update(r.keys())
    print(f"[SAMPLE] Clés disponibles ({len(all_keys)}) : {sorted(all_keys)}")

    # Champs canal potentiels présents
    canal_fields_present = [f for f in _CHANNEL_FIELDS if f in all_keys]
    canal_fields_absent  = [f for f in _CHANNEL_FIELDS if f not in all_keys]
    print(f"[SAMPLE] Champs canal présents  : {canal_fields_present}")
    print(f"[SAMPLE] Champs canal absents   : {canal_fields_absent}")

    # Canal résolu par réservation (sans afficher données perso)
    nb_unknown = 0
    for i, r in enumerate(results):
        channel, source_brute = resolve_channel(r)
        res_id = r.get("id", "?")
        status = r.get("status", "?")
        if channel == "UNKNOWN":
            nb_unknown += 1
            print(f"[SAMPLE]   resa #{i+1} id={res_id} status={status} -> canal=UNKNOWN (champs vides)")
        else:
            print(f"[SAMPLE]   resa #{i+1} id={res_id} status={status} -> canal={channel} (source='{source_brute}')")

    print(f"[SAMPLE] Réservations avec canal UNKNOWN dans ce lot : {nb_unknown}/{len(results)}")

    if nb_unknown == len(results):
        print("[SAMPLE] ATTENTION : tous les canaux sont UNKNOWN — vérifier les champs Hostaway")
    elif nb_unknown > 0:
        print("[SAMPLE] Certains canaux absents — anomalie CHANNEL_ABSENT sera créée")
    else:
        print("[SAMPLE] Tous les canaux résolus — OK")

    print("[SAMPLE] OK — diagnostic réservations terminé")
    sys.exit(0)


# ═══════════════════════════════════════════════════════════════
# CHECK-AUTH — diagnostic complet, print() uniquement, jamais log
# ═══════════════════════════════════════════════════════════════
def check_auth_mode(env_file: Path) -> None:
    """
    Mode --check-auth : test OAuth2 avec diagnostic non-sensible.
    Toujours sys.exit(0|1|2). Ne retourne jamais.
    Utilise print() uniquement — bypasse le logging et son filtre.
    """
    print("[AUTH] Démarrage vérification authentification Hostaway")
    print(f"[AUTH] Fichier .env : {env_file}")

    # 1. Existence .env
    if not env_file.exists():
        print(f"[AUTH] ÉCHEC — .env introuvable : {env_file}")
        print("[AUTH] Créer le fichier .env à la racine du projet avec :")
        print("         HOSTAWAY_CLIENT_ID=...")
        print("         HOSTAWAY_CLIENT_SECRET=...")
        print("         HOSTAWAY_ACCOUNT_ID=...")
        print("         HOSTAWAY_BASE_URL=https://api.hostaway.com")
        sys.exit(1)

    load_dotenv(env_file)
    client_id     = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id    = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url      = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com")

    # 2. Variables manquantes
    missing = [k for k, v in [
        ("HOSTAWAY_CLIENT_ID",     client_id),
        ("HOSTAWAY_CLIENT_SECRET", client_secret),
        ("HOSTAWAY_ACCOUNT_ID",    account_id),
    ] if not v]
    if missing:
        print(f"[AUTH] ÉCHEC — variable(s) manquante(s) dans .env : {', '.join(missing)}")
        sys.exit(1)

    # Affichage masqué total — jamais de valeur réelle
    print("[AUTH] Variables .env : CLIENT_ID=*** CLIENT_SECRET=*** ACCOUNT_ID=*** — toutes présentes")
    print(f"[AUTH] BASE_URL      : {base_url}")

    # 3. Tentative OAuth2
    token_url = f"{base_url.rstrip('/')}/v1/accessTokens"
    print(f"[AUTH] Token URL     : {token_url}")
    print("[AUTH] Envoi requête OAuth2 (timeout=15s)...")

    try:
        resp = requests.post(
            token_url,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "general",
            },
            timeout=15,
        )
    except requests.exceptions.ConnectionError:
        print(f"[AUTH] ÉCHEC — erreur réseau : impossible de joindre {base_url}")
        print("[AUTH] Vérifier : connexion internet, BASE_URL dans .env, pare-feu")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print(f"[AUTH] ÉCHEC — timeout réseau (15s) — {token_url} ne répond pas")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"[AUTH] ÉCHEC — erreur réseau inattendue : {type(e).__name__}")
        sys.exit(1)
    except Exception as e:
        print(f"[AUTH] ÉCHEC — erreur technique inattendue : {type(e).__name__}: {e}")
        sys.exit(2)

    print(f"[AUTH] HTTP {resp.status_code}")

    # 4. Diagnostic HTTP
    if resp.status_code == 401:
        print("[AUTH] ÉCHEC — credentials invalides (HTTP 401)")
        print("[AUTH] Vérifier HOSTAWAY_CLIENT_ID et HOSTAWAY_CLIENT_SECRET dans .env")
        sys.exit(1)
    if resp.status_code == 403:
        print("[AUTH] ÉCHEC — accès refusé (HTTP 403) — compte ou scope incorrect")
        sys.exit(1)
    if resp.status_code == 404:
        print(f"[AUTH] ÉCHEC — endpoint incorrect (HTTP 404) : {token_url}")
        print("[AUTH] Vérifier HOSTAWAY_BASE_URL dans .env")
        sys.exit(1)
    if resp.status_code >= 500:
        print(f"[AUTH] ÉCHEC — erreur serveur Hostaway (HTTP {resp.status_code})")
        print("[AUTH] Réessayer dans quelques minutes")
        sys.exit(1)
    if resp.status_code >= 400:
        body_safe = resp.text[:300] if resp.text else "(corps vide)"
        print(f"[AUTH] ÉCHEC — erreur HTTP {resp.status_code}")
        print(f"[AUTH] Réponse (tronquée, sans secrets) : {body_safe[:200]}")
        sys.exit(1)

    # 5. Parsing JSON
    try:
        body = resp.json()
    except Exception:
        print("[AUTH] ÉCHEC — réponse JSON invalide malgré HTTP 200")
        sys.exit(1)

    if "access_token" not in body:
        print("[AUTH] ÉCHEC — champ 'access_token' absent de la réponse JSON")
        print(f"[AUTH] Clés reçues : {list(body.keys())}")
        sys.exit(1)

    expires_in = body.get("expires_in", "?")
    print(f"[AUTH] Token reçu (non affiché) — expire dans {expires_in}s")
    print("[AUTH] OK — authentification réussie, credentials valides")
    sys.exit(0)


# ═══════════════════════════════════════════════════════════════
# AUTHENTIFICATION OAuth2 — secrets jamais loggés
# ═══════════════════════════════════════════════════════════════
class HostawayAuth:
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self._base   = base_url.rstrip("/")
        self._cid    = client_id
        self._csec   = client_secret   # JAMAIS loggé
        self._token  = None
        self._exp    = 0.0

    def get_token(self) -> str:
        if self._token and time.time() < self._exp - 60:
            return self._token
        self._refresh()
        return self._token

    def _refresh(self):
        resp = requests.post(
            f"{self._base}/v1/accessTokens",
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._cid,
                "client_secret": self._csec,
                "scope":         "general",
            },
            timeout=30,
        )
        resp.raise_for_status()
        body         = resp.json()
        self._token  = body["access_token"]       # jamais affiché
        self._exp    = time.time() + int(body.get("expires_in", 3600))

    def test(self) -> bool:
        try:
            self._refresh()
            return bool(self._token)
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════════
# CLIENT API
# ═══════════════════════════════════════════════════════════════
class HostawayClient:
    def __init__(self, auth: HostawayAuth, account_id: str, log):
        self._auth  = auth
        self._aid   = str(account_id)
        self._base  = auth._base
        self._log   = log

    def _headers(self) -> dict:
        # Authorization header jamais loggé
        return {
            "Authorization": f"Bearer {self._auth.get_token()}",
            "account-id":    self._aid,
            "Content-Type":  "application/json",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self._base}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=60)
                if r.status_code == 429:
                    wait = RETRY_BASE_WAIT * (2 ** attempt)
                    self._log.warning(f"Rate limit 429 — attente {wait}s (tentative {attempt})")
                    time.sleep(wait)
                    continue
                # Erreurs HTTP : message propre, sans traceback brut
                if r.status_code == 404:
                    raise RuntimeError(
                        f"Endpoint introuvable (HTTP 404) : {path}\n"
                        f"  URL complète : {url}\n"
                        f"  Vérifier que cet endpoint existe dans l'API Hostaway."
                    )
                if r.status_code == 401:
                    raise RuntimeError(f"Token expiré ou invalide (HTTP 401) : {path}")
                if r.status_code >= 400:
                    raise RuntimeError(f"Erreur HTTP {r.status_code} sur {path}")
                r.raise_for_status()
                return r.json()
            except RuntimeError:
                raise  # propager sans retry
            except requests.exceptions.ConnectionError:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Erreur réseau : impossible de joindre {self._base}")
                wait = RETRY_BASE_WAIT * attempt
                self._log.warning(f"Erreur réseau (t.{attempt}/{MAX_RETRIES}) — retry {wait}s")
                time.sleep(wait)
            except requests.exceptions.Timeout:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Timeout (60s) sur {path}")
                wait = RETRY_BASE_WAIT * attempt
                self._log.warning(f"Timeout (t.{attempt}/{MAX_RETRIES}) — retry {wait}s")
                time.sleep(wait)
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Erreur requête sur {path} : {type(exc).__name__}")
                wait = RETRY_BASE_WAIT * attempt
                self._log.warning(f"Erreur requête (t.{attempt}/{MAX_RETRIES}) : {exc} — retry {wait}s")
                time.sleep(wait)
        return {}

    # ── Endpoints ────────────────────────────────────────────

    def get_listings(self) -> list:
        """
        Retourne la liste des listings via GET /v1/listings.
        Chaque listing : id = identifiant interne Hostaway = ce que les réservations
        appellent listingMapId. Pas de /v1/listingMaps (endpoint inexistant).
        """
        data = self._get("/v1/listings", {"limit": 200, "includeResources": 0})
        return data.get("result", [])

    def count_reservations(self, date_from: str) -> int:
        data = self._get("/v1/reservations", {
            "dateFrom": date_from, "limit": 1, "offset": 0,
        })
        return data.get("count", 0)

    def get_reservations_page(self, date_from: str, offset: int) -> list:
        data = self._get("/v1/reservations", {
            "dateFrom":       date_from,
            "limit":          PAGE_SIZE,
            "offset":         offset,
        })
        return data.get("result", [])

    def get_reservation_detail(self, res_id: int) -> dict:
        data = self._get(f"/v1/reservations/{res_id}")
        return data.get("result", {})

    def get_tasks(self, date_from: str) -> list:
        results, offset = [], 0
        while True:
            data  = self._get("/v1/tasks", {
                "dateFrom": date_from, "limit": PAGE_SIZE, "offset": offset,
            })
            batch = data.get("result", [])
            results.extend(batch)
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.2)
        return results


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def row_hash(*values) -> str:
    raw = "|".join(str(v) for v in values).encode()
    return hashlib.md5(raw).hexdigest()[:16]

def safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _first(*values):
    """Retourne la première valeur non-None non-zéro."""
    for v in values:
        if v is not None and v != "" and v != 0 and v != 0.0:
            return v
    return None

def parse_finance_fields(money: dict) -> list:
    """Extraire la liste [{name, value}] depuis money.financeFields ou money.currencies."""
    if not money:
        return []
    ff = money.get("financeFields") or money.get("finance_fields", [])
    if ff:
        return ff
    # Fallback : money.currencies[0].fields
    currencies = money.get("currencies", [])
    if currencies and isinstance(currencies[0], dict):
        return currencies[0].get("fields", [])
    return []

def ff_map(ff_list: list) -> dict:
    """Dict {name: float_value} depuis la liste finance fields."""
    return {
        f.get("name", ""): safe_float(f.get("value"))
        for f in ff_list
        if f.get("name")
    }

# Champs potentiels de canal dans la réponse Hostaway (ordre de priorité)
# source + channelName confirmés présents ; originalChannel présent aussi
_CHANNEL_FIELDS = (
    "source", "channelName", "originalChannel",
    "channel", "origin", "reservationChannel", "channelSource",
)

def detect_channel(source) -> str:
    """Normalise une valeur de canal. Robuste si None, vide ou non-string."""
    s = str(source or "").lower().strip()
    if not s:
        return "UNKNOWN"
    if s in {c.lower() for c in CH_AIRBNB}:
        return "AIRBNB"
    if s in {c.lower() for c in CH_BOOKING}:
        return "BOOKING"
    if s in {c.lower() for c in CH_VRBO}:
        return "VRBO"
    return "DIRECT"

def resolve_channel(res: dict) -> tuple:
    """
    Retourne (channel_normalise, source_brute) en essayant plusieurs champs.
    Si aucun champ ne contient de valeur → ("UNKNOWN", "").
    """
    for field in _CHANNEL_FIELDS:
        val = res.get(field)
        if val and str(val).strip():
            return detect_channel(val), str(val).strip()
    return "UNKNOWN", ""

def source_financiere(channel: str) -> str:
    return {
        "AIRBNB":  "HOSTAWAY_AIRBNB",
        "BOOKING": "HOSTAWAY_BOOKING",
        "VRBO":    "A_CONTROLER",
        "DIRECT":  "MANUEL_HORS_HOSTAWAY",
    }.get(channel, "INCONNU")


def _ff_from_res(res: dict) -> list:
    """
    Extrait la liste [{name, value}] depuis le champ financeField de la réponse liste.
    financeField peut être un dict {name: value} ou une liste [{name, value}].
    Retourne toujours une liste normalisée.
    """
    raw = res.get("financeField")
    if not raw:
        return []
    if isinstance(raw, list):
        return raw  # déjà au bon format
    if isinstance(raw, dict):
        return [{"name": k, "value": v} for k, v in raw.items()]
    return []

def _needs_detail(res: dict, channel: str, ff_list: list) -> bool:
    """
    Retourne True si l'appel détail est nécessaire pour calculer le payout.
    En mode minimal : évite le détail quand les données liste sont suffisantes.
    - AIRBNB : suffisant si airbnbExpectedPayoutAmount ou airbnbPayoutSum disponible
    - BOOKING : suffisant si totalPriceFromChannel dans ff_list
    - VRBO Unknown : détail inutile (payout impossible de toute façon)
    - Cancelled : suffisant si cancellationAmount déjà dans la liste
    - ownerStay : pas de payout, détail inutile
    """
    status = str(res.get("status") or "")
    if status in STATUS_OWNER:
        return False
    if status in STATUS_CANCEL:
        cancel = safe_float(_first(res.get("cancellationAmount"), res.get("cancellationPayout")))
        return cancel <= 0  # si montant présent dans liste → pas de détail
    if channel == "AIRBNB":
        return not (safe_float(res.get("airbnbExpectedPayoutAmount"))
                    or ff_map(ff_list).get("airbnbPayoutSum"))
    if channel == "BOOKING":
        ffd = ff_map(ff_list)
        return not ffd.get("totalPriceFromChannel")
    if channel == "VRBO":
        pay_status = str(res.get("paymentStatus") or "").lower()
        return pay_status not in ("", "unknown")  # unknown → détail inutile
    return False  # DIRECT → pas de valorisation Hostaway


# ═══════════════════════════════════════════════════════════════
# CALCUL PAYOUT (H1 / H2 / H3)
# ═══════════════════════════════════════════════════════════════
class PayoutCalculator:
    """
    Calcule payout et ménage retenu canal par canal.
    Returns: (payout, source_payout, statut_calcul_payout, menage_retenu)
    H3: Direct/VRBO-Unknown → None, jamais valorisé depuis Hostaway.
    """

    def calc(self, res: dict, ff_list: list, fees: list, channel: str = None) -> tuple:
        if channel is None:
            channel, _ = resolve_channel(res)
        status  = res.get("status", "")
        ffd     = ff_map(ff_list)

        # Annulé sans indemnité
        cancel = safe_float(
            _first(res.get("cancellationAmount"), res.get("cancellationPayout"))
        )
        if status in STATUS_CANCEL:
            if cancel > 0:
                return cancel, "cancellationAmount", "ANNULE_AVEC_PAYOUT", 0.0
            return 0.0, "ANNULE", "ANNULE_SANS_PAYOUT", 0.0

        if channel == "AIRBNB":
            return self._airbnb(res, ffd)
        if channel == "BOOKING":
            return self._booking(res, ffd, fees)
        if channel == "VRBO":
            return self._vrbo(res, ffd)
        # Direct (H3) — pas de valorisation Hostaway
        return None, "DIRECT_HORS_HOSTAWAY", "A_CONTROLER", 0.0

    def _airbnb(self, res: dict, ffd: dict) -> tuple:
        # H1 : airbnbExpectedPayoutAmount > fallback airbnbPayoutSum
        payout = safe_float(res.get("airbnbExpectedPayoutAmount")) or None
        src    = "airbnbExpectedPayoutAmount"
        if not payout:
            payout = ffd.get("airbnbPayoutSum") or None
            src    = "airbnbPayoutSum_fallback"
        if not payout:
            return None, "ABSENT", "PAYOUT_ABSENT", 0.0
        # §8.3 : ménage Airbnb via finance field UNIQUEMENT (colonne cleaningFee = 0)
        menage = ffd.get("cleaningFee", 0.0)
        return payout, src, "NORMAL", menage

    def _booking(self, res: dict, ffd: dict, fees: list) -> tuple:
        # H2 : formule finance fields
        total = ffd.get("totalPriceFromChannel")
        if total:
            city    = ffd.get("cityTax", 0.0)
            ota     = ffd.get("otaPaymentProcessingFee", 0.0)
            ch_fee  = ffd.get("hostChannelFee", 0.0)
            payout  = total - city - ota - ch_fee
            # §8.3 : ménage Booking = colonne cleaningFee du payout (pas finance fields)
            menage  = safe_float(res.get("cleaningFee", 0))
            return payout, "totalPriceFromChannel_formula", "NORMAL", menage
        # Fallback Booking (moins fiable)
        total_p = safe_float(res.get("totalPrice"))
        if total_p > 0:
            ch_comm = safe_float(res.get("channelCommissionAmount", 0))
            tax_fee = sum(
                safe_float(f.get("amount"))
                for f in fees
                if f.get("type", "").lower() in ("city_tax", "citytax", "tax", "tourist_tax")
            )
            payout = total_p - ch_comm - tax_fee
            menage = safe_float(res.get("cleaningFee", 0))
            return payout, "totalPrice_fallback", "PAYOUT_INCOMPLET", menage
        return None, "ABSENT", "PAYOUT_ABSENT", 0.0

    def _vrbo(self, res: dict, ffd: dict) -> tuple:
        pay_status = (res.get("paymentStatus") or "").lower()
        if pay_status == "unknown" or not pay_status:
            return None, "VRBO_UNKNOWN", "A_CONTROLER", 0.0
        payout = ffd.get("totalPriceFromChannel") or safe_float(res.get("totalPrice")) or None
        if payout:
            menage = ffd.get("cleaningFee", 0.0)
            return payout, "vrbo_totalPrice", "PAYOUT_INCOMPLET", menage
        return None, "ABSENT", "A_CONTROLER", 0.0


# ═══════════════════════════════════════════════════════════════
# ANOMALY DETECTOR
# ═══════════════════════════════════════════════════════════════
class AnomalyDetector:
    def __init__(self, known_ids: set):
        self._known = known_ids
        self._rows  = []

    def check_channel_absent(self, res_id):
        self._add(res_id, "CHANNEL_ABSENT", "A_CONTROLER",
                  "Aucun champ canal trouvé dans la réservation — classé UNKNOWN")

    def check_reservation(self, res_id, channel, payout_status, map_id):
        if channel == "UNKNOWN":
            self.check_channel_absent(res_id)
        if channel == "BOOKING" and payout_status == "PAYOUT_ABSENT":
            self._add(res_id, "BOOKING_PAYOUT_INCOMPLET", "BLOQUANT",
                      "Réservation Booking active sans payout calculable (H2 impossible)")
        if channel == "VRBO" and payout_status == "A_CONTROLER":
            self._add(res_id, "VRBO_MONTANT_NON_RENSEIGNE", "A_CONTROLER",
                      "VRBO paymentStatus=Unknown — saisie manuelle requise au Lot 4")
        if payout_status == "PAYOUT_INCOMPLET":
            self._add(res_id, "PAYOUT_INCOMPLET", "A_CONTROLER",
                      "Payout calculé via fallback — champs financiers partiels")
        if map_id and map_id not in self._known:
            self._add(res_id, "LISTING_ORPHELIN_A_CONTROLER", "A_CONTROLER",
                      f"listingMapId {map_id} absent de REF_Logements — Lot 2 requis")

    def check_listing(self, map_id):
        if map_id and map_id not in self._known:
            self._add(None, "LISTING_ORPHELIN_A_CONTROLER", "A_CONTROLER",
                      f"Listing {map_id} dans API Hostaway absent de REF_Logements")

    def _add(self, res_id, code, sev, desc):
        self._rows.append({
            "reservation_id":  res_id,
            "code_anomalie":   code,
            "severite":        sev,
            "description":     desc,
            "statut":          "OUVERT",
            "date_detection":  now_utc(),
            "ROW_HASH":        row_hash(code, str(res_id)),
        })

    def to_df(self) -> "pd.DataFrame":
        if self._rows:
            return pd.DataFrame(self._rows).drop_duplicates(subset=["code_anomalie", "reservation_id"])
        return pd.DataFrame(columns=[
            "reservation_id", "code_anomalie", "severite",
            "description", "statut", "date_detection", "ROW_HASH",
        ])

    def bloquants(self) -> int:
        df = self.to_df()
        return len(df[df["severite"] == "BLOQUANT"]) if not df.empty else 0

    def a_controler(self) -> int:
        df = self.to_df()
        return len(df[df["severite"] == "A_CONTROLER"]) if not df.empty else 0


# ═══════════════════════════════════════════════════════════════
# WRITER
# ═══════════════════════════════════════════════════════════════
def write_excel(df: "pd.DataFrame", path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="data", index=False)


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT REF_LOGEMENTS
# ═══════════════════════════════════════════════════════════════
def load_known_listing_ids(ref_path: Path, log) -> set:
    """Charge les hostaway_listing_id de REF_Logements pour flag orphelin."""
    try:
        import openpyxl as xl
        wb = xl.load_workbook(ref_path, read_only=True, data_only=True)
        ws = wb["REF_Logements"]
        rows = list(ws.iter_rows(values_only=True))
        headers = list(rows[0])
        idx = headers.index("hostaway_listing_id")
        ids = {r[idx] for r in rows[1:] if r[idx] is not None}
        wb.close()
        log.info(f"REF_Logements : {len(ids)} IDs Hostaway connus (flag orphelin actif)")
        return ids
    except Exception as e:
        log.warning(f"REF_Logements illisible ({e}) — flag orphelin désactivé")
        return set()


# ═══════════════════════════════════════════════════════════════
# CLEANING TASKS — fonction isolée, non-bloquante
# ═══════════════════════════════════════════════════════════════
def _extract_cleaning_tasks(client, date_from: str, detector, log) -> tuple:
    """
    Extrait les tâches ménage. Retourne (rows_tasks, statut).
    statut: "OK" | "INCOMPLETE" | "FAILED"
    Non-bloquant : toujours retourne quelque chose, même vide.
    H6 : cost = NULL irrévocablement.
    """
    log.info("Extraction taches menage (cleaning tasks, H6)...")
    rows_tasks = []
    statut     = "FAILED"
    try:
        tasks_raw = client.get_tasks(date_from)
        for t in tasks_raw:
            rows_tasks.append({
                "task_id":        t.get("id"),
                "reservation_id": t.get("reservationId"),
                "listingMapId":   t.get("listingMapId"),
                "task_type":      t.get("taskType") or t.get("type"),
                "status":         t.get("status"),
                "scheduled_date": t.get("scheduledDate") or t.get("date"),
                "assignee":       t.get("assigneeName") or t.get("assignee"),
                "cost":           None,  # H6 : jamais valorise
                "h6_note":        "cost=NULL_H6_comptage_uniquement",
                "extrait_le":     now_utc(),
                "ROW_HASH":       row_hash(t.get("id")),
            })
        statut = "OK"
        log.info(f"  {len(rows_tasks)} taches menage extraites.")
    except Exception as e:
        log.warning(f"Extraction taches menage echouee : {type(e).__name__}: {e}")
        log.warning("  Les tables principales restent valides. Relancer avec --only-cleaning-tasks.")
        detector._add(None, "CLEANING_TASKS_EXTRACTION_INCOMPLETE", "A_CONTROLER",
                       f"Extraction taches menage incomplete : {type(e).__name__}")
        statut = "INCOMPLETE" if rows_tasks else "FAILED"
    return rows_tasks, statut


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Lot 1 — Extraction Hostaway API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check-auth", action="store_true",
        help="Test OAuth2 uniquement. Aucun secret affiché.",
    )
    parser.add_argument(
        "--check-listings", action="store_true",
        help="Auth + test /v1/listings : compte, champs, 3 IDs. Aucune écriture.",
    )
    parser.add_argument(
        "--check-reservations-sample", action="store_true",
        help="Auth + 5 réservations : clés, champs canal, nb UNKNOWN. Aucune écriture.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Auth + comptage réservations. Aucune écriture disque.",
    )
    parser.add_argument(
        "--details-mode", choices=[DETAILS_MINIMAL, DETAILS_FULL],
        default=DETAILS_MINIMAL,
        help="minimal (défaut) : détail uniquement si payout impossible. full : toujours.",
    )
    parser.add_argument(
        "--skip-cleaning-tasks", action="store_true",
        help="Ignorer l'extraction des tâches ménage (plus rapide).",
    )
    parser.add_argument(
        "--only-cleaning-tasks", action="store_true",
        help="Extraire uniquement les tâches ménage (nécessite tables réservations existantes).",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limiter à N réservations (0 = pas de limite). Pour tests.",
    )
    args = parser.parse_args()

    # ── Modes diagnostic : sys.exit() direct ─────────────────────
    if args.check_auth:
        check_auth_mode(ENV_FILE)
        # jamais atteint
    if args.check_listings:
        check_listings_mode(ENV_FILE)
    if args.check_reservations_sample:
        check_reservations_sample_mode(ENV_FILE)
        # jamais atteint

    run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_start = datetime.now(timezone.utc)
    log       = setup_logging(run_id, silent_file=args.dry_run)

    details_mode = getattr(args, "details_mode", DETAILS_MINIMAL)
    skip_tasks   = getattr(args, "skip_cleaning_tasks", False)
    only_tasks   = getattr(args, "only_cleaning_tasks", False)
    limit        = getattr(args, "limit", 0)

    log.info("=" * 60)
    log.info(f"LOT 1 — EXTRACTION HOSTAWAY — run_id={run_id}")
    if args.dry_run:
        mode_str = "--dry-run"
    elif only_tasks:
        mode_str = "--only-cleaning-tasks"
    else:
        mode_str = f"EXTRACTION (details={details_mode}{', limit='+str(limit) if limit else ''}{', skip-tasks' if skip_tasks else ''})"
    log.info(f"Mode     : {mode_str}")
    log.info(f"Date from: {DATE_FROM}")
    log.info("=" * 60)

    # ── Charger .env ─────────────────────────────────────────
    if not ENV_FILE.exists():
        log.error(f".env introuvable : {ENV_FILE}")
        sys.exit(1)

    load_dotenv(ENV_FILE)
    client_id     = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id    = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url      = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com")

    missing_vars = [k for k, v in [
        ("HOSTAWAY_CLIENT_ID", client_id),
        ("HOSTAWAY_CLIENT_SECRET", client_secret),
        ("HOSTAWAY_ACCOUNT_ID", account_id),
    ] if not v]

    if missing_vars:
        log.error(f"Variables manquantes dans .env : {', '.join(missing_vars)}")
        sys.exit(1)

    # Jamais de valeur affichée — CLIENT_ID, ACCOUNT_ID, CLIENT_SECRET masqués
    log.info("Variables .env : CLIENT_ID=*** ACCOUNT_ID=*** — présentes")
    log.info(f"BASE_URL     : {base_url}")

    # ── Auth ─────────────────────────────────────────────────
    log.info("Authentification OAuth2...")
    auth = HostawayAuth(base_url, client_id, client_secret)
    ok   = auth.test()
    if not ok:
        log.error("Authentification echouee. Lancer --check-auth pour diagnostic détaillé.")
        sys.exit(1)
    log.info("Authentification OK. Token valide (non affiché).")

    # ── Client ───────────────────────────────────────────────
    client = HostawayClient(auth, account_id, log)

    # ── Comptage (dry-run s'arrête ici) ──────────────────────
    log.info(f"Comptage réservations depuis {DATE_FROM}...")
    total = client.count_reservations(DATE_FROM)
    log.info(f"API Reservations count = {total}")

    if args.dry_run:
        log.info("--dry-run termine. Aucune ecriture. Lancer sans --dry-run pour extraire.")
        return

    # ── REF_Logements (flag orphelin) ────────────────────────
    known_ids = load_known_listing_ids(REF_FILE, log)
    detector  = AnomalyDetector(known_ids)
    payout_c  = PayoutCalculator()

    # Variables de suivi run (initialisées tôt pour le try/finally)
    rows_listings = []
    rows_res      = []
    rows_details  = []
    rows_ff       = []
    rows_fees     = []
    rows_payout   = []
    rows_tasks    = []
    processed     = 0
    skipped       = 0
    nb_details    = 0
    tasks_statut  = "SKIPPED" if (skip_tasks or only_tasks) else "PENDING"
    statut_run    = "FAILED"

    try:
        # ── ONLY-CLEANING-TASKS : branche dédiée ─────────────
        if only_tasks:
            log.info("Mode --only-cleaning-tasks : skip réservations/listings.")
            rows_tasks, tasks_statut = _extract_cleaning_tasks(client, DATE_FROM, detector, log)
            log.info("Ecriture MASTER_FACT_HA_CleaningTasks_Discovery...")
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_excel(pd.DataFrame(rows_tasks),
                        OUT_DIR / "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx")
            log.info(f"  {len(rows_tasks)} taches menage.")
            statut_run = "TERMINE_OK" if tasks_statut == "OK" else "PARTIAL_OK"
            total = 0
        else:
            # ── LISTINGS ─────────────────────────────────────
            log.info("Extraction listings via /v1/listings...")
            try:
                listings_raw = client.get_listings()
            except RuntimeError as e:
                log.error(f"Echec recuperation listings : {e}")
                sys.exit(1)

            for lst in listings_raw:
                lid    = lst.get("id")
                map_id = lst.get("listingMapId") or lid
                detector.check_listing(map_id)
                special = lst.get("specialStatus") or ""
                actif   = "NON" if special.strip() else "OUI"
                rows_listings.append({
                    "listingMapId":      map_id,
                    "listing_id_ha":     lid,
                    "nom_listing":       lst.get("name"),
                    "internalName":      lst.get("internalListingName") or lst.get("name"),
                    "ville":             lst.get("city"),
                    "actif":             actif,
                    "special_status":    special or None,
                    "airbnb_status":     lst.get("airbnbExportStatus"),
                    "bookingcom_status": lst.get("bookingcomExportStatus"),
                    "sur_hostaway":      "OUI",
                    "extrait_le":        now_utc(),
                    "ROW_HASH":          row_hash(map_id, lid),
                })
            log.info(f"  {len(rows_listings)} listings.")

            # ── RESERVATIONS (pagination) ─────────────────────
            log.info(f"Extraction reservations depuis {DATE_FROM} ({total} attendues, "
                     f"details={details_mode}{', limit='+str(limit) if limit else ''})...")
            offset = 0
            stopped_by_limit = False

            while True:
                batch = client.get_reservations_page(DATE_FROM, offset)
                if not batch:
                    break

                for res in batch:
                    # Limite de test
                    if limit and processed >= limit:
                        stopped_by_limit = True
                        break

                    res_id = res.get("id")
                    status = str(res.get("status") or "")

                    if status in STATUS_SKIP:
                        skipped += 1
                        continue

                    channel, source_brute = resolve_channel(res)
                    if channel == "UNKNOWN":
                        log.warning(f"  Reservation {res_id} — canal absent")

                    is_active    = status in STATUS_ACTIVE
                    is_cancelled = status in STATUS_CANCEL
                    is_owner     = status in STATUS_OWNER
                    cancel_amt   = safe_float(
                        _first(res.get("cancellationAmount"), res.get("cancellationPayout"))
                    )
                    include_financial = (is_active or (is_cancelled and cancel_amt > 0)) and not is_owner
                    include_any       = is_active or is_owner or (is_cancelled and cancel_amt > 0)

                    if not include_any:
                        skipped += 1
                        continue

                    try:
                        map_id = res.get("listingMapId")

                        rows_res.append({
                            "reservation_id":       res_id,
                            "listingMapId":         map_id,
                            "source":               source_brute,
                            "channel_type":         channel,
                            "source_financiere":    source_financiere(channel),
                            "status":               status,
                            "paymentStatus":        res.get("paymentStatus"),
                            "checkInDate":          res.get("arrivalDate") or res.get("checkInDate"),
                            "checkOutDate":         res.get("departureDate") or res.get("checkOutDate"),
                            "nights":               res.get("nights"),
                            "guestCount":           res.get("guestCount"),
                            "guestName":            res.get("guestName"),
                            "totalPrice":           safe_float(res.get("totalPrice")),
                            "cleaningFee_res":      safe_float(res.get("cleaningFee")),
                            "channelCommission":    safe_float(res.get("channelCommissionAmount")),
                            "airbnbExpectedPayout": safe_float(res.get("airbnbExpectedPayoutAmount")),
                            "is_ownerStay":         "OUI" if is_owner else "NON",
                            "inclure_resultat":     "OUI" if include_financial else "NON",
                            "updatedOn":            res.get("updatedOn"),
                            "createdOn":            res.get("createdOn"),
                            "extrait_le":           now_utc(),
                            "ROW_HASH":             row_hash(res_id, res.get("updatedOn")),
                        })

                        # Finance fields depuis la liste (sans appel détail)
                        ff_list_base = _ff_from_res(res)
                        fees_list    = res.get("reservationFees") or []

                        # Décision appel détail
                        fetch_detail = (
                            details_mode == DETAILS_FULL
                            or (not is_owner and _needs_detail(res, channel, ff_list_base))
                        )

                        if fetch_detail:
                            try:
                                detail = client.get_reservation_detail(res_id)
                                nb_details += 1
                                time.sleep(0.1)
                            except Exception as e:
                                log.warning(f"  Detail resa {res_id} echoue : {e}")
                                detail = {}

                            ff_list  = _ff_from_res(detail) or ff_list_base
                            fees_list = detail.get("reservationFees") or fees_list

                            rows_details.append({
                                "reservation_id": res_id,
                                "json_snapshot":  json.dumps(detail, ensure_ascii=False)[:4000],
                                "extrait_le":     now_utc(),
                                "ROW_HASH":       row_hash(res_id, "detail"),
                            })
                        else:
                            ff_list = ff_list_base

                        # Finance fields → table
                        for ff_item in ff_list:
                            name = ff_item.get("name", "")
                            if not name:
                                continue
                            rows_ff.append({
                                "reservation_id":     res_id,
                                "financeField_name":  name,
                                "financeField_value": safe_float(ff_item.get("value")),
                                "currency":           ff_item.get("currency", "EUR"),
                                "ROW_HASH":           row_hash(res_id, name),
                            })

                        # Fees → table
                        for fee in fees_list:
                            fee_id = fee.get("id") or row_hash(res_id, fee.get("name"), str(fee.get("amount")))
                            rows_fees.append({
                                "reservation_id": res_id,
                                "fee_id":         fee_id,
                                "fee_name":       fee.get("name"),
                                "fee_type":       fee.get("type"),
                                "amount":         safe_float(fee.get("amount")),
                                "currency":       fee.get("currency", "EUR"),
                                "ROW_HASH":       row_hash(res_id, str(fee_id)),
                            })

                        # Payout H1/H2/H3
                        if not is_owner:
                            payout, payout_src, payout_status, menage = payout_c.calc(
                                res, ff_list, fees_list, channel=channel
                            )
                            detector.check_reservation(res_id, channel, payout_status, map_id)
                            assiette = (payout - menage) if payout is not None else None
                            rows_payout.append({
                                "reservation_id":        res_id,
                                "listingMapId":          map_id,
                                "source":                source_brute,
                                "channel_type":          channel,
                                "statut_calcul_payout":  payout_status,
                                "payout_calcule":        payout,
                                "source_payout":         payout_src,
                                "menage_retenu":         menage,
                                "assiette_commission":   assiette,
                                "inclure_resultat_auto": "OUI" if payout_status == "NORMAL" else "NON",
                                "extrait_le":            now_utc(),
                                "ROW_HASH":              row_hash(res_id, payout_status),
                            })

                        processed += 1

                    except Exception as exc:
                        log.warning(f"  Resa {res_id} ignoree — {type(exc).__name__}: {exc}")
                        detector._add(res_id, "RESERVATION_SCHEMA_INATTENDU", "A_CONTROLER",
                                       f"Schema inattendu : {type(exc).__name__}")
                        skipped += 1

                if stopped_by_limit:
                    log.info(f"  --limit {limit} atteint — arret pagination.")
                    break

                log.info(f"  offset={offset:4d} : lot {len(batch)} — traite={processed} "
                         f"saute={skipped} details={nb_details}")
                if len(batch) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                time.sleep(0.1)

            log.info(f"Reservations : {processed} traitees, {skipped} sautees, "
                     f"{nb_details} appels detail sur {processed} ({100*nb_details//max(processed,1)}%)")

            # ── ÉCRITURE TABLES PRINCIPALES (avant cleaning tasks) ──
            log.info("Ecriture tables principales Excel...")
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            main_tables = {
                "MASTER_REF_HA_Listings":                 pd.DataFrame(rows_listings),
                "MASTER_FACT_HA_Reservations":             pd.DataFrame(rows_res),
                "MASTER_FACT_HA_ReservationDetails":       pd.DataFrame(rows_details),
                "MASTER_FACT_HA_ReservationFinanceFields": pd.DataFrame(rows_ff),
                "MASTER_FACT_HA_ReservationFees":          pd.DataFrame(rows_fees),
                "MASTER_CALC_HA_Payout":                   pd.DataFrame(rows_payout),
                "MASTER_CTRL_HA_Anomalies":                detector.to_df(),
            }
            for name, df in main_tables.items():
                write_excel(df, OUT_DIR / f"{name}.xlsx")
                log.info(f"  {name} : {len(df)} lignes")

            # ── CLEANING TASKS (non-bloquant) ─────────────────
            if skip_tasks:
                log.info("--skip-cleaning-tasks : taches menage ignorees.")
                tasks_statut = "SKIPPED"
            else:
                rows_tasks, tasks_statut = _extract_cleaning_tasks(client, DATE_FROM, detector, log)
                if tasks_statut != "OK":
                    log.warning("Taches menage incompletes — voir anomalie CLEANING_TASKS_EXTRACTION_INCOMPLETE.")

            write_excel(pd.DataFrame(rows_tasks),
                        OUT_DIR / "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx")
            log.info(f"  MASTER_FACT_HA_CleaningTasks_Discovery : {len(rows_tasks)} lignes [{tasks_statut}]")

            # Réécrire anomalies avec éventuelles anomalies cleaning
            write_excel(detector.to_df(), OUT_DIR / "MASTER_CTRL_HA_Anomalies.xlsx")

            nb_bloq = detector.bloquants()
            if nb_bloq > 0 or tasks_statut not in ("OK", "SKIPPED"):
                statut_run = "PARTIAL_OK" if tasks_statut not in ("OK", "SKIPPED") else "TERMINE_AVEC_BLOQUANTS"
            else:
                statut_run = "TERMINE_OK"

    except Exception as fatal:
        log.error(f"Erreur fatale run : {type(fatal).__name__}: {fatal}")
        statut_run = "FAILED"

    finally:
        # ── MASTER_RUN_LOG — toujours écrit ──────────────────
        run_end  = datetime.now(timezone.utc)
        duree    = round((run_end - run_start).total_seconds(), 1)
        anom_df  = detector.to_df()
        nb_bloq  = detector.bloquants()
        nb_actrl = detector.a_controler()
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        df_log = pd.DataFrame([{
            "run_id":                   run_id,
            "date_extraction":          run_start.isoformat(),
            "date_fin":                 run_end.isoformat(),
            "duree_secondes":           duree,
            "date_from":                DATE_FROM,
            "details_mode":             details_mode,
            "skip_cleaning_tasks":      str(skip_tasks),
            "limit":                    limit or "none",
            "nb_listings":              len(rows_listings),
            "nb_reservations_total_api":total if not only_tasks else 0,
            "nb_reservations_traitees": processed,
            "nb_reservations_sautees":  skipped,
            "nb_details_calls":         nb_details,
            "pct_details":              f"{100*nb_details//max(processed,1)}%",
            "nb_finance_fields":        len(rows_ff),
            "nb_fees":                  len(rows_fees),
            "nb_payout_calcules":       len(rows_payout),
            "nb_payout_NORMAL":         len([r for r in rows_payout if r.get("statut_calcul_payout") == "NORMAL"]),
            "nb_payout_INCOMPLET":      len([r for r in rows_payout if r.get("statut_calcul_payout") == "PAYOUT_INCOMPLET"]),
            "nb_payout_ABSENT":         len([r for r in rows_payout if r.get("statut_calcul_payout") == "PAYOUT_ABSENT"]),
            "nb_cleaning_tasks":        len(rows_tasks),
            "tasks_statut":             tasks_statut,
            "nb_anomalies_total":       len(anom_df),
            "nb_anomalies_BLOQUANT":    nb_bloq,
            "nb_anomalies_A_CONTROLER": nb_actrl,
            "statut_run":               statut_run,
            "lot1_validable":           "NON" if nb_bloq > 0 or statut_run == "FAILED"
                                        else "OUI_sous_reserve_controle",
            "commentaire":              f"Lot1 {details_mode} depuis {DATE_FROM}",
        }])
        try:
            write_excel(df_log, OUT_DIR / "MASTER_RUN_Log.xlsx")
        except Exception as e:
            log.error(f"Impossible d'ecrire MASTER_RUN_Log : {e}")

    # ── RÉSUMÉ CONSOLE (dans finally les vars sont disponibles) ──
    run_end_summary = datetime.now(timezone.utc)
    duree_s = round((run_end_summary - run_start).total_seconds(), 1)
    nb_bloq_s  = detector.bloquants()
    nb_actrl_s = detector.a_controler()
    log.info("=" * 60)
    log.info(f"RUN {statut_run} — {run_id}  ({duree_s}s)")
    log.info(f"  Listings           : {len(rows_listings)}")
    log.info(f"  Reservations API   : {total if not only_tasks else 'n/a'}")
    log.info(f"  Traitees           : {processed}")
    log.info(f"  Sautees (skip)     : {skipped}")
    log.info(f"  Appels detail      : {nb_details} ({100*nb_details//max(processed,1)}%)")
    log.info(f"  Finance fields     : {len(rows_ff)}")
    log.info(f"  Fees               : {len(rows_fees)}")
    log.info(f"  Payouts calcules   : {len(rows_payout)}")
    log.info(f"    NORMAL           : {len([r for r in rows_payout if r.get('statut_calcul_payout')=='NORMAL'])}")
    log.info(f"    INCOMPLET        : {len([r for r in rows_payout if r.get('statut_calcul_payout')=='PAYOUT_INCOMPLET'])}")
    log.info(f"    ABSENT           : {len([r for r in rows_payout if r.get('statut_calcul_payout')=='PAYOUT_ABSENT'])}")
    log.info(f"    A_CONTROLER      : {len([r for r in rows_payout if r.get('statut_calcul_payout')=='A_CONTROLER'])}")
    log.info(f"  Taches menage      : {len(rows_tasks)} [{tasks_statut}]")
    log.info(f"  Anomalies BLOQUANT : {nb_bloq_s}")
    log.info(f"  Anomalies A_CTRL   : {nb_actrl_s}")
    log.info("=" * 60)
    if nb_bloq_s > 0:
        log.warning(f"  ATTENTION : {nb_bloq_s} anomalie(s) BLOQUANTE(s) — Lot 1 NON validable.")
        log.warning("  Voir MASTER_CTRL_HA_Anomalies.xlsx")
    elif statut_run in ("TERMINE_OK", "PARTIAL_OK"):
        log.info("  Aucun bloquant. Verification humaine requise avant marquage FAIT.")
    log.info(f"  Tables dans : {OUT_DIR}")


if __name__ == "__main__":
    main()

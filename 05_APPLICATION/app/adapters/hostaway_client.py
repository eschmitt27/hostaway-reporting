"""Client Hostaway canonique — auth OAuth2, appels API avec retry/rate-limit, détection
d'anomalie, extraction CleaningTasks.

DÉPLACÉ depuis le script legacy `lot1_hostaway_extract.py` (mission stabilisation 2026-09-09), à
l'IDENTIQUE (aucune ligne de comportement changée), pour corriger une violation d'architecture :
`app/services/hostaway_cleaning_tasks_actualisation_service.py` atteignait ce script legacy
directement pour ses classes — interdit par la règle testée dans
`tests/test_no_metier_calc.py::test_no_import_of_travail_modules` (le code applicatif sous `app/`
ne référence jamais un module du dossier des scripts historiques).

Ce module est maintenant la SEULE définition de ces classes/fonctions. Le script legacy en devient
un CONSOMMATEUR (bootstrap `sys.path` puis import, exactement le même schéma que sa propre
fonction `_service_raw()`, qui rejoint déjà `app.services.hostaway_raw_service` en sens inverse) —
jamais une seconde copie qui diverge au premier changement.

Deux consommateurs, un seul comportement :
  - `app/services/hostaway_cleaning_tasks_actualisation_service.py` (API → SQLite direct, in-process)
  - le script legacy d'extraction complète Lot1 (réservations/listings/payout)
"""
from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# ── Constantes retry/pagination ──────────────────────────────────────────────────────────────
PAGE_SIZE       = 100
MAX_RETRIES     = 3
RETRY_BASE_WAIT = 2   # secondes (exponentiel)
RETRY_MAX_WAIT  = 60  # plafond d'une attente unitaire (s)
RETRY_BUDGET_S  = 180 # budget total d'attente pour UNE requete (s)


class RateLimitEpuise(RuntimeError):
    """Le debit de l'API a bloque la requete jusqu'a epuisement des tentatives.

    Type distinct d'une erreur HTTP ordinaire : il dit « donnee non obtenue », jamais
    « donnee absente ». C'est precisement la confusion qui a coute une heure d'appels vides.
    """

    def __init__(self, path, tentatives, attente_totale):
        self.path = path
        self.tentatives = tentatives
        self.attente_totale = attente_totale
        super().__init__(
            f"Rate limit HTTP 429 non resorbe sur {path} apres {tentatives} tentative(s) "
            f"et {attente_totale:.0f}s d'attente. Donnee NON obtenue (et non pas absente).")


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
        attente_totale = 0.0
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=60)
                if r.status_code == 429:
                    # `Retry-After` fait foi quand l'API le fournit : c'est elle qui sait quand
                    # elle acceptera de repondre. Sinon, backoff exponentiel PLAFONNE.
                    entete = (r.headers or {}).get("Retry-After")
                    wait = None
                    if entete:
                        try:
                            wait = float(str(entete).strip())
                        except ValueError:
                            wait = None
                    if wait is None:
                        wait = RETRY_BASE_WAIT * (2 ** attempt)
                    wait = min(wait, RETRY_MAX_WAIT)
                    # Budget d'attente par requete : sans lui, une API durablement saturee
                    # ferait patienter indefiniment sans jamais rien ramener.
                    if attente_totale + wait > RETRY_BUDGET_S or attempt == MAX_RETRIES:
                        raise RateLimitEpuise(path, attempt, attente_totale + wait)
                    self._log.warning(
                        f"Rate limit 429 sur {path} — attente {wait:.0f}s "
                        f"(tentative {attempt}/{MAX_RETRIES}, cumul {attente_totale:.0f}s)")
                    time.sleep(wait)
                    attente_totale += wait
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
            except RateLimitEpuise:
                raise  # remonter tel quel : « non obtenu » n'est pas « absent »
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
        # Jamais `return {}` ici. Rendre un dictionnaire vide apres epuisement des tentatives
        # faisait passer « je n'ai pas pu lire » pour « il n'y a rien » : l'appelant enchainait
        # sur la page suivante et le lot produisait des sorties amputees sans rien signaler.
        raise RuntimeError(
            f"Aucune reponse exploitable de {path} apres {MAX_RETRIES} tentatives.")

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
        """`/v1/tasks` ignore `limit`/`offset` : chaque appel rend la totalité du résultat
        (constaté : `count`=727, `result` de longueur 727 quel que soit `offset`). Le test
        `len(batch) < PAGE_SIZE` ne s'arrête donc jamais — boucle infinie observée en
        production (diagnostic mission cleaning-tasks). Arrêt dès que `count` (fourni par
        l'API) est atteint, avec dédoublonnage défensif par `id` au cas où un futur
        comportement de l'API redevienne partiellement paginé."""
        results, offset = [], 0
        # Défense en profondeur (mission cleaning-tasks) : la boucle est DÉJÀ bornée par le test
        # count/len(batch) ci-dessus — ce plafond ne fait rien tant que l'API se comporte comme
        # constaté. Il existe pour qu'une régression future de l'API (retour à une pagination
        # partielle incohérente, `count` toujours nul, etc.) ne puisse plus jamais reproduire la
        # boucle infinie historique : 200 pages à PAGE_SIZE=100 couvre large le volume réel observé
        # (count≈727, soit ~8 pages).
        MAX_PAGES = 200
        for _ in range(MAX_PAGES):
            data  = self._get("/v1/tasks", {
                "dateFrom": date_from, "limit": PAGE_SIZE, "offset": offset,
            })
            batch = data.get("result", [])
            results.extend(batch)
            total = data.get("count")
            if not batch or len(batch) < PAGE_SIZE or (total is not None and len(results) >= total):
                break
            offset += PAGE_SIZE
            time.sleep(0.2)
        else:
            raise RuntimeError(
                f"HostawayClient.get_tasks : plafond de {MAX_PAGES} pages atteint sans fin de "
                "pagination détectée — arrêt (garde-fou anti-boucle infinie).")
        seen: set = set()
        deduped = []
        for t in results:
            tid = t.get("id")
            if tid in seen:
                continue
            seen.add(tid)
            deduped.append(t)
        return deduped


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def row_hash(*values) -> str:
    raw = "|".join(str(v) for v in values).encode()
    return hashlib.md5(raw).hexdigest()[:16]

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
        # L'IDENTIFIANT EST UN LIBELLÉ, PAS UN NOMBRE.
        #
        # `to_df()` construit un DataFrame de ces lignes. Dès qu'une seule anomalie porte
        # `reservation_id = None` — c'est le cas de `check_listing`, qui vise une annonce et non une
        # réservation — pandas type la colonne en flottant et réécrit tous les autres identifiants
        # « 66096017 » en « 66096017.0 ». Ces lignes ne se rattachent alors plus à aucune
        # réservation : l'anomalie existe, mais on ne peut plus dire laquelle elle concerne.
        #
        # Le défaut est resté invisible tant que toutes les annonces étaient connues. Il est apparu
        # à la première annonce Hostaway absente du parc. Convertir ici, à l'écriture, est la seule
        # place où la valeur est encore sûrement un identifiant.
        self._rows.append({
            "reservation_id":  None if res_id is None else str(res_id),
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
# CLEANING TASKS — fonction isolée, non-bloquante
# ═══════════════════════════════════════════════════════════════
def extraire_cleaning_tasks(client, date_from: str, detector, log) -> tuple:
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
            # `scheduledDate`/`date` n'existent plus dans le schema /v1/tasks constate (diagnostic
            # mission cleaning-tasks) : la tache expose desormais `canStartFrom`
            # ("2026-02-22 10:00:00", date-heure a laquelle le menage peut demarrer, juste apres le
            # depart) — c'est le champ le plus proche d'une date de menage planifiee. Sans lui,
            # `shouldEndBy` (echeance) en repli. Avant ce correctif, l'absence totale de ces deux
            # champs laissait `scheduled_date` toujours vide (727/727 taches A_CONTROLER).
            date_brute = t.get("canStartFrom") or t.get("shouldEndBy")
            scheduled_date = date_brute.split(" ")[0] if date_brute else None
            rows_tasks.append({
                "task_id":        t.get("id"),
                "reservation_id": t.get("reservationId"),
                "listingMapId":   t.get("listingMapId"),
                "task_type":      t.get("taskType") or t.get("type"),
                "status":         t.get("status"),
                "scheduled_date": scheduled_date,
                "assignee":       t.get("assigneeName") or t.get("assignee"),
                # Champs bruts additionnels — requis par le pont SQLite
                # (`hostaway_cleaning_tasks_raw_service`, colonnes `title`/`can_start_from`/
                # `assignee_user_id`, alias sur les noms API ci-dessous). Sans eux, la reprise via
                # Excel/`depuis_master_excel` les laissait toujours NULL en SQLite (diagnostic
                # mission cleaning-tasks) — jamais lus par ce dict avant ce correctif.
                "title":          t.get("title"),
                "canStartFrom":   t.get("canStartFrom"),
                "assigneeUserId": t.get("assigneeUserId"),
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

"""Lot 1 — pagination `/v1/tasks` (diagnostic mission Hostaway CleaningTasks).

Constaté en réel (2026-09-02) : `/v1/tasks` ignore `limit`/`offset` — chaque appel rend
INTÉGRALEMENT le résultat (`count`=727 pour toute valeur d'`offset`, y compris 0/100/400/500/600/
700). `HostawayClient.get_tasks()` bouclait sur `len(batch) < PAGE_SIZE`, jamais vrai puisque
`batch` valait toujours 727 ≥ `PAGE_SIZE`(100) : boucle infinie, aucune erreur, aucune progression
visible — l'incident observé (36+ minutes sans aboutissement) n'était pas un débit limité (aucun
429 constaté), mais cette boucle qui ne se termine jamais.

Aucun appel réseau : `requests.get` est remplacé par un double.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

RACINE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RACINE / "02_TRAVAIL"))

LOT1 = RACINE / "02_TRAVAIL" / "lot1_hostaway_extract.py"
pytestmark = pytest.mark.skipif(not LOT1.exists(), reason="lot1 absent")


def _module(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["lot1_hostaway_extract.py"])
    sys.modules.pop("lot1_hostaway_extract", None)
    import lot1_hostaway_extract as mod
    return mod


class _Reponse:
    def __init__(self, status, headers=None, payload=None):
        self.status_code = status
        self.headers = headers or {}
        self._payload = payload if payload is not None else {"result": []}

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def _client(mod):
    client = mod.HostawayClient.__new__(mod.HostawayClient)
    client._base = "https://exemple.test"
    client._log = types.SimpleNamespace(
        warning=lambda *a, **k: None, info=lambda *a, **k: None, error=lambda *a, **k: None)
    client._headers = lambda: {}
    return client


def test_offset_ignore_par_l_api_ne_boucle_pas_indefiniment(monkeypatch):
    """Reproduit exactement le comportement réel constaté : chaque appel rend les 727 mêmes
    tâches quel que soit `offset`. Le correctif doit s'arrêter dès le premier appel."""
    mod = _module(monkeypatch)
    client = _client(mod)
    taches_completes = [{"id": i} for i in range(727)]
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        appels["n"] += 1
        # Comportement réel : `limit`/`offset` ignorés, `result` et `count` identiques à chaque appel.
        return _Reponse(200, payload={"result": taches_completes, "count": 727,
                                       "offset": kwargs.get("params", {}).get("offset", 0),
                                       "status": "success"})

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    resultat = client.get_tasks("2026-01-01")

    assert appels["n"] == 1, "un seul appel suffit : `count` atteint dès la première réponse"
    assert len(resultat) == 727
    assert [t["id"] for t in resultat] == list(range(727)), "aucun doublon, ordre préservé"


def test_pagination_reelle_toujours_geree_correctement(monkeypatch):
    """Cas où l'API paginerait réellement (comportement documenté à l'origine) : le correctif ne
    doit pas casser ce chemin — il doit toujours s'arrêter proprement quand une page est plus
    courte que `PAGE_SIZE`."""
    mod = _module(monkeypatch)
    client = _client(mod)
    pages = [
        {"result": [{"id": i} for i in range(100)], "count": 150},
        {"result": [{"id": i} for i in range(100, 150)], "count": 150},
    ]
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        page = pages[appels["n"]]
        appels["n"] += 1
        return _Reponse(200, payload=page)

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    resultat = client.get_tasks("2026-01-01")

    assert appels["n"] == 2
    assert len(resultat) == 150
    assert [t["id"] for t in resultat] == list(range(150))


def test_reponse_vide_arrete_la_boucle(monkeypatch):
    mod = _module(monkeypatch)
    client = _client(mod)
    monkeypatch.setattr(mod.requests, "get",
                        lambda url, **k: _Reponse(200, payload={"result": [], "count": 0}))
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    assert client.get_tasks("2026-01-01") == []


def test_scheduled_date_derivee_de_canstartfrom(monkeypatch):
    """`scheduledDate`/`date` n'existent plus dans le schéma /v1/tasks réel (constaté) : la date
    doit venir de `canStartFrom` (ou `shouldEndBy` en repli), jamais rester vide alors qu'un de
    ces deux champs est présent."""
    mod = _module(monkeypatch)
    log = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
    detector = types.SimpleNamespace(_add=lambda *a, **k: None)
    client = _client(mod)
    monkeypatch.setattr(client, "get_tasks", lambda date_from: [
        {"id": 1, "reservationId": 10, "listingMapId": 20, "status": "completed",
         "title": "Ménage Test", "canStartFrom": "2026-03-15 10:00:00",
         "shouldEndBy": "2026-03-15 15:00:00", "assigneeUserId": None},
        {"id": 2, "reservationId": 11, "listingMapId": 21, "status": "confirmed",
         "title": "Ménage Test 2", "canStartFrom": None,
         "shouldEndBy": "2026-04-01 15:00:00", "assigneeUserId": None},
    ])

    rows, statut = mod._extract_cleaning_tasks(client, "2026-01-01", detector, log)

    assert statut == "OK"
    assert rows[0]["scheduled_date"] == "2026-03-15"
    assert rows[1]["scheduled_date"] == "2026-04-01", "repli sur shouldEndBy si canStartFrom absent"
    assert rows[0]["canStartFrom"] == "2026-03-15 10:00:00", "champ brut conservé pour le pont SQLite"
    assert rows[0]["title"] == "Ménage Test"

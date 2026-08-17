"""Lot 1 — comportement face au débit limité de l'API (HTTP 429).

Le défaut corrigé est plus grave qu'une simple boucle : après épuisement des tentatives, `_get`
faisait `return {}`. Une page bloquée par le débit devenait donc **« aucune donnée »**, l'appelant
enchaînait sur la suivante, et le lot produisait des sorties amputées sans rien signaler. C'est ce
qui a fait tourner l'étape des tâches ménage plus d'une heure pour rien le 2026-08-17.

« Je n'ai pas pu lire » et « il n'y a rien à lire » sont deux réponses opposées. Ces tests
verrouillent la distinction.

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


def _client(mod, reponses):
    """Client Hostaway dont chaque appel rend la réponse suivante de la liste."""
    client = mod.HostawayClient.__new__(mod.HostawayClient)
    client._base = "https://exemple.test"
    client._log = types.SimpleNamespace(
        warning=lambda *a, **k: None, info=lambda *a, **k: None, error=lambda *a, **k: None)
    client._headers = lambda: {}
    return client


# ── D. Retry-After honoré ───────────────────────────────────────────────────────────────────────

def test_retry_after_est_respecte(monkeypatch):
    """Quand l'API dit quand revenir, c'est elle qui sait."""
    mod = _module(monkeypatch)
    client = _client(mod, None)
    attentes: list[float] = []
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        appels["n"] += 1
        if appels["n"] == 1:
            return _Reponse(429, {"Retry-After": "7"})
        return _Reponse(200, payload={"result": [{"id": 1}]})

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: attentes.append(s))

    assert client._get("/v1/x") == {"result": [{"id": 1}]}
    assert attentes == [7.0], "l'attente doit venir de l'en-tête, pas du backoff calculé"


def test_retry_after_plafonne(monkeypatch):
    """Un Retry-After démesuré ne doit pas immobiliser le lot."""
    mod = _module(monkeypatch)
    client = _client(mod, None)
    attentes: list[float] = []
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        appels["n"] += 1
        return _Reponse(429, {"Retry-After": "99999"}) if appels["n"] == 1 else _Reponse(200)

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: attentes.append(s))
    client._get("/v1/x")
    assert attentes == [mod.RETRY_MAX_WAIT]


def test_retry_after_illisible_bascule_sur_le_backoff(monkeypatch):
    mod = _module(monkeypatch)
    client = _client(mod, None)
    attentes: list[float] = []
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        appels["n"] += 1
        return _Reponse(429, {"Retry-After": "bientôt"}) if appels["n"] == 1 else _Reponse(200)

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: attentes.append(s))
    client._get("/v1/x")
    assert attentes and attentes[0] > 0


# ── E. Épuisement : lève, ne rend jamais du vide ────────────────────────────────────────────────

def test_429_persistant_leve_une_erreur_explicite(monkeypatch):
    """LE test central : un débit non résorbé ne doit JAMAIS ressembler à une absence de données."""
    mod = _module(monkeypatch)
    client = _client(mod, None)

    monkeypatch.setattr(mod.requests, "get", lambda url, **k: _Reponse(429))
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    with pytest.raises(mod.RateLimitEpuise) as exc:
        client._get("/v1/cleaningTasks")

    message = str(exc.value)
    assert "429" in message
    assert "NON obtenue" in message, "le message doit distinguer « non obtenu » de « absent »"
    assert "cleaningTasks" in message


def test_le_nombre_de_tentatives_est_borne(monkeypatch):
    mod = _module(monkeypatch)
    client = _client(mod, None)
    appels = {"n": 0}

    def faux_get(url, **kwargs):
        appels["n"] += 1
        return _Reponse(429)

    monkeypatch.setattr(mod.requests, "get", faux_get)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    with pytest.raises(mod.RateLimitEpuise):
        client._get("/v1/x")
    assert appels["n"] <= mod.MAX_RETRIES, f"{appels['n']} appels : la boucle n'est pas bornée"


def test_budget_d_attente_borne(monkeypatch):
    """L'attente cumulée d'une requête ne peut pas dépasser le budget."""
    mod = _module(monkeypatch)
    client = _client(mod, None)
    attentes: list[float] = []

    monkeypatch.setattr(mod.requests, "get",
                        lambda url, **k: _Reponse(429, {"Retry-After": str(mod.RETRY_MAX_WAIT)}))
    monkeypatch.setattr(mod.time, "sleep", lambda s: attentes.append(s))
    with pytest.raises(mod.RateLimitEpuise):
        client._get("/v1/x")
    assert sum(attentes) <= mod.RETRY_BUDGET_S


def test_aucun_retour_vide_apres_epuisement():
    """Garde-fou textuel : `return {}` en fin de `_get` était la faute d'origine."""
    source = LOT1.read_text(encoding="utf-8")
    corps = source[source.index("def _get(self"):]
    corps = corps[:corps.index("\n    # ── Endpoints")]
    lignes = [l.strip() for l in corps.splitlines()
              if l.strip().startswith("return") and not l.strip().startswith("#")]
    assert "return {}" not in lignes, "un retour vide masquerait une lecture impossible"


def test_une_reponse_normale_passe_sans_attente(monkeypatch):
    """Le correctif ne doit pas ralentir le cas nominal."""
    mod = _module(monkeypatch)
    client = _client(mod, None)
    attentes: list[float] = []
    monkeypatch.setattr(mod.requests, "get",
                        lambda url, **k: _Reponse(200, payload={"result": [1, 2]}))
    monkeypatch.setattr(mod.time, "sleep", lambda s: attentes.append(s))
    assert client._get("/v1/x") == {"result": [1, 2]}
    assert attentes == []

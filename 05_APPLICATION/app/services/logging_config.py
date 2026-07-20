"""Logger applicatif centralisé (APP-SEC-1) — jamais de print() technique brut.

Un seul logger nommé `"pilotage"`, niveau configurable (`LOG_LEVEL`), sortie locale (stderr par
défaut, capturée par pytest/uvicorn). Toute donnée journalisée passe par le sanitizer avant écriture :
jamais de chemin absolu, jamais de traceback complet brut, jamais de secret. N'accepte jamais l'objet
`Request` complet — seuls la méthode et le chemin logique sont journalisés.
"""
from __future__ import annotations

import logging
import os

from app.services.path_sanitizer import sanitize_text

_LOGGER_NOM = "pilotage"
_logger: logging.Logger | None = None


def get_logger() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger
    logger = logging.getLogger(_LOGGER_NOM)
    if not logger.handlers:
        niveau = os.environ.get("LOG_LEVEL", "info").upper()
        logger.setLevel(getattr(logging, niveau, logging.INFO))
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    _logger = logger
    return logger


def log_erreur(ref: str, exc: BaseException, methode: str = "", route: str = "") -> None:
    """Journalise une erreur non gérée : référence, type, message sanitisé, méthode, route logique.

    Jamais de chemin absolu, jamais de traceback complet, jamais de headers/cookies/corps de requête.
    """
    detail = sanitize_text(str(exc))[:500]
    get_logger().error(f"[{ref}] {type(exc).__name__}: {detail} | {methode} {route}")

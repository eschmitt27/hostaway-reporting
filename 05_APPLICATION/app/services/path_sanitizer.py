"""Sanitisation centralisée des chemins (APP-SEC-1) — jamais de chemin réel côté client.

Remplace toute occurrence de racines sensibles (PROJECT_ROOT, APP_DATA_DIR, worktree courant,
répertoire utilisateur, dossier temporaire) par un jeton logique stable, dans des chaînes, des
`Path`, des messages d'exception et des textes de commande. Le NOM D'UTILISATEUR est masqué en
plus, pour lui-même : les jetons de racine conservent leur suffixe, et ce suffixe peut le
contenir. Le remplacement se fait sur le chemin le
plus SPÉCIFIQUE en premier (le plus long), pour ne jamais laisser un préfixe partiel révélateur
(ex. le nom du profil Windows) une fois PROJECT_ROOT/APP_DATA_DIR retirés.
"""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

_TOKENS = {
    "PROJECT_ROOT": "<PROJECT_ROOT>",
    "APP_DATA_DIR": "<APP_DATA_DIR>",
    "WORKTREE": "<WORKTREE>",
    "USER_HOME": "<USER_HOME>",
    "TEMP": "<TEMP>",
}


def _norm(s: str) -> str:
    """Casse Windows insensible, slashs unifiés — pour la recherche uniquement (pas la sortie)."""
    return s.replace("/", "\\").lower()


def _racines_sensibles() -> list[tuple[str, str]]:
    """[(chemin_absolu, jeton)] triés du plus long au plus court (le plus spécifique gagne)."""
    import app.config as cfg

    candidats: list[tuple[str, str]] = []
    for attr, jeton in (
        ("PROJECT_ROOT", "PROJECT_ROOT"),
        ("DATA_DIR", "APP_DATA_DIR"),
    ):
        v = getattr(cfg, attr, None)
        if v:
            candidats.append((str(v), _TOKENS[jeton]))
    app_root = getattr(cfg, "APP_ROOT", None)
    if app_root:
        candidats.append((str(Path(app_root).parent), _TOKENS["WORKTREE"]))  # racine du worktree
    home = str(Path.home())
    if home:
        candidats.append((home, _TOKENS["USER_HOME"]))
    tmp = tempfile.gettempdir()
    if tmp:
        candidats.append((tmp, _TOKENS["TEMP"]))
    # dédoublonner en gardant la première occurrence (ordre = priorité)
    vus: set[str] = set()
    uniq = []
    for chemin, jeton in candidats:
        k = _norm(chemin)
        if k and k not in vus:
            vus.add(k)
            uniq.append((chemin, jeton))
    # le plus long (le plus spécifique) en premier
    return sorted(uniq, key=lambda t: -len(t[0]))


# Chemin Windows absolu générique : lettre de lecteur + `:\` ou `:/`, suivi de segments sans espace ni
# guillemet/chevron. Sert de FILET après les racines connues : garantit qu'aucune lettre de lecteur
# suivie de `:\` ne survit, même pour un chemin dont la racine n'est pas dans la liste connue
# (ex. profil Windows différent apparu dans un message d'erreur tiers).
_RE_CHEMIN_QUOTE_DBL = re.compile(r'"([A-Za-z]:[\\/][^"]*)"')
_RE_CHEMIN_QUOTE_SPL = re.compile(r"'([A-Za-z]:[\\/][^']*)'")
_RE_CHEMIN_GENERIQUE = re.compile(r'[A-Za-z]:[\\/][^\s"\'<>]*')
_RE_UNC = re.compile(r'\\\\[^\s"\'<>]*')
_RE_ENV_VARS = re.compile(r'%USERPROFILE%|%TEMP%|%APPDATA%|%LOCALAPPDATA%', re.IGNORECASE)

_TOKEN_GENERIQUE = "<PATH>"

_TOKEN_UTILISATEUR = "<USER>"

# Longueur minimale pour masquer le nom d'utilisateur. Un nom très court risquerait de mutiler du
# texte légitime ; en dessous de ce seuil on préfère ne rien faire plutôt que rendre un message
# illisible.
_LONGUEUR_MIN_UTILISATEUR = 3


def _nom_utilisateur() -> str:
    """Nom du compte courant, ou chaîne vide."""
    return (os.environ.get("USERNAME") or os.environ.get("USER")
            or Path.home().name or "").strip()

# Identifiants bancaires reconnus (forme réelle du codebase, cf. banques_reader.masquer_compte /
# banques_controle_service.id_opaque) : ne doivent jamais apparaître bruts, même hors chemin.
_RE_MOUVEMENT_BRUT = re.compile(r'MVT-CM_\d+_\d+[A-Za-z0-9\-]*')
_RE_COMPTE_BRUT = re.compile(r'\bCM_\d{4,}_\d{6,}\b')
_RE_IBAN = re.compile(r'\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b')


def sanitize_text(texte: Any) -> str:
    """Remplace toutes les racines sensibles connues dans une chaîne libre, puis applique un filet
    générique sur tout chemin Windows absolu ou UNC restant (racine inconnue).

    Insensible à la casse Windows, gère les deux séparateurs (`\\`, `/`), les chemins entre
    guillemets, les chemins UNC et le préfixe étendu Windows, les variables d'environnement
    littérales (`%USERPROFILE%`, `%TEMP%`), et traite plusieurs occurrences dans un même texte
    (message d'exception, ligne de commande, traceback). Idempotent : sanitiser un texte déjà sanitisé ne le
    modifie pas (les jetons `<...>` ne correspondent à aucun motif de chemin).
    """
    if texte is None:
        return ""
    out = str(texte)
    if not out:
        return out
    # 1) racines connues (les plus spécifiques d'abord) — jetons sémantiques précis.
    #    USER_HOME est la racine la plus générique (n'importe quel sous-dossier Windows peut suivre,
    #    Desktop/Documents/OneDrive/...) : on consomme tout le reste du segment de chemin pour ne
    #    jamais laisser un nom de sous-dossier après le token. Les autres racines (PROJECT_ROOT,
    #    APP_DATA_DIR, WORKTREE, TEMP) conservent leur suffixe (utile au diagnostic : ex.
    #    `<PROJECT_ROOT>\01_SOURCES_BRUTES\...`).
    for chemin, jeton in _racines_sensibles():
        suffixe = r'[^\s"\'<>]*' if jeton == _TOKENS["USER_HOME"] else ""
        for variante in (chemin, chemin.replace("\\", "/")):
            if not variante:
                continue
            out = re.sub(re.escape(variante) + suffixe, jeton, out, flags=re.IGNORECASE)
    # 2) variables d'environnement littérales
    out = _RE_ENV_VARS.sub(_TOKEN_GENERIQUE, out)
    # 3) filet générique — chemins entre guillemets d'abord (préserve les guillemets), puis nus,
    #    puis UNC/`\\?\` — chaque motif remplace le chemin ENTIER (jamais un fragment) pour ne
    #    jamais laisser de sous-chaîne partielle révélatrice.
    out = _RE_CHEMIN_QUOTE_DBL.sub(lambda m: f'"{_TOKEN_GENERIQUE}"', out)
    out = _RE_CHEMIN_QUOTE_SPL.sub(lambda m: f"'{_TOKEN_GENERIQUE}'", out)
    out = _RE_UNC.sub(_TOKEN_GENERIQUE, out)
    out = _RE_CHEMIN_GENERIQUE.sub(_TOKEN_GENERIQUE, out)
    # 4) identifiants bancaires reconnus (défense en profondeur) : un compte/mouvement brut ne doit
    #    jamais apparaître, même dans un message d'exception non prévu par le code applicatif normal
    #    (qui masque déjà systématiquement via masquer_compte()/id_opaque() avant toute mise en texte).
    out = _RE_MOUVEMENT_BRUT.sub("<MVT_BRUT>", out)
    out = _RE_COMPTE_BRUT.sub("<COMPTE_BRUT>", out)
    out = _RE_IBAN.sub("<IBAN>", out)
    # 5) nom d'utilisateur, où qu'il apparaisse.
    #    Les racines connues ne le masquent que lorsqu'il fait partie du chemin du profil. Or il se
    #    retrouve aussi dans des NOMS DE DOSSIER conservés après un jeton de racine — ces jetons
    #    gardent volontairement leur suffixe, utile au diagnostic — et dans des messages d'outils
    #    tiers. Le nom doit donc être masqué pour lui-même, pas seulement comme partie d'un chemin.
    utilisateur = _nom_utilisateur()
    if len(utilisateur) >= _LONGUEUR_MIN_UTILISATEUR:
        out = re.sub(re.escape(utilisateur), _TOKEN_UTILISATEUR, out, flags=re.IGNORECASE)
    return out


def sanitize_path(chemin: Any) -> str:
    """Sanitise un `Path` ou une chaîne de chemin (absolu, relatif, inexistant)."""
    return sanitize_text(str(chemin))


def sanitize_exception(exc: BaseException, limite: int = 500) -> str:
    """Message d'exception sanitisé, tronqué — jamais de chemin brut, jamais de traceback complet."""
    return sanitize_text(str(exc))[:limite]


def sanitize_command(args: list[str] | str) -> str:
    """Ligne de commande (subprocess) sanitisée pour affichage/log."""
    texte = args if isinstance(args, str) else " ".join(str(a) for a in args)
    return sanitize_text(texte)

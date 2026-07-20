"""Démarrage local (APP-SEC-1) — application mono-utilisateur, aucune exposition réseau par défaut.

Hôtes autorisés sans configuration : 127.0.0.1 et localhost. Toute autre valeur (0.0.0.0, IP LAN,
IPv6 global) est refusée sauf `cfg.ALLOW_NETWORK_BIND = True` explicite (variable d'environnement
`ALLOW_NETWORK_BIND=true`) — jamais activé par défaut. Les chemins ne sont jamais affichés au
démarrage : seul l'état logique (writers activés ou non) l'est.
"""
import socket
import sys

import uvicorn

from app.config import PORT, LOG_LEVEL, ALLOW_NETWORK_BIND

HOST = "127.0.0.1"
_HOTES_LOCAUX = {"127.0.0.1", "localhost", "::1"}


def _refuse_si_reseau(host: str) -> None:
    if host in _HOTES_LOCAUX:
        return
    if ALLOW_NETWORK_BIND:
        print(f"[AVERTISSEMENT] Écoute réseau explicitement autorisée sur {host} (ALLOW_NETWORK_BIND=true).")
        return
    sys.exit(
        f"[REFUS] Hôte '{host}' non local refusé par défaut. "
        "Application locale mono-utilisateur : définir ALLOW_NETWORK_BIND=true pour l'autoriser explicitement."
    )


def _instance_deja_active(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


if __name__ == "__main__":
    _refuse_si_reseau(HOST)
    if _instance_deja_active(HOST, PORT):
        sys.exit(f"[REFUS] Une instance écoute déjà sur {HOST}:{PORT}. Arrêtez-la ou changez PORT.")

    print(f"Démarrage local — http://{HOST}:{PORT}")
    from app.config import BANQUE_REAL_WRITE_ENABLED, CONTROLES_REAL_WRITE_ENABLED
    print(f"Writers réels : banque={'ACTIVÉ' if BANQUE_REAL_WRITE_ENABLED else 'désactivé'}, "
         f"contrôles={'ACTIVÉ' if CONTROLES_REAL_WRITE_ENABLED else 'désactivé'}")

    uvicorn.run(
        "app.main:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level=LOG_LEVEL,
    )

"""Health & diagnostic (APP-SEC-1).

`/health` est PUBLIC (aucune authentification côté application locale) : sa réponse doit rester
minimale, sans chemin absolu, sans nom de profil Windows, sans variable d'environnement. Le détail
technique complet (utile en développement) vit dans `/health/diagnostic`, désactivé par défaut
(`cfg.DIAGNOSTIC_DETAILS_ENABLED = False`), jamais lié dans le menu, réservé aux requêtes locales, et
n'expose jamais de chemin absolu réel (racines toujours sanitisées).
"""
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.requests import Request as StarletteRequest

import app.config as cfg
from app.config import PROJECT_ROOT, MASTER_RUN_LOG, EXPORTS_POWERBI, REF_SETUP, SNAPSHOTS_DIR
from app.db.connection import get_db
from app.services.path_sanitizer import sanitize_path

router = APIRouter()

APPLICATION_NOM = "Pilotage Conciergerie"

_ADRESSES_LOCALES = {"127.0.0.1", "::1", "testclient"}


def _client_local(request: StarletteRequest) -> bool:
    client = request.client
    return bool(client) and client.host in _ADRESSES_LOCALES


def _writers_enabled() -> bool:
    return bool(
        getattr(cfg, "BANQUE_REAL_WRITE_ENABLED", False)
        or getattr(cfg, "CONTROLES_REAL_WRITE_ENABLED", False)
        or getattr(cfg, "HH_REAL_WRITE_ENABLED", False)
        or getattr(cfg, "CHARGES_REAL_WRITE_ENABLED", False)
        or getattr(cfg, "REF_ASSOC_MODE_REAL_WRITE_ENABLED", False)
    )


def _sqlite_ok() -> bool:
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return True
    except Exception:
        return False


def _sources_ok() -> bool:
    return PROJECT_ROOT.exists() and MASTER_RUN_LOG.exists() and REF_SETUP.exists()


@router.get("/health")
def health_check():
    """Réponse publique minimale — aucun chemin, aucune information technique."""
    db_ok = _sqlite_ok()
    src_ok = _sources_ok()
    all_ok = db_ok and src_ok
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "degraded",
            "application": APPLICATION_NOM,
            "database": "ok" if db_ok else "error",
            "sources": "ok" if src_ok else "partial",
            "writers_enabled": _writers_enabled(),
        },
        status_code=200 if all_ok else 503,
        headers={"Cache-Control": "no-store"},
    )


@router.get("/health/diagnostic")
def health_diagnostic(request: Request):
    """Diagnostic détaillé — désactivé par défaut, local uniquement, chemins toujours sanitisés."""
    if not getattr(cfg, "DIAGNOSTIC_DETAILS_ENABLED", False):
        return JSONResponse(content={"detail": "Non trouvé."}, status_code=404)
    if not _client_local(request):
        return JSONResponse(content={"detail": "Accès refusé."}, status_code=403)

    from app.services.file_registry import is_writable

    def etat_fichier(nom_logique: str, chemin) -> dict:
        p = chemin
        return {
            "source": nom_logique,
            "present": p.exists(),
            "lecture_possible": p.exists() and p.is_file(),
            "ecriture_autorisee": is_writable(p) if p.exists() else False,
        }

    checks = {
        "project_root": {"present": PROJECT_ROOT.exists(), "chemin_logique": sanitize_path(PROJECT_ROOT)},
        "master_run_log": etat_fichier("MASTER_RUN_Log", MASTER_RUN_LOG),
        "exports_powerbi": {"present": EXPORTS_POWERBI.exists(), "chemin_logique": sanitize_path(EXPORTS_POWERBI)},
        "ref_setup": etat_fichier("REF_Setup", REF_SETUP),
        "snapshots_dir": {"present": SNAPSHOTS_DIR.exists(), "chemin_logique": sanitize_path(SNAPSHOTS_DIR)},
        "sqlite": {"ok": _sqlite_ok(), "chemin_logique": sanitize_path(cfg.DB_PATH)},
        "write_guard_ref_setup": {"writable": is_writable(REF_SETUP), "expected": False},
        "writers_enabled": _writers_enabled(),
    }
    return JSONResponse(content={"status": "diagnostic", "checks": checks}, status_code=200,
                        headers={"Cache-Control": "no-store"})

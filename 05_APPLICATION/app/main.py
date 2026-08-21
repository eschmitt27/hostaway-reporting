import os
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import STATIC_DIR, DATA_DIR, SNAPSHOTS_DIR
from app.db.connection import apply_migrations
from app.services.logging_config import log_erreur
from app.routes import home, actualisation, administration_referentiels, sources_calculs, health, logements, reservations, menages, fournisseurs, proprietaires, proprietaires_tresorerie, banques, proprietaires_reglements, controles_cloture, clotures, pilotage_mensuel, fournisseurs_referentiel, charges_controle, factures, factures_proprietaires, creances_dettes, calculs, comptabilite, resultats, referentiel_setup, comptes_proprietaires


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    apply_migrations()
    yield


# Application locale mono-utilisateur, sans exposition Internet : documentation API désactivée par
# défaut (DOCS_ENABLED=false) — activation explicite requise, jamais liée dans le menu métier.
_DOCS_ENABLED = os.environ.get("DOCS_ENABLED", "false").lower() == "true"

app = FastAPI(
    title="Chouette Patrimoine — Pilotage Conciergerie",
    lifespan=lifespan,
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
)


@app.exception_handler(Exception)
async def gestionnaire_erreurs(request: Request, exc: Exception):
    """Réservé aux exceptions NON gérées. Les HTTPException (404/403/422...) restent gérées par
    FastAPI/Starlette et conservent leur code — jamais transformées en 500. Aucune stack trace, aucun
    chemin, aucune requête SQL côté client : message générique + référence courte de corrélation ; le
    détail sanitisé reste dans le journal serveur local (jamais l'objet Request complet)."""
    if isinstance(exc, (HTTPException, StarletteHTTPException, RequestValidationError)):
        raise exc
    ref = "ERR-" + secrets.token_hex(4).upper()
    log_erreur(ref, exc, methode=request.method, route=request.url.path)
    return JSONResponse(
        content={"detail": f"Une erreur technique est survenue. Référence : {ref}."},
        status_code=500,
    )


@app.middleware("http")
async def headers_securite(request: Request, call_next):
    """Protection raisonnable (pas d'authentification) : clickjacking, MIME sniffing, referrer,
    et no-store sur les pages contenant des données métier — jamais de secret dans les headers."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    path = request.url.path
    if path.startswith(("/banques-caisse", "/controles-cloture", "/clotures", "/proprietaires",
                        "/reservations", "/logements", "/menages", "/fournisseurs", "/health",
                        "/pilotage-mensuel", "/referentiel-fournisseurs")):
        response.headers["Cache-Control"] = "no-store"
    return response


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(home.router)
app.include_router(logements.router)
app.include_router(reservations.router)
app.include_router(menages.router)
app.include_router(fournisseurs.router)
app.include_router(proprietaires_tresorerie.router)
app.include_router(proprietaires.router)
app.include_router(banques.router)
app.include_router(proprietaires_reglements.router)
app.include_router(controles_cloture.router)
app.include_router(clotures.router)
app.include_router(pilotage_mensuel.router)
app.include_router(fournisseurs_referentiel.router)
app.include_router(factures.router)
app.include_router(factures_proprietaires.router)
app.include_router(creances_dettes.router)
app.include_router(calculs.router)
app.include_router(comptabilite.router)
app.include_router(resultats.router)
app.include_router(charges_controle.router)
app.include_router(sources_calculs.router)
app.include_router(actualisation.router)
app.include_router(referentiel_setup.router)
app.include_router(administration_referentiels.router)
app.include_router(comptes_proprietaires.router)
app.include_router(health.router)

# ── Bandeau MODE RECETTE : exposé à tous les templates (globals Jinja centralisés) ──
# APP-SEC : jamais de chemin absolu ni de nom d'utilisateur dans une page rendue — seul le nom
# logique de l'environnement (basename) est affiché, jamais le chemin complet (trouvé exposé en
# clair sur toute page RECETTE_MODE lors d'une recette globale, 2026-08-02 — corrigé ici).
import app.config as _cfg


def _nom_logique(chemin) -> str:
    """Nom logique d'un chemin (dernier segment) — jamais le chemin absolu complet."""
    from pathlib import Path
    p = Path(chemin)
    return p.name or str(p)


_recette_globals = {
    "RECETTE_MODE": _cfg.RECETTE_MODE,
    "RECETTE_ROOT": _nom_logique(_cfg.RECETTE_ROOT),
    "RECETTE_DB": f"{_nom_logique(_cfg.DB_PATH.parent)}/{_nom_logique(_cfg.DB_PATH)}",
    "RECETTE_CHARGES_WRITE": bool(_cfg.CHARGES_REAL_WRITE_ENABLED),
}
for _mod in (home, logements, reservations, menages, fournisseurs, proprietaires,
             proprietaires_tresorerie, banques,
             proprietaires_reglements, controles_cloture, clotures, pilotage_mensuel,
             fournisseurs_referentiel, sources_calculs, actualisation, charges_controle, factures, factures_proprietaires, creances_dettes, calculs, referentiel_setup, comptes_proprietaires):
    _t = getattr(_mod, "templates", None)
    if _t is not None:
        _t.env.globals.update(_recette_globals)

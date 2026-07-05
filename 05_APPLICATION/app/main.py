from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.config import STATIC_DIR, DATA_DIR, SNAPSHOTS_DIR
from app.db.connection import apply_migrations
from app.routes import home, sources_calculs, health, logements, reservations, menages


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    apply_migrations()
    yield


app = FastAPI(title="Chouette Patrimoine — Pilotage Conciergerie", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(home.router)
app.include_router(logements.router)
app.include_router(reservations.router)
app.include_router(menages.router)
app.include_router(sources_calculs.router)
app.include_router(health.router)

# Chouette Patrimoine — Application locale de pilotage

Application FastAPI locale mono-utilisateur pour le pilotage de la conciergerie.
Lot APP-0 — Socle technique.

## Lancement rapide

```
cd 05_APPLICATION
pip install -r requirements.txt
python run_app.py
```

http://localhost:8000

## Documentation

- `docs/LANCEMENT_LOCAL.md` — installation et configuration
- `docs/CONTRAT_FICHIERS.md` — règles d'accès fichiers
- `docs/CARTE_FLUX_DONNEES.md` — flux de données

## Règle fondamentale

L'application orchestre, affiche et trace. Elle ne recalcule jamais les règles métier.
Source de vérité : moteur Python `02_TRAVAIL/` + Excel.

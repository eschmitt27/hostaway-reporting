# LANCEMENT_LOCAL.md — Chouette Patrimoine

## Prérequis

- Python 3.12+
- `pip install -r requirements.txt` depuis `05_APPLICATION/`

## Lancement

```
cd 05_APPLICATION
python run_app.py
```

Accès : http://localhost:8000

## Configuration (.env)

Copier `.env.example` en `.env`. Les chemins sont détectés automatiquement.
Ne modifier `PROJECT_ROOT` que si l'arborescence diffère du standard.

## Tests

```
cd 05_APPLICATION
pytest tests/ -v
```

## Vérification santé

http://localhost:8000/health

## Fichiers jamais modifiés par l'application

Voir `docs/CONTRAT_FICHIERS.md`.

## JavaScript

APP-0 fonctionne en HTML classique avec rechargement complet de page.
Aucune dépendance JS externe requise au Lot APP-0.
HTMX sera intégré dans un lot ultérieur, uniquement à partir d'un fichier local exact, vérifié et validé.

## Arrêt

Ctrl+C dans le terminal.

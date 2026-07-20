"""STUB de lib_sheet_source — recette ménages sur COPIES (aucun réseau).

Remplace le module réseau `lib_sheet_source` DANS LE WORKSPACE uniquement. Le vrai
module du dépôt n'est jamais modifié : ce stub est copié par le service de chaîne à la
place du fichier réel dans l'arbre miroir.

`fetch_sheet_csv` ne contacte JAMAIS le réseau : il rend les octets d'une source
déclarations COPIÉE (`<projet>/02_DONNEES_NORMALISEES/menages/source_sheet_copiee.csv`),
avec une provenance explicite `resolution_source = "COPIE_RECETTE"`. Jamais d'invention,
jamais de reprise silencieuse d'un cache réseau.
"""
import io
import csv
import os
import datetime


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _source_copiee_path(cache_dir):
    # cache_dir = <...>/02_DONNEES_NORMALISEES/menages/_cache_google_sheet
    menages_dir = os.path.dirname(cache_dir.rstrip("/\\"))
    return os.path.join(menages_dir, "source_sheet_copiee.csv")


def begin_step(cache_dir, step):
    """No-op tracé : en recette copies, il n'y a pas de transaction réseau à marquer."""
    os.makedirs(cache_dir, exist_ok=True)


def commit_step(cache_dir, step, prov):
    """No-op : la provenance de recette est portée par le dict `prov` retourné plus haut."""
    return None


def fetch_sheet_csv(url, cache_dir, step, max_age_h=72, retries=3, waits=(5, 10), timeout=20):
    src = _source_copiee_path(cache_dir)
    if not os.path.exists(src):
        raise SystemExit(f"SOURCE_SHEET_COPIEE_ABSENTE (recette copies, step={step}) : {src}")
    txt = open(src, encoding="utf-8").read()
    rows = list(csv.reader(io.StringIO(txt)))
    if len(rows) < 2 or len(rows[0]) < 3:
        raise SystemExit(f"SOURCE_SHEET_COPIEE_INVALIDE (step={step})")
    prov = {"resolved_at_utc": _now(), "resolution_source": "COPIE_RECETTE",
            "source_url": url, "sha256_csv": None,
            "cache_date_extraction_utc": None, "cache_age_h": 0.0, "script_or_step": step}
    return txt, prov

"""
lib_sheet_source.py — Récupération fiabilisée de la Google Sheet ménages (DEF-1)
================================================================================
Source normale = Google Sheet publiée (CSV). En cas de panne réseau : retry
contrôlé puis bascule sur un cache local TRAÇABLE, jamais silencieuse.

Règles (validées) :
  - 3 tentatives, timeout 20s/essai, attentes 5s puis 10s (rien après le dernier).
  - téléchargement validé AVANT d'écraser le cache ; échec/incomplet -> cache préservé.
  - écriture ATOMIQUE (tmp + os.replace) du csv, du meta et du manifeste.
  - bascule cache uniquement si âge <= max_age_h (défaut 72h). Sinon -> erreur.
  - aucun cache fiable -> SystemExit("SOURCE_SHEET_INDISPONIBLE") (rc != 0).
  - écrit last_resolution.json = provenance RÉELLE de l'exécution (RESEAU|CACHE),
    base du contrôle SOURCE_SHEET_CACHE_UTILISE en lot11.
  - JAMAIS d'invention : on rend les octets exacts (réseau ou cache), ou on bloque.

Fichiers (dans cache_dir, ignoré Git) :
  suivi_menage.csv         octets bruts dernière extraction réseau réussie
  suivi_menage.meta.json   date_extraction_utc, source_url, sha256_csv, nb_lignes,
                           nb_colonnes, hash_entetes, version_cache
  last_resolution.json     resolved_at_utc, resolution_source, source_url, sha256_csv,
                           cache_date_extraction_utc, cache_age_h, script_or_step
"""

import os, io, csv, json, time, hashlib, subprocess, datetime

VERSION_CACHE = 1
EXIT_CODE = "SOURCE_SHEET_INDISPONIBLE"

def _now():
    return datetime.datetime.now(datetime.timezone.utc)

def _sha(t):
    return hashlib.sha256(t.encode("utf-8")).hexdigest()

def _valid_csv(txt):
    if not txt or len(txt) < 50:
        return False
    try:
        rows = list(csv.reader(io.StringIO(txt)))
    except Exception:
        return False
    return len(rows) >= 2 and len(rows[0]) >= 3   # entête + >=1 ligne, >=3 colonnes

def _write_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(data)
    os.replace(tmp, path)

def _pending_path(cache_dir, step):
    return os.path.join(cache_dir, f"pending_resolution_{step}.json")

def begin_step(cache_dir, step):
    """Marqueur PENDING écrit AVANT de remplacer la/les sortie(s) métier de l'étape.
    Tant qu'il existe (et qu'une sortie existe), lot11 refuse de considérer l'étape RESEAU."""
    os.makedirs(cache_dir, exist_ok=True)
    _write_atomic(_pending_path(cache_dir, step),
                  json.dumps({"step": step, "started_at_utc": _now().isoformat()}, ensure_ascii=False))

def commit_step(cache_dir, step, prov):
    """À appeler APRÈS écriture réussie de la sortie métier : enregistre la provenance
    officielle (atomique) PUIS supprime le marqueur PENDING. Si l'enregistrement échoue,
    le marqueur reste -> lot11 signale, jamais de faux RESEAU."""
    _update_resolution(os.path.join(cache_dir, "last_resolution.json"), step, prov)
    p = _pending_path(cache_dir, step)
    if os.path.exists(p):
        os.remove(p)

def _update_resolution(res_path, step, entry):
    """Manifeste PAR ÉTAPE : conserve la dernière provenance réelle de chaque étape.
    Pas de reset global / run_id : une sortie encore issue du cache reste visible tant
    que SON étape n'a pas été régénérée avec succès en RESEAU. Écriture atomique."""
    data = {"resolved_at_utc": _now().isoformat(), "steps": {}}
    if os.path.exists(res_path):
        try:
            old = json.load(open(res_path, encoding="utf-8"))
            if isinstance(old.get("steps"), dict):
                data["steps"] = old["steps"]
        except Exception:
            data["steps"] = {}   # manifeste corrompu -> repart vide (lot11 signalera INCOMPLETE)
    data["steps"][step] = entry
    _write_atomic(res_path, json.dumps(data, ensure_ascii=False, indent=2))

def _curl(url, timeout):
    r = subprocess.run(["curl", "-sL", "-A", "Mozilla/5.0", "--max-time", str(timeout), url],
                       capture_output=True, text=True, encoding="utf-8")
    return r.returncode, (r.stdout or "")

def fetch_sheet_csv(url, cache_dir, step, max_age_h=72, retries=3, waits=(5, 10), timeout=20):
    """Retourne (csv_text, prov_dict). Lève SystemExit(EXIT_CODE) si indisponible."""
    os.makedirs(cache_dir, exist_ok=True)
    csv_path = os.path.join(cache_dir, "suivi_menage.csv")
    meta_path = os.path.join(cache_dir, "suivi_menage.meta.json")

    # ── 1. Tentatives réseau ────────────────────────────────────────────────
    txt = None
    for i in range(retries):
        rc, out = _curl(url, timeout)
        if rc == 0 and _valid_csv(out):
            txt = out
            break
        if i < retries - 1:
            time.sleep(waits[i] if i < len(waits) else waits[-1])

    if txt is not None:
        rows = list(csv.reader(io.StringIO(txt)))
        meta = {"date_extraction_utc": _now().isoformat(), "source_url": url,
                "sha256_csv": _sha(txt), "nb_lignes": len(rows) - 1, "nb_colonnes": len(rows[0]),
                "hash_entetes": _sha(";".join(str(c) for c in rows[0])), "version_cache": VERSION_CACHE}
        _write_atomic(csv_path, txt)                                  # validé avant écrasement
        _write_atomic(meta_path, json.dumps(meta, ensure_ascii=False, indent=2))
        prov = {"resolved_at_utc": _now().isoformat(), "resolution_source": "RESEAU",
                "source_url": url, "sha256_csv": meta["sha256_csv"],
                "cache_date_extraction_utc": meta["date_extraction_utc"], "cache_age_h": 0.0,
                "script_or_step": step}
        # Provenance officielle ecrite par l'appelant via commit_step() APRES ecriture sortie metier.
        return txt, prov

    # ── 2. Réseau KO -> cache (si présent ET <= max_age_h) ──────────────────
    if os.path.exists(csv_path) and os.path.exists(meta_path):
        try:
            meta = json.load(open(meta_path, encoding="utf-8"))
            de = datetime.datetime.fromisoformat(meta["date_extraction_utc"])
            if de.tzinfo is None:
                de = de.replace(tzinfo=datetime.timezone.utc)
            age_h = (_now() - de).total_seconds() / 3600.0
        except Exception:
            raise SystemExit(f"{EXIT_CODE} (reseau KO, meta cache illisible)")
        if age_h <= max_age_h:
            cached = open(csv_path, encoding="utf-8").read()
            if not _valid_csv(cached):
                raise SystemExit(f"{EXIT_CODE} (reseau KO, cache invalide)")
            prov = {"resolved_at_utc": _now().isoformat(), "resolution_source": "CACHE",
                    "source_url": url, "sha256_csv": meta["sha256_csv"],
                    "cache_date_extraction_utc": meta["date_extraction_utc"],
                    "cache_age_h": round(age_h, 2), "script_or_step": step}
            # Provenance officielle ecrite par l'appelant via commit_step() APRES ecriture sortie metier.
            return cached, prov
        raise SystemExit(f"{EXIT_CODE} (reseau KO, cache age {age_h:.1f}h > {max_age_h}h)")

    raise SystemExit(f"{EXIT_CODE} (reseau KO, aucun cache disponible)")

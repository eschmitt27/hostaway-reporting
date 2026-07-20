"""APP-4B — Writer bancaire SUR COPIES. Aucune écriture réelle tant que les flags sont False.

Garde-fous :
- refus total d'écriture réelle (BANQUE_REAL_WRITE_ENABLED / _CONFIRMATION à False) ;
- ne travaille que dans un workspace isolé sous data/ ; jamais un chemin hors workspace ;
- opère sur une COPIE de BANQUE_LOT8_IMPORT.xlsx : onglets moteur préservés, formules préservées,
  colonnes non ciblées préservées ; l'override est écrit dans un onglet DÉDIÉ (OVERRIDE_APP4B) et
  appliqué sur une COPIE de NORM_Banque uniquement ;
- snapshot de la copie, journal SQLite (avant/après), vérification que le fichier RÉEL est intact ;
- refuse un fichier absent / ouvert (~$) ; transactionnel (échec => run ECHEC lisible, pas de 500).
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import hashlib

import app.config as cfg
from app.db.connection import get_db
from app.services import snapshot_service
from app.services.audit_service import log_event

ST_SUCCES = "SUCCES"
ST_ECHEC = "ECHEC"
ST_BLOQUE = "BLOQUE"

COLS_APPLIQUEES = ("categorie", "type_flux_id", "statut_controle")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fichier_office_verrou(path: Path) -> Path:
    return path.parent / ("~$" + path.name)


def _overrides_actifs(db_path) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM banque_overrides WHERE actif=1").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _enregistrer_run(db_path, **champs) -> int:
    conn = get_db(db_path)
    try:
        cols = ", ".join(champs)
        ph = ", ".join("?" for _ in champs)
        cur = conn.execute(f"INSERT INTO banque_controle_runs ({cols}) VALUES ({ph})", tuple(champs.values()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def enregistrer_sur_copie(mode: str = "COPIES", db_path=None) -> dict[str, Any]:
    """Applique les décisions actives sur une COPIE de la banque. Réel refusé tant que flags False."""
    db_path = db_path or cfg.DB_PATH

    # ── Garde mode réel ──────────────────────────────────────────────────────
    if mode == "REEL" and not (cfg.BANQUE_REAL_WRITE_ENABLED and cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED):
        run_id = _enregistrer_run(db_path, mode="REEL", statut=ST_BLOQUE,
                                  erreur_code="E_REEL_DESACTIVE",
                                  erreur_resume="BANQUE_REAL_WRITE_ENABLED=False — aucune écriture réelle.")
        log_event("BANQUE_WRITE_BLOQUE", {"mode": "REEL"}, db_path=db_path)
        return {"ok": False, "run_id": run_id, "statut": ST_BLOQUE, "erreur_code": "E_REEL_DESACTIVE",
                "message": "Écriture bancaire réelle désactivée. Aucun fichier réel touché."}

    reel = Path(cfg.MASTER_BANQUE)
    if not reel.exists():
        run_id = _enregistrer_run(db_path, mode="COPIES", statut=ST_ECHEC, erreur_code="E_FICHIER_ABSENT",
                                  erreur_resume=str(reel.name))
        return {"ok": False, "run_id": run_id, "statut": ST_ECHEC, "erreur_code": "E_FICHIER_ABSENT",
                "message": f"Fichier bancaire absent : {reel.name}."}
    if _fichier_office_verrou(reel).exists():
        run_id = _enregistrer_run(db_path, mode="COPIES", statut=ST_ECHEC, erreur_code="E_EXCEL_OUVERT",
                                  erreur_resume=reel.name)
        return {"ok": False, "run_id": run_id, "statut": ST_ECHEC, "erreur_code": "E_EXCEL_OUVERT",
                "message": f"Fichier probablement ouvert dans Excel : {reel.name}. Fermez-le."}

    overrides = _overrides_actifs(db_path)
    sha_reel_avant = _sha256(reel)
    try:
        # ── Snapshot du réel (traçabilité, lecture seule) ────────────────────
        snap = snapshot_service.create_snapshot("BANQUE_APP4B_AVANT", [reel], db_path=db_path)

        # ── Workspace isolé + copie ──────────────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        workspace = Path(cfg.BANQUE_CONTROLE_WORKSPACE) / ts
        if workspace.exists():
            shutil.rmtree(workspace)
        workspace.mkdir(parents=True, exist_ok=True)
        copie = workspace / reel.name
        shutil.copy2(reel, copie)

        # ── Écriture onglet override + application sur NORM (COPIE) ───────────
        ctrl_avant, ctrl_apres = _appliquer(copie, overrides)
        sha_copie = _sha256(copie)

        # ── Garde : le RÉEL n'a pas bougé ────────────────────────────────────
        sha_reel_apres = _sha256(reel)
        reel_intact = sha_reel_apres == sha_reel_avant
        statut = ST_SUCCES if reel_intact else ST_ECHEC
        erreur_code = None if reel_intact else "E_REEL_MODIFIE"

        run_id = _enregistrer_run(
            db_path, mode="COPIES", statut=statut, workspace_path=str(workspace),
            snapshot_id=snap["id"], nb_overrides=len(overrides), reel_intact=1 if reel_intact else 0,
            sha256_avant=sha_reel_avant, sha256_apres=sha_copie,
            controles_avant_json=json.dumps(ctrl_avant, ensure_ascii=False),
            controles_apres_json=json.dumps(ctrl_apres, ensure_ascii=False),
            erreur_code=erreur_code,
        )
        # relie les overrides à ce run
        conn = get_db(db_path)
        try:
            conn.execute("UPDATE banque_overrides SET run_id=? WHERE actif=1 AND run_id IS NULL", (run_id,))
            conn.commit()
        finally:
            conn.close()
        log_event("BANQUE_WRITE_COPIE", {"run_id": run_id, "nb": len(overrides),
                                         "reel_intact": reel_intact}, db_path=db_path)
        return {"ok": statut == ST_SUCCES, "run_id": run_id, "statut": statut,
                "workspace": str(workspace), "copie": str(copie), "nb_overrides": len(overrides),
                "reel_intact": reel_intact, "controles_avant": ctrl_avant, "controles_apres": ctrl_apres}
    except Exception as exc:
        run_id = _enregistrer_run(db_path, mode="COPIES", statut=ST_ECHEC, erreur_code="E_INATTENDU",
                                  erreur_resume=f"{type(exc).__name__}: {exc}")
        return {"ok": False, "run_id": run_id, "statut": ST_ECHEC, "erreur_code": "E_INATTENDU",
                "message": f"Erreur inattendue ({type(exc).__name__}). Aucun fichier réel touché."}


def _appliquer(copie: Path, overrides: list[dict[str, Any]]) -> tuple[dict, dict]:
    """Écrit l'onglet OVERRIDE_APP4B et applique les décisions sur NORM_Banque de la COPIE.

    Retourne (comptage_statuts_avant, comptage_statuts_apres) de NORM_Banque (pour l'impact contrôle).
    Onglets moteur et colonnes non ciblées préservés.
    """
    import openpyxl
    wb = openpyxl.load_workbook(copie)   # garde formules ; pas read_only
    ws = wb["NORM_Banque"]
    hdr = [c.value for c in ws[1]]
    idx = {h: i for i, h in enumerate(hdr)}
    i_mid = idx.get("mouvement_id")
    i_stat = idx.get("statut_controle")

    def compte_statuts():
        c: dict[str, int] = {}
        for row in ws.iter_rows(min_row=2):
            s = str(row[i_stat].value) if i_stat is not None else ""
            c[s] = c.get(s, 0) + 1
        return c

    ctrl_avant = compte_statuts()

    par_mid = {o["mouvement_id_interne"]: o for o in overrides}
    for row in ws.iter_rows(min_row=2):
        mid = str(row[i_mid].value) if i_mid is not None else ""
        ov = par_mid.get(mid)
        if not ov:
            continue
        # applique uniquement les colonnes ciblées, si une valeur est décidée
        if ov.get("categorie_validee") and "categorie" in idx:
            row[idx["categorie"]].value = ov["categorie_validee"]
        if ov.get("type_flux_id") and "type_flux_id" in idx:
            row[idx["type_flux_id"]].value = ov["type_flux_id"]
        if ov.get("statut_controle") and i_stat is not None:
            row[i_stat].value = ov["statut_controle"]

    # Onglet override dédié (jamais dans un onglet moteur)
    nom_ov = cfg.BANQUE_OVERRIDE_SHEET
    if nom_ov in wb.sheetnames:
        del wb[nom_ov]
    wso = wb.create_sheet(nom_ov)
    cols = ["mouvement_id_opaque", "mouvement_id_interne", "categorie_validee", "type_flux_id",
            "proprietaire_id", "logement_id", "reservation_id", "facture_id", "statut_controle",
            "commentaire", "justification", "proposition_categorie", "auteur", "date_action",
            "source_action", "version"]
    wso.append(cols)
    for o in overrides:
        wso.append([o.get(c) for c in cols])

    ctrl_apres = compte_statuts()
    wb.save(copie)
    wb.close()
    return ctrl_avant, ctrl_apres


def load_run(run_id: int, db_path=None) -> dict[str, Any] | None:
    db_path = db_path or cfg.DB_PATH
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM banque_controle_runs WHERE id=?", (run_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    d = dict(row)
    for champ in ("controles_avant_json", "controles_apres_json"):
        if d.get(champ):
            try:
                d[champ.replace("_json", "")] = json.loads(d[champ])
            except (json.JSONDecodeError, TypeError):
                pass
    return d

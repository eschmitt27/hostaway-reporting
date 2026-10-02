"""Cutover V1 (2026-09-01) — commande d'exploitation, une étape à la fois.

    python tools/cutover_v1.py etat
    python tools/cutover_v1.py sauvegarder
    python tools/cutover_v1.py simuler
    python tools/cutover_v1.py executer --confirmer --acteur <nom> --dossier <dossier de sauvegarde>
    python tools/cutover_v1.py verifier
    python tools/cutover_v1.py reconstruire

FINITION (2026-10-03, D-V1-FIN-1 / D-V1-FIN-2) — purge de tout reliquat antérieur à la V1 :
    python tools/cutover_v1.py finition-sauvegarder
    python tools/cutover_v1.py finition-simuler --dossier <dossier de finition>
    python tools/cutover_v1.py finition-executer --confirmer --acteur <nom> --dossier <dossier>
    python tools/cutover_v1.py finition-reconstruire --dossier <dossier>
    python tools/cutover_v1.py finition-verifier --dossier <dossier>

La base visée est celle de l'application (`APP_DATA_DIR` du `.env`). L'application doit être
ARRÊTÉE pour `sauvegarder` et `executer` (le port est vérifié) : la sauvegarde doit décrire
exactement la base que le cutover modifie. Chaque étape écrit son rapport JSON dans le dossier de
sauvegarde du cutover, `<APP_DATA_DIR>/backups/cutover_v1_<horodatage>/`.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

RACINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RACINE))
os.chdir(RACINE)

import app.config as cfg  # noqa: E402  (charge le .env : la vraie base de l'instance)
from app.db.connection import apply_migrations  # noqa: E402
from app.services import cutover_v1_service as cut  # noqa: E402
from app.services import cutover_v1_finition_service as fin  # noqa: E402


def _sha256(chemin: Path) -> str:
    h = hashlib.sha256()
    with open(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def _application_active() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", int(cfg.PORT))) == 0


def etat(db: Path) -> dict:
    """Empreinte et contrôles de la base, SANS l'ouvrir en écriture (checkpoint compris : la
    commande refuse si l'application tourne, le fichier WAL est donc stable)."""
    conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        total = sum(conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables)
        integrite = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        volumetrie = cut._volumetrie_cle(conn)
        volumetrie.update({
            "rapprochements_banque_charges": conn.execute(
                "SELECT COUNT(*) FROM banque_rapprochements WHERE type_objet IN "
                "('CHARGE_FOURNISSEUR','CHARGE') AND statut='CONFIRME'").fetchone()[0],
            "creances (factures émises)": volumetrie["factures_emises"],
        })
    finally:
        conn.close()
    wal = db.with_name(db.name + "-wal")
    stat = db.stat()
    return {"base": str(db), "sha256": _sha256(db), "taille": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "wal_taille": wal.stat().st_size if wal.exists() else 0,
            "schema": version, "integrity_check": integrite,
            "foreign_key_check": len(fk), "nb_tables": len(tables), "nb_lignes": total,
            "volumetrie": volumetrie}


def empreinte_logique(db: Path) -> dict:
    """Empreinte du CONTENU (toutes tables, lignes triées) et du schéma — indépendante de la mise en
    page des pages SQLite : deux bases au contenu identique ont la même, quel que soit le fichier."""
    conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        tables = cut._tables(conn)
        contenu = hashlib.sha256()
        for t in tables:
            contenu.update(f"{t}:{cut.empreinte(conn, t)}".encode("utf-8"))
        schema = hashlib.sha256("\n".join(r[0] or "" for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name")
        ).encode("utf-8")).hexdigest()[:16]
    finally:
        conn.close()
    return {"contenu": contenu.hexdigest()[:24], "schema": schema, "nb_tables": len(tables)}


def _ecrire(dossier: Path, nom: str, contenu: dict) -> Path:
    dossier.mkdir(parents=True, exist_ok=True)
    chemin = dossier / nom
    chemin.write_text(json.dumps(contenu, ensure_ascii=False, indent=1, default=str),
                      encoding="utf-8")
    return chemin


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("etape", choices=("etat", "sauvegarder", "simuler", "executer", "verifier",
                                     "reconstruire", "finition-sauvegarder", "finition-simuler",
                                     "finition-executer", "finition-reconstruire",
                                     "finition-verifier"))
    p.add_argument("--dossier", help="dossier de sauvegarde du cutover (créé par « sauvegarder »)")
    p.add_argument("--confirmer", action="store_true")
    p.add_argument("--acteur", default="")
    args = p.parse_args()

    db = Path(cfg.DB_PATH)
    print(f"Base : {db}")
    dossier = Path(args.dossier) if args.dossier else None

    if args.etape == "etat":
        print(json.dumps(etat(db), ensure_ascii=False, indent=1))
        return 0

    if args.etape == "sauvegarder":
        if _application_active():
            print("REFUS : l'application tourne (port %s). L'arrêter d'abord." % cfg.PORT)
            return 2
        horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dossier = Path(cfg.BACKUPS_DIR) / f"cutover_v1_{horodatage}"
        dossier.mkdir(parents=True)
        avant = etat(db)
        cible = Path(cfg.BACKUPS_DIR) / f"app_avant_cutover_v1_{horodatage}.db"
        src = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
        dst = sqlite3.connect(str(cible))
        src.backup(dst)
        dst.close()
        src.close()
        verif = etat(cible)
        lisible = (verif["integrity_check"] == "ok" and verif["nb_lignes"] == avant["nb_lignes"]
                   and verif["nb_tables"] == avant["nb_tables"])
        pdf = Path(cfg.DATA_DIR) / "factures_proprietaires"
        if pdf.exists():
            shutil.copytree(pdf, dossier / "factures_proprietaires_avant")
        rapport = {"etat_avant": avant, "sauvegarde": str(cible),
                   "sauvegarde_sha256": verif["sha256"], "sauvegarde_lisible": lisible,
                   "sauvegarde_controle": verif}
        _ecrire(dossier, "01_etat_et_sauvegarde.json", rapport)
        print(json.dumps(rapport, ensure_ascii=False, indent=1))
        print(f"\nDOSSIER DU CUTOVER : {dossier}")
        return 0 if lisible else 3

    if args.etape == "simuler":
        r1, r2 = cut.simuler(), cut.simuler()
        identiques = r1["empreinte_rapport"] == r2["empreinte_rapport"]
        if dossier:
            _ecrire(dossier, "02_simulation_1.json", r1)
            _ecrire(dossier, "02_simulation_2.json", r2)
        print("CUTOVER V1 — SIMULATION")
        for k in ("tables_analysees", "lignes", "factures_supprimees", "creances_supprimees",
                  "charges_supprimees", "charges_conservees",
                  "rapprochements_banque_charges_conserves", "rapprochements_autres_conserves",
                  "mouvements_bancaires_conserves", "reservations_conservees",
                  "referentiels_conserves", "anomalies"):
            print(f"  {k} : {r1[k]}")
        print(f"  deux simulations identiques : {identiques} ({r1['empreinte_rapport']})")
        return 0 if identiques and not r1["anomalies"] else 4

    if args.etape == "executer":
        if not dossier or not (dossier / "01_etat_et_sauvegarde.json").exists():
            print("REFUS : « sauvegarder » d'abord, puis passer --dossier <dossier du cutover>.")
            return 2
        sauvegarde = json.loads((dossier / "01_etat_et_sauvegarde.json").read_text("utf-8"))
        if not sauvegarde.get("sauvegarde_lisible"):
            print("REFUS : la sauvegarde n'est pas lisible.")
            return 3
        if _application_active():
            print("REFUS : l'application tourne (port %s). L'arrêter d'abord." % cfg.PORT)
            return 2
        apply_migrations(db)          # le garde-fou 0120 doit exister avant de poser la date
        r = cut.executer(confirmer=args.confirmer, acteur=args.acteur,
                         archive_dir=dossier / "documents_archives")
        r["etat_apres"] = etat(db) if r.get("ok") else None
        _ecrire(dossier, "03_execution.json", r)
        print(json.dumps({k: r.get(k) for k in ("ok", "code", "message", "journal_suppressions",
                                                 "documents_archives", "anomalies", "echecs")},
                         ensure_ascii=False, indent=1, default=str))
        for v in r.get("verifications") or []:
            print(f"  {v['code']:>5} {'OK' if v['ok'] else 'KO'}  {v['libelle']}")
        return 0 if r.get("ok") else 5

    if args.etape == "verifier":
        v = cut.verifier()
        v["etat"] = etat(db)
        if dossier:
            _ecrire(dossier, f"04_verification_{datetime.now(timezone.utc):%H%M%S}.json", v)
        print(json.dumps(v, ensure_ascii=False, indent=1, default=str))
        return 0 if v["checks"]["ok"] else 6

    if args.etape == "reconstruire":
        from app.services import orchestrateur_service as orch
        r = orch.actualiser(cibles=None, inclure_imports_externes=False, declencheur="CUTOVER_V1")
        resume = {"statut": r["statut"], "run_id": r["run_id"],
                  "etapes": [(e["dataset"], e["statut"]) for e in r["etapes"]]}
        if dossier:
            _ecrire(dossier, "05_reconstruction.json", resume)
        print(json.dumps(resume, ensure_ascii=False, indent=1))
        return 0 if r["statut"] in ("SUCCES", "PARTIEL") else 7

    # ── FINITION ────────────────────────────────────────────────────────────────────────────────
    if args.etape == "finition-sauvegarder":
        if _application_active():
            print("REFUS : l'application tourne (port %s). L'arrêter d'abord." % cfg.PORT)
            return 2
        horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dossier = Path(cfg.BACKUPS_DIR) / f"finition_cutover_v1_{horodatage}"
        dossier.mkdir(parents=True)
        avant = etat(db)
        cible = Path(cfg.BACKUPS_DIR) / f"app_avant_finition_cutover_v1_{horodatage}.db"
        src = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
        dst = sqlite3.connect(str(cible))
        src.backup(dst)
        # Sauvegarde AUTONOME : un seul fichier, sans -wal/-shm à côté (le mode WAL de la source
        # serait sinon recopié, et un simple contrôle en lecture laisserait des fichiers annexes).
        dst.execute("PRAGMA journal_mode=DELETE")
        dst.close()
        src.close()
        verif = etat(cible)
        lisible = (verif["integrity_check"] == "ok" and verif["foreign_key_check"] == 0
                   and verif["nb_lignes"] == avant["nb_lignes"]
                   and verif["nb_tables"] == avant["nb_tables"])
        # Restauration dans un emplacement ISOLÉ : la sauvegarde redonne-t-elle la même base ?
        isole = dossier / "restauration_isolee"
        isole.mkdir()
        restauree = isole / "app.db"
        src = sqlite3.connect(f"file:{cible.as_posix()}?mode=ro", uri=True)
        dst = sqlite3.connect(str(restauree))
        src.backup(dst)
        dst.execute("PRAGMA journal_mode=DELETE")
        dst.close()
        src.close()
        e_source, e_restauree = empreinte_logique(db), empreinte_logique(restauree)
        controle_restauree = etat(restauree)
        restauration = {"emplacement": str(restauree), "empreinte_source": e_source,
                        "empreinte_restauree": e_restauree,
                        "integrity_check": controle_restauree["integrity_check"],
                        "foreign_key_check": controle_restauree["foreign_key_check"],
                        "nb_tables": controle_restauree["nb_tables"],
                        "nb_lignes": controle_restauree["nb_lignes"],
                        "identique": e_source == e_restauree}
        shutil.rmtree(isole)        # la copie de contrôle ne reste pas : la preuve est dans le rapport
        rapport = {"etat_avant": avant, "sauvegarde": str(cible),
                   "sauvegarde_sha256": verif["sha256"], "sauvegarde_lisible": lisible,
                   "sauvegarde_controle": verif, "restauration_isolee": restauration}
        _ecrire(dossier, "01_etat_et_sauvegarde.json", rapport)
        print(json.dumps(rapport, ensure_ascii=False, indent=1))
        print(f"\nDOSSIER DE FINITION : {dossier}")
        return 0 if lisible and restauration["identique"] else 3

    if args.etape == "finition-simuler":
        r1, r2 = fin.simuler(), fin.simuler()
        identiques = r1["empreinte_rapport"] == r2["empreinte_rapport"]
        if dossier:
            _ecrire(dossier, "02_simulation_1.json", r1)
            _ecrire(dossier, "02_simulation_2.json", r2)
        print("CUTOVER V1 — FINITION — SIMULATION")
        for t, c in {**r1["comptabilite_proprietaire"], **r1["factures_fournisseurs_tables"]}.items():
            extra = {k: v for k, v in c.items() if k not in ("avant", "a_supprimer", "apres_attendu",
                                                              "par_run")}
            print(f"  {t:<38} avant {c['avant']:>6}  à supprimer {c['a_supprimer']:>6}  "
                  f"après {c['apres_attendu']:>6}  {extra or ''}")
        print(f"  factures fournisseurs : {r1['factures_fournisseurs_resume']}")
        print(f"  dettes actives avant : {r1['dettes_actives_avant']} → après attendu : 0")
        print(f"  verdicts ANTERIEURE_V1 à poser : {r1['verdicts_anterieure_v1_a_poser']}")
        rc = r1["rapprochements_charges"]
        print(f"  rapprochements → charges : {rc['nb_rapprochements']} → {rc['nb_charges']} ; "
              f"{rc['total_charges']} € ↔ {rc['total_rapproche']} €")
        print(f"  impact : {r1['impact']}")
        print(f"  lignes à supprimer : {r1['lignes_a_supprimer']}")
        print(f"  anomalies : {r1['anomalies']}")
        print(f"  deux simulations identiques : {identiques} ({r1['empreinte_rapport']})")
        return 0 if identiques and not r1["anomalies"] else 4

    if args.etape == "finition-executer":
        if not dossier or not (dossier / "01_etat_et_sauvegarde.json").exists():
            print("REFUS : « finition-sauvegarder » d'abord, puis --dossier <dossier de finition>.")
            return 2
        sauvegarde = json.loads((dossier / "01_etat_et_sauvegarde.json").read_text("utf-8"))
        if not (sauvegarde.get("sauvegarde_lisible")
                and (sauvegarde.get("restauration_isolee") or {}).get("identique")):
            print("REFUS : la sauvegarde n'est pas lisible ou sa restauration n'est pas prouvée.")
            return 3
        if _application_active():
            print("REFUS : l'application tourne (port %s). L'arrêter d'abord." % cfg.PORT)
            return 2
        apply_migrations(db)          # 0121 : le verrou base des factures fournisseurs pré-V1
        r = fin.executer(confirmer=args.confirmer, acteur=args.acteur)
        r["etat_apres"] = etat(db) if r.get("ok") else None
        _ecrire(dossier, "03_execution.json", r)
        print(json.dumps({k: r.get(k) for k in ("ok", "code", "message", "journal_suppressions",
                                                 "anomalies", "echecs")},
                         ensure_ascii=False, indent=1, default=str))
        for v in r.get("verifications") or []:
            print(f"  {v['code']:>9} {'OK' if v['ok'] else 'KO'}  {v['libelle']}"
                  + (f"  [{v['detail']}]" if v.get("detail") not in ("", None, [], {}) else ""))
        return 0 if r.get("ok") else 5

    if args.etape == "finition-reconstruire":
        from app.services import orchestrateur_service as orch
        # Seuls les calculs comptables : Lot10 (qui exclut désormais tout mois antérieur à la V1)
        # puis ses descendants Lot11 et Lot12. Aucune source, aucun import, aucun recalcul des
        # réservations ni des ménages de juin à août.
        r = orch.actualiser(cibles=["LOT10"], inclure_imports_externes=False,
                            declencheur="CUTOVER_V1_FINITION")
        resume = {"statut": r.get("statut"), "run_id": r.get("run_id"),
                  "etapes": [(e["dataset"], e["statut"]) for e in r.get("etapes") or []]}
        if dossier:
            _ecrire(dossier, "05_reconstruction.json", resume)
        print(json.dumps(resume, ensure_ascii=False, indent=1))
        return 0 if r.get("statut") in ("SUCCES", "PARTIEL") else 7

    if args.etape == "finition-verifier":
        v = fin.verifier()
        v["cutover"] = cut.verifier()["checks"]
        v["etat"] = etat(db)
        if dossier:
            _ecrire(dossier, f"04_verification_{datetime.now(timezone.utc):%H%M%S}.json", v)
        print(json.dumps({"checks": v["checks"], "cutover": v["cutover"],
                          "etat_comptable_anterieur": v["etat_comptable_anterieur"],
                          "etat": {k: v["etat"][k] for k in ("sha256", "taille", "wal_taille",
                                                              "schema", "integrity_check",
                                                              "foreign_key_check", "nb_tables",
                                                              "nb_lignes")}},
                         ensure_ascii=False, indent=1, default=str))
        return 0 if v["checks"]["ok"] and v["cutover"]["ok"] else 6
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

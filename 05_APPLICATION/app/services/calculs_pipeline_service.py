"""Pilotage transactionnel des runs de calcul + clôture mensuelle.

Ne remplace aucun orchestrateur métier : l'ordre des lots et leurs sorties viennent de
`calculs_executeur_service` (lui-même repris des runners `02_TRAVAIL/run_*.py`). Ce service ajoute
ce qui manquait côté application : prévisualisation scellée, sauvegarde/restauration des sorties,
journal par lot, comparaison avant/après, et statut de clôture.

Sécurité : le mode RÉEL est refusé par défaut (`CALCULS_REAL_RUN_ENABLED`), et en recette toute
écriture reste sous la racine de recette (write-guard).
"""
from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import calculs_executeur_service as ex

MODE_RECETTE = "RECETTE"
MODE_REEL = "REEL"

ST_PREPARE = "PREPARE"
ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_ECHEC = "ECHEC"
ST_RESTAURE = "RESTAURE"
ST_ANNULE = "ANNULE"

# Clôture mensuelle
CL_OUVERTE = "OUVERTE"
CL_EN_CALCUL = "EN_CALCUL"
CL_A_CONTROLER = "A_CONTROLER"
CL_BLOQUEE = "BLOQUEE"
CL_VALIDEE = "VALIDEE"
CL_CLOTUREE = "CLOTUREE"
CL_ROUVERTE = "ROUVERTE"

CLOTURE_STATUTS = [CL_OUVERTE, CL_EN_CALCUL, CL_A_CONTROLER, CL_BLOQUEE, CL_VALIDEE, CL_CLOTUREE,
                   CL_ROUVERTE]
CLOTURE_TRANSITIONS: dict[str, set[str]] = {
    CL_OUVERTE: {CL_EN_CALCUL, CL_BLOQUEE},
    CL_EN_CALCUL: {CL_A_CONTROLER, CL_BLOQUEE, CL_OUVERTE},
    CL_A_CONTROLER: {CL_VALIDEE, CL_BLOQUEE, CL_EN_CALCUL},
    CL_BLOQUEE: {CL_OUVERTE, CL_EN_CALCUL},
    CL_VALIDEE: {CL_CLOTUREE, CL_A_CONTROLER},
    CL_CLOTUREE: {CL_ROUVERTE},
    CL_ROUVERTE: {CL_EN_CALCUL, CL_A_CONTROLER},
}

E_MODE_REEL_INTERDIT = "E01_MODE_REEL_DESACTIVE"
E_PREREQUIS = "E02_PREREQUIS_NON_SATISFAITS"
E_TOKEN_INCONNU = "E03_TOKEN_INCONNU"
E_ENTREES_MODIFIEES = "E04_ENTREES_MODIFIEES"
E_RUN_INCONNU = "E05_RUN_INCONNU"
E_TRANSITION = "E06_TRANSITION_CLOTURE_INTERDITE"
E_CLOTURE_REFUSEE = "E07_CONDITIONS_CLOTURE_NON_REUNIES"

MESSAGES = {
    E_MODE_REEL_INTERDIT: "Le mode réel est désactivé sur cette installation.",
    E_PREREQUIS: "Prérequis non satisfaits : le pipeline n'a pas été lancé.",
    E_TOKEN_INCONNU: "Prévisualisation introuvable ou expirée.",
    E_ENTREES_MODIFIEES: "Les entrées ont changé depuis la prévisualisation : relancez-la.",
    E_RUN_INCONNU: "Run introuvable.",
    E_TRANSITION: "Transition de clôture interdite.",
    E_CLOTURE_REFUSEE: "Conditions de clôture non réunies.",
}

# Indicateurs relevés après un run — (nom, fichier, onglet, colonne à sommer).
# Colonne None = seul le nombre de lignes fait sens pour cet indicateur.
# Les noms de colonnes sont ceux RÉELLEMENT produits par les moteurs, relevés sur les sorties d'un
# run réussi. Un nom approximatif produirait une valeur absente — jamais un faux total — mais prive
# la comparaison avant/après de son intérêt : c'était le cas avant qu'une chaîne aille au bout.
INDICATEURS = (
    ("flux_lignes", "02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx", "MASTER", None),
    ("ca_payout", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx", "COMMISSIONS",
     "payout_calcule"),
    ("commissions", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx", "COMMISSIONS",
     "commission_conciergerie"),
    ("menages", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx", "COMMISSIONS",
     "menage_retenu"),
    ("forfaits", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx", "REGLEMENT",
     "charge_fixe_mensuelle"),
    ("net_proprietaire", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx", "REGLEMENT",
     "net_proprietaire_apres_charge_mois"),
    ("somme_a_payer", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx", "REGLEMENT",
     "reste_a_payer_conciergerie"),
    # L'onglet GLOBAL porte une ligne par vision (REEL / COMPTABLE / HORS_COMPTA) : sommer les
    # trois double-compterait. Chaque vision est donc relevée séparément, avec un filtre explicite.
    ("produits_reel", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", "GLOBAL",
     "total_produits", ("vision", "REEL")),
    ("charges_reel", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", "GLOBAL",
     "total_charges", ("vision", "REEL")),
    ("resultat_reel", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", "GLOBAL",
     "resultat", ("vision", "REEL")),
    ("resultat_comptable", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", "GLOBAL",
     "resultat", ("vision", "COMPTABLE")),
    ("resultat_hors_compta", "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx", "GLOBAL",
     "resultat", ("vision", "HORS_COMPTA")),
    ("controles", "02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx", "MASTER", None),
    ("prefactures", "02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx",
     "FACT_FACTURE_ENTETE", None),
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "", **extra: Any) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail,
            **extra}


def _mode_reel_autorise() -> bool:
    return bool(getattr(cfg, "CALCULS_REAL_RUN_ENABLED", False)
                and getattr(cfg, "CALCULS_REAL_RUN_CONFIRMATION_ENABLED", False))


def _racine(racine: Path | None = None) -> Path:
    return Path(racine or cfg.PROJECT_ROOT)


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for bloc in iter(lambda: fh.read(65536), b""):
            h.update(bloc)
    return h.hexdigest()


def _entree_banque() -> dict[str, Any]:
    """État du jeu de données Banque, présenté comme les autres entrées du pipeline.

    La Banque n'est plus un fichier : parler de « fichier absent » ferait chercher un classeur qui
    n'existe plus. On expose donc l'état du dataset — non initialisé, importé mais non classé, ou
    prêt — avec la date du dernier import comme repère de fraîcheur.
    """
    from app.services import banque_mouvements_service as bq
    from app.services import banque_vues_service as vues

    etat = vues.etat()
    imports = bq.imports()
    dernier = max((i.get("date_import") or "" for i in imports), default="")
    return {
        "fichier": "Banque (base de pilotage)",
        "dataset": "banque_mouvements",
        "existe": etat == vues.ETAT_OK,
        "etat": etat,
        "message": vues.MESSAGES.get(etat, ""),
        "nb_mouvements": bq.compter(),
        "modifie_le": dernier.replace("T", " ")[:16] or None,
    }


def empreinte_entrees(lots: list[str], racine: Path | None = None) -> str:
    """Empreinte des SORTIES existantes des lots amont + des sources de saisie : si l'une change
    entre prévisualisation et lancement, le token est refusé."""
    r = _racine(racine)
    parts: list[str] = []
    cibles: list[str] = []
    for nom in lots:
        lot = ex.TOUS_LES_LOTS.get(nom)
        if lot:
            cibles.extend(lot.sorties)
    cibles.extend(["01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx",
                   "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"])
    for rel in sorted(set(cibles)):
        p = r / rel
        parts.append(f"{rel}:{_sha256(p) if p.exists() else 'ABSENT'}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


# ── Prévisualisation ────────────────────────────────────────────────────────

def previsualiser(mois: str, lots: list[str], *, racine: Path | None = None,
                  dryruns_root: Path | None = None) -> dict[str, Any]:
    """Aucune exécution. Produit un manifeste scellé (token) + l'état réel des entrées/sorties."""
    r = _racine(racine)
    prereq = ex.verifier_prerequis(lots, racine=r)

    entrees: list[dict[str, Any]] = []
    sorties_remplacees: list[dict[str, Any]] = []
    for nom in lots:
        lot = ex.TOUS_LES_LOTS.get(nom)
        if lot is None:
            continue
        for rel in lot.sorties:
            p = r / rel
            sorties_remplacees.append({
                "fichier": rel, "existe": p.exists(),
                "modifie_le": (datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                               if p.exists() else None),
                "taille": p.stat().st_size if p.exists() else None})
    for rel in ("01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx",
                "01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm"):
        p = r / rel
        entrees.append({
            "fichier": rel, "existe": p.exists(),
            "modifie_le": (datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                           if p.exists() else None)})
    entrees.append(_entree_banque())

    token = uuid.uuid4().hex
    empreinte = empreinte_entrees(lots, racine=r)
    root = Path(dryruns_root or cfg.DRYRUNS_DIR) / "calculs" / token
    root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "token": token, "mois": mois, "lots": lots, "racine": str(r),
        "empreinte_entrees": empreinte, "prerequis": prereq,
        "entrees": entrees, "sorties_remplacees": sorties_remplacees,
        "mode": MODE_RECETTE if cfg.RECETTE_MODE else MODE_REEL,
        "cree_le": _now(),
    }
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2,
                                                  default=str), encoding="utf-8")
    return {"ok": True, **manifest}


def charger_manifest(token: str, dryruns_root: Path | None = None) -> dict[str, Any] | None:
    p = Path(dryruns_root or cfg.DRYRUNS_DIR) / "calculs" / token / "manifest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


# ── Sauvegarde / restauration ───────────────────────────────────────────────

def _sauvegarder_sorties(run_id: str, lots: list[str], racine: Path,
                         db_path=None) -> list[dict[str, Any]]:
    """Copie les sorties existantes avant exécution, pour pouvoir restaurer."""
    dossier = Path(cfg.DATA_DIR) / "calculs_sauvegardes" / run_id
    dossier.mkdir(parents=True, exist_ok=True)
    faits: list[dict[str, Any]] = []
    conn = get_db(db_path)
    try:
        for nom in lots:
            lot = ex.TOUS_LES_LOTS.get(nom)
            if lot is None:
                continue
            for rel in lot.sorties:
                src = racine / rel
                if not src.exists():
                    continue
                dst = dossier / rel.replace("/", "__")
                shutil.copy2(src, dst)
                sha = _sha256(src)
                conn.execute(
                    "INSERT INTO calculs_sauvegardes (run_id_opaque, fichier, sauvegarde, "
                    "sha256_avant) VALUES (?,?,?,?)", (run_id, rel, str(dst), sha))
                faits.append({"fichier": rel, "sauvegarde": str(dst), "sha256": sha})
        conn.commit()
    finally:
        conn.close()
    return faits


def restaurer(run_id: str, *, racine: Path | None = None, db_path=None) -> dict[str, Any]:
    """Restaure les sorties sauvegardées avant ce run. Marque le run RESTAURE."""
    r = _racine(racine)
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM calculs_sauvegardes WHERE run_id_opaque=? AND restaure=0",
            (run_id,)).fetchall()
    finally:
        conn.close()
    restaures: list[str] = []
    for row in rows:
        src = Path(row["sauvegarde"])
        dst = r / row["fichier"]
        if not src.exists():
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        from app.services.saisie_charges_transaction_service import _remplacer_fichier
        tmp = dst.parent / (dst.name + ".restore.tmp")
        shutil.copy2(src, tmp)
        try:
            _remplacer_fichier(tmp, dst)      # passe par le write-guard
        except Exception:
            tmp.unlink(missing_ok=True)
            continue
        restaures.append(row["fichier"])
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE calculs_sauvegardes SET restaure=1 WHERE run_id_opaque=?", (run_id,))
        conn.execute("UPDATE calculs_runs SET statut=? WHERE run_id_opaque=?",
                     (ST_RESTAURE, run_id))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "run_id_opaque": run_id, "fichiers_restaures": restaures}


# ── Exécution transactionnelle ──────────────────────────────────────────────

def lancer(token: str, *, acteur: str = "", timeout_s: int = ex.TIMEOUT_DEFAUT_S,
           dryruns_root: Path | None = None, db_path=None,
           sauvegarder: bool = True) -> dict[str, Any]:
    """Exécute la chaîne du manifeste, dans l'ordre, en s'arrêtant au premier échec.

    Ne présente JAMAIS un pipeline partiel comme réussi : le run est SUCCES seulement si tous les
    lots demandés sont SUCCES.
    """
    manifest = charger_manifest(token, dryruns_root)
    if manifest is None:
        return _refus(E_TOKEN_INCONNU, token)

    mode = manifest["mode"]
    if mode == MODE_REEL and not _mode_reel_autorise():
        return _refus(E_MODE_REEL_INTERDIT)

    racine = Path(manifest["racine"])
    lots = list(manifest["lots"])

    if empreinte_entrees(lots, racine=racine) != manifest["empreinte_entrees"]:
        return _refus(E_ENTREES_MODIFIEES)

    prereq = ex.verifier_prerequis(lots, racine=racine)
    if not prereq["ok"]:
        return _refus(E_PREREQUIS, json.dumps(prereq, ensure_ascii=False, default=str),
                      prerequis=prereq)

    run_id = "RUN-" + uuid.uuid4().hex[:12].upper()
    interp = ex.verifier_interpreteur()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO calculs_runs (run_id_opaque, mois, mode, statut, token_previsualisation, "
            "empreinte_entrees, racine, interpreteur, lots_demandes, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run_id, manifest["mois"], mode, ST_EN_COURS, token, manifest["empreinte_entrees"],
             str(racine), interp.get("chemin", ""), json.dumps(lots), acteur or "local"))
        for i, nom in enumerate(lots, 1):
            conn.execute("INSERT INTO calculs_run_lots (run_id_opaque, ordre, lot, statut) "
                         "VALUES (?,?,?,?)", (run_id, i, nom, ex.ST_ATTENDU))
        conn.commit()
    finally:
        conn.close()

    if sauvegarder:
        _sauvegarder_sorties(run_id, lots, racine, db_path)

    resultats: list[ex.ResultatLot] = []
    duree_totale = 0.0
    echec = False
    for i, nom in enumerate(lots, 1):
        if echec:
            _maj_lot(run_id, i, ex.ST_IGNORE, None, 0.0, "", "", {},
                     "Non lancé : un lot précédent a échoué.", db_path)
            continue
        _maj_lot(run_id, i, ex.ST_EN_COURS, None, 0.0, "", "", {}, "", db_path,
                 debut=_now())
        res = ex.executer_lot(nom, racine=racine, timeout_s=timeout_s)
        resultats.append(res)
        duree_totale += res.duree_s
        _maj_lot(run_id, i, res.statut, res.code_retour, res.duree_s,
                 ex.nettoyer_chemins(res.stdout, racine),
                 ex.nettoyer_chemins(res.stderr, racine), res.sorties,
                 ex.nettoyer_chemins(res.message, racine), db_path, fin=_now())
        if res.statut != ex.ST_SUCCES:
            echec = True

    nb_ok = sum(1 for r in resultats if r.statut == ex.ST_SUCCES)
    nb_ko = sum(1 for r in resultats if r.statut != ex.ST_SUCCES)
    statut_run = ST_SUCCES if (nb_ko == 0 and nb_ok == len(lots)) else ST_ECHEC
    resume = "" if statut_run == ST_SUCCES else "; ".join(
        f"{r.lot}: {r.message or r.statut}" for r in resultats if r.statut != ex.ST_SUCCES)

    conn = get_db(db_path)
    try:
        conn.execute("UPDATE calculs_runs SET statut=?, nb_lots_reussis=?, nb_lots_echoues=?, "
                     "duree_totale_s=?, erreur_resume=?, date_fin=? WHERE run_id_opaque=?",
                     (statut_run, nb_ok, nb_ko, round(duree_totale, 2),
                      ex.nettoyer_chemins(resume, racine), _now(), run_id))
        conn.commit()
    finally:
        conn.close()

    if statut_run == ST_SUCCES:
        relever_indicateurs(run_id, manifest["mois"], racine=racine, db_path=db_path)

    return {"ok": statut_run == ST_SUCCES, "run_id_opaque": run_id, "statut": statut_run,
            "nb_lots_reussis": nb_ok, "nb_lots_echoues": nb_ko,
            "duree_totale_s": round(duree_totale, 2), "erreur_resume": resume,
            "lots": [{"lot": r.lot, "statut": r.statut, "code_retour": r.code_retour,
                      "duree_s": r.duree_s, "message": r.message} for r in resultats]}


def _maj_lot(run_id: str, ordre: int, statut: str, code: int | None, duree: float, out: str,
             err: str, sorties: dict, message: str, db_path=None, *, debut: str = "",
             fin: str = "") -> None:
    conn = get_db(db_path)
    try:
        champs = ["statut=?", "code_retour=?", "duree_s=?", "stdout_extrait=?", "stderr_extrait=?",
                  "sorties_json=?", "message=?"]
        vals: list[Any] = [statut, code, duree, out, err,
                           json.dumps(sorties, ensure_ascii=False), message]
        if debut:
            champs.append("date_debut=?"); vals.append(debut)
        if fin:
            champs.append("date_fin=?"); vals.append(fin)
        vals.extend([run_id, ordre])
        conn.execute(f"UPDATE calculs_run_lots SET {', '.join(champs)} "
                     "WHERE run_id_opaque=? AND ordre=?", vals)
        conn.commit()
    finally:
        conn.close()


# ── Indicateurs et comparaison ──────────────────────────────────────────────

def relever_indicateurs(run_id: str, mois: str, *, racine: Path | None = None,
                        db_path=None) -> list[dict[str, Any]]:
    """Relève les indicateurs depuis les fichiers RÉELLEMENT produits. Un fichier absent donne un
    indicateur absent, jamais un zéro inventé."""
    r = _racine(racine)
    releves: list[dict[str, Any]] = []
    for entree in INDICATEURS:
        nom, rel, onglet, colonne = entree[:4]
        filtre = entree[4] if len(entree) > 4 else None
        p = r / rel
        if not p.exists():
            continue
        try:
            import openpyxl
            wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
            try:
                if onglet not in wb.sheetnames:
                    continue
                ws = wb[onglet]
                rows = list(ws.iter_rows(values_only=True))
            finally:
                wb.close()
        except Exception:
            continue
        if not rows:
            continue
        hdr = [str(c) if c is not None else "" for c in rows[0]]
        data = [dict(zip(hdr, row)) for row in rows[1:] if any(c is not None for c in row)]
        if filtre:
            col_f, val_f = filtre
            data = [d for d in data if str(d.get(col_f)) == str(val_f)]
        valeur = None
        if colonne and colonne in hdr:
            total = 0.0
            for d in data:
                v = d.get(colonne)
                if isinstance(v, (int, float)):
                    total += float(v)
            valeur = round(total, 2)
        releves.append({"indicateur": nom, "valeur": valeur, "nb_lignes": len(data)})

    conn = get_db(db_path)
    try:
        for rel_ind in releves:
            conn.execute(
                "INSERT INTO calculs_indicateurs (run_id_opaque, mois, indicateur, valeur, "
                "nb_lignes) VALUES (?,?,?,?,?)",
                (run_id, mois, rel_ind["indicateur"], rel_ind["valeur"], rel_ind["nb_lignes"]))
        conn.commit()
    finally:
        conn.close()
    return releves


def comparer(mois: str, db_path=None) -> dict[str, Any]:
    """Compare les indicateurs du dernier run avec ceux du run précédent (même mois)."""
    conn = get_db(db_path)
    try:
        runs = conn.execute(
            "SELECT run_id_opaque FROM calculs_runs WHERE mois=? AND statut=? "
            "ORDER BY id DESC LIMIT 2", (mois, ST_SUCCES)).fetchall()
        if not runs:
            return {"statut": "AUCUN_RUN", "lignes": []}
        courant = runs[0]["run_id_opaque"]
        precedent = runs[1]["run_id_opaque"] if len(runs) > 1 else None
        def _ind(run):
            if run is None:
                return {}
            rows = conn.execute(
                "SELECT indicateur, valeur, nb_lignes FROM calculs_indicateurs "
                "WHERE run_id_opaque=?", (run,)).fetchall()
            return {r["indicateur"]: dict(r) for r in rows}
        ind_c, ind_p = _ind(courant), _ind(precedent)
    finally:
        conn.close()

    lignes = []
    for nom in sorted(set(ind_c) | set(ind_p)):
        c, p = ind_c.get(nom), ind_p.get(nom)
        vc = c["valeur"] if c else None
        vp = p["valeur"] if p else None
        ecart = None
        if isinstance(vc, (int, float)) and isinstance(vp, (int, float)):
            ecart = round(vc - vp, 2)
        lignes.append({
            "indicateur": nom, "precedent": vp, "nouveau": vc, "ecart": ecart,
            "nb_lignes_precedent": p["nb_lignes"] if p else None,
            "nb_lignes_nouveau": c["nb_lignes"] if c else None,
            "anormal": bool(ecart is not None and vp not in (None, 0)
                            and abs(ecart) > abs(vp) * 0.5),
        })
    return {"statut": "OK", "run_courant": courant, "run_precedent": precedent, "lignes": lignes}


# ── Consultation ────────────────────────────────────────────────────────────

def charger_run(run_id: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        run = conn.execute("SELECT * FROM calculs_runs WHERE run_id_opaque=?", (run_id,)).fetchone()
        if run is None:
            return None
        lots = conn.execute(
            "SELECT * FROM calculs_run_lots WHERE run_id_opaque=? ORDER BY ordre",
            (run_id,)).fetchall()
        indicateurs = conn.execute(
            "SELECT * FROM calculs_indicateurs WHERE run_id_opaque=?", (run_id,)).fetchall()
    finally:
        conn.close()
    d = dict(run)
    d["lots"] = [dict(x) for x in lots]
    d["indicateurs"] = [dict(x) for x in indicateurs]
    return d


def lister_runs(mois: str = "", limit: int = 25, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if mois:
            rows = conn.execute("SELECT * FROM calculs_runs WHERE mois=? ORDER BY id DESC LIMIT ?",
                                (mois, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM calculs_runs ORDER BY id DESC LIMIT ?",
                                (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Clôture mensuelle ───────────────────────────────────────────────────────

def statut_cloture(mois: str, db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM cloture_statuts WHERE mois=?", (mois,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return {"mois": mois, "statut": CL_OUVERTE, "run_id_opaque": None, "commentaire": None}
    return dict(row)


def conditions_cloture(mois: str, *, racine: Path | None = None, db_path=None) -> dict[str, Any]:
    """Vérifie les conditions RÉELLES de clôture. Aucune n'est supposée satisfaite par défaut."""
    r = _racine(racine)
    runs = [x for x in lister_runs(mois, limit=50, db_path=db_path) if x["statut"] == ST_SUCCES]
    dernier = runs[0] if runs else None

    lots_requis = ("lot9", "lot10", "lot11", "lot12")
    lots_ok: dict[str, bool] = {}
    if dernier:
        detail = charger_run(dernier["run_id_opaque"], db_path)
        faits = {l["lot"]: l["statut"] for l in (detail or {}).get("lots", [])}
        lots_ok = {nom: faits.get(nom) == ex.ST_SUCCES for nom in lots_requis}
    else:
        lots_ok = {nom: False for nom in lots_requis}

    prefactures = (r / "02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx").exists()
    resultats = (r / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx").exists()
    nets = (r / "02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx").exists()

    # Contrôles bancaires bloquants (module Banque déjà livré).
    bloquants_banque = 0
    try:
        from app.services import banques_controles_catalogue_service as bq
        bloquants_banque = bq.controler(db_path=db_path).get("nb_bloquants", 0)
    except Exception:
        bloquants_banque = 0
    # Contrôles factures bloquants.
    bloquants_factures = 0
    try:
        from app.services import factures_controles_service as fc
        bloquants_factures = fc.controler(db_path=db_path).get("nb_bloquants", 0)
    except Exception:
        bloquants_factures = 0

    conditions = {
        "run_reussi": dernier is not None,
        "lots_requis_reussis": all(lots_ok.values()),
        "prefactures_generees": prefactures,
        "resultats_disponibles": resultats,
        "nets_proprietaires_calcules": nets,
        "aucun_controle_bloquant": (bloquants_banque + bloquants_factures) == 0,
    }
    return {
        "conditions": conditions, "toutes_reunies": all(conditions.values()),
        "detail_lots": lots_ok, "dernier_run": dernier,
        "bloquants_banque": bloquants_banque, "bloquants_factures": bloquants_factures,
    }


def changer_statut_cloture(mois: str, nouveau: str, *, commentaire: str = "", acteur: str = "",
                           racine: Path | None = None, db_path=None) -> dict[str, Any]:
    if nouveau not in CLOTURE_STATUTS:
        return _refus(E_TRANSITION, nouveau)
    courant = statut_cloture(mois, db_path)
    ancien = courant["statut"]
    if nouveau != ancien and nouveau not in CLOTURE_TRANSITIONS.get(ancien, set()):
        return _refus(E_TRANSITION, f"{ancien} -> {nouveau}")

    # VALIDEE exige que toutes les conditions soient réellement réunies.
    if nouveau == CL_VALIDEE:
        c = conditions_cloture(mois, racine=racine, db_path=db_path)
        if not c["toutes_reunies"]:
            manquantes = [k for k, v in c["conditions"].items() if not v]
            return _refus(E_CLOTURE_REFUSEE, ", ".join(manquantes), conditions=c)

    conn = get_db(db_path)
    try:
        if courant.get("id"):
            conn.execute("UPDATE cloture_statuts SET statut=?, commentaire=?, acteur=?, "
                         "date_modification=?, version=version+1 WHERE mois=?",
                         (nouveau, commentaire or None, acteur or "local", _now(), mois))
        else:
            conn.execute("INSERT INTO cloture_statuts (mois, statut, commentaire, acteur) "
                         "VALUES (?,?,?,?)", (mois, nouveau, commentaire or None, acteur or "local"))
        conn.execute("INSERT INTO cloture_statut_evenements (mois, ancien_statut, nouveau_statut, "
                     "commentaire, acteur) VALUES (?,?,?,?,?)",
                     (mois, ancien, nouveau, commentaire or None, acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "mois": mois, "ancien_statut": ancien, "statut": nouveau}


def historique_cloture(mois: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_statut_evenements WHERE mois=? ORDER BY id DESC",
            (mois,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

"""APP-4B — Application des décisions de contrôle bancaire. SQLITE UNIQUEMENT.

CE QUE CE MODULE FAISAIT, ET POURQUOI CE N'EST PLUS NÉCESSAIRE
Il recopiait `BANQUE_LOT8_IMPORT.xlsx` dans un workspace isolé, réécrivait les cellules de
`NORM_Banque` d'après les décisions humaines, et vérifiait que le fichier réel n'avait pas bougé.
Tout cet appareil existait parce qu'Excel était la vérité métier : une décision n'était « appliquée »
qu'écrite dans une cellule, et il fallait donc un double pour ne pas abîmer l'original.

La vérité est désormais en base. Une décision est enregistrée dans `banque_overrides` (migration
0006) et la vue de lecture l'applique par-dessus la classification — sans réécrire quoi que ce soit,
et sans jamais perdre ce que la règle avait proposé. Il n'y a plus de copie à faire, donc plus
d'original à protéger.

CE QUI EST CONSERVÉ
Le RUN reste : `banque_controle_runs` garde la trace de chaque application, avec la répartition des
statuts avant et après. C'est ce qui permet de dire quel lot de décisions a produit quel effet — une
information d'audit, pas un artefact du support Excel. Le garde du mode réel reste également : il
protège maintenant l'écriture en base au lieu de l'écriture fichier.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import banque_vues_service as vues
from app.services.audit_service import log_event

ST_SUCCES = "SUCCES"
ST_ECHEC = "ECHEC"
ST_BLOQUE = "BLOQUE"

# Colonnes qu'une décision humaine peut porter. Identiques à avant : le vocabulaire métier ne change
# pas parce que le support change.
COLS_APPLIQUEES = vues.COLONNES_SURCHARGEES


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        cur = conn.execute(f"INSERT INTO banque_controle_runs ({cols}) VALUES ({ph})",
                           tuple(champs.values()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _repartition_statuts(db_path, *, avec_decisions: bool) -> dict[str, int]:
    """Compte les mouvements par statut de contrôle, décisions appliquées ou non.

    « Avant » et « après » se lisent sur la MÊME donnée, vue avec puis sans les décisions. Aucune
    copie n'est nécessaire pour comparer, et les deux comptages portent sur le même ensemble de
    mouvements par construction — ce que deux fichiers ne garantissaient pas.
    """
    from app.services import banque_classification_service as cls

    lignes = vues.mouvements_normalises(db_path=db_path)
    if not avec_decisions:
        classifs = {c["mouvement_id_opaque"]: c for c in cls.classifications(db_path=db_path)}
        lignes = [{**l, "statut_controle": classifs.get(l["mouvement_id"], {}).get(
            "statut_controle")} for l in lignes]
    compte: dict[str, int] = {}
    for l in lignes:
        cle = str(l.get("statut_controle") or "")
        compte[cle] = compte.get(cle, 0) + 1
    return compte


def appliquer_decisions(mode: str = "COPIES", db_path=None) -> dict[str, Any]:
    """Journalise l'application des décisions actives et l'effet qu'elles produisent.

    Aucune donnée n'est réécrite : les décisions sont déjà enregistrées, et la vue les applique. Ce
    run les rattache à un lot et mesure leur effet.
    """
    db_path = db_path or cfg.DB_PATH

    # Le garde protège désormais l'écriture en base. Il reste refusé par défaut : activer le mode
    # réel est une décision explicite, pas un effet de bord d'un clic dans l'interface.
    if mode == "REEL" and not (cfg.BANQUE_REAL_WRITE_ENABLED
                               and cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED):
        run_id = _enregistrer_run(
            db_path, mode="REEL", statut=ST_BLOQUE, erreur_code="E_REEL_DESACTIVE",
            erreur_resume="BANQUE_REAL_WRITE_ENABLED=False — aucune écriture réelle.")
        log_event("BANQUE_WRITE_BLOQUE", {"mode": "REEL"}, db_path=db_path)
        return {"ok": False, "run_id": run_id, "statut": ST_BLOQUE,
                "erreur_code": "E_REEL_DESACTIVE",
                "message": "Écriture bancaire réelle désactivée. Aucune donnée réelle touchée."}

    if not vues.initialisee(db_path=db_path):
        run_id = _enregistrer_run(db_path, mode="COPIES", statut=ST_ECHEC,
                                  erreur_code=vues.ETAT_NON_INITIALISEE,
                                  erreur_resume="Aucun mouvement bancaire en base.")
        return {"ok": False, "run_id": run_id, "statut": ST_ECHEC,
                "erreur_code": vues.ETAT_NON_INITIALISEE,
                "message": vues.MESSAGES[vues.ETAT_NON_INITIALISEE]}

    overrides = _overrides_actifs(db_path)
    try:
        ctrl_avant = _repartition_statuts(db_path, avec_decisions=False)
        ctrl_apres = _repartition_statuts(db_path, avec_decisions=True)

        run_id = _enregistrer_run(
            db_path, mode="COPIES", statut=ST_SUCCES, nb_overrides=len(overrides),
            reel_intact=1,
            controles_avant_json=json.dumps(ctrl_avant, ensure_ascii=False),
            controles_apres_json=json.dumps(ctrl_apres, ensure_ascii=False))

        conn = get_db(db_path)
        try:
            conn.execute("UPDATE banque_overrides SET run_id=? WHERE actif=1 AND run_id IS NULL",
                         (run_id,))
            conn.commit()
        finally:
            conn.close()
        log_event("BANQUE_DECISIONS_APPLIQUEES", {"run_id": run_id, "nb": len(overrides)},
                  db_path=db_path)
        return {"ok": True, "run_id": run_id, "statut": ST_SUCCES,
                "nb_overrides": len(overrides), "reel_intact": True,
                "controles_avant": ctrl_avant, "controles_apres": ctrl_apres}
    except Exception as exc:
        run_id = _enregistrer_run(db_path, mode="COPIES", statut=ST_ECHEC,
                                  erreur_code="E_INATTENDU",
                                  erreur_resume=f"{type(exc).__name__}: {exc}")
        return {"ok": False, "run_id": run_id, "statut": ST_ECHEC, "erreur_code": "E_INATTENDU",
                "message": f"Erreur inattendue ({type(exc).__name__}). Aucune donnée réelle "
                           "touchée."}


# Ancien nom, conservé le temps que les appelants soient renommés : il n'y a plus de copie, mais
# rompre les appels en même temps qu'on change le mécanisme rendrait une panne illisible.
enregistrer_sur_copie = appliquer_decisions


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
